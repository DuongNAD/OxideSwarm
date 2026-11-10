//! The worker's workspace store (design 0039 D2 and D3).
//!
//! A workspace is a named directory tree that survives between tasks, so a build in it can reuse
//! its `target/`. This module is self-contained: it holds the root, the layout, the sync of a
//! [`TreeManifest`] into `tree/`, the locks and the size bounds. Nothing here talks to the
//! network; blobs arrive through a [`BlobSource`].
//!
//! Layout: `<root>/<name>/{tree,tmp,state.json,lock}` and `<root>/store.lock`.
//!
//! Sync differs from the text of D3 in two details that do not change the outcome. Deletions run
//! before the writes, so a path that turns from a file into a directory (or back) needs no special
//! case. Temporary files for a write live in `<name>/tmp/`, which is on the same file system as
//! the tree (so the rename is atomic) and which a crash cannot leave half-files in the tree from.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusty_grid_core::blob::{
    fold_blob_path, open_regular_file_nofollow, validate_blob_path, validate_symlink_target,
    BlobFile, BlobHasher, BlobId, BlobSymlink, FoldedPathTrie, TreeManifest,
};
use rusty_grid_core::task::validate_workspace_name;
use serde::{Deserialize, Serialize};

use crate::sandbox::{ensure_private_dir, sanitize_relative_path, SandboxError};

/// Environment variable that carries the workspace name into the task.
pub const ENV_WORKSPACE: &str = "RUSTY_GRID_WORKSPACE";
/// Environment variable that carries the tree path into the task.
pub const ENV_WORKSPACE_DIR: &str = "RUSTY_GRID_WORKSPACE_DIR";
/// Default for `--workspace-limit-mb`.
pub const DEFAULT_WORKSPACE_LIMIT_MB: u64 = 20_480;
/// Default for `--max-workspaces`.
pub const DEFAULT_MAX_WORKSPACES: usize = 8;
/// Default cap on the directory entries visited when a workspace is measured.
pub const DEFAULT_MEASURE_ENTRY_CAP: usize = 2_000_000;

const STATE_FILE: &str = "state.json";
const STATE_VERSION: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum WorkspaceError {
    #[error("workspace root '{}' cannot be used: {reason}", path.display())]
    Root { path: PathBuf, reason: String },
    #[error("invalid workspace name: {0}")]
    InvalidName(String),
    #[error("workspace '{0}' is in use by another task")]
    Busy(String),
    #[error("workspace path '{path}' refused: {reason}")]
    Path { path: String, reason: String },
    #[error("blob for '{path}' could not be read: {reason}")]
    Fetch { path: String, reason: String },
    #[error("blob for '{path}' does not match its id (size or BLAKE3 hash)")]
    Integrity { path: String },
    #[error("{context}: {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },
}

fn io_err(context: impl Into<String>) -> impl FnOnce(std::io::Error) -> WorkspaceError {
    let context = context.into();
    move |source| WorkspaceError::Io { context, source }
}

/// Where a sync gets the content of a blob. The later wiring plugs the blob cache in; tests
/// supply blobs from memory. A plain closure works too.
pub trait BlobSource {
    fn open(&self, id: &BlobId) -> std::io::Result<Box<dyn Read + Send>>;
}

impl<F> BlobSource for F
where
    F: Fn(&BlobId) -> std::io::Result<Box<dyn Read + Send>>,
{
    fn open(&self, id: &BlobId) -> std::io::Result<Box<dyn Read + Send>> {
        self(id)
    }
}

/// Host family for the default root. `Platform::current()` is the one in use; the others exist so
/// the rules can be tested on every host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Linux,
    MacOs,
    Windows,
}

impl Platform {
    pub fn current() -> Self {
        if cfg!(windows) {
            Platform::Windows
        } else if cfg!(target_os = "macos") {
            Platform::MacOs
        } else {
            Platform::Linux
        }
    }
}

/// `<cache dir>/oxideswarm/workspaces` (D2): `$XDG_CACHE_HOME` or `~/.cache` on Linux,
/// `~/Library/Caches` on macOS, `%LOCALAPPDATA%` on Windows. `get` reads an environment variable
/// (empty values count as unset). `None` when the needed variable is missing.
pub fn default_root_with(
    platform: Platform,
    get: impl Fn(&str) -> Option<String>,
) -> Option<PathBuf> {
    let get = |k: &str| get(k).filter(|v| !v.is_empty());
    let cache: PathBuf = match platform {
        Platform::Linux => match get("XDG_CACHE_HOME") {
            Some(x) if x.starts_with('/') => PathBuf::from(x),
            _ => PathBuf::from(get("HOME")?).join(".cache"),
        },
        Platform::MacOs => PathBuf::from(get("HOME")?).join("Library").join("Caches"),
        Platform::Windows => PathBuf::from(get("LOCALAPPDATA")?),
    };
    Some(cache.join("oxideswarm").join("workspaces"))
}

/// [`default_root_with`] for this host and its real environment. Windows has no `HOME`
/// fallback, so a private `USERPROFILE` does not reach it; set `LOCALAPPDATA` for tests.
pub fn default_root() -> Option<PathBuf> {
    default_root_with(Platform::current(), |k| std::env::var(k).ok())
}

/// Size bounds that apply to the whole root.
#[derive(Debug, Clone, Copy)]
pub struct WorkspaceLimits {
    pub limit_mb: u64,
    pub max_workspaces: usize,
    /// Directory entries visited per measurement; more counts as above the limit.
    pub measure_entry_cap: usize,
}

impl Default for WorkspaceLimits {
    fn default() -> Self {
        Self {
            limit_mb: DEFAULT_WORKSPACE_LIMIT_MB,
            max_workspaces: DEFAULT_MAX_WORKSPACES,
            measure_entry_cap: DEFAULT_MEASURE_ENTRY_CAP,
        }
    }
}

impl WorkspaceLimits {
    fn limit_bytes(&self) -> u64 {
        self.limit_mb.saturating_mul(1024 * 1024)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncState {
    Ready,
    Syncing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FileRecord {
    /// BLAKE3 id, 64 hex digits.
    id: String,
    size: u64,
    /// Modification time as the sync left it, nanoseconds since the Unix epoch.
    mtime_ns: u64,
}

/// The content of `state.json` (D2, D3, W11).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceState {
    version: u32,
    pub state: SyncState,
    records: BTreeMap<String, FileRecord>,
    pending: BTreeSet<String>,
    #[serde(default)]
    symlinks: BTreeMap<String, String>,
    #[serde(default)]
    pending_symlinks: BTreeSet<String>,
    #[serde(default)]
    empty_dirs: BTreeSet<String>,
    #[serde(default)]
    pending_dirs: BTreeSet<String>,
    /// Milliseconds since the Unix epoch.
    pub last_used_ms: u64,
    pub size_bytes: u64,
}

impl WorkspaceState {
    fn fresh() -> Self {
        Self {
            version: STATE_VERSION,
            state: SyncState::Syncing,
            records: BTreeMap::new(),
            pending: BTreeSet::new(),
            symlinks: BTreeMap::new(),
            pending_symlinks: BTreeSet::new(),
            empty_dirs: BTreeSet::new(),
            pending_dirs: BTreeSet::new(),
            last_used_ms: 0,
            size_bytes: 0,
        }
    }
}

/// What a sync did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SyncReport {
    /// Files written (new or changed): blobs fetched.
    pub written: usize,
    /// Files kept as they were.
    pub unchanged: usize,
    /// Files hashed (not skipped by the size and time fast path).
    pub hashed: usize,
    pub deleted: usize,
    /// Symbolic links skipped when the Windows worker lacks `SeCreateSymbolicLinkPrivilege`
    /// (OS error `1314`) and Developer Mode is off.
    pub skipped_symlinks: usize,
    /// Warning for the task's stderr when symbolic links were skipped due to error `1314`.
    pub warning: Option<String>,
}

/// One row of [`WorkspaceStore::list`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceInfo {
    pub name: String,
    pub state: SyncState,
    pub size_bytes: u64,
    pub last_used_ms: u64,
}

/// What [`WorkspaceGuard::finish`] decided.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FinishReport {
    pub size_bytes: u64,
    /// Text for the task's stderr when the workspace alone exceeded the limit (it was evicted).
    pub warning: Option<String>,
    /// Other workspaces evicted to make the total fit.
    pub evicted: Vec<String>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn mtime_ns(meta: &fs::Metadata) -> u64 {
    meta.modified()
        .ok()
        .and_then(|m| m.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Names (full lock paths) of workspaces this process holds, so tasks of one worker queue before
/// they reach the file lock.
fn in_process_held() -> &'static Mutex<HashSet<PathBuf>> {
    static HELD: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    HELD.get_or_init(|| Mutex::new(HashSet::new()))
}

/// The root directory of all workspaces of one worker.
#[derive(Debug, Clone)]
pub struct WorkspaceStore {
    root: PathBuf,
    limits: WorkspaceLimits,
    clock: fn() -> u64,
}

impl WorkspaceStore {
    /// Opens (creating it private) the root: `root` when given, else [`default_root`]. Applies the
    /// design 0021 checks (mode 0700, owned by this user, not a symbolic link).
    pub fn open(root: Option<PathBuf>, limits: WorkspaceLimits) -> Result<Self, WorkspaceError> {
        let root = match root.or_else(default_root) {
            Some(r) => r,
            None => {
                return Err(WorkspaceError::Root {
                    path: PathBuf::new(),
                    reason: "no cache directory is known; pass --workspace-root".to_string(),
                })
            }
        };
        if !root.is_absolute() {
            return Err(WorkspaceError::Root {
                path: root,
                reason: "the root must be an absolute path".to_string(),
            });
        }
        #[cfg(not(unix))]
        fs::create_dir_all(&root).map_err(io_err(format!("creating {}", root.display())))?;
        ensure_private_dir(&root).map_err(|e: SandboxError| WorkspaceError::Root {
            path: root.clone(),
            reason: e.to_string(),
        })?;
        Ok(Self {
            root,
            limits,
            clock: now_ms,
        })
    }

    /// Replaces the clock (milliseconds since the epoch); for tests that order uses.
    pub fn with_clock(mut self, clock: fn() -> u64) -> Self {
        self.clock = clock;
        self
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn limits(&self) -> WorkspaceLimits {
        self.limits
    }

    fn ws_dir(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    /// Takes the exclusive lock of workspace `name`, waiting up to `wait` for another task (this
    /// process or another worker on the same root) to end. [`WorkspaceError::Busy`] on timeout.
    pub fn acquire(&self, name: &str, wait: Duration) -> Result<WorkspaceGuard, WorkspaceError> {
        validate_workspace_name(name).map_err(WorkspaceError::InvalidName)?;
        let dir = self.ws_dir(name);
        make_private_dir(&dir)?;
        let lock_path = dir.join("lock");
        let deadline = Instant::now() + wait;
        loop {
            if let Some(guard) = self.try_acquire_inner(name, &dir, &lock_path)? {
                return Ok(guard);
            }
            if Instant::now() >= deadline {
                return Err(WorkspaceError::Busy(name.to_string()));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn try_acquire_inner(
        &self,
        name: &str,
        dir: &Path,
        lock_path: &Path,
    ) -> Result<Option<WorkspaceGuard>, WorkspaceError> {
        {
            let mut held = in_process_held().lock().unwrap_or_else(|e| e.into_inner());
            if !held.insert(lock_path.to_path_buf()) {
                return Ok(None);
            }
        }
        let release = |lp: &Path| {
            in_process_held()
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(lp);
        };
        let file = match OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(lock_path)
        {
            Ok(f) => f,
            Err(e) => {
                release(lock_path);
                return Err(io_err(format!("opening {}", lock_path.display()))(e));
            }
        };
        match file.try_lock() {
            Ok(()) => Ok(Some(WorkspaceGuard {
                store: self.clone(),
                name: name.to_string(),
                dir: dir.to_path_buf(),
                lock_path: lock_path.to_path_buf(),
                _file: file,
            })),
            Err(fs::TryLockError::WouldBlock) => {
                release(lock_path);
                Ok(None)
            }
            Err(fs::TryLockError::Error(e)) => {
                release(lock_path);
                Err(io_err(format!("locking {}", lock_path.display()))(e))
            }
        }
    }

    /// Whether the lock of `name` can be taken right now, and takes it when it can.
    fn try_lock_workspace(&self, name: &str) -> Option<WorkspaceGuard> {
        let dir = self.ws_dir(name);
        let lock_path = dir.join("lock");
        self.try_acquire_inner(name, &dir, &lock_path)
            .ok()
            .flatten()
    }

    /// The workspaces on disk, in no particular order. One without a readable `state.json` is listed as `Syncing` with its measured size.
    pub fn list(&self) -> Vec<WorkspaceInfo> {
        let mut out = Vec::new();
        let Ok(rd) = fs::read_dir(&self.root) else {
            return out;
        };
        for entry in rd.flatten() {
            let Ok(ft) = entry.file_type() else { continue };
            if !ft.is_dir() {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            if validate_workspace_name(&name).is_err() {
                continue;
            }
            let path = entry.path();
            if let Some(st) = read_state(&path) {
                out.push(WorkspaceInfo {
                    name,
                    state: st.state,
                    size_bytes: st.size_bytes,
                    last_used_ms: st.last_used_ms,
                });
            } else if ["tree", "tmp", STATE_FILE]
                .iter()
                .any(|n| fs::symlink_metadata(path.join(n)).is_ok())
            {
                // No readable state.json but content: measure it, so it is counted and evictable.
                let (bytes, complete) = measure_dir(&path, self.limits.measure_entry_cap);
                out.push(WorkspaceInfo {
                    name,
                    state: SyncState::Syncing,
                    size_bytes: if complete {
                        bytes
                    } else {
                        bytes.max(self.limits.limit_bytes().saturating_add(1))
                    },
                    last_used_ms: entry
                        .metadata()
                        .ok()
                        .map(|m| mtime_ns(&m) / 1_000_000)
                        .unwrap_or(0),
                });
            }
        }
        out
    }

    fn lock_store(&self) -> Result<File, WorkspaceError> {
        let path = self.root.join("store.lock");
        let f = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&path)
            .map_err(io_err(format!("opening {}", path.display())))?;
        f.lock()
            .map_err(io_err(format!("locking {}", path.display())))?;
        Ok(f)
    }

    /// Evicts whole workspaces, least recently used first, until the recorded sizes fit
    /// `limit_mb` and the count fits `max_workspaces`. Skips workspaces whose lock is held (and
    /// `keep`). Runs under `store.lock`. Returns the names evicted.
    pub fn evict_to_fit(&self, keep: Option<&str>) -> Result<Vec<String>, WorkspaceError> {
        let _store = self.lock_store()?;
        let mut infos = self.list();
        let mut total: u64 = infos.iter().map(|i| i.size_bytes).sum();
        let mut count = infos.len();
        infos.sort_by(|a, b| {
            a.last_used_ms
                .cmp(&b.last_used_ms)
                .then_with(|| a.name.cmp(&b.name))
        });
        let limit = self.limits.limit_bytes();
        let mut evicted = Vec::new();
        for info in infos {
            if total <= limit && count <= self.limits.max_workspaces {
                break;
            }
            if Some(info.name.as_str()) == keep {
                continue;
            }
            let Some(guard) = self.try_lock_workspace(&info.name) else {
                continue;
            };
            guard.wipe();
            total = total.saturating_sub(info.size_bytes);
            count -= 1;
            evicted.push(info.name);
        }
        Ok(evicted)
    }
}

fn make_private_dir(dir: &Path) -> Result<(), WorkspaceError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        match fs::DirBuilder::new().mode(0o700).create(dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(io_err(format!("creating {}", dir.display()))(e)),
        }
    }
    #[cfg(not(unix))]
    {
        match fs::create_dir(dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(io_err(format!("creating {}", dir.display()))(e)),
        }
    }
    let meta =
        fs::symlink_metadata(dir).map_err(io_err(format!("inspecting {}", dir.display())))?;
    if is_link_like(&meta) || !meta.is_dir() {
        return Err(WorkspaceError::Root {
            path: dir.to_path_buf(),
            reason: "it is not a real directory".to_string(),
        });
    }
    Ok(())
}

fn is_link_like(meta: &fs::Metadata) -> bool {
    if meta.file_type().is_symlink() {
        return true;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if meta.file_attributes() & 0x0400 != 0 {
            return true;
        }
    }
    false
}

fn is_real_dir(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok_and(|m| m.is_dir() && !is_link_like(&m))
}

fn read_state(ws_dir: &Path) -> Option<WorkspaceState> {
    let bytes = fs::read(ws_dir.join(STATE_FILE)).ok()?;
    let st: WorkspaceState = serde_json::from_slice(&bytes).ok()?;
    (st.version == STATE_VERSION).then_some(st)
}

/// Counts the bytes under `path` without following links, visiting at most `cap` entries.
/// Returns the bytes and whether the walk was complete.
pub fn measure_dir(path: &Path, cap: usize) -> (u64, bool) {
    let mut total = 0u64;
    let mut visited = 0usize;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = fs::read_dir(&dir) else { continue };
        for entry in rd.flatten() {
            visited += 1;
            if visited > cap {
                return (total, false);
            }
            let Ok(meta) = fs::symlink_metadata(entry.path()) else {
                continue;
            };
            if is_link_like(&meta) {
                continue;
            }
            if meta.is_dir() {
                stack.push(entry.path());
            } else {
                total = total.saturating_add(meta.len());
            }
        }
    }
    (total, true)
}

/// Exclusive hold on one workspace. Dropping it releases the lock.
#[derive(Debug)]
pub struct WorkspaceGuard {
    store: WorkspaceStore,
    name: String,
    dir: PathBuf,
    lock_path: PathBuf,
    _file: File,
}

impl Drop for WorkspaceGuard {
    fn drop(&mut self) {
        let _ = self._file.unlock();
        in_process_held()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.lock_path);
    }
}

impl WorkspaceGuard {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The task's current directory.
    pub fn tree_dir(&self) -> PathBuf {
        self.dir.join("tree")
    }

    /// The task's `TMPDIR`.
    pub fn tmp_dir(&self) -> PathBuf {
        self.dir.join("tmp")
    }

    /// `RUSTY_GRID_WORKSPACE` and `RUSTY_GRID_WORKSPACE_DIR` for the task's environment.
    pub fn task_env(&self) -> Vec<(&'static str, String)> {
        vec![
            (ENV_WORKSPACE, self.name.clone()),
            (
                ENV_WORKSPACE_DIR,
                self.tree_dir().to_string_lossy().into_owned(),
            ),
        ]
    }

    /// The saved state, if `state.json` exists and is readable.
    pub fn state(&self) -> Option<WorkspaceState> {
        read_state(&self.dir)
    }

    /// True when the workspace holds a tree that finished a sync (a task without a source can run).
    pub fn is_ready(&self) -> bool {
        self.state().is_some_and(|s| s.state == SyncState::Ready) && is_real_dir(&self.tree_dir())
    }

    /// Marks a corrupted workspace for repair under the held workspace lock: wipes everything in
    /// the workspace except the lock file without following symbolic links, recreates clean real
    /// `tree/` and `tmp/` directories, and writes a fresh `Syncing` state so the workspace cannot
    /// run without a sync and the next sync rebuilds it cleanly.
    pub fn mark_for_repair(&self) -> Result<(), WorkspaceError> {
        self.wipe();
        make_private_dir(&self.tree_dir())?;
        make_private_dir(&self.tmp_dir())?;
        self.write_state(&WorkspaceState::fresh())
    }

    /// Pre-run check executed before every workspace task (with or without a source): if `tree` or
    /// `tmp` already exists on disk, it must be a real directory (not a symlink or Windows
    /// junction/reparse point, and not a regular file) whose canonical path is
    /// `<canonical workspace root>/<name>/{tree,tmp}`. On failure the workspace is wiped and
    /// marked for repair under the workspace lock and the task is refused.
    pub fn check_existing_dirs(&self) -> Result<(), WorkspaceError> {
        let canon_root = self
            .store
            .root()
            .canonicalize()
            .map_err(io_err("canonicalizing workspace root"))?;
        for (leaf, dir) in [("tree", self.tree_dir()), ("tmp", self.tmp_dir())] {
            match fs::symlink_metadata(&dir) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => {
                    let _ = self.mark_for_repair();
                    return Err(io_err(format!("inspecting workspace {leaf}"))(e));
                }
                Ok(meta) if is_link_like(&meta) || !meta.is_dir() => {
                    let _ = self.mark_for_repair();
                    return Err(WorkspaceError::Path {
                        path: leaf.to_string(),
                        reason: format!(
                            "workspace '{}' {leaf} directory is not a real directory; workspace reset for repair",
                            self.name
                        ),
                    });
                }
                Ok(_) => {
                    let canon_dir = match dir.canonicalize() {
                        Ok(c) => c,
                        Err(e) => {
                            let _ = self.mark_for_repair();
                            return Err(io_err(format!("canonicalizing workspace {leaf}"))(e));
                        }
                    };
                    let expected = canon_root.join(&self.name).join(leaf);
                    if canon_dir != expected {
                        let _ = self.mark_for_repair();
                        return Err(WorkspaceError::Path {
                            path: leaf.to_string(),
                            reason: format!(
                                "workspace '{}' {leaf} canonical path '{}' does not match expected '{}'; workspace reset for repair",
                                self.name,
                                canon_dir.display(),
                                expected.display()
                            ),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// Verifies that both `<ws>/tree` and `<ws>/tmp` exist as real directories (via
    /// `symlink_metadata`) and canonicalize to `<canonical workspace root>/<name>/{tree,tmp}`.
    /// On failure the workspace is wiped and marked for repair under the lock and the task is
    /// refused.
    pub fn verify_run_dirs(&self) -> Result<(), WorkspaceError> {
        let canon_root = self
            .store
            .root()
            .canonicalize()
            .map_err(io_err("canonicalizing workspace root"))?;
        for (leaf, dir) in [("tree", self.tree_dir()), ("tmp", self.tmp_dir())] {
            let meta = match fs::symlink_metadata(&dir) {
                Ok(m) => m,
                Err(e) => {
                    let _ = self.mark_for_repair();
                    return Err(io_err(format!("inspecting workspace {leaf}"))(e));
                }
            };
            if is_link_like(&meta) || !meta.is_dir() {
                let _ = self.mark_for_repair();
                return Err(WorkspaceError::Path {
                    path: leaf.to_string(),
                    reason: format!(
                        "workspace '{}' {leaf} directory is not a real directory; workspace reset for repair",
                        self.name
                    ),
                });
            }
            let canon_dir = match dir.canonicalize() {
                Ok(c) => c,
                Err(e) => {
                    let _ = self.mark_for_repair();
                    return Err(io_err(format!("canonicalizing workspace {leaf}"))(e));
                }
            };
            let expected = canon_root.join(&self.name).join(leaf);
            if canon_dir != expected {
                let _ = self.mark_for_repair();
                return Err(WorkspaceError::Path {
                    path: leaf.to_string(),
                    reason: format!(
                        "workspace '{}' {leaf} canonical path '{}' does not match expected '{}'; workspace reset for repair",
                        self.name,
                        canon_dir.display(),
                        expected.display()
                    ),
                });
            }
        }
        Ok(())
    }

    /// Empties `tmp/` (and creates it); call before each task.
    pub fn prepare_tmp(&self) -> Result<(), WorkspaceError> {
        let tmp = self.tmp_dir();
        match fs::symlink_metadata(&tmp) {
            Ok(m) if m.is_dir() && !is_link_like(&m) => {
                for entry in fs::read_dir(&tmp).map_err(io_err("reading tmp"))?.flatten() {
                    let p = entry.path();
                    let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
                    let r = if is_dir {
                        fs::remove_dir_all(&p)
                    } else {
                        fs::remove_file(&p)
                    };
                    r.map_err(io_err(format!("emptying {}", p.display())))?;
                }
            }
            Ok(_) => {
                // A link or a file where tmp/ belongs: remove the entry itself, never follow it.
                fs::remove_file(&tmp)
                    .or_else(|_| fs::remove_dir(&tmp))
                    .map_err(io_err("replacing tmp"))?;
                fs::create_dir(&tmp).map_err(io_err("creating tmp"))?;
            }
            Err(_) => fs::create_dir(&tmp).map_err(io_err("creating tmp"))?,
        }
        Ok(())
    }

    fn write_state(&self, st: &WorkspaceState) -> Result<(), WorkspaceError> {
        let tmp = self.dir.join("state.json.tmp");
        let json = serde_json::to_vec(st).map_err(|e| WorkspaceError::Io {
            context: "encoding state.json".to_string(),
            source: std::io::Error::other(e),
        })?;
        {
            let mut f = File::create(&tmp).map_err(io_err("writing state.json"))?;
            f.write_all(&json).map_err(io_err("writing state.json"))?;
            f.sync_all().map_err(io_err("flushing state.json"))?;
        }
        fs::rename(&tmp, self.dir.join(STATE_FILE)).map_err(io_err("replacing state.json"))
    }

    /// Removes everything in the workspace directory but the lock file (a held lock stays). Used by eviction; errors are ignored.
    fn wipe(&self) {
        let Ok(rd) = fs::read_dir(&self.dir) else {
            return;
        };
        for entry in rd.flatten() {
            if entry.file_name() == "lock" {
                continue;
            }
            let p = entry.path();
            match fs::symlink_metadata(&p) {
                Ok(m) if m.is_dir() && !is_link_like(&m) => {
                    let _ = fs::remove_dir_all(&p);
                }
                Ok(_) => {
                    let _ = fs::remove_file(&p).or_else(|_| fs::remove_dir(&p));
                }
                Err(_) => {}
            }
        }
    }

    /// Brings `tree/` to `manifest` (D3). On error the workspace stays `Syncing` and the next
    /// sync repairs it.
    pub fn sync(
        &self,
        manifest: &TreeManifest,
        source: &dyn BlobSource,
    ) -> Result<SyncReport, WorkspaceError> {
        let tree = self.tree_dir();
        let mut st = read_state(&self.dir).unwrap_or_else(WorkspaceState::fresh);
        let force_hash = st.state != SyncState::Ready;

        let folded_manifest_dirs = check_case_collisions(manifest)?;
        // Check every path before any change, so a bad manifest touches nothing.
        let mut targets: Vec<(&BlobFile, PathBuf)> = Vec::with_capacity(manifest.files().len());
        for f in manifest.files() {
            targets.push((f, checked_path(&tree, &f.path)?));
        }
        let mut link_targets: Vec<(&BlobSymlink, PathBuf, String)> =
            Vec::with_capacity(manifest.symlinks().len());
        for s in manifest.symlinks() {
            let link_path = checked_path(&tree, &s.path)?;
            let resolved = validate_symlink_target(&s.path, &s.target).map_err(|reason| {
                WorkspaceError::Path {
                    path: s.path.clone(),
                    reason,
                }
            })?;
            checked_path(&tree, &resolved).map_err(|e| WorkspaceError::Path {
                path: s.path.clone(),
                reason: format!("symbolic link target '{resolved}': {e}"),
            })?;
            link_targets.push((s, link_path, resolved));
        }
        let mut empty_dir_targets: Vec<(&str, PathBuf)> =
            Vec::with_capacity(manifest.empty_dirs().len());
        for d in manifest.empty_dirs() {
            empty_dir_targets.push((d.as_str(), checked_path(&tree, d)?));
        }

        let new_file_paths: BTreeSet<&str> =
            manifest.files().iter().map(|f| f.path.as_str()).collect();
        let new_symlink_paths: BTreeSet<&str> = manifest
            .symlinks()
            .iter()
            .map(|s| s.path.as_str())
            .collect();
        let new_empty_dirs: BTreeSet<&str> =
            manifest.empty_dirs().iter().map(|d| d.as_str()).collect();
        let was_tracked_symlink: HashSet<String> = st
            .symlinks
            .keys()
            .chain(st.pending_symlinks.iter())
            .cloned()
            .collect();
        let folded_tracked_symlinks: HashSet<String> = was_tracked_symlink
            .iter()
            .map(|p| fold_blob_path(p))
            .collect();
        for (s, _, _) in &link_targets {
            ensure_target_walks_no_on_disk_symlink(
                &tree,
                &s.path,
                &s.target,
                &folded_tracked_symlinks,
            )?;
        }

        // 1. pending paths, Syncing, flush.
        for p in &new_file_paths {
            st.pending.insert((*p).to_string());
        }
        for p in &new_symlink_paths {
            st.pending_symlinks.insert((*p).to_string());
        }
        for p in &new_empty_dirs {
            st.pending_dirs.insert((*p).to_string());
        }
        st.state = SyncState::Syncing;
        st.last_used_ms = (self.store.clock)();
        self.write_state(&st)?;

        match fs::symlink_metadata(&tree) {
            Ok(m) if m.is_dir() && !is_link_like(&m) => {}
            Ok(_) => {
                return Err(WorkspaceError::Path {
                    path: "tree".to_string(),
                    reason: "the tree is not a real directory".to_string(),
                })
            }
            Err(_) => fs::create_dir(&tree).map_err(io_err("creating tree"))?,
        }
        // A task may have replaced tmp/ with a link: recreate it as a real directory (this also
        // removes the temporary files a crashed sync left behind).
        self.prepare_tmp()?;
        let tmp_dir = self.tmp_dir();

        let mut report = SyncReport::default();

        // 4. delete tracked paths the manifest dropped (regular files, tracked symlinks, and
        //    tracked empty directories), then remove directories left empty.
        let stale_files: Vec<String> = st
            .records
            .keys()
            .chain(st.pending.iter())
            .filter(|p| !new_file_paths.contains(p.as_str()))
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let stale_symlinks: Vec<String> = st
            .symlinks
            .keys()
            .chain(st.pending_symlinks.iter())
            .filter(|p| !new_symlink_paths.contains(p.as_str()))
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let stale_dirs: Vec<String> = st
            .empty_dirs
            .iter()
            .chain(st.pending_dirs.iter())
            .filter(|p| !manifest.has_dir(p.as_str()))
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let mut dirs: BTreeSet<PathBuf> = BTreeSet::new();
        for p in &stale_files {
            let Ok(path) = checked_path(&tree, p) else {
                continue;
            };
            if !parents_are_real_dirs(&tree, &path) {
                continue;
            }
            match fs::symlink_metadata(&path) {
                Ok(m) if m.is_file() && !is_link_like(&m) => {
                    fs::remove_file(&path).map_err(io_err(format!("deleting {p}")))?;
                    report.deleted += 1;
                }
                _ => {}
            }
            let mut cur = path.parent();
            while let Some(d) = cur {
                if d == tree {
                    break;
                }
                dirs.insert(d.to_path_buf());
                cur = d.parent();
            }
        }
        for p in &stale_symlinks {
            let Ok(path) = checked_path(&tree, p) else {
                continue;
            };
            if !parents_are_real_dirs(&tree, &path) {
                continue;
            }
            if let Ok(m) = fs::symlink_metadata(&path) {
                if is_link_like(&m) {
                    remove_symlink(&path).map_err(io_err(format!("deleting symbolic link {p}")))?;
                    report.deleted += 1;
                }
            }
            let mut cur = path.parent();
            while let Some(d) = cur {
                if d == tree {
                    break;
                }
                dirs.insert(d.to_path_buf());
                cur = d.parent();
            }
        }
        for p in &stale_dirs {
            let Ok(path) = checked_path(&tree, p) else {
                continue;
            };
            if !parents_are_real_dirs(&tree, &path) {
                continue;
            }
            let mut cur = Some(path.as_path());
            while let Some(d) = cur {
                if d == tree {
                    break;
                }
                dirs.insert(d.to_path_buf());
                cur = d.parent();
            }
        }
        // Deepest first; remove_dir fails on a non-empty directory, which is the point.
        for d in dirs.iter().rev() {
            if let Ok(m) = fs::symlink_metadata(d) {
                if m.is_dir()
                    && !is_link_like(&m)
                    && fs::remove_dir(d).is_ok()
                    && stale_dirs
                        .iter()
                        .any(|sd| d.strip_prefix(&tree).ok() == Some(Path::new(sd)))
                {
                    report.deleted += 1;
                }
            }
        }

        // 2 and 3. add or replace regular files, empty directories, and in-tree symbolic links.
        let mut records = BTreeMap::new();
        for (file, path) in &targets {
            ensure_parents(&tree, path, &file.path)?;
            let want_exec = file.executable;
            let kept = match fs::symlink_metadata(path) {
                Ok(m) if is_link_like(&m) => {
                    return Err(WorkspaceError::Path {
                        path: file.path.clone(),
                        reason: "the tree has a symbolic link here".to_string(),
                    })
                }
                Ok(m) if m.is_dir() => {
                    return Err(WorkspaceError::Path {
                        path: file.path.clone(),
                        reason: "the tree has a directory here".to_string(),
                    })
                }
                Ok(m) if m.is_file() => {
                    let fast = !force_hash
                        && st.records.get(&file.path).is_some_and(|r| {
                            r.id == file.id.to_hex()
                                && r.size == m.len()
                                && r.mtime_ns == mtime_ns(&m)
                        });
                    if fast {
                        true
                    } else {
                        report.hashed += 1;
                        hash_matches(path, file)
                    }
                }
                Ok(_) => {
                    return Err(WorkspaceError::Path {
                        path: file.path.clone(),
                        reason: "the tree has something that is not a regular file here"
                            .to_string(),
                    })
                }
                Err(_) => false,
            };
            if kept {
                report.unchanged += 1;
                set_exec(path, want_exec);
            } else {
                let tmp = tmp_dir.join(format!(".sync-{}", uuid::Uuid::new_v4().simple()));
                let res = write_blob(source, file, &tmp, want_exec).and_then(|()| {
                    fs::rename(&tmp, path).map_err(io_err(format!("placing {}", file.path)))
                });
                if let Err(e) = res {
                    let _ = fs::remove_file(&tmp);
                    return Err(e);
                }
                report.written += 1;
            }
            let meta =
                fs::symlink_metadata(path).map_err(io_err(format!("inspecting {}", file.path)))?;
            records.insert(
                file.path.clone(),
                FileRecord {
                    id: file.id.to_hex(),
                    size: meta.len(),
                    mtime_ns: mtime_ns(&meta),
                },
            );
        }

        let mut empty_dirs_out = BTreeSet::new();
        for (dir_rel, dir_path) in &empty_dir_targets {
            ensure_parents(&tree, dir_path, dir_rel)?;
            match fs::symlink_metadata(dir_path) {
                Ok(m) if m.is_dir() && !is_link_like(&m) => {}
                Ok(m) => {
                    let what = if is_link_like(&m) {
                        "a symbolic link"
                    } else {
                        "a file"
                    };
                    return Err(WorkspaceError::Path {
                        path: (*dir_rel).to_string(),
                        reason: format!("the tree has {what} here"),
                    });
                }
                Err(_) => {
                    fs::create_dir(dir_path)
                        .map_err(io_err(format!("creating empty directory {dir_rel}")))?;
                }
            }
            empty_dirs_out.insert((*dir_rel).to_string());
        }

        let symlinks_out = materialize_symlinks(
            &tree,
            &self.name,
            &link_targets,
            &folded_manifest_dirs,
            &was_tracked_symlink,
            &mut report,
        )?;

        // 5. records := new manifest, clear pending, Ready, flush.
        st.records = records;
        st.pending.clear();
        st.symlinks = symlinks_out;
        st.pending_symlinks.clear();
        st.empty_dirs = empty_dirs_out;
        st.pending_dirs.clear();
        st.state = SyncState::Ready;
        self.write_state(&st)?;
        Ok(report)
    }

    /// Call after the task ends: measures the workspace, records size and use, evicts this
    /// workspace when it alone exceeds the limit (the warning is for the task's stderr), and then
    /// evicts others (least recently used first) until the whole root fits.
    pub fn finish(&self) -> Result<FinishReport, WorkspaceError> {
        let limits = self.store.limits;
        let limit = limits.limit_bytes();
        let (mut bytes, complete) = measure_dir(&self.dir, limits.measure_entry_cap);
        if !complete {
            bytes = bytes.max(limit.saturating_add(1));
        }
        let mut report = FinishReport {
            size_bytes: bytes,
            ..Default::default()
        };
        {
            // A missing or malformed state.json still gets a record, so the workspace is counted
            // and can be evicted.
            let mut st = read_state(&self.dir).unwrap_or_else(WorkspaceState::fresh);
            st.size_bytes = bytes;
            st.last_used_ms = (self.store.clock)();
            if bytes > limit {
                self.wipe();
                let mib = |b: u64| b / (1024 * 1024);
                report.warning = Some(format!(
                    "workspace '{}' is {} MiB{}, above the limit of {} MiB; it was evicted, so the \
                     next task in it starts cold",
                    self.name,
                    mib(bytes),
                    if complete { "" } else { " or more" },
                    limits.limit_mb
                ));
            } else {
                self.write_state(&st)?;
            }
        }
        report.evicted = self.store.evict_to_fit(Some(&self.name))?;
        Ok(report)
    }
}

/// Refuses a manifest whose paths differ only by case or Unicode NFC/NFD normalisation (`A.rs`
/// and `a.rs`, `caf\u{00e9}.rs` and `cafe\u{0301}.rs`, or a file `A` next to `a/x`): on the
/// case- and normalisation-insensitive default file systems of macOS and Windows they are one
/// file, so the sync would leave one of the contents in place and rewrite it every time. Uses
/// [`FoldedPathTrie`] (node-ID trie over [`fold_blob_path`]) on every host.
fn check_case_collisions(manifest: &TreeManifest) -> Result<FoldedPathTrie, WorkspaceError> {
    FoldedPathTrie::from_manifest(manifest).map_err(|(path, other)| WorkspaceError::Path {
        path,
        reason: format!(
            "it differs from '{other}' only by case or Unicode normalisation, which one file \
             system entry cannot hold on macOS and Windows"
        ),
    })
}

#[cfg(test)]
thread_local! {
    static FORCE_SYMLINK_1314: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(windows)]
fn verbatim_dir_path(p: &Path) -> PathBuf {
    use std::os::windows::ffi::{OsStrExt, OsStringExt};
    let wide: Vec<u16> = p.as_os_str().encode_wide().collect();
    if wide.starts_with(&[b'\\' as u16, b'\\' as u16, b'?' as u16, b'\\' as u16])
        || wide.starts_with(&[b'\\' as u16, b'\\' as u16, b'.' as u16, b'\\' as u16])
    {
        return p.to_path_buf();
    }
    if p.is_absolute() {
        let mut out: Vec<u16> = Vec::with_capacity(wide.len() + 4);
        out.extend_from_slice(&[b'\\' as u16, b'\\' as u16, b'?' as u16, b'\\' as u16]);
        for &ch in &wide {
            out.push(if ch == b'/' as u16 { b'\\' as u16 } else { ch });
        }
        return PathBuf::from(std::ffi::OsString::from_wide(&out));
    }
    p.to_path_buf()
}

#[cfg(unix)]
const MAX_OPEN_WALK_FDS: usize = 32;

#[cfg(unix)]
enum UnixWalkState {
    Dir(Option<std::os::fd::OwnedFd>),
    NonTraversable,
}

#[cfg(unix)]
fn openat_dir_nofollow(
    parent_fd: std::os::fd::RawFd,
    seg: &str,
    link_path: &str,
    target: &str,
    rel: &str,
) -> Result<std::os::fd::OwnedFd, WorkspaceError> {
    use std::os::fd::FromRawFd;
    let c_seg = std::ffi::CString::new(seg).map_err(|_| WorkspaceError::Path {
        path: link_path.to_string(),
        reason: format!("symbolic link target '{target}' contains a NUL byte"),
    })?;
    let raw = unsafe {
        libc::openat(
            parent_fd,
            c_seg.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if raw < 0 {
        let err = std::io::Error::last_os_error();
        return Err(WorkspaceError::Path {
            path: link_path.to_string(),
            reason: format!(
                "cannot open directory '{rel}' while checking symbolic link target '{target}': {err}"
            ),
        });
    }
    Ok(unsafe { std::os::fd::OwnedFd::from_raw_fd(raw) })
}

#[cfg(unix)]
fn ensure_unix_parent_fd(
    root_fd: std::os::fd::RawFd,
    cur: &[&str],
    stack: &mut [UnixWalkState],
    idx: usize,
    link_path: &str,
    target: &str,
) -> Result<std::os::fd::RawFd, WorkspaceError> {
    use std::os::fd::AsRawFd;
    if let UnixWalkState::Dir(Some(fd)) = &stack[idx] {
        return Ok(fd.as_raw_fd());
    }
    let mut start = idx;
    while start > 0 {
        if matches!(stack[start - 1], UnixWalkState::Dir(Some(_))) {
            break;
        }
        start -= 1;
    }
    let mut cur_fd = if start == 0 {
        root_fd
    } else if let UnixWalkState::Dir(Some(fd)) = &stack[start - 1] {
        fd.as_raw_fd()
    } else {
        unreachable!()
    };
    for step in start..=idx {
        let rel = cur[..=step].join("/");
        let opened = openat_dir_nofollow(cur_fd, cur[step], link_path, target, &rel)?;
        cur_fd = opened.as_raw_fd();
        let open_count = stack
            .iter()
            .filter(|s| matches!(s, UnixWalkState::Dir(Some(_))))
            .count();
        if open_count >= MAX_OPEN_WALK_FDS {
            if let Some(slot) = stack
                .iter_mut()
                .find(|s| matches!(s, UnixWalkState::Dir(Some(_))))
            {
                *slot = UnixWalkState::Dir(None);
            }
        }
        stack[step] = UnixWalkState::Dir(Some(opened));
    }
    Ok(cur_fd)
}

fn ensure_target_walks_no_on_disk_symlink<'a>(
    tree: &Path,
    link_path: &'a str,
    target: &'a str,
    ignored_folded_symlinks: &HashSet<String>,
) -> Result<(), WorkspaceError> {
    match fs::symlink_metadata(tree) {
        Ok(m) if m.is_dir() && !is_link_like(&m) => {}
        Ok(_) => {
            return Err(WorkspaceError::Path {
                path: "tree".to_string(),
                reason: "the tree is not a real directory".to_string(),
            })
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(WorkspaceError::Path {
                path: link_path.to_string(),
                reason: format!(
                "cannot inspect workspace tree while checking symbolic link target '{target}': {e}"
            ),
            })
        }
    }

    #[cfg(unix)]
    {
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
        use std::os::unix::ffi::OsStrExt;

        let c_tree = std::ffi::CString::new(tree.as_os_str().as_bytes()).map_err(|_| {
            WorkspaceError::Path {
                path: link_path.to_string(),
                reason: "workspace tree path contains a NUL byte".to_string(),
            }
        })?;
        let raw_root = unsafe {
            libc::open(
                c_tree.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if raw_root < 0 {
            let err = std::io::Error::last_os_error();
            return Err(WorkspaceError::Path {
                path: link_path.to_string(),
                reason: format!(
                    "cannot open workspace tree while checking symbolic link target '{target}': {err}"
                ),
            });
        }
        let root_fd = unsafe { OwnedFd::from_raw_fd(raw_root) };
        let mut cur: Vec<&str> = Vec::new();
        let mut stack: Vec<UnixWalkState> = Vec::new();

        let push_seg = |seg: &'a str,
                        cur: &mut Vec<&'a str>,
                        stack: &mut Vec<UnixWalkState>|
         -> Result<(), WorkspaceError> {
            let parent_idx = cur.len();
            cur.push(seg);
            if parent_idx > 0 && matches!(stack[parent_idx - 1], UnixWalkState::NonTraversable) {
                stack.push(UnixWalkState::NonTraversable);
                return Ok(());
            }
            let parent_fd = if parent_idx == 0 {
                root_fd.as_raw_fd()
            } else {
                ensure_unix_parent_fd(
                    root_fd.as_raw_fd(),
                    &cur[..parent_idx],
                    stack,
                    parent_idx - 1,
                    link_path,
                    target,
                )?
            };
            let c_seg = std::ffi::CString::new(seg).map_err(|_| WorkspaceError::Path {
                path: link_path.to_string(),
                reason: format!("symbolic link target '{target}' contains a NUL byte"),
            })?;
            let mut st = std::mem::MaybeUninit::<libc::stat>::uninit();
            let rc = unsafe {
                libc::fstatat(
                    parent_fd,
                    c_seg.as_ptr(),
                    st.as_mut_ptr(),
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            };
            if rc != 0 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::ENOENT) {
                    stack.push(UnixWalkState::NonTraversable);
                    return Ok(());
                }
                let rel = cur.join("/");
                return Err(WorkspaceError::Path {
                    path: link_path.to_string(),
                    reason: format!(
                        "cannot inspect '{rel}' while checking symbolic link target '{target}': {err}"
                    ),
                });
            }
            let st = unsafe { st.assume_init() };
            let mode = st.st_mode & libc::S_IFMT;
            if mode == libc::S_IFLNK {
                let rel = cur.join("/");
                if ignored_folded_symlinks.contains(&fold_blob_path(&rel)) {
                    stack.push(UnixWalkState::NonTraversable);
                    return Ok(());
                }
                return Err(WorkspaceError::Path {
                    path: link_path.to_string(),
                    reason: format!(
                        "symbolic link target '{target}' traverses through symbolic link '{rel}'"
                    ),
                });
            }
            if mode == libc::S_IFDIR {
                let rel = cur.join("/");
                let child_fd = openat_dir_nofollow(parent_fd, seg, link_path, target, &rel)?;
                let open_count = stack
                    .iter()
                    .filter(|s| matches!(s, UnixWalkState::Dir(Some(_))))
                    .count();
                if open_count >= MAX_OPEN_WALK_FDS {
                    if let Some(slot) = stack
                        .iter_mut()
                        .find(|s| matches!(s, UnixWalkState::Dir(Some(_))))
                    {
                        *slot = UnixWalkState::Dir(None);
                    }
                }
                stack.push(UnixWalkState::Dir(Some(child_fd)));
            } else {
                stack.push(UnixWalkState::NonTraversable);
            }
            Ok(())
        };

        let mut init_segs: Vec<&str> = link_path.split('/').collect();
        init_segs.pop();
        for seg in init_segs {
            push_seg(seg, &mut cur, &mut stack)?;
        }
        for segment in target.split('/') {
            match segment {
                "" | "." => {}
                ".." => {
                    if cur.pop().is_some() {
                        stack.pop();
                    }
                }
                s => push_seg(s, &mut cur, &mut stack)?,
            }
        }
        Ok(())
    }

    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        };

        let mut disk_path = verbatim_dir_path(tree);
        let mut cur: Vec<&str> = Vec::new();
        // `true` when the prefix is a confirmed real directory on disk; `false` when it is absent,
        // a non-directory file, or an ignored tracked symlink.
        let mut stack: Vec<bool> = Vec::new();

        let push_seg = |seg: &'a str,
                        cur: &mut Vec<&'a str>,
                        disk_path: &mut PathBuf,
                        stack: &mut Vec<bool>|
         -> Result<(), WorkspaceError> {
            let parent_is_dir = stack.last().copied().unwrap_or(true);
            cur.push(seg);
            disk_path.push(seg);
            if !parent_is_dir {
                stack.push(false);
                return Ok(());
            }
            let meta_res = OpenOptions::new()
                .access_mode(0)
                .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS)
                .open(&*disk_path)
                .and_then(|f| f.metadata());
            match meta_res {
                Ok(m) => {
                    if is_link_like(&m) {
                        let rel = cur.join("/");
                        if ignored_folded_symlinks.contains(&fold_blob_path(&rel)) {
                            stack.push(false);
                            return Ok(());
                        }
                        return Err(WorkspaceError::Path {
                            path: link_path.to_string(),
                            reason: format!(
                                "symbolic link target '{target}' traverses through symbolic link '{rel}'"
                            ),
                        });
                    }
                    stack.push(m.is_dir());
                    Ok(())
                }
                Err(e) if e.raw_os_error() == Some(2) => {
                    stack.push(false);
                    Ok(())
                }
                Err(e) => {
                    let rel = cur.join("/");
                    Err(WorkspaceError::Path {
                        path: link_path.to_string(),
                        reason: format!(
                            "cannot inspect '{rel}' while checking symbolic link target '{target}': {e}"
                        ),
                    })
                }
            }
        };

        let mut init_segs: Vec<&str> = link_path.split('/').collect();
        init_segs.pop();
        for seg in init_segs {
            push_seg(seg, &mut cur, &mut disk_path, &mut stack)?;
        }
        for segment in target.split('/') {
            match segment {
                "" | "." => {}
                ".." => {
                    if cur.pop().is_some() {
                        disk_path.pop();
                        stack.pop();
                    }
                }
                s => push_seg(s, &mut cur, &mut disk_path, &mut stack)?,
            }
        }
        Ok(())
    }
}

fn symlink_kind_matches(meta: &fs::Metadata, target_is_dir: bool) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileTypeExt;
        let ft = meta.file_type();
        if target_is_dir {
            ft.is_symlink_dir()
        } else {
            ft.is_symlink_file()
        }
    }
    #[cfg(not(windows))]
    {
        let _ = (meta, target_is_dir);
        true
    }
}

fn create_in_tree_symlink(
    target: &str,
    link_path: &Path,
    target_is_dir: bool,
) -> std::io::Result<()> {
    #[cfg(test)]
    if FORCE_SYMLINK_1314.with(|c| c.get()) {
        return Err(std::io::Error::from_raw_os_error(1314));
    }
    #[cfg(unix)]
    {
        let _ = target_is_dir;
        std::os::unix::fs::symlink(target, link_path)
    }
    #[cfg(windows)]
    {
        let win_target = target.replace('/', "\\");
        let verbatim_link = verbatim_dir_path(link_path);
        if target_is_dir {
            std::os::windows::fs::symlink_dir(&win_target, &verbatim_link)
        } else {
            std::os::windows::fs::symlink_file(&win_target, &verbatim_link)
        }
    }
}

fn remove_symlink(path: &Path) -> std::io::Result<()> {
    #[cfg(windows)]
    let p = verbatim_dir_path(path);
    #[cfg(not(windows))]
    let p = path;
    fs::remove_file(&p).or_else(|_| fs::remove_dir(&p))
}

/// `tree` joined with the manifest path `rel`, after the checks of design 0021 and the blob path
/// rules. On Windows the sandbox rules apply in full (device names, trailing dots and spaces);
/// elsewhere a name such as `aux.rs` is a legal file.
fn checked_path(tree: &Path, rel: &str) -> Result<PathBuf, WorkspaceError> {
    let refuse = |reason: String| WorkspaceError::Path {
        path: rel.to_string(),
        reason,
    };
    validate_blob_path(rel).map_err(refuse)?;
    let joined = tree.join(rel);
    if cfg!(windows) {
        if rel.split('/').any(|s| s.ends_with('.') || s.ends_with(' ')) {
            return Err(refuse(
                "Windows drops a trailing dot or space from a name".to_string(),
            ));
        }
        let clean =
            sanitize_relative_path(tree, Path::new(rel)).map_err(|e| refuse(e.to_string()))?;
        if clean != joined {
            return Err(refuse(
                "the path is not canonical on this system".to_string(),
            ));
        }
    }
    Ok(joined)
}

/// True when every directory between `tree` and the file is a real directory (no link).
fn parents_are_real_dirs(tree: &Path, path: &Path) -> bool {
    let Ok(rel) = path.strip_prefix(tree) else {
        return false;
    };
    let mut cur = tree.to_path_buf();
    let comps: Vec<_> = rel.components().collect();
    for c in comps.iter().take(comps.len().saturating_sub(1)) {
        cur.push(c);
        match fs::symlink_metadata(&cur) {
            Ok(m) if m.is_dir() && !is_link_like(&m) => {}
            _ => return false,
        }
    }
    true
}

/// Creates the missing parents of `path`, refusing a link or a file in the way.
fn ensure_parents(tree: &Path, path: &Path, rel: &str) -> Result<(), WorkspaceError> {
    let Ok(sub) = path.strip_prefix(tree) else {
        return Err(WorkspaceError::Path {
            path: rel.to_string(),
            reason: "the path leaves the tree".to_string(),
        });
    };
    let comps: Vec<_> = sub.components().collect();
    let mut cur = tree.to_path_buf();
    for c in comps.iter().take(comps.len().saturating_sub(1)) {
        cur.push(c);
        match fs::symlink_metadata(&cur) {
            Ok(m) if m.is_dir() && !is_link_like(&m) => {}
            Ok(m) => {
                let what = if is_link_like(&m) {
                    "a symbolic link"
                } else {
                    "a file"
                };
                return Err(WorkspaceError::Path {
                    path: rel.to_string(),
                    reason: format!("a parent directory of it is {what} in the tree"),
                });
            }
            Err(_) => {
                fs::create_dir(&cur).map_err(io_err(format!("creating a parent of {rel}")))?
            }
        }
    }
    Ok(())
}

fn hash_matches(path: &Path, file: &BlobFile) -> bool {
    let Ok((f, meta)) = open_regular_file_nofollow(path) else {
        return false;
    };
    if meta.len() != file.size {
        return false;
    }
    rusty_grid_core::blob::hash_reader(f).is_ok_and(|(id, len)| id == file.id && len == file.size)
}

fn write_blob(
    source: &dyn BlobSource,
    file: &BlobFile,
    tmp: &Path,
    executable: bool,
) -> Result<(), WorkspaceError> {
    let reader = source.open(&file.id).map_err(|e| WorkspaceError::Fetch {
        path: file.path.clone(),
        reason: e.to_string(),
    })?;
    let mut reader = reader.take(file.size.saturating_add(1));
    let mut out = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(tmp)
        .map_err(io_err(format!(
            "creating a temporary file for {}",
            file.path
        )))?;
    let mut hasher = BlobHasher::new();
    let mut total = 0u64;
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let n = reader.read(&mut buf).map_err(|e| WorkspaceError::Fetch {
            path: file.path.clone(),
            reason: e.to_string(),
        })?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        total += n as u64;
        out.write_all(&buf[..n])
            .map_err(io_err(format!("writing {}", file.path)))?;
    }
    if total != file.size || hasher.finish() != file.id {
        return Err(WorkspaceError::Integrity {
            path: file.path.clone(),
        });
    }
    drop(out);
    set_exec(tmp, executable);
    Ok(())
}

#[cfg(unix)]
fn set_exec(path: &Path, executable: bool) {
    use std::os::unix::fs::PermissionsExt;
    let mode = if executable { 0o755 } else { 0o644 };
    if let Ok(m) = fs::metadata(path) {
        use std::os::unix::fs::MetadataExt;
        // A file with other hard links may be shared with something outside the tree.
        if m.nlink() > 1 {
            return;
        }
        if m.permissions().mode() & 0o777 != mode {
            let _ = fs::set_permissions(path, fs::Permissions::from_mode(mode));
        }
    }
}

#[cfg(not(unix))]
fn set_exec(_path: &Path, _executable: bool) {}

fn materialize_symlinks(
    tree: &Path,
    workspace_name: &str,
    link_targets: &[(&BlobSymlink, PathBuf, String)],
    folded_manifest_dirs: &FoldedPathTrie,
    was_tracked_symlink: &HashSet<String>,
    report: &mut SyncReport,
) -> Result<BTreeMap<String, String>, WorkspaceError> {
    let mut symlinks_out = BTreeMap::new();
    let mut active_symlinks: Vec<&Path> = Vec::new();
    let empty_set: HashSet<String> = HashSet::new();
    let rollback_symlinks = |paths: &[&Path]| {
        for p in paths {
            let _ = remove_symlink(p);
        }
    };
    for (link, path, resolved) in link_targets {
        if let Err(e) = ensure_parents(tree, path, &link.path).and_then(|()| {
            ensure_target_walks_no_on_disk_symlink(tree, &link.path, &link.target, &empty_set)
        }) {
            rollback_symlinks(&active_symlinks);
            return Err(e);
        }
        let target_is_dir =
            folded_manifest_dirs.contains_dir(resolved) || is_real_dir(&tree.join(resolved));
        let tracked = was_tracked_symlink.contains(&link.path);
        let kept = match fs::symlink_metadata(path) {
            Ok(m) if is_link_like(&m) => {
                let existing = fs::read_link(path)
                    .ok()
                    .and_then(|p| p.to_str().map(|s| s.replace('\\', "/")));
                let same_target = existing.as_deref() == Some(link.target.as_str());
                if same_target && symlink_kind_matches(&m, target_is_dir) {
                    true
                } else if same_target || tracked {
                    if let Err(e) = remove_symlink(path)
                        .map_err(io_err(format!("replacing symbolic link {}", link.path)))
                    {
                        rollback_symlinks(&active_symlinks);
                        return Err(e);
                    }
                    false
                } else {
                    rollback_symlinks(&active_symlinks);
                    return Err(WorkspaceError::Path {
                        path: link.path.clone(),
                        reason: "the tree already has a different symbolic link here".to_string(),
                    });
                }
            }
            Ok(m) if m.is_dir() => {
                rollback_symlinks(&active_symlinks);
                return Err(WorkspaceError::Path {
                    path: link.path.clone(),
                    reason: "the tree has a directory here".to_string(),
                });
            }
            Ok(_) => {
                if tracked {
                    if let Err(e) =
                        fs::remove_file(path).map_err(io_err(format!("replacing {}", link.path)))
                    {
                        rollback_symlinks(&active_symlinks);
                        return Err(e);
                    }
                    false
                } else {
                    rollback_symlinks(&active_symlinks);
                    return Err(WorkspaceError::Path {
                        path: link.path.clone(),
                        reason: "the tree has a file here".to_string(),
                    });
                }
            }
            Err(_) => false,
        };
        if kept {
            active_symlinks.push(path.as_path());
            report.unchanged += 1;
            symlinks_out.insert(link.path.clone(), link.target.clone());
        } else {
            match create_in_tree_symlink(&link.target, path, target_is_dir) {
                Ok(()) => {
                    active_symlinks.push(path.as_path());
                    report.written += 1;
                    symlinks_out.insert(link.path.clone(), link.target.clone());
                }
                Err(e) if (cfg!(windows) || cfg!(test)) && e.raw_os_error() == Some(1314) => {
                    report.skipped_symlinks += 1;
                }
                Err(e) => {
                    rollback_symlinks(&active_symlinks);
                    return Err(io_err(format!("creating symbolic link {}", link.path))(e));
                }
            }
        }
    }
    // Re-verify every link's target walk on disk after all links of this sync exist, so the
    // check uses the host filesystem's own case/Unicode folding and does not depend on the
    // order in which links were created.
    for (link, _, _) in link_targets {
        if let Err(e) =
            ensure_target_walks_no_on_disk_symlink(tree, &link.path, &link.target, &empty_set)
        {
            rollback_symlinks(&active_symlinks);
            return Err(e);
        }
    }
    if report.skipped_symlinks > 0 {
        report.warning = Some(format!(
            "workspace '{workspace_name}': skipped {} symbolic link(s) because Windows returned \
             ERROR_PRIVILEGE_NOT_HELD (1314); enable Developer Mode in Settings -> System -> \
             For developers -> Developer Mode (or grant SeCreateSymbolicLinkPrivilege) to \
             materialize symbolic links",
            report.skipped_symlinks
        ));
    }
    Ok(symlinks_out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::Arc;

    static CLOCK: AtomicU64 = AtomicU64::new(1);
    fn tick() -> u64 {
        CLOCK.fetch_add(1, Ordering::SeqCst)
    }

    type Files<'a> = &'a [(&'a str, &'a str, bool)];

    fn manifest(files: Files) -> TreeManifest {
        TreeManifest::new(
            files
                .iter()
                .map(|(p, c, x)| BlobFile {
                    path: p.to_string(),
                    id: BlobId::of(c.as_bytes()),
                    size: c.len() as u64,
                    executable: *x,
                })
                .collect(),
        )
        .unwrap()
    }

    /// A source over `files` that counts the blobs it hands out.
    struct Mem {
        blobs: HashMap<BlobId, Vec<u8>>,
        fetches: Arc<AtomicUsize>,
    }
    impl Mem {
        fn new(files: Files) -> Self {
            Self {
                blobs: files
                    .iter()
                    .map(|(_, c, _)| (BlobId::of(c.as_bytes()), c.as_bytes().to_vec()))
                    .collect(),
                fetches: Arc::new(AtomicUsize::new(0)),
            }
        }
        fn count(&self) -> usize {
            self.fetches.load(Ordering::SeqCst)
        }
    }
    impl BlobSource for Mem {
        fn open(&self, id: &BlobId) -> std::io::Result<Box<dyn Read + Send>> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            match self.blobs.get(id) {
                Some(b) => Ok(Box::new(Cursor::new(b.clone()))),
                None => Err(std::io::Error::new(std::io::ErrorKind::NotFound, "no blob")),
            }
        }
    }

    fn store_in(td: &tempfile::TempDir, limits: WorkspaceLimits) -> WorkspaceStore {
        WorkspaceStore::open(Some(td.path().join("ws")), limits)
            .unwrap()
            .with_clock(tick)
    }

    fn ready(td: &tempfile::TempDir) -> (WorkspaceStore, WorkspaceGuard) {
        let s = store_in(td, WorkspaceLimits::default());
        let g = s.acquire("proj", Duration::ZERO).unwrap();
        (s, g)
    }

    fn read(g: &WorkspaceGuard, rel: &str) -> String {
        fs::read_to_string(g.tree_dir().join(rel)).unwrap()
    }

    #[test]
    fn first_sync_writes_every_file_and_marks_ready() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        let f: Files = &[
            ("Cargo.toml", "[package]", false),
            ("src/a/b.rs", "fn b(){}", false),
        ];
        let src = Mem::new(f);
        let r = g.sync(&manifest(f), &src).unwrap();
        assert_eq!((r.written, r.unchanged, r.deleted), (2, 0, 0));
        assert_eq!(read(&g, "src/a/b.rs"), "fn b(){}");
        assert!(g.is_ready());
        assert_eq!(g.state().unwrap().state, SyncState::Ready);
    }

    #[test]
    fn incremental_sync_fetches_only_changed_blobs_and_hashes_nothing_when_unchanged() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        let v1: Files = &[("a.rs", "one", false), ("b.rs", "two", false)];
        g.sync(&manifest(v1), &Mem::new(v1)).unwrap();

        let src = Mem::new(v1);
        let r = g.sync(&manifest(v1), &src).unwrap();
        assert_eq!((r.written, r.hashed, r.unchanged), (0, 0, 2));
        assert_eq!(src.count(), 0);

        let v2: Files = &[("a.rs", "one", false), ("b.rs", "TWO", false)];
        let src = Mem::new(v2);
        let r = g.sync(&manifest(v2), &src).unwrap();
        assert_eq!(r.written, 1);
        assert_eq!(src.count(), 1, "only the changed blob is fetched");
        assert_eq!(read(&g, "b.rs"), "TWO");
    }

    #[test]
    fn same_size_change_is_rewritten() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        let v1: Files = &[("t.rs", "assert_eq!(x, 1)", false)];
        let v2: Files = &[("t.rs", "assert_eq!(x, 2)", false)];
        g.sync(&manifest(v1), &Mem::new(v1)).unwrap();
        g.sync(&manifest(v2), &Mem::new(v2)).unwrap();
        assert_eq!(read(&g, "t.rs"), "assert_eq!(x, 2)");
    }

    #[test]
    fn a_task_edit_to_a_tracked_file_is_restored() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        let v: Files = &[("a.rs", "good", false)];
        g.sync(&manifest(v), &Mem::new(v)).unwrap();
        fs::write(g.tree_dir().join("a.rs"), "evil!").unwrap();
        let r = g.sync(&manifest(v), &Mem::new(v)).unwrap();
        assert_eq!(r.written, 1);
        assert_eq!(read(&g, "a.rs"), "good");
    }

    #[test]
    fn deleted_files_go_and_untracked_build_output_stays() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        let v1: Files = &[
            ("keep.rs", "k", false),
            ("old/x.rs", "x", false),
            ("old/y/z.rs", "z", false),
        ];
        g.sync(&manifest(v1), &Mem::new(v1)).unwrap();
        fs::create_dir_all(g.tree_dir().join("target/debug")).unwrap();
        fs::write(g.tree_dir().join("target/debug/bin"), "obj").unwrap();
        // Untracked file next to tracked ones keeps its directory alive.
        fs::write(g.tree_dir().join("old/y/cache.bin"), "c").unwrap();

        let v2: Files = &[("keep.rs", "k", false)];
        let r = g.sync(&manifest(v2), &Mem::new(v2)).unwrap();
        assert_eq!(r.deleted, 2);
        assert!(!g.tree_dir().join("old/x.rs").exists());
        assert!(!g.tree_dir().join("old/y/z.rs").exists());
        assert!(!g.tree_dir().join("old").join("x.rs").exists());
        assert!(g.tree_dir().join("old/y/cache.bin").exists());
        assert_eq!(read(&g, "target/debug/bin"), "obj");
        // A directory left empty is removed.
        let v3: Files = &[("keep.rs", "k", false)];
        fs::remove_file(g.tree_dir().join("old/y/cache.bin")).unwrap();
        g.sync(&manifest(v3), &Mem::new(v3)).unwrap();
        // old/ and old/y were not tracked any more, so nothing removes them; they are empty dirs.
        assert!(g.tree_dir().join("target").is_dir());
    }

    #[test]
    fn dirs_emptied_by_a_delete_are_removed() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        let v1: Files = &[("keep.rs", "k", false), ("d/e/f.rs", "f", false)];
        g.sync(&manifest(v1), &Mem::new(v1)).unwrap();
        let v2: Files = &[("keep.rs", "k", false)];
        g.sync(&manifest(v2), &Mem::new(v2)).unwrap();
        assert!(!g.tree_dir().join("d").exists());
    }

    #[test]
    fn a_tracked_path_over_an_untracked_file_replaces_it() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        fs::create_dir_all(g.tree_dir()).unwrap();
        fs::write(g.tree_dir().join("gen.rs"), "local").unwrap();
        let v: Files = &[("gen.rs", "from client", false)];
        g.sync(&manifest(v), &Mem::new(v)).unwrap();
        assert_eq!(read(&g, "gen.rs"), "from client");
    }

    #[test]
    fn file_and_directory_swap_places() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        let v1: Files = &[("a", "file", false)];
        let v2: Files = &[("a/b", "inner", false)];
        let v3: Files = &[("a", "file again", false)];
        g.sync(&manifest(v1), &Mem::new(v1)).unwrap();
        g.sync(&manifest(v2), &Mem::new(v2)).unwrap();
        assert_eq!(read(&g, "a/b"), "inner");
        g.sync(&manifest(v3), &Mem::new(v3)).unwrap();
        assert_eq!(read(&g, "a"), "file again");
    }

    #[test]
    fn executable_bit_follows_the_manifest() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        let v1: Files = &[("run.sh", "#!/bin/sh", true), ("plain", "p", false)];
        g.sync(&manifest(v1), &Mem::new(v1)).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = |p: &str| {
                fs::metadata(g.tree_dir().join(p))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777
            };
            assert_eq!(mode("run.sh"), 0o755);
            assert_eq!(mode("plain"), 0o644);
            // Same content, bit flipped: fixed without a fetch.
            let v2: Files = &[("run.sh", "#!/bin/sh", false), ("plain", "p", true)];
            let src = Mem::new(v2);
            g.sync(&manifest(v2), &src).unwrap();
            assert_eq!(src.count(), 0);
            assert_eq!(mode("run.sh"), 0o644);
            assert_eq!(mode("plain"), 0o755);
        }
    }

    #[test]
    fn traversal_and_absolute_paths_never_reach_the_tree() {
        // A TreeManifest refuses them; checked_path is the second line of defence.
        for bad in [
            "../x",
            "a/../../x",
            "/etc/passwd",
            "a\\b",
            "C:/x",
            "a/./b",
            "",
        ] {
            assert!(
                TreeManifest::new(vec![BlobFile {
                    path: bad.to_string(),
                    id: BlobId::of(b"x"),
                    size: 1,
                    executable: false
                }])
                .is_err(),
                "{bad:?}"
            );
            assert!(checked_path(Path::new("/t"), bad).is_err(), "{bad:?}");
        }
        assert!(checked_path(Path::new("/t"), "a/b.rs").is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn a_symlinked_parent_is_refused_and_nothing_is_written_through_it() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        let outside = td.path().join("outside");
        fs::create_dir(&outside).unwrap();
        fs::create_dir_all(g.tree_dir()).unwrap();
        std::os::unix::fs::symlink(&outside, g.tree_dir().join("src")).unwrap();
        let v: Files = &[("src/evil.rs", "x", false)];
        let err = g.sync(&manifest(v), &Mem::new(v)).unwrap_err();
        assert!(err.to_string().contains("src/evil.rs"), "{err}");
        assert!(fs::read_dir(&outside).unwrap().next().is_none());
        assert_eq!(g.state().unwrap().state, SyncState::Syncing);
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_or_directory_at_a_manifest_path_fails_naming_the_path() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        let outside = td.path().join("secret.txt");
        fs::write(&outside, "keep").unwrap();
        fs::create_dir_all(g.tree_dir()).unwrap();
        std::os::unix::fs::symlink(&outside, g.tree_dir().join("lnk")).unwrap();
        let v: Files = &[("lnk", "new", false)];
        let err = g.sync(&manifest(v), &Mem::new(v)).unwrap_err();
        assert!(err.to_string().contains("lnk"), "{err}");
        assert_eq!(fs::read_to_string(&outside).unwrap(), "keep");

        fs::remove_file(g.tree_dir().join("lnk")).unwrap();
        fs::create_dir(g.tree_dir().join("lnk")).unwrap();
        let err = g.sync(&manifest(v), &Mem::new(v)).unwrap_err();
        assert!(err.to_string().contains("lnk"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn deleting_never_follows_a_link() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        let v1: Files = &[("d/f.rs", "f", false), ("keep", "k", false)];
        g.sync(&manifest(v1), &Mem::new(v1)).unwrap();
        // The task replaced the tracked directory with a link to a directory that holds a file of the same name.
        let outside = td.path().join("outside");
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("f.rs"), "precious").unwrap();
        fs::remove_dir_all(g.tree_dir().join("d")).unwrap();
        std::os::unix::fs::symlink(&outside, g.tree_dir().join("d")).unwrap();
        let v2: Files = &[("keep", "k", false)];
        g.sync(&manifest(v2), &Mem::new(v2)).unwrap();
        assert_eq!(
            fs::read_to_string(outside.join("f.rs")).unwrap(),
            "precious"
        );
    }

    #[test]
    fn a_failed_sync_leaves_syncing_and_the_next_sync_removes_what_it_wrote() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        let v1: Files = &[("a.rs", "a", false)];
        g.sync(&manifest(v1), &Mem::new(v1)).unwrap();

        // Manifest adds tests/new.rs and z.rs; the source lacks z.rs's blob, so the sync fails
        // after tests/new.rs was written.
        let v2: Files = &[
            ("a.rs", "a", false),
            ("tests/new.rs", "t", false),
            ("z.rs", "z", false),
        ];
        let partial: Files = &[("a.rs", "a", false), ("tests/new.rs", "t", false)];
        let err = g.sync(&manifest(v2), &Mem::new(partial)).unwrap_err();
        assert!(matches!(err, WorkspaceError::Fetch { .. }), "{err}");
        let st = g.state().unwrap();
        assert_eq!(st.state, SyncState::Syncing);
        assert!(g.tree_dir().join("tests/new.rs").exists());
        assert!(!g.is_ready());

        // The client deleted tests/new.rs meanwhile; the next manifest lacks it.
        let v3: Files = &[("a.rs", "a", false)];
        let r = g.sync(&manifest(v3), &Mem::new(v3)).unwrap();
        assert!(!g.tree_dir().join("tests/new.rs").exists());
        assert!(!g.tree_dir().join("tests").exists());
        assert!(r.deleted >= 1);
        assert!(g.is_ready());
        assert!(g.state().unwrap().pending.is_empty());
    }

    #[test]
    fn recovery_from_syncing_hashes_every_file() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        let v: Files = &[("a.rs", "aaa", false), ("b.rs", "bbb", false)];
        g.sync(&manifest(v), &Mem::new(v)).unwrap();
        // A crash: state says Syncing and the file was damaged keeping size and mtime.
        let mut st = g.state().unwrap();
        st.state = SyncState::Syncing;
        g.write_state(&st).unwrap();
        let p = g.tree_dir().join("a.rs");
        let m = fs::metadata(&p).unwrap().modified().unwrap();
        fs::write(&p, "XXX").unwrap();
        File::options()
            .write(true)
            .open(&p)
            .unwrap()
            .set_modified(m)
            .unwrap();
        let r = g.sync(&manifest(v), &Mem::new(v)).unwrap();
        assert_eq!(r.hashed, 2);
        assert_eq!(read(&g, "a.rs"), "aaa");
    }

    #[test]
    fn a_corrupt_state_file_is_treated_as_syncing() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        fs::write(g.dir.join(STATE_FILE), "{not json").unwrap();
        assert!(g.state().is_none());
        let v: Files = &[("a.rs", "a", false)];
        g.sync(&manifest(v), &Mem::new(v)).unwrap();
        assert!(g.is_ready());
    }

    #[test]
    fn a_blob_that_does_not_match_its_id_is_refused() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        let claimed: Files = &[("a.rs", "good", false)];
        let mut src = Mem::new(claimed);
        for b in src.blobs.values_mut() {
            *b = b"evil".to_vec();
        }
        let err = g.sync(&manifest(claimed), &src).unwrap_err();
        assert!(matches!(err, WorkspaceError::Integrity { .. }), "{err}");
        assert!(!g.tree_dir().join("a.rs").exists());
        assert_eq!(
            fs::read_dir(g.tmp_dir()).unwrap().count(),
            0,
            "no temp file left"
        );
    }

    #[test]
    fn a_task_without_a_source_can_use_a_ready_workspace_only() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        assert!(!g.is_ready());
        let v: Files = &[("a.rs", "a", false)];
        g.sync(&manifest(v), &Mem::new(v)).unwrap();
        assert!(g.is_ready());
    }

    #[test]
    fn tmp_is_emptied_before_each_task() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        g.prepare_tmp().unwrap();
        fs::create_dir_all(g.tmp_dir().join("sub")).unwrap();
        fs::write(g.tmp_dir().join("sub/x"), "x").unwrap();
        fs::write(g.tmp_dir().join("y"), "y").unwrap();
        g.prepare_tmp().unwrap();
        assert_eq!(fs::read_dir(g.tmp_dir()).unwrap().count(), 0);
    }

    #[test]
    fn task_env_names_the_workspace_and_tree() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        let env = g.task_env();
        assert_eq!(env[0], (ENV_WORKSPACE, "proj".to_string()));
        assert_eq!(env[1].0, ENV_WORKSPACE_DIR);
        assert!(env[1].1.ends_with("tree"));
    }

    #[test]
    fn a_workspace_is_held_by_one_task_at_a_time() {
        let td = tempfile::tempdir().unwrap();
        let s = store_in(&td, WorkspaceLimits::default());
        let g = s.acquire("proj", Duration::ZERO).unwrap();
        assert!(matches!(
            s.acquire("proj", Duration::ZERO),
            Err(WorkspaceError::Busy(_))
        ));
        // A second store on the same root (another worker) meets the file lock too.
        let s2 = store_in(&td, WorkspaceLimits::default());
        assert!(matches!(
            s2.acquire("proj", Duration::from_millis(40)),
            Err(WorkspaceError::Busy(_))
        ));
        let f = OpenOptions::new()
            .write(true)
            .open(g.dir.join("lock"))
            .unwrap();
        assert!(matches!(f.try_lock(), Err(fs::TryLockError::WouldBlock)));
        drop(f);
        s.acquire("other", Duration::ZERO).unwrap();
        drop(g);
        s2.acquire("proj", Duration::ZERO).unwrap();
    }

    #[test]
    fn acquire_waits_for_a_release() {
        let td = tempfile::tempdir().unwrap();
        let s = store_in(&td, WorkspaceLimits::default());
        let g = s.acquire("proj", Duration::ZERO).unwrap();
        let s2 = s.clone();
        let t = std::thread::spawn(move || s2.acquire("proj", Duration::from_secs(5)).map(|_| ()));
        std::thread::sleep(Duration::from_millis(100));
        drop(g);
        t.join().unwrap().unwrap();
    }

    #[test]
    fn bad_names_are_refused() {
        let td = tempfile::tempdir().unwrap();
        let s = store_in(&td, WorkspaceLimits::default());
        for n in ["Foo", "foo.", "con", "nul.txt", "../x", "a/b", "", "-x"] {
            assert!(
                matches!(
                    s.acquire(n, Duration::ZERO),
                    Err(WorkspaceError::InvalidName(_))
                ),
                "{n:?}"
            );
        }
    }

    fn small(td: &tempfile::TempDir, max: usize) -> WorkspaceStore {
        store_in(
            td,
            WorkspaceLimits {
                limit_mb: 1024,
                max_workspaces: max,
                measure_entry_cap: 1000,
            },
        )
    }

    fn use_ws(s: &WorkspaceStore, name: &str) -> FinishReport {
        let g = s.acquire(name, Duration::ZERO).unwrap();
        let v: Files = &[("a.rs", "a", false)];
        g.sync(&manifest(v), &Mem::new(v)).unwrap();
        g.finish().unwrap()
    }

    #[test]
    fn eviction_is_least_recently_used_first() {
        let td = tempfile::tempdir().unwrap();
        let s = small(&td, 2);
        use_ws(&s, "a");
        use_ws(&s, "b");
        use_ws(&s, "a"); // a is newer than b now
        let r = use_ws(&s, "c");
        assert_eq!(r.evicted, vec!["b".to_string()]);
        let mut names: Vec<_> = s.list().into_iter().map(|i| i.name).collect();
        names.sort();
        assert_eq!(names, ["a", "c"]);
    }

    #[test]
    fn a_locked_workspace_is_skipped_by_eviction() {
        let td = tempfile::tempdir().unwrap();
        let s = small(&td, 2);
        use_ws(&s, "a");
        use_ws(&s, "b");
        let held = s.acquire("a", Duration::ZERO).unwrap();
        let r = use_ws(&s, "c");
        assert_eq!(r.evicted, vec!["b".to_string()]);
        assert!(held.tree_dir().exists());
    }

    #[test]
    fn total_size_bounds_the_root() {
        let td = tempfile::tempdir().unwrap();
        let s = store_in(
            &td,
            WorkspaceLimits {
                limit_mb: 3,
                max_workspaces: 8,
                measure_entry_cap: 1000,
            },
        );
        for n in ["a", "b"] {
            let g = s.acquire(n, Duration::ZERO).unwrap();
            let v: Files = &[("x", "x", false)];
            g.sync(&manifest(v), &Mem::new(v)).unwrap();
            fs::create_dir_all(g.tree_dir().join("target")).unwrap();
            fs::write(g.tree_dir().join("target/o"), vec![0u8; 2 * 1024 * 1024]).unwrap();
            let r = g.finish().unwrap();
            assert!(r.warning.is_none());
            if n == "b" {
                assert_eq!(r.evicted, vec!["a".to_string()]);
            }
        }
    }

    #[test]
    fn a_workspace_above_the_limit_is_evicted_after_its_task_with_a_warning() {
        let td = tempfile::tempdir().unwrap();
        let s = store_in(
            &td,
            WorkspaceLimits {
                limit_mb: 1,
                max_workspaces: 8,
                measure_entry_cap: 1000,
            },
        );
        let g = s.acquire("big", Duration::ZERO).unwrap();
        let v: Files = &[("x", "x", false)];
        g.sync(&manifest(v), &Mem::new(v)).unwrap();
        fs::create_dir_all(g.tree_dir().join("target")).unwrap();
        fs::write(g.tree_dir().join("target/o"), vec![0u8; 2 * 1024 * 1024]).unwrap();
        let r = g.finish().unwrap();
        let w = r.warning.expect("warning");
        assert!(
            w.contains("big") && w.contains("2 MiB") && w.contains("1 MiB"),
            "{w}"
        );
        assert!(!g.tree_dir().exists());
        assert!(!g.is_ready());
        assert!(s.list().is_empty());
        // The next task starts cold and works.
        g.sync(&manifest(v), &Mem::new(v)).unwrap();
        assert_eq!(read(&g, "x"), "x");
    }

    #[test]
    fn the_entry_cap_counts_as_over_the_limit() {
        let td = tempfile::tempdir().unwrap();
        let s = store_in(
            &td,
            WorkspaceLimits {
                limit_mb: 1024,
                max_workspaces: 8,
                measure_entry_cap: 5,
            },
        );
        let g = s.acquire("many", Duration::ZERO).unwrap();
        let v: Files = &[("x", "x", false)];
        g.sync(&manifest(v), &Mem::new(v)).unwrap();
        for i in 0..20 {
            fs::write(g.tree_dir().join(format!("f{i}")), "1").unwrap();
        }
        let (_, complete) = measure_dir(&g.dir, 5);
        assert!(!complete);
        let r = g.finish().unwrap();
        assert!(r.warning.unwrap().contains("or more"));
    }

    #[test]
    fn measure_ignores_links() {
        let td = tempfile::tempdir().unwrap();
        let d = td.path().join("m");
        fs::create_dir(&d).unwrap();
        fs::write(d.join("a"), vec![0u8; 100]).unwrap();
        #[cfg(unix)]
        {
            let big = td.path().join("big");
            fs::write(&big, vec![0u8; 10_000]).unwrap();
            std::os::unix::fs::symlink(&big, d.join("l")).unwrap();
        }
        assert_eq!(measure_dir(&d, 100), (100, true));
    }

    #[test]
    fn default_root_follows_the_platform() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |k: &str| {
                pairs
                    .iter()
                    .find(|(n, _)| *n == k)
                    .map(|(_, v)| v.to_string())
            }
        };
        let tail = Path::new("oxideswarm").join("workspaces");
        assert_eq!(
            default_root_with(
                Platform::Linux,
                env(&[("XDG_CACHE_HOME", "/x/cache"), ("HOME", "/h")])
            ),
            Some(Path::new("/x").join("cache").join(&tail))
        );
        assert_eq!(
            default_root_with(Platform::Linux, env(&[("HOME", "/h")])),
            Some(Path::new("/h").join(".cache").join(&tail))
        );
        // A relative or empty XDG_CACHE_HOME is ignored.
        assert_eq!(
            default_root_with(
                Platform::Linux,
                env(&[("XDG_CACHE_HOME", "rel"), ("HOME", "/h")])
            ),
            Some(Path::new("/h").join(".cache").join(&tail))
        );
        assert_eq!(
            default_root_with(Platform::MacOs, env(&[("HOME", "/Users/u")])),
            Some(
                Path::new("/Users/u")
                    .join("Library")
                    .join("Caches")
                    .join(&tail)
            )
        );
        assert_eq!(
            default_root_with(
                Platform::Windows,
                env(&[("LOCALAPPDATA", "C:\\Users\\u\\AppData\\Local")])
            ),
            Some(Path::new("C:\\Users\\u\\AppData\\Local").join(&tail))
        );
        assert_eq!(
            default_root_with(Platform::Windows, env(&[("HOME", "/h")])),
            None
        );
        assert_eq!(default_root_with(Platform::Linux, env(&[])), None);
    }

    #[test]
    fn a_relative_root_is_refused() {
        assert!(matches!(
            WorkspaceStore::open(Some(PathBuf::from("rel/ws")), WorkspaceLimits::default()),
            Err(WorkspaceError::Root { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn root_checks_refuse_a_link_and_an_open_mode() {
        use std::os::unix::fs::PermissionsExt;
        let td = tempfile::tempdir().unwrap();
        let real = td.path().join("real");
        fs::create_dir(&real).unwrap();
        let link = td.path().join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert!(matches!(
            WorkspaceStore::open(Some(link), WorkspaceLimits::default()),
            Err(WorkspaceError::Root { .. })
        ));
        let open = td.path().join("open");
        fs::create_dir(&open).unwrap();
        fs::set_permissions(&open, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(matches!(
            WorkspaceStore::open(Some(open), WorkspaceLimits::default()),
            Err(WorkspaceError::Root { .. })
        ));
        // A fresh root is created private, and so is a workspace directory.
        let s = WorkspaceStore::open(Some(td.path().join("fresh")), WorkspaceLimits::default())
            .unwrap();
        let mode = |p: &Path| fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(s.root()), 0o700);
        let g = s.acquire("w", Duration::ZERO).unwrap();
        assert_eq!(mode(&g.dir), 0o700);
    }

    #[test]
    fn windows_device_names_and_trailing_dots_are_refused_as_paths_on_windows() {
        // On Windows `aux.rs` and `a.` cannot be created; elsewhere they are ordinary names.
        let r = checked_path(Path::new("C:\\t"), "src/aux.rs");
        let dot = checked_path(Path::new("C:\\t"), "src/a.");
        if cfg!(windows) {
            assert!(r.is_err());
            assert!(dot.is_err());
        } else {
            assert!(r.is_ok());
        }
        assert!(checked_path(Path::new("C:\\t"), "src\\a.rs").is_err());
        assert!(checked_path(Path::new("C:\\t"), "c:/x").is_err());
        assert!(checked_path(Path::new("C:\\t"), "a/b:stream").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn rv1_a_tmp_replaced_by_a_link_is_recreated_not_followed() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        let v1: Files = &[("a.rs", "aaa", false)];
        g.sync(&manifest(v1), &Mem::new(v1)).unwrap();
        let outside = td.path().join("outside");
        fs::create_dir(&outside).unwrap();
        fs::remove_dir_all(g.tmp_dir()).unwrap();
        std::os::unix::fs::symlink(&outside, g.tmp_dir()).unwrap();
        let v2: Files = &[("a.rs", "aaa", false), ("b.rs", "bbb", false)];
        g.sync(&manifest(v2), &Mem::new(v2)).unwrap();
        assert_eq!(
            fs::read_dir(&outside).unwrap().count(),
            0,
            "nothing written outside"
        );
        assert!(fs::symlink_metadata(g.tmp_dir()).unwrap().is_dir());
        assert_eq!(read(&g, "b.rs"), "bbb");
    }

    #[test]
    fn rv2_stale_temp_files_never_break_a_sync() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        let v1: Files = &[("a.rs", "aaa", false)];
        g.sync(&manifest(v1), &Mem::new(v1)).unwrap();
        for n in 0..3 {
            fs::write(
                g.tmp_dir()
                    .join(format!(".sync-{}-{n}", std::process::id())),
                "stale",
            )
            .unwrap();
        }
        let v2: Files = &[("a.rs", "aaa", false), ("b.rs", "bbb", false)];
        g.sync(&manifest(v2), &Mem::new(v2)).unwrap();
        assert_eq!(fs::read_dir(g.tmp_dir()).unwrap().count(), 0);
    }

    #[test]
    fn rv3_case_colliding_paths_are_refused_before_any_change() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        for v in [
            &[("A.rs", "upper", false), ("a.rs", "lower", false)][..],
            &[
                ("Src/x", "1", false),
                ("src/y", "2", false),
                ("SRC", "3", false),
            ][..],
            &[("\u{c9}.rs", "1", false), ("\u{e9}.rs", "2", false)][..],
        ] {
            let err = g.sync(&manifest(v), &Mem::new(v)).unwrap_err();
            assert!(matches!(err, WorkspaceError::Path { .. }), "{err}");
        }
        assert!(!g.tree_dir().exists(), "nothing was created");
        // Same case, different directories is fine.
        let ok: Files = &[("a/x", "1", false), ("A2/x", "2", false)];
        g.sync(&manifest(ok), &Mem::new(ok)).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn rv4_a_hard_linked_file_keeps_its_mode() {
        use std::os::unix::fs::PermissionsExt;
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        let v: Files = &[("f", "same", false)];
        g.sync(&manifest(v), &Mem::new(v)).unwrap();
        let outside = td.path().join("o");
        fs::write(&outside, "same").unwrap();
        fs::set_permissions(&outside, fs::Permissions::from_mode(0o600)).unwrap();
        fs::remove_file(g.tree_dir().join("f")).unwrap();
        fs::hard_link(&outside, g.tree_dir().join("f")).unwrap();
        g.sync(&manifest(v), &Mem::new(v)).unwrap();
        assert_eq!(
            fs::metadata(&outside).unwrap().permissions().mode() & 0o777,
            0o600
        );
        // Different content replaces the link, it does not write through it.
        fs::write(&outside, "DIFFERENT").unwrap();
        g.sync(&manifest(v), &Mem::new(v)).unwrap();
        assert_eq!(fs::read_to_string(&outside).unwrap(), "DIFFERENT");
        assert_eq!(read(&g, "f"), "same");
    }

    #[test]
    fn rv5_a_workspace_with_a_malformed_state_is_counted_and_evicted() {
        let td = tempfile::tempdir().unwrap();
        let s = small(&td, 1);
        let g = s.acquire("old", Duration::ZERO).unwrap();
        let v: Files = &[("a.rs", "a", false)];
        g.sync(&manifest(v), &Mem::new(v)).unwrap();
        fs::write(g.dir.join(STATE_FILE), "garbage").unwrap();
        let r = g.finish().unwrap();
        assert!(r.size_bytes > 0);
        assert_eq!(g.state().unwrap().state, SyncState::Syncing);
        drop(g);
        // Missing state.json altogether, content still there: listed and evictable.
        let g = s.acquire("lost", Duration::ZERO).unwrap();
        g.sync(&manifest(v), &Mem::new(v)).unwrap();
        fs::remove_file(g.dir.join(STATE_FILE)).unwrap();
        drop(g);
        assert_eq!(s.list().len(), 2);
        let r = use_ws(&s, "new");
        assert_eq!(r.evicted.len(), 2, "{r:?}");
        let dir_names: Vec<_> = fs::read_dir(s.root())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        for dead in ["old", "lost"] {
            let d = s.root().join(dead);
            let left: Vec<_> = fs::read_dir(&d)
                .map(|rd| rd.flatten().map(|e| e.file_name()).collect())
                .unwrap_or_default();
            assert!(
                left.iter().all(|n| n == "lock"),
                "{dead}: {left:?} {dir_names:?}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn rv6_eviction_never_follows_links_in_tree_or_tmp() {
        let td = tempfile::tempdir().unwrap();
        let s = small(&td, 1);
        let outside = td.path().join("out");
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("k"), "keep").unwrap();
        {
            let g = s.acquire("a", Duration::ZERO).unwrap();
            let v: Files = &[("x", "x", false)];
            g.sync(&manifest(v), &Mem::new(v)).unwrap();
            std::os::unix::fs::symlink(&outside, g.tree_dir().join("lnk")).unwrap();
            std::os::unix::fs::symlink(&outside, g.tmp_dir().join("lnk2")).unwrap();
            g.finish().unwrap();
        }
        let r = use_ws(&s, "b");
        assert_eq!(r.evicted, vec!["a".to_string()]);
        assert!(outside.join("k").exists());
    }

    #[cfg(unix)]
    #[test]
    fn rv7_a_symlinked_parent_is_neither_deleted_through_nor_written_through() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        let v1: Files = &[("sub/x", "x", false), ("k", "k", false)];
        g.sync(&manifest(v1), &Mem::new(v1)).unwrap();
        let victim = td.path().join("victim");
        fs::create_dir(&victim).unwrap();
        fs::write(victim.join("x"), "x").unwrap();
        fs::remove_dir_all(g.tree_dir().join("sub")).unwrap();
        std::os::unix::fs::symlink(&victim, g.tree_dir().join("sub")).unwrap();
        let v2: Files = &[("k", "k", false)];
        g.sync(&manifest(v2), &Mem::new(v2)).unwrap();
        assert!(victim.join("x").exists());
        assert!(g.tree_dir().join("sub").is_symlink());
        assert!(g.sync(&manifest(v1), &Mem::new(v1)).is_err());
        assert_eq!(fs::read_dir(&victim).unwrap().count(), 1);
    }

    #[test]
    fn w11_empty_directories_are_materialized_and_cleaned_up_when_dropped() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        let v1_files: Files = &[("src/main.rs", "fn main() {}", false)];
        let m1 = TreeManifest::with_entries(
            manifest(v1_files).into_files(),
            vec![],
            vec!["empty/nested".to_string(), "fixtures".to_string()],
        )
        .unwrap();
        g.sync(&m1, &Mem::new(v1_files)).unwrap();
        assert!(g.tree_dir().join("empty/nested").is_dir());
        assert!(g.tree_dir().join("fixtures").is_dir());

        // Dropping "empty/nested" removes both "empty/nested" and "empty".
        let m2 = TreeManifest::with_entries(
            manifest(v1_files).into_files(),
            vec![],
            vec!["fixtures".to_string()],
        )
        .unwrap();
        let r = g.sync(&m2, &Mem::new(v1_files)).unwrap();
        assert!(r.deleted >= 1);
        assert!(!g.tree_dir().join("empty").exists());
        assert!(g.tree_dir().join("fixtures").is_dir());
    }

    #[test]
    fn w11_in_tree_symlinks_arrive_as_links_and_update_or_delete_cleanly() {
        let td = tempfile::tempdir().unwrap();
        let probe_orig = td.path().join("probe_orig");
        let probe_link = td.path().join("probe_link");
        fs::write(&probe_orig, "probe").unwrap();
        if !crate::sandbox::try_create_test_symlink_file(&probe_orig, &probe_link) {
            return;
        }
        let _ = remove_symlink(&probe_link);

        let (_s, g) = ready(&td);
        let v1_files: Files = &[
            ("scripts/mode.sh", "#!/bin/sh\necho ok\n", true),
            ("scripts/alt.sh", "#!/bin/sh\necho alt\n", true),
        ];
        let m1 = TreeManifest::with_entries(
            manifest(v1_files).into_files(),
            vec![BlobSymlink {
                path: "ox-mode".to_string(),
                target: "scripts/mode.sh".to_string(),
            }],
            vec![],
        )
        .unwrap();
        let r1 = g.sync(&m1, &Mem::new(v1_files)).unwrap();
        assert_eq!(r1.written, 3);
        let link_meta = fs::symlink_metadata(g.tree_dir().join("ox-mode")).unwrap();
        assert!(is_link_like(&link_meta), "ox-mode must arrive as a symlink");
        let link_target = fs::read_link(g.tree_dir().join("ox-mode"))
            .unwrap()
            .to_str()
            .unwrap()
            .replace('\\', "/");
        assert_eq!(link_target, "scripts/mode.sh");
        assert_eq!(read(&g, "ox-mode"), "#!/bin/sh\necho ok\n");

        // Second sync with unchanged symlink keeps it without rewriting.
        let r2 = g.sync(&m1, &Mem::new(v1_files)).unwrap();
        assert_eq!((r2.written, r2.unchanged), (0, 3));

        // Updating the symlink target replaces the link.
        let m2 = TreeManifest::with_entries(
            manifest(v1_files).into_files(),
            vec![BlobSymlink {
                path: "ox-mode".to_string(),
                target: "scripts/alt.sh".to_string(),
            }],
            vec![],
        )
        .unwrap();
        let r3 = g.sync(&m2, &Mem::new(v1_files)).unwrap();
        assert_eq!(r3.written, 1);
        assert_eq!(read(&g, "ox-mode"), "#!/bin/sh\necho alt\n");

        // Dropping the symlink removes it from the tree.
        let m3 = manifest(v1_files);
        let r4 = g.sync(&m3, &Mem::new(v1_files)).unwrap();
        assert_eq!(r4.deleted, 1);
        assert!(fs::symlink_metadata(g.tree_dir().join("ox-mode")).is_err());
    }

    #[test]
    fn w11_windows_error_1314_skips_symlinks_with_developer_mode_warning() {
        struct Force1314Guard;
        impl Drop for Force1314Guard {
            fn drop(&mut self) {
                FORCE_SYMLINK_1314.with(|c| c.set(false));
            }
        }
        FORCE_SYMLINK_1314.with(|c| c.set(true));
        let _guard = Force1314Guard;

        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        let v1_files: Files = &[("src/main.rs", "fn main() {}", false)];
        let m1 = TreeManifest::with_entries(
            manifest(v1_files).into_files(),
            vec![
                BlobSymlink {
                    path: "alias1".to_string(),
                    target: "src/main.rs".to_string(),
                },
                BlobSymlink {
                    path: "alias2".to_string(),
                    target: "src".to_string(),
                },
            ],
            vec!["empty_dir".to_string()],
        )
        .unwrap();
        let r = g
            .sync(&m1, &Mem::new(v1_files))
            .expect("sync must succeed with a warning when error 1314 occurs");
        assert_eq!(r.written, 1);
        assert_eq!(r.skipped_symlinks, 2);
        let warning = r.warning.expect("expected warning on skipped symlinks");
        assert!(
            warning.contains("1314") && warning.contains("Developer Mode"),
            "unexpected warning text: {warning}"
        );
        assert_eq!(read(&g, "src/main.rs"), "fn main() {}");
        assert!(g.tree_dir().join("empty_dir").is_dir());
        assert!(!g.tree_dir().join("alias1").exists());
        assert!(!g.tree_dir().join("alias2").exists());
        let st = g.state().unwrap();
        assert_eq!(st.state, SyncState::Ready);
        assert!(st.symlinks.is_empty());
    }

    #[test]
    fn w11_untracked_file_at_new_symlink_path_is_refused_and_preserved() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        let v1_files: Files = &[("a.rs", "aaa", false)];
        g.sync(&manifest(v1_files), &Mem::new(v1_files)).unwrap();

        // A build writes an untracked file `out.bin` in `tree/`.
        fs::write(g.tree_dir().join("out.bin"), "build-artifact").unwrap();

        // Next sync introduces a symlink at `out.bin -> a.rs`. Because `out.bin` was not a tracked
        // symlink, `sync` must refuse with `WorkspaceError::Path` and leave `out.bin` untouched.
        let m2 = TreeManifest::with_entries(
            manifest(v1_files).into_files(),
            vec![BlobSymlink {
                path: "out.bin".to_string(),
                target: "a.rs".to_string(),
            }],
            vec![],
        )
        .unwrap();
        let err = g.sync(&m2, &Mem::new(v1_files)).unwrap_err();
        assert!(
            matches!(err, WorkspaceError::Path { ref path, .. } if path == "out.bin"),
            "expected WorkspaceError::Path for untracked file at symlink path, got: {err}"
        );
        assert_eq!(read(&g, "out.bin"), "build-artifact");
    }

    #[test]
    fn w11_unicode_nfc_nfd_colliding_paths_are_refused_before_any_change() {
        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        // `caf\u{00e9}.rs` (NFC) vs `cafe\u{0301}.rs` (NFD)
        let nfc_nfd: Files = &[
            ("caf\u{00e9}.rs", "nfc", false),
            ("cafe\u{0301}.rs", "nfd", false),
        ];
        let err = g.sync(&manifest(nfc_nfd), &Mem::new(nfc_nfd)).unwrap_err();
        assert!(matches!(err, WorkspaceError::Path { .. }), "{err}");
        assert!(!g.tree_dir().exists(), "nothing was created");
    }

    #[test]
    fn w11_symlink_target_changing_between_file_and_dir_recreates_link() {
        let td = tempfile::tempdir().unwrap();
        let probe_orig = td.path().join("probe_orig");
        let probe_link = td.path().join("probe_link");
        fs::write(&probe_orig, "probe").unwrap();
        if !crate::sandbox::try_create_test_symlink_file(&probe_orig, &probe_link) {
            return;
        }
        let _ = remove_symlink(&probe_link);

        let (_s, g) = ready(&td);
        // Sync 1: `item` is a regular file, `alias -> item`.
        let v1_files: Files = &[("item", "file-body", false)];
        let m1 = TreeManifest::with_entries(
            manifest(v1_files).into_files(),
            vec![BlobSymlink {
                path: "alias".to_string(),
                target: "item".to_string(),
            }],
            vec![],
        )
        .unwrap();
        g.sync(&m1, &Mem::new(v1_files)).unwrap();
        assert_eq!(read(&g, "alias"), "file-body");

        // Sync 2: `item/sub.txt` is a file inside directory `item`, and `alias -> item` changes
        // target kind from regular file to directory without changing its target string.
        let v2_files: Files = &[("item/sub.txt", "inside-dir", false)];
        let m2 = TreeManifest::with_entries(
            manifest(v2_files).into_files(),
            vec![BlobSymlink {
                path: "alias".to_string(),
                target: "item".to_string(),
            }],
            vec![],
        )
        .unwrap();
        g.sync(&m2, &Mem::new(v2_files)).unwrap();
        let meta = fs::symlink_metadata(g.tree_dir().join("alias")).unwrap();
        assert!(symlink_kind_matches(&meta, true));
        assert_eq!(read(&g, "alias/sub.txt"), "inside-dir");

        // Sync 3: `ITEM/sub.txt` differs in case from `alias_folded -> item`; `target_is_dir`
        // folds case when choosing the symlink kind on Windows, and resolves through
        // `alias_folded/sub.txt` on case-insensitive filesystems.
        let v3_files: Files = &[("ITEM/sub.txt", "inside-dir-folded", false)];
        let m3 = TreeManifest::with_entries(
            manifest(v3_files).into_files(),
            vec![BlobSymlink {
                path: "alias_folded".to_string(),
                target: "item".to_string(),
            }],
            vec![],
        )
        .unwrap();
        g.sync(&m3, &Mem::new(v3_files)).unwrap();
        let meta = fs::symlink_metadata(g.tree_dir().join("alias_folded")).unwrap();
        assert!(symlink_kind_matches(&meta, true));
        if g.tree_dir().join("item").exists() {
            assert_eq!(read(&g, "alias_folded/sub.txt"), "inside-dir-folded");
        }
    }

    #[test]
    fn w11_symlinked_parent_left_by_task_refuses_links_and_empty_dirs_and_skips_deletes() {
        let td = tempfile::tempdir().unwrap();
        let probe_orig = td.path().join("probe_orig");
        let probe_link = td.path().join("probe_link");
        fs::create_dir(&probe_orig).unwrap();
        if !crate::sandbox::try_create_test_symlink_dir(&probe_orig, &probe_link) {
            return;
        }
        let _ = remove_symlink(&probe_link);

        let (_s, g) = ready(&td);
        let v1_files: Files = &[("k", "keep", false)];
        let m_with_link = TreeManifest::with_entries(
            manifest(v1_files).into_files(),
            vec![BlobSymlink {
                path: "sub/alias".to_string(),
                target: "../k".to_string(),
            }],
            vec![],
        )
        .unwrap();
        let m_with_empty = TreeManifest::with_entries(
            manifest(v1_files).into_files(),
            vec![],
            vec!["sub/empty".to_string()],
        )
        .unwrap();
        let m_both = TreeManifest::with_entries(
            manifest(v1_files).into_files(),
            vec![BlobSymlink {
                path: "sub/alias".to_string(),
                target: "../k".to_string(),
            }],
            vec!["sub/empty".to_string()],
        )
        .unwrap();
        g.sync(&m_both, &Mem::new(v1_files)).unwrap();

        // A task replaces `tree/sub` with a symlink to `victim` outside `tree/`.
        let victim = td.path().join("victim");
        fs::create_dir_all(victim.join("empty")).unwrap();
        assert!(crate::sandbox::try_create_test_symlink_file(
            Path::new("../k"),
            &victim.join("alias")
        ));
        fs::remove_dir_all(g.tree_dir().join("sub")).unwrap();
        assert!(crate::sandbox::try_create_test_symlink_dir(
            &victim,
            &g.tree_dir().join("sub")
        ));

        // Dropping `sub/alias` and `sub/empty` must NOT delete inside `victim` through `tree/sub`.
        let m_only_k = manifest(v1_files);
        g.sync(&m_only_k, &Mem::new(v1_files)).unwrap();
        assert!(fs::symlink_metadata(victim.join("alias")).is_ok());
        assert!(victim.join("empty").is_dir());
        assert!(is_link_like(
            &fs::symlink_metadata(g.tree_dir().join("sub")).unwrap()
        ));

        // Placing a symlink or empty directory under `tree/sub` must fail without touching `victim`.
        let _ = remove_symlink(&victim.join("alias"));
        fs::remove_dir(victim.join("empty")).unwrap();
        assert!(g.sync(&m_with_link, &Mem::new(v1_files)).is_err());
        assert!(fs::symlink_metadata(victim.join("alias")).is_err());
        assert!(g.sync(&m_with_empty, &Mem::new(v1_files)).is_err());
        assert!(!victim.join("empty").exists());
    }

    #[test]
    fn w11_switching_entry_between_directory_symlink_and_file_across_syncs() {
        let td = tempfile::tempdir().unwrap();
        let probe_orig = td.path().join("probe_orig");
        let probe_link = td.path().join("probe_link");
        fs::write(&probe_orig, "probe").unwrap();
        if !crate::sandbox::try_create_test_symlink_file(&probe_orig, &probe_link) {
            return;
        }
        let _ = remove_symlink(&probe_link);

        let (_s, g) = ready(&td);
        // Sync 1: `d` is a non-empty directory (`d/inside.txt`), `empty_d` is an empty directory,
        // and `target.txt` is a regular file.
        let v1_files: Files = &[
            ("d/inside.txt", "in-d", false),
            ("target.txt", "target-body", false),
        ];
        let m1 = TreeManifest::with_entries(
            manifest(v1_files).into_files(),
            vec![],
            vec!["empty_d".to_string()],
        )
        .unwrap();
        g.sync(&m1, &Mem::new(v1_files)).unwrap();
        assert!(g.tree_dir().join("d").is_dir());
        assert!(g.tree_dir().join("empty_d").is_dir());

        // Sync 2: `d` and `empty_d` both switch from directories to symlinks pointing to `target.txt`.
        let v2_files: Files = &[("target.txt", "target-body", false)];
        let m2 = TreeManifest::with_entries(
            manifest(v2_files).into_files(),
            vec![
                BlobSymlink {
                    path: "d".to_string(),
                    target: "target.txt".to_string(),
                },
                BlobSymlink {
                    path: "empty_d".to_string(),
                    target: "target.txt".to_string(),
                },
            ],
            vec![],
        )
        .unwrap();
        g.sync(&m2, &Mem::new(v2_files)).unwrap();
        assert!(is_link_like(
            &fs::symlink_metadata(g.tree_dir().join("d")).unwrap()
        ));
        assert!(is_link_like(
            &fs::symlink_metadata(g.tree_dir().join("empty_d")).unwrap()
        ));
        assert_eq!(read(&g, "d"), "target-body");
        assert_eq!(read(&g, "empty_d"), "target-body");

        // Sync 3: `d` switches from a symlink to a regular file, and `empty_d` switches from a
        // symlink back to an empty directory.
        let v3_files: Files = &[
            ("d", "regular-d", false),
            ("target.txt", "target-body", false),
        ];
        let m3 = TreeManifest::with_entries(
            manifest(v3_files).into_files(),
            vec![],
            vec!["empty_d".to_string()],
        )
        .unwrap();
        g.sync(&m3, &Mem::new(v3_files)).unwrap();
        let d_meta = fs::symlink_metadata(g.tree_dir().join("d")).unwrap();
        assert!(d_meta.is_file() && !is_link_like(&d_meta));
        assert_eq!(read(&g, "d"), "regular-d");
        let empty_meta = fs::symlink_metadata(g.tree_dir().join("empty_d")).unwrap();
        assert!(empty_meta.is_dir() && !is_link_like(&empty_meta));
    }

    #[test]
    fn w11_case_folded_symlink_chain_and_task_leftover_link_traversal_are_refused() {
        // 1. In-manifest case-folded chains (`a/b/up -> ..` + `hop -> A/B/UP/../..`,
        //    `s/b/up -> ..` + `hop -> ſ/b/up/../..`, and deeper `s/b/c/up -> ../..` +
        //    `hop -> ſ/b/c/up/../../..`) are refused.
        for (up_path, up_target, hop_target) in [
            ("a/b/up", "..", "A/B/UP/../.."),
            ("s/b/up", "..", "\u{017f}/b/up/../.."),
            ("s/b/c/up", "../..", "\u{017f}/b/c/up/../../.."),
            ("ss/k/up", "..", "ẞ/\u{212a}/up/../.."),
        ] {
            let res = TreeManifest::with_entries(
                vec![],
                vec![
                    BlobSymlink {
                        path: up_path.to_string(),
                        target: up_target.to_string(),
                    },
                    BlobSymlink {
                        path: "hop".to_string(),
                        target: hop_target.to_string(),
                    },
                ],
                vec![],
            );
            assert!(
                res.is_err(),
                "case-folded chain ({up_path}, {hop_target}) in manifest must be refused"
            );
        }

        // 2. If a task leaves an untracked symlink `a/b/up -> ..` on disk in `tree/`, a later sync
        //    introducing `hop -> a/b/up/../..` (and `hop -> A/B/UP/../..` on case-insensitive
        //    filesystems) is refused before creating `hop`.
        let td = tempfile::tempdir().unwrap();
        let probe_orig = td.path().join("probe_orig");
        let probe_link = td.path().join("probe_link");
        fs::create_dir(&probe_orig).unwrap();
        if !crate::sandbox::try_create_test_symlink_dir(&probe_orig, &probe_link) {
            return;
        }
        let _ = remove_symlink(&probe_link);

        let (_s, g) = ready(&td);
        let v1_files: Files = &[("a/b/keep.txt", "ok", false)];
        g.sync(&manifest(v1_files), &Mem::new(v1_files)).unwrap();
        assert!(crate::sandbox::try_create_test_symlink_dir(
            Path::new(".."),
            &g.tree_dir().join("a/b/up")
        ));

        let m_exact = TreeManifest::with_entries(
            manifest(v1_files).into_files(),
            vec![BlobSymlink {
                path: "hop".to_string(),
                target: "a/b/up/../..".to_string(),
            }],
            vec![],
        )
        .unwrap();
        let err_exact = g.sync(&m_exact, &Mem::new(v1_files)).unwrap_err();
        assert!(
            matches!(err_exact, WorkspaceError::Path { ref path, .. } if path == "hop"),
            "expected WorkspaceError::Path for hop traversing on-disk link, got: {err_exact}"
        );
        assert!(fs::symlink_metadata(g.tree_dir().join("hop")).is_err());

        if g.tree_dir().join("A/B/UP").exists() {
            let m_folded = TreeManifest::with_entries(
                manifest(v1_files).into_files(),
                vec![BlobSymlink {
                    path: "hop".to_string(),
                    target: "A/B/UP/../..".to_string(),
                }],
                vec![],
            )
            .unwrap();
            let err_folded = g.sync(&m_folded, &Mem::new(v1_files)).unwrap_err();
            assert!(
                matches!(err_folded, WorkspaceError::Path { ref path, .. } if path == "hop"),
                "expected WorkspaceError::Path for case-folded hop traversing on-disk link, got: {err_folded}"
            );
            assert!(fs::symlink_metadata(g.tree_dir().join("hop")).is_err());
        }

        // 3. Order-independent post-creation verification & rollback in `materialize_symlinks`:
        //    even when `hop` is created BEFORE `s/b/up` (or deeper `s/b/c/up`), the post-creation
        //    walk catches `hop`, rolls back all links created in the pass, and fails the sync.
        let _ = remove_symlink(&g.tree_dir().join("a/b/up"));
        for (up_rel, up_target, hop_target, hop_resolved) in [
            ("s/b/up", "..", "s/b/up/../..", "s"),
            ("s/b/c/up", "../..", "s/b/c/up/../../..", "s"),
        ] {
            let l_hop = BlobSymlink {
                path: "hop".to_string(),
                target: hop_target.to_string(),
            };
            let l_up = BlobSymlink {
                path: up_rel.to_string(),
                target: up_target.to_string(),
            };
            let targets = vec![
                (&l_hop, g.tree_dir().join("hop"), hop_resolved.to_string()),
                (&l_up, g.tree_dir().join(up_rel), "s".to_string()),
            ];
            let mut report = SyncReport::default();
            let s_trie = FoldedPathTrie::from_manifest(
                &TreeManifest::with_entries(vec![], vec![], vec!["s".to_string()]).unwrap(),
            )
            .unwrap();
            let err = materialize_symlinks(
                &g.tree_dir(),
                "proj",
                &targets,
                &s_trie,
                &HashSet::new(),
                &mut report,
            )
            .unwrap_err();
            assert!(
                matches!(err, WorkspaceError::Path { ref path, .. } if path == "hop"),
                "expected post-creation failure on 'hop', got: {err}"
            );
            assert!(
                fs::symlink_metadata(g.tree_dir().join("hop")).is_err(),
                "created 'hop' link must be rolled back on failure"
            );
            assert!(
                fs::symlink_metadata(g.tree_dir().join(up_rel)).is_err(),
                "created '{up_rel}' link must be rolled back on failure"
            );
        }
    }

    #[test]
    fn w11_long_target_segment_walk_enametoolong_and_kept_symlink_rollback() {
        // 1. In-manifest 230-level deep pair with 'ẞ' (U+1E9E) and 'K' (U+212A) whose target
        //    exceeds 1,024 bytes (`PATH_MAX` on macOS) is refused by `TreeManifest` on every OS.
        let depth = 230usize;
        let k_chain = vec!["k"; depth].join("/");
        let kelvin_chain = vec!["\u{212a}"; depth].join("/");
        let up_path = format!("ss/{k_chain}/up");
        let up_target = vec![".."; depth].join("/");
        let hop_dots = vec![".."; depth + 1].join("/");
        let hop_target_unicode = format!("ẞ/{kelvin_chain}/up/{hop_dots}");
        assert!(
            hop_target_unicode.len() > 1024,
            "target byte length ({}) must exceed macOS PATH_MAX (1024)",
            hop_target_unicode.len()
        );
        let manifest_err = TreeManifest::with_entries(
            vec![],
            vec![
                BlobSymlink {
                    path: up_path.clone(),
                    target: up_target.clone(),
                },
                BlobSymlink {
                    path: "hop".to_string(),
                    target: hop_target_unicode.clone(),
                },
            ],
            vec![],
        )
        .unwrap_err();
        assert!(
            manifest_err.contains("traverses through symbolic link"),
            "{manifest_err}"
        );

        let td = tempfile::tempdir().unwrap();
        let (_s, g) = ready(&td);
        let base_files: Files = &[("s/b/keep.txt", "ok", false)];
        g.sync(&manifest(base_files), &Mem::new(base_files))
            .unwrap();

        // 2. Non-ENOENT metadata error (overlong 300-byte component -> ENAMETOOLONG /
        //    ERROR_FILENAME_EXCED_RANGE) must fail closed instead of being ignored as "not a link".
        let overlong_target = format!("{}/../s/b/keep.txt", "a".repeat(300));
        let err_overlong = ensure_target_walks_no_on_disk_symlink(
            &g.tree_dir(),
            "hop",
            &overlong_target,
            &HashSet::new(),
        )
        .unwrap_err();
        assert!(
            matches!(err_overlong, WorkspaceError::Path { ref path, .. } if path == "hop"),
            "overlong component error must fail closed on 'hop', got: {err_overlong}"
        );

        // 3. Segment-by-segment on-disk walk at depth 230 with target > 1,024 bytes:
        //    create `ss/k/k/.../k/up -> ../..` on disk and verify a > 1,024-byte target walking
        //    through `up` is caught even when the walked path exceeds `PATH_MAX` (1024).
        let deep_parent = g.tree_dir().join("ss").join(&k_chain);
        #[cfg(windows)]
        let deep_parent_create = verbatim_dir_path(&deep_parent);
        #[cfg(not(windows))]
        let deep_parent_create = deep_parent.clone();
        fs::create_dir_all(&deep_parent_create).unwrap();
        let deep_up = deep_parent.join("up");
        let deep_link_created = match create_in_tree_symlink("../..", &deep_up, true) {
            Ok(()) => true,
            #[cfg(windows)]
            Err(e) if e.raw_os_error() == Some(1314) => std::process::Command::new("cmd")
                .args(["/C", "mklink", "/J"])
                .arg(verbatim_dir_path(&deep_up))
                .arg(g.tree_dir().join("ss"))
                .output()
                .is_ok_and(|o| o.status.success()),
            Err(_) => false,
        };
        assert!(
            deep_link_created,
            "must create deep symlink or junction for 230-level walk test"
        );
        {
            let hop_target_ascii_long = format!("{}ss/{k_chain}/up/{hop_dots}", "./".repeat(300));
            assert!(hop_target_ascii_long.len() > 1024);
            let err_deep_ascii = ensure_target_walks_no_on_disk_symlink(
                &g.tree_dir(),
                "hop",
                &hop_target_ascii_long,
                &HashSet::new(),
            )
            .unwrap_err();
            assert!(
                matches!(err_deep_ascii, WorkspaceError::Path { ref path, ref reason }
                    if path == "hop" && reason.contains("traverses through symbolic link")),
                "expected deep on-disk symlink traversal error, got: {err_deep_ascii}"
            );

            // On macOS (APFS) or any filesystem that folds 'ẞ'/'ss' or 'K'/'k', verify the 3-byte
            // UTF-8 target (`> 1024` bytes without `./` padding) is also caught on disk.
            #[cfg(windows)]
            let kelvin_probe = verbatim_dir_path(&g.tree_dir().join("ss").join(&kelvin_chain));
            #[cfg(not(windows))]
            let kelvin_probe = g.tree_dir().join("ss").join(&kelvin_chain);
            if kelvin_probe.exists() {
                let hop_kelvin = format!("ss/{kelvin_chain}/up/{hop_dots}");
                assert!(hop_kelvin.len() > 1024);
                let err_kelvin = ensure_target_walks_no_on_disk_symlink(
                    &g.tree_dir(),
                    "hop",
                    &hop_kelvin,
                    &HashSet::new(),
                )
                .unwrap_err();
                assert!(
                    matches!(err_kelvin, WorkspaceError::Path { ref path, ref reason }
                        if path == "hop" && reason.contains("traverses through symbolic link")),
                    "expected Kelvin-folded deep symlink traversal error, got: {err_kelvin}"
                );
            }
            #[cfg(target_os = "macos")]
            if g.tree_dir().join("ẞ").exists() {
                let err_macos = ensure_target_walks_no_on_disk_symlink(
                    &g.tree_dir(),
                    "hop",
                    &hop_target_unicode,
                    &HashSet::new(),
                )
                .unwrap_err();
                assert!(
                    matches!(err_macos, WorkspaceError::Path { ref path, ref reason }
                        if path == "hop" && reason.contains("traverses through symbolic link")),
                    "expected macOS APFS sharp-S + Kelvin symlink traversal error, got: {err_macos}"
                );
            }
            let _ = remove_symlink(&deep_up);
        }

        let probe_orig = td.path().join("probe_orig");
        let probe_link = td.path().join("probe_link");
        fs::create_dir(&probe_orig).unwrap();
        if !crate::sandbox::try_create_test_symlink_dir(&probe_orig, &probe_link) {
            return;
        }
        let _ = remove_symlink(&probe_link);

        // 4. Item 7: if `hop` was kept from an earlier sync (`kept == true` in pass 1) and then
        //    fails the second on-disk pass in `materialize_symlinks` after `s/b/up` is created,
        //    `hop` must be removed from disk along with `s/b/up`.
        let hop_disk = g.tree_dir().join("hop");
        create_in_tree_symlink("s/b/up/../..", &hop_disk, true).unwrap();
        assert!(fs::symlink_metadata(&hop_disk).is_ok());

        let l_hop = BlobSymlink {
            path: "hop".to_string(),
            target: "s/b/up/../..".to_string(),
        };
        let l_up = BlobSymlink {
            path: "s/b/up".to_string(),
            target: "..".to_string(),
        };
        let targets = vec![
            (&l_hop, hop_disk.clone(), "s".to_string()),
            (&l_up, g.tree_dir().join("s/b/up"), "s".to_string()),
        ];
        let mut report = SyncReport::default();
        let s_trie = FoldedPathTrie::from_manifest(
            &TreeManifest::with_entries(vec![], vec![], vec!["s".to_string()]).unwrap(),
        )
        .unwrap();
        let err_kept = materialize_symlinks(
            &g.tree_dir(),
            "proj",
            &targets,
            &s_trie,
            &HashSet::from(["hop".to_string()]),
            &mut report,
        )
        .unwrap_err();
        assert!(
            matches!(err_kept, WorkspaceError::Path { ref path, .. } if path == "hop"),
            "expected second-pass failure on kept 'hop', got: {err_kept}"
        );
        assert!(
            fs::symlink_metadata(&hop_disk).is_err(),
            "kept 'hop' link that failed the second pass must be removed from disk"
        );
        assert!(
            fs::symlink_metadata(g.tree_dir().join("s/b/up")).is_err(),
            "created 's/b/up' link must be rolled back on failure"
        );
    }

    #[test]
    fn w11_folded_path_trie_handles_deep_unshared_paths_and_case_folded_dirs() {
        let tail_2040 = vec!["a"; 2_040].join("/");
        let dummy_id = BlobId::of(b"x");
        let deep_files: Vec<BlobFile> = (0..200)
            .map(|i| BlobFile {
                path: format!("f{i:03}/{tail_2040}"),
                id: dummy_id,
                size: 1,
                executable: false,
            })
            .collect();
        let m = TreeManifest::with_entries(deep_files, vec![], vec!["EmptyDir/ẞ_Sub".to_string()])
            .unwrap();
        let trie = check_case_collisions(&m).unwrap();
        assert!(trie.contains_dir("f000/a/a"));
        assert!(trie.contains_dir("F000/A/A"));
        assert!(trie.contains_dir("emptydir/ss_sub"));
        assert!(!trie.contains_dir(&format!("f000/{tail_2040}")));
    }
}
