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
    open_regular_file_nofollow, validate_blob_path, BlobFile, BlobHasher, BlobId, TreeManifest,
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

/// The content of `state.json` (D2, D3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceState {
    version: u32,
    pub state: SyncState,
    records: BTreeMap<String, FileRecord>,
    pending: BTreeSet<String>,
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

        check_case_collisions(manifest)?;
        // Check every path before any change, so a bad manifest touches nothing.
        let mut targets: Vec<(&BlobFile, PathBuf)> = Vec::with_capacity(manifest.files().len());
        for f in manifest.files() {
            targets.push((f, checked_path(&tree, &f.path)?));
        }
        let new_paths: BTreeSet<&str> = manifest.files().iter().map(|f| f.path.as_str()).collect();

        // 1. pending paths, Syncing, flush.
        for p in &new_paths {
            st.pending.insert((*p).to_string());
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

        // 4. delete tracked paths the manifest dropped (regular files only), then empty dirs.
        let stale: Vec<String> = st
            .records
            .keys()
            .chain(st.pending.iter())
            .filter(|p| !new_paths.contains(p.as_str()))
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let mut dirs: BTreeSet<PathBuf> = BTreeSet::new();
        for p in &stale {
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
        // Deepest first; remove_dir fails on a non-empty directory, which is the point.
        for d in dirs.iter().rev() {
            let _ = fs::remove_dir(d);
        }

        // 2 and 3. add or replace.
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

        // 5. records := new manifest, clear pending, Ready, flush.
        st.records = records;
        st.pending.clear();
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

/// Refuses a manifest whose paths differ only by case (`A.rs` and `a.rs`, or a file `A` next to
/// `a/x`): on the case-insensitive default file systems of macOS and Windows they are one file,
/// so the sync would leave one of the contents in place and rewrite it every time. Uses Unicode
/// lowercasing on every host. Residual: names that differ only by NFC/NFD normalisation (macOS
/// APFS treats them as equal) are not detected.
fn check_case_collisions(manifest: &TreeManifest) -> Result<(), WorkspaceError> {
    let mut files: HashSet<String> = HashSet::new();
    let mut dirs: HashSet<String> = HashSet::new();
    let clash = |path: &str, other: &str| WorkspaceError::Path {
        path: path.to_string(),
        reason: format!(
            "it differs from '{other}' only by case, which one file system entry cannot hold \
             on macOS and Windows"
        ),
    };
    for f in manifest.files() {
        let folded = f.path.to_lowercase();
        if dirs.contains(&folded) || !files.insert(folded.clone()) {
            return Err(clash(&f.path, &folded));
        }
        for (pos, _) in folded.match_indices('/') {
            let d = &folded[..pos];
            if files.contains(d) {
                return Err(clash(&f.path, d));
            }
            dirs.insert(d.to_string());
        }
    }
    Ok(())
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
}
