//! The worker's blob cache: task input and output files kept on disk by content
//! (`rusty_grid_core::blob`), so a file that did not change is fetched from the Master once, not
//! once per task. Design record: `docs/design/0006-content-addressed-data-plane.md`.
//!
//! * **Layout.** `<sandbox base>/cache/blobs/objects/<first two hex digits>/<64 hex digits>`, one
//!   verified blob per file, and `<sandbox base>/cache/blobs/tmp/` for blobs being written. Task
//!   sandboxes are siblings of `cache/`, so moving an output file into the cache is a rename.
//! * **Copies, not links.** A blob is copied into a sandbox ([`BlobCache::materialize`]): a task
//!   that rewrites one of its inputs in place must not change the blob the next task gets. The
//!   copy is hashed on the way; a blob that no longer matches its id is dropped and the task fails
//!   with a message saying so (the next task fetches it again).
//! * **Bound.** The cache holds at most `max_bytes` once [`BlobCache::evict_to_fit`] has run,
//!   dropping the least recently used blobs that no running task pins ([`BlobCache::pin`]). The
//!   worker calls it after every task, never while a task's inputs or outputs are in flight.
//!
//! Several worker processes that share one sandbox base directory share the cache. That works,
//! but one process may evict a blob another one is about to use; give each worker its own
//! `--sandbox-base-dir` to avoid it.

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use tracing::{debug, info, warn};
use uuid::Uuid;

use rusty_grid_core::blob::{hash_file, BlobHasher, BlobId};
use rusty_grid_core::task::TaskId;

/// Default bound of the worker's blob cache: 2 GiB.
pub const DEFAULT_BLOB_CACHE_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Temporary files older than this are left-overs of a worker that died mid-write.
const STALE_TMP_AGE: Duration = Duration::from_secs(24 * 3600);

/// Maximum number of evicted blob files deleted per lock hold.
const EVICTION_BATCH_SIZE: usize = 64;

struct Entry {
    size: u64,
    tick: u64,
}

#[derive(Default)]
struct Inner {
    entries: HashMap<BlobId, Entry>,
    /// Least recently used first.
    lru: BTreeMap<u64, BlobId>,
    tick: u64,
    total_bytes: u64,
    pins: HashMap<BlobId, usize>,
    task_pins: HashMap<TaskId, Vec<BlobId>>,
}

impl Inner {
    fn touch(&mut self, id: &BlobId) -> bool {
        self.tick += 1;
        let tick = self.tick;
        match self.entries.get_mut(id) {
            Some(entry) => {
                self.lru.remove(&entry.tick);
                entry.tick = tick;
                self.lru.insert(tick, *id);
                true
            }
            None => false,
        }
    }

    fn insert(&mut self, id: BlobId, size: u64) {
        if self.touch(&id) {
            return;
        }
        self.entries.insert(
            id,
            Entry {
                size,
                tick: self.tick,
            },
        );
        self.lru.insert(self.tick, id);
        self.total_bytes += size;
    }

    fn remove(&mut self, id: &BlobId) {
        if let Some(entry) = self.entries.remove(id) {
            self.lru.remove(&entry.tick);
            self.total_bytes -= entry.size;
        }
    }

    fn unpin_ids(&mut self, ids: impl IntoIterator<Item = BlobId>) {
        for id in ids {
            if let Some(count) = self.pins.get_mut(&id) {
                *count -= 1;
                if *count == 0 {
                    self.pins.remove(&id);
                }
            }
        }
    }
}

/// A worker's bounded, content-addressed cache of blobs. Shared as `Arc<BlobCache>`.
pub struct BlobCache {
    objects: PathBuf,
    tmp: PathBuf,
    max_bytes: u64,
    inner: Mutex<Inner>,
}

impl BlobCache {
    /// Opens (creating it if needed) the cache in `dir` (`<sandbox base>/cache/blobs`), indexing
    /// the blobs already there and removing stale temporary files.
    pub fn open(dir: &Path, max_bytes: u64) -> io::Result<Arc<Self>> {
        Self::open_with_dir_check(dir, max_bytes, ensure_private_cache_dir)
    }

    fn open_with_dir_check(
        dir: &Path,
        max_bytes: u64,
        mut check_dir: impl FnMut(&Path, bool) -> io::Result<()>,
    ) -> io::Result<Arc<Self>> {
        #[cfg(windows)]
        if let Some(cache_parent) = dir.parent().filter(|p| {
            p.file_name()
                .is_some_and(|n| n.eq_ignore_ascii_case("cache"))
        }) {
            check_dir(cache_parent, true)?;
        }
        check_dir(dir, true)?;

        let objects = dir.join("objects");
        let tmp = dir.join("tmp");
        check_dir(&objects, true)?;
        check_dir(&tmp, true)?;

        // Another worker process may be writing into `tmp/` right now: only old files go.
        for entry in std::fs::read_dir(&tmp)?.flatten() {
            let entry_path = entry.path();
            let Ok(meta) = std::fs::symlink_metadata(&entry_path) else {
                continue;
            };
            #[cfg(windows)]
            {
                use std::os::windows::fs::MetadataExt;
                const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
                if (meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT) != 0
                    || !rusty_grid_core::private_file::is_path_trusted_owner_no_follow(&entry_path)
                {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!(
                            "Task sandboxes cannot use '{}' safely: cache entry is a link or \
                             belongs to an untrusted account",
                            entry_path.display()
                        ),
                    ));
                }
            }
            if !crate::sandbox::is_real_owned_file(&entry_path, &meta) {
                continue;
            }
            let stale = meta
                .modified()
                .map(|t| {
                    SystemTime::now()
                        .duration_since(t)
                        .is_ok_and(|age| age > STALE_TMP_AGE)
                })
                .unwrap_or(false);
            if stale {
                let _ = std::fs::remove_file(&entry_path);
            }
        }

        let mut found = Vec::new();
        for fanout in std::fs::read_dir(&objects)?.flatten() {
            let fanout_path = fanout.path();
            let Ok(fanout_meta) = std::fs::symlink_metadata(&fanout_path) else {
                continue;
            };
            #[cfg(windows)]
            {
                use std::os::windows::fs::MetadataExt;
                const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
                if (fanout_meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT) != 0
                    || fanout_meta.is_dir()
                {
                    check_dir(&fanout_path, false)?;
                }
            }
            if fanout_meta.file_type().is_symlink() || !fanout_meta.is_dir() {
                continue;
            }
            for object in std::fs::read_dir(&fanout_path)?.flatten() {
                let object_path = object.path();
                let Ok(meta) = std::fs::symlink_metadata(&object_path) else {
                    continue;
                };
                #[cfg(windows)]
                {
                    use std::os::windows::fs::MetadataExt;
                    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
                    if (meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT) != 0
                        || !rusty_grid_core::private_file::is_path_trusted_owner_no_follow(
                            &object_path,
                        )
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            format!(
                                "Task sandboxes cannot use '{}' safely: cached blob entry is a \
                                 link or belongs to an untrusted account",
                                object_path.display()
                            ),
                        ));
                    }
                }
                let id = object
                    .file_name()
                    .to_str()
                    .and_then(|name| BlobId::from_hex(name).ok());
                if let (Some(id), true) =
                    (id, crate::sandbox::is_real_owned_file(&object_path, &meta))
                {
                    found.push((
                        meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                        id,
                        meta.len(),
                    ));
                }
            }
        }
        found.sort();
        let mut inner = Inner::default();
        for (_, id, size) in found {
            inner.insert(id, size);
        }
        debug!(
            dir = %dir.display(),
            blobs = inner.entries.len(),
            bytes = inner.total_bytes,
            "Worker blob cache opened"
        );
        Ok(Arc::new(Self {
            objects,
            tmp,
            max_bytes,
            inner: Mutex::new(inner),
        }))
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Where blob `id` is (or would be) stored.
    pub fn object_path(&self, id: &BlobId) -> PathBuf {
        let hex = id.to_hex();
        self.objects.join(&hex[..2]).join(hex)
    }

    /// Whether the cache holds `id` (and counts it as just used).
    pub fn contains(&self, id: &BlobId) -> bool {
        self.lock().touch(id)
    }

    /// Blobs and bytes held.
    pub fn usage(&self) -> (usize, u64) {
        let inner = self.lock();
        (inner.entries.len(), inner.total_bytes)
    }

    /// Keeps `ids` from being evicted while the returned guard lives.
    pub fn pin(self: &Arc<Self>, ids: impl IntoIterator<Item = BlobId>) -> CachePin {
        let mut pin = CachePin {
            cache: Arc::clone(self),
            ids: Vec::new(),
        };
        pin.extend(ids);
        pin
    }

    /// Transfers the blobs pinned by `pin` to `task_id`, keeping them pinned in the cache until
    /// [`BlobCache::take_task_outputs`] or [`BlobCache::release_task_outputs`] is called for
    /// `task_id`.
    pub fn keep_task_outputs(&self, task_id: TaskId, mut pin: CachePin) {
        let ids = std::mem::take(&mut pin.ids);
        if ids.is_empty() {
            return;
        }
        let mut inner = self.lock();
        if let Some(previous) = inner.task_pins.insert(task_id, ids) {
            inner.unpin_ids(previous);
        }
    }

    /// Takes back the output pin held for `task_id` as a [`CachePin`] guard (dropping the
    /// returned guard releases the pin).
    pub fn take_task_outputs(self: &Arc<Self>, task_id: TaskId) -> Option<CachePin> {
        let ids = self.lock().task_pins.remove(&task_id)?;
        Some(CachePin {
            cache: Arc::clone(self),
            ids,
        })
    }

    /// Releases any output blobs pinned for `task_id`.
    pub fn release_task_outputs(&self, task_id: TaskId) {
        let mut inner = self.lock();
        if let Some(ids) = inner.task_pins.remove(&task_id) {
            inner.unpin_ids(ids);
        }
    }

    /// A fresh path in the cache's temporary directory, for a blob being written.
    pub fn temp_path(&self) -> PathBuf {
        self.tmp.join(format!("{}.part", Uuid::new_v4()))
    }

    /// Takes over the temporary file `tmp`, which holds blob `id` (`size` bytes), already
    /// verified by whoever wrote it.
    pub fn adopt(&self, tmp: &Path, id: BlobId, size: u64) -> io::Result<()> {
        self.adopt_inner(tmp, id, size, None)
    }

    fn adopt_inner(
        &self,
        tmp: &Path,
        id: BlobId,
        size: u64,
        pin: Option<&mut CachePin>,
    ) -> io::Result<()> {
        let dest = self.object_path(&id);
        if let Some(parent) = dest.parent() {
            create_private_dir_all(parent)?;
        }
        let mut inner = self.lock();
        if let Err(e) = std::fs::rename(tmp, &dest) {
            // Windows does not replace an existing file: another task stored the same blob first.
            if !dest.exists() {
                return Err(e);
            }
            let _ = std::fs::remove_file(tmp);
        }
        inner.insert(id, size);
        if let Some(pin) = pin {
            *inner.pins.entry(id).or_insert(0) += 1;
            pin.ids.push(id);
        }
        Ok(())
    }

    /// Stores the file at `path` (an output a task produced) and returns its id and size. With
    /// `allow_move` the file is moved in when that is safe (a single link on the same file
    /// system), otherwise copied.
    pub fn insert_file(&self, path: &Path, allow_move: bool) -> io::Result<(BlobId, u64)> {
        self.insert_file_inner(path, allow_move, None)
    }

    /// Stores the file at `path` like [`BlobCache::insert_file`] and adds it to `pin` under the
    /// same cache lock that records or refreshes the entry, so a concurrent
    /// [`BlobCache::evict_to_fit`] can never evict it before the caller receives its id.
    pub fn insert_file_pinned(
        &self,
        path: &Path,
        allow_move: bool,
        pin: &mut CachePin,
    ) -> io::Result<(BlobId, u64)> {
        self.insert_file_inner(path, allow_move, Some(pin))
    }

    fn insert_file_inner(
        &self,
        path: &Path,
        allow_move: bool,
        mut pin: Option<&mut CachePin>,
    ) -> io::Result<(BlobId, u64)> {
        let (id, size) = hash_file(path)?;
        {
            let mut inner = self.lock();
            if inner.touch(&id) && self.object_path(&id).exists() {
                if let Some(pin) = pin.as_deref_mut() {
                    *inner.pins.entry(id).or_insert(0) += 1;
                    pin.ids.push(id);
                }
                return Ok((id, size));
            }
        }
        let tmp = self.temp_path();
        let moved = allow_move && single_link(path) && std::fs::rename(path, &tmp).is_ok();
        if !moved {
            if let Err(e) = copy_verified(path, &tmp, id) {
                let _ = std::fs::remove_file(&tmp);
                return Err(e);
            }
        }
        if let Err(e) = self.adopt_inner(&tmp, id, size, pin) {
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
        Ok((id, size))
    }

    /// Reads a whole blob of at most `limit` bytes and checks it against its id.
    pub fn read_verified(&self, id: &BlobId, limit: u64) -> io::Result<Vec<u8>> {
        let path = self.object_path(id);
        let size = std::fs::metadata(&path)?.len();
        if size > limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("blob {id} has {size} bytes, more than the {limit} allowed here"),
            ));
        }
        let data = std::fs::read(&path)?;
        if BlobId::of(&data) != *id {
            self.drop_corrupt(id);
            return Err(corrupt(id));
        }
        self.lock().touch(id);
        Ok(data)
    }

    /// Copies blob `id` to `dest` (which must not exist), checking the content on the way, and
    /// makes it executable when asked to.
    pub fn materialize(&self, id: &BlobId, dest: &Path, executable: bool) -> io::Result<()> {
        let src = self.object_path(id);
        match copy_verified(&src, dest, *id) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                let _ = std::fs::remove_file(dest);
                self.drop_corrupt(id);
                return Err(e);
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound && !src.exists() => {
                self.lock().remove(id);
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("blob {id} is not in the worker's blob cache"),
                ));
            }
            Err(e) => return Err(e),
        }
        self.lock().touch(id);
        crate::sandbox::set_permissions(dest, if executable { 0o755 } else { 0o644 })
    }

    pub(crate) fn drop_corrupt(&self, id: &BlobId) {
        warn!(blob = %id, "Cached blob no longer matches its id; dropping it");
        let mut inner = self.lock();
        let _ = std::fs::remove_file(self.object_path(id));
        inner.remove(id);
    }

    /// Evicts least recently used blobs that nothing pins, at most [`EVICTION_BATCH_SIZE`] files
    /// per lock hold, until the cache fits its bound. Returns how many were evicted.
    pub fn evict_to_fit(&self) -> usize {
        let mut total_evicted = 0;
        loop {
            let batch = {
                let mut inner = self.lock();
                let mut excess = inner.total_bytes.saturating_sub(self.max_bytes);
                if excess == 0 {
                    break;
                }
                let mut victims = Vec::with_capacity(EVICTION_BATCH_SIZE);
                for id in inner.lru.values() {
                    if excess == 0 || victims.len() >= EVICTION_BATCH_SIZE {
                        break;
                    }
                    if inner.pins.contains_key(id) {
                        continue;
                    }
                    excess = excess.saturating_sub(inner.entries[id].size.max(1));
                    victims.push(*id);
                }
                if victims.is_empty() {
                    break;
                }
                for id in &victims {
                    inner.remove(id);
                    if let Err(e) = std::fs::remove_file(self.object_path(id)) {
                        debug!(blob = %id, error = %e, "Could not delete an evicted blob");
                    }
                }
                victims.len()
            };
            total_evicted += batch;
            if batch < EVICTION_BATCH_SIZE {
                break;
            }
        }
        if total_evicted > 0 {
            info!(
                evicted = total_evicted,
                "Worker blob cache over its bound; evicted the least recently used blobs"
            );
        }
        total_evicted
    }
}

impl crate::workspace::BlobSource for BlobCache {
    fn open(&self, id: &BlobId) -> io::Result<Box<dyn Read + Send>> {
        let path = self.object_path(id);
        match File::open(&path) {
            Ok(file) => {
                self.lock().touch(id);
                Ok(Box::new(file))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                self.lock().remove(id);
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("blob {id} is not in the worker's blob cache"),
                ))
            }
            Err(e) => Err(e),
        }
    }
}

/// Keeps blobs pinned in a [`BlobCache`] until dropped.
pub struct CachePin {
    cache: Arc<BlobCache>,
    ids: Vec<BlobId>,
}

impl std::fmt::Debug for CachePin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachePin").field("ids", &self.ids).finish()
    }
}

impl CachePin {
    /// Pins more blobs under this guard.
    pub fn extend(&mut self, ids: impl IntoIterator<Item = BlobId>) {
        let mut inner = self.cache.lock();
        for id in ids {
            *inner.pins.entry(id).or_insert(0) += 1;
            self.ids.push(id);
        }
    }
}

impl Drop for CachePin {
    fn drop(&mut self) {
        if self.ids.is_empty() {
            return;
        }
        let mut inner = self.cache.lock();
        inner.unpin_ids(self.ids.drain(..));
    }
}

/// Creates or validates `dir` (and any missing ancestors) as a private cache directory:
/// - on Unix, creates missing directories with mode `0700`;
/// - on Windows, delegates to [`crate::sandbox::ensure_private_dir`] /
///   [`crate::sandbox::ensure_private_dir_recheck`] so missing directories get the protected
///   owner-only DACL and existing directories are refused if they are reparse points, have an
///   untrusted owner, or carry foreign write-class grants.
fn ensure_private_cache_dir(dir: &Path, emit_warnings: bool) -> io::Result<()> {
    #[cfg(windows)]
    {
        let res = if emit_warnings {
            crate::sandbox::ensure_private_dir(dir)
        } else {
            crate::sandbox::ensure_private_dir_recheck(dir)
        };
        res.map_err(|e| match e {
            crate::sandbox::SandboxError::BaseDirUnusable { source, .. } => source,
            other => io::Error::new(io::ErrorKind::PermissionDenied, other.to_string()),
        })
    }
    #[cfg(not(windows))]
    {
        let _ = emit_warnings;
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(dir)
    }
}

fn create_private_dir_all(dir: &Path) -> io::Result<()> {
    ensure_private_cache_dir(dir, false)
}

fn corrupt(id: &BlobId) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("cached blob {id} no longer matches its id (it was dropped; run the task again)"),
    )
}

/// Copies `src` to the new file `dest`, hashing on the way, and fails with `InvalidData` unless
/// the content is blob `id`.
fn copy_verified(src: &Path, dest: &Path, id: BlobId) -> io::Result<()> {
    let mut input = File::open(src)?;
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dest)?;
    let mut hasher = BlobHasher::new();
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let n = match input.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        hasher.update(&buf[..n]);
        output.write_all(&buf[..n])?;
    }
    if hasher.finish() != id {
        return Err(corrupt(&id));
    }
    Ok(())
}

/// True when moving `path` cannot change any other file: it is the only link to its content.
fn single_link(path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        std::fs::symlink_metadata(path).is_ok_and(|m| m.is_file() && m.nlink() == 1)
    }
    #[cfg(not(unix))]
    {
        std::fs::symlink_metadata(path).is_ok_and(|m| m.is_file())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache_in(dir: &Path, max: u64) -> Arc<BlobCache> {
        BlobCache::open(&dir.join("blobs"), max).unwrap()
    }

    fn put(cache: &BlobCache, dir: &Path, data: &[u8]) -> BlobId {
        let src = dir.join(format!("src-{}", Uuid::new_v4()));
        std::fs::write(&src, data).unwrap();
        let (id, size) = cache.insert_file(&src, true).unwrap();
        assert_eq!(size, data.len() as u64);
        id
    }

    #[test]
    fn inserted_files_are_found_again_after_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache_in(dir.path(), 1 << 20);
        let id = put(&cache, dir.path(), b"hello");
        assert_eq!(id, BlobId::of(b"hello"));
        assert!(cache.contains(&id));
        drop(cache);
        let reopened = cache_in(dir.path(), 1 << 20);
        assert!(reopened.contains(&id));
        assert_eq!(reopened.usage(), (1, 5));
        assert_eq!(reopened.read_verified(&id, 100).unwrap(), b"hello");
    }

    #[test]
    fn materialized_inputs_are_copies_so_a_task_cannot_change_the_cache() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache_in(dir.path(), 1 << 20);
        let id = put(&cache, dir.path(), b"original input");
        let dest = dir.path().join("sandbox-input.txt");
        cache.materialize(&id, &dest, true).unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"original input");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&dest).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o755);
        }

        // The task rewrites its input in place: the next task still gets the original.
        std::fs::write(&dest, b"modified by the task").unwrap();
        let again = dir.path().join("second-run.txt");
        cache.materialize(&id, &again, false).unwrap();
        assert_eq!(std::fs::read(&again).unwrap(), b"original input");

        // An existing destination is refused, never overwritten.
        assert!(cache.materialize(&id, &again, false).is_err());
    }

    #[test]
    fn a_corrupted_blob_is_detected_and_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache_in(dir.path(), 1 << 20);
        let id = put(&cache, dir.path(), b"good content");
        std::fs::write(cache.object_path(&id), b"bad content!").unwrap();
        let err = cache
            .materialize(&id, &dir.path().join("out"), false)
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(!dir.path().join("out").exists());
        assert!(!cache.contains(&id), "a corrupt blob must be fetched again");
    }

    #[test]
    fn eviction_keeps_pinned_and_recently_used_blobs() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache_in(dir.path(), 2500);
        let a = put(&cache, dir.path(), &[1u8; 1000]);
        let b = put(&cache, dir.path(), &[2u8; 1000]);
        let c = put(&cache, dir.path(), &[3u8; 1000]);
        let d = put(&cache, dir.path(), &[4u8; 1000]);
        let pin = cache.pin([a]);
        assert!(
            cache.contains(&b),
            "using b makes c the least recently used"
        );
        assert_eq!(cache.evict_to_fit(), 2);
        assert!(cache.contains(&a) && cache.contains(&b));
        assert!(!cache.contains(&c) && !cache.contains(&d));
        assert!(!cache.object_path(&c).exists());
        drop(pin);
        assert!(cache.usage().1 <= 2500);
    }

    #[test]
    fn moving_an_output_in_never_steals_a_file_with_other_links() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache_in(dir.path(), 1 << 20);
        let output = dir.path().join("output.bin");
        std::fs::write(&output, b"artifact").unwrap();
        let (id, _) = cache.insert_file(&output, true).unwrap();
        assert!(
            !output.exists(),
            "a single-link output is moved into the cache"
        );
        assert!(cache.object_path(&id).exists());

        #[cfg(unix)]
        {
            let shared = dir.path().join("shared.bin");
            let link = dir.path().join("link.bin");
            std::fs::write(&shared, b"shared content").unwrap();
            std::fs::hard_link(&shared, &link).unwrap();
            cache.insert_file(&link, true).unwrap();
            assert!(
                link.exists() && shared.exists(),
                "a linked file is copied, not moved"
            );
        }
    }

    #[test]
    fn evict_to_fit_deletes_across_multiple_batches_and_ends_within_bound() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache_in(dir.path(), 500);
        for i in 0..150u32 {
            let mut data = vec![0u8; 100];
            data[..4].copy_from_slice(&i.to_le_bytes());
            put(&cache, dir.path(), &data);
        }
        assert_eq!(cache.usage(), (150, 15_000));
        assert_eq!(cache.evict_to_fit(), 145);
        assert_eq!(cache.usage(), (5, 500));
    }

    #[cfg(windows)]
    #[test]
    fn windows_blob_cache_creates_private_dirs_and_refuses_junctions_untrusted_owners_and_foreign_write_grants(
    ) {
        use rusty_grid_core::private_file::{
            create_or_open_private_dir_all, current_user_sid, dir_open_grants,
            inspect_dir_security_no_follow, read_dacl, DirSecuritySnapshot,
        };
        use std::process::Command;

        fn make_junction(link: &Path, target: &Path) {
            let status = Command::new("cmd")
                .args(["/c", "mklink", "/J"])
                .arg(link)
                .arg(target)
                .output()
                .expect("cmd mklink /J");
            assert!(
                status.status.success(),
                "mklink /J failed: {}",
                String::from_utf8_lossy(&status.stderr)
            );
        }

        let user = current_user_sid().unwrap();

        // 1. Missing `<base>\cache\blobs\{objects,tmp}` (plus fanout dirs) are created with a
        //    protected owner-only DACL, even when `<base>` had no protected inheritance.
        let root_tmp = tempfile::tempdir().unwrap();
        let base = root_tmp.path().join("base");
        crate::sandbox::ensure_private_dir(&base).unwrap();
        let blobs_dir = base.join("cache").join("blobs");
        let cache = BlobCache::open(&blobs_dir, 1 << 20).unwrap();
        let id = put(&cache, root_tmp.path(), b"private-cache-blob");
        let fanout_dir = cache.object_path(&id).parent().unwrap().to_path_buf();
        for d in [
            base.join("cache"),
            blobs_dir.clone(),
            blobs_dir.join("objects"),
            blobs_dir.join("tmp"),
            fanout_dir,
        ] {
            let (aces, protected) = read_dacl(&d).unwrap();
            assert!(protected, "{} must have a protected DACL", d.display());
            assert!(
                dir_open_grants(&aces, &user).is_empty(),
                "{} must grant no foreign access: {aces:?}",
                d.display()
            );
        }
        drop(cache);

        // 2. A junction at `<base>\cache` is refused without writing into the junction target.
        let junc_base = root_tmp.path().join("base_junc_cache");
        crate::sandbox::ensure_private_dir(&junc_base).unwrap();
        let canary_target = root_tmp.path().join("canary_target");
        std::fs::create_dir(&canary_target).unwrap();
        let cache_junc = junc_base.join("cache");
        make_junction(&cache_junc, &canary_target);
        let err_junc_cache = match BlobCache::open(&cache_junc.join("blobs"), 1 << 20) {
            Ok(_) => panic!("junction at <base>\\cache must be refused"),
            Err(e) => e,
        };
        assert_eq!(err_junc_cache.kind(), io::ErrorKind::PermissionDenied);
        let msg_junc = err_junc_cache.to_string();
        assert!(
            msg_junc.contains(&cache_junc.display().to_string())
                && msg_junc.contains("reparse point"),
            "unexpected error: {msg_junc}"
        );
        assert_eq!(
            std::fs::read_dir(&canary_target).unwrap().count(),
            0,
            "junction target must stay untouched"
        );

        // 3. A junction at a fanout subdirectory (`objects\ab`) is also refused on open.
        let junc_fanout_base = root_tmp.path().join("base_junc_fanout");
        let fanout_blobs = junc_fanout_base.join("cache").join("blobs");
        drop(BlobCache::open(&fanout_blobs, 1 << 20).unwrap());
        make_junction(&fanout_blobs.join("objects").join("ab"), &canary_target);
        let err_fanout = match BlobCache::open(&fanout_blobs, 1 << 20) {
            Ok(_) => panic!("junction at objects\\ab must be refused"),
            Err(e) => e,
        };
        assert!(
            err_fanout.to_string().contains("reparse point"),
            "{err_fanout}"
        );

        // 4. Existing `<base>\cache` or `<base>\cache\blobs\objects` with a foreign write grant
        //    (such as `BUILTIN\Users:(OI)(CI)(W)`) is refused with the base's `icacls` remedy.
        let write_base = root_tmp.path().join("base_foreign_write");
        let write_blobs = write_base.join("cache").join("blobs");
        drop(BlobCache::open(&write_blobs, 1 << 20).unwrap());
        let status = Command::new("icacls")
            .arg(write_base.join("cache"))
            .arg("/grant")
            .arg("*S-1-5-32-545:(OI)(CI)(W)")
            .output()
            .expect("icacls /grant");
        assert!(status.status.success());
        let err_write = match BlobCache::open(&write_blobs, 1 << 20) {
            Ok(_) => panic!("foreign write grant on <base>\\cache must be refused"),
            Err(e) => e,
        };
        assert!(
            err_write
                .to_string()
                .contains("grants write access to other accounts")
                && err_write.to_string().contains("icacls"),
            "{err_write}"
        );

        // 5a. Real read-only check on `%SystemRoot%` (`TrustedInstaller` owner) is refused.
        if let Some(sys_root) = std::env::var_os("SystemRoot") {
            let sys_path = PathBuf::from(sys_root);
            if let Ok(snap) = inspect_dir_security_no_follow(&sys_path) {
                if let Ok(owner) = snap.owner_sid {
                    if !rusty_grid_core::private_file::is_trusted_owner_sid(&owner, &user) {
                        let err_sys = match BlobCache::open(&sys_path, 1 << 20) {
                            Ok(_) => panic!("TrustedInstaller-owned cache dir must be refused"),
                            Err(e) => e,
                        };
                        assert!(err_sys.to_string().contains(&owner), "{err_sys}");
                    }
                }
            }
        }

        // 5b. Deterministic non-elevated check: `<base>` is owned by the worker account while
        //     `<base>\cache` (left over from an old open base) belongs to another local user SID
        //     (`S-1-5-21-1-2-3-1002`).
        let foreign_owner_base = root_tmp.path().join("base_foreign_owner");
        let foreign_cache = foreign_owner_base.join("cache");
        let foreign_blobs = foreign_cache.join("blobs");
        drop(BlobCache::open(&foreign_blobs, 1 << 20).unwrap());
        let worker_sid = "S-1-5-21-1-2-3-1001";
        let attacker_sid = "S-1-5-21-1-2-3-1002";
        let err_owner = match BlobCache::open_with_dir_check(&foreign_blobs, 1 << 20, |p, warn| {
            crate::sandbox::ensure_private_dir_windows_with(
                p,
                warn,
                || Ok(worker_sid.to_string()),
                create_or_open_private_dir_all,
                |inspected| {
                    let owner = if inspected == foreign_cache {
                        attacker_sid
                    } else {
                        worker_sid
                    };
                    Ok(DirSecuritySnapshot {
                        is_reparse_point: false,
                        is_dir: true,
                        persistent_acls: Ok(true),
                        owner_sid: Ok(owner.to_string()),
                        dacl: Ok((Vec::new(), true)),
                    })
                },
            )
            .map_err(|e| io::Error::new(io::ErrorKind::PermissionDenied, e.to_string()))
        }) {
            Ok(_) => panic!("foreign-owned <base>\\cache must be refused"),
            Err(e) => e,
        };
        assert!(
            err_owner.to_string().contains(attacker_sid)
                && err_owner
                    .to_string()
                    .contains(&foreign_cache.display().to_string()),
            "{err_owner}"
        );
    }
}
