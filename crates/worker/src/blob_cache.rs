//! The worker's blob cache: task input and output files kept on disk by content
//! (`rusty_grid_core::blob`), so a file that did not change is fetched from the Master once, not
//! once per task. Design record: `docs/design/0005-content-addressed-data-plane.md`.
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

/// Default bound of the worker's blob cache: 2 GiB.
pub const DEFAULT_BLOB_CACHE_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Temporary files older than this are left-overs of a worker that died mid-write.
const STALE_TMP_AGE: Duration = Duration::from_secs(24 * 3600);

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
        let objects = dir.join("objects");
        let tmp = dir.join("tmp");
        create_private_dir_all(&objects)?;
        create_private_dir_all(&tmp)?;

        // Another worker process may be writing into `tmp/` right now: only old files go.
        for entry in std::fs::read_dir(&tmp)?.flatten() {
            let stale = entry
                .metadata()
                .and_then(|m| m.modified())
                .map(|t| {
                    SystemTime::now()
                        .duration_since(t)
                        .is_ok_and(|age| age > STALE_TMP_AGE)
                })
                .unwrap_or(false);
            if stale {
                let _ = std::fs::remove_file(entry.path());
            }
        }

        let mut found = Vec::new();
        for fanout in std::fs::read_dir(&objects)?.flatten() {
            if !fanout.file_type().is_ok_and(|t| t.is_dir()) {
                continue;
            }
            for object in std::fs::read_dir(fanout.path())?.flatten() {
                let Ok(meta) = object.metadata() else {
                    continue;
                };
                let id = object
                    .file_name()
                    .to_str()
                    .and_then(|name| BlobId::from_hex(name).ok());
                if let (Some(id), true) = (id, meta.is_file()) {
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

    /// A fresh path in the cache's temporary directory, for a blob being written.
    pub fn temp_path(&self) -> PathBuf {
        self.tmp.join(format!("{}.part", Uuid::new_v4()))
    }

    /// Takes over the temporary file `tmp`, which holds blob `id` (`size` bytes), already
    /// verified by whoever wrote it.
    pub fn adopt(&self, tmp: &Path, id: BlobId, size: u64) -> io::Result<()> {
        let dest = self.object_path(&id);
        if let Some(parent) = dest.parent() {
            create_private_dir_all(parent)?;
        }
        if let Err(e) = std::fs::rename(tmp, &dest) {
            // Windows does not replace an existing file: another task stored the same blob first.
            if !dest.exists() {
                return Err(e);
            }
            let _ = std::fs::remove_file(tmp);
        }
        self.lock().insert(id, size);
        Ok(())
    }

    /// Stores the file at `path` (an output a task produced) and returns its id and size. With
    /// `allow_move` the file is moved in when that is safe (a single link on the same file
    /// system), otherwise copied.
    pub fn insert_file(&self, path: &Path, allow_move: bool) -> io::Result<(BlobId, u64)> {
        let (id, size) = hash_file(path)?;
        if self.contains(&id) && self.object_path(&id).exists() {
            return Ok((id, size));
        }
        let tmp = self.temp_path();
        let moved = allow_move && single_link(path) && std::fs::rename(path, &tmp).is_ok();
        if !moved {
            if let Err(e) = copy_verified(path, &tmp, id) {
                let _ = std::fs::remove_file(&tmp);
                return Err(e);
            }
        }
        if let Err(e) = self.adopt(&tmp, id, size) {
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

    fn drop_corrupt(&self, id: &BlobId) {
        warn!(blob = %id, "Cached blob no longer matches its id; dropping it");
        let _ = std::fs::remove_file(self.object_path(id));
        self.lock().remove(id);
    }

    /// Evicts least recently used blobs that nothing pins until the cache fits its bound.
    /// Returns how many were evicted.
    pub fn evict_to_fit(&self) -> usize {
        let victims: Vec<BlobId> = {
            let mut inner = self.lock();
            let mut excess = inner.total_bytes.saturating_sub(self.max_bytes);
            let mut victims = Vec::new();
            for id in inner.lru.values() {
                if excess == 0 {
                    break;
                }
                if inner.pins.contains_key(id) {
                    continue;
                }
                excess = excess.saturating_sub(inner.entries[id].size.max(1));
                victims.push(*id);
            }
            for id in &victims {
                inner.remove(id);
            }
            victims
        };
        for id in &victims {
            if let Err(e) = std::fs::remove_file(self.object_path(id)) {
                debug!(blob = %id, error = %e, "Could not delete an evicted blob");
            }
        }
        if !victims.is_empty() {
            info!(
                evicted = victims.len(),
                "Worker blob cache over its bound; evicted the least recently used blobs"
            );
        }
        victims.len()
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
        let mut inner = self.cache.lock();
        for id in self.ids.drain(..) {
            if let Some(count) = inner.pins.get_mut(&id) {
                *count -= 1;
                if *count == 0 {
                    inner.pins.remove(&id);
                }
            }
        }
    }
}

/// Creates `dir` and its missing parents readable by the owner only (Unix): the cache holds the
/// files of every task, and the sandbox base directory may be a shared temporary directory.
fn create_private_dir_all(dir: &Path) -> io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(dir)
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
}
