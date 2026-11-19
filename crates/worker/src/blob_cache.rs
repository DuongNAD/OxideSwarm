//! The worker's blob cache: task input and output files kept on disk by content
//! (`rusty_grid_core::blob`), so a file that did not change is fetched from the Master once, not
//! once per task. Design record: `docs/design/0006-content-addressed-data-plane.md`.
//!
//! * **Layout.** `<sandbox base>/cache/blobs/objects/<first two hex digits>/<64 hex digits>`, one
//!   verified blob per file, `<sandbox base>/cache/blobs/tmp/` for blobs being written
//!   (`<owner_uuid>.<part_uuid>.part`), and `<sandbox base>/cache/blobs/owners/` for per-cache
//!   advisory owner lock files (`<owner_uuid>.lock`). Task sandboxes are siblings of `cache/`, so
//!   moving an output file into the cache is a rename.
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

use rusty_grid_core::blob::{hash_reader, open_regular_file_nofollow, BlobHasher, BlobId};
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

struct OwnerLockGuard {
    owner_id: Uuid,
    path: PathBuf,
    file: Option<File>,
}

impl Drop for OwnerLockGuard {
    fn drop(&mut self) {
        if let Some(lock_file) = self.file.take() {
            let _ = std::fs::remove_file(&self.path);
            let _ = lock_file.unlock();
            drop(lock_file);
        }
    }
}

/// A worker's bounded, content-addressed cache of blobs. Shared as `Arc<BlobCache>`.
pub struct BlobCache {
    objects: PathBuf,
    tmp: PathBuf,
    max_bytes: u64,
    _owner_lock: OwnerLockGuard,
    inner: Mutex<Inner>,
}

impl BlobCache {
    /// Opens (creating it if needed) the cache in `dir` (`<sandbox base>/cache/blobs`), indexing
    /// the blobs already there and removing dead-owner and stale temporary files.
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
        let owners = dir.join("owners");
        check_dir(&objects, true)?;
        check_dir(&tmp, true)?;
        check_dir(&owners, true)?;

        let owner_lock = acquire_owner_lock(&owners)?;
        sweep_tmp_and_owners(&tmp, &owners, Some(owner_lock.owner_id), true, true)?;

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
                check_trusted_leaf_entry(&object_path, &meta, "cached blob entry")?;
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
            _owner_lock: owner_lock,
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
        self.tmp.join(format!(
            "{}.{}.part",
            self._owner_lock.owner_id,
            Uuid::new_v4()
        ))
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
        pin: Option<&mut CachePin>,
    ) -> io::Result<(BlobId, u64)> {
        let (file, meta) = open_regular_file_nofollow(path)?;
        self.insert_open_file(path, file, &meta, allow_move, pin)
    }

    pub(crate) fn insert_open_file(
        &self,
        path: &Path,
        mut file: File,
        meta: &std::fs::Metadata,
        allow_move: bool,
        mut pin: Option<&mut CachePin>,
    ) -> io::Result<(BlobId, u64)> {
        let (id, size) = hash_reader(&mut file)?;
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
        let moved = if allow_move
            && descriptor_is_single_link(&file, meta)
            && std::fs::rename(path, &tmp).is_ok()
        {
            if moved_entry_matches_descriptor(&tmp, &file, meta, size) {
                true
            } else {
                remove_moved_entry(&tmp);
                false
            }
        } else {
            false
        };
        if !moved {
            if let Err(e) = copy_descriptor_verified(&mut file, &tmp, id, size) {
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

fn check_trusted_leaf_entry(path: &Path, meta: &std::fs::Metadata, label: &str) -> io::Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
        if (meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT) != 0
            || !rusty_grid_core::private_file::is_path_trusted_owner_no_follow(path)
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "Task sandboxes cannot use '{}' safely: {label} is a link or belongs to an \
                     untrusted account",
                    path.display()
                ),
            ));
        }
    }
    #[cfg(not(windows))]
    let _ = (path, meta, label);
    Ok(())
}

fn parse_owned_part_owner(file_name: &str) -> Option<Uuid> {
    let stem = file_name.strip_suffix(".part")?;
    let (owner_str, part_str) = stem.split_once('.')?;
    let owner = Uuid::parse_str(owner_str).ok()?;
    let _part = Uuid::parse_str(part_str).ok()?;
    Some(owner)
}

fn parse_owner_lock_id(file_name: &str) -> Option<Uuid> {
    let stem = file_name.strip_suffix(".lock")?;
    Uuid::parse_str(stem).ok()
}

fn acquire_owner_lock(owners: &Path) -> io::Result<OwnerLockGuard> {
    for _ in 0..8 {
        let owner_id = Uuid::new_v4();
        let path = owners.join(format!("{owner_id}.lock"));
        let mut opts = std::fs::OpenOptions::new();
        opts.create_new(true).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let file = match opts.open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        };
        match file.try_lock() {
            Ok(()) => {
                return Ok(OwnerLockGuard {
                    owner_id,
                    path,
                    file: Some(file),
                });
            }
            Err(std::fs::TryLockError::WouldBlock) => {
                let _ = std::fs::remove_file(&path);
                drop(file);
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!(
                        "owner lock '{}' was locked by another process",
                        path.display()
                    ),
                ));
            }
            Err(std::fs::TryLockError::Error(err)) => {
                let _ = std::fs::remove_file(&path);
                drop(file);
                return Err(err);
            }
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "exhausted attempts creating a unique blob cache owner lock",
    ))
}

fn open_existing_owner_lock(lock_path: &Path) -> io::Result<File> {
    let mut opts = std::fs::OpenOptions::new();
    opts.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // FILE_FLAG_OPEN_REPARSE_POINT: never follow a symbolic link or reparse point at lock_path.
        opts.custom_flags(0x0020_0000);
    }
    opts.open(lock_path)
}

enum OwnerProbe {
    Alive,
    Dead(Option<(PathBuf, File)>),
    Unknown,
}

fn try_claim_dead_owner_lock(lock_path: &Path, strict_security: bool) -> io::Result<OwnerProbe> {
    let meta = match std::fs::symlink_metadata(lock_path) {
        Ok(meta) => meta,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(OwnerProbe::Dead(None)),
        Err(e) => {
            return if strict_security && e.kind() == io::ErrorKind::PermissionDenied {
                Err(e)
            } else {
                Ok(OwnerProbe::Unknown)
            };
        }
    };
    if strict_security {
        check_trusted_leaf_entry(lock_path, &meta, "owner lock entry")?;
    }
    if !crate::sandbox::is_real_owned_file(lock_path, &meta) {
        return Ok(OwnerProbe::Unknown);
    }
    let file = match open_existing_owner_lock(lock_path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(OwnerProbe::Dead(None)),
        Err(_) => return Ok(OwnerProbe::Unknown),
    };
    let Ok(file_meta) = file.metadata() else {
        return Ok(OwnerProbe::Unknown);
    };
    if !crate::sandbox::is_real_owned_open_file(&file, &file_meta) {
        return if strict_security {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "Task sandboxes cannot use '{}' safely: owner lock is a link or belongs to \
                     an untrusted account",
                    lock_path.display()
                ),
            ))
        } else {
            Ok(OwnerProbe::Unknown)
        };
    }
    match file.try_lock() {
        Ok(()) => {
            if crate::sandbox::lock_file_is_still_named(lock_path, &file, &file_meta) {
                Ok(OwnerProbe::Dead(Some((lock_path.to_path_buf(), file))))
            } else {
                let _ = file.unlock();
                drop(file);
                Ok(OwnerProbe::Dead(None))
            }
        }
        Err(std::fs::TryLockError::WouldBlock) => Ok(OwnerProbe::Alive),
        Err(std::fs::TryLockError::Error(_)) => Ok(OwnerProbe::Unknown),
    }
}

fn is_stale_tmp_file(meta: &std::fs::Metadata) -> bool {
    meta.modified()
        .map(|t| {
            SystemTime::now()
                .duration_since(t)
                .is_ok_and(|age| age > STALE_TMP_AGE)
        })
        .unwrap_or(false)
}

fn sweep_tmp_and_owners(
    tmp: &Path,
    owners: &Path,
    self_owner: Option<Uuid>,
    strict_security: bool,
    sweep_legacy_stale: bool,
) -> io::Result<()> {
    let mut probed: HashMap<Uuid, OwnerProbe> = HashMap::new();

    for entry in std::fs::read_dir(tmp)?.flatten() {
        let entry_path = entry.path();
        let Ok(meta) = std::fs::symlink_metadata(&entry_path) else {
            continue;
        };
        if strict_security {
            check_trusted_leaf_entry(&entry_path, &meta, "cache entry")?;
        }
        if !crate::sandbox::is_real_owned_file(&entry_path, &meta) {
            continue;
        }
        let owner_opt = entry.file_name().to_str().and_then(parse_owned_part_owner);
        let should_remove = match owner_opt {
            Some(owner_id) => {
                if self_owner == Some(owner_id) {
                    false
                } else {
                    let probe = match probed.entry(owner_id) {
                        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            let lock_path = owners.join(format!("{owner_id}.lock"));
                            let res = try_claim_dead_owner_lock(&lock_path, strict_security)?;
                            entry.insert(res)
                        }
                    };
                    match probe {
                        OwnerProbe::Dead(_) => true,
                        OwnerProbe::Alive => false,
                        OwnerProbe::Unknown => sweep_legacy_stale && is_stale_tmp_file(&meta),
                    }
                }
            }
            None => sweep_legacy_stale && is_stale_tmp_file(&meta),
        };
        if should_remove {
            let _ = std::fs::remove_file(&entry_path);
        }
    }

    let mut handled_owners: Vec<Uuid> = Vec::with_capacity(probed.len());
    for (owner_id, probe) in probed {
        handled_owners.push(owner_id);
        if let OwnerProbe::Dead(Some((lock_path, lock_file))) = probe {
            let _ = std::fs::remove_file(&lock_path);
            let _ = lock_file.unlock();
            drop(lock_file);
        }
    }

    let owner_entries = match std::fs::read_dir(owners) {
        Ok(rd) => rd,
        Err(e) if !strict_security && e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    for entry in owner_entries.flatten() {
        let lock_path = entry.path();
        let Ok(meta) = std::fs::symlink_metadata(&lock_path) else {
            continue;
        };
        if strict_security {
            check_trusted_leaf_entry(&lock_path, &meta, "owner lock entry")?;
        }
        if !crate::sandbox::is_real_owned_file(&lock_path, &meta) {
            continue;
        }
        let Some(owner_id) = entry.file_name().to_str().and_then(parse_owner_lock_id) else {
            continue;
        };
        if self_owner == Some(owner_id) || handled_owners.contains(&owner_id) {
            continue;
        }
        if let Ok(OwnerProbe::Dead(Some((dead_path, lock_file)))) =
            try_claim_dead_owner_lock(&lock_path, strict_security)
        {
            let _ = std::fs::remove_file(&dead_path);
            let _ = lock_file.unlock();
            drop(lock_file);
        }
    }

    Ok(())
}

/// Sweeps dead-owner `.part` files and orphan dead owner locks under an existing
/// `<sandbox base>/cache/blobs` directory at worker startup without creating any directories.
pub(crate) fn sweep_dead_owner_tmp(blobs_dir: &Path) {
    let Some(cache_dir) = blobs_dir.parent() else {
        return;
    };
    let Ok(cache_meta) = std::fs::symlink_metadata(cache_dir) else {
        return;
    };
    if !crate::sandbox::is_real_owned_dir(cache_dir, &cache_meta) {
        return;
    }
    let Ok(blobs_meta) = std::fs::symlink_metadata(blobs_dir) else {
        return;
    };
    if !crate::sandbox::is_real_owned_dir(blobs_dir, &blobs_meta) {
        return;
    }
    let tmp = blobs_dir.join("tmp");
    let Ok(tmp_meta) = std::fs::symlink_metadata(&tmp) else {
        return;
    };
    if !crate::sandbox::is_real_owned_dir(&tmp, &tmp_meta) {
        return;
    }
    let owners = blobs_dir.join("owners");
    match std::fs::symlink_metadata(&owners) {
        Ok(owners_meta) if !crate::sandbox::is_real_owned_dir(&owners, &owners_meta) => return,
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(_) => return,
    }
    let _ = sweep_tmp_and_owners(&tmp, &owners, None, false, false);
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

/// Copies the open `input` descriptor (rewound to offset 0) to the new file `dest`, hashing on the
/// way, and fails with `InvalidData` unless the content has `expected_size` bytes and matches `id`.
fn copy_descriptor_verified(
    input: &mut File,
    dest: &Path,
    id: BlobId,
    expected_size: u64,
) -> io::Result<()> {
    use std::io::{Seek, SeekFrom};
    input.seek(SeekFrom::Start(0))?;
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dest)?;
    let mut hasher = BlobHasher::new();
    let mut buf = vec![0u8; 256 * 1024];
    let mut written = 0u64;
    loop {
        let n = match input.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        hasher.update(&buf[..n]);
        output.write_all(&buf[..n])?;
        written += n as u64;
    }
    if written != expected_size || hasher.finish() != id {
        return Err(corrupt(&id));
    }
    Ok(())
}

/// True when moving the open descriptor cannot change any other file: it is a regular file with a
/// single link to its content.
fn descriptor_is_single_link(file: &File, meta: &std::fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let _ = file;
        meta.is_file() && !meta.file_type().is_symlink() && meta.nlink() == 1
    }
    #[cfg(windows)]
    {
        windows_file_identity(file).is_some_and(|(_, _, nlink)| meta.is_file() && nlink == 1)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (file, meta);
        false
    }
}

/// True when `tmp` after `rename(path, tmp)` is a regular file (never a symbolic link or reparse
/// point) with the same inode/file-index and single link as the open descriptor `file` / `meta`.
fn moved_entry_matches_descriptor(
    tmp: &Path,
    file: &File,
    meta: &std::fs::Metadata,
    expected_size: u64,
) -> bool {
    let Ok(tmp_meta) = std::fs::symlink_metadata(tmp) else {
        return false;
    };
    if tmp_meta.file_type().is_symlink() || !tmp_meta.is_file() || tmp_meta.len() != expected_size {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let _ = file;
        tmp_meta.dev() == meta.dev() && tmp_meta.ino() == meta.ino() && tmp_meta.nlink() == 1
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
        let _ = meta;
        if (tmp_meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT) != 0 {
            return false;
        }
        let Some((vol, idx, _)) = windows_file_identity(file) else {
            return false;
        };
        let Ok((tmp_file, _)) = open_regular_file_nofollow(tmp) else {
            return false;
        };
        windows_file_identity(&tmp_file)
            .is_some_and(|(t_vol, t_idx, t_nlink)| t_vol == vol && t_idx == idx && t_nlink == 1)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (file, meta);
        false
    }
}

fn remove_moved_entry(tmp: &Path) {
    if std::fs::remove_file(tmp).is_err()
        && std::fs::remove_dir(tmp).is_err()
        && std::fs::symlink_metadata(tmp).is_ok_and(|m| m.is_dir() && !m.file_type().is_symlink())
    {
        let _ = std::fs::remove_dir_all(tmp);
    }
}

#[cfg(windows)]
fn windows_file_identity(file: &File) -> Option<(u32, u64, u32)> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::Storage::FileSystem::{
        GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION, FILE_ATTRIBUTE_REPARSE_POINT,
    };
    let handle = file.as_raw_handle() as HANDLE;
    // SAFETY: `BY_HANDLE_FILE_INFORMATION` is plain integer fields.
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    // SAFETY: `file` owns `handle` for the duration of this call.
    if unsafe { GetFileInformationByHandle(handle, &mut info) } == 0 {
        return None;
    }
    if (info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0 {
        return None;
    }
    let index = ((info.nFileIndexHigh as u64) << 32) | (info.nFileIndexLow as u64);
    // ReFS/Dev Drive (128-bit file IDs) or network volumes can report a zero 64-bit file index
    // alongside a non-zero volume serial number. Returning `None` when either is zero prevents two
    // different files from comparing equal and forces `insert_open_file` to copy from the open
    // descriptor (`copy_descriptor_verified`).
    if info.dwVolumeSerialNumber == 0 || index == 0 {
        return None;
    }
    Some((info.dwVolumeSerialNumber, index, info.nNumberOfLinks))
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

        #[cfg(any(unix, windows))]
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

    #[cfg(unix)]
    #[test]
    fn insert_file_refuses_symlink_outside_sandbox_and_never_caches_target() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache_in(dir.path(), 1 << 20);
        let secret = dir.path().join("outside-secret.txt");
        let secret_bytes = b"private host secret";
        std::fs::write(&secret, secret_bytes).unwrap();

        let sandbox = dir.path().join("sandbox");
        std::fs::create_dir_all(&sandbox).unwrap();
        let link = sandbox.join("out.txt");
        std::os::unix::fs::symlink(&secret, &link).unwrap();

        for allow_move in [true, false] {
            let err = cache.insert_file(&link, allow_move).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
            assert!(err.to_string().contains("symbolic link"), "{err}");
            assert!(!cache.contains(&BlobId::of(secret_bytes)));
            assert!(!cache.object_path(&BlobId::of(secret_bytes)).exists());
            assert_eq!(cache.usage(), (0, 0));
        }
        assert_eq!(std::fs::read(&secret).unwrap(), secret_bytes);
    }

    #[cfg(unix)]
    #[test]
    fn insert_file_refuses_fifo_without_blocking() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let dir = tempfile::tempdir().unwrap();
        let cache = cache_in(dir.path(), 1 << 20);
        let fifo = dir.path().join("output.fifo");
        let fifo_c = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        // SAFETY: `fifo_c` is a valid NUL-terminated path in a private tempdir.
        assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0);

        let err = cache.insert_file(&fifo, true).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("not a regular file"), "{err}");
        assert_eq!(cache.usage(), (0, 0));
    }

    #[cfg(unix)]
    #[test]
    fn insert_open_file_falls_back_to_descriptor_copy_when_path_is_swapped_for_symlink_before_rename(
    ) {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache_in(dir.path(), 1 << 20);
        let secret = dir.path().join("secret.txt");
        let secret_bytes = b"worker-only secret";
        std::fs::write(&secret, secret_bytes).unwrap();

        let output = dir.path().join("output.txt");
        let regular_bytes = b"legitimate task output";
        std::fs::write(&output, regular_bytes).unwrap();

        // Open the regular file descriptor first, then simulate a surviving process replacing the
        // path on disk with a symlink to `secret` before `insert_open_file` renames `output`.
        let (file, meta) = open_regular_file_nofollow(&output).unwrap();
        std::fs::remove_file(&output).unwrap();
        std::os::unix::fs::symlink(&secret, &output).unwrap();

        let (id, size) = cache
            .insert_open_file(&output, file, &meta, true, None)
            .unwrap();
        assert_eq!(id, BlobId::of(regular_bytes));
        assert_eq!(size, regular_bytes.len() as u64);
        assert!(!cache.contains(&BlobId::of(secret_bytes)));

        let cached_path = cache.object_path(&id);
        let cached_meta = std::fs::symlink_metadata(&cached_path).unwrap();
        assert!(
            cached_meta.is_file() && !cached_meta.file_type().is_symlink(),
            "cache object must never be a symbolic link"
        );
        assert_eq!(
            cache.read_verified(&id, 1024).unwrap(),
            regular_bytes,
            "cache must store bytes from the open descriptor, not the swapped symlink"
        );
        assert_eq!(
            std::fs::read_dir(&cache.tmp).unwrap().count(),
            0,
            "rejected moved symlink in tmp/ must be cleaned up"
        );
    }

    #[cfg(windows)]
    #[test]
    fn insert_open_file_falls_back_to_descriptor_copy_when_path_is_replaced_before_rename() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache_in(dir.path(), 1 << 20);

        let output = dir.path().join("output.txt");
        let original_bytes = b"legitimate task output";
        let swapped_bytes = b"replaced same-len data";
        assert_eq!(original_bytes.len(), swapped_bytes.len());
        std::fs::write(&output, original_bytes).unwrap();

        // Open the regular file descriptor first (Rust opens with FILE_SHARE_DELETE on Windows),
        // then rename the original path aside and write a different regular file of equal length
        // at `output` before `insert_open_file` renames `output` into `tmp/`.
        let (file, meta) = open_regular_file_nofollow(&output).unwrap();
        let aside = dir.path().join("output-aside.txt");
        std::fs::rename(&output, &aside).unwrap();
        std::fs::write(&output, swapped_bytes).unwrap();

        let (id, size) = cache
            .insert_open_file(&output, file, &meta, true, None)
            .unwrap();
        assert_eq!(id, BlobId::of(original_bytes));
        assert_eq!(size, original_bytes.len() as u64);
        assert!(!cache.contains(&BlobId::of(swapped_bytes)));
        assert_eq!(
            cache.read_verified(&id, 1024).unwrap(),
            original_bytes,
            "cache must store the original descriptor bytes, not the swapped file"
        );
        assert_eq!(
            std::fs::read_dir(&cache.tmp).unwrap().count(),
            0,
            "rejected moved file in tmp/ must be cleaned up"
        );
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
            blobs_dir.join("owners"),
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

        // 3b. A junction at `<base>\cache\blobs\owners` is also refused on open.
        let junc_owners_base = root_tmp.path().join("base_junc_owners");
        let owners_blobs = junc_owners_base.join("cache").join("blobs");
        drop(BlobCache::open(&owners_blobs, 1 << 20).unwrap());
        let owners_dir = owners_blobs.join("owners");
        std::fs::remove_dir(&owners_dir).unwrap();
        make_junction(&owners_dir, &canary_target);
        let err_owners = match BlobCache::open(&owners_blobs, 1 << 20) {
            Ok(_) => panic!("junction at owners must be refused"),
            Err(e) => e,
        };
        assert!(
            err_owners.to_string().contains("reparse point"),
            "{err_owners}"
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

    #[test]
    fn killed_writer_part_is_removed_and_live_writer_part_is_kept_across_processes() {
        if let Some(blobs_dir_os) = std::env::var_os("BUG34_CHILD_BLOBS_DIR") {
            let part_out = std::env::var_os("BUG34_CHILD_PART_FILE")
                .expect("BUG34_CHILD_PART_FILE must be set");
            let ready_out = std::env::var_os("BUG34_CHILD_READY_FILE")
                .expect("BUG34_CHILD_READY_FILE must be set");
            let cache = BlobCache::open(Path::new(&blobs_dir_os), 1 << 20).unwrap();
            let part = cache.temp_path();
            std::fs::write(&part, b"live partial blob").unwrap();
            std::fs::write(&part_out, part.to_string_lossy().as_bytes()).unwrap();
            std::fs::write(&ready_out, b"ready").unwrap();
            // Hold the BlobCache owner lock until parent kills this process.
            for _ in 0..600 {
                std::thread::sleep(Duration::from_millis(100));
            }
            return;
        }

        struct ChildKillGuard(Option<std::process::Child>);
        impl Drop for ChildKillGuard {
            fn drop(&mut self) {
                if let Some(mut c) = self.0.take() {
                    let _ = c.kill();
                    let _ = c.wait();
                }
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let blobs_dir = dir.path().join("blobs");
        let part_file = dir.path().join("child_part_path.txt");
        let ready_file = dir.path().join("child_ready.txt");
        let exe = std::env::current_exe().unwrap();
        let mut guard = ChildKillGuard(Some(
            std::process::Command::new(exe)
                .args([
                    "--exact",
                    "blob_cache::tests::killed_writer_part_is_removed_and_live_writer_part_is_kept_across_processes",
                    "--nocapture",
                ])
                .env("BUG34_CHILD_BLOBS_DIR", &blobs_dir)
                .env("BUG34_CHILD_PART_FILE", &part_file)
                .env("BUG34_CHILD_READY_FILE", &ready_file)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .expect("spawn child writer process"),
        ));

        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while !ready_file.exists() {
            if let Some(status) = guard.0.as_mut().unwrap().try_wait().unwrap() {
                panic!("child writer exited early before signalling ready: {status}");
            }
            assert!(
                std::time::Instant::now() < deadline,
                "child writer did not signal ready within 30s"
            );
            std::thread::sleep(Duration::from_millis(20));
        }

        let part_path_str = std::fs::read_to_string(&part_file).unwrap();
        let child_part = PathBuf::from(part_path_str.trim());
        assert!(
            child_part.is_file(),
            "child .part file must exist while child lives"
        );

        // While the child process lives and holds its owner lock, opening another cache or running
        // the startup sweep must leave the live writer's .part file untouched.
        let live_sibling = BlobCache::open(&blobs_dir, 1 << 20).unwrap();
        sweep_dead_owner_tmp(&blobs_dir);
        assert!(
            child_part.is_file(),
            "live child writer's .part file must not be removed by sibling open/sweep"
        );
        assert_eq!(
            std::fs::read(&child_part).unwrap(),
            b"live partial blob",
            "live child writer's .part file contents must remain intact"
        );
        assert_eq!(
            std::fs::read_dir(blobs_dir.join("owners")).unwrap().count(),
            2,
            "both live owner locks must be present"
        );
        drop(live_sibling);
        assert_eq!(
            std::fs::read_dir(blobs_dir.join("owners")).unwrap().count(),
            1,
            "clean Drop must release only the closed owner lock"
        );
        assert!(child_part.is_file());

        // Hard-kill the child so Drop does not run, leaving an orphaned .part and unlocked .lock.
        let mut child = guard.0.take().unwrap();
        child.kill().unwrap();
        let _ = child.wait().unwrap();
        assert!(
            child_part.is_file(),
            "killed child's orphaned .part file remains until next open/sweep"
        );

        let reopened = BlobCache::open(&blobs_dir, 1 << 20).unwrap();
        assert!(
            !child_part.exists(),
            "orphaned .part file of killed writer must be removed on next open"
        );
        assert_eq!(
            std::fs::read_dir(blobs_dir.join("tmp")).unwrap().count(),
            0,
            "tmp/ must be empty after dead-owner sweep"
        );
        assert_eq!(
            std::fs::read_dir(blobs_dir.join("owners")).unwrap().count(),
            1,
            "only the reopened cache's own lock may remain in owners/"
        );
        drop(reopened);
        assert_eq!(
            std::fs::read_dir(blobs_dir.join("owners")).unwrap().count(),
            0,
            "owners/ must be empty once all caches drop cleanly"
        );
    }

    #[tokio::test]
    async fn startup_sweep_removes_killed_owner_part_and_keeps_live_and_legacy_parts() {
        let root = tempfile::tempdir().unwrap();
        let base_dir = root.path().join("sandboxes");
        let config = crate::sandbox::SandboxConfig::new(&base_dir);
        config.ensure_usable().await.unwrap();
        assert!(
            !base_dir.join("cache").exists(),
            "startup sweep must not create cache directories when none exist"
        );

        let blobs_dir = base_dir.join("cache").join("blobs");
        let live_cache = BlobCache::open(&blobs_dir, 1 << 20).unwrap();
        let live_part = live_cache.temp_path();
        std::fs::write(&live_part, b"live-in-flight").unwrap();

        let dead_owner_id = Uuid::new_v4();
        let dead_lock = blobs_dir
            .join("owners")
            .join(format!("{dead_owner_id}.lock"));
        std::fs::write(&dead_lock, b"").unwrap();
        let dead_part = blobs_dir
            .join("tmp")
            .join(format!("{dead_owner_id}.{}.part", Uuid::new_v4()));
        std::fs::write(&dead_part, b"killed-worker").unwrap();

        let legacy_part = blobs_dir
            .join("tmp")
            .join(format!("{}.part", Uuid::new_v4()));
        std::fs::write(&legacy_part, b"legacy-part").unwrap();

        config.ensure_usable().await.unwrap();
        assert!(
            !dead_part.exists(),
            "startup sweep must remove killed owner's .part file"
        );
        assert!(
            !dead_lock.exists(),
            "startup sweep must remove dead owner's .lock file"
        );
        assert!(
            live_part.is_file(),
            "startup sweep must keep live owner's .part file"
        );
        assert!(
            legacy_part.is_file(),
            "startup sweep must keep fresh legacy unowned .part file"
        );
        drop(live_cache);
        assert_eq!(
            std::fs::read_dir(blobs_dir.join("owners")).unwrap().count(),
            0
        );
    }

    #[test]
    fn legacy_unowned_part_keeps_24h_stale_age_and_orphan_dead_owner_lock_is_swept() {
        let dir = tempfile::tempdir().unwrap();
        let blobs_dir = dir.path().join("blobs");
        let cache1 = BlobCache::open(&blobs_dir, 1 << 20).unwrap();
        let cache2 = BlobCache::open(&blobs_dir, 1 << 20).unwrap();

        let legacy_part = blobs_dir
            .join("tmp")
            .join(format!("{}.part", Uuid::new_v4()));
        std::fs::write(&legacy_part, b"fresh-legacy").unwrap();

        let orphan_dead_id = Uuid::new_v4();
        let orphan_dead_lock = blobs_dir
            .join("owners")
            .join(format!("{orphan_dead_id}.lock"));
        std::fs::write(&orphan_dead_lock, b"").unwrap();

        drop(cache1);
        let cache3 = BlobCache::open(&blobs_dir, 1 << 20).unwrap();
        assert!(
            legacy_part.is_file(),
            "fresh unowned legacy .part file must keep its 24h stale-age protection"
        );
        assert!(
            !orphan_dead_lock.exists(),
            "orphan unlocked dead owner lock file must be swept on open"
        );
        assert_eq!(
            std::fs::read_dir(blobs_dir.join("owners")).unwrap().count(),
            2,
            "live cache2 and cache3 owner locks must remain"
        );
        drop(cache2);
        drop(cache3);
        assert_eq!(
            std::fs::read_dir(blobs_dir.join("owners")).unwrap().count(),
            0
        );
    }
}
