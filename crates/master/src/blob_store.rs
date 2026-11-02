//! The Master's blob store: task inputs and output files, kept on disk by content
//! (`rusty_grid_core::blob`, design record `docs/design/0006-content-addressed-data-plane.md`).
//!
//! * **Layout.** `<dir>/objects/<first two hex digits>/<64 hex digits>` holds one verified blob
//!   per file; uploads in progress live in `<dir>/tmp/` and are renamed into place once their
//!   BLAKE3 hash matches the id they were announced under. `tmp/` is emptied at start-up. The
//!   directories are created (private to the owner) when the first blob arrives, so a master that
//!   never stores a blob leaves no trace on disk.
//! * **Bound.** The store keeps at most `max_bytes` across stored objects and uploads in progress,
//!   evicting the least recently used blobs when a new one is uploaded or stored. Blobs that a
//!   queued or running task needs are pinned and never evicted, so the store can exceed its bound
//!   while pinned blobs alone fill it. Output blobs of finished tasks stay pinned for
//!   `output_pin_retention`, releasing the oldest terminal output pins early when total pinned
//!   output bytes exceed `max_bytes`, but never those of the newest task (so a single task whose
//!   outputs exceed `max_bytes` can also hold the store above its bound during the retention
//!   window). The index lives in memory and is rebuilt from the directory (oldest modification
//!   time first) when the Master starts.
//! * **Trust.** Only authenticated peers reach the store; still, every upload is verified against
//!   its id, every chunk is size-checked, and nothing a peer sends names a path: files are named
//!   by the hex form of their id only.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use rusty_grid_core::blob::{
    join_blob_path, BlobHasher, BlobId, InputSource, TaskInput, TreeManifest, MAX_MANIFEST_BYTES,
};
use rusty_grid_core::blob_transfer::{
    check_chunk_len, BlobReply, BlobRequest, BLOB_CHUNK_BYTES, MAX_MISSING_QUERY,
};
use rusty_grid_core::task::{Bytes, TaskId};

/// Default bound of the blob store: 4 GiB.
pub const DEFAULT_BLOB_STORE_MAX_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// An upload that has not received a chunk for this long is abandoned and its file removed.
pub const STALE_UPLOAD_AFTER: Duration = Duration::from_secs(600);

/// How long output blobs of a finished task stay pinned so the client can download them: 10 minutes.
pub const DEFAULT_OUTPUT_PIN_RETENTION: Duration = Duration::from_secs(600);

/// Maximum number of evicted blob files deleted per lock hold.
const EVICTION_BATCH_SIZE: usize = 64;

/// Where the blob store lives and how large it may grow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobStoreConfig {
    pub dir: PathBuf,
    pub max_bytes: u64,
    pub output_pin_retention: Duration,
}

impl BlobStoreConfig {
    pub fn new(dir: impl Into<PathBuf>, max_bytes: u64) -> Self {
        Self {
            dir: dir.into(),
            max_bytes,
            output_pin_retention: DEFAULT_OUTPUT_PIN_RETENTION,
        }
    }

    pub fn with_output_pin_retention(mut self, retention: Duration) -> Self {
        self.output_pin_retention = retention;
        self
    }
}

/// Counters describing the store, for the dashboard, logs and tests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct BlobStoreStats {
    /// Blobs held.
    pub blobs: usize,
    /// Bytes held.
    pub bytes: u64,
    /// The configured bound.
    pub max_bytes: u64,
    /// Blobs pinned by queued or running tasks.
    pub pinned_blobs: usize,
    /// Uploads in progress.
    pub uploads: usize,
    /// Blobs stored from uploads since start-up.
    pub blobs_received: u64,
    /// Bytes of blob content stored from uploads since start-up.
    pub bytes_received: u64,
    /// Chunks sent to readers since start-up.
    pub chunks_served: u64,
    /// Bytes of blob content sent to readers since start-up.
    pub bytes_served: u64,
    /// Blobs evicted to stay within the bound since start-up.
    pub evictions: u64,
}

struct Entry {
    size: u64,
    tick: u64,
}

struct Upload {
    tmp: PathBuf,
    file: File,
    hasher: BlobHasher,
    received: u64,
    total: u64,
    last_chunk: Instant,
}

enum UploadSlot {
    Idle(Box<Upload>),
    Writing { total: u64 },
}

impl UploadSlot {
    fn total(&self) -> u64 {
        match self {
            Self::Idle(u) => u.total,
            Self::Writing { total } => *total,
        }
    }
}

struct OutputPinEntry {
    pinned_at: Instant,
    seq: u64,
    ids: Vec<BlobId>,
}

#[derive(Default)]
struct Inner {
    entries: HashMap<BlobId, Entry>,
    /// Least recently used first.
    lru: BTreeMap<u64, BlobId>,
    tick: u64,
    total_bytes: u64,
    upload_bytes: u64,
    pins: HashMap<TaskId, (Instant, Vec<BlobId>)>,
    output_pins: HashMap<TaskId, OutputPinEntry>,
    output_pin_counts: HashMap<BlobId, u32>,
    output_pin_seq: u64,
    pin_counts: HashMap<BlobId, u32>,
    uploads: HashMap<(u64, BlobId), UploadSlot>,
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
        if self.entries.contains_key(&id) {
            self.touch(&id);
            return;
        }
        self.tick += 1;
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

    fn remove(&mut self, id: &BlobId) -> Option<u64> {
        let entry = self.entries.remove(id)?;
        self.lru.remove(&entry.tick);
        self.total_bytes -= entry.size;
        Some(entry.size)
    }

    fn output_pinned_bytes(&self) -> u64 {
        self.output_pin_counts
            .keys()
            .filter_map(|id| self.entries.get(id).map(|e| e.size))
            .sum()
    }

    /// Unpinned blobs to drop, least recently used first, until the store fits `max_bytes`.
    /// Never drops `keep` (the blob just committed).
    fn eviction_victims(&mut self, max_bytes: u64, keep: Option<&BlobId>) -> Vec<BlobId> {
        let mut victims = Vec::new();
        let used = self.total_bytes.saturating_add(self.upload_bytes);
        let mut excess = used.saturating_sub(max_bytes);
        if excess == 0 {
            return victims;
        }
        for id in self.lru.values() {
            if excess == 0 {
                break;
            }
            if self.pin_counts.contains_key(id) || keep == Some(id) {
                continue;
            }
            let size = self.entries[id].size;
            excess = excess.saturating_sub(size.max(1));
            victims.push(*id);
        }
        for id in &victims {
            self.remove(id);
        }
        victims
    }
}

/// The bounded, content-addressed blob store of a Master. Shared as `Arc<BlobStore>`.
pub struct BlobStore {
    objects: PathBuf,
    tmp: PathBuf,
    max_bytes: u64,
    output_pin_retention: Duration,
    inner: Mutex<Inner>,
    next_owner: AtomicU64,
    blobs_received: AtomicU64,
    bytes_received: AtomicU64,
    chunks_served: AtomicU64,
    bytes_served: AtomicU64,
    evictions: AtomicU64,
}

fn object_path(objects: &Path, id: &BlobId) -> PathBuf {
    let hex = id.to_hex();
    objects.join(&hex[..2]).join(hex)
}

fn failed(message: impl Into<String>) -> BlobReply {
    BlobReply::Failed {
        message: message.into(),
    }
}

impl BlobStore {
    /// Opens (creating it if needed) the store described by `config`: empties `tmp/`, indexes
    /// `objects/`, and evicts down to the bound if the directory holds more.
    pub fn open(config: &BlobStoreConfig) -> io::Result<Arc<Self>> {
        let objects = config.dir.join("objects");
        let tmp = config.dir.join("tmp");
        if tmp.exists() {
            std::fs::remove_dir_all(&tmp)?;
        }

        let mut found = Vec::new();
        let fanouts = match std::fs::read_dir(&objects) {
            Ok(entries) => entries.collect::<Vec<_>>(),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e),
        };
        for fanout in fanouts {
            let fanout = fanout?;
            if !fanout.file_type()?.is_dir() {
                continue;
            }
            for object in std::fs::read_dir(fanout.path())? {
                let object = object?;
                let meta = object.metadata()?;
                if !meta.is_file() {
                    continue;
                }
                let Some(id) = object
                    .file_name()
                    .to_str()
                    .and_then(|name| BlobId::from_hex(name).ok())
                else {
                    continue;
                };
                let modified = meta.modified().unwrap_or(std::time::UNIX_EPOCH);
                found.push((modified, id, meta.len()));
            }
        }
        found.sort();

        let mut inner = Inner::default();
        for (_, id, size) in found {
            inner.insert(id, size);
        }
        let victims = inner.eviction_victims(config.max_bytes, None);
        for id in &victims {
            let _ = std::fs::remove_file(object_path(&objects, id));
        }
        info!(
            dir = %config.dir.display(),
            blobs = inner.entries.len(),
            bytes = inner.total_bytes,
            max_bytes = config.max_bytes,
            evicted = victims.len(),
            "Blob store ready"
        );
        Ok(Arc::new(Self {
            objects,
            tmp,
            max_bytes: config.max_bytes,
            output_pin_retention: config.output_pin_retention,
            inner: Mutex::new(inner),
            next_owner: AtomicU64::new(1),
            blobs_received: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            chunks_served: AtomicU64::new(0),
            bytes_served: AtomicU64::new(0),
            evictions: AtomicU64::new(victims.len() as u64),
        }))
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The bound in bytes.
    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// How long output blobs of a finished task stay pinned for client downloads.
    pub fn output_pin_retention(&self) -> Duration {
        self.output_pin_retention
    }

    /// A fresh token naming one connection's uploads ([`BlobStore::abort_uploads`] drops them).
    pub fn new_owner(&self) -> u64 {
        self.next_owner.fetch_add(1, Ordering::Relaxed)
    }

    /// Whether the store holds `id`.
    pub fn contains(&self, id: &BlobId) -> bool {
        self.lock().entries.contains_key(id)
    }

    /// The size of blob `id`, if the store holds it.
    pub fn size_of(&self, id: &BlobId) -> Option<u64> {
        self.lock().entries.get(id).map(|e| e.size)
    }

    pub fn stats(&self) -> BlobStoreStats {
        let inner = self.lock();
        BlobStoreStats {
            blobs: inner.entries.len(),
            bytes: inner.total_bytes,
            max_bytes: self.max_bytes,
            pinned_blobs: inner.pin_counts.len(),
            uploads: inner.uploads.len(),
            blobs_received: self.blobs_received.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            chunks_served: self.chunks_served.load(Ordering::Relaxed),
            bytes_served: self.bytes_served.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }

    /// Answers one request of the connection `owner` (from [`BlobStore::new_owner`]).
    pub async fn handle(self: &Arc<Self>, owner: u64, request: BlobRequest) -> BlobReply {
        match request {
            BlobRequest::Missing { ids } => self.missing(&ids),
            BlobRequest::Read {
                id,
                offset,
                max_len,
            } => self.read_chunk(id, offset, max_len).await,
            BlobRequest::Write {
                id,
                total_size,
                offset,
                data,
            } => self.write_chunk(owner, id, total_size, offset, data).await,
        }
    }

    /// Answers a `Missing` request: the ids among `ids` the store lacks.
    pub fn missing(&self, ids: &[BlobId]) -> BlobReply {
        if ids.len() > MAX_MISSING_QUERY {
            return failed(format!(
                "a Missing request may name at most {MAX_MISSING_QUERY} blobs, this one names {}",
                ids.len()
            ));
        }
        BlobReply::Missing {
            ids: self.missing_ids(ids),
        }
    }

    /// The ids among `ids` the store lacks, in order and without repeats; the ones it holds
    /// count as just used.
    pub fn missing_ids(&self, ids: &[BlobId]) -> Vec<BlobId> {
        let mut inner = self.lock();
        let mut seen = HashSet::with_capacity(ids.len());
        ids.iter()
            .filter(|id| seen.insert(**id))
            .filter(|id| !inner.touch(id))
            .copied()
            .collect()
    }

    async fn read_chunk(self: &Arc<Self>, id: BlobId, offset: u64, max_len: u32) -> BlobReply {
        let Some(size) = ({
            let mut inner = self.lock();
            inner.touch(&id);
            inner.entries.get(&id).map(|e| e.size)
        }) else {
            return BlobReply::NotFound { id };
        };
        if offset > size {
            return failed(format!(
                "offset {offset} is past the end of blob {id} ({size} bytes)"
            ));
        }
        let len = (size - offset)
            .min(u64::from(max_len))
            .min(BLOB_CHUNK_BYTES as u64) as usize;
        let path = object_path(&self.objects, &id);
        let read = tokio::task::spawn_blocking(move || -> io::Result<Vec<u8>> {
            let mut file = File::open(&path)?;
            file.seek(SeekFrom::Start(offset))?;
            let mut buf = vec![0u8; len];
            file.read_exact(&mut buf)?;
            Ok(buf)
        })
        .await;
        match read {
            Ok(Ok(data)) => {
                self.chunks_served.fetch_add(1, Ordering::Relaxed);
                self.bytes_served
                    .fetch_add(data.len() as u64, Ordering::Relaxed);
                BlobReply::Chunk {
                    id,
                    offset,
                    total_size: size,
                    data: Bytes::from(data),
                }
            }
            Ok(Err(e)) if e.kind() == io::ErrorKind::NotFound => {
                // Removed behind the store's back: forget it, the client can upload it again.
                warn!(blob = %id, "Blob vanished from the store directory");
                self.lock().remove(&id);
                BlobReply::NotFound { id }
            }
            Ok(Err(e)) => failed(format!("reading blob {id} failed: {e}")),
            Err(e) => failed(format!("reading blob {id} failed: {e}")),
        }
    }

    async fn write_chunk(
        self: &Arc<Self>,
        owner: u64,
        id: BlobId,
        total_size: u64,
        offset: u64,
        data: Bytes,
    ) -> BlobReply {
        if total_size > self.max_bytes {
            return failed(format!(
                "blob {id} has {total_size} bytes, more than the master's whole blob store \
                 ({} bytes); raise `master --blob-store-mb`",
                self.max_bytes
            ));
        }
        let (upload, need_evict) = {
            let mut inner = self.lock();
            if inner.touch(&id) {
                // Already here (another peer uploaded it, or it was never missing).
                if let Some(stale) = inner.uploads.remove(&(owner, id)) {
                    inner.upload_bytes = inner.upload_bytes.saturating_sub(stale.total());
                    if let UploadSlot::Idle(u) = stale {
                        let _ = std::fs::remove_file(&u.tmp);
                    }
                }
                return BlobReply::Written {
                    id,
                    received: total_size,
                    complete: true,
                };
            }
            let existing = inner.uploads.remove(&(owner, id));
            match existing {
                Some(UploadSlot::Writing { total }) => {
                    inner
                        .uploads
                        .insert((owner, id), UploadSlot::Writing { total });
                    return failed(format!(
                        "an upload of blob {id} is already writing a chunk on this connection"
                    ));
                }
                Some(UploadSlot::Idle(upload)) if offset != 0 => {
                    inner.uploads.insert(
                        (owner, id),
                        UploadSlot::Writing {
                            total: upload.total,
                        },
                    );
                    (Some(*upload), false)
                }
                Some(UploadSlot::Idle(stale)) => {
                    // Starting over: drop the old attempt.
                    inner.upload_bytes = inner.upload_bytes.saturating_sub(stale.total);
                    let _ = std::fs::remove_file(&stale.tmp);
                    if inner.upload_bytes.saturating_add(total_size) > self.max_bytes {
                        return failed(format!(
                            "cannot upload blob {id} ({total_size} bytes): uploads in progress \
                             already reserve {} of {} bytes; raise `master --blob-store-mb`",
                            inner.upload_bytes, self.max_bytes
                        ));
                    }
                    inner.upload_bytes = inner.upload_bytes.saturating_add(total_size);
                    inner
                        .uploads
                        .insert((owner, id), UploadSlot::Writing { total: total_size });
                    (None, true)
                }
                None if offset == 0 => {
                    if inner.upload_bytes.saturating_add(total_size) > self.max_bytes {
                        return failed(format!(
                            "cannot upload blob {id} ({total_size} bytes): uploads in progress \
                             already reserve {} of {} bytes; raise `master --blob-store-mb`",
                            inner.upload_bytes, self.max_bytes
                        ));
                    }
                    inner.upload_bytes = inner.upload_bytes.saturating_add(total_size);
                    inner
                        .uploads
                        .insert((owner, id), UploadSlot::Writing { total: total_size });
                    (None, true)
                }
                None => {
                    return failed(format!(
                        "no upload of blob {id} in progress here; start at offset 0"
                    ))
                }
            }
        };

        let store = Arc::clone(self);
        let outcome = tokio::task::spawn_blocking(move || {
            if need_evict {
                store.evict_to_fit(None);
            }
            store.append(owner, id, total_size, offset, data.as_bytes(), upload)
        })
        .await;
        match outcome {
            Ok(reply) => reply,
            Err(e) => {
                let mut inner = self.lock();
                if let Some(slot) = inner.uploads.remove(&(owner, id)) {
                    inner.upload_bytes = inner.upload_bytes.saturating_sub(slot.total());
                }
                failed(format!("storing blob {id} failed: {e}"))
            }
        }
    }

    /// Writes one chunk (blocking I/O): creates the upload on the first chunk, puts it back in
    /// the table while it is incomplete, and commits it when the last byte arrived.
    fn append(
        &self,
        owner: u64,
        id: BlobId,
        total_size: u64,
        offset: u64,
        data: &[u8],
        upload: Option<Upload>,
    ) -> BlobReply {
        let abandon_slot = |this: &Self| {
            let mut inner = this.lock();
            if let Some(slot) = inner.uploads.remove(&(owner, id)) {
                inner.upload_bytes = inner.upload_bytes.saturating_sub(slot.total());
            }
        };
        let mut upload = match upload {
            Some(upload) => upload,
            None => {
                let tmp = self.tmp.join(format!("{}.{owner}.part", id.to_hex()));
                match create_private_dir_all(&self.tmp).and_then(|()| File::create(&tmp)) {
                    Ok(file) => Upload {
                        tmp,
                        file,
                        hasher: BlobHasher::new(),
                        received: 0,
                        total: total_size,
                        last_chunk: Instant::now(),
                    },
                    Err(e) => {
                        abandon_slot(self);
                        return failed(format!("cannot store blob {id}: {e}"));
                    }
                }
            }
        };
        let abandon = |upload: Upload, reply: BlobReply| {
            abandon_slot(self);
            drop(upload.file);
            let _ = std::fs::remove_file(&upload.tmp);
            reply
        };
        if upload.total != total_size {
            let reply = failed(format!(
                "blob {id} was announced with {} bytes, now with {total_size}",
                upload.total
            ));
            return abandon(upload, reply);
        }
        if offset != upload.received {
            let reply = failed(format!(
                "chunk of blob {id} at offset {offset}, but {} bytes were received so far",
                upload.received
            ));
            return abandon(upload, reply);
        }
        if let Err(e) = check_chunk_len(data.len(), total_size - upload.received) {
            return abandon(upload, failed(format!("blob {id}: {e}")));
        }
        if let Err(e) = upload.file.write_all(data) {
            return abandon(upload, failed(format!("cannot store blob {id}: {e}")));
        }
        upload.hasher.update(data);
        upload.received += data.len() as u64;
        upload.last_chunk = Instant::now();

        if upload.received < total_size {
            let received = upload.received;
            let mut inner = self.lock();
            if let Some(slot @ UploadSlot::Writing { .. }) = inner.uploads.get_mut(&(owner, id)) {
                *slot = UploadSlot::Idle(Box::new(upload));
                return BlobReply::Written {
                    id,
                    received,
                    complete: false,
                };
            }
            drop(inner);
            drop(upload.file);
            let _ = std::fs::remove_file(&upload.tmp);
            return failed(format!("upload of blob {id} was aborted"));
        }
        self.commit(owner, id, upload)
    }

    fn commit(&self, owner: u64, id: BlobId, upload: Upload) -> BlobReply {
        let abandon_slot = |this: &Self| {
            let mut inner = this.lock();
            if let Some(slot) = inner.uploads.remove(&(owner, id)) {
                inner.upload_bytes = inner.upload_bytes.saturating_sub(slot.total());
            }
        };
        let actual = upload.hasher.finish();
        if actual != id {
            abandon_slot(self);
            drop(upload.file);
            let _ = std::fs::remove_file(&upload.tmp);
            return failed(format!(
                "the uploaded content of blob {id} hashes to {actual} (did the file change during the upload?)"
            ));
        }
        if let Err(e) = upload.file.sync_all() {
            abandon_slot(self);
            drop(upload.file);
            let _ = std::fs::remove_file(&upload.tmp);
            return failed(format!("cannot store blob {id}: {e}"));
        }
        drop(upload.file);
        let dest = object_path(&self.objects, &id);
        if let Some(parent) = dest.parent() {
            if let Err(e) = create_private_dir_all(parent) {
                abandon_slot(self);
                let _ = std::fs::remove_file(&upload.tmp);
                return failed(format!("cannot store blob {id}: {e}"));
            }
        }
        {
            let mut inner = self.lock();
            let Some(slot) = inner.uploads.remove(&(owner, id)) else {
                drop(inner);
                let _ = std::fs::remove_file(&upload.tmp);
                return failed(format!("upload of blob {id} was aborted"));
            };
            inner.upload_bytes = inner.upload_bytes.saturating_sub(slot.total());
            if let Err(e) = std::fs::rename(&upload.tmp, &dest) {
                let _ = std::fs::remove_file(&upload.tmp);
                if !dest.exists() {
                    return failed(format!("cannot store blob {id}: {e}"));
                }
            }
            inner.insert(id, upload.total);
        }
        self.blobs_received.fetch_add(1, Ordering::Relaxed);
        self.bytes_received
            .fetch_add(upload.total, Ordering::Relaxed);
        self.evict_to_fit(Some(&id));
        debug!(blob = %id, bytes = upload.total, "Blob stored");
        BlobReply::Written {
            id,
            received: upload.total,
            complete: true,
        }
    }

    /// Evicts least recently used unpinned blobs, at most [`EVICTION_BATCH_SIZE`] files per lock
    /// hold, until `total_bytes + upload_bytes <= max_bytes`. Never drops `keep`.
    fn evict_to_fit(&self, keep: Option<&BlobId>) -> usize {
        let mut total_evicted = 0;
        loop {
            let batch = {
                let mut inner = self.lock();
                let used = inner.total_bytes.saturating_add(inner.upload_bytes);
                let mut excess = used.saturating_sub(self.max_bytes);
                if excess == 0 {
                    break;
                }
                let mut victims = Vec::with_capacity(EVICTION_BATCH_SIZE);
                for id in inner.lru.values() {
                    if excess == 0 || victims.len() >= EVICTION_BATCH_SIZE {
                        break;
                    }
                    if inner.pin_counts.contains_key(id) || keep == Some(id) {
                        continue;
                    }
                    let size = inner.entries[id].size;
                    excess = excess.saturating_sub(size.max(1));
                    victims.push(*id);
                }
                if victims.is_empty() {
                    break;
                }
                for id in &victims {
                    inner.remove(id);
                    if let Err(e) = std::fs::remove_file(object_path(&self.objects, id)) {
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
            self.evictions
                .fetch_add(total_evicted as u64, Ordering::Relaxed);
            info!(
                evicted = total_evicted,
                "Blob store over its bound; evicted the least recently used blobs"
            );
        }
        total_evicted
    }

    /// Stores `data` as a blob (for in-process callers such as tests); returns its id.
    pub async fn put(self: &Arc<Self>, data: &[u8]) -> Result<BlobId, String> {
        let id = BlobId::of(data);
        let owner = self.new_owner();
        let mut offset = 0usize;
        loop {
            let end = (offset + BLOB_CHUNK_BYTES).min(data.len());
            match self
                .write_chunk(
                    owner,
                    id,
                    data.len() as u64,
                    offset as u64,
                    Bytes::copy_from_slice(&data[offset..end]),
                )
                .await
            {
                BlobReply::Written { complete: true, .. } => return Ok(id),
                BlobReply::Written { .. } => offset = end,
                BlobReply::Failed { message } => return Err(message),
                other => return Err(format!("unexpected reply {other:?}")),
            }
        }
    }

    /// Reads a whole blob of at most `limit` bytes (directory manifests).
    pub async fn read_all(self: &Arc<Self>, id: BlobId, limit: u64) -> Result<Vec<u8>, String> {
        let size = self
            .size_of(&id)
            .ok_or_else(|| format!("blob {id} is not on the master"))?;
        if size > limit {
            return Err(format!(
                "blob {id} has {size} bytes, more than the {limit} allowed here"
            ));
        }
        let path = object_path(&self.objects, &id);
        tokio::task::spawn_blocking(move || std::fs::read(path))
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| format!("reading blob {id} failed: {e}"))
    }

    /// Drops every upload of connection `owner` that has not completed (the connection ended).
    pub fn abort_uploads(&self, owner: u64) {
        let dropped: Vec<Upload> = {
            let mut inner = self.lock();
            let keys: Vec<_> = inner
                .uploads
                .keys()
                .filter(|(o, _)| *o == owner)
                .copied()
                .collect();
            keys.into_iter()
                .filter_map(|key| {
                    let slot = inner.uploads.remove(&key)?;
                    inner.upload_bytes = inner.upload_bytes.saturating_sub(slot.total());
                    match slot {
                        UploadSlot::Idle(upload) => Some(*upload),
                        UploadSlot::Writing { .. } => None,
                    }
                })
                .collect()
        };
        for upload in dropped {
            drop(upload.file);
            let _ = std::fs::remove_file(&upload.tmp);
        }
    }

    /// Drops uploads that received nothing for [`STALE_UPLOAD_AFTER`].
    pub fn sweep_stale_uploads(&self) {
        let stale: Vec<Upload> = {
            let mut inner = self.lock();
            let keys: Vec<_> = inner
                .uploads
                .iter()
                .filter_map(|(k, slot)| match slot {
                    UploadSlot::Idle(u) if u.last_chunk.elapsed() > STALE_UPLOAD_AFTER => Some(*k),
                    _ => None,
                })
                .collect();
            keys.into_iter()
                .filter_map(|key| match inner.uploads.remove(&key)? {
                    UploadSlot::Idle(u) => {
                        inner.upload_bytes = inner.upload_bytes.saturating_sub(u.total);
                        Some(*u)
                    }
                    UploadSlot::Writing { .. } => None,
                })
                .collect()
        };
        for upload in stale {
            warn!(tmp = %upload.tmp.display(), "Abandoning a blob upload that stalled");
            drop(upload.file);
            let _ = std::fs::remove_file(&upload.tmp);
        }
    }

    /// Checks that every blob `inputs` need is here: each file blob, each tree manifest and every
    /// file it lists, with the announced sizes. Returns all of their ids.
    pub async fn resolve_inputs(
        self: &Arc<Self>,
        inputs: &[TaskInput],
    ) -> Result<Vec<BlobId>, String> {
        let mut ids = Vec::new();
        for input in inputs {
            match &input.source {
                InputSource::File { id, size, .. } => {
                    self.check_present(*id, *size, &input.path)?;
                    ids.push(*id);
                }
                InputSource::Tree {
                    manifest,
                    manifest_size,
                } => {
                    self.check_present(*manifest, *manifest_size, &input.path)?;
                    let bytes = self.read_all(*manifest, MAX_MANIFEST_BYTES).await?;
                    let tree = TreeManifest::decode(&bytes)
                        .map_err(|e| format!("input '{}': {e}", input.path))?;
                    ids.push(*manifest);
                    for file in tree.files() {
                        self.check_present(
                            file.id,
                            file.size,
                            &join_blob_path(&input.path, &file.path),
                        )?;
                        ids.push(file.id);
                    }
                }
            }
        }
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    fn check_present(&self, id: BlobId, size: u64, path: &str) -> Result<(), String> {
        match self.size_of(&id) {
            Some(stored) if stored == size => Ok(()),
            Some(stored) => Err(format!(
                "input '{path}': blob {id} has {stored} bytes on the master, the task says {size}"
            )),
            None => Err(format!(
                "input '{path}': blob {id} is not on the master; upload the inputs before \
                 submitting (it may also have been evicted: raise `master --blob-store-mb`)"
            )),
        }
    }

    /// Keeps `ids` from being evicted until [`BlobStore::unpin_task`] is called for `task_id`.
    /// Returns the ids that are no longer in the store (evicted since they were checked).
    pub fn pin_task(&self, task_id: TaskId, ids: Vec<BlobId>) -> Vec<BlobId> {
        let mut inner = self.lock();
        let gone: Vec<BlobId> = ids
            .iter()
            .filter(|id| !inner.entries.contains_key(id))
            .copied()
            .collect();
        for id in &ids {
            *inner.pin_counts.entry(*id).or_insert(0) += 1;
        }
        if let Some((_, previous)) = inner.pins.insert(task_id, (Instant::now(), ids)) {
            release_pins(&mut inner, previous);
        }
        gone
    }

    /// Releases the pins of `task_id` (it finished, failed or was cancelled).
    pub async fn unpin_task(self: &Arc<Self>, task_id: &TaskId) {
        let need_evict = {
            let mut inner = self.lock();
            if let Some((_, ids)) = inner.pins.remove(task_id) {
                release_pins(&mut inner, ids);
                inner.total_bytes.saturating_add(inner.upload_bytes) > self.max_bytes
            } else {
                false
            }
        };
        if need_evict {
            let store = Arc::clone(self);
            let _ = tokio::task::spawn_blocking(move || store.evict_to_fit(None)).await;
        }
    }

    /// Tasks that currently pin blobs, with when they pinned them.
    pub fn pinned_tasks(&self) -> Vec<(TaskId, Instant)> {
        self.lock()
            .pins
            .iter()
            .map(|(task_id, (pinned_at, _))| (*task_id, *pinned_at))
            .collect()
    }

    /// Checks that every blob in `ids` is in the store (marking each as recently used) and pins
    /// them atomically under one lock acquisition. If any blob in `ids` is absent, nothing is
    /// pinned and the missing ids are returned in `Err`.
    pub fn check_and_pin_outputs(
        self: &Arc<Self>,
        ids: &[BlobId],
    ) -> Result<OutputPinGuard, Vec<BlobId>> {
        let mut inner = self.lock();
        let mut unique = Vec::with_capacity(ids.len());
        let mut seen = HashSet::with_capacity(ids.len());
        let mut missing = Vec::new();
        for &id in ids {
            if !seen.insert(id) {
                continue;
            }
            if inner.touch(&id) {
                unique.push(id);
            } else {
                missing.push(id);
            }
        }
        if !missing.is_empty() {
            return Err(missing);
        }
        for &id in &unique {
            *inner.pin_counts.entry(id).or_insert(0) += 1;
        }
        Ok(OutputPinGuard {
            store: Arc::clone(self),
            ids: unique,
        })
    }

    /// Releases output pins of terminal tasks that have been held for at least
    /// [`BlobStore::output_pin_retention`].
    pub async fn sweep_expired_output_pins(self: &Arc<Self>) {
        let need_evict = {
            let mut inner = self.lock();
            let expired: Vec<TaskId> = inner
                .output_pins
                .iter()
                .filter(|(_, entry)| entry.pinned_at.elapsed() >= self.output_pin_retention)
                .map(|(task_id, _)| *task_id)
                .collect();
            if expired.is_empty() {
                false
            } else {
                for task_id in expired {
                    if let Some(entry) = inner.output_pins.remove(&task_id) {
                        release_output_pins(&mut inner, entry.ids);
                    }
                }
                inner.total_bytes.saturating_add(inner.upload_bytes) > self.max_bytes
            }
        };
        if need_evict {
            let store = Arc::clone(self);
            let _ = tokio::task::spawn_blocking(move || store.evict_to_fit(None)).await;
        }
    }
}

/// Temporary pin on a task result's output blobs, held from the atomic check in
/// [`BlobStore::check_and_pin_outputs`] until [`OutputPinGuard::commit_terminal`] binds it to
/// the terminal task (or until dropped on a stale or retried result).
pub struct OutputPinGuard {
    store: Arc<BlobStore>,
    ids: Vec<BlobId>,
}

impl OutputPinGuard {
    /// Binds the pinned output blobs to `task_id` for [`BlobStore::output_pin_retention`] after
    /// the task reached a terminal state. When total pinned output bytes exceed `max_bytes`,
    /// releases the oldest terminal output pins early, never those of `task_id`.
    pub async fn commit_terminal(mut self, task_id: TaskId) {
        let ids = std::mem::take(&mut self.ids);
        if ids.is_empty() {
            return;
        }
        let need_evict = {
            let mut inner = self.store.lock();
            for &id in &ids {
                *inner.output_pin_counts.entry(id).or_insert(0) += 1;
            }
            inner.output_pin_seq += 1;
            let seq = inner.output_pin_seq;
            if let Some(previous) = inner.output_pins.insert(
                task_id,
                OutputPinEntry {
                    pinned_at: Instant::now(),
                    seq,
                    ids,
                },
            ) {
                release_output_pins(&mut inner, previous.ids);
            }
            while inner.output_pins.len() > 1 && inner.output_pinned_bytes() > self.store.max_bytes
            {
                let Some(oldest_task) = inner
                    .output_pins
                    .iter()
                    .filter(|(&tid, _)| tid != task_id)
                    .min_by_key(|(_, entry)| entry.seq)
                    .map(|(&tid, _)| tid)
                else {
                    break;
                };
                if let Some(removed) = inner.output_pins.remove(&oldest_task) {
                    release_output_pins(&mut inner, removed.ids);
                }
            }
            inner.total_bytes.saturating_add(inner.upload_bytes) > self.store.max_bytes
        };
        if need_evict {
            let store = Arc::clone(&self.store);
            let _ = tokio::task::spawn_blocking(move || store.evict_to_fit(None)).await;
        }
    }
}

impl Drop for OutputPinGuard {
    fn drop(&mut self) {
        let ids = std::mem::take(&mut self.ids);
        if !ids.is_empty() {
            let mut inner = self.store.lock();
            release_pins(&mut inner, ids);
        }
    }
}

/// Creates `dir` and its missing parents readable by the owner only (Unix): blobs are the
/// clients' files.
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

fn release_pins(inner: &mut Inner, ids: Vec<BlobId>) {
    for id in ids {
        if let Some(count) = inner.pin_counts.get_mut(&id) {
            *count -= 1;
            if *count == 0 {
                inner.pin_counts.remove(&id);
            }
        }
    }
}

fn release_output_pins(inner: &mut Inner, ids: Vec<BlobId>) {
    for &id in &ids {
        if let Some(count) = inner.output_pin_counts.get_mut(&id) {
            *count -= 1;
            if *count == 0 {
                inner.output_pin_counts.remove(&id);
            }
        }
    }
    release_pins(inner, ids);
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::blob::BlobFile;
    use rusty_grid_core::blob_transfer::{
        download_blob, missing_blobs, upload_blob, BlobChannel, BlobError,
    };

    struct Conn {
        store: Arc<BlobStore>,
        owner: u64,
    }

    impl BlobChannel for Conn {
        async fn call(&self, request: BlobRequest) -> Result<BlobReply, BlobError> {
            Ok(self.store.handle(self.owner, request).await)
        }
    }

    fn conn(store: &Arc<BlobStore>) -> Conn {
        Conn {
            store: Arc::clone(store),
            owner: store.new_owner(),
        }
    }

    fn open(dir: &Path, max_bytes: u64) -> Arc<BlobStore> {
        BlobStore::open(&BlobStoreConfig::new(dir, max_bytes)).unwrap()
    }

    #[tokio::test]
    async fn uploads_are_verified_stored_by_id_and_survive_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path(), 1 << 30);
        let data: Vec<u8> = (0..(BLOB_CHUNK_BYTES + 1000))
            .map(|i| (i % 253) as u8)
            .collect();
        let id = BlobId::of(&data);
        let c = conn(&store);

        assert_eq!(missing_blobs(&c, &[id, id]).await.unwrap(), vec![id]);
        upload_blob(&c, id, data.len() as u64, &data[..])
            .await
            .unwrap();
        assert!(store.contains(&id));
        let on_disk = dir
            .path()
            .join("objects")
            .join(&id.to_hex()[..2])
            .join(id.to_hex());
        assert_eq!(std::fs::read(&on_disk).unwrap(), data);
        assert_eq!(store.stats().blobs_received, 1);

        drop(store);
        let reopened = open(dir.path(), 1 << 30);
        assert_eq!(reopened.size_of(&id), Some(data.len() as u64));
        let mut out = Vec::new();
        download_blob(&conn(&reopened), id, Some(data.len() as u64), &mut out)
            .await
            .unwrap();
        assert_eq!(out, data);
    }

    #[tokio::test]
    async fn the_store_creates_nothing_until_a_blob_arrives_and_keeps_it_private() {
        let home = tempfile::tempdir().unwrap();
        let dir = home.path().join(".oxideswarm").join("blobs");
        let store = open(&dir, 1 << 20);
        assert!(!home.path().join(".oxideswarm").exists());
        assert!(matches!(
            store.missing(&[BlobId::of(b"x")]),
            BlobReply::Missing { .. }
        ));
        assert!(!dir.exists(), "queries create nothing");
        store.put(b"first blob").await.unwrap();
        assert!(dir.join("objects").is_dir());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o700, "other users must not read the clients' files");
        }
    }

    #[tokio::test]
    async fn a_wrong_hash_or_an_out_of_order_chunk_stores_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path(), 1 << 30);
        let c = conn(&store);
        let lie = BlobId::of(b"the announced content");
        let err = upload_blob(&c, lie, 5, &b"other"[..]).await.unwrap_err();
        assert!(err.to_string().contains("hashes to"), "{err}");
        assert!(!store.contains(&lie));

        let reply = store
            .handle(
                c.owner,
                BlobRequest::Write {
                    id: lie,
                    total_size: 10,
                    offset: 5,
                    data: Bytes::from(vec![0u8; 5]),
                },
            )
            .await;
        assert!(matches!(reply, BlobReply::Failed { .. }), "{reply:?}");
        assert_eq!(
            std::fs::read_dir(dir.path().join("tmp")).unwrap().count(),
            0
        );
    }

    #[tokio::test]
    async fn the_bound_evicts_least_recently_used_but_never_pinned_blobs() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path(), 3000);
        let blob = |n: u8| vec![n; 1000];
        let a = store.put(&blob(1)).await.unwrap();
        let b = store.put(&blob(2)).await.unwrap();
        let c = store.put(&blob(3)).await.unwrap();
        assert_eq!(store.pin_task(TaskId::new(), vec![a]), Vec::<BlobId>::new());
        // `b` is now the least recently used unpinned blob.
        let d = store.put(&blob(4)).await.unwrap();
        assert!(store.contains(&a), "pinned blobs are never evicted");
        assert!(!store.contains(&b));
        assert!(store.contains(&c) && store.contains(&d));
        assert_eq!(store.stats().evictions, 1);

        // Using a blob makes it recent: `c` survives the next eviction, `d` does not.
        let _ = store.missing(&[c]);
        let e = store.put(&blob(5)).await.unwrap();
        assert!(store.contains(&c) && store.contains(&e) && !store.contains(&d));

        let too_big = vec![7u8; 4000];
        let err = store.put(&too_big).await.unwrap_err();
        assert!(err.contains("--blob-store-mb"), "{err}");
    }

    #[tokio::test]
    async fn unpinning_releases_blobs_shared_between_tasks_only_once_both_are_done() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path(), 1 << 20);
        let shared = store.put(b"shared input").await.unwrap();
        let (t1, t2) = (TaskId::new(), TaskId::new());
        store.pin_task(t1, vec![shared]);
        store.pin_task(t2, vec![shared]);
        store.unpin_task(&t1).await;
        assert_eq!(store.stats().pinned_blobs, 1);
        store.unpin_task(&t2).await;
        assert_eq!(store.stats().pinned_blobs, 0);
        assert!(store.pinned_tasks().is_empty());
    }

    #[tokio::test]
    async fn output_pin_guard_releases_on_stale_or_retry_and_retains_when_terminal() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlobStore::open(
            &BlobStoreConfig::new(dir.path(), 2000)
                .with_output_pin_retention(Duration::from_millis(30)),
        )
        .unwrap();
        let input = store.put(&vec![1u8; 800]).await.unwrap();
        let out_stale = store.put(&vec![2u8; 800]).await.unwrap();
        let task_id = TaskId::new();
        store.pin_task(task_id, vec![input]);

        // Stale or retried result: dropping the guard releases the output pin while keeping input pins.
        let guard = store.check_and_pin_outputs(&[out_stale]).unwrap();
        assert_eq!(store.stats().pinned_blobs, 2);
        drop(guard);
        assert_eq!(store.stats().pinned_blobs, 1);

        // Terminal result: input pins are released and output pins stay until retention expires.
        let out_final = store.put(&vec![3u8; 800]).await.unwrap();
        let guard = store.check_and_pin_outputs(&[out_final]).unwrap();
        store.unpin_task(&task_id).await;
        guard.commit_terminal(task_id).await;
        assert_eq!(store.stats().pinned_blobs, 1);

        std::thread::sleep(Duration::from_millis(50));
        store.sweep_expired_output_pins().await;
        assert_eq!(store.stats().pinned_blobs, 0);
    }

    #[tokio::test]
    async fn inputs_resolve_only_when_every_blob_of_every_tree_is_present() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path(), 1 << 20);
        let main = store.put(b"fn main() {}").await.unwrap();
        let missing = BlobId::of(b"not uploaded");
        let tree = TreeManifest::new(vec![
            BlobFile {
                path: "src/main.rs".into(),
                id: main,
                size: 12,
                executable: false,
            },
            BlobFile {
                path: "README.md".into(),
                id: missing,
                size: 12,
                executable: false,
            },
        ])
        .unwrap();
        let encoded = tree.encode();
        let manifest = store.put(&encoded).await.unwrap();
        let inputs = vec![TaskInput::tree("proj", manifest, encoded.len() as u64)];
        let err = store.resolve_inputs(&inputs).await.unwrap_err();
        assert!(
            err.contains("proj/README.md") && err.contains("not on the master"),
            "{err}"
        );

        store.put(b"not uploaded").await.unwrap();
        let ids = store.resolve_inputs(&inputs).await.unwrap();
        assert_eq!(ids.len(), 3);

        let wrong_size = vec![TaskInput::file("x", main, 99, false)];
        assert!(store
            .resolve_inputs(&wrong_size)
            .await
            .unwrap_err()
            .contains("99"));
    }

    #[tokio::test]
    async fn aborted_and_restarted_uploads_leave_no_temporary_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path(), 1 << 30);
        let data = vec![9u8; BLOB_CHUNK_BYTES * 2];
        let id = BlobId::of(&data);
        let owner = store.new_owner();
        let first = |offset: usize| BlobRequest::Write {
            id,
            total_size: data.len() as u64,
            offset: offset as u64,
            data: Bytes::from(data[offset..offset + BLOB_CHUNK_BYTES].to_vec()),
        };
        assert!(matches!(
            store.handle(owner, first(0)).await,
            BlobReply::Written {
                complete: false,
                ..
            }
        ));
        // Starting over replaces the partial upload.
        assert!(matches!(
            store.handle(owner, first(0)).await,
            BlobReply::Written {
                complete: false,
                ..
            }
        ));
        assert_eq!(store.stats().uploads, 1);
        store.abort_uploads(owner);
        assert_eq!(store.stats().uploads, 0);
        assert_eq!(
            std::fs::read_dir(dir.path().join("tmp")).unwrap().count(),
            0
        );
    }

    #[tokio::test]
    async fn uploads_in_progress_count_towards_max_bytes_and_cannot_exceed_bound() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path(), 3000);
        let a = store.put(&vec![1u8; 1000]).await.unwrap();
        let b = store.put(&vec![2u8; 1000]).await.unwrap();
        assert!(store.contains(&a) && store.contains(&b));

        // Starting a 2000-byte upload in `tmp/` brings `bytes + upload_bytes` to 4000 > 3000,
        // so the oldest unpinned blob `a` must be evicted immediately.
        let c_data = vec![3u8; 2000];
        let c_id = BlobId::of(&c_data);
        let owner1 = store.new_owner();
        let reply1 = store
            .handle(
                owner1,
                BlobRequest::Write {
                    id: c_id,
                    total_size: 2000,
                    offset: 0,
                    data: Bytes::from(c_data[..1000].to_vec()),
                },
            )
            .await;
        assert!(
            matches!(
                reply1,
                BlobReply::Written {
                    complete: false,
                    ..
                }
            ),
            "{reply1:?}"
        );
        assert!(
            !store.contains(&a),
            "in-progress upload in tmp/ must evict oldest unpinned blob to stay within max_bytes"
        );
        assert!(store.contains(&b));

        // A second concurrent 2000-byte upload while 2000 bytes are already reserved in `tmp/`
        // would push `tmp/` alone to 4000 > 3000 and must be refused.
        let d_data = vec![4u8; 2000];
        let d_id = BlobId::of(&d_data);
        let owner2 = store.new_owner();
        let reply2 = store
            .handle(
                owner2,
                BlobRequest::Write {
                    id: d_id,
                    total_size: 2000,
                    offset: 0,
                    data: Bytes::from(d_data[..1000].to_vec()),
                },
            )
            .await;
        assert!(
            matches!(reply2, BlobReply::Failed { .. }),
            "concurrent uploads exceeding max_bytes must fail, got {reply2:?}"
        );

        store.abort_uploads(owner1);
    }

    #[tokio::test]
    async fn abort_uploads_while_chunk_is_in_flight_does_not_reinsert_or_leak_tmp_file() {
        let dir = tempfile::tempdir().unwrap();
        let data = vec![7u8; BLOB_CHUNK_BYTES * 2];
        let total = data.len() as u64;
        let store = open(dir.path(), total);
        let id = BlobId::of(&data);
        let owner = store.new_owner();

        // 1. Mid-upload abort while chunk 0 is in flight (`UploadSlot::Writing`):
        //    `abort_uploads(owner)` must release `upload_bytes` (M5c), and `append` must not
        //    put the upload back into `inner.uploads` or leave its `.part` file in `tmp/` (M5a).
        {
            let mut inner = store.lock();
            inner.upload_bytes = total;
            inner
                .uploads
                .insert((owner, id), UploadSlot::Writing { total });
        }
        assert_eq!(store.lock().upload_bytes, total);
        store.abort_uploads(owner);
        assert_eq!(
            store.lock().upload_bytes,
            0,
            "abort_uploads must release upload_bytes reservation for Writing slots"
        );
        let reply = store.append(owner, id, total, 0, &data[..BLOB_CHUNK_BYTES], None);
        assert!(
            matches!(reply, BlobReply::Failed { .. }),
            "append for an aborted owner must fail instead of re-inserting, got {reply:?}"
        );
        assert_eq!(store.stats().uploads, 0);
        assert_eq!(store.lock().upload_bytes, 0);
        assert_eq!(
            std::fs::read_dir(dir.path().join("tmp"))
                .map(|d| d.count())
                .unwrap_or(0),
            0
        );

        // 2. Abort during the last chunk (`commit`, M5b) and `upload_bytes` release (M5c):
        //    send chunk 0 normally (so `upload_bytes == total`), transition the slot to
        //    `Writing` as `write_chunk` does for chunk 1, abort `owner2`, and run `append`
        //    for the final chunk (which calls `self.commit`).
        let owner2 = store.new_owner();
        let r0 = store
            .handle(
                owner2,
                BlobRequest::Write {
                    id,
                    total_size: total,
                    offset: 0,
                    data: Bytes::from(data[..BLOB_CHUNK_BYTES].to_vec()),
                },
            )
            .await;
        assert!(
            matches!(
                r0,
                BlobReply::Written {
                    complete: false,
                    ..
                }
            ),
            "{r0:?}"
        );
        assert_eq!(store.lock().upload_bytes, total);
        let in_flight_upload = match store
            .lock()
            .uploads
            .insert((owner2, id), UploadSlot::Writing { total })
        {
            Some(UploadSlot::Idle(u)) => *u,
            _ => panic!("expected Idle upload after chunk 0"),
        };
        store.abort_uploads(owner2);
        assert_eq!(
            store.lock().upload_bytes,
            0,
            "abort_uploads must release upload_bytes before last-chunk commit finishes"
        );
        let r1 = store.append(
            owner2,
            id,
            total,
            BLOB_CHUNK_BYTES as u64,
            &data[BLOB_CHUNK_BYTES..],
            Some(in_flight_upload),
        );
        assert!(
            matches!(r1, BlobReply::Failed { .. }),
            "commit for an aborted owner must fail, got {r1:?}"
        );
        assert!(
            !store.contains(&id),
            "aborted last-chunk upload must not insert the blob into the store"
        );
        assert_eq!(store.stats().uploads, 0);
        assert_eq!(store.lock().upload_bytes, 0);
        assert_eq!(
            std::fs::read_dir(dir.path().join("tmp"))
                .map(|d| d.count())
                .unwrap_or(0),
            0
        );
    }

    #[tokio::test]
    async fn output_pins_exceeding_max_bytes_release_oldest_terminal_pins_early_but_keep_newest() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path(), 3000);
        let out1 = store.put(&vec![1u8; 1200]).await.unwrap();
        store
            .check_and_pin_outputs(&[out1])
            .unwrap()
            .commit_terminal(TaskId::new())
            .await;
        let out2 = store.put(&vec![2u8; 1200]).await.unwrap();
        store
            .check_and_pin_outputs(&[out2])
            .unwrap()
            .commit_terminal(TaskId::new())
            .await;
        assert_eq!(store.stats().pinned_blobs, 2);
        assert!(store.contains(&out1) && store.contains(&out2));

        // Third 1200-byte output brings pinned output bytes to 3600 > 3000 (max_bytes):
        // the oldest terminal output pin (`out1`) must be released early and evicted.
        let out3 = store.put(&vec![3u8; 1200]).await.unwrap();
        store
            .check_and_pin_outputs(&[out3])
            .unwrap()
            .commit_terminal(TaskId::new())
            .await;
        assert!(
            !store.contains(&out1),
            "oldest terminal output pin must be released early and evicted when output pins exceed max_bytes"
        );
        assert!(store.contains(&out2) && store.contains(&out3));
        assert_eq!(store.stats().pinned_blobs, 2);
        assert!(store.stats().bytes <= 3000);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn eviction_and_concurrent_commit_of_same_blob_never_leaves_index_without_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path(), 8000);
        let mut pinned = Vec::new();
        for i in 0..7u8 {
            pinned.push(store.put(&vec![i + 10; 1000]).await.unwrap());
        }
        store.pin_task(TaskId::new(), pinned);

        let a_data = vec![0x41u8; 1000];
        let a_id = BlobId::of(&a_data);

        for round in 0..200u32 {
            // Prepare 7 connections that have already sent the first 500 bytes of `a` while `a`
            // is not yet in `entries`, so their second chunk will run `append -> commit(a)`.
            store.lock().remove(&a_id);
            let _ = std::fs::remove_file(object_path(&store.objects, &a_id));
            let mut owners = Vec::with_capacity(7);
            for _ in 0..7 {
                let owner = store.new_owner();
                let r = store
                    .handle(
                        owner,
                        BlobRequest::Write {
                            id: a_id,
                            total_size: 1000,
                            offset: 0,
                            data: Bytes::from(a_data[..500].to_vec()),
                        },
                    )
                    .await;
                assert!(
                    matches!(
                        r,
                        BlobReply::Written {
                            complete: false,
                            ..
                        }
                    ),
                    "{r:?}"
                );
                owners.push(owner);
            }
            // Commit `a` once so it is the eviction victim when `b` commits.
            let first = owners.pop().unwrap();
            let r = store
                .handle(
                    first,
                    BlobRequest::Write {
                        id: a_id,
                        total_size: 1000,
                        offset: 500,
                        data: Bytes::from(a_data[500..].to_vec()),
                    },
                )
                .await;
            assert!(matches!(r, BlobReply::Written { complete: true, .. }));

            let mut b_data = vec![0x42u8; 1000];
            b_data[..4].copy_from_slice(&round.to_le_bytes());
            let store_b = Arc::clone(&store);
            let evictor = tokio::spawn(async move {
                let _ = store_b.put(&b_data).await;
            });
            let mut committers = Vec::new();
            for owner in owners {
                let store_a = Arc::clone(&store);
                let tail = Bytes::from(a_data[500..].to_vec());
                let upload = match store_a
                    .lock()
                    .uploads
                    .insert((owner, a_id), UploadSlot::Writing { total: 1000 })
                {
                    Some(UploadSlot::Idle(u)) => Some(*u),
                    _ => None,
                };
                committers.push(tokio::task::spawn_blocking(move || {
                    let _ = store_a.append(owner, a_id, 1000, 500, tail.as_bytes(), upload);
                }));
            }
            let _ = evictor.await;
            for c in committers {
                let _ = c.await;
            }
            if store.contains(&a_id) {
                assert!(
                    object_path(&store.objects, &a_id).exists(),
                    "round {round}: index contains {a_id} after concurrent eviction+commit, but its file was deleted from disk"
                );
            }
        }
    }
}
