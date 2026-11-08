//! Durable task store: an append-only journal plus a self-describing snapshot (design 0046 D1-D2).
//!
//! `TaskStore` is a standalone module. It is not yet wired into `TaskQueue` or `MasterServer`;
//! the integration (`--state-dir`, replay into the queue) lands separately.
//!
//! # Files under `<state_dir>`
//!
//! * `store.lock`: exclusive advisory lock held for the lifetime of the `TaskStore`.
//! * `journal.bin`: 16-byte header, then frames `[payload_len u32 LE][blake3 32B][payload]`, where
//!   the payload is the bincode form of a [`JournalRecord`].
//! * `snapshot.bin`: 16-byte header, then one frame whose payload is the JSON form of a
//!   [`SnapshotImage`] (human-readable serde representations, so it survives protocol bumps).
//!
//! The 16-byte header is `[magic 8B][store_format_version u32 LE][protocol_version u32 LE]`.
//!
//! # Durability
//!
//! [`TaskStore::append_durable`] returns only after a `sync_data()` that covers its record.
//! Durable appends arriving within [`GROUP_COMMIT_WINDOW`] (up to [`MAX_GROUP_BATCH`] records)
//! share one `sync_data()`. [`TaskStore::append`] writes without syncing.
//!
//! # Compaction contract
//!
//! The store cannot see the queue, so the caller builds the [`SnapshotImage`]. The image must
//! cover exactly the records appended before the `compact` call: build it and call `compact` under
//! the same lock that orders appends. A crash between the snapshot rename and the journal reset
//! leaves a journal whose records the snapshot already holds, so a replaying consumer must treat
//! a record for a state it already has (same task and sequence) as idempotent.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::protocol::{MAX_FRAME_SIZE, PROTOCOL_VERSION_CURRENT};
use rusty_grid_core::task::{CheckpointData, Task, TaskId, TaskResult};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tracing::{error, warn};
use uuid::Uuid;

use crate::queue::{RetryAttempt, TaskState};

/// Magic bytes at the start of `journal.bin`.
pub const JOURNAL_MAGIC: &[u8; 8] = b"OXSWJRN1";
/// Magic bytes at the start of `snapshot.bin`.
pub const SNAPSHOT_MAGIC: &[u8; 8] = b"OXSWSNP1";
/// On-disk layout version understood by this module.
pub const STORE_FORMAT_VERSION: u32 = 1;
/// Size of the file header in bytes.
pub const HEADER_LEN: usize = 16;
/// Size of a frame header: `payload_len` (4) plus the BLAKE3 hash (32).
pub const FRAME_HEADER_LEN: usize = 4 + 32;
/// Group commit window: a durable append waits at most this long for companions.
pub const GROUP_COMMIT_WINDOW: Duration = Duration::from_millis(5);
/// A group commit never batches more than this many records.
pub const MAX_GROUP_BATCH: usize = 256;
/// Journal size above which [`TaskStore::needs_compaction`] turns true.
pub const DEFAULT_COMPACT_MAX_BYTES: u64 = 64 * 1024 * 1024;
/// Journal record count above which [`TaskStore::needs_compaction`] turns true.
pub const DEFAULT_COMPACT_MAX_RECORDS: u64 = 10_000;

const LOCK_FILE: &str = "store.lock";
const JOURNAL_FILE: &str = "journal.bin";
const SNAPSHOT_FILE: &str = "snapshot.bin";
const SNAPSHOT_TMP_FILE: &str = "snapshot.bin.tmp";

/// One persisted event in `journal.bin` (design 0046 D2.2).
// The shape is fixed by the design; records are short-lived values, not stored in bulk.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JournalRecord {
    TaskSubmitted {
        task: Box<Task>,
        priority: u32,
        sequence: u64,
        submitted_at_utc: u64,
    },
    TaskDispatched {
        task_id: TaskId,
        worker_id: Uuid,
        timestamp_utc: u64,
    },
    TaskRunning {
        task_id: TaskId,
        worker_id: Uuid,
        timestamp_utc: u64,
    },
    CheckpointRecorded {
        task_id: TaskId,
        checkpoint: CheckpointData,
    },
    TaskRequeued {
        task_id: TaskId,
        /// `Queued` or `Retrying`.
        state: TaskState,
        retry_count: u32,
        failover_count: u32,
        evacuation_count: u32,
        rejection_count: u32,
        avoid_workers: Vec<Uuid>,
        retry_attempt: Option<RetryAttempt>,
        retry_delay_ms: Option<u64>,
    },
    TaskAdopted {
        task_id: TaskId,
        worker_id: Uuid,
        timestamp_utc: u64,
    },
    TaskTerminal {
        task_id: TaskId,
        /// `Completed`, `Failed` or `Cancelled`.
        state: TaskState,
        assigned_worker_id: Option<Uuid>,
        retry_count: u32,
        failover_count: u32,
        evacuation_count: u32,
        rejection_count: u32,
        retry_attempt: Option<RetryAttempt>,
        result: Option<TaskResult>,
        error_message: Option<String>,
        timestamp_utc: u64,
    },
    TaskEvicted {
        task_id: TaskId,
    },
}

/// One task as stored in a snapshot. Counters and optional fields default so that a snapshot
/// written by an older binary still loads after a field is added.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistedTaskEntry {
    pub task: Task,
    pub priority: u32,
    pub sequence: u64,
    pub state: TaskState,
    #[serde(default)]
    pub assigned_worker_id: Option<Uuid>,
    #[serde(default)]
    pub submitted_at_utc: u64,
    #[serde(default)]
    pub retry_count: u32,
    #[serde(default)]
    pub failover_count: u32,
    #[serde(default)]
    pub evacuation_count: u32,
    #[serde(default)]
    pub rejection_count: u32,
    #[serde(default)]
    pub avoid_workers: Vec<Uuid>,
    #[serde(default)]
    pub retry_history: Vec<RetryAttempt>,
    #[serde(default)]
    pub result: Option<TaskResult>,
    #[serde(default)]
    pub error_message: Option<String>,
}

/// Point-in-time image of the queue written by compaction.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct SnapshotImage {
    /// Next queue sequence number to hand out.
    pub next_sequence: u64,
    /// All non-evicted tasks.
    #[serde(default)]
    pub tasks: Vec<PersistedTaskEntry>,
    /// Terminal tasks, oldest first (eviction order).
    #[serde(default)]
    pub terminal_order: Vec<TaskId>,
}

/// Everything `TaskStore::open` read back from disk.
#[derive(Debug, Default)]
pub struct Recovered {
    /// The latest snapshot, if one exists.
    pub snapshot: Option<SnapshotImage>,
    /// Journal records written after that snapshot, in append order.
    pub records: Vec<JournalRecord>,
    /// True if a torn final journal frame was cut off.
    pub truncated_tail: bool,
}

/// Tuning knobs; [`StoreOptions::default`] gives the design values.
#[derive(Debug, Clone)]
pub struct StoreOptions {
    pub compact_max_bytes: u64,
    pub compact_max_records: u64,
    pub group_commit_window: Duration,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            compact_max_bytes: DEFAULT_COMPACT_MAX_BYTES,
            compact_max_records: DEFAULT_COMPACT_MAX_RECORDS,
            group_commit_window: GROUP_COMMIT_WINDOW,
        }
    }
}

type Ack = oneshot::Sender<GridResult<()>>;

enum Cmd {
    Append { frame: Vec<u8>, ack: Option<Ack> },
    Compact { snapshot_frame: Vec<u8>, ack: Ack },
}

#[derive(Default)]
struct Shared {
    sync_calls: AtomicU64,
    journal_bytes: AtomicU64,
    journal_records: AtomicU64,
    compaction_due: AtomicBool,
    failed: Mutex<Option<String>>,
}

/// Handle to the on-disk store. Dropping it flushes pending records, stops the writer thread and
/// releases `store.lock`.
pub struct TaskStore {
    dir: PathBuf,
    tx: Option<mpsc::Sender<Cmd>>,
    thread: Option<JoinHandle<()>>,
    shared: Arc<Shared>,
}

impl std::fmt::Debug for TaskStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskStore").field("dir", &self.dir).finish()
    }
}

impl TaskStore {
    /// Opens (creating if needed) the store in `state_dir` with default options.
    pub fn open(state_dir: impl AsRef<Path>) -> GridResult<(TaskStore, Recovered)> {
        Self::open_with(state_dir, StoreOptions::default())
    }

    /// Opens the store with explicit options.
    pub fn open_with(
        state_dir: impl AsRef<Path>,
        options: StoreOptions,
    ) -> GridResult<(TaskStore, Recovered)> {
        let dir = state_dir.as_ref().to_path_buf();
        prepare_dir(&dir)?;

        let lock_path = dir.join(LOCK_FILE);
        let lock = open_rw(&lock_path, false)?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(GridError::Config(format!(
                    "the state directory is in use: {} is locked by another master process",
                    lock_path.display()
                )));
            }
            Err(std::fs::TryLockError::Error(e)) => {
                return Err(storage_io(&lock_path, "lock", e));
            }
        }

        // A leftover temp file is an interrupted compaction; the old snapshot is still valid.
        let _ = std::fs::remove_file(dir.join(SNAPSHOT_TMP_FILE));

        let snapshot_path = dir.join(SNAPSHOT_FILE);
        let (snapshot, snapshot_version) = read_snapshot(&snapshot_path)?;

        let journal_path = dir.join(JOURNAL_FILE);
        let loaded = read_journal(&journal_path)?;
        if let Some(image) = &snapshot {
            if snapshot_version != Some(PROTOCOL_VERSION_CURRENT) {
                // Self-describing JSON: carry it over to the current protocol version.
                write_snapshot_file(&dir, &encode_snapshot(image)?)?;
            }
        }

        let journal = open_rw(&journal_path, true)?;
        let mut journal_len = loaded.valid_len;
        if loaded.fresh || loaded.rewrite_header {
            journal
                .set_len(0)
                .map_err(|e| storage_io(&journal_path, "reset", e))?;
            (&journal)
                .write_all(&header_bytes(JOURNAL_MAGIC))
                .map_err(|e| storage_io(&journal_path, "write header of", e))?;
            journal_len = HEADER_LEN as u64;
            journal
                .sync_all()
                .map_err(|e| storage_io(&journal_path, "sync", e))?;
        } else if loaded.truncated_tail {
            journal
                .set_len(loaded.valid_len)
                .map_err(|e| storage_io(&journal_path, "truncate", e))?;
            journal
                .sync_all()
                .map_err(|e| storage_io(&journal_path, "sync", e))?;
        }

        let shared = Arc::new(Shared::default());
        shared.journal_bytes.store(journal_len, Ordering::Relaxed);
        shared
            .journal_records
            .store(loaded.records.len() as u64, Ordering::Relaxed);
        let writer = Writer {
            dir: dir.clone(),
            journal,
            _lock: lock,
            bytes: journal_len,
            records: loaded.records.len() as u64,
            options,
            shared: Arc::clone(&shared),
        };
        writer.update_due();

        let (tx, rx) = mpsc::channel();
        let thread = std::thread::Builder::new()
            .name("task-store-writer".into())
            .spawn(move || writer.run(rx))
            .map_err(|e| GridError::Storage(format!("cannot start the store writer: {e}")))?;

        let store = TaskStore {
            dir,
            tx: Some(tx),
            thread: Some(thread),
            shared,
        };
        let recovered = Recovered {
            snapshot,
            records: loaded.records,
            truncated_tail: loaded.truncated_tail,
        };
        Ok((store, recovered))
    }

    /// The state directory this store owns.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Queues `record` for writing without waiting for it to reach disk.
    pub fn append(&self, record: &JournalRecord) -> GridResult<()> {
        let frame = encode_record_frame(record)?;
        self.send(Cmd::Append { frame, ack: None })
    }

    /// Appends `record` and returns once a `sync_data()` covering it has completed.
    pub async fn append_durable(&self, record: &JournalRecord) -> GridResult<()> {
        let rx = self.start_durable(record)?;
        rx.await
            .map_err(|_| GridError::Storage("the store writer stopped".into()))?
    }

    /// Blocking form of [`append_durable`](Self::append_durable) for non-async callers. Must not
    /// be called from inside a tokio runtime worker.
    pub fn append_durable_blocking(&self, record: &JournalRecord) -> GridResult<()> {
        let rx = self.start_durable(record)?;
        rx.blocking_recv()
            .map_err(|_| GridError::Storage("the store writer stopped".into()))?
    }

    /// Writes `image` as the new snapshot and resets the journal (see the module docs for the
    /// contract between `image` and previously appended records).
    pub async fn compact(&self, image: &SnapshotImage) -> GridResult<()> {
        let rx = self.start_compact(image)?;
        rx.await
            .map_err(|_| GridError::Storage("the store writer stopped".into()))?
    }

    /// Blocking form of [`compact`](Self::compact).
    pub fn compact_blocking(&self, image: &SnapshotImage) -> GridResult<()> {
        let rx = self.start_compact(image)?;
        rx.blocking_recv()
            .map_err(|_| GridError::Storage("the store writer stopped".into()))?
    }

    /// True once the journal passed the size or record-count threshold.
    pub fn needs_compaction(&self) -> bool {
        self.shared.compaction_due.load(Ordering::Relaxed)
    }

    /// Number of `sync_data()` calls the writer has made (test hook for group commit).
    pub fn sync_count(&self) -> u64 {
        self.shared.sync_calls.load(Ordering::Relaxed)
    }

    /// Records currently in the journal (written so far, not necessarily synced).
    pub fn journal_records(&self) -> u64 {
        self.shared.journal_records.load(Ordering::Relaxed)
    }

    /// Current journal size in bytes.
    pub fn journal_bytes(&self) -> u64 {
        self.shared.journal_bytes.load(Ordering::Relaxed)
    }

    fn start_durable(
        &self,
        record: &JournalRecord,
    ) -> GridResult<oneshot::Receiver<GridResult<()>>> {
        let frame = encode_record_frame(record)?;
        let (ack, rx) = oneshot::channel();
        self.send(Cmd::Append {
            frame,
            ack: Some(ack),
        })?;
        Ok(rx)
    }

    fn start_compact(
        &self,
        image: &SnapshotImage,
    ) -> GridResult<oneshot::Receiver<GridResult<()>>> {
        let snapshot_frame = encode_snapshot(image)?;
        let (ack, rx) = oneshot::channel();
        self.send(Cmd::Compact {
            snapshot_frame,
            ack,
        })?;
        Ok(rx)
    }

    fn send(&self, cmd: Cmd) -> GridResult<()> {
        if let Some(reason) = self.shared.failed.lock().unwrap().clone() {
            return Err(GridError::Storage(reason));
        }
        self.tx
            .as_ref()
            .ok_or_else(|| GridError::Storage("the store is closed".into()))?
            .send(cmd)
            .map_err(|_| GridError::Storage("the store writer stopped".into()))
    }
}

impl Drop for TaskStore {
    fn drop(&mut self) {
        // Closing the channel makes the writer drain, sync, and release the lock when it exits.
        self.tx.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct Writer {
    dir: PathBuf,
    journal: File,
    /// Held only so the lock lives as long as the writer.
    _lock: File,
    bytes: u64,
    records: u64,
    options: StoreOptions,
    shared: Arc<Shared>,
}

impl Writer {
    fn run(mut self, rx: mpsc::Receiver<Cmd>) {
        let mut next = rx.recv().ok();
        while let Some(cmd) = next {
            next = match cmd {
                Cmd::Append { frame, ack } => self.handle_append(frame, ack, &rx),
                Cmd::Compact {
                    snapshot_frame,
                    ack,
                } => {
                    let res = self
                        .check_failed()
                        .and_then(|()| self.compact(&snapshot_frame));
                    self.finish(res, std::iter::once(ack));
                    rx.recv().ok()
                }
            };
        }
        // Channel closed: make anything written without a sync durable before the lock goes away.
        if self.check_failed().is_ok() {
            if let Err(e) = self.sync() {
                error!("task store: final sync failed: {e}");
            }
        }
    }

    /// Writes one record. When it (or a companion arriving within the window) wants durability,
    /// syncs once for the whole group. Returns a command read ahead that is not an append.
    fn handle_append(
        &mut self,
        frame: Vec<u8>,
        ack: Option<Ack>,
        rx: &mpsc::Receiver<Cmd>,
    ) -> Option<Cmd> {
        let mut waiters: Vec<Ack> = Vec::new();
        let mut result = self.check_failed().and_then(|()| self.write_frame(&frame));
        waiters.extend(ack);
        if waiters.is_empty() {
            if let Err(e) = result {
                self.fail(&e);
            }
            return rx.recv().ok();
        }

        let mut count = 1usize;
        let mut deferred = None;
        let deadline = Instant::now() + self.options.group_commit_window;
        while count < MAX_GROUP_BATCH {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match rx.recv_timeout(remaining) {
                Ok(Cmd::Append { frame, ack }) => {
                    count += 1;
                    if result.is_ok() {
                        result = self.write_frame(&frame);
                    }
                    waiters.extend(ack);
                }
                Ok(other) => {
                    deferred = Some(other);
                    break;
                }
                Err(RecvTimeoutError::Timeout) | Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        let result = result.and_then(|()| self.sync());
        self.finish(result, waiters);
        match deferred {
            Some(cmd) => Some(cmd),
            None => rx.recv().ok(),
        }
    }

    fn write_frame(&mut self, frame: &[u8]) -> GridResult<()> {
        let path = self.dir.join(JOURNAL_FILE);
        self.journal
            .write_all(frame)
            .map_err(|e| storage_io(&path, "append to", e))?;
        self.bytes += frame.len() as u64;
        self.records += 1;
        self.publish_counters();
        Ok(())
    }

    fn sync(&mut self) -> GridResult<()> {
        self.shared.sync_calls.fetch_add(1, Ordering::Relaxed);
        self.journal
            .sync_data()
            .map_err(|e| storage_io(&self.dir.join(JOURNAL_FILE), "sync", e))
    }

    fn compact(&mut self, snapshot_frame: &[u8]) -> GridResult<()> {
        write_snapshot_file(&self.dir, snapshot_frame)?;
        let path = self.dir.join(JOURNAL_FILE);
        self.journal
            .set_len(0)
            .and_then(|()| self.journal.write_all(&header_bytes(JOURNAL_MAGIC)))
            .and_then(|()| self.journal.sync_all())
            .map_err(|e| storage_io(&path, "reset", e))?;
        self.bytes = HEADER_LEN as u64;
        self.records = 0;
        self.publish_counters();
        Ok(())
    }

    fn publish_counters(&self) {
        self.shared
            .journal_bytes
            .store(self.bytes, Ordering::Relaxed);
        self.shared
            .journal_records
            .store(self.records, Ordering::Relaxed);
        self.update_due();
    }

    fn update_due(&self) {
        let due = self.bytes > self.options.compact_max_bytes
            || self.records > self.options.compact_max_records;
        self.shared.compaction_due.store(due, Ordering::Relaxed);
    }

    fn check_failed(&self) -> GridResult<()> {
        match self.shared.failed.lock().unwrap().as_ref() {
            Some(reason) => Err(GridError::Storage(reason.clone())),
            None => Ok(()),
        }
    }

    /// Marks the store failed: the journal tail is unknown after a failed write, so every later
    /// operation is refused rather than risking a gap.
    fn fail(&self, err: &GridError) {
        error!("task store: {err}; refusing further writes");
        let mut failed = self.shared.failed.lock().unwrap();
        if failed.is_none() {
            *failed = Some(err.to_string());
        }
    }

    fn finish(&self, result: GridResult<()>, acks: impl IntoIterator<Item = Ack>) {
        match result {
            Ok(()) => {
                for ack in acks {
                    let _ = ack.send(Ok(()));
                }
            }
            Err(e) => {
                self.fail(&e);
                let msg = e.to_string();
                for ack in acks {
                    let _ = ack.send(Err(GridError::Storage(msg.clone())));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Directory, file and frame helpers
// ---------------------------------------------------------------------------------------------

fn storage_io(path: &Path, what: &str, err: std::io::Error) -> GridError {
    GridError::Storage(format!("cannot {what} {}: {err}", path.display()))
}

/// Creates `dir` (mode 0700 on Unix) and verifies it is a real directory owned by this user.
fn prepare_dir(dir: &Path) -> GridResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)
            .map_err(|e| storage_io(dir, "create the state directory", e))?;
    }
    #[cfg(not(unix))]
    std::fs::create_dir_all(dir).map_err(|e| storage_io(dir, "create the state directory", e))?;

    let meta = std::fs::symlink_metadata(dir)
        .map_err(|e| storage_io(dir, "inspect the state directory", e))?;
    if meta.file_type().is_symlink() {
        return Err(GridError::Storage(format!(
            "the state directory {} is a symbolic link; refusing to use it",
            dir.display()
        )));
    }
    if !meta.is_dir() {
        return Err(GridError::Storage(format!(
            "the state directory {} is not a directory",
            dir.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        // SAFETY: `geteuid()` is thread-safe and has no preconditions.
        let euid = unsafe { libc::geteuid() };
        if meta.uid() != euid {
            return Err(GridError::Storage(format!(
                "the state directory {} is owned by uid {}, not by this user (uid {euid})",
                dir.display(),
                meta.uid()
            )));
        }
        if meta.mode() & 0o077 != 0 {
            warn!(
                "task store: tightening permissions of {} to 0700",
                dir.display()
            );
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
                .map_err(|e| storage_io(dir, "restrict", e))?;
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
        if meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(GridError::Storage(format!(
                "the state directory {} is a reparse point; refusing to use it",
                dir.display()
            )));
        }
        // TODO(FEAT-1): restrict the DACL to the current user, SYSTEM and Administrators
        // (design 0046 D1.3); until then the directory inherits its parent's ACL.
    }
    Ok(())
}

/// Opens `path` read-write, creating it with mode 0600 on Unix.
fn open_rw(path: &Path, append: bool) -> GridResult<File> {
    let mut opts = OpenOptions::new();
    opts.read(true).create(true);
    if append {
        opts.append(true);
    } else {
        opts.write(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    opts.open(path).map_err(|e| storage_io(path, "open", e))
}

fn header_bytes(magic: &[u8; 8]) -> [u8; HEADER_LEN] {
    let mut h = [0u8; HEADER_LEN];
    h[..8].copy_from_slice(magic);
    h[8..12].copy_from_slice(&STORE_FORMAT_VERSION.to_le_bytes());
    h[12..16].copy_from_slice(&PROTOCOL_VERSION_CURRENT.to_le_bytes());
    h
}

/// Validates the magic and format version; returns the recorded protocol version.
fn parse_header(path: &Path, magic: &[u8; 8], bytes: &[u8]) -> GridResult<u32> {
    if bytes.len() < HEADER_LEN || &bytes[..8] != magic {
        return Err(GridError::Storage(format!(
            "{} is not a task store file (bad magic); refusing to open",
            path.display()
        )));
    }
    let format = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    if format != STORE_FORMAT_VERSION {
        return Err(GridError::Storage(format!(
            "{} has store format version {format}; this binary reads version {STORE_FORMAT_VERSION}",
            path.display()
        )));
    }
    Ok(u32::from_le_bytes(bytes[12..16].try_into().unwrap()))
}

fn encode_frame(payload: &[u8]) -> GridResult<Vec<u8>> {
    if payload.len() > MAX_FRAME_SIZE {
        return Err(GridError::FrameTooLarge {
            size: payload.len(),
            max: MAX_FRAME_SIZE,
        });
    }
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(blake3::hash(payload).as_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

fn encode_record_frame(record: &JournalRecord) -> GridResult<Vec<u8>> {
    let payload = bincode::serialize(record)
        .map_err(|e| GridError::Storage(format!("cannot encode a journal record: {e}")))?;
    encode_frame(&payload)
}

/// Returns the full snapshot file body after the header: one frame holding the JSON image.
fn encode_snapshot(image: &SnapshotImage) -> GridResult<Vec<u8>> {
    let payload = serde_json::to_vec(image)?;
    encode_frame(&payload)
}

/// Writes `snapshot.bin.tmp`, syncs it, and renames it over `snapshot.bin`.
fn write_snapshot_file(dir: &Path, snapshot_frame: &[u8]) -> GridResult<()> {
    let tmp = dir.join(SNAPSHOT_TMP_FILE);
    let dest = dir.join(SNAPSHOT_FILE);
    let _ = std::fs::remove_file(&tmp);
    let mut file = OpenOptions::new();
    file.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        file.mode(0o600);
    }
    let mut file = file.open(&tmp).map_err(|e| storage_io(&tmp, "create", e))?;
    file.write_all(&header_bytes(SNAPSHOT_MAGIC))
        .and_then(|()| file.write_all(snapshot_frame))
        .and_then(|()| file.sync_all())
        .map_err(|e| storage_io(&tmp, "write", e))?;
    drop(file);
    std::fs::rename(&tmp, &dest).map_err(|e| storage_io(&dest, "install", e))?;
    #[cfg(unix)]
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

/// Reads and verifies `snapshot.bin`. Returns the image and the header's protocol version.
fn read_snapshot(path: &Path) -> GridResult<(Option<SnapshotImage>, Option<u32>)> {
    let mut bytes = Vec::new();
    match File::open(path) {
        Ok(mut f) => {
            f.read_to_end(&mut bytes)
                .map_err(|e| storage_io(path, "read", e))?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((None, None)),
        Err(e) => return Err(storage_io(path, "open", e)),
    }
    let version = parse_header(path, SNAPSHOT_MAGIC, &bytes)?;
    let corrupt = |why: &str| GridError::Storage(format!("{} is corrupt: {why}", path.display()));
    let body = &bytes[HEADER_LEN..];
    if body.len() < FRAME_HEADER_LEN {
        return Err(corrupt("truncated frame header"));
    }
    let len = u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
    if len > MAX_FRAME_SIZE {
        return Err(corrupt("frame length exceeds the maximum"));
    }
    if body.len() != FRAME_HEADER_LEN + len {
        return Err(corrupt("frame length does not match the file size"));
    }
    let payload = &body[FRAME_HEADER_LEN..];
    if blake3::hash(payload).as_bytes() != &body[4..FRAME_HEADER_LEN] {
        return Err(corrupt("checksum mismatch"));
    }
    let image = serde_json::from_slice(payload)
        .map_err(|e| corrupt(&format!("cannot decode the snapshot: {e}")))?;
    Ok((Some(image), Some(version)))
}

struct LoadedJournal {
    records: Vec<JournalRecord>,
    /// Length of the valid prefix (header plus whole frames).
    valid_len: u64,
    truncated_tail: bool,
    /// The file is missing or holds only a torn header: create it.
    fresh: bool,
    /// Header-only journal written by another protocol version: stamp the current one.
    rewrite_header: bool,
}

fn read_journal(path: &Path) -> GridResult<LoadedJournal> {
    let mut bytes = Vec::new();
    match File::open(path) {
        Ok(mut f) => {
            f.read_to_end(&mut bytes)
                .map_err(|e| storage_io(path, "read", e))?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(storage_io(path, "open", e)),
    }
    let mut out = LoadedJournal {
        records: Vec::new(),
        valid_len: HEADER_LEN as u64,
        truncated_tail: false,
        fresh: false,
        rewrite_header: false,
    };
    if bytes.len() < HEADER_LEN {
        // Empty, or the process died while writing the header: only a prefix of ours is allowed.
        let n = bytes.len().min(8);
        if bytes[..n] != JOURNAL_MAGIC[..n] {
            return Err(GridError::Storage(format!(
                "{} is not a task store file (bad magic); refusing to open",
                path.display()
            )));
        }
        out.fresh = true;
        return Ok(out);
    }
    let version = parse_header(path, JOURNAL_MAGIC, &bytes)?;
    if bytes.len() == HEADER_LEN {
        out.rewrite_header = version != PROTOCOL_VERSION_CURRENT;
        return Ok(out);
    }
    if version != PROTOCOL_VERSION_CURRENT {
        return Err(GridError::Storage(format!(
            "{} holds uncompacted records written by protocol version {version}, but this binary \
             is protocol version {PROTOCOL_VERSION_CURRENT}; start the previous binary and stop \
             it cleanly (which compacts the journal) before upgrading",
            path.display()
        )));
    }

    let total = bytes.len();
    let mut off = HEADER_LEN;
    while off < total {
        let remaining = total - off;
        if remaining < FRAME_HEADER_LEN {
            return torn(path, out, off, "short frame header");
        }
        let len = u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap()) as usize;
        if len > MAX_FRAME_SIZE {
            return Err(GridError::Storage(format!(
                "{} is corrupt: the frame at offset {off} claims {len} bytes (maximum {MAX_FRAME_SIZE})",
                path.display()
            )));
        }
        let end = off + FRAME_HEADER_LEN + len;
        if end > total {
            return torn(path, out, off, "short frame payload");
        }
        let payload = &bytes[off + FRAME_HEADER_LEN..end];
        if blake3::hash(payload).as_bytes() != &bytes[off + 4..off + FRAME_HEADER_LEN] {
            if end == total {
                return torn(path, out, off, "checksum mismatch in the final frame");
            }
            return Err(GridError::Storage(format!(
                "{} is corrupt: checksum mismatch in the frame at offset {off} (not the last frame)",
                path.display()
            )));
        }
        let record = bincode::deserialize(payload).map_err(|e| {
            GridError::Storage(format!(
                "{} is corrupt: the frame at offset {off} does not decode: {e}",
                path.display()
            ))
        })?;
        out.records.push(record);
        off = end;
        out.valid_len = off as u64;
    }
    Ok(out)
}

fn torn(path: &Path, mut out: LoadedJournal, off: usize, why: &str) -> GridResult<LoadedJournal> {
    warn!(
        "task store: {why} at offset {off} of {}; discarding the torn tail ({} records kept)",
        path.display(),
        out.records.len()
    );
    out.valid_len = off as u64;
    out.truncated_tail = true;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::join_all;
    use rusty_grid_core::task::{TaskRequirements, TaskSpec};

    fn task(name: &str) -> Task {
        Task::new(
            TaskSpec::command("echo", vec![name.into()]),
            TaskRequirements::generic(1, 10),
        )
    }

    fn submitted(name: &str, sequence: u64) -> JournalRecord {
        JournalRecord::TaskSubmitted {
            task: Box::new(task(name)),
            priority: 5,
            sequence,
            submitted_at_utc: 1_700_000_000 + sequence,
        }
    }

    fn marker(i: u64) -> JournalRecord {
        JournalRecord::TaskEvicted {
            task_id: TaskId(Uuid::from_u128(i as u128)),
        }
    }

    fn attempt() -> RetryAttempt {
        RetryAttempt {
            attempt: 1,
            timestamp_utc: 42,
            failed_worker_id: Some(Uuid::new_v4()),
            reason: "worker lost".into(),
            exit_code: Some(137),
            error: Some("killed".into()),
        }
    }

    fn all_records() -> Vec<JournalRecord> {
        let t = task("all");
        let id = t.id;
        let worker = Uuid::new_v4();
        vec![
            JournalRecord::TaskSubmitted {
                task: Box::new(t),
                priority: 9,
                sequence: 7,
                submitted_at_utc: 11,
            },
            JournalRecord::TaskDispatched {
                task_id: id,
                worker_id: worker,
                timestamp_utc: 12,
            },
            JournalRecord::TaskRunning {
                task_id: id,
                worker_id: worker,
                timestamp_utc: 13,
            },
            JournalRecord::CheckpointRecorded {
                task_id: id,
                checkpoint: CheckpointData::new(3, vec![1u8, 2, 3]),
            },
            JournalRecord::TaskRequeued {
                task_id: id,
                state: TaskState::Retrying,
                retry_count: 2,
                failover_count: 1,
                evacuation_count: 3,
                rejection_count: 4,
                avoid_workers: vec![worker],
                retry_attempt: Some(attempt()),
                retry_delay_ms: Some(250),
            },
            JournalRecord::TaskAdopted {
                task_id: id,
                worker_id: worker,
                timestamp_utc: 14,
            },
            JournalRecord::TaskTerminal {
                task_id: id,
                state: TaskState::Failed,
                assigned_worker_id: Some(worker),
                retry_count: 2,
                failover_count: 1,
                evacuation_count: 3,
                rejection_count: 4,
                retry_attempt: Some(attempt()),
                result: Some(TaskResult::failure(
                    worker,
                    id,
                    1,
                    "out",
                    "err",
                    9,
                    Some("boom".into()),
                )),
                error_message: Some("boom".into()),
                timestamp_utc: 15,
            },
            JournalRecord::TaskEvicted { task_id: id },
        ]
    }

    fn journal_path(dir: &Path) -> PathBuf {
        dir.join(JOURNAL_FILE)
    }

    fn write_all(dir: &Path, records: &[JournalRecord]) {
        let (store, _) = TaskStore::open(dir).unwrap();
        for r in records {
            store.append_durable_blocking(r).unwrap();
        }
    }

    fn expect_storage_err(res: GridResult<(TaskStore, Recovered)>) -> String {
        match res {
            Err(GridError::Storage(msg)) => msg,
            Err(other) => panic!("expected a Storage error, got {other:?}"),
            Ok(_) => panic!("expected an error, but open succeeded"),
        }
    }

    #[test]
    fn round_trips_every_record_type() {
        let dir = tempfile::tempdir().unwrap();
        let records = all_records();
        write_all(dir.path(), &records);

        let (_store, rec) = TaskStore::open(dir.path()).unwrap();
        assert!(rec.snapshot.is_none());
        assert!(!rec.truncated_tail);
        assert_eq!(rec.records, records);
    }

    #[test]
    fn non_durable_appends_survive_a_clean_close() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (store, _) = TaskStore::open(dir.path()).unwrap();
            for i in 0..20 {
                store.append(&marker(i)).unwrap();
            }
        }
        let (_s, rec) = TaskStore::open(dir.path()).unwrap();
        assert_eq!(rec.records.len(), 20);
    }

    #[test]
    fn torn_tail_short_payload_is_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let records: Vec<_> = (0..5).map(|i| submitted("t", i)).collect();
        write_all(dir.path(), &records);

        let path = journal_path(dir.path());
        let full = std::fs::metadata(&path).unwrap().len();
        let f = OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(full - 7).unwrap();
        drop(f);

        let (_s, rec) = TaskStore::open(dir.path()).unwrap();
        assert!(rec.truncated_tail);
        assert_eq!(rec.records, records[..4]);
        let boundary: u64 = HEADER_LEN as u64
            + records[..4]
                .iter()
                .map(|r| encode_record_frame(r).unwrap().len() as u64)
                .sum::<u64>();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), boundary);
    }

    #[test]
    fn torn_tail_flipped_byte_in_last_frame_is_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let records: Vec<_> = (0..4).map(|i| submitted("t", i)).collect();
        write_all(dir.path(), &records);

        let path = journal_path(dir.path());
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 3;
        bytes[last] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        let (store, rec) = TaskStore::open(dir.path()).unwrap();
        assert!(rec.truncated_tail);
        assert_eq!(rec.records, records[..3]);
        // The store keeps working after the repair.
        store
            .append_durable_blocking(&submitted("again", 9))
            .unwrap();
        drop(store);
        let (_s, rec) = TaskStore::open(dir.path()).unwrap();
        assert_eq!(rec.records.len(), 4);
        assert!(!rec.truncated_tail);
    }

    #[test]
    fn torn_tail_short_frame_header_is_truncated() {
        let dir = tempfile::tempdir().unwrap();
        write_all(dir.path(), &[marker(1), marker(2)]);
        let path = journal_path(dir.path());
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[9, 9, 9]).unwrap();
        drop(f);
        let (_s, rec) = TaskStore::open(dir.path()).unwrap();
        assert!(rec.truncated_tail);
        assert_eq!(rec.records.len(), 2);
    }

    #[test]
    fn corrupt_middle_frame_refuses_to_open() {
        let dir = tempfile::tempdir().unwrap();
        let records: Vec<_> = (0..4).map(|i| submitted("t", i)).collect();
        write_all(dir.path(), &records);

        let path = journal_path(dir.path());
        let mut bytes = std::fs::read(&path).unwrap();
        let first = encode_record_frame(&records[0]).unwrap().len();
        // A byte inside the 2nd frame's payload.
        bytes[HEADER_LEN + first + FRAME_HEADER_LEN + 2] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        let msg = expect_storage_err(TaskStore::open(dir.path()));
        assert!(msg.contains("journal.bin"), "{msg}");
    }

    #[test]
    fn oversized_frame_length_bad_magic_and_format_are_refused() {
        // payload_len above the bound.
        let dir = tempfile::tempdir().unwrap();
        write_all(dir.path(), &[marker(1), marker(2)]);
        let path = journal_path(dir.path());
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[HEADER_LEN..HEADER_LEN + 4]
            .copy_from_slice(&((MAX_FRAME_SIZE as u32) + 1).to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();
        expect_storage_err(TaskStore::open(dir.path()));

        // Bad magic.
        let dir = tempfile::tempdir().unwrap();
        write_all(dir.path(), &[marker(1)]);
        let path = journal_path(dir.path());
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] = b'X';
        std::fs::write(&path, &bytes).unwrap();
        expect_storage_err(TaskStore::open(dir.path()));

        // Unknown store format version.
        let dir = tempfile::tempdir().unwrap();
        write_all(dir.path(), &[marker(1)]);
        let path = journal_path(dir.path());
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[8..12].copy_from_slice(&99u32.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();
        let msg = expect_storage_err(TaskStore::open(dir.path()));
        assert!(msg.contains("99"), "{msg}");
    }

    fn image_with(n: u64) -> SnapshotImage {
        let tasks: Vec<_> = (0..n)
            .map(|i| PersistedTaskEntry {
                task: task("snap"),
                priority: 1,
                sequence: i,
                state: TaskState::Queued,
                assigned_worker_id: None,
                submitted_at_utc: 5,
                retry_count: 1,
                failover_count: 2,
                evacuation_count: 0,
                rejection_count: 0,
                avoid_workers: vec![],
                retry_history: vec![attempt()],
                result: None,
                error_message: None,
            })
            .collect();
        SnapshotImage {
            next_sequence: n,
            terminal_order: vec![],
            tasks,
        }
    }

    #[test]
    fn compaction_then_appends_then_reopen_gives_full_state() {
        let dir = tempfile::tempdir().unwrap();
        let image = image_with(3);
        {
            let (store, _) = TaskStore::open(dir.path()).unwrap();
            for i in 0..5 {
                store.append_durable_blocking(&marker(i)).unwrap();
            }
            store.compact_blocking(&image).unwrap();
            assert_eq!(store.journal_records(), 0);
            assert_eq!(store.journal_bytes(), HEADER_LEN as u64);
            store
                .append_durable_blocking(&submitted("after", 3))
                .unwrap();
            store.append(&marker(77)).unwrap();
        }
        assert!(!dir.path().join(SNAPSHOT_TMP_FILE).exists());
        let (_s, rec) = TaskStore::open(dir.path()).unwrap();
        assert_eq!(rec.snapshot.as_ref(), Some(&image));
        assert_eq!(rec.records.len(), 2);
        assert_eq!(rec.records[1], marker(77));
    }

    #[test]
    fn compaction_thresholds_flag_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let opts = StoreOptions {
            compact_max_records: 3,
            ..StoreOptions::default()
        };
        let (store, _) = TaskStore::open_with(dir.path(), opts).unwrap();
        for i in 0..3 {
            store.append_durable_blocking(&marker(i)).unwrap();
        }
        assert!(!store.needs_compaction());
        store.append_durable_blocking(&marker(3)).unwrap();
        assert!(store.needs_compaction());
        store.compact_blocking(&SnapshotImage::default()).unwrap();
        assert!(!store.needs_compaction());
    }

    #[test]
    fn second_open_fails_while_the_first_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let (first, _) = TaskStore::open(dir.path()).unwrap();
        let err = TaskStore::open(dir.path()).unwrap_err();
        match err {
            GridError::Config(msg) => assert!(msg.contains("store.lock"), "{msg}"),
            other => panic!("unexpected error {other:?}"),
        }
        drop(first);
        TaskStore::open(dir.path()).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn group_commit_shares_syncs_between_concurrent_appends() {
        const N: u64 = 1_000;
        let dir = tempfile::tempdir().unwrap();
        let (store, _) = TaskStore::open(dir.path()).unwrap();
        let store = Arc::new(store);

        let futures = (0..N).map(|i| {
            let store = Arc::clone(&store);
            async move { store.append_durable(&marker(i)).await }
        });
        for res in join_all(futures).await {
            res.unwrap();
        }
        let syncs = store.sync_count();
        assert_eq!(store.journal_records(), N);
        // At least ceil(N / MAX_GROUP_BATCH) syncs are needed; one per append would be N.
        assert!(syncs >= 1, "no sync happened");
        assert!(syncs <= N / 10, "{syncs} syncs for {N} appends");

        let store = Arc::try_unwrap(store).unwrap();
        drop(store);
        let (_s, rec) = TaskStore::open(dir.path()).unwrap();
        assert_eq!(rec.records.len() as u64, N);
    }

    #[test]
    fn a_durable_append_is_covered_by_a_sync_before_it_returns() {
        let dir = tempfile::tempdir().unwrap();
        let (store, _) = TaskStore::open(dir.path()).unwrap();
        let before = store.sync_count();
        store.append_durable_blocking(&marker(1)).unwrap();
        assert!(store.sync_count() > before);
        let before = store.sync_count();
        store.append(&marker(2)).unwrap();
        // A non-durable append alone never triggers a sync from the caller's side.
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(store.sync_count(), before);
    }

    fn set_header_version(path: &Path, version: u32) {
        let mut bytes = std::fs::read(path).unwrap();
        bytes[12..16].copy_from_slice(&version.to_le_bytes());
        std::fs::write(path, bytes).unwrap();
    }

    #[test]
    fn journal_with_another_protocol_version_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        write_all(dir.path(), &[marker(1)]);
        let path = journal_path(dir.path());
        set_header_version(&path, PROTOCOL_VERSION_CURRENT + 1);

        let msg = expect_storage_err(TaskStore::open(dir.path()));
        assert!(msg.contains("journal.bin"), "{msg}");
        assert!(
            msg.contains(&format!(
                "protocol version {}",
                PROTOCOL_VERSION_CURRENT + 1
            )),
            "{msg}"
        );
        assert!(
            msg.contains(&format!("protocol version {PROTOCOL_VERSION_CURRENT}")),
            "{msg}"
        );
    }

    #[test]
    fn empty_journal_and_snapshot_with_another_protocol_version_load() {
        let dir = tempfile::tempdir().unwrap();
        let image = image_with(2);
        {
            let (store, _) = TaskStore::open(dir.path()).unwrap();
            store.compact_blocking(&image).unwrap();
        }
        let old = PROTOCOL_VERSION_CURRENT.saturating_sub(1);
        set_header_version(&dir.path().join(SNAPSHOT_FILE), old);
        set_header_version(&journal_path(dir.path()), old);

        let (store, rec) = TaskStore::open(dir.path()).unwrap();
        assert_eq!(rec.snapshot.as_ref(), Some(&image));
        assert!(rec.records.is_empty());
        drop(store);

        // Both headers now carry the current version.
        for name in [SNAPSHOT_FILE, JOURNAL_FILE] {
            let bytes = std::fs::read(dir.path().join(name)).unwrap();
            let v = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
            assert_eq!(v, PROTOCOL_VERSION_CURRENT, "{name}");
        }
        let (_s, rec) = TaskStore::open(dir.path()).unwrap();
        assert_eq!(rec.snapshot.as_ref(), Some(&image));
    }

    #[test]
    fn corrupt_snapshot_refuses_to_open() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (store, _) = TaskStore::open(dir.path()).unwrap();
            store.compact_blocking(&image_with(1)).unwrap();
        }
        let path = dir.path().join(SNAPSHOT_FILE);
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 2;
        bytes[last] ^= 0xFF;
        std::fs::write(&path, bytes).unwrap();
        let msg = expect_storage_err(TaskStore::open(dir.path()));
        assert!(msg.contains("snapshot.bin"), "{msg}");
    }

    #[cfg(unix)]
    #[test]
    fn state_dir_is_private_and_symlinks_are_refused() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        let base = tempfile::tempdir().unwrap();
        let dir = base.path().join("state");
        let (store, _) = TaskStore::open(&dir).unwrap();
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&dir), 0o700);
        assert_eq!(mode(&dir.join(LOCK_FILE)), 0o600);
        assert_eq!(mode(&dir.join(JOURNAL_FILE)), 0o600);
        drop(store);

        let link = base.path().join("link");
        symlink(&dir, &link).unwrap();
        let msg = expect_storage_err(TaskStore::open(&link));
        assert!(msg.contains("symbolic link"), "{msg}");
    }
}
