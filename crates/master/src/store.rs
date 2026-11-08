//! Durable task store: an append-only journal plus a self-describing snapshot (design 0046 D1-D2).
//!
//! `TaskStore` backs `TaskQueue` and `MasterServer` when `master --state-dir`
//! (`RUSTY_GRID_STATE_DIR` / `[master].state_dir`) is configured.
//!
//! # Files under `<state_dir>`
//!
//! * `store.lock`: exclusive advisory lock held for the lifetime of the `TaskStore`.
//! * `journal.bin`: 24-byte header, then frames `[payload_len u32 LE][len_check u32 LE][blake3 32B][payload]`, where
//!   the payload is the bincode form of a [`JournalRecord`].
//! * `snapshot.bin`: 24-byte header, then one frame `[payload_len u64 LE][blake3 32B][payload]`
//!   whose payload is the JSON form of a [`SnapshotImage`] (human-readable serde representations,
//!   so it survives protocol bumps). The length is `u64` and bounded by [`SNAPSHOT_MAX_BYTES`].
//!
//! `len_check` is a bijective mix of `payload_len` ([`len_check`]), so a damaged length is detected
//! without reading the payload. The header is `[magic 8B][store_format_version u32 LE][protocol_version u32 LE][generation u64 LE]`.
//!
//! **A change to the bincode layout of [`JournalRecord`] or of any type it contains (`Task`,
//! `TaskState`, `RetryAttempt`, `CheckpointData`, `TaskResult`) is a change of the journal format:
//! bump [`STORE_FORMAT_VERSION`].** `store_layout_is_frozen` guards this with recorded bytes.
//!
//! # Generations
//!
//! Compaction writes the snapshot with generation `g + 1`, then resets the journal to a header with
//! generation `g + 1`. A journal whose generation is older than the snapshot's is what a crash
//! between those two steps leaves behind; `open` discards its records, because the snapshot
//! already holds them (replaying them would, for example, duplicate retry history).
//!
//! # Durability and ordering
//!
//! [`TaskStore::append_durable`] enqueues the record **synchronously** (so the on-disk order is the
//! order of the calls) and returns a [`DurableAck`] that resolves after a `sync_data()` covering
//! the record. Durable appends arriving within [`GROUP_COMMIT_WINDOW`] (up to [`MAX_GROUP_BATCH`]
//! records) share one `sync_data()`. [`TaskStore::append`] enqueues without waiting for a sync.
//!
//! The intended pattern for a queue guarded by a lock is: enqueue under the lock, drop the lock,
//! then await the ack. Awaiting while holding the lock serializes every commit behind its own
//! `sync_data()` and defeats the group commit.
//!
//! Known limits (documented, not bugs): a lone durable append waits up to the 5 ms window for
//! companions; the command channel to the writer is unbounded; `open` reads whole files into
//! memory; dropping the store joins the writer thread (it drains and syncs first, normally
//! milliseconds), so drop it from a shutdown path or `spawn_blocking`, not from a hot async path.
//!
//! # Compaction contract
//!
//! The store cannot see the queue, so the caller builds the [`SnapshotImage`]. The image must
//! cover exactly the records appended before the `compact` call: build it, enqueue `compact`, and
//! enqueue appends under the same lock.
//!
//! # Torn tails and corruption
//!
//! A frame whose header check is valid but whose payload runs past the end of the file is a torn
//! write: it is truncated, with no scan. A frame whose header check is invalid (bit rot in the
//! length, a zero-filled tail) or whose BLAKE3 does not match is classified by looking for a valid
//! frame after it, probing only offsets whose header check passes (linear in the tail size, with a
//! bounded hashing budget). If one exists the bad frame is in the middle of acknowledged history
//! and `open` refuses; otherwise it is a torn tail and is truncated. A torn payload that happens
//! to contain journal bytes is therefore still a torn tail.
//!
//! The snapshot's `Bytes` fields keep the core JSON representation (a string if UTF-8, otherwise
//! a number array, up to 4x the raw size), hence the 4 GiB snapshot bound. Moving them to base64
//! later is a format change (the current reader takes a JSON string as raw UTF-8): bump
//! [`STORE_FORMAT_VERSION`]. `compact` takes the image by value and encodes it on the writer
//! thread, so the caller only enqueues.

use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
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
pub const HEADER_LEN: usize = 24;
/// Size of a journal frame header: `payload_len` (4), `len_check` (4) and the BLAKE3 hash (32).
pub const FRAME_HEADER_LEN: usize = 4 + 4 + 32;
/// Size of the snapshot frame header: `payload_len` (8) plus the BLAKE3 hash (32).
pub const SNAPSHOT_FRAME_HEADER_LEN: usize = 8 + 32;
/// Upper bound of the snapshot payload (JSON) in bytes.
pub const SNAPSHOT_MAX_BYTES: u64 = 4 * 1024 * 1024 * 1024;
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
    pub adoptable_by_worker: Option<Uuid>,
    #[serde(default)]
    pub fail_if_not_adopted: bool,
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
    /// True if a journal older than the snapshot (crash during compaction) was discarded.
    pub stale_journal_discarded: bool,
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

/// Resolves when the record it was returned for is on disk (a `sync_data()` covered it), or with
/// the error that stopped the writer. Dropping it does not cancel the write.
#[derive(Debug)]
pub struct DurableAck(oneshot::Receiver<GridResult<()>>);

impl DurableAck {
    /// Blocks the calling thread until the record is durable. Must not be called from inside a
    /// tokio runtime worker; `.await` the ack there.
    pub fn wait(self) -> GridResult<()> {
        self.0
            .blocking_recv()
            .unwrap_or_else(|_| Err(writer_stopped()))
    }
}

impl Future for DurableAck {
    type Output = GridResult<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map(|r| r.unwrap_or_else(|_| Err(writer_stopped())))
    }
}

fn writer_stopped() -> GridError {
    GridError::Storage("the store writer stopped".into())
}

enum Cmd {
    Append { frame: Vec<u8>, ack: Option<Ack> },
    Compact { image: SnapshotImage, ack: Ack },
}

#[derive(Default)]
struct Shared {
    sync_calls: AtomicU64,
    journal_bytes: AtomicU64,
    journal_records: AtomicU64,
    pending_compactions: AtomicU64,
    compaction_due: AtomicBool,
    failed: Mutex<Option<String>>,
    /// Test-only fault injection: 1 = `sync_data` fails, 2 = the writer panics at the sync.
    #[cfg(test)]
    inject: std::sync::atomic::AtomicU8,
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
        let lock = open_rw(&lock_path)?;
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
        let snapshot = read_snapshot(&snapshot_path)?;
        let snapshot_generation = snapshot.as_ref().map_or(0, |s| s.generation);

        let journal_path = dir.join(JOURNAL_FILE);
        let loaded = read_journal(&journal_path, snapshot_generation)?;
        if let Some(s) = &snapshot {
            if s.version != PROTOCOL_VERSION_CURRENT {
                // Self-describing JSON: carry it over to the current protocol version.
                write_snapshot_file(&dir, s.generation, &s.image)?;
            }
        }

        let mut journal = open_rw(&journal_path)?;
        let journal_len = if loaded.reset {
            reset_journal(&mut journal, &dir, snapshot_generation, "journal_header")?;
            sync_dir(&dir, "journal_dir")?;
            HEADER_LEN as u64
        } else {
            if loaded.truncated_tail {
                journal
                    .set_len(loaded.valid_len)
                    .and_then(|()| sync_file_all(&journal, &dir, "journal_tail"))
                    .map_err(|e| storage_io(&journal_path, "truncate", e))?;
            }
            journal
                .seek(SeekFrom::End(0))
                .map_err(|e| storage_io(&journal_path, "seek in", e))?;
            loaded.valid_len
        };

        let shared = Arc::new(Shared::default());
        shared.journal_bytes.store(journal_len, Ordering::Relaxed);
        shared
            .journal_records
            .store(loaded.records.len() as u64, Ordering::Relaxed);
        let writer = Writer {
            dir: dir.clone(),
            journal,
            _lock: lock,
            generation: snapshot_generation,
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
            snapshot: snapshot.map(|s| s.image),
            records: loaded.records,
            truncated_tail: loaded.truncated_tail,
            stale_journal_discarded: loaded.stale,
        };
        Ok((store, recovered))
    }

    /// The state directory this store owns.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Enqueues `record` without waiting for a sync.
    pub fn append(&self, record: &JournalRecord) -> GridResult<()> {
        let frame = encode_record_frame(record)?;
        self.send(Cmd::Append { frame, ack: None })
    }

    /// Enqueues `record` now (fixing its place in the journal) and returns an ack that resolves
    /// once a `sync_data()` covering it has completed. Call it under the lock that orders your
    /// mutations, then drop the lock and await the ack.
    pub fn append_durable(&self, record: &JournalRecord) -> GridResult<DurableAck> {
        let frame = encode_record_frame(record)?;
        let (ack, rx) = oneshot::channel();
        self.send(Cmd::Append {
            frame,
            ack: Some(ack),
        })?;
        Ok(DurableAck(rx))
    }

    /// `append_durable(record)?.wait()` for non-async callers.
    pub fn append_durable_blocking(&self, record: &JournalRecord) -> GridResult<()> {
        self.append_durable(record)?.wait()
    }

    /// Enqueues a compaction to `image` (see the module docs for the contract) and returns an ack
    /// that resolves when the snapshot is installed and the journal reset. The image is encoded
    /// on the writer thread.
    pub fn compact(&self, image: SnapshotImage) -> GridResult<DurableAck> {
        let (ack, rx) = oneshot::channel();
        self.shared
            .pending_compactions
            .fetch_add(1, Ordering::AcqRel);
        self.shared.compaction_due.store(false, Ordering::Release);
        if let Err(err) = self.send(Cmd::Compact { image, ack }) {
            self.shared
                .pending_compactions
                .fetch_sub(1, Ordering::AcqRel);
            return Err(err);
        }
        Ok(DurableAck(rx))
    }

    /// `compact(image)?.wait()` for non-async callers.
    pub fn compact_blocking(&self, image: SnapshotImage) -> GridResult<()> {
        self.compact(image)?.wait()
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

    /// Returns the error reason if the store writer has failed since [`TaskStore::open`], or
    /// `None` while durability is healthy. Once set, durability is gone until the master restarts.
    pub fn failure_reason(&self) -> Option<String> {
        self.shared.failed.lock().unwrap().clone()
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn record_failure_for_test(&self, reason: impl Into<String>) {
        let mut slot = self.shared.failed.lock().unwrap();
        if slot.is_none() {
            *slot = Some(reason.into());
        }
    }

    /// Injects a sync failure (`1`) or writer panic (`2`) for fault-injection tests.
    #[cfg(test)]
    pub(crate) fn set_inject(&self, mode: u8) {
        self.shared.inject.store(mode, Ordering::SeqCst);
    }

    fn send(&self, cmd: Cmd) -> GridResult<()> {
        if let Some(reason) = self.shared.failed.lock().unwrap().clone() {
            return Err(GridError::Storage(reason));
        }
        self.tx
            .as_ref()
            .ok_or_else(|| GridError::Storage("the store is closed".into()))?
            .send(cmd)
            .map_err(|_| writer_stopped())
    }
}

impl Drop for TaskStore {
    fn drop(&mut self) {
        // Closing the channel makes the writer drain, sync, and release the lock when it exits.
        // The join blocks the caller for that drain (normally milliseconds).
        self.tx.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct Writer {
    dir: PathBuf,
    /// Positioned at the end of the journal; only this thread writes.
    journal: File,
    /// Held only so the lock lives as long as the writer.
    _lock: File,
    generation: u64,
    bytes: u64,
    records: u64,
    options: StoreOptions,
    shared: Arc<Shared>,
}

impl Drop for Writer {
    fn drop(&mut self) {
        // Unlock `store.lock` explicitly on the kernel open file description so a concurrent
        // `fork()` in another thread of the same process (before `execve` closes `O_CLOEXEC`
        // descriptors) or an unwinding writer thread cannot keep `flock` held past `join()`.
        let _ = self._lock.unlock();
    }
}

impl Writer {
    fn run(mut self, rx: mpsc::Receiver<Cmd>) {
        let mut next = rx.recv().ok();
        while let Some(cmd) = next {
            next = match cmd {
                Cmd::Append { frame, ack } => self.handle_append(frame, ack, &rx),
                Cmd::Compact { image, ack } => {
                    let res = self.check_failed().and_then(|()| self.compact(&image));
                    self.shared
                        .pending_compactions
                        .fetch_sub(1, Ordering::AcqRel);
                    self.update_due();
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
        #[cfg(test)]
        match self.shared.inject.load(Ordering::SeqCst) {
            1 => return Err(GridError::Storage("injected sync failure".into())),
            2 => panic!("injected writer panic"),
            _ => {}
        }
        self.shared.sync_calls.fetch_add(1, Ordering::Relaxed);
        sync_file_data(&self.journal, &self.dir, "journal_data")
            .map_err(|e| storage_io(&self.dir.join(JOURNAL_FILE), "sync", e))
    }

    fn compact(&mut self, image: &SnapshotImage) -> GridResult<()> {
        let next = self.generation + 1;
        write_snapshot_file(&self.dir, next, image)?;
        reset_journal(&mut self.journal, &self.dir, next, "journal_reset")?;
        self.generation = next;
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
        let due = self.shared.pending_compactions.load(Ordering::Acquire) == 0
            && (self.bytes > self.options.compact_max_bytes
                || self.records > self.options.compact_max_records);
        self.shared.compaction_due.store(due, Ordering::Release);
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

/// Test-only record of fsync calls as `(state dir, kind)`, so tests can check that each
/// durability step happens and in which order.
#[cfg(test)]
static FSYNC_LOG: Mutex<Vec<(PathBuf, &'static str)>> = Mutex::new(Vec::new());

fn note_fsync(_dir: &Path, _kind: &'static str) {
    #[cfg(test)]
    FSYNC_LOG.lock().unwrap().push((_dir.to_path_buf(), _kind));
}

/// The only places that call `sync_all` / `sync_data`: each syncs and then records `kind`, so a
/// test that expects a log entry fails when the sync is skipped.
fn sync_file_all(file: &File, dir: &Path, kind: &'static str) -> std::io::Result<()> {
    file.sync_all()?;
    note_fsync(dir, kind);
    Ok(())
}

fn sync_file_data(file: &File, dir: &Path, kind: &'static str) -> std::io::Result<()> {
    file.sync_data()?;
    note_fsync(dir, kind);
    Ok(())
}

/// `fsync` of a directory so a create or rename in it survives a crash. A no-op on Windows,
/// which has no directory fsync.
fn sync_dir(dir: &Path, kind: &'static str) -> GridResult<()> {
    #[cfg(unix)]
    File::open(dir)
        .and_then(|d| sync_file_all(&d, dir, kind))
        .map_err(|e| storage_io(dir, "sync", e))?;
    #[cfg(not(unix))]
    let _ = (dir, kind);
    Ok(())
}

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

/// Opens `path` read-write (not append: Windows append handles cannot `set_len`), creating it with
/// mode 0600 on Unix. The position is the start of the file.
fn open_rw(path: &Path) -> GridResult<File> {
    let mut opts = OpenOptions::new();
    opts.read(true).write(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    opts.open(path).map_err(|e| storage_io(path, "open", e))
}

fn header_bytes(magic: &[u8; 8], generation: u64) -> [u8; HEADER_LEN] {
    let mut h = [0u8; HEADER_LEN];
    h[..8].copy_from_slice(magic);
    h[8..12].copy_from_slice(&STORE_FORMAT_VERSION.to_le_bytes());
    h[12..16].copy_from_slice(&PROTOCOL_VERSION_CURRENT.to_le_bytes());
    h[16..24].copy_from_slice(&generation.to_le_bytes());
    h
}

/// Empties the journal and writes a fresh header, leaving the position after it; fsyncs the file.
fn reset_journal(
    journal: &mut File,
    dir: &Path,
    generation: u64,
    kind: &'static str,
) -> GridResult<()> {
    let path = dir.join(JOURNAL_FILE);
    journal
        .set_len(0)
        .and_then(|()| journal.seek(SeekFrom::Start(0)))
        .and_then(|_| journal.write_all(&header_bytes(JOURNAL_MAGIC, generation)))
        .and_then(|()| sync_file_all(journal, dir, kind))
        .map_err(|e| storage_io(&path, "reset", e))
}

struct Header {
    protocol: u32,
    generation: u64,
}

/// Validates the magic and format version of a complete header.
fn parse_header(path: &Path, magic: &[u8; 8], bytes: &[u8]) -> GridResult<Header> {
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
    Ok(Header {
        protocol: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
        generation: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
    })
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
    frame.extend_from_slice(&len_check(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(blake3::hash(payload).as_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

/// Header check of a frame length: the murmur3 32-bit finalizer of the length (a bijection, so
/// any change of the length alone is detected) xor a constant (so a zeroed header never passes).
/// Cheap enough to probe every byte offset of a damaged tail.
pub fn len_check(len: u32) -> u32 {
    let mut x = len ^ 0x9E37_79B9;
    x ^= x >> 16;
    x = x.wrapping_mul(0x85EB_CA6B);
    x ^= x >> 13;
    x = x.wrapping_mul(0xC2B2_AE35);
    x ^ (x >> 16)
}

fn encode_record_frame(record: &JournalRecord) -> GridResult<Vec<u8>> {
    let payload = bincode::serialize(record)
        .map_err(|e| GridError::Storage(format!("cannot encode a journal record: {e}")))?;
    encode_frame(&payload)
}

/// Encodes `image` and writes `snapshot.bin.tmp` (header, `[len u64][blake3][json]`), syncs it,
/// renames it over `snapshot.bin`, and syncs the directory. The JSON is not copied into a frame.
fn write_snapshot_file(dir: &Path, generation: u64, image: &SnapshotImage) -> GridResult<()> {
    let payload = serde_json::to_vec(image)?;
    if payload.len() as u64 > SNAPSHOT_MAX_BYTES {
        return Err(GridError::Storage(format!(
            "the snapshot is {} bytes, above the {SNAPSHOT_MAX_BYTES} byte bound",
            payload.len()
        )));
    }
    let tmp = dir.join(SNAPSHOT_TMP_FILE);
    let dest = dir.join(SNAPSHOT_FILE);
    let _ = std::fs::remove_file(&tmp);
    let mut opts = OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts.open(&tmp).map_err(|e| storage_io(&tmp, "create", e))?;
    file.write_all(&header_bytes(SNAPSHOT_MAGIC, generation))
        .and_then(|()| file.write_all(&(payload.len() as u64).to_le_bytes()))
        .and_then(|()| file.write_all(blake3::hash(&payload).as_bytes()))
        .and_then(|()| file.write_all(&payload))
        .and_then(|()| sync_file_all(&file, dir, "snapshot_file"))
        .map_err(|e| storage_io(&tmp, "write", e))?;
    drop(file);
    std::fs::rename(&tmp, &dest).map_err(|e| storage_io(&dest, "install", e))?;
    sync_dir(dir, "snapshot_dir")
}

struct LoadedSnapshot {
    image: SnapshotImage,
    version: u32,
    generation: u64,
}

/// Reads and verifies `snapshot.bin`. It is installed by rename, so it has no torn-tail
/// tolerance: any damage is an error.
fn read_snapshot(path: &Path) -> GridResult<Option<LoadedSnapshot>> {
    let mut bytes = Vec::new();
    match File::open(path) {
        Ok(mut f) => {
            f.read_to_end(&mut bytes)
                .map_err(|e| storage_io(path, "read", e))?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(storage_io(path, "open", e)),
    }
    let header = parse_header(path, SNAPSHOT_MAGIC, &bytes)?;
    let corrupt = |why: &str| GridError::Storage(format!("{} is corrupt: {why}", path.display()));
    let body = &bytes[HEADER_LEN..];
    if body.len() < SNAPSHOT_FRAME_HEADER_LEN {
        return Err(corrupt("truncated frame header"));
    }
    let len = u64::from_le_bytes(body[..8].try_into().unwrap());
    if len > SNAPSHOT_MAX_BYTES {
        return Err(corrupt("frame length exceeds the maximum"));
    }
    if body.len() as u64 != SNAPSHOT_FRAME_HEADER_LEN as u64 + len {
        return Err(corrupt("frame length does not match the file size"));
    }
    let payload = &body[SNAPSHOT_FRAME_HEADER_LEN..];
    if blake3::hash(payload).as_bytes() != &body[8..SNAPSHOT_FRAME_HEADER_LEN] {
        return Err(corrupt("checksum mismatch"));
    }
    let image = serde_json::from_slice(payload)
        .map_err(|e| corrupt(&format!("cannot decode the snapshot: {e}")))?;
    Ok(Some(LoadedSnapshot {
        image,
        version: header.protocol,
        generation: header.generation,
    }))
}

struct LoadedJournal {
    records: Vec<JournalRecord>,
    /// Length of the valid prefix (header plus whole frames).
    valid_len: u64,
    truncated_tail: bool,
    /// The file must be recreated with a fresh header (missing, torn or all-zero header, a
    /// header-only journal of another protocol version, or a stale generation).
    reset: bool,
    /// The records were discarded because the journal predates the snapshot.
    stale: bool,
}

enum Scan {
    Nothing,
    Found(usize),
    /// Too much hashing would be needed to classify the tail; treated as corruption.
    TooCostly,
}

/// Looks for a complete valid frame at or after `from`, probing only offsets whose header check
/// passes. Used to tell a torn tail (nothing valid follows) from damage in the middle of the
/// history. Hashing work is capped at a few times the file size.
fn valid_frame_follows(bytes: &[u8], from: usize) -> Scan {
    let total = bytes.len();
    let mut budget = 4 * total + (1 << 20);
    let mut o = from;
    while o + FRAME_HEADER_LEN <= total {
        let len = u32::from_le_bytes(bytes[o..o + 4].try_into().unwrap());
        let chk = u32::from_le_bytes(bytes[o + 4..o + 8].try_into().unwrap());
        if chk == len_check(len) && len as usize <= MAX_FRAME_SIZE {
            let end = o + FRAME_HEADER_LEN + len as usize;
            if end <= total {
                if len as usize > budget {
                    return Scan::TooCostly;
                }
                budget -= len as usize;
                if blake3::hash(&bytes[o + FRAME_HEADER_LEN..end]).as_bytes()
                    == &bytes[o + 8..o + FRAME_HEADER_LEN]
                {
                    return Scan::Found(o);
                }
            }
        }
        o += 1;
    }
    Scan::Nothing
}

fn read_journal(path: &Path, snapshot_generation: u64) -> GridResult<LoadedJournal> {
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
        reset: false,
        stale: false,
    };
    if bytes.len() < HEADER_LEN {
        // Empty, or the process died while writing the header: only a prefix of ours (or zeros,
        // when the OS extended the file) is allowed.
        let n = bytes.len().min(8);
        let zeros = bytes.iter().all(|b| *b == 0);
        if !zeros && bytes[..n] != JOURNAL_MAGIC[..n] {
            return Err(GridError::Storage(format!(
                "{} is not a task store file (bad magic); refusing to open",
                path.display()
            )));
        }
        out.reset = true;
        return Ok(out);
    }
    if bytes[..HEADER_LEN].iter().all(|b| *b == 0) && bytes.iter().all(|b| *b == 0) {
        warn!("task store: {} is all zeros; recreating it", path.display());
        out.reset = true;
        return Ok(out);
    }
    let header = parse_header(path, JOURNAL_MAGIC, &bytes)?;
    if header.generation < snapshot_generation {
        warn!(
            "task store: {} (generation {}) predates the snapshot (generation {}); a compaction \
             was interrupted, so its records are already in the snapshot and are discarded",
            path.display(),
            header.generation,
            snapshot_generation
        );
        out.reset = true;
        out.stale = true;
        return Ok(out);
    }
    if header.generation > snapshot_generation {
        return Err(GridError::Storage(format!(
            "{} has generation {} but the snapshot has generation {snapshot_generation}; the \
             snapshot is missing or was replaced; refusing to open",
            path.display(),
            header.generation
        )));
    }
    if bytes.len() == HEADER_LEN {
        out.reset = header.protocol != PROTOCOL_VERSION_CURRENT;
        return Ok(out);
    }
    if header.protocol != PROTOCOL_VERSION_CURRENT {
        return Err(GridError::Storage(format!(
            "{} holds uncompacted records written by protocol version {}, but this binary \
             is protocol version {PROTOCOL_VERSION_CURRENT}; start the previous binary and stop \
             it cleanly (which compacts the journal) before upgrading",
            path.display(),
            header.protocol
        )));
    }

    let total = bytes.len();
    let mut off = HEADER_LEN;
    while off < total {
        // `scan_from` is where to look for later valid frames when this one is bad.
        let bad: Option<(&str, usize)> = if total - off < FRAME_HEADER_LEN {
            Some(("short frame header", off + 1))
        } else {
            let len = u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap());
            let chk = u32::from_le_bytes(bytes[off + 4..off + 8].try_into().unwrap());
            if chk != len_check(len) {
                Some(("damaged frame header", off + 1))
            } else if len as usize > MAX_FRAME_SIZE {
                return Err(GridError::Storage(format!(
                    "{} is corrupt: the frame at offset {off} claims {len} bytes (maximum {MAX_FRAME_SIZE})",
                    path.display()
                )));
            } else {
                let end = off + FRAME_HEADER_LEN + len as usize;
                if end > total {
                    // A torn write: the header is intact and the payload never fully landed.
                    warn!(
                        "task store: short frame payload at offset {off} of {}; discarding the torn tail ({} records kept)",
                        path.display(),
                        out.records.len()
                    );
                    out.valid_len = off as u64;
                    out.truncated_tail = true;
                    return Ok(out);
                } else if blake3::hash(&bytes[off + FRAME_HEADER_LEN..end]).as_bytes()
                    != &bytes[off + 8..off + FRAME_HEADER_LEN]
                {
                    Some(("checksum mismatch", end))
                } else {
                    None
                }
            }
        };
        if let Some((why, scan_from)) = bad {
            let detail = match valid_frame_follows(&bytes, scan_from) {
                Scan::Nothing => None,
                Scan::Found(later) => Some(format!("a valid frame follows at offset {later}")),
                Scan::TooCostly => Some("the damaged tail is too large to classify".to_string()),
            };
            if let Some(detail) = detail {
                return Err(GridError::Storage(format!(
                    "{} is corrupt: the frame at offset {off} is bad ({why}) and {detail}; \
                     refusing to drop acknowledged history",
                    path.display()
                )));
            }
            warn!(
                "task store: {why} at offset {off} of {}; discarding the torn tail ({} records kept)",
                path.display(),
                out.records.len()
            );
            out.valid_len = off as u64;
            out.truncated_tail = true;
            return Ok(out);
        }
        let len = u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap()) as usize;
        let end = off + FRAME_HEADER_LEN + len;
        let record = bincode::deserialize(&bytes[off + FRAME_HEADER_LEN..end]).map_err(|e| {
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
        let huge = (MAX_FRAME_SIZE as u32) + 1;
        bytes[HEADER_LEN..HEADER_LEN + 4].copy_from_slice(&huge.to_le_bytes());
        bytes[HEADER_LEN + 4..HEADER_LEN + 8].copy_from_slice(&len_check(huge).to_le_bytes());
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
                adoptable_by_worker: None,
                fail_if_not_adopted: false,
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
            store.compact_blocking(image.clone()).unwrap();
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
        store.compact_blocking(SnapshotImage::default()).unwrap();
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
    async fn group_commit_shares_syncs_between_enqueued_appends() {
        const N: u64 = 1_000;
        let dir = tempfile::tempdir().unwrap();
        let (store, _) = TaskStore::open(dir.path()).unwrap();
        let acks: Vec<_> = (0..N)
            .map(|i| store.append_durable(&marker(i)).unwrap())
            .collect();
        for res in join_all(acks).await {
            res.unwrap();
        }
        let syncs = store.sync_count();
        assert_eq!(store.journal_records(), N);
        assert!(syncs >= 1, "no sync happened");
        assert!(syncs <= N / 10, "{syncs} syncs for {N} appends");
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
            store.compact_blocking(image.clone()).unwrap();
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
            store.compact_blocking(image_with(1)).unwrap();
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

    // ---- review fixes -------------------------------------------------------------------

    fn rec_kinds(rec: &Recovered) -> Vec<&'static str> {
        rec.records
            .iter()
            .map(|r| match r {
                JournalRecord::TaskSubmitted { .. } => "TaskSubmitted",
                JournalRecord::TaskEvicted { .. } => "TaskEvicted",
                _ => "other",
            })
            .collect()
    }

    fn header_generation(path: &Path) -> u64 {
        let bytes = std::fs::read(path).unwrap();
        u64::from_le_bytes(bytes[16..24].try_into().unwrap())
    }

    fn fsync_kinds(dir: &Path) -> Vec<&'static str> {
        FSYNC_LOG
            .lock()
            .unwrap()
            .iter()
            .filter(|(d, _)| d == dir)
            .map(|(_, k)| *k)
            .collect()
    }

    fn assert_subsequence(log: &[&str], wanted: &[&str]) {
        let mut it = log.iter();
        for w in wanted {
            assert!(
                it.any(|k| k == w),
                "fsync kind {w:?} missing or out of order in {log:?}"
            );
        }
    }

    // A flipped bit in the length of a MIDDLE frame must not silently drop later frames.
    #[test]
    fn middle_frame_length_bitrot_is_refused_and_leaves_the_file_alone() {
        let dir = tempfile::tempdir().unwrap();
        let records: Vec<_> = (0..4).map(|i| submitted("t", i)).collect();
        write_all(dir.path(), &records);
        let path = journal_path(dir.path());
        let mut bytes = std::fs::read(&path).unwrap();
        let first = encode_record_frame(&records[0]).unwrap().len();
        bytes[HEADER_LEN + first + 2] ^= 0x10; // length ~1 MiB, below MAX_FRAME_SIZE
        std::fs::write(&path, &bytes).unwrap();
        let msg = expect_storage_err(TaskStore::open(dir.path()));
        assert!(msg.contains("valid frame"), "{msg}");
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn zero_filled_tail_is_truncated() {
        let dir = tempfile::tempdir().unwrap();
        write_all(dir.path(), &[marker(1), marker(2)]);
        let path = journal_path(dir.path());
        let good = std::fs::metadata(&path).unwrap().len();
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[0u8; 4096]).unwrap();
        drop(f);
        let (s, rec) = TaskStore::open(dir.path()).unwrap();
        assert!(rec.truncated_tail);
        assert_eq!(rec.records.len(), 2);
        drop(s);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), good);
    }

    #[test]
    fn hash_valid_undecodable_frame_is_refused_last_and_middle() {
        let bad = encode_frame(&[0xFFu8; 12]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        write_all(dir.path(), &[marker(1)]);
        let mut f = OpenOptions::new()
            .append(true)
            .open(journal_path(dir.path()))
            .unwrap();
        f.write_all(&bad).unwrap();
        drop(f);
        expect_storage_err(TaskStore::open(dir.path()));

        let dir = tempfile::tempdir().unwrap();
        write_all(dir.path(), &[marker(1)]);
        let mut f = OpenOptions::new()
            .append(true)
            .open(journal_path(dir.path()))
            .unwrap();
        f.write_all(&bad).unwrap();
        f.write_all(&encode_record_frame(&marker(2)).unwrap())
            .unwrap();
        drop(f);
        expect_storage_err(TaskStore::open(dir.path()));
    }

    #[test]
    fn torn_or_zeroed_journal_headers_are_recreated() {
        for keep in [0usize, 3, 8, 10, 15, 23] {
            let dir = tempfile::tempdir().unwrap();
            drop(TaskStore::open(dir.path()).unwrap());
            let path = journal_path(dir.path());
            let f = OpenOptions::new().write(true).open(&path).unwrap();
            f.set_len(keep as u64).unwrap();
            drop(f);
            let (s, rec) = TaskStore::open(dir.path()).unwrap_or_else(|e| panic!("{keep}: {e}"));
            assert!(rec.records.is_empty());
            s.append_durable_blocking(&marker(1)).unwrap();
            drop(s);
            let (_s, rec) = TaskStore::open(dir.path()).unwrap();
            assert_eq!(rec.records.len(), 1, "keep={keep}");
        }
        for zeros in [16usize, HEADER_LEN, 4096] {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(journal_path(dir.path()), vec![0u8; zeros]).unwrap();
            let (s, rec) = TaskStore::open(dir.path()).unwrap_or_else(|e| panic!("{zeros}: {e}"));
            assert!(rec.records.is_empty());
            s.append_durable_blocking(&marker(1)).unwrap();
        }
    }

    #[test]
    fn crash_after_tmp_before_rename_keeps_the_old_state() {
        let dir = tempfile::tempdir().unwrap();
        write_all(dir.path(), &[marker(1), marker(2)]);
        std::fs::write(dir.path().join(SNAPSHOT_TMP_FILE), b"half a snapshot").unwrap();
        let (_s, rec) = TaskStore::open(dir.path()).unwrap();
        assert!(rec.snapshot.is_none());
        assert_eq!(rec.records, vec![marker(1), marker(2)]);
        assert!(!dir.path().join(SNAPSHOT_TMP_FILE).exists());
    }

    #[test]
    fn crash_after_rename_before_reset_discards_the_stale_journal() {
        let dir = tempfile::tempdir().unwrap();
        let image = SnapshotImage {
            next_sequence: 3,
            ..Default::default()
        };
        let saved;
        {
            let (store, _) = TaskStore::open(dir.path()).unwrap();
            for i in 0..3 {
                store.append_durable_blocking(&submitted("x", i)).unwrap();
            }
            saved = std::fs::read(journal_path(dir.path())).unwrap();
            store.compact_blocking(image.clone()).unwrap();
        }
        assert_eq!(header_generation(&dir.path().join(SNAPSHOT_FILE)), 1);
        assert_eq!(header_generation(&journal_path(dir.path())), 1);
        // Put the pre-compaction journal (generation 0) back: the crash window.
        std::fs::write(journal_path(dir.path()), &saved).unwrap();
        let (store, rec) = TaskStore::open(dir.path()).unwrap();
        assert_eq!(rec.snapshot.as_ref(), Some(&image));
        assert!(rec.records.is_empty());
        assert!(rec.stale_journal_discarded);
        store.append_durable_blocking(&marker(5)).unwrap();
        drop(store);
        assert_eq!(header_generation(&journal_path(dir.path())), 1);
        let (_s, rec) = TaskStore::open(dir.path()).unwrap();
        assert_eq!(rec.records, vec![marker(5)]);
        assert!(!rec.stale_journal_discarded);
    }

    #[test]
    fn a_journal_newer_than_the_snapshot_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (store, _) = TaskStore::open(dir.path()).unwrap();
            store.compact_blocking(image_with(1)).unwrap();
            store.append_durable_blocking(&marker(1)).unwrap();
        }
        std::fs::remove_file(dir.path().join(SNAPSHOT_FILE)).unwrap();
        let msg = expect_storage_err(TaskStore::open(dir.path()));
        assert!(msg.contains("generation"), "{msg}");
    }

    #[tokio::test]
    async fn a_durable_future_created_first_lands_on_disk_first() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (store, _) = TaskStore::open(dir.path()).unwrap();
            let ack = store.append_durable(&submitted("first", 1)).unwrap();
            store.append(&marker(2)).unwrap();
            ack.await.unwrap();
        }
        let (_s, rec) = TaskStore::open(dir.path()).unwrap();
        assert_eq!(rec_kinds(&rec), ["TaskSubmitted", "TaskEvicted"]);
    }

    async fn every_waiter_gets_an_error(mode: u8) {
        let dir = tempfile::tempdir().unwrap();
        let (store, _) = TaskStore::open(dir.path()).unwrap();
        let store = Arc::new(store);
        store.shared.inject.store(mode, Ordering::SeqCst);
        let mut handles = Vec::new();
        for i in 0..600u64 {
            let s = Arc::clone(&store);
            handles.push(tokio::spawn(
                async move { s.append_durable(&marker(i))?.await },
            ));
        }
        for h in handles {
            let r = tokio::time::timeout(Duration::from_secs(10), h)
                .await
                .expect("a waiter hung")
                .unwrap();
            assert!(r.is_err(), "mode {mode}: a durable append succeeded");
        }
        let later = match store.append_durable(&marker(9999)) {
            Ok(ack) => ack.await,
            Err(e) => Err(e),
        };
        assert!(later.is_err());
        assert!(store.append(&marker(10000)).is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_sync_failure_reaches_every_waiter() {
        every_waiter_gets_an_error(1).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_writer_panic_reaches_every_waiter() {
        every_waiter_gets_an_error(2).await;
    }

    #[test]
    fn a_snapshot_with_a_20_mib_binary_result_compacts_and_reloads() {
        let dir = tempfile::tempdir().unwrap();
        let t = task("big");
        let id = t.id;
        let result = TaskResult::success(Uuid::new_v4(), id, vec![0xFFu8; 20 << 20], 1, false);
        let image = SnapshotImage {
            next_sequence: 1,
            tasks: vec![PersistedTaskEntry {
                task: t,
                priority: 0,
                sequence: 0,
                state: TaskState::Completed,
                assigned_worker_id: None,
                submitted_at_utc: 0,
                retry_count: 0,
                failover_count: 0,
                evacuation_count: 0,
                rejection_count: 0,
                avoid_workers: vec![],
                adoptable_by_worker: None,
                fail_if_not_adopted: false,
                retry_history: vec![],
                result: Some(result),
                error_message: None,
            }],
            terminal_order: vec![id],
        };
        {
            let (store, _) = TaskStore::open(dir.path()).unwrap();
            store.compact_blocking(image.clone()).unwrap();
        }
        let (_s, rec) = TaskStore::open(dir.path()).unwrap();
        assert!(rec.snapshot.as_ref() == Some(&image));
    }

    // The documented pattern: enqueue under the lock, await after dropping it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn enqueue_under_a_lock_and_await_outside_batches_commits() {
        const N: u64 = 1_000;
        let dir = tempfile::tempdir().unwrap();
        let (store, _) = TaskStore::open(dir.path()).unwrap();
        let store = Arc::new(store);
        let lock = Arc::new(Mutex::new(()));
        let mut hs = Vec::new();
        for i in 0..N {
            let (s, l) = (Arc::clone(&store), Arc::clone(&lock));
            hs.push(tokio::spawn(async move {
                let ack = {
                    let _g = l.lock().unwrap();
                    s.append_durable(&marker(i)).unwrap()
                };
                ack.await.unwrap();
            }));
        }
        for h in hs {
            h.await.unwrap();
        }
        let syncs = store.sync_count();
        assert!(syncs <= N / 2, "{syncs} syncs for {N} appends");
        drop(store);
        let (_s, rec) = TaskStore::open(dir.path()).unwrap();
        assert_eq!(rec.records.len() as u64, N);
    }

    #[test]
    fn fresh_open_syncs_the_journal_header_then_its_directory() {
        let dir = tempfile::tempdir().unwrap();
        drop(TaskStore::open(dir.path()).unwrap());
        let log = fsync_kinds(dir.path());
        #[cfg(unix)]
        assert_subsequence(&log, &["journal_header", "journal_dir"]);
        #[cfg(not(unix))]
        assert_subsequence(&log, &["journal_header"]);
    }

    #[test]
    fn compaction_syncs_snapshot_then_directory_then_journal_reset() {
        let dir = tempfile::tempdir().unwrap();
        let (store, _) = TaskStore::open(dir.path()).unwrap();
        store.append_durable_blocking(&marker(1)).unwrap();
        store.compact_blocking(image_with(1)).unwrap();
        let log = fsync_kinds(dir.path());
        #[cfg(unix)]
        assert_subsequence(
            &log,
            &[
                "journal_data",
                "snapshot_file",
                "snapshot_dir",
                "journal_reset",
            ],
        );
        #[cfg(not(unix))]
        assert_subsequence(&log, &["journal_data", "snapshot_file", "journal_reset"]);
    }

    // ---- recorded byte layout of the journal --------------------------------------------

    fn layout_task() -> Task {
        let mut t = Task::new(
            TaskSpec::command("echo", vec!["hi".into()]),
            TaskRequirements::generic(1, 10),
        );
        t.id = TaskId(Uuid::from_u128(1));
        t.created_at_utc = 1_700_000_000;
        t
    }

    fn layout_records() -> Vec<(&'static str, JournalRecord)> {
        let id = TaskId(Uuid::from_u128(1));
        let w = Uuid::from_u128(2);
        let attempt = RetryAttempt {
            attempt: 1,
            timestamp_utc: 42,
            failed_worker_id: Some(w),
            reason: "lost".into(),
            exit_code: Some(137),
            error: Some("killed".into()),
        };
        vec![
            (
                "TaskSubmitted",
                JournalRecord::TaskSubmitted {
                    task: Box::new(layout_task()),
                    priority: 9,
                    sequence: 7,
                    submitted_at_utc: 11,
                },
            ),
            (
                "TaskDispatched",
                JournalRecord::TaskDispatched {
                    task_id: id,
                    worker_id: w,
                    timestamp_utc: 12,
                },
            ),
            (
                "TaskRunning",
                JournalRecord::TaskRunning {
                    task_id: id,
                    worker_id: w,
                    timestamp_utc: 13,
                },
            ),
            (
                "CheckpointRecorded",
                JournalRecord::CheckpointRecorded {
                    task_id: id,
                    checkpoint: CheckpointData {
                        sequence: 3,
                        delta_state: vec![1u8, 2, 3].into(),
                        timestamp: 5,
                    },
                },
            ),
            (
                "TaskRequeued",
                JournalRecord::TaskRequeued {
                    task_id: id,
                    state: TaskState::Retrying,
                    retry_count: 2,
                    failover_count: 1,
                    evacuation_count: 3,
                    rejection_count: 4,
                    avoid_workers: vec![w],
                    retry_attempt: Some(attempt.clone()),
                    retry_delay_ms: Some(250),
                },
            ),
            (
                "TaskAdopted",
                JournalRecord::TaskAdopted {
                    task_id: id,
                    worker_id: w,
                    timestamp_utc: 14,
                },
            ),
            (
                "TaskTerminal",
                JournalRecord::TaskTerminal {
                    task_id: id,
                    state: TaskState::Failed,
                    assigned_worker_id: Some(w),
                    retry_count: 2,
                    failover_count: 1,
                    evacuation_count: 3,
                    rejection_count: 4,
                    retry_attempt: Some(attempt),
                    result: Some(TaskResult::failure(
                        w,
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
            ),
            ("TaskEvicted", JournalRecord::TaskEvicted { task_id: id }),
        ]
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Recorded bincode bytes of every `JournalRecord` variant (plus the file header). If this
    /// fails you changed the on-disk format: bump `STORE_FORMAT_VERSION`, then regenerate the
    /// file with `OX_BLESS_JOURNAL_LAYOUT=1 cargo test -p rusty_grid_master --lib store_layout`.
    #[test]
    fn store_layout_is_frozen() {
        let mut lines = vec![format!(
            "header {}",
            hex(&header_bytes(JOURNAL_MAGIC, 0)[..12])
        )];
        let states = [
            TaskState::Submitted,
            TaskState::Queued,
            TaskState::Scheduled,
            TaskState::Running,
            TaskState::Completed,
            TaskState::Failed,
            TaskState::Cancelled,
            TaskState::Retrying,
        ];
        lines.push(format!(
            "TaskState {}",
            hex(&bincode::serialize(&states).unwrap())
        ));
        for (name, rec) in layout_records() {
            lines.push(format!(
                "{name} {}",
                hex(&bincode::serialize(&rec).unwrap())
            ));
        }
        let actual = lines.join("\n") + "\n";
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/journal_layout.txt");
        if std::env::var_os("OX_BLESS_JOURNAL_LAYOUT").is_some() {
            std::fs::create_dir_all(Path::new(path).parent().unwrap()).unwrap();
            std::fs::write(path, &actual).unwrap();
        }
        let expected = include_str!("../tests/data/journal_layout.txt");
        assert_eq!(
            actual.replace("\r\n", "\n"),
            expected.replace("\r\n", "\n"),
            "journal byte layout changed: bump STORE_FORMAT_VERSION and re-bless"
        );
    }

    // A torn final frame whose payload embeds a valid frame is still a torn tail.
    #[test]
    fn torn_frame_with_an_embedded_frame_is_still_a_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        write_all(dir.path(), &[marker(1), marker(2)]);
        let inner = encode_record_frame(&marker(9)).unwrap();
        let outer = encode_record_frame(&JournalRecord::CheckpointRecorded {
            task_id: TaskId(Uuid::from_u128(7)),
            checkpoint: CheckpointData {
                sequence: 1,
                delta_state: inner.into(),
                timestamp: 5,
            },
        })
        .unwrap();
        let path = journal_path(dir.path());
        let good = std::fs::metadata(&path).unwrap().len();
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&outer[..outer.len() - 4]).unwrap();
        drop(f);
        let (s, rec) = TaskStore::open(dir.path()).unwrap();
        assert!(rec.truncated_tail);
        assert_eq!(rec.records.len(), 2);
        drop(s);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), good);
    }

    fn torn_tail_open_time(tail: &[u8]) -> Duration {
        let dir = tempfile::tempdir().unwrap();
        write_all(dir.path(), &[marker(1)]);
        let mut f = OpenOptions::new()
            .append(true)
            .open(journal_path(dir.path()))
            .unwrap();
        // A damaged frame header (no valid check), then garbage.
        f.write_all(&((tail.len() + 1024) as u32).to_le_bytes())
            .unwrap();
        f.write_all(&[0xAB; 36]).unwrap();
        f.write_all(tail).unwrap();
        drop(f);
        let t0 = Instant::now();
        let (_s, rec) = TaskStore::open(dir.path()).unwrap();
        assert!(rec.truncated_tail);
        t0.elapsed()
    }

    #[test]
    fn classifying_a_damaged_tail_is_linear() {
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut rnd = |n: usize| {
            (0..n)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x as u8
                })
                .collect::<Vec<u8>>()
        };
        let random = rnd(8 << 20);
        let t = torn_tail_open_time(&random);
        assert!(t < Duration::from_secs(1), "8 MiB random tail took {t:?}");
        let patterned: Vec<u8> = [0u8, 0, 1, 0]
            .iter()
            .copied()
            .cycle()
            .take(8 << 20)
            .collect();
        let t = torn_tail_open_time(&patterned);
        assert!(
            t < Duration::from_secs(1),
            "8 MiB patterned tail took {t:?}"
        );
    }
}
