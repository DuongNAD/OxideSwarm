//! Durable journal and snapshot store for the master grid mailbox (`TEAM-6`, design 0111 D8).
//!
//! Uses `mail_journal.bin` (`OXSWMLJ1`) and `mail_snapshot.bin` (`OXSWMLS1`) inside `--state-dir`
//! so the mailbox persists independently of `TaskStore`'s `STORE_FORMAT_VERSION`.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::mail::MailMessage;
use rusty_grid_core::protocol::{MAX_FRAME_SIZE, PROTOCOL_VERSION_CURRENT};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tracing::{error, warn};

use crate::store::{
    durable_ack_from_rx, encode_frame, len_check, open_rw, prepare_dir, scan_valid_frame_follows,
    storage_io, sync_dir, sync_file_all, sync_file_data, DurableAck, FRAME_HEADER_LEN, HEADER_LEN,
};

/// Magic bytes at the start of `mail_journal.bin`.
pub const MAIL_JOURNAL_MAGIC: &[u8; 8] = b"OXSWMLJ1";
/// Magic bytes at the start of `mail_snapshot.bin`.
pub const MAIL_SNAPSHOT_MAGIC: &[u8; 8] = b"OXSWMLS1";
/// On-disk format version for mailbox files.
pub const MAIL_STORE_FORMAT_VERSION: u32 = 1;

const MAIL_JOURNAL_FILE: &str = "mail_journal.bin";
const MAIL_SNAPSHOT_FILE: &str = "mail_snapshot.bin";
const MAIL_SNAPSHOT_TMP_FILE: &str = "mail_snapshot.bin.tmp";
const MAIL_LOCK_FILE: &str = "mail_store.lock";
const SNAPSHOT_MAX_BYTES: u64 = u32::MAX as u64;
const SNAPSHOT_FRAME_HEADER_LEN: usize = 8 + 32;
const GROUP_COMMIT_WINDOW: Duration = Duration::from_millis(5);
const MAX_GROUP_BATCH: usize = 128;

/// One mutation recorded in `mail_journal.bin` (bincode-encoded; no `skip_serializing_if`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MailJournalRecord {
    Send { message: MailMessage },
    Ack { consumer: String, up_to_id: u64 },
    Evict { ids: Vec<u64> },
}

/// Point-in-time mailbox snapshot written to `mail_snapshot.bin` (JSON-encoded).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MailSnapshotImage {
    pub next_id: u64,
    #[serde(default)]
    pub messages: Vec<MailMessage>,
    #[serde(default)]
    pub cursors: BTreeMap<String, u64>,
}

/// Everything [`MailStore::open`] recovers from disk.
#[derive(Debug, Default)]
pub struct MailRecovered {
    pub snapshot: Option<MailSnapshotImage>,
    pub records: Vec<MailJournalRecord>,
    pub truncated_tail: bool,
}

type Ack = oneshot::Sender<GridResult<()>>;

enum Cmd {
    Append {
        frame: Vec<u8>,
        ack: Option<Ack>,
    },
    Compact {
        image: MailSnapshotImage,
        ack: Option<Ack>,
    },
}

/// Handle to the durable mailbox journal/snapshot writer.
pub struct MailStore {
    dir: PathBuf,
    tx: Option<mpsc::Sender<Cmd>>,
    thread: Option<JoinHandle<()>>,
    failed: Arc<Mutex<Option<String>>>,
}

impl std::fmt::Debug for MailStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MailStore").field("dir", &self.dir).finish()
    }
}

impl MailStore {
    /// Opens or creates `mail_journal.bin` and `mail_snapshot.bin` inside `state_dir`.
    pub fn open(state_dir: impl AsRef<Path>) -> GridResult<(Self, MailRecovered)> {
        let dir = state_dir.as_ref().to_path_buf();
        prepare_dir(&dir)?;

        let lock_path = dir.join(MAIL_LOCK_FILE);
        let lock = open_rw(&lock_path)?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(GridError::Config(format!(
                    "the mail state directory is in use: {} is locked by another master process",
                    lock_path.display()
                )));
            }
            Err(std::fs::TryLockError::Error(e)) => {
                return Err(storage_io(&lock_path, "lock", e));
            }
        }

        let _ = std::fs::remove_file(dir.join(MAIL_SNAPSHOT_TMP_FILE));

        let snapshot = read_snapshot(&dir.join(MAIL_SNAPSHOT_FILE))?;
        let snapshot_generation = snapshot.as_ref().map_or(0, |s| s.generation);

        let journal_path = dir.join(MAIL_JOURNAL_FILE);
        let loaded = read_journal(&journal_path, snapshot_generation)?;

        let mut journal = open_rw(&journal_path)?;
        if loaded.reset {
            reset_journal(&mut journal, &dir, snapshot_generation)?;
            sync_dir(&dir, "mail_journal_dir")?;
        } else {
            if loaded.truncated_tail {
                journal
                    .set_len(loaded.valid_len)
                    .and_then(|()| sync_file_all(&journal, &dir, "mail_journal_tail"))
                    .map_err(|e| storage_io(&journal_path, "truncate", e))?;
            }
            journal
                .seek(SeekFrom::End(0))
                .map_err(|e| storage_io(&journal_path, "seek in", e))?;
        }

        let failed = Arc::new(Mutex::new(None));
        let writer = Writer {
            dir: dir.clone(),
            journal,
            _lock: lock,
            generation: snapshot_generation,
            failed: Arc::clone(&failed),
        };
        let (tx, rx) = mpsc::channel();
        let thread = std::thread::Builder::new()
            .name("mail-store-writer".into())
            .spawn(move || writer.run(rx))
            .map_err(|e| GridError::Storage(format!("cannot start the mail store writer: {e}")))?;

        let store = Self {
            dir,
            tx: Some(tx),
            thread: Some(thread),
            failed,
        };
        let recovered = MailRecovered {
            snapshot: snapshot.map(|s| s.image),
            records: loaded.records,
            truncated_tail: loaded.truncated_tail,
        };
        Ok((store, recovered))
    }

    /// Enqueues `record` synchronously on the writer queue and returns a [`DurableAck`] resolving
    /// once `sync_data()` covers it.
    pub fn append_durable(&self, record: &MailJournalRecord) -> GridResult<DurableAck> {
        if let Some(reason) = self.failed.lock().unwrap().as_ref() {
            return Err(GridError::Storage(reason.clone()));
        }
        let payload = bincode::serialize(record)
            .map_err(|e| GridError::Storage(format!("cannot encode mail journal record: {e}")))?;
        let frame = encode_frame(&payload)?;
        let (ack, rx) = oneshot::channel();
        self.tx
            .as_ref()
            .ok_or_else(|| GridError::Storage("mail store is closing".into()))?
            .send(Cmd::Append {
                frame,
                ack: Some(ack),
            })
            .map_err(|_| GridError::Storage("mail store writer stopped".into()))?;
        Ok(durable_ack_from_rx(rx))
    }

    /// Enqueues compaction to `image` and resolves once the snapshot is installed and journal reset.
    pub fn compact(&self, image: MailSnapshotImage) -> GridResult<DurableAck> {
        if let Some(reason) = self.failed.lock().unwrap().as_ref() {
            return Err(GridError::Storage(reason.clone()));
        }
        let (ack, rx) = oneshot::channel();
        self.tx
            .as_ref()
            .ok_or_else(|| GridError::Storage("mail store is closing".into()))?
            .send(Cmd::Compact {
                image,
                ack: Some(ack),
            })
            .map_err(|_| GridError::Storage("mail store writer stopped".into()))?;
        Ok(durable_ack_from_rx(rx))
    }

    /// Optionally compacts `image`, then closes the writer channel and joins the writer thread so
    /// `mail_store.lock` is released synchronously.
    pub fn compact_and_close(mut self, image: Option<MailSnapshotImage>) {
        if let (Some(image), Some(tx)) = (image, self.tx.as_ref()) {
            let _ = tx.send(Cmd::Compact { image, ack: None });
        }
        self.tx.take();
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for MailStore {
    fn drop(&mut self) {
        self.tx.take();
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

struct Writer {
    dir: PathBuf,
    journal: File,
    _lock: File,
    generation: u64,
    failed: Arc<Mutex<Option<String>>>,
}

impl Writer {
    fn check_failed(&self) -> GridResult<()> {
        if let Some(reason) = self.failed.lock().unwrap().as_ref() {
            return Err(GridError::Storage(reason.clone()));
        }
        Ok(())
    }

    fn run(mut self, rx: mpsc::Receiver<Cmd>) {
        let mut next = rx.recv().ok();
        while let Some(cmd) = next {
            next = match cmd {
                Cmd::Append { frame, ack } => self.handle_append(frame, ack, &rx),
                Cmd::Compact { image, ack } => {
                    let res = self.check_failed().and_then(|()| self.compact(&image));
                    Self::finish(&self.failed, res, ack);
                    rx.recv().ok()
                }
            };
        }
        if self.check_failed().is_ok() {
            if let Err(e) = sync_file_data(&self.journal, &self.dir, "mail_journal_data") {
                error!("mail store: final sync failed: {e}");
            }
        }
    }

    fn finish(
        failed: &Arc<Mutex<Option<String>>>,
        res: GridResult<()>,
        acks: impl IntoIterator<Item = Ack>,
    ) {
        match res {
            Ok(()) => {
                for ack in acks {
                    let _ = ack.send(Ok(()));
                }
            }
            Err(e) => {
                let mut guard = failed.lock().unwrap();
                if guard.is_none() {
                    *guard = Some(e.to_string());
                }
                let msg = e.to_string();
                for ack in acks {
                    let _ = ack.send(Err(GridError::Storage(msg.clone())));
                }
            }
        }
    }

    fn handle_append(
        &mut self,
        frame: Vec<u8>,
        ack: Option<Ack>,
        rx: &mpsc::Receiver<Cmd>,
    ) -> Option<Cmd> {
        let mut waiters = Vec::new();
        let mut result = self.check_failed().and_then(|()| self.write_frame(&frame));
        waiters.extend(ack);
        let mut count = 1usize;
        let mut deferred = None;
        let deadline = Instant::now() + GROUP_COMMIT_WINDOW;
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
        let result = result.and_then(|()| {
            sync_file_data(&self.journal, &self.dir, "mail_journal_data")
                .map_err(|e| storage_io(&self.dir.join(MAIL_JOURNAL_FILE), "sync", e))
        });
        Self::finish(&self.failed, result, waiters);
        match deferred {
            Some(cmd) => Some(cmd),
            None => rx.recv().ok(),
        }
    }

    fn write_frame(&mut self, frame: &[u8]) -> GridResult<()> {
        let path = self.dir.join(MAIL_JOURNAL_FILE);
        self.journal
            .write_all(frame)
            .map_err(|e| storage_io(&path, "append to", e))
    }

    fn compact(&mut self, image: &MailSnapshotImage) -> GridResult<()> {
        let next = self.generation + 1;
        write_snapshot_file(&self.dir, next, image)?;
        reset_journal(&mut self.journal, &self.dir, next)?;
        self.generation = next;
        Ok(())
    }
}

fn header_bytes(magic: &[u8; 8], generation: u64) -> [u8; HEADER_LEN] {
    let mut h = [0u8; HEADER_LEN];
    h[..8].copy_from_slice(magic);
    h[8..12].copy_from_slice(&MAIL_STORE_FORMAT_VERSION.to_le_bytes());
    h[12..16].copy_from_slice(&PROTOCOL_VERSION_CURRENT.to_le_bytes());
    h[16..24].copy_from_slice(&generation.to_le_bytes());
    h
}

fn reset_journal(journal: &mut File, dir: &Path, generation: u64) -> GridResult<()> {
    let path = dir.join(MAIL_JOURNAL_FILE);
    journal
        .set_len(0)
        .and_then(|()| journal.seek(SeekFrom::Start(0)))
        .and_then(|_| journal.write_all(&header_bytes(MAIL_JOURNAL_MAGIC, generation)))
        .and_then(|()| sync_file_all(journal, dir, "mail_journal_reset"))
        .map_err(|e| storage_io(&path, "reset", e))
}

struct Header {
    generation: u64,
}

fn parse_header(path: &Path, magic: &[u8; 8], bytes: &[u8]) -> GridResult<Header> {
    if bytes.len() < HEADER_LEN || &bytes[..8] != magic {
        return Err(GridError::Storage(format!(
            "{} is not a mail store file (bad magic); refusing to open",
            path.display()
        )));
    }
    let format = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    if format != MAIL_STORE_FORMAT_VERSION {
        return Err(GridError::Storage(format!(
            "{} has mail store format version {format}; expected {MAIL_STORE_FORMAT_VERSION}",
            path.display()
        )));
    }
    Ok(Header {
        generation: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
    })
}

fn write_snapshot_file(dir: &Path, generation: u64, image: &MailSnapshotImage) -> GridResult<()> {
    let payload = serde_json::to_vec(image)?;
    if payload.len() as u64 > SNAPSHOT_MAX_BYTES {
        return Err(GridError::Storage(format!(
            "mail snapshot is {} bytes, above {SNAPSHOT_MAX_BYTES}",
            payload.len()
        )));
    }
    let tmp = dir.join(MAIL_SNAPSHOT_TMP_FILE);
    let dest = dir.join(MAIL_SNAPSHOT_FILE);
    let _ = std::fs::remove_file(&tmp);
    let mut opts = OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts.open(&tmp).map_err(|e| storage_io(&tmp, "create", e))?;
    file.write_all(&header_bytes(MAIL_SNAPSHOT_MAGIC, generation))
        .and_then(|()| file.write_all(&(payload.len() as u64).to_le_bytes()))
        .and_then(|()| file.write_all(blake3::hash(&payload).as_bytes()))
        .and_then(|()| file.write_all(&payload))
        .and_then(|()| sync_file_all(&file, dir, "mail_snapshot_file"))
        .map_err(|e| storage_io(&tmp, "write", e))?;
    drop(file);
    std::fs::rename(&tmp, &dest).map_err(|e| storage_io(&dest, "install", e))?;
    sync_dir(dir, "mail_snapshot_dir")
}

struct LoadedSnapshot {
    image: MailSnapshotImage,
    generation: u64,
}

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
    let header = parse_header(path, MAIL_SNAPSHOT_MAGIC, &bytes)?;
    let corrupt = |why: &str| GridError::Storage(format!("{} is corrupt: {why}", path.display()));
    let body = &bytes[HEADER_LEN..];
    if body.len() < SNAPSHOT_FRAME_HEADER_LEN {
        return Err(corrupt("truncated frame header"));
    }
    let len = u64::from_le_bytes(body[..8].try_into().unwrap());
    if len > SNAPSHOT_MAX_BYTES {
        return Err(corrupt("frame length exceeds maximum"));
    }
    if body.len() as u64 != SNAPSHOT_FRAME_HEADER_LEN as u64 + len {
        return Err(corrupt("frame length does not match file size"));
    }
    let payload = &body[SNAPSHOT_FRAME_HEADER_LEN..];
    if blake3::hash(payload).as_bytes() != &body[8..SNAPSHOT_FRAME_HEADER_LEN] {
        return Err(corrupt("checksum mismatch"));
    }
    let image = serde_json::from_slice(payload)
        .map_err(|e| corrupt(&format!("cannot decode mail snapshot: {e}")))?;
    Ok(Some(LoadedSnapshot {
        image,
        generation: header.generation,
    }))
}

struct LoadedJournal {
    records: Vec<MailJournalRecord>,
    valid_len: u64,
    truncated_tail: bool,
    reset: bool,
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
    parse_journal(path, &bytes, snapshot_generation)
}

fn parse_journal(path: &Path, bytes: &[u8], snapshot_generation: u64) -> GridResult<LoadedJournal> {
    let mut out = LoadedJournal {
        records: Vec::new(),
        valid_len: HEADER_LEN as u64,
        truncated_tail: false,
        reset: false,
    };
    if bytes.len() < HEADER_LEN {
        let n = bytes.len().min(8);
        let zeros = bytes.iter().all(|b| *b == 0);
        if !zeros && bytes[..n] != MAIL_JOURNAL_MAGIC[..n] {
            return Err(GridError::Storage(format!(
                "{} is not a mail store file (bad magic); refusing to open",
                path.display()
            )));
        }
        out.reset = true;
        return Ok(out);
    }
    if bytes[..HEADER_LEN].iter().all(|b| *b == 0) && bytes.iter().all(|b| *b == 0) {
        out.reset = true;
        return Ok(out);
    }
    let header = parse_header(path, MAIL_JOURNAL_MAGIC, bytes)?;
    if header.generation < snapshot_generation {
        out.reset = true;
        return Ok(out);
    }
    if header.generation > snapshot_generation {
        return Err(GridError::Storage(format!(
            "{} has generation {} but snapshot has generation {snapshot_generation}; refusing to open",
            path.display(),
            header.generation
        )));
    }
    let total = bytes.len();
    let mut off = HEADER_LEN;
    while off < total {
        if off + FRAME_HEADER_LEN > total {
            warn!(
                "mail store: truncating incomplete tail frame in {}",
                path.display()
            );
            out.truncated_tail = true;
            break;
        }
        let len = u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap());
        let chk = u32::from_le_bytes(bytes[off + 4..off + 8].try_into().unwrap());
        if chk != len_check(len) || len as usize > MAX_FRAME_SIZE {
            if scan_valid_frame_follows(bytes, off + 1) {
                return Err(GridError::Storage(format!(
                    "{} is corrupt at byte {off}; refusing to open",
                    path.display()
                )));
            }
            out.truncated_tail = true;
            break;
        }
        let end = off + FRAME_HEADER_LEN + len as usize;
        if end > total {
            out.truncated_tail = true;
            break;
        }
        let payload = &bytes[off + FRAME_HEADER_LEN..end];
        if blake3::hash(payload).as_bytes() != &bytes[off + 8..off + FRAME_HEADER_LEN] {
            if scan_valid_frame_follows(bytes, end) {
                return Err(GridError::Storage(format!(
                    "{} checksum mismatch at byte {off}; refusing to open",
                    path.display()
                )));
            }
            out.truncated_tail = true;
            break;
        }
        let record: MailJournalRecord = bincode::deserialize(payload).map_err(|e| {
            GridError::Storage(format!(
                "{} cannot decode mail journal record at byte {off}: {e}",
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

    fn msg(id: u64, body: &str) -> MailMessage {
        MailMessage {
            id,
            from: "agent-mac".into(),
            to: vec!["agent-win-2".into()],
            topic: "unit".into(),
            body: body.into(),
            attachments: vec![],
            created_at_utc: 1_700_000_000,
        }
    }

    #[test]
    fn journal_and_snapshot_round_trip_and_truncate_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (store, rec) = MailStore::open(dir.path()).unwrap();
            assert!(rec.snapshot.is_none());
            assert!(rec.records.is_empty());
            store
                .append_durable(&MailJournalRecord::Send {
                    message: msg(1, "first"),
                })
                .unwrap()
                .wait()
                .unwrap();
            store
                .append_durable(&MailJournalRecord::Ack {
                    consumer: "agent-win-2".into(),
                    up_to_id: 1,
                })
                .unwrap()
                .wait()
                .unwrap();
        }
        // Corrupt trailing bytes to simulate a crash mid-write.
        let jpath = dir.path().join(MAIL_JOURNAL_FILE);
        let mut raw = std::fs::read(&jpath).unwrap();
        raw.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        std::fs::write(&jpath, raw).unwrap();

        let (store, rec) = MailStore::open(dir.path()).unwrap();
        assert!(rec.truncated_tail);
        assert_eq!(rec.records.len(), 2);

        let image = MailSnapshotImage {
            next_id: 2,
            messages: vec![msg(1, "first")],
            cursors: [("agent-win-2".into(), 1)].into_iter().collect(),
        };
        store.compact(image.clone()).unwrap().wait().unwrap();
        store
            .append_durable(&MailJournalRecord::Send {
                message: msg(2, "second"),
            })
            .unwrap()
            .wait()
            .unwrap();
        drop(store);

        let (_store2, rec2) = MailStore::open(dir.path()).unwrap();
        assert_eq!(rec2.snapshot, Some(image));
        assert_eq!(rec2.records.len(), 1);
    }
}
