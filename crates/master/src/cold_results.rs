//! Cold out-of-band storage for aged terminal task results (`design 0050` D2.3).
//!
//! When `--state-dir` (`TaskStore`) is configured, the queue keeps full [`TaskResult`] payloads
//! (`stdout`, `stderr`, `asset_bytes`, `artifacts`) in RAM only for the newest terminal tasks
//! within [`DEFAULT_MAX_HOT_TERMINAL_TASKS`] / [`DEFAULT_MAX_HOT_TERMINAL_BYTES`]. Older terminal
//! results spill to `<state_dir>/cold_results/<xx>/<task_id>.bin` while retaining only a compact
//! [`ColdResultSummary`] in [`crate::queue::TaskEntry`].
//!
//! # File format (`OXSWCLD1`)
//!
//! `[magic 8B: "OXSWCLD1"][version u32 LE: 1][payload_len u32 LE][blake3 32B][bincode(TaskResult)]`
//!
//! Reads validate the magic, version, length bound, and BLAKE3 checksum before decoding. Writes
//! stage into a sibling temp file, fsync, and atomically rename into place inside a private
//! (`0700` / protected DACL) directory created via [`crate::store::prepare_dir`].

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use rusty_grid_core::blob::BlobId;
use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::task::{TaskId, TaskResult};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::store::{prepare_dir, storage_io, sync_dir, sync_file_all};

/// Default count of newest terminal tasks kept hot in RAM when a state store is attached.
pub const DEFAULT_MAX_HOT_TERMINAL_TASKS: usize = 1024;
/// Default byte budget for hot terminal results kept in RAM when a state store is attached (4 MiB).
pub const DEFAULT_MAX_HOT_TERMINAL_BYTES: usize = 4 * 1024 * 1024;
/// Subdirectory under `<state_dir>` holding spilled terminal results.
pub const COLD_RESULTS_DIR: &str = "cold_results";

const MAGIC: &[u8; 8] = b"OXSWCLD1";
const FORMAT_VERSION: u32 = 1;
const HEADER_LEN: usize = 8 + 4 + 4 + 32;
/// Defensive ceiling matching the largest TaskResult frame the wire layer can deliver (256 MiB).
const MAX_COLD_PAYLOAD_BYTES: usize = 256 * 1024 * 1024;
/// Maximum concurrent worker threads used by [`ColdResultStore::put_batch`] for staging/fsync.
const PUT_BATCH_PARALLEL_THRESHOLD: usize = 4;
const PUT_BATCH_MAX_WORKERS: usize = 8;

/// Compact in-memory summary retained for a terminal task whose full [`TaskResult`] was spilled to
/// `<state_dir>/cold_results/`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColdResultSummary {
    pub execution_time_ms: u64,
    pub exit_code: i32,
    pub result_bytes: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub output_blob_ids: Vec<BlobId>,
}

impl ColdResultSummary {
    /// Builds a summary for `result` with precomputed retained byte count `result_bytes`.
    pub fn from_result(result: &TaskResult, result_bytes: usize) -> Self {
        Self {
            execution_time_ms: result.execution_time_ms,
            exit_code: result.exit_code,
            result_bytes,
            error: result.error.clone(),
            output_blob_ids: result.outputs.iter().map(|b| b.id).collect(),
        }
    }
}

/// Computes the byte size charged to a [`TaskResult`] alone (matching `queue::retained_bytes`).
pub(crate) fn task_result_bytes(r: &TaskResult) -> usize {
    r.stdout.0.len()
        + r.stderr.0.len()
        + r.error.as_ref().map_or(0, String::len)
        + r.device_name.as_ref().map_or(0, String::len)
        + r.asset_bytes.as_ref().map_or(0, |b| b.0.len())
        + r.artifacts
            .iter()
            .map(|a| a.path.len() + a.data.len())
            .sum::<usize>()
        + r.outputs.iter().map(|o| o.path.len() + 48).sum::<usize>()
}

/// Sharded file store for cold terminal [`TaskResult`] records under `<state_dir>/cold_results`.
#[derive(Debug)]
pub(crate) struct ColdResultStore {
    root: PathBuf,
    temp_seq: AtomicU64,
    prepared_shards: [AtomicBool; 256],
    shard_locks: [Mutex<()>; 256],
}

impl ColdResultStore {
    /// Opens or creates `<state_dir>/cold_results`, enforcing private ownership and directory
    /// permissions, and cleans up any leftover `.tmp` staging files from interrupted spills.
    pub(crate) fn open(state_dir: &Path) -> GridResult<Self> {
        let root = state_dir.join(COLD_RESULTS_DIR);
        prepare_dir(&root)?;
        cleanup_leftover_temps(&root);
        Ok(Self {
            root,
            temp_seq: AtomicU64::new(0),
            prepared_shards: std::array::from_fn(|_| AtomicBool::new(false)),
            shard_locks: std::array::from_fn(|_| Mutex::new(())),
        })
    }

    pub(crate) fn path_for(&self, task_id: &TaskId) -> PathBuf {
        let prefix = format!("{:02x}", task_id.0.as_bytes()[0]);
        self.root.join(prefix).join(format!("{}.bin", task_id.0))
    }

    fn ensure_shard_prepared(&self, shard_idx: usize, shard_dir: &Path) -> GridResult<()> {
        if self.prepared_shards[shard_idx].load(Ordering::Acquire) {
            return Ok(());
        }
        let _guard = self.shard_locks[shard_idx]
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if !self.prepared_shards[shard_idx].load(Ordering::Acquire) {
            prepare_dir(shard_dir)?;
            self.prepared_shards[shard_idx].store(true, Ordering::Release);
        }
        Ok(())
    }

    fn write_staged_no_dir_sync(
        &self,
        task_id: &TaskId,
        shard_idx: usize,
        shard_dir: &Path,
        final_path: &Path,
        header: &[u8; HEADER_LEN],
        payload: &[u8],
    ) -> GridResult<()> {
        self.ensure_shard_prepared(shard_idx, shard_dir)?;
        let seq = self.temp_seq.fetch_add(1, Ordering::Relaxed);
        let tmp_path = shard_dir.join(format!("{}.tmp.{seq}", task_id.0));
        let write_res = (|| -> GridResult<()> {
            let mut file = match create_private_file_io(&tmp_path) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    self.prepared_shards[shard_idx].store(false, Ordering::Release);
                    self.ensure_shard_prepared(shard_idx, shard_dir)?;
                    create_private_file_io(&tmp_path)
                        .map_err(|err| storage_io(&tmp_path, "create", err))?
                }
                Err(e) => return Err(storage_io(&tmp_path, "create", e)),
            };
            file.write_all(header)
                .and_then(|()| file.write_all(payload))
                .and_then(|()| sync_file_all(&file, shard_dir, "cold_result"))
                .map_err(|e| storage_io(&tmp_path, "write", e))?;
            drop(file);
            std::fs::rename(&tmp_path, final_path)
                .map_err(|e| storage_io(final_path, "rename", e))?;
            Ok(())
        })();
        if write_res.is_err() {
            let _ = std::fs::remove_file(&tmp_path);
        }
        write_res
    }

    /// Writes `result` atomically to disk and verifies no symlink/reparse point is crossed.
    #[cfg(test)]
    pub(crate) fn put(&self, task_id: &TaskId, result: &TaskResult) -> GridResult<()> {
        let (header, payload) = encode_cold_frame(task_id, result)?;
        let shard_idx = usize::from(task_id.0.as_bytes()[0]);
        let final_path = self.path_for(task_id);
        let shard_dir = final_path
            .parent()
            .ok_or_else(|| GridError::Storage("cold result path missing shard dir".into()))?;
        self.ensure_shard_prepared(shard_idx, shard_dir)?;
        if existing_matches(&final_path, &header, &payload) {
            return Ok(());
        }
        self.write_staged_no_dir_sync(
            task_id,
            shard_idx,
            shard_dir,
            &final_path,
            &header,
            &payload,
        )?;
        sync_dir(shard_dir, "cold_result_dir")?;
        Ok(())
    }

    /// Writes (or reuses identical existing files for) a batch of cold results, returning all
    /// `(task_id, summary)` entries durably present on disk.
    pub(crate) fn put_batch(
        &self,
        batch: Vec<(TaskId, TaskResult, ColdResultSummary)>,
    ) -> Vec<(TaskId, ColdResultSummary)> {
        if batch.is_empty() {
            return Vec::new();
        }

        struct BatchWriteResult {
            tid: TaskId,
            summary: ColdResultSummary,
            touched_shard: Option<(usize, PathBuf)>,
        }

        let process_one = |tid: TaskId,
                           res: TaskResult,
                           summary: ColdResultSummary|
         -> Option<BatchWriteResult> {
            let (header, payload) = match encode_cold_frame(&tid, &res) {
                Ok(frame) => frame,
                Err(err) => {
                    tracing::warn!(
                        task_id = %tid.0,
                        error = %err,
                        "Failed to encode cold terminal result"
                    );
                    return None;
                }
            };
            let shard_idx = usize::from(tid.0.as_bytes()[0]);
            let final_path = self.path_for(&tid);
            let Some(shard_dir) = final_path.parent().map(Path::to_path_buf) else {
                tracing::warn!(task_id = %tid.0, "Cold result path missing shard dir");
                return None;
            };
            if let Err(err) = self.ensure_shard_prepared(shard_idx, &shard_dir) {
                tracing::warn!(
                    task_id = %tid.0,
                    error = %err,
                    "Failed to prepare cold result shard directory"
                );
                return None;
            }
            if existing_matches(&final_path, &header, &payload) {
                return Some(BatchWriteResult {
                    tid,
                    summary,
                    touched_shard: None,
                });
            }
            match self.write_staged_no_dir_sync(
                &tid,
                shard_idx,
                &shard_dir,
                &final_path,
                &header,
                &payload,
            ) {
                Ok(()) => Some(BatchWriteResult {
                    tid,
                    summary,
                    touched_shard: Some((shard_idx, shard_dir)),
                }),
                Err(err) => {
                    tracing::warn!(
                        task_id = %tid.0,
                        error = %err,
                        "Failed to spill cold terminal result to disk"
                    );
                    None
                }
            }
        };

        let mut results: Vec<BatchWriteResult> = Vec::with_capacity(batch.len());
        if batch.len() < PUT_BATCH_PARALLEL_THRESHOLD {
            for (tid, res, summary) in batch {
                if let Some(done) = process_one(tid, res, summary) {
                    results.push(done);
                }
            }
        } else {
            let workers = batch.len().min(PUT_BATCH_MAX_WORKERS);
            let iter = Mutex::new(batch.into_iter());
            std::thread::scope(|scope| {
                let mut handles = Vec::with_capacity(workers);
                for _ in 0..workers {
                    handles.push(scope.spawn(|| {
                        let mut local = Vec::new();
                        loop {
                            let next = {
                                let mut guard = iter.lock().unwrap_or_else(|e| e.into_inner());
                                guard.next()
                            };
                            let Some((tid, res, summary)) = next else {
                                break;
                            };
                            if let Some(done) = process_one(tid, res, summary) {
                                local.push(done);
                            }
                        }
                        local
                    }));
                }
                for h in handles {
                    if let Ok(local) = h.join() {
                        results.extend(local);
                    }
                }
            });
        }

        let mut shard_dirs: Vec<Option<PathBuf>> = vec![None; 256];
        for item in &results {
            if let Some((shard_idx, shard_dir)) = &item.touched_shard {
                if shard_dirs[*shard_idx].is_none() {
                    shard_dirs[*shard_idx] = Some(shard_dir.clone());
                }
            }
        }
        let mut shard_ok = [true; 256];
        for (shard_idx, maybe_dir) in shard_dirs.into_iter().enumerate() {
            if let Some(shard_dir) = maybe_dir {
                if let Err(err) = sync_dir(&shard_dir, "cold_result_dir") {
                    tracing::warn!(
                        shard = %format!("{shard_idx:02x}"),
                        error = %err,
                        "Failed to sync cold result shard directory"
                    );
                    shard_ok[shard_idx] = false;
                }
            }
        }

        let mut ok = Vec::with_capacity(results.len());
        for item in results {
            let synced = item
                .touched_shard
                .is_none_or(|(shard_idx, _)| shard_ok[shard_idx]);
            if synced {
                ok.push((item.tid, item.summary));
            }
        }
        ok
    }

    /// Reads and checksum-verifies the cold [`TaskResult`] for `task_id`.
    /// Returns `Ok(None)` when no cold file exists for `task_id`.
    pub(crate) fn get(&self, task_id: &TaskId) -> GridResult<Option<TaskResult>> {
        let path = self.path_for(task_id);
        let meta = match std::fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(storage_io(&path, "inspect", e)),
        };
        if meta.file_type().is_symlink() || is_reparse_point(&meta) {
            return Err(GridError::Storage(format!(
                "cold result {} is a symbolic link; refusing to read",
                path.display()
            )));
        }
        let mut file = File::open(&path).map_err(|e| storage_io(&path, "open", e))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|e| storage_io(&path, "read", e))?;
        if bytes.len() < HEADER_LEN || &bytes[..8] != MAGIC {
            return Err(GridError::Storage(format!(
                "cold result {} has bad magic header",
                path.display()
            )));
        }
        let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        if version != FORMAT_VERSION {
            return Err(GridError::Storage(format!(
                "cold result {} has unsupported version {version}",
                path.display()
            )));
        }
        let payload_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
        if payload_len > MAX_COLD_PAYLOAD_BYTES || bytes.len() != HEADER_LEN + payload_len {
            return Err(GridError::Storage(format!(
                "cold result {} has truncated or mismatched length",
                path.display()
            )));
        }
        let expected_hash = &bytes[16..48];
        let payload = &bytes[HEADER_LEN..];
        let actual_hash = blake3::hash(payload);
        if actual_hash.as_bytes() != expected_hash {
            return Err(GridError::Storage(format!(
                "cold result {} failed BLAKE3 checksum verification",
                path.display()
            )));
        }
        let decoded: TaskResult = bincode::deserialize(payload).map_err(|e| {
            GridError::Storage(format!(
                "cold result {} failed to decode bincode payload: {e}",
                path.display()
            ))
        })?;
        Ok(Some(decoded))
    }

    pub(crate) fn contains(&self, task_id: &TaskId) -> bool {
        let path = self.path_for(task_id);
        match std::fs::symlink_metadata(&path) {
            Ok(meta) => {
                meta.is_file() && !meta.file_type().is_symlink() && !is_reparse_point(&meta)
            }
            Err(_) => false,
        }
    }

    pub(crate) fn remove(&self, task_id: &TaskId) {
        let path = self.path_for(task_id);
        let _ = std::fs::remove_file(path);
    }

    #[cfg(test)]
    pub(crate) fn temp_write_count(&self) -> u64 {
        self.temp_seq.load(Ordering::Relaxed)
    }

    /// Removes any `.bin` files on disk whose `TaskId` is not in `live_ids`.
    pub(crate) fn retain_only(&self, live_ids: &HashSet<TaskId>) {
        let Ok(shards) = std::fs::read_dir(&self.root) else {
            return;
        };
        for shard_entry in shards.flatten() {
            let shard_path = shard_entry.path();
            if shard_entry
                .file_type()
                .map_or(true, |ft| ft.is_symlink() || !ft.is_dir())
            {
                continue;
            }
            let Ok(files) = std::fs::read_dir(&shard_path) else {
                continue;
            };
            for file_entry in files.flatten() {
                let path = file_entry.path();
                let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                if name.contains(".tmp.") {
                    let _ = std::fs::remove_file(&path);
                    continue;
                }
                let Some(stem) = name.strip_suffix(".bin") else {
                    continue;
                };
                match Uuid::parse_str(stem) {
                    Ok(uuid) if live_ids.contains(&TaskId(uuid)) => {}
                    Ok(_) => {
                        let _ = std::fs::remove_file(&path);
                    }
                    Err(_) => {}
                }
            }
        }
    }
}

fn cleanup_leftover_temps(root: &Path) {
    let Ok(shards) = std::fs::read_dir(root) else {
        return;
    };
    for shard_entry in shards.flatten() {
        if shard_entry
            .file_type()
            .map_or(true, |ft| ft.is_symlink() || !ft.is_dir())
        {
            continue;
        }
        let Ok(files) = std::fs::read_dir(shard_entry.path()) else {
            continue;
        };
        for file_entry in files.flatten() {
            if let Some(name) = file_entry.file_name().to_str() {
                if name.contains(".tmp.") {
                    let _ = std::fs::remove_file(file_entry.path());
                }
            }
        }
    }
}

fn encode_cold_frame(
    task_id: &TaskId,
    result: &TaskResult,
) -> GridResult<([u8; HEADER_LEN], Vec<u8>)> {
    let payload = bincode::serialize(result).map_err(|e| {
        GridError::Storage(format!(
            "failed to encode cold result for task {task_id}: {e}"
        ))
    })?;
    if payload.len() > MAX_COLD_PAYLOAD_BYTES {
        return Err(GridError::Storage(format!(
            "cold result for task {task_id} is {} bytes (limit {MAX_COLD_PAYLOAD_BYTES})",
            payload.len()
        )));
    }
    let len_u32 = u32::try_from(payload.len()).map_err(|_| {
        GridError::Storage(format!(
            "cold result for task {task_id} exceeds u32 frame limit"
        ))
    })?;
    let mut header = [0u8; HEADER_LEN];
    header[..8].copy_from_slice(MAGIC);
    header[8..12].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    header[12..16].copy_from_slice(&len_u32.to_le_bytes());
    header[16..48].copy_from_slice(blake3::hash(&payload).as_bytes());
    Ok((header, payload))
}

fn existing_matches(path: &Path, header: &[u8; HEADER_LEN], payload: &[u8]) -> bool {
    let Ok(meta) = std::fs::symlink_metadata(path) else {
        return false;
    };
    if !meta.is_file() || meta.file_type().is_symlink() || is_reparse_point(&meta) {
        return false;
    }
    let expected_len = HEADER_LEN.saturating_add(payload.len());
    if meta.len() != expected_len as u64 {
        return false;
    }
    let Ok(mut file) = File::open(path) else {
        return false;
    };
    let mut buf = Vec::with_capacity(expected_len);
    if file.read_to_end(&mut buf).is_err() || buf.len() != expected_len {
        return false;
    }
    buf[..HEADER_LEN] == *header && buf[HEADER_LEN..] == *payload
}

fn create_private_file_io(path: &Path) -> std::io::Result<File> {
    let mut opts = OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    opts.open(path)
}

#[cfg(windows)]
fn is_reparse_point(meta: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
    meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(windows))]
fn is_reparse_point(_meta: &std::fs::Metadata) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::task::{Bytes, OutputArtifact};

    #[test]
    fn put_get_remove_and_retain_only_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = ColdResultStore::open(dir.path()).unwrap();

        let id1 = TaskId::new();
        let id2 = TaskId::new();
        let mut r1 = TaskResult::success(Uuid::new_v4(), id1, b"hello cold".to_vec(), 42, false);
        r1.stderr = Bytes::from_static(b"warn");
        r1.artifacts = vec![OutputArtifact::new("out.txt", vec![1, 2, 3, 4])];
        let r2 = TaskResult::success(Uuid::new_v4(), id2, b"keep".to_vec(), 7, true);

        store.put(&id1, &r1).unwrap();
        store.put(&id2, &r2).unwrap();
        assert!(store.contains(&id1));
        assert!(store.contains(&id2));
        assert_eq!(store.get(&id1).unwrap().as_ref(), Some(&r1));
        assert_eq!(store.get(&id2).unwrap().as_ref(), Some(&r2));

        let mut live = HashSet::new();
        live.insert(id2);
        store.retain_only(&live);
        assert!(!store.contains(&id1));
        assert!(store.contains(&id2));
        assert!(store.get(&id1).unwrap().is_none());

        store.remove(&id2);
        assert!(!store.contains(&id2));
    }

    #[test]
    fn corrupt_checksum_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let store = ColdResultStore::open(dir.path()).unwrap();
        let id = TaskId::new();
        let res = TaskResult::success(Uuid::new_v4(), id, b"integrity".to_vec(), 5, false);
        store.put(&id, &res).unwrap();

        let path = store.path_for(&id);
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        let err = store.get(&id).unwrap_err();
        assert!(err.to_string().contains("BLAKE3"), "unexpected err: {err}");
    }

    #[test]
    fn put_batch_writes_and_reuses_identical_existing_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = ColdResultStore::open(dir.path()).unwrap();

        let mut items = Vec::new();
        let wid = Uuid::new_v4();
        for i in 0..12 {
            let tid = TaskId::new();
            let res =
                TaskResult::success(wid, tid, format!("batch-{i}").into_bytes(), i as u64, false);
            let summary = ColdResultSummary::from_result(&res, res.stdout.0.len());
            items.push((tid, res, summary));
        }

        let done1 = store.put_batch(items.clone());
        assert_eq!(done1.len(), 12);
        assert_eq!(store.temp_seq.load(Ordering::Relaxed), 12);
        for (tid, res, _) in &items {
            assert_eq!(store.get(tid).unwrap().as_ref(), Some(res));
        }

        let done2 = store.put_batch(items.clone());
        assert_eq!(done2.len(), 12);
        assert_eq!(
            store.temp_seq.load(Ordering::Relaxed),
            12,
            "identical existing cold files must be reused without temp writes"
        );

        let corrupted_path = store.path_for(&items[0].0);
        let mut bytes = std::fs::read(&corrupted_path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xAA;
        std::fs::write(&corrupted_path, &bytes).unwrap();

        let done3 = store.put_batch(items.clone());
        assert_eq!(done3.len(), 12);
        assert_eq!(
            store.temp_seq.load(Ordering::Relaxed),
            13,
            "only the corrupted file should be rewritten"
        );
        assert_eq!(store.get(&items[0].0).unwrap().as_ref(), Some(&items[0].1));
    }
}
