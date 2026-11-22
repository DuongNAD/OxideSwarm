//! In-band `OXCP1` process-task checkpoints (`FEAT-5`, design 0102).
//!
//! Packs and restores `$OXIDESWARM_CHECKPOINT_DIR` inside `CheckpointData.delta_state`
//! with an explicit `OXCP1\x01` header so process-task directory snapshots never collide
//! with `BuiltinTest`, `Generative3D`, or `AutoRig` JSON/binary progress payloads on
//! Protocol 10.

use std::collections::HashSet;
use std::fmt;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::warn;

#[cfg(not(unix))]
use rusty_grid_core::blob::open_regular_file_nofollow;
use rusty_grid_core::blob::{fold_blob_path, validate_blob_path, BlobId};
use rusty_grid_core::protocol::WorkerMessage;
use rusty_grid_core::task::{Bytes, CheckpointData, TaskId};

use crate::sandbox::{
    is_real_owned_dir, normalize_relative_path_str, sanitize_relative_path, set_permissions,
};
#[cfg(unix)]
use crate::sandbox::{PinnedDir, PinnedEntryKind};

/// Reserved directory name inside task sandboxes (or workspace `tmp/`) holding process checkpoint state.
pub(crate) const PROCESS_CHECKPOINT_DIR_NAME: &str = ".oxideswarm_checkpoint";

/// Environment variable injected into `Command`, `ShellScript`, and `Workspace` child processes.
pub(crate) const CHECKPOINT_ENV_VAR: &str = "OXIDESWARM_CHECKPOINT_DIR";

/// Magic prefix identifying process-task checkpoints (`OXCP1`).
const OXCP1_PREFIX: &[u8; 5] = b"OXCP1";
/// Versioned `OXCP1` v1 header magic.
const OXCP1_MAGIC: &[u8; 6] = b"OXCP1\x01";
/// Header length: `magic (6) + flags (2) + entry_count (4) + blake3_digest (32)`.
const OXCP1_HEADER_LEN: usize = 44;
/// Per-entry fixed metadata length: `kind (1) + path_len (4) + executable (1) + data_len (8)`.
const OXCP1_ENTRY_META_LEN: usize = 14;

const ENTRY_KIND_FILE: u8 = 0;
const ENTRY_KIND_EMPTY_DIR: u8 = 1;

/// Hard cap on packed `OXCP1` payload size (`12 MiB`).
///
/// Leaves ample room inside `MAX_FRAME_SIZE` (`64 MiB`) alongside `MAX_AGGREGATE_STAGED_BYTES`
/// (`32 MiB`) in `AssignTaskWithCheckpoint`, bounds `snapshot.bin` JSON `bytes_serde` 4x expansion
/// (`<= 48 MiB`), and stays well within `DEFAULT_MAX_ACTIVE_BYTES` (`256 MiB`).
pub(crate) const MAX_PROCESS_CHECKPOINT_BYTES: usize = 12 * 1024 * 1024;

/// Maximum entries (regular files + leaf empty directories) in one `OXCP1` snapshot.
pub(crate) const MAX_PROCESS_CHECKPOINT_FILES: usize = 4096;

/// Maximum relative path byte length in one `OXCP1` entry.
pub(crate) const MAX_PROCESS_CHECKPOINT_PATH_BYTES: usize = 4096;

/// Default periodic scan interval for process-task checkpoint emission.
pub(crate) const DEFAULT_CHECKPOINT_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Minimum clamped poll interval so `tokio::time::interval` never panics on zero.
pub(crate) const MIN_CHECKPOINT_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Returns `true` if `rel` (even with leading `./`, `.\`, or case variations on APFS/NTFS)
/// targets or lies under `.oxideswarm_checkpoint`.
pub(crate) fn is_reserved_checkpoint_rel(rel: &str) -> bool {
    let normalized = rel.trim().replace('\\', "/");
    let first = normalized
        .split('/')
        .find(|seg| !seg.is_empty() && *seg != ".");
    first.is_some_and(|seg| {
        #[cfg(windows)]
        let seg = seg.trim_end_matches([' ', '.']);
        seg.eq_ignore_ascii_case(PROCESS_CHECKPOINT_DIR_NAME)
    })
}

/// Canonical BLAKE3 digest of an empty checkpoint directory (`BlobId::of(&[]).0`).
pub(crate) fn empty_checkpoint_digest() -> [u8; 32] {
    BlobId::of(&[]).0
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CheckpointError {
    LimitExceeded(String),
    UnsafeEntry(String),
    CorruptPayload(String),
    Io(String),
}

impl fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LimitExceeded(msg) => write!(f, "checkpoint limit exceeded: {msg}"),
            Self::UnsafeEntry(msg) => write!(f, "unsafe checkpoint entry: {msg}"),
            Self::CorruptPayload(msg) => write!(f, "corrupt OXCP1 checkpoint: {msg}"),
            Self::Io(msg) => write!(f, "checkpoint I/O error: {msg}"),
        }
    }
}

impl std::error::Error for CheckpointError {}

/// Runtime state carried into `execute_process_supervised_with_checkpoint` for periodic and final emission.
#[derive(Debug, Clone)]
pub(crate) struct ProcessCheckpointCtx {
    pub(crate) task_id: TaskId,
    pub(crate) checkpoint_dir: PathBuf,
    pub(crate) checkpoint_tx: Option<mpsc::Sender<WorkerMessage>>,
    pub(crate) next_sequence: u64,
    pub(crate) last_emitted_hash: [u8; 32],
    pub(crate) poll_interval: Duration,
}

impl ProcessCheckpointCtx {
    pub(crate) fn prepare_and_restore(
        task_id: TaskId,
        checkpoint_dir: PathBuf,
        checkpoint: Option<&CheckpointData>,
        checkpoint_tx: Option<mpsc::Sender<WorkerMessage>>,
        poll_interval: Duration,
    ) -> Result<Self, String> {
        prepare_checkpoint_dir(&checkpoint_dir)?;
        let last_emitted_hash = restore_checkpoint_dir(&checkpoint_dir, checkpoint);
        let next_sequence = checkpoint.map_or(1, |cp| cp.sequence.saturating_add(1));
        Ok(Self {
            task_id,
            checkpoint_dir,
            checkpoint_tx,
            next_sequence,
            last_emitted_hash,
            poll_interval: poll_interval.max(MIN_CHECKPOINT_POLL_INTERVAL),
        })
    }
}

/// Ensures `checkpoint_dir` exists as a real owned `0o700` directory (not a symlink/reparse point).
pub(crate) fn prepare_checkpoint_dir(checkpoint_dir: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        match std::fs::DirBuilder::new()
            .mode(0o700)
            .create(checkpoint_dir)
        {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => {
                return Err(format!(
                    "failed to create checkpoint directory '{}': {e}",
                    checkpoint_dir.display()
                ));
            }
        }
    }
    #[cfg(not(unix))]
    {
        match std::fs::create_dir(checkpoint_dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => {
                return Err(format!(
                    "failed to create checkpoint directory '{}': {e}",
                    checkpoint_dir.display()
                ));
            }
        }
    }

    let meta = std::fs::symlink_metadata(checkpoint_dir).map_err(|e| {
        format!(
            "cannot inspect checkpoint directory '{}': {e}",
            checkpoint_dir.display()
        )
    })?;
    if !is_real_owned_dir(checkpoint_dir, &meta) {
        return Err(format!(
            "checkpoint directory '{}' is a symbolic link, non-directory, or owned by another user",
            checkpoint_dir.display()
        ));
    }
    if let Err(e) = set_permissions(checkpoint_dir, 0o700) {
        return Err(format!(
            "failed to set 0o700 permissions on checkpoint directory '{}': {e}",
            checkpoint_dir.display()
        ));
    }

    let parent = checkpoint_dir.parent().ok_or_else(|| {
        format!(
            "checkpoint directory '{}' has no parent",
            checkpoint_dir.display()
        )
    })?;
    let canon_parent = parent.canonicalize().map_err(|e| {
        format!(
            "cannot canonicalize checkpoint parent '{}': {e}",
            parent.display()
        )
    })?;
    let canon_dir = checkpoint_dir.canonicalize().map_err(|e| {
        format!(
            "cannot canonicalize checkpoint directory '{}': {e}",
            checkpoint_dir.display()
        )
    })?;
    let file_name = checkpoint_dir
        .file_name()
        .ok_or_else(|| "checkpoint directory has no file name".to_string())?;
    if canon_dir != canon_parent.join(file_name) {
        return Err(format!(
            "checkpoint directory '{}' resolved unexpectedly outside its parent",
            checkpoint_dir.display()
        ));
    }
    Ok(())
}

fn validate_checkpoint_rel_path(
    rel: &str,
    canon_root: &Path,
    seen_folded: &mut HashSet<String>,
) -> Result<(), CheckpointError> {
    if rel.len() > MAX_PROCESS_CHECKPOINT_PATH_BYTES {
        return Err(CheckpointError::LimitExceeded(format!(
            "relative path exceeds {MAX_PROCESS_CHECKPOINT_PATH_BYTES} bytes"
        )));
    }
    validate_blob_path(rel).map_err(CheckpointError::UnsafeEntry)?;
    let normalized = normalize_relative_path_str(rel)
        .map_err(|e| CheckpointError::UnsafeEntry(e.to_string()))?;
    if normalized != rel {
        return Err(CheckpointError::UnsafeEntry(format!(
            "non-canonical checkpoint relative path '{rel}'"
        )));
    }
    sanitize_relative_path(canon_root, Path::new(rel))
        .map_err(|e| CheckpointError::UnsafeEntry(e.to_string()))?;
    let folded = fold_blob_path(rel);
    if !seen_folded.insert(folded) {
        return Err(CheckpointError::UnsafeEntry(format!(
            "duplicate or case-colliding checkpoint path '{rel}'"
        )));
    }
    Ok(())
}

struct PackedEntry {
    kind: u8,
    rel_path: String,
    executable: bool,
    data: Vec<u8>,
}

/// Packs `checkpoint_dir` into a deterministic `OXCP1` payload if its digest differs from `last_hash`.
pub(crate) fn pack_checkpoint_if_changed(
    checkpoint_dir: &Path,
    last_hash: [u8; 32],
) -> Result<Option<([u8; 32], Bytes)>, CheckpointError> {
    let (digest, payload) = pack_checkpoint_dir(checkpoint_dir)?;
    if digest == last_hash {
        return Ok(None);
    }
    Ok(Some((digest, Bytes::from(payload))))
}

fn push_bounded_file_entry(
    rel: String,
    file: std::fs::File,
    meta: std::fs::Metadata,
    entries: &mut Vec<PackedEntry>,
    encoded_body_bytes: &mut usize,
) -> Result<(), CheckpointError> {
    if !meta.is_file() {
        return Err(CheckpointError::UnsafeEntry(format!(
            "entry '{rel}' is not a regular file"
        )));
    }

    let meta_len = meta.len();
    let entry_fixed = OXCP1_ENTRY_META_LEN.saturating_add(rel.len());
    let max_body = MAX_PROCESS_CHECKPOINT_BYTES.saturating_sub(OXCP1_HEADER_LEN);
    let remaining_for_data = max_body
        .checked_sub(encoded_body_bytes.saturating_add(entry_fixed))
        .ok_or_else(|| {
            CheckpointError::LimitExceeded(format!(
                "payload exceeds {MAX_PROCESS_CHECKPOINT_BYTES} bytes"
            ))
        })?;
    if meta_len > remaining_for_data as u64 {
        return Err(CheckpointError::LimitExceeded(format!(
            "file '{rel}' ({meta_len} bytes) exceeds remaining checkpoint budget ({remaining_for_data} bytes)"
        )));
    }

    let mut buf = Vec::with_capacity(meta_len as usize);
    file.take(remaining_for_data as u64 + 1)
        .read_to_end(&mut buf)
        .map_err(|e| CheckpointError::Io(format!("cannot read '{rel}': {e}")))?;
    if buf.len() > remaining_for_data {
        return Err(CheckpointError::LimitExceeded(format!(
            "file '{rel}' grew beyond {MAX_PROCESS_CHECKPOINT_BYTES}-byte limit while packing"
        )));
    }

    *encoded_body_bytes = encoded_body_bytes
        .checked_add(entry_fixed)
        .and_then(|n| n.checked_add(buf.len()))
        .ok_or_else(|| {
            CheckpointError::LimitExceeded("checkpoint byte size overflow".to_string())
        })?;

    #[cfg(unix)]
    let executable = {
        use std::os::unix::fs::PermissionsExt;
        (meta.permissions().mode() & 0o111) != 0
    };
    #[cfg(not(unix))]
    let executable = false;

    entries.push(PackedEntry {
        kind: ENTRY_KIND_FILE,
        rel_path: rel,
        executable,
        data: buf,
    });
    Ok(())
}

fn push_empty_dir_entry(
    rel_dir: String,
    canon_root: &Path,
    seen_folded: &mut HashSet<String>,
    entries: &mut Vec<PackedEntry>,
    encoded_body_bytes: &mut usize,
) -> Result<(), CheckpointError> {
    if entries.len() >= MAX_PROCESS_CHECKPOINT_FILES {
        return Err(CheckpointError::LimitExceeded(format!(
            "exceeds maximum of {MAX_PROCESS_CHECKPOINT_FILES} files"
        )));
    }
    validate_checkpoint_rel_path(&rel_dir, canon_root, seen_folded)?;
    let entry_bytes = OXCP1_ENTRY_META_LEN.saturating_add(rel_dir.len());
    let max_body = MAX_PROCESS_CHECKPOINT_BYTES.saturating_sub(OXCP1_HEADER_LEN);
    if encoded_body_bytes.saturating_add(entry_bytes) > max_body {
        return Err(CheckpointError::LimitExceeded(format!(
            "payload exceeds {MAX_PROCESS_CHECKPOINT_BYTES} bytes"
        )));
    }
    *encoded_body_bytes += entry_bytes;
    entries.push(PackedEntry {
        kind: ENTRY_KIND_EMPTY_DIR,
        rel_path: rel_dir,
        executable: false,
        data: Vec::new(),
    });
    Ok(())
}

#[cfg(unix)]
fn collect_entries_pinned_unix(
    checkpoint_dir: &Path,
    root_meta: &std::fs::Metadata,
    canon_root: &Path,
    entries: &mut Vec<PackedEntry>,
    seen_folded: &mut HashSet<String>,
    encoded_body_bytes: &mut usize,
) -> Result<(), CheckpointError> {
    use std::os::unix::fs::MetadataExt;

    let root_dir = PinnedDir::open_root_nofollow(checkpoint_dir).map_err(|e| {
        if e.kind() == std::io::ErrorKind::InvalidInput {
            CheckpointError::UnsafeEntry(format!(
                "checkpoint root '{}' is a symlink or untrusted: {e}",
                checkpoint_dir.display()
            ))
        } else {
            CheckpointError::Io(format!(
                "cannot open checkpoint directory '{}': {e}",
                checkpoint_dir.display()
            ))
        }
    })?;
    let (dev, ino) = root_dir.dev_ino().map_err(|e| {
        CheckpointError::Io(format!(
            "cannot inspect checkpoint directory '{}': {e}",
            checkpoint_dir.display()
        ))
    })?;
    if dev != root_meta.dev() || ino != root_meta.ino() {
        return Err(CheckpointError::UnsafeEntry(
            "checkpoint root changed concurrently during open".to_string(),
        ));
    }

    let mut stack: Vec<(PinnedDir, String)> = vec![(root_dir, String::new())];
    while let Some((dir_fd, rel_dir)) = stack.pop() {
        let dir_entries = dir_fd.read_entries().map_err(|e| {
            CheckpointError::Io(format!(
                "cannot read checkpoint directory '{}': {e}",
                if rel_dir.is_empty() { "." } else { &rel_dir }
            ))
        })?;
        let had_children = !dir_entries.is_empty();
        for entry in dir_entries {
            let name = entry.name.to_str().ok_or_else(|| {
                CheckpointError::UnsafeEntry("checkpoint entry name is not valid UTF-8".to_string())
            })?;
            let rel = if rel_dir.is_empty() {
                name.to_string()
            } else {
                format!("{rel_dir}/{name}")
            };

            match entry.kind {
                PinnedEntryKind::Symlink => {
                    return Err(CheckpointError::UnsafeEntry(format!(
                        "symbolic link '{rel}' is prohibited inside checkpoint directory"
                    )));
                }
                PinnedEntryKind::Dir => {
                    let sub_fd = dir_fd.open_dir_nofollow(&entry.name).map_err(|e| {
                        if e.kind() == std::io::ErrorKind::InvalidInput {
                            CheckpointError::UnsafeEntry(format!(
                                "directory '{rel}' is a symlink or untrusted: {e}"
                            ))
                        } else {
                            CheckpointError::Io(format!("cannot open directory '{rel}': {e}"))
                        }
                    })?;
                    stack.push((sub_fd, rel));
                }
                PinnedEntryKind::File => {
                    if entries.len() >= MAX_PROCESS_CHECKPOINT_FILES {
                        return Err(CheckpointError::LimitExceeded(format!(
                            "exceeds maximum of {MAX_PROCESS_CHECKPOINT_FILES} files"
                        )));
                    }
                    validate_checkpoint_rel_path(&rel, canon_root, seen_folded)?;

                    let (file, meta) = dir_fd.open_file_nofollow(&entry.name).map_err(|e| {
                        if e.kind() == std::io::ErrorKind::InvalidInput {
                            CheckpointError::UnsafeEntry(format!(
                                "entry '{rel}' is a symlink or non-regular file: {e}"
                            ))
                        } else {
                            CheckpointError::Io(format!("cannot open '{rel}': {e}"))
                        }
                    })?;
                    push_bounded_file_entry(rel, file, meta, entries, encoded_body_bytes)?;
                }
                PinnedEntryKind::Other => {
                    return Err(CheckpointError::UnsafeEntry(format!(
                        "special file '{rel}' (FIFO, socket, or device) is prohibited inside checkpoint directory"
                    )));
                }
            }
        }

        if !had_children && !rel_dir.is_empty() {
            push_empty_dir_entry(
                rel_dir,
                canon_root,
                seen_folded,
                entries,
                encoded_body_bytes,
            )?;
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn collect_entries_non_unix(
    canon_root: &Path,
    entries: &mut Vec<PackedEntry>,
    seen_folded: &mut HashSet<String>,
    encoded_body_bytes: &mut usize,
) -> Result<(), CheckpointError> {
    let mut stack: Vec<(PathBuf, String)> = vec![(canon_root.to_path_buf(), String::new())];
    while let Some((dir_path, rel_dir)) = stack.pop() {
        let dir_meta = std::fs::symlink_metadata(&dir_path).map_err(|e| {
            CheckpointError::Io(format!(
                "cannot stat checkpoint subdirectory '{}': {e}",
                dir_path.display()
            ))
        })?;
        if !is_real_owned_dir(&dir_path, &dir_meta) {
            return Err(CheckpointError::UnsafeEntry(format!(
                "checkpoint subdirectory '{}' is a symlink or untrusted",
                dir_path.display()
            )));
        }
        let canon_dir = dir_path.canonicalize().map_err(|e| {
            CheckpointError::Io(format!(
                "cannot canonicalize checkpoint subdirectory '{}': {e}",
                dir_path.display()
            ))
        })?;
        if !canon_dir.starts_with(canon_root) {
            return Err(CheckpointError::UnsafeEntry(format!(
                "checkpoint subdirectory '{}' escaped root",
                dir_path.display()
            )));
        }

        let read_dir = std::fs::read_dir(&dir_path).map_err(|e| {
            CheckpointError::Io(format!(
                "cannot read checkpoint directory '{}': {e}",
                dir_path.display()
            ))
        })?;
        let mut had_children = false;
        for item in read_dir {
            let entry = item.map_err(|e| {
                CheckpointError::Io(format!(
                    "cannot iterate checkpoint directory '{}': {e}",
                    dir_path.display()
                ))
            })?;
            had_children = true;

            let file_name = entry.file_name();
            let name = file_name.to_str().ok_or_else(|| {
                CheckpointError::UnsafeEntry("checkpoint entry name is not valid UTF-8".to_string())
            })?;
            let rel = if rel_dir.is_empty() {
                name.to_string()
            } else {
                format!("{rel_dir}/{name}")
            };

            let file_type = entry
                .file_type()
                .map_err(|e| CheckpointError::Io(format!("cannot inspect entry '{rel}': {e}")))?;
            if file_type.is_symlink() {
                return Err(CheckpointError::UnsafeEntry(format!(
                    "symbolic link '{rel}' is prohibited inside checkpoint directory"
                )));
            }

            if file_type.is_dir() {
                let entry_path = entry.path();
                let sub_meta = std::fs::symlink_metadata(&entry_path).map_err(|e| {
                    CheckpointError::Io(format!("cannot stat directory '{rel}': {e}"))
                })?;
                if !is_real_owned_dir(&entry_path, &sub_meta) {
                    return Err(CheckpointError::UnsafeEntry(format!(
                        "directory '{rel}' is a reparse point or untrusted"
                    )));
                }
                stack.push((entry_path, rel));
            } else if file_type.is_file() {
                if entries.len() >= MAX_PROCESS_CHECKPOINT_FILES {
                    return Err(CheckpointError::LimitExceeded(format!(
                        "exceeds maximum of {MAX_PROCESS_CHECKPOINT_FILES} files"
                    )));
                }
                validate_checkpoint_rel_path(&rel, canon_root, seen_folded)?;

                let entry_path = entry.path();
                let (file, meta) = open_regular_file_nofollow(&entry_path).map_err(|e| {
                    if e.kind() == std::io::ErrorKind::InvalidInput {
                        CheckpointError::UnsafeEntry(format!(
                            "entry '{rel}' is a symlink or non-regular file: {e}"
                        ))
                    } else {
                        CheckpointError::Io(format!("cannot open '{rel}': {e}"))
                    }
                })?;
                push_bounded_file_entry(rel, file, meta, entries, encoded_body_bytes)?;
            } else {
                return Err(CheckpointError::UnsafeEntry(format!(
                    "special file '{rel}' (FIFO, socket, or device) is prohibited inside checkpoint directory"
                )));
            }
        }

        if !had_children && !rel_dir.is_empty() {
            push_empty_dir_entry(
                rel_dir,
                canon_root,
                seen_folded,
                entries,
                encoded_body_bytes,
            )?;
        }
    }
    Ok(())
}

/// Packs all safe regular files and leaf empty directories under `checkpoint_dir` into `OXCP1\x01`.
pub(crate) fn pack_checkpoint_dir(
    checkpoint_dir: &Path,
) -> Result<([u8; 32], Vec<u8>), CheckpointError> {
    let root_meta = std::fs::symlink_metadata(checkpoint_dir).map_err(|e| {
        CheckpointError::Io(format!(
            "cannot stat checkpoint directory '{}': {e}",
            checkpoint_dir.display()
        ))
    })?;
    if !is_real_owned_dir(checkpoint_dir, &root_meta) {
        return Err(CheckpointError::UnsafeEntry(
            "checkpoint root is a symlink, non-directory, or owned by another user".to_string(),
        ));
    }
    let canon_root = checkpoint_dir.canonicalize().map_err(|e| {
        CheckpointError::Io(format!(
            "cannot canonicalize checkpoint directory '{}': {e}",
            checkpoint_dir.display()
        ))
    })?;

    let mut entries: Vec<PackedEntry> = Vec::new();
    let mut seen_folded: HashSet<String> = HashSet::new();
    let mut encoded_body_bytes: usize = 0;

    #[cfg(unix)]
    collect_entries_pinned_unix(
        checkpoint_dir,
        &root_meta,
        &canon_root,
        &mut entries,
        &mut seen_folded,
        &mut encoded_body_bytes,
    )?;
    #[cfg(not(unix))]
    collect_entries_non_unix(
        &canon_root,
        &mut entries,
        &mut seen_folded,
        &mut encoded_body_bytes,
    )?;

    entries.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));

    let mut body = Vec::with_capacity(encoded_body_bytes);
    for entry in &entries {
        body.push(entry.kind);
        let path_bytes = entry.rel_path.as_bytes();
        body.extend_from_slice(&(path_bytes.len() as u32).to_le_bytes());
        body.extend_from_slice(path_bytes);
        body.push(u8::from(entry.executable));
        body.extend_from_slice(&(entry.data.len() as u64).to_le_bytes());
        body.extend_from_slice(&entry.data);
    }

    let total_len = OXCP1_HEADER_LEN.saturating_add(body.len());
    if total_len > MAX_PROCESS_CHECKPOINT_BYTES {
        return Err(CheckpointError::LimitExceeded(format!(
            "encoded checkpoint ({total_len} bytes) exceeds {MAX_PROCESS_CHECKPOINT_BYTES} bytes"
        )));
    }

    let digest = BlobId::of(&body).0;
    let mut payload = Vec::with_capacity(total_len);
    payload.extend_from_slice(OXCP1_MAGIC);
    payload.extend_from_slice(&0u16.to_le_bytes());
    payload.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    payload.extend_from_slice(&digest);
    payload.extend_from_slice(&body);

    Ok((digest, payload))
}

struct ParsedEntry<'a> {
    kind: u8,
    rel_path: &'a str,
    executable: bool,
    data: &'a [u8],
}

fn read_u16_le(cursor: &mut &[u8]) -> Result<u16, CheckpointError> {
    if cursor.len() < 2 {
        return Err(CheckpointError::CorruptPayload(
            "unexpected EOF reading u16".to_string(),
        ));
    }
    let (head, rest) = cursor.split_at(2);
    *cursor = rest;
    Ok(u16::from_le_bytes([head[0], head[1]]))
}

fn read_u32_le(cursor: &mut &[u8]) -> Result<u32, CheckpointError> {
    if cursor.len() < 4 {
        return Err(CheckpointError::CorruptPayload(
            "unexpected EOF reading u32".to_string(),
        ));
    }
    let (head, rest) = cursor.split_at(4);
    *cursor = rest;
    Ok(u32::from_le_bytes([head[0], head[1], head[2], head[3]]))
}

fn read_u64_le(cursor: &mut &[u8]) -> Result<u64, CheckpointError> {
    if cursor.len() < 8 {
        return Err(CheckpointError::CorruptPayload(
            "unexpected EOF reading u64".to_string(),
        ));
    }
    let (head, rest) = cursor.split_at(8);
    *cursor = rest;
    Ok(u64::from_le_bytes([
        head[0], head[1], head[2], head[3], head[4], head[5], head[6], head[7],
    ]))
}

/// Validates and unpacks an `OXCP1` payload into `checkpoint_dir`.
///
/// Returns `Ok(None)` without mutating `checkpoint_dir` when `payload` does not begin with `b"OXCP1"`.
pub(crate) fn unpack_checkpoint_payload(
    checkpoint_dir: &Path,
    payload: &[u8],
) -> Result<Option<[u8; 32]>, CheckpointError> {
    if !payload.starts_with(OXCP1_PREFIX) {
        return Ok(None);
    }
    if payload.len() > MAX_PROCESS_CHECKPOINT_BYTES {
        return Err(CheckpointError::LimitExceeded(format!(
            "OXCP1 payload ({} bytes) exceeds {MAX_PROCESS_CHECKPOINT_BYTES} bytes",
            payload.len()
        )));
    }
    if payload.len() < OXCP1_HEADER_LEN {
        return Err(CheckpointError::CorruptPayload(
            "truncated OXCP1 header".to_string(),
        ));
    }
    if &payload[..6] != OXCP1_MAGIC {
        return Err(CheckpointError::CorruptPayload(
            "unsupported OXCP1 version magic".to_string(),
        ));
    }

    let mut header_cursor = &payload[6..OXCP1_HEADER_LEN];
    let flags = read_u16_le(&mut header_cursor)?;
    if flags != 0 {
        return Err(CheckpointError::CorruptPayload(format!(
            "unsupported OXCP1 flags: {flags:#x}"
        )));
    }
    let entry_count = read_u32_le(&mut header_cursor)? as usize;
    if entry_count > MAX_PROCESS_CHECKPOINT_FILES {
        return Err(CheckpointError::LimitExceeded(format!(
            "OXCP1 entry count {entry_count} exceeds {MAX_PROCESS_CHECKPOINT_FILES}"
        )));
    }
    let mut expected_digest = [0u8; 32];
    expected_digest.copy_from_slice(header_cursor);

    let body = &payload[OXCP1_HEADER_LEN..];
    let actual_digest = BlobId::of(body).0;
    if actual_digest != expected_digest {
        return Err(CheckpointError::CorruptPayload(
            "OXCP1 BLAKE3 digest mismatch".to_string(),
        ));
    }

    let root_meta = std::fs::symlink_metadata(checkpoint_dir).map_err(|e| {
        CheckpointError::Io(format!(
            "cannot stat checkpoint directory '{}': {e}",
            checkpoint_dir.display()
        ))
    })?;
    if !is_real_owned_dir(checkpoint_dir, &root_meta) {
        return Err(CheckpointError::UnsafeEntry(
            "checkpoint destination is a symlink, non-directory, or owned by another user"
                .to_string(),
        ));
    }
    let canon_root = checkpoint_dir.canonicalize().map_err(|e| {
        CheckpointError::Io(format!(
            "cannot canonicalize checkpoint destination '{}': {e}",
            checkpoint_dir.display()
        ))
    })?;

    let mut cursor = body;
    let mut parsed: Vec<ParsedEntry<'_>> = Vec::with_capacity(entry_count.min(256));
    let mut seen_folded: HashSet<String> = HashSet::with_capacity(entry_count.min(256));
    let mut file_folded: HashSet<String> = HashSet::new();

    for _ in 0..entry_count {
        if cursor.is_empty() {
            return Err(CheckpointError::CorruptPayload(
                "truncated OXCP1 entry stream".to_string(),
            ));
        }
        let kind = cursor[0];
        cursor = &cursor[1..];
        if kind != ENTRY_KIND_FILE && kind != ENTRY_KIND_EMPTY_DIR {
            return Err(CheckpointError::CorruptPayload(format!(
                "unknown OXCP1 entry kind {kind}"
            )));
        }

        let path_len = read_u32_le(&mut cursor)? as usize;
        if path_len == 0 || path_len > MAX_PROCESS_CHECKPOINT_PATH_BYTES || path_len > cursor.len()
        {
            return Err(CheckpointError::CorruptPayload(format!(
                "invalid OXCP1 entry path length {path_len}"
            )));
        }
        let (path_bytes, rest) = cursor.split_at(path_len);
        cursor = rest;
        let rel_path = std::str::from_utf8(path_bytes).map_err(|_| {
            CheckpointError::CorruptPayload("OXCP1 entry path is not valid UTF-8".to_string())
        })?;
        validate_checkpoint_rel_path(rel_path, &canon_root, &mut seen_folded)?;

        if cursor.is_empty() {
            return Err(CheckpointError::CorruptPayload(
                "truncated OXCP1 executable flag".to_string(),
            ));
        }
        let exec_byte = cursor[0];
        cursor = &cursor[1..];
        if exec_byte > 1 || (kind == ENTRY_KIND_EMPTY_DIR && exec_byte != 0) {
            return Err(CheckpointError::CorruptPayload(format!(
                "invalid OXCP1 executable byte {exec_byte} for kind {kind}"
            )));
        }
        let executable = exec_byte == 1;

        let data_len = read_u64_le(&mut cursor)?;
        if kind == ENTRY_KIND_EMPTY_DIR && data_len != 0 {
            return Err(CheckpointError::CorruptPayload(
                "OXCP1 empty directory entry cannot carry payload bytes".to_string(),
            ));
        }
        if data_len > cursor.len() as u64 {
            return Err(CheckpointError::CorruptPayload(format!(
                "OXCP1 entry '{rel_path}' data length ({data_len}) exceeds remaining payload ({})",
                cursor.len()
            )));
        }
        let (data, rest) = cursor.split_at(data_len as usize);
        cursor = rest;

        if kind == ENTRY_KIND_FILE {
            file_folded.insert(fold_blob_path(rel_path));
        }
        parsed.push(ParsedEntry {
            kind,
            rel_path,
            executable,
            data,
        });
    }

    if !cursor.is_empty() {
        return Err(CheckpointError::CorruptPayload(format!(
            "trailing {} bytes after OXCP1 entries",
            cursor.len()
        )));
    }

    // Ensure no file path is used as a parent directory of another entry (case/NFD folded).
    for entry in &parsed {
        let mut rest = entry.rel_path;
        while let Some((parent, _)) = rest.rsplit_once('/') {
            if file_folded.contains(&fold_blob_path(parent)) {
                return Err(CheckpointError::UnsafeEntry(format!(
                    "path '{parent}' collides as both file and directory of '{}'",
                    entry.rel_path
                )));
            }
            rest = parent;
        }
    }

    clear_dir_contents(&canon_root)?;

    for entry in &parsed {
        if let Err(err) = materialize_entry(&canon_root, entry) {
            let _ = clear_dir_contents(&canon_root);
            return Err(err);
        }
    }

    Ok(Some(actual_digest))
}

fn ensure_safe_dir_chain(canon_root: &Path, rel_dir: &str) -> Result<PathBuf, CheckpointError> {
    let mut cur = canon_root.to_path_buf();
    if rel_dir.is_empty() {
        return Ok(cur);
    }
    for seg in rel_dir.split('/') {
        cur.push(seg);
        match std::fs::symlink_metadata(&cur) {
            Ok(meta) => {
                if !is_real_owned_dir(&cur, &meta) {
                    return Err(CheckpointError::UnsafeEntry(format!(
                        "existing path '{}' is a symlink or non-directory",
                        cur.display()
                    )));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::DirBuilderExt;
                    std::fs::DirBuilder::new()
                        .mode(0o700)
                        .create(&cur)
                        .map_err(|ce| {
                            CheckpointError::Io(format!(
                                "cannot create dir '{}': {ce}",
                                cur.display()
                            ))
                        })?;
                }
                #[cfg(not(unix))]
                {
                    std::fs::create_dir(&cur).map_err(|ce| {
                        CheckpointError::Io(format!("cannot create dir '{}': {ce}", cur.display()))
                    })?;
                }
            }
            Err(e) => {
                return Err(CheckpointError::Io(format!(
                    "cannot stat dir '{}': {e}",
                    cur.display()
                )));
            }
        }
        let canon_cur = cur.canonicalize().map_err(|e| {
            CheckpointError::Io(format!("cannot canonicalize '{}': {e}", cur.display()))
        })?;
        if !canon_cur.starts_with(canon_root) {
            return Err(CheckpointError::UnsafeEntry(format!(
                "directory '{}' resolved outside checkpoint root",
                cur.display()
            )));
        }
    }
    Ok(cur)
}

fn materialize_entry(canon_root: &Path, entry: &ParsedEntry<'_>) -> Result<(), CheckpointError> {
    if entry.kind == ENTRY_KIND_EMPTY_DIR {
        ensure_safe_dir_chain(canon_root, entry.rel_path)?;
        return Ok(());
    }

    let (parent_rel, file_name) = match entry.rel_path.rsplit_once('/') {
        Some((p, f)) => (p, f),
        None => ("", entry.rel_path),
    };
    let parent_path = ensure_safe_dir_chain(canon_root, parent_rel)?;
    let target = parent_path.join(file_name);

    match std::fs::symlink_metadata(&target) {
        Ok(_) => {
            return Err(CheckpointError::UnsafeEntry(format!(
                "checkpoint target '{}' already exists",
                entry.rel_path
            )));
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(CheckpointError::Io(format!(
                "cannot stat target '{}': {e}",
                entry.rel_path
            )));
        }
    }

    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.custom_flags(libc::O_NOFOLLOW);
        opts.mode(if entry.executable { 0o700 } else { 0o600 });
    }
    let mut file = opts.open(&target).map_err(|e| {
        CheckpointError::Io(format!(
            "cannot create restored file '{}': {e}",
            entry.rel_path
        ))
    })?;
    file.write_all(entry.data).map_err(|e| {
        CheckpointError::Io(format!(
            "cannot write restored file '{}': {e}",
            entry.rel_path
        ))
    })?;
    drop(file);

    let mode = if entry.executable { 0o700 } else { 0o600 };
    if let Err(e) = set_permissions(&target, mode) {
        return Err(CheckpointError::Io(format!(
            "cannot set permissions on '{}': {e}",
            entry.rel_path
        )));
    }
    Ok(())
}

fn clear_dir_contents(dir: &Path) -> Result<(), CheckpointError> {
    let entries = std::fs::read_dir(dir).map_err(|e| {
        CheckpointError::Io(format!(
            "cannot read checkpoint directory '{}': {e}",
            dir.display()
        ))
    })?;
    for item in entries {
        let entry = item.map_err(|e| {
            CheckpointError::Io(format!(
                "cannot iterate checkpoint directory '{}': {e}",
                dir.display()
            ))
        })?;
        let path = entry.path();
        let ft = entry.file_type().map_err(|e| {
            CheckpointError::Io(format!(
                "cannot inspect checkpoint entry '{}': {e}",
                path.display()
            ))
        })?;
        if ft.is_dir() {
            std::fs::remove_dir_all(&path).map_err(|e| {
                CheckpointError::Io(format!("cannot remove '{}': {e}", path.display()))
            })?;
        } else {
            std::fs::remove_file(&path).map_err(|e| {
                CheckpointError::Io(format!("cannot remove '{}': {e}", path.display()))
            })?;
        }
    }
    Ok(())
}

/// Restores prior `OXCP1` state into `checkpoint_dir` if present, ignoring non-`OXCP1` payloads
/// and recovering gracefully from corrupt/unsafe payloads without failing the task or worker.
pub(crate) fn restore_checkpoint_dir(
    checkpoint_dir: &Path,
    checkpoint: Option<&CheckpointData>,
) -> [u8; 32] {
    let Some(cp) = checkpoint else {
        return empty_checkpoint_digest();
    };
    match unpack_checkpoint_payload(checkpoint_dir, cp.delta_state.as_bytes()) {
        Ok(Some(digest)) => digest,
        Ok(None) => empty_checkpoint_digest(),
        Err(err) => {
            warn!(
                sequence = cp.sequence,
                error = %err,
                "Ignoring invalid OXCP1 process checkpoint payload; starting with empty checkpoint directory"
            );
            let _ = clear_dir_contents(checkpoint_dir);
            empty_checkpoint_digest()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_cp(bytes: impl Into<Vec<u8>>, seq: u64) -> CheckpointData {
        CheckpointData {
            sequence: seq,
            delta_state: Bytes::from(bytes.into()),
            timestamp: 1,
        }
    }

    #[test]
    fn oxcp1_roundtrip_nested_empty_dirs_executable_and_deterministic() {
        let src = tempfile::tempdir().unwrap();
        let cp_dir = src.path().join(PROCESS_CHECKPOINT_DIR_NAME);
        prepare_checkpoint_dir(&cp_dir).unwrap();

        std::fs::create_dir_all(cp_dir.join("nested/sub")).unwrap();
        std::fs::create_dir_all(cp_dir.join("empty_dir/leaf")).unwrap();
        std::fs::write(cp_dir.join("state.json"), b"{\"step\":2}").unwrap();
        std::fs::write(cp_dir.join("nested/sub/run.sh"), b"#!/bin/sh\necho hi\n").unwrap();
        set_permissions(&cp_dir.join("nested/sub/run.sh"), 0o700).unwrap();

        let (digest1, payload1) = pack_checkpoint_dir(&cp_dir).unwrap();
        let (digest2, payload2) = pack_checkpoint_dir(&cp_dir).unwrap();
        assert_eq!(digest1, digest2);
        assert_eq!(payload1, payload2);
        assert!(payload1.starts_with(OXCP1_MAGIC));

        // No-op when unchanged.
        assert!(pack_checkpoint_if_changed(&cp_dir, digest1)
            .unwrap()
            .is_none());

        // Restore into a fresh checkpoint directory.
        let dst = tempfile::tempdir().unwrap();
        let dst_cp = dst.path().join(PROCESS_CHECKPOINT_DIR_NAME);
        prepare_checkpoint_dir(&dst_cp).unwrap();

        let restored = unpack_checkpoint_payload(&dst_cp, &payload1)
            .unwrap()
            .expect("OXCP1 should unpack");
        assert_eq!(restored, digest1);
        assert_eq!(
            std::fs::read(dst_cp.join("state.json")).unwrap(),
            b"{\"step\":2}"
        );
        assert_eq!(
            std::fs::read(dst_cp.join("nested/sub/run.sh")).unwrap(),
            b"#!/bin/sh\necho hi\n"
        );
        assert!(dst_cp.join("empty_dir/leaf").is_dir());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dst_cp.join("nested/sub/run.sh"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o700);
        }

        // Repack of restored dir matches original digest byte-for-byte.
        let (digest3, payload3) = pack_checkpoint_dir(&dst_cp).unwrap();
        assert_eq!(digest3, digest1);
        assert_eq!(payload3, payload1);
    }

    #[test]
    fn non_oxcp1_payloads_are_silently_ignored_without_modifying_dir() {
        let dir = tempfile::tempdir().unwrap();
        let cp_dir = dir.path().join(PROCESS_CHECKPOINT_DIR_NAME);
        prepare_checkpoint_dir(&cp_dir).unwrap();

        for payload in [
            b"".as_slice(),
            b"{\"progress\":0.5,\"stage\":\"generative_3d\"}",
            b"{\"iteration\":4,\"state\":99}",
            &[1u8; 16],
        ] {
            assert_eq!(unpack_checkpoint_payload(&cp_dir, payload).unwrap(), None);
            let digest = restore_checkpoint_dir(&cp_dir, Some(&mk_cp(payload, 5)));
            assert_eq!(digest, empty_checkpoint_digest());
        }
        assert_eq!(std::fs::read_dir(&cp_dir).unwrap().count(), 0);
    }

    #[test]
    fn corrupt_truncated_and_bad_hash_oxcp1_fail_gracefully_and_leave_dir_clean() {
        let src = tempfile::tempdir().unwrap();
        let src_cp = src.path().join(PROCESS_CHECKPOINT_DIR_NAME);
        prepare_checkpoint_dir(&src_cp).unwrap();
        std::fs::write(src_cp.join("step.txt"), b"done").unwrap();
        let (_, valid) = pack_checkpoint_dir(&src_cp).unwrap();

        let dst = tempfile::tempdir().unwrap();
        let dst_cp = dst.path().join(PROCESS_CHECKPOINT_DIR_NAME);
        prepare_checkpoint_dir(&dst_cp).unwrap();

        // 1. Truncated header.
        assert!(matches!(
            unpack_checkpoint_payload(&dst_cp, b"OXCP1\x01short"),
            Err(CheckpointError::CorruptPayload(_))
        ));

        // 2. Unsupported version byte.
        let mut bad_ver = valid.clone();
        bad_ver[5] = 0x02;
        assert!(matches!(
            unpack_checkpoint_payload(&dst_cp, &bad_ver),
            Err(CheckpointError::CorruptPayload(_))
        ));

        // 3. Tampered payload byte -> digest mismatch.
        let mut tampered = valid.clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 0xff;
        assert!(matches!(
            unpack_checkpoint_payload(&dst_cp, &tampered),
            Err(CheckpointError::CorruptPayload(_))
        ));

        // 4. Trailing garbage -> digest mismatch / trailing bytes.
        let mut trailing = valid.clone();
        trailing.extend_from_slice(b"extra");
        assert!(unpack_checkpoint_payload(&dst_cp, &trailing).is_err());

        // `restore_checkpoint_dir` recovers cleanly without panic or leftover files.
        let digest = restore_checkpoint_dir(&dst_cp, Some(&mk_cp(tampered, 9)));
        assert_eq!(digest, empty_checkpoint_digest());
        assert_eq!(std::fs::read_dir(&dst_cp).unwrap().count(), 0);
    }

    #[test]
    fn symlink_and_traversal_rejected_on_pack_and_unpack() {
        let outside = tempfile::tempdir().unwrap();
        let secret = outside.path().join("secret.txt");
        std::fs::write(&secret, b"top-secret").unwrap();

        let dir = tempfile::tempdir().unwrap();
        let cp_dir = dir.path().join(PROCESS_CHECKPOINT_DIR_NAME);
        prepare_checkpoint_dir(&cp_dir).unwrap();

        if crate::sandbox::try_create_test_symlink_file(&secret, &cp_dir.join("leak.txt")) {
            let err = pack_checkpoint_dir(&cp_dir).unwrap_err();
            assert!(matches!(err, CheckpointError::UnsafeEntry(_)), "{err:?}");
        }

        // Craft an OXCP1 payload with traversal path `../escape.txt` and valid digest.
        for bad_path in [
            "../escape.txt",
            "/etc/passwd",
            "a/../../b",
            "C:evil",
            "CON",
            "a\\b",
        ] {
            let mut body = Vec::new();
            body.push(ENTRY_KIND_FILE);
            body.extend_from_slice(&(bad_path.len() as u32).to_le_bytes());
            body.extend_from_slice(bad_path.as_bytes());
            body.push(0);
            body.extend_from_slice(&4u64.to_le_bytes());
            body.extend_from_slice(b"evil");
            let digest = BlobId::of(&body).0;
            let mut payload = Vec::new();
            payload.extend_from_slice(OXCP1_MAGIC);
            payload.extend_from_slice(&0u16.to_le_bytes());
            payload.extend_from_slice(&1u32.to_le_bytes());
            payload.extend_from_slice(&digest);
            payload.extend_from_slice(&body);

            let dst = tempfile::tempdir().unwrap();
            let dst_cp = dst.path().join(PROCESS_CHECKPOINT_DIR_NAME);
            prepare_checkpoint_dir(&dst_cp).unwrap();
            let err = unpack_checkpoint_payload(&dst_cp, &payload).unwrap_err();
            assert!(
                matches!(err, CheckpointError::UnsafeEntry(_)),
                "path '{bad_path}' should fail UnsafeEntry, got {err:?}"
            );
            assert!(!dst.path().join("escape.txt").exists());
        }
    }

    #[cfg(unix)]
    #[test]
    fn fifo_special_file_rejected_on_pack() {
        let dir = tempfile::tempdir().unwrap();
        let cp_dir = dir.path().join(PROCESS_CHECKPOINT_DIR_NAME);
        prepare_checkpoint_dir(&cp_dir).unwrap();
        let fifo_path = cp_dir.join("pipe.fifo");
        let status = std::process::Command::new("mkfifo")
            .arg(&fifo_path)
            .status()
            .unwrap();
        assert!(status.success());

        let err = pack_checkpoint_dir(&cp_dir).unwrap_err();
        assert!(matches!(err, CheckpointError::UnsafeEntry(_)), "{err:?}");
    }

    #[test]
    fn size_and_file_count_limits_enforced() {
        let dir = tempfile::tempdir().unwrap();
        let cp_dir = dir.path().join(PROCESS_CHECKPOINT_DIR_NAME);
        prepare_checkpoint_dir(&cp_dir).unwrap();

        let big = vec![b'A'; MAX_PROCESS_CHECKPOINT_BYTES + 1];
        std::fs::write(cp_dir.join("too_big.bin"), big).unwrap();
        let err = pack_checkpoint_dir(&cp_dir).unwrap_err();
        assert!(matches!(err, CheckpointError::LimitExceeded(_)), "{err:?}");

        std::fs::remove_file(cp_dir.join("too_big.bin")).unwrap();
        for i in 0..=MAX_PROCESS_CHECKPOINT_FILES {
            std::fs::write(cp_dir.join(format!("f{i:04}")), b"").unwrap();
        }
        let err = pack_checkpoint_dir(&cp_dir).unwrap_err();
        assert!(matches!(err, CheckpointError::LimitExceeded(_)), "{err:?}");
    }

    #[test]
    fn sequence_monotonicity_across_restore() {
        let dir = tempfile::tempdir().unwrap();
        let cp_dir = dir.path().join(PROCESS_CHECKPOINT_DIR_NAME);
        let ctx0 = ProcessCheckpointCtx::prepare_and_restore(
            TaskId::new(),
            cp_dir.clone(),
            None,
            None,
            DEFAULT_CHECKPOINT_POLL_INTERVAL,
        )
        .unwrap();
        assert_eq!(ctx0.next_sequence, 1);

        std::fs::write(cp_dir.join("step1.ok"), b"1").unwrap();
        let (d1, p1) = pack_checkpoint_dir(&cp_dir).unwrap();

        let dir2 = tempfile::tempdir().unwrap();
        let cp_dir2 = dir2.path().join(PROCESS_CHECKPOINT_DIR_NAME);
        let ctx1 = ProcessCheckpointCtx::prepare_and_restore(
            TaskId::new(),
            cp_dir2,
            Some(&mk_cp(p1, 7)),
            None,
            DEFAULT_CHECKPOINT_POLL_INTERVAL,
        )
        .unwrap();
        assert_eq!(ctx1.next_sequence, 8);
        assert_eq!(ctx1.last_emitted_hash, d1);
    }

    #[test]
    fn is_reserved_checkpoint_rel_case_insensitive_and_dot_slash_normalized() {
        for blocked in [
            ".oxideswarm_checkpoint",
            ".oxideswarm_checkpoint/secret.bin",
            ".OXIDESWARM_CHECKPOINT",
            ".OXIDESWARM_CHECKPOINT/secret_cp.bin",
            ".Oxideswarm_Checkpoint/sub/nested.bin",
            "./.Oxideswarm_Checkpoint/secret_cp.bin",
            "././.OXIDESWARM_CHECKPOINT/a.txt",
            ".\\.Oxideswarm_Checkpoint\\secret_cp.bin",
            "/.OXIDESWARM_CHECKPOINT/secret.bin",
        ] {
            assert!(
                is_reserved_checkpoint_rel(blocked),
                "expected '{blocked}' to be blocked"
            );
        }

        for allowed in [
            "",
            ".",
            ".oxideswarm_checkpoint_extra",
            ".oxideswarm_checkpoint2/file.txt",
            "out/.oxideswarm_checkpoint/file.txt",
            "result.txt",
        ] {
            assert!(
                !is_reserved_checkpoint_rel(allowed),
                "expected '{allowed}' to be allowed"
            );
        }
    }

    #[test]
    fn fd_pinned_pack_rejects_symlinked_intermediate_dirs_and_handles_valid_entries() {
        let outside = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(outside.path().join("deep")).unwrap();
        std::fs::write(outside.path().join("deep/secret.txt"), b"outside-secret").unwrap();

        let dir = tempfile::tempdir().unwrap();
        let cp_dir = dir.path().join(PROCESS_CHECKPOINT_DIR_NAME);
        prepare_checkpoint_dir(&cp_dir).unwrap();
        std::fs::create_dir_all(cp_dir.join("real_sub/empty_leaf")).unwrap();
        std::fs::write(cp_dir.join("real_sub/data.bin"), b"ok-bytes").unwrap();

        // Valid tree with regular file + subdir + empty leaf dir packs cleanly.
        let (_, payload) = pack_checkpoint_dir(&cp_dir).unwrap();
        assert!(payload.starts_with(OXCP1_MAGIC));

        // Symlinked intermediate directory inside checkpoint dir is rejected.
        let link_sub = cp_dir.join("real_sub/escape_dir");
        if crate::sandbox::try_create_test_symlink_dir(outside.path(), &link_sub) {
            let err = pack_checkpoint_dir(&cp_dir).unwrap_err();
            assert!(matches!(err, CheckpointError::UnsafeEntry(_)), "{err:?}");
            let _ = std::fs::remove_file(&link_sub);
        }

        // Symlinked root checkpoint directory is also rejected.
        let link_root = dir.path().join("symlink_cp_root");
        if crate::sandbox::try_create_test_symlink_dir(outside.path(), &link_root) {
            let err = pack_checkpoint_dir(&link_root).unwrap_err();
            assert!(matches!(err, CheckpointError::UnsafeEntry(_)), "{err:?}");
        }
    }
}
