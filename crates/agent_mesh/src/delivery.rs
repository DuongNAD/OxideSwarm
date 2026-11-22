//! Consumer side of verified data delivery (MESH-2, design 0107).
//!
//! A node that receives `DataPayload` envelopes decodes and verifies every chunk, reassembles
//! multi-chunk transfers within fixed memory bounds, checks the SHA-256 of the whole payload,
//! persists it (with `--inbox-dir`, under a quota) and only then answers with
//! `DeliveryAck(data_received_verified)`. Each chunk that is verified but does not complete its
//! transfer is answered with `DeliveryAck(chunk_received)`, which the sender uses for flow control.

use crate::protocol::{
    verify_sha256, AgentMeshEnvelope, ERR_INBOX_FULL, ERR_PAYLOAD_CORRUPT, ERR_PERSIST_FAILED,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, Instant};

/// Maximum concurrent in-flight multi-chunk reassemblies tracked per client connection.
pub(crate) const MAX_PENDING_ASSEMBLIES: usize = 64;
/// Maximum number of chunks accepted for a single multi-chunk transfer.
pub(crate) const MAX_CHUNKS_PER_TRANSFER: u32 = 1024;
/// Maximum reassembled payload size accepted by a client (64 MiB).
pub(crate) const MAX_ASSEMBLED_BYTES: usize = 64 * 1024 * 1024;
/// Maximum aggregate payload bytes buffered across all in-flight reassemblies (128 MiB).
pub(crate) const MAX_TOTAL_REASSEMBLER_BYTES: usize = 128 * 1024 * 1024;
/// Time-to-live for incomplete multi-chunk transfers before eviction.
pub(crate) const CHUNK_ASSEMBLY_TTL: Duration = Duration::from_secs(60);
/// How many refused transfers a consumer remembers, so the chunks a sender still had in flight
/// when it was refused are dropped instead of starting a new reassembly.
const MAX_REMEMBERED_REFUSALS: usize = 256;

/// Default `--inbox-max-mb`: total bytes the files in an inbox directory may hold (1 GiB).
pub const DEFAULT_INBOX_MAX_BYTES: u64 = 1024 * 1024 * 1024;
/// Default `--inbox-max-files`: number of files an inbox directory may hold.
pub const DEFAULT_INBOX_MAX_FILES: usize = 10_000;

/// Limits on what a consumer writes into its `--inbox-dir`. Files that a downstream reader removes
/// free their share again, because usage is measured from the directory itself before each write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InboxQuota {
    /// Total size of the regular files in the directory, the new one included.
    pub max_bytes: u64,
    /// Number of regular files in the directory, the new one included.
    pub max_files: usize,
}

impl Default for InboxQuota {
    fn default() -> Self {
        Self {
            max_bytes: DEFAULT_INBOX_MAX_BYTES,
            max_files: DEFAULT_INBOX_MAX_FILES,
        }
    }
}

pub(crate) struct ReassembledPayload {
    pub(crate) bytes: Vec<u8>,
    pub(crate) expected_total_sha256: Option<String>,
}

struct ChunkAssembly {
    total_chunks: u32,
    expected_total_sha256: Option<String>,
    chunks: HashMap<u32, Vec<u8>>,
    total_bytes: usize,
    updated_at: Instant,
    seq_order: u64,
}

pub(crate) struct ChunkReassembler {
    inflight: HashMap<(String, String), ChunkAssembly>,
    total_inflight_bytes: usize,
    next_seq_order: u64,
    max_pending_assemblies: usize,
    max_assembled_bytes: usize,
    max_total_bytes: usize,
    ttl: Duration,
}

impl ChunkReassembler {
    pub(crate) fn new() -> Self {
        Self::with_limits(
            MAX_PENDING_ASSEMBLIES,
            MAX_ASSEMBLED_BYTES,
            MAX_TOTAL_REASSEMBLER_BYTES,
        )
    }

    fn with_limits(
        max_pending_assemblies: usize,
        max_assembled_bytes: usize,
        max_total_bytes: usize,
    ) -> Self {
        Self {
            inflight: HashMap::new(),
            total_inflight_bytes: 0,
            next_seq_order: 0,
            max_pending_assemblies,
            max_assembled_bytes,
            max_total_bytes,
            ttl: CHUNK_ASSEMBLY_TTL,
        }
    }

    #[cfg(test)]
    fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    fn remove_assembly(&mut self, key: &(String, String)) -> Option<ChunkAssembly> {
        let removed = self.inflight.remove(key)?;
        self.total_inflight_bytes = self
            .total_inflight_bytes
            .saturating_sub(removed.total_bytes);
        Some(removed)
    }

    fn evict_oldest_fifo(&mut self, exclude_key: Option<&(String, String)>) -> bool {
        let oldest_key = self
            .inflight
            .iter()
            .filter(|(k, _)| exclude_key != Some(*k))
            .min_by_key(|(_, v)| v.seq_order)
            .map(|(k, _)| k.clone());
        if let Some(k) = oldest_key {
            self.remove_assembly(&k);
            true
        } else {
            false
        }
    }

    fn prune_expired(&mut self, now: Instant) {
        let ttl = self.ttl;
        let mut removed_bytes = 0usize;
        self.inflight.retain(|_, asm| {
            let keep = now.saturating_duration_since(asm.updated_at) <= ttl;
            if !keep {
                removed_bytes = removed_bytes.saturating_add(asm.total_bytes);
            }
            keep
        });
        self.total_inflight_bytes = self.total_inflight_bytes.saturating_sub(removed_bytes);
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn insert_chunk(
        &mut self,
        from: &str,
        corr_id: &str,
        seq: u32,
        total_chunks: u32,
        expected_total_sha256: Option<&str>,
        bytes: Vec<u8>,
        now: Instant,
    ) -> Result<Option<ReassembledPayload>, String> {
        self.prune_expired(now);

        if bytes.len() > self.max_assembled_bytes || bytes.len() > self.max_total_bytes {
            let key = (from.to_string(), corr_id.to_string());
            self.remove_assembly(&key);
            return Err(format!(
                "Chunk size {} exceeds cap of {} bytes",
                bytes.len(),
                self.max_assembled_bytes.min(self.max_total_bytes)
            ));
        }

        let key = (from.to_string(), corr_id.to_string());
        if !self.inflight.contains_key(&key) {
            while self.inflight.len() >= self.max_pending_assemblies
                || self.total_inflight_bytes.saturating_add(bytes.len()) > self.max_total_bytes
            {
                if !self.evict_oldest_fifo(None) {
                    break;
                }
            }
            let seq_order = self.next_seq_order;
            self.next_seq_order = self.next_seq_order.saturating_add(1);
            self.inflight.insert(
                key.clone(),
                ChunkAssembly {
                    total_chunks,
                    expected_total_sha256: expected_total_sha256.map(str::to_string),
                    chunks: HashMap::new(),
                    total_bytes: 0,
                    updated_at: now,
                    seq_order,
                },
            );
        }

        let (expected_chunks, prev_len, new_total_bytes, seq_mismatch, sha_mismatch, set_sha) = {
            let asm = self
                .inflight
                .get(&key)
                .expect("assembly inserted immediately above");

            let seq_mismatch = asm.total_chunks != total_chunks;
            let mut sha_mismatch = false;
            let mut set_sha = false;
            if let Some(new_sha) = expected_total_sha256 {
                if let Some(ref existing_sha) = asm.expected_total_sha256 {
                    if !existing_sha.eq_ignore_ascii_case(new_sha) {
                        sha_mismatch = true;
                    }
                } else {
                    set_sha = true;
                }
            }
            let prev_len = asm.chunks.get(&seq).map_or(0, Vec::len);
            let new_total_bytes = asm
                .total_bytes
                .saturating_sub(prev_len)
                .saturating_add(bytes.len());
            (
                asm.total_chunks,
                prev_len,
                new_total_bytes,
                seq_mismatch,
                sha_mismatch,
                set_sha,
            )
        };

        if seq_mismatch {
            self.remove_assembly(&key);
            return Err(format!(
                "Inconsistent total_chunks in multi-chunk transfer (expected {expected_chunks}, got {total_chunks})"
            ));
        }

        if sha_mismatch {
            self.remove_assembly(&key);
            return Err("Inconsistent total_sha256 in multi-chunk transfer".to_string());
        }

        if new_total_bytes > self.max_assembled_bytes {
            self.remove_assembly(&key);
            return Err(format!(
                "Assembled payload size {new_total_bytes} exceeds cap of {} bytes",
                self.max_assembled_bytes
            ));
        }

        // Evict oldest *other* in-flight assemblies FIFO before exceeding aggregate cap.
        while self
            .total_inflight_bytes
            .saturating_sub(prev_len)
            .saturating_add(bytes.len())
            > self.max_total_bytes
        {
            if !self.evict_oldest_fifo(Some(&key)) {
                self.remove_assembly(&key);
                return Err(format!(
                    "Aggregate reassembler buffer cap of {} bytes exceeded",
                    self.max_total_bytes
                ));
            }
        }

        let new_bytes_len = bytes.len();
        let is_complete = {
            let asm = self
                .inflight
                .get_mut(&key)
                .expect("assembly retained while evicting others");
            if set_sha {
                asm.expected_total_sha256 = expected_total_sha256.map(str::to_string);
            }
            asm.total_bytes = new_total_bytes;
            asm.updated_at = now;
            asm.chunks.insert(seq, bytes);
            asm.chunks.len() >= total_chunks as usize
        };
        self.total_inflight_bytes = self
            .total_inflight_bytes
            .saturating_sub(prev_len)
            .saturating_add(new_bytes_len);

        if !is_complete {
            return Ok(None);
        }

        let mut completed = self
            .remove_assembly(&key)
            .expect("assembly present before completion");
        let mut assembled = Vec::with_capacity(completed.total_bytes);
        for idx in 0..total_chunks {
            let Some(part) = completed.chunks.remove(&idx) else {
                return Err(format!("Missing chunk {idx} during reassembly"));
            };
            assembled.extend_from_slice(&part);
        }
        Ok(Some(ReassembledPayload {
            bytes: assembled,
            expected_total_sha256: completed.expected_total_sha256,
        }))
    }
}

/// What a consumer does with one `DataPayload` envelope.
#[derive(Debug)]
pub(crate) enum DataVerdict {
    /// A verified chunk of a multi-chunk transfer that is not complete yet: answer
    /// `DeliveryAck(chunk_received)`.
    ChunkAccepted,
    /// The whole payload matched its SHA-256: persist it, then answer
    /// `DeliveryAck(data_received_verified)`. `envelope` is the reassembled payload.
    Complete {
        bytes: Vec<u8>,
        envelope: Box<AgentMeshEnvelope>,
    },
    /// Refused: answer `DeliveryNack(code, reason)`.
    Rejected { code: &'static str, reason: String },
    /// A late chunk of a transfer this consumer already refused, or not a `DataPayload`: no answer.
    Dropped,
}

fn corrupt(reason: impl Into<String>) -> DataVerdict {
    DataVerdict::Rejected {
        code: ERR_PAYLOAD_CORRUPT,
        reason: reason.into(),
    }
}

/// Decodes the Latin-1 text of a `DataPayload` (one character per byte). `None` when a character
/// is above U+00FF, which no sender produces from bytes.
fn decode_latin1(text: &str) -> Option<Vec<u8>> {
    let mut decoded = Vec::with_capacity(text.len());
    for c in text.chars() {
        decoded.push(u8::try_from(u32::from(c)).ok()?);
    }
    Some(decoded)
}

/// Per-connection consumer state: the chunk reassembler and the transfers it refused.
pub(crate) struct DataConsumer {
    reassembler: ChunkReassembler,
    refused_order: VecDeque<(String, String)>,
    refused: HashSet<(String, String)>,
}

impl DataConsumer {
    pub(crate) fn new() -> Self {
        Self::with_reassembler(ChunkReassembler::new())
    }

    fn with_reassembler(reassembler: ChunkReassembler) -> Self {
        Self {
            reassembler,
            refused_order: VecDeque::new(),
            refused: HashSet::new(),
        }
    }

    fn remember_refusal(&mut self, key: (String, String)) {
        if self.refused.contains(&key) {
            return;
        }
        while self.refused_order.len() >= MAX_REMEMBERED_REFUSALS {
            if let Some(old) = self.refused_order.pop_front() {
                self.refused.remove(&old);
            }
        }
        self.refused_order.push_back(key.clone());
        self.refused.insert(key);
    }

    /// Verifies one `DataPayload` (chunk checksum, chunk metadata, and for the last chunk of a
    /// transfer the SHA-256 of the whole payload) and says how to answer it.
    pub(crate) fn accept(&mut self, envelope: &AgentMeshEnvelope, now: Instant) -> DataVerdict {
        let AgentMeshEnvelope::DataPayload {
            version,
            correlation_id,
            from,
            to,
            payload_type,
            sequence_number,
            total_chunks,
            data,
            checksum_sha256,
            payload,
            metadata,
            timestamp,
            ..
        } = envelope
        else {
            return DataVerdict::Dropped;
        };
        let key = (from.clone(), correlation_id.clone());
        if self.refused.contains(&key) {
            return DataVerdict::Dropped;
        }

        let verdict = (|| {
            // 1. Strictly decode the bytes (no silent truncation of wide characters).
            let raw_bytes = match (data, payload) {
                (Some(text), _) => match decode_latin1(text) {
                    Some(bytes) => bytes,
                    None => return corrupt("Data payload contains non-Latin-1 code points"),
                },
                (None, Some(value)) => match serde_json::to_vec(value) {
                    Ok(bytes) => bytes,
                    Err(e) => return corrupt(format!("Failed to encode JSON payload: {e}")),
                },
                (None, None) => return corrupt("Missing data payload"),
            };

            // 2. Every chunk carries the SHA-256 of its own bytes.
            let Some(claimed_sha) = checksum_sha256 else {
                return corrupt("Missing SHA-256 checksum");
            };
            if !verify_sha256(&raw_bytes, claimed_sha) {
                return corrupt("SHA-256 checksum mismatch");
            }

            // 3. Chunk sequence metadata.
            let effective_total = if *total_chunks == 0 && *sequence_number == 0 {
                1
            } else {
                *total_chunks
            };
            if effective_total == 0
                || *sequence_number >= effective_total
                || effective_total > MAX_CHUNKS_PER_TRANSFER
            {
                return corrupt(format!(
                    "Invalid chunk metadata: sequence_number={sequence_number}, total_chunks={total_chunks}"
                ));
            }
            let total_sha_hint = metadata
                .as_ref()
                .and_then(|m| m.get("total_sha256"))
                .map(String::as_str);

            // 4. A single chunk is the whole payload.
            if effective_total == 1 {
                if let Some(expected_total) = total_sha_hint {
                    if !verify_sha256(&raw_bytes, expected_total) {
                        return corrupt("Payload total_sha256 mismatch");
                    }
                }
                return DataVerdict::Complete {
                    bytes: raw_bytes,
                    envelope: Box::new(envelope.clone()),
                };
            }

            // 5. A multi-chunk transfer must name the SHA-256 of the whole payload, which is the
            //    only thing that proves the chunks were put back together right.
            let Some(total_sha) = total_sha_hint else {
                return corrupt("Multi-chunk transfer carries no metadata total_sha256");
            };
            match self.reassembler.insert_chunk(
                from,
                correlation_id,
                *sequence_number,
                effective_total,
                Some(total_sha),
                raw_bytes,
                now,
            ) {
                Ok(None) => DataVerdict::ChunkAccepted,
                Ok(Some(ReassembledPayload {
                    bytes,
                    expected_total_sha256,
                })) => {
                    let expected = expected_total_sha256.unwrap_or_else(|| total_sha.to_string());
                    if !verify_sha256(&bytes, &expected) {
                        return corrupt("Assembled payload SHA-256 mismatch");
                    }
                    let text: String = bytes.iter().map(|&b| b as char).collect();
                    let assembled = AgentMeshEnvelope::DataPayload {
                        version: version.clone(),
                        correlation_id: correlation_id.clone(),
                        from: from.clone(),
                        to: to.clone(),
                        payload_type: payload_type.clone(),
                        sequence_number: 0,
                        total_chunks: effective_total,
                        chunk_size_bytes: bytes.len(),
                        data: Some(text),
                        checksum_sha256: Some(expected.to_ascii_lowercase()),
                        payload: None,
                        metadata: metadata.clone(),
                        timestamp: *timestamp,
                    };
                    DataVerdict::Complete {
                        bytes,
                        envelope: Box::new(assembled),
                    }
                }
                Err(reason) => corrupt(reason),
            }
        })();

        if matches!(verdict, DataVerdict::Rejected { .. }) {
            self.remember_refusal(key);
        }
        verdict
    }

    /// Forgets a transfer that completed but could not be persisted, so its late chunks (none are
    /// expected) are dropped like those of any other refused transfer.
    pub(crate) fn refuse(&mut self, from: &str, correlation_id: &str) {
        self.remember_refusal((from.to_string(), correlation_id.to_string()));
    }
}

/// Why a verified payload was not written to the inbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PersistError {
    pub(crate) code: &'static str,
    pub(crate) reason: String,
}

impl PersistError {
    fn failed(reason: impl Into<String>) -> Self {
        Self {
            code: ERR_PERSIST_FAILED,
            reason: reason.into(),
        }
    }
}

/// Creates `path` (and missing parents) private to the current user: mode 0700 on Unix.
fn create_private_dir_all(path: &Path) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)
}

/// Number and total size of the regular files directly in `dir` (links are not followed).
fn inbox_usage(dir: &Path) -> std::io::Result<(usize, u64)> {
    let mut files = 0usize;
    let mut bytes = 0u64;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        // `DirEntry::metadata` does not follow symbolic links.
        let Ok(meta) = entry.metadata() else {
            continue; // removed by a reader in the meantime
        };
        if meta.is_file() {
            files = files.saturating_add(1);
            bytes = bytes.saturating_add(meta.len());
        }
    }
    Ok((files, bytes))
}

/// Refuses a payload of `incoming` bytes that would take the inbox past `quota`.
fn check_inbox_quota(dir: &Path, quota: InboxQuota, incoming: u64) -> Result<(), PersistError> {
    let (files, used) = inbox_usage(dir).map_err(|e| {
        PersistError::failed(format!(
            "failed to measure inbox directory '{}': {e}",
            dir.display()
        ))
    })?;
    if files.saturating_add(1) > quota.max_files {
        return Err(PersistError {
            code: ERR_INBOX_FULL,
            reason: format!(
                "inbox quota reached: the directory already holds the maximum of {} files (--inbox-max-files)",
                quota.max_files
            ),
        });
    }
    if used.saturating_add(incoming) > quota.max_bytes {
        return Err(PersistError {
            code: ERR_INBOX_FULL,
            reason: format!(
                "inbox quota reached: a payload of {incoming} bytes does not fit under the limit of {} bytes (--inbox-max-mb)",
                quota.max_bytes
            ),
        });
    }
    Ok(())
}

/// Validates and canonicalizes an `--inbox-dir` path before accepting incoming payloads.
///
/// Rejects empty paths, symlinks, and non-directory entries (such as regular files). Creates the
/// directory if it does not yet exist (mode 0700 on Unix), then returns its canonical path. An
/// existing directory keeps its permissions.
pub fn validate_inbox_dir(
    path: &Path,
) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    if path.as_os_str().is_empty() {
        return Err("inbox directory path must not be empty".into());
    }
    // Strip trailing path separators (and trailing `/.`) while preserving root `/` so POSIX
    // `lstat` inspects the symlink itself rather than dereferencing `/path/to/symlink/`.
    let inspect_path = path.components().as_path();
    if inspect_path.as_os_str().is_empty() {
        return Err("inbox directory path must not be empty".into());
    }
    match std::fs::symlink_metadata(inspect_path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(
                    format!("inbox directory '{}' must not be a symlink", path.display()).into(),
                );
            }
            if !meta.is_dir() {
                return Err(format!(
                    "inbox directory '{}' exists and is not a directory",
                    path.display()
                )
                .into());
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            create_private_dir_all(inspect_path).map_err(|err| {
                format!(
                    "failed to create inbox directory '{}': {err}",
                    path.display()
                )
            })?;
        }
        Err(e) => {
            return Err(format!(
                "failed to inspect inbox directory '{}': {e}",
                path.display()
            )
            .into());
        }
    }

    let canonical = std::fs::canonicalize(inspect_path).map_err(|e| {
        format!(
            "failed to canonicalize inbox directory '{}': {e}",
            path.display()
        )
    })?;
    let canon_meta = std::fs::symlink_metadata(&canonical).map_err(|e| {
        format!(
            "failed to inspect canonical inbox directory '{}': {e}",
            canonical.display()
        )
    })?;
    if canon_meta.file_type().is_symlink() || !canon_meta.is_dir() {
        return Err(format!(
            "canonical inbox directory '{}' is not a valid directory",
            canonical.display()
        )
        .into());
    }
    Ok(canonical)
}

fn sanitize_filename_component(raw: &str, max_len: usize) -> String {
    let filtered: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .take(max_len)
        .collect();
    let collapsed = filtered.replace("..", "_");
    let trimmed = collapsed.trim_matches('.');
    if trimmed.is_empty() || trimmed == "." || trimmed == ".." {
        "payload".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Writes a verified payload into `inbox_dir` atomically (temporary file, `fsync`, rename) as a
/// file only the current user can read (mode 0600 on Unix), after checking `quota`.
pub(crate) fn persist_payload_atomic(
    inbox_dir: &Path,
    quota: InboxQuota,
    corr_id: &str,
    timestamp_ms: u64,
    metadata: Option<&HashMap<String, String>>,
    bytes: &[u8],
) -> Result<PathBuf, PersistError> {
    let dir_meta = std::fs::symlink_metadata(inbox_dir).map_err(|e| {
        PersistError::failed(format!(
            "inbox directory '{}' inaccessible: {e}",
            inbox_dir.display()
        ))
    })?;
    if dir_meta.file_type().is_symlink() || !dir_meta.is_dir() {
        return Err(PersistError::failed(format!(
            "inbox directory '{}' is not a real directory",
            inbox_dir.display()
        )));
    }

    check_inbox_quota(
        inbox_dir,
        quota,
        u64::try_from(bytes.len()).unwrap_or(u64::MAX),
    )?;

    let safe_corr = sanitize_filename_component(corr_id, 64);
    let suffix_hint = metadata
        .and_then(|m| m.get("filename"))
        .map(|f| sanitize_filename_component(f, 48));

    let base_name = match suffix_hint {
        Some(ref hint) => format!("{timestamp_ms}_{safe_corr}_{hint}.bin"),
        None => format!("{timestamp_ms}_{safe_corr}.bin"),
    };

    let mut final_name = base_name;
    let mut final_path = inbox_dir.join(&final_name);
    if std::fs::symlink_metadata(&final_path).is_ok() {
        let uniq = &uuid::Uuid::new_v4().to_string()[..8];
        final_name = match suffix_hint {
            Some(ref hint) => format!("{timestamp_ms}_{safe_corr}_{hint}_{uniq}.bin"),
            None => format!("{timestamp_ms}_{safe_corr}_{uniq}.bin"),
        };
        final_path = inbox_dir.join(&final_name);
    }

    let mut comps = Path::new(&final_name).components();
    if !matches!(comps.next(), Some(Component::Normal(_)))
        || comps.next().is_some()
        || final_path.parent() != Some(inbox_dir)
    {
        return Err(PersistError::failed(
            "resolved inbox filename failed path-traversal check",
        ));
    }

    if let Ok(existing_meta) = std::fs::symlink_metadata(&final_path) {
        if existing_meta.file_type().is_symlink() {
            return Err(PersistError::failed(
                "refusing to overwrite symlink in inbox directory",
            ));
        }
    }

    let tmp_name = format!(".tmp_{}.part", uuid::Uuid::new_v4());
    let tmp_path = inbox_dir.join(&tmp_name);
    if tmp_path.parent() != Some(inbox_dir) {
        return Err(PersistError::failed(
            "resolved temp filename escaped inbox directory",
        ));
    }

    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&tmp_path)
        .map_err(|e| PersistError::failed(format!("failed to create temp inbox file: {e}")))?;

    let write_res = (|| -> std::io::Result<()> {
        file.write_all(bytes)?;
        file.flush()?;
        file.sync_all()?;
        Ok(())
    })();
    drop(file);

    if let Err(e) = write_res {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(PersistError::failed(format!(
            "failed to write inbox file: {e}"
        )));
    }

    if let Err(e) = std::fs::rename(&tmp_path, &final_path) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(PersistError::failed(format!(
            "failed to finalize inbox file: {e}"
        )));
    }

    Ok(final_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{compute_sha256, ERR_INBOX_FULL};

    #[test]
    fn test_validate_inbox_dir_rejects_file_empty_and_symlink() {
        assert!(validate_inbox_dir(Path::new("")).is_err());

        let tmp = tempfile::tempdir().unwrap();
        let file_path = tmp.path().join("not-a-dir.txt");
        std::fs::write(&file_path, b"hello").unwrap();
        let file_err = validate_inbox_dir(&file_path).expect_err("regular file must be rejected");
        assert!(
            file_err.to_string().contains("not a directory"),
            "{file_err}"
        );

        #[cfg(unix)]
        {
            let real_dir = tmp.path().join("real");
            std::fs::create_dir(&real_dir).unwrap();
            let link_dir = tmp.path().join("link_inbox");
            std::os::unix::fs::symlink(&real_dir, &link_dir).unwrap();
            let link_err = validate_inbox_dir(&link_dir).expect_err("symlink must be rejected");
            assert!(link_err.to_string().contains("symlink"), "{link_err}");

            // Trailing path separator(s) on a symlink must still be rejected as a symlink.
            let trailing_link = PathBuf::from(format!("{}/", link_dir.display()));
            let trailing_err = validate_inbox_dir(&trailing_link)
                .expect_err("trailing-slash symlink must be rejected");
            assert!(
                trailing_err.to_string().contains("symlink"),
                "{trailing_err}"
            );

            let multi_trailing_link = PathBuf::from(format!("{}///", link_dir.display()));
            let multi_err = validate_inbox_dir(&multi_trailing_link)
                .expect_err("multi-trailing-slash symlink must be rejected");
            assert!(multi_err.to_string().contains("symlink"), "{multi_err}");

            // Broken symlink with trailing slash must also be rejected as a symlink.
            let broken_link = tmp.path().join("broken_link");
            std::os::unix::fs::symlink(tmp.path().join("does-not-exist"), &broken_link).unwrap();
            let broken_trailing = PathBuf::from(format!("{}/", broken_link.display()));
            let broken_err = validate_inbox_dir(&broken_trailing)
                .expect_err("broken trailing-slash symlink must be rejected");
            assert!(broken_err.to_string().contains("symlink"), "{broken_err}");
        }

        // Root `/` normalization keeps `/` intact.
        assert_eq!(Path::new("/").components().as_path(), Path::new("/"));

        let new_dir = tmp.path().join("nested").join("inbox");
        let validated = validate_inbox_dir(&new_dir).expect("new dir should be created");
        assert!(validated.is_dir());
    }

    #[test]
    fn test_persist_payload_atomic_traversal_binary_and_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let inbox = validate_inbox_dir(tmp.path().join("inbox").as_path()).unwrap();
        let quota = InboxQuota::default();

        let mut meta = HashMap::new();
        meta.insert("filename".to_string(), "../../etc/passwd".to_string());
        let binary = b"\x00\x01\x80\xffhello\xfe";
        let saved_bin =
            persist_payload_atomic(&inbox, quota, "../evil/corr", 12345, Some(&meta), binary)
                .unwrap();
        assert_eq!(saved_bin.parent(), Some(inbox.as_path()));
        assert_eq!(std::fs::read(&saved_bin).unwrap(), binary);

        let saved_empty =
            persist_payload_atomic(&inbox, quota, "empty-corr", 12346, None, b"").unwrap();
        assert_eq!(saved_empty.parent(), Some(inbox.as_path()));
        assert_eq!(std::fs::read(&saved_empty).unwrap(), b"");

        // Ensure no hidden .tmp_* files remain
        let entries: Vec<_> = std::fs::read_dir(&inbox)
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert_eq!(entries.len(), 2);
        for e in entries {
            assert!(!e.file_name().to_string_lossy().starts_with(".tmp_"));
        }
    }

    #[test]
    fn test_inbox_quota_refuses_by_file_count_and_bytes_and_frees_on_removal() {
        let tmp = tempfile::tempdir().unwrap();
        let inbox = validate_inbox_dir(&tmp.path().join("quota-inbox")).unwrap();
        let quota = InboxQuota {
            max_bytes: 100,
            max_files: 2,
        };

        let first = persist_payload_atomic(&inbox, quota, "a", 1, None, &[1u8; 40]).unwrap();
        persist_payload_atomic(&inbox, quota, "b", 2, None, &[2u8; 40]).unwrap();

        // A third file breaks the file count.
        let count_err = persist_payload_atomic(&inbox, quota, "c", 3, None, b"x").unwrap_err();
        assert_eq!(count_err.code, ERR_INBOX_FULL);
        assert!(
            count_err.reason.contains("--inbox-max-files"),
            "{count_err:?}"
        );

        // A reader that removes a file frees its share again.
        std::fs::remove_file(&first).unwrap();
        // 40 + 61 = 101 > 100 bytes.
        let bytes_err =
            persist_payload_atomic(&inbox, quota, "d", 4, None, &[3u8; 61]).unwrap_err();
        assert_eq!(bytes_err.code, ERR_INBOX_FULL);
        assert!(bytes_err.reason.contains("--inbox-max-mb"), "{bytes_err:?}");
        // 40 + 60 = 100 fits exactly.
        persist_payload_atomic(&inbox, quota, "e", 5, None, &[4u8; 60]).unwrap();

        // Refusals leave no partial file behind.
        let names: Vec<String> = std::fs::read_dir(&inbox)
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names.len(), 2, "{names:?}");
        assert!(names.iter().all(|n| !n.starts_with(".tmp_")), "{names:?}");
    }

    #[cfg(unix)]
    #[test]
    fn test_inbox_files_are_0600_and_a_created_inbox_dir_is_0700() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let inbox = validate_inbox_dir(&tmp.path().join("fresh").join("inbox")).unwrap();
        let dir_mode = std::fs::metadata(&inbox).unwrap().permissions().mode() & 0o777;
        assert_eq!(dir_mode, 0o700, "inbox dir mode {dir_mode:o}");
        let parent_mode = std::fs::metadata(tmp.path().join("fresh"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(parent_mode, 0o700, "created parent mode {parent_mode:o}");

        let saved = persist_payload_atomic(
            &inbox,
            InboxQuota::default(),
            "perm",
            7,
            None,
            b"secret-ish",
        )
        .unwrap();
        let file_mode = std::fs::metadata(&saved).unwrap().permissions().mode() & 0o777;
        assert_eq!(file_mode, 0o600, "inbox file mode {file_mode:o}");
    }

    fn chunk_bytes(env: &AgentMeshEnvelope) -> Vec<u8> {
        match env {
            AgentMeshEnvelope::DataPayload { data: Some(d), .. } => decode_latin1(d).unwrap(),
            other => panic!("expected DataPayload, got {other:?}"),
        }
    }

    #[test]
    fn test_consumer_acks_chunks_then_verifies_the_whole_payload() {
        let payload: Vec<u8> = (0..250u32).map(|i| (i % 256) as u8).collect();
        let (envs, _, total_sha) =
            AgentMeshEnvelope::new_data_payload_chunks("src", "dst", &payload, 100);
        assert_eq!(envs.len(), 3);
        let mut consumer = DataConsumer::new();
        let now = Instant::now();
        assert!(matches!(
            consumer.accept(&envs[0], now),
            DataVerdict::ChunkAccepted
        ));
        assert!(matches!(
            consumer.accept(&envs[1], now),
            DataVerdict::ChunkAccepted
        ));
        match consumer.accept(&envs[2], now) {
            DataVerdict::Complete { bytes, envelope } => {
                assert_eq!(bytes, payload);
                assert_eq!(chunk_bytes(&envelope), payload);
                if let AgentMeshEnvelope::DataPayload {
                    checksum_sha256, ..
                } = *envelope
                {
                    assert_eq!(checksum_sha256.as_deref(), Some(total_sha.as_str()));
                }
            }
            other => panic!("expected Complete, got {other:?}"),
        }
    }

    #[test]
    fn test_consumer_refuses_missing_checksums_and_drops_late_chunks_of_a_refused_transfer() {
        let mut consumer = DataConsumer::new();
        let now = Instant::now();

        // Missing chunk checksum.
        let (mut single, _) = AgentMeshEnvelope::new_data_payload("src", "dst", b"abc");
        if let AgentMeshEnvelope::DataPayload {
            ref mut checksum_sha256,
            ..
        } = single
        {
            *checksum_sha256 = None;
        }
        match consumer.accept(&single, now) {
            DataVerdict::Rejected { code, reason } => {
                assert_eq!(code, ERR_PAYLOAD_CORRUPT);
                assert!(reason.contains("Missing SHA-256"), "{reason}");
            }
            other => panic!("expected Rejected, got {other:?}"),
        }

        // Multi-chunk transfer without the whole-payload SHA-256.
        let (mut envs, _, _) =
            AgentMeshEnvelope::new_data_payload_chunks("src", "dst", b"abcdefgh", 3);
        for env in &mut envs {
            if let AgentMeshEnvelope::DataPayload { metadata, .. } = env {
                *metadata = None;
            }
        }
        match consumer.accept(&envs[0], now) {
            DataVerdict::Rejected { reason, .. } => {
                assert!(reason.contains("total_sha256"), "{reason}")
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
        // The rest of that transfer is dropped without an answer and buffers nothing.
        assert!(matches!(
            consumer.accept(&envs[1], now),
            DataVerdict::Dropped
        ));
        assert!(matches!(
            consumer.accept(&envs[2], now),
            DataVerdict::Dropped
        ));
        assert_eq!(consumer.reassembler.total_inflight_bytes, 0);

        // A wrong whole-payload SHA-256 is refused on the last chunk, not acknowledged.
        let (mut bad, _, _) =
            AgentMeshEnvelope::new_data_payload_chunks("src", "dst", b"abcdef", 3);
        for env in &mut bad {
            if let AgentMeshEnvelope::DataPayload {
                metadata: Some(meta),
                ..
            } = env
            {
                meta.insert("total_sha256".to_string(), compute_sha256(b"other"));
            }
        }
        assert!(matches!(
            consumer.accept(&bad[0], now),
            DataVerdict::ChunkAccepted
        ));
        assert!(matches!(
            consumer.accept(&bad[1], now),
            DataVerdict::Rejected { .. }
        ));
    }

    #[test]
    fn test_chunk_reassembler_aggregate_byte_cap_evicts_oldest_fifo() {
        // Allow up to 10 assemblies, 100 bytes per assembly, and 120 bytes aggregate across all.
        let mut reassembler = ChunkReassembler::with_limits(10, 100, 120);
        let now = Instant::now();

        // Insert incomplete 50-byte chunk 0 for transfers "t1" and "t2" (total = 100 <= 120).
        assert!(reassembler
            .insert_chunk("peer", "t1", 0, 2, None, vec![b'1'; 50], now)
            .unwrap()
            .is_none());
        assert!(reassembler
            .insert_chunk("peer", "t2", 0, 2, None, vec![b'2'; 50], now)
            .unwrap()
            .is_none());
        assert_eq!(reassembler.inflight.len(), 2);
        assert_eq!(reassembler.total_inflight_bytes, 100);

        // Inserting "t3" (50 bytes) would hit 150 > 120, so oldest "t1" must be evicted FIFO.
        assert!(reassembler
            .insert_chunk("peer", "t3", 0, 2, None, vec![b'3'; 50], now)
            .unwrap()
            .is_none());
        assert!(!reassembler
            .inflight
            .contains_key(&("peer".to_string(), "t1".to_string())));
        assert!(reassembler
            .inflight
            .contains_key(&("peer".to_string(), "t2".to_string())));
        assert!(reassembler
            .inflight
            .contains_key(&("peer".to_string(), "t3".to_string())));
        assert_eq!(reassembler.total_inflight_bytes, 100);

        // Adding chunk 1 (40 bytes) to "t3" would reach 140 > 120, so oldest remaining "t2"
        // is evicted FIFO and "t3" completes cleanly, returning total_inflight_bytes to 0.
        let assembled = reassembler
            .insert_chunk("peer", "t3", 1, 2, None, vec![b'4'; 40], now)
            .unwrap()
            .expect("t3 should complete after second chunk");
        assert_eq!(assembled.bytes.len(), 90);
        assert!(reassembler.inflight.is_empty());
        assert_eq!(reassembler.total_inflight_bytes, 0);
    }

    #[test]
    fn test_chunk_reassembler_prune_expired_ttl() {
        // The clock is passed in, so the test needs no sleep and cannot race a slow runner.
        let ttl = Duration::from_secs(60);
        let mut reassembler = ChunkReassembler::new().with_ttl(ttl);
        let t0 = Instant::now();

        // Insert incomplete chunk 0 for "stale" (32 bytes).
        assert!(reassembler
            .insert_chunk("peer", "stale", 0, 2, None, vec![b's'; 32], t0)
            .unwrap()
            .is_none());
        assert_eq!(reassembler.inflight.len(), 1);
        assert_eq!(reassembler.total_inflight_bytes, 32);

        // At exactly the TTL the assembly is kept.
        reassembler.prune_expired(t0 + ttl);
        assert_eq!(reassembler.inflight.len(), 1);
        assert_eq!(reassembler.total_inflight_bytes, 32);

        // A fresh chunk after the TTL prunes the expired "stale" assembly and reclaims its bytes.
        let later = t0 + ttl + Duration::from_millis(1);
        assert!(reassembler
            .insert_chunk("peer", "fresh", 0, 2, None, vec![b'f'; 16], later)
            .unwrap()
            .is_none());
        assert!(!reassembler
            .inflight
            .contains_key(&("peer".to_string(), "stale".to_string())));
        assert!(reassembler
            .inflight
            .contains_key(&("peer".to_string(), "fresh".to_string())));
        assert_eq!(reassembler.inflight.len(), 1);
        assert_eq!(reassembler.total_inflight_bytes, 16);

        // Sending chunk 1 for "stale" after expiry starts a new assembly rather than completing
        // with the expired chunk 0.
        assert!(reassembler
            .insert_chunk("peer", "stale", 1, 2, None, vec![b'x'; 8], later)
            .unwrap()
            .is_none());
        assert_eq!(reassembler.total_inflight_bytes, 24);

        // A prune past the TTL of both clears everything and zeroes the byte count.
        reassembler.prune_expired(later + ttl + Duration::from_millis(1));
        assert!(reassembler.inflight.is_empty());
        assert_eq!(reassembler.total_inflight_bytes, 0);
    }
}
