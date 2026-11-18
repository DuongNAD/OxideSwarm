//! Entry points for the fuzz targets of `fuzz/` (Cargo feature `fuzzing`, off by default; LV-18).
//!
//! [`TaskStore::open`](super::TaskStore::open) reads `journal.bin` and `snapshot.bin` from a
//! directory. These functions run the same readers on bytes in memory, and build the files the
//! way the store writes them, so a target can check that what the readers accept survives a
//! write and a second read. Nothing here is compiled into a normal build.

use std::path::Path;

use rusty_grid_core::error::GridResult;

use super::{
    encode_frame, encode_record_frame, header_bytes, parse_journal, parse_snapshot, JournalRecord,
    SnapshotImage, HEADER_LEN, JOURNAL_FILE, JOURNAL_MAGIC, SNAPSHOT_FILE, SNAPSHOT_MAGIC,
};

/// What the journal reader made of the bytes of `journal.bin`.
#[derive(Debug)]
pub struct JournalScan {
    /// The records of the valid prefix, in order.
    pub records: Vec<JournalRecord>,
    /// Length of the valid prefix (header plus whole frames); `open` cuts the file to it.
    pub valid_len: u64,
    /// A torn final frame was found and is cut off.
    pub truncated_tail: bool,
    /// The file is recreated with a fresh header.
    pub reset: bool,
    /// The records were discarded because the journal predates the snapshot.
    pub stale: bool,
}

/// What the snapshot reader made of the bytes of `snapshot.bin`.
#[derive(Debug)]
pub struct SnapshotScan {
    pub image: SnapshotImage,
    /// Protocol version in the file header.
    pub protocol_version: u32,
    /// Generation in the file header.
    pub generation: u64,
}

/// Reads `bytes` as `journal.bin` next to a snapshot of generation `snapshot_generation`, as
/// [`TaskStore::open`](super::TaskStore::open) does.
pub fn read_journal(bytes: &[u8], snapshot_generation: u64) -> GridResult<JournalScan> {
    let loaded = parse_journal(Path::new(JOURNAL_FILE), bytes, snapshot_generation)?;
    Ok(JournalScan {
        records: loaded.records,
        valid_len: loaded.valid_len,
        truncated_tail: loaded.truncated_tail,
        reset: loaded.reset,
        stale: loaded.stale,
    })
}

/// Reads `bytes` as `snapshot.bin`, as [`TaskStore::open`](super::TaskStore::open) does.
pub fn read_snapshot(bytes: &[u8]) -> GridResult<SnapshotScan> {
    let loaded = parse_snapshot(Path::new(SNAPSHOT_FILE), bytes)?;
    Ok(SnapshotScan {
        image: loaded.image,
        protocol_version: loaded.version,
        generation: loaded.generation,
    })
}

/// The header of a journal of `generation` written by this binary.
pub fn journal_header(generation: u64) -> [u8; HEADER_LEN] {
    header_bytes(JOURNAL_MAGIC, generation)
}

/// One journal frame around any `payload`: length, length check, BLAKE3, payload.
pub fn journal_frame(payload: &[u8]) -> GridResult<Vec<u8>> {
    encode_frame(payload)
}

/// One journal frame holding `record`, as the store appends it.
pub fn journal_record_frame(record: &JournalRecord) -> GridResult<Vec<u8>> {
    encode_record_frame(record)
}

/// A whole `snapshot.bin` of `generation` around any `payload`: header, length, BLAKE3, payload.
pub fn snapshot_file(generation: u64, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + 8 + 32 + payload.len());
    out.extend_from_slice(&header_bytes(SNAPSHOT_MAGIC, generation));
    out.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    out.extend_from_slice(blake3::hash(payload).as_bytes());
    out.extend_from_slice(payload);
    out
}

/// A whole `snapshot.bin` of `generation` holding `image`, with the JSON encoding compaction
/// writes.
pub fn snapshot_image_file(generation: u64, image: &SnapshotImage) -> GridResult<Vec<u8>> {
    Ok(snapshot_file(generation, &serde_json::to_vec(image)?))
}
