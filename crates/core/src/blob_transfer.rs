//! Moving blobs between peers: the request / reply messages and the transfer routines the
//! client, the worker and the Master share.
//!
//! Every exchange is one [`BlobRequest`] answered by one [`BlobReply`]. Clients send requests as
//! `ClientMessage::BlobRequest` (answered in order), workers as `WorkerMessage::BlobRequest` with a
//! `request_id` the Master echoes in `MasterMessage::BlobReply`. A worker sends them only to a
//! Master that announced [`crate::protocol::master_features::BLOBS`], and on a QUIC connection both
//! directions use the data stream, so bulk transfer never delays heartbeats on the control stream.
//!
//! * **Pull, not push.** A peer first asks which blobs the other side lacks
//!   ([`BlobRequest::Missing`]) and sends only those; a worker fetches only the blobs its cache
//!   lacks, when a task needs them.
//! * **Chunks.** A blob moves in chunks of at most [`BLOB_CHUNK_BYTES`], far below the 64 MiB
//!   `MAX_FRAME_SIZE`, so a blob of any size fits the framing. A receiver refuses a chunk longer
//!   than [`MAX_CHUNK_BYTES`] or than what is left of the blob. Chunks are not compressed.
//! * **Verification.** The receiver of a whole blob hashes it and refuses it unless the hash is the
//!   blob's id, so a blob can be corrupted in transit or on disk but never mistaken for another.

use std::future::Future;

use futures::stream::{self, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::blob::{BlobHasher, BlobId};
use crate::task::Bytes;

/// Bytes of blob content per chunk a sender produces.
pub const BLOB_CHUNK_BYTES: usize = 4 * 1024 * 1024;
/// Most bytes of blob content a receiver accepts in one chunk.
pub const MAX_CHUNK_BYTES: usize = 8 * 1024 * 1024;
/// Most blob ids in one [`BlobRequest::Missing`].
pub const MAX_MISSING_QUERY: usize = 65_536;
/// Blobs a peer moves at the same time (each one chunk at a time).
pub const TRANSFER_CONCURRENCY: usize = 8;
/// Most [`BlobRequest`]s one worker session keeps in flight at the same time (smaller than the
/// Master's per-session queue, so a cooperative worker never fills it).
pub const MAX_IN_FLIGHT_REQUESTS: usize = 16;

/// Checks a received chunk: not longer than [`MAX_CHUNK_BYTES`] or than the `remaining` bytes
/// of its blob, and not empty while bytes remain.
pub fn check_chunk_len(len: usize, remaining: u64) -> Result<(), String> {
    if len > MAX_CHUNK_BYTES || len as u64 > remaining {
        return Err(format!(
            "chunk of {len} bytes is longer than allowed ({} bytes)",
            remaining.min(MAX_CHUNK_BYTES as u64)
        ));
    }
    if len == 0 && remaining > 0 {
        return Err("empty chunk while bytes of the blob remain".to_string());
    }
    Ok(())
}

/// A request about blobs, sent to the Master.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobRequest {
    /// Which of `ids` the Master does not hold (at most [`MAX_MISSING_QUERY`]).
    Missing { ids: Vec<BlobId> },
    /// Up to `max_len` bytes of blob `id` from `offset` on.
    Read {
        id: BlobId,
        offset: u64,
        max_len: u32,
    },
    /// The next chunk of blob `id`, `total_size` bytes long: `data` goes at `offset`, which must
    /// be where the previous chunk ended (0 starts the upload).
    Write {
        id: BlobId,
        total_size: u64,
        offset: u64,
        data: Bytes,
    },
}

impl BlobRequest {
    /// Short name for log lines.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Missing { .. } => "Missing",
            Self::Read { .. } => "Read",
            Self::Write { .. } => "Write",
        }
    }
}

/// The Master's answer to a [`BlobRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobReply {
    /// The requested ids the Master does not hold, in request order.
    Missing { ids: Vec<BlobId> },
    /// Part of blob `id` (`total_size` bytes in all), starting at `offset`. An empty chunk at
    /// `offset == total_size` marks the end.
    Chunk {
        id: BlobId,
        offset: u64,
        total_size: u64,
        data: Bytes,
    },
    /// `received` bytes of the upload of `id` are stored; `complete` once the whole blob is
    /// verified and kept (also the answer to the first chunk of a blob the Master already has).
    Written {
        id: BlobId,
        received: u64,
        complete: bool,
    },
    /// The Master does not hold blob `id` (never uploaded, or evicted).
    NotFound { id: BlobId },
    /// The request was refused or failed.
    Failed { message: String },
}

/// What can go wrong moving a blob.
#[derive(Debug, thiserror::Error)]
pub enum BlobError {
    #[error("blob {0} is not on the master (never uploaded, or evicted from its blob store)")]
    NotFound(BlobId),
    #[error("blob {id} failed verification: {reason}")]
    Corrupt { id: BlobId, reason: String },
    #[error("blob transfer refused: {0}")]
    Refused(String),
    #[error("blob transfer failed: {0}")]
    Transport(String),
    #[error("I/O error during blob transfer: {0}")]
    Io(#[from] std::io::Error),
}

/// A way to send [`BlobRequest`]s to the Master and wait for the matching [`BlobReply`].
///
/// `call` may be used from several tasks at once; an implementation that can only do one
/// exchange at a time serialises them itself.
pub trait BlobChannel: Sync {
    fn call(
        &self,
        request: BlobRequest,
    ) -> impl Future<Output = Result<BlobReply, BlobError>> + Send;
}

fn unexpected(reply: BlobReply, wanted: &str) -> BlobError {
    match reply {
        BlobReply::Failed { message } => BlobError::Refused(message),
        BlobReply::NotFound { id } => BlobError::NotFound(id),
        other => BlobError::Transport(format!("expected {wanted}, the master answered {other:?}")),
    }
}

/// The ids among `ids` the Master lacks (asked in batches of [`MAX_MISSING_QUERY`]).
pub async fn missing_blobs<C: BlobChannel>(
    channel: &C,
    ids: &[BlobId],
) -> Result<Vec<BlobId>, BlobError> {
    let mut missing = Vec::new();
    for batch in ids.chunks(MAX_MISSING_QUERY) {
        match channel
            .call(BlobRequest::Missing {
                ids: batch.to_vec(),
            })
            .await?
        {
            BlobReply::Missing { ids } => missing.extend(ids),
            other => return Err(unexpected(other, "Missing")),
        }
    }
    Ok(missing)
}

/// Uploads blob `id` (`size` bytes, read from `reader`) chunk by chunk and returns the bytes
/// sent. Stops early when the Master says it already holds the blob.
pub async fn upload_blob<C, R>(
    channel: &C,
    id: BlobId,
    size: u64,
    mut reader: R,
) -> Result<u64, BlobError>
where
    C: BlobChannel,
    R: AsyncRead + Unpin + Send,
{
    let mut offset = 0u64;
    let mut buf = vec![0u8; (size.min(BLOB_CHUNK_BYTES as u64)) as usize];
    loop {
        let want = (size - offset).min(BLOB_CHUNK_BYTES as u64) as usize;
        reader.read_exact(&mut buf[..want]).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                BlobError::Corrupt {
                    id,
                    reason: format!(
                        "the source ended before {size} bytes (did it change during the upload?)"
                    ),
                }
            } else {
                BlobError::Io(e)
            }
        })?;
        let reply = channel
            .call(BlobRequest::Write {
                id,
                total_size: size,
                offset,
                data: Bytes::copy_from_slice(&buf[..want]),
            })
            .await?;
        offset += want as u64;
        match reply {
            BlobReply::Written { complete: true, .. } => return Ok(offset),
            BlobReply::Written { received, .. } if received == offset && offset < size => {}
            BlobReply::Written { received, .. } => {
                return Err(BlobError::Transport(format!(
                    "the master holds {received} bytes of blob {id} after {offset} were sent"
                )))
            }
            other => return Err(unexpected(other, "Written")),
        }
    }
}

/// Downloads blob `id` into `writer`, checks it against its id and returns its size.
/// `expected_size`, when known, must match what the Master reports.
pub async fn download_blob<C, W>(
    channel: &C,
    id: BlobId,
    expected_size: Option<u64>,
    mut writer: W,
) -> Result<u64, BlobError>
where
    C: BlobChannel,
    W: AsyncWrite + Unpin + Send,
{
    let mut hasher = BlobHasher::new();
    let mut offset = 0u64;
    loop {
        let reply = channel
            .call(BlobRequest::Read {
                id,
                offset,
                max_len: BLOB_CHUNK_BYTES as u32,
            })
            .await?;
        let (chunk_offset, total_size, data) = match reply {
            BlobReply::Chunk {
                id: chunk_id,
                offset,
                total_size,
                data,
            } if chunk_id == id => (offset, total_size, data),
            other => return Err(unexpected(other, "Chunk")),
        };
        if let Some(expected) = expected_size.filter(|&e| e != total_size) {
            return Err(BlobError::Corrupt {
                id,
                reason: format!("expected {expected} bytes, the master holds {total_size}"),
            });
        }
        if chunk_offset != offset || offset > total_size {
            return Err(BlobError::Transport(format!(
                "asked for offset {offset} of blob {id} ({total_size} bytes), got {chunk_offset}"
            )));
        }
        check_chunk_len(data.len(), total_size - offset)
            .map_err(|reason| BlobError::Corrupt { id, reason })?;
        hasher.update(data.as_bytes());
        writer.write_all(data.as_bytes()).await?;
        offset += data.len() as u64;
        if offset == total_size {
            break;
        }
    }
    writer.flush().await?;
    let actual = hasher.finish();
    if actual != id {
        return Err(BlobError::Corrupt {
            id,
            reason: format!("content hashes to {actual}"),
        });
    }
    Ok(offset)
}

/// Runs `job` for every item, [`TRANSFER_CONCURRENCY`] at a time, and adds up the byte counts
/// they return. Stops at the first error.
pub async fn for_each_concurrently<T, F, Fut>(items: Vec<T>, job: F) -> Result<u64, BlobError>
where
    F: Fn(T) -> Fut,
    Fut: Future<Output = Result<u64, BlobError>>,
{
    stream::iter(items.into_iter().map(job))
        .buffer_unordered(TRANSFER_CONCURRENCY)
        .try_fold(0u64, |total, bytes| async move { Ok(total + bytes) })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// A Master in a box: the blob semantics of the real store, without disk or network.
    #[derive(Default)]
    struct MemoryMaster {
        blobs: Mutex<HashMap<BlobId, Vec<u8>>>,
        uploads: Mutex<HashMap<BlobId, Vec<u8>>>,
        writes: Mutex<usize>,
    }

    impl BlobChannel for MemoryMaster {
        async fn call(&self, request: BlobRequest) -> Result<BlobReply, BlobError> {
            Ok(match request {
                BlobRequest::Missing { ids } => {
                    let blobs = self.blobs.lock().unwrap();
                    BlobReply::Missing {
                        ids: ids
                            .into_iter()
                            .filter(|id| !blobs.contains_key(id))
                            .collect(),
                    }
                }
                BlobRequest::Read {
                    id,
                    offset,
                    max_len,
                } => {
                    let blobs = self.blobs.lock().unwrap();
                    let Some(blob) = blobs.get(&id) else {
                        return Ok(BlobReply::NotFound { id });
                    };
                    let start = offset as usize;
                    let end = (start + max_len as usize).min(blob.len());
                    BlobReply::Chunk {
                        id,
                        offset,
                        total_size: blob.len() as u64,
                        data: Bytes::copy_from_slice(&blob[start..end]),
                    }
                }
                BlobRequest::Write {
                    id,
                    total_size,
                    offset,
                    data,
                } => {
                    *self.writes.lock().unwrap() += 1;
                    if self.blobs.lock().unwrap().contains_key(&id) {
                        return Ok(BlobReply::Written {
                            id,
                            received: total_size,
                            complete: true,
                        });
                    }
                    let mut uploads = self.uploads.lock().unwrap();
                    let buf = uploads.entry(id).or_default();
                    assert_eq!(buf.len() as u64, offset, "chunks arrive in order");
                    buf.extend_from_slice(data.as_bytes());
                    let received = buf.len() as u64;
                    if received == total_size {
                        let done = uploads.remove(&id).unwrap();
                        if BlobId::of(&done) != id {
                            return Ok(BlobReply::Failed {
                                message: "content does not match the blob id".into(),
                            });
                        }
                        self.blobs.lock().unwrap().insert(id, done);
                    }
                    BlobReply::Written {
                        id,
                        received,
                        complete: received == total_size,
                    }
                }
            })
        }
    }

    fn sample(len: usize, seed: u8) -> Vec<u8> {
        (0..len)
            .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
            .collect()
    }

    #[test]
    fn chunk_length_checks_refuse_oversized_and_empty_chunks() {
        assert!(check_chunk_len(10, 10).is_ok());
        assert!(check_chunk_len(0, 0).is_ok(), "the end of an empty blob");
        assert!(check_chunk_len(11, 10).is_err());
        assert!(check_chunk_len(0, 5).is_err());
        assert!(check_chunk_len(MAX_CHUNK_BYTES + 1, u64::MAX).is_err());
    }

    #[tokio::test]
    async fn upload_then_download_round_trips_multi_chunk_blobs() {
        let master = MemoryMaster::default();
        for len in [
            0usize,
            1,
            BLOB_CHUNK_BYTES - 1,
            BLOB_CHUNK_BYTES,
            2 * BLOB_CHUNK_BYTES + 17,
        ] {
            let data = sample(len, len as u8);
            let id = BlobId::of(&data);
            assert_eq!(missing_blobs(&master, &[id]).await.unwrap(), vec![id]);
            let sent = upload_blob(&master, id, len as u64, &data[..])
                .await
                .unwrap();
            assert_eq!(sent, len as u64);
            assert!(missing_blobs(&master, &[id]).await.unwrap().is_empty());

            let mut out = Vec::new();
            let got = download_blob(&master, id, Some(len as u64), &mut out)
                .await
                .unwrap();
            assert_eq!(out, data, "{len} bytes must round-trip");
            assert_eq!(got, len as u64);
        }
    }

    #[tokio::test]
    async fn uploading_a_blob_the_master_has_stops_after_one_chunk() {
        let master = MemoryMaster::default();
        let data = sample(3 * BLOB_CHUNK_BYTES, 9);
        let id = BlobId::of(&data);
        upload_blob(&master, id, data.len() as u64, &data[..])
            .await
            .unwrap();
        let before = *master.writes.lock().unwrap();
        upload_blob(&master, id, data.len() as u64, &data[..])
            .await
            .unwrap();
        assert_eq!(*master.writes.lock().unwrap(), before + 1);
    }

    #[tokio::test]
    async fn content_that_does_not_match_its_id_is_refused_both_ways() {
        let master = MemoryMaster::default();
        let data = sample(1000, 1);
        let wrong_id = BlobId::of(b"something else");
        let err = upload_blob(&master, wrong_id, 1000, &data[..])
            .await
            .unwrap_err();
        assert!(matches!(err, BlobError::Refused(_)), "{err}");

        // A store that hands out the wrong bytes is caught by the downloader.
        let id = BlobId::of(&data);
        master.blobs.lock().unwrap().insert(id, sample(1000, 2));
        let err = download_blob(&master, id, None, &mut Vec::new())
            .await
            .unwrap_err();
        assert!(matches!(err, BlobError::Corrupt { .. }), "{err}");

        let absent = BlobId::of(b"absent");
        let err = download_blob(&master, absent, None, &mut Vec::new())
            .await
            .unwrap_err();
        assert!(
            matches!(err, BlobError::NotFound(id) if id == absent),
            "{err}"
        );
    }

    #[tokio::test]
    async fn a_source_shorter_than_announced_is_an_error_not_a_hang() {
        let master = MemoryMaster::default();
        let data = sample(10, 3);
        let err = upload_blob(&master, BlobId::of(&data), 20, &data[..])
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("change during the upload"),
            "{err}"
        );
    }

    #[test]
    fn requests_and_replies_round_trip_in_both_codecs() {
        let id = BlobId::of(b"x");
        let request = BlobRequest::Write {
            id,
            total_size: 3,
            offset: 0,
            data: Bytes::from(vec![0xff, 0x00, 0x80]),
        };
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains(&id.to_hex()), "{json}");
        assert_eq!(serde_json::from_str::<BlobRequest>(&json).unwrap(), request);
        let bin = bincode::serialize(&request).unwrap();
        assert_eq!(bincode::deserialize::<BlobRequest>(&bin).unwrap(), request);

        let reply = BlobReply::Missing { ids: vec![id] };
        let bin = bincode::serialize(&reply).unwrap();
        assert_eq!(bincode::deserialize::<BlobReply>(&bin).unwrap(), reply);
    }
}
