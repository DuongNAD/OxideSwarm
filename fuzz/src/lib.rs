//! Shared plumbing of the fuzz targets: an in-memory stream and a paused-clock runtime.

use std::io;
use std::pin::Pin;
use std::sync::OnceLock;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// An in-memory stream. Reads return `input` and then end of stream; writes are collected.
/// There are no tasks and no timers behind it, so a run is deterministic.
pub struct FuzzIo {
    input: Vec<u8>,
    pos: usize,
    /// Everything the code under test wrote.
    pub written: Vec<u8>,
}

impl FuzzIo {
    pub fn new(input: &[u8]) -> Self {
        Self {
            input: input.to_vec(),
            pos: 0,
            written: Vec::new(),
        }
    }
}

impl AsyncRead for FuzzIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let n = buf.remaining().min(self.input.len() - self.pos);
        let start = self.pos;
        buf.put_slice(&self.input[start..start + n]);
        self.pos += n;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for FuzzIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.written.extend_from_slice(data);
        Poll::Ready(Ok(data.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Runs `fut` on a current-thread runtime whose clock is paused: the handshake's 250 ms
/// failure delay then costs no wall-clock time.
pub fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("tokio runtime")
    })
    .block_on(fut)
}

/// The fixed test secret of the `server_handshake` target (v2).
pub const TEST_SECRET: [u8; 32] = [0x5a; 32];
/// The fixed test private key of the master in the `server_handshake` target (v3).
pub const TEST_MASTER_KEY: [u8; 32] = [0x11; 32];
/// The fixed test private key of the client that `gen_seeds` uses (v3).
pub const TEST_CLIENT_KEY: [u8; 32] = [0x22; 32];
/// The fixed test session key of the `aead_record` target: both directions' record keys are
/// derived from it, as after a completed handshake.
pub const TEST_SESSION_KEY: [u8; 32] = [0x33; 32];
/// The fixed test passphrase of the `invite.oxs` files of the `join_invite` target.
pub const TEST_PASSPHRASE: &str = "fuzz-test-passphrase";

/// Splits `data` into at most `max` chunks of `[len: u16 LE][len bytes]`. A chunk whose length
/// says more than what is left takes what is left; one stray byte at the end is ignored.
pub fn chunks(mut data: &[u8], max: usize) -> Vec<&[u8]> {
    let mut out = Vec::new();
    while data.len() >= 2 && out.len() < max {
        let len = usize::from(u16::from_le_bytes([data[0], data[1]]));
        let rest = &data[2..];
        let n = len.min(rest.len());
        out.push(&rest[..n]);
        data = &rest[n..];
    }
    out
}

/// The inverse of [`chunks`], for the seeds.
pub fn join_chunks<'a>(parts: impl IntoIterator<Item = &'a [u8]>) -> Vec<u8> {
    let mut out = Vec::new();
    for part in parts {
        let len = u16::try_from(part.len()).expect("a seed chunk fits in u16");
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(part);
    }
    out
}

/// `[len: u32 BE][payload]`, the framing of `MessageTransport`.
pub fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = (payload.len() as u32).to_be_bytes().to_vec();
    out.extend_from_slice(payload);
    out
}
