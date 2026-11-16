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
