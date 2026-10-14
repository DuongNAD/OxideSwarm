//! Receive-side liveness tracking for worker connections.
//!
//! A worker on the single-lane TCP transport shares one socket between heartbeats and task results,
//! so while it uploads a multi-megabyte `TaskResult` no heartbeat can be sent and a pure
//! heartbeat-based reaper would kill a healthy connection mid-frame. [`StampedStream`] wraps the
//! socket and records the arrival time of every non-empty read in an [`RxClock`]; the registry
//! counts that as proof of life in addition to heartbeats. A connection that goes completely
//! silent stops advancing its clock and is reaped exactly as before.

use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Lock-free "last byte received" clock shared between a connection's reader and the registry.
#[derive(Debug)]
pub struct RxClock {
    epoch: Instant,
    last_rx_nanos: AtomicU64,
}

impl RxClock {
    /// Creates a clock whose last receive time is "now" (the connection was just accepted).
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            epoch: Instant::now(),
            last_rx_nanos: AtomicU64::new(0),
        })
    }

    /// Records that bytes were just received.
    pub fn touch(&self) {
        let nanos = self.epoch.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.last_rx_nanos.fetch_max(nanos, Ordering::Relaxed);
    }

    /// Monotonic instant of the most recent receive.
    pub fn last_rx(&self) -> Instant {
        self.epoch + Duration::from_nanos(self.last_rx_nanos.load(Ordering::Relaxed))
    }
}

/// `AsyncRead + AsyncWrite` adapter that stamps an [`RxClock`] whenever the inner stream yields data.
pub struct StampedStream<S> {
    inner: S,
    clock: Arc<RxClock>,
}

impl<S> StampedStream<S> {
    /// Wraps `inner`, stamping `clock` on every successful non-empty read.
    pub fn new(inner: S, clock: Arc<RxClock>) -> Self {
        Self { inner, clock }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for StampedStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &poll {
            if buf.filled().len() > before {
                self.clock.touch();
            }
        }
        poll
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for StampedStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn test_clock_advances_only_when_bytes_arrive() {
        let (mut a, b) = tokio::io::duplex(1024);
        let clock = RxClock::new();
        let mut stamped = StampedStream::new(b, clock.clone());

        let t0 = clock.last_rx();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(
            clock.last_rx(),
            t0,
            "an idle connection must not advance the clock"
        );

        a.write_all(b"hello").await.unwrap();
        let mut buf = [0u8; 5];
        stamped.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
        assert!(
            clock.last_rx() >= t0 + Duration::from_millis(25),
            "a non-empty read must stamp the clock"
        );

        // Writes pass through untouched and do not count as liveness.
        let before = clock.last_rx();
        stamped.write_all(b"pong").await.unwrap();
        let mut echo = [0u8; 4];
        a.read_exact(&mut echo).await.unwrap();
        assert_eq!(&echo, b"pong");
        assert_eq!(clock.last_rx(), before);
    }

    #[tokio::test]
    async fn test_eof_does_not_count_as_liveness() {
        let (a, b) = tokio::io::duplex(64);
        let clock = RxClock::new();
        let mut stamped = StampedStream::new(b, clock.clone());
        let t0 = clock.last_rx();
        drop(a);
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut buf = [0u8; 8];
        assert_eq!(stamped.read(&mut buf).await.unwrap(), 0);
        assert_eq!(clock.last_rx(), t0);
    }
}
