//! Unified transport abstraction for TCP, P2P (iroh), and in-memory streams.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// ALPN token identifying rusty-grid peer-to-peer connections over QUIC.
pub const GRID_ALPN: &[u8] = b"rusty-grid/v1";

/// Dedicated Control / Telemetry Stream discriminator tag (high priority).
pub const STREAM_CONTROL: u8 = 0x01;
/// Dedicated Task Data / Result Stream discriminator tag (bulk priority).
pub const STREAM_DATA: u8 = 0x02;

#[cfg(feature = "p2p")]
pub use iroh;


/// A bidirectional stream adapter combining separate `AsyncRead` and `AsyncWrite` handles.
pub struct BiStream<R, W> {
    reader: R,
    writer: W,
}

impl<R, W> BiStream<R, W> {
    pub fn new(reader: R, writer: W) -> Self {
        Self { reader, writer }
    }

    pub fn into_split(self) -> (R, W) {
        (self.reader, self.writer)
    }

    pub fn reader(&self) -> &R {
        &self.reader
    }

    pub fn writer(&self) -> &W {
        &self.writer
    }
}

impl<R: AsyncRead + Unpin, W: Unpin> AsyncRead for BiStream<R, W> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.reader).poll_read(cx, buf)
    }
}

impl<R: Unpin, W: AsyncWrite + Unpin> AsyncWrite for BiStream<R, W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.writer).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.writer).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.writer).poll_shutdown(cx)
    }
}

/// Dynamic stream abstraction supporting all cluster communication mediums.
pub enum GridStream {
    Tcp(TcpStream),
    Mock(tokio::io::DuplexStream),
    #[cfg(feature = "p2p")]
    P2p(BiStream<iroh::endpoint::RecvStream, iroh::endpoint::SendStream>),
}

impl AsyncRead for GridStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            GridStream::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            GridStream::Mock(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "p2p")]
            GridStream::P2p(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for GridStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            GridStream::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            GridStream::Mock(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "p2p")]
            GridStream::P2p(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            GridStream::Tcp(s) => Pin::new(s).poll_flush(cx),
            GridStream::Mock(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "p2p")]
            GridStream::P2p(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            GridStream::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            GridStream::Mock(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "p2p")]
            GridStream::P2p(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

#[cfg(feature = "p2p")]
pub fn serialize_p2p_ticket(addr: &iroh::EndpointAddr) -> Result<String, serde_json::Error> {
    serde_json::to_string(addr)
}

#[cfg(feature = "p2p")]
pub fn parse_p2p_ticket(ticket: &str) -> Result<iroh::EndpointAddr, serde_json::Error> {
    serde_json::from_str(ticket)
}

/// Runtime path information for an active P2P (iroh QUIC) connection.
#[cfg(feature = "p2p")]
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct P2pPathInfo {
    pub is_selected: bool,
    pub is_relay: bool,
    pub is_ip: bool,
    pub remote_addr: String,
    pub rtt_ms: f64,
    pub connection_type: String,
}

/// Inspects active paths of an iroh QUIC connection and extracts current selected/active path info.
#[cfg(feature = "p2p")]
pub fn inspect_connection_paths(conn: &iroh::endpoint::Connection) -> Option<P2pPathInfo> {
    let paths = conn.paths();
    let selected = paths.iter().find(|p| p.is_selected()).or_else(|| paths.iter().next());
    selected.map(|p| {
        let rtt_ms = p.rtt().as_secs_f64() * 1000.0;
        let is_relay = p.is_relay();
        let is_ip = p.is_ip();
        let remote_addr = p.remote_addr().to_string();
        let connection_type = if is_relay {
            format!("Relay (DERP: {remote_addr})")
        } else if is_ip {
            format!("Direct P2P (QUIC: {remote_addr})")
        } else {
            format!("P2P ({remote_addr})")
        };
        P2pPathInfo {
            is_selected: p.is_selected(),
            is_relay,
            is_ip,
            remote_addr,
            rtt_ms,
            connection_type,
        }
    })
}



#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::MessageTransport;
    use crate::protocol::WorkerMessage;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_bistream_adapter() {
        let (r_in, mut w_out) = tokio::io::duplex(1024);
        let (mut r_out, w_in) = tokio::io::duplex(1024);

        let mut bi = BiStream::new(r_in, w_in);

        // Test writing via BiStream
        tokio::io::AsyncWriteExt::write_all(&mut bi, b"hello bistream")
            .await
            .unwrap();
        let mut read_buf = vec![0u8; 14];
        tokio::io::AsyncReadExt::read_exact(&mut r_out, &mut read_buf)
            .await
            .unwrap();
        assert_eq!(&read_buf, b"hello bistream");

        // Test reading via BiStream
        tokio::io::AsyncWriteExt::write_all(&mut w_out, b"world bistream")
            .await
            .unwrap();
        let mut read_buf2 = vec![0u8; 14];
        tokio::io::AsyncReadExt::read_exact(&mut bi, &mut read_buf2)
            .await
            .unwrap();
        assert_eq!(&read_buf2, b"world bistream");
    }

    #[tokio::test]
    async fn test_gridstream_mock_with_messagetransport() {
        let (s1, s2) = tokio::io::duplex(4096);
        let mut t1 = MessageTransport::new(GridStream::Mock(s1));
        let mut t2 = MessageTransport::new(GridStream::Mock(s2));

        let hb = WorkerMessage::Heartbeat {
            worker_id: Uuid::new_v4(),
            timestamp: 12345,
            active_tasks: 0,
            cpu_usage_pct: 12.5,
            ram_available_mb: 4096,
        };

        t1.send_msg(&hb).await.unwrap();
        let received: Option<WorkerMessage> = t2.recv_msg().await.unwrap();
        assert_eq!(received, Some(hb));
    }

    #[cfg(feature = "p2p")]
    #[test]
    fn test_ticket_serialization_roundtrip() {
        use iroh::endpoint::presets::N0;
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let endpoint = iroh::Endpoint::builder(N0)
                .alpns(vec![GRID_ALPN.to_vec()])
                .bind()
                .await
                .unwrap();
            let addr = endpoint.addr();
            let ticket = serialize_p2p_ticket(&addr).unwrap();
            assert!(!ticket.is_empty());
            let parsed = parse_p2p_ticket(&ticket).unwrap();
            assert_eq!(addr, parsed);
        });
    }
}
