//! Unified transport abstraction for TCP, P2P (iroh), and in-memory streams.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// ALPN token identifying rusty-grid peer-to-peer connections over QUIC.
pub const GRID_ALPN: &[u8] = b"rusty-grid/v1";

/// Dedicated Control / Telemetry Stream discriminator tag (high priority).
pub const STREAM_CONTROL: u8 = 0x01;
/// Dedicated Task Data / Result Stream discriminator tag (bulk priority).
pub const STREAM_DATA: u8 = 0x02;
/// Single-stream Client RPC / Blob Stream discriminator tag (`connect_master`).
pub const STREAM_CLIENT: u8 = 0x03;

#[cfg(feature = "p2p")]
pub use iroh;

/// Keeps a client's ephemeral `iroh::Endpoint` and `iroh::endpoint::Connection` alive for the
/// lifetime of its `GridStream::P2p`, closing both when the stream drops.
#[cfg(feature = "p2p")]
pub struct P2pClientGuard {
    conn: iroh::endpoint::Connection,
    endpoint: iroh::Endpoint,
}

#[cfg(feature = "p2p")]
impl P2pClientGuard {
    pub fn new(conn: iroh::endpoint::Connection, endpoint: iroh::Endpoint) -> Self {
        Self { conn, endpoint }
    }
}

#[cfg(feature = "p2p")]
impl Drop for P2pClientGuard {
    fn drop(&mut self) {
        self.conn.close(0u32.into(), b"client done");
        let ep = self.endpoint.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                ep.close().await;
            });
        }
    }
}

/// Idle time before the first TCP keepalive probe is sent.
pub const TCP_KEEPALIVE_IDLE: Duration = Duration::from_secs(20);
/// Interval between TCP keepalive probes.
pub const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
/// Unanswered probes after which the kernel drops the connection.
pub const TCP_KEEPALIVE_RETRIES: u32 = 3;

/// Applies the grid's standard socket options to a connected TCP stream:
/// `TCP_NODELAY` (small control frames must not wait for Nagle) and TCP keepalive
/// (idle 20 s, probe every 10 s, 3 retries), so a peer that vanished without a FIN
/// (Wi-Fi drop, suspended phone, pulled cable) is detected in roughly 50 s instead of
/// the OS default of hours. Both options are always attempted; the first error is returned.
///
/// Platform notes: idle time, probe interval and retry count are requested on Linux, Android,
/// macOS/iOS and their siblings, the BSDs and Windows (socket2 maps them to the closest option
/// each OS offers). On any other target only the idle time is configured.
pub fn configure_tcp_stream(stream: &TcpStream) -> io::Result<()> {
    let nodelay = stream.set_nodelay(true);

    let keepalive = socket2::TcpKeepalive::new().with_time(TCP_KEEPALIVE_IDLE);
    #[cfg(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "illumos",
        target_os = "ios",
        target_os = "linux",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "tvos",
        target_os = "visionos",
        target_os = "watchos",
        target_os = "windows",
    ))]
    let keepalive = keepalive
        .with_interval(TCP_KEEPALIVE_INTERVAL)
        .with_retries(TCP_KEEPALIVE_RETRIES);

    let keepalive = socket2::SockRef::from(stream).set_tcp_keepalive(&keepalive);
    nodelay.and(keepalive)
}

/// A bidirectional stream adapter combining separate `AsyncRead` and `AsyncWrite` handles.
pub struct BiStream<R, W> {
    reader: R,
    writer: W,
    #[cfg(feature = "p2p")]
    #[allow(dead_code)]
    p2p_guard: Option<P2pClientGuard>,
}

impl<R, W> BiStream<R, W> {
    pub fn new(reader: R, writer: W) -> Self {
        Self {
            reader,
            writer,
            #[cfg(feature = "p2p")]
            p2p_guard: None,
        }
    }

    #[cfg(feature = "p2p")]
    pub fn with_p2p_guard(
        reader: R,
        writer: W,
        conn: iroh::endpoint::Connection,
        endpoint: iroh::Endpoint,
    ) -> Self {
        Self {
            reader,
            writer,
            p2p_guard: Some(P2pClientGuard::new(conn, endpoint)),
        }
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

/// Binds an iroh endpoint configured for `GRID_ALPN`, using `presets::Minimal` when
/// `minimal_preset` is true (offline / loopback test mode) and `presets::N0` otherwise.
#[cfg(feature = "p2p")]
pub async fn bind_p2p_endpoint(minimal_preset: bool) -> crate::GridResult<iroh::Endpoint> {
    let ep_builder = if minimal_preset {
        iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
    } else {
        iroh::Endpoint::builder(iroh::endpoint::presets::N0)
    };
    ep_builder
        .alpns(vec![GRID_ALPN.to_vec()])
        .bind()
        .await
        .map_err(|e| {
            crate::GridError::ConnectionFailed(format!("Failed to bind iroh endpoint: {e}"))
        })
}

/// Dials `node_addr` with `GRID_ALPN` within `connect_timeout`.
#[cfg(feature = "p2p")]
pub async fn dial_p2p_endpoint(
    endpoint: &iroh::Endpoint,
    node_addr: iroh::EndpointAddr,
    connect_timeout: Duration,
) -> crate::GridResult<iroh::endpoint::Connection> {
    match tokio::time::timeout(connect_timeout, endpoint.connect(node_addr, GRID_ALPN)).await {
        Ok(Ok(c)) => Ok(c),
        Ok(Err(e)) => Err(crate::GridError::ConnectionFailed(format!(
            "P2P connect failed: {e}"
        ))),
        Err(_) => Err(crate::GridError::Timeout {
            operation: "p2p_connect".into(),
            duration_secs: connect_timeout.as_secs(),
        }),
    }
}

/// Opens a bidirectional QUIC stream on `conn` and writes the 1-byte lane `tag`.
#[cfg(feature = "p2p")]
pub async fn open_p2p_tagged_stream(
    conn: &iroh::endpoint::Connection,
    tag: u8,
    label: &str,
) -> crate::GridResult<(iroh::endpoint::SendStream, iroh::endpoint::RecvStream)> {
    let (mut send, recv) = conn.open_bi().await.map_err(|e| {
        crate::GridError::ConnectionFailed(format!(
            "Failed to open bidirectional {label} stream: {e}"
        ))
    })?;
    tokio::io::AsyncWriteExt::write_all(&mut send, &[tag])
        .await
        .map_err(crate::GridError::Io)?;
    tokio::io::AsyncWriteExt::flush(&mut send)
        .await
        .map_err(crate::GridError::Io)?;
    Ok((send, recv))
}

/// Binds an ephemeral iroh endpoint, dials `node_addr` with `GRID_ALPN`, opens one bidirectional
/// stream tagged with [`STREAM_CLIENT`], and returns a [`GridStream::P2p`] whose endpoint closes
/// when the stream drops.
#[cfg(feature = "p2p")]
pub async fn connect_p2p_client_stream(
    node_addr: iroh::EndpointAddr,
    connect_timeout: Duration,
    minimal_preset: bool,
) -> crate::GridResult<GridStream> {
    let endpoint = bind_p2p_endpoint(minimal_preset).await?;
    let conn = match dial_p2p_endpoint(&endpoint, node_addr, connect_timeout).await {
        Ok(c) => c,
        Err(e) => {
            endpoint.close().await;
            return Err(e);
        }
    };
    let opened = tokio::time::timeout(
        connect_timeout,
        open_p2p_tagged_stream(&conn, STREAM_CLIENT, "client"),
    )
    .await;
    let (send, recv) = match opened {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => {
            conn.close(0u32.into(), b"client stream open failed");
            endpoint.close().await;
            return Err(e);
        }
        Err(_) => {
            conn.close(0u32.into(), b"client stream open timeout");
            endpoint.close().await;
            return Err(crate::GridError::Timeout {
                operation: "p2p_open_streams".into(),
                duration_secs: connect_timeout.as_secs(),
            });
        }
    };
    Ok(GridStream::P2p(BiStream::with_p2p_guard(
        recv, send, conn, endpoint,
    )))
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
    let selected = paths
        .iter()
        .find(|p| p.is_selected())
        .or_else(|| paths.iter().next());
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
    async fn test_configure_tcp_stream_sets_nodelay_and_keepalive() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (client, accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
        let client = client.unwrap();
        let (server, _) = accepted.unwrap();

        for stream in [&client, &server] {
            assert!(
                !stream.nodelay().unwrap(),
                "precondition: Nagle is on by default"
            );
            configure_tcp_stream(stream).unwrap();
            assert!(stream.nodelay().unwrap(), "TCP_NODELAY must be enabled");
            let sock = socket2::SockRef::from(stream);
            assert!(sock.keepalive().unwrap(), "SO_KEEPALIVE must be enabled");
            // The read-back getters exist on these targets only (the setters cover more).
            #[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
            assert_eq!(sock.tcp_keepalive_time().unwrap(), TCP_KEEPALIVE_IDLE);
            #[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
            assert_eq!(
                sock.tcp_keepalive_interval().unwrap(),
                TCP_KEEPALIVE_INTERVAL
            );
            #[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
            assert_eq!(sock.tcp_keepalive_retries().unwrap(), TCP_KEEPALIVE_RETRIES);
        }
        // Idempotent: applying it twice is fine.
        configure_tcp_stream(&client).unwrap();
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
        use iroh::endpoint::presets::Minimal;
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let endpoint = iroh::Endpoint::builder(Minimal)
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
