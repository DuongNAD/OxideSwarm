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

#[cfg(feature = "p2p")]
static PENDING_P2P_CLIENT_CLOSES: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(feature = "p2p")]
fn register_pending_p2p_client_close(handle: tokio::task::JoinHandle<()>) {
    if let Ok(mut pending) = PENDING_P2P_CLIENT_CLOSES.lock() {
        pending.retain(|h| !h.is_finished());
        pending.push(handle);
    }
}

/// Awaits any in-flight background `iroh::Endpoint::close()` tasks spawned when a
/// [`P2pClientGuard`] was dropped inside a Tokio runtime, ensuring every ephemeral client
/// endpoint finishes closing cleanly before the runtime shuts down.
#[cfg(feature = "p2p")]
pub async fn drain_pending_p2p_client_closes() {
    // A close that does not finish in time must not keep the process from exiting: past the
    // deadline the remaining close tasks are aborted, and the endpoints drop ungracefully.
    let deadline = tokio::time::Instant::now() + P2P_CLIENT_CLOSE_TIMEOUT;
    loop {
        let batch: Vec<tokio::task::JoinHandle<()>> = match PENDING_P2P_CLIENT_CLOSES.lock() {
            Ok(mut pending) => std::mem::take(&mut *pending),
            Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
        };
        if batch.is_empty() {
            break;
        }
        for mut handle in batch {
            if tokio::time::timeout_at(deadline, &mut handle)
                .await
                .is_err()
            {
                handle.abort();
            }
        }
    }
}

/// Upper bound on waiting for client P2P endpoints to close, in [`P2pClientGuard::close`] and in
/// [`drain_pending_p2p_client_closes`].
#[cfg(feature = "p2p")]
pub const P2P_CLIENT_CLOSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

#[cfg(not(feature = "p2p"))]
pub async fn drain_pending_p2p_client_closes() {}

/// Keeps a client's ephemeral `iroh::Endpoint` and `iroh::endpoint::Connection` alive for the
/// lifetime of its `GridStream::P2p`, closing both when [`P2pClientGuard::close`] is called or
/// when the stream (or both of its [`BiStream::into_split`] halves) drops.
///
/// When closed or dropped inside a Tokio runtime, `endpoint.close()` is spawned onto the runtime
/// and registered with [`drain_pending_p2p_client_closes`]; when dropped outside a Tokio runtime,
/// a temporary current-thread runtime drives `endpoint.close()` synchronously so the endpoint
/// still shuts down cleanly.
#[cfg(feature = "p2p")]
pub struct P2pClientGuard {
    conn: iroh::endpoint::Connection,
    endpoint: iroh::Endpoint,
    close_started: std::sync::atomic::AtomicBool,
    closed_tx: tokio::sync::watch::Sender<bool>,
    closed_rx: tokio::sync::watch::Receiver<bool>,
}

#[cfg(feature = "p2p")]
impl P2pClientGuard {
    pub fn new(conn: iroh::endpoint::Connection, endpoint: iroh::Endpoint) -> Self {
        let (closed_tx, closed_rx) = tokio::sync::watch::channel(false);
        Self {
            conn,
            endpoint,
            close_started: std::sync::atomic::AtomicBool::new(false),
            closed_tx,
            closed_rx,
        }
    }

    /// Initiates graceful shutdown of the QUIC connection and underlying [`iroh::Endpoint`]
    /// without awaiting directly: spawns `endpoint.close()` onto the active Tokio runtime
    /// (registering its handle with [`drain_pending_p2p_client_closes`]), or drives it
    /// synchronously on a temporary current-thread runtime when no Tokio runtime is active.
    pub fn start_close(&self) {
        if self
            .close_started
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        self.conn.close(0u32.into(), b"client done");
        if self.endpoint.is_closed() {
            let _ = self.closed_tx.send(true);
            return;
        }
        let ep = self.endpoint.clone();
        let closed_tx = self.closed_tx.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let join = handle.spawn(async move {
                ep.close().await;
                let _ = closed_tx.send(true);
            });
            register_pending_p2p_client_close(join);
        } else if let Ok(rt) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            rt.block_on(ep.close());
            let _ = closed_tx.send(true);
        }
    }

    /// Closes the QUIC connection and awaits graceful shutdown of the underlying
    /// [`iroh::Endpoint`]. Safe to call multiple times; subsequent calls and [`Drop`] are no-ops
    /// once the endpoint is closed.
    pub async fn close(&self) {
        self.start_close();
        let mut rx = self.closed_rx.clone();
        let _ = tokio::time::timeout(P2P_CLIENT_CLOSE_TIMEOUT, rx.wait_for(|closed| *closed)).await;
    }
}

#[cfg(feature = "p2p")]
impl Drop for P2pClientGuard {
    fn drop(&mut self) {
        self.start_close();
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

/// Read half produced by [`BiStream::into_split`], retaining a shared reference to any
/// [`P2pClientGuard`] so a client P2P connection stays open until both halves drop.
pub struct BiStreamReadHalf<R> {
    reader: R,
    #[cfg(feature = "p2p")]
    #[allow(dead_code)]
    p2p_guard: Option<std::sync::Arc<P2pClientGuard>>,
}

impl<R> BiStreamReadHalf<R> {
    pub fn inner(&self) -> &R {
        &self.reader
    }

    pub fn inner_mut(&mut self) -> &mut R {
        &mut self.reader
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for BiStreamReadHalf<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.reader).poll_read(cx, buf)
    }
}

/// Write half produced by [`BiStream::into_split`], retaining a shared reference to any
/// [`P2pClientGuard`] so a client P2P connection stays open until both halves drop.
pub struct BiStreamWriteHalf<W> {
    writer: W,
    #[cfg(feature = "p2p")]
    #[allow(dead_code)]
    p2p_guard: Option<std::sync::Arc<P2pClientGuard>>,
}

impl<W> BiStreamWriteHalf<W> {
    pub fn inner(&self) -> &W {
        &self.writer
    }

    pub fn inner_mut(&mut self) -> &mut W {
        &mut self.writer
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for BiStreamWriteHalf<W> {
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

/// A bidirectional stream adapter combining separate `AsyncRead` and `AsyncWrite` handles.
pub struct BiStream<R, W> {
    reader: R,
    writer: W,
    #[cfg(feature = "p2p")]
    #[allow(dead_code)]
    p2p_guard: Option<std::sync::Arc<P2pClientGuard>>,
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
            p2p_guard: Some(std::sync::Arc::new(P2pClientGuard::new(conn, endpoint))),
        }
    }

    /// Splits the stream into independent read and write halves while keeping any attached
    /// [`P2pClientGuard`] alive until both halves are dropped.
    pub fn into_split(self) -> (BiStreamReadHalf<R>, BiStreamWriteHalf<W>) {
        #[cfg(feature = "p2p")]
        let read_guard = self.p2p_guard.clone();
        #[cfg(feature = "p2p")]
        let write_guard = self.p2p_guard;
        (
            BiStreamReadHalf {
                reader: self.reader,
                #[cfg(feature = "p2p")]
                p2p_guard: read_guard,
            },
            BiStreamWriteHalf {
                writer: self.writer,
                #[cfg(feature = "p2p")]
                p2p_guard: write_guard,
            },
        )
    }

    pub fn reader(&self) -> &R {
        &self.reader
    }

    pub fn writer(&self) -> &W {
        &self.writer
    }

    #[cfg(feature = "p2p")]
    pub fn p2p_guard(&self) -> Option<std::sync::Arc<P2pClientGuard>> {
        self.p2p_guard.clone()
    }

    #[cfg(feature = "p2p")]
    pub async fn close_p2p(&self) {
        if let Some(ref guard) = self.p2p_guard {
            guard.close().await;
        }
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

impl GridStream {
    #[cfg(feature = "p2p")]
    pub fn p2p_guard(&self) -> Option<std::sync::Arc<P2pClientGuard>> {
        match self {
            GridStream::P2p(bi) => bi.p2p_guard(),
            _ => None,
        }
    }

    /// Gracefully closes the underlying client P2P endpoint (if this is a [`GridStream::P2p`]
    /// with an attached [`P2pClientGuard`]). No-op for TCP and in-memory streams.
    pub async fn close(&mut self) {
        #[cfg(feature = "p2p")]
        if let GridStream::P2p(bi) = self {
            bi.close_p2p().await;
        }
    }
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

/// Environment variable naming a custom iroh relay URL (`https://...`) for master, worker,
/// and client P2P endpoints.
pub const RELAY_URL_ENV: &str = "RUSTY_GRID_RELAY_URL";

#[cfg(feature = "p2p")]
pub fn serialize_p2p_ticket(addr: &iroh::EndpointAddr) -> Result<String, serde_json::Error> {
    serde_json::to_string(addr)
}

#[cfg(feature = "p2p")]
pub fn parse_p2p_ticket(ticket: &str) -> Result<iroh::EndpointAddr, serde_json::Error> {
    serde_json::from_str(ticket)
}

/// Validates a custom iroh relay URL (`--relay-url` / `RUSTY_GRID_RELAY_URL` / `relay_url`):
///
/// * Must not be empty or contain whitespace.
/// * Must parse as an [`iroh::RelayUrl`].
/// * Scheme must be `https` (`http://` and other schemes are refused).
/// * Must not contain embedded `user:password` (or `user@`) credentials.
/// * Must have a non-empty host and no query or fragment.
///
/// Never echoes embedded credentials in error messages.
#[cfg(feature = "p2p")]
pub fn validate_relay_url(raw: &str) -> crate::GridResult<iroh::RelayUrl> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(crate::GridError::Config(
            "Invalid relay URL: URL cannot be empty (expected https://<host>[:<port>])".into(),
        ));
    }
    if trimmed.chars().any(char::is_whitespace) {
        return Err(crate::GridError::Config(
            "Invalid relay URL: URL must not contain whitespace".into(),
        ));
    }
    let has_at_in_authority = trimmed
        .find("://")
        .map(|idx| {
            let after_scheme = &trimmed[idx + 3..];
            let authority_end = after_scheme
                .find(['/', '?', '#'])
                .unwrap_or(after_scheme.len());
            after_scheme[..authority_end].contains('@')
        })
        .unwrap_or_else(|| trimmed.contains('@'));
    if has_at_in_authority {
        return Err(crate::GridError::Config(
            "Invalid relay URL: URL must not contain user:password credentials".into(),
        ));
    }
    let parsed: iroh::RelayUrl = trimmed.parse().map_err(|e| {
        crate::GridError::Config(format!(
            "Invalid relay URL '{trimmed}': must be a valid https:// relay URL ({e})"
        ))
    })?;
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(crate::GridError::Config(
            "Invalid relay URL: URL must not contain user:password credentials".into(),
        ));
    }
    if parsed.scheme() != "https" {
        return Err(crate::GridError::Config(format!(
            "Invalid relay URL '{trimmed}': scheme must be 'https' (got '{}')",
            parsed.scheme()
        )));
    }
    if parsed.host_str().is_none_or(|h| h.is_empty()) {
        return Err(crate::GridError::Config(format!(
            "Invalid relay URL '{trimmed}': missing host"
        )));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(crate::GridError::Config(format!(
            "Invalid relay URL '{trimmed}': URL must not include a query string or fragment"
        )));
    }
    Ok(parsed)
}

/// Validates `raw` and builds a custom [`iroh::RelayMap`] containing that single relay URL.
#[cfg(feature = "p2p")]
pub fn build_custom_relay_map(raw: &str) -> crate::GridResult<iroh::RelayMap> {
    let url = validate_relay_url(raw)?;
    Ok(iroh::RelayMap::from(url))
}

/// Resolves the [`iroh::RelayMode`] for a P2P endpoint from `minimal_preset` and `relay_url`.
///
/// * `minimal_preset = true` with `relay_url = Some(_)` is refused as a configuration error.
/// * `minimal_preset = true` with `relay_url = None` returns [`iroh::RelayMode::Disabled`].
/// * `minimal_preset = false` with `relay_url = Some(url)` validates `url` and returns
///   [`iroh::RelayMode::Custom`] containing that single-entry [`iroh::RelayMap`].
/// * `minimal_preset = false` with `relay_url = None` returns [`iroh::RelayMode::Default`].
#[cfg(feature = "p2p")]
pub fn resolve_p2p_relay_mode(
    minimal_preset: bool,
    relay_url: Option<&str>,
) -> crate::GridResult<iroh::RelayMode> {
    if let Some(raw) = relay_url {
        let map = build_custom_relay_map(raw)?;
        if minimal_preset {
            return Err(crate::GridError::Config(
                "Cannot combine a custom relay URL (--relay-url / RUSTY_GRID_RELAY_URL / relay_url) \
                 with P2P minimal/no-relay mode (RUSTY_GRID_P2P_MINIMAL / p2p_minimal_preset)"
                    .into(),
            ));
        }
        Ok(iroh::RelayMode::Custom(map))
    } else if minimal_preset {
        Ok(iroh::RelayMode::Disabled)
    } else {
        Ok(iroh::RelayMode::Default)
    }
}

/// Builds an [`iroh::endpoint::Builder`] for the requested `minimal_preset` and optional
/// `relay_url`:
///
/// * `minimal_preset = true`, `relay_url = None`: `presets::Minimal` (no relay, no pkarr/DNS).
/// * `minimal_preset = false`, `relay_url = Some(url)`: `presets::Minimal` +
///   `RelayMode::Custom(RelayMap::from(url))` (uses the owner's relay instead of `presets::N0`).
/// * `minimal_preset = false`, `relay_url = None`: `presets::N0` (default).
#[cfg(feature = "p2p")]
pub fn p2p_endpoint_builder(
    minimal_preset: bool,
    relay_url: Option<&str>,
) -> crate::GridResult<iroh::endpoint::Builder> {
    match resolve_p2p_relay_mode(minimal_preset, relay_url)? {
        iroh::RelayMode::Disabled => Ok(iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)),
        iroh::RelayMode::Custom(relay_map) => {
            Ok(iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
                .relay_mode(iroh::RelayMode::Custom(relay_map)))
        }
        iroh::RelayMode::Default | iroh::RelayMode::Staging => {
            Ok(iroh::Endpoint::builder(iroh::endpoint::presets::N0))
        }
    }
}

/// Binds an iroh endpoint configured for `GRID_ALPN`, using `presets::Minimal` when
/// `minimal_preset` is true (offline / loopback test mode) and `presets::N0` otherwise.
#[cfg(feature = "p2p")]
pub async fn bind_p2p_endpoint(minimal_preset: bool) -> crate::GridResult<iroh::Endpoint> {
    bind_p2p_endpoint_with_relay(minimal_preset, None).await
}

/// Binds an iroh endpoint configured for `GRID_ALPN`, using `minimal_preset` or an optional
/// custom `relay_url` (`iroh::RelayMode::Custom`) instead of `presets::N0`.
#[cfg(feature = "p2p")]
pub async fn bind_p2p_endpoint_with_relay(
    minimal_preset: bool,
    relay_url: Option<&str>,
) -> crate::GridResult<iroh::Endpoint> {
    let ep_builder = p2p_endpoint_builder(minimal_preset, relay_url)?;
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
    connect_p2p_client_stream_with_relay(node_addr, connect_timeout, minimal_preset, None).await
}

/// Like [`connect_p2p_client_stream`], but accepts an optional custom `relay_url` (`https://...`)
/// so the ephemeral client endpoint uses [`iroh::RelayMode::Custom`] instead of `presets::N0`.
#[cfg(feature = "p2p")]
pub async fn connect_p2p_client_stream_with_relay(
    node_addr: iroh::EndpointAddr,
    connect_timeout: Duration,
    minimal_preset: bool,
    relay_url: Option<&str>,
) -> crate::GridResult<GridStream> {
    let endpoint = bind_p2p_endpoint_with_relay(minimal_preset, relay_url).await?;
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
            endpoint.close().await;
        });
    }

    #[cfg(feature = "p2p")]
    #[test]
    fn test_bistream_into_split_preserves_p2p_guard_and_closes_outside_runtime() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (read_half, write_half, server_ep, server_task) = rt.block_on(async {
            let server_ep = bind_p2p_endpoint(true).await.unwrap();
            let addr = server_ep.addr();
            let server_ep_clone = server_ep.clone();
            let server_task = tokio::spawn(async move {
                let incoming = server_ep_clone.accept().await.unwrap();
                let conn = incoming.await.unwrap();
                let (mut send, mut recv) = conn.accept_bi().await.unwrap();
                let mut tag = [0u8; 1];
                tokio::io::AsyncReadExt::read_exact(&mut recv, &mut tag)
                    .await
                    .unwrap();
                assert_eq!(tag[0], STREAM_CLIENT);
                let mut buf = [0u8; 5];
                tokio::io::AsyncReadExt::read_exact(&mut recv, &mut buf)
                    .await
                    .unwrap();
                assert_eq!(&buf, b"hello");
                tokio::io::AsyncWriteExt::write_all(&mut send, b"world")
                    .await
                    .unwrap();
                tokio::io::AsyncWriteExt::flush(&mut send).await.unwrap();
                conn
            });

            let stream = connect_p2p_client_stream(addr, Duration::from_secs(5), true)
                .await
                .unwrap();
            let GridStream::P2p(bi) = stream else {
                panic!("expected GridStream::P2p");
            };
            let (mut r, mut w) = bi.into_split();
            tokio::io::AsyncWriteExt::write_all(&mut w, b"hello")
                .await
                .unwrap();
            tokio::io::AsyncWriteExt::flush(&mut w).await.unwrap();
            let mut reply = [0u8; 5];
            tokio::io::AsyncReadExt::read_exact(&mut r, &mut reply)
                .await
                .unwrap();
            assert_eq!(&reply, b"world");
            (r, w, server_ep, server_task)
        });

        let server_conn = rt.block_on(server_task).unwrap();
        rt.block_on(server_ep.close());
        drop(rt);
        drop(server_conn);

        // Dropping the split halves outside any Tokio runtime must still cleanly close the
        // client's P2pClientGuard without panicking.
        drop(read_half);
        drop(write_half);
    }

    #[cfg(feature = "p2p")]
    #[test]
    fn test_validate_relay_url_accepts_https_and_builds_custom_relay_map() {
        let url = validate_relay_url("https://relay.example.com").unwrap();
        assert_eq!(url.scheme(), "https");
        assert_eq!(url.host_str(), Some("relay.example.com"));

        let url_with_port = validate_relay_url("  https://relay.example.com:8443/  ").unwrap();
        assert_eq!(url_with_port.scheme(), "https");
        assert_eq!(url_with_port.host_str(), Some("relay.example.com"));
        assert_eq!(url_with_port.port(), Some(8443));

        let map = build_custom_relay_map("https://relay.example.com:8443").unwrap();
        assert!(map.contains(&url_with_port));
        let urls: Vec<_> = map.urls();
        assert_eq!(urls, vec![url_with_port.clone()]);

        let mode = resolve_p2p_relay_mode(false, Some("https://relay.example.com:8443")).unwrap();
        match mode {
            iroh::RelayMode::Custom(custom_map) => {
                assert!(custom_map.contains(&url_with_port));
                assert_eq!(custom_map.urls::<Vec<_>>(), vec![url_with_port]);
            }
            other => panic!("expected RelayMode::Custom, got {other:?}"),
        }

        assert!(matches!(
            resolve_p2p_relay_mode(false, None).unwrap(),
            iroh::RelayMode::Default
        ));
        assert!(matches!(
            resolve_p2p_relay_mode(true, None).unwrap(),
            iroh::RelayMode::Disabled
        ));
    }

    #[cfg(feature = "p2p")]
    #[test]
    fn test_validate_relay_url_refuses_http_credentials_and_garbage() {
        // Non-https schemes must be refused.
        for bad_scheme in [
            "http://relay.example.com",
            "http://relay.example.com:3340",
            "ws://relay.example.com",
            "wss://relay.example.com",
            "ftp://relay.example.com",
        ] {
            let err = validate_relay_url(bad_scheme).unwrap_err().to_string();
            assert!(
                err.contains("https"),
                "expected https error for '{bad_scheme}', got: {err}"
            );
        }

        // Credentials must be refused without echoing the secret.
        for with_creds in [
            "https://alice:hunter2@relay.example.com",
            "https://alice@relay.example.com",
            "https://:hunter2@relay.example.com:8443",
        ] {
            let err = validate_relay_url(with_creds).unwrap_err().to_string();
            assert!(
                err.contains("credentials"),
                "expected credentials error for '{with_creds}', got: {err}"
            );
            assert!(
                !err.contains("hunter2"),
                "error must not echo password: {err}"
            );
        }

        // Empty, whitespace, query/fragment, or unparseable garbage must be refused.
        for garbage in [
            "",
            "   ",
            "not-a-url",
            "://missing-scheme",
            "https://",
            "https://relay.example.com/path with spaces",
            "https://relay.example.com?token=abc",
            "https://relay.example.com#frag",
        ] {
            assert!(
                validate_relay_url(garbage).is_err(),
                "expected '{garbage}' to be refused"
            );
        }
    }

    #[cfg(feature = "p2p")]
    #[test]
    fn test_resolve_p2p_relay_mode_refuses_minimal_plus_custom_relay_url() {
        let err = resolve_p2p_relay_mode(true, Some("https://relay.example.com"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("minimal") && err.contains("relay"),
            "expected minimal+relay conflict error, got: {err}"
        );
        assert!(p2p_endpoint_builder(true, Some("https://relay.example.com")).is_err());
    }
}
