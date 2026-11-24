//! Remote Master [`GridBackend`] implementation (`--grid-connect`).
//!
//! Connects directly over `rusty_grid_core` transport, discovery, and OXS-AUTH v2/v3 primitives
//! (mirroring `crates/cli/src/connect.rs` and `McpState` without creating a Cargo dependency cycle
//! with `rusty_grid_cli`).

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::info;

use rusty_grid_core::cluster_auth::{client_handshake, ClusterSecret};
use rusty_grid_core::discovery::{
    discover_master_with_secret_and_rendezvous, parse_rendezvous_url, DEFAULT_DISCOVERY_PORT,
    RENDEZVOUS_ENV,
};
use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::noise_auth::{v3_client_handshake, HandshakePurpose, NodeKeypair, PublicKey};
use rusty_grid_core::placement::{tag_value, STATE_TAG_PREFIX};
use rusty_grid_core::protocol::{
    ClientMessage, ClientResponse, EndpointRole, MessageTransport, ProtocolError, WireCodec,
    PROTOCOL_VERSION_CURRENT,
};
use rusty_grid_core::task::{Task, TaskId, TaskResult, TaskStatus};
use rusty_grid_core::transport::{
    configure_tcp_stream, connect_p2p_client_stream_with_relay, drain_pending_p2p_client_closes,
    parse_p2p_ticket, resolve_p2p_relay_mode, GridStream,
};

use crate::grid_backend::{
    BoxGridFuture, GridBackend, GridClusterStatus, GridTaskResult, GridTaskStatus,
};

/// Default TCP port of an OxideSwarm Master's grid listener.
pub const DEFAULT_MASTER_PORT: u16 = 8088;
/// How long a client waits for the TCP/P2P connection to the Master.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a client waits for cluster authentication and version check.
pub const AUTH_TIMEOUT: Duration = Duration::from_secs(10);
/// How long LAN auto-discovery (`auto`) waits for a beacon.
pub const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(3);
/// Default timeout for bounded short RPCs and `/api/nodes` health checks.
pub const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(2);
/// Polling interval for `wait_task` against a remote Master.
const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(150);

/// Configuration for [`RemoteMasterBackend`].
#[derive(Clone)]
pub struct RemoteMasterConfig {
    pub master_addr: String,
    pub secret: Option<ClusterSecret>,
    pub v3_auth: Option<(NodeKeypair, PublicKey)>,
    pub relay_url: Option<String>,
    pub insecure: bool,
    pub rpc_timeout: Duration,
}

impl std::fmt::Debug for RemoteMasterConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteMasterConfig")
            .field("master_addr", &self.master_addr)
            .field("secret", &self.secret.as_ref().map(|_| "<redacted>"))
            .field(
                "v3_auth",
                &self
                    .v3_auth
                    .as_ref()
                    .map(|(_, master_pub)| master_pub.fingerprint()),
            )
            .field("relay_url", &self.relay_url)
            .field("insecure", &self.insecure)
            .field("rpc_timeout", &self.rpc_timeout)
            .finish()
    }
}

impl RemoteMasterConfig {
    /// Creates a config targeting `master_addr` with default 2-second short-RPC timeout.
    pub fn new(master_addr: impl Into<String>) -> Self {
        Self {
            master_addr: master_addr.into(),
            secret: None,
            v3_auth: None,
            relay_url: None,
            insecure: false,
            rpc_timeout: DEFAULT_RPC_TIMEOUT,
        }
    }

    /// Validates configuration invariants and fail-closed credential requirements.
    pub fn validate(&self) -> GridResult<()> {
        let trimmed = self.master_addr.trim();
        if trimmed.is_empty() {
            return Err(GridError::Config(
                "--grid-connect / OXIDE_GRID_CONNECT cannot be empty".into(),
            ));
        }
        resolve_client_wire_codec()?;
        let minimal = resolve_p2p_minimal();
        resolve_p2p_relay_mode(minimal, self.relay_url.as_deref())?;
        if is_auto(trimmed) {
            let _ = resolve_rendezvous_url()?;
        }

        if master_requires_secret(trimmed, is_p2p_ticket(trimmed))
            && self.secret.is_none()
            && self.v3_auth.is_none()
            && !self.insecure
        {
            return Err(GridError::AuthRequired(format!(
                "connecting to Master at '{trimmed}' requires a cluster secret \
                 (--secret / --secret-file / OXIDESWARM_SECRET / OXIDESWARM_SECRET_FILE / \
                 ~/.oxideswarm/cluster.secret) or --master-key / RUSTY_GRID_MASTER_KEY; \
                 pass --insecure to connect without authentication"
            )));
        }

        Ok(())
    }
}

fn is_auto(addr: &str) -> bool {
    addr.eq_ignore_ascii_case("auto") || addr.to_ascii_lowercase().starts_with("auto:")
}

/// Returns `true` when `addr` is an iroh P2P ticket.
pub fn is_p2p_ticket(addr: &str) -> bool {
    let trimmed = addr.trim();
    trimmed.starts_with('{')
        || (trimmed.starts_with("node1")
            && trimmed.len() > 32
            && !trimmed.contains(':')
            && !trimmed.contains('.'))
        || (!trimmed.is_empty() && parse_p2p_ticket(trimmed).is_ok())
}

/// Completes a Master target address, appending [`DEFAULT_MASTER_PORT`] (`8088`) to bare hosts/IPs.
pub fn normalize_master_addr(addr: &str) -> String {
    let addr = addr.trim();
    if addr.is_empty() || is_auto(addr) || is_p2p_ticket(addr) || addr.parse::<SocketAddr>().is_ok()
    {
        return addr.to_string();
    }
    if let Ok(ip) = addr.parse::<IpAddr>() {
        return SocketAddr::new(ip, DEFAULT_MASTER_PORT).to_string();
    }
    if addr.starts_with('[') && addr.ends_with(']') {
        return format!("{addr}:{DEFAULT_MASTER_PORT}");
    }
    if addr.contains(':') {
        return addr.to_string();
    }
    format!("{addr}:{DEFAULT_MASTER_PORT}")
}

/// Whether connecting to `master_addr` requires a cluster credential unless `--insecure` is set.
pub fn master_requires_secret(master_addr: &str, via_p2p_ticket: bool) -> bool {
    if via_p2p_ticket || is_p2p_ticket(master_addr) {
        return true;
    }
    let addr = normalize_master_addr(master_addr);
    if addr.is_empty() || is_auto(&addr) {
        return true;
    }
    if let Ok(socket) = addr.parse::<SocketAddr>() {
        return !socket.ip().is_loopback();
    }
    let host = addr.rsplit_once(':').map_or(addr.as_str(), |(h, _)| h);
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
        .trim_end_matches('.');
    if let Ok(ip) = host.parse::<IpAddr>() {
        return !ip.is_loopback();
    }
    let host = host.to_ascii_lowercase();
    !(host == "localhost" || host.ends_with(".localhost"))
}

fn resolve_p2p_minimal() -> bool {
    let env_enabled = std::env::var("RUSTY_GRID_P2P_MINIMAL")
        .ok()
        .map(|v| {
            let t = v.trim();
            t == "1" || t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("yes")
        })
        .unwrap_or(false);
    cfg!(test) || env_enabled
}

fn resolve_client_wire_codec() -> GridResult<WireCodec> {
    match std::env::var("RUSTY_GRID_WIRE_CODEC")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        Some(raw) => raw
            .parse::<WireCodec>()
            .map_err(|e| GridError::Config(format!("RUSTY_GRID_WIRE_CODEC: {e}"))),
        None => Ok(WireCodec::default()),
    }
}

fn resolve_rendezvous_url() -> GridResult<Option<String>> {
    let candidate = std::env::var(RENDEZVOUS_ENV)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            std::env::var("RUSTY_GRID_RENDEZVOUS")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        });
    if let Some(ref url) = candidate {
        parse_rendezvous_url(url).map_err(GridError::Config)?;
    }
    Ok(candidate)
}

async fn resolve_master_target(
    addr: &str,
    secret: Option<&ClusterSecret>,
) -> GridResult<(String, Option<String>)> {
    let addr = addr.trim();
    if is_p2p_ticket(addr) {
        return Ok((addr.to_string(), None));
    }
    if !is_auto(addr) {
        return Ok((normalize_master_addr(addr), None));
    }

    let rendezvous_url = resolve_rendezvous_url()?;
    let disc_port = addr
        .split_once(':')
        .and_then(|(_, port)| port.parse::<u16>().ok())
        .unwrap_or(DEFAULT_DISCOVERY_PORT);
    info!(
        port = disc_port,
        authenticated = secret.is_some(),
        "Master address set to 'auto'; performing LAN UDP discovery probe..."
    );
    match discover_master_with_secret_and_rendezvous(
        DISCOVERY_TIMEOUT,
        disc_port,
        secret,
        rendezvous_url.as_deref(),
    )
    .await
    {
        Some(beacon) => {
            info!(
                master = %beacon.cluster_addr,
                hostname = %beacon.hostname,
                "Discovered active Master via discovery"
            );
            Ok((beacon.cluster_addr, beacon.p2p_ticket))
        }
        None => Err(GridError::ConnectionFailed(format!(
            "Auto-discovery found no Master on UDP port {disc_port}{}; start a Master or pass \
             --grid-connect HOST:{DEFAULT_MASTER_PORT}",
            if secret.is_some() {
                " (only Masters holding the same cluster secret are accepted)"
            } else {
                ""
            }
        ))),
    }
}

async fn connect_p2p_ticket_stream(
    ticket: &str,
    connect_timeout: Duration,
    minimal: bool,
    relay_url: Option<&str>,
) -> GridResult<(String, GridStream, bool)> {
    let endpoint_addr = parse_p2p_ticket(ticket)?;
    let label = format!("iroh://{}", endpoint_addr.id);
    let stream =
        connect_p2p_client_stream_with_relay(endpoint_addr, connect_timeout, minimal, relay_url)
            .await
            .map_err(|e| match e {
                GridError::Timeout { duration_secs, .. } => GridError::ConnectionFailed(format!(
                    "Failed to connect to Master at {label}: timed out after {duration_secs}s"
                )),
                GridError::ConnectionFailed(detail) => GridError::ConnectionFailed(format!(
                    "Failed to connect to Master at {label}: {detail}"
                )),
                other => other,
            })?;
    Ok((label, stream, true))
}

fn map_protocol_error(err: ProtocolError, timeout: Duration) -> GridError {
    match err {
        ProtocolError::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::TimedOut => {
            GridError::Timeout {
                operation: "master_rpc".into(),
                duration_secs: timeout.as_secs(),
            }
        }
        other => GridError::from(other),
    }
}

/// [`GridBackend`] connected over TCP/P2P + OXS-AUTH v2/v3 to a separately running Master.
pub struct RemoteMasterBackend {
    config: RemoteMasterConfig,
    resolved_target: Mutex<Option<(String, Option<String>)>>,
    transport: Mutex<Option<MessageTransport<GridStream>>>,
}

impl RemoteMasterBackend {
    /// Validates `config` and constructs a [`RemoteMasterBackend`] (connections are established
    /// lazily or via [`GridBackend::health_check`]).
    pub fn new(config: RemoteMasterConfig) -> GridResult<Self> {
        config.validate()?;
        Ok(Self {
            config,
            resolved_target: Mutex::new(None),
            transport: Mutex::new(None),
        })
    }

    /// Returns the configured Master target string.
    pub fn master_addr(&self) -> &str {
        &self.config.master_addr
    }

    async fn resolve_target_cached(&self) -> GridResult<(String, Option<String>, bool)> {
        if let Some(cached) = self.resolved_target.lock().await.clone() {
            return Ok((cached.0, cached.1, true));
        }
        let resolved =
            resolve_master_target(&self.config.master_addr, self.config.secret.as_ref()).await?;
        *self.resolved_target.lock().await = Some(resolved.clone());
        Ok((resolved.0, resolved.1, false))
    }

    async fn invalidate_resolved_target(&self) {
        *self.resolved_target.lock().await = None;
    }

    async fn open_and_auth_stream(
        &self,
        resolved_addr: &str,
        discovered_ticket: Option<&str>,
        connect_timeout: Duration,
        auth_timeout: Duration,
    ) -> GridResult<MessageTransport<GridStream>> {
        let codec = resolve_client_wire_codec()?;
        let minimal = resolve_p2p_minimal();
        resolve_p2p_relay_mode(minimal, self.config.relay_url.as_deref())?;

        let (label, stream, via_p2p) = if is_p2p_ticket(resolved_addr) {
            connect_p2p_ticket_stream(
                resolved_addr,
                connect_timeout,
                minimal,
                self.config.relay_url.as_deref(),
            )
            .await?
        } else {
            let stream = match tokio::time::timeout(
                connect_timeout,
                TcpStream::connect(resolved_addr),
            )
            .await
            {
                Ok(Ok(stream)) => stream,
                Ok(Err(e)) => {
                    if let Some(ticket) = discovered_ticket.filter(|t| !t.is_empty()) {
                        return self
                            .finish_p2p_handshake(ticket, connect_timeout, auth_timeout, codec)
                            .await;
                    }
                    return Err(GridError::ConnectionFailed(format!(
                        "Failed to connect to Master at {resolved_addr}: {e}"
                    )));
                }
                Err(_) => {
                    if let Some(ticket) = discovered_ticket.filter(|t| !t.is_empty()) {
                        return self
                            .finish_p2p_handshake(ticket, connect_timeout, auth_timeout, codec)
                            .await;
                    }
                    return Err(GridError::ConnectionFailed(format!(
                        "Failed to connect to Master at {resolved_addr}: timed out after {}s",
                        connect_timeout.as_secs()
                    )));
                }
            };
            let _ = configure_tcp_stream(&stream);
            (resolved_addr.to_string(), GridStream::Tcp(stream), false)
        };

        self.authenticate_and_probe(stream, &label, via_p2p, codec, auth_timeout)
            .await
    }

    async fn finish_p2p_handshake(
        &self,
        ticket: &str,
        connect_timeout: Duration,
        auth_timeout: Duration,
        codec: WireCodec,
    ) -> GridResult<MessageTransport<GridStream>> {
        let minimal = resolve_p2p_minimal();
        let (label, stream, via_p2p) = connect_p2p_ticket_stream(
            ticket,
            connect_timeout,
            minimal,
            self.config.relay_url.as_deref(),
        )
        .await?;
        self.authenticate_and_probe(stream, &label, via_p2p, codec, auth_timeout)
            .await
    }

    async fn authenticate_and_probe(
        &self,
        stream: GridStream,
        label: &str,
        via_p2p: bool,
        codec: WireCodec,
        auth_timeout: Duration,
    ) -> GridResult<MessageTransport<GridStream>> {
        let upgrade_hint = |detail: &str| {
            GridError::ConnectionFailed(format!(
                "Failed to connect to Master at {label}: the reply to the protocol version check \
                 could not be read ({detail}). If the Master is running an older or newer build, \
                 upgrade both Master and agent-mesh to protocol version {PROTOCOL_VERSION_CURRENT}"
            ))
        };

        let mut transport = MessageTransport::with_codec(stream, codec);
        if let Some((ref node_key, ref master_pub)) = self.config.v3_auth {
            let hs_res = tokio::time::timeout(
                auth_timeout,
                v3_client_handshake(
                    &mut transport,
                    node_key,
                    master_pub,
                    HandshakePurpose::Member,
                ),
            )
            .await
            .map_err(|_| GridError::Timeout {
                operation: "cluster_authentication".into(),
                duration_secs: auth_timeout.as_secs(),
            })?;
            let session_key = match hs_res {
                Ok(key) => key,
                Err(GridError::HandshakeFailed(detail)) if via_p2p => {
                    return Err(upgrade_hint(&detail));
                }
                Err(GridError::ConnectionClosed) if via_p2p => {
                    return Err(upgrade_hint("connection closed by peer"));
                }
                Err(GridError::Io(e)) if via_p2p => {
                    return Err(upgrade_hint(&e.to_string()));
                }
                Err(other) => return Err(other),
            };
            transport.enable_encryption(&session_key, EndpointRole::Client);
        } else if let Some(ref secret) = self.config.secret {
            let hs_res =
                tokio::time::timeout(auth_timeout, client_handshake(&mut transport, secret))
                    .await
                    .map_err(|_| GridError::Timeout {
                        operation: "cluster_authentication".into(),
                        duration_secs: auth_timeout.as_secs(),
                    })?;
            match hs_res {
                Ok(session_key) => {
                    transport.enable_encryption(&session_key, EndpointRole::Client);
                }
                Err(GridError::HandshakeFailed(detail)) if via_p2p => {
                    return Err(upgrade_hint(&detail));
                }
                Err(GridError::ConnectionClosed) if via_p2p => {
                    return Err(upgrade_hint("connection closed by peer"));
                }
                Err(GridError::Io(e)) if via_p2p => {
                    return Err(upgrade_hint(&e.to_string()));
                }
                Err(other) => return Err(other),
            }
        }

        let probe_future = async {
            transport.send_msg(&ClientMessage::Hello).await?;
            transport.recv_msg::<ClientResponse>().await
        };
        let probe_reply = tokio::time::timeout(auth_timeout, probe_future)
            .await
            .map_err(|_| GridError::Timeout {
                operation: "protocol_version_check".into(),
                duration_secs: auth_timeout.as_secs(),
            })?
            .map_err(|e| match GridError::from(e) {
                GridError::Framing(detail) => upgrade_hint(&detail),
                GridError::ConnectionClosed if via_p2p => upgrade_hint("connection closed by peer"),
                GridError::Io(io_err) if via_p2p => upgrade_hint(&io_err.to_string()),
                other => other,
            })?;

        match probe_reply {
            Some(ClientResponse::Hello { .. }) => Ok(transport),
            Some(ClientResponse::Error { message }) => {
                if message.contains("Incompatible protocol version") {
                    Err(GridError::IncompatibleVersion(message))
                } else {
                    Err(GridError::ConnectionFailed(message))
                }
            }
            Some(other) => Err(GridError::UnexpectedMessage {
                expected: "Hello".into(),
                actual: format!("{other:?}"),
            }),
            None if via_p2p => Err(upgrade_hint("connection closed by peer")),
            None => Err(GridError::ConnectionClosed),
        }
    }

    async fn connect_fresh(
        &self,
        timeout_override: Option<Duration>,
    ) -> GridResult<MessageTransport<GridStream>> {
        let connect_timeout = timeout_override.map_or(CONNECT_TIMEOUT, |d| d.min(CONNECT_TIMEOUT));
        let auth_timeout = timeout_override.map_or(AUTH_TIMEOUT, |d| d.min(AUTH_TIMEOUT));
        let (addr, ticket, was_cached) = self.resolve_target_cached().await?;
        match self
            .open_and_auth_stream(&addr, ticket.as_deref(), connect_timeout, auth_timeout)
            .await
        {
            Ok(transport) => Ok(transport),
            Err(err) => {
                let is_connect_err = matches!(
                    err,
                    GridError::ConnectionFailed(_)
                        | GridError::ConnectionClosed
                        | GridError::Timeout { .. }
                        | GridError::Io(_)
                );
                if is_connect_err {
                    self.invalidate_resolved_target().await;
                    if was_cached && is_auto(self.config.master_addr.trim()) {
                        if let Ok((fresh_addr, fresh_ticket, _)) =
                            self.resolve_target_cached().await
                        {
                            if fresh_addr != addr {
                                if let Ok(t) = self
                                    .open_and_auth_stream(
                                        &fresh_addr,
                                        fresh_ticket.as_deref(),
                                        connect_timeout,
                                        auth_timeout,
                                    )
                                    .await
                                {
                                    return Ok(t);
                                }
                            }
                        }
                    }
                }
                Err(err)
            }
        }
    }

    async fn request_short(
        &self,
        message: &ClientMessage,
        is_idempotent: bool,
        timeout: Duration,
    ) -> GridResult<(ClientResponse, bool)> {
        let mut retried_after_pooled_write = false;
        let pooled = self.transport.lock().await.take();

        if let Some(mut transport) = pooled {
            if transport
                .send_msg_with_idle_timeout(message, timeout)
                .await
                .is_ok()
            {
                match transport
                    .recv_msg_with_idle_timeout::<ClientResponse>(timeout)
                    .await
                {
                    Ok(Some(resp)) => {
                        let mut guard = self.transport.lock().await;
                        if guard.is_none() {
                            *guard = Some(transport);
                        } else {
                            drop(guard);
                            transport.close().await;
                        }
                        return Ok((resp, false));
                    }
                    Ok(None) if !is_idempotent => {
                        transport.close().await;
                        return Err(GridError::ConnectionClosed);
                    }
                    Err(e) if !is_idempotent => {
                        transport.close().await;
                        return Err(map_protocol_error(e, timeout));
                    }
                    Ok(None) | Err(_) => {
                        transport.close().await;
                        retried_after_pooled_write = true;
                    }
                }
            } else {
                transport.close().await;
            }
        }

        let mut transport = self.connect_fresh(Some(timeout)).await?;
        if let Err(e) = transport.send_msg_with_idle_timeout(message, timeout).await {
            transport.close().await;
            return Err(map_protocol_error(e, timeout));
        }
        let resp = match transport
            .recv_msg_with_idle_timeout::<ClientResponse>(timeout)
            .await
        {
            Ok(Some(resp)) => resp,
            Ok(None) => {
                transport.close().await;
                return Err(GridError::ConnectionClosed);
            }
            Err(e) => {
                transport.close().await;
                return Err(map_protocol_error(e, timeout));
            }
        };

        let mut guard = self.transport.lock().await;
        if guard.is_none() {
            *guard = Some(transport);
        } else {
            drop(guard);
            transport.close().await;
        }
        Ok((resp, retried_after_pooled_write))
    }
}

fn map_error_response(task_id: Option<TaskId>, message: String) -> GridError {
    if let Some(tid) = task_id {
        if message.to_ascii_lowercase().contains("not found") {
            return GridError::TaskNotFound(tid.0);
        }
    }
    GridError::Config(message)
}

fn synthesized_terminal_result(
    task_id: TaskId,
    status: TaskStatus,
    assigned_worker: Option<uuid::Uuid>,
    error: Option<String>,
) -> TaskResult {
    let exit_code = match status {
        TaskStatus::Cancelled => 130,
        TaskStatus::TimedOut => 124,
        _ => 1,
    };
    TaskResult::failure(
        assigned_worker.unwrap_or_else(uuid::Uuid::nil),
        task_id,
        exit_code,
        "",
        "",
        0,
        error.or_else(|| Some("Task terminated".into())),
    )
}

impl GridBackend for RemoteMasterBackend {
    fn submit_task(&self, task: Task, _priority: u32) -> BoxGridFuture<'_, TaskId> {
        Box::pin(async move {
            let (resp, _) = self
                .request_short(
                    &ClientMessage::SubmitTask { task, wait: false },
                    false,
                    self.config.rpc_timeout,
                )
                .await?;
            match resp {
                ClientResponse::TaskSubmitted { task_id } => Ok(task_id),
                ClientResponse::Error { message } => Err(map_error_response(None, message)),
                other => Err(GridError::UnexpectedMessage {
                    expected: "TaskSubmitted".into(),
                    actual: format!("{other:?}"),
                }),
            }
        })
    }

    fn submit_and_wait(
        &self,
        task: Task,
        _priority: u32,
        timeout: Duration,
    ) -> BoxGridFuture<'_, TaskResult> {
        Box::pin(async move {
            let conn_budget = timeout.max(Duration::from_secs(1));
            let mut transport = self.connect_fresh(Some(conn_budget)).await?;
            let send_res = transport
                .send_msg_with_idle_timeout(
                    &ClientMessage::SubmitTask { task, wait: true },
                    self.config.rpc_timeout.min(conn_budget),
                )
                .await;
            if let Err(e) = send_res {
                transport.close().await;
                return Err(map_protocol_error(e, timeout));
            }
            let recv_res = transport
                .recv_msg_with_idle_timeout::<ClientResponse>(timeout)
                .await;
            transport.close().await;
            match recv_res {
                Ok(Some(ClientResponse::TaskCompleted { result, .. })) => Ok(result),
                Ok(Some(ClientResponse::Error { message })) => {
                    Err(map_error_response(None, message))
                }
                Ok(Some(other)) => Err(GridError::UnexpectedMessage {
                    expected: "TaskCompleted".into(),
                    actual: format!("{other:?}"),
                }),
                Ok(None) => Err(GridError::ConnectionClosed),
                Err(e) => Err(map_protocol_error(e, timeout)),
            }
        })
    }

    fn get_task_status(&self, task_id: TaskId) -> BoxGridFuture<'_, GridTaskStatus> {
        Box::pin(async move {
            let (resp, _) = self
                .request_short(
                    &ClientMessage::GetTaskStatus { task_id },
                    true,
                    self.config.rpc_timeout,
                )
                .await?;
            match resp {
                ClientResponse::TaskStatusInfo {
                    task_id,
                    status,
                    state_name,
                    assigned_worker,
                    error,
                } => Ok(GridTaskStatus {
                    task_id,
                    status,
                    state_name,
                    assigned_worker,
                    error,
                    task_info: None,
                    checkpoint: None,
                }),
                ClientResponse::Error { message } => {
                    Err(map_error_response(Some(task_id), message))
                }
                other => Err(GridError::UnexpectedMessage {
                    expected: "TaskStatusInfo".into(),
                    actual: format!("{other:?}"),
                }),
            }
        })
    }

    fn get_task_result(&self, task_id: TaskId) -> BoxGridFuture<'_, GridTaskResult> {
        Box::pin(async move {
            let (resp, _) = self
                .request_short(
                    &ClientMessage::GetTaskResult { task_id },
                    true,
                    self.config.rpc_timeout,
                )
                .await?;
            match resp {
                ClientResponse::TaskCompleted { result, .. } => {
                    Ok(GridTaskResult::Completed(result))
                }
                ClientResponse::TaskStatusInfo {
                    task_id,
                    status,
                    state_name,
                    assigned_worker,
                    error,
                } => Ok(GridTaskResult::Pending(GridTaskStatus {
                    task_id,
                    status,
                    state_name,
                    assigned_worker,
                    error,
                    task_info: None,
                    checkpoint: None,
                })),
                ClientResponse::Error { message } => {
                    Err(map_error_response(Some(task_id), message))
                }
                other => Err(GridError::UnexpectedMessage {
                    expected: "TaskCompleted or TaskStatusInfo".into(),
                    actual: format!("{other:?}"),
                }),
            }
        })
    }

    fn wait_task(&self, task_id: TaskId, timeout: Duration) -> BoxGridFuture<'_, TaskResult> {
        Box::pin(async move {
            let deadline = tokio::time::Instant::now() + timeout;
            let conn_budget = timeout.max(Duration::from_secs(1));
            let mut transport = self.connect_fresh(Some(conn_budget)).await?;

            loop {
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    transport.close().await;
                    return Err(GridError::Timeout {
                        operation: format!("wait_task({task_id})"),
                        duration_secs: timeout.as_secs(),
                    });
                }
                let remaining = deadline.saturating_duration_since(now);
                let rpc_budget = remaining.min(self.config.rpc_timeout.max(Duration::from_secs(1)));

                if let Err(e) = transport
                    .send_msg_with_idle_timeout(
                        &ClientMessage::GetTaskResult { task_id },
                        rpc_budget,
                    )
                    .await
                {
                    transport.close().await;
                    return Err(map_protocol_error(e, remaining));
                }

                let recv = transport
                    .recv_msg_with_idle_timeout::<ClientResponse>(rpc_budget)
                    .await;
                match recv {
                    Ok(Some(ClientResponse::TaskCompleted { result, .. })) => {
                        transport.close().await;
                        return Ok(result);
                    }
                    Ok(Some(ClientResponse::TaskStatusInfo {
                        task_id: tid,
                        status,
                        assigned_worker,
                        error,
                        ..
                    })) if status.is_terminal() => {
                        transport.close().await;
                        return Ok(synthesized_terminal_result(
                            tid,
                            status,
                            assigned_worker,
                            error,
                        ));
                    }
                    Ok(Some(ClientResponse::TaskStatusInfo { .. })) => {
                        let now = tokio::time::Instant::now();
                        if now >= deadline {
                            transport.close().await;
                            return Err(GridError::Timeout {
                                operation: format!("wait_task({task_id})"),
                                duration_secs: timeout.as_secs(),
                            });
                        }
                        let sleep_until = (now + WAIT_POLL_INTERVAL).min(deadline);
                        tokio::time::sleep_until(sleep_until).await;
                    }
                    Ok(Some(ClientResponse::Error { message })) => {
                        transport.close().await;
                        return Err(map_error_response(Some(task_id), message));
                    }
                    Ok(Some(other)) => {
                        transport.close().await;
                        return Err(GridError::UnexpectedMessage {
                            expected: "TaskCompleted or TaskStatusInfo".into(),
                            actual: format!("{other:?}"),
                        });
                    }
                    Ok(None) => {
                        transport.close().await;
                        return Err(GridError::ConnectionClosed);
                    }
                    Err(e) => {
                        transport.close().await;
                        return Err(map_protocol_error(e, remaining));
                    }
                }
            }
        })
    }

    fn cancel_task(&self, task_id: TaskId) -> BoxGridFuture<'_, bool> {
        Box::pin(async move {
            let (resp, retried) = self
                .request_short(
                    &ClientMessage::CancelTask { task_id },
                    true,
                    self.config.rpc_timeout,
                )
                .await?;
            match resp {
                ClientResponse::TaskCancelled { success, .. } => {
                    if !success && retried {
                        if let Ok((ClientResponse::TaskStatusInfo { status, .. }, _)) = self
                            .request_short(
                                &ClientMessage::GetTaskStatus { task_id },
                                true,
                                Duration::from_millis(500),
                            )
                            .await
                        {
                            if matches!(status, TaskStatus::Cancelled) {
                                return Ok(true);
                            }
                        }
                    }
                    Ok(success)
                }
                ClientResponse::Error { message } => {
                    Err(map_error_response(Some(task_id), message))
                }
                other => Err(GridError::UnexpectedMessage {
                    expected: "TaskCancelled".into(),
                    actual: format!("{other:?}"),
                }),
            }
        })
    }

    fn cluster_status(&self) -> BoxGridFuture<'_, GridClusterStatus> {
        Box::pin(async move {
            let (resp, _) = self
                .request_short(&ClientMessage::ClusterStatus, true, self.config.rpc_timeout)
                .await?;
            match resp {
                ClientResponse::ClusterStatus {
                    total_tasks,
                    pending_tasks,
                    running_tasks,
                    completed_tasks,
                    failed_tasks,
                    workers,
                    active_bytes,
                    max_active_bytes,
                    store_error,
                    cancelled_tasks,
                } => {
                    let active_workers = workers
                        .iter()
                        .filter(|w| tag_value(&w.tags, STATE_TAG_PREFIX) == Some("connected"))
                        .count();
                    let queue_json = serde_json::json!({
                        "total": total_tasks,
                        "queued": pending_tasks,
                        "running": running_tasks,
                        "completed": completed_tasks,
                        "failed": failed_tasks,
                        "cancelled": cancelled_tasks,
                        "active_bytes": active_bytes,
                        "max_active_bytes": max_active_bytes,
                        "store_error": store_error,
                    });
                    let workers_json = serde_json::to_value(&workers)?;
                    Ok(GridClusterStatus {
                        active_workers,
                        total_workers: workers.len(),
                        queued_tasks: pending_tasks,
                        running_tasks,
                        completed_tasks,
                        failed_tasks,
                        queue_json,
                        workers_json,
                    })
                }
                ClientResponse::Error { message } => Err(map_error_response(None, message)),
                other => Err(GridError::UnexpectedMessage {
                    expected: "ClusterStatus".into(),
                    actual: format!("{other:?}"),
                }),
            }
        })
    }

    fn list_workers(&self) -> BoxGridFuture<'_, serde_json::Value> {
        Box::pin(async move {
            let (resp, _) = self
                .request_short(&ClientMessage::ListWorkers, true, self.config.rpc_timeout)
                .await?;
            match resp {
                ClientResponse::WorkerList { workers } => Ok(serde_json::to_value(&workers)?),
                ClientResponse::Error { message } => Err(map_error_response(None, message)),
                other => Err(GridError::UnexpectedMessage {
                    expected: "WorkerList".into(),
                    actual: format!("{other:?}"),
                }),
            }
        })
    }

    fn health_check(&self) -> BoxGridFuture<'_, ()> {
        Box::pin(async move {
            let (resp, _) = self
                .request_short(&ClientMessage::Hello, true, self.config.rpc_timeout)
                .await?;
            match resp {
                ClientResponse::Hello { .. } => Ok(()),
                ClientResponse::Error { message } => Err(map_error_response(None, message)),
                other => Err(GridError::UnexpectedMessage {
                    expected: "Hello".into(),
                    actual: format!("{other:?}"),
                }),
            }
        })
    }

    fn close(&self) -> futures::future::BoxFuture<'_, ()> {
        Box::pin(async move {
            let pooled = self.transport.lock().await.take();
            if let Some(mut transport) = pooled {
                transport.close().await;
            }
            drain_pending_p2p_client_closes().await;
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn master_requires_secret_enforces_loopback_vs_remote() {
        assert!(!master_requires_secret("127.0.0.1:8088", false));
        assert!(!master_requires_secret("127.0.0.1", false));
        assert!(!master_requires_secret("localhost:8088", false));
        assert!(!master_requires_secret("[::1]:8088", false));
        assert!(master_requires_secret("192.168.1.10:8088", false));
        assert!(master_requires_secret("auto", false));
        assert!(master_requires_secret("auto:9999", false));
        assert!(master_requires_secret("127.0.0.1:8088", true));
    }

    #[test]
    fn normalize_master_addr_appends_default_port() {
        assert_eq!(normalize_master_addr("127.0.0.1"), "127.0.0.1:8088");
        assert_eq!(normalize_master_addr("10.0.0.5:9000"), "10.0.0.5:9000");
        assert_eq!(normalize_master_addr("auto"), "auto");
        assert_eq!(normalize_master_addr("myhost"), "myhost:8088");
        assert_eq!(normalize_master_addr("[::1]"), "[::1]:8088");
    }

    #[test]
    fn remote_config_validate_rejects_remote_without_credentials() {
        let cfg = RemoteMasterConfig::new("10.0.0.5:8088");
        let err = cfg
            .validate()
            .expect_err("non-loopback target without secret or --insecure must fail");
        assert!(matches!(err, GridError::AuthRequired(_)));

        let mut insecure_cfg = RemoteMasterConfig::new("10.0.0.5:8088");
        insecure_cfg.insecure = true;
        assert!(insecure_cfg.validate().is_ok());

        let loopback_cfg = RemoteMasterConfig::new("127.0.0.1:8088");
        assert!(loopback_cfg.validate().is_ok());

        let cfg_192 = RemoteMasterConfig::new("192.0.2.1:8088");
        assert!(matches!(
            cfg_192.validate(),
            Err(GridError::AuthRequired(_))
        ));

        let cfg_auto = RemoteMasterConfig::new("auto");
        assert!(matches!(
            cfg_auto.validate(),
            Err(GridError::AuthRequired(_))
        ));

        let empty_cfg = RemoteMasterConfig::new("   ");
        assert!(matches!(empty_cfg.validate(), Err(GridError::Config(_))));
    }

    #[tokio::test]
    async fn wait_task_stops_immediately_on_terminal_task_status_info() {
        use rusty_grid_core::protocol::PROTOCOL_VERSION_MIN_COMPATIBLE;
        use tokio::net::TcpListener;
        use uuid::Uuid;

        let worker_id = Uuid::new_v4();
        let cases = [
            (
                TaskId::new(),
                TaskStatus::Cancelled,
                "Cancelled",
                Some(worker_id),
                Some("Cancelled by user".to_string()),
                130,
                "Cancelled by user",
            ),
            (
                TaskId::new(),
                TaskStatus::Failed,
                "Failed",
                None,
                None,
                1,
                "Task terminated",
            ),
            (
                TaskId::new(),
                TaskStatus::TimedOut,
                "TimedOut",
                Some(worker_id),
                Some("Timed out".to_string()),
                124,
                "Timed out",
            ),
            (
                TaskId::new(),
                TaskStatus::Completed,
                "Completed",
                Some(worker_id),
                None,
                1,
                "Task terminated",
            ),
        ];

        for (
            expected_id,
            status,
            state_name,
            assigned_worker,
            error_msg,
            expected_exit,
            expected_err,
        ) in cases
        {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind mock master");
            let addr = listener.local_addr().unwrap();
            let state_name = state_name.to_string();
            let error_msg_srv = error_msg.clone();

            let srv = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.expect("accept client");
                let mut transport =
                    MessageTransport::with_codec(GridStream::Tcp(stream), WireCodec::Bincode);
                loop {
                    match transport.recv_msg::<ClientMessage>().await {
                        Ok(Some(ClientMessage::Hello)) => {
                            transport
                                .send_msg(&ClientResponse::Hello {
                                    min_compatible_version: PROTOCOL_VERSION_MIN_COMPATIBLE,
                                })
                                .await
                                .expect("send Hello");
                        }
                        Ok(Some(ClientMessage::GetTaskResult { task_id })) => {
                            transport
                                .send_msg(&ClientResponse::TaskStatusInfo {
                                    task_id,
                                    status,
                                    state_name: state_name.clone(),
                                    assigned_worker,
                                    error: error_msg_srv.clone(),
                                })
                                .await
                                .expect("send TaskStatusInfo");
                            break;
                        }
                        Ok(None) => break,
                        Ok(Some(other)) => panic!("unexpected client msg: {other:?}"),
                        Err(e) => panic!("mock master recv error: {e}"),
                    }
                }
                transport.close().await;
            });

            let backend = RemoteMasterBackend::new(RemoteMasterConfig::new(addr.to_string()))
                .expect("construct RemoteMasterBackend");
            let started = std::time::Instant::now();
            let result = backend
                .wait_task(expected_id, Duration::from_secs(10))
                .await
                .expect("terminal TaskStatusInfo must resolve immediately instead of timing out");
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "wait_task spun instead of returning immediately for {status:?}"
            );
            assert_eq!(result.task_id, expected_id);
            assert_eq!(result.worker_id, assigned_worker.unwrap_or_else(Uuid::nil));
            assert_eq!(result.exit_code, expected_exit);
            assert_eq!(result.error.as_deref(), Some(expected_err));

            srv.await.expect("mock server task");
            backend.close().await;
        }
    }
}
