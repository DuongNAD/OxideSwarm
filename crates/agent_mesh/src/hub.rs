//! OxideSwarm WebSocket Relay Hub Server
//!
//! Provides the centralized switchboard routing engine, active directory
//! node registry, point-to-point command routing, delivery ACKs/NACKs,
//! and HTTP observability endpoints.
//!
//! Access control (design "OXS-AUTH v1", section 4):
//! * a socket is anonymous until it sends a valid `NodeRegistration` (carrying the hub token when
//!   one is configured); anything else from an anonymous socket gets a NACK and the socket is
//!   closed, and a socket that never registers is dropped after `registration_timeout`;
//! * WebSocket upgrades that carry a browser `Origin` that is not loopback are refused;
//! * every envelope a socket sends must name one of the node ids registered on *that* socket in
//!   `from`, so a peer can neither impersonate another node nor forge its responses;
//! * a node id can be held by one live connection at a time (a second registration is NACKed
//!   instead of silently taking the routing entry over) and every id of a socket is released when
//!   the socket goes away;
//! * each connection has a bounded outbound queue; a peer that does not drain it is disconnected;
//! * the hub refuses to listen on a non-loopback address without a token unless explicitly told
//!   to (`HubConfig::allow_insecure_bind`, `--insecure`);
//! * `/api/nodes` and `/api/status` need the token when one is configured, and without a token
//!   they answer only requests whose `Host` names a loopback host: a web page on an attacker's
//!   domain that is re-pointed (DNS rebinding) at a loopback hub still sends its own `Host`, so it
//!   cannot read the node catalog. (A token-less hub bound beyond loopback with `--insecure` is
//!   therefore readable by loopback `Host` only; set a token to read it from other machines.)
//!
//! The token travels in clear text inside the WebSocket, so over untrusted networks put the hub
//! behind TLS (e.g. a `wss://` reverse proxy) - this module authenticates peers, it does not
//! encrypt.

use crate::bridge::AgentGridBridge;
use crate::protocol::{
    validate_node_id, AgentMeshEnvelope, NodeDescriptor, ERR_DUPLICATE_NODE_ID,
    ERR_INVALID_NODE_ID, ERR_NODE_UNAVAILABLE, ERR_NOT_REGISTERED, ERR_SENDER_MISMATCH,
    ERR_UNAUTHORIZED, MAX_NODE_ID_LEN,
};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        ConnectInfo, State,
    },
    http::{header, HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use futures::{SinkExt, StreamExt};
use ring::hmac;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Default cap on messages queued for one connection before it is considered a slow consumer.
pub const DEFAULT_MAX_QUEUED_MESSAGES: usize = 1024;
/// Default cap on bytes queued for one connection (see `DEFAULT_MAX_MESSAGE_BYTES`: it must hold
/// at least a couple of maximum-size messages).
pub const DEFAULT_MAX_QUEUED_BYTES: usize = 32 * 1024 * 1024;
/// Default largest WebSocket message the hub accepts (and the Python client's `max_size`).
pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
/// Most node ids one socket may register.
pub const MAX_NODE_IDS_PER_SOCKET: usize = 16;

/// Largest frame accepted from a socket that has not registered yet (a registration is < 4 KiB).
const PRE_REGISTRATION_MAX_BYTES: usize = 64 * 1024;
/// Pause before closing a socket that failed authentication, to blunt brute forcing.
const AUTH_FAILURE_DELAY: Duration = Duration::from_millis(250);
/// How long a deliberate close waits for the queued NACK/close frame to reach the peer.
const CLOSE_FLUSH_TIMEOUT: Duration = Duration::from_secs(2);

/// Hub tuning and access-control settings.
#[derive(Clone)]
pub struct HubConfig {
    /// Shared secret every node must present in its `NodeRegistration`. `None` disables
    /// authentication (only acceptable on loopback; see `allow_insecure_bind`).
    pub auth_token: Option<String>,
    /// Permit listening on a non-loopback address without `auth_token` (`--insecure`).
    pub allow_insecure_bind: bool,
    /// Per-connection outbound queue depth (messages).
    pub max_queued_messages: usize,
    /// Per-connection outbound queue size (bytes).
    pub max_queued_bytes: usize,
    /// Largest inbound WebSocket message/frame.
    pub max_message_bytes: usize,
    /// A socket that has not registered within this long is dropped.
    pub registration_timeout: Duration,
    /// How often the hub pings each connection.
    pub ping_interval: Duration,
    /// A connection that sent nothing (not even a pong) for this long is dropped, which also
    /// frees the node ids a half-open socket would otherwise hold forever.
    pub idle_timeout: Duration,
}

impl Default for HubConfig {
    fn default() -> Self {
        Self {
            auth_token: None,
            allow_insecure_bind: false,
            max_queued_messages: DEFAULT_MAX_QUEUED_MESSAGES,
            max_queued_bytes: DEFAULT_MAX_QUEUED_BYTES,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            registration_timeout: Duration::from_secs(10),
            ping_interval: Duration::from_secs(15),
            idle_timeout: Duration::from_secs(60),
        }
    }
}

impl std::fmt::Debug for HubConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HubConfig")
            .field(
                "auth_token",
                &self.auth_token.as_ref().map(|_| "<redacted>"),
            )
            .field("allow_insecure_bind", &self.allow_insecure_bind)
            .field("max_queued_messages", &self.max_queued_messages)
            .field("max_queued_bytes", &self.max_queued_bytes)
            .field("max_message_bytes", &self.max_message_bytes)
            .field("registration_timeout", &self.registration_timeout)
            .field("ping_interval", &self.ping_interval)
            .field("idle_timeout", &self.idle_timeout)
            .finish()
    }
}

impl HubConfig {
    /// Requires `token` from every node (and from the `/api/nodes` / `/api/status` readers).
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Allows a non-loopback listen address without a token.
    pub fn with_insecure_bind(mut self, allow: bool) -> Self {
        self.allow_insecure_bind = allow;
        self
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct HubStats {
    pub routed_commands: usize,
    pub routed_responses: usize,
    pub routed_data: usize,
    pub delivered_acks: usize,
    pub delivered_nacks: usize,
}

/// Why a message could not be queued for a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushError {
    /// The peer is not draining its queue; it has been scheduled for disconnection.
    Full,
    /// The connection is already gone.
    Closed,
}

/// Bounded handle to one connection's outbound queue. Cloning shares the queue.
#[derive(Clone)]
pub struct Outbox {
    tx: mpsc::Sender<Message>,
    queued_bytes: Arc<AtomicUsize>,
    max_bytes: usize,
    kick: CancellationToken,
}

fn message_weight(msg: &Message) -> usize {
    match msg {
        Message::Text(t) => t.len(),
        Message::Binary(b) | Message::Ping(b) | Message::Pong(b) => b.len(),
        Message::Close(_) => 0,
    }
}

impl Outbox {
    /// Queues `msg` without waiting. A full queue (by message count or by bytes) means the peer
    /// is a slow consumer: the connection is kicked so memory use stays bounded.
    pub fn push(&self, msg: Message) -> Result<(), PushError> {
        if self.is_dead() {
            return Err(PushError::Closed);
        }
        let weight = message_weight(&msg);
        let before = self.queued_bytes.fetch_add(weight, Ordering::AcqRel);
        // A single message is always accepted into an empty queue, however large, so a byte budget
        // below the largest message cannot make a healthy peer undeliverable.
        if before != 0 && before.saturating_add(weight) > self.max_bytes {
            self.queued_bytes.fetch_sub(weight, Ordering::AcqRel);
            self.kick.cancel();
            return Err(PushError::Full);
        }
        match self.tx.try_send(msg) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.queued_bytes.fetch_sub(weight, Ordering::AcqRel);
                self.kick.cancel();
                Err(PushError::Full)
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.queued_bytes.fetch_sub(weight, Ordering::AcqRel);
                Err(PushError::Closed)
            }
        }
    }

    /// True once the connection behind this outbox has ended or is being disconnected.
    pub fn is_dead(&self) -> bool {
        self.tx.is_closed() || self.kick.is_cancelled()
    }
}

pub struct NodeSession {
    pub node_id: String,
    pub platform: String,
    pub hostname: String,
    pub capabilities: Value,
    pub connected_at: u64,
    pub last_heartbeat: u64,
    pub tx: Outbox,
    /// Id of the connection that owns this registration.
    pub conn_id: u64,
}

pub struct HubState {
    pub nodes: RwLock<HashMap<String, NodeSession>>,
    pub stats: RwLock<HubStats>,
    pub start_time: Instant,
    pub bridge: RwLock<Option<Arc<AgentGridBridge>>>,
    pub config: HubConfig,
    next_conn_id: AtomicU64,
}

impl Default for HubState {
    fn default() -> Self {
        Self::new()
    }
}

impl HubState {
    pub fn new() -> Self {
        Self::with_config(HubConfig::default())
    }

    pub fn with_config(config: HubConfig) -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            stats: RwLock::new(HubStats::default()),
            start_time: Instant::now(),
            bridge: RwLock::new(None),
            config,
            next_conn_id: AtomicU64::new(1),
        }
    }

    pub async fn get_catalog(&self) -> Vec<NodeDescriptor> {
        let mut catalog: Vec<NodeDescriptor> = {
            let nodes = self.nodes.read().await;
            nodes
                .values()
                .map(|s| NodeDescriptor {
                    node_id: s.node_id.clone(),
                    platform: s.platform.clone(),
                    hostname: s.hostname.clone(),
                    status: "online".to_string(),
                    capabilities: s.capabilities.clone(),
                    connected_at: s.connected_at,
                })
                .collect()
        };

        if self.bridge.read().await.is_some() {
            catalog.push(NodeDescriptor {
                node_id: "grid".to_string(),
                platform: "grid-supercomputer".to_string(),
                hostname: "rusty-grid-master".to_string(),
                status: "online".to_string(),
                capabilities: serde_json::json!({
                    "supported_commands": [
                        "grid_compute",
                        "grid_compile",
                        "grid_submit",
                        "grid_status",
                        "grid_3d_generate",
                        "grid_auto_rig",
                        "compute",
                        "compile",
                        "status",
                        "submit",
                        "3d_generate",
                        "auto_rig"
                    ],
                    "engine": "OxideSwarm Supercomputing Grid",
                    "layer": 1,
                }),
                connected_at: self.start_time.elapsed().as_secs(),
            });
        }

        catalog
    }

    /// Unconditionally drops the registration of `node_id`. Connections clean up with
    /// `remove_connection` instead, which cannot evict a session another connection took over.
    pub async fn remove_node(&self, node_id: &str) {
        let mut nodes = self.nodes.write().await;
        if nodes.remove(node_id).is_some() {
            info!("Hub: Node disconnected -> '{}'", node_id);
        }
    }

    /// Drops every registration owned by connection `conn_id` (and only those).
    pub async fn remove_connection(&self, conn_id: u64) {
        let mut removed = Vec::new();
        {
            let mut nodes = self.nodes.write().await;
            nodes.retain(|id, s| {
                if s.conn_id == conn_id {
                    removed.push(id.clone());
                    false
                } else {
                    true
                }
            });
        }
        for id in removed {
            info!("Hub: Node disconnected -> '{}'", id);
        }
    }

    /// Looks up the outbound queue of `node_id`.
    async fn outbox_of(&self, node_id: &str) -> Option<Outbox> {
        self.nodes.read().await.get(node_id).map(|s| s.tx.clone())
    }

    /// Removes `node_id` if its connection is already dead (a phantom entry).
    async fn prune_if_dead(&self, node_id: &str) {
        let mut nodes = self.nodes.write().await;
        if nodes.get(node_id).is_some_and(|s| s.tx.is_dead()) {
            nodes.remove(node_id);
            info!("Hub: Pruned dead node entry -> '{}'", node_id);
        }
    }
}

pub struct AgentMeshHub {
    state: Arc<HubState>,
    shutdown_tx: RwLock<Option<oneshot::Sender<()>>>,
}

impl Default for AgentMeshHub {
    fn default() -> Self {
        Self::new()
    }
}

impl AgentMeshHub {
    /// A hub without authentication. `start` only lets it listen on loopback addresses unless
    /// `HubConfig::allow_insecure_bind` is set; use `with_config` to require a token.
    pub fn new() -> Self {
        Self::with_config(HubConfig::default())
    }

    pub fn with_config(config: HubConfig) -> Self {
        Self {
            state: Arc::new(HubState::with_config(config)),
            shutdown_tx: RwLock::new(None),
        }
    }

    pub fn new_with_bridge(bridge: Arc<AgentGridBridge>) -> Self {
        let hub = Self::new();
        if let Ok(mut b) = hub.state.bridge.try_write() {
            *b = Some(bridge);
        }
        hub
    }

    pub async fn attach_bridge(&self, bridge: Arc<AgentGridBridge>) {
        let mut b = self.state.bridge.write().await;
        *b = Some(bridge);
        info!("AgentGridBridge attached to AgentMeshHub");
    }

    pub fn create_router(state: Arc<HubState>) -> Router {
        Self::build_router(state)
    }

    pub fn state(&self) -> Arc<HubState> {
        self.state.clone()
    }

    /// Starts the hub listening on the specified address.
    /// Supports ephemeral port binding (e.g. "127.0.0.1:0") and returns the actual bound port.
    ///
    /// Fails (without serving anything) when the address is not a loopback address and the hub
    /// has neither an auth token nor `allow_insecure_bind`.
    pub async fn start(
        &self,
        bind_addr: &str,
    ) -> Result<u16, Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(bind_addr).await?;
        let local_addr = listener.local_addr()?;
        let port = local_addr.port();

        let config = &self.state.config;
        if !local_addr.ip().is_loopback() && config.auth_token.is_none() {
            if !config.allow_insecure_bind {
                return Err(format!(
                    "refusing to listen on non-loopback address {local_addr} without an auth token: \
                     set --token / OXIDE_MESH_TOKEN, or pass --insecure to accept unauthenticated peers"
                )
                .into());
            }
            warn!(
                "Hub: listening on {} WITHOUT authentication (--insecure): any host that can reach \
                 this port can run commands on every node",
                local_addr
            );
        }

        info!(
            "OxideRelay Hub listening on ws://{}:{} (token auth {})",
            local_addr.ip(),
            port,
            if config.auth_token.is_some() {
                "on"
            } else {
                "off"
            }
        );

        let app = Self::build_router(self.state.clone());

        let (tx, rx) = oneshot::channel::<()>();
        *self.shutdown_tx.write().await = Some(tx);

        tokio::spawn(async move {
            let server = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            );
            if let Err(e) = server
                .with_graceful_shutdown(async move {
                    let _ = rx.await;
                })
                .await
            {
                error!("Hub server error: {}", e);
            }
        });

        Ok(port)
    }

    pub async fn stop(&self) {
        if let Some(tx) = self.shutdown_tx.write().await.take() {
            let _ = tx.send(());
            info!("OxideRelay Hub stopped.");
        }
    }

    /// Builds the router. There is deliberately no CORS layer: browsers on other origins must not
    /// be able to read the REST endpoints (and `ws_handler` refuses their WebSocket upgrades).
    pub fn build_router(state: Arc<HubState>) -> Router {
        Router::new()
            .route("/", get(ws_handler))
            .route("/ws", get(ws_handler))
            .route("/api/nodes", get(api_nodes))
            .route("/api/status", get(api_status))
            .route("/api/health", get(api_health))
            .route("/health", get(api_health))
            .with_state(state)
    }
}

/// Constant-time comparison of two secrets: `b` is MACed under a fixed public key and `a` is
/// checked against that tag with `ring::hmac::verify`, so the comparison always runs over two
/// fixed-length tags and neither the content nor the length of `b` is leaked by an early exit.
fn secrets_equal(a: &str, b: &str) -> bool {
    let key = hmac::Key::new(hmac::HMAC_SHA256, b"oxs1-mesh-token-compare");
    let expected_tag = hmac::sign(&key, b.as_bytes());
    hmac::verify(&key, a.as_bytes(), expected_tag.as_ref()).is_ok()
}

/// True when the `Origin` header value names a loopback host (`localhost`, `127.0.0.0/8`, `::1`).
/// Anything else - another host, `null`, an unparsable value - is treated as a foreign origin.
pub fn origin_is_loopback(origin: &str) -> bool {
    let Some((scheme, rest)) = origin.trim().split_once("://") else {
        return false;
    };
    if !matches!(
        scheme.to_ascii_lowercase().as_str(),
        "http" | "https" | "ws" | "wss"
    ) {
        return false;
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    if authority.is_empty() || authority.contains('@') {
        return false;
    }
    let host = if let Some(bracketed) = authority.strip_prefix('[') {
        match bracketed.split_once(']') {
            Some((h, tail)) if tail.is_empty() || tail.starts_with(':') => h,
            _ => return false,
        }
    } else {
        match authority.rsplit_once(':') {
            Some((h, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => h,
            Some(_) => return false,
            None => authority,
        }
    };

    if host.eq_ignore_ascii_case("localhost") || host.to_ascii_lowercase().ends_with(".localhost") {
        return true;
    }
    match host.parse::<IpAddr>() {
        Ok(IpAddr::V4(v4)) => v4.is_loopback(),
        Ok(IpAddr::V6(v6)) => {
            v6.is_loopback() || v6.to_ipv4_mapped().is_some_and(|v4| v4.is_loopback())
        }
        Err(_) => false,
    }
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<HubState>>,
    headers: HeaderMap,
    peer: Option<ConnectInfo<SocketAddr>>,
) -> Response {
    // Cross-site WebSocket hijacking: a page on any website the owner visits can open a socket to
    // a loopback hub. Browsers always send `Origin`; native clients do not.
    if let Some(origin) = headers.get(header::ORIGIN) {
        let allowed = origin.to_str().is_ok_and(origin_is_loopback);
        if !allowed {
            warn!(
                "Hub: refused WebSocket upgrade from {} with foreign Origin {:?}",
                peer.map(|ConnectInfo(a)| a.to_string())
                    .unwrap_or_else(|| "unknown peer".into()),
                origin
            );
            return (
                StatusCode::FORBIDDEN,
                "cross-origin WebSocket connections are not allowed",
            )
                .into_response();
        }
    }

    let max = state.config.max_message_bytes;
    let peer_addr = peer.map(|ConnectInfo(a)| a);
    ws.max_message_size(max)
        .max_frame_size(max)
        .on_upgrade(move |socket| handle_socket(socket, state, peer_addr))
}

/// What the socket loop should do after a frame was handled.
enum Flow {
    Continue,
    /// Close the connection; `graceful` flushes the queued NACK and a close frame first.
    Close {
        graceful: bool,
    },
}

/// Per-socket routing state.
struct Connection {
    id: u64,
    state: Arc<HubState>,
    outbox: Outbox,
    peer: String,
    /// Node ids registered on this socket, in registration order. Empty = anonymous.
    owned: Vec<String>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Shortens attacker-controlled text and defuses control characters before it goes into a log
/// line or a reason string (no log forging, no terminal escape sequences, no megabyte lines).
fn clip(s: &str) -> String {
    s.chars()
        .take(64)
        .map(|c| if c.is_control() { '\u{fffd}' } else { c })
        .collect()
}

/// The id to put in the `to` of a NACK for a peer that claimed to be `id`: the id itself when it
/// is a plausible one (so the peer's own `to == my id` filter lets the NACK through), a clipped
/// stand-in otherwise.
fn echo_id(id: &str) -> String {
    if id.len() <= MAX_NODE_ID_LEN && !id.chars().any(char::is_control) {
        id.to_string()
    } else {
        clip(id)
    }
}

impl Connection {
    fn owns(&self, id: &str) -> bool {
        self.owned.iter().any(|o| o == id)
    }

    fn queue(&self, env: &AgentMeshEnvelope) {
        if let Ok(json) = env.to_json_string() {
            let _ = self.outbox.push(Message::Text(json));
        }
    }

    fn nack(&self, to: &str, corr_id: &str, code: &str, reason: impl Into<String>) {
        self.queue(&AgentMeshEnvelope::new_delivery_nack(
            "hub", to, corr_id, code, reason,
        ));
    }

    /// Counts a routing NACK and queues it for this socket.
    async fn routing_nack(&self, to: &str, corr_id: &str, code: &str, reason: impl Into<String>) {
        self.state.stats.write().await.delivered_nacks += 1;
        self.nack(to, corr_id, code, reason);
    }

    fn token_ok(&self, presented: Option<&str>) -> bool {
        match (&self.state.config.auth_token, presented) {
            (None, _) => true,
            (Some(expected), Some(given)) => secrets_equal(given, expected),
            (Some(expected), None) => {
                // Still burn the comparison so a missing token is not faster than a wrong one.
                let _ = secrets_equal("", expected);
                false
            }
        }
    }

    async fn handle_text(&mut self, raw_text: String) -> Flow {
        let registered = !self.owned.is_empty();
        if !registered && raw_text.len() > PRE_REGISTRATION_MAX_BYTES {
            warn!(
                "Hub: dropping unregistered peer {}: oversized first frame",
                self.peer
            );
            return Flow::Close { graceful: false };
        }

        let envelope = match AgentMeshEnvelope::from_json_str(&raw_text) {
            Ok(env) => env,
            Err(e) => {
                if !registered {
                    warn!(
                        "Hub: dropping unregistered peer {}: malformed first frame",
                        self.peer
                    );
                    return Flow::Close { graceful: false };
                }
                error!(
                    "Hub received malformed JSON envelope: {}",
                    clip(&e.to_string())
                );
                return Flow::Continue;
            }
        };

        let corr_id = envelope.correlation_id().to_string();

        if matches!(envelope, AgentMeshEnvelope::NodeRegistration { .. }) {
            return self.handle_registration(envelope).await;
        }

        if !registered {
            warn!(
                "Hub: dropping unregistered peer {}: sent {} before registering",
                self.peer,
                envelope.msg_type()
            );
            self.nack(
                &echo_id(envelope.from_node()),
                &corr_id,
                ERR_NOT_REGISTERED,
                "Register with a NodeRegistration envelope (and the hub token, if one is required) \
                 before sending anything else.",
            );
            return Flow::Close { graceful: true };
        }

        // Hub-originated envelope types are never accepted from a peer.
        if matches!(
            envelope,
            AgentMeshEnvelope::NodeRegistrationAck { .. }
                | AgentMeshEnvelope::NodeListResponse { .. }
                | AgentMeshEnvelope::HeartbeatAck { .. }
        ) {
            return Flow::Continue;
        }

        // The sender identity is the registration, not whatever the envelope claims.
        let claimed_from = envelope.from_node().to_string();
        if !self.owns(&claimed_from) {
            warn!(
                "Hub: {} sent {} claiming to be '{}', which this connection did not register",
                self.peer,
                envelope.msg_type(),
                clip(&claimed_from)
            );
            let primary = self.owned[0].clone();
            self.nack(
                &primary,
                &corr_id,
                ERR_SENDER_MISMATCH,
                format!(
                    "Envelope 'from' ('{}') is not a node id registered on this connection.",
                    clip(&claimed_from)
                ),
            );
            return Flow::Continue;
        }

        self.route(envelope, raw_text, corr_id).await;
        Flow::Continue
    }

    async fn handle_registration(&mut self, envelope: AgentMeshEnvelope) -> Flow {
        let AgentMeshEnvelope::NodeRegistration {
            correlation_id,
            from,
            platform,
            hostname,
            capabilities,
            token,
            ..
        } = envelope
        else {
            return Flow::Continue;
        };

        // A second registration on an authenticated socket (one connection serving several ids)
        // rides on the first one's authentication.
        let first = self.owned.is_empty();
        let reply_to = echo_id(&from);

        if first && !self.token_ok(token.as_deref()) {
            let missing = token.is_none();
            warn!(
                "Hub: rejected registration of '{}' from {}: {}",
                reply_to,
                self.peer,
                if missing {
                    "token missing"
                } else {
                    "token mismatch"
                }
            );
            self.nack(
                &reply_to,
                &correlation_id,
                ERR_UNAUTHORIZED,
                if missing {
                    "Authentication required: the hub needs its token (--token or OXIDE_MESH_TOKEN)."
                } else {
                    "Authentication failed."
                },
            );
            tokio::time::sleep(AUTH_FAILURE_DELAY).await;
            return Flow::Close { graceful: true };
        }

        let close_if_first = if first {
            Flow::Close { graceful: true }
        } else {
            Flow::Continue
        };

        if let Err(why) = validate_node_id(&from) {
            warn!("Hub: rejected registration from {}: {}", self.peer, why);
            self.nack(
                &reply_to,
                &correlation_id,
                ERR_INVALID_NODE_ID,
                format!("Invalid node id: {why}."),
            );
            return close_if_first;
        }
        if !self.owns(&from) && self.owned.len() >= MAX_NODE_IDS_PER_SOCKET {
            self.nack(
                &reply_to,
                &correlation_id,
                ERR_INVALID_NODE_ID,
                format!("A connection may register at most {MAX_NODE_IDS_PER_SOCKET} node ids."),
            );
            return close_if_first;
        }

        let now = now_ms();
        let taken = {
            let mut nodes = self.state.nodes.write().await;
            let held_by_live_peer = nodes
                .get(&from)
                .is_some_and(|s| s.conn_id != self.id && !s.tx.is_dead());
            if held_by_live_peer {
                true
            } else {
                // Free, ours already (idempotent re-registration), or left behind by a dead
                // connection whose cleanup has not run yet: take it.
                nodes.insert(
                    from.clone(),
                    NodeSession {
                        node_id: from.clone(),
                        platform: platform.clone(),
                        hostname,
                        capabilities,
                        connected_at: now,
                        last_heartbeat: now,
                        tx: self.outbox.clone(),
                        conn_id: self.id,
                    },
                );
                false
            }
        };

        if taken {
            warn!(
                "Hub: rejected duplicate registration of '{}' from {}",
                reply_to, self.peer
            );
            self.nack(
                &reply_to,
                &correlation_id,
                ERR_DUPLICATE_NODE_ID,
                format!("Node id '{reply_to}' is already registered by another live connection."),
            );
            return close_if_first;
        }

        if !self.owns(&from) {
            self.owned.push(from.clone());
        }
        info!("Hub: Node registered -> '{}' ({})", from, clip(&platform));

        self.queue(&AgentMeshEnvelope::NodeRegistrationAck {
            version: "1.0".to_string(),
            correlation_id,
            from: "hub".to_string(),
            to: from.clone(),
            status: "accepted".to_string(),
            assigned_node_id: from,
            heartbeat_interval_ms: 10000,
            cluster_id: "oxideswarm-mesh".to_string(),
            message: Some("Node registered successfully".to_string()),
            timestamp: now,
        });
        Flow::Continue
    }

    /// Queues `raw_text` for `dest`. A queue that is full disconnects `dest` (slow consumer).
    async fn deliver(&self, dest: &str, raw_text: String) -> Result<(), PushError> {
        let Some(outbox) = self.state.outbox_of(dest).await else {
            return Err(PushError::Closed);
        };
        match outbox.push(Message::Text(raw_text)) {
            Ok(()) => Ok(()),
            Err(PushError::Full) => {
                warn!(
                    "Hub: node '{}' is not draining its queue; disconnecting it",
                    dest
                );
                Err(PushError::Full)
            }
            Err(PushError::Closed) => {
                // Registration left behind by a connection that is already gone.
                self.state.prune_if_dead(dest).await;
                Err(PushError::Closed)
            }
        }
    }

    /// Routes an authenticated envelope whose `from` is one of this socket's node ids.
    async fn route(&self, envelope: AgentMeshEnvelope, raw_text: String, corr_id: String) {
        let state = self.state.clone();
        match envelope {
            AgentMeshEnvelope::NodeList { from, .. } => {
                let catalog = state.get_catalog().await;
                self.queue(&AgentMeshEnvelope::NodeListResponse {
                    version: "1.0".to_string(),
                    correlation_id: corr_id,
                    from: "hub".to_string(),
                    to: from,
                    nodes: catalog,
                    timestamp: now_ms(),
                });
            }

            AgentMeshEnvelope::Heartbeat { from, .. } => {
                let now = now_ms();
                {
                    let mut nodes = state.nodes.write().await;
                    if let Some(session) = nodes.get_mut(&from) {
                        if session.conn_id == self.id {
                            session.last_heartbeat = now;
                        }
                    }
                }
                self.queue(&AgentMeshEnvelope::HeartbeatAck {
                    version: "1.0".to_string(),
                    correlation_id: corr_id,
                    from: "hub".to_string(),
                    to: from,
                    timestamp: now,
                });
            }

            AgentMeshEnvelope::CommandRequest {
                from,
                to,
                command,
                args,
                payload,
                timeout_ms,
                require_ack,
                ..
            } => {
                {
                    let mut stats = state.stats.write().await;
                    stats.routed_commands += 1;
                }

                let is_grid_targeted = to == "grid"
                    || to == "grid-master"
                    || command.starts_with("grid_")
                    || (to == "hub" && (command == "grid_status" || command.starts_with("grid_")));

                if is_grid_targeted {
                    let bridge_opt = {
                        let b = state.bridge.read().await;
                        b.clone()
                    };

                    if let Some(bridge) = bridge_opt {
                        if require_ack {
                            {
                                let mut stats = state.stats.write().await;
                                stats.delivered_acks += 1;
                            }
                            self.queue(&AgentMeshEnvelope::new_delivery_ack(
                                "hub",
                                &from,
                                &corr_id,
                                "target_forwarded",
                            ));
                        }

                        let outbox = self.outbox.clone();
                        let state_clone = state.clone();
                        let from_node = from.clone();
                        let req_envelope = AgentMeshEnvelope::CommandRequest {
                            version: "1.0".to_string(),
                            correlation_id: corr_id.clone(),
                            from: from.clone(),
                            to: to.clone(),
                            command: command.clone(),
                            args,
                            payload,
                            timeout_ms,
                            require_ack,
                            timestamp: now_ms(),
                        };

                        tokio::spawn(async move {
                            let resp = bridge.handle_command(&req_envelope).await;
                            if let Ok(resp_json) = resp.to_json_string() {
                                if outbox.push(Message::Text(resp_json.clone())).is_err() {
                                    // The requesting connection is gone; the node may have
                                    // reconnected, so try its current registration.
                                    let current = state_clone.outbox_of(&from_node).await;
                                    if let Some(current) = current {
                                        let _ = current.push(Message::Text(resp_json));
                                    }
                                }
                            }
                        });
                    } else {
                        warn!("Hub: Grid command received but AgentGridBridge is not configured: '{}'", command);
                        self.routing_nack(
                            &from,
                            &corr_id,
                            "ERR_BRIDGE_NOT_CONFIGURED",
                            "AgentGridBridge is not attached to this hub",
                        )
                        .await;
                    }
                } else {
                    match self.deliver(&to, raw_text).await {
                        Ok(()) => {
                            // Emit DeliveryAck back to sender if requested
                            if require_ack {
                                {
                                    let mut stats = state.stats.write().await;
                                    stats.delivered_acks += 1;
                                }
                                self.queue(&AgentMeshEnvelope::new_delivery_ack(
                                    "hub",
                                    &from,
                                    &corr_id,
                                    "target_forwarded",
                                ));
                            }
                        }
                        Err(PushError::Full) => {
                            self.routing_nack(
                                &from,
                                &corr_id,
                                ERR_NODE_UNAVAILABLE,
                                format!(
                                    "Target node '{}' is not keeping up and was disconnected.",
                                    clip(&to)
                                ),
                            )
                            .await;
                        }
                        Err(PushError::Closed) => {
                            warn!("Hub: Routing failed - target '{}' not found!", clip(&to));
                            self.routing_nack(
                                &from,
                                &corr_id,
                                "ERR_NODE_NOT_FOUND",
                                format!("Target node '{}' is not registered with the hub.", to),
                            )
                            .await;
                        }
                    }
                }
            }

            AgentMeshEnvelope::CommandResponse { ref to, .. } => {
                {
                    let mut stats = state.stats.write().await;
                    stats.routed_responses += 1;
                }

                if self.deliver(to, raw_text).await.is_err() {
                    warn!("Hub: Command response target '{}' not connected.", clip(to));
                }
            }

            AgentMeshEnvelope::DataPayload {
                ref from, ref to, ..
            } => {
                {
                    let mut stats = state.stats.write().await;
                    stats.routed_data += 1;
                }

                match self.deliver(to, raw_text).await {
                    Ok(()) => {}
                    Err(PushError::Full) => {
                        self.nack(
                            from,
                            &corr_id,
                            ERR_NODE_UNAVAILABLE,
                            format!(
                                "Data target '{}' is not keeping up and was disconnected.",
                                clip(to)
                            ),
                        );
                    }
                    Err(PushError::Closed) => {
                        warn!("Hub: Data payload target '{}' not found!", clip(to));
                        self.nack(
                            from,
                            &corr_id,
                            "ERR_NODE_NOT_FOUND",
                            format!("Data target '{}' not found.", to),
                        );
                    }
                }
            }

            AgentMeshEnvelope::DeliveryAck { ref to, .. }
            | AgentMeshEnvelope::DeliveryNack { ref to, .. } => {
                if to != "hub" {
                    let _ = self.deliver(to, raw_text).await;
                }
            }

            AgentMeshEnvelope::Broadcast { ref from, .. } => {
                let all_txs: Vec<(String, Outbox)> = {
                    let nodes = state.nodes.read().await;
                    nodes
                        .iter()
                        .filter(|(nid, _)| *nid != from)
                        .map(|(nid, s)| (nid.clone(), s.tx.clone()))
                        .collect()
                };

                for (nid, dest_tx) in all_txs {
                    if dest_tx.push(Message::Text(raw_text.clone())) == Err(PushError::Full) {
                        warn!(
                            "Hub: node '{}' is not draining its queue; disconnecting it",
                            nid
                        );
                    }
                }
            }

            // Registration is handled before routing; the hub-only types were dropped earlier.
            _ => {}
        }
    }
}

async fn handle_socket(socket: WebSocket, state: Arc<HubState>, peer: Option<SocketAddr>) {
    let (mut ws_sender, mut ws_receiver) = socket.split();
    let config = state.config.clone();

    let (tx, mut rx) = mpsc::channel::<Message>(config.max_queued_messages.max(1));
    let queued_bytes = Arc::new(AtomicUsize::new(0));
    let kick = CancellationToken::new();
    let outbox = Outbox {
        tx,
        queued_bytes: queued_bytes.clone(),
        max_bytes: config.max_queued_bytes,
        kick: kick.clone(),
    };

    // Outbound write pump
    let write_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let weight = message_weight(&msg);
            let is_close = matches!(msg, Message::Close(_));
            let sent = ws_sender.send(msg).await;
            queued_bytes.fetch_sub(weight, Ordering::AcqRel);
            if sent.is_err() || is_close {
                break;
            }
        }
    });

    let mut conn = Connection {
        id: state.next_conn_id.fetch_add(1, Ordering::Relaxed),
        state: state.clone(),
        outbox: outbox.clone(),
        peer: peer
            .map(|p| p.to_string())
            .unwrap_or_else(|| "unknown peer".to_string()),
        owned: Vec::new(),
    };

    let mut last_rx = Instant::now();
    let period = config.ping_interval.max(Duration::from_millis(10));
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let registration_deadline = tokio::time::sleep(config.registration_timeout);
    tokio::pin!(registration_deadline);
    let mut graceful = false;

    loop {
        tokio::select! {
            biased;

            _ = kick.cancelled() => {
                warn!("Hub: disconnecting {} (outbound queue overflow)", conn.peer);
                break;
            }

            _ = &mut registration_deadline, if conn.owned.is_empty() => {
                warn!("Hub: dropping {}: no registration within {:?}", conn.peer, config.registration_timeout);
                break;
            }

            _ = ticker.tick() => {
                if last_rx.elapsed() > config.idle_timeout {
                    warn!(
                        "Hub: dropping {} ({:?}): silent for more than {:?}",
                        conn.peer, conn.owned, config.idle_timeout
                    );
                    break;
                }
                let _ = outbox.push(Message::Ping(Vec::new()));
            }

            frame = ws_receiver.next() => {
                let msg = match frame {
                    Some(Ok(m)) => m,
                    _ => break,
                };
                last_rx = Instant::now();

                match msg {
                    Message::Text(raw_text) => {
                        if let Flow::Close { graceful: g } = conn.handle_text(raw_text).await {
                            graceful = g;
                            break;
                        }
                    }
                    Message::Close(_) => break,
                    Message::Ping(payload) => {
                        let _ = outbox.push(Message::Pong(payload));
                    }
                    _ => {}
                }
            }
        }
    }

    // Release every id this socket held - and nothing a newer connection has since taken over.
    state.remove_connection(conn.id).await;

    if graceful {
        let _ = outbox.push(Message::Close(None));
        let mut write_task = write_task;
        if tokio::time::timeout(CLOSE_FLUSH_TIMEOUT, &mut write_task)
            .await
            .is_err()
        {
            write_task.abort();
        }
    } else {
        write_task.abort();
    }
}

/// `Authorization: Bearer <token>` check for the read-only REST endpoints.
fn rest_authorized(state: &HubState, headers: &HeaderMap) -> bool {
    let Some(expected) = state.config.auth_token.as_deref() else {
        return true;
    };
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .is_some_and(|given| secrets_equal(given, expected))
}

/// The host the request was addressed to: the `Host` header, or the authority of an absolute-form
/// request URI (HTTP/2 has no `Host` header).
fn request_host(headers: &HeaderMap, uri: &Uri) -> Option<String> {
    headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .or_else(|| uri.authority().map(|a| a.to_string()))
}

/// DNS-rebinding guard for the read-only REST endpoints of a hub without a token: the request
/// must have been addressed to a loopback host. Missing or unparsable `Host` fails closed.
fn rest_host_allowed(state: &HubState, headers: &HeaderMap, uri: &Uri) -> bool {
    if state.config.auth_token.is_some() {
        return true;
    }
    request_host(headers, uri).is_some_and(|host| origin_is_loopback(&format!("http://{host}")))
}

fn forbidden_host() -> Response {
    (
        StatusCode::FORBIDDEN,
        "this hub has no token, so it only answers requests addressed to a loopback host \
         (localhost, 127.0.0.1, [::1]); use one of those, or start the hub with --token",
    )
        .into_response()
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
        "hub token required",
    )
        .into_response()
}

async fn api_nodes(State(state): State<Arc<HubState>>, headers: HeaderMap, uri: Uri) -> Response {
    if !rest_authorized(&state, &headers) {
        return unauthorized();
    }
    if !rest_host_allowed(&state, &headers, &uri) {
        return forbidden_host();
    }
    let catalog = state.get_catalog().await;
    Json(catalog).into_response()
}

async fn api_status(State(state): State<Arc<HubState>>, headers: HeaderMap, uri: Uri) -> Response {
    if !rest_authorized(&state, &headers) {
        return unauthorized();
    }
    if !rest_host_allowed(&state, &headers, &uri) {
        return forbidden_host();
    }
    let stats = state.stats.read().await.clone();
    let node_count = state.nodes.read().await.len();
    let uptime_s = state.start_time.elapsed().as_secs();

    Json(serde_json::json!({
        "status": "online",
        "uptime_seconds": uptime_s,
        "connected_nodes": node_count,
        "stats": stats,
    }))
    .into_response()
}

async fn api_health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_hub_lifecycle() {
        let hub = AgentMeshHub::new();
        let port = hub.start("127.0.0.1:0").await.expect("Failed to start hub");
        assert!(port > 0);

        let catalog = hub.state().get_catalog().await;
        assert_eq!(catalog.len(), 0);

        hub.stop().await;
    }

    #[test]
    fn test_origin_is_loopback_table() {
        for ok in [
            "http://localhost",
            "http://localhost:8080",
            "HTTP://LOCALHOST:8080",
            "https://127.0.0.1",
            "http://127.0.0.1:3000",
            "http://127.1.2.3",
            "http://[::1]",
            "http://[::1]:8088",
            "http://[::ffff:127.0.0.1]:80",
            "http://app.localhost:5173",
            "ws://127.0.0.1:8088",
        ] {
            assert!(origin_is_loopback(ok), "{ok} must count as loopback");
        }
        for bad in [
            "",
            "null",
            "https://evil.example",
            "http://evil.example:8080",
            "http://localhost.evil.example",
            "http://127.0.0.1.evil.example",
            "http://localhost@evil.example",
            "http://evil.example@127.0.0.1",
            "http://192.168.1.10:8080",
            "http://0.0.0.0:8080",
            "http://[::]:8080",
            "file://localhost/etc/passwd",
            "chrome-extension://abcdef",
            "http://127.0.0.1:notaport",
            "localhost",
        ] {
            assert!(
                !origin_is_loopback(bad),
                "{bad:?} must be treated as foreign"
            );
        }
    }

    #[test]
    fn test_secrets_equal() {
        assert!(secrets_equal("abc", "abc"));
        assert!(!secrets_equal("abc", "abd"));
        assert!(!secrets_equal("abc", "abcd"));
        assert!(!secrets_equal("", "abc"));
    }

    fn test_outbox(max_messages: usize, max_bytes: usize) -> (Outbox, mpsc::Receiver<Message>) {
        let (tx, rx) = mpsc::channel(max_messages);
        (
            Outbox {
                tx,
                queued_bytes: Arc::new(AtomicUsize::new(0)),
                max_bytes,
                kick: CancellationToken::new(),
            },
            rx,
        )
    }

    #[test]
    fn test_outbox_overflow_by_count_kicks_the_connection() {
        let (outbox, _rx) = test_outbox(2, 1 << 20);
        assert_eq!(outbox.push(Message::Text("a".into())), Ok(()));
        assert_eq!(outbox.push(Message::Text("b".into())), Ok(()));
        assert!(!outbox.is_dead());
        assert_eq!(outbox.push(Message::Text("c".into())), Err(PushError::Full));
        assert!(
            outbox.is_dead(),
            "a full queue must schedule the slow consumer for disconnection"
        );
        assert_eq!(
            outbox.push(Message::Text("d".into())),
            Err(PushError::Closed)
        );
    }

    #[test]
    fn test_outbox_overflow_by_bytes_kicks_the_connection() {
        let (outbox, _rx) = test_outbox(1000, 10);
        assert_eq!(outbox.push(Message::Text("123456".into())), Ok(()));
        assert_eq!(
            outbox.push(Message::Text("123456".into())),
            Err(PushError::Full)
        );
        assert!(outbox.is_dead());
    }

    #[test]
    fn test_outbox_accepts_one_oversized_message_into_an_empty_queue() {
        let (outbox, mut rx) = test_outbox(4, 10);
        assert_eq!(outbox.push(Message::Text("x".repeat(100))), Ok(()));
        assert!(!outbox.is_dead());
        // ...but nothing more until it has been drained.
        assert_eq!(outbox.push(Message::Text("y".into())), Err(PushError::Full));
        assert!(rx.try_recv().is_ok());
    }

    #[test]
    fn test_outbox_reports_a_dropped_receiver_as_closed() {
        let (outbox, rx) = test_outbox(4, 1 << 20);
        drop(rx);
        assert!(outbox.is_dead());
        assert_eq!(
            outbox.push(Message::Text("x".into())),
            Err(PushError::Closed)
        );
    }

    #[tokio::test]
    async fn test_non_loopback_bind_requires_a_token_or_insecure_flag() {
        let hub = AgentMeshHub::new();
        let err = hub
            .start("0.0.0.0:0")
            .await
            .expect_err("0.0.0.0 without a token must be refused");
        let msg = err.to_string();
        assert!(
            msg.contains("--token") && msg.contains("--insecure"),
            "unhelpful message: {msg}"
        );

        let with_token = AgentMeshHub::with_config(HubConfig::default().with_auth_token("t0ken"));
        with_token
            .start("0.0.0.0:0")
            .await
            .expect("a token makes a public bind acceptable");
        with_token.stop().await;

        let insecure = AgentMeshHub::with_config(HubConfig::default().with_insecure_bind(true));
        insecure
            .start("0.0.0.0:0")
            .await
            .expect("--insecure allows a public bind");
        insecure.stop().await;
    }

    /// One raw HTTP/1.1 GET, so the `Host` header can be anything (HTTP clients derive it from the
    /// URL). Returns the status code.
    async fn raw_get(
        port: u16,
        path: &str,
        host: Option<&str>,
        authorization: Option<&str>,
    ) -> u16 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut request = match host {
            Some(host) => format!("GET {path} HTTP/1.1\r\nHost: {host}\r\n"),
            None => format!("GET {path} HTTP/1.0\r\n"),
        };
        if let Some(auth) = authorization {
            request.push_str(&format!("Authorization: {auth}\r\n"));
        }
        request.push_str("Connection: close\r\n\r\n");

        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
            .split_whitespace()
            .nth(1)
            .and_then(|code| code.parse().ok())
            .unwrap_or_else(|| panic!("no status line in {response:?}"))
    }

    #[tokio::test]
    async fn test_rest_endpoints_of_a_tokenless_hub_only_answer_loopback_hosts() {
        let hub = AgentMeshHub::new();
        let port = hub.start("127.0.0.1:0").await.expect("start hub");

        for path in ["/api/nodes", "/api/status"] {
            for host in [
                format!("127.0.0.1:{port}"),
                format!("localhost:{port}"),
                format!("[::1]:{port}"),
                "127.0.0.1".to_string(),
                format!("hub.localhost:{port}"),
            ] {
                assert_eq!(
                    raw_get(port, path, Some(&host), None).await,
                    200,
                    "{path} with Host {host}"
                );
            }
            // DNS rebinding: the page's own domain resolves to 127.0.0.1, but Host stays its own.
            for host in [
                "evil.example",
                "evil.example:8088",
                "127.0.0.1.evil.example",
                "localhost.evil.example",
                "192.168.1.5:8088",
                "0.0.0.0:8088",
                "localhost@evil.example",
                "",
            ] {
                assert_eq!(
                    raw_get(port, path, Some(host), None).await,
                    403,
                    "{path} with Host {host:?}"
                );
            }
            // A request that names no host cannot be shown to be loopback.
            assert_eq!(
                raw_get(port, path, None, None).await,
                403,
                "{path} without Host"
            );
        }

        // The health probe returns nothing about the hub and stays reachable.
        assert_eq!(
            raw_get(port, "/api/health", Some("evil.example"), None).await,
            200
        );

        hub.stop().await;
    }

    #[tokio::test]
    async fn test_rest_endpoints_of_a_hub_with_a_token_are_gated_by_the_token_not_the_host() {
        let hub = AgentMeshHub::with_config(HubConfig::default().with_auth_token("t0ken"));
        let port = hub.start("127.0.0.1:0").await.expect("start hub");

        for path in ["/api/nodes", "/api/status"] {
            assert_eq!(raw_get(port, path, Some("evil.example"), None).await, 401);
            assert_eq!(
                raw_get(port, path, Some("127.0.0.1"), Some("Bearer wrong")).await,
                401
            );
            // Who holds the token may read it from any address the hub is reachable at.
            assert_eq!(
                raw_get(port, path, Some("hub.lan:8088"), Some("Bearer t0ken")).await,
                200
            );
            assert_eq!(
                raw_get(port, path, Some("127.0.0.1"), Some("Bearer t0ken")).await,
                200
            );
        }

        hub.stop().await;
    }

    #[test]
    fn test_hub_config_debug_does_not_leak_the_token() {
        let cfg = HubConfig::default().with_auth_token("super-secret-value");
        let shown = format!("{cfg:?}");
        assert!(!shown.contains("super-secret-value"), "{shown}");
    }
}
