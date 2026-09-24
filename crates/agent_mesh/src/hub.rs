//! OxideSwarm WebSocket Relay Hub Server
//!
//! Provides the centralized switchboard routing engine, active directory
//! node registry, point-to-point command routing, delivery ACKs/NACKs,
//! and HTTP observability endpoints.

use crate::protocol::{
    AgentMeshEnvelope, NodeDescriptor,
};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, RwLock};
use tower_http::cors::CorsLayer;
use tracing::{error, info, warn};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct HubStats {
    pub routed_commands: usize,
    pub routed_responses: usize,
    pub routed_data: usize,
    pub delivered_acks: usize,
    pub delivered_nacks: usize,
}

pub struct NodeSession {
    pub node_id: String,
    pub platform: String,
    pub hostname: String,
    pub capabilities: Value,
    pub connected_at: u64,
    pub last_heartbeat: u64,
    pub tx: mpsc::UnboundedSender<Message>,
}

pub struct HubState {
    pub nodes: RwLock<HashMap<String, NodeSession>>,
    pub stats: RwLock<HubStats>,
    pub start_time: Instant,
}

impl HubState {
    pub fn new() -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            stats: RwLock::new(HubStats::default()),
            start_time: Instant::now(),
        }
    }

    pub async fn get_catalog(&self) -> Vec<NodeDescriptor> {
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
    }

    pub async fn remove_node(&self, node_id: &str) {
        let mut nodes = self.nodes.write().await;
        if nodes.remove(node_id).is_some() {
            info!("Hub: Node disconnected -> '{}'", node_id);
        }
    }
}

pub struct AgentMeshHub {
    state: Arc<HubState>,
    shutdown_tx: RwLock<Option<oneshot::Sender<()>>>,
}

impl AgentMeshHub {
    pub fn new() -> Self {
        Self {
            state: Arc::new(HubState::new()),
            shutdown_tx: RwLock::new(None),
        }
    }

    pub fn state(&self) -> Arc<HubState> {
        self.state.clone()
    }

    /// Starts the hub listening on the specified address.
    /// Supports ephemeral port binding (e.g. "127.0.0.1:0") and returns the actual bound port.
    pub async fn start(&self, bind_addr: &str) -> Result<u16, Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(bind_addr).await?;
        let local_addr = listener.local_addr()?;
        let port = local_addr.port();
        info!("OxideRelay Hub listening on ws://{}:{}", local_addr.ip(), port);

        let app = Self::build_router(self.state.clone());

        let (tx, rx) = oneshot::channel::<()>();
        *self.shutdown_tx.write().await = Some(tx);

        tokio::spawn(async move {
            let server = axum::serve(listener, app);
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

    pub fn build_router(state: Arc<HubState>) -> Router {
        Router::new()
            .route("/", get(ws_handler))
            .route("/ws", get(ws_handler))
            .route("/api/nodes", get(api_nodes))
            .route("/api/status", get(api_status))
            .route("/api/health", get(api_health))
            .route("/health", get(api_health))
            .layer(CorsLayer::permissive())
            .with_state(state)
    }
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<HubState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<HubState>) {
    let (mut ws_sender, mut ws_receiver) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

    // Outbound write pump
    let write_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if ws_sender.send(msg).await.is_err() {
                break;
            }
        }
    });

    let mut current_node_id: Option<String> = None;

    while let Some(msg_res) = ws_receiver.next().await {
        let msg = match msg_res {
            Ok(m) => m,
            Err(_) => break,
        };

        match msg {
            Message::Text(raw_text) => {
                let envelope = match AgentMeshEnvelope::from_json_str(&raw_text) {
                    Ok(env) => env,
                    Err(e) => {
                        error!("Hub received malformed JSON envelope: {}", e);
                        continue;
                    }
                };

                let corr_id = envelope.correlation_id().to_string();

                match envelope {
                    AgentMeshEnvelope::NodeRegistration {
                        from,
                        platform,
                        hostname,
                        capabilities,
                        ..
                    } => {
                        let node_id = from.clone();
                        current_node_id = Some(node_id.clone());

                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;

                        let session = NodeSession {
                            node_id: node_id.clone(),
                            platform: platform.clone(),
                            hostname,
                            capabilities,
                            connected_at: now,
                            last_heartbeat: now,
                            tx: tx.clone(),
                        };

                        {
                            let mut nodes = state.nodes.write().await;
                            nodes.insert(node_id.clone(), session);
                        }
                        info!("Hub: Node registered -> '{}' ({})", node_id, platform);

                        let ack = AgentMeshEnvelope::NodeRegistrationAck {
                            version: "1.0".to_string(),
                            correlation_id: corr_id,
                            from: "hub".to_string(),
                            to: node_id,
                            status: "accepted".to_string(),
                            assigned_node_id: from,
                            heartbeat_interval_ms: 10000,
                            cluster_id: "oxideswarm-mesh".to_string(),
                            message: Some("Node registered successfully".to_string()),
                            timestamp: now,
                        };

                        if let Ok(ack_json) = ack.to_json_string() {
                            let _ = tx.send(Message::Text(ack_json));
                        }
                    }

                    AgentMeshEnvelope::NodeList { from, .. } => {
                        let catalog = state.get_catalog().await;
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;

                        let res = AgentMeshEnvelope::NodeListResponse {
                            version: "1.0".to_string(),
                            correlation_id: corr_id,
                            from: "hub".to_string(),
                            to: from,
                            nodes: catalog,
                            timestamp: now,
                        };

                        if let Ok(res_json) = res.to_json_string() {
                            let _ = tx.send(Message::Text(res_json));
                        }
                    }

                    AgentMeshEnvelope::Heartbeat { from, .. } => {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;

                        {
                            let mut nodes = state.nodes.write().await;
                            if let Some(session) = nodes.get_mut(&from) {
                                session.last_heartbeat = now;
                            }
                        }

                        let ack = AgentMeshEnvelope::HeartbeatAck {
                            version: "1.0".to_string(),
                            correlation_id: corr_id,
                            from: "hub".to_string(),
                            to: from,
                            timestamp: now,
                        };

                        if let Ok(ack_json) = ack.to_json_string() {
                            let _ = tx.send(Message::Text(ack_json));
                        }
                    }

                    AgentMeshEnvelope::CommandRequest {
                        ref from,
                        ref to,
                        require_ack,
                        ..
                    } => {
                        {
                            let mut stats = state.stats.write().await;
                            stats.routed_commands += 1;
                        }

                        let target_tx = {
                            let nodes = state.nodes.read().await;
                            nodes.get(to).map(|s| s.tx.clone())
                        };

                        if let Some(dest_tx) = target_tx {
                            // Forward command request to target node
                            let _ = dest_tx.send(Message::Text(raw_text.clone()));

                            // Emit DeliveryAck back to sender if requested
                            if require_ack {
                                {
                                    let mut stats = state.stats.write().await;
                                    stats.delivered_acks += 1;
                                }
                                let ack = AgentMeshEnvelope::new_delivery_ack(
                                    "hub",
                                    from,
                                    corr_id,
                                    "target_forwarded",
                                );
                                if let Ok(ack_json) = ack.to_json_string() {
                                    let _ = tx.send(Message::Text(ack_json));
                                }
                            }
                        } else {
                            warn!("Hub: Routing failed - target '{}' not found!", to);
                            {
                                let mut stats = state.stats.write().await;
                                stats.delivered_nacks += 1;
                            }
                            let nack = AgentMeshEnvelope::new_delivery_nack(
                                "hub",
                                from,
                                corr_id,
                                "ERR_NODE_NOT_FOUND",
                                format!("Target node '{}' is not registered with the hub.", to),
                            );
                            if let Ok(nack_json) = nack.to_json_string() {
                                let _ = tx.send(Message::Text(nack_json));
                            }
                        }
                    }

                    AgentMeshEnvelope::CommandResponse { ref to, .. } => {
                        {
                            let mut stats = state.stats.write().await;
                            stats.routed_responses += 1;
                        }

                        let target_tx = {
                            let nodes = state.nodes.read().await;
                            nodes.get(to).map(|s| s.tx.clone())
                        };

                        if let Some(dest_tx) = target_tx {
                            let _ = dest_tx.send(Message::Text(raw_text));
                        } else {
                            warn!("Hub: Command response target '{}' not connected.", to);
                        }
                    }

                    AgentMeshEnvelope::DataPayload { ref from, ref to, .. } => {
                        {
                            let mut stats = state.stats.write().await;
                            stats.routed_data += 1;
                        }

                        let target_tx = {
                            let nodes = state.nodes.read().await;
                            nodes.get(to).map(|s| s.tx.clone())
                        };

                        if let Some(dest_tx) = target_tx {
                            let _ = dest_tx.send(Message::Text(raw_text));
                        } else {
                            warn!("Hub: Data payload target '{}' not found!", to);
                            let nack = AgentMeshEnvelope::new_delivery_nack(
                                "hub",
                                from,
                                corr_id,
                                "ERR_NODE_NOT_FOUND",
                                format!("Data target '{}' not found.", to),
                            );
                            if let Ok(nack_json) = nack.to_json_string() {
                                let _ = tx.send(Message::Text(nack_json));
                            }
                        }
                    }

                    AgentMeshEnvelope::DeliveryAck { ref to, .. }
                    | AgentMeshEnvelope::DeliveryNack { ref to, .. } => {
                        if to != "hub" {
                            let target_tx = {
                                let nodes = state.nodes.read().await;
                                nodes.get(to).map(|s| s.tx.clone())
                            };
                            if let Some(dest_tx) = target_tx {
                                let _ = dest_tx.send(Message::Text(raw_text));
                            }
                        }
                    }

                    AgentMeshEnvelope::Broadcast { ref from, .. } => {
                        let all_txs: Vec<(String, mpsc::UnboundedSender<Message>)> = {
                            let nodes = state.nodes.read().await;
                            nodes
                                .iter()
                                .filter(|(nid, _)| *nid != from)
                                .map(|(nid, s)| (nid.clone(), s.tx.clone()))
                                .collect()
                        };

                        for (_, dest_tx) in all_txs {
                            let _ = dest_tx.send(Message::Text(raw_text.clone()));
                        }
                    }

                    _ => {}
                }
            }
            Message::Close(_) => break,
            Message::Ping(payload) => {
                let _ = tx.send(Message::Pong(payload));
            }
            _ => {}
        }
    }

    if let Some(ref node_id) = current_node_id {
        state.remove_node(node_id).await;
    }

    write_task.abort();
}

async fn api_nodes(State(state): State<Arc<HubState>>) -> impl IntoResponse {
    let catalog = state.get_catalog().await;
    Json(catalog)
}

async fn api_status(State(state): State<Arc<HubState>>) -> impl IntoResponse {
    let stats = state.stats.read().await.clone();
    let node_count = state.nodes.read().await.len();
    let uptime_s = state.start_time.elapsed().as_secs();

    Json(serde_json::json!({
        "status": "online",
        "uptime_seconds": uptime_s,
        "connected_nodes": node_count,
        "stats": stats,
    }))
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
}
