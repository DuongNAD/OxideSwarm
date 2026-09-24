//! Native Rust WebSocket Agent Mesh Client
//!
//! Provides connection management, node registration, periodic heartbeats,
//! remote command execution, data transmission, and request/response correlation.

use crate::executor::CommandExecutor;
use crate::protocol::{
    verify_sha256, AgentMeshEnvelope, CorrelationId, NodeDescriptor,
};
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMessage};
use tracing::{info, warn};

pub struct AgentMeshClient {
    hub_url: String,
    node_id: String,
    platform: String,
    hostname: String,
    tx: Arc<RwLock<Option<mpsc::UnboundedSender<WsMessage>>>>,
    pending: Arc<RwLock<HashMap<CorrelationId, oneshot::Sender<AgentMeshEnvelope>>>>,
    received_payloads: Arc<RwLock<Vec<AgentMeshEnvelope>>>,
    executed_count: Arc<AtomicUsize>,
    running: Arc<AtomicBool>,
}

impl AgentMeshClient {
    pub fn new(
        hub_url: impl Into<String>,
        node_id: impl Into<String>,
        platform: impl Into<String>,
    ) -> Self {
        let hostname = sysinfo::System::host_name().unwrap_or_else(|| "unknown-host".to_string());
        Self {
            hub_url: hub_url.into(),
            node_id: node_id.into(),
            platform: platform.into(),
            hostname,
            tx: Arc::new(RwLock::new(None)),
            pending: Arc::new(RwLock::new(HashMap::new())),
            received_payloads: Arc::new(RwLock::new(Vec::new())),
            executed_count: Arc::new(AtomicUsize::new(0)),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn platform(&self) -> &str {
        &self.platform
    }

    pub fn executed_commands_count(&self) -> usize {
        self.executed_count.load(Ordering::SeqCst)
    }

    pub async fn received_data_payloads(&self) -> Vec<AgentMeshEnvelope> {
        self.received_payloads.read().await.clone()
    }

    /// Establishes the WebSocket connection to the Hub and spawns the read/write message pump.
    pub async fn connect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Connecting to OxideRelay Hub at {}...", self.hub_url);
        let (ws_stream, _) = connect_async(&self.hub_url).await?;
        let (mut ws_write, mut ws_read) = ws_stream.split();

        let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<WsMessage>();
        *self.tx.write().await = Some(outbound_tx.clone());
        self.running.store(true, Ordering::SeqCst);

        // Spawn outbound writer task
        let running_clone = self.running.clone();
        tokio::spawn(async move {
            while let Some(msg) = outbound_rx.recv().await {
                if ws_write.send(msg).await.is_err() {
                    break;
                }
            }
            running_clone.store(false, Ordering::SeqCst);
        });

        // Send registration message
        let reg = AgentMeshEnvelope::NodeRegistration {
            version: "1.0".to_string(),
            correlation_id: uuid::Uuid::new_v4().to_string(),
            from: self.node_id.clone(),
            to: "hub".to_string(),
            platform: self.platform.clone(),
            hostname: self.hostname.clone(),
            capabilities: serde_json::json!({
                "os": self.platform,
                "arch": std::env::consts::ARCH,
                "supported_commands": ["echo", "ping", "system_info", "shell_exec", "sha256_verify"],
            }),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };

        if let Ok(reg_json) = reg.to_json_string() {
            let _ = outbound_tx.send(WsMessage::Text(reg_json));
        }

        // Spawn inbound reader task
        let client_node_id = self.node_id.clone();
        let pending_map = self.pending.clone();
        let received_list = self.received_payloads.clone();
        let exec_counter = self.executed_count.clone();
        let out_tx = outbound_tx.clone();
        let running_flag = self.running.clone();

        tokio::spawn(async move {
            while let Some(msg_res) = ws_read.next().await {
                let msg = match msg_res {
                    Ok(m) => m,
                    Err(e) => {
                        warn!("WebSocket read error: {}", e);
                        break;
                    }
                };

                match msg {
                    WsMessage::Text(raw_text) => {
                        let envelope = match AgentMeshEnvelope::from_json_str(&raw_text) {
                            Ok(env) => env,
                            Err(e) => {
                                warn!("Failed to parse incoming envelope: {}", e);
                                continue;
                            }
                        };

                        let target = envelope.to_node();
                        if target != client_node_id && target != "all" && target != "hub" {
                            // Message targeted to another node; ignore to prevent cross-talk
                            continue;
                        }

                        let corr_id = envelope.correlation_id().to_string();

                        match envelope {
                            AgentMeshEnvelope::CommandRequest {
                                ref from,
                                ref command,
                                ref args,
                                timeout_ms,
                                ..
                            } => {
                                exec_counter.fetch_add(1, Ordering::SeqCst);
                                let sender = from.clone();
                                let cmd = command.clone();
                                let cmd_args = args.clone();
                                let node_id = client_node_id.clone();
                                let reply_tx = out_tx.clone();
                                let request_corr_id = corr_id.clone();

                                tokio::spawn(async move {
                                    info!("[{}] Executing command '{}' from '{}'", node_id, cmd, sender);
                                    let exec_res = CommandExecutor::execute(&cmd, &cmd_args, timeout_ms).await;

                                    let resp = AgentMeshEnvelope::new_command_response(
                                        node_id,
                                        sender,
                                        request_corr_id,
                                        Some(cmd),
                                        exec_res.status,
                                        exec_res.exit_code,
                                        exec_res.stdout,
                                        exec_res.stderr,
                                        exec_res.duration_ms,
                                    );

                                    if let Ok(json_str) = resp.to_json_string() {
                                        let _ = reply_tx.send(WsMessage::Text(json_str));
                                    }
                                });
                            }

                            AgentMeshEnvelope::DataPayload {
                                ref from,
                                ref data,
                                ref checksum_sha256,
                                ..
                            } => {
                                received_list.write().await.push(envelope.clone());

                                // Check payload integrity
                                let valid = if let (Some(d), Some(hash)) = (data, checksum_sha256) {
                                    let bytes: Vec<u8> = d.chars().map(|c| c as u8).collect();
                                    verify_sha256(&bytes, hash)
                                } else {
                                    true
                                };

                                let reply_envelope = if valid {
                                    AgentMeshEnvelope::new_delivery_ack(
                                        client_node_id.clone(),
                                        from,
                                        corr_id.clone(),
                                        "data_received_verified",
                                    )
                                } else {
                                    AgentMeshEnvelope::new_delivery_nack(
                                        client_node_id.clone(),
                                        from,
                                        corr_id.clone(),
                                        "ERR_PAYLOAD_CORRUPT",
                                        "SHA-256 checksum mismatch",
                                    )
                                };

                                if let Ok(json_str) = reply_envelope.to_json_string() {
                                    let _ = out_tx.send(WsMessage::Text(json_str));
                                }

                                // Resolve any local pending future matching correlation ID
                                if let Some(sender_channel) = pending_map.write().await.remove(&corr_id) {
                                    let _ = sender_channel.send(envelope);
                                }
                            }

                            AgentMeshEnvelope::DeliveryAck { ref status, .. } => {
                                if status != "target_forwarded" {
                                    if let Some(sender_channel) = pending_map.write().await.remove(&corr_id) {
                                        let _ = sender_channel.send(envelope);
                                    }
                                }
                            }

                            AgentMeshEnvelope::CommandResponse { .. }
                            | AgentMeshEnvelope::DeliveryNack { .. }
                            | AgentMeshEnvelope::NodeListResponse { .. }
                            | AgentMeshEnvelope::NodeRegistrationAck { .. } => {
                                if let Some(sender_channel) = pending_map.write().await.remove(&corr_id) {
                                    let _ = sender_channel.send(envelope);
                                }
                            }

                            _ => {}
                        }
                    }
                    WsMessage::Ping(p) => {
                        let _ = out_tx.send(WsMessage::Pong(p));
                    }
                    WsMessage::Close(_) => break,
                    _ => {}
                }
            }
            running_flag.store(false, Ordering::SeqCst);
        });

        Ok(())
    }

    /// Sends a structured command to a target node and awaits the result.
    pub async fn send_command(
        &self,
        target_id: &str,
        command: &str,
        args: Value,
        timeout_ms: Option<u64>,
    ) -> Result<AgentMeshEnvelope, Box<dyn std::error::Error + Send + Sync>> {
        let corr_id = uuid::Uuid::new_v4().to_string();
        let (rx_tx, rx_fut) = oneshot::channel::<AgentMeshEnvelope>();
        self.pending.write().await.insert(corr_id.clone(), rx_tx);

        let timeout_val = timeout_ms.unwrap_or(15000);
        let req = AgentMeshEnvelope::CommandRequest {
            version: "1.0".to_string(),
            correlation_id: corr_id.clone(),
            from: self.node_id.clone(),
            to: target_id.to_string(),
            command: command.to_string(),
            args,
            payload: None,
            timeout_ms: Some(timeout_val),
            require_ack: true,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };

        self.send_envelope(req).await?;

        // Await response or timeout
        let res = tokio::time::timeout(Duration::from_millis(timeout_val + 2000), rx_fut).await??;
        Ok(res)
    }

    /// Transmits a data payload (with SHA-256 verification) to a target node.
    pub async fn send_data_payload(
        &self,
        target_id: &str,
        data_bytes: &[u8],
    ) -> Result<(String, String), Box<dyn std::error::Error + Send + Sync>> {
        let (env, hash) = AgentMeshEnvelope::new_data_payload(&self.node_id, target_id, data_bytes);
        let corr_id = env.correlation_id().to_string();
        self.send_envelope(env).await?;
        Ok((corr_id, hash))
    }

    /// Queries the Hub for the list of online nodes.
    pub async fn query_node_list(
        &self,
        timeout_ms: Option<u64>,
    ) -> Result<Vec<NodeDescriptor>, Box<dyn std::error::Error + Send + Sync>> {
        let corr_id = uuid::Uuid::new_v4().to_string();
        let (rx_tx, rx_fut) = oneshot::channel::<AgentMeshEnvelope>();
        self.pending.write().await.insert(corr_id.clone(), rx_tx);

        let req = AgentMeshEnvelope::NodeList {
            version: "1.0".to_string(),
            correlation_id: corr_id,
            from: self.node_id.clone(),
            to: "hub".to_string(),
            filter_platform: None,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };

        self.send_envelope(req).await?;

        let timeout_val = timeout_ms.unwrap_or(5000);
        let res = tokio::time::timeout(Duration::from_millis(timeout_val), rx_fut).await??;

        if let AgentMeshEnvelope::NodeListResponse { nodes, .. } = res {
            Ok(nodes)
        } else {
            Err("Expected NodeListResponse envelope".into())
        }
    }

    /// Submits a GPU/matrix compute job to the OxideSwarm Grid via the Agent-Grid Bridge.
    pub async fn submit_grid_compute(
        &self,
        kernel_name: &str,
        matrix_dim: u32,
        timeout_ms: Option<u64>,
    ) -> Result<AgentMeshEnvelope, Box<dyn std::error::Error + Send + Sync>> {
        let args = serde_json::json!({
            "kernel_name": kernel_name,
            "kernel": kernel_name,
            "matrix_dim": matrix_dim,
            "simulated_matrix_dim": matrix_dim,
        });
        self.send_command("grid", "grid_compute", args, timeout_ms).await
    }

    /// Submits a distributed Rust compilation job to the OxideSwarm Grid via the Agent-Grid Bridge.
    pub async fn submit_grid_compilation<I>(
        &self,
        crate_name: &str,
        source_files: I,
        compiler_flags: Vec<String>,
        timeout_ms: Option<u64>,
    ) -> Result<AgentMeshEnvelope, Box<dyn std::error::Error + Send + Sync>>
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let files: HashMap<String, String> = source_files.into_iter().collect();
        let args = serde_json::json!({
            "crate_name": crate_name,
            "source_files": files,
            "compiler_flags": compiler_flags,
        });
        self.send_command("grid", "grid_compile", args, timeout_ms.or(Some(30000))).await
    }

    /// Convenience alias for `submit_grid_compilation`.
    pub async fn submit_grid_compile(
        &self,
        crate_name: &str,
        files: HashMap<String, String>,
        flags: Vec<String>,
        timeout_ms: Option<u64>,
    ) -> Result<AgentMeshEnvelope, Box<dyn std::error::Error + Send + Sync>> {
        self.submit_grid_compilation(crate_name, files, flags, timeout_ms).await
    }

    /// Queries cluster status and scheduler queue metrics from the Master Grid scheduler.
    pub async fn query_grid_status(
        &self,
        timeout_ms: Option<u64>,
    ) -> Result<AgentMeshEnvelope, Box<dyn std::error::Error + Send + Sync>> {
        self.send_command("grid", "grid_status", serde_json::json!({}), timeout_ms.or(Some(5000))).await
    }

    /// Queries grid status or specific task info.
    pub async fn submit_grid_status(
        &self,
        task_id: Option<&str>,
        timeout_ms: Option<u64>,
    ) -> Result<AgentMeshEnvelope, Box<dyn std::error::Error + Send + Sync>> {
        let mut args = serde_json::json!({});
        if let Some(id) = task_id {
            args["task_id"] = serde_json::json!(id);
        }
        self.send_command("grid", "grid_status", args, timeout_ms.or(Some(5000))).await
    }

    pub async fn send_envelope(
        &self,
        envelope: AgentMeshEnvelope,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let json_str = envelope.to_json_string()?;
        let tx_guard = self.tx.read().await;
        if let Some(ref tx) = *tx_guard {
            tx.send(WsMessage::Text(json_str))
                .map_err(|e| format!("Failed to send outbound message: {}", e))?;
            Ok(())
        } else {
            Err("Client is not connected".into())
        }
    }

    pub async fn close(&self) {
        self.running.store(false, Ordering::SeqCst);
        let mut tx_guard = self.tx.write().await;
        if let Some(tx) = tx_guard.take() {
            let _ = tx.send(WsMessage::Close(None));
        }
    }

    pub fn is_connected(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Runs a long-lived agent node loop with automatic reconnection.
    pub async fn run_daemon(&self) {
        while self.running.load(Ordering::SeqCst) || true {
            if let Err(e) = self.connect().await {
                warn!("Connection to hub failed: {}. Reconnecting in 3s...", e);
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }

            // Heartbeat ticker loop
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            while self.is_connected() {
                interval.tick().await;
                let hb = AgentMeshEnvelope::Heartbeat {
                    version: "1.0".to_string(),
                    correlation_id: uuid::Uuid::new_v4().to_string(),
                    from: self.node_id.clone(),
                    to: "hub".to_string(),
                    cpu_usage_pct: None,
                    memory_free_mb: None,
                    active_commands: Some(0),
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                };
                if let Err(e) = self.send_envelope(hb).await {
                    warn!("Failed to send heartbeat: {}", e);
                    break;
                }
            }

            warn!("Connection lost. Reconnecting in 3s...");
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub::AgentMeshHub;

    #[tokio::test]
    async fn test_client_connect_and_node_list() {
        let hub = AgentMeshHub::new();
        let port = hub.start("127.0.0.1:0").await.expect("Hub failed to start");
        let hub_url = format!("ws://127.0.0.1:{}/ws", port);

        let client1 = AgentMeshClient::new(&hub_url, "test-client-1", "test-os");
        client1.connect().await.expect("Client 1 failed to connect");
        tokio::time::sleep(Duration::from_millis(100)).await;

        let nodes = client1.query_node_list(Some(3000)).await.expect("Query failed");
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].node_id, "test-client-1");

        client1.close().await;
        hub.stop().await;
    }

    #[tokio::test]
    async fn test_client_send_command_e2e() {
        let hub = AgentMeshHub::new();
        let port = hub.start("127.0.0.1:0").await.expect("Hub failed to start");
        let hub_url = format!("ws://127.0.0.1:{}/ws", port);

        let node1 = AgentMeshClient::new(&hub_url, "node-sender", "windows");
        let node2 = AgentMeshClient::new(&hub_url, "node-executor", "linux");

        node1.connect().await.expect("Node 1 failed to connect");
        node2.connect().await.expect("Node 2 failed to connect");
        tokio::time::sleep(Duration::from_millis(100)).await;

        let res = node1
            .send_command(
                "node-executor",
                "echo",
                serde_json::json!({"message": "Hello Rust Agent Mesh"}),
                Some(3000),
            )
            .await
            .expect("Command send failed");

        if let AgentMeshEnvelope::CommandResponse {
            status,
            exit_code,
            stdout,
            from,
            to,
            ..
        } = res
        {
            assert_eq!(status, "success");
            assert_eq!(exit_code, 0);
            assert_eq!(stdout, "Hello Rust Agent Mesh");
            assert_eq!(from, "node-executor");
            assert_eq!(to, "node-sender");
        } else {
            panic!("Expected CommandResponse, got {:?}", res);
        }

        assert_eq!(node2.executed_commands_count(), 1);
        assert_eq!(node1.executed_commands_count(), 0);

        node1.close().await;
        node2.close().await;
        hub.stop().await;
    }

    #[tokio::test]
    async fn test_client_send_data_payload_e2e() {
        let hub = AgentMeshHub::new();
        let port = hub.start("127.0.0.1:0").await.expect("Hub failed to start");
        let hub_url = format!("ws://127.0.0.1:{}/ws", port);

        let node1 = AgentMeshClient::new(&hub_url, "node-data-src", "macos");
        let node2 = AgentMeshClient::new(&hub_url, "node-data-dst", "android");

        node1.connect().await.expect("Node 1 failed to connect");
        node2.connect().await.expect("Node 2 failed to connect");
        tokio::time::sleep(Duration::from_millis(100)).await;

        let sample_data = b"Arbitrary binary data payload 1234567890 \x00\x01\x02\xFF";
        let (_corr_id, hash) = node1
            .send_data_payload("node-data-dst", sample_data)
            .await
            .expect("Failed to send data payload");

        tokio::time::sleep(Duration::from_millis(200)).await;

        let received = node2.received_data_payloads().await;
        assert_eq!(received.len(), 1);
        if let AgentMeshEnvelope::DataPayload {
            checksum_sha256,
            from,
            to,
            ..
        } = &received[0]
        {
            assert_eq!(from, "node-data-src");
            assert_eq!(to, "node-data-dst");
            assert_eq!(checksum_sha256.as_deref(), Some(hash.as_str()));
        } else {
            panic!("Expected DataPayload, got {:?}", received[0]);
        }

        node1.close().await;
        node2.close().await;
        hub.stop().await;
    }

    #[tokio::test]
    async fn test_client_send_unknown_target_nack() {
        let hub = AgentMeshHub::new();
        let port = hub.start("127.0.0.1:0").await.expect("Hub failed to start");
        let hub_url = format!("ws://127.0.0.1:{}/ws", port);

        let node1 = AgentMeshClient::new(&hub_url, "node-caller", "windows");
        node1.connect().await.expect("Node 1 failed to connect");
        tokio::time::sleep(Duration::from_millis(100)).await;

        let res = node1
            .send_command("non-existent-target", "ping", serde_json::Value::Null, Some(2000))
            .await
            .expect("Send command should resolve with NACK");

        if let AgentMeshEnvelope::DeliveryNack { error_code, reason, .. } = res {
            assert_eq!(error_code, "ERR_NODE_NOT_FOUND");
            assert!(reason.contains("non-existent-target"));
        } else {
            panic!("Expected DeliveryNack, got {:?}", res);
        }

        node1.close().await;
        hub.stop().await;
    }
}
