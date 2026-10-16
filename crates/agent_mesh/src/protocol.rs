//! OxideSwarm Cross-Platform Coding Agent Wire Protocol
//!
//! Provides the canonical envelope schemas, message discriminators,
//! cryptographic integrity verification (SHA-256), and serialization helpers.

use ring::digest::{digest, SHA256};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

pub type NodeId = String;
pub type CorrelationId = String;

/// `DeliveryNack` error codes the hub emits for access-control decisions. Clients match on these
/// (e.g. to stop retrying a registration that can never succeed), so keep them stable.
pub const ERR_UNAUTHORIZED: &str = "ERR_UNAUTHORIZED";
pub const ERR_NOT_REGISTERED: &str = "ERR_NOT_REGISTERED";
pub const ERR_DUPLICATE_NODE_ID: &str = "ERR_DUPLICATE_NODE_ID";
pub const ERR_INVALID_NODE_ID: &str = "ERR_INVALID_NODE_ID";
pub const ERR_SENDER_MISMATCH: &str = "ERR_SENDER_MISMATCH";
pub const ERR_NODE_UNAVAILABLE: &str = "ERR_NODE_UNAVAILABLE";

/// Node ids the hub reserves for itself and for routing targets; no peer may register them.
pub const RESERVED_NODE_IDS: [&str; 4] = ["hub", "grid", "grid-master", "all"];

/// Longest node id (in bytes) the hub accepts.
pub const MAX_NODE_ID_LEN: usize = 128;

/// Checks that `id` is something a peer may register: 1..=128 bytes, no control characters and
/// not one of the ids the hub reserves for routing (`hub`, `grid`, `grid-master`, `all`).
pub fn validate_node_id(id: &str) -> Result<(), String> {
    if id.is_empty() {
        return Err("node id must not be empty".to_string());
    }
    if id.len() > MAX_NODE_ID_LEN {
        return Err(format!("node id is longer than {MAX_NODE_ID_LEN} bytes"));
    }
    if id.chars().any(|c| c.is_control()) {
        return Err("node id must not contain control characters".to_string());
    }
    if RESERVED_NODE_IDS.iter().any(|r| r.eq_ignore_ascii_case(id)) {
        return Err("node id is reserved by the hub".to_string());
    }
    Ok(())
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn default_version() -> String {
    "1.0".to_string()
}

fn default_correlation_id() -> String {
    Uuid::new_v4().to_string()
}

fn default_hub_target() -> String {
    "hub".to_string()
}

fn default_unknown() -> String {
    "unknown".to_string()
}

fn default_status_online() -> String {
    "online".to_string()
}

fn default_true() -> bool {
    true
}

pub fn compute_sha256(bytes: &[u8]) -> String {
    let d = digest(&SHA256, bytes);
    d.as_ref().iter().map(|b| format!("{:02x}", b)).collect()
}

pub fn verify_sha256(bytes: &[u8], expected_hex: &str) -> bool {
    let actual = compute_sha256(bytes);
    actual.eq_ignore_ascii_case(expected_hex)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeDescriptor {
    pub node_id: String,
    #[serde(default = "default_unknown")]
    pub platform: String,
    #[serde(default = "default_unknown")]
    pub hostname: String,
    #[serde(default = "default_status_online")]
    pub status: String,
    #[serde(default)]
    pub capabilities: Value,
    #[serde(default)]
    pub connected_at: u64,
}

/// Canonical wire envelope for all communication in the OxideSwarm Agent Mesh.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AgentMeshEnvelope {
    #[serde(alias = "node_registration", alias = "register")]
    NodeRegistration {
        #[serde(default = "default_version")]
        version: String,
        #[serde(default = "default_correlation_id", alias = "id")]
        correlation_id: CorrelationId,
        #[serde(alias = "node_id")]
        from: NodeId,
        #[serde(default = "default_hub_target")]
        to: NodeId,
        #[serde(default = "default_unknown")]
        platform: String,
        #[serde(default = "default_unknown")]
        hostname: String,
        #[serde(default)]
        capabilities: Value,
        /// Shared hub token (`--token` / `OXIDE_MESH_TOKEN`). Absent from older clients, so it
        /// defaults to `None`; a hub started without a token ignores it. Never log this field.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token: Option<String>,
        #[serde(default = "current_time_ms")]
        timestamp: u64,
    },

    #[serde(alias = "node_registration_ack", alias = "register_ack")]
    NodeRegistrationAck {
        #[serde(default = "default_version")]
        version: String,
        #[serde(default = "default_correlation_id", alias = "id")]
        correlation_id: CorrelationId,
        #[serde(default = "default_hub_target")]
        from: NodeId,
        to: NodeId,
        status: String,
        assigned_node_id: NodeId,
        #[serde(default)]
        heartbeat_interval_ms: u64,
        #[serde(default)]
        cluster_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        #[serde(default = "current_time_ms")]
        timestamp: u64,
    },

    #[serde(alias = "node_list", alias = "query_nodes")]
    NodeList {
        #[serde(default = "default_version")]
        version: String,
        #[serde(default = "default_correlation_id", alias = "id")]
        correlation_id: CorrelationId,
        from: NodeId,
        #[serde(default = "default_hub_target")]
        to: NodeId,
        #[serde(skip_serializing_if = "Option::is_none")]
        filter_platform: Option<String>,
        #[serde(default = "current_time_ms")]
        timestamp: u64,
    },

    #[serde(alias = "node_list_response", alias = "node_catalog")]
    NodeListResponse {
        #[serde(default = "default_version")]
        version: String,
        #[serde(default = "default_correlation_id", alias = "id")]
        correlation_id: CorrelationId,
        #[serde(default = "default_hub_target")]
        from: NodeId,
        to: NodeId,
        nodes: Vec<NodeDescriptor>,
        #[serde(default = "current_time_ms")]
        timestamp: u64,
    },

    #[serde(alias = "heartbeat")]
    Heartbeat {
        #[serde(default = "default_version")]
        version: String,
        #[serde(default = "default_correlation_id", alias = "id")]
        correlation_id: CorrelationId,
        from: NodeId,
        #[serde(default = "default_hub_target")]
        to: NodeId,
        #[serde(skip_serializing_if = "Option::is_none")]
        cpu_usage_pct: Option<f32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        memory_free_mb: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        active_commands: Option<usize>,
        #[serde(default = "current_time_ms")]
        timestamp: u64,
    },

    #[serde(alias = "heartbeat_ack")]
    HeartbeatAck {
        #[serde(default = "default_version")]
        version: String,
        #[serde(default = "default_correlation_id", alias = "id")]
        correlation_id: CorrelationId,
        #[serde(default = "default_hub_target")]
        from: NodeId,
        to: NodeId,
        #[serde(default = "current_time_ms")]
        timestamp: u64,
    },

    #[serde(alias = "command_request", alias = "route_command")]
    CommandRequest {
        #[serde(default = "default_version")]
        version: String,
        #[serde(default = "default_correlation_id", alias = "id")]
        correlation_id: CorrelationId,
        from: NodeId,
        to: NodeId,
        command: String,
        #[serde(default)]
        args: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        payload: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        timeout_ms: Option<u64>,
        #[serde(default = "default_true")]
        require_ack: bool,
        #[serde(default = "current_time_ms")]
        timestamp: u64,
    },

    #[serde(alias = "command_response", alias = "command_result")]
    CommandResponse {
        #[serde(default = "default_version")]
        version: String,
        #[serde(default = "default_correlation_id", alias = "id")]
        correlation_id: CorrelationId,
        from: NodeId,
        to: NodeId,
        #[serde(skip_serializing_if = "Option::is_none")]
        command: Option<String>,
        status: String,
        exit_code: i32,
        #[serde(default)]
        stdout: String,
        #[serde(default)]
        stderr: String,
        #[serde(default)]
        execution_duration_ms: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        payload: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
        #[serde(default = "current_time_ms")]
        timestamp: u64,
    },

    #[serde(alias = "data_payload")]
    DataPayload {
        #[serde(default = "default_version")]
        version: String,
        #[serde(default = "default_correlation_id", alias = "id")]
        correlation_id: CorrelationId,
        from: NodeId,
        to: NodeId,
        #[serde(skip_serializing_if = "Option::is_none")]
        payload_type: Option<String>,
        #[serde(default)]
        sequence_number: u32,
        #[serde(default)]
        total_chunks: u32,
        #[serde(default)]
        chunk_size_bytes: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        data: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        checksum_sha256: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        payload: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        metadata: Option<HashMap<String, String>>,
        #[serde(default = "current_time_ms")]
        timestamp: u64,
    },

    #[serde(alias = "delivery_ack", alias = "message_ack")]
    DeliveryAck {
        #[serde(default = "default_version")]
        version: String,
        #[serde(default = "default_correlation_id", alias = "id")]
        correlation_id: CorrelationId,
        from: NodeId,
        to: NodeId,
        status: String,
        #[serde(default = "current_time_ms")]
        timestamp: u64,
    },

    #[serde(alias = "delivery_nack", alias = "route_error")]
    DeliveryNack {
        #[serde(default = "default_version")]
        version: String,
        #[serde(default = "default_correlation_id", alias = "id")]
        correlation_id: CorrelationId,
        from: NodeId,
        to: NodeId,
        error_code: String,
        reason: String,
        #[serde(default = "current_time_ms")]
        timestamp: u64,
    },

    #[serde(alias = "broadcast")]
    Broadcast {
        #[serde(default = "default_version")]
        version: String,
        #[serde(default = "default_correlation_id", alias = "id")]
        correlation_id: CorrelationId,
        from: NodeId,
        #[serde(default)]
        to: NodeId,
        #[serde(skip_serializing_if = "Option::is_none")]
        topic: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        event: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        data: Option<Value>,
        #[serde(default = "current_time_ms")]
        timestamp: u64,
    },
}

impl AgentMeshEnvelope {
    pub fn correlation_id(&self) -> &str {
        match self {
            Self::NodeRegistration { correlation_id, .. } => correlation_id,
            Self::NodeRegistrationAck { correlation_id, .. } => correlation_id,
            Self::NodeList { correlation_id, .. } => correlation_id,
            Self::NodeListResponse { correlation_id, .. } => correlation_id,
            Self::Heartbeat { correlation_id, .. } => correlation_id,
            Self::HeartbeatAck { correlation_id, .. } => correlation_id,
            Self::CommandRequest { correlation_id, .. } => correlation_id,
            Self::CommandResponse { correlation_id, .. } => correlation_id,
            Self::DataPayload { correlation_id, .. } => correlation_id,
            Self::DeliveryAck { correlation_id, .. } => correlation_id,
            Self::DeliveryNack { correlation_id, .. } => correlation_id,
            Self::Broadcast { correlation_id, .. } => correlation_id,
        }
    }

    pub fn from_node(&self) -> &str {
        match self {
            Self::NodeRegistration { from, .. } => from,
            Self::NodeRegistrationAck { from, .. } => from,
            Self::NodeList { from, .. } => from,
            Self::NodeListResponse { from, .. } => from,
            Self::Heartbeat { from, .. } => from,
            Self::HeartbeatAck { from, .. } => from,
            Self::CommandRequest { from, .. } => from,
            Self::CommandResponse { from, .. } => from,
            Self::DataPayload { from, .. } => from,
            Self::DeliveryAck { from, .. } => from,
            Self::DeliveryNack { from, .. } => from,
            Self::Broadcast { from, .. } => from,
        }
    }

    pub fn to_node(&self) -> &str {
        match self {
            Self::NodeRegistration { to, .. } => to,
            Self::NodeRegistrationAck { to, .. } => to,
            Self::NodeList { to, .. } => to,
            Self::NodeListResponse { to, .. } => to,
            Self::Heartbeat { to, .. } => to,
            Self::HeartbeatAck { to, .. } => to,
            Self::CommandRequest { to, .. } => to,
            Self::CommandResponse { to, .. } => to,
            Self::DataPayload { to, .. } => to,
            Self::DeliveryAck { to, .. } => to,
            Self::DeliveryNack { to, .. } => to,
            Self::Broadcast { to, .. } => to,
        }
    }

    pub fn msg_type(&self) -> &'static str {
        match self {
            Self::NodeRegistration { .. } => "NodeRegistration",
            Self::NodeRegistrationAck { .. } => "NodeRegistrationAck",
            Self::NodeList { .. } => "NodeList",
            Self::NodeListResponse { .. } => "NodeListResponse",
            Self::Heartbeat { .. } => "Heartbeat",
            Self::HeartbeatAck { .. } => "HeartbeatAck",
            Self::CommandRequest { .. } => "CommandRequest",
            Self::CommandResponse { .. } => "CommandResponse",
            Self::DataPayload { .. } => "DataPayload",
            Self::DeliveryAck { .. } => "DeliveryAck",
            Self::DeliveryNack { .. } => "DeliveryNack",
            Self::Broadcast { .. } => "Broadcast",
        }
    }

    /// Normalizes JSON values that may use alternate field names (`msg_type`, `id`, `node_id`)
    pub fn normalize_json_value(val: &mut Value) {
        if let Some(obj) = val.as_object_mut() {
            if !obj.contains_key("type") {
                if let Some(msg_type) = obj.remove("msg_type") {
                    obj.insert("type".to_string(), msg_type);
                }
            } else {
                obj.remove("msg_type");
            }
            if !obj.contains_key("correlation_id") {
                if let Some(id) = obj.remove("id") {
                    obj.insert("correlation_id".to_string(), id);
                }
            } else {
                obj.remove("id");
            }
            if !obj.contains_key("from") {
                if let Some(node_id) = obj.remove("node_id") {
                    obj.insert("from".to_string(), node_id);
                }
            } else {
                obj.remove("node_id");
            }
        }
    }

    pub fn from_json_str(s: &str) -> Result<Self, serde_json::Error> {
        let mut val: Value = serde_json::from_str(s)?;
        Self::normalize_json_value(&mut val);
        serde_json::from_value(val)
    }

    pub fn to_json_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    // Factory methods
    pub fn new_command_request(
        from: impl Into<String>,
        to: impl Into<String>,
        command: impl Into<String>,
        args: Value,
        timeout_ms: Option<u64>,
    ) -> Self {
        Self::CommandRequest {
            version: default_version(),
            correlation_id: default_correlation_id(),
            from: from.into(),
            to: to.into(),
            command: command.into(),
            args,
            payload: None,
            timeout_ms,
            require_ack: true,
            timestamp: current_time_ms(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_command_response(
        from: impl Into<String>,
        to: impl Into<String>,
        correlation_id: impl Into<String>,
        command: Option<String>,
        status: impl Into<String>,
        exit_code: i32,
        stdout: impl Into<String>,
        stderr: impl Into<String>,
        execution_duration_ms: u64,
    ) -> Self {
        Self::CommandResponse {
            version: default_version(),
            correlation_id: correlation_id.into(),
            from: from.into(),
            to: to.into(),
            command,
            status: status.into(),
            exit_code,
            stdout: stdout.into(),
            stderr: stderr.into(),
            execution_duration_ms,
            payload: None,
            error: None,
            timestamp: current_time_ms(),
        }
    }

    pub fn new_data_payload(
        from: impl Into<String>,
        to: impl Into<String>,
        data_bytes: &[u8],
    ) -> (Self, String) {
        let hash = compute_sha256(data_bytes);
        // Use latin1-safe encoding for arbitrary byte transmission over JSON strings
        let text: String = data_bytes.iter().map(|&b| b as char).collect();
        let envelope = Self::DataPayload {
            version: default_version(),
            correlation_id: default_correlation_id(),
            from: from.into(),
            to: to.into(),
            payload_type: Some("raw_bytes".to_string()),
            sequence_number: 0,
            total_chunks: 1,
            chunk_size_bytes: data_bytes.len(),
            data: Some(text),
            checksum_sha256: Some(hash.clone()),
            payload: None,
            metadata: None,
            timestamp: current_time_ms(),
        };
        (envelope, hash)
    }

    pub fn new_delivery_ack(
        from: impl Into<String>,
        to: impl Into<String>,
        correlation_id: impl Into<String>,
        status: impl Into<String>,
    ) -> Self {
        Self::DeliveryAck {
            version: default_version(),
            correlation_id: correlation_id.into(),
            from: from.into(),
            to: to.into(),
            status: status.into(),
            timestamp: current_time_ms(),
        }
    }

    pub fn new_delivery_nack(
        from: impl Into<String>,
        to: impl Into<String>,
        correlation_id: impl Into<String>,
        error_code: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self::DeliveryNack {
            version: default_version(),
            correlation_id: correlation_id.into(),
            from: from.into(),
            to: to.into(),
            error_code: error_code.into(),
            reason: reason.into(),
            timestamp: current_time_ms(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha256_computation_and_verification() {
        let data = b"Hello OxideSwarm Agent Mesh!";
        let hash = compute_sha256(data);
        assert_eq!(hash.len(), 64);
        assert!(verify_sha256(data, &hash));
        assert!(!verify_sha256(b"Wrong data", &hash));
    }

    #[test]
    fn test_envelope_serialization_roundtrip() {
        let (env, hash) =
            AgentMeshEnvelope::new_data_payload("node-1", "node-2", b"test data payload");
        let json_str = env.to_json_string().unwrap();
        let decoded = AgentMeshEnvelope::from_json_str(&json_str).unwrap();

        if let AgentMeshEnvelope::DataPayload {
            checksum_sha256,
            from,
            to,
            ..
        } = decoded
        {
            assert_eq!(from, "node-1");
            assert_eq!(to, "node-2");
            assert_eq!(checksum_sha256.as_deref(), Some(hash.as_str()));
        } else {
            panic!("Expected DataPayload variant");
        }
    }

    #[test]
    fn test_registration_token_is_optional_on_the_wire() {
        // Pre-token clients send no `token` field; it must still parse (and stay unset).
        let legacy = r#"{"type":"NodeRegistration","from":"old-node","platform":"linux"}"#;
        match AgentMeshEnvelope::from_json_str(legacy).unwrap() {
            AgentMeshEnvelope::NodeRegistration { from, token, .. } => {
                assert_eq!(from, "old-node");
                assert_eq!(token, None);
            }
            other => panic!("expected NodeRegistration, got {other:?}"),
        }

        let with_token = r#"{"type":"NodeRegistration","from":"n","token":"s3cret"}"#;
        match AgentMeshEnvelope::from_json_str(with_token).unwrap() {
            AgentMeshEnvelope::NodeRegistration { token, .. } => {
                assert_eq!(token.as_deref(), Some("s3cret"));
            }
            other => panic!("expected NodeRegistration, got {other:?}"),
        }

        // A token-less registration must not serialise a `token: null` that old hubs choke on.
        let reg = AgentMeshEnvelope::NodeRegistration {
            version: "1.0".into(),
            correlation_id: "c".into(),
            from: "n".into(),
            to: "hub".into(),
            platform: "linux".into(),
            hostname: "h".into(),
            capabilities: Value::Null,
            token: None,
            timestamp: 0,
        };
        assert!(!reg.to_json_string().unwrap().contains("token"));
    }

    #[test]
    fn test_validate_node_id_rules() {
        assert!(validate_node_id("node-linux-box").is_ok());
        assert!(validate_node_id("nodé-🚀").is_ok());
        for bad in [
            "",
            "hub",
            "HUB",
            "grid",
            "grid-master",
            "all",
            "has\nnewline",
            "nul\0byte",
        ] {
            assert!(validate_node_id(bad).is_err(), "{bad:?} must be rejected");
        }
        assert!(validate_node_id(&"x".repeat(MAX_NODE_ID_LEN)).is_ok());
        assert!(validate_node_id(&"x".repeat(MAX_NODE_ID_LEN + 1)).is_err());
    }

    #[test]
    fn test_msg_type_field_normalization() {
        let raw_json = r#"{
            "msg_type": "CommandRequest",
            "id": "1234-abcd",
            "from": "node-win",
            "to": "node-android",
            "command": "shell_exec",
            "args": {"cmd": "uname -a"}
        }"#;

        let env = AgentMeshEnvelope::from_json_str(raw_json).unwrap();
        assert_eq!(env.correlation_id(), "1234-abcd");
        assert_eq!(env.from_node(), "node-win");
        assert_eq!(env.to_node(), "node-android");
        assert_eq!(env.msg_type(), "CommandRequest");
    }
}
