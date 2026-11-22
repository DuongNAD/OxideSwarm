//! OxideSwarm Cross-Platform Coding Agent Communication Framework
//!
//! Provides the core primitives, relay hub, native WebSocket client,
//! command executor, and protocol envelopes for multi-platform agent communication.

#![deny(unsafe_code)]

pub mod bridge;
pub mod client;
mod delivery;
// LV-4: cfg(unix) libc::killpg process-group teardown and signal probe in tests.
#[allow(unsafe_code)]
pub mod executor;
pub mod hub;
pub mod protocol;

pub use bridge::AgentGridBridge;
pub use client::{
    validate_inbox_dir, AgentMeshClient, DataDeliveryReceipt, RegistrationRejected,
    DEFAULT_INBOX_MAX_BYTES, DEFAULT_INBOX_MAX_FILES, DEFAULT_MAX_RECEIVED_BYTES,
    DEFAULT_MAX_RECEIVED_PAYLOADS,
};
pub use executor::{CommandExecutionResult, CommandExecutor};
pub use hub::{origin_is_loopback, AgentMeshHub, HubConfig, HubState, HubStats};
pub use protocol::{
    compute_sha256, max_chunk_json_bytes, validate_node_id, verify_sha256, AgentMeshEnvelope,
    CorrelationId, NodeDescriptor, NodeId, ACK_STATUS_CHUNK_RECEIVED, ACK_STATUS_DATA_VERIFIED,
    ACK_STATUS_TARGET_FORWARDED, DATA_CHUNK_WINDOW, DEFAULT_DATA_CHUNK_BYTES,
    ERR_DUPLICATE_NODE_ID, ERR_INBOX_FULL, ERR_INVALID_NODE_ID, ERR_NODE_NOT_FOUND,
    ERR_NODE_UNAVAILABLE, ERR_NOT_REGISTERED, ERR_PAYLOAD_CORRUPT, ERR_PERSIST_FAILED,
    ERR_SENDER_MISMATCH, ERR_UNAUTHORIZED,
};
