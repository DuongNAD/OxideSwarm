//! OxideSwarm Cross-Platform Coding Agent Communication Framework
//!
//! Provides the core primitives, relay hub, native WebSocket client,
//! command executor, and protocol envelopes for multi-platform agent communication.

#![deny(unsafe_code)]

pub mod bridge;
pub mod client;
// LV-4: cfg(unix) libc::killpg process-group teardown and signal probe in tests.
#[allow(unsafe_code)]
pub mod executor;
pub mod grid_backend;
pub mod hub;
pub mod protocol;
pub mod remote_backend;

pub use bridge::AgentGridBridge;
pub use client::{
    validate_inbox_dir, AgentMeshClient, DataDeliveryReceipt, ExtractedArtifact,
    RegistrationRejected, DEFAULT_MAX_RECEIVED_BYTES, DEFAULT_MAX_RECEIVED_PAYLOADS,
};
pub use executor::{CommandExecutionResult, CommandExecutor};
pub use grid_backend::{
    BoxGridFuture, GridBackend, GridClusterStatus, GridTaskResult, GridTaskStatus, InProcessBackend,
};
pub use hub::{origin_is_loopback, AgentMeshHub, HubConfig, HubState, HubStats};
pub use protocol::{
    compute_sha256, validate_node_id, verify_sha256, AgentMeshEnvelope, CorrelationId,
    NodeDescriptor, NodeId, DEFAULT_DATA_CHUNK_BYTES, ERR_DUPLICATE_NODE_ID, ERR_INVALID_NODE_ID,
    ERR_NODE_NOT_FOUND, ERR_NODE_UNAVAILABLE, ERR_NOT_REGISTERED, ERR_PAYLOAD_CORRUPT,
    ERR_PERSIST_FAILED, ERR_SENDER_MISMATCH, ERR_UNAUTHORIZED,
};
pub use remote_backend::{
    is_p2p_ticket, master_requires_secret, normalize_master_addr, RemoteMasterBackend,
    RemoteMasterConfig, DEFAULT_RPC_TIMEOUT,
};
