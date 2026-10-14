//! OxideSwarm Cross-Platform Coding Agent Communication Framework
//!
//! Provides the core primitives, relay hub, native WebSocket client,
//! execution sandbox, and protocol envelopes for multi-platform agent communication.

pub mod bridge;
pub mod client;
pub mod executor;
pub mod hub;
pub mod protocol;

pub use bridge::AgentGridBridge;
pub use client::{AgentMeshClient, RegistrationRejected};
pub use executor::{CommandExecutionResult, CommandExecutor};
pub use hub::{origin_is_loopback, AgentMeshHub, HubConfig, HubState, HubStats};
pub use protocol::{
    compute_sha256, validate_node_id, verify_sha256, AgentMeshEnvelope, CorrelationId,
    NodeDescriptor, NodeId, ERR_DUPLICATE_NODE_ID, ERR_INVALID_NODE_ID, ERR_NODE_UNAVAILABLE,
    ERR_NOT_REGISTERED, ERR_SENDER_MISMATCH, ERR_UNAUTHORIZED,
};
