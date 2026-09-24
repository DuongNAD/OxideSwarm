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
pub use client::AgentMeshClient;
pub use executor::{CommandExecutionResult, CommandExecutor};
pub use hub::{AgentMeshHub, HubState, HubStats};
pub use protocol::{
    compute_sha256, verify_sha256, AgentMeshEnvelope, CorrelationId, NodeDescriptor, NodeId,
};
