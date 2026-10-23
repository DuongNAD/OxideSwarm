//! Core protocol wire types, framing codecs, capabilities, task schemas, and error hierarchy for `rusty_grid`.

pub mod capabilities;
pub mod cluster_auth;
pub mod discovery;
pub mod error;
pub mod mapreduce;
pub mod mode;
pub mod placement;
pub mod protocol;
pub mod task;
pub mod transport;

pub use capabilities::WorkerCapabilities;
pub use cluster_auth::{client_handshake, server_handshake, ClusterSecret};
pub use discovery::{
    discover_master, discover_master_with_secret, MasterBeacon, DEFAULT_DISCOVERY_PORT,
    DISCOVERY_MAGIC_REQUEST,
};
pub use error::{GridError, GridResult};
pub use mode::{ModeExecutionResult, WorkflowMode};
pub use placement::Placement;
pub use protocol::{
    default_codec, deserialize_message, serialize_message, MasterMessage, MessageReader,
    MessageTransport, MessageWriter, ProtocolError, WireCodec, WorkerMessage, LENGTH_FIELD_BYTES,
    MAX_FRAME_SIZE, WIRE_FORMAT_AUTH, WIRE_FORMAT_BINCODE, WIRE_FORMAT_JSON,
};
pub use task::{Task, TaskId, TaskRequirements, TaskResult, TaskSpec, TaskStatus};
