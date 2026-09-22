//! Core protocol wire types, framing codecs, capabilities, task schemas, and error hierarchy for `rusty_grid`.

pub mod capabilities;
pub mod discovery;
pub mod error;
pub mod mapreduce;
pub mod mode;
pub mod protocol;
pub mod task;
pub mod transport;

pub use capabilities::WorkerCapabilities;
pub use discovery::{discover_master, MasterBeacon, DEFAULT_DISCOVERY_PORT, DISCOVERY_MAGIC_REQUEST};
pub use error::{GridError, GridResult};
pub use mode::{ModeExecutionResult, WorkflowMode};
pub use protocol::{
    default_codec, MasterMessage, MessageReader, MessageTransport, MessageWriter, ProtocolError,
    WorkerMessage, LENGTH_FIELD_BYTES, MAX_FRAME_SIZE,
};
pub use task::{Task, TaskId, TaskRequirements, TaskResult, TaskSpec, TaskStatus};
