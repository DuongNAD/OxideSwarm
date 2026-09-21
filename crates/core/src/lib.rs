//! Core protocol wire types, framing codecs, capabilities, task schemas, and error hierarchy for `rusty_grid`.

pub mod capabilities;
pub mod error;
pub mod mapreduce;
pub mod protocol;
pub mod task;
pub mod transport;

pub use capabilities::WorkerCapabilities;
pub use error::{GridError, GridResult};
pub use protocol::{
    default_codec, deserialize_message, serialize_message, MasterMessage, MessageReader,
    MessageTransport, MessageWriter, ProtocolError, WireCodec, WorkerMessage, LENGTH_FIELD_BYTES,
    MAX_FRAME_SIZE, WIRE_FORMAT_BINCODE, WIRE_FORMAT_JSON,
};
pub use task::{Task, TaskId, TaskRequirements, TaskResult, TaskSpec, TaskStatus};
