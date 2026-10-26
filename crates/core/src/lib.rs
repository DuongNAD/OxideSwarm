//! Core protocol wire types, framing codecs, capabilities, task schemas, and error hierarchy for `rusty_grid`.

pub mod arbitrage;
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

pub use arbitrage::{
    calculate_authority_score, check_mobile_master_eligibility, evaluate_master_claim,
    evaluate_mobile_master_refusal, evaluate_mobile_master_refusal_matrix, resolve_election_tie,
    AuthorityEvaluation, CandidateRanking, ClaimVerdict, LeaseValidationError, MasterLeaseTicket,
    MasterRefusalReason, MobileEnvironmentTelemetry, PlatformType, ELECTION_THRESHOLD_SCORE,
    LEASE_RENEWAL_INTERVAL_MS, LEASE_VALIDITY_DURATION_MS, SCORE_MAX, SCORE_MIN,
};
pub use capabilities::{
    calculate_effective_concurrency, calculate_keepalive_interval, is_task_eligible, BatteryTier,
    MobileCapabilities, MobileLiveStatus, ThermalTier, WorkerCapabilities,
};
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
