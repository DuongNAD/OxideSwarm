//! Wire protocol message types, length-delimited framing codec, and transport helpers.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::capabilities::WorkerCapabilities;
use crate::task::{
    Bytes as TaskBytes, CheckpointData, Task, TaskId, TaskResult, TaskStatus, WorkspaceAffinity,
};

mod client_msg_mirrors;
mod client_resp_mirrors;
mod codec;
mod worker_master_mirrors;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod version_and_aead_tests;

#[cfg(test)]
pub(crate) use client_msg_mirrors::serialize_client_message_with_version;
#[cfg(test)]
pub(crate) use client_resp_mirrors::serialize_client_response_with_version;
pub use codec::{
    default_codec, derive_aead_keys, deserialize_message, deserialize_message_with_version,
    serialize_message, serialize_message_for_peer, AeadReaderSession, AeadWriterSession,
    MessageReader, MessageTransport, MessageWriter, AEAD_RECORD_OVERHEAD,
};
#[cfg(any(test, feature = "test-extensions"))]
pub use codec::{deserialize_message_as_v9, serialize_v9_client_message};

/// Maximum allowable frame size in bytes (64 MB).
pub const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;
/// Length prefix field width in bytes (4-byte unsigned big-endian integer).
pub const LENGTH_FIELD_BYTES: usize = 4;

/// Wire protocol format discriminator identifiers.
pub const WIRE_FORMAT_JSON: u8 = 0x01;
pub const WIRE_FORMAT_BINCODE: u8 = 0x02;
/// Discriminator of cluster-authentication handshake frames (see `cluster_auth`).
/// These frames never carry a `WorkerMessage`/`MasterMessage`/`ClientMessage`; the typed
/// decoder rejects them with `ProtocolError::InvalidFormatTag(0x03)`.
pub const WIRE_FORMAT_AUTH: u8 = 0x03;
/// Discriminator of post-handshake AEAD-encrypted frames (`[0x04][seq: 8B][ciphertext][tag: 16B]`).
pub const WIRE_FORMAT_ENCRYPTED: u8 = 0x04;

/// Current wire protocol version.
pub const PROTOCOL_VERSION_CURRENT: u32 = 10;
/// Minimum wire protocol version compatible with this build. One version back while the window
/// of design 0083 is open (`CURRENT - 1`); equal to `CURRENT` when a wire change closes it.
pub const PROTOCOL_VERSION_MIN_COMPATIBLE: u32 = 9;
/// Protocol version assumed when a JSON peer omits `protocol_version`.
pub const LEGACY_UNVERSIONED_PROTOCOL_VERSION: u32 = 2;

/// Default protocol version used in deserialization fallbacks when `protocol_version` is omitted.
pub const fn default_protocol_version() -> u32 {
    LEGACY_UNVERSIONED_PROTOCOL_VERSION
}

/// Returns `true` when `peer_version` is within the supported inclusive range
/// [`PROTOCOL_VERSION_MIN_COMPATIBLE`]`..=`[`PROTOCOL_VERSION_CURRENT`].
pub const fn is_protocol_version_compatible(peer_version: u32) -> bool {
    peer_version >= PROTOCOL_VERSION_MIN_COMPATIBLE && peer_version <= PROTOCOL_VERSION_CURRENT
}

/// Returns `true` when a worker or client accepts `master_version` as the master's echo: only its
/// own version (design 0083 D5). The window protects a newer master from older peers, never the
/// reverse, so a peer does not take a master that answers at an older version.
pub const fn is_master_echo_acceptable(master_version: u32) -> bool {
    master_version == PROTOCOL_VERSION_CURRENT
}

/// Formats a human-readable refusal message when a peer speaks an incompatible wire protocol
/// version.
pub fn format_incompatible_version_error(peer_role: &str, peer_version: u32) -> String {
    let upgrade_targets = if peer_role.eq_ignore_ascii_case("master") {
        "master and worker/client".to_string()
    } else {
        format!("master and {peer_role}")
    };
    format!(
        "Incompatible protocol version {peer_version} from {peer_role} \
         (supported: {PROTOCOL_VERSION_MIN_COMPATIBLE}..={PROTOCOL_VERSION_CURRENT}; \
         upgrade both {upgrade_targets} to matching builds)"
    )
}

/// Negotiable protocol capabilities and feature flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProtocolCapabilities(pub u64);

impl ProtocolCapabilities {
    pub const NONE: Self = Self(0);
    /// Informational only: the record layer is selected by the cluster handshake version
    /// (`cluster_auth::AUTH_VERSION`), never by this bit, which travels after the handshake.
    pub const AEAD_ENCRYPTION: Self = Self(1 << 0);

    pub const fn empty() -> Self {
        Self(0)
    }

    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }
    pub const MOBILE_TELEMETRY: Self = Self(1 << 1);
    pub const CHECKPOINT_DELTA: Self = Self(1 << 2);
    pub const GPU_SCHEDULE_PRIO: Self = Self(1 << 3);
    pub const EXTENDED_ERROR_INFO: Self = Self(1 << 4);
    pub const BATCH_DISPATCH: Self = Self(1 << 5);
    /// Distributed file staging and output artifact collection.
    pub const FILE_STAGING: Self = Self(1 << 6);
    /// Autonomous role arbitrage, lease tickets, and graceful role handover (design-only;
    /// not advertised in [`ProtocolCapabilities::all_supported`]).
    pub const ROLE_ARBITRAGE: Self = Self(1 << 7);
    /// Cellular RRC-aware adaptive keepalive intervals.
    pub const ADAPTIVE_KEEPALIVE: Self = Self(1 << 8);
    /// Content-addressed task inputs and output files moved through the Master's blob store
    /// (`Task::inputs`, `Task::output_globs`, see `crate::blob`).
    pub const BLOB_TRANSFER: Self = Self(1 << 9);
    /// Persistent named workspaces (`Task::workspace`, design 0039). Reserved at `1 << 10`; not
    /// advertised in [`ProtocolCapabilities::all_supported`] until the worker store and the
    /// scheduler affinity (design 0039 W2, W3) land.
    pub const WORKSPACES: Self = Self(1 << 10);
    /// Generative 3D (`TaskSpec::Generative3D`) and skeletal auto-rigging (`TaskSpec::AutoRig`)
    /// execution support (advertised by workers compiled with the `3d` Cargo feature).
    pub const THREE_D: Self = Self(1 << 11);
    /// Worker session resumption across transient master restarts or network drops
    /// (`WorkerMessage::ResumeSession`, `master_features::SESSION_RESUME_PREFIX`).
    pub const SESSION_RESUME: Self = Self(1 << 12);
    /// `oxideswarm-tree/2` directory manifests carrying symbolic links or empty directories
    /// (design 0039 W11).
    pub const TREE_V2: Self = Self(1 << 13);

    pub const fn all_supported() -> Self {
        Self(
            Self::AEAD_ENCRYPTION.0
                | Self::MOBILE_TELEMETRY.0
                | Self::CHECKPOINT_DELTA.0
                | Self::GPU_SCHEDULE_PRIO.0
                | Self::EXTENDED_ERROR_INFO.0
                | Self::BATCH_DISPATCH.0
                | Self::FILE_STAGING.0
                | Self::ADAPTIVE_KEEPALIVE.0
                | Self::BLOB_TRANSFER.0
                | Self::THREE_D.0
                | Self::SESSION_RESUME.0
                | Self::TREE_V2.0,
        )
    }

    pub fn contains(&self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    pub fn intersects(&self, other: Self) -> bool {
        (self.0 & other.0) != 0
    }

    pub fn insert(&mut self, other: Self) {
        self.0 |= other.0;
    }

    pub fn remove(&mut self, other: Self) {
        self.0 &= !other.0;
    }

    pub fn bits(&self) -> u64 {
        self.0
    }

    pub fn from_bits_truncate(bits: u64) -> Self {
        Self(bits)
    }
}

impl Default for ProtocolCapabilities {
    fn default() -> Self {
        Self::all_supported()
    }
}

impl std::ops::BitOr for ProtocolCapabilities {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl std::ops::BitAnd for ProtocolCapabilities {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self {
        Self(self.0 & rhs.0)
    }
}

/// Endpoint role in a bidirectional connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointRole {
    Client,
    Server,
}

/// Supported wire serialization formats for framed messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[repr(u8)]
pub enum WireCodec {
    /// Text-based JSON encoding (discriminator 0x01 or raw '{').
    Json = WIRE_FORMAT_JSON,
    /// High-throughput binary encoding via Bincode (discriminator 0x02).
    #[default]
    Bincode = WIRE_FORMAT_BINCODE,
}

impl std::str::FromStr for WireCodec {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "bincode" | "bin" | "binary" => Ok(WireCodec::Bincode),
            "json" => Ok(WireCodec::Json),
            other => Err(format!(
                "Unknown wire codec '{other}', expected 'bincode' or 'json'"
            )),
        }
    }
}

impl std::fmt::Display for WireCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WireCodec::Json => write!(f, "json"),
            WireCodec::Bincode => write!(f, "bincode"),
        }
    }
}

/// Protocol-level error variants encountered during framing and serialization.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    /// I/O error reading from or writing to the underlying stream.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization or deserialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Bincode serialization or deserialization error.
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),

    /// Unsupported wire format discriminator byte.
    #[error("Unsupported wire format discriminator: 0x{0:02x}")]
    InvalidFormatTag(u8),

    /// Frame size exceeds maximum allowed threshold.
    #[error("Frame size {size} bytes exceeds maximum permitted {max} bytes")]
    FrameTooLarge {
        /// Actual frame size in bytes.
        size: usize,
        /// Maximum allowable frame size in bytes.
        max: usize,
    },

    /// Unexpected EOF encountered while reading frame payload.
    #[error("Unexpected EOF encountered while reading frame payload")]
    UnexpectedEof,

    /// Connection closed gracefully by remote peer.
    #[error("Connection closed gracefully by peer")]
    ConnectionClosed,

    /// Peer sent a message with an incompatible wire protocol version.
    #[error(
        "Incompatible protocol version {peer_version} (supported: {min_compatible_version}..={current_version}; upgrade both master and client/worker to matching builds)"
    )]
    IncompatibleVersion {
        peer_version: u32,
        current_version: u32,
        min_compatible_version: u32,
    },

    /// General protocol violation.
    #[error("Protocol violation: {0}")]
    Violation(String),
}

/// Upstream messages sent from Worker nodes to the Master node.
#[derive(Debug, Clone, PartialEq)]
pub enum WorkerMessage {
    /// Initial registration advertising worker identity, capabilities, and protocol negotiation.
    Register {
        worker_id: Uuid,
        capabilities: WorkerCapabilities,
        protocol_version: u32,
        features: ProtocolCapabilities,
    },
    /// Periodic heartbeat informing the master of worker liveness and load.
    Heartbeat {
        worker_id: Uuid,
        timestamp: u64,
        active_tasks: usize,
        cpu_usage_pct: f32,
        ram_available_mb: u64,
    },
    /// Incremental progress update or state change for an active task.
    TaskProgress {
        worker_id: Uuid,
        task_id: TaskId,
        status: TaskStatus,
    },
    /// Final execution outcome and metrics for a completed or failed task.
    TaskResult {
        worker_id: Uuid,
        task_id: TaskId,
        exit_code: i32,
        stdout: TaskBytes,
        stderr: TaskBytes,
        execution_time_ms: u64,
        is_gpu_executed: bool,
        device_name: Option<String>,
        error: Option<String>,
        asset_bytes: Option<TaskBytes>,
        artifacts: Vec<crate::task::OutputArtifact>,
        outputs: Vec<crate::blob::BlobFile>,
    },
    /// Incremental computational snapshot checkpoint emitted during task execution.
    Checkpoint {
        task_id: TaskId,
        sequence: u64,
        delta_state: TaskBytes,
    },
    /// Graceful announcement that the worker is disconnecting.
    Disconnecting { worker_id: Uuid, reason: String },
    /// Live battery/thermal status of a mobile or battery-powered worker, sent whenever it
    /// changes.
    MobileTelemetry {
        worker_id: Uuid,
        battery_pct: Option<u8>,
        is_charging: Option<bool>,
        thermal_throttled: bool,
        battery_temperature: Option<f32>,
        network_type: Option<String>,
        battery_tier: Option<crate::capabilities::BatteryTier>,
        thermal_tier: Option<crate::capabilities::ThermalTier>,
    },
    /// Claims the Master coordinator role by presenting a valid cryptographic lease ticket (§5.3).
    ClaimMasterRole {
        ticket: crate::arbitrage::MasterLeaseTicket,
    },
    /// Forward-compatible fallback variant for unrecognized messages from future protocol versions.
    UnknownVariant { tag: u32, raw_bytes: Vec<u8> },
    /// A blob request (`crate::blob_transfer`), answered by `MasterMessage::BlobReply` with the
    /// same `request_id`. Sent only to a Master that announced [`master_features::BLOBS`] (an
    /// older one cannot decode it). Appended after `UnknownVariant` so that existing positional
    /// (bincode) variant indices do not move.
    BlobRequest {
        request_id: u64,
        request: crate::blob_transfer::BlobRequest,
    },
    /// Sent immediately after `RegisterAck` when both worker and master support
    /// `SESSION_RESUME` and the worker still has tasks running or buffered from its previous session.
    ResumeSession {
        worker_id: Uuid,
        running: Vec<TaskId>,
    },
}

impl WorkerMessage {
    /// Helper constructor for creating a standard registration message.
    pub fn register(worker_id: Uuid, capabilities: WorkerCapabilities) -> Self {
        Self::Register {
            worker_id,
            capabilities,
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
        }
    }

    /// Returns the static variant name of the message.
    pub fn variant_name(&self) -> &'static str {
        match self {
            Self::Register { .. } => "Register",
            Self::Heartbeat { .. } => "Heartbeat",
            Self::TaskProgress { .. } => "TaskProgress",
            Self::TaskResult { .. } => "TaskResult",
            Self::Checkpoint { .. } => "Checkpoint",
            Self::Disconnecting { .. } => "Disconnecting",
            Self::MobileTelemetry { .. } => "MobileTelemetry",
            Self::ClaimMasterRole { .. } => "ClaimMasterRole",
            Self::UnknownVariant { .. } => "UnknownVariant",
            Self::BlobRequest { .. } => "BlobRequest",
            Self::ResumeSession { .. } => "ResumeSession",
        }
    }

    /// Returns the worker UUID associated with this message.
    pub fn worker_id(&self) -> Uuid {
        match self {
            Self::Register { worker_id, .. } => *worker_id,
            Self::Heartbeat { worker_id, .. } => *worker_id,
            Self::TaskProgress { worker_id, .. } => *worker_id,
            Self::TaskResult { worker_id, .. } => *worker_id,
            Self::Checkpoint { .. } => Uuid::nil(),
            Self::Disconnecting { worker_id, .. } => *worker_id,
            Self::MobileTelemetry { worker_id, .. } => *worker_id,
            Self::ClaimMasterRole { ticket } => ticket.master_id,
            Self::UnknownVariant { .. } => Uuid::nil(),
            Self::BlobRequest { .. } => Uuid::nil(),
            Self::ResumeSession { worker_id, .. } => *worker_id,
        }
    }
}

impl From<TaskResult> for WorkerMessage {
    fn from(r: TaskResult) -> Self {
        WorkerMessage::TaskResult {
            worker_id: r.worker_id,
            task_id: r.task_id,
            exit_code: r.exit_code,
            stdout: r.stdout,
            stderr: r.stderr,
            execution_time_ms: r.execution_time_ms,
            is_gpu_executed: r.is_gpu_executed,
            device_name: r.device_name,
            error: r.error,
            asset_bytes: r.asset_bytes,
            artifacts: r.artifacts,
            outputs: r.outputs,
        }
    }
}

/// Downstream messages sent from the Master node to Worker nodes.
#[derive(Debug, Clone, PartialEq)]
pub enum MasterMessage {
    /// Acknowledgment of worker registration with operational parameters and protocol negotiation.
    RegisterAck {
        accepted: bool,
        worker_id: Uuid,
        heartbeat_interval_secs: u64,
        protocol_version: u32,
        features: ProtocolCapabilities,
        message: Option<String>,
    },
    /// Acknowledgment of heartbeat receipt confirming master liveness.
    HeartbeatAck { timestamp: u64 },
    /// Assignment of a task payload for execution.
    AssignTask { task: Task },
    /// Assignment of a task payload with checkpoint data for delta resumption.
    AssignTaskWithCheckpoint {
        task: Task,
        latest_checkpoint: Option<CheckpointData>,
    },
    /// Directive to immediately abort and cancel an assigned or running task.
    CancelTask {
        task_id: TaskId,
        reason: Option<String>,
    },
    /// Directive notifying the worker of master shutdown or cluster evacuation.
    Shutdown {
        reason: String,
        grace_period_secs: Option<u64>,
    },
    /// Response to ClaimMasterRole acknowledging or rejecting coordinator role handover.
    RoleHandoverAck {
        accepted: bool,
        reason: Option<String>,
        task_queue_snapshot: Vec<Task>,
        connected_workers: Vec<WorkerCapabilities>,
    },
    /// Forward-compatible fallback variant for unrecognized messages from future protocol versions.
    UnknownVariant { tag: u32, raw_bytes: Vec<u8> },
    /// The answer to the `WorkerMessage::BlobRequest` with the same `request_id`. Appended after
    /// `UnknownVariant` so that existing positional (bincode) variant indices do not move.
    BlobReply {
        request_id: u64,
        reply: crate::blob_transfer::BlobReply,
    },
}

/// Features a Master announces in the `message` of an accepted `RegisterAck`
/// (`features=<name>,<name>`; a rejection's `message` stays a plain reason).
///
/// The wire format has no version negotiation and bincode is positional, so a message variant added
/// after a Master was built is a decode error there that drops the connection. A Worker therefore
/// sends such variants only to a Master that said it understands them. A Master that predates the
/// announcement sends no `message`, which reads as "no optional features".
pub mod master_features {
    /// Understands `WorkerMessage::MobileTelemetry` (live battery / thermal reports).
    pub const MOBILE_TELEMETRY: &str = "mobile-telemetry";

    /// Has a blob store and answers `WorkerMessage::BlobRequest` (task inputs and output files,
    /// see `crate::blob`). Announced only by a Master that runs with a blob store, so it is not
    /// part of [`SUPPORTED`].
    pub const BLOBS: &str = "blobs";

    /// Prefix for the parameterized session-resume hold feature token (`session-resume=<secs>`).
    /// Announced only when the Master has a non-zero session-resume hold configured.
    pub const SESSION_RESUME_PREFIX: &str = "session-resume=";

    /// Upper bound (`1 hour`) on the session-resume hold seconds accepted by
    /// [`parse_session_resume_hold_secs`], preventing `Instant + Duration` overflow on bogus input.
    pub const MAX_SESSION_RESUME_HOLD_SECS: u64 = 3_600;

    /// `MasterMessage::CancelTask { task_id, reason: Some(format_result_ack_reason(token)) }`
    /// prefix sent by the Master on the control lane after it has accepted and durably recorded a
    /// `WorkerMessage::TaskResult` from a `SESSION_RESUME`-capable Worker. The Worker retains
    /// the completed result (and its pinned output files) in `pending_resume_outbox` until it
    /// receives this acknowledgment for the matching attempt token (or a regular `CancelTask` for
    /// `task_id`), preventing result loss if the Master shuts down gracefully or the session drops
    /// mid-flush without letting an earlier attempt's acknowledgment evict a later retry's result.
    pub const RESULT_ACK_REASON: &str = "oxs:result-ack";

    /// Every feature this build of the Master supports unconditionally.
    pub const SUPPORTED: &[&str] = &[MOBILE_TELEMETRY];

    const PREFIX: &str = "features=";

    /// Computes a deterministic 64-bit BLAKE3 token for a [`crate::task::TaskResult`] over a
    /// canonical little-endian encoding of `task_id`, `worker_id`, `exit_code`,
    /// `execution_time_ms`, `is_gpu_executed`, `device_name`, `stdout`, `stderr`, `error`,
    /// `asset_bytes`, `artifacts`, and `outputs`, taking the first 8 digest bytes in little-endian
    /// order so a result acknowledgment (`"oxs:result-ack:<token>"`) is stable across Rust versions
    /// and builds and only ever acknowledges the matching attempt's buffered result.
    pub fn result_ack_token(result: &crate::task::TaskResult) -> u64 {
        fn put_bytes(h: &mut blake3::Hasher, bytes: &[u8]) {
            h.update(&(bytes.len() as u64).to_le_bytes());
            h.update(bytes);
        }
        fn put_opt_bytes(h: &mut blake3::Hasher, opt: Option<&[u8]>) {
            match opt {
                Some(bytes) => {
                    h.update(&[1u8]);
                    put_bytes(h, bytes);
                }
                None => {
                    h.update(&[0u8]);
                }
            }
        }

        let mut hasher = blake3::Hasher::new();
        hasher.update(result.task_id.0.as_bytes());
        hasher.update(result.worker_id.as_bytes());
        hasher.update(&result.exit_code.to_le_bytes());
        hasher.update(&result.execution_time_ms.to_le_bytes());
        hasher.update(&[u8::from(result.is_gpu_executed)]);
        put_opt_bytes(
            &mut hasher,
            result.device_name.as_deref().map(str::as_bytes),
        );
        put_bytes(&mut hasher, result.stdout.as_bytes());
        put_bytes(&mut hasher, result.stderr.as_bytes());
        put_opt_bytes(&mut hasher, result.error.as_deref().map(str::as_bytes));
        put_opt_bytes(
            &mut hasher,
            result.asset_bytes.as_ref().map(|b| b.as_bytes()),
        );
        hasher.update(&(result.artifacts.len() as u64).to_le_bytes());
        for a in &result.artifacts {
            put_bytes(&mut hasher, a.path.as_bytes());
            hasher.update(&a.size.to_le_bytes());
            hasher.update(&a.sha256);
            put_bytes(&mut hasher, a.data.as_bytes());
        }
        hasher.update(&(result.outputs.len() as u64).to_le_bytes());
        for o in &result.outputs {
            put_bytes(&mut hasher, o.path.as_bytes());
            hasher.update(&o.id.0);
            hasher.update(&o.size.to_le_bytes());
            hasher.update(&[u8::from(o.executable)]);
        }
        let digest = hasher.finalize();
        let mut out = [0u8; 8];
        out.copy_from_slice(&digest.as_bytes()[..8]);
        u64::from_le_bytes(out)
    }

    /// Formats a `MasterMessage::CancelTask` reason string acknowledging `result` (`token`).
    pub fn format_result_ack_reason(token: u64) -> String {
        format!("{RESULT_ACK_REASON}:{token:016x}")
    }

    /// Parses a `MasterMessage::CancelTask` reason string to check whether it is a result
    /// acknowledgment (`"oxs:result-ack"` or `"oxs:result-ack:<hex-token>"`).
    ///
    /// Returns `Some(Some(token))` for a token-keyed acknowledgment, `Some(None)` for a bare
    /// [`RESULT_ACK_REASON`], or `None` for an ordinary cancellation reason.
    pub fn parse_result_ack_reason(reason: &str) -> Option<Option<u64>> {
        if reason == RESULT_ACK_REASON {
            return Some(None);
        }
        let hex = reason.strip_prefix(RESULT_ACK_REASON)?.strip_prefix(':')?;
        u64::from_str_radix(hex, 16).ok().map(Some)
    }

    /// Formats the `session-resume=<hold_secs>` feature token.
    pub fn session_resume_feature(hold_secs: u64) -> String {
        format!("{SESSION_RESUME_PREFIX}{hold_secs}")
    }

    /// Extracts the Master's advertised session-resume hold in seconds from a `RegisterAck`'s
    /// `message`, or `None` if `session-resume=<secs>` is not announced, cannot be parsed, or
    /// exceeds [`MAX_SESSION_RESUME_HOLD_SECS`].
    pub fn parse_session_resume_hold_secs(message: Option<&str>) -> Option<u64> {
        message
            .and_then(|m| m.trim().strip_prefix(PREFIX))
            .and_then(|list| {
                list.split(',')
                    .find_map(|f| f.trim().strip_prefix(SESSION_RESUME_PREFIX))
            })
            .and_then(|secs| secs.parse::<u64>().ok())
            .filter(|&secs| secs <= MAX_SESSION_RESUME_HOLD_SECS)
    }

    /// The `RegisterAck` message announcing `features`.
    pub fn announcement(features: &[&str]) -> String {
        format!("{PREFIX}{}", features.join(","))
    }

    /// Whether a `RegisterAck`'s `message` announces `feature`.
    pub fn announced(message: Option<&str>, feature: &str) -> bool {
        message
            .and_then(|m| m.trim().strip_prefix(PREFIX))
            .is_some_and(|list| list.split(',').any(|f| f.trim() == feature))
    }
}

impl MasterMessage {
    /// Helper constructor for creating a standard RegisterAck message.
    pub fn register_ack(
        accepted: bool,
        worker_id: Uuid,
        heartbeat_interval_secs: u64,
        features: ProtocolCapabilities,
        message: Option<String>,
    ) -> Self {
        Self::RegisterAck {
            accepted,
            worker_id,
            heartbeat_interval_secs,
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features,
            message,
        }
    }

    /// Returns the static variant name of the message.
    pub fn variant_name(&self) -> &'static str {
        match self {
            Self::RegisterAck { .. } => "RegisterAck",
            Self::HeartbeatAck { .. } => "HeartbeatAck",
            Self::AssignTask { .. } => "AssignTask",
            Self::AssignTaskWithCheckpoint { .. } => "AssignTaskWithCheckpoint",
            Self::CancelTask { .. } => "CancelTask",
            Self::Shutdown { .. } => "Shutdown",
            Self::RoleHandoverAck { .. } => "RoleHandoverAck",
            Self::UnknownVariant { .. } => "UnknownVariant",
            Self::BlobReply { .. } => "BlobReply",
        }
    }
}

/// Messages sent from CLI client (or external RPC) to the Master node.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum ClientMessage {
    /// Submits a task for execution.
    SubmitTask { task: Task, wait: bool },
    /// Queries the status and outcome of a specific task.
    GetTaskStatus { task_id: TaskId },
    /// Requests cancellation of a task.
    CancelTask { task_id: TaskId },
    /// Requests overall cluster metrics.
    ClusterStatus,
    /// Requests list of connected worker nodes.
    ListWorkers,
    /// Submits an in-memory Map/Reduce job for execution.
    SubmitMapReduce {
        job: crate::mapreduce::MapReduceJobSpec,
    },
    /// Polls for the outcome of a task without blocking: answered with
    /// `ClientResponse::TaskCompleted` once the task is terminal and its result is retained,
    /// `ClientResponse::TaskStatusInfo` while it is still in flight, and `ClientResponse::Error`
    /// for an id the master does not know. Appended last so existing positional (bincode)
    /// variant indices do not move.
    GetTaskResult { task_id: TaskId },
    /// A blob request (`crate::blob_transfer`: upload task inputs, download output files),
    /// answered by `ClientResponse::BlobReply`; requests on one connection are answered in order.
    /// A Master built before this variant cannot decode it and closes the connection: restart
    /// the Master first when upgrading. Appended last so existing variant indices do not move.
    BlobRequest {
        request: crate::blob_transfer::BlobRequest,
    },
    /// A lightweight, side-effect-free protocol version probe, answered by
    /// `ClientResponse::Hello`. `connect_master` sends it first on every client connection. A
    /// Master built before protocol version 6 cannot decode it and closes the connection.
    /// Appended last so existing variant indices do not move.
    Hello,
    /// Requests the Master's workspace affinity map, answered by `ClientResponse::Workspaces`.
    /// Appended last so existing variant indices do not move.
    ListWorkspaces,
    /// Requests enrolment of this node's static key on a v3 `enroll` connection (design 0051 D4).
    Enroll {
        invite_id: String,
        proof: crate::noise_auth::RedactedBytes32,
        name: String,
        os: String,
        arch: String,
    },
    /// Creates a new enrollment invite (design 0051 D3; requires `admin` role).
    CreateInvite {
        roles: Vec<crate::noise_auth::MemberRole>,
        max_uses: u32,
        expires_in_secs: u64,
        require_approval: bool,
    },
    /// Lists invites on the Master (design 0051 D3; requires `admin` role).
    ListInvites,
    /// Revokes an invite by ID or prefix (design 0051 D3; requires `admin` role).
    RevokeInvite { invite_id: String },
    /// Lists enrolled members (design 0051 D5; requires `admin` role).
    ListMembers,
    /// Lists pending enrollment requests (design 0051 D5; requires `admin` role).
    ListPendingEnrollments,
    /// Approves a pending enrollment request after verifying the 6-digit pairing check
    /// (design 0051 D4/D5; requires `admin` role).
    ApproveEnrollment {
        pending_id: String,
        pairing_code: crate::noise_auth::RedactedString,
    },
    /// Rejects a pending enrollment request (design 0051 D4/D5; requires `admin` role).
    RejectEnrollment { pending_id: String },
    /// Revokes an enrolled member by ID, prefix, or name (design 0051 D5; requires `admin` role).
    RevokeMember { target: String },
    /// Renames an enrolled member by ID, prefix, or name (design 0051 D5; requires `admin` role).
    RenameMember { target: String, new_name: String },
    /// Releases a non-live `worker_id` claim so an enrolled member can register with that ID
    /// (design 0051 D5/D6; requires `admin` role).
    ReleaseWorkerId { worker_id: Uuid },
}

/// Responses sent from the Master node to the CLI client.
#[derive(Debug, Clone, PartialEq)]
pub enum ClientResponse {
    TaskSubmitted {
        task_id: TaskId,
    },
    TaskCompleted {
        task_id: TaskId,
        result: TaskResult,
    },
    TaskStatusInfo {
        task_id: TaskId,
        status: TaskStatus,
        state_name: String,
        assigned_worker: Option<Uuid>,
        error: Option<String>,
    },
    TaskCancelled {
        task_id: TaskId,
        success: bool,
    },
    ClusterStatus {
        total_tasks: usize,
        pending_tasks: usize,
        running_tasks: usize,
        completed_tasks: usize,
        failed_tasks: usize,
        workers: Vec<WorkerCapabilities>,
        active_bytes: usize,
        max_active_bytes: usize,
        store_error: Option<String>,
    },
    WorkerList {
        workers: Vec<WorkerCapabilities>,
    },
    MapReduceSubmitted {
        job_id: Uuid,
    },
    MapReduceCompleted {
        result: crate::mapreduce::MapReduceResult,
    },
    Error {
        message: String,
    },
    /// The answer to `ClientMessage::BlobRequest`. Appended last so existing variant indices do
    /// not move.
    BlobReply {
        reply: crate::blob_transfer::BlobReply,
    },
    /// The answer to `ClientMessage::Hello`: the oldest protocol version this Master accepts (the
    /// newest is the `protocol_version` every response carries). Appended last so existing
    /// variant indices do not move.
    Hello {
        min_compatible_version: u32,
    },
    /// The answer to `ClientMessage::ListWorkspaces`. Appended last so existing variant indices
    /// do not move.
    Workspaces {
        workspaces: Vec<WorkspaceAffinity>,
    },
    /// Enrollment request queued for owner approval (`oxideswarm members approve <pending_id>`).
    EnrollPending {
        pending_id: String,
    },
    /// Keepalive sent on a pending enrollment connection while waiting for approval.
    EnrollKeepalive,
    /// Enrollment succeeded; the machine's `node.key` is now in `members.json`.
    Enrolled {
        member_id: String,
        roles: Vec<crate::noise_auth::MemberRole>,
    },
    /// Enrollment was refused (invalid/expired/exhausted/revoked invite, bad proof, or rejected).
    EnrollRejected {
        message: String,
    },
    /// Answer to `ClientMessage::CreateInvite`.
    InviteCreated {
        invite_id: String,
        invite_secret: crate::noise_auth::RedactedBytes32,
        roles: Vec<crate::noise_auth::MemberRole>,
        max_uses: u32,
        expires_at: u64,
        require_approval: bool,
        master_key: crate::noise_auth::PublicKey,
        p2p_ticket: Option<String>,
    },
    /// Answer to `ClientMessage::ListInvites`.
    InvitesList {
        invites: Vec<crate::noise_auth::InviteSummary>,
    },
    /// Answer to `ClientMessage::RevokeInvite`.
    InviteRevoked {
        invite_id: String,
    },
    /// Answer to `ClientMessage::ListMembers`.
    MembersList {
        members: Vec<crate::noise_auth::MemberEntry>,
    },
    /// Answer to `ClientMessage::ListPendingEnrollments`.
    PendingEnrollmentsList {
        pending: Vec<crate::noise_auth::PendingEnrollmentInfo>,
    },
    /// Answer to `ClientMessage::ApproveEnrollment`.
    EnrollmentApproved {
        pending_id: String,
        member_id: String,
        name: String,
        roles: Vec<crate::noise_auth::MemberRole>,
    },
    /// Answer to `ClientMessage::RejectEnrollment`.
    EnrollmentRejected {
        pending_id: String,
    },
    /// Answer to `ClientMessage::RevokeMember`.
    MemberRevoked {
        member_id: String,
        disconnected_sessions: usize,
    },
    /// Answer to `ClientMessage::RenameMember`.
    MemberRenamed {
        member_id: String,
        name: String,
    },
    /// Answer to `ClientMessage::ReleaseWorkerId`.
    WorkerIdReleased {
        worker_id: Uuid,
    },
}

/// Unified message discriminator for multiplexed connections on the Master.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum InboundMessage {
    Worker(WorkerMessage),
    Client(ClientMessage),
}
