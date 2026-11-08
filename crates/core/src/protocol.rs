//! Wire protocol message types, length-delimited framing codec, and transport helpers.

use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, CHACHA20_POLY1305};
use ring::hmac;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::{split, AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tokio_util::codec::{Framed, FramedParts, FramedWrite, LengthDelimitedCodec};
use uuid::Uuid;

use crate::capabilities::WorkerCapabilities;
use crate::task::{Bytes as TaskBytes, CheckpointData, Task, TaskId, TaskResult, TaskStatus};

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
pub const PROTOCOL_VERSION_CURRENT: u32 = 6;
/// Minimum wire protocol version compatible with this build.
pub const PROTOCOL_VERSION_MIN_COMPATIBLE: u32 = 6;
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
                | Self::SESSION_RESUME.0,
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

// Error texts of the record layer. They are fixed strings on purpose: callers log them, so they
// must never carry key material, nonces / sequence numbers or plaintext.
const AEAD_ERR_TOO_SHORT: &str = "Encrypted frame too short to be an AEAD record";
const AEAD_ERR_NOT_ENCRYPTED: &str = "Expected encrypted wire format (0x04) after the handshake";
const AEAD_ERR_SEQUENCE: &str =
    "Anti-replay check failed: record out of sequence (replayed, reordered or dropped)";
const AEAD_ERR_INTEGRITY: &str = "Ciphertext integrity verification failed";
const AEAD_ERR_OVERFLOW: &str = "AEAD sequence counter overflow";
const AEAD_ERR_CLOSED: &str = "AEAD session closed after an earlier record failure";

/// Bytes an AEAD record adds to the frame it carries: format byte, 8-byte sequence number and
/// 16-byte Poly1305 tag.
pub const AEAD_RECORD_OVERHEAD: usize = 1 + 8 + 16;

/// AEAD encryption session for sealing outbound frames (one direction of a connection).
///
/// Each record uses the nonce `0x00000000 || seq` (big endian) and authenticates its header as
/// AAD. `seq` starts at 0 and grows by one per record; it never wraps, so a nonce is never used
/// twice under one key.
pub struct AeadWriterSession {
    key: LessSafeKey,
    seq: u64,
}

impl AeadWriterSession {
    /// Creates a new writer session with a 32-byte symmetric key.
    pub fn new(write_key: &[u8; 32]) -> Self {
        let unbound = UnboundKey::new(&CHACHA20_POLY1305, write_key)
            .expect("valid 32-byte key for ChaCha20-Poly1305");
        Self {
            key: LessSafeKey::new(unbound),
            seq: 0,
        }
    }

    /// A writer whose next record carries `seq` (for the counter-overflow tests).
    #[cfg(test)]
    fn starting_at(write_key: &[u8; 32], seq: u64) -> Self {
        Self {
            seq,
            ..Self::new(write_key)
        }
    }

    /// Encrypts `plaintext` into a framed encrypted payload:
    /// `[0x04][seq: 8 bytes BE][ciphertext][16-byte Poly1305 tag]`
    ///
    /// A record that would exceed [`MAX_FRAME_SIZE`] is refused without using up a sequence
    /// number, so the direction stays in sync. Once the counter is exhausted every call fails.
    pub fn encrypt_frame(&mut self, plaintext: &[u8]) -> Result<Bytes, ProtocolError> {
        let size = plaintext.len() + AEAD_RECORD_OVERHEAD;
        if size > MAX_FRAME_SIZE {
            return Err(ProtocolError::FrameTooLarge {
                size,
                max: MAX_FRAME_SIZE,
            });
        }
        let seq = self.seq;
        // The counter only moves once the record is sealed; `u64::MAX` itself is never used.
        let next_seq = seq
            .checked_add(1)
            .ok_or_else(|| ProtocolError::Violation(AEAD_ERR_OVERFLOW.into()))?;

        let mut nonce_bytes = [0u8; 12];
        nonce_bytes[4..12].copy_from_slice(&seq.to_be_bytes());
        let nonce = Nonce::try_assume_unique_for_key(&nonce_bytes)
            .map_err(|_| ProtocolError::Violation("Invalid AEAD nonce".into()))?;

        let mut aad = [0u8; 9];
        aad[0] = WIRE_FORMAT_ENCRYPTED;
        aad[1..9].copy_from_slice(&seq.to_be_bytes());

        let mut in_out = plaintext.to_vec();
        self.key
            .seal_in_place_append_tag(nonce, Aad::from(&aad), &mut in_out)
            .map_err(|_| ProtocolError::Violation("AEAD encryption failure".into()))?;

        let mut frame = Vec::with_capacity(9 + in_out.len());
        frame.extend_from_slice(&aad);
        frame.extend_from_slice(&in_out);

        self.seq = next_seq;
        Ok(Bytes::from(frame))
    }

    /// Returns the next sequence number to be sent.
    pub fn current_seq(&self) -> u64 {
        self.seq
    }
}

/// AEAD decryption session for opening inbound frames (one direction of a connection).
///
/// It accepts exactly the next sequence number, so a replayed, reordered, dropped or injected
/// record is refused, and it fails closed: after the first refused record every later one is
/// refused too, even a genuine one.
pub struct AeadReaderSession {
    key: LessSafeKey,
    expected_seq: u64,
    /// Set by the first refused record.
    failed: bool,
}

impl AeadReaderSession {
    /// Creates a new reader session with a 32-byte symmetric key.
    pub fn new(read_key: &[u8; 32]) -> Self {
        let unbound = UnboundKey::new(&CHACHA20_POLY1305, read_key)
            .expect("valid 32-byte key for ChaCha20-Poly1305");
        Self {
            key: LessSafeKey::new(unbound),
            expected_seq: 0,
            failed: false,
        }
    }

    /// A reader that expects `expected_seq` next (for the counter-overflow tests).
    #[cfg(test)]
    fn starting_at(read_key: &[u8; 32], expected_seq: u64) -> Self {
        Self {
            expected_seq,
            ..Self::new(read_key)
        }
    }

    /// Decrypts a framed encrypted payload:
    /// `[0x04][seq: 8 bytes BE][ciphertext][16-byte tag]`
    /// and returns the decrypted plaintext bytes.
    pub fn decrypt_frame(&mut self, frame: &[u8]) -> Result<Bytes, ProtocolError> {
        if self.failed {
            return Err(ProtocolError::Violation(AEAD_ERR_CLOSED.into()));
        }
        let opened = self.open(frame);
        self.failed = opened.is_err();
        opened
    }

    fn open(&mut self, frame: &[u8]) -> Result<Bytes, ProtocolError> {
        if frame.len() < AEAD_RECORD_OVERHEAD {
            return Err(ProtocolError::Violation(AEAD_ERR_TOO_SHORT.into()));
        }

        if frame[0] != WIRE_FORMAT_ENCRYPTED {
            return Err(ProtocolError::Violation(AEAD_ERR_NOT_ENCRYPTED.into()));
        }

        let seq = u64::from_be_bytes(frame[1..9].try_into().unwrap());
        if seq != self.expected_seq {
            return Err(ProtocolError::Violation(AEAD_ERR_SEQUENCE.into()));
        }

        let mut nonce_bytes = [0u8; 12];
        nonce_bytes[4..12].copy_from_slice(&seq.to_be_bytes());
        let nonce = Nonce::try_assume_unique_for_key(&nonce_bytes)
            .map_err(|_| ProtocolError::Violation("Invalid AEAD nonce".into()))?;

        let aad = &frame[0..9];
        let mut in_out = frame[9..].to_vec();

        let plaintext_slice = self
            .key
            .open_in_place(nonce, Aad::from(aad), &mut in_out)
            .map_err(|_| ProtocolError::Violation(AEAD_ERR_INTEGRITY.into()))?;

        let plaintext = Bytes::copy_from_slice(plaintext_slice);
        self.expected_seq = self
            .expected_seq
            .checked_add(1)
            .ok_or_else(|| ProtocolError::Violation(AEAD_ERR_OVERFLOW.into()))?;

        Ok(plaintext)
    }

    /// Returns the expected sequence number of the next incoming frame.
    pub fn expected_seq(&self) -> u64 {
        self.expected_seq
    }
}

/// Derives directional AEAD keys from the HMAC session key.
pub fn derive_aead_keys(
    session_key: &[u8; 32],
    role: EndpointRole,
) -> (AeadWriterSession, AeadReaderSession) {
    let key = hmac::Key::new(hmac::HMAC_SHA256, session_key);
    let c2s = hmac::sign(&key, b"oxs1-aead-c2s");
    let s2c = hmac::sign(&key, b"oxs1-aead-s2c");

    let mut client_write_key = [0u8; 32];
    client_write_key.copy_from_slice(c2s.as_ref());

    let mut server_write_key = [0u8; 32];
    server_write_key.copy_from_slice(s2c.as_ref());

    match role {
        EndpointRole::Client => (
            AeadWriterSession::new(&client_write_key),
            AeadReaderSession::new(&server_write_key),
        ),
        EndpointRole::Server => (
            AeadWriterSession::new(&server_write_key),
            AeadReaderSession::new(&client_write_key),
        ),
    }
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

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum HumanWorkerMessage {
    Register {
        worker_id: Uuid,
        capabilities: WorkerCapabilities,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
        #[serde(default)]
        features: ProtocolCapabilities,
    },
    Heartbeat {
        worker_id: Uuid,
        timestamp: u64,
        active_tasks: usize,
        #[serde(default)]
        cpu_usage_pct: f32,
        #[serde(default)]
        ram_available_mb: u64,
    },
    TaskProgress {
        worker_id: Uuid,
        task_id: TaskId,
        status: TaskStatus,
    },
    TaskResult {
        worker_id: Uuid,
        task_id: TaskId,
        exit_code: i32,
        stdout: TaskBytes,
        stderr: TaskBytes,
        execution_time_ms: u64,
        is_gpu_executed: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        device_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
        #[serde(
            default,
            with = "crate::task::opt_bytes_serde",
            skip_serializing_if = "Option::is_none"
        )]
        asset_bytes: Option<TaskBytes>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        artifacts: Vec<crate::task::OutputArtifact>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        outputs: Vec<crate::blob::BlobFile>,
    },
    Checkpoint {
        task_id: TaskId,
        sequence: u64,
        delta_state: TaskBytes,
    },
    Disconnecting {
        worker_id: Uuid,
        reason: String,
    },
    MobileTelemetry {
        worker_id: Uuid,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        battery_pct: Option<u8>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        is_charging: Option<bool>,
        #[serde(default)]
        thermal_throttled: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        battery_temperature: Option<f32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        network_type: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        battery_tier: Option<crate::capabilities::BatteryTier>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thermal_tier: Option<crate::capabilities::ThermalTier>,
    },
    ClaimMasterRole {
        ticket: crate::arbitrage::MasterLeaseTicket,
    },
    #[serde(skip_deserializing)]
    UnknownVariant {
        tag: u32,
        raw_bytes: Vec<u8>,
    },
    BlobRequest {
        request_id: u64,
        request: crate::blob_transfer::BlobRequest,
    },
    ResumeSession {
        worker_id: Uuid,
        running: Vec<TaskId>,
    },
}

#[derive(Serialize, Deserialize)]
enum BinaryWorkerMessage {
    Register {
        worker_id: Uuid,
        capabilities: WorkerCapabilities,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
        #[serde(default)]
        features: ProtocolCapabilities,
    },
    Heartbeat {
        worker_id: Uuid,
        timestamp: u64,
        active_tasks: usize,
        cpu_usage_pct: f32,
        ram_available_mb: u64,
    },
    TaskProgress {
        worker_id: Uuid,
        task_id: TaskId,
        status: TaskStatus,
    },
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
        #[serde(default, with = "crate::task::opt_bytes_serde")]
        asset_bytes: Option<TaskBytes>,
        #[serde(default)]
        artifacts: Vec<crate::task::OutputArtifact>,
        #[serde(default)]
        outputs: Vec<crate::blob::BlobFile>,
    },
    Checkpoint {
        task_id: TaskId,
        sequence: u64,
        delta_state: TaskBytes,
    },
    Disconnecting {
        worker_id: Uuid,
        reason: String,
    },
    MobileTelemetry {
        worker_id: Uuid,
        battery_pct: Option<u8>,
        is_charging: Option<bool>,
        thermal_throttled: bool,
        #[serde(default)]
        battery_temperature: Option<f32>,
        #[serde(default)]
        network_type: Option<String>,
        #[serde(default)]
        battery_tier: Option<crate::capabilities::BatteryTier>,
        #[serde(default)]
        thermal_tier: Option<crate::capabilities::ThermalTier>,
    },
    ClaimMasterRole {
        ticket: crate::arbitrage::MasterLeaseTicket,
    },
    UnknownVariant {
        tag: u32,
        raw_bytes: Vec<u8>,
    },
    BlobRequest {
        request_id: u64,
        request: crate::blob_transfer::BlobRequest,
    },
    ResumeSession {
        worker_id: Uuid,
        running: Vec<TaskId>,
    },
}

#[derive(Serialize, Deserialize)]
enum LegacyBinaryWorkerMessage {
    Register {
        worker_id: Uuid,
        capabilities: WorkerCapabilities,
    },
    Heartbeat {
        worker_id: Uuid,
        timestamp: u64,
        active_tasks: usize,
        cpu_usage_pct: f32,
        ram_available_mb: u64,
    },
    TaskProgress {
        worker_id: Uuid,
        task_id: TaskId,
        status: TaskStatus,
    },
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
    },
    Checkpoint {
        task_id: TaskId,
        sequence: u64,
        delta_state: TaskBytes,
    },
    Disconnecting {
        worker_id: Uuid,
        reason: String,
    },
    MobileTelemetry {
        worker_id: Uuid,
        battery_pct: Option<u8>,
        is_charging: Option<bool>,
        thermal_throttled: bool,
    },
}

impl From<LegacyBinaryWorkerMessage> for WorkerMessage {
    fn from(msg: LegacyBinaryWorkerMessage) -> Self {
        match msg {
            LegacyBinaryWorkerMessage::Register {
                worker_id,
                capabilities,
            } => WorkerMessage::Register {
                worker_id,
                capabilities,
                protocol_version: 0,
                features: ProtocolCapabilities::empty(),
            },
            LegacyBinaryWorkerMessage::Heartbeat {
                worker_id,
                timestamp,
                active_tasks,
                cpu_usage_pct,
                ram_available_mb,
            } => WorkerMessage::Heartbeat {
                worker_id,
                timestamp,
                active_tasks,
                cpu_usage_pct,
                ram_available_mb,
            },
            LegacyBinaryWorkerMessage::TaskProgress {
                worker_id,
                task_id,
                status,
            } => WorkerMessage::TaskProgress {
                worker_id,
                task_id,
                status,
            },
            LegacyBinaryWorkerMessage::TaskResult {
                worker_id,
                task_id,
                exit_code,
                stdout,
                stderr,
                execution_time_ms,
                is_gpu_executed,
                device_name,
                error,
            } => WorkerMessage::TaskResult {
                worker_id,
                task_id,
                exit_code,
                stdout,
                stderr,
                execution_time_ms,
                is_gpu_executed,
                device_name,
                error,
                asset_bytes: None,
                artifacts: Vec::new(),
                outputs: Vec::new(),
            },
            LegacyBinaryWorkerMessage::Checkpoint {
                task_id,
                sequence,
                delta_state,
            } => WorkerMessage::Checkpoint {
                task_id,
                sequence,
                delta_state,
            },
            LegacyBinaryWorkerMessage::Disconnecting { worker_id, reason } => {
                WorkerMessage::Disconnecting { worker_id, reason }
            }
            LegacyBinaryWorkerMessage::MobileTelemetry {
                worker_id,
                battery_pct,
                is_charging,
                thermal_throttled,
            } => WorkerMessage::MobileTelemetry {
                worker_id,
                battery_pct,
                is_charging,
                thermal_throttled,
                battery_temperature: None,
                network_type: None,
                battery_tier: None,
                thermal_tier: None,
            },
        }
    }
}

impl From<WorkerMessage> for HumanWorkerMessage {
    fn from(msg: WorkerMessage) -> Self {
        match msg {
            WorkerMessage::Register {
                worker_id,
                capabilities,
                protocol_version,
                features,
            } => HumanWorkerMessage::Register {
                worker_id,
                capabilities,
                protocol_version,
                features,
            },
            WorkerMessage::Heartbeat {
                worker_id,
                timestamp,
                active_tasks,
                cpu_usage_pct,
                ram_available_mb,
            } => HumanWorkerMessage::Heartbeat {
                worker_id,
                timestamp,
                active_tasks,
                cpu_usage_pct,
                ram_available_mb,
            },
            WorkerMessage::TaskProgress {
                worker_id,
                task_id,
                status,
            } => HumanWorkerMessage::TaskProgress {
                worker_id,
                task_id,
                status,
            },
            WorkerMessage::TaskResult {
                worker_id,
                task_id,
                exit_code,
                stdout,
                stderr,
                execution_time_ms,
                is_gpu_executed,
                device_name,
                error,
                asset_bytes,
                artifacts,
                outputs,
            } => HumanWorkerMessage::TaskResult {
                worker_id,
                task_id,
                exit_code,
                stdout,
                stderr,
                execution_time_ms,
                is_gpu_executed,
                device_name,
                error,
                asset_bytes,
                artifacts,
                outputs,
            },
            WorkerMessage::Checkpoint {
                task_id,
                sequence,
                delta_state,
            } => HumanWorkerMessage::Checkpoint {
                task_id,
                sequence,
                delta_state,
            },
            WorkerMessage::Disconnecting { worker_id, reason } => {
                HumanWorkerMessage::Disconnecting { worker_id, reason }
            }
            WorkerMessage::MobileTelemetry {
                worker_id,
                battery_pct,
                is_charging,
                thermal_throttled,
                battery_temperature,
                network_type,
                battery_tier,
                thermal_tier,
            } => HumanWorkerMessage::MobileTelemetry {
                worker_id,
                battery_pct,
                is_charging,
                thermal_throttled,
                battery_temperature,
                network_type,
                battery_tier,
                thermal_tier,
            },
            WorkerMessage::ClaimMasterRole { ticket } => {
                HumanWorkerMessage::ClaimMasterRole { ticket }
            }
            WorkerMessage::UnknownVariant { tag, raw_bytes } => {
                HumanWorkerMessage::UnknownVariant { tag, raw_bytes }
            }
            WorkerMessage::BlobRequest {
                request_id,
                request,
            } => HumanWorkerMessage::BlobRequest {
                request_id,
                request,
            },
            WorkerMessage::ResumeSession { worker_id, running } => {
                HumanWorkerMessage::ResumeSession { worker_id, running }
            }
        }
    }
}

impl From<HumanWorkerMessage> for WorkerMessage {
    fn from(msg: HumanWorkerMessage) -> Self {
        match msg {
            HumanWorkerMessage::Register {
                worker_id,
                capabilities,
                protocol_version,
                features,
            } => WorkerMessage::Register {
                worker_id,
                capabilities,
                protocol_version,
                features,
            },
            HumanWorkerMessage::Heartbeat {
                worker_id,
                timestamp,
                active_tasks,
                cpu_usage_pct,
                ram_available_mb,
            } => WorkerMessage::Heartbeat {
                worker_id,
                timestamp,
                active_tasks,
                cpu_usage_pct,
                ram_available_mb,
            },
            HumanWorkerMessage::TaskProgress {
                worker_id,
                task_id,
                status,
            } => WorkerMessage::TaskProgress {
                worker_id,
                task_id,
                status,
            },
            HumanWorkerMessage::TaskResult {
                worker_id,
                task_id,
                exit_code,
                stdout,
                stderr,
                execution_time_ms,
                is_gpu_executed,
                device_name,
                error,
                asset_bytes,
                artifacts,
                outputs,
            } => WorkerMessage::TaskResult {
                worker_id,
                task_id,
                exit_code,
                stdout,
                stderr,
                execution_time_ms,
                is_gpu_executed,
                device_name,
                error,
                asset_bytes,
                artifacts,
                outputs,
            },
            HumanWorkerMessage::Checkpoint {
                task_id,
                sequence,
                delta_state,
            } => WorkerMessage::Checkpoint {
                task_id,
                sequence,
                delta_state,
            },
            HumanWorkerMessage::Disconnecting { worker_id, reason } => {
                WorkerMessage::Disconnecting { worker_id, reason }
            }
            HumanWorkerMessage::MobileTelemetry {
                worker_id,
                battery_pct,
                is_charging,
                thermal_throttled,
                battery_temperature,
                network_type,
                battery_tier,
                thermal_tier,
            } => WorkerMessage::MobileTelemetry {
                worker_id,
                battery_pct,
                is_charging,
                thermal_throttled,
                battery_temperature,
                network_type,
                battery_tier,
                thermal_tier,
            },
            HumanWorkerMessage::ClaimMasterRole { ticket } => {
                WorkerMessage::ClaimMasterRole { ticket }
            }
            HumanWorkerMessage::UnknownVariant { tag, raw_bytes } => {
                WorkerMessage::UnknownVariant { tag, raw_bytes }
            }
            HumanWorkerMessage::BlobRequest {
                request_id,
                request,
            } => WorkerMessage::BlobRequest {
                request_id,
                request,
            },
            HumanWorkerMessage::ResumeSession { worker_id, running } => {
                WorkerMessage::ResumeSession { worker_id, running }
            }
        }
    }
}

impl From<WorkerMessage> for BinaryWorkerMessage {
    fn from(msg: WorkerMessage) -> Self {
        match msg {
            WorkerMessage::Register {
                worker_id,
                capabilities,
                protocol_version,
                features,
            } => BinaryWorkerMessage::Register {
                worker_id,
                capabilities,
                protocol_version,
                features,
            },
            WorkerMessage::Heartbeat {
                worker_id,
                timestamp,
                active_tasks,
                cpu_usage_pct,
                ram_available_mb,
            } => BinaryWorkerMessage::Heartbeat {
                worker_id,
                timestamp,
                active_tasks,
                cpu_usage_pct,
                ram_available_mb,
            },
            WorkerMessage::TaskProgress {
                worker_id,
                task_id,
                status,
            } => BinaryWorkerMessage::TaskProgress {
                worker_id,
                task_id,
                status,
            },
            WorkerMessage::TaskResult {
                worker_id,
                task_id,
                exit_code,
                stdout,
                stderr,
                execution_time_ms,
                is_gpu_executed,
                device_name,
                error,
                asset_bytes,
                artifacts,
                outputs,
            } => BinaryWorkerMessage::TaskResult {
                worker_id,
                task_id,
                exit_code,
                stdout,
                stderr,
                execution_time_ms,
                is_gpu_executed,
                device_name,
                error,
                asset_bytes,
                artifacts,
                outputs,
            },
            WorkerMessage::Checkpoint {
                task_id,
                sequence,
                delta_state,
            } => BinaryWorkerMessage::Checkpoint {
                task_id,
                sequence,
                delta_state,
            },
            WorkerMessage::Disconnecting { worker_id, reason } => {
                BinaryWorkerMessage::Disconnecting { worker_id, reason }
            }
            WorkerMessage::MobileTelemetry {
                worker_id,
                battery_pct,
                is_charging,
                thermal_throttled,
                battery_temperature,
                network_type,
                battery_tier,
                thermal_tier,
            } => BinaryWorkerMessage::MobileTelemetry {
                worker_id,
                battery_pct,
                is_charging,
                thermal_throttled,
                battery_temperature,
                network_type,
                battery_tier,
                thermal_tier,
            },
            WorkerMessage::ClaimMasterRole { ticket } => {
                BinaryWorkerMessage::ClaimMasterRole { ticket }
            }
            WorkerMessage::UnknownVariant { tag, raw_bytes } => {
                BinaryWorkerMessage::UnknownVariant { tag, raw_bytes }
            }
            WorkerMessage::BlobRequest {
                request_id,
                request,
            } => BinaryWorkerMessage::BlobRequest {
                request_id,
                request,
            },
            WorkerMessage::ResumeSession { worker_id, running } => {
                BinaryWorkerMessage::ResumeSession { worker_id, running }
            }
        }
    }
}

impl From<BinaryWorkerMessage> for WorkerMessage {
    fn from(msg: BinaryWorkerMessage) -> Self {
        match msg {
            BinaryWorkerMessage::Register {
                worker_id,
                capabilities,
                protocol_version,
                features,
            } => WorkerMessage::Register {
                worker_id,
                capabilities,
                protocol_version,
                features,
            },
            BinaryWorkerMessage::Heartbeat {
                worker_id,
                timestamp,
                active_tasks,
                cpu_usage_pct,
                ram_available_mb,
            } => WorkerMessage::Heartbeat {
                worker_id,
                timestamp,
                active_tasks,
                cpu_usage_pct,
                ram_available_mb,
            },
            BinaryWorkerMessage::TaskProgress {
                worker_id,
                task_id,
                status,
            } => WorkerMessage::TaskProgress {
                worker_id,
                task_id,
                status,
            },
            BinaryWorkerMessage::TaskResult {
                worker_id,
                task_id,
                exit_code,
                stdout,
                stderr,
                execution_time_ms,
                is_gpu_executed,
                device_name,
                error,
                asset_bytes,
                artifacts,
                outputs,
            } => WorkerMessage::TaskResult {
                worker_id,
                task_id,
                exit_code,
                stdout,
                stderr,
                execution_time_ms,
                is_gpu_executed,
                device_name,
                error,
                asset_bytes,
                artifacts,
                outputs,
            },
            BinaryWorkerMessage::Checkpoint {
                task_id,
                sequence,
                delta_state,
            } => WorkerMessage::Checkpoint {
                task_id,
                sequence,
                delta_state,
            },
            BinaryWorkerMessage::Disconnecting { worker_id, reason } => {
                WorkerMessage::Disconnecting { worker_id, reason }
            }
            BinaryWorkerMessage::MobileTelemetry {
                worker_id,
                battery_pct,
                is_charging,
                thermal_throttled,
                battery_temperature,
                network_type,
                battery_tier,
                thermal_tier,
            } => WorkerMessage::MobileTelemetry {
                worker_id,
                battery_pct,
                is_charging,
                thermal_throttled,
                battery_temperature,
                network_type,
                battery_tier,
                thermal_tier,
            },
            BinaryWorkerMessage::ClaimMasterRole { ticket } => {
                WorkerMessage::ClaimMasterRole { ticket }
            }
            BinaryWorkerMessage::UnknownVariant { tag, raw_bytes } => {
                WorkerMessage::UnknownVariant { tag, raw_bytes }
            }
            BinaryWorkerMessage::BlobRequest {
                request_id,
                request,
            } => WorkerMessage::BlobRequest {
                request_id,
                request,
            },
            BinaryWorkerMessage::ResumeSession { worker_id, running } => {
                WorkerMessage::ResumeSession { worker_id, running }
            }
        }
    }
}

impl Serialize for WorkerMessage {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            HumanWorkerMessage::from(self.clone()).serialize(serializer)
        } else {
            BinaryWorkerMessage::from(self.clone()).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for WorkerMessage {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            HumanWorkerMessage::deserialize(deserializer).map(Into::into)
        } else {
            BinaryWorkerMessage::deserialize(deserializer).map(Into::into)
        }
    }
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

    /// Every feature this build of the Master supports unconditionally.
    pub const SUPPORTED: &[&str] = &[MOBILE_TELEMETRY];

    const PREFIX: &str = "features=";

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

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum HumanMasterMessage {
    RegisterAck {
        accepted: bool,
        worker_id: Uuid,
        heartbeat_interval_secs: u64,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
        #[serde(default)]
        features: ProtocolCapabilities,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    HeartbeatAck {
        timestamp: u64,
    },
    AssignTask {
        task: Task,
    },
    AssignTaskWithCheckpoint {
        task: Task,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        latest_checkpoint: Option<CheckpointData>,
    },
    CancelTask {
        task_id: TaskId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    Shutdown {
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        grace_period_secs: Option<u64>,
    },
    RoleHandoverAck {
        accepted: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        task_queue_snapshot: Vec<Task>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        connected_workers: Vec<WorkerCapabilities>,
    },
    #[serde(skip_deserializing)]
    UnknownVariant {
        tag: u32,
        raw_bytes: Vec<u8>,
    },
    BlobReply {
        request_id: u64,
        reply: crate::blob_transfer::BlobReply,
    },
}

#[derive(Serialize, Deserialize)]
enum BinaryMasterMessage {
    RegisterAck {
        accepted: bool,
        worker_id: Uuid,
        heartbeat_interval_secs: u64,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
        #[serde(default)]
        features: ProtocolCapabilities,
        message: Option<String>,
    },
    HeartbeatAck {
        timestamp: u64,
    },
    AssignTask {
        task: Task,
    },
    AssignTaskWithCheckpoint {
        task: Task,
        latest_checkpoint: Option<CheckpointData>,
    },
    CancelTask {
        task_id: TaskId,
        reason: Option<String>,
    },
    Shutdown {
        reason: String,
        grace_period_secs: Option<u64>,
    },
    RoleHandoverAck {
        accepted: bool,
        reason: Option<String>,
        task_queue_snapshot: Vec<Task>,
        connected_workers: Vec<WorkerCapabilities>,
    },
    UnknownVariant {
        tag: u32,
        raw_bytes: Vec<u8>,
    },
    BlobReply {
        request_id: u64,
        reply: crate::blob_transfer::BlobReply,
    },
}

#[derive(Serialize, Deserialize)]
enum LegacyBinaryMasterMessage {
    RegisterAck {
        accepted: bool,
        worker_id: Uuid,
        heartbeat_interval_secs: u64,
        message: Option<String>,
    },
    HeartbeatAck {
        timestamp: u64,
    },
    AssignTask {
        task: Task,
    },
    AssignTaskWithCheckpoint {
        task: Task,
        latest_checkpoint: Option<CheckpointData>,
    },
    CancelTask {
        task_id: TaskId,
        reason: Option<String>,
    },
    Shutdown {
        reason: String,
        grace_period_secs: Option<u64>,
    },
}

impl From<LegacyBinaryMasterMessage> for MasterMessage {
    fn from(msg: LegacyBinaryMasterMessage) -> Self {
        match msg {
            LegacyBinaryMasterMessage::RegisterAck {
                accepted,
                worker_id,
                heartbeat_interval_secs,
                message,
            } => MasterMessage::RegisterAck {
                accepted,
                worker_id,
                heartbeat_interval_secs,
                protocol_version: 0,
                features: ProtocolCapabilities::empty(),
                message,
            },
            LegacyBinaryMasterMessage::HeartbeatAck { timestamp } => {
                MasterMessage::HeartbeatAck { timestamp }
            }
            LegacyBinaryMasterMessage::AssignTask { task } => MasterMessage::AssignTask { task },
            LegacyBinaryMasterMessage::AssignTaskWithCheckpoint {
                task,
                latest_checkpoint,
            } => MasterMessage::AssignTaskWithCheckpoint {
                task,
                latest_checkpoint,
            },
            LegacyBinaryMasterMessage::CancelTask { task_id, reason } => {
                MasterMessage::CancelTask { task_id, reason }
            }
            LegacyBinaryMasterMessage::Shutdown {
                reason,
                grace_period_secs,
            } => MasterMessage::Shutdown {
                reason,
                grace_period_secs,
            },
        }
    }
}

impl From<MasterMessage> for HumanMasterMessage {
    fn from(msg: MasterMessage) -> Self {
        match msg {
            MasterMessage::RegisterAck {
                accepted,
                worker_id,
                heartbeat_interval_secs,
                protocol_version,
                features,
                message,
            } => HumanMasterMessage::RegisterAck {
                accepted,
                worker_id,
                heartbeat_interval_secs,
                protocol_version,
                features,
                message,
            },
            MasterMessage::HeartbeatAck { timestamp } => {
                HumanMasterMessage::HeartbeatAck { timestamp }
            }
            MasterMessage::AssignTask { task } => HumanMasterMessage::AssignTask { task },
            MasterMessage::AssignTaskWithCheckpoint {
                task,
                latest_checkpoint,
            } => HumanMasterMessage::AssignTaskWithCheckpoint {
                task,
                latest_checkpoint,
            },
            MasterMessage::CancelTask { task_id, reason } => {
                HumanMasterMessage::CancelTask { task_id, reason }
            }
            MasterMessage::Shutdown {
                reason,
                grace_period_secs,
            } => HumanMasterMessage::Shutdown {
                reason,
                grace_period_secs,
            },
            MasterMessage::RoleHandoverAck {
                accepted,
                reason,
                task_queue_snapshot,
                connected_workers,
            } => HumanMasterMessage::RoleHandoverAck {
                accepted,
                reason,
                task_queue_snapshot,
                connected_workers,
            },
            MasterMessage::UnknownVariant { tag, raw_bytes } => {
                HumanMasterMessage::UnknownVariant { tag, raw_bytes }
            }
            MasterMessage::BlobReply { request_id, reply } => {
                HumanMasterMessage::BlobReply { request_id, reply }
            }
        }
    }
}

impl From<HumanMasterMessage> for MasterMessage {
    fn from(msg: HumanMasterMessage) -> Self {
        match msg {
            HumanMasterMessage::RegisterAck {
                accepted,
                worker_id,
                heartbeat_interval_secs,
                protocol_version,
                features,
                message,
            } => MasterMessage::RegisterAck {
                accepted,
                worker_id,
                heartbeat_interval_secs,
                protocol_version,
                features,
                message,
            },
            HumanMasterMessage::HeartbeatAck { timestamp } => {
                MasterMessage::HeartbeatAck { timestamp }
            }
            HumanMasterMessage::AssignTask { task } => MasterMessage::AssignTask { task },
            HumanMasterMessage::AssignTaskWithCheckpoint {
                task,
                latest_checkpoint,
            } => MasterMessage::AssignTaskWithCheckpoint {
                task,
                latest_checkpoint,
            },
            HumanMasterMessage::CancelTask { task_id, reason } => {
                MasterMessage::CancelTask { task_id, reason }
            }
            HumanMasterMessage::Shutdown {
                reason,
                grace_period_secs,
            } => MasterMessage::Shutdown {
                reason,
                grace_period_secs,
            },
            HumanMasterMessage::RoleHandoverAck {
                accepted,
                reason,
                task_queue_snapshot,
                connected_workers,
            } => MasterMessage::RoleHandoverAck {
                accepted,
                reason,
                task_queue_snapshot,
                connected_workers,
            },
            HumanMasterMessage::UnknownVariant { tag, raw_bytes } => {
                MasterMessage::UnknownVariant { tag, raw_bytes }
            }
            HumanMasterMessage::BlobReply { request_id, reply } => {
                MasterMessage::BlobReply { request_id, reply }
            }
        }
    }
}

impl From<MasterMessage> for BinaryMasterMessage {
    fn from(msg: MasterMessage) -> Self {
        match msg {
            MasterMessage::RegisterAck {
                accepted,
                worker_id,
                heartbeat_interval_secs,
                protocol_version,
                features,
                message,
            } => BinaryMasterMessage::RegisterAck {
                accepted,
                worker_id,
                heartbeat_interval_secs,
                protocol_version,
                features,
                message,
            },
            MasterMessage::HeartbeatAck { timestamp } => {
                BinaryMasterMessage::HeartbeatAck { timestamp }
            }
            MasterMessage::AssignTask { task } => BinaryMasterMessage::AssignTask { task },
            MasterMessage::AssignTaskWithCheckpoint {
                task,
                latest_checkpoint,
            } => BinaryMasterMessage::AssignTaskWithCheckpoint {
                task,
                latest_checkpoint,
            },
            MasterMessage::CancelTask { task_id, reason } => {
                BinaryMasterMessage::CancelTask { task_id, reason }
            }
            MasterMessage::Shutdown {
                reason,
                grace_period_secs,
            } => BinaryMasterMessage::Shutdown {
                reason,
                grace_period_secs,
            },
            MasterMessage::RoleHandoverAck {
                accepted,
                reason,
                task_queue_snapshot,
                connected_workers,
            } => BinaryMasterMessage::RoleHandoverAck {
                accepted,
                reason,
                task_queue_snapshot,
                connected_workers,
            },
            MasterMessage::UnknownVariant { tag, raw_bytes } => {
                BinaryMasterMessage::UnknownVariant { tag, raw_bytes }
            }
            MasterMessage::BlobReply { request_id, reply } => {
                BinaryMasterMessage::BlobReply { request_id, reply }
            }
        }
    }
}

impl From<BinaryMasterMessage> for MasterMessage {
    fn from(msg: BinaryMasterMessage) -> Self {
        match msg {
            BinaryMasterMessage::RegisterAck {
                accepted,
                worker_id,
                heartbeat_interval_secs,
                protocol_version,
                features,
                message,
            } => MasterMessage::RegisterAck {
                accepted,
                worker_id,
                heartbeat_interval_secs,
                protocol_version,
                features,
                message,
            },
            BinaryMasterMessage::HeartbeatAck { timestamp } => {
                MasterMessage::HeartbeatAck { timestamp }
            }
            BinaryMasterMessage::AssignTask { task } => MasterMessage::AssignTask { task },
            BinaryMasterMessage::AssignTaskWithCheckpoint {
                task,
                latest_checkpoint,
            } => MasterMessage::AssignTaskWithCheckpoint {
                task,
                latest_checkpoint,
            },
            BinaryMasterMessage::CancelTask { task_id, reason } => {
                MasterMessage::CancelTask { task_id, reason }
            }
            BinaryMasterMessage::Shutdown {
                reason,
                grace_period_secs,
            } => MasterMessage::Shutdown {
                reason,
                grace_period_secs,
            },
            BinaryMasterMessage::RoleHandoverAck {
                accepted,
                reason,
                task_queue_snapshot,
                connected_workers,
            } => MasterMessage::RoleHandoverAck {
                accepted,
                reason,
                task_queue_snapshot,
                connected_workers,
            },
            BinaryMasterMessage::UnknownVariant { tag, raw_bytes } => {
                MasterMessage::UnknownVariant { tag, raw_bytes }
            }
            BinaryMasterMessage::BlobReply { request_id, reply } => {
                MasterMessage::BlobReply { request_id, reply }
            }
        }
    }
}

impl Serialize for MasterMessage {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            HumanMasterMessage::from(self.clone()).serialize(serializer)
        } else {
            BinaryMasterMessage::from(self.clone()).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for MasterMessage {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            HumanMasterMessage::deserialize(deserializer).map(Into::into)
        } else {
            BinaryMasterMessage::deserialize(deserializer).map(Into::into)
        }
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
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
#[allow(clippy::large_enum_variant)]
enum HumanClientMessage {
    SubmitTask {
        task: Task,
        #[serde(default)]
        wait: bool,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    GetTaskStatus {
        task_id: TaskId,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    CancelTask {
        task_id: TaskId,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    ClusterStatus {
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    ListWorkers {
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    SubmitMapReduce {
        job: crate::mapreduce::MapReduceJobSpec,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    GetTaskResult {
        task_id: TaskId,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    BlobRequest {
        request: crate::blob_transfer::BlobRequest,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    Hello {
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
}

#[derive(Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
enum BinaryClientMessage {
    SubmitTask {
        task: Task,
        wait: bool,
        protocol_version: u32,
    },
    GetTaskStatus {
        task_id: TaskId,
        protocol_version: u32,
    },
    CancelTask {
        task_id: TaskId,
        protocol_version: u32,
    },
    ClusterStatus {
        protocol_version: u32,
    },
    ListWorkers {
        protocol_version: u32,
    },
    SubmitMapReduce {
        job: crate::mapreduce::MapReduceJobSpec,
        protocol_version: u32,
    },
    GetTaskResult {
        task_id: TaskId,
        protocol_version: u32,
    },
    BlobRequest {
        request: crate::blob_transfer::BlobRequest,
        protocol_version: u32,
    },
    Hello {
        protocol_version: u32,
    },
}

/// Pre-v3 (protocol version 2) positional bincode layout of [`ClientMessage`], which carried no
/// `protocol_version` trailer. Used only so a v3 Master can recognize an older v2 client frame and
/// refuse it with [`ProtocolError::IncompatibleVersion`] instead of an opaque bincode EOF error.
#[derive(Deserialize)]
#[allow(clippy::large_enum_variant, dead_code)]
enum LegacyV2BinaryClientMessage {
    SubmitTask {
        task: Task,
        wait: bool,
    },
    GetTaskStatus {
        task_id: TaskId,
    },
    CancelTask {
        task_id: TaskId,
    },
    ClusterStatus,
    ListWorkers,
    SubmitMapReduce {
        job: crate::mapreduce::MapReduceJobSpec,
    },
    GetTaskResult {
        task_id: TaskId,
    },
    BlobRequest {
        request: crate::blob_transfer::BlobRequest,
    },
}

/// Protocol v3 to v5 positional bincode layout of [`Task`], which ended at `output_globs` before
/// `workspace` was appended in v6. Used only to read a v5 `SubmitTask` far enough to recognise it.
#[derive(Deserialize)]
#[allow(dead_code)]
struct LegacyV5Task {
    id: crate::task::TaskId,
    spec: crate::task::TaskSpec,
    requirements: crate::task::TaskRequirements,
    created_at_utc: u64,
    tags: Vec<String>,
    latest_checkpoint: Option<crate::task::CheckpointData>,
    staged_files: Vec<crate::task::StagedFile>,
    expected_outputs: Vec<String>,
    inputs: Vec<crate::blob::TaskInput>,
    output_globs: Vec<String>,
}

/// Protocol v3 to v5 positional bincode layout of [`ClientMessage`]: the same variants as v6
/// except `Hello`, but `SubmitTask` carries a [`LegacyV5Task`]. Used only so a v6 Master can
/// refuse a v5 client frame with [`ProtocolError::IncompatibleVersion`] instead of an opaque
/// bincode error. Every other variant has the v6 layout and is refused by the version check of
/// [`BinaryClientMessage`] itself.
#[derive(Deserialize)]
#[allow(clippy::large_enum_variant, dead_code)]
enum LegacyV5BinaryClientMessage {
    SubmitTask {
        task: LegacyV5Task,
        wait: bool,
        protocol_version: u32,
    },
}

impl HumanClientMessage {
    fn from_client_message(msg: ClientMessage, protocol_version: u32) -> Self {
        match msg {
            ClientMessage::SubmitTask { task, wait } => HumanClientMessage::SubmitTask {
                task,
                wait,
                protocol_version,
            },
            ClientMessage::GetTaskStatus { task_id } => HumanClientMessage::GetTaskStatus {
                task_id,
                protocol_version,
            },
            ClientMessage::CancelTask { task_id } => HumanClientMessage::CancelTask {
                task_id,
                protocol_version,
            },
            ClientMessage::ClusterStatus => HumanClientMessage::ClusterStatus { protocol_version },
            ClientMessage::ListWorkers => HumanClientMessage::ListWorkers { protocol_version },
            ClientMessage::SubmitMapReduce { job } => HumanClientMessage::SubmitMapReduce {
                job,
                protocol_version,
            },
            ClientMessage::GetTaskResult { task_id } => HumanClientMessage::GetTaskResult {
                task_id,
                protocol_version,
            },
            ClientMessage::BlobRequest { request } => HumanClientMessage::BlobRequest {
                request,
                protocol_version,
            },
            ClientMessage::Hello => HumanClientMessage::Hello { protocol_version },
        }
    }

    const fn protocol_version(&self) -> u32 {
        match self {
            Self::SubmitTask {
                protocol_version, ..
            }
            | Self::GetTaskStatus {
                protocol_version, ..
            }
            | Self::CancelTask {
                protocol_version, ..
            }
            | Self::ClusterStatus { protocol_version }
            | Self::ListWorkers { protocol_version }
            | Self::SubmitMapReduce {
                protocol_version, ..
            }
            | Self::GetTaskResult {
                protocol_version, ..
            }
            | Self::BlobRequest {
                protocol_version, ..
            }
            | Self::Hello { protocol_version } => *protocol_version,
        }
    }
}

impl BinaryClientMessage {
    fn from_client_message(msg: ClientMessage, protocol_version: u32) -> Self {
        match msg {
            ClientMessage::SubmitTask { task, wait } => BinaryClientMessage::SubmitTask {
                task,
                wait,
                protocol_version,
            },
            ClientMessage::GetTaskStatus { task_id } => BinaryClientMessage::GetTaskStatus {
                task_id,
                protocol_version,
            },
            ClientMessage::CancelTask { task_id } => BinaryClientMessage::CancelTask {
                task_id,
                protocol_version,
            },
            ClientMessage::ClusterStatus => BinaryClientMessage::ClusterStatus { protocol_version },
            ClientMessage::ListWorkers => BinaryClientMessage::ListWorkers { protocol_version },
            ClientMessage::SubmitMapReduce { job } => BinaryClientMessage::SubmitMapReduce {
                job,
                protocol_version,
            },
            ClientMessage::GetTaskResult { task_id } => BinaryClientMessage::GetTaskResult {
                task_id,
                protocol_version,
            },
            ClientMessage::BlobRequest { request } => BinaryClientMessage::BlobRequest {
                request,
                protocol_version,
            },
            ClientMessage::Hello => BinaryClientMessage::Hello { protocol_version },
        }
    }

    const fn protocol_version(&self) -> u32 {
        match self {
            Self::SubmitTask {
                protocol_version, ..
            }
            | Self::GetTaskStatus {
                protocol_version, ..
            }
            | Self::CancelTask {
                protocol_version, ..
            }
            | Self::ClusterStatus { protocol_version }
            | Self::ListWorkers { protocol_version }
            | Self::SubmitMapReduce {
                protocol_version, ..
            }
            | Self::GetTaskResult {
                protocol_version, ..
            }
            | Self::BlobRequest {
                protocol_version, ..
            }
            | Self::Hello { protocol_version } => *protocol_version,
        }
    }
}

impl From<ClientMessage> for HumanClientMessage {
    fn from(msg: ClientMessage) -> Self {
        Self::from_client_message(msg, PROTOCOL_VERSION_CURRENT)
    }
}

impl From<HumanClientMessage> for ClientMessage {
    fn from(msg: HumanClientMessage) -> Self {
        match msg {
            HumanClientMessage::SubmitTask { task, wait, .. } => {
                ClientMessage::SubmitTask { task, wait }
            }
            HumanClientMessage::GetTaskStatus { task_id, .. } => {
                ClientMessage::GetTaskStatus { task_id }
            }
            HumanClientMessage::CancelTask { task_id, .. } => ClientMessage::CancelTask { task_id },
            HumanClientMessage::ClusterStatus { .. } => ClientMessage::ClusterStatus,
            HumanClientMessage::ListWorkers { .. } => ClientMessage::ListWorkers,
            HumanClientMessage::SubmitMapReduce { job, .. } => {
                ClientMessage::SubmitMapReduce { job }
            }
            HumanClientMessage::GetTaskResult { task_id, .. } => {
                ClientMessage::GetTaskResult { task_id }
            }
            HumanClientMessage::BlobRequest { request, .. } => {
                ClientMessage::BlobRequest { request }
            }
            HumanClientMessage::Hello { .. } => ClientMessage::Hello,
        }
    }
}

impl From<ClientMessage> for BinaryClientMessage {
    fn from(msg: ClientMessage) -> Self {
        Self::from_client_message(msg, PROTOCOL_VERSION_CURRENT)
    }
}

impl From<BinaryClientMessage> for ClientMessage {
    fn from(msg: BinaryClientMessage) -> Self {
        match msg {
            BinaryClientMessage::SubmitTask { task, wait, .. } => {
                ClientMessage::SubmitTask { task, wait }
            }
            BinaryClientMessage::GetTaskStatus { task_id, .. } => {
                ClientMessage::GetTaskStatus { task_id }
            }
            BinaryClientMessage::CancelTask { task_id, .. } => {
                ClientMessage::CancelTask { task_id }
            }
            BinaryClientMessage::ClusterStatus { .. } => ClientMessage::ClusterStatus,
            BinaryClientMessage::ListWorkers { .. } => ClientMessage::ListWorkers,
            BinaryClientMessage::SubmitMapReduce { job, .. } => {
                ClientMessage::SubmitMapReduce { job }
            }
            BinaryClientMessage::GetTaskResult { task_id, .. } => {
                ClientMessage::GetTaskResult { task_id }
            }
            BinaryClientMessage::BlobRequest { request, .. } => {
                ClientMessage::BlobRequest { request }
            }
            BinaryClientMessage::Hello { .. } => ClientMessage::Hello,
        }
    }
}

impl Serialize for ClientMessage {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            HumanClientMessage::from(self.clone()).serialize(serializer)
        } else {
            BinaryClientMessage::from(self.clone()).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for ClientMessage {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            HumanClientMessage::deserialize(deserializer).map(Into::into)
        } else {
            BinaryClientMessage::deserialize(deserializer).map(Into::into)
        }
    }
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
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum HumanClientResponse {
    TaskSubmitted {
        task_id: TaskId,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    TaskCompleted {
        task_id: TaskId,
        result: TaskResult,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    TaskStatusInfo {
        task_id: TaskId,
        status: TaskStatus,
        state_name: String,
        assigned_worker: Option<Uuid>,
        error: Option<String>,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    TaskCancelled {
        task_id: TaskId,
        success: bool,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    ClusterStatus {
        total_tasks: usize,
        pending_tasks: usize,
        running_tasks: usize,
        completed_tasks: usize,
        failed_tasks: usize,
        workers: Vec<WorkerCapabilities>,
        #[serde(default)]
        active_bytes: usize,
        #[serde(default)]
        max_active_bytes: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        store_error: Option<String>,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    WorkerList {
        workers: Vec<WorkerCapabilities>,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    MapReduceSubmitted {
        job_id: Uuid,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    MapReduceCompleted {
        result: crate::mapreduce::MapReduceResult,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    Error {
        message: String,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    /// The answer to `ClientMessage::BlobRequest`. Appended last so existing variant indices do
    /// not move.
    BlobReply {
        reply: crate::blob_transfer::BlobReply,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    Hello {
        min_compatible_version: u32,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
}

#[derive(Serialize, Deserialize)]
enum BinaryClientResponse {
    TaskSubmitted {
        task_id: TaskId,
        protocol_version: u32,
    },
    TaskCompleted {
        task_id: TaskId,
        result: TaskResult,
        protocol_version: u32,
    },
    TaskStatusInfo {
        task_id: TaskId,
        status: TaskStatus,
        state_name: String,
        assigned_worker: Option<Uuid>,
        error: Option<String>,
        protocol_version: u32,
    },
    TaskCancelled {
        task_id: TaskId,
        success: bool,
        protocol_version: u32,
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
        protocol_version: u32,
    },
    WorkerList {
        workers: Vec<WorkerCapabilities>,
        protocol_version: u32,
    },
    MapReduceSubmitted {
        job_id: Uuid,
        protocol_version: u32,
    },
    MapReduceCompleted {
        result: crate::mapreduce::MapReduceResult,
        protocol_version: u32,
    },
    Error {
        message: String,
        protocol_version: u32,
    },
    /// The answer to `ClientMessage::BlobRequest`. Appended last so existing variant indices do
    /// not move.
    BlobReply {
        reply: crate::blob_transfer::BlobReply,
        protocol_version: u32,
    },
    Hello {
        min_compatible_version: u32,
        protocol_version: u32,
    },
}

/// Pre-v3 (protocol version 2) positional bincode layout of [`ClientResponse`], which carried no
/// `protocol_version` trailer. Used so a v3 client can recognize an older v2 Master reply and
/// report [`ProtocolError::IncompatibleVersion`] instead of an opaque bincode EOF error.
#[derive(Deserialize)]
#[allow(dead_code)]
enum LegacyV2BinaryClientResponse {
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
    BlobReply {
        reply: crate::blob_transfer::BlobReply,
    },
}

/// Protocol v3/v4 positional bincode layout of [`ClientResponse::ClusterStatus`], which carried
/// `protocol_version` before `store_error` was added in v5.
#[derive(Deserialize)]
#[allow(dead_code)]
enum LegacyV3V4BinaryClientResponse {
    TaskSubmitted {
        task_id: TaskId,
        protocol_version: u32,
    },
    TaskCompleted {
        task_id: TaskId,
        result: TaskResult,
        protocol_version: u32,
    },
    TaskStatusInfo {
        task_id: TaskId,
        status: TaskStatus,
        state_name: String,
        assigned_worker: Option<Uuid>,
        error: Option<String>,
        protocol_version: u32,
    },
    TaskCancelled {
        task_id: TaskId,
        success: bool,
        protocol_version: u32,
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
        protocol_version: u32,
    },
}

impl HumanClientResponse {
    fn from_client_response(msg: ClientResponse, protocol_version: u32) -> Self {
        match msg {
            ClientResponse::TaskSubmitted { task_id } => HumanClientResponse::TaskSubmitted {
                task_id,
                protocol_version,
            },
            ClientResponse::TaskCompleted { task_id, result } => {
                HumanClientResponse::TaskCompleted {
                    task_id,
                    result,
                    protocol_version,
                }
            }
            ClientResponse::TaskStatusInfo {
                task_id,
                status,
                state_name,
                assigned_worker,
                error,
            } => HumanClientResponse::TaskStatusInfo {
                task_id,
                status,
                state_name,
                assigned_worker,
                error,
                protocol_version,
            },
            ClientResponse::TaskCancelled { task_id, success } => {
                HumanClientResponse::TaskCancelled {
                    task_id,
                    success,
                    protocol_version,
                }
            }
            ClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                workers,
                active_bytes,
                max_active_bytes,
                store_error,
            } => HumanClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                workers,
                active_bytes,
                max_active_bytes,
                store_error,
                protocol_version,
            },
            ClientResponse::WorkerList { workers } => HumanClientResponse::WorkerList {
                workers,
                protocol_version,
            },
            ClientResponse::MapReduceSubmitted { job_id } => {
                HumanClientResponse::MapReduceSubmitted {
                    job_id,
                    protocol_version,
                }
            }
            ClientResponse::MapReduceCompleted { result } => {
                HumanClientResponse::MapReduceCompleted {
                    result,
                    protocol_version,
                }
            }
            ClientResponse::Error { message } => HumanClientResponse::Error {
                message,
                protocol_version,
            },
            ClientResponse::BlobReply { reply } => HumanClientResponse::BlobReply {
                reply,
                protocol_version,
            },
            ClientResponse::Hello {
                min_compatible_version,
            } => HumanClientResponse::Hello {
                min_compatible_version,
                protocol_version,
            },
        }
    }

    const fn protocol_version(&self) -> u32 {
        match self {
            Self::TaskSubmitted {
                protocol_version, ..
            }
            | Self::TaskCompleted {
                protocol_version, ..
            }
            | Self::TaskStatusInfo {
                protocol_version, ..
            }
            | Self::TaskCancelled {
                protocol_version, ..
            }
            | Self::ClusterStatus {
                protocol_version, ..
            }
            | Self::WorkerList {
                protocol_version, ..
            }
            | Self::MapReduceSubmitted {
                protocol_version, ..
            }
            | Self::MapReduceCompleted {
                protocol_version, ..
            }
            | Self::Error {
                protocol_version, ..
            }
            | Self::BlobReply {
                protocol_version, ..
            }
            | Self::Hello {
                protocol_version, ..
            } => *protocol_version,
        }
    }

    const fn is_error(&self) -> bool {
        matches!(self, Self::Error { .. })
    }
}

impl BinaryClientResponse {
    fn from_client_response(msg: ClientResponse, protocol_version: u32) -> Self {
        match msg {
            ClientResponse::TaskSubmitted { task_id } => BinaryClientResponse::TaskSubmitted {
                task_id,
                protocol_version,
            },
            ClientResponse::TaskCompleted { task_id, result } => {
                BinaryClientResponse::TaskCompleted {
                    task_id,
                    result,
                    protocol_version,
                }
            }
            ClientResponse::TaskStatusInfo {
                task_id,
                status,
                state_name,
                assigned_worker,
                error,
            } => BinaryClientResponse::TaskStatusInfo {
                task_id,
                status,
                state_name,
                assigned_worker,
                error,
                protocol_version,
            },
            ClientResponse::TaskCancelled { task_id, success } => {
                BinaryClientResponse::TaskCancelled {
                    task_id,
                    success,
                    protocol_version,
                }
            }
            ClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                workers,
                active_bytes,
                max_active_bytes,
                store_error,
            } => BinaryClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                workers,
                active_bytes,
                max_active_bytes,
                store_error,
                protocol_version,
            },
            ClientResponse::WorkerList { workers } => BinaryClientResponse::WorkerList {
                workers,
                protocol_version,
            },
            ClientResponse::MapReduceSubmitted { job_id } => {
                BinaryClientResponse::MapReduceSubmitted {
                    job_id,
                    protocol_version,
                }
            }
            ClientResponse::MapReduceCompleted { result } => {
                BinaryClientResponse::MapReduceCompleted {
                    result,
                    protocol_version,
                }
            }
            ClientResponse::Error { message } => BinaryClientResponse::Error {
                message,
                protocol_version,
            },
            ClientResponse::BlobReply { reply } => BinaryClientResponse::BlobReply {
                reply,
                protocol_version,
            },
            ClientResponse::Hello {
                min_compatible_version,
            } => BinaryClientResponse::Hello {
                min_compatible_version,
                protocol_version,
            },
        }
    }

    const fn protocol_version(&self) -> u32 {
        match self {
            Self::TaskSubmitted {
                protocol_version, ..
            }
            | Self::TaskCompleted {
                protocol_version, ..
            }
            | Self::TaskStatusInfo {
                protocol_version, ..
            }
            | Self::TaskCancelled {
                protocol_version, ..
            }
            | Self::ClusterStatus {
                protocol_version, ..
            }
            | Self::WorkerList {
                protocol_version, ..
            }
            | Self::MapReduceSubmitted {
                protocol_version, ..
            }
            | Self::MapReduceCompleted {
                protocol_version, ..
            }
            | Self::Error {
                protocol_version, ..
            }
            | Self::BlobReply {
                protocol_version, ..
            }
            | Self::Hello {
                protocol_version, ..
            } => *protocol_version,
        }
    }

    const fn is_error(&self) -> bool {
        matches!(self, Self::Error { .. })
    }
}

impl From<ClientResponse> for HumanClientResponse {
    fn from(msg: ClientResponse) -> Self {
        Self::from_client_response(msg, PROTOCOL_VERSION_CURRENT)
    }
}

impl From<HumanClientResponse> for ClientResponse {
    fn from(msg: HumanClientResponse) -> Self {
        match msg {
            HumanClientResponse::TaskSubmitted { task_id, .. } => {
                ClientResponse::TaskSubmitted { task_id }
            }
            HumanClientResponse::TaskCompleted {
                task_id, result, ..
            } => ClientResponse::TaskCompleted { task_id, result },
            HumanClientResponse::TaskStatusInfo {
                task_id,
                status,
                state_name,
                assigned_worker,
                error,
                ..
            } => ClientResponse::TaskStatusInfo {
                task_id,
                status,
                state_name,
                assigned_worker,
                error,
            },
            HumanClientResponse::TaskCancelled {
                task_id, success, ..
            } => ClientResponse::TaskCancelled { task_id, success },
            HumanClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                workers,
                active_bytes,
                max_active_bytes,
                store_error,
                ..
            } => ClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                workers,
                active_bytes,
                max_active_bytes,
                store_error,
            },
            HumanClientResponse::WorkerList { workers, .. } => {
                ClientResponse::WorkerList { workers }
            }
            HumanClientResponse::MapReduceSubmitted { job_id, .. } => {
                ClientResponse::MapReduceSubmitted { job_id }
            }
            HumanClientResponse::MapReduceCompleted { result, .. } => {
                ClientResponse::MapReduceCompleted { result }
            }
            HumanClientResponse::Error { message, .. } => ClientResponse::Error { message },
            HumanClientResponse::BlobReply { reply, .. } => ClientResponse::BlobReply { reply },
            HumanClientResponse::Hello {
                min_compatible_version,
                ..
            } => ClientResponse::Hello {
                min_compatible_version,
            },
        }
    }
}

impl From<ClientResponse> for BinaryClientResponse {
    fn from(msg: ClientResponse) -> Self {
        Self::from_client_response(msg, PROTOCOL_VERSION_CURRENT)
    }
}

impl From<BinaryClientResponse> for ClientResponse {
    fn from(msg: BinaryClientResponse) -> Self {
        match msg {
            BinaryClientResponse::TaskSubmitted { task_id, .. } => {
                ClientResponse::TaskSubmitted { task_id }
            }
            BinaryClientResponse::TaskCompleted {
                task_id, result, ..
            } => ClientResponse::TaskCompleted { task_id, result },
            BinaryClientResponse::TaskStatusInfo {
                task_id,
                status,
                state_name,
                assigned_worker,
                error,
                ..
            } => ClientResponse::TaskStatusInfo {
                task_id,
                status,
                state_name,
                assigned_worker,
                error,
            },
            BinaryClientResponse::TaskCancelled {
                task_id, success, ..
            } => ClientResponse::TaskCancelled { task_id, success },
            BinaryClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                workers,
                active_bytes,
                max_active_bytes,
                store_error,
                ..
            } => ClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                workers,
                active_bytes,
                max_active_bytes,
                store_error,
            },
            BinaryClientResponse::WorkerList { workers, .. } => {
                ClientResponse::WorkerList { workers }
            }
            BinaryClientResponse::MapReduceSubmitted { job_id, .. } => {
                ClientResponse::MapReduceSubmitted { job_id }
            }
            BinaryClientResponse::MapReduceCompleted { result, .. } => {
                ClientResponse::MapReduceCompleted { result }
            }
            BinaryClientResponse::Error { message, .. } => ClientResponse::Error { message },
            BinaryClientResponse::BlobReply { reply, .. } => ClientResponse::BlobReply { reply },
            BinaryClientResponse::Hello {
                min_compatible_version,
                ..
            } => ClientResponse::Hello {
                min_compatible_version,
            },
        }
    }
}

impl Serialize for ClientResponse {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            HumanClientResponse::from(self.clone()).serialize(serializer)
        } else {
            BinaryClientResponse::from(self.clone()).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for ClientResponse {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            HumanClientResponse::deserialize(deserializer).map(Into::into)
        } else {
            BinaryClientResponse::deserialize(deserializer).map(Into::into)
        }
    }
}

/// Serializes a [`ClientMessage`] stamped with an explicit `protocol_version` (for version-skew
/// tests). Normal callers use [`serialize_message`], which stamps [`PROTOCOL_VERSION_CURRENT`].
#[cfg(test)]
fn serialize_client_message_with_version(
    msg: &ClientMessage,
    protocol_version: u32,
    codec: WireCodec,
) -> Result<Bytes, ProtocolError> {
    match codec {
        WireCodec::Bincode => {
            let wire = BinaryClientMessage::from_client_message(msg.clone(), protocol_version);
            serialize_message(&wire, WireCodec::Bincode)
        }
        WireCodec::Json => {
            let wire = HumanClientMessage::from_client_message(msg.clone(), protocol_version);
            serialize_message(&wire, WireCodec::Json)
        }
    }
}

/// Serializes a [`ClientResponse`] stamped with an explicit `protocol_version` (for version-skew
/// tests). Normal callers use [`serialize_message`], which stamps [`PROTOCOL_VERSION_CURRENT`].
#[cfg(test)]
fn serialize_client_response_with_version(
    msg: &ClientResponse,
    protocol_version: u32,
    codec: WireCodec,
) -> Result<Bytes, ProtocolError> {
    match codec {
        WireCodec::Bincode => {
            let wire = BinaryClientResponse::from_client_response(msg.clone(), protocol_version);
            serialize_message(&wire, WireCodec::Bincode)
        }
        WireCodec::Json => {
            let wire = HumanClientResponse::from_client_response(msg.clone(), protocol_version);
            serialize_message(&wire, WireCodec::Json)
        }
    }
}

/// Unified message discriminator for multiplexed connections on the Master.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum InboundMessage {
    Worker(WorkerMessage),
    Client(ClientMessage),
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
enum HumanInboundMessage {
    Worker(WorkerMessage),
    Client(ClientMessage),
}

#[derive(Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
enum BinaryInboundMessage {
    Worker(WorkerMessage),
    Client(ClientMessage),
}

impl From<InboundMessage> for HumanInboundMessage {
    fn from(msg: InboundMessage) -> Self {
        match msg {
            InboundMessage::Worker(w) => HumanInboundMessage::Worker(w),
            InboundMessage::Client(c) => HumanInboundMessage::Client(c),
        }
    }
}

impl From<HumanInboundMessage> for InboundMessage {
    fn from(msg: HumanInboundMessage) -> Self {
        match msg {
            HumanInboundMessage::Worker(w) => InboundMessage::Worker(w),
            HumanInboundMessage::Client(c) => InboundMessage::Client(c),
        }
    }
}

impl From<InboundMessage> for BinaryInboundMessage {
    fn from(msg: InboundMessage) -> Self {
        match msg {
            InboundMessage::Worker(w) => BinaryInboundMessage::Worker(w),
            InboundMessage::Client(c) => BinaryInboundMessage::Client(c),
        }
    }
}

impl From<BinaryInboundMessage> for InboundMessage {
    fn from(msg: BinaryInboundMessage) -> Self {
        match msg {
            BinaryInboundMessage::Worker(w) => InboundMessage::Worker(w),
            BinaryInboundMessage::Client(c) => InboundMessage::Client(c),
        }
    }
}

impl Serialize for InboundMessage {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            HumanInboundMessage::from(self.clone()).serialize(serializer)
        } else {
            BinaryInboundMessage::from(self.clone()).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for InboundMessage {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            HumanInboundMessage::deserialize(deserializer).map(Into::into)
        } else {
            BinaryInboundMessage::deserialize(deserializer).map(Into::into)
        }
    }
}

/// Constructs a `LengthDelimitedCodec` configured for rusty_grid:
/// - 4-byte length prefix
/// - Big-endian byte order
/// - Strips length prefix on decode
/// - Rejects frames > 64 MB
pub fn default_codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .length_field_offset(0)
        .length_field_length(LENGTH_FIELD_BYTES)
        .length_adjustment(0)
        .num_skip(LENGTH_FIELD_BYTES)
        .big_endian()
        .max_frame_length(MAX_FRAME_SIZE)
        .new_codec()
}

/// Serializes a message into a byte frame according to the specified wire codec.
pub fn serialize_message<M: Serialize>(msg: &M, codec: WireCodec) -> Result<Bytes, ProtocolError> {
    match codec {
        WireCodec::Bincode => {
            let payload_len = bincode::serialized_size(msg)? as usize;
            let size = 1usize.saturating_add(payload_len);
            if size > MAX_FRAME_SIZE {
                return Err(ProtocolError::FrameTooLarge {
                    size,
                    max: MAX_FRAME_SIZE,
                });
            }
            let mut buf = Vec::with_capacity(size);
            buf.push(WIRE_FORMAT_BINCODE);
            bincode::serialize_into(&mut buf, msg)?;
            Ok(Bytes::from(buf))
        }
        WireCodec::Json => {
            let mut buf = Vec::with_capacity(128);
            buf.push(WIRE_FORMAT_JSON);
            serde_json::to_writer(&mut buf, msg)?;
            if buf.len() > MAX_FRAME_SIZE {
                return Err(ProtocolError::FrameTooLarge {
                    size: buf.len(),
                    max: MAX_FRAME_SIZE,
                });
            }
            Ok(Bytes::from(buf))
        }
    }
}

fn bincode_exact_deserialize<T: DeserializeOwned>(
    slice: &[u8],
) -> Result<T, Box<bincode::ErrorKind>> {
    use bincode::Options;
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
        .with_limit(slice.len() as u64)
        .deserialize(slice)
}

fn downcast_msg<T: 'static, M: 'static>(val: T) -> M {
    let boxed: Box<dyn std::any::Any> = Box::new(val);
    *boxed
        .downcast::<M>()
        .expect("caller checked TypeId::of::<M>() == TypeId::of::<T>()")
}

fn incompatible_version_error(peer_version: u32) -> ProtocolError {
    ProtocolError::IncompatibleVersion {
        peer_version,
        current_version: PROTOCOL_VERSION_CURRENT,
        min_compatible_version: PROTOCOL_VERSION_MIN_COMPATIBLE,
    }
}

fn decode_bincode_client_message(slice: &[u8]) -> Result<ClientMessage, ProtocolError> {
    match bincode_exact_deserialize::<BinaryClientMessage>(slice) {
        Ok(bin_msg) => {
            let ver = bin_msg.protocol_version();
            if !is_protocol_version_compatible(ver) {
                return Err(incompatible_version_error(ver));
            }
            Ok(bin_msg.into())
        }
        Err(err) => {
            if bincode_exact_deserialize::<LegacyV2BinaryClientMessage>(slice).is_ok() {
                return Err(incompatible_version_error(2));
            }
            if let Ok(LegacyV5BinaryClientMessage::SubmitTask {
                protocol_version, ..
            }) = bincode_exact_deserialize::<LegacyV5BinaryClientMessage>(slice)
            {
                if !is_protocol_version_compatible(protocol_version) {
                    return Err(incompatible_version_error(protocol_version));
                }
            }
            Err(ProtocolError::Bincode(err))
        }
    }
}

fn decode_bincode_client_response(slice: &[u8]) -> Result<ClientResponse, ProtocolError> {
    match bincode_exact_deserialize::<BinaryClientResponse>(slice) {
        Ok(bin_resp) => {
            let ver = bin_resp.protocol_version();
            if !bin_resp.is_error() && !is_protocol_version_compatible(ver) {
                return Err(incompatible_version_error(ver));
            }
            Ok(bin_resp.into())
        }
        Err(err) => {
            if let Ok(LegacyV3V4BinaryClientResponse::ClusterStatus {
                protocol_version, ..
            }) = bincode_exact_deserialize::<LegacyV3V4BinaryClientResponse>(slice)
            {
                return Err(incompatible_version_error(protocol_version));
            }
            if let Ok(legacy_resp) =
                bincode_exact_deserialize::<LegacyV2BinaryClientResponse>(slice)
            {
                if let LegacyV2BinaryClientResponse::Error { message } = legacy_resp {
                    return Ok(ClientResponse::Error { message });
                }
                return Err(incompatible_version_error(2));
            }
            Err(ProtocolError::Bincode(err))
        }
    }
}

fn decode_bincode_inbound_message(slice: &[u8]) -> Result<InboundMessage, ProtocolError> {
    // 1. Bare modern WorkerMessage opening with Register.
    if let Ok(w) = bincode_exact_deserialize::<WorkerMessage>(slice) {
        if matches!(w, WorkerMessage::Register { .. }) {
            return Ok(InboundMessage::Worker(w));
        }
    }

    // 2. Bare ClientMessage (checks protocol_version and legacy v2 client layout).
    let client_res = decode_bincode_client_message(slice);
    if let Ok(c) = client_res {
        return Ok(InboundMessage::Client(c));
    }

    // 3. Bare non-Register WorkerMessage.
    if let Ok(w) = bincode_exact_deserialize::<WorkerMessage>(slice) {
        return Ok(InboundMessage::Worker(w));
    }

    // 4. Bare legacy WorkerMessage.
    if let Ok(legacy) = bincode_exact_deserialize::<LegacyBinaryWorkerMessage>(slice) {
        return Ok(InboundMessage::Worker(legacy.into()));
    }

    // 5. Wrapped BinaryInboundMessage.
    if let Ok(wrapped) = bincode_exact_deserialize::<BinaryInboundMessage>(slice) {
        return Ok(wrapped.into());
    }

    // 6. Surface version mismatch when the frame matched a ClientMessage layout with an
    // incompatible protocol version.
    if let Err(err @ ProtocolError::IncompatibleVersion { .. }) = client_res {
        return Err(err);
    }

    // 7. Unknown future WorkerMessage variant.
    if slice.len() >= 4 {
        let tag = u32::from_le_bytes(slice[0..4].try_into().unwrap());
        if (8..10_000).contains(&tag) {
            return Ok(InboundMessage::Worker(WorkerMessage::UnknownVariant {
                tag,
                raw_bytes: slice[4..].to_vec(),
            }));
        }
    }

    Err(client_res.unwrap_err())
}

fn decode_json_client_message(raw: &[u8]) -> Result<ClientMessage, ProtocolError> {
    let human: HumanClientMessage = serde_json::from_slice(raw)?;
    let ver = human.protocol_version();
    if !is_protocol_version_compatible(ver) {
        return Err(incompatible_version_error(ver));
    }
    Ok(human.into())
}

fn decode_json_client_response(raw: &[u8]) -> Result<ClientResponse, ProtocolError> {
    let human: HumanClientResponse = serde_json::from_slice(raw)?;
    let ver = human.protocol_version();
    if !human.is_error() && !is_protocol_version_compatible(ver) {
        return Err(incompatible_version_error(ver));
    }
    Ok(human.into())
}

fn decode_json_typed<M: DeserializeOwned + 'static>(raw: &[u8]) -> Result<M, ProtocolError> {
    if std::any::TypeId::of::<M>() == std::any::TypeId::of::<ClientMessage>() {
        return decode_json_client_message(raw).map(downcast_msg);
    }
    if std::any::TypeId::of::<M>() == std::any::TypeId::of::<ClientResponse>() {
        return decode_json_client_response(raw).map(downcast_msg);
    }
    if std::any::TypeId::of::<M>() == std::any::TypeId::of::<InboundMessage>() {
        if let Ok(w) = serde_json::from_slice::<WorkerMessage>(raw) {
            return Ok(downcast_msg(InboundMessage::Worker(w)));
        }
        return decode_json_client_message(raw).map(|c| downcast_msg(InboundMessage::Client(c)));
    }
    serde_json::from_slice::<M>(raw).map_err(ProtocolError::Json)
}

fn peek_wire_codec(bytes: &[u8]) -> Option<WireCodec> {
    match bytes.first().copied() {
        Some(WIRE_FORMAT_BINCODE) => Some(WireCodec::Bincode),
        Some(WIRE_FORMAT_JSON | b'{' | b' ' | b'\t' | b'\r' | b'\n') => Some(WireCodec::Json),
        _ => None,
    }
}

/// Deserializes a message from a byte frame, automatically identifying the wire format
/// from the 1-byte discriminator tag or legacy raw JSON format.
pub fn deserialize_message<M: DeserializeOwned + 'static>(
    bytes: &[u8],
) -> Result<(M, WireCodec), ProtocolError> {
    if bytes.is_empty() {
        return serde_json::from_slice::<M>(bytes)
            .map(|m| (m, WireCodec::Json))
            .map_err(ProtocolError::Json);
    }

    match bytes[0] {
        WIRE_FORMAT_BINCODE => {
            let slice = &bytes[1..];
            if std::any::TypeId::of::<M>() == std::any::TypeId::of::<ClientMessage>() {
                return decode_bincode_client_message(slice)
                    .map(|msg| (downcast_msg(msg), WireCodec::Bincode));
            }
            if std::any::TypeId::of::<M>() == std::any::TypeId::of::<ClientResponse>() {
                return decode_bincode_client_response(slice)
                    .map(|msg| (downcast_msg(msg), WireCodec::Bincode));
            }
            if std::any::TypeId::of::<M>() == std::any::TypeId::of::<InboundMessage>() {
                return decode_bincode_inbound_message(slice)
                    .map(|msg| (downcast_msg(msg), WireCodec::Bincode));
            }

            match bincode_exact_deserialize::<M>(slice) {
                Ok(msg) => Ok((msg, WireCodec::Bincode)),
                Err(e) => {
                    // Fallback for WorkerMessage (legacy or unknown variant)
                    if std::any::TypeId::of::<M>() == std::any::TypeId::of::<WorkerMessage>() {
                        if let Ok(legacy) =
                            bincode_exact_deserialize::<LegacyBinaryWorkerMessage>(slice)
                        {
                            let msg: WorkerMessage = legacy.into();
                            return Ok((downcast_msg(msg), WireCodec::Bincode));
                        }
                        if slice.len() >= 4 {
                            let tag = u32::from_le_bytes(slice[0..4].try_into().unwrap());
                            if (8..10_000).contains(&tag) {
                                let unknown = WorkerMessage::UnknownVariant {
                                    tag,
                                    raw_bytes: slice[4..].to_vec(),
                                };
                                return Ok((downcast_msg(unknown), WireCodec::Bincode));
                            }
                        }
                    }

                    // Fallback for MasterMessage (legacy or unknown variant)
                    if std::any::TypeId::of::<M>() == std::any::TypeId::of::<MasterMessage>() {
                        if let Ok(legacy) =
                            bincode_exact_deserialize::<LegacyBinaryMasterMessage>(slice)
                        {
                            let msg: MasterMessage = legacy.into();
                            return Ok((downcast_msg(msg), WireCodec::Bincode));
                        }
                        if slice.len() >= 4 {
                            let tag = u32::from_le_bytes(slice[0..4].try_into().unwrap());
                            if (7..10_000).contains(&tag) {
                                let unknown = MasterMessage::UnknownVariant {
                                    tag,
                                    raw_bytes: slice[4..].to_vec(),
                                };
                                return Ok((downcast_msg(unknown), WireCodec::Bincode));
                            }
                        }
                    }

                    Err(ProtocolError::Bincode(e))
                }
            }
        }
        WIRE_FORMAT_JSON => decode_json_typed::<M>(&bytes[1..]).map(|msg| (msg, WireCodec::Json)),
        b'{' | b' ' | b'\t' | b'\r' | b'\n' => {
            decode_json_typed::<M>(bytes).map(|msg| (msg, WireCodec::Json))
        }
        tag => Err(ProtocolError::InvalidFormatTag(tag)),
    }
}

/// Unified bidirectional framed transport over any `AsyncRead + AsyncWrite + Unpin` stream.
pub struct MessageTransport<T> {
    inner: Framed<T, LengthDelimitedCodec>,
    codec: WireCodec,
    last_detected_codec: Option<WireCodec>,
    aead_writer: Option<AeadWriterSession>,
    aead_reader: Option<AeadReaderSession>,
}

impl<T> MessageTransport<T> {
    /// Wraps an I/O stream with the standard 4-byte length-delimited framing codec
    /// using the default wire codec (Bincode).
    pub fn new(io: T) -> Self {
        Self::with_codec(io, WireCodec::default())
    }

    /// Wraps an I/O stream with the standard 4-byte length-delimited framing codec
    /// using a specific outbound wire codec.
    pub fn with_codec(io: T, codec: WireCodec) -> Self {
        Self {
            inner: Framed::new(io, default_codec()),
            codec,
            last_detected_codec: None,
            aead_writer: None,
            aead_reader: None,
        }
    }

    /// Enables AEAD frame encryption/decryption using directional keys derived from `session_key`.
    pub fn enable_encryption(&mut self, session_key: &[u8; 32], role: EndpointRole) {
        let (writer, reader) = derive_aead_keys(session_key, role);
        self.aead_writer = Some(writer);
        self.aead_reader = Some(reader);
    }

    /// Returns `true` if this transport has AEAD encryption enabled.
    pub fn is_encrypted(&self) -> bool {
        self.aead_writer.is_some() || self.aead_reader.is_some()
    }

    /// Returns the currently configured outbound wire codec.
    pub fn codec(&self) -> WireCodec {
        self.codec
    }

    /// Sets the outbound wire codec.
    pub fn set_outbound_codec(&mut self, codec: WireCodec) {
        self.codec = codec;
    }

    /// Returns the wire codec detected from the most recently received frame, if any.
    pub fn last_detected_codec(&self) -> Option<WireCodec> {
        self.last_detected_codec
    }

    /// Consumes this transport, returning the underlying `Framed` stream.
    pub fn into_inner(self) -> Framed<T, LengthDelimitedCodec> {
        self.inner
    }

    /// Returns a reference to the underlying I/O stream.
    pub fn get_ref(&self) -> &T {
        self.inner.get_ref()
    }

    /// Returns a mutable reference to the underlying I/O stream.
    pub fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut()
    }

    /// Returns the number of unread bytes currently buffered in the framing reader.
    pub fn read_buffer_len(&self) -> usize {
        self.inner.read_buffer().len()
    }

    /// Returns the number of unwritten bytes currently buffered in the framing writer.
    pub fn write_buffer_len(&self) -> usize {
        self.inner.write_buffer().len()
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> MessageTransport<T> {
    /// Splits transport into independent read and write halves via `tokio::io::split`,
    /// preserving any unconsumed read buffer or unflushed write buffer, propagating
    /// the configured outbound wire codec and AEAD crypto sessions.
    pub fn split(self) -> (MessageWriter<WriteHalf<T>>, MessageReader<ReadHalf<T>>) {
        let parts = self.inner.into_parts();
        let (read_half, write_half) = split(parts.io);
        (
            MessageWriter::with_codec_buffer_and_aead(
                write_half,
                self.codec,
                parts.write_buf,
                self.aead_writer,
            ),
            MessageReader::with_buffer_and_aead(read_half, parts.read_buf, self.aead_reader),
        )
    }

    /// Serializes and sends a message framed with a 4-byte length prefix using the configured wire codec.
    /// If encryption is enabled, the frame is encrypted using the AEAD writer session.
    pub async fn send_msg<M: Serialize>(&mut self, msg: &M) -> Result<(), ProtocolError> {
        let payload = serialize_message(msg, self.codec)?;
        let frame = match self.aead_writer.as_mut() {
            Some(w) => w.encrypt_frame(&payload)?,
            None => payload,
        };
        self.inner.send(frame).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                ProtocolError::UnexpectedEof
            } else {
                ProtocolError::Io(e)
            }
        })?;
        Ok(())
    }

    /// Serializes and sends a message framed with a 4-byte length prefix, requiring write progress
    /// at least once every `idle_timeout` so large frames over slow links succeed as long as bytes
    /// continue to drain.
    pub async fn send_msg_with_idle_timeout<M: Serialize>(
        &mut self,
        msg: &M,
        idle_timeout: std::time::Duration,
    ) -> Result<(), ProtocolError> {
        let payload = serialize_message(msg, self.codec)?;
        let frame = match self.aead_writer.as_mut() {
            Some(w) => w.encrypt_frame(&payload)?,
            None => payload,
        };
        tokio::time::timeout(idle_timeout, self.inner.feed(frame))
            .await
            .map_err(|_| {
                ProtocolError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "write timed out",
                ))
            })?
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    ProtocolError::UnexpectedEof
                } else {
                    ProtocolError::Io(e)
                }
            })?;
        let mut last_remaining = self.inner.write_buffer().len();
        loop {
            match tokio::time::timeout(idle_timeout, SinkExt::<Bytes>::flush(&mut self.inner)).await
            {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(e)) => {
                    return if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        Err(ProtocolError::UnexpectedEof)
                    } else {
                        Err(ProtocolError::Io(e))
                    };
                }
                Err(_) => {
                    let now_remaining = self.inner.write_buffer().len();
                    if now_remaining < last_remaining {
                        last_remaining = now_remaining;
                        continue;
                    }
                    return Err(ProtocolError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "write timed out",
                    )));
                }
            }
        }
    }

    /// Reads the next framed message from the stream.
    /// Returns `Ok(Some(msg))` on success, `Ok(None)` on clean EOF, or `Err`.
    pub async fn recv_msg<M: DeserializeOwned + 'static>(
        &mut self,
    ) -> Result<Option<M>, ProtocolError> {
        match self.recv_msg_with_codec::<M>().await? {
            Some((msg, _codec)) => Ok(Some(msg)),
            None => Ok(None),
        }
    }

    /// Reads the next framed message from the stream, requiring read progress at least once every
    /// `idle_timeout` so large frames (up to 64 MB) over slow links succeed as long as bytes
    /// continue arriving.
    pub async fn recv_msg_with_idle_timeout<M: DeserializeOwned + 'static>(
        &mut self,
        idle_timeout: std::time::Duration,
    ) -> Result<Option<M>, ProtocolError> {
        let mut last_state = (
            self.inner.read_buffer().len(),
            self.inner.read_buffer().capacity(),
        );
        loop {
            match tokio::time::timeout(idle_timeout, self.recv_msg::<M>()).await {
                Ok(res) => return res,
                Err(_) => {
                    let now_state = (
                        self.inner.read_buffer().len(),
                        self.inner.read_buffer().capacity(),
                    );
                    if now_state.0 > last_state.0 || now_state.1 > last_state.1 {
                        last_state = now_state;
                        continue;
                    }
                    return Err(ProtocolError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "read timed out",
                    )));
                }
            }
        }
    }

    /// Reads the next framed message and returns it alongside the detected wire codec.
    /// If encryption is enabled, the frame is opened and verified using the AEAD reader session.
    pub async fn recv_msg_with_codec<M: DeserializeOwned + 'static>(
        &mut self,
    ) -> Result<Option<(M, WireCodec)>, ProtocolError> {
        match self.inner.next().await {
            Some(Ok(bytes)) => {
                let plaintext = match self.aead_reader.as_mut() {
                    Some(r) => r.decrypt_frame(&bytes)?,
                    None => bytes.freeze(),
                };
                if let Some(detected) = peek_wire_codec(&plaintext) {
                    self.last_detected_codec = Some(detected);
                }
                let (msg, codec) = deserialize_message::<M>(&plaintext)?;
                self.last_detected_codec = Some(codec);
                Ok(Some((msg, codec)))
            }
            Some(Err(e)) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    Err(ProtocolError::UnexpectedEof)
                } else if e.kind() == std::io::ErrorKind::InvalidData {
                    Err(ProtocolError::FrameTooLarge {
                        size: 0,
                        max: MAX_FRAME_SIZE,
                    })
                } else {
                    Err(ProtocolError::Io(e))
                }
            }
            None => Ok(None),
        }
    }

    /// Sends raw bytes as a framed message.
    pub async fn send_raw_frame(&mut self, data: Bytes) -> Result<(), ProtocolError> {
        if data.len() > MAX_FRAME_SIZE {
            return Err(ProtocolError::FrameTooLarge {
                size: data.len(),
                max: MAX_FRAME_SIZE,
            });
        }
        self.inner.send(data).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                ProtocolError::UnexpectedEof
            } else {
                ProtocolError::Io(e)
            }
        })?;
        Ok(())
    }

    /// Reads raw frame bytes without deserializing.
    pub async fn recv_raw_frame(&mut self) -> Result<Option<Bytes>, ProtocolError> {
        match self.inner.next().await {
            Some(Ok(bytes)) => Ok(Some(bytes.freeze())),
            Some(Err(e)) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    Err(ProtocolError::UnexpectedEof)
                } else if e.kind() == std::io::ErrorKind::InvalidData {
                    Err(ProtocolError::FrameTooLarge {
                        size: 0,
                        max: MAX_FRAME_SIZE,
                    })
                } else {
                    Err(ProtocolError::Io(e))
                }
            }
            None => Ok(None),
        }
    }

    /// Reads raw frame bytes without deserializing, refusing frames longer than `max_len`.
    pub async fn recv_raw_frame_max(
        &mut self,
        max_len: usize,
    ) -> Result<Option<Bytes>, ProtocolError> {
        let limit = max_len.min(MAX_FRAME_SIZE);
        self.inner.codec_mut().set_max_frame_length(limit);
        let res = match self.inner.next().await {
            Some(Ok(bytes)) => Ok(Some(bytes.freeze())),
            Some(Err(e)) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    Err(ProtocolError::UnexpectedEof)
                } else if e.kind() == std::io::ErrorKind::InvalidData {
                    Err(ProtocolError::FrameTooLarge {
                        size: 0,
                        max: limit,
                    })
                } else {
                    Err(ProtocolError::Io(e))
                }
            }
            None => Ok(None),
        };
        self.inner.codec_mut().set_max_frame_length(MAX_FRAME_SIZE);
        res
    }
}

/// Unidirectional framed reader for receiving messages.
pub struct MessageReader<R> {
    inner: Framed<R, LengthDelimitedCodec>,
    last_detected_codec: Option<WireCodec>,
    aead_reader: Option<AeadReaderSession>,
}

impl<R: AsyncRead + Unpin> MessageReader<R> {
    /// Creates a new `MessageReader` wrapping an async reader.
    pub fn new(reader: R) -> Self {
        Self::with_buffer(reader, BytesMut::new())
    }

    /// Creates a new `MessageReader` wrapping an async reader with pre-buffered data.
    pub fn with_buffer(reader: R, buffer: BytesMut) -> Self {
        Self::with_buffer_and_aead(reader, buffer, None)
    }

    /// Creates a new `MessageReader` with pre-buffered data and optional AEAD reader session.
    pub fn with_buffer_and_aead(
        reader: R,
        buffer: BytesMut,
        aead_reader: Option<AeadReaderSession>,
    ) -> Self {
        let mut parts = FramedParts::new::<Bytes>(reader, default_codec());
        parts.read_buf = buffer;
        Self {
            inner: Framed::from_parts(parts),
            last_detected_codec: None,
            aead_reader,
        }
    }

    /// Enables AEAD decryption session.
    pub fn enable_encryption(&mut self, reader: AeadReaderSession) {
        self.aead_reader = Some(reader);
    }

    /// Returns `true` if AEAD decryption is enabled.
    pub fn is_encrypted(&self) -> bool {
        self.aead_reader.is_some()
    }

    /// Reads the next framed message from the reader.
    pub async fn recv_msg<M: DeserializeOwned + 'static>(
        &mut self,
    ) -> Result<Option<M>, ProtocolError> {
        match self.recv_msg_with_codec::<M>().await? {
            Some((msg, _codec)) => Ok(Some(msg)),
            None => Ok(None),
        }
    }

    /// Reads the next framed message and returns it alongside the detected wire codec.
    pub async fn recv_msg_with_codec<M: DeserializeOwned + 'static>(
        &mut self,
    ) -> Result<Option<(M, WireCodec)>, ProtocolError> {
        match self.inner.next().await {
            Some(Ok(bytes)) => {
                let plaintext = match self.aead_reader.as_mut() {
                    Some(r) => r.decrypt_frame(&bytes)?,
                    None => bytes.freeze(),
                };
                if let Some(detected) = peek_wire_codec(&plaintext) {
                    self.last_detected_codec = Some(detected);
                }
                let (msg, codec) = deserialize_message::<M>(&plaintext)?;
                self.last_detected_codec = Some(codec);
                Ok(Some((msg, codec)))
            }
            Some(Err(e)) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    Err(ProtocolError::UnexpectedEof)
                } else if e.kind() == std::io::ErrorKind::InvalidData {
                    Err(ProtocolError::FrameTooLarge {
                        size: 0,
                        max: MAX_FRAME_SIZE,
                    })
                } else {
                    Err(ProtocolError::Io(e))
                }
            }
            None => Ok(None),
        }
    }

    /// Returns the wire codec detected from the most recently received frame, if any.
    pub fn last_detected_codec(&self) -> Option<WireCodec> {
        self.last_detected_codec
    }

    /// Reads raw frame bytes without deserializing.
    pub async fn recv_raw_frame(&mut self) -> Result<Option<Bytes>, ProtocolError> {
        match self.inner.next().await {
            Some(Ok(bytes)) => Ok(Some(bytes.freeze())),
            Some(Err(e)) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    Err(ProtocolError::UnexpectedEof)
                } else if e.kind() == std::io::ErrorKind::InvalidData {
                    Err(ProtocolError::FrameTooLarge {
                        size: 0,
                        max: MAX_FRAME_SIZE,
                    })
                } else {
                    Err(ProtocolError::Io(e))
                }
            }
            None => Ok(None),
        }
    }
}

/// Unidirectional framed writer for transmitting messages.
pub struct MessageWriter<W> {
    inner: FramedWrite<W, LengthDelimitedCodec>,
    codec: WireCodec,
    aead_writer: Option<AeadWriterSession>,
}

impl<W: AsyncWrite + Unpin> MessageWriter<W> {
    /// Creates a new `MessageWriter` wrapping an async writer with default wire codec (Bincode).
    pub fn new(writer: W) -> Self {
        Self::with_codec(writer, WireCodec::default())
    }

    /// Creates a new `MessageWriter` wrapping an async writer with specific wire codec.
    pub fn with_codec(writer: W, codec: WireCodec) -> Self {
        Self::with_codec_buffer_and_aead(writer, codec, BytesMut::new(), None)
    }

    /// Creates a new `MessageWriter` wrapping an async writer with pre-buffered data and default codec.
    pub fn with_buffer(writer: W, buffer: BytesMut) -> Self {
        Self::with_codec_buffer_and_aead(writer, WireCodec::default(), buffer, None)
    }

    /// Creates a new `MessageWriter` wrapping an async writer with pre-buffered data and specified codec.
    pub fn with_codec_and_buffer(writer: W, codec: WireCodec, buffer: BytesMut) -> Self {
        Self::with_codec_buffer_and_aead(writer, codec, buffer, None)
    }

    /// Creates a new `MessageWriter` wrapping an async writer with pre-buffered data, specified codec, and AEAD session.
    pub fn with_codec_buffer_and_aead(
        writer: W,
        codec: WireCodec,
        buffer: BytesMut,
        aead_writer: Option<AeadWriterSession>,
    ) -> Self {
        let mut framed = FramedWrite::new(writer, default_codec());
        if !buffer.is_empty() {
            framed.write_buffer_mut().extend_from_slice(&buffer);
        }
        Self {
            inner: framed,
            codec,
            aead_writer,
        }
    }

    /// Enables AEAD encryption session.
    pub fn enable_encryption(&mut self, writer: AeadWriterSession) {
        self.aead_writer = Some(writer);
    }

    /// Returns `true` if AEAD encryption is enabled.
    pub fn is_encrypted(&self) -> bool {
        self.aead_writer.is_some()
    }

    /// Returns the currently configured outbound wire codec.
    pub fn codec(&self) -> WireCodec {
        self.codec
    }

    /// Sets the outbound wire codec.
    pub fn set_codec(&mut self, codec: WireCodec) {
        self.codec = codec;
    }

    /// Serializes and sends a message framed with a 4-byte length prefix using the configured wire codec.
    pub async fn send_msg<M: Serialize>(&mut self, msg: &M) -> Result<(), ProtocolError> {
        let payload = serialize_message(msg, self.codec)?;
        let frame = match self.aead_writer.as_mut() {
            Some(w) => w.encrypt_frame(&payload)?,
            None => payload,
        };
        self.inner.send(frame).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                ProtocolError::UnexpectedEof
            } else {
                ProtocolError::Io(e)
            }
        })?;
        Ok(())
    }

    /// Sends raw bytes as a framed message.
    pub async fn send_raw_frame(&mut self, data: Bytes) -> Result<(), ProtocolError> {
        if data.len() > MAX_FRAME_SIZE {
            return Err(ProtocolError::FrameTooLarge {
                size: data.len(),
                max: MAX_FRAME_SIZE,
            });
        }
        self.inner.send(data).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                ProtocolError::UnexpectedEof
            } else {
                ProtocolError::Io(e)
            }
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capabilities::WorkerCapabilities;
    use crate::task::{Task, TaskId, TaskRequirements, TaskSpec, TaskStatus};
    use std::collections::HashMap;

    #[test]
    fn test_worker_message_roundtrip() {
        let worker_id = Uuid::new_v4();
        let task_id = TaskId::new();

        let messages = vec![
            WorkerMessage::Register {
                worker_id,
                capabilities: WorkerCapabilities {
                    name: "test-worker-1".into(),
                    cpu_cores: 8,
                    ram_mb: 16384,
                    has_gpu: true,
                    is_simulated_gpu: true,
                    gpu_device_name: Some("Simulated M1 GPU".into()),
                    tags: vec!["fast".into()],
                    mobile: None,
                    ..Default::default()
                },
                protocol_version: PROTOCOL_VERSION_CURRENT,
                features: ProtocolCapabilities::all_supported(),
            },
            WorkerMessage::Heartbeat {
                worker_id,
                timestamp: 1726747200,
                active_tasks: 2,
                cpu_usage_pct: 42.5,
                ram_available_mb: 8192,
            },
            WorkerMessage::TaskProgress {
                worker_id,
                task_id,
                status: TaskStatus::Running,
            },
            WorkerMessage::TaskResult {
                worker_id,
                task_id,
                exit_code: 0,
                stdout: "Success output".into(),
                stderr: "".into(),
                execution_time_ms: 120,
                is_gpu_executed: true,
                device_name: Some("Simulated M1 GPU".into()),
                error: None,
                asset_bytes: None,
                artifacts: Vec::new(),
                outputs: Vec::new(),
            },
            WorkerMessage::Checkpoint {
                task_id,
                sequence: 1,
                delta_state: TaskBytes::copy_from_slice(b"checkpoint-50"),
            },
            WorkerMessage::Disconnecting {
                worker_id,
                reason: "Normal shutdown".into(),
            },
        ];

        for msg in messages {
            let json = serde_json::to_string(&msg).expect("serialization failed");
            let deserialized: WorkerMessage =
                serde_json::from_str(&json).expect("deserialization failed");
            assert_eq!(msg, deserialized);
            if let WorkerMessage::Checkpoint { .. } = msg {
                assert_eq!(msg.worker_id(), Uuid::nil());
            } else {
                assert_eq!(msg.worker_id(), worker_id);
            }
            assert!(!msg.variant_name().is_empty());
        }
    }

    #[test]
    fn test_master_message_roundtrip() {
        let worker_id = Uuid::new_v4();
        let task_id = TaskId::new();

        let messages = vec![
            MasterMessage::RegisterAck {
                accepted: true,
                worker_id,
                heartbeat_interval_secs: 3,
                protocol_version: PROTOCOL_VERSION_CURRENT,
                features: ProtocolCapabilities::all_supported(),
                message: Some("Registered".into()),
            },
            MasterMessage::HeartbeatAck {
                timestamp: 1726747201,
            },
            MasterMessage::AssignTask {
                task: Task {
                    id: task_id,
                    spec: TaskSpec::Command {
                        program: "cargo".into(),
                        args: vec!["test".into()],
                        env: HashMap::new(),
                        working_dir: None,
                        stdin: None,
                    },
                    requirements: TaskRequirements {
                        cpu_cores: 4,
                        ram_mb: 8192,
                        gpu_required: false,
                        timeout_secs: 60,
                        max_retries: None,
                    },
                    created_at_utc: 1726747200,
                    tags: vec!["test".into()],
                    latest_checkpoint: None,
                    staged_files: Vec::new(),
                    expected_outputs: Vec::new(),
                    inputs: Vec::new(),
                    output_globs: Vec::new(),
                    workspace: None,
                },
            },
            MasterMessage::AssignTaskWithCheckpoint {
                task: Task {
                    id: task_id,
                    spec: TaskSpec::Command {
                        program: "cargo".into(),
                        args: vec!["test".into()],
                        env: HashMap::new(),
                        working_dir: None,
                        stdin: None,
                    },
                    requirements: TaskRequirements {
                        cpu_cores: 4,
                        ram_mb: 8192,
                        gpu_required: false,
                        timeout_secs: 60,
                        max_retries: None,
                    },
                    created_at_utc: 1726747200,
                    tags: vec!["test".into()],
                    latest_checkpoint: None,
                    staged_files: Vec::new(),
                    expected_outputs: Vec::new(),
                    inputs: Vec::new(),
                    output_globs: Vec::new(),
                    workspace: None,
                },
                latest_checkpoint: Some(CheckpointData::new(
                    5,
                    TaskBytes::copy_from_slice(b"checkpoint-state"),
                )),
            },
            MasterMessage::CancelTask {
                task_id,
                reason: Some("Canceled by test".into()),
            },
            MasterMessage::Shutdown {
                reason: "Shutdown command".into(),
                grace_period_secs: Some(10),
            },
        ];

        for msg in messages {
            let json = serde_json::to_string(&msg).expect("serialization failed");
            let deserialized: MasterMessage =
                serde_json::from_str(&json).expect("deserialization failed");
            assert_eq!(msg, deserialized);

            assert!(!msg.variant_name().is_empty());
        }
    }

    #[test]
    fn test_codec_big_endian_wire_layout() {
        use bytes::BytesMut;
        use tokio_util::codec::{Decoder, Encoder};

        let mut codec = default_codec();
        let mut buf = BytesMut::new();
        let payload = Bytes::from_static(b"{\"type\":\"HeartbeatAck\",\"timestamp\":123}");

        codec.encode(payload.clone(), &mut buf).unwrap();

        // Must prepend 4-byte big-endian length prefix
        assert_eq!(buf.len(), 4 + payload.len());
        let wire_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        assert_eq!(wire_len, payload.len());
        assert_eq!(&buf[4..], payload.as_ref());

        // Decode must strip prefix and yield exact payload
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, payload);
        assert_eq!(buf.len(), 0);
    }

    #[tokio::test]
    async fn test_message_transport_duplex() {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let mut client = MessageTransport::new(client_io);
        let mut server = MessageTransport::new(server_io);

        let worker_id = Uuid::new_v4();
        let reg = WorkerMessage::Register {
            worker_id,
            capabilities: WorkerCapabilities {
                name: "w1".into(),
                cpu_cores: 4,
                ram_mb: 8192,
                has_gpu: false,
                is_simulated_gpu: false,
                gpu_device_name: None,
                tags: vec![],
                mobile: None,
                ..Default::default()
            },
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
        };

        // Client -> Server
        client.send_msg(&reg).await.unwrap();
        let server_recv: Option<WorkerMessage> = server.recv_msg().await.unwrap();
        assert_eq!(server_recv, Some(reg));

        // Server -> Client
        let ack = MasterMessage::RegisterAck {
            accepted: true,
            worker_id,
            heartbeat_interval_secs: 3,
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
            message: None,
        };
        server.send_msg(&ack).await.unwrap();
        let client_recv: Option<MasterMessage> = client.recv_msg().await.unwrap();
        assert_eq!(client_recv, Some(ack));
    }

    #[tokio::test]
    async fn test_message_transport_split() {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let (mut client_tx, mut client_rx) = MessageTransport::new(client_io).split();
        let (mut server_tx, mut server_rx) = MessageTransport::new(server_io).split();

        let hb = WorkerMessage::Heartbeat {
            worker_id: Uuid::new_v4(),
            timestamp: 1000,
            active_tasks: 0,
            cpu_usage_pct: 0.0,
            ram_available_mb: 0,
        };

        client_tx.send_msg(&hb).await.unwrap();
        let received: Option<WorkerMessage> = server_rx.recv_msg().await.unwrap();
        assert_eq!(received, Some(hb));

        let ack = MasterMessage::HeartbeatAck { timestamp: 1000 };
        server_tx.send_msg(&ack).await.unwrap();
        let ack_received: Option<MasterMessage> = client_rx.recv_msg().await.unwrap();
        assert_eq!(ack_received, Some(ack));
    }

    #[test]
    fn test_partial_frame_streaming() {
        use bytes::BytesMut;
        use tokio_util::codec::Decoder;

        let mut codec = default_codec();
        let mut buf = BytesMut::new();

        // 1. Feed only 2 bytes of length prefix (< 4 bytes)
        buf.extend_from_slice(&[0, 0]);
        let res = codec.decode(&mut buf).unwrap();
        assert!(res.is_none());

        // 2. Complete the 4-byte header: length = 10
        buf.extend_from_slice(&[0, 10]);
        // And feed only 4 bytes of payload
        buf.extend_from_slice(b"abcd");
        let res = codec.decode(&mut buf).unwrap();
        assert!(res.is_none());

        // 3. Feed remaining 6 bytes of payload
        buf.extend_from_slice(b"efghij");
        let res = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(res, Bytes::from_static(b"abcdefghij"));
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_pipelined_multiple_frames() {
        use bytes::BytesMut;
        use tokio_util::codec::{Decoder, Encoder};

        let mut codec = default_codec();
        let mut buf = BytesMut::new();

        let msg1 = Bytes::from_static(b"frame 1");
        let msg2 = Bytes::from_static(b"frame 2");
        let msg3 = Bytes::from_static(b"frame 3");

        codec.encode(msg1.clone(), &mut buf).unwrap();
        codec.encode(msg2.clone(), &mut buf).unwrap();
        codec.encode(msg3.clone(), &mut buf).unwrap();

        // Verify sequential retrieval from combined buffer
        assert_eq!(codec.decode(&mut buf).unwrap().unwrap(), msg1);
        assert_eq!(codec.decode(&mut buf).unwrap().unwrap(), msg2);
        assert_eq!(codec.decode(&mut buf).unwrap().unwrap(), msg3);
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn test_oversized_frame_rejection() {
        use bytes::BytesMut;
        use tokio_util::codec::Decoder;

        let mut codec = default_codec();
        let mut buf = BytesMut::new();

        // Send a frame length of 70 MB (> 64 MB MAX_FRAME_SIZE)
        let bad_length: u32 = 70 * 1024 * 1024;
        buf.extend_from_slice(&bad_length.to_be_bytes());

        let res = codec.decode(&mut buf);
        assert!(res.is_err(), "Oversized frame must trigger error");
        let err = res.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn test_malformed_json_recovery() {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let mut client_writer = MessageWriter::new(client_io);
        let mut server = MessageTransport::new(server_io);

        // Client sends corrupted JSON in Frame 1
        client_writer
            .send_raw_frame(Bytes::from_static(b"{\"type\": \"Corrupt\""))
            .await
            .unwrap();

        // Client sends valid JSON in Frame 2
        let valid_hb = WorkerMessage::Heartbeat {
            worker_id: Uuid::new_v4(),
            timestamp: 2000,
            active_tasks: 0,
            cpu_usage_pct: 0.0,
            ram_available_mb: 0,
        };
        client_writer.send_msg(&valid_hb).await.unwrap();

        // Server reads Frame 1 -> JSON parse error
        let res1: Result<Option<WorkerMessage>, _> = server.recv_msg().await;
        assert!(res1.is_err(), "Frame 1 must fail JSON deserialization");

        // Server reads Frame 2 -> Frame boundary preserved, succeeds!
        let res2: Option<WorkerMessage> = server.recv_msg().await.unwrap();
        assert_eq!(res2, Some(valid_hb));
    }

    #[tokio::test]
    async fn test_eof_handling() {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let mut server = MessageTransport::new(server_io);

        // Drop client immediately (clean EOF)
        drop(client_io);

        let msg: Option<WorkerMessage> = server.recv_msg().await.unwrap();
        assert!(msg.is_none(), "Clean disconnect must return Ok(None)");
    }

    #[test]
    fn test_heartbeat_serde_default_backward_compat() {
        // Old JSON without cpu_usage_pct and ram_available_mb
        let json = r#"{"type":"Heartbeat","worker_id":"00000000-0000-0000-0000-000000000001","timestamp":100,"active_tasks":1}"#;
        let parsed: WorkerMessage = serde_json::from_str(json).expect("deserialize old heartbeat");
        match parsed {
            WorkerMessage::Heartbeat {
                cpu_usage_pct,
                ram_available_mb,
                ..
            } => {
                assert_eq!(cpu_usage_pct, 0.0);
                assert_eq!(ram_available_mb, 0);
            }
            _ => panic!("Expected Heartbeat"),
        }
    }

    #[test]
    fn test_client_message_response_roundtrip() {
        let task_id = TaskId::new();
        let submit_msg = ClientMessage::GetTaskStatus { task_id };
        let json = serde_json::to_string(&submit_msg).expect("serialize ClientMessage");
        let deserialized: ClientMessage = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(submit_msg, deserialized);

        let response = ClientResponse::TaskSubmitted { task_id };
        let resp_json = serde_json::to_string(&response).expect("serialize ClientResponse");
        let resp_deserialized: ClientResponse =
            serde_json::from_str(&resp_json).expect("deserialize");
        assert_eq!(response, resp_deserialized);
    }

    #[test]
    fn test_wire_codec_discriminator_serialization() {
        let wid = Uuid::new_v4();
        let msg = WorkerMessage::Heartbeat {
            worker_id: wid,
            timestamp: 12345,
            active_tasks: 2,
            cpu_usage_pct: 12.5,
            ram_available_mb: 4096,
        };

        // Bincode serialized format begins with 0x02
        let bin_bytes = serialize_message(&msg, WireCodec::Bincode).expect("serialize bincode");
        assert_eq!(bin_bytes[0], WIRE_FORMAT_BINCODE);

        // JSON serialized format begins with 0x01
        let json_bytes = serialize_message(&msg, WireCodec::Json).expect("serialize json");
        assert_eq!(json_bytes[0], WIRE_FORMAT_JSON);

        // Verify deserialization correctly recovers the message and detected codec
        let (recovered_bin, codec_bin): (WorkerMessage, WireCodec) =
            deserialize_message(&bin_bytes).expect("deserialize bincode");
        assert_eq!(recovered_bin, msg);
        assert_eq!(codec_bin, WireCodec::Bincode);

        let (recovered_json, codec_json): (WorkerMessage, WireCodec) =
            deserialize_message(&json_bytes).expect("deserialize json");
        assert_eq!(recovered_json, msg);
        assert_eq!(codec_json, WireCodec::Json);
    }

    #[test]
    fn test_wire_codec_raw_json_fallback() {
        let wid = Uuid::new_v4();
        let raw_json = format!(
            r#"{{"type":"Heartbeat","worker_id":"{}","timestamp":999,"active_tasks":0,"cpu_usage_pct":0.0,"ram_available_mb":1024}}"#,
            wid
        );
        let raw_bytes = Bytes::copy_from_slice(raw_json.as_bytes());

        let (msg, codec): (WorkerMessage, WireCodec) =
            deserialize_message(&raw_bytes).expect("deserialize raw json");
        assert_eq!(codec, WireCodec::Json);
        match msg {
            WorkerMessage::Heartbeat {
                worker_id,
                timestamp,
                ..
            } => {
                assert_eq!(worker_id, wid);
                assert_eq!(timestamp, 999);
            }
            _ => panic!("unexpected message variant"),
        }
    }

    #[test]
    fn test_wire_codec_invalid_discriminator() {
        let bad_payload = Bytes::from_static(&[0xFF, 0x01, 0x02, 0x03]);
        let err = deserialize_message::<WorkerMessage>(&bad_payload).unwrap_err();
        match err {
            ProtocolError::InvalidFormatTag(0xFF) => {}
            other => panic!("expected InvalidFormatTag(0xFF), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_transport_auto_codec_negotiation() {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);

        // Client uses Json codec
        let mut client = MessageTransport::with_codec(client_io, WireCodec::Json);
        // Server defaults to Bincode
        let mut server = MessageTransport::with_codec(server_io, WireCodec::Bincode);

        let wid = Uuid::new_v4();
        let reg_msg = WorkerMessage::Register {
            worker_id: wid,
            capabilities: crate::capabilities::WorkerCapabilities::new(
                "worker-1", 4, 8192, false, false, None,
            ),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
        };

        // Client sends Register message in JSON format
        client.send_msg(&reg_msg).await.expect("client send");

        // Server receives and auto-detects codec
        let (inbound, detected_codec) = server
            .recv_msg_with_codec::<InboundMessage>()
            .await
            .expect("server recv")
            .expect("some message");

        assert_eq!(detected_codec, WireCodec::Json);
        assert_eq!(inbound, InboundMessage::Worker(reg_msg));

        // Server adjusts outbound codec to match client
        server.set_outbound_codec(detected_codec);

        let ack = MasterMessage::RegisterAck {
            accepted: true,
            worker_id: wid,
            heartbeat_interval_secs: 3,
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
            message: None,
        };
        server.send_msg(&ack).await.expect("server send ack");

        // Client receives RegisterAck successfully in JSON
        let client_ack: MasterMessage = client
            .recv_msg()
            .await
            .expect("client recv")
            .expect("some ack");
        assert_eq!(client_ack, ack);
    }

    #[tokio::test]
    async fn test_recv_raw_frame_max_enforces_limit_on_header_and_restores_it() {
        use tokio::io::AsyncWriteExt;
        let (mut raw, server_io) = tokio::io::duplex(1 << 20);
        let mut server = MessageTransport::new(server_io);

        // Exactly at the limit is accepted; the transport limit is back to normal afterwards.
        let mut frame = (64u32).to_be_bytes().to_vec();
        frame.extend_from_slice(&[7u8; 64]);
        raw.write_all(&frame).await.unwrap();
        let got = server.recv_raw_frame_max(64).await.unwrap().unwrap();
        assert_eq!(got.len(), 64);

        let mut big = (4096u32).to_be_bytes().to_vec();
        big.extend_from_slice(&[9u8; 4096]);
        raw.write_all(&big).await.unwrap();
        assert_eq!(server.recv_raw_frame().await.unwrap().unwrap().len(), 4096);

        // One byte over is refused from the header alone: the payload never has to arrive.
        raw.write_all(&(65u32).to_be_bytes()).await.unwrap();
        match server.recv_raw_frame_max(64).await {
            Err(ProtocolError::FrameTooLarge { max, .. }) => assert_eq!(max, 64),
            other => panic!("expected FrameTooLarge, got {other:?}"),
        }

        // Clean EOF is reported as None, and the limit cannot be raised above the global cap.
        let (raw2, io2) = tokio::io::duplex(64);
        let mut server2 = MessageTransport::new(io2);
        drop(raw2);
        assert!(server2
            .recv_raw_frame_max(usize::MAX)
            .await
            .unwrap()
            .is_none());
    }

    /// Bincode is positional, so a new variant is only wire-compatible if it is appended: every
    /// existing variant must keep its index and `GetTaskResult` must take the next free one.
    #[test]
    fn test_get_task_result_is_appended_last_and_keeps_existing_bincode_indices() {
        fn variant_index(msg: &ClientMessage) -> u32 {
            let bytes = serialize_message(msg, WireCodec::Bincode).expect("serialize");
            assert_eq!(bytes[0], WIRE_FORMAT_BINCODE);
            u32::from_le_bytes(bytes[1..5].try_into().unwrap())
        }
        let task_id = TaskId::new();
        let job = crate::mapreduce::MapReduceJobSpec::new(
            "idx",
            vec!["a".into()],
            crate::mapreduce::MapFunctionSpec::Builtin {
                operator: "word_count".into(),
            },
            crate::mapreduce::ReduceFunctionSpec::Builtin {
                operator: "sum".into(),
            },
            1,
            1,
            30,
        );
        let task = Task::new(
            TaskSpec::command("echo", vec!["x".into()]),
            TaskRequirements::generic(1, 10),
        );
        assert_eq!(
            variant_index(&ClientMessage::SubmitTask { task, wait: false }),
            0
        );
        assert_eq!(variant_index(&ClientMessage::GetTaskStatus { task_id }), 1);
        assert_eq!(variant_index(&ClientMessage::CancelTask { task_id }), 2);
        assert_eq!(variant_index(&ClientMessage::ClusterStatus), 3);
        assert_eq!(variant_index(&ClientMessage::ListWorkers), 4);
        assert_eq!(variant_index(&ClientMessage::SubmitMapReduce { job }), 5);
        assert_eq!(variant_index(&ClientMessage::GetTaskResult { task_id }), 6);
    }

    #[test]
    fn test_get_task_result_roundtrip_both_codecs() {
        let task_id = TaskId::new();
        let msg = ClientMessage::GetTaskResult { task_id };

        // Plain serde_json (human-readable mirror enum)
        let json = serde_json::to_string(&msg).expect("serialize json");
        assert!(json.contains("\"type\":\"GetTaskResult\""), "{json}");
        let back: ClientMessage = serde_json::from_str(&json).expect("deserialize json");
        assert_eq!(back, msg);

        // Both wire codecs, as a raw ClientMessage and wrapped in an InboundMessage
        for codec in [WireCodec::Bincode, WireCodec::Json] {
            let bytes = serialize_message(&msg, codec).expect("serialize");
            let (recovered, detected): (ClientMessage, WireCodec) =
                deserialize_message(&bytes).expect("deserialize ClientMessage");
            assert_eq!(recovered, msg);
            assert_eq!(detected, codec);

            // A peer that opens with the bare ClientMessage (what the CLI does) must be routed as a client.
            let (inbound, detected): (InboundMessage, WireCodec) =
                deserialize_message(&bytes).expect("deserialize InboundMessage from bare message");
            assert_eq!(inbound, InboundMessage::Client(msg.clone()));
            assert_eq!(detected, codec);

            let wrapped = InboundMessage::Client(msg.clone());
            let bytes = serialize_message(&wrapped, codec).expect("serialize inbound");
            let (recovered, _): (InboundMessage, WireCodec) =
                deserialize_message(&bytes).expect("deserialize wrapped");
            assert_eq!(recovered, wrapped);
        }
    }

    /// `MobileTelemetry` is appended after `Disconnecting`: every earlier `WorkerMessage` keeps its
    /// positional (bincode) index and the new variant takes the next free one.
    #[test]
    fn test_mobile_telemetry_is_appended_last_and_keeps_existing_bincode_indices() {
        fn variant_index(msg: &WorkerMessage) -> u32 {
            let bytes = serialize_message(msg, WireCodec::Bincode).expect("serialize");
            assert_eq!(bytes[0], WIRE_FORMAT_BINCODE);
            u32::from_le_bytes(bytes[1..5].try_into().unwrap())
        }
        let worker_id = Uuid::new_v4();
        let task_id = TaskId::new();
        assert_eq!(
            variant_index(&WorkerMessage::Register {
                worker_id,
                capabilities: WorkerCapabilities::detect(Some("idx".into()), Some(1), false),
                protocol_version: PROTOCOL_VERSION_CURRENT,
                features: ProtocolCapabilities::all_supported(),
            }),
            0
        );
        assert_eq!(
            variant_index(&WorkerMessage::Heartbeat {
                worker_id,
                timestamp: 1,
                active_tasks: 0,
                cpu_usage_pct: 0.0,
                ram_available_mb: 1,
            }),
            1
        );
        assert_eq!(
            variant_index(&WorkerMessage::TaskProgress {
                worker_id,
                task_id,
                status: TaskStatus::Running,
            }),
            2
        );
        assert_eq!(
            variant_index(&WorkerMessage::Checkpoint {
                task_id,
                sequence: 1,
                delta_state: TaskBytes::copy_from_slice(b"x"),
            }),
            4
        );
        assert_eq!(
            variant_index(&WorkerMessage::Disconnecting {
                worker_id,
                reason: "bye".into(),
            }),
            5
        );
        assert_eq!(
            variant_index(&WorkerMessage::MobileTelemetry {
                worker_id,
                battery_pct: Some(9),
                is_charging: Some(false),
                thermal_throttled: true,
                battery_temperature: None,
                network_type: None,
                battery_tier: None,
                thermal_tier: None,
            }),
            6
        );
    }

    #[test]
    fn test_mobile_telemetry_roundtrip_both_codecs() {
        let worker_id = Uuid::new_v4();
        let cases = [
            WorkerMessage::MobileTelemetry {
                worker_id,
                battery_pct: Some(14),
                is_charging: Some(false),
                thermal_throttled: true,
                battery_temperature: Some(38.5),
                network_type: Some("cellular".into()),
                battery_tier: Some(crate::capabilities::BatteryTier::Critical),
                thermal_tier: Some(crate::capabilities::ThermalTier::Warm),
            },
            // Unknown readings are legitimate (no battery sensor reachable): `None` must survive.
            WorkerMessage::MobileTelemetry {
                worker_id,
                battery_pct: None,
                is_charging: None,
                thermal_throttled: false,
                battery_temperature: None,
                network_type: None,
                battery_tier: None,
                thermal_tier: None,
            },
        ];
        for msg in cases {
            assert_eq!(msg.variant_name(), "MobileTelemetry");
            assert_eq!(msg.worker_id(), worker_id);

            // Plain serde_json goes through the human-readable mirror.
            let json = serde_json::to_string(&msg).expect("serialize json");
            assert!(json.contains("\"type\":\"MobileTelemetry\""), "{json}");
            let back: WorkerMessage = serde_json::from_str(&json).expect("deserialize json");
            assert_eq!(back, msg);

            for codec in [WireCodec::Bincode, WireCodec::Json] {
                let bytes = serialize_message(&msg, codec).expect("serialize");
                let (recovered, detected): (WorkerMessage, WireCodec) =
                    deserialize_message(&bytes).expect("deserialize");
                assert_eq!(recovered, msg);
                assert_eq!(detected, codec);
            }
        }

        // A JSON peer may omit the optional readings entirely.
        let sparse = format!(r#"{{"type":"MobileTelemetry","worker_id":"{worker_id}"}}"#);
        let parsed: WorkerMessage = serde_json::from_str(&sparse).expect("sparse json");
        assert_eq!(
            parsed,
            WorkerMessage::MobileTelemetry {
                worker_id,
                battery_pct: None,
                is_charging: None,
                thermal_throttled: false,
                battery_temperature: None,
                network_type: None,
                battery_tier: None,
                thermal_tier: None,
            }
        );
    }

    /// Bytes a transport read ahead of a handshake (the next frame sent right behind `RegisterAck`)
    /// must be delivered by the split reader at once, not stranded until more bytes arrive.
    #[tokio::test]
    async fn test_split_reader_decodes_frames_that_were_read_ahead_of_the_split() {
        use tokio::io::AsyncWriteExt;
        let (mut peer, ours) = tokio::io::duplex(64 * 1024);
        let ack = MasterMessage::HeartbeatAck { timestamp: 1 };
        let next = MasterMessage::HeartbeatAck { timestamp: 2 };
        // Both frames arrive in a single write, so one socket read buffers both.
        let mut both = Vec::new();
        for msg in [&ack, &next] {
            let payload = serialize_message(msg, WireCodec::Bincode).unwrap();
            both.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            both.extend_from_slice(&payload);
        }
        peer.write_all(&both).await.unwrap();

        let mut transport = MessageTransport::new(ours);
        assert_eq!(
            transport.recv_msg::<MasterMessage>().await.unwrap(),
            Some(ack)
        );

        // The peer sends nothing more. The second frame is already in the buffer.
        let (_writer, mut reader) = transport.split();
        let got = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            reader.recv_msg::<MasterMessage>(),
        )
        .await
        .expect("a frame read ahead of the split must not wait for more socket traffic")
        .unwrap();
        assert_eq!(got, Some(next));
    }

    #[test]
    fn test_auth_frames_are_not_typed_messages() {
        // The typed decoder must refuse handshake frames, never misparse them as messages.
        let frame = [WIRE_FORMAT_AUTH, 0x01, 1, 2, 3];
        assert!(matches!(
            deserialize_message::<WorkerMessage>(&frame),
            Err(ProtocolError::InvalidFormatTag(WIRE_FORMAT_AUTH))
        ));
        assert!(matches!(
            deserialize_message::<MasterMessage>(&frame),
            Err(ProtocolError::InvalidFormatTag(WIRE_FORMAT_AUTH))
        ));
        assert!(matches!(
            deserialize_message::<InboundMessage>(&frame),
            Err(ProtocolError::InvalidFormatTag(WIRE_FORMAT_AUTH))
        ));
    }
    #[test]
    fn test_master_feature_announcement_round_trips_and_old_masters_announce_nothing() {
        use super::master_features::{announced, announcement, MOBILE_TELEMETRY, SUPPORTED};

        let message = announcement(SUPPORTED);
        assert!(announced(Some(&message), MOBILE_TELEMETRY));
        assert!(announced(
            Some("features=other, mobile-telemetry"),
            MOBILE_TELEMETRY
        ));
        // A master built before the announcement sends no message on an accepted ack.
        assert!(!announced(None, MOBILE_TELEMETRY));
        assert!(!announced(Some(""), MOBILE_TELEMETRY));
        // Not a feature list, or a different one: nothing is assumed.
        assert!(!announced(Some("mobile-telemetry"), MOBILE_TELEMETRY));
        assert!(!announced(
            Some("features=mobile-telemetry-v2"),
            MOBILE_TELEMETRY
        ));
        assert!(!announced(Some("features="), MOBILE_TELEMETRY));

        // The announcement is just the `message` of an accepted RegisterAck: both codecs carry it.
        let ack = MasterMessage::RegisterAck {
            accepted: true,
            worker_id: Uuid::nil(),
            heartbeat_interval_secs: 3,
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
            message: Some(message),
        };
        for codec in [WireCodec::Json, WireCodec::Bincode] {
            let bytes = serialize_message(&ack, codec).unwrap();
            let (back, _): (MasterMessage, _) = deserialize_message(&bytes).unwrap();
            assert_eq!(back, ack, "{codec:?}");
        }
    }

    #[test]
    fn test_session_resume_feature_and_message_round_trip() {
        use super::master_features::{
            announced, announcement, parse_session_resume_hold_secs, session_resume_feature, BLOBS,
            MAX_SESSION_RESUME_HOLD_SECS, MOBILE_TELEMETRY, SESSION_RESUME_PREFIX,
        };

        assert_eq!(ProtocolCapabilities::SESSION_RESUME.bits(), 1 << 12);
        assert!(
            ProtocolCapabilities::all_supported().contains(ProtocolCapabilities::SESSION_RESUME),
            "SESSION_RESUME must be advertised in all_supported()"
        );
        assert_eq!(SESSION_RESUME_PREFIX, "session-resume=");
        assert_eq!(MAX_SESSION_RESUME_HOLD_SECS, 3_600);

        let token = session_resume_feature(30);
        assert_eq!(token, "session-resume=30");
        let msg = announcement(&[MOBILE_TELEMETRY, &token, BLOBS]);
        assert_eq!(msg, "features=mobile-telemetry,session-resume=30,blobs");
        assert!(announced(Some(&msg), MOBILE_TELEMETRY));
        assert!(announced(Some(&msg), BLOBS));
        assert_eq!(parse_session_resume_hold_secs(Some(&msg)), Some(30));
        assert_eq!(
            parse_session_resume_hold_secs(Some("features=session-resume=0")),
            Some(0)
        );
        assert_eq!(
            parse_session_resume_hold_secs(Some("features=mobile-telemetry, session-resume=45")),
            Some(45)
        );
        assert_eq!(
            parse_session_resume_hold_secs(Some("features=session-resume=3600")),
            Some(3_600)
        );
        assert_eq!(
            parse_session_resume_hold_secs(Some("features=session-resume=3601")),
            None
        );
        assert_eq!(
            parse_session_resume_hold_secs(Some(&format!("features=session-resume={}", u64::MAX))),
            None
        );
        assert_eq!(parse_session_resume_hold_secs(None), None);
        assert_eq!(parse_session_resume_hold_secs(Some("")), None);
        assert_eq!(
            parse_session_resume_hold_secs(Some("features=mobile-telemetry,blobs")),
            None
        );
        assert_eq!(
            parse_session_resume_hold_secs(Some("session-resume=30")),
            None
        );
        assert_eq!(
            parse_session_resume_hold_secs(Some("features=session-resume=")),
            None
        );
        assert_eq!(
            parse_session_resume_hold_secs(Some("features=session-resume=abc")),
            None
        );
        assert_eq!(
            parse_session_resume_hold_secs(Some("features=session-resume=-5")),
            None
        );

        let worker_id = Uuid::from_u128(0x1111_2222_3333_4444_5555_6666_7777_8888);
        let t1 = TaskId(Uuid::from_u128(0xaaaa));
        let t2 = TaskId(Uuid::from_u128(0xbbbb));
        let resume = WorkerMessage::ResumeSession {
            worker_id,
            running: vec![t1, t2],
        };
        assert_eq!(resume.variant_name(), "ResumeSession");
        assert_eq!(resume.worker_id(), worker_id);

        for codec in [WireCodec::Bincode, WireCodec::Json] {
            let bytes = serialize_message(&resume, codec).unwrap();
            let (decoded, detected): (WorkerMessage, _) = deserialize_message(&bytes).unwrap();
            assert_eq!(detected, codec);
            assert_eq!(decoded, resume);

            let (inbound, _): (InboundMessage, _) = deserialize_message(&bytes).unwrap();
            assert_eq!(inbound, InboundMessage::Worker(resume.clone()));
        }
    }

    /// The text of a `ProtocolError::Violation`; the record layer reports nothing else.
    fn violation_text(err: ProtocolError) -> String {
        match err {
            ProtocolError::Violation(text) => text,
            other => panic!("expected a protocol violation, got {other:?}"),
        }
    }

    fn server_reader(session_key: &[u8; 32]) -> AeadReaderSession {
        derive_aead_keys(session_key, EndpointRole::Server).1
    }

    #[test]
    fn test_aead_sessions_roundtrip_in_both_directions() {
        let session_key = [42u8; 32];
        let (mut client_writer, mut client_reader) =
            derive_aead_keys(&session_key, EndpointRole::Client);
        let (mut server_writer, mut server_reader) =
            derive_aead_keys(&session_key, EndpointRole::Server);

        for seq in 0..3u64 {
            let body = format!("client frame number {seq}");
            let encrypted = client_writer.encrypt_frame(body.as_bytes()).unwrap();
            assert_eq!(encrypted[0], WIRE_FORMAT_ENCRYPTED);
            assert_eq!(&encrypted[1..9], &seq.to_be_bytes());
            assert_eq!(encrypted.len(), body.len() + AEAD_RECORD_OVERHEAD);
            assert!(!encrypted.windows(body.len()).any(|w| w == body.as_bytes()));
            let decrypted = server_reader.decrypt_frame(&encrypted).unwrap();
            assert_eq!(&decrypted[..], body.as_bytes());

            let server_reply = server_writer.encrypt_frame(b"ack payload").unwrap();
            let client_decrypted = client_reader.decrypt_frame(&server_reply).unwrap();
            assert_eq!(&client_decrypted[..], b"ack payload");
        }
        assert_eq!(client_writer.current_seq(), 3);
        assert_eq!(server_reader.expected_seq(), 3);
    }

    #[test]
    fn test_aead_record_with_any_flipped_bit_is_refused_and_closes_the_session() {
        let session_key = [7u8; 32];
        let (mut writer, _) = derive_aead_keys(&session_key, EndpointRole::Client);
        let body = b"AssignTask: echo hello";
        let record = writer.encrypt_frame(body).unwrap();

        for index in 0..record.len() {
            for bit in [0u8, 3, 7] {
                let mut tampered = record.to_vec();
                tampered[index] ^= 1 << bit;
                let mut reader = server_reader(&session_key);
                let refused = violation_text(reader.decrypt_frame(&tampered).unwrap_err());
                // The header fails the format or sequence check, everything else Poly1305.
                let expected = match index {
                    0 => AEAD_ERR_NOT_ENCRYPTED,
                    1..=8 => AEAD_ERR_SEQUENCE,
                    _ => AEAD_ERR_INTEGRITY,
                };
                assert_eq!(refused, expected, "byte {index} bit {bit}");
                // Fail closed: after the forgery even the genuine record is refused.
                assert_eq!(
                    violation_text(reader.decrypt_frame(&record).unwrap_err()),
                    AEAD_ERR_CLOSED,
                    "byte {index} bit {bit}"
                );
            }
        }

        // Control: the untouched record opens.
        let mut reader = server_reader(&session_key);
        assert_eq!(&reader.decrypt_frame(&record).unwrap()[..], body);
    }

    #[test]
    fn test_aead_replayed_reordered_and_dropped_records_are_refused() {
        let session_key = [9u8; 32];
        let (mut writer, _) = derive_aead_keys(&session_key, EndpointRole::Client);
        let records: Vec<Bytes> = (0..3u8)
            .map(|i| writer.encrypt_frame(&[i; 8]).unwrap())
            .collect();

        // Replay: the same record twice. The session is closed afterwards.
        let mut reader = server_reader(&session_key);
        reader.decrypt_frame(&records[0]).unwrap();
        assert_eq!(
            violation_text(reader.decrypt_frame(&records[0]).unwrap_err()),
            AEAD_ERR_SEQUENCE
        );
        assert_eq!(
            violation_text(reader.decrypt_frame(&records[1]).unwrap_err()),
            AEAD_ERR_CLOSED
        );

        // Reorder: record 1 before record 0.
        let mut reader = server_reader(&session_key);
        assert_eq!(
            violation_text(reader.decrypt_frame(&records[1]).unwrap_err()),
            AEAD_ERR_SEQUENCE
        );

        // Drop: record 1 never arrives.
        let mut reader = server_reader(&session_key);
        reader.decrypt_frame(&records[0]).unwrap();
        assert_eq!(
            violation_text(reader.decrypt_frame(&records[2]).unwrap_err()),
            AEAD_ERR_SEQUENCE
        );

        // Control: in order all three open.
        let mut reader = server_reader(&session_key);
        for (i, record) in records.iter().enumerate() {
            assert_eq!(&reader.decrypt_frame(record).unwrap()[..], &[i as u8; 8]);
        }
    }

    #[test]
    fn test_aead_reflected_and_foreign_records_are_refused() {
        let session_key = [11u8; 32];
        let (mut client_w, mut client_r) = derive_aead_keys(&session_key, EndpointRole::Client);
        let (mut server_w, mut server_r) = derive_aead_keys(&session_key, EndpointRole::Server);

        // Reflection: a record sent back to its own sender, who opens with the other direction's
        // key. The sequence number matches, so only the key can stop it.
        let to_server = client_w.encrypt_frame(b"from the client").unwrap();
        assert_eq!(
            violation_text(client_r.decrypt_frame(&to_server).unwrap_err()),
            AEAD_ERR_INTEGRITY
        );
        let to_client = server_w.encrypt_frame(b"from the server").unwrap();
        assert_eq!(
            violation_text(server_r.decrypt_frame(&to_client).unwrap_err()),
            AEAD_ERR_INTEGRITY
        );

        // A record of another connection (other session key) with the expected sequence number.
        let (mut other_w, _) = derive_aead_keys(&[12u8; 32], EndpointRole::Client);
        let foreign = other_w.encrypt_frame(b"from the client").unwrap();
        assert_eq!(
            violation_text(
                server_reader(&session_key)
                    .decrypt_frame(&foreign)
                    .unwrap_err()
            ),
            AEAD_ERR_INTEGRITY
        );

        // Control: delivered to the right side, the record opens.
        assert_eq!(
            &server_reader(&session_key)
                .decrypt_frame(&to_server)
                .unwrap()[..],
            b"from the client"
        );
    }

    #[test]
    fn test_aead_sequence_counter_never_wraps() {
        let key = [13u8; 32];
        let mut writer = AeadWriterSession::starting_at(&key, u64::MAX - 1);
        let last = writer.encrypt_frame(b"last record").unwrap();
        assert_eq!(&last[1..9], &(u64::MAX - 1).to_be_bytes());
        assert_eq!(writer.current_seq(), u64::MAX);

        // `u64::MAX` is never used as a nonce and the counter does not wrap to 0: the writer
        // refuses for good.
        for _ in 0..2 {
            assert_eq!(
                violation_text(writer.encrypt_frame(b"one more").unwrap_err()),
                AEAD_ERR_OVERFLOW
            );
            assert_eq!(writer.current_seq(), u64::MAX);
        }

        let mut reader = AeadReaderSession::starting_at(&key, u64::MAX - 1);
        assert_eq!(&reader.decrypt_frame(&last).unwrap()[..], b"last record");
        assert_eq!(reader.expected_seq(), u64::MAX);
    }

    #[test]
    fn test_aead_oversized_frame_is_refused_without_using_a_sequence_number() {
        let key = [14u8; 32];
        let mut writer = AeadWriterSession::new(&key);
        let mut reader = AeadReaderSession::new(&key);

        match writer.encrypt_frame(&vec![0u8; MAX_FRAME_SIZE - AEAD_RECORD_OVERHEAD + 1]) {
            Err(ProtocolError::FrameTooLarge { size, max }) => {
                assert_eq!((size, max), (MAX_FRAME_SIZE + 1, MAX_FRAME_SIZE));
            }
            other => panic!(
                "expected FrameTooLarge, got {:?}",
                other.map(|record| record.len())
            ),
        }
        assert_eq!(
            writer.current_seq(),
            0,
            "a refused frame must not use a nonce"
        );

        // The direction is still in sync: the next record carries sequence number 0 and opens.
        let record = writer.encrypt_frame(b"small").unwrap();
        assert_eq!(&reader.decrypt_frame(&record).unwrap()[..], b"small");
    }

    #[test]
    fn test_aead_reader_refuses_truncated_and_unencrypted_frames() {
        let key = [15u8; 32];
        let header = [vec![WIRE_FORMAT_ENCRYPTED], 0u64.to_be_bytes().to_vec()].concat();
        let cases: Vec<(&str, Vec<u8>, &str)> = vec![
            ("empty", vec![], AEAD_ERR_TOO_SHORT),
            ("header only", header.clone(), AEAD_ERR_TOO_SHORT),
            (
                "one byte short of a tag",
                [header.clone(), vec![0u8; 15]].concat(),
                AEAD_ERR_TOO_SHORT,
            ),
            (
                "header and a zero tag",
                [header, vec![0u8; 16]].concat(),
                AEAD_ERR_INTEGRITY,
            ),
            (
                "plaintext bincode",
                [vec![WIRE_FORMAT_BINCODE], vec![7u8; 40]].concat(),
                AEAD_ERR_NOT_ENCRYPTED,
            ),
            (
                "plaintext json",
                br#"{"type":"ClusterStatus","padding":"............"}"#.to_vec(),
                AEAD_ERR_NOT_ENCRYPTED,
            ),
            (
                "handshake frame",
                [vec![WIRE_FORMAT_AUTH, 0x01, 2], vec![1u8; 32]].concat(),
                AEAD_ERR_NOT_ENCRYPTED,
            ),
        ];
        for (name, frame, expected) in cases {
            let mut reader = AeadReaderSession::new(&key);
            // Fixed texts: nothing from the frame (sequence number, bytes) is echoed.
            let text = violation_text(reader.decrypt_frame(&frame).unwrap_err());
            assert_eq!(text, expected, "{name}");
        }
    }

    #[tokio::test]
    async fn test_encrypted_transport_duplex_and_rejection() {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let mut client = MessageTransport::new(client_io);
        let mut server = MessageTransport::new(server_io);

        let session_key = [0x55u8; 32];
        client.enable_encryption(&session_key, EndpointRole::Client);
        server.enable_encryption(&session_key, EndpointRole::Server);

        assert!(client.is_encrypted());
        assert!(server.is_encrypted());

        let wid = Uuid::new_v4();
        let reg = WorkerMessage::register(
            wid,
            WorkerCapabilities::new("sec-worker", 4, 8192, false, false, None),
        );

        client.send_msg(&reg).await.unwrap();
        let recv_reg: WorkerMessage = server.recv_msg().await.unwrap().unwrap();
        assert_eq!(recv_reg, reg);

        let ack = MasterMessage::register_ack(
            true,
            wid,
            5,
            ProtocolCapabilities::all_supported(),
            Some("secure ok".into()),
        );
        server.send_msg(&ack).await.unwrap();
        let recv_ack: MasterMessage = client.recv_msg().await.unwrap().unwrap();
        assert_eq!(recv_ack, ack);
    }

    #[tokio::test]
    async fn test_encrypted_transport_drops_a_tampered_frame_for_good() {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let mut client = MessageTransport::new(client_io);
        let mut server = MessageTransport::new(server_io);
        let session_key = [0x56u8; 32];
        client.enable_encryption(&session_key, EndpointRole::Client);
        server.enable_encryption(&session_key, EndpointRole::Server);

        // What an on-path observer sees: a record, without the plaintext.
        let marker = "plaintext-marker-7f3a";
        let bye = WorkerMessage::Disconnecting {
            worker_id: Uuid::new_v4(),
            reason: marker.into(),
        };
        client.send_msg(&bye).await.unwrap();
        let on_wire = server.recv_raw_frame().await.unwrap().unwrap();
        assert_eq!(on_wire[0], WIRE_FORMAT_ENCRYPTED);
        assert!(!on_wire
            .windows(marker.len())
            .any(|w| w == marker.as_bytes()));

        // An attacker flips one bit of that record in transit: the receiver refuses it...
        let mut tampered = on_wire.to_vec();
        tampered[on_wire.len() / 2] ^= 0x01;
        client.send_raw_frame(Bytes::from(tampered)).await.unwrap();
        assert_eq!(
            violation_text(server.recv_msg::<WorkerMessage>().await.unwrap_err()),
            AEAD_ERR_INTEGRITY
        );
        // ...and nothing gets through afterwards, neither the genuine record nor a new one.
        client.send_raw_frame(on_wire).await.unwrap();
        assert_eq!(
            violation_text(server.recv_msg::<WorkerMessage>().await.unwrap_err()),
            AEAD_ERR_CLOSED
        );
        client.send_msg(&bye).await.unwrap();
        assert_eq!(
            violation_text(server.recv_msg::<WorkerMessage>().await.unwrap_err()),
            AEAD_ERR_CLOSED
        );
    }

    #[tokio::test]
    async fn test_encrypted_split_reader_refuses_an_injected_plaintext_assignment() {
        // An on-path host injects a well-formed plaintext `AssignTask` into an encrypted
        // master -> worker connection (the worker reads through the split reader half).
        let (attacker_io, worker_io) = tokio::io::duplex(64 * 1024);
        let mut attacker = MessageTransport::new(attacker_io);
        let mut worker = MessageTransport::new(worker_io);
        worker.enable_encryption(&[0x66u8; 32], EndpointRole::Client);
        let (_worker_tx, mut worker_rx) = worker.split();
        assert!(worker_rx.is_encrypted());

        let task = Task::new(
            TaskSpec::new_shell_script("touch /tmp/injected"),
            TaskRequirements::generic(1, 30),
        );
        attacker
            .send_msg(&MasterMessage::AssignTask { task })
            .await
            .unwrap();
        assert_eq!(
            violation_text(worker_rx.recv_msg::<MasterMessage>().await.unwrap_err()),
            AEAD_ERR_NOT_ENCRYPTED
        );
    }

    #[test]
    fn test_unknown_variant_graceful_handling() {
        // Unknown future WorkerMessage tag (e.g. tag 99) in Bincode
        let mut buf = vec![WIRE_FORMAT_BINCODE];
        buf.extend_from_slice(&99u32.to_le_bytes());
        buf.extend_from_slice(b"future-payload-bytes");

        let (msg, codec): (WorkerMessage, _) = deserialize_message(&buf).unwrap();
        assert_eq!(codec, WireCodec::Bincode);
        match msg {
            WorkerMessage::UnknownVariant { tag, raw_bytes } => {
                assert_eq!(tag, 99);
                assert_eq!(raw_bytes, b"future-payload-bytes");
            }
            other => panic!("expected UnknownVariant, got {other:?}"),
        }

        // InboundMessage wrapping unknown variant
        let (inbound, _): (InboundMessage, _) = deserialize_message(&buf).unwrap();
        match inbound {
            InboundMessage::Worker(WorkerMessage::UnknownVariant { tag, raw_bytes }) => {
                assert_eq!(tag, 99);
                assert_eq!(raw_bytes, b"future-payload-bytes");
            }
            other => panic!("expected InboundMessage::Worker(UnknownVariant), got {other:?}"),
        }

        // Unknown future MasterMessage tag in Bincode
        let mut master_buf = vec![WIRE_FORMAT_BINCODE];
        master_buf.extend_from_slice(&88u32.to_le_bytes());
        master_buf.extend_from_slice(b"future-master-payload");
        let (m_msg, _): (MasterMessage, _) = deserialize_message(&master_buf).unwrap();
        match m_msg {
            MasterMessage::UnknownVariant { tag, raw_bytes } => {
                assert_eq!(tag, 88);
                assert_eq!(raw_bytes, b"future-master-payload");
            }
            other => panic!("expected MasterMessage::UnknownVariant, got {other:?}"),
        }
    }

    #[test]
    fn test_cluster_status_trailing_budget_fields_decode_into_legacy_bincode_client() {
        // F36-1: `active_bytes` and `max_active_bytes` must sit after `workers` in
        // `BinaryClientResponse::ClusterStatus` so a legacy bincode client (whose
        // `ClusterStatus` ends at `workers`) decodes the prefix cleanly and ignores
        // the trailing 16 bytes even when tasks are queued (`active_bytes > 0`).
        #[derive(Deserialize, Debug)]
        #[allow(dead_code)]
        enum LegacyBinaryClientResponse {
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
            },
        }

        let worker_cap = WorkerCapabilities::new("w1", 8, 16_384, false, false, None);
        let msg = ClientResponse::ClusterStatus {
            total_tasks: 3,
            pending_tasks: 2,
            running_tasks: 1,
            completed_tasks: 0,
            failed_tasks: 0,
            workers: vec![worker_cap],
            active_bytes: 601_029,
            max_active_bytes: 1_048_576,
            store_error: None,
        };
        let encoded = serialize_message(&msg, WireCodec::Bincode).unwrap();
        assert_eq!(encoded[0], WIRE_FORMAT_BINCODE);

        // Decode with the legacy bincode schema (which uses `bincode::deserialize_from` on a slice
        // reader, just like `deserialize_message` in `protocol.rs`).
        let mut cursor = std::io::Cursor::new(&encoded[1..]);
        let legacy: LegacyBinaryClientResponse = bincode::deserialize_from(&mut cursor).unwrap();
        match legacy {
            LegacyBinaryClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                workers,
            } => {
                assert_eq!(total_tasks, 3);
                assert_eq!(pending_tasks, 2);
                assert_eq!(running_tasks, 1);
                assert_eq!(completed_tasks, 0);
                assert_eq!(failed_tasks, 0);
                assert_eq!(workers.len(), 1);
                assert_eq!(workers[0].name, "w1");
            }
            other => panic!("expected LegacyBinaryClientResponse::ClusterStatus, got {other:?}"),
        }
    }

    #[test]
    fn test_all_supported_does_not_advertise_role_arbitrage() {
        assert!(
            !ProtocolCapabilities::all_supported().contains(ProtocolCapabilities::ROLE_ARBITRAGE),
            "ROLE_ARBITRAGE is design-only and must not be advertised in all_supported()"
        );
    }

    #[test]
    fn test_client_message_and_response_version_skew_refused_in_both_codecs() {
        let req = ClientMessage::ClusterStatus;
        let resp = ClientResponse::TaskSubmitted {
            task_id: TaskId::new(),
        };

        for codec in [WireCodec::Bincode, WireCodec::Json] {
            for bad_ver in [
                1u32,
                2,
                PROTOCOL_VERSION_CURRENT - 1,
                PROTOCOL_VERSION_CURRENT + 1,
                99,
            ] {
                let raw_req = serialize_client_message_with_version(&req, bad_ver, codec).unwrap();
                match deserialize_message::<ClientMessage>(&raw_req) {
                    Err(ProtocolError::IncompatibleVersion {
                        peer_version,
                        current_version,
                        min_compatible_version,
                    }) => {
                        assert_eq!(peer_version, bad_ver);
                        assert_eq!(current_version, PROTOCOL_VERSION_CURRENT);
                        assert_eq!(min_compatible_version, PROTOCOL_VERSION_MIN_COMPATIBLE);
                    }
                    other => panic!(
                        "expected IncompatibleVersion({bad_ver}) for {codec:?}, got {other:?}"
                    ),
                }

                match deserialize_message::<InboundMessage>(&raw_req) {
                    Err(ProtocolError::IncompatibleVersion {
                        peer_version,
                        current_version,
                        min_compatible_version,
                    }) => {
                        assert_eq!(peer_version, bad_ver);
                        assert_eq!(current_version, PROTOCOL_VERSION_CURRENT);
                        assert_eq!(min_compatible_version, PROTOCOL_VERSION_MIN_COMPATIBLE);
                    }
                    other => panic!(
                        "expected InboundMessage IncompatibleVersion({bad_ver}) for {codec:?}, got {other:?}"
                    ),
                }

                let raw_resp =
                    serialize_client_response_with_version(&resp, bad_ver, codec).unwrap();
                match deserialize_message::<ClientResponse>(&raw_resp) {
                    Err(ProtocolError::IncompatibleVersion {
                        peer_version,
                        current_version,
                        min_compatible_version,
                    }) => {
                        assert_eq!(peer_version, bad_ver);
                        assert_eq!(current_version, PROTOCOL_VERSION_CURRENT);
                        assert_eq!(min_compatible_version, PROTOCOL_VERSION_MIN_COMPATIBLE);
                    }
                    other => panic!(
                        "expected ClientResponse IncompatibleVersion({bad_ver}) for {codec:?}, got {other:?}"
                    ),
                }

                // ClientResponse::Error is always decoded regardless of protocol_version so a
                // rejected client can display the Master's refusal message.
                let err_resp = ClientResponse::Error {
                    message: format_incompatible_version_error("client", bad_ver),
                };
                let raw_err =
                    serialize_client_response_with_version(&err_resp, bad_ver, codec).unwrap();
                let (decoded_err, detected) =
                    deserialize_message::<ClientResponse>(&raw_err).unwrap();
                assert_eq!(detected, codec);
                assert_eq!(decoded_err, err_resp);
            }
        }

        // Also verify that an unversioned legacy v2 bincode ClientMessage (4-byte ClusterStatus
        // tag only) is recognized as peer_version = 2 rather than failing with an opaque EOF error.
        let mut legacy_v2_cluster_status = vec![WIRE_FORMAT_BINCODE];
        legacy_v2_cluster_status.extend_from_slice(&3u32.to_le_bytes());
        match deserialize_message::<InboundMessage>(&legacy_v2_cluster_status) {
            Err(ProtocolError::IncompatibleVersion { peer_version, .. }) => {
                assert_eq!(peer_version, 2);
            }
            other => panic!("expected IncompatibleVersion(2) for legacy v2 frame, got {other:?}"),
        }
    }

    #[test]
    fn test_unversioned_json_client_message_and_response_refused_with_version_2() {
        let raw_json_req = br#"{"type":"ClusterStatus"}"#;
        let mut framed_json_req = vec![WIRE_FORMAT_JSON];
        framed_json_req.extend_from_slice(raw_json_req);

        for frame in [raw_json_req.as_slice(), framed_json_req.as_slice()] {
            match deserialize_message::<ClientMessage>(frame) {
                Err(ProtocolError::IncompatibleVersion { peer_version, .. }) => {
                    assert_eq!(peer_version, LEGACY_UNVERSIONED_PROTOCOL_VERSION);
                    assert_eq!(peer_version, 2);
                }
                other => {
                    panic!("expected IncompatibleVersion(2) for unversioned JSON ClientMessage, got {other:?}")
                }
            }
            match deserialize_message::<InboundMessage>(frame) {
                Err(ProtocolError::IncompatibleVersion { peer_version, .. }) => {
                    assert_eq!(peer_version, LEGACY_UNVERSIONED_PROTOCOL_VERSION);
                    assert_eq!(peer_version, 2);
                }
                other => {
                    panic!("expected IncompatibleVersion(2) for unversioned JSON InboundMessage, got {other:?}")
                }
            }
        }

        let raw_json_resp = br#"{"type":"ClusterStatus","total_tasks":0,"pending_tasks":0,"running_tasks":0,"completed_tasks":0,"failed_tasks":0,"workers":[]}"#;
        let mut framed_json_resp = vec![WIRE_FORMAT_JSON];
        framed_json_resp.extend_from_slice(raw_json_resp);

        for frame in [raw_json_resp.as_slice(), framed_json_resp.as_slice()] {
            match deserialize_message::<ClientResponse>(frame) {
                Err(ProtocolError::IncompatibleVersion { peer_version, .. }) => {
                    assert_eq!(peer_version, LEGACY_UNVERSIONED_PROTOCOL_VERSION);
                    assert_eq!(peer_version, 2);
                }
                other => {
                    panic!("expected IncompatibleVersion(2) for unversioned JSON ClientResponse, got {other:?}")
                }
            }
        }

        // Unversioned JSON Register and RegisterAck also default to protocol_version = 2 and fail
        // `is_protocol_version_compatible`.
        let raw_json_register = br#"{"type":"Register","worker_id":"00000000-0000-0000-0000-000000000001","capabilities":{"name":"w","cpu_cores":2,"ram_mb":1024,"has_gpu":false,"is_simulated_gpu":false,"tags":[]}}"#;
        let (decoded_reg, _) = deserialize_message::<WorkerMessage>(raw_json_register).unwrap();
        match decoded_reg {
            WorkerMessage::Register {
                protocol_version, ..
            } => {
                assert_eq!(protocol_version, LEGACY_UNVERSIONED_PROTOCOL_VERSION);
                assert!(!is_protocol_version_compatible(protocol_version));
            }
            other => panic!("expected WorkerMessage::Register, got {other:?}"),
        }

        let raw_json_ack = br#"{"type":"RegisterAck","accepted":true,"worker_id":"00000000-0000-0000-0000-000000000001","heartbeat_interval_secs":5}"#;
        let (decoded_ack, _) = deserialize_message::<MasterMessage>(raw_json_ack).unwrap();
        match decoded_ack {
            MasterMessage::RegisterAck {
                protocol_version, ..
            } => {
                assert_eq!(protocol_version, LEGACY_UNVERSIONED_PROTOCOL_VERSION);
                assert!(!is_protocol_version_compatible(protocol_version));
            }
            other => panic!("expected MasterMessage::RegisterAck, got {other:?}"),
        }
    }

    // --- Protocol version 6: Task::workspace, Hello (design 0039 W1) ---------------------------

    fn sample_workspace_task() -> Task {
        Task::new(
            TaskSpec::new_command("cargo", vec!["test".into()]),
            crate::task::TaskRequirements::default(),
        )
    }

    /// The bytes a v5 client sent for `SubmitTask`: the v6 frame without the `workspace` tag byte
    /// (`None`), which sits just before the one-byte `wait` and the four-byte version trailer.
    fn v5_submit_task_frame(wait: bool) -> Vec<u8> {
        let msg = ClientMessage::SubmitTask {
            task: sample_workspace_task(),
            wait,
        };
        let raw = serialize_client_message_with_version(&msg, 5, WireCodec::Bincode)
            .unwrap()
            .to_vec();
        let len = raw.len();
        assert_eq!(raw[len - 6], 0, "workspace None tag byte");
        let mut v5 = raw[..len - 6].to_vec();
        v5.extend_from_slice(&raw[len - 5..]);
        v5
    }

    #[test]
    fn test_protocol_6_hello_round_trips_in_both_codecs_and_is_appended_last() {
        assert_eq!(PROTOCOL_VERSION_CURRENT, 6);
        assert_eq!(PROTOCOL_VERSION_MIN_COMPATIBLE, 6);
        for codec in [WireCodec::Bincode, WireCodec::Json] {
            let raw = serialize_message(&ClientMessage::Hello, codec).unwrap();
            let (msg, detected) = deserialize_message::<ClientMessage>(&raw).unwrap();
            assert_eq!(msg, ClientMessage::Hello);
            assert_eq!(detected, codec);
            let (inbound, _) = deserialize_message::<InboundMessage>(&raw).unwrap();
            assert!(matches!(
                inbound,
                InboundMessage::Client(ClientMessage::Hello)
            ));

            let resp = ClientResponse::Hello {
                min_compatible_version: PROTOCOL_VERSION_MIN_COMPATIBLE,
            };
            let raw = serialize_message(&resp, codec).unwrap();
            let (back, detected) = deserialize_message::<ClientResponse>(&raw).unwrap();
            assert_eq!(back, resp);
            assert_eq!(detected, codec);
        }
        // Bincode variant indices: Hello is the ninth request and the eleventh response, so every
        // earlier index is unchanged.
        let req = serialize_message(&ClientMessage::Hello, WireCodec::Bincode).unwrap();
        assert_eq!(&req[1..5], &8u32.to_le_bytes());
        let resp = serialize_message(
            &ClientResponse::Hello {
                min_compatible_version: 6,
            },
            WireCodec::Bincode,
        )
        .unwrap();
        assert_eq!(&resp[1..5], &10u32.to_le_bytes());
    }

    #[test]
    fn test_workspaces_bit_is_reserved_and_not_advertised() {
        assert_eq!(ProtocolCapabilities::WORKSPACES.0, 1 << 10);
        assert!(!ProtocolCapabilities::all_supported().contains(ProtocolCapabilities::WORKSPACES));
    }

    #[test]
    fn test_task_workspace_round_trips_in_both_codecs() {
        use crate::task::{WorkspaceRef, WorkspaceSource};
        let source = WorkspaceSource::Tree {
            manifest: crate::blob::BlobId([7; 32]),
            manifest_size: 99,
        };
        for workspace in [
            None,
            Some(WorkspaceRef::new("agent-1")),
            Some(WorkspaceRef {
                name: "agent-1".into(),
                source: Some(source),
            }),
        ] {
            let mut task = sample_workspace_task();
            task.workspace = workspace;
            let msg = ClientMessage::SubmitTask {
                task: task.clone(),
                wait: true,
            };
            for codec in [WireCodec::Bincode, WireCodec::Json] {
                let raw = serialize_message(&msg, codec).unwrap();
                let (back, _) = deserialize_message::<ClientMessage>(&raw).unwrap();
                assert_eq!(back, msg, "{codec:?}");
            }
        }
    }

    #[test]
    fn test_v5_submit_task_at_a_v6_master_is_refused_as_version_5() {
        for wait in [false, true] {
            let mut frame = v5_submit_task_frame(wait);
            // Not decodable with the v6 layout, so the opaque error would have been a bincode one.
            assert!(bincode_exact_deserialize::<BinaryClientMessage>(&frame[1..]).is_err());
            match deserialize_message::<ClientMessage>(&frame) {
                Err(ProtocolError::IncompatibleVersion {
                    peer_version,
                    current_version,
                    min_compatible_version,
                }) => {
                    assert_eq!(peer_version, 5);
                    assert_eq!(current_version, PROTOCOL_VERSION_CURRENT);
                    assert_eq!(min_compatible_version, PROTOCOL_VERSION_MIN_COMPATIBLE);
                }
                other => panic!("expected IncompatibleVersion(5), got {other:?}"),
            }
            match deserialize_message::<InboundMessage>(&frame) {
                Err(ProtocolError::IncompatibleVersion { peer_version, .. }) => {
                    assert_eq!(peer_version, 5);
                }
                other => panic!("expected InboundMessage IncompatibleVersion(5), got {other:?}"),
            }
            // A v4 client's frame had the same shape; its own version is reported.
            let len = frame.len();
            frame[len - 4..].copy_from_slice(&4u32.to_le_bytes());
            match deserialize_message::<ClientMessage>(&frame) {
                Err(ProtocolError::IncompatibleVersion { peer_version, .. }) => {
                    assert_eq!(peer_version, 4);
                }
                other => panic!("expected IncompatibleVersion(4), got {other:?}"),
            }
        }
    }

    #[test]
    fn test_garbage_submit_task_is_still_an_opaque_decode_error() {
        let mut frame = v5_submit_task_frame(true);
        frame.truncate(frame.len() - 7);
        assert!(matches!(
            deserialize_message::<ClientMessage>(&frame),
            Err(ProtocolError::Bincode(_))
        ));
    }

    #[test]
    fn test_v5_non_task_requests_at_a_v6_master_are_refused_as_version_5() {
        // These have the v6 layout and are refused by the version check itself.
        for msg in [
            ClientMessage::ClusterStatus,
            ClientMessage::ListWorkers,
            ClientMessage::GetTaskStatus {
                task_id: TaskId::new(),
            },
        ] {
            for codec in [WireCodec::Bincode, WireCodec::Json] {
                let raw = serialize_client_message_with_version(&msg, 5, codec).unwrap();
                match deserialize_message::<InboundMessage>(&raw) {
                    Err(ProtocolError::IncompatibleVersion { peer_version, .. }) => {
                        assert_eq!(peer_version, 5, "{msg:?} {codec:?}");
                    }
                    other => panic!("expected IncompatibleVersion(5), got {other:?}"),
                }
            }
        }
    }

    #[test]
    fn test_v5_json_submit_task_without_workspace_is_refused_as_version_5() {
        let task = serde_json::to_value(sample_workspace_task()).unwrap();
        let mut task = task;
        task.as_object_mut().unwrap().remove("workspace");
        let frame = serde_json::json!({
            "type": "SubmitTask",
            "task": task,
            "wait": true,
            "protocol_version": 5,
        });
        let mut raw = vec![WIRE_FORMAT_JSON];
        raw.extend_from_slice(frame.to_string().as_bytes());
        match deserialize_message::<ClientMessage>(&raw) {
            Err(ProtocolError::IncompatibleVersion { peer_version, .. }) => {
                assert_eq!(peer_version, 5);
            }
            other => panic!("expected IncompatibleVersion(5), got {other:?}"),
        }
    }

    #[test]
    fn test_v5_responses_at_a_v6_client_are_refused_as_version_5() {
        // A v5 Master answers with the v6 response layout plus a version-5 trailer (Hello was
        // appended, nothing moved), so the client reads the version and refuses it. This includes
        // the ClusterStatus a v5 Master sends back to the old probe.
        let responses = [
            ClientResponse::ClusterStatus {
                total_tasks: 1,
                pending_tasks: 0,
                running_tasks: 0,
                completed_tasks: 1,
                failed_tasks: 0,
                workers: Vec::new(),
                active_bytes: 0,
                max_active_bytes: 1,
                store_error: Some("disk".into()),
            },
            ClientResponse::WorkerList {
                workers: Vec::new(),
            },
            ClientResponse::TaskSubmitted {
                task_id: TaskId::new(),
            },
        ];
        for resp in responses {
            for codec in [WireCodec::Bincode, WireCodec::Json] {
                let raw = serialize_client_response_with_version(&resp, 5, codec).unwrap();
                match deserialize_message::<ClientResponse>(&raw) {
                    Err(ProtocolError::IncompatibleVersion {
                        peer_version,
                        current_version,
                        ..
                    }) => {
                        assert_eq!(peer_version, 5, "{resp:?} {codec:?}");
                        assert_eq!(current_version, PROTOCOL_VERSION_CURRENT);
                    }
                    other => panic!("expected IncompatibleVersion(5), got {other:?}"),
                }
            }
        }
        // A Hello answer stamped v5 is refused as well.
        let hello = ClientResponse::Hello {
            min_compatible_version: 5,
        };
        let raw = serialize_client_response_with_version(&hello, 5, WireCodec::Bincode).unwrap();
        assert!(matches!(
            deserialize_message::<ClientResponse>(&raw),
            Err(ProtocolError::IncompatibleVersion {
                peer_version: 5,
                ..
            })
        ));
    }
}
