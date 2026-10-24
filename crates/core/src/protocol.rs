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
pub const PROTOCOL_VERSION_CURRENT: u32 = 1;
/// Minimum wire protocol version compatible with this build.
pub const PROTOCOL_VERSION_MIN_COMPATIBLE: u32 = 1;

/// Default protocol version used in deserialization fallbacks.
pub const fn default_protocol_version() -> u32 {
    PROTOCOL_VERSION_CURRENT
}

/// Negotiable protocol capabilities and feature flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProtocolCapabilities(pub u64);

impl ProtocolCapabilities {
    pub const NONE: Self = Self(0);
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

    pub const fn all_supported() -> Self {
        Self(
            Self::AEAD_ENCRYPTION.0
                | Self::MOBILE_TELEMETRY.0
                | Self::CHECKPOINT_DELTA.0
                | Self::GPU_SCHEDULE_PRIO.0
                | Self::EXTENDED_ERROR_INFO.0
                | Self::BATCH_DISPATCH.0,
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

/// AEAD encryption session for sealing outbound frames.
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

    /// Encrypts `plaintext` into a framed encrypted payload:
    /// `[0x04][seq: 8 bytes BE][ciphertext][16-byte Poly1305 tag]`
    pub fn encrypt_frame(&mut self, plaintext: &[u8]) -> Result<Bytes, ProtocolError> {
        let seq = self.seq;
        self.seq = self
            .seq
            .checked_add(1)
            .ok_or_else(|| ProtocolError::Violation("AEAD sequence counter overflow".into()))?;

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

        if frame.len() > MAX_FRAME_SIZE {
            return Err(ProtocolError::FrameTooLarge {
                size: frame.len(),
                max: MAX_FRAME_SIZE,
            });
        }

        Ok(Bytes::from(frame))
    }

    /// Returns the next sequence number to be sent.
    pub fn current_seq(&self) -> u64 {
        self.seq
    }
}

/// AEAD decryption session for opening inbound frames.
pub struct AeadReaderSession {
    key: LessSafeKey,
    expected_seq: u64,
}

impl AeadReaderSession {
    /// Creates a new reader session with a 32-byte symmetric key.
    pub fn new(read_key: &[u8; 32]) -> Self {
        let unbound = UnboundKey::new(&CHACHA20_POLY1305, read_key)
            .expect("valid 32-byte key for ChaCha20-Poly1305");
        Self {
            key: LessSafeKey::new(unbound),
            expected_seq: 0,
        }
    }

    /// Decrypts a framed encrypted payload:
    /// `[0x04][seq: 8 bytes BE][ciphertext][16-byte tag]`
    /// and returns the decrypted plaintext bytes.
    pub fn decrypt_frame(&mut self, frame: &[u8]) -> Result<Bytes, ProtocolError> {
        const TAG_LEN: usize = 16;
        const HEADER_LEN: usize = 9; // 1 byte format + 8 bytes seq
        const MIN_ENCRYPTED_LEN: usize = HEADER_LEN + TAG_LEN;

        if frame.len() < MIN_ENCRYPTED_LEN {
            return Err(ProtocolError::Violation(format!(
                "Encrypted frame too short: {} bytes (minimum {MIN_ENCRYPTED_LEN})",
                frame.len()
            )));
        }

        if frame[0] != WIRE_FORMAT_ENCRYPTED {
            return Err(ProtocolError::Violation(format!(
                "Expected encrypted wire format (0x04), received 0x{:02x}",
                frame[0]
            )));
        }

        let seq = u64::from_be_bytes(frame[1..9].try_into().unwrap());
        if seq != self.expected_seq {
            return Err(ProtocolError::Violation(format!(
                "Anti-replay check failed: expected sequence number {}, received {}",
                self.expected_seq, seq
            )));
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
            .map_err(|_| {
                ProtocolError::Violation("Ciphertext integrity verification failed".into())
            })?;

        let plaintext = Bytes::copy_from_slice(plaintext_slice);
        self.expected_seq = self
            .expected_seq
            .checked_add(1)
            .ok_or_else(|| ProtocolError::Violation("AEAD sequence counter overflow".into()))?;

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
        match s.to_ascii_lowercase().as_str() {
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
    },
    /// Forward-compatible fallback variant for unrecognized messages from future protocol versions.
    UnknownVariant { tag: u32, raw_bytes: Vec<u8> },
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
        #[serde(default, with = "crate::task::opt_bytes_serde", skip_serializing_if = "Option::is_none")]
        asset_bytes: Option<TaskBytes>,
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
    },
    #[serde(skip_deserializing)]
    UnknownVariant {
        tag: u32,
        raw_bytes: Vec<u8>,
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
    UnknownVariant {
        tag: u32,
        raw_bytes: Vec<u8>,
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
        #[serde(default, with = "crate::task::opt_bytes_serde")]
        asset_bytes: Option<TaskBytes>,
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
            } => HumanWorkerMessage::MobileTelemetry {
                worker_id,
                battery_pct,
                is_charging,
                thermal_throttled,
            },
            WorkerMessage::UnknownVariant { tag, raw_bytes } => {
                HumanWorkerMessage::UnknownVariant { tag, raw_bytes }
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
            } => WorkerMessage::MobileTelemetry {
                worker_id,
                battery_pct,
                is_charging,
                thermal_throttled,
            },
            HumanWorkerMessage::UnknownVariant { tag, raw_bytes } => {
                WorkerMessage::UnknownVariant { tag, raw_bytes }
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
            } => BinaryWorkerMessage::MobileTelemetry {
                worker_id,
                battery_pct,
                is_charging,
                thermal_throttled,
            },
            WorkerMessage::UnknownVariant { tag, raw_bytes } => {
                BinaryWorkerMessage::UnknownVariant { tag, raw_bytes }
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
            } => WorkerMessage::MobileTelemetry {
                worker_id,
                battery_pct,
                is_charging,
                thermal_throttled,
            },
            BinaryWorkerMessage::UnknownVariant { tag, raw_bytes } => {
                WorkerMessage::UnknownVariant { tag, raw_bytes }
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
            Self::UnknownVariant { .. } => "UnknownVariant",
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
            Self::UnknownVariant { .. } => Uuid::nil(),
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
    /// Forward-compatible fallback variant for unrecognized messages from future protocol versions.
    UnknownVariant { tag: u32, raw_bytes: Vec<u8> },
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

    /// Every feature this build of the Master supports.
    pub const SUPPORTED: &[&str] = &[MOBILE_TELEMETRY];

    const PREFIX: &str = "features=";

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
    #[serde(skip_deserializing)]
    UnknownVariant {
        tag: u32,
        raw_bytes: Vec<u8>,
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
    UnknownVariant {
        tag: u32,
        raw_bytes: Vec<u8>,
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
            MasterMessage::UnknownVariant { tag, raw_bytes } => {
                HumanMasterMessage::UnknownVariant { tag, raw_bytes }
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
            HumanMasterMessage::UnknownVariant { tag, raw_bytes } => {
                MasterMessage::UnknownVariant { tag, raw_bytes }
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
            MasterMessage::UnknownVariant { tag, raw_bytes } => {
                BinaryMasterMessage::UnknownVariant { tag, raw_bytes }
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
            BinaryMasterMessage::UnknownVariant { tag, raw_bytes } => {
                MasterMessage::UnknownVariant { tag, raw_bytes }
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
            Self::UnknownVariant { .. } => "UnknownVariant",
        }
    }
}

/// Messages sent from CLI client (or external RPC) to the Master node.
#[derive(Debug, Clone, PartialEq)]
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
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum HumanClientMessage {
    SubmitTask {
        task: Task,
        #[serde(default)]
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
}

#[derive(Serialize, Deserialize)]
enum BinaryClientMessage {
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
}

impl From<ClientMessage> for HumanClientMessage {
    fn from(msg: ClientMessage) -> Self {
        match msg {
            ClientMessage::SubmitTask { task, wait } => {
                HumanClientMessage::SubmitTask { task, wait }
            }
            ClientMessage::GetTaskStatus { task_id } => {
                HumanClientMessage::GetTaskStatus { task_id }
            }
            ClientMessage::CancelTask { task_id } => HumanClientMessage::CancelTask { task_id },
            ClientMessage::ClusterStatus => HumanClientMessage::ClusterStatus,
            ClientMessage::ListWorkers => HumanClientMessage::ListWorkers,
            ClientMessage::SubmitMapReduce { job } => HumanClientMessage::SubmitMapReduce { job },
            ClientMessage::GetTaskResult { task_id } => {
                HumanClientMessage::GetTaskResult { task_id }
            }
        }
    }
}

impl From<HumanClientMessage> for ClientMessage {
    fn from(msg: HumanClientMessage) -> Self {
        match msg {
            HumanClientMessage::SubmitTask { task, wait } => {
                ClientMessage::SubmitTask { task, wait }
            }
            HumanClientMessage::GetTaskStatus { task_id } => {
                ClientMessage::GetTaskStatus { task_id }
            }
            HumanClientMessage::CancelTask { task_id } => ClientMessage::CancelTask { task_id },
            HumanClientMessage::ClusterStatus => ClientMessage::ClusterStatus,
            HumanClientMessage::ListWorkers => ClientMessage::ListWorkers,
            HumanClientMessage::SubmitMapReduce { job } => ClientMessage::SubmitMapReduce { job },
            HumanClientMessage::GetTaskResult { task_id } => {
                ClientMessage::GetTaskResult { task_id }
            }
        }
    }
}

impl From<ClientMessage> for BinaryClientMessage {
    fn from(msg: ClientMessage) -> Self {
        match msg {
            ClientMessage::SubmitTask { task, wait } => {
                BinaryClientMessage::SubmitTask { task, wait }
            }
            ClientMessage::GetTaskStatus { task_id } => {
                BinaryClientMessage::GetTaskStatus { task_id }
            }
            ClientMessage::CancelTask { task_id } => BinaryClientMessage::CancelTask { task_id },
            ClientMessage::ClusterStatus => BinaryClientMessage::ClusterStatus,
            ClientMessage::ListWorkers => BinaryClientMessage::ListWorkers,
            ClientMessage::SubmitMapReduce { job } => BinaryClientMessage::SubmitMapReduce { job },
            ClientMessage::GetTaskResult { task_id } => {
                BinaryClientMessage::GetTaskResult { task_id }
            }
        }
    }
}

impl From<BinaryClientMessage> for ClientMessage {
    fn from(msg: BinaryClientMessage) -> Self {
        match msg {
            BinaryClientMessage::SubmitTask { task, wait } => {
                ClientMessage::SubmitTask { task, wait }
            }
            BinaryClientMessage::GetTaskStatus { task_id } => {
                ClientMessage::GetTaskStatus { task_id }
            }
            BinaryClientMessage::CancelTask { task_id } => ClientMessage::CancelTask { task_id },
            BinaryClientMessage::ClusterStatus => ClientMessage::ClusterStatus,
            BinaryClientMessage::ListWorkers => ClientMessage::ListWorkers,
            BinaryClientMessage::SubmitMapReduce { job } => ClientMessage::SubmitMapReduce { job },
            BinaryClientMessage::GetTaskResult { task_id } => {
                ClientMessage::GetTaskResult { task_id }
            }
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
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum HumanClientResponse {
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
}

#[derive(Serialize, Deserialize)]
enum BinaryClientResponse {
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
}

impl From<ClientResponse> for HumanClientResponse {
    fn from(msg: ClientResponse) -> Self {
        match msg {
            ClientResponse::TaskSubmitted { task_id } => {
                HumanClientResponse::TaskSubmitted { task_id }
            }
            ClientResponse::TaskCompleted { task_id, result } => {
                HumanClientResponse::TaskCompleted { task_id, result }
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
            },
            ClientResponse::TaskCancelled { task_id, success } => {
                HumanClientResponse::TaskCancelled { task_id, success }
            }
            ClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                workers,
            } => HumanClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                workers,
            },
            ClientResponse::WorkerList { workers } => HumanClientResponse::WorkerList { workers },
            ClientResponse::MapReduceSubmitted { job_id } => {
                HumanClientResponse::MapReduceSubmitted { job_id }
            }
            ClientResponse::MapReduceCompleted { result } => {
                HumanClientResponse::MapReduceCompleted { result }
            }
            ClientResponse::Error { message } => HumanClientResponse::Error { message },
        }
    }
}

impl From<HumanClientResponse> for ClientResponse {
    fn from(msg: HumanClientResponse) -> Self {
        match msg {
            HumanClientResponse::TaskSubmitted { task_id } => {
                ClientResponse::TaskSubmitted { task_id }
            }
            HumanClientResponse::TaskCompleted { task_id, result } => {
                ClientResponse::TaskCompleted { task_id, result }
            }
            HumanClientResponse::TaskStatusInfo {
                task_id,
                status,
                state_name,
                assigned_worker,
                error,
            } => ClientResponse::TaskStatusInfo {
                task_id,
                status,
                state_name,
                assigned_worker,
                error,
            },
            HumanClientResponse::TaskCancelled { task_id, success } => {
                ClientResponse::TaskCancelled { task_id, success }
            }
            HumanClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                workers,
            } => ClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                workers,
            },
            HumanClientResponse::WorkerList { workers } => ClientResponse::WorkerList { workers },
            HumanClientResponse::MapReduceSubmitted { job_id } => {
                ClientResponse::MapReduceSubmitted { job_id }
            }
            HumanClientResponse::MapReduceCompleted { result } => {
                ClientResponse::MapReduceCompleted { result }
            }
            HumanClientResponse::Error { message } => ClientResponse::Error { message },
        }
    }
}

impl From<ClientResponse> for BinaryClientResponse {
    fn from(msg: ClientResponse) -> Self {
        match msg {
            ClientResponse::TaskSubmitted { task_id } => {
                BinaryClientResponse::TaskSubmitted { task_id }
            }
            ClientResponse::TaskCompleted { task_id, result } => {
                BinaryClientResponse::TaskCompleted { task_id, result }
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
            },
            ClientResponse::TaskCancelled { task_id, success } => {
                BinaryClientResponse::TaskCancelled { task_id, success }
            }
            ClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                workers,
            } => BinaryClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                workers,
            },
            ClientResponse::WorkerList { workers } => BinaryClientResponse::WorkerList { workers },
            ClientResponse::MapReduceSubmitted { job_id } => {
                BinaryClientResponse::MapReduceSubmitted { job_id }
            }
            ClientResponse::MapReduceCompleted { result } => {
                BinaryClientResponse::MapReduceCompleted { result }
            }
            ClientResponse::Error { message } => BinaryClientResponse::Error { message },
        }
    }
}

impl From<BinaryClientResponse> for ClientResponse {
    fn from(msg: BinaryClientResponse) -> Self {
        match msg {
            BinaryClientResponse::TaskSubmitted { task_id } => {
                ClientResponse::TaskSubmitted { task_id }
            }
            BinaryClientResponse::TaskCompleted { task_id, result } => {
                ClientResponse::TaskCompleted { task_id, result }
            }
            BinaryClientResponse::TaskStatusInfo {
                task_id,
                status,
                state_name,
                assigned_worker,
                error,
            } => ClientResponse::TaskStatusInfo {
                task_id,
                status,
                state_name,
                assigned_worker,
                error,
            },
            BinaryClientResponse::TaskCancelled { task_id, success } => {
                ClientResponse::TaskCancelled { task_id, success }
            }
            BinaryClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                workers,
            } => ClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                workers,
            },
            BinaryClientResponse::WorkerList { workers } => ClientResponse::WorkerList { workers },
            BinaryClientResponse::MapReduceSubmitted { job_id } => {
                ClientResponse::MapReduceSubmitted { job_id }
            }
            BinaryClientResponse::MapReduceCompleted { result } => {
                ClientResponse::MapReduceCompleted { result }
            }
            BinaryClientResponse::Error { message } => ClientResponse::Error { message },
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

/// Unified message discriminator for multiplexed connections on the Master.
#[derive(Debug, Clone, PartialEq)]
pub enum InboundMessage {
    Worker(WorkerMessage),
    Client(ClientMessage),
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum HumanInboundMessage {
    Client(ClientMessage),
    Worker(WorkerMessage),
}

#[derive(Serialize, Deserialize)]
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
            let mut buf = Vec::with_capacity(128);
            buf.push(WIRE_FORMAT_BINCODE);
            bincode::serialize_into(&mut buf, msg)?;
            if buf.len() > MAX_FRAME_SIZE {
                return Err(ProtocolError::FrameTooLarge {
                    size: buf.len(),
                    max: MAX_FRAME_SIZE,
                });
            }
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
            match bincode::deserialize::<M>(&bytes[1..]) {
                Ok(msg) => Ok((msg, WireCodec::Bincode)),
                Err(e) => {
                    let slice = &bytes[1..];
                    // Fallback for WorkerMessage (legacy or unknown variant)
                    if std::any::TypeId::of::<M>() == std::any::TypeId::of::<WorkerMessage>() {
                        if let Ok(legacy) = bincode::deserialize::<LegacyBinaryWorkerMessage>(slice)
                        {
                            let msg: WorkerMessage = legacy.into();
                            let boxed: Box<dyn std::any::Any> = Box::new(msg);
                            if let Ok(m) = boxed.downcast::<M>() {
                                return Ok((*m, WireCodec::Bincode));
                            }
                        }
                        if slice.len() >= 4 {
                            let tag = u32::from_le_bytes(slice[0..4].try_into().unwrap());
                            if (8..10_000).contains(&tag) {
                                let unknown = WorkerMessage::UnknownVariant {
                                    tag,
                                    raw_bytes: slice[4..].to_vec(),
                                };
                                let boxed: Box<dyn std::any::Any> = Box::new(unknown);
                                if let Ok(m) = boxed.downcast::<M>() {
                                    return Ok((*m, WireCodec::Bincode));
                                }
                            }
                        }
                    }

                    // Fallback for MasterMessage (legacy or unknown variant)
                    if std::any::TypeId::of::<M>() == std::any::TypeId::of::<MasterMessage>() {
                        if let Ok(legacy) = bincode::deserialize::<LegacyBinaryMasterMessage>(slice)
                        {
                            let msg: MasterMessage = legacy.into();
                            let boxed: Box<dyn std::any::Any> = Box::new(msg);
                            if let Ok(m) = boxed.downcast::<M>() {
                                return Ok((*m, WireCodec::Bincode));
                            }
                        }
                        if slice.len() >= 4 {
                            let tag = u32::from_le_bytes(slice[0..4].try_into().unwrap());
                            if (7..10_000).contains(&tag) {
                                let unknown = MasterMessage::UnknownVariant {
                                    tag,
                                    raw_bytes: slice[4..].to_vec(),
                                };
                                let boxed: Box<dyn std::any::Any> = Box::new(unknown);
                                if let Ok(m) = boxed.downcast::<M>() {
                                    return Ok((*m, WireCodec::Bincode));
                                }
                            }
                        }
                    }

                    // Fallback for InboundMessage when peer sent raw WorkerMessage or ClientMessage
                    if std::any::TypeId::of::<M>() == std::any::TypeId::of::<InboundMessage>() {
                        use bincode::Options;
                        let slice_opts = bincode::DefaultOptions::new()
                            .with_fixint_encoding()
                            .allow_trailing_bytes()
                            .with_limit(slice.len() as u64);

                        // 1. Try modern WorkerMessage: verify exact byte consumption AND worker registration invariant
                        let mut cursor_w = std::io::Cursor::new(slice);
                        if let Ok(w) =
                            slice_opts.deserialize_from::<_, WorkerMessage>(&mut cursor_w)
                        {
                            let matches_size = cursor_w.position() as usize == slice.len();
                            let is_initial_worker_msg = matches!(w, WorkerMessage::Register { .. });
                            if matches_size && is_initial_worker_msg {
                                let inbound = InboundMessage::Worker(w);
                                let boxed: Box<dyn std::any::Any> = Box::new(inbound);
                                if let Ok(m) = boxed.downcast::<M>() {
                                    return Ok((*m, WireCodec::Bincode));
                                }
                            }
                        }
                        // 2. Try ClientMessage: verify exact byte consumption
                        let mut cursor_c = std::io::Cursor::new(slice);
                        if let Ok(c) =
                            slice_opts.deserialize_from::<_, ClientMessage>(&mut cursor_c)
                        {
                            if cursor_c.position() as usize == slice.len() {
                                let inbound = InboundMessage::Client(c);
                                let boxed: Box<dyn std::any::Any> = Box::new(inbound);
                                if let Ok(m) = boxed.downcast::<M>() {
                                    return Ok((*m, WireCodec::Bincode));
                                }
                            }
                        }
                        // 3. Fallback: if WorkerMessage consumed all bytes without Register requirement
                        let mut cursor_w2 = std::io::Cursor::new(slice);
                        if let Ok(w) =
                            slice_opts.deserialize_from::<_, WorkerMessage>(&mut cursor_w2)
                        {
                            if cursor_w2.position() as usize == slice.len() {
                                let inbound = InboundMessage::Worker(w);
                                let boxed: Box<dyn std::any::Any> = Box::new(inbound);
                                if let Ok(m) = boxed.downcast::<M>() {
                                    return Ok((*m, WireCodec::Bincode));
                                }
                            }
                        }

                        // 4. Try legacy WorkerMessage (must consume all bytes of legacy frame)
                        let mut cursor_leg = std::io::Cursor::new(slice);
                        if let Ok(legacy) = slice_opts
                            .deserialize_from::<_, LegacyBinaryWorkerMessage>(&mut cursor_leg)
                        {
                            if cursor_leg.position() as usize == slice.len() {
                                let inbound = InboundMessage::Worker(legacy.into());
                                let boxed: Box<dyn std::any::Any> = Box::new(inbound);
                                if let Ok(m) = boxed.downcast::<M>() {
                                    return Ok((*m, WireCodec::Bincode));
                                }
                            }
                        }
                        // 4. Fallback: if ClientMessage was sent without exact size check
                        if let Ok(c) = slice_opts.deserialize::<ClientMessage>(slice) {
                            let inbound = InboundMessage::Client(c);
                            let boxed: Box<dyn std::any::Any> = Box::new(inbound);
                            if let Ok(m) = boxed.downcast::<M>() {
                                return Ok((*m, WireCodec::Bincode));
                            }
                        }
                        // 5. Fallback: Unknown variant WorkerMessage
                        if slice.len() >= 4 {
                            let tag = u32::from_le_bytes(slice[0..4].try_into().unwrap());
                            if (8..10_000).contains(&tag) {
                                let unknown = WorkerMessage::UnknownVariant {
                                    tag,
                                    raw_bytes: slice[4..].to_vec(),
                                };
                                let inbound = InboundMessage::Worker(unknown);
                                let boxed: Box<dyn std::any::Any> = Box::new(inbound);
                                if let Ok(m) = boxed.downcast::<M>() {
                                    return Ok((*m, WireCodec::Bincode));
                                }
                            }
                        }
                    }
                    Err(ProtocolError::Bincode(e))
                }
            }
        }
        WIRE_FORMAT_JSON => {
            let msg: M = serde_json::from_slice(&bytes[1..])?;
            Ok((msg, WireCodec::Json))
        }
        b'{' | b' ' | b'\t' | b'\r' | b'\n' => {
            let msg: M = serde_json::from_slice(bytes)?;
            Ok((msg, WireCodec::Json))
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
    use crate::task::{
        Generative3DSpec, Task, TaskId, TaskRequirements, TaskResult, TaskSpec, TaskStatus,
    };
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
            },
            // Unknown readings are legitimate (no battery sensor reachable): `None` must survive.
            WorkerMessage::MobileTelemetry {
                worker_id,
                battery_pct: None,
                is_charging: None,
                thermal_throttled: false,
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
    fn test_aead_sessions_roundtrip_tamper_and_replay() {
        let session_key = [42u8; 32];
        let (mut client_writer, mut client_reader) =
            derive_aead_keys(&session_key, EndpointRole::Client);
        let (mut server_writer, mut server_reader) =
            derive_aead_keys(&session_key, EndpointRole::Server);

        let plaintext = b"hello secure swarm world";
        let encrypted = client_writer.encrypt_frame(plaintext).unwrap();
        assert_eq!(encrypted[0], WIRE_FORMAT_ENCRYPTED);
        assert_eq!(u64::from_be_bytes(encrypted[1..9].try_into().unwrap()), 0);

        // Server decrypts successfully
        let decrypted = server_reader.decrypt_frame(&encrypted).unwrap();
        assert_eq!(&decrypted[..], plaintext);

        // Replay of the same frame fails anti-replay
        let replay_err = server_reader.decrypt_frame(&encrypted).unwrap_err();
        match replay_err {
            ProtocolError::Violation(v) => assert!(v.contains("Anti-replay")),
            other => panic!("expected Violation, got {other:?}"),
        }

        // Tampered ciphertext fails Poly1305 verification
        let encrypted2 = client_writer.encrypt_frame(b"second message").unwrap();
        let mut tampered = encrypted2.to_vec();
        let last = tampered.len() - 1;
        tampered[last] ^= 0x01; // flip 1 bit in Poly1305 tag
        let tamper_err = server_reader.decrypt_frame(&tampered).unwrap_err();
        match tamper_err {
            ProtocolError::Violation(v) => assert!(v.contains("Ciphertext integrity")),
            other => panic!("expected Violation, got {other:?}"),
        }

        // Server response roundtrips to client
        let server_reply = server_writer.encrypt_frame(b"ack payload").unwrap();
        let client_decrypted = client_reader.decrypt_frame(&server_reply).unwrap();
        assert_eq!(&client_decrypted[..], b"ack payload");
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
}
