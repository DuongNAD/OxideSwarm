//! Wire protocol message types, length-delimited framing codec, and transport helpers.

use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::{split, AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tokio_util::codec::{Framed, FramedRead, FramedWrite, LengthDelimitedCodec};
use uuid::Uuid;

use crate::capabilities::WorkerCapabilities;
use crate::task::{Task, TaskId, TaskResult, TaskStatus};

/// Maximum allowable frame size in bytes (64 MB).
pub const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;
/// Length prefix field width in bytes (4-byte unsigned big-endian integer).
pub const LENGTH_FIELD_BYTES: usize = 4;

/// Protocol-level error variants encountered during framing and serialization.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    /// I/O error reading from or writing to the underlying stream.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization or deserialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum WorkerMessage {
    /// Initial registration advertising worker identity and capabilities.
    Register {
        worker_id: Uuid,
        capabilities: WorkerCapabilities,
    },
    /// Periodic heartbeat informing the master of worker liveness and load.
    Heartbeat {
        worker_id: Uuid,
        timestamp: u64,
        active_tasks: usize,
        #[serde(default)]
        cpu_usage_pct: f32,
        #[serde(default)]
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
        stdout: String,
        stderr: String,
        execution_time_ms: u64,
        is_gpu_executed: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    /// Graceful announcement that the worker is disconnecting.
    Disconnecting { worker_id: Uuid, reason: String },
}

impl WorkerMessage {
    /// Returns the static variant name of the message.
    pub fn variant_name(&self) -> &'static str {
        match self {
            Self::Register { .. } => "Register",
            Self::Heartbeat { .. } => "Heartbeat",
            Self::TaskProgress { .. } => "TaskProgress",
            Self::TaskResult { .. } => "TaskResult",
            Self::Disconnecting { .. } => "Disconnecting",
        }
    }

    /// Returns the worker UUID associated with this message.
    pub fn worker_id(&self) -> Uuid {
        match self {
            Self::Register { worker_id, .. } => *worker_id,
            Self::Heartbeat { worker_id, .. } => *worker_id,
            Self::TaskProgress { worker_id, .. } => *worker_id,
            Self::TaskResult { worker_id, .. } => *worker_id,
            Self::Disconnecting { worker_id, .. } => *worker_id,
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
            error: r.error,
        }
    }
}

/// Downstream messages sent from the Master node to Worker nodes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum MasterMessage {
    /// Acknowledgment of worker registration with operational parameters.
    RegisterAck {
        accepted: bool,
        worker_id: Uuid,
        heartbeat_interval_secs: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    /// Acknowledgment of heartbeat receipt confirming master liveness.
    HeartbeatAck { timestamp: u64 },
    /// Assignment of a task payload for execution.
    AssignTask { task: Task },
    /// Directive to immediately abort and cancel an assigned or running task.
    CancelTask {
        task_id: TaskId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    /// Directive notifying the worker of master shutdown or cluster evacuation.
    Shutdown {
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        grace_period_secs: Option<u64>,
    },
}

impl MasterMessage {
    /// Returns the static variant name of the message.
    pub fn variant_name(&self) -> &'static str {
        match self {
            Self::RegisterAck { .. } => "RegisterAck",
            Self::HeartbeatAck { .. } => "HeartbeatAck",
            Self::AssignTask { .. } => "AssignTask",
            Self::CancelTask { .. } => "CancelTask",
            Self::Shutdown { .. } => "Shutdown",
        }
    }
}

/// Messages sent from CLI client (or external RPC) to the Master node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum ClientMessage {
    /// Submits a task for execution.
    SubmitTask {
        task: Task,
        #[serde(default)]
        wait: bool,
    },
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
}

/// Responses sent from the Master node to the CLI client.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
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

/// Unified message discriminator for multiplexed connections on the Master.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum InboundMessage {
    Worker(WorkerMessage),
    Client(ClientMessage),
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

/// Unified bidirectional framed transport over any `AsyncRead + AsyncWrite + Unpin` stream.
pub struct MessageTransport<T> {
    inner: Framed<T, LengthDelimitedCodec>,
}

impl<T> MessageTransport<T> {
    /// Wraps an I/O stream with the standard 4-byte length-delimited framing codec.
    pub fn new(io: T) -> Self {
        Self {
            inner: Framed::new(io, default_codec()),
        }
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
    /// preserving any unconsumed read buffer or unflushed write buffer.
    pub fn split(self) -> (MessageWriter<WriteHalf<T>>, MessageReader<ReadHalf<T>>) {
        let parts = self.inner.into_parts();
        let (read_half, write_half) = split(parts.io);
        (
            MessageWriter::with_buffer(write_half, parts.write_buf),
            MessageReader::with_buffer(read_half, parts.read_buf),
        )
    }

    /// Serializes and sends a message framed with a 4-byte length prefix.
    pub async fn send_msg<M: Serialize>(&mut self, msg: &M) -> Result<(), ProtocolError> {
        let serialized = serde_json::to_vec(msg)?;
        if serialized.len() > MAX_FRAME_SIZE {
            return Err(ProtocolError::FrameTooLarge {
                size: serialized.len(),
                max: MAX_FRAME_SIZE,
            });
        }
        self.inner
            .send(Bytes::from(serialized))
            .await
            .map_err(|e| {
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
    pub async fn recv_msg<M: DeserializeOwned>(&mut self) -> Result<Option<M>, ProtocolError> {
        match self.inner.next().await {
            Some(Ok(bytes)) => {
                let msg = serde_json::from_slice::<M>(&bytes)?;
                Ok(Some(msg))
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
}

/// Unidirectional framed reader for receiving messages.
pub struct MessageReader<R> {
    inner: FramedRead<R, LengthDelimitedCodec>,
}

impl<R: AsyncRead + Unpin> MessageReader<R> {
    /// Creates a new `MessageReader` wrapping an async reader.
    pub fn new(reader: R) -> Self {
        Self {
            inner: FramedRead::new(reader, default_codec()),
        }
    }

    /// Creates a new `MessageReader` wrapping an async reader with pre-buffered data.
    pub fn with_buffer(reader: R, buffer: BytesMut) -> Self {
        let mut framed = FramedRead::new(reader, default_codec());
        if !buffer.is_empty() {
            framed.read_buffer_mut().extend_from_slice(&buffer);
        }
        Self { inner: framed }
    }

    /// Reads the next framed message from the reader.
    pub async fn recv_msg<M: DeserializeOwned>(&mut self) -> Result<Option<M>, ProtocolError> {
        match self.inner.next().await {
            Some(Ok(bytes)) => {
                let msg = serde_json::from_slice::<M>(&bytes)?;
                Ok(Some(msg))
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
}

impl<W: AsyncWrite + Unpin> MessageWriter<W> {
    /// Creates a new `MessageWriter` wrapping an async writer.
    pub fn new(writer: W) -> Self {
        Self {
            inner: FramedWrite::new(writer, default_codec()),
        }
    }

    /// Creates a new `MessageWriter` wrapping an async writer with pre-buffered data.
    pub fn with_buffer(writer: W, buffer: BytesMut) -> Self {
        let mut framed = FramedWrite::new(writer, default_codec());
        if !buffer.is_empty() {
            framed.write_buffer_mut().extend_from_slice(&buffer);
        }
        Self { inner: framed }
    }

    /// Serializes and sends a message framed with a 4-byte length prefix.
    pub async fn send_msg<M: Serialize>(&mut self, msg: &M) -> Result<(), ProtocolError> {
        let serialized = serde_json::to_vec(msg)?;
        if serialized.len() > MAX_FRAME_SIZE {
            return Err(ProtocolError::FrameTooLarge {
                size: serialized.len(),
                max: MAX_FRAME_SIZE,
            });
        }
        self.inner
            .send(Bytes::from(serialized))
            .await
            .map_err(|e| {
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
                },
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
                error: None,
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
            assert_eq!(msg.worker_id(), worker_id);
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
                    },
                    created_at_utc: 1726747200,
                    tags: vec!["test".into()],
                },
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
            },
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
}
