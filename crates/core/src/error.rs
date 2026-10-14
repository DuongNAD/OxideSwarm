//! Comprehensive error types and result aliases for `rusty_grid`.

use std::io;
use thiserror::Error;
use uuid::Uuid;

use crate::protocol::ProtocolError;

/// Result type alias using [`GridError`] as default error type.
pub type GridResult<T> = Result<T, GridError>;

/// Comprehensive error enumeration for the `rusty_grid` distributed computing framework.
#[derive(Error, Debug)]
pub enum GridError {
    /// Standard I/O failures (network sockets, filesystem reads/writes, pipes).
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Serde JSON serialization or deserialization failures.
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Protocol framing error (malformed length header, invalid delimiter).
    #[error("Protocol framing error: {0}")]
    Framing(String),

    /// Received frame exceeded configured maximum size limit (e.g. > 64 MB).
    #[error("Frame size {size} bytes exceeds maximum allowed limit of {max} bytes")]
    FrameTooLarge {
        /// Actual frame size in bytes.
        size: usize,
        /// Maximum allowed frame size in bytes.
        max: usize,
    },

    /// Unexpected or out-of-order message received for the current protocol state.
    #[error("Unexpected message: expected {expected}, received {actual}")]
    UnexpectedMessage {
        /// Expected message variant name or description.
        expected: String,
        /// Actual received message variant name or description.
        actual: String,
    },

    /// Worker registration or capability handshake failed.
    #[error("Registration handshake failed: {0}")]
    HandshakeFailed(String),

    /// Network connection abruptly closed by remote peer or EOF encountered.
    #[error("Connection closed unexpectedly")]
    ConnectionClosed,

    /// Connection failure or could not establish transport.
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    /// General operation timeout.
    #[error("Operation '{operation}' timed out after {duration_secs}s")]
    Timeout {
        /// Description of the operation that timed out.
        operation: String,
        /// Timeout duration in seconds.
        duration_secs: u64,
    },

    /// Worker missed heartbeat interval and was marked dead by the reaper.
    #[error("Worker {worker_id} heartbeat timed out (last seen {last_seen_secs}s ago)")]
    HeartbeatTimeout {
        /// Worker UUID.
        worker_id: Uuid,
        /// Seconds since last heartbeat.
        last_seen_secs: u64,
    },

    /// Task execution exceeded configured watchdog timeout.
    #[error("Task {task_id} timed out after {timeout_secs}s")]
    TaskTimeout {
        /// Task UUID.
        task_id: Uuid,
        /// Timeout limit in seconds.
        timeout_secs: u64,
    },

    /// Process execution failure or non-zero exit.
    #[error("Task {task_id} execution failed (exit code: {exit_code:?}): {reason}")]
    ExecutionFailed {
        /// Task UUID.
        task_id: Uuid,
        /// Process exit code if available.
        exit_code: Option<i32>,
        /// Reason or error message.
        reason: String,
    },

    /// Sandbox directory creation, isolation, or cleanup error.
    #[error("Sandbox error for task {task_id}: {reason}")]
    SandboxError {
        /// Task UUID.
        task_id: Uuid,
        /// Error details.
        reason: String,
    },

    /// Process spawning failure.
    #[error("Failed to spawn process '{command}': {source}")]
    ProcessSpawn {
        /// Executable command attempted.
        command: String,
        /// Underlying IO error.
        #[source]
        source: io::Error,
    },

    /// GPU workload execution error.
    #[error("GPU execution error for task {task_id}: {reason}")]
    GpuExecution {
        /// Task UUID.
        task_id: Uuid,
        /// Error details.
        reason: String,
    },

    /// Task identifier not found in task queue or history.
    #[error("Task not found: {0}")]
    TaskNotFound(Uuid),

    /// Worker identifier not found in worker registry.
    #[error("Worker not found: {0}")]
    WorkerNotFound(Uuid),

    /// Scheduler could not find an eligible worker matching task requirements.
    #[error("No eligible worker available for task {task_id}: {requirements_summary}")]
    NoEligibleWorker {
        /// Task UUID.
        task_id: Uuid,
        /// Human-readable requirements summary.
        requirements_summary: String,
    },

    /// Illegal task state transition in task finite state machine.
    #[error("Invalid task state transition for task {task_id}: cannot transition from {current} to {attempted}")]
    InvalidTaskState {
        /// Task UUID.
        task_id: Uuid,
        /// Current state.
        current: String,
        /// Attempted invalid target state.
        attempted: String,
    },

    /// Task queue capacity reached.
    #[error("Task queue is full (capacity: {capacity})")]
    QueueFull {
        /// Maximum capacity limit.
        capacity: usize,
    },

    /// Worker registration rejected by master.
    #[error("Worker registration rejected: {0}")]
    RegistrationRejected(String),

    /// The peer demands cluster authentication but this side did not (or could not) provide it.
    #[error("Authentication required: {0}")]
    AuthRequired(String),

    /// Cluster authentication was attempted and failed (wrong secret, rogue peer, bad handshake).
    #[error("Authentication failed: {0}")]
    AuthFailed(String),

    /// Configuration error (CLI flags, environment, invalid socket address).
    #[error("Configuration error: {0}")]
    Config(String),

    /// Ephemeral port file operation error.
    #[error("Port file error at '{path}': {reason}")]
    PortFile {
        /// Path to port file.
        path: String,
        /// Error details.
        reason: String,
    },
}

impl GridError {
    /// Returns a stable, machine-readable string error code.
    pub fn error_code(&self) -> &'static str {
        match self {
            GridError::Io(_) => "IO_ERROR",
            GridError::Serialization(_) => "SERIALIZATION_ERROR",
            GridError::Framing(_) => "FRAMING_ERROR",
            GridError::FrameTooLarge { .. } => "FRAME_TOO_LARGE",
            GridError::UnexpectedMessage { .. } => "UNEXPECTED_MESSAGE",
            GridError::HandshakeFailed(_) => "HANDSHAKE_FAILED",
            GridError::ConnectionClosed => "CONNECTION_CLOSED",
            GridError::ConnectionFailed(_) => "CONNECTION_FAILED",
            GridError::Timeout { .. } => "TIMEOUT",
            GridError::HeartbeatTimeout { .. } => "HEARTBEAT_TIMEOUT",
            GridError::TaskTimeout { .. } => "TASK_TIMEOUT",
            GridError::ExecutionFailed { .. } => "EXECUTION_FAILED",
            GridError::SandboxError { .. } => "SANDBOX_ERROR",
            GridError::ProcessSpawn { .. } => "PROCESS_SPAWN_ERROR",
            GridError::GpuExecution { .. } => "GPU_EXECUTION_ERROR",
            GridError::TaskNotFound(_) => "TASK_NOT_FOUND",
            GridError::WorkerNotFound(_) => "WORKER_NOT_FOUND",
            GridError::NoEligibleWorker { .. } => "NO_ELIGIBLE_WORKER",
            GridError::InvalidTaskState { .. } => "INVALID_TASK_STATE",
            GridError::QueueFull { .. } => "QUEUE_FULL",
            GridError::RegistrationRejected(_) => "REGISTRATION_REJECTED",
            GridError::AuthRequired(_) => "AUTH_REQUIRED",
            GridError::AuthFailed(_) => "AUTH_FAILED",
            GridError::Config(_) => "CONFIG_ERROR",
            GridError::PortFile { .. } => "PORT_FILE_ERROR",
        }
    }

    /// Determines whether the error is transient/recoverable by retrying.
    pub fn is_transient(&self) -> bool {
        match self {
            GridError::ConnectionClosed => true,
            GridError::ConnectionFailed(_) => true,
            GridError::Timeout { .. } => true,
            GridError::HeartbeatTimeout { .. } => false,
            GridError::QueueFull { .. } => true,
            GridError::Io(err) => matches!(
                err.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::TimedOut
                    | io::ErrorKind::WouldBlock
                    | io::ErrorKind::Interrupted
            ),
            _ => false,
        }
    }

    /// Maps the error to an appropriate process exit code for CLI reporting.
    pub fn exit_code(&self) -> i32 {
        match self {
            GridError::Config(_) => 2,
            GridError::ConnectionClosed | GridError::ConnectionFailed(_) | GridError::Io(_) => 3,
            GridError::Timeout { .. }
            | GridError::HeartbeatTimeout { .. }
            | GridError::TaskTimeout { .. } => 4,
            GridError::NoEligibleWorker { .. } => 5,
            GridError::AuthRequired(_) | GridError::AuthFailed(_) => 6,
            GridError::ExecutionFailed {
                exit_code: Some(code),
                ..
            } => *code,
            _ => 1,
        }
    }
}

impl From<tokio::time::error::Elapsed> for GridError {
    fn from(_err: tokio::time::error::Elapsed) -> Self {
        GridError::Timeout {
            operation: "tokio_timer".to_string(),
            duration_secs: 0,
        }
    }
}

impl From<uuid::Error> for GridError {
    fn from(err: uuid::Error) -> Self {
        GridError::Config(format!("Invalid UUID: {err}"))
    }
}

impl From<ProtocolError> for GridError {
    fn from(err: ProtocolError) -> Self {
        match err {
            ProtocolError::Io(e) => GridError::Io(e),
            ProtocolError::Json(e) => GridError::Serialization(e),
            ProtocolError::Bincode(e) => GridError::Framing(format!("Bincode error: {e}")),
            // A client that has no secret and expects a typed reply but receives an
            // authentication frame has been rejected by a master that requires the secret.
            ProtocolError::InvalidFormatTag(crate::protocol::WIRE_FORMAT_AUTH) => {
                GridError::AuthRequired(crate::cluster_auth::AUTH_REQUIRED_HINT.to_string())
            }
            ProtocolError::InvalidFormatTag(tag) => GridError::Framing(format!(
                "Unsupported wire format discriminator: 0x{tag:02x}"
            )),
            ProtocolError::FrameTooLarge { size, max } => GridError::FrameTooLarge { size, max },
            ProtocolError::UnexpectedEof => GridError::ConnectionClosed,
            ProtocolError::ConnectionClosed => GridError::ConnectionClosed,
            ProtocolError::Violation(msg) => GridError::Framing(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display_and_codes() {
        let err = GridError::FrameTooLarge {
            size: 70_000_000,
            max: 64_000_000,
        };
        assert_eq!(err.error_code(), "FRAME_TOO_LARGE");
        assert!(err.to_string().contains("70000000 bytes"));
        assert!(!err.is_transient());

        let io_err = GridError::Io(io::Error::new(io::ErrorKind::ConnectionReset, "reset"));
        assert_eq!(io_err.error_code(), "IO_ERROR");
        assert!(io_err.is_transient());

        let closed = GridError::ConnectionClosed;
        assert_eq!(closed.exit_code(), 3);
        assert!(closed.is_transient());
    }

    #[test]
    fn test_from_conversions() {
        let io_e = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let grid_e: GridError = io_e.into();
        assert_eq!(grid_e.error_code(), "IO_ERROR");

        let json_str = "{ invalid json }";
        let json_res: Result<serde_json::Value, _> = serde_json::from_str(json_str);
        let grid_json: GridError = json_res.unwrap_err().into();
        assert_eq!(grid_json.error_code(), "SERIALIZATION_ERROR");

        let uuid_e = Uuid::parse_str("not-a-uuid").unwrap_err();
        let grid_uuid: GridError = uuid_e.into();
        assert_eq!(grid_uuid.error_code(), "CONFIG_ERROR");

        let proto_eof = ProtocolError::UnexpectedEof;
        let grid_proto: GridError = proto_eof.into();
        assert_eq!(grid_proto.error_code(), "CONNECTION_CLOSED");
    }

    #[test]
    fn test_transient_classification() {
        let transient_timeout = GridError::Timeout {
            operation: "dispatch".to_string(),
            duration_secs: 5,
        };
        assert!(transient_timeout.is_transient());

        let permanent_task = GridError::TaskNotFound(Uuid::new_v4());
        assert!(!permanent_task.is_transient());

        let q_full = GridError::QueueFull { capacity: 100 };
        assert!(q_full.is_transient());
    }

    #[test]
    fn test_auth_errors_are_distinct_and_permanent() {
        let required = GridError::AuthRequired("need secret".into());
        let failed = GridError::AuthFailed("bad proof".into());
        assert_eq!(required.error_code(), "AUTH_REQUIRED");
        assert_eq!(failed.error_code(), "AUTH_FAILED");
        // Retrying with the same secret cannot succeed, so callers must not hot-loop on these.
        assert!(!required.is_transient());
        assert!(!failed.is_transient());
        assert_eq!(required.exit_code(), 6);
        assert_eq!(failed.exit_code(), 6);
        assert!(required.to_string().contains("need secret"));
        assert!(failed.to_string().contains("bad proof"));
    }

    #[test]
    fn test_auth_frame_where_typed_reply_expected_means_auth_required() {
        use crate::protocol::WIRE_FORMAT_AUTH;
        // A client without a secret that gets a REJECT frame back sees an undecodable 0x03 frame.
        let err: GridError = ProtocolError::InvalidFormatTag(WIRE_FORMAT_AUTH).into();
        match err {
            GridError::AuthRequired(msg) => {
                assert!(msg.contains("cluster secret"), "{msg}");
                assert!(msg.contains("OXIDESWARM_SECRET"), "{msg}");
            }
            other => panic!("expected AuthRequired, got {other:?}"),
        }
        // Every other bad tag is still a plain framing error.
        let err: GridError = ProtocolError::InvalidFormatTag(0x7e).into();
        assert_eq!(err.error_code(), "FRAMING_ERROR");
    }

    #[test]
    fn test_exit_codes() {
        assert_eq!(GridError::Config("bad flag".into()).exit_code(), 2);
        assert_eq!(GridError::ConnectionClosed.exit_code(), 3);
        assert_eq!(
            GridError::Timeout {
                operation: "wait".into(),
                duration_secs: 1
            }
            .exit_code(),
            4
        );
        assert_eq!(
            GridError::NoEligibleWorker {
                task_id: Uuid::new_v4(),
                requirements_summary: "GPU".into()
            }
            .exit_code(),
            5
        );
        assert_eq!(
            GridError::ExecutionFailed {
                task_id: Uuid::new_v4(),
                exit_code: Some(137),
                reason: "SIGKILL".into()
            }
            .exit_code(),
            137
        );
    }
}
