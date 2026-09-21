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

/// Wire protocol format discriminator identifiers.
pub const WIRE_FORMAT_JSON: u8 = 0x01;
pub const WIRE_FORMAT_BINCODE: u8 = 0x02;

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
        stdout: String,
        stderr: String,
        execution_time_ms: u64,
        is_gpu_executed: bool,
        error: Option<String>,
    },
    /// Graceful announcement that the worker is disconnecting.
    Disconnecting { worker_id: Uuid, reason: String },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum HumanWorkerMessage {
    Register {
        worker_id: Uuid,
        capabilities: WorkerCapabilities,
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
        stdout: String,
        stderr: String,
        execution_time_ms: u64,
        is_gpu_executed: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    Disconnecting { worker_id: Uuid, reason: String },
}

#[derive(Serialize, Deserialize)]
enum BinaryWorkerMessage {
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
        stdout: String,
        stderr: String,
        execution_time_ms: u64,
        is_gpu_executed: bool,
        error: Option<String>,
    },
    Disconnecting { worker_id: Uuid, reason: String },
}

impl From<WorkerMessage> for HumanWorkerMessage {
    fn from(msg: WorkerMessage) -> Self {
        match msg {
            WorkerMessage::Register { worker_id, capabilities } => {
                HumanWorkerMessage::Register { worker_id, capabilities }
            }
            WorkerMessage::Heartbeat { worker_id, timestamp, active_tasks, cpu_usage_pct, ram_available_mb } => {
                HumanWorkerMessage::Heartbeat { worker_id, timestamp, active_tasks, cpu_usage_pct, ram_available_mb }
            }
            WorkerMessage::TaskProgress { worker_id, task_id, status } => {
                HumanWorkerMessage::TaskProgress { worker_id, task_id, status }
            }
            WorkerMessage::TaskResult { worker_id, task_id, exit_code, stdout, stderr, execution_time_ms, is_gpu_executed, error } => {
                HumanWorkerMessage::TaskResult { worker_id, task_id, exit_code, stdout, stderr, execution_time_ms, is_gpu_executed, error }
            }
            WorkerMessage::Disconnecting { worker_id, reason } => {
                HumanWorkerMessage::Disconnecting { worker_id, reason }
            }
        }
    }
}

impl From<HumanWorkerMessage> for WorkerMessage {
    fn from(msg: HumanWorkerMessage) -> Self {
        match msg {
            HumanWorkerMessage::Register { worker_id, capabilities } => {
                WorkerMessage::Register { worker_id, capabilities }
            }
            HumanWorkerMessage::Heartbeat { worker_id, timestamp, active_tasks, cpu_usage_pct, ram_available_mb } => {
                WorkerMessage::Heartbeat { worker_id, timestamp, active_tasks, cpu_usage_pct, ram_available_mb }
            }
            HumanWorkerMessage::TaskProgress { worker_id, task_id, status } => {
                WorkerMessage::TaskProgress { worker_id, task_id, status }
            }
            HumanWorkerMessage::TaskResult { worker_id, task_id, exit_code, stdout, stderr, execution_time_ms, is_gpu_executed, error } => {
                WorkerMessage::TaskResult { worker_id, task_id, exit_code, stdout, stderr, execution_time_ms, is_gpu_executed, error }
            }
            HumanWorkerMessage::Disconnecting { worker_id, reason } => {
                WorkerMessage::Disconnecting { worker_id, reason }
            }
        }
    }
}

impl From<WorkerMessage> for BinaryWorkerMessage {
    fn from(msg: WorkerMessage) -> Self {
        match msg {
            WorkerMessage::Register { worker_id, capabilities } => {
                BinaryWorkerMessage::Register { worker_id, capabilities }
            }
            WorkerMessage::Heartbeat { worker_id, timestamp, active_tasks, cpu_usage_pct, ram_available_mb } => {
                BinaryWorkerMessage::Heartbeat { worker_id, timestamp, active_tasks, cpu_usage_pct, ram_available_mb }
            }
            WorkerMessage::TaskProgress { worker_id, task_id, status } => {
                BinaryWorkerMessage::TaskProgress { worker_id, task_id, status }
            }
            WorkerMessage::TaskResult { worker_id, task_id, exit_code, stdout, stderr, execution_time_ms, is_gpu_executed, error } => {
                BinaryWorkerMessage::TaskResult { worker_id, task_id, exit_code, stdout, stderr, execution_time_ms, is_gpu_executed, error }
            }
            WorkerMessage::Disconnecting { worker_id, reason } => {
                BinaryWorkerMessage::Disconnecting { worker_id, reason }
            }
        }
    }
}

impl From<BinaryWorkerMessage> for WorkerMessage {
    fn from(msg: BinaryWorkerMessage) -> Self {
        match msg {
            BinaryWorkerMessage::Register { worker_id, capabilities } => {
                WorkerMessage::Register { worker_id, capabilities }
            }
            BinaryWorkerMessage::Heartbeat { worker_id, timestamp, active_tasks, cpu_usage_pct, ram_available_mb } => {
                WorkerMessage::Heartbeat { worker_id, timestamp, active_tasks, cpu_usage_pct, ram_available_mb }
            }
            BinaryWorkerMessage::TaskProgress { worker_id, task_id, status } => {
                WorkerMessage::TaskProgress { worker_id, task_id, status }
            }
            BinaryWorkerMessage::TaskResult { worker_id, task_id, exit_code, stdout, stderr, execution_time_ms, is_gpu_executed, error } => {
                WorkerMessage::TaskResult { worker_id, task_id, exit_code, stdout, stderr, execution_time_ms, is_gpu_executed, error }
            }
            BinaryWorkerMessage::Disconnecting { worker_id, reason } => {
                WorkerMessage::Disconnecting { worker_id, reason }
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
#[derive(Debug, Clone, PartialEq)]
pub enum MasterMessage {
    /// Acknowledgment of worker registration with operational parameters.
    RegisterAck {
        accepted: bool,
        worker_id: Uuid,
        heartbeat_interval_secs: u64,
        message: Option<String>,
    },
    /// Acknowledgment of heartbeat receipt confirming master liveness.
    HeartbeatAck { timestamp: u64 },
    /// Assignment of a task payload for execution.
    AssignTask { task: Task },
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
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum HumanMasterMessage {
    RegisterAck {
        accepted: bool,
        worker_id: Uuid,
        heartbeat_interval_secs: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    HeartbeatAck { timestamp: u64 },
    AssignTask { task: Task },
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
}

#[derive(Serialize, Deserialize)]
enum BinaryMasterMessage {
    RegisterAck {
        accepted: bool,
        worker_id: Uuid,
        heartbeat_interval_secs: u64,
        message: Option<String>,
    },
    HeartbeatAck { timestamp: u64 },
    AssignTask { task: Task },
    CancelTask {
        task_id: TaskId,
        reason: Option<String>,
    },
    Shutdown {
        reason: String,
        grace_period_secs: Option<u64>,
    },
}

impl From<MasterMessage> for HumanMasterMessage {
    fn from(msg: MasterMessage) -> Self {
        match msg {
            MasterMessage::RegisterAck { accepted, worker_id, heartbeat_interval_secs, message } => {
                HumanMasterMessage::RegisterAck { accepted, worker_id, heartbeat_interval_secs, message }
            }
            MasterMessage::HeartbeatAck { timestamp } => {
                HumanMasterMessage::HeartbeatAck { timestamp }
            }
            MasterMessage::AssignTask { task } => {
                HumanMasterMessage::AssignTask { task }
            }
            MasterMessage::CancelTask { task_id, reason } => {
                HumanMasterMessage::CancelTask { task_id, reason }
            }
            MasterMessage::Shutdown { reason, grace_period_secs } => {
                HumanMasterMessage::Shutdown { reason, grace_period_secs }
            }
        }
    }
}

impl From<HumanMasterMessage> for MasterMessage {
    fn from(msg: HumanMasterMessage) -> Self {
        match msg {
            HumanMasterMessage::RegisterAck { accepted, worker_id, heartbeat_interval_secs, message } => {
                MasterMessage::RegisterAck { accepted, worker_id, heartbeat_interval_secs, message }
            }
            HumanMasterMessage::HeartbeatAck { timestamp } => {
                MasterMessage::HeartbeatAck { timestamp }
            }
            HumanMasterMessage::AssignTask { task } => {
                MasterMessage::AssignTask { task }
            }
            HumanMasterMessage::CancelTask { task_id, reason } => {
                MasterMessage::CancelTask { task_id, reason }
            }
            HumanMasterMessage::Shutdown { reason, grace_period_secs } => {
                MasterMessage::Shutdown { reason, grace_period_secs }
            }
        }
    }
}

impl From<MasterMessage> for BinaryMasterMessage {
    fn from(msg: MasterMessage) -> Self {
        match msg {
            MasterMessage::RegisterAck { accepted, worker_id, heartbeat_interval_secs, message } => {
                BinaryMasterMessage::RegisterAck { accepted, worker_id, heartbeat_interval_secs, message }
            }
            MasterMessage::HeartbeatAck { timestamp } => {
                BinaryMasterMessage::HeartbeatAck { timestamp }
            }
            MasterMessage::AssignTask { task } => {
                BinaryMasterMessage::AssignTask { task }
            }
            MasterMessage::CancelTask { task_id, reason } => {
                BinaryMasterMessage::CancelTask { task_id, reason }
            }
            MasterMessage::Shutdown { reason, grace_period_secs } => {
                BinaryMasterMessage::Shutdown { reason, grace_period_secs }
            }
        }
    }
}

impl From<BinaryMasterMessage> for MasterMessage {
    fn from(msg: BinaryMasterMessage) -> Self {
        match msg {
            BinaryMasterMessage::RegisterAck { accepted, worker_id, heartbeat_interval_secs, message } => {
                MasterMessage::RegisterAck { accepted, worker_id, heartbeat_interval_secs, message }
            }
            BinaryMasterMessage::HeartbeatAck { timestamp } => {
                MasterMessage::HeartbeatAck { timestamp }
            }
            BinaryMasterMessage::AssignTask { task } => {
                MasterMessage::AssignTask { task }
            }
            BinaryMasterMessage::CancelTask { task_id, reason } => {
                MasterMessage::CancelTask { task_id, reason }
            }
            BinaryMasterMessage::Shutdown { reason, grace_period_secs } => {
                MasterMessage::Shutdown { reason, grace_period_secs }
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
#[derive(Debug, Clone, PartialEq)]
pub enum ClientMessage {
    /// Submits a task for execution.
    SubmitTask {
        task: Task,
        wait: bool,
    },
    /// Queries the status and outcome of a specific task.
    GetTaskStatus {
        task_id: TaskId,
    },
    /// Requests cancellation of a task.
    CancelTask {
        task_id: TaskId,
    },
    /// Requests overall cluster metrics.
    ClusterStatus,
    /// Requests list of connected worker nodes.
    ListWorkers,
    /// Submits an in-memory Map/Reduce job for execution.
    SubmitMapReduce {
        job: crate::mapreduce::MapReduceJobSpec,
    },
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
}

impl From<ClientMessage> for HumanClientMessage {
    fn from(msg: ClientMessage) -> Self {
        match msg {
            ClientMessage::SubmitTask { task, wait } => HumanClientMessage::SubmitTask { task, wait },
            ClientMessage::GetTaskStatus { task_id } => HumanClientMessage::GetTaskStatus { task_id },
            ClientMessage::CancelTask { task_id } => HumanClientMessage::CancelTask { task_id },
            ClientMessage::ClusterStatus => HumanClientMessage::ClusterStatus,
            ClientMessage::ListWorkers => HumanClientMessage::ListWorkers,
            ClientMessage::SubmitMapReduce { job } => HumanClientMessage::SubmitMapReduce { job },
        }
    }
}

impl From<HumanClientMessage> for ClientMessage {
    fn from(msg: HumanClientMessage) -> Self {
        match msg {
            HumanClientMessage::SubmitTask { task, wait } => ClientMessage::SubmitTask { task, wait },
            HumanClientMessage::GetTaskStatus { task_id } => ClientMessage::GetTaskStatus { task_id },
            HumanClientMessage::CancelTask { task_id } => ClientMessage::CancelTask { task_id },
            HumanClientMessage::ClusterStatus => ClientMessage::ClusterStatus,
            HumanClientMessage::ListWorkers => ClientMessage::ListWorkers,
            HumanClientMessage::SubmitMapReduce { job } => ClientMessage::SubmitMapReduce { job },
        }
    }
}

impl From<ClientMessage> for BinaryClientMessage {
    fn from(msg: ClientMessage) -> Self {
        match msg {
            ClientMessage::SubmitTask { task, wait } => BinaryClientMessage::SubmitTask { task, wait },
            ClientMessage::GetTaskStatus { task_id } => BinaryClientMessage::GetTaskStatus { task_id },
            ClientMessage::CancelTask { task_id } => BinaryClientMessage::CancelTask { task_id },
            ClientMessage::ClusterStatus => BinaryClientMessage::ClusterStatus,
            ClientMessage::ListWorkers => BinaryClientMessage::ListWorkers,
            ClientMessage::SubmitMapReduce { job } => BinaryClientMessage::SubmitMapReduce { job },
        }
    }
}

impl From<BinaryClientMessage> for ClientMessage {
    fn from(msg: BinaryClientMessage) -> Self {
        match msg {
            BinaryClientMessage::SubmitTask { task, wait } => ClientMessage::SubmitTask { task, wait },
            BinaryClientMessage::GetTaskStatus { task_id } => ClientMessage::GetTaskStatus { task_id },
            BinaryClientMessage::CancelTask { task_id } => ClientMessage::CancelTask { task_id },
            BinaryClientMessage::ClusterStatus => ClientMessage::ClusterStatus,
            BinaryClientMessage::ListWorkers => ClientMessage::ListWorkers,
            BinaryClientMessage::SubmitMapReduce { job } => ClientMessage::SubmitMapReduce { job },
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
            ClientResponse::TaskSubmitted { task_id } => HumanClientResponse::TaskSubmitted { task_id },
            ClientResponse::TaskCompleted { task_id, result } => HumanClientResponse::TaskCompleted { task_id, result },
            ClientResponse::TaskStatusInfo { task_id, status, state_name, assigned_worker, error } => {
                HumanClientResponse::TaskStatusInfo { task_id, status, state_name, assigned_worker, error }
            }
            ClientResponse::TaskCancelled { task_id, success } => HumanClientResponse::TaskCancelled { task_id, success },
            ClientResponse::ClusterStatus { total_tasks, pending_tasks, running_tasks, completed_tasks, failed_tasks, workers } => {
                HumanClientResponse::ClusterStatus { total_tasks, pending_tasks, running_tasks, completed_tasks, failed_tasks, workers }
            }
            ClientResponse::WorkerList { workers } => HumanClientResponse::WorkerList { workers },
            ClientResponse::MapReduceSubmitted { job_id } => HumanClientResponse::MapReduceSubmitted { job_id },
            ClientResponse::MapReduceCompleted { result } => HumanClientResponse::MapReduceCompleted { result },
            ClientResponse::Error { message } => HumanClientResponse::Error { message },
        }
    }
}

impl From<HumanClientResponse> for ClientResponse {
    fn from(msg: HumanClientResponse) -> Self {
        match msg {
            HumanClientResponse::TaskSubmitted { task_id } => ClientResponse::TaskSubmitted { task_id },
            HumanClientResponse::TaskCompleted { task_id, result } => ClientResponse::TaskCompleted { task_id, result },
            HumanClientResponse::TaskStatusInfo { task_id, status, state_name, assigned_worker, error } => {
                ClientResponse::TaskStatusInfo { task_id, status, state_name, assigned_worker, error }
            }
            HumanClientResponse::TaskCancelled { task_id, success } => ClientResponse::TaskCancelled { task_id, success },
            HumanClientResponse::ClusterStatus { total_tasks, pending_tasks, running_tasks, completed_tasks, failed_tasks, workers } => {
                ClientResponse::ClusterStatus { total_tasks, pending_tasks, running_tasks, completed_tasks, failed_tasks, workers }
            }
            HumanClientResponse::WorkerList { workers } => ClientResponse::WorkerList { workers },
            HumanClientResponse::MapReduceSubmitted { job_id } => ClientResponse::MapReduceSubmitted { job_id },
            HumanClientResponse::MapReduceCompleted { result } => ClientResponse::MapReduceCompleted { result },
            HumanClientResponse::Error { message } => ClientResponse::Error { message },
        }
    }
}

impl From<ClientResponse> for BinaryClientResponse {
    fn from(msg: ClientResponse) -> Self {
        match msg {
            ClientResponse::TaskSubmitted { task_id } => BinaryClientResponse::TaskSubmitted { task_id },
            ClientResponse::TaskCompleted { task_id, result } => BinaryClientResponse::TaskCompleted { task_id, result },
            ClientResponse::TaskStatusInfo { task_id, status, state_name, assigned_worker, error } => {
                BinaryClientResponse::TaskStatusInfo { task_id, status, state_name, assigned_worker, error }
            }
            ClientResponse::TaskCancelled { task_id, success } => BinaryClientResponse::TaskCancelled { task_id, success },
            ClientResponse::ClusterStatus { total_tasks, pending_tasks, running_tasks, completed_tasks, failed_tasks, workers } => {
                BinaryClientResponse::ClusterStatus { total_tasks, pending_tasks, running_tasks, completed_tasks, failed_tasks, workers }
            }
            ClientResponse::WorkerList { workers } => BinaryClientResponse::WorkerList { workers },
            ClientResponse::MapReduceSubmitted { job_id } => BinaryClientResponse::MapReduceSubmitted { job_id },
            ClientResponse::MapReduceCompleted { result } => BinaryClientResponse::MapReduceCompleted { result },
            ClientResponse::Error { message } => BinaryClientResponse::Error { message },
        }
    }
}

impl From<BinaryClientResponse> for ClientResponse {
    fn from(msg: BinaryClientResponse) -> Self {
        match msg {
            BinaryClientResponse::TaskSubmitted { task_id } => ClientResponse::TaskSubmitted { task_id },
            BinaryClientResponse::TaskCompleted { task_id, result } => ClientResponse::TaskCompleted { task_id, result },
            BinaryClientResponse::TaskStatusInfo { task_id, status, state_name, assigned_worker, error } => {
                ClientResponse::TaskStatusInfo { task_id, status, state_name, assigned_worker, error }
            }
            BinaryClientResponse::TaskCancelled { task_id, success } => ClientResponse::TaskCancelled { task_id, success },
            BinaryClientResponse::ClusterStatus { total_tasks, pending_tasks, running_tasks, completed_tasks, failed_tasks, workers } => {
                ClientResponse::ClusterStatus { total_tasks, pending_tasks, running_tasks, completed_tasks, failed_tasks, workers }
            }
            BinaryClientResponse::WorkerList { workers } => ClientResponse::WorkerList { workers },
            BinaryClientResponse::MapReduceSubmitted { job_id } => ClientResponse::MapReduceSubmitted { job_id },
            BinaryClientResponse::MapReduceCompleted { result } => ClientResponse::MapReduceCompleted { result },
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
    Worker(WorkerMessage),
    Client(ClientMessage),
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
pub fn deserialize_message<M: DeserializeOwned + 'static>(bytes: &[u8]) -> Result<(M, WireCodec), ProtocolError> {
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
                    // Fallback for InboundMessage when peer sent raw WorkerMessage or ClientMessage
                    if std::any::TypeId::of::<M>() == std::any::TypeId::of::<InboundMessage>() {
                        if let Ok(w) = bincode::deserialize::<WorkerMessage>(&bytes[1..]) {
                            let inbound = InboundMessage::Worker(w);
                            let boxed: Box<dyn std::any::Any> = Box::new(inbound);
                            if let Ok(m) = boxed.downcast::<M>() {
                                return Ok((*m, WireCodec::Bincode));
                            }
                        }
                        if let Ok(c) = bincode::deserialize::<ClientMessage>(&bytes[1..]) {
                            let inbound = InboundMessage::Client(c);
                            let boxed: Box<dyn std::any::Any> = Box::new(inbound);
                            if let Ok(m) = boxed.downcast::<M>() {
                                return Ok((*m, WireCodec::Bincode));
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
        }
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
    /// preserving any unconsumed read buffer or unflushed write buffer and propagating
    /// the configured outbound wire codec to `MessageWriter`.
    pub fn split(self) -> (MessageWriter<WriteHalf<T>>, MessageReader<ReadHalf<T>>) {
        let parts = self.inner.into_parts();
        let (read_half, write_half) = split(parts.io);
        (
            MessageWriter::with_codec_and_buffer(write_half, self.codec, parts.write_buf),
            MessageReader::with_buffer(read_half, parts.read_buf),
        )
    }

    /// Serializes and sends a message framed with a 4-byte length prefix using the configured wire codec.
    pub async fn send_msg<M: Serialize>(&mut self, msg: &M) -> Result<(), ProtocolError> {
        let payload = serialize_message(msg, self.codec)?;
        self.inner
            .send(payload)
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
    pub async fn recv_msg<M: DeserializeOwned + 'static>(&mut self) -> Result<Option<M>, ProtocolError> {
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
                let (msg, codec) = deserialize_message::<M>(&bytes)?;
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
}

/// Unidirectional framed reader for receiving messages.
pub struct MessageReader<R> {
    inner: FramedRead<R, LengthDelimitedCodec>,
    last_detected_codec: Option<WireCodec>,
}

impl<R: AsyncRead + Unpin> MessageReader<R> {
    /// Creates a new `MessageReader` wrapping an async reader.
    pub fn new(reader: R) -> Self {
        Self {
            inner: FramedRead::new(reader, default_codec()),
            last_detected_codec: None,
        }
    }

    /// Creates a new `MessageReader` wrapping an async reader with pre-buffered data.
    pub fn with_buffer(reader: R, buffer: BytesMut) -> Self {
        let mut framed = FramedRead::new(reader, default_codec());
        if !buffer.is_empty() {
            framed.read_buffer_mut().extend_from_slice(&buffer);
        }
        Self {
            inner: framed,
            last_detected_codec: None,
        }
    }

    /// Reads the next framed message from the reader.
    pub async fn recv_msg<M: DeserializeOwned + 'static>(&mut self) -> Result<Option<M>, ProtocolError> {
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
                let (msg, codec) = deserialize_message::<M>(&bytes)?;
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
}

impl<W: AsyncWrite + Unpin> MessageWriter<W> {
    /// Creates a new `MessageWriter` wrapping an async writer with default wire codec (Bincode).
    pub fn new(writer: W) -> Self {
        Self::with_codec(writer, WireCodec::default())
    }

    /// Creates a new `MessageWriter` wrapping an async writer with specific wire codec.
    pub fn with_codec(writer: W, codec: WireCodec) -> Self {
        Self {
            inner: FramedWrite::new(writer, default_codec()),
            codec,
        }
    }

    /// Creates a new `MessageWriter` wrapping an async writer with pre-buffered data and default codec.
    pub fn with_buffer(writer: W, buffer: BytesMut) -> Self {
        Self::with_codec_and_buffer(writer, WireCodec::default(), buffer)
    }

    /// Creates a new `MessageWriter` wrapping an async writer with pre-buffered data and specified codec.
    pub fn with_codec_and_buffer(writer: W, codec: WireCodec, buffer: BytesMut) -> Self {
        let mut framed = FramedWrite::new(writer, default_codec());
        if !buffer.is_empty() {
            framed.write_buffer_mut().extend_from_slice(&buffer);
        }
        Self {
            inner: framed,
            codec,
        }
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
        self.inner
            .send(payload)
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
                        max_retries: None,
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
            WorkerMessage::Heartbeat { worker_id, timestamp, .. } => {
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
            capabilities: crate::capabilities::WorkerCapabilities::new("worker-1", 4, 8192, false, false, None),
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
}


