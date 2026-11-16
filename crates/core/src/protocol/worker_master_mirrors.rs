use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::capabilities::WorkerCapabilities;
use crate::task::{Bytes as TaskBytes, CheckpointData, Task, TaskId, TaskStatus};

use super::{
    default_protocol_version, ClientMessage, InboundMessage, MasterMessage, ProtocolCapabilities,
    WorkerMessage,
};

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
pub(super) enum LegacyBinaryWorkerMessage {
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
pub(super) enum LegacyBinaryMasterMessage {
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

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
enum HumanInboundMessage {
    Worker(WorkerMessage),
    Client(ClientMessage),
}

#[derive(Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub(super) enum BinaryInboundMessage {
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
