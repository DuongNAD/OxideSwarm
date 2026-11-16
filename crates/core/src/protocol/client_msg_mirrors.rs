use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::task::{Task, TaskId};

use super::{default_protocol_version, ClientMessage, PROTOCOL_VERSION_CURRENT};

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
#[allow(clippy::large_enum_variant)]
pub(super) enum HumanClientMessage {
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
    ListWorkspaces {
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    Enroll {
        invite_id: String,
        proof: crate::noise_auth::RedactedBytes32,
        name: String,
        os: String,
        arch: String,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    CreateInvite {
        roles: Vec<crate::noise_auth::MemberRole>,
        max_uses: u32,
        expires_in_secs: u64,
        require_approval: bool,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    ListInvites {
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    RevokeInvite {
        invite_id: String,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    ListMembers {
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    ListPendingEnrollments {
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    ApproveEnrollment {
        pending_id: String,
        pairing_code: crate::noise_auth::RedactedString,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    RejectEnrollment {
        pending_id: String,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    RevokeMember {
        target: String,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    RenameMember {
        target: String,
        new_name: String,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    ReleaseWorkerId {
        worker_id: Uuid,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
}

#[derive(Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub(super) enum BinaryClientMessage {
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
    ListWorkspaces {
        protocol_version: u32,
    },
    Enroll {
        invite_id: String,
        proof: crate::noise_auth::RedactedBytes32,
        name: String,
        os: String,
        arch: String,
        protocol_version: u32,
    },
    CreateInvite {
        roles: Vec<crate::noise_auth::MemberRole>,
        max_uses: u32,
        expires_in_secs: u64,
        require_approval: bool,
        protocol_version: u32,
    },
    ListInvites {
        protocol_version: u32,
    },
    RevokeInvite {
        invite_id: String,
        protocol_version: u32,
    },
    ListMembers {
        protocol_version: u32,
    },
    ListPendingEnrollments {
        protocol_version: u32,
    },
    ApproveEnrollment {
        pending_id: String,
        pairing_code: crate::noise_auth::RedactedString,
        protocol_version: u32,
    },
    RejectEnrollment {
        pending_id: String,
        protocol_version: u32,
    },
    RevokeMember {
        target: String,
        protocol_version: u32,
    },
    RenameMember {
        target: String,
        new_name: String,
        protocol_version: u32,
    },
    ReleaseWorkerId {
        worker_id: Uuid,
        protocol_version: u32,
    },
}

/// Pre-v3 (protocol version 2) positional bincode layout of [`ClientMessage`], which carried no
/// `protocol_version` trailer. Used only so a v3 Master can recognize an older v2 client frame and
/// refuse it with [`ProtocolError::IncompatibleVersion`] instead of an opaque bincode EOF error.
#[derive(Deserialize)]
#[allow(clippy::large_enum_variant, dead_code)]
pub(super) enum LegacyV2BinaryClientMessage {
    SubmitTask {
        task: LegacyV5Task,
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

/// Pre-v9 (protocol version 3 to 8) positional bincode layout of [`TaskRequirements`], which
/// ended at `max_retries` before `vram_mb` was appended in v9.
#[derive(Deserialize)]
#[allow(dead_code)]
pub(super) struct LegacyPreV9TaskRequirements {
    cpu_cores: usize,
    ram_mb: u64,
    gpu_required: bool,
    timeout_secs: u64,
    max_retries: Option<u32>,
}

/// Protocol v3 to v5 positional bincode layout of [`Task`], which ended at `output_globs` before
/// `workspace` was appended in v6. Used only to read a v5 `SubmitTask` far enough to recognise it.
#[derive(Deserialize)]
#[allow(dead_code)]
pub(super) struct LegacyV5Task {
    id: crate::task::TaskId,
    spec: crate::task::TaskSpec,
    requirements: LegacyPreV9TaskRequirements,
    created_at_utc: u64,
    tags: Vec<String>,
    latest_checkpoint: Option<crate::task::CheckpointData>,
    staged_files: Vec<crate::task::StagedFile>,
    expected_outputs: Vec<String>,
    inputs: Vec<crate::blob::TaskInput>,
    output_globs: Vec<String>,
}

/// Protocol v6 to v8 positional bincode layout of [`Task`], whose [`TaskRequirements`] ended at
/// `max_retries` before `vram_mb` was appended in v9. Used only so a v9 Master can refuse a
/// v6..=v8 `SubmitTask` frame with [`ProtocolError::IncompatibleVersion`].
#[derive(Deserialize)]
#[allow(dead_code)]
pub(super) struct LegacyV6ToV8Task {
    id: crate::task::TaskId,
    spec: crate::task::TaskSpec,
    requirements: LegacyPreV9TaskRequirements,
    created_at_utc: u64,
    tags: Vec<String>,
    latest_checkpoint: Option<crate::task::CheckpointData>,
    staged_files: Vec<crate::task::StagedFile>,
    expected_outputs: Vec<String>,
    inputs: Vec<crate::blob::TaskInput>,
    output_globs: Vec<String>,
    workspace: Option<crate::task::WorkspaceRef>,
}

/// Protocol v3 to v5 positional bincode layout of [`ClientMessage`]: the same variants as v6
/// except `Hello`, but `SubmitTask` carries a [`LegacyV5Task`]. Used only so a v6+ Master can
/// refuse a v5 client frame with [`ProtocolError::IncompatibleVersion`] instead of an opaque
/// bincode error. Every other variant keeps the current layout and is refused by the version check
/// of [`BinaryClientMessage`] itself.
#[derive(Deserialize)]
#[allow(clippy::large_enum_variant, dead_code)]
pub(super) enum LegacyV5BinaryClientMessage {
    SubmitTask {
        task: LegacyV5Task,
        wait: bool,
        protocol_version: u32,
    },
}

/// Protocol v6 to v8 positional bincode layout of [`ClientMessage::SubmitTask`] carrying
/// [`LegacyV6ToV8Task`]. Used only so a v9+ Master refuses a v6..=v8 `SubmitTask` with
/// [`ProtocolError::IncompatibleVersion`] instead of an opaque bincode error.
#[derive(Deserialize)]
#[allow(clippy::large_enum_variant, dead_code)]
pub(super) enum LegacyV6ToV8BinaryClientMessage {
    SubmitTask {
        task: LegacyV6ToV8Task,
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
            ClientMessage::ListWorkspaces => {
                HumanClientMessage::ListWorkspaces { protocol_version }
            }
            ClientMessage::Enroll {
                invite_id,
                proof,
                name,
                os,
                arch,
            } => HumanClientMessage::Enroll {
                invite_id,
                proof,
                name,
                os,
                arch,
                protocol_version,
            },
            ClientMessage::CreateInvite {
                roles,
                max_uses,
                expires_in_secs,
                require_approval,
            } => HumanClientMessage::CreateInvite {
                roles,
                max_uses,
                expires_in_secs,
                require_approval,
                protocol_version,
            },
            ClientMessage::ListInvites => HumanClientMessage::ListInvites { protocol_version },
            ClientMessage::RevokeInvite { invite_id } => HumanClientMessage::RevokeInvite {
                invite_id,
                protocol_version,
            },
            ClientMessage::ListMembers => HumanClientMessage::ListMembers { protocol_version },
            ClientMessage::ListPendingEnrollments => {
                HumanClientMessage::ListPendingEnrollments { protocol_version }
            }
            ClientMessage::ApproveEnrollment {
                pending_id,
                pairing_code,
            } => HumanClientMessage::ApproveEnrollment {
                pending_id,
                pairing_code,
                protocol_version,
            },
            ClientMessage::RejectEnrollment { pending_id } => {
                HumanClientMessage::RejectEnrollment {
                    pending_id,
                    protocol_version,
                }
            }
            ClientMessage::RevokeMember { target } => HumanClientMessage::RevokeMember {
                target,
                protocol_version,
            },
            ClientMessage::RenameMember { target, new_name } => HumanClientMessage::RenameMember {
                target,
                new_name,
                protocol_version,
            },
            ClientMessage::ReleaseWorkerId { worker_id } => HumanClientMessage::ReleaseWorkerId {
                worker_id,
                protocol_version,
            },
        }
    }

    pub(super) const fn protocol_version(&self) -> u32 {
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
            | Self::Hello { protocol_version }
            | Self::ListWorkspaces { protocol_version }
            | Self::Enroll {
                protocol_version, ..
            }
            | Self::CreateInvite {
                protocol_version, ..
            }
            | Self::ListInvites { protocol_version }
            | Self::RevokeInvite {
                protocol_version, ..
            }
            | Self::ListMembers { protocol_version }
            | Self::ListPendingEnrollments { protocol_version }
            | Self::ApproveEnrollment {
                protocol_version, ..
            }
            | Self::RejectEnrollment {
                protocol_version, ..
            }
            | Self::RevokeMember {
                protocol_version, ..
            }
            | Self::RenameMember {
                protocol_version, ..
            }
            | Self::ReleaseWorkerId {
                protocol_version, ..
            } => *protocol_version,
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
            ClientMessage::ListWorkspaces => {
                BinaryClientMessage::ListWorkspaces { protocol_version }
            }
            ClientMessage::Enroll {
                invite_id,
                proof,
                name,
                os,
                arch,
            } => BinaryClientMessage::Enroll {
                invite_id,
                proof,
                name,
                os,
                arch,
                protocol_version,
            },
            ClientMessage::CreateInvite {
                roles,
                max_uses,
                expires_in_secs,
                require_approval,
            } => BinaryClientMessage::CreateInvite {
                roles,
                max_uses,
                expires_in_secs,
                require_approval,
                protocol_version,
            },
            ClientMessage::ListInvites => BinaryClientMessage::ListInvites { protocol_version },
            ClientMessage::RevokeInvite { invite_id } => BinaryClientMessage::RevokeInvite {
                invite_id,
                protocol_version,
            },
            ClientMessage::ListMembers => BinaryClientMessage::ListMembers { protocol_version },
            ClientMessage::ListPendingEnrollments => {
                BinaryClientMessage::ListPendingEnrollments { protocol_version }
            }
            ClientMessage::ApproveEnrollment {
                pending_id,
                pairing_code,
            } => BinaryClientMessage::ApproveEnrollment {
                pending_id,
                pairing_code,
                protocol_version,
            },
            ClientMessage::RejectEnrollment { pending_id } => {
                BinaryClientMessage::RejectEnrollment {
                    pending_id,
                    protocol_version,
                }
            }
            ClientMessage::RevokeMember { target } => BinaryClientMessage::RevokeMember {
                target,
                protocol_version,
            },
            ClientMessage::RenameMember { target, new_name } => BinaryClientMessage::RenameMember {
                target,
                new_name,
                protocol_version,
            },
            ClientMessage::ReleaseWorkerId { worker_id } => BinaryClientMessage::ReleaseWorkerId {
                worker_id,
                protocol_version,
            },
        }
    }

    pub(super) const fn protocol_version(&self) -> u32 {
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
            | Self::Hello { protocol_version }
            | Self::ListWorkspaces { protocol_version }
            | Self::Enroll {
                protocol_version, ..
            }
            | Self::CreateInvite {
                protocol_version, ..
            }
            | Self::ListInvites { protocol_version }
            | Self::RevokeInvite {
                protocol_version, ..
            }
            | Self::ListMembers { protocol_version }
            | Self::ListPendingEnrollments { protocol_version }
            | Self::ApproveEnrollment {
                protocol_version, ..
            }
            | Self::RejectEnrollment {
                protocol_version, ..
            }
            | Self::RevokeMember {
                protocol_version, ..
            }
            | Self::RenameMember {
                protocol_version, ..
            }
            | Self::ReleaseWorkerId {
                protocol_version, ..
            } => *protocol_version,
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
            HumanClientMessage::ListWorkspaces { .. } => ClientMessage::ListWorkspaces,
            HumanClientMessage::Enroll {
                invite_id,
                proof,
                name,
                os,
                arch,
                ..
            } => ClientMessage::Enroll {
                invite_id,
                proof,
                name,
                os,
                arch,
            },
            HumanClientMessage::CreateInvite {
                roles,
                max_uses,
                expires_in_secs,
                require_approval,
                ..
            } => ClientMessage::CreateInvite {
                roles,
                max_uses,
                expires_in_secs,
                require_approval,
            },
            HumanClientMessage::ListInvites { .. } => ClientMessage::ListInvites,
            HumanClientMessage::RevokeInvite { invite_id, .. } => {
                ClientMessage::RevokeInvite { invite_id }
            }
            HumanClientMessage::ListMembers { .. } => ClientMessage::ListMembers,
            HumanClientMessage::ListPendingEnrollments { .. } => {
                ClientMessage::ListPendingEnrollments
            }
            HumanClientMessage::ApproveEnrollment {
                pending_id,
                pairing_code,
                ..
            } => ClientMessage::ApproveEnrollment {
                pending_id,
                pairing_code,
            },
            HumanClientMessage::RejectEnrollment { pending_id, .. } => {
                ClientMessage::RejectEnrollment { pending_id }
            }
            HumanClientMessage::RevokeMember { target, .. } => {
                ClientMessage::RevokeMember { target }
            }
            HumanClientMessage::RenameMember {
                target, new_name, ..
            } => ClientMessage::RenameMember { target, new_name },
            HumanClientMessage::ReleaseWorkerId { worker_id, .. } => {
                ClientMessage::ReleaseWorkerId { worker_id }
            }
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
            BinaryClientMessage::ListWorkspaces { .. } => ClientMessage::ListWorkspaces,
            BinaryClientMessage::Enroll {
                invite_id,
                proof,
                name,
                os,
                arch,
                ..
            } => ClientMessage::Enroll {
                invite_id,
                proof,
                name,
                os,
                arch,
            },
            BinaryClientMessage::CreateInvite {
                roles,
                max_uses,
                expires_in_secs,
                require_approval,
                ..
            } => ClientMessage::CreateInvite {
                roles,
                max_uses,
                expires_in_secs,
                require_approval,
            },
            BinaryClientMessage::ListInvites { .. } => ClientMessage::ListInvites,
            BinaryClientMessage::RevokeInvite { invite_id, .. } => {
                ClientMessage::RevokeInvite { invite_id }
            }
            BinaryClientMessage::ListMembers { .. } => ClientMessage::ListMembers,
            BinaryClientMessage::ListPendingEnrollments { .. } => {
                ClientMessage::ListPendingEnrollments
            }
            BinaryClientMessage::ApproveEnrollment {
                pending_id,
                pairing_code,
                ..
            } => ClientMessage::ApproveEnrollment {
                pending_id,
                pairing_code,
            },
            BinaryClientMessage::RejectEnrollment { pending_id, .. } => {
                ClientMessage::RejectEnrollment { pending_id }
            }
            BinaryClientMessage::RevokeMember { target, .. } => {
                ClientMessage::RevokeMember { target }
            }
            BinaryClientMessage::RenameMember {
                target, new_name, ..
            } => ClientMessage::RenameMember { target, new_name },
            BinaryClientMessage::ReleaseWorkerId { worker_id, .. } => {
                ClientMessage::ReleaseWorkerId { worker_id }
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

/// Serializes a [`ClientMessage`] stamped with an explicit `protocol_version` (for version-skew
/// tests). Normal callers use [`super::codec::serialize_message`], which stamps [`PROTOCOL_VERSION_CURRENT`].
#[cfg(test)]
pub(super) fn serialize_client_message_with_version(
    msg: &ClientMessage,
    protocol_version: u32,
    codec: super::WireCodec,
) -> Result<bytes::Bytes, super::ProtocolError> {
    match codec {
        super::WireCodec::Bincode => {
            let wire = BinaryClientMessage::from_client_message(msg.clone(), protocol_version);
            super::codec::serialize_message(&wire, super::WireCodec::Bincode)
        }
        super::WireCodec::Json => {
            let wire = HumanClientMessage::from_client_message(msg.clone(), protocol_version);
            super::codec::serialize_message(&wire, super::WireCodec::Json)
        }
    }
}
