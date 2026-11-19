use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::capabilities::WorkerCapabilities;
use crate::task::{TaskId, TaskResult, TaskStatus, WorkspaceAffinity};

use super::{default_protocol_version, ClientResponse, PROTOCOL_VERSION_CURRENT};

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub(super) enum HumanClientResponse {
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
        #[serde(default)]
        cancelled_tasks: usize,
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
    Workspaces {
        workspaces: Vec<WorkspaceAffinity>,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    EnrollPending {
        pending_id: String,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    EnrollKeepalive {
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    Enrolled {
        member_id: String,
        roles: Vec<crate::noise_auth::MemberRole>,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    EnrollRejected {
        message: String,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    InviteCreated {
        invite_id: String,
        invite_secret: crate::noise_auth::RedactedBytes32,
        roles: Vec<crate::noise_auth::MemberRole>,
        max_uses: u32,
        expires_at: u64,
        require_approval: bool,
        master_key: crate::noise_auth::PublicKey,
        p2p_ticket: Option<String>,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    InvitesList {
        invites: Vec<crate::noise_auth::InviteSummary>,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    InviteRevoked {
        invite_id: String,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    MembersList {
        members: Vec<crate::noise_auth::MemberEntry>,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    PendingEnrollmentsList {
        pending: Vec<crate::noise_auth::PendingEnrollmentInfo>,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    EnrollmentApproved {
        pending_id: String,
        member_id: String,
        name: String,
        roles: Vec<crate::noise_auth::MemberRole>,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    EnrollmentRejected {
        pending_id: String,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    MemberRevoked {
        member_id: String,
        disconnected_sessions: usize,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    MemberRenamed {
        member_id: String,
        name: String,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
    WorkerIdReleased {
        worker_id: Uuid,
        #[serde(default = "default_protocol_version")]
        protocol_version: u32,
    },
}

#[derive(Serialize, Deserialize)]
pub(super) enum BinaryClientResponse {
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
        cancelled_tasks: usize,
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
    Workspaces {
        workspaces: Vec<WorkspaceAffinity>,
        protocol_version: u32,
    },
    EnrollPending {
        pending_id: String,
        protocol_version: u32,
    },
    EnrollKeepalive {
        protocol_version: u32,
    },
    Enrolled {
        member_id: String,
        roles: Vec<crate::noise_auth::MemberRole>,
        protocol_version: u32,
    },
    EnrollRejected {
        message: String,
        protocol_version: u32,
    },
    InviteCreated {
        invite_id: String,
        invite_secret: crate::noise_auth::RedactedBytes32,
        roles: Vec<crate::noise_auth::MemberRole>,
        max_uses: u32,
        expires_at: u64,
        require_approval: bool,
        master_key: crate::noise_auth::PublicKey,
        p2p_ticket: Option<String>,
        protocol_version: u32,
    },
    InvitesList {
        invites: Vec<crate::noise_auth::InviteSummary>,
        protocol_version: u32,
    },
    InviteRevoked {
        invite_id: String,
        protocol_version: u32,
    },
    MembersList {
        members: Vec<crate::noise_auth::MemberEntry>,
        protocol_version: u32,
    },
    PendingEnrollmentsList {
        pending: Vec<crate::noise_auth::PendingEnrollmentInfo>,
        protocol_version: u32,
    },
    EnrollmentApproved {
        pending_id: String,
        member_id: String,
        name: String,
        roles: Vec<crate::noise_auth::MemberRole>,
        protocol_version: u32,
    },
    EnrollmentRejected {
        pending_id: String,
        protocol_version: u32,
    },
    MemberRevoked {
        member_id: String,
        disconnected_sessions: usize,
        protocol_version: u32,
    },
    MemberRenamed {
        member_id: String,
        name: String,
        protocol_version: u32,
    },
    WorkerIdReleased {
        worker_id: Uuid,
        protocol_version: u32,
    },
}

/// Pre-v3 (protocol version 2) positional bincode layout of [`ClientResponse`], which carried no
/// `protocol_version` trailer. Used so a v3 client can recognize an older v2 Master reply and
/// report [`ProtocolError::IncompatibleVersion`] instead of an opaque bincode EOF error.
#[derive(Deserialize)]
#[allow(dead_code)]
pub(super) enum LegacyV2BinaryClientResponse {
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
pub(super) enum LegacyV3V4BinaryClientResponse {
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

/// Protocol v10 positional bincode layout of [`ClientResponse::ClusterStatus`], which ended at
/// `store_error` before `cancelled_tasks` was appended in v11 (design 0098).
///
/// Variants 0..=3 keep `ClusterStatus` at bincode variant index `4`. Used both so a v11 Master
/// serializes v10-shaped `ClusterStatus` replies to v10 bincode clients and so a v11 client
/// recognizes a v10 Master's `ClusterStatus` frame (reporting `IncompatibleVersion`) instead of an
/// opaque bincode decode error.
#[derive(Serialize, Deserialize)]
#[allow(dead_code)]
pub(super) enum LegacyV10BinaryClientResponse {
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
}

/// Protocol v10 JSON layout of [`ClientResponse::ClusterStatus`], which had no `cancelled_tasks`
/// field (design 0098). Used so a v11 Master serves a v10 JSON client without emitting
/// `cancelled_tasks` (folding it into `failed_tasks`), and frozen v10 test readers can decode
/// v10 JSON `ClusterStatus` payloads. Name starts with `Human` per `crates/core/src/lib.rs`.
#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub(super) enum HumanLegacyV10ClientResponse {
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
}

#[cfg(any(test, feature = "test-extensions"))]
impl HumanLegacyV10ClientResponse {
    pub(super) const fn protocol_version(&self) -> u32 {
        match self {
            Self::ClusterStatus {
                protocol_version, ..
            } => *protocol_version,
        }
    }
}

#[cfg(any(test, feature = "test-extensions"))]
impl From<HumanLegacyV10ClientResponse> for ClientResponse {
    fn from(msg: HumanLegacyV10ClientResponse) -> Self {
        match msg {
            HumanLegacyV10ClientResponse::ClusterStatus {
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
                cancelled_tasks: 0,
            },
        }
    }
}

impl LegacyV10BinaryClientResponse {
    pub(super) const fn protocol_version(&self) -> u32 {
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
            } => *protocol_version,
        }
    }

    #[cfg(any(test, feature = "test-extensions"))]
    pub(super) fn into_cluster_status(self) -> Option<ClientResponse> {
        match self {
            Self::ClusterStatus {
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
            } => Some(ClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                workers,
                active_bytes,
                max_active_bytes,
                store_error,
                cancelled_tasks: 0,
            }),
            _ => None,
        }
    }
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
                cancelled_tasks,
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
                cancelled_tasks,
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
            ClientResponse::Workspaces { workspaces } => HumanClientResponse::Workspaces {
                workspaces,
                protocol_version,
            },
            ClientResponse::EnrollPending { pending_id } => HumanClientResponse::EnrollPending {
                pending_id,
                protocol_version,
            },
            ClientResponse::EnrollKeepalive => {
                HumanClientResponse::EnrollKeepalive { protocol_version }
            }
            ClientResponse::Enrolled { member_id, roles } => HumanClientResponse::Enrolled {
                member_id,
                roles,
                protocol_version,
            },
            ClientResponse::EnrollRejected { message } => HumanClientResponse::EnrollRejected {
                message,
                protocol_version,
            },
            ClientResponse::InviteCreated {
                invite_id,
                invite_secret,
                roles,
                max_uses,
                expires_at,
                require_approval,
                master_key,
                p2p_ticket,
            } => HumanClientResponse::InviteCreated {
                invite_id,
                invite_secret,
                roles,
                max_uses,
                expires_at,
                require_approval,
                master_key,
                p2p_ticket,
                protocol_version,
            },
            ClientResponse::InvitesList { invites } => HumanClientResponse::InvitesList {
                invites,
                protocol_version,
            },
            ClientResponse::InviteRevoked { invite_id } => HumanClientResponse::InviteRevoked {
                invite_id,
                protocol_version,
            },
            ClientResponse::MembersList { members } => HumanClientResponse::MembersList {
                members,
                protocol_version,
            },
            ClientResponse::PendingEnrollmentsList { pending } => {
                HumanClientResponse::PendingEnrollmentsList {
                    pending,
                    protocol_version,
                }
            }
            ClientResponse::EnrollmentApproved {
                pending_id,
                member_id,
                name,
                roles,
            } => HumanClientResponse::EnrollmentApproved {
                pending_id,
                member_id,
                name,
                roles,
                protocol_version,
            },
            ClientResponse::EnrollmentRejected { pending_id } => {
                HumanClientResponse::EnrollmentRejected {
                    pending_id,
                    protocol_version,
                }
            }
            ClientResponse::MemberRevoked {
                member_id,
                disconnected_sessions,
            } => HumanClientResponse::MemberRevoked {
                member_id,
                disconnected_sessions,
                protocol_version,
            },
            ClientResponse::MemberRenamed { member_id, name } => {
                HumanClientResponse::MemberRenamed {
                    member_id,
                    name,
                    protocol_version,
                }
            }
            ClientResponse::WorkerIdReleased { worker_id } => {
                HumanClientResponse::WorkerIdReleased {
                    worker_id,
                    protocol_version,
                }
            }
        }
    }

    pub(super) const fn protocol_version(&self) -> u32 {
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
            }
            | Self::Workspaces {
                protocol_version, ..
            }
            | Self::EnrollPending {
                protocol_version, ..
            }
            | Self::EnrollKeepalive { protocol_version }
            | Self::Enrolled {
                protocol_version, ..
            }
            | Self::EnrollRejected {
                protocol_version, ..
            }
            | Self::InviteCreated {
                protocol_version, ..
            }
            | Self::InvitesList {
                protocol_version, ..
            }
            | Self::InviteRevoked {
                protocol_version, ..
            }
            | Self::MembersList {
                protocol_version, ..
            }
            | Self::PendingEnrollmentsList {
                protocol_version, ..
            }
            | Self::EnrollmentApproved {
                protocol_version, ..
            }
            | Self::EnrollmentRejected {
                protocol_version, ..
            }
            | Self::MemberRevoked {
                protocol_version, ..
            }
            | Self::MemberRenamed {
                protocol_version, ..
            }
            | Self::WorkerIdReleased {
                protocol_version, ..
            } => *protocol_version,
        }
    }

    pub(super) const fn is_error(&self) -> bool {
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
                cancelled_tasks,
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
                cancelled_tasks,
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
            ClientResponse::Workspaces { workspaces } => BinaryClientResponse::Workspaces {
                workspaces,
                protocol_version,
            },
            ClientResponse::EnrollPending { pending_id } => BinaryClientResponse::EnrollPending {
                pending_id,
                protocol_version,
            },
            ClientResponse::EnrollKeepalive => {
                BinaryClientResponse::EnrollKeepalive { protocol_version }
            }
            ClientResponse::Enrolled { member_id, roles } => BinaryClientResponse::Enrolled {
                member_id,
                roles,
                protocol_version,
            },
            ClientResponse::EnrollRejected { message } => BinaryClientResponse::EnrollRejected {
                message,
                protocol_version,
            },
            ClientResponse::InviteCreated {
                invite_id,
                invite_secret,
                roles,
                max_uses,
                expires_at,
                require_approval,
                master_key,
                p2p_ticket,
            } => BinaryClientResponse::InviteCreated {
                invite_id,
                invite_secret,
                roles,
                max_uses,
                expires_at,
                require_approval,
                master_key,
                p2p_ticket,
                protocol_version,
            },
            ClientResponse::InvitesList { invites } => BinaryClientResponse::InvitesList {
                invites,
                protocol_version,
            },
            ClientResponse::InviteRevoked { invite_id } => BinaryClientResponse::InviteRevoked {
                invite_id,
                protocol_version,
            },
            ClientResponse::MembersList { members } => BinaryClientResponse::MembersList {
                members,
                protocol_version,
            },
            ClientResponse::PendingEnrollmentsList { pending } => {
                BinaryClientResponse::PendingEnrollmentsList {
                    pending,
                    protocol_version,
                }
            }
            ClientResponse::EnrollmentApproved {
                pending_id,
                member_id,
                name,
                roles,
            } => BinaryClientResponse::EnrollmentApproved {
                pending_id,
                member_id,
                name,
                roles,
                protocol_version,
            },
            ClientResponse::EnrollmentRejected { pending_id } => {
                BinaryClientResponse::EnrollmentRejected {
                    pending_id,
                    protocol_version,
                }
            }
            ClientResponse::MemberRevoked {
                member_id,
                disconnected_sessions,
            } => BinaryClientResponse::MemberRevoked {
                member_id,
                disconnected_sessions,
                protocol_version,
            },
            ClientResponse::MemberRenamed { member_id, name } => {
                BinaryClientResponse::MemberRenamed {
                    member_id,
                    name,
                    protocol_version,
                }
            }
            ClientResponse::WorkerIdReleased { worker_id } => {
                BinaryClientResponse::WorkerIdReleased {
                    worker_id,
                    protocol_version,
                }
            }
        }
    }

    pub(super) const fn protocol_version(&self) -> u32 {
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
            }
            | Self::Workspaces {
                protocol_version, ..
            }
            | Self::EnrollPending {
                protocol_version, ..
            }
            | Self::EnrollKeepalive { protocol_version }
            | Self::Enrolled {
                protocol_version, ..
            }
            | Self::EnrollRejected {
                protocol_version, ..
            }
            | Self::InviteCreated {
                protocol_version, ..
            }
            | Self::InvitesList {
                protocol_version, ..
            }
            | Self::InviteRevoked {
                protocol_version, ..
            }
            | Self::MembersList {
                protocol_version, ..
            }
            | Self::PendingEnrollmentsList {
                protocol_version, ..
            }
            | Self::EnrollmentApproved {
                protocol_version, ..
            }
            | Self::EnrollmentRejected {
                protocol_version, ..
            }
            | Self::MemberRevoked {
                protocol_version, ..
            }
            | Self::MemberRenamed {
                protocol_version, ..
            }
            | Self::WorkerIdReleased {
                protocol_version, ..
            } => *protocol_version,
        }
    }

    pub(super) const fn is_error(&self) -> bool {
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
                cancelled_tasks,
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
                cancelled_tasks,
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
            HumanClientResponse::Workspaces { workspaces, .. } => {
                ClientResponse::Workspaces { workspaces }
            }
            HumanClientResponse::EnrollPending { pending_id, .. } => {
                ClientResponse::EnrollPending { pending_id }
            }
            HumanClientResponse::EnrollKeepalive { .. } => ClientResponse::EnrollKeepalive,
            HumanClientResponse::Enrolled {
                member_id, roles, ..
            } => ClientResponse::Enrolled { member_id, roles },
            HumanClientResponse::EnrollRejected { message, .. } => {
                ClientResponse::EnrollRejected { message }
            }
            HumanClientResponse::InviteCreated {
                invite_id,
                invite_secret,
                roles,
                max_uses,
                expires_at,
                require_approval,
                master_key,
                p2p_ticket,
                ..
            } => ClientResponse::InviteCreated {
                invite_id,
                invite_secret,
                roles,
                max_uses,
                expires_at,
                require_approval,
                master_key,
                p2p_ticket,
            },
            HumanClientResponse::InvitesList { invites, .. } => {
                ClientResponse::InvitesList { invites }
            }
            HumanClientResponse::InviteRevoked { invite_id, .. } => {
                ClientResponse::InviteRevoked { invite_id }
            }
            HumanClientResponse::MembersList { members, .. } => {
                ClientResponse::MembersList { members }
            }
            HumanClientResponse::PendingEnrollmentsList { pending, .. } => {
                ClientResponse::PendingEnrollmentsList { pending }
            }
            HumanClientResponse::EnrollmentApproved {
                pending_id,
                member_id,
                name,
                roles,
                ..
            } => ClientResponse::EnrollmentApproved {
                pending_id,
                member_id,
                name,
                roles,
            },
            HumanClientResponse::EnrollmentRejected { pending_id, .. } => {
                ClientResponse::EnrollmentRejected { pending_id }
            }
            HumanClientResponse::MemberRevoked {
                member_id,
                disconnected_sessions,
                ..
            } => ClientResponse::MemberRevoked {
                member_id,
                disconnected_sessions,
            },
            HumanClientResponse::MemberRenamed {
                member_id, name, ..
            } => ClientResponse::MemberRenamed { member_id, name },
            HumanClientResponse::WorkerIdReleased { worker_id, .. } => {
                ClientResponse::WorkerIdReleased { worker_id }
            }
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
                cancelled_tasks,
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
                cancelled_tasks,
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
            BinaryClientResponse::Workspaces { workspaces, .. } => {
                ClientResponse::Workspaces { workspaces }
            }
            BinaryClientResponse::EnrollPending { pending_id, .. } => {
                ClientResponse::EnrollPending { pending_id }
            }
            BinaryClientResponse::EnrollKeepalive { .. } => ClientResponse::EnrollKeepalive,
            BinaryClientResponse::Enrolled {
                member_id, roles, ..
            } => ClientResponse::Enrolled { member_id, roles },
            BinaryClientResponse::EnrollRejected { message, .. } => {
                ClientResponse::EnrollRejected { message }
            }
            BinaryClientResponse::InviteCreated {
                invite_id,
                invite_secret,
                roles,
                max_uses,
                expires_at,
                require_approval,
                master_key,
                p2p_ticket,
                ..
            } => ClientResponse::InviteCreated {
                invite_id,
                invite_secret,
                roles,
                max_uses,
                expires_at,
                require_approval,
                master_key,
                p2p_ticket,
            },
            BinaryClientResponse::InvitesList { invites, .. } => {
                ClientResponse::InvitesList { invites }
            }
            BinaryClientResponse::InviteRevoked { invite_id, .. } => {
                ClientResponse::InviteRevoked { invite_id }
            }
            BinaryClientResponse::MembersList { members, .. } => {
                ClientResponse::MembersList { members }
            }
            BinaryClientResponse::PendingEnrollmentsList { pending, .. } => {
                ClientResponse::PendingEnrollmentsList { pending }
            }
            BinaryClientResponse::EnrollmentApproved {
                pending_id,
                member_id,
                name,
                roles,
                ..
            } => ClientResponse::EnrollmentApproved {
                pending_id,
                member_id,
                name,
                roles,
            },
            BinaryClientResponse::EnrollmentRejected { pending_id, .. } => {
                ClientResponse::EnrollmentRejected { pending_id }
            }
            BinaryClientResponse::MemberRevoked {
                member_id,
                disconnected_sessions,
                ..
            } => ClientResponse::MemberRevoked {
                member_id,
                disconnected_sessions,
            },
            BinaryClientResponse::MemberRenamed {
                member_id, name, ..
            } => ClientResponse::MemberRenamed { member_id, name },
            BinaryClientResponse::WorkerIdReleased { worker_id, .. } => {
                ClientResponse::WorkerIdReleased { worker_id }
            }
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

/// Serializes a [`ClientResponse`] stamped with an explicit `protocol_version`: the reply to a peer
/// of that version (design 0083 D3) and the version-skew tests. [`super::codec::serialize_message`]
/// stamps [`PROTOCOL_VERSION_CURRENT`].
pub(crate) fn serialize_client_response_with_version(
    msg: &ClientResponse,
    protocol_version: u32,
    codec: super::WireCodec,
) -> Result<bytes::Bytes, super::ProtocolError> {
    match codec {
        super::WireCodec::Bincode => {
            let wire = BinaryClientResponse::from_client_response(msg.clone(), protocol_version);
            super::codec::serialize_message(&wire, super::WireCodec::Bincode)
        }
        super::WireCodec::Json => {
            let wire = HumanClientResponse::from_client_response(msg.clone(), protocol_version);
            super::codec::serialize_message(&wire, super::WireCodec::Json)
        }
    }
}

/// Serializes a [`ClientResponse`] for a peer speaking `peer_version` (design 0083 D3, design 0098).
///
/// For `peer_version >= 11`, encodes the current layout stamped with `peer_version`. For
/// `peer_version < 11` (protocol 10), `ClusterStatus` folds `cancelled_tasks` back into
/// `failed_tasks` via [`ClientResponse::for_peer`] and omits `cancelled_tasks` from both wire
/// formats (`LegacyV10BinaryClientResponse` / `HumanLegacyV10ClientResponse`).
pub(super) fn serialize_client_response_for_peer(
    msg: &ClientResponse,
    peer_version: u32,
    codec: super::WireCodec,
) -> Result<bytes::Bytes, super::ProtocolError> {
    if peer_version >= 11 {
        return serialize_client_response_with_version(msg, peer_version, codec);
    }
    let adapted = msg.for_peer(peer_version);
    match adapted {
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
            cancelled_tasks: _,
        } => match codec {
            super::WireCodec::Bincode => {
                let wire = LegacyV10BinaryClientResponse::ClusterStatus {
                    total_tasks,
                    pending_tasks,
                    running_tasks,
                    completed_tasks,
                    failed_tasks,
                    workers,
                    active_bytes,
                    max_active_bytes,
                    store_error,
                    protocol_version: peer_version,
                };
                super::codec::serialize_message(&wire, super::WireCodec::Bincode)
            }
            super::WireCodec::Json => {
                let wire = HumanLegacyV10ClientResponse::ClusterStatus {
                    total_tasks,
                    pending_tasks,
                    running_tasks,
                    completed_tasks,
                    failed_tasks,
                    workers,
                    active_bytes,
                    max_active_bytes,
                    store_error,
                    protocol_version: peer_version,
                };
                super::codec::serialize_message(&wire, super::WireCodec::Json)
            }
        },
        other => serialize_client_response_with_version(&other, peer_version, codec),
    }
}
