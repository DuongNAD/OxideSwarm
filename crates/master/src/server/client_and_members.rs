//! Client request dispatch, cluster member store resync, enrollment, and admin handling.

#[cfg(feature = "p2p")]
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{oneshot, watch};
use tracing::{debug, info, warn};
use uuid::Uuid;

use rusty_grid_core::blob_transfer::BlobReply;
use rusty_grid_core::noise_auth::{AuthenticatedV3Peer, HandshakePurpose};
use rusty_grid_core::protocol::{
    format_incompatible_version_error, ClientMessage, ClientResponse, ProtocolError,
};
use rusty_grid_core::task::{TaskId, TaskStatus};
use rusty_grid_core::{GridError, GridResult, MessageTransport, WorkerCapabilities};

use crate::action_cache::ActionCache;
use crate::blob_store::BlobStore;
use crate::mapreduce::MapReduceEngine;
use crate::member_connections::ConnectionId;
use crate::queue::{TaskQueue, TaskState};
use crate::registry::{WorkerInfo, WorkerRegistry};

use super::handshakes::{check_v3_client_message_role, MasterAuthContext, REFUSAL_DRAIN_TIMEOUT};
use super::members::{self, MemberStore};
use super::tasks_and_blobs::{
    admit_blob_task, cancel_task_and_notify, handle_workers_revoked_with_broadcast,
    prune_closed_waiters, send_client_response, try_serve_action_cache_hit, BlobPipeline,
    UploadOwner, NO_BLOB_STORE,
};
use super::WaiterMap;

/// Answers `GetTaskStatus` (and the non-terminal half of `GetTaskResult`).
async fn task_status_response(queue: &TaskQueue, task_id: TaskId) -> ClientResponse {
    match queue.get_status_detail(&task_id).await {
        Some(detail) => {
            let info = detail.info;
            let (status, err) = match detail.result_exit_code {
                Some(0) => (TaskStatus::Completed, detail.result_error),
                Some(130) => (TaskStatus::Cancelled, detail.result_error),
                Some(_) => (TaskStatus::Failed, detail.result_error),
                None => match info.state {
                    TaskState::Submitted
                    | TaskState::Queued
                    | TaskState::Scheduled
                    | TaskState::Retrying => (TaskStatus::Queued, None),
                    TaskState::Running => (TaskStatus::Running, None),
                    TaskState::Completed => (TaskStatus::Completed, None),
                    TaskState::Failed => (TaskStatus::Failed, info.error_message.clone()),
                    TaskState::Cancelled => (TaskStatus::Cancelled, info.error_message.clone()),
                },
            };
            ClientResponse::TaskStatusInfo {
                task_id,
                status,
                state_name: format!("{:?}", info.state),
                assigned_worker: info.assigned_worker_id,
                error: err,
            }
        }
        None => ClientResponse::Error {
            message: format!("Task {task_id} not found"),
        },
    }
}

/// A worker as `ClusterStatus` and `ListWorkers` report it: its capabilities plus the tags only the
/// master knows, `link:<path>`, `id:<worker id>`, `state:<state>` and `proto:<version>` (see
/// `rusty_grid_core::placement`). Clients get no `WorkerInfo`, so the `id:` tag is how they learn
/// the id a `require:worker=` constraint can name.
async fn listed_worker(
    w: WorkerInfo,
    #[cfg(feature = "p2p")] p2p_conns: Option<
        &Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>>,
    >,
) -> WorkerCapabilities {
    #[cfg(feature = "p2p")]
    let link = match p2p_conns {
        Some(conns) => match conns.read().await.get(&w.worker_id) {
            Some(conn) => match rusty_grid_core::transport::inspect_connection_paths(conn) {
                Some(info) if info.is_relay => format!("link:Relay (DERP):{:.1}ms", info.rtt_ms),
                Some(info) => format!("link:Direct P2P (QUIC):{:.1}ms", info.rtt_ms),
                None => "link:Direct P2P (QUIC)".to_string(),
            },
            None => "link:TCP/LAN".to_string(),
        },
        None => "link:TCP/LAN".to_string(),
    };
    #[cfg(not(feature = "p2p"))]
    let link = "link:TCP/LAN".to_string();

    let mut caps = w.capabilities;
    caps.tags.push(link);
    caps.tags.push(format!(
        "{}{}",
        rusty_grid_core::placement::ID_TAG_PREFIX,
        w.worker_id
    ));
    caps.tags.push(format!(
        "{}{}",
        rusty_grid_core::placement::STATE_TAG_PREFIX,
        w.status.as_str()
    ));
    if w.features
        .contains(rusty_grid_core::protocol::ProtocolCapabilities::WORKSPACES)
    {
        caps.tags
            .push(rusty_grid_core::placement::FEAT_WORKSPACES_TAG.to_string());
    }
    caps
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_client_connection<S: AsyncRead + AsyncWrite + Unpin + Send>(
    first_msg: ClientMessage,
    mut transport: MessageTransport<S>,
    registry: WorkerRegistry,
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    waiters: WaiterMap,
    mapreduce_parallelism: usize,
    blob_store: Option<Arc<BlobStore>>,
    action_cache: Option<Arc<ActionCache>>,
    auth_ctx: MasterAuthContext,
    v3_peer: Option<AuthenticatedV3Peer>,
    member_conn: Option<ConnectionId>,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<
        tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>,
    >,
    #[cfg(feature = "p2p")] p2p_conns: Option<
        Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>>,
    >,
) -> GridResult<()> {
    let client_submitter = crate::fair_share::SubmitterId::Client(Uuid::new_v4());
    let mut next_msg = Some(first_msg);
    let mut first_iteration = true;
    // Uploads this connection leaves unfinished are dropped when it ends.
    let uploads = blob_store
        .as_ref()
        .map(|store| UploadOwner::new(Arc::clone(store)));
    let mapreduce = MapReduceEngine::new(queue.clone(), scheduler_notify.clone(), waiters.clone())
        .with_registry(registry.clone())
        .with_parallelism(mapreduce_parallelism);

    // Blob requests of this connection that are being handled, oldest first. Their replies are
    // sent in that order (the client matches them by position), and no other answer is sent
    // while one is pending.
    let mut blob_pipeline = BlobPipeline::default();

    loop {
        let received = match next_msg.take() {
            Some(m) => Ok(Some(m)),
            None => loop {
                // A full pipeline stops reading: further requests wait in the socket buffers.
                if blob_pipeline.is_full() {
                    let reply = blob_pipeline.next_reply().await;
                    send_client_response(&mut transport, &ClientResponse::BlobReply { reply })
                        .await?;
                    continue;
                }
                if blob_pipeline.is_empty() {
                    break transport.recv_msg::<ClientMessage>().await;
                }
                tokio::select! {
                    biased;
                    reply = blob_pipeline.next_reply() => {
                        send_client_response(&mut transport, &ClientResponse::BlobReply { reply })
                            .await?;
                    }
                    received = transport.recv_msg::<ClientMessage>() => break received,
                }
            },
        };
        let msg = match received {
            Ok(Some(m)) => m,
            Ok(None) => break,
            Err(ProtocolError::IncompatibleVersion { peer_version, .. }) => {
                let message = format_incompatible_version_error("client", peer_version);
                warn!(peer_version, error = %message, "Refusing client message with incompatible protocol version");
                let _ = send_client_response(
                    &mut transport,
                    &ClientResponse::Error {
                        message: message.clone(),
                    },
                )
                .await;
                return Err(GridError::IncompatibleVersion(message));
            }
            Err(e) => return Err(e.into()),
        };

        // Every other message is answered after the blob replies that precede it.
        if !matches!(msg, ClientMessage::BlobRequest { .. }) {
            while !blob_pipeline.is_empty() {
                let reply = blob_pipeline.next_reply().await;
                send_client_response(&mut transport, &ClientResponse::BlobReply { reply }).await?;
            }
        }

        let is_first = std::mem::replace(&mut first_iteration, false);
        if let Some(ref peer) = v3_peer {
            match peer.purpose {
                HandshakePurpose::Enroll => {
                    if !is_first || !matches!(msg, ClientMessage::Enroll { .. }) {
                        warn!(
                            member_id = %peer.member_id,
                            "Refusing non-Enroll message on OXS-AUTH v3 enroll connection"
                        );
                        break;
                    }
                }
                HandshakePurpose::Member => {
                    auth_ctx.members.touch_last_seen(&peer.public_key);
                    if let Err((log_msg, wire_reason)) =
                        check_v3_client_message_role(&auth_ctx, &peer.public_key, &msg)
                    {
                        warn!(member_id = %peer.member_id, "{log_msg}");
                        send_client_response(
                            &mut transport,
                            &ClientResponse::Error {
                                message: format!("Authentication failed: {wire_reason}"),
                            },
                        )
                        .await?;
                        continue;
                    }
                }
            }
        }

        match msg {
            ClientMessage::SubmitTask { task, wait } => {
                let task_id = task.id;
                // A task id the queue already knows is refused by `submit` below; its pins
                // belong to the original task and must stay untouched.
                let known = queue.get_task(&task_id).await.is_some();
                let mut needs_tree_v2 = false;
                if !known {
                    match admit_blob_task(blob_store.as_ref(), &task).await {
                        Ok(v2) => needs_tree_v2 = v2,
                        Err(message) => {
                            warn!(task_id = %task_id, error = %message, "SubmitTask rejected");
                            send_client_response(
                                &mut transport,
                                &ClientResponse::Error { message },
                            )
                            .await?;
                            continue;
                        }
                    }

                    match try_serve_action_cache_hit(
                        &task,
                        0,
                        client_submitter,
                        &queue,
                        &waiters,
                        blob_store.as_ref(),
                        action_cache.as_ref(),
                        #[cfg(feature = "dashboard")]
                        broadcast_tx.as_ref(),
                    )
                    .await
                    {
                        Ok(Some(cached_result)) => {
                            let resp = if wait {
                                ClientResponse::TaskCompleted {
                                    task_id,
                                    result: cached_result,
                                }
                            } else {
                                ClientResponse::TaskSubmitted { task_id }
                            };
                            send_client_response(&mut transport, &resp).await?;
                            continue;
                        }
                        Ok(None) => {}
                        Err(e) => {
                            if let Some(store) = blob_store.as_ref() {
                                store.unpin_task(&task_id).await;
                            }
                            warn!(task_id = %task_id, error = %e, "SubmitTask rejected");
                            let resp = ClientResponse::Error {
                                message: e.to_string(),
                            };
                            send_client_response(&mut transport, &resp).await?;
                            continue;
                        }
                    }
                }
                let rx = if wait {
                    let (tx, rx) = oneshot::channel();
                    let mut lock = waiters.write().await;
                    lock.entry(task_id).or_default().push(tx);
                    Some(rx)
                } else {
                    None
                };

                if let Err(e) = queue
                    .submit_from_with_tree_v2(task, 0, client_submitter, needs_tree_v2)
                    .await
                {
                    // Validation failure, queue full, or a task id that is already in use: tell the
                    // client why, keep the connection, and take back the waiter registered above
                    // (it must never be resolved by the result of another task with the same id).
                    drop(rx);
                    prune_closed_waiters(&waiters, &task_id).await;
                    if let (false, Some(store)) = (known, blob_store.as_ref()) {
                        store.unpin_task(&task_id).await;
                    }
                    warn!(task_id = %task_id, error = %e, "SubmitTask rejected");
                    let resp = ClientResponse::Error {
                        message: e.to_string(),
                    };
                    send_client_response(&mut transport, &resp).await?;
                    continue;
                }
                scheduler_notify.notify_one();

                #[cfg(feature = "dashboard")]
                if let Some(ref b_tx) = broadcast_tx {
                    if b_tx.receiver_count() > 0 {
                        if let Some(info) = queue.get_task(&task_id).await {
                            let _ = b_tx.send(
                                crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(info),
                            );
                        }
                        let _ =
                            b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(
                                queue.stats().await,
                            ));
                    }
                }

                if let Some(rx) = rx {
                    match rx.await {
                        Ok(result) => {
                            let resp = ClientResponse::TaskCompleted { task_id, result };
                            send_client_response(&mut transport, &resp).await?;
                        }
                        Err(_) => {
                            let resp = ClientResponse::Error {
                                message: "Task completion channel closed unexpectedly".into(),
                            };
                            send_client_response(&mut transport, &resp).await?;
                        }
                    }
                } else {
                    let resp = ClientResponse::TaskSubmitted { task_id };
                    send_client_response(&mut transport, &resp).await?;
                }
            }
            ClientMessage::GetTaskStatus { task_id } => {
                let resp = task_status_response(&queue, task_id).await;
                send_client_response(&mut transport, &resp).await?;
            }
            ClientMessage::CancelTask { task_id } => {
                // Cancels in the queue, tells the executing worker to stop and wakes `wait`ers.
                // `success` is false for unknown ids and for tasks that had already finished.
                let success = match cancel_task_and_notify(
                    &registry,
                    &queue,
                    &scheduler_notify,
                    &waiters,
                    task_id,
                    "Cancelled by client",
                )
                .await
                {
                    Ok(newly_cancelled) => newly_cancelled,
                    Err(e) => {
                        debug!(task_id = %task_id, error = %e, "CancelTask failed");
                        false
                    }
                };
                #[cfg(feature = "dashboard")]
                if success {
                    if let Some(ref b_tx) = broadcast_tx {
                        if b_tx.receiver_count() > 0 {
                            if let Some(info) = queue.get_task(&task_id).await {
                                let _ = b_tx.send(
                                    crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(
                                        info,
                                    ),
                                );
                            }
                            let _ = b_tx.send(
                                crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(
                                    queue.stats().await,
                                ),
                            );
                        }
                    }
                }
                let resp = ClientResponse::TaskCancelled { task_id, success };
                send_client_response(&mut transport, &resp).await?;
            }
            ClientMessage::ClusterStatus => {
                let workers_info = registry.list_all_workers().await;
                let mut workers = Vec::with_capacity(workers_info.len());
                for w in workers_info {
                    workers.push(
                        listed_worker(
                            w,
                            #[cfg(feature = "p2p")]
                            p2p_conns.as_ref(),
                        )
                        .await,
                    );
                }
                let stats = queue.stats().await;
                let (pending, running, completed, failed, cancelled) = stats.cluster_buckets();
                let resp = ClientResponse::ClusterStatus {
                    total_tasks: stats.total,
                    pending_tasks: pending,
                    running_tasks: running,
                    completed_tasks: completed,
                    failed_tasks: failed,
                    cancelled_tasks: cancelled,
                    active_bytes: stats.active_bytes,
                    max_active_bytes: stats.max_active_bytes,
                    store_error: stats.store_error,
                    workers,
                };
                send_client_response(&mut transport, &resp).await?;
            }
            ClientMessage::Hello => {
                let resp = ClientResponse::Hello {
                    min_compatible_version:
                        rusty_grid_core::protocol::PROTOCOL_VERSION_MIN_COMPATIBLE,
                };
                send_client_response(&mut transport, &resp).await?;
            }
            ClientMessage::ListWorkers => {
                let workers_info = registry.list_all_workers().await;
                let mut workers = Vec::with_capacity(workers_info.len());
                for w in workers_info {
                    workers.push(
                        listed_worker(
                            w,
                            #[cfg(feature = "p2p")]
                            p2p_conns.as_ref(),
                        )
                        .await,
                    );
                }
                let resp = ClientResponse::WorkerList { workers };
                send_client_response(&mut transport, &resp).await?;
            }
            ClientMessage::ListWorkspaces => {
                let workspaces = queue.list_workspaces().await;
                let resp = ClientResponse::Workspaces { workspaces };
                send_client_response(&mut transport, &resp).await?;
            }
            ClientMessage::SubmitMapReduce { job } => {
                let job_id = job.job_id;
                let mut exec = std::pin::pin!(mapreduce.execute(job));
                let outcome = loop {
                    tokio::select! {
                        res = &mut exec => break Some(res),
                        peer = transport.recv_msg::<ClientMessage>(), if next_msg.is_none() => {
                            match peer {
                                Ok(Some(later)) => {
                                    next_msg = Some(later);
                                }
                                Ok(None) | Err(_) => {
                                    info!(
                                        job_id = %job_id,
                                        "Client disconnected while MapReduce job was in flight; cancelling job"
                                    );
                                    break None;
                                }
                            }
                        }
                    }
                };
                match outcome {
                    Some(Ok(result)) => {
                        let resp = ClientResponse::MapReduceCompleted { result };
                        send_client_response(&mut transport, &resp).await?;
                    }
                    Some(Err(e)) => {
                        let resp = ClientResponse::Error {
                            message: e.to_string(),
                        };
                        send_client_response(&mut transport, &resp).await?;
                    }
                    None => break,
                }
            }
            ClientMessage::GetTaskResult { task_id } => {
                // Non-blocking poll: the retained result of a finished task, its status while it is
                // still in flight, an error for an id the master does not know (or no longer keeps).
                let resp = match queue.get_task(&task_id).await {
                    Some(info) if info.state.is_terminal() => {
                        match queue.get_result(&task_id).await {
                            Some(result) => ClientResponse::TaskCompleted { task_id, result },
                            None => task_status_response(&queue, task_id).await,
                        }
                    }
                    _ => task_status_response(&queue, task_id).await,
                };
                send_client_response(&mut transport, &resp).await?;
            }
            ClientMessage::BlobRequest { request } => match (&blob_store, &uploads) {
                (Some(store), Some(uploads)) => {
                    blob_pipeline.push(Arc::clone(store), uploads.owner, request);
                }
                _ => {
                    let reply = BlobReply::Failed {
                        message: NO_BLOB_STORE.to_string(),
                    };
                    // Keep the answers in request order.
                    blob_pipeline.push_ready(reply);
                }
            },
            ClientMessage::Enroll {
                invite_id,
                proof,
                name,
                os,
                arch,
            } => {
                handle_enroll_connection(
                    &mut transport,
                    v3_peer.as_ref(),
                    &auth_ctx,
                    &registry,
                    &queue,
                    &scheduler_notify,
                    &waiters,
                    #[cfg(feature = "dashboard")]
                    broadcast_tx.as_ref(),
                    &invite_id,
                    &proof,
                    name,
                    os,
                    arch,
                )
                .await;
                break;
            }
            msg @ (ClientMessage::CreateInvite { .. }
            | ClientMessage::ListInvites
            | ClientMessage::RevokeInvite { .. }
            | ClientMessage::ListMembers
            | ClientMessage::ListPendingEnrollments
            | ClientMessage::ApproveEnrollment { .. }
            | ClientMessage::RejectEnrollment { .. }
            | ClientMessage::RevokeMember { .. }
            | ClientMessage::RenameMember { .. }
            | ClientMessage::ReleaseWorkerId { .. }) => {
                handle_member_admin_message(
                    &mut transport,
                    msg,
                    &auth_ctx,
                    v3_peer.as_ref(),
                    member_conn,
                    &registry,
                    &queue,
                    &scheduler_notify,
                    &waiters,
                    #[cfg(feature = "dashboard")]
                    broadcast_tx.as_ref(),
                )
                .await?;
            }
        }
    }

    Ok(())
}

pub(super) const ENROLL_CONFIRM_TIMEOUT_SECS: u64 = 120;

/// How often the master retries flushing `members.json` when an earlier rollback, resync, or
/// bookkeeping write failed (`S1`, `NIT 2`).
const MEMBER_STORE_RESYNC_INTERVAL: Duration = Duration::from_secs(2);

pub(super) async fn run_member_store_blocking<T, F>(f: F) -> GridResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> GridResult<T> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| GridError::Config(format!("member store task panicked: {e}")))?
}

pub(super) fn spawn_member_store_resync(
    members: Arc<MemberStore>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(MEMBER_STORE_RESYNC_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if members.is_dirty() {
                        let m = Arc::clone(&members);
                        match run_member_store_blocking(move || m.flush_dirty()).await {
                            Ok(()) => {
                                info!("Flushed previously failed members.json update to disk");
                            }
                            Err(e) => {
                                warn!(
                                    error = %e,
                                    "Periodic retry failed to flush dirty members.json"
                                );
                            }
                        }
                    }
                }
                res = shutdown_rx.changed() => {
                    if res.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }
    });
}

#[allow(clippy::too_many_arguments)]
async fn rollback_enrollment_and_disconnect_workers(
    auth_ctx: &MasterAuthContext,
    rollback: members::ApprovedEnrollmentRollback,
    disconnect_reason: &str,
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: &WaiterMap,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<
        &tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>,
    >,
) {
    let member_id = rollback.member_id.clone();
    let pubkey = rollback.public_key;
    let members_store = auth_ctx.members.clone();
    let released_claims = match run_member_store_blocking(move || {
        members_store.rollback_approved_enrollment(rollback)
    })
    .await
    {
        Ok(claims) => claims,
        Err(e) => {
            warn!(
                member_id = %member_id,
                error = %e,
                "Failed to persist rollback of unconfirmed OXS-AUTH v3 enrollment"
            );
            Vec::new()
        }
    };
    if auth_ctx.members.get(&pubkey).is_none() {
        // The rolled-back key is no member any more: whatever it opened meanwhile closes too.
        let closed = auth_ctx.member_connections.close_member(&member_id, None);
        if !closed.is_empty() {
            info!(
                member_id = %member_id,
                closed = closed.len(),
                "Closed the connections of a rolled-back enrollment"
            );
        }
    }
    let still_has_worker_role = auth_ctx
        .members
        .get(&pubkey)
        .is_some_and(|m| m.has_worker_role());
    if !still_has_worker_role {
        // Without the worker role these workers cannot register again, so their tasks take the
        // revocation path: requeued at once, without the resume hold and their checkpoints.
        let mut wids = released_claims;
        for wid in registry.release_workers_owned_by(&member_id).await {
            if !wids.contains(&wid) {
                wids.push(wid);
            }
        }
        for wid in &wids {
            registry.release_worker_owner(wid).await;
        }
        handle_workers_revoked_with_broadcast(
            registry,
            queue,
            scheduler_notify,
            waiters,
            &wids,
            disconnect_reason,
            #[cfg(feature = "dashboard")]
            broadcast_tx,
        )
        .await;
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_enroll_connection<S>(
    transport: &mut MessageTransport<S>,
    v3_peer: Option<&AuthenticatedV3Peer>,
    auth_ctx: &MasterAuthContext,
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: &WaiterMap,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<
        &tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>,
    >,
    invite_id: &str,
    proof: &rusty_grid_core::noise_auth::RedactedBytes32,
    name: String,
    os: String,
    arch: String,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let Some(peer) = v3_peer else {
        let _ = send_client_response(
            transport,
            &ClientResponse::EnrollRejected {
                message: "ENROLL requires an OXS-AUTH v3 enroll handshake".into(),
            },
        )
        .await;
        return;
    };
    if peer.purpose != HandshakePurpose::Enroll {
        let _ = send_client_response(
            transport,
            &ClientResponse::EnrollRejected {
                message: "ENROLL requires an OXS-AUTH v3 enroll handshake".into(),
            },
        )
        .await;
        return;
    }
    let members_store = auth_ctx.members.clone();
    let peer_pk = peer.public_key;
    let hh_bytes = *peer.handshake_hash.as_bytes();
    let master_fp = peer.master_fingerprint.clone();
    let remote_addr = peer.remote_addr.clone();
    let invite_id_owned = invite_id.to_string();
    let proof_owned = *proof;
    match run_member_store_blocking(move || {
        members_store.handle_enroll_request(
            peer_pk,
            &hh_bytes,
            &master_fp,
            &remote_addr,
            &invite_id_owned,
            &proof_owned,
            name,
            os,
            arch,
        )
    })
    .await
    {
        Ok(members::EnrollOutcome::Rejected {
            log_reason,
            wire_message,
        }) => {
            warn!(
                member_id = %peer.member_id,
                reason = %log_reason,
                "Refusing v3 enrollment request"
            );
            let _ = send_client_response(
                transport,
                &ClientResponse::EnrollRejected {
                    message: wire_message,
                },
            )
            .await;
            let _ = tokio::time::timeout(
                REFUSAL_DRAIN_TIMEOUT,
                transport.recv_raw_frame_max(rusty_grid_core::cluster_auth::PRE_AUTH_MAX_FRAME),
            )
            .await;
        }
        Ok(members::EnrollOutcome::ImmediateApproved { member_id, roles }) => {
            info!(
                member_id = %member_id,
                ?roles,
                "Enrolled v3 member (--no-approval)"
            );
            let _ = send_client_response(transport, &ClientResponse::Enrolled { member_id, roles })
                .await;
            let _ = tokio::time::timeout(
                REFUSAL_DRAIN_TIMEOUT,
                transport.recv_raw_frame_max(rusty_grid_core::cluster_auth::PRE_AUTH_MAX_FRAME),
            )
            .await;
        }
        Ok(members::EnrollOutcome::Pending {
            pending_id,
            decision_rx,
        }) => {
            wait_for_pending_enrollment(
                transport,
                auth_ctx,
                registry,
                queue,
                scheduler_notify,
                waiters,
                #[cfg(feature = "dashboard")]
                broadcast_tx,
                &peer.member_id,
                pending_id,
                decision_rx,
            )
            .await;
        }
        Err(e) => {
            warn!(
                member_id = %peer.member_id,
                error = %e,
                "Failed to process v3 enrollment request"
            );
            let _ = send_client_response(
                transport,
                &ClientResponse::EnrollRejected {
                    message: members::GENERIC_ENROLL_REJECT_MESSAGE.to_string(),
                },
            )
            .await;
            let _ = tokio::time::timeout(
                REFUSAL_DRAIN_TIMEOUT,
                transport.recv_raw_frame_max(rusty_grid_core::cluster_auth::PRE_AUTH_MAX_FRAME),
            )
            .await;
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn wait_for_pending_enrollment<S>(
    transport: &mut MessageTransport<S>,
    auth_ctx: &MasterAuthContext,
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: &WaiterMap,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<
        &tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>,
    >,
    member_id: &str,
    pending_id: String,
    mut decision_rx: tokio::sync::oneshot::Receiver<members::PendingDecision>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    info!(
        pending_id = %pending_id,
        member_id = %member_id,
        "Queued v3 enrollment request for admin approval"
    );
    let mut decision_consumed = false;
    if send_client_response(
        transport,
        &ClientResponse::EnrollPending {
            pending_id: pending_id.clone(),
        },
    )
    .await
    .is_ok()
    {
        let enroll_deadline =
            tokio::time::Instant::now() + Duration::from_secs(members::PENDING_ENROLLMENT_TTL_SECS);
        let mut keepalive_interval = tokio::time::interval(Duration::from_secs(15));
        keepalive_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        keepalive_interval.tick().await;
        loop {
            tokio::select! {
                dec = &mut decision_rx => {
                    decision_consumed = true;
                    match dec {
                        Ok(members::PendingDecision::Approved {
                            member_id,
                            roles,
                            rollback,
                        }) => {
                            info!(
                                pending_id = %pending_id,
                                member_id = %member_id,
                                ?roles,
                                "Admin approved pending v3 enrollment; waiting for joining machine confirmation"
                            );
                            if send_client_response(
                                transport,
                                &ClientResponse::Enrolled {
                                    member_id: member_id.clone(),
                                    roles,
                                },
                            )
                            .await
                            .is_err()
                            {
                                info!(
                                    pending_id = %pending_id,
                                    member_id = %member_id,
                                    "Joining machine disconnected while sending Enrolled; rolling back enrollment"
                                );
                                rollback_enrollment_and_disconnect_workers(
                                    auth_ctx,
                                    *rollback,
                                    "Enrollment confirmation declined by joining machine",
                                    registry,
                                    queue,
                                    scheduler_notify,
                                    waiters,
                                    #[cfg(feature = "dashboard")]
                                    broadcast_tx,
                                )
                                .await;
                                break;
                            }
                            let confirm_deadline =
                                tokio::time::Instant::now() + auth_ctx.enroll_confirm_timeout;
                            let confirm_res = tokio::time::timeout_at(
                                confirm_deadline,
                                transport.recv_msg::<ClientMessage>(),
                            )
                            .await;
                            match confirm_res {
                                Ok(Ok(Some(ClientMessage::Hello))) => {
                                    let members_store = auth_ctx.members.clone();
                                    let rollback_to_confirm = *rollback;
                                    let _ = run_member_store_blocking(move || {
                                        members_store
                                            .confirm_approved_enrollment(&rollback_to_confirm);
                                        Ok(())
                                    })
                                    .await;
                                    info!(
                                        pending_id = %pending_id,
                                        member_id = %member_id,
                                        "Joining machine confirmed v3 enrollment"
                                    );
                                    let _ = send_client_response(
                                        transport,
                                        &ClientResponse::Hello {
                                            min_compatible_version:
                                                rusty_grid_core::protocol::PROTOCOL_VERSION_MIN_COMPATIBLE,
                                        },
                                    )
                                    .await;
                                }
                                Err(_) => {
                                    info!(
                                        pending_id = %pending_id,
                                        member_id = %member_id,
                                        timeout_secs = auth_ctx.enroll_confirm_timeout.as_secs(),
                                        "Enrollment confirmation timed out after approval; rolling back enrollment"
                                    );
                                    rollback_enrollment_and_disconnect_workers(
                                        auth_ctx,
                                        *rollback,
                                        "Enrollment confirmation timed out",
                                        registry,
                                        queue,
                                        scheduler_notify,
                                        waiters,
                                        #[cfg(feature = "dashboard")]
                                        broadcast_tx,
                                    )
                                    .await;
                                    let _ = send_client_response(
                                        transport,
                                        &ClientResponse::EnrollRejected {
                                            message: "enrollment confirmation timed out".into(),
                                        },
                                    )
                                    .await;
                                }
                                Ok(_) => {
                                    info!(
                                        pending_id = %pending_id,
                                        member_id = %member_id,
                                        "Joining machine declined confirmation or disconnected after approval; rolling back enrollment"
                                    );
                                    rollback_enrollment_and_disconnect_workers(
                                        auth_ctx,
                                        *rollback,
                                        "Enrollment confirmation declined by joining machine",
                                        registry,
                                        queue,
                                        scheduler_notify,
                                        waiters,
                                        #[cfg(feature = "dashboard")]
                                        broadcast_tx,
                                    )
                                    .await;
                                    let _ = send_client_response(
                                        transport,
                                        &ClientResponse::EnrollRejected {
                                            message: "enrollment cancelled by joining machine".into(),
                                        },
                                    )
                                    .await;
                                }
                            }
                        }
                        Ok(members::PendingDecision::Rejected { message }) => {
                            info!(
                                pending_id = %pending_id,
                                "Admin rejected pending v3 enrollment"
                            );
                            let _ = send_client_response(
                                transport,
                                &ClientResponse::EnrollRejected { message },
                            )
                            .await;
                        }
                        Err(_) => {
                            let _ = send_client_response(
                                transport,
                                &ClientResponse::EnrollRejected {
                                    message: "enrollment request was closed".into(),
                                },
                            )
                            .await;
                        }
                    }
                    let _ = tokio::time::timeout(
                        REFUSAL_DRAIN_TIMEOUT,
                        transport.recv_raw_frame_max(
                            rusty_grid_core::cluster_auth::PRE_AUTH_MAX_FRAME,
                        ),
                    )
                    .await;
                    break;
                }
                _ = keepalive_interval.tick() => {
                    if send_client_response(
                        transport,
                        &ClientResponse::EnrollKeepalive,
                    )
                    .await
                    .is_err()
                    {
                        debug!(
                            pending_id = %pending_id,
                            "Pending enrollment peer disconnected during keepalive"
                        );
                        break;
                    }
                }
                peer_frame = transport.recv_raw_frame_max(rusty_grid_core::cluster_auth::PRE_AUTH_MAX_FRAME) => {
                    match peer_frame {
                        Ok(None) | Err(_) => {
                            debug!(
                                pending_id = %pending_id,
                                "Pending enrollment peer closed connection before approval; dropping pending entry"
                            );
                        }
                        Ok(Some(_)) => {
                            warn!(
                                pending_id = %pending_id,
                                "Pending enrollment peer sent unexpected frame while waiting for approval; dropping pending entry"
                            );
                        }
                    }
                    break;
                }
                _ = tokio::time::sleep_until(enroll_deadline) => {
                    info!(
                        pending_id = %pending_id,
                        "Pending enrollment timed out waiting for admin approval"
                    );
                    decision_rx.close();
                    auth_ctx.members.remove_pending(&pending_id);
                    if let Ok(members::PendingDecision::Approved { rollback, .. }) =
                        decision_rx.try_recv()
                    {
                        decision_consumed = true;
                        rollback_enrollment_and_disconnect_workers(
                            auth_ctx,
                            *rollback,
                            "Enrollment request timed out before confirmation",
                            registry,
                            queue,
                            scheduler_notify,
                            waiters,
                            #[cfg(feature = "dashboard")]
                            broadcast_tx,
                        )
                        .await;
                    }
                    let _ = send_client_response(
                        transport,
                        &ClientResponse::EnrollRejected {
                            message: "enrollment request timed out waiting for approval".into(),
                        },
                    )
                    .await;
                    let _ = tokio::time::timeout(
                        REFUSAL_DRAIN_TIMEOUT,
                        transport.recv_raw_frame_max(
                            rusty_grid_core::cluster_auth::PRE_AUTH_MAX_FRAME,
                        ),
                    )
                    .await;
                    break;
                }
            }
        }
    }
    if !decision_consumed {
        decision_rx.close();
    }
    auth_ctx.members.remove_pending(&pending_id);
    if !decision_consumed {
        if let Ok(members::PendingDecision::Approved { rollback, .. }) = decision_rx.try_recv() {
            info!(
                pending_id = %pending_id,
                member_id = %member_id,
                "Pending enrollment disconnected concurrently with admin approval; rolling back unconfirmed enrollment"
            );
            rollback_enrollment_and_disconnect_workers(
                auth_ctx,
                *rollback,
                "Enrollment confirmation declined by joining machine",
                registry,
                queue,
                scheduler_notify,
                waiters,
                #[cfg(feature = "dashboard")]
                broadcast_tx,
            )
            .await;
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_member_admin_message<S>(
    transport: &mut MessageTransport<S>,
    msg: ClientMessage,
    auth_ctx: &MasterAuthContext,
    v3_peer: Option<&AuthenticatedV3Peer>,
    member_conn: Option<ConnectionId>,
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &Arc<tokio::sync::Notify>,
    waiters: &WaiterMap,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<
        &tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>,
    >,
) -> GridResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let is_v3_admin = v3_peer.is_some_and(|peer| {
        peer.purpose == HandshakePurpose::Member
            && auth_ctx
                .members
                .get(&peer.public_key)
                .is_some_and(|m| m.has_admin_role())
    });
    let is_v2_authenticated = v3_peer.is_none() && transport.is_encrypted();
    if !is_v3_admin && !is_v2_authenticated {
        send_client_response(
            transport,
            &ClientResponse::Error {
                message: "Authentication failed: member and invite administration requires cluster secret (v2) or admin member (v3) authentication (run `oxideswarm secret generate` or start the master with `--auth members`)".into(),
            },
        )
        .await?;
        return Ok(());
    }
    match msg {
        ClientMessage::CreateInvite {
            roles,
            max_uses,
            expires_in_secs,
            require_approval,
        } => {
            if !auth_ctx.auth_mode.allows_members() {
                send_client_response(
                    transport,
                    &ClientResponse::Error {
                        message:
                            "cannot create an invite when the master is configured with --auth secret"
                                .into(),
                    },
                )
                .await?;
                return Ok(());
            }
            let auth_ctx_cloned = auth_ctx.clone();
            let create_res = run_member_store_blocking(move || {
                let pk = auth_ctx_cloned.ensure_v3_key_persisted()?;
                let rec = auth_ctx_cloned.members.create_invite(
                    roles,
                    max_uses,
                    expires_in_secs,
                    require_approval,
                )?;
                Ok((pk, rec))
            })
            .await;
            match create_res {
                Ok((master_key_pub, rec)) => {
                    info!(
                        invite_id = %rec.invite_id,
                        roles = ?rec.roles,
                        max_uses = rec.max_uses,
                        expires_at = rec.expires_at,
                        require_approval = rec.require_approval,
                        "Created cluster invite"
                    );
                    let resp = ClientResponse::InviteCreated {
                        invite_id: rec.invite_id,
                        invite_secret: rec.invite_secret,
                        roles: rec.roles,
                        max_uses: rec.max_uses,
                        expires_at: rec.expires_at,
                        require_approval: rec.require_approval,
                        master_key: master_key_pub,
                        p2p_ticket: auth_ctx.p2p_ticket.clone(),
                    };
                    send_client_response(transport, &resp).await?;
                }
                Err(e) => {
                    send_client_response(
                        transport,
                        &ClientResponse::Error {
                            message: e.to_string(),
                        },
                    )
                    .await?;
                }
            }
        }
        ClientMessage::ListInvites => {
            let invites = auth_ctx.members.list_invites();
            send_client_response(transport, &ClientResponse::InvitesList { invites }).await?;
        }
        ClientMessage::RevokeInvite { invite_id } => {
            let members_store = auth_ctx.members.clone();
            match run_member_store_blocking(move || members_store.revoke_invite(&invite_id)).await {
                Ok(resolved_id) => {
                    info!(invite_id = %resolved_id, "Revoked cluster invite");
                    send_client_response(
                        transport,
                        &ClientResponse::InviteRevoked {
                            invite_id: resolved_id,
                        },
                    )
                    .await?;
                }
                Err(e) => {
                    send_client_response(
                        transport,
                        &ClientResponse::Error {
                            message: e.to_string(),
                        },
                    )
                    .await?;
                }
            }
        }
        ClientMessage::ListMembers => {
            let members = auth_ctx.members.list_members();
            send_client_response(transport, &ClientResponse::MembersList { members }).await?;
        }
        ClientMessage::ListPendingEnrollments => {
            let pending = auth_ctx.members.list_pending();
            send_client_response(
                transport,
                &ClientResponse::PendingEnrollmentsList { pending },
            )
            .await?;
        }
        ClientMessage::ApproveEnrollment {
            pending_id,
            pairing_code,
        } => {
            let members_store = auth_ctx.members.clone();
            let pid_clone = pending_id.clone();
            match run_member_store_blocking(move || {
                members_store.approve_pending(&pid_clone, pairing_code.as_str())
            })
            .await
            {
                Ok(entry) => {
                    info!(
                        pending_id = %pending_id,
                        member_id = %entry.id,
                        name = %entry.name,
                        roles = ?entry.roles,
                        "Approved pending member enrollment"
                    );
                    send_client_response(
                        transport,
                        &ClientResponse::EnrollmentApproved {
                            pending_id,
                            member_id: entry.id,
                            name: entry.name,
                            roles: entry.roles,
                        },
                    )
                    .await?;
                }
                Err(e) => {
                    send_client_response(
                        transport,
                        &ClientResponse::Error {
                            message: e.to_string(),
                        },
                    )
                    .await?;
                }
            }
        }
        ClientMessage::RejectEnrollment { pending_id } => {
            match auth_ctx.members.reject_pending(&pending_id) {
                Ok(resolved_id) => {
                    info!(pending_id = %resolved_id, "Rejected pending member enrollment");
                    send_client_response(
                        transport,
                        &ClientResponse::EnrollmentRejected {
                            pending_id: resolved_id,
                        },
                    )
                    .await?;
                }
                Err(e) => {
                    send_client_response(
                        transport,
                        &ClientResponse::Error {
                            message: e.to_string(),
                        },
                    )
                    .await?;
                }
            }
        }
        ClientMessage::RevokeMember { target } => {
            let members_store = auth_ctx.members.clone();
            match run_member_store_blocking(move || members_store.revoke_member(&target)).await {
                Ok((entry, owned_workers)) => {
                    // Design 0051 D5. The member is already gone from the store, so its key is
                    // refused at its next handshake (step 4) and any connection that registers
                    // from now on refuses itself.
                    //
                    // Step 1: close every connection of the member, on every lane, at once. An
                    // admin revoking its own key keeps this connection until the answer is sent.
                    let own_conn = member_conn
                        .filter(|id| auth_ctx.member_connections.contains(&entry.id, *id));
                    let closed = auth_ctx
                        .member_connections
                        .close_member(&entry.id, own_conn);
                    let closed_worker_sessions =
                        closed.iter().filter(|lane| lane.is_worker()).count();

                    // Step 3: take the member's workers out of service and requeue their tasks at
                    // once, without the resume hold and without their checkpoints. The store's
                    // claims and the registry's owners together cover every worker id the member
                    // registered.
                    let mut worker_ids = owned_workers;
                    for wid in registry.release_workers_owned_by(&entry.id).await {
                        if !worker_ids.contains(&wid) {
                            worker_ids.push(wid);
                        }
                    }
                    for wid in &worker_ids {
                        registry.release_worker_owner(wid).await;
                    }
                    handle_workers_revoked_with_broadcast(
                        registry,
                        queue,
                        scheduler_notify,
                        waiters,
                        &worker_ids,
                        "Member revoked by admin",
                        #[cfg(feature = "dashboard")]
                        broadcast_tx,
                    )
                    .await;

                    let disconnected_sessions = closed.len() + usize::from(own_conn.is_some());
                    info!(
                        member_id = %entry.id,
                        name = %entry.name,
                        disconnected_sessions,
                        worker_sessions = closed_worker_sessions,
                        client_connections = disconnected_sessions - closed_worker_sessions,
                        worker_ids = worker_ids.len(),
                        "Revoked cluster member and closed its connections"
                    );
                    send_client_response(
                        transport,
                        &ClientResponse::MemberRevoked {
                            member_id: entry.id.clone(),
                            disconnected_sessions,
                        },
                    )
                    .await?;
                    if let Some(own) = own_conn {
                        auth_ctx.member_connections.close_connection(&entry.id, own);
                    }
                }
                Err(e) => {
                    send_client_response(
                        transport,
                        &ClientResponse::Error {
                            message: e.to_string(),
                        },
                    )
                    .await?;
                }
            }
        }
        ClientMessage::RenameMember { target, new_name } => {
            let members_store = auth_ctx.members.clone();
            match run_member_store_blocking(move || members_store.rename_member(&target, &new_name))
                .await
            {
                Ok(entry) => {
                    info!(
                        member_id = %entry.id,
                        name = %entry.name,
                        "Renamed cluster member"
                    );
                    send_client_response(
                        transport,
                        &ClientResponse::MemberRenamed {
                            member_id: entry.id,
                            name: entry.name,
                        },
                    )
                    .await?;
                }
                Err(e) => {
                    send_client_response(
                        transport,
                        &ClientResponse::Error {
                            message: e.to_string(),
                        },
                    )
                    .await?;
                }
            }
        }
        ClientMessage::ReleaseWorkerId { worker_id } => {
            match registry
                .release_non_live_worker_id(worker_id, &auth_ctx.members)
                .await
            {
                Ok(()) => {
                    info!(
                        worker_id = %worker_id,
                        "Released non-live worker_id claim"
                    );
                    send_client_response(
                        transport,
                        &ClientResponse::WorkerIdReleased { worker_id },
                    )
                    .await?;
                }
                Err(e) => {
                    warn!(
                        worker_id = %worker_id,
                        error = %e,
                        "Failed to release non-live worker_id claim"
                    );
                    send_client_response(
                        transport,
                        &ClientResponse::Error {
                            message: e.to_string(),
                        },
                    )
                    .await?;
                }
            }
        }
        _ => unreachable!("non-admin ClientMessage passed to handle_member_admin_message"),
    }
    Ok(())
}
