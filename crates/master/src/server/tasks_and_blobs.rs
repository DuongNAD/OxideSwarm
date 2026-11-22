//! Waiter resolution, blob transfer pipeline, result/progress handling, and disconnect failover.

use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};
use uuid::Uuid;

use rusty_grid_core::blob_transfer::{BlobReply, BlobRequest, MAX_IN_FLIGHT_REQUESTS};
use rusty_grid_core::capabilities::MobileLiveStatus;
use rusty_grid_core::protocol::ClientResponse;
use rusty_grid_core::task::{CheckpointData, Task, TaskId, TaskResult, TaskStatus};
use rusty_grid_core::{GridResult, MasterMessage, MessageTransport, WorkerMessage};

use crate::blob_store::BlobStore;
use crate::queue::{ResultDisposition, TaskQueue, TaskState};
use crate::registry::WorkerRegistry;

use super::handshakes::AbortOnDrop;
use super::WaiterMap;

/// Dashboard broadcast channel type, so signatures stay readable.
#[cfg(feature = "dashboard")]
pub(crate) type DashboardTx =
    tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>;

/// Removes waiters of `task_id` whose receiving side has gone away (timed-out or abandoned
/// waits), so they do not accumulate until the task finishes. Live waiters are kept.
pub(crate) async fn prune_closed_waiters(waiters: &WaiterMap, task_id: &TaskId) {
    let mut lock = waiters.write().await;
    if let Some(senders) = lock.get_mut(task_id) {
        senders.retain(|tx| !tx.is_closed());
        if senders.is_empty() {
            lock.remove(task_id);
        }
    }
}

/// The result to hand to clients of `task_id` if it has reached a terminal state, else `None`.
/// Falls back to a synthesised failure when a terminal task has no recorded result.
pub(super) async fn terminal_result(
    queue: &TaskQueue,
    task_id: &TaskId,
    fallback_worker: Option<Uuid>,
) -> Option<TaskResult> {
    let info = queue.get_task(task_id).await?;
    if !info.state.is_terminal() {
        return None;
    }
    Some(match queue.get_result(task_id).await {
        Some(result) => result,
        None => TaskResult::failure(
            info.assigned_worker_id
                .or(fallback_worker)
                .unwrap_or_else(Uuid::nil),
            *task_id,
            if info.state == TaskState::Cancelled {
                130
            } else {
                1
            },
            "",
            "",
            0,
            info.error_message
                .clone()
                .or_else(|| Some("Task terminated".into())),
        ),
    })
}

/// Wakes every client waiting on one of `task_ids` that has reached a terminal state (for example
/// a task that ran out of failovers). Tasks that are still alive keep their waiters.
pub(crate) async fn resolve_terminal_waiters(
    queue: &TaskQueue,
    waiters: &WaiterMap,
    task_ids: &[TaskId],
    fallback_worker: Option<Uuid>,
) {
    for task_id in task_ids {
        if let Some(result) = terminal_result(queue, task_id, fallback_worker).await {
            let mut lock = waiters.write().await;
            if let Some(senders) = lock.remove(task_id) {
                for tx in senders {
                    let _ = tx.send(result.clone());
                }
            }
        }
    }
}

/// Cancels a task, tells the worker executing it to stop, and wakes everybody waiting on it with
/// the cancelled result. Shared by the client protocol and `MasterHandle::cancel_task`.
///
/// Returns `Ok(true)` if the task was cancelled now, `Ok(false)` if it had already finished and
/// was left alone, and `TaskNotFound` for an unknown id.
pub(crate) async fn cancel_task_and_notify(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: &WaiterMap,
    task_id: TaskId,
    reason: &str,
) -> GridResult<bool> {
    let outcome = queue
        .cancel_task_detailed(&task_id, Some(reason.to_string()))
        .await?;
    if !outcome.newly_cancelled {
        return Ok(false);
    }

    if let Some(worker_id) = outcome.worker_to_notify {
        let msg = MasterMessage::CancelTask {
            task_id,
            reason: Some(reason.to_string()),
        };
        if let Err(e) = registry.send_to_worker(&worker_id, msg).await {
            debug!(task_id = %task_id, worker_id = %worker_id, error = %e, "Could not deliver CancelTask to worker");
        }
    }
    if let Some((spec_wid, spec_sid, spec_live)) = outcome.extra_worker_to_notify {
        let msg = MasterMessage::CancelTask {
            task_id,
            reason: Some(format!(
                "{reason}; task is no longer assigned to this worker"
            )),
        };
        if let Err(e) = registry
            .send_to_worker_session(&spec_wid, Some(spec_sid), msg)
            .await
        {
            debug!(
                task_id = %task_id,
                worker_id = %spec_wid,
                error = %e,
                "Could not deliver CancelTask to speculative worker"
            );
        }
        match spec_live {
            Some(live) => {
                registry
                    .release_dispatch_if_live(&spec_wid, spec_sid, &live)
                    .await;
            }
            None => registry.release_dispatch(&spec_wid, spec_sid).await,
        }
    }

    let result = outcome.result.unwrap_or_else(|| {
        TaskResult::failure(
            outcome.worker_to_notify.unwrap_or_else(Uuid::nil),
            task_id,
            130,
            "",
            "",
            0,
            Some(reason.to_string()),
        )
    });
    let mut lock = waiters.write().await;
    if let Some(senders) = lock.remove(&task_id) {
        for tx in senders {
            let _ = tx.send(result.clone());
        }
    }
    drop(lock);

    scheduler_notify.notify_one();
    Ok(true)
}

/// The reason a request that needs the blob store fails on a master that runs without one.
pub(super) const NO_BLOB_STORE: &str =
    "this master has no blob store, so it cannot move task input or \
     output files; start it with `oxideswarm master` (which keeps one under ~/.oxideswarm/blobs) \
     or `oxideswarm master --blob-dir DIR`";

/// How often the blob store sweeper releases the pins of finished tasks.
const BLOB_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// Admits a task that moves files through the blob store: every input blob (and every file of
/// every input tree or workspace source tree) must already be in the store, and they are pinned
/// there until the task finishes, so the bound cannot evict them before a worker fetched them.
/// Tasks without inputs, output patterns, or a workspace source tree pass untouched.
/// Returns whether any input or workspace tree manifest uses `oxideswarm-tree/2` (`TREE_V2`).
pub(crate) async fn admit_blob_task(
    blob_store: Option<&Arc<BlobStore>>,
    task: &Task,
) -> Result<bool, String> {
    let workspace_tree = task
        .workspace
        .as_ref()
        .and_then(|ws| match ws.source.as_ref()? {
            rusty_grid_core::task::WorkspaceSource::Tree {
                manifest,
                manifest_size,
            } => Some(rusty_grid_core::blob::TaskInput {
                path: format!("workspace:{}", ws.name),
                source: rusty_grid_core::blob::InputSource::Tree {
                    manifest: *manifest,
                    manifest_size: *manifest_size,
                },
            }),
        });
    if !task.uses_blobs() && workspace_tree.is_none() {
        return Ok(false);
    }
    let Some(store) = blob_store else {
        return Err(format!(
            "Task {} has input or output files, but {NO_BLOB_STORE}",
            task.id
        ));
    };
    task.validate()?;
    let mut inputs = task.inputs.clone();
    if let Some(ws_input) = workspace_tree {
        inputs.push(ws_input);
    }
    let (ids, needs_tree_v2) = store.resolve_inputs_with_features(&inputs).await?;
    if ids.is_empty() {
        return Ok(needs_tree_v2);
    }
    let gone = store.pin_task(task.id, ids);
    if let Some(id) = gone.first() {
        store.unpin_task(&task.id).await;
        return Err(format!(
            "input blob {id} was evicted from the master's blob store while the task was being \
             submitted; upload the inputs again (or raise `master --blob-store-mb`)"
        ));
    }
    Ok(needs_tree_v2)
}

/// Checks that a result's output files are in the store and pins them atomically if present,
/// returning either the pin guard or an error naming the first missing output.
fn pin_result_outputs(
    blob_store: Option<&Arc<BlobStore>>,
    result: &TaskResult,
) -> Result<Option<crate::blob_store::OutputPinGuard>, String> {
    if result.outputs.is_empty() {
        return Ok(None);
    }
    let Some(store) = blob_store else {
        return Err(format!(
            "the worker reported {} output files, but {NO_BLOB_STORE}",
            result.outputs.len()
        ));
    };
    let ids: Vec<_> = result.outputs.iter().map(|f| f.id).collect();
    match store.check_and_pin_outputs(&ids) {
        Ok(pin) => Ok(Some(pin)),
        Err(missing) => {
            let first = result
                .outputs
                .iter()
                .find(|f| missing.contains(&f.id))
                .expect("missing id comes from result.outputs");
            Err(format!(
                "output '{}' (blob {}) is not in the master's blob store ({} of {} outputs missing)",
                first.path,
                first.id,
                missing.len(),
                result.outputs.len()
            ))
        }
    }
}

/// Drops the unfinished uploads of one connection when the connection ends.
pub(super) struct UploadOwner {
    pub(super) store: Arc<BlobStore>,
    pub(super) owner: u64,
}

impl UploadOwner {
    pub(super) fn new(store: Arc<BlobStore>) -> Self {
        let owner = store.new_owner();
        Self { store, owner }
    }
}

impl Drop for UploadOwner {
    fn drop(&mut self) {
        self.store.abort_uploads(self.owner);
    }
}

/// The `BlobRequest`s of one client connection that are being handled. Up to
/// [`MAX_IN_FLIGHT_REQUESTS`] run at the same time (different blobs overlap their disk work),
/// and their replies leave in the order the requests arrived. A client that sends more than that
/// is held back: the connection stops reading until a reply went out, so the number of requests
/// held is bounded. Their size is not bounded by the chunk size: a frame may be up to the 64 MiB
/// frame limit until `append` rejects it, so the worst case is the limit times that.
#[derive(Default)]
pub(super) struct BlobPipeline {
    running: std::collections::VecDeque<tokio::task::JoinHandle<BlobReply>>,
}

impl BlobPipeline {
    pub(super) fn len(&self) -> usize {
        self.running.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.running.is_empty()
    }

    pub(super) fn is_full(&self) -> bool {
        self.len() >= MAX_IN_FLIGHT_REQUESTS
    }

    pub(super) fn push(&mut self, store: Arc<BlobStore>, owner: u64, request: BlobRequest) {
        self.running.push_back(tokio::spawn(
            async move { store.handle(owner, request).await },
        ));
    }

    pub(super) fn push_ready(&mut self, reply: BlobReply) {
        self.running.push_back(tokio::spawn(async move { reply }));
    }

    /// Waits for the oldest request's reply. Must not be called when the pipeline is empty.
    pub(super) async fn next_reply(&mut self) -> BlobReply {
        let Some(front) = self.running.front_mut() else {
            return std::future::pending().await;
        };
        let outcome = front.await;
        self.running.pop_front();
        outcome.unwrap_or_else(|e| BlobReply::Failed {
            message: format!("handling the blob request failed: {e}"),
        })
    }
}

impl Drop for BlobPipeline {
    fn drop(&mut self) {
        for handle in &self.running {
            handle.abort();
        }
    }
}

/// Answers one worker connection's `BlobRequest`s in arrival order, off its inbound loop. A
/// worker caps its requests in flight per session at [`MAX_IN_FLIGHT_REQUESTS`], smaller than
/// this queue (`MAX_IN_FLIGHT_REQUESTS * 2`), so a cooperative worker never fills the queue and
/// blocks its own heartbeats or results. If a peer ignores that limit or stops draining its data
/// lane, buffering for the session is bounded across this request channel, the 128-slot session
/// `outbound_tx` channel, the 64-slot `data_out_tx` channel, and `router_task`'s `pending_data`
/// cap (`MAX_PENDING_DATA_MESSAGES = 256`, `MAX_PENDING_DATA_BYTES = 64 MiB` in `session.rs`),
/// while `DATA_LANE_WRITE_TIMEOUT` (`60 s`) terminates a session whose data stream stalls.
pub(crate) struct BlobService {
    requests: mpsc::Sender<(u64, BlobRequest)>,
    _task: AbortOnDrop,
}

impl BlobService {
    pub(crate) fn spawn(store: Arc<BlobStore>, replies: mpsc::Sender<MasterMessage>) -> Self {
        let (requests, mut incoming) =
            mpsc::channel::<(u64, BlobRequest)>(MAX_IN_FLIGHT_REQUESTS * 2);
        let task = tokio::spawn(async move {
            let uploads = UploadOwner::new(Arc::clone(&store));
            while let Some((request_id, request)) = incoming.recv().await {
                let reply = store.handle(uploads.owner, request).await;
                if replies
                    .send(MasterMessage::BlobReply { request_id, reply })
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        Self {
            requests,
            _task: AbortOnDrop(task.abort_handle()),
        }
    }
}

/// Hands a worker's `BlobRequest` to its connection's [`BlobService`], or answers it with a
/// refusal on a master without a blob store.
pub(crate) async fn serve_worker_blob_request(
    service: Option<&BlobService>,
    replies: &mpsc::Sender<MasterMessage>,
    request_id: u64,
    request: BlobRequest,
) {
    match service {
        Some(service) => {
            let _ = service.requests.send((request_id, request)).await;
        }
        None => {
            let reply = BlobReply::Failed {
                message: NO_BLOB_STORE.to_string(),
            };
            let _ = replies
                .send(MasterMessage::BlobReply { request_id, reply })
                .await;
        }
    }
}

/// The `RegisterAck` message announcing the optional features of this master.
pub(crate) fn feature_announcement(
    blob_store: Option<&Arc<BlobStore>>,
    session_resume_hold_secs: u64,
) -> String {
    use rusty_grid_core::protocol::master_features;
    let mut features = master_features::SUPPORTED.to_vec();
    if blob_store.is_some() {
        features.push(master_features::BLOBS);
    }
    let session_resume_token;
    if session_resume_hold_secs > 0 {
        session_resume_token = master_features::session_resume_feature(session_resume_hold_secs);
        features.push(&session_resume_token);
    }
    master_features::announcement(&features)
}

/// Releases the input blob pins of tasks that finished without a result that released them
/// (failover out of retries, the reaper, a cancellation), releases terminal output pins once
/// their retention period has elapsed, and drops uploads that stalled.
pub(super) fn spawn_blob_store_sweeper(
    store: Arc<BlobStore>,
    queue: TaskQueue,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let sweep_interval = BLOB_SWEEP_INTERVAL
        .min(store.output_pin_retention())
        .max(Duration::from_millis(20));
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(sweep_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    store.sweep_stale_uploads();
                    store.sweep_expired_output_pins().await;
                    for (task_id, pinned_at) in store.pinned_tasks() {
                        let release = match queue.get_task(&task_id).await {
                            Some(info) => info.state.is_terminal(),
                            // Pinned just before it entered the queue, or gone long ago.
                            None => pinned_at.elapsed() > BLOB_SWEEP_INTERVAL,
                        };
                        if release {
                            store.unpin_task(&task_id).await;
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

/// Handles incoming `WorkerMessage::TaskResult` messages.
///
/// The sender is taken from `result.worker_id`; the connection handlers attribute results to the
/// authenticated session instead (see `handle_session_task_message`).
pub async fn handle_task_result(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: &WaiterMap,
    result: TaskResult,
) {
    let worker_id = result.worker_id;
    apply_task_result(
        registry,
        queue,
        scheduler_notify,
        waiters,
        worker_id,
        None,
        result,
        None,
        #[cfg(feature = "dashboard")]
        None,
    )
    .await;
}

/// Handles incoming `WorkerMessage::TaskResult` messages with optional dashboard telemetry broadcast.
#[cfg(feature = "dashboard")]
pub async fn handle_task_result_with_broadcast(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: &WaiterMap,
    result: TaskResult,
    broadcast_tx: Option<
        &tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>,
    >,
) {
    let worker_id = result.worker_id;
    apply_task_result(
        registry,
        queue,
        scheduler_notify,
        waiters,
        worker_id,
        None,
        result,
        None,
        broadcast_tx,
    )
    .await;
}

/// Applies one worker result: records it in the queue FSM on behalf of the sending connection
/// `(worker_id, session_id)`, then does the bookkeeping that follows from what the queue decided.
///
/// A result from a worker or session that the task is not currently assigned to (reaped, failed
/// over, superseded, or a peer that never had the task) changes nothing: no FSM transition, no
/// waiter wake-up, no capacity accounting.
#[allow(clippy::too_many_arguments)]
pub(super) async fn apply_task_result(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: &WaiterMap,
    worker_id: Uuid,
    session_id: Option<u64>,
    result: TaskResult,
    blob_store: Option<&Arc<BlobStore>>,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<&DashboardTx>,
) {
    apply_task_result_inner(
        registry,
        queue,
        scheduler_notify,
        waiters,
        worker_id,
        session_id,
        result,
        blob_store,
        false,
        #[cfg(feature = "dashboard")]
        broadcast_tx,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn apply_task_result_inner(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: &WaiterMap,
    worker_id: Uuid,
    session_id: Option<u64>,
    mut result: TaskResult,
    blob_store: Option<&Arc<BlobStore>>,
    spawn_waiter_ack: bool,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<&DashboardTx>,
) {
    let task_id = result.task_id;
    if result.worker_id != worker_id {
        warn!(
            worker_id = %worker_id,
            claimed = %result.worker_id,
            task_id = %task_id,
            "Result carries a worker_id that is not the sending connection; attributing it to the connection"
        );
        result.worker_id = worker_id;
    }

    info!(
        worker_id = %worker_id,
        task_id = %task_id,
        exit_code = result.exit_code,
        "Task execution result received"
    );

    // Output files a client cannot download would turn a success into a silent loss; check and
    // pin them atomically so no upload can evict them before the task outcome is recorded.
    let output_pin = match pin_result_outputs(blob_store, &result) {
        Ok(pin) => pin,
        Err(problem) => {
            warn!(task_id = %task_id, problem = %problem, "Result refers to output files the master does not hold");
            result.error = Some(match result.error.take() {
                Some(error) => format!("{error}; {problem}"),
                None => problem,
            });
            None
        }
    };

    // 1. Record outcome in TaskQueue FSM and enqueue its journal record without blocking the
    //    worker session loop on the 5 ms group-commit window.
    let (outcome, durable_ack) = match queue
        .record_result_from_enqueued(worker_id, session_id, result.clone())
        .await
    {
        Ok(pair) => pair,
        Err(e) => {
            warn!(task_id = %task_id, error = %e, "Failed to record task result in queue");
            return;
        }
    };

    if outcome.disposition == ResultDisposition::Stale {
        debug!(
            worker_id = %worker_id,
            task_id = %task_id,
            state = ?outcome.state,
            "Ignored result from a worker/session that does not own the task"
        );
        return;
    }

    #[cfg(feature = "dashboard")]
    if let Some(b_tx) = broadcast_tx {
        if b_tx.receiver_count() > 0 {
            if let Some(info) = queue.get_task(&task_id).await {
                let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(
                    info,
                ));
            }
            let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(
                queue.stats().await,
            ));
        }
    }

    // 2. Release the worker's slot immediately and keep its circuit breaker current
    if !outcome.adopted_from_hold {
        match (outcome.session_id, outcome.session_live.as_ref()) {
            (Some(sid), Some(live)) => {
                registry
                    .release_dispatch_if_live(&worker_id, sid, live)
                    .await;
            }
            (Some(sid), None) => {
                registry.release_dispatch(&worker_id, sid).await;
            }
            (None, _) => {
                let _ = registry.decrement_active_tasks(&worker_id).await;
            }
        }
    }
    if outcome.rejected {
        let _ = registry.record_rejection(&worker_id, session_id).await;
    } else if outcome.disposition != ResultDisposition::Late {
        registry.clear_rejections(&worker_id).await;
    }
    // The acknowledgement token hashes the whole result, `asset_bytes` included, and is sent
    // only when this master holds sessions for resume, so it is computed only then (BUG-27).
    let ack_token = (queue.session_resume_hold_secs() > 0)
        .then(|| rusty_grid_core::protocol::master_features::result_ack_token(&result));

    // 3. Notify awaiting clients, but only once the task is really finished: a requeued
    //    task (rejection, execution retry) will still produce its real result and keeps its
    //    input pins. When terminal, input pins are released and output pins stay held for
    //    `output_pin_retention` so the client can download them.
    if outcome.disposition == ResultDisposition::Terminal {
        if let Some((loser_wid, loser_sid, loser_live)) = outcome.cancel_sibling {
            let cancel_msg = MasterMessage::CancelTask {
                task_id,
                reason: Some(
                    "Speculative sibling completed; task is no longer assigned to this worker"
                        .to_string(),
                ),
            };
            // Sent from its own task: a wedged loser with a full outbound channel must not block
            // the winner's session loop (and its heartbeats) until the loser is reaped.
            let cancel_registry = registry.clone();
            tokio::spawn(async move {
                if let Err(e) = cancel_registry
                    .send_to_worker_session(&loser_wid, loser_sid, cancel_msg)
                    .await
                {
                    debug!(
                        task_id = %task_id,
                        worker_id = %loser_wid,
                        error = %e,
                        "Could not deliver CancelTask to losing sibling worker"
                    );
                }
            });
            match (loser_sid, loser_live) {
                (Some(sid), Some(live)) => {
                    registry
                        .release_dispatch_if_live(&loser_wid, sid, &live)
                        .await;
                }
                (Some(sid), None) => registry.release_dispatch(&loser_wid, sid).await,
                (None, _) => {
                    // Only reachable via sessionless test helpers (`schedule_task` / `pop_and_schedule`);
                    // production `WorkloadScheduler` and `adopt_session_tasks` always record both
                    // `session_id` and `session_live`.
                    let _ = registry.decrement_active_tasks(&loser_wid).await;
                }
            }
        }
        if !outcome.resumed_from_checkpoint {
            if let Some(sb) = registry.speed_book() {
                crate::speed::mutate_shared_speed_book(&sb, |book| {
                    book.record_result(worker_id, outcome.task_key, outcome.timeout_secs, &result);
                });
            }
        }
        if let Some(store) = blob_store {
            store.unpin_task(&task_id).await;
        }
        if let Some(pin) = output_pin {
            pin.commit_terminal(task_id).await;
        }
        if spawn_waiter_ack {
            if let Some(ack) = durable_ack {
                let waiters = Arc::clone(waiters);
                let ack_registry = registry.clone();
                let cold_queue = queue.clone();
                tokio::spawn(async move {
                    if let Err(err) = ack.await {
                        warn!(
                            task_id = %task_id.0,
                            error = %err,
                            "Failed to sync TaskTerminal journal record for task result"
                        );
                    }
                    if let Some(ack_token) = ack_token {
                        ack_registry
                            .send_result_ack(&worker_id, session_id, task_id, ack_token)
                            .await;
                    }
                    {
                        let mut lock = waiters.write().await;
                        if let Some(senders) = lock.remove(&task_id) {
                            for tx in senders {
                                let _ = tx.send(result.clone());
                            }
                        }
                    }
                    cold_queue.spawn_cold_maintenance().await;
                });
            } else {
                if let Some(ack_token) = ack_token {
                    registry
                        .send_result_ack(&worker_id, session_id, task_id, ack_token)
                        .await;
                }
                {
                    let mut lock = waiters.write().await;
                    if let Some(senders) = lock.remove(&task_id) {
                        for tx in senders {
                            let _ = tx.send(result.clone());
                        }
                    }
                }
                queue.spawn_cold_maintenance().await;
            }
        } else {
            if let Some(ack) = durable_ack {
                if let Err(err) = ack.await {
                    warn!(
                        task_id = %task_id.0,
                        error = %err,
                        "Failed to sync TaskTerminal journal record for task result"
                    );
                }
            }
            if let Some(ack_token) = ack_token {
                registry
                    .send_result_ack(&worker_id, session_id, task_id, ack_token)
                    .await;
            }
            {
                let mut lock = waiters.write().await;
                if let Some(senders) = lock.remove(&task_id) {
                    for tx in senders {
                        let _ = tx.send(result.clone());
                    }
                }
            }
            queue.spawn_cold_maintenance().await;
        }
    } else if let Some(ack_token) = ack_token {
        if let Some(ack) = durable_ack {
            if let Err(err) = ack.await {
                warn!(
                    task_id = %task_id.0,
                    error = %err,
                    "Failed to sync journal record for non-terminal task result"
                );
            }
        }
        registry
            .send_result_ack(&worker_id, session_id, task_id, ack_token)
            .await;
    }

    // 4. Wake scheduler loop to assign pending tasks
    scheduler_notify.notify_one();
}

/// Fails only the task when an outbound `MasterMessage::AssignTask` or
/// `AssignTaskWithCheckpoint` exceeds `MAX_FRAME_SIZE`, keeping the worker session alive so
/// unrelated tasks on the worker can continue.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_oversized_master_message(
    msg: MasterMessage,
    size: usize,
    max: usize,
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: &WaiterMap,
    worker_id: Uuid,
    session_id: u64,
    blob_store: Option<&Arc<BlobStore>>,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<&DashboardTx>,
) {
    let task_id = match msg {
        MasterMessage::AssignTask { task }
        | MasterMessage::AssignTaskWithCheckpoint { task, .. } => task.id,
        other => {
            warn!(
                worker_id = %worker_id,
                session_id = session_id,
                variant = %other.variant_name(),
                size = size,
                max = max,
                "Outbound master message exceeds frame limit; dropping frame without closing session"
            );
            return;
        }
    };

    let reason = format!(
        "Task assignment frame ({size} bytes) exceeds maximum protocol frame limit ({max} bytes)"
    );
    warn!(
        worker_id = %worker_id,
        session_id = session_id,
        task_id = %task_id,
        size = size,
        max = max,
        "Refusing oversized task assignment without terminating worker session"
    );
    let result = TaskResult::failure(
        worker_id,
        task_id,
        1,
        rusty_grid_core::task::Bytes::new(),
        format!("[rusty_grid: {reason}]\n").into_bytes(),
        0,
        Some(reason),
    );

    let (disposition, session_live) = match queue
        .fail_unsendable_assignment(worker_id, Some(session_id), result.clone())
        .await
    {
        Ok(pair) => pair,
        Err(e) => {
            warn!(task_id = %task_id, error = %e, "Failed to record oversized assignment failure");
            return;
        }
    };

    if disposition == ResultDisposition::Stale {
        return;
    }

    #[cfg(feature = "dashboard")]
    if let Some(b_tx) = broadcast_tx {
        if b_tx.receiver_count() > 0 {
            if let Some(info) = queue.get_task(&task_id).await {
                let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(
                    info,
                ));
            }
            let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(
                queue.stats().await,
            ));
        }
    }

    match session_live {
        Some(live) => {
            registry
                .release_dispatch_if_live(&worker_id, session_id, &live)
                .await;
        }
        None => registry.release_dispatch(&worker_id, session_id).await,
    }

    if disposition == ResultDisposition::Terminal {
        if let Some(store) = blob_store {
            store.unpin_task(&task_id).await;
        }
        {
            let mut lock = waiters.write().await;
            if let Some(senders) = lock.remove(&task_id) {
                for tx in senders {
                    let _ = tx.send(result.clone());
                }
            }
        }
        queue.spawn_cold_maintenance().await;
    }

    scheduler_notify.notify_one();
}

/// Sends `resp` to a client, converting `ProtocolError::FrameTooLarge` into a
/// `ClientResponse::Error` frame instead of dropping the connection with no explanation.
pub(super) async fn send_client_response<S: AsyncRead + AsyncWrite + Unpin + Send>(
    transport: &mut MessageTransport<S>,
    resp: &ClientResponse,
) -> Result<(), rusty_grid_core::protocol::ProtocolError> {
    // The reply is encoded for the version the client stamped on its last frame (design 0083 D2).
    if let Some(version) = transport.last_peer_version() {
        transport.set_peer_version(version);
    }
    match transport.send_msg(resp).await {
        Ok(()) => Ok(()),
        Err(rusty_grid_core::protocol::ProtocolError::FrameTooLarge { size, max }) => {
            let message = match resp {
                ClientResponse::TaskCompleted { task_id, .. } => format!(
                    "Task {task_id} result frame ({size} bytes) exceeds maximum protocol frame limit ({max} bytes); write large outputs to files and retrieve them with `--output`"
                ),
                _ => format!(
                    "Master response frame ({size} bytes) exceeds maximum protocol frame limit ({max} bytes)"
                ),
            };
            warn!(
                size = size,
                max = max,
                "Client response exceeds frame limit; returning ClientResponse::Error"
            );
            transport.send_msg(&ClientResponse::Error { message }).await
        }
        Err(e) => Err(e),
    }
}

/// Handles incoming `WorkerMessage::TaskProgress` messages.
pub async fn handle_task_progress(
    queue: &TaskQueue,
    worker_id: Uuid,
    task_id: TaskId,
    status: TaskStatus,
) {
    apply_task_progress(
        queue,
        worker_id,
        None,
        task_id,
        status,
        #[cfg(feature = "dashboard")]
        None,
    )
    .await;
}

/// Handles incoming `WorkerMessage::TaskProgress` messages with optional dashboard telemetry broadcast.
#[cfg(feature = "dashboard")]
pub async fn handle_task_progress_with_broadcast(
    queue: &TaskQueue,
    worker_id: Uuid,
    task_id: TaskId,
    status: TaskStatus,
    broadcast_tx: Option<
        &tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>,
    >,
) {
    apply_task_progress(queue, worker_id, None, task_id, status, broadcast_tx).await;
}

/// Applies a progress report from connection `(worker_id, session_id)`. Only the worker the task
/// is assigned to can move it to `Running`; anything else is ignored.
async fn apply_task_progress(
    queue: &TaskQueue,
    worker_id: Uuid,
    session_id: Option<u64>,
    task_id: TaskId,
    status: TaskStatus,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<&DashboardTx>,
) {
    debug!(worker_id = %worker_id, task_id = %task_id, ?status, "Task progress update");
    if status != TaskStatus::Running {
        return;
    }
    if let Err(e) = queue
        .mark_running_from(worker_id, session_id, &task_id)
        .await
    {
        debug!(worker_id = %worker_id, task_id = %task_id, error = %e, "Ignoring progress update");
        return;
    }
    #[cfg(feature = "dashboard")]
    if let Some(b_tx) = broadcast_tx {
        if b_tx.receiver_count() > 0 {
            if let Some(info) = queue.get_task(&task_id).await {
                let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(
                    info,
                ));
            }
            let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(
                queue.stats().await,
            ));
        }
    }
}

/// Applies a `WorkerMessage::MobileTelemetry` report received on the authenticated connection
/// `(worker_id, session_id)` to the registry, so battery / thermal gating uses the live value.
///
/// As with every other message, the identity comes from the connection: a report naming another
/// worker is logged and applied to the sender only. A change wakes the scheduler, because a phone
/// that just recovered (plugged in, cooled down) may now take queued heavy work.
pub(crate) async fn apply_mobile_telemetry(
    registry: &WorkerRegistry,
    scheduler_notify: &Arc<tokio::sync::Notify>,
    worker_id: Uuid,
    session_id: u64,
    claimed: Uuid,
    status: MobileLiveStatus,
) {
    if claimed != worker_id {
        warn!(worker_id = %worker_id, claimed = %claimed, "Mobile telemetry names a different worker than its connection");
    }
    let bat_pct = status.battery_pct;
    let is_chg = status.is_charging;
    let th_thr = status.thermal_throttled;
    match registry
        .update_mobile_telemetry(&worker_id, Some(session_id), status)
        .await
    {
        Ok(true) => {
            debug!(
                worker_id = %worker_id,
                battery_pct = ?bat_pct,
                is_charging = ?is_chg,
                thermal_throttled = th_thr,
                "Worker mobile telemetry updated"
            );
            scheduler_notify.notify_one();
        }
        Ok(false) => {}
        Err(e) => {
            debug!(worker_id = %worker_id, error = %e, "Mobile telemetry for an unknown worker ignored")
        }
    }
}

/// Processes a task-related message (`TaskProgress`, `TaskResult`, `Checkpoint`) received on the
/// authenticated connection `(worker_id, session_id)`.
///
/// The identity comes from the connection. A `worker_id` inside the message body is never trusted:
/// otherwise any registered worker could complete, fail or poison a task it does not own.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_session_task_message(
    msg: WorkerMessage,
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &Arc<tokio::sync::Notify>,
    waiters: &WaiterMap,
    worker_id: Uuid,
    session_id: u64,
    blob_store: Option<&Arc<BlobStore>>,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<&DashboardTx>,
) {
    match msg {
        WorkerMessage::TaskProgress {
            worker_id: claimed,
            task_id,
            status,
        } => {
            if claimed != worker_id {
                warn!(worker_id = %worker_id, claimed = %claimed, task_id = %task_id, "Progress report names a different worker than its connection");
            }
            apply_task_progress(
                queue,
                worker_id,
                Some(session_id),
                task_id,
                status,
                #[cfg(feature = "dashboard")]
                broadcast_tx,
            )
            .await;
        }
        WorkerMessage::TaskResult {
            worker_id: claimed,
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
        } => {
            if claimed != worker_id {
                warn!(worker_id = %worker_id, claimed = %claimed, task_id = %task_id, "Result names a different worker than its connection");
            }
            let result = TaskResult {
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
            };
            // Once a result starts being applied it must finish, even if this connection is
            // aborted meanwhile (reaper, or the worker re-registering): the queue transition and
            // the waiter wake-up are one logical step, and cutting them apart would leave a
            // client waiting forever on a task that is already finished.
            let registry = registry.clone();
            let queue = queue.clone();
            let scheduler_notify = Arc::clone(scheduler_notify);
            let waiters = Arc::clone(waiters);
            let blob_store = blob_store.cloned();
            #[cfg(feature = "dashboard")]
            let broadcast_tx = broadcast_tx.cloned();
            let _ = tokio::spawn(async move {
                apply_task_result_inner(
                    &registry,
                    &queue,
                    &scheduler_notify,
                    &waiters,
                    worker_id,
                    Some(session_id),
                    result,
                    blob_store.as_ref(),
                    true,
                    #[cfg(feature = "dashboard")]
                    broadcast_tx.as_ref(),
                )
                .await;
            })
            .await;
        }
        WorkerMessage::Checkpoint {
            task_id,
            sequence,
            delta_state,
        } => {
            let checkpoint = CheckpointData::new(sequence, delta_state.into_bytes());
            match queue
                .record_checkpoint_from(worker_id, Some(session_id), task_id, checkpoint)
                .await
            {
                Ok(true) => {
                    debug!(worker_id = %worker_id, task_id = %task_id, sequence = sequence, "Recorded mid-task checkpoint");
                }
                Ok(false) => {
                    debug!(worker_id = %worker_id, task_id = %task_id, sequence = sequence, "Dropped checkpoint (stale, foreign or not newer)");
                }
                Err(e) => {
                    warn!(worker_id = %worker_id, task_id = %task_id, error = %e, "Failed to record mid-task checkpoint");
                }
            }
        }
        other => {
            warn!(worker_id = %worker_id, message = %other.variant_name(), "Unexpected message routed as task message");
        }
    }
}

/// Handles fast-path worker disconnection (TCP EOF, socket error, or graceful Disconnecting message).
pub async fn handle_worker_disconnect(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    worker_id: &Uuid,
    session_id: Option<u64>,
    reason: &str,
    immediate_reschedule: bool,
) {
    handle_worker_disconnect_internal(
        registry,
        queue,
        scheduler_notify,
        None,
        worker_id,
        session_id,
        reason,
        immediate_reschedule,
    )
    .await;
}

/// Handles worker disconnection and notifies active waiters for any tasks that reached terminal state.
#[allow(clippy::too_many_arguments)]
pub async fn handle_worker_disconnect_with_waiters(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: &WaiterMap,
    worker_id: &Uuid,
    session_id: Option<u64>,
    reason: &str,
    immediate_reschedule: bool,
) {
    handle_worker_disconnect_internal(
        registry,
        queue,
        scheduler_notify,
        Some(waiters),
        worker_id,
        session_id,
        reason,
        immediate_reschedule,
    )
    .await;
}

/// Disconnect worker and notify broadcast subscribers
#[allow(clippy::too_many_arguments)]
pub async fn handle_worker_disconnect_with_broadcast(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: Option<&WaiterMap>,
    worker_id: &Uuid,
    session_id: Option<u64>,
    reason: &str,
    immediate_reschedule: bool,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<
        &tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>,
    >,
) -> bool {
    let unregistered = registry
        .unregister(worker_id, session_id)
        .await
        .unwrap_or(false);
    if !unregistered {
        debug!(worker_id = %worker_id, "Worker unregister skipped (stale session or already handled)");
        return false;
    }
    if let Some(sid) = session_id {
        queue.set_worker_session_resume_connected(*worker_id, sid, false);
    }

    warn!(
        worker_id = %worker_id,
        reason = %reason,
        immediate = immediate_reschedule,
        "Worker disconnected; executing failover sweep"
    );

    #[cfg(feature = "dashboard")]
    if let Some(b_tx) = broadcast_tx {
        if b_tx.receiver_count() > 0 {
            let _ = b_tx.send(
                crate::dashboard::dto::DashboardStreamMessage::WorkerDisconnected {
                    worker_id: *worker_id,
                    reason: reason.to_string(),
                },
            );
        }
    }

    failover_session_tasks(
        queue,
        scheduler_notify,
        waiters,
        worker_id,
        session_id,
        reason,
        immediate_reschedule,
        #[cfg(feature = "dashboard")]
        broadcast_tx,
    )
    .await;
    true
}

/// Unregisters a worker session that announced a graceful drain (`WorkerMessage::Disconnecting`),
/// returns its in-flight tasks to `Queued` without charging `retry_count` or `failover_count`
/// while under `max_evacuations` (or takes the charged failover path past that cap), resolves
/// terminal waiters if any task reached `Failed`, notifies dashboard subscribers, and pokes the
/// scheduler (`MOB-3`).
#[allow(clippy::too_many_arguments)]
pub async fn handle_worker_evacuate_with_broadcast(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: Option<&WaiterMap>,
    worker_id: &Uuid,
    session_id: Option<u64>,
    reason: &str,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<
        &tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>,
    >,
) {
    let unregistered = registry
        .unregister(worker_id, session_id)
        .await
        .unwrap_or(false);
    if !unregistered {
        debug!(worker_id = %worker_id, "Worker drain unregister skipped (stale session or already handled)");
        return;
    }
    if let Some(sid) = session_id {
        queue.set_worker_session_resume_connected(*worker_id, sid, false);
    }

    info!(
        worker_id = %worker_id,
        reason = %reason,
        "Worker announced drain; evacuating in-flight tasks"
    );

    #[cfg(feature = "dashboard")]
    if let Some(b_tx) = broadcast_tx {
        if b_tx.receiver_count() > 0 {
            let _ = b_tx.send(
                crate::dashboard::dto::DashboardStreamMessage::WorkerDisconnected {
                    worker_id: *worker_id,
                    reason: reason.to_string(),
                },
            );
        }
    }

    let affected_tasks = queue
        .handle_session_evacuated(worker_id, session_id, reason)
        .await;
    if affected_tasks.is_empty() {
        return;
    }

    info!(
        worker_id = %worker_id,
        count = affected_tasks.len(),
        tasks = ?affected_tasks,
        "Processed evacuated tasks from draining worker"
    );

    #[cfg(feature = "dashboard")]
    if let Some(b_tx) = broadcast_tx {
        if b_tx.receiver_count() > 0 {
            for task_id in &affected_tasks {
                if let Some(info) = queue.get_task(task_id).await {
                    let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(
                        info,
                    ));
                }
            }
            let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(
                queue.stats().await,
            ));
        }
    }

    if let Some(waiters) = waiters {
        resolve_terminal_waiters(queue, waiters, &affected_tasks, Some(*worker_id)).await;
    }

    scheduler_notify.notify_one();
}

/// Fails over the in-flight tasks of one worker session: re-enqueues those with budget left,
/// fails the rest, wakes the clients of tasks that became terminal and pokes the scheduler.
/// With `session_id = Some(s)` only tasks dispatched on that session are touched.
#[allow(clippy::too_many_arguments)]
async fn failover_session_tasks(
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: Option<&WaiterMap>,
    worker_id: &Uuid,
    session_id: Option<u64>,
    reason: &str,
    immediate_reschedule: bool,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<&DashboardTx>,
) {
    let affected_tasks = queue
        .handle_session_disconnected(worker_id, session_id, reason, immediate_reschedule)
        .await;
    publish_failover(
        queue,
        scheduler_notify,
        waiters,
        worker_id,
        &affected_tasks,
        #[cfg(feature = "dashboard")]
        broadcast_tx,
    )
    .await;
}

/// Takes the workers of a member that can never come back (revoked, or an enrollment rolled
/// back) out of service (design 0051 D5, revocation step 3).
///
/// Unregisters each worker (aborting its session handler if one is still live), then takes back
/// every task they ran or were held for in one queue sweep: no session-resume hold, since these
/// workers cannot resume them, and without the checkpoints those tasks carry
/// (`TaskQueue::handle_workers_revoked`, which compacts the store once). Returns the number of
/// workers that had a live session.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_workers_revoked_with_broadcast(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: &WaiterMap,
    worker_ids: &[Uuid],
    reason: &str,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<&DashboardTx>,
) -> usize {
    let mut live = 0;
    for worker_id in worker_ids {
        if !registry.unregister(worker_id, None).await.unwrap_or(false) {
            continue;
        }
        live += 1;
        warn!(
            worker_id = %worker_id,
            reason = %reason,
            "Worker of a revoked member disconnected; requeuing its tasks without the resume hold"
        );
        #[cfg(feature = "dashboard")]
        if let Some(b_tx) = broadcast_tx {
            if b_tx.receiver_count() > 0 {
                let _ = b_tx.send(
                    crate::dashboard::dto::DashboardStreamMessage::WorkerDisconnected {
                        worker_id: *worker_id,
                        reason: reason.to_string(),
                    },
                );
            }
        }
    }
    for (worker_id, affected_tasks) in queue.handle_workers_revoked(worker_ids, reason).await {
        publish_failover(
            queue,
            scheduler_notify,
            Some(waiters),
            &worker_id,
            &affected_tasks,
            #[cfg(feature = "dashboard")]
            broadcast_tx,
        )
        .await;
    }
    live
}

/// Reports the tasks a failover sweep took back: dashboard updates, waiters of the tasks that
/// became terminal, and a scheduler wake-up.
async fn publish_failover(
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: Option<&WaiterMap>,
    worker_id: &Uuid,
    affected_tasks: &[TaskId],
    #[cfg(feature = "dashboard")] broadcast_tx: Option<&DashboardTx>,
) {
    if affected_tasks.is_empty() {
        return;
    }

    info!(
        worker_id = %worker_id,
        count = affected_tasks.len(),
        tasks = ?affected_tasks,
        "Re-enqueued orphaned tasks for reassignment"
    );

    #[cfg(feature = "dashboard")]
    if let Some(b_tx) = broadcast_tx {
        if b_tx.receiver_count() > 0 {
            for task_id in affected_tasks {
                if let Some(info) = queue.get_task(task_id).await {
                    let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(
                        info,
                    ));
                }
            }
            let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(
                queue.stats().await,
            ));
        }
    }

    // Resolve waiters for any tasks that reached terminal state (e.g. retries exhausted)
    if let Some(waiters) = waiters {
        resolve_terminal_waiters(queue, waiters, affected_tasks, Some(*worker_id)).await;
    }

    scheduler_notify.notify_one();
}

/// Fails over the tasks `old_session` of `worker_id` still holds, right after the worker id
/// registered again as `new_session`.
///
/// The caller is the new session's connection handler, and the registry already holds its abort
/// handle: a third registration of the same worker id aborts it at its first contended await. The
/// sweep therefore runs in a task of its own that the caller merely awaits (as for task results),
/// so it finishes even when the handler is cancelled meanwhile. If it were cancelled before it
/// ran, the next sweep of this worker still recovers the tasks, because a sweep of session `s`
/// takes every older session along (see `TaskQueue::handle_session_disconnected`).
pub(crate) async fn failover_superseded_session(
    queue: &TaskQueue,
    scheduler_notify: &Arc<tokio::sync::Notify>,
    waiters: &WaiterMap,
    worker_id: Uuid,
    old_session: u64,
    new_session: u64,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<&DashboardTx>,
) {
    warn!(worker_id = %worker_id, old_session = old_session, new_session = new_session, "Worker re-registered while its previous session was live; failing over the superseded session");
    let queue = queue.clone();
    let scheduler_notify = Arc::clone(scheduler_notify);
    let waiters = Arc::clone(waiters);
    #[cfg(feature = "dashboard")]
    let broadcast_tx = broadcast_tx.cloned();
    let _ = tokio::spawn(async move {
        failover_session_tasks(
            &queue,
            &scheduler_notify,
            Some(&waiters),
            &worker_id,
            Some(old_session),
            "Superseded by a new session of the same worker",
            true,
            #[cfg(feature = "dashboard")]
            broadcast_tx.as_ref(),
        )
        .await;
    })
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn handle_worker_disconnect_internal(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    scheduler_notify: &tokio::sync::Notify,
    waiters: Option<&WaiterMap>,
    worker_id: &Uuid,
    session_id: Option<u64>,
    reason: &str,
    immediate_reschedule: bool,
) {
    handle_worker_disconnect_with_broadcast(
        registry,
        queue,
        scheduler_notify,
        waiters,
        worker_id,
        session_id,
        reason,
        immediate_reschedule,
        #[cfg(feature = "dashboard")]
        None,
    )
    .await;
}
