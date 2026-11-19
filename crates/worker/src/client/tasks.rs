//! Worker task execution, checkpointing, cancellation, and resume outbox handling.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use rusty_grid_core::contributor_control_refusal;
use rusty_grid_core::protocol::{master_features, ProtocolCapabilities, WorkerMessage};
use rusty_grid_core::task::{CheckpointData, Task, TaskId, TaskResult, TaskStatus};

use crate::blob_client::{fetch_task_inputs, upload_task_outputs};
use crate::heartbeat::{current_epoch_secs, HeartbeatTracker};
use crate::runner::{TaskRunner, EXIT_CODE_CANCELLED, EXIT_CODE_GENERAL_ERROR};

use super::{ActiveTaskTable, CancelSignal, TaskExecutionHandle, WorkerClient, TASK_DRAIN_TIMEOUT};

async fn receive_cancel_signal(slot: &mut Option<oneshot::Receiver<CancelSignal>>) -> CancelSignal {
    let Some(receiver) = slot.as_mut() else {
        return std::future::pending().await;
    };
    let outcome = receiver.await;
    *slot = None;
    match outcome {
        Ok(sig) => sig,
        Err(_) => std::future::pending().await,
    }
}

fn resolve_current_tx(
    session_data_tx: &std::sync::RwLock<Option<mpsc::Sender<WorkerMessage>>>,
    fallback_tx: Option<&mpsc::Sender<WorkerMessage>>,
) -> Option<mpsc::Sender<WorkerMessage>> {
    if let Some(tx) = session_data_tx
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
    {
        if !tx.is_closed() {
            return Some(tx);
        }
    }
    fallback_tx.filter(|tx| !tx.is_closed()).cloned()
}

fn cancelled_task_result(
    worker_id: uuid::Uuid,
    task_id: TaskId,
    signal: CancelSignal,
) -> TaskResult {
    match signal {
        CancelSignal::MasterCancel(reason) => TaskResult::failure(
            worker_id,
            task_id,
            EXIT_CODE_CANCELLED,
            "",
            "",
            0,
            Some(format!("Cancelled: {reason}")),
        ),
        CancelSignal::ContributorPause(reason) => {
            let msg = contributor_control_refusal(&reason);
            TaskResult::failure(worker_id, task_id, 1, "", msg.clone(), 0, Some(msg))
        }
    }
}

/// Safety margin subtracted from the Master's advertised `session-resume=<secs>` hold so the
/// worker stops or reconnects before the Master's hold timer expires (design 0046 D2.4).
pub const RESUME_SAFETY_MARGIN: Duration = Duration::from_secs(3);

/// Computes the worker's effective session-resume grace window (`design 0046 D4.3`):
/// `min(max(0, hold - dead_peer_timeout - 3s), grace_cap)`, or `Duration::ZERO` when `hold` is
/// absent, `<= dead_peer_timeout + 3s`, or `grace_cap == Some(Duration::ZERO)`.
pub fn compute_effective_resume_grace(
    hold: Option<Duration>,
    dead_peer_timeout: Duration,
    grace_cap: Option<Duration>,
) -> Duration {
    let Some(hold) = hold else {
        return Duration::ZERO;
    };
    let max_safe_grace = hold
        .saturating_sub(dead_peer_timeout)
        .saturating_sub(RESUME_SAFETY_MARGIN);
    let effective = match grace_cap {
        Some(cap) => cap.min(max_safe_grace),
        None => max_safe_grace,
    };
    if effective < Duration::from_secs(1) {
        Duration::ZERO
    } else {
        effective
    }
}

/// Maximum total bytes of task results buffered in the session-resume outbox (design 0046 D4).
pub const MAX_RESUME_OUTBOX_BYTES: usize = 32 * 1024 * 1024;

/// Pause before the one repeat of a failed sandbox base re-check: a base that is being removed
/// (seen on Windows) is usually back, re-created by the re-check itself, a moment later.
const BASE_RECHECK_RETRY_DELAY: Duration = Duration::from_millis(250);

/// The per-task sandbox base re-check, repeated once after a short pause when it fails.
async fn recheck_base_once_more(
    config: &crate::sandbox::SandboxConfig,
) -> Result<(), crate::sandbox::SandboxError> {
    match config.ensure_private_root_recheck().await {
        Ok(()) => Ok(()),
        Err(_) => {
            tokio::time::sleep(BASE_RECHECK_RETRY_DELAY).await;
            config.ensure_private_root_recheck().await
        }
    }
}

fn buffered_result_bytes(result: &TaskResult) -> usize {
    256usize
        .saturating_add(result.stdout.0.len())
        .saturating_add(result.stderr.0.len())
        .saturating_add(result.error.as_ref().map_or(0, String::len))
        .saturating_add(result.device_name.as_ref().map_or(0, String::len))
        .saturating_add(result.asset_bytes.as_ref().map_or(0, |b| b.0.len()))
        .saturating_add(
            result
                .artifacts
                .iter()
                .map(|a| a.path.len().saturating_add(a.data.len()))
                .sum::<usize>(),
        )
        .saturating_add(
            result
                .outputs
                .iter()
                .map(|o| o.path.len().saturating_add(64))
                .sum::<usize>(),
        )
}

/// The acknowledgement token of `result` when the session keeps finished results for resume
/// (`resume_enabled`), else `None`. The token (`token`: `master_features::result_ack_token`
/// outside tests) hashes the whole result, `asset_bytes` included, and only keys the resume
/// outbox, so a session without a resume window never computes it (BUG-27).
pub(super) fn resume_ack_token(
    resume_enabled: bool,
    result: &TaskResult,
    token: impl FnOnce(&TaskResult) -> u64,
) -> Option<u64> {
    resume_enabled.then(|| token(result))
}

/// Completed task result buffered in the worker's session-resume outbox until acknowledged by the
/// Master (`MasterMessage::CancelTask { reason: Some("oxs:result-ack:<token>") }`) or until the
/// resume window expires. Holds `output_pin` (`Arc<CachePin>`) so output blobs stay pinned in
/// `BlobCache` across transient session drops.
#[derive(Clone)]
pub(super) struct BufferedTaskResult {
    pub(super) task: Task,
    pub(super) result: TaskResult,
    pub(super) ack_token: u64,
    pub(super) byte_size: usize,
    pub(super) _output_pin: Option<Arc<crate::blob_cache::CachePin>>,
    pub(super) used_blobs: bool,
}

impl BufferedTaskResult {
    /// `ack_token` is `master_features::result_ack_token(&result)`, computed by the caller
    /// (once, and not under the outbox lock).
    pub(super) fn new(
        task: Task,
        result: TaskResult,
        ack_token: u64,
        output_pin: Option<Arc<crate::blob_cache::CachePin>>,
        used_blobs: bool,
    ) -> Self {
        let byte_size = buffered_result_bytes(&result);
        Self {
            task,
            result,
            ack_token,
            byte_size,
            _output_pin: output_pin,
            used_blobs,
        }
    }
}

/// Bounded per-worker outbox for checkpoints and task results retained across transient session
/// drops (design 0046 D2.5 / D4).
pub(super) struct PendingResumeOutbox {
    pub(super) checkpoints: HashMap<TaskId, WorkerMessage>,
    pub(super) results: Vec<BufferedTaskResult>,
    pub(super) cancelled: std::collections::HashSet<TaskId>,
    pub(super) max_results: usize,
    pub(super) total_bytes: usize,
    pub(super) max_bytes: usize,
}

impl Default for PendingResumeOutbox {
    fn default() -> Self {
        Self::with_capacity(8)
    }
}

impl PendingResumeOutbox {
    pub(super) fn with_capacity(max_results: usize) -> Self {
        Self {
            checkpoints: HashMap::new(),
            results: Vec::new(),
            cancelled: std::collections::HashSet::new(),
            max_results: max_results.max(2),
            total_bytes: 0,
            max_bytes: MAX_RESUME_OUTBOX_BYTES,
        }
    }

    pub(super) fn clear(&mut self) {
        self.checkpoints.clear();
        self.results.clear();
        self.cancelled.clear();
        self.total_bytes = 0;
    }

    pub(super) fn push_result(&mut self, item: BufferedTaskResult) -> bool {
        let task_id = item.result.task_id;
        if self.cancelled.contains(&task_id) {
            return false;
        }
        let mut evicted_used_blobs = false;
        let mut removed_bytes = 0usize;
        self.results.retain(|r| {
            if r.result.task_id == task_id {
                evicted_used_blobs |= r.used_blobs;
                removed_bytes = removed_bytes.saturating_add(r.byte_size);
                false
            } else {
                true
            }
        });
        self.total_bytes = self.total_bytes.saturating_sub(removed_bytes);
        while self.results.len() >= self.max_results
            || (!self.results.is_empty()
                && self.total_bytes.saturating_add(item.byte_size) > self.max_bytes)
        {
            let evicted = self.results.remove(0);
            self.total_bytes = self.total_bytes.saturating_sub(evicted.byte_size);
            evicted_used_blobs |= evicted.used_blobs;
        }
        self.total_bytes = self.total_bytes.saturating_add(item.byte_size);
        self.results.push(item);
        evicted_used_blobs
    }

    pub(super) fn update_result(
        &mut self,
        task_id: TaskId,
        prev_token: u64,
        result: TaskResult,
    ) -> u64 {
        let new_token = master_features::result_ack_token(&result);
        let new_size = buffered_result_bytes(&result);
        for item in &mut self.results {
            if item.result.task_id == task_id && item.ack_token == prev_token {
                self.total_bytes = self
                    .total_bytes
                    .saturating_sub(item.byte_size)
                    .saturating_add(new_size);
                item.result = result;
                item.ack_token = new_token;
                item.byte_size = new_size;
                break;
            }
        }
        new_token
    }

    pub(super) fn ack_result(&mut self, task_id: TaskId, ack_token: Option<u64>) -> bool {
        let mut used_blobs = false;
        let mut removed_any = false;
        let mut removed_bytes = 0usize;
        self.results.retain(|item| {
            if item.result.task_id == task_id && ack_token.is_none_or(|t| item.ack_token == t) {
                used_blobs |= item.used_blobs;
                removed_any = true;
                removed_bytes = removed_bytes.saturating_add(item.byte_size);
                false
            } else {
                true
            }
        });
        self.total_bytes = self.total_bytes.saturating_sub(removed_bytes);
        if ack_token.is_none() || removed_any {
            self.checkpoints.remove(&task_id);
        }
        used_blobs
    }

    pub(super) fn cancel_task(&mut self, task_id: TaskId) -> bool {
        self.checkpoints.remove(&task_id);
        let mut used_blobs = false;
        let mut removed_bytes = 0usize;
        self.results.retain(|item| {
            if item.result.task_id == task_id {
                used_blobs |= item.used_blobs;
                removed_bytes = removed_bytes.saturating_add(item.byte_size);
                false
            } else {
                true
            }
        });
        self.total_bytes = self.total_bytes.saturating_sub(removed_bytes);
        self.cancelled.insert(task_id);
        used_blobs
    }
}

/// Task ids remembered after a `CancelTask` that found nothing to cancel (see
/// `WorkerClient::handle_cancel_task`).
const CANCELLED_UNSEEN_MEMORY: usize = 256;

/// Removes the table entry of `task_id` only if it still belongs to run `attempt`.
pub(super) async fn remove_active(table: &ActiveTaskTable, task_id: TaskId, attempt: u64) {
    let mut lock = table.write().await;
    if lock.get(&task_id).is_some_and(|h| h.attempt == attempt) {
        lock.remove(&task_id);
    }
}

/// RAII guard ensuring active task counter is decremented, output pins are released, and table entry is pruned on completion or abort.
pub(super) struct ActiveTaskGuard {
    pub(super) task_id: TaskId,
    pub(super) attempt: u64,
    pub(super) heartbeat_tracker: Arc<HeartbeatTracker>,
    pub(super) active_task_table: ActiveTaskTable,
    pub(super) runner: Option<Arc<TaskRunner>>,
    pub(super) _permit: OwnedSemaphorePermit,
}

impl Drop for ActiveTaskGuard {
    fn drop(&mut self) {
        self.heartbeat_tracker.decrement_active_tasks();
        if let Some(ref runner) = self.runner {
            runner.release_task_outputs(self.task_id);
        }
        let table = Arc::clone(&self.active_task_table);
        let (task_id, attempt) = (self.task_id, self.attempt);
        tokio::spawn(async move {
            remove_active(&table, task_id, attempt).await;
        });
    }
}

impl WorkerClient {
    /// Task IDs that are currently executing or have completed results waiting in the
    /// session-resume outbox.
    pub(super) async fn resumable_task_ids(&self) -> Vec<TaskId> {
        let active_ids: Vec<TaskId> = {
            let table = self.active_task_table.read().await;
            table.keys().copied().collect()
        };
        let mut seen = std::collections::HashSet::with_capacity(active_ids.len());
        let mut running = Vec::with_capacity(active_ids.len());
        for id in active_ids {
            if seen.insert(id) {
                running.push(id);
            }
        }
        let outbox = self
            .pending_resume_outbox
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for item in &outbox.results {
            let id = item.result.task_id;
            if !outbox.cancelled.contains(&id) && seen.insert(id) {
                running.push(id);
            }
        }
        running
    }

    /// Ends the session-resume grace window: clears any buffered checkpoints/results (releasing
    /// their pinned output blobs) and cancels any tasks still running.
    pub(super) async fn expire_resume_window(&self, reason: &str) {
        self.resume_window_active.store(false, Ordering::Release);
        let had_used_blobs = {
            let mut outbox = self
                .pending_resume_outbox
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let used_blobs = outbox.results.iter().any(|r| r.used_blobs);
            outbox.clear();
            used_blobs
        };
        self.cancel_all_active_tasks(reason).await;
        self.pending_resume_outbox
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        if had_used_blobs {
            if let Some(cache) = self.runner.blob_cache_if_open() {
                let _ = tokio::task::spawn_blocking(move || cache.evict_to_fit()).await;
            }
        }
    }

    /// Dispatches an assigned task asynchronously, managing permits, telemetry, and results.
    pub async fn handle_assign_task(&self, task: Task, outbound_tx: mpsc::Sender<WorkerMessage>) {
        let checkpoint = task.latest_checkpoint.clone();
        self.handle_assign_task_with_checkpoint(task, checkpoint, outbound_tx)
            .await;
    }

    /// Dispatches an assigned task asynchronously with checkpoint delta resumption.
    pub async fn handle_assign_task_with_checkpoint(
        &self,
        task: Task,
        checkpoint: Option<CheckpointData>,
        outbound_tx: mpsc::Sender<WorkerMessage>,
    ) {
        let task_id = task.id;
        let worker_id = self.worker_id;

        // Cancelled before it got here (see `cancelled_unseen`): report it cancelled instead of
        // running a task the Master has already given up on, which would also hold a slot the
        // Master believes is free.
        let was_cancelled = {
            let mut seen = self
                .cancelled_unseen
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            match seen.iter().position(|id| *id == task_id) {
                Some(pos) => {
                    seen.remove(pos);
                    true
                }
                None => false,
            }
        };
        if was_cancelled {
            warn!(task_id = %task_id, "Refusing an AssignTask for a task the Master already cancelled");
            let result = TaskResult::failure(
                worker_id,
                task_id,
                EXIT_CODE_CANCELLED,
                "",
                "",
                0,
                Some("Cancelled before it was assigned".to_string()),
            );
            let _ = outbound_tx.send(WorkerMessage::from(result)).await;
            return;
        }

        // Defence in depth (design 0082 D2.3): the Master never places a task with an extension this
        // worker lacks, but a task with an unknown critical extension, or a malformed extension
        // list, is never run without the restriction it carries.
        if let Err(reason) = task.validate_extensions("worker") {
            warn!(task_id = %task_id, %reason, "Refusing task: extension check failed");
            let result = TaskResult::failure(
                worker_id,
                task_id,
                EXIT_CODE_GENERAL_ERROR,
                "",
                reason.clone(),
                0,
                Some(reason.clone()),
            );
            let _ = outbound_tx.send(WorkerMessage::from(result)).await;
            return;
        }

        // Verify task requirements against worker capabilities
        if !self.capabilities.satisfies(&task.requirements) {
            warn!(task_id = %task_id, "Task requirements exceed worker capabilities");
            let result = TaskResult::failure(
                worker_id,
                task_id,
                1,
                "",
                "Task requirements exceed worker capabilities (CPU, RAM, or GPU)",
                0,
                Some("Worker capabilities insufficient for task requirements".to_string()),
            );
            let _ = outbound_tx.send(WorkerMessage::from(result)).await;
            return;
        }

        let negotiated = self.negotiated_features();
        if task.workspace.is_some()
            && (!self.config.workspaces || !negotiated.contains(ProtocolCapabilities::WORKSPACES))
        {
            warn!(
                task_id = %task_id,
                "Refusing task with workspace: WORKSPACES is not enabled on this worker or was not negotiated in RegisterAck"
            );
            let result = TaskResult::failure(
                worker_id,
                task_id,
                EXIT_CODE_GENERAL_ERROR,
                "",
                "Task requires WORKSPACES capability, which is not enabled on this worker (start the worker with --workspaces)",
                0,
                Some("WORKSPACES capability not enabled or negotiated".to_string()),
            );
            let _ = outbound_tx.send(WorkerMessage::from(result)).await;
            return;
        }

        let has_workspace_tree = task
            .workspace
            .as_ref()
            .is_some_and(|ws| ws.source.is_some());

        if (!task.staged_files.is_empty() || !task.expected_outputs.is_empty())
            && !negotiated.contains(ProtocolCapabilities::FILE_STAGING)
        {
            warn!(
                task_id = %task_id,
                "Refusing task with staged_files/expected_outputs: FILE_STAGING was not acknowledged in RegisterAck"
            );
            let result = TaskResult::failure(
                worker_id,
                task_id,
                EXIT_CODE_GENERAL_ERROR,
                "",
                "Task requires FILE_STAGING capability, which was not negotiated in RegisterAck",
                0,
                Some(
                    "FILE_STAGING capability not negotiated with master in RegisterAck".to_string(),
                ),
            );
            let _ = outbound_tx.send(WorkerMessage::from(result)).await;
            return;
        }

        if (!task.inputs.is_empty() || !task.output_globs.is_empty() || has_workspace_tree)
            && !negotiated.contains(ProtocolCapabilities::BLOB_TRANSFER)
        {
            warn!(
                task_id = %task_id,
                "Refusing task with blob inputs/output_globs/workspace tree: BLOB_TRANSFER was not acknowledged in RegisterAck"
            );
            let result = TaskResult::failure(
                worker_id,
                task_id,
                EXIT_CODE_GENERAL_ERROR,
                "",
                "Task requires BLOB_TRANSFER capability, which was not negotiated in RegisterAck",
                0,
                Some(
                    "BLOB_TRANSFER capability not negotiated with master in RegisterAck"
                        .to_string(),
                ),
            );
            let _ = outbound_tx.send(WorkerMessage::from(result)).await;
            return;
        }

        // Mobile constraints: reject task if battery < 15% and discharging. The reading is the
        // live one (platform callbacks / periodic poll), not the snapshot taken at startup.
        if let Some(mobile) = self.mobile.snapshot() {
            if let (Some(pct), Some(false)) = (mobile.battery_pct, mobile.is_charging) {
                if pct < 15 {
                    warn!(
                        task_id = %task_id,
                        pct = pct,
                        "Task rejected by worker: Critical battery preservation (<15% discharging)"
                    );
                    let result = TaskResult::failure(
                        worker_id,
                        task_id,
                        1,
                        "",
                        "Task rejected by worker: Critical battery preservation (<15% discharging)",
                        0,
                        Some("Battery critically low (<15% discharging)".to_string()),
                    );
                    let _ = outbound_tx.send(WorkerMessage::from(result)).await;
                    return;
                }
            }
        }

        // Contributor controls (`NET-9`, design 0095): hold back / hand back tasks whenever the
        // worker is paused locally or above/outside a configured contributor limit.
        if let Err(block_reason) = self.controls.check_admission() {
            let refusal = block_reason.refusal_message();
            info!(
                task_id = %task_id,
                control = block_reason.control_name(),
                reason = %block_reason,
                "Holding back task due to local contributor control"
            );
            let result =
                TaskResult::failure(worker_id, task_id, 1, "", refusal.clone(), 0, Some(refusal));
            let _ = outbound_tx.send(WorkerMessage::from(result)).await;
            return;
        }

        // The same task id can arrive while an earlier run of it is still alive (a duplicate
        // delivery). Replacing the table entry would drop the old run's cancel handle and make it
        // look like a Master cancellation, so stop it explicitly first, silently.
        self.retire_previous_run(task_id).await;
        let skip_sandbox_touch = self.runner.config().fake_runner.is_some()
            && task.inputs.is_empty()
            && task.output_globs.is_empty()
            && !has_workspace_tree;
        if !skip_sandbox_touch {
            // Nothing may touch the sandbox base (the reclaim below, the blob cache, the sandbox
            // itself) while another local user could have replaced its private root (design 0021).
            if let Err(e) = recheck_base_once_more(&self.runner.config().sandbox_config).await {
                warn!(task_id = %task_id, error = %e, "Refusing a task: the sandbox base is not usable");
                // A refusal the master hands to another worker (design 0090), not a task failure.
                let result = crate::runner::base_refusal(worker_id, task_id, &e);
                let _ = outbound_tx.send(WorkerMessage::from(result)).await;
                return;
            }
            // A sandbox directory left behind by a killed worker process (or kept for debugging)
            // would make `Sandbox::create` fail with "already exists", and that failure is
            // terminal.
            self.reclaim_stale_sandbox(task_id).await;
        }

        let attempt = self.next_attempt.fetch_add(1, Ordering::Relaxed) + 1;
        let (cancel_tx, cancel_rx) = oneshot::channel::<CancelSignal>();
        let child_pid = Arc::new(AtomicU32::new(0));
        let suppress_result = Arc::new(AtomicBool::new(false));
        let result_sent = Arc::new(AtomicBool::new(false));
        let outbound_tx_for_handle = outbound_tx.clone();

        let tracker = Arc::clone(&self.heartbeat_tracker);
        let table = Arc::clone(&self.active_task_table);
        let semaphore = Arc::clone(&self.concurrency_semaphore);
        let runner = Arc::clone(&self.runner);
        let child_pid_clone = Arc::clone(&child_pid);
        let suppress = Arc::clone(&suppress_result);
        let sent_flag = Arc::clone(&result_sent);
        let blob_client_slot = Arc::clone(&self.blob_client);
        let session_data_tx = Arc::clone(&self.session_data_tx);
        let current_session_hold_secs = Arc::clone(&self.current_session_hold_secs);
        let resume_window_active = Arc::clone(&self.resume_window_active);
        let pending_resume_outbox = Arc::clone(&self.pending_resume_outbox);
        let controls = Arc::clone(&self.controls);
        let grace_enabled = self.config.session_resume_grace_cap != Some(Duration::ZERO);

        // The table lock is held from before the spawn until the handle is inserted, so the run's
        // own cleanup (which needs the lock) can never overtake the insertion and leave a stale
        // entry behind.
        let mut lock = self.active_task_table.write().await;
        if self.controls.is_paused() {
            drop(lock);
            let refusal = cancelled_task_result(
                worker_id,
                task_id,
                CancelSignal::ContributorPause("paused by contributor".to_string()),
            );
            let _ = outbound_tx.send(WorkerMessage::from(refusal)).await;
            return;
        }
        let runner_handle = tokio::spawn(async move {
            let mut cancel_rx = cancel_rx;
            let permit = tokio::select! {
                res = semaphore.acquire_owned() => match res {
                    Ok(p) => p,
                    Err(_) => return, // Worker shutting down
                },
                cancel_msg = &mut cancel_rx => {
                    let sig = cancel_msg.unwrap_or_else(|_| CancelSignal::MasterCancel("Cancelled while queued".into()));
                    let res = cancelled_task_result(worker_id, task_id, sig);
                    remove_active(&table, task_id, attempt).await;
                    if !suppress.load(Ordering::Acquire) && !sent_flag.swap(true, Ordering::AcqRel) {
                        if let Some(tx) = resolve_current_tx(&session_data_tx, Some(&outbound_tx)) {
                            let _ = tx.send(WorkerMessage::from(res)).await;
                        }
                    }
                    return;
                }
            };

            let mut _guard = ActiveTaskGuard {
                task_id,
                attempt,
                heartbeat_tracker: Arc::clone(&tracker),
                active_task_table: Arc::clone(&table),
                runner: Some(Arc::clone(&runner)),
                _permit: permit,
            };

            tracker.increment_active_tasks();

            let progress = WorkerMessage::TaskProgress {
                worker_id,
                task_id,
                status: TaskStatus::Running,
            };
            if let Some(tx) = resolve_current_tx(&session_data_tx, Some(&outbound_tx)) {
                if tx.send(progress).await.is_err() && !resume_window_active.load(Ordering::Acquire)
                {
                    return;
                }
            } else if !resume_window_active.load(Ordering::Acquire) {
                return;
            }

            // Every blob the inputs (and workspace tree, if any) need is in the cache (and pinned)
            // before the run starts.
            let mut cancel_slot = Some(cancel_rx);
            let _inputs = if task.inputs.is_empty() && !has_workspace_tree {
                None
            } else {
                let current_blob_client = blob_client_slot
                    .read()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone();
                let fetch = async {
                    // Checked again: the task may have waited for a slot since it was assigned.
                    // A base that broke meanwhile is a refusal, not a failed input fetch.
                    if let Err(e) = recheck_base_once_more(&runner.config().sandbox_config).await {
                        return Err(Err(crate::runner::base_refusal(worker_id, task_id, &e)));
                    }
                    let cache = runner.ensure_blob_cache().await.map_err(Ok)?;
                    fetch_task_inputs(&task, current_blob_client.as_deref(), &cache)
                        .await
                        .map_err(Ok)
                };
                let failure = tokio::select! {
                    fetched = fetch => match fetched {
                        Ok(pin) => Ok(pin),
                        Err(Err(refusal)) => Err(refusal),
                        Err(Ok(e)) => Err(TaskResult::failure(
                            worker_id,
                            task_id,
                            EXIT_CODE_GENERAL_ERROR,
                            "",
                            format!("Fetching input files failed: {e}"),
                            0,
                            Some(format!("Input files: {e}")),
                        )),
                    },
                    sig = receive_cancel_signal(&mut cancel_slot) => Err(cancelled_task_result(worker_id, task_id, sig)),
                };
                match failure {
                    Ok(pin) => Some(pin),
                    Err(result) => {
                        if !suppress.load(Ordering::Acquire)
                            && !sent_flag.swap(true, Ordering::AcqRel)
                        {
                            if let Some(tx) =
                                resolve_current_tx(&session_data_tx, Some(&outbound_tx))
                            {
                                let _ = tx.send(WorkerMessage::from(result)).await;
                            }
                        }
                        return;
                    }
                }
            };

            let effective_checkpoint = checkpoint.or_else(|| task.latest_checkpoint.clone());
            let (cp_tx, mut cp_rx) = mpsc::channel::<WorkerMessage>(16);
            let checkpoint_tx = negotiated
                .contains(ProtocolCapabilities::CHECKPOINT_DELTA)
                .then_some(cp_tx);
            let forward_checkpoint = |cp_msg: WorkerMessage| {
                let mut sent = false;
                if let Some(tx) = resolve_current_tx(&session_data_tx, Some(&outbound_tx)) {
                    if tx.try_send(cp_msg.clone()).is_ok() {
                        sent = true;
                    }
                }
                if !sent && resume_window_active.load(Ordering::Acquire) {
                    let mut outbox = pending_resume_outbox
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    if !outbox.cancelled.contains(&task_id) {
                        outbox.checkpoints.insert(task_id, cp_msg);
                    }
                }
            };
            let (exec_cancel_tx, exec_cancel_rx) = oneshot::channel::<String>();
            let mut exec_cancel_tx = Some(exec_cancel_tx);
            let mut cancelled_signal: Option<CancelSignal> = None;
            let mut result = {
                let exec_fut = runner.execute_task_with_checkpoint(
                    &task,
                    effective_checkpoint.as_ref(),
                    checkpoint_tx,
                    Some(child_pid_clone),
                    Some(exec_cancel_rx),
                );
                tokio::pin!(exec_fut);
                loop {
                    tokio::select! {
                        res = &mut exec_fut => break res,
                        Some(cp_msg) = cp_rx.recv() => {
                            forward_checkpoint(cp_msg);
                        }
                        sig = receive_cancel_signal(&mut cancel_slot) => {
                            if let Some(tx) = exec_cancel_tx.take() {
                                let reason = match &sig {
                                    CancelSignal::MasterCancel(r)
                                    | CancelSignal::ContributorPause(r) => r.clone(),
                                };
                                let _ = tx.send(reason);
                            }
                            // Remember the cancellation even when `tx.send` succeeded: if the
                            // child process already exited and `execute_process_supervised` is
                            // draining pipes or collecting outputs, `exec_cancel_rx` is not
                            // polled again and `exec_fut` returns the process's exit code.
                            cancelled_signal = Some(sig);
                        }
                    }
                }
            };
            while let Ok(cp_msg) = cp_rx.try_recv() {
                forward_checkpoint(cp_msg);
            }

            // Output files reach the Master's blob store before the result that names them.
            let used_blobs = _inputs.is_some() || !result.outputs.is_empty();
            let output_pin = runner
                .blob_cache_if_open()
                .and_then(|cache| cache.take_task_outputs(task.id))
                .map(Arc::new);
            // Ownership of this attempt's output pin now lives in `output_pin`; prevent `_guard`
            // from releasing a subsequent retry attempt's `task_outputs` entry on drop.
            _guard.runner = None;
            let resume_enabled = grace_enabled
                && (current_session_hold_secs.load(Ordering::Acquire) > 0
                    || resume_window_active.load(Ordering::Acquire));
            // `Some` exactly when `resume_enabled`: only the resume outbox uses the token.
            let mut buffered_token =
                resume_ack_token(resume_enabled, &result, master_features::result_ack_token);

            if cancelled_signal.is_none() && !suppress.load(Ordering::Acquire) {
                let (live_tx, evicted_used_blobs) = {
                    let mut outbox = pending_resume_outbox
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    let tx = resolve_current_tx(&session_data_tx, Some(&outbound_tx));
                    let evicted = match buffered_token {
                        Some(ack_token) => outbox.push_result(BufferedTaskResult::new(
                            task.clone(),
                            result.clone(),
                            ack_token,
                            output_pin.clone(),
                            used_blobs,
                        )),
                        None => false,
                    };
                    (tx, evicted)
                };
                if evicted_used_blobs {
                    if let Some(cache) = runner.blob_cache_if_open() {
                        let _ = tokio::task::spawn_blocking(move || cache.evict_to_fit()).await;
                    }
                }
                if live_tx.is_none() && resume_enabled {
                    drop(output_pin);
                    drop(_inputs);
                    drop(cancel_slot);
                    return;
                }

                let current_blob_client = blob_client_slot
                    .read()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone();
                let had_outputs = !result.outputs.is_empty();
                if had_outputs {
                    match runner.ensure_blob_cache().await {
                        Ok(cache) => {
                            tokio::select! {
                                () = upload_task_outputs(&task, &mut result, current_blob_client.as_deref(), &cache) => {}
                                sig = receive_cancel_signal(&mut cancel_slot) => {
                                    cancelled_signal = Some(sig);
                                }
                            }
                        }
                        Err(e) => {
                            result.outputs.clear();
                            result.error = Some(e);
                        }
                    }
                    // A session that ended while the upload ran made it fail only because the
                    // connection went away: the buffered result still holds the outputs, and the
                    // resume uploads them. Overwriting it with this failure would hand the client
                    // a failed task for a task that ran fine (TEST-23).
                    let session_ended = current_blob_client
                        .as_ref()
                        .is_some_and(|client| client.is_closed());
                    if cancelled_signal.is_none() && !session_ended {
                        if let Some(prev_token) = buffered_token {
                            buffered_token = Some(
                                pending_resume_outbox
                                    .lock()
                                    .unwrap_or_else(|e| e.into_inner())
                                    .update_result(task.id, prev_token, result.clone()),
                            );
                        }
                    }
                }

                let blob_client_closed = had_outputs
                    && current_blob_client
                        .as_ref()
                        .is_some_and(|client| client.is_closed());
                if cancelled_signal.is_none()
                    && !suppress.load(Ordering::Acquire)
                    && resume_enabled
                    && (blob_client_closed
                        || resolve_current_tx(&session_data_tx, Some(&outbound_tx)).is_none())
                {
                    drop(output_pin);
                    drop(_inputs);
                    drop(cancel_slot);
                    return;
                }
            }

            if let Some(token) = buffered_token {
                if cancelled_signal.is_some() || suppress.load(Ordering::Acquire) {
                    pending_resume_outbox
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .ack_result(task.id, Some(token));
                }
            }

            drop(output_pin);
            drop(_inputs);
            if used_blobs {
                if let Some(cache) = runner.blob_cache_if_open() {
                    let _ = tokio::task::spawn_blocking(move || cache.evict_to_fit()).await;
                }
            }

            match cancelled_signal {
                Some(CancelSignal::MasterCancel(reason)) => {
                    if result.exit_code != EXIT_CODE_CANCELLED {
                        result.exit_code = EXIT_CODE_CANCELLED;
                        result.error = Some(format!("Cancelled: {reason}"));
                    }
                    result.outputs.clear();
                    result.artifacts.clear();
                    result.asset_bytes = None;
                }
                Some(CancelSignal::ContributorPause(reason)) => {
                    result = cancelled_task_result(
                        worker_id,
                        task_id,
                        CancelSignal::ContributorPause(reason),
                    );
                }
                None => {
                    if !suppress.load(Ordering::Acquire) {
                        controls
                            .record_completed_task(Duration::from_millis(result.execution_time_ms));
                    }
                }
            }

            if !suppress.load(Ordering::Acquire) && !sent_flag.swap(true, Ordering::AcqRel) {
                if let Some(tx) = resolve_current_tx(&session_data_tx, Some(&outbound_tx)) {
                    let _ = tx.send(WorkerMessage::from(result)).await;
                }
            }
            drop(cancel_slot);
        });

        lock.insert(
            task_id,
            TaskExecutionHandle {
                task_id,
                cancel_tx: Some(cancel_tx),
                abort_handle: runner_handle.abort_handle(),
                child_pid,
                started_at_epoch_sec: current_epoch_secs(),
                attempt,
                suppress_result,
                result_sent,
                outbound_tx: Some(outbound_tx_for_handle),
                join: Some(runner_handle),
            },
        );
    }

    /// Cancels an active task by signaling the cancellation channel and triggering abort if necessary.
    pub async fn handle_cancel_task(&self, task_id: TaskId, reason: Option<String>) {
        if let Some(ack_token) = reason
            .as_deref()
            .and_then(rusty_grid_core::protocol::master_features::parse_result_ack_reason)
        {
            let used_blobs = self
                .pending_resume_outbox
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .ack_result(task_id, ack_token);
            if used_blobs {
                if let Some(cache) = self.runner.blob_cache_if_open() {
                    let _ = tokio::task::spawn_blocking(move || cache.evict_to_fit()).await;
                }
            }
            return;
        }

        let (had_buffered, used_blobs) = {
            let mut outbox = self
                .pending_resume_outbox
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let had = outbox.checkpoints.contains_key(&task_id)
                || outbox.results.iter().any(|r| r.result.task_id == task_id);
            let used_blobs = outbox.cancel_task(task_id);
            (had, used_blobs)
        };
        if used_blobs {
            self.runner.release_task_outputs(task_id);
            if let Some(cache) = self.runner.blob_cache_if_open() {
                let _ = tokio::task::spawn_blocking(move || cache.evict_to_fit()).await;
            }
        }
        let mut lock = self.active_task_table.write().await;
        if let Some(mut handle) = lock.remove(&task_id) {
            let r = reason.unwrap_or_else(|| "Cancelled by master directive".into());
            if r.contains("no longer assigned to this worker") {
                handle.suppress_result.store(true, Ordering::Release);
            }
            info!(task_id = %task_id, reason = %r, "Cancelling active task");
            let sent = if let Some(tx) = handle.cancel_tx.take() {
                tx.send(CancelSignal::MasterCancel(r)).is_ok()
            } else {
                false
            };
            if !sent {
                handle.abort_handle.abort();
            }
        } else if had_buffered {
            info!(
                task_id = %task_id,
                reason = ?reason,
                "Dropped buffered session-resume result for task cancelled by master"
            );
        } else {
            warn!(task_id = %task_id, "CancelTask received for unknown or already completed task");
            // The Master sends AssignTask before CancelTask, but a cancelled task that is still
            // on its way must not run once it arrives; a task id is never assigned again after
            // it was cancelled, so remembering the id is safe.
            let mut seen = self
                .cancelled_unseen
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if !seen.contains(&task_id) {
                if seen.len() >= CANCELLED_UNSEEN_MEMORY {
                    seen.pop_front();
                }
                seen.push_back(task_id);
            }
        }
    }
}

pub(super) async fn pause_active_tasks_inner(
    worker_id: uuid::Uuid,
    active_task_table: &ActiveTaskTable,
    session_data_tx: &std::sync::RwLock<Option<mpsc::Sender<WorkerMessage>>>,
    pause_stop_timeout: Duration,
    reason: &str,
) -> usize {
    let handles: Vec<TaskExecutionHandle> = {
        let mut lock = active_task_table.write().await;
        lock.drain().map(|(_, handle)| handle).collect()
    };
    if handles.is_empty() {
        return 0;
    }
    let count = handles.len();
    info!(
        worker_id = %worker_id,
        tasks = count,
        reason = %reason,
        "Pausing active tasks under contributor control; stopping process trees and handing tasks back"
    );
    let mut pending = Vec::with_capacity(count);
    for mut handle in handles {
        let sent = handle.cancel_tx.take().is_some_and(|tx| {
            tx.send(CancelSignal::ContributorPause(reason.to_string()))
                .is_ok()
        });
        if !sent {
            handle.abort_handle.abort();
        }
        pending.push((
            handle.task_id,
            handle.suppress_result,
            handle.result_sent,
            handle.outbound_tx,
            handle.join.take(),
            sent,
        ));
    }
    let deadline = tokio::time::Instant::now() + pause_stop_timeout;
    for (task_id, suppress_result, result_sent, fallback_tx, join_opt, sent) in pending {
        let mut aborted = !sent;
        if let Some(mut join) = join_opt {
            if sent {
                match tokio::time::timeout_at(deadline, &mut join).await {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => {
                        aborted = true;
                    }
                    Err(_) => {
                        warn!(
                            task_id = %task_id,
                            timeout_secs = pause_stop_timeout.as_secs_f32(),
                            "A paused task did not stop within PAUSE_STOP_TIMEOUT; aborting it"
                        );
                        join.abort();
                        let _ = join.await;
                        aborted = true;
                    }
                }
            } else {
                let _ = join.await;
                aborted = true;
            }
        }
        if aborted
            && !suppress_result.load(Ordering::Acquire)
            && !result_sent.swap(true, Ordering::AcqRel)
        {
            let refusal = cancelled_task_result(
                worker_id,
                task_id,
                CancelSignal::ContributorPause(reason.to_string()),
            );
            if let Some(tx) = resolve_current_tx(session_data_tx, fallback_tx.as_ref()) {
                let _ = tx.send(WorkerMessage::from(refusal)).await;
            }
        }
    }
    count
}

pub(super) async fn handle_pause_transition(
    worker_id: uuid::Uuid,
    active_task_table: &ActiveTaskTable,
    session_data_tx: &std::sync::RwLock<Option<mpsc::Sender<WorkerMessage>>>,
    pause_stop_timeout: Duration,
    transition: crate::controls::PauseTransition,
) {
    match transition {
        crate::controls::PauseTransition::BecamePaused => {
            info!(
                worker_id = %worker_id,
                control = "local_pause",
                "Contributor pause activated; holding new tasks and stopping active tasks"
            );
            pause_active_tasks_inner(
                worker_id,
                active_task_table,
                session_data_tx,
                pause_stop_timeout,
                "paused by contributor",
            )
            .await;
        }
        crate::controls::PauseTransition::BecameResumed => {
            info!(
                worker_id = %worker_id,
                control = "local_pause",
                "Contributor pause cleared; resuming task acceptance"
            );
        }
        crate::controls::PauseTransition::Unchanged => {}
    }
}

impl WorkerClient {
    /// Stops a run the worker itself decided to end: its result will not be sent.
    /// Returns the run's join handle so the caller can wait for the cleanup.
    pub(super) fn begin_stop(
        mut handle: TaskExecutionHandle,
        reason: &str,
    ) -> Option<JoinHandle<()>> {
        handle.suppress_result.store(true, Ordering::Release);
        let sent = handle.cancel_tx.take().is_some_and(|tx| {
            tx.send(CancelSignal::MasterCancel(reason.to_string()))
                .is_ok()
        });
        if !sent {
            handle.abort_handle.abort();
        }
        handle.join.take()
    }

    /// Interrupts every active task under a contributor local pause (`worker pause`, `NET-9`),
    /// waiting up to `pause_stop_timeout` (`5 s`) for their process trees to exit while keeping
    /// `suppress_result == false` so each interrupted or force-aborted task reports a design 0090
    /// worker refusal and is requeued without charging execution retries.
    pub async fn pause_active_tasks(&self, reason: &str) -> usize {
        pause_active_tasks_inner(
            self.worker_id,
            &self.active_task_table,
            &self.session_data_tx,
            self.config.pause_stop_timeout,
            reason,
        )
        .await
    }

    /// Cancels the previous run of `task_id`, if any, and waits until it is gone.
    pub(super) async fn retire_previous_run(&self, task_id: TaskId) {
        let previous = self.active_task_table.write().await.remove(&task_id);
        let Some(previous) = previous else {
            return;
        };
        warn!(task_id = %task_id, "Task assigned again while its previous run is still active; cancelling the old run first");
        if let Some(mut join) =
            Self::begin_stop(previous, "superseded by a new assignment of the same task")
        {
            if tokio::time::timeout(TASK_DRAIN_TIMEOUT, &mut join)
                .await
                .is_err()
            {
                warn!(task_id = %task_id, "Previous run did not stop in time; aborting it");
                join.abort();
            }
        }
    }

    /// Cancels every task still running on this worker, silently, and waits (bounded by
    /// [`TASK_DRAIN_TIMEOUT`]) until their process trees are killed and their sandboxes removed.
    /// Called whenever a session ends; returns how many tasks were running.
    ///
    /// No result is sent for them: the Master only accepts a result from the session a task was
    /// assigned in and requeues the task when that session ends, so a result could only be dropped.
    pub async fn cancel_all_active_tasks(&self, reason: &str) -> usize {
        let handles: Vec<TaskExecutionHandle> = {
            let mut lock = self.active_task_table.write().await;
            lock.drain().map(|(_, handle)| handle).collect()
        };
        if handles.is_empty() {
            return 0;
        }
        let count = handles.len();
        warn!(
            worker_id = %self.worker_id,
            tasks = count,
            reason = %reason,
            "Cancelling running tasks: their results can no longer be delivered, and the Master requeues them"
        );
        let joins: Vec<JoinHandle<()>> = handles
            .into_iter()
            .filter_map(|handle| Self::begin_stop(handle, reason))
            .collect();
        let is_fast_evac = {
            let r = reason.to_ascii_uppercase();
            r.contains("EVAC")
                || r.contains("BATTERY")
                || r.contains("SHUTDOWN")
                || r.contains("FAST")
        };
        if is_fast_evac {
            for join in joins {
                join.abort();
            }
        } else {
            let deadline = tokio::time::Instant::now() + TASK_DRAIN_TIMEOUT;
            for mut join in joins {
                if tokio::time::timeout_at(deadline, &mut join).await.is_err() {
                    warn!("A cancelled task did not stop within the drain timeout; aborting it");
                    join.abort();
                }
            }
        }
        count
    }

    /// Removes (or, with `keep_sandboxes`, moves aside) a `<task id>` sandbox directory that no
    /// run of this worker owns. Called only after any earlier run of the id has been awaited.
    pub(super) async fn reclaim_stale_sandbox(&self, task_id: TaskId) {
        let path = self.sandbox_base_dir.join(task_id.to_string());
        if !tokio::fs::try_exists(&path).await.unwrap_or(false) {
            return;
        }
        if self.config.keep_sandboxes {
            let aside = self.sandbox_base_dir.join(format!(
                "{task_id}.stale-{}",
                chrono::Utc::now().timestamp_millis()
            ));
            match tokio::fs::rename(&path, &aside).await {
                Ok(()) => {
                    info!(task_id = %task_id, moved_to = %aside.display(), "Moved a leftover sandbox of this task aside")
                }
                Err(e) => {
                    warn!(task_id = %task_id, error = %e, "Could not move a leftover sandbox aside")
                }
            }
        } else {
            match tokio::fs::remove_dir_all(&path).await {
                Ok(()) => {
                    info!(task_id = %task_id, path = %path.display(), "Removed a leftover sandbox of this task")
                }
                Err(e) => {
                    warn!(task_id = %task_id, error = %e, "Could not remove a leftover sandbox")
                }
            }
        }
    }
}
