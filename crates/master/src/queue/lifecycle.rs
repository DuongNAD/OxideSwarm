//! Checkpoints, terminal result recording, evacuation, disconnect failover, and cancellation.

use std::cmp::Reverse;
use std::collections::HashSet;
use std::time::{Duration, Instant};

use chrono::Utc;
use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::task::{CheckpointData, TaskId, TaskResult};
use uuid::Uuid;

use crate::store::{DurableAck, JournalRecord};

use super::{
    is_worker_rejection, owned_by, retained_bytes, CancelOutcome, DelayedRetryKey, QueueOrderKey,
    ResultDisposition, ResultOutcome, RetryAttempt, TaskEntry, TaskQueue, TaskQueueInner,
    TaskState, MAX_WORKER_REJECTIONS_PER_TASK,
};

/// What one worker's failover sweep did, for the caller that holds the queue lock to journal.
struct WorkerSweep {
    affected: Vec<TaskId>,
    had_terminal: bool,
    records: Vec<JournalRecord>,
    dropped_checkpoints: usize,
}

impl TaskQueue {
    /// Records a mid-task computation checkpoint for an in-flight task.
    ///
    /// Monotonically enforces checkpoint ordering by sequence number.
    /// Outdated out-of-order checkpoint snapshots are safely ignored.
    ///
    /// This variant does not know who sent the checkpoint; connection handlers use
    /// [`record_checkpoint_from`](Self::record_checkpoint_from).
    pub async fn record_checkpoint(
        &self,
        task_id: TaskId,
        checkpoint: CheckpointData,
    ) -> Result<(), GridError> {
        let ack = {
            let mut inner = self.inner.write().await;
            let max_active = inner.max_active_bytes;
            let active_bytes = inner.active_bytes;
            let has_store = inner.store.is_some();
            let entry = inner
                .tasks
                .get_mut(&task_id)
                .ok_or(GridError::TaskNotFound(task_id.0))?;

            if entry.state.is_terminal() {
                return Ok(());
            }

            if let Some(task_bytes) =
                Self::checkpoint_exceeds_active_cap(entry, &checkpoint, active_bytes, max_active)
            {
                return Err(GridError::QueueByteLimitExceeded {
                    task_bytes,
                    in_use_bytes: active_bytes,
                    max_bytes: max_active,
                });
            }

            let record_cp = has_store.then(|| checkpoint.clone());
            if Self::apply_checkpoint(entry, checkpoint) {
                inner.update_active_task(task_id);
                let ack = if let Some(cp) = record_cp {
                    inner.append_durable_warn(&JournalRecord::CheckpointRecorded {
                        task_id,
                        checkpoint: cp,
                    })
                } else {
                    None
                };
                inner.maybe_trigger_compaction();
                ack
            } else {
                None
            }
        };

        if let Some(ack) = ack {
            if let Err(err) = ack.await {
                tracing::warn!(error = %err, "Failed to sync CheckpointRecorded journal record");
            }
        }
        Ok(())
    }

    /// Records a checkpoint sent by connection `(worker_id, session_id)`.
    ///
    /// Only the worker and session a task is assigned to may checkpoint it: a stale or foreign
    /// checkpoint with a huge sequence number would otherwise poison the state the next failover
    /// resumes from and lock out every genuine later checkpoint. Returns whether it was stored.
    pub async fn record_checkpoint_from(
        &self,
        worker_id: Uuid,
        session_id: Option<u64>,
        task_id: TaskId,
        checkpoint: CheckpointData,
    ) -> Result<bool, GridError> {
        let (applied, ack) = {
            let mut inner = self.inner.write().await;
            let max_active = inner.max_active_bytes;
            let active_bytes = inner.active_bytes;
            let has_store = inner.store.is_some();
            let now_instant = Instant::now();
            let entry = inner
                .tasks
                .get_mut(&task_id)
                .ok_or(GridError::TaskNotFound(task_id.0))?;

            let held_by_sender = entry.state == TaskState::Queued
                && entry
                    .adoptable_by
                    .is_some_and(|(w, until)| w == worker_id && now_instant < until);
            let active_owned = entry.state.is_active() && owned_by(entry, &worker_id, session_id);
            if !active_owned && !held_by_sender {
                tracing::debug!(
                    task_id = %task_id,
                    worker_id = %worker_id,
                    state = ?entry.state,
                    "Dropping checkpoint from a worker/session the task is not assigned to"
                );
                return Ok(false);
            }

            if let Some(task_bytes) =
                Self::checkpoint_exceeds_active_cap(entry, &checkpoint, active_bytes, max_active)
            {
                return Err(GridError::QueueByteLimitExceeded {
                    task_bytes,
                    in_use_bytes: active_bytes,
                    max_bytes: max_active,
                });
            }

            let record_cp = has_store.then(|| checkpoint.clone());
            let applied = Self::apply_checkpoint(entry, checkpoint);
            let ack = if applied {
                inner.update_active_task(task_id);
                let ack = if let Some(cp) = record_cp {
                    inner.append_durable_warn(&JournalRecord::CheckpointRecorded {
                        task_id,
                        checkpoint: cp,
                    })
                } else {
                    None
                };
                inner.maybe_trigger_compaction();
                ack
            } else {
                None
            };
            (applied, ack)
        };

        if let Some(ack) = ack {
            if let Err(err) = ack.await {
                tracing::warn!(error = %err, "Failed to sync CheckpointRecorded journal record");
            }
        }
        Ok(applied)
    }

    fn is_newer_checkpoint(entry: &TaskEntry, checkpoint: &CheckpointData) -> bool {
        match &entry.task.latest_checkpoint {
            Some(existing) => checkpoint.sequence > existing.sequence,
            None => true,
        }
    }

    fn checkpoint_exceeds_active_cap(
        entry: &TaskEntry,
        checkpoint: &CheckpointData,
        active_bytes: usize,
        max_active_bytes: usize,
    ) -> Option<usize> {
        if !Self::is_newer_checkpoint(entry, checkpoint) {
            return None;
        }
        let old_cp = entry
            .task
            .latest_checkpoint
            .as_ref()
            .map_or(0, |c| c.delta_state.0.len());
        let new_cp = checkpoint.delta_state.0.len();
        if new_cp > old_cp
            && active_bytes.saturating_sub(old_cp).saturating_add(new_cp) > max_active_bytes
        {
            Some(
                retained_bytes(entry)
                    .saturating_sub(old_cp)
                    .saturating_add(new_cp),
            )
        } else {
            None
        }
    }

    fn apply_checkpoint(entry: &mut TaskEntry, checkpoint: CheckpointData) -> bool {
        let is_newer = Self::is_newer_checkpoint(entry, &checkpoint);

        if is_newer {
            entry.task.update_checkpoint(checkpoint);
        }
        is_newer
    }

    /// Retrieves the latest verified mid-task checkpoint for a task, if available.
    pub async fn get_checkpoint(&self, task_id: &TaskId) -> Option<CheckpointData> {
        let inner = self.inner.read().await;
        inner
            .tasks
            .get(task_id)
            .and_then(|e| e.task.latest_checkpoint.clone())
    }

    /// Records execution outcome returned by a worker and transitions state.
    ///
    /// Trusts `result.worker_id` as the sender; connection handlers, which know who is really on
    /// the other end of the socket, use [`record_result_from`](Self::record_result_from).
    pub async fn record_result(&self, result: TaskResult) -> GridResult<TaskState> {
        let worker_id = result.worker_id;
        self.record_result_from(worker_id, None, result)
            .await
            .map(|outcome| outcome.state)
    }

    /// Records the result `result` sent by connection `(worker_id, session_id)`.
    ///
    /// The sender is the *connection*, never the id in the message body. Anything that does not
    /// come from the worker and session the task is currently assigned to is dropped with a debug
    /// log, without touching the FSM, so a reaped, superseded or hostile peer can neither
    /// complete, fail nor cancel a task that now belongs to someone else.
    pub async fn record_result_from(
        &self,
        worker_id: Uuid,
        session_id: Option<u64>,
        result: TaskResult,
    ) -> GridResult<ResultOutcome> {
        let (outcome, ack) = self
            .record_result_from_enqueued(worker_id, session_id, result)
            .await?;
        if let Some(ack) = ack {
            if let Err(err) = ack.await {
                tracing::warn!(error = %err, "Failed to sync task result journal record");
            }
        }
        if outcome.state.is_terminal() {
            self.flush_cold_maintenance().await;
        }
        Ok(outcome)
    }

    /// Transitions the task state for `result` in memory and enqueues the durable journal record,
    /// returning the [`ResultOutcome`] and optional [`DurableAck`] without blocking the caller on
    /// `sync_data()`.
    ///
    /// Session handlers use this so multiple results arriving on one worker connection can release
    /// worker slots immediately and batch their `sync_data()` calls inside the 5 ms group-commit
    /// window before waking `--wait` clients.
    pub(crate) async fn record_result_from_enqueued(
        &self,
        worker_id: Uuid,
        session_id: Option<u64>,
        result: TaskResult,
    ) -> GridResult<(ResultOutcome, Option<DurableAck>)> {
        let task_id = result.task_id;
        #[cfg(feature = "otel")]
        let exit_code = i64::from(result.exit_code);

        let mut inner = self.inner.write().await;
        if inner.store_closed {
            return Err(GridError::Storage(
                "task store is closed (master shutting down)".into(),
            ));
        }
        let policy = inner.retry_policy.clone();

        let now_instant = Instant::now();
        let now_epoch = Utc::now().timestamp() as u64;
        let (
            state,
            owner,
            adoptable_by_sender,
            priority,
            sequence,
            task_key,
            timeout_secs,
            resumed_from_checkpoint,
        ) = {
            let entry = inner
                .tasks
                .get(&task_id)
                .ok_or(GridError::TaskNotFound(task_id.0))?;
            let owner = owned_by(entry, &worker_id, session_id);
            let adoptable = !owner
                && entry.state == TaskState::Queued
                && entry
                    .adoptable_by
                    .is_some_and(|(w, until)| w == worker_id && now_instant < until);
            (
                entry.state,
                owner,
                adoptable,
                entry.priority,
                entry.sequence,
                entry.task_key,
                entry.task.requirements.timeout_secs,
                entry.resumed_from_checkpoint,
            )
        };

        if !owner && !adoptable_by_sender {
            tracing::debug!(
                task_id = %task_id,
                worker_id = %worker_id,
                session_id = ?session_id,
                state = ?state,
                "Dropping result from a worker/session the task is not assigned to"
            );
            return Ok((
                ResultOutcome {
                    disposition: ResultDisposition::Stale,
                    state,
                    rejected: false,
                    adopted_from_hold: false,
                    task_key,
                    timeout_secs,
                    resumed_from_checkpoint,
                },
                None,
            ));
        }

        if adoptable_by_sender {
            inner.remove_ready(&QueueOrderKey {
                priority_rev: Reverse(priority),
                sequence,
                task_id,
            });
            if let Some(entry) = inner.tasks.get_mut(&task_id) {
                entry.state = TaskState::Running;
                entry.assigned_worker_id = Some(worker_id);
                entry.assigned_session = session_id;
                entry.started_instant = Some(now_instant);
                entry.adoptable_by = None;
                entry.fail_if_not_adopted = false;
                entry.pending_failover_charge = false;
                entry.pending_failover_reason = None;
            }
            inner.track_adoption_hold(task_id, false);
            inner.append_non_durable(&JournalRecord::TaskAdopted {
                task_id,
                worker_id,
                timestamp_utc: now_epoch,
            });
        }

        // Remove from worker assignment tracking
        if let Some(tasks) = inner.worker_assignments.get_mut(&worker_id) {
            tasks.remove(&task_id);
        }
        inner.refresh_worker_resume_protection(worker_id);

        // If task is already terminal (e.g. Cancelled by master directive, Completed, Failed), preserve state
        if state.is_terminal() {
            tracing::debug!(task_id = %task_id, state = ?state, "Ignoring result for terminal task");
            return Ok((
                ResultOutcome {
                    disposition: ResultDisposition::Late,
                    state,
                    rejected: false,
                    adopted_from_hold: false,
                    task_key,
                    timeout_secs,
                    resumed_from_checkpoint,
                },
                None,
            ));
        }

        let rejected = is_worker_rejection(&result);

        let mut delay_key_to_insert = None;
        let mut ready_key_to_insert = None;

        let (res_state, disposition, journal_record) = {
            let entry = inner
                .tasks
                .get_mut(&task_id)
                .ok_or(GridError::TaskNotFound(task_id.0))?;

            if result.is_success() {
                if !entry.state.can_transition_to(TaskState::Completed) {
                    return Err(GridError::InvalidTaskState {
                        task_id: task_id.0,
                        current: format!("{:?}", entry.state),
                        attempted: "Completed".into(),
                    });
                }
                entry.state = TaskState::Completed;
                entry.result = Some(result.clone());
                entry.cold_result = None;
                let record = JournalRecord::TaskTerminal {
                    task_id,
                    state: TaskState::Completed,
                    assigned_worker_id: entry.assigned_worker_id,
                    retry_count: entry.retry_count,
                    failover_count: entry.failover_count,
                    evacuation_count: entry.evacuation_count,
                    rejection_count: entry.rejection_count,
                    retry_attempt: None,
                    result: Some(result),
                    error_message: entry.error_message.clone(),
                    timestamp_utc: now_epoch,
                };
                (TaskState::Completed, ResultDisposition::Terminal, record)
            } else if result.exit_code == 130 {
                // Task cancellation from worker
                entry.state = TaskState::Cancelled;
                entry.error_message = result.error.clone().or_else(|| Some("Cancelled".into()));
                entry.result = Some(result.clone());
                entry.cold_result = None;
                let record = JournalRecord::TaskTerminal {
                    task_id,
                    state: TaskState::Cancelled,
                    assigned_worker_id: entry.assigned_worker_id,
                    retry_count: entry.retry_count,
                    failover_count: entry.failover_count,
                    evacuation_count: entry.evacuation_count,
                    rejection_count: entry.rejection_count,
                    retry_attempt: None,
                    result: Some(result),
                    error_message: entry.error_message.clone(),
                    timestamp_utc: now_epoch,
                };
                (TaskState::Cancelled, ResultDisposition::Terminal, record)
            } else if rejected
                && policy.retry_on_worker_disconnect
                && entry.rejection_count < MAX_WORKER_REJECTIONS_PER_TASK
                && policy.failover_budget(entry).0
            {
                // The worker refused to run it: that is the worker's problem, not the task's. Hand
                // the task to somebody else without charging its execution retry budget.
                entry.rejection_count += 1;
                entry.failover_count += 1;
                entry.retry_count += 1;
                if !entry.avoid_workers.contains(&worker_id) {
                    entry.avoid_workers.push(worker_id);
                }
                let attempt = RetryAttempt {
                    attempt: entry.retry_count,
                    timestamp_utc: now_epoch,
                    failed_worker_id: Some(worker_id),
                    reason: "Worker rejected the task without running it".into(),
                    exit_code: Some(result.exit_code),
                    error: result.error.clone(),
                };
                entry.retry_history.push(attempt.clone());
                entry.state = TaskState::Queued;
                entry.assigned_worker_id = None;
                entry.assigned_session = None;
                entry.scheduled_instant = None;
                entry.started_instant = None;
                ready_key_to_insert = Some(QueueOrderKey {
                    priority_rev: Reverse(entry.priority),
                    sequence: entry.sequence,
                    task_id,
                });
                let record = JournalRecord::TaskRequeued {
                    task_id,
                    state: TaskState::Queued,
                    retry_count: entry.retry_count,
                    failover_count: entry.failover_count,
                    evacuation_count: entry.evacuation_count,
                    rejection_count: entry.rejection_count,
                    avoid_workers: entry.avoid_workers.clone(),
                    retry_attempt: Some(attempt),
                    retry_delay_ms: Some(0),
                };
                (TaskState::Queued, ResultDisposition::Requeued, record)
            } else if policy.retry_on_execution_failure
                && entry.retry_count.saturating_sub(entry.failover_count) < policy.max_retries
            {
                entry.retry_count += 1;
                let delay_ms = policy.calculate_backoff(entry.retry_count);
                let retry_after = now_instant + Duration::from_millis(delay_ms);

                entry.state = TaskState::Retrying;
                entry.assigned_worker_id = None;
                entry.assigned_session = None;
                entry.retry_after_instant = Some(retry_after);
                let attempt = RetryAttempt {
                    attempt: entry.retry_count,
                    timestamp_utc: now_epoch,
                    failed_worker_id: Some(worker_id),
                    reason: "Non-zero exit code execution failure".into(),
                    exit_code: Some(result.exit_code),
                    error: result.error.clone(),
                };
                entry.retry_history.push(attempt.clone());

                delay_key_to_insert = Some(DelayedRetryKey {
                    retry_after,
                    task_id,
                });

                let record = JournalRecord::TaskRequeued {
                    task_id,
                    state: TaskState::Retrying,
                    retry_count: entry.retry_count,
                    failover_count: entry.failover_count,
                    evacuation_count: entry.evacuation_count,
                    rejection_count: entry.rejection_count,
                    avoid_workers: entry.avoid_workers.clone(),
                    retry_attempt: Some(attempt),
                    retry_delay_ms: Some(delay_ms),
                };
                (TaskState::Retrying, ResultDisposition::Requeued, record)
            } else {
                // Unrecoverable or exhausted failure
                entry.state = TaskState::Failed;
                entry.error_message = result.error.clone();
                entry.result = Some(result.clone());
                entry.cold_result = None;
                let record = JournalRecord::TaskTerminal {
                    task_id,
                    state: TaskState::Failed,
                    assigned_worker_id: entry.assigned_worker_id,
                    retry_count: entry.retry_count,
                    failover_count: entry.failover_count,
                    evacuation_count: entry.evacuation_count,
                    rejection_count: entry.rejection_count,
                    retry_attempt: None,
                    result: Some(result),
                    error_message: entry.error_message.clone(),
                    timestamp_utc: now_epoch,
                };
                (TaskState::Failed, ResultDisposition::Terminal, record)
            }
        };

        if let Some(key) = delay_key_to_insert {
            inner.delayed_retries.insert(key);
        }
        if let Some(key) = ready_key_to_insert {
            inner.insert_ready(key);
        }
        inner.sync_workspace_active(task_id);

        if res_state.is_terminal() {
            if !rejected && matches!(res_state, TaskState::Completed | TaskState::Failed) {
                if let Some(ws_name) = inner
                    .tasks
                    .get(&task_id)
                    .and_then(|e| e.task.workspace.as_ref().map(|ws| ws.name.clone()))
                {
                    inner.record_workspace_affinity(&ws_name, worker_id, now_epoch);
                }
            }
            inner.track_terminal_task(task_id);
        } else {
            inner.update_active_task(task_id);
        }

        #[cfg(feature = "otel")]
        {
            let attempt = inner
                .tasks
                .get(&task_id)
                .map_or(0, |e| i64::from(e.retry_count));
            let outcome = match res_state {
                TaskState::Completed => "completed",
                TaskState::Failed => "failed",
                TaskState::Cancelled => "cancelled",
                TaskState::Queued | TaskState::Retrying => "requeued",
                _ => "other",
            };
            let _result_span = tracing::info_span!(
                target: crate::otel::OTEL_SPAN_TARGET,
                "result",
                task_id = %task_id,
                worker_id = %worker_id,
                exit_code = exit_code,
                attempt = attempt,
                outcome = outcome,
            );
        }

        let ack = inner.append_durable_warn(&journal_record);
        inner.maybe_trigger_compaction();

        Ok((
            ResultOutcome {
                disposition,
                state: res_state,
                rejected,
                adopted_from_hold: adoptable_by_sender,
                task_key,
                timeout_secs,
                resumed_from_checkpoint,
            },
            ack,
        ))
    }

    /// Fails a task terminally when its assignment message exceeds `MAX_FRAME_SIZE`
    /// (`ProtocolError::FrameTooLarge`). Never retries, even when `retry_on_execution_failure` is
    /// enabled, because the assignment itself cannot fit a protocol frame.
    pub async fn fail_unsendable_assignment(
        &self,
        worker_id: Uuid,
        session_id: Option<u64>,
        result: TaskResult,
    ) -> GridResult<ResultDisposition> {
        let task_id = result.task_id;
        let ack = {
            let mut inner = self.inner.write().await;

            let (state, owner) = {
                let entry = inner
                    .tasks
                    .get(&task_id)
                    .ok_or(GridError::TaskNotFound(task_id.0))?;
                (entry.state, owned_by(entry, &worker_id, session_id))
            };

            if !owner {
                return Ok(ResultDisposition::Stale);
            }

            if let Some(tasks) = inner.worker_assignments.get_mut(&worker_id) {
                tasks.remove(&task_id);
            }
            inner.refresh_worker_resume_protection(worker_id);

            if state.is_terminal() {
                return Ok(ResultDisposition::Late);
            }

            let now_epoch = Utc::now().timestamp() as u64;
            let record = {
                let entry = inner
                    .tasks
                    .get_mut(&task_id)
                    .ok_or(GridError::TaskNotFound(task_id.0))?;
                if !entry.state.can_transition_to(TaskState::Failed) {
                    return Err(GridError::InvalidTaskState {
                        task_id: task_id.0,
                        current: format!("{:?}", entry.state),
                        attempted: "Failed".into(),
                    });
                }
                entry.state = TaskState::Failed;
                entry.error_message = result.error.clone();
                entry.result = Some(result.clone());
                entry.cold_result = None;
                JournalRecord::TaskTerminal {
                    task_id,
                    state: TaskState::Failed,
                    assigned_worker_id: entry.assigned_worker_id,
                    retry_count: entry.retry_count,
                    failover_count: entry.failover_count,
                    evacuation_count: entry.evacuation_count,
                    rejection_count: entry.rejection_count,
                    retry_attempt: None,
                    result: Some(result),
                    error_message: entry.error_message.clone(),
                    timestamp_utc: now_epoch,
                }
            };

            inner.sync_workspace_active(task_id);
            inner.track_terminal_task(task_id);
            let ack = inner.append_durable_warn(&record);
            inner.maybe_trigger_compaction();
            ack
        };

        if let Some(ack) = ack {
            if let Err(err) = ack.await {
                tracing::warn!(error = %err, "Failed to sync unsendable assignment failure record");
            }
        }
        self.flush_cold_maintenance().await;
        Ok(ResultDisposition::Terminal)
    }

    /// Fails a queued task terminally when no registered worker in the cluster can satisfy its
    /// capabilities or placement constraints (`SchedulerConfig::fail_unfulfillable_tasks`), or
    /// when its placement constraints are syntactically invalid.
    ///
    /// Returns `Some(TaskResult)` when the task transitioned from `Queued` to `Failed`, or `None`
    /// if the task was already scheduled, cancelled, or finished in the meantime.
    pub async fn fail_unfulfillable_task(
        &self,
        task_id: &TaskId,
        reason: String,
    ) -> Option<TaskResult> {
        let (result, ack) = {
            let mut inner = self.inner.write().await;
            let (priority, sequence) = {
                let entry = inner.tasks.get(task_id)?;
                if entry.state != TaskState::Queued {
                    return None;
                }
                (entry.priority, entry.sequence)
            };

            inner.remove_ready(&QueueOrderKey {
                priority_rev: Reverse(priority),
                sequence,
                task_id: *task_id,
            });

            let result =
                TaskResult::failure(Uuid::nil(), *task_id, 1, "", "", 0, Some(reason.clone()));
            let now_epoch = Utc::now().timestamp() as u64;

            let mut record = None;
            let mut prev_hold_worker = None;
            if let Some(entry) = inner.tasks.get_mut(task_id) {
                prev_hold_worker = entry.adoptable_by.map(|(w, _)| w);
                entry.state = TaskState::Failed;
                entry.adoptable_by = None;
                entry.fail_if_not_adopted = false;
                entry.pending_failover_charge = false;
                entry.pending_failover_reason = None;
                entry.error_message = Some(reason.clone());
                entry.result = Some(result.clone());
                entry.cold_result = None;
                record = Some(JournalRecord::TaskTerminal {
                    task_id: *task_id,
                    state: TaskState::Failed,
                    assigned_worker_id: entry.assigned_worker_id,
                    retry_count: entry.retry_count,
                    failover_count: entry.failover_count,
                    evacuation_count: entry.evacuation_count,
                    rejection_count: entry.rejection_count,
                    retry_attempt: None,
                    result: Some(result.clone()),
                    error_message: Some(reason),
                    timestamp_utc: now_epoch,
                });
            }
            inner.track_adoption_hold(*task_id, false);
            if let Some(wid) = prev_hold_worker {
                inner.refresh_worker_resume_protection(wid);
            }
            inner.track_terminal_task(*task_id);
            let ack = record.and_then(|r| inner.append_durable_warn(&r));
            inner.maybe_trigger_compaction();
            (result, ack)
        };

        if let Some(ack) = ack {
            let _ = ack.await;
        }
        self.flush_cold_maintenance().await;
        Some(result)
    }

    // --- Worker Disconnect & Failover ---

    /// Returns all active tasks assigned to a draining worker (`WorkerMessage::Disconnecting`) to
    /// `Queued` immediately (without charging `retry_count` or `failover_count` while under
    /// [`RetryPolicy::max_evacuations`](super::RetryPolicy::max_evacuations), or via the charged failover path past that cap).
    /// See [`handle_session_evacuated`](Self::handle_session_evacuated).
    pub async fn handle_worker_evacuated(&self, worker_id: &Uuid, reason: &str) -> Vec<TaskId> {
        self.handle_session_evacuated(worker_id, None, reason).await
    }

    /// Handles the in-flight tasks of one draining worker session (`WorkerMessage::Disconnecting`).
    ///
    /// While `entry.evacuation_count < policy.max_evacuations`, `policy.retry_on_worker_disconnect`
    /// is enabled, and the task is not `Running` under `max_retries == 0`, the drain increments
    /// `evacuation_count`, records an audit entry in `retry_history`, and returns the task to
    /// `Queued` (`ready_queue`, 0 ms delay) without charging `retry_count` or `failover_count`.
    /// Once `evacuation_count >= max_evacuations` (or when `max_retries == 0` on a `Running` task,
    /// or `retry_on_worker_disconnect == false`), the drain takes the charged failover path so a
    /// task cannot be re-dispatched without limit.
    pub async fn handle_session_evacuated(
        &self,
        worker_id: &Uuid,
        session_id: Option<u64>,
        reason: &str,
    ) -> Vec<TaskId> {
        let (affected, had_terminal, acks) = {
            let mut guard = self.inner.write().await;
            let inner = &mut *guard;
            if inner.store_closed {
                return Vec::new();
            }

            let mut assigned: HashSet<TaskId> = inner
                .worker_assignments
                .get(worker_id)
                .cloned()
                .unwrap_or_default();

            for (id, entry) in &inner.tasks {
                if entry.assigned_worker_id == Some(*worker_id) && entry.state.is_active() {
                    assigned.insert(*id);
                }
            }

            let now_epoch = Utc::now().timestamp() as u64;
            let policy = inner.retry_policy.clone();

            let mut affected = Vec::with_capacity(assigned.len());
            let mut ready_to_insert = Vec::new();
            let mut terminal_to_track = Vec::new();
            let mut records = Vec::new();

            for task_id in assigned {
                let Some(entry) = inner.tasks.get_mut(&task_id) else {
                    continue;
                };
                let session_matches = match (session_id, entry.assigned_session) {
                    (Some(swept), Some(assigned)) => assigned <= swept,
                    _ => true,
                };
                if !entry.state.is_active()
                    || entry.assigned_worker_id != Some(*worker_id)
                    || !session_matches
                {
                    continue;
                }

                affected.push(task_id);
                if let Some(tasks) = inner.worker_assignments.get_mut(worker_id) {
                    tasks.remove(&task_id);
                }

                let running_with_zero_retries =
                    entry.state == TaskState::Running && policy.zero_retries_for(entry);
                if policy.retry_on_worker_disconnect
                    && !running_with_zero_retries
                    && entry.evacuation_count < policy.max_evacuations
                {
                    entry.evacuation_count += 1;
                    tracing::debug!(
                        task_id = %task_id,
                        worker_id = %worker_id,
                        session_id = ?session_id,
                        evacuation_count = entry.evacuation_count,
                        max_evacuations = policy.max_evacuations,
                        reason = %reason,
                        "Returning task from announced worker drain to ready_queue without charging retry budget"
                    );
                    let attempt = RetryAttempt {
                        attempt: entry.retry_count,
                        timestamp_utc: now_epoch,
                        failed_worker_id: Some(*worker_id),
                        reason: reason.to_string(),
                        exit_code: None,
                        error: Some(format!(
                            "Worker draining ({reason}); uncharged evacuation {}/{}",
                            entry.evacuation_count, policy.max_evacuations
                        )),
                    };
                    entry.retry_history.push(attempt.clone());
                    entry.state = TaskState::Queued;
                    entry.assigned_worker_id = None;
                    entry.assigned_session = None;
                    entry.scheduled_instant = None;
                    entry.started_instant = None;
                    entry.retry_after_instant = None;
                    ready_to_insert.push(QueueOrderKey {
                        priority_rev: Reverse(entry.priority),
                        sequence: entry.sequence,
                        task_id,
                    });
                    records.push(JournalRecord::TaskRequeued {
                        task_id,
                        state: TaskState::Queued,
                        retry_count: entry.retry_count,
                        failover_count: entry.failover_count,
                        evacuation_count: entry.evacuation_count,
                        rejection_count: entry.rejection_count,
                        avoid_workers: entry.avoid_workers.clone(),
                        retry_attempt: Some(attempt),
                        retry_delay_ms: Some(0),
                    });
                } else {
                    // Past `max_evacuations` (or `Running` with `max_retries == 0`, or
                    // `!retry_on_worker_disconnect`): take the charged path with 0 ms reschedule delay.
                    let (budget_left, cap) = policy.failover_budget(entry);
                    if policy.retry_on_worker_disconnect
                        && !running_with_zero_retries
                        && budget_left
                    {
                        entry.retry_count += 1;
                        entry.failover_count += 1;
                        entry.assigned_worker_id = None;
                        entry.assigned_session = None;
                        let attempt = RetryAttempt {
                            attempt: entry.retry_count,
                            timestamp_utc: now_epoch,
                            failed_worker_id: Some(*worker_id),
                            reason: reason.to_string(),
                            exit_code: None,
                            error: Some(format!("Worker evicted ({reason})")),
                        };
                        entry.retry_history.push(attempt.clone());
                        entry.state = TaskState::Queued;
                        entry.scheduled_instant = None;
                        entry.started_instant = None;
                        entry.retry_after_instant = None;
                        ready_to_insert.push(QueueOrderKey {
                            priority_rev: Reverse(entry.priority),
                            sequence: entry.sequence,
                            task_id,
                        });
                        records.push(JournalRecord::TaskRequeued {
                            task_id,
                            state: TaskState::Queued,
                            retry_count: entry.retry_count,
                            failover_count: entry.failover_count,
                            evacuation_count: entry.evacuation_count,
                            rejection_count: entry.rejection_count,
                            avoid_workers: entry.avoid_workers.clone(),
                            retry_attempt: Some(attempt),
                            retry_delay_ms: Some(0),
                        });
                    } else {
                        let used = if entry.task.requirements.max_retries.is_some() {
                            entry.retry_count
                        } else {
                            entry.failover_count
                        };
                        entry.state = TaskState::Failed;
                        entry.assigned_worker_id = None;
                        entry.assigned_session = None;
                        let err_msg = format!(
                            "Task execution failed: maximum retry limit ({cap}) reached after worker eviction (Worker disconnected and retries exhausted ({used}/{cap}), last failure on worker {worker_id}: {reason})"
                        );
                        entry.error_message = Some(err_msg.clone());
                        let fail_result = TaskResult::failure(
                            *worker_id,
                            task_id,
                            1,
                            "",
                            "",
                            0,
                            Some(err_msg.clone()),
                        );
                        entry.result = Some(fail_result.clone());
                        entry.cold_result = None;
                        terminal_to_track.push(task_id);
                        records.push(JournalRecord::TaskTerminal {
                            task_id,
                            state: TaskState::Failed,
                            assigned_worker_id: None,
                            retry_count: entry.retry_count,
                            failover_count: entry.failover_count,
                            evacuation_count: entry.evacuation_count,
                            rejection_count: entry.rejection_count,
                            retry_attempt: None,
                            result: Some(fail_result),
                            error_message: Some(err_msg),
                            timestamp_utc: now_epoch,
                        });
                    }
                }
            }

            if inner
                .worker_assignments
                .get(worker_id)
                .is_some_and(|tasks| tasks.is_empty())
            {
                inner.worker_assignments.remove(worker_id);
            }

            for key in ready_to_insert {
                let tid = key.task_id;
                inner.insert_ready(key);
                inner.update_active_task(tid);
                inner.sync_workspace_active(tid);
            }
            let had_terminal = !terminal_to_track.is_empty();
            for tid in terminal_to_track {
                inner.sync_workspace_active(tid);
                inner.track_terminal_task(tid);
            }
            if !inner.worker_assignments.contains_key(worker_id) {
                let mut tracker = inner
                    .session_resume_tracker
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                let clear = match session_id {
                    Some(swept) => tracker
                        .connected
                        .get(worker_id)
                        .is_some_and(|&cur| cur <= swept),
                    None => true,
                };
                if clear {
                    tracker.connected.remove(worker_id);
                }
            }
            inner.refresh_worker_resume_protection(*worker_id);

            let mut acks = Vec::with_capacity(records.len());
            for record in &records {
                if let Some(ack) = inner.append_durable_warn(record) {
                    acks.push(ack);
                }
            }
            inner.maybe_trigger_compaction();

            (affected, had_terminal, acks)
        };

        for ack in acks {
            let _ = ack.await;
        }
        if had_terminal {
            self.flush_cold_maintenance().await;
        }

        affected
    }

    /// Reaps all tasks assigned to a disconnected or timed-out worker, whatever session they were
    /// dispatched on. See [`handle_session_disconnected`](Self::handle_session_disconnected).
    pub async fn handle_worker_disconnected(
        &self,
        worker_id: &Uuid,
        reason: &str,
        immediate_reschedule: bool,
    ) -> Vec<TaskId> {
        self.handle_session_disconnected(worker_id, None, reason, immediate_reschedule)
            .await
    }

    /// Reaps the tasks of one connection of a disconnected, timed-out or superseded worker.
    ///
    /// Active tasks (`Scheduled` or `Running`) assigned to `worker_id` are transitioned to
    /// `Retrying` or `Queued` (if their failover budget remains) or `Failed`. With
    /// `session_id = Some(s)` only tasks dispatched on session `s`, on an older session or on an
    /// unknown session are touched, so the tasks of a newer session of the same worker survive a
    /// late sweep of an older one. Taking the older sessions along is what makes the sweeps heal
    /// each other: when a connection is aborted before its own sweep ran (the worker id
    /// re-registered a third time, the reaper fired), the next sweep of that worker still recovers
    /// the tasks it left on a session that no longer exists.
    /// If `immediate_reschedule` is true (e.g. superseded session), tasks are
    /// re-enqueued directly to `ready_queue` with 0ms delay.
    /// Otherwise, exponential backoff is scheduled via `delayed_retries`.
    /// Infrastructure failovers are counted in `failover_count` against `max_failovers`,
    /// not against the task's execution retry budget; announced drains use
    /// [`handle_session_evacuated`](Self::handle_session_evacuated) instead so neither counter is charged.
    /// Returns the list of affected TaskIds.
    pub async fn handle_session_disconnected(
        &self,
        worker_id: &Uuid,
        session_id: Option<u64>,
        reason: &str,
        immediate_reschedule: bool,
    ) -> Vec<TaskId> {
        let (affected, had_terminal, acks) = {
            let mut guard = self.inner.write().await;
            let inner = &mut *guard;
            if inner.store_closed {
                return Vec::new();
            }
            let sweep = Self::sweep_worker_tasks(
                inner,
                worker_id,
                session_id,
                reason,
                immediate_reschedule,
                false,
            );
            let acks = Self::append_sweep_records(inner, &sweep.records);
            inner.maybe_trigger_compaction();
            (sweep.affected, sweep.had_terminal, acks)
        };

        for ack in acks {
            let _ = ack.await;
        }
        if had_terminal {
            self.flush_cold_maintenance().await;
        }

        affected
    }

    /// Takes back every task of `worker_ids`, the workers of a revoked member (design 0051 D5,
    /// revocation step 3), and returns the affected task ids of each worker.
    ///
    /// Unlike [`handle_session_disconnected`](Self::handle_session_disconnected):
    /// - the tasks are requeued at once, never held for a session resume (the member cannot come
    ///   back), and tasks already held for these workers by an earlier disconnect or a master
    ///   restart are released from their hold now;
    /// - the checkpoint of each of these tasks is dropped, so no later attempt resumes from state
    ///   the revoked machine wrote, and so is the checkpoint of a queued or retrying task whose
    ///   last failed attempt ran on one of these workers. The store has no record that clears a
    ///   checkpoint, so when one was dropped the queue compacts its store once, under the same
    ///   lock, and the snapshot no longer holds it.
    ///
    /// Tasks whose failover budget is spent fail as on any disconnect.
    pub async fn handle_workers_revoked(
        &self,
        worker_ids: &[Uuid],
        reason: &str,
    ) -> Vec<(Uuid, Vec<TaskId>)> {
        for worker_id in worker_ids {
            self.set_worker_session_resume(*worker_id, false);
        }
        let (swept, had_terminal, acks) = {
            let mut guard = self.inner.write().await;
            let inner = &mut *guard;
            if inner.store_closed {
                return Vec::new();
            }
            let mut swept = Vec::with_capacity(worker_ids.len());
            let mut records = Vec::new();
            let mut had_terminal = false;
            let mut dropped_checkpoints = 0usize;
            for worker_id in worker_ids {
                let sweep = Self::sweep_worker_tasks(inner, worker_id, None, reason, true, true);
                records.extend(sweep.records);
                had_terminal |= sweep.had_terminal;
                dropped_checkpoints += sweep.dropped_checkpoints;
                swept.push((*worker_id, sweep.affected));
            }
            let mut acks = Self::append_sweep_records(inner, &records);
            if dropped_checkpoints > 0 {
                tracing::info!(
                    workers = worker_ids.len(),
                    dropped_checkpoints,
                    "Dropped the checkpoints of the tasks a revoked member's workers held"
                );
                // No journal record clears a checkpoint, so a replay would bring the dropped ones
                // back: write one snapshot without them, after this sweep's own records.
                if let Some(store) = inner.store.as_ref() {
                    match store.compact(inner.snapshot_image()) {
                        Ok(ack) => acks.push(ack),
                        Err(err) => tracing::warn!(
                            error = %err,
                            "Failed to compact the task store after dropping checkpoints"
                        ),
                    }
                }
            } else {
                inner.maybe_trigger_compaction();
            }
            (swept, had_terminal, acks)
        };

        for ack in acks {
            let _ = ack.await;
        }
        if had_terminal {
            self.flush_cold_maintenance().await;
        }

        swept
    }

    fn append_sweep_records(
        inner: &mut TaskQueueInner,
        records: &[JournalRecord],
    ) -> Vec<DurableAck> {
        records
            .iter()
            .filter_map(|record| inner.append_durable_warn(record))
            .collect()
    }

    /// The tasks of one worker that a disconnect (or, with `revoked`, a revocation) takes back,
    /// swept under the queue lock the caller holds. The caller appends `records` and compacts.
    fn sweep_worker_tasks(
        inner: &mut TaskQueueInner,
        worker_id: &Uuid,
        session_id: Option<u64>,
        reason: &str,
        immediate_reschedule: bool,
        revoked: bool,
    ) -> WorkerSweep {
        let mut assigned: HashSet<TaskId> = inner
            .worker_assignments
            .get(worker_id)
            .cloned()
            .unwrap_or_default();

        // Defense-in-depth: Also capture any active task in inner.tasks assigned to this worker
        for (id, entry) in &inner.tasks {
            if entry.assigned_worker_id == Some(*worker_id) && entry.state.is_active() {
                assigned.insert(*id);
            }
        }

        let now_instant = Instant::now();
        let now_epoch = Utc::now().timestamp() as u64;
        let policy = inner.retry_policy.clone();
        let hold_for_resume = !revoked
            && !inner.session_resume_hold.is_zero()
            && inner
                .session_resume_workers
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains(worker_id);
        let hold_duration = inner.session_resume_hold;

        let mut affected = Vec::with_capacity(assigned.len());
        let mut delays_to_insert = Vec::new();
        let mut ready_to_insert = Vec::new();
        let mut holds_to_track = Vec::new();
        let mut terminal_to_track = Vec::new();
        let mut records = Vec::new();
        let mut dropped_checkpoints = 0usize;

        for task_id in assigned {
            let Some(entry) = inner.tasks.get_mut(&task_id) else {
                continue;
            };
            // Session ids only grow, and a newer session of a worker id aborts every older one,
            // so a task on an older session than the swept one is stranded for sure.
            let session_matches = match (session_id, entry.assigned_session) {
                (Some(swept), Some(assigned)) => assigned <= swept,
                _ => true,
            };
            if !entry.state.is_active()
                || entry.assigned_worker_id != Some(*worker_id)
                || !session_matches
            {
                continue;
            }

            affected.push(task_id);
            if revoked && entry.task.latest_checkpoint.take().is_some() {
                dropped_checkpoints += 1;
            }
            if let Some(tasks) = inner.worker_assignments.get_mut(worker_id) {
                tasks.remove(&task_id);
            }

            if hold_for_resume {
                let hold_until = now_instant + hold_duration;
                let zero_retries = policy.zero_retries_for(entry);
                entry.state = TaskState::Queued;
                entry.assigned_worker_id = None;
                entry.assigned_session = None;
                entry.scheduled_instant = None;
                entry.started_instant = None;
                entry.retry_after_instant = None;
                entry.adoptable_by = Some((*worker_id, hold_until));
                entry.fail_if_not_adopted = zero_retries;
                entry.pending_failover_charge = true;
                entry.pending_failover_reason = Some(reason.to_string());
                ready_to_insert.push(QueueOrderKey {
                    priority_rev: Reverse(entry.priority),
                    sequence: entry.sequence,
                    task_id,
                });
                holds_to_track.push(task_id);
                continue;
            }

            let (budget_left, cap) = policy.failover_budget(entry);
            if policy.retry_on_worker_disconnect && budget_left {
                entry.retry_count += 1;
                entry.failover_count += 1;
                let delay_ms = if immediate_reschedule {
                    0
                } else {
                    policy.calculate_backoff(entry.failover_count)
                };

                entry.assigned_worker_id = None;
                entry.assigned_session = None;
                let attempt = RetryAttempt {
                    attempt: entry.retry_count,
                    timestamp_utc: now_epoch,
                    failed_worker_id: Some(*worker_id),
                    reason: reason.to_string(),
                    exit_code: None,
                    error: Some(format!("Worker evicted ({reason})")),
                };
                entry.retry_history.push(attempt.clone());

                let next_state = if delay_ms == 0 {
                    // Immediate re-enqueue straight to ready_queue
                    entry.state = TaskState::Queued;
                    entry.scheduled_instant = None;
                    entry.started_instant = None;
                    entry.retry_after_instant = None;
                    ready_to_insert.push(QueueOrderKey {
                        priority_rev: Reverse(entry.priority),
                        sequence: entry.sequence,
                        task_id,
                    });
                    TaskState::Queued
                } else {
                    // Delayed retry with exponential backoff
                    let retry_after = now_instant + Duration::from_millis(delay_ms);
                    entry.state = TaskState::Retrying;
                    entry.retry_after_instant = Some(retry_after);
                    delays_to_insert.push(DelayedRetryKey {
                        retry_after,
                        task_id,
                    });
                    TaskState::Retrying
                };

                records.push(JournalRecord::TaskRequeued {
                    task_id,
                    state: next_state,
                    retry_count: entry.retry_count,
                    failover_count: entry.failover_count,
                    evacuation_count: entry.evacuation_count,
                    rejection_count: entry.rejection_count,
                    avoid_workers: entry.avoid_workers.clone(),
                    retry_attempt: Some(attempt),
                    retry_delay_ms: Some(delay_ms),
                });
            } else {
                // Terminal Failed state
                let used = if entry.task.requirements.max_retries.is_some() {
                    entry.retry_count
                } else {
                    entry.failover_count
                };
                entry.state = TaskState::Failed;
                entry.assigned_worker_id = None;
                entry.assigned_session = None;
                let err_msg = format!(
                        "Task execution failed: maximum retry limit ({cap}) reached after worker eviction (Worker disconnected and retries exhausted ({used}/{cap}), last failure on worker {worker_id}: {reason})"
                    );
                entry.error_message = Some(err_msg.clone());
                let fail_result =
                    TaskResult::failure(*worker_id, task_id, 1, "", "", 0, Some(err_msg.clone()));
                entry.result = Some(fail_result.clone());
                entry.cold_result = None;
                terminal_to_track.push(task_id);
                records.push(JournalRecord::TaskTerminal {
                    task_id,
                    state: TaskState::Failed,
                    assigned_worker_id: None,
                    retry_count: entry.retry_count,
                    failover_count: entry.failover_count,
                    evacuation_count: entry.evacuation_count,
                    rejection_count: entry.rejection_count,
                    retry_attempt: None,
                    result: Some(fail_result),
                    error_message: Some(err_msg),
                    timestamp_utc: now_epoch,
                });
            }
        }

        if inner
            .worker_assignments
            .get(worker_id)
            .is_some_and(|tasks| tasks.is_empty())
        {
            inner.worker_assignments.remove(worker_id);
        }

        for key in delays_to_insert {
            let tid = key.task_id;
            inner.delayed_retries.insert(key);
            inner.update_active_task(tid);
            inner.sync_workspace_active(tid);
        }
        for key in ready_to_insert {
            let tid = key.task_id;
            inner.insert_ready(key);
            inner.update_active_task(tid);
            inner.sync_workspace_active(tid);
        }
        for tid in holds_to_track {
            inner.track_adoption_hold(tid, true);
        }
        let mut had_terminal = !terminal_to_track.is_empty();
        for tid in terminal_to_track {
            inner.sync_workspace_active(tid);
            inner.track_terminal_task(tid);
        }
        if revoked {
            // Tasks an earlier disconnect or a master restart left held for this worker: the
            // hold ends now, and so does their checkpoint.
            let held: Vec<(TaskId, bool, bool)> = inner
                .held_tasks
                .iter()
                .filter_map(|tid| {
                    let entry = inner.tasks.get(tid)?;
                    (entry.state == TaskState::Queued
                        && entry.adoptable_by.is_some_and(|(w, _)| w == *worker_id))
                    .then_some((
                        *tid,
                        entry.fail_if_not_adopted,
                        entry.pending_failover_charge,
                    ))
                })
                .collect();
            let mut released_any = false;
            for (tid, fail_if_not_adopted, pending_failover_charge) in held {
                if let Some(entry) = inner.tasks.get_mut(&tid) {
                    if entry.task.latest_checkpoint.take().is_some() {
                        dropped_checkpoints += 1;
                    }
                }
                let (record, failed, released) = Self::release_unadopted_task(
                    inner,
                    tid,
                    Some(*worker_id),
                    fail_if_not_adopted,
                    pending_failover_charge,
                    now_epoch,
                );
                records.extend(record);
                if released {
                    inner.update_active_task(tid);
                    released_any = true;
                }
                had_terminal |= failed;
                if released || failed {
                    affected.push(tid);
                }
            }
            if released_any {
                inner.ready_epoch = inner.ready_epoch.wrapping_add(1);
            }

            // A queued or retrying task whose last failed attempt ran on this worker still
            // carries the checkpoint that attempt left (or an older one it resumed from).
            let mut retried_on_worker = Vec::new();
            for (tid, entry) in inner.tasks.iter_mut() {
                if matches!(entry.state, TaskState::Queued | TaskState::Retrying)
                    && entry.adoptable_by.is_none()
                    && entry
                        .retry_history
                        .last()
                        .is_some_and(|attempt| attempt.failed_worker_id == Some(*worker_id))
                    && entry.task.latest_checkpoint.take().is_some()
                {
                    dropped_checkpoints += 1;
                    retried_on_worker.push(*tid);
                }
            }
            for tid in retried_on_worker {
                inner.update_active_task(tid);
            }
        }
        if !inner.worker_assignments.contains_key(worker_id) {
            let mut tracker = inner
                .session_resume_tracker
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let clear = match session_id {
                Some(swept) => tracker
                    .connected
                    .get(worker_id)
                    .is_some_and(|&cur| cur <= swept),
                None => true,
            };
            if clear {
                tracker.connected.remove(worker_id);
            }
        }
        inner.refresh_worker_resume_protection(*worker_id);

        WorkerSweep {
            affected,
            had_terminal,
            records,
            dropped_checkpoints,
        }
    }

    // --- Backoff Advancement ---

    /// Scans delayed retries and transitions tasks whose backoff elapsed from `Retrying` to `Queued`.
    /// Returns count of tasks re-enqueued.
    pub async fn process_delayed_retries(&self) -> usize {
        let mut inner = self.inner.write().await;
        let now = Instant::now();

        let mut ready_tasks = Vec::new();
        while let Some(key) = inner.delayed_retries.first().copied() {
            if key.retry_after <= now {
                inner.delayed_retries.remove(&key);
                ready_tasks.push(key.task_id);
            } else {
                break;
            }
        }

        let count = ready_tasks.len();
        for task_id in ready_tasks {
            let key_and_record = {
                if let Some(entry) = inner.tasks.get_mut(&task_id) {
                    if entry.state == TaskState::Retrying {
                        entry.state = TaskState::Queued;
                        entry.retry_after_instant = None;
                        let record = JournalRecord::TaskRequeued {
                            task_id,
                            state: TaskState::Queued,
                            retry_count: entry.retry_count,
                            failover_count: entry.failover_count,
                            evacuation_count: entry.evacuation_count,
                            rejection_count: entry.rejection_count,
                            avoid_workers: entry.avoid_workers.clone(),
                            retry_attempt: None,
                            retry_delay_ms: None,
                        };
                        Some((
                            QueueOrderKey {
                                priority_rev: Reverse(entry.priority),
                                sequence: entry.sequence,
                                task_id,
                            },
                            record,
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                }
            };
            if let Some((k, record)) = key_and_record {
                inner.insert_ready(k);
                inner.append_non_durable(&record);
            }
        }
        if count > 0 {
            inner.maybe_trigger_compaction();
        }

        count
    }

    // --- Task Cancellation ---

    /// Cancels a task.
    ///
    /// If the task is active on a worker, returns `Some(worker_id)` so the caller
    /// can send `MasterMessage::CancelTask` to the assigned worker.
    pub async fn cancel_task(
        &self,
        task_id: &TaskId,
        reason: Option<String>,
    ) -> GridResult<Option<Uuid>> {
        self.cancel_task_detailed(task_id, reason)
            .await
            .map(|outcome| outcome.worker_to_notify)
    }

    /// Cancels a task and reports what the caller still has to do about it.
    ///
    /// A task that already finished is left untouched and reported as not newly cancelled.
    pub async fn cancel_task_detailed(
        &self,
        task_id: &TaskId,
        reason: Option<String>,
    ) -> GridResult<CancelOutcome> {
        let (outcome, ack) = {
            let mut inner = self.inner.write().await;
            let (state, priority_info, assigned_worker_id, retry_after) = {
                let entry = inner
                    .tasks
                    .get(task_id)
                    .ok_or(GridError::TaskNotFound(task_id.0))?;
                (
                    entry.state,
                    (entry.priority, entry.sequence),
                    entry.assigned_worker_id,
                    entry.retry_after_instant,
                )
            };

            if state.is_terminal() {
                let needs_cold = inner
                    .tasks
                    .get(task_id)
                    .is_some_and(|e| e.result.is_none() && e.cold_result.is_some());
                let cached = inner.tasks.get(task_id).and_then(|e| e.result.clone());
                drop(inner);
                let result = if needs_cold {
                    self.get_result(task_id).await
                } else {
                    cached
                };
                return Ok(CancelOutcome {
                    newly_cancelled: false,
                    worker_to_notify: None,
                    result,
                });
            }

            let worker_to_notify = match state {
                TaskState::Queued => {
                    inner.remove_ready(&QueueOrderKey {
                        priority_rev: Reverse(priority_info.0),
                        sequence: priority_info.1,
                        task_id: *task_id,
                    });
                    None
                }
                TaskState::Retrying => {
                    if let Some(after) = retry_after {
                        inner.delayed_retries.remove(&DelayedRetryKey {
                            retry_after: after,
                            task_id: *task_id,
                        });
                    }
                    None
                }
                TaskState::Scheduled | TaskState::Running => {
                    if let Some(wid) = assigned_worker_id {
                        if let Some(tasks) = inner.worker_assignments.get_mut(&wid) {
                            tasks.remove(task_id);
                        }
                    }
                    assigned_worker_id
                }
                TaskState::Submitted => None,
                _ => None,
            };

            let cancel_reason = reason.or_else(|| Some("Cancelled by user".into()));
            let cancel_result = TaskResult::failure(
                assigned_worker_id.unwrap_or_else(Uuid::nil),
                *task_id,
                130,
                "",
                "",
                0,
                cancel_reason.clone(),
            );
            let now_epoch = Utc::now().timestamp() as u64;

            let mut recorded = None;
            let mut record = None;
            let mut prev_hold_worker = None;
            if let Some(entry) = inner.tasks.get_mut(task_id) {
                prev_hold_worker = entry.adoptable_by.map(|(w, _)| w);
                entry.state = TaskState::Cancelled;
                entry.adoptable_by = None;
                entry.fail_if_not_adopted = false;
                entry.pending_failover_charge = false;
                entry.pending_failover_reason = None;
                entry.error_message = cancel_reason.clone();
                if entry.result.is_none() {
                    entry.result = Some(cancel_result);
                }
                entry.cold_result = None;
                recorded = entry.result.clone();
                record = Some(JournalRecord::TaskTerminal {
                    task_id: *task_id,
                    state: TaskState::Cancelled,
                    assigned_worker_id,
                    retry_count: entry.retry_count,
                    failover_count: entry.failover_count,
                    evacuation_count: entry.evacuation_count,
                    rejection_count: entry.rejection_count,
                    retry_attempt: None,
                    result: recorded.clone(),
                    error_message: entry.error_message.clone(),
                    timestamp_utc: now_epoch,
                });
            }
            inner.track_adoption_hold(*task_id, false);
            if let Some(wid) = assigned_worker_id {
                inner.refresh_worker_resume_protection(wid);
            }
            if let Some(wid) = prev_hold_worker {
                inner.refresh_worker_resume_protection(wid);
            }
            inner.track_terminal_task(*task_id);
            let ack = if let Some(ref r) = record {
                inner.append_durable_warn(r)
            } else {
                None
            };
            inner.maybe_trigger_compaction();

            (
                CancelOutcome {
                    newly_cancelled: true,
                    worker_to_notify,
                    result: recorded,
                },
                ack,
            )
        };

        if let Some(ack) = ack {
            if let Err(err) = ack.await {
                tracing::warn!(
                    task_id = %task_id.0,
                    error = %err,
                    "Failed to sync TaskTerminal journal record for cancelled task"
                );
            }
        }
        self.flush_cold_maintenance().await;

        Ok(outcome)
    }

    /// Cancels every non-terminal task originally submitted by `member_id` (`ENROLL-1c` step 2,
    /// design 0051 D5). Returns `(task_id, outcome)` for every task newly transitioned to
    /// `Cancelled` so callers can notify executing workers and wake waiting clients.
    pub async fn cancel_tasks_submitted_by(
        &self,
        member_id: &str,
        reason: &str,
    ) -> Vec<(TaskId, CancelOutcome)> {
        let target_ids: Vec<TaskId> = {
            let inner = self.inner.read().await;
            inner
                .tasks
                .values()
                .filter(|e| {
                    !e.state.is_terminal() && e.submitted_by_member.as_deref() == Some(member_id)
                })
                .map(|e| e.task.id)
                .collect()
        };
        let mut outcomes = Vec::with_capacity(target_ids.len());
        for tid in target_ids {
            if let Ok(outcome) = self
                .cancel_task_detailed(&tid, Some(reason.to_string()))
                .await
            {
                if outcome.newly_cancelled {
                    outcomes.push((tid, outcome));
                }
            }
        }
        outcomes
    }
}
