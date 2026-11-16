//! Session-resume tracking, journal recovery, adoption holds, and store compaction.

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use rusty_grid_core::error::GridResult;
use rusty_grid_core::task::{TaskId, TaskResult};
use uuid::Uuid;

use crate::cold_results::task_result_bytes;
use crate::fair_share::SubmitterId;
use crate::store::{DurableAck, JournalRecord, PersistedTaskEntry, Recovered, TaskStore};

use super::{
    is_worker_rejection, retained_bytes, task_has_tree_input, AdoptionOutcome, QueueOrderKey,
    RetryAttempt, TaskEntry, TaskQueue, TaskQueueInner, TaskState, MAX_SESSION_RESUME_WORKERS,
    UNRESUMED_ZERO_RETRY_ERROR,
};

impl TaskQueue {
    /// Configures the session-resume hold duration advertised in `RegisterAck` and applied to
    /// disconnected `SESSION_RESUME`-capable workers (`Duration::ZERO` disables session resume).
    pub async fn set_session_resume_hold(&self, hold: Duration) {
        let max_secs = rusty_grid_core::protocol::master_features::MAX_SESSION_RESUME_HOLD_SECS;
        let max_hold = Duration::from_secs(max_secs);
        if hold > max_hold {
            tracing::warn!(
                requested_secs = hold.as_secs(),
                max_secs = max_secs,
                "Requested session-resume hold exceeds maximum; clamping to {max_secs}s"
            );
        }
        let clamped_secs = hold.as_secs().min(max_secs);
        let clamped = if hold.is_zero() {
            Duration::ZERO
        } else {
            hold.min(max_hold)
        };
        let mut inner = self.inner.write().await;
        inner.session_resume_hold = clamped;
        self.session_resume_hold_secs
            .store(clamped_secs, std::sync::atomic::Ordering::Release);
    }

    /// Returns the configured session-resume hold duration in whole seconds (`0` when disabled).
    pub fn session_resume_hold_secs(&self) -> u64 {
        self.session_resume_hold_secs
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Records whether `worker_id`'s current registration supports `ProtocolCapabilities::SESSION_RESUME`.
    ///
    /// When the tracked set reaches [`MAX_SESSION_RESUME_WORKERS`], only the oldest worker that is
    /// not currently connected and has no active assignments or adoption holds is evicted.
    pub fn set_worker_session_resume(&self, worker_id: Uuid, enabled: bool) {
        let mut set = self
            .session_resume_workers
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut tracker = self
            .session_resume_tracker
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if enabled {
            if set.contains(&worker_id) {
                tracker.order.retain(|w| *w != worker_id);
                tracker.order.push_back(worker_id);
                return;
            }
            while set.len() >= MAX_SESSION_RESUME_WORKERS {
                let evict_pos = tracker.order.iter().position(|candidate| {
                    !tracker.connected.contains_key(candidate)
                        && !tracker.with_work_or_hold.contains(candidate)
                });
                let Some(pos) = evict_pos else {
                    break;
                };
                if let Some(evict) = tracker.order.remove(pos) {
                    set.remove(&evict);
                }
            }
            set.insert(worker_id);
            tracker.order.push_back(worker_id);
        } else {
            set.remove(&worker_id);
            tracker.connected.remove(&worker_id);
            tracker.order.retain(|w| *w != worker_id);
        }
    }

    /// Marks whether `session_id` of a `SESSION_RESUME`-capable worker is currently connected so
    /// the [`MAX_SESSION_RESUME_WORKERS`] bound never evicts a connected worker even while idle,
    /// and a late disconnect sweep for an older session never clears `connected` while a newer
    /// session of the same worker is live.
    pub fn set_worker_session_resume_connected(
        &self,
        worker_id: Uuid,
        session_id: u64,
        connected: bool,
    ) {
        let mut tracker = self
            .session_resume_tracker
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if connected {
            tracker
                .connected
                .entry(worker_id)
                .and_modify(|cur| *cur = (*cur).max(session_id))
                .or_insert(session_id);
        } else if tracker
            .connected
            .get(&worker_id)
            .is_some_and(|&cur| cur <= session_id)
        {
            tracker.connected.remove(&worker_id);
        }
    }

    fn fail_unadopted_zero_retry_task(
        inner: &mut TaskQueueInner,
        task_id: TaskId,
        last_worker_id: Option<Uuid>,
        now_epoch: u64,
    ) -> Option<JournalRecord> {
        let (priority, sequence) = {
            let entry = inner.tasks.get(&task_id)?;
            if entry.state.is_terminal() {
                return None;
            }
            (entry.priority, entry.sequence)
        };
        inner.remove_ready(&QueueOrderKey {
            priority_rev: Reverse(priority),
            sequence,
            task_id,
        });
        let msg = UNRESUMED_ZERO_RETRY_ERROR.to_string();
        let fail_result = TaskResult::failure(
            last_worker_id.unwrap_or_else(Uuid::nil),
            task_id,
            1,
            "",
            "",
            0,
            Some(msg.clone()),
        );
        let record = {
            let entry = inner.tasks.get_mut(&task_id)?;
            entry.state = TaskState::Failed;
            entry.assigned_worker_id = None;
            entry.assigned_session = None;
            entry.adoptable_by = None;
            entry.fail_if_not_adopted = false;
            entry.pending_failover_charge = false;
            entry.pending_failover_reason = None;
            entry.error_message = Some(msg.clone());
            entry.result = Some(fail_result.clone());
            JournalRecord::TaskTerminal {
                task_id,
                state: TaskState::Failed,
                assigned_worker_id: last_worker_id,
                retry_count: entry.retry_count,
                failover_count: entry.failover_count,
                evacuation_count: entry.evacuation_count,
                rejection_count: entry.rejection_count,
                retry_attempt: None,
                result: Some(fail_result),
                error_message: Some(msg),
                timestamp_utc: now_epoch,
            }
        };
        inner.track_adoption_hold(task_id, false);
        if let Some(wid) = last_worker_id {
            inner.refresh_worker_resume_protection(wid);
        }
        inner.track_terminal_task(task_id);
        Some(record)
    }

    /// Releases a held task that was either omitted from `ResumeSession` or whose `adoptable_by`
    /// deadline expired, returning `(optional_journal_record, became_failed, released_to_ready)`.
    fn release_unadopted_task(
        inner: &mut TaskQueueInner,
        task_id: TaskId,
        last_worker_id: Option<Uuid>,
        fail_if_not_adopted: bool,
        pending_failover_charge: bool,
        now_epoch: u64,
    ) -> (Option<JournalRecord>, bool, bool) {
        if pending_failover_charge {
            let policy = inner.retry_policy.clone();
            let Some(entry) = inner.tasks.get(&task_id) else {
                return (None, false, false);
            };
            if entry.state.is_terminal() {
                return (None, false, false);
            }
            let (budget_left, cap) = policy.failover_budget(entry);
            let can_retry =
                policy.retry_on_worker_disconnect && budget_left && !fail_if_not_adopted;
            let (priority, sequence) = (entry.priority, entry.sequence);

            if can_retry {
                let record = {
                    let entry = inner.tasks.get_mut(&task_id).expect("checked above");
                    let reason = entry
                        .pending_failover_reason
                        .take()
                        .unwrap_or_else(|| "session resume hold expired".into());
                    entry.retry_count += 1;
                    entry.failover_count += 1;
                    entry.adoptable_by = None;
                    entry.fail_if_not_adopted = false;
                    entry.pending_failover_charge = false;
                    entry.queued_instant = Instant::now();
                    let attempt = RetryAttempt {
                        attempt: entry.retry_count,
                        timestamp_utc: now_epoch,
                        failed_worker_id: last_worker_id,
                        reason: reason.clone(),
                        exit_code: None,
                        error: Some(format!("Worker evicted ({reason})")),
                    };
                    entry.retry_history.push(attempt.clone());
                    JournalRecord::TaskRequeued {
                        task_id,
                        state: TaskState::Queued,
                        retry_count: entry.retry_count,
                        failover_count: entry.failover_count,
                        evacuation_count: entry.evacuation_count,
                        rejection_count: entry.rejection_count,
                        avoid_workers: entry.avoid_workers.clone(),
                        retry_attempt: Some(attempt),
                        retry_delay_ms: Some(0),
                    }
                };
                inner.track_adoption_hold(task_id, false);
                if let Some(wid) = last_worker_id {
                    inner.refresh_worker_resume_protection(wid);
                }
                inner.update_active_task(task_id);
                return (Some(record), false, true);
            } else {
                inner.remove_ready(&QueueOrderKey {
                    priority_rev: Reverse(priority),
                    sequence,
                    task_id,
                });
                let worker_id = last_worker_id.unwrap_or_else(Uuid::nil);
                let record = {
                    let entry = inner.tasks.get_mut(&task_id).expect("checked above");
                    let reason = entry
                        .pending_failover_reason
                        .take()
                        .unwrap_or_else(|| "session resume hold expired".into());
                    let used = if entry.task.requirements.max_retries.is_some() {
                        entry.retry_count
                    } else {
                        entry.failover_count
                    };
                    entry.state = TaskState::Failed;
                    entry.assigned_worker_id = None;
                    entry.assigned_session = None;
                    entry.adoptable_by = None;
                    entry.fail_if_not_adopted = false;
                    entry.pending_failover_charge = false;
                    let err_msg = format!(
                        "Task execution failed: maximum retry limit ({cap}) reached after worker eviction (Worker disconnected and retries exhausted ({used}/{cap}), last failure on worker {worker_id}: {reason})"
                    );
                    entry.error_message = Some(err_msg.clone());
                    let fail_result = TaskResult::failure(
                        worker_id,
                        task_id,
                        1,
                        "",
                        "",
                        0,
                        Some(err_msg.clone()),
                    );
                    entry.result = Some(fail_result.clone());
                    JournalRecord::TaskTerminal {
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
                    }
                };
                inner.track_adoption_hold(task_id, false);
                if let Some(wid) = last_worker_id {
                    inner.refresh_worker_resume_protection(wid);
                }
                inner.track_terminal_task(task_id);
                return (Some(record), true, false);
            }
        }

        if fail_if_not_adopted {
            let record =
                Self::fail_unadopted_zero_retry_task(inner, task_id, last_worker_id, now_epoch);
            let failed = record.is_some();
            (record, failed, false)
        } else if let Some(entry) = inner.tasks.get_mut(&task_id) {
            entry.adoptable_by = None;
            entry.pending_failover_charge = false;
            entry.pending_failover_reason = None;
            entry.queued_instant = Instant::now();
            inner.track_adoption_hold(task_id, false);
            if let Some(wid) = last_worker_id {
                inner.refresh_worker_resume_protection(wid);
            }
            (None, false, true)
        } else {
            (None, false, false)
        }
    }

    /// Attaches a [`TaskStore`] and replays its [`Recovered`] snapshot and journal into this
    /// queue according to design 0046 D2.5.
    ///
    /// Tasks that were `Scheduled` or `Running` before the restart are placed in `Queued` without
    /// charging `retry_count`, `failover_count`, or `evacuation_count`. When `session_resume_hold`
    /// is non-zero and the task had an `assigned_worker_id`, `adoptable_by` is set to
    /// `(worker_id, Instant::now() + session_resume_hold)` so that worker can reclaim it via
    /// [`adopt_session_tasks`](Self::adopt_session_tasks). Tasks that were `Scheduled` or `Running`
    /// under effective `max_retries == 0` are marked `fail_if_not_adopted` and fail with
    /// `"master restarted while the task ran; not re-run because max_retries is 0"` if not
    /// adopted before `session_resume_hold` elapses (immediately when `session_resume_hold` is
    /// `Duration::ZERO`).
    pub async fn attach_store_and_replay(
        &self,
        store: TaskStore,
        recovered: Recovered,
        session_resume_hold: Duration,
    ) -> GridResult<()> {
        let mut persisted: HashMap<TaskId, PersistedTaskEntry> = HashMap::new();
        let mut terminal_order_vec: Vec<TaskId> = Vec::new();
        let mut terminal_timestamps: HashMap<TaskId, u64> = HashMap::new();
        let mut max_seq: u64 = 1;

        if let Some(snapshot) = recovered.snapshot {
            max_seq = snapshot.next_sequence.max(1);
            for entry in snapshot.tasks {
                max_seq = max_seq.max(entry.sequence.saturating_add(1));
                persisted.insert(entry.task.id, entry);
            }
            terminal_order_vec = snapshot.terminal_order;
        }

        for record in recovered.records {
            match record {
                JournalRecord::TaskSubmitted {
                    task,
                    priority,
                    sequence,
                    submitted_at_utc,
                } => {
                    max_seq = max_seq.max(sequence.saturating_add(1));
                    let task_id = task.id;
                    persisted.insert(
                        task_id,
                        PersistedTaskEntry {
                            task: *task,
                            priority,
                            sequence,
                            state: TaskState::Queued,
                            assigned_worker_id: None,
                            submitted_at_utc,
                            retry_count: 0,
                            failover_count: 0,
                            evacuation_count: 0,
                            rejection_count: 0,
                            avoid_workers: Vec::new(),
                            adoptable_by_worker: None,
                            fail_if_not_adopted: false,
                            retry_history: Vec::new(),
                            result: None,
                            cold_result: None,
                            error_message: None,
                        },
                    );
                }
                JournalRecord::TaskDispatched {
                    task_id, worker_id, ..
                } => {
                    if let Some(entry) = persisted.get_mut(&task_id) {
                        if !entry.state.is_terminal() {
                            entry.state = TaskState::Scheduled;
                            entry.assigned_worker_id = Some(worker_id);
                            entry.adoptable_by_worker = None;
                            entry.fail_if_not_adopted = false;
                        }
                    }
                }
                JournalRecord::TaskRunning {
                    task_id, worker_id, ..
                } => {
                    if let Some(entry) = persisted.get_mut(&task_id) {
                        if !entry.state.is_terminal() {
                            entry.state = TaskState::Running;
                            entry.assigned_worker_id = Some(worker_id);
                            entry.adoptable_by_worker = None;
                            entry.fail_if_not_adopted = false;
                        }
                    }
                }
                JournalRecord::CheckpointRecorded {
                    task_id,
                    checkpoint,
                } => {
                    if let Some(entry) = persisted.get_mut(&task_id) {
                        if !entry.state.is_terminal()
                            && entry
                                .task
                                .latest_checkpoint
                                .as_ref()
                                .is_none_or(|c| checkpoint.sequence > c.sequence)
                        {
                            entry.task.update_checkpoint(checkpoint);
                        }
                    }
                }
                JournalRecord::TaskRequeued {
                    task_id,
                    state,
                    retry_count,
                    failover_count,
                    evacuation_count,
                    rejection_count,
                    avoid_workers,
                    retry_attempt,
                    ..
                } => {
                    if let Some(entry) = persisted.get_mut(&task_id) {
                        if !entry.state.is_terminal() {
                            entry.state = if state == TaskState::Retrying {
                                TaskState::Retrying
                            } else {
                                TaskState::Queued
                            };
                            entry.assigned_worker_id = None;
                            entry.adoptable_by_worker = None;
                            entry.fail_if_not_adopted = false;
                            entry.retry_count = retry_count;
                            entry.failover_count = failover_count;
                            entry.evacuation_count = evacuation_count;
                            entry.rejection_count = rejection_count;
                            entry.avoid_workers = avoid_workers;
                            if let Some(attempt) = retry_attempt {
                                entry.retry_history.push(attempt);
                            }
                        }
                    }
                }
                JournalRecord::TaskAdopted {
                    task_id, worker_id, ..
                } => {
                    if let Some(entry) = persisted.get_mut(&task_id) {
                        if !entry.state.is_terminal() {
                            entry.state = TaskState::Running;
                            entry.assigned_worker_id = Some(worker_id);
                            entry.adoptable_by_worker = None;
                            entry.fail_if_not_adopted = false;
                        }
                    }
                }
                JournalRecord::TaskTerminal {
                    task_id,
                    state,
                    assigned_worker_id,
                    retry_count,
                    failover_count,
                    evacuation_count,
                    rejection_count,
                    retry_attempt,
                    result,
                    error_message,
                    timestamp_utc,
                } => {
                    terminal_timestamps.insert(task_id, timestamp_utc);
                    if let Some(entry) = persisted.get_mut(&task_id) {
                        entry.state = state;
                        entry.assigned_worker_id = assigned_worker_id;
                        entry.adoptable_by_worker = None;
                        entry.fail_if_not_adopted = false;
                        entry.retry_count = retry_count;
                        entry.failover_count = failover_count;
                        entry.evacuation_count = evacuation_count;
                        entry.rejection_count = rejection_count;
                        if let Some(attempt) = retry_attempt {
                            entry.retry_history.push(attempt);
                        }
                        entry.result = result;
                        entry.cold_result = None;
                        entry.error_message = error_message;
                        if !terminal_order_vec.contains(&task_id) {
                            terminal_order_vec.push(task_id);
                        }
                    }
                }
                JournalRecord::TaskEvicted { task_id } => {
                    persisted.remove(&task_id);
                    terminal_timestamps.remove(&task_id);
                    terminal_order_vec.retain(|id| *id != task_id);
                }
            }
        }

        let mut sorted_entries: Vec<PersistedTaskEntry> = persisted.into_values().collect();
        sorted_entries.sort_unstable_by_key(|e| e.sequence);

        let now = Instant::now();
        let now_epoch = Utc::now().timestamp() as u64;
        let max_secs = rusty_grid_core::protocol::master_features::MAX_SESSION_RESUME_HOLD_SECS;
        let max_hold = Duration::from_secs(max_secs);
        if session_resume_hold > max_hold {
            tracing::warn!(
                requested_secs = session_resume_hold.as_secs(),
                max_secs = max_secs,
                "Requested session-resume hold exceeds maximum; clamping to {max_secs}s"
            );
        }
        let clamped_hold = if session_resume_hold.is_zero() {
            Duration::ZERO
        } else {
            session_resume_hold.min(max_hold)
        };
        let hold_until = (!clamped_hold.is_zero()).then(|| now + clamped_hold);

        let mut immediate_zero_retry_failures: Vec<(TaskId, Option<Uuid>)> = Vec::new();
        let mut acks: Vec<DurableAck> = Vec::new();
        let cold_store = Arc::clone(store.cold_results());
        let (live_cold_ids, recovered_cold_ids) = {
            let mut inner = self.inner.write().await;
            inner.session_resume_hold = clamped_hold;
            self.session_resume_hold_secs
                .store(clamped_hold.as_secs(), std::sync::atomic::Ordering::Release);
            inner.tasks.clear();
            inner.ready_queue.clear();
            inner.fair_queues.clear_entries_preserving_weights();
            inner.delayed_retries.clear();
            inner.worker_assignments.clear();
            inner.terminal_order.clear();
            inner.terminal_sizes.clear();
            inner.terminal_bytes = 0;
            inner.hot_terminal_order.clear();
            inner.hot_terminal_sizes.clear();
            inner.hot_terminal_bytes = 0;
            inner.pending_cold_deletes.clear();
            inner.cold_results = Some(Arc::clone(&cold_store));
            inner.active_sizes.clear();
            inner.active_bytes = 0;
            inner.held_tasks.clear();
            inner.workspace_affinity.clear();
            inner.workspace_active_tasks.clear();
            inner.sync_adoption_holds();
            inner.store_closed = false;
            inner.next_sequence = max_seq.max(1);
            let default_max_retries = inner.retry_policy.max_retries;

            for p in sorted_entries {
                let task_id = p.task.id;
                if matches!(p.state, TaskState::Completed | TaskState::Failed) {
                    if let (Some(ws), Some(wid)) = (&p.task.workspace, p.assigned_worker_id) {
                        let rejected = p.result.as_ref().is_some_and(is_worker_rejection);
                        if !rejected {
                            let finished_utc = terminal_timestamps
                                .get(&task_id)
                                .copied()
                                .unwrap_or(p.submitted_at_utc);
                            inner.record_workspace_affinity(&ws.name, wid, finished_utc);
                        }
                    }
                }
                let zero_retries = p
                    .task
                    .requirements
                    .max_retries
                    .unwrap_or(default_max_retries)
                    == 0;
                let (state, assigned_worker_id, adoptable_by, fail_if_not_adopted) = match p.state {
                    TaskState::Completed | TaskState::Failed | TaskState::Cancelled => {
                        (p.state, p.assigned_worker_id, None, false)
                    }
                    TaskState::Submitted | TaskState::Queued | TaskState::Retrying => {
                        let hold = match (p.adoptable_by_worker, hold_until) {
                            (Some(wid), Some(until)) => Some((wid, until)),
                            _ => None,
                        };
                        if p.fail_if_not_adopted && hold.is_none() {
                            immediate_zero_retry_failures.push((task_id, p.adoptable_by_worker));
                        }
                        (TaskState::Queued, None, hold, p.fail_if_not_adopted)
                    }
                    TaskState::Scheduled | TaskState::Running => {
                        let fail_unadopted = zero_retries || p.fail_if_not_adopted;
                        let last_worker = p.assigned_worker_id.or(p.adoptable_by_worker);
                        let hold = match (last_worker, hold_until) {
                            (Some(wid), Some(until)) => Some((wid, until)),
                            _ => None,
                        };
                        if fail_unadopted && hold.is_none() {
                            immediate_zero_retry_failures.push((task_id, last_worker));
                        }
                        (TaskState::Queued, None, hold, fail_unadopted)
                    }
                };

                let has_hold = adoptable_by.is_some() || fail_if_not_adopted;
                let requires_tree_v2 = task_has_tree_input(&p.task);
                let task_key = crate::speed::TaskKey::for_task(&p.task);
                let entry = TaskEntry {
                    task: p.task,
                    task_key,
                    priority: p.priority,
                    sequence: p.sequence,
                    state,
                    assigned_worker_id,
                    submitted_at_utc: p.submitted_at_utc,
                    submitted_instant: now,
                    queued_instant: now,
                    scheduled_instant: None,
                    started_instant: None,
                    retry_after_instant: None,
                    retry_count: p.retry_count,
                    failover_count: p.failover_count,
                    evacuation_count: p.evacuation_count,
                    rejection_count: p.rejection_count,
                    avoid_workers: p.avoid_workers,
                    assigned_session: None,
                    resumed_from_checkpoint: false,
                    adoptable_by,
                    fail_if_not_adopted,
                    pending_failover_charge: false,
                    pending_failover_reason: None,
                    retry_history: p.retry_history,
                    result: p.result,
                    error_message: p.error_message,
                    submitter: SubmitterId::Default,
                    cold_result: p.cold_result,
                    requires_tree_v2,
                };

                let is_terminal = entry.state.is_terminal();
                let priority = entry.priority;
                let sequence = entry.sequence;
                let size = retained_bytes(&entry);
                inner.tasks.insert(task_id, entry);
                if has_hold {
                    inner.held_tasks.insert(task_id);
                }
                inner.sync_workspace_active(task_id);

                if !is_terminal {
                    inner.active_sizes.insert(task_id, size);
                    inner.active_bytes = inner.active_bytes.saturating_add(size);
                    inner.insert_ready(QueueOrderKey {
                        priority_rev: Reverse(priority),
                        sequence,
                        task_id,
                    });
                }
            }

            for tid in terminal_order_vec {
                if inner.tasks.get(&tid).is_some_and(|e| e.state.is_terminal())
                    && !inner.terminal_sizes.contains_key(&tid)
                {
                    let size = inner.tasks.get(&tid).map_or(0, retained_bytes);
                    inner.terminal_sizes.insert(tid, size);
                    inner.terminal_bytes = inner.terminal_bytes.saturating_add(size);
                    inner.terminal_order.push_back(tid);
                }
            }
            let mut unlisted_terminals: Vec<(u64, TaskId)> = inner
                .tasks
                .iter()
                .filter(|(tid, e)| e.state.is_terminal() && !inner.terminal_sizes.contains_key(tid))
                .map(|(tid, e)| (e.sequence, *tid))
                .collect();
            unlisted_terminals.sort_unstable_by_key(|(seq, _)| *seq);
            for (_, tid) in unlisted_terminals {
                let size = inner.tasks.get(&tid).map_or(0, retained_bytes);
                inner.terminal_sizes.insert(tid, size);
                inner.terminal_bytes = inner.terminal_bytes.saturating_add(size);
                inner.terminal_order.push_back(tid);
            }

            inner.store = Some(store);
            inner.enforce_terminal_limits();

            for (task_id, last_worker_id) in immediate_zero_retry_failures {
                if let Some(record) = Self::fail_unadopted_zero_retry_task(
                    &mut inner,
                    task_id,
                    last_worker_id,
                    now_epoch,
                ) {
                    if let Some(ack) = inner.append_durable_warn(&record) {
                        acks.push(ack);
                    }
                }
            }
            inner.sync_adoption_holds();
            inner.rebuild_all_resume_protection();

            for &tid in &inner.terminal_order.clone() {
                if let Some(hsize) = inner
                    .tasks
                    .get(&tid)
                    .filter(|e| e.state.is_terminal())
                    .and_then(|e| e.result.as_ref().map(task_result_bytes))
                {
                    inner.hot_terminal_sizes.insert(tid, hsize);
                    inner.hot_terminal_bytes = inner.hot_terminal_bytes.saturating_add(hsize);
                    inner.hot_terminal_order.push_back(tid);
                }
            }
            let live_cold: HashSet<TaskId> = inner
                .tasks
                .iter()
                .filter(|(_, e)| e.state.is_terminal() && e.cold_result.is_some())
                .map(|(tid, _)| *tid)
                .collect();
            let recovered_cold: Vec<TaskId> = live_cold.iter().copied().collect();
            let mut keep_cold = live_cold;
            keep_cold.extend(inner.collect_cold_spill_ids());

            inner.maybe_trigger_compaction();
            (keep_cold, recovered_cold)
        };

        for ack in acks {
            if let Err(err) = ack.await {
                tracing::warn!(error = %err, "Failed to sync replay zero-retry failure record");
            }
        }
        let cold_for_sync = Arc::clone(&cold_store);
        let live_for_sync = live_cold_ids;
        let missing_cold = tokio::task::spawn_blocking(move || {
            cold_for_sync.retain_only(&live_for_sync);
            recovered_cold_ids
                .into_iter()
                .filter(|tid| !cold_for_sync.contains(tid))
                .collect::<Vec<TaskId>>()
        })
        .await
        .unwrap_or_default();
        if !missing_cold.is_empty() {
            let mut inner = self.inner.write().await;
            for tid in missing_cold {
                tracing::warn!(
                    task_id = %tid,
                    "Cold result file missing on replay; clearing cold summary reference"
                );
                if let Some(entry) = inner.tasks.get_mut(&tid) {
                    entry.cold_result = None;
                }
            }
        }
        self.flush_cold_maintenance().await;
        Ok(())
    }

    /// Re-pins input blobs of recovered non-terminal tasks and output blobs of recovered retained
    /// terminal tasks in `blob_store` so they are protected from eviction until the task finishes
    /// or is evicted (design 0046 D3).
    pub async fn repin_recovered_blobs(&self, blob_store: &Arc<crate::blob_store::BlobStore>) {
        use rusty_grid_core::blob::BlobId;
        let (active_inputs, terminal_outputs) = {
            let inner = self.inner.read().await;
            let mut active = Vec::new();
            let mut terminal = Vec::new();
            for entry in inner.tasks.values() {
                if entry.state.is_terminal() {
                    if let Some(ref res) = entry.result {
                        let ids: Vec<BlobId> = res.outputs.iter().map(|o| o.id).collect();
                        if !ids.is_empty() {
                            terminal.push((entry.task.id, ids));
                        }
                    } else if let Some(ref cold) = entry.cold_result {
                        if !cold.output_blob_ids.is_empty() {
                            terminal.push((entry.task.id, cold.output_blob_ids.clone()));
                        }
                    }
                } else {
                    let mut inputs = entry.task.inputs.clone();
                    if let Some(ref ws) = entry.task.workspace {
                        if let Some(rusty_grid_core::task::WorkspaceSource::Tree {
                            manifest,
                            manifest_size,
                        }) = ws.source
                        {
                            inputs.push(rusty_grid_core::blob::TaskInput {
                                path: format!("workspace:{}", ws.name),
                                source: rusty_grid_core::blob::InputSource::Tree {
                                    manifest,
                                    manifest_size,
                                },
                            });
                        }
                    }
                    if !inputs.is_empty() {
                        active.push((entry.task.id, inputs));
                    }
                }
            }
            (active, terminal)
        };

        for (task_id, inputs) in active_inputs {
            match blob_store.resolve_inputs_with_features(&inputs).await {
                Ok((ids, needs_tree_v2)) => {
                    let missing = blob_store.pin_task(task_id, ids);
                    self.set_requires_tree_v2(&task_id, needs_tree_v2 || !missing.is_empty())
                        .await;
                    if !missing.is_empty() {
                        tracing::warn!(
                            task_id = %task_id,
                            missing_count = missing.len(),
                            "Recovered active task references blob(s) missing from the blob store"
                        );
                    }
                }
                Err(err) => {
                    self.set_requires_tree_v2(&task_id, true).await;
                    tracing::warn!(
                        task_id = %task_id,
                        error = %err,
                        "Recovered active task input blob check failed"
                    );
                }
            }
        }

        for (task_id, output_ids) in terminal_outputs {
            match blob_store.check_and_pin_outputs(&output_ids) {
                Ok(pin) => {
                    pin.commit_terminal(task_id).await;
                }
                Err(missing) => {
                    tracing::warn!(
                        task_id = %task_id,
                        missing_count = missing.len(),
                        "Recovered terminal task references output blob(s) missing from the blob store"
                    );
                }
            }
        }
    }

    /// Re-attaches tasks held in their `adoptable_by` window for `worker_id` when that worker
    /// reconnects on `session_id` and reports `running` (design 0046 D1 / D2.5).
    pub async fn adopt_session_tasks(
        &self,
        worker_id: Uuid,
        session_id: u64,
        running: &[TaskId],
    ) -> AdoptionOutcome {
        if running.is_empty()
            && self
                .adoption_holds
                .load(std::sync::atomic::Ordering::Acquire)
                == 0
        {
            return AdoptionOutcome::default();
        }
        let mut outcome = AdoptionOutcome::default();
        let mut acks: Vec<DurableAck> = Vec::new();

        {
            let mut inner = self.inner.write().await;
            let now = Instant::now();
            let now_epoch = Utc::now().timestamp() as u64;

            for &task_id in running {
                let can_adopt = inner.tasks.get(&task_id).is_some_and(|entry| {
                    entry.state == TaskState::Queued
                        && entry
                            .adoptable_by
                            .is_some_and(|(w, until)| w == worker_id && now < until)
                });
                if can_adopt {
                    let (priority, sequence) = {
                        let entry = inner.tasks.get(&task_id).expect("checked above");
                        (entry.priority, entry.sequence)
                    };
                    inner.remove_ready(&QueueOrderKey {
                        priority_rev: Reverse(priority),
                        sequence,
                        task_id,
                    });
                    if let Some(entry) = inner.tasks.get_mut(&task_id) {
                        entry.state = TaskState::Running;
                        entry.assigned_worker_id = Some(worker_id);
                        entry.assigned_session = Some(session_id);
                        entry.started_instant = Some(now);
                        entry.adoptable_by = None;
                        entry.fail_if_not_adopted = false;
                        entry.pending_failover_charge = false;
                        entry.pending_failover_reason = None;
                    }
                    inner.track_adoption_hold(task_id, false);
                    inner
                        .worker_assignments
                        .entry(worker_id)
                        .or_default()
                        .insert(task_id);
                    inner.append_non_durable(&JournalRecord::TaskAdopted {
                        task_id,
                        worker_id,
                        timestamp_utc: now_epoch,
                    });
                    outcome.adopted.push(task_id);
                } else {
                    let (already_owned, already_completed_in_session) =
                        inner.tasks.get(&task_id).map_or((false, false), |entry| {
                            let same_session = entry.assigned_worker_id == Some(worker_id)
                                && entry.assigned_session == Some(session_id);
                            (
                                entry.state == TaskState::Running && same_session,
                                entry.state.is_terminal() && same_session,
                            )
                        });
                    if already_owned {
                        outcome.adopted.push(task_id);
                    } else if !already_completed_in_session {
                        outcome.stale_to_cancel.push(task_id);
                    }
                }
            }

            let remaining_for_worker: Vec<(TaskId, bool, bool)> = inner
                .held_tasks
                .iter()
                .filter_map(|&tid| {
                    let entry = inner.tasks.get(&tid)?;
                    (entry.state == TaskState::Queued
                        && entry.adoptable_by.is_some_and(|(w, _)| w == worker_id))
                    .then_some((
                        tid,
                        entry.fail_if_not_adopted,
                        entry.pending_failover_charge,
                    ))
                })
                .collect();

            let mut released_any = false;
            for (task_id, fail_if_not_adopted, pending_failover_charge) in remaining_for_worker {
                let (record, failed, released) = Self::release_unadopted_task(
                    &mut inner,
                    task_id,
                    Some(worker_id),
                    fail_if_not_adopted,
                    pending_failover_charge,
                    now_epoch,
                );
                if let Some(rec) = record {
                    if let Some(ack) = inner.append_durable_warn(&rec) {
                        acks.push(ack);
                    }
                }
                if failed {
                    outcome.newly_failed.push(task_id);
                }
                if released {
                    released_any = true;
                }
            }
            if released_any {
                inner.ready_epoch = inner.ready_epoch.wrapping_add(1);
            }
            inner.refresh_worker_resume_protection(worker_id);
            inner.maybe_trigger_compaction();
        }

        for ack in acks {
            if let Err(err) = ack.await {
                tracing::warn!(error = %err, "Failed to sync terminal journal record after session adoption");
            }
        }
        outcome
    }

    /// Releases any `adoptable_by` holds whose deadline has passed, failing tasks marked
    /// `fail_if_not_adopted` (`Scheduled` or `Running` with `max_retries == 0` before restart) and
    /// returning their IDs so the caller can wake result waiters.
    pub async fn release_expired_adoption_holds(&self) -> Vec<TaskId> {
        if self
            .adoption_holds
            .load(std::sync::atomic::Ordering::Acquire)
            == 0
        {
            return Vec::new();
        }

        let now = Instant::now();
        {
            let inner = self.inner.read().await;
            let any_expired = inner.held_tasks.iter().any(|tid| {
                inner.tasks.get(tid).is_some_and(|entry| {
                    entry.state == TaskState::Queued
                        && (entry.adoptable_by.is_some_and(|(_, until)| now >= until)
                            || (entry.fail_if_not_adopted && entry.adoptable_by.is_none()))
                })
            });
            if !any_expired {
                return Vec::new();
            }
        }

        let mut newly_failed = Vec::new();
        let mut acks = Vec::new();
        {
            let mut inner = self.inner.write().await;
            let now = Instant::now();
            let now_epoch = Utc::now().timestamp() as u64;
            let expired: Vec<(TaskId, Option<Uuid>, bool, bool)> = inner
                .held_tasks
                .iter()
                .filter_map(|&tid| {
                    let entry = inner.tasks.get(&tid)?;
                    if entry.state != TaskState::Queued {
                        return None;
                    }
                    match entry.adoptable_by {
                        Some((wid, until)) if now >= until => Some((
                            tid,
                            Some(wid),
                            entry.fail_if_not_adopted,
                            entry.pending_failover_charge,
                        )),
                        None if entry.fail_if_not_adopted => {
                            Some((tid, None, true, entry.pending_failover_charge))
                        }
                        _ => None,
                    }
                })
                .collect();

            let mut released_any = false;
            let mut expired_workers = HashSet::new();
            for (task_id, last_worker, fail_if_not_adopted, pending_failover_charge) in expired {
                if let Some(wid) = last_worker {
                    expired_workers.insert(wid);
                }
                let (record, failed, released) = Self::release_unadopted_task(
                    &mut inner,
                    task_id,
                    last_worker,
                    fail_if_not_adopted,
                    pending_failover_charge,
                    now_epoch,
                );
                if let Some(rec) = record {
                    if let Some(ack) = inner.append_durable_warn(&rec) {
                        acks.push(ack);
                    }
                }
                if failed {
                    newly_failed.push(task_id);
                }
                if released {
                    released_any = true;
                }
            }
            for wid in expired_workers {
                inner.refresh_worker_resume_protection(wid);
                let is_protected = {
                    let tracker = inner
                        .session_resume_tracker
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    tracker.connected.contains_key(&wid) || tracker.with_work_or_hold.contains(&wid)
                };
                if !is_protected {
                    inner
                        .session_resume_workers
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .remove(&wid);
                    inner
                        .session_resume_tracker
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .order
                        .retain(|w| *w != wid);
                }
            }
            if released_any {
                inner.ready_epoch = inner.ready_epoch.wrapping_add(1);
            }
            inner.maybe_trigger_compaction();
        }

        for ack in acks {
            if let Err(err) = ack.await {
                tracing::warn!(error = %err, "Failed to sync expired adoption failure record");
            }
        }
        if !newly_failed.is_empty() {
            self.flush_cold_maintenance().await;
        }
        newly_failed
    }

    /// Writes a compacted snapshot of the current queue state to disk and resets `journal.bin`.
    pub async fn compact_store(&self) -> GridResult<()> {
        let ack = {
            let inner = self.inner.write().await;
            let Some(ref store) = inner.store else {
                return Ok(());
            };
            let image = inner.snapshot_image();
            store.compact(image)?
        };
        ack.await
    }

    /// Compacts the task store (if attached), detaches it from the queue, and waits for the writer
    /// thread to flush and release `store.lock`.
    pub async fn close_and_compact_store(&self) {
        let _cold_guard = self.cold_flush_lock.lock().await;
        let (store, ack, cold_store, deletes) = {
            let mut inner = self.inner.write().await;
            inner.store_closed = true;
            let cold_store = inner.cold_results.take();
            let deletes = std::mem::take(&mut inner.pending_cold_deletes);
            let Some(store) = inner.store.take() else {
                drop(inner);
                if let Some(cold) = cold_store {
                    if !deletes.is_empty() {
                        let _ = tokio::task::spawn_blocking(move || {
                            for tid in &deletes {
                                cold.remove(tid);
                            }
                        })
                        .await;
                    }
                }
                return;
            };
            let image = inner.snapshot_image();
            let ack = store.compact(image).ok();
            (store, ack, cold_store, deletes)
        };
        if let Some(ack) = ack {
            let _ = ack.await;
        }
        if let Some(cold) = cold_store {
            if !deletes.is_empty() {
                let _ = tokio::task::spawn_blocking(move || {
                    for tid in &deletes {
                        cold.remove(tid);
                    }
                })
                .await;
            }
        }
        drop(store);
    }

    /// Synchronous counterpart to [`close_and_compact_store`](Self::close_and_compact_store) for
    /// callers such as `MasterHandle::shutdown` that run outside an `async fn`.
    pub fn close_and_compact_store_sync(&self) {
        if let Ok(mut inner) = self.inner.try_write() {
            inner.store_closed = true;
            let cold_store = inner.cold_results.take();
            let deletes = std::mem::take(&mut inner.pending_cold_deletes);
            if let Some(store) = inner.store.take() {
                let image = inner.snapshot_image();
                let _ = store.compact(image);
                drop(inner);
                if let Some(cold) = cold_store {
                    for tid in &deletes {
                        cold.remove(tid);
                    }
                }
                drop(store);
            } else if let Some(cold) = cold_store {
                drop(inner);
                for tid in &deletes {
                    cold.remove(tid);
                }
            }
            return;
        }
        let queue = self.clone();
        let _ = std::thread::spawn(move || {
            let mut inner = queue.inner.blocking_write();
            inner.store_closed = true;
            let cold_store = inner.cold_results.take();
            let deletes = std::mem::take(&mut inner.pending_cold_deletes);
            if let Some(store) = inner.store.take() {
                let image = inner.snapshot_image();
                let _ = store.compact(image);
                drop(inner);
                if let Some(cold) = cold_store {
                    for tid in &deletes {
                        cold.remove(tid);
                    }
                }
                drop(store);
            } else if let Some(cold) = cold_store {
                drop(inner);
                for tid in &deletes {
                    cold.remove(tid);
                }
            }
        })
        .join();
    }

    /// Number of `sync_data()` calls performed by the attached [`TaskStore`], if any.
    pub async fn store_sync_count(&self) -> Option<u64> {
        self.inner
            .read()
            .await
            .store
            .as_ref()
            .map(TaskStore::sync_count)
    }

    /// Number of records currently in `journal.bin` since the last compaction, if a [`TaskStore`]
    /// is attached.
    pub async fn store_journal_records(&self) -> Option<u64> {
        self.inner
            .read()
            .await
            .store
            .as_ref()
            .map(TaskStore::journal_records)
    }
}
