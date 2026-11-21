//! Task submission, ready-view paging, assigned load tracking, and batch scheduling.

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::protocol::ProtocolCapabilities;
use rusty_grid_core::task::{Task, TaskId, TaskRequirements};
use uuid::Uuid;

use crate::fair_share::{FairWalkCursor, SubmitterId};
use crate::store::JournalRecord;

use super::{
    is_schedulable_queued, mark_entry_scheduled, owned_by, owned_by_speculative, task_active_bytes,
    task_has_tree_input, AssignedLoad, QueueOrderKey, RetryAttempt, SpeculativeAttempt,
    StragglerCandidate, TaskEntry, TaskQueue, TaskQueueInner, TaskState, TaskView,
    WorkspaceAffinity,
};

/// Outcome of [`TaskQueue::try_start_speculative_and_deliver`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpeculativeStartOutcome {
    /// Speculative attempt was recorded and `deliver` succeeded inside the same write lock.
    Delivered,
    /// Task was no longer `Running`/eligible for a speculative attempt; queue state unchanged.
    NotEligible,
    /// Task carries a critical worker extension unsupported by `peer_version`; queue state unchanged.
    WithheldFromPeer,
    /// `deliver` returned `false` (channel full or closed); queue state unchanged.
    DeliveryFailed,
}

impl TaskQueue {
    // --- Task Submission ---

    /// Submits a task with default priority (0).
    pub async fn submit(&self, task: Task) -> GridResult<TaskId> {
        self.submit_from(task, 0, SubmitterId::Default).await
    }

    /// Checks under the read lock whether a task of `task_bytes` bytes can currently fit in the
    /// queue (both task-count `capacity` and `max_active_bytes`), without cloning `Task`.
    pub(crate) async fn check_room(&self, task_bytes: usize) -> GridResult<()> {
        let inner = self.inner.read().await;
        if let Some(cap) = inner.capacity {
            let active_count = inner
                .tasks
                .values()
                .filter(|e| !e.state.is_terminal())
                .count();
            if active_count >= cap {
                return Err(GridError::QueueFull { capacity: cap });
            }
        }
        if inner.active_bytes.saturating_add(task_bytes) > inner.max_active_bytes {
            return Err(GridError::QueueByteLimitExceeded {
                task_bytes,
                in_use_bytes: inner.active_bytes,
                max_bytes: inner.max_active_bytes,
            });
        }
        Ok(())
    }

    /// Attempts to submit the task in `task_slot` with priority 0, taking ownership from
    /// `task_slot` (`*task_slot = None`) only if the task is accepted into the queue.
    ///
    /// When the queue is full (`QueueFull` or `QueueByteLimitExceeded`), `task_slot` is left
    /// untouched so the caller can retry after backoff without ever cloning the task payload.
    pub(crate) async fn try_submit_slot(&self, task_slot: &mut Option<Task>) -> GridResult<TaskId> {
        self.try_submit_slot_from(task_slot, SubmitterId::Default)
            .await
    }

    /// Like [`try_submit_slot`](Self::try_submit_slot), associating the task with `submitter` for
    /// multi-client fair-share scheduling.
    pub(crate) async fn try_submit_slot_from(
        &self,
        task_slot: &mut Option<Task>,
        submitter: SubmitterId,
    ) -> GridResult<TaskId> {
        let task_ref = task_slot
            .as_ref()
            .ok_or_else(|| GridError::Config("task_slot is empty".into()))?;
        task_ref.validate().map_err(GridError::Config)?;
        let entry_bytes = task_active_bytes(task_ref);

        self.check_room(entry_bytes).await?;

        let (task_id, sequence, ack) = {
            let mut inner = self.inner.write().await;

            if inner.store_closed {
                return Err(GridError::Storage(
                    "task store is closed (master shutting down)".into(),
                ));
            }

            let task_ref = task_slot.as_ref().expect("task_slot checked above");
            if let Some(existing) = inner.tasks.get(&task_ref.id) {
                return Err(GridError::Config(format!(
                    "Task {} already exists (state {:?}); task ids must be unique, query its outcome instead of resubmitting",
                    task_ref.id, existing.state
                )));
            }

            if let Some(cap) = inner.capacity {
                let active_count = inner
                    .tasks
                    .values()
                    .filter(|e| !e.state.is_terminal())
                    .count();
                if active_count >= cap {
                    return Err(GridError::QueueFull { capacity: cap });
                }
            }

            if inner.active_bytes.saturating_add(entry_bytes) > inner.max_active_bytes {
                return Err(GridError::QueueByteLimitExceeded {
                    task_bytes: entry_bytes,
                    in_use_bytes: inner.active_bytes,
                    max_bytes: inner.max_active_bytes,
                });
            }

            let sequence = inner.next_sequence;
            let now_instant = Instant::now();
            let now_epoch = Utc::now().timestamp() as u64;

            let ack = if inner.store.is_some() {
                let record = JournalRecord::TaskSubmitted {
                    task: Box::new(task_ref.clone()),
                    priority: 0,
                    sequence,
                    submitted_at_utc: now_epoch,
                };
                inner.append_durable(&record)?
            } else {
                None
            };

            let task = task_slot.take().expect("task_slot checked above");
            let task_id = task.id;
            let requires_tree_v2 = task_has_tree_input(&task);
            let task_key = crate::speed::TaskKey::for_task(&task);
            inner.next_sequence += 1;

            let entry = TaskEntry {
                task,
                task_key,
                priority: 0,
                sequence,
                submitter,
                state: TaskState::Queued,
                assigned_worker_id: None,
                submitted_at_utc: now_epoch,
                submitted_instant: now_instant,
                queued_instant: now_instant,
                scheduled_instant: None,
                started_instant: None,
                retry_after_instant: None,
                retry_count: 0,
                failover_count: 0,
                evacuation_count: 0,
                rejection_count: 0,
                avoid_workers: Vec::new(),
                assigned_session: None,
                resumed_from_checkpoint: false,
                adoptable_by: None,
                fail_if_not_adopted: false,
                pending_failover_charge: false,
                pending_failover_reason: None,
                retry_history: Vec::new(),
                result: None,
                cold_result: None,
                error_message: None,
                requires_tree_v2,
                speculative: None,
                speculative_spawned: false,
            };

            inner.tasks.insert(task_id, entry);
            inner.active_sizes.insert(task_id, entry_bytes);
            inner.active_bytes = inner.active_bytes.saturating_add(entry_bytes);
            if ack.is_none() {
                inner.insert_ready(QueueOrderKey {
                    priority_rev: Reverse(0),
                    sequence,
                    task_id,
                });
            }
            inner.maybe_trigger_compaction();
            (task_id, sequence, ack)
        };

        if let Some(durable_ack) = ack {
            let ready_key = QueueOrderKey {
                priority_rev: Reverse(0),
                sequence,
                task_id,
            };
            let inner_arc = Arc::clone(&self.inner);
            tokio::spawn(async move {
                match durable_ack.await {
                    Ok(()) => {
                        let mut inner = inner_arc.write().await;
                        if inner
                            .tasks
                            .get(&task_id)
                            .is_some_and(|e| e.state == TaskState::Queued)
                        {
                            inner.insert_ready(ready_key);
                        }
                        Ok(())
                    }
                    Err(err) => {
                        let mut inner = inner_arc.write().await;
                        inner.remove_ready(&ready_key);
                        inner.remove_retained(&task_id);
                        inner.terminal_order.retain(|id| *id != task_id);
                        Err(err)
                    }
                }
            })
            .await
            .map_err(|e| GridError::Storage(format!("submit ack worker cancelled: {e}")))??;
        }

        Ok(task_id)
    }

    /// Submits a task with explicit scheduling priority.
    ///
    /// Task ids are unique for as long as the queue retains the entry: resubmitting an id that is
    /// queued, running or finished is refused instead of replacing the entry (which would orphan
    /// its queue keys and run the work twice).
    pub async fn submit_with_priority(&self, task: Task, priority: u32) -> GridResult<TaskId> {
        let requires_tree_v2 = task_has_tree_input(&task);
        self.submit_from_with_tree_v2(task, priority, SubmitterId::Default, requires_tree_v2)
            .await
    }

    /// Submits a task with explicit scheduling `priority` attributed to `submitter` for multi-client
    /// fair-share scheduling.
    pub async fn submit_from(
        &self,
        task: Task,
        priority: u32,
        submitter: SubmitterId,
    ) -> GridResult<TaskId> {
        let requires_tree_v2 = task_has_tree_input(&task);
        self.submit_from_with_tree_v2(task, priority, submitter, requires_tree_v2)
            .await
    }

    /// Submits a task with explicit scheduling priority and whether its input/workspace tree
    /// manifests require `ProtocolCapabilities::TREE_V2`.
    pub async fn submit_with_priority_and_tree_v2(
        &self,
        task: Task,
        priority: u32,
        requires_tree_v2: bool,
    ) -> GridResult<TaskId> {
        self.submit_from_with_tree_v2(task, priority, SubmitterId::Default, requires_tree_v2)
            .await
    }

    /// Submits a task with explicit `priority`, `submitter` fair-share attribution, and
    /// pre-checked `requires_tree_v2` feature requirement.
    pub async fn submit_from_with_tree_v2(
        &self,
        task: Task,
        priority: u32,
        submitter: SubmitterId,
        requires_tree_v2: bool,
    ) -> GridResult<TaskId> {
        task.validate().map_err(GridError::Config)?;
        #[cfg(feature = "otel")]
        let requires_gpu = task.requirements.gpu_required;

        let (task_id, sequence, ack) = {
            let mut inner = self.inner.write().await;

            if inner.store_closed {
                return Err(GridError::Storage(
                    "task store is closed (master shutting down)".into(),
                ));
            }

            if let Some(existing) = inner.tasks.get(&task.id) {
                return Err(GridError::Config(format!(
                    "Task {} already exists (state {:?}); task ids must be unique, query its outcome instead of resubmitting",
                    task.id, existing.state
                )));
            }

            if let Some(cap) = inner.capacity {
                let active_count = inner
                    .tasks
                    .values()
                    .filter(|e| !e.state.is_terminal())
                    .count();
                if active_count >= cap {
                    return Err(GridError::QueueFull { capacity: cap });
                }
            }

            let entry_bytes = task_active_bytes(&task);
            if inner.active_bytes.saturating_add(entry_bytes) > inner.max_active_bytes {
                return Err(GridError::QueueByteLimitExceeded {
                    task_bytes: entry_bytes,
                    in_use_bytes: inner.active_bytes,
                    max_bytes: inner.max_active_bytes,
                });
            }

            let sequence = inner.next_sequence;
            let now_instant = Instant::now();
            let now_epoch = Utc::now().timestamp() as u64;

            let ack = if inner.store.is_some() {
                let record = JournalRecord::TaskSubmitted {
                    task: Box::new(task.clone()),
                    priority,
                    sequence,
                    submitted_at_utc: now_epoch,
                };
                inner.append_durable(&record)?
            } else {
                None
            };

            let task_id = task.id;
            let task_key = crate::speed::TaskKey::for_task(&task);
            inner.next_sequence += 1;

            let entry = TaskEntry {
                task,
                task_key,
                priority,
                sequence,
                submitter,
                state: TaskState::Queued,
                assigned_worker_id: None,
                submitted_at_utc: now_epoch,
                submitted_instant: now_instant,
                queued_instant: now_instant,
                scheduled_instant: None,
                started_instant: None,
                retry_after_instant: None,
                retry_count: 0,
                failover_count: 0,
                evacuation_count: 0,
                rejection_count: 0,
                avoid_workers: Vec::new(),
                assigned_session: None,
                resumed_from_checkpoint: false,
                adoptable_by: None,
                fail_if_not_adopted: false,
                pending_failover_charge: false,
                pending_failover_reason: None,
                retry_history: Vec::new(),
                result: None,
                cold_result: None,
                error_message: None,
                requires_tree_v2,
                speculative: None,
                speculative_spawned: false,
            };

            inner.tasks.insert(task_id, entry);
            inner.active_sizes.insert(task_id, entry_bytes);
            inner.active_bytes = inner.active_bytes.saturating_add(entry_bytes);
            if ack.is_none() {
                inner.insert_ready(QueueOrderKey {
                    priority_rev: Reverse(priority),
                    sequence,
                    task_id,
                });
            }
            inner.maybe_trigger_compaction();
            (task_id, sequence, ack)
        };

        #[cfg(feature = "otel")]
        let submit_span = tracing::info_span!(
            target: crate::otel::OTEL_SPAN_TARGET,
            "submit",
            task_id = %task_id,
            priority = i64::from(priority),
            requires_gpu = requires_gpu,
            outcome = tracing::field::Empty,
        );

        if let Some(durable_ack) = ack {
            let ready_key = QueueOrderKey {
                priority_rev: Reverse(priority),
                sequence,
                task_id,
            };
            let inner_arc = Arc::clone(&self.inner);
            let wait_res = tokio::spawn(async move {
                match durable_ack.await {
                    Ok(()) => {
                        let mut inner = inner_arc.write().await;
                        if inner
                            .tasks
                            .get(&task_id)
                            .is_some_and(|e| e.state == TaskState::Queued)
                        {
                            inner.insert_ready(ready_key);
                        }
                        Ok(())
                    }
                    Err(err) => {
                        let mut inner = inner_arc.write().await;
                        inner.remove_ready(&ready_key);
                        inner.remove_retained(&task_id);
                        inner.terminal_order.retain(|id| *id != task_id);
                        Err(err)
                    }
                }
            })
            .await
            .map_err(|e| GridError::Storage(format!("submit ack worker cancelled: {e}")));
            match wait_res {
                Ok(Ok(())) => {}
                Ok(Err(err)) | Err(err) => {
                    #[cfg(feature = "otel")]
                    submit_span.record("outcome", "error");
                    return Err(err);
                }
            }
        }

        #[cfg(feature = "otel")]
        submit_span.record("outcome", "accepted");
        self.metrics.record_submitted();
        Ok(task_id)
    }

    // --- Scheduler Matchmaking & Queries ---

    /// Records whether `task_id` requires `ProtocolCapabilities::TREE_V2` (used when store replay
    /// resolves input manifests).
    pub async fn set_requires_tree_v2(&self, task_id: &TaskId, requires_tree_v2: bool) {
        let mut inner = self.inner.write().await;
        if let Some(entry) = inner.tasks.get_mut(task_id) {
            entry.requires_tree_v2 = requires_tree_v2;
        }
    }

    /// Returns a list of tasks currently in `Queued` state in priority-FIFO order.
    pub async fn get_schedulable_tasks(&self) -> Vec<Task> {
        let inner = self.inner.read().await;
        let now = Instant::now();
        let policy = &inner.retry_policy;
        inner
            .ready_queue
            .iter()
            .filter_map(|k| {
                inner
                    .tasks
                    .get(&k.task_id)
                    .filter(|e| is_schedulable_queued(e, policy, now))
                    .map(|e| e.task.clone())
            })
            .collect()
    }

    /// Scheduling views of up to `limit` queued tasks in priority-FIFO order, starting after the
    /// ready-queue position `after` (`None` = from the front).
    ///
    /// Returns the views plus the position to continue from (`None` when the queue is exhausted).
    /// Nothing is cloned but the small requirement records, and the read lock is held only for
    /// this page, so a pass can walk an arbitrarily long backlog without blocking writers.
    /// Keys whose task is no longer `Queued` are skipped.
    pub async fn ready_views(
        &self,
        after: Option<QueueOrderKey>,
        limit: usize,
    ) -> (Vec<TaskView>, Option<QueueOrderKey>) {
        #[cfg(test)]
        self.ready_views_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let inner = self.inner.read().await;
        let now = Instant::now();
        let policy = &inner.retry_policy;
        let lower = match after {
            Some(key) => Bound::Excluded(key),
            None => Bound::Unbounded,
        };
        let mut views = Vec::with_capacity(limit.min(256));
        let mut range = inner.ready_queue.range((lower, Bound::Unbounded));
        let mut last_key = None;
        for key in &mut range {
            let Some(entry) = inner
                .tasks
                .get(&key.task_id)
                .filter(|e| is_schedulable_queued(e, policy, now))
            else {
                continue;
            };
            let mut view = TaskView::of_with_key(&entry.task, entry.task_key);
            if entry.requires_tree_v2 {
                view.required_features.insert(ProtocolCapabilities::TREE_V2);
            }
            view.avoid = entry.avoid_workers.clone();
            view.queued_for = now.saturating_duration_since(entry.queued_instant);
            views.push(view);
            if views.len() >= limit {
                last_key = Some(*key);
                break;
            }
        }
        let next = last_key.filter(|_| {
            range.any(|k| {
                inner
                    .tasks
                    .get(&k.task_id)
                    .is_some_and(|e| is_schedulable_queued(e, policy, now))
            })
        });
        (views, next)
    }

    /// Number of distinct submitters that currently have at least one ready task in the queue.
    pub async fn active_submitter_count(&self) -> usize {
        self.inner.read().await.fair_queues.active_submitter_count()
    }

    /// Sets the WRR scheduling weight (`>= 1`) assigned to `submitter`.
    pub async fn set_submitter_weight(&self, submitter: SubmitterId, weight: u32) {
        self.inner
            .write()
            .await
            .fair_queues
            .set_weight(submitter, weight);
    }

    /// Scheduling views of up to `limit` queued tasks walking across active submitters in
    /// weighted round-robin order using `cursor`.
    pub(crate) async fn ready_views_fair(
        &self,
        cursor: &mut FairWalkCursor,
        limit: usize,
    ) -> Vec<TaskView> {
        if limit == 0 {
            return Vec::new();
        }
        #[cfg(test)]
        self.ready_views_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let inner = self.inner.read().await;
        let now = Instant::now();
        let policy = &inner.retry_policy;
        let mut views = Vec::with_capacity(limit.min(512));
        while views.len() < limit {
            let remaining = limit - views.len();
            let batch = inner.fair_queues.walk(cursor, remaining);
            if batch.is_empty() {
                break;
            }
            for (key, _submitter) in batch {
                let Some(entry) = inner
                    .tasks
                    .get(&key.task_id)
                    .filter(|e| is_schedulable_queued(e, policy, now))
                else {
                    continue;
                };
                let mut view = TaskView::of_with_key(&entry.task, entry.task_key);
                if entry.requires_tree_v2 {
                    view.required_features.insert(ProtocolCapabilities::TREE_V2);
                }
                view.avoid = entry.avoid_workers.clone();
                view.queued_for = now.saturating_duration_since(entry.queued_instant);
                views.push(view);
            }
        }
        views
    }

    /// Monotonic counter incremented whenever a task enters `ready_queue`.
    pub async fn ready_epoch(&self) -> u64 {
        self.inner.read().await.ready_epoch
    }

    /// Number of [`ready_views`](Self::ready_views) calls made on this queue (for tests).
    #[cfg(test)]
    pub fn ready_views_call_count(&self) -> u64 {
        self.ready_views_calls
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Ids of up to `limit` queued tasks in priority-FIFO order, without cloning any task.
    pub async fn ready_task_ids(&self, limit: usize) -> Vec<TaskId> {
        let inner = self.inner.read().await;
        let now = Instant::now();
        let policy = &inner.retry_policy;
        inner
            .ready_queue
            .iter()
            .filter(|k| {
                inner
                    .tasks
                    .get(&k.task_id)
                    .is_some_and(|e| is_schedulable_queued(e, policy, now))
            })
            .take(limit)
            .map(|k| k.task_id)
            .collect()
    }

    /// True if any queued task satisfies `pred`. Stops at the first hit; takes no clones.
    pub async fn any_queued<F: Fn(&Task) -> bool>(&self, pred: F) -> bool {
        let inner = self.inner.read().await;
        let now = Instant::now();
        let policy = &inner.retry_policy;
        inner.ready_queue.iter().any(|k| {
            inner
                .tasks
                .get(&k.task_id)
                .is_some_and(|e| is_schedulable_queued(e, policy, now) && pred(&e.task))
        })
    }

    /// What each worker is carrying according to the queue's own assignment set, for every worker
    /// that has at least one assigned task. Heartbeats cannot corrupt this: it is derived from
    /// the tasks themselves. Tasks held in an active `adoptable_by` window for `worker_id` also
    /// reserve their slots on `worker_id` so the scheduler does not over-subscribe a reconnecting
    /// worker before its `ResumeSession` message is processed.
    pub async fn assigned_loads(&self) -> HashMap<Uuid, AssignedLoad> {
        let inner = self.inner.read().await;
        let now = Instant::now();
        let mut loads = HashMap::with_capacity(inner.worker_assignments.len());
        for (worker_id, task_ids) in &inner.worker_assignments {
            let mut load = AssignedLoad::default();
            for task_id in task_ids {
                let Some(entry) = inner.tasks.get(task_id) else {
                    continue;
                };
                let attempt_state = if entry.assigned_worker_id == Some(*worker_id) {
                    entry.state
                } else if let Some(ref spec) = entry.speculative {
                    if spec.worker_id == *worker_id {
                        spec.state
                    } else {
                        continue;
                    }
                } else {
                    continue;
                };
                if !attempt_state.is_active() {
                    continue;
                };
                load.tasks += 1;
                load.slots += entry.task.requirements.cpu_cores.max(1);
                if attempt_state == TaskState::Scheduled {
                    load.unstarted_ram_mb += entry.task.requirements.ram_mb;
                }
                load.vram_mb = load
                    .vram_mb
                    .saturating_add(entry.task.requirements.effective_vram_mb());
            }
            if load.tasks > 0 {
                loads.insert(*worker_id, load);
            }
        }
        for task_id in &inner.held_tasks {
            let Some(entry) = inner.tasks.get(task_id) else {
                continue;
            };
            if entry.state == TaskState::Queued {
                if let Some((worker_id, until)) = entry.adoptable_by {
                    if now < until {
                        let load = loads.entry(worker_id).or_default();
                        load.tasks += 1;
                        load.slots += entry.task.requirements.cpu_cores.max(1);
                        load.vram_mb = load
                            .vram_mb
                            .saturating_add(entry.task.requirements.effective_vram_mb());
                    }
                }
            }
        }
        loads
    }

    /// For each worker with active (`Scheduled` or `Running`) or held tasks, returns
    /// `(task_key, elapsed)` pairs used by [`crate::scheduler::WorkloadScheduler`] to estimate
    /// when a slot on `best` next opens (`0060` D2 step 3).
    pub async fn active_running_tasks(
        &self,
    ) -> HashMap<Uuid, Vec<(crate::speed::TaskKey, Duration)>> {
        let inner = self.inner.read().await;
        let now = Instant::now();
        let mut running = HashMap::with_capacity(inner.worker_assignments.len());
        for (worker_id, task_ids) in &inner.worker_assignments {
            let mut items = Vec::new();
            for task_id in task_ids {
                let Some(entry) = inner.tasks.get(task_id) else {
                    continue;
                };
                let started = if entry.assigned_worker_id == Some(*worker_id) {
                    if !entry.state.is_active() {
                        continue;
                    }
                    entry.started_instant.or(entry.scheduled_instant)
                } else if let Some(ref spec) = entry.speculative {
                    if spec.worker_id != *worker_id || !spec.state.is_active() {
                        continue;
                    }
                    spec.started_instant.or(Some(spec.scheduled_instant))
                } else {
                    continue;
                };
                let elapsed = started.map_or(Duration::ZERO, |t| now.saturating_duration_since(t));
                items.push((entry.task_key, elapsed));
            }
            if !items.is_empty() {
                running.insert(*worker_id, items);
            }
        }
        for task_id in &inner.held_tasks {
            let Some(entry) = inner.tasks.get(task_id) else {
                continue;
            };
            if entry.state == TaskState::Queued {
                if let Some((worker_id, until)) = entry.adoptable_by {
                    if now < until {
                        let elapsed = entry
                            .started_instant
                            .or(entry.scheduled_instant)
                            .map_or(Duration::ZERO, |t| now.saturating_duration_since(t));
                        running
                            .entry(worker_id)
                            .or_default()
                            .push((entry.task_key, elapsed));
                    }
                }
            }
        }
        running
    }

    /// For each of `task_ids` that has rejected-by workers, the workers the scheduler should avoid.
    /// Tasks without any are omitted, so the usual result is an empty map.
    pub async fn avoided_workers_for(&self, task_ids: &[TaskId]) -> HashMap<TaskId, Vec<Uuid>> {
        let inner = self.inner.read().await;
        task_ids
            .iter()
            .filter_map(|id| {
                inner
                    .tasks
                    .get(id)
                    .filter(|e| !e.avoid_workers.is_empty())
                    .map(|e| (*id, e.avoid_workers.clone()))
            })
            .collect()
    }

    /// Returns a snapshot of `(workspace_affinity_map, active_workspace_names)` for a scheduling pass:
    /// - `workspace_affinity_map` maps each known workspace name to the worker UUID that last
    ///   finished (`Completed` or `Failed`) a task in that workspace.
    /// - `active_workspace_names` contains every workspace name that currently has a `Scheduled`
    ///   or `Running` task, or a task held for adoption (`adoptable_by` / `fail_if_not_adopted`),
    ///   in the queue.
    pub async fn workspace_scheduling_state(&self) -> (HashMap<String, Uuid>, HashSet<String>) {
        let inner = self.inner.read().await;
        let now = Instant::now();
        let affinities = inner
            .workspace_affinity
            .iter()
            .map(|(name, entry)| (name.clone(), entry.worker))
            .collect();
        let active = inner
            .workspace_active_tasks
            .keys()
            .filter(|name| inner.is_workspace_active(name, now))
            .cloned()
            .collect();
        (affinities, active)
    }

    /// Returns the worker UUID that last finished a task in `workspace_name`, if known.
    pub async fn workspace_affinity(&self, workspace_name: &str) -> Option<Uuid> {
        let inner = self.inner.read().await;
        inner
            .workspace_affinity
            .get(workspace_name)
            .map(|e| e.worker)
    }

    /// Returns all known workspace affinity entries sorted by workspace name.
    pub async fn list_workspaces(&self) -> Vec<WorkspaceAffinity> {
        let inner = self.inner.read().await;
        let mut list: Vec<WorkspaceAffinity> = inner.workspace_affinity.values().cloned().collect();
        list.sort_by(|a, b| a.name.cmp(&b.name));
        list
    }

    /// Applies a deferred failover charge (`entry.pending_failover_charge`) when an expired hold
    /// is dispatched before `release_expired_adoption_holds` ran.
    fn apply_pending_failover_charge_before_dispatch(
        inner: &mut TaskQueueInner,
        task_id: TaskId,
        now_epoch: u64,
    ) {
        let has_charge = inner
            .tasks
            .get(&task_id)
            .is_some_and(|e| e.pending_failover_charge);
        if !has_charge {
            return;
        }
        let (record, last_worker_id) = {
            let entry = inner.tasks.get_mut(&task_id).expect("checked above");
            let last_worker_id = entry.adoptable_by.map(|(w, _)| w);
            let reason = entry
                .pending_failover_reason
                .take()
                .unwrap_or_else(|| "session resume hold expired".into());
            entry.retry_count += 1;
            entry.failover_count += 1;
            entry.adoptable_by = None;
            entry.fail_if_not_adopted = false;
            entry.pending_failover_charge = false;
            let attempt = RetryAttempt {
                attempt: entry.retry_count,
                timestamp_utc: now_epoch,
                failed_worker_id: last_worker_id,
                reason: reason.clone(),
                exit_code: None,
                error: Some(format!("Worker evicted ({reason})")),
            };
            entry.retry_history.push(attempt.clone());
            (
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
                },
                last_worker_id,
            )
        };
        inner.track_adoption_hold(task_id, false);
        if let Some(wid) = last_worker_id {
            inner.refresh_worker_resume_protection(wid);
        }
        inner.append_non_durable(&record);
    }

    /// Pops the highest-priority queued task matching `predicate`, marks it `Scheduled` to `worker_id`,
    /// and records worker assignment.
    pub async fn pop_and_schedule<F>(&self, worker_id: Uuid, predicate: F) -> Option<Task>
    where
        F: Fn(&TaskRequirements) -> bool,
    {
        let (scheduled, ack) = {
            let mut inner = self.inner.write().await;
            let now = Instant::now();
            let now_epoch = Utc::now().timestamp() as u64;
            let policy = inner.retry_policy.clone();

            let found_key = inner
                .ready_queue
                .iter()
                .find(|k| {
                    if let Some(entry) = inner.tasks.get(&k.task_id) {
                        let ws_busy = entry
                            .task
                            .workspace
                            .as_ref()
                            .is_some_and(|ws| inner.is_workspace_active(&ws.name, now));
                        is_schedulable_queued(entry, &policy, now)
                            && !ws_busy
                            && predicate(&entry.task.requirements)
                    } else {
                        false
                    }
                })
                .copied();

            if let Some(key) = found_key {
                inner.remove_ready(&key);
                Self::apply_pending_failover_charge_before_dispatch(
                    &mut inner,
                    key.task_id,
                    now_epoch,
                );
                let metrics = Arc::clone(&inner.metrics);
                let (task, zero_retries, submitter) = {
                    let entry = inner.tasks.get_mut(&key.task_id)?;
                    mark_entry_scheduled(entry, worker_id, None, now, &policy, &metrics)
                };
                inner.fair_queues.on_scheduled(submitter);
                inner.track_adoption_hold(key.task_id, false);

                inner
                    .worker_assignments
                    .entry(worker_id)
                    .or_default()
                    .insert(key.task_id);
                inner.refresh_worker_resume_protection(worker_id);

                let record = JournalRecord::TaskDispatched {
                    task_id: key.task_id,
                    worker_id,
                    timestamp_utc: now_epoch,
                };
                let ack = if zero_retries {
                    inner.append_durable_warn(&record)
                } else {
                    inner.append_non_durable(&record);
                    None
                };
                inner.maybe_trigger_compaction();

                (Some(task), ack)
            } else {
                (None, None)
            }
        };

        if let Some(ack) = ack {
            let _ = ack.await;
        }
        scheduled
    }

    /// Explicitly transitions a specific Queued task to Scheduled for the designated worker.
    ///
    /// Atomically removes the task from `ready_queue`, sets state to `Scheduled`,
    /// and records worker assignment. Returns `Some(Task)` if successful, or `None`
    /// if the task is no longer in `Queued` state (e.g. cancelled concurrently).
    pub async fn schedule_task(&self, task_id: &TaskId, worker_id: Uuid) -> Option<Task> {
        let (task, ack) = {
            let mut inner = self.inner.write().await;
            let now = Instant::now();
            let now_epoch = Utc::now().timestamp() as u64;
            let policy = inner.retry_policy.clone();
            let priority_info = {
                let entry = inner.tasks.get(task_id)?;
                if !is_schedulable_queued(entry, &policy, now) {
                    return None;
                }
                if entry
                    .task
                    .workspace
                    .as_ref()
                    .is_some_and(|ws| inner.is_workspace_active(&ws.name, now))
                {
                    return None;
                }
                (entry.priority, entry.sequence)
            };

            inner.remove_ready(&QueueOrderKey {
                priority_rev: Reverse(priority_info.0),
                sequence: priority_info.1,
                task_id: *task_id,
            });
            Self::apply_pending_failover_charge_before_dispatch(&mut inner, *task_id, now_epoch);

            let metrics = Arc::clone(&inner.metrics);
            let (task, zero_retries, submitter) = {
                let entry = inner.tasks.get_mut(task_id)?;
                mark_entry_scheduled(entry, worker_id, None, now, &policy, &metrics)
            };
            inner.fair_queues.on_scheduled(submitter);
            inner.track_adoption_hold(*task_id, false);

            inner
                .worker_assignments
                .entry(worker_id)
                .or_default()
                .insert(*task_id);
            inner.refresh_worker_resume_protection(worker_id);

            let record = JournalRecord::TaskDispatched {
                task_id: *task_id,
                worker_id,
                timestamp_utc: now_epoch,
            };
            let ack = if zero_retries {
                inner.append_durable_warn(&record)
            } else {
                inner.append_non_durable(&record);
                None
            };
            inner.maybe_trigger_compaction();
            (task, ack)
        };

        if let Some(ack) = ack {
            let _ = ack.await;
        }
        Some(task)
    }

    /// Atomically transitions a batch of tasks to Scheduled state under a single write lock.
    ///
    /// Accepts a slice of `(TaskId, Uuid)` pairs and returns `Vec<(TaskId, Uuid, Task)>`
    /// containing all tasks that were successfully transitioned from `Queued` to `Scheduled`.
    /// Any task that is no longer in `Queued` state (e.g. cancelled concurrently) is skipped.
    pub async fn schedule_tasks_batch(
        &self,
        assignments: &[(TaskId, Uuid)],
    ) -> Vec<(TaskId, Uuid, Task)> {
        let with_sessions: Vec<(TaskId, Uuid, Option<u64>)> = assignments
            .iter()
            .map(|&(task_id, worker_id)| (task_id, worker_id, None))
            .collect();
        self.schedule_batch_inner(&with_sessions).await
    }

    /// Like [`schedule_tasks_batch`](Self::schedule_tasks_batch), recording which connection
    /// session each task was assigned on so that only that session may report on it.
    pub async fn schedule_tasks_batch_in_sessions(
        &self,
        assignments: &[(TaskId, Uuid, u64)],
    ) -> Vec<(TaskId, Uuid, Task)> {
        let with_sessions: Vec<(TaskId, Uuid, Option<u64>)> = assignments
            .iter()
            .map(|&(task_id, worker_id, session_id)| (task_id, worker_id, Some(session_id)))
            .collect();
        self.schedule_batch_inner(&with_sessions).await
    }

    async fn schedule_batch_inner(
        &self,
        assignments: &[(TaskId, Uuid, Option<u64>)],
    ) -> Vec<(TaskId, Uuid, Task)> {
        if assignments.is_empty() {
            return Vec::new();
        }

        let (scheduled, acks) = {
            let mut inner = self.inner.write().await;
            let now = Instant::now();
            let now_epoch = Utc::now().timestamp() as u64;
            let policy = inner.retry_policy.clone();
            let mut scheduled = Vec::with_capacity(assignments.len());
            let mut acks = Vec::new();

            for &(task_id, worker_id, session_id) in assignments {
                let (priority, sequence) = match inner.tasks.get(&task_id) {
                    Some(entry) if is_schedulable_queued(entry, &policy, now) => {
                        if entry
                            .task
                            .workspace
                            .as_ref()
                            .is_some_and(|ws| inner.is_workspace_active(&ws.name, now))
                        {
                            continue;
                        }
                        (entry.priority, entry.sequence)
                    }
                    Some(entry) if entry.state == TaskState::Queued => continue,
                    Some(entry) => {
                        // A ready key can only be stale here (the entry left Queued without
                        // dropping it); keeping it would be re-offered to every scheduling pass.
                        let stale = QueueOrderKey {
                            priority_rev: Reverse(entry.priority),
                            sequence: entry.sequence,
                            task_id,
                        };
                        inner.remove_ready(&stale);
                        continue;
                    }
                    None => continue,
                };

                inner.remove_ready(&QueueOrderKey {
                    priority_rev: Reverse(priority),
                    sequence,
                    task_id,
                });
                Self::apply_pending_failover_charge_before_dispatch(&mut inner, task_id, now_epoch);

                let metrics = Arc::clone(&inner.metrics);
                let mut dispatched_submitter = None;
                if let Some((task, zero_retries, submitter)) =
                    inner.tasks.get_mut(&task_id).map(|entry| {
                        mark_entry_scheduled(entry, worker_id, session_id, now, &policy, &metrics)
                    })
                {
                    dispatched_submitter = Some(submitter);
                    inner.track_adoption_hold(task_id, false);

                    inner
                        .worker_assignments
                        .entry(worker_id)
                        .or_default()
                        .insert(task_id);
                    inner.refresh_worker_resume_protection(worker_id);

                    let record = JournalRecord::TaskDispatched {
                        task_id,
                        worker_id,
                        timestamp_utc: now_epoch,
                    };
                    if zero_retries {
                        if let Some(ack) = inner.append_durable_warn(&record) {
                            acks.push(ack);
                        }
                    } else {
                        inner.append_non_durable(&record);
                    }

                    scheduled.push((task_id, worker_id, task));
                }
                if let Some(submitter) = dispatched_submitter {
                    inner.fair_queues.on_scheduled(submitter);
                }
            }

            if !scheduled.is_empty() {
                inner.maybe_trigger_compaction();
            }
            (scheduled, acks)
        };

        for ack in acks {
            let _ = ack.await;
        }
        scheduled
    }

    /// Re-enqueues a task back to `Queued` status if dispatch to the worker failed.
    ///
    /// Refuses to resurrect a finished task.
    pub async fn requeue_task(&self, task_id: &TaskId) -> GridResult<()> {
        let ack = {
            let mut inner = self.inner.write().await;
            let prev_worker = {
                let entry = inner
                    .tasks
                    .get_mut(task_id)
                    .ok_or(GridError::TaskNotFound(task_id.0))?;
                if entry.state.is_terminal() {
                    return Err(GridError::InvalidTaskState {
                        task_id: task_id.0,
                        current: format!("{:?}", entry.state),
                        attempted: "Queued".into(),
                    });
                }
                entry.assigned_session = None;
                entry.assigned_worker_id.take()
            };

            if let Some(wid) = prev_worker {
                if let Some(tasks) = inner.worker_assignments.get_mut(&wid) {
                    tasks.remove(task_id);
                }
                inner.refresh_worker_resume_protection(wid);
            }

            let (priority, sequence, record) = {
                let entry = inner
                    .tasks
                    .get_mut(task_id)
                    .ok_or(GridError::TaskNotFound(task_id.0))?;
                entry.state = TaskState::Queued;
                entry.scheduled_instant = None;
                let record = JournalRecord::TaskRequeued {
                    task_id: *task_id,
                    state: TaskState::Queued,
                    retry_count: entry.retry_count,
                    failover_count: entry.failover_count,
                    evacuation_count: entry.evacuation_count,
                    rejection_count: entry.rejection_count,
                    avoid_workers: entry.avoid_workers.clone(),
                    retry_attempt: None,
                    retry_delay_ms: None,
                };
                (entry.priority, entry.sequence, record)
            };

            inner.sync_workspace_active(*task_id);
            inner.close_run_span(task_id);
            inner.insert_ready(QueueOrderKey {
                priority_rev: Reverse(priority),
                sequence,
                task_id: *task_id,
            });
            let ack = inner.append_durable_warn(&record);
            inner.maybe_trigger_compaction();
            ack
        };

        if let Some(ack) = ack {
            if let Err(err) = ack.await {
                tracing::warn!(error = %err, "Failed to sync requeue_task journal record");
            }
        }
        Ok(())
    }

    /// Runs `deliver` (which must not block) only while `task_id` is still `Scheduled` on exactly
    /// `worker_id`'s `session_id`, and returns its result; `None` means the task is no longer
    /// there and nothing was delivered.
    ///
    /// The queue is read-locked for the whole call, so a cancel, failover or reassignment cannot
    /// slip between the check and the delivery: either it already happened (nothing is sent) or it
    /// happens afterwards, and whatever it sends to the worker (`CancelTask`) follows what
    /// `deliver` queued. A pass marks up to a whole batch `Scheduled` first and delivers them one
    /// by one, so without this a task cancelled in between reached the worker after its `CancelTask`.
    pub async fn deliver_if_scheduled<R>(
        &self,
        task_id: &TaskId,
        worker_id: &Uuid,
        session_id: u64,
        deliver: impl FnOnce() -> R,
    ) -> Option<R> {
        let inner = self.inner.read().await;
        let entry = inner.tasks.get(task_id)?;
        if entry.state != TaskState::Scheduled || !owned_by(entry, worker_id, Some(session_id)) {
            return None;
        }
        Some(deliver())
    }

    /// Puts back a task whose `AssignTask` could not be delivered to `worker_id`'s `session_id`.
    ///
    /// The task never reached the worker, so no retry or failover budget is charged. It is only
    /// requeued while it is still `Scheduled` on exactly that worker and session; a task that was
    /// cancelled, failed over or re-assigned in the meantime is left alone. Returns whether it was requeued.
    pub async fn requeue_dispatch_failure(
        &self,
        task_id: &TaskId,
        worker_id: &Uuid,
        session_id: u64,
    ) -> GridResult<bool> {
        let mut inner = self.inner.write().await;
        let (priority, sequence, record) = {
            let entry = inner
                .tasks
                .get_mut(task_id)
                .ok_or(GridError::TaskNotFound(task_id.0))?;
            if entry.state != TaskState::Scheduled || !owned_by(entry, worker_id, Some(session_id))
            {
                return Ok(false);
            }
            entry.state = TaskState::Queued;
            entry.assigned_worker_id = None;
            entry.assigned_session = None;
            entry.scheduled_instant = None;
            let record = JournalRecord::TaskRequeued {
                task_id: *task_id,
                state: TaskState::Queued,
                retry_count: entry.retry_count,
                failover_count: entry.failover_count,
                evacuation_count: entry.evacuation_count,
                rejection_count: entry.rejection_count,
                avoid_workers: entry.avoid_workers.clone(),
                retry_attempt: None,
                retry_delay_ms: None,
            };
            (entry.priority, entry.sequence, record)
        };
        if let Some(tasks) = inner.worker_assignments.get_mut(worker_id) {
            tasks.remove(task_id);
        }
        inner.sync_workspace_active(*task_id);
        inner.close_run_span(task_id);
        inner.refresh_worker_resume_protection(*worker_id);
        inner.insert_ready(QueueOrderKey {
            priority_rev: Reverse(priority),
            sequence,
            task_id: *task_id,
        });
        inner.append_non_durable(&record);
        inner.maybe_trigger_compaction();
        Ok(true)
    }

    // --- Worker Telemetry & Result Handling ---

    /// Transitions a Scheduled task to Running upon worker progress telemetry.
    ///
    /// Only the worker the task is assigned to may move it; see [`mark_running_from`](Self::mark_running_from).
    pub async fn mark_running(&self, task_id: &TaskId, worker_id: Uuid) -> GridResult<()> {
        self.mark_running_from(worker_id, None, task_id).await
    }

    /// `Scheduled -> Running` on behalf of the connection `(worker_id, session_id)`.
    ///
    /// Rejected with `InvalidTaskState` unless the task is assigned to exactly that worker (and
    /// session, when known); progress never reassigns a task.
    pub async fn mark_running_from(
        &self,
        worker_id: Uuid,
        session_id: Option<u64>,
        task_id: &TaskId,
    ) -> GridResult<()> {
        let ack = {
            let mut inner = self.inner.write().await;
            let (is_speculative, zero_retries) = {
                let entry = inner
                    .tasks
                    .get(task_id)
                    .ok_or(GridError::TaskNotFound(task_id.0))?;

                if owned_by_speculative(entry, &worker_id, session_id) {
                    let spec_state = entry
                        .speculative
                        .as_ref()
                        .map_or(TaskState::Running, |s| s.state);
                    if !spec_state.can_transition_to(TaskState::Running) {
                        return Err(GridError::InvalidTaskState {
                            task_id: task_id.0,
                            current: format!("{spec_state:?}"),
                            attempted: "Running".into(),
                        });
                    }
                    (true, false)
                } else {
                    if !owned_by(entry, &worker_id, session_id) {
                        return Err(GridError::InvalidTaskState {
                            task_id: task_id.0,
                            current: format!(
                                "{:?} (not assigned to this worker/session)",
                                entry.state
                            ),
                            attempted: "Running".into(),
                        });
                    }

                    if !entry.state.can_transition_to(TaskState::Running) {
                        return Err(GridError::InvalidTaskState {
                            task_id: task_id.0,
                            current: format!("{:?}", entry.state),
                            attempted: "Running".into(),
                        });
                    }
                    (false, inner.retry_policy.zero_retries_for(entry))
                }
            };

            if is_speculative {
                if let Some(entry) = inner.tasks.get_mut(task_id) {
                    if let Some(ref mut spec) = entry.speculative {
                        spec.state = TaskState::Running;
                        spec.started_instant = Some(Instant::now());
                    }
                }
                return Ok(());
            }

            let mut run_attempt = None;
            if let Some(entry) = inner.tasks.get_mut(task_id) {
                entry.state = TaskState::Running;
                entry.started_instant = Some(Instant::now());
                run_attempt = Some(i64::from(entry.retry_count));
            }
            inner.sync_workspace_active(*task_id);
            #[cfg(feature = "otel")]
            if let Some(attempt) = run_attempt {
                inner.close_run_span(task_id);
                let run_span = tracing::info_span!(
                    target: crate::otel::OTEL_SPAN_TARGET,
                    "run",
                    task_id = %task_id,
                    worker_id = %worker_id,
                    attempt = attempt,
                );
                inner.run_spans.insert(*task_id, run_span);
            }
            #[cfg(not(feature = "otel"))]
            let _ = run_attempt;

            let record = JournalRecord::TaskRunning {
                task_id: *task_id,
                worker_id,
                timestamp_utc: Utc::now().timestamp() as u64,
            };
            let ack = if zero_retries {
                inner.append_durable_warn(&record)
            } else {
                inner.append_non_durable(&record);
                None
            };
            inner.maybe_trigger_compaction();
            ack
        };

        if let Some(ack) = ack {
            if let Err(err) = ack.await {
                tracing::warn!(error = %err, "Failed to sync TaskRunning journal record");
            }
        }
        Ok(())
    }

    /// Returns `(in_flight_speculative_slots, running_repeatable_candidates)` for the reaper's
    /// periodic straggler pass (`0060` D3).
    pub async fn speculative_snapshot(&self) -> (usize, Vec<StragglerCandidate>) {
        let inner = self.inner.read().await;
        let now = Instant::now();
        let mut in_flight_spec_slots = 0usize;
        let mut candidates = Vec::new();
        for entry in inner.tasks.values() {
            if !entry.state.is_active() {
                continue;
            }
            if entry.speculative.is_some() {
                in_flight_spec_slots += entry.task.requirements.cpu_cores.max(1);
                continue;
            }
            if entry.state != TaskState::Running
                || entry.speculative_spawned
                || entry.task.workspace.is_some()
                || !entry.task.is_repeatable()
            {
                continue;
            }
            let (Some(primary_worker_id), Some(started)) =
                (entry.assigned_worker_id, entry.started_instant)
            else {
                continue;
            };
            let mut view = TaskView::of_with_key(&entry.task, entry.task_key);
            if entry.requires_tree_v2 {
                view.required_features.insert(ProtocolCapabilities::TREE_V2);
            }
            view.avoid = entry.avoid_workers.clone();
            candidates.push(StragglerCandidate {
                view,
                primary_worker_id,
                elapsed: now.saturating_duration_since(started),
            });
        }
        (in_flight_spec_slots, candidates)
    }

    /// Attempts to register a speculative straggler copy on `(worker_id, session_id)` for a
    /// currently `Running` repeatable task (`0060` D3). Returns `Some(task)` on success.
    #[cfg(test)]
    pub async fn try_start_speculative(
        &self,
        task_id: TaskId,
        worker_id: Uuid,
        session_id: u64,
    ) -> Option<Task> {
        let mut inner = self.inner.write().await;
        let task = {
            let entry = inner.tasks.get_mut(&task_id)?;
            if entry.state != TaskState::Running
                || entry.speculative.is_some()
                || entry.speculative_spawned
                || entry.task.workspace.is_some()
                || !entry.task.is_repeatable()
                || entry.assigned_worker_id == Some(worker_id)
            {
                return None;
            }
            entry.speculative_spawned = true;
            entry.speculative = Some(SpeculativeAttempt {
                worker_id,
                session_id,
                scheduled_instant: Instant::now(),
                started_instant: None,
                state: TaskState::Scheduled,
            });
            entry.task.clone()
        };
        inner
            .worker_assignments
            .entry(worker_id)
            .or_default()
            .insert(task_id);
        inner.refresh_worker_resume_protection(worker_id);
        Some(task)
    }

    /// Atomically checks eligibility, checks `peer_version` extension compatibility, delivers the
    /// prepared `Task` via non-blocking `deliver`, and records `entry.speculative` under a single
    /// `queue.inner.write()` lock. Because no `.await` occurs between recording `entry.speculative`
    /// and queuing `AssignTask`, a concurrent primary failure/promotion cannot strand the task in
    /// `Scheduled` without delivery, and a concurrent primary completion/cancel cannot double-call
    /// `release_dispatch`.
    pub async fn try_start_speculative_and_deliver(
        &self,
        task_id: TaskId,
        worker_id: Uuid,
        session_id: u64,
        peer_version: Option<u32>,
        deliver: impl FnOnce(Task) -> bool,
    ) -> SpeculativeStartOutcome {
        let mut inner = self.inner.write().await;
        let prepared = {
            let Some(entry) = inner.tasks.get(&task_id) else {
                return SpeculativeStartOutcome::NotEligible;
            };
            if entry.state != TaskState::Running
                || entry.speculative.is_some()
                || entry.speculative_spawned
                || entry.task.workspace.is_some()
                || !entry.task.is_repeatable()
                || entry.assigned_worker_id == Some(worker_id)
            {
                return SpeculativeStartOutcome::NotEligible;
            }
            if peer_version.is_some_and(|v| entry.task.withheld_from_peer(v)) {
                return SpeculativeStartOutcome::WithheldFromPeer;
            }
            let mut task = entry.task.clone();
            task.latest_checkpoint = None;
            task.strip_master_extensions();
            task
        };

        if !deliver(prepared) {
            return SpeculativeStartOutcome::DeliveryFailed;
        }

        if let Some(entry) = inner.tasks.get_mut(&task_id) {
            entry.speculative_spawned = true;
            entry.speculative = Some(SpeculativeAttempt {
                worker_id,
                session_id,
                scheduled_instant: Instant::now(),
                started_instant: None,
                state: TaskState::Scheduled,
            });
        }
        inner
            .worker_assignments
            .entry(worker_id)
            .or_default()
            .insert(task_id);
        inner.refresh_worker_resume_protection(worker_id);
        SpeculativeStartOutcome::Delivered
    }
}
