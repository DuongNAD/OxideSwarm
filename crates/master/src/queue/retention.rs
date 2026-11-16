//! Task queries, queue metrics, hot/cold terminal retention, and eviction.

use std::cmp::Reverse;
use std::sync::Arc;

use rusty_grid_core::task::{Task, TaskId, TaskResult};

use crate::cold_results::ColdResultSummary;

use super::{
    QueueStats, RetryAttempt, TaskEntry, TaskInfo, TaskQueue, TaskState, TaskStatusDetail,
};

impl TaskQueue {
    // --- Queries ---

    /// Retrieves task metadata snapshot.
    pub async fn get_task(&self, task_id: &TaskId) -> Option<TaskInfo> {
        let inner = self.inner.read().await;
        inner.tasks.get(task_id).map(|e| e.to_info())
    }

    /// Retrieves task metadata and terminal exit code/error purely from RAM without loading
    /// spilled cold result files from disk.
    pub async fn get_status_detail(&self, task_id: &TaskId) -> Option<TaskStatusDetail> {
        let inner = self.inner.read().await;
        let entry = inner.tasks.get(task_id)?;
        let (result_exit_code, result_error) = if let Some(ref res) = entry.result {
            (Some(res.exit_code), res.error.clone())
        } else if let Some(ref cold) = entry.cold_result {
            (Some(cold.exit_code), cold.error.clone())
        } else {
            (None, None)
        };
        Some(TaskStatusDetail {
            info: entry.to_info(),
            result_exit_code,
            result_error,
        })
    }

    /// Retrieves full task specification.
    pub async fn get_task_spec(&self, task_id: &TaskId) -> Option<Task> {
        let inner = self.inner.read().await;
        inner.tasks.get(task_id).map(|e| e.task.clone())
    }

    /// Retrieves current lifecycle state.
    pub async fn get_state(&self, task_id: &TaskId) -> Option<TaskState> {
        let inner = self.inner.read().await;
        inner.tasks.get(task_id).map(|e| e.state)
    }

    /// Retrieves recorded task execution result, if finished, transparently reading from cold
    /// out-of-band storage when the result was spilled out of RAM.
    pub async fn get_result(&self, task_id: &TaskId) -> Option<TaskResult> {
        let cold_lookup = {
            let inner = self.inner.read().await;
            let entry = inner.tasks.get(task_id)?;
            if let Some(ref res) = entry.result {
                return Some(res.clone());
            }
            if !entry.state.is_terminal() || entry.cold_result.is_none() {
                return None;
            }
            inner.cold_results.clone()
        }?;
        let tid = *task_id;
        match tokio::task::spawn_blocking(move || cold_lookup.get(&tid)).await {
            Ok(Ok(res)) => res,
            Ok(Err(err)) => {
                tracing::warn!(task_id = %task_id.0, error = %err, "Failed to read cold task result");
                None
            }
            Err(err) => {
                tracing::warn!(task_id = %task_id.0, error = %err, "Cold result reader task panicked");
                None
            }
        }
    }

    /// Retrieves the recorded retry and evacuation history for a task.
    pub async fn get_retry_history(&self, task_id: &TaskId) -> Option<Vec<RetryAttempt>> {
        let inner = self.inner.read().await;
        inner.tasks.get(task_id).map(|e| e.retry_history.clone())
    }

    /// Returns a snapshot of all managed tasks.
    pub async fn list_tasks(&self) -> Vec<TaskInfo> {
        let inner = self.inner.read().await;
        inner.tasks.values().map(|e| e.to_info()).collect()
    }

    /// A page of the tasks `filter` accepts, newest submission first (`offset` entries skipped,
    /// at most `limit` returned). Unlike [`list_tasks`](Self::list_tasks) the order is defined, so
    /// "the last 20 tasks" means exactly that, and only the page is converted to `TaskInfo`.
    pub async fn recent_tasks<F: Fn(&TaskEntry) -> bool>(
        &self,
        filter: F,
        offset: usize,
        limit: usize,
    ) -> Vec<TaskInfo> {
        let inner = self.inner.read().await;
        let mut matching: Vec<&TaskEntry> = inner.tasks.values().filter(|e| filter(e)).collect();
        let wanted = offset.saturating_add(limit);
        if matching.len() > wanted && wanted > 0 {
            // Partial selection: only the `wanted` newest need ordering, not the whole table.
            matching.select_nth_unstable_by_key(wanted - 1, |e| Reverse(e.sequence));
            matching.truncate(wanted);
        }
        matching.sort_unstable_by_key(|e| Reverse(e.sequence));
        matching
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|e| e.to_info())
            .collect()
    }

    /// Returns aggregated queue statistics.
    pub async fn stats(&self) -> QueueStats {
        let inner = self.inner.read().await;
        let mut stats = QueueStats {
            total: inner.tasks.len(),
            active_bytes: inner.active_bytes,
            max_active_bytes: inner.max_active_bytes,
            store_error: inner.store.as_ref().and_then(|s| s.failure_reason()),
            ..Default::default()
        };

        for entry in inner.tasks.values() {
            match entry.state {
                TaskState::Submitted => {}
                TaskState::Queued => stats.queued += 1,
                TaskState::Scheduled => stats.scheduled += 1,
                TaskState::Running => stats.running += 1,
                TaskState::Retrying => stats.retrying += 1,
                TaskState::Completed => stats.completed += 1,
                TaskState::Failed => stats.failed += 1,
                TaskState::Cancelled => stats.cancelled += 1,
            }
        }

        stats
    }

    /// Returns the error reason if the attached `TaskStore` writer has failed since startup, or
    /// `None` when durability is healthy (or no `--state-dir` is configured).
    pub async fn store_error(&self) -> Option<String> {
        let inner = self.inner.read().await;
        inner.store.as_ref().and_then(|s| s.failure_reason())
    }

    /// Returns the lock-free queue counters and scheduling latency histogram handle.
    pub fn metrics(&self) -> &Arc<crate::metrics::QueueMetrics> {
        &self.metrics
    }

    /// Captures durable store health, journal counters, and fsync duration histogram for `/metrics`.
    pub async fn store_metrics_snapshot(&self) -> crate::metrics::StoreMetricsSnapshot {
        let inner = self.inner.read().await;
        match inner.store.as_ref() {
            Some(store) => crate::metrics::StoreMetricsSnapshot {
                configured: true,
                healthy: store.failure_reason().is_none(),
                journal_bytes: store.journal_bytes(),
                journal_records: store.journal_records(),
                fsync_duration: store.fsync_histogram().snapshot(),
            },
            None => crate::metrics::StoreMetricsSnapshot::default(),
        }
    }

    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn fail_store_for_test(&self, reason: impl Into<String>) {
        let inner = self.inner.read().await;
        if let Some(ref store) = inner.store {
            store.record_failure_for_test(reason);
        }
    }

    /// Dynamically adjusts the retention limit for terminal tasks and evicts oldest tasks exceeding this limit.
    pub async fn set_max_terminal_tasks(&self, limit: usize) {
        {
            let mut inner = self.inner.write().await;
            inner.max_terminal_tasks = limit;
            inner.enforce_terminal_limits();
            inner.maybe_trigger_compaction();
        }
        self.flush_cold_maintenance().await;
    }

    /// Dynamically adjusts the byte budget of retained terminal tasks and evicts oldest tasks
    /// until it holds (see [`DEFAULT_MAX_TERMINAL_BYTES`](super::DEFAULT_MAX_TERMINAL_BYTES)).
    pub async fn set_max_terminal_bytes(&self, limit: usize) {
        {
            let mut inner = self.inner.write().await;
            inner.max_terminal_bytes = limit;
            inner.enforce_terminal_limits();
            inner.maybe_trigger_compaction();
        }
        self.flush_cold_maintenance().await;
    }

    /// Configures the hot in-RAM terminal result limits and spills excess results if a durable
    /// store is attached.
    pub async fn set_hot_terminal_limits(&self, max_tasks: usize, max_bytes: usize) {
        {
            let mut inner = self.inner.write().await;
            inner.max_hot_terminal_tasks = max_tasks;
            inner.max_hot_terminal_bytes = max_bytes;
        }
        self.flush_cold_maintenance().await;
    }

    /// Returns `(hot_terminal_tasks, hot_terminal_bytes)` currently held in RAM.
    pub async fn hot_terminal_stats(&self) -> (usize, usize) {
        let inner = self.inner.read().await;
        (inner.hot_terminal_sizes.len(), inner.hot_terminal_bytes)
    }

    async fn needs_cold_maintenance(&self) -> bool {
        let inner = self.inner.read().await;
        if inner.cold_results.is_none() {
            return false;
        }
        let need_spill = inner.hot_terminal_sizes.len() > inner.max_hot_terminal_tasks
            || inner.hot_terminal_bytes > inner.max_hot_terminal_bytes;
        !inner.pending_cold_deletes.is_empty() || need_spill
    }

    async fn flush_cold_maintenance_step(&self) -> bool {
        let (store, candidates, deletes) = {
            let mut inner = self.inner.write().await;
            let deletes = std::mem::take(&mut inner.pending_cold_deletes);
            let Some(ref store) = inner.cold_results else {
                return false;
            };
            let store = Arc::clone(store);
            let spill_ids = inner.collect_cold_spill_ids();
            let mut candidates: Vec<(TaskId, TaskResult, ColdResultSummary)> =
                Vec::with_capacity(spill_ids.len());
            for tid in spill_ids {
                if let (Some(&hsize), Some(res)) = (
                    inner.hot_terminal_sizes.get(&tid),
                    inner.tasks.get(&tid).and_then(|e| e.result.as_ref()),
                ) {
                    let summary = ColdResultSummary::from_result(res, hsize);
                    candidates.push((tid, res.clone(), summary));
                }
            }
            (store, candidates, deletes)
        };

        let had_deletes = !deletes.is_empty();
        if candidates.is_empty() && !had_deletes {
            return false;
        }

        let store_bg = Arc::clone(&store);
        let written = tokio::task::spawn_blocking(move || {
            for tid in &deletes {
                store_bg.remove(tid);
            }
            store_bg.put_batch(candidates)
        })
        .await
        .unwrap_or_default();

        if written.is_empty() {
            return had_deletes;
        }

        let mut orphan_deletes = Vec::new();
        {
            let mut inner = self.inner.write().await;
            let same_store = inner
                .cold_results
                .as_ref()
                .is_some_and(|s| Arc::ptr_eq(s, &store));
            if !same_store {
                return false;
            }
            for (tid, summary) in &written {
                let committed = match inner.tasks.get_mut(tid) {
                    Some(entry) if entry.state.is_terminal() && entry.result.is_some() => {
                        entry.result = None;
                        entry.cold_result = Some(summary.clone());
                        true
                    }
                    Some(entry) if entry.state.is_terminal() && entry.cold_result.is_some() => true,
                    _ => false,
                };
                if committed {
                    if let Some(hsize) = inner.hot_terminal_sizes.remove(tid) {
                        inner.hot_terminal_bytes = inner.hot_terminal_bytes.saturating_sub(hsize);
                    }
                } else {
                    orphan_deletes.push(*tid);
                }
            }
            let inner_ref = &mut *inner;
            inner_ref
                .hot_terminal_order
                .retain(|id| inner_ref.hot_terminal_sizes.contains_key(id));
        }

        if !orphan_deletes.is_empty() {
            let _ = tokio::task::spawn_blocking(move || {
                for tid in &orphan_deletes {
                    store.remove(tid);
                }
            })
            .await;
        }
        true
    }

    async fn run_cold_maintenance_locked(&self, mut guard: tokio::sync::OwnedMutexGuard<()>) {
        loop {
            loop {
                self.cold_maintenance_requested
                    .store(false, std::sync::atomic::Ordering::SeqCst);
                if !self.needs_cold_maintenance().await {
                    break;
                }
                if !self.flush_cold_maintenance_step().await {
                    break;
                }
                if !self
                    .cold_maintenance_requested
                    .load(std::sync::atomic::Ordering::SeqCst)
                    && !self.needs_cold_maintenance().await
                {
                    break;
                }
            }
            drop(guard);
            if self
                .cold_maintenance_requested
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                if let Ok(next_guard) = Arc::clone(&self.cold_flush_lock).try_lock_owned() {
                    guard = next_guard;
                    continue;
                }
            }
            break;
        }
    }

    /// Flushes pending cold-result spills and unlinks cold files of evicted tasks outside the
    /// queue lock. No-ops quickly when no `ColdResultStore` is attached or limits hold.
    pub(crate) async fn flush_cold_maintenance(&self) {
        if !self.needs_cold_maintenance().await {
            return;
        }
        self.cold_maintenance_requested
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let guard = Arc::clone(&self.cold_flush_lock).lock_owned().await;
        self.run_cold_maintenance_locked(guard).await;
    }

    /// Triggers single-flight asynchronous cold-result maintenance without stalling task result
    /// delivery or spawning overlapping workers under burst load.
    pub(crate) async fn spawn_cold_maintenance(&self) {
        if !self.needs_cold_maintenance().await {
            return;
        }
        self.cold_maintenance_requested
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if let Ok(guard) = Arc::clone(&self.cold_flush_lock).try_lock_owned() {
            let queue = self.clone();
            tokio::spawn(async move {
                queue.run_cold_maintenance_locked(guard).await;
            });
        }
    }

    /// Bytes currently charged for retained terminal tasks.
    pub async fn terminal_retained_bytes(&self) -> usize {
        self.inner.read().await.terminal_bytes
    }

    /// Bytes currently charged for active (non-terminal) tasks.
    pub async fn active_retained_bytes(&self) -> usize {
        self.inner.read().await.active_bytes
    }

    /// Evicts oldest terminal tasks until at most `max_retained` terminal tasks remain.
    ///
    /// Returns the number of evicted tasks.
    pub async fn evict_terminal_tasks(&self, max_retained: usize) -> usize {
        let evicted = {
            let mut inner = self.inner.write().await;
            let evicted = inner.evict_terminal_tasks(max_retained);
            if evicted > 0 {
                inner.maybe_trigger_compaction();
            }
            evicted
        };
        if evicted > 0 {
            self.flush_cold_maintenance().await;
        }
        evicted
    }

    #[cfg(test)]
    pub(crate) async fn inject_store_sync_failure(&self, mode: u8) {
        let inner = self.inner.read().await;
        if let Some(ref store) = inner.store {
            store.set_inject(mode);
        }
    }
}
