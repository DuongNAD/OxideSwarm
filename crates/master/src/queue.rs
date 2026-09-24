//! Thread-safe Task Queue and 8-State Finite State Machine (FSM) for `rusty_grid_master`.
//!
//! Provides priority-FIFO indexing, O(1) task lookup, worker assignment tracking,
//! configurable exponential backoff retries, and atomic state transitions.

use std::cmp::Reverse;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use uuid::Uuid;

use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::task::{CheckpointData, Task, TaskId, TaskRequirements, TaskResult, TaskStatus};

/// 8-State Task Lifecycle Finite State Machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Submitted,
    Queued,
    Scheduled,
    Running,
    Completed,
    Failed,
    Cancelled,
    Retrying,
}

impl TaskState {
    /// Validates whether transitioning from `self` to `next` is permissible under the FSM.
    pub fn can_transition_to(&self, next: TaskState) -> bool {
        match self {
            TaskState::Submitted => matches!(
                next,
                TaskState::Queued | TaskState::Cancelled | TaskState::Failed
            ),
            TaskState::Queued => matches!(
                next,
                TaskState::Scheduled | TaskState::Cancelled | TaskState::Failed
            ),
            TaskState::Scheduled => matches!(
                next,
                TaskState::Running
                    | TaskState::Completed
                    | TaskState::Failed
                    | TaskState::Retrying
                    | TaskState::Queued
                    | TaskState::Cancelled
            ),
            TaskState::Running => matches!(
                next,
                TaskState::Completed
                    | TaskState::Failed
                    | TaskState::Retrying
                    | TaskState::Queued
                    | TaskState::Cancelled
            ),
            TaskState::Retrying => matches!(
                next,
                TaskState::Queued | TaskState::Failed | TaskState::Cancelled
            ),
            TaskState::Completed | TaskState::Failed | TaskState::Cancelled => false,
        }
    }

    /// Returns true if this state represents a final, immutable lifecycle end-state.
    #[inline]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskState::Completed | TaskState::Failed | TaskState::Cancelled
        )
    }

    /// Returns true if this state represents an active execution assignment on a worker node.
    #[inline]
    pub fn is_active(&self) -> bool {
        matches!(self, TaskState::Scheduled | TaskState::Running)
    }
}

impl From<TaskState> for TaskStatus {
    fn from(state: TaskState) -> Self {
        match state {
            TaskState::Submitted | TaskState::Queued => TaskStatus::Queued,
            TaskState::Scheduled => TaskStatus::Assigned,
            TaskState::Running => TaskStatus::Running,
            TaskState::Completed => TaskStatus::Completed,
            TaskState::Failed => TaskStatus::Failed,
            TaskState::Cancelled => TaskStatus::Cancelled,
            TaskState::Retrying => TaskStatus::Retried,
        }
    }
}

impl From<TaskStatus> for TaskState {
    fn from(status: TaskStatus) -> Self {
        match status {
            TaskStatus::Queued => TaskState::Queued,
            TaskStatus::Assigned => TaskState::Scheduled,
            TaskStatus::Running => TaskState::Running,
            TaskStatus::Completed => TaskState::Completed,
            TaskStatus::Failed | TaskStatus::TimedOut => TaskState::Failed,
            TaskStatus::Retried => TaskState::Retrying,
            TaskStatus::Cancelled => TaskState::Cancelled,
        }
    }
}

/// Key used to index queued tasks in `ready_queue: BTreeSet<QueueOrderKey>`.
///
/// Natural sorting guarantees:
/// 1. Higher numerical priority scheduled first (`Reverse(priority)`).
/// 2. FIFO order for identical priority (`sequence`).
/// 3. TaskId breaks any remaining ties.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueOrderKey {
    pub priority_rev: Reverse<u32>,
    pub sequence: u64,
    pub task_id: TaskId,
}

impl Ord for QueueOrderKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority_rev
            .cmp(&other.priority_rev)
            .then_with(|| self.sequence.cmp(&other.sequence))
            .then_with(|| self.task_id.0.cmp(&other.task_id.0))
    }
}

impl PartialOrd for QueueOrderKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Key used to index retrying tasks in `delayed_retries: BTreeSet<DelayedRetryKey>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DelayedRetryKey {
    pub retry_after: Instant,
    pub task_id: TaskId,
}

impl Ord for DelayedRetryKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.retry_after
            .cmp(&other.retry_after)
            .then_with(|| self.task_id.0.cmp(&other.task_id.0))
    }
}

impl PartialOrd for DelayedRetryKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Configurable retry policy and exponential backoff strategy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Maximum number of retries permitted per task (default: 3).
    pub max_retries: u32,
    /// Base delay for the first retry attempt in ms (default: 500 ms).
    pub initial_backoff_ms: u64,
    /// Ceiling limit for backoff delay in ms (default: 30,000 ms).
    pub max_backoff_ms: u64,
    /// Exponential growth multiplier (default: 2.0).
    pub backoff_multiplier: f64,
    /// Whether non-zero exit codes from worker execution are retryable (default: false).
    pub retry_on_execution_failure: bool,
    /// Whether worker disconnection or heartbeat timeout is retryable (default: true).
    pub retry_on_worker_disconnect: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff_ms: 500,
            max_backoff_ms: 30_000,
            backoff_multiplier: 2.0,
            retry_on_execution_failure: false,
            retry_on_worker_disconnect: true,
        }
    }
}

impl RetryPolicy {
    /// Calculates exponential backoff delay in milliseconds for the given 1-based attempt.
    pub fn calculate_backoff(&self, attempt: u32) -> u64 {
        if attempt == 0 {
            return 0;
        }
        let exp = (attempt - 1) as i32;
        let factor = self.backoff_multiplier.powi(exp);
        let delay = (self.initial_backoff_ms as f64 * factor).round() as u64;
        delay.min(self.max_backoff_ms)
    }
}

/// Audit record detailing a retry attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryAttempt {
    pub attempt: u32,
    pub timestamp_utc: u64,
    pub failed_worker_id: Option<Uuid>,
    pub reason: String,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
}

/// Internal in-memory task entry holding complete metadata.
#[derive(Debug, Clone)]
pub struct TaskEntry {
    pub task: Task,
    pub priority: u32,
    pub sequence: u64,
    pub state: TaskState,
    pub assigned_worker_id: Option<Uuid>,
    pub submitted_at_utc: u64,
    pub submitted_instant: Instant,
    pub scheduled_instant: Option<Instant>,
    pub started_instant: Option<Instant>,
    pub finished_instant: Option<Instant>,
    pub retry_after_instant: Option<Instant>,
    pub retry_count: u32,
    pub retry_history: Vec<RetryAttempt>,
    pub result: Option<TaskResult>,
    pub error_message: Option<String>,
}

impl TaskEntry {
    pub fn to_info(&self) -> TaskInfo {
        TaskInfo {
            task_id: self.task.id,
            state: self.state,
            priority: self.priority,
            gpu_required: self.task.requirements.gpu_required || self.task.spec.requires_gpu(),
            assigned_worker_id: self.assigned_worker_id,
            retry_count: self.retry_count,
            submitted_at_utc: self.submitted_at_utc,
            execution_time_ms: self.result.as_ref().map(|r| r.execution_time_ms),
            exit_code: self.result.as_ref().map(|r| r.exit_code),
            error_message: self.error_message.clone(),
        }
    }
}

/// Public serializable summary of task status and execution metrics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskInfo {
    pub task_id: TaskId,
    pub state: TaskState,
    pub priority: u32,
    pub gpu_required: bool,
    pub assigned_worker_id: Option<Uuid>,
    pub retry_count: u32,
    pub submitted_at_utc: u64,
    pub execution_time_ms: Option<u64>,
    pub exit_code: Option<i32>,
    pub error_message: Option<String>,
}

/// Aggregated statistics of tasks currently managed by `TaskQueue`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct QueueStats {
    pub total: usize,
    pub queued: usize,
    pub scheduled: usize,
    pub running: usize,
    pub retrying: usize,
    pub completed: usize,
    pub failed: usize,
    pub cancelled: usize,
}

pub const DEFAULT_MAX_TERMINAL_TASKS: usize = 10_000;

struct TaskQueueInner {
    tasks: HashMap<TaskId, TaskEntry>,
    ready_queue: BTreeSet<QueueOrderKey>,
    delayed_retries: BTreeSet<DelayedRetryKey>,
    worker_assignments: HashMap<Uuid, HashSet<TaskId>>,
    next_sequence: u64,
    retry_policy: RetryPolicy,
    capacity: Option<usize>,
    max_terminal_tasks: usize,
    terminal_order: std::collections::VecDeque<TaskId>,
}

impl TaskQueueInner {
    fn track_terminal_task(&mut self, task_id: TaskId) {
        self.terminal_order.push_back(task_id);
        while self.terminal_order.len() > self.max_terminal_tasks {
            if let Some(oldest) = self.terminal_order.pop_front() {
                if let Some(entry) = self.tasks.get(&oldest) {
                    if entry.state.is_terminal() {
                        self.tasks.remove(&oldest);
                    }
                }
            } else {
                break;
            }
        }
    }

    fn evict_terminal_tasks(&mut self, max_retained: usize) -> usize {
        let mut evicted = 0;
        while self.terminal_order.len() > max_retained {
            if let Some(oldest) = self.terminal_order.pop_front() {
                if let Some(entry) = self.tasks.get(&oldest) {
                    if entry.state.is_terminal() {
                        self.tasks.remove(&oldest);
                        evicted += 1;
                    }
                }
            } else {
                break;
            }
        }
        evicted
    }

    fn prune_terminal_older_than(&mut self, ttl: Duration) -> usize {
        let now = Instant::now();
        let mut evicted = 0;
        let mut remaining = std::collections::VecDeque::new();
        while let Some(task_id) = self.terminal_order.pop_front() {
            let should_evict = if let Some(entry) = self.tasks.get(&task_id) {
                if entry.state.is_terminal() {
                    if let Some(finished) = entry.finished_instant {
                        now.duration_since(finished) >= ttl
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            };

            if should_evict {
                self.tasks.remove(&task_id);
                evicted += 1;
            } else if self.tasks.contains_key(&task_id) {
                remaining.push_back(task_id);
            }
        }
        self.terminal_order = remaining;
        evicted
    }
}

/// Thread-safe in-memory Task Queue and lifecycle coordinator.
#[derive(Clone)]
pub struct TaskQueue {
    inner: Arc<RwLock<TaskQueueInner>>,
}

impl Default for TaskQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskQueue {
    /// Creates a new TaskQueue with default retry policy and unbounded capacity.
    pub fn new() -> Self {
        Self::with_config(RetryPolicy::default(), None)
    }

    /// Creates a new TaskQueue with custom retry policy and optional capacity limit.
    pub fn with_config(retry_policy: RetryPolicy, capacity: Option<usize>) -> Self {
        Self::with_retention(retry_policy, capacity, DEFAULT_MAX_TERMINAL_TASKS)
    }

    /// Creates a new TaskQueue with custom retry policy, capacity limit, and terminal retention limit.
    pub fn with_retention(
        retry_policy: RetryPolicy,
        capacity: Option<usize>,
        max_terminal_tasks: usize,
    ) -> Self {
        Self {
            inner: Arc::new(RwLock::new(TaskQueueInner {
                tasks: HashMap::new(),
                ready_queue: BTreeSet::new(),
                delayed_retries: BTreeSet::new(),
                worker_assignments: HashMap::new(),
                next_sequence: 1,
                retry_policy,
                capacity,
                max_terminal_tasks,
                terminal_order: std::collections::VecDeque::new(),
            })),
        }
    }

    // --- Task Submission ---

    /// Submits a task with default priority (0).
    pub async fn submit(&self, task: Task) -> GridResult<TaskId> {
        self.submit_with_priority(task, 0).await
    }

    /// Submits a task with explicit scheduling priority.
    pub async fn submit_with_priority(&self, task: Task, priority: u32) -> GridResult<TaskId> {
        task.validate().map_err(GridError::Config)?;

        let mut inner = self.inner.write().await;

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

        let task_id = task.id;
        let sequence = inner.next_sequence;
        inner.next_sequence += 1;

        let now_instant = Instant::now();
        let now_epoch = Utc::now().timestamp() as u64;

        let entry = TaskEntry {
            task,
            priority,
            sequence,
            state: TaskState::Queued,
            assigned_worker_id: None,
            submitted_at_utc: now_epoch,
            submitted_instant: now_instant,
            scheduled_instant: None,
            started_instant: None,
            finished_instant: None,
            retry_after_instant: None,
            retry_count: 0,
            retry_history: Vec::new(),
            result: None,
            error_message: None,
        };

        inner.tasks.insert(task_id, entry);
        inner.ready_queue.insert(QueueOrderKey {
            priority_rev: Reverse(priority),
            sequence,
            task_id,
        });

        Ok(task_id)
    }

    // --- Scheduler Matchmaking & Queries ---

    /// Returns a list of tasks currently in `Queued` state in priority-FIFO order.
    pub async fn get_schedulable_tasks(&self) -> Vec<Task> {
        let inner = self.inner.read().await;
        inner
            .ready_queue
            .iter()
            .filter_map(|k| inner.tasks.get(&k.task_id).map(|e| e.task.clone()))
            .collect()
    }

    /// Pops the highest-priority queued task matching `predicate`, marks it `Scheduled` to `worker_id`,
    /// and records worker assignment.
    pub async fn pop_and_schedule<F>(&self, worker_id: Uuid, predicate: F) -> Option<Task>
    where
        F: Fn(&TaskRequirements) -> bool,
    {
        let mut inner = self.inner.write().await;

        let found_key = inner
            .ready_queue
            .iter()
            .find(|k| {
                if let Some(entry) = inner.tasks.get(&k.task_id) {
                    predicate(&entry.task.requirements)
                } else {
                    false
                }
            })
            .copied();

        if let Some(key) = found_key {
            inner.ready_queue.remove(&key);
            let task = {
                let entry = inner.tasks.get_mut(&key.task_id)?;
                entry.state = TaskState::Scheduled;
                entry.assigned_worker_id = Some(worker_id);
                entry.scheduled_instant = Some(Instant::now());
                entry.task.clone()
            };

            inner
                .worker_assignments
                .entry(worker_id)
                .or_default()
                .insert(key.task_id);

            Some(task)
        } else {
            None
        }
    }

    /// Explicitly transitions a specific Queued task to Scheduled for the designated worker.
    ///
    /// Atomically removes the task from `ready_queue`, sets state to `Scheduled`,
    /// and records worker assignment. Returns `Some(Task)` if successful, or `None`
    /// if the task is no longer in `Queued` state (e.g. cancelled concurrently).
    pub async fn schedule_task(&self, task_id: &TaskId, worker_id: Uuid) -> Option<Task> {
        let mut inner = self.inner.write().await;
        let priority_info = {
            let entry = inner.tasks.get(task_id)?;
            if entry.state != TaskState::Queued {
                return None;
            }
            (entry.priority, entry.sequence)
        };

        inner.ready_queue.remove(&QueueOrderKey {
            priority_rev: Reverse(priority_info.0),
            sequence: priority_info.1,
            task_id: *task_id,
        });

        let task = {
            let entry = inner.tasks.get_mut(task_id)?;
            entry.state = TaskState::Scheduled;
            entry.assigned_worker_id = Some(worker_id);
            entry.scheduled_instant = Some(Instant::now());
            entry.task.clone()
        };

        inner
            .worker_assignments
            .entry(worker_id)
            .or_default()
            .insert(*task_id);

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
        if assignments.is_empty() {
            return Vec::new();
        }

        let mut inner = self.inner.write().await;
        let now = Instant::now();
        let mut scheduled = Vec::with_capacity(assignments.len());

        for &(task_id, worker_id) in assignments {
            let priority_info = match inner.tasks.get(&task_id) {
                Some(entry) if entry.state == TaskState::Queued => {
                    (entry.priority, entry.sequence)
                }
                _ => continue,
            };

            inner.ready_queue.remove(&QueueOrderKey {
                priority_rev: Reverse(priority_info.0),
                sequence: priority_info.1,
                task_id,
            });

            if let Some(entry) = inner.tasks.get_mut(&task_id) {
                entry.state = TaskState::Scheduled;
                entry.assigned_worker_id = Some(worker_id);
                entry.scheduled_instant = Some(now);
                let task = entry.task.clone();

                inner
                    .worker_assignments
                    .entry(worker_id)
                    .or_default()
                    .insert(task_id);

                scheduled.push((task_id, worker_id, task));
            }
        }

        scheduled
    }

    /// Re-enqueues a task back to `Queued` status if dispatch to the worker failed.
    pub async fn requeue_task(&self, task_id: &TaskId) -> GridResult<()> {
        let mut inner = self.inner.write().await;
        let prev_worker = {
            let entry = inner
                .tasks
                .get_mut(task_id)
                .ok_or(GridError::TaskNotFound(task_id.0))?;
            entry.assigned_worker_id.take()
        };

        if let Some(wid) = prev_worker {
            if let Some(tasks) = inner.worker_assignments.get_mut(&wid) {
                tasks.remove(task_id);
            }
        }

        let (priority, sequence) = {
            let entry = inner
                .tasks
                .get_mut(task_id)
                .ok_or(GridError::TaskNotFound(task_id.0))?;
            entry.state = TaskState::Queued;
            entry.scheduled_instant = None;
            (entry.priority, entry.sequence)
        };

        inner.ready_queue.insert(QueueOrderKey {
            priority_rev: Reverse(priority),
            sequence,
            task_id: *task_id,
        });

        Ok(())
    }

    // --- Worker Telemetry & Result Handling ---

    /// Transitions a Scheduled task to Running upon worker progress telemetry.
    pub async fn mark_running(&self, task_id: &TaskId, worker_id: Uuid) -> GridResult<()> {
        let mut inner = self.inner.write().await;
        let entry = inner
            .tasks
            .get_mut(task_id)
            .ok_or(GridError::TaskNotFound(task_id.0))?;

        if !entry.state.can_transition_to(TaskState::Running) {
            return Err(GridError::InvalidTaskState {
                task_id: task_id.0,
                current: format!("{:?}", entry.state),
                attempted: "Running".into(),
            });
        }

        entry.state = TaskState::Running;
        entry.assigned_worker_id = Some(worker_id);
        entry.started_instant = Some(Instant::now());
        Ok(())
    }

    /// Records a mid-task computation checkpoint for an in-flight task.
    ///
    /// Monotonically enforces checkpoint ordering by sequence number.
    /// Outdated out-of-order checkpoint snapshots are safely ignored.
    pub async fn record_checkpoint(
        &self,
        task_id: TaskId,
        checkpoint: CheckpointData,
    ) -> Result<(), GridError> {
        let mut inner = self.inner.write().await;
        let entry = inner
            .tasks
            .get_mut(&task_id)
            .ok_or(GridError::TaskNotFound(task_id.0))?;

        if entry.state.is_terminal() {
            return Ok(());
        }

        let is_newer = match &entry.task.latest_checkpoint {
            Some(existing) => checkpoint.sequence > existing.sequence,
            None => true,
        };

        if is_newer {
            entry.task.update_checkpoint(checkpoint);
        }

        Ok(())
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
    pub async fn record_result(&self, result: TaskResult) -> GridResult<TaskState> {
        let task_id = result.task_id;
        let worker_id = result.worker_id;

        let mut inner = self.inner.write().await;
        let policy = inner.retry_policy.clone();

        // Remove from worker assignment tracking
        if let Some(tasks) = inner.worker_assignments.get_mut(&worker_id) {
            tasks.remove(&task_id);
        }

        let now_instant = Instant::now();
        let now_epoch = Utc::now().timestamp() as u64;

        let mut delay_key_to_insert = None;

        let res_state = {
            let entry = inner
                .tasks
                .get_mut(&task_id)
                .ok_or(GridError::TaskNotFound(task_id.0))?;

            // If task is already terminal (e.g. Cancelled by master directive, Completed, Failed), preserve state
            if entry.state.is_terminal() {
                tracing::debug!(task_id = %task_id, state = ?entry.state, "Ignoring result for terminal task");
                return Ok(entry.state);
            }

            if result.is_success() {
                if !entry.state.can_transition_to(TaskState::Completed) {
                    return Err(GridError::InvalidTaskState {
                        task_id: task_id.0,
                        current: format!("{:?}", entry.state),
                        attempted: "Completed".into(),
                    });
                }
                entry.state = TaskState::Completed;
                entry.finished_instant = Some(now_instant);
                entry.result = Some(result);
                TaskState::Completed
            } else if result.exit_code == 130 {
                // Task cancellation from worker
                entry.state = TaskState::Cancelled;
                entry.finished_instant = Some(now_instant);
                entry.error_message = result.error.clone().or_else(|| Some("Cancelled".into()));
                entry.result = Some(result);
                TaskState::Cancelled
            } else if policy.retry_on_execution_failure && entry.retry_count < policy.max_retries {
                entry.retry_count += 1;
                let delay_ms = policy.calculate_backoff(entry.retry_count);
                let retry_after = now_instant + Duration::from_millis(delay_ms);

                entry.state = TaskState::Retrying;
                entry.assigned_worker_id = None;
                entry.retry_after_instant = Some(retry_after);
                entry.retry_history.push(RetryAttempt {
                    attempt: entry.retry_count,
                    timestamp_utc: now_epoch,
                    failed_worker_id: Some(worker_id),
                    reason: "Non-zero exit code execution failure".into(),
                    exit_code: Some(result.exit_code),
                    error: result.error.clone(),
                });

                delay_key_to_insert = Some(DelayedRetryKey {
                    retry_after,
                    task_id,
                });

                TaskState::Retrying
            } else {
                // Unrecoverable or exhausted failure
                entry.state = TaskState::Failed;
                entry.finished_instant = Some(now_instant);
                entry.error_message = result.error.clone();
                entry.result = Some(result);
                TaskState::Failed
            }
        };

        if let Some(key) = delay_key_to_insert {
            inner.delayed_retries.insert(key);
        }

        if res_state.is_terminal() {
            inner.track_terminal_task(task_id);
        }

        Ok(res_state)
    }

    // --- Worker Disconnect & Failover ---

    /// Reaps all tasks assigned to a disconnected or timed-out worker.
    ///
    /// Active tasks (`Scheduled` or `Running`) are transitioned to `Retrying` or `Queued`
    /// (if retries remain) or `Failed`.
    /// If `immediate_reschedule` is true (e.g. graceful worker disconnect), tasks are
    /// re-enqueued directly to `ready_queue` with 0ms delay.
    /// Otherwise, exponential backoff is scheduled via `delayed_retries`.
    /// Returns the list of affected TaskIds.
    pub async fn handle_worker_disconnected(
        &self,
        worker_id: &Uuid,
        reason: &str,
        immediate_reschedule: bool,
    ) -> Vec<TaskId> {
        let mut inner = self.inner.write().await;
        let mut assigned = inner
            .worker_assignments
            .remove(worker_id)
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

        let mut affected = Vec::with_capacity(assigned.len());
        let mut delays_to_insert = Vec::new();
        let mut ready_to_insert = Vec::new();
        let mut terminal_to_track = Vec::new();

        for task_id in assigned {
            if let Some(entry) = inner.tasks.get_mut(&task_id) {
                if entry.state.is_active() {
                    affected.push(task_id);
                    // Check per-task max_retries override first, falling back to policy.max_retries
                    let max_retries = entry
                        .task
                        .requirements
                        .max_retries
                        .unwrap_or(policy.max_retries);

                    if policy.retry_on_worker_disconnect && entry.retry_count < max_retries {
                        entry.retry_count += 1;
                        let delay_ms = if immediate_reschedule {
                            0
                        } else {
                            policy.calculate_backoff(entry.retry_count)
                        };

                        entry.assigned_worker_id = None;
                        entry.retry_history.push(RetryAttempt {
                            attempt: entry.retry_count,
                            timestamp_utc: now_epoch,
                            failed_worker_id: Some(*worker_id),
                            reason: reason.to_string(),
                            exit_code: None,
                            error: Some(format!("Worker evicted ({reason})")),
                        });

                        if delay_ms == 0 {
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
                        } else {
                            // Delayed retry with exponential backoff
                            let retry_after = now_instant + Duration::from_millis(delay_ms);
                            entry.state = TaskState::Retrying;
                            entry.retry_after_instant = Some(retry_after);
                            delays_to_insert.push(DelayedRetryKey {
                                retry_after,
                                task_id,
                            });
                        }
                    } else {
                        // Terminal Failed state
                        entry.state = TaskState::Failed;
                        entry.assigned_worker_id = None;
                        entry.finished_instant = Some(now_instant);
                        let err_msg = format!(
                            "Task execution failed: maximum retry limit ({max_retries}) reached after worker eviction (Worker disconnected and retries exhausted ({}/{max_retries}), last failure on worker {worker_id}: {reason})",
                            entry.retry_count
                        );
                        entry.error_message = Some(err_msg.clone());
                        let fail_result =
                            TaskResult::failure(*worker_id, task_id, 1, "", "", 0, Some(err_msg));
                        entry.result = Some(fail_result);
                        terminal_to_track.push(task_id);
                    }
                }
            }
        }

        for key in delays_to_insert {
            inner.delayed_retries.insert(key);
        }
        for key in ready_to_insert {
            inner.ready_queue.insert(key);
        }
        for tid in terminal_to_track {
            inner.track_terminal_task(tid);
        }

        affected
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
            let key = {
                if let Some(entry) = inner.tasks.get_mut(&task_id) {
                    if entry.state == TaskState::Retrying {
                        entry.state = TaskState::Queued;
                        entry.retry_after_instant = None;
                        Some(QueueOrderKey {
                            priority_rev: Reverse(entry.priority),
                            sequence: entry.sequence,
                            task_id,
                        })
                    } else {
                        None
                    }
                } else {
                    None
                }
            };
            if let Some(k) = key {
                inner.ready_queue.insert(k);
            }
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
            return Ok(None);
        }

        let worker_to_notify = match state {
            TaskState::Queued => {
                inner.ready_queue.remove(&QueueOrderKey {
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

        if let Some(entry) = inner.tasks.get_mut(task_id) {
            entry.state = TaskState::Cancelled;
            entry.finished_instant = Some(Instant::now());
            entry.error_message = cancel_reason;
            if entry.result.is_none() {
                entry.result = Some(cancel_result);
            }
        }
        inner.track_terminal_task(*task_id);

        Ok(worker_to_notify)
    }

    // --- Queries ---

    /// Retrieves task metadata snapshot.
    pub async fn get_task(&self, task_id: &TaskId) -> Option<TaskInfo> {
        let inner = self.inner.read().await;
        inner.tasks.get(task_id).map(|e| e.to_info())
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

    /// Retrieves recorded task execution result, if finished.
    pub async fn get_result(&self, task_id: &TaskId) -> Option<TaskResult> {
        let inner = self.inner.read().await;
        inner.tasks.get(task_id).and_then(|e| e.result.clone())
    }

    /// Returns a snapshot of all managed tasks.
    pub async fn list_tasks(&self) -> Vec<TaskInfo> {
        let inner = self.inner.read().await;
        inner.tasks.values().map(|e| e.to_info()).collect()
    }

    /// Returns aggregated queue statistics.
    pub async fn stats(&self) -> QueueStats {
        let inner = self.inner.read().await;
        let mut stats = QueueStats {
            total: inner.tasks.len(),
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

    /// Dynamically adjusts the retention limit for terminal tasks and evicts oldest tasks exceeding this limit.
    pub async fn set_max_terminal_tasks(&self, limit: usize) {
        let mut inner = self.inner.write().await;
        inner.max_terminal_tasks = limit;
        inner.evict_terminal_tasks(limit);
    }

    /// Evicts oldest terminal tasks until at most `max_retained` terminal tasks remain.
    ///
    /// Returns the number of evicted tasks.
    pub async fn evict_terminal_tasks(&self, max_retained: usize) -> usize {
        let mut inner = self.inner.write().await;
        inner.evict_terminal_tasks(max_retained)
    }

    /// Evicts terminal tasks (Completed, Failed, Cancelled) whose execution completed older than `ttl`.
    ///
    /// Returns the number of evicted tasks.
    pub async fn prune_terminal_older_than(&self, ttl: Duration) -> usize {
        let mut inner = self.inner.write().await;
        inner.prune_terminal_older_than(ttl)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::task::{TaskRequirements, TaskSpec};

    #[tokio::test]
    async fn test_fsm_valid_transitions() {
        assert!(TaskState::Submitted.can_transition_to(TaskState::Queued));
        assert!(TaskState::Queued.can_transition_to(TaskState::Scheduled));
        assert!(TaskState::Scheduled.can_transition_to(TaskState::Running));
        assert!(TaskState::Scheduled.can_transition_to(TaskState::Completed)); // Fast finish
        assert!(TaskState::Running.can_transition_to(TaskState::Completed));
        assert!(TaskState::Running.can_transition_to(TaskState::Failed));
        assert!(TaskState::Running.can_transition_to(TaskState::Retrying));
        assert!(TaskState::Retrying.can_transition_to(TaskState::Queued));
    }

    #[tokio::test]
    async fn test_fsm_terminal_invariance() {
        assert!(!TaskState::Completed.can_transition_to(TaskState::Queued));
        assert!(!TaskState::Failed.can_transition_to(TaskState::Running));
        assert!(!TaskState::Cancelled.can_transition_to(TaskState::Completed));
    }

    #[tokio::test]
    async fn test_priority_fifo_ordering() {
        let queue = TaskQueue::new();

        let t1 = Task::new(
            TaskSpec::command("echo", vec!["t1".into()]),
            TaskRequirements::generic(1, 10),
        );
        let t2 = Task::new(
            TaskSpec::command("echo", vec!["t2".into()]),
            TaskRequirements::generic(1, 10),
        );
        let t3 = Task::new(
            TaskSpec::command("echo", vec!["t3".into()]),
            TaskRequirements::generic(1, 10),
        );

        // Submit t1 priority 0, t2 priority 10, t3 priority 0
        let id1 = queue.submit_with_priority(t1, 0).await.unwrap();
        let id2 = queue.submit_with_priority(t2, 10).await.unwrap();
        let id3 = queue.submit_with_priority(t3, 0).await.unwrap();

        let w_id = Uuid::new_v4();

        // 1st pop must be t2 (highest priority = 10)
        let pop1 = queue.pop_and_schedule(w_id, |_| true).await.unwrap();
        assert_eq!(pop1.id, id2);

        // 2nd pop must be t1 (priority 0, earlier sequence than t3)
        let pop2 = queue.pop_and_schedule(w_id, |_| true).await.unwrap();
        assert_eq!(pop2.id, id1);

        // 3rd pop must be t3
        let pop3 = queue.pop_and_schedule(w_id, |_| true).await.unwrap();
        assert_eq!(pop3.id, id3);

        assert!(queue.pop_and_schedule(w_id, |_| true).await.is_none());
    }

    #[tokio::test]
    async fn test_fast_finishing_task_scheduled_to_completed() {
        let queue = TaskQueue::new();
        let task = Task::new(
            TaskSpec::command("echo", vec!["fast".into()]),
            TaskRequirements::generic(1, 10),
        );
        let task_id = queue.submit(task).await.unwrap();
        let worker_id = Uuid::new_v4();

        let scheduled_task = queue.pop_and_schedule(worker_id, |_| true).await.unwrap();
        assert_eq!(scheduled_task.id, task_id);
        assert_eq!(
            queue.get_state(&task_id).await.unwrap(),
            TaskState::Scheduled
        );

        // Record successful result directly without mark_running
        let result = TaskResult::success(worker_id, task_id, "fast\n", 5, false);
        let state = queue.record_result(result).await.unwrap();
        assert_eq!(state, TaskState::Completed);
        assert_eq!(
            queue.get_state(&task_id).await.unwrap(),
            TaskState::Completed
        );
    }

    #[tokio::test]
    async fn test_worker_disconnect_reaping_and_backoff_advancement() {
        let policy = RetryPolicy {
            initial_backoff_ms: 10,
            ..Default::default()
        };
        let queue = TaskQueue::with_config(policy, None);

        let task = Task::new(
            TaskSpec::command("echo", vec!["retry".into()]),
            TaskRequirements::generic(1, 10),
        );
        let task_id = queue.submit(task).await.unwrap();
        let worker_id = Uuid::new_v4();

        let _ = queue.pop_and_schedule(worker_id, |_| true).await.unwrap();
        queue.mark_running(&task_id, worker_id).await.unwrap();

        // Worker drops
        let affected = queue
            .handle_worker_disconnected(&worker_id, "Connection severed", false)
            .await;
        assert_eq!(affected, vec![task_id]);
        assert_eq!(
            queue.get_state(&task_id).await.unwrap(),
            TaskState::Retrying
        );

        // Immediately before backoff elapsed: 0 ready
        assert_eq!(queue.process_delayed_retries().await, 0);

        // Wait 15ms for backoff to pass
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert_eq!(queue.process_delayed_retries().await, 1);
        assert_eq!(queue.get_state(&task_id).await.unwrap(), TaskState::Queued);

        // Schedulable again to survivor worker
        let survivor_id = Uuid::new_v4();
        let reassigned = queue.pop_and_schedule(survivor_id, |_| true).await.unwrap();
        assert_eq!(reassigned.id, task_id);
    }

    #[tokio::test]
    async fn test_cancel_task_populates_entry_result() {
        let queue = TaskQueue::new();
        let task = Task::new(
            TaskSpec::command("echo", vec!["cancel_me".into()]),
            TaskRequirements::generic(1, 10),
        );
        let task_id = queue.submit(task).await.unwrap();
        queue
            .cancel_task(&task_id, Some("user requested".into()))
            .await
            .unwrap();

        assert_eq!(
            queue.get_state(&task_id).await.unwrap(),
            TaskState::Cancelled
        );
        let res = queue
            .get_result(&task_id)
            .await
            .expect("result must be present");
        assert_eq!(res.exit_code, 130);
        assert_eq!(res.task_id, task_id);
        assert_eq!(res.error.as_deref(), Some("user requested"));
    }

    #[tokio::test]
    async fn test_worker_disconnect_exhaustion_populates_entry_result() {
        let policy = RetryPolicy {
            max_retries: 0,
            ..Default::default()
        };
        let queue = TaskQueue::with_config(policy, None);
        let task = Task::new(
            TaskSpec::command("echo", vec!["exhaust".into()]),
            TaskRequirements::generic(1, 10),
        );
        let task_id = queue.submit(task).await.unwrap();
        let worker_id = Uuid::new_v4();
        let _ = queue.pop_and_schedule(worker_id, |_| true).await.unwrap();

        let affected = queue
            .handle_worker_disconnected(&worker_id, "crash", false)
            .await;
        assert_eq!(affected, vec![task_id]);
        assert_eq!(queue.get_state(&task_id).await.unwrap(), TaskState::Failed);
        let res = queue
            .get_result(&task_id)
            .await
            .expect("result must be present on failure");
        assert_eq!(res.exit_code, 1);
        assert!(res
            .error
            .as_deref()
            .unwrap_or("")
            .contains("retries exhausted"));
    }

    #[tokio::test]
    async fn test_immediate_graceful_disconnect_and_per_task_max_retries() {
        // Policy allows 5 retries by default
        let policy = RetryPolicy {
            max_retries: 5,
            initial_backoff_ms: 10_000, // Large backoff to verify immediate bypasses it
            ..Default::default()
        };
        let queue = TaskQueue::with_config(policy, None);

        // Task with per-task max_retries = 1 override
        let task = Task::new(
            TaskSpec::command("echo", vec!["per_task".into()]),
            TaskRequirements::generic(1, 10).with_max_retries(1),
        );
        let task_id = queue.submit(task).await.unwrap();
        let worker_1 = Uuid::new_v4();
        let _ = queue.pop_and_schedule(worker_1, |_| true).await.unwrap();
        queue.mark_running(&task_id, worker_1).await.unwrap();

        // Graceful disconnect (immediate_reschedule = true)
        let affected = queue
            .handle_worker_disconnected(&worker_1, "Graceful shutdown", true)
            .await;
        assert_eq!(affected, vec![task_id]);

        // State must immediately be Queued (ready for re-scheduling, 0ms delay)
        assert_eq!(
            queue.get_state(&task_id).await.unwrap(),
            TaskState::Queued,
            "Immediate reschedule must transition directly to Queued"
        );

        let info = queue.get_task(&task_id).await.unwrap();
        assert_eq!(info.retry_count, 1);

        // Schedule to worker 2
        let worker_2 = Uuid::new_v4();
        let sched = queue.pop_and_schedule(worker_2, |_| true).await;
        assert!(sched.is_some(), "Task must be immediately schedulable");

        // Worker 2 disconnects -> should exceed per-task max_retries (1) and fail!
        let affected2 = queue
            .handle_worker_disconnected(&worker_2, "Worker 2 crash", false)
            .await;
        assert_eq!(affected2, vec![task_id]);
        assert_eq!(queue.get_state(&task_id).await.unwrap(), TaskState::Failed);

        let res = queue.get_result(&task_id).await.unwrap();
        assert_eq!(res.exit_code, 1);
        let err = res.error.unwrap();
        assert!(
            err.contains("maximum retry limit (1) reached"),
            "Must record max retry limit 1: {err}"
        );
    }

    #[tokio::test]
    async fn test_terminal_task_lru_retention_and_eviction() {
        let queue = TaskQueue::with_retention(RetryPolicy::default(), None, 3);
        let worker_id = Uuid::new_v4();

        let mut ids = Vec::new();
        for i in 0..5 {
            let task = Task::new(
                TaskSpec::command("echo", vec![format!("{i}")]),
                TaskRequirements::generic(1, 10),
            );
            let tid = queue.submit(task).await.unwrap();
            ids.push(tid);
            let _ = queue.pop_and_schedule(worker_id, |_| true).await;
            queue.mark_running(&tid, worker_id).await.unwrap();
            let res = TaskResult::success(worker_id, tid, "ok", 10, false);
            queue.record_result(res).await.unwrap();
        }

        // We submitted and completed 5 tasks with retention limit = 3.
        // Oldest 2 tasks (ids[0] and ids[1]) should have been evicted.
        assert!(queue.get_task(&ids[0]).await.is_none());
        assert!(queue.get_result(&ids[0]).await.is_none());
        assert!(queue.get_task(&ids[1]).await.is_none());
        assert!(queue.get_result(&ids[1]).await.is_none());

        // Newest 3 tasks (ids[2], ids[3], ids[4]) must be retained.
        assert!(queue.get_task(&ids[2]).await.is_some());
        assert!(queue.get_task(&ids[3]).await.is_some());
        assert!(queue.get_task(&ids[4]).await.is_some());

        // Dynamic eviction down to 1
        let evicted = queue.evict_terminal_tasks(1).await;
        assert_eq!(evicted, 2);
        assert!(queue.get_task(&ids[2]).await.is_none());
        assert!(queue.get_task(&ids[3]).await.is_none());
        assert!(queue.get_task(&ids[4]).await.is_some());

        // TTL pruning
        tokio::time::sleep(Duration::from_millis(20)).await;
        let pruned = queue.prune_terminal_older_than(Duration::from_millis(10)).await;
        assert_eq!(pruned, 1);
        assert!(queue.get_task(&ids[4]).await.is_none());
    }
}
