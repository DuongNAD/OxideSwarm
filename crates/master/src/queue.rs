//! Thread-safe Task Queue and 8-State Finite State Machine (FSM) for `rusty_grid_master`.
//!
//! Provides priority-FIFO indexing, O(1) task lookup, worker assignment tracking,
//! configurable exponential backoff retries, and atomic state transitions.

use std::cmp::Reverse;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use uuid::Uuid;

use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::placement::Placement;
use rusty_grid_core::task::{
    CheckpointData, Task, TaskId, TaskRequirements, TaskResult, TaskSpec, TaskStatus,
};

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

    /// The canonical spelling used by every JSON surface (`snake_case`, identical to this enum's
    /// serde representation): `/api/tasks`, `/api/tasks/:id`, `/api/status` and the streams.
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskState::Submitted => "submitted",
            TaskState::Queued => "queued",
            TaskState::Scheduled => "scheduled",
            TaskState::Running => "running",
            TaskState::Completed => "completed",
            TaskState::Failed => "failed",
            TaskState::Cancelled => "cancelled",
            TaskState::Retrying => "retrying",
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

/// Default cap on infrastructure failovers (worker lost, reaped or superseded) per task.
pub const DEFAULT_MAX_FAILOVERS: u32 = 10;

/// A task that workers keep handing back without running it is failed after this many rejections.
pub const MAX_WORKER_REJECTIONS_PER_TASK: u32 = 3;

fn default_max_failovers() -> u32 {
    DEFAULT_MAX_FAILOVERS
}

/// Configurable retry policy and exponential backoff strategy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Maximum number of retries permitted per task (default: 3).
    ///
    /// Bounds *execution* retries (`retry_on_execution_failure`). Infrastructure failovers have their
    /// own budget, `max_failovers`; the only effect `max_retries` has on them is that `0` means
    /// "never run this task twice" and so disables failover re-runs as well.
    pub max_retries: u32,
    /// Maximum infrastructure failovers per task (default: 10).
    ///
    /// A failover is a re-dispatch caused by the worker, not the task: its connection dropped, its
    /// heartbeat timed out, it re-registered, or it refused the task unrun. A 2-hour job on a phone
    /// that roams networks must not be failed for the network's sake after three handoffs, but a
    /// task that kills every worker it touches still has to stop eventually. The effective cap is
    /// `max(max_failovers, max_retries)`, so raising `max_retries` never reduces it. An explicit
    /// per-task `requirements.max_retries` overrides this and bounds all re-runs of that task.
    #[serde(default = "default_max_failovers")]
    pub max_failovers: u32,
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
            max_failovers: DEFAULT_MAX_FAILOVERS,
            initial_backoff_ms: 500,
            max_backoff_ms: 30_000,
            backoff_multiplier: 2.0,
            retry_on_execution_failure: false,
            retry_on_worker_disconnect: true,
        }
    }
}

impl RetryPolicy {
    /// Whether `entry` may be re-run once more after an infrastructure failure, and the cap that applied.
    fn failover_budget(&self, entry: &TaskEntry) -> (bool, u32) {
        match entry.task.requirements.max_retries {
            // An explicit per-task limit bounds every re-run of that task, whatever caused it.
            Some(limit) => (entry.retry_count < limit, limit),
            None => {
                let cap = if self.max_retries == 0 {
                    0
                } else {
                    self.max_failovers.max(self.max_retries)
                };
                (entry.failover_count < cap, cap)
            }
        }
    }

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
    /// Total re-dispatches of this task: execution retries plus `failover_count`.
    pub retry_count: u32,
    /// Re-dispatches caused by infrastructure (worker lost, reaped, superseded, or refused the task
    /// unrun), tracked apart from execution retries so network churn cannot exhaust `max_retries`.
    pub failover_count: u32,
    /// Times a worker handed this task back without running it.
    pub rejection_count: u32,
    /// Workers that rejected this task; the scheduler places it elsewhere while it can.
    pub avoid_workers: Vec<Uuid>,
    /// Connection session of `assigned_worker_id` the task was dispatched on (`None` for
    /// assignments made without session information). Results, progress and checkpoints are only
    /// accepted from this worker *and* session, so a superseded or reaped session cannot touch it.
    pub assigned_session: Option<u64>,
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

impl QueueStats {
    /// Collapses the eight FSM states into the four buckets of `ClientResponse::ClusterStatus`
    /// as `(pending, running, completed, failed)`; every task lands in exactly one, so the sum
    /// equals `total`.
    ///
    /// `pending` also holds tasks waiting out a retry backoff; `running` also holds tasks already
    /// assigned to a worker but not yet reported running; `failed` also holds cancelled tasks
    /// (the wire message has no separate cancelled counter).
    pub fn cluster_buckets(&self) -> (usize, usize, usize, usize) {
        (
            self.queued + self.retrying,
            self.scheduled + self.running,
            self.completed,
            self.failed + self.cancelled,
        )
    }
}

pub const DEFAULT_MAX_TERMINAL_TASKS: usize = 10_000;

/// Default byte budget for retained terminal tasks (256 MiB): payloads plus captured output.
///
/// Count alone is no bound: 10,000 retained tasks that each captured the worker's 10 MiB cap per
/// stream would be 200 GB.
pub const DEFAULT_MAX_TERMINAL_BYTES: usize = 256 * 1024 * 1024;

/// Fixed bookkeeping cost charged to every retained task on top of its payload.
const RETAINED_ENTRY_OVERHEAD_BYTES: usize = 1024;

/// What a scheduling pass needs to know about a queued task, without the task's payload.
///
/// A pass looks at far more tasks than it places (everything in front of the first placeable one),
/// so it works on these views and only the tasks it really assigns are cloned, by
/// [`TaskQueue::schedule_tasks_batch_in_sessions`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskView {
    pub task_id: TaskId,
    pub requirements: TaskRequirements,
    /// `requirements.gpu_required || spec.requires_gpu()`: only GPU-capable workers may run it.
    pub requires_gpu: bool,
    /// Compilation and GPU specs, which throttled or low-battery mobile workers must not receive.
    pub heavy: bool,
    /// True if the task has staged input files or expects output artifacts (Requirement R2).
    pub requires_file_staging: bool,
    /// Workers that already handed this task back without running it.
    pub avoid: Vec<Uuid>,
    /// The task's placement constraints (`require:` tags), or why they cannot be read. Submission
    /// validates them, so an error only reaches here for a task that bypassed it; such a task is
    /// placed nowhere.
    pub placement: Result<Placement, String>,
}

impl TaskView {
    /// The view of `task` with no avoided workers.
    pub fn of(task: &Task) -> Self {
        Self {
            task_id: task.id,
            requirements: task.requirements.clone(),
            requires_gpu: task.requirements.gpu_required || task.spec.requires_gpu(),
            heavy: matches!(
                task.spec,
                TaskSpec::RustCompilation { .. } | TaskSpec::GpuCompute { .. }
            ),
            requires_file_staging: !task.staged_files.is_empty()
                || !task.expected_outputs.is_empty(),
            avoid: Vec::new(),
            placement: task.placement(),
        }
    }
}

/// What the master's own books say a worker is carrying: the tasks assigned to it that have not
/// reported a result (`Scheduled` or `Running`), whatever its last heartbeat claimed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AssignedLoad {
    /// Number of assigned tasks.
    pub tasks: usize,
    /// Concurrency slots they occupy: `max(1, requirements.cpu_cores)` each.
    pub slots: usize,
    /// Declared RAM of the assigned tasks that have not started yet. A running task's memory
    /// already shows in the worker's reported free RAM; a merely scheduled one does not.
    pub unstarted_ram_mb: u64,
}

/// What `TaskQueue::record_result_from` did with a worker's result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultDisposition {
    /// Applied: the task is now `Completed`, `Failed` or `Cancelled`.
    Terminal,
    /// The task goes around again (execution retry, or the worker refused it and it was
    /// re-queued for another worker); nobody waiting on it may be woken.
    Requeued,
    /// From the worker the task is assigned to, but the task was already terminal (for example
    /// cancelled by the master while the worker was still running it). State untouched.
    Late,
    /// From a worker or session the task is not currently assigned to (failed over, reaped,
    /// superseded, or simply never its task). Dropped without touching the FSM.
    Stale,
}

/// Outcome of `TaskQueue::record_result_from`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResultOutcome {
    pub disposition: ResultDisposition,
    /// State of the task after the call.
    pub state: TaskState,
    /// The result was a worker-side refusal to run the task at all (see [`is_worker_rejection`]);
    /// the caller should penalise the worker.
    pub rejected: bool,
}

/// Outcome of `TaskQueue::cancel_task_detailed`.
#[derive(Debug, Clone, PartialEq)]
pub struct CancelOutcome {
    /// False when the task had already finished; nothing was changed.
    pub newly_cancelled: bool,
    /// Worker currently executing the task, which must be told to stop.
    pub worker_to_notify: Option<Uuid>,
    /// The cancelled result recorded for the task, to hand to everybody waiting on it.
    pub result: Option<TaskResult>,
}

/// Substrings (lower case) the worker puts into the result of a task it refuses to run.
/// They come from `WorkerClient::handle_assign_task_with_checkpoint` and the task runner and are
/// unit-tested against those exact messages.
const WORKER_REJECTION_MARKERS: &[&str] = &[
    "rejected by worker",
    "exceed worker capabilities",
    "capabilities insufficient",
    "lacks gpu capability",
];

/// True when `result` is a worker refusing to run a task at all (critically low battery,
/// capabilities insufficient, no GPU for a GPU task) rather than the task failing.
///
/// Such a result is produced instantly, before anything executes, so it says nothing about the
/// task: another worker may run it fine. It also makes the refusing worker look permanently idle,
/// which is how one stale-snapshot phone ends up absorbing a large share of a cluster's work.
pub fn is_worker_rejection(result: &TaskResult) -> bool {
    if result.exit_code == 0
        || result.exit_code == 130
        || result.execution_time_ms != 0
        || !result.stdout.0.is_empty()
    {
        return false;
    }
    let mut text = result.error.clone().unwrap_or_default();
    text.push(' ');
    text.push_str(&result.stderr_str());
    let text = text.to_ascii_lowercase();
    WORKER_REJECTION_MARKERS.iter().any(|m| text.contains(m))
}

/// True when `entry` is currently assigned to `worker_id` (and, if both sides know it, `session_id`).
fn owned_by(entry: &TaskEntry, worker_id: &Uuid, session_id: Option<u64>) -> bool {
    entry.assigned_worker_id == Some(*worker_id)
        && match (session_id, entry.assigned_session) {
            (Some(claimed), Some(assigned)) => claimed == assigned,
            _ => true,
        }
}

struct TaskQueueInner {
    tasks: HashMap<TaskId, TaskEntry>,
    ready_queue: BTreeSet<QueueOrderKey>,
    delayed_retries: BTreeSet<DelayedRetryKey>,
    worker_assignments: HashMap<Uuid, HashSet<TaskId>>,
    next_sequence: u64,
    retry_policy: RetryPolicy,
    capacity: Option<usize>,
    max_terminal_tasks: usize,
    max_terminal_bytes: usize,
    terminal_order: std::collections::VecDeque<TaskId>,
    /// Bytes each retained terminal task was charged when it finished (see `retained_bytes`).
    terminal_sizes: HashMap<TaskId, usize>,
    /// Sum of `terminal_sizes`.
    terminal_bytes: usize,
}

/// Bytes of payload and output a task entry keeps alive: its spec (stdin, sources, input data),
/// checkpoint, captured stdout/stderr and the strings of its history.
fn retained_bytes(entry: &TaskEntry) -> usize {
    let spec = match &entry.task.spec {
        TaskSpec::Command {
            program,
            args,
            env,
            stdin,
            ..
        } => {
            program.len()
                + args.iter().map(String::len).sum::<usize>()
                + env.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>()
                + stdin.as_ref().map_or(0, Vec::len)
        }
        TaskSpec::ShellScript {
            script,
            interpreter,
            env,
        } => {
            script.len()
                + interpreter.as_ref().map_or(0, String::len)
                + env.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>()
        }
        TaskSpec::RustCompilation {
            crate_name,
            source_files,
            compiler_flags,
            ..
        } => {
            crate_name.len()
                + source_files
                    .iter()
                    .map(|(k, v)| k.len() + v.len())
                    .sum::<usize>()
                + compiler_flags.iter().map(String::len).sum::<usize>()
        }
        TaskSpec::GpuCompute {
            kernel_name,
            input_data,
            ..
        } => kernel_name.len() + input_data.len(),
        TaskSpec::BuiltinTest { test_name, .. } => test_name.len(),
        TaskSpec::Generative3D(s) => {
            s.prompt.as_ref().map_or(0, String::len)
                + s.image_data.as_ref().map_or(0, |b| b.len())
                + s.quality.len()
                + s.engine_preference.as_ref().map_or(0, String::len)
        }
        TaskSpec::AutoRig(s) => {
            s.input_mesh.len()
                + s.rig_preset.len()
                + s.embed_animations.iter().map(String::len).sum::<usize>()
        }
    };
    let checkpoint = entry
        .task
        .latest_checkpoint
        .as_ref()
        .map_or(0, |c| c.delta_state.0.len());
    let staged_files_bytes: usize = entry
        .task
        .staged_files
        .iter()
        .map(|f| f.path.len() + f.data.len())
        .sum();
    let expected_outputs_bytes: usize = entry.task.expected_outputs.iter().map(String::len).sum();
    let result = entry.result.as_ref().map_or(0, |r| {
        r.stdout.0.len()
            + r.stderr.0.len()
            + r.error.as_ref().map_or(0, String::len)
            + r.device_name.as_ref().map_or(0, String::len)
            + r.artifacts
                .iter()
                .map(|a| a.path.len() + a.data.len())
                .sum::<usize>()
    });
    let history: usize = entry
        .retry_history
        .iter()
        .map(|a| a.reason.len() + a.error.as_ref().map_or(0, String::len))
        .sum();
    RETAINED_ENTRY_OVERHEAD_BYTES
        + spec
        + staged_files_bytes
        + expected_outputs_bytes
        + checkpoint
        + result
        + history
        + entry.error_message.as_ref().map_or(0, String::len)
        + entry.task.tags.iter().map(String::len).sum::<usize>()
}

impl TaskQueueInner {
    /// Drops a retained terminal task and its byte charge.
    fn remove_retained(&mut self, task_id: &TaskId) {
        self.tasks.remove(task_id);
        if let Some(size) = self.terminal_sizes.remove(task_id) {
            self.terminal_bytes = self.terminal_bytes.saturating_sub(size);
        }
    }

    /// Evicts the oldest retained terminal task. Returns false when none is left.
    fn evict_oldest_terminal(&mut self) -> bool {
        let Some(oldest) = self.terminal_order.pop_front() else {
            return false;
        };
        if self
            .tasks
            .get(&oldest)
            .is_some_and(|entry| entry.state.is_terminal())
        {
            self.remove_retained(&oldest);
        } else {
            self.terminal_sizes.remove(&oldest);
        }
        true
    }

    /// Evicts oldest-first until both the count and the byte budget hold. The task that just
    /// finished is never evicted for bytes alone (a single result larger than the whole budget is
    /// still readable by its client), so the budget can be exceeded by at most that one task.
    fn enforce_terminal_limits(&mut self) {
        while self.terminal_order.len() > self.max_terminal_tasks {
            if !self.evict_oldest_terminal() {
                break;
            }
        }
        while self.terminal_bytes > self.max_terminal_bytes && self.terminal_order.len() > 1 {
            if !self.evict_oldest_terminal() {
                break;
            }
        }
    }

    fn track_terminal_task(&mut self, task_id: TaskId) {
        if self.terminal_sizes.contains_key(&task_id) {
            return;
        }
        let size = self.tasks.get(&task_id).map_or(0, retained_bytes);
        self.terminal_sizes.insert(task_id, size);
        self.terminal_bytes += size;
        self.terminal_order.push_back(task_id);
        self.enforce_terminal_limits();
    }

    fn evict_terminal_tasks(&mut self, max_retained: usize) -> usize {
        let mut evicted = 0;
        while self.terminal_order.len() > max_retained {
            let had_entry = self
                .terminal_order
                .front()
                .and_then(|id| self.tasks.get(id))
                .is_some_and(|entry| entry.state.is_terminal());
            if !self.evict_oldest_terminal() {
                break;
            }
            if had_entry {
                evicted += 1;
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
                self.remove_retained(&task_id);
                evicted += 1;
            } else if self.tasks.contains_key(&task_id) {
                remaining.push_back(task_id);
            } else {
                self.terminal_sizes.remove(&task_id);
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
    /// Holds the queue's write lock until the returned guard is dropped, so a test can park every
    /// queue operation at a chosen point.
    #[cfg(test)]
    pub(crate) async fn hold_write_lock_for_test(&self) -> impl Send + 'static {
        self.inner.clone().write_owned().await
    }

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
        Self::with_retention_limits(
            retry_policy,
            capacity,
            max_terminal_tasks,
            DEFAULT_MAX_TERMINAL_BYTES,
        )
    }

    /// Like [`with_retention`](Self::with_retention), also bounding the bytes (payloads plus
    /// captured output) kept for finished tasks. Whichever limit is hit first evicts the oldest
    /// finished task first.
    pub fn with_retention_limits(
        retry_policy: RetryPolicy,
        capacity: Option<usize>,
        max_terminal_tasks: usize,
        max_terminal_bytes: usize,
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
                max_terminal_bytes,
                terminal_order: std::collections::VecDeque::new(),
                terminal_sizes: HashMap::new(),
                terminal_bytes: 0,
            })),
        }
    }

    // --- Task Submission ---

    /// Submits a task with default priority (0).
    pub async fn submit(&self, task: Task) -> GridResult<TaskId> {
        self.submit_with_priority(task, 0).await
    }

    /// Submits a task with explicit scheduling priority.
    ///
    /// Task ids are unique for as long as the queue retains the entry: resubmitting an id that is
    /// queued, running or finished is refused instead of replacing the entry (which would orphan
    /// its queue keys and run the work twice).
    pub async fn submit_with_priority(&self, task: Task, priority: u32) -> GridResult<TaskId> {
        task.validate().map_err(GridError::Config)?;

        let mut inner = self.inner.write().await;

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
            failover_count: 0,
            rejection_count: 0,
            avoid_workers: Vec::new(),
            assigned_session: None,
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
            .filter_map(|k| {
                inner
                    .tasks
                    .get(&k.task_id)
                    .filter(|e| e.state == TaskState::Queued)
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
        let inner = self.inner.read().await;
        let lower = match after {
            Some(key) => Bound::Excluded(key),
            None => Bound::Unbounded,
        };
        let mut views = Vec::with_capacity(limit.min(256));
        for key in inner.ready_queue.range((lower, Bound::Unbounded)) {
            let Some(entry) = inner
                .tasks
                .get(&key.task_id)
                .filter(|e| e.state == TaskState::Queued)
            else {
                continue;
            };
            let mut view = TaskView::of(&entry.task);
            view.avoid = entry.avoid_workers.clone();
            views.push(view);
            if views.len() >= limit {
                return (views, Some(*key));
            }
        }
        (views, None)
    }

    /// Ids of up to `limit` queued tasks in priority-FIFO order, without cloning any task.
    pub async fn ready_task_ids(&self, limit: usize) -> Vec<TaskId> {
        let inner = self.inner.read().await;
        inner
            .ready_queue
            .iter()
            .filter(|k| {
                inner
                    .tasks
                    .get(&k.task_id)
                    .is_some_and(|e| e.state == TaskState::Queued)
            })
            .take(limit)
            .map(|k| k.task_id)
            .collect()
    }

    /// True if any queued task satisfies `pred`. Stops at the first hit; takes no clones.
    pub async fn any_queued<F: Fn(&Task) -> bool>(&self, pred: F) -> bool {
        let inner = self.inner.read().await;
        inner.ready_queue.iter().any(|k| {
            inner
                .tasks
                .get(&k.task_id)
                .is_some_and(|e| e.state == TaskState::Queued && pred(&e.task))
        })
    }

    /// What each worker is carrying according to the queue's own assignment set, for every worker
    /// that has at least one assigned task. Heartbeats cannot corrupt this: it is derived from
    /// the tasks themselves.
    pub async fn assigned_loads(&self) -> HashMap<Uuid, AssignedLoad> {
        let inner = self.inner.read().await;
        let mut loads = HashMap::with_capacity(inner.worker_assignments.len());
        for (worker_id, task_ids) in &inner.worker_assignments {
            let mut load = AssignedLoad::default();
            for task_id in task_ids {
                let Some(entry) = inner.tasks.get(task_id) else {
                    continue;
                };
                if !entry.state.is_active() {
                    continue;
                }
                load.tasks += 1;
                load.slots += entry.task.requirements.cpu_cores.max(1);
                if entry.state == TaskState::Scheduled {
                    load.unstarted_ram_mb += entry.task.requirements.ram_mb;
                }
            }
            if load.tasks > 0 {
                loads.insert(*worker_id, load);
            }
        }
        loads
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
                    entry.state == TaskState::Queued && predicate(&entry.task.requirements)
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
                entry.assigned_session = None;
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
            entry.assigned_session = None;
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

        let mut inner = self.inner.write().await;
        let now = Instant::now();
        let mut scheduled = Vec::with_capacity(assignments.len());

        for &(task_id, worker_id, session_id) in assignments {
            let (priority, sequence) = match inner.tasks.get(&task_id) {
                Some(entry) if entry.state == TaskState::Queued => (entry.priority, entry.sequence),
                Some(entry) => {
                    // A ready key can only be stale here (the entry left Queued without
                    // dropping it); keeping it would be re-offered to every scheduling pass.
                    let stale = QueueOrderKey {
                        priority_rev: Reverse(entry.priority),
                        sequence: entry.sequence,
                        task_id,
                    };
                    inner.ready_queue.remove(&stale);
                    continue;
                }
                None => continue,
            };

            inner.ready_queue.remove(&QueueOrderKey {
                priority_rev: Reverse(priority),
                sequence,
                task_id,
            });

            if let Some(entry) = inner.tasks.get_mut(&task_id) {
                entry.state = TaskState::Scheduled;
                entry.assigned_worker_id = Some(worker_id);
                entry.assigned_session = session_id;
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
    ///
    /// Refuses to resurrect a finished task.
    pub async fn requeue_task(&self, task_id: &TaskId) -> GridResult<()> {
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
        let (priority, sequence) = {
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
            (entry.priority, entry.sequence)
        };
        if let Some(tasks) = inner.worker_assignments.get_mut(worker_id) {
            tasks.remove(task_id);
        }
        inner.ready_queue.insert(QueueOrderKey {
            priority_rev: Reverse(priority),
            sequence,
            task_id: *task_id,
        });
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
        let mut inner = self.inner.write().await;
        let entry = inner
            .tasks
            .get_mut(task_id)
            .ok_or(GridError::TaskNotFound(task_id.0))?;

        if !owned_by(entry, &worker_id, session_id) {
            return Err(GridError::InvalidTaskState {
                task_id: task_id.0,
                current: format!("{:?} (not assigned to this worker/session)", entry.state),
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

        entry.state = TaskState::Running;
        entry.started_instant = Some(Instant::now());
        Ok(())
    }

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
        let mut inner = self.inner.write().await;
        let entry = inner
            .tasks
            .get_mut(&task_id)
            .ok_or(GridError::TaskNotFound(task_id.0))?;

        if entry.state.is_terminal() {
            return Ok(());
        }

        Self::apply_checkpoint(entry, checkpoint);
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
        let mut inner = self.inner.write().await;
        let entry = inner
            .tasks
            .get_mut(&task_id)
            .ok_or(GridError::TaskNotFound(task_id.0))?;

        if !entry.state.is_active() || !owned_by(entry, &worker_id, session_id) {
            tracing::debug!(
                task_id = %task_id,
                worker_id = %worker_id,
                state = ?entry.state,
                "Dropping checkpoint from a worker/session the task is not assigned to"
            );
            return Ok(false);
        }

        Ok(Self::apply_checkpoint(entry, checkpoint))
    }

    fn apply_checkpoint(entry: &mut TaskEntry, checkpoint: CheckpointData) -> bool {
        let is_newer = match &entry.task.latest_checkpoint {
            Some(existing) => checkpoint.sequence > existing.sequence,
            None => true,
        };

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
        let task_id = result.task_id;

        let mut inner = self.inner.write().await;
        let policy = inner.retry_policy.clone();

        let (state, owner) = {
            let entry = inner
                .tasks
                .get(&task_id)
                .ok_or(GridError::TaskNotFound(task_id.0))?;
            (entry.state, owned_by(entry, &worker_id, session_id))
        };

        if !owner {
            tracing::debug!(
                task_id = %task_id,
                worker_id = %worker_id,
                session_id = ?session_id,
                state = ?state,
                "Dropping result from a worker/session the task is not assigned to"
            );
            return Ok(ResultOutcome {
                disposition: ResultDisposition::Stale,
                state,
                rejected: false,
            });
        }

        // Remove from worker assignment tracking
        if let Some(tasks) = inner.worker_assignments.get_mut(&worker_id) {
            tasks.remove(&task_id);
        }

        // If task is already terminal (e.g. Cancelled by master directive, Completed, Failed), preserve state
        if state.is_terminal() {
            tracing::debug!(task_id = %task_id, state = ?state, "Ignoring result for terminal task");
            return Ok(ResultOutcome {
                disposition: ResultDisposition::Late,
                state,
                rejected: false,
            });
        }

        let now_instant = Instant::now();
        let now_epoch = Utc::now().timestamp() as u64;
        let rejected = is_worker_rejection(&result);

        let mut delay_key_to_insert = None;
        let mut ready_key_to_insert = None;

        let (res_state, disposition) = {
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
                entry.finished_instant = Some(now_instant);
                entry.result = Some(result);
                (TaskState::Completed, ResultDisposition::Terminal)
            } else if result.exit_code == 130 {
                // Task cancellation from worker
                entry.state = TaskState::Cancelled;
                entry.finished_instant = Some(now_instant);
                entry.error_message = result.error.clone().or_else(|| Some("Cancelled".into()));
                entry.result = Some(result);
                (TaskState::Cancelled, ResultDisposition::Terminal)
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
                entry.retry_history.push(RetryAttempt {
                    attempt: entry.retry_count,
                    timestamp_utc: now_epoch,
                    failed_worker_id: Some(worker_id),
                    reason: "Worker rejected the task without running it".into(),
                    exit_code: Some(result.exit_code),
                    error: result.error.clone(),
                });
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
                (TaskState::Queued, ResultDisposition::Requeued)
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

                (TaskState::Retrying, ResultDisposition::Requeued)
            } else {
                // Unrecoverable or exhausted failure
                entry.state = TaskState::Failed;
                entry.finished_instant = Some(now_instant);
                entry.error_message = result.error.clone();
                entry.result = Some(result);
                (TaskState::Failed, ResultDisposition::Terminal)
            }
        };

        if let Some(key) = delay_key_to_insert {
            inner.delayed_retries.insert(key);
        }
        if let Some(key) = ready_key_to_insert {
            inner.ready_queue.insert(key);
        }

        if res_state.is_terminal() {
            inner.track_terminal_task(task_id);
        }

        Ok(ResultOutcome {
            disposition,
            state: res_state,
            rejected,
        })
    }

    // --- Worker Disconnect & Failover ---

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
    /// If `immediate_reschedule` is true (e.g. graceful worker disconnect), tasks are
    /// re-enqueued directly to `ready_queue` with 0ms delay.
    /// Otherwise, exponential backoff is scheduled via `delayed_retries`.
    /// Infrastructure failovers are counted in `failover_count` against `max_failovers`,
    /// not against the task's execution retry budget.
    /// Returns the list of affected TaskIds.
    pub async fn handle_session_disconnected(
        &self,
        worker_id: &Uuid,
        session_id: Option<u64>,
        reason: &str,
        immediate_reschedule: bool,
    ) -> Vec<TaskId> {
        let mut guard = self.inner.write().await;
        let inner = &mut *guard;

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

        let mut affected = Vec::with_capacity(assigned.len());
        let mut delays_to_insert = Vec::new();
        let mut ready_to_insert = Vec::new();
        let mut terminal_to_track = Vec::new();

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
            if let Some(tasks) = inner.worker_assignments.get_mut(worker_id) {
                tasks.remove(&task_id);
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
                let used = if entry.task.requirements.max_retries.is_some() {
                    entry.retry_count
                } else {
                    entry.failover_count
                };
                entry.state = TaskState::Failed;
                entry.assigned_worker_id = None;
                entry.assigned_session = None;
                entry.finished_instant = Some(now_instant);
                let err_msg = format!(
                    "Task execution failed: maximum retry limit ({cap}) reached after worker eviction (Worker disconnected and retries exhausted ({used}/{cap}), last failure on worker {worker_id}: {reason})"
                );
                entry.error_message = Some(err_msg.clone());
                let fail_result =
                    TaskResult::failure(*worker_id, task_id, 1, "", "", 0, Some(err_msg));
                entry.result = Some(fail_result);
                terminal_to_track.push(task_id);
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
            return Ok(CancelOutcome {
                newly_cancelled: false,
                worker_to_notify: None,
                result: inner.tasks.get(task_id).and_then(|e| e.result.clone()),
            });
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

        let mut recorded = None;
        if let Some(entry) = inner.tasks.get_mut(task_id) {
            entry.state = TaskState::Cancelled;
            entry.finished_instant = Some(Instant::now());
            entry.error_message = cancel_reason;
            if entry.result.is_none() {
                entry.result = Some(cancel_result);
            }
            recorded = entry.result.clone();
        }
        inner.track_terminal_task(*task_id);

        Ok(CancelOutcome {
            newly_cancelled: true,
            worker_to_notify,
            result: recorded,
        })
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
        inner.enforce_terminal_limits();
    }

    /// Dynamically adjusts the byte budget of retained terminal tasks and evicts oldest tasks
    /// until it holds (see [`DEFAULT_MAX_TERMINAL_BYTES`]).
    pub async fn set_max_terminal_bytes(&self, limit: usize) {
        let mut inner = self.inner.write().await;
        inner.max_terminal_bytes = limit;
        inner.enforce_terminal_limits();
    }

    /// Bytes currently charged for retained terminal tasks.
    pub async fn terminal_retained_bytes(&self) -> usize {
        self.inner.read().await.terminal_bytes
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
        let pruned = queue
            .prune_terminal_older_than(Duration::from_millis(10))
            .await;
        assert_eq!(pruned, 1);
        assert!(queue.get_task(&ids[4]).await.is_none());
    }

    fn echo_task(label: &str) -> Task {
        Task::new(
            TaskSpec::command("echo", vec![label.to_string()]),
            TaskRequirements::generic(1, 10),
        )
    }

    /// Submits `task`, assigns it to `(worker, session)` and returns its id.
    async fn submit_and_assign(
        queue: &TaskQueue,
        task: Task,
        worker: Uuid,
        session: u64,
    ) -> TaskId {
        let id = queue.submit(task).await.unwrap();
        let scheduled = queue
            .schedule_tasks_batch_in_sessions(&[(id, worker, session)])
            .await;
        assert_eq!(scheduled.len(), 1);
        id
    }

    #[tokio::test]
    async fn test_duplicate_task_id_is_rejected_and_live_entry_untouched() {
        let queue = TaskQueue::new();
        let worker = Uuid::new_v4();

        // Case A: the original is Running; a resubmit must not reset it to Queued.
        let task = echo_task("dup-running");
        let clone_of_task = task.clone();
        let id = submit_and_assign(&queue, task, worker, 1).await;
        queue.mark_running_from(worker, Some(1), &id).await.unwrap();
        let err = queue.submit(clone_of_task).await.unwrap_err();
        assert!(err.to_string().contains("already exists"), "{err}");
        assert_eq!(queue.get_state(&id).await, Some(TaskState::Running));
        assert!(queue.get_schedulable_tasks().await.is_empty());

        // Case B: the original is Queued; a resubmit must not add a second ready-queue key.
        let queued = echo_task("dup-queued");
        let queued_clone = queued.clone();
        let qid = queue.submit(queued).await.unwrap();
        assert!(queue.submit(queued_clone).await.is_err());
        assert_eq!(queue.get_schedulable_tasks().await.len(), 1);
        let popped = queue.pop_and_schedule(worker, |_| true).await.unwrap();
        assert_eq!(popped.id, qid);
        assert!(
            queue.pop_and_schedule(worker, |_| true).await.is_none(),
            "no stale key may offer the same task twice"
        );

        // Case C: the original is terminal; it must not be resurrected.
        let done = echo_task("dup-done");
        let done_clone = done.clone();
        let did = submit_and_assign(&queue, done, worker, 1).await;
        queue
            .record_result_from(
                worker,
                Some(1),
                TaskResult::success(worker, did, "ok", 5, false),
            )
            .await
            .unwrap();
        assert!(queue.submit(done_clone).await.is_err());
        assert_eq!(queue.get_state(&did).await, Some(TaskState::Completed));
    }

    #[tokio::test]
    async fn test_stale_result_after_failover_is_dropped_and_leaves_no_poison_key() {
        let queue = TaskQueue::new();
        let (w1, w2) = (Uuid::new_v4(), Uuid::new_v4());
        let id = submit_and_assign(&queue, echo_task("stale"), w1, 1).await;
        queue.mark_running_from(w1, Some(1), &id).await.unwrap();

        // w1's session is lost; the task goes back to the queue
        let affected = queue
            .handle_session_disconnected(&w1, Some(1), "lost", true)
            .await;
        assert_eq!(affected, vec![id]);
        assert_eq!(queue.get_state(&id).await, Some(TaskState::Queued));

        // The zombie finishes anyway: both a failure and a success must be ignored.
        for zombie in [
            TaskResult::failure(w1, id, 1, "", "boom", 50, None),
            TaskResult::success(w1, id, "late", 50, false),
            TaskResult::failure(w1, id, 130, "", "", 0, None),
        ] {
            let outcome = queue.record_result_from(w1, Some(1), zombie).await.unwrap();
            assert_eq!(outcome.disposition, ResultDisposition::Stale);
            assert_eq!(outcome.state, TaskState::Queued);
        }
        assert_eq!(queue.get_state(&id).await, Some(TaskState::Queued));
        assert_eq!(
            queue.get_schedulable_tasks().await.len(),
            1,
            "exactly one live ready-queue key, no poisoned duplicates"
        );

        // It is still schedulable, and only the new owner can finish it.
        assert_eq!(
            queue
                .schedule_tasks_batch_in_sessions(&[(id, w2, 7)])
                .await
                .len(),
            1
        );
        let wrong_session = queue
            .record_result_from(w2, Some(1), TaskResult::success(w2, id, "x", 1, false))
            .await
            .unwrap();
        assert_eq!(wrong_session.disposition, ResultDisposition::Stale);
        let right = queue
            .record_result_from(w2, Some(7), TaskResult::success(w2, id, "ok", 1, false))
            .await
            .unwrap();
        assert_eq!(right.disposition, ResultDisposition::Terminal);
        assert_eq!(queue.get_state(&id).await, Some(TaskState::Completed));
    }

    #[tokio::test]
    async fn test_result_from_foreign_worker_cannot_complete_fail_or_cancel_a_task() {
        let queue = TaskQueue::new();
        let (owner, intruder) = (Uuid::new_v4(), Uuid::new_v4());
        let id = submit_and_assign(&queue, echo_task("owned"), owner, 1).await;
        queue.mark_running_from(owner, Some(1), &id).await.unwrap();

        // The intruder spoofs the owner's id in the body; the sender is the connection, not the body.
        for forged in [
            TaskResult::success(owner, id, "forged", 1, false),
            TaskResult::failure(owner, id, 1, "", "forged failure", 1, None),
            TaskResult::failure(owner, id, 130, "", "", 0, None),
        ] {
            let outcome = queue
                .record_result_from(intruder, Some(9), forged)
                .await
                .unwrap();
            assert_eq!(outcome.disposition, ResultDisposition::Stale);
        }
        assert_eq!(queue.get_state(&id).await, Some(TaskState::Running));

        // A failure result for a task that was never assigned (still Queued) must not fail it either.
        let queued = queue.submit(echo_task("never-assigned")).await.unwrap();
        let outcome = queue
            .record_result_from(
                intruder,
                Some(9),
                TaskResult::failure(intruder, queued, 1, "", "x", 1, None),
            )
            .await
            .unwrap();
        assert_eq!(outcome.disposition, ResultDisposition::Stale);
        assert_eq!(queue.get_state(&queued).await, Some(TaskState::Queued));
        assert_eq!(queue.get_schedulable_tasks().await.len(), 1);

        // Progress cannot reassign the task and checkpoints from the intruder are not stored.
        assert!(queue
            .mark_running_from(intruder, Some(9), &id)
            .await
            .is_err());
        assert_eq!(
            queue.get_task(&id).await.unwrap().assigned_worker_id,
            Some(owner)
        );
        // Same for a task that has not reported Running yet: a foreign "Running" report must neither
        // advance it nor take it over.
        let pending = submit_and_assign(&queue, echo_task("not-yet-running"), owner, 1).await;
        assert!(queue
            .mark_running_from(intruder, Some(9), &pending)
            .await
            .is_err());
        let info = queue.get_task(&pending).await.unwrap();
        assert_eq!(info.state, TaskState::Scheduled);
        assert_eq!(info.assigned_worker_id, Some(owner));
        queue
            .mark_running_from(owner, Some(1), &pending)
            .await
            .unwrap();
        let poison = CheckpointData::new(
            u64::MAX,
            rusty_grid_core::task::Bytes::copy_from_slice(b"junk"),
        );
        assert!(!queue
            .record_checkpoint_from(intruder, Some(9), id, poison)
            .await
            .unwrap());
        assert!(queue.get_checkpoint(&id).await.is_none());
        let genuine = CheckpointData::new(1, rusty_grid_core::task::Bytes::copy_from_slice(b"ok"));
        assert!(queue
            .record_checkpoint_from(owner, Some(1), id, genuine)
            .await
            .unwrap());
        assert_eq!(queue.get_checkpoint(&id).await.unwrap().sequence, 1);

        // The genuine result still goes through.
        let done = queue
            .record_result_from(
                owner,
                Some(1),
                TaskResult::success(owner, id, "real", 3, false),
            )
            .await
            .unwrap();
        assert_eq!(done.disposition, ResultDisposition::Terminal);
        assert_eq!(queue.get_result(&id).await.unwrap().stdout_str(), "real");
    }

    #[tokio::test]
    async fn test_late_result_from_owner_of_cancelled_task_is_not_stale() {
        let queue = TaskQueue::new();
        let w = Uuid::new_v4();
        let id = submit_and_assign(&queue, echo_task("cancel-late"), w, 3).await;
        queue.mark_running_from(w, Some(3), &id).await.unwrap();
        let cancel = queue
            .cancel_task_detailed(&id, Some("stop".into()))
            .await
            .unwrap();
        assert!(cancel.newly_cancelled);
        assert_eq!(cancel.worker_to_notify, Some(w));
        assert_eq!(cancel.result.as_ref().unwrap().exit_code, 130);

        // The worker acknowledges with its own cancelled result: from the owner, so the caller
        // must still release the slot, but the state stays Cancelled.
        let outcome = queue
            .record_result_from(w, Some(3), TaskResult::failure(w, id, 130, "", "", 1, None))
            .await
            .unwrap();
        assert_eq!(outcome.disposition, ResultDisposition::Late);
        assert_eq!(outcome.state, TaskState::Cancelled);

        let again = queue.cancel_task_detailed(&id, None).await.unwrap();
        assert!(!again.newly_cancelled);
        assert!(queue
            .cancel_task_detailed(&TaskId::new(), None)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_session_scoped_failover_spares_the_newer_session() {
        let queue = TaskQueue::new();
        let w = Uuid::new_v4();
        let old = submit_and_assign(&queue, echo_task("old-session"), w, 1).await;
        let new = submit_and_assign(&queue, echo_task("new-session"), w, 2).await;

        let affected = queue
            .handle_session_disconnected(&w, Some(1), "superseded", true)
            .await;
        assert_eq!(
            affected,
            vec![old],
            "only the superseded session is failed over"
        );
        assert_eq!(queue.get_state(&old).await, Some(TaskState::Queued));
        assert_eq!(queue.get_state(&new).await, Some(TaskState::Scheduled));
        assert_eq!(queue.get_task(&new).await.unwrap().retry_count, 0);

        // Failing over the same session twice is a no-op: exactly once.
        assert!(queue
            .handle_session_disconnected(&w, Some(1), "again", true)
            .await
            .is_empty());
        assert_eq!(queue.get_task(&old).await.unwrap().retry_count, 1);

        // A session-less sweep (legacy callers) takes everything the worker still has.
        assert_eq!(
            queue.handle_worker_disconnected(&w, "gone", true).await,
            vec![new]
        );
    }

    #[tokio::test]
    async fn test_sweep_of_a_later_session_recovers_tasks_stranded_on_an_older_one() {
        // Session 1's sweep never ran (its handler was aborted by session 2's registration, and
        // session 2's by session 3's): the sweep of session 2 must still take session 1's tasks.
        let queue = TaskQueue::new();
        let w = Uuid::new_v4();
        let first = submit_and_assign(&queue, echo_task("stranded-1"), w, 1).await;
        queue.mark_running_from(w, Some(1), &first).await.unwrap();
        let second = submit_and_assign(&queue, echo_task("stranded-2"), w, 2).await;
        let third = submit_and_assign(&queue, echo_task("live-3"), w, 3).await;

        let mut affected = queue
            .handle_session_disconnected(&w, Some(2), "superseded", true)
            .await;
        affected.sort_by_key(|id| id.0);
        let mut expected = vec![first, second];
        expected.sort_by_key(|id| id.0);
        assert_eq!(affected, expected, "sessions 1 and 2 are both dead");
        assert_eq!(queue.get_state(&first).await, Some(TaskState::Queued));
        assert_eq!(queue.get_state(&second).await, Some(TaskState::Queued));
        assert_eq!(
            queue.get_state(&third).await,
            Some(TaskState::Scheduled),
            "the newest session keeps what it was given"
        );
        assert_eq!(queue.get_task(&first).await.unwrap().retry_count, 1);
    }

    #[tokio::test]
    async fn test_deliver_if_scheduled_only_runs_for_the_exact_owner_of_a_scheduled_task() {
        let queue = TaskQueue::new();
        let w = Uuid::new_v4();
        let id = submit_and_assign(&queue, echo_task("deliver"), w, 4).await;

        assert_eq!(queue.deliver_if_scheduled(&id, &w, 4, || 7).await, Some(7));
        // Another session of the same worker, another worker, an unknown task: nothing runs.
        assert_eq!(queue.deliver_if_scheduled(&id, &w, 5, || 7).await, None);
        assert_eq!(
            queue
                .deliver_if_scheduled(&id, &Uuid::new_v4(), 4, || 7)
                .await,
            None
        );
        assert_eq!(
            queue
                .deliver_if_scheduled(&TaskId::new(), &w, 4, || 7)
                .await,
            None
        );

        // Once the task is running, or cancelled, it is no longer the dispatcher's to deliver.
        queue.mark_running_from(w, Some(4), &id).await.unwrap();
        assert_eq!(queue.deliver_if_scheduled(&id, &w, 4, || 7).await, None);
        let cancelled = submit_and_assign(&queue, echo_task("deliver-cancelled"), w, 4).await;
        queue.cancel_task_detailed(&cancelled, None).await.unwrap();
        assert_eq!(
            queue.deliver_if_scheduled(&cancelled, &w, 4, || 7).await,
            None
        );
    }

    #[tokio::test]
    async fn test_failover_has_its_own_budget_and_keeps_execution_retries_intact() {
        // Defaults: max_retries = 3 but max_failovers = 10 -> far more than 3 network handoffs survive.
        let queue = TaskQueue::new();
        let id = queue.submit(echo_task("roaming")).await.unwrap();
        for round in 1..=6u32 {
            let w = Uuid::new_v4();
            queue.schedule_task(&id, w).await.expect("schedulable");
            let affected = queue.handle_worker_disconnected(&w, "handoff", true).await;
            assert_eq!(affected, vec![id]);
            assert_eq!(
                queue.get_state(&id).await,
                Some(TaskState::Queued),
                "round {round}"
            );
        }
        assert_eq!(queue.get_task(&id).await.unwrap().retry_count, 6);

        // Execution retries are budgeted separately: 6 failovers did not use up max_retries = 1.
        let policy = RetryPolicy {
            max_retries: 1,
            retry_on_execution_failure: true,
            initial_backoff_ms: 1,
            ..Default::default()
        };
        let queue = TaskQueue::with_config(policy, None);
        let id = queue.submit(echo_task("flaky")).await.unwrap();
        for _ in 0..6 {
            let w = Uuid::new_v4();
            queue.schedule_task(&id, w).await.unwrap();
            queue.handle_worker_disconnected(&w, "handoff", true).await;
        }
        let w = Uuid::new_v4();
        queue.schedule_task(&id, w).await.unwrap();
        let first_failure = queue
            .record_result(TaskResult::failure(w, id, 2, "", "nope", 10, None))
            .await
            .unwrap();
        assert_eq!(
            first_failure,
            TaskState::Retrying,
            "one execution retry is still available"
        );
        tokio::time::sleep(Duration::from_millis(150)).await; // backoff doubles per re-dispatch
        queue.process_delayed_retries().await;
        queue.schedule_task(&id, w).await.unwrap();
        let second_failure = queue
            .record_result(TaskResult::failure(w, id, 2, "", "nope", 10, None))
            .await
            .unwrap();
        assert_eq!(
            second_failure,
            TaskState::Failed,
            "execution budget (1) is now spent"
        );
    }

    #[tokio::test]
    async fn test_failover_budget_is_bounded_and_zero_retries_means_never_rerun() {
        let policy = RetryPolicy {
            max_retries: 1,
            max_failovers: 2,
            ..Default::default()
        };
        let queue = TaskQueue::with_config(policy, None);
        let id = queue.submit(echo_task("poison")).await.unwrap();
        for _ in 0..2 {
            let w = Uuid::new_v4();
            queue.schedule_task(&id, w).await.unwrap();
            queue.handle_worker_disconnected(&w, "crash", true).await;
            assert_eq!(queue.get_state(&id).await, Some(TaskState::Queued));
        }
        let w = Uuid::new_v4();
        queue.schedule_task(&id, w).await.unwrap();
        queue.handle_worker_disconnected(&w, "crash", true).await;
        assert_eq!(queue.get_state(&id).await, Some(TaskState::Failed));
        let err = queue.get_result(&id).await.unwrap().error.unwrap();
        assert!(err.contains("maximum retry limit (2) reached"), "{err}");
        assert!(err.contains("(2/2)"), "{err}");

        // max_retries = 0 on the policy: never run a task twice, so no failover either.
        let queue = TaskQueue::with_config(
            RetryPolicy {
                max_retries: 0,
                ..Default::default()
            },
            None,
        );
        let id = queue.submit(echo_task("once")).await.unwrap();
        let w = Uuid::new_v4();
        queue.schedule_task(&id, w).await.unwrap();
        queue.handle_worker_disconnected(&w, "crash", true).await;
        assert_eq!(queue.get_state(&id).await, Some(TaskState::Failed));
    }

    /// The exact texts `WorkerClient` and the task runner emit when they refuse to run a task.
    fn worker_rejection(worker: Uuid, task: TaskId, stderr: &str, error: &str) -> TaskResult {
        TaskResult::failure(worker, task, 1, "", stderr, 0, Some(error.to_string()))
    }

    #[test]
    fn test_worker_rejection_detection_matches_worker_messages_only() {
        let (w, t) = (Uuid::new_v4(), TaskId::new());
        assert!(is_worker_rejection(&worker_rejection(
            w,
            t,
            "Task rejected by worker: Critical battery preservation (<15% discharging)",
            "Battery critically low (<15% discharging)"
        )));
        assert!(is_worker_rejection(&worker_rejection(
            w,
            t,
            "Task requirements exceed worker capabilities (CPU, RAM, or GPU)",
            "Worker capabilities insufficient for task requirements"
        )));
        assert!(is_worker_rejection(&worker_rejection(
            w,
            t,
            "Execution Error: BuiltinTest requires GPU, but worker has no GPU capability (Worker lacks GPU capability).",
            "Worker lacks GPU capability"
        )));

        // A task that genuinely ran and failed is not a rejection, however fast it was.
        assert!(!is_worker_rejection(&TaskResult::failure(
            w, t, 1, "", "boom", 0, None
        )));
        assert!(!is_worker_rejection(&TaskResult::failure(
            w,
            t,
            1,
            "",
            "BuiltinTest: intentional failure requested",
            0,
            Some("Intentional failure".into())
        )));
        assert!(
            !is_worker_rejection(&TaskResult::failure(
                w,
                t,
                1,
                "",
                "Task rejected by worker: x",
                250,
                Some("x".into())
            )),
            "a refusal is produced before anything runs, so it takes no measurable time"
        );
        assert!(!is_worker_rejection(&TaskResult::success(
            w, t, "ok", 0, false
        )));
    }

    #[tokio::test]
    async fn test_rejected_task_goes_to_another_worker_and_is_bounded() {
        let queue = TaskQueue::new();
        let phone = Uuid::new_v4();
        let id = submit_and_assign(&queue, echo_task("battery"), phone, 4).await;
        let rejection = worker_rejection(
            phone,
            id,
            "Task rejected by worker: Critical battery preservation (<15% discharging)",
            "Battery critically low (<15% discharging)",
        );
        let outcome = queue
            .record_result_from(phone, Some(4), rejection.clone())
            .await
            .unwrap();
        assert_eq!(outcome.disposition, ResultDisposition::Requeued);
        assert!(outcome.rejected);
        assert_eq!(outcome.state, TaskState::Queued);
        let info = queue.get_task(&id).await.unwrap();
        assert_eq!(info.assigned_worker_id, None);
        assert_eq!(info.retry_count, 1);
        assert_eq!(queue.avoided_workers_for(&[id]).await[&id], vec![phone]);
        assert_eq!(queue.get_schedulable_tasks().await.len(), 1);

        // Keeps being rejected: bounded, then the real reason is reported.
        let mut last = outcome;
        for round in 2..=MAX_WORKER_REJECTIONS_PER_TASK + 1 {
            let w = Uuid::new_v4();
            queue
                .schedule_tasks_batch_in_sessions(&[(id, w, round as u64)])
                .await;
            last = queue
                .record_result_from(
                    w,
                    Some(round as u64),
                    worker_rejection(
                        w,
                        id,
                        "Task rejected by worker: Critical battery",
                        "Battery critically low",
                    ),
                )
                .await
                .unwrap();
        }
        assert_eq!(last.disposition, ResultDisposition::Terminal);
        assert_eq!(last.state, TaskState::Failed);
        assert!(queue
            .get_result(&id)
            .await
            .unwrap()
            .error
            .unwrap()
            .contains("Battery critically low"));
    }

    #[tokio::test]
    async fn test_requeue_dispatch_failure_is_guarded_and_uncharged() {
        let queue = TaskQueue::new();
        let w = Uuid::new_v4();
        let id = submit_and_assign(&queue, echo_task("undelivered"), w, 5).await;

        // Wrong session / wrong worker: nothing happens.
        assert!(!queue.requeue_dispatch_failure(&id, &w, 6).await.unwrap());
        assert!(!queue
            .requeue_dispatch_failure(&id, &Uuid::new_v4(), 5)
            .await
            .unwrap());
        assert_eq!(queue.get_state(&id).await, Some(TaskState::Scheduled));

        assert!(queue.requeue_dispatch_failure(&id, &w, 5).await.unwrap());
        let info = queue.get_task(&id).await.unwrap();
        assert_eq!(info.state, TaskState::Queued);
        assert_eq!(
            info.retry_count, 0,
            "an undelivered task was never tried, so nothing is charged"
        );
        assert_eq!(queue.get_schedulable_tasks().await.len(), 1);

        // A task cancelled in the meantime is never resurrected, by either requeue entry point.
        let id2 = submit_and_assign(&queue, echo_task("cancelled-meanwhile"), w, 5).await;
        queue.cancel_task(&id2, None).await.unwrap();
        assert!(!queue.requeue_dispatch_failure(&id2, &w, 5).await.unwrap());
        assert!(queue.requeue_task(&id2).await.is_err());
        assert_eq!(queue.get_state(&id2).await, Some(TaskState::Cancelled));
    }

    #[tokio::test]
    async fn test_cluster_buckets_cover_every_state() {
        let policy = RetryPolicy {
            initial_backoff_ms: 60_000,
            ..Default::default()
        };
        let queue = TaskQueue::with_config(policy, None);
        let w = Uuid::new_v4();

        let _queued = queue.submit(echo_task("q")).await.unwrap();
        let scheduled = submit_and_assign(&queue, echo_task("s"), w, 1).await;
        let running = submit_and_assign(&queue, echo_task("r"), w, 1).await;
        queue.mark_running_from(w, Some(1), &running).await.unwrap();
        let completed = submit_and_assign(&queue, echo_task("c"), w, 1).await;
        queue
            .record_result_from(w, Some(1), TaskResult::success(w, completed, "", 1, false))
            .await
            .unwrap();
        let failed = submit_and_assign(&queue, echo_task("f"), w, 1).await;
        queue
            .record_result_from(
                w,
                Some(1),
                TaskResult::failure(w, failed, 2, "", "", 1, None),
            )
            .await
            .unwrap();
        let cancelled = queue.submit(echo_task("x")).await.unwrap();
        queue.cancel_task(&cancelled, None).await.unwrap();
        let retrying = submit_and_assign(&queue, echo_task("t"), Uuid::new_v4(), 1).await;
        let retrying_worker = queue
            .get_task(&retrying)
            .await
            .unwrap()
            .assigned_worker_id
            .unwrap();
        queue
            .handle_worker_disconnected(&retrying_worker, "lost", false)
            .await;
        let _ = scheduled;

        let stats = queue.stats().await;
        assert_eq!(stats.total, 7);
        assert_eq!(
            (stats.queued, stats.scheduled, stats.running, stats.retrying),
            (1, 1, 1, 1)
        );
        let (pending, running_bucket, completed_bucket, failed_bucket) = stats.cluster_buckets();
        assert_eq!(pending, 2, "queued + retrying");
        assert_eq!(running_bucket, 2, "scheduled + running");
        assert_eq!(completed_bucket, 1);
        assert_eq!(failed_bucket, 2, "failed + cancelled");
        assert_eq!(
            pending + running_bucket + completed_bucket + failed_bucket,
            stats.total
        );
    }

    #[tokio::test]
    async fn test_stale_ready_keys_are_never_offered_and_get_pruned() {
        // Whatever leaks a ready-queue key for a task that is no longer Queued (the unguarded failure
        // paths used to), that key must not take scheduling slots or be dispatched a second time.
        let queue = TaskQueue::new();
        let w = Uuid::new_v4();
        let live = queue.submit(echo_task("live")).await.unwrap();
        let done = submit_and_assign(&queue, echo_task("done"), w, 1).await;
        queue
            .record_result_from(w, Some(1), TaskResult::success(w, done, "ok", 1, false))
            .await
            .unwrap();
        let stale_key = {
            let mut inner = queue.inner.write().await;
            let entry = &inner.tasks[&done];
            let key = QueueOrderKey {
                priority_rev: Reverse(entry.priority),
                sequence: entry.sequence,
                task_id: done,
            };
            inner.ready_queue.insert(key);
            key
        };

        let offered: Vec<TaskId> = queue
            .get_schedulable_tasks()
            .await
            .iter()
            .map(|t| t.id)
            .collect();
        assert_eq!(
            offered,
            vec![live],
            "a finished task must not be offered to the scheduler"
        );

        // Even if a scheduler pass names it, it is not re-dispatched, its state is untouched and the
        // key is dropped instead of being re-offered forever.
        assert!(queue
            .schedule_tasks_batch_in_sessions(&[(done, w, 2)])
            .await
            .is_empty());
        assert_eq!(queue.get_state(&done).await, Some(TaskState::Completed));
        assert!(!queue.inner.read().await.ready_queue.contains(&stale_key));

        // The one-by-one path skips stale keys too.
        let popped = queue.pop_and_schedule(w, |_| true).await.unwrap();
        assert_eq!(popped.id, live);
        assert!(queue.pop_and_schedule(w, |_| true).await.is_none());
    }

    // --- Step B: byte-bounded retention, scheduling views, loads, listing ---

    /// Runs `task` on `worker` (session 1) and finishes it with `output_bytes` of stdout.
    async fn finish_with_output(
        queue: &TaskQueue,
        task: Task,
        worker: Uuid,
        output_bytes: usize,
    ) -> TaskId {
        let id = submit_and_assign(queue, task, worker, 1).await;
        let result = TaskResult::success(worker, id, "x".repeat(output_bytes), 1, false);
        queue
            .record_result_from(worker, Some(1), result)
            .await
            .unwrap();
        id
    }

    #[tokio::test]
    async fn test_terminal_retention_is_bounded_by_bytes_oldest_first() {
        const MIB: usize = 1024 * 1024;
        // Plenty of room by count, 2.5 MiB by bytes: only the two newest 1 MiB results fit.
        let queue =
            TaskQueue::with_retention_limits(RetryPolicy::default(), None, 10_000, 5 * MIB / 2);
        let worker = Uuid::new_v4();
        let mut ids = Vec::new();
        for i in 0..5 {
            ids.push(
                finish_with_output(&queue, echo_task(&format!("bulky-{i}")), worker, MIB).await,
            );
        }

        for old in &ids[..3] {
            assert!(
                queue.get_task(old).await.is_none(),
                "the oldest finished tasks must be evicted once the byte budget is exceeded"
            );
        }
        for kept in &ids[3..] {
            assert!(queue.get_task(kept).await.is_some());
            assert!(queue.get_result(kept).await.is_some());
        }
        assert!(
            queue.terminal_retained_bytes().await <= 5 * MIB / 2,
            "retained bytes must stay within the budget"
        );
        assert!(queue.terminal_retained_bytes().await >= 2 * MIB);
    }

    #[tokio::test]
    async fn test_the_newest_result_survives_even_when_larger_than_the_whole_budget() {
        const MIB: usize = 1024 * 1024;
        let queue = TaskQueue::with_retention_limits(RetryPolicy::default(), None, 10_000, MIB / 4);
        let worker = Uuid::new_v4();
        let small = finish_with_output(&queue, echo_task("small"), worker, 10).await;
        let huge = finish_with_output(&queue, echo_task("huge"), worker, 2 * MIB).await;

        // The client that just got `huge` must still be able to read it back...
        assert_eq!(
            queue.get_result(&huge).await.unwrap().stdout.0.len(),
            2 * MIB
        );
        // ...at the price of everything older.
        assert!(queue.get_task(&small).await.is_none());
    }

    #[tokio::test]
    async fn test_lowering_the_byte_budget_evicts_and_eviction_releases_the_charge() {
        const MIB: usize = 1024 * 1024;
        let queue = TaskQueue::new();
        let worker = Uuid::new_v4();
        let mut ids = Vec::new();
        for i in 0..4 {
            ids.push(finish_with_output(&queue, echo_task(&format!("t-{i}")), worker, MIB).await);
        }
        assert!(queue.terminal_retained_bytes().await >= 4 * MIB);

        queue.set_max_terminal_bytes(2 * MIB + MIB / 2).await;
        assert!(queue.get_task(&ids[0]).await.is_none());
        assert!(queue.get_task(&ids[1]).await.is_none());
        assert!(queue.get_task(&ids[3]).await.is_some());
        assert!(queue.terminal_retained_bytes().await <= 2 * MIB + MIB / 2);

        // Count-based and TTL eviction release the byte charge as well.
        queue.set_max_terminal_tasks(0).await;
        assert_eq!(queue.terminal_retained_bytes().await, 0);
        assert_eq!(queue.stats().await.total, 0);
    }

    #[tokio::test]
    async fn test_payload_of_a_finished_task_counts_toward_the_budget() {
        // A task that carries a large stdin and prints nothing still holds that stdin.
        let queue = TaskQueue::new();
        let worker = Uuid::new_v4();
        let task = Task::new(
            TaskSpec::Command {
                program: "cat".into(),
                args: vec![],
                env: Default::default(),
                working_dir: None,
                stdin: Some(vec![0u8; 3 * 1024 * 1024]),
            },
            TaskRequirements::generic(1, 10),
        );
        finish_with_output(&queue, task, worker, 1).await;
        assert!(queue.terminal_retained_bytes().await >= 3 * 1024 * 1024);
    }

    #[tokio::test]
    async fn test_ready_views_page_through_the_queue_in_order_without_cloning_tasks() {
        let queue = TaskQueue::new();
        let mut ids = Vec::new();
        for i in 0..10 {
            ids.push(queue.submit(echo_task(&format!("v-{i}"))).await.unwrap());
        }
        // One of them is a GPU task: the view must say so.
        let gpu = Task::new(TaskSpec::gpu_compute("gemm", 8), TaskRequirements::gpu(30));
        let gpu_id = queue.submit(gpu).await.unwrap();

        let mut seen = Vec::new();
        let mut cursor = None;
        let mut pages = 0;
        loop {
            let (views, next) = queue.ready_views(cursor, 4).await;
            pages += 1;
            for v in &views {
                if v.task_id == gpu_id {
                    assert!(v.requires_gpu && v.heavy);
                } else {
                    assert!(!v.requires_gpu && !v.heavy);
                }
            }
            seen.extend(views.into_iter().map(|v| v.task_id));
            match next {
                Some(key) => cursor = Some(key),
                None => break,
            }
        }
        let mut expected = ids.clone();
        expected.push(gpu_id);
        assert_eq!(
            seen, expected,
            "pages must cover the queue exactly once, in FIFO order"
        );
        assert!(pages >= 3);
    }

    #[tokio::test]
    async fn test_ready_views_skip_tasks_that_are_no_longer_queued_and_carry_avoid_lists() {
        let queue = TaskQueue::new();
        let worker = Uuid::new_v4();
        let assigned = submit_and_assign(&queue, echo_task("gone"), worker, 1).await;
        let waiting = queue.submit(echo_task("waiting")).await.unwrap();
        // Hand-feed a rejection so the entry has an avoid list.
        {
            let mut inner = queue.inner.write().await;
            inner
                .tasks
                .get_mut(&waiting)
                .unwrap()
                .avoid_workers
                .push(worker);
        }

        let (views, next) = queue.ready_views(None, 10).await;
        assert!(next.is_none());
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].task_id, waiting);
        assert_eq!(views[0].avoid, vec![worker]);
        assert_ne!(views[0].task_id, assigned);
        assert_eq!(queue.ready_task_ids(10).await, vec![waiting]);
    }

    #[tokio::test]
    async fn test_assigned_loads_come_from_the_assignment_set_with_slots_and_unstarted_ram() {
        let queue = TaskQueue::new();
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let big = Task::new(
            TaskSpec::command("make", vec![]),
            TaskRequirements::new(8, 4096, false, 60),
        );
        let big_id = submit_and_assign(&queue, big, a, 1).await;
        let small_id = submit_and_assign(&queue, echo_task("small"), a, 1).await;
        let other_id = submit_and_assign(&queue, echo_task("other"), b, 1).await;

        let loads = queue.assigned_loads().await;
        assert_eq!(loads[&a].tasks, 2);
        assert_eq!(loads[&a].slots, 8 + 1, "an 8-core task occupies 8 slots");
        assert_eq!(loads[&a].unstarted_ram_mb, 4096 + 512);
        assert_eq!(loads[&b].tasks, 1);

        // Once the worker reports a task running its RAM is in the worker's own figure.
        queue.mark_running_from(a, Some(1), &big_id).await.unwrap();
        assert_eq!(queue.assigned_loads().await[&a].unstarted_ram_mb, 512);

        // A finished task leaves the books; a worker with nothing left disappears from the map.
        queue
            .record_result_from(b, Some(1), TaskResult::success(b, other_id, "", 1, false))
            .await
            .unwrap();
        queue
            .record_result_from(a, Some(1), TaskResult::success(a, small_id, "", 1, false))
            .await
            .unwrap();
        let loads = queue.assigned_loads().await;
        assert!(!loads.contains_key(&b));
        assert_eq!(loads[&a].tasks, 1);
    }

    #[tokio::test]
    async fn test_recent_tasks_are_newest_first_with_offset_limit_and_filter() {
        let queue = TaskQueue::new();
        let worker = Uuid::new_v4();
        let mut ids = Vec::new();
        for i in 0..30 {
            ids.push(queue.submit(echo_task(&format!("r-{i}"))).await.unwrap());
        }
        // Finish the 5 oldest.
        for id in &ids[..5] {
            let scheduled = queue
                .schedule_tasks_batch_in_sessions(&[(*id, worker, 1)])
                .await;
            assert_eq!(scheduled.len(), 1);
            queue
                .record_result_from(
                    worker,
                    Some(1),
                    TaskResult::success(worker, *id, "", 1, false),
                )
                .await
                .unwrap();
        }

        let newest: Vec<TaskId> = queue
            .recent_tasks(|_| true, 0, 3)
            .await
            .into_iter()
            .map(|t| t.task_id)
            .collect();
        assert_eq!(newest, vec![ids[29], ids[28], ids[27]]);

        let page2: Vec<TaskId> = queue
            .recent_tasks(|_| true, 3, 3)
            .await
            .into_iter()
            .map(|t| t.task_id)
            .collect();
        assert_eq!(page2, vec![ids[26], ids[25], ids[24]]);

        let done: Vec<TaskId> = queue
            .recent_tasks(|e| e.state == TaskState::Completed, 0, 100)
            .await
            .into_iter()
            .map(|t| t.task_id)
            .collect();
        assert_eq!(done, vec![ids[4], ids[3], ids[2], ids[1], ids[0]]);

        assert_eq!(queue.recent_tasks(|_| true, 0, 0).await.len(), 0);
        assert_eq!(queue.recent_tasks(|_| true, 100, 10).await.len(), 0);
        assert_eq!(queue.recent_tasks(|_| true, 0, usize::MAX).await.len(), 30);
        assert_eq!(queue.recent_tasks(|_| true, usize::MAX, 5).await.len(), 0);
    }

    #[test]
    fn test_task_state_spelling_matches_its_serde_representation() {
        for state in [
            TaskState::Submitted,
            TaskState::Queued,
            TaskState::Scheduled,
            TaskState::Running,
            TaskState::Completed,
            TaskState::Failed,
            TaskState::Cancelled,
            TaskState::Retrying,
        ] {
            assert_eq!(
                serde_json::to_value(state).unwrap(),
                serde_json::Value::String(state.as_str().to_string()),
                "TaskState::as_str must be the serde spelling every endpoint uses"
            );
        }
    }
}
