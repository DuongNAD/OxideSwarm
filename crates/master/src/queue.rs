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
use rusty_grid_core::protocol::ProtocolCapabilities;
use rusty_grid_core::task::{
    CheckpointData, Task, TaskId, TaskRequirements, TaskResult, TaskSpec, TaskStatus,
};

use crate::store::{
    DurableAck, JournalRecord, PersistedTaskEntry, Recovered, SnapshotImage, TaskStore,
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

/// Default cap on uncharged voluntary worker drains (`WorkerMessage::Disconnecting`) per task.
pub const DEFAULT_MAX_EVACUATIONS: u32 = 10;

/// A task that workers keep handing back without running it is failed after this many rejections.
pub const MAX_WORKER_REJECTIONS_PER_TASK: u32 = 3;

fn default_max_failovers() -> u32 {
    DEFAULT_MAX_FAILOVERS
}

fn default_max_evacuations() -> u32 {
    DEFAULT_MAX_EVACUATIONS
}

/// Configurable retry policy and exponential backoff strategy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Maximum number of retries permitted per task (default: 3).
    ///
    /// Bounds *execution* retries (`retry_on_execution_failure`). Infrastructure failovers have their
    /// own budget, `max_failovers`, and announced voluntary drains have `max_evacuations`; the only
    /// effect `max_retries` has on them is that `0` means "never run this task twice" and so disables
    /// failover re-runs (and drain re-runs once the task is `Running`) as well.
    pub max_retries: u32,
    /// Maximum infrastructure failovers per task (default: 10).
    ///
    /// A failover is an unannounced or involuntary re-dispatch caused by the worker, not the task: its
    /// connection dropped without `WorkerMessage::Disconnecting`, its heartbeat timed out, it
    /// re-registered over an existing session, or it refused the task unrun. A 2-hour job on a phone
    /// that roams networks must not be failed for the network's sake after three handoffs, but a
    /// task that kills every worker it touches still has to stop eventually. The effective cap is
    /// `max(max_failovers, max_retries)`, so raising `max_retries` never reduces it. An explicit
    /// per-task `requirements.max_retries` overrides this and bounds all charged re-runs (execution
    /// retries, infrastructure failovers, and drains beyond `max_evacuations`) of that task.
    #[serde(default = "default_max_failovers")]
    pub max_failovers: u32,
    /// Maximum uncharged voluntary worker drains (`WorkerMessage::Disconnecting`) per task (default: 10).
    ///
    /// Each announced drain below this cap returns the task to `Queued` immediately and increments
    /// `evacuation_count` (recording an audit entry in `retry_history`) without charging `retry_count`
    /// or `failover_count`. Once `evacuation_count >= max_evacuations`, further drains take the
    /// charged failover path, and when `max_retries` is `0` (either on `RetryPolicy` or on
    /// `requirements.max_retries`), a `Running` task is never re-queued by a drain.
    #[serde(default = "default_max_evacuations")]
    pub max_evacuations: u32,
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
            max_evacuations: DEFAULT_MAX_EVACUATIONS,
            initial_backoff_ms: 500,
            max_backoff_ms: 30_000,
            backoff_multiplier: 2.0,
            retry_on_execution_failure: false,
            retry_on_worker_disconnect: true,
        }
    }
}

impl RetryPolicy {
    /// Whether `entry` may be re-run once more on the charged failover path (unannounced disconnect,
    /// heartbeat timeout, superseded session, or announced drain past `max_evacuations`), and the cap
    /// that applied.
    fn failover_budget(&self, entry: &TaskEntry) -> (bool, u32) {
        match entry.task.requirements.max_retries {
            // An explicit per-task limit bounds every charged re-run (execution retries,
            // infrastructure failovers, and drains past `max_evacuations`), with `0` also forbidding
            // any drain re-run once the task is `Running`.
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

    /// True when "run at most once" (`max_retries == 0`) is in effect for `entry`.
    fn zero_retries_for(&self, entry: &TaskEntry) -> bool {
        match entry.task.requirements.max_retries {
            Some(limit) => limit == 0,
            None => self.max_retries == 0,
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
    pub retry_after_instant: Option<Instant>,
    /// Total re-dispatches of this task: execution retries plus `failover_count`.
    pub retry_count: u32,
    /// Re-dispatches caused by infrastructure (worker lost, reaped, superseded, or refused the task
    /// unrun, or an announced drain past `max_evacuations`), tracked apart from execution retries
    /// so network churn cannot exhaust `max_retries`.
    pub failover_count: u32,
    /// Voluntary worker drains (`WorkerMessage::Disconnecting`, handled by
    /// [`TaskQueue::handle_session_evacuated`]) that returned this task to `Queued` without
    /// charging `retry_count` or `failover_count` (bounded by [`RetryPolicy::max_evacuations`]).
    pub evacuation_count: u32,
    /// Times a worker handed this task back without running it.
    pub rejection_count: u32,
    /// Workers that rejected this task; the scheduler places it elsewhere while it can.
    pub avoid_workers: Vec<Uuid>,
    /// Connection session of `assigned_worker_id` the task was dispatched on (`None` for
    /// assignments made without session information). Results, progress and checkpoints are only
    /// accepted from this worker *and* session, so a superseded or reaped session cannot touch it.
    pub assigned_session: Option<u64>,
    /// When set (`Some((worker_id, hold_until))`), this `Queued` task is held for `worker_id` to
    /// resume via `WorkerMessage::ResumeSession` until `hold_until` expires.
    pub adoptable_by: Option<(Uuid, Instant)>,
    /// True when a task was `Running` with effective `max_retries == 0` before a master restart
    /// (or worker disconnect hold) and must transition to `TaskState::Failed` if not adopted
    /// before `adoptable_by` expires.
    pub fail_if_not_adopted: bool,
    /// True when `adoptable_by` was entered on a live worker disconnect (`handle_session_disconnected`)
    /// rather than a master restart (`attach_store_and_replay`), so if the hold expires or the
    /// worker omits this task from `ResumeSession`, the deferred failover charge (`retry_count += 1`,
    /// `failover_count += 1`) is applied then.
    pub pending_failover_charge: bool,
    /// Disconnect reason captured when `pending_failover_charge` is set, used to populate
    /// `RetryAttempt` or the terminal failure error if the task is not adopted.
    pub pending_failover_reason: Option<String>,
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
            failover_count: self.failover_count,
            evacuation_count: self.evacuation_count,
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
    #[serde(default)]
    pub failover_count: u32,
    #[serde(default)]
    pub evacuation_count: u32,
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
    /// Bytes currently charged for active (non-terminal) tasks in the queue.
    #[serde(default)]
    pub active_bytes: usize,
    /// Configured active byte budget (`--max-active-mb`, default 256 MiB).
    #[serde(default)]
    pub max_active_bytes: usize,
    /// Error message when the attached `TaskStore` writer has failed since startup (`None` when
    /// durability is healthy or no `--state-dir` is configured).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store_error: Option<String>,
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

/// Default byte cap for active (non-terminal) tasks (256 MiB): payloads, checkpoints and history.
pub const DEFAULT_MAX_ACTIVE_BYTES: usize = 256 * 1024 * 1024;

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
    /// Protocol features a worker must advertise to run the task: `FILE_STAGING` for inline staged
    /// files or expected outputs (Requirement R2), `BLOB_TRANSFER` for blob inputs or output
    /// patterns, `THREE_D` for `Generative3D` and `AutoRig` tasks.
    pub required_features: ProtocolCapabilities,
    /// Workers that already handed this task back without running it.
    pub avoid: Vec<Uuid>,
    /// The task's placement constraints (`require:` tags), or why they cannot be read. Submission
    /// validates them, so an error only reaches here for a task that bypassed it; such a task is
    /// placed nowhere.
    pub placement: Result<Placement, String>,
    /// Named persistent workspace (`Task::workspace`), if any (design 0039 D4).
    pub workspace: Option<String>,
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
            required_features: required_features(task),
            avoid: Vec::new(),
            placement: task.placement(),
            workspace: task.workspace.as_ref().map(|w| w.name.clone()),
        }
    }
}

/// The protocol features a worker needs to run `task` (see [`TaskView::required_features`]).
fn required_features(task: &Task) -> ProtocolCapabilities {
    let mut features = ProtocolCapabilities::empty();
    if !task.staged_files.is_empty() || !task.expected_outputs.is_empty() {
        features.insert(ProtocolCapabilities::FILE_STAGING);
    }
    if task.uses_blobs() {
        features.insert(ProtocolCapabilities::BLOB_TRANSFER);
    }
    if task.inputs.iter().any(|input| {
        matches!(
            input.source,
            rusty_grid_core::blob::InputSource::Tree { manifest, .. }
                if rusty_grid_core::blob::is_v2_manifest_blob(&manifest)
        )
    }) {
        features.insert(ProtocolCapabilities::TREE_V2);
    }
    if matches!(task.spec, TaskSpec::Generative3D(_) | TaskSpec::AutoRig(_)) {
        features.insert(ProtocolCapabilities::THREE_D);
    }
    if let Some(workspace) = &task.workspace {
        features.insert(ProtocolCapabilities::WORKSPACES);
        if let Some(source) = &workspace.source {
            features.insert(ProtocolCapabilities::BLOB_TRANSFER);
            let rusty_grid_core::task::WorkspaceSource::Tree { manifest, .. } = source;
            if rusty_grid_core::blob::is_v2_manifest_blob(manifest) {
                features.insert(ProtocolCapabilities::TREE_V2);
            }
        }
    }
    features
}

pub use rusty_grid_core::task::WorkspaceAffinity;

/// Upper bound on in-memory workspace affinity entries so ephemeral workspace names cannot grow
/// the map without bound.
const MAX_WORKSPACE_AFFINITIES: usize = 4_096;

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
    /// True when the task was held in `TaskState::Queued` under an active `adoptable_by` window for
    /// this worker and was adopted + completed in one step (e.g. a QUIC data-lane `TaskResult`
    /// arriving before `ResumeSession` on the control lane).
    pub adopted_from_hold: bool,
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

/// Outcome of [`TaskQueue::adopt_session_tasks`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AdoptionOutcome {
    /// Tasks adopted into `TaskState::Running` on `(worker_id, session_id)`.
    pub adopted: Vec<TaskId>,
    /// Tasks reported in `running` that could not be adopted (unknown, terminal, expired hold, or
    /// owned by another worker) and must receive `MasterMessage::CancelTask`.
    pub stale_to_cancel: Vec<TaskId>,
    /// Tasks previously held for `worker_id` that were omitted from `running` (or whose hold
    /// expired) and transitioned to `TaskState::Failed` (for example `Running` with
    /// `max_retries == 0`).
    pub newly_failed: Vec<TaskId>,
}

/// Upper bound on tracked `SESSION_RESUME`-capable worker IDs so disconnected ephemeral workers
/// cannot grow the set without bound.
const MAX_SESSION_RESUME_WORKERS: usize = 4_096;

#[derive(Default)]
struct SessionResumeTracker {
    /// FIFO insertion order of workers in `session_resume_workers`.
    order: std::collections::VecDeque<Uuid>,
    /// Workers with a currently connected `WorkerSession`, mapped to the highest live `session_id`
    /// so an older session's disconnect sweep never clears `connected` while a newer session lives.
    connected: HashMap<Uuid, u64>,
    /// Workers that currently have active assignments (`worker_assignments`) or active holds (`held_tasks`).
    with_work_or_hold: HashSet<Uuid>,
}

/// True when `entry` is in `TaskState::Queued` and eligible for scheduling now (not held in an
/// active `adoptable_by` window, not awaiting `fail_if_not_adopted` failure, and if it has a
/// deferred failover charge, its failover budget is not exhausted).
#[inline]
fn is_schedulable_queued(entry: &TaskEntry, policy: &RetryPolicy, now: Instant) -> bool {
    entry.state == TaskState::Queued
        && !entry.fail_if_not_adopted
        && entry.adoptable_by.is_none_or(|(_, until)| now >= until)
        && (!entry.pending_failover_charge
            || (policy.retry_on_worker_disconnect && policy.failover_budget(entry).0))
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
    max_active_bytes: usize,
    terminal_order: std::collections::VecDeque<TaskId>,
    /// Bytes each retained terminal task was charged when it finished (see `retained_bytes`).
    terminal_sizes: HashMap<TaskId, usize>,
    /// Sum of `terminal_sizes`.
    terminal_bytes: usize,
    /// Bytes each active (non-terminal) task is currently charged (see `retained_bytes`).
    active_sizes: HashMap<TaskId, usize>,
    /// Sum of `active_sizes`.
    active_bytes: usize,
    /// Monotonically incremented whenever a task is inserted into `ready_queue`.
    ready_epoch: u64,
    /// Optional durable task journal and snapshot store (`master --state-dir`).
    store: Option<TaskStore>,
    /// True once [`TaskQueue::close_and_compact_store`] / [`TaskQueue::close_and_compact_store_sync`]
    /// has detached the store for master shutdown; new submissions and post-shutdown disconnect
    /// charges are refused so acknowledged work is never lost and clean shutdowns never charge
    /// running tasks.
    store_closed: bool,
    /// Tasks currently carrying an `adoptable_by` hold or `fail_if_not_adopted` flag.
    held_tasks: HashSet<TaskId>,
    /// Atomic mirror of `held_tasks.len()` so `release_expired_adoption_holds` is O(1) lock-free
    /// when no hold is active.
    adoption_holds: Arc<std::sync::atomic::AtomicUsize>,
    /// How long the master holds tasks of a `SESSION_RESUME`-capable worker on disconnect or
    /// restart before releasing or charging failover (`Duration::ZERO` disables holds).
    session_resume_hold: Duration,
    /// Workers whose most recent registration negotiated `ProtocolCapabilities::SESSION_RESUME`.
    session_resume_workers: Arc<std::sync::Mutex<HashSet<Uuid>>>,
    /// Tracks FIFO order and liveness/assignment protection for `session_resume_workers`.
    session_resume_tracker: Arc<std::sync::Mutex<SessionResumeTracker>>,
    /// Per-workspace affinity recorded when a worker finishes a task in that workspace (design 0039 D4).
    workspace_affinity: HashMap<String, WorkspaceAffinity>,
    /// Index of tasks currently occupying each workspace (`Scheduled`, `Running`, or held in
    /// `held_tasks` via `adoptable_by` / `fail_if_not_adopted`).
    workspace_active_tasks: HashMap<String, HashSet<TaskId>>,
}

/// Bytes one blob reference (id, size, flags) is charged in addition to its path.
const BLOB_REF_BYTES: usize = 48;

/// Bytes a freshly submitted task is charged against the active byte budget.
pub(crate) fn task_active_bytes(task: &Task) -> usize {
    let spec = match &task.spec {
        TaskSpec::Command {
            program,
            args,
            env,
            working_dir,
            stdin,
        } => {
            program.len()
                + args.iter().map(String::len).sum::<usize>()
                + env.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>()
                + working_dir.as_ref().map_or(0, |p| p.as_os_str().len())
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
            target_dir,
        } => {
            crate_name.len()
                + source_files
                    .iter()
                    .map(|(k, v)| k.len() + v.len())
                    .sum::<usize>()
                + compiler_flags.iter().map(String::len).sum::<usize>()
                + target_dir.as_ref().map_or(0, |p| p.as_os_str().len())
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
                + s.engine_preference.as_ref().map_or(0, |e| e.as_str().len())
        }
        TaskSpec::AutoRig(s) => {
            s.input_mesh.len()
                + s.rig_preset.len()
                + s.embed_animations.iter().map(String::len).sum::<usize>()
                + s.preferred_provider.as_ref().map_or(0, String::len)
        }
    };
    let checkpoint = task
        .latest_checkpoint
        .as_ref()
        .map_or(0, |c| c.delta_state.0.len());
    let staged_files_bytes: usize = task
        .staged_files
        .iter()
        .map(|f| f.path.len() + f.data.len())
        .sum();
    let expected_outputs_bytes: usize = task.expected_outputs.iter().map(String::len).sum();
    // Blob inputs and outputs are references (the content lives in the blob store).
    let blob_refs_bytes: usize = task
        .inputs
        .iter()
        .map(|i| i.path.len() + BLOB_REF_BYTES)
        .chain(task.output_globs.iter().map(String::len))
        .sum();
    let workspace_bytes = task.workspace.as_ref().map_or(0, |w| {
        w.name.len()
            + if w.source.is_some() {
                BLOB_REF_BYTES
            } else {
                0
            }
    });
    RETAINED_ENTRY_OVERHEAD_BYTES
        + spec
        + staged_files_bytes
        + expected_outputs_bytes
        + blob_refs_bytes
        + workspace_bytes
        + checkpoint
        + task.tags.iter().map(String::len).sum::<usize>()
}

/// Bytes of payload and output a task entry keeps alive: its spec (stdin, sources, input data),
/// checkpoint, captured stdout/stderr and the strings of its history.
fn retained_bytes(entry: &TaskEntry) -> usize {
    let result = entry.result.as_ref().map_or(0, |r| {
        r.stdout.0.len()
            + r.stderr.0.len()
            + r.error.as_ref().map_or(0, String::len)
            + r.device_name.as_ref().map_or(0, String::len)
            + r.asset_bytes.as_ref().map_or(0, |b| b.0.len())
            + r.artifacts
                .iter()
                .map(|a| a.path.len() + a.data.len())
                .sum::<usize>()
            + r.outputs
                .iter()
                .map(|o| o.path.len() + BLOB_REF_BYTES)
                .sum::<usize>()
    });
    let history: usize = entry
        .retry_history
        .iter()
        .map(|a| a.reason.len() + a.error.as_ref().map_or(0, String::len))
        .sum();
    task_active_bytes(&entry.task)
        + result
        + history
        + entry.error_message.as_ref().map_or(0, String::len)
}

impl TaskQueueInner {
    fn untrack_active_task(&mut self, task_id: &TaskId) {
        if let Some(size) = self.active_sizes.remove(task_id) {
            self.active_bytes = self.active_bytes.saturating_sub(size);
        }
    }

    fn update_active_task(&mut self, task_id: TaskId) {
        self.untrack_active_task(&task_id);
        if let Some(entry) = self.tasks.get(&task_id).filter(|e| !e.state.is_terminal()) {
            let size = retained_bytes(entry);
            self.active_sizes.insert(task_id, size);
            self.active_bytes = self.active_bytes.saturating_add(size);
        }
    }

    /// Drops a retained terminal task and its byte charge.
    fn remove_retained(&mut self, task_id: &TaskId) {
        let (prev_hold_worker, ws_name) = self
            .tasks
            .get(task_id)
            .map(|e| {
                (
                    e.adoptable_by.map(|(w, _)| w),
                    e.task.workspace.as_ref().map(|ws| ws.name.clone()),
                )
            })
            .unwrap_or((None, None));
        self.tasks.remove(task_id);
        self.untrack_active_task(task_id);
        self.track_adoption_hold(*task_id, false);
        if let Some(ws) = ws_name {
            if let Some(set) = self.workspace_active_tasks.get_mut(&ws) {
                set.remove(task_id);
                if set.is_empty() {
                    self.workspace_active_tasks.remove(&ws);
                }
            }
        }
        if let Some(wid) = prev_hold_worker {
            self.refresh_worker_resume_protection(wid);
        }
        if let Some(size) = self.terminal_sizes.remove(task_id) {
            self.terminal_bytes = self.terminal_bytes.saturating_sub(size);
        }
    }

    fn sync_adoption_holds(&self) {
        self.adoption_holds
            .store(self.held_tasks.len(), std::sync::atomic::Ordering::Release);
    }

    fn track_adoption_hold(&mut self, task_id: TaskId, has_hold: bool) {
        let changed = if has_hold {
            self.held_tasks.insert(task_id)
        } else {
            self.held_tasks.remove(&task_id)
        };
        if changed {
            self.sync_adoption_holds();
        }
        self.sync_workspace_active(task_id);
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
            self.append_non_durable(&JournalRecord::TaskEvicted { task_id: oldest });
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
        self.untrack_active_task(&task_id);
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

    fn insert_ready(&mut self, key: QueueOrderKey) {
        if self.ready_queue.insert(key) {
            self.ready_epoch = self.ready_epoch.wrapping_add(1);
        }
    }

    fn snapshot_image(&self) -> SnapshotImage {
        let mut tasks: Vec<PersistedTaskEntry> = self
            .tasks
            .values()
            .map(|entry| PersistedTaskEntry {
                task: entry.task.clone(),
                priority: entry.priority,
                sequence: entry.sequence,
                state: entry.state,
                assigned_worker_id: entry.assigned_worker_id,
                submitted_at_utc: entry.submitted_at_utc,
                retry_count: entry.retry_count,
                failover_count: entry.failover_count,
                evacuation_count: entry.evacuation_count,
                rejection_count: entry.rejection_count,
                avoid_workers: entry.avoid_workers.clone(),
                adoptable_by_worker: entry.adoptable_by.map(|(w, _)| w),
                fail_if_not_adopted: entry.fail_if_not_adopted,
                retry_history: entry.retry_history.clone(),
                result: entry.result.clone(),
                error_message: entry.error_message.clone(),
            })
            .collect();
        tasks.sort_unstable_by_key(|e| e.sequence);
        SnapshotImage {
            next_sequence: self.next_sequence,
            tasks,
            terminal_order: self.terminal_order.iter().copied().collect(),
        }
    }

    /// Enqueues a snapshot compaction if the journal has crossed the size or record threshold.
    ///
    /// Must be called only at the end of a queue operation after all in-memory mutations and all
    /// journal records for that operation have been appended, so `snapshot_image()` and the
    /// journal cut never fall in the middle of a multi-step transition.
    fn maybe_trigger_compaction(&self) {
        let Some(ref store) = self.store else {
            return;
        };
        if !store.needs_compaction() {
            return;
        }
        let image = self.snapshot_image();
        if let Err(err) = store.compact(image) {
            tracing::warn!(error = %err, "Failed to enqueue task store compaction");
        }
    }

    fn append_non_durable(&self, record: &JournalRecord) {
        if let Some(ref store) = self.store {
            if let Err(err) = store.append(record) {
                tracing::warn!(error = %err, "Failed to append non-durable task journal record");
            }
        }
    }

    fn append_durable(&self, record: &JournalRecord) -> GridResult<Option<DurableAck>> {
        let Some(ref store) = self.store else {
            return Ok(None);
        };
        let ack = store.append_durable(record)?;
        Ok(Some(ack))
    }

    fn append_durable_warn(&self, record: &JournalRecord) -> Option<DurableAck> {
        match self.append_durable(record) {
            Ok(ack) => ack,
            Err(err) => {
                tracing::warn!(error = %err, "Failed to enqueue durable task journal record");
                None
            }
        }
    }

    fn record_workspace_affinity(&mut self, name: &str, worker_id: Uuid, last_used_utc: u64) {
        if !self.workspace_affinity.contains_key(name)
            && self.workspace_affinity.len() >= MAX_WORKSPACE_AFFINITIES
        {
            if let Some(oldest_key) = self
                .workspace_affinity
                .iter()
                .min_by_key(|(k, v)| (v.last_used_utc, *k))
                .map(|(k, _)| k.clone())
            {
                self.workspace_affinity.remove(&oldest_key);
            }
        }
        self.workspace_affinity.insert(
            name.to_string(),
            WorkspaceAffinity {
                name: name.to_string(),
                worker: worker_id,
                last_used_utc,
            },
        );
    }

    fn sync_workspace_active(&mut self, task_id: TaskId) {
        let Some(entry) = self.tasks.get(&task_id) else {
            return;
        };
        let Some(ws) = entry.task.workspace.as_ref() else {
            return;
        };
        let occupies =
            entry.state.is_active() || entry.fail_if_not_adopted || entry.adoptable_by.is_some();
        if occupies {
            if let Some(set) = self.workspace_active_tasks.get_mut(&ws.name) {
                set.insert(task_id);
            } else {
                let mut set = HashSet::new();
                set.insert(task_id);
                self.workspace_active_tasks.insert(ws.name.clone(), set);
            }
        } else if let Some(set) = self.workspace_active_tasks.get_mut(&ws.name) {
            set.remove(&task_id);
            if set.is_empty() {
                let name = ws.name.clone();
                self.workspace_active_tasks.remove(&name);
            }
        }
    }

    fn is_workspace_active(&self, name: &str, now: Instant) -> bool {
        self.workspace_active_tasks.get(name).is_some_and(|ids| {
            ids.iter().any(|tid| {
                self.tasks.get(tid).is_some_and(|entry| {
                    entry.state.is_active()
                        || entry.fail_if_not_adopted
                        || entry.adoptable_by.is_some_and(|(_, until)| now < until)
                })
            })
        })
    }

    fn refresh_worker_resume_protection(&self, worker_id: Uuid) {
        let has_active = self
            .worker_assignments
            .get(&worker_id)
            .is_some_and(|tasks| !tasks.is_empty());
        let has_holds = self.held_tasks.iter().any(|tid| {
            self.tasks
                .get(tid)
                .is_some_and(|e| e.adoptable_by.is_some_and(|(w, _)| w == worker_id))
        });
        let mut tracker = self
            .session_resume_tracker
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if has_active || has_holds {
            tracker.with_work_or_hold.insert(worker_id);
        } else {
            tracker.with_work_or_hold.remove(&worker_id);
        }
    }

    fn rebuild_all_resume_protection(&self) {
        let mut protected = HashSet::new();
        for (wid, tasks) in &self.worker_assignments {
            if !tasks.is_empty() {
                protected.insert(*wid);
            }
        }
        for tid in &self.held_tasks {
            if let Some((wid, _)) = self.tasks.get(tid).and_then(|e| e.adoptable_by) {
                protected.insert(wid);
            }
        }
        let mut tracker = self
            .session_resume_tracker
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        tracker.with_work_or_hold = protected;
    }
}

const UNRESUMED_ZERO_RETRY_ERROR: &str =
    "master restarted while the task ran; not re-run because max_retries is 0";

/// Thread-safe in-memory Task Queue and lifecycle coordinator.
#[derive(Clone)]
pub struct TaskQueue {
    inner: Arc<RwLock<TaskQueueInner>>,
    adoption_holds: Arc<std::sync::atomic::AtomicUsize>,
    session_resume_hold_secs: Arc<std::sync::atomic::AtomicU64>,
    session_resume_workers: Arc<std::sync::Mutex<HashSet<Uuid>>>,
    session_resume_tracker: Arc<std::sync::Mutex<SessionResumeTracker>>,
    #[cfg(test)]
    ready_views_calls: Arc<std::sync::atomic::AtomicU64>,
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
            DEFAULT_MAX_ACTIVE_BYTES,
        )
    }

    /// Like [`with_retention`](Self::with_retention), also bounding the bytes (payloads plus
    /// captured output) kept for finished tasks and the bytes held by active (non-terminal) tasks.
    pub fn with_retention_limits(
        retry_policy: RetryPolicy,
        capacity: Option<usize>,
        max_terminal_tasks: usize,
        max_terminal_bytes: usize,
        max_active_bytes: usize,
    ) -> Self {
        let adoption_holds = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let session_resume_hold_secs = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let session_resume_workers = Arc::new(std::sync::Mutex::new(HashSet::new()));
        let session_resume_tracker =
            Arc::new(std::sync::Mutex::new(SessionResumeTracker::default()));
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
                max_active_bytes,
                terminal_order: std::collections::VecDeque::new(),
                terminal_sizes: HashMap::new(),
                terminal_bytes: 0,
                active_sizes: HashMap::new(),
                active_bytes: 0,
                ready_epoch: 0,
                store: None,
                store_closed: false,
                held_tasks: HashSet::new(),
                adoption_holds: Arc::clone(&adoption_holds),
                session_resume_hold: Duration::ZERO,
                session_resume_workers: Arc::clone(&session_resume_workers),
                session_resume_tracker: Arc::clone(&session_resume_tracker),
                workspace_affinity: HashMap::new(),
                workspace_active_tasks: HashMap::new(),
            })),
            adoption_holds,
            session_resume_hold_secs,
            session_resume_workers,
            session_resume_tracker,
            #[cfg(test)]
            ready_views_calls: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

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
        inner.ready_queue.remove(&QueueOrderKey {
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
                inner.ready_queue.remove(&QueueOrderKey {
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

        {
            let mut inner = self.inner.write().await;
            inner.session_resume_hold = clamped_hold;
            self.session_resume_hold_secs
                .store(clamped_hold.as_secs(), std::sync::atomic::Ordering::Release);
            inner.tasks.clear();
            inner.ready_queue.clear();
            inner.delayed_retries.clear();
            inner.worker_assignments.clear();
            inner.terminal_order.clear();
            inner.terminal_sizes.clear();
            inner.terminal_bytes = 0;
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
                let entry = TaskEntry {
                    task: p.task,
                    priority: p.priority,
                    sequence: p.sequence,
                    state,
                    assigned_worker_id,
                    submitted_at_utc: p.submitted_at_utc,
                    submitted_instant: now,
                    scheduled_instant: None,
                    started_instant: None,
                    retry_after_instant: None,
                    retry_count: p.retry_count,
                    failover_count: p.failover_count,
                    evacuation_count: p.evacuation_count,
                    rejection_count: p.rejection_count,
                    avoid_workers: p.avoid_workers,
                    assigned_session: None,
                    adoptable_by,
                    fail_if_not_adopted,
                    pending_failover_charge: false,
                    pending_failover_reason: None,
                    retry_history: p.retry_history,
                    result: p.result,
                    error_message: p.error_message,
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
            inner.maybe_trigger_compaction();
        }

        for ack in acks {
            if let Err(err) = ack.await {
                tracing::warn!(error = %err, "Failed to sync replay zero-retry failure record");
            }
        }
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
            match blob_store.resolve_inputs(&inputs).await {
                Ok(ids) => {
                    let missing = blob_store.pin_task(task_id, ids);
                    if !missing.is_empty() {
                        tracing::warn!(
                            task_id = %task_id,
                            missing_count = missing.len(),
                            "Recovered active task references blob(s) missing from the blob store"
                        );
                    }
                }
                Err(err) => {
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
                    inner.ready_queue.remove(&QueueOrderKey {
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
        let (store, ack) = {
            let mut inner = self.inner.write().await;
            inner.store_closed = true;
            let Some(store) = inner.store.take() else {
                return;
            };
            let image = inner.snapshot_image();
            let ack = store.compact(image).ok();
            (store, ack)
        };
        if let Some(ack) = ack {
            let _ = ack.await;
        }
        drop(store);
    }

    /// Synchronous counterpart to [`close_and_compact_store`](Self::close_and_compact_store) for
    /// callers such as `MasterHandle::shutdown` that run outside an `async fn`.
    pub fn close_and_compact_store_sync(&self) {
        if let Ok(mut inner) = self.inner.try_write() {
            inner.store_closed = true;
            if let Some(store) = inner.store.take() {
                let image = inner.snapshot_image();
                let _ = store.compact(image);
                drop(inner);
                drop(store);
            }
            return;
        }
        let queue = self.clone();
        let _ = std::thread::spawn(move || {
            let mut inner = queue.inner.blocking_write();
            inner.store_closed = true;
            if let Some(store) = inner.store.take() {
                let image = inner.snapshot_image();
                let _ = store.compact(image);
                drop(inner);
                drop(store);
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

    // --- Task Submission ---

    /// Submits a task with default priority (0).
    pub async fn submit(&self, task: Task) -> GridResult<TaskId> {
        self.submit_with_priority(task, 0).await
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
            inner.next_sequence += 1;

            let entry = TaskEntry {
                task,
                priority: 0,
                sequence,
                state: TaskState::Queued,
                assigned_worker_id: None,
                submitted_at_utc: now_epoch,
                submitted_instant: now_instant,
                scheduled_instant: None,
                started_instant: None,
                retry_after_instant: None,
                retry_count: 0,
                failover_count: 0,
                evacuation_count: 0,
                rejection_count: 0,
                avoid_workers: Vec::new(),
                assigned_session: None,
                adoptable_by: None,
                fail_if_not_adopted: false,
                pending_failover_charge: false,
                pending_failover_reason: None,
                retry_history: Vec::new(),
                result: None,
                error_message: None,
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
                        inner.ready_queue.remove(&ready_key);
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
        task.validate().map_err(GridError::Config)?;

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
            inner.next_sequence += 1;

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
                retry_after_instant: None,
                retry_count: 0,
                failover_count: 0,
                evacuation_count: 0,
                rejection_count: 0,
                avoid_workers: Vec::new(),
                assigned_session: None,
                adoptable_by: None,
                fail_if_not_adopted: false,
                pending_failover_charge: false,
                pending_failover_reason: None,
                retry_history: Vec::new(),
                result: None,
                error_message: None,
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

        if let Some(durable_ack) = ack {
            let ready_key = QueueOrderKey {
                priority_rev: Reverse(priority),
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
                        inner.ready_queue.remove(&ready_key);
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

    // --- Scheduler Matchmaking & Queries ---

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
            let mut view = TaskView::of(&entry.task);
            view.avoid = entry.avoid_workers.clone();
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
                if !entry.state.is_active() {
                    continue;
                };
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
                    }
                }
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
                inner.ready_queue.remove(&key);
                Self::apply_pending_failover_charge_before_dispatch(
                    &mut inner,
                    key.task_id,
                    now_epoch,
                );
                let (task, zero_retries) = {
                    let entry = inner.tasks.get_mut(&key.task_id)?;
                    let zr = policy.zero_retries_for(entry);
                    entry.state = TaskState::Scheduled;
                    entry.assigned_worker_id = Some(worker_id);
                    entry.assigned_session = None;
                    entry.adoptable_by = None;
                    entry.fail_if_not_adopted = false;
                    entry.pending_failover_charge = false;
                    entry.pending_failover_reason = None;
                    entry.scheduled_instant = Some(now);
                    (entry.task.clone(), zr)
                };
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

            inner.ready_queue.remove(&QueueOrderKey {
                priority_rev: Reverse(priority_info.0),
                sequence: priority_info.1,
                task_id: *task_id,
            });
            Self::apply_pending_failover_charge_before_dispatch(&mut inner, *task_id, now_epoch);

            let (task, zero_retries) = {
                let entry = inner.tasks.get_mut(task_id)?;
                let zr = policy.zero_retries_for(entry);
                entry.state = TaskState::Scheduled;
                entry.assigned_worker_id = Some(worker_id);
                entry.assigned_session = None;
                entry.adoptable_by = None;
                entry.fail_if_not_adopted = false;
                entry.pending_failover_charge = false;
                entry.pending_failover_reason = None;
                entry.scheduled_instant = Some(now);
                (entry.task.clone(), zr)
            };
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
                Self::apply_pending_failover_charge_before_dispatch(&mut inner, task_id, now_epoch);

                if let Some(entry) = inner.tasks.get_mut(&task_id) {
                    let zero_retries = policy.zero_retries_for(entry);
                    entry.state = TaskState::Scheduled;
                    entry.assigned_worker_id = Some(worker_id);
                    entry.assigned_session = session_id;
                    entry.adoptable_by = None;
                    entry.fail_if_not_adopted = false;
                    entry.pending_failover_charge = false;
                    entry.pending_failover_reason = None;
                    entry.scheduled_instant = Some(now);
                    let task = entry.task.clone();
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
            let zero_retries = {
                let entry = inner
                    .tasks
                    .get(task_id)
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
                inner.retry_policy.zero_retries_for(entry)
            };

            if let Some(entry) = inner.tasks.get_mut(task_id) {
                entry.state = TaskState::Running;
                entry.started_instant = Some(Instant::now());
            }
            inner.sync_workspace_active(*task_id);

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

        let mut inner = self.inner.write().await;
        if inner.store_closed {
            return Err(GridError::Storage(
                "task store is closed (master shutting down)".into(),
            ));
        }
        let policy = inner.retry_policy.clone();

        let now_instant = Instant::now();
        let now_epoch = Utc::now().timestamp() as u64;
        let (state, owner, adoptable_by_sender, priority, sequence) = {
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
                },
                None,
            ));
        }

        if adoptable_by_sender {
            inner.ready_queue.remove(&QueueOrderKey {
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

        let ack = inner.append_durable_warn(&journal_record);
        inner.maybe_trigger_compaction();

        Ok((
            ResultOutcome {
                disposition,
                state: res_state,
                rejected,
                adopted_from_hold: adoptable_by_sender,
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

            inner.ready_queue.remove(&QueueOrderKey {
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
        Some(result)
    }

    // --- Worker Disconnect & Failover ---

    /// Returns all active tasks assigned to a draining worker (`WorkerMessage::Disconnecting`) to
    /// `Queued` immediately (without charging `retry_count` or `failover_count` while under
    /// [`RetryPolicy::max_evacuations`], or via the charged failover path past that cap).
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
        let (affected, acks) = {
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

            (affected, acks)
        };

        for ack in acks {
            let _ = ack.await;
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
        let (affected, acks) = {
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

            // Defense-in-depth: Also capture any active task in inner.tasks assigned to this worker
            for (id, entry) in &inner.tasks {
                if entry.assigned_worker_id == Some(*worker_id) && entry.state.is_active() {
                    assigned.insert(*id);
                }
            }

            let now_instant = Instant::now();
            let now_epoch = Utc::now().timestamp() as u64;
            let policy = inner.retry_policy.clone();
            let hold_for_resume = !inner.session_resume_hold.is_zero()
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

            (affected, acks)
        };

        for ack in acks {
            let _ = ack.await;
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

        Ok(outcome)
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
        let mut inner = self.inner.write().await;
        inner.max_terminal_tasks = limit;
        inner.enforce_terminal_limits();
        inner.maybe_trigger_compaction();
    }

    /// Dynamically adjusts the byte budget of retained terminal tasks and evicts oldest tasks
    /// until it holds (see [`DEFAULT_MAX_TERMINAL_BYTES`]).
    pub async fn set_max_terminal_bytes(&self, limit: usize) {
        let mut inner = self.inner.write().await;
        inner.max_terminal_bytes = limit;
        inner.enforce_terminal_limits();
        inner.maybe_trigger_compaction();
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
        let mut inner = self.inner.write().await;
        let evicted = inner.evict_terminal_tasks(max_retained);
        if evicted > 0 {
            inner.maybe_trigger_compaction();
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

    #[tokio::test]
    async fn test_repeated_evacuations_leave_failover_and_retry_counts_at_zero_and_preserve_budgets(
    ) {
        // 1. Drains 1..=max_evacuations (3) leave retry_count == 0 and failover_count == 0,
        //    increment evacuation_count, and record each drain in retry_history.
        //    Past max_evacuations, drain 4 takes the charged path (retry_count = 1,
        //    failover_count = 1), and drain 5 (N + 1 after both uncharged and charged caps)
        //    transitions the task to Failed.
        let policy = RetryPolicy {
            max_retries: 1,
            max_failovers: 1,
            max_evacuations: 3,
            ..Default::default()
        };
        let queue = TaskQueue::with_config(policy, None);
        let id = queue
            .submit(Task::new(
                TaskSpec::command("echo", vec!["evac".into()]),
                TaskRequirements::generic(1, 10).with_max_retries(1),
            ))
            .await
            .unwrap();

        for round in 1..=3u64 {
            let w = Uuid::new_v4();
            let scheduled = queue
                .schedule_tasks_batch_in_sessions(&[(id, w, round)])
                .await;
            assert_eq!(scheduled.len(), 1, "round {round} must schedule");
            queue.mark_running_from(w, Some(round), &id).await.unwrap();

            let affected = if round % 2 == 1 {
                queue
                    .handle_session_evacuated(&w, Some(round), "FAST_PRE_SLEEP_BATTERY_LOW")
                    .await
            } else {
                queue
                    .handle_worker_evacuated(&w, "Graceful worker shutdown")
                    .await
            };
            assert_eq!(affected, vec![id]);

            let info = queue.get_task(&id).await.unwrap();
            assert_eq!(info.state, TaskState::Queued, "round {round}");
            assert_eq!(info.retry_count, 0, "round {round}");
            assert_eq!(info.failover_count, 0, "round {round}");
            assert_eq!(info.evacuation_count, round as u32, "round {round}");
            assert_eq!(info.assigned_worker_id, None, "round {round}");
            let history = queue.get_retry_history(&id).await.unwrap();
            assert_eq!(history.len(), round as usize, "round {round}");
            assert!(
                history
                    .last()
                    .unwrap()
                    .error
                    .as_deref()
                    .unwrap_or("")
                    .contains(&format!("uncharged evacuation {round}/3")),
                "history entry must record uncharged evacuation: {history:?}"
            );
        }

        // Drain 4: past max_evacuations (3), so it takes the charged path (retry_count = 1, failover_count = 1).
        let w4 = Uuid::new_v4();
        assert_eq!(
            queue
                .schedule_tasks_batch_in_sessions(&[(id, w4, 4)])
                .await
                .len(),
            1
        );
        queue.mark_running_from(w4, Some(4), &id).await.unwrap();
        queue
            .handle_session_evacuated(&w4, Some(4), "FAST_PRE_SLEEP_BATTERY_LOW")
            .await;
        let info4 = queue.get_task(&id).await.unwrap();
        assert_eq!(info4.state, TaskState::Queued);
        assert_eq!(info4.evacuation_count, 3);
        assert_eq!(info4.retry_count, 1);
        assert_eq!(info4.failover_count, 1);
        assert_eq!(queue.get_retry_history(&id).await.unwrap().len(), 4);

        // Drain 5 (N + 1): both max_evacuations (3) and charged budget (1) are exhausted -> Failed.
        let w5 = Uuid::new_v4();
        assert_eq!(
            queue
                .schedule_tasks_batch_in_sessions(&[(id, w5, 5)])
                .await
                .len(),
            1
        );
        queue.mark_running_from(w5, Some(5), &id).await.unwrap();
        queue
            .handle_session_evacuated(&w5, Some(5), "FAST_PRE_SLEEP_BATTERY_LOW")
            .await;
        let info5 = queue.get_task(&id).await.unwrap();
        assert_eq!(info5.state, TaskState::Failed);
        assert_eq!(info5.evacuation_count, 3);
        assert_eq!(info5.retry_count, 1);

        // 2. When a task's charged budget is already spent (or N = max_evacuations = 3 while
        //    Scheduled with 0 charged budget), N drains succeed and the (N + 1)-th drain fails.
        let sched_id = queue
            .submit(Task::new(
                TaskSpec::command("echo", vec!["sched-zero".into()]),
                TaskRequirements::generic(1, 10).with_max_retries(0),
            ))
            .await
            .unwrap();
        for round in 1..=3u64 {
            let w = Uuid::new_v4();
            queue
                .schedule_tasks_batch_in_sessions(&[(sched_id, w, round)])
                .await;
            queue
                .handle_session_evacuated(&w, Some(round), "drain before start")
                .await;
            assert_eq!(queue.get_state(&sched_id).await, Some(TaskState::Queued));
        }
        let w_n_plus_1 = Uuid::new_v4();
        queue
            .schedule_tasks_batch_in_sessions(&[(sched_id, w_n_plus_1, 4)])
            .await;
        queue
            .handle_session_evacuated(&w_n_plus_1, Some(4), "drain 4")
            .await;
        assert_eq!(
            queue.get_state(&sched_id).await,
            Some(TaskState::Failed),
            "N + 1 (4th) drain with 0 charged budget must end in Failed"
        );

        // 3. A Running task with max_retries(0) (per-task or policy-level) is never run again:
        //    the very first drain transitions it to Failed.
        let run_once_id = queue
            .submit(Task::new(
                TaskSpec::command("echo", vec!["run-once".into()]),
                TaskRequirements::generic(1, 10).with_max_retries(0),
            ))
            .await
            .unwrap();
        let w_once = Uuid::new_v4();
        queue
            .schedule_tasks_batch_in_sessions(&[(run_once_id, w_once, 1)])
            .await;
        queue
            .mark_running_from(w_once, Some(1), &run_once_id)
            .await
            .unwrap();
        queue
            .handle_session_evacuated(&w_once, Some(1), "FAST_PRE_SLEEP_BATTERY_LOW")
            .await;
        let once_info = queue.get_task(&run_once_id).await.unwrap();
        assert_eq!(
            once_info.state,
            TaskState::Failed,
            "Running task with max_retries(0) must not be run again on drain"
        );
        assert_eq!(once_info.evacuation_count, 0);
        assert_eq!(once_info.retry_count, 0);

        let policy_zero_queue = TaskQueue::with_config(
            RetryPolicy {
                max_retries: 0,
                max_evacuations: 10,
                ..Default::default()
            },
            None,
        );
        let policy_zero_id = policy_zero_queue
            .submit(echo_task("policy-zero-once"))
            .await
            .unwrap();
        let w_pz = Uuid::new_v4();
        policy_zero_queue
            .schedule_tasks_batch_in_sessions(&[(policy_zero_id, w_pz, 1)])
            .await;
        policy_zero_queue
            .mark_running_from(w_pz, Some(1), &policy_zero_id)
            .await
            .unwrap();
        policy_zero_queue
            .handle_session_evacuated(&w_pz, Some(1), "Graceful worker shutdown")
            .await;
        assert_eq!(
            policy_zero_queue.get_state(&policy_zero_id).await,
            Some(TaskState::Failed),
            "Running task under policy max_retries = 0 must fail on first drain"
        );
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
        let queue = TaskQueue::with_retention_limits(
            RetryPolicy::default(),
            None,
            10_000,
            5 * MIB / 2,
            DEFAULT_MAX_ACTIVE_BYTES,
        );
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
        let queue = TaskQueue::with_retention_limits(
            RetryPolicy::default(),
            None,
            10_000,
            MIB / 4,
            DEFAULT_MAX_ACTIVE_BYTES,
        );
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
    async fn test_asset_bytes_are_charged_to_terminal_retention_budget() {
        const MIB: usize = 1024 * 1024;
        let queue = TaskQueue::with_retention_limits(
            RetryPolicy::default(),
            None,
            10_000,
            5 * MIB / 2,
            DEFAULT_MAX_ACTIVE_BYTES,
        );
        let worker = Uuid::new_v4();
        let mut ids = Vec::new();
        for i in 0..4 {
            let id = submit_and_assign(&queue, echo_task(&format!("asset-{i}")), worker, 1).await;
            let mut result = TaskResult::success(worker, id, "", 1, false);
            result.asset_bytes = Some(rusty_grid_core::task::Bytes::from(vec![0u8; MIB]));
            queue
                .record_result_from(worker, Some(1), result)
                .await
                .unwrap();
            ids.push(id);
        }

        assert!(
            queue.get_task(&ids[0]).await.is_none(),
            "oldest task with 1 MiB asset_bytes must be evicted once the 2.5 MiB budget is exceeded"
        );
        assert!(
            queue.get_task(&ids[1]).await.is_none(),
            "second oldest task with 1 MiB asset_bytes must also be evicted"
        );
        assert!(queue.get_task(&ids[2]).await.is_some());
        assert!(queue.get_task(&ids[3]).await.is_some());
        assert!(queue.terminal_retained_bytes().await >= 2 * MIB);
        assert!(queue.terminal_retained_bytes().await <= 5 * MIB / 2);
    }

    #[tokio::test]
    async fn test_active_task_byte_cap_bounds_non_terminal_tasks_and_releases_on_completion() {
        const MIB: usize = 1024 * 1024;
        let queue = TaskQueue::with_retention_limits(
            RetryPolicy::default(),
            None,
            DEFAULT_MAX_TERMINAL_TASKS,
            DEFAULT_MAX_TERMINAL_BYTES,
            5 * MIB / 2,
        );
        let worker = Uuid::new_v4();

        let make_heavy_task = |label: &str, bytes: usize| {
            Task::new(
                TaskSpec::Command {
                    program: "cat".into(),
                    args: vec![label.to_string()],
                    env: Default::default(),
                    working_dir: None,
                    stdin: Some(vec![b'a'; bytes]),
                },
                TaskRequirements::generic(1, 10),
            )
        };

        let id1 = queue.submit(make_heavy_task("t1", MIB)).await.unwrap();
        let id2 = queue.submit(make_heavy_task("t2", MIB)).await.unwrap();
        assert!(
            queue.active_retained_bytes().await >= 2 * MIB,
            "active tasks must be charged toward active_retained_bytes"
        );

        let err = queue.submit(make_heavy_task("t3", MIB)).await.unwrap_err();
        assert!(
            matches!(err, GridError::QueueByteLimitExceeded { .. }),
            "third 1 MiB task must be rejected when active byte cap is 2.5 MiB, got {err:?}"
        );

        // Schedule id1 and verify an oversized checkpoint is also rejected by the active byte cap.
        let scheduled = queue
            .schedule_tasks_batch_in_sessions(&[(id1, worker, 1)])
            .await;
        assert_eq!(scheduled.len(), 1);
        let cp_err = queue
            .record_checkpoint_from(
                worker,
                Some(1),
                id1,
                CheckpointData::new(1, rusty_grid_core::task::Bytes::from(vec![0u8; MIB])),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(cp_err, GridError::QueueByteLimitExceeded { .. }),
            "checkpoint exceeding active byte cap must be rejected, got {cp_err:?}"
        );

        // Finishing id1 moves its charge from active to terminal, freeing room for t3.
        queue
            .record_result_from(
                worker,
                Some(1),
                TaskResult::success(worker, id1, "done", 1, false),
            )
            .await
            .unwrap();
        assert!(queue.active_retained_bytes().await < 2 * MIB);

        let id3 = queue
            .submit(make_heavy_task("t3-after-finish", MIB))
            .await
            .expect("room freed after id1 completed");

        // Cancelling id2 and id3 releases the remaining active byte charge completely.
        queue.cancel_task(&id2, None).await.unwrap();
        queue.cancel_task(&id3, None).await.unwrap();
        assert_eq!(queue.active_retained_bytes().await, 0);
    }

    #[tokio::test]
    async fn test_checkpoint_over_active_cap_accepts_equal_or_smaller_and_refuses_larger() {
        // Cap is 3_000 bytes. Submit a task with a 1_000-byte initial checkpoint (~2_037 bytes
        // including entry overhead) so it fits at submission, then push the queue's active total
        // over the 3_000-byte cap via retry_history growth on worker eviction.
        let queue = TaskQueue::with_retention_limits(
            RetryPolicy::default(),
            None,
            DEFAULT_MAX_TERMINAL_TASKS,
            DEFAULT_MAX_TERMINAL_BYTES,
            3_000,
        );
        let worker_1 = Uuid::new_v4();
        let worker_2 = Uuid::new_v4();

        let mut task = echo_task("cp-growth");
        task.update_checkpoint(CheckpointData::new(
            1,
            rusty_grid_core::task::Bytes::from(vec![b'a'; 1_000]),
        ));
        let task_id = queue.submit(task).await.unwrap();
        queue
            .schedule_tasks_batch_in_sessions(&[(task_id, worker_1, 1)])
            .await;

        // Evicting worker_1 with a long reason pushes `active_retained_bytes()` above the 3_000 cap.
        queue
            .handle_session_disconnected(&worker_1, Some(1), &"r".repeat(1_500), true)
            .await;
        assert!(
            queue.active_retained_bytes().await > 3_000,
            "retry_history growth pushed active_retained_bytes ({}) over the 3_000 cap",
            queue.active_retained_bytes().await
        );

        queue
            .schedule_tasks_batch_in_sessions(&[(task_id, worker_2, 2)])
            .await;

        // Equal-size checkpoint (1_000 bytes) does not grow the entry and must be stored.
        let stored_equal = queue
            .record_checkpoint_from(
                worker_2,
                Some(2),
                task_id,
                CheckpointData::new(2, rusty_grid_core::task::Bytes::from(vec![b'b'; 1_000])),
            )
            .await
            .expect("equal-size checkpoint must be accepted even when total is over cap");
        assert!(stored_equal);
        assert_eq!(queue.get_checkpoint(&task_id).await.unwrap().sequence, 2);

        // Smaller checkpoint (600 bytes) reduces the entry and must be stored.
        let stored_smaller = queue
            .record_checkpoint_from(
                worker_2,
                Some(2),
                task_id,
                CheckpointData::new(3, rusty_grid_core::task::Bytes::from(vec![b'c'; 600])),
            )
            .await
            .expect("smaller checkpoint must be accepted even when total is over cap");
        assert!(stored_smaller);
        assert_eq!(queue.get_checkpoint(&task_id).await.unwrap().sequence, 3);

        // Larger checkpoint (700 bytes > 600 bytes while total > 2_000) must be refused.
        let err = queue
            .record_checkpoint_from(
                worker_2,
                Some(2),
                task_id,
                CheckpointData::new(4, rusty_grid_core::task::Bytes::from(vec![b'd'; 700])),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, GridError::QueueByteLimitExceeded { .. }),
            "growing checkpoint over active cap must be refused, got {err:?}"
        );
        assert_eq!(queue.get_checkpoint(&task_id).await.unwrap().sequence, 3);
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

    #[tokio::test]
    async fn test_task_active_bytes_counts_working_dir_target_dir_and_preferred_provider() {
        use rusty_grid_core::task::AutoRigSpec;

        let base_cmd = Task::new(
            TaskSpec::Command {
                program: "echo".into(),
                args: vec![],
                env: Default::default(),
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, 30),
        );
        let with_wdir = Task::new(
            TaskSpec::Command {
                program: "echo".into(),
                args: vec![],
                env: Default::default(),
                working_dir: Some("/custom/working/directory/path".into()),
                stdin: None,
            },
            TaskRequirements::generic(1, 30),
        );
        assert_eq!(
            task_active_bytes(&with_wdir) - task_active_bytes(&base_cmd),
            "/custom/working/directory/path".len()
        );

        let base_compile = Task::new(
            TaskSpec::RustCompilation {
                crate_name: "proj".into(),
                source_files: Default::default(),
                compiler_flags: vec![],
                target_dir: None,
            },
            TaskRequirements::generic(1, 30),
        );
        let with_tdir = Task::new(
            TaskSpec::RustCompilation {
                crate_name: "proj".into(),
                source_files: Default::default(),
                compiler_flags: vec![],
                target_dir: Some("/tmp/custom-target-dir".into()),
            },
            TaskRequirements::generic(1, 30),
        );
        assert_eq!(
            task_active_bytes(&with_tdir) - task_active_bytes(&base_compile),
            "/tmp/custom-target-dir".len()
        );

        let base_rig = Task::new(
            TaskSpec::AutoRig(AutoRigSpec {
                input_mesh: "mesh.glb".into(),
                rig_preset: "humanoid".into(),
                embed_animations: vec![],
                preferred_provider: None,
            }),
            TaskRequirements::generic(1, 30),
        );
        let with_prov = Task::new(
            TaskSpec::AutoRig(AutoRigSpec {
                input_mesh: "mesh.glb".into(),
                rig_preset: "humanoid".into(),
                embed_animations: vec![],
                preferred_provider: Some("rig_net_provider".into()),
            }),
            TaskRequirements::generic(1, 30),
        );
        assert_eq!(
            task_active_bytes(&with_prov) - task_active_bytes(&base_rig),
            "rig_net_provider".len()
        );

        let queue = TaskQueue::with_retention_limits(
            RetryPolicy::default(),
            None,
            DEFAULT_MAX_TERMINAL_TASKS,
            DEFAULT_MAX_TERMINAL_BYTES,
            2_000,
        );
        let expected_bytes = task_active_bytes(&with_wdir);
        queue.submit(with_wdir).await.unwrap();
        let stats = queue.stats().await;
        assert_eq!(stats.active_bytes, expected_bytes);
        assert_eq!(stats.max_active_bytes, 2_000);
    }

    #[tokio::test]
    async fn test_three_d_tasks_require_three_d_protocol_capability_for_placement() {
        use crate::registry::{WorkerInfo, WorkerStatus};
        use crate::scheduler::{ScheduleSkipReason, SchedulerConfig, WorkloadScheduler};
        use rusty_grid_core::capabilities::WorkerCapabilities;
        use rusty_grid_core::task::{AutoRigSpec, Generative3DSpec};

        let gen3d_task = Task::new(
            TaskSpec::Generative3D(
                Generative3DSpec::new_prompt("obsidian crystal").with_quality("fast"),
            ),
            TaskRequirements::generic(1, 30),
        );
        let autorig_task = Task::new(
            TaskSpec::AutoRig(AutoRigSpec::new(vec![1, 2, 3, 4], "biped")),
            TaskRequirements::generic(1, 30),
        );

        assert!(
            TaskView::of(&gen3d_task)
                .required_features
                .contains(ProtocolCapabilities::THREE_D),
            "Generative3D TaskView must require ProtocolCapabilities::THREE_D"
        );
        assert!(
            TaskView::of(&autorig_task)
                .required_features
                .contains(ProtocolCapabilities::THREE_D),
            "AutoRig TaskView must require ProtocolCapabilities::THREE_D"
        );

        let mut features_without_3d = ProtocolCapabilities::all_supported();
        features_without_3d.remove(ProtocolCapabilities::THREE_D);

        let worker_no_3d = WorkerInfo {
            worker_id: Uuid::new_v4(),
            session_id: 1,
            capabilities: WorkerCapabilities::new("worker-no-3d", 4, 8192, true, true, None),
            features: features_without_3d,
            status: WorkerStatus::Connected,
            last_heartbeat_timestamp: 1000,
            registered_timestamp: 1000,
            active_tasks: 0,
            remote_addr: "127.0.0.1:9000".into(),
            cpu_usage_pct: 0.0,
            ram_available_mb: 8192,
            mobile_status: None,
        };

        let tasks = vec![gen3d_task.clone(), autorig_task.clone()];
        let gen3d_id = gen3d_task.id;
        let autorig_id = autorig_task.id;

        // 1. Default scheduler: both 3D tasks wait with ClusterLacksCapability("... THREE_D ...").
        let default_cfg = SchedulerConfig::default();
        let report =
            WorkloadScheduler::matchmake(&default_cfg, &tasks, std::slice::from_ref(&worker_no_3d));
        assert!(
            report.assignments.is_empty(),
            "3D tasks must not be placed on a worker without THREE_D"
        );
        assert_eq!(report.skipped.len(), 2);
        for (_, reason) in &report.skipped {
            match reason {
                ScheduleSkipReason::ClusterLacksCapability(msg) => {
                    assert!(
                        msg.contains("THREE_D"),
                        "expected THREE_D capability skip reason, got: {msg}"
                    );
                }
                other => panic!("expected ClusterLacksCapability, got {other:?}"),
            }
        }

        // 2. With --fail-unfulfillable-tasks: both 3D tasks are reported unfulfillable.
        let strict_cfg = SchedulerConfig {
            fail_unfulfillable_tasks: true,
            ..SchedulerConfig::default()
        };
        let strict_report =
            WorkloadScheduler::matchmake(&strict_cfg, &tasks, std::slice::from_ref(&worker_no_3d));
        assert!(strict_report.assignments.is_empty());
        assert_eq!(strict_report.unfulfillable.len(), 2);
        for (_, msg) in &strict_report.unfulfillable {
            assert!(
                msg.contains("THREE_D"),
                "expected THREE_D in unfulfillable error, got: {msg}"
            );
        }

        // 3. Once a worker advertising ProtocolCapabilities::THREE_D registers, both are placed on it.
        let mut worker_with_3d = worker_no_3d.clone();
        worker_with_3d.worker_id = Uuid::new_v4();
        worker_with_3d.capabilities.name = "worker-with-3d".into();
        worker_with_3d.features = ProtocolCapabilities::all_supported();
        let workers = vec![worker_no_3d, worker_with_3d.clone()];

        let placed_report = WorkloadScheduler::matchmake(&default_cfg, &tasks, &workers);
        assert_eq!(placed_report.assignments.len(), 2);
        assert_eq!(placed_report.assignments[0].task_id, gen3d_id);
        assert_eq!(
            placed_report.assignments[0].worker_id,
            worker_with_3d.worker_id
        );
        assert_eq!(placed_report.assignments[1].task_id, autorig_id);
        assert_eq!(
            placed_report.assignments[1].worker_id,
            worker_with_3d.worker_id
        );
    }

    #[tokio::test]
    async fn test_attach_store_and_replay_recovers_all_d2_5_states_and_preserves_counters() {
        let dir = tempfile::tempdir().unwrap();
        let worker_id = Uuid::new_v4();

        let (
            submitted_id,
            queued_id,
            scheduled_id,
            running_retryable_id,
            running_zero_retry_id,
            retrying_id,
            completed_id,
            failed_id,
            cancelled_id,
        ) = {
            let (store, recovered) = TaskStore::open(dir.path()).unwrap();
            let queue = TaskQueue::with_config(
                RetryPolicy {
                    retry_on_execution_failure: true,
                    ..RetryPolicy::default()
                },
                None,
            );
            queue
                .attach_store_and_replay(store, recovered, Duration::ZERO)
                .await
                .unwrap();

            let submitted_task = echo_task("submitted");
            let submitted_id = submitted_task.id;

            let queued_id = queue.submit(echo_task("queued")).await.unwrap();

            let scheduled_id = queue.submit(echo_task("scheduled")).await.unwrap();
            queue
                .schedule_tasks_batch_in_sessions(&[(scheduled_id, worker_id, 1)])
                .await;

            let running_retryable_id = queue.submit(echo_task("running-retryable")).await.unwrap();
            queue
                .schedule_tasks_batch_in_sessions(&[(running_retryable_id, worker_id, 1)])
                .await;
            queue
                .mark_running_from(worker_id, Some(1), &running_retryable_id)
                .await
                .unwrap();
            queue
                .record_checkpoint_from(
                    worker_id,
                    Some(1),
                    running_retryable_id,
                    CheckpointData::new(3, vec![9, 8, 7]),
                )
                .await
                .unwrap();

            let mut zero_retry_task = echo_task("running-zero-retry");
            zero_retry_task.requirements.max_retries = Some(0);
            let running_zero_retry_id = queue.submit(zero_retry_task).await.unwrap();
            queue
                .schedule_tasks_batch_in_sessions(&[(running_zero_retry_id, worker_id, 1)])
                .await;
            queue
                .mark_running_from(worker_id, Some(1), &running_zero_retry_id)
                .await
                .unwrap();

            let retrying_id = queue.submit(echo_task("retrying")).await.unwrap();
            queue
                .schedule_tasks_batch_in_sessions(&[(retrying_id, worker_id, 1)])
                .await;
            queue
                .mark_running_from(worker_id, Some(1), &retrying_id)
                .await
                .unwrap();
            let retry_outcome = queue
                .record_result_from(
                    worker_id,
                    Some(1),
                    TaskResult::failure(worker_id, retrying_id, 2, "", "transient", 10, None),
                )
                .await
                .unwrap();
            assert_eq!(retry_outcome.state, TaskState::Retrying);

            let completed_id = queue.submit(echo_task("completed")).await.unwrap();
            queue
                .schedule_tasks_batch_in_sessions(&[(completed_id, worker_id, 1)])
                .await;
            queue
                .record_result_from(
                    worker_id,
                    Some(1),
                    TaskResult::success(worker_id, completed_id, "done-out", 5, false),
                )
                .await
                .unwrap();

            let other_worker = Uuid::new_v4();
            let mut fail_once_task = echo_task("failed");
            fail_once_task.requirements.max_retries = Some(0);
            let failed_id = queue.submit(fail_once_task).await.unwrap();
            queue
                .schedule_tasks_batch_in_sessions(&[(failed_id, other_worker, 99)])
                .await;
            queue
                .mark_running_from(other_worker, Some(99), &failed_id)
                .await
                .unwrap();
            queue
                .handle_session_disconnected(&other_worker, Some(99), "boom", false)
                .await;

            // Inject a snapshot entry with `TaskState::Submitted` to cover all 9 D2.5 states.
            {
                let mut inner = queue.inner.write().await;
                let seq = inner.next_sequence;
                inner.next_sequence += 1;
                inner.tasks.insert(
                    submitted_id,
                    TaskEntry {
                        task: submitted_task,
                        priority: 0,
                        sequence: seq,
                        state: TaskState::Submitted,
                        assigned_worker_id: None,
                        submitted_at_utc: 100,
                        submitted_instant: Instant::now(),
                        scheduled_instant: None,
                        started_instant: None,
                        retry_after_instant: None,
                        retry_count: 0,
                        failover_count: 0,
                        evacuation_count: 0,
                        rejection_count: 0,
                        avoid_workers: Vec::new(),
                        assigned_session: None,
                        adoptable_by: None,
                        fail_if_not_adopted: false,
                        pending_failover_charge: false,
                        pending_failover_reason: None,
                        retry_history: Vec::new(),
                        result: None,
                        error_message: None,
                    },
                );
            }

            let cancelled_id = queue.submit(echo_task("cancelled")).await.unwrap();
            queue
                .cancel_task(&cancelled_id, Some("user abort".into()))
                .await
                .unwrap();

            queue.close_and_compact_store().await;

            (
                submitted_id,
                queued_id,
                scheduled_id,
                running_retryable_id,
                running_zero_retry_id,
                retrying_id,
                completed_id,
                failed_id,
                cancelled_id,
            )
        };

        let (store2, recovered2) = TaskStore::open(dir.path()).unwrap();
        let recovered_queue = TaskQueue::new();
        recovered_queue
            .attach_store_and_replay(store2, recovered2, Duration::ZERO)
            .await
            .unwrap();

        assert_eq!(
            recovered_queue.get_state(&submitted_id).await,
            Some(TaskState::Queued)
        );
        assert_eq!(
            recovered_queue.get_state(&queued_id).await,
            Some(TaskState::Queued)
        );

        let sched_info = recovered_queue.get_task(&scheduled_id).await.unwrap();
        assert_eq!(sched_info.state, TaskState::Queued);
        assert_eq!(sched_info.retry_count, 0);
        assert_eq!(sched_info.failover_count, 0);
        assert_eq!(sched_info.evacuation_count, 0);

        let run_info = recovered_queue
            .get_task(&running_retryable_id)
            .await
            .unwrap();
        assert_eq!(run_info.state, TaskState::Queued);
        assert_eq!(run_info.retry_count, 0);
        assert_eq!(run_info.failover_count, 0);
        assert_eq!(run_info.evacuation_count, 0);
        assert_eq!(
            recovered_queue
                .get_checkpoint(&running_retryable_id)
                .await
                .unwrap()
                .sequence,
            3
        );

        let zero_info = recovered_queue
            .get_task(&running_zero_retry_id)
            .await
            .unwrap();
        assert_eq!(zero_info.state, TaskState::Failed);
        assert_eq!(
            zero_info.error_message.as_deref(),
            Some("master restarted while the task ran; not re-run because max_retries is 0")
        );

        let retry_info = recovered_queue.get_task(&retrying_id).await.unwrap();
        assert_eq!(retry_info.state, TaskState::Queued);
        assert_eq!(retry_info.retry_count, 1);

        assert_eq!(
            recovered_queue.get_state(&completed_id).await,
            Some(TaskState::Completed)
        );
        assert_eq!(
            recovered_queue
                .get_result(&completed_id)
                .await
                .unwrap()
                .stdout
                .as_str(),
            "done-out"
        );
        assert_eq!(
            recovered_queue.get_state(&failed_id).await,
            Some(TaskState::Failed)
        );
        assert_eq!(
            recovered_queue.get_state(&cancelled_id).await,
            Some(TaskState::Cancelled)
        );

        recovered_queue.close_and_compact_store().await;
    }

    #[tokio::test]
    async fn test_attach_store_and_replay_adoptable_by_hold_and_adopt_session_tasks() {
        let dir = tempfile::tempdir().unwrap();
        let worker_id = Uuid::new_v4();

        let (adopted_id, omitted_zero_retry_id, omitted_scheduled_id) = {
            let (store, recovered) = TaskStore::open(dir.path()).unwrap();
            let queue = TaskQueue::with_config(
                RetryPolicy {
                    retry_on_execution_failure: true,
                    ..RetryPolicy::default()
                },
                None,
            );
            queue
                .attach_store_and_replay(store, recovered, Duration::ZERO)
                .await
                .unwrap();

            let adopted_id = queue.submit(echo_task("adopted")).await.unwrap();
            let mut zero_task = echo_task("omitted-zero");
            zero_task.requirements.max_retries = Some(0);
            let omitted_zero_retry_id = queue.submit(zero_task).await.unwrap();
            let omitted_scheduled_id = queue.submit(echo_task("omitted-scheduled")).await.unwrap();

            queue
                .schedule_tasks_batch_in_sessions(&[
                    (adopted_id, worker_id, 1),
                    (omitted_zero_retry_id, worker_id, 1),
                    (omitted_scheduled_id, worker_id, 1),
                ])
                .await;
            queue
                .mark_running_from(worker_id, Some(1), &adopted_id)
                .await
                .unwrap();
            queue
                .mark_running_from(worker_id, Some(1), &omitted_zero_retry_id)
                .await
                .unwrap();

            queue.close_and_compact_store().await;
            (adopted_id, omitted_zero_retry_id, omitted_scheduled_id)
        };

        let (store2, recovered2) = TaskStore::open(dir.path()).unwrap();
        let queue2 = TaskQueue::new();
        queue2
            .attach_store_and_replay(store2, recovered2, Duration::from_secs(30))
            .await
            .unwrap();

        // While held in `adoptable_by`, none of the 3 tasks is offered to a normal scheduling pass.
        assert!(queue2.get_schedulable_tasks().await.is_empty());
        assert!(queue2.ready_views(None, 10).await.0.is_empty());

        let stale_unknown = TaskId::new();
        let outcome = queue2
            .adopt_session_tasks(worker_id, 7, &[adopted_id, stale_unknown])
            .await;

        assert_eq!(outcome.adopted, vec![adopted_id]);
        assert_eq!(outcome.stale_to_cancel, vec![stale_unknown]);
        assert_eq!(outcome.newly_failed, vec![omitted_zero_retry_id]);

        assert_eq!(
            queue2.get_state(&adopted_id).await,
            Some(TaskState::Running)
        );
        let zero_info = queue2.get_task(&omitted_zero_retry_id).await.unwrap();
        assert_eq!(zero_info.state, TaskState::Failed);
        assert_eq!(
            zero_info.error_message.as_deref(),
            Some("master restarted while the task ran; not re-run because max_retries is 0")
        );

        // `omitted_scheduled_id` had its hold released when `worker_id` resumed without it.
        let schedulable = queue2.get_schedulable_tasks().await;
        assert_eq!(schedulable.len(), 1);
        assert_eq!(schedulable[0].id, omitted_scheduled_id);

        queue2.close_and_compact_store().await;
    }

    #[tokio::test]
    async fn test_release_expired_adoption_holds_fails_zero_retry_and_frees_retryable() {
        let dir = tempfile::tempdir().unwrap();
        let worker_id = Uuid::new_v4();

        let (retryable_id, zero_retry_id) = {
            let (store, recovered) = TaskStore::open(dir.path()).unwrap();
            let queue = TaskQueue::with_config(
                RetryPolicy {
                    retry_on_execution_failure: true,
                    ..RetryPolicy::default()
                },
                None,
            );
            queue
                .attach_store_and_replay(store, recovered, Duration::ZERO)
                .await
                .unwrap();

            let retryable_id = queue.submit(echo_task("retryable")).await.unwrap();
            let mut zero_task = echo_task("zero");
            zero_task.requirements.max_retries = Some(0);
            let zero_retry_id = queue.submit(zero_task).await.unwrap();

            queue
                .schedule_tasks_batch_in_sessions(&[
                    (retryable_id, worker_id, 1),
                    (zero_retry_id, worker_id, 1),
                ])
                .await;
            queue
                .mark_running_from(worker_id, Some(1), &retryable_id)
                .await
                .unwrap();
            queue
                .mark_running_from(worker_id, Some(1), &zero_retry_id)
                .await
                .unwrap();

            queue.close_and_compact_store().await;
            (retryable_id, zero_retry_id)
        };

        let (store2, recovered2) = TaskStore::open(dir.path()).unwrap();
        let queue2 = TaskQueue::new();
        queue2
            .attach_store_and_replay(store2, recovered2, Duration::from_millis(20))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(35)).await;
        let newly_failed = queue2.release_expired_adoption_holds().await;
        assert_eq!(newly_failed, vec![zero_retry_id]);

        let zero_info = queue2.get_task(&zero_retry_id).await.unwrap();
        assert_eq!(zero_info.state, TaskState::Failed);
        assert_eq!(
            zero_info.error_message.as_deref(),
            Some("master restarted while the task ran; not re-run because max_retries is 0")
        );

        let retryable_info = queue2.get_task(&retryable_id).await.unwrap();
        assert_eq!(retryable_info.state, TaskState::Queued);
        assert_eq!(retryable_info.retry_count, 0);
        assert_eq!(retryable_info.failover_count, 0);
        assert_eq!(retryable_info.evacuation_count, 0);
        assert_eq!(queue2.get_schedulable_tasks().await.len(), 1);

        queue2.close_and_compact_store().await;
    }

    #[derive(Debug, Clone, PartialEq)]
    struct ReplaySnap {
        state: TaskState,
        retry: u32,
        failover: u32,
        evac: u32,
        history: usize,
        has_result: bool,
        err: Option<String>,
        ckpt: Option<u64>,
    }

    async fn replay_snap(q: &TaskQueue, id: &TaskId) -> Option<ReplaySnap> {
        let info = q.get_task(id).await?;
        Some(ReplaySnap {
            state: info.state,
            retry: info.retry_count,
            failover: info.failover_count,
            evac: info.evacuation_count,
            history: q.get_retry_history(id).await.map_or(0, |h| h.len()),
            has_result: q.get_result(id).await.is_some(),
            err: info.error_message,
            ckpt: q.get_checkpoint(id).await.map(|c| c.sequence),
        })
    }

    fn replay_policy() -> RetryPolicy {
        RetryPolicy {
            retry_on_execution_failure: true,
            initial_backoff_ms: 60_000,
            ..RetryPolicy::default()
        }
    }

    async fn drive_all_transitions(q: &TaskQueue) -> Vec<(TaskId, u32)> {
        let w = Uuid::new_v4();
        let w2 = Uuid::new_v4();
        let mut ids = Vec::new();
        for i in 0..14u32 {
            let prio = [0, 5, 1, 5, 0, 2, 0, 0, 3, 0, 1, 0, 0, 4][i as usize];
            let id = q
                .submit_with_priority(echo_task(&format!("t{i}")), prio)
                .await
                .unwrap();
            ids.push((id, prio));
        }
        let v: Vec<TaskId> = ids.iter().map(|(t, _)| *t).collect();
        let id = |i: usize| v[i];
        // Finish a few early tasks so retention eviction happens inside later sweeps.
        for i in [10usize, 11, 12] {
            q.schedule_tasks_batch_in_sessions(&[(id(i), w, 1)]).await;
            q.record_result_from(w, Some(1), TaskResult::success(w, id(i), "early", 1, false))
                .await
                .unwrap();
        }
        q.schedule_tasks_batch_in_sessions(&(1..=6).map(|i| (id(i), w, 1)).collect::<Vec<_>>())
            .await;
        for i in 2..=6 {
            q.mark_running_from(w, Some(1), &id(i)).await.unwrap();
        }
        q.record_checkpoint_from(w, Some(1), id(2), CheckpointData::new(3, vec![1, 2, 3]))
            .await
            .unwrap();
        q.record_result_from(w, Some(1), TaskResult::success(w, id(3), "ok", 5, false))
            .await
            .unwrap();
        // Execution failure -> Retrying (long backoff).
        q.record_result_from(
            w,
            Some(1),
            TaskResult::failure(w, id(4), 2, "", "x", 10, None),
        )
        .await
        .unwrap();
        // Worker rejection -> Queued, rejection_count 1.
        q.record_result_from(
            w,
            Some(1),
            TaskResult::failure(
                w,
                id(5),
                1,
                "",
                "",
                0,
                Some("capabilities insufficient".into()),
            ),
        )
        .await
        .unwrap();
        q.cancel_task(&id(6), Some("user abort running".into()))
            .await
            .unwrap();
        q.cancel_task(&id(7), Some("user abort queued".into()))
            .await
            .unwrap();
        q.fail_unfulfillable_task(&id(8), "no worker can".into())
            .await;
        // Tighten retention so the next sweep evicts while it runs.
        q.set_max_terminal_tasks(3).await;
        // Evacuation on w2.
        q.schedule_tasks_batch_in_sessions(&[(id(9), w2, 2)]).await;
        q.mark_running_from(w2, Some(2), &id(9)).await.unwrap();
        q.handle_session_evacuated(&w2, Some(2), "drain").await;
        // Disconnect of w: t1 Scheduled, t2 Running with checkpoint -> charged failover.
        let mut zero = echo_task("zero");
        zero.requirements.max_retries = Some(0);
        let zid = q.submit(zero).await.unwrap();
        ids.push((zid, 0));
        q.schedule_tasks_batch_in_sessions(&[(zid, w, 1)]).await;
        q.mark_running_from(w, Some(1), &zid).await.unwrap();
        q.handle_session_disconnected(&w, Some(1), "boom", true)
            .await;
        // Leave one Scheduled and one Running (with retry budget > 0) for replay.
        q.schedule_tasks_batch_in_sessions(&[(id(0), w2, 3), (id(13), w2, 3)])
            .await;
        q.mark_running_from(w2, Some(3), &id(13)).await.unwrap();
        ids
    }

    fn expected_replayed_state(live: TaskState) -> TaskState {
        match live {
            TaskState::Scheduled
            | TaskState::Running
            | TaskState::Retrying
            | TaskState::Submitted => TaskState::Queued,
            s => s,
        }
    }

    async fn compare_replay_against_live(
        label: &str,
        opts: crate::store::StoreOptions,
    ) -> Vec<String> {
        compare_replay_against_live_with_retention(label, opts, DEFAULT_MAX_TERMINAL_TASKS).await
    }

    async fn compare_replay_against_live_with_retention(
        label: &str,
        opts: crate::store::StoreOptions,
        retention: usize,
    ) -> Vec<String> {
        let dir = tempfile::tempdir().unwrap();
        let (live, ids) = {
            let (store, rec) = TaskStore::open_with(dir.path(), opts.clone()).unwrap();
            let q = TaskQueue::with_retention(replay_policy(), None, retention);
            q.attach_store_and_replay(store, rec, Duration::ZERO)
                .await
                .unwrap();
            let ids = drive_all_transitions(&q).await;
            let mut live = Vec::new();
            for (id, _) in &ids {
                live.push(replay_snap(&q, id).await);
            }
            // Crash-equivalent: drop without calling close_and_compact_store (writer flushes queued records).
            drop(q);
            (live, ids)
        };
        let (store, rec) = TaskStore::open_with(dir.path(), opts).unwrap();
        if label == "journal-only" || label == "evict-journal-only" {
            assert!(
                rec.snapshot.is_none(),
                "journal-only replay must have no snapshot.bin"
            );
            assert!(
                rec.records.len() > 15,
                "journal-only replay must recover > 15 journal records, got {}",
                rec.records.len()
            );
        }
        let q2 = TaskQueue::with_retention(replay_policy(), None, retention);
        q2.attach_store_and_replay(store, rec, Duration::ZERO)
            .await
            .unwrap();
        let mut diffs = Vec::new();
        let mut expect_ready = Vec::new();
        for (i, ((id, prio), l)) in ids.iter().zip(live.iter()).enumerate() {
            let got = replay_snap(&q2, id).await;
            let want = l.clone().map(|mut s| {
                s.state = expected_replayed_state(s.state);
                s
            });
            if let Some(ref w) = want {
                if w.state == TaskState::Queued {
                    expect_ready.push((std::cmp::Reverse(*prio), i, *id));
                }
            }
            if got != want {
                diffs.push(format!(
                    "[{label}] t{i}: live->expected {want:?}\n            replayed {got:?}"
                ));
            }
        }
        expect_ready.sort_by_key(|(p, i, _)| (*p, *i));
        let expect_ready: Vec<TaskId> = expect_ready.into_iter().map(|(_, _, id)| id).collect();
        let ready = q2.ready_task_ids(100).await;
        if ready != expect_ready {
            diffs.push(format!(
                "[{label}] ready order differs:\n want {expect_ready:?}\n got  {ready:?}"
            ));
        }
        // next_sequence restored: a new submission sorts after every recovered one of equal priority.
        let fresh = q2.submit(echo_task("fresh")).await.unwrap();
        let r = q2.ready_task_ids(100).await;
        if r.last() != Some(&fresh) {
            diffs.push(format!("[{label}] new task not last: {r:?}"));
        }
        diffs
    }

    #[tokio::test]
    async fn test_journal_replay_without_compaction_matches_live() {
        let diffs =
            compare_replay_against_live("journal-only", crate::store::StoreOptions::default())
                .await;
        assert!(
            diffs.is_empty(),
            "replay differences:\n{}",
            diffs.join("\n")
        );
    }

    #[tokio::test]
    async fn test_replay_with_frequent_compaction_matches_live() {
        let mut all = Vec::new();
        for n in [1u64, 2, 3, 5, 8, 13] {
            let opts = crate::store::StoreOptions {
                compact_max_records: n,
                ..crate::store::StoreOptions::default()
            };
            all.extend(compare_replay_against_live(&format!("compact@{n}"), opts).await);
        }
        assert!(
            all.is_empty(),
            "frequent compaction replay differences:\n{}",
            all.join("\n")
        );
    }

    #[tokio::test]
    async fn test_scheduled_zero_retry_task_fails_on_restart_and_persists_fail_if_not_adopted_across_compaction(
    ) {
        let dir = tempfile::tempdir().unwrap();
        let worker_id = Uuid::new_v4();

        // 1. Crash while a max_retries == 0 task is in `Scheduled` (before `mark_running_from`),
        //    with no snapshot compaction (journal-only).
        let scheduled_zero_id = {
            let (store, recovered) = TaskStore::open(dir.path()).unwrap();
            let queue = TaskQueue::with_config(replay_policy(), None);
            queue
                .attach_store_and_replay(store, recovered, Duration::ZERO)
                .await
                .unwrap();

            let mut zero_task = echo_task("scheduled-zero");
            zero_task.requirements.max_retries = Some(0);
            let tid = queue.submit(zero_task).await.unwrap();
            queue
                .schedule_tasks_batch_in_sessions(&[(tid, worker_id, 1)])
                .await;
            assert_eq!(queue.get_state(&tid).await, Some(TaskState::Scheduled));
            drop(queue);
            tid
        };

        // Reopen with a 30s adoption hold, then trigger snapshot compaction while held, and drop.
        {
            let opts = crate::store::StoreOptions {
                compact_max_records: 1,
                ..crate::store::StoreOptions::default()
            };
            let (store2, recovered2) = TaskStore::open_with(dir.path(), opts).unwrap();
            let queue2 = TaskQueue::with_config(replay_policy(), None);
            queue2
                .attach_store_and_replay(store2, recovered2, Duration::from_secs(30))
                .await
                .unwrap();

            // Must be held for adoption, not offered to the normal scheduler.
            assert!(queue2.get_schedulable_tasks().await.is_empty());

            // Submit another task to trigger snapshot compaction while `scheduled_zero_id` is held.
            let _other = queue2.submit(echo_task("trigger-compact")).await.unwrap();
            queue2.close_and_compact_store().await;
        }

        // Reopen after compaction with a 20ms hold; when the hold expires, the task must fail
        // rather than re-running because `fail_if_not_adopted` and `adoptable_by_worker` were persisted.
        let (store3, recovered3) = TaskStore::open(dir.path()).unwrap();
        assert!(recovered3.snapshot.is_some());
        let queue3 = TaskQueue::with_config(replay_policy(), None);
        queue3
            .attach_store_and_replay(store3, recovered3, Duration::from_millis(20))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(35)).await;
        let newly_failed = queue3.release_expired_adoption_holds().await;
        assert_eq!(newly_failed, vec![scheduled_zero_id]);
        let info = queue3.get_task(&scheduled_zero_id).await.unwrap();
        assert_eq!(info.state, TaskState::Failed);
        assert_eq!(
            info.error_message.as_deref(),
            Some("master restarted while the task ran; not re-run because max_retries is 0")
        );
        queue3.close_and_compact_store().await;
    }

    #[tokio::test]
    async fn test_storage_failure_rolls_back_submit_and_preserves_in_flight_result_transition() {
        let dir = tempfile::tempdir().unwrap();
        let (store, rec) = TaskStore::open(dir.path()).unwrap();
        let q = TaskQueue::with_config(replay_policy(), None);
        q.attach_store_and_replay(store, rec, Duration::ZERO)
            .await
            .unwrap();

        let ok_id = q.submit(echo_task("before")).await.unwrap();
        let w = Uuid::new_v4();
        q.schedule_tasks_batch_in_sessions(&[(ok_id, w, 1)]).await;
        let baseline_active_bytes = q.active_retained_bytes().await;

        // Inject sync_data failure on the next batch.
        q.inject_store_sync_failure(1).await;

        let t = echo_task("during-failure");
        let tid = t.id;
        let res = q.submit(t).await;
        assert!(
            matches!(res, Err(GridError::Storage(_))),
            "expected Err(GridError::Storage), got {res:?}"
        );
        assert_eq!(
            q.get_state(&tid).await,
            None,
            "failed submission must be rolled back from tasks table"
        );
        assert!(
            !q.ready_task_ids(10).await.contains(&tid),
            "failed submission must not be in ready_queue"
        );
        assert_eq!(
            q.active_retained_bytes().await,
            baseline_active_bytes,
            "failed submission must not leak active_bytes"
        );

        // A second submission while the store is already marked failed is refused before mutating tasks.
        let t2 = echo_task("after-failure");
        let tid2 = t2.id;
        let res2 = q.submit(t2).await;
        assert!(matches!(res2, Err(GridError::Storage(_))));
        assert_eq!(q.get_state(&tid2).await, None);
        assert!(!q.ready_task_ids(10).await.contains(&tid2));

        // Recording a result for an already-admitted task commits in memory and returns Ok
        // so the server can release the worker's slot, unpin blobs, and wake completion waiters.
        let outcome = q
            .record_result_from(w, Some(1), TaskResult::success(w, ok_id, "o", 1, false))
            .await
            .expect("record_result_from must succeed in memory even if store is failed");
        assert_eq!(outcome.disposition, ResultDisposition::Terminal);
        assert_eq!(q.get_state(&ok_id).await, Some(TaskState::Completed));
    }

    #[tokio::test]
    async fn test_release_expired_adoption_holds_fast_path_with_10k_tasks() {
        let q = TaskQueue::with_config(replay_policy(), None);
        let w = Uuid::new_v4();
        let mut ids = Vec::new();
        for i in 0..10_000 {
            ids.push(q.submit(echo_task(&format!("c{i}"))).await.unwrap());
        }
        let assign: Vec<_> = ids.iter().map(|id| (*id, w, 1u64)).collect();
        q.schedule_tasks_batch_in_sessions(&assign).await;
        for id in &ids {
            q.record_result_from(w, Some(1), TaskResult::success(w, *id, "o", 1, false))
                .await
                .unwrap();
        }
        let start = Instant::now();
        for _ in 0..1_000 {
            assert!(q.release_expired_adoption_holds().await.is_empty());
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(50),
            "1,000 calls to release_expired_adoption_holds with 10,000 retained tasks took {elapsed:?} (expected fast-path < 50ms)"
        );
    }

    #[tokio::test]
    async fn test_submit_holds_off_ready_queue_until_durable_ack_resolves() {
        let dir = tempfile::tempdir().unwrap();
        let opts = crate::store::StoreOptions {
            group_commit_window: Duration::from_millis(30),
            ..crate::store::StoreOptions::default()
        };
        let (store, rec) = TaskStore::open_with(dir.path(), opts).unwrap();
        let q = TaskQueue::with_config(replay_policy(), None);
        q.attach_store_and_replay(store, rec, Duration::ZERO)
            .await
            .unwrap();

        let t = echo_task("hold-off-ready");
        let tid = t.id;
        let q_clone = q.clone();
        let submit_fut = tokio::spawn(async move { q_clone.submit(t).await });

        // Wait briefly so `submit` enters the group-commit wait before inserting into `ready_queue`.
        tokio::time::sleep(Duration::from_millis(3)).await;
        assert!(
            !q.ready_task_ids(10).await.contains(&tid),
            "task awaiting DurableAck must not be in ready_queue"
        );
        assert!(
            q.get_schedulable_tasks().await.is_empty(),
            "task awaiting DurableAck must not be schedulable"
        );

        let returned_id = submit_fut.await.unwrap().unwrap();
        assert_eq!(returned_id, tid);
        assert_eq!(q.get_state(&tid).await, Some(TaskState::Queued));
        assert!(q.ready_task_ids(10).await.contains(&tid));
    }

    #[tokio::test]
    async fn rvb_drop_submit_future_mid_ack_loses_task_from_ready_queue() {
        let dir = tempfile::tempdir().unwrap();
        let opts = crate::store::StoreOptions {
            group_commit_window: Duration::from_millis(150),
            ..crate::store::StoreOptions::default()
        };
        let (store, rec) = TaskStore::open_with(dir.path(), opts).unwrap();
        let q = TaskQueue::with_config(replay_policy(), None);
        q.attach_store_and_replay(store, rec, Duration::ZERO)
            .await
            .unwrap();

        let t = echo_task("dropped");
        let id = t.id;
        let q2 = q.clone();
        let h = tokio::spawn(async move { q2.submit(t).await });
        // Abort only once `submit` has really entered its durable-ack wait (the task is in the
        // map; it only reaches `ready_queue` after the commit window). A fixed
        // sleep was a race: on a loaded CI host the spawned submit may not have run at all
        // after 20 ms, and aborting it first leaves no task (state `None`).
        let entered = poll_test_condition(|| async { q.get_state(&id).await.is_some() }).await;
        assert!(entered, "submit never reached its durable-ack wait");
        h.abort();
        let _ = h.await;

        // The detached ack task must still publish the task. Wait for that with a generous
        // bound instead of guessing how long the commit window plus scheduling takes.
        let published =
            poll_test_condition(|| async { q.ready_task_ids(10).await.contains(&id) }).await;
        let st = q.get_state(&id).await;
        assert!(
            published,
            "ZOMBIE: task is {st:?} but not in ready_queue (never scheduled)"
        );
        assert_eq!(st, Some(TaskState::Queued));
    }

    /// Polls `cond` every 5 ms until it holds or 20 s pass (a bound, not an expected delay).
    async fn poll_test_condition<F, Fut>(mut cond: F) -> bool
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            if cond().await {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        false
    }

    #[tokio::test]
    async fn rvb_rollback_cancel_race() {
        // Case 1: cancel while submit awaits its ack and the ack succeeds -> stays Cancelled, not in ready_queue.
        let dir = tempfile::tempdir().unwrap();
        let opts = crate::store::StoreOptions {
            group_commit_window: Duration::from_millis(150),
            ..crate::store::StoreOptions::default()
        };
        let (store, rec) = TaskStore::open_with(dir.path(), opts.clone()).unwrap();
        let q = TaskQueue::with_config(replay_policy(), None);
        q.attach_store_and_replay(store, rec, Duration::ZERO)
            .await
            .unwrap();

        let t = echo_task("cancel-race-ok");
        let id = t.id;
        let q2 = q.clone();
        let h = tokio::spawn(async move { q2.submit(t).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        let c = q.cancel_task(&id, None).await;
        assert!(c.is_ok());
        let r = h.await.unwrap();
        assert!(r.is_ok());
        assert_eq!(q.get_state(&id).await, Some(TaskState::Cancelled));
        assert!(!q.ready_task_ids(10).await.contains(&id));

        // Case 2: cancel while submit awaits its ack and the store sync fails -> rolled back cleanly.
        let dir2 = tempfile::tempdir().unwrap();
        let (store2, rec2) = TaskStore::open_with(dir2.path(), opts).unwrap();
        let q_fail = TaskQueue::with_config(replay_policy(), None);
        q_fail
            .attach_store_and_replay(store2, rec2, Duration::ZERO)
            .await
            .unwrap();
        q_fail.inject_store_sync_failure(1).await;

        let t2 = echo_task("cancel-race-fail");
        let id2 = t2.id;
        let q_fail2 = q_fail.clone();
        let h2 = tokio::spawn(async move { q_fail2.submit(t2).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        let _ = q_fail.cancel_task(&id2, None).await;
        let r2 = h2.await.unwrap();
        assert!(r2.is_err());
        assert_eq!(q_fail.get_state(&id2).await, None);
        assert!(!q_fail.ready_task_ids(10).await.contains(&id2));
        assert_eq!(q_fail.active_retained_bytes().await, 0);
        assert_eq!(q_fail.terminal_retained_bytes().await, 0);
    }

    #[tokio::test]
    async fn rvb_zero_retry_scheduled_after_crash() {
        let dir = tempfile::tempdir().unwrap();
        let (store, rec) = TaskStore::open(dir.path()).unwrap();
        let q = TaskQueue::with_config(replay_policy(), None);
        q.attach_store_and_replay(store, rec, Duration::ZERO)
            .await
            .unwrap();
        let mut t = echo_task("zero");
        t.requirements.max_retries = Some(0);
        let id = t.id;
        q.submit(t).await.unwrap();
        let mut t2 = echo_task("nonzero");
        t2.requirements.max_retries = Some(3);
        let id2 = t2.id;
        q.submit(t2).await.unwrap();
        let w = Uuid::new_v4();
        q.schedule_tasks_batch_in_sessions(&[(id, w, 1), (id2, w, 1)])
            .await;
        drop(q);
        let (store, rec) = TaskStore::open(dir.path()).unwrap();
        let q2 = TaskQueue::with_config(replay_policy(), None);
        q2.attach_store_and_replay(store, rec, Duration::ZERO)
            .await
            .unwrap();
        let a = q2.get_task(&id).await.unwrap();
        let b = q2.get_task(&id2).await.unwrap();
        assert_eq!(a.state, TaskState::Failed);
        assert_eq!(b.state, TaskState::Queued);
    }

    #[tokio::test]
    async fn rvb_compaction_boundaries_many() {
        let mut all = Vec::new();
        for n in 1u64..=24 {
            let opts = crate::store::StoreOptions {
                compact_max_records: n,
                ..crate::store::StoreOptions::default()
            };
            all.extend(compare_replay_against_live(&format!("compact@{n}"), opts).await);
        }
        assert!(
            all.is_empty(),
            "compaction boundary differences:\n{}",
            all.join("\n")
        );
    }

    #[tokio::test]
    async fn rvb_compaction_with_eviction() {
        let mut all = Vec::new();
        for n in 1u64..=20 {
            let opts = crate::store::StoreOptions {
                compact_max_records: n,
                ..crate::store::StoreOptions::default()
            };
            all.extend(
                compare_replay_against_live_with_retention(&format!("evict-compact@{n}"), opts, 3)
                    .await,
            );
        }
        all.extend(
            compare_replay_against_live_with_retention(
                "evict-journal-only",
                crate::store::StoreOptions::default(),
                3,
            )
            .await,
        );
        assert!(
            all.is_empty(),
            "compaction with eviction differences:\n{}",
            all.join("\n")
        );
    }

    #[tokio::test]
    async fn test_session_disconnect_with_session_resume_defers_failover_until_unadopted_or_expired(
    ) {
        let queue = TaskQueue::with_config(
            RetryPolicy {
                max_retries: 2,
                max_failovers: 2,
                retry_on_worker_disconnect: true,
                initial_backoff_ms: 0,
                ..RetryPolicy::default()
            },
            None,
        );
        queue
            .set_session_resume_hold(Duration::from_millis(50))
            .await;

        let worker_id = Uuid::new_v4();
        queue.set_worker_session_resume(worker_id, true);

        let t_adopted = queue.submit(echo_task("adopted")).await.unwrap();
        let t_omitted = queue.submit(echo_task("omitted")).await.unwrap();
        let mut zero_task = echo_task("zero-retry");
        zero_task.requirements.max_retries = Some(0);
        let t_zero = queue.submit(zero_task).await.unwrap();

        queue
            .schedule_tasks_batch_in_sessions(&[
                (t_adopted, worker_id, 1),
                (t_omitted, worker_id, 1),
                (t_zero, worker_id, 1),
            ])
            .await;
        for tid in [t_adopted, t_omitted, t_zero] {
            queue
                .mark_running_from(worker_id, Some(1), &tid)
                .await
                .unwrap();
        }

        // Disconnect session 1: all 3 tasks are held in `Queued` with `adoptable_by` and
        // `pending_failover_charge = true`, with 0 failovers charged yet.
        let affected = queue
            .handle_session_disconnected(&worker_id, Some(1), "Worker dropped", true)
            .await;
        assert_eq!(affected.len(), 3);
        assert!(queue.ready_task_ids(10).await.is_empty());

        for tid in [t_adopted, t_omitted, t_zero] {
            let info = queue.get_task(&tid).await.unwrap();
            assert_eq!(info.state, TaskState::Queued);
            assert_eq!(info.retry_count, 0);
            assert_eq!(info.failover_count, 0);
        }

        // Reconnect as session 2 and adopt only `t_adopted`:
        // - `t_adopted` returns to `Running` with `failover_count == 0`
        // - `t_omitted` is released immediately with its deferred failover charge (`failover_count == 1`)
        // - `t_zero` fails immediately because `max_retries == 0`
        let outcome = queue.adopt_session_tasks(worker_id, 2, &[t_adopted]).await;
        assert_eq!(outcome.adopted, vec![t_adopted]);
        assert!(outcome.stale_to_cancel.is_empty());
        assert_eq!(outcome.newly_failed, vec![t_zero]);

        let adopted_info = queue.get_task(&t_adopted).await.unwrap();
        assert_eq!(adopted_info.state, TaskState::Running);
        assert_eq!(adopted_info.retry_count, 0);
        assert_eq!(adopted_info.failover_count, 0);

        let omitted_info = queue.get_task(&t_omitted).await.unwrap();
        assert_eq!(omitted_info.state, TaskState::Queued);
        assert_eq!(omitted_info.retry_count, 1);
        assert_eq!(omitted_info.failover_count, 1);

        let zero_info = queue.get_task(&t_zero).await.unwrap();
        assert_eq!(zero_info.state, TaskState::Failed);

        // Now disconnect session 2 and let the hold expire for `t_adopted`.
        queue
            .handle_session_disconnected(&worker_id, Some(2), "Second drop", true)
            .await;
        tokio::time::sleep(Duration::from_millis(65)).await;
        let expired_failed = queue.release_expired_adoption_holds().await;
        assert!(expired_failed.is_empty());

        let adopted_after_expiry = queue.get_task(&t_adopted).await.unwrap();
        assert_eq!(adopted_after_expiry.state, TaskState::Queued);
        assert_eq!(adopted_after_expiry.retry_count, 1);
        assert_eq!(adopted_after_expiry.failover_count, 1);

        // Late `ResumeSession` after the hold expired must report `t_adopted` in `stale_to_cancel`.
        let late = queue.adopt_session_tasks(worker_id, 3, &[t_adopted]).await;
        assert!(late.adopted.is_empty());
        assert_eq!(late.stale_to_cancel, vec![t_adopted]);
    }

    #[tokio::test]
    async fn test_workspace_tasks_are_not_placed_on_workers_without_the_workspaces_bit() {
        use crate::registry::{WorkerInfo, WorkerStatus};
        use crate::scheduler::{ScheduleSkipReason, SchedulerConfig, WorkloadScheduler};
        use rusty_grid_core::capabilities::WorkerCapabilities;
        use rusty_grid_core::task::WorkspaceRef;

        let ws_task = Task::new(
            TaskSpec::new_command("cargo", vec!["test".into()]),
            TaskRequirements::generic(1, 30),
        )
        .with_workspace(WorkspaceRef::new("agent-1"));
        let plain_task = Task::new(
            TaskSpec::new_command("echo", vec![]),
            TaskRequirements::generic(1, 30),
        );
        assert!(TaskView::of(&ws_task)
            .required_features
            .contains(ProtocolCapabilities::WORKSPACES));
        assert!(!TaskView::of(&plain_task)
            .required_features
            .contains(ProtocolCapabilities::WORKSPACES));

        let worker = |features| WorkerInfo {
            worker_id: Uuid::new_v4(),
            session_id: 1,
            capabilities: WorkerCapabilities::new("w", 4, 8192, false, false, None),
            features,
            status: WorkerStatus::Connected,
            last_heartbeat_timestamp: 1000,
            registered_timestamp: 1000,
            active_tasks: 0,
            remote_addr: "127.0.0.1:9000".into(),
            cpu_usage_pct: 0.0,
            ram_available_mb: 8192,
            mobile_status: None,
        };
        let cfg = SchedulerConfig::default();
        let tasks = vec![ws_task.clone(), plain_task.clone()];

        // `all_supported()` does not carry the bit: the workspace task waits, the plain one runs.
        let w = worker(ProtocolCapabilities::all_supported());
        let report = WorkloadScheduler::matchmake(&cfg, &tasks, std::slice::from_ref(&w));
        assert_eq!(report.assignments.len(), 1);
        assert_eq!(report.assignments[0].task_id, plain_task.id);
        assert_eq!(report.skipped.len(), 1);
        match &report.skipped[0].1 {
            ScheduleSkipReason::ClusterLacksCapability(msg) => {
                assert!(msg.contains("WORKSPACES"), "{msg}")
            }
            other => panic!("expected ClusterLacksCapability, got {other:?}"),
        }

        // A worker that advertises the bit gets both.
        let mut features = ProtocolCapabilities::all_supported();
        features.insert(ProtocolCapabilities::WORKSPACES);
        let w = worker(features);
        let report = WorkloadScheduler::matchmake(&cfg, &tasks, std::slice::from_ref(&w));
        assert!(report.skipped.is_empty());
        assert!(report.assignments.iter().any(|a| a.task_id == ws_task.id));

        // A workspace task with a tree source requires both WORKSPACES and BLOB_TRANSFER.
        let ws_tree_task = Task::new(
            TaskSpec::new_command("cargo", vec!["test".into()]),
            TaskRequirements::generic(1, 30),
        )
        .with_workspace(
            WorkspaceRef::new("agent-1")
                .with_tree(rusty_grid_core::blob::BlobId::of(b"manifest"), 16),
        );
        let tree_features = TaskView::of(&ws_tree_task).required_features;
        assert!(tree_features.contains(ProtocolCapabilities::WORKSPACES));
        assert!(tree_features.contains(ProtocolCapabilities::BLOB_TRANSFER));
    }

    #[tokio::test]
    async fn rv158_probe_expired_hold_dispatched_before_release_skips_failover_charge() {
        let queue = TaskQueue::with_config(
            RetryPolicy {
                max_retries: 2,
                max_failovers: 2,
                retry_on_worker_disconnect: true,
                initial_backoff_ms: 0,
                ..RetryPolicy::default()
            },
            None,
        );
        queue
            .set_session_resume_hold(Duration::from_millis(20))
            .await;
        let w1 = Uuid::new_v4();
        let w2 = Uuid::new_v4();
        queue.set_worker_session_resume(w1, true);
        let t = queue.submit(echo_task("rv158-p2")).await.unwrap();
        queue.schedule_tasks_batch_in_sessions(&[(t, w1, 1)]).await;
        queue.mark_running_from(w1, Some(1), &t).await.unwrap();
        queue
            .handle_session_disconnected(&w1, Some(1), "drop", true)
            .await;
        assert!(
            queue.ready_task_ids(10).await.is_empty(),
            "held while hold active"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(queue.ready_task_ids(10).await, vec![t]);
        let placed = queue.schedule_tasks_batch_in_sessions(&[(t, w2, 7)]).await;
        assert_eq!(
            placed.len(),
            1,
            "expired-but-unreleased hold was dispatched"
        );
        let _ = queue.release_expired_adoption_holds().await;
        let info = queue.get_task(&t).await.unwrap();
        eprintln!(
            "RV158 PROBE P2: state={:?} retry_count={} failover_count={}",
            info.state, info.retry_count, info.failover_count
        );
        assert_eq!(
            info.failover_count, 1,
            "deferred failover charge was skipped"
        );
    }

    #[tokio::test]
    async fn test_cross_worker_cannot_adopt_another_workers_held_task() {
        let queue = TaskQueue::with_config(
            RetryPolicy {
                max_retries: 2,
                max_failovers: 2,
                retry_on_worker_disconnect: true,
                initial_backoff_ms: 0,
                ..RetryPolicy::default()
            },
            None,
        );
        queue.set_session_resume_hold(Duration::from_secs(30)).await;
        let worker_a = Uuid::new_v4();
        let worker_b = Uuid::new_v4();
        queue.set_worker_session_resume(worker_a, true);
        queue.set_worker_session_resume(worker_b, true);

        let tid = queue.submit(echo_task("held-by-a")).await.unwrap();
        queue
            .schedule_tasks_batch_in_sessions(&[(tid, worker_a, 1)])
            .await;
        queue
            .mark_running_from(worker_a, Some(1), &tid)
            .await
            .unwrap();
        queue
            .handle_session_disconnected(&worker_a, Some(1), "drop", true)
            .await;

        // `worker_b` attempts to adopt `worker_a`'s held task: must be rejected in `stale_to_cancel`.
        let b_outcome = queue.adopt_session_tasks(worker_b, 2, &[tid]).await;
        assert!(b_outcome.adopted.is_empty());
        assert_eq!(b_outcome.stale_to_cancel, vec![tid]);

        // `worker_b` attempts to send a Checkpoint and a TaskResult for `worker_a`'s held task:
        // both must be refused (`Ok(false)` and `ResultDisposition::Stale`).
        let cp = CheckpointData::new(1, b"forged-cp".to_vec());
        let cp_applied = queue
            .record_checkpoint_from(worker_b, Some(2), tid, cp)
            .await
            .unwrap();
        assert!(
            !cp_applied,
            "worker_b must not be able to checkpoint a task held by worker_a"
        );
        assert!(queue.get_checkpoint(&tid).await.is_none());

        let forged_res = TaskResult::success(worker_b, tid, "forged", 10, false);
        let res_outcome = queue
            .record_result_from(worker_b, Some(2), forged_res)
            .await
            .unwrap();
        assert_eq!(
            res_outcome.disposition,
            ResultDisposition::Stale,
            "worker_b must not be able to complete a task held by worker_a"
        );
        assert!(!res_outcome.adopted_from_hold);

        // And `worker_a`'s hold must still be intact so `worker_a` can adopt it.
        let a_outcome = queue.adopt_session_tasks(worker_a, 3, &[tid]).await;
        assert_eq!(a_outcome.adopted, vec![tid]);
        assert!(a_outcome.stale_to_cancel.is_empty());
        let info = queue.get_task(&tid).await.unwrap();
        assert_eq!(info.state, TaskState::Running);
        assert_eq!(info.assigned_worker_id, Some(worker_a));
    }

    #[tokio::test]
    async fn rv158b_probe_bound_evicts_live_worker_resume_flag() {
        let queue = TaskQueue::new();
        queue.set_session_resume_hold(Duration::from_secs(60)).await;
        let live_worker = Uuid::new_v4();
        queue.set_worker_session_resume(live_worker, true);

        let tid = queue.submit(echo_task("rv158b-p4")).await.unwrap();
        queue
            .schedule_tasks_batch_in_sessions(&[(tid, live_worker, 1)])
            .await;
        queue
            .mark_running_from(live_worker, Some(1), &tid)
            .await
            .unwrap();

        for _ in 0..MAX_SESSION_RESUME_WORKERS {
            queue.set_worker_session_resume(Uuid::new_v4(), true);
        }

        assert!(
            queue
                .inner
                .read()
                .await
                .session_resume_workers
                .lock()
                .unwrap()
                .contains(&live_worker),
            "live worker with an active task must not be evicted by MAX_SESSION_RESUME_WORKERS churn"
        );
    }

    #[tokio::test]
    async fn rv158b_probe_bound_evicts_worker_with_active_hold() {
        let queue = TaskQueue::new();
        queue.set_session_resume_hold(Duration::from_secs(60)).await;
        let worker_a = Uuid::new_v4();
        queue.set_worker_session_resume(worker_a, true);

        let tid = queue.submit(echo_task("rv158b-p4b")).await.unwrap();
        queue
            .schedule_tasks_batch_in_sessions(&[(tid, worker_a, 1)])
            .await;
        queue
            .mark_running_from(worker_a, Some(1), &tid)
            .await
            .unwrap();
        queue
            .handle_session_disconnected(&worker_a, Some(1), "socket drop", true)
            .await;

        for _ in 0..MAX_SESSION_RESUME_WORKERS {
            queue.set_worker_session_resume(Uuid::new_v4(), true);
        }

        assert!(
            queue
                .inner
                .read()
                .await
                .session_resume_workers
                .lock()
                .unwrap()
                .contains(&worker_a),
            "worker with an active adoption hold must not be evicted by MAX_SESSION_RESUME_WORKERS churn"
        );
    }

    #[tokio::test]
    async fn rv158b_probe_evicted_live_worker_gets_no_hold() {
        let queue = TaskQueue::new();
        queue.set_session_resume_hold(Duration::from_secs(60)).await;
        let live_worker = Uuid::new_v4();
        queue.set_worker_session_resume(live_worker, true);

        let mut task = echo_task("rv158b-p4c");
        task.requirements.max_retries = Some(0);
        let tid = queue.submit(task).await.unwrap();
        queue
            .schedule_tasks_batch_in_sessions(&[(tid, live_worker, 1)])
            .await;
        queue
            .mark_running_from(live_worker, Some(1), &tid)
            .await
            .unwrap();

        for _ in 0..MAX_SESSION_RESUME_WORKERS {
            queue.set_worker_session_resume(Uuid::new_v4(), true);
        }

        queue
            .handle_session_disconnected(&live_worker, Some(1), "socket drop", true)
            .await;
        let info = queue.get_task(&tid).await.unwrap();
        assert_eq!(
            info.state,
            TaskState::Queued,
            "live worker's zero-retry task must be held for ResumeSession after disconnect despite churn"
        );
        let outcome = queue.adopt_session_tasks(live_worker, 2, &[tid]).await;
        assert_eq!(outcome.adopted, vec![tid]);
    }

    #[tokio::test]
    async fn test_session_resume_workers_bounded_and_pruned_on_hold_expiry() {
        let queue = TaskQueue::new();
        queue
            .set_session_resume_hold(Duration::from_millis(20))
            .await;

        for _ in 0..(MAX_SESSION_RESUME_WORKERS + 100) {
            queue.set_worker_session_resume(Uuid::new_v4(), true);
        }
        assert_eq!(
            queue
                .inner
                .read()
                .await
                .session_resume_workers
                .lock()
                .unwrap()
                .len(),
            MAX_SESSION_RESUME_WORKERS
        );

        let queue2 = TaskQueue::new();
        queue2
            .set_session_resume_hold(Duration::from_millis(20))
            .await;
        let w = Uuid::new_v4();
        queue2.set_worker_session_resume(w, true);
        let tid = queue2.submit(echo_task("prune-w")).await.unwrap();
        queue2
            .schedule_tasks_batch_in_sessions(&[(tid, w, 1)])
            .await;
        queue2.mark_running_from(w, Some(1), &tid).await.unwrap();
        queue2
            .handle_session_disconnected(&w, Some(1), "drop", true)
            .await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        let _ = queue2.release_expired_adoption_holds().await;
        assert!(
            !queue2
                .inner
                .read()
                .await
                .session_resume_workers
                .lock()
                .unwrap()
                .contains(&w),
            "expired worker with no active tasks or holds must be pruned from session_resume_workers"
        );
    }

    #[tokio::test]
    async fn test_session_resume_connected_survives_stale_session_disconnect_and_registration_churn(
    ) {
        let queue = TaskQueue::new();
        queue.set_session_resume_hold(Duration::from_secs(60)).await;
        let live_worker = Uuid::new_v4();

        // Worker connects on session 1, then quickly reconnects on session 2 (idle).
        queue.set_worker_session_resume(live_worker, true);
        queue.set_worker_session_resume_connected(live_worker, 1, true);
        queue.set_worker_session_resume(live_worker, true);
        queue.set_worker_session_resume_connected(live_worker, 2, true);

        // Session 1's late disconnect sweep runs after session 2 is already live.
        queue.set_worker_session_resume_connected(live_worker, 1, false);
        queue
            .handle_session_disconnected(&live_worker, Some(1), "superseded session 1", true)
            .await;

        // Over 4,096 ephemeral workers register and disconnect.
        for i in 0..(MAX_SESSION_RESUME_WORKERS + 128) {
            let ephemeral = Uuid::new_v4();
            let sid = 100 + i as u64;
            queue.set_worker_session_resume(ephemeral, true);
            queue.set_worker_session_resume_connected(ephemeral, sid, true);
            queue.set_worker_session_resume_connected(ephemeral, sid, false);
        }

        // The live worker on session 2 must still be marked SESSION_RESUME-capable.
        assert!(
            queue
                .inner
                .read()
                .await
                .session_resume_workers
                .lock()
                .unwrap()
                .contains(&live_worker),
            "live worker on session 2 must not be evicted after session 1 disconnect sweep and >4,096 registrations"
        );

        // When session 2 later disconnects and another wave of ephemeral workers registers,
        // the worker is no longer connected and can be evicted.
        queue.set_worker_session_resume_connected(live_worker, 2, false);
        queue
            .handle_session_disconnected(&live_worker, Some(2), "session 2 closed", true)
            .await;
        for _ in 0..(MAX_SESSION_RESUME_WORKERS + 10) {
            queue.set_worker_session_resume(Uuid::new_v4(), true);
        }
        assert!(
            !queue
                .inner
                .read()
                .await
                .session_resume_workers
                .lock()
                .unwrap()
                .contains(&live_worker),
            "disconnected idle worker must be evictable once its latest session disconnects"
        );
    }

    #[tokio::test]
    async fn test_workspace_one_at_a_time_across_master_restart_and_disconnect_holds() {
        use rusty_grid_core::task::WorkspaceRef;

        let dir = tempfile::tempdir().unwrap();
        let worker_a = Uuid::new_v4();
        let worker_b = Uuid::new_v4();

        // 1. Reviewer's exact scenario: task A in `ws-a` is Running; store is reopened with a
        //    30 s hold; task B in `ws-a` is submitted.
        let task_a_id = {
            let (store, rec) = TaskStore::open(dir.path()).unwrap();
            let q = TaskQueue::with_config(replay_policy(), None);
            q.attach_store_and_replay(store, rec, Duration::ZERO)
                .await
                .unwrap();

            let task_a = echo_task("ws-a-task-a").with_workspace(WorkspaceRef::new("ws-a"));
            let a_id = q.submit(task_a).await.unwrap();
            q.schedule_tasks_batch_in_sessions(&[(a_id, worker_a, 1)])
                .await;
            q.mark_running_from(worker_a, Some(1), &a_id).await.unwrap();
            drop(q);
            a_id
        };

        let (store2, rec2) = TaskStore::open(dir.path()).unwrap();
        let q2 = TaskQueue::with_config(replay_policy(), None);
        q2.attach_store_and_replay(store2, rec2, Duration::from_secs(30))
            .await
            .unwrap();

        let task_b = echo_task("ws-a-task-b").with_workspace(WorkspaceRef::new("ws-a"));
        let task_b_id = q2.submit(task_b).await.unwrap();

        let (_aff, active_ws) = q2.workspace_scheduling_state().await;
        assert!(
            active_ws.contains("ws-a"),
            "held task A after master restart must keep workspace ws-a active"
        );

        // Neither pop_and_schedule, schedule_task, nor schedule_tasks_batch_in_sessions may
        // dispatch task B while task A is held for adoption.
        assert!(
            q2.pop_and_schedule(worker_b, |_| true).await.is_none(),
            "pop_and_schedule must not return task B while task A holds ws-a"
        );
        assert!(
            q2.schedule_task(&task_b_id, worker_b).await.is_none(),
            "schedule_task must not schedule task B while task A holds ws-a"
        );
        assert!(
            q2.schedule_tasks_batch_in_sessions(&[(task_b_id, worker_b, 1)])
                .await
                .is_empty(),
            "schedule_tasks_batch_in_sessions must not schedule task B while task A holds ws-a"
        );

        // After `adopt_session_tasks`, only A is Running and B stays Queued.
        let adopted = q2.adopt_session_tasks(worker_a, 2, &[task_a_id]).await;
        assert_eq!(adopted.adopted, vec![task_a_id]);
        assert_eq!(q2.get_state(&task_a_id).await, Some(TaskState::Running));
        assert_eq!(q2.get_state(&task_b_id).await, Some(TaskState::Queued));
        assert!(q2.pop_and_schedule(worker_b, |_| true).await.is_none());

        // 2. F1-c disconnect hold: `worker_a` (with SESSION_RESUME) disconnects; task A enters an
        //    adoption hold again, and task B must still be refused until A finishes.
        q2.set_worker_session_resume(worker_a, true);
        q2.handle_session_disconnected(&worker_a, Some(2), "brief drop", true)
            .await;
        assert_eq!(q2.get_state(&task_a_id).await, Some(TaskState::Queued));
        let (_aff, active_after_disc) = q2.workspace_scheduling_state().await;
        assert!(
            active_after_disc.contains("ws-a"),
            "F1-c disconnect hold on task A must keep workspace ws-a active"
        );
        assert!(q2.pop_and_schedule(worker_b, |_| true).await.is_none());
        assert!(q2.schedule_task(&task_b_id, worker_b).await.is_none());

        // Re-adopt and complete task A; now task B can be scheduled.
        let adopted2 = q2.adopt_session_tasks(worker_a, 3, &[task_a_id]).await;
        assert_eq!(adopted2.adopted, vec![task_a_id]);
        q2.record_result_from(
            worker_a,
            Some(3),
            TaskResult::success(worker_a, task_a_id, "ok", 5, false),
        )
        .await
        .unwrap();

        let (_aff, active_after_done) = q2.workspace_scheduling_state().await;
        assert!(!active_after_done.contains("ws-a"));
        let popped_b = q2.pop_and_schedule(worker_b, |_| true).await.unwrap();
        assert_eq!(popped_b.id, task_b_id);
        q2.close_and_compact_store().await;
    }

    #[tokio::test]
    async fn test_workspace_queue_level_guards_refuse_second_task_when_engine_pass_is_stale() {
        use rusty_grid_core::task::WorkspaceRef;

        let q = TaskQueue::new();
        let w1 = Uuid::new_v4();
        let w2 = Uuid::new_v4();

        let t1 = q
            .submit(echo_task("ws-1").with_workspace(WorkspaceRef::new("ws-guard")))
            .await
            .unwrap();
        let t2 = q
            .submit(echo_task("ws-2").with_workspace(WorkspaceRef::new("ws-guard")))
            .await
            .unwrap();
        let _t3 = q
            .submit(echo_task("ws-3").with_workspace(WorkspaceRef::new("ws-guard")))
            .await
            .unwrap();
        let t_other = q
            .submit(echo_task("other").with_workspace(WorkspaceRef::new("ws-other")))
            .await
            .unwrap();

        // Simulate a stale engine pass (or direct batch call) attempting to schedule two tasks of
        // the same workspace (`t1` and `t2`) in one batch alongside `t_other`.
        let placed = q
            .schedule_tasks_batch_in_sessions(&[(t1, w1, 1), (t2, w2, 1), (t_other, w2, 1)])
            .await;
        let placed_ids: Vec<TaskId> = placed.iter().map(|(id, _, _)| *id).collect();
        assert_eq!(
            placed_ids,
            vec![t1, t_other],
            "schedule_batch_inner guard must refuse the second task of ws-guard"
        );
        assert_eq!(q.get_state(&t2).await, Some(TaskState::Queued));

        // While `t1` is Scheduled in `ws-guard`, both `schedule_task` and `pop_and_schedule` must
        // refuse `t2` and `t3`.
        assert!(
            q.schedule_task(&t2, w2).await.is_none(),
            "schedule_task guard must refuse t2 while t1 is Scheduled in ws-guard"
        );
        assert!(
            q.pop_and_schedule(w2, |_| true).await.is_none(),
            "pop_and_schedule guard must refuse t2/t3 while t1 is Scheduled in ws-guard"
        );

        // Complete `t1`; now `schedule_task` accepts `t2`, and while `t2` is Scheduled,
        // `pop_and_schedule` still refuses `t3`.
        q.record_result_from(w1, Some(1), TaskResult::success(w1, t1, "done", 1, false))
            .await
            .unwrap();
        let scheduled_t2 = q.schedule_task(&t2, w2).await.unwrap();
        assert_eq!(scheduled_t2.id, t2);
        assert!(
            q.pop_and_schedule(w1, |_| true).await.is_none(),
            "pop_and_schedule guard must refuse t3 while t2 is Scheduled in ws-guard"
        );

        // Requeue `t2` (e.g. dispatch failure); `ws-guard` is now free and `pop_and_schedule`
        // dispatches `t2`.
        q.requeue_task(&t2).await.unwrap();
        let popped = q.pop_and_schedule(w1, |_| true).await.unwrap();
        assert_eq!(popped.id, t2);
    }

    #[tokio::test]
    async fn test_workspace_affinity_replay_uses_terminal_finish_timestamp() {
        use rusty_grid_core::task::WorkspaceRef;

        let dir = tempfile::tempdir().unwrap();
        let w1 = Uuid::new_v4();
        let tid = TaskId::new();
        let mut task = echo_task("ws-replay-ts").with_workspace(WorkspaceRef::new("ws-ts"));
        task.id = tid;

        {
            let (store, _rec) = TaskStore::open(dir.path()).unwrap();
            store
                .append_durable(&JournalRecord::TaskSubmitted {
                    task: Box::new(task),
                    priority: 0,
                    sequence: 1,
                    submitted_at_utc: 1_000,
                })
                .unwrap()
                .await
                .unwrap();
            store
                .append_durable(&JournalRecord::TaskDispatched {
                    task_id: tid,
                    worker_id: w1,
                    timestamp_utc: 1_010,
                })
                .unwrap()
                .await
                .unwrap();
            store
                .append_durable(&JournalRecord::TaskTerminal {
                    task_id: tid,
                    state: TaskState::Completed,
                    assigned_worker_id: Some(w1),
                    retry_count: 0,
                    failover_count: 0,
                    evacuation_count: 0,
                    rejection_count: 0,
                    retry_attempt: None,
                    result: Some(TaskResult::success(w1, tid, "ok", 5, false)),
                    error_message: None,
                    timestamp_utc: 2_500,
                })
                .unwrap()
                .await
                .unwrap();
        }

        let (store2, rec2) = TaskStore::open(dir.path()).unwrap();
        let q = TaskQueue::new();
        q.attach_store_and_replay(store2, rec2, Duration::ZERO)
            .await
            .unwrap();

        let workspaces = q.list_workspaces().await;
        assert_eq!(workspaces.len(), 1);
        assert_eq!(workspaces[0].name, "ws-ts");
        assert_eq!(workspaces[0].worker, w1);
        assert_eq!(
            workspaces[0].last_used_utc, 2_500,
            "replay must record workspace affinity with the TaskTerminal finish timestamp"
        );
        q.close_and_compact_store().await;
    }
}
