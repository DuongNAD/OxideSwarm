//! Thread-safe Task Queue and 8-State Finite State Machine (FSM) for `rusty_grid_master`.
//!
//! Provides priority-FIFO indexing, O(1) task lookup, worker assignment tracking,
//! configurable exponential backoff retries, and atomic state transitions.

use std::cmp::Reverse;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use uuid::Uuid;

use rusty_grid_core::error::GridResult;
use rusty_grid_core::placement::Placement;
use rusty_grid_core::protocol::ProtocolCapabilities;
use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskResult, TaskSpec, TaskStatus};

use crate::cold_results::{
    task_result_bytes, ColdResultStore, ColdResultSummary, DEFAULT_MAX_HOT_TERMINAL_BYTES,
    DEFAULT_MAX_HOT_TERMINAL_TASKS,
};
use crate::fair_share::{FairShareQueues, SubmitterId};
use crate::store::{DurableAck, JournalRecord, PersistedTaskEntry, SnapshotImage, TaskStore};

mod lifecycle;
mod recovery;
mod retention;
mod scheduling;

#[cfg(test)]
mod recovery_tests;
#[cfg(test)]
mod tests;

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
    pub task_key: crate::speed::TaskKey,
    pub priority: u32,
    pub sequence: u64,
    pub state: TaskState,
    pub assigned_worker_id: Option<Uuid>,
    pub submitted_at_utc: u64,
    pub submitted_instant: Instant,
    /// Timestamp when this task most recently became schedulable in `Queued` state (`0060` D2).
    pub queued_instant: Instant,
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
    /// True when the current dispatch resumed from a prior checkpoint (`AssignTaskWithCheckpoint`).
    pub resumed_from_checkpoint: bool,
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
    /// Fair-share submitter identity (not persisted across restarts).
    pub submitter: SubmitterId,
    /// Compact metadata retained in RAM after the full `TaskResult` spills to `cold_results/`.
    pub cold_result: Option<ColdResultSummary>,
    /// True when the task's input trees or workspace source tree require `oxideswarm-tree/2`
    /// (`ProtocolCapabilities::TREE_V2`), recorded when `BlobStore::resolve_inputs_with_features`
    /// decodes the manifest at task admission or store recovery.
    pub requires_tree_v2: bool,
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
            execution_time_ms: self
                .result
                .as_ref()
                .map(|r| r.execution_time_ms)
                .or_else(|| self.cold_result.as_ref().map(|c| c.execution_time_ms)),
            exit_code: self
                .result
                .as_ref()
                .map(|r| r.exit_code)
                .or_else(|| self.cold_result.as_ref().map(|c| c.exit_code)),
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

/// RAM-only task status summary for `GetTaskStatus` without reading cold result files.
#[derive(Debug, Clone)]
pub struct TaskStatusDetail {
    pub info: TaskInfo,
    pub result_exit_code: Option<i32>,
    pub result_error: Option<String>,
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
    /// Speed-history key of the task (`0060` D1/D2).
    pub task_key: crate::speed::TaskKey,
    /// How long the task has waited in `Queued` state since it last became schedulable (`0060` D2).
    pub queued_for: Duration,
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
    /// The view of `task` with no avoided workers and zero wait duration.
    pub fn of(task: &Task) -> Self {
        Self::of_with_key(task, crate::speed::TaskKey::for_task(task))
    }

    pub(crate) fn of_with_key(task: &Task, task_key: crate::speed::TaskKey) -> Self {
        Self {
            task_id: task.id,
            task_key,
            queued_for: Duration::ZERO,
            requirements: task.requirements.clone(),
            requires_gpu: task.requirements.requires_gpu_or_vram() || task.spec.requires_gpu(),
            heavy: matches!(
                task.spec,
                TaskSpec::RustCompilation { .. } | TaskSpec::GpuCompute { .. }
            ),
            required_features: required_features(task),
            avoid: Vec::new(),
            placement: task.scheduling_placement(),
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
    if matches!(task.spec, TaskSpec::Generative3D(_) | TaskSpec::AutoRig(_)) {
        features.insert(ProtocolCapabilities::THREE_D);
    }
    if let Some(workspace) = &task.workspace {
        features.insert(ProtocolCapabilities::WORKSPACES);
        if workspace.source.is_some() {
            features.insert(ProtocolCapabilities::BLOB_TRANSFER);
        }
    }
    features
}

/// True when `task` references any tree manifest (`InputSource::Tree` or `WorkspaceSource::Tree`).
///
/// Direct `TaskQueue::submit` / `try_submit_slot` / `attach_store_and_replay` paths that have not
/// yet inspected the manifest via `admit_blob_task` / `repin_recovered_blobs` default
/// `requires_tree_v2` to `task_has_tree_input(task)` so uninspected tree tasks fail closed
/// (requiring `TREE_V2` until the manifest is verified to be v1).
fn task_has_tree_input(task: &Task) -> bool {
    task.inputs
        .iter()
        .any(|i| matches!(i.source, rusty_grid_core::blob::InputSource::Tree { .. }))
        || task.workspace.as_ref().is_some_and(|w| {
            matches!(
                w.source,
                Some(rusty_grid_core::task::WorkspaceSource::Tree { .. })
            )
        })
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
    /// Declared GPU VRAM of all active tasks (`Scheduled`, `Running`, or held for adoption).
    /// Unlike host RAM, worker heartbeats report static total VRAM rather than live free VRAM.
    pub vram_mb: u64,
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
    /// Speed-history key of the task (`0060` D1).
    pub task_key: crate::speed::TaskKey,
    /// Declared timeout of the task in seconds (`0060` D1).
    pub timeout_secs: u64,
    /// True when this attempt resumed from a prior checkpoint (`AssignTaskWithCheckpoint`).
    pub resumed_from_checkpoint: bool,
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
    // `rusty_grid_core::SANDBOX_BASE_REFUSAL_PREFIX`, lower case (design 0090); it also holds
    // "rejected by worker", which is what an older master matches.
    "rejected by worker: sandbox base unusable",
    // `rusty_grid_core::CONTRIBUTOR_CONTROL_REFUSAL_PREFIX`, lower case (NET-9, design 0095); it
    // also holds "rejected by worker", which is what an older master matches.
    "rejected by worker: contributor control",
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
    fair_queues: FairShareQueues,
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
    /// FIFO order of terminal tasks whose full `TaskResult` is still hot in RAM.
    hot_terminal_order: std::collections::VecDeque<TaskId>,
    /// Result payload byte size per hot terminal task.
    hot_terminal_sizes: HashMap<TaskId, usize>,
    /// Sum of `hot_terminal_sizes`.
    hot_terminal_bytes: usize,
    /// Max hot terminal tasks kept in RAM when `cold_results` is active.
    max_hot_terminal_tasks: usize,
    /// Max hot terminal result bytes kept in RAM when `cold_results` is active.
    max_hot_terminal_bytes: usize,
    /// Cold result files queued for unlinking after eviction (flushed off the lock).
    pending_cold_deletes: Vec<TaskId>,
    /// Out-of-band cold result storage attached alongside `store`.
    cold_results: Option<Arc<ColdResultStore>>,
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
    /// Lock-free Prometheus queue counters and latency histogram shared with [`TaskQueue`].
    metrics: Arc<crate::metrics::QueueMetrics>,
    /// Active `"run"` OpenTelemetry spans kept open from [`TaskQueue::mark_running_from`] until
    /// execution completes, retries, or cancels.
    #[cfg(feature = "otel")]
    run_spans: HashMap<TaskId, tracing::Span>,
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
    // Extension values count with the rest of the task (design 0082 D4); the 3 bytes are the id
    // and the critical flag.
    let extensions_bytes: usize = task.all_extensions().map(|e| 3 + e.bytes.len()).sum();
    RETAINED_ENTRY_OVERHEAD_BYTES
        + spec
        + extensions_bytes
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
    let result = entry
        .result
        .as_ref()
        .map(task_result_bytes)
        .or_else(|| entry.cold_result.as_ref().map(|c| c.result_bytes))
        .unwrap_or(0);
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

fn mark_entry_scheduled(
    entry: &mut TaskEntry,
    worker_id: Uuid,
    session_id: Option<u64>,
    now: Instant,
    policy: &RetryPolicy,
    metrics: &crate::metrics::QueueMetrics,
) -> (Task, bool, SubmitterId) {
    let zr = policy.zero_retries_for(entry);
    metrics.observe_scheduling_latency(now.saturating_duration_since(entry.queued_instant));
    entry.state = TaskState::Scheduled;
    entry.assigned_worker_id = Some(worker_id);
    entry.assigned_session = session_id;
    entry.resumed_from_checkpoint = entry.task.latest_checkpoint.is_some();
    entry.adoptable_by = None;
    entry.fail_if_not_adopted = false;
    entry.pending_failover_charge = false;
    entry.pending_failover_reason = None;
    entry.scheduled_instant = Some(now);
    (entry.task.clone(), zr, entry.submitter)
}

impl TaskQueueInner {
    fn close_run_span(&mut self, task_id: &TaskId) {
        #[cfg(feature = "otel")]
        {
            self.run_spans.remove(task_id);
        }
        #[cfg(not(feature = "otel"))]
        let _ = task_id;
    }

    fn untrack_active_task(&mut self, task_id: &TaskId) {
        if let Some(size) = self.active_sizes.remove(task_id) {
            self.active_bytes = self.active_bytes.saturating_sub(size);
        }
    }

    fn update_active_task(&mut self, task_id: TaskId) {
        self.untrack_active_task(&task_id);
        if !self
            .tasks
            .get(&task_id)
            .is_some_and(|e| e.state == TaskState::Running)
        {
            self.close_run_span(&task_id);
        }
        if let Some(entry) = self.tasks.get(&task_id).filter(|e| !e.state.is_terminal()) {
            let size = retained_bytes(entry);
            self.active_sizes.insert(task_id, size);
            self.active_bytes = self.active_bytes.saturating_add(size);
        }
    }

    /// Drops a retained terminal task and its byte charge.
    fn remove_retained(&mut self, task_id: &TaskId) {
        let (prev_hold_worker, ws_name, had_cold) = self
            .tasks
            .get(task_id)
            .map(|e| {
                (
                    e.adoptable_by.map(|(w, _)| w),
                    e.task.workspace.as_ref().map(|ws| ws.name.clone()),
                    e.cold_result.is_some(),
                )
            })
            .unwrap_or((None, None, false));
        self.tasks.remove(task_id);
        self.untrack_active_task(task_id);
        self.close_run_span(task_id);
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
        if let Some(hsize) = self.hot_terminal_sizes.remove(task_id) {
            self.hot_terminal_bytes = self.hot_terminal_bytes.saturating_sub(hsize);
            if self.hot_terminal_order.len()
                > self.hot_terminal_sizes.len().saturating_mul(2).max(64)
            {
                self.hot_terminal_order
                    .retain(|id| self.hot_terminal_sizes.contains_key(id));
            }
        }
        if had_cold && self.cold_results.is_some() {
            self.pending_cold_deletes.push(*task_id);
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
        self.close_run_span(&task_id);
        if self.terminal_sizes.contains_key(&task_id) {
            return;
        }
        if let Some(state) = self.tasks.get(&task_id).map(|e| e.state) {
            self.metrics.record_terminal(state);
        }
        let (size, hot_size) = self.tasks.get(&task_id).map_or((0, None), |e| {
            (retained_bytes(e), e.result.as_ref().map(task_result_bytes))
        });
        self.terminal_sizes.insert(task_id, size);
        self.terminal_bytes += size;
        self.terminal_order.push_back(task_id);
        if let Some(hsize) = hot_size {
            self.hot_terminal_sizes.insert(task_id, hsize);
            self.hot_terminal_bytes = self.hot_terminal_bytes.saturating_add(hsize);
            self.hot_terminal_order.push_back(task_id);
        }
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

    fn collect_cold_spill_ids(&self) -> Vec<TaskId> {
        let mut candidates = Vec::new();
        let mut sim_tasks = self.hot_terminal_sizes.len();
        let mut sim_bytes = self.hot_terminal_bytes;
        for tid in &self.hot_terminal_order {
            if sim_tasks <= self.max_hot_terminal_tasks && sim_bytes <= self.max_hot_terminal_bytes
            {
                break;
            }
            let Some(&hsize) = self.hot_terminal_sizes.get(tid) else {
                continue;
            };
            let Some(entry) = self.tasks.get(tid) else {
                sim_tasks = sim_tasks.saturating_sub(1);
                continue;
            };
            if !entry.state.is_terminal() || entry.result.is_none() {
                sim_tasks = sim_tasks.saturating_sub(1);
                continue;
            }
            candidates.push(*tid);
            sim_tasks = sim_tasks.saturating_sub(1);
            sim_bytes = sim_bytes.saturating_sub(hsize);
        }
        candidates
    }

    fn insert_ready(&mut self, key: QueueOrderKey) {
        if self.ready_queue.insert(key) {
            self.ready_epoch = self.ready_epoch.wrapping_add(1);
            let submitter = self
                .tasks
                .get(&key.task_id)
                .map_or(SubmitterId::Default, |e| e.submitter);
            self.fair_queues.insert(submitter, key);
            if let Some(entry) = self.tasks.get_mut(&key.task_id) {
                entry.queued_instant = Instant::now();
            }
        }
    }

    fn remove_ready(&mut self, key: &QueueOrderKey) -> bool {
        let removed = self.ready_queue.remove(key);
        let submitter = self
            .tasks
            .get(&key.task_id)
            .map_or(SubmitterId::Default, |e| e.submitter);
        self.fair_queues.remove(submitter, key);
        removed
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
                cold_result: entry.cold_result.clone(),
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
    metrics: Arc<crate::metrics::QueueMetrics>,
    adoption_holds: Arc<std::sync::atomic::AtomicUsize>,
    session_resume_hold_secs: Arc<std::sync::atomic::AtomicU64>,
    session_resume_workers: Arc<std::sync::Mutex<HashSet<Uuid>>>,
    session_resume_tracker: Arc<std::sync::Mutex<SessionResumeTracker>>,
    cold_flush_lock: Arc<tokio::sync::Mutex<()>>,
    cold_maintenance_requested: Arc<std::sync::atomic::AtomicBool>,
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
        let metrics = Arc::new(crate::metrics::QueueMetrics::new());
        Self {
            inner: Arc::new(RwLock::new(TaskQueueInner {
                tasks: HashMap::new(),
                ready_queue: BTreeSet::new(),
                fair_queues: FairShareQueues::default(),
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
                hot_terminal_order: std::collections::VecDeque::new(),
                hot_terminal_sizes: HashMap::new(),
                hot_terminal_bytes: 0,
                max_hot_terminal_tasks: DEFAULT_MAX_HOT_TERMINAL_TASKS,
                max_hot_terminal_bytes: DEFAULT_MAX_HOT_TERMINAL_BYTES,
                pending_cold_deletes: Vec::new(),
                cold_results: None,
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
                metrics: Arc::clone(&metrics),
                #[cfg(feature = "otel")]
                run_spans: HashMap::new(),
            })),
            metrics,
            adoption_holds,
            session_resume_hold_secs,
            session_resume_workers,
            session_resume_tracker,
            cold_flush_lock: Arc::new(tokio::sync::Mutex::new(())),
            cold_maintenance_requested: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            ready_views_calls: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }
}
