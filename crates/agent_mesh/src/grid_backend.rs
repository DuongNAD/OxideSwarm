//! Backend abstraction decoupling [`crate::bridge::AgentGridBridge`] from a concrete
//! in-process [`MasterHandle`].
//!
//! Provides the object-safe async [`GridBackend`] trait and its in-process implementation
//! [`InProcessBackend`]. Remote master attachment is provided by
//! [`crate::remote_backend::RemoteMasterBackend`].

use std::time::Duration;

use futures::future::BoxFuture;
use uuid::Uuid;

use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::task::{CheckpointData, Task, TaskId, TaskResult, TaskStatus};
use rusty_grid_master::queue::TaskState;
use rusty_grid_master::registry::WorkerStatus;
use rusty_grid_master::server::MasterHandle;

/// Boxed `Send` future alias used by the object-safe [`GridBackend`] trait.
pub type BoxGridFuture<'a, T> = BoxFuture<'a, GridResult<T>>;

/// Normalized task status returned by [`GridBackend::get_task_status`] and pending
/// [`GridTaskResult`] responses.
#[derive(Debug, Clone)]
pub struct GridTaskStatus {
    pub task_id: TaskId,
    pub status: TaskStatus,
    pub state_name: String,
    pub assigned_worker: Option<Uuid>,
    pub error: Option<String>,
    /// Populated by [`InProcessBackend`] for backward-compatible `TaskInfo` JSON payloads.
    pub task_info: Option<serde_json::Value>,
    /// Populated by [`InProcessBackend`] when a task checkpoint is recorded.
    pub checkpoint: Option<CheckpointData>,
}

/// Outcome of a non-blocking [`GridBackend::get_task_result`] query.
#[derive(Debug, Clone)]
pub enum GridTaskResult {
    Completed(TaskResult),
    Pending(GridTaskStatus),
}

/// Unified cluster status snapshot across in-process and remote Master backends.
#[derive(Debug, Clone)]
pub struct GridClusterStatus {
    pub active_workers: usize,
    pub total_workers: usize,
    pub queued_tasks: usize,
    pub running_tasks: usize,
    pub completed_tasks: usize,
    pub failed_tasks: usize,
    pub cancelled_tasks: usize,
    pub queue_json: serde_json::Value,
    pub workers_json: serde_json::Value,
}

/// Object-safe async backend trait powering [`crate::bridge::AgentGridBridge`].
pub trait GridBackend: Send + Sync {
    /// Submits a task asynchronously (`wait = false`) and returns its assigned [`TaskId`].
    fn submit_task(&self, task: Task, priority: u32) -> BoxGridFuture<'_, TaskId>;

    /// Submits a task and waits up to `timeout` for completion (`wait = true`).
    fn submit_and_wait(
        &self,
        task: Task,
        priority: u32,
        timeout: Duration,
    ) -> BoxGridFuture<'_, TaskResult>;

    /// Queries the current status of a specific task by ID.
    fn get_task_status(&self, task_id: TaskId) -> BoxGridFuture<'_, GridTaskStatus>;

    /// Queries the retained result of a task if terminal, or its current status otherwise.
    fn get_task_result(&self, task_id: TaskId) -> BoxGridFuture<'_, GridTaskResult>;

    /// Waits up to `timeout` for an already-submitted task to finish and returns its result.
    fn wait_task(&self, task_id: TaskId, timeout: Duration) -> BoxGridFuture<'_, TaskResult>;

    /// Requests cancellation of a task.
    ///
    /// Returns `Ok(true)` if the task was newly cancelled, or `Ok(false)` if it was already
    /// terminal or unknown.
    fn cancel_task(&self, task_id: TaskId) -> BoxGridFuture<'_, bool>;

    /// Queries cluster-wide queue metrics and worker inventory.
    fn cluster_status(&self) -> BoxGridFuture<'_, GridClusterStatus>;

    /// Lists workers serialized as JSON array elements.
    fn list_workers(&self) -> BoxGridFuture<'_, serde_json::Value>;

    /// Performs a bounded liveness check against the Master.
    fn health_check(&self) -> BoxGridFuture<'_, ()>;

    /// Closes any persistent transport resources held by the backend.
    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

/// [`GridBackend`] backed by an embedded in-process [`MasterHandle`] (`--grid-master`).
#[derive(Clone)]
pub struct InProcessBackend {
    master: MasterHandle,
}

impl InProcessBackend {
    /// Creates a new [`InProcessBackend`] wrapping `master`.
    pub fn new(master: MasterHandle) -> Self {
        Self { master }
    }

    /// Returns a reference to the underlying [`MasterHandle`].
    pub fn master(&self) -> &MasterHandle {
        &self.master
    }
}

impl GridBackend for InProcessBackend {
    fn submit_task(&self, task: Task, priority: u32) -> BoxGridFuture<'_, TaskId> {
        Box::pin(async move { self.master.submit_task_with_priority(task, priority).await })
    }

    fn submit_and_wait(
        &self,
        task: Task,
        priority: u32,
        timeout: Duration,
    ) -> BoxGridFuture<'_, TaskResult> {
        Box::pin(async move {
            let task_id = self
                .master
                .submit_task_with_priority(task, priority)
                .await?;
            self.master.wait_task(task_id, Some(timeout)).await
        })
    }

    fn get_task_status(&self, task_id: TaskId) -> BoxGridFuture<'_, GridTaskStatus> {
        Box::pin(async move {
            let info = self.master.get_task_info(task_id).await?;
            let checkpoint = self.master.get_checkpoint(&task_id).await;
            let detail = self.master.queue().get_status_detail(&task_id).await;
            let (status, error) = if let Some(ref d) = detail {
                match d.result_exit_code {
                    Some(0) => (TaskStatus::Completed, d.result_error.clone()),
                    Some(130) => (TaskStatus::Cancelled, d.result_error.clone()),
                    Some(_) => (TaskStatus::Failed, d.result_error.clone()),
                    None => (info.state.into(), info.error_message.clone()),
                }
            } else {
                (info.state.into(), info.error_message.clone())
            };
            let task_info = serde_json::to_value(&info).ok();
            Ok(GridTaskStatus {
                task_id,
                status,
                state_name: format!("{:?}", info.state),
                assigned_worker: info.assigned_worker_id,
                error,
                task_info,
                checkpoint,
            })
        })
    }

    fn get_task_result(&self, task_id: TaskId) -> BoxGridFuture<'_, GridTaskResult> {
        Box::pin(async move {
            let info = self.master.get_task_info(task_id).await?;
            if info.state.is_terminal() {
                if let Some(res) = self.master.queue().get_result(&task_id).await {
                    return Ok(GridTaskResult::Completed(res));
                }
            }
            let status = self.get_task_status(task_id).await?;
            Ok(GridTaskResult::Pending(status))
        })
    }

    fn wait_task(&self, task_id: TaskId, timeout: Duration) -> BoxGridFuture<'_, TaskResult> {
        Box::pin(async move { self.master.wait_task(task_id, Some(timeout)).await })
    }

    fn cancel_task(&self, task_id: TaskId) -> BoxGridFuture<'_, bool> {
        Box::pin(async move {
            let was_terminal = match self.master.get_task_info(task_id).await {
                Ok(info) => info.state.is_terminal(),
                Err(GridError::TaskNotFound(_)) => return Ok(false),
                Err(e) => return Err(e),
            };
            if was_terminal {
                return Ok(false);
            }
            match self.master.cancel_task(task_id).await {
                Ok(()) => match self.master.get_task_info(task_id).await {
                    Ok(info) => Ok(info.state == TaskState::Cancelled),
                    Err(GridError::TaskNotFound(_)) => Ok(false),
                    Err(e) => Err(e),
                },
                Err(GridError::TaskNotFound(_)) => Ok(false),
                Err(e) => Err(e),
            }
        })
    }

    fn cluster_status(&self) -> BoxGridFuture<'_, GridClusterStatus> {
        Box::pin(async move {
            let stats = self.master.queue_stats().await?;
            let workers = self.master.list_workers().await?;
            let active_workers = workers
                .iter()
                .filter(|w| w.status == WorkerStatus::Connected)
                .count();
            let (queued_tasks, running_tasks, completed_tasks, failed_tasks, cancelled_tasks) =
                stats.cluster_buckets();
            let queue_json = serde_json::to_value(&stats)?;
            let workers_json = serde_json::to_value(&workers)?;
            Ok(GridClusterStatus {
                active_workers,
                total_workers: workers.len(),
                queued_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                cancelled_tasks,
                queue_json,
                workers_json,
            })
        })
    }

    fn list_workers(&self) -> BoxGridFuture<'_, serde_json::Value> {
        Box::pin(async move {
            let workers = self.master.list_workers().await?;
            Ok(serde_json::to_value(&workers)?)
        })
    }

    fn health_check(&self) -> BoxGridFuture<'_, ()> {
        Box::pin(async move { self.master.queue_stats().await.map(|_| ()) })
    }
}
