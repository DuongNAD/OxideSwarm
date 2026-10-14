//! Master node server, worker registry, task queue, scheduler, and heartbeat reaper for `rusty_grid`.

pub mod auth;
#[cfg(feature = "dashboard")]
pub mod dashboard;
pub mod liveness;
pub mod mapreduce;
pub mod queue;
pub mod reaper;
pub mod registry;
pub mod scheduler;
pub mod server;
#[cfg(feature = "dashboard")]
pub mod web_ui;

pub use auth::{
    dashboard_admin_token, resolve_dashboard_auth, ApiToken, AuthConfig, AuthIdentity, AuthScope,
    DevBypass,
};

pub use liveness::{RxClock, StampedStream};
pub use mapreduce::MapReduceEngine;
pub use queue::{
    is_worker_rejection, AssignedLoad, CancelOutcome, DelayedRetryKey, QueueOrderKey, QueueStats,
    ResultDisposition, ResultOutcome, RetryAttempt, RetryPolicy, TaskEntry, TaskInfo, TaskQueue,
    TaskState, TaskView, DEFAULT_MAX_TERMINAL_BYTES, DEFAULT_MAX_TERMINAL_TASKS,
};
pub use reaper::{spawn_reaper, spawn_reaper_with_queue, ReaperConfig};
pub use registry::{
    HeartbeatOutcome, Registration, WorkerEntry, WorkerInfo, WorkerRegistry, WorkerStatus,
};
pub use scheduler::{
    ScheduleAssignment, ScheduleReport, ScheduleSkipReason, SchedulerConfig, SchedulingPolicy,
    WorkloadScheduler,
};
pub use server::{
    handle_task_progress, handle_task_result, handle_worker_disconnect, remove_port_file,
    write_port_file, MasterHandle, MasterServer, ServerConfig, WaiterMap,
};

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
