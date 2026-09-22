//! Master node server, worker registry, task queue, scheduler, and heartbeat reaper for `rusty_grid`.

#[cfg(feature = "dashboard")]
pub mod dashboard;
pub mod mapreduce;
pub mod queue;
pub mod reaper;
pub mod registry;
pub mod scheduler;
pub mod server;
pub mod web_ui;

pub use mapreduce::MapReduceEngine;
pub use queue::{
    DelayedRetryKey, QueueOrderKey, QueueStats, RetryAttempt, RetryPolicy, TaskEntry, TaskInfo,
    TaskQueue, TaskState,
};
pub use reaper::{spawn_reaper, spawn_reaper_with_queue, ReaperConfig};
pub use registry::{WorkerEntry, WorkerInfo, WorkerRegistry, WorkerStatus};
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
