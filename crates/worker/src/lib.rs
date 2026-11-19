//! Worker node agent for `rusty_grid`.
//!
//! Provides TCP client networking, hardware capability detection,
//! registration handshake, periodic heartbeat telemetry, and resilient backoff.

#[cfg(feature = "3d")]
pub mod autorig;
pub mod backoff;
pub mod blob_cache;
pub mod blob_client;
pub mod client;
pub mod controls;
pub mod cpu_simd;
#[cfg(feature = "3d")]
pub mod gen3d;
pub mod gpu;
pub mod heartbeat;
pub mod identity;
pub mod isolation;
pub mod process_tree;
pub mod runner;
pub mod sandbox;
pub mod sim;
pub mod staging;
#[cfg(feature = "gpu-wgpu")]
pub mod wgpu_engine;
pub mod workspace;

pub use backoff::{BackoffConfig, ExponentialBackoff};
pub use blob_cache::{BlobCache, CachePin, DEFAULT_BLOB_CACHE_MAX_BYTES};
pub use client::{
    compute_effective_resume_grace, WorkerClient, WorkerConfig, RESUME_SAFETY_MARGIN,
};
pub use controls::{
    check_watcher_status, default_control_file_path, is_pause_file_present, is_process_alive,
    remove_pause_file, remove_watch_file_if_owned, validate_daily_task_hours,
    validate_max_host_cpu_pct, watch_file_path, write_pause_file, write_watch_file,
    AllowHoursSchedule, AtomicHostCpuProbe, BlockReason, Clock, ContributorControlsConfig,
    HostCpuProbe, NetworkCost, NetworkCostProbe, PauseTransition, PowerProbe, PowerSource,
    SystemClock, WatcherStatus, WorkerControls, WorkerControlsHandle, PAUSE_STOP_TIMEOUT,
};
pub use gpu::{GpuError, GpuNotice, NoticeLevel, WGPU_COMPILED};
pub use heartbeat::{HeartbeatHandle, HeartbeatTracker, MobileTelemetryHandle};
pub use isolation::{
    validate_extra_read_only_dirs, verify_isolation_support, verify_isolation_support_with_config,
    HostDirs, IsolationLevel, ISOLATION_READ_DIRS_ENV,
};
pub use process_tree::{terminate_all_task_process_trees, ProcessPriority};
pub use runner::{
    read_bounded_stream, RunnerConfig, TaskRunner, TaskRunnerError, DEFAULT_MAX_OUTPUT_BYTES,
    EXIT_CODE_CANCELLED, EXIT_CODE_GENERAL_ERROR, EXIT_CODE_INTERRUPTED, EXIT_CODE_SUCCESS,
    EXIT_CODE_TIMEOUT,
};
pub use rusty_grid_core::{is_contributor_hold_cpu_pct, CONTRIBUTOR_HOLD_CPU_PCT};
pub use sandbox::{sanitize_relative_path, Sandbox, SandboxConfig, SandboxError};
pub use sim::{
    FakeRunnerConfig, LinkLossAction, SimulatedLinkConfig, SimulatedLinkStream,
    SimulatedNetworkControl, SimulatedNetworkSnapshot,
};
pub use staging::{OutputLimits, DEFAULT_MAX_OUTPUT_FILES, DEFAULT_OUTPUT_LIMIT_BYTES};
pub use workspace::{
    WorkspaceLimits, WorkspaceStore, DEFAULT_MAX_WORKSPACES, DEFAULT_WORKSPACE_LIMIT_MB,
};

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
