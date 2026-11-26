//! Worker node agent for `rusty_grid`.
//!
//! Provides TCP client networking, hardware capability detection,
//! registration handshake, periodic heartbeat telemetry, and resilient backoff.

#![deny(unsafe_code)]

#[cfg(feature = "3d")]
pub mod autorig;
pub mod backoff;
// LV-4: cfg(windows)/cfg(unix) volume device identity and link-count syscalls to reject hard links/reparse points.
#[allow(unsafe_code)]
pub mod blob_cache;
pub mod blob_client;
pub mod client;
pub mod control;
pub mod cpu_simd;
#[cfg(feature = "3d")]
pub mod gen3d;
pub mod gpu;
pub mod heartbeat;
pub mod identity;
// LV-4: OS sandbox primitives (Windows AppContainer/ACLs/Desktop/CreateProcessW, Linux namespaces/seccomp, macOS sandbox_init).
#[allow(unsafe_code)]
pub mod isolation;
// LV-4: cfg(windows) Job Objects / snapshots and cfg(unix) setsid/killpg/proc_pidinfo process-tree supervision.
#[allow(unsafe_code)]
pub mod process_tree;
pub mod runner;
// LV-4: cfg(windows)/cfg(unix) symlink/reparse and file-identity checks for sandbox directories.
#[allow(unsafe_code)]
pub mod sandbox;
pub mod sim;
pub mod staging;
#[cfg(feature = "gpu-wgpu")]
pub mod wgpu_engine;
// LV-4: cfg(windows) HANDLE DACL/path verification syscalls for workspace-root protection.
#[allow(unsafe_code)]
pub mod workspace;

pub use backoff::{BackoffConfig, ExponentialBackoff};
pub use blob_cache::{BlobCache, CachePin, DEFAULT_BLOB_CACHE_MAX_BYTES};
pub use client::{
    compute_effective_resume_grace, WorkerClient, WorkerConfig, RESUME_SAFETY_MARGIN,
};
pub use control::{
    ControlEndpoint, ControlHandle, ControlRequest, ControlResponse, ControlStatusSnapshot,
    LimitPersistHook, WorkerControlState, WorkerLimitsConfig,
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
