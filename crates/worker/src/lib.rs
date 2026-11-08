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
pub mod staging;
#[cfg(feature = "gpu-wgpu")]
pub mod wgpu_engine;

pub use backoff::{BackoffConfig, ExponentialBackoff};
pub use blob_cache::{BlobCache, CachePin, DEFAULT_BLOB_CACHE_MAX_BYTES};
pub use client::{WorkerClient, WorkerConfig};
pub use gpu::{GpuError, GpuNotice, NoticeLevel, WGPU_COMPILED};
pub use heartbeat::{HeartbeatHandle, HeartbeatTracker, MobileTelemetryHandle};
pub use isolation::{verify_isolation_support, HostDirs, IsolationLevel};
pub use process_tree::{terminate_all_task_process_trees, ProcessPriority};
pub use runner::{
    read_bounded_stream, RunnerConfig, TaskRunner, TaskRunnerError, DEFAULT_MAX_OUTPUT_BYTES,
    EXIT_CODE_CANCELLED, EXIT_CODE_GENERAL_ERROR, EXIT_CODE_INTERRUPTED, EXIT_CODE_SUCCESS,
    EXIT_CODE_TIMEOUT,
};
pub use sandbox::{sanitize_relative_path, Sandbox, SandboxConfig, SandboxError};
pub use staging::{OutputLimits, DEFAULT_MAX_OUTPUT_FILES, DEFAULT_OUTPUT_LIMIT_BYTES};

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
