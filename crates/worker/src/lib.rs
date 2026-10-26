//! Worker node agent for `rusty_grid`.
//!
//! Provides TCP client networking, hardware capability detection,
//! registration handshake, periodic heartbeat telemetry, and resilient backoff.

pub mod autorig;
pub mod backoff;
pub mod client;
pub mod cpu_simd;
pub mod gen3d;
pub mod gpu;
pub mod heartbeat;
pub mod identity;
pub mod job_object;
pub mod process_tree;
pub mod runner;
pub mod sandbox;
#[cfg(feature = "gpu-wgpu")]
pub mod wgpu_engine;

pub use backoff::{BackoffConfig, ExponentialBackoff};
pub use client::{WorkerClient, WorkerConfig};
pub use gpu::{GpuError, GpuNotice, NoticeLevel, WGPU_COMPILED};
pub use heartbeat::{HeartbeatHandle, HeartbeatTracker, MobileTelemetryHandle};
pub use process_tree::{terminate_all_task_process_trees, ProcessPriority};
pub use runner::{
    read_bounded_stream, RunnerConfig, TaskRunner, TaskRunnerError, DEFAULT_MAX_OUTPUT_BYTES,
    EXIT_CODE_CANCELLED, EXIT_CODE_GENERAL_ERROR, EXIT_CODE_INTERRUPTED, EXIT_CODE_SUCCESS,
    EXIT_CODE_TIMEOUT,
};
pub use sandbox::{sanitize_relative_path, Sandbox, SandboxConfig, SandboxError};

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
