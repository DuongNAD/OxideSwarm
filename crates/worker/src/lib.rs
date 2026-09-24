//! Worker node agent for `rusty_grid`.
//!
//! Provides TCP client networking, hardware capability detection,
//! registration handshake, periodic heartbeat telemetry, and resilient backoff.

pub mod backoff;
pub mod client;
pub mod cpu_simd;
pub mod heartbeat;
pub mod runner;
pub mod sandbox;
#[cfg(feature = "gpu-wgpu")]
pub mod wgpu_engine;

pub use backoff::{BackoffConfig, ExponentialBackoff};
pub use client::{WorkerClient, WorkerConfig};
pub use heartbeat::{HeartbeatHandle, HeartbeatTracker};
pub use runner::{
    read_bounded_stream, RunnerConfig, TaskRunner, TaskRunnerError, DEFAULT_MAX_OUTPUT_BYTES,
    EXIT_CODE_CANCELLED, EXIT_CODE_GENERAL_ERROR, EXIT_CODE_SUCCESS, EXIT_CODE_TIMEOUT,
};
pub use sandbox::{sanitize_relative_path, Sandbox, SandboxConfig, SandboxError};

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
