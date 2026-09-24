//! Backward-compatible binary alias (`rusty-grid`) for OxideSwarm CLI.
//!
//! Provides the primary command-line interface for orchestrating the OxideSwarm cluster:
//! - `rusty-grid master`: Starts coordinator server with TCP/P2P listener and Web UI.
//! - `rusty-grid worker`: Starts compute agent with hardware auto-detection & overrides.
//! - `rusty-grid submit`: Submits generic, GPU, or compilation workloads to the grid.
//! - `rusty-grid status`: Queries cluster metrics, task lifecycle state, or worker details.
//! - `rusty-grid workers`: Inspects active worker nodes advertising capabilities.
//! - `rusty-grid mapreduce`: Orchestrates in-memory distributed map-reduce jobs.

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    rusty_grid_cli::run_cli().await
}
