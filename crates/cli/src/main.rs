//! OxideSwarm CLI binary (`oxideswarm`).
//!
//! Provides the primary command-line interface for orchestrating the OxideSwarm cluster:
//! - `oxideswarm master`: Starts coordinator server with TCP/P2P listener and Web UI.
//! - `oxideswarm worker`: Starts compute agent with hardware auto-detection & overrides.
//! - `oxideswarm submit`: Submits generic, GPU, or compilation workloads to the grid.
//! - `oxideswarm status`: Queries cluster metrics, task lifecycle state, or worker details.
//! - `oxideswarm workers`: Inspects active worker nodes advertising capabilities.
//! - `oxideswarm mapreduce`: Orchestrates in-memory distributed map-reduce jobs.

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    rusty_grid_cli::run_cli().await
}
