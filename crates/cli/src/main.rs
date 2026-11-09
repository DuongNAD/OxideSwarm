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

/// Stack size for the thread that runs the CLI's top-level future. `run_cli` builds one large
/// async state machine for every subcommand; on Windows the default 1 MiB main-thread stack is
/// too small for it in debug builds (the process aborted with "thread 'main' has overflowed its
/// stack"), so the future runs on a thread with an explicit 16 MiB stack instead.
const CLI_MAIN_STACK_BYTES: usize = 16 * 1024 * 1024;

fn main() -> ExitCode {
    let runner = std::thread::Builder::new()
        .name("oxideswarm-main".into())
        .stack_size(CLI_MAIN_STACK_BYTES)
        .spawn(|| {
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(e) => {
                    eprintln!("Error: cannot start the async runtime: {e}");
                    return ExitCode::FAILURE;
                }
            };
            runtime.block_on(Box::pin(rusty_grid_cli::run_cli()))
        });
    match runner {
        Ok(handle) => handle.join().unwrap_or(ExitCode::FAILURE),
        Err(e) => {
            eprintln!("Error: cannot start the main thread: {e}");
            ExitCode::FAILURE
        }
    }
}
