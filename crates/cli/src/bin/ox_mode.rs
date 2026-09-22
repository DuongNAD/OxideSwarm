//! Standalone binary entry point for `ox-mode`.
//!
//! Provides direct access to OxideSwarm's workflow modes:
//! - `ox-mode test`: Testing & quality verification
//! - `ox-mode dev`: Development & local cluster
//! - `ox-mode doc`: Documentation & architecture spec
//! - `ox-mode research`: Research & performance benchmarking
//! - `ox-mode`: Interactive terminal selection menu

use clap::Parser;
use rusty_grid_cli::mode_cli::{run_mode_cli, ModeCliArgs};
use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    let args = ModeCliArgs::parse();
    let code = run_mode_cli(args).await;
    if code == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(code as u8)
    }
}
