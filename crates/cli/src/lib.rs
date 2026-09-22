//! Library exports for `rusty_grid_cli`.

pub mod config;
pub mod mode_cli;

pub use mode_cli::{run_mode_cli, ModeCliArgs};
