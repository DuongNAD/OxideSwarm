//! Library exports for `rusty_grid_cli`.

#![deny(unsafe_code)]

pub mod blobs;
pub mod cli_app;
pub mod config;
pub mod connect;
pub mod doctor;
// LV-4: Windows console mode / Unix termios calls for no-echo password prompts.
#[allow(unsafe_code)]
pub mod join;
pub mod mcp;
pub mod members_cmd;
pub mod mode_cli;
pub mod pair;
pub mod rendezvous_cmd;
pub mod run_summary;
pub mod secret_cmd;
pub mod size_check;
pub mod usb_kit;
pub mod verify;
pub mod wordlist;

pub use cli_app::run_cli;
pub use connect::{connect_master, DEFAULT_MASTER_ADDR, DEFAULT_MASTER_PORT};
pub use mcp::{run_mcp, McpArgs};
pub use mode_cli as mode;
pub use mode_cli::{run_mode_cli, ModeCliArgs};
