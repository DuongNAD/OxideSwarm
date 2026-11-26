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
pub mod matrix;
pub mod mcp;
pub mod members_cmd;
pub mod mode_cli;
pub mod pair;
pub mod pr_body;
pub mod rendezvous_cmd;
pub mod run_summary;
pub mod secret_cmd;
pub mod shard;
pub mod size_check;
pub mod usb_kit;
pub mod verify;
pub mod wordlist;

pub use cli_app::run_cli;
pub use connect::{connect_master, DEFAULT_MASTER_ADDR, DEFAULT_MASTER_PORT};
pub use mcp::{run_mcp, McpArgs};
pub use mode_cli as mode;
pub use mode_cli::{run_mode_cli, ModeCliArgs};

/// Process-wide test synchronization and panic-safe environment guards for `rusty_grid_cli` tests
/// that mutate or depend on `std::env` variables (`TEST-94`).
#[cfg(test)]
pub(crate) mod test_env_lock {
    use std::ffi::{OsStr, OsString};
    use std::sync::OnceLock;
    use tokio::sync::{Mutex, MutexGuard};

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    /// Serializes async unit tests that read or mutate process-wide environment variables.
    pub(crate) async fn lock_async() -> MutexGuard<'static, ()> {
        ENV_LOCK.get_or_init(|| Mutex::new(())).lock().await
    }

    /// Serializes synchronous unit tests that read or mutate process-wide environment variables.
    ///
    /// Call only outside an active Tokio runtime; async tests must use [`lock_async`].
    pub(crate) fn lock_blocking() -> MutexGuard<'static, ()> {
        ENV_LOCK.get_or_init(|| Mutex::new(())).blocking_lock()
    }

    /// Sets or removes a process-wide environment variable and restores its previous value on drop.
    #[must_use = "keep the ScopedEnvVar guard alive until the env mutation scope ends"]
    pub(crate) struct ScopedEnvVar {
        key: &'static str,
        prev: Option<OsString>,
    }

    impl ScopedEnvVar {
        pub(crate) fn set(key: &'static str, val: impl AsRef<OsStr>) -> Self {
            let prev = std::env::var_os(key);
            std::env::set_var(key, val);
            Self { key, prev }
        }

        pub(crate) fn remove(key: &'static str) -> Self {
            let prev = std::env::var_os(key);
            std::env::remove_var(key);
            Self { key, prev }
        }
    }

    impl Drop for ScopedEnvVar {
        fn drop(&mut self) {
            match self.prev.take() {
                Some(v) => std::env::set_var(self.key, v),
                None => std::env::remove_var(self.key),
            }
        }
    }
}
