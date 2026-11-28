//! Local IPC client wrapper for the worker's same-user control endpoint (`TRAY-1`).

use rusty_grid_worker::control::{
    send_control_request, ControlEndpoint, ControlRequest, ControlResponse, ControlStatusSnapshot,
    WorkerLimitsConfig,
};
use std::path::{Path, PathBuf};
use std::time::Duration;

const CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(3);

/// Resolves the active OxideSwarm home directory (`OXIDESWARM_HOME` or `<HOME>/.oxideswarm`).
pub fn resolve_oxideswarm_home() -> Option<PathBuf> {
    if let Some(explicit) = std::env::var_os("OXIDESWARM_HOME").filter(|v| !v.is_empty()) {
        return Some(PathBuf::from(explicit));
    }
    #[cfg(windows)]
    let home = std::env::var_os("USERPROFILE").or_else(|| std::env::var_os("HOME"));
    #[cfg(not(windows))]
    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"));
    home.filter(|v| !v.is_empty())
        .map(|h| PathBuf::from(h).join(".oxideswarm"))
}

/// Returns the default log directory used for worker logs and "Open logs".
pub fn default_log_dir() -> PathBuf {
    resolve_oxideswarm_home()
        .unwrap_or_else(std::env::temp_dir)
        .join("logs")
}

/// Thin async client over [`ControlEndpoint`] with bounded timeouts.
#[derive(Debug, Clone)]
pub struct ControlClient {
    endpoint: ControlEndpoint,
    timeout: Duration,
}

impl ControlClient {
    /// Resolves the control endpoint matching the optional `--control-socket` and `--name` flags.
    pub fn resolve(explicit_socket: Option<&Path>, worker_name: Option<&str>) -> Option<Self> {
        ControlEndpoint::resolve(explicit_socket, worker_name).map(Self::new)
    }

    /// Creates a client targeting an already-resolved [`ControlEndpoint`].
    pub fn new(endpoint: ControlEndpoint) -> Self {
        Self {
            endpoint,
            timeout: CONTROL_RPC_TIMEOUT,
        }
    }

    /// Human-readable control endpoint description.
    pub fn display_target(&self) -> String {
        self.endpoint.display_target()
    }

    async fn call(&self, req: ControlRequest) -> Result<ControlResponse, String> {
        let resp = tokio::time::timeout(self.timeout, send_control_request(&self.endpoint, &req))
            .await
            .map_err(|_| format!("control request timed out after {:?}", self.timeout))??;
        if let ControlResponse::Error { message } = resp {
            return Err(message);
        }
        Ok(resp)
    }

    /// Fetches the current [`ControlStatusSnapshot`].
    pub async fn poll_status(&self) -> Result<ControlStatusSnapshot, String> {
        match self.call(ControlRequest::Status).await? {
            ControlResponse::Status { snapshot } | ControlResponse::Ok { snapshot, .. } => {
                Ok(snapshot)
            }
            other => Err(format!("unexpected status response: {other:?}")),
        }
    }

    /// Requests immediate pause, returning the updated snapshot.
    pub async fn pause(
        &self,
        until_epoch_ms: Option<u64>,
        reason: Option<String>,
    ) -> Result<ControlStatusSnapshot, String> {
        match self
            .call(ControlRequest::Pause {
                until_epoch_ms,
                reason,
            })
            .await?
        {
            ControlResponse::Ok { snapshot, .. } | ControlResponse::Status { snapshot } => {
                Ok(snapshot)
            }
            other => Err(format!("unexpected pause response: {other:?}")),
        }
    }

    /// Requests resume, returning the updated snapshot.
    pub async fn resume(&self) -> Result<ControlStatusSnapshot, String> {
        match self.call(ControlRequest::Resume).await? {
            ControlResponse::Ok { snapshot, .. } | ControlResponse::Status { snapshot } => {
                Ok(snapshot)
            }
            other => Err(format!("unexpected resume response: {other:?}")),
        }
    }

    /// Fetches persisted worker limits.
    pub async fn limits_get(&self) -> Result<(WorkerLimitsConfig, Option<String>), String> {
        match self.call(ControlRequest::LimitsGet).await? {
            ControlResponse::Limits {
                limits,
                config_path,
            } => Ok((limits, config_path)),
            other => Err(format!("unexpected limits_get response: {other:?}")),
        }
    }

    /// Updates a single worker limit key via `limits_set`.
    pub async fn limits_set(
        &self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(WorkerLimitsConfig, Option<String>), String> {
        match self
            .call(ControlRequest::LimitsSet {
                key: key.into(),
                value: value.into(),
            })
            .await?
        {
            ControlResponse::Limits {
                limits,
                config_path,
            } => Ok((limits, config_path)),
            other => Err(format!("unexpected limits_set response: {other:?}")),
        }
    }
}
