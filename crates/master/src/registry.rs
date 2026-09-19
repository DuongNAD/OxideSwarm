//! Thread-safe worker registry tracking registered worker nodes, capabilities,
//! operational status, and connection sessions.

use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::task::AbortHandle;
use uuid::Uuid;

use rusty_grid_core::{GridError, GridResult, MasterMessage, TaskRequirements, WorkerCapabilities};

/// Operational lifecycle state of a registered Worker node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerStatus {
    /// Connected, healthy, and idle (ready to accept new tasks).
    Connected,
    /// Connected and executing tasks up to active capacity.
    Busy,
    /// Connection severed, heartbeat timed out, or unregister in progress.
    Disconnected,
}

/// Internal mutable state for a single connected worker.
pub struct WorkerEntry {
    pub worker_id: Uuid,
    pub session_id: u64,
    pub capabilities: WorkerCapabilities,
    pub status: WorkerStatus,
    /// Monotonic timestamp for accurate heartbeat timeout calculation (immune to NTP skew).
    pub last_heartbeat_instant: Instant,
    /// Wall-clock epoch timestamp (seconds) reported by worker.
    pub last_heartbeat_timestamp: u64,
    pub registered_at_instant: Instant,
    pub registered_timestamp: u64,
    pub active_tasks: usize,
    pub remote_addr: SocketAddr,
    /// Outbound channel sender to worker's writer task.
    pub sender: mpsc::Sender<MasterMessage>,
    /// Optional abort handle to terminate worker connection tasks when unregistering/reaping.
    pub abort_handle: Option<AbortHandle>,
    pub cpu_usage_pct: f32,
    pub ram_available_mb: u64,
}

/// Serializable public snapshot of worker metadata for CLI queries and monitoring.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub worker_id: Uuid,
    pub session_id: u64,
    pub capabilities: WorkerCapabilities,
    pub status: WorkerStatus,
    pub last_heartbeat_timestamp: u64,
    pub registered_timestamp: u64,
    pub active_tasks: usize,
    pub remote_addr: String,
    #[serde(default)]
    pub cpu_usage_pct: f32,
    #[serde(default)]
    pub ram_available_mb: u64,
}

impl WorkerInfo {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        worker_id: Uuid,
        session_id: u64,
        capabilities: WorkerCapabilities,
        status: WorkerStatus,
        last_heartbeat_timestamp: u64,
        registered_timestamp: u64,
        active_tasks: usize,
        remote_addr: String,
    ) -> Self {
        let ram = capabilities.ram_mb;
        Self {
            worker_id,
            session_id,
            capabilities,
            status,
            last_heartbeat_timestamp,
            registered_timestamp,
            active_tasks,
            remote_addr,
            cpu_usage_pct: 0.0,
            ram_available_mb: ram,
        }
    }
}

impl WorkerEntry {
    pub fn to_info(&self) -> WorkerInfo {
        WorkerInfo {
            worker_id: self.worker_id,
            session_id: self.session_id,
            capabilities: self.capabilities.clone(),
            status: self.status,
            last_heartbeat_timestamp: self.last_heartbeat_timestamp,
            registered_timestamp: self.registered_timestamp,
            active_tasks: self.active_tasks,
            remote_addr: self.remote_addr.to_string(),
            cpu_usage_pct: self.cpu_usage_pct,
            ram_available_mb: self.ram_available_mb,
        }
    }
}

struct WorkerRegistryInner {
    workers: HashMap<Uuid, WorkerEntry>,
    next_session_id: u64,
}

/// Central in-memory registry of all worker nodes connected to the Master.
///
/// Backed by an `Arc<RwLock<...>>`, making it cheaply cloneable across async tasks.
#[derive(Clone)]
pub struct WorkerRegistry {
    inner: Arc<RwLock<WorkerRegistryInner>>,
}

impl Default for WorkerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerRegistry {
    /// Creates a new, empty worker registry.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(WorkerRegistryInner {
                workers: HashMap::new(),
                next_session_id: 1,
            })),
        }
    }

    /// Registers a new worker or reconnects an existing worker.
    ///
    /// Allocates a new monotonic `session_id`. If an existing connection for `worker_id`
    /// was active, its old abort handle is cancelled to prevent race conditions.
    /// Returns the newly assigned `session_id`.
    pub async fn register(
        &self,
        worker_id: Uuid,
        capabilities: WorkerCapabilities,
        remote_addr: SocketAddr,
        sender: mpsc::Sender<MasterMessage>,
        abort_handle: Option<AbortHandle>,
    ) -> GridResult<u64> {
        let mut inner = self.inner.write().await;
        let session_id = inner.next_session_id;
        inner.next_session_id += 1;

        let now_instant = Instant::now();
        let now_epoch = Utc::now().timestamp() as u64;

        if let Some(existing) = inner.workers.get_mut(&worker_id) {
            // Cancel old connection tasks if still running
            if let Some(old_abort) = existing.abort_handle.take() {
                old_abort.abort();
            }
            let ram = capabilities.ram_mb;
            existing.session_id = session_id;
            existing.capabilities = capabilities;
            existing.status = WorkerStatus::Connected;
            existing.last_heartbeat_instant = now_instant;
            existing.last_heartbeat_timestamp = now_epoch;
            existing.remote_addr = remote_addr;
            existing.sender = sender;
            existing.abort_handle = abort_handle;
            existing.ram_available_mb = ram;
            tracing::info!(worker_id = %worker_id, session_id = session_id, "Rebound existing worker registration");
        } else {
            let ram = capabilities.ram_mb;
            let entry = WorkerEntry {
                worker_id,
                session_id,
                capabilities,
                status: WorkerStatus::Connected,
                last_heartbeat_instant: now_instant,
                last_heartbeat_timestamp: now_epoch,
                registered_at_instant: now_instant,
                registered_timestamp: now_epoch,
                active_tasks: 0,
                remote_addr,
                sender,
                abort_handle,
                cpu_usage_pct: 0.0,
                ram_available_mb: ram,
            };
            inner.workers.insert(worker_id, entry);
            tracing::info!(worker_id = %worker_id, session_id = session_id, "Registered new worker");
        }

        Ok(session_id)
    }

    /// Marks a worker as disconnected.
    ///
    /// If `expected_session` is supplied, unregisters only if the worker's current
    /// `session_id` matches. This prevents stale connection disconnects from unregistering
    /// a worker that has already reconnected on a newer session.
    pub async fn unregister(
        &self,
        worker_id: &Uuid,
        expected_session: Option<u64>,
    ) -> GridResult<bool> {
        let mut inner = self.inner.write().await;
        if let Some(entry) = inner.workers.get_mut(worker_id) {
            if let Some(expected) = expected_session {
                if entry.session_id != expected {
                    tracing::debug!(
                        worker_id = %worker_id,
                        expected = expected,
                        current = entry.session_id,
                        "Ignoring unregister from stale session"
                    );
                    return Ok(false);
                }
            }
            entry.status = WorkerStatus::Disconnected;
            if let Some(abort) = entry.abort_handle.take() {
                abort.abort();
            }
            tracing::info!(worker_id = %worker_id, "Worker marked as Disconnected");
            return Ok(true);
        }
        Ok(false)
    }

    /// Convenience wrapper to unconditionally mark a worker as Disconnected.
    pub async fn mark_disconnected(&self, worker_id: &Uuid) -> GridResult<bool> {
        self.unregister(worker_id, None).await
    }

    /// Updates worker status with session validation.
    pub async fn set_status(
        &self,
        worker_id: &Uuid,
        status: WorkerStatus,
        expected_session: Option<u64>,
    ) -> GridResult<()> {
        let mut inner = self.inner.write().await;
        let entry = inner
            .workers
            .get_mut(worker_id)
            .ok_or(GridError::WorkerNotFound(*worker_id))?;
        if let Some(expected) = expected_session {
            if entry.session_id != expected {
                return Ok(());
            }
        }
        entry.status = status;
        Ok(())
    }

    /// Records heartbeat update, refreshing monotonic timestamp, epoch timestamp, active tasks, and telemetry.
    pub async fn record_heartbeat(
        &self,
        worker_id: &Uuid,
        active_tasks: usize,
        timestamp: u64,
        cpu_usage_pct: f32,
        ram_available_mb: u64,
        expected_session: Option<u64>,
    ) -> GridResult<()> {
        let mut inner = self.inner.write().await;
        let entry = inner
            .workers
            .get_mut(worker_id)
            .ok_or(GridError::WorkerNotFound(*worker_id))?;
        if let Some(expected) = expected_session {
            if entry.session_id != expected {
                tracing::debug!(
                    worker_id = %worker_id,
                    expected = expected,
                    current = entry.session_id,
                    "Ignoring heartbeat from stale session"
                );
                return Ok(());
            }
        }
        entry.last_heartbeat_instant = Instant::now();
        entry.last_heartbeat_timestamp = timestamp;
        entry.active_tasks = active_tasks;
        entry.cpu_usage_pct = cpu_usage_pct;
        entry.ram_available_mb = ram_available_mb;
        if entry.status != WorkerStatus::Disconnected {
            entry.status = if active_tasks > 0 {
                WorkerStatus::Busy
            } else {
                WorkerStatus::Connected
            };
        }
        Ok(())
    }

    /// Atomically increments the active task count for a worker and updates status to Busy.
    pub async fn increment_active_tasks(&self, worker_id: &Uuid) -> GridResult<usize> {
        let mut inner = self.inner.write().await;
        let entry = inner
            .workers
            .get_mut(worker_id)
            .ok_or(GridError::WorkerNotFound(*worker_id))?;
        entry.active_tasks += 1;
        if entry.status == WorkerStatus::Connected {
            entry.status = WorkerStatus::Busy;
        }
        Ok(entry.active_tasks)
    }

    /// Atomically decrements the active task count for a worker and updates status to Connected if 0.
    pub async fn decrement_active_tasks(&self, worker_id: &Uuid) -> GridResult<usize> {
        let mut inner = self.inner.write().await;
        let entry = inner
            .workers
            .get_mut(worker_id)
            .ok_or(GridError::WorkerNotFound(*worker_id))?;
        entry.active_tasks = entry.active_tasks.saturating_sub(1);
        if entry.status == WorkerStatus::Busy && entry.active_tasks == 0 {
            entry.status = WorkerStatus::Connected;
        }
        Ok(entry.active_tasks)
    }

    /// Retrieves a single worker's metadata snapshot.
    pub async fn get_worker(&self, worker_id: Uuid) -> Option<WorkerInfo> {
        let inner = self.inner.read().await;
        inner.workers.get(&worker_id).map(|e| e.to_info())
    }

    /// Returns snapshots of all registered workers regardless of operational status.
    pub async fn list_all_workers(&self) -> Vec<WorkerInfo> {
        let inner = self.inner.read().await;
        inner.workers.values().map(|e| e.to_info()).collect()
    }

    /// Alias for `list_all_workers`.
    pub async fn list_workers(&self) -> Vec<WorkerInfo> {
        self.list_all_workers().await
    }

    /// Returns active (Connected or Busy) workers.
    pub async fn list_active_workers(&self) -> Vec<WorkerInfo> {
        let inner = self.inner.read().await;
        inner
            .workers
            .values()
            .filter(|e| e.status != WorkerStatus::Disconnected)
            .map(|e| e.to_info())
            .collect()
    }

    /// Returns available (Connected and idle) workers ready for task assignment.
    pub async fn list_available_workers(&self) -> Vec<WorkerInfo> {
        let inner = self.inner.read().await;
        inner
            .workers
            .values()
            .filter(|e| e.status == WorkerStatus::Connected)
            .map(|e| e.to_info())
            .collect()
    }

    /// Returns active workers possessing GPU compute capabilities (physical or simulated).
    pub async fn list_gpu_workers(&self) -> Vec<WorkerInfo> {
        let inner = self.inner.read().await;
        inner
            .workers
            .values()
            .filter(|e| e.status != WorkerStatus::Disconnected && e.capabilities.can_execute_gpu())
            .map(|e| e.to_info())
            .collect()
    }

    /// Returns active workers that do NOT have GPU capabilities (CPU-only).
    pub async fn list_non_gpu_workers(&self) -> Vec<WorkerInfo> {
        let inner = self.inner.read().await;
        inner
            .workers
            .values()
            .filter(|e| e.status != WorkerStatus::Disconnected && !e.capabilities.can_execute_gpu())
            .map(|e| e.to_info())
            .collect()
    }

    /// Finds active workers satisfying the specific requirements of a task.
    pub async fn find_eligible_workers(&self, requirements: &TaskRequirements) -> Vec<WorkerInfo> {
        let inner = self.inner.read().await;
        inner
            .workers
            .values()
            .filter(|e| {
                e.status != WorkerStatus::Disconnected && e.capabilities.satisfies(requirements)
            })
            .map(|e| e.to_info())
            .collect()
    }

    /// Sends a MasterMessage to a specific worker without holding the registry lock during transmission.
    pub async fn send_to_worker(&self, worker_id: &Uuid, msg: MasterMessage) -> GridResult<()> {
        let sender = {
            let inner = self.inner.read().await;
            let entry = inner
                .workers
                .get(worker_id)
                .ok_or(GridError::WorkerNotFound(*worker_id))?;
            if entry.status == WorkerStatus::Disconnected {
                return Err(GridError::ConnectionClosed);
            }
            entry.sender.clone()
        };

        sender
            .send(msg)
            .await
            .map_err(|_| GridError::ConnectionClosed)?;
        Ok(())
    }

    /// Broadcasts a MasterMessage to all active workers. Returns the count of successful transmissions.
    pub async fn broadcast(&self, msg: MasterMessage) -> GridResult<usize> {
        let senders: Vec<(Uuid, mpsc::Sender<MasterMessage>)> = {
            let inner = self.inner.read().await;
            inner
                .workers
                .iter()
                .filter(|(_, e)| e.status != WorkerStatus::Disconnected)
                .map(|(id, e)| (*id, e.sender.clone()))
                .collect()
        };

        let mut sent_count = 0;
        for (worker_id, sender) in senders {
            if sender.send(msg.clone()).await.is_ok() {
                sent_count += 1;
            } else {
                let _ = self.unregister(&worker_id, None).await;
            }
        }
        Ok(sent_count)
    }

    /// Scans active workers and transitions any exceeding `timeout` to Disconnected.
    ///
    /// Implements a two-phase check (read lock candidate search followed by write lock double-check)
    /// to avoid unnecessary lock contention.
    /// Returns the list of newly reaped worker UUIDs.
    pub async fn reap_stale_workers(&self, timeout: Duration) -> Vec<Uuid> {
        let now = Instant::now();

        // Phase 1: Read lock candidate identification
        let candidates: Vec<Uuid> = {
            let inner = self.inner.read().await;
            inner
                .workers
                .values()
                .filter(|entry| {
                    entry.status != WorkerStatus::Disconnected
                        && now.duration_since(entry.last_heartbeat_instant) > timeout
                })
                .map(|entry| entry.worker_id)
                .collect()
        };

        if candidates.is_empty() {
            return Vec::new();
        }

        // Phase 2: Targeted write lock mutation
        let mut reaped = Vec::with_capacity(candidates.len());
        let mut inner = self.inner.write().await;

        for id in candidates {
            if let Some(entry) = inner.workers.get_mut(&id) {
                let elapsed = now.duration_since(entry.last_heartbeat_instant);
                // Double check to prevent race condition with concurrent heartbeat
                if entry.status != WorkerStatus::Disconnected && elapsed > timeout {
                    entry.status = WorkerStatus::Disconnected;
                    if let Some(abort) = entry.abort_handle.take() {
                        abort.abort();
                    }
                    tracing::warn!(
                        worker_id = %id,
                        worker_name = %entry.capabilities.name,
                        last_seen_secs = elapsed.as_secs(),
                        timeout_secs = timeout.as_secs(),
                        "Worker heartbeat timed out; transitioned to Disconnected"
                    );
                    reaped.push(id);
                }
            }
        }

        reaped
    }

    /// Alias for `reap_stale_workers`.
    pub async fn reap_timed_out_workers(&self, timeout: Duration) -> Vec<Uuid> {
        self.reap_stale_workers(timeout).await
    }

    /// Returns cluster counts: `(total_registered, active_connected_or_busy, active_gpu)`.
    pub async fn counts(&self) -> (usize, usize, usize) {
        let inner = self.inner.read().await;
        let total = inner.workers.len();
        let active = inner
            .workers
            .values()
            .filter(|e| e.status != WorkerStatus::Disconnected)
            .count();
        let gpu = inner
            .workers
            .values()
            .filter(|e| e.status != WorkerStatus::Disconnected && e.capabilities.can_execute_gpu())
            .count();
        (total, active, gpu)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_registry_registration_and_status() {
        let registry = WorkerRegistry::new();
        let (tx, _rx) = mpsc::channel(16);
        let worker_id = Uuid::new_v4();
        let caps = WorkerCapabilities::new("worker-1", 4, 8192, false, false, None);
        let addr = "127.0.0.1:9001".parse().unwrap();

        let session_1 = registry
            .register(worker_id, caps.clone(), addr, tx.clone(), None)
            .await
            .unwrap();
        assert_eq!(session_1, 1);

        let info = registry.get_worker(worker_id).await.unwrap();
        assert_eq!(info.status, WorkerStatus::Connected);
        assert_eq!(info.capabilities.cpu_cores, 4);
        assert_eq!(info.active_tasks, 0);

        // Heartbeat with active tasks transitions to Busy
        registry
            .record_heartbeat(&worker_id, 2, 1000, 45.0, 4096, Some(session_1))
            .await
            .unwrap();
        let info = registry.get_worker(worker_id).await.unwrap();
        assert_eq!(info.status, WorkerStatus::Busy);
        assert_eq!(info.active_tasks, 2);
        assert_eq!(info.cpu_usage_pct, 45.0);
        assert_eq!(info.ram_available_mb, 4096);

        // Heartbeat back to 0 tasks transitions to Connected
        registry
            .record_heartbeat(&worker_id, 0, 1003, 5.0, 7000, Some(session_1))
            .await
            .unwrap();
        let info = registry.get_worker(worker_id).await.unwrap();
        assert_eq!(info.status, WorkerStatus::Connected);
        assert_eq!(info.cpu_usage_pct, 5.0);
        assert_eq!(info.ram_available_mb, 7000);
    }

    #[tokio::test]
    async fn test_registry_session_isolation_on_reconnect() {
        let registry = WorkerRegistry::new();
        let (tx1, _rx1) = mpsc::channel(16);
        let (tx2, _rx2) = mpsc::channel(16);
        let worker_id = Uuid::new_v4();
        let caps = WorkerCapabilities::new("worker-1", 4, 8192, false, false, None);
        let addr = "127.0.0.1:9001".parse().unwrap();

        let session_1 = registry
            .register(worker_id, caps.clone(), addr, tx1, None)
            .await
            .unwrap();

        // Worker reconnects with session 2
        let session_2 = registry
            .register(worker_id, caps.clone(), addr, tx2, None)
            .await
            .unwrap();
        assert!(session_2 > session_1);

        // Stale unregister from session 1 should be ignored
        let unreg_stale = registry
            .unregister(&worker_id, Some(session_1))
            .await
            .unwrap();
        assert!(!unreg_stale);
        assert_eq!(
            registry.get_worker(worker_id).await.unwrap().status,
            WorkerStatus::Connected
        );

        // Unregister with session 2 should succeed
        let unreg_valid = registry
            .unregister(&worker_id, Some(session_2))
            .await
            .unwrap();
        assert!(unreg_valid);
        assert_eq!(
            registry.get_worker(worker_id).await.unwrap().status,
            WorkerStatus::Disconnected
        );
    }

    #[tokio::test]
    async fn test_registry_gpu_capability_filtering() {
        let registry = WorkerRegistry::new();
        let (tx, _rx) = mpsc::channel(16);

        let cpu_id = Uuid::new_v4();
        let cpu_caps = WorkerCapabilities::new("cpu-worker", 4, 4096, false, false, None);
        registry
            .register(
                cpu_id,
                cpu_caps,
                "127.0.0.1:9001".parse().unwrap(),
                tx.clone(),
                None,
            )
            .await
            .unwrap();

        let gpu_id = Uuid::new_v4();
        let gpu_caps = WorkerCapabilities::new(
            "gpu-worker",
            8,
            16384,
            true,
            true,
            Some("Simulated GPU".into()),
        );
        registry
            .register(
                gpu_id,
                gpu_caps,
                "127.0.0.1:9002".parse().unwrap(),
                tx.clone(),
                None,
            )
            .await
            .unwrap();

        let gpu_workers = registry.list_gpu_workers().await;
        assert_eq!(gpu_workers.len(), 1);
        assert_eq!(gpu_workers[0].worker_id, gpu_id);

        let non_gpu = registry.list_non_gpu_workers().await;
        assert_eq!(non_gpu.len(), 1);
        assert_eq!(non_gpu[0].worker_id, cpu_id);

        let (total, active, gpu_count) = registry.counts().await;
        assert_eq!(total, 2);
        assert_eq!(active, 2);
        assert_eq!(gpu_count, 1);
    }
}
