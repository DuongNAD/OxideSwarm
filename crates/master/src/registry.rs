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

use rusty_grid_core::capabilities::{MobileCapabilities, MobileLiveStatus};
use rusty_grid_core::{GridError, GridResult, MasterMessage, TaskRequirements, WorkerCapabilities};

use crate::liveness::RxClock;

/// A connection whose heartbeat is overdue but whose socket still delivers bytes (a large
/// `TaskResult` upload on the single TCP lane) is kept alive until the heartbeat has been missing
/// for this many reaper timeouts. It bounds how long a trickling peer can hold its tasks hostage.
pub const RX_LIVENESS_CEILING_FACTOR: u32 = 60;

/// First cooldown applied to a worker that rejected a task; doubles per consecutive rejection.
pub const REJECTION_COOLDOWN_BASE: Duration = Duration::from_secs(5);
/// Upper bound of the rejection cooldown.
pub const REJECTION_COOLDOWN_MAX: Duration = Duration::from_secs(60);

/// Aborts a connection task unless the caller *is* that task.
///
/// The registry stores the abort handle of each connection's own task, and the connection's
/// disconnect cleanup runs inside that same task. Aborting it from there would cancel the
/// cleanup (the failover sweep) at its first contended await.
fn abort_unless_current(handle: AbortHandle) {
    if tokio::task::try_id() == Some(handle.id()) {
        return;
    }
    handle.abort();
}

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

impl WorkerStatus {
    /// The state's name as it is serialized (`connected`, `busy`, `disconnected`).
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkerStatus::Connected => "connected",
            WorkerStatus::Busy => "busy",
            WorkerStatus::Disconnected => "disconnected",
        }
    }
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
    /// Tasks the master has dispatched to this worker and not yet seen finish (its own books:
    /// `reserve_dispatch` / `increment_active_tasks` add, `decrement_active_tasks` removes). A
    /// heartbeat never overwrites it.
    pub active_tasks: usize,
    /// Running-task count the worker last reported in a heartbeat. Informational: a worker
    /// counts a task only once it holds a concurrency permit, so it under-reports whenever tasks
    /// are queued locally, and trusting it made the master top such a worker up on every beat.
    pub reported_active_tasks: usize,
    pub remote_addr: SocketAddr,
    /// Outbound channel sender to worker's writer task.
    pub sender: mpsc::Sender<MasterMessage>,
    /// Optional abort handle to terminate worker connection tasks when unregistering/reaping.
    pub abort_handle: Option<AbortHandle>,
    pub cpu_usage_pct: f32,
    pub ram_available_mb: u64,
    /// Receive-side liveness clock of the connection (any inbound bytes count as proof of life).
    pub rx_clock: Option<Arc<RxClock>>,
    /// Consecutive tasks this worker rejected without running them (circuit breaker input).
    pub consecutive_rejections: u32,
    /// Scheduling exclusion deadline after rejections; the worker stays registered meanwhile.
    pub excluded_until: Option<Instant>,
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
    /// The larger of the master's own count and the worker's report: what to display, and whether
    /// the worker counts as busy. The scheduler does not use it for capacity (it derives that from
    /// the queue's assignment set).
    fn visible_active_tasks(&self) -> usize {
        self.active_tasks.max(self.reported_active_tasks)
    }

    /// True when the worker has shown no sign of life for longer than `timeout`.
    ///
    /// A heartbeat inside the window is always enough. Past it, bytes arriving on the connection
    /// still count (a multi-MB result upload starves heartbeats on the single TCP lane), but only
    /// until the heartbeat has been missing for [`RX_LIVENESS_CEILING_FACTOR`] timeouts.
    fn is_silent(&self, now: Instant, timeout: Duration) -> bool {
        let since_heartbeat = now.saturating_duration_since(self.last_heartbeat_instant);
        if since_heartbeat <= timeout {
            return false;
        }
        match &self.rx_clock {
            Some(clock) => {
                now.saturating_duration_since(clock.last_rx()) > timeout
                    || since_heartbeat > timeout.saturating_mul(RX_LIVENESS_CEILING_FACTOR)
            }
            None => true,
        }
    }

    pub fn to_info(&self) -> WorkerInfo {
        WorkerInfo {
            worker_id: self.worker_id,
            session_id: self.session_id,
            capabilities: self.capabilities.clone(),
            status: self.status,
            last_heartbeat_timestamp: self.last_heartbeat_timestamp,
            registered_timestamp: self.registered_timestamp,
            active_tasks: self.visible_active_tasks(),
            remote_addr: self.remote_addr.to_string(),
            cpu_usage_pct: self.cpu_usage_pct,
            ram_available_mb: self.ram_available_mb,
        }
    }
}

/// Result of [`WorkerRegistry::register_session`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Registration {
    /// Session id allocated to the new connection.
    pub session_id: u64,
    /// The live session this registration replaced, if any (see `register_session`).
    pub superseded_session: Option<u64>,
}

/// Result of [`WorkerRegistry::record_heartbeat_session`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatOutcome {
    /// Telemetry refreshed.
    Applied,
    /// The worker had been marked `Disconnected` (reaped) but its live session is heartbeating
    /// again, so it is schedulable once more. Its old tasks were already failed over.
    Revived,
    /// The heartbeat belongs to a superseded session and was ignored.
    StaleSession,
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
    ///
    /// This does not touch the task queue: callers that own one must use
    /// [`register_session`](Self::register_session) and fail over the superseded session.
    pub async fn register(
        &self,
        worker_id: Uuid,
        capabilities: WorkerCapabilities,
        remote_addr: SocketAddr,
        sender: mpsc::Sender<MasterMessage>,
        abort_handle: Option<AbortHandle>,
    ) -> GridResult<u64> {
        self.register_session(worker_id, capabilities, remote_addr, sender, abort_handle)
            .await
            .map(|registration| registration.session_id)
    }

    /// Like [`register`](Self::register), but also reports the session it replaced.
    ///
    /// When `worker_id` re-registers while its previous session is still live (the master never
    /// saw an EOF, e.g. a Wi-Fi to LTE handoff), the old connection is aborted and its in-flight
    /// tasks lost their only channel to the worker. `superseded_session` names that session so the
    /// caller can fail its tasks over exactly once; the aborted handler never runs its own cleanup.
    pub async fn register_session(
        &self,
        worker_id: Uuid,
        capabilities: WorkerCapabilities,
        remote_addr: SocketAddr,
        sender: mpsc::Sender<MasterMessage>,
        abort_handle: Option<AbortHandle>,
    ) -> GridResult<Registration> {
        let mut inner = self.inner.write().await;
        let session_id = inner.next_session_id;
        inner.next_session_id += 1;

        let now_instant = Instant::now();
        let now_epoch = Utc::now().timestamp() as u64;
        let mut superseded_session = None;

        if let Some(existing) = inner.workers.get_mut(&worker_id) {
            if existing.status != WorkerStatus::Disconnected {
                superseded_session = Some(existing.session_id);
            }
            // Cancel old connection tasks if still running
            if let Some(old_abort) = existing.abort_handle.take() {
                abort_unless_current(old_abort);
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
            // The old session's tasks are failed over by the caller; the new session starts empty.
            existing.active_tasks = 0;
            existing.reported_active_tasks = 0;
            existing.rx_clock = None;
            // A fresh Register carries a fresh capability snapshot, so a past rejection streak no longer applies.
            existing.consecutive_rejections = 0;
            existing.excluded_until = None;
            tracing::info!(worker_id = %worker_id, session_id = session_id, superseded = ?superseded_session, "Rebound existing worker registration");
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
                reported_active_tasks: 0,
                remote_addr,
                sender,
                abort_handle,
                cpu_usage_pct: 0.0,
                ram_available_mb: ram,
                rx_clock: None,
                consecutive_rejections: 0,
                excluded_until: None,
            };
            inner.workers.insert(worker_id, entry);
            tracing::info!(worker_id = %worker_id, session_id = session_id, "Registered new worker");
        }

        Ok(Registration {
            session_id,
            superseded_session,
        })
    }

    /// Attaches the receive-side liveness clock of `session_id`'s connection.
    pub async fn set_rx_clock(&self, worker_id: &Uuid, session_id: u64, clock: Arc<RxClock>) {
        let mut inner = self.inner.write().await;
        if let Some(entry) = inner.workers.get_mut(worker_id) {
            if entry.session_id == session_id {
                entry.rx_clock = Some(clock);
            }
        }
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
            // IDEMPOTENCY: If already disconnected, do not trigger failover again
            if entry.status == WorkerStatus::Disconnected {
                return Ok(false);
            }
            entry.status = WorkerStatus::Disconnected;
            entry.active_tasks = 0; // Clear active tasks on eviction
            entry.reported_active_tasks = 0;
            if let Some(abort) = entry.abort_handle.take() {
                abort_unless_current(abort);
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

    /// Unconditionally removes a worker from the registry (closing any active abort handle).
    pub async fn remove_worker(&self, worker_id: &Uuid) -> Option<WorkerInfo> {
        let mut inner = self.inner.write().await;
        if let Some(entry) = inner.workers.remove(worker_id) {
            if let Some(abort) = entry.abort_handle.clone() {
                abort_unless_current(abort);
            }
            Some(entry.to_info())
        } else {
            None
        }
    }

    /// Evicts disconnected workers whose last heartbeat or registration occurred longer ago than `stale_timeout`.
    pub async fn prune_stale_workers(&self, stale_timeout: Duration) -> usize {
        let mut inner = self.inner.write().await;
        let now = Instant::now();
        let mut to_remove = Vec::new();
        for (id, entry) in &inner.workers {
            if entry.status == WorkerStatus::Disconnected
                && now.duration_since(entry.last_heartbeat_instant) >= stale_timeout
            {
                to_remove.push(*id);
            }
        }
        let count = to_remove.len();
        for id in to_remove {
            if let Some(entry) = inner.workers.remove(&id) {
                if let Some(abort) = entry.abort_handle {
                    abort_unless_current(abort);
                }
            }
        }
        count
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
        self.record_heartbeat_session(
            worker_id,
            active_tasks,
            timestamp,
            cpu_usage_pct,
            ram_available_mb,
            expected_session,
        )
        .await
        .map(|_| ())
    }

    /// Like [`record_heartbeat`](Self::record_heartbeat), but reports what happened.
    ///
    /// A heartbeat carrying the *current* session id (`expected_session`) from a worker that was
    /// marked `Disconnected` revives it: the connection is evidently alive (a QUIC link that
    /// survived a stall, a reap that raced a late heartbeat), and leaving it `Disconnected` would
    /// make it permanently unschedulable while it keeps heartbeating. The tasks of the reaped
    /// period were already failed over, so nothing is restored beyond the status.
    pub async fn record_heartbeat_session(
        &self,
        worker_id: &Uuid,
        active_tasks: usize,
        timestamp: u64,
        cpu_usage_pct: f32,
        ram_available_mb: u64,
        expected_session: Option<u64>,
    ) -> GridResult<HeartbeatOutcome> {
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
                return Ok(HeartbeatOutcome::StaleSession);
            }
        }
        entry.last_heartbeat_instant = Instant::now();
        entry.last_heartbeat_timestamp = timestamp;
        // Only the report is stored: `active_tasks` is the master's own dispatch count and must
        // survive a heartbeat sampled before the latest AssignTask reached the worker.
        entry.reported_active_tasks = active_tasks;
        entry.cpu_usage_pct = cpu_usage_pct;
        entry.ram_available_mb = ram_available_mb;
        let revive = entry.status == WorkerStatus::Disconnected && expected_session.is_some();
        if entry.status != WorkerStatus::Disconnected || revive {
            entry.status = if entry.visible_active_tasks() > 0 {
                WorkerStatus::Busy
            } else {
                WorkerStatus::Connected
            };
        }
        if revive {
            tracing::warn!(
                worker_id = %worker_id,
                session_id = entry.session_id,
                "Heartbeat resumed on a reaped session; worker is schedulable again"
            );
            return Ok(HeartbeatOutcome::Revived);
        }
        Ok(HeartbeatOutcome::Applied)
    }

    /// Applies a worker's live battery/thermal report to its advertised capabilities.
    ///
    /// `Register` only carries the snapshot taken when the worker connected; the scheduler's
    /// battery and thermal gates (and the dashboard) read `capabilities.mobile`, so without this
    /// a phone that registered at 90% and has since drained to 4% would keep receiving heavy
    /// work. A report from a superseded session is ignored. Returns whether anything changed.
    pub async fn update_mobile_telemetry(
        &self,
        worker_id: &Uuid,
        expected_session: Option<u64>,
        status: MobileLiveStatus,
    ) -> GridResult<bool> {
        let mut inner = self.inner.write().await;
        let entry = inner
            .workers
            .get_mut(worker_id)
            .ok_or(GridError::WorkerNotFound(*worker_id))?;
        if let Some(expected) = expected_session {
            if entry.session_id != expected {
                return Ok(false);
            }
        }
        let updated = match entry.capabilities.mobile.as_ref() {
            Some(current) => current.with_live_status(status),
            // The worker registered without mobile data but now reports some (an app feeding
            // platform telemetry): there is no OS/SoC descriptor to keep.
            None => MobileCapabilities {
                os_version: "unknown".into(),
                soc_model: "unknown".into(),
                battery_pct: status.battery_pct,
                is_charging: status.is_charging,
                thermal_throttled: status.thermal_throttled,
            },
        };
        let changed = entry.capabilities.mobile.as_ref() != Some(&updated);
        entry.capabilities.mobile = Some(updated);
        Ok(changed)
    }

    /// Atomically reserves a dispatch slot on `session_id`'s connection and returns its channel.
    ///
    /// Fails with `ConnectionClosed` when the worker has since been disconnected or re-registered
    /// on a newer session, so a task assigned against a stale snapshot is never delivered to (or
    /// accounted against) a different connection than the one it was assigned for.
    pub async fn reserve_dispatch(
        &self,
        worker_id: &Uuid,
        session_id: u64,
    ) -> GridResult<mpsc::Sender<MasterMessage>> {
        let mut inner = self.inner.write().await;
        let entry = inner
            .workers
            .get_mut(worker_id)
            .ok_or(GridError::WorkerNotFound(*worker_id))?;
        if entry.session_id != session_id || entry.status == WorkerStatus::Disconnected {
            return Err(GridError::ConnectionClosed);
        }
        entry.active_tasks += 1;
        if entry.status == WorkerStatus::Connected {
            entry.status = WorkerStatus::Busy;
        }
        Ok(entry.sender.clone())
    }

    /// Gives back the slot [`reserve_dispatch`](Self::reserve_dispatch) took on `session_id`
    /// because the task was not delivered after all. A worker that has re-registered since started
    /// its books from zero, so its counter is left alone.
    pub async fn release_dispatch(&self, worker_id: &Uuid, session_id: u64) {
        let mut inner = self.inner.write().await;
        if let Some(entry) = inner.workers.get_mut(worker_id) {
            if entry.session_id == session_id {
                entry.active_tasks = entry.active_tasks.saturating_sub(1);
                if entry.status == WorkerStatus::Busy && entry.visible_active_tasks() == 0 {
                    entry.status = WorkerStatus::Connected;
                }
            }
        }
    }

    /// Records that `worker_id` handed a task back without running it (battery, capability or
    /// sandbox rejection) and excludes it from scheduling for an exponentially growing cooldown.
    ///
    /// Without this an instantly-rejecting worker looks permanently idle and wins every
    /// least-loaded placement, silently failing a large share of the cluster's work.
    /// Returns the cooldown applied, or `None` if the worker is unknown or the session is stale.
    pub async fn record_rejection(
        &self,
        worker_id: &Uuid,
        expected_session: Option<u64>,
    ) -> Option<Duration> {
        let mut inner = self.inner.write().await;
        let entry = inner.workers.get_mut(worker_id)?;
        if let Some(expected) = expected_session {
            if entry.session_id != expected {
                return None;
            }
        }
        entry.consecutive_rejections = entry.consecutive_rejections.saturating_add(1);
        let doublings = (entry.consecutive_rejections - 1).min(16);
        let cooldown = REJECTION_COOLDOWN_BASE
            .saturating_mul(1u32 << doublings)
            .min(REJECTION_COOLDOWN_MAX);
        entry.excluded_until = Some(Instant::now() + cooldown);
        tracing::warn!(
            worker_id = %worker_id,
            consecutive = entry.consecutive_rejections,
            cooldown_secs = cooldown.as_secs(),
            "Worker rejected a task; excluding it from scheduling for a cooldown"
        );
        Some(cooldown)
    }

    /// Clears the rejection streak of a worker that ran a task (successfully or not).
    pub async fn clear_rejections(&self, worker_id: &Uuid) {
        // Called for every finished task; take the write lock only when there is a streak to clear.
        let has_streak = {
            let inner = self.inner.read().await;
            inner
                .workers
                .get(worker_id)
                .is_some_and(|e| e.consecutive_rejections > 0 || e.excluded_until.is_some())
        };
        if !has_streak {
            return;
        }
        let mut inner = self.inner.write().await;
        if let Some(entry) = inner.workers.get_mut(worker_id) {
            entry.consecutive_rejections = 0;
            entry.excluded_until = None;
        }
    }

    /// Remaining scheduling exclusion of a worker, if it is cooling down after rejections.
    pub async fn exclusion_remaining(&self, worker_id: &Uuid) -> Option<Duration> {
        let inner = self.inner.read().await;
        let entry = inner.workers.get(worker_id)?;
        let until = entry.excluded_until?;
        until.checked_duration_since(Instant::now())
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
        if entry.status == WorkerStatus::Busy && entry.visible_active_tasks() == 0 {
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

    /// Returns the workers the scheduler may place tasks on: active (Connected or Busy) and not
    /// cooling down after rejecting tasks (see [`record_rejection`](Self::record_rejection)).
    pub async fn list_schedulable_workers(&self) -> Vec<WorkerInfo> {
        let inner = self.inner.read().await;
        let now = Instant::now();
        inner
            .workers
            .values()
            .filter(|e| e.status != WorkerStatus::Disconnected)
            .filter(|e| !matches!(e.excluded_until, Some(until) if until > now))
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
        self.reap_stale_sessions(timeout)
            .await
            .into_iter()
            .map(|(worker_id, _)| worker_id)
            .collect()
    }

    /// Like [`reap_stale_workers`](Self::reap_stale_workers), but also returns the reaped session
    /// of each worker so the caller can fail over exactly that session's tasks. A worker that
    /// re-registers between the reap and the failover sweep keeps the tasks of its new session.
    ///
    /// A worker counts as alive while it heartbeats *or* its connection delivers any bytes (see
    /// [`RxClock`]); a connection that goes completely silent is reaped after `timeout` as before.
    pub async fn reap_stale_sessions(&self, timeout: Duration) -> Vec<(Uuid, u64)> {
        let now = Instant::now();

        // Phase 1: Read lock candidate identification
        let candidates: Vec<Uuid> = {
            let inner = self.inner.read().await;
            inner
                .workers
                .values()
                .filter(|entry| {
                    entry.status != WorkerStatus::Disconnected && entry.is_silent(now, timeout)
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
        let now = Instant::now();

        for id in candidates {
            if let Some(entry) = inner.workers.get_mut(&id) {
                // Double check to prevent race condition with concurrent heartbeat or inbound bytes
                if entry.status != WorkerStatus::Disconnected && entry.is_silent(now, timeout) {
                    let elapsed = now.saturating_duration_since(entry.last_heartbeat_instant);
                    entry.status = WorkerStatus::Disconnected;
                    entry.active_tasks = 0;
                    entry.reported_active_tasks = 0;
                    if let Some(abort) = entry.abort_handle.take() {
                        abort_unless_current(abort);
                    }
                    tracing::warn!(
                        worker_id = %id,
                        worker_name = %entry.capabilities.name,
                        last_seen_secs = elapsed.as_secs(),
                        timeout_secs = timeout.as_secs(),
                        "Worker heartbeat timed out; transitioned to Disconnected"
                    );
                    reaped.push((id, entry.session_id));
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

    #[tokio::test]
    async fn test_unregister_idempotency_and_active_tasks_reset() {
        let registry = WorkerRegistry::new();
        let (tx, _rx) = mpsc::channel(16);
        let worker_id = Uuid::new_v4();
        let caps = WorkerCapabilities::new("w-test", 4, 8192, false, false, None);

        let session_id = registry
            .register(worker_id, caps, "127.0.0.1:9099".parse().unwrap(), tx, None)
            .await
            .unwrap();

        // Increment active tasks
        registry.increment_active_tasks(&worker_id).await.unwrap();
        registry.increment_active_tasks(&worker_id).await.unwrap();
        let entry = registry.get_worker(worker_id).await.unwrap();
        assert_eq!(entry.active_tasks, 2);
        assert_eq!(entry.status, WorkerStatus::Busy);

        // First unregister: returns Ok(true), status becomes Disconnected, active_tasks reset to 0
        let unreg1 = registry
            .unregister(&worker_id, Some(session_id))
            .await
            .unwrap();
        assert!(unreg1, "First unregister must succeed");

        let entry_disconnected = registry.get_worker(worker_id).await.unwrap();
        assert_eq!(entry_disconnected.status, WorkerStatus::Disconnected);
        assert_eq!(
            entry_disconnected.active_tasks, 0,
            "active_tasks must be reset to 0 on disconnect"
        );

        // Second unregister: returns Ok(false) due to idempotency
        let unreg2 = registry
            .unregister(&worker_id, Some(session_id))
            .await
            .unwrap();
        assert!(!unreg2, "Second unregister must return false (idempotent)");
    }

    #[tokio::test]
    async fn test_remove_worker_and_prune_stale_workers() {
        let registry = WorkerRegistry::new();
        let (tx, _rx) = mpsc::channel(16);
        let worker_1 = Uuid::new_v4();
        let worker_2 = Uuid::new_v4();
        let caps = WorkerCapabilities::new("w-prune", 2, 4096, false, false, None);

        registry
            .register(
                worker_1,
                caps.clone(),
                "127.0.0.1:9101".parse().unwrap(),
                tx.clone(),
                None,
            )
            .await
            .unwrap();
        registry
            .register(worker_2, caps, "127.0.0.1:9102".parse().unwrap(), tx, None)
            .await
            .unwrap();

        // Remove worker 1 explicitly
        let removed = registry.remove_worker(&worker_1).await;
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().worker_id, worker_1);
        assert!(registry.get_worker(worker_1).await.is_none());

        // Disconnect worker 2
        registry.mark_disconnected(&worker_2).await.unwrap();

        // Prune with large timeout should not remove worker 2 yet
        let pruned = registry.prune_stale_workers(Duration::from_secs(60)).await;
        assert_eq!(pruned, 0);
        assert!(registry.get_worker(worker_2).await.is_some());

        // Prune with 0 timeout removes disconnected worker 2
        let pruned = registry.prune_stale_workers(Duration::from_millis(0)).await;
        assert_eq!(pruned, 1);
        assert!(registry.get_worker(worker_2).await.is_none());
    }

    fn caps(name: &str) -> WorkerCapabilities {
        WorkerCapabilities::new(name, 4, 8192, false, false, None)
    }

    #[tokio::test]
    async fn test_register_session_reports_a_live_session_it_supersedes() {
        let registry = WorkerRegistry::new();
        let (tx1, _rx1) = mpsc::channel(4);
        let (tx2, _rx2) = mpsc::channel(4);
        let (tx3, _rx3) = mpsc::channel(4);
        let id = Uuid::new_v4();
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();

        let first = registry
            .register_session(id, caps("w"), addr, tx1, None)
            .await
            .unwrap();
        assert_eq!(
            first.superseded_session, None,
            "a brand new worker supersedes nothing"
        );

        // Re-registering while the previous session is still live supersedes it.
        registry.increment_active_tasks(&id).await.unwrap();
        let second = registry
            .register_session(id, caps("w"), addr, tx2, None)
            .await
            .unwrap();
        assert_eq!(second.superseded_session, Some(first.session_id));
        assert_eq!(
            registry.get_worker(id).await.unwrap().active_tasks,
            0,
            "the new session starts with an empty task count"
        );

        // Re-registering over an already-disconnected entry supersedes nothing: whoever
        // disconnected it already owns the failover.
        registry
            .unregister(&id, Some(second.session_id))
            .await
            .unwrap();
        let third = registry
            .register_session(id, caps("w"), addr, tx3, None)
            .await
            .unwrap();
        assert_eq!(third.superseded_session, None);
    }

    /// Registers the abort handle of a freshly spawned task running `body` with the registry.
    async fn spawn_registered<F, Fut>(
        registry: &WorkerRegistry,
        id: Uuid,
        body: F,
    ) -> (u64, tokio::task::JoinHandle<()>)
    where
        F: FnOnce(WorkerRegistry, Uuid, u64) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let (abort_tx, abort_rx) = tokio::sync::oneshot::channel::<AbortHandle>();
        let (session_tx, session_rx) = tokio::sync::oneshot::channel::<u64>();
        let reg = registry.clone();
        let handle = tokio::spawn(async move {
            let session = session_rx.await.unwrap();
            let _own_abort = abort_rx.await.unwrap();
            body(reg, id, session).await;
        });
        let (tx, _rx) = mpsc::channel(4);
        let session = registry
            .register(
                id,
                caps("conn"),
                "127.0.0.1:9002".parse().unwrap(),
                tx,
                Some(handle.abort_handle()),
            )
            .await
            .unwrap();
        session_tx.send(session).unwrap();
        abort_tx.send(handle.abort_handle()).unwrap();
        (session, handle)
    }

    #[tokio::test]
    async fn test_unregister_from_the_connection_task_itself_does_not_cancel_its_cleanup() {
        // The failover sweep runs in the connection task right after unregister(); the abort handle
        // stored in the registry belongs to that very task, so aborting it would cancel the sweep
        // at its first await that returns Pending.
        let registry = WorkerRegistry::new();
        let id = Uuid::new_v4();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let (_session, handle) =
            spawn_registered(&registry, id, move |reg, id, session| async move {
                assert!(reg.unregister(&id, Some(session)).await.unwrap());
                tokio::task::yield_now().await; // the first contended await of a real sweep
                tokio::time::sleep(Duration::from_millis(5)).await;
                let _ = done_tx.send(());
            })
            .await;
        tokio::time::timeout(Duration::from_secs(2), done_rx)
            .await
            .expect("cleanup finished")
            .expect("the connection task was aborted by its own unregister()");
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_unregister_from_another_task_still_aborts_the_connection() {
        let registry = WorkerRegistry::new();
        let id = Uuid::new_v4();
        let (_session, handle) = spawn_registered(&registry, id, |_, _, _| async move {
            std::future::pending::<()>().await;
        })
        .await;
        registry.unregister(&id, None).await.unwrap();
        let joined = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("aborted task finishes");
        assert!(joined.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn test_reregistration_aborts_the_superseded_connection_task() {
        let registry = WorkerRegistry::new();
        let id = Uuid::new_v4();
        let (_session, handle) = spawn_registered(&registry, id, |_, _, _| async move {
            std::future::pending::<()>().await;
        })
        .await;
        let (tx, _rx) = mpsc::channel(4);
        registry
            .register(id, caps("new"), "127.0.0.1:9003".parse().unwrap(), tx, None)
            .await
            .unwrap();
        let joined = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("aborted task finishes");
        assert!(joined.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn test_inbound_bytes_keep_a_heartbeat_starved_worker_alive() {
        let registry = WorkerRegistry::new();
        let id = Uuid::new_v4();
        let (tx, _rx) = mpsc::channel(4);
        let session = registry
            .register(
                id,
                caps("uploader"),
                "127.0.0.1:9004".parse().unwrap(),
                tx,
                None,
            )
            .await
            .unwrap();
        let clock = RxClock::new();
        registry.set_rx_clock(&id, session, clock.clone()).await;

        // No heartbeat for longer than the timeout, but bytes keep arriving (a big result upload).
        let timeout = Duration::from_millis(120);
        for _ in 0..6 {
            tokio::time::sleep(Duration::from_millis(40)).await;
            clock.touch();
            assert!(
                registry.reap_stale_sessions(timeout).await.is_empty(),
                "a connection that is receiving data must not be reaped"
            );
        }
        assert_eq!(
            registry.get_worker(id).await.unwrap().status,
            WorkerStatus::Connected
        );

        // Once the bytes stop too, it is reaped like any silent worker, reporting the session.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            registry.reap_stale_sessions(timeout).await,
            vec![(id, session)]
        );
    }

    #[tokio::test]
    async fn test_silent_connection_without_rx_clock_is_reaped_as_before() {
        let registry = WorkerRegistry::new();
        let id = Uuid::new_v4();
        let (tx, _rx) = mpsc::channel(4);
        registry
            .register(
                id,
                caps("silent"),
                "127.0.0.1:9005".parse().unwrap(),
                tx,
                None,
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(
            registry.reap_stale_workers(Duration::from_millis(40)).await,
            vec![id]
        );
    }

    #[tokio::test]
    async fn test_rx_liveness_is_bounded_by_a_ceiling() {
        // A peer that trickles a byte now and then must not hold its tasks hostage forever.
        let registry = WorkerRegistry::new();
        let id = Uuid::new_v4();
        let (tx, _rx) = mpsc::channel(4);
        let session = registry
            .register(
                id,
                caps("trickle"),
                "127.0.0.1:9006".parse().unwrap(),
                tx,
                None,
            )
            .await
            .unwrap();
        let clock = RxClock::new();
        registry.set_rx_clock(&id, session, clock.clone()).await;

        let timeout = Duration::from_millis(2);
        let ceiling = timeout * RX_LIVENESS_CEILING_FACTOR;
        tokio::time::sleep(ceiling + Duration::from_millis(30)).await;
        clock.touch(); // fresh bytes, but the heartbeat has been missing for far too long
        assert_eq!(
            registry.reap_stale_sessions(timeout).await,
            vec![(id, session)]
        );
    }

    #[tokio::test]
    async fn test_heartbeat_on_the_live_session_revives_a_reaped_worker() {
        let registry = WorkerRegistry::new();
        let id = Uuid::new_v4();
        let (tx, _rx) = mpsc::channel(4);
        let session = registry
            .register(
                id,
                caps("quic"),
                "127.0.0.1:9007".parse().unwrap(),
                tx,
                None,
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(
            registry
                .reap_stale_sessions(Duration::from_millis(10))
                .await,
            vec![(id, session)]
        );
        assert!(registry.list_schedulable_workers().await.is_empty());

        // Heartbeat from a different (older) session, or without session info, revives nothing.
        let stale = registry
            .record_heartbeat_session(&id, 0, 1, 0.0, 100, Some(session + 5))
            .await
            .unwrap();
        assert_eq!(stale, HeartbeatOutcome::StaleSession);
        let anonymous = registry
            .record_heartbeat_session(&id, 0, 1, 0.0, 100, None)
            .await
            .unwrap();
        assert_eq!(anonymous, HeartbeatOutcome::Applied);
        assert_eq!(
            registry.get_worker(id).await.unwrap().status,
            WorkerStatus::Disconnected
        );

        // Heartbeat on the current session: schedulable again, Busy if it reports running tasks.
        let revived = registry
            .record_heartbeat_session(&id, 2, 2, 5.0, 4096, Some(session))
            .await
            .unwrap();
        assert_eq!(revived, HeartbeatOutcome::Revived);
        let info = registry.get_worker(id).await.unwrap();
        assert_eq!(info.status, WorkerStatus::Busy);
        assert_eq!(info.active_tasks, 2);
        assert_eq!(registry.list_schedulable_workers().await.len(), 1);
    }

    #[tokio::test]
    async fn test_rejecting_worker_is_excluded_with_growing_cooldown_until_it_runs_something() {
        let registry = WorkerRegistry::new();
        let (good_tx, _good_rx) = mpsc::channel(4);
        let (bad_tx, _bad_rx) = mpsc::channel(4);
        let good = Uuid::new_v4();
        let bad = Uuid::new_v4();
        registry
            .register(
                good,
                caps("good"),
                "127.0.0.1:9008".parse().unwrap(),
                good_tx,
                None,
            )
            .await
            .unwrap();
        let bad_session = registry
            .register(
                bad,
                caps("bad"),
                "127.0.0.1:9009".parse().unwrap(),
                bad_tx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(registry.list_schedulable_workers().await.len(), 2);

        // A stale session cannot penalise the current one.
        assert!(registry
            .record_rejection(&bad, Some(bad_session + 1))
            .await
            .is_none());

        assert_eq!(
            registry.record_rejection(&bad, Some(bad_session)).await,
            Some(REJECTION_COOLDOWN_BASE)
        );
        assert_eq!(
            registry.record_rejection(&bad, Some(bad_session)).await,
            Some(REJECTION_COOLDOWN_BASE * 2)
        );
        let schedulable: Vec<Uuid> = registry
            .list_schedulable_workers()
            .await
            .into_iter()
            .map(|w| w.worker_id)
            .collect();
        assert_eq!(
            schedulable,
            vec![good],
            "the rejecting worker is out of the rotation"
        );
        assert_eq!(
            registry.list_active_workers().await.len(),
            2,
            "but it stays registered and visible"
        );
        assert!(registry.exclusion_remaining(&bad).await.is_some());

        for _ in 0..10 {
            registry.record_rejection(&bad, None).await;
        }
        assert_eq!(
            registry.record_rejection(&bad, None).await,
            Some(REJECTION_COOLDOWN_MAX),
            "the cooldown is capped"
        );

        // Running a task for real clears the streak.
        registry.clear_rejections(&bad).await;
        assert!(registry.exclusion_remaining(&bad).await.is_none());
        assert_eq!(registry.list_schedulable_workers().await.len(), 2);
    }

    #[tokio::test]
    async fn test_reserve_dispatch_is_bound_to_the_session_it_was_assigned_on() {
        let registry = WorkerRegistry::new();
        let id = Uuid::new_v4();
        let (tx1, _rx1) = mpsc::channel(4);
        let (tx2, mut rx2) = mpsc::channel(4);
        let addr: SocketAddr = "127.0.0.1:9010".parse().unwrap();
        let old = registry
            .register(id, caps("w"), addr, tx1, None)
            .await
            .unwrap();
        let new = registry
            .register(id, caps("w"), addr, tx2, None)
            .await
            .unwrap();

        // An assignment made against the old snapshot must not land on (or count against) the new connection.
        assert!(registry.reserve_dispatch(&id, old).await.is_err());
        assert_eq!(registry.get_worker(id).await.unwrap().active_tasks, 0);

        let sender = registry.reserve_dispatch(&id, new).await.unwrap();
        assert_eq!(registry.get_worker(id).await.unwrap().active_tasks, 1);
        sender
            .send(MasterMessage::HeartbeatAck { timestamp: 7 })
            .await
            .unwrap();
        assert!(matches!(
            rx2.recv().await,
            Some(MasterMessage::HeartbeatAck { timestamp: 7 })
        ));

        registry.unregister(&id, Some(new)).await.unwrap();
        assert!(registry.reserve_dispatch(&id, new).await.is_err());
    }

    #[tokio::test]
    async fn test_heartbeat_never_overwrites_the_masters_own_dispatch_count() {
        let registry = WorkerRegistry::new();
        let (tx, _rx) = mpsc::channel(16);
        let id = Uuid::new_v4();
        let caps = WorkerCapabilities::new("phone", 10, 8192, false, false, None);
        let session = registry
            .register(id, caps, "127.0.0.1:9020".parse().unwrap(), tx, None)
            .await
            .unwrap();

        // The master hands out 6 tasks; the worker has a lower concurrency limit than its core
        // count, so its heartbeat only counts the 4 it is actually running.
        for _ in 0..6 {
            registry.reserve_dispatch(&id, session).await.unwrap();
        }
        registry
            .record_heartbeat(&id, 4, 1000, 10.0, 4096, Some(session))
            .await
            .unwrap();
        let info = registry.get_worker(id).await.unwrap();
        assert_eq!(
            info.active_tasks, 6,
            "the heartbeat must not shrink the master's own count"
        );
        assert_eq!(info.status, WorkerStatus::Busy);

        // A heartbeat that reports more than the master knows (tasks it lost track of) is shown.
        registry
            .record_heartbeat(&id, 9, 1003, 10.0, 4096, Some(session))
            .await
            .unwrap();
        assert_eq!(registry.get_worker(id).await.unwrap().active_tasks, 9);

        // Results release the master's count one at a time...
        for _ in 0..6 {
            registry.decrement_active_tasks(&id).await.unwrap();
        }
        // ...and an idle report with nothing outstanding on the master's side means idle.
        registry
            .record_heartbeat(&id, 0, 1006, 5.0, 8000, Some(session))
            .await
            .unwrap();
        let info = registry.get_worker(id).await.unwrap();
        assert_eq!(info.active_tasks, 0);
        assert_eq!(info.status, WorkerStatus::Connected);
    }

    fn phone_caps(
        battery_pct: u8,
        is_charging: bool,
        thermal_throttled: bool,
    ) -> WorkerCapabilities {
        WorkerCapabilities::new("phone", 8, 8192, false, false, None).with_mobile(Some(
            MobileCapabilities {
                os_version: "Android 14".into(),
                soc_model: "Snapdragon".into(),
                battery_pct: Some(battery_pct),
                is_charging: Some(is_charging),
                thermal_throttled,
            },
        ))
    }

    #[tokio::test]
    async fn test_live_mobile_telemetry_replaces_the_register_time_snapshot() {
        let registry = WorkerRegistry::new();
        let (tx, _rx) = mpsc::channel(16);
        let id = Uuid::new_v4();
        let session = registry
            .register(
                id,
                phone_caps(90, true, false),
                "127.0.0.1:9030".parse().unwrap(),
                tx,
                None,
            )
            .await
            .unwrap();

        let drained = MobileLiveStatus {
            battery_pct: Some(4),
            is_charging: Some(false),
            thermal_throttled: true,
        };
        assert!(registry
            .update_mobile_telemetry(&id, Some(session), drained)
            .await
            .unwrap());
        let mobile = registry
            .get_worker(id)
            .await
            .unwrap()
            .capabilities
            .mobile
            .unwrap();
        assert_eq!(mobile.battery_pct, Some(4));
        assert_eq!(mobile.is_charging, Some(false));
        assert!(mobile.thermal_throttled);
        // The static descriptors of the registration survive.
        assert_eq!(mobile.os_version, "Android 14");
        assert_eq!(mobile.soc_model, "Snapdragon");

        // Re-sending the same reading reports no change (the caller does not wake the scheduler).
        assert!(!registry
            .update_mobile_telemetry(&id, Some(session), drained)
            .await
            .unwrap());

        // A report from a superseded session is ignored.
        let stale = MobileLiveStatus {
            battery_pct: Some(100),
            is_charging: Some(true),
            thermal_throttled: false,
        };
        assert!(!registry
            .update_mobile_telemetry(&id, Some(session + 1), stale)
            .await
            .unwrap());
        assert_eq!(
            registry
                .get_worker(id)
                .await
                .unwrap()
                .capabilities
                .mobile
                .unwrap()
                .battery_pct,
            Some(4)
        );

        // Unknown worker.
        assert!(registry
            .update_mobile_telemetry(&Uuid::new_v4(), None, stale)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_live_telemetry_creates_mobile_data_for_a_worker_that_registered_without() {
        let registry = WorkerRegistry::new();
        let (tx, _rx) = mpsc::channel(16);
        let id = Uuid::new_v4();
        let session = registry
            .register(
                id,
                WorkerCapabilities::new("app", 4, 4096, false, false, None),
                "127.0.0.1:9031".parse().unwrap(),
                tx,
                None,
            )
            .await
            .unwrap();
        assert!(registry
            .get_worker(id)
            .await
            .unwrap()
            .capabilities
            .mobile
            .is_none());

        let status = MobileLiveStatus {
            battery_pct: Some(50),
            is_charging: None,
            thermal_throttled: false,
        };
        assert!(registry
            .update_mobile_telemetry(&id, Some(session), status)
            .await
            .unwrap());
        let mobile = registry
            .get_worker(id)
            .await
            .unwrap()
            .capabilities
            .mobile
            .unwrap();
        assert_eq!(mobile.battery_pct, Some(50));
        assert_eq!(mobile.is_charging, None);
    }

    /// End to end through the real registry: the scheduler keeps giving heavy work to a phone
    /// until its live telemetry says the battery is nearly flat, then stops, then resumes once it
    /// is charging again. With only the registration snapshot (90%, charging) it never stops.
    #[tokio::test]
    async fn test_scheduler_gates_on_live_battery_and_thermal_telemetry() {
        use crate::scheduler::{SchedulerConfig, WorkloadScheduler};
        use rusty_grid_core::{Task, TaskRequirements, TaskSpec};

        let registry = WorkerRegistry::new();
        let (tx, _rx) = mpsc::channel(16);
        let id = Uuid::new_v4();
        let session = registry
            .register(
                id,
                phone_caps(90, true, false),
                "127.0.0.1:9032".parse().unwrap(),
                tx,
                None,
            )
            .await
            .unwrap();

        let config = SchedulerConfig::default();
        let compile = || {
            Task::new(
                TaskSpec::rust_compilation("c", Default::default(), vec![]),
                TaskRequirements::generic(1, 60),
            )
        };
        let multi_core = || {
            Task::new(
                TaskSpec::command("true", vec![]),
                TaskRequirements::generic(2, 60),
            )
        };
        let placed = |tasks: Vec<Task>, workers: Vec<WorkerInfo>| {
            WorkloadScheduler::matchmake(&config, &tasks, &workers)
                .assignments
                .len()
        };

        let workers = registry.list_active_workers().await;
        assert_eq!(
            placed(vec![compile()], workers.clone()),
            1,
            "healthy phone takes the build"
        );
        assert_eq!(placed(vec![multi_core()], workers), 1);

        // Unplugged and drained to 4%: heavy work stops, light work still goes.
        registry
            .update_mobile_telemetry(
                &id,
                Some(session),
                MobileLiveStatus {
                    battery_pct: Some(4),
                    is_charging: Some(false),
                    thermal_throttled: false,
                },
            )
            .await
            .unwrap();
        let workers = registry.list_active_workers().await;
        assert_eq!(
            placed(vec![compile()], workers.clone()),
            0,
            "a flat battery must not get builds"
        );
        assert_eq!(placed(vec![multi_core()], workers), 1);

        // Plugged back in: eligible again.
        registry
            .update_mobile_telemetry(
                &id,
                Some(session),
                MobileLiveStatus {
                    battery_pct: Some(4),
                    is_charging: Some(true),
                    thermal_throttled: false,
                },
            )
            .await
            .unwrap();
        let workers = registry.list_active_workers().await;
        assert_eq!(placed(vec![compile()], workers), 1);

        // Overheated: multi-core and heavy work stop.
        registry
            .update_mobile_telemetry(
                &id,
                Some(session),
                MobileLiveStatus {
                    battery_pct: Some(80),
                    is_charging: Some(true),
                    thermal_throttled: true,
                },
            )
            .await
            .unwrap();
        let workers = registry.list_active_workers().await;
        assert_eq!(placed(vec![multi_core()], workers.clone()), 0);
        assert_eq!(placed(vec![compile()], workers), 0);
    }
}
