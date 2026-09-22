//! Master periodic heartbeat reaper background monitor.
//!
//! Sweeps the `WorkerRegistry` at configured intervals (default: 1.0s) and transitions
//! any worker whose last heartbeat exceeds the timeout (default: 10.0s) to `WorkerStatus::Disconnected`.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{debug, info, warn};

use rusty_grid_core::task::TaskResult;

use crate::queue::TaskQueue;
use crate::registry::WorkerRegistry;

/// Configuration parameters for the Master Heartbeat Reaper monitor.
#[derive(Debug, Clone)]
pub struct ReaperConfig {
    /// How frequently the reaper scans the registry for stale workers (default: 1.0 second).
    pub scan_interval: Duration,
    /// Inactivity threshold after which an uncommunicative worker is declared Disconnected (default: 10.0 seconds).
    pub timeout: Duration,
}

impl Default for ReaperConfig {
    fn default() -> Self {
        Self {
            scan_interval: Duration::from_secs(1),
            timeout: Duration::from_secs(10),
        }
    }
}

impl ReaperConfig {
    pub fn new(scan_interval: Duration, timeout: Duration) -> Self {
        Self {
            scan_interval,
            timeout,
        }
    }

    pub fn with_scan_interval(mut self, interval: Duration) -> Self {
        self.scan_interval = interval;
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

/// Spawns the Master Heartbeat Reaper background task with task failover integration.
///
/// Periodically scans for dead workers, marks them Disconnected, reaps any active tasks
/// assigned to them (transitioning them to Retrying or Failed), and signals the scheduler.
/// Any tasks reaching a terminal Failed state immediately resolve active client waiters.
pub fn spawn_reaper_with_queue(
    registry: WorkerRegistry,
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    waiters: Option<crate::server::WaiterMap>,
    config: ReaperConfig,
    shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    #[cfg(feature = "dashboard")]
    {
        spawn_reaper_with_broadcast(registry, queue, scheduler_notify, waiters, config, shutdown_rx, None)
    }
    #[cfg(not(feature = "dashboard"))]
    {
        spawn_reaper_internal(registry, queue, scheduler_notify, waiters, config, shutdown_rx)
    }
}

/// Spawns heartbeat reaper with optional broadcast channel for live telemetry event emission.
#[cfg(feature = "dashboard")]
pub fn spawn_reaper_with_broadcast(
    registry: WorkerRegistry,
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    waiters: Option<crate::server::WaiterMap>,
    config: ReaperConfig,
    mut shutdown_rx: watch::Receiver<bool>,
    broadcast_tx: Option<tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!(
            scan_interval_ms = config.scan_interval.as_millis(),
            timeout_secs = config.timeout.as_secs(),
            "Starting Master heartbeat reaper with failover supervision"
        );

        let mut ticker = interval(config.scan_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let reaped = registry.reap_stale_workers(config.timeout).await;
                    if !reaped.is_empty() {
                        warn!(
                            count = reaped.len(),
                            reaped_workers = ?reaped,
                            "Reaper pass completed: marked dead workers as Disconnected"
                        );

                        let mut total_failed_over = 0;
                        for worker_id in &reaped {
                            if let Some(ref b_tx) = broadcast_tx {
                                let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::WorkerDisconnected {
                                    worker_id: *worker_id,
                                    reason: "Heartbeat timeout (reaper detected)".to_string(),
                                });
                            }

                            let tasks = queue.handle_worker_disconnected(
                                worker_id,
                                "Heartbeat timeout (reaper detected)",
                                false,
                            ).await;
                            total_failed_over += tasks.len();

                            if let Some(ref b_tx) = broadcast_tx {
                                for task_id in &tasks {
                                    if let Some(info) = queue.get_task(task_id).await {
                                        let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(info));
                                    }
                                }
                            }

                            // Resolve waiters for any tasks that reached terminal state
                            if let Some(ref w_map) = waiters {
                                for task_id in &tasks {
                                    if let Some(info) = queue.get_task(task_id).await {
                                        if info.state.is_terminal() {
                                            let res = queue.get_result(task_id).await.unwrap_or_else(|| {
                                                TaskResult::failure(
                                                    *worker_id,
                                                    *task_id,
                                                    1,
                                                    "",
                                                    "",
                                                    0,
                                                    info.error_message.clone().or_else(|| Some("Heartbeat timeout".into())),
                                                )
                                            });
                                            let mut lock = w_map.write().await;
                                            if let Some(senders) = lock.remove(task_id) {
                                                for tx in senders {
                                                    let _ = tx.send(res.clone());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        if total_failed_over > 0 {
                            if let Some(ref b_tx) = broadcast_tx {
                                let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(queue.stats().await));
                            }
                            info!(count = total_failed_over, "Reaper triggered task failover reassignment");
                            scheduler_notify.notify_one();
                        }
                    } else {
                        debug!("Reaper check completed: all active workers healthy");
                    }
                }
                res = shutdown_rx.changed() => {
                    if res.is_ok() && *shutdown_rx.borrow() {
                        info!("Reaper loop received shutdown signal; terminating");
                        break;
                    } else if res.is_err() {
                        break;
                    }
                }
            }
        }
    })
}

#[cfg(not(feature = "dashboard"))]
fn spawn_reaper_internal(
    registry: WorkerRegistry,
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    waiters: Option<crate::server::WaiterMap>,
    config: ReaperConfig,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!(
            scan_interval_ms = config.scan_interval.as_millis(),
            timeout_secs = config.timeout.as_secs(),
            "Starting Master heartbeat reaper with failover supervision"
        );

        let mut ticker = interval(config.scan_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let reaped = registry.reap_stale_workers(config.timeout).await;
                    if !reaped.is_empty() {
                        warn!(
                            count = reaped.len(),
                            reaped_workers = ?reaped,
                            "Reaper pass completed: marked dead workers as Disconnected"
                        );

                        let mut total_failed_over = 0;
                        for worker_id in &reaped {
                            let tasks = queue.handle_worker_disconnected(
                                worker_id,
                                "Heartbeat timeout (reaper detected)",
                                false,
                            ).await;
                            total_failed_over += tasks.len();

                            // Resolve waiters for any tasks that reached terminal state
                            if let Some(ref w_map) = waiters {
                                for task_id in &tasks {
                                    if let Some(info) = queue.get_task(task_id).await {
                                        if info.state.is_terminal() {
                                            let res = queue.get_result(task_id).await.unwrap_or_else(|| {
                                                TaskResult::failure(
                                                    *worker_id,
                                                    *task_id,
                                                    1,
                                                    "",
                                                    "",
                                                    0,
                                                    info.error_message.clone().or_else(|| Some("Heartbeat timeout".into())),
                                                )
                                            });
                                            let mut lock = w_map.write().await;
                                            if let Some(senders) = lock.remove(task_id) {
                                                for tx in senders {
                                                    let _ = tx.send(res.clone());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        if total_failed_over > 0 {
                            info!(count = total_failed_over, "Reaper triggered task failover reassignment");
                            scheduler_notify.notify_one();
                        }
                    } else {
                        debug!("Reaper check completed: all active workers healthy");
                    }
                }
                res = shutdown_rx.changed() => {
                    if res.is_ok() && *shutdown_rx.borrow() {
                        info!("Reaper loop received shutdown signal; terminating");
                        break;
                    } else if res.is_err() {
                        break;
                    }
                }
            }
        }
    })
}

/// Spawns the Master Heartbeat Reaper background task without queue failover.
pub fn spawn_reaper(
    registry: WorkerRegistry,
    config: ReaperConfig,
    shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    let dummy_queue = TaskQueue::new();
    let dummy_notify = Arc::new(tokio::sync::Notify::new());
    spawn_reaper_with_queue(registry, dummy_queue, dummy_notify, None, config, shutdown_rx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::WorkerStatus;
    use rusty_grid_core::WorkerCapabilities;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_reaper_transitions_timed_out_worker() {
        let registry = WorkerRegistry::new();
        let (tx, _rx) = mpsc::channel(16);
        let worker_id = Uuid::new_v4();
        let caps = WorkerCapabilities::new("timed-out-worker", 2, 2048, false, false, None);

        registry
            .register(worker_id, caps, "127.0.0.1:9001".parse().unwrap(), tx, None)
            .await
            .unwrap();

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let config = ReaperConfig {
            scan_interval: Duration::from_millis(20),
            timeout: Duration::from_millis(50),
        };

        let handle = spawn_reaper(registry.clone(), config, shutdown_rx);

        // Sleep to let worker exceed timeout and reaper run
        tokio::time::sleep(Duration::from_millis(150)).await;

        let info = registry.get_worker(worker_id).await.unwrap();
        assert_eq!(info.status, WorkerStatus::Disconnected);

        let _ = shutdown_tx.send(true);
        let _ = handle.await;
    }
}
