//! Master periodic heartbeat reaper background monitor.
//!
//! Sweeps the `WorkerRegistry` at configured intervals (default: 1.0s) and transitions
//! any worker whose last heartbeat exceeds the timeout (default: 10.0s) to `WorkerStatus::Disconnected`.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{debug, info, warn};

use crate::queue::TaskQueue;
use crate::registry::WorkerRegistry;

/// Configuration parameters for the Master Heartbeat Reaper monitor.
#[derive(Debug, Clone)]
pub struct ReaperConfig {
    /// How frequently the reaper scans the registry for stale workers (default: 1.0 second).
    pub scan_interval: Duration,
    /// Inactivity threshold after which an uncommunicative worker is declared Disconnected (default: 10.0 seconds).
    pub timeout: Duration,
    /// Inactivity threshold after which a disconnected worker is pruned from memory (default: 1 hour).
    pub stale_worker_timeout: Option<Duration>,
}

impl Default for ReaperConfig {
    fn default() -> Self {
        Self {
            scan_interval: Duration::from_secs(1),
            timeout: Duration::from_secs(10),
            stale_worker_timeout: Some(Duration::from_secs(3600)),
        }
    }
}

impl ReaperConfig {
    pub fn new(scan_interval: Duration, timeout: Duration) -> Self {
        Self {
            scan_interval,
            timeout,
            stale_worker_timeout: Some(Duration::from_secs(3600)),
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

    pub fn with_stale_worker_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.stale_worker_timeout = timeout;
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
        spawn_reaper_with_broadcast(
            registry,
            queue,
            scheduler_notify,
            waiters,
            config,
            shutdown_rx,
            None,
        )
    }
    #[cfg(not(feature = "dashboard"))]
    {
        spawn_reaper_internal(
            registry,
            queue,
            scheduler_notify,
            waiters,
            config,
            shutdown_rx,
        )
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
    broadcast_tx: Option<
        tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>,
    >,
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
                    let reaped = registry.reap_stale_sessions(config.timeout).await;
                    if let Some(stale_timeout) = config.stale_worker_timeout {
                        let pruned_workers = registry.prune_stale_workers(stale_timeout).await;
                        if pruned_workers > 0 {
                            debug!(count = pruned_workers, "Reaper pruned stale disconnected workers");
                        }
                    }
                    if !reaped.is_empty() {
                        warn!(
                            count = reaped.len(),
                            reaped_workers = ?reaped,
                            "Reaper pass completed: marked dead workers as Disconnected"
                        );

                        let mut total_failed_over = 0;
                        for (worker_id, session_id) in &reaped {
                            if let Some(ref b_tx) = broadcast_tx {
                                let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::WorkerDisconnected {
                                    worker_id: *worker_id,
                                    reason: "Heartbeat timeout (reaper detected)".to_string(),
                                });
                            }

                            // Only the reaped session's tasks: if the worker has already
                            // re-registered, its new session keeps what it was given.
                            let tasks = queue.handle_session_disconnected(
                                worker_id,
                                Some(*session_id),
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
                                crate::server::resolve_terminal_waiters(&queue, w_map, &tasks, Some(*worker_id)).await;
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
                    let reaped = registry.reap_stale_sessions(config.timeout).await;
                    if let Some(stale_timeout) = config.stale_worker_timeout {
                        let pruned_workers = registry.prune_stale_workers(stale_timeout).await;
                        if pruned_workers > 0 {
                            debug!(count = pruned_workers, "Reaper pruned stale disconnected workers");
                        }
                    }
                    if !reaped.is_empty() {
                        warn!(
                            count = reaped.len(),
                            reaped_workers = ?reaped,
                            "Reaper pass completed: marked dead workers as Disconnected"
                        );

                        let mut total_failed_over = 0;
                        for (worker_id, session_id) in &reaped {
                            // Only the reaped session's tasks (see the dashboard variant above).
                            let tasks = queue.handle_session_disconnected(
                                worker_id,
                                Some(*session_id),
                                "Heartbeat timeout (reaper detected)",
                                false,
                            ).await;
                            total_failed_over += tasks.len();

                            // Resolve waiters for any tasks that reached terminal state
                            if let Some(ref w_map) = waiters {
                                crate::server::resolve_terminal_waiters(&queue, w_map, &tasks, Some(*worker_id)).await;
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
    spawn_reaper_with_queue(
        registry,
        dummy_queue,
        dummy_notify,
        None,
        config,
        shutdown_rx,
    )
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
        let config = ReaperConfig::new(Duration::from_millis(20), Duration::from_millis(50));

        let handle = spawn_reaper(registry.clone(), config, shutdown_rx);

        // Sleep to let worker exceed timeout and reaper run
        tokio::time::sleep(Duration::from_millis(150)).await;

        let info = registry.get_worker(worker_id).await.unwrap();
        assert_eq!(info.status, WorkerStatus::Disconnected);

        let _ = shutdown_tx.send(true);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn test_reaper_reaps_laptop_battery_worker_at_configured_reaper_timeout() {
        use rusty_grid_core::capabilities::{MobileCapabilities, MobileLiveStatus};

        let registry = WorkerRegistry::new();
        let (tx, _rx) = mpsc::channel(16);
        let worker_id = Uuid::new_v4();
        let mut caps =
            WorkerCapabilities::new("laptop-battery-worker", 4, 8192, false, false, None);
        caps.mobile = Some(MobileCapabilities {
            os_version: "macOS 14.5".into(),
            soc_model: "Apple M2".into(),
            battery_pct: Some(78),
            is_charging: Some(false),
            thermal_throttled: false,
        });

        let session = registry
            .register(worker_id, caps, "127.0.0.1:9002".parse().unwrap(), tx, None)
            .await
            .unwrap();
        registry
            .update_mobile_telemetry(
                &worker_id,
                Some(session),
                MobileLiveStatus {
                    battery_pct: Some(77),
                    is_charging: Some(false),
                    network_type: None,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        registry
            .record_heartbeat(&worker_id, 0, 100, 12.0, 6000, Some(session))
            .await
            .unwrap();

        let real_interval = Duration::from_millis(25);
        let reaper_window = real_interval * 4;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let config = ReaperConfig::new(Duration::from_millis(10), reaper_window);
        let handle = spawn_reaper(registry.clone(), config, shutdown_rx);

        // Before 4 * real_interval (at 2 * real_interval), the battery worker is still Connected.
        tokio::time::sleep(real_interval * 2).await;
        assert_eq!(
            registry.get_worker(worker_id).await.unwrap().status,
            WorkerStatus::Connected
        );

        // Once 4 * real_interval has elapsed, the reaper marks the battery worker Disconnected
        // instead of waiting 240 s (4 * 60 s).
        tokio::time::sleep(real_interval * 4).await;
        assert_eq!(
            registry.get_worker(worker_id).await.unwrap().status,
            WorkerStatus::Disconnected
        );

        let _ = shutdown_tx.send(true);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn test_reaper_keeps_wifi_and_cellular_phones_alive_until_four_times_interval() {
        use rusty_grid_core::capabilities::{MobileCapabilities, MobileLiveStatus};

        let registry = WorkerRegistry::new();
        let (tx1, _rx1) = mpsc::channel(16);
        let (tx2, _rx2) = mpsc::channel(16);
        let wifi_id = Uuid::new_v4();
        let cell_id = Uuid::new_v4();

        let mut wifi_caps = WorkerCapabilities::new("wifi-phone", 4, 4096, false, false, None);
        wifi_caps.mobile = Some(MobileCapabilities {
            os_version: "Android 14".into(),
            soc_model: "Snapdragon 8 Gen 2".into(),
            battery_pct: Some(85),
            is_charging: Some(false),
            thermal_throttled: false,
        });
        let cell_caps = wifi_caps.clone();

        let wifi_session = registry
            .register(
                wifi_id,
                wifi_caps,
                "127.0.0.1:9003".parse().unwrap(),
                tx1,
                None,
            )
            .await
            .unwrap();
        let cell_session = registry
            .register(
                cell_id,
                cell_caps,
                "127.0.0.1:9004".parse().unwrap(),
                tx2,
                None,
            )
            .await
            .unwrap();

        registry
            .update_mobile_telemetry(
                &wifi_id,
                Some(wifi_session),
                MobileLiveStatus {
                    battery_pct: Some(85),
                    is_charging: Some(false),
                    network_type: Some("wifi".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        registry
            .update_mobile_telemetry(
                &cell_id,
                Some(cell_session),
                MobileLiveStatus {
                    battery_pct: Some(85),
                    is_charging: Some(false),
                    network_type: Some("cellular".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Spawn reaper with a short 50 ms default timeout: neither the 60 s Wi-Fi phone (240 s
        // window) nor the 25 s cellular phone (100 s window) may be reaped after 120 ms.
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let config = ReaperConfig::new(Duration::from_millis(15), Duration::from_millis(50));
        let handle = spawn_reaper(registry.clone(), config, shutdown_rx);

        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(
            registry.get_worker(wifi_id).await.unwrap().status,
            WorkerStatus::Connected,
            "Wi-Fi phone with 60 s interval must survive past the 50 ms default reaper timeout"
        );
        assert_eq!(
            registry.get_worker(cell_id).await.unwrap().status,
            WorkerStatus::Connected,
            "Cellular phone with 25 s interval must survive past the 50 ms default reaper timeout"
        );

        // At 95 s of silence: cellular (100 s window) and Wi-Fi (240 s window) are both still Connected.
        registry
            .backdate_last_heartbeat(&cell_id, Duration::from_secs(95))
            .await;
        registry
            .backdate_last_heartbeat(&wifi_id, Duration::from_secs(235))
            .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            registry.get_worker(cell_id).await.unwrap().status,
            WorkerStatus::Connected
        );
        assert_eq!(
            registry.get_worker(wifi_id).await.unwrap().status,
            WorkerStatus::Connected
        );

        // At 105 s of silence, cellular (4 × 25 s = 100 s) is reaped while Wi-Fi at 235 s stays Connected.
        registry
            .backdate_last_heartbeat(&cell_id, Duration::from_secs(105))
            .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            registry.get_worker(cell_id).await.unwrap().status,
            WorkerStatus::Disconnected
        );
        assert_eq!(
            registry.get_worker(wifi_id).await.unwrap().status,
            WorkerStatus::Connected
        );

        // At 245 s of silence, Wi-Fi (4 × 60 s = 240 s) is reaped.
        registry
            .backdate_last_heartbeat(&wifi_id, Duration::from_secs(245))
            .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            registry.get_worker(wifi_id).await.unwrap().status,
            WorkerStatus::Disconnected
        );

        let _ = shutdown_tx.send(true);
        let _ = handle.await;
    }
}
