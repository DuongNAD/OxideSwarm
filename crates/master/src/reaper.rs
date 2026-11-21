//! Master periodic heartbeat reaper background monitor.
//!
//! Sweeps the `WorkerRegistry` at configured intervals (default: 1.0s) and transitions
//! any worker whose last heartbeat exceeds the timeout (default: 10.0s) to `WorkerStatus::Disconnected`.

use rusty_grid_core::protocol::MasterMessage;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{debug, info, warn};

use crate::queue::TaskQueue;
use crate::registry::{WorkerInfo, WorkerRegistry, WorkerStatus};
use crate::scheduler::{
    compute_mobile_flags, effective_worker_limits, placement_admits, statically_fits,
    SchedulerConfig,
};

/// Minimum elapsed time before a running `opt:repeatable` task qualifies as a straggler (`0060` D3).
pub const DEFAULT_STRAGGLER_MIN_ELAPSED: Duration = Duration::from_secs(10);
/// Ratio above the fastest matching worker's `SpeedBook` average required for straggler re-run.
pub const DEFAULT_STRAGGLER_MULTIPLIER: f64 = 3.0;

/// Configuration parameters for the Master Heartbeat Reaper monitor.
#[derive(Debug, Clone)]
pub struct ReaperConfig {
    /// How frequently the reaper scans the registry for stale workers (default: 1.0 second).
    pub scan_interval: Duration,
    /// Inactivity threshold after which an uncommunicative worker is declared Disconnected (default: 10.0 seconds).
    pub timeout: Duration,
    /// Inactivity threshold after which a disconnected worker is pruned from memory (default: 1 hour).
    pub stale_worker_timeout: Option<Duration>,
    /// Scheduler configuration shared with the straggler pass so eligibility and slot limits match.
    pub scheduler_config: SchedulerConfig,
    /// Minimum running duration before an `opt:repeatable` task can spawn a speculative copy.
    pub straggler_min_elapsed: Duration,
    /// Multiple of the fastest matching worker's average duration required to trigger a copy.
    pub straggler_multiplier: f64,
}

impl Default for ReaperConfig {
    fn default() -> Self {
        Self {
            scan_interval: Duration::from_secs(1),
            timeout: Duration::from_secs(10),
            stale_worker_timeout: Some(Duration::from_secs(3600)),
            scheduler_config: SchedulerConfig::default(),
            straggler_min_elapsed: DEFAULT_STRAGGLER_MIN_ELAPSED,
            straggler_multiplier: DEFAULT_STRAGGLER_MULTIPLIER,
        }
    }
}

impl ReaperConfig {
    pub fn new(scan_interval: Duration, timeout: Duration) -> Self {
        Self {
            scan_interval,
            timeout,
            ..Self::default()
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

    pub fn with_scheduler_config(mut self, scheduler_config: SchedulerConfig) -> Self {
        self.scheduler_config = scheduler_config;
        self
    }

    pub fn with_straggler_min_elapsed(mut self, min_elapsed: Duration) -> Self {
        self.straggler_min_elapsed = min_elapsed;
        self
    }

    pub fn with_straggler_multiplier(mut self, multiplier: f64) -> Self {
        self.straggler_multiplier = multiplier;
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
                    run_straggler_pass(&registry, &queue, &config).await;
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
                    run_straggler_pass(&registry, &queue, &config).await;
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

/// Scans running `opt:repeatable` tasks for stragglers (`0060` D3) and dispatches at most one
/// speculative copy onto an idle matching worker within the 10% cluster slot cap.
pub(crate) async fn run_straggler_pass(
    registry: &WorkerRegistry,
    queue: &TaskQueue,
    config: &ReaperConfig,
) {
    if !config.straggler_multiplier.is_finite() || config.straggler_multiplier <= 0.0 {
        return;
    }
    let Some(sb) = registry.speed_book() else {
        return;
    };
    let speed_book = { sb.lock().unwrap_or_else(|e| e.into_inner()).clone() };
    if speed_book.is_empty() {
        return;
    }

    let (in_flight_spec_init, mut candidates) = queue.speculative_snapshot().await;
    candidates.retain(|c| c.elapsed > config.straggler_min_elapsed);
    if candidates.is_empty() {
        return;
    }

    let (schedulable, registered, _) = registry.scheduler_snapshot().await;
    let connected_workers: Vec<&WorkerInfo> = registered
        .iter()
        .filter(|w| w.status != WorkerStatus::Disconnected)
        .collect();
    let total_cluster_slots: usize = connected_workers
        .iter()
        .map(|w| effective_worker_limits(&config.scheduler_config, w).1)
        .sum();
    if total_cluster_slots == 0 {
        return;
    }
    let max_spec_slots = (total_cluster_slots / 10).max(1);
    let mut in_flight_spec_slots = in_flight_spec_init;
    if in_flight_spec_slots >= max_spec_slots {
        return;
    }

    let mut loads = queue.assigned_loads().await;
    candidates.sort_by(|a, b| {
        b.elapsed
            .cmp(&a.elapsed)
            .then_with(|| a.view.task_id.0.cmp(&b.view.task_id.0))
    });

    for cand in candidates {
        let weight = cand.view.requirements.cpu_cores.max(1);
        if in_flight_spec_slots.saturating_add(weight) > max_spec_slots {
            continue;
        }
        let Ok(ref placement) = cand.view.placement else {
            continue;
        };
        let req = &cand.view.requirements;
        let req_vram = req.effective_vram_mb();
        let mobile_bucket = (usize::from(req.cpu_cores > 1)) | (usize::from(cand.view.heavy) << 1);

        let fastest_matching_avg_ms = connected_workers
            .iter()
            .filter(|w| {
                (!cand.view.requires_gpu || w.capabilities.can_execute_gpu())
                    && w.features.contains(cand.view.required_features)
                    && statically_fits(req, w)
                    && placement_admits(placement, w)
                    && compute_mobile_flags(&config.scheduler_config, w)[mobile_bucket]
            })
            .filter_map(|w| {
                let entry = speed_book.get(&w.worker_id, cand.view.task_key)?;
                (entry.samples >= 1 && entry.avg_ms.is_finite() && entry.avg_ms > 0.0)
                    .then_some(entry.avg_ms)
            })
            .reduce(f64::min);

        let Some(fastest_avg_ms) = fastest_matching_avg_ms else {
            continue;
        };
        let elapsed_ms = cand.elapsed.as_secs_f64() * 1_000.0;
        if !elapsed_ms.is_finite() || elapsed_ms <= config.straggler_multiplier * fastest_avg_ms {
            continue;
        }

        let target = schedulable
            .iter()
            .filter(|w| {
                w.worker_id != cand.primary_worker_id
                    && !cand.view.avoid.contains(&w.worker_id)
                    && (!cand.view.requires_gpu || w.capabilities.can_execute_gpu())
                    && w.features.contains(cand.view.required_features)
                    && statically_fits(req, w)
                    && placement_admits(placement, w)
                    && compute_mobile_flags(&config.scheduler_config, w)[mobile_bucket]
                    && w.cpu_usage_pct <= config.scheduler_config.max_host_cpu_pct
            })
            .filter(|w| {
                let (max_slots, max_tasks) = effective_worker_limits(&config.scheduler_config, w);
                let load = loads.get(&w.worker_id).copied().unwrap_or_default();
                let rem_slots = if load.tasks >= max_tasks {
                    0
                } else {
                    max_slots.saturating_sub(load.slots)
                };
                if rem_slots < weight {
                    return false;
                }
                if req.ram_mb > 0
                    && w.ram_available_mb > 0
                    && w.ram_available_mb.saturating_sub(load.unstarted_ram_mb) < req.ram_mb
                {
                    return false;
                }
                if req_vram > 0
                    && w.capabilities
                        .vram_mb
                        .is_some_and(|cap| cap.saturating_sub(load.vram_mb) < req_vram)
                {
                    return false;
                }
                true
            })
            .min_by(|a, b| {
                let avg_a = speed_book
                    .get(&a.worker_id, cand.view.task_key)
                    .map_or(f64::INFINITY, |e| e.avg_ms);
                let avg_b = speed_book
                    .get(&b.worker_id, cand.view.task_key)
                    .map_or(f64::INFINITY, |e| e.avg_ms);
                let load_a = loads.get(&a.worker_id).map_or(0, |l| l.tasks);
                let load_b = loads.get(&b.worker_id).map_or(0, |l| l.tasks);
                let gpu_spill_a = !cand.view.requires_gpu && a.capabilities.can_execute_gpu();
                let gpu_spill_b = !cand.view.requires_gpu && b.capabilities.can_execute_gpu();
                avg_a
                    .total_cmp(&avg_b)
                    .then_with(|| gpu_spill_a.cmp(&gpu_spill_b))
                    .then_with(|| load_a.cmp(&load_b))
                    .then_with(|| a.worker_id.cmp(&b.worker_id))
            });

        let Some(target) = target else {
            continue;
        };

        let peer_version = registry
            .peer_version_of(&target.worker_id, target.session_id)
            .await;

        let Ok((sender, session_live)) = registry
            .reserve_dispatch(&target.worker_id, target.session_id)
            .await
        else {
            continue;
        };

        match queue
            .try_start_speculative_and_deliver(
                cand.view.task_id,
                target.worker_id,
                target.session_id,
                peer_version,
                |task| {
                    session_live.load(Ordering::Acquire)
                        && sender.try_send(MasterMessage::AssignTask { task }).is_ok()
                },
            )
            .await
        {
            crate::queue::SpeculativeStartOutcome::Delivered => {
                let load = loads.entry(target.worker_id).or_default();
                load.tasks += 1;
                load.slots += weight;
                load.unstarted_ram_mb += req.ram_mb;
                load.vram_mb = load.vram_mb.saturating_add(req_vram);
                in_flight_spec_slots += weight;
                info!(
                    task_id = %cand.view.task_id,
                    primary_worker_id = %cand.primary_worker_id,
                    speculative_worker_id = %target.worker_id,
                    elapsed_ms = elapsed_ms as u64,
                    fastest_avg_ms = fastest_avg_ms as u64,
                    "Dispatched speculative straggler copy for repeatable task"
                );
            }
            crate::queue::SpeculativeStartOutcome::NotEligible
            | crate::queue::SpeculativeStartOutcome::WithheldFromPeer
            | crate::queue::SpeculativeStartOutcome::DeliveryFailed => {
                registry
                    .release_dispatch_if_live(&target.worker_id, target.session_id, &session_live)
                    .await;
            }
        }
    }
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
        let registered_at = registry
            .last_heartbeat_instant(&worker_id)
            .await
            .expect("registered worker must have heartbeat instant");

        let reaper_timeout = Duration::from_millis(50);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let config = ReaperConfig::new(Duration::from_millis(10), reaper_timeout);

        let handle = spawn_reaper(registry.clone(), config, shutdown_rx);

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let info = registry.get_worker(worker_id).await.unwrap();
            if info.status == WorkerStatus::Disconnected {
                assert!(
                    registered_at.elapsed() >= reaper_timeout,
                    "reaper must not transition worker before {:?} timeout (elapsed {:?})",
                    reaper_timeout,
                    registered_at.elapsed()
                );
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed-out worker was not transitioned to Disconnected within 5s"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

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
        let hb_at = registry
            .last_heartbeat_instant(&worker_id)
            .await
            .expect("battery worker must have heartbeat instant");

        let real_interval = Duration::from_millis(50);
        let reaper_window = real_interval * 4;
        // Before 4 * real_interval, the battery worker is not reaped.
        assert!(registry
            .reap_stale_sessions_at(hb_at + real_interval * 2, reaper_window)
            .await
            .is_empty());
        assert_eq!(
            registry.get_worker(worker_id).await.unwrap().status,
            WorkerStatus::Connected
        );

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let config = ReaperConfig::new(Duration::from_millis(10), reaper_window);
        let handle = spawn_reaper(registry.clone(), config, shutdown_rx);

        // Once 4 * real_interval has elapsed, the reaper marks the battery worker Disconnected
        // instead of waiting 240 s (4 * 60 s).
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if registry.get_worker(worker_id).await.unwrap().status == WorkerStatus::Disconnected {
                assert!(
                    hb_at.elapsed() >= reaper_window,
                    "battery worker reaped before {:?} window (elapsed {:?})",
                    reaper_window,
                    hb_at.elapsed()
                );
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "battery worker was not reaped within 5s (instead of waiting 240s)"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

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

        let cell_hb = registry
            .last_heartbeat_instant(&cell_id)
            .await
            .expect("cellular last_heartbeat_instant");
        let wifi_hb = registry
            .last_heartbeat_instant(&wifi_id)
            .await
            .expect("wifi last_heartbeat_instant");

        // At 95 s of silence: cellular (100 s window) and Wi-Fi (240 s window) are both still Connected.
        assert!(registry
            .reap_stale_sessions_at(cell_hb + Duration::from_secs(95), Duration::from_millis(50))
            .await
            .is_empty());
        assert_eq!(
            registry.get_worker(cell_id).await.unwrap().status,
            WorkerStatus::Connected
        );
        assert_eq!(
            registry.get_worker(wifi_id).await.unwrap().status,
            WorkerStatus::Connected
        );

        // At 105 s of silence, cellular (4 × 25 s = 100 s) is reaped while Wi-Fi stays Connected.
        let reaped_at_105 = registry
            .reap_stale_sessions_at(
                cell_hb + Duration::from_secs(105),
                Duration::from_millis(50),
            )
            .await;
        assert_eq!(reaped_at_105, vec![(cell_id, cell_session)]);
        assert_eq!(
            registry.get_worker(cell_id).await.unwrap().status,
            WorkerStatus::Disconnected
        );
        assert_eq!(
            registry.get_worker(wifi_id).await.unwrap().status,
            WorkerStatus::Connected
        );

        // At 235 s of silence, Wi-Fi (4 × 60 s = 240 s) is still Connected.
        assert!(registry
            .reap_stale_sessions_at(
                wifi_hb + Duration::from_secs(235),
                Duration::from_millis(50),
            )
            .await
            .is_empty());
        assert_eq!(
            registry.get_worker(wifi_id).await.unwrap().status,
            WorkerStatus::Connected
        );

        // At 245 s of silence, Wi-Fi (4 × 60 s = 240 s) is reaped.
        let reaped_at_245 = registry
            .reap_stale_sessions_at(
                wifi_hb + Duration::from_secs(245),
                Duration::from_millis(50),
            )
            .await;
        assert_eq!(reaped_at_245, vec![(wifi_id, wifi_session)]);
        assert_eq!(
            registry.get_worker(wifi_id).await.unwrap().status,
            WorkerStatus::Disconnected
        );

        let _ = shutdown_tx.send(true);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn sched4c_reaper_straggler_pass_respects_floor_3x_repeatable_and_10pct_cap() {
        use crate::speed::{mutate_shared_speed_book, new_shared_speed_book, TaskKey};
        use rusty_grid_core::placement::OPT_REPEATABLE_TAG;
        use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};

        let registry = WorkerRegistry::new();
        let speed_book = new_shared_speed_book();
        registry.set_speed_book(Arc::clone(&speed_book));
        let queue = TaskQueue::new();

        // 10 total cluster slots: w_fast (8), w_slow1 (1), w_slow2 (1) => cap = (10 / 10).max(1) = 1.
        let w_fast = Uuid::new_v4();
        let w_slow1 = Uuid::new_v4();
        let w_slow2 = Uuid::new_v4();
        let (tx_fast, mut rx_fast) = mpsc::channel(16);
        let (tx_s1, _rx_s1) = mpsc::channel(16);
        let (tx_s2, _rx_s2) = mpsc::channel(16);

        let s_fast = registry
            .register(
                w_fast,
                WorkerCapabilities::new("fast-8", 8, 16_384, false, false, None),
                "127.0.0.1:9601".parse().unwrap(),
                tx_fast,
                None,
            )
            .await
            .unwrap();
        let s_s1 = registry
            .register(
                w_slow1,
                WorkerCapabilities::new("slow-1", 1, 4096, false, false, None),
                "127.0.0.1:9602".parse().unwrap(),
                tx_s1,
                None,
            )
            .await
            .unwrap();
        let s_s2 = registry
            .register(
                w_slow2,
                WorkerCapabilities::new("slow-2", 1, 4096, false, false, None),
                "127.0.0.1:9603".parse().unwrap(),
                tx_s2,
                None,
            )
            .await
            .unwrap();

        let spec = TaskSpec::command("sleep", vec!["20".into()]);
        let key = TaskKey::for_task(&Task::new(spec.clone(), TaskRequirements::generic(1, 64)));
        // Fastest average is 50 ms, so 3x = 150 ms.
        mutate_shared_speed_book(&speed_book, |sb| {
            sb.record_sample(w_fast, key, 50, 60);
        });

        let non_rep = Task::new(spec.clone(), TaskRequirements::generic(1, 64));
        let non_rep_id = queue.submit(non_rep).await.unwrap();
        let rep1 = Task::new(spec.clone(), TaskRequirements::generic(1, 64))
            .with_tags(vec![OPT_REPEATABLE_TAG.to_string()]);
        let rep1_id = queue.submit(rep1).await.unwrap();
        let rep2 = Task::new(spec, TaskRequirements::generic(1, 64))
            .with_tags(vec![OPT_REPEATABLE_TAG.to_string()]);
        let rep2_id = queue.submit(rep2).await.unwrap();

        // Start rep1 on slow-1, rep2 on slow-2, and non_rep on fast (just to keep non_rep running).
        queue
            .schedule_tasks_batch_in_sessions(&[
                (rep1_id, w_slow1, s_s1),
                (rep2_id, w_slow2, s_s2),
                (non_rep_id, w_fast, s_fast),
            ])
            .await;
        queue
            .mark_running_from(w_slow1, Some(s_s1), &rep1_id)
            .await
            .unwrap();
        queue
            .mark_running_from(w_slow2, Some(s_s2), &rep2_id)
            .await
            .unwrap();
        queue
            .mark_running_from(w_fast, Some(s_fast), &non_rep_id)
            .await
            .unwrap();

        // 1. Under the 10s floor (with default ReaperConfig), nothing is duplicated.
        run_straggler_pass(&registry, &queue, &ReaperConfig::default()).await;
        assert!(rx_fast.try_recv().is_err());

        // 2. With a 200ms threshold, non_rep is ignored, and only 1 repeatable task gets a
        //    speculative copy on w_fast because 10% of 10 cluster slots = 1 slot cap.
        tokio::time::sleep(Duration::from_millis(220)).await;
        let fast_cfg = ReaperConfig::default()
            .with_scheduler_config(SchedulerConfig::default().without_host_cpu_backpressure())
            .with_straggler_min_elapsed(Duration::from_millis(200))
            .with_straggler_multiplier(3.0);
        run_straggler_pass(&registry, &queue, &fast_cfg).await;

        let msg = rx_fast
            .try_recv()
            .expect("expected 1 speculative AssignTask on fast worker");
        match msg {
            MasterMessage::AssignTask { task } => {
                assert!(task.id == rep1_id || task.id == rep2_id);
                assert_ne!(task.id, non_rep_id);
            }
            other => panic!("unexpected message: {other:?}"),
        }
        assert!(
            rx_fast.try_recv().is_err(),
            "10% cluster slot cap (1 of 10 slots) must prevent a second speculative copy"
        );
    }

    #[tokio::test]
    async fn tier5_reaper_20_slot_cluster_allows_2_speculative_and_excludes_workspace_tasks() {
        use crate::speed::{mutate_shared_speed_book, new_shared_speed_book, TaskKey};
        use rusty_grid_core::placement::OPT_REPEATABLE_TAG;
        use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec, WorkspaceRef};

        let registry = WorkerRegistry::new();
        let speed_book = new_shared_speed_book();
        registry.set_speed_book(Arc::clone(&speed_book));
        let queue = TaskQueue::new();

        // 20 total cluster slots: w_fast (16), w_s1 (1), w_s2 (1), w_s3 (2) => cap = 20 / 10 = 2.
        let w_fast = Uuid::new_v4();
        let w_s1 = Uuid::new_v4();
        let w_s2 = Uuid::new_v4();
        let w_s3 = Uuid::new_v4();
        let (tx_fast, mut rx_fast) = mpsc::channel(16);
        let (tx_s1, _rx_s1) = mpsc::channel(16);
        let (tx_s2, _rx_s2) = mpsc::channel(16);
        let (tx_s3, _rx_s3) = mpsc::channel(16);

        let s_fast = registry
            .register(
                w_fast,
                WorkerCapabilities::new("fast-16", 16, 32_768, false, false, None),
                "127.0.0.1:9611".parse().unwrap(),
                tx_fast,
                None,
            )
            .await
            .unwrap();
        let s_s1 = registry
            .register(
                w_s1,
                WorkerCapabilities::new("slow-1", 1, 4096, false, false, None),
                "127.0.0.1:9612".parse().unwrap(),
                tx_s1,
                None,
            )
            .await
            .unwrap();
        let s_s2 = registry
            .register(
                w_s2,
                WorkerCapabilities::new("slow-2", 1, 4096, false, false, None),
                "127.0.0.1:9613".parse().unwrap(),
                tx_s2,
                None,
            )
            .await
            .unwrap();
        let s_s3 = registry
            .register(
                w_s3,
                WorkerCapabilities::new("slow-3", 2, 4096, false, false, None),
                "127.0.0.1:9614".parse().unwrap(),
                tx_s3,
                None,
            )
            .await
            .unwrap();

        let spec = TaskSpec::command("sleep", vec!["20".into()]);
        let key = TaskKey::for_task(&Task::new(spec.clone(), TaskRequirements::generic(1, 64)));
        mutate_shared_speed_book(&speed_book, |sb| {
            sb.record_sample(w_fast, key, 50, 60);
        });

        let rep1 = Task::new(spec.clone(), TaskRequirements::generic(1, 64))
            .with_tags(vec![OPT_REPEATABLE_TAG.to_string()]);
        let rep1_id = queue.submit(rep1).await.unwrap();
        let rep2 = Task::new(spec.clone(), TaskRequirements::generic(1, 64))
            .with_tags(vec![OPT_REPEATABLE_TAG.to_string()]);
        let rep2_id = queue.submit(rep2).await.unwrap();
        let rep3 = Task::new(spec.clone(), TaskRequirements::generic(1, 64))
            .with_tags(vec![OPT_REPEATABLE_TAG.to_string()]);
        let rep3_id = queue.submit(rep3).await.unwrap();
        let ws_rep = Task::new(spec, TaskRequirements::generic(1, 64))
            .with_tags(vec![OPT_REPEATABLE_TAG.to_string()])
            .with_workspace(WorkspaceRef {
                name: "shared-ws".to_string(),
                source: None,
            });
        let ws_rep_id = queue.submit(ws_rep).await.unwrap();

        queue
            .schedule_tasks_batch_in_sessions(&[
                (rep1_id, w_s1, s_s1),
                (rep2_id, w_s2, s_s2),
                (rep3_id, w_s3, s_s3),
                (ws_rep_id, w_s3, s_s3),
            ])
            .await;
        for (wid, sid, tid) in [
            (w_s1, s_s1, rep1_id),
            (w_s2, s_s2, rep2_id),
            (w_s3, s_s3, rep3_id),
            (w_s3, s_s3, ws_rep_id),
        ] {
            queue.mark_running_from(wid, Some(sid), &tid).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(220)).await;
        let fast_cfg = ReaperConfig::default()
            .with_scheduler_config(SchedulerConfig::default().without_host_cpu_backpressure())
            .with_straggler_min_elapsed(Duration::from_millis(200))
            .with_straggler_multiplier(3.0);
        run_straggler_pass(&registry, &queue, &fast_cfg).await;

        let mut dispatched = Vec::new();
        while let Ok(MasterMessage::AssignTask { task }) = rx_fast.try_recv() {
            assert_ne!(
                task.id, ws_rep_id,
                "workspace-bound task must never receive a speculative copy"
            );
            dispatched.push(task.id);
        }
        assert_eq!(
            dispatched.len(),
            2,
            "20-slot cluster must permit 2 concurrent speculative copies, got {dispatched:?}"
        );

        // Complete the first speculative copy -> 3rd repeatable straggler can now be speculated,
        // while ws_rep remains excluded.
        let completed_id = dispatched[0];
        let _ = queue
            .record_result_from(
                w_fast,
                Some(s_fast),
                rusty_grid_core::task::TaskResult::success(w_fast, completed_id, "ok", 50, false),
            )
            .await
            .unwrap();
        registry.release_dispatch(&w_fast, s_fast).await;

        run_straggler_pass(&registry, &queue, &fast_cfg).await;
        let mut second_wave = Vec::new();
        while let Ok(MasterMessage::AssignTask { task }) = rx_fast.try_recv() {
            assert_ne!(task.id, ws_rep_id);
            second_wave.push(task.id);
        }
        assert_eq!(second_wave.len(), 1);
        assert!(!dispatched.contains(&second_wave[0]));
        assert!([rep1_id, rep2_id, rep3_id].contains(&second_wave[0]));
    }

    #[tokio::test]
    async fn tier5_reaper_cold_start_no_speed_samples_does_not_speculate() {
        use crate::speed::new_shared_speed_book;
        use rusty_grid_core::placement::OPT_REPEATABLE_TAG;
        use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};

        let registry = WorkerRegistry::new();
        registry.set_speed_book(new_shared_speed_book());
        let queue = TaskQueue::new();

        let w_fast = Uuid::new_v4();
        let w_slow = Uuid::new_v4();
        let (tx_fast, mut rx_fast) = mpsc::channel(16);
        let (tx_slow, _rx_slow) = mpsc::channel(16);
        registry
            .register(
                w_fast,
                WorkerCapabilities::new("fast", 4, 8192, false, false, None),
                "127.0.0.1:9621".parse().unwrap(),
                tx_fast,
                None,
            )
            .await
            .unwrap();
        let s_slow = registry
            .register(
                w_slow,
                WorkerCapabilities::new("slow", 1, 4096, false, false, None),
                "127.0.0.1:9622".parse().unwrap(),
                tx_slow,
                None,
            )
            .await
            .unwrap();

        let rep = Task::new(
            TaskSpec::command("sleep", vec!["20".into()]),
            TaskRequirements::generic(1, 64),
        )
        .with_tags(vec![OPT_REPEATABLE_TAG.to_string()]);
        let rep_id = queue.submit(rep).await.unwrap();
        queue
            .schedule_tasks_batch_in_sessions(&[(rep_id, w_slow, s_slow)])
            .await;
        queue
            .mark_running_from(w_slow, Some(s_slow), &rep_id)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(220)).await;
        let fast_cfg = ReaperConfig::default()
            .with_scheduler_config(SchedulerConfig::default().without_host_cpu_backpressure())
            .with_straggler_min_elapsed(Duration::from_millis(200))
            .with_straggler_multiplier(3.0);
        run_straggler_pass(&registry, &queue, &fast_cfg).await;

        assert!(
            rx_fast.try_recv().is_err(),
            "cold-start SpeedBook with zero samples must not emit speculative copies"
        );
    }

    #[tokio::test]
    async fn tier5_reaper_rollback_branches_withheld_reserve_fail_and_channel_full() {
        use crate::speed::{mutate_shared_speed_book, new_shared_speed_book, TaskKey};
        use rusty_grid_core::placement::OPT_REPEATABLE_TAG;
        use rusty_grid_core::task::{Extension, Task, TaskRequirements, TaskSpec};

        let fast_cfg = ReaperConfig::default()
            .with_scheduler_config(SchedulerConfig::default().without_host_cpu_backpressure())
            .with_straggler_min_elapsed(Duration::from_millis(200))
            .with_straggler_multiplier(3.0);

        // Branch 1: target worker on protocol v9 with critical Worker extension -> WithheldFromPeer.
        {
            let registry = WorkerRegistry::new();
            let sb = new_shared_speed_book();
            registry.set_speed_book(Arc::clone(&sb));
            let queue = TaskQueue::new();

            let w_v9 = Uuid::new_v4();
            let w_slow = Uuid::new_v4();
            let (tx_v9, mut rx_v9) = mpsc::channel(4);
            let (tx_slow, _rx_slow) = mpsc::channel(4);
            let s_v9 = registry
                .register(
                    w_v9,
                    WorkerCapabilities::new("fast-v9", 8, 16384, false, false, None),
                    "127.0.0.1:9631".parse().unwrap(),
                    tx_v9,
                    None,
                )
                .await
                .unwrap();
            registry.set_peer_version(&w_v9, s_v9, 9).await;
            let s_slow = registry
                .register(
                    w_slow,
                    WorkerCapabilities::new("slow-v10", 1, 4096, false, false, None),
                    "127.0.0.1:9632".parse().unwrap(),
                    tx_slow,
                    None,
                )
                .await
                .unwrap();

            let mut task = Task::new(
                TaskSpec::command("sleep", vec!["20".into()]),
                TaskRequirements::generic(1, 64),
            )
            .with_tags(vec![OPT_REPEATABLE_TAG.to_string()]);
            task.extensions = vec![Extension {
                id: 0x8002,
                critical: true,
                bytes: vec![1],
            }];
            let key = TaskKey::for_task(&task);
            mutate_shared_speed_book(&sb, |b| b.record_sample(w_v9, key, 40, 60));

            let tid = queue.submit(task).await.unwrap();
            queue
                .schedule_tasks_batch_in_sessions(&[(tid, w_slow, s_slow)])
                .await;
            queue
                .mark_running_from(w_slow, Some(s_slow), &tid)
                .await
                .unwrap();

            tokio::time::sleep(Duration::from_millis(220)).await;
            run_straggler_pass(&registry, &queue, &fast_cfg).await;

            assert!(rx_v9.try_recv().is_err());
            assert_eq!(registry.get_worker(w_v9).await.unwrap().active_tasks, 0);
            let (in_flight, cands) = queue.speculative_snapshot().await;
            assert_eq!(in_flight, 0);
            assert_eq!(cands.len(), 1);
        }

        // Branch 2: channel full -> DeliveryFailed -> slot released and speculative not recorded.
        {
            let registry = WorkerRegistry::new();
            let sb = new_shared_speed_book();
            registry.set_speed_book(Arc::clone(&sb));
            let queue = TaskQueue::new();

            let w_full = Uuid::new_v4();
            let w_slow = Uuid::new_v4();
            let (tx_full, _rx_full) = mpsc::channel(1);
            // Pre-fill the 1-slot outbound channel so try_send fails.
            tx_full
                .try_send(MasterMessage::Shutdown {
                    reason: "fill".into(),
                    grace_period_secs: None,
                })
                .unwrap();
            let (tx_slow, _rx_slow) = mpsc::channel(4);
            let _s_full = registry
                .register(
                    w_full,
                    WorkerCapabilities::new("fast-full", 8, 16384, false, false, None),
                    "127.0.0.1:9633".parse().unwrap(),
                    tx_full,
                    None,
                )
                .await
                .unwrap();
            let s_slow = registry
                .register(
                    w_slow,
                    WorkerCapabilities::new("slow", 1, 4096, false, false, None),
                    "127.0.0.1:9634".parse().unwrap(),
                    tx_slow,
                    None,
                )
                .await
                .unwrap();

            let task = Task::new(
                TaskSpec::command("sleep", vec!["20".into()]),
                TaskRequirements::generic(1, 64),
            )
            .with_tags(vec![OPT_REPEATABLE_TAG.to_string()]);
            let key = TaskKey::for_task(&task);
            mutate_shared_speed_book(&sb, |b| b.record_sample(w_full, key, 40, 60));

            let tid = queue.submit(task).await.unwrap();
            queue
                .schedule_tasks_batch_in_sessions(&[(tid, w_slow, s_slow)])
                .await;
            queue
                .mark_running_from(w_slow, Some(s_slow), &tid)
                .await
                .unwrap();

            tokio::time::sleep(Duration::from_millis(220)).await;
            run_straggler_pass(&registry, &queue, &fast_cfg).await;

            assert_eq!(registry.get_worker(w_full).await.unwrap().active_tasks, 0);
            let (in_flight, cands) = queue.speculative_snapshot().await;
            assert_eq!(in_flight, 0);
            assert_eq!(cands.len(), 1);

            // Branch 3: disconnected target -> reserve_dispatch Err handled cleanly without panic.
            registry.mark_disconnected(&w_full).await.unwrap();
            run_straggler_pass(&registry, &queue, &fast_cfg).await;
            let (in_flight2, cands2) = queue.speculative_snapshot().await;
            assert_eq!(in_flight2, 0);
            assert_eq!(cands2.len(), 1);
        }
    }

    #[tokio::test]
    async fn tier5_reaper_bug2_promotion_and_completion_during_speculative_dispatch() {
        use crate::queue::SpeculativeStartOutcome;
        use rusty_grid_core::placement::OPT_REPEATABLE_TAG;
        use rusty_grid_core::task::{Task, TaskRequirements, TaskResult, TaskSpec};

        // Sub-case 2a: Primary fails after reserve_dispatch and before try_start_speculative_and_deliver
        // acquires queue lock, vs after Delivered. Never leaves a promoted task in Scheduled with empty rx_fast!
        {
            let registry = WorkerRegistry::new();
            let queue = TaskQueue::new();
            let w_fast = Uuid::new_v4();
            let w_slow = Uuid::new_v4();
            let (tx_fast, mut rx_fast) = mpsc::channel(8);
            let (tx_slow, _rx_slow) = mpsc::channel(8);
            let s_fast = registry
                .register(
                    w_fast,
                    WorkerCapabilities::new("fast", 4, 8192, false, false, None),
                    "127.0.0.1:9641".parse().unwrap(),
                    tx_fast,
                    None,
                )
                .await
                .unwrap();
            let s_slow = registry
                .register(
                    w_slow,
                    WorkerCapabilities::new("slow", 1, 4096, false, false, None),
                    "127.0.0.1:9642".parse().unwrap(),
                    tx_slow,
                    None,
                )
                .await
                .unwrap();

            // Case A: primary fails between reserve_dispatch and try_start_speculative_and_deliver.
            let t1 = Task::new(
                TaskSpec::command("echo", vec!["2a-before".into()]),
                TaskRequirements::generic(1, 64),
            )
            .with_tags(vec![OPT_REPEATABLE_TAG.to_string()]);
            let id1 = queue.submit(t1).await.unwrap();
            queue
                .schedule_tasks_batch_in_sessions(&[(id1, w_slow, s_slow)])
                .await;
            queue
                .mark_running_from(w_slow, Some(s_slow), &id1)
                .await
                .unwrap();

            let (sender, session_live) = registry.reserve_dispatch(&w_fast, s_fast).await.unwrap();
            let _ = queue
                .record_result_from(
                    w_slow,
                    Some(s_slow),
                    TaskResult::failure(w_slow, id1, 1, "", "boom", 50, None),
                )
                .await
                .unwrap();
            let outcome = queue
                .try_start_speculative_and_deliver(id1, w_fast, s_fast, Some(10), |task| {
                    session_live.load(Ordering::Acquire)
                        && sender.try_send(MasterMessage::AssignTask { task }).is_ok()
                })
                .await;
            assert_eq!(outcome, SpeculativeStartOutcome::NotEligible);
            registry.release_dispatch(&w_fast, s_fast).await;
            assert_eq!(registry.get_worker(w_fast).await.unwrap().active_tasks, 0);
            assert!(rx_fast.try_recv().is_err());
            assert_ne!(
                queue.get_state(&id1).await,
                Some(crate::queue::TaskState::Scheduled)
            );

            // Case B: primary fails after Delivered -> AssignTask is already in rx_fast.
            let t2 = Task::new(
                TaskSpec::command("echo", vec!["2a-after".into()]),
                TaskRequirements::generic(1, 64),
            )
            .with_tags(vec![OPT_REPEATABLE_TAG.to_string()]);
            let id2 = queue.submit(t2).await.unwrap();
            queue
                .schedule_tasks_batch_in_sessions(&[(id2, w_slow, s_slow)])
                .await;
            queue
                .mark_running_from(w_slow, Some(s_slow), &id2)
                .await
                .unwrap();
            let (sender2, session_live2) =
                registry.reserve_dispatch(&w_fast, s_fast).await.unwrap();
            let outcome2 = queue
                .try_start_speculative_and_deliver(id2, w_fast, s_fast, Some(10), |task| {
                    session_live2.load(Ordering::Acquire)
                        && sender2.try_send(MasterMessage::AssignTask { task }).is_ok()
                })
                .await;
            assert_eq!(outcome2, SpeculativeStartOutcome::Delivered);
            let fail_outcome = queue
                .record_result_from(
                    w_slow,
                    Some(s_slow),
                    TaskResult::failure(w_slow, id2, 1, "", "boom", 50, None),
                )
                .await
                .unwrap();
            assert_eq!(fail_outcome.state, crate::queue::TaskState::Scheduled);
            assert_eq!(
                queue.get_task(&id2).await.unwrap().assigned_worker_id,
                Some(w_fast)
            );
            assert!(matches!(
                rx_fast.try_recv(),
                Ok(MasterMessage::AssignTask { task }) if task.id == id2
            ));
            assert_eq!(registry.get_worker(w_fast).await.unwrap().active_tasks, 1);
        }

        // Sub-case 2b: Unrelated active task on w_fast must not be undercounted by double release_dispatch.
        {
            let registry = WorkerRegistry::new();
            let queue = TaskQueue::new();
            let w_fast = Uuid::new_v4();
            let w_slow = Uuid::new_v4();
            let (tx_fast, mut rx_fast) = mpsc::channel(8);
            let (tx_slow, _rx_slow) = mpsc::channel(8);
            let s_fast = registry
                .register(
                    w_fast,
                    WorkerCapabilities::new("fast", 8, 8192, false, false, None),
                    "127.0.0.1:9643".parse().unwrap(),
                    tx_fast,
                    None,
                )
                .await
                .unwrap();
            let s_slow = registry
                .register(
                    w_slow,
                    WorkerCapabilities::new("slow", 1, 4096, false, false, None),
                    "127.0.0.1:9644".parse().unwrap(),
                    tx_slow,
                    None,
                )
                .await
                .unwrap();

            // Reserve 1 unrelated active task on w_fast.
            let (_unrelated_sender, _) = registry.reserve_dispatch(&w_fast, s_fast).await.unwrap();
            assert_eq!(registry.get_worker(w_fast).await.unwrap().active_tasks, 1);

            // Primary completes between reserve_dispatch and try_start_speculative_and_deliver.
            let t = Task::new(
                TaskSpec::command("echo", vec!["2b".into()]),
                TaskRequirements::generic(1, 64),
            )
            .with_tags(vec![OPT_REPEATABLE_TAG.to_string()]);
            let id = queue.submit(t).await.unwrap();
            queue
                .schedule_tasks_batch_in_sessions(&[(id, w_slow, s_slow)])
                .await;
            queue
                .mark_running_from(w_slow, Some(s_slow), &id)
                .await
                .unwrap();

            let (spec_sender, spec_live) =
                registry.reserve_dispatch(&w_fast, s_fast).await.unwrap();
            assert_eq!(registry.get_worker(w_fast).await.unwrap().active_tasks, 2);

            let win = queue
                .record_result_from(
                    w_slow,
                    Some(s_slow),
                    TaskResult::success(w_slow, id, "done", 50, false),
                )
                .await
                .unwrap();
            // Primary completed before speculative was recorded, so cancel_sibling is None!
            assert_eq!(win.cancel_sibling, None);

            let outcome = queue
                .try_start_speculative_and_deliver(id, w_fast, s_fast, Some(10), |task| {
                    spec_live.load(Ordering::Acquire)
                        && spec_sender
                            .try_send(MasterMessage::AssignTask { task })
                            .is_ok()
                })
                .await;
            assert_eq!(outcome, SpeculativeStartOutcome::NotEligible);
            registry.release_dispatch(&w_fast, s_fast).await;

            // Unrelated task's slot count remains exactly 1 (never 0!).
            assert_eq!(registry.get_worker(w_fast).await.unwrap().active_tasks, 1);
            assert!(rx_fast.try_recv().is_err());
        }
    }

    #[tokio::test]
    async fn tier5_reaper_bug2c_target_disconnect_during_speculative_dispatch() {
        use crate::queue::{SpeculativeStartOutcome, TaskState};
        use crate::registry::HeartbeatOutcome;
        use rusty_grid_core::placement::OPT_REPEATABLE_TAG;
        use rusty_grid_core::protocol::ProtocolCapabilities;
        use rusty_grid_core::task::{Task, TaskRequirements, TaskResult, TaskSpec};

        // Case 1: Target worker disconnects (unregister + sweep_worker_tasks) AFTER
        // reserve_dispatch and BEFORE try_start_speculative_and_deliver while its writer task
        // still keeps outbound_rx open. Delivery must fail cleanly without recording an orphaned
        // SpeculativeAttempt on the Disconnected worker.
        {
            let registry = WorkerRegistry::new();
            let queue = TaskQueue::new();
            let notify = tokio::sync::Notify::new();
            let w_fast = Uuid::new_v4();
            let w_slow = Uuid::new_v4();
            let (tx_fast, mut rx_fast) = mpsc::channel(8);
            let (tx_slow, _rx_slow) = mpsc::channel(8);
            let s_fast = registry
                .register(
                    w_fast,
                    WorkerCapabilities::new("fast", 4, 8192, false, false, None),
                    "127.0.0.1:9651".parse().unwrap(),
                    tx_fast,
                    None,
                )
                .await
                .unwrap();
            let s_slow = registry
                .register(
                    w_slow,
                    WorkerCapabilities::new("slow", 1, 4096, false, false, None),
                    "127.0.0.1:9652".parse().unwrap(),
                    tx_slow,
                    None,
                )
                .await
                .unwrap();

            let task = Task::new(
                TaskSpec::command("echo", vec!["2c-disconnect".into()]),
                TaskRequirements::generic(1, 64),
            )
            .with_tags(vec![OPT_REPEATABLE_TAG.to_string()]);
            let tid = queue.submit(task).await.unwrap();
            queue
                .schedule_tasks_batch_in_sessions(&[(tid, w_slow, s_slow)])
                .await;
            queue
                .mark_running_from(w_slow, Some(s_slow), &tid)
                .await
                .unwrap();

            let (sender, session_live) = registry.reserve_dispatch(&w_fast, s_fast).await.unwrap();
            assert!(session_live.load(Ordering::Acquire));

            // Keep `rx_fast` alive (writer task has not exited yet), while disconnect cleanup
            // unregisters w_fast and sweeps its tasks in queue.
            crate::server::handle_worker_disconnect(
                &registry,
                &queue,
                &notify,
                &w_fast,
                Some(s_fast),
                "EOF before writer drop",
                false,
            )
            .await;
            assert!(!session_live.load(Ordering::Acquire));

            let outcome = queue
                .try_start_speculative_and_deliver(tid, w_fast, s_fast, Some(10), |task| {
                    session_live.load(Ordering::Acquire)
                        && sender.try_send(MasterMessage::AssignTask { task }).is_ok()
                })
                .await;
            assert_eq!(outcome, SpeculativeStartOutcome::DeliveryFailed);
            registry.release_dispatch(&w_fast, s_fast).await;

            let fast_info = registry.get_worker(w_fast).await.unwrap();
            assert_eq!(fast_info.status, WorkerStatus::Disconnected);
            assert_eq!(fast_info.active_tasks, 0);
            assert!(rx_fast.try_recv().is_err());

            // Primary failure must NOT wedge the task in Scheduled on Disconnected w_fast.
            let fail_outcome = queue
                .record_result_from(
                    w_slow,
                    Some(s_slow),
                    TaskResult::failure(w_slow, tid, 1, "", "primary boom", 50, None),
                )
                .await
                .unwrap();
            assert_ne!(fail_outcome.state, TaskState::Scheduled);
            let stored = queue.get_task(&tid).await.unwrap();
            assert_ne!(stored.state, TaskState::Scheduled);
            assert_ne!(stored.assigned_worker_id, Some(w_fast));
        }

        // Case 2: Target worker is superseded by a new session (register_session_with_member_store)
        // AFTER reserve_dispatch and BEFORE try_start_speculative_and_deliver while old rx1 is open.
        {
            let registry = WorkerRegistry::new();
            let queue = TaskQueue::new();
            let w_fast = Uuid::new_v4();
            let w_slow = Uuid::new_v4();
            let (tx1, mut rx1) = mpsc::channel(8);
            let (tx2, _rx2) = mpsc::channel(8);
            let (tx_slow, _rx_slow) = mpsc::channel(8);
            let reg1 = registry
                .register_session_with_member_store(
                    w_fast,
                    WorkerCapabilities::new("fast", 4, 8192, false, false, None),
                    ProtocolCapabilities::empty(),
                    "127.0.0.1:9653".parse().unwrap(),
                    tx1,
                    None,
                    None,
                    false,
                    None,
                )
                .await
                .unwrap();
            let s_slow = registry
                .register(
                    w_slow,
                    WorkerCapabilities::new("slow", 1, 4096, false, false, None),
                    "127.0.0.1:9654".parse().unwrap(),
                    tx_slow,
                    None,
                )
                .await
                .unwrap();

            let task = Task::new(
                TaskSpec::command("echo", vec!["2c-supersede".into()]),
                TaskRequirements::generic(1, 64),
            )
            .with_tags(vec![OPT_REPEATABLE_TAG.to_string()]);
            let tid = queue.submit(task).await.unwrap();
            queue
                .schedule_tasks_batch_in_sessions(&[(tid, w_slow, s_slow)])
                .await;
            queue
                .mark_running_from(w_slow, Some(s_slow), &tid)
                .await
                .unwrap();

            let (sender1, live1) = registry
                .reserve_dispatch(&w_fast, reg1.session_id)
                .await
                .unwrap();
            assert!(live1.load(Ordering::Acquire));

            // Re-registration supersedes session 1 and invalidates live1 under registry write lock.
            let reg2 = registry
                .register_session_with_member_store(
                    w_fast,
                    WorkerCapabilities::new("fast", 4, 8192, false, false, None),
                    ProtocolCapabilities::empty(),
                    "127.0.0.1:9655".parse().unwrap(),
                    tx2,
                    None,
                    None,
                    false,
                    None,
                )
                .await
                .unwrap();
            assert_eq!(reg2.superseded_session, Some(reg1.session_id));
            assert!(!live1.load(Ordering::Acquire));
            queue
                .handle_session_disconnected(&w_fast, Some(reg1.session_id), "superseded", true)
                .await;

            let outcome = queue
                .try_start_speculative_and_deliver(tid, w_fast, reg1.session_id, Some(10), |task| {
                    live1.load(Ordering::Acquire)
                        && sender1.try_send(MasterMessage::AssignTask { task }).is_ok()
                })
                .await;
            assert_eq!(outcome, SpeculativeStartOutcome::DeliveryFailed);
            registry.release_dispatch(&w_fast, reg1.session_id).await;

            assert_eq!(registry.get_worker(w_fast).await.unwrap().active_tasks, 0);
            assert!(rx1.try_recv().is_err());

            let fail_outcome = queue
                .record_result_from(
                    w_slow,
                    Some(s_slow),
                    TaskResult::failure(w_slow, tid, 1, "", "primary boom", 50, None),
                )
                .await
                .unwrap();
            assert_ne!(fail_outcome.state, TaskState::Scheduled);
            assert_ne!(
                queue.get_task(&tid).await.unwrap().assigned_worker_id,
                Some(w_fast)
            );
        }

        // Case 3: Reaper timeout clears session_live; subsequent HeartbeatOutcome::Revived
        // keeps pre-reap handle false while issuing a fresh live flag for post-revive dispatches.
        {
            let registry = WorkerRegistry::new();
            let w_id = Uuid::new_v4();
            let (tx, _rx) = mpsc::channel(8);
            let session = registry
                .register(
                    w_id,
                    WorkerCapabilities::new("reap-revive", 4, 8192, false, false, None),
                    "127.0.0.1:9656".parse().unwrap(),
                    tx,
                    None,
                )
                .await
                .unwrap();
            let (_sender_pre, live_pre) = registry.reserve_dispatch(&w_id, session).await.unwrap();
            assert!(live_pre.load(Ordering::Acquire));

            let hb_at = registry.last_heartbeat_instant(&w_id).await.unwrap();
            let reaped = registry
                .reap_stale_sessions_at(hb_at + Duration::from_secs(30), Duration::from_secs(5))
                .await;
            assert_eq!(reaped, vec![(w_id, session)]);
            assert!(!live_pre.load(Ordering::Acquire));

            let revived = registry
                .record_heartbeat_session(&w_id, 0, 2000, 10.0, 4096, Some(session))
                .await
                .unwrap();
            assert_eq!(revived, HeartbeatOutcome::Revived);
            assert!(!live_pre.load(Ordering::Acquire));

            let (_sender_post, live_post) =
                registry.reserve_dispatch(&w_id, session).await.unwrap();
            assert!(live_post.load(Ordering::Acquire));
        }
    }
}
