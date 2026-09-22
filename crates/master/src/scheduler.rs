//! Workload-aware task scheduling and routing engine for `rusty_grid_master`.
//!
//! Enforces:
//! 1. Strict GPU capability gating (dual-gated: `gpu_required || requires_gpu()`).
//!    Non-GPU workers NEVER receive GPU tasks under any circumstances.
//! 2. Parallel batch spread scheduling using in-pass provisional load tracking
//!    to balance independent sub-tasks evenly across all eligible workers.
//! 3. Mobile and environmental constraints (thermal throttling, low battery).
//! 4. Autonomous event-driven scheduling loop with fallback periodic ticks.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Notify};
use tracing::{debug, info, warn};
use uuid::Uuid;

use rusty_grid_core::error::GridResult;
use rusty_grid_core::protocol::MasterMessage;
use rusty_grid_core::task::{Task, TaskId, TaskSpec};

use crate::queue::TaskQueue;
use crate::registry::{WorkerInfo, WorkerRegistry, WorkerStatus};

/// Scheduling policies for load balancing across workers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SchedulingPolicy {
    /// Picks worker with minimum raw (active_tasks + provisional_tasks).
    LeastLoaded,
    /// Picks worker with minimum (effective_tasks / cpu_cores).
    #[default]
    WeightedLeastLoaded,
    /// Distributes tasks in round-robin sequence across available workers.
    RoundRobin,
}

/// Configuration options for the `WorkloadScheduler`.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Load balancing algorithm.
    pub policy: SchedulingPolicy,
    /// Multiplier against worker CPU cores to calculate maximum concurrent tasks (default: 1.0).
    pub core_concurrency_multiplier: f64,
    /// Hard cap on maximum tasks per worker, regardless of core count (None = unbounded by cap).
    pub max_tasks_per_worker: Option<usize>,
    /// If true, generic CPU tasks will prioritize CPU-only workers to preserve GPU nodes (default: true).
    pub preserve_gpu_for_gpu_tasks: bool,
    /// If true, fails tasks immediately if cluster has no capable workers registered (default: false).
    pub fail_unfulfillable_tasks: bool,
    /// Minimum battery percentage on mobile devices required to execute compilation/GPU tasks (default: 15).
    pub mobile_min_battery_pct: u8,
    /// Maximum host CPU usage percentage before anti-stuttering backpressure rejects assignments (default: 85.0).
    pub max_host_cpu_pct: f32,
    /// Fallback periodic tick interval for the scheduling loop (default: 100ms).
    pub tick_interval: Duration,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            policy: SchedulingPolicy::WeightedLeastLoaded,
            core_concurrency_multiplier: 1.0,
            max_tasks_per_worker: None,
            preserve_gpu_for_gpu_tasks: true,
            fail_unfulfillable_tasks: false,
            mobile_min_battery_pct: 15,
            max_host_cpu_pct: 85.0,
            tick_interval: Duration::from_millis(100),
        }
    }
}

impl SchedulerConfig {
    /// Computes the maximum concurrent tasks allowed for a given worker.
    pub fn effective_max_concurrency(&self, worker: &WorkerInfo) -> usize {
        let cores = worker.capabilities.cpu_cores.max(1);
        let by_multiplier = (cores as f64 * self.core_concurrency_multiplier).ceil() as usize;
        let limit = by_multiplier.max(1);
        match self.max_tasks_per_worker {
            Some(cap) => limit.min(cap),
            None => limit,
        }
    }
}

/// Matchmaking decision pairing a task with an eligible worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleAssignment {
    pub task_id: TaskId,
    pub worker_id: Uuid,
    pub task: Task,
}

/// Explanatory reason why a task could not be scheduled during a pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleSkipReason {
    NoEligibleWorkers,
    NoGpuWorkersAvailable,
    AllEligibleWorkersSaturated,
    MobileThrottledOrLowBattery,
    ClusterLacksCapability(String),
}

/// Comprehensive outcome report produced by a scheduling pass.
#[derive(Debug, Clone, Default)]
pub struct ScheduleReport {
    pub assignments: Vec<ScheduleAssignment>,
    pub skipped: Vec<(TaskId, ScheduleSkipReason)>,
    pub unfulfillable: Vec<(TaskId, String)>,
}

/// Autonomous workload-specific scheduling engine.
pub struct WorkloadScheduler {
    config: SchedulerConfig,
    registry: WorkerRegistry,
    queue: TaskQueue,
    trigger: Arc<Notify>,
}

impl WorkloadScheduler {
    /// Creates a new WorkloadScheduler instance.
    pub fn new(
        config: SchedulerConfig,
        registry: WorkerRegistry,
        queue: TaskQueue,
        trigger: Arc<Notify>,
    ) -> Self {
        Self {
            config,
            registry,
            queue,
            trigger,
        }
    }

    /// Returns a reference to the scheduler configuration.
    pub fn config(&self) -> &SchedulerConfig {
        &self.config
    }

    /// Pure matchmaking function: matches a list of queued tasks against a snapshot of worker metadata.
    ///
    /// This function is entirely synchronous, pure, and free of I/O or global state,
    /// making it 100% deterministic and testable.
    pub fn matchmake(
        config: &SchedulerConfig,
        tasks: &[Task],
        workers: &[WorkerInfo],
    ) -> ScheduleReport {
        let mut report = ScheduleReport::default();
        if tasks.is_empty() || workers.is_empty() {
            for task in tasks {
                report
                    .skipped
                    .push((task.id, ScheduleSkipReason::NoEligibleWorkers));
            }
            return report;
        }

        // Ephemeral in-pass provisional load counter for spread scheduling
        let mut provisional_loads: HashMap<Uuid, usize> = HashMap::new();
        let mut round_robin_cursor: usize = 0;

        for task in tasks {
            let requires_gpu = task.requirements.gpu_required || task.spec.requires_gpu();

            // Check if cluster possesses any GPU worker at all
            if requires_gpu && config.fail_unfulfillable_tasks {
                let any_gpu = workers.iter().any(|w| w.capabilities.can_execute_gpu());
                if !any_gpu {
                    report.unfulfillable.push((
                        task.id,
                        "Cluster lacks workers with GPU capabilities".to_string(),
                    ));
                    continue;
                }
            }

            // Filter eligible candidates
            let mut eligible_candidates: Vec<(&WorkerInfo, usize)> = Vec::new();

            for worker in workers {
                // Stage 1: Connectivity
                if worker.status == WorkerStatus::Disconnected {
                    continue;
                }

                // Stage 2: Hardware requirements (CPU & RAM) & Anti-stuttering backpressure
                if task.requirements.cpu_cores > 0
                    && worker.capabilities.cpu_cores < task.requirements.cpu_cores
                {
                    continue;
                }
                if task.requirements.ram_mb > 0
                    && worker.capabilities.ram_mb < task.requirements.ram_mb
                {
                    continue;
                }
                // Anti-stuttering: if host CPU exceeds threshold, back off and skip this worker
                if worker.cpu_usage_pct > config.max_host_cpu_pct {
                    continue;
                }
                // Free RAM check: if task specifies RAM and reported available RAM is strictly less
                if task.requirements.ram_mb > 0
                    && worker.ram_available_mb > 0
                    && worker.ram_available_mb < task.requirements.ram_mb
                {
                    continue;
                }

                // Stage 3: Strict GPU Gating (Requirement R3 & AC4)
                // Dual-gated: if task demands GPU, worker MUST advertise has_gpu or is_simulated_gpu
                // Non-GPU workers NEVER receive GPU tasks under any circumstances!
                if requires_gpu && !worker.capabilities.can_execute_gpu() {
                    continue;
                }

                // Stage 4: Mobile & Environmental Constraints via capability tags and mobile telemetry
                let is_thermal_throttled = worker
                    .capabilities
                    .tags
                    .iter()
                    .any(|t| t == "thermal_throttled")
                    || worker
                        .capabilities
                        .mobile
                        .as_ref()
                        .is_some_and(|m| m.thermal_throttled);
                let is_low_battery = worker.capabilities.tags.iter().any(|t| t == "low_battery")
                    || worker.capabilities.mobile.as_ref().is_some_and(|m| {
                        m.battery_pct
                            .is_some_and(|pct| pct < config.mobile_min_battery_pct)
                            && m.is_charging == Some(false)
                    });

                if is_thermal_throttled
                    && (task.requirements.cpu_cores > 1
                        || matches!(
                            task.spec,
                            TaskSpec::RustCompilation { .. } | TaskSpec::GpuCompute { .. }
                        ))
                {
                    continue;
                }
                if is_low_battery
                    && matches!(
                        task.spec,
                        TaskSpec::RustCompilation { .. } | TaskSpec::GpuCompute { .. }
                    )
                {
                    continue;
                }

                // Stage 5: Concurrency Saturation
                let current_provisional = *provisional_loads.get(&worker.worker_id).unwrap_or(&0);
                let total_active = worker.active_tasks + current_provisional;
                let max_concurrency = config.effective_max_concurrency(worker);

                if total_active >= max_concurrency {
                    continue; // Worker is saturated
                }

                eligible_candidates.push((worker, total_active));
            }

            if eligible_candidates.is_empty() {
                let skip_reason = if requires_gpu {
                    ScheduleSkipReason::NoGpuWorkersAvailable
                } else {
                    ScheduleSkipReason::AllEligibleWorkersSaturated
                };
                report.skipped.push((task.id, skip_reason));
                continue;
            }

            // Stage 6: Ranking & Selection according to policy
            let selected_worker = match config.policy {
                SchedulingPolicy::RoundRobin => {
                    let idx = round_robin_cursor % eligible_candidates.len();
                    round_robin_cursor += 1;
                    eligible_candidates[idx].0
                }
                SchedulingPolicy::LeastLoaded => {
                    eligible_candidates.sort_by(|(w_a, load_a), (w_b, load_b)| {
                        load_a
                            .cmp(load_b)
                            .then_with(|| {
                                // GPU preservation tie-break: generic tasks prefer CPU-only workers
                                if config.preserve_gpu_for_gpu_tasks && !requires_gpu {
                                    let a_gpu = w_a.capabilities.can_execute_gpu();
                                    let b_gpu = w_b.capabilities.can_execute_gpu();
                                    a_gpu.cmp(&b_gpu)
                                } else {
                                    std::cmp::Ordering::Equal
                                }
                            })
                            .then_with(|| {
                                w_b.capabilities.cpu_cores.cmp(&w_a.capabilities.cpu_cores)
                            })
                            .then_with(|| w_a.worker_id.cmp(&w_b.worker_id))
                    });
                    eligible_candidates[0].0
                }
                SchedulingPolicy::WeightedLeastLoaded => {
                    eligible_candidates.sort_by(|(w_a, load_a), (w_b, load_b)| {
                        let ratio_a = (*load_a as f64 / w_a.capabilities.cpu_cores.max(1) as f64)
                            + 0.5 * (w_a.cpu_usage_pct.max(0.0) as f64 / 100.0);
                        let ratio_b = (*load_b as f64 / w_b.capabilities.cpu_cores.max(1) as f64)
                            + 0.5 * (w_b.cpu_usage_pct.max(0.0) as f64 / 100.0);
                        ratio_a
                            .partial_cmp(&ratio_b)
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then_with(|| {
                                if config.preserve_gpu_for_gpu_tasks && !requires_gpu {
                                    let a_gpu = w_a.capabilities.can_execute_gpu();
                                    let b_gpu = w_b.capabilities.can_execute_gpu();
                                    a_gpu.cmp(&b_gpu)
                                } else {
                                    std::cmp::Ordering::Equal
                                }
                            })
                            .then_with(|| {
                                w_b.capabilities.cpu_cores.cmp(&w_a.capabilities.cpu_cores)
                            })
                            .then_with(|| w_a.worker_id.cmp(&w_b.worker_id))
                    });
                    eligible_candidates[0].0
                }
            };

            // In-pass provisional load accounting: update load for next iteration in batch
            *provisional_loads
                .entry(selected_worker.worker_id)
                .or_insert(0) += 1;
            report.assignments.push(ScheduleAssignment {
                task_id: task.id,
                worker_id: selected_worker.worker_id,
                task: task.clone(),
            });
        }

        report
    }

    /// Performs a single scheduling pass:
    /// 1. Processes expired delayed retries in the queue.
    /// 2. Fetches ready tasks and active workers.
    /// 3. Executes `matchmake`.
    /// 4. Atomically transitions assigned tasks to `Scheduled` and dispatches `AssignTask`.
    pub async fn schedule_once(&self) -> GridResult<ScheduleReport> {
        // Step 1: Advance any delayed retries whose backoff has passed
        let advanced_retries = self.queue.process_delayed_retries().await;
        if advanced_retries > 0 {
            debug!(
                count = advanced_retries,
                "Re-enqueued backoff-elapsed retries"
            );
        }

        // Step 2: Fetch schedulable tasks and active workers
        let tasks = self.queue.get_schedulable_tasks().await;
        if tasks.is_empty() {
            return Ok(ScheduleReport::default());
        }

        let workers = self.registry.list_active_workers().await;
        if workers.is_empty() {
            let mut report = ScheduleReport::default();
            for task in tasks {
                report
                    .skipped
                    .push((task.id, ScheduleSkipReason::NoEligibleWorkers));
            }
            return Ok(report);
        }

        // Step 3: Run pure matchmaking algorithm
        let report = Self::matchmake(&self.config, &tasks, &workers);

        // Step 4: Dispatch assignments
        for assignment in &report.assignments {
            // Atomically mark task Scheduled in queue
            if let Some(task) = self
                .queue
                .schedule_task(&assignment.task_id, assignment.worker_id)
                .await
            {
                // Increment active tasks in registry
                let _ = self
                    .registry
                    .increment_active_tasks(&assignment.worker_id)
                    .await;

                // Dispatch AssignTask frame to worker outbound channel
                let msg = MasterMessage::AssignTask { task: task.clone() };
                if let Err(e) = self
                    .registry
                    .send_to_worker(&assignment.worker_id, msg)
                    .await
                {
                    warn!(
                        task_id = %assignment.task_id,
                        worker_id = %assignment.worker_id,
                        error = %e,
                        "Failed to dispatch AssignTask to worker; rolling back assignment"
                    );
                    let _ = self
                        .registry
                        .decrement_active_tasks(&assignment.worker_id)
                        .await;
                    let _ = self
                        .queue
                        .handle_worker_disconnected(
                            &assignment.worker_id,
                            "Failed to deliver AssignTask message",
                            false,
                        )
                        .await;
                    let _ = self.registry.unregister(&assignment.worker_id, None).await;
                } else {
                    debug!(
                        task_id = %assignment.task_id,
                        worker_id = %assignment.worker_id,
                        "Successfully dispatched AssignTask to worker"
                    );
                }
            }
        }

        Ok(report)
    }

    /// Spawns the autonomous event-driven scheduling loop.
    pub fn spawn(
        self: Arc<Self>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            info!(
                policy = ?self.config.policy,
                tick_interval_ms = self.config.tick_interval.as_millis(),
                "Starting WorkloadScheduler event-driven background loop"
            );

            let mut ticker = tokio::time::interval(self.config.tick_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = self.trigger.notified() => {
                        debug!("Scheduler loop woken by event notification");
                        let _ = self.schedule_once().await;
                    }
                    _ = ticker.tick() => {
                        let _ = self.schedule_once().await;
                    }
                    res = shutdown_rx.changed() => {
                        if res.is_ok() && *shutdown_rx.borrow() {
                            info!("WorkloadScheduler received shutdown signal; terminating");
                            break;
                        } else if res.is_err() {
                            break;
                        }
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::capabilities::WorkerCapabilities;
    use rusty_grid_core::task::{TaskRequirements, TaskSpec};

    fn make_worker(
        name: &str,
        cores: usize,
        ram_mb: u64,
        gpu: bool,
        simulated_gpu: bool,
    ) -> WorkerInfo {
        let caps = WorkerCapabilities::new(name, cores, ram_mb, gpu, simulated_gpu, None);
        WorkerInfo {
            worker_id: Uuid::new_v4(),
            session_id: 1,
            capabilities: caps,
            status: WorkerStatus::Connected,
            last_heartbeat_timestamp: 1000,
            registered_timestamp: 1000,
            active_tasks: 0,
            remote_addr: "127.0.0.1:9000".into(),
            cpu_usage_pct: 0.0,
            ram_available_mb: ram_mb,
        }
    }

    #[test]
    fn test_strict_gpu_routing_assigned_only_to_gpu_worker() {
        let config = SchedulerConfig::default();

        let w1 = make_worker("worker-cpu-1", 4, 8192, false, false);
        let w2 = make_worker("worker-cpu-2", 4, 8192, false, false);
        let w3 = make_worker("worker-gpu-3", 8, 16384, true, true);
        let workers = vec![w1.clone(), w2.clone(), w3.clone()];

        let gpu_task = Task::new(
            TaskSpec::gpu_compute("gemm_kernel", 64),
            TaskRequirements::gpu(30),
        );

        let report =
            WorkloadScheduler::matchmake(&config, std::slice::from_ref(&gpu_task), &workers);

        assert_eq!(report.assignments.len(), 1);
        assert_eq!(report.assignments[0].task_id, gpu_task.id);
        assert_eq!(report.assignments[0].worker_id, w3.worker_id);
        assert_ne!(report.assignments[0].worker_id, w1.worker_id);
        assert_ne!(report.assignments[0].worker_id, w2.worker_id);
    }

    #[test]
    fn test_strict_gpu_routing_non_gpu_workers_never_receive_gpu_task() {
        let config = SchedulerConfig::default();

        let w1 = make_worker("worker-cpu-1", 4, 8192, false, false);
        let w2 = make_worker("worker-cpu-2", 4, 8192, false, false);
        let workers = vec![w1, w2];

        let gpu_task = Task::new(
            TaskSpec::gpu_compute("gemm_kernel", 64),
            TaskRequirements::gpu(30),
        );

        let report =
            WorkloadScheduler::matchmake(&config, std::slice::from_ref(&gpu_task), &workers);

        assert_eq!(report.assignments.len(), 0);
        assert_eq!(report.skipped.len(), 1);
        assert_eq!(report.skipped[0].0, gpu_task.id);
        assert_eq!(
            report.skipped[0].1,
            ScheduleSkipReason::NoGpuWorkersAvailable
        );
    }

    #[test]
    fn test_strict_gpu_routing_saturated_gpu_worker_does_not_spill_to_idle_cpu() {
        let config = SchedulerConfig {
            core_concurrency_multiplier: 1.0,
            ..Default::default()
        };

        let w1_cpu = make_worker("worker-cpu-1", 4, 8192, false, false);
        let mut w2_gpu = make_worker("worker-gpu-2", 2, 4096, true, true);
        w2_gpu.active_tasks = 2; // Saturated: 2 cores, 2 active tasks

        let workers = vec![w1_cpu, w2_gpu];

        let gpu_task = Task::new(
            TaskSpec::gpu_compute("gemm_kernel", 64),
            TaskRequirements::gpu(30),
        );

        let report =
            WorkloadScheduler::matchmake(&config, std::slice::from_ref(&gpu_task), &workers);

        // Must NOT assign to idle w1_cpu!
        assert_eq!(report.assignments.len(), 0);
        assert_eq!(report.skipped.len(), 1);
        assert_eq!(
            report.skipped[0].1,
            ScheduleSkipReason::NoGpuWorkersAvailable
        );
    }

    #[test]
    fn test_parallel_batch_spread_scheduling_5_tasks_across_3_workers() {
        let config = SchedulerConfig {
            policy: SchedulingPolicy::WeightedLeastLoaded,
            ..Default::default()
        };

        let w1 = make_worker("worker-1", 4, 8192, false, false);
        let w2 = make_worker("worker-2", 4, 8192, false, false);
        let w3 = make_worker("worker-3", 4, 8192, true, true);
        let workers = vec![w1.clone(), w2.clone(), w3.clone()];

        let tasks: Vec<Task> = (0..5)
            .map(|i| {
                Task::new(
                    TaskSpec::command("echo", vec![format!("task_{i}")]),
                    TaskRequirements::generic(1, 10),
                )
            })
            .collect();

        let report = WorkloadScheduler::matchmake(&config, &tasks, &workers);

        assert_eq!(report.assignments.len(), 5);

        let mut counts: HashMap<Uuid, usize> = HashMap::new();
        for a in &report.assignments {
            *counts.entry(a.worker_id).or_insert(0) += 1;
        }

        // All 3 workers must receive at least 1 task
        assert_eq!(counts.len(), 3, "Tasks must be spread across all 3 workers");
        assert!(counts.contains_key(&w1.worker_id));
        assert!(counts.contains_key(&w2.worker_id));
        assert!(counts.contains_key(&w3.worker_id));

        // Distribution of 5 tasks across 3 equal workers must be {2, 2, 1}
        let mut count_values: Vec<usize> = counts.values().copied().collect();
        count_values.sort_unstable();
        assert_eq!(count_values, vec![1, 2, 2]);
    }

    #[test]
    fn test_gpu_worker_preservation_for_generic_workload() {
        let config = SchedulerConfig {
            policy: SchedulingPolicy::LeastLoaded,
            preserve_gpu_for_gpu_tasks: true,
            ..Default::default()
        };

        let w_cpu = make_worker("worker-cpu", 4, 8192, false, false);
        let w_gpu = make_worker("worker-gpu", 4, 8192, true, true);
        let workers = vec![w_cpu.clone(), w_gpu.clone()];

        let generic_task = Task::new(
            TaskSpec::command("echo", vec!["hello".into()]),
            TaskRequirements::generic(1, 10),
        );

        let report = WorkloadScheduler::matchmake(&config, &[generic_task], &workers);

        assert_eq!(report.assignments.len(), 1);
        assert_eq!(
            report.assignments[0].worker_id, w_cpu.worker_id,
            "Generic task should prefer CPU worker to preserve GPU node"
        );
    }

    #[test]
    fn test_mobile_thermal_throttling_constraint() {
        let config = SchedulerConfig::default();

        let mut mobile_worker = make_worker("android-node", 4, 4096, false, false);
        mobile_worker
            .capabilities
            .tags
            .push("thermal_throttled".into());

        let desktop_worker = make_worker("desktop-node", 8, 16384, false, false);
        let workers = vec![mobile_worker.clone(), desktop_worker.clone()];

        // Multi-core task (requires 2 cores)
        let multi_core_task = Task::new(
            TaskSpec::command("cargo", vec!["build".into()]),
            TaskRequirements::generic(2, 60),
        );

        let report = WorkloadScheduler::matchmake(&config, &[multi_core_task], &workers);

        assert_eq!(report.assignments.len(), 1);
        assert_eq!(
            report.assignments[0].worker_id, desktop_worker.worker_id,
            "Thermal-throttled mobile worker must be bypassed for multi-core tasks"
        );
    }

    #[test]
    fn test_anti_stuttering_cpu_backpressure() {
        let config = SchedulerConfig::default();

        let mut overloaded_worker = make_worker("busy-worker", 4, 8192, false, false);
        overloaded_worker.cpu_usage_pct = 92.0; // Exceeds default 85.0%

        let healthy_worker = make_worker("idle-worker", 4, 8192, false, false);
        let workers = vec![overloaded_worker.clone(), healthy_worker.clone()];

        let task = Task::new(
            TaskSpec::command("echo", vec!["test".into()]),
            TaskRequirements::generic(1, 10),
        );

        let report = WorkloadScheduler::matchmake(&config, &[task], &workers);

        assert_eq!(report.assignments.len(), 1);
        assert_eq!(
            report.assignments[0].worker_id, healthy_worker.worker_id,
            "Overloaded worker (>85% CPU) should be bypassed by anti-stuttering backpressure"
        );
    }

    #[test]
    fn test_composite_load_ranking() {
        let config = SchedulerConfig::default(); // WeightedLeastLoaded

        let mut w1 = make_worker("w1", 4, 8192, false, false);
        w1.cpu_usage_pct = 60.0;

        let mut w2 = make_worker("w2", 4, 8192, false, false);
        w2.cpu_usage_pct = 10.0;

        let workers = vec![w1.clone(), w2.clone()];

        let task = Task::new(
            TaskSpec::command("echo", vec!["test".into()]),
            TaskRequirements::generic(1, 10),
        );

        let report = WorkloadScheduler::matchmake(&config, &[task], &workers);

        assert_eq!(report.assignments.len(), 1);
        assert_eq!(
            report.assignments[0].worker_id, w2.worker_id,
            "Worker with lower composite load (10% CPU vs 60% CPU) should be selected"
        );
    }
}
