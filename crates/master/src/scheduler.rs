//! Workload-aware task scheduling and routing engine for `rusty_grid_master`.
//!
//! Enforces:
//! 1. Strict GPU capability gating (dual-gated: `gpu_required || requires_gpu()`).
//!    Non-GPU workers NEVER receive GPU tasks under any circumstances.
//! 2. Parallel batch spread scheduling using in-pass provisional load tracking
//!    to balance independent sub-tasks evenly across all eligible workers.
//! 3. Mobile and environmental constraints (thermal throttling, low battery).
//! 4. Autonomous event-driven scheduling loop with fallback periodic ticks.
//! 5. Capacity taken from the master's own books (the queue's assignment set, with cores and
//!    declared RAM reserved per task), never from a heartbeat's running-task count.
//! 6. No head-of-line blocking: a pass walks the queue past tasks nobody can run right now and
//!    only clones the tasks it actually assigns.
//! 7. No starvation of wide tasks: a task that only waits for free slots (a 4-core build on a
//!    4-core worker that keeps being topped up with 1-core jobs) holds back the worker closest to
//!    having room for it, so the tasks behind it cannot refill that worker.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{watch, Notify};
use tracing::{debug, info, warn};
use uuid::Uuid;

use rusty_grid_core::capabilities::{
    calculate_effective_concurrency, BatteryTier, MobileLiveStatus, ThermalTier,
};
use rusty_grid_core::error::GridResult;
use rusty_grid_core::placement::Placement;
use rusty_grid_core::protocol::{MasterMessage, ProtocolCapabilities};
use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskSpec};

use crate::queue::{AssignedLoad, QueueOrderKey, TaskQueue, TaskView};
use crate::registry::{WorkerInfo, WorkerRegistry, WorkerStatus};
use crate::server::WaiterMap;

/// Queued tasks examined per page of a scheduling pass (each page is one short read lock).
const SCAN_PAGE: usize = 256;

/// How long an idle probe of the queue for GPU work is trusted (see `GpuDemand`).
const GPU_PROBE_INTERVAL: Duration = Duration::from_secs(1);

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
    /// If true (default), generic CPU tasks stay clear of GPU workers while GPU-capable work needs
    /// them: with a GPU task *waiting*, CPU tasks go to CPU-only workers whenever one can take them
    /// and only spill onto a GPU worker when none can; and while GPU work is waiting or *likely*
    /// (seen within `gpu_demand_window`), a spill-over leaves `gpu_reserved_slots` slots of the GPU
    /// worker free. With no GPU work around, GPU workers are ordinary candidates (CPU-only workers
    /// are merely preferred on a tie), so an idle GPU machine still contributes its cores.
    pub preserve_gpu_for_gpu_tasks: bool,
    /// Slots a GPU worker keeps free for GPU tasks against CPU-only spill-over while GPU-capable
    /// work is waiting or likely (default: 1). Capped at `slots - 1`, so a one-slot GPU worker
    /// stays usable for CPU work.
    pub gpu_reserved_slots: usize,
    /// How long after GPU-capable work was last seen queued or assigned it still counts as
    /// "likely" (default: 30 s).
    pub gpu_demand_window: Duration,
    /// If true, fails tasks immediately if cluster has no capable workers registered (default: false).
    pub fail_unfulfillable_tasks: bool,
    /// Minimum battery percentage on mobile devices required to execute compilation/GPU tasks (default: 15).
    pub mobile_min_battery_pct: u8,
    /// Maximum host CPU usage percentage before anti-stuttering backpressure rejects assignments (default: 85.0).
    pub max_host_cpu_pct: f32,
    /// Minimum VRAM in MB required for "fast" quality 3D neural generation (default: 2048 MB).
    pub min_vram_fast_mb: u64,
    /// Minimum VRAM in MB required for "standard" quality 3D neural generation (default: 4096 MB).
    pub min_vram_standard_mb: u64,
    /// Minimum VRAM in MB required for "high" quality 3D neural generation (default: 6144 MB).
    pub min_vram_high_mb: u64,
    /// Fallback periodic tick interval for the scheduling loop (default: 100ms).
    pub tick_interval: Duration,
    /// Micro-batch accumulation debounce window during burst submissions (default: 5ms).
    pub micro_batch_window: Duration,
    /// Maximum number of tasks to assign in a single pass (default: 128).
    pub max_batch_size: usize,
    /// Maximum number of queued tasks one pass examines while looking for placeable ones
    /// (default: 20,000; at least 1). Tasks no worker can take right now are skipped, not counted against
    /// `max_batch_size`, so a backlog of them does not starve the tasks behind it.
    pub max_scan_per_pass: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            policy: SchedulingPolicy::WeightedLeastLoaded,
            core_concurrency_multiplier: 1.0,
            max_tasks_per_worker: None,
            preserve_gpu_for_gpu_tasks: true,
            gpu_reserved_slots: 1,
            gpu_demand_window: Duration::from_secs(30),
            fail_unfulfillable_tasks: false,
            mobile_min_battery_pct: 15,
            max_host_cpu_pct: 85.0,
            min_vram_fast_mb: 2048,
            min_vram_standard_mb: 4096,
            min_vram_high_mb: 6144,
            tick_interval: Duration::from_millis(100),
            micro_batch_window: Duration::from_millis(5),
            max_batch_size: 128,
            max_scan_per_pass: 20_000,
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

    /// Determines the required VRAM in MB for a given task.
    pub fn required_vram_mb(&self, task: &Task) -> u64 {
        match &task.spec {
            TaskSpec::Generative3D(spec) => {
                match spec.engine_preference.as_deref() {
                    Some("sdf") | Some("procedural") => 0,
                    _ => match spec.quality.as_str() {
                        "fast" => self.min_vram_fast_mb,
                        "high" => self.min_vram_high_mb,
                        _ => self.min_vram_standard_mb,
                    },
                }
            }
            _ => 0,
        }
    }

    /// Sets the debounce micro-batch window.
    pub fn with_micro_batch_window(mut self, window: Duration) -> Self {
        self.micro_batch_window = window;
        self
    }

    /// Sets the maximum batch size for scheduling.
    pub fn with_max_batch_size(mut self, size: usize) -> Self {
        self.max_batch_size = size;
        self
    }
}

/// Matchmaking decision pairing a task with an eligible worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleAssignment {
    pub task_id: TaskId,
    pub worker_id: Uuid,
    /// Connection session of `worker_id` this assignment was made against. The task is only ever
    /// delivered to, and accepted back from, that session.
    pub session_id: u64,
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

/// Everything about a task that decides whether a worker may take it. Two queued tasks with the
/// same shape are interchangeable to the placement engine, so once one finds no worker the rest
/// of the pass skips the others without looking at a single worker again.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Shape {
    requires_gpu: bool,
    heavy: bool,
    requires_file_staging: bool,
    cpu_cores: usize,
    ram_mb: u64,
    placement: Placement,
}

impl Shape {
    fn of(view: &TaskView, placement: &Placement) -> Self {
        Self {
            requires_gpu: view.requires_gpu,
            heavy: view.heavy,
            requires_file_staging: view.requires_file_staging,
            cpu_cores: view.requirements.cpu_cores,
            ram_mb: view.requirements.ram_mb,
            placement: placement.clone(),
        }
    }
}

/// True if `worker` could run a task with `requirements` when idle (static capability check).
fn statically_fits(requirements: &TaskRequirements, worker: &WorkerInfo) -> bool {
    (requirements.cpu_cores == 0 || worker.capabilities.cpu_cores >= requirements.cpu_cores)
        && (requirements.ram_mb == 0 || worker.capabilities.ram_mb >= requirements.ram_mb)
}

/// True if `worker` satisfies a task's placement constraints (`require:` tags: os, arch, worker,
/// tag; see `rusty_grid_core::placement`).
fn placement_admits(placement: &Placement, worker: &WorkerInfo) -> bool {
    placement.matches(
        worker.worker_id,
        &worker.capabilities.name,
        &worker.capabilities.tags,
    )
}

/// [`statically_fits`] and [`placement_admits`] for a queued task's view. A view whose constraints
/// cannot be read fits nowhere.
fn view_fits(view: &TaskView, worker: &WorkerInfo) -> bool {
    statically_fits(&view.requirements, worker)
        && view
            .placement
            .as_ref()
            .is_ok_and(|placement| placement_admits(placement, worker))
}

/// One worker as the placement engine tracks it during a pass.
struct Seat<'a> {
    worker: &'a WorkerInfo,
    /// Concurrency slots of the worker (`SchedulerConfig::effective_max_concurrency`).
    max_slots: usize,
    /// Load from the master's books at the start of the pass.
    base: AssignedLoad,
    /// Load added by tasks placed during this pass.
    placed: AssignedLoad,
}

impl Seat<'_> {
    fn used_slots(&self) -> usize {
        self.base.slots + self.placed.slots
    }

    fn load(&self) -> usize {
        self.base.tasks + self.placed.tasks
    }
}

/// What the engine decided for one task.
enum Verdict {
    /// Placed on the worker at this index of the engine's workers.
    Placed(usize),
    Skip(ScheduleSkipReason),
    Unfulfillable(String),
}

/// The pure placement engine: feeds on task views in queue order, tracks the load it has added,
/// and picks a worker for each. Free of I/O and global state, so it is deterministic and testable.
struct Engine<'a> {
    config: &'a SchedulerConfig,
    seats: Vec<Seat<'a>>,
    rr_cursor: usize,
    /// A GPU task that some GPU worker could run is waiting (see
    /// `SchedulerConfig::preserve_gpu_for_gpu_tasks`).
    gpu_waiting: bool,
    /// GPU work was seen within `gpu_demand_window`.
    gpu_recent: bool,
    any_gpu_worker: bool,
    dead: HashSet<Shape>,
    /// Slots still free across the cluster; the pass stops looking once this reaches zero.
    free_slots: usize,
    /// Seats held back for a task ahead in the queue that is waiting for slots to free up on them
    /// (see `hold_back`): nothing placed later in the pass may land there.
    reserved: Vec<bool>,
}

impl<'a> Engine<'a> {
    /// `loads` is the master's own accounting per worker. `None` takes each worker's
    /// `active_tasks` as its load instead (the pure `matchmake` API, which has no queue).
    fn new(
        config: &'a SchedulerConfig,
        workers: &'a [WorkerInfo],
        loads: Option<&HashMap<Uuid, AssignedLoad>>,
        gpu_waiting: bool,
        gpu_recent: bool,
    ) -> Self {
        let seats: Vec<Seat<'a>> = workers
            .iter()
            .map(|worker| {
                let base = match loads {
                    Some(loads) => loads.get(&worker.worker_id).copied().unwrap_or_default(),
                    None => AssignedLoad {
                        tasks: worker.active_tasks,
                        slots: worker.active_tasks,
                        unstarted_ram_mb: 0,
                    },
                };
                let mut max_slots = config.effective_max_concurrency(worker);
                if let Some(ref mobile) = worker.capabilities.mobile {
                    let live = mobile.live_status();
                    let bat = live.battery_tier.unwrap_or_else(|| {
                        MobileLiveStatus::compute_battery_tier(live.battery_pct, live.is_charging)
                    });
                    let therm = live.thermal_tier.unwrap_or_else(|| {
                        MobileLiveStatus::compute_thermal_tier(
                            live.battery_temperature,
                            live.thermal_throttled,
                        )
                    });
                    let eff =
                        calculate_effective_concurrency(worker.capabilities.cpu_cores, bat, therm);
                    max_slots = max_slots.min(eff);
                }
                Seat {
                    worker,
                    max_slots,
                    base,
                    placed: AssignedLoad::default(),
                }
            })
            .collect();
        let free_slots = seats
            .iter()
            .filter(|seat| seat.worker.status != WorkerStatus::Disconnected)
            .map(|seat| seat.max_slots.saturating_sub(seat.base.slots))
            .sum();
        Self {
            config,
            any_gpu_worker: workers.iter().any(|w| w.capabilities.can_execute_gpu()),
            reserved: vec![false; seats.len()],
            seats,
            rr_cursor: 0,
            gpu_waiting,
            gpu_recent: gpu_recent || gpu_waiting,
            dead: HashSet::new(),
            free_slots,
        }
    }

    fn place(&mut self, view: &TaskView) -> Verdict {
        let placement = match &view.placement {
            Ok(placement) => placement,
            Err(e) => return Verdict::Unfulfillable(format!("Invalid placement constraints: {e}")),
        };
        if view.requires_gpu && self.config.fail_unfulfillable_tasks && !self.any_gpu_worker {
            return Verdict::Unfulfillable("Cluster lacks workers with GPU capabilities".into());
        }
        if view.requires_file_staging
            && self.config.fail_unfulfillable_tasks
            && !self.seats.iter().any(|seat| {
                seat.worker
                    .features
                    .contains(ProtocolCapabilities::FILE_STAGING)
            })
        {
            return Verdict::Unfulfillable(
                "Cluster lacks workers with FILE_STAGING capability".into(),
            );
        }
        let nobody_matches = !placement.is_empty()
            && !self
                .seats
                .iter()
                .any(|seat| placement_admits(placement, seat.worker));
        if nobody_matches && self.config.fail_unfulfillable_tasks {
            return Verdict::Unfulfillable(format!(
                "No registered worker matches the placement constraints ({placement})"
            ));
        }
        let skip_reason = if view.requires_gpu {
            ScheduleSkipReason::NoGpuWorkersAvailable
        } else if view.requires_file_staging
            && !self.seats.iter().any(|seat| {
                seat.worker
                    .features
                    .contains(ProtocolCapabilities::FILE_STAGING)
            })
        {
            ScheduleSkipReason::ClusterLacksCapability(
                "cluster lacks workers with FILE_STAGING capability".into(),
            )
        } else if nobody_matches {
            ScheduleSkipReason::ClusterLacksCapability(format!(
                "no worker matches the placement constraints ({placement})"
            ))
        } else {
            ScheduleSkipReason::AllEligibleWorkersSaturated
        };

        let shape = Shape::of(view, placement);
        if self.dead.contains(&shape) {
            return Verdict::Skip(skip_reason);
        }

        let weight = view.requirements.cpu_cores.max(1);
        let mut full = Vec::new();
        let mut candidates = self.candidates(view, &view.avoid, &mut full);
        if candidates.is_empty() && !view.avoid.is_empty() {
            // Avoiding is a preference: with nobody else eligible, a worker that refused the task
            // before still gets it rather than leaving it to starve.
            candidates = self.candidates(view, &[], &mut full);
        }
        if candidates.is_empty() {
            self.hold_back(&full, weight);
            self.dead.insert(shape);
            return Verdict::Skip(skip_reason);
        }

        let chosen = self.rank(view, &mut candidates);
        let seat = &mut self.seats[chosen];
        let free_before = seat.max_slots.saturating_sub(seat.used_slots());
        seat.placed.tasks += 1;
        seat.placed.slots += weight;
        seat.placed.unstarted_ram_mb += view.requirements.ram_mb;
        self.free_slots = self.free_slots.saturating_sub(weight.min(free_before));
        Verdict::Placed(chosen)
    }

    /// A task found no worker only because the workers that could run it lack free slots (`full`,
    /// from `candidates`). Without a rule, narrower tasks further down the queue keep refilling
    /// those workers as slots free up, and a task that needs many slots at once (it fits a busy
    /// worker only when enough of its slots are free, or when it is idle) never gets them. So the
    /// worker that needs the fewest slots to free up for it is held back for the rest of the
    /// pass: it takes nothing more, drains, and the task is placed on it in a later pass. Later
    /// tasks only lose a worker that could not have run the waiting task either way.
    fn hold_back(&mut self, full: &[usize], weight: usize) {
        let slots_to_free = |seat: &Seat<'_>| {
            let used = seat.used_slots();
            if weight > seat.max_slots {
                // Only an idle worker takes it.
                used
            } else {
                (used + weight).saturating_sub(seat.max_slots)
            }
        };
        let Some(&idx) = full
            .iter()
            .min_by_key(|&&idx| (slots_to_free(&self.seats[idx]), idx))
        else {
            return;
        };
        self.reserved[idx] = true;
        let seat = &self.seats[idx];
        // Its free slots are out of reach for the rest of the pass.
        self.free_slots = self
            .free_slots
            .saturating_sub(seat.max_slots.saturating_sub(seat.used_slots()));
    }

    /// Indices of the workers that may run `view` right now. `full` is cleared and then receives
    /// the workers that would have taken it but for free slots.
    fn candidates(&self, view: &TaskView, avoided: &[Uuid], full: &mut Vec<usize>) -> Vec<usize> {
        let config = self.config;
        let req = &view.requirements;
        let weight = req.cpu_cores.max(1);
        let mut found: Vec<usize> = Vec::new();
        full.clear();

        for (idx, seat) in self.seats.iter().enumerate() {
            let worker = seat.worker;
            if self.reserved[idx] {
                continue;
            }
            // Stage 1: Connectivity
            if worker.status == WorkerStatus::Disconnected || avoided.contains(&worker.worker_id) {
                continue;
            }

            // Stage 2: Hardware requirements (CPU & RAM), placement constraints (os, arch, worker,
            // tag) & Anti-stuttering backpressure
            if !view_fits(view, worker) {
                continue;
            }
            // Anti-stuttering: if host CPU exceeds threshold, back off and skip this worker
            if worker.cpu_usage_pct > config.max_host_cpu_pct {
                continue;
            }
            // Free RAM check against what the worker last reported free, minus the declared RAM of
            // tasks that were handed to it but have not started (they are not in that figure yet).
            if req.ram_mb > 0 && worker.ram_available_mb > 0 {
                let reserved = seat.base.unstarted_ram_mb + seat.placed.unstarted_ram_mb;
                if worker.ram_available_mb.saturating_sub(reserved) < req.ram_mb {
                    continue;
                }
            }

            // Stage 3: Strict GPU Gating (Requirement R3 & AC4)
            // Dual-gated: if task demands GPU, worker MUST advertise has_gpu or is_simulated_gpu
            // Non-GPU workers NEVER receive GPU tasks under any circumstances!
            if view.requires_gpu && !worker.capabilities.can_execute_gpu() {
                continue;
            }

            // Stage 3b: File Staging capability gating (Requirement R2)
            if view.requires_file_staging
                && !worker.features.contains(ProtocolCapabilities::FILE_STAGING)
            {
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

            if is_thermal_throttled && (req.cpu_cores > 1 || view.heavy) {
                continue;
            }
            if is_low_battery && view.heavy {
                continue;
            }

            if let Some(ref mobile) = worker.capabilities.mobile {
                let live = mobile.live_status();
                let bat = live.battery_tier.unwrap_or_else(|| {
                    MobileLiveStatus::compute_battery_tier(live.battery_pct, live.is_charging)
                });
                let therm = live.thermal_tier.unwrap_or_else(|| {
                    MobileLiveStatus::compute_thermal_tier(
                        live.battery_temperature,
                        live.thermal_throttled,
                    )
                });
                let is_charging = live.is_charging.unwrap_or(false);

                // Zero effective concurrency means complete load-shedding
                let eff =
                    calculate_effective_concurrency(worker.capabilities.cpu_cores, bat, therm);
                if eff == 0 {
                    continue;
                }

                // Task eligibility based on battery tier
                match bat {
                    BatteryTier::Critical => continue,
                    BatteryTier::SubLow => {
                        if req.cpu_cores > 1 || view.heavy {
                            continue;
                        }
                    }
                    BatteryTier::Low => {
                        if view.heavy {
                            continue;
                        }
                    }
                    BatteryTier::Normal => {}
                }

                // Task eligibility based on thermal tier
                match therm {
                    ThermalTier::Critical => continue,
                    ThermalTier::Hot => {
                        if req.cpu_cores > 1 || view.heavy {
                            continue;
                        }
                    }
                    ThermalTier::Warm => {
                        if view.heavy && !is_charging {
                            continue;
                        }
                    }
                    ThermalTier::Normal => {}
                }
            }

            // Stage 5: Concurrency saturation, in slots: a task occupies as many as the cores it
            // asks for, so eight 8-core builds cannot all land on a 16-core machine. An idle
            // worker always takes a task it is capable of.
            let used = seat.used_slots();
            if used > 0 && used + weight > seat.max_slots {
                full.push(idx);
                continue;
            }

            found.push(idx);
        }

        // Stage 6: GPU preservation. While a GPU task waits, a generic task takes a CPU-only worker
        // whenever one can run it and only spills onto a GPU worker when none can. While GPU work
        // waits or is likely, a spill-over must leave that GPU worker some slots free.
        if !view.requires_gpu
            && config.preserve_gpu_for_gpu_tasks
            && (self.gpu_waiting || self.gpu_recent)
        {
            let is_gpu = |idx: &usize| self.seats[*idx].worker.capabilities.can_execute_gpu();
            if self.gpu_waiting && found.iter().any(|idx| !is_gpu(idx)) {
                found.retain(|idx| !is_gpu(idx));
            } else {
                found.retain(|idx| {
                    if !is_gpu(idx) {
                        return true;
                    }
                    let seat = &self.seats[*idx];
                    let reserve = config
                        .gpu_reserved_slots
                        .min(seat.max_slots.saturating_sub(1));
                    seat.max_slots.saturating_sub(seat.used_slots() + weight) >= reserve
                });
            }
        }

        // Stage 7: Real-GPU priority.
        // For GPU-requiring tasks, prioritize hardware GPU (wgpu) workers over simulated
        // GPU workers. Simulated GPU workers are only used when no idle real-GPU worker is available.
        if view.requires_gpu {
            let is_real_gpu = |seat: &Seat<'_>| {
                seat.worker.capabilities.has_gpu && !seat.worker.capabilities.is_simulated_gpu
            };
            let has_idle_real_gpu = found
                .iter()
                .any(|&idx| is_real_gpu(&self.seats[idx]) && self.seats[idx].used_slots() == 0);
            if has_idle_real_gpu {
                found.retain(|&idx| is_real_gpu(&self.seats[idx]));
            }
        }

        found
    }

    /// Picks the candidate to use according to the policy.
    fn rank(&mut self, view: &TaskView, candidates: &mut [usize]) -> usize {
        let config = self.config;
        let seats = &self.seats;
        let prefer_cpu_only = config.preserve_gpu_for_gpu_tasks && !view.requires_gpu;
        let gpu_pref = |a: &Seat<'_>, b: &Seat<'_>| {
            if prefer_cpu_only {
                a.worker
                    .capabilities
                    .can_execute_gpu()
                    .cmp(&b.worker.capabilities.can_execute_gpu())
            } else if view.requires_gpu {
                let a_real =
                    a.worker.capabilities.has_gpu && !a.worker.capabilities.is_simulated_gpu;
                let b_real =
                    b.worker.capabilities.has_gpu && !b.worker.capabilities.is_simulated_gpu;
                b_real.cmp(&a_real)
            } else {
                std::cmp::Ordering::Equal
            }
        };
        match config.policy {
            SchedulingPolicy::RoundRobin => {
                if view.requires_gpu {
                    let real_candidates: Vec<usize> = candidates
                        .iter()
                        .copied()
                        .filter(|&c| {
                            seats[c].worker.capabilities.has_gpu
                                && !seats[c].worker.capabilities.is_simulated_gpu
                        })
                        .collect();
                    if !real_candidates.is_empty() {
                        let idx = self.rr_cursor % real_candidates.len();
                        self.rr_cursor += 1;
                        return real_candidates[idx];
                    }
                }
                let idx = self.rr_cursor % candidates.len();
                self.rr_cursor += 1;
                candidates[idx]
            }
            SchedulingPolicy::LeastLoaded => {
                candidates.sort_by(|&a, &b| {
                    let (sa, sb) = (&seats[a], &seats[b]);
                    sa.load()
                        .cmp(&sb.load())
                        .then_with(|| gpu_pref(sa, sb))
                        .then_with(|| {
                            sb.worker
                                .capabilities
                                .cpu_cores
                                .cmp(&sa.worker.capabilities.cpu_cores)
                        })
                        .then_with(|| sa.worker.worker_id.cmp(&sb.worker.worker_id))
                });
                candidates[0]
            }
            SchedulingPolicy::WeightedLeastLoaded => {
                let ratio = |s: &Seat<'_>| {
                    (s.load() as f64 / s.worker.capabilities.cpu_cores.max(1) as f64)
                        + 0.5 * (s.worker.cpu_usage_pct.max(0.0) as f64 / 100.0)
                };
                candidates.sort_by(|&a, &b| {
                    let (sa, sb) = (&seats[a], &seats[b]);
                    ratio(sa)
                        .partial_cmp(&ratio(sb))
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| gpu_pref(sa, sb))
                        .then_with(|| {
                            sb.worker
                                .capabilities
                                .cpu_cores
                                .cmp(&sa.worker.capabilities.cpu_cores)
                        })
                        .then_with(|| sa.worker.worker_id.cmp(&sb.worker.worker_id))
                });
                candidates[0]
            }
        }
    }
}

/// Whether GPU-capable work is waiting or likely, and when it was last seen.
#[derive(Default)]
struct GpuDemand {
    /// Last time a placeable GPU task was seen queued or assigned.
    last_seen: Option<Instant>,
    /// Last time the whole queue was probed for GPU work, and what the probe found.
    last_probe: Option<Instant>,
    last_waiting: bool,
}

/// Autonomous workload-specific scheduling engine.
pub struct WorkloadScheduler {
    config: SchedulerConfig,
    registry: WorkerRegistry,
    queue: TaskQueue,
    trigger: Arc<Notify>,
    /// Client waiters, so a task that runs out of failovers during a dispatch rollback still wakes
    /// its `wait = true` client.
    waiters: Option<WaiterMap>,
    gpu_demand: std::sync::Mutex<GpuDemand>,
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
            waiters: None,
            gpu_demand: std::sync::Mutex::new(GpuDemand::default()),
        }
    }

    /// Hands the scheduler the client waiter map (see the `waiters` field).
    pub fn with_waiters(mut self, waiters: WaiterMap) -> Self {
        self.waiters = Some(waiters);
        self
    }

    /// Returns a reference to the scheduler configuration.
    pub fn config(&self) -> &SchedulerConfig {
        &self.config
    }

    /// Pure matchmaking function: matches a list of queued tasks against a snapshot of worker metadata.
    ///
    /// This function is entirely synchronous, pure, and free of I/O or global state,
    /// making it 100% deterministic and testable. A worker's `active_tasks` is its load.
    pub fn matchmake(
        config: &SchedulerConfig,
        tasks: &[Task],
        workers: &[WorkerInfo],
    ) -> ScheduleReport {
        Self::matchmake_with_avoid(config, tasks, workers, &HashMap::new())
    }

    /// [`matchmake`](Self::matchmake) honouring per-task "avoid these workers" lists (workers that
    /// already rejected the task). Avoiding is a preference: if every otherwise eligible worker is
    /// on a task's list, the task is placed on one of them anyway rather than starving.
    pub fn matchmake_with_avoid(
        config: &SchedulerConfig,
        tasks: &[Task],
        workers: &[WorkerInfo],
        avoid: &HashMap<TaskId, Vec<Uuid>>,
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

        let views: Vec<TaskView> = tasks
            .iter()
            .map(|task| {
                let mut view = TaskView::of(task);
                if let Some(avoided) = avoid.get(&task.id) {
                    view.avoid = avoided.clone();
                }
                view
            })
            .collect();
        // GPU-capable work in the batch itself is "waiting".
        let gpu_waiting = views.iter().any(|v| v.requires_gpu);
        let mut engine = Engine::new(config, workers, None, gpu_waiting, false);
        for (task, view) in tasks.iter().zip(&views) {
            match engine.place(view) {
                Verdict::Placed(idx) => {
                    let worker = engine.seats[idx].worker;
                    report.assignments.push(ScheduleAssignment {
                        task_id: task.id,
                        worker_id: worker.worker_id,
                        session_id: worker.session_id,
                        task: task.clone(),
                    });
                }
                Verdict::Skip(reason) => report.skipped.push((task.id, reason)),
                Verdict::Unfulfillable(reason) => report.unfulfillable.push((task.id, reason)),
            }
        }
        report
    }

    /// `(waiting, recent)`: whether a GPU task that some GPU worker could run is queued right now,
    /// and whether GPU work was seen within `gpu_demand_window` (see `GpuDemand`). The queue is
    /// probed at most once per `GPU_PROBE_INTERVAL` (the last answer is reused in between), and
    /// not at all when there is no GPU worker or preservation is off.
    async fn gpu_demand(&self, workers: &[WorkerInfo]) -> (bool, bool) {
        if !self.config.preserve_gpu_for_gpu_tasks {
            return (false, false);
        }
        let gpu_workers: Vec<&WorkerInfo> = workers
            .iter()
            .filter(|w| w.capabilities.can_execute_gpu())
            .collect();
        if gpu_workers.is_empty() {
            return (false, false);
        }
        let now = Instant::now();
        let (cached_waiting, probe_due) = {
            let state = self.gpu_demand.lock().unwrap_or_else(|e| e.into_inner());
            (
                state.last_waiting,
                match state.last_probe {
                    Some(probe) => now.duration_since(probe) >= GPU_PROBE_INTERVAL,
                    None => true,
                },
            )
        };
        let waiting = if probe_due {
            let found = self
                .queue
                .any_queued(|task| {
                    (task.requirements.gpu_required || task.spec.requires_gpu())
                        && task.placement().is_ok_and(|placement| {
                            gpu_workers.iter().any(|w| {
                                statically_fits(&task.requirements, w)
                                    && placement_admits(&placement, w)
                            })
                        })
                })
                .await;
            let mut state = self.gpu_demand.lock().unwrap_or_else(|e| e.into_inner());
            state.last_probe = Some(now);
            state.last_waiting = found;
            if found {
                state.last_seen = Some(now);
            }
            found
        } else {
            cached_waiting
        };
        let recent = {
            let state = self.gpu_demand.lock().unwrap_or_else(|e| e.into_inner());
            state
                .last_seen
                .is_some_and(|seen| now.duration_since(seen) < self.config.gpu_demand_window)
        };
        (waiting, recent || waiting)
    }

    fn note_gpu_demand(&self) {
        let mut state = self.gpu_demand.lock().unwrap_or_else(|e| e.into_inner());
        state.last_seen = Some(Instant::now());
    }

    /// Performs an atomic batch scheduling pass assigning up to `max_batch_size` tasks:
    /// 1. Processes expired delayed retries in the queue.
    /// 2. Takes the active workers and what the master's own books say each one carries.
    /// 3. Walks the queue in priority-FIFO order, a page at a time, through the pure placement
    ///    engine. Tasks nobody can run right now are skipped and the walk continues, so they never
    ///    starve the tasks behind them; it ends when `max_batch_size` tasks are placed, the cluster
    ///    has no free slot left, or the queue (or `max_scan_per_pass`) is exhausted.
    /// 4. Atomically transitions all assigned tasks to `Scheduled` under a single write lock.
    /// 5. Dispatches `AssignTask` messages to the matched workers.
    pub async fn schedule_batch(&self, max_batch_size: usize) -> GridResult<ScheduleReport> {
        // Step 1: Advance any delayed retries whose backoff has passed
        let advanced_retries = self.queue.process_delayed_retries().await;
        if advanced_retries > 0 {
            debug!(
                count = advanced_retries,
                "Re-enqueued backoff-elapsed retries"
            );
        }

        // Step 2: Active workers (checked before touching the queue: no worker, nothing to clone)
        let workers = self.registry.list_schedulable_workers().await;
        if workers.is_empty() {
            let mut report = ScheduleReport::default();
            for task_id in self.queue.ready_task_ids(max_batch_size).await {
                report
                    .skipped
                    .push((task_id, ScheduleSkipReason::NoEligibleWorkers));
            }
            return Ok(report);
        }

        let loads = self.queue.assigned_loads().await;
        let (gpu_waiting, gpu_recent) = self.gpu_demand(&workers).await;
        let mut engine = Engine::new(
            &self.config,
            &workers,
            Some(&loads),
            gpu_waiting,
            gpu_recent,
        );

        // Step 3: Walk the queue
        let mut report = ScheduleReport::default();
        let mut placements: Vec<(TaskId, usize)> = Vec::new();
        if engine.free_slots > 0 && max_batch_size > 0 {
            let mut cursor: Option<QueueOrderKey> = None;
            let mut scanned = 0usize;
            let scan_budget = self.config.max_scan_per_pass.max(1);
            'scan: loop {
                let page_limit = SCAN_PAGE.min(scan_budget.saturating_sub(scanned));
                if page_limit == 0 {
                    break;
                }
                let (views, next) = self.queue.ready_views(cursor, page_limit).await;
                scanned += views.len();
                if !engine.gpu_waiting
                    && self.config.preserve_gpu_for_gpu_tasks
                    && views.iter().any(|v| {
                        v.requires_gpu
                            && workers
                                .iter()
                                .any(|w| w.capabilities.can_execute_gpu() && view_fits(v, w))
                    })
                {
                    engine.gpu_waiting = true;
                    engine.gpu_recent = true;
                    self.note_gpu_demand();
                }
                for view in &views {
                    match engine.place(view) {
                        Verdict::Placed(idx) => {
                            placements.push((view.task_id, idx));
                            if view.requires_gpu {
                                self.note_gpu_demand();
                            }
                            if placements.len() >= max_batch_size || engine.free_slots == 0 {
                                break 'scan;
                            }
                        }
                        Verdict::Skip(reason) => {
                            if report.skipped.len() < max_batch_size {
                                report.skipped.push((view.task_id, reason));
                            }
                            // A worker held back for this task may have been the last one with room.
                            if engine.free_slots == 0 {
                                break 'scan;
                            }
                        }
                        Verdict::Unfulfillable(reason) => {
                            report.unfulfillable.push((view.task_id, reason));
                        }
                    }
                }
                match next {
                    Some(key) => cursor = Some(key),
                    None => break,
                }
            }
        }
        if placements.is_empty() {
            return Ok(report);
        }

        // Step 4: Atomically transition batch in TaskQueue, recording the session each task is bound to
        let assignment_triples: Vec<(TaskId, Uuid, u64)> = placements
            .iter()
            .map(|&(task_id, idx)| {
                let worker = engine.seats[idx].worker;
                (task_id, worker.worker_id, worker.session_id)
            })
            .collect();

        let mut scheduled: HashMap<TaskId, Task> = self
            .queue
            .schedule_tasks_batch_in_sessions(&assignment_triples)
            .await
            .into_iter()
            .map(|(task_id, _worker_id, task)| (task_id, task))
            .collect();

        // Only tasks that really left `Queued` (not cancelled meanwhile) are assignments.
        for (task_id, worker_id, session_id) in assignment_triples {
            if let Some(task) = scheduled.remove(&task_id) {
                report.assignments.push(ScheduleAssignment {
                    task_id,
                    worker_id,
                    session_id,
                    task,
                });
            }
        }

        // Step 5: Dispatch AssignTask frames to workers
        for assignment in &report.assignments {
            let task = &assignment.task;
            let msg = if task.latest_checkpoint.is_some() {
                MasterMessage::AssignTaskWithCheckpoint {
                    task: task.clone(),
                    latest_checkpoint: task.latest_checkpoint.clone(),
                }
            } else {
                MasterMessage::AssignTask { task: task.clone() }
            };
            self.dispatch(assignment, msg).await;
        }

        Ok(report)
    }

    /// Delivers one assignment to the connection it was made against.
    ///
    /// The slot is reserved on the assignment's *session*: if the worker disconnected or
    /// re-registered since the snapshot, nothing was sent and only this task is put back. If the
    /// session's channel is dead the whole session is torn down and failed over (waking any
    /// `wait = true` client whose task runs out of failovers), instead of the former sweep that
    /// requeued, and charged a retry to, every unrelated healthy task of the worker.
    async fn dispatch(&self, assignment: &ScheduleAssignment, msg: MasterMessage) {
        let ScheduleAssignment {
            task_id,
            worker_id,
            session_id,
            ..
        } = assignment;

        let sender = match self.registry.reserve_dispatch(worker_id, *session_id).await {
            Ok(sender) => sender,
            Err(e) => {
                warn!(
                    task_id = %task_id,
                    worker_id = %worker_id,
                    session_id = session_id,
                    error = %e,
                    "Worker session is gone; returning assignment to the queue"
                );
                let _ = self
                    .queue
                    .requeue_dispatch_failure(task_id, worker_id, *session_id)
                    .await;
                return;
            }
        };

        // Wait for room in the worker's channel first (a slow worker can keep us here for a long
        // time, and a client may cancel the task meanwhile), and only then confirm, with nothing
        // in between, that the task is still ours to deliver.
        let delivered = match sender.reserve().await {
            Ok(permit) => Ok(self
                .queue
                .deliver_if_scheduled(task_id, worker_id, *session_id, move || permit.send(msg))
                .await),
            Err(_) => Err(()),
        };
        match delivered {
            Ok(Some(())) => {
                debug!(
                    task_id = %task_id,
                    worker_id = %worker_id,
                    "Successfully dispatched AssignTask to worker"
                );
                return;
            }
            Ok(None) => {
                // Cancelled, failed over or reassigned between the placement and now: the worker
                // never hears of it, so it must not run it (and its slot is free again).
                debug!(
                    task_id = %task_id,
                    worker_id = %worker_id,
                    session_id = session_id,
                    "Task is no longer scheduled on this session; not delivering AssignTask"
                );
                self.registry.release_dispatch(worker_id, *session_id).await;
                return;
            }
            Err(()) => {}
        }

        warn!(
            task_id = %task_id,
            worker_id = %worker_id,
            session_id = session_id,
            "Failed to dispatch AssignTask to worker; rolling back assignment"
        );
        let _ = self.registry.decrement_active_tasks(worker_id).await;
        // Requeue this task first, uncharged: it never left the master, and the sweep below
        // would otherwise count it as a failover.
        let _ = self
            .queue
            .requeue_dispatch_failure(task_id, worker_id, *session_id)
            .await;
        crate::server::handle_worker_disconnect_with_broadcast(
            &self.registry,
            &self.queue,
            &self.trigger,
            self.waiters.as_ref(),
            worker_id,
            Some(*session_id),
            "Failed to deliver AssignTask message",
            false,
            #[cfg(feature = "dashboard")]
            None,
        )
        .await;
    }

    /// Performs a single scheduling pass, delegating to `schedule_batch` with configured `max_batch_size`.
    pub async fn schedule_once(&self) -> GridResult<ScheduleReport> {
        self.schedule_batch(self.config.max_batch_size).await
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
                        if self.config.micro_batch_window > Duration::ZERO {
                            tokio::time::sleep(self.config.micro_batch_window).await;
                        }
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
            features: ProtocolCapabilities::all_supported(),
        }
    }

    /// A 4-core CPU worker carrying `tags` (as `os:` / `arch:` / user tags arrive from a worker).
    fn tagged_worker(name: &str, tags: &[&str]) -> WorkerInfo {
        let mut worker = make_worker(name, 4, 8192, false, false);
        worker.capabilities.tags = tags.iter().map(|t| t.to_string()).collect();
        worker
    }

    /// A one-core built-in task with these `require:` constraints.
    fn constrained_task(constraints: &[&str]) -> Task {
        let task = Task::new(
            TaskSpec::builtin_test("hash_compute", 10),
            TaskRequirements::generic(1, 30),
        )
        .with_tags(constraints.iter().map(|t| t.to_string()).collect());
        task.validate().expect("test constraints must be valid");
        task
    }

    #[test]
    fn test_placement_pins_a_task_to_one_worker_by_id_or_name() {
        let config = SchedulerConfig::default();
        let mac = tagged_worker("MAC-M5", &["os:macos", "arch:aarch64"]);
        let dell = tagged_worker("DELL-VOSTRO", &["os:windows", "arch:x86_64"]);
        let workers = vec![mac.clone(), dell.clone()];

        let by_id = constrained_task(&[&format!("require:worker={}", dell.worker_id)]);
        let by_name = constrained_task(&["require:worker=mac-m5"]);
        let report =
            WorkloadScheduler::matchmake(&config, &[by_id.clone(), by_name.clone()], &workers);
        let placed: HashMap<TaskId, Uuid> = report
            .assignments
            .iter()
            .map(|a| (a.task_id, a.worker_id))
            .collect();
        assert_eq!(placed.get(&by_id.id), Some(&dell.worker_id));
        assert_eq!(
            placed.get(&by_name.id),
            Some(&mac.worker_id),
            "names ignore case"
        );
    }

    #[test]
    fn test_placement_os_arch_and_tag_constraints_select_matching_workers_only() {
        let config = SchedulerConfig::default();
        let mac = tagged_worker("mac", &["os:macos", "arch:aarch64", "laptop"]);
        let dell = tagged_worker("dell", &["os:windows", "arch:x86_64", "laptop"]);
        let desk = tagged_worker("desk", &["os:windows", "arch:x86_64", "gpu-box"]);
        let workers = vec![mac.clone(), dell.clone(), desk.clone()];

        for _ in 0..8 {
            let on_windows_laptop = constrained_task(&["require:os=win", "require:tag=laptop"]);
            let on_arm = constrained_task(&["require:arch=arm64"]);
            let report = WorkloadScheduler::matchmake(
                &config,
                &[on_windows_laptop.clone(), on_arm.clone()],
                &workers,
            );
            for a in &report.assignments {
                if a.task_id == on_windows_laptop.id {
                    assert_eq!(a.worker_id, dell.worker_id, "only dell is a Windows laptop");
                } else {
                    assert_eq!(a.worker_id, mac.worker_id, "only mac is aarch64");
                }
            }
            assert_eq!(report.assignments.len(), 2);
        }
    }

    #[test]
    fn test_placement_without_a_match_waits_and_does_not_block_other_tasks() {
        let config = SchedulerConfig::default();
        let workers = vec![tagged_worker("dell", &["os:windows"])];
        let on_linux = constrained_task(&["require:os=linux"]);
        let anywhere = constrained_task(&[]);

        let report =
            WorkloadScheduler::matchmake(&config, &[on_linux.clone(), anywhere.clone()], &workers);
        assert_eq!(report.assignments.len(), 1);
        assert_eq!(report.assignments[0].task_id, anywhere.id);
        assert_eq!(report.skipped.len(), 1);
        let (skipped_id, reason) = &report.skipped[0];
        assert_eq!(*skipped_id, on_linux.id);
        match reason {
            ScheduleSkipReason::ClusterLacksCapability(msg) => {
                assert!(msg.contains("os=linux"), "{msg}")
            }
            other => panic!("unexpected skip reason {other:?}"),
        }
        assert!(
            report.unfulfillable.is_empty(),
            "it waits for a matching worker"
        );

        let strict = SchedulerConfig {
            fail_unfulfillable_tasks: true,
            ..SchedulerConfig::default()
        };
        let report =
            WorkloadScheduler::matchmake(&strict, std::slice::from_ref(&on_linux), &workers);
        assert_eq!(report.unfulfillable.len(), 1);
        assert!(report.unfulfillable[0].1.contains("os=linux"));
    }

    #[test]
    fn test_placement_is_part_of_the_task_shape() {
        // Same resources, different constraints: the first finds no worker, which must not make
        // the engine skip the second as "the same kind of task".
        let config = SchedulerConfig::default();
        let workers = vec![tagged_worker("mac", &["os:macos"])];
        let on_windows = constrained_task(&["require:os=windows"]);
        let on_macos = constrained_task(&["require:os=macos"]);

        let report = WorkloadScheduler::matchmake(
            &config,
            &[on_windows.clone(), on_macos.clone()],
            &workers,
        );
        assert_eq!(report.assignments.len(), 1);
        assert_eq!(report.assignments[0].task_id, on_macos.id);
    }

    #[test]
    fn test_a_pinned_task_waiting_for_slots_never_spills_to_another_worker() {
        let config = SchedulerConfig {
            core_concurrency_multiplier: 1.0,
            ..Default::default()
        };
        let mut busy = tagged_worker("busy", &["os:macos"]);
        busy.capabilities.cpu_cores = 2;
        busy.active_tasks = 2;
        let idle = tagged_worker("idle", &["os:windows"]);
        let workers = vec![busy.clone(), idle.clone()];

        let pinned = constrained_task(&[&format!("require:worker={}", busy.worker_id)]);
        let free = constrained_task(&[]);
        let report =
            WorkloadScheduler::matchmake(&config, &[pinned.clone(), free.clone()], &workers);
        assert_eq!(report.assignments.len(), 1, "{report:?}");
        assert_eq!(report.assignments[0].task_id, free.id);
        assert_eq!(report.assignments[0].worker_id, idle.worker_id);
        assert_eq!(report.skipped[0].0, pinned.id);
        assert_eq!(
            report.skipped[0].1,
            ScheduleSkipReason::AllEligibleWorkersSaturated
        );
    }

    #[test]
    fn test_unreadable_placement_constraints_place_the_task_nowhere() {
        let config = SchedulerConfig::default();
        let workers = vec![tagged_worker("w", &["os:linux"])];
        // Bypasses `Task::validate`, as only a task inserted without submission could.
        let task = Task::new(
            TaskSpec::builtin_test("hash_compute", 10),
            TaskRequirements::generic(1, 30),
        )
        .with_tags(vec!["require:gpu_vendor=nvidia".into()]);
        let report = WorkloadScheduler::matchmake(&config, std::slice::from_ref(&task), &workers);
        assert!(report.assignments.is_empty());
        assert_eq!(report.unfulfillable.len(), 1);
        assert!(
            report.unfulfillable[0]
                .1
                .contains("Invalid placement constraints"),
            "{:?}",
            report.unfulfillable
        );
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

    fn cmd_task(label: &str) -> Task {
        Task::new(
            TaskSpec::command("echo", vec![label.to_string()]),
            TaskRequirements::generic(1, 10),
        )
    }

    #[test]
    fn test_matchmake_prefers_workers_that_did_not_reject_the_task() {
        let config = SchedulerConfig::default();
        let rejecting = make_worker("phone", 8, 8192, false, false); // the least-loaded, biggest worker
        let mut other = make_worker("mac", 2, 8192, false, false);
        other.active_tasks = 1;
        let task = cmd_task("t");

        let tasks = std::slice::from_ref(&task);
        let plain =
            WorkloadScheduler::matchmake(&config, tasks, &[rejecting.clone(), other.clone()]);
        assert_eq!(plain.assignments[0].worker_id, rejecting.worker_id);

        let mut avoid = HashMap::new();
        avoid.insert(task.id, vec![rejecting.worker_id]);
        let steered = WorkloadScheduler::matchmake_with_avoid(
            &config,
            tasks,
            &[rejecting.clone(), other.clone()],
            &avoid,
        );
        assert_eq!(steered.assignments[0].worker_id, other.worker_id);
        assert_eq!(steered.assignments[0].session_id, other.session_id);

        // Avoiding is a preference, not a ban: with nobody else eligible the task still runs.
        let only = WorkloadScheduler::matchmake_with_avoid(
            &config,
            tasks,
            std::slice::from_ref(&rejecting),
            &avoid,
        );
        assert_eq!(only.assignments.len(), 1);
        assert_eq!(only.assignments[0].worker_id, rejecting.worker_id);
    }

    async fn scheduler_fixture() -> (WorkerRegistry, TaskQueue, WaiterMap, Arc<Notify>) {
        (
            WorkerRegistry::new(),
            TaskQueue::new(),
            Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            Arc::new(Notify::new()),
        )
    }

    #[tokio::test]
    async fn test_task_cancelled_while_the_pass_waits_on_a_full_channel_is_never_assigned() {
        let (registry, queue, waiters, notify) = scheduler_fixture().await;
        let w = Uuid::new_v4();
        // Room for one message: the worker's channel is full until the test drains it.
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        registry
            .register(
                w,
                WorkerCapabilities::new("w", 4, 4096, false, false, None),
                "127.0.0.1:9101".parse().unwrap(),
                tx.clone(),
                None,
            )
            .await
            .unwrap();
        tx.send(MasterMessage::HeartbeatAck { timestamp: 1 })
            .await
            .unwrap();
        let first = queue.submit(cmd_task("t1")).await.unwrap();
        let second = queue.submit(cmd_task("t2")).await.unwrap();
        let sched = Arc::new(
            WorkloadScheduler::new(
                SchedulerConfig::default(),
                registry.clone(),
                queue.clone(),
                notify.clone(),
            )
            .with_waiters(waiters.clone()),
        );
        // Both tasks are placed in one pass and marked Scheduled; delivering t1 then blocks.
        let pass = {
            let sched = sched.clone();
            tokio::spawn(async move { sched.schedule_once().await })
        };
        for _ in 0..200 {
            if queue.get_state(&second).await == Some(crate::queue::TaskState::Scheduled) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            queue.get_state(&second).await,
            Some(crate::queue::TaskState::Scheduled)
        );

        // The client cancels t2; its CancelTask has to wait for room in the same full channel.
        let cancel = {
            let (registry, queue, waiters, notify) = (
                registry.clone(),
                queue.clone(),
                waiters.clone(),
                notify.clone(),
            );
            tokio::spawn(async move {
                crate::server::cancel_task_and_notify(
                    &registry, &queue, &notify, &waiters, second, "stop",
                )
                .await
            })
        };
        for _ in 0..200 {
            if queue.get_state(&second).await == Some(crate::queue::TaskState::Cancelled) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            queue.get_state(&second).await,
            Some(crate::queue::TaskState::Cancelled)
        );

        // The worker now reads everything the master sent it.
        let mut received = Vec::new();
        while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(500), rx.recv()).await
        {
            received.push(msg);
        }
        pass.await.unwrap().unwrap();
        assert!(cancel.await.unwrap().unwrap());

        let assigned: Vec<TaskId> = received
            .iter()
            .filter_map(|msg| match msg {
                MasterMessage::AssignTask { task } => Some(task.id),
                _ => None,
            })
            .collect();
        assert_eq!(
            assigned,
            vec![first],
            "a task cancelled before delivery must not be assigned: {received:?}"
        );
        assert!(received.iter().any(
            |msg| matches!(msg, MasterMessage::CancelTask { task_id, .. } if *task_id == second)
        ));
        // The slot reserved for the undelivered task is free again.
        assert_eq!(registry.get_worker(w).await.unwrap().active_tasks, 1);
    }

    #[tokio::test]
    async fn test_assignment_is_bound_to_the_session_it_was_made_for() {
        let (registry, queue, waiters, notify) = scheduler_fixture().await;
        let w = Uuid::new_v4();
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let session = registry
            .register(
                w,
                WorkerCapabilities::new("w", 4, 4096, false, false, None),
                "127.0.0.1:9100".parse().unwrap(),
                tx,
                None,
            )
            .await
            .unwrap();
        let id = queue.submit(cmd_task("bound")).await.unwrap();
        let sched = WorkloadScheduler::new(
            SchedulerConfig::default(),
            registry.clone(),
            queue.clone(),
            notify,
        )
        .with_waiters(waiters);
        let report = sched.schedule_once().await.unwrap();
        assert_eq!(report.assignments.len(), 1);
        assert_eq!(report.assignments[0].session_id, session);
        assert!(matches!(
            rx.recv().await,
            Some(MasterMessage::AssignTask { .. })
        ));
        assert_eq!(registry.get_worker(w).await.unwrap().active_tasks, 1);

        // Only the session the task was dispatched on may report on it.
        let stale = queue
            .record_result_from(
                w,
                Some(session + 1),
                rusty_grid_core::task::TaskResult::success(w, id, "x", 1, false),
            )
            .await
            .unwrap();
        assert_eq!(stale.disposition, crate::queue::ResultDisposition::Stale);
        let genuine = queue
            .record_result_from(
                w,
                Some(session),
                rusty_grid_core::task::TaskResult::success(w, id, "x", 1, false),
            )
            .await
            .unwrap();
        assert_eq!(
            genuine.disposition,
            crate::queue::ResultDisposition::Terminal
        );
    }

    #[tokio::test]
    async fn test_dispatch_to_a_superseded_session_requeues_only_that_task_without_penalty() {
        let (registry, queue, _waiters, notify) = scheduler_fixture().await;
        let w = Uuid::new_v4();
        let caps = WorkerCapabilities::new("w", 4, 4096, false, false, None);
        let addr: std::net::SocketAddr = "127.0.0.1:9101".parse().unwrap();
        let (tx1, _rx1) = tokio::sync::mpsc::channel(8);
        let old = registry
            .register(w, caps.clone(), addr, tx1, None)
            .await
            .unwrap();
        let id = queue.submit(cmd_task("racing")).await.unwrap();

        // The assignment is made against the old session; before the dispatch, the worker re-registers.
        queue
            .schedule_tasks_batch_in_sessions(&[(id, w, old)])
            .await;
        let (tx2, mut rx2) = tokio::sync::mpsc::channel(8);
        registry.register(w, caps, addr, tx2, None).await.unwrap();

        let sched = WorkloadScheduler::new(
            SchedulerConfig::default(),
            registry.clone(),
            queue.clone(),
            notify,
        );
        sched
            .dispatch(
                &ScheduleAssignment {
                    task_id: id,
                    worker_id: w,
                    session_id: old,
                    task: cmd_task("racing"),
                },
                MasterMessage::HeartbeatAck { timestamp: 0 },
            )
            .await;

        assert!(
            rx2.try_recv().is_err(),
            "nothing may be delivered to the new connection"
        );
        let info = queue.get_task(&id).await.unwrap();
        assert_eq!(info.state, crate::queue::TaskState::Queued);
        assert_eq!(
            info.retry_count, 0,
            "an undelivered task is not charged a retry"
        );
        assert_eq!(registry.get_worker(w).await.unwrap().active_tasks, 0);
        assert_eq!(
            registry.get_worker(w).await.unwrap().status,
            WorkerStatus::Connected,
            "the fresh session must not be torn down for the old one's failure"
        );
    }

    #[tokio::test]
    async fn test_dispatch_failure_rollback_wakes_waiters_of_tasks_that_run_out_of_failovers() {
        let (registry, queue, waiters, notify) = scheduler_fixture().await;
        let w = Uuid::new_v4();
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let session = registry
            .register(
                w,
                WorkerCapabilities::new("w", 4, 4096, false, false, None),
                "127.0.0.1:9102".parse().unwrap(),
                tx,
                None,
            )
            .await
            .unwrap();

        // An older, non-restartable task is running on the session; a client waits for it.
        let precious = Task::new(
            TaskSpec::command("sleep", vec!["60".into()]),
            TaskRequirements::generic(1, 60).with_max_retries(0),
        );
        let precious_id = queue.submit(precious).await.unwrap();
        queue
            .schedule_tasks_batch_in_sessions(&[(precious_id, w, session)])
            .await;
        let (wtx, wrx) = tokio::sync::oneshot::channel();
        waiters
            .write()
            .await
            .entry(precious_id)
            .or_default()
            .push(wtx);

        // The connection's writer is dead, so the next dispatch fails.
        drop(rx);
        let fresh_id = queue.submit(cmd_task("fresh")).await.unwrap();

        let sched = WorkloadScheduler::new(
            SchedulerConfig::default(),
            registry.clone(),
            queue.clone(),
            notify,
        )
        .with_waiters(waiters.clone());
        sched.schedule_once().await.unwrap();

        // The task that could not be delivered goes back, uncharged...
        let fresh = queue.get_task(&fresh_id).await.unwrap();
        assert_eq!(fresh.state, crate::queue::TaskState::Queued);
        assert_eq!(fresh.retry_count, 0);
        // ...the session is failed over, and the task that ran out of failovers wakes its client.
        assert_eq!(
            queue.get_state(&precious_id).await,
            Some(crate::queue::TaskState::Failed)
        );
        let woken = tokio::time::timeout(Duration::from_secs(1), wrx)
            .await
            .expect("waiter must be resolved, not left hanging")
            .expect("sender must not be dropped unresolved");
        assert_eq!(woken.exit_code, 1);
        assert!(waiters.read().await.get(&precious_id).is_none());
        assert_eq!(
            registry.get_worker(w).await.unwrap().status,
            WorkerStatus::Disconnected
        );
    }

    #[tokio::test]
    async fn test_rejecting_worker_gets_no_new_tasks_while_it_cools_down() {
        let (registry, queue, _waiters, notify) = scheduler_fixture().await;
        let caps = |n: &str| WorkerCapabilities::new(n, 8, 8192, false, false, None);
        let (phone_tx, mut phone_rx) = tokio::sync::mpsc::channel(8);
        let (pc_tx, mut pc_rx) = tokio::sync::mpsc::channel(8);
        let (phone, pc) = (Uuid::new_v4(), Uuid::new_v4());
        let phone_session = registry
            .register(
                phone,
                caps("phone"),
                "127.0.0.1:9103".parse().unwrap(),
                phone_tx,
                None,
            )
            .await
            .unwrap();
        registry
            .register(
                pc,
                caps("pc"),
                "127.0.0.1:9104".parse().unwrap(),
                pc_tx,
                None,
            )
            .await
            .unwrap();
        registry.record_rejection(&phone, Some(phone_session)).await;

        for i in 0..4 {
            queue.submit(cmd_task(&format!("job-{i}"))).await.unwrap();
        }
        let sched = WorkloadScheduler::new(SchedulerConfig::default(), registry, queue, notify);
        let report = sched.schedule_once().await.unwrap();
        assert_eq!(report.assignments.len(), 4);
        assert!(report.assignments.iter().all(|a| a.worker_id == pc));
        assert!(
            phone_rx.try_recv().is_err(),
            "the cooling-down worker must receive nothing"
        );
        assert!(pc_rx.recv().await.is_some());
    }

    // --- Step B: head-of-line blocking, master-side accounting, reservations, GPU preservation ---

    fn gpu_task() -> Task {
        Task::new(
            TaskSpec::gpu_compute("gemm_kernel", 64),
            TaskRequirements::gpu(30),
        )
    }

    async fn register_worker(
        registry: &WorkerRegistry,
        name: &str,
        cores: usize,
        ram_mb: u64,
        gpu: bool,
    ) -> (Uuid, u64, tokio::sync::mpsc::Receiver<MasterMessage>) {
        let id = Uuid::new_v4();
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        let session = registry
            .register(
                id,
                WorkerCapabilities::new(name, cores, ram_mb, gpu, gpu, None),
                "127.0.0.1:9300".parse().unwrap(),
                tx,
                None,
            )
            .await
            .unwrap();
        (id, session, rx)
    }

    #[tokio::test]
    async fn test_unplaceable_tasks_at_the_front_do_not_starve_placeable_ones_behind_them() {
        let (registry, queue, _waiters, notify) = scheduler_fixture().await;
        // Only a CPU worker exists: every GPU task is unplaceable.
        let (cpu, _, mut cpu_rx) = register_worker(&registry, "mac", 4, 16384, false).await;
        // More unplaceable tasks than a scan page, let alone the old 128-task window.
        for _ in 0..600 {
            queue.submit(gpu_task()).await.unwrap();
        }
        let mut cpu_ids = Vec::new();
        for i in 0..3 {
            cpu_ids.push(
                queue
                    .submit(cmd_task(&format!("behind-{i}")))
                    .await
                    .unwrap(),
            );
        }

        let sched =
            WorkloadScheduler::new(SchedulerConfig::default(), registry, queue.clone(), notify);
        let report = sched.schedule_once().await.unwrap();

        assert_eq!(
            report.assignments.len(),
            3,
            "the CPU tasks behind the GPU backlog must be placed"
        );
        assert!(report.assignments.iter().all(|a| a.worker_id == cpu));
        for id in &cpu_ids {
            assert_eq!(
                queue.get_state(id).await,
                Some(crate::queue::TaskState::Scheduled)
            );
        }
        assert!(matches!(
            cpu_rx.recv().await,
            Some(MasterMessage::AssignTask { .. })
        ));
        // The GPU backlog is untouched, not failed or dropped.
        assert_eq!(queue.stats().await.queued, 600);
    }

    #[tokio::test]
    async fn test_scan_budget_bounds_how_far_a_pass_looks() {
        let (registry, queue, _waiters, notify) = scheduler_fixture().await;
        register_worker(&registry, "mac", 4, 16384, false).await;
        for _ in 0..300 {
            queue.submit(gpu_task()).await.unwrap();
        }
        let behind = queue.submit(cmd_task("behind")).await.unwrap();

        let config = SchedulerConfig {
            max_scan_per_pass: 100,
            ..Default::default()
        };
        let sched = WorkloadScheduler::new(config, registry, queue.clone(), notify);
        let report = sched.schedule_once().await.unwrap();
        assert!(
            report.assignments.is_empty(),
            "a pass never looks past max_scan_per_pass"
        );
        assert_eq!(
            queue.get_state(&behind).await,
            Some(crate::queue::TaskState::Queued)
        );
    }

    #[tokio::test]
    async fn test_no_workers_reports_a_bounded_skip_list_without_touching_tasks() {
        let (registry, queue, _waiters, notify) = scheduler_fixture().await;
        for _ in 0..500 {
            queue.submit(cmd_task("waiting")).await.unwrap();
        }
        let sched =
            WorkloadScheduler::new(SchedulerConfig::default(), registry, queue.clone(), notify);
        let report = sched.schedule_once().await.unwrap();
        assert!(report.assignments.is_empty());
        assert_eq!(report.skipped.len(), 128, "bounded by max_batch_size");
        assert!(report
            .skipped
            .iter()
            .all(|(_, reason)| *reason == ScheduleSkipReason::NoEligibleWorkers));
        assert_eq!(queue.stats().await.queued, 500);
    }

    #[tokio::test]
    async fn test_a_pass_assigns_at_most_max_batch_size_tasks() {
        let (registry, queue, _waiters, notify) = scheduler_fixture().await;
        let (_big, _, _big_rx) = register_worker(&registry, "big", 64, 65536, false).await;
        for _ in 0..40 {
            queue.submit(cmd_task("burst")).await.unwrap();
        }
        let config = SchedulerConfig::default().with_max_batch_size(16);
        let sched = WorkloadScheduler::new(config, registry, queue.clone(), notify);
        assert_eq!(sched.schedule_once().await.unwrap().assignments.len(), 16);
        assert_eq!(sched.schedule_once().await.unwrap().assignments.len(), 16);
        assert_eq!(sched.schedule_once().await.unwrap().assignments.len(), 8);
        assert_eq!(queue.stats().await.queued, 0);
    }

    #[tokio::test]
    async fn test_heartbeat_running_count_does_not_make_the_master_top_a_worker_up() {
        let (registry, queue, _waiters, notify) = scheduler_fixture().await;
        let (phone, session, _rx) = register_worker(&registry, "phone", 4, 8192, false).await;
        for i in 0..8 {
            queue.submit(cmd_task(&format!("job-{i}"))).await.unwrap();
        }
        let sched = WorkloadScheduler::new(
            SchedulerConfig::default(),
            registry.clone(),
            queue.clone(),
            notify,
        );
        assert_eq!(
            sched.schedule_once().await.unwrap().assignments.len(),
            4,
            "4 cores = 4 slots"
        );

        // The worker has a smaller concurrency limit than its core count, so its heartbeat only
        // counts what is running (here: 1 of the 4 handed to it). That must not free slots.
        registry
            .record_heartbeat(&phone, 1, 1000, 10.0, 4096, Some(session))
            .await
            .unwrap();
        let report = sched.schedule_once().await.unwrap();
        assert!(
            report.assignments.is_empty(),
            "the worker still holds 4 unfinished tasks; a heartbeat must not reset that"
        );
        assert_eq!(queue.stats().await.queued, 4);

        // Finishing one frees exactly one slot.
        let first = queue
            .list_tasks()
            .await
            .into_iter()
            .find(|t| t.state == crate::queue::TaskState::Scheduled)
            .unwrap()
            .task_id;
        queue
            .record_result_from(
                phone,
                Some(session),
                rusty_grid_core::task::TaskResult::success(phone, first, "", 1, false),
            )
            .await
            .unwrap();
        assert_eq!(sched.schedule_once().await.unwrap().assignments.len(), 1);
    }

    #[test]
    fn test_declared_ram_is_reserved_against_the_workers_free_ram() {
        let config = SchedulerConfig::default();
        let mut mac = make_worker("mac", 10, 16384, false, false);
        mac.ram_available_mb = 14000;
        let tasks: Vec<Task> = (0..10)
            .map(|_| {
                Task::new(
                    TaskSpec::command("train", vec![]),
                    TaskRequirements::new(1, 8192, false, 60),
                )
            })
            .collect();

        let report = WorkloadScheduler::matchmake(&config, &tasks, std::slice::from_ref(&mac));
        assert_eq!(
            report.assignments.len(),
            1,
            "14000 MB free fits one 8192 MB task; the second would overcommit RAM"
        );
        assert_eq!(report.skipped.len(), 9);
    }

    #[tokio::test]
    async fn test_unstarted_tasks_keep_their_ram_reserved_across_passes() {
        let (registry, queue, _waiters, notify) = scheduler_fixture().await;
        let (mac, session, _rx) = register_worker(&registry, "mac", 10, 16384, false).await;
        registry
            .record_heartbeat(&mac, 0, 1000, 5.0, 14000, Some(session))
            .await
            .unwrap();
        for _ in 0..3 {
            let task = Task::new(
                TaskSpec::command("train", vec![]),
                TaskRequirements::new(1, 8192, false, 60),
            );
            queue.submit(task).await.unwrap();
        }
        let sched =
            WorkloadScheduler::new(SchedulerConfig::default(), registry, queue.clone(), notify);
        assert_eq!(sched.schedule_once().await.unwrap().assignments.len(), 1);
        // Handed over but not started: its 8192 MB is not yet in the worker's free-RAM figure.
        assert_eq!(sched.schedule_once().await.unwrap().assignments.len(), 0);

        // Once the worker reports it running, its memory is part of what the worker reports free.
        let first = queue
            .list_tasks()
            .await
            .into_iter()
            .find(|t| t.state == crate::queue::TaskState::Scheduled)
            .unwrap()
            .task_id;
        queue
            .mark_running_from(mac, Some(session), &first)
            .await
            .unwrap();
        assert_eq!(sched.schedule_once().await.unwrap().assignments.len(), 1);
        assert_eq!(sched.schedule_once().await.unwrap().assignments.len(), 0);
    }

    #[test]
    fn test_multi_core_tasks_occupy_as_many_slots_as_cores() {
        let config = SchedulerConfig::default();
        let pc = make_worker("pc", 16, 65536, false, false);
        let builds: Vec<Task> = (0..20)
            .map(|_| {
                Task::new(
                    TaskSpec::command("cargo", vec!["build".into()]),
                    TaskRequirements::generic(8, 60),
                )
            })
            .collect();
        let report = WorkloadScheduler::matchmake(&config, &builds, std::slice::from_ref(&pc));
        assert_eq!(
            report.assignments.len(),
            2,
            "two 8-core builds fill 16 cores"
        );
        assert_eq!(report.skipped.len(), 18);

        // An idle worker always takes a task it is capable of, even one wider than its slot cap.
        let capped = SchedulerConfig {
            max_tasks_per_worker: Some(4),
            ..Default::default()
        };
        let wide = Task::new(
            TaskSpec::command("cargo", vec!["build".into()]),
            TaskRequirements::generic(8, 60),
        );
        let report = WorkloadScheduler::matchmake(&capped, &[wide], std::slice::from_ref(&pc));
        assert_eq!(report.assignments.len(), 1);
    }

    fn placements_per_worker(report: &ScheduleReport) -> HashMap<Uuid, usize> {
        let mut counts = HashMap::new();
        for a in &report.assignments {
            *counts.entry(a.worker_id).or_insert(0) += 1;
        }
        counts
    }

    #[test]
    fn test_cpu_tasks_avoid_the_gpu_worker_while_a_gpu_task_waits() {
        let config = SchedulerConfig::default();
        let gpu = make_worker("gpu-pc", 16, 32768, true, true);
        let mac = make_worker("mac", 8, 16384, false, false);
        let phone = make_worker("phone", 8, 8192, false, false);
        let workers = vec![gpu.clone(), mac.clone(), phone.clone()];

        let cpu_tasks =
            |n: usize| -> Vec<Task> { (0..n).map(|i| cmd_task(&format!("c-{i}"))).collect() };
        let with_gpu_task = |n: usize| -> Vec<Task> {
            let mut batch = vec![gpu_task()];
            batch.extend(cpu_tasks(n));
            batch
        };

        // A GPU task waits: 16 CPU tasks exactly fit the two CPU-only workers, so the GPU PC is
        // left to the GPU task alone (it used to take about a third of them on equal load).
        let report = WorkloadScheduler::matchmake(&config, &with_gpu_task(16), &workers);
        let counts = placements_per_worker(&report);
        assert_eq!(report.assignments.len(), 17);
        assert_eq!(counts[&gpu.worker_id], 1, "only the GPU task: {counts:?}");

        // Beyond the CPU-only capacity the surplus spills onto the GPU PC instead of waiting.
        let report = WorkloadScheduler::matchmake(&config, &with_gpu_task(20), &workers);
        let counts = placements_per_worker(&report);
        assert_eq!(report.assignments.len(), 21);
        assert_eq!(counts[&gpu.worker_id], 1 + 4, "{counts:?}");

        // No GPU work around: the GPU PC is an ordinary worker and shares the load.
        let report = WorkloadScheduler::matchmake(&config, &cpu_tasks(16), &workers);
        let counts = placements_per_worker(&report);
        assert!(
            counts.get(&gpu.worker_id).copied().unwrap_or(0) > 0,
            "{counts:?}"
        );

        // Switched off, a waiting GPU task changes nothing.
        let off = SchedulerConfig {
            preserve_gpu_for_gpu_tasks: false,
            ..Default::default()
        };
        let report = WorkloadScheduler::matchmake(&off, &with_gpu_task(16), &workers);
        assert!(placements_per_worker(&report)[&gpu.worker_id] > 1);
    }

    #[test]
    fn test_a_gpu_worker_keeps_slots_for_gpu_work_against_cpu_spillover_while_gpu_work_waits() {
        let config = SchedulerConfig::default(); // gpu_reserved_slots = 1
        let gpu = make_worker("gpu-pc", 4, 32768, true, true);
        let mac = make_worker("mac", 1, 16384, false, false);
        let workers = vec![gpu.clone(), mac.clone()];

        // A GPU task waits in the same batch: CPU spill-over must leave one GPU slot free.
        let mut batch = vec![gpu_task()];
        batch.extend((0..8).map(|i| cmd_task(&format!("c-{i}"))));
        let report = WorkloadScheduler::matchmake(&config, &batch, &workers);
        let counts = placements_per_worker(&report);
        assert_eq!(counts[&mac.worker_id], 1);
        assert_eq!(
            counts[&gpu.worker_id], 3,
            "1 GPU task + 2 CPU tasks, 1 slot kept free: {counts:?}"
        );

        // Without GPU work around, the same CPU tasks fill the GPU PC completely.
        let cpu_only: Vec<Task> = (0..8).map(|i| cmd_task(&format!("c-{i}"))).collect();
        let report = WorkloadScheduler::matchmake(&config, &cpu_only, &workers);
        let counts = placements_per_worker(&report);
        assert_eq!(counts[&gpu.worker_id], 4, "{counts:?}");

        // A one-slot GPU worker is never starved of CPU work by the reserve.
        let solo = make_worker("solo", 1, 8192, true, true);
        let mut batch = vec![gpu_task()];
        batch.push(cmd_task("c"));
        let report = WorkloadScheduler::matchmake(&config, &batch, std::slice::from_ref(&solo));
        assert_eq!(
            report.assignments.len(),
            1,
            "one slot: the GPU task takes it"
        );
        let report =
            WorkloadScheduler::matchmake(&config, &[cmd_task("c")], std::slice::from_ref(&solo));
        assert_eq!(report.assignments.len(), 1);
    }

    #[tokio::test]
    async fn test_gpu_demand_is_probed_from_the_queue_and_remembered_for_the_window() {
        let (registry, queue, _waiters, notify) = scheduler_fixture().await;
        register_worker(&registry, "gpu-pc", 4, 32768, true).await;
        let workers = registry.list_schedulable_workers().await;
        let sched = WorkloadScheduler::new(
            SchedulerConfig::default(),
            registry.clone(),
            queue.clone(),
            notify.clone(),
        );

        // A GPU task is queued and a GPU worker could run it: demand is waiting (and so recent).
        let gpu_id = queue.submit(gpu_task()).await.unwrap();
        assert_eq!(sched.gpu_demand(&workers).await, (true, true));

        // It leaves the queue; the probe answer is reused for a second, but the sighting is remembered.
        queue.cancel_task(&gpu_id, None).await.unwrap();
        let (_, recent) = sched.gpu_demand(&workers).await;
        assert!(
            recent,
            "GPU work was seen within the window, so more is likely"
        );

        // With a zero window nothing is remembered; and without a GPU worker there is no demand at all.
        let forgetful = WorkloadScheduler::new(
            SchedulerConfig {
                gpu_demand_window: Duration::ZERO,
                ..Default::default()
            },
            registry.clone(),
            queue.clone(),
            notify.clone(),
        );
        assert_eq!(forgetful.gpu_demand(&workers).await, (false, false));
        let cpu_only = vec![make_worker("mac", 4, 8192, false, false)];
        let gpu_id = queue.submit(gpu_task()).await.unwrap();
        assert_eq!(sched.gpu_demand(&cpu_only).await, (false, false));
        queue.cancel_task(&gpu_id, None).await.unwrap();
    }

    #[tokio::test]
    async fn test_a_waiting_gpu_task_keeps_cpu_work_off_the_gpu_worker_through_the_real_pass() {
        let (registry, queue, _waiters, notify) = scheduler_fixture().await;
        let (gpu_pc, _, _gpu_rx) = register_worker(&registry, "gpu-pc", 4, 32768, true).await;
        let (mac, _, _mac_rx) = register_worker(&registry, "mac", 4, 16384, false).await;
        // Queue order: the GPU task first, then CPU tasks that exactly fit the Mac.
        let gpu_id = queue.submit(gpu_task()).await.unwrap();
        let mut cpu_ids = Vec::new();
        for i in 0..4 {
            cpu_ids.push(queue.submit(cmd_task(&format!("cpu-{i}"))).await.unwrap());
        }
        let sched =
            WorkloadScheduler::new(SchedulerConfig::default(), registry, queue.clone(), notify);
        let report = sched.schedule_once().await.unwrap();
        assert_eq!(report.assignments.len(), 5);
        let on = |id: &TaskId| {
            report
                .assignments
                .iter()
                .find(|a| a.task_id == *id)
                .unwrap()
                .worker_id
        };
        assert_eq!(on(&gpu_id), gpu_pc);
        for id in &cpu_ids {
            assert_eq!(
                on(id),
                mac,
                "CPU tasks fit the Mac and must leave the GPU PC alone"
            );
        }
    }

    #[test]
    fn test_a_wide_task_does_not_fit_next_to_many_small_ones() {
        let config = SchedulerConfig::default();
        let pc = make_worker("pc", 16, 65536, false, false);
        // Nine 1-core tasks leave 7 of 16 slots: an 8-core build must not be squeezed in.
        let mut batch: Vec<Task> = (0..9).map(|i| cmd_task(&format!("small-{i}"))).collect();
        let wide = Task::new(
            TaskSpec::command("cargo", vec!["build".into()]),
            TaskRequirements::generic(8, 60),
        );
        batch.push(wide.clone());
        let report = WorkloadScheduler::matchmake(&config, &batch, std::slice::from_ref(&pc));
        assert_eq!(report.assignments.len(), 9);
        assert_eq!(report.skipped.len(), 1);
        assert_eq!(report.skipped[0].0, wide.id);

        // Seven 1-core tasks leave exactly 9: now it fits.
        let mut batch: Vec<Task> = (0..7).map(|i| cmd_task(&format!("small-{i}"))).collect();
        batch.push(wide);
        let report = WorkloadScheduler::matchmake(&config, &batch, std::slice::from_ref(&pc));
        assert_eq!(report.assignments.len(), 8);
    }
    fn wide_task(cores: usize) -> Task {
        Task::new(
            TaskSpec::command("cargo", vec!["build".into()]),
            TaskRequirements::generic(cores, 60),
        )
    }

    #[test]
    fn test_a_wide_task_waiting_for_slots_holds_its_worker_back_from_the_tasks_behind_it() {
        let config = SchedulerConfig::default();
        let mut busy = make_worker("busy", 4, 8192, false, false);
        busy.active_tasks = 3;
        let wide = wide_task(4);
        let mut batch = vec![wide.clone()];
        batch.extend((0..3).map(|i| cmd_task(&format!("narrow-{i}"))));

        // One slot is free, but the 4-core build behind which they queue needs the whole worker:
        // refilling that slot would postpone it again.
        let report = WorkloadScheduler::matchmake(&config, &batch, std::slice::from_ref(&busy));
        assert!(report.assignments.is_empty(), "{:?}", report.assignments);
        assert_eq!(report.skipped.len(), 4);

        // Tasks ahead of the wide one in the queue are not held back by it.
        let mut batch = vec![cmd_task("ahead")];
        batch.push(wide.clone());
        let report = WorkloadScheduler::matchmake(&config, &batch, std::slice::from_ref(&busy));
        assert_eq!(report.assignments.len(), 1);

        // A second worker keeps serving narrow tasks: only the worker closest to having room for
        // the wide task is held back.
        let mut closer = make_worker("closer", 4, 8192, false, false);
        closer.active_tasks = 2;
        let mut batch = vec![wide.clone()];
        batch.extend((0..3).map(|i| cmd_task(&format!("narrow-{i}"))));
        let report = WorkloadScheduler::matchmake(&config, &batch, &[busy.clone(), closer.clone()]);
        assert_eq!(
            placements_per_worker(&report),
            HashMap::from([(busy.worker_id, 1)]),
            "closer (2 busy) is held back for the wide task, busy (3 busy) has one slot left"
        );

        // Once the worker is idle the wide task takes it, and nothing else fits next to it.
        busy.active_tasks = 0;
        let mut batch = vec![wide];
        batch.extend((0..3).map(|i| cmd_task(&format!("narrow-{i}"))));
        let report = WorkloadScheduler::matchmake(&config, &batch, std::slice::from_ref(&busy));
        assert_eq!(report.assignments.len(), 1);
    }

    #[tokio::test]
    async fn test_a_wide_task_is_not_starved_by_a_stream_of_narrow_ones() {
        let (registry, queue, _waiters, notify) = scheduler_fixture().await;
        let (worker, session, _rx) = register_worker(&registry, "builder", 4, 8192, false).await;
        let sched = WorkloadScheduler::new(
            SchedulerConfig::default(),
            registry.clone(),
            queue.clone(),
            notify,
        );
        // The worker is full of 1-core jobs ...
        let mut running = Vec::new();
        for i in 0..4 {
            running.push(
                queue
                    .submit(cmd_task(&format!("running-{i}")))
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(sched.schedule_once().await.unwrap().assignments.len(), 4);
        // ... when a 4-core build with the highest priority and a long stream of 1-core jobs arrive.
        let wide = queue.submit_with_priority(wide_task(4), 10).await.unwrap();
        for i in 0..20 {
            queue
                .submit(cmd_task(&format!("stream-{i}")))
                .await
                .unwrap();
        }

        // Each job that finishes frees a slot; none of them may be given to the stream.
        for (n, id) in running.iter().enumerate() {
            queue
                .record_result_from(
                    worker,
                    Some(session),
                    rusty_grid_core::task::TaskResult::success(worker, *id, "", 1, false),
                )
                .await
                .unwrap();
            let report = sched.schedule_once().await.unwrap();
            if n + 1 < running.len() {
                assert!(
                    report.assignments.is_empty(),
                    "after {} finished the freed slot must be kept for the wide task, got {:?}",
                    n + 1,
                    report
                        .assignments
                        .iter()
                        .map(|a| a.task_id)
                        .collect::<Vec<_>>()
                );
            } else {
                let placed: Vec<TaskId> = report.assignments.iter().map(|a| a.task_id).collect();
                assert_eq!(
                    placed,
                    vec![wide],
                    "the idle worker takes the wide task first"
                );
            }
        }
    }

    #[test]
    fn test_real_gpu_prioritized_over_simulated_gpu_when_both_idle() {
        let config = SchedulerConfig::default();

        let w_real = make_worker("worker-wgpu-real", 8, 16384, true, false);
        let w_sim = make_worker("worker-gpu-simulated", 8, 16384, true, true);
        let workers = vec![w_real.clone(), w_sim.clone()];

        let gpu_task = Task::new(
            TaskSpec::gpu_compute("gemm_kernel", 64),
            TaskRequirements::gpu(30),
        );

        let report =
            WorkloadScheduler::matchmake(&config, std::slice::from_ref(&gpu_task), &workers);

        assert_eq!(report.assignments.len(), 1);
        assert_eq!(report.assignments[0].task_id, gpu_task.id);
        assert_eq!(report.assignments[0].worker_id, w_real.worker_id);
        assert_ne!(report.assignments[0].worker_id, w_sim.worker_id);
    }

    #[test]
    fn test_simulated_gpu_used_when_real_gpu_is_busy() {
        let config = SchedulerConfig {
            core_concurrency_multiplier: 1.0,
            ..Default::default()
        };

        let mut w_real = make_worker("worker-wgpu-real", 2, 8192, true, false);
        w_real.active_tasks = 1; // Real GPU is busy (not idle)
        let w_sim = make_worker("worker-gpu-simulated", 4, 8192, true, true);
        let workers = vec![w_real.clone(), w_sim.clone()];

        let gpu_task = Task::new(
            TaskSpec::gpu_compute("gemm_kernel", 64),
            TaskRequirements::gpu(30),
        );

        let report =
            WorkloadScheduler::matchmake(&config, std::slice::from_ref(&gpu_task), &workers);

        assert_eq!(report.assignments.len(), 1);
        assert_eq!(report.assignments[0].task_id, gpu_task.id);
        // Because real GPU is not idle, simulated GPU can be used and is chosen by least-loaded
        assert_eq!(report.assignments[0].worker_id, w_sim.worker_id);
    }

    #[test]
    fn test_simulated_gpu_used_when_no_real_gpu_worker_in_cluster() {
        let config = SchedulerConfig::default();

        let w_cpu = make_worker("worker-cpu-only", 4, 8192, false, false);
        let w_sim = make_worker("worker-gpu-simulated", 8, 16384, true, true);
        let workers = vec![w_cpu.clone(), w_sim.clone()];

        let gpu_task = Task::new(
            TaskSpec::gpu_compute("gemm_kernel", 64),
            TaskRequirements::gpu(30),
        );

        let report =
            WorkloadScheduler::matchmake(&config, std::slice::from_ref(&gpu_task), &workers);

        assert_eq!(report.assignments.len(), 1);
        assert_eq!(report.assignments[0].task_id, gpu_task.id);
        assert_eq!(report.assignments[0].worker_id, w_sim.worker_id);
        assert_ne!(report.assignments[0].worker_id, w_cpu.worker_id);
    }

    #[test]
    fn test_real_gpu_priority_under_round_robin_policy() {
        let config = SchedulerConfig {
            policy: SchedulingPolicy::RoundRobin,
            ..Default::default()
        };

        let w_real1 = make_worker("worker-wgpu-1", 4, 8192, true, false);
        let w_real2 = make_worker("worker-wgpu-2", 4, 8192, true, false);
        let w_sim = make_worker("worker-gpu-sim", 4, 8192, true, true);
        let workers = vec![w_real1.clone(), w_real2.clone(), w_sim.clone()];

        let task1 = Task::new(TaskSpec::gpu_compute("k1", 10), TaskRequirements::gpu(10));
        let task2 = Task::new(TaskSpec::gpu_compute("k2", 10), TaskRequirements::gpu(10));
        let tasks = vec![task1.clone(), task2.clone()];

        let report = WorkloadScheduler::matchmake(&config, &tasks, &workers);

        assert_eq!(report.assignments.len(), 2);
        // Both tasks must go to real GPU workers, not simulated
        let assigned_ids: Vec<Uuid> = report.assignments.iter().map(|a| a.worker_id).collect();
        assert!(assigned_ids.contains(&w_real1.worker_id));
        assert!(assigned_ids.contains(&w_real2.worker_id));
        assert!(!assigned_ids.contains(&w_sim.worker_id));
    }
}
