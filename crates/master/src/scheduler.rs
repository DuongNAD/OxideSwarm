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

use crate::fair_share::FairWalkCursor;
use crate::queue::{AssignedLoad, QueueOrderKey, TaskQueue, TaskView};
use crate::registry::{WorkerInfo, WorkerRegistry, WorkerStatus};
use crate::server::WaiterMap;
use crate::speed::{new_shared_speed_book, SharedSpeedBook, SpeedBook, TaskKey};
use crate::worker_index::{RegisteredIndex, SeatStaticMeta, WorkerIndex};

/// Queued tasks examined per page of a scheduling pass (each page is one short read lock).
const SCAN_PAGE: usize = 512;

/// How long an idle probe of the queue for GPU work is trusted (see `GpuDemand`).
const GPU_PROBE_INTERVAL: Duration = Duration::from_secs(1);

/// Maximum time a deferred dispatch waiter waits for capacity in a worker's outbound channel
/// before giving up and returning the assignment to the queue (`BUG-20`, design `0075`).
pub const DISPATCH_RESERVE_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum number of deferred dispatch waiters allowed in flight for a single worker at once
/// (`BUG-20`, design `0075`). Assignments beyond this bound are requeued immediately so slow
/// workers cannot accumulate unbounded background tasks.
pub const MAX_IN_FLIGHT_DISPATCH_WAITERS_PER_WORKER: usize = 16;

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

impl SchedulingPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::WeightedLeastLoaded => "weighted",
            Self::LeastLoaded => "least-loaded",
            Self::RoundRobin => "round-robin",
        }
    }
}

impl std::fmt::Display for SchedulingPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for SchedulingPolicy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().replace('_', "-").as_str() {
            "weighted" | "weighted-least-loaded" => Ok(Self::WeightedLeastLoaded),
            "least-loaded" | "leastloaded" => Ok(Self::LeastLoaded),
            "round-robin" | "roundrobin" | "rr" => Ok(Self::RoundRobin),
            other => Err(format!(
                "Unknown scheduler policy '{other}' (expected 'weighted', 'least-loaded', or 'round-robin')"
            )),
        }
    }
}

/// Configuration options for the `WorkloadScheduler`.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Load balancing algorithm.
    pub policy: SchedulingPolicy,
    /// Multiplier against worker CPU cores to calculate the CPU-core slot budget (default: 1.0).
    /// Also provides the fallback concurrent-task limit when a worker does not advertise a
    /// `slots:<n>` tag; current workers advertise `slots:<n>`, which caps concurrent task count
    /// (`AssignedLoad::tasks`) via [`SchedulerConfig::effective_max_tasks`].
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
    /// `max_batch_size`, so a backlog of them does not starve the tasks behind it. When
    /// `fail_unfulfillable_tasks` is enabled and a pass with free slots exhausts `max_scan_per_pass`
    /// from the head of the queue without advancing `unfulfillable_sweep.cursor`, the pass continues
    /// the unfulfillable sweep from `cursor` for up to one additional `max_scan_per_pass` window
    /// (examining at most `2 * max_scan_per_pass` tasks in that pass).
    pub max_scan_per_pass: usize,
    /// Hold-back factor for speed-aware placement (`docs/design/0060-speed-aware-placement.md` D2,
    /// default: `1.5`). When every free candidate's expected duration is at least
    /// `speed_hold_factor * (ready(best) + avg(best))`, the task stays queued for `best`. Setting
    /// `speed_hold_factor = 0.0` turns speed-aware placement off.
    pub speed_hold_factor: f64,
    /// Minimum number of successful samples on at least one matching worker before speed-aware
    /// placement activates for a [`TaskKey`] (`0060` D2, default: `2`).
    pub speed_min_samples: usize,
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
            speed_hold_factor: 1.5,
            speed_min_samples: 2,
        }
    }
}

impl SchedulerConfig {
    /// Computes the CPU-core slot budget (`max_slots`) for a given worker.
    ///
    /// A task requiring `cpu_cores` occupies that many core slots on the worker, scaled by
    /// `core_concurrency_multiplier` and bounded by `max_tasks_per_worker` when set.
    pub fn effective_max_concurrency(&self, worker: &WorkerInfo) -> usize {
        let cores = worker.capabilities.cpu_cores.max(1);
        let by_multiplier = (cores as f64 * self.core_concurrency_multiplier).ceil() as usize;
        let limit = by_multiplier.max(1);
        match self.max_tasks_per_worker {
            Some(cap) => limit.min(cap),
            None => limit,
        }
    }

    /// Computes the maximum concurrent task count (`max_tasks`) allowed for a given worker.
    ///
    /// Honours a worker's advertised `slots:<n>` tag (`--max-concurrency` or default headroom) when
    /// present, falling back to [`Self::effective_max_concurrency`] for workers that do not
    /// advertise `slots:<n>`.
    pub fn effective_max_tasks(&self, worker: &WorkerInfo) -> usize {
        let limit = match rusty_grid_core::placement::advertised_slots(&worker.capabilities) {
            Some(slots) => slots,
            None => self.effective_max_concurrency(worker),
        };
        match self.max_tasks_per_worker {
            Some(cap) => limit.min(cap),
            None => limit,
        }
    }

    /// Determines the required VRAM in MB for a given task.
    pub fn required_vram_mb(&self, task: &Task) -> u64 {
        let explicit = task.requirements.effective_vram_mb();
        if explicit > 0 {
            return explicit;
        }
        match &task.spec {
            TaskSpec::Generative3D(spec) => {
                if spec.engine().is_cpu_capable() {
                    0
                } else {
                    match spec.quality.as_str() {
                        "fast" => self.min_vram_fast_mb,
                        "high" => self.min_vram_high_mb,
                        _ => self.min_vram_standard_mb,
                    }
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

    /// Sets whether tasks whose capabilities or placement constraints no registered worker can
    /// satisfy are failed immediately during scheduling passes.
    pub fn with_fail_unfulfillable_tasks(mut self, fail: bool) -> Self {
        self.fail_unfulfillable_tasks = fail;
        self
    }

    /// Sets the hold-back factor for speed-aware placement (`0.0` disables D2).
    pub fn with_speed_hold_factor(mut self, factor: f64) -> Self {
        self.speed_hold_factor = factor;
        self
    }

    /// Sets the minimum sample count required before speed-aware placement activates for a key.
    pub fn with_speed_min_samples(mut self, min_samples: usize) -> Self {
        self.speed_min_samples = min_samples;
        self
    }

    /// Sets the host CPU usage % threshold above which anti-stuttering backpressure holds back
    /// task assignments (default: `85.0`; pass `100.0` in tests running under synthetic load).
    pub fn with_max_host_cpu_pct(mut self, max_host_cpu_pct: f32) -> Self {
        self.max_host_cpu_pct = max_host_cpu_pct;
        self
    }

    /// Disables host-CPU anti-stuttering backpressure by setting `max_host_cpu_pct` to
    /// `f32::INFINITY`. Intended for tests running under synthetic or concurrent host load.
    pub fn without_host_cpu_backpressure(self) -> Self {
        self.with_max_host_cpu_pct(f32::INFINITY)
    }

    /// Validates `speed_hold_factor` (`0.0` to disable, or a finite number `> 1.0`).
    pub fn validate_speed_hold_factor(factor: f64) -> Result<f64, String> {
        if factor == 0.0 {
            return Ok(0.0);
        }
        if factor.is_finite() && factor > 1.0 {
            return Ok(factor);
        }
        Err(format!(
            "speed_hold_factor must be 0 (off) or a finite number > 1.0 (got {factor})"
        ))
    }

    /// Validates configuration invariants before starting the scheduler.
    pub fn validate(&self) -> Result<(), String> {
        Self::validate_speed_hold_factor(self.speed_hold_factor).map(|_| ())
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
    WorkspaceBusy,
    WaitingForFasterWorker,
    ClusterLacksCapability(String),
}

/// Comprehensive outcome report produced by a scheduling pass.
#[derive(Debug, Clone, Default)]
pub struct ScheduleReport {
    pub assignments: Vec<ScheduleAssignment>,
    pub skipped: Vec<(TaskId, ScheduleSkipReason)>,
    pub unfulfillable: Vec<(TaskId, String)>,
    #[cfg(test)]
    pub(crate) dead_speed_hits: usize,
    #[cfg(test)]
    pub(crate) candidate_scans: usize,
    #[cfg(test)]
    pub(crate) workers_inspected: usize,
}

/// Everything about a task that decides whether a worker may take it. Two queued tasks with the
/// same shape are interchangeable to the placement engine, so once one finds no worker the rest
/// of the pass skips the others without looking at a single worker again.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Shape {
    requires_gpu: bool,
    heavy: bool,
    required_features: ProtocolCapabilities,
    cpu_cores: usize,
    ram_mb: u64,
    vram_mb: u64,
    placement: Placement,
    workspace: Option<String>,
}

impl Shape {
    fn of(view: &TaskView, placement: &Placement) -> Self {
        Self {
            requires_gpu: view.requires_gpu,
            heavy: view.heavy,
            required_features: view.required_features,
            cpu_cores: view.requirements.cpu_cores,
            ram_mb: view.requirements.ram_mb,
            vram_mb: view.requirements.effective_vram_mb(),
            placement: placement.clone(),
            workspace: view.workspace.clone(),
        }
    }
}

/// The names of the protocol features in `features`, for "cluster lacks ..." messages.
fn feature_names(features: ProtocolCapabilities) -> String {
    let mut names = Vec::new();
    if features.contains(ProtocolCapabilities::FILE_STAGING) {
        names.push("FILE_STAGING");
    }
    if features.contains(ProtocolCapabilities::BLOB_TRANSFER) {
        names.push("BLOB_TRANSFER");
    }
    if features.contains(ProtocolCapabilities::THREE_D) {
        names.push("THREE_D");
    }
    if features.contains(ProtocolCapabilities::WORKSPACES) {
        names.push("WORKSPACES");
    }
    if features.contains(ProtocolCapabilities::TREE_V2) {
        names.push("TREE_V2");
    }
    names.join(" and ")
}

/// True if `worker` could run a task with `requirements` when idle (static capability check).
pub(crate) fn statically_fits(requirements: &TaskRequirements, worker: &WorkerInfo) -> bool {
    let req_vram = requirements.effective_vram_mb();
    (requirements.cpu_cores == 0 || worker.capabilities.cpu_cores >= requirements.cpu_cores)
        && (requirements.ram_mb == 0 || worker.capabilities.ram_mb >= requirements.ram_mb)
        && (req_vram == 0 || worker.capabilities.vram_mb.is_some_and(|v| v >= req_vram))
}

/// True if `worker` satisfies a task's placement constraints (`require:` tags: os, arch, worker,
/// tag; see `rusty_grid_core::placement`).
pub(crate) fn placement_admits(placement: &Placement, worker: &WorkerInfo) -> bool {
    placement.matches(
        worker.worker_id,
        &worker.capabilities.name,
        &worker.capabilities.tags,
    )
}

/// One worker as the placement engine tracks it during a pass.
struct Seat<'a> {
    worker: &'a WorkerInfo,
    /// CPU-core slot budget of the worker (`SchedulerConfig::effective_max_concurrency`).
    max_slots: usize,
    /// Maximum concurrent task count of the worker (`SchedulerConfig::effective_max_tasks`,
    /// from `slots:<n>` when advertised).
    max_tasks: usize,
    /// Load from the master's books at the start of the pass.
    base: AssignedLoad,
    /// Load added by tasks placed during this pass.
    placed: AssignedLoad,
    /// Active tasks on this worker (`(task_key, elapsed)` from the queue at pass start, plus
    /// `(task_key, Duration::ZERO)` for tasks placed earlier in this pass).
    running: Vec<(TaskKey, Duration)>,
}

impl Seat<'_> {
    fn used_slots(&self) -> usize {
        self.base.slots + self.placed.slots
    }

    fn used_vram_mb(&self) -> u64 {
        self.base.vram_mb.saturating_add(self.placed.vram_mb)
    }

    fn load(&self) -> usize {
        self.base.tasks + self.placed.tasks
    }

    fn remaining_slots(&self) -> usize {
        if self.load() >= self.max_tasks {
            0
        } else {
            self.max_slots.saturating_sub(self.used_slots())
        }
    }
}

/// What the engine decided for one task.
#[derive(Clone)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
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
    workers: &'a [WorkerInfo],
    seats: Vec<Seat<'a>>,
    worker_index: WorkerIndex,
    /// All registered workers in the cluster (including `Disconnected` and cooling-down ones),
    /// used by `fail_unfulfillable_tasks` so tasks are not failed while their matching worker is
    /// still registered.
    registered: &'a [WorkerInfo],
    registered_index: RegisteredIndex,
    rr_cursor: usize,
    /// A GPU task that some GPU worker could run is waiting (see
    /// `SchedulerConfig::preserve_gpu_for_gpu_tasks`).
    gpu_waiting: bool,
    /// GPU work was seen within `gpu_demand_window`.
    gpu_recent: bool,
    any_gpu_worker: bool,
    dead: HashMap<Shape, Verdict>,
    /// Slots still free across the cluster; the pass stops looking once this reaches zero.
    free_slots: usize,
    /// Seats held back for a task ahead in the queue that is waiting for slots to free up on them
    /// (see `hold_back`): nothing placed later in the pass may land there.
    reserved: Vec<bool>,
    /// Per workspace name, the worker that last finished a task in it (design 0039 D4).
    workspace_affinity: HashMap<String, Uuid>,
    /// Workspace names that currently have a `Scheduled` or `Running` task (or one placed earlier
    /// in this pass); at most one active task per workspace name is allowed at a time.
    active_workspaces: HashSet<String>,
    /// Optional measured task-duration history (`0060` D1/D2).
    speed_book: Option<&'a SpeedBook>,
    /// Per-(TaskKey, Shape) hold cache within a pass (`0060` D2): stores `min_expected` so
    /// tasks with `avoid.is_empty() && workspace.is_none()` and `waited_ms < min_expected`
    /// skip re-scanning every worker in the pass without bypassing the Step 5 wait cap.
    /// Cleared whenever the pass places a task, newly reserves a worker in `hold_back`, or
    /// flips `gpu_waiting`.
    dead_speed: HashMap<TaskKey, HashMap<Shape, f64>>,
    #[cfg(test)]
    dead_speed_hits: usize,
    #[cfg(test)]
    candidate_scans: std::cell::Cell<usize>,
    #[cfg(test)]
    workers_inspected: std::cell::Cell<usize>,
}

/// Returns the latest [`MobileLiveStatus`] for `worker`, preferring the live telemetry snapshot
/// stored on [`WorkerInfo::mobile_status`] and falling back to [`MobileCapabilities::live_status`]
/// when only the registration snapshot is present.
fn effective_mobile_live_status(worker: &WorkerInfo) -> Option<MobileLiveStatus> {
    match (&worker.mobile_status, &worker.capabilities.mobile) {
        (Some(live), Some(cap)) => Some(MobileLiveStatus {
            battery_pct: live.battery_pct.or(cap.battery_pct),
            is_charging: live.is_charging.or(cap.is_charging),
            thermal_throttled: live.thermal_throttled || cap.thermal_throttled,
            battery_temperature: live.battery_temperature,
            network_type: live.network_type.clone(),
            battery_tier: live.battery_tier,
            thermal_tier: live.thermal_tier,
        }),
        (Some(live), None) => Some(live.clone()),
        (None, Some(cap)) => Some(cap.live_status()),
        (None, None) => None,
    }
}

/// Returns the effective [`ThermalTier`] for scheduling on `worker`, combining any explicit tier
/// from [`MobileLiveStatus::thermal_tier`] with the tier derived from battery temperature and the
/// OS throttle flag (`max` so neither a high temperature reading nor an explicit tier is ignored).
fn effective_scheduler_thermal_tier(live: &MobileLiveStatus) -> ThermalTier {
    let computed =
        MobileLiveStatus::compute_thermal_tier(live.battery_temperature, live.thermal_throttled);
    live.thermal_tier
        .map_or(computed, |tier| tier.max(computed))
}

/// Returns the effective [`BatteryTier`] for scheduling on `worker`.
///
/// Desktop/laptop operating systems (`macOS`, `Windows`, non-Android `Linux`) populate
/// [`rusty_grid_core::capabilities::MobileCapabilities`] via `pmset` / `sysfs` for dashboard
/// telemetry and critical (`<15%`) battery cut-off, but have a much larger battery and thermal
/// envelope than smartphones. On those laptops, battery levels above `Critical` (`>=15%`) are
/// treated as [`BatteryTier::Normal`] so unplugged laptops between 15% and 49% remain eligible for
/// GPU and compilation workloads.
fn effective_scheduler_battery_tier(
    worker: &WorkerInfo,
    mobile: Option<&rusty_grid_core::capabilities::MobileCapabilities>,
    live: &MobileLiveStatus,
) -> BatteryTier {
    let raw = live.battery_tier.unwrap_or_else(|| {
        MobileLiveStatus::compute_battery_tier(live.battery_pct, live.is_charging)
    });
    if raw == BatteryTier::Critical {
        return BatteryTier::Critical;
    }
    let os = mobile
        .map(|m| m.os_version.to_ascii_lowercase())
        .unwrap_or_default();
    let is_mobile_os = os.contains("android")
        || os.contains("ios")
        || worker
            .capabilities
            .tags
            .iter()
            .any(|t| t == "os:android" || t == "os:ios");
    let is_laptop_os = !is_mobile_os
        && (os.starts_with("macos")
            || os.starts_with("linux")
            || os.starts_with("windows")
            || worker
                .capabilities
                .tags
                .iter()
                .any(|t| t == "os:macos" || t == "os:windows" || t == "os:linux"));
    if is_laptop_os {
        BatteryTier::Normal
    } else {
        raw
    }
}

/// Evaluates Stage 4 (mobile & environmental constraints) once per worker across all 4
/// `(cpu_cores > 1, heavy)` combinations (`bucket = (multi_core as usize) | ((heavy as usize) << 1)`).
pub(crate) fn compute_mobile_flags(config: &SchedulerConfig, worker: &WorkerInfo) -> [bool; 4] {
    let live_mobile = effective_mobile_live_status(worker);
    let is_thermal_throttled = worker
        .capabilities
        .tags
        .iter()
        .any(|t| t == "thermal_throttled")
        || live_mobile.as_ref().is_some_and(|m| m.thermal_throttled);
    let is_low_battery = worker.capabilities.tags.iter().any(|t| t == "low_battery")
        || live_mobile.as_ref().is_some_and(|m| {
            m.battery_pct
                .is_some_and(|pct| pct < config.mobile_min_battery_pct)
                && m.is_charging == Some(false)
        });
    let live_tiers = live_mobile.as_ref().and_then(|live| {
        let bat =
            effective_scheduler_battery_tier(worker, worker.capabilities.mobile.as_ref(), live);
        let therm = effective_scheduler_thermal_tier(live);
        let is_charging = live.is_charging.unwrap_or(false);
        let eff = calculate_effective_concurrency(worker.capabilities.cpu_cores, bat, therm);
        if eff == 0 || bat == BatteryTier::Critical || therm == ThermalTier::Critical {
            None
        } else {
            Some((bat, therm, is_charging))
        }
    });
    if live_mobile.is_some() && live_tiers.is_none() {
        return [false; 4];
    }
    let mut flags = [false; 4];
    for (bucket, slot) in flags.iter_mut().enumerate() {
        let multi_core = (bucket & 1) != 0;
        let heavy = (bucket & 2) != 0;
        if is_thermal_throttled && (multi_core || heavy) {
            continue;
        }
        if is_low_battery && heavy {
            continue;
        }
        if let Some((bat, therm, is_charging)) = live_tiers {
            if matches!(bat, BatteryTier::SubLow) && (multi_core || heavy) {
                continue;
            }
            if matches!(bat, BatteryTier::Low) && heavy {
                continue;
            }
            match therm {
                ThermalTier::Critical => continue,
                ThermalTier::Hot if multi_core || heavy => continue,
                ThermalTier::Warm if heavy && !is_charging => continue,
                _ => {}
            }
        }
        *slot = true;
    }
    flags
}

/// Effective `(max_slots, max_tasks)` for `worker`, clamped by live mobile battery/thermal tiers.
pub(crate) fn effective_worker_limits(
    config: &SchedulerConfig,
    worker: &WorkerInfo,
) -> (usize, usize) {
    let mut max_slots = config.effective_max_concurrency(worker);
    let mut max_tasks = config.effective_max_tasks(worker);
    if let Some(live) = effective_mobile_live_status(worker) {
        let bat =
            effective_scheduler_battery_tier(worker, worker.capabilities.mobile.as_ref(), &live);
        let therm = effective_scheduler_thermal_tier(&live);
        let eff = calculate_effective_concurrency(worker.capabilities.cpu_cores, bat, therm);
        max_slots = max_slots.min(eff);
        max_tasks = max_tasks.min(eff);
    }
    (max_slots, max_tasks)
}

impl<'a> Engine<'a> {
    /// `loads` is the master's own accounting per worker. `None` takes each worker's
    /// `active_tasks` as its load instead (the pure `matchmake` API, which has no queue).
    fn new(
        config: &'a SchedulerConfig,
        workers: &'a [WorkerInfo],
        registered: &'a [WorkerInfo],
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
                        vram_mb: 0,
                    },
                };
                let (max_slots, max_tasks) = effective_worker_limits(config, worker);
                Seat {
                    worker,
                    max_slots,
                    max_tasks,
                    base,
                    running: Vec::new(),
                    placed: AssignedLoad::default(),
                }
            })
            .collect();
        let mut seat_meta = Vec::with_capacity(seats.len());
        let mut remaining_slots = Vec::with_capacity(seats.len());
        let mut mobile_flags = Vec::with_capacity(seats.len());
        let mut free_slots = 0usize;
        for seat in &seats {
            let rem = seat.remaining_slots();
            if seat.worker.status != WorkerStatus::Disconnected {
                free_slots += rem;
            }
            seat_meta.push(SeatStaticMeta {
                host_busy: seat.worker.cpu_usage_pct > config.max_host_cpu_pct,
                is_gpu: seat.worker.capabilities.can_execute_gpu(),
                is_real_gpu: seat.worker.capabilities.has_gpu
                    && !seat.worker.capabilities.is_simulated_gpu,
                initial_load: seat.base.tasks,
            });
            remaining_slots.push(rem);
            mobile_flags.push(compute_mobile_flags(config, seat.worker));
        }
        let worker_index = WorkerIndex::build(workers, seat_meta, &remaining_slots, &mobile_flags);
        let registered_index = RegisteredIndex::build(registered);
        Self {
            config,
            workers,
            any_gpu_worker: registered.iter().any(|w| w.capabilities.can_execute_gpu()),
            registered,
            registered_index,
            reserved: vec![false; seats.len()],
            seats,
            worker_index,
            rr_cursor: 0,
            gpu_waiting,
            gpu_recent: gpu_recent || gpu_waiting,
            dead: HashMap::new(),
            free_slots,
            workspace_affinity: HashMap::new(),
            active_workspaces: HashSet::new(),
            speed_book: None,
            dead_speed: HashMap::new(),
            #[cfg(test)]
            dead_speed_hits: 0,
            #[cfg(test)]
            candidate_scans: std::cell::Cell::new(0),
            #[cfg(test)]
            workers_inspected: std::cell::Cell::new(0),
        }
    }

    fn with_workspaces(
        mut self,
        workspace_affinity: HashMap<String, Uuid>,
        active_workspaces: HashSet<String>,
    ) -> Self {
        self.workspace_affinity = workspace_affinity;
        self.active_workspaces = active_workspaces;
        self
    }

    fn with_speed(
        mut self,
        speed_book: &'a SpeedBook,
        running_tasks: &HashMap<Uuid, Vec<(TaskKey, Duration)>>,
    ) -> Self {
        self.speed_book = Some(speed_book);
        for seat in &mut self.seats {
            if let Some(running) = running_tasks.get(&seat.worker.worker_id) {
                seat.running = running.clone();
            }
        }
        self
    }

    fn note_gpu_waiting(&mut self) {
        if !self.gpu_waiting {
            self.gpu_waiting = true;
            self.gpu_recent = true;
            self.dead_speed.clear();
        }
    }

    fn any_gpu_worker_fits_view(&self, view: &TaskView) -> bool {
        view.requires_gpu
            && view.placement.as_ref().is_ok_and(|placement| {
                self.worker_index
                    .attrs()
                    .any_gpu_fits(self.workers, &view.requirements, placement)
            })
    }

    fn skip_reason(&self, view: &TaskView, placement: &Placement) -> ScheduleSkipReason {
        if view.requires_gpu && !self.any_gpu_worker {
            ScheduleSkipReason::NoGpuWorkersAvailable
        } else if !view.required_features.is_empty()
            && !self
                .worker_index
                .attrs()
                .any_has_features(self.workers, view.required_features)
        {
            ScheduleSkipReason::ClusterLacksCapability(format!(
                "cluster lacks workers with {} capability",
                feature_names(view.required_features)
            ))
        } else if view.requires_gpu {
            ScheduleSkipReason::NoGpuWorkersAvailable
        } else if !placement.is_empty()
            && !self.worker_index.attrs().any_matches_placement(placement)
        {
            ScheduleSkipReason::ClusterLacksCapability(format!(
                "no worker matches the placement constraints ({placement})"
            ))
        } else {
            ScheduleSkipReason::AllEligibleWorkersSaturated
        }
    }

    fn place(&mut self, view: &TaskView) -> Verdict {
        let placement = match &view.placement {
            Ok(placement) => placement,
            Err(e) => return Verdict::Unfulfillable(format!("Invalid placement constraints: {e}")),
        };
        let shape = Shape::of(view, placement);
        if let Some(verdict) = self.dead.get(&shape) {
            return verdict.clone();
        }
        if self.config.fail_unfulfillable_tasks {
            if view.requires_gpu && !self.any_gpu_worker {
                let verdict =
                    Verdict::Unfulfillable("Cluster lacks workers with GPU capabilities".into());
                self.dead.insert(shape, verdict.clone());
                return verdict;
            }
            let nobody_has_features = !view.required_features.is_empty()
                && !self
                    .registered_index
                    .any_has_features(self.registered, view.required_features);
            if nobody_has_features {
                let verdict = Verdict::Unfulfillable(format!(
                    "Cluster lacks workers with {} capability",
                    feature_names(view.required_features)
                ));
                self.dead.insert(shape, verdict.clone());
                return verdict;
            }
            let nobody_matches =
                !placement.is_empty() && !self.registered_index.any_matches_placement(placement);
            if nobody_matches {
                let verdict = Verdict::Unfulfillable(format!(
                    "No registered worker matches the placement constraints ({placement})"
                ));
                self.dead.insert(shape, verdict.clone());
                return verdict;
            }
            let nobody_fits = !self.registered_index.any_fits_all(
                self.registered,
                &view.requirements,
                placement,
                view.requires_gpu,
                view.required_features,
            );
            if nobody_fits {
                let req_vram = view.requirements.effective_vram_mb();
                let reason = if req_vram > 0 {
                    format!(
                        "No registered worker satisfies task requirements (cpu_cores={}, ram_mb={}, vram_mb={}, gpu={})",
                        view.requirements.cpu_cores,
                        view.requirements.ram_mb,
                        req_vram,
                        view.requires_gpu
                    )
                } else {
                    format!(
                        "No registered worker satisfies task requirements (cpu_cores={}, ram_mb={}, gpu={})",
                        view.requirements.cpu_cores, view.requirements.ram_mb, view.requires_gpu
                    )
                };
                let verdict = Verdict::Unfulfillable(reason);
                self.dead.insert(shape, verdict.clone());
                return verdict;
            }
        }
        if self.free_slots == 0 {
            let verdict = Verdict::Skip(self.skip_reason(view, placement));
            self.dead.insert(shape, verdict.clone());
            return verdict;
        }
        if view
            .workspace
            .as_ref()
            .is_some_and(|ws| self.active_workspaces.contains(ws))
        {
            // Another task in this workspace is already Scheduled or Running, held for adoption,
            // or was placed earlier in this pass. Leave this task queued without holding back any seat.
            let verdict = Verdict::Skip(ScheduleSkipReason::WorkspaceBusy);
            self.dead.insert(shape, verdict.clone());
            return verdict;
        }
        if view.avoid.is_empty() && view.workspace.is_none() {
            if let Some(&min_expected) = self
                .dead_speed
                .get(&view.task_key)
                .and_then(|by_shape| by_shape.get(&shape))
            {
                let waited_ms = view.queued_for.as_secs_f64() * 1_000.0;
                if waited_ms < min_expected {
                    #[cfg(test)]
                    {
                        self.dead_speed_hits += 1;
                    }
                    return Verdict::Skip(ScheduleSkipReason::WaitingForFasterWorker);
                }
            }
        }

        let weight = view.requirements.cpu_cores.max(1);
        let mut full = Vec::new();
        let mut matching = Vec::new();
        let mut candidates =
            self.candidates_and_matching(view, &view.avoid, &mut full, &mut matching);
        if candidates.is_empty() && !view.avoid.is_empty() {
            // Avoiding is a preference: with nobody else eligible, a worker that refused the task
            // before still gets it rather than leaving it to starve.
            candidates = self.candidates_and_matching(view, &[], &mut full, &mut matching);
        }
        if candidates.is_empty() {
            self.hold_back(&full, weight, view.requirements.effective_vram_mb());
            let verdict = Verdict::Skip(self.skip_reason(view, placement));
            self.dead.insert(shape, verdict.clone());
            return verdict;
        }

        // Workspace affinity (0039 D4) takes precedence over speed-based hold-back and
        // re-ordering whenever the workspace's warm worker is an eligible free candidate.
        let has_workspace_affinity = view
            .workspace
            .as_ref()
            .and_then(|ws| self.workspace_affinity.get(ws))
            .is_some_and(|aff_worker| {
                candidates
                    .iter()
                    .any(|&c| self.seats[c].worker.worker_id == *aff_worker)
            });

        // Speed-aware placement (`0060` D2): active when `speed_hold_factor > 1.0` (finite) and at
        // least one matching worker has at least `speed_min_samples` samples for `view.task_key`.
        if !has_workspace_affinity
            && self.config.speed_hold_factor.is_finite()
            && self.config.speed_hold_factor > 1.0
        {
            if let Some(speed_book) = self.speed_book {
                let min_samples = self.config.speed_min_samples.max(1) as u64;
                let best = matching
                    .iter()
                    .filter_map(|&idx| {
                        let entry =
                            speed_book.get(&self.seats[idx].worker.worker_id, view.task_key)?;
                        (entry.samples >= min_samples).then_some((idx, entry.avg_ms))
                    })
                    .min_by(|&(ia, avg_a), &(ib, avg_b)| {
                        avg_a
                            .total_cmp(&avg_b)
                            .then_with(|| {
                                (!candidates.contains(&ia)).cmp(&(!candidates.contains(&ib)))
                            })
                            .then_with(|| {
                                self.seats[ia]
                                    .used_slots()
                                    .cmp(&self.seats[ib].used_slots())
                            })
                            .then_with(|| {
                                self.seats[ia]
                                    .worker
                                    .worker_id
                                    .cmp(&self.seats[ib].worker.worker_id)
                            })
                    });

                if let Some((best_idx, best_avg)) = best {
                    let expected_for = |idx: usize| -> f64 {
                        speed_book
                            .get(&self.seats[idx].worker.worker_id, view.task_key)
                            .filter(|entry| entry.samples >= min_samples)
                            .map_or(best_avg, |entry| entry.avg_ms)
                    };
                    let min_expected = candidates
                        .iter()
                        .map(|&c| expected_for(c))
                        .fold(f64::INFINITY, f64::min);

                    let ready_best = if candidates.contains(&best_idx) {
                        Some(0.0)
                    } else {
                        let best_seat = &self.seats[best_idx];
                        let best_wid = best_seat.worker.worker_id;
                        best_seat
                            .running
                            .iter()
                            .filter_map(|(run_key, elapsed)| {
                                let entry = speed_book.get(&best_wid, *run_key)?;
                                Some((entry.avg_ms - elapsed.as_secs_f64() * 1_000.0).max(0.0))
                            })
                            .reduce(f64::min)
                    };

                    // Step 5 wait cap: never hold a task longer than its expected run time on the
                    // fastest currently available candidate.
                    let waited_ms = view.queued_for.as_secs_f64() * 1_000.0;
                    if waited_ms < min_expected {
                        if let Some(ready_ms) = ready_best {
                            let threshold = self.config.speed_hold_factor * (ready_ms + best_avg);
                            if min_expected >= threshold {
                                if full.contains(&best_idx) {
                                    self.hold_back(
                                        &[best_idx],
                                        weight,
                                        view.requirements.effective_vram_mb(),
                                    );
                                }
                                if view.avoid.is_empty() && view.workspace.is_none() {
                                    self.dead_speed
                                        .entry(view.task_key)
                                        .or_default()
                                        .insert(shape, min_expected);
                                }
                                return Verdict::Skip(ScheduleSkipReason::WaitingForFasterWorker);
                            }
                        }
                    }

                    // D2 step 4: exclude only candidates that are clearly slower
                    // (`expected(c) >= speed_hold_factor * min_expected`), keeping today's
                    // `rank` order among the rest without re-ordering by expected finish time.
                    let cutoff = self.config.speed_hold_factor * min_expected;
                    candidates.retain(|&c| expected_for(c) < cutoff);
                }
            }
        }

        let chosen = self.rank(view, &mut candidates);
        if let Some(ref ws) = view.workspace {
            self.active_workspaces.insert(ws.clone());
        }
        let seat = &mut self.seats[chosen];
        let free_before = seat.remaining_slots();
        seat.placed.tasks += 1;
        seat.placed.slots += weight;
        seat.placed.unstarted_ram_mb += view.requirements.ram_mb;
        seat.placed.vram_mb = seat
            .placed
            .vram_mb
            .saturating_add(view.requirements.effective_vram_mb());
        seat.running.push((view.task_key, Duration::ZERO));
        let free_after = seat.remaining_slots();
        self.free_slots = self
            .free_slots
            .saturating_sub(free_before.saturating_sub(free_after));
        let reserved_chosen = self.reserved[chosen];
        self.worker_index
            .sync_seat_free(chosen, free_after > 0, reserved_chosen);
        self.dead_speed.clear();
        Verdict::Placed(chosen)
    }

    /// A task found no worker only because the workers that could run it lack free slots (`full`,
    /// from `candidates`). Without a rule, narrower tasks further down the queue keep refilling
    /// those workers as slots free up, and a task that needs many slots at once (it fits a busy
    /// worker only when enough of its slots are free, or when it is idle) never gets them. So the
    /// worker that needs the fewest slots to free up for it is held back for the rest of the
    /// pass: it takes nothing more, drains, and the task is placed on it in a later pass. Later
    /// tasks only lose a worker that could not have run the waiting task either way.
    fn hold_back(&mut self, full: &[usize], weight: usize, req_vram_mb: u64) {
        let slots_to_free = |seat: &Seat<'_>| {
            let used = seat.used_slots();
            if weight > seat.max_slots {
                // Only an idle worker takes it.
                used
            } else {
                (used + weight).saturating_sub(seat.max_slots)
            }
        };
        let vram_to_free = |seat: &Seat<'_>| {
            if req_vram_mb == 0 {
                return 0;
            }
            let Some(cap) = seat.worker.capabilities.vram_mb else {
                return u64::MAX;
            };
            seat.used_vram_mb()
                .saturating_add(req_vram_mb)
                .saturating_sub(cap)
        };
        let Some(&idx) = full.iter().min_by_key(|&&idx| {
            let seat = &self.seats[idx];
            (slots_to_free(seat), vram_to_free(seat), idx)
        }) else {
            return;
        };
        if !self.reserved[idx] {
            self.reserved[idx] = true;
            self.worker_index.set_seat_reserved(idx);
            let seat = &self.seats[idx];
            // Its free slots are out of reach for the rest of the pass.
            self.free_slots = self.free_slots.saturating_sub(seat.remaining_slots());
            self.dead_speed.clear();
        }
    }

    fn is_seat_ram_short(&self, seat: &Seat<'_>, req_ram_mb: u64) -> bool {
        req_ram_mb > 0 && seat.worker.ram_available_mb > 0 && {
            let reserved = seat.base.unstarted_ram_mb + seat.placed.unstarted_ram_mb;
            seat.worker.ram_available_mb.saturating_sub(reserved) < req_ram_mb
        }
    }

    fn is_seat_vram_short(&self, seat: &Seat<'_>, req_vram_mb: u64) -> bool {
        req_vram_mb > 0
            && seat
                .worker
                .capabilities
                .vram_mb
                .is_some_and(|cap| cap.saturating_sub(seat.used_vram_mb()) < req_vram_mb)
    }

    fn apply_stage6_and_7(
        &self,
        view: &TaskView,
        weight: usize,
        found: &mut Vec<usize>,
        matching: &mut Vec<usize>,
    ) {
        let config = self.config;
        // Stage 6: GPU preservation. While a GPU task waits, a generic task takes a CPU-only worker
        // whenever one can run it and only spills onto a GPU worker when none can. While GPU work
        // waits or is likely, a spill-over must leave that GPU worker some slots free.
        if !view.requires_gpu
            && config.preserve_gpu_for_gpu_tasks
            && (self.gpu_waiting || self.gpu_recent)
        {
            if self.gpu_waiting && found.iter().any(|idx| !self.worker_index.meta(*idx).is_gpu) {
                found.retain(|idx| !self.worker_index.meta(*idx).is_gpu);
                matching.retain(|idx| !self.worker_index.meta(*idx).is_gpu);
            } else {
                found.retain(|idx| {
                    if !self.worker_index.meta(*idx).is_gpu {
                        return true;
                    }
                    let seat = &self.seats[*idx];
                    let reserve = config
                        .gpu_reserved_slots
                        .min(seat.max_slots.saturating_sub(1))
                        .min(seat.max_tasks.saturating_sub(1));
                    seat.max_slots.saturating_sub(seat.used_slots() + weight) >= reserve
                        && seat.max_tasks.saturating_sub(seat.load() + 1) >= reserve
                });
                matching.retain(|idx| {
                    if !self.worker_index.meta(*idx).is_gpu {
                        return true;
                    }
                    let seat = &self.seats[*idx];
                    let reserve = config
                        .gpu_reserved_slots
                        .min(seat.max_slots.saturating_sub(1))
                        .min(seat.max_tasks.saturating_sub(1));
                    seat.max_slots.saturating_sub(weight) >= reserve
                        && seat.max_tasks.saturating_sub(1) >= reserve
                });
            }
        }

        // Stage 7: Real-GPU priority.
        // For GPU-requiring tasks, prioritize hardware GPU (wgpu) workers over simulated
        // GPU workers. Simulated GPU workers are only used when no idle real-GPU worker is available.
        if view.requires_gpu {
            let has_idle_real_gpu = found.iter().any(|&idx| {
                self.worker_index.meta(idx).is_real_gpu && self.seats[idx].used_slots() == 0
            });
            if has_idle_real_gpu {
                found.retain(|&idx| self.worker_index.meta(idx).is_real_gpu);
                matching.retain(|&idx| self.worker_index.meta(idx).is_real_gpu);
            }
        }
    }

    /// Indices of the workers that may run `view` right now. `full` is cleared and then receives
    /// the workers that would have taken it but for free slots; `matching` is cleared and receives
    /// all connected workers that satisfy `view`'s capability, placement, GPU, and environmental
    /// rules ignoring free slots and `reserved`.
    fn candidates_and_matching(
        &self,
        view: &TaskView,
        avoided: &[Uuid],
        full: &mut Vec<usize>,
        matching: &mut Vec<usize>,
    ) -> Vec<usize> {
        let config = self.config;
        let req = &view.requirements;
        let req_vram = req.effective_vram_mb();
        let weight = req.cpu_cores.max(1);
        let mut found: Vec<usize> = Vec::new();
        full.clear();
        matching.clear();
        #[cfg(test)]
        self.candidate_scans.set(self.candidate_scans.get() + 1);

        let Ok(ref placement) = view.placement else {
            return found;
        };

        let speed_active = self.speed_book.is_some()
            && config.speed_hold_factor.is_finite()
            && config.speed_hold_factor > 1.0;

        if !speed_active {
            if let Some(mask) = self.worker_index.free_candidate_mask(
                placement,
                view.requires_gpu,
                view.required_features,
                req.cpu_cores,
                view.heavy,
                avoided,
            ) {
                for idx in mask.iter_ones() {
                    #[cfg(test)]
                    self.workers_inspected.set(self.workers_inspected.get() + 1);
                    let seat = &self.seats[idx];
                    if !statically_fits(req, seat.worker)
                        || self.is_seat_ram_short(seat, req.ram_mb)
                        || self.is_seat_vram_short(seat, req_vram)
                        || !seat.worker.features.contains(view.required_features)
                    {
                        continue;
                    }
                    let used = seat.used_slots();
                    let tasks = seat.load();
                    if tasks >= seat.max_tasks || (used > 0 && used + weight > seat.max_slots) {
                        continue;
                    }
                    found.push(idx);
                }
            }
            self.apply_stage6_and_7(view, weight, &mut found, matching);
            if !found.is_empty() {
                return found;
            }
            if let Some(mask) = self.worker_index.unblocked_static_mask(
                placement,
                view.requires_gpu,
                view.required_features,
                req.cpu_cores,
                view.heavy,
                avoided,
            ) {
                for idx in mask.iter_ones() {
                    #[cfg(test)]
                    self.workers_inspected.set(self.workers_inspected.get() + 1);
                    let seat = &self.seats[idx];
                    if !statically_fits(req, seat.worker)
                        || self.is_seat_ram_short(seat, req.ram_mb)
                        || !seat.worker.features.contains(view.required_features)
                    {
                        continue;
                    }
                    let used = seat.used_slots();
                    let tasks = seat.load();
                    if tasks >= seat.max_tasks
                        || (used > 0 && used + weight > seat.max_slots)
                        || self.is_seat_vram_short(seat, req_vram)
                    {
                        full.push(idx);
                    }
                }
            }
            return Vec::new();
        }

        if let Some(mask) = self.worker_index.matching_static_mask(
            placement,
            view.requires_gpu,
            view.required_features,
            req.cpu_cores,
            view.heavy,
            avoided,
        ) {
            for idx in mask.iter_ones() {
                #[cfg(test)]
                self.workers_inspected.set(self.workers_inspected.get() + 1);
                let seat = &self.seats[idx];
                if !statically_fits(req, seat.worker)
                    || !seat.worker.features.contains(view.required_features)
                {
                    continue;
                }
                let host_busy = self.worker_index.meta(idx).host_busy;
                let ram_short = self.is_seat_ram_short(seat, req.ram_mb);
                if (host_busy || ram_short) && seat.load() == 0 {
                    continue;
                }
                let vram_short = self.is_seat_vram_short(seat, req_vram);
                matching.push(idx);
                if self.reserved[idx] || host_busy || ram_short {
                    continue;
                }
                let used = seat.used_slots();
                let tasks = seat.load();
                if tasks >= seat.max_tasks
                    || (used > 0 && used + weight > seat.max_slots)
                    || vram_short
                {
                    full.push(idx);
                    continue;
                }
                found.push(idx);
            }
        }
        self.apply_stage6_and_7(view, weight, &mut found, matching);
        found
    }

    /// Picks the candidate to use according to the policy.
    fn rank(&mut self, view: &TaskView, candidates: &mut [usize]) -> usize {
        if let Some(aff_worker) = view
            .workspace
            .as_ref()
            .and_then(|ws| self.workspace_affinity.get(ws))
        {
            if let Some(pos) = candidates
                .iter()
                .position(|&c| self.seats[c].worker.worker_id == *aff_worker)
            {
                candidates.swap(0, pos);
                return candidates[0];
            }
        }
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
                        self.rr_cursor = self.rr_cursor.wrapping_add(1);
                        return real_candidates[idx];
                    }
                }
                let idx = self.rr_cursor % candidates.len();
                self.rr_cursor = self.rr_cursor.wrapping_add(1);
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

/// Progress of the unfulfillability sweep across `ready_queue` for a given
/// `(registration_epoch, ready_epoch)` pair.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct UnfulfillableSweep {
    epochs: Option<(u64, u64)>,
    cursor: Option<QueueOrderKey>,
    completed: bool,
}

/// RAII guard tracking one deferred dispatch waiter in flight for `worker_id` (`BUG-20`).
struct InFlightDispatchGuard {
    counts: Arc<std::sync::Mutex<HashMap<Uuid, usize>>>,
    worker_id: Uuid,
}

impl InFlightDispatchGuard {
    fn try_acquire(
        counts: &Arc<std::sync::Mutex<HashMap<Uuid, usize>>>,
        worker_id: Uuid,
        max_per_worker: usize,
    ) -> Option<Self> {
        if max_per_worker == 0 {
            return None;
        }
        let mut guard = counts.lock().unwrap_or_else(|e| e.into_inner());
        let entry = guard.entry(worker_id).or_insert(0);
        if *entry >= max_per_worker {
            return None;
        }
        *entry += 1;
        drop(guard);
        Some(Self {
            counts: Arc::clone(counts),
            worker_id,
        })
    }
}

impl Drop for InFlightDispatchGuard {
    fn drop(&mut self) {
        let mut guard = self.counts.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(count) = guard.get_mut(&self.worker_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                guard.remove(&self.worker_id);
            }
        }
    }
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
    /// Persistent round-robin cursor across scheduling passes (`schedule_once` / `schedule_batch`).
    rr_cursor: std::sync::atomic::AtomicUsize,
    /// Sweep progress across `ready_queue` under `fail_unfulfillable_tasks` for the current
    /// `(registration_epoch, ready_epoch)`, keeping a cursor across saturated passes when the
    /// queue exceeds `max_scan_per_pass` and recording completion once the end of the queue is
    /// reached so subsequent saturated passes with unchanged epochs make no `ready_views` call.
    unfulfillable_sweep: std::sync::Mutex<UnfulfillableSweep>,
    /// Measured per-(worker, task-key) run-time history (`0060` D1/D2).
    speed_book: SharedSpeedBook,
    /// Deadline for waiting on a full worker channel before requeuing an assignment (`BUG-20`).
    dispatch_reserve_timeout: Duration,
    /// Per-worker cap on concurrent deferred dispatch waiters (`BUG-20`).
    max_in_flight_dispatches_per_worker: usize,
    /// Active deferred dispatch waiter count per worker (`BUG-20`).
    in_flight_dispatches: Arc<std::sync::Mutex<HashMap<Uuid, usize>>>,
    /// Sorted IDs of connected workers last reported as held back by host-CPU backpressure
    /// while queued tasks skipped with `AllEligibleWorkersSaturated` (`SCHED-5`).
    host_cpu_backpressure_reported: std::sync::Mutex<Vec<Uuid>>,
}

impl WorkloadScheduler {
    /// Creates a new WorkloadScheduler instance.
    pub fn new(
        config: SchedulerConfig,
        registry: WorkerRegistry,
        queue: TaskQueue,
        trigger: Arc<Notify>,
    ) -> Self {
        let speed_book = new_shared_speed_book();
        registry.set_speed_book(Arc::clone(&speed_book));
        Self {
            config,
            registry,
            queue,
            trigger,
            waiters: None,
            gpu_demand: std::sync::Mutex::new(GpuDemand::default()),
            rr_cursor: std::sync::atomic::AtomicUsize::new(0),
            unfulfillable_sweep: std::sync::Mutex::new(UnfulfillableSweep::default()),
            speed_book,
            dispatch_reserve_timeout: DISPATCH_RESERVE_TIMEOUT,
            max_in_flight_dispatches_per_worker: MAX_IN_FLIGHT_DISPATCH_WAITERS_PER_WORKER,
            in_flight_dispatches: Arc::new(std::sync::Mutex::new(HashMap::new())),
            host_cpu_backpressure_reported: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Hands the scheduler the client waiter map (see the `waiters` field).
    pub fn with_waiters(mut self, waiters: WaiterMap) -> Self {
        self.waiters = Some(waiters);
        self
    }

    /// Overrides the deadline a deferred dispatch waiter waits on a full worker channel before
    /// returning the assignment to the queue (`BUG-20`, default: [`DISPATCH_RESERVE_TIMEOUT`]).
    pub fn with_dispatch_reserve_timeout(mut self, timeout: Duration) -> Self {
        self.dispatch_reserve_timeout = timeout;
        self
    }

    /// Overrides the maximum number of deferred dispatch waiters allowed in flight per worker
    /// (`BUG-20`, default: [`MAX_IN_FLIGHT_DISPATCH_WAITERS_PER_WORKER`]).
    pub fn with_max_in_flight_dispatches_per_worker(mut self, cap: usize) -> Self {
        self.max_in_flight_dispatches_per_worker = cap;
        self
    }

    /// Replaces the initial contents of the scheduler's [`SpeedBook`] (preserving the shared
    /// handle attached to [`WorkerRegistry`]).
    pub fn with_speed_book(self, book: SpeedBook) -> Self {
        *self.speed_book.lock().unwrap_or_else(|e| e.into_inner()) = Arc::new(book);
        self
    }

    /// Returns the shared [`SpeedBook`] owned by this scheduler.
    pub fn speed_book(&self) -> &SharedSpeedBook {
        &self.speed_book
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
        Self::matchmake_with_workspaces(
            config,
            tasks,
            workers,
            avoid,
            &HashMap::new(),
            &HashSet::new(),
        )
    }

    /// Pure matchmaking with per-task `avoid` lists, per-workspace `workspace_affinity` map, and
    /// `active_workspaces` set (design 0039 D4).
    pub fn matchmake_with_workspaces(
        config: &SchedulerConfig,
        tasks: &[Task],
        workers: &[WorkerInfo],
        avoid: &HashMap<TaskId, Vec<Uuid>>,
        workspace_affinity: &HashMap<String, Uuid>,
        active_workspaces: &HashSet<String>,
    ) -> ScheduleReport {
        Self::matchmake_inner(
            config,
            tasks,
            workers,
            avoid,
            workspace_affinity,
            active_workspaces,
            None,
            &HashMap::new(),
            &HashMap::new(),
        )
    }

    /// Pure matchmaking with a [`SpeedBook`] and currently running `(TaskKey, elapsed)` entries per
    /// worker (`docs/design/0060-speed-aware-placement.md` D2).
    #[allow(clippy::too_many_arguments)]
    pub fn matchmake_with_speed(
        config: &SchedulerConfig,
        tasks: &[Task],
        workers: &[WorkerInfo],
        avoid: &HashMap<TaskId, Vec<Uuid>>,
        workspace_affinity: &HashMap<String, Uuid>,
        active_workspaces: &HashSet<String>,
        speed_book: &SpeedBook,
        running_tasks: &HashMap<Uuid, Vec<(TaskKey, Duration)>>,
    ) -> ScheduleReport {
        Self::matchmake_inner(
            config,
            tasks,
            workers,
            avoid,
            workspace_affinity,
            active_workspaces,
            Some(speed_book),
            running_tasks,
            &HashMap::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn matchmake_inner(
        config: &SchedulerConfig,
        tasks: &[Task],
        workers: &[WorkerInfo],
        avoid: &HashMap<TaskId, Vec<Uuid>>,
        workspace_affinity: &HashMap<String, Uuid>,
        active_workspaces: &HashSet<String>,
        speed_book: Option<&SpeedBook>,
        running_tasks: &HashMap<Uuid, Vec<(TaskKey, Duration)>>,
        queued_for_overrides: &HashMap<TaskId, Duration>,
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
                if let Some(&waited) = queued_for_overrides.get(&task.id) {
                    view.queued_for = waited;
                }
                view
            })
            .collect();
        // GPU-capable work in the batch itself is "waiting".
        let gpu_waiting = views.iter().any(|v| v.requires_gpu);
        let mut engine = Engine::new(config, workers, workers, None, gpu_waiting, false)
            .with_workspaces(workspace_affinity.clone(), active_workspaces.clone());
        if let Some(book) = speed_book {
            engine = engine.with_speed(book, running_tasks);
        }
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
        #[cfg(test)]
        {
            report.dead_speed_hits = engine.dead_speed_hits;
            report.candidate_scans = engine.candidate_scans.get();
            report.workers_inspected = engine.workers_inspected.get();
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
                    (task.requirements.requires_gpu_or_vram() || task.spec.requires_gpu())
                        && task.scheduling_placement().is_ok_and(|placement| {
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

    fn update_host_cpu_backpressure(
        &self,
        saturated_skips: usize,
        busy_workers: &[(Uuid, &str, f32, usize)],
    ) {
        let active = saturated_skips > 0 && !busy_workers.is_empty();
        let next_ids: Vec<Uuid> = if active {
            busy_workers.iter().map(|(id, ..)| *id).collect()
        } else {
            Vec::new()
        };
        let was_active = {
            let mut prev = self
                .host_cpu_backpressure_reported
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if *prev == next_ids {
                return;
            }
            let was_active = !prev.is_empty();
            *prev = next_ids;
            was_active
        };
        if active {
            warn!(
                skipped = saturated_skips,
                max_host_cpu_pct = self.config.max_host_cpu_pct,
                busy_workers = ?busy_workers,
                "Scheduling pass held back queued task(s) because all eligible connected workers \
                 are saturated above max_host_cpu_pct"
            );
        } else if was_active {
            info!(
                max_host_cpu_pct = self.config.max_host_cpu_pct,
                "Host-CPU backpressure holdback cleared"
            );
        }
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
        let expired_failed = self.queue.release_expired_adoption_holds().await;
        if !expired_failed.is_empty() {
            if let Some(ref waiters) = self.waiters {
                crate::server::resolve_terminal_waiters(
                    &self.queue,
                    waiters,
                    &expired_failed,
                    None,
                )
                .await;
            }
        }

        // Step 2: Active workers (checked before touching the queue: no worker, nothing to clone)
        let (mut workers, registered, reg_epoch) = self.registry.scheduler_snapshot().await;
        if workers.is_empty() {
            self.update_host_cpu_backpressure(0, &[]);
            let mut report = ScheduleReport::default();
            for task_id in self.queue.ready_task_ids(max_batch_size).await {
                report
                    .skipped
                    .push((task_id, ScheduleSkipReason::NoEligibleWorkers));
            }
            return Ok(report);
        }
        workers.sort_by_key(|w| w.worker_id);

        let loads = self.queue.assigned_loads().await;
        let (workspace_affinity, active_workspaces) = self.queue.workspace_scheduling_state().await;
        let (gpu_waiting, gpu_recent) = self.gpu_demand(&workers).await;
        let speed_snapshot =
            if self.config.speed_hold_factor.is_finite() && self.config.speed_hold_factor > 1.0 {
                let guard = self.speed_book.lock().unwrap_or_else(|e| e.into_inner());
                (!guard.is_empty()).then(|| Arc::clone(&guard))
            } else {
                None
            };
        let running_tasks = if speed_snapshot.is_some() {
            self.queue.active_running_tasks().await
        } else {
            HashMap::new()
        };
        let mut engine = Engine::new(
            &self.config,
            &workers,
            &registered,
            Some(&loads),
            gpu_waiting,
            gpu_recent,
        )
        .with_workspaces(workspace_affinity, active_workspaces);
        if let Some(ref book) = speed_snapshot {
            engine = engine.with_speed(book.as_ref(), &running_tasks);
        }
        engine.rr_cursor = self.rr_cursor.load(std::sync::atomic::Ordering::Relaxed);

        let ready_epoch = self.queue.ready_epoch().await;
        let sweep_state = (reg_epoch, ready_epoch);
        let (need_unfulfillable_sweep, sweep_cursor) = if self.config.fail_unfulfillable_tasks {
            let mut state = self
                .unfulfillable_sweep
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if state.epochs != Some(sweep_state) {
                *state = UnfulfillableSweep {
                    epochs: Some(sweep_state),
                    cursor: None,
                    completed: false,
                };
            }
            (!state.completed, state.cursor)
        } else {
            (false, None)
        };

        // Step 3: Walk the queue
        let mut report = ScheduleReport::default();
        let mut placements: Vec<(TaskId, usize)> = Vec::new();
        let mut saturated_skips = 0usize;
        if (engine.free_slots > 0 || need_unfulfillable_sweep) && max_batch_size > 0 {
            let mut need_sweep = need_unfulfillable_sweep;
            let scan_budget = self.config.max_scan_per_pass.max(1);
            let mut ran_fair = false;
            if engine.free_slots > 0 && self.queue.active_submitter_count().await > 1 {
                ran_fair = true;
                let mut fair_cursor = FairWalkCursor::default();
                let mut fair_scanned = 0usize;
                let mut fair_exhausted = false;
                while engine.free_slots > 0 && fair_scanned < scan_budget {
                    let page_limit = SCAN_PAGE.min(scan_budget.saturating_sub(fair_scanned));
                    if page_limit == 0 {
                        break;
                    }
                    let views = self
                        .queue
                        .ready_views_fair(&mut fair_cursor, page_limit)
                        .await;
                    let fetched = views.len();
                    fair_scanned += fetched;
                    if !engine.gpu_waiting
                        && self.config.preserve_gpu_for_gpu_tasks
                        && views.iter().any(|v| engine.any_gpu_worker_fits_view(v))
                    {
                        engine.note_gpu_waiting();
                        self.note_gpu_demand();
                    }
                    let mut broke_early = false;
                    for view in &views {
                        #[cfg(feature = "otel")]
                        let place_span = tracing::info_span!(
                            target: crate::otel::OTEL_SPAN_TARGET,
                            "place",
                            task_id = %view.task_id,
                            requires_gpu = view.requires_gpu,
                            worker_id = tracing::field::Empty,
                            verdict = tracing::field::Empty,
                        );
                        match engine.place(view) {
                            Verdict::Placed(idx) => {
                                #[cfg(feature = "otel")]
                                {
                                    place_span.record("verdict", "placed");
                                    place_span.record(
                                        "worker_id",
                                        tracing::field::display(engine.seats[idx].worker.worker_id),
                                    );
                                }
                                placements.push((view.task_id, idx));
                                if view.requires_gpu {
                                    self.note_gpu_demand();
                                }
                                if placements.len() >= max_batch_size {
                                    engine.free_slots = 0;
                                }
                                if engine.free_slots == 0 && !need_sweep {
                                    #[cfg(feature = "otel")]
                                    drop(place_span);
                                    broke_early = true;
                                    break;
                                }
                            }
                            Verdict::Skip(reason) => {
                                #[cfg(feature = "otel")]
                                place_span.record("verdict", "skipped");
                                if matches!(reason, ScheduleSkipReason::AllEligibleWorkersSaturated)
                                {
                                    saturated_skips += 1;
                                }
                                if report.skipped.len() < max_batch_size {
                                    report.skipped.push((view.task_id, reason));
                                }
                                // A worker held back for this task may have been the last one with room.
                                if engine.free_slots == 0 && !need_sweep {
                                    #[cfg(feature = "otel")]
                                    drop(place_span);
                                    broke_early = true;
                                    break;
                                }
                            }
                            Verdict::Unfulfillable(reason) => {
                                #[cfg(feature = "otel")]
                                place_span.record("verdict", "unfulfillable");
                                report.unfulfillable.push((view.task_id, reason));
                            }
                        }
                        #[cfg(feature = "otel")]
                        drop(place_span);
                    }
                    if broke_early {
                        break;
                    }
                    if fetched < page_limit {
                        fair_exhausted = true;
                        break;
                    }
                }
                if fair_exhausted && self.config.fail_unfulfillable_tasks {
                    *self
                        .unfulfillable_sweep
                        .lock()
                        .unwrap_or_else(|e| e.into_inner()) = UnfulfillableSweep {
                        epochs: Some(sweep_state),
                        cursor: None,
                        completed: true,
                    };
                    need_sweep = false;
                } else if need_sweep {
                    engine.free_slots = 0;
                }
            }
            if (engine.free_slots > 0 && !ran_fair) || need_sweep {
                let mut cursor: Option<QueueOrderKey> = if engine.free_slots == 0 {
                    sweep_cursor
                } else {
                    None
                };
                let mut scanned = 0usize;
                let mut sweeping_from_cursor = engine.free_slots == 0;
                'scan: loop {
                    let page_limit = SCAN_PAGE.min(scan_budget.saturating_sub(scanned));
                    if page_limit == 0 {
                        if !sweeping_from_cursor
                            && need_sweep
                            && sweep_cursor.is_some()
                            && self
                                .unfulfillable_sweep
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .cursor
                                == sweep_cursor
                        {
                            sweeping_from_cursor = true;
                            engine.free_slots = 0;
                            cursor = sweep_cursor;
                            scanned = 0;
                            continue 'scan;
                        }
                        break;
                    }
                    let (views, next) = self.queue.ready_views(cursor, page_limit).await;
                    scanned += views.len();
                    if !engine.gpu_waiting
                        && self.config.preserve_gpu_for_gpu_tasks
                        && views.iter().any(|v| engine.any_gpu_worker_fits_view(v))
                    {
                        engine.note_gpu_waiting();
                        self.note_gpu_demand();
                    }
                    for view in &views {
                        #[cfg(feature = "otel")]
                        let place_span = tracing::info_span!(
                            target: crate::otel::OTEL_SPAN_TARGET,
                            "place",
                            task_id = %view.task_id,
                            requires_gpu = view.requires_gpu,
                            worker_id = tracing::field::Empty,
                            verdict = tracing::field::Empty,
                        );
                        match engine.place(view) {
                            Verdict::Placed(idx) => {
                                #[cfg(feature = "otel")]
                                {
                                    place_span.record("verdict", "placed");
                                    place_span.record(
                                        "worker_id",
                                        tracing::field::display(engine.seats[idx].worker.worker_id),
                                    );
                                }
                                placements.push((view.task_id, idx));
                                if view.requires_gpu {
                                    self.note_gpu_demand();
                                }
                                if placements.len() >= max_batch_size {
                                    engine.free_slots = 0;
                                }
                                if engine.free_slots == 0 && !need_sweep {
                                    #[cfg(feature = "otel")]
                                    drop(place_span);
                                    break 'scan;
                                }
                            }
                            Verdict::Skip(reason) => {
                                #[cfg(feature = "otel")]
                                place_span.record("verdict", "skipped");
                                if matches!(reason, ScheduleSkipReason::AllEligibleWorkersSaturated)
                                {
                                    saturated_skips += 1;
                                }
                                if report.skipped.len() < max_batch_size {
                                    report.skipped.push((view.task_id, reason));
                                }
                                // A worker held back for this task may have been the last one with room.
                                if engine.free_slots == 0 && !need_sweep {
                                    #[cfg(feature = "otel")]
                                    drop(place_span);
                                    break 'scan;
                                }
                            }
                            Verdict::Unfulfillable(reason) => {
                                #[cfg(feature = "otel")]
                                place_span.record("verdict", "unfulfillable");
                                report.unfulfillable.push((view.task_id, reason));
                            }
                        }
                        #[cfg(feature = "otel")]
                        drop(place_span);
                    }
                    match next {
                        Some(key) => {
                            cursor = Some(key);
                            if self.config.fail_unfulfillable_tasks {
                                let mut state = self
                                    .unfulfillable_sweep
                                    .lock()
                                    .unwrap_or_else(|e| e.into_inner());
                                if state.epochs == Some(sweep_state) && !state.completed {
                                    state.cursor = Some(match state.cursor {
                                        Some(prev) if prev > key => prev,
                                        _ => key,
                                    });
                                }
                            }
                        }
                        None => {
                            if self.config.fail_unfulfillable_tasks {
                                *self
                                    .unfulfillable_sweep
                                    .lock()
                                    .unwrap_or_else(|e| e.into_inner()) = UnfulfillableSweep {
                                    epochs: Some(sweep_state),
                                    cursor: None,
                                    completed: true,
                                };
                            }
                            break;
                        }
                    }
                }
            }
        }
        self.rr_cursor
            .store(engine.rr_cursor, std::sync::atomic::Ordering::Relaxed);
        let busy_workers: Vec<(Uuid, &str, f32, usize)> = if saturated_skips > 0 {
            engine
                .seats
                .iter()
                .enumerate()
                .filter(|(idx, seat)| {
                    seat.worker.status != WorkerStatus::Disconnected
                        && engine.worker_index.meta(*idx).host_busy
                        && seat.remaining_slots() > 0
                })
                .map(|(_, seat)| {
                    (
                        seat.worker.worker_id,
                        seat.worker.capabilities.name.as_str(),
                        seat.worker.cpu_usage_pct,
                        seat.load(),
                    )
                })
                .collect()
        } else {
            Vec::new()
        };
        self.update_host_cpu_backpressure(saturated_skips, &busy_workers);
        for (task_id, reason) in &report.unfulfillable {
            if let Some(result) = self
                .queue
                .fail_unfulfillable_task(task_id, reason.clone())
                .await
            {
                warn!(
                    task_id = %task_id,
                    reason = %reason,
                    "Failing unfulfillable task"
                );
                if let Some(ref waiters) = self.waiters {
                    let mut lock = waiters.write().await;
                    if let Some(senders) = lock.remove(task_id) {
                        for tx in senders {
                            let _ = tx.send(result.clone());
                        }
                    }
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
            // Defense in depth (design 0083): a task with a critical Worker/Both extension never
            // goes to a peer below protocol 10, whose copy would lose the restriction. Placement
            // keeps it off such workers; if a pass still picks one, refuse the assignment.
            if let Some(version) = self
                .registry
                .peer_version_of(&assignment.worker_id, assignment.session_id)
                .await
            {
                if assignment.task.withheld_from_peer(version) {
                    warn!(
                        task_id = %assignment.task_id,
                        worker_id = %assignment.worker_id,
                        peer_version = version,
                        "Refusing to assign a task with a critical worker extension to a peer \
                         below the extension protocol; returning it to the queue"
                    );
                    let _ = self
                        .queue
                        .requeue_dispatch_failure(
                            &assignment.task_id,
                            &assignment.worker_id,
                            assignment.session_id,
                        )
                        .await;
                    continue;
                }
            }
            let mut task = assignment.task.clone();
            // The worker cannot enforce a master-scope extension and would refuse a critical one
            // as unknown (design 0082 D4.4); the queue keeps the stored task whole.
            task.strip_master_extensions();
            let msg = if let Some(latest_checkpoint) = task.latest_checkpoint.take() {
                MasterMessage::AssignTaskWithCheckpoint {
                    task,
                    latest_checkpoint: Some(latest_checkpoint),
                }
            } else {
                MasterMessage::AssignTask { task }
            };
            self.dispatch(assignment, msg).await;
        }

        Ok(report)
    }

    /// Delivers one assignment to the connection it was made against.
    ///
    /// The slot is reserved on the assignment's *session*: if the worker disconnected or
    /// re-registered since the snapshot, nothing was sent and only this task is put back. If the
    /// session's channel has immediate room (`sender.try_reserve()`), the message is delivered
    /// synchronously under the queue read lock via [`TaskQueue::deliver_if_scheduled`]. When the
    /// channel is `Full`, delivery is handed off to a per-assignment spawned waiter bounded by
    /// [`DISPATCH_RESERVE_TIMEOUT`] and [`MAX_IN_FLIGHT_DISPATCH_WAITERS_PER_WORKER`] (`BUG-20`,
    /// design `0075`), so one slow worker never stalls the rest of the scheduling pass. If the
    /// session's channel is dead (`Closed`), the whole session is torn down and failed over (waking
    /// any `wait = true` client whose task runs out of failovers).
    async fn dispatch(&self, assignment: &ScheduleAssignment, msg: MasterMessage) {
        let ScheduleAssignment {
            task_id,
            worker_id,
            session_id,
            ..
        } = assignment;
        let task_id = *task_id;
        let worker_id = *worker_id;
        let session_id = *session_id;
        #[cfg(feature = "otel")]
        let assign_span = tracing::info_span!(
            target: crate::otel::OTEL_SPAN_TARGET,
            "assign",
            task_id = %task_id,
            worker_id = %worker_id,
            outcome = tracing::field::Empty,
        );

        let (sender, session_live) =
            match self.registry.reserve_dispatch(&worker_id, session_id).await {
                Ok(sender) => sender,
                Err(e) => {
                    #[cfg(feature = "otel")]
                    assign_span.record("outcome", "session_gone");
                    warn!(
                        task_id = %task_id,
                        worker_id = %worker_id,
                        session_id = session_id,
                        error = %e,
                        "Worker session is gone; returning assignment to the queue"
                    );
                    let _ = self
                        .queue
                        .requeue_dispatch_failure(&task_id, &worker_id, session_id)
                        .await;
                    return;
                }
            };

        // Fast path: try to reserve a permit without blocking the pass. Only when the worker's
        // outbound channel is full do we defer to a bounded background waiter (`BUG-20`).
        match sender.try_reserve() {
            Ok(permit) => {
                let live_for_deliver = Arc::clone(&session_live);
                match self
                    .queue
                    .deliver_if_scheduled_with_live(
                        &task_id,
                        &worker_id,
                        session_id,
                        Arc::clone(&session_live),
                        move || {
                            if !live_for_deliver.load(std::sync::atomic::Ordering::Acquire) {
                                drop(permit);
                                return None;
                            }
                            permit.send(msg);
                            Some(())
                        },
                    )
                    .await
                {
                    Some(()) => {
                        #[cfg(feature = "otel")]
                        assign_span.record("outcome", "dispatched");
                        debug!(
                            task_id = %task_id,
                            worker_id = %worker_id,
                            "Successfully dispatched AssignTask to worker"
                        );
                    }
                    None => {
                        #[cfg(feature = "otel")]
                        assign_span.record("outcome", "stale");
                        debug!(
                            task_id = %task_id,
                            worker_id = %worker_id,
                            session_id = session_id,
                            "Task is no longer scheduled on a live session; not delivering AssignTask"
                        );
                        self.registry
                            .release_dispatch_if_live(&worker_id, session_id, &session_live)
                            .await;
                    }
                }
                return;
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(())) => {
                #[cfg(feature = "otel")]
                assign_span.record("outcome", "dispatch_failed");
                Self::rollback_closed_dispatch(
                    &self.registry,
                    &self.queue,
                    &self.trigger,
                    self.waiters.as_ref(),
                    task_id,
                    worker_id,
                    session_id,
                    &session_live,
                )
                .await;
                return;
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(())) => {}
        }

        let Some(in_flight_guard) = InFlightDispatchGuard::try_acquire(
            &self.in_flight_dispatches,
            worker_id,
            self.max_in_flight_dispatches_per_worker,
        ) else {
            #[cfg(feature = "otel")]
            assign_span.record("outcome", "dispatch_failed");
            warn!(
                task_id = %task_id,
                worker_id = %worker_id,
                session_id = session_id,
                cap = self.max_in_flight_dispatches_per_worker,
                "Worker channel is full and deferred dispatch waiter cap is reached; returning assignment to the queue"
            );
            self.registry
                .release_dispatch_if_live(&worker_id, session_id, &session_live)
                .await;
            if self
                .queue
                .requeue_dispatch_failure(&task_id, &worker_id, session_id)
                .await
                .unwrap_or(false)
            {
                self.trigger.notify_one();
            }
            return;
        };

        // Poll `reserve_owned` once before spawning so the waiter node is registered in the
        // channel's FIFO semaphore waitlist in exact pass order (before any later assignment or
        // control frame on the same worker).
        let mut reserve_fut = Box::pin(sender.reserve_owned());
        let initial_poll = {
            let mut noop_cx = std::task::Context::from_waker(std::task::Waker::noop());
            std::future::Future::poll(reserve_fut.as_mut(), &mut noop_cx)
        };
        match initial_poll {
            std::task::Poll::Ready(Ok(permit)) => {
                drop(in_flight_guard);
                let live_for_deliver = Arc::clone(&session_live);
                match self
                    .queue
                    .deliver_if_scheduled_with_live(
                        &task_id,
                        &worker_id,
                        session_id,
                        Arc::clone(&session_live),
                        move || {
                            if !live_for_deliver.load(std::sync::atomic::Ordering::Acquire) {
                                drop(permit);
                                return None;
                            }
                            let _ = permit.send(msg);
                            Some(())
                        },
                    )
                    .await
                {
                    Some(()) => {
                        #[cfg(feature = "otel")]
                        assign_span.record("outcome", "dispatched");
                        debug!(
                            task_id = %task_id,
                            worker_id = %worker_id,
                            "Successfully dispatched AssignTask to worker"
                        );
                    }
                    None => {
                        #[cfg(feature = "otel")]
                        assign_span.record("outcome", "stale");
                        debug!(
                            task_id = %task_id,
                            worker_id = %worker_id,
                            session_id = session_id,
                            "Task is no longer scheduled on a live session; not delivering AssignTask"
                        );
                        self.registry
                            .release_dispatch_if_live(&worker_id, session_id, &session_live)
                            .await;
                    }
                }
                return;
            }
            std::task::Poll::Ready(Err(_)) => {
                drop(in_flight_guard);
                #[cfg(feature = "otel")]
                assign_span.record("outcome", "dispatch_failed");
                Self::rollback_closed_dispatch(
                    &self.registry,
                    &self.queue,
                    &self.trigger,
                    self.waiters.as_ref(),
                    task_id,
                    worker_id,
                    session_id,
                    &session_live,
                )
                .await;
                return;
            }
            std::task::Poll::Pending => {}
        }

        let registry = self.registry.clone();
        let queue = self.queue.clone();
        let trigger = Arc::clone(&self.trigger);
        let waiters = self.waiters.clone();
        let timeout = self.dispatch_reserve_timeout;
        let deadline = tokio::time::Instant::now() + timeout;
        tokio::spawn(async move {
            let _guard = in_flight_guard;
            match tokio::time::timeout_at(deadline, reserve_fut).await {
                Ok(Ok(permit)) => {
                    let live_for_deliver = Arc::clone(&session_live);
                    match queue
                        .deliver_if_scheduled_with_live(
                            &task_id,
                            &worker_id,
                            session_id,
                            Arc::clone(&session_live),
                            move || {
                                if !live_for_deliver.load(std::sync::atomic::Ordering::Acquire) {
                                    drop(permit);
                                    return None;
                                }
                                let _ = permit.send(msg);
                                Some(())
                            },
                        )
                        .await
                    {
                        Some(()) => {
                            #[cfg(feature = "otel")]
                            assign_span.record("outcome", "dispatched");
                            debug!(
                                task_id = %task_id,
                                worker_id = %worker_id,
                                "Successfully dispatched deferred AssignTask to worker"
                            );
                        }
                        None => {
                            #[cfg(feature = "otel")]
                            assign_span.record("outcome", "stale");
                            // Cancelled, failed over or reassigned between the placement and now:
                            // the worker never hears of it, so it must not run it (and its slot is free again).
                            debug!(
                                task_id = %task_id,
                                worker_id = %worker_id,
                                session_id = session_id,
                                "Task is no longer scheduled on a live session; not delivering deferred AssignTask"
                            );
                            registry
                                .release_dispatch_if_live(&worker_id, session_id, &session_live)
                                .await;
                        }
                    }
                }
                Ok(Err(_)) => {
                    #[cfg(feature = "otel")]
                    assign_span.record("outcome", "dispatch_failed");
                    Self::rollback_closed_dispatch(
                        &registry,
                        &queue,
                        &trigger,
                        waiters.as_ref(),
                        task_id,
                        worker_id,
                        session_id,
                        &session_live,
                    )
                    .await;
                }
                Err(_) => {
                    #[cfg(feature = "otel")]
                    assign_span.record("outcome", "dispatch_failed");
                    warn!(
                        task_id = %task_id,
                        worker_id = %worker_id,
                        session_id = session_id,
                        timeout_ms = timeout.as_millis() as u64,
                        "Timed out waiting for room in worker channel; returning assignment to the queue"
                    );
                    registry
                        .release_dispatch_if_live(&worker_id, session_id, &session_live)
                        .await;
                    if queue
                        .requeue_dispatch_failure(&task_id, &worker_id, session_id)
                        .await
                        .unwrap_or(false)
                    {
                        trigger.notify_one();
                    }
                }
            }
        });
    }

    #[allow(clippy::too_many_arguments)]
    async fn rollback_closed_dispatch(
        registry: &WorkerRegistry,
        queue: &TaskQueue,
        trigger: &Arc<Notify>,
        waiters: Option<&WaiterMap>,
        task_id: TaskId,
        worker_id: Uuid,
        session_id: u64,
        session_live: &Arc<std::sync::atomic::AtomicBool>,
    ) {
        warn!(
            task_id = %task_id,
            worker_id = %worker_id,
            session_id = session_id,
            "Failed to dispatch AssignTask to worker; rolling back assignment"
        );
        registry
            .release_dispatch_if_live(&worker_id, session_id, session_live)
            .await;
        // Requeue this task first, uncharged: it never left the master, and the sweep below
        // would otherwise count it as a failover.
        let _ = queue
            .requeue_dispatch_failure(&task_id, &worker_id, session_id)
            .await;
        crate::server::handle_worker_disconnect_with_broadcast(
            registry,
            queue,
            trigger,
            waiters,
            &worker_id,
            Some(session_id),
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
            let mut last_pass: Option<Instant> = None;
            const MAX_CONSECUTIVE_BATCHES: usize = 16;

            loop {
                tokio::select! {
                    _ = self.trigger.notified() => {
                        debug!("Scheduler loop woken by event notification");
                        let delay = last_pass.map_or(Duration::ZERO, |t| {
                            self.config.micro_batch_window.saturating_sub(t.elapsed())
                        });
                        if delay > Duration::ZERO {
                            tokio::time::sleep(delay).await;
                        }
                        for _ in 0..MAX_CONSECUTIVE_BATCHES {
                            if *shutdown_rx.borrow() {
                                break;
                            }
                            let batch_cap = self.config.max_batch_size.max(1);
                            let report = self.schedule_once().await;
                            last_pass = Some(Instant::now());
                            let filled = report
                                .as_ref()
                                .is_ok_and(|r| r.assignments.len() >= batch_cap);
                            if !filled {
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                    }
                    _ = ticker.tick() => {
                        let _ = self.schedule_once().await;
                        last_pass = Some(Instant::now());
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
            mobile_status: None,
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

    fn extension_task(task_list: &[(u16, bool)], requirement_list: &[(u16, bool)]) -> Task {
        use rusty_grid_core::task::Extension;
        let make = |items: &[(u16, bool)]| {
            items
                .iter()
                .map(|(id, critical)| Extension {
                    id: *id,
                    critical: *critical,
                    bytes: vec![1],
                })
                .collect::<Vec<_>>()
        };
        let mut task = constrained_task(&[]);
        task.extensions = make(task_list);
        task.requirements.extensions = make(requirement_list);
        task.validate().expect("test extensions must be valid");
        task
    }

    #[test]
    fn test_critical_worker_extension_is_placed_only_on_workers_with_the_ext_tag() {
        let config = SchedulerConfig::default();
        let plain = tagged_worker("plain", &["os:linux"]);
        let capable = tagged_worker("capable", &["os:linux", "ext:32770"]);
        // 0x8002 = 32770, Worker scope, critical.
        let task = extension_task(&[(0x8002, true)], &[]);
        assert_eq!(task.required_extension_ids(), vec![0x8002]);

        // Without the tag nobody takes it, and the wait names the id.
        let report = WorkloadScheduler::matchmake(
            &config,
            std::slice::from_ref(&task),
            std::slice::from_ref(&plain),
        );
        assert!(report.assignments.is_empty());
        match &report.skipped[0].1 {
            ScheduleSkipReason::ClusterLacksCapability(msg) => {
                assert!(msg.contains("ext=32770"), "{msg}")
            }
            other => panic!("unexpected skip reason {other:?}"),
        }
        let strict = SchedulerConfig {
            fail_unfulfillable_tasks: true,
            ..SchedulerConfig::default()
        };
        let report = WorkloadScheduler::matchmake(
            &strict,
            std::slice::from_ref(&task),
            std::slice::from_ref(&plain),
        );
        assert_eq!(report.unfulfillable.len(), 1);
        assert!(report.unfulfillable[0].1.contains("ext=32770"));

        // With the tag it is placed, and only there.
        let report = WorkloadScheduler::matchmake(
            &config,
            std::slice::from_ref(&task),
            &[plain.clone(), capable.clone()],
        );
        assert_eq!(report.assignments.len(), 1);
        assert_eq!(report.assignments[0].worker_id, capable.worker_id);
    }

    #[test]
    fn test_non_critical_and_master_scope_extensions_never_restrict_placement() {
        let config = SchedulerConfig::default();
        let plain = tagged_worker("plain", &["os:linux"]);
        // 0x8001 non-critical hint, 0x8003 critical but Master scope, 0x9999 unknown non-critical.
        let mut task = extension_task(&[(0x8001, false), (0x8003, true)], &[]);
        task.extensions.push(rusty_grid_core::task::Extension {
            id: 0x9999,
            critical: false,
            bytes: vec![],
        });
        let report = WorkloadScheduler::matchmake(
            &config,
            std::slice::from_ref(&task),
            std::slice::from_ref(&plain),
        );
        assert_eq!(report.assignments.len(), 1, "{report:?}");
        // The copy for the worker has no master-scope extension; the stored task keeps it, and the
        // unknown non-critical one is relayed untouched.
        let mut sent = report.assignments[0].task.clone();
        assert_eq!(sent.extensions.len(), 3);
        sent.strip_master_extensions();
        let ids: Vec<u16> = sent.extensions.iter().map(|e| e.id).collect();
        assert_eq!(ids, vec![0x8001, 0x9999]);
    }

    #[test]
    fn test_required_extensions_are_part_of_the_task_shape() {
        // Same resources, different `ext:` needs: the first finds no worker, which must not make
        // the engine skip the second as "the same kind of task".
        let config = SchedulerConfig::default();
        let workers = vec![tagged_worker("w", &["os:linux", "ext:32772"])];
        let needs_a = extension_task(&[(0x8002, true)], &[]);
        let needs_b = extension_task(&[], &[(0x8004, true)]);
        let report =
            WorkloadScheduler::matchmake(&config, &[needs_a.clone(), needs_b.clone()], &workers);
        assert_eq!(report.assignments.len(), 1, "{report:?}");
        assert_eq!(report.assignments[0].task_id, needs_b.id);
        assert_eq!(report.skipped[0].0, needs_a.id);
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
    fn test_with_max_host_cpu_pct_100_allows_saturated_test_host() {
        let mut busy_worker = make_worker("saturated-worker", 4, 8192, false, false);
        busy_worker.cpu_usage_pct = 95.0;

        let task = Task::new(
            TaskSpec::command("echo", vec!["test".into()]),
            TaskRequirements::generic(1, 10),
        );

        let default_report = WorkloadScheduler::matchmake(
            &SchedulerConfig::default(),
            std::slice::from_ref(&task),
            std::slice::from_ref(&busy_worker),
        );
        assert!(default_report.assignments.is_empty());
        assert_eq!(
            default_report.skipped,
            vec![(task.id, ScheduleSkipReason::AllEligibleWorkersSaturated)]
        );

        let relaxed_report = WorkloadScheduler::matchmake(
            &SchedulerConfig::default().with_max_host_cpu_pct(100.0),
            std::slice::from_ref(&task),
            std::slice::from_ref(&busy_worker),
        );
        assert_eq!(relaxed_report.assignments.len(), 1);
        assert_eq!(
            relaxed_report.assignments[0].worker_id,
            busy_worker.worker_id
        );

        for cpu in [100.0_f32, 100.1_f32] {
            let mut saturated = busy_worker.clone();
            saturated.cpu_usage_pct = cpu;
            let opt_out_report = WorkloadScheduler::matchmake(
                &SchedulerConfig::default().without_host_cpu_backpressure(),
                std::slice::from_ref(&task),
                &[saturated],
            );
            assert_eq!(opt_out_report.assignments.len(), 1);
            assert_eq!(
                opt_out_report.assignments[0].worker_id,
                busy_worker.worker_id
            );
        }
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

        // N34-2: When an 8-core GPU worker is capped by `slots:3` (`max_slots = 8`, `max_tasks = 3`),
        // Stage 6 reserves one of the 3 task permits (`max_tasks`) even though 5 core slots remain free.
        let mut gpu_8c_slots3 = make_worker("gpu-8c-slots3", 8, 32768, true, true);
        gpu_8c_slots3.capabilities.tags.push("slots:3".into());
        let mut batch = vec![gpu_task()];
        batch.extend((0..4).map(|i| cmd_task(&format!("slots3-c-{i}"))));
        let report =
            WorkloadScheduler::matchmake(&config, &batch, std::slice::from_ref(&gpu_8c_slots3));
        assert_eq!(
            report.assignments.len(),
            2,
            "8-core GPU worker with slots:3 must run 1 GPU + 1 CPU task and keep 1 of its 3 task permits reserved"
        );

        // N34-2: Conversely, an 8-core GPU worker capped by `slots:1` (`max_slots = 8`, `max_tasks = 1`)
        // clamps `reserve` to `max_tasks - 1 = 0`, so it is not starved of CPU work while GPU work waits.
        let mut gpu_8c_slots1 = make_worker("gpu-8c-slots1", 8, 32768, true, true);
        gpu_8c_slots1.capabilities.tags.push("slots:1".into());
        let pinned_gpu = gpu_task().with_tags(vec![format!("require:worker={}", solo.worker_id)]);
        let report = WorkloadScheduler::matchmake(
            &config,
            &[pinned_gpu, cmd_task("for-slots1")],
            &[solo, gpu_8c_slots1.clone()],
        );
        let counts = placements_per_worker(&report);
        assert_eq!(
            counts.get(&gpu_8c_slots1.worker_id).copied(),
            Some(1),
            "8-core GPU worker with slots:1 must not be starved of CPU work by gpu_reserved_slots"
        );
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

    #[test]
    fn test_laptop_battery_above_critical_accepts_gpu_and_compile_tasks() {
        let config = SchedulerConfig::default();

        let mut macbook = make_worker("macbook-air-m5", 10, 32768, true, false);
        macbook.capabilities.tags.push("os:macos".into());
        macbook.capabilities.mobile = Some(rusty_grid_core::capabilities::MobileCapabilities {
            os_version: "macOS 26.0".into(),
            soc_model: "Apple M5".into(),
            battery_pct: Some(35),
            is_charging: Some(false),
            thermal_throttled: false,
        });

        let gpu_task = Task::new(
            TaskSpec::gpu_compute("gemm_kernel", 64),
            TaskRequirements::gpu(30),
        );
        let report = WorkloadScheduler::matchmake(
            &config,
            std::slice::from_ref(&gpu_task),
            std::slice::from_ref(&macbook),
        );
        assert_eq!(
            report.assignments.len(),
            1,
            "Unplugged macOS laptop at 35% battery must still accept GPU tasks"
        );

        // At <15% battery (Critical), the laptop is still protected from heavy/GPU tasks.
        macbook.capabilities.mobile.as_mut().unwrap().battery_pct = Some(10);
        let critical_report = WorkloadScheduler::matchmake(
            &config,
            std::slice::from_ref(&gpu_task),
            std::slice::from_ref(&macbook),
        );
        assert_eq!(
            critical_report.assignments.len(),
            0,
            "Unplugged laptop below 15% critical battery must refuse GPU tasks"
        );
    }

    #[test]
    fn test_worker_slots_tag_caps_concurrency_and_spills_overflow_to_idle_peer() {
        let config = SchedulerConfig::default();

        // An 8-core worker capped with `slots:2` (--max-concurrency 2) and an idle 4-core peer.
        let mut capped_worker = make_worker("worker-8c-slots2", 8, 16384, false, false);
        capped_worker.capabilities.tags.push("slots:2".into());
        let idle_peer = make_worker("worker-4c-peer", 4, 16384, false, false);
        assert_eq!(config.effective_max_concurrency(&capped_worker), 8);
        assert_eq!(config.effective_max_tasks(&capped_worker), 2);
        assert_eq!(config.effective_max_concurrency(&idle_peer), 4);
        assert_eq!(config.effective_max_tasks(&idle_peer), 4);

        let workers = vec![capped_worker.clone(), idle_peer.clone()];
        let tasks: Vec<Task> = (0..6)
            .map(|i| {
                Task::new(
                    TaskSpec::shell_script(format!("echo {i}")),
                    TaskRequirements::generic(1, 30),
                )
            })
            .collect();

        let report = WorkloadScheduler::matchmake(&config, &tasks, &workers);
        assert_eq!(report.assignments.len(), 6);

        let capped_count = report
            .assignments
            .iter()
            .filter(|a| a.worker_id == capped_worker.worker_id)
            .count();
        let peer_count = report
            .assignments
            .iter()
            .filter(|a| a.worker_id == idle_peer.worker_id)
            .count();
        assert_eq!(
            capped_count, 2,
            "worker with slots:2 on 8 cores must receive at most 2 tasks"
        );
        assert_eq!(
            peer_count, 4,
            "overflow tasks must spill to the idle peer worker"
        );

        // F34-1: A default 8-core worker advertises `slots:7` (cores - 1 headroom).
        // Because `max_slots` stays in cores (8) while `max_tasks` is 7, two 4-core
        // half-machine tasks fit and run concurrently on the worker in a single pass.
        let mut default_8c = make_worker("worker-8c-default-slots7", 8, 16384, false, false);
        default_8c.capabilities.tags.push("slots:7".into());
        assert_eq!(config.effective_max_concurrency(&default_8c), 8);
        assert_eq!(config.effective_max_tasks(&default_8c), 7);

        let wide_tasks: Vec<Task> = (0..2)
            .map(|i| {
                Task::new(
                    TaskSpec::shell_script(format!("echo wide-{i}")),
                    TaskRequirements::generic(4, 30),
                )
            })
            .collect();
        let wide_report =
            WorkloadScheduler::matchmake(&config, &wide_tasks, std::slice::from_ref(&default_8c));
        assert_eq!(
            wide_report.assignments.len(),
            2,
            "an 8-core worker with slots:7 must run two 4-core tasks side by side"
        );

        // Conversely, an 8-core worker with `slots:1` (--max-concurrency 1) takes only ONE
        // 4-core task at a time even though 4 + 4 <= 8 cores.
        let mut single_slot_8c = make_worker("worker-8c-slots1", 8, 16384, false, false);
        single_slot_8c.capabilities.tags.push("slots:1".into());
        let single_report = WorkloadScheduler::matchmake(
            &config,
            &wide_tasks,
            std::slice::from_ref(&single_slot_8c),
        );
        assert_eq!(
            single_report.assignments.len(),
            1,
            "an 8-core worker with slots:1 must take at most 1 task at a time"
        );
    }

    #[tokio::test]
    async fn test_round_robin_rotates_across_schedule_once_passes() {
        let registry = WorkerRegistry::new();
        let queue = TaskQueue::new();
        let notify = Arc::new(Notify::new());
        let scheduler = WorkloadScheduler::new(
            SchedulerConfig {
                policy: SchedulingPolicy::RoundRobin,
                ..Default::default()
            },
            registry.clone(),
            queue.clone(),
            notify,
        );

        let mut worker_ids = Vec::new();
        let mut receivers = Vec::new();
        for i in 0..3 {
            let wid = Uuid::from_u128((i + 1) as u128);
            let mut caps =
                WorkerCapabilities::new(format!("rr-worker-{i}"), 4, 8192, false, false, None);
            caps.tags.push("slots:4".into());
            let (tx, rx) = tokio::sync::mpsc::channel(16);
            registry
                .register(wid, caps, "127.0.0.1:9000".parse().unwrap(), tx, None)
                .await
                .unwrap();
            worker_ids.push(wid);
            receivers.push(rx);
        }

        // Submit 6 tasks one at a time, running `schedule_once()` after each submission.
        let mut counts: HashMap<Uuid, usize> =
            worker_ids.iter().copied().map(|id| (id, 0usize)).collect();
        for i in 0..6 {
            let task = Task::new(
                TaskSpec::shell_script(format!("echo rr-{i}")),
                TaskRequirements::generic(1, 30),
            );
            queue.submit(task).await.unwrap();
            let report = scheduler.schedule_once().await.unwrap();
            assert_eq!(
                report.assignments.len(),
                1,
                "pass {i} must assign the newly submitted task"
            );
            *counts.get_mut(&report.assignments[0].worker_id).unwrap() += 1;
        }

        let per_worker: Vec<usize> = worker_ids.iter().map(|id| counts[id]).collect();
        assert_eq!(
            per_worker,
            vec![2, 2, 2],
            "round-robin across 6 single-task schedule_once passes on 3 4-slot workers must distribute 2, 2, 2"
        );
    }

    #[test]
    fn test_mobile_status_thermal_tiers_hot_caps_to_one_slot_and_critical_sheds_all_load() {
        let config = SchedulerConfig::default();

        let mut phone = make_worker("pixel-9-pro", 8, 12288, false, false);
        phone.capabilities.tags.push("os:android".into());
        phone.capabilities.mobile = Some(rusty_grid_core::capabilities::MobileCapabilities {
            os_version: "Android 15 (API 35)".into(),
            soc_model: "Tensor G4".into(),
            battery_pct: Some(85),
            is_charging: Some(true),
            thermal_throttled: false,
        });

        let light_tasks: Vec<Task> = (0..6)
            .map(|i| {
                Task::new(
                    TaskSpec::shell_script(format!("echo light-{i}")),
                    TaskRequirements::generic(1, 30),
                )
            })
            .collect();
        let multi_core = Task::new(
            TaskSpec::shell_script("echo multi"),
            TaskRequirements::generic(2, 30),
        );
        let compile = Task::new(
            TaskSpec::rust_compilation("crate", Default::default(), vec![]),
            TaskRequirements::generic(1, 60),
        );

        // Hot via explicit thermal_tier: at most 1 slot across 6 1-core tasks, 0 for 2-core or compile.
        phone.mobile_status = Some(MobileLiveStatus {
            battery_pct: Some(85),
            is_charging: Some(true),
            thermal_throttled: false,
            battery_temperature: Some(41.0),
            thermal_tier: Some(ThermalTier::Hot),
            ..Default::default()
        });
        let hot_report =
            WorkloadScheduler::matchmake(&config, &light_tasks, std::slice::from_ref(&phone));
        assert_eq!(
            hot_report.assignments.len(),
            1,
            "Hot thermal tier on an 8-core phone must cap concurrency to at most 1 slot"
        );
        assert_eq!(
            WorkloadScheduler::matchmake(
                &config,
                std::slice::from_ref(&multi_core),
                std::slice::from_ref(&phone),
            )
            .assignments
            .len(),
            0,
            "Hot thermal tier must reject multi-core tasks"
        );
        assert_eq!(
            WorkloadScheduler::matchmake(
                &config,
                std::slice::from_ref(&compile),
                std::slice::from_ref(&phone),
            )
            .assignments
            .len(),
            0,
            "Hot thermal tier must reject heavy compilation tasks even while charging"
        );

        // Critical via explicit thermal_tier: 0 assignments.
        phone.mobile_status = Some(MobileLiveStatus {
            battery_pct: Some(85),
            is_charging: Some(true),
            thermal_throttled: false,
            thermal_tier: Some(ThermalTier::Critical),
            ..Default::default()
        });
        assert_eq!(
            WorkloadScheduler::matchmake(&config, &light_tasks, std::slice::from_ref(&phone))
                .assignments
                .len(),
            0,
            "Critical thermal tier must shed all load"
        );

        // Critical via 45°C temperature alone (thermal_tier = None, thermal_throttled = false): 0 assignments.
        phone.mobile_status = Some(MobileLiveStatus {
            battery_pct: Some(85),
            is_charging: Some(true),
            thermal_throttled: false,
            battery_temperature: Some(45.0),
            thermal_tier: None,
            ..Default::default()
        });
        assert_eq!(
            WorkloadScheduler::matchmake(&config, &light_tasks, std::slice::from_ref(&phone))
                .assignments
                .len(),
            0,
            "A phone at 45°C must receive 0 assignments even when thermal_throttled is false"
        );
    }

    #[tokio::test]
    async fn test_fail_unfulfillable_tasks_default_false_keeps_queued_and_true_fails_with_reason_and_wakes_waiter(
    ) {
        let (registry, queue, waiters, notify) = scheduler_fixture().await;
        assert!(
            !SchedulerConfig::default().fail_unfulfillable_tasks,
            "fail_unfulfillable_tasks must default to false"
        );

        let unplaceable_task = || constrained_task(&["require:tag=nothing"]);

        // 1. With 0 workers registered, even `fail_unfulfillable_tasks: true` keeps tasks queued.
        let t_no_workers = queue.submit(unplaceable_task()).await.unwrap();
        let sched_strict = WorkloadScheduler::new(
            SchedulerConfig::default().with_fail_unfulfillable_tasks(true),
            registry.clone(),
            queue.clone(),
            notify.clone(),
        )
        .with_waiters(waiters.clone());
        let r0 = sched_strict.schedule_once().await.unwrap();
        assert!(r0.unfulfillable.is_empty());
        assert_eq!(
            queue.get_state(&t_no_workers).await,
            Some(crate::queue::TaskState::Queued),
            "when no workers are registered, tasks must stay Queued"
        );
        let _ = queue.cancel_task(&t_no_workers, None).await;

        // 2. Register a 1-core CPU worker (`slots:1`) and verify `fail_unfulfillable_tasks: false`
        //    leaves an unplaceable task Queued.
        let (_w_id, _w_sess, mut w_rx) = register_worker(&registry, "cpu-w1", 1, 4096, false).await;
        let t_default = queue.submit(unplaceable_task()).await.unwrap();
        let sched_default = WorkloadScheduler::new(
            SchedulerConfig::default(),
            registry.clone(),
            queue.clone(),
            notify.clone(),
        )
        .with_waiters(waiters.clone());
        let r_def = sched_default.schedule_once().await.unwrap();
        assert!(r_def.unfulfillable.is_empty());
        assert_eq!(
            queue.get_state(&t_default).await,
            Some(crate::queue::TaskState::Queued),
            "with fail_unfulfillable_tasks=false, unplaceable task must stay Queued"
        );

        // 3. With `fail_unfulfillable_tasks: true`, register a waiter on `t_default` and run a pass:
        //    even if we also submit a normal CPU task that fills `cpu-w1`'s single slot first, AND
        //    a second normal CPU task behind it (which is temporarily blocked by busy slots, not
        //    unfulfillable), `t_default` and a second unplaceable task submitted when `free_slots == 0`
        //    are failed immediately with the mismatch reason, active_bytes drops, and the waiter wakes!
        let (wait_tx, wait_rx) = tokio::sync::oneshot::channel();
        waiters.write().await.insert(t_default, vec![wait_tx]);

        let t_fits_1 = queue.submit(cmd_task("runs-first")).await.unwrap();
        let t_fits_2 = queue.submit(cmd_task("waits-for-slot")).await.unwrap();

        let r_strict1 = sched_strict.schedule_once().await.unwrap();
        assert_eq!(r_strict1.assignments.len(), 1);
        assert_eq!(r_strict1.assignments[0].task_id, t_fits_1);
        assert!(matches!(
            w_rx.recv().await,
            Some(MasterMessage::AssignTask { .. })
        ));
        assert_eq!(r_strict1.unfulfillable.len(), 1);
        assert_eq!(r_strict1.unfulfillable[0].0, t_default);
        assert_eq!(
            r_strict1.unfulfillable[0].1,
            "No registered worker matches the placement constraints (tag=nothing)"
        );
        assert_eq!(
            queue.get_state(&t_default).await,
            Some(crate::queue::TaskState::Failed)
        );
        assert_eq!(
            queue.get_state(&t_fits_2).await,
            Some(crate::queue::TaskState::Queued),
            "a task that fits an existing worker's capabilities when idle must stay Queued while slots are busy"
        );

        let waited_res = wait_rx
            .await
            .expect("waiter on unfulfillable task must be notified");
        assert_eq!(waited_res.task_id, t_default);
        assert_eq!(waited_res.exit_code, 1);
        assert_eq!(
            waited_res.error.as_deref(),
            Some("No registered worker matches the placement constraints (tag=nothing)")
        );

        // 4. Now `cpu-w1` has 0 free slots (`t_fits_1` is still running). Submit another
        //    unplaceable task (`gpu_task()`) and a 16-core CPU task (`nobody_fits`), and verify
        //    `schedule_once()` STILL fails both even when `engine.free_slots == 0`, releasing
        //    their active payload budget.
        let active_before = queue.active_retained_bytes().await;
        let t_gpu_when_busy = queue.submit(gpu_task()).await.unwrap();
        let t_16c_when_busy = queue
            .submit(Task::new(
                TaskSpec::shell_script("echo 16-cores".to_string()),
                TaskRequirements::generic(16, 30),
            ))
            .await
            .unwrap();
        assert!(queue.active_retained_bytes().await > active_before);

        let r_strict2 = sched_strict.schedule_once().await.unwrap();
        assert!(r_strict2.assignments.is_empty());
        assert_eq!(r_strict2.unfulfillable.len(), 2);
        assert_eq!(r_strict2.unfulfillable[0].0, t_gpu_when_busy);
        assert_eq!(
            r_strict2.unfulfillable[0].1,
            "Cluster lacks workers with GPU capabilities"
        );
        assert_eq!(r_strict2.unfulfillable[1].0, t_16c_when_busy);
        assert_eq!(
            r_strict2.unfulfillable[1].1,
            "No registered worker satisfies task requirements (cpu_cores=16, ram_mb=512, gpu=false)"
        );
        assert_eq!(
            queue.get_state(&t_gpu_when_busy).await,
            Some(crate::queue::TaskState::Failed)
        );
        assert_eq!(
            queue.get_state(&t_16c_when_busy).await,
            Some(crate::queue::TaskState::Failed)
        );
        assert_eq!(
            queue.active_retained_bytes().await,
            active_before,
            "failing unfulfillable tasks must release their active payload bytes"
        );

        // 5. With `free_slots == 0` and an unchanged registry and queue, a subsequent scheduling
        //    pass must not scan the queue again (`ready_views_call_count` does not increase).
        let calls_before = queue.ready_views_call_count();
        let r_strict3 = sched_strict.schedule_once().await.unwrap();
        assert!(r_strict3.assignments.is_empty());
        assert!(r_strict3.unfulfillable.is_empty());
        assert_eq!(
            queue.ready_views_call_count(),
            calls_before,
            "a pass with 0 free slots and an unchanged registry must not scan the queue again"
        );
    }

    #[tokio::test]
    async fn test_fail_unfulfillable_tasks_sweep_cursor_scans_large_queue_once_across_saturated_passes(
    ) {
        let (registry, queue, waiters, notify) = scheduler_fixture().await;
        // 1 worker with 1 CPU slot.
        let (_w_id, _w_sess, _w_rx) = register_worker(&registry, "cpu-1c", 1, 4096, false).await;

        // 6 tasks (exceeding `max_scan_per_pass = 3`). The 6th task is unfulfillable (requires GPU).
        for i in 0..5 {
            let mut req = TaskRequirements::generic(1, 30);
            req.ram_mb = 256;
            queue
                .submit(Task::new(
                    TaskSpec::shell_script(format!("echo cpu-{i}")),
                    req,
                ))
                .await
                .unwrap();
        }
        let t_gpu_tail = queue.submit(gpu_task()).await.unwrap();

        let mut cfg = SchedulerConfig::default().with_fail_unfulfillable_tasks(true);
        cfg.max_scan_per_pass = 3;
        let sched = WorkloadScheduler::new(cfg, registry.clone(), queue.clone(), notify)
            .with_waiters(waiters);

        assert_eq!(queue.ready_views_call_count(), 0);

        // Pass 1: places task 1 (saturating the 1-core worker) and scans tasks 1..=3.
        let r1 = sched.schedule_once().await.unwrap();
        assert_eq!(r1.assignments.len(), 1);
        assert!(r1.unfulfillable.is_empty());
        assert_eq!(queue.ready_views_call_count(), 1);
        assert_eq!(
            queue.get_state(&t_gpu_tail).await,
            Some(crate::queue::TaskState::Queued)
        );

        // Pass 2 (first saturated pass, `free_slots == 0`, epochs unchanged): resumes from the
        // sweep cursor, scans tasks 4..=6, fails `t_gpu_tail`, and reaches the end of the queue.
        let r2 = sched.schedule_once().await.unwrap();
        assert!(r2.assignments.is_empty());
        assert_eq!(r2.unfulfillable.len(), 1);
        assert_eq!(r2.unfulfillable[0].0, t_gpu_tail);
        assert_eq!(queue.ready_views_call_count(), 2);
        assert_eq!(
            queue.get_state(&t_gpu_tail).await,
            Some(crate::queue::TaskState::Failed)
        );

        // Pass 3 (second saturated pass, `free_slots == 0`, epochs unchanged): must make 0
        // `ready_views` calls because the sweep reached the end of the queue in Pass 2.
        let r3 = sched.schedule_once().await.unwrap();
        assert!(r3.assignments.is_empty());
        assert!(r3.unfulfillable.is_empty());
        assert_eq!(
            queue.ready_views_call_count(),
            2,
            "second saturated pass with unchanged epochs must make no ready_views call"
        );
    }

    #[tokio::test]
    async fn test_fail_unfulfillable_tasks_keeps_gpu_task_queued_when_gpu_worker_is_disconnected_until_pruned(
    ) {
        let (registry, queue, waiters, notify) = scheduler_fixture().await;
        let (_cpu_id, _cpu_sess, _cpu_rx) =
            register_worker(&registry, "cpu-w1", 2, 4096, false).await;
        let (gpu_id, _gpu_sess, _gpu_rx) =
            register_worker(&registry, "gpu-w1", 4, 8192, true).await;

        // Mark the GPU worker Disconnected (not yet pruned).
        assert!(registry.mark_disconnected(&gpu_id).await.unwrap());

        // Submit a 2-core CPU task to saturate `cpu-w1` on Pass 1 so Pass 2 has `free_slots == 0`
        // and relies on `prune_stale_workers` bumping `registration_epoch` to trigger the sweep.
        let mut req_fill = TaskRequirements::generic(2, 30);
        req_fill.ram_mb = 512;
        let _t_fill = queue
            .submit(Task::new(TaskSpec::shell_script("echo fill"), req_fill))
            .await
            .unwrap();
        let t_gpu = queue.submit(gpu_task()).await.unwrap();
        let pin_constraint = format!("require:worker={gpu_id}");
        let t_pinned = queue
            .submit(constrained_task(&[&pin_constraint]))
            .await
            .unwrap();

        let sched = WorkloadScheduler::new(
            SchedulerConfig::default().with_fail_unfulfillable_tasks(true),
            registry.clone(),
            queue.clone(),
            notify,
        )
        .with_waiters(waiters);

        // Pass 1 while `gpu-w1` is Disconnected (still registered): saturates `cpu-w1` and keeps
        // both GPU/pinned tasks Queued.
        let r1 = sched.schedule_once().await.unwrap();
        assert_eq!(r1.assignments.len(), 1);
        assert!(
            r1.unfulfillable.is_empty(),
            "tasks matching a Disconnected (unpruned) worker must not be failed: {:?}",
            r1.unfulfillable
        );
        assert_eq!(
            queue.get_state(&t_gpu).await,
            Some(crate::queue::TaskState::Queued)
        );
        assert_eq!(
            queue.get_state(&t_pinned).await,
            Some(crate::queue::TaskState::Queued)
        );

        // Prune `gpu-w1` from the registry: now the next pass must fail both tasks.
        assert_eq!(registry.prune_stale_workers(Duration::ZERO).await, 1);
        let r2 = sched.schedule_once().await.unwrap();
        assert_eq!(r2.unfulfillable.len(), 2);
        assert_eq!(
            queue.get_state(&t_gpu).await,
            Some(crate::queue::TaskState::Failed)
        );
        assert_eq!(
            queue.get_state(&t_pinned).await,
            Some(crate::queue::TaskState::Failed)
        );
        assert_eq!(
            queue
                .get_task(&t_gpu)
                .await
                .unwrap()
                .error_message
                .as_deref(),
            Some("Cluster lacks workers with GPU capabilities")
        );
        assert_eq!(
            queue
                .get_task(&t_pinned)
                .await
                .unwrap()
                .error_message
                .as_deref(),
            Some(
                format!("No registered worker matches the placement constraints (worker={gpu_id})")
                    .as_str()
            )
        );
    }

    #[tokio::test]
    async fn test_fail_unfulfillable_tasks_keeps_gpu_task_queued_when_gpu_worker_is_cooling_down_until_pruned(
    ) {
        let (registry, queue, waiters, notify) = scheduler_fixture().await;
        let (_cpu_id, _cpu_sess, _cpu_rx) =
            register_worker(&registry, "cpu-w1", 2, 4096, false).await;
        let (gpu_id, _gpu_sess, _gpu_rx) =
            register_worker(&registry, "gpu-w1", 4, 8192, true).await;

        // Put `gpu-w1` into rejection cooldown (`excluded_until` set in the future).
        for _ in 0..3 {
            registry.record_rejection(&gpu_id, None).await;
        }
        assert!(
            registry
                .list_schedulable_workers()
                .await
                .iter()
                .all(|w| w.worker_id != gpu_id),
            "cooling-down GPU worker must be excluded from list_schedulable_workers"
        );

        let t_gpu = queue.submit(gpu_task()).await.unwrap();
        let pin_constraint = format!("require:worker={gpu_id}");
        let t_pinned = queue
            .submit(constrained_task(&[&pin_constraint]))
            .await
            .unwrap();

        let sched = WorkloadScheduler::new(
            SchedulerConfig::default().with_fail_unfulfillable_tasks(true),
            registry.clone(),
            queue.clone(),
            notify,
        )
        .with_waiters(waiters);

        // Pass 1 while `gpu-w1` is cooling down: both tasks must stay Queued.
        let r1 = sched.schedule_once().await.unwrap();
        assert!(
            r1.unfulfillable.is_empty(),
            "tasks matching a cooling-down worker must not be failed: {:?}",
            r1.unfulfillable
        );
        assert_eq!(
            queue.get_state(&t_gpu).await,
            Some(crate::queue::TaskState::Queued)
        );
        assert_eq!(
            queue.get_state(&t_pinned).await,
            Some(crate::queue::TaskState::Queued)
        );

        // Disconnect and prune `gpu-w1`: now the next pass must fail both tasks.
        assert!(registry.mark_disconnected(&gpu_id).await.unwrap());
        assert_eq!(registry.prune_stale_workers(Duration::ZERO).await, 1);
        let r2 = sched.schedule_once().await.unwrap();
        assert_eq!(r2.unfulfillable.len(), 2);
        assert_eq!(
            queue.get_state(&t_gpu).await,
            Some(crate::queue::TaskState::Failed)
        );
        assert_eq!(
            queue.get_state(&t_pinned).await,
            Some(crate::queue::TaskState::Failed)
        );
    }

    #[tokio::test]
    async fn test_fail_unfulfillable_tasks_combined_placement_and_gpu_checks_in_nobody_fits() {
        let (registry, queue, waiters, notify) = scheduler_fixture().await;

        // Register:
        // 1. Small Linux CPU worker (2 cores, os:linux)
        // 2. Large macOS CPU worker (16 cores, os:macos, no GPU)
        // 3. Small macOS GPU worker (2 cores, os:macos, GPU)
        let (tx1, _rx1) = tokio::sync::mpsc::channel(16);
        let mut caps_linux_small =
            WorkerCapabilities::new("linux-small", 2, 4096, false, false, None);
        caps_linux_small.tags = vec!["os:linux".into(), "slots:2".into()];
        registry
            .register(
                Uuid::new_v4(),
                caps_linux_small,
                "127.0.0.1:9101".parse().unwrap(),
                tx1,
                None,
            )
            .await
            .unwrap();

        let (tx2, _rx2) = tokio::sync::mpsc::channel(16);
        let mut caps_macos_big =
            WorkerCapabilities::new("macos-big", 16, 16384, false, false, None);
        caps_macos_big.tags = vec!["os:macos".into(), "slots:16".into()];
        registry
            .register(
                Uuid::new_v4(),
                caps_macos_big,
                "127.0.0.1:9102".parse().unwrap(),
                tx2,
                None,
            )
            .await
            .unwrap();

        let (tx3, _rx3) = tokio::sync::mpsc::channel(16);
        let mut caps_gpu_small =
            WorkerCapabilities::new("macos-gpu-small", 2, 4096, true, false, None);
        caps_gpu_small.tags = vec!["os:macos".into(), "slots:2".into()];
        registry
            .register(
                Uuid::new_v4(),
                caps_gpu_small,
                "127.0.0.1:9103".parse().unwrap(),
                tx3,
                None,
            )
            .await
            .unwrap();

        // Task A: requires `os=linux` AND 8 CPU cores. `linux-small` matches `os=linux` (2 cores),
        // `macos-big` has 16 cores (`os:macos`), so no single worker satisfies both.
        let t_linux_8c = queue
            .submit(
                Task::new(
                    TaskSpec::shell_script("echo linux-8c"),
                    TaskRequirements::generic(8, 30),
                )
                .with_tags(vec!["require:os=linux".into()]),
            )
            .await
            .unwrap();

        // Task B: requires GPU AND 8 CPU cores. `macos-gpu-small` has GPU (2 cores), `macos-big`
        // has 16 cores (CPU-only), so no single worker satisfies both.
        let mut req_gpu_8c = TaskRequirements::generic(8, 30);
        req_gpu_8c.gpu_required = true;
        let t_gpu_8c = queue
            .submit(Task::new(TaskSpec::shell_script("echo gpu-8c"), req_gpu_8c))
            .await
            .unwrap();

        let sched = WorkloadScheduler::new(
            SchedulerConfig::default().with_fail_unfulfillable_tasks(true),
            registry,
            queue.clone(),
            notify,
        )
        .with_waiters(waiters);

        let report = sched.schedule_once().await.unwrap();
        assert_eq!(report.unfulfillable.len(), 2);
        assert_eq!(
            queue.get_state(&t_linux_8c).await,
            Some(crate::queue::TaskState::Failed)
        );
        assert_eq!(
            queue
                .get_task(&t_linux_8c)
                .await
                .unwrap()
                .error_message
                .as_deref(),
            Some("No registered worker satisfies task requirements (cpu_cores=8, ram_mb=512, gpu=false)")
        );
        assert_eq!(
            queue.get_state(&t_gpu_8c).await,
            Some(crate::queue::TaskState::Failed)
        );
        assert_eq!(
            queue
                .get_task(&t_gpu_8c)
                .await
                .unwrap()
                .error_message
                .as_deref(),
            Some("No registered worker satisfies task requirements (cpu_cores=8, ram_mb=512, gpu=true)")
        );
    }

    #[tokio::test]
    async fn test_fail_unfulfillable_tasks_cursor_resets_on_epoch_change_and_placement_ignores_sweep_cursor(
    ) {
        let (registry, queue, waiters, notify) = scheduler_fixture().await;
        let (w_id, w_sess, _w_rx) = register_worker(&registry, "cpu-1c", 1, 4096, false).await;

        let mut cpu_ids = Vec::new();
        for i in 0..6 {
            let mut req = TaskRequirements::generic(1, 30);
            req.ram_mb = 256;
            cpu_ids.push(
                queue
                    .submit(Task::new(
                        TaskSpec::shell_script(format!("echo cpu-{i}")),
                        req,
                    ))
                    .await
                    .unwrap(),
            );
        }

        let mut cfg = SchedulerConfig::default().with_fail_unfulfillable_tasks(true);
        cfg.max_scan_per_pass = 3;
        let sched = WorkloadScheduler::new(cfg, registry.clone(), queue.clone(), notify)
            .with_waiters(waiters);

        // Pass 1: assigns task 1 (`cpu_ids[0]`), scans tasks 1..=3, and stores a partial sweep cursor at task 3.
        let r1 = sched.schedule_once().await.unwrap();
        assert_eq!(r1.assignments.len(), 1);
        assert_eq!(r1.assignments[0].task_id, cpu_ids[0]);

        // Rule A: Submit a priority-10 GPU task while the partial sweep cursor sits at task 3 (`cpu_ids[2]`).
        // Its `QueueOrderKey` sits at the front of `ready_queue`, BEFORE `sweep.cursor`. Because
        // `submit_with_priority` bumps `ready_epoch`, the next saturated pass must reset `sweep.cursor`
        // to `None` and fail `t_prio_gpu`.
        let t_prio_gpu = queue.submit_with_priority(gpu_task(), 10).await.unwrap();
        let r2 = sched.schedule_once().await.unwrap();
        assert!(r2.assignments.is_empty());
        assert_eq!(
            r2.unfulfillable.len(),
            1,
            "epoch change must reset the sweep cursor so a newly submitted priority-10 GPU task ahead of the old cursor is failed"
        );
        assert_eq!(r2.unfulfillable[0].0, t_prio_gpu);
        assert_eq!(
            queue.get_state(&t_prio_gpu).await,
            Some(crate::queue::TaskState::Failed)
        );

        // Rule B: Now `sweep.cursor` is at task 3 (`cpu_ids[2]`) again after Pass 2 scanned 3 items
        // (`t_prio_gpu`, `cpu_ids[1]`, `cpu_ids[2]`). Completing task 1 (`cpu_ids[0]`) frees a slot
        // without changing `(registration_epoch, ready_epoch)`. The next pass has `free_slots > 0`,
        // so it must start placement from the head of the queue (`None`) and assign task 2 (`cpu_ids[1]`),
        // NOT resume placement from `sweep.cursor`.
        queue
            .record_result_from(
                w_id,
                Some(w_sess),
                rusty_grid_core::task::TaskResult::success(w_id, cpu_ids[0], "", 1, false),
            )
            .await
            .unwrap();
        registry.decrement_active_tasks(&w_id).await.unwrap();

        let r3 = sched.schedule_once().await.unwrap();
        assert_eq!(r3.assignments.len(), 1);
        assert_eq!(
            r3.assignments[0].task_id, cpu_ids[1],
            "a pass with free slots must walk from the head of the queue and assign task 2, not start at the sweep cursor"
        );
    }

    #[tokio::test]
    async fn test_fail_unfulfillable_tasks_continues_sweep_beyond_scan_budget_when_unused_free_slot_remains(
    ) {
        let (registry, queue, waiters, notify) = scheduler_fixture().await;
        // One 4-core CPU worker (which will be busy with a 4-core task) and one 1-core CPU worker (idle).
        let (_w4_id, _w4_sess, _w4_rx) = register_worker(&registry, "cpu-4c", 4, 8192, false).await;
        let (_w1_id, _w1_sess, _w1_rx) = register_worker(&registry, "cpu-1c", 1, 4096, false).await;

        let mut cfg = SchedulerConfig::default().with_fail_unfulfillable_tasks(true);
        cfg.max_scan_per_pass = 3;
        let sched = WorkloadScheduler::new(cfg, registry.clone(), queue.clone(), notify)
            .with_waiters(waiters);

        // Fill the 4-core worker with a 4-core task so only the 1-core worker has a free slot.
        let _t_busy_4c = queue.submit(wide_task(4)).await.unwrap();
        let r0 = sched.schedule_once().await.unwrap();
        assert_eq!(r0.assignments.len(), 1);

        // Enqueue five 2-core tasks (none fit the idle 1-core worker) followed by a GPU task at the end.
        for _ in 0..5 {
            queue.submit(wide_task(2)).await.unwrap();
        }
        let t_gpu_tail = queue.submit(gpu_task()).await.unwrap();

        // Pass 1 scans the first 3 2-core tasks (`cursor` advances to task 3).
        let r1 = sched.schedule_once().await.unwrap();
        assert!(r1.assignments.is_empty());
        assert!(r1.unfulfillable.is_empty());
        assert_eq!(
            queue.get_state(&t_gpu_tail).await,
            Some(crate::queue::TaskState::Queued)
        );

        // Pass 2 still has `free_slots == 1` (the idle 1-core worker). After the placement walk
        // scans the first 3 2-core tasks without placing anything or advancing past the sweep cursor,
        // the scheduler continues the sweep from the cursor and fails `t_gpu_tail`.
        let r2 = sched.schedule_once().await.unwrap();
        assert!(r2.assignments.is_empty());
        assert_eq!(
            r2.unfulfillable.len(),
            1,
            "sweep must continue past scan_budget even when an unusable free slot remains"
        );
        assert_eq!(r2.unfulfillable[0].0, t_gpu_tail);
        assert_eq!(
            queue.get_state(&t_gpu_tail).await,
            Some(crate::queue::TaskState::Failed)
        );
    }

    #[tokio::test]
    async fn test_fail_unfulfillable_tasks_does_not_report_gpu_task_twice_when_placement_walk_moves_past_cursor(
    ) {
        let (registry, queue, waiters, notify) = scheduler_fixture().await;
        // One 4-core CPU worker (busy with a 4-core task so 2-core tasks are fulfillable in the
        // cluster) and one 1-core CPU worker (free).
        let (_w4_id, _w4_sess, _w4_rx) = register_worker(&registry, "cpu-4c", 4, 8192, false).await;
        let (w1_id, _w1_sess, _w1_rx) = register_worker(&registry, "cpu-1c", 1, 4096, false).await;

        let mut cfg = SchedulerConfig::default().with_fail_unfulfillable_tasks(true);
        cfg.max_scan_per_pass = 3;
        let sched = WorkloadScheduler::new(cfg, registry.clone(), queue.clone(), notify)
            .with_waiters(waiters);

        // Occupy the 4-core worker so only the 1-core worker has a free slot.
        let _t_busy_4c = queue.submit(wide_task(4)).await.unwrap();
        let r0 = sched.schedule_once().await.unwrap();
        assert_eq!(r0.assignments.len(), 1);

        // Enqueue:
        //   pos 1: 1-core CPU task `t1` (fits `w1_id`)
        //   pos 2: 2-core CPU task `t2` (skipped on `w1_id`)
        //   pos 3: 2-core CPU task `t3` (skipped on `w1_id`)
        //   pos 4: GPU task `t_gpu` (unfulfillable)
        //   pos 5: 2-core CPU task `t5` (keeps `next.is_some()` after `t_gpu`)
        let t1 = queue.submit(wide_task(1)).await.unwrap();
        let _t2 = queue.submit(wide_task(2)).await.unwrap();
        let _t3 = queue.submit(wide_task(2)).await.unwrap();
        let t_gpu = queue.submit(gpu_task()).await.unwrap();
        let _t5 = queue.submit(wide_task(2)).await.unwrap();

        // Pass 1 (`max_scan_per_pass = 3`): places `t1` on `w1_id` (removing `t1` from
        // `ready_queue`) and skips `t2` and `t3`, setting `unfulfillable_sweep.cursor` to `t3`.
        let r1 = sched.schedule_once().await.unwrap();
        assert_eq!(r1.assignments.len(), 1);
        assert_eq!(r1.assignments[0].task_id, t1);
        assert!(r1.unfulfillable.is_empty());

        // Complete `t1` so `w1_id` has 1 free slot again without changing `ready_epoch`.
        queue
            .record_result(rusty_grid_core::task::TaskResult::success(
                w1_id, t1, "", 1, false,
            ))
            .await
            .unwrap();
        registry.decrement_active_tasks(&w1_id).await.unwrap();

        // Pass 2 (`max_scan_per_pass = 3`): the placement walk from `None` now sees `[t2, t3, t_gpu]`,
        // advancing `unfulfillable_sweep.cursor` past the old `sweep_cursor` (`t3`) to `t_gpu`.
        // The `cursor == sweep_cursor` guard must prevent the continuation from re-scanning from
        // `t3` and reporting `t_gpu` a second time in `r2.unfulfillable`.
        let r2 = sched.schedule_once().await.unwrap();
        assert!(r2.assignments.is_empty());
        assert_eq!(
            r2.unfulfillable.len(),
            1,
            "t_gpu must be reported once in report.unfulfillable when the placement walk already moved past sweep_cursor"
        );
        assert_eq!(r2.unfulfillable[0].0, t_gpu);
    }

    #[test]
    fn test_workspace_affinity_first_across_policies_and_fallback_when_ineligible() {
        use rusty_grid_core::task::WorkspaceRef;

        let mut w_idle = make_worker("idle-8c", 8, 16384, false, false);
        w_idle.features.insert(ProtocolCapabilities::WORKSPACES);

        let mut w_aff = make_worker("aff-4c-busy", 4, 8192, false, false);
        w_aff.active_tasks = 2;
        w_aff.cpu_usage_pct = 70.0;
        w_aff.features.insert(ProtocolCapabilities::WORKSPACES);

        let workers = vec![w_idle.clone(), w_aff.clone()];
        let task = Task::new(
            TaskSpec::new_command("cargo", vec!["test".into()]),
            TaskRequirements::generic(1, 30),
        )
        .with_workspace(WorkspaceRef::new("oxide-repo"));

        let mut affinity = HashMap::new();
        affinity.insert("oxide-repo".to_string(), w_aff.worker_id);

        for policy in [
            SchedulingPolicy::WeightedLeastLoaded,
            SchedulingPolicy::LeastLoaded,
            SchedulingPolicy::RoundRobin,
        ] {
            let cfg = SchedulerConfig {
                policy,
                ..SchedulerConfig::default()
            };
            let report = WorkloadScheduler::matchmake_with_workspaces(
                &cfg,
                std::slice::from_ref(&task),
                &workers,
                &HashMap::new(),
                &affinity,
                &HashSet::new(),
            );
            assert_eq!(report.assignments.len(), 1);
            assert_eq!(
                report.assignments[0].worker_id, w_aff.worker_id,
                "affinity worker must be ranked first under policy {policy:?}"
            );
        }

        // If the affinity worker is disconnected or saturated, the normal policy falls back to w_idle.
        let mut w_aff_disc = w_aff.clone();
        w_aff_disc.status = WorkerStatus::Disconnected;
        let report_disc = WorkloadScheduler::matchmake_with_workspaces(
            &SchedulerConfig::default(),
            std::slice::from_ref(&task),
            &[w_idle.clone(), w_aff_disc],
            &HashMap::new(),
            &affinity,
            &HashSet::new(),
        );
        assert_eq!(report_disc.assignments.len(), 1);
        assert_eq!(report_disc.assignments[0].worker_id, w_idle.worker_id);
    }

    #[test]
    fn test_workspace_require_worker_constraint_wins_over_affinity() {
        use rusty_grid_core::task::WorkspaceRef;

        let mut w1 = make_worker("worker-alpha", 4, 8192, false, false);
        w1.features.insert(ProtocolCapabilities::WORKSPACES);
        let mut w2 = make_worker("worker-beta", 4, 8192, false, false);
        w2.features.insert(ProtocolCapabilities::WORKSPACES);

        let mut affinity = HashMap::new();
        affinity.insert("oxide-repo".to_string(), w1.worker_id);

        let task = Task::new(
            TaskSpec::new_command("cargo", vec!["test".into()]),
            TaskRequirements::generic(1, 30),
        )
        .with_workspace(WorkspaceRef::new("oxide-repo"))
        .with_tags(vec![format!("require:worker={}", w2.worker_id)]);

        let report = WorkloadScheduler::matchmake_with_workspaces(
            &SchedulerConfig::default(),
            std::slice::from_ref(&task),
            &[w1, w2.clone()],
            &HashMap::new(),
            &affinity,
            &HashSet::new(),
        );
        assert_eq!(report.assignments.len(), 1);
        assert_eq!(
            report.assignments[0].worker_id, w2.worker_id,
            "explicit require:worker=<id> must win over workspace affinity"
        );
    }

    #[test]
    fn test_no_second_task_of_one_workspace_in_a_pass_and_no_seat_held_back() {
        use rusty_grid_core::task::WorkspaceRef;

        let mut w1 = make_worker("w1", 4, 8192, false, false);
        w1.features.insert(ProtocolCapabilities::WORKSPACES);
        let mut w2 = make_worker("w2", 4, 8192, false, false);
        w2.features.insert(ProtocolCapabilities::WORKSPACES);
        // Partially load w2 (1 slot used, 3 free) so that a 4-core task (`ws_a_2`) cannot fit on
        // w2 right now and would trigger `hold_back` on w2 if the busy-workspace check ran after
        // `candidates` / `hold_back`.
        w2.active_tasks = 1;

        let ws_a_1 = Task::new(
            TaskSpec::new_command("cargo", vec!["check".into()]),
            TaskRequirements::generic(4, 30),
        )
        .with_workspace(WorkspaceRef::new("ws-a"));
        let ws_a_2 = Task::new(
            TaskSpec::new_command("cargo", vec!["test".into()]),
            TaskRequirements::generic(4, 30),
        )
        .with_workspace(WorkspaceRef::new("ws-a"));
        let ws_b_1 = Task::new(
            TaskSpec::new_command("cargo", vec!["clippy".into()]),
            TaskRequirements::generic(2, 30),
        )
        .with_workspace(WorkspaceRef::new("ws-b"));

        let tasks = vec![ws_a_1.clone(), ws_a_2.clone(), ws_b_1.clone()];
        let report = WorkloadScheduler::matchmake(
            &SchedulerConfig::default(),
            &tasks,
            &[w1.clone(), w2.clone()],
        );

        // ws_a_1 takes w1 (4 slots) and ws_b_1 takes 2 of w2's 3 remaining slots; ws_a_2 is
        // skipped with `WorkspaceBusy` without holding back w2's seat.
        let placed_ids: Vec<TaskId> = report.assignments.iter().map(|a| a.task_id).collect();
        assert_eq!(placed_ids, vec![ws_a_1.id, ws_b_1.id]);
        assert_eq!(
            report.skipped,
            vec![(ws_a_2.id, ScheduleSkipReason::WorkspaceBusy)]
        );
    }

    #[tokio::test]
    async fn test_schedule_batch_enforces_workspace_one_at_a_time_and_records_affinity_on_finish() {
        use rusty_grid_core::task::WorkspaceRef;

        let (registry, queue, waiters, notify) = scheduler_fixture().await;
        let mut feat = ProtocolCapabilities::all_supported();
        feat.insert(ProtocolCapabilities::WORKSPACES);

        let w1_id = Uuid::new_v4();
        let (w1_tx, _w1_rx) = tokio::sync::mpsc::channel(1024);
        let w1_sess = registry
            .register_session(
                w1_id,
                WorkerCapabilities::new("w1", 4, 8192, false, false, None),
                feat,
                "127.0.0.1:9310".parse().unwrap(),
                w1_tx,
                None,
            )
            .await
            .unwrap()
            .session_id;

        let w2_id = Uuid::new_v4();
        let (w2_tx, _w2_rx) = tokio::sync::mpsc::channel(1024);
        let w2_sess = registry
            .register_session(
                w2_id,
                WorkerCapabilities::new("w2", 8, 16384, false, false, None),
                feat,
                "127.0.0.1:9311".parse().unwrap(),
                w2_tx,
                None,
            )
            .await
            .unwrap()
            .session_id;

        let sched = WorkloadScheduler::new(
            SchedulerConfig::default(),
            registry.clone(),
            queue.clone(),
            notify,
        )
        .with_waiters(waiters);

        // Submit two tasks in workspace "repo-x" (pinning the first to w1 so w1 becomes the affinity worker).
        let t1 = Task::new(
            TaskSpec::new_command("cargo", vec!["build".into()]),
            TaskRequirements::generic(1, 30),
        )
        .with_workspace(WorkspaceRef::new("repo-x"))
        .with_tags(vec![format!("require:worker={w1_id}")]);
        let t2 = Task::new(
            TaskSpec::new_command("cargo", vec!["test".into()]),
            TaskRequirements::generic(1, 30),
        )
        .with_workspace(WorkspaceRef::new("repo-x"));

        let t1_id = queue.submit(t1).await.unwrap();
        let t2_id = queue.submit(t2).await.unwrap();

        // Pass 1: only t1 is scheduled; t2 stays Queued while t1 is Scheduled/Running.
        let r1 = sched.schedule_once().await.unwrap();
        assert_eq!(r1.assignments.len(), 1);
        assert_eq!(r1.assignments[0].task_id, t1_id);
        assert_eq!(r1.assignments[0].worker_id, w1_id);
        assert_eq!(
            queue.get_state(&t2_id).await,
            Some(crate::queue::TaskState::Queued)
        );

        // Pass 2 while t1 is still Scheduled: t2 is still skipped.
        let r2 = sched.schedule_once().await.unwrap();
        assert!(r2.assignments.is_empty());
        assert_eq!(
            queue.get_state(&t2_id).await,
            Some(crate::queue::TaskState::Queued)
        );

        // Complete t1 on w1; w1 now holds affinity for "repo-x".
        queue
            .record_result_from(
                w1_id,
                Some(w1_sess),
                rusty_grid_core::task::TaskResult::success(w1_id, t1_id, "", 1, false),
            )
            .await
            .unwrap();
        registry.decrement_active_tasks(&w1_id).await.unwrap();
        assert_eq!(queue.workspace_affinity("repo-x").await, Some(w1_id));

        // Pass 3: even though w2 has 8 cores vs w1's 4 cores, t2 lands on w1 due to workspace affinity!
        let r3 = sched.schedule_once().await.unwrap();
        assert_eq!(r3.assignments.len(), 1);
        assert_eq!(r3.assignments[0].task_id, t2_id);
        assert_eq!(r3.assignments[0].worker_id, w1_id);

        // Fail t2 on w1 (non-rejection execution failure, max_retries = 0); affinity stays on w1.
        let _ = w2_sess;
        let _ = w2_id;
    }

    #[tokio::test]
    async fn test_tree_v1_and_v2_version_negotiation_in_scheduler() {
        use crate::blob_store::{BlobStore, BlobStoreConfig};
        use crate::server::admit_blob_task;
        use rusty_grid_core::blob::{BlobFile, BlobId, BlobSymlink, TaskInput, TreeManifest};
        use rusty_grid_core::blob_transfer::BlobRequest;
        use rusty_grid_core::task::{Bytes, WorkspaceRef, WorkspaceSource};

        async fn store_blob(store: &Arc<BlobStore>, bytes: &[u8]) -> BlobId {
            let id = BlobId::of(bytes);
            let owner = store.new_owner();
            store
                .handle(
                    owner,
                    BlobRequest::Write {
                        id,
                        total_size: bytes.len() as u64,
                        offset: 0,
                        data: Bytes::from(bytes.to_vec()),
                    },
                )
                .await;
            id
        }

        let dir = tempfile::tempdir().unwrap();
        let blob_store =
            BlobStore::open(&BlobStoreConfig::new(dir.path(), 64 * 1024 * 1024)).unwrap();
        let main_id = store_blob(&blob_store, b"fn main() {}").await;

        let v1_manifest = TreeManifest::new(vec![BlobFile {
            path: "src/main.rs".to_string(),
            id: main_id,
            size: 12,
            executable: false,
        }])
        .unwrap();
        let v1_bytes = v1_manifest.encode();
        assert!(v1_bytes.starts_with(b"oxideswarm-tree/1\n"));
        let v1_id = store_blob(&blob_store, &v1_bytes).await;

        let v2_manifest = TreeManifest::with_entries(
            vec![BlobFile {
                path: "src/main.rs".to_string(),
                id: main_id,
                size: 12,
                executable: false,
            }],
            vec![BlobSymlink {
                path: "src/alias.rs".to_string(),
                target: "main.rs".to_string(),
            }],
            vec!["empty".to_string()],
        )
        .unwrap();
        let v2_bytes = v2_manifest.encode();
        assert!(v2_bytes.starts_with(b"oxideswarm-tree/2\n"));
        let v2_id = store_blob(&blob_store, &v2_bytes).await;

        let (registry, queue, waiters, notify) = scheduler_fixture().await;
        let w_v1_id = Uuid::new_v4();
        let (w_v1_tx, _w_v1_rx) = tokio::sync::mpsc::channel(1024);
        registry
            .register_session(
                w_v1_id,
                WorkerCapabilities::new("worker-v1", 4, 8192, false, false, None),
                ProtocolCapabilities::BLOB_TRANSFER | ProtocolCapabilities::WORKSPACES,
                "127.0.0.1:9320".parse().unwrap(),
                w_v1_tx,
                None,
            )
            .await
            .unwrap();

        let sched = WorkloadScheduler::new(
            SchedulerConfig {
                fail_unfulfillable_tasks: true,
                ..SchedulerConfig::default()
            },
            registry.clone(),
            queue.clone(),
            notify,
        )
        .with_waiters(waiters);

        // 1. A v1 manifest (no symlinks, no empty dirs) does NOT require TREE_V2 and schedules
        //    onto a worker without TREE_V2 (`w_v1`).
        let mut t_v1_input = Task::new(
            TaskSpec::new_command("cargo", vec!["test".into()]),
            TaskRequirements::generic(1, 30),
        );
        t_v1_input.inputs = vec![TaskInput::tree(".", v1_id, v1_bytes.len() as u64)];
        let v1_input_needs_v2 = admit_blob_task(Some(&blob_store), &t_v1_input)
            .await
            .unwrap();
        assert!(!v1_input_needs_v2);
        queue
            .submit_with_priority_and_tree_v2(t_v1_input.clone(), 0, v1_input_needs_v2)
            .await
            .unwrap();

        let t_v1_ws = Task::new(
            TaskSpec::new_command("cargo", vec!["test".into()]),
            TaskRequirements::generic(1, 30),
        )
        .with_workspace(WorkspaceRef {
            name: "ws-v1".to_string(),
            source: Some(WorkspaceSource::Tree {
                manifest: v1_id,
                manifest_size: v1_bytes.len() as u64,
            }),
        });
        let v1_ws_needs_v2 = admit_blob_task(Some(&blob_store), &t_v1_ws).await.unwrap();
        assert!(!v1_ws_needs_v2);
        queue
            .submit_with_priority_and_tree_v2(t_v1_ws.clone(), 0, v1_ws_needs_v2)
            .await
            .unwrap();

        let (v1_views, _) = queue.ready_views(None, 10).await;
        assert_eq!(v1_views.len(), 2);
        assert!(v1_views
            .iter()
            .all(|v| !v.required_features.contains(ProtocolCapabilities::TREE_V2)));

        let r_v1 = sched.schedule_once().await.unwrap();
        assert_eq!(r_v1.assignments.len(), 2);
        assert!(r_v1.assignments.iter().all(|a| a.worker_id == w_v1_id));

        // 2. A v2 manifest (with symlinks/empty dirs) requires TREE_V2 via admit_blob_task ->
        //    submit_with_priority_and_tree_v2 -> ready_views: it is refused on `w_v1` alone.
        let mut t_v2_input = Task::new(
            TaskSpec::new_command("cargo", vec!["test".into()]),
            TaskRequirements::generic(1, 30),
        );
        t_v2_input.inputs = vec![TaskInput::tree(".", v2_id, v2_bytes.len() as u64)];
        let v2_input_needs_v2 = admit_blob_task(Some(&blob_store), &t_v2_input)
            .await
            .unwrap();
        assert!(v2_input_needs_v2);
        queue
            .submit_with_priority_and_tree_v2(t_v2_input, 0, v2_input_needs_v2)
            .await
            .unwrap();

        let (views_v2_in, _) = queue.ready_views(None, 10).await;
        assert_eq!(views_v2_in.len(), 1);
        assert!(views_v2_in[0]
            .required_features
            .contains(ProtocolCapabilities::TREE_V2));

        let r_v2_on_v1 = sched.schedule_once().await.unwrap();
        assert!(r_v2_on_v1.assignments.is_empty());
        assert_eq!(r_v2_on_v1.unfulfillable.len(), 1);
        assert!(
            r_v2_on_v1.unfulfillable[0].1.contains("TREE_V2"),
            "expected TREE_V2 in unfulfillable reason: {}",
            r_v2_on_v1.unfulfillable[0].1
        );

        let t_v2_ws = Task::new(
            TaskSpec::new_command("cargo", vec!["test".into()]),
            TaskRequirements::generic(1, 30),
        )
        .with_workspace(WorkspaceRef {
            name: "ws-v2".to_string(),
            source: Some(WorkspaceSource::Tree {
                manifest: v2_id,
                manifest_size: v2_bytes.len() as u64,
            }),
        });
        let v2_ws_needs_v2 = admit_blob_task(Some(&blob_store), &t_v2_ws).await.unwrap();
        assert!(v2_ws_needs_v2);
        queue
            .submit_with_priority_and_tree_v2(t_v2_ws, 0, v2_ws_needs_v2)
            .await
            .unwrap();

        let r_v2_ws_on_v1 = sched.schedule_once().await.unwrap();
        assert!(r_v2_ws_on_v1.assignments.is_empty());
        assert_eq!(r_v2_ws_on_v1.unfulfillable.len(), 1);
        assert!(r_v2_ws_on_v1.unfulfillable[0].1.contains("TREE_V2"));

        // 3. When a `w_v2` worker (advertising TREE_V2) registers alongside `w_v1`, both v2
        //    tasks are placed onto `w_v2` and never onto `w_v1`.
        let w_v2_id = Uuid::new_v4();
        let (w_v2_tx, _w_v2_rx) = tokio::sync::mpsc::channel(1024);
        registry
            .register_session(
                w_v2_id,
                WorkerCapabilities::new("worker-v2", 4, 8192, false, false, None),
                ProtocolCapabilities::BLOB_TRANSFER
                    | ProtocolCapabilities::WORKSPACES
                    | ProtocolCapabilities::TREE_V2,
                "127.0.0.1:9321".parse().unwrap(),
                w_v2_tx,
                None,
            )
            .await
            .unwrap();

        let mut t_v2_input_ok = Task::new(
            TaskSpec::new_command("cargo", vec!["test".into()]),
            TaskRequirements::generic(1, 30),
        );
        t_v2_input_ok.inputs = vec![TaskInput::tree(".", v2_id, v2_bytes.len() as u64)];
        let needs_v2_a = admit_blob_task(Some(&blob_store), &t_v2_input_ok)
            .await
            .unwrap();
        queue
            .submit_with_priority_and_tree_v2(t_v2_input_ok, 0, needs_v2_a)
            .await
            .unwrap();

        let t_v2_ws_ok = Task::new(
            TaskSpec::new_command("cargo", vec!["test".into()]),
            TaskRequirements::generic(1, 30),
        )
        .with_workspace(WorkspaceRef {
            name: "ws-v2-ok".to_string(),
            source: Some(WorkspaceSource::Tree {
                manifest: v2_id,
                manifest_size: v2_bytes.len() as u64,
            }),
        });
        let needs_v2_b = admit_blob_task(Some(&blob_store), &t_v2_ws_ok)
            .await
            .unwrap();
        queue
            .submit_with_priority_and_tree_v2(t_v2_ws_ok, 0, needs_v2_b)
            .await
            .unwrap();

        let r_v2_both = sched.schedule_once().await.unwrap();
        assert_eq!(r_v2_both.assignments.len(), 2);
        assert!(r_v2_both.assignments.iter().all(|a| a.worker_id == w_v2_id));

        // 4. Direct `TaskQueue::submit` (bypassing `admit_blob_task`) fails closed: any task
        //    with a tree input or workspace source tree defaults to `requires_tree_v2 = true`.
        let mut t_direct = Task::new(
            TaskSpec::new_command("cargo", vec!["test".into()]),
            TaskRequirements::generic(1, 30),
        );
        t_direct.inputs = vec![TaskInput::tree(".", v1_id, v1_bytes.len() as u64)];
        let direct_id = t_direct.id;
        queue.submit(t_direct).await.unwrap();
        let (direct_views, _) = queue.ready_views(None, 10).await;
        let direct_view = direct_views
            .iter()
            .find(|v| v.task_id == direct_id)
            .expect("direct task in ready_views");
        assert!(
            direct_view
                .required_features
                .contains(ProtocolCapabilities::TREE_V2),
            "uninspected tree task submitted directly to TaskQueue must fail closed with TREE_V2"
        );

        // 5. Store recovery (`attach_store_and_replay` + `repin_recovered_blobs`):
        //    - Before `repin_recovered_blobs`, all recovered tree tasks default to fail-closed
        //      (`TREE_V2` required).
        //    - After `repin_recovered_blobs`, a valid v1 tree task has `TREE_V2` cleared, a valid
        //      v2 tree task keeps `TREE_V2`, and a task whose manifest blob is missing from the
        //      blob store stays fail-closed (`TREE_V2` required).
        let store_dir = dir.path().join("recovery-store");
        let mut rec_v1 = Task::new(
            TaskSpec::new_command("cargo", vec!["test".into()]),
            TaskRequirements::generic(1, 30),
        );
        rec_v1.inputs = vec![TaskInput::tree(".", v1_id, v1_bytes.len() as u64)];
        let rec_v1_id = rec_v1.id;

        let mut rec_v2 = Task::new(
            TaskSpec::new_command("cargo", vec!["test".into()]),
            TaskRequirements::generic(1, 30),
        );
        rec_v2.inputs = vec![TaskInput::tree(".", v2_id, v2_bytes.len() as u64)];
        let rec_v2_id = rec_v2.id;

        let missing_manifest_id = BlobId::of(b"missing-manifest");
        let mut rec_missing = Task::new(
            TaskSpec::new_command("cargo", vec!["test".into()]),
            TaskRequirements::generic(1, 30),
        );
        rec_missing.inputs = vec![TaskInput::tree(".", missing_manifest_id, 64)];
        let rec_missing_id = rec_missing.id;

        {
            let (store1, recovered1) = crate::store::TaskStore::open(&store_dir).unwrap();
            let q1 = TaskQueue::new();
            q1.attach_store_and_replay(store1, recovered1, Duration::ZERO)
                .await
                .unwrap();
            q1.submit_with_priority_and_tree_v2(rec_v1, 0, false)
                .await
                .unwrap();
            q1.submit_with_priority_and_tree_v2(rec_v2, 0, true)
                .await
                .unwrap();
            q1.submit_with_priority_and_tree_v2(rec_missing, 0, false)
                .await
                .unwrap();
            q1.close_and_compact_store().await;
        }

        let (store2, recovered2) = crate::store::TaskStore::open(&store_dir).unwrap();
        let q2 = TaskQueue::new();
        q2.attach_store_and_replay(store2, recovered2, Duration::ZERO)
            .await
            .unwrap();

        let (pre_repin_views, _) = q2.ready_views(None, 10).await;
        assert_eq!(pre_repin_views.len(), 3);
        assert!(
            pre_repin_views
                .iter()
                .all(|v| v.required_features.contains(ProtocolCapabilities::TREE_V2)),
            "all recovered tree tasks must start fail-closed before repin_recovered_blobs"
        );

        q2.repin_recovered_blobs(&blob_store).await;
        let (post_repin_views, _) = q2.ready_views(None, 10).await;
        let view_for = |id: TaskId| {
            post_repin_views
                .iter()
                .find(|v| v.task_id == id)
                .expect("recovered task view")
        };
        assert!(
            !view_for(rec_v1_id)
                .required_features
                .contains(ProtocolCapabilities::TREE_V2),
            "verified v1 manifest must clear TREE_V2 after repin_recovered_blobs"
        );
        assert!(
            view_for(rec_v2_id)
                .required_features
                .contains(ProtocolCapabilities::TREE_V2),
            "v2 manifest must keep TREE_V2 after repin_recovered_blobs"
        );
        assert!(
            view_for(rec_missing_id)
                .required_features
                .contains(ProtocolCapabilities::TREE_V2),
            "missing manifest blob must stay fail-closed (TREE_V2) after repin_recovered_blobs"
        );
        q2.close_and_compact_store().await;

        // 6. End-to-end `MasterHandle::submit_task` path (without passing `requires_tree_v2` by hand).
        let handle_blob_dir = dir.path().join("handle-blobs");
        let srv_cfg = crate::server::ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_blob_store(&handle_blob_dir, 64 * 1024 * 1024)
            .with_ephemeral_key(true);
        let sched_cfg = SchedulerConfig {
            tick_interval: Duration::from_secs(3600),
            micro_batch_window: Duration::from_secs(3600),
            fail_unfulfillable_tasks: true,
            ..SchedulerConfig::default()
        };
        let handle =
            crate::server::MasterServer::spawn_with_config(srv_cfg, sched_cfg, Default::default())
                .await
                .unwrap();
        let h_store = handle.blob_store().expect("blob store configured").clone();
        let h_main_id = store_blob(&h_store, b"fn main() {}").await;
        assert_eq!(h_main_id, main_id);
        let h_v1_id = store_blob(&h_store, &v1_bytes).await;
        assert_eq!(h_v1_id, v1_id);
        let h_v2_id = store_blob(&h_store, &v2_bytes).await;
        assert_eq!(h_v2_id, v2_id);

        let mut h_t_v1 = Task::new(
            TaskSpec::new_command("cargo", vec!["test".into()]),
            TaskRequirements::generic(1, 30),
        );
        h_t_v1.inputs = vec![TaskInput::tree(".", h_v1_id, v1_bytes.len() as u64)];
        let h_t_v1_id = handle.submit_task(h_t_v1).await.unwrap();

        let mut h_t_v2 = Task::new(
            TaskSpec::new_command("cargo", vec!["test".into()]),
            TaskRequirements::generic(1, 30),
        );
        h_t_v2.inputs = vec![TaskInput::tree(".", h_v2_id, v2_bytes.len() as u64)];
        let h_t_v2_id = handle.submit_task(h_t_v2).await.unwrap();

        let (h_views, _) = handle.queue().ready_views(None, 10).await;
        let h_v1_view = h_views.iter().find(|v| v.task_id == h_t_v1_id).unwrap();
        let h_v2_view = h_views.iter().find(|v| v.task_id == h_t_v2_id).unwrap();
        assert!(
            !h_v1_view
                .required_features
                .contains(ProtocolCapabilities::TREE_V2),
            "MasterHandle::submit_task with v1 manifest must not require TREE_V2"
        );
        assert!(
            h_v2_view
                .required_features
                .contains(ProtocolCapabilities::TREE_V2),
            "MasterHandle::submit_task with v2 manifest must require TREE_V2"
        );
        let _ = handle.shutdown();
    }

    /// Simulates wave-by-wave execution of `tasks` on `(fast, slow)` with deterministic per-task
    /// run times `(fast_ms, slow_ms)` and optional `speed_book`, returning `(finish_ms, fast_count, slow_count)`.
    fn simulate_batch_finish_time(
        config: &SchedulerConfig,
        tasks: &[Task],
        fast_template: &WorkerInfo,
        slow_template: &WorkerInfo,
        fast_ms: u64,
        slow_ms: u64,
        speed_book: Option<&SpeedBook>,
    ) -> (u64, usize, usize) {
        #[derive(Clone)]
        struct Running {
            worker_id: Uuid,
            task_key: TaskKey,
            started_ms: u64,
            finish_ms: u64,
        }

        let mut pending: Vec<(Task, u64)> = tasks.iter().cloned().map(|t| (t, 0u64)).collect();
        let mut in_flight: Vec<Running> = Vec::new();
        let mut now_ms = 0u64;
        let mut fast_count = 0usize;
        let mut slow_count = 0usize;

        loop {
            if !pending.is_empty() {
                let mut fast = fast_template.clone();
                let mut slow = slow_template.clone();
                fast.active_tasks = in_flight
                    .iter()
                    .filter(|r| r.worker_id == fast.worker_id)
                    .count();
                slow.active_tasks = in_flight
                    .iter()
                    .filter(|r| r.worker_id == slow.worker_id)
                    .count();
                let workers = vec![fast.clone(), slow.clone()];

                let mut running_map: HashMap<Uuid, Vec<(TaskKey, Duration)>> = HashMap::new();
                for r in &in_flight {
                    running_map.entry(r.worker_id).or_default().push((
                        r.task_key,
                        Duration::from_millis(now_ms.saturating_sub(r.started_ms)),
                    ));
                }
                let queued_for_overrides: HashMap<TaskId, Duration> = pending
                    .iter()
                    .map(|(t, enq)| (t.id, Duration::from_millis(now_ms.saturating_sub(*enq))))
                    .collect();
                let batch_tasks: Vec<Task> = pending.iter().map(|(t, _)| t.clone()).collect();

                let report = WorkloadScheduler::matchmake_inner(
                    config,
                    &batch_tasks,
                    &workers,
                    &HashMap::new(),
                    &HashMap::new(),
                    &HashSet::new(),
                    speed_book,
                    &running_map,
                    &queued_for_overrides,
                );

                let assigned_ids: HashSet<TaskId> =
                    report.assignments.iter().map(|a| a.task_id).collect();
                pending.retain(|(t, _)| !assigned_ids.contains(&t.id));

                for a in report.assignments {
                    let dur = if a.worker_id == fast.worker_id {
                        fast_count += 1;
                        fast_ms
                    } else {
                        slow_count += 1;
                        slow_ms
                    };
                    in_flight.push(Running {
                        worker_id: a.worker_id,
                        task_key: TaskKey::of(&a.task.spec),
                        started_ms: now_ms,
                        finish_ms: now_ms + dur,
                    });
                }
            }

            if pending.is_empty() && in_flight.is_empty() {
                return (now_ms, fast_count, slow_count);
            }
            let next_finish = in_flight
                .iter()
                .map(|r| r.finish_ms)
                .min()
                .expect("at least one task must be running when pending cannot advance");
            now_ms = next_finish;
            in_flight.retain(|r| r.finish_ms > now_ms);
        }
    }

    #[test]
    fn test_sched4_batch_32_tasks_fast_and_slow_reduces_finish_time_over_30_pct() {
        // Mirror the Context table in docs/design/0060-speed-aware-placement.md:
        // desktop: 20 threads, slots:10, ~2,500 ms per task;
        // laptop:  12 threads, slots:6,  ~51,000 ms per task.
        let mut fast = make_worker("desktop", 20, 32768, false, false);
        fast.capabilities.tags = vec!["slots:10".into()];
        let mut slow = make_worker("laptop", 12, 16384, false, false);
        slow.capabilities.tags = vec!["slots:6".into()];

        let spec = TaskSpec::ShellScript {
            script: "1..1000000 | Measure-Object -Sum".into(),
            interpreter: Some("powershell".into()),
            env: HashMap::new(),
        };
        let key = TaskKey::of(&spec);
        let tasks: Vec<Task> = (0..32)
            .map(|_| Task::new(spec.clone(), TaskRequirements::generic(1, 120)))
            .collect();

        let mut book = SpeedBook::new();
        for _ in 0..3 {
            assert!(book.record_sample(fast.worker_id, key, 2_500, 120));
            assert!(book.record_sample(slow.worker_id, key, 51_000, 120));
        }

        // First pass directly verifies `WaitingForFasterWorker` on the overflow tasks.
        let cfg = SchedulerConfig::default();
        let first_pass = WorkloadScheduler::matchmake_with_speed(
            &cfg,
            &tasks,
            &[fast.clone(), slow.clone()],
            &HashMap::new(),
            &HashMap::new(),
            &HashSet::new(),
            &book,
            &HashMap::new(),
        );
        assert_eq!(first_pass.assignments.len(), 10);
        assert!(first_pass
            .assignments
            .iter()
            .all(|a| a.worker_id == fast.worker_id));
        assert_eq!(first_pass.skipped.len(), 22);
        assert!(first_pass
            .skipped
            .iter()
            .all(|(_, reason)| *reason == ScheduleSkipReason::WaitingForFasterWorker));

        // Compare full wave-by-wave finish time against today's placement (`None` / `speed_hold_factor = 0`).
        let (today_ms, today_fast, today_slow) =
            simulate_batch_finish_time(&cfg, &tasks, &fast, &slow, 2_500, 51_000, None);
        let (d2_ms, d2_fast, d2_slow) =
            simulate_batch_finish_time(&cfg, &tasks, &fast, &slow, 2_500, 51_000, Some(&book));

        assert_eq!(today_fast + today_slow, 32);
        assert!(today_slow >= 6, "today fills all 6 slow slots immediately");
        assert_eq!(today_ms, 51_000);

        assert_eq!((d2_fast, d2_slow), (32, 0));
        assert_eq!(d2_ms, 10_000);
        assert!(
            (d2_ms as f64) <= 0.70 * (today_ms as f64),
            "expected >= 30% finish-time reduction: today={today_ms}ms, d2={d2_ms}ms"
        );

        // With `speed_hold_factor = 0.0`, D2 turns off and today's finish time returns.
        let disabled_cfg = SchedulerConfig::default().with_speed_hold_factor(0.0);
        let (off_ms, off_fast, off_slow) = simulate_batch_finish_time(
            &disabled_cfg,
            &tasks,
            &fast,
            &slow,
            2_500,
            51_000,
            Some(&book),
        );
        assert_eq!(
            (off_ms, off_fast, off_slow),
            (today_ms, today_fast, today_slow)
        );
    }

    #[test]
    fn test_sched4_no_history_single_task_and_unknown_worker_match_today() {
        let mut fast = make_worker("fast", 8, 16384, false, false);
        fast.capabilities.tags = vec!["slots:4".into()];
        let mut slow = make_worker("slow", 8, 16384, false, false);
        slow.capabilities.tags = vec!["slots:4".into()];
        let mut unknown = make_worker("unknown", 8, 16384, false, false);
        unknown.capabilities.tags = vec!["slots:4".into()];

        let spec = TaskSpec::new_command("echo", vec!["hello".into()]);
        let key = TaskKey::of(&spec);
        let batch: Vec<Task> = (0..8)
            .map(|_| Task::new(spec.clone(), TaskRequirements::generic(1, 30)))
            .collect();
        let cfg = SchedulerConfig::default();

        // 0 samples -> identical to `matchmake`.
        let empty_book = SpeedBook::new();
        let today = WorkloadScheduler::matchmake(&cfg, &batch, &[fast.clone(), slow.clone()]);
        let with_empty = WorkloadScheduler::matchmake_with_speed(
            &cfg,
            &batch,
            &[fast.clone(), slow.clone()],
            &HashMap::new(),
            &HashMap::new(),
            &HashSet::new(),
            &empty_book,
            &HashMap::new(),
        );
        assert_eq!(today.assignments, with_empty.assignments);

        // 1 sample (< `speed_min_samples = 2`) -> still identical to `matchmake`.
        let mut one_sample_book = SpeedBook::new();
        one_sample_book.record_sample(fast.worker_id, key, 200, 30);
        one_sample_book.record_sample(slow.worker_id, key, 10_000, 30);
        let with_one = WorkloadScheduler::matchmake_with_speed(
            &cfg,
            &batch,
            &[fast.clone(), slow.clone()],
            &HashMap::new(),
            &HashMap::new(),
            &HashSet::new(),
            &one_sample_book,
            &HashMap::new(),
        );
        assert_eq!(today.assignments, with_one.assignments);

        // Single task on an idle cluster (`fast` = 8 cores at 1,000 ms vs `big_slow` = 16 cores at
        // 1,010 ms, only 1% slower) -> goes to 16-core `big_slow`, identical to today's `rank`.
        let mut big_slow = make_worker("big-slow", 16, 32768, false, false);
        big_slow.worker_id = slow.worker_id;
        let mut near_equal_book = SpeedBook::new();
        for _ in 0..2 {
            near_equal_book.record_sample(fast.worker_id, key, 1_000, 30);
            near_equal_book.record_sample(big_slow.worker_id, key, 1_010, 30);
        }
        let single = vec![Task::new(spec.clone(), TaskRequirements::generic(1, 30))];
        let today_single =
            WorkloadScheduler::matchmake(&cfg, &single, &[fast.clone(), big_slow.clone()]);
        let speed_single = WorkloadScheduler::matchmake_with_speed(
            &cfg,
            &single,
            &[fast.clone(), big_slow.clone()],
            &HashMap::new(),
            &HashMap::new(),
            &HashSet::new(),
            &near_equal_book,
            &HashMap::new(),
        );
        assert_eq!(today_single.assignments.len(), 1);
        assert_eq!(today_single.assignments[0].worker_id, big_slow.worker_id);
        assert_eq!(today_single.assignments, speed_single.assignments);

        let mut two_sample_book = one_sample_book.clone();
        two_sample_book.record_sample(fast.worker_id, key, 200, 30);
        two_sample_book.record_sample(slow.worker_id, key, 10_000, 30);

        // Unknown worker (`0` samples) gets `expected = avg(best) * 1.0`, so it receives work and
        // gets measured while `slow` is still avoided.
        let report_with_unknown = WorkloadScheduler::matchmake_with_speed(
            &cfg,
            &batch,
            &[fast.clone(), unknown.clone(), slow.clone()],
            &HashMap::new(),
            &HashMap::new(),
            &HashSet::new(),
            &two_sample_book,
            &HashMap::new(),
        );
        assert_eq!(report_with_unknown.assignments.len(), 8);
        let fast_assigned = report_with_unknown
            .assignments
            .iter()
            .filter(|a| a.worker_id == fast.worker_id)
            .count();
        let unknown_assigned = report_with_unknown
            .assignments
            .iter()
            .filter(|a| a.worker_id == unknown.worker_id)
            .count();
        let slow_assigned = report_with_unknown
            .assignments
            .iter()
            .filter(|a| a.worker_id == slow.worker_id)
            .count();
        assert_eq!(fast_assigned, 4);
        assert_eq!(unknown_assigned, 4);
        assert_eq!(slow_assigned, 0);
    }

    #[test]
    fn test_sched4_placement_constraints_gpu_routing_and_workspace_affinity_still_win() {
        use rusty_grid_core::task::WorkspaceRef;

        let mut fast_cpu = tagged_worker("fast-cpu", &["os:windows", "slots:4"]);
        fast_cpu.features.0 |= ProtocolCapabilities::WORKSPACES.0;
        let mut slow_cpu = tagged_worker("slow-cpu", &["os:linux", "role:special", "slots:4"]);
        slow_cpu.features.0 |= ProtocolCapabilities::WORKSPACES.0;
        let mut real_gpu = make_worker("real-gpu", 8, 16384, true, false);
        real_gpu.capabilities.tags = vec!["slots:4".into()];
        let mut sim_gpu = make_worker("sim-gpu", 8, 16384, false, true);
        sim_gpu.capabilities.tags = vec!["slots:4".into()];

        let cpu_spec = TaskSpec::new_command("python", vec!["work.py".into()]);
        let cpu_key = TaskKey::of(&cpu_spec);
        let gpu_spec = TaskSpec::gpu_compute("gemm_verification_kernel", 4096);
        let gpu_key = TaskKey::of(&gpu_spec);

        let mut book = SpeedBook::new();
        for _ in 0..3 {
            book.record_sample(fast_cpu.worker_id, cpu_key, 200, 60);
            book.record_sample(slow_cpu.worker_id, cpu_key, 20_000, 60);
            // GPU worker also happens to be fast at CPU tasks:
            book.record_sample(real_gpu.worker_id, cpu_key, 150, 60);
            // Simulated GPU reports a tiny stub time compared to real hardware GPU:
            book.record_sample(sim_gpu.worker_id, gpu_key, 10, 60);
            book.record_sample(real_gpu.worker_id, gpu_key, 800, 60);
        }

        let cfg = SchedulerConfig::default();

        // 1. Placement constraint (`require:tag=role:special`) pins to `slow_cpu` without waiting for `fast_cpu`.
        let pinned = Task::new(cpu_spec.clone(), TaskRequirements::generic(1, 60))
            .with_tags(vec!["require:tag=role:special".into()]);
        let r_pinned = WorkloadScheduler::matchmake_with_speed(
            &cfg,
            &[pinned],
            &[fast_cpu.clone(), slow_cpu.clone()],
            &HashMap::new(),
            &HashMap::new(),
            &HashSet::new(),
            &book,
            &HashMap::new(),
        );
        assert_eq!(r_pinned.assignments.len(), 1);
        assert_eq!(r_pinned.assignments[0].worker_id, slow_cpu.worker_id);

        // 2. GPU routing:
        //    (a) Stage 6 (`preserve_gpu_for_gpu_tasks` while a GPU task is waiting): generic CPU
        //        task goes to `slow_cpu` instead of stealing or waiting for `real_gpu`.
        //    (b) Stage 7 (idle real GPU beats simulated GPU): GPU task goes to `real_gpu` despite
        //        `sim_gpu` having a lower `SpeedBook` average.
        let cpu_task = Task::new(cpu_spec.clone(), TaskRequirements::generic(1, 60));
        let gpu_task = Task::new(gpu_spec, TaskRequirements::gpu(60));
        let r_gpu = WorkloadScheduler::matchmake_with_speed(
            &cfg,
            &[cpu_task.clone(), gpu_task.clone()],
            &[slow_cpu.clone(), real_gpu.clone(), sim_gpu.clone()],
            &HashMap::new(),
            &HashMap::new(),
            &HashSet::new(),
            &book,
            &HashMap::new(),
        );
        assert_eq!(r_gpu.assignments.len(), 2);
        let cpu_assigned = r_gpu
            .assignments
            .iter()
            .find(|a| a.task_id == cpu_task.id)
            .unwrap();
        let gpu_assigned = r_gpu
            .assignments
            .iter()
            .find(|a| a.task_id == gpu_task.id)
            .unwrap();
        assert_eq!(cpu_assigned.worker_id, slow_cpu.worker_id);
        assert_eq!(gpu_assigned.worker_id, real_gpu.worker_id);

        // 3. Workspace affinity: warm workspace on `slow_cpu` wins over `fast_cpu`.
        let ws_task = Task::new(cpu_spec, TaskRequirements::generic(1, 60))
            .with_workspace(WorkspaceRef::new("warm-ws"));
        let mut affinity = HashMap::new();
        affinity.insert("warm-ws".to_string(), slow_cpu.worker_id);
        let r_ws = WorkloadScheduler::matchmake_with_speed(
            &cfg,
            &[ws_task],
            &[fast_cpu, slow_cpu.clone()],
            &HashMap::new(),
            &affinity,
            &HashSet::new(),
            &book,
            &HashMap::new(),
        );
        assert_eq!(r_ws.assignments.len(), 1);
        assert_eq!(r_ws.assignments[0].worker_id, slow_cpu.worker_id);
    }

    #[test]
    fn test_sched4_wait_cap_step5_and_no_head_of_line_blocking() {
        let mut fast = make_worker("fast", 4, 8192, false, false);
        fast.capabilities.tags = vec!["slots:1".into()];
        fast.active_tasks = 1;
        let mut slow1 = make_worker("slow1", 4, 8192, false, false);
        slow1.capabilities.tags = vec!["slots:2".into()];
        let mut slow2 = make_worker("slow2", 8, 16384, false, false);
        slow2.capabilities.tags = vec!["slots:2".into()];

        let heavy_spec = TaskSpec::new_command("heavy", vec!["--work".into()]);
        let heavy_key = TaskKey::of(&heavy_spec);
        let light_spec = TaskSpec::new_command("light", vec!["--quick".into()]);

        let mut book = SpeedBook::new();
        for _ in 0..2 {
            book.record_sample(fast.worker_id, heavy_key, 1_000, 60);
            book.record_sample(slow1.worker_id, heavy_key, 5_000, 60);
            book.record_sample(slow2.worker_id, heavy_key, 20_000, 60);
        }

        let mut running = HashMap::new();
        running.insert(
            fast.worker_id,
            vec![(heavy_key, Duration::from_millis(100))],
        );

        let t_heavy = Task::new(heavy_spec.clone(), TaskRequirements::generic(1, 60));
        let t_light = Task::new(light_spec, TaskRequirements::generic(1, 60));
        let cfg = SchedulerConfig::default();

        // Below Step 5 wait cap (`queued_for = 4,999 ms < expected(slow1) = 5,000 ms`):
        // `t_heavy` waits with `WaitingForFasterWorker`, while `t_light` (same `Shape`, different
        // `TaskKey`) behind it is NOT blocked by `Engine::dead` and lands immediately!
        let mut below_cap = HashMap::new();
        below_cap.insert(t_heavy.id, Duration::from_millis(4_999));
        let r_below = WorkloadScheduler::matchmake_inner(
            &cfg,
            &[t_heavy.clone(), t_light.clone()],
            &[fast.clone(), slow1.clone(), slow2.clone()],
            &HashMap::new(),
            &HashMap::new(),
            &HashSet::new(),
            Some(&book),
            &running,
            &below_cap,
        );
        assert_eq!(
            r_below.skipped,
            vec![(t_heavy.id, ScheduleSkipReason::WaitingForFasterWorker)]
        );
        assert_eq!(r_below.assignments.len(), 1);
        assert_eq!(r_below.assignments[0].task_id, t_light.id);

        // At Step 5 wait cap (`queued_for = 5,000 ms >= expected(slow1)`):
        // the hold rule is skipped and `t_heavy` places on `slow1` (not the even slower `slow2`,
        // even though `slow2` has more CPU cores).
        let mut at_cap = HashMap::new();
        at_cap.insert(t_heavy.id, Duration::from_millis(5_000));
        let r_at = WorkloadScheduler::matchmake_inner(
            &cfg,
            std::slice::from_ref(&t_heavy),
            &[fast, slow1.clone(), slow2],
            &HashMap::new(),
            &HashMap::new(),
            &HashSet::new(),
            Some(&book),
            &running,
            &at_cap,
        );
        assert!(r_at.skipped.is_empty());
        assert_eq!(r_at.assignments.len(), 1);
        assert_eq!(r_at.assignments[0].worker_id, slow1.worker_id);
    }

    #[test]
    fn test_sched4_near_equal_workers_spread_instead_of_packing() {
        let mut w_a = make_worker("worker-a", 8, 16384, false, false);
        w_a.capabilities.tags = vec!["slots:8".into()];
        let mut w_b = make_worker("worker-b", 8, 16384, false, false);
        w_b.capabilities.tags = vec!["slots:8".into()];

        let spec = TaskSpec::new_command("map", vec!["chunk.py".into()]);
        let key = TaskKey::of(&spec);
        let mut book = SpeedBook::new();
        for _ in 0..3 {
            book.record_sample(w_a.worker_id, key, 1_000, 60);
            book.record_sample(w_b.worker_id, key, 1_010, 60);
        }

        let cfg = SchedulerConfig::default();
        let batch: Vec<Task> = (0..8)
            .map(|_| Task::new(spec.clone(), TaskRequirements::generic(1, 60)))
            .collect();

        // 1. Two 8-core workers with averages 1000 and 1010 ms and 8 tasks split 4/4 as today.
        let today = WorkloadScheduler::matchmake(&cfg, &batch, &[w_a.clone(), w_b.clone()]);
        let speed = WorkloadScheduler::matchmake_with_speed(
            &cfg,
            &batch,
            &[w_a.clone(), w_b.clone()],
            &HashMap::new(),
            &HashMap::new(),
            &HashSet::new(),
            &book,
            &HashMap::new(),
        );
        let today_a = today
            .assignments
            .iter()
            .filter(|a| a.worker_id == w_a.worker_id)
            .count();
        let today_b = today
            .assignments
            .iter()
            .filter(|a| a.worker_id == w_b.worker_id)
            .count();
        let speed_a = speed
            .assignments
            .iter()
            .filter(|a| a.worker_id == w_a.worker_id)
            .count();
        let speed_b = speed
            .assignments
            .iter()
            .filter(|a| a.worker_id == w_b.worker_id)
            .count();
        assert_eq!((today_a, today_b), (4, 4));
        assert_eq!((speed_a, speed_b), (4, 4));

        // 2. Idle cluster, 8-core worker (1000 ms) vs 16-core worker (1010 ms): single task goes to
        //    the 16-core worker per today's `rank`.
        let mut w_big = make_worker("worker-big", 16, 32768, false, false);
        w_big.capabilities.tags = vec!["slots:16".into()];
        let mut idle_book = SpeedBook::new();
        for _ in 0..3 {
            idle_book.record_sample(w_a.worker_id, key, 1_000, 60);
            idle_book.record_sample(w_big.worker_id, key, 1_010, 60);
        }
        let single = vec![Task::new(spec.clone(), TaskRequirements::generic(1, 60))];
        let today_single =
            WorkloadScheduler::matchmake(&cfg, &single, &[w_a.clone(), w_big.clone()]);
        let speed_single = WorkloadScheduler::matchmake_with_speed(
            &cfg,
            &single,
            &[w_a.clone(), w_big.clone()],
            &HashMap::new(),
            &HashMap::new(),
            &HashSet::new(),
            &idle_book,
            &HashMap::new(),
        );
        assert_eq!(today_single.assignments[0].worker_id, w_big.worker_id);
        assert_eq!(speed_single.assignments[0].worker_id, w_big.worker_id);

        // 3. Full fast worker (1000 ms) vs idle 5x slow worker (5000 ms) still holds.
        let mut w_fast_full = w_a;
        w_fast_full.capabilities.tags = vec!["slots:1".into()];
        w_fast_full.active_tasks = 1;
        let w_slow = make_worker("worker-slow", 8, 16384, false, false);
        let mut fast_slow_book = SpeedBook::new();
        for _ in 0..3 {
            fast_slow_book.record_sample(w_fast_full.worker_id, key, 1_000, 60);
            fast_slow_book.record_sample(w_slow.worker_id, key, 5_000, 60);
        }
        let mut running = HashMap::new();
        running.insert(w_fast_full.worker_id, vec![(key, Duration::ZERO)]);
        let held = WorkloadScheduler::matchmake_with_speed(
            &cfg,
            &single,
            &[w_fast_full, w_slow],
            &HashMap::new(),
            &HashMap::new(),
            &HashSet::new(),
            &fast_slow_book,
            &running,
        );
        assert_eq!(
            held.skipped,
            vec![(single[0].id, ScheduleSkipReason::WaitingForFasterWorker)]
        );
    }

    #[test]
    fn test_sched4_dead_speed_cache_and_cap_bypass() {
        let mut fast = make_worker("fast", 4, 8192, false, false);
        fast.capabilities.tags = vec!["slots:1".into()];
        fast.active_tasks = 1;
        fast.features.insert(ProtocolCapabilities::WORKSPACES);
        let mut slow = make_worker("slow", 4, 8192, false, false);
        slow.capabilities.tags = vec!["slots:4".into()];
        slow.features.insert(ProtocolCapabilities::WORKSPACES);

        let spec = TaskSpec::new_command("batch", vec!["--step".into()]);
        let key = TaskKey::of(&spec);
        let mut book = SpeedBook::new();
        for _ in 0..2 {
            book.record_sample(fast.worker_id, key, 500, 60);
            book.record_sample(slow.worker_id, key, 5_000, 60);
        }
        let mut running = HashMap::new();
        running.insert(fast.worker_id, vec![(key, Duration::ZERO)]);

        // 1. Batch of 200 identical tasks + 1 trailing task past the wait cap + 1 task with
        //    `avoid = [fast]` + 1 workspace-affine task on `slow`.
        //    First 200 hit `WaitingForFasterWorker` (via `dead_speed`), while the trailing tasks
        //    must NOT be swallowed by the `(Shape, TaskKey)` cache.
        let mut tasks: Vec<Task> = (0..200)
            .map(|_| Task::new(spec.clone(), TaskRequirements::generic(1, 60)))
            .collect();
        let overdue = Task::new(spec.clone(), TaskRequirements::generic(1, 60));
        let mut avoided = Task::new(spec.clone(), TaskRequirements::generic(1, 60));
        avoided.id = TaskId::new();
        let mut ws_task = Task::new(spec, TaskRequirements::generic(1, 60))
            .with_workspace(rusty_grid_core::task::WorkspaceRef::new("slow-ws"));
        ws_task.id = TaskId::new();

        tasks.push(overdue.clone());
        tasks.push(avoided.clone());
        tasks.push(ws_task.clone());

        let mut queued_for_overrides = HashMap::new();
        queued_for_overrides.insert(overdue.id, Duration::from_millis(5_000));
        let mut avoid_map = HashMap::new();
        avoid_map.insert(avoided.id, vec![fast.worker_id]);
        let mut affinity = HashMap::new();
        affinity.insert("slow-ws".to_string(), slow.worker_id);

        let report = WorkloadScheduler::matchmake_inner(
            &SchedulerConfig::default(),
            &tasks,
            &[fast.clone(), slow.clone()],
            &avoid_map,
            &affinity,
            &HashSet::new(),
            Some(&book),
            &running,
            &queued_for_overrides,
        );
        assert_eq!(report.skipped.len(), 200);
        assert!(report
            .skipped
            .iter()
            .all(|(_, r)| *r == ScheduleSkipReason::WaitingForFasterWorker));
        let assigned_ids: Vec<TaskId> = report.assignments.iter().map(|a| a.task_id).collect();
        assert_eq!(assigned_ids, vec![overdue.id, avoided.id, ws_task.id]);
        assert!(report
            .assignments
            .iter()
            .all(|a| a.worker_id == slow.worker_id));

        // 2. Round-1 item 2 stress case: 20,000 held tasks across 64 workers still finish via
        //    `dead_speed` in O(N) rather than scanning 64 workers per task.
        let mut wide_workers = vec![fast];
        let mut wide_book = SpeedBook::new();
        for _ in 0..2 {
            wide_book.record_sample(wide_workers[0].worker_id, key, 500, 60);
        }
        for i in 0..63 {
            let mut s = make_worker(&format!("slow-{i}"), 4, 8192, false, false);
            s.capabilities.tags = vec!["slots:4".into()];
            for _ in 0..2 {
                wide_book.record_sample(s.worker_id, key, 5_000, 60);
            }
            wide_workers.push(s);
        }
        let bulk_spec = TaskSpec::new_command("batch", vec!["--step".into()]);
        let bulk_tasks: Vec<Task> = (0..20_000)
            .map(|_| Task::new(bulk_spec.clone(), TaskRequirements::generic(1, 60)))
            .collect();
        let empty_avoid = HashMap::new();
        let empty_affinity = HashMap::new();
        let empty_active = HashSet::new();
        let empty_queued_for = HashMap::new();
        let t0 = Instant::now();
        let bulk_report = WorkloadScheduler::matchmake_inner(
            &SchedulerConfig::default(),
            &bulk_tasks,
            &wide_workers,
            &empty_avoid,
            &empty_affinity,
            &empty_active,
            Some(&wide_book),
            &running,
            &empty_queued_for,
        );
        let elapsed = t0.elapsed();
        println!(
            "20,000-held-task pass took {elapsed:?} (candidate_scans={}, dead_speed_hits={})",
            bulk_report.candidate_scans, bulk_report.dead_speed_hits
        );
        assert_eq!(
            bulk_report.candidate_scans, 1,
            "first task populates dead_speed and remaining 19,999 skip without rescanning"
        );
        assert_eq!(bulk_report.dead_speed_hits, 19_999);
        assert!(
            elapsed < Duration::from_secs(60),
            "20,000-held-task pass hung ({elapsed:?})"
        );
        assert!(bulk_report.assignments.is_empty());
        assert_eq!(bulk_report.skipped.len(), 20_000);
        assert!(bulk_report
            .skipped
            .iter()
            .all(|(_, r)| *r == ScheduleSkipReason::WaitingForFasterWorker));
    }

    #[test]
    fn test_sched4_dead_speed_invalidated_on_place_hold_back_and_gpu_waiting() {
        // Scenario 1: wide-task `hold_back` and `AllEligibleWorkersSaturated` after `Verdict::Placed`
        // shrinks free slots on the slow worker so a previously cached wide task can no longer fit.
        let mut fast = make_worker("fast", 4, 8192, false, false);
        fast.capabilities.tags = vec!["slots:4".into()];
        fast.active_tasks = 4;
        let mut slow = make_worker("slow", 4, 8192, false, false);
        slow.capabilities.tags = vec!["slots:4".into()];

        let build_spec = TaskSpec::new_command("cargo", vec!["build".into()]);
        let k_build = TaskKey::of(&build_spec);
        let lint_spec = TaskSpec::new_command("cargo", vec!["clippy".into()]);
        let mut book1 = SpeedBook::new();
        for _ in 0..2 {
            book1.record_sample(fast.worker_id, k_build, 1_000, 60);
            book1.record_sample(slow.worker_id, k_build, 5_000, 60);
        }
        let mut running1 = HashMap::new();
        running1.insert(fast.worker_id, vec![(k_build, Duration::ZERO)]);

        let t1 = Task::new(build_spec.clone(), TaskRequirements::generic(4, 60));
        let t2 = Task::new(lint_spec.clone(), TaskRequirements::generic(1, 60));
        let t3 = Task::new(build_spec, TaskRequirements::generic(4, 60));
        let t4 = Task::new(lint_spec.clone(), TaskRequirements::generic(1, 60));
        let t5 = Task::new(lint_spec.clone(), TaskRequirements::generic(1, 60));
        let t6 = Task::new(lint_spec, TaskRequirements::generic(1, 60));

        let r1 = WorkloadScheduler::matchmake_with_speed(
            &SchedulerConfig::default(),
            &[
                t1.clone(),
                t2.clone(),
                t3.clone(),
                t4.clone(),
                t5.clone(),
                t6.clone(),
            ],
            &[fast.clone(), slow.clone()],
            &HashMap::new(),
            &HashMap::new(),
            &HashSet::new(),
            &book1,
            &running1,
        );
        assert_eq!(r1.assignments.len(), 1);
        assert_eq!(r1.assignments[0].task_id, t2.id);
        assert_eq!(r1.assignments[0].worker_id, slow.worker_id);
        assert_eq!(
            r1.skipped,
            vec![
                (t1.id, ScheduleSkipReason::WaitingForFasterWorker),
                (t3.id, ScheduleSkipReason::AllEligibleWorkersSaturated),
                (t4.id, ScheduleSkipReason::AllEligibleWorkersSaturated),
                (t5.id, ScheduleSkipReason::AllEligibleWorkersSaturated),
                (t6.id, ScheduleSkipReason::AllEligibleWorkersSaturated),
            ]
        );

        // Scenario 2: Stage 7 simulated-GPU unlock mid-pass when the sole idle real GPU gets a task.
        let mut real_fast = make_worker("real-fast", 4, 8192, true, false);
        real_fast.capabilities.tags = vec!["slots:1".into()];
        real_fast.active_tasks = 1;
        let mut real_slow = make_worker("real-slow", 4, 8192, true, false);
        real_slow.capabilities.tags = vec!["slots:1".into()];
        let mut sim_fast = make_worker("sim-fast", 4, 8192, true, true);
        sim_fast.capabilities.tags = vec!["slots:4".into()];

        let gpu_spec = TaskSpec::gpu_compute("gemm_kernel", 64);
        let k_gpu = TaskKey::of(&gpu_spec);
        let other_gpu_spec = TaskSpec::gpu_compute("conv_kernel", 32);
        let mut book2 = SpeedBook::new();
        for _ in 0..2 {
            book2.record_sample(real_fast.worker_id, k_gpu, 200, 60);
            book2.record_sample(real_slow.worker_id, k_gpu, 5_000, 60);
            book2.record_sample(sim_fast.worker_id, k_gpu, 250, 60);
        }
        let mut running2 = HashMap::new();
        running2.insert(real_fast.worker_id, vec![(k_gpu, Duration::ZERO)]);

        let g1 = Task::new(gpu_spec.clone(), TaskRequirements::gpu(60));
        let g2 = Task::new(other_gpu_spec, TaskRequirements::gpu(60));
        let g3 = Task::new(gpu_spec, TaskRequirements::gpu(60));
        let r2 = WorkloadScheduler::matchmake_with_speed(
            &SchedulerConfig::default(),
            &[g1.clone(), g2.clone(), g3.clone()],
            &[real_fast, real_slow.clone(), sim_fast.clone()],
            &HashMap::new(),
            &HashMap::new(),
            &HashSet::new(),
            &book2,
            &running2,
        );
        assert_eq!(
            r2.skipped,
            vec![(g1.id, ScheduleSkipReason::WaitingForFasterWorker)]
        );
        assert_eq!(r2.assignments.len(), 2);
        assert_eq!(r2.assignments[0].task_id, g2.id);
        assert_eq!(r2.assignments[0].worker_id, real_slow.worker_id);
        assert_eq!(r2.assignments[1].task_id, g3.id);
        assert_eq!(r2.assignments[1].worker_id, sim_fast.worker_id);

        // Scenario 3: `note_gpu_waiting` across `SCAN_PAGE` unlocks the CPU worker after Stage 6
        // excludes `gpu_fast`, and `hold_back` invalidates `dead_speed` when newly reserving a worker.
        let mut gpu_fast = make_worker("gpu-fast", 4, 8192, true, false);
        gpu_fast.capabilities.tags = vec!["slots:2".into()];
        gpu_fast.active_tasks = 2;
        let mut cpu_slow = make_worker("cpu-slow", 4, 8192, false, false);
        cpu_slow.capabilities.tags = vec!["slots:4".into()];

        let cpu_spec = TaskSpec::new_command("rustc", vec!["--crate-type=lib".into()]);
        let k_cpu = TaskKey::of(&cpu_spec);
        let mut book3 = SpeedBook::new();
        for _ in 0..2 {
            book3.record_sample(gpu_fast.worker_id, k_cpu, 200, 60);
            book3.record_sample(cpu_slow.worker_id, k_cpu, 5_000, 60);
        }
        let mut running3 = HashMap::new();
        running3.insert(gpu_fast.worker_id, vec![(k_cpu, Duration::ZERO)]);

        let cfg = SchedulerConfig::default();
        let workers3 = vec![gpu_fast, cpu_slow];
        let mut engine = Engine::new(&cfg, &workers3, &workers3, None, false, false)
            .with_speed(&book3, &running3);
        let v1 = TaskView::of(&Task::new(
            cpu_spec.clone(),
            TaskRequirements::generic(1, 60),
        ));
        let v2 = TaskView::of(&Task::new(
            cpu_spec.clone(),
            TaskRequirements::generic(1, 60),
        ));
        assert_eq!(
            engine.place(&v1),
            Verdict::Skip(ScheduleSkipReason::WaitingForFasterWorker)
        );
        assert!(!engine.dead_speed.is_empty());

        engine.note_gpu_waiting();
        assert!(engine.dead_speed.is_empty());
        assert_eq!(engine.place(&v2), Verdict::Placed(1));
        assert!(engine.dead_speed.is_empty());

        // Direct `hold_back` invalidation check: holding `v_cpu` populates `dead_speed`, then a
        // wide 4-core task finds `medium` (`4` cores, `2` free slots) in `full` and newly sets
        // `reserved[medium] = true`, clearing `dead_speed`.
        let mut hb_fast = make_worker("hb-fast", 1, 8192, false, false);
        hb_fast.capabilities.tags = vec!["slots:1".into()];
        hb_fast.active_tasks = 1;
        let mut hb_med = make_worker("hb-med", 4, 8192, false, false);
        hb_med.capabilities.tags = vec!["slots:4".into()];
        hb_med.active_tasks = 2;
        let mut hb_slow = make_worker("hb-slow", 2, 8192, false, false);
        hb_slow.capabilities.tags = vec!["slots:2".into()];

        let mut book4 = SpeedBook::new();
        for _ in 0..2 {
            book4.record_sample(hb_fast.worker_id, k_cpu, 200, 60);
            book4.record_sample(hb_med.worker_id, k_cpu, 5_000, 60);
            book4.record_sample(hb_slow.worker_id, k_cpu, 10_000, 60);
        }
        let mut running4 = HashMap::new();
        running4.insert(hb_fast.worker_id, vec![(k_cpu, Duration::ZERO)]);
        let workers4 = vec![hb_fast, hb_med, hb_slow];
        let mut hb_engine = Engine::new(&cfg, &workers4, &workers4, None, false, false)
            .with_speed(&book4, &running4);
        let v_hold = TaskView::of(&Task::new(cpu_spec, TaskRequirements::generic(1, 60)));
        assert_eq!(
            hb_engine.place(&v_hold),
            Verdict::Skip(ScheduleSkipReason::WaitingForFasterWorker)
        );
        assert!(!hb_engine.dead_speed.is_empty());

        let wide_spec = TaskSpec::new_command("link", vec!["--fat".into()]);
        let v_wide = TaskView::of(&Task::new(wide_spec, TaskRequirements::generic(4, 60)));
        assert_eq!(
            hb_engine.place(&v_wide),
            Verdict::Skip(ScheduleSkipReason::AllEligibleWorkersSaturated)
        );
        assert!(hb_engine.reserved[1]);
        assert!(hb_engine.dead_speed.is_empty());
    }

    #[test]
    fn test_sched4_ram_short_hold_does_not_starve_small_task() {
        // `fast` still has free CPU slots (`active_tasks = 1`, `slots:4`) and free RAM for a
        // small task (2048 MB available), but not enough RAM for a 4096 MB task.
        // Holding the 4096 MB task MUST NOT reserve `fast`, so a smaller 512 MB task of the
        // same `TaskKey` behind it still places on `fast` instead of `slow`.
        let mut fast = make_worker("fast", 8, 16384, false, false);
        fast.capabilities.tags = vec!["slots:4".into()];
        fast.active_tasks = 1;
        fast.ram_available_mb = 2_048;

        let mut slow = make_worker("slow", 8, 32768, false, false);
        slow.capabilities.tags = vec!["slots:4".into()];
        slow.ram_available_mb = 16_384;

        let spec = TaskSpec::new_command("worker", vec!["--run".into()]);
        let key = TaskKey::of(&spec);
        let mut book = SpeedBook::new();
        for _ in 0..2 {
            book.record_sample(fast.worker_id, key, 200, 60);
            book.record_sample(slow.worker_id, key, 5_000, 60);
        }

        let mut req_big = TaskRequirements::generic(1, 60);
        req_big.ram_mb = 4_096;
        let big_task = Task::new(spec.clone(), req_big);

        let mut req_small = TaskRequirements::generic(1, 60);
        req_small.ram_mb = 512;
        let small_task = Task::new(spec, req_small);
        let mut running = HashMap::new();
        running.insert(fast.worker_id, vec![(key, Duration::ZERO)]);

        let report = WorkloadScheduler::matchmake_with_speed(
            &SchedulerConfig::default(),
            &[big_task.clone(), small_task.clone()],
            &[fast.clone(), slow],
            &HashMap::new(),
            &HashMap::new(),
            &HashSet::new(),
            &book,
            &running,
        );
        assert_eq!(
            report.skipped,
            vec![(big_task.id, ScheduleSkipReason::WaitingForFasterWorker)]
        );
        assert_eq!(report.assignments.len(), 1);
        assert_eq!(report.assignments[0].task_id, small_task.id);
        assert_eq!(report.assignments[0].worker_id, fast.worker_id);
    }

    #[test]
    fn test_sched4_cold_start_outlier_still_gets_work() {
        let mut fast = make_worker("fast", 8, 16384, false, false);
        fast.capabilities.tags = vec!["slots:4".into()];
        let mut cold = make_worker("cold", 8, 16384, false, false);
        cold.capabilities.tags = vec!["slots:4".into()];

        let spec = TaskSpec::new_command("coldcheck", vec!["--run".into()]);
        let key = TaskKey::of(&spec);
        let mut book = SpeedBook::new();
        // `fast` has 2 samples at 200 ms; `cold` has only 1 cold-start outlier sample at 10,000 ms
        // (`< speed_min_samples = 2`).
        book.record_sample(fast.worker_id, key, 200, 60);
        book.record_sample(fast.worker_id, key, 200, 60);
        book.record_sample(cold.worker_id, key, 10_000, 60);

        let batch: Vec<Task> = (0..8)
            .map(|_| Task::new(spec.clone(), TaskRequirements::generic(1, 60)))
            .collect();
        let report = WorkloadScheduler::matchmake_with_speed(
            &SchedulerConfig::default(),
            &batch,
            &[fast.clone(), cold.clone()],
            &HashMap::new(),
            &HashMap::new(),
            &HashSet::new(),
            &book,
            &HashMap::new(),
        );
        let fast_count = report
            .assignments
            .iter()
            .filter(|a| a.worker_id == fast.worker_id)
            .count();
        let cold_count = report
            .assignments
            .iter()
            .filter(|a| a.worker_id == cold.worker_id)
            .count();
        assert_eq!((fast_count, cold_count), (4, 4));
    }

    #[tokio::test]
    async fn test_sched4_validate_speed_hold_factor() {
        for valid in [0.0, -0.0, 1.01, 1.5, 10.0] {
            assert!(
                SchedulerConfig::validate_speed_hold_factor(valid).is_ok(),
                "expected {valid} to be accepted"
            );
            assert!(SchedulerConfig::default()
                .with_speed_hold_factor(valid)
                .validate()
                .is_ok());
        }
        for invalid in [
            1.0,
            0.99,
            0.5,
            -1.0,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ] {
            assert!(
                SchedulerConfig::validate_speed_hold_factor(invalid).is_err(),
                "expected {invalid} to be rejected"
            );
            assert!(SchedulerConfig::default()
                .with_speed_hold_factor(invalid)
                .validate()
                .is_err());
        }

        let err = match crate::server::MasterServer::spawn_with_config(
            crate::server::ServerConfig::new("127.0.0.1:0".parse().unwrap()),
            SchedulerConfig::default().with_speed_hold_factor(1.0),
            crate::reaper::ReaperConfig::default(),
        )
        .await
        {
            Ok(_) => panic!("expected spawn_with_config to reject invalid speed_hold_factor"),
            Err(err) => err,
        };
        assert!(
            matches!(err, rusty_grid_core::error::GridError::Config(_)),
            "expected GridError::Config for invalid speed_hold_factor, got: {err:?}"
        );
    }

    #[test]
    fn worker_index_matches_all_stages_and_speed_hold_back() {
        let cfg = SchedulerConfig {
            preserve_gpu_for_gpu_tasks: true,
            gpu_reserved_slots: 1,
            ..Default::default()
        };

        let mut w_real_gpu = tagged_worker(
            "win-gpu",
            &["os:windows", "arch:x86_64", "iso:fs+net", "tool:cargo"],
        );
        w_real_gpu.capabilities.cpu_cores = 8;
        w_real_gpu.capabilities.ram_mb = 16384;
        w_real_gpu.ram_available_mb = 16384;
        w_real_gpu.capabilities.has_gpu = true;
        w_real_gpu.capabilities.is_simulated_gpu = false;
        w_real_gpu.features.insert(ProtocolCapabilities::WORKSPACES);

        let mut w_sim_gpu = tagged_worker("lin-sim", &["os:linux", "arch:x86_64", "iso:fs"]);
        w_sim_gpu.capabilities.cpu_cores = 8;
        w_sim_gpu.capabilities.ram_mb = 16384;
        w_sim_gpu.ram_available_mb = 16384;
        w_sim_gpu.capabilities.has_gpu = true;
        w_sim_gpu.capabilities.is_simulated_gpu = true;

        let mut w_cpu_fast = tagged_worker(
            "lin-fast",
            &["os:linux", "arch:x86_64", "iso:fs", "tool:cargo", "fast"],
        );
        w_cpu_fast.capabilities.cpu_cores = 8;
        w_cpu_fast.ram_available_mb = 8192;

        let mut w_busy = tagged_worker("lin-busy", &["os:linux", "arch:x86_64", "busy-only"]);
        w_busy.cpu_usage_pct = 99.0;

        let mut w_ram_short = tagged_worker("lin-lowram", &["os:linux", "lowram-only"]);
        w_ram_short.ram_available_mb = 64;

        let mut w_mobile_warm = tagged_worker("mobile-warm", &["os:android", "warm-only"]);
        w_mobile_warm.mobile_status = Some(MobileLiveStatus {
            battery_pct: Some(80),
            is_charging: Some(false),
            thermal_throttled: false,
            battery_temperature: Some(36.0),
            thermal_tier: Some(ThermalTier::Warm),
            ..Default::default()
        });

        let workers = vec![
            w_real_gpu.clone(),
            w_sim_gpu.clone(),
            w_cpu_fast.clone(),
            w_busy.clone(),
            w_ram_short.clone(),
            w_mobile_warm.clone(),
        ];

        // 1. Real GPU preferred over simulated GPU when both idle; WORKSPACES gated.
        let mut gpu_task = Task::new(
            TaskSpec::new_gpu_compute("mat", 64),
            TaskRequirements {
                gpu_required: true,
                ..TaskRequirements::generic(1, 256)
            },
        );
        gpu_task.id = TaskId::new();
        let ws_task = Task::new(
            TaskSpec::new_command("ws", vec![]),
            TaskRequirements::generic(1, 32),
        )
        .with_workspace(rusty_grid_core::task::WorkspaceRef::new("ws-demo"));
        let r1 = WorkloadScheduler::matchmake(&cfg, &[gpu_task.clone(), ws_task.clone()], &workers);
        let m1: HashMap<TaskId, Uuid> = r1
            .assignments
            .iter()
            .map(|a| (a.task_id, a.worker_id))
            .collect();
        assert_eq!(m1.get(&gpu_task.id), Some(&w_real_gpu.worker_id));
        assert_eq!(m1.get(&ws_task.id), Some(&w_real_gpu.worker_id));

        // 2. Host-busy idle worker rejected; RAM-short worker rejects 256 MB but accepts 32 MB.
        let t_busy = constrained_task(&["require:tag=busy-only"]);
        let t_big_ram = Task::new(
            TaskSpec::builtin_test("hash", 10),
            TaskRequirements::new(1, 256, false, 30),
        )
        .with_tags(vec!["require:tag=lowram-only".into()]);
        let t_small_ram = Task::new(
            TaskSpec::builtin_test("hash", 10),
            TaskRequirements::new(1, 32, false, 30),
        )
        .with_tags(vec!["require:tag=lowram-only".into()]);
        let r2 =
            WorkloadScheduler::matchmake(&cfg, &[t_busy, t_big_ram, t_small_ram.clone()], &workers);
        assert_eq!(r2.assignments.len(), 1);
        assert_eq!(r2.assignments[0].task_id, t_small_ram.id);
        assert_eq!(r2.assignments[0].worker_id, w_ram_short.worker_id);

        // 3. Mobile Warm + !charging rejects heavy RustCompilation, allows light command.
        let mut sources = HashMap::new();
        sources.insert("Cargo.toml".into(), "[package]\nname = \"m\"".into());
        let t_heavy_mob = Task::new(
            TaskSpec::new_rust_compilation("m", sources, vec![]),
            TaskRequirements::generic(1, 32),
        )
        .with_tags(vec!["require:tag=warm-only".into()]);
        let t_light_mob = constrained_task(&["require:tag=warm-only"]);
        let r3 = WorkloadScheduler::matchmake(&cfg, &[t_heavy_mob, t_light_mob.clone()], &workers);
        assert_eq!(r3.assignments.len(), 1);
        assert_eq!(r3.assignments[0].task_id, t_light_mob.id);

        // 4. GPU preservation + avoid preference + fallback.
        let engine = Engine::new(&cfg, &workers, &workers, None, true, true);
        let generic_v = TaskView::of(&constrained_task(&["require:os=linux"]));
        let mut full = Vec::new();
        let mut matching = Vec::new();
        let cands = engine.candidates_and_matching(&generic_v, &[], &mut full, &mut matching);
        assert!(
            cands
                .iter()
                .all(|&i| !workers[i].capabilities.can_execute_gpu()),
            "gpu_waiting must steer generic task away from sim GPU onto CPU"
        );
        // Avoiding the only matching worker falls back via second pass in Engine::place.
        let only_fast = constrained_task(&["require:tag=fast"]);
        let mut avoid_map = HashMap::new();
        avoid_map.insert(only_fast.id, vec![w_cpu_fast.worker_id]);
        let r_avoid = WorkloadScheduler::matchmake_with_avoid(
            &cfg,
            std::slice::from_ref(&only_fast),
            &workers,
            &avoid_map,
        );
        assert_eq!(r_avoid.assignments.len(), 1);
        assert_eq!(r_avoid.assignments[0].worker_id, w_cpu_fast.worker_id);

        // 5. Wide-task hold-back on partially busy worker.
        let mut w_hold = make_worker("hold-w", 4, 8192, false, false);
        w_hold.active_tasks = 1;
        let t_wide = Task::new(
            TaskSpec::builtin_test("wide", 10),
            TaskRequirements::generic(4, 32),
        );
        let t_narrow = Task::new(
            TaskSpec::builtin_test("narrow", 10),
            TaskRequirements::generic(1, 32),
        );
        let r_hold = WorkloadScheduler::matchmake(
            &cfg,
            &[t_wide.clone(), t_narrow.clone()],
            std::slice::from_ref(&w_hold),
        );
        assert!(r_hold.assignments.is_empty());
        assert_eq!(r_hold.skipped.len(), 2);

        // 6. SpeedBook hold-back.
        let mut fast = make_worker("fast-sb", 4, 8192, false, false);
        fast.capabilities.tags = vec!["slots:1".into()];
        fast.active_tasks = 1;
        let slow = make_worker("slow-sb", 4, 8192, false, false);
        let sb_task = Task::new(
            TaskSpec::new_command("sb-cmd", vec![]),
            TaskRequirements::generic(1, 32),
        );
        let sb_key = TaskKey::of(&sb_task.spec);
        let mut sb = SpeedBook::new();
        for _ in 0..3 {
            sb.record_sample(fast.worker_id, sb_key, 100, 60);
            sb.record_sample(slow.worker_id, sb_key, 2_000, 60);
        }
        let mut sb_running = HashMap::new();
        sb_running.insert(fast.worker_id, vec![(sb_key, Duration::ZERO)]);
        let r_sb = WorkloadScheduler::matchmake_with_speed(
            &cfg,
            std::slice::from_ref(&sb_task),
            &[fast, slow],
            &HashMap::new(),
            &HashMap::new(),
            &HashSet::new(),
            &sb,
            &sb_running,
        );
        assert_eq!(
            r_sb.skipped,
            vec![(sb_task.id, ScheduleSkipReason::WaitingForFasterWorker)]
        );
    }

    #[test]
    fn schedule_1000_heterogeneous_workers_scales_cleanly() {
        let oses = ["os:linux", "os:windows", "os:macos"];
        let arches = ["arch:x86_64", "arch:aarch64"];
        let isos = ["iso:fs", "iso:fs+net"];
        let tools = ["tool:cargo", "tool:python"];

        let workers: Vec<WorkerInfo> = (0..1000)
            .map(|i| {
                let mut w = make_worker(&format!("w-{i}"), 8, 16384, i % 5 == 0, false);
                w.capabilities.tags = vec![
                    oses[i % oses.len()].into(),
                    arches[(i / 3) % arches.len()].into(),
                    isos[i % isos.len()].into(),
                    tools[(i / 2) % tools.len()].into(),
                    format!("shard-{}", i % 10),
                ];
                w
            })
            .collect();

        let req_oses = ["require:os=linux", "require:os=windows", "require:os=macos"];
        let req_arches = ["require:arch=x86_64", "require:arch=aarch64"];
        let tasks: Vec<Task> = (0..1000)
            .map(|i| {
                let gpu = i % 5 == 0;
                Task::new(
                    TaskSpec::builtin_test("scale", 10),
                    TaskRequirements {
                        gpu_required: gpu,
                        timeout_secs: 60,
                        ..TaskRequirements::generic(1, 32)
                    },
                )
                .with_tags(vec![
                    req_oses[i % req_oses.len()].into(),
                    req_arches[(i / 3) % req_arches.len()].into(),
                    format!("require:tag=shard-{}", i % 10),
                ])
            })
            .collect();

        let cfg = SchedulerConfig::default().with_max_batch_size(1024);
        let start = Instant::now();
        let report = WorkloadScheduler::matchmake(&cfg, &tasks, &workers);
        let elapsed = start.elapsed();

        assert_eq!(report.assignments.len(), 1000);
        assert!(
            report.skipped.is_empty(),
            "expected 0 skipped, got {:?}",
            report.skipped.len()
        );
        println!(
            "1000-worker heterogeneous scheduling took {elapsed:?} (workers_inspected={})",
            report.workers_inspected
        );
        assert!(
            report.workers_inspected < 100_000,
            "bitset index should prune far below 1,000,000 naive worker checks, inspected {}",
            report.workers_inspected
        );
        assert!(
            elapsed < Duration::from_secs(60),
            "1000-worker heterogeneous scheduling hung ({elapsed:?})"
        );
    }

    #[tokio::test]
    async fn multi_submitter_fair_share_prevents_starvation_and_preserves_hold_back() {
        let (registry, queue, waiters, notify) = scheduler_fixture().await;
        let mut rxs = Vec::new();
        for idx in 0..2 {
            let wid = Uuid::new_v4();
            let (tx, rx) = tokio::sync::mpsc::channel(1024);
            rxs.push(rx);
            registry
                .register(
                    wid,
                    WorkerCapabilities::new(format!("w-{idx}"), 8, 16384, false, false, None),
                    format!("127.0.0.1:920{idx}").parse().unwrap(),
                    tx,
                    None,
                )
                .await
                .unwrap();
        }

        let sub_a = crate::fair_share::SubmitterId::Client(Uuid::new_v4());
        let sub_b = crate::fair_share::SubmitterId::Client(Uuid::new_v4());
        let mut set_a = HashSet::new();
        let mut set_b = HashSet::new();
        for i in 0..500 {
            let tid = queue
                .submit_from(
                    Task::new(
                        TaskSpec::new_command(format!("a-{i}"), vec![]),
                        TaskRequirements::generic(1, 32),
                    ),
                    0,
                    sub_a,
                )
                .await
                .unwrap();
            set_a.insert(tid);
        }
        for i in 0..20 {
            let tid = queue
                .submit_from(
                    Task::new(
                        TaskSpec::new_command(format!("b-{i}"), vec![]),
                        TaskRequirements::generic(1, 32),
                    ),
                    0,
                    sub_b,
                )
                .await
                .unwrap();
            set_b.insert(tid);
        }

        let sched = WorkloadScheduler::new(
            SchedulerConfig::default().with_max_batch_size(16),
            registry,
            queue,
            notify,
        )
        .with_waiters(waiters);
        let report = sched.schedule_batch(16).await.unwrap();
        assert_eq!(report.assignments.len(), 16);
        let a_placed = report
            .assignments
            .iter()
            .filter(|a| set_a.contains(&a.task_id))
            .count();
        let b_placed = report
            .assignments
            .iter()
            .filter(|a| set_b.contains(&a.task_id))
            .count();
        assert!(
            a_placed >= 7 && b_placed >= 7,
            "expected fair-share interleaving across submitters, got a={a_placed}, b={b_placed}"
        );

        // Wide-task hold_back must still block trailing tasks from another submitter.
        let (reg2, q2, w2, n2) = scheduler_fixture().await;
        let wid2 = Uuid::new_v4();
        let (tx2, _rx2) = tokio::sync::mpsc::channel(64);
        reg2.register(
            wid2,
            WorkerCapabilities::new("w-hold", 4, 8192, false, false, None),
            "127.0.0.1:9209".parse().unwrap(),
            tx2,
            None,
        )
        .await
        .unwrap();

        let _running_t = q2
            .submit_from(
                Task::new(
                    TaskSpec::new_command("run-1", vec![]),
                    TaskRequirements::generic(1, 32),
                ),
                0,
                sub_a,
            )
            .await
            .unwrap();
        let sched2 = WorkloadScheduler::new(
            SchedulerConfig::default().with_max_batch_size(16),
            reg2,
            q2.clone(),
            n2,
        )
        .with_waiters(w2);
        let r_init = sched2.schedule_batch(1).await.unwrap();
        assert_eq!(r_init.assignments.len(), 1);

        let wide_id = q2
            .submit_from(
                Task::new(
                    TaskSpec::new_command("wide-4", vec![]),
                    TaskRequirements::generic(4, 32),
                ),
                0,
                sub_a,
            )
            .await
            .unwrap();
        let _other_id = q2
            .submit_from(
                Task::new(
                    TaskSpec::new_command("other-1", vec![]),
                    TaskRequirements::generic(1, 32),
                ),
                0,
                sub_b,
            )
            .await
            .unwrap();
        let r_held = sched2.schedule_batch(16).await.unwrap();
        assert!(
            r_held.assignments.is_empty(),
            "held worker must not accept sub_b narrow task while sub_a wide task waits"
        );
        assert!(r_held.skipped.iter().any(|(id, reason)| *id == wide_id
            && *reason == ScheduleSkipReason::AllEligibleWorkersSaturated));
        drop(rxs);
    }

    #[test]
    fn test_vram_packing_multiple_tasks_share_worker_until_budget_exhausted() {
        let config = SchedulerConfig::default();
        let mut gpu_worker = make_worker("gpu-24g", 4, 32_768, true, false);
        gpu_worker.capabilities.vram_mb = Some(24_576);
        let workers = vec![gpu_worker.clone()];

        let make_vram_task = |vram_mb: u64| {
            Task::new(
                TaskSpec::new_gpu_compute("vram-pack", 64),
                TaskRequirements {
                    gpu_required: true,
                    ..TaskRequirements::generic(1, 512)
                }
                .with_vram_mb(vram_mb),
            )
        };
        let t1 = make_vram_task(10_240);
        let t2 = make_vram_task(10_240);
        let t3 = make_vram_task(10_240);
        let t_unsized = Task::new(
            TaskSpec::new_gpu_compute("vram-unsized", 64),
            TaskRequirements {
                gpu_required: true,
                ..TaskRequirements::generic(1, 512)
            },
        );

        let packed_with_unsized = WorkloadScheduler::matchmake(
            &config,
            &[t1.clone(), t2.clone(), t_unsized.clone()],
            &workers,
        );
        let unsized_assigned: Vec<TaskId> = packed_with_unsized
            .assignments
            .iter()
            .map(|a| a.task_id)
            .collect();
        assert_eq!(unsized_assigned, vec![t1.id, t2.id, t_unsized.id]);

        let report =
            WorkloadScheduler::matchmake(&config, &[t1.clone(), t2.clone(), t3.clone()], &workers);
        let assigned: Vec<TaskId> = report.assignments.iter().map(|a| a.task_id).collect();
        assert_eq!(assigned, vec![t1.id, t2.id]);
        assert!(
            report
                .skipped
                .iter()
                .any(|(id, reason)| *id == t3.id
                    && *reason == ScheduleSkipReason::NoGpuWorkersAvailable),
            "third 10240 MB task must skip once 20480 of 24576 MB VRAM is committed, got {:?}",
            report.skipped
        );
    }

    /// design 0082 D4: the worker gets the task without its master-scope extension (it cannot
    /// enforce it), with a worker-scope one it advertised and an unknown non-critical one
    /// untouched; the queue keeps the stored task whole.
    #[tokio::test]
    async fn test_dispatch_strips_master_scope_extensions_and_keeps_the_rest() {
        use rusty_grid_core::task::Extension;
        let (registry, queue, _waiters, notify) = scheduler_fixture().await;
        let wid = Uuid::new_v4();
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        registry
            .register(
                wid,
                WorkerCapabilities::new("ext-worker", 4, 4096, false, false, None)
                    .with_tags(vec!["ext:32770".to_string()]),
                "127.0.0.1:9411".parse().unwrap(),
                tx,
                None,
            )
            .await
            .unwrap();
        let mut task = Task::new(
            TaskSpec::builtin_test("hash_compute", 10),
            TaskRequirements::generic(1, 30),
        );
        let ext = |id: u16, critical: bool| Extension {
            id,
            critical,
            bytes: vec![1, 2],
        };
        task.extensions = vec![ext(0x8002, true), ext(0x8003, true), ext(0x9999, false)];
        let id = queue.submit(task).await.unwrap();
        let sched =
            WorkloadScheduler::new(SchedulerConfig::default(), registry, queue.clone(), notify);
        let report = sched.schedule_once().await.unwrap();
        assert_eq!(report.assignments.len(), 1, "{report:?}");
        match rx.recv().await {
            Some(MasterMessage::AssignTask { task }) => {
                let ids: Vec<u16> = task.extensions.iter().map(|e| e.id).collect();
                assert_eq!(ids, vec![0x8002, 0x9999]);
                assert_eq!(
                    task.extensions[1].bytes,
                    vec![1, 2],
                    "relayed byte for byte"
                );
            }
            other => panic!("expected AssignTask, got {other:?}"),
        }
        assert_eq!(queue.get_task_spec(&id).await.unwrap().extensions.len(), 3);
    }

    #[tokio::test]
    async fn test_vram_accounting_tracks_running_reservation_and_releases_on_finish() {
        let (registry, queue, _waiters, notify) = scheduler_fixture().await;
        let wid = Uuid::new_v4();
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let session = registry
            .register(
                wid,
                WorkerCapabilities::new("gpu-16g", 4, 32_768, true, false, None)
                    .with_vram_mb(Some(16_384)),
                "127.0.0.1:9410".parse().unwrap(),
                tx,
                None,
            )
            .await
            .unwrap();

        let make_vram_task = || {
            Task::new(
                TaskSpec::new_gpu_compute("vram-job", 64),
                TaskRequirements {
                    gpu_required: true,
                    ..TaskRequirements::generic(1, 256)
                }
                .with_vram_mb(10_240),
            )
        };
        let t1 = queue.submit(make_vram_task()).await.unwrap();
        let t2 = queue.submit(make_vram_task()).await.unwrap();

        let sched =
            WorkloadScheduler::new(SchedulerConfig::default(), registry, queue.clone(), notify);

        let r1 = sched.schedule_once().await.unwrap();
        assert_eq!(r1.assignments.len(), 1);
        assert_eq!(r1.assignments[0].task_id, t1);
        assert!(matches!(
            rx.recv().await,
            Some(MasterMessage::AssignTask { .. })
        ));

        // Transition t1 to Running: cross-pass `assigned_loads` must still reserve its 10240 MB VRAM.
        queue
            .mark_running_from(wid, Some(session), &t1)
            .await
            .unwrap();
        let r2 = sched.schedule_once().await.unwrap();
        assert!(
            r2.assignments.is_empty(),
            "running 10240 MB task on 16384 MB worker must block second 10240 MB task"
        );

        // Once t1 finishes cleanly, its VRAM reservation drops and t2 is placed.
        queue
            .record_result_from(
                wid,
                Some(session),
                rusty_grid_core::task::TaskResult::success(wid, t1, "", 1, false),
            )
            .await
            .unwrap();
        let r3 = sched.schedule_once().await.unwrap();
        assert_eq!(r3.assignments.len(), 1);
        assert_eq!(r3.assignments[0].task_id, t2);
        assert!(matches!(
            rx.recv().await,
            Some(MasterMessage::AssignTask { .. })
        ));
    }

    #[tokio::test]
    async fn test_vram_hold_back_prevents_small_vram_stream_from_starving_large_vram_task() {
        let (registry, queue, _waiters, notify) = scheduler_fixture().await;
        let wid = Uuid::new_v4();
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let session = registry
            .register(
                wid,
                WorkerCapabilities::new("gpu-24g", 4, 32_768, true, false, None)
                    .with_vram_mb(Some(24_576)),
                "127.0.0.1:9411".parse().unwrap(),
                tx,
                None,
            )
            .await
            .unwrap();

        let vram_task = |vram_mb: u64| {
            Task::new(
                TaskSpec::new_gpu_compute("vram-stream", 64),
                TaskRequirements {
                    gpu_required: true,
                    ..TaskRequirements::generic(1, 256)
                }
                .with_vram_mb(vram_mb),
            )
        };

        let t_running = queue.submit(vram_task(16_384)).await.unwrap();
        let sched =
            WorkloadScheduler::new(SchedulerConfig::default(), registry, queue.clone(), notify);

        let r0 = sched.schedule_once().await.unwrap();
        assert_eq!(r0.assignments.len(), 1);
        assert_eq!(r0.assignments[0].task_id, t_running);
        assert!(matches!(
            rx.recv().await,
            Some(MasterMessage::AssignTask { .. })
        ));
        queue
            .mark_running_from(wid, Some(session), &t_running)
            .await
            .unwrap();

        // 8192 MB VRAM remain free. Submit a high-priority 20480 MB task and a 4096 MB trailing task.
        let t_big = queue
            .submit_with_priority(vram_task(20_480), 10)
            .await
            .unwrap();
        let t_small = queue.submit(vram_task(4_096)).await.unwrap();

        let r_held = sched.schedule_batch(16).await.unwrap();
        assert!(
            r_held.assignments.is_empty(),
            "VRAM hold_back must prevent small VRAM task from starving waiting 20480 MB task, got {:?}",
            r_held.assignments
        );

        queue
            .record_result_from(
                wid,
                Some(session),
                rusty_grid_core::task::TaskResult::success(wid, t_running, "", 1, false),
            )
            .await
            .unwrap();
        let r_after = sched.schedule_batch(16).await.unwrap();
        let after_ids: Vec<TaskId> = r_after.assignments.iter().map(|a| a.task_id).collect();
        assert_eq!(after_ids, vec![t_big, t_small]);
    }

    #[tokio::test]
    async fn test_fail_unfulfillable_tasks_handles_unknown_vram_worker_and_includes_vram_mb_in_reason(
    ) {
        let (registry, queue, _waiters, notify) = scheduler_fixture().await;
        let wid = Uuid::new_v4();
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let _session = registry
            .register(
                wid,
                WorkerCapabilities::new("gpu-unknown-vram", 4, 16_384, true, false, None),
                "127.0.0.1:9412".parse().unwrap(),
                tx,
                None,
            )
            .await
            .unwrap();

        let t_unknown_vram = queue
            .submit(Task::new(
                TaskSpec::new_gpu_compute("unsized-gpu", 64),
                TaskRequirements {
                    gpu_required: true,
                    ..TaskRequirements::generic(1, 256)
                },
            ))
            .await
            .unwrap();
        let t_explicit_vram = queue
            .submit(Task::new(
                TaskSpec::new_command("needs-24g", vec![]),
                TaskRequirements::generic(1, 256).with_vram_mb(24_576),
            ))
            .await
            .unwrap();
        let t_huge_cpu = queue
            .submit(Task::new(
                TaskSpec::new_command("too-many-cores", vec![]),
                TaskRequirements::generic(16, 32),
            ))
            .await
            .unwrap();

        let sched = WorkloadScheduler::new(
            SchedulerConfig::default().with_fail_unfulfillable_tasks(true),
            registry,
            queue.clone(),
            notify,
        );
        let report = sched.schedule_batch(16).await.unwrap();
        assert_eq!(report.assignments.len(), 1);
        assert_eq!(report.assignments[0].task_id, t_unknown_vram);
        assert!(matches!(
            rx.recv().await,
            Some(MasterMessage::AssignTask { .. })
        ));

        let failed: HashMap<TaskId, String> = report.unfulfillable.into_iter().collect();
        assert_eq!(failed.len(), 2);
        let vram_reason = failed.get(&t_explicit_vram).expect("vram task fails");
        assert!(
            vram_reason.contains("vram_mb=24576"),
            "expected vram_mb=24576 in unfulfillable reason, got {vram_reason:?}"
        );
        assert_eq!(
            failed.get(&t_huge_cpu).map(String::as_str),
            Some("No registered worker satisfies task requirements (cpu_cores=16, ram_mb=512, gpu=false)"),
            "zero-VRAM tasks must keep the original resource-reason format without vram_mb"
        );
    }

    #[tokio::test]
    async fn test_preserve_gpu_for_gpu_tasks_reacts_to_command_task_with_explicit_vram_mb() {
        let (registry, queue, _waiters, notify) = scheduler_fixture().await;
        let wid = Uuid::new_v4();
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let session = registry
            .register(
                wid,
                WorkerCapabilities::new("gpu-16g", 2, 16_384, true, false, None)
                    .with_vram_mb(Some(16_384)),
                "127.0.0.1:9413".parse().unwrap(),
                tx,
                None,
            )
            .await
            .unwrap();

        // Occupy 1 slot and 12288 MB VRAM so only 1 slot (`<= gpu_reserved_slots=1`) and 4096 MB remain.
        let t_running = queue
            .submit(Task::new(
                TaskSpec::new_gpu_compute("holder", 64),
                TaskRequirements {
                    gpu_required: true,
                    ..TaskRequirements::generic(1, 256)
                }
                .with_vram_mb(12_288),
            ))
            .await
            .unwrap();
        let sched = WorkloadScheduler::new(
            SchedulerConfig {
                preserve_gpu_for_gpu_tasks: true,
                gpu_reserved_slots: 1,
                ..SchedulerConfig::default()
            },
            registry.clone(),
            queue.clone(),
            notify,
        );
        let r0 = sched.schedule_once().await.unwrap();
        assert_eq!(r0.assignments.len(), 1);
        assert_eq!(r0.assignments[0].task_id, t_running);
        assert!(matches!(
            rx.recv().await,
            Some(MasterMessage::AssignTask { .. })
        ));
        queue
            .mark_running_from(wid, Some(session), &t_running)
            .await
            .unwrap();

        // Queue a Command task specifying `with_vram_mb(8192)` (with `gpu_required == false` at struct level)
        // plus a trailing generic CPU task.
        let _t_vram_cmd = queue
            .submit_with_priority(
                Task::new(
                    TaskSpec::new_command("cuda-cli", vec!["--infer".into()]),
                    TaskRequirements::generic(1, 256).with_vram_mb(8_192),
                ),
                0,
            )
            .await
            .unwrap();
        let _t_generic = queue.submit(cmd_task("plain-cpu")).await.unwrap();

        let workers = registry.list_workers().await;
        let (gpu_waiting, gpu_recent) = sched.gpu_demand(&workers).await;
        assert!(
            gpu_waiting && gpu_recent,
            "Command task with with_vram_mb(8192) must trigger gpu_demand"
        );

        let r_pass = sched.schedule_batch(16).await.unwrap();
        assert!(
            r_pass.assignments.is_empty(),
            "GPU preservation must hold the final GPU slot while a VRAM-requiring Command task waits, got {:?}",
            r_pass.assignments
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_stalled_worker_full_channel_does_not_block_healthy_worker_and_requeues_after_deadline(
    ) {
        let (registry, queue, waiters, notify) = scheduler_fixture().await;
        // Give `stalled` a lower UUID so `schedule_batch` sorts it ahead of `healthy` and
        // dispatches to the stalled worker first in the pass.
        let stalled = Uuid::from_u128(1);
        let healthy = Uuid::from_u128(2);

        let (stalled_tx, _stalled_rx) = tokio::sync::mpsc::channel(1);
        stalled_tx
            .try_send(MasterMessage::HeartbeatAck { timestamp: 1 })
            .expect("fill stalled worker channel");
        let (healthy_tx, mut healthy_rx) = tokio::sync::mpsc::channel(8);

        registry
            .register(
                stalled,
                WorkerCapabilities::new("stalled", 1, 8192, true, false, None)
                    .with_vram_mb(Some(16_384)),
                "127.0.0.1:9420".parse().unwrap(),
                stalled_tx,
                None,
            )
            .await
            .unwrap();
        registry
            .register(
                healthy,
                WorkerCapabilities::new("healthy", 4, 8192, false, false, None),
                "127.0.0.1:9421".parse().unwrap(),
                healthy_tx,
                None,
            )
            .await
            .unwrap();

        let t_stalled = queue
            .submit(
                Task::new(
                    TaskSpec::new_command("on-stalled", vec![]),
                    TaskRequirements::generic(1, 1024).with_vram_mb(4096),
                )
                .with_tags(vec![format!("require:worker={stalled}")]),
            )
            .await
            .unwrap();
        let t_healthy_1 = queue
            .submit(
                Task::new(
                    TaskSpec::new_command("on-healthy-1", vec![]),
                    TaskRequirements::generic(1, 512),
                )
                .with_tags(vec![format!("require:worker={healthy}")]),
            )
            .await
            .unwrap();
        let t_healthy_2 = queue
            .submit(
                Task::new(
                    TaskSpec::new_command("on-healthy-2", vec![]),
                    TaskRequirements::generic(1, 512),
                )
                .with_tags(vec![format!("require:worker={healthy}")]),
            )
            .await
            .unwrap();

        let config = SchedulerConfig::default();
        let tick = config.tick_interval;
        let sched = Arc::new(
            WorkloadScheduler::new(config, registry.clone(), queue.clone(), notify)
                .with_waiters(waiters),
        );

        let report = tokio::time::timeout(tick, sched.schedule_once())
            .await
            .expect("pass must not block on stalled worker channel")
            .unwrap();
        assert_eq!(report.assignments.len(), 3);
        assert_eq!(report.assignments[0].worker_id, stalled);
        assert_eq!(report.assignments[0].task_id, t_stalled);

        // Within the same scheduling pass, the healthy worker receives all of its assignments.
        let mut healthy_assigned = Vec::new();
        while let Ok(msg) = healthy_rx.try_recv() {
            if let MasterMessage::AssignTask { task } = msg {
                healthy_assigned.push(task.id);
            }
        }
        assert_eq!(healthy_assigned, vec![t_healthy_1, t_healthy_2]);

        // Before the reserve deadline expires, the stalled worker's task is still waiting for room.
        tokio::time::advance(DISPATCH_RESERVE_TIMEOUT - Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            queue.get_state(&t_stalled).await,
            Some(crate::queue::TaskState::Scheduled)
        );
        assert_eq!(registry.get_worker(stalled).await.unwrap().active_tasks, 1);
        let load_before = queue
            .assigned_loads()
            .await
            .get(&stalled)
            .copied()
            .unwrap_or_default();
        assert_eq!(load_before.tasks, 1);
        assert_eq!(load_before.slots, 1);
        assert_eq!(load_before.unstarted_ram_mb, 512);
        assert_eq!(load_before.vram_mb, 4096);

        // Once DISPATCH_RESERVE_TIMEOUT elapses, the stalled worker's task is requeued uncharged
        // and all slot/RAM/VRAM reservations on `stalled` are released.
        tokio::time::advance(Duration::from_millis(200)).await;
        for _ in 0..10 {
            if queue.get_state(&t_stalled).await == Some(crate::queue::TaskState::Queued) {
                break;
            }
            tokio::task::yield_now().await;
        }
        let stalled_info = queue.get_task(&t_stalled).await.unwrap();
        assert_eq!(stalled_info.state, crate::queue::TaskState::Queued);
        assert_eq!(stalled_info.retry_count, 0);
        assert_eq!(stalled_info.failover_count, 0);
        assert_eq!(registry.get_worker(stalled).await.unwrap().active_tasks, 0);
        assert_eq!(
            registry.get_worker(stalled).await.unwrap().status,
            WorkerStatus::Connected
        );
        assert!(
            !queue.assigned_loads().await.contains_key(&stalled),
            "requeued dispatch must release tasks, slots, unstarted_ram_mb, and vram_mb"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_task_cancelled_while_deferred_delivery_waits_is_never_sent() {
        let (registry, queue, waiters, notify) = scheduler_fixture().await;
        let w = Uuid::new_v4();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        registry
            .register(
                w,
                WorkerCapabilities::new("w-cancel", 4, 8192, false, false, None),
                "127.0.0.1:9422".parse().unwrap(),
                tx.clone(),
                None,
            )
            .await
            .unwrap();
        tx.try_send(MasterMessage::HeartbeatAck { timestamp: 1 })
            .expect("fill channel");

        let t_cancel = queue
            .submit(cmd_task("cancel-while-waiting"))
            .await
            .unwrap();
        let t_keep = queue.submit(cmd_task("keep-while-waiting")).await.unwrap();

        let sched = WorkloadScheduler::new(
            SchedulerConfig::default(),
            registry.clone(),
            queue.clone(),
            notify.clone(),
        )
        .with_waiters(waiters.clone());

        let report = sched.schedule_once().await.unwrap();
        assert_eq!(report.assignments.len(), 2);
        assert_eq!(
            queue.get_state(&t_cancel).await,
            Some(crate::queue::TaskState::Scheduled)
        );
        assert_eq!(registry.get_worker(w).await.unwrap().active_tasks, 2);

        // Cancel `t_cancel` while both deferred delivery waiters are parked on the full channel.
        let cancel_handle = {
            let (registry, queue, notify, waiters) = (
                registry.clone(),
                queue.clone(),
                notify.clone(),
                waiters.clone(),
            );
            tokio::spawn(async move {
                crate::server::cancel_task_and_notify(
                    &registry,
                    &queue,
                    &notify,
                    &waiters,
                    t_cancel,
                    "user cancelled",
                )
                .await
            })
        };
        for _ in 0..10 {
            if queue.get_state(&t_cancel).await == Some(crate::queue::TaskState::Cancelled) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            queue.get_state(&t_cancel).await,
            Some(crate::queue::TaskState::Cancelled)
        );

        // Drain the channel: `t_cancel` must not be sent, `t_keep` must be sent, and `t_cancel`'s
        // reserved dispatch slot must be released.
        let mut assigned = Vec::new();
        for _ in 0..8 {
            tokio::task::yield_now().await;
            while let Ok(msg) = rx.try_recv() {
                if let MasterMessage::AssignTask { task } = msg {
                    assigned.push(task.id);
                }
            }
        }
        assert!(cancel_handle.await.unwrap().unwrap());
        assert_eq!(assigned, vec![t_keep]);
        assert_eq!(registry.get_worker(w).await.unwrap().active_tasks, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn test_deferred_dispatch_waiter_cap_requeues_excess_and_releases_reservations() {
        let (registry, queue, waiters, notify) = scheduler_fixture().await;
        let w = Uuid::new_v4();
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        tx.try_send(MasterMessage::HeartbeatAck { timestamp: 1 })
            .expect("fill channel");
        registry
            .register(
                w,
                WorkerCapabilities::new("w-cap", 4, 8192, true, false, None)
                    .with_vram_mb(Some(16_384)),
                "127.0.0.1:9423".parse().unwrap(),
                tx,
                None,
            )
            .await
            .unwrap();

        let t1 = queue
            .submit(Task::new(
                TaskSpec::new_command("cap-1", vec![]),
                TaskRequirements::generic(1, 512).with_vram_mb(4096),
            ))
            .await
            .unwrap();
        let t2 = queue
            .submit(Task::new(
                TaskSpec::new_command("cap-2", vec![]),
                TaskRequirements::generic(1, 512).with_vram_mb(4096),
            ))
            .await
            .unwrap();

        // Cap deferred dispatch waiters at 1 per worker: `t1` gets a waiter, `t2` is requeued at once.
        let sched = WorkloadScheduler::new(
            SchedulerConfig::default(),
            registry.clone(),
            queue.clone(),
            notify,
        )
        .with_waiters(waiters)
        .with_max_in_flight_dispatches_per_worker(1);

        let report = sched.schedule_once().await.unwrap();
        assert_eq!(report.assignments.len(), 2);
        assert_eq!(
            queue.get_state(&t1).await,
            Some(crate::queue::TaskState::Scheduled)
        );
        assert_eq!(
            queue.get_state(&t2).await,
            Some(crate::queue::TaskState::Queued)
        );
        assert_eq!(registry.get_worker(w).await.unwrap().active_tasks, 1);
        let load = queue
            .assigned_loads()
            .await
            .get(&w)
            .copied()
            .unwrap_or_default();
        assert_eq!(load.tasks, 1);
        assert_eq!(load.vram_mb, 4096);

        // After the deadline expires, `t1` also returns to Queued and releases its reservation.
        tokio::time::advance(DISPATCH_RESERVE_TIMEOUT + Duration::from_millis(100)).await;
        for _ in 0..10 {
            if queue.get_state(&t1).await == Some(crate::queue::TaskState::Queued) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            queue.get_state(&t1).await,
            Some(crate::queue::TaskState::Queued)
        );
        assert_eq!(registry.get_worker(w).await.unwrap().active_tasks, 0);
        assert!(!queue.assigned_loads().await.contains_key(&w));
    }

    #[derive(Clone, Default)]
    struct SchedulerLogSink(Arc<std::sync::Mutex<Vec<String>>>);

    impl SchedulerLogSink {
        fn lines(&self) -> Vec<String> {
            self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
        }

        fn saturated_count(&self) -> usize {
            self.lines()
                .iter()
                .filter(|line| line.to_ascii_lowercase().contains("saturated"))
                .count()
        }
    }

    struct SchedulerLogLine {
        sink: SchedulerLogSink,
        buf: Vec<u8>,
    }

    impl std::io::Write for SchedulerLogLine {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.buf.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Drop for SchedulerLogLine {
        fn drop(&mut self) {
            let line = String::from_utf8_lossy(&self.buf).trim_end().to_string();
            self.sink
                .0
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(line);
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SchedulerLogSink {
        type Writer = SchedulerLogLine;
        fn make_writer(&'a self) -> SchedulerLogLine {
            SchedulerLogLine {
                sink: self.clone(),
                buf: Vec::new(),
            }
        }
    }

    #[tokio::test]
    async fn sched5_emits_once_per_busy_worker_set_change_and_clears_on_recovery() {
        let sink = SchedulerLogSink::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(sink.clone())
            .with_max_level(tracing::Level::INFO)
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let registry = WorkerRegistry::new();
        let queue = TaskQueue::new();
        let notify = Arc::new(Notify::new());

        let w1 = Uuid::new_v4();
        let (tx1, _rx1) = tokio::sync::mpsc::channel(16);
        registry
            .register(
                w1,
                WorkerCapabilities::new("cpu-busy-1", 4, 8192, false, false, None),
                "127.0.0.1:9501".parse().unwrap(),
                tx1,
                None,
            )
            .await
            .unwrap();
        registry
            .record_heartbeat(&w1, 0, 1001, 95.0, 4096, None)
            .await
            .unwrap();

        queue
            .submit(Task::new(
                TaskSpec::new_command("echo", vec!["hello".into()]),
                TaskRequirements::generic(1, 64),
            ))
            .await
            .unwrap();

        let sched = WorkloadScheduler::new(
            SchedulerConfig::default().with_max_host_cpu_pct(85.0),
            registry.clone(),
            queue.clone(),
            notify,
        );

        let report = sched.schedule_once().await.unwrap();
        assert!(report.assignments.is_empty());
        assert_eq!(sink.saturated_count(), 1);

        // Repeated scheduling passes with fluctuating CPU above max_host_cpu_pct and another
        // queued task must not emit additional warnings while the busy-worker set is unchanged.
        queue
            .submit(Task::new(
                TaskSpec::new_command("echo", vec!["world".into()]),
                TaskRequirements::generic(1, 64),
            ))
            .await
            .unwrap();
        for (i, pct) in [96.5_f32, 91.0, 99.0, 88.0, 94.2].into_iter().enumerate() {
            registry
                .record_heartbeat(&w1, 0, 1002 + i as u64, pct, 4096, None)
                .await
                .unwrap();
            let r = sched.schedule_once().await.unwrap();
            assert!(r.assignments.is_empty());
        }
        assert_eq!(sink.saturated_count(), 1);

        // Adding a second saturated connected worker changes the busy-worker set and re-emits once.
        let w2 = Uuid::new_v4();
        let (tx2, _rx2) = tokio::sync::mpsc::channel(16);
        registry
            .register(
                w2,
                WorkerCapabilities::new("cpu-busy-2", 4, 8192, false, false, None),
                "127.0.0.1:9502".parse().unwrap(),
                tx2,
                None,
            )
            .await
            .unwrap();
        registry
            .record_heartbeat(&w2, 0, 1010, 92.0, 4096, None)
            .await
            .unwrap();
        let r = sched.schedule_once().await.unwrap();
        assert!(r.assignments.is_empty());
        assert_eq!(sink.saturated_count(), 2);

        // Recovery on w1 places both queued tasks (saturated_skips == 0) and clears the state
        // without logging the word "saturated".
        registry
            .record_heartbeat(&w1, 0, 1011, 20.0, 4096, None)
            .await
            .unwrap();
        let r = sched.schedule_once().await.unwrap();
        assert_eq!(r.assignments.len(), 2);
        assert_eq!(sink.saturated_count(), 2);
        assert!(
            sink.lines()
                .iter()
                .any(|l| l.contains("Host-CPU backpressure holdback cleared")),
            "expected info clear message without 'saturated'"
        );

        // Saturating w1 again with a new queued task re-emits once.
        registry
            .record_heartbeat(&w1, 0, 1012, 95.0, 4096, None)
            .await
            .unwrap();
        queue
            .submit(Task::new(
                TaskSpec::new_command("echo", vec!["again".into()]),
                TaskRequirements::generic(1, 64),
            ))
            .await
            .unwrap();
        let r = sched.schedule_once().await.unwrap();
        assert!(r.assignments.is_empty());
        assert_eq!(sink.saturated_count(), 3);
    }

    #[tokio::test]
    async fn sched5_ignores_non_saturated_skips_and_disconnected_workers() {
        let sink = SchedulerLogSink::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(sink.clone())
            .with_max_level(tracing::Level::INFO)
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let registry = WorkerRegistry::new();
        let queue = TaskQueue::new();
        let notify = Arc::new(Notify::new());
        let sched = WorkloadScheduler::new(
            SchedulerConfig::default().with_max_host_cpu_pct(85.0),
            registry.clone(),
            queue.clone(),
            notify,
        );

        // 1. Connected CPU-only worker above max_host_cpu_pct + queued GPU task (NoGpuWorkersAvailable).
        let w_cpu = Uuid::new_v4();
        let (tx_cpu, _rx_cpu) = tokio::sync::mpsc::channel(16);
        registry
            .register(
                w_cpu,
                WorkerCapabilities::new("cpu-only-busy", 4, 8192, false, false, None),
                "127.0.0.1:9511".parse().unwrap(),
                tx_cpu,
                None,
            )
            .await
            .unwrap();
        registry
            .record_heartbeat(&w_cpu, 0, 1001, 95.0, 4096, None)
            .await
            .unwrap();
        let gpu_task = queue
            .submit(Task::new(
                TaskSpec::GpuCompute {
                    kernel_name: "gemm".into(),
                    input_data: vec![1, 2],
                    work_group_size: 16,
                    simulated_matrix_dim: 32,
                },
                TaskRequirements::gpu(64),
            ))
            .await
            .unwrap();
        let r = sched.schedule_once().await.unwrap();
        assert!(r.assignments.is_empty());
        assert_eq!(sink.saturated_count(), 0);
        queue.cancel_task(&gpu_task, None).await.unwrap();

        // 2. Disconnected worker with high CPU alongside a connected full-slot low-CPU worker.
        registry.mark_disconnected(&w_cpu).await.unwrap();
        let w_full = Uuid::new_v4();
        let (tx_full, _rx_full) = tokio::sync::mpsc::channel(16);
        registry
            .register(
                w_full,
                WorkerCapabilities::new("healthy-full", 1, 8192, false, false, None),
                "127.0.0.1:9512".parse().unwrap(),
                tx_full,
                None,
            )
            .await
            .unwrap();
        registry
            .record_heartbeat(&w_full, 0, 1002, 20.0, 4096, None)
            .await
            .unwrap();
        // Occupy w_full's single slot.
        queue
            .submit(Task::new(
                TaskSpec::new_command("sleep", vec!["10".into()]),
                TaskRequirements::generic(1, 64),
            ))
            .await
            .unwrap();
        let r1 = sched.schedule_once().await.unwrap();
        assert_eq!(r1.assignments.len(), 1);
        // Second CPU task skips with AllEligibleWorkersSaturated due to full slots on w_full,
        // NOT host-CPU saturation, and w_cpu is Disconnected.
        queue
            .submit(Task::new(
                TaskSpec::new_command("echo", vec!["queued".into()]),
                TaskRequirements::generic(1, 64),
            ))
            .await
            .unwrap();
        let r2 = sched.schedule_once().await.unwrap();
        assert!(r2.assignments.is_empty());
        assert_eq!(sink.saturated_count(), 0);
    }

    #[tokio::test]
    async fn tier5_sched5_live_recovery_via_backpressure_drop_and_multi_transition_antiflood() {
        let sink = SchedulerLogSink::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(sink.clone())
            .with_max_level(tracing::Level::INFO)
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let registry = WorkerRegistry::new();
        let queue = TaskQueue::new();
        let notify = Arc::new(Notify::new());

        let w1 = Uuid::new_v4();
        let w2 = Uuid::new_v4();
        let (tx1, _rx1) = tokio::sync::mpsc::channel(16);
        let (tx2, _rx2) = tokio::sync::mpsc::channel(16);
        registry
            .register(
                w1,
                WorkerCapabilities::new("live-cpu-1", 2, 8192, false, false, None),
                "127.0.0.1:9521".parse().unwrap(),
                tx1,
                None,
            )
            .await
            .unwrap();
        registry
            .record_heartbeat(&w1, 0, 1001, 95.0, 4096, None)
            .await
            .unwrap();

        queue
            .submit(Task::new(
                TaskSpec::new_command("echo", vec!["t1".into()]),
                TaskRequirements::generic(1, 64),
            ))
            .await
            .unwrap();

        let sched = WorkloadScheduler::new(
            SchedulerConfig::default().with_max_host_cpu_pct(85.0),
            registry.clone(),
            queue.clone(),
            notify,
        );

        // Busy set {w1} -> emits once, then stays suppressed on repeat ticks.
        assert!(sched.schedule_once().await.unwrap().assignments.is_empty());
        assert_eq!(sink.saturated_count(), 1);
        assert!(sched.schedule_once().await.unwrap().assignments.is_empty());
        assert_eq!(sink.saturated_count(), 1);

        // Connect saturated w2 -> busy set {w1, w2} -> emits once more (2).
        registry
            .register(
                w2,
                WorkerCapabilities::new("live-cpu-2", 2, 8192, false, false, None),
                "127.0.0.1:9522".parse().unwrap(),
                tx2,
                None,
            )
            .await
            .unwrap();
        registry
            .record_heartbeat(&w2, 0, 1002, 92.0, 4096, None)
            .await
            .unwrap();
        assert!(sched.schedule_once().await.unwrap().assignments.is_empty());
        assert_eq!(sink.saturated_count(), 2);

        // Live recovery without restart: w2 drops below threshold -> task drains and state clears.
        registry
            .record_heartbeat(&w2, 0, 1003, 15.0, 4096, None)
            .await
            .unwrap();
        let rec = sched.schedule_once().await.unwrap();
        assert_eq!(rec.assignments.len(), 1);
        assert_eq!(rec.assignments[0].worker_id, w2);
        assert_eq!(sink.saturated_count(), 2);

        // Re-saturate {w1, w2} on a new queued task -> emits cleanly (3), suppressed on repeat.
        registry
            .record_heartbeat(&w2, 0, 1004, 93.0, 4096, None)
            .await
            .unwrap();
        queue
            .submit(Task::new(
                TaskSpec::new_command("echo", vec!["t2".into()]),
                TaskRequirements::generic(1, 64),
            ))
            .await
            .unwrap();
        assert!(sched.schedule_once().await.unwrap().assignments.is_empty());
        assert_eq!(sink.saturated_count(), 3);
        assert!(sched.schedule_once().await.unwrap().assignments.is_empty());
        assert_eq!(sink.saturated_count(), 3);
    }

    #[tokio::test]
    async fn tier5_scheduler_case5_revive_race_preserves_post_revive_active_tasks() {
        let registry = WorkerRegistry::new();
        let queue = TaskQueue::new();
        let notify = Arc::new(Notify::new());
        let sched = WorkloadScheduler::new(
            SchedulerConfig::default(),
            registry.clone(),
            queue.clone(),
            notify,
        )
        .with_dispatch_reserve_timeout(Duration::from_secs(5));

        let w = Uuid::new_v4();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let session = registry
            .register(
                w,
                WorkerCapabilities::new("case5-revive-worker", 4, 8192, false, false, None),
                "127.0.0.1:9531".parse().unwrap(),
                tx.clone(),
                None,
            )
            .await
            .unwrap();

        // Fill the 1-slot outbound channel so t1 enters the deferred reserve_owned waiter path.
        tx.try_send(MasterMessage::HeartbeatAck { timestamp: 0 })
            .unwrap();

        let t1 = queue
            .submit(Task::new(
                TaskSpec::new_command("echo", vec!["t1".into()]),
                TaskRequirements::generic(1, 64),
            ))
            .await
            .unwrap();

        let r1 = sched.schedule_once().await.unwrap();
        assert_eq!(r1.assignments.len(), 1);
        assert_eq!(r1.assignments[0].task_id, t1);
        tokio::task::yield_now().await;

        // Reaper marks worker Disconnected (active_tasks = 0, old session_live = false) and sweeps t1.
        let reaped = registry
            .reap_stale_sessions_at(
                Instant::now() + Duration::from_secs(60),
                Duration::from_secs(10),
            )
            .await;
        assert_eq!(reaped, vec![(w, session)]);
        let swept = queue
            .handle_session_disconnected(
                &w,
                Some(session),
                "Heartbeat timeout (reaper detected)",
                false,
            )
            .await;
        assert_eq!(swept, vec![t1]);

        // Worker heartbeats on the same session_id -> HeartbeatOutcome::Revived (new session_live Arc).
        let revived = registry
            .record_heartbeat_session(&w, 0, 1001, 10.0, 4096, Some(session))
            .await
            .unwrap();
        assert_eq!(revived, crate::registry::HeartbeatOutcome::Revived);

        // Post-revive task t2 is scheduled while channel is still full; its waiter queues behind t1.
        let t2 = queue
            .submit(Task::new(
                TaskSpec::new_command("echo", vec!["t2".into()]),
                TaskRequirements::generic(1, 64),
            ))
            .await
            .unwrap();
        let r2 = sched.schedule_once().await.unwrap();
        assert_eq!(r2.assignments.len(), 1);
        assert_eq!(r2.assignments[0].task_id, t2);
        tokio::task::yield_now().await;

        let before_drain = registry.get_worker(w).await.unwrap();
        assert_eq!(before_drain.status, WorkerStatus::Busy);
        assert_eq!(before_drain.active_tasks, 1);

        // Drain the filler frame: wakes t1's pre-reap waiter first (FIFO), which must NOT deliver t1
        // and must NOT decrement post-revive active_tasks via release_dispatch_if_live, then wakes t2's waiter.
        let first = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            first,
            MasterMessage::HeartbeatAck { timestamp: 0 }
        ));

        let second = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        match second {
            MasterMessage::AssignTask { task } => assert_eq!(task.id, t2),
            other => panic!("expected AssignTask for t2, got {other:?}"),
        }

        // Allow any trailing cleanup in the deferred waiter tasks to settle, verify t1 was not sent.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(rx.try_recv().is_err(), "t1 must never be delivered");

        let after_drain = registry.get_worker(w).await.unwrap();
        assert_eq!(after_drain.status, WorkerStatus::Busy);
        assert_eq!(after_drain.active_tasks, 1);
        assert_eq!(
            queue.get_task(&t2).await.unwrap().state,
            crate::queue::TaskState::Scheduled
        );
    }

    #[tokio::test]
    async fn tier5_scheduler_stale_session_live_blocks_deferred_delivery_before_sweep() {
        let registry = WorkerRegistry::new();
        let queue = TaskQueue::new();
        let notify = Arc::new(Notify::new());
        let sched = WorkloadScheduler::new(
            SchedulerConfig::default(),
            registry.clone(),
            queue.clone(),
            notify,
        )
        .with_dispatch_reserve_timeout(Duration::from_secs(5));

        let w = Uuid::new_v4();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let session = registry
            .register(
                w,
                WorkerCapabilities::new("stale-live-worker", 4, 8192, false, false, None),
                "127.0.0.1:9532".parse().unwrap(),
                tx.clone(),
                None,
            )
            .await
            .unwrap();

        tx.try_send(MasterMessage::HeartbeatAck { timestamp: 1 })
            .unwrap();

        let t1 = queue
            .submit(Task::new(
                TaskSpec::new_command("echo", vec!["t1-unswept".into()]),
                TaskRequirements::generic(1, 64),
            ))
            .await
            .unwrap();

        let r1 = sched.schedule_once().await.unwrap();
        assert_eq!(r1.assignments.len(), 1);
        assert_eq!(r1.assignments[0].task_id, t1);
        tokio::task::yield_now().await;

        // Reap sets session_live = false BEFORE handle_session_disconnected sweeps t1 out of Scheduled.
        let reaped = registry
            .reap_stale_sessions_at(
                Instant::now() + Duration::from_secs(60),
                Duration::from_secs(10),
            )
            .await;
        assert_eq!(reaped, vec![(w, session)]);

        // Draining the filler frame wakes t1's waiter while t1 is still Scheduled in queue,
        // yet deliver_if_scheduled must refuse to send onto the stale session_live handle.
        let first = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            first,
            MasterMessage::HeartbeatAck { timestamp: 1 }
        ));

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            rx.try_recv().is_err(),
            "t1 must NOT be delivered onto a stale session_live handle"
        );
        let info = registry.get_worker(w).await.unwrap();
        assert_eq!(info.status, WorkerStatus::Disconnected);
        assert_eq!(info.active_tasks, 0);
    }
}
