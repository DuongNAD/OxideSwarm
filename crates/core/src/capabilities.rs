//! Worker hardware capability models, auto-detection, and requirement matching.

use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::task::TaskRequirements;

/// Battery status tier for mobile load-shedding and concurrency limiting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum BatteryTier {
    Normal = 0,
    Low = 1,
    SubLow = 2,
    Critical = 3,
}

impl BatteryTier {
    pub fn from_readings(pct: Option<u8>, is_charging: Option<bool>) -> Self {
        MobileLiveStatus::compute_battery_tier(pct, is_charging)
    }
}

/// Thermal status tier for mobile load-shedding and hardware protection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ThermalTier {
    Normal = 0,
    Warm = 1,
    Hot = 2,
    Critical = 3,
}

impl ThermalTier {
    pub fn from_temp_c(temp: Option<f32>, thermal_throttled: bool) -> Self {
        MobileLiveStatus::compute_thermal_tier(temp, thermal_throttled)
    }
}

/// Mobile-specific hardware and environmental telemetry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileCapabilities {
    /// Operating system version and API level (e.g. "Android 14 (API 34)", "macOS 14.5").
    pub os_version: String,
    /// System-on-Chip descriptor (e.g. "Snapdragon 8 Gen 2", "Apple M2 Max").
    pub soc_model: String,
    /// Battery charge percentage (0-100), or `None` if running on AC power or emulator.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub battery_pct: Option<u8>,
    /// Whether the device is actively drawing external power / charging.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_charging: Option<bool>,
    /// Whether the device is currently experiencing CPU/GPU thermal throttling.
    #[serde(default)]
    pub thermal_throttled: bool,
}

impl Default for MobileCapabilities {
    fn default() -> Self {
        Self {
            os_version: "unknown".into(),
            soc_model: "unknown".into(),
            battery_pct: None,
            is_charging: None,
            thermal_throttled: false,
        }
    }
}

impl MobileCapabilities {
    /// Returns a copy with the fast-changing readings replaced by `status`; the OS version and SoC
    /// descriptor, which never change while the process runs, are kept.
    pub fn with_live_status(&self, status: MobileLiveStatus) -> Self {
        Self {
            os_version: self.os_version.clone(),
            soc_model: self.soc_model.clone(),
            battery_pct: status.battery_pct,
            is_charging: status.is_charging,
            thermal_throttled: status.thermal_throttled,
        }
    }

    /// The fast-changing readings of this snapshot.
    pub fn live_status(&self) -> MobileLiveStatus {
        MobileLiveStatus {
            battery_pct: self.battery_pct,
            is_charging: self.is_charging,
            thermal_throttled: self.thermal_throttled,
            battery_temperature: None,
            network_type: None,
            battery_tier: None,
            thermal_tier: None,
        }
    }
}

/// The part of [`MobileCapabilities`] that changes while a worker runs: what has to be re-read
/// (or pushed by the platform, e.g. Android's `BatteryManager`) to keep scheduling decisions fresh.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MobileLiveStatus {
    /// Battery charge percentage (0-100), `None` when unknown or on AC power.
    pub battery_pct: Option<u8>,
    /// Whether the device is drawing external power, `None` when unknown.
    pub is_charging: Option<bool>,
    /// Whether the device is currently thermally throttled.
    pub thermal_throttled: bool,
    /// Battery temperature in degrees Celsius (from HAL/sysfs).
    pub battery_temperature: Option<f32>,
    /// Network connection type (e.g., "wifi", "cellular", "ethernet").
    pub network_type: Option<String>,
    /// Computed battery status tier.
    pub battery_tier: Option<BatteryTier>,
    /// Computed thermal status tier.
    pub thermal_tier: Option<ThermalTier>,
}

impl PartialEq for MobileLiveStatus {
    fn eq(&self, other: &Self) -> bool {
        self.battery_pct == other.battery_pct
            && self.is_charging == other.is_charging
            && self.thermal_throttled == other.thermal_throttled
            && match (self.battery_temperature, other.battery_temperature) {
                (Some(a), Some(b)) => a.to_bits() == b.to_bits(),
                (None, None) => true,
                _ => false,
            }
            && self.network_type == other.network_type
            && self.battery_tier == other.battery_tier
            && self.thermal_tier == other.thermal_tier
    }
}

impl Eq for MobileLiveStatus {}

impl MobileLiveStatus {
    /// Computes the battery tier based on state of charge and charging status.
    pub fn compute_battery_tier(pct: Option<u8>, is_charging: Option<bool>) -> BatteryTier {
        if is_charging.unwrap_or(false) {
            return BatteryTier::Normal;
        }
        match pct {
            Some(p) if p >= 50 => BatteryTier::Normal,
            Some(p) if p >= 30 => BatteryTier::Low,
            Some(p) if p >= 15 => BatteryTier::SubLow,
            Some(_) => BatteryTier::Critical,
            None => BatteryTier::Normal,
        }
    }

    /// Computes the thermal tier based on battery/chassis temperature and OS throttle flag.
    pub fn compute_thermal_tier(temp: Option<f32>, thermal_throttled: bool) -> ThermalTier {
        if let Some(t) = temp {
            if t > 42.0 {
                ThermalTier::Critical
            } else if t > 40.0 {
                ThermalTier::Hot
            } else if t > 37.0 || thermal_throttled {
                ThermalTier::Warm
            } else {
                ThermalTier::Normal
            }
        } else if thermal_throttled {
            ThermalTier::Warm
        } else {
            ThermalTier::Normal
        }
    }

    /// Derives effective concurrency capacity: C_eff = min(C_bat, C_therm).
    pub fn effective_concurrency(&self, max_cores: usize) -> usize {
        let bat = self
            .battery_tier
            .unwrap_or_else(|| Self::compute_battery_tier(self.battery_pct, self.is_charging));
        let therm = self.thermal_tier.unwrap_or_else(|| {
            Self::compute_thermal_tier(self.battery_temperature, self.thermal_throttled)
        });

        calculate_effective_concurrency(max_cores, bat, therm)
    }

    /// Calculates RRC-aware heartbeat interval in seconds.
    pub fn recommended_keepalive_interval(&self, active_tasks: usize) -> Duration {
        let is_cellular = self.network_type.as_ref().is_some_and(|net| {
            let n = net.to_ascii_lowercase();
            n.contains("cellular")
                || n.contains("mobile")
                || n.contains("lte")
                || n.contains("5g")
                || n.contains("4g")
                || n.contains("3g")
                || n.contains("cgnat")
        });

        if is_cellular {
            if active_tasks > 0 {
                Duration::from_secs(10)
            } else {
                Duration::from_secs(25)
            }
        } else {
            Duration::from_secs(60)
        }
    }
}

/// Derives effective concurrency capacity: C_eff = min(C_bat, C_therm).
pub fn calculate_effective_concurrency(
    max_cores: usize,
    bat: BatteryTier,
    therm: ThermalTier,
) -> usize {
    let c_bat = match bat {
        BatteryTier::Normal => max_cores,
        BatteryTier::Low => (max_cores / 2).max(1),
        BatteryTier::SubLow => 1,
        BatteryTier::Critical => 0,
    };
    let c_therm = match therm {
        ThermalTier::Normal => max_cores,
        ThermalTier::Warm => (max_cores / 2).max(1),
        ThermalTier::Hot => 1,
        ThermalTier::Critical => 0,
    };
    c_bat.min(c_therm)
}

/// Helper function to check task eligibility under battery and thermal tiers.
pub fn is_task_eligible(
    spec: &crate::task::TaskSpec,
    bat: BatteryTier,
    therm: ThermalTier,
    is_charging: bool,
) -> bool {
    let eff = calculate_effective_concurrency(8, bat, therm);
    if eff == 0 {
        return false;
    }
    let is_heavy = matches!(
        spec,
        crate::task::TaskSpec::RustCompilation { .. } | crate::task::TaskSpec::GpuCompute { .. }
    );
    match bat {
        BatteryTier::Critical => return false,
        BatteryTier::SubLow | BatteryTier::Low => {
            if is_heavy {
                return false;
            }
        }
        BatteryTier::Normal => {}
    }
    match therm {
        ThermalTier::Critical => return false,
        ThermalTier::Hot => {
            if is_heavy {
                return false;
            }
        }
        ThermalTier::Warm => {
            if is_heavy && !is_charging {
                return false;
            }
        }
        ThermalTier::Normal => {}
    }
    true
}

/// Calculates RRC-aware heartbeat interval in seconds.
pub fn calculate_keepalive_interval(network_type: &str, active_tasks: usize) -> Duration {
    let n = network_type.to_ascii_lowercase();
    let is_cellular = n.contains("cellular")
        || n.contains("mobile")
        || n.contains("lte")
        || n.contains("5g")
        || n.contains("4g")
        || n.contains("3g")
        || n.contains("cgnat");

    if is_cellular {
        if active_tasks > 0 {
            Duration::from_secs(10)
        } else {
            Duration::from_secs(25)
        }
    } else {
        Duration::from_secs(60)
    }
}

/// Hardware capability override inputs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HardwareOverrides {
    pub name: Option<String>,
    pub cores: Option<usize>,
    pub ram_mb: Option<u64>,
    pub gpu: Option<bool>,
    pub no_gpu: bool,
    pub simulate_gpu: bool,
    pub gpu_name: Option<String>,
    // --- 3D and Compute Overrides (Milestone 1) ---
    pub blender: Option<bool>,
    pub no_blender: bool,
    pub blender_path: Option<String>,
    pub blender_version: Option<String>,
    pub vram_mb: Option<u64>,
    pub cuda: Option<bool>,
    pub no_cuda: bool,
}

/// Represents the hardware resources and computational capabilities of a Worker node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerCapabilities {
    /// Human-readable identifier or hostname for the worker.
    pub name: String,
    /// Number of available CPU cores for execution.
    pub cpu_cores: usize,
    /// Total system RAM in Megabytes (MB).
    pub ram_mb: u64,
    /// Whether a physical hardware GPU is available.
    pub has_gpu: bool,
    /// Whether the worker is configured to simulate GPU compute workloads.
    pub is_simulated_gpu: bool,
    /// Human-readable model or descriptor of the GPU.
    pub gpu_device_name: Option<String>,
    /// Optional categorization or affinity tags.
    pub tags: Vec<String>,
    /// Optional mobile capabilities (populated automatically on Android, iOS, or battery-powered devices).
    pub mobile: Option<MobileCapabilities>,
    // --- 3D & Hardware Acceleration Telemetry (Milestone 1) ---
    /// Whether the host has Blender installed and available for headless auto-rigging.
    pub has_blender: bool,
    /// Discovered or reported Blender version (e.g. "Blender 4.5.4 LTS").
    pub blender_version: Option<String>,
    /// Dedicated Video RAM in Megabytes (MB), if GPU is present.
    pub vram_mb: Option<u64>,
    /// Whether the host supports NVIDIA CUDA execution.
    pub has_cuda: bool,
}

#[derive(Serialize, Deserialize)]
struct HumanWorkerCapabilities {
    pub name: String,
    pub cpu_cores: usize,
    pub ram_mb: u64,
    pub has_gpu: bool,
    pub is_simulated_gpu: bool,
    pub gpu_device_name: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mobile: Option<MobileCapabilities>,
    #[serde(default)]
    pub has_blender: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blender_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vram_mb: Option<u64>,
    #[serde(default)]
    pub has_cuda: bool,
}

#[derive(Serialize, Deserialize)]
struct BinaryWorkerCapabilities {
    pub name: String,
    pub cpu_cores: usize,
    pub ram_mb: u64,
    pub has_gpu: bool,
    pub is_simulated_gpu: bool,
    pub gpu_device_name: Option<String>,
    pub tags: Vec<String>,
    pub mobile: Option<MobileCapabilities>,
    #[serde(default)]
    pub has_blender: bool,
    pub blender_version: Option<String>,
    pub vram_mb: Option<u64>,
    #[serde(default)]
    pub has_cuda: bool,
}

impl From<WorkerCapabilities> for HumanWorkerCapabilities {
    fn from(w: WorkerCapabilities) -> Self {
        Self {
            name: w.name,
            cpu_cores: w.cpu_cores,
            ram_mb: w.ram_mb,
            has_gpu: w.has_gpu,
            is_simulated_gpu: w.is_simulated_gpu,
            gpu_device_name: w.gpu_device_name,
            tags: w.tags,
            mobile: w.mobile,
            has_blender: w.has_blender,
            blender_version: w.blender_version,
            vram_mb: w.vram_mb,
            has_cuda: w.has_cuda,
        }
    }
}

impl From<HumanWorkerCapabilities> for WorkerCapabilities {
    fn from(h: HumanWorkerCapabilities) -> Self {
        Self {
            name: h.name,
            cpu_cores: h.cpu_cores,
            ram_mb: h.ram_mb,
            has_gpu: h.has_gpu,
            is_simulated_gpu: h.is_simulated_gpu,
            gpu_device_name: h.gpu_device_name,
            tags: h.tags,
            mobile: h.mobile,
            has_blender: h.has_blender,
            blender_version: h.blender_version,
            vram_mb: h.vram_mb,
            has_cuda: h.has_cuda,
        }
    }
}

impl From<WorkerCapabilities> for BinaryWorkerCapabilities {
    fn from(w: WorkerCapabilities) -> Self {
        Self {
            name: w.name,
            cpu_cores: w.cpu_cores,
            ram_mb: w.ram_mb,
            has_gpu: w.has_gpu,
            is_simulated_gpu: w.is_simulated_gpu,
            gpu_device_name: w.gpu_device_name,
            tags: w.tags,
            mobile: w.mobile,
            has_blender: w.has_blender,
            blender_version: w.blender_version,
            vram_mb: w.vram_mb,
            has_cuda: w.has_cuda,
        }
    }
}

impl From<BinaryWorkerCapabilities> for WorkerCapabilities {
    fn from(b: BinaryWorkerCapabilities) -> Self {
        Self {
            name: b.name,
            cpu_cores: b.cpu_cores,
            ram_mb: b.ram_mb,
            has_gpu: b.has_gpu,
            is_simulated_gpu: b.is_simulated_gpu,
            gpu_device_name: b.gpu_device_name,
            tags: b.tags,
            mobile: b.mobile,
            has_blender: b.has_blender,
            blender_version: b.blender_version,
            vram_mb: b.vram_mb,
            has_cuda: b.has_cuda,
        }
    }
}

impl Serialize for WorkerCapabilities {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            HumanWorkerCapabilities::from(self.clone()).serialize(serializer)
        } else {
            BinaryWorkerCapabilities::from(self.clone()).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for WorkerCapabilities {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            HumanWorkerCapabilities::deserialize(deserializer).map(Into::into)
        } else {
            BinaryWorkerCapabilities::deserialize(deserializer).map(Into::into)
        }
    }
}

impl Default for WorkerCapabilities {
    fn default() -> Self {
        Self {
            name: "default-worker".to_string(),
            cpu_cores: 1,
            ram_mb: 1024,
            has_gpu: false,
            is_simulated_gpu: false,
            gpu_device_name: None,
            tags: Vec::new(),
            mobile: None,
            has_blender: false,
            blender_version: None,
            vram_mb: None,
            has_cuda: false,
        }
    }
}

impl WorkerCapabilities {
    /// Creates an explicit WorkerCapabilities instance.
    /// Preserves exact 6-parameter signature for 100% backward compatibility.
    pub fn new(
        name: impl Into<String>,
        cpu_cores: usize,
        ram_mb: u64,
        has_gpu: bool,
        is_simulated_gpu: bool,
        gpu_device_name: Option<String>,
    ) -> Self {
        Self {
            name: name.into(),
            cpu_cores,
            ram_mb,
            has_gpu,
            is_simulated_gpu,
            gpu_device_name,
            tags: Vec::new(),
            mobile: None,
            has_blender: false,
            blender_version: None,
            vram_mb: None,
            has_cuda: false,
        }
    }

    /// Builder method to attach tags.
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    /// Builder method to attach mobile capabilities.
    pub fn with_mobile(mut self, mobile: Option<MobileCapabilities>) -> Self {
        self.mobile = mobile;
        self
    }

    /// Builder method to attach Blender capability and version.
    pub fn with_blender(mut self, has_blender: bool, version: Option<String>) -> Self {
        self.has_blender = has_blender;
        self.blender_version = version;
        self
    }

    /// Builder method to attach VRAM in MB.
    pub fn with_vram_mb(mut self, vram_mb: Option<u64>) -> Self {
        self.vram_mb = vram_mb;
        self
    }

    /// Builder method to attach CUDA capability.
    pub fn with_cuda(mut self, has_cuda: bool) -> Self {
        self.has_cuda = has_cuda;
        self
    }

    /// Builder method to attach comprehensive 3D capabilities.
    pub fn with_3d_capabilities(
        mut self,
        has_blender: bool,
        blender_version: Option<String>,
        vram_mb: Option<u64>,
        has_cuda: bool,
    ) -> Self {
        self.has_blender = has_blender;
        self.blender_version = blender_version;
        self.vram_mb = vram_mb;
        self.has_cuda = has_cuda;
        self
    }

    /// Automatically detects host system capabilities, applying hardware overrides.
    pub fn detect_with_overrides(overrides: HardwareOverrides) -> Self {
        let name = overrides.name.unwrap_or_else(detect_worker_name);

        let cpu_cores = match overrides.cores {
            Some(c) if c > 0 => c,
            _ => std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1),
        };

        let ram_mb = match overrides.ram_mb {
            Some(r) if r > 0 => r,
            _ => detect_system_ram_mb(),
        };

        let (has_gpu, is_simulated_gpu, gpu_device_name) = if overrides.no_gpu {
            (false, false, None)
        } else if overrides.simulate_gpu {
            (
                true,
                true,
                Some(
                    overrides
                        .gpu_name
                        .unwrap_or_else(|| "Simulated Virtual GPU (Matrix Engine)".to_string()),
                ),
            )
        } else if overrides.gpu == Some(true) {
            (
                true,
                false,
                Some(
                    overrides
                        .gpu_name
                        .unwrap_or_else(|| "Physical GPU (Manual Override)".to_string()),
                ),
            )
        } else {
            let (has, sim, dev) = detect_physical_gpu();
            if has {
                (has, sim, overrides.gpu_name.or(dev))
            } else {
                (false, false, None)
            }
        };

        // --- Blender Detection & Overrides ---
        let (has_blender, blender_version) = if overrides.no_blender {
            (false, None)
        } else if overrides.blender == Some(true) {
            let ver = overrides
                .blender_version
                .or_else(|| probe_blender_version_from_path(overrides.blender_path.as_deref()))
                .or_else(|| Some("Blender (Manual Override)".to_string()));
            (true, ver)
        } else if overrides.blender == Some(false) {
            (false, None)
        } else {
            let env_no_blender = std::env::var("RUSTY_GRID_NO_BLENDER")
                .or_else(|_| std::env::var("OXIDE_SWARM_NO_BLENDER"))
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);

            if env_no_blender {
                (false, None)
            } else {
                detect_blender(overrides.blender_path.as_deref())
            }
        };

        // --- VRAM Detection & Overrides ---
        let vram_mb = if overrides.no_gpu {
            None
        } else if let Some(vram) = overrides.vram_mb {
            Some(vram)
        } else if let Ok(vram_str) = std::env::var("RUSTY_GRID_VRAM_MB").or_else(|_| std::env::var("OXIDE_SWARM_VRAM_MB")) {
            vram_str.trim().parse::<u64>().ok()
        } else if has_gpu {
            detect_gpu_vram_mb()
        } else {
            None
        };

        // --- CUDA Detection & Overrides ---
        let has_cuda = if overrides.no_gpu || overrides.no_cuda {
            false
        } else if let Some(c) = overrides.cuda {
            c
        } else if let Ok(val) = std::env::var("RUSTY_GRID_CUDA").or_else(|_| std::env::var("OXIDE_SWARM_CUDA")) {
            val == "1" || val.eq_ignore_ascii_case("true")
        } else if let Ok(val) = std::env::var("RUSTY_GRID_NO_CUDA").or_else(|_| std::env::var("OXIDE_SWARM_NO_CUDA")) {
            if val == "1" || val.eq_ignore_ascii_case("true") {
                false
            } else {
                detect_cuda_support(has_gpu)
            }
        } else if has_gpu {
            detect_cuda_support(has_gpu)
        } else {
            false
        };

        let mobile = detect_mobile_capabilities();

        Self {
            name,
            cpu_cores,
            ram_mb,
            has_gpu,
            is_simulated_gpu,
            gpu_device_name,
            tags: Vec::new(),
            mobile,
            has_blender,
            blender_version,
            vram_mb,
            has_cuda,
        }
    }

    /// Automatically detects host system capabilities, applying optional CLI overrides.
    pub fn detect(
        name_override: Option<String>,
        cores_override: Option<usize>,
        simulate_gpu: bool,
    ) -> Self {
        Self::detect_with_overrides(HardwareOverrides {
            name: name_override,
            cores: cores_override,
            simulate_gpu,
            ..Default::default()
        })
    }

    /// Returns true if this worker has GPU compute capabilities (either physical or simulated).
    #[inline]
    pub fn can_execute_gpu(&self) -> bool {
        self.has_gpu || self.is_simulated_gpu
    }

    /// Evaluates whether this worker satisfies the requirements of a given task.
    pub fn satisfies(&self, requirements: &TaskRequirements) -> bool {
        // 1. Strict GPU constraint (Requirement R3, AC4)
        if requirements.gpu_required && !self.can_execute_gpu() {
            return false;
        }

        // 2. CPU Core constraint
        if requirements.cpu_cores > 0 && self.cpu_cores < requirements.cpu_cores {
            return false;
        }

        // 3. RAM constraint
        if requirements.ram_mb > 0 && self.ram_mb < requirements.ram_mb {
            return false;
        }

        true
    }
}

/// Discovers a reasonable hostname or fallback worker name.
///
/// The OS hostname (`gethostname`, via sysinfo) comes first because `HOSTNAME` is a shell
/// variable that is normally *not* exported to child processes on macOS/Linux and does not exist
/// under launchd, Termux services or Android. The environment variables remain as fallbacks,
/// then `worker-<pid>`.
fn detect_worker_name() -> String {
    pick_worker_name(
        sysinfo::System::host_name(),
        std::env::var("HOSTNAME").ok(),
        std::env::var("COMPUTERNAME").ok(),
        std::process::id(),
    )
}

/// Chooses the first usable name: OS hostname, `HOSTNAME`, `COMPUTERNAME`, `worker-<pid>`.
/// Blank values and a bare "localhost" (Android's default, identical on every device) are skipped.
fn pick_worker_name(
    os_hostname: Option<String>,
    env_hostname: Option<String>,
    env_computername: Option<String>,
    pid: u32,
) -> String {
    [os_hostname, env_hostname, env_computername]
        .into_iter()
        .flatten()
        .map(|h| h.trim().to_string())
        .find(|h| !h.is_empty() && !h.eq_ignore_ascii_case("localhost"))
        .unwrap_or_else(|| format!("worker-{pid}"))
}

/// Converts a byte count to whole MiB, or `None` if it is below 1 MiB (i.e. not a real reading).
fn bytes_to_mb(bytes: u64) -> Option<u64> {
    let mb = bytes / (1024 * 1024);
    (mb > 0).then_some(mb)
}

/// Inspects host OS total memory in Megabytes.
///
/// `sysinfo` is the primary source on every OS (Linux, macOS, Windows, Android); Windows has
/// neither `/proc/meminfo` nor `sysctl`, which used to leave it at the hard-coded 8192 MB. The
/// older probes stay as fallbacks, and the final 8 GB guess is logged because it is only a guess.
fn detect_system_ram_mb() -> u64 {
    // 1. sysinfo (cross-platform total physical memory, reported in bytes)
    let mut sys = sysinfo::System::new();
    sys.refresh_memory();
    if let Some(mb) = bytes_to_mb(sys.total_memory()) {
        return mb;
    }

    // 2. Linux /proc/meminfo inspection
    if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
        for line in meminfo.lines() {
            if line.starts_with("MemTotal:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    if let Ok(kb) = parts[1].parse::<u64>() {
                        return kb / 1024;
                    }
                }
            }
        }
    }

    // 3. macOS / Darwin sysctl hw.memsize inspection
    #[cfg(unix)]
    {
        if let Ok(output) = std::process::Command::new("sysctl")
            .arg("-n")
            .arg("hw.memsize")
            .output()
        {
            if output.status.success() {
                if let Ok(s) = std::str::from_utf8(&output.stdout) {
                    if let Ok(bytes) = s.trim().parse::<u64>() {
                        return bytes / (1024 * 1024);
                    }
                }
            }
        }
    }

    // 4. Sensible fallback (8 GB)
    tracing::warn!("could not detect system RAM; assuming 8192 MB (override with --ram-mb)");
    8192
}

/// Detects physical GPU presence.
/// Note: To guarantee that local multi-worker tests (AC1-AC5) do not accidentally
/// flag CPU workers as GPU-capable on machines with integrated GPUs, this returns None
/// unless explicitly enabled by environment variable or verified discrete GPU driver.
fn detect_physical_gpu() -> (bool, bool, Option<String>) {
    if std::env::var("RUSTY_GRID_ENABLE_PHYSICAL_GPU").as_deref() == Ok("1") {
        // Optional probe for nvidia-smi
        if let Ok(output) = std::process::Command::new("nvidia-smi")
            .arg("--query-gpu=name")
            .arg("--format=csv,noheader")
            .output()
        {
            if output.status.success() {
                if let Ok(s) = std::str::from_utf8(&output.stdout) {
                    let name = s.trim();
                    if !name.is_empty() {
                        return (true, false, Some(name.to_string()));
                    }
                }
            }
        }
    }

    // Default: CPU-only worker
    (false, false, None)
}

/// Auto-detects mobile hardware attributes and real-time battery/thermal status.
/// Returns `None` on standard desktop/server environments without battery unless on Android.
pub fn detect_mobile_capabilities() -> Option<MobileCapabilities> {
    let is_android = is_android_env();
    let (battery_pct, is_charging) = detect_battery();
    let thermal_throttled = detect_thermal_throttling();
    let soc_model = detect_soc_model(is_android);
    let os_version = detect_os_version(is_android);

    if !is_android && battery_pct.is_none() {
        return None;
    }

    Some(MobileCapabilities {
        os_version,
        soc_model,
        battery_pct,
        is_charging,
        thermal_throttled,
    })
}

/// Re-reads only the battery and thermal state (no SoC / OS probing), cheap enough to run every
/// few seconds. Returns `None` on a machine without a readable battery that is not Android, i.e.
/// exactly when [`detect_mobile_capabilities`] would. Blocking (sysfs reads, `pmset`/`dumpsys`
/// child processes): call it from `spawn_blocking`.
pub fn detect_live_mobile_status() -> Option<MobileLiveStatus> {
    let (battery_pct, is_charging) = detect_battery();
    if !is_android_env() && battery_pct.is_none() {
        return None;
    }
    let thermal_throttled = detect_thermal_throttling();
    let bat_tier = MobileLiveStatus::compute_battery_tier(battery_pct, is_charging);
    let therm_tier = MobileLiveStatus::compute_thermal_tier(None, thermal_throttled);
    Some(MobileLiveStatus {
        battery_pct,
        is_charging,
        thermal_throttled,
        battery_temperature: None,
        network_type: None,
        battery_tier: Some(bat_tier),
        thermal_tier: Some(therm_tier),
    })
}

fn is_android_env() -> bool {
    cfg!(target_os = "android")
        || std::path::Path::new("/system/build.prop").exists()
        || std::path::Path::new("/system/bin/getprop").exists()
}

fn detect_battery() -> (Option<u8>, Option<bool>) {
    // 1. Linux/Android sysfs: /sys/class/power_supply
    let power_supply = std::path::Path::new("/sys/class/power_supply");
    if power_supply.is_dir() {
        for name in &["battery", "BAT0", "BAT1"] {
            let bat_dir = power_supply.join(name);
            if bat_dir.is_dir() {
                let capacity = std::fs::read_to_string(bat_dir.join("capacity"))
                    .ok()
                    .and_then(|s| s.trim().parse::<u8>().ok());
                let status_str =
                    std::fs::read_to_string(bat_dir.join("status")).unwrap_or_default();
                let status_trim = status_str.trim().to_lowercase();
                let is_charging = if status_trim == "charging" || status_trim == "full" {
                    Some(true)
                } else if status_trim == "discharging" || status_trim == "not charging" {
                    Some(false)
                } else {
                    None
                };
                if capacity.is_some() {
                    return (capacity, is_charging);
                }
            }
        }
    }

    // 2. macOS pmset fallback
    #[cfg(target_os = "macos")]
    {
        if let Ok(output) = std::process::Command::new("pmset")
            .arg("-g")
            .arg("batt")
            .output()
        {
            let text = String::from_utf8_lossy(&output.stdout);
            let mut pct: Option<u8> = None;
            let mut charging: Option<bool> = None;

            for part in text.split(';') {
                let p = part.trim();
                if p.ends_with('%') {
                    if let Some(num_str) = p.split_whitespace().last() {
                        let clean = num_str.trim_end_matches('%');
                        pct = clean.parse::<u8>().ok();
                    }
                } else if p.eq_ignore_ascii_case("charging") || p.eq_ignore_ascii_case("charged") {
                    charging = Some(true);
                } else if p.eq_ignore_ascii_case("discharging") {
                    charging = Some(false);
                }
            }
            if pct.is_some() {
                return (pct, charging);
            }
        }
    }

    // 3. Android dumpsys battery fallback
    if is_android_env() {
        if let Ok(output) = std::process::Command::new("/system/bin/dumpsys")
            .arg("battery")
            .output()
        {
            let text = String::from_utf8_lossy(&output.stdout);
            let mut level: Option<u8> = None;
            let mut is_charging: Option<bool> = None;

            for line in text.lines() {
                let trimmed = line.trim();
                if let Some(rest) = trimmed.strip_prefix("level:") {
                    level = rest.trim().parse::<u8>().ok();
                } else if let Some(rest) = trimmed.strip_prefix("status:") {
                    if let Ok(st) = rest.trim().parse::<u32>() {
                        is_charging = Some(st == 2 || st == 5);
                    }
                } else if let Some(rest) = trimmed.strip_prefix("AC powered:") {
                    if rest.trim() == "true" {
                        is_charging = Some(true);
                    }
                } else if let Some(rest) = trimmed.strip_prefix("USB powered:") {
                    if rest.trim() == "true" {
                        is_charging = Some(true);
                    }
                } else if let Some(rest) = trimmed.strip_prefix("Wireless powered:") {
                    if rest.trim() == "true" {
                        is_charging = Some(true);
                    }
                }
            }
            if level.is_some() {
                return (level, is_charging);
            }
        }
    }

    (None, None)
}

fn detect_thermal_throttling() -> bool {
    // 1. Android dumpsys thermalservice check
    if is_android_env() {
        if let Ok(output) = std::process::Command::new("/system/bin/dumpsys")
            .arg("thermalservice")
            .output()
        {
            let text = String::from_utf8_lossy(&output.stdout);
            for line in text.lines() {
                let trimmed = line.trim();
                if let Some(rest) = trimmed.strip_prefix("Thermal Status:") {
                    if let Ok(status) = rest.trim().parse::<u32>() {
                        return status >= 2;
                    }
                }
            }
        }
    }

    let thermal_base = std::path::Path::new("/sys/class/thermal");
    if !thermal_base.is_dir() {
        return false;
    }
    if let Ok(entries) = std::fs::read_dir(thermal_base) {
        for entry in entries.flatten() {
            let file_name = entry.file_name().to_string_lossy().to_string();
            if file_name.starts_with("cooling_device") {
                let kind = std::fs::read_to_string(entry.path().join("type")).unwrap_or_default();
                if let Ok(cur_state_str) = std::fs::read_to_string(entry.path().join("cur_state")) {
                    if let Ok(state) = cur_state_str.trim().parse::<u32>() {
                        if cooling_device_indicates_throttling(&kind, state) {
                            return true;
                        }
                    }
                }
            }
            if file_name.starts_with("thermal_zone") {
                if let Ok(temp_str) = std::fs::read_to_string(entry.path().join("temp")) {
                    if let Ok(temp) = temp_str.trim().parse::<u64>() {
                        if temp >= 75_000 {
                            return true;
                        }
                    }
                }
            }
        }
    }
    false
}

/// True when a sysfs cooling device in a non-zero state means the CPU/GPU is being throttled.
///
/// A fan running (`Fan`, `pwm-fan`, ...) is a machine doing its job, not throttling: counting it
/// flagged every laptop with an active fan as `thermal_throttled`, which made the scheduler skip
/// it for multi-core, compile and GPU work. Only processor / frequency / idle-injection devices
/// count; an unknown or unreadable `type` does not.
fn cooling_device_indicates_throttling(kind: &str, cur_state: u32) -> bool {
    if cur_state == 0 {
        return false;
    }
    let kind = kind.trim().to_ascii_lowercase();
    if kind.contains("fan") || kind.contains("pwm") {
        return false;
    }
    ["cpu", "gpu", "processor", "devfreq", "powerclamp"]
        .iter()
        .any(|marker| kind.contains(marker))
}

fn detect_soc_model(is_android: bool) -> String {
    if is_android {
        for prop in &["ro.soc.model", "ro.board.platform", "ro.product.board"] {
            if let Ok(output) = std::process::Command::new("getprop").arg(prop).output() {
                let val = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if !val.is_empty() {
                    return val;
                }
            }
        }
    }
    if let Ok(cpuinfo) = std::fs::read_to_string("/proc/cpuinfo") {
        for line in cpuinfo.lines() {
            if line.starts_with("Hardware") || line.starts_with("Model") {
                if let Some((_, val)) = line.split_once(':') {
                    let trimmed = val.trim();
                    if !trimmed.is_empty() {
                        return trimmed.to_string();
                    }
                }
            }
        }
    }
    #[cfg(target_os = "macos")]
    {
        if let Ok(output) = std::process::Command::new("sysctl")
            .arg("-n")
            .arg("machdep.cpu.brand_string")
            .output()
        {
            let val = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !val.is_empty() {
                return val;
            }
        }
    }
    format!("{} ({})", std::env::consts::ARCH, std::env::consts::OS)
}

fn detect_os_version(is_android: bool) -> String {
    if is_android {
        let release = std::process::Command::new("getprop")
            .arg("ro.build.version.release")
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_else(|_| "Unknown".into());
        let sdk = std::process::Command::new("getprop")
            .arg("ro.build.version.sdk")
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_else(|_| "Unknown".into());
        return format!("Android {} (API {})", release, sdk);
    }
    #[cfg(target_os = "macos")]
    {
        if let Ok(output) = std::process::Command::new("sw_vers")
            .arg("-productVersion")
            .output()
        {
            let val = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !val.is_empty() {
                return format!("macOS {}", val);
            }
        }
    }
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

/// Discovers Blender executable and queries its version.
pub fn detect_blender(explicit_path: Option<&str>) -> (bool, Option<String>) {
    // 1. Check explicit path
    if let Some(path_str) = explicit_path {
        let p = std::path::Path::new(path_str);
        if p.is_file() {
            if let Some(ver) = query_blender_version(p) {
                return (true, Some(ver));
            }
            return (true, Some("Blender (Custom Path)".to_string()));
        }
    }

    // 2. Check environment variables
    for env_var in &["RUSTY_GRID_BLENDER_PATH", "OXIDE_SWARM_BLENDER_PATH", "BLENDER_PATH"] {
        if let Ok(env_path) = std::env::var(env_var) {
            let p = std::path::Path::new(env_path.trim());
            if p.is_file() {
                if let Some(ver) = query_blender_version(p) {
                    return (true, Some(ver));
                }
                return (true, Some("Blender (Env Path)".to_string()));
            }
        }
    }

    // 3. Platform standard paths
    #[cfg(windows)]
    {
        let mut search_dirs = Vec::new();
        if let Ok(pf) = std::env::var("ProgramFiles") {
            search_dirs.push(std::path::PathBuf::from(pf).join("Blender Foundation"));
        }
        if let Ok(pf86) = std::env::var("ProgramFiles(x86)") {
            search_dirs.push(std::path::PathBuf::from(pf86).join("Blender Foundation"));
        }
        if let Ok(local) = std::env::var("LOCALAPPDATA") {
            search_dirs.push(std::path::PathBuf::from(local).join("Programs").join("Blender Foundation"));
        }
        search_dirs.push(std::path::PathBuf::from(r"C:\Program Files\Blender Foundation"));

        for base_dir in search_dirs {
            if base_dir.is_dir() {
                if let Ok(entries) = std::fs::read_dir(&base_dir) {
                    let mut version_dirs: Vec<std::path::PathBuf> = entries
                        .flatten()
                        .map(|e| e.path())
                        .filter(|p| p.is_dir())
                        .collect();

                    // Sort descending so newer versions (e.g. 4.5) precede older versions (e.g. 4.2)
                    version_dirs.sort_by(|a, b| b.file_name().cmp(&a.file_name()));

                    for dir in version_dirs {
                        let candidate = dir.join("blender.exe");
                        if candidate.is_file() {
                            if let Some(ver) = query_blender_version(&candidate) {
                                return (true, Some(ver));
                            }
                            let dir_name = dir.file_name().map(|n| n.to_string_lossy().to_string());
                            return (true, dir_name);
                        }
                    }
                }
            }
        }

        // Check Steam standard path
        let steam_blender = std::path::PathBuf::from(r"C:\Program Files (x86)\Steam\steamapps\common\Blender\blender.exe");
        if steam_blender.is_file() {
            if let Some(ver) = query_blender_version(&steam_blender) {
                return (true, Some(ver));
            }
            return (true, Some("Blender (Steam)".to_string()));
        }
    }

    #[cfg(target_os = "macos")]
    {
        for app_path in &[
            "/Applications/Blender.app/Contents/MacOS/Blender",
            "/Applications/Blender/Blender.app/Contents/MacOS/Blender",
        ] {
            let p = std::path::Path::new(app_path);
            if p.is_file() {
                if let Some(ver) = query_blender_version(p) {
                    return (true, Some(ver));
                }
                return (true, Some("Blender".to_string()));
            }
        }
    }

    #[cfg(target_os = "linux")]
    {
        for linux_path in &[
            "/usr/bin/blender",
            "/usr/local/bin/blender",
            "/snap/bin/blender",
        ] {
            let p = std::path::Path::new(linux_path);
            if p.is_file() {
                if let Some(ver) = query_blender_version(p) {
                    return (true, Some(ver));
                }
                return (true, Some("Blender".to_string()));
            }
        }
    }

    // 4. PATH lookup fallback (e.g. `blender --version`)
    if let Ok(output) = std::process::Command::new("blender")
        .arg("--version")
        .output()
    {
        if output.status.success() {
            if let Ok(text) = std::str::from_utf8(&output.stdout) {
                if let Some(first_line) = text.lines().next() {
                    let trimmed = first_line.trim();
                    if trimmed.starts_with("Blender") {
                        return (true, Some(trimmed.to_string()));
                    }
                }
            }
            return (true, Some("Blender".to_string()));
        }
    }

    (false, None)
}

fn query_blender_version(exe_path: &std::path::Path) -> Option<String> {
    let output = std::process::Command::new(exe_path)
        .arg("--version")
        .output()
        .ok()?;
    if output.status.success() {
        let text = std::str::from_utf8(&output.stdout).ok()?;
        let first_line = text.lines().next()?.trim();
        if first_line.starts_with("Blender") {
            return Some(first_line.to_string());
        }
    }
    None
}

fn probe_blender_version_from_path(path: Option<&str>) -> Option<String> {
    let path_str = path?;
    query_blender_version(std::path::Path::new(path_str))
}

/// Inspects host GPU dedicated VRAM in Megabytes.
pub fn detect_gpu_vram_mb() -> Option<u64> {
    // 1. Probe nvidia-smi
    if let Ok(output) = std::process::Command::new("nvidia-smi")
        .arg("--query-gpu=memory.total")
        .arg("--format=csv,noheader,nounits")
        .output()
    {
        if output.status.success() {
            if let Ok(text) = std::str::from_utf8(&output.stdout) {
                for line in text.lines() {
                    if let Ok(mb) = line.trim().parse::<u64>() {
                        if mb > 0 {
                            return Some(mb);
                        }
                    }
                }
            }
        }
    }

    // 2. Linux sysfs: /sys/class/drm/card0/device/mem_info_vram_total
    #[cfg(target_os = "linux")]
    {
        for card_idx in 0..4 {
            let path = format!("/sys/class/drm/card{card_idx}/device/mem_info_vram_total");
            if let Ok(bytes_str) = std::fs::read_to_string(&path) {
                if let Ok(bytes) = bytes_str.trim().parse::<u64>() {
                    return Some(bytes / (1024 * 1024));
                }
            }
        }
    }

    None
}

/// Discovers whether NVIDIA CUDA toolkit/runtime is available on host.
pub fn detect_cuda_support(has_gpu: bool) -> bool {
    // 1. Check CUDA_PATH or CUDA_HOME environment variables
    for env_key in &["CUDA_PATH", "CUDA_HOME", "CUDA_ROOT"] {
        if let Ok(cuda_dir) = std::env::var(env_key) {
            let p = std::path::Path::new(cuda_dir.trim());
            if p.is_dir() {
                #[cfg(windows)]
                let nvcc = p.join("bin").join("nvcc.exe");
                #[cfg(not(windows))]
                let nvcc = p.join("bin").join("nvcc");

                if nvcc.is_file() || p.join("include").join("cuda.h").is_file() || p.is_dir() {
                    return true;
                }
            }
        }
    }

    // 2. Probe nvcc compiler on PATH
    if let Ok(output) = std::process::Command::new("nvcc")
        .arg("--version")
        .output()
    {
        if output.status.success() {
            return true;
        }
    }

    // 3. Platform standard paths
    #[cfg(windows)]
    {
        let cuda_base = std::path::Path::new(r"C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA");
        if cuda_base.is_dir() {
            if let Ok(entries) = std::fs::read_dir(cuda_base) {
                for entry in entries.flatten() {
                    if entry.path().is_dir() {
                        return true;
                    }
                }
            }
        }
    }

    #[cfg(target_os = "linux")]
    {
        if std::path::Path::new("/usr/local/cuda").exists() {
            return true;
        }
    }

    // 4. If has_gpu is true and nvidia-smi ran successfully, NVIDIA driver provides CUDA runtime
    if has_gpu {
        if let Ok(output) = std::process::Command::new("nvidia-smi").output() {
            if output.status.success() {
                return true;
            }
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::TaskRequirements;

    #[test]
    fn test_capabilities_detect_default() {
        let caps = WorkerCapabilities::detect(None, None, false);
        assert!(!caps.name.is_empty(), "Worker name should not be empty");
        assert!(caps.cpu_cores >= 1, "CPU cores must be >= 1");
        assert!(caps.ram_mb > 0, "RAM MB must be positive");
        assert!(
            !caps.is_simulated_gpu,
            "Simulated GPU must be false by default"
        );
    }

    #[test]
    fn test_pick_worker_name_prefers_os_hostname_and_skips_useless_values() {
        let some = |s: &str| Some(s.to_string());
        // The OS hostname wins even when the env vars are set.
        assert_eq!(
            pick_worker_name(some("mac-mini"), some("from-env"), some("WINBOX"), 7),
            "mac-mini"
        );
        // Env fallbacks keep their old order.
        assert_eq!(
            pick_worker_name(None, some("from-env"), some("WINBOX"), 7),
            "from-env"
        );
        assert_eq!(pick_worker_name(None, None, some("WINBOX"), 7), "WINBOX");
        // Blank and "localhost" (Android default) are not names.
        assert_eq!(
            pick_worker_name(some("  "), some(""), some("Localhost"), 7),
            "worker-7"
        );
        assert_eq!(
            pick_worker_name(some("localhost"), None, some("WINBOX"), 7),
            "WINBOX"
        );
        // Whitespace is trimmed; nothing at all falls back to the pid.
        assert_eq!(pick_worker_name(some(" box \n"), None, None, 7), "box");
        assert_eq!(pick_worker_name(None, None, None, 4242), "worker-4242");
    }

    #[test]
    fn test_detect_worker_name_uses_real_hostname_not_pid() {
        let name = detect_worker_name();
        assert!(!name.is_empty());
        if let Some(host) = sysinfo::System::host_name() {
            let host = host.trim();
            if !host.is_empty() && !host.eq_ignore_ascii_case("localhost") {
                assert_eq!(name, host, "worker name must come from the OS hostname");
            }
        }
    }

    #[test]
    fn test_bytes_to_mb_rounds_down_and_rejects_noise() {
        assert_eq!(bytes_to_mb(0), None);
        assert_eq!(bytes_to_mb(1024 * 1024 - 1), None);
        assert_eq!(bytes_to_mb(1024 * 1024), Some(1));
        assert_eq!(bytes_to_mb(64 * 1024 * 1024 * 1024), Some(65536));
        assert_eq!(bytes_to_mb(u64::MAX), Some(u64::MAX / (1024 * 1024)));
    }

    #[test]
    fn test_detect_system_ram_matches_sysinfo_total_memory() {
        // This is the code path Windows relies on (no /proc, no sysctl): the figure must be the
        // OS-reported total, not the old hard-coded 8192 MB fallback.
        let mut sys = sysinfo::System::new();
        sys.refresh_memory();
        let expected = sys.total_memory() / (1024 * 1024);
        if expected > 0 {
            assert_eq!(detect_system_ram_mb(), expected);
        }
        assert!(detect_system_ram_mb() > 0);
    }

    #[test]
    fn test_capabilities_detect_cores_override() {
        let caps = WorkerCapabilities::detect(Some("worker-test".into()), Some(16), false);
        assert_eq!(caps.name, "worker-test");
        assert_eq!(caps.cpu_cores, 16);
    }

    #[test]
    fn test_capabilities_detect_simulate_gpu() {
        let caps = WorkerCapabilities::detect(Some("gpu-worker".into()), Some(4), true);
        assert_eq!(caps.name, "gpu-worker");
        assert!(caps.has_gpu);
        assert!(caps.is_simulated_gpu);
        assert!(caps.gpu_device_name.is_some());
        assert!(caps.can_execute_gpu());
    }

    #[test]
    fn test_capabilities_hardware_overrides() {
        let overrides = HardwareOverrides {
            name: Some("custom-node".into()),
            cores: Some(32),
            ram_mb: Some(65536),
            gpu: None,
            no_gpu: false,
            simulate_gpu: true,
            gpu_name: Some("Custom Tensor Node".into()),
            ..Default::default()
        };
        let caps = WorkerCapabilities::detect_with_overrides(overrides);
        assert_eq!(caps.name, "custom-node");
        assert_eq!(caps.cpu_cores, 32);
        assert_eq!(caps.ram_mb, 65536);
        assert!(caps.has_gpu);
        assert!(caps.is_simulated_gpu);
        assert_eq!(caps.gpu_device_name.as_deref(), Some("Custom Tensor Node"));

        // Test no_gpu takes precedence
        let no_gpu_overrides = HardwareOverrides {
            no_gpu: true,
            simulate_gpu: true,
            ..Default::default()
        };
        let caps_no_gpu = WorkerCapabilities::detect_with_overrides(no_gpu_overrides);
        assert!(!caps_no_gpu.has_gpu);
        assert!(!caps_no_gpu.is_simulated_gpu);
        assert!(caps_no_gpu.gpu_device_name.is_none());
    }

    #[test]
    fn test_capabilities_satisfies_cpu_requirements() {
        let caps = WorkerCapabilities::new("worker", 4, 8192, false, false, None);

        let req_less = TaskRequirements::generic(2, 60);
        let req_exact = TaskRequirements::generic(4, 60);
        let req_more = TaskRequirements::generic(8, 60);

        assert!(caps.satisfies(&req_less));
        assert!(caps.satisfies(&req_exact));
        assert!(!caps.satisfies(&req_more));
    }

    #[test]
    fn test_capabilities_satisfies_ram_requirements() {
        let caps = WorkerCapabilities::new("worker", 4, 4096, false, false, None);

        let mut req_pass = TaskRequirements::generic(2, 60);
        req_pass.ram_mb = 2048;

        let mut req_fail = TaskRequirements::generic(2, 60);
        req_fail.ram_mb = 8192;

        assert!(caps.satisfies(&req_pass));
        assert!(!caps.satisfies(&req_fail));
    }

    #[test]
    fn test_capabilities_satisfies_gpu_requirements() {
        let cpu_worker = WorkerCapabilities::new("cpu-worker", 8, 16384, false, false, None);
        let gpu_worker = WorkerCapabilities::new(
            "gpu-worker",
            8,
            16384,
            true,
            true,
            Some("Simulated Virtual GPU".into()),
        );

        let generic_req = TaskRequirements::generic(2, 60);
        let gpu_req = TaskRequirements::gpu(60);

        // CPU worker satisfies generic tasks, but NEVER GPU tasks (AC4 verification)
        assert!(cpu_worker.satisfies(&generic_req));
        assert!(!cpu_worker.satisfies(&gpu_req));

        // GPU worker satisfies both generic and GPU tasks
        assert!(gpu_worker.satisfies(&generic_req));
        assert!(gpu_worker.satisfies(&gpu_req));
    }

    #[test]
    fn test_capabilities_serialization_roundtrip() {
        let mut caps = WorkerCapabilities::new(
            "worker-serde",
            8,
            16384,
            true,
            true,
            Some("Simulated Virtual GPU".into()),
        );
        caps = caps.with_mobile(Some(MobileCapabilities {
            os_version: "Android 14".into(),
            soc_model: "Snapdragon 8 Gen 2".into(),
            battery_pct: Some(85),
            is_charging: Some(true),
            thermal_throttled: false,
        }));

        let json = serde_json::to_string(&caps).expect("Failed to serialize");
        let deserialized: WorkerCapabilities =
            serde_json::from_str(&json).expect("Failed to deserialize");

        assert_eq!(caps, deserialized);
        assert_eq!(deserialized.mobile.as_ref().unwrap().battery_pct, Some(85));
    }

    #[test]
    fn test_mobile_detection_safe_non_panicking() {
        let mob = detect_mobile_capabilities();
        // Should not panic, can be Some or None depending on OS
        if let Some(m) = mob {
            assert!(!m.os_version.is_empty());
            assert!(!m.soc_model.is_empty());
        }
        // The live re-read must agree with the full probe about whether this is a mobile device.
        assert_eq!(
            detect_live_mobile_status().is_some(),
            detect_mobile_capabilities().is_some()
        );
    }

    #[test]
    fn test_live_status_replaces_only_the_fast_changing_fields() {
        let snapshot = MobileCapabilities {
            os_version: "Android 14".into(),
            soc_model: "Snapdragon 8 Gen 2".into(),
            battery_pct: Some(90),
            is_charging: Some(true),
            thermal_throttled: false,
        };
        let live = snapshot.with_live_status(MobileLiveStatus {
            battery_pct: Some(4),
            is_charging: Some(false),
            thermal_throttled: true,
            battery_temperature: Some(41.0),
            network_type: Some("cellular".into()),
            battery_tier: Some(BatteryTier::Critical),
            thermal_tier: Some(ThermalTier::Hot),
        });
        assert_eq!(live.os_version, "Android 14");
        assert_eq!(live.soc_model, "Snapdragon 8 Gen 2");
        assert_eq!(live.battery_pct, Some(4));
        assert_eq!(live.is_charging, Some(false));
        assert!(live.thermal_throttled);
        assert_eq!(live.live_status().battery_pct, Some(4));
        // The original snapshot is untouched.
        assert_eq!(snapshot.battery_pct, Some(90));
    }

    #[test]
    fn test_battery_and_thermal_tier_computations() {
        // Battery tiers
        assert_eq!(
            MobileLiveStatus::compute_battery_tier(Some(80), Some(false)),
            BatteryTier::Normal
        );
        assert_eq!(
            MobileLiveStatus::compute_battery_tier(Some(50), Some(false)),
            BatteryTier::Normal
        );
        assert_eq!(
            MobileLiveStatus::compute_battery_tier(Some(49), Some(false)),
            BatteryTier::Low
        );
        assert_eq!(
            MobileLiveStatus::compute_battery_tier(Some(30), Some(false)),
            BatteryTier::Low
        );
        assert_eq!(
            MobileLiveStatus::compute_battery_tier(Some(29), Some(false)),
            BatteryTier::SubLow
        );
        assert_eq!(
            MobileLiveStatus::compute_battery_tier(Some(15), Some(false)),
            BatteryTier::SubLow
        );
        assert_eq!(
            MobileLiveStatus::compute_battery_tier(Some(14), Some(false)),
            BatteryTier::Critical
        );
        // Charging overrides battery percentage
        assert_eq!(
            MobileLiveStatus::compute_battery_tier(Some(10), Some(true)),
            BatteryTier::Normal
        );

        // Thermal tiers
        assert_eq!(
            MobileLiveStatus::compute_thermal_tier(Some(35.0), false),
            ThermalTier::Normal
        );
        assert_eq!(
            MobileLiveStatus::compute_thermal_tier(Some(37.0), false),
            ThermalTier::Normal
        );
        assert_eq!(
            MobileLiveStatus::compute_thermal_tier(Some(38.0), false),
            ThermalTier::Warm
        );
        assert_eq!(
            MobileLiveStatus::compute_thermal_tier(Some(40.0), false),
            ThermalTier::Warm
        );
        assert_eq!(
            MobileLiveStatus::compute_thermal_tier(Some(41.0), false),
            ThermalTier::Hot
        );
        assert_eq!(
            MobileLiveStatus::compute_thermal_tier(Some(42.0), false),
            ThermalTier::Hot
        );
        assert_eq!(
            MobileLiveStatus::compute_thermal_tier(Some(43.0), false),
            ThermalTier::Critical
        );
        assert_eq!(
            MobileLiveStatus::compute_thermal_tier(None, true),
            ThermalTier::Warm
        );

        // Effective concurrency C_eff = min(C_bat, C_therm)
        assert_eq!(
            calculate_effective_concurrency(8, BatteryTier::Normal, ThermalTier::Normal),
            8
        );
        assert_eq!(
            calculate_effective_concurrency(8, BatteryTier::Low, ThermalTier::Normal),
            4
        );
        assert_eq!(
            calculate_effective_concurrency(8, BatteryTier::SubLow, ThermalTier::Warm),
            1
        );
        assert_eq!(
            calculate_effective_concurrency(8, BatteryTier::Low, ThermalTier::Hot),
            1
        );
        assert_eq!(
            calculate_effective_concurrency(8, BatteryTier::Critical, ThermalTier::Normal),
            0
        );
        assert_eq!(
            calculate_effective_concurrency(8, BatteryTier::Normal, ThermalTier::Critical),
            0
        );

        // Keepalive intervals
        assert_eq!(
            calculate_keepalive_interval("wifi", 0),
            Duration::from_secs(60)
        );
        assert_eq!(
            calculate_keepalive_interval("ethernet", 2),
            Duration::from_secs(60)
        );
        assert_eq!(
            calculate_keepalive_interval("cellular", 0),
            Duration::from_secs(25)
        );
        assert_eq!(
            calculate_keepalive_interval("5G LTE", 1),
            Duration::from_secs(10)
        );
    }

    #[test]
    fn test_running_fan_is_not_thermal_throttling() {
        // Fans and unknown devices never count, whatever their state.
        assert!(!cooling_device_indicates_throttling("Fan", 3));
        assert!(!cooling_device_indicates_throttling("pwm-fan", 1));
        assert!(!cooling_device_indicates_throttling("", 5));
        assert!(!cooling_device_indicates_throttling("Battery", 2));
        // An idle processor device is not throttling either.
        assert!(!cooling_device_indicates_throttling("Processor", 0));
        // CPU/GPU frequency capping and idle injection are.
        assert!(cooling_device_indicates_throttling("Processor\n", 1));
        assert!(cooling_device_indicates_throttling("thermal-cpufreq-0", 2));
        assert!(cooling_device_indicates_throttling("cpu-isolate0", 1));
        assert!(cooling_device_indicates_throttling("devfreq-gpu", 1));
        assert!(cooling_device_indicates_throttling("intel_powerclamp", 10));
    }

    #[test]
    fn test_capabilities_3d_extensions_default() {
        let caps = WorkerCapabilities::new("worker-3d", 4, 8192, false, false, None);
        assert!(!caps.has_blender);
        assert!(caps.blender_version.is_none());
        assert!(caps.vram_mb.is_none());
        assert!(!caps.has_cuda);
    }

    #[test]
    fn test_capabilities_3d_builders() {
        let caps = WorkerCapabilities::new("worker-3d", 8, 16384, true, false, Some("RTX 4090".into()))
            .with_blender(true, Some("Blender 4.5.4 LTS".into()))
            .with_vram_mb(Some(24576))
            .with_cuda(true);

        assert!(caps.has_blender);
        assert_eq!(caps.blender_version.as_deref(), Some("Blender 4.5.4 LTS"));
        assert_eq!(caps.vram_mb, Some(24576));
        assert!(caps.has_cuda);
    }

    #[test]
    fn test_capabilities_json_backward_compatibility() {
        // Legacy JSON missing has_blender, blender_version, vram_mb, has_cuda
        let legacy_json = r#"{
            "name": "legacy-worker",
            "cpu_cores": 4,
            "ram_mb": 8192,
            "has_gpu": false,
            "is_simulated_gpu": false,
            "gpu_device_name": null,
            "tags": []
        }"#;

        let caps: WorkerCapabilities = serde_json::from_str(legacy_json).expect("Deserialization failed");
        assert_eq!(caps.name, "legacy-worker");
        assert_eq!(caps.cpu_cores, 4);
        assert_eq!(caps.ram_mb, 8192);
        assert!(!caps.has_blender);
        assert!(caps.blender_version.is_none());
        assert!(caps.vram_mb.is_none());
        assert!(!caps.has_cuda);
    }

    #[test]
    fn test_capabilities_json_roundtrip_with_3d() {
        let caps = WorkerCapabilities::new("3d-worker", 16, 32768, true, false, Some("RTX 5090".into()))
            .with_3d_capabilities(true, Some("Blender 4.5".into()), Some(32768), true);

        let json = serde_json::to_string(&caps).expect("Failed to serialize");
        let deserialized: WorkerCapabilities = serde_json::from_str(&json).expect("Failed to deserialize");

        assert_eq!(caps, deserialized);
        assert!(deserialized.has_blender);
        assert_eq!(deserialized.blender_version.as_deref(), Some("Blender 4.5"));
        assert_eq!(deserialized.vram_mb, Some(32768));
        assert!(deserialized.has_cuda);
    }

    #[test]
    fn test_capabilities_detect_with_blender_overrides() {
        let overrides = HardwareOverrides {
            blender: Some(true),
            blender_version: Some("Blender 4.5 Custom".into()),
            ..Default::default()
        };
        let caps = WorkerCapabilities::detect_with_overrides(overrides);
        assert!(caps.has_blender);
        assert_eq!(caps.blender_version.as_deref(), Some("Blender 4.5 Custom"));

        let no_blender_overrides = HardwareOverrides {
            no_blender: true,
            blender: Some(true),
            blender_version: Some("Should be ignored".into()),
            ..Default::default()
        };
        let caps_disabled = WorkerCapabilities::detect_with_overrides(no_blender_overrides);
        assert!(!caps_disabled.has_blender);
        assert!(caps_disabled.blender_version.is_none());
    }

    #[test]
    fn test_capabilities_detect_with_vram_and_cuda_overrides() {
        let overrides = HardwareOverrides {
            gpu: Some(true),
            vram_mb: Some(16384),
            cuda: Some(true),
            ..Default::default()
        };
        let caps = WorkerCapabilities::detect_with_overrides(overrides);
        assert!(caps.has_gpu);
        assert_eq!(caps.vram_mb, Some(16384));
        assert!(caps.has_cuda);

        let overrides_no_cuda = HardwareOverrides {
            gpu: Some(true),
            vram_mb: Some(8192),
            cuda: Some(true),
            no_cuda: true,
            ..Default::default()
        };
        let caps_no_cuda = WorkerCapabilities::detect_with_overrides(overrides_no_cuda);
        assert!(!caps_no_cuda.has_cuda);
    }

    #[test]
    fn test_capabilities_no_gpu_dominates_vram_and_cuda() {
        let overrides = HardwareOverrides {
            no_gpu: true,
            vram_mb: Some(32768),
            cuda: Some(true),
            ..Default::default()
        };
        let caps = WorkerCapabilities::detect_with_overrides(overrides);
        assert!(!caps.has_gpu);
        assert!(caps.vram_mb.is_none(), "no_gpu must strip vram_mb");
        assert!(!caps.has_cuda, "no_gpu must force has_cuda to false");
    }

    #[test]
    fn test_detect_blender_non_panicking() {
        let (has_blender, ver) = detect_blender(None);
        if has_blender {
            assert!(ver.is_some());
        }
    }
}
