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
    /// Battery charge percentage (0-100), or `None` if no battery could be read.
    #[serde(default)]
    pub battery_pct: Option<u8>,
    /// Whether the device is actively drawing external power / charging.
    #[serde(default)]
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
    /// Battery charge percentage (0-100), or `None` if no battery could be read.
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

    /// Calculates the RRC-aware heartbeat interval when a mobile network type is known.
    ///
    /// Returns `None` when `network_type` is absent or unknown (for example on a battery-powered
    /// laptop), signalling that the caller should keep the master-negotiated heartbeat interval.
    pub fn adaptive_keepalive_interval(&self, active_tasks: usize) -> Option<Duration> {
        let net = self.network_type.as_deref()?.trim();
        if net.is_empty() || net.eq_ignore_ascii_case("unknown") || net.eq_ignore_ascii_case("none")
        {
            return None;
        }
        Some(calculate_keepalive_interval(net, active_tasks))
    }

    /// Returns the RRC-aware keepalive interval when `network_type` is known, or `negotiated`
    /// when `network_type` is unknown (such as on a laptop).
    pub fn recommended_keepalive_interval_or(
        &self,
        active_tasks: usize,
        negotiated: Duration,
    ) -> Duration {
        self.adaptive_keepalive_interval(active_tasks)
            .unwrap_or(negotiated)
    }

    /// Calculates RRC-aware heartbeat interval in seconds.
    pub fn recommended_keepalive_interval(&self, active_tasks: usize) -> Duration {
        if active_tasks > 0 {
            return Duration::from_secs(10);
        }
        match self.adaptive_keepalive_interval(active_tasks) {
            Some(interval) => interval,
            None => Duration::from_secs(60),
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
    if active_tasks > 0 {
        return Duration::from_secs(10);
    }
    let n = network_type.to_ascii_lowercase();
    let is_wifi = n.contains("wifi") || n.contains("wi-fi") || n.contains("wlan");
    let has_5g_token = !is_wifi
        && n.split(|c: char| !c.is_ascii_alphanumeric())
            .any(|token| token == "5g" || token == "5gnr");
    let is_cellular = n.contains("cellular")
        || n.contains("mobile")
        || n.contains("lte")
        || has_5g_token
        || n.contains("4g")
        || n.contains("3g")
        || n.contains("cgnat");

    if is_cellular {
        Duration::from_secs(25)
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
    /// Disables host battery and mobile capability probing (`pmset`, `/sys/class/power_supply`, `dumpsys`).
    pub no_battery: bool,
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

        let probe_physical_gpu = has_gpu && !is_simulated_gpu;

        // --- VRAM Detection & Overrides ---
        let vram_mb = if overrides.no_gpu {
            None
        } else if let Some(vram) = overrides.vram_mb {
            Some(vram)
        } else if let Ok(vram_str) =
            std::env::var("RUSTY_GRID_VRAM_MB").or_else(|_| std::env::var("OXIDE_SWARM_VRAM_MB"))
        {
            vram_str.trim().parse::<u64>().ok()
        } else if probe_physical_gpu {
            detect_gpu_vram_mb()
        } else {
            None
        };

        // --- CUDA Detection & Overrides ---
        let has_cuda = if overrides.no_gpu || overrides.no_cuda {
            false
        } else if let Some(c) = overrides.cuda {
            c
        } else if let Ok(val) =
            std::env::var("RUSTY_GRID_CUDA").or_else(|_| std::env::var("OXIDE_SWARM_CUDA"))
        {
            val == "1" || val.eq_ignore_ascii_case("true")
        } else if let Ok(val) =
            std::env::var("RUSTY_GRID_NO_CUDA").or_else(|_| std::env::var("OXIDE_SWARM_NO_CUDA"))
        {
            if val == "1" || val.eq_ignore_ascii_case("true") {
                false
            } else {
                detect_cuda_support(probe_physical_gpu)
            }
        } else if probe_physical_gpu {
            detect_cuda_support(probe_physical_gpu)
        } else {
            false
        };

        let mobile = if overrides.no_battery {
            None
        } else {
            detect_mobile_capabilities()
        };

        Self {
            name,
            cpu_cores,
            ram_mb,
            has_gpu,
            is_simulated_gpu,
            gpu_device_name,
            tags: detect_host_tools(),
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
        let mut cmd = std::process::Command::new("sysctl");
        cmd.arg("-n").arg("hw.memsize");
        if let Some(output) = run_host_probe(&mut cmd, "sysctl hw.memsize") {
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
        let mut cmd = std::process::Command::new("nvidia-smi");
        cmd.arg("--query-gpu=name").arg("--format=csv,noheader");
        if let Some(output) = run_host_probe(&mut cmd, "nvidia-smi --query-gpu=name") {
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

std::thread_local! {
    static TEST_BATTERY_PROBE: std::cell::Cell<Option<(Option<u8>, Option<bool>)>> =
        const { std::cell::Cell::new(None) };
}

/// Overrides the host battery reading returned by `detect_battery` on the current thread for the
/// duration of `f`. Used by unit tests to simulate a discharging battery on any host (such as a
/// headless CI runner without a battery).
#[doc(hidden)]
pub fn with_test_battery_probe<R>(reading: (Option<u8>, Option<bool>), f: impl FnOnce() -> R) -> R {
    struct Reset(Option<(Option<u8>, Option<bool>)>);
    impl Drop for Reset {
        fn drop(&mut self) {
            TEST_BATTERY_PROBE.with(|c| c.set(self.0));
        }
    }
    let prev = TEST_BATTERY_PROBE.with(|c| {
        let prev = c.get();
        c.set(Some(reading));
        prev
    });
    let _reset = Reset(prev);
    f()
}

/// Auto-detects mobile hardware attributes and real-time battery/thermal status.
/// Returns `None` on standard desktop/server environments without battery unless on Android.
///
/// Platform sensing coverage:
/// - **Android:** reads `/sys/class/power_supply` (with `/system/bin/dumpsys battery` fallback) and
///   `/system/bin/dumpsys thermalservice` / `/sys/class/thermal`; at runtime the Android app pushes
///   full [`MobileLiveStatus`] readings (including `battery_temperature`, `network_type`,
///   `battery_tier`, and `thermal_tier`) via `OxideWorkerBridge.nativeUpdateMobileTelemetry`.
/// - **Linux:** reads `/sys/class/power_supply` (`BAT0`/`BAT1`/`battery`) and `/sys/class/thermal`.
/// - **macOS:** reads battery percentage and power source via `pmset -g batt`; does not probe
///   battery/die temperature (`thermal_throttled` is `false`, `battery_temperature` is `None`).
/// - **Windows:** does not probe host battery or thermal sensors (`detect_battery` returns
///   `(None, None)` and `detect_thermal_throttling` returns `false`), so this function returns
///   `None` unless `TEST_BATTERY_PROBE` is set in tests.
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
/// exactly when [`detect_mobile_capabilities`] would (including on Windows when no test probe is
/// active). On macOS, `pmset -g batt` supplies `battery_pct` and `is_charging` while
/// `battery_temperature` and `network_type` remain `None`. Blocking (sysfs reads, `pmset`/`dumpsys`
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

/// Probes battery charge percentage and charging state on Linux/Android (`/sys/class/power_supply`
/// and `/system/bin/dumpsys battery`) and macOS (`pmset -g batt`). Windows has no host battery
/// probe and returns `(None, None)`.
fn detect_battery() -> (Option<u8>, Option<bool>) {
    if let Some(reading) = TEST_BATTERY_PROBE.with(|c| c.get()) {
        return reading;
    }
    // 1. Linux/Android sysfs: /sys/class/power_supply
    let (sysfs_pct, sysfs_charging) =
        parse_sysfs_power_supply(std::path::Path::new("/sys/class/power_supply"));
    if sysfs_pct.is_some() {
        return (sysfs_pct, sysfs_charging);
    }

    // 2. macOS pmset fallback
    #[cfg(target_os = "macos")]
    {
        let mut cmd = std::process::Command::new("pmset");
        cmd.arg("-g").arg("batt");
        if let Some(output) = run_host_probe(&mut cmd, "pmset -g batt") {
            let text = String::from_utf8_lossy(&output.stdout);
            let (pct, charging) = parse_pmset_batt(&text);
            if pct.is_some() {
                return (pct, charging);
            }
        }
    }

    // 3. Android dumpsys battery fallback
    if is_android_env() {
        let mut cmd = std::process::Command::new("/system/bin/dumpsys");
        cmd.arg("battery");
        if let Some(output) = run_host_probe(&mut cmd, "/system/bin/dumpsys battery") {
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

/// Returns whether a sysfs `/sys/class/power_supply/<entry>/type` value names an external
/// power source (`Mains`, `USB*`, or `Wireless`) rather than a battery.
fn is_external_power_supply_type(kind: &str) -> bool {
    let k = kind.trim();
    k.eq_ignore_ascii_case("Mains")
        || k.eq_ignore_ascii_case("Wireless")
        || k.get(..3).is_some_and(|p| p.eq_ignore_ascii_case("USB"))
}

/// Parses a Linux/Android `/sys/class/power_supply` directory into `(battery_pct, is_charging)`.
///
/// Reports `Some(true)` when any external power supply (`Mains`, `USB*`, `Wireless`) has
/// `online == 1` or when the battery `status` attribute is `Charging` or `Full`. Otherwise falls
/// back to `external_online` (`Some(false)` when all present external supplies have `online == 0`)
/// and then `status_charging`.
fn parse_sysfs_power_supply(power_supply: &std::path::Path) -> (Option<u8>, Option<bool>) {
    if !power_supply.is_dir() {
        return (None, None);
    }

    let mut external_online: Option<bool> = None;
    if let Ok(entries) = std::fs::read_dir(power_supply) {
        for entry in entries.flatten() {
            let dir = entry.path();
            if !dir.is_dir() {
                continue;
            }
            let is_external = std::fs::read_to_string(dir.join("type"))
                .map(|t| is_external_power_supply_type(&t))
                .unwrap_or(false);
            if !is_external {
                continue;
            }
            if let Ok(online) = std::fs::read_to_string(dir.join("online")) {
                match online.trim() {
                    "1" => {
                        external_online = Some(true);
                        break;
                    }
                    "0" => {
                        external_online.get_or_insert(false);
                    }
                    _ => {}
                }
            }
        }
    }

    for name in &["battery", "BAT0", "BAT1"] {
        let bat_dir = power_supply.join(name);
        if bat_dir.is_dir() {
            let capacity = std::fs::read_to_string(bat_dir.join("capacity"))
                .ok()
                .and_then(|s| s.trim().parse::<u8>().ok());
            let status_str = std::fs::read_to_string(bat_dir.join("status")).unwrap_or_default();
            let status_trim = status_str.trim().to_lowercase();
            let status_charging = if status_trim == "charging" || status_trim == "full" {
                Some(true)
            } else if status_trim == "discharging" || status_trim == "not charging" {
                Some(false)
            } else {
                None
            };
            if capacity.is_some() {
                let is_charging = if external_online == Some(true) || status_charging == Some(true)
                {
                    Some(true)
                } else {
                    external_online.or(status_charging)
                };
                return (capacity, is_charging);
            }
        }
    }

    (None, None)
}

/// Parses the output of `pmset -g batt` into `(battery_pct, is_charging)`.
///
/// The power source line (`Now drawing from 'AC Power'` or `'Battery Power'`) is the primary
/// source of `is_charging`. The semicolon-separated state words on the battery line (`charging`,
/// `charged`, `finishing charge`, `AC attached`, `discharging`, `not charging`) are used only as
/// a fallback when the power source header line is absent.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn parse_pmset_batt(text: &str) -> (Option<u8>, Option<bool>) {
    let mut pct: Option<u8> = None;
    let mut source_charging: Option<bool> = None;
    let mut state_charging: Option<bool> = None;

    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.contains("'AC Power'") {
            source_charging = Some(true);
        } else if trimmed.contains("'Battery Power'") {
            source_charging = Some(false);
        }

        for part in trimmed.split(';') {
            let p = part.trim();
            if p.ends_with('%') {
                if let Some(num_str) = p.split_whitespace().last() {
                    let clean = num_str.trim_end_matches('%');
                    if let Ok(val) = clean.parse::<u8>() {
                        pct = Some(val);
                    }
                }
            } else if p.eq_ignore_ascii_case("charging")
                || p.eq_ignore_ascii_case("charged")
                || p.eq_ignore_ascii_case("finishing charge")
                || p.eq_ignore_ascii_case("ac attached")
            {
                state_charging = Some(true);
            } else if p.eq_ignore_ascii_case("discharging")
                || (state_charging.is_none()
                    && (p.eq_ignore_ascii_case("not charging")
                        || p.to_ascii_lowercase().starts_with("not charging ")))
            {
                state_charging = Some(false);
            }
        }
    }

    if pct.is_some() {
        (pct, source_charging.or(state_charging))
    } else {
        (None, None)
    }
}

/// Probes OS/sysfs thermal throttling state on Android (`/system/bin/dumpsys thermalservice`) and
/// Linux (`/sys/class/thermal`). macOS and Windows do not expose `/sys/class/thermal` and return
/// `false`.
fn detect_thermal_throttling() -> bool {
    // 1. Android dumpsys thermalservice check
    if is_android_env() {
        let mut cmd = std::process::Command::new("/system/bin/dumpsys");
        cmd.arg("thermalservice");
        if let Some(output) = run_host_probe(&mut cmd, "/system/bin/dumpsys thermalservice") {
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
            let mut cmd = std::process::Command::new("getprop");
            cmd.arg(prop);
            if let Some(output) = run_host_probe(&mut cmd, &format!("getprop {prop}")) {
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
        let mut cmd = std::process::Command::new("sysctl");
        cmd.arg("-n").arg("machdep.cpu.brand_string");
        if let Some(output) = run_host_probe(&mut cmd, "sysctl machdep.cpu.brand_string") {
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
        let mut rel_cmd = std::process::Command::new("getprop");
        rel_cmd.arg("ro.build.version.release");
        let release = run_host_probe(&mut rel_cmd, "getprop ro.build.version.release")
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "Unknown".into());
        let mut sdk_cmd = std::process::Command::new("getprop");
        sdk_cmd.arg("ro.build.version.sdk");
        let sdk = run_host_probe(&mut sdk_cmd, "getprop ro.build.version.sdk")
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "Unknown".into());
        return format!("Android {} (API {})", release, sdk);
    }
    #[cfg(target_os = "macos")]
    {
        let mut cmd = std::process::Command::new("sw_vers");
        cmd.arg("-productVersion");
        if let Some(output) = run_host_probe(&mut cmd, "sw_vers -productVersion") {
            let val = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !val.is_empty() {
                return format!("macOS {}", val);
            }
        }
    }
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

/// Maximum wall-clock time a single host capability probe subprocess (`blender --version`,
/// `cargo --version`, `rustc --version`, `git --version`, `pmset`, `sysctl`, `sw_vers`,
/// `nvidia-smi`, `nvcc`, `dumpsys`, `getprop`) may take before it is killed and treated as
/// unavailable.
pub const HOST_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

#[derive(Debug)]
enum ProbeExecOutcome {
    Output(std::process::Output),
    TimedOut,
    SpawnFailed,
}

#[cfg(unix)]
fn kill_probe_group(pid: u32) {
    if let Ok(pgid) = libc::pid_t::try_from(pid) {
        if pgid > 1 {
            // SAFETY: `pgid` is the leader PID of the process group created by
            // `cmd.process_group(0)` for this probe subprocess.
            unsafe {
                let _ = libc::killpg(pgid, libc::SIGKILL);
            }
        }
    }
}

/// Windows creation flag (`CREATE_SUSPENDED`) used so a spawned child can be assigned to a Job
/// Object before its main thread executes any user code or spawns grandchildren.
#[cfg(windows)]
pub const WINDOWS_CREATE_SUSPENDED: u32 = 0x0000_0004;

/// Windows process error-mode flags that suppress modal hard-error and crash dialogs:
/// `SEM_FAILCRITICALERRORS` (`0x0001`) | `SEM_NOGPFAULTERRORBOX` (`0x0002`) |
/// `SEM_NOOPENFILEERRORBOX` (`0x8000`) = `0x8003`.
///
/// Child processes on Windows inherit the parent process's error mode unless spawned with
/// `CREATE_DEFAULT_ERROR_MODE`.
#[cfg(windows)]
pub const WINDOWS_SUPPRESS_ERROR_DIALOGS_MODE: u32 = 0x0001 | 0x0002 | 0x8000;

/// Sets `SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX | SEM_NOOPENFILEERRORBOX` (`0x8003`) on
/// the current Windows process (preserving any additional error-mode bits already set) so neither
/// this process nor any child process it spawns can block on a modal Windows hard-error dialog
/// (such as 16-bit/64-bit `.exe` incompatibility or missing-DLL pop-ups). No-op on non-Windows.
pub fn suppress_windows_error_dialogs() {
    #[cfg(windows)]
    {
        #[link(name = "kernel32")]
        extern "system" {
            fn SetErrorMode(umode: u32) -> u32;
        }
        // SAFETY: `SetErrorMode` is a thread-safe process-wide Win32 API with no pointer or
        // handle preconditions. Calling `SetErrorMode(prev | mode)` preserves any additional
        // error-mode flags the process already had enabled.
        unsafe {
            let prev = SetErrorMode(WINDOWS_SUPPRESS_ERROR_DIALOGS_MODE);
            let _ = SetErrorMode(prev | WINDOWS_SUPPRESS_ERROR_DIALOGS_MODE);
        }
    }
}

/// Creates a Windows Job Object configured with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`, assigns
/// the suspended `process` handle to it, and resumes `process` via `NtResumeProcess`.
///
/// Returns the Job Object handle as `isize` on success, or `None` if creation/assignment failed
/// (in which case `process` is still resumed and any partially created job handle is closed).
#[cfg(windows)]
pub fn attach_suspended_process_to_kill_on_close_job(
    process: *mut core::ffi::c_void,
) -> Option<isize> {
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
        SetInformationJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    };
    use windows_sys::Win32::System::LibraryLoader::{GetModuleHandleA, GetProcAddress};

    unsafe fn resume_process(process: HANDLE) {
        type NtResumeProcessFn = unsafe extern "system" fn(HANDLE) -> i32;
        let ntdll = GetModuleHandleA(c"ntdll.dll".as_ptr() as *const u8);
        if !ntdll.is_null() {
            let func = GetProcAddress(ntdll, c"NtResumeProcess".as_ptr() as *const u8);
            if let Some(func) = func {
                let nt_resume: NtResumeProcessFn = std::mem::transmute(func);
                let _ = nt_resume(process);
            }
        }
    }

    let process: HANDLE = process;
    if process.is_null() {
        return None;
    }
    // SAFETY: Win32 calls on a newly created job handle and the caller's live process handle;
    // the job handle is closed on every failure path and returned to the caller on success.
    unsafe {
        let job = CreateJobObjectW(std::ptr::null(), std::ptr::null());
        if job.is_null() {
            resume_process(process);
            return None;
        }
        let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        let configured = SetInformationJobObject(
            job,
            JobObjectExtendedLimitInformation,
            &info as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION as *const core::ffi::c_void,
            std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        );
        if configured == 0 || AssignProcessToJobObject(job, process) == 0 {
            CloseHandle(job);
            resume_process(process);
            return None;
        }
        resume_process(process);
        Some(job as isize)
    }
}

/// Terminates every process in the Windows Job Object `job`.
#[cfg(windows)]
pub fn terminate_job_object(job: isize) {
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::System::JobObjects::TerminateJobObject;
    // SAFETY: `job` is a live Job Object handle created by
    // `attach_suspended_process_to_kill_on_close_job`.
    unsafe {
        TerminateJobObject(job as HANDLE, 1);
    }
}

/// Closes the Windows Job Object handle `job`. Because the job was configured with
/// `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`, closing the last handle also terminates any remaining
/// processes in the job.
#[cfg(windows)]
pub fn close_job_object(job: isize) {
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    // SAFETY: last use of the Job Object handle.
    unsafe {
        CloseHandle(job as HANDLE);
    }
}

#[cfg(windows)]
struct ProbeJobGuard(Option<isize>);

#[cfg(windows)]
impl ProbeJobGuard {
    fn attach(child: &std::process::Child) -> Self {
        use std::os::windows::io::AsRawHandle;
        Self(attach_suspended_process_to_kill_on_close_job(
            child.as_raw_handle(),
        ))
    }

    fn terminate_and_close(&mut self) {
        if let Some(job) = self.0.take() {
            terminate_job_object(job);
            close_job_object(job);
        }
    }
}

#[cfg(windows)]
impl Drop for ProbeJobGuard {
    fn drop(&mut self) {
        self.terminate_and_close();
    }
}

fn reap_probe_child_detached(mut child: std::process::Child) {
    #[cfg(unix)]
    kill_probe_group(child.id());
    let _ = child.kill();
    // Reap on a detached thread so a probe stuck in uninterruptible kernel sleep (e.g.
    // `nvidia-smi` on a wedged GPU driver) never blocks worker start-up past the deadline.
    std::thread::spawn(move || {
        let _ = child.wait();
    });
}

/// Runs `cmd` with stdin/stderr closed and stdout captured, killing the process (and its Unix
/// process group or Windows Job Object) if it or its stdout pipe does not finish within `timeout`.
fn run_command_bounded(
    cmd: &mut std::process::Command,
    timeout: std::time::Duration,
) -> ProbeExecOutcome {
    cmd.stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        suppress_windows_error_dialogs();
        cmd.creation_flags(WINDOWS_CREATE_SUSPENDED);
    }

    let mut child = {
        let mut attempt = 0;
        loop {
            match cmd.spawn() {
                Ok(c) => break c,
                // On Unix, executing a freshly written script while another thread forks can yield
                // ETXTBSY (26); retry briefly.
                Err(e) if e.raw_os_error() == Some(26) && attempt < 5 => {
                    attempt += 1;
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                Err(_) => return ProbeExecOutcome::SpawnFailed,
            }
        }
    };

    #[cfg(windows)]
    let mut job_guard = ProbeJobGuard::attach(&child);

    let stdout_rx = child.stdout.take().map(|out| {
        let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(1);
        std::thread::spawn(move || {
            use std::io::Read;
            let mut buf = Vec::new();
            let _ = out.take(64 * 1024).read_to_end(&mut buf);
            let _ = tx.send(buf);
        });
        rx
    });

    let deadline = std::time::Instant::now() + timeout;
    let mut poll_interval = std::time::Duration::from_millis(5);
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                // Kill the probe's process group (Unix) or Job Object (Windows) when the direct
                // child exits so a wrapper script that leaves a background grandchild holding
                // stdout (`sleep 8 &` or `start /b`) is terminated immediately and closes the
                // inherited stdout pipe.
                #[cfg(unix)]
                kill_probe_group(child.id());
                #[cfg(windows)]
                job_guard.terminate_and_close();

                let stdout = match stdout_rx {
                    Some(rx) => {
                        let remaining =
                            deadline.saturating_duration_since(std::time::Instant::now());
                        match rx.recv_timeout(remaining) {
                            Ok(bytes) => bytes,
                            Err(_) => return ProbeExecOutcome::TimedOut,
                        }
                    }
                    None => Vec::new(),
                };
                return ProbeExecOutcome::Output(std::process::Output {
                    status,
                    stdout,
                    stderr: Vec::new(),
                });
            }
            Ok(None) => {
                let now = std::time::Instant::now();
                if now >= deadline {
                    #[cfg(windows)]
                    job_guard.terminate_and_close();
                    reap_probe_child_detached(child);
                    return ProbeExecOutcome::TimedOut;
                }
                let remaining = deadline.saturating_duration_since(now);
                std::thread::sleep(poll_interval.min(remaining));
                poll_interval = (poll_interval * 2).min(std::time::Duration::from_millis(25));
            }
            Err(_) => {
                #[cfg(windows)]
                job_guard.terminate_and_close();
                reap_probe_child_detached(child);
                return ProbeExecOutcome::SpawnFailed;
            }
        }
    }
}

fn run_host_probe(cmd: &mut std::process::Command, label: &str) -> Option<std::process::Output> {
    match run_command_bounded(cmd, HOST_PROBE_TIMEOUT) {
        ProbeExecOutcome::Output(output) => Some(output),
        ProbeExecOutcome::TimedOut => {
            tracing::warn!(
                probe = %label,
                timeout_secs = HOST_PROBE_TIMEOUT.as_secs_f32(),
                "Host capability probe timed out; ignoring probe result"
            );
            None
        }
        ProbeExecOutcome::SpawnFailed => None,
    }
}

/// Host tools probed at worker start-up (`<tool> --version`, bounded by [`HOST_PROBE_TIMEOUT`]).
pub const HOST_PROBED_TOOLS: [&str; 3] = ["cargo", "rustc", "git"];

/// Prepends standard Git for Windows / MSYS2 binary directories (`C:\Program Files\Git\usr\bin`,
/// `C:\Program Files\Git\bin`, `C:\Program Files (x86)\Git\usr\bin`,
/// `C:\Program Files (x86)\Git\bin`, `C:\msys64\usr\bin`) to `existing_path` when present on disk.
#[cfg(windows)]
pub fn augment_windows_path(existing_path: &str) -> String {
    let paths_to_check = [
        std::path::PathBuf::from(r"C:\Program Files\Git\usr\bin"),
        std::path::PathBuf::from(r"C:\Program Files\Git\bin"),
        std::path::PathBuf::from(r"C:\Program Files (x86)\Git\usr\bin"),
        std::path::PathBuf::from(r"C:\Program Files (x86)\Git\bin"),
        std::path::PathBuf::from(r"C:\msys64\usr\bin"),
    ];

    let mut prefix = String::new();
    for p in &paths_to_check {
        if p.exists() {
            let p_str = p.to_string_lossy();
            if !existing_path.contains(&*p_str) {
                if !prefix.is_empty() {
                    prefix.push(';');
                }
                prefix.push_str(&p_str);
            }
        }
    }
    if prefix.is_empty() {
        existing_path.to_string()
    } else if existing_path.is_empty() {
        prefix
    } else {
        format!("{prefix};{existing_path}")
    }
}

/// Returns `true` when `dir` is an MSYS2 or Cygwin POSIX binary directory (`...\Git\usr\bin`,
/// `...\git\current\usr\bin`, `...\msys64\usr\bin`, `...\cygwin64\bin`, or any folder containing
/// `msys-2.0.dll` or `cygwin1.dll`) whose executables link against `msys-2.0.dll` / `cygwin1.dll`.
///
/// Inside a Windows AppContainer (`--isolation fs` or `--isolation fs+net`), `msys-2.0.dll` and
/// `cygwin1.dll` fail during `DllMain(DLL_PROCESS_ATTACH)` Cygwin shared-heap initialization and
/// terminate the child process with `0xC0000142` (`STATUS_DLL_INIT_FAILURE`). Native Win32 tools
/// in `%SystemRoot%\System32` (`whoami.exe`, `cmd.exe`, `powershell.exe`, `where.exe`, `tar.exe`,
/// `curl.exe`, `find.exe`, `sort.exe`, `timeout.exe`) and `Git\cmd` / `Git\bin` (`git.exe`) do
/// not link `msys-2.0.dll` and run cleanly inside an AppContainer.
#[cfg(windows)]
pub fn is_msys_usr_bin_dir(dir: &std::path::Path) -> bool {
    if dir.join("msys-2.0.dll").is_file() || dir.join("cygwin1.dll").is_file() {
        return true;
    }
    let norm = dir
        .to_string_lossy()
        .replace('/', "\\")
        .trim_end_matches('\\')
        .to_ascii_lowercase();
    norm.ends_with(r"\git\usr\bin")
        || norm.ends_with(r"\git\current\usr\bin")
        || norm.ends_with(r"\msys64\usr\bin")
        || norm.ends_with(r"\msys32\usr\bin")
        || norm.ends_with(r"\cygwin64\bin")
        || norm.ends_with(r"\cygwin\bin")
}

/// Augments `existing_path` for a task running inside a Windows AppContainer (`--isolation fs`
/// or `--isolation fs+net`): keeps native directories (such as `%SystemRoot%\System32` and
/// `Git\cmd` / `Git\bin`) ahead of MSYS2 `usr\bin` directories so bare command names resolve to
/// native Win32 binaries instead of `msys-2.0.dll` tools that fail with `0xC0000142`.
#[cfg(windows)]
pub fn augment_windows_path_for_isolation(existing_path: &str) -> String {
    let mut native_parts: Vec<String> = Vec::new();
    let mut msys_parts: Vec<String> = Vec::new();
    for part in existing_path.split(';').filter(|s| !s.is_empty()) {
        if is_msys_usr_bin_dir(std::path::Path::new(part)) {
            msys_parts.push(part.to_string());
        } else {
            native_parts.push(part.to_string());
        }
    }

    let contains_ci = |list: &[String], candidate: &str| {
        let cand_norm = candidate
            .replace('/', "\\")
            .trim_end_matches('\\')
            .to_ascii_lowercase();
        list.iter().any(|entry| {
            entry
                .replace('/', "\\")
                .trim_end_matches('\\')
                .to_ascii_lowercase()
                == cand_norm
        })
    };

    for native_git in [
        r"C:\Program Files\Git\cmd",
        r"C:\Program Files\Git\bin",
        r"C:\Program Files (x86)\Git\cmd",
        r"C:\Program Files (x86)\Git\bin",
    ] {
        let p = std::path::Path::new(native_git);
        if p.exists() && !contains_ci(&native_parts, native_git) {
            native_parts.push(native_git.to_string());
        }
    }

    for msys_dir in [
        r"C:\Program Files\Git\usr\bin",
        r"C:\Program Files (x86)\Git\usr\bin",
        r"C:\msys64\usr\bin",
    ] {
        let p = std::path::Path::new(msys_dir);
        if p.exists() && !contains_ci(&msys_parts, msys_dir) {
            msys_parts.push(msys_dir.to_string());
        }
    }

    native_parts.extend(msys_parts);
    native_parts.join(";")
}

#[cfg(windows)]
const WINDOWS_EXEC_EXTS: &[&str] = &[".com", ".exe", ".bat", ".cmd"];

#[cfg(windows)]
fn has_windows_exec_ext(path: &std::path::Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| {
            matches!(
                ext.to_ascii_lowercase().as_str(),
                "com" | "exe" | "bat" | "cmd"
            )
        })
        .unwrap_or(false)
}

#[cfg(windows)]
fn is_valid_windows_executable(path: &std::path::Path) -> bool {
    if !path.is_file() {
        return false;
    }
    if !has_windows_exec_ext(path) {
        tracing::debug!(
            path = %path.display(),
            "Skipping Windows executable candidate without .com/.exe/.bat/.cmd extension"
        );
        return false;
    }
    match path.extension().and_then(|ext| ext.to_str()) {
        Some(ext) if matches!(ext.to_ascii_lowercase().as_str(), "com" | "exe") => {
            use std::io::Read;
            let mut magic = [0u8; 2];
            let valid_mz = std::fs::File::open(path)
                .and_then(|mut f| f.read_exact(&mut magic))
                .map(|()| &magic == b"MZ")
                .unwrap_or(false);
            if !valid_mz {
                tracing::debug!(
                    path = %path.display(),
                    "Skipping Windows PE candidate without valid MZ header"
                );
            }
            valid_mz
        }
        _ => true,
    }
}

#[cfg(windows)]
fn probe_candidate_in_dir(dir: &std::path::Path, bare_name: &str) -> Option<String> {
    let candidate = dir.join(bare_name);
    if is_valid_windows_executable(&candidate) {
        return Some(candidate.to_string_lossy().to_string());
    }
    for ext in WINDOWS_EXEC_EXTS {
        let candidate_ext = dir.join(format!("{bare_name}{ext}"));
        if is_valid_windows_executable(&candidate_ext) {
            return Some(candidate_ext.to_string_lossy().to_string());
        }
    }
    None
}

/// Resolves `program` on Windows using the same rules as `TaskRunner` (`Git\usr\bin` / `Git\bin`
/// fast path followed by `effective_path` entries, checking `.com`, `.exe`, `.bat`, `.cmd` and
/// verifying `MZ` headers on `.com`/`.exe`).
#[cfg(windows)]
pub fn resolve_windows_executable(program: &str, effective_path: &str) -> String {
    let p = std::path::Path::new(program);
    let has_path_sep = program.contains('/') || program.contains('\\') || program.contains(':');
    if has_path_sep {
        if is_valid_windows_executable(p) {
            return program.to_string();
        }
        for ext in WINDOWS_EXEC_EXTS {
            let with_ext = format!("{program}{ext}");
            if is_valid_windows_executable(std::path::Path::new(&with_ext)) {
                return with_ext;
            }
        }
        return program.to_string();
    }

    let bare_name = p.file_name().and_then(|n| n.to_str()).unwrap_or(program);

    // Fast path: check Git for Windows usr/bin first so bare Unix tool names
    // (find, sort, timeout, tar, ssh, sh, git) resolve consistently with macOS/Linux
    // workers even when Git's directories sit after System32 in PATH.
    for base in &[r"C:\Program Files\Git\usr\bin", r"C:\Program Files\Git\bin"] {
        if let Some(found) = probe_candidate_in_dir(std::path::Path::new(base), bare_name) {
            return found;
        }
    }

    for dir in std::env::split_paths(effective_path) {
        if let Some(found) = probe_candidate_in_dir(&dir, bare_name) {
            return found;
        }
    }

    program.to_string()
}

/// Resolves `program` on Windows for a task running inside a Windows AppContainer
/// (`--isolation fs` or `--isolation fs+net`).
///
/// Unlike [`resolve_windows_executable`], this function searches native directories in
/// `effective_path` (such as `%SystemRoot%\System32` and `Git\cmd` / `Git\bin`) **before**
/// MSYS2 `usr\bin` directories (`...\Git\usr\bin`, `...\msys64\usr\bin`). MSYS2 `usr\bin`
/// executables link against `msys-2.0.dll`, which fails during `DllMain(DLL_PROCESS_ATTACH)`
/// Cygwin shared-heap initialization inside a Windows AppContainer with `0xC0000142`
/// (`STATUS_DLL_INIT_FAILURE`), whereas native Win32 binaries in `%SystemRoot%\System32`
/// (`whoami.exe`, `cmd.exe`, `powershell.exe`, `where.exe`, `tar.exe`, `curl.exe`, `find.exe`,
/// `sort.exe`, `timeout.exe`) and `Git\cmd` / `Git\bin` (`git.exe`) do not link `msys-2.0.dll`
/// and run cleanly inside an AppContainer.
#[cfg(windows)]
pub fn resolve_windows_executable_for_isolation(program: &str, effective_path: &str) -> String {
    let p = std::path::Path::new(program);
    let has_path_sep = program.contains('/') || program.contains('\\') || program.contains(':');
    if has_path_sep {
        if is_valid_windows_executable(p) {
            return program.to_string();
        }
        for ext in WINDOWS_EXEC_EXTS {
            let with_ext = format!("{program}{ext}");
            if is_valid_windows_executable(std::path::Path::new(&with_ext)) {
                return with_ext;
            }
        }
        return program.to_string();
    }

    let bare_name = p.file_name().and_then(|n| n.to_str()).unwrap_or(program);

    // 1. Search all non-MSYS2 directories in `effective_path` first (preserving caller order).
    let mut deferred_msys_dirs: Vec<std::path::PathBuf> = Vec::new();
    for dir in std::env::split_paths(effective_path) {
        if is_msys_usr_bin_dir(&dir) {
            deferred_msys_dirs.push(dir);
            continue;
        }
        if let Some(found) = probe_candidate_in_dir(&dir, bare_name) {
            return found;
        }
    }

    // 2. Fallback to %SystemRoot%\System32 and native Git wrapper directories if not in PATH.
    let sys_root = std::env::var("SystemRoot").unwrap_or_else(|_| r"C:\Windows".to_string());
    let sys32 = std::path::Path::new(&sys_root).join("System32");
    if let Some(found) = probe_candidate_in_dir(&sys32, bare_name) {
        return found;
    }
    for base in &[r"C:\Program Files\Git\cmd", r"C:\Program Files\Git\bin"] {
        if let Some(found) = probe_candidate_in_dir(std::path::Path::new(base), bare_name) {
            return found;
        }
    }

    // 3. Only check MSYS2 `usr\bin` directories as a last resort when no native binary exists.
    for dir in &deferred_msys_dirs {
        if let Some(found) = probe_candidate_in_dir(dir, bare_name) {
            return found;
        }
    }
    if let Some(found) = probe_candidate_in_dir(
        std::path::Path::new(r"C:\Program Files\Git\usr\bin"),
        bare_name,
    ) {
        return found;
    }

    program.to_string()
}

#[cfg(test)]
std::thread_local! {
    static TEST_TOOL_PROBE: std::cell::RefCell<Option<Vec<String>>> =
        const { std::cell::RefCell::new(None) };
}

/// Overrides the host tool tags returned by [`detect_host_tools`] on the current thread for the
/// duration of `f`. Used by unit tests to simulate a worker with or without specific tools
/// (`cargo`, `rustc`, `git`).
#[cfg(test)]
pub fn with_test_tool_probe<R>(tools: Vec<String>, f: impl FnOnce() -> R) -> R {
    struct Reset(Option<Vec<String>>);
    impl Drop for Reset {
        fn drop(&mut self) {
            TEST_TOOL_PROBE.with(|c| *c.borrow_mut() = self.0.take());
        }
    }
    let normalized: Vec<String> = tools
        .into_iter()
        .map(|t| {
            if t.starts_with(crate::placement::TOOL_TAG_PREFIX) {
                t
            } else {
                format!("{}{t}", crate::placement::TOOL_TAG_PREFIX)
            }
        })
        .collect();
    let prev = TEST_TOOL_PROBE.with(|c| c.borrow_mut().replace(normalized));
    let _reset = Reset(prev);
    f()
}

/// Runs `<program> --version` bounded by `timeout` (with cluster secrets stripped via
/// [`crate::cluster_auth::probe_command`]) and returns `true` when the subprocess exits 0.
pub fn probe_tool_with_timeout<P: AsRef<std::ffi::OsStr>>(
    program: P,
    timeout: std::time::Duration,
) -> bool {
    let program_ref = program.as_ref();
    #[cfg(windows)]
    let (resolved_program, effective_path) = {
        let base_path = std::env::var("PATH").unwrap_or_default();
        let effective_path = augment_windows_path(&base_path);
        let resolved = resolve_windows_executable(&program_ref.to_string_lossy(), &effective_path);
        (resolved, effective_path)
    };
    #[cfg(windows)]
    let mut cmd = {
        let mut c = crate::cluster_auth::probe_command(&resolved_program);
        c.env("PATH", &effective_path);
        c
    };
    #[cfg(not(windows))]
    let mut cmd = crate::cluster_auth::probe_command(program_ref);
    cmd.arg("--version");
    match run_command_bounded(&mut cmd, timeout) {
        ProbeExecOutcome::Output(output) => output.status.success(),
        ProbeExecOutcome::TimedOut => {
            tracing::warn!(
                tool = %program_ref.to_string_lossy(),
                timeout_secs = timeout.as_secs_f32(),
                "Host tool version probe timed out; treating tool as unavailable"
            );
            false
        }
        ProbeExecOutcome::SpawnFailed => false,
    }
}

/// Probes [`HOST_PROBED_TOOLS`] (`cargo`, `rustc`, `git`) with [`HOST_PROBE_TIMEOUT`] without
/// consulting the process-wide cache, returning `tool:<name>` tags in [`HOST_PROBED_TOOLS`] order.
pub fn detect_host_tools_uncached() -> Vec<String> {
    HOST_PROBED_TOOLS
        .iter()
        .filter(|&&tool| probe_tool_with_timeout(tool, HOST_PROBE_TIMEOUT))
        .map(|tool| format!("{}{tool}", crate::placement::TOOL_TAG_PREFIX))
        .collect()
}

/// Probes `cargo --version`, `rustc --version` and `git --version` on `PATH` (each bounded by
/// [`HOST_PROBE_TIMEOUT`]) and returns `tool:<name>` tags for those that succeed.
pub fn detect_host_tools() -> Vec<String> {
    #[cfg(test)]
    if let Some(override_tags) = TEST_TOOL_PROBE.with(|c| c.borrow().clone()) {
        return override_tags;
    }
    static DETECTED: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
    DETECTED.get_or_init(detect_host_tools_uncached).clone()
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BlenderProbeResult {
    Version(String),
    SucceededWithoutVersion,
    Failed,
    TimedOut,
}

fn probe_blender_with_timeout<P: AsRef<std::ffi::OsStr>>(
    program: P,
    timeout: std::time::Duration,
) -> BlenderProbeResult {
    let program_ref = program.as_ref();
    let mut cmd = crate::cluster_auth::probe_command(program_ref);
    cmd.arg("--version");
    match run_command_bounded(&mut cmd, timeout) {
        ProbeExecOutcome::Output(output) => {
            if output.status.success() {
                if let Ok(text) = std::str::from_utf8(&output.stdout) {
                    if let Some(first_line) = text.lines().next() {
                        let trimmed = first_line.trim();
                        if trimmed.starts_with("Blender") {
                            return BlenderProbeResult::Version(trimmed.to_string());
                        }
                    }
                }
                BlenderProbeResult::SucceededWithoutVersion
            } else {
                BlenderProbeResult::Failed
            }
        }
        ProbeExecOutcome::TimedOut => {
            tracing::warn!(
                path = %program_ref.to_string_lossy(),
                timeout_secs = timeout.as_secs_f32(),
                "Blender version probe timed out; treating Blender as unavailable"
            );
            BlenderProbeResult::TimedOut
        }
        ProbeExecOutcome::SpawnFailed => BlenderProbeResult::Failed,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BlenderResolution {
    /// An explicit argument or environment variable override was set to a non-empty path that
    /// does not point to an existing regular file. Do not fall through to host installations.
    InvalidOverride,
    /// Found a concrete Blender executable file on disk, along with its fallback version label.
    File {
        path: std::path::PathBuf,
        fallback_label: String,
    },
    /// No override was set and no standard OS installation path matched; try `blender` on `PATH`.
    NotFoundInStandardPaths,
}

fn resolve_blender_candidate(explicit_path: Option<&str>) -> BlenderResolution {
    resolve_blender_candidate_with_env(explicit_path, |key| std::env::var(key).ok())
}

fn resolve_blender_candidate_with_env<F>(
    explicit_path: Option<&str>,
    env_lookup: F,
) -> BlenderResolution
where
    F: Fn(&str) -> Option<String>,
{
    // 1. Check explicit path override. If non-empty and missing, do not fall through.
    if let Some(path_str) = explicit_path {
        let trimmed = path_str.trim();
        if !trimmed.is_empty() {
            let p = std::path::PathBuf::from(trimmed);
            if p.is_file() {
                return BlenderResolution::File {
                    path: p,
                    fallback_label: "Blender (Custom Path)".to_string(),
                };
            }
            return BlenderResolution::InvalidOverride;
        }
    }

    // 2. Check environment variable overrides in precedence order.
    // The first non-empty variable is authoritative; if it points at no file, do not fall through.
    for env_var in &[
        "RUSTY_GRID_BLENDER_PATH",
        "OXIDE_SWARM_BLENDER_PATH",
        "BLENDER_PATH",
    ] {
        if let Some(env_path) = env_lookup(env_var) {
            let trimmed = env_path.trim();
            if !trimmed.is_empty() {
                let p = std::path::PathBuf::from(trimmed);
                if p.is_file() {
                    return BlenderResolution::File {
                        path: p,
                        fallback_label: "Blender (Env Path)".to_string(),
                    };
                }
                return BlenderResolution::InvalidOverride;
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
            search_dirs.push(
                std::path::PathBuf::from(local)
                    .join("Programs")
                    .join("Blender Foundation"),
            );
        }
        search_dirs.push(std::path::PathBuf::from(
            r"C:\Program Files\Blender Foundation",
        ));

        for base_dir in search_dirs {
            if base_dir.is_dir() {
                if let Ok(entries) = std::fs::read_dir(&base_dir) {
                    let mut version_dirs: Vec<std::path::PathBuf> = entries
                        .flatten()
                        .map(|e| e.path())
                        .filter(|p| {
                            p.is_dir()
                                && p.file_name()
                                    .unwrap_or_default()
                                    .to_string_lossy()
                                    .starts_with("Blender")
                        })
                        .collect();

                    // Sort descending so newer versions (e.g. 4.5) precede older versions (e.g. 4.2)
                    version_dirs.sort_by(|a, b| b.file_name().cmp(&a.file_name()));

                    for dir in version_dirs {
                        let candidate = dir.join("blender.exe");
                        if candidate.is_file() {
                            let dir_name = dir
                                .file_name()
                                .map(|n| n.to_string_lossy().to_string())
                                .unwrap_or_else(|| "Blender".to_string());
                            return BlenderResolution::File {
                                path: candidate,
                                fallback_label: dir_name,
                            };
                        }
                    }
                }
            }
        }

        // Check Steam standard path
        let steam_blender = std::path::PathBuf::from(
            r"C:\Program Files (x86)\Steam\steamapps\common\Blender\blender.exe",
        );
        if steam_blender.is_file() {
            return BlenderResolution::File {
                path: steam_blender,
                fallback_label: "Blender (Steam)".to_string(),
            };
        }
    }

    #[cfg(target_os = "macos")]
    {
        for app_path in &[
            "/Applications/Blender.app/Contents/MacOS/Blender",
            "/Applications/Blender/Blender.app/Contents/MacOS/Blender",
        ] {
            let p = std::path::PathBuf::from(app_path);
            if p.is_file() {
                return BlenderResolution::File {
                    path: p,
                    fallback_label: "Blender".to_string(),
                };
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
            let p = std::path::PathBuf::from(linux_path);
            if p.is_file() {
                return BlenderResolution::File {
                    path: p,
                    fallback_label: "Blender".to_string(),
                };
            }
        }
    }

    BlenderResolution::NotFoundInStandardPaths
}

/// Resolves the file path to the host Blender executable using the shared resolution order:
/// explicit path override -> environment variable overrides (`RUSTY_GRID_BLENDER_PATH`,
/// `OXIDE_SWARM_BLENDER_PATH`, `BLENDER_PATH`) -> OS standard installation paths -> `PATH`.
///
/// If a non-empty explicit path or environment variable override is set but does not point to an
/// existing regular file, returns `None` rather than falling through to host installations.
pub fn resolve_blender_binary(explicit_path: Option<&str>) -> Option<std::path::PathBuf> {
    match resolve_blender_candidate(explicit_path) {
        BlenderResolution::InvalidOverride => None,
        BlenderResolution::File { path, .. } => Some(path),
        BlenderResolution::NotFoundInStandardPaths => {
            if probe_blender_binary("blender").0 {
                Some(std::path::PathBuf::from("blender"))
            } else {
                None
            }
        }
    }
}

/// Discovers Blender executable and queries its version.
pub fn detect_blender(explicit_path: Option<&str>) -> (bool, Option<String>) {
    match resolve_blender_candidate(explicit_path) {
        BlenderResolution::InvalidOverride => (false, None),
        BlenderResolution::File {
            path,
            fallback_label,
        } => match probe_blender_with_timeout(&path, HOST_PROBE_TIMEOUT) {
            BlenderProbeResult::Version(ver) => (true, Some(ver)),
            BlenderProbeResult::TimedOut => (false, None),
            BlenderProbeResult::SucceededWithoutVersion | BlenderProbeResult::Failed => {
                (true, Some(fallback_label))
            }
        },
        BlenderResolution::NotFoundInStandardPaths => probe_blender_binary("blender"),
    }
}

/// Probes a Blender executable candidate by running `<program> --version` with secrets stripped.
pub fn probe_blender_binary<P: AsRef<std::ffi::OsStr>>(program: P) -> (bool, Option<String>) {
    match probe_blender_with_timeout(program, HOST_PROBE_TIMEOUT) {
        BlenderProbeResult::Version(ver) => (true, Some(ver)),
        BlenderProbeResult::SucceededWithoutVersion => (true, Some("Blender".to_string())),
        BlenderProbeResult::Failed | BlenderProbeResult::TimedOut => (false, None),
    }
}

pub fn query_blender_version<P: AsRef<std::ffi::OsStr>>(program: P) -> Option<String> {
    match probe_blender_with_timeout(program, HOST_PROBE_TIMEOUT) {
        BlenderProbeResult::Version(ver) => Some(ver),
        BlenderProbeResult::SucceededWithoutVersion
        | BlenderProbeResult::Failed
        | BlenderProbeResult::TimedOut => None,
    }
}

fn probe_blender_version_from_path(path: Option<&str>) -> Option<String> {
    let path_str = path?;
    query_blender_version(std::path::Path::new(path_str))
}

/// Inspects host GPU dedicated VRAM in Megabytes.
pub fn detect_gpu_vram_mb() -> Option<u64> {
    // 1. Probe nvidia-smi
    let mut cmd = std::process::Command::new("nvidia-smi");
    cmd.arg("--query-gpu=memory.total")
        .arg("--format=csv,noheader,nounits");
    if let Some(output) = run_host_probe(&mut cmd, "nvidia-smi --query-gpu=memory.total") {
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
    let mut nvcc_cmd = std::process::Command::new("nvcc");
    nvcc_cmd.arg("--version");
    if let Some(output) = run_host_probe(&mut nvcc_cmd, "nvcc --version") {
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
        let mut smi_cmd = std::process::Command::new("nvidia-smi");
        if let Some(output) = run_host_probe(&mut smi_cmd, "nvidia-smi") {
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
            calculate_keepalive_interval("wifi-5g", 0),
            Duration::from_secs(60)
        );
        assert_eq!(
            calculate_keepalive_interval("wi-fi 5g", 0),
            Duration::from_secs(60)
        );
        assert_eq!(
            calculate_keepalive_interval("wlan-5g", 0),
            Duration::from_secs(60)
        );
        assert_eq!(
            calculate_keepalive_interval("wifi 5ghz", 0),
            Duration::from_secs(60)
        );
        assert_eq!(
            calculate_keepalive_interval("wifi", 1),
            Duration::from_secs(10)
        );
        assert_eq!(
            calculate_keepalive_interval("ethernet", 0),
            Duration::from_secs(60)
        );
        assert_eq!(
            calculate_keepalive_interval("ethernet", 2),
            Duration::from_secs(10)
        );
        assert_eq!(
            calculate_keepalive_interval("cellular", 0),
            Duration::from_secs(25)
        );
        assert_eq!(
            calculate_keepalive_interval("5g", 0),
            Duration::from_secs(25)
        );
        assert_eq!(
            calculate_keepalive_interval("5G NR", 0),
            Duration::from_secs(25)
        );
        assert_eq!(
            calculate_keepalive_interval("5G LTE", 1),
            Duration::from_secs(10)
        );

        let wifi_status = MobileLiveStatus {
            network_type: Some("wifi".into()),
            ..Default::default()
        };
        assert_eq!(
            wifi_status.recommended_keepalive_interval(0),
            Duration::from_secs(60)
        );
        assert_eq!(
            wifi_status.recommended_keepalive_interval(1),
            Duration::from_secs(10)
        );
        assert_eq!(
            wifi_status.recommended_keepalive_interval_or(1, Duration::from_secs(1)),
            Duration::from_secs(10)
        );

        let laptop_status = MobileLiveStatus {
            battery_pct: Some(80),
            is_charging: Some(false),
            network_type: None,
            ..Default::default()
        };
        assert_eq!(laptop_status.adaptive_keepalive_interval(0), None);
        assert_eq!(laptop_status.adaptive_keepalive_interval(1), None);
        assert_eq!(
            laptop_status.recommended_keepalive_interval_or(0, Duration::from_secs(1)),
            Duration::from_secs(1)
        );
        assert_eq!(
            laptop_status.recommended_keepalive_interval_or(1, Duration::from_secs(1)),
            Duration::from_secs(1)
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
        let caps =
            WorkerCapabilities::new("worker-3d", 8, 16384, true, false, Some("RTX 4090".into()))
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

        let caps: WorkerCapabilities =
            serde_json::from_str(legacy_json).expect("Deserialization failed");
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
        let caps =
            WorkerCapabilities::new("3d-worker", 16, 32768, true, false, Some("RTX 5090".into()))
                .with_3d_capabilities(true, Some("Blender 4.5".into()), Some(32768), true);

        let json = serde_json::to_string(&caps).expect("Failed to serialize");
        let deserialized: WorkerCapabilities =
            serde_json::from_str(&json).expect("Failed to deserialize");

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

    #[test]
    fn test_stale_blender_override_returns_none_and_false_without_fallthrough() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("stale_missing_blender_404.exe");
        let missing_str = missing.to_string_lossy().to_string();
        let existing_file = dir.path().join("fake_blender_file.exe");
        std::fs::write(&existing_file, b"not-a-real-blender").expect("write fake file");
        let existing_str = existing_file.to_string_lossy().to_string();

        // 1. Stale explicit_path returns None / (false, None) without falling through to host or env.
        assert_eq!(resolve_blender_binary(Some(&missing_str)), None);
        assert_eq!(detect_blender(Some(&missing_str)), (false, None));
        let stale_caps = WorkerCapabilities::detect_with_overrides(HardwareOverrides {
            blender_path: Some(missing_str.clone()),
            ..Default::default()
        });
        assert!(!stale_caps.has_blender);
        assert!(stale_caps.blender_version.is_none());

        // 2. Valid explicit_path resolves on both sides.
        assert_eq!(
            resolve_blender_binary(Some(&existing_str)),
            Some(existing_file.clone()),
            "valid explicit_path must resolve to the file"
        );
        let (has_valid, ver_valid) = detect_blender(Some(&existing_str));
        assert!(
            has_valid,
            "valid explicit_path file must report has_blender"
        );
        assert!(ver_valid.is_some());
        let valid_caps = WorkerCapabilities::detect_with_overrides(HardwareOverrides {
            blender_path: Some(existing_str.clone()),
            ..Default::default()
        });
        assert!(valid_caps.has_blender);
        assert!(valid_caps.blender_version.is_some());

        // 3. Stale explicit_path does not fall through even when BLENDER_PATH points to an existing file.
        let res_stale_explicit = resolve_blender_candidate_with_env(Some(&missing_str), |k| {
            (k == "BLENDER_PATH").then(|| existing_str.clone())
        });
        assert!(matches!(
            res_stale_explicit,
            BlenderResolution::InvalidOverride
        ));

        // 4. Stale env override in any of the three variables returns InvalidOverride without falling through.
        for &var in &[
            "RUSTY_GRID_BLENDER_PATH",
            "OXIDE_SWARM_BLENDER_PATH",
            "BLENDER_PATH",
        ] {
            let res = resolve_blender_candidate_with_env(None, |k| {
                (k == var).then(|| missing_str.clone())
            });
            assert!(
                matches!(res, BlenderResolution::InvalidOverride),
                "stale {var} must not fall through"
            );

            let res_valid = resolve_blender_candidate_with_env(None, |k| {
                (k == var).then(|| existing_str.clone())
            });
            assert!(
                matches!(res_valid, BlenderResolution::File { ref path, .. } if path == &existing_file),
                "valid {var} must resolve to the file"
            );
        }

        // 5. Higher-precedence stale override (RUSTY_GRID_BLENDER_PATH) does not fall through to
        //    lower-precedence valid override (BLENDER_PATH).
        let res_prec = resolve_blender_candidate_with_env(None, |k| match k {
            "RUSTY_GRID_BLENDER_PATH" => Some(missing_str.clone()),
            "BLENDER_PATH" => Some(existing_str.clone()),
            _ => None,
        });
        assert!(matches!(res_prec, BlenderResolution::InvalidOverride));
    }

    #[test]
    fn test_register_bincode_roundtrip_all_mobile_battery_combinations() {
        use crate::protocol::{
            deserialize_message, serialize_message, InboundMessage, ProtocolCapabilities,
            WireCodec, WorkerMessage, PROTOCOL_VERSION_CURRENT,
        };
        use uuid::Uuid;

        let worker_id = Uuid::new_v4();
        let combinations = [
            (None, None),
            (Some(80), None),
            (None, Some(true)),
            (Some(100), Some(false)),
            (Some(80), Some(true)),
        ];

        for (battery_pct, is_charging) in combinations {
            let caps = WorkerCapabilities::new(
                "mac-worker",
                10,
                16384,
                true,
                false,
                Some("Apple M2 Pro".into()),
            )
            .with_mobile(Some(MobileCapabilities {
                os_version: "macOS 15.1".into(),
                soc_model: "Apple M2 Pro".into(),
                battery_pct,
                is_charging,
                thermal_throttled: false,
            }))
            .with_3d_capabilities(
                true,
                Some("Blender 4.5.4 LTS".into()),
                Some(16384),
                false,
            );

            let msg = WorkerMessage::Register {
                worker_id,
                capabilities: caps,
                protocol_version: PROTOCOL_VERSION_CURRENT,
                features: ProtocolCapabilities::all_supported(),
            };

            let bytes = serialize_message(&msg, WireCodec::Bincode)
                .expect("bincode serialization of WorkerMessage::Register failed");

            let (decoded_worker, codec) = deserialize_message::<WorkerMessage>(&bytes)
                .expect("bincode deserialization of WorkerMessage::Register failed");
            assert_eq!(codec, WireCodec::Bincode);
            assert_eq!(decoded_worker, msg);

            let (decoded_inbound, inbound_codec) = deserialize_message::<InboundMessage>(&bytes)
                .expect("bincode deserialization of InboundMessage::Worker(Register) failed");
            assert_eq!(inbound_codec, WireCodec::Bincode);
            assert_eq!(decoded_inbound, InboundMessage::Worker(msg));
        }
    }

    #[test]
    fn test_parse_pmset_batt_all_five_states() {
        // 1. charging
        let charging = "\
Now drawing from 'AC Power'
 -InternalBattery-0 (id=23658595)\t65%; charging; 1:15 remaining present: true";
        assert_eq!(parse_pmset_batt(charging), (Some(65), Some(true)));

        // 2. charged
        let charged = "\
Now drawing from 'AC Power'
 -InternalBattery-0 (id=23658595)\t100%; charged; 0:00 remaining present: true";
        assert_eq!(parse_pmset_batt(charged), (Some(100), Some(true)));

        // 3. discharging
        let discharging = "\
Now drawing from 'Battery Power'
 -InternalBattery-0 (id=23658595)\t72%; discharging; 3:40 remaining present: true";
        assert_eq!(parse_pmset_batt(discharging), (Some(72), Some(false)));

        // 4. 100%; finishing charge
        let finishing_charge = "\
Now drawing from 'AC Power'
 -InternalBattery-0 (id=23658595)\t100%; finishing charge; 0:00 remaining present: true";
        assert_eq!(parse_pmset_batt(finishing_charge), (Some(100), Some(true)));

        // 5. 80%; AC attached; not charging
        let ac_attached_not_charging = "\
Now drawing from 'AC Power'
 -InternalBattery-0 (id=23658595)\t80%; AC attached; not charging present: true";
        assert_eq!(
            parse_pmset_batt(ac_attached_not_charging),
            (Some(80), Some(true))
        );

        // Fallback when the "Now drawing from ..." header line is absent
        assert_eq!(
            parse_pmset_batt(" -InternalBattery-0\t65%; charging; 1:15 remaining"),
            (Some(65), Some(true))
        );
        assert_eq!(
            parse_pmset_batt(" -InternalBattery-0\t100%; charged; 0:00 remaining"),
            (Some(100), Some(true))
        );
        assert_eq!(
            parse_pmset_batt(" -InternalBattery-0\t72%; discharging; 3:40 remaining"),
            (Some(72), Some(false))
        );
        assert_eq!(
            parse_pmset_batt(" -InternalBattery-0\t100%; finishing charge; 0:00 remaining"),
            (Some(100), Some(true))
        );
        assert_eq!(
            parse_pmset_batt(" -InternalBattery-0\t80%; AC attached; not charging present: true"),
            (Some(80), Some(true))
        );

        // Power source header takes precedence over unknown or conflicting state words
        assert_eq!(
            parse_pmset_batt(
                "Now drawing from 'AC Power'\n -InternalBattery-0\t80%; not charging present: true"
            ),
            (Some(80), Some(true))
        );
        assert_eq!(
            parse_pmset_batt(
                "Now drawing from 'Battery Power'\n -InternalBattery-0\t80%; unknown state"
            ),
            (Some(80), Some(false))
        );

        // Desktop Mac on AC power without an internal battery returns (None, None)
        assert_eq!(
            parse_pmset_batt("Now drawing from 'AC Power'\n"),
            (None, None)
        );
    }

    #[test]
    fn test_is_external_power_supply_type_non_ascii_does_not_panic() {
        assert!(!is_external_power_supply_type("abéz"));
        assert!(!is_external_power_supply_type("é"));
        assert!(is_external_power_supply_type("USB_Type_C"));
    }

    #[test]
    fn test_parse_sysfs_power_supply_mains_and_status_fallback() {
        let write_sysfs = |bat_capacity: Option<u8>,
                           bat_status: &str,
                           mains_online: Option<&str>|
         -> tempfile::TempDir {
            let dir = tempfile::tempdir().expect("temp sysfs");
            if let Some(cap) = bat_capacity {
                let bat = dir.path().join("BAT0");
                std::fs::create_dir_all(&bat).unwrap();
                std::fs::write(bat.join("type"), "Battery\n").unwrap();
                // A `Battery` device's own `online` attribute must never be mistaken for an
                // external adapter (`R3`).
                std::fs::write(bat.join("online"), "1\n").unwrap();
                std::fs::write(bat.join("capacity"), format!("{cap}\n")).unwrap();
                std::fs::write(bat.join("status"), format!("{bat_status}\n")).unwrap();
            }
            if let Some(online) = mains_online {
                let ac = dir.path().join("AC0");
                std::fs::create_dir_all(&ac).unwrap();
                std::fs::write(ac.join("type"), "Mains\n").unwrap();
                std::fs::write(ac.join("online"), format!("{online}\n")).unwrap();
            }
            dir
        };

        // AC online ("1") across all four statuses: Charging, Full, Discharging, Not charging
        for status in ["Charging", "Full", "Discharging", "Not charging"] {
            let sysfs = write_sysfs(Some(10), status, Some("1"));
            let (pct, charging) = parse_sysfs_power_supply(sysfs.path());
            assert_eq!(
                (pct, charging),
                (Some(10), Some(true)),
                "AC online with status '{status}' must report is_charging = Some(true)"
            );
            assert_eq!(
                MobileLiveStatus::compute_battery_tier(pct, charging),
                BatteryTier::Normal
            );
        }

        // Offline external supplies ("0") never override a battery that reports `Charging` or
        // `Full` (`N1`), and report `Some(false)` when the battery is `Discharging` or
        // `Not charging`.
        for (status, expected) in [
            ("Charging", Some(true)),
            ("Full", Some(true)),
            ("Discharging", Some(false)),
            ("Not charging", Some(false)),
        ] {
            let sysfs = write_sysfs(Some(45), status, Some("0"));
            assert_eq!(
                parse_sysfs_power_supply(sysfs.path()),
                (Some(45), expected),
                "Mains offline with status '{status}' must report is_charging = {expected:?}"
            );

            let usb_only = write_sysfs(Some(45), status, None);
            let usb = usb_only.path().join("usb");
            std::fs::create_dir_all(&usb).unwrap();
            std::fs::write(usb.join("type"), "USB\n").unwrap();
            std::fs::write(usb.join("online"), "0\n").unwrap();
            assert_eq!(
                parse_sysfs_power_supply(usb_only.path()),
                (Some(45), expected),
                "USB offline with status '{status}' must report is_charging = {expected:?}"
            );
        }

        // Non-ASCII `type` file does not panic (`N2`).
        assert!(!is_external_power_supply_type("abéz"));
        {
            let sysfs = write_sysfs(Some(50), "Discharging", None);
            let weird = sysfs.path().join("weird");
            std::fs::create_dir_all(&weird).unwrap();
            std::fs::write(weird.join("type"), "abéz\n").unwrap();
            std::fs::write(weird.join("online"), "1\n").unwrap();
            assert_eq!(
                parse_sysfs_power_supply(sysfs.path()),
                (Some(50), Some(false))
            );
        }

        // USB / USB-C supplies count as external power even when a barrel `Mains` adapter is
        // offline (`F1`), and multiple adapters in any `read_dir` order report `Some(true)` as
        // soon as any one external supply is online (`R4`).
        {
            let sysfs = write_sysfs(Some(40), "Discharging", Some("0"));
            let usb = sysfs.path().join("usb");
            std::fs::create_dir_all(&usb).unwrap();
            std::fs::write(usb.join("type"), "USB\n").unwrap();
            std::fs::write(usb.join("online"), "1\n").unwrap();
            let (pct, charging) = parse_sysfs_power_supply(sysfs.path());
            assert_eq!((pct, charging), (Some(40), Some(true)));
            assert_eq!(
                MobileLiveStatus::compute_battery_tier(pct, charging),
                BatteryTier::Normal
            );
        }
        {
            let sysfs = write_sysfs(Some(20), "Not charging", Some("0"));
            for (name, kind, online) in [
                ("AA_ADP0", "Mains", "0"),
                ("ucsi-source-psy", "USB_Type_C", "1"),
                ("ZZ_ADP1", "Mains", "0"),
            ] {
                let d = sysfs.path().join(name);
                std::fs::create_dir_all(&d).unwrap();
                std::fs::write(d.join("type"), format!("{kind}\n")).unwrap();
                std::fs::write(d.join("online"), format!("{online}\n")).unwrap();
            }
            let (pct, charging) = parse_sysfs_power_supply(sysfs.path());
            assert_eq!((pct, charging), (Some(20), Some(true)));
            assert_eq!(
                MobileLiveStatus::compute_battery_tier(pct, charging),
                BatteryTier::Normal
            );
        }

        // Fallback when no Mains supply is present: status alone determines is_charging
        for (status, expected) in [
            ("Charging", Some(true)),
            ("Full", Some(true)),
            ("Discharging", Some(false)),
            ("Not charging", Some(false)),
            ("Unknown", None),
        ] {
            let sysfs = write_sysfs(Some(60), status, None);
            assert_eq!(
                parse_sysfs_power_supply(sysfs.path()),
                (Some(60), expected),
                "fallback without Mains for status '{status}'"
            );
        }

        // Desktop with Mains online but no battery returns (None, None)
        let desktop = write_sysfs(None, "", Some("1"));
        assert_eq!(parse_sysfs_power_supply(desktop.path()), (None, None));
    }

    fn write_test_script(
        dir: &std::path::Path,
        name: &str,
        unix_body: &str,
        win_body: &str,
    ) -> std::path::PathBuf {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = win_body;
            let path = dir.join(format!("{name}.sh"));
            std::fs::write(&path, format!("#!/bin/sh\n{unix_body}\n")).expect("write script");
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
                .expect("chmod script");
            path
        }
        #[cfg(windows)]
        {
            let _ = unix_body;
            let path = dir.join(format!("{name}.cmd"));
            std::fs::write(&path, format!("@echo off\r\n{win_body}\r\n")).expect("write script");
            path
        }
    }

    fn assert_pid_file_process_dead(pid_file: &std::path::Path) {
        let raw = std::fs::read_to_string(pid_file).unwrap_or_else(|e| {
            panic!(
                "expected probe script to write pid file {}: {e}",
                pid_file.display()
            )
        });
        let pid: u32 = raw.trim().parse().unwrap_or_else(|e| {
            panic!(
                "invalid pid '{}' in {}: {e}",
                raw.trim(),
                pid_file.display()
            )
        });
        assert!(pid > 1, "unexpected pid {pid}");
        let mut sys = sysinfo::System::new();
        let wait_budget = if cfg!(windows) {
            std::time::Duration::from_secs(5)
        } else {
            std::time::Duration::from_secs(2)
        };
        let deadline = std::time::Instant::now() + wait_budget;
        loop {
            sys.refresh_processes();
            let alive = sys
                .process(sysinfo::Pid::from(pid as usize))
                .is_some_and(|p| p.status() != sysinfo::ProcessStatus::Zombie);
            if !alive {
                return;
            }
            if std::time::Instant::now() >= deadline {
                if let Some(p) = sys.process(sysinfo::Pid::from(pid as usize)) {
                    let _ = p.kill();
                }
                panic!("probe grandchild process {pid} survived after probe finished");
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }

    #[test]
    fn test_probe_blender_with_timeout_handles_version_non_version_failure_and_timeout() {
        let dir = tempfile::tempdir().expect("tempdir");
        let simple_probe_timeout = if cfg!(windows) {
            std::time::Duration::from_secs(15)
        } else {
            std::time::Duration::from_secs(2)
        };

        let ok_ver = write_test_script(
            dir.path(),
            "blender_ok",
            "echo 'Blender 4.5.4 LTS'",
            "echo Blender 4.5.4 LTS",
        );
        assert_eq!(
            probe_blender_with_timeout(&ok_ver, simple_probe_timeout),
            BlenderProbeResult::Version("Blender 4.5.4 LTS".to_string())
        );

        let no_ver = write_test_script(
            dir.path(),
            "blender_custom",
            "echo 'custom stub'",
            "echo custom stub",
        );
        assert_eq!(
            probe_blender_with_timeout(&no_ver, simple_probe_timeout),
            BlenderProbeResult::SucceededWithoutVersion
        );

        let fail = write_test_script(dir.path(), "blender_fail", "exit 1", "exit /b 1");
        assert_eq!(
            probe_blender_with_timeout(&fail, simple_probe_timeout),
            BlenderProbeResult::Failed
        );

        // Wrapper script that prints a version, leaves a background grandchild holding stdout open,
        // and exits immediately: group kill on child exit (Unix) or Job Object termination on
        // child exit (Windows) must kill the grandchild so stdout closes and the version is
        // returned well before the grandchild's 60s sleep.
        let bg_pid_file = dir.path().join("bg_grandchild.pid");
        let bg_ver = write_test_script(
            dir.path(),
            "blender_bg_stdout",
            &format!(
                "echo 'Blender 4.5.4 LTS'\nsleep 60 &\necho $! > '{}'\nexit 0",
                bg_pid_file.display()
            ),
            &format!(
                "echo Blender 4.5.4 LTS\r\npython -c \"import subprocess; p = subprocess.Popen(['ping', '-n', '60', '127.0.0.1']); open(r'{}', 'w').write(str(p.pid))\"\r\nexit /b 0",
                bg_pid_file.display()
            ),
        );
        let bg_timeout = if cfg!(windows) {
            std::time::Duration::from_secs(15)
        } else {
            std::time::Duration::from_secs(2)
        };
        let bg_elapsed_bound = if cfg!(windows) {
            std::time::Duration::from_secs(10)
        } else {
            std::time::Duration::from_millis(1500)
        };
        let bg_start = std::time::Instant::now();
        assert_eq!(
            probe_blender_with_timeout(&bg_ver, bg_timeout),
            BlenderProbeResult::Version("Blender 4.5.4 LTS".to_string())
        );
        assert!(
            bg_start.elapsed() < bg_elapsed_bound,
            "background-stdout wrapper took {:?}",
            bg_start.elapsed()
        );
        assert_pid_file_process_dead(&bg_pid_file);

        // Even if a grandchild escapes the process group (`setsid`) and holds stdout open after the
        // direct child exits, `recv_timeout(remaining)` must bound the probe to `timeout`.
        #[cfg(unix)]
        {
            let detached_pid_file = dir.path().join("detached_grandchild.pid");
            let detached = write_test_script(
                dir.path(),
                "blender_detached_stdout",
                &format!(
                    "python3 -c 'import os, time\npid = os.fork()\nif pid > 0:\n    open({:?}, \"w\").write(str(pid))\n    os._exit(0)\nos.setsid()\ntime.sleep(60)\n'",
                    detached_pid_file.to_string_lossy()
                ),
                "",
            );
            let det_start = std::time::Instant::now();
            let det_res =
                probe_blender_with_timeout(&detached, std::time::Duration::from_millis(800));
            let det_elapsed = det_start.elapsed();
            assert!(
                detached_pid_file.is_file(),
                "setsid grandchild PID file must exist so the post-exit stdout deadline was exercised"
            );
            if let Ok(raw) = std::fs::read_to_string(&detached_pid_file) {
                if let Ok(pid) = raw.trim().parse::<libc::pid_t>() {
                    if pid > 1 {
                        unsafe {
                            let _ = libc::kill(pid, libc::SIGKILL);
                        }
                    }
                }
            }
            assert_eq!(
                det_res,
                BlenderProbeResult::TimedOut,
                "unexpected result for detached-stdout wrapper: {det_res:?}"
            );
            assert!(
                det_elapsed < std::time::Duration::from_secs(3),
                "detached-stdout wrapper blocked past timeout: {det_elapsed:?}"
            );
        }

        let hung_pid_file = dir.path().join("hung_grandchild.pid");
        let hung = write_test_script(
            dir.path(),
            "blender_hung",
            &format!("sleep 60 &\necho $! > '{}'\nwait", hung_pid_file.display()),
            &format!(
                "python -c \"import subprocess; p = subprocess.Popen(['ping', '-n', '60', '127.0.0.1']); open(r'{}', 'w').write(str(p.pid)); p.wait()\"",
                hung_pid_file.display()
            ),
        );
        let hung_timeout = if cfg!(windows) {
            std::time::Duration::from_secs(5)
        } else {
            std::time::Duration::from_millis(800)
        };
        let hung_elapsed_bound = if cfg!(windows) {
            std::time::Duration::from_secs(15)
        } else {
            std::time::Duration::from_secs(4)
        };
        let start = std::time::Instant::now();
        assert_eq!(
            probe_blender_with_timeout(&hung, hung_timeout),
            BlenderProbeResult::TimedOut
        );
        assert!(
            start.elapsed() < hung_elapsed_bound,
            "timed-out probe took {:?}",
            start.elapsed()
        );
        assert_pid_file_process_dead(&hung_pid_file);
    }

    #[test]
    fn test_hung_blender_probe_via_override_times_out_and_reports_no_blender() {
        let dir = tempfile::tempdir().expect("tempdir");
        #[cfg(unix)]
        let env_pid_file = dir.path().join("env_hung_grandchild.pid");
        let unix_script = {
            #[cfg(unix)]
            {
                format!("sleep 60 &\necho $! > '{}'\nwait", env_pid_file.display())
            }
            #[cfg(not(unix))]
            {
                String::new()
            }
        };
        let hung = write_test_script(
            dir.path(),
            "hung_blender_env",
            &unix_script,
            "ping -n 60 127.0.0.1 >NUL",
        );

        let start = std::time::Instant::now();
        let hung_str = hung.to_string_lossy().to_string();
        let (has_blender, blender_version) = detect_blender(Some(&hung_str));
        let elapsed = start.elapsed();

        assert!(
            !has_blender,
            "a hung blender_path override probe must report has_blender = false"
        );
        assert!(
            blender_version.is_none(),
            "a hung blender_path override probe must report blender_version = None, got {:?}",
            blender_version
        );
        let max_overhead = if cfg!(windows) {
            std::time::Duration::from_secs(10)
        } else {
            std::time::Duration::from_secs(5)
        };
        assert!(
            elapsed >= std::time::Duration::from_secs(2)
                && elapsed < HOST_PROBE_TIMEOUT + max_overhead,
            "expected probe to time out around {:?}, took {:?}",
            HOST_PROBE_TIMEOUT,
            elapsed
        );
        #[cfg(unix)]
        assert_pid_file_process_dead(&env_pid_file);
    }

    #[test]
    fn test_probe_tool_with_timeout_and_host_tool_tags() {
        let dir = tempfile::tempdir().expect("tempdir");
        let simple_timeout = if cfg!(windows) {
            std::time::Duration::from_secs(15)
        } else {
            std::time::Duration::from_secs(2)
        };

        let ok_tool = write_test_script(
            dir.path(),
            "tool_ok",
            "echo 'cargo 1.91.0'",
            "echo cargo 1.91.0",
        );
        assert!(probe_tool_with_timeout(&ok_tool, simple_timeout));

        let fail_tool = write_test_script(dir.path(), "tool_fail", "exit 1", "exit /b 1");
        assert!(!probe_tool_with_timeout(&fail_tool, simple_timeout));

        let hung_pid_file = dir.path().join("hung_tool.pid");
        let hung_tool = write_test_script(
            dir.path(),
            "tool_hung",
            &format!("sleep 60 &\necho $! > '{}'\nwait", hung_pid_file.display()),
            &format!(
                "python -c \"import subprocess; p = subprocess.Popen(['ping', '-n', '60', '127.0.0.1']); open(r'{}', 'w').write(str(p.pid)); p.wait()\"",
                hung_pid_file.display()
            ),
        );
        let hung_timeout = if cfg!(windows) {
            HOST_PROBE_TIMEOUT
        } else {
            std::time::Duration::from_millis(800)
        };
        let start = std::time::Instant::now();
        assert!(!probe_tool_with_timeout(&hung_tool, hung_timeout));
        assert!(start.elapsed() < std::time::Duration::from_secs(15));
        assert_pid_file_process_dead(&hung_pid_file);

        let uncached = detect_host_tools_uncached();
        for tag in &uncached {
            assert!(
                matches!(tag.as_str(), "tool:cargo" | "tool:rustc" | "tool:git"),
                "unexpected tool tag {tag}"
            );
        }

        with_test_tool_probe(vec!["git".into()], || {
            let caps = WorkerCapabilities::detect_with_overrides(HardwareOverrides {
                no_gpu: true,
                no_blender: true,
                no_cuda: true,
                no_battery: true,
                ..Default::default()
            });
            assert_eq!(caps.tags, vec!["tool:git".to_string()]);
        });
    }

    #[cfg(windows)]
    #[test]
    fn test_windows_executable_resolution_for_isolation_prefers_system32_over_msys_usr_bin() {
        assert!(is_msys_usr_bin_dir(std::path::Path::new(
            r"C:\Program Files\Git\usr\bin"
        )));
        assert!(is_msys_usr_bin_dir(std::path::Path::new(
            r"C:\Users\dev\scoop\apps\git\current\usr\bin"
        )));
        assert!(is_msys_usr_bin_dir(std::path::Path::new(
            r"C:\msys64\usr\bin\"
        )));
        assert!(is_msys_usr_bin_dir(std::path::Path::new(
            r"C:\cygwin64\bin"
        )));
        assert!(!is_msys_usr_bin_dir(std::path::Path::new(
            r"C:\Windows\System32"
        )));
        assert!(!is_msys_usr_bin_dir(std::path::Path::new(
            r"C:\Program Files\Git\cmd"
        )));
        assert!(!is_msys_usr_bin_dir(std::path::Path::new(
            r"C:\Program Files\Git\bin"
        )));

        // A custom folder containing msys-2.0.dll or cygwin1.dll is also detected (N4).
        let custom_msys = tempfile::tempdir().expect("tempdir");
        assert!(!is_msys_usr_bin_dir(custom_msys.path()));
        std::fs::write(custom_msys.path().join("msys-2.0.dll"), b"MZ").unwrap();
        assert!(is_msys_usr_bin_dir(custom_msys.path()));

        // Even when the caller's PATH places Git\usr\bin ahead of System32 (e.g. Git Bash),
        // augment_windows_path_for_isolation moves MSYS2 usr\bin after native directories,
        // and resolve_windows_executable_for_isolation resolves bare `whoami` to
        // System32\whoami.exe instead of Git\usr\bin\whoami.exe (BUG-18).
        let git_bash_path = r"C:\Program Files\Git\usr\bin;C:\Windows\System32;C:\Windows";
        let eff_path = augment_windows_path_for_isolation(git_bash_path);
        let parts: Vec<&str> = eff_path.split(';').collect();
        let sys32_idx = parts
            .iter()
            .position(|p| p.eq_ignore_ascii_case(r"C:\Windows\System32"))
            .expect("System32 in effective PATH");
        if let Some(msys_idx) = parts
            .iter()
            .position(|p| is_msys_usr_bin_dir(std::path::Path::new(p)))
        {
            assert!(
                sys32_idx < msys_idx,
                "System32 must precede MSYS2 usr\\bin in isolated PATH: {eff_path}"
            );
        }

        let resolved_whoami = resolve_windows_executable_for_isolation("whoami", git_bash_path);
        assert!(
            !resolved_whoami
                .to_ascii_lowercase()
                .contains(r"\git\usr\bin\"),
            "whoami under isolation must not resolve to MSYS2 Git\\usr\\bin: {resolved_whoami}"
        );
        assert!(
            resolved_whoami
                .to_ascii_lowercase()
                .ends_with(r"\system32\whoami.exe"),
            "whoami under isolation must resolve to System32\\whoami.exe, got: {resolved_whoami}"
        );
    }
}
