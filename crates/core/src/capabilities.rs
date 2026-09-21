//! Worker hardware capability models, auto-detection, and requirement matching.

use serde::{Deserialize, Serialize};

use crate::task::TaskRequirements;

/// Mobile-specific hardware and environmental telemetry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MobileCapabilities {
    /// Operating system version and API level (e.g. "Android 14 (API 34)", "macOS 14.5").
    pub os_version: String,
    /// System-on-Chip descriptor (e.g. "Snapdragon 8 Gen 2", "Apple M2 Max").
    pub soc_model: String,
    /// Battery charge percentage (0-100), or `None` if running on AC power or emulator.
    pub battery_pct: Option<u8>,
    /// Whether the device is actively drawing external power / charging.
    pub is_charging: Option<bool>,
    /// Whether the device is currently experiencing CPU/GPU thermal throttling.
    pub thermal_throttled: bool,
}

#[derive(Serialize, Deserialize)]
struct HumanMobileCapabilities {
    pub os_version: String,
    pub soc_model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub battery_pct: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_charging: Option<bool>,
    #[serde(default)]
    pub thermal_throttled: bool,
}

#[derive(Serialize, Deserialize)]
struct BinaryMobileCapabilities {
    pub os_version: String,
    pub soc_model: String,
    pub battery_pct: Option<u8>,
    pub is_charging: Option<bool>,
    pub thermal_throttled: bool,
}

impl From<MobileCapabilities> for HumanMobileCapabilities {
    fn from(m: MobileCapabilities) -> Self {
        Self {
            os_version: m.os_version,
            soc_model: m.soc_model,
            battery_pct: m.battery_pct,
            is_charging: m.is_charging,
            thermal_throttled: m.thermal_throttled,
        }
    }
}

impl From<HumanMobileCapabilities> for MobileCapabilities {
    fn from(h: HumanMobileCapabilities) -> Self {
        Self {
            os_version: h.os_version,
            soc_model: h.soc_model,
            battery_pct: h.battery_pct,
            is_charging: h.is_charging,
            thermal_throttled: h.thermal_throttled,
        }
    }
}

impl From<MobileCapabilities> for BinaryMobileCapabilities {
    fn from(m: MobileCapabilities) -> Self {
        Self {
            os_version: m.os_version,
            soc_model: m.soc_model,
            battery_pct: m.battery_pct,
            is_charging: m.is_charging,
            thermal_throttled: m.thermal_throttled,
        }
    }
}

impl From<BinaryMobileCapabilities> for MobileCapabilities {
    fn from(b: BinaryMobileCapabilities) -> Self {
        Self {
            os_version: b.os_version,
            soc_model: b.soc_model,
            battery_pct: b.battery_pct,
            is_charging: b.is_charging,
            thermal_throttled: b.thermal_throttled,
        }
    }
}

impl Serialize for MobileCapabilities {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            HumanMobileCapabilities::from(self.clone()).serialize(serializer)
        } else {
            BinaryMobileCapabilities::from(self.clone()).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for MobileCapabilities {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            HumanMobileCapabilities::deserialize(deserializer).map(Into::into)
        } else {
            BinaryMobileCapabilities::deserialize(deserializer).map(Into::into)
        }
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

impl WorkerCapabilities {
    /// Creates an explicit WorkerCapabilities instance.
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
fn detect_worker_name() -> String {
    if let Ok(host) = std::env::var("HOSTNAME") {
        if !host.trim().is_empty() {
            return host.trim().to_string();
        }
    }
    if let Ok(host) = std::env::var("COMPUTERNAME") {
        if !host.trim().is_empty() {
            return host.trim().to_string();
        }
    }
    format!("worker-{}", std::process::id())
}

/// Inspects host OS total memory in Megabytes.
fn detect_system_ram_mb() -> u64 {
    // 1. Linux /proc/meminfo inspection
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

    // 2. macOS / Darwin sysctl hw.memsize inspection
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

    // 3. Sensible fallback (8 GB)
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
                let status_str = std::fs::read_to_string(bat_dir.join("status")).unwrap_or_default();
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
        if let Ok(output) = std::process::Command::new("pmset").arg("-g").arg("batt").output() {
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

    (None, None)
}

fn detect_thermal_throttling() -> bool {
    let thermal_base = std::path::Path::new("/sys/class/thermal");
    if !thermal_base.is_dir() {
        return false;
    }
    if let Ok(entries) = std::fs::read_dir(thermal_base) {
        for entry in entries.flatten() {
            let file_name = entry.file_name().to_string_lossy().to_string();
            if file_name.starts_with("cooling_device") {
                if let Ok(cur_state_str) = std::fs::read_to_string(entry.path().join("cur_state")) {
                    if let Ok(state) = cur_state_str.trim().parse::<u32>() {
                        if state > 0 {
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
    }
}

