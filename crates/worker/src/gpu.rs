//! What a worker may claim about its GPU, and the limits GPU-class tasks are checked against.
//!
//! Everything here compiles without the `gpu-wgpu` feature, so the rules can be tested on any
//! machine and the same text is printed by a build that cannot drive a GPU:
//!
//! * [`apply_gpu_policy`] decides what [`WorkerCapabilities`] advertise. A *real* GPU is
//!   advertised only when this binary can actually drive one (`gpu-wgpu` compiled in) and an
//!   adapter was found and initialised; `--gpu` on a build that cannot is refused with a
//!   [`GpuNotice`] that says how to get a GPU-capable binary. `--simulate-gpu` stays an explicit,
//!   labelled test mode.
//! * [`DeviceLimits`] / [`check_gemm_request`] turn the task parameters (matrix dimension, work
//!   group size) into either a go or a clear error; nothing is silently clamped.
//! * [`GpuError`] is the one error type of the GPU path.

use rusty_grid_core::capabilities::WorkerCapabilities;

/// True when this binary contains the wgpu compute engine.
pub const WGPU_COMPILED: bool = cfg!(feature = "gpu-wgpu");

/// The command that builds a GPU-capable `oxideswarm` binary.
pub const ENABLE_GPU_HINT: &str = "cargo build --release --bin oxideswarm --features gpu-wgpu";

/// Largest matrix dimension a real GPU is asked for, whatever the device could hold: the matrices
/// are generated and read back on the host (about 3 * 4 * dim^2 bytes there), and the machine is
/// shared with its owner.
pub const GPU_MAX_DIM: u32 = 8192;

/// Largest matrix dimension the simulated GPU (a CPU multiplication) accepts.
pub const SIMULATOR_MAX_DIM: u32 = 2048;

/// Why a GPU-class task did not produce a result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GpuError {
    /// The request is outside what the device (or this worker) allows. Nothing was run.
    InvalidRequest(String),
    /// There is no usable GPU engine in this process (not compiled in, no adapter, or it could
    /// not be initialised).
    Unavailable(String),
    /// The task was cancelled or ran out of time while the work was under way. A GPU dispatch
    /// that was already submitted is not recalled: the engine stops waiting for it and drops the
    /// buffers.
    Interrupted(&'static str),
    /// wgpu reported a validation, out-of-memory or internal error, the device was lost, or the
    /// driver panicked. The engine is considered dead and is rebuilt for the next task.
    Device(String),
    /// The GPU produced a result that does not match the CPU recomputation.
    VerificationFailed(String),
}

impl std::fmt::Display for GpuError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GpuError::InvalidRequest(msg) => write!(f, "invalid GPU request: {msg}"),
            GpuError::Unavailable(msg) => write!(f, "GPU unavailable: {msg}"),
            GpuError::Interrupted(msg) => write!(f, "GPU work interrupted: {msg}"),
            GpuError::Device(msg) => write!(f, "GPU device error: {msg}"),
            GpuError::VerificationFailed(msg) => {
                write!(f, "GPU result failed CPU verification: {msg}")
            }
        }
    }
}

impl std::error::Error for GpuError {}

/// The parts of a device's limits that bound a tiled matrix multiplication, as plain numbers so
/// the checks need no GPU (the wgpu engine converts its `wgpu::Limits` into this).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceLimits {
    /// Largest dimension this device is asked for (a worker policy, see [`GPU_MAX_DIM`]).
    pub max_dim: u32,
    /// Largest single matrix buffer in bytes (storage binding and buffer size limits).
    pub max_matrix_bytes: u64,
    /// Workgroups per dispatch dimension.
    pub max_workgroups_per_dimension: u32,
    /// Largest workgroup edge (`min(max_compute_workgroup_size_x, _y)`).
    pub max_workgroup_edge: u32,
    /// Threads per workgroup.
    pub max_workgroup_invocations: u32,
    /// Workgroup shared memory in bytes.
    pub max_workgroup_shared_bytes: u32,
}

impl DeviceLimits {
    /// What the simulated GPU claims to be: a mid-range device, bounded by what a CPU can multiply
    /// in a reasonable time.
    pub const fn simulator() -> Self {
        Self {
            max_dim: SIMULATOR_MAX_DIM,
            max_matrix_bytes: SIMULATOR_MAX_DIM as u64 * SIMULATOR_MAX_DIM as u64 * 4,
            max_workgroups_per_dimension: 65_535,
            max_workgroup_edge: 32,
            max_workgroup_invocations: 1024,
            max_workgroup_shared_bytes: 32 * 1024,
        }
    }

    /// Largest dimension whose matrices fit these limits.
    pub fn max_supported_dim(&self) -> u32 {
        let by_bytes = ((self.max_matrix_bytes / 4) as f64).sqrt().floor() as u64;
        by_bytes.min(self.max_dim as u64).min(u32::MAX as u64) as u32
    }
}

/// Checks the task parameters against `limits`: `dim` is the matrix edge (`simulated_matrix_dim`),
/// `tile` the work group edge (`work_group_size`, `tile x tile` threads). Returns the plain
/// values on success and a message naming the parameter and the limit on failure; nothing is
/// clamped or rounded.
pub fn check_gemm_request(
    dim: u32,
    tile: u32,
    limits: &DeviceLimits,
) -> Result<(usize, usize), GpuError> {
    let invalid = |msg: String| Err(GpuError::InvalidRequest(msg));
    if dim == 0 {
        return invalid("simulated_matrix_dim must be at least 1 (got 0)".to_string());
    }
    if tile == 0 {
        return invalid("work_group_size must be at least 1 (got 0)".to_string());
    }
    let max_dim = limits.max_supported_dim();
    if dim > max_dim {
        return invalid(format!(
            "simulated_matrix_dim {dim} is above what this device accepts (at most {max_dim}: one \
             {dim}x{dim} f32 matrix is {} bytes, the device allows {} per buffer, and the worker \
             caps the dimension at {})",
            dim as u64 * dim as u64 * 4,
            limits.max_matrix_bytes,
            limits.max_dim,
        ));
    }
    if tile > limits.max_workgroup_edge {
        return invalid(format!(
            "work_group_size {tile} is above this device's workgroup edge limit of {}",
            limits.max_workgroup_edge
        ));
    }
    let invocations = tile as u64 * tile as u64;
    if invocations > limits.max_workgroup_invocations as u64 {
        return invalid(format!(
            "work_group_size {tile} needs {invocations} threads per workgroup, the device allows {}",
            limits.max_workgroup_invocations
        ));
    }
    let shared_bytes = 2 * invocations * 4;
    if shared_bytes > limits.max_workgroup_shared_bytes as u64 {
        return invalid(format!(
            "work_group_size {tile} needs {shared_bytes} bytes of workgroup memory, the device allows {}",
            limits.max_workgroup_shared_bytes
        ));
    }
    let groups = dim.div_ceil(tile);
    if groups > limits.max_workgroups_per_dimension {
        return invalid(format!(
            "simulated_matrix_dim {dim} with work_group_size {tile} needs {groups} workgroups per \
             dimension, the device allows {}",
            limits.max_workgroups_per_dimension
        ));
    }
    Ok((dim as usize, tile as usize))
}

/// How loudly a [`GpuNotice`] should be shown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoticeLevel {
    /// Good to know; logged.
    Info,
    /// The user asked for something that will not happen; printed to the terminal.
    Warn,
}

/// A line about GPU capability the worker owes its owner at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GpuNotice {
    pub level: NoticeLevel,
    pub message: String,
}

impl GpuNotice {
    fn info(message: impl Into<String>) -> Self {
        Self {
            level: NoticeLevel::Info,
            message: message.into(),
        }
    }

    fn warn(message: impl Into<String>) -> Self {
        Self {
            level: NoticeLevel::Warn,
            message: message.into(),
        }
    }
}

/// The GPU related inputs of a worker's configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GpuRequest {
    /// `--no-gpu`: advertise nothing.
    pub no_gpu: bool,
    /// `--simulate-gpu`: advertise the labelled CPU simulator.
    pub simulate_gpu: bool,
    /// `--gpu` (`Some(true)`): the owner insists on a real GPU. `Some(false)` switches it off.
    pub gpu: Option<bool>,
    /// Look for a real GPU without being told to (what the CLI does).
    pub auto_detect: bool,
    /// `--gpu-name`: the label to advertise instead of the adapter's own name.
    pub gpu_name: Option<String>,
}

/// An adapter the engine was initialised on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GpuProbe {
    pub device_name: String,
    pub backend: String,
    pub device_type: String,
}

/// What looking for a real GPU found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GpuProbeOutcome {
    /// This binary was built without the `gpu-wgpu` feature.
    NotCompiled,
    /// The engine could be built but no usable adapter is present.
    NoAdapter,
    /// An adapter exists but the engine could not be initialised on it.
    Failed(String),
    /// A GPU the engine runs on.
    Found(GpuProbe),
}

fn clear_gpu(caps: &mut WorkerCapabilities) {
    caps.has_gpu = false;
    caps.is_simulated_gpu = false;
    caps.gpu_device_name = None;
}

/// Decides what `caps` advertise about the GPU.
///
/// * `--no-gpu`: no GPU.
/// * `--simulate-gpu`: whatever the caller already put in `caps` (a labelled simulated GPU).
/// * `gpu: Some(false)` (`RUSTY_GRID_GPU=false`): no real GPU.
/// * a real GPU wanted (`--gpu`, or `auto_detect`): `probe()` is called, and only
///   [`GpuProbeOutcome::Found`] advertises `has_gpu` (with the real adapter name, unless
///   `--gpu-name` overrides the label). Every other outcome advertises no GPU, whatever the
///   caller put in `caps`, and explains why; it is a warning when `--gpu` was explicit.
/// * nothing asked: no GPU. (Detection is never implicit for library users: a test cluster on a
///   laptop must not turn every CPU worker into a GPU worker.)
///
/// `probe` is only called when a real GPU is wanted, because building the engine is not free.
pub fn apply_gpu_policy(
    caps: &mut WorkerCapabilities,
    request: &GpuRequest,
    probe: impl FnOnce() -> GpuProbeOutcome,
) -> Vec<GpuNotice> {
    if request.no_gpu {
        clear_gpu(caps);
        return Vec::new();
    }
    if request.simulate_gpu {
        return vec![GpuNotice::info(
            "advertising a SIMULATED GPU (--simulate-gpu): GPU tasks run on a CPU simulator and \
             are labelled as such; this is a test mode, not your GPU",
        )];
    }
    if request.gpu == Some(false) {
        clear_gpu(caps);
        return Vec::new();
    }
    let explicit = request.gpu == Some(true);
    if !explicit && !request.auto_detect {
        clear_gpu(caps);
        return Vec::new();
    }

    let outcome = probe();
    let refused = |detail: String, caps: &mut WorkerCapabilities| {
        clear_gpu(caps);
        vec![if explicit {
            GpuNotice::warn(format!("--gpu ignored, no GPU is advertised: {detail}"))
        } else {
            GpuNotice::info(format!("no GPU is advertised: {detail}"))
        }]
    };
    match outcome {
        GpuProbeOutcome::Found(found) => {
            caps.has_gpu = true;
            caps.is_simulated_gpu = false;
            let label = request
                .gpu_name
                .clone()
                .unwrap_or_else(|| found.device_name.clone());
            caps.gpu_device_name = Some(label);
            vec![GpuNotice::info(format!(
                "GPU compute enabled on {} ({}, {})",
                found.device_name, found.backend, found.device_type
            ))]
        }
        GpuProbeOutcome::NotCompiled => refused(
            format!(
                "this binary was built without the `gpu-wgpu` feature, so it cannot run GPU \
                 compute. Build a GPU-capable one with `{ENABLE_GPU_HINT}` (or pass \
                 `--features gpu-wgpu` to build_windows_worker.sh); `--simulate-gpu` advertises a \
                 labelled simulated GPU for testing"
            ),
            caps,
        ),
        GpuProbeOutcome::NoAdapter => refused(
            "no usable GPU adapter (Metal, Vulkan or DirectX 12 with compute support) was found \
             on this machine"
                .to_string(),
            caps,
        ),
        GpuProbeOutcome::Failed(reason) => refused(
            format!(
                "a GPU adapter was found but the compute engine could not start on it: {reason}"
            ),
            caps,
        ),
    }
}

/// Looks for a real GPU this process can compute on, building the engine if there is one (the
/// engine is shared with the task runner, so the work is not repeated for the first task).
pub fn probe_real_gpu() -> GpuProbeOutcome {
    #[cfg(feature = "gpu-wgpu")]
    {
        crate::wgpu_engine::probe_shared_engine()
    }
    #[cfg(not(feature = "gpu-wgpu"))]
    {
        GpuProbeOutcome::NotCompiled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caps() -> WorkerCapabilities {
        WorkerCapabilities::new("w", 4, 8192, false, false, None)
    }

    fn found(name: &str) -> GpuProbeOutcome {
        GpuProbeOutcome::Found(GpuProbe {
            device_name: name.to_string(),
            backend: "Metal".to_string(),
            device_type: "IntegratedGpu".to_string(),
        })
    }

    fn never_probed() -> GpuProbeOutcome {
        panic!("the GPU must not be probed when nobody asked for a real GPU");
    }

    // ---- check_gemm_request -------------------------------------------------------------

    #[test]
    fn requests_inside_the_limits_are_taken_literally() {
        let limits = DeviceLimits::simulator();
        assert_eq!(check_gemm_request(1, 1, &limits), Ok((1, 1)));
        assert_eq!(check_gemm_request(64, 16, &limits), Ok((64, 16)));
        assert_eq!(check_gemm_request(1000, 7, &limits), Ok((1000, 7)));
        assert_eq!(check_gemm_request(2048, 32, &limits), Ok((2048, 32)));
    }

    #[test]
    fn requests_outside_the_limits_are_errors_never_clamped() {
        let limits = DeviceLimits::simulator();
        for (dim, tile, needle) in [
            (0, 16, "simulated_matrix_dim must be at least 1"),
            (16, 0, "work_group_size must be at least 1"),
            (2049, 16, "simulated_matrix_dim 2049 is above"),
            (100_000, 16, "simulated_matrix_dim 100000 is above"),
            (64, 33, "work_group_size 33 is above"),
            (64, 1000, "work_group_size 1000 is above"),
        ] {
            let err = check_gemm_request(dim, tile, &limits).unwrap_err();
            let GpuError::InvalidRequest(message) = &err else {
                panic!("{err:?}");
            };
            assert!(message.contains(needle), "({dim}, {tile}): {message}");
        }
    }

    #[test]
    fn device_limits_bound_dim_workgroup_and_dispatch() {
        let webgpu_defaults = DeviceLimits {
            max_dim: GPU_MAX_DIM,
            max_matrix_bytes: 128 * 1024 * 1024,
            max_workgroups_per_dimension: 65_535,
            max_workgroup_edge: 256,
            max_workgroup_invocations: 256,
            max_workgroup_shared_bytes: 16_384,
        };
        // 128 MiB / 4 = 32 Mi entries -> sqrt = 5792
        assert_eq!(webgpu_defaults.max_supported_dim(), 5792);
        assert!(check_gemm_request(5792, 16, &webgpu_defaults).is_ok());
        let err = check_gemm_request(5793, 16, &webgpu_defaults)
            .unwrap_err()
            .to_string();
        assert!(err.contains("at most 5792"), "{err}");

        // a 32x32 workgroup is 1024 threads: more than the 256 this device allows
        let err = check_gemm_request(64, 32, &webgpu_defaults)
            .unwrap_err()
            .to_string();
        assert!(err.contains("1024 threads"), "{err}");

        // the worker's own cap wins on a big device
        let big = DeviceLimits {
            max_matrix_bytes: u32::MAX as u64,
            ..webgpu_defaults
        };
        assert_eq!(big.max_supported_dim(), GPU_MAX_DIM);
        assert!(check_gemm_request(GPU_MAX_DIM + 1, 16, &big).is_err());

        // tiles of one thread need a workgroup per element
        let few_groups = DeviceLimits {
            max_workgroups_per_dimension: 100,
            ..webgpu_defaults
        };
        let err = check_gemm_request(1000, 1, &few_groups)
            .unwrap_err()
            .to_string();
        assert!(err.contains("1000 workgroups per dimension"), "{err}");
        assert!(check_gemm_request(1000, 16, &few_groups).is_ok());

        // shared memory: 2 * tile^2 * 4 bytes
        let little_shared = DeviceLimits {
            max_workgroup_shared_bytes: 1024,
            ..webgpu_defaults
        };
        assert!(check_gemm_request(64, 11, &little_shared).is_ok()); // 968 bytes
        let err = check_gemm_request(64, 12, &little_shared)
            .unwrap_err()
            .to_string();
        assert!(err.contains("1152 bytes of workgroup memory"), "{err}");
    }

    // ---- apply_gpu_policy ---------------------------------------------------------------

    #[test]
    fn explicit_gpu_on_a_build_without_wgpu_advertises_nothing_and_says_how_to_fix_it() {
        let mut c = caps();
        // what core's declarative override would have produced
        c.has_gpu = true;
        c.gpu_device_name = Some("Physical GPU (Manual Override)".into());
        let notices = apply_gpu_policy(
            &mut c,
            &GpuRequest {
                gpu: Some(true),
                ..Default::default()
            },
            || GpuProbeOutcome::NotCompiled,
        );
        assert!(
            !c.has_gpu && !c.is_simulated_gpu && c.gpu_device_name.is_none(),
            "{c:?}"
        );
        assert!(!c.can_execute_gpu());
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].level, NoticeLevel::Warn);
        assert!(
            notices[0].message.contains("--features gpu-wgpu"),
            "{}",
            notices[0].message
        );
        assert!(notices[0].message.contains("--simulate-gpu"));
    }

    #[test]
    fn auto_detect_on_a_build_without_wgpu_is_informational() {
        let mut c = caps();
        let notices = apply_gpu_policy(
            &mut c,
            &GpuRequest {
                auto_detect: true,
                ..Default::default()
            },
            || GpuProbeOutcome::NotCompiled,
        );
        assert!(!c.can_execute_gpu());
        assert_eq!(notices[0].level, NoticeLevel::Info);
        assert!(notices[0].message.contains("gpu-wgpu"));
    }

    #[test]
    fn a_found_adapter_is_advertised_under_its_real_name() {
        for request in [
            GpuRequest {
                gpu: Some(true),
                ..Default::default()
            },
            GpuRequest {
                auto_detect: true,
                ..Default::default()
            },
        ] {
            let mut c = caps();
            let notices = apply_gpu_policy(&mut c, &request, || found("Apple M2 Pro"));
            assert!(c.has_gpu && !c.is_simulated_gpu);
            assert_eq!(c.gpu_device_name.as_deref(), Some("Apple M2 Pro"));
            assert_eq!(notices.len(), 1);
            assert_eq!(notices[0].level, NoticeLevel::Info);
            assert!(notices[0].message.contains("Apple M2 Pro"));
            assert!(notices[0].message.contains("Metal"));
        }
    }

    #[test]
    fn gpu_name_overrides_only_the_advertised_label() {
        let mut c = caps();
        apply_gpu_policy(
            &mut c,
            &GpuRequest {
                gpu: Some(true),
                gpu_name: Some("RTX in the study".into()),
                ..Default::default()
            },
            || found("NVIDIA GeForce RTX 5060 Ti"),
        );
        assert!(c.has_gpu);
        assert_eq!(c.gpu_device_name.as_deref(), Some("RTX in the study"));
    }

    #[test]
    fn no_adapter_or_a_failed_engine_advertises_nothing() {
        for (outcome, needle) in [
            (GpuProbeOutcome::NoAdapter, "no usable GPU adapter"),
            (GpuProbeOutcome::Failed("device lost".into()), "device lost"),
        ] {
            let mut c = caps();
            c.has_gpu = true;
            let notices = apply_gpu_policy(
                &mut c,
                &GpuRequest {
                    gpu: Some(true),
                    ..Default::default()
                },
                || outcome.clone(),
            );
            assert!(!c.can_execute_gpu(), "{outcome:?}");
            assert_eq!(notices[0].level, NoticeLevel::Warn);
            assert!(
                notices[0].message.contains(needle),
                "{}",
                notices[0].message
            );
        }
    }

    #[test]
    fn simulate_gpu_is_left_alone_and_labelled() {
        let mut c = WorkerCapabilities::new(
            "w",
            4,
            8192,
            true,
            true,
            Some("Simulated Virtual GPU (Matrix Engine)".into()),
        );
        let notices = apply_gpu_policy(
            &mut c,
            &GpuRequest {
                simulate_gpu: true,
                ..Default::default()
            },
            never_probed,
        );
        assert!(c.has_gpu && c.is_simulated_gpu);
        assert_eq!(
            c.gpu_device_name.as_deref(),
            Some("Simulated Virtual GPU (Matrix Engine)")
        );
        assert!(notices[0].message.contains("SIMULATED"));
    }

    #[test]
    fn no_gpu_wins_over_everything() {
        for request in [
            GpuRequest {
                no_gpu: true,
                gpu: Some(true),
                auto_detect: true,
                ..Default::default()
            },
            GpuRequest {
                gpu: Some(false),
                auto_detect: true,
                ..Default::default()
            },
        ] {
            let mut c = caps();
            c.has_gpu = true;
            let notices = apply_gpu_policy(&mut c, &request, never_probed);
            assert!(!c.can_execute_gpu());
            assert!(notices.is_empty());
        }
    }

    #[test]
    fn nothing_asked_means_no_gpu_and_no_probe() {
        let mut c = caps();
        c.has_gpu = true; // e.g. from core's nvidia-smi heuristic
        let notices = apply_gpu_policy(&mut c, &GpuRequest::default(), never_probed);
        assert!(!c.can_execute_gpu());
        assert!(notices.is_empty());
    }

    #[test]
    fn display_of_errors_names_the_kind() {
        assert!(GpuError::Unavailable("x".into())
            .to_string()
            .starts_with("GPU unavailable"));
        assert!(GpuError::Device("x".into())
            .to_string()
            .contains("device error"));
        assert!(GpuError::VerificationFailed("x".into())
            .to_string()
            .contains("CPU verification"));
        assert!(GpuError::Interrupted("x")
            .to_string()
            .contains("interrupted"));
        assert!(GpuError::InvalidRequest("x".into())
            .to_string()
            .starts_with("invalid GPU request"));
    }
}
