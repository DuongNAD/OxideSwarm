//! Cross-Platform Hardware-Accelerated Compute Shaders via wgpu (Requirement R4).
//!
//! Provides adapter negotiation prioritizing physical hardware GPUs (Discrete > Integrated > Virtual),
//! WGSL compute pipeline compilation, workgroup dispatch, and safe staging buffer readback.
//!
//! # Failure model
//!
//! Nothing in here panics out of its public API and nothing waits forever:
//!
//! * wgpu's default reaction to a validation, out-of-memory or device-lost error is to panic. The
//!   device gets an uncaptured-error handler and a device-lost callback instead, every group of
//!   GPU calls runs inside validation / out-of-memory / internal error scopes, and the whole of
//!   [`WgpuEngine::execute_gemm`] runs under `catch_unwind` because a few wgpu calls (queue
//!   submission, polling, mapping) still panic on a dead device. All of it becomes a
//!   [`GpuError`], and an engine that returned a device error is marked lost.
//! * [`EngineProvider`] rebuilds a lost engine for the next task, so a driver reset costs the task
//!   that was running, not the worker for the rest of its life.
//! * The wait for the GPU is a polling loop that honours cancellation and a deadline
//!   ([`GemmControl`]). A dispatch that was already submitted **cannot be interrupted**: the GPU
//!   finishes it; the engine only stops waiting and drops its buffers.
//! * The functions here block (adapter discovery, shader compilation, the wait for the GPU): the
//!   task runner calls them from `spawn_blocking`.

use std::any::Any;
use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::TryRecvError;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};
use uuid::Uuid;
use wgpu::*;

use crate::cpu_simd::{self, ComputeReport, MatrixMetrics, Verification};
use crate::gpu::{
    check_gemm_request, DeviceLimits, GpuError, GpuProbe, GpuProbeOutcome, GPU_MAX_DIM,
};

/// The GEMM shader with the work group edge as a `const` line to be substituted.
const GEMM_TEMPLATE: &str = include_str!("shaders/gemm.wgsl");
/// The line of [`GEMM_TEMPLATE`] holding the work group edge.
const TILE_LINE: &str = "const TILE: u32 = 16u;";
/// Work group edge compiled when the engine starts (the default `work_group_size`).
const DEFAULT_TILE: u32 = 16;
/// How long a failed engine start-up is remembered before another is attempted.
const INIT_RETRY_COOLDOWN: Duration = Duration::from_secs(2);

/// Configuration dimensions for GEMM shader uniforms.
#[repr(C)]
#[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
struct GpuDimensions {
    pub m: u32,
    pub k: u32,
    pub n: u32,
    pub pad: u32, // Enforces 16-byte uniform alignment mandated by WebGPU
}

/// Execution outcome from a physical hardware GPU compute shader run.
#[derive(Debug, Clone)]
pub struct WgpuComputeOutcome {
    pub stdout: String,
    /// Upload, multiplication and read-back, in milliseconds (at least 1). Adapter discovery,
    /// shader compilation, matrix generation and verification are not part of it.
    pub elapsed_ms: u64,
    pub device_name: String,
    pub backend_name: String,
    pub metrics: MatrixMetrics,
    /// The product read back from the GPU.
    pub matrix_c: Vec<f32>,
    /// The check of `matrix_c` against a CPU recomputation. Always a passing one: a failing check
    /// is a [`GpuError::VerificationFailed`] instead of an outcome.
    pub verification: Verification,
}

/// Cancellation, deadline and report decoration for one [`WgpuEngine::execute_gemm_controlled`].
#[derive(Clone, Default)]
pub struct GemmControl {
    /// Set to ask the engine to stop waiting for the GPU.
    pub cancel: Option<Arc<AtomicBool>>,
    /// Stop waiting for the GPU at this instant.
    pub deadline: Option<Instant>,
    /// `Name: value` lines added to the report after `Execution Time`.
    pub extra_report_lines: Vec<String>,
}

impl GemmControl {
    fn interruption(&self) -> Option<&'static str> {
        if self
            .cancel
            .as_ref()
            .is_some_and(|c| c.load(Ordering::Relaxed))
        {
            return Some("the task was cancelled");
        }
        if self.deadline.is_some_and(|d| Instant::now() >= d) {
            return Some("the task ran out of time");
        }
        None
    }

    fn check(&self) -> Result<(), GpuError> {
        match self.interruption() {
            Some(why) => Err(GpuError::Interrupted(why)),
            None => Ok(()),
        }
    }
}

fn new_instance() -> Instance {
    Instance::new(InstanceDescriptor {
        backends: Backends::all(),
        flags: InstanceFlags::default(),
        dx12_shader_compiler: Dx12Compiler::default(),
        gles_minor_version: Gles3MinorVersion::default(),
    })
}

/// Preference order of an adapter, or `None` if it cannot be used: software rasterizers, and
/// adapters without compute shaders, are excluded. A discrete GPU beats an integrated one beats a
/// virtual one; between two adapters of the same kind the native APIs (Metal, Vulkan) come before
/// DirectX 12 before OpenGL.
fn rank_adapter(info: &AdapterInfo, can_run_gemm: bool) -> Option<i32> {
    if !can_run_gemm {
        return None;
    }
    let by_type = match info.device_type {
        DeviceType::DiscreteGpu => 400,
        DeviceType::IntegratedGpu => 300,
        DeviceType::VirtualGpu => 200,
        DeviceType::Other => 100,
        DeviceType::Cpu => return None,
    };
    let by_backend = match info.backend {
        Backend::Metal | Backend::Vulkan => 3,
        Backend::Dx12 => 2,
        Backend::BrowserWebGpu => 1,
        Backend::Gl | Backend::Empty => 0,
    };
    Some(by_type + by_backend)
}

/// Whether `adapter` has what the GEMM pipeline needs: compute shaders, three storage buffers,
/// a bind group and a uniform buffer.
fn adapter_can_run_gemm(adapter: &Adapter) -> bool {
    let limits = adapter.limits();
    adapter
        .get_downlevel_capabilities()
        .flags
        .contains(DownlevelFlags::COMPUTE_SHADERS)
        && limits.max_storage_buffers_per_shader_stage >= 3
        && limits.max_bind_groups >= 1
        && limits.max_uniform_buffer_binding_size >= std::mem::size_of::<GpuDimensions>() as u32
}

fn adapter_rank(adapter: &Adapter) -> Option<i32> {
    rank_adapter(&adapter.get_info(), adapter_can_run_gemm(adapter))
}

/// Picks the adapter to compute on: wgpu's own high-performance choice (a discrete GPU wins over
/// an integrated one) if it is usable, otherwise the best usable one by [`rank_adapter`].
fn select_adapter(instance: &Instance) -> Option<Adapter> {
    let preferred = pollster::block_on(instance.request_adapter(&RequestAdapterOptions {
        power_preference: PowerPreference::HighPerformance,
        force_fallback_adapter: false,
        compatible_surface: None,
    }));
    if let Some(adapter) = preferred {
        if adapter_rank(&adapter).is_some() {
            return Some(adapter);
        }
    }
    let mut ranked: Vec<(i32, Adapter)> = instance
        .enumerate_adapters(Backends::all())
        .into_iter()
        .filter_map(|adapter| adapter_rank(&adapter).map(|rank| (rank, adapter)))
        .collect();
    ranked.sort_by_key(|(rank, _)| std::cmp::Reverse(*rank));
    ranked.into_iter().next().map(|(_, adapter)| adapter)
}

/// Probes the host system for a compatible physical hardware GPU adapter (no device is created).
/// Returns the adapter if found, or None if only CPU or no adapters are available.
pub fn probe_physical_adapter() -> Option<AdapterInfo> {
    select_adapter(&new_instance()).map(|a| a.get_info())
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_string()
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

/// One line for a wgpu error (they are multi-line, with a `Caused by` chain).
fn describe_error(error: &Error) -> String {
    let (kind, text) = match error {
        Error::OutOfMemory { source } => ("out of memory", source.to_string()),
        Error::Validation { description, .. } => ("validation error", description.clone()),
        Error::Internal { description, .. } => ("internal error", description.clone()),
    };
    let flat = text.split_whitespace().collect::<Vec<_>>().join(" ");
    format!("{kind}: {flat}")
}

/// Runs `f` (a group of wgpu calls) with validation, out-of-memory and internal error scopes
/// around it, so the errors those calls raise come back as `Err` instead of reaching wgpu's
/// panicking default handler. `what` names the step in the message.
fn with_error_scopes<T>(device: &Device, what: &str, f: impl FnOnce() -> T) -> Result<T, GpuError> {
    device.push_error_scope(ErrorFilter::Internal);
    device.push_error_scope(ErrorFilter::OutOfMemory);
    device.push_error_scope(ErrorFilter::Validation);
    let value = f();
    let validation = pollster::block_on(device.pop_error_scope());
    let out_of_memory = pollster::block_on(device.pop_error_scope());
    let internal = pollster::block_on(device.pop_error_scope());
    match validation.or(out_of_memory).or(internal) {
        None => Ok(value),
        Some(error) => Err(GpuError::Device(format!(
            "{what}: {}",
            describe_error(&error)
        ))),
    }
}

fn device_limits_from(limits: &Limits) -> DeviceLimits {
    DeviceLimits {
        max_dim: GPU_MAX_DIM,
        max_matrix_bytes: (limits.max_storage_buffer_binding_size as u64)
            .min(limits.max_buffer_size),
        max_workgroups_per_dimension: limits.max_compute_workgroups_per_dimension,
        max_workgroup_edge: limits
            .max_compute_workgroup_size_x
            .min(limits.max_compute_workgroup_size_y),
        max_workgroup_invocations: limits.max_compute_invocations_per_workgroup,
        max_workgroup_shared_bytes: limits.max_compute_workgroup_storage_size,
    }
}

/// The GEMM shader source for work groups of `tile` x `tile` threads.
fn gemm_source(tile: u32) -> String {
    GEMM_TEMPLATE.replacen(TILE_LINE, &format!("const TILE: u32 = {tile}u;"), 1)
}

/// Hardware context holding an initialized WGPU Device, Queue, and Adapter information.
pub struct WgpuEngine {
    pub adapter_info: AdapterInfo,
    pub device: Arc<Device>,
    pub queue: Arc<Queue>,
    /// Device limits as granted at start-up (`Device::limits` panics on a lost device).
    device_limits: DeviceLimits,
    gemm_bind_group_layout: BindGroupLayout,
    /// One compiled pipeline per work group edge.
    pipelines: Mutex<HashMap<u32, Arc<ComputePipeline>>>,
    /// Set by the device-lost callback, by a panic out of wgpu and by any device error.
    lost: Arc<AtomicBool>,
    /// Why the device was lost, or the last error nothing was waiting for.
    background_error: Arc<Mutex<Option<String>>>,
}

impl std::fmt::Debug for WgpuEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WgpuEngine")
            .field("adapter", &self.adapter_info.name)
            .field("backend", &self.adapter_info.backend)
            .field("lost", &self.is_lost())
            .finish()
    }
}

impl WgpuEngine {
    /// Attempts to initialize the WGPU compute engine with a physical GPU.
    /// Returns Ok(None) if no physical GPU adapter is available on the host.
    pub fn try_init() -> Result<Option<Self>, String> {
        let initialised = catch_unwind(AssertUnwindSafe(Self::try_init_unguarded));
        match initialised {
            Ok(result) => result,
            Err(payload) => Err(format!(
                "the graphics driver panicked while starting the GPU engine: {}",
                panic_message(payload.as_ref())
            )),
        }
    }

    fn try_init_unguarded() -> Result<Option<Self>, String> {
        let instance = new_instance();
        let adapter = match select_adapter(&instance) {
            Some(a) => a,
            None => return Ok(None),
        };

        let adapter_info = adapter.get_info();
        let adapter_limits = adapter.limits();
        tracing::info!(
            name = %adapter_info.name,
            backend = ?adapter_info.backend,
            device_type = ?adapter_info.device_type,
            max_buffer_bytes = adapter_limits.max_buffer_size,
            max_storage_binding_bytes = adapter_limits.max_storage_buffer_binding_size,
            max_workgroup_invocations = adapter_limits.max_compute_invocations_per_workgroup,
            "Initializing hardware-accelerated WGPU compute engine"
        );

        // The adapter's own limits, not the WebGPU defaults: asking for more than a weaker GPU
        // has makes request_device fail, and asking for less caps the matrices at 128 MiB.
        let (device, queue) = pollster::block_on(adapter.request_device(
            &DeviceDescriptor {
                label: Some("oxideswarm_wgpu_engine_device"),
                required_features: Features::empty(),
                required_limits: adapter_limits.clone(),
                memory_hints: MemoryHints::Performance,
            },
            None,
        ))
        .map_err(|e| format!("Failed to request WGPU device from adapter: {e}"))?;

        let lost = Arc::new(AtomicBool::new(false));
        let background_error = Arc::new(Mutex::new(None::<String>));
        {
            let lost = Arc::clone(&lost);
            let reason = Arc::clone(&background_error);
            device.set_device_lost_callback(move |why, message| {
                // Also called when the device is dropped normally; nobody reads the flag then.
                tracing::warn!(reason = ?why, "WGPU device lost: {message}");
                *lock(&reason) = Some(format!("device lost ({why:?}): {message}"));
                lost.store(true, Ordering::SeqCst);
            });
        }
        {
            let reason = Arc::clone(&background_error);
            device.on_uncaptured_error(Box::new(move |error| {
                let text = describe_error(&error);
                tracing::error!("WGPU error outside any error scope: {text}");
                *lock(&reason) = Some(text);
            }));
        }

        let gemm_bind_group_layout =
            with_error_scopes(&device, "creating the GEMM bind group layout", || {
                let storage = |binding: u32, read_only: bool| BindGroupLayoutEntry {
                    binding,
                    visibility: ShaderStages::COMPUTE,
                    ty: BindingType::Buffer {
                        ty: BufferBindingType::Storage { read_only },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                };
                device.create_bind_group_layout(&BindGroupLayoutDescriptor {
                    label: Some("gemm_bind_group_layout"),
                    entries: &[
                        // Binding 0: Uniforms (Dimensions)
                        BindGroupLayoutEntry {
                            binding: 0,
                            visibility: ShaderStages::COMPUTE,
                            ty: BindingType::Buffer {
                                ty: BufferBindingType::Uniform,
                                has_dynamic_offset: false,
                                min_binding_size: None,
                            },
                            count: None,
                        },
                        // Binding 1: Matrix A (Storage, Read-Only)
                        storage(1, true),
                        // Binding 2: Matrix B (Storage, Read-Only)
                        storage(2, true),
                        // Binding 3: Matrix C (Storage, Read-Write)
                        storage(3, false),
                    ],
                })
            })
            .map_err(|e| e.to_string())?;

        let engine = Self {
            adapter_info,
            device: Arc::new(device),
            queue: Arc::new(queue),
            device_limits: device_limits_from(&adapter_limits),
            gemm_bind_group_layout,
            pipelines: Mutex::new(HashMap::new()),
            lost,
            background_error,
        };
        // Compile the default pipeline now so a broken shader or driver fails the start-up (and so
        // the advertised GPU) rather than the first task.
        engine
            .pipeline_for_tile(DEFAULT_TILE)
            .map_err(|e| e.to_string())?;
        Ok(Some(engine))
    }

    /// True once the device has been lost, the driver panicked, or a GPU call failed: the engine
    /// must not be used again (see [`EngineProvider`]).
    pub fn is_lost(&self) -> bool {
        self.lost.load(Ordering::SeqCst)
    }

    /// The limits tasks are checked against ([`check_gemm_request`]).
    pub fn device_limits(&self) -> DeviceLimits {
        self.device_limits
    }

    fn lost_reason(&self) -> String {
        lock(&self.background_error)
            .clone()
            .unwrap_or_else(|| "the GPU engine is no longer usable".to_string())
    }

    /// The compiled pipeline for work groups of `tile` x `tile` threads (compiled once).
    fn pipeline_for_tile(&self, tile: u32) -> Result<Arc<ComputePipeline>, GpuError> {
        let mut cache = lock(&self.pipelines);
        if let Some(pipeline) = cache.get(&tile) {
            return Ok(Arc::clone(pipeline));
        }
        let source = gemm_source(tile);
        if tile != DEFAULT_TILE && source == GEMM_TEMPLATE {
            return Err(GpuError::Device(
                "the GEMM shader template lost its `const TILE` line".to_string(),
            ));
        }
        let pipeline = with_error_scopes(&self.device, "compiling the GEMM shader", || {
            let shader = self.device.create_shader_module(ShaderModuleDescriptor {
                label: Some("gemm_shader_module"),
                source: ShaderSource::Wgsl(source.into()),
            });
            let layout = self
                .device
                .create_pipeline_layout(&PipelineLayoutDescriptor {
                    label: Some("gemm_pipeline_layout"),
                    bind_group_layouts: &[&self.gemm_bind_group_layout],
                    push_constant_ranges: &[],
                });
            self.device
                .create_compute_pipeline(&ComputePipelineDescriptor {
                    label: Some("gemm_compute_pipeline"),
                    layout: Some(&layout),
                    module: &shader,
                    entry_point: "main",
                    compilation_options: PipelineCompilationOptions::default(),
                    cache: None,
                })
        })?;
        let pipeline = Arc::new(pipeline);
        cache.insert(tile, Arc::clone(&pipeline));
        Ok(pipeline)
    }

    /// Executes matrix multiplication on the physical GPU and returns the computation outcome.
    /// `dim` and `tile` (the work group edge) are used exactly as given or rejected with
    /// [`GpuError::InvalidRequest`]; see [`WgpuEngine::execute_gemm_controlled`].
    pub fn execute_gemm(
        &self,
        kernel_name: &str,
        input_data: &[u8],
        task_id: Uuid,
        dim: usize,
        tile: usize,
    ) -> Result<WgpuComputeOutcome, GpuError> {
        self.execute_gemm_controlled(
            kernel_name,
            input_data,
            task_id,
            dim,
            tile,
            &GemmControl::default(),
        )
    }

    /// [`WgpuEngine::execute_gemm`] that stops waiting when `control` says so.
    ///
    /// Generates the seeded `dim x dim` matrices, multiplies them on the GPU with `tile x tile`
    /// work groups, reads the product back and checks it against a CPU recomputation
    /// ([`cpu_simd::verify_gemm`]); a product that fails the check is an error, not an outcome.
    /// Every wgpu error, a lost device and a panic out of the driver come back as
    /// [`GpuError::Device`]. Blocking.
    pub fn execute_gemm_controlled(
        &self,
        kernel_name: &str,
        input_data: &[u8],
        task_id: Uuid,
        dim: usize,
        tile: usize,
        control: &GemmControl,
    ) -> Result<WgpuComputeOutcome, GpuError> {
        let too_big =
            |what: &str| GpuError::InvalidRequest(format!("{what} does not fit in 32 bits"));
        let dim32 = u32::try_from(dim).map_err(|_| too_big("simulated_matrix_dim"))?;
        let tile32 = u32::try_from(tile).map_err(|_| too_big("work_group_size"))?;
        check_gemm_request(dim32, tile32, &self.device_limits)?;
        if self.is_lost() {
            return Err(GpuError::Device(self.lost_reason()));
        }

        let run = catch_unwind(AssertUnwindSafe(|| {
            self.run_gemm(kernel_name, input_data, task_id, dim, tile32, control)
        }));
        let result = match run {
            Ok(result) => result,
            Err(payload) => Err(GpuError::Device(format!(
                "the graphics driver panicked: {}",
                panic_message(payload.as_ref())
            ))),
        };
        if let Err(GpuError::Device(message)) = &result {
            // Remember why, so the next task's error says more than "unusable".
            lock(&self.background_error).get_or_insert_with(|| message.clone());
            self.lost.store(true, Ordering::SeqCst);
        }
        result
    }

    fn run_gemm(
        &self,
        kernel_name: &str,
        input_data: &[u8],
        task_id: Uuid,
        dim: usize,
        tile: u32,
        control: &GemmControl,
    ) -> Result<WgpuComputeOutcome, GpuError> {
        control.check()?;
        let seed = cpu_simd::derive_seed(input_data, task_id);
        let (a, b) = cpu_simd::generate_deterministic_matrices(dim, seed);
        let pipeline = self.pipeline_for_tile(tile)?;
        control.check()?;

        let num_elements = dim * dim;
        let buffer_size = (num_elements * std::mem::size_of::<f32>()) as u64;
        let dims_uniform = GpuDimensions {
            m: dim as u32,
            k: dim as u32,
            n: dim as u32,
            pad: 0,
        };

        let started = Instant::now();
        // Everything that talks to the device runs inside error scopes: buffer creation (too big,
        // out of memory), upload, bind group, encoding, submission and the map request.
        let (staging_buf, mapped) = with_error_scopes(&self.device, "running the GEMM", || {
            let device = &self.device;
            let uniform_buf = device.create_buffer(&BufferDescriptor {
                label: Some("gemm_uniform_buffer"),
                size: std::mem::size_of::<GpuDimensions>() as u64,
                usage: BufferUsages::UNIFORM | BufferUsages::COPY_DST,
                mapped_at_creation: false,
            });
            let storage = |label: &'static str, usage: BufferUsages| {
                device.create_buffer(&BufferDescriptor {
                    label: Some(label),
                    size: buffer_size,
                    usage,
                    mapped_at_creation: false,
                })
            };
            let buf_a = storage(
                "gemm_matrix_a_buffer",
                BufferUsages::STORAGE | BufferUsages::COPY_DST,
            );
            let buf_b = storage(
                "gemm_matrix_b_buffer",
                BufferUsages::STORAGE | BufferUsages::COPY_DST,
            );
            let buf_c = storage(
                "gemm_matrix_c_buffer",
                BufferUsages::STORAGE | BufferUsages::COPY_SRC | BufferUsages::COPY_DST,
            );
            let staging_buf = storage(
                "gemm_staging_buffer",
                BufferUsages::MAP_READ | BufferUsages::COPY_DST,
            );

            self.queue
                .write_buffer(&uniform_buf, 0, bytemuck::bytes_of(&dims_uniform));
            self.queue.write_buffer(&buf_a, 0, bytemuck::cast_slice(&a));
            self.queue.write_buffer(&buf_b, 0, bytemuck::cast_slice(&b));

            let bind_group = device.create_bind_group(&BindGroupDescriptor {
                label: Some("gemm_bind_group"),
                layout: &self.gemm_bind_group_layout,
                entries: &[
                    BindGroupEntry {
                        binding: 0,
                        resource: uniform_buf.as_entire_binding(),
                    },
                    BindGroupEntry {
                        binding: 1,
                        resource: buf_a.as_entire_binding(),
                    },
                    BindGroupEntry {
                        binding: 2,
                        resource: buf_b.as_entire_binding(),
                    },
                    BindGroupEntry {
                        binding: 3,
                        resource: buf_c.as_entire_binding(),
                    },
                ],
            });

            let mut encoder = device.create_command_encoder(&CommandEncoderDescriptor {
                label: Some("gemm_compute_encoder"),
            });
            {
                let mut compute_pass = encoder.begin_compute_pass(&ComputePassDescriptor {
                    label: Some("gemm_compute_pass"),
                    timestamp_writes: None,
                });
                compute_pass.set_pipeline(&pipeline);
                compute_pass.set_bind_group(0, &bind_group, &[]);
                // One work group of tile x tile threads per output tile.
                let groups = dim.div_ceil(tile as usize) as u32;
                compute_pass.dispatch_workgroups(groups, groups, 1);
            }
            encoder.copy_buffer_to_buffer(&buf_c, 0, &staging_buf, 0, buffer_size);
            self.queue.submit(Some(encoder.finish()));

            let (tx, rx) = std::sync::mpsc::channel();
            staging_buf
                .slice(..)
                .map_async(MapMode::Read, move |result| {
                    let _ = tx.send(result);
                });
            (staging_buf, rx)
        })?;

        self.wait_for_map(&mapped, control)?;

        let mut matrix_c = vec![0.0f32; num_elements];
        {
            let view = staging_buf.slice(..).get_mapped_range();
            let target_bytes = bytemuck::cast_slice_mut(&mut matrix_c[..]);
            target_bytes.copy_from_slice(&view[..target_bytes.len()]);
        }
        staging_buf.unmap();
        let elapsed_ms = std::cmp::max(1, started.elapsed().as_millis() as u64);

        let verification = cpu_simd::verify_gemm(&a, &b, &matrix_c, dim);
        if !verification.passed() {
            return Err(GpuError::VerificationFailed(verification.summary()));
        }
        let metrics = cpu_simd::compute_matrix_metrics(&matrix_c, dim);
        let backend_name = format!("{:?}", self.adapter_info.backend);
        let stdout = cpu_simd::format_report(&ComputeReport {
            banner: "[GPU COMPUTE WGPU]",
            device: &self.adapter_info.name,
            backend: &backend_name,
            kernel_name,
            dim,
            tile: tile as usize,
            work_group_note: "",
            execution_ms: elapsed_ms,
            extra_lines: &control.extra_report_lines,
            metrics: &metrics,
            digest_scope: "GPU-specific: fused multiply-add and summation order make it differ from a CPU worker's digest; compare Matrix Trace and Frobenius Norm within tolerance instead",
            verification: &verification,
        });

        Ok(WgpuComputeOutcome {
            stdout,
            elapsed_ms,
            device_name: self.adapter_info.name.clone(),
            backend_name,
            metrics,
            matrix_c,
            verification,
        })
    }

    /// Polls the device until the staging buffer is mapped, the device is lost, or `control`
    /// asks to stop. (`Maintain::Wait` would block with no way out if the driver hangs.)
    fn wait_for_map(
        &self,
        mapped: &std::sync::mpsc::Receiver<Result<(), BufferAsyncError>>,
        control: &GemmControl,
    ) -> Result<(), GpuError> {
        let mut polls = 0u32;
        loop {
            self.device.poll(Maintain::Poll);
            match mapped.try_recv() {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(error)) => {
                    return Err(GpuError::Device(format!(
                        "mapping the result buffer failed: {error:?}{}",
                        if self.is_lost() {
                            format!(" ({})", self.lost_reason())
                        } else {
                            String::new()
                        }
                    )))
                }
                Err(TryRecvError::Disconnected) => {
                    return Err(GpuError::Device(
                        "the result buffer mapping was abandoned".to_string(),
                    ))
                }
                Err(TryRecvError::Empty) => {}
            }
            if self.is_lost() {
                return Err(GpuError::Device(self.lost_reason()));
            }
            control.check()?;
            polls += 1;
            if polls < 64 {
                std::thread::yield_now();
            } else {
                std::thread::sleep(Duration::from_micros(250));
            }
        }
    }
}

/// Why no engine could be handed out.
#[derive(Debug, Clone, PartialEq, Eq)]
enum InitFailure {
    NoAdapter,
    Failed(String),
}

impl InitFailure {
    fn into_error(self) -> GpuError {
        match self {
            InitFailure::NoAdapter => {
                GpuError::Unavailable("no usable GPU adapter was found on this machine".to_string())
            }
            InitFailure::Failed(reason) => {
                GpuError::Unavailable(format!("the GPU engine could not be started: {reason}"))
            }
        }
    }
}

struct ProviderSlot {
    engine: Option<Arc<WgpuEngine>>,
    last_failure: Option<(Instant, InitFailure)>,
}

/// An engine for one task, and how long building it took if that happened for this task.
pub struct EngineLease {
    pub engine: Arc<WgpuEngine>,
    /// Milliseconds spent starting the engine for this lease; `None` if it was already running.
    pub init_ms: Option<u64>,
}

/// Owns the process's [`WgpuEngine`]: starts it on first use, hands it out, and starts a new one
/// when the old one is lost. A start-up that fails is remembered for a couple of seconds so a
/// burst of tasks does not enumerate adapters once each.
pub struct EngineProvider {
    slot: Mutex<ProviderSlot>,
}

impl Default for EngineProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl EngineProvider {
    pub fn new() -> Self {
        Self {
            slot: Mutex::new(ProviderSlot {
                engine: None,
                last_failure: None,
            }),
        }
    }

    /// The provider every [`crate::runner::TaskRunner`] and the start-up probe share, so the
    /// engine built to find out whether a GPU works is the one the first task runs on.
    pub fn shared() -> Arc<EngineProvider> {
        static SHARED: OnceLock<Arc<EngineProvider>> = OnceLock::new();
        Arc::clone(SHARED.get_or_init(|| Arc::new(EngineProvider::new())))
    }

    fn acquire(&self) -> Result<EngineLease, InitFailure> {
        let mut slot = lock(&self.slot);
        if let Some(engine) = &slot.engine {
            if !engine.is_lost() {
                return Ok(EngineLease {
                    engine: Arc::clone(engine),
                    init_ms: None,
                });
            }
            tracing::warn!("the GPU engine was lost; starting a new one");
            slot.engine = None;
        }
        if let Some((failed_at, failure)) = &slot.last_failure {
            if failed_at.elapsed() < INIT_RETRY_COOLDOWN {
                return Err(failure.clone());
            }
        }
        let started = Instant::now();
        match WgpuEngine::try_init() {
            Ok(Some(engine)) => {
                let engine = Arc::new(engine);
                slot.engine = Some(Arc::clone(&engine));
                slot.last_failure = None;
                Ok(EngineLease {
                    engine,
                    init_ms: Some(started.elapsed().as_millis() as u64),
                })
            }
            Ok(None) => {
                slot.last_failure = Some((Instant::now(), InitFailure::NoAdapter));
                Err(InitFailure::NoAdapter)
            }
            Err(reason) => {
                tracing::warn!("WGPU engine initialization failed: {reason}");
                let failure = InitFailure::Failed(reason);
                slot.last_failure = Some((Instant::now(), failure.clone()));
                Err(failure)
            }
        }
    }

    /// A working engine, starting (or restarting) one if needed. Blocking on the first call.
    pub fn get(&self) -> Result<EngineLease, GpuError> {
        self.acquire().map_err(InitFailure::into_error)
    }

    /// Whether this provider can hand out an engine, as the capability policy wants to know it.
    pub fn probe(&self) -> GpuProbeOutcome {
        match self.acquire() {
            Ok(lease) => GpuProbeOutcome::Found(GpuProbe {
                device_name: lease.engine.adapter_info.name.clone(),
                backend: format!("{:?}", lease.engine.adapter_info.backend),
                device_type: format!("{:?}", lease.engine.adapter_info.device_type),
            }),
            Err(InitFailure::NoAdapter) => GpuProbeOutcome::NoAdapter,
            Err(InitFailure::Failed(reason)) => GpuProbeOutcome::Failed(reason),
        }
    }
}

/// [`EngineProvider::probe`] on the shared provider.
pub fn probe_shared_engine() -> GpuProbeOutcome {
    EngineProvider::shared().probe()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(device_type: DeviceType, backend: Backend) -> AdapterInfo {
        AdapterInfo {
            name: "test".into(),
            vendor: 0,
            device: 0,
            device_type,
            driver: String::new(),
            driver_info: String::new(),
            backend,
        }
    }

    #[test]
    fn adapter_ranking_prefers_discrete_over_integrated_and_excludes_software() {
        let discrete = rank_adapter(&info(DeviceType::DiscreteGpu, Backend::Vulkan), true).unwrap();
        let integrated =
            rank_adapter(&info(DeviceType::IntegratedGpu, Backend::Metal), true).unwrap();
        let virtual_gpu =
            rank_adapter(&info(DeviceType::VirtualGpu, Backend::Vulkan), true).unwrap();
        let other = rank_adapter(&info(DeviceType::Other, Backend::Vulkan), true).unwrap();
        assert!(discrete > integrated && integrated > virtual_gpu && virtual_gpu > other);
        // the type decides, whatever the backend
        let discrete_gl = rank_adapter(&info(DeviceType::DiscreteGpu, Backend::Gl), true).unwrap();
        assert!(discrete_gl > integrated);
        // a software rasterizer is never a GPU, and an adapter without compute cannot run GEMM
        assert_eq!(
            rank_adapter(&info(DeviceType::Cpu, Backend::Vulkan), true),
            None
        );
        assert_eq!(
            rank_adapter(&info(DeviceType::DiscreteGpu, Backend::Vulkan), false),
            None
        );
    }

    #[test]
    fn same_gpu_through_two_apis_prefers_native_over_gl() {
        let vulkan = rank_adapter(&info(DeviceType::DiscreteGpu, Backend::Vulkan), true).unwrap();
        let dx12 = rank_adapter(&info(DeviceType::DiscreteGpu, Backend::Dx12), true).unwrap();
        let gl = rank_adapter(&info(DeviceType::DiscreteGpu, Backend::Gl), true).unwrap();
        assert!(vulkan > dx12 && dx12 > gl);
    }

    #[test]
    fn the_shader_template_has_exactly_one_substitutable_tile_line() {
        assert_eq!(GEMM_TEMPLATE.matches(TILE_LINE).count(), 1);
        assert!(gemm_source(8).contains("const TILE: u32 = 8u;"));
        assert!(!gemm_source(8).contains(TILE_LINE));
        assert_eq!(gemm_source(16), GEMM_TEMPLATE);
    }

    #[test]
    fn wgpu_limits_map_onto_device_limits() {
        let limits = Limits::default();
        let mapped = device_limits_from(&limits);
        assert_eq!(mapped.max_matrix_bytes, 128 * 1024 * 1024);
        assert_eq!(mapped.max_workgroup_invocations, 256);
        assert_eq!(mapped.max_workgroup_shared_bytes, 16_384);
        assert_eq!(mapped.max_dim, GPU_MAX_DIM);
        // the default limits allow the default tile at the 5792 edge and not one more
        assert!(check_gemm_request(5792, 16, &mapped).is_ok());
        assert!(check_gemm_request(5793, 16, &mapped).is_err());
    }

    #[test]
    fn panic_messages_are_extracted() {
        let from_str = catch_unwind(|| panic!("boom")).unwrap_err();
        assert_eq!(panic_message(from_str.as_ref()), "boom");
        let from_string = catch_unwind(|| panic!("{}", String::from("bang"))).unwrap_err();
        assert_eq!(panic_message(from_string.as_ref()), "bang");
    }

    /// An engine of the test's own (so destroying or poisoning it affects nobody), or None on a
    /// host without a GPU. `RUSTY_GRID_REQUIRE_GPU=1` turns that skip into a failure.
    fn own_engine(test: &str) -> Option<WgpuEngine> {
        match WgpuEngine::try_init() {
            Ok(Some(engine)) => Some(engine),
            other => {
                assert!(
                    std::env::var("RUSTY_GRID_REQUIRE_GPU").as_deref() != Ok("1"),
                    "RUSTY_GRID_REQUIRE_GPU=1 but {test} could not start a GPU engine: {other:?}"
                );
                println!("{test}: skipped, no GPU engine on this host ({other:?})");
                None
            }
        }
    }

    fn oversized_storage_buffer(engine: &WgpuEngine) -> Buffer {
        engine.device.create_buffer(&BufferDescriptor {
            label: Some("oversized"),
            size: engine.device.limits().max_buffer_size + 1,
            usage: BufferUsages::STORAGE,
            mapped_at_creation: false,
        })
    }

    #[test]
    fn the_device_is_created_with_the_adapters_own_limits_not_the_webgpu_defaults() {
        let Some(engine) = own_engine("the_device_is_created_with_the_adapters_own_limits") else {
            return;
        };
        let adapter =
            select_adapter(&new_instance()).expect("the engine just started on an adapter");
        let granted = engine.device.limits();
        let offered = adapter.limits();
        assert_eq!(granted.max_buffer_size, offered.max_buffer_size);
        assert_eq!(
            granted.max_storage_buffer_binding_size,
            offered.max_storage_buffer_binding_size
        );
        assert_eq!(
            granted.max_compute_invocations_per_workgroup,
            offered.max_compute_invocations_per_workgroup
        );
        assert_eq!(engine.device_limits(), device_limits_from(&offered));
    }

    #[test]
    fn an_oversized_buffer_is_an_error_result_not_a_panic() {
        let Some(engine) = own_engine("an_oversized_buffer_is_an_error_result_not_a_panic") else {
            return;
        };
        // Inside the scopes wgpu's validation error comes back as a value ...
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            with_error_scopes(&engine.device, "allocating too much", || {
                oversized_storage_buffer(&engine)
            })
        }))
        .expect("the error scope must absorb the validation error instead of panicking");
        let GpuError::Device(message) = outcome
            .map(|_| ())
            .expect_err("an oversized buffer is invalid")
        else {
            panic!("expected a device error");
        };
        assert!(
            message.starts_with("allocating too much: validation error"),
            "{message}"
        );
        assert!(!message.contains('\n'), "one line, got {message:?}");
        // ... and the engine is still fine afterwards.
        assert!(!engine.is_lost());
        assert!(engine
            .execute_gemm("after", b"x", Uuid::new_v4(), 16, 16)
            .is_ok());
    }

    #[test]
    fn an_error_outside_any_scope_is_recorded_not_a_panic() {
        let Some(engine) = own_engine("an_error_outside_any_scope_is_recorded_not_a_panic") else {
            return;
        };
        // With wgpu's default handler this very call panics ("wgpu error: Validation Error").
        let created = catch_unwind(AssertUnwindSafe(|| oversized_storage_buffer(&engine)));
        assert!(
            created.is_ok(),
            "the uncaptured-error handler must replace wgpu's panicking default"
        );
        let recorded = lock(&engine.background_error)
            .clone()
            .expect("the error was recorded");
        assert!(recorded.starts_with("validation error"), "{recorded}");
    }

    #[test]
    fn a_broken_shader_is_an_error_result_not_a_panic() {
        let Some(engine) = own_engine("a_broken_shader_is_an_error_result_not_a_panic") else {
            return;
        };
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            with_error_scopes(&engine.device, "compiling a broken shader", || {
                engine.device.create_shader_module(ShaderModuleDescriptor {
                    label: Some("broken"),
                    source: ShaderSource::Wgsl("this is not wgsl".into()),
                })
            })
        }))
        .expect("a shader compile error must not panic");
        let err = outcome.map(|_| ()).expect_err("garbage is not a shader");
        assert!(err.to_string().contains("validation error"), "{err}");
    }

    #[test]
    fn waiting_for_the_gpu_stops_on_cancel_and_on_deadline() {
        let Some(engine) = own_engine("waiting_for_the_gpu_stops_on_cancel_and_on_deadline") else {
            return;
        };
        // A mapping that never completes (the sender stays alive, as with a hung driver).
        let (never_sent, receiver) = std::sync::mpsc::channel::<Result<(), BufferAsyncError>>();

        let cancel = Arc::new(AtomicBool::new(false));
        let setter = {
            let cancel = Arc::clone(&cancel);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(50));
                cancel.store(true, Ordering::SeqCst);
            })
        };
        let started = Instant::now();
        let err = engine
            .wait_for_map(
                &receiver,
                &GemmControl {
                    cancel: Some(cancel),
                    ..Default::default()
                },
            )
            .expect_err("cancelled");
        assert!(matches!(err, GpuError::Interrupted(_)), "{err:?}");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{:?}",
            started.elapsed()
        );
        setter.join().unwrap();

        let started = Instant::now();
        let err = engine
            .wait_for_map(
                &receiver,
                &GemmControl {
                    deadline: Some(Instant::now() + Duration::from_millis(50)),
                    ..Default::default()
                },
            )
            .expect_err("deadline");
        assert!(matches!(err, GpuError::Interrupted(_)), "{err:?}");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{:?}",
            started.elapsed()
        );
        drop(never_sent);
        // a sender that went away is an error too, not an endless wait
        let err = engine
            .wait_for_map(&receiver, &GemmControl::default())
            .expect_err("abandoned");
        assert!(matches!(err, GpuError::Device(_)), "{err:?}");
    }

    #[test]
    fn control_reports_cancel_and_deadline() {
        assert!(GemmControl::default().check().is_ok());
        let cancel = Arc::new(AtomicBool::new(false));
        let control = GemmControl {
            cancel: Some(Arc::clone(&cancel)),
            ..Default::default()
        };
        assert!(control.check().is_ok());
        cancel.store(true, Ordering::Relaxed);
        assert!(matches!(control.check(), Err(GpuError::Interrupted(_))));
        let late = GemmControl {
            deadline: Some(
                Instant::now()
                    .checked_sub(Duration::from_secs(1))
                    .unwrap_or_else(Instant::now),
            ),
            ..Default::default()
        };
        assert!(matches!(late.check(), Err(GpuError::Interrupted(_))));
    }
}
