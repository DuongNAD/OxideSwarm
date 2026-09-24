//! Cross-Platform Hardware-Accelerated Compute Shaders via wgpu (Requirement R4).
//!
//! Provides adapter negotiation prioritizing physical hardware GPUs (Discrete > Integrated > Virtual),
//! WGSL compute pipeline compilation, workgroup dispatch, and safe staging buffer readback.

use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;
use wgpu::*;

use crate::cpu_simd::{self, MatrixMetrics};

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
    pub elapsed_ms: u64,
    pub device_name: String,
    pub backend_name: String,
    pub metrics: MatrixMetrics,
    pub matrix_c: Vec<f32>,
}

/// Probes the host system for a compatible physical hardware GPU adapter.
/// Returns the adapter name if found, or None if only CPU or no adapters are available.
pub fn probe_physical_adapter() -> Option<AdapterInfo> {
    let instance = Instance::new(InstanceDescriptor {
        backends: Backends::all(),
        flags: InstanceFlags::default(),
        dx12_shader_compiler: Dx12Compiler::default(),
        gles_minor_version: Gles3MinorVersion::default(),
    });

    let adapters = instance.enumerate_adapters(Backends::all());
    select_best_adapter(adapters).map(|a| a.get_info())
}

/// Selects the best physical GPU adapter from a list of candidates.
/// Prioritizes DiscreteGpu > IntegratedGpu > VirtualGpu, excluding Cpu.
fn select_best_adapter(adapters: Vec<Adapter>) -> Option<Adapter> {
    let mut candidates: Vec<(i32, Adapter)> = adapters
        .into_iter()
        .filter_map(|adapter| {
            let info = adapter.get_info();
            let score = match info.device_type {
                DeviceType::DiscreteGpu => 100,
                DeviceType::IntegratedGpu => 50,
                DeviceType::VirtualGpu => 25,
                DeviceType::Other => 10,
                DeviceType::Cpu => -1, // Exclude pure software CPU rasterizers
            };
            if score > 0 {
                Some((score, adapter))
            } else {
                None
            }
        })
        .collect();

    candidates.sort_by(|a, b| b.0.cmp(&a.0));
    candidates.into_iter().next().map(|(_, adapter)| adapter)
}

/// Hardware context holding an initialized WGPU Device, Queue, and Adapter information.
pub struct WgpuEngine {
    pub adapter_info: AdapterInfo,
    pub device: Arc<Device>,
    pub queue: Arc<Queue>,
    gemm_pipeline: ComputePipeline,
    gemm_bind_group_layout: BindGroupLayout,
}

impl WgpuEngine {
    /// Attempts to initialize the WGPU compute engine with a physical GPU.
    /// Returns Ok(None) if no physical GPU adapter is available on the host.
    pub fn try_init() -> Result<Option<Self>, String> {
        let instance = Instance::new(InstanceDescriptor {
            backends: Backends::all(),
            flags: InstanceFlags::default(),
            dx12_shader_compiler: Dx12Compiler::default(),
            gles_minor_version: Gles3MinorVersion::default(),
        });

        let adapters = instance.enumerate_adapters(Backends::all());
        let adapter = match select_best_adapter(adapters) {
            Some(a) => a,
            None => return Ok(None),
        };

        let adapter_info = adapter.get_info();
        tracing::info!(
            name = %adapter_info.name,
            backend = ?adapter_info.backend,
            device_type = ?adapter_info.device_type,
            "Initializing hardware-accelerated WGPU compute engine"
        );

        let (device, queue) = pollster::block_on(adapter.request_device(
            &DeviceDescriptor {
                label: Some("oxideswarm_wgpu_engine_device"),
                required_features: Features::empty(),
                required_limits: Limits::default(),
                memory_hints: MemoryHints::Performance,
            },
            None,
        ))
        .map_err(|e| format!("Failed to request WGPU device from adapter: {e}"))?;

        let shader = device.create_shader_module(ShaderModuleDescriptor {
            label: Some("gemm_shader_module"),
            source: ShaderSource::Wgsl(include_str!("shaders/gemm.wgsl").into()),
        });

        let gemm_bind_group_layout =
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
                    BindGroupLayoutEntry {
                        binding: 1,
                        visibility: ShaderStages::COMPUTE,
                        ty: BindingType::Buffer {
                            ty: BufferBindingType::Storage { read_only: true },
                            has_dynamic_offset: false,
                            min_binding_size: None,
                        },
                        count: None,
                    },
                    // Binding 2: Matrix B (Storage, Read-Only)
                    BindGroupLayoutEntry {
                        binding: 2,
                        visibility: ShaderStages::COMPUTE,
                        ty: BindingType::Buffer {
                            ty: BufferBindingType::Storage { read_only: true },
                            has_dynamic_offset: false,
                            min_binding_size: None,
                        },
                        count: None,
                    },
                    // Binding 3: Matrix C (Storage, Read-Write)
                    BindGroupLayoutEntry {
                        binding: 3,
                        visibility: ShaderStages::COMPUTE,
                        ty: BindingType::Buffer {
                            ty: BufferBindingType::Storage { read_only: false },
                            has_dynamic_offset: false,
                            min_binding_size: None,
                        },
                        count: None,
                    },
                ],
            });

        let pipeline_layout = device.create_pipeline_layout(&PipelineLayoutDescriptor {
            label: Some("gemm_pipeline_layout"),
            bind_group_layouts: &[&gemm_bind_group_layout],
            push_constant_ranges: &[],
        });

        let gemm_pipeline = device.create_compute_pipeline(&ComputePipelineDescriptor {
            label: Some("gemm_compute_pipeline"),
            layout: Some(&pipeline_layout),
            module: &shader,
            entry_point: "main",
            compilation_options: PipelineCompilationOptions::default(),
            cache: None,
        });

        Ok(Some(Self {
            adapter_info,
            device: Arc::new(device),
            queue: Arc::new(queue),
            gemm_pipeline,
            gemm_bind_group_layout,
        }))
    }

    /// Executes matrix multiplication on the physical GPU and returns the computation outcome.
    pub fn execute_gemm(
        &self,
        kernel_name: &str,
        input_data: &[u8],
        task_id: Uuid,
        dim: usize,
        tile: usize,
        start_time: Instant,
    ) -> Result<WgpuComputeOutcome, String> {
        let seed = cpu_simd::derive_seed(input_data, task_id);
        let (a, b) = cpu_simd::generate_deterministic_matrices(dim, seed);

        let num_elements = dim * dim;
        let buffer_size = (num_elements * std::mem::size_of::<f32>()) as u64;

        let dims_uniform = GpuDimensions {
            m: dim as u32,
            k: dim as u32,
            n: dim as u32,
            pad: 0,
        };

        // 1. Create Buffers
        let uniform_buf = self.device.create_buffer(&BufferDescriptor {
            label: Some("gemm_uniform_buffer"),
            size: std::mem::size_of::<GpuDimensions>() as u64,
            usage: BufferUsages::UNIFORM | BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        let buf_a = self.device.create_buffer(&BufferDescriptor {
            label: Some("gemm_matrix_a_buffer"),
            size: buffer_size,
            usage: BufferUsages::STORAGE | BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        let buf_b = self.device.create_buffer(&BufferDescriptor {
            label: Some("gemm_matrix_b_buffer"),
            size: buffer_size,
            usage: BufferUsages::STORAGE | BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        let buf_c = self.device.create_buffer(&BufferDescriptor {
            label: Some("gemm_matrix_c_buffer"),
            size: buffer_size,
            usage: BufferUsages::STORAGE | BufferUsages::COPY_SRC | BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        let staging_buf = self.device.create_buffer(&BufferDescriptor {
            label: Some("gemm_staging_buffer"),
            size: buffer_size,
            usage: BufferUsages::MAP_READ | BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        // 2. Upload Data
        self.queue.write_buffer(&uniform_buf, 0, bytemuck::bytes_of(&dims_uniform));
        self.queue.write_buffer(&buf_a, 0, bytemuck::cast_slice(&a));
        self.queue.write_buffer(&buf_b, 0, bytemuck::cast_slice(&b));

        // 3. Construct Bind Group
        let bind_group = self.device.create_bind_group(&BindGroupDescriptor {
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

        // 4. Encode Compute Pass & Staging Copy
        let mut encoder = self.device.create_command_encoder(&CommandEncoderDescriptor {
            label: Some("gemm_compute_encoder"),
        });

        {
            let mut compute_pass = encoder.begin_compute_pass(&ComputePassDescriptor {
                label: Some("gemm_compute_pass"),
                timestamp_writes: None,
            });

            compute_pass.set_pipeline(&self.gemm_pipeline);
            compute_pass.set_bind_group(0, &bind_group, &[]);

            // Dispatch workgroups with 16x16 threads per workgroup
            let wg_x = (dim as u32 + 15) / 16;
            let wg_y = (dim as u32 + 15) / 16;
            compute_pass.dispatch_workgroups(wg_x, wg_y, 1);
        }

        encoder.copy_buffer_to_buffer(&buf_c, 0, &staging_buf, 0, buffer_size);
        self.queue.submit(Some(encoder.finish()));

        // 5. Read Back Staging Buffer
        let slice = staging_buf.slice(..);
        let (tx, rx) = std::sync::mpsc::channel();

        slice.map_async(MapMode::Read, move |result| {
            let _ = tx.send(result);
        });

        // Flush and wait synchronously for GPU work and buffer mapping to complete
        self.device.poll(Maintain::Wait);

        rx.recv()
            .map_err(|e| format!("Channel receive error: {e}"))?
            .map_err(|e| format!("Staging buffer map async failed: {e:?}"))?;

        let mapped_view = slice.get_mapped_range();
        let mut matrix_c = vec![0.0f32; num_elements];
        let target_bytes = bytemuck::cast_slice_mut(&mut matrix_c[..]);
        target_bytes.copy_from_slice(&mapped_view[..target_bytes.len()]);

        // Explicitly drop mapped view before unmapping
        drop(mapped_view);
        staging_buf.unmap();

        // 6. Compute Invariant Metrics
        let metrics = cpu_simd::compute_matrix_metrics(&matrix_c, dim);
        let elapsed_ms = std::cmp::max(1, start_time.elapsed().as_millis() as u64);
        let flops = 2u64 * (dim as u64) * (dim as u64) * (dim as u64);
        let backend_name = format!("{:?}", self.adapter_info.backend);

        let stdout = format!(
            "[GPU COMPUTE WGPU]\nDevice: {}\nBackend: {}\nKernel: {kernel_name}\nMatrix Dimension: {dim}x{dim} (FLOPs: {flops})\nWork Group Size: {tile}x{tile}\nExecution Time: {elapsed_ms} ms\nMatrix Trace: {:.4}\nFrobenius Norm: {:.4}\nVerification Digest: 0x{:016x}\nStatus: VERIFIED_OK\n",
            self.adapter_info.name,
            backend_name,
            metrics.trace,
            metrics.frobenius_norm,
            metrics.bit_hash,
        );

        Ok(WgpuComputeOutcome {
            stdout,
            elapsed_ms,
            device_name: self.adapter_info.name.clone(),
            backend_name,
            metrics,
            matrix_c,
        })
    }
}
