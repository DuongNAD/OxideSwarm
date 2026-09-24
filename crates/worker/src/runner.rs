//! Task execution runner, process supervision, watchdog timeouts, and GPU simulation.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::WorkerMessage;
use rusty_grid_core::task::{Bytes, BytesMut, CheckpointData, Task, TaskId, TaskResult, TaskSpec};

use crate::cpu_simd;
use crate::sandbox::{sanitize_relative_path, Sandbox, SandboxConfig, SandboxError};

/// Exit code emitted on successful completion.
pub const EXIT_CODE_SUCCESS: i32 = 0;
/// Exit code emitted on general execution failure.
pub const EXIT_CODE_GENERAL_ERROR: i32 = 1;
/// Exit code emitted when a task is cancelled via Master directive.
pub const EXIT_CODE_CANCELLED: i32 = 130;
/// Standard GNU timeout exit code emitted when the watchdog terminates a process.
pub const EXIT_CODE_TIMEOUT: i32 = 124;

/// Default maximum buffer size for captured stdout and stderr streams (10 MB).
pub const DEFAULT_MAX_OUTPUT_BYTES: usize = 10 * 1024 * 1024;

/// Configuration options for the TaskRunner.
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    pub sandbox_config: SandboxConfig,
    pub max_output_bytes: usize,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            sandbox_config: SandboxConfig::default(),
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
        }
    }
}

impl RunnerConfig {
    pub fn new(sandbox_config: SandboxConfig) -> Self {
        Self {
            sandbox_config,
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
        }
    }

    pub fn with_max_output_bytes(mut self, bytes: usize) -> Self {
        self.max_output_bytes = bytes;
        self
    }
}

/// Errors occurring within the task runner.
#[derive(Debug, thiserror::Error)]
pub enum TaskRunnerError {
    #[error("Sandbox error: {0}")]
    Sandbox(#[from] SandboxError),

    #[error("Process spawn failed for command '{command}': {source}")]
    ProcessSpawn {
        command: String,
        #[source]
        source: std::io::Error,
    },

    #[error("Task {task_id} timed out after {timeout_secs}s")]
    Timeout { task_id: TaskId, timeout_secs: u64 },

    #[error("Task {task_id} execution was cancelled: {reason}")]
    Cancelled { task_id: TaskId, reason: String },

    #[error("GPU execution error for task {task_id}: {reason}")]
    GpuExecution { task_id: TaskId, reason: String },

    #[error("I/O error during task execution: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(windows)]
fn augment_windows_path(existing_path: &str) -> String {
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

#[cfg(windows)]
fn resolve_windows_executable(program: &str, effective_path: &str) -> String {
    let p = Path::new(program);
    if p.is_file() {
        return program.to_string();
    }
    for ext in &[".exe", ".cmd", ".bat"] {
        let with_ext = format!("{program}{ext}");
        if Path::new(&with_ext).is_file() {
            return with_ext;
        }
    }

    let bare_name = p.file_name().and_then(|n| n.to_str()).unwrap_or(program);

    // Fast path: check Git for Windows usr/bin first
    for base in &[r"C:\Program Files\Git\usr\bin", r"C:\Program Files\Git\bin"] {
        let base_path = Path::new(base);
        for ext in &[".exe", ".cmd", ".bat", ""] {
            let candidate = base_path.join(format!("{bare_name}{ext}"));
            if candidate.is_file() {
                return candidate.to_string_lossy().to_string();
            }
        }
    }

    for dir in std::env::split_paths(effective_path) {
        let candidate = dir.join(bare_name);
        if candidate.is_file() {
            return candidate.to_string_lossy().to_string();
        }
        for ext in &[".exe", ".cmd", ".bat"] {
            let candidate_ext = dir.join(format!("{bare_name}{ext}"));
            if candidate_ext.is_file() {
                return candidate_ext.to_string_lossy().to_string();
            }
        }
    }

    program.to_string()
}

/// Reads an async stream into a bounded memory buffer without pipe deadlocks.
///
/// Continues draining the pipe to EOF even after the buffer is filled, ensuring
/// the child process does not block on write operations.
pub async fn read_bounded_stream<R: tokio::io::AsyncRead + Unpin>(
    mut reader: R,
    max_bytes: usize,
) -> (Bytes, bool) {
    use tokio::io::AsyncReadExt;

    let mut captured = BytesMut::with_capacity(std::cmp::min(max_bytes, 64 * 1024));
    let mut scratch = [0u8; 8192];
    let mut truncated = false;

    loop {
        match reader.read(&mut scratch).await {
            Ok(0) => break, // EOF reached
            Ok(n) => {
                if captured.len() < max_bytes {
                    let available = max_bytes - captured.len();
                    let to_copy = std::cmp::min(available, n);
                    captured.extend_from_slice(&scratch[..to_copy]);
                    if n > to_copy {
                        truncated = true;
                    }
                } else {
                    truncated = true;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        }
    }

    if truncated {
        captured.extend_from_slice(b"\n[rusty_grid: output truncated after exceeding size limit]\n");
    }

    (captured.freeze().into(), truncated)
}

/// Core computational workload execution engine for Worker nodes.
pub struct TaskRunner {
    worker_id: Uuid,
    capabilities: WorkerCapabilities,
    config: RunnerConfig,
    #[cfg(feature = "gpu-wgpu")]
    gpu_engine: std::sync::OnceLock<Option<Arc<crate::wgpu_engine::WgpuEngine>>>,
}

impl TaskRunner {
    pub fn new(worker_id: Uuid, capabilities: WorkerCapabilities, config: RunnerConfig) -> Self {
        Self {
            worker_id,
            capabilities,
            config,
            #[cfg(feature = "gpu-wgpu")]
            gpu_engine: std::sync::OnceLock::new(),
        }
    }

    #[cfg(feature = "gpu-wgpu")]
    fn get_gpu_engine(&self) -> Option<Arc<crate::wgpu_engine::WgpuEngine>> {
        self.gpu_engine
            .get_or_init(|| match crate::wgpu_engine::WgpuEngine::try_init() {
                Ok(engine) => engine.map(Arc::new),
                Err(err) => {
                    warn!("WGPU engine initialization failed: {err}");
                    None
                }
            })
            .clone()
    }

    pub fn worker_id(&self) -> Uuid {
        self.worker_id
    }

    pub fn capabilities(&self) -> &WorkerCapabilities {
        &self.capabilities
    }

    pub fn config(&self) -> &RunnerConfig {
        &self.config
    }

    /// Primary execution dispatcher with checkpoint resumption and mid-task checkpoint emission.
    pub async fn execute_task_with_checkpoint(
        &self,
        task: &Task,
        checkpoint: Option<&CheckpointData>,
        checkpoint_tx: Option<mpsc::Sender<WorkerMessage>>,
        child_pid: Option<Arc<AtomicU32>>,
        cancel_rx: Option<oneshot::Receiver<String>>,
    ) -> TaskResult {
        let start_time = Instant::now();
        let timeout_secs = if task.requirements.timeout_secs == 0 {
            60
        } else {
            task.requirements.timeout_secs
        };
        let timeout_duration = Duration::from_secs(timeout_secs);

        // 1. Strict GPU capability gating
        if let TaskSpec::GpuCompute { .. } = &task.spec {
            if !self.capabilities.can_execute_gpu() {
                warn!(
                    worker_id = %self.worker_id,
                    task_id = %task.id,
                    "GPU task rejected: worker lacks physical or simulated GPU capability"
                );
                return TaskResult {
                    worker_id: self.worker_id,
                    task_id: task.id,
                    exit_code: EXIT_CODE_GENERAL_ERROR,
                    stdout: Bytes::new(),
                    stderr: Bytes::from_static(b"Execution Error: Task requires GPU capability, but this worker has no GPU (Worker lacks GPU capability)."),
                    execution_time_ms: 0,
                    is_gpu_executed: false,
                    device_name: None,
                    error: Some("Worker lacks GPU capability".to_string()),
                };
            }
        }

        // 2. Dispatch in-memory workloads (BuiltinTest, GpuCompute) or process-based workloads
        match &task.spec {
            TaskSpec::BuiltinTest { .. } => {
                self.execute_builtin_test(task, checkpoint, checkpoint_tx, timeout_duration, cancel_rx, start_time)
                    .await
            }

            TaskSpec::GpuCompute { .. } => {
                self.execute_gpu_compute(task, timeout_duration, cancel_rx, start_time)
                    .await
            }

            TaskSpec::Command {
                program,
                args,
                env,
                working_dir,
                stdin,
            } => {
                self.execute_command_workload(
                    task,
                    program,
                    args,
                    env,
                    working_dir.as_ref(),
                    stdin.as_deref(),
                    timeout_duration,
                    child_pid,
                    cancel_rx,
                    start_time,
                )
                .await
            }

            TaskSpec::ShellScript {
                script,
                interpreter,
                env,
            } => {
                self.execute_shell_script_workload(
                    task,
                    script,
                    interpreter.as_deref(),
                    env,
                    timeout_duration,
                    child_pid,
                    cancel_rx,
                    start_time,
                )
                .await
            }

            TaskSpec::RustCompilation {
                crate_name,
                source_files,
                compiler_flags,
                target_dir,
            } => {
                self.execute_rust_compilation_workload(
                    task,
                    crate_name,
                    source_files,
                    compiler_flags,
                    target_dir.as_ref(),
                    timeout_duration,
                    child_pid,
                    cancel_rx,
                    start_time,
                )
                .await
            }
        }
    }

    /// Primary execution dispatcher: validates capabilities, establishes isolation,
    /// runs workload, supervises timeouts, handles cancellation, and emits TaskResult.
    pub async fn execute_task(
        &self,
        task: &Task,
        child_pid: Option<Arc<AtomicU32>>,
        cancel_rx: Option<oneshot::Receiver<String>>,
    ) -> TaskResult {
        self.execute_task_with_checkpoint(task, task.latest_checkpoint.as_ref(), None, child_pid, cancel_rx)
            .await
    }

    /// Executes in-memory BuiltinTest tasks with optional sleep, arithmetic stress, or hash computation,
    /// supporting mid-task checkpoint emission and delta resumption.
    async fn execute_builtin_test(
        &self,
        task: &Task,
        checkpoint: Option<&CheckpointData>,
        checkpoint_tx: Option<mpsc::Sender<WorkerMessage>>,
        timeout: Duration,
        mut cancel_rx: Option<oneshot::Receiver<String>>,
        start_time: Instant,
    ) -> TaskResult {
        let task_id = task.id;
        let (test_name, iterations, duration_ms, should_fail, require_gpu) = match &task.spec {
            TaskSpec::BuiltinTest {
                test_name,
                iterations,
                duration_ms,
                should_fail,
                require_gpu,
            } => (
                test_name.as_str(),
                *iterations,
                *duration_ms,
                *should_fail,
                *require_gpu,
            ),
            _ => ("", 0, 0, false, false),
        };

        if require_gpu && !self.capabilities.can_execute_gpu() {
            return TaskResult::failure(
                self.worker_id,
                task_id,
                EXIT_CODE_GENERAL_ERROR,
                "",
                "Execution Error: BuiltinTest requires GPU, but worker has no GPU capability (Worker lacks GPU capability).",
                0,
                Some("Worker lacks GPU capability".to_string()),
            );
        }

        if should_fail || test_name == "fail" {
            return TaskResult::failure(
                self.worker_id,
                task_id,
                EXIT_CODE_GENERAL_ERROR,
                "",
                "BuiltinTest: intentional failure requested",
                start_time.elapsed().as_millis() as u64,
                Some("Intentional failure".to_string()),
            );
        }

        let compute_fut = async {
            if test_name == "sleep"
                || test_name == "duration"
                || (duration_ms > 0
                    && test_name != "checkpoint"
                    && test_name != "checkpoint_iterative"
                    && test_name != "hash_compute")
            {
                tokio::time::sleep(Duration::from_millis(duration_ms)).await;
                format!("BuiltinTest sleep complete: {duration_ms} ms\n")
            } else if test_name == "arithmetic_stress" || test_name == "arithmetic" {
                let iters = if iterations == 0 { 100_000 } else { iterations };
                let mut acc = 0.0f64;
                for i in 0..iters {
                    let sign = if i % 2 == 0 { 1.0 } else { -1.0 };
                    acc += sign / (2.0 * i as f64 + 1.0);
                }
                let pi_est = acc * 4.0;
                format!("BuiltinTest arithmetic_stress complete: {iters} iterations, pi_estimate = {pi_est:.10}\n")
            } else {
                // Default: deterministic hash computation with checkpoint resumption & emission
                let iters = if iterations == 0 { 100_000 } else { iterations };
                let mut start_iter: u32 = 0;
                let mut state: u64 = 0xcbf29ce484222325;

                // 1. Restore delta state from checkpoint if available
                if let Some(cp) = checkpoint {
                    let slice = cp.delta_state.as_slice();
                    if slice.len() >= 16 {
                        let iter_bytes: [u8; 8] = slice[0..8].try_into().unwrap_or([0; 8]);
                        let state_bytes: [u8; 8] = slice[8..16].try_into().unwrap_or([0; 8]);
                        start_iter = u64::from_le_bytes(iter_bytes) as u32;
                        state = u64::from_le_bytes(state_bytes);
                    } else if let Ok(val) = serde_json::from_slice::<serde_json::Value>(slice) {
                        if let (Some(iter), Some(st)) = (
                            val.get("iteration").and_then(|v| v.as_u64()),
                            val.get("state").and_then(|v| v.as_u64()),
                        ) {
                            start_iter = iter as u32;
                            state = st;
                        }
                    }
                }

                // If not resuming from a checkpoint, initialize state from task_id bytes
                if start_iter == 0 {
                    for b in task_id.as_uuid().as_bytes() {
                        state ^= *b as u64;
                        state = state.wrapping_mul(0x100000001b3);
                    }
                }

                let mut sequence: u64 = checkpoint.map(|c| c.sequence).unwrap_or(0);
                let checkpoint_interval = if iters >= 2 { iters / 2 } else { 1 };

                for i in start_iter..iters {
                    state ^= i as u64;
                    state = state.wrapping_mul(0x100000001b3);
                    state = state.rotate_left(13);

                    // Mid-task checkpoint emission (at halfway point or periodic interval)
                    let current_count = i + 1;
                    if let Some(ref tx) = checkpoint_tx {
                        if current_count < iters
                            && (current_count % checkpoint_interval == 0 || (iters >= 100 && current_count == 50))
                        {
                            sequence += 1;
                            let mut delta = Vec::with_capacity(16);
                            delta.extend_from_slice(&(current_count as u64).to_le_bytes());
                            delta.extend_from_slice(&state.to_le_bytes());

                            let cp_msg = WorkerMessage::Checkpoint {
                                task_id,
                                sequence,
                                delta_state: Bytes::from(delta),
                            };
                            let _ = tx.send(cp_msg).await;

                            if duration_ms > 0 && start_iter == 0 {
                                tokio::time::sleep(Duration::from_millis(duration_ms)).await;
                            }
                        }
                    }
                }

                if start_iter > 0 {
                    format!("BuiltinTest hash_compute complete: {iters} iterations (resumed from {start_iter}), digest = {state:016x}\n")
                } else {
                    format!("BuiltinTest hash_compute complete: {iters} iterations, digest = {state:016x}\n")
                }
            }
        };

        tokio::select! {
            output = compute_fut => {
                let elapsed = start_time.elapsed().as_millis() as u64;
                TaskResult::success(self.worker_id, task_id, output, elapsed, require_gpu)
            }

            cancel_msg = async {
                if let Some(ref mut rx) = cancel_rx {
                    rx.await.ok()
                } else {
                    std::future::pending().await
                }
            } => {
                let elapsed = start_time.elapsed().as_millis() as u64;
                let reason = cancel_msg.unwrap_or_else(|| "Task cancelled".to_string());
                TaskResult::failure(
                    self.worker_id,
                    task_id,
                    EXIT_CODE_CANCELLED,
                    "",
                    "",
                    elapsed,
                    Some(format!("Cancelled: {reason}")),
                )
            }

            _ = tokio::time::sleep(timeout) => {
                let elapsed = start_time.elapsed().as_millis() as u64;
                let msg = format!("Task timed out after {}s", timeout.as_secs());
                TaskResult::failure(
                    self.worker_id,
                    task_id,
                    EXIT_CODE_TIMEOUT,
                    "",
                    msg.clone(),
                    elapsed,
                    Some(msg),
                )
            }
        }
    }

    /// Executes genuine matrix multiplication workload for GPU / simulated GPU workers.
    async fn execute_gpu_compute(
        &self,
        task: &Task,
        timeout: Duration,
        mut cancel_rx: Option<oneshot::Receiver<String>>,
        start_time: Instant,
    ) -> TaskResult {
        let task_id = task.id;
        let (kernel_name, input_data, work_group_size, simulated_matrix_dim) = match &task.spec {
            TaskSpec::GpuCompute {
                kernel_name,
                input_data,
                work_group_size,
                simulated_matrix_dim,
                ..
            } => (
                kernel_name.as_str(),
                input_data.as_ref(),
                *work_group_size,
                *simulated_matrix_dim,
            ),
            _ => ("", [].as_slice(), 16, 64),
        };
        let dim = simulated_matrix_dim.clamp(16, 512) as usize;
        let tile = work_group_size.clamp(4, 32) as usize;

        let compute_fut = async {
            if self.capabilities.is_simulated_gpu {
                let (stdout, elapsed) = cpu_simd::execute_simd_compute(
                    kernel_name,
                    input_data,
                    *task_id.as_uuid(),
                    dim,
                    tile,
                    start_time,
                );
                (
                    stdout,
                    elapsed,
                    "Simulated Virtual GPU (Matrix Engine)".to_string(),
                )
            } else {
                #[cfg(feature = "gpu-wgpu")]
                let wgpu_res = if let Some(engine) = self.get_gpu_engine() {
                    match engine.execute_gemm(
                        kernel_name,
                        input_data,
                        *task_id.as_uuid(),
                        dim,
                        tile,
                        start_time,
                    ) {
                        Ok(outcome) => {
                            Some((outcome.stdout, outcome.elapsed_ms, outcome.device_name))
                        }
                        Err(err) => {
                            warn!("WGPU execution failed, falling back to CPU SIMD: {err}");
                            None
                        }
                    }
                } else {
                    None
                };

                #[cfg(not(feature = "gpu-wgpu"))]
                let wgpu_res: Option<(String, u64, String)> = None;

                match wgpu_res {
                    Some(res) => res,
                    None => {
                        let (stdout, elapsed) = cpu_simd::execute_simd_compute_with_device(
                            kernel_name,
                            input_data,
                            *task_id.as_uuid(),
                            dim,
                            tile,
                            start_time,
                            "CPU SIMD Fallback",
                            "[GPU COMPUTE SIMULATOR]",
                        );
                        (stdout, elapsed, "CPU SIMD Fallback".to_string())
                    }
                }
            }
        };

        tokio::select! {
            (stdout, elapsed, device_name) = compute_fut => {
                TaskResult {
                    worker_id: self.worker_id,
                    task_id,
                    exit_code: EXIT_CODE_SUCCESS,
                    stdout: stdout.into(),
                    stderr: Bytes::new(),
                    execution_time_ms: elapsed,
                    is_gpu_executed: true,
                    device_name: Some(device_name),
                    error: None,
                }
            }

            cancel_msg = async {
                if let Some(ref mut rx) = cancel_rx {
                    rx.await.ok()
                } else {
                    std::future::pending().await
                }
            } => {
                let elapsed = start_time.elapsed().as_millis() as u64;
                let reason = cancel_msg.unwrap_or_else(|| "Task cancelled".to_string());
                TaskResult::failure(
                    self.worker_id,
                    task_id,
                    EXIT_CODE_CANCELLED,
                    "",
                    "",
                    elapsed,
                    Some(format!("Cancelled: {reason}")),
                )
            }

            _ = tokio::time::sleep(timeout) => {
                let elapsed = start_time.elapsed().as_millis() as u64;
                let msg = format!("Task timed out after {}s", timeout.as_secs());
                TaskResult::failure(
                    self.worker_id,
                    task_id,
                    EXIT_CODE_TIMEOUT,
                    "",
                    msg.clone(),
                    elapsed,
                    Some(msg),
                )
            }
        }
    }

    /// Executes `TaskSpec::Command` inside an isolated sandbox directory.
    #[allow(clippy::too_many_arguments)]
    async fn execute_command_workload(
        &self,
        task: &Task,
        program: &str,
        args: &[String],
        env: &HashMap<String, String>,
        working_dir: Option<&PathBuf>,
        stdin: Option<&[u8]>,
        timeout: Duration,
        child_pid: Option<Arc<AtomicU32>>,
        cancel_rx: Option<oneshot::Receiver<String>>,
        start_time: Instant,
    ) -> TaskResult {
        let mut sandbox = match Sandbox::create(&self.config.sandbox_config, task.id).await {
            Ok(s) => s,
            Err(e) => {
                return TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_GENERAL_ERROR,
                    "",
                    format!("Failed to initialize sandbox: {e}"),
                    start_time.elapsed().as_millis() as u64,
                    Some(format!("Sandbox initialization error: {e}")),
                );
            }
        };

        let effective_cwd = if let Some(sub) = working_dir {
            match sanitize_relative_path(sandbox.path(), sub) {
                Ok(p) => {
                    let _ = tokio::fs::create_dir_all(&p).await;
                    p
                }
                Err(e) => {
                    let _ = sandbox.destroy().await;
                    return TaskResult::failure(
                        self.worker_id,
                        task.id,
                        EXIT_CODE_GENERAL_ERROR,
                        "",
                        format!("Illegal working_dir: {e}"),
                        start_time.elapsed().as_millis() as u64,
                        Some(format!("Invalid working directory path: {e}")),
                    );
                }
            }
        } else {
            sandbox.path().to_path_buf()
        };

        let result = self
            .execute_process_supervised(
                task.id,
                sandbox.path(),
                program,
                args,
                &effective_cwd,
                env,
                stdin.map(|s| s.to_vec()),
                timeout,
                child_pid,
                cancel_rx,
                start_time,
            )
            .await;

        let _ = sandbox.destroy().await;
        result
    }

    /// Executes `TaskSpec::ShellScript` by materializing the script into sandbox and executing via interpreter.
    #[allow(clippy::too_many_arguments)]
    async fn execute_shell_script_workload(
        &self,
        task: &Task,
        script: &str,
        interpreter: Option<&str>,
        env: &HashMap<String, String>,
        timeout: Duration,
        child_pid: Option<Arc<AtomicU32>>,
        cancel_rx: Option<oneshot::Receiver<String>>,
        start_time: Instant,
    ) -> TaskResult {
        let mut sandbox = match Sandbox::create(&self.config.sandbox_config, task.id).await {
            Ok(s) => s,
            Err(e) => {
                return TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_GENERAL_ERROR,
                    "",
                    format!("Failed to initialize sandbox: {e}"),
                    start_time.elapsed().as_millis() as u64,
                    Some(format!("Sandbox initialization error: {e}")),
                );
            }
        };

        let script_path = match sandbox.write_script("task_script.sh", script).await {
            Ok(p) => p,
            Err(e) => {
                let _ = sandbox.destroy().await;
                return TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_GENERAL_ERROR,
                    "",
                    format!("Failed to materialize shell script: {e}"),
                    start_time.elapsed().as_millis() as u64,
                    Some(format!("Script write error: {e}")),
                );
            }
        };

        #[cfg(windows)]
        let effective_path = {
            let base = env
                .get("PATH")
                .cloned()
                .unwrap_or_else(|| std::env::var("PATH").unwrap_or_default());
            augment_windows_path(&base)
        };

        #[cfg(windows)]
        let (prog, args) = if let Some(interp) = interpreter {
            (
                interp.to_string(),
                vec![script_path.to_string_lossy().to_string()],
            )
        } else {
            let sh_resolved = resolve_windows_executable("sh", &effective_path);
            if Path::new(&sh_resolved).is_file() {
                (sh_resolved, vec![script_path.to_string_lossy().to_string()])
            } else {
                (
                    "cmd.exe".to_string(),
                    vec!["/C".to_string(), script_path.to_string_lossy().to_string()],
                )
            }
        };

        #[cfg(not(windows))]
        let (prog, args) = if let Some(interp) = interpreter {
            (
                interp.to_string(),
                vec![script_path.to_string_lossy().to_string()],
            )
        } else {
            (
                "/bin/sh".to_string(),
                vec![script_path.to_string_lossy().to_string()],
            )
        };

        let result = self
            .execute_process_supervised(
                task.id,
                sandbox.path(),
                &prog,
                &args,
                sandbox.path(),
                env,
                None,
                timeout,
                child_pid,
                cancel_rx,
                start_time,
            )
            .await;

        let _ = sandbox.destroy().await;
        result
    }

    /// Executes `TaskSpec::RustCompilation` by materializing source files, invoking `rustc`, and verifying artifacts.
    #[allow(clippy::too_many_arguments)]
    async fn execute_rust_compilation_workload(
        &self,
        task: &Task,
        crate_name: &str,
        source_files: &HashMap<String, String>,
        compiler_flags: &[String],
        target_dir: Option<&PathBuf>,
        timeout: Duration,
        child_pid: Option<Arc<AtomicU32>>,
        cancel_rx: Option<oneshot::Receiver<String>>,
        start_time: Instant,
    ) -> TaskResult {
        let mut sandbox = match Sandbox::create(&self.config.sandbox_config, task.id).await {
            Ok(s) => s,
            Err(e) => {
                return TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_GENERAL_ERROR,
                    "",
                    format!("Failed to initialize sandbox: {e}"),
                    start_time.elapsed().as_millis() as u64,
                    Some(format!("Sandbox initialization error: {e}")),
                );
            }
        };

        // Materialize source files into sandbox
        for (rel_path, content) in source_files {
            if let Err(e) = sandbox.write_file(rel_path, content.as_bytes()).await {
                let _ = sandbox.destroy().await;
                return TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_GENERAL_ERROR,
                    "",
                    format!("Failed to write source file '{rel_path}': {e}"),
                    start_time.elapsed().as_millis() as u64,
                    Some(format!("Source materialization error: {e}")),
                );
            }
        }

        // Determine entry point and default crate type
        let entry_candidates = [
            ("src/lib.rs", "lib"),
            ("src/main.rs", "bin"),
            ("lib.rs", "lib"),
            ("main.rs", "bin"),
        ];

        let (entry_rel, default_crate_type) = entry_candidates
            .iter()
            .find(|(path, _)| source_files.contains_key(*path))
            .map(|(path, ctype)| (*path, *ctype))
            .unwrap_or_else(|| {
                // Fallback: look for any .rs file in source_files
                source_files
                    .keys()
                    .find(|k| k.ends_with(".rs"))
                    .map(|k| (k.as_str(), "lib"))
                    .unwrap_or(("src/lib.rs", "lib"))
            });

        let entry_file = sandbox.path().join(entry_rel);

        let out_dir = if let Some(td) = target_dir {
            match sanitize_relative_path(sandbox.path(), td) {
                Ok(p) => p,
                Err(e) => {
                    let _ = sandbox.destroy().await;
                    return TaskResult::failure(
                        self.worker_id,
                        task.id,
                        EXIT_CODE_GENERAL_ERROR,
                        "",
                        format!("Invalid target_dir: {e}"),
                        start_time.elapsed().as_millis() as u64,
                        Some(format!("Target directory path error: {e}")),
                    );
                }
            }
        } else {
            sandbox.path().join("target")
        };

        let _ = tokio::fs::create_dir_all(&out_dir).await;

        let mut args = vec![
            entry_file.to_string_lossy().to_string(),
            "--crate-name".to_string(),
            crate_name.to_string(),
            "--out-dir".to_string(),
            out_dir.to_string_lossy().to_string(),
        ];

        if !compiler_flags.iter().any(|f| f == "--crate-type") {
            args.push("--crate-type".to_string());
            args.push(default_crate_type.to_string());
        }

        for flag in compiler_flags {
            args.push(flag.clone());
        }

        let mut res = self
            .execute_process_supervised(
                task.id,
                sandbox.path(),
                "rustc",
                &args,
                sandbox.path(),
                &HashMap::new(),
                None,
                timeout,
                child_pid,
                cancel_rx,
                start_time,
            )
            .await;

        // On successful compilation, check for generated library / binary artifact
        if res.is_success() {
            let mut found_artifacts = Vec::new();
            if let Ok(mut entries) = tokio::fs::read_dir(&out_dir).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let fname = entry.file_name().to_string_lossy().to_string();
                    if fname.ends_with(".rlib")
                        || fname.ends_with(".dylib")
                        || fname.ends_with(".so")
                        || fname.ends_with(".a")
                        || fname == crate_name
                        || fname == format!("{crate_name}.exe")
                    {
                        found_artifacts.push(fname);
                    }
                }
            }
            if !found_artifacts.is_empty() {
                let notice = format!(
                    "\n[rusty_grid: compilation artifacts generated in target/: {}]\n",
                    found_artifacts.join(", ")
                );
                let mut combined = BytesMut::from(res.stdout.as_bytes());
                combined.extend_from_slice(notice.as_bytes());
                res.stdout = combined.freeze().into();
            }
        }

        let _ = sandbox.destroy().await;
        res
    }

    /// Spawns a child process with deadlock-free bounded I/O, watchdog timeouts, and cooperative cancellation.
    #[allow(clippy::too_many_arguments)]
    async fn execute_process_supervised(
        &self,
        task_id: TaskId,
        sandbox_path: &Path,
        program: &str,
        args: &[String],
        cwd: &Path,
        env: &HashMap<String, String>,
        stdin_data: Option<Vec<u8>>,
        timeout: Duration,
        child_pid: Option<Arc<AtomicU32>>,
        mut cancel_rx: Option<oneshot::Receiver<String>>,
        start_time: Instant,
    ) -> TaskResult {
        let effective_path = match env.get("PATH") {
            Some(p) => {
                #[cfg(windows)]
                {
                    augment_windows_path(p)
                }
                #[cfg(not(windows))]
                {
                    p.clone()
                }
            }
            None => {
                let base = std::env::var("PATH").unwrap_or_default();
                #[cfg(windows)]
                {
                    augment_windows_path(&base)
                }
                #[cfg(not(windows))]
                {
                    base
                }
            }
        };

        #[cfg(windows)]
        let resolved_program = resolve_windows_executable(program, &effective_path);
        #[cfg(not(windows))]
        let resolved_program = program.to_string();

        let mut cmd = tokio::process::Command::new(&resolved_program);
        cmd.args(args);
        cmd.current_dir(cwd);

        cmd.env("PATH", &effective_path);

        // Inject user environment variables (skipping PATH to preserve effective_path)
        for (k, v) in env {
            if k != "PATH" {
                cmd.env(k, v);
            }
        }

        // Inject standard framework diagnostics
        cmd.env("RUSTY_GRID_TASK_ID", task_id.to_string());
        cmd.env("RUSTY_GRID_WORKER_ID", self.worker_id.to_string());
        cmd.env(
            "RUSTY_GRID_SANDBOX_DIR",
            sandbox_path.to_string_lossy().to_string(),
        );

        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        if stdin_data.is_some() {
            cmd.stdin(std::process::Stdio::piped());
        } else {
            cmd.stdin(std::process::Stdio::null());
        }

        // Guarantee process cleanup if future is dropped
        cmd.kill_on_drop(true);

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                return TaskResult::failure(
                    self.worker_id,
                    task_id,
                    EXIT_CODE_GENERAL_ERROR,
                    "",
                    format!("Failed to spawn process '{program}': {e}"),
                    start_time.elapsed().as_millis() as u64,
                    Some(format!("Process spawn error: {e}")),
                );
            }
        };

        if let (Some(ref holder), Some(pid)) = (child_pid, child.id()) {
            holder.store(pid, Ordering::Release);
        }

        let child_stdin = child.stdin.take();
        let child_stdout = child.stdout.take();
        let child_stderr = child.stderr.take();
        let max_output = self.config.max_output_bytes;

        // Concurrent asynchronous stdin writer
        let stdin_fut = async move {
            if let (Some(mut writer), Some(bytes)) = (child_stdin, stdin_data) {
                use tokio::io::AsyncWriteExt;
                let _ = writer.write_all(&bytes).await;
                let _ = writer.flush().await;
                drop(writer); // Drop triggers EOF for the child process
            }
        };

        // Concurrent asynchronous bounded stdout reader
        let stdout_fut = async move {
            if let Some(pipe) = child_stdout {
                read_bounded_stream(pipe, max_output).await
            } else {
                (Bytes::new(), false)
            }
        };

        // Concurrent asynchronous bounded stderr reader
        let stderr_fut = async move {
            if let Some(pipe) = child_stderr {
                read_bounded_stream(pipe, max_output).await
            } else {
                (Bytes::new(), false)
            }
        };

        let execution_fut = async {
            let (_, (stdout_bytes, _), (stderr_bytes, _), status_res) =
                tokio::join!(stdin_fut, stdout_fut, stderr_fut, child.wait());
            (stdout_bytes, stderr_bytes, status_res)
        };

        tokio::select! {
            (stdout_bytes, stderr_bytes, status_res) = execution_fut => {
                let elapsed = start_time.elapsed().as_millis() as u64;
                match status_res {
                    Ok(status) => {
                        let code = match status.code() {
                            Some(c) => c,
                            None => {
                                #[cfg(unix)]
                                {
                                    use std::os::unix::process::ExitStatusExt;
                                    status.signal().map(|s| 128 + s).unwrap_or(EXIT_CODE_GENERAL_ERROR)
                                }
                                #[cfg(not(unix))]
                                {
                                    EXIT_CODE_GENERAL_ERROR
                                }
                            }
                        };
                        if status.success() {
                            let mut res = TaskResult::success(self.worker_id, task_id, stdout_bytes, elapsed, false);
                            res.stderr = stderr_bytes;
                            res
                        } else {
                            TaskResult::failure(
                                self.worker_id,
                                task_id,
                                code,
                                stdout_bytes,
                                stderr_bytes,
                                elapsed,
                                None,
                            )
                        }
                    }
                    Err(e) => {
                        TaskResult::failure(
                            self.worker_id,
                            task_id,
                            EXIT_CODE_GENERAL_ERROR,
                            stdout_bytes,
                            stderr_bytes,
                            elapsed,
                            Some(format!("Process wait error: {e}")),
                        )
                    }
                }
            }

            cancel_msg = async {
                if let Some(ref mut rx) = cancel_rx {
                    rx.await.ok()
                } else {
                    std::future::pending().await
                }
            } => {
                info!(task_id = %task_id, "Aborting child process per cancellation signal");
                if let Some(pid) = child.id() {
                    Self::terminate_process_tree(pid);
                }
                let _ = child.kill().await;
                let _ = child.wait().await;
                let elapsed = start_time.elapsed().as_millis() as u64;
                let reason = cancel_msg.unwrap_or_else(|| "Task cancelled by master".to_string());
                TaskResult::failure(
                    self.worker_id,
                    task_id,
                    EXIT_CODE_CANCELLED,
                    "",
                    "",
                    elapsed,
                    Some(format!("Cancelled: {reason}")),
                )
            }

            _ = tokio::time::sleep(timeout) => {
                warn!(task_id = %task_id, timeout_secs = timeout.as_secs(), "Killing runaway process after watchdog timeout");
                if let Some(pid) = child.id() {
                    Self::terminate_process_tree(pid);
                }
                let _ = child.kill().await;
                let _ = child.wait().await;
                let elapsed = start_time.elapsed().as_millis() as u64;
                let msg = format!("Task timed out after {}s", timeout.as_secs());
                TaskResult::failure(
                    self.worker_id,
                    task_id,
                    EXIT_CODE_TIMEOUT,
                    "",
                    msg.clone(),
                    elapsed,
                    Some(msg),
                )
            }
        }
    }

    /// Cross-platform process tree terminator to eliminate orphaned grandchild processes.
    pub fn terminate_process_tree(pid: u32) {
        #[cfg(target_os = "windows")]
        {
            // taskkill /F /T forcefully terminates the specified process and all its child processes
            let _ = std::process::Command::new("taskkill")
                .args(["/F", "/T", "/PID", &pid.to_string()])
                .output();
        }

        #[cfg(unix)]
        {
            // On Unix, kill the process group (-pgid) and direct child processes via pkill
            let pgid = pid as i32;
            let _ = std::process::Command::new("kill")
                .args(["-9", &format!("-{pgid}")])
                .output();
            let _ = std::process::Command::new("pkill")
                .args(["-9", "-P", &pid.to_string()])
                .output();
        }

        let mut sys = sysinfo::System::new();
        sys.refresh_processes();
        let target_pid = sysinfo::Pid::from(pid as usize);

        // Collect all descendant PIDs
        let mut to_kill = Vec::new();
        let mut queue = vec![target_pid];

        while let Some(parent) = queue.pop() {
            for (&p, proc) in sys.processes() {
                if proc.parent() == Some(parent) {
                    to_kill.push(p);
                    queue.push(p);
                }
            }
        }

        for p in to_kill {
            if let Some(proc) = sys.process(p) {
                let _ = proc.kill();
            }
        }

        if let Some(proc) = sys.process(target_pid) {
            let _ = proc.kill();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::task::TaskRequirements;

    fn test_capabilities(has_gpu: bool) -> WorkerCapabilities {
        WorkerCapabilities {
            name: "test-worker".into(),
            cpu_cores: 4,
            ram_mb: 8192,
            has_gpu,
            is_simulated_gpu: has_gpu,
            gpu_device_name: if has_gpu {
                Some("Test Virtual GPU".into())
            } else {
                None
            },
            tags: vec![],
            mobile: None,
        }
    }

    fn test_runner(has_gpu: bool) -> TaskRunner {
        let temp_dir = std::env::temp_dir().join(format!("runner_test_{}", Uuid::new_v4()));
        let config = RunnerConfig::new(SandboxConfig::new(temp_dir));
        TaskRunner::new(Uuid::new_v4(), test_capabilities(has_gpu), config)
    }

    #[tokio::test]
    async fn test_runner_command_echo_success() {
        let runner = test_runner(false);
        let task = Task::new(
            TaskSpec::command("echo", vec!["hello".into(), "rusty_grid".into()]),
            TaskRequirements::generic(1, 10),
        );

        let res = runner.execute_task(&task, None, None).await;
        assert!(res.is_success());
        assert_eq!(res.exit_code, 0);
        assert!(res.stdout_str().contains("hello rusty_grid"));
        assert!(!res.is_gpu_executed);
    }

    #[tokio::test]
    async fn test_runner_command_exit_code_failure() {
        let runner = test_runner(false);
        let task = Task::new(
            TaskSpec::command("sh", vec!["-c".into(), "exit 42".into()]),
            TaskRequirements::generic(1, 10),
        );

        let res = runner.execute_task(&task, None, None).await;
        assert!(!res.is_success());
        assert_eq!(res.exit_code, 42);
    }

    #[tokio::test]
    async fn test_runner_command_stdin_piping() {
        let runner = test_runner(false);
        let spec = TaskSpec::Command {
            program: "cat".into(),
            args: vec![],
            env: HashMap::new(),
            working_dir: None,
            stdin: Some(b"piped input data\n".to_vec().into()),
        };
        let task = Task::new(spec, TaskRequirements::generic(1, 10));

        let res = runner.execute_task(&task, None, None).await;
        assert!(res.is_success());
        assert_eq!(res.stdout_str(), "piped input data\n");
    }

    #[tokio::test]
    async fn test_runner_command_env_injection() {
        let runner = test_runner(false);
        let mut env = HashMap::new();
        env.insert("GRID_TEST_VAR".into(), "MY_CUSTOM_VAL".into());

        let spec = TaskSpec::Command {
            program: "sh".into(),
            args: vec![
                "-c".into(),
                "echo $GRID_TEST_VAR-$RUSTY_GRID_TASK_ID".into(),
            ],
            env,
            working_dir: None,
            stdin: None,
        };
        let task = Task::new(spec, TaskRequirements::generic(1, 10));

        let res = runner.execute_task(&task, None, None).await;
        assert!(res.is_success());
        assert!(res.stdout_str().contains("MY_CUSTOM_VAL"));
        assert!(res.stdout_str().contains(&task.id.to_string()));
    }

    #[tokio::test]
    async fn test_runner_shell_script_execution() {
        let runner = test_runner(false);
        let script = "A=5\nB=10\necho $((A + B))";
        let task = Task::new(
            TaskSpec::shell_script(script),
            TaskRequirements::generic(1, 10),
        );

        let res = runner.execute_task(&task, None, None).await;
        assert!(res.is_success());
        assert_eq!(res.stdout_str().trim(), "15");
    }

    #[tokio::test]
    async fn test_runner_timeout_watchdog_terminates_runaway() {
        let runner = test_runner(false);
        // Sleep for 60s, timeout in 1s
        let task = Task::new(
            TaskSpec::command("sleep", vec!["60".into()]),
            TaskRequirements::generic(1, 1),
        );

        let res = runner.execute_task(&task, None, None).await;
        assert_eq!(res.exit_code, EXIT_CODE_TIMEOUT);
        assert!(!res.is_success());
        assert!(res.error.as_ref().unwrap().contains("timed out after 1s"));
        assert!(res.stderr_str().contains("Task timed out after 1s"));
    }

    #[tokio::test]
    async fn test_runner_cancellation_signal() {
        let runner = test_runner(false);
        let task = Task::new(
            TaskSpec::command("sleep", vec!["60".into()]),
            TaskRequirements::generic(1, 10),
        );

        let (cancel_tx, cancel_rx) = oneshot::channel();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = cancel_tx.send("user requested cancel".to_string());
        });

        let res = runner.execute_task(&task, None, Some(cancel_rx)).await;
        assert_eq!(res.exit_code, EXIT_CODE_CANCELLED);
        assert_eq!(res.exit_code, 130);
        assert!(!res.is_success());
        assert!(res.error.unwrap().contains("Cancelled"));
    }

    #[tokio::test]
    async fn test_runner_gpu_strict_rejection_on_cpu_worker() {
        let runner = test_runner(false); // Non-GPU worker
        let task = Task::new(
            TaskSpec::gpu_compute("matrix_mult", 64),
            TaskRequirements::gpu(10),
        );

        let res = runner.execute_task(&task, None, None).await;
        assert_eq!(res.exit_code, 1);
        assert!(!res.is_success());
        assert!(!res.is_gpu_executed);
        assert_eq!(res.execution_time_ms, 0);
        assert!(res.error.unwrap().contains("Worker lacks GPU capability"));
    }

    #[tokio::test]
    async fn test_runner_gpu_matrix_compute_on_gpu_worker() {
        let runner = test_runner(true); // GPU worker
        let task = Task::new(
            TaskSpec::gpu_compute("matrix_mult", 32),
            TaskRequirements::gpu(10),
        );

        let res = runner.execute_task(&task, None, None).await;
        assert!(res.is_success());
        assert_eq!(res.exit_code, 0);
        assert!(res.is_gpu_executed);
        assert!(res.execution_time_ms >= 1);
        assert!(res.stdout_str().contains("VERIFIED_OK"));
        assert!(res.stdout_str().contains("Matrix Trace:"));
        assert!(res.stdout_str().contains("Frobenius Norm:"));
    }

    #[tokio::test]
    async fn test_runner_builtin_test_deterministic_hash() {
        let runner = test_runner(false);
        let task1 = Task::new(
            TaskSpec::builtin_test("hash_compute", 50_000),
            TaskRequirements::generic(1, 10),
        );
        let mut task2 = task1.clone();
        task2.id = task1.id; // Same task ID

        let res1 = runner.execute_task(&task1, None, None).await;
        let res2 = runner.execute_task(&task2, None, None).await;

        assert!(res1.is_success());
        assert!(res2.is_success());
        assert_eq!(
            res1.stdout, res2.stdout,
            "Identical seed & iterations must produce identical hash"
        );
    }

    #[tokio::test]
    async fn test_runner_checkpoint_emission_and_resumption() {
        let runner = test_runner(false);
        let task = Task::new(
            TaskSpec::builtin_test("hash_compute", 100),
            TaskRequirements::generic(1, 10),
        );

        // 1. Run full 100 iterations uninterrupted
        let full_res = runner.execute_task(&task, None, None).await;
        assert!(full_res.is_success());
        let full_output = full_res.stdout_str();
        assert!(full_output.contains("100 iterations"));

        let digest_pos = full_output.find("digest = ").expect("digest must exist");
        let full_digest = &full_output[digest_pos..];

        // 2. Run with checkpoint emission channel
        let (cp_tx, mut cp_rx) = mpsc::channel(10);
        let task_clone = task.clone();
        let runner_clone = test_runner(false);
        let run_handle = tokio::spawn(async move {
            runner_clone
                .execute_task_with_checkpoint(&task_clone, None, Some(cp_tx), None, None)
                .await
        });

        // Receive emitted checkpoint at iteration 50
        let received_cp = cp_rx.recv().await.expect("must emit checkpoint");
        let (cp_seq, cp_delta) = match received_cp {
            WorkerMessage::Checkpoint { sequence, delta_state, .. } => (sequence, delta_state),
            other => panic!("Unexpected message: {:?}", other),
        };
        assert_eq!(cp_seq, 1);
        assert_eq!(cp_delta.len(), 16);

        let _ = run_handle.await;

        // 3. Resume another worker from iteration 50 checkpoint
        let cp_data = CheckpointData {
            sequence: cp_seq,
            delta_state: cp_delta,
            timestamp: 12345,
        };
        let resumed_res = runner
            .execute_task_with_checkpoint(&task, Some(&cp_data), None, None, None)
            .await;
        assert!(resumed_res.is_success());
        let resumed_output = resumed_res.stdout_str();
        assert!(resumed_output.contains("resumed from 50"));
        let resumed_digest_pos = resumed_output.find("digest = ").expect("digest must exist");
        let resumed_digest = &resumed_output[resumed_digest_pos..];

        assert_eq!(full_digest, resumed_digest);
    }

    #[tokio::test]
    async fn test_runner_rust_compilation_workflow() {
        let runner = test_runner(false);
        let mut sources = HashMap::new();
        sources.insert(
            "src/lib.rs".into(),
            "pub fn compute_sum(a: i32, b: i32) -> i32 { a + b }".into(),
        );

        let task = Task::new(
            TaskSpec::rust_compilation("test_math_crate", sources, vec![]),
            TaskRequirements::generic(1, 30),
        );

        let res = runner.execute_task(&task, None, None).await;
        assert!(res.is_success());
        assert_eq!(res.exit_code, 0);
        assert!(res.stdout_str().contains("compilation artifacts generated"));
    }
}
