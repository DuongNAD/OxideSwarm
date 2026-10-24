//! Task execution runner, process supervision, watchdog timeouts, and GPU-class compute.
//!
//! # Outcomes and exit codes
//!
//! Every result carries an `exit_code`, and the Master reads two values of it as verdicts
//! (`130` = cancelled), so the runner keeps them apart from what a process can report itself:
//!
//! | outcome | `exit_code` | `error` |
//! |---|---|---|
//! | process exited 0 | `0` | none |
//! | process exited `n` | `n` | none |
//! | process killed by signal `s` (Unix) | `128 + s` | `Process terminated by signal ...` |
//! | process interrupted (`SIGINT`) or exited with `130` | [`EXIT_CODE_INTERRUPTED`] | says so |
//! | cancelled by the Master or by the worker | [`EXIT_CODE_CANCELLED`] (`130`) | `Cancelled: <reason>` |
//! | watchdog timeout | [`EXIT_CODE_TIMEOUT`] (`124`) | `Task timed out after Ns` |
//! | the runner could not start or finish the work | [`EXIT_CODE_GENERAL_ERROR`] (`1`) | the cause |
//!
//! `130` is therefore produced by cancellation and by nothing else: a script that ends with
//! `exit 130` (or dies of Ctrl-C) is a failed task, not a cancelled one.
//!
//! # Process supervision
//!
//! Every task process leads its own process group (Unix) or Job Object (Windows), see
//! [`crate::process_tree`]; cancellation, timeout, normal completion and an aborted task future all
//! take the whole tree down, so grandchildren and background helpers never outlive their task. The
//! runner returns once the direct child has exited and the output pipes have been drained for a
//! bounded time: a background process that inherited stdout cannot turn a finished task into a
//! timeout. Task processes run at a lower CPU priority by default ([`ProcessPriority`]); memory and
//! disk are **not** limited. Secrets in the worker's own environment (`OXIDESWARM_SECRET`,
//! `OXIDESWARM_SECRET_FILE`, `OXIDE_MESH_TOKEN`) are not passed on to task processes.
//!
//! # GPU-class tasks
//!
//! A [`TaskSpec::GpuCompute`] is a seeded `matrix_dim x matrix_dim` single-precision matrix
//! multiplication (`simulated_matrix_dim`, tiled with `work_group_size` x `work_group_size`
//! threads). `kernel_name` is only a label echoed in the report and `compute_intensity` is not
//! read: it is a benchmark workload, not a general kernel launcher, and the input matrices are
//! generated from `input_data` rather than supplied by the caller.
//!
//! * A worker with a **real** GPU (`has_gpu`, which a worker advertises only if its binary
//!   contains the wgpu engine, feature `gpu-wgpu`, and an adapter started) multiplies on the GPU
//!   and checks the product against a CPU recomputation before saying `VERIFIED_OK`. If the GPU
//!   cannot be used the task **fails** with the reason; it is never quietly run on the CPU and
//!   labelled as GPU work.
//! * A worker started with `--simulate-gpu` multiplies on the CPU. The result is labelled
//!   (`[GPU COMPUTE SIMULATOR]`, device `Simulated Virtual GPU (Matrix Engine)`) and
//!   `is_gpu_executed` is true by that test mode's definition.
//! * The parameters are used as given or rejected with a message naming the limit; nothing is
//!   clamped. The multiplication runs on a blocking thread, so the heartbeat, cancellation and the
//!   timeout keep working; a CPU multiplication stops at the next row, a GPU dispatch that was
//!   already submitted **cannot be interrupted** (the runner stops waiting and the GPU finishes it
//!   on its own).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{info, warn};
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::cluster_auth::{ENV_SECRET, ENV_SECRET_FILE};
use rusty_grid_core::protocol::WorkerMessage;
use rusty_grid_core::task::{Bytes, BytesMut, CheckpointData, Task, TaskId, TaskResult, TaskSpec};

use crate::cpu_simd;
use crate::gpu::{check_gemm_request, DeviceLimits, GpuError};
use crate::process_tree::{configure_command, ProcessPriority, ProcessTree};
use crate::sandbox::{sanitize_relative_path, Sandbox, SandboxConfig, SandboxError};

/// Exit code emitted on successful completion.
pub const EXIT_CODE_SUCCESS: i32 = 0;
/// Exit code emitted on general execution failure.
pub const EXIT_CODE_GENERAL_ERROR: i32 = 1;
/// Exit code emitted when a task is cancelled via Master directive. Reserved: the Master treats it
/// as "cancelled", so no process outcome is ever reported with it (see [`EXIT_CODE_INTERRUPTED`]).
pub const EXIT_CODE_CANCELLED: i32 = 130;
/// Standard GNU timeout exit code emitted when the watchdog terminates a process.
pub const EXIT_CODE_TIMEOUT: i32 = 124;
/// What a process that was interrupted (`SIGINT`) or exited with status `130` is reported as.
/// 130 itself is taken by [`EXIT_CODE_CANCELLED`]; this value (130 + 256) lies outside the 8-bit
/// range of Unix exit statuses and cannot collide with `128 + signal` either.
pub const EXIT_CODE_INTERRUPTED: i32 = 130 + 256;

/// Default maximum buffer size for captured stdout and stderr streams (10 MB).
pub const DEFAULT_MAX_OUTPUT_BYTES: usize = 10 * 1024 * 1024;

/// Default time the runner keeps reading the output pipes after the direct child has exited. A
/// background process that inherited the pipes can keep them open indefinitely; once this has
/// elapsed the process tree is killed and the output captured so far is returned.
pub const DEFAULT_PIPE_DRAIN_GRACE: Duration = Duration::from_secs(2);

/// Default time between asking a process tree to stop (`SIGTERM`) and killing it (`SIGKILL`).
pub const DEFAULT_STOP_GRACE: Duration = Duration::from_millis(500);

/// How long to wait for the output pipes to reach EOF once the tree has been killed.
const POST_KILL_DRAIN: Duration = Duration::from_millis(500);

/// How long to wait for the killed leader to be reaped.
const REAP_TIMEOUT: Duration = Duration::from_secs(2);

/// Variables of the worker's own environment that task processes must never inherit: they hold
/// the cluster secret or the agent-mesh token. (A task can still be handed one explicitly through
/// its own `env`.)
pub const SECRET_ENV_VARS: [&str; 3] = [ENV_SECRET, ENV_SECRET_FILE, "OXIDE_MESH_TOKEN"];

/// Configuration options for the TaskRunner.
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    pub sandbox_config: SandboxConfig,
    pub max_output_bytes: usize,
    /// CPU priority of task processes (default: below normal; `RUSTY_GRID_TASK_PRIORITY`
    /// overrides the default).
    pub process_priority: ProcessPriority,
    /// See [`DEFAULT_PIPE_DRAIN_GRACE`].
    pub pipe_drain_grace: Duration,
    /// See [`DEFAULT_STOP_GRACE`].
    pub stop_grace: Duration,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self::new(SandboxConfig::default())
    }
}

impl RunnerConfig {
    pub fn new(sandbox_config: SandboxConfig) -> Self {
        Self {
            sandbox_config,
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
            process_priority: ProcessPriority::from_env().unwrap_or_default(),
            pipe_drain_grace: DEFAULT_PIPE_DRAIN_GRACE,
            stop_grace: DEFAULT_STOP_GRACE,
        }
    }

    pub fn with_max_output_bytes(mut self, bytes: usize) -> Self {
        self.max_output_bytes = bytes;
        self
    }

    pub fn with_process_priority(mut self, priority: ProcessPriority) -> Self {
        self.process_priority = priority;
        self
    }

    pub fn with_pipe_drain_grace(mut self, grace: Duration) -> Self {
        self.pipe_drain_grace = grace;
        self
    }

    pub fn with_stop_grace(mut self, grace: Duration) -> Self {
        self.stop_grace = grace;
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

/// The notice appended to a capture that exceeded its size limit.
const TRUNCATION_NOTICE: &[u8] = b"\n[rusty_grid: output truncated after exceeding size limit]\n";

/// Bytes captured from one output stream so far. Shared with the reader so that what has been
/// read is still there when the reader has to be abandoned (a timeout, or a background process
/// that never closes the pipe).
#[derive(Default)]
struct StreamCapture {
    data: BytesMut,
    truncated: bool,
}

impl StreamCapture {
    fn push(&mut self, chunk: &[u8], max_bytes: usize) {
        if self.data.len() < max_bytes {
            let to_copy = std::cmp::min(max_bytes - self.data.len(), chunk.len());
            self.data.extend_from_slice(&chunk[..to_copy]);
            if chunk.len() > to_copy {
                self.truncated = true;
            }
        } else {
            self.truncated = true;
        }
    }

    fn finish(mut self) -> (Bytes, bool) {
        if self.truncated {
            self.data.extend_from_slice(TRUNCATION_NOTICE);
        }
        (self.data.freeze().into(), self.truncated)
    }
}

type SharedCapture = Arc<Mutex<StreamCapture>>;

fn lock_capture(capture: &SharedCapture) -> std::sync::MutexGuard<'_, StreamCapture> {
    capture
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Reads `reader` to EOF into `capture`, keeping at most `max_bytes` and draining the rest.
async fn read_into_capture<R: tokio::io::AsyncRead + Unpin>(
    mut reader: R,
    max_bytes: usize,
    capture: SharedCapture,
) {
    use tokio::io::AsyncReadExt;

    let mut scratch = [0u8; 8192];
    loop {
        match reader.read(&mut scratch).await {
            Ok(0) => break, // EOF reached
            Ok(n) => lock_capture(&capture).push(&scratch[..n], max_bytes),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        }
    }
}

/// Reads an async stream into a bounded memory buffer without pipe deadlocks.
///
/// Continues draining the pipe to EOF even after the buffer is filled, ensuring
/// the child process does not block on write operations.
pub async fn read_bounded_stream<R: tokio::io::AsyncRead + Unpin>(
    reader: R,
    max_bytes: usize,
) -> (Bytes, bool) {
    let capture = SharedCapture::default();
    read_into_capture(reader, max_bytes, Arc::clone(&capture)).await;
    let captured = std::mem::take(&mut *lock_capture(&capture));
    captured.finish()
}

/// The two output readers of a supervised process, with the buffers they fill.
struct OutputReaders {
    stdout: Option<JoinHandle<()>>,
    stderr: Option<JoinHandle<()>>,
    stdout_capture: SharedCapture,
    stderr_capture: SharedCapture,
}

impl OutputReaders {
    fn spawn(
        stdout: Option<tokio::process::ChildStdout>,
        stderr: Option<tokio::process::ChildStderr>,
        max_bytes: usize,
    ) -> Self {
        let stdout_capture = SharedCapture::default();
        let stderr_capture = SharedCapture::default();
        Self {
            stdout: stdout.map(|pipe| {
                tokio::spawn(read_into_capture(
                    pipe,
                    max_bytes,
                    Arc::clone(&stdout_capture),
                ))
            }),
            stderr: stderr.map(|pipe| {
                tokio::spawn(read_into_capture(
                    pipe,
                    max_bytes,
                    Arc::clone(&stderr_capture),
                ))
            }),
            stdout_capture,
            stderr_capture,
        }
    }

    /// Waits until both pipes reached EOF, for at most `grace`. Returns whether they did.
    async fn wait(&mut self, grace: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + grace;
        for slot in [&mut self.stdout, &mut self.stderr] {
            if let Some(reader) = slot.as_mut() {
                if tokio::time::timeout_at(deadline, reader).await.is_err() {
                    return false;
                }
                *slot = None;
            }
        }
        true
    }

    /// Stops the readers (a reader that is still waiting for EOF is abandoned) and returns what
    /// they captured: `(stdout, stderr)`.
    fn finish(mut self) -> (Bytes, Bytes) {
        self.abort();
        let take = |capture: &SharedCapture| std::mem::take(&mut *lock_capture(capture)).finish().0;
        (take(&self.stdout_capture), take(&self.stderr_capture))
    }

    fn abort(&mut self) {
        for reader in [self.stdout.take(), self.stderr.take()]
            .into_iter()
            .flatten()
        {
            reader.abort();
        }
    }
}

impl Drop for OutputReaders {
    /// A run that is dropped mid-flight must not leave readers behind: a daemon that escaped the
    /// process tree could hold the pipes (and these tasks) open forever.
    fn drop(&mut self) {
        self.abort();
    }
}

/// Resolves with the reason once the task is cancelled (by the Master's `CancelTask` or by the
/// worker itself). It never resolves when there is no cancel channel, or when the sender was
/// dropped without sending: a sender that vanishes is not a cancellation, and treating it as one
/// used to kill healthy tasks and report them as cancelled.
async fn cancellation(slot: &mut Option<oneshot::Receiver<String>>) -> String {
    let Some(receiver) = slot.as_mut() else {
        return std::future::pending().await;
    };
    let outcome = receiver.await;
    // A completed receiver must not be polled again.
    *slot = None;
    match outcome {
        Ok(reason) => reason,
        Err(_) => std::future::pending().await,
    }
}

/// Shell that runs a `ShellScript` without an explicit interpreter on Unix-like systems.
///
/// `/bin/sh` where it exists; otherwise the first `sh` on `path` (Termux keeps its shell in
/// `$PREFIX/bin`, which is on its PATH); otherwise the Android system shell `/system/bin/sh`;
/// otherwise the bare name `sh`, left to the operating system to resolve.
#[cfg(not(windows))]
fn resolve_default_shell(path: &str) -> String {
    pick_shell(&["/bin/sh"], path, &["/system/bin/sh"])
}

/// [`resolve_default_shell`] with the fixed locations as parameters (for tests).
#[cfg(not(windows))]
fn pick_shell(preferred: &[&str], path: &str, fallbacks: &[&str]) -> String {
    let existing = |candidate: &str| Path::new(candidate).is_file();
    if let Some(found) = preferred.iter().find(|candidate| existing(candidate)) {
        return found.to_string();
    }
    if let Some(found) = std::env::split_paths(path)
        .map(|dir| dir.join("sh"))
        .find(|candidate| candidate.is_file())
    {
        return found.to_string_lossy().into_owned();
    }
    if let Some(found) = fallbacks.iter().find(|candidate| existing(candidate)) {
        return found.to_string();
    }
    "sh".to_string()
}

/// Human-readable description of how a process ended, as `(exit_code, error)`; see the table in
/// the module docs. Only called for an unsuccessful status.
fn describe_exit(status: &std::process::ExitStatus) -> (i32, Option<String>) {
    match status.code() {
        Some(130) => (
            EXIT_CODE_INTERRUPTED,
            Some(format!(
                "Process exited with status 130 (interrupted); reported as {EXIT_CODE_INTERRUPTED} because 130 means \"cancelled\""
            )),
        ),
        Some(code) => (code, None),
        None => {
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                if let Some(signal) = status.signal() {
                    let name = signal_name(signal);
                    return if signal == 2 {
                        (
                            EXIT_CODE_INTERRUPTED,
                            Some(format!(
                                "Process terminated by signal 2 (SIGINT); reported as {EXIT_CODE_INTERRUPTED} because 130 means \"cancelled\""
                            )),
                        )
                    } else {
                        (128 + signal, Some(format!("Process terminated by signal {signal}{name}")))
                    };
                }
            }
            (
                EXIT_CODE_GENERAL_ERROR,
                Some("Process ended without an exit status".to_string()),
            )
        }
    }
}

/// ` (SIGKILL)`-style suffix for the common signals, empty for the rest.
#[cfg(unix)]
fn signal_name(signal: i32) -> &'static str {
    match signal {
        1 => " (SIGHUP)",
        2 => " (SIGINT)",
        3 => " (SIGQUIT)",
        6 => " (SIGABRT)",
        9 => " (SIGKILL)",
        11 => " (SIGSEGV)",
        13 => " (SIGPIPE)",
        14 => " (SIGALRM)",
        15 => " (SIGTERM)",
        _ => "",
    }
}

/// What a finished GPU-class job hands back.
struct GpuJobDone {
    stdout: String,
    /// Time spent computing, not counting engine start-up.
    elapsed_ms: u64,
    device_name: String,
}

/// Raises the flag when dropped; the GPU-class job polls it between steps.
struct StopOnDrop(Arc<AtomicBool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// The name the simulated GPU reports as its device.
const SIMULATED_GPU_NAME: &str = "Simulated Virtual GPU (Matrix Engine)";

/// Starts the CPU multiplication of a simulated GPU on a blocking thread; it ends early when
/// `stop` is raised.
fn spawn_simulated_gpu_job(
    kernel_name: String,
    input_data: Vec<u8>,
    task_uuid: Uuid,
    dim: usize,
    tile: usize,
    stop: Arc<AtomicBool>,
) -> JoinHandle<Result<GpuJobDone, GpuError>> {
    tokio::task::spawn_blocking(move || {
        let outcome = cpu_simd::run_simd_compute(
            &cpu_simd::SimdRequest {
                kernel_name: &kernel_name,
                input_data: &input_data,
                task_id: task_uuid,
                dim,
                tile,
                device_name: SIMULATED_GPU_NAME,
                banner: "[GPU COMPUTE SIMULATOR]",
            },
            &|| stop.load(Ordering::Relaxed),
        )?;
        if !outcome.verification.passed() {
            return Err(GpuError::VerificationFailed(outcome.verification.summary()));
        }
        Ok(GpuJobDone {
            stdout: outcome.stdout,
            elapsed_ms: outcome.elapsed_ms,
            device_name: SIMULATED_GPU_NAME.to_string(),
        })
    })
}

/// Core computational workload execution engine for Worker nodes.
pub struct TaskRunner {
    worker_id: Uuid,
    capabilities: WorkerCapabilities,
    config: RunnerConfig,
    /// Where GPU-class tasks get their engine; rebuilds it after a device loss.
    #[cfg(feature = "gpu-wgpu")]
    gpu_engine: Arc<crate::wgpu_engine::EngineProvider>,
}

impl TaskRunner {
    pub fn new(worker_id: Uuid, capabilities: WorkerCapabilities, config: RunnerConfig) -> Self {
        Self {
            worker_id,
            capabilities,
            config,
            #[cfg(feature = "gpu-wgpu")]
            gpu_engine: crate::wgpu_engine::EngineProvider::shared(),
        }
    }

    /// Runs GPU-class tasks on `provider` instead of the process-wide engine (tests that
    /// destroy a device use one of their own).
    #[cfg(feature = "gpu-wgpu")]
    pub fn with_gpu_engine_provider(
        mut self,
        provider: Arc<crate::wgpu_engine::EngineProvider>,
    ) -> Self {
        self.gpu_engine = provider;
        self
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
                    asset_bytes: None,
                };
            }
        }

        // 2. Dispatch in-memory workloads (BuiltinTest, GpuCompute) or process-based workloads
        match &task.spec {
            TaskSpec::BuiltinTest { .. } => {
                self.execute_builtin_test(
                    task,
                    checkpoint,
                    checkpoint_tx,
                    timeout_duration,
                    cancel_rx,
                    start_time,
                )
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

            TaskSpec::Generative3D(spec) => {
                self.execute_generative_3d_workload(
                    task,
                    spec,
                    checkpoint,
                    checkpoint_tx,
                    timeout_duration,
                    child_pid,
                    cancel_rx,
                    start_time,
                )
                .await
            }

            TaskSpec::AutoRig(ref spec) => {
                self.execute_auto_rig_workload(
                    task,
                    spec,
                    checkpoint,
                    checkpoint_tx,
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
        self.execute_task_with_checkpoint(
            task,
            task.latest_checkpoint.as_ref(),
            None,
            child_pid,
            cancel_rx,
        )
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
                            && (current_count % checkpoint_interval == 0
                                || (iters >= 100 && current_count == 50))
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

            reason = cancellation(&mut cancel_rx) => {
                let elapsed = start_time.elapsed().as_millis() as u64;
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

    /// Executes the seeded matrix multiplication of a [`TaskSpec::GpuCompute`] (see the module
    /// documentation, "GPU-class tasks"): on the real GPU, or on the CPU for a simulated GPU.
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
                kernel_name.clone(),
                input_data.clone(),
                *work_group_size,
                *simulated_matrix_dim,
            ),
            _ => (String::new(), Vec::new(), 16, 64),
        };

        let stop = Arc::new(AtomicBool::new(false));
        let job = if self.capabilities.is_simulated_gpu {
            let (dim, tile) = match check_gemm_request(
                simulated_matrix_dim,
                work_group_size,
                &DeviceLimits::simulator(),
            ) {
                Ok(checked) => checked,
                Err(err) => return self.gpu_failure(task_id, &err, timeout, start_time),
            };
            spawn_simulated_gpu_job(
                kernel_name,
                input_data,
                *task_id.as_uuid(),
                dim,
                tile,
                Arc::clone(&stop),
            )
        } else {
            self.spawn_real_gpu_job(
                kernel_name,
                input_data,
                *task_id.as_uuid(),
                simulated_matrix_dim,
                work_group_size,
                Arc::clone(&stop),
                timeout,
            )
        };
        // Whichever way this function ends, the blocking thread is told to stop waiting.
        let _stop_on_exit = StopOnDrop(stop);

        tokio::select! {
            joined = job => match joined {
                Ok(Ok(done)) => TaskResult {
                    worker_id: self.worker_id,
                    task_id,
                    exit_code: EXIT_CODE_SUCCESS,
                    stdout: done.stdout.into(),
                    stderr: Bytes::new(),
                    execution_time_ms: done.elapsed_ms,
                    // True for the real GPU and, by the definition of that test mode, for the
                    // simulator; `device_name` says which one it was.
                    is_gpu_executed: true,
                    device_name: Some(done.device_name),
                    error: None,
                },
                Ok(Err(err)) => self.gpu_failure(task_id, &err, timeout, start_time),
                Err(join_error) => {
                    let what = if join_error.is_panic() {
                        "the GPU compute thread panicked"
                    } else {
                        "the GPU compute thread was dropped before it finished"
                    };
                    warn!(%task_id, "{what}: {join_error}");
                    self.gpu_failure(task_id, &GpuError::Device(what.to_string()), timeout, start_time)
                }
            },

            reason = cancellation(&mut cancel_rx) => {
                let elapsed = start_time.elapsed().as_millis() as u64;
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

    /// The failed [`TaskResult`] for a GPU-class task that ended in `err`. A job that gave up
    /// because its deadline passed is a timeout like any other; everything else is a plain
    /// failure whose message says what was wrong, so the Master can retry it elsewhere.
    fn gpu_failure(
        &self,
        task_id: TaskId,
        err: &GpuError,
        timeout: Duration,
        start_time: Instant,
    ) -> TaskResult {
        let elapsed = start_time.elapsed().as_millis() as u64;
        if let GpuError::Interrupted(_) = err {
            let msg = format!("Task timed out after {}s", timeout.as_secs());
            return TaskResult::failure(
                self.worker_id,
                task_id,
                EXIT_CODE_TIMEOUT,
                "",
                msg.clone(),
                elapsed,
                Some(msg),
            );
        }
        warn!(%task_id, "GPU task failed: {err}");
        let msg = err.to_string();
        TaskResult::failure(
            self.worker_id,
            task_id,
            EXIT_CODE_GENERAL_ERROR,
            "",
            format!("Execution Error: {msg}"),
            elapsed,
            Some(msg),
        )
    }

    /// Starts the real-GPU multiplication on a blocking thread (adapter discovery, shader
    /// compilation and the wait for the GPU all block).
    #[cfg(feature = "gpu-wgpu")]
    #[allow(clippy::too_many_arguments)]
    fn spawn_real_gpu_job(
        &self,
        kernel_name: String,
        input_data: Vec<u8>,
        task_uuid: Uuid,
        dim: u32,
        tile: u32,
        stop: Arc<AtomicBool>,
        timeout: Duration,
    ) -> JoinHandle<Result<GpuJobDone, GpuError>> {
        let provider = Arc::clone(&self.gpu_engine);
        let deadline = Instant::now() + timeout;
        tokio::task::spawn_blocking(move || {
            let lease = provider.get()?;
            let mut control = crate::wgpu_engine::GemmControl {
                cancel: Some(stop),
                deadline: Some(deadline),
                extra_report_lines: Vec::new(),
            };
            if let Some(init_ms) = lease.init_ms {
                control
                    .extra_report_lines
                    .push(format!("Engine Init Time: {init_ms} ms"));
            }
            let outcome = lease.engine.execute_gemm_controlled(
                &kernel_name,
                &input_data,
                task_uuid,
                dim as usize,
                tile as usize,
                &control,
            )?;
            Ok(GpuJobDone {
                stdout: outcome.stdout,
                elapsed_ms: outcome.elapsed_ms,
                device_name: outcome.device_name,
            })
        })
    }

    /// Without the wgpu engine a worker has no real GPU to compute on: the task fails, saying how
    /// to get a binary that has one.
    #[cfg(not(feature = "gpu-wgpu"))]
    #[allow(clippy::too_many_arguments)]
    fn spawn_real_gpu_job(
        &self,
        _kernel_name: String,
        _input_data: Vec<u8>,
        _task_uuid: Uuid,
        _dim: u32,
        _tile: u32,
        _stop: Arc<AtomicBool>,
        _timeout: Duration,
    ) -> JoinHandle<Result<GpuJobDone, GpuError>> {
        tokio::task::spawn_blocking(|| {
            Err(GpuError::Unavailable(format!(
                "this worker binary was built without the `gpu-wgpu` feature, so it cannot run GPU \
                 compute even though the worker advertises a GPU. Rebuild with `{}` (or start the \
                 worker with --simulate-gpu for a labelled CPU simulation)",
                crate::gpu::ENABLE_GPU_HINT
            )))
        })
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
            let path = env
                .get("PATH")
                .cloned()
                .unwrap_or_else(|| std::env::var("PATH").unwrap_or_default());
            (
                resolve_default_shell(&path),
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

    /// Executes a `TaskSpec::Generative3D` workload via Tier 0 (SDF) or Tier 1 (Subprocess).
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_generative_3d_workload(
        &self,
        task: &Task,
        spec: &rusty_grid_core::task::Generative3DSpec,
        _checkpoint: Option<&CheckpointData>,
        checkpoint_tx: Option<mpsc::Sender<WorkerMessage>>,
        timeout: Duration,
        child_pid: Option<Arc<AtomicU32>>,
        mut cancel_rx: Option<oneshot::Receiver<String>>,
        start_time: Instant,
    ) -> TaskResult {
        let pref = spec.engine_preference.as_deref().unwrap_or("auto").to_lowercase();

        // 1. If explicit SDF requested, execute Tier 0 directly in-process (< 50ms)
        if pref == "sdf" {
            info!(task_id = %task.id, "Executing Tier 0 Procedural SDF engine");
            return self.execute_tier0_sdf(task, spec, checkpoint_tx.clone(), start_time).await;
        }

        // 2. Check for Python & Entrypoint availability
        let py_bin = crate::gen3d::runner::resolve_python_runtime();
        let script = crate::gen3d::runner::resolve_entrypoint_script();

        if py_bin.is_none() || script.is_none() {
            if pref == "auto" {
                info!(task_id = %task.id, "Local Python 3D engine not found; degrading gracefully to Tier 0 SDF");
                return self.execute_tier0_sdf(task, spec, checkpoint_tx.clone(), start_time).await;
            } else {
                return TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_GENERAL_ERROR,
                    Bytes::new(),
                    Bytes::from_static(b"Local 3D engine script or Python runtime not found"),
                    start_time.elapsed().as_millis() as u64,
                    Some("Missing Python or engines/generative_3d/entrypoint.py".to_string()),
                );
            }
        }

        let py_bin = py_bin.unwrap();
        let script = script.unwrap();

        // 3. Create isolated sandbox workspace
        let mut sandbox = match Sandbox::create(&self.config.sandbox_config, task.id).await {
            Ok(s) => s,
            Err(e) => {
                return TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_GENERAL_ERROR,
                    Bytes::new(),
                    format!("Failed to create 3D task sandbox: {e}"),
                    start_time.elapsed().as_millis() as u64,
                    Some(format!("Sandbox error: {e}")),
                );
            }
        };

        // 4. Stage input assets into sandbox
        let mut cmd_args = Vec::new();
        cmd_args.push(script.to_string_lossy().to_string());

        if let Some(ref img_bytes) = spec.image_data {
            let input_img_path = sandbox.path().join("input_concept.png");
            if let Err(e) = tokio::fs::write(&input_img_path, img_bytes.as_slice()).await {
                let _ = sandbox.destroy().await;
                return TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_GENERAL_ERROR,
                    Bytes::new(),
                    format!("Failed to write input image to sandbox: {e}"),
                    start_time.elapsed().as_millis() as u64,
                    Some(format!("I/O error: {e}")),
                );
            }
            cmd_args.push("--input".to_string());
            cmd_args.push(input_img_path.to_string_lossy().to_string());
        }

        if let Some(ref prompt) = spec.prompt {
            cmd_args.push("--prompt".to_string());
            cmd_args.push(prompt.clone());
        }

        let output_glb_path = sandbox.path().join("output_model.glb");
        cmd_args.push("--output".to_string());
        cmd_args.push(output_glb_path.to_string_lossy().to_string());

        // Device Selection
        let device = if self.capabilities.has_cuda {
            "cuda:0"
        } else {
            "cpu"
        };
        cmd_args.push("--device".to_string());
        cmd_args.push(device.to_string());

        cmd_args.push("--quality".to_string());
        cmd_args.push(spec.quality.clone());

        if spec.export_texture {
            cmd_args.push("--export-texture".to_string());
        } else {
            cmd_args.push("--no-texture".to_string());
        }

        cmd_args.push("--offline".to_string());

        // 5. Initialize Win32 Job Object for kernel process tree supervision
        let mut job_object = match crate::job_object::JobObject::create() {
            Ok(job) => Some(job),
            Err(e) => {
                warn!("Failed to create Win32 Job Object (falling back to standard supervision): {e}");
                None
            }
        };

        // 6. Build Tokio Command
        let mut cmd = tokio::process::Command::new(&py_bin);
        cmd.args(&cmd_args);
        cmd.current_dir(sandbox.path());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        cmd.stdin(std::process::Stdio::null());
        cmd.kill_on_drop(true);

        #[cfg(unix)]
        {
            cmd.process_group(0);
        }

        cmd.env("HF_HUB_OFFLINE", "1");
        cmd.env("TRANSFORMERS_OFFLINE", "1");
        cmd.env("RUSTY_GRID_TASK_ID", task.id.to_string());
        cmd.env("RUSTY_GRID_WORKER_ID", self.worker_id.to_string());

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                let _ = sandbox.destroy().await;
                if pref == "auto" {
                    info!(task_id = %task.id, "Failed to spawn Python process ({e}); degrading gracefully to Tier 0 SDF");
                    return self.execute_tier0_sdf(task, spec, checkpoint_tx.clone(), start_time).await;
                }
                return TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_GENERAL_ERROR,
                    Bytes::new(),
                    format!("Failed to spawn 3D engine subprocess: {e}"),
                    start_time.elapsed().as_millis() as u64,
                    Some(format!("Spawn error: {e}")),
                );
            }
        };

        if let Some(pid) = child.id() {
            if let Some(ref holder) = child_pid {
                holder.store(pid, Ordering::Release);
            }
            if let Some(ref mut job) = job_object {
                let _ = job.assign_pid(pid);
            }
        }

        // 7. Supervised execution with streaming stdout, stderr, and cancellation
        let child_stdout = child.stdout.take();
        let child_stderr = child.stderr.take();
        let max_output = self.config.max_output_bytes;

        let stdout_fut = async move {
            if let Some(pipe) = child_stdout {
                read_bounded_stream(pipe, max_output).await
            } else {
                (Bytes::new(), false)
            }
        };

        let stderr_fut = async move {
            if let Some(pipe) = child_stderr {
                read_bounded_stream(pipe, max_output).await
            } else {
                (Bytes::new(), false)
            }
        };

        let execution_fut = async {
            let ((stdout_bytes, _), (stderr_bytes, _), status_res) =
                tokio::join!(stdout_fut, stderr_fut, child.wait());
            (stdout_bytes, stderr_bytes, status_res)
        };

        let result = tokio::select! {
            (stdout_bytes, stderr_bytes, status_res) = execution_fut => {
                let elapsed = start_time.elapsed().as_millis() as u64;

                // Process progress checkpoints from stdout if checkpoint_tx provided
                if let Some(ref tx) = checkpoint_tx {
                    let stdout_str = String::from_utf8_lossy(stdout_bytes.as_slice());
                    let mut seq = 1u64;
                    for line in stdout_str.lines() {
                        if let Some((frac, msg)) = crate::gen3d::runner::parse_progress_marker(line) {
                            let delta = serde_json::json!({
                                "progress": frac,
                                "message": msg,
                                "stage": "generative_3d",
                            });
                            let _ = tx.send(WorkerMessage::Checkpoint {
                                task_id: task.id,
                                sequence: seq,
                                delta_state: Bytes::from(delta.to_string().into_bytes()),
                            }).await;
                            seq += 1;
                        }
                    }
                }

                match status_res {
                    Ok(status) if status.success() => {
                        if output_glb_path.is_file() {
                            match tokio::fs::read(&output_glb_path).await {
                                Ok(data) => {
                                    if data.len() >= 12 && &data[0..4] == b"glTF" {
                                        let asset_bytes = Bytes::from(data);
                                        let mut res = TaskResult::success(
                                            self.worker_id,
                                            task.id,
                                            stdout_bytes,
                                            elapsed,
                                            self.capabilities.has_cuda,
                                        );
                                        res.stderr = stderr_bytes;
                                        res = res.with_asset_bytes(asset_bytes);
                                        res = res.with_device_name(if self.capabilities.has_cuda { "TripoSR-CUDA" } else { "TripoSR-CPU" });
                                        res
                                    } else {
                                        TaskResult::failure(
                                            self.worker_id,
                                            task.id,
                                            EXIT_CODE_GENERAL_ERROR,
                                            stdout_bytes,
                                            stderr_bytes,
                                            elapsed,
                                            Some("Generated .glb file has invalid glTF 2.0 header".to_string()),
                                        )
                                    }
                                }
                                Err(e) => {
                                    TaskResult::failure(
                                        self.worker_id,
                                        task.id,
                                        EXIT_CODE_GENERAL_ERROR,
                                        stdout_bytes,
                                        stderr_bytes,
                                        elapsed,
                                        Some(format!("Failed to read generated .glb asset: {e}")),
                                    )
                                }
                            }
                        } else if pref == "auto" {
                            warn!("Subprocess exited 0 but output.glb missing; falling back to Tier 0 SDF");
                            self.execute_tier0_sdf(task, spec, checkpoint_tx.clone(), start_time).await
                        } else {
                            TaskResult::failure(
                                self.worker_id,
                                task.id,
                                EXIT_CODE_GENERAL_ERROR,
                                stdout_bytes,
                                stderr_bytes,
                                elapsed,
                                Some("Subprocess succeeded but output_model.glb was not produced".to_string()),
                            )
                        }
                    }
                    Ok(status) => {
                        let code = status.code().unwrap_or(EXIT_CODE_GENERAL_ERROR);
                        if pref == "auto" {
                            warn!("Tier 1 engine exited with error code {code}; falling back to Tier 0 SDF");
                            self.execute_tier0_sdf(task, spec, checkpoint_tx.clone(), start_time).await
                        } else {
                            TaskResult::failure(
                                self.worker_id,
                                task.id,
                                code,
                                stdout_bytes,
                                stderr_bytes,
                                elapsed,
                                Some(format!("3D engine subprocess failed with exit code {code}")),
                            )
                        }
                    }
                    Err(e) => {
                        TaskResult::failure(
                            self.worker_id,
                            task.id,
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
                info!(task_id = %task.id, "Cancelling 3D engine subprocess");
                if let Some(ref job) = job_object {
                    let _ = job.terminate(EXIT_CODE_CANCELLED as u32);
                } else if let Some(pid) = child.id() {
                    Self::terminate_process_tree(pid);
                }
                let _ = child.kill().await;
                let _ = child.wait().await;
                let elapsed = start_time.elapsed().as_millis() as u64;
                let reason = cancel_msg.unwrap_or_else(|| "Task cancelled by master".to_string());
                TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_CANCELLED,
                    Bytes::new(),
                    Bytes::new(),
                    elapsed,
                    Some(format!("Cancelled: {reason}")),
                )
            }

            _ = tokio::time::sleep(timeout) => {
                warn!(task_id = %task.id, "3D engine subprocess timed out after {}s", timeout.as_secs());
                if let Some(ref job) = job_object {
                    let _ = job.terminate(EXIT_CODE_TIMEOUT as u32);
                } else if let Some(pid) = child.id() {
                    Self::terminate_process_tree(pid);
                }
                let _ = child.kill().await;
                let _ = child.wait().await;
                let elapsed = start_time.elapsed().as_millis() as u64;
                let msg = format!("3D generation timed out after {}s", timeout.as_secs());
                TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_TIMEOUT,
                    Bytes::new(),
                    msg.clone(),
                    elapsed,
                    Some(msg),
                )
            }
        };

        let _ = sandbox.destroy().await;
        result
    }

    /// Direct Tier 0 Procedural Signed Distance Function mesh generator (< 50 ms)
    /// with incremental milestone progress checkpoints (25%, 50%, 75%, 100%).
    async fn execute_tier0_sdf(
        &self,
        task: &Task,
        spec: &rusty_grid_core::task::Generative3DSpec,
        checkpoint_tx: Option<mpsc::Sender<WorkerMessage>>,
        start_time: Instant,
    ) -> TaskResult {
        let task_id = task.id;
        let (prog_tx, mut prog_rx) = tokio::sync::mpsc::unbounded_channel();
        let spec_clone = spec.clone();

        let compute_handle = tokio::task::spawn_blocking(move || {
            crate::gen3d::generate_procedural_3d_with_progress(&spec_clone, |frac, stage, msg| {
                let _ = prog_tx.send((frac, stage, msg));
            })
        });

        let mut seq = 1u64;
        while let Some((frac, stage, msg)) = prog_rx.recv().await {
            if let Some(ref tx) = checkpoint_tx {
                let delta = serde_json::json!({
                    "progress": frac,
                    "stage": stage,
                    "message": msg,
                });
                let _ = tx
                    .send(WorkerMessage::Checkpoint {
                        task_id,
                        sequence: seq,
                        delta_state: Bytes::from(delta.to_string().into_bytes()),
                    })
                    .await;
                seq += 1;
            }
        }

        match compute_handle.await {
            Ok(Ok((glb_bytes, stats))) => {
                let elapsed = start_time.elapsed().as_millis() as u64;
                let mut res = TaskResult::success(
                    self.worker_id,
                    task_id,
                    Bytes::from(format!(
                        "Tier 0 procedural SDF mesh generated: {} vertices, {} triangles in {} ms\n",
                        stats.vertex_count, stats.triangle_count, stats.execution_time_ms
                    )),
                    elapsed,
                    false,
                );
                res = res.with_asset_bytes(Bytes::from(glb_bytes));
                res = res.with_device_name("Procedural-SDF-PureRust");
                res
            }
            Ok(Err(e)) => {
                let elapsed = start_time.elapsed().as_millis() as u64;
                TaskResult::failure(
                    self.worker_id,
                    task_id,
                    EXIT_CODE_GENERAL_ERROR,
                    Bytes::new(),
                    format!("Procedural SDF generation error: {e}"),
                    elapsed,
                    Some(format!("SDF error: {e}")),
                )
            }
            Err(e) => {
                let elapsed = start_time.elapsed().as_millis() as u64;
                TaskResult::failure(
                    self.worker_id,
                    task_id,
                    EXIT_CODE_GENERAL_ERROR,
                    Bytes::new(),
                    format!("Compute join error: {e}"),
                    elapsed,
                    Some(format!("Join error: {e}")),
                )
            }
        }
    }

    /// Executes an automated skeleton rigging and animation synthesis workload (`TaskSpec::AutoRig`)
    /// with incremental milestone progress checkpoints (25%, 50%, 75%, 100%).
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_auto_rig_workload(
        &self,
        task: &Task,
        spec: &rusty_grid_core::task::AutoRigSpec,
        _checkpoint: Option<&CheckpointData>,
        checkpoint_tx: Option<mpsc::Sender<WorkerMessage>>,
        timeout: Duration,
        _child_pid: Option<Arc<AtomicU32>>,
        mut cancel_rx: Option<oneshot::Receiver<String>>,
        start_time: Instant,
    ) -> TaskResult {
        let task_id = task.id;
        let pref = spec.preferred_provider.as_deref().unwrap_or("builtin").to_lowercase();
        let (prog_tx, mut prog_rx) = tokio::sync::mpsc::unbounded_channel();
        let spec_clone = spec.clone();
        let tx_clone = checkpoint_tx.clone();
        let tx_forward = tx_clone.clone();

        // Background forwarder for progress checkpoints
        let forward_handle = tokio::spawn(async move {
            let mut seq = 1u64;
            while let Some((frac, stage, msg)) = prog_rx.recv().await {
                if let Some(ref tx) = tx_forward {
                    let delta = serde_json::json!({
                        "progress": frac,
                        "stage": stage,
                        "message": msg,
                    });
                    let _ = tx
                        .send(WorkerMessage::Checkpoint {
                            task_id,
                            sequence: seq,
                            delta_state: Bytes::from(delta.to_string().into_bytes()),
                        })
                        .await;
                    seq += 1;
                }
            }
        });

        let compute_fut = async {
            if pref == "blender" {
                if let Some(blender_bin) = crate::autorig::blender_provider::resolve_blender_binary() {
                    if let Some(ref tx) = tx_clone {
                        let _ = tx
                            .send(WorkerMessage::Checkpoint {
                                task_id,
                                sequence: 1,
                                delta_state: Bytes::from_static(b"analyzing_mesh"),
                            })
                            .await;
                    }
                    match crate::autorig::BlenderRigProvider::execute_rigging(
                        &blender_bin,
                        spec_clone.input_mesh.as_bytes(),
                        &spec_clone.rig_preset,
                        &spec_clone.embed_animations,
                        60,
                    ).await {
                        Ok(glb) => {
                            if let Some(ref tx) = tx_clone {
                                let _ = tx
                                    .send(WorkerMessage::Checkpoint {
                                        task_id,
                                        sequence: 2,
                                        delta_state: Bytes::from_static(b"completed"),
                                    })
                                    .await;
                            }
                            return Ok(glb);
                        }
                        Err(e) => {
                            warn!("Blender auto-rigging failed ({e}); falling back to pure-Rust engine");
                        }
                    }
                } else {
                    warn!("Blender executable not found; falling back to pure-Rust engine");
                }
            }

            tokio::task::spawn_blocking(move || {
                crate::autorig::auto_rig_mesh_with_progress(&spec_clone, |frac, stage, msg| {
                    let _ = prog_tx.send((frac, stage, msg));
                })
            })
            .await
            .map_err(|e| crate::autorig::WorkerError::AutoRig(format!("Join error: {e}")))?
        };

        tokio::select! {
            result = tokio::time::timeout(timeout, compute_fut) => {
                let _ = forward_handle.await;
                match result {
                    Ok(Ok(glb_bytes)) => {
                        let elapsed = start_time.elapsed().as_millis() as u64;
                        let mut res = TaskResult::success(
                            self.worker_id,
                            task.id,
                            Bytes::from(format!(
                                "Auto-rigging completed successfully: {} bytes .glb generated in {} ms\n",
                                glb_bytes.len(),
                                elapsed
                            )),
                            elapsed,
                            false,
                        );
                        res = res.with_asset_bytes(Bytes::from(glb_bytes));
                        res = res.with_device_name("OxideSwarm-AutoRig-Engine");
                        res
                    }
                    Ok(Err(e)) => {
                        let elapsed = start_time.elapsed().as_millis() as u64;
                        TaskResult::failure(
                            self.worker_id,
                            task.id,
                            EXIT_CODE_GENERAL_ERROR,
                            Bytes::new(),
                            Bytes::from(format!("Auto-rigging execution error: {e}")),
                            elapsed,
                            Some(format!("Auto-rig error: {e}")),
                        )
                    }
                    Err(_) => {
                        let elapsed = start_time.elapsed().as_millis() as u64;
                        TaskResult::failure(
                            self.worker_id,
                            task.id,
                            EXIT_CODE_TIMEOUT,
                            Bytes::new(),
                            Bytes::from(format!("Auto-rigging task timed out after {}s", timeout.as_secs())),
                            elapsed,
                            Some("Task timed out".to_string()),
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
                forward_handle.abort();
                let elapsed = start_time.elapsed().as_millis() as u64;
                let reason = cancel_msg.unwrap_or_else(|| "Task cancelled by master".to_string());
                TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_CANCELLED,
                    Bytes::new(),
                    Bytes::new(),
                    elapsed,
                    Some(format!("Cancelled: {reason}")),
                )
            }
        }
    }

    /// Spawns a child process with deadlock-free bounded I/O, watchdog timeouts, and cooperative cancellation.
    ///
    /// The child leads its own process tree (see [`crate::process_tree`]). Whatever ends the run
    /// (exit, cancellation, timeout, or this future being dropped) the whole tree is killed, so no
    /// grandchild outlives the task. The run is over when the direct child has exited and its
    /// output has been drained for at most `pipe_drain_grace`: a background process that kept the
    /// output pipes open cannot turn a finished task into a timeout.
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

        // The worker's own secrets are none of the task's business. Removed before the task's own
        // environment is applied, so a task that is handed one explicitly still gets it.
        for name in SECRET_ENV_VARS {
            cmd.env_remove(name);
        }

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

        // Last line of defence for the direct child if this future is dropped; the process tree
        // below takes care of everything else.
        cmd.kill_on_drop(true);
        configure_command(&mut cmd, self.config.process_priority);

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

        // Declared before the readers so it is dropped after them: dropping it kills what is left.
        let tree = ProcessTree::attach(&child);
        if !tree.is_tracked() {
            warn!(task_id = %task_id, "Could not place the task process in its own process group or job; only the direct child can be terminated reliably");
        }

        if let (Some(ref holder), Some(pid)) = (child_pid, child.id()) {
            holder.store(pid, Ordering::Release);
        }

        // Concurrent asynchronous stdin writer
        let _stdin_writer = match (child.stdin.take(), stdin_data) {
            (Some(mut writer), Some(bytes)) => Some(AbortOnDrop(tokio::spawn(async move {
                use tokio::io::AsyncWriteExt;
                let _ = writer.write_all(&bytes).await;
                let _ = writer.flush().await;
                drop(writer); // Drop triggers EOF for the child process
            }))),
            _ => None,
        };

        // Concurrent asynchronous bounded stdout / stderr readers
        let mut readers = OutputReaders::spawn(
            child.stdout.take(),
            child.stderr.take(),
            self.config.max_output_bytes,
        );

        enum Ended {
            Exited(std::io::Result<std::process::ExitStatus>),
            Cancelled(String),
            TimedOut,
        }

        let ended = tokio::select! {
            status = child.wait() => Ended::Exited(status),
            reason = cancellation(&mut cancel_rx) => Ended::Cancelled(reason),
            _ = tokio::time::sleep(timeout) => Ended::TimedOut,
        };

        match ended {
            Ended::Exited(status_res) => {
                // The direct child is gone and its output normally follows within moments. A
                // background process that inherited the pipes can hold them open for as long as it
                // lives, so wait a bounded time, then take the tree down and keep what arrived.
                let drained = readers.wait(self.config.pipe_drain_grace).await;
                // Nothing the task started may outlive it: its sandbox is deleted next.
                tree.kill(None);
                if !drained {
                    readers.wait(POST_KILL_DRAIN).await;
                }
                let (stdout_bytes, mut stderr_bytes) = readers.finish();
                if !drained {
                    warn!(task_id = %task_id, "Background processes kept the output pipes open after the task exited; terminated them");
                    stderr_bytes = append_note(
                        stderr_bytes,
                        "\n[rusty_grid: background processes kept the output pipes open after the task exited and were terminated]\n",
                    );
                }

                let elapsed = start_time.elapsed().as_millis() as u64;
                match status_res {
                    Ok(status) if status.success() => {
                        let mut res = TaskResult::success(
                            self.worker_id,
                            task_id,
                            stdout_bytes,
                            elapsed,
                            false,
                        );
                        res.stderr = stderr_bytes;
                        res
                    }
                    Ok(status) => {
                        let (code, error) = describe_exit(&status);
                        TaskResult::failure(
                            self.worker_id,
                            task_id,
                            code,
                            stdout_bytes,
                            stderr_bytes,
                            elapsed,
                            error,
                        )
                    }
                    Err(e) => TaskResult::failure(
                        self.worker_id,
                        task_id,
                        EXIT_CODE_GENERAL_ERROR,
                        stdout_bytes,
                        stderr_bytes,
                        elapsed,
                        Some(format!("Process wait error: {e}")),
                    ),
                }
            }

            Ended::Cancelled(reason) => {
                info!(task_id = %task_id, "Aborting child process per cancellation signal");
                self.stop_tree(&mut child, &tree).await;
                readers.wait(POST_KILL_DRAIN).await;
                let (stdout_bytes, stderr_bytes) = readers.finish();
                TaskResult::failure(
                    self.worker_id,
                    task_id,
                    EXIT_CODE_CANCELLED,
                    stdout_bytes,
                    stderr_bytes,
                    start_time.elapsed().as_millis() as u64,
                    Some(format!("Cancelled: {reason}")),
                )
            }

            Ended::TimedOut => {
                warn!(task_id = %task_id, timeout_secs = timeout.as_secs(), "Killing runaway process after watchdog timeout");
                self.stop_tree(&mut child, &tree).await;
                readers.wait(POST_KILL_DRAIN).await;
                let (stdout_bytes, stderr_bytes) = readers.finish();
                let msg = format!("Task timed out after {}s", timeout.as_secs());
                let stderr_bytes = append_note(stderr_bytes, &format!("\n{msg}"));
                TaskResult::failure(
                    self.worker_id,
                    task_id,
                    EXIT_CODE_TIMEOUT,
                    stdout_bytes,
                    stderr_bytes,
                    start_time.elapsed().as_millis() as u64,
                    Some(msg),
                )
            }
        }
    }

    /// Ends a task's process tree: a polite stop (`SIGTERM`; Windows has none), a short grace
    /// period for the leader to wind down, then a kill of everything that is left, and the leader
    /// is reaped so that no zombie remains. Never blocks a runtime thread.
    async fn stop_tree(&self, child: &mut tokio::process::Child, tree: &ProcessTree) {
        if tree.request_stop() {
            let _ = tokio::time::timeout(self.config.stop_grace, child.wait()).await;
        }
        tree.kill(child.id());
        // The leader at least, even where no tree could be tracked.
        let _ = child.start_kill();
        let _ = tokio::time::timeout(REAP_TIMEOUT, child.wait()).await;
    }
}

/// Aborts the wrapped task when dropped.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// `bytes` followed by `note`; a newline first when `bytes` is non-empty and does not end in one
/// and `note` does not start with one.
fn append_note(bytes: Bytes, note: &str) -> Bytes {
    let mut combined = BytesMut::from(bytes.as_bytes());
    if !combined.is_empty() && !combined.ends_with(b"\n") && !note.starts_with('\n') {
        combined.extend_from_slice(b"\n");
    }
    combined.extend_from_slice(note.as_bytes());
    combined.freeze().into()
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
            ..Default::default()
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
            stdin: Some(b"piped input data\n".to_vec()),
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
            WorkerMessage::Checkpoint {
                sequence,
                delta_state,
                ..
            } => (sequence, delta_state),
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

    #[tokio::test]
    async fn test_runner_generative_3d_tier0_checkpoints() {
        let spec = rusty_grid_core::task::Generative3DSpec::new_prompt("procedural drone".to_string())
            .with_engine("sdf")
            .with_quality("fast");
        let task = Task::new(TaskSpec::Generative3D(spec), TaskRequirements::generic(1, 30));

        let (cp_tx, mut cp_rx) = mpsc::channel(10);
        let task_clone = task.clone();
        let runner_clone = test_runner(false);

        let run_handle = tokio::spawn(async move {
            runner_clone
                .execute_task_with_checkpoint(&task_clone, None, Some(cp_tx), None, None)
                .await
        });

        // Collect emitted checkpoints
        let mut checkpoints = Vec::new();
        while let Some(msg) = cp_rx.recv().await {
            if let WorkerMessage::Checkpoint { sequence, delta_state, .. } = msg {
                checkpoints.push((sequence, delta_state));
            }
        }

        let res = run_handle.await.expect("task must join");
        assert!(res.is_success());
        assert_eq!(res.exit_code, 0);

        // Verify sequential checkpoints 1..4
        assert_eq!(checkpoints.len(), 4);
        assert_eq!(checkpoints[0].0, 1);
        assert_eq!(checkpoints[1].0, 2);
        assert_eq!(checkpoints[2].0, 3);
        assert_eq!(checkpoints[3].0, 4);

        let delta1: serde_json::Value = serde_json::from_slice(checkpoints[0].1.as_slice()).unwrap();
        assert_eq!(delta1["stage"], "sdf_geometry");
        assert_eq!(delta1["progress"], 0.25);

        let delta4: serde_json::Value = serde_json::from_slice(checkpoints[3].1.as_slice()).unwrap();
        assert_eq!(delta4["stage"], "gltf_pack");
        assert_eq!(delta4["progress"], 1.00);

        // Verify asset bytes
        assert!(res.asset_bytes.is_some());
        let asset = res.asset_bytes.unwrap();
        assert!(asset.len() >= 20);
        assert_eq!(&asset.as_slice()[0..4], b"glTF");
    }

    #[tokio::test]
    async fn test_runner_autorig_checkpoints() {
        // Generate a small mesh first
        let gen_spec = rusty_grid_core::task::Generative3DSpec::new_prompt("cylinder".to_string())
            .with_engine("sdf")
            .with_quality("fast");
        let (mesh_bytes, _) = crate::gen3d::generate_procedural_3d(&gen_spec).expect("mesh gen failed");

        let rig_spec = rusty_grid_core::task::AutoRigSpec::new(mesh_bytes, "biped")
            .with_animations(vec!["idle".to_string()]);
        let task = Task::new(TaskSpec::AutoRig(rig_spec), TaskRequirements::generic(1, 30));

        let (cp_tx, mut cp_rx) = mpsc::channel(10);
        let task_clone = task.clone();
        let runner_clone = test_runner(false);

        let run_handle = tokio::spawn(async move {
            runner_clone
                .execute_task_with_checkpoint(&task_clone, None, Some(cp_tx), None, None)
                .await
        });

        // Collect emitted checkpoints
        let mut checkpoints = Vec::new();
        while let Some(msg) = cp_rx.recv().await {
            if let WorkerMessage::Checkpoint { sequence, delta_state, .. } = msg {
                checkpoints.push((sequence, delta_state));
            }
        }

        let res = run_handle.await.expect("task must join");
        assert!(res.is_success());
        assert_eq!(res.exit_code, 0);

        // Verify sequential checkpoints 1..4
        assert_eq!(checkpoints.len(), 4);
        assert_eq!(checkpoints[0].0, 1);
        assert_eq!(checkpoints[1].0, 2);
        assert_eq!(checkpoints[2].0, 3);
        assert_eq!(checkpoints[3].0, 4);

        let delta1: serde_json::Value = serde_json::from_slice(checkpoints[0].1.as_slice()).unwrap();
        assert_eq!(delta1["stage"], "landmark_extraction");
        assert_eq!(delta1["progress"], 0.25);

        let delta4: serde_json::Value = serde_json::from_slice(checkpoints[3].1.as_slice()).unwrap();
        assert_eq!(delta4["stage"], "animation_export");
        assert_eq!(delta4["progress"], 1.00);

        // Verify asset bytes
        assert!(res.asset_bytes.is_some());
        let asset = res.asset_bytes.unwrap();
        assert!(asset.len() >= 20);
        assert_eq!(&asset.as_slice()[0..4], b"glTF");
    }
}

/// Tests of exit-status mapping, the cancellation channel and the default shell; no processes of
/// their own beyond `sh`, so they run everywhere `sh` does.
#[cfg(test)]
mod outcome_tests {
    use super::*;
    use rusty_grid_core::task::TaskRequirements;

    fn runner_in(dir: &Path) -> TaskRunner {
        let caps = WorkerCapabilities {
            name: "outcome-test-worker".into(),
            cpu_cores: 4,
            ram_mb: 8192,
            has_gpu: false,
            is_simulated_gpu: false,
            gpu_device_name: None,
            tags: vec![],
            mobile: None,
        };
        TaskRunner::new(
            Uuid::new_v4(),
            caps,
            RunnerConfig::new(SandboxConfig::new(dir)),
        )
    }

    #[cfg(unix)]
    #[test]
    fn test_describe_exit_keeps_130_for_cancellation_only() {
        use std::os::unix::process::ExitStatusExt;
        let exited = |code: i32| std::process::ExitStatus::from_raw(code << 8);
        let signalled = |sig: i32| std::process::ExitStatus::from_raw(sig);

        // Ordinary exit codes pass through untouched, with no error text.
        assert_eq!(describe_exit(&exited(1)), (1, None));
        assert_eq!(describe_exit(&exited(42)), (42, None));
        assert_eq!(describe_exit(&exited(124)), (124, None));

        // A script that ends with `exit 130` is a failure, never the Master's "cancelled" verdict.
        let (code, error) = describe_exit(&exited(130));
        assert_eq!(code, EXIT_CODE_INTERRUPTED);
        assert_ne!(code, EXIT_CODE_CANCELLED);
        assert!(error.unwrap().contains("130"));

        // SIGINT collides with it in the shell's 128+signal convention: distinct as well.
        let (code, error) = describe_exit(&signalled(2));
        assert_eq!(code, EXIT_CODE_INTERRUPTED);
        assert!(error.unwrap().contains("SIGINT"));

        // Every other signal keeps the conventional 128+signal and says which one it was.
        let (code, error) = describe_exit(&signalled(9));
        assert_eq!(code, 137);
        assert!(error.unwrap().contains("signal 9 (SIGKILL)"));
        let (code, error) = describe_exit(&signalled(15));
        assert_eq!(code, 143);
        assert!(error.unwrap().contains("SIGTERM"));
        let (code, error) = describe_exit(&signalled(31));
        assert_eq!(code, 128 + 31);
        assert!(error.unwrap().contains("signal 31"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_process_exiting_130_is_a_failure_not_a_cancellation() {
        let dir = tempfile::tempdir().unwrap();
        let runner = runner_in(dir.path());
        let task = Task::new(
            TaskSpec::command("sh", vec!["-c".into(), "exit 130".into()]),
            TaskRequirements::generic(1, 10),
        );

        let res = runner.execute_task(&task, None, None).await;
        assert!(!res.is_success());
        assert_ne!(
            res.exit_code, EXIT_CODE_CANCELLED,
            "130 is reserved for cancellation"
        );
        assert_eq!(res.exit_code, EXIT_CODE_INTERRUPTED);
        assert!(!res.error.as_deref().unwrap_or("").starts_with("Cancelled"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_process_killed_by_a_signal_reports_it() {
        let dir = tempfile::tempdir().unwrap();
        let runner = runner_in(dir.path());
        let task = Task::new(
            TaskSpec::command("sh", vec!["-c".into(), "kill -TERM $$".into()]),
            TaskRequirements::generic(1, 10),
        );

        let res = runner.execute_task(&task, None, None).await;
        assert_eq!(res.exit_code, 128 + 15);
        let error = res.error.expect("a signalled process must say so");
        assert!(
            error.contains("signal 15") && error.contains("SIGTERM"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn test_dropped_cancel_sender_is_not_a_cancellation() {
        let dir = tempfile::tempdir().unwrap();
        let runner = runner_in(dir.path());

        // A process task...
        #[cfg(unix)]
        {
            let (tx, rx) = oneshot::channel::<String>();
            drop(tx);
            let task = Task::new(
                TaskSpec::command("sh", vec!["-c".into(), "sleep 0.3; echo survived".into()]),
                TaskRequirements::generic(1, 10),
            );
            let res = runner.execute_task(&task, None, Some(rx)).await;
            assert!(
                res.is_success(),
                "a vanished cancel sender must not kill the task: {res:?}"
            );
            assert_eq!(res.stdout_str().trim(), "survived");
        }

        // ...and an in-memory one.
        let (tx, rx) = oneshot::channel::<String>();
        drop(tx);
        let task = Task::new(
            TaskSpec::BuiltinTest {
                test_name: "sleep".into(),
                iterations: 0,
                duration_ms: 200,
                should_fail: false,
                require_gpu: false,
            },
            TaskRequirements::generic(1, 10),
        );
        let res = runner.execute_task(&task, None, Some(rx)).await;
        assert!(res.is_success(), "{res:?}");
    }

    #[tokio::test]
    async fn test_cancellation_helper_ignores_a_closed_channel_but_delivers_a_reason() {
        let mut none: Option<oneshot::Receiver<String>> = None;
        assert!(
            tokio::time::timeout(Duration::from_millis(50), cancellation(&mut none))
                .await
                .is_err()
        );

        let (tx, rx) = oneshot::channel::<String>();
        drop(tx);
        let mut closed = Some(rx);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), cancellation(&mut closed))
                .await
                .is_err()
        );

        let (tx, rx) = oneshot::channel::<String>();
        tx.send("because".into()).unwrap();
        let mut sent = Some(rx);
        assert_eq!(cancellation(&mut sent).await, "because");
    }

    #[cfg(unix)]
    #[test]
    fn test_default_shell_lookup_order() {
        let dir = tempfile::tempdir().unwrap();
        let path_dir = dir.path().join("bin");
        std::fs::create_dir_all(&path_dir).unwrap();
        let path_sh = path_dir.join("sh");
        std::fs::write(&path_sh, "#!/bin/sh\n").unwrap();
        let preferred = dir.path().join("preferred_sh");
        let fallback = dir.path().join("fallback_sh");
        std::fs::write(&preferred, "").unwrap();
        std::fs::write(&fallback, "").unwrap();
        let (preferred, fallback) = (preferred.to_str().unwrap(), fallback.to_str().unwrap());
        let path = path_dir.to_str().unwrap();
        let missing = dir.path().join("does_not_exist");
        let missing = missing.to_str().unwrap();

        // /bin/sh (the preferred location) wins where it exists...
        assert_eq!(pick_shell(&[preferred], path, &[fallback]), preferred);
        // ...Termux-style: no /bin/sh, but an `sh` on PATH...
        assert_eq!(
            pick_shell(&[missing], path, &[fallback]),
            path_sh.to_str().unwrap()
        );
        // ...Android: no /bin/sh and none on PATH, the system shell...
        assert_eq!(pick_shell(&[missing], "", &[fallback]), fallback);
        assert_eq!(pick_shell(&[missing], missing, &[fallback]), fallback);
        // ...and the bare name as the last resort, never a hard-coded path that does not exist.
        assert_eq!(pick_shell(&[missing], "", &[missing]), "sh");
    }

    #[cfg(unix)]
    #[test]
    fn test_default_shell_exists_on_this_host() {
        let shell = resolve_default_shell(&std::env::var("PATH").unwrap_or_default());
        assert!(
            Path::new(&shell).is_file(),
            "resolved shell {shell:?} must exist"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_shell_script_without_interpreter_runs_via_the_resolved_shell() {
        let dir = tempfile::tempdir().unwrap();
        let runner = runner_in(dir.path());
        let task = Task::new(
            TaskSpec::shell_script("echo via-default-shell"),
            TaskRequirements::generic(1, 10),
        );
        let res = runner.execute_task(&task, None, None).await;
        assert!(res.is_success(), "{res:?}");
        assert_eq!(res.stdout_str().trim(), "via-default-shell");
    }
}

/// Real process trees: every test here spawns grandchildren (`sh -c` with a background `sleep`)
/// and checks that they are gone, and that nothing unrelated is touched.
#[cfg(all(test, unix))]
mod process_tree_tests {
    use super::*;
    use rusty_grid_core::task::TaskRequirements;

    fn runner_with(
        configure: impl FnOnce(RunnerConfig) -> RunnerConfig,
    ) -> (Arc<TaskRunner>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let caps = WorkerCapabilities {
            name: "tree-test-worker".into(),
            cpu_cores: 4,
            ram_mb: 8192,
            has_gpu: false,
            is_simulated_gpu: false,
            gpu_device_name: None,
            tags: vec![],
            mobile: None,
        };
        let config = configure(RunnerConfig::new(SandboxConfig::new(dir.path())));
        (Arc::new(TaskRunner::new(Uuid::new_v4(), caps, config)), dir)
    }

    fn script_task(script: String, timeout_secs: u64) -> Task {
        Task::new(
            TaskSpec::shell_script(script),
            TaskRequirements::generic(1, timeout_secs),
        )
    }

    /// Alive means running: a zombie (killed, not yet reaped by its new parent) does not count.
    fn alive(pid: u32) -> bool {
        let mut sys = sysinfo::System::new();
        sys.refresh_processes();
        sys.process(sysinfo::Pid::from(pid as usize))
            .is_some_and(|p| p.status() != sysinfo::ProcessStatus::Zombie)
    }

    fn kill_stray(pid: u32) {
        // SAFETY: SIGKILL to a PID this test recorded from its own leftover process.
        unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
    }

    /// The PID a script recorded in `file` (waits for the script to get that far).
    async fn recorded_pid(file: &Path) -> u32 {
        for _ in 0..200 {
            if let Some(pid) = std::fs::read_to_string(file)
                .ok()
                .and_then(|text| text.trim().parse::<u32>().ok())
            {
                return pid;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("{} was never written", file.display());
    }

    async fn assert_dies(what: &str, pid: u32) {
        for _ in 0..100 {
            if !alive(pid) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        kill_stray(pid);
        panic!("{what} (pid {pid}) survived the end of its task");
    }

    /// A script that leaves two descendants running: a background `sleep` of its own and, one level
    /// deeper, another one under a nested `sh`.
    fn tree_script(pids: &Path) -> String {
        format!(
            "sleep 300 &\necho $! > '{child}'\nsh -c 'sleep 300 & echo $! > \"$0\"; wait' '{grandchild}' &\nwait\n",
            child = pids.join("child.pid").display(),
            grandchild = pids.join("grandchild.pid").display(),
        )
    }

    #[tokio::test]
    async fn test_timeout_kills_the_whole_tree_including_grandchildren() {
        let (runner, _dir) = runner_with(|c| c);
        let pids = tempfile::tempdir().unwrap();
        let task = script_task(tree_script(pids.path()), 2);

        let res = runner.execute_task(&task, None, None).await;

        assert_eq!(res.exit_code, EXIT_CODE_TIMEOUT);
        assert_dies(
            "background child",
            recorded_pid(&pids.path().join("child.pid")).await,
        )
        .await;
        assert_dies(
            "nested grandchild",
            recorded_pid(&pids.path().join("grandchild.pid")).await,
        )
        .await;
    }

    #[tokio::test]
    async fn test_cancellation_kills_the_whole_tree_including_grandchildren() {
        let (runner, _dir) = runner_with(|c| c);
        let pids = tempfile::tempdir().unwrap();
        let task = script_task(tree_script(pids.path()), 60);
        let (cancel_tx, cancel_rx) = oneshot::channel();

        let run = tokio::spawn({
            let runner = Arc::clone(&runner);
            async move { runner.execute_task(&task, None, Some(cancel_rx)).await }
        });
        let child = recorded_pid(&pids.path().join("child.pid")).await;
        let grandchild = recorded_pid(&pids.path().join("grandchild.pid")).await;
        assert!(
            alive(child) && alive(grandchild),
            "the tree must be running before the cancel"
        );

        cancel_tx.send("stop".into()).unwrap();
        let res = run.await.unwrap();

        assert_eq!(res.exit_code, EXIT_CODE_CANCELLED);
        assert!(res.error.unwrap().contains("Cancelled: stop"));
        assert_dies("background child", child).await;
        assert_dies("nested grandchild", grandchild).await;
    }

    #[tokio::test]
    async fn test_dropping_the_run_kills_the_tree() {
        // This is what `abort_handle.abort()` (a cancel the channel could not deliver) and a
        // shut-down runtime do to a running task.
        let (runner, _dir) = runner_with(|c| c);
        let pids = tempfile::tempdir().unwrap();
        let task = script_task(tree_script(pids.path()), 60);

        let run = tokio::spawn({
            let runner = Arc::clone(&runner);
            async move { runner.execute_task(&task, None, None).await }
        });
        let child = recorded_pid(&pids.path().join("child.pid")).await;
        let grandchild = recorded_pid(&pids.path().join("grandchild.pid")).await;

        run.abort();
        assert!(run.await.unwrap_err().is_cancelled());

        assert_dies("background child", child).await;
        assert_dies("nested grandchild", grandchild).await;
    }

    #[tokio::test]
    async fn test_a_daemon_left_behind_by_a_finished_task_is_killed_with_it() {
        // The shell exits at once; the helper redirected its output, so no pipe keeps the task
        // open. It must still not outlive the task (the sandbox is deleted right after).
        let (runner, _dir) = runner_with(|c| c);
        let pids = tempfile::tempdir().unwrap();
        let pid_file = pids.path().join("daemon.pid");
        let task = script_task(
            format!(
                "( sleep 300 >/dev/null 2>&1 </dev/null & echo $! > '{}' )\necho finished\n",
                pid_file.display()
            ),
            30,
        );

        let res = runner.execute_task(&task, None, None).await;

        assert!(res.is_success(), "{res:?}");
        assert_eq!(res.stdout_str().trim(), "finished");
        assert_dies("daemon", recorded_pid(&pid_file).await).await;
    }

    #[tokio::test]
    async fn test_a_background_process_holding_the_pipes_does_not_turn_exit_7_into_a_timeout() {
        let (runner, _dir) = runner_with(|c| c.with_pipe_drain_grace(Duration::from_millis(300)));
        let pids = tempfile::tempdir().unwrap();
        let pid_file = pids.path().join("holder.pid");
        // `sleep` inherits stdout and stderr, so the pipes stay open for five minutes.
        let task = script_task(
            format!(
                "sleep 300 &\necho $! > '{}'\necho done-output\necho some-warning >&2\nexit 7\n",
                pid_file.display()
            ),
            20,
        );

        let t0 = Instant::now();
        let res = runner.execute_task(&task, None, None).await;
        let elapsed = t0.elapsed();

        assert!(
            elapsed < Duration::from_secs(10),
            "must not wait for the pipes or the timeout: {elapsed:?}"
        );
        assert_eq!(
            res.exit_code, 7,
            "the real exit status, not a timeout: {res:?}"
        );
        assert_ne!(res.exit_code, EXIT_CODE_TIMEOUT);
        assert!(
            res.stdout_str().contains("done-output"),
            "output must be kept: {:?}",
            res.stdout_str()
        );
        assert!(res.stderr_str().contains("some-warning"));
        assert!(
            res.stderr_str().contains("kept the output pipes open"),
            "the result should explain the terminated helpers: {:?}",
            res.stderr_str()
        );
        assert_dies("pipe holder", recorded_pid(&pid_file).await).await;
    }

    #[tokio::test]
    async fn test_a_background_process_holding_the_pipes_does_not_fail_a_successful_task() {
        let (runner, _dir) = runner_with(|c| c.with_pipe_drain_grace(Duration::from_millis(300)));
        let pids = tempfile::tempdir().unwrap();
        let pid_file = pids.path().join("holder.pid");
        let task = script_task(
            format!(
                "sleep 300 &\necho $! > '{}'\necho built\n",
                pid_file.display()
            ),
            20,
        );

        let res = runner.execute_task(&task, None, None).await;

        assert!(res.is_success(), "{res:?}");
        assert_eq!(res.exit_code, 0);
        assert!(res.stdout_str().contains("built"));
        assert_dies("pipe holder", recorded_pid(&pid_file).await).await;
    }

    #[tokio::test]
    async fn test_a_helper_that_escaped_the_process_group_cannot_hold_the_run_hostage() {
        // A helper in its own session is out of reach of the group kill and holds the pipes. The
        // run must still end after the drain grace, with the real status and the output so far.
        if std::process::Command::new("python3")
            .arg("--version")
            .output()
            .is_err()
        {
            eprintln!("python3 not available; skipping");
            return;
        }
        let (runner, _dir) = runner_with(|c| c.with_pipe_drain_grace(Duration::from_millis(300)));
        let task = Task::new(
            TaskSpec::command(
                "python3",
                vec![
                    "-c".into(),
                    "import subprocess; p = subprocess.Popen(['sleep', '300'], start_new_session=True); print(p.pid, flush=True)".into(),
                ],
            ),
            TaskRequirements::generic(1, 20),
        );

        let t0 = Instant::now();
        let res = runner.execute_task(&task, None, None).await;
        let elapsed = t0.elapsed();

        let escaped: Option<u32> = res
            .stdout_str()
            .lines()
            .next()
            .and_then(|l| l.trim().parse().ok());
        if let Some(pid) = escaped {
            kill_stray(pid);
        }
        assert!(res.is_success(), "{res:?}");
        assert!(
            escaped.is_some(),
            "the pid line was captured: {:?}",
            res.stdout_str()
        );
        assert!(elapsed < Duration::from_secs(10), "{elapsed:?}");
    }

    #[tokio::test]
    async fn test_partial_output_survives_timeout_and_cancellation() {
        let (runner, _dir) = runner_with(|c| c);
        let task = script_task(
            "echo partial-out\necho partial-err >&2\nsleep 300\n".into(),
            1,
        );
        let res = runner.execute_task(&task, None, None).await;
        assert_eq!(res.exit_code, EXIT_CODE_TIMEOUT);
        assert!(
            res.stdout_str().contains("partial-out"),
            "{:?}",
            res.stdout_str()
        );
        assert!(
            res.stderr_str().contains("partial-err"),
            "{:?}",
            res.stderr_str()
        );
        assert!(
            res.stderr_str().contains("Task timed out after 1s"),
            "{:?}",
            res.stderr_str()
        );
        assert!(res.error.as_deref().unwrap().contains("timed out after 1s"));

        let task = script_task("echo partial-out\nsleep 300\n".into(), 60);
        let (cancel_tx, cancel_rx) = oneshot::channel();
        let run = tokio::spawn({
            let runner = Arc::clone(&runner);
            async move { runner.execute_task(&task, None, Some(cancel_rx)).await }
        });
        tokio::time::sleep(Duration::from_millis(600)).await;
        cancel_tx.send("enough".into()).unwrap();
        let res = run.await.unwrap();
        assert_eq!(res.exit_code, EXIT_CODE_CANCELLED);
        assert!(
            res.stdout_str().contains("partial-out"),
            "{:?}",
            res.stdout_str()
        );
    }

    #[tokio::test]
    async fn test_ending_a_tree_leaves_an_unrelated_process_alone() {
        let mut bystander = std::process::Command::new("sleep")
            .arg("300")
            .spawn()
            .unwrap();
        let bystander_pid = bystander.id();

        let (runner, _dir) = runner_with(|c| c);
        let pids = tempfile::tempdir().unwrap();
        let res = runner
            .execute_task(&script_task(tree_script(pids.path()), 1), None, None)
            .await;
        assert_eq!(res.exit_code, EXIT_CODE_TIMEOUT);
        let finished = runner
            .execute_task(&script_task("echo fine".into(), 10), None, None)
            .await;
        assert!(finished.is_success());

        let still_there = alive(bystander_pid);
        let _ = bystander.kill();
        let _ = bystander.wait();
        assert!(
            still_there,
            "an unrelated process was killed along with a task's tree"
        );
    }

    #[tokio::test]
    async fn test_task_processes_run_at_lower_priority_by_default_and_it_is_configurable() {
        /// The nice value `ps` reports for the shell that runs the task.
        async fn task_nice(runner: &TaskRunner) -> i32 {
            let task = Task::new(
                TaskSpec::command("sh", vec!["-c".into(), "ps -o ni= -p $$".into()]),
                TaskRequirements::generic(1, 10),
            );
            let res = runner.execute_task(&task, None, None).await;
            assert!(res.is_success(), "{res:?}");
            res.stdout_str()
                .trim()
                .parse()
                .unwrap_or_else(|_| panic!("no nice value in {:?}", res.stdout_str()))
        }

        let own_nice: i32 = {
            let out = std::process::Command::new("ps")
                .args(["-o", "ni=", "-p", &std::process::id().to_string()])
                .output()
                .unwrap();
            String::from_utf8_lossy(&out.stdout).trim().parse().unwrap()
        };

        let (default_runner, _d1) =
            runner_with(|c| c.with_process_priority(ProcessPriority::default()));
        assert_eq!(ProcessPriority::default(), ProcessPriority::BelowNormal);
        assert_eq!(task_nice(&default_runner).await, own_nice.max(10));

        let (normal, _d2) = runner_with(|c| c.with_process_priority(ProcessPriority::Normal));
        assert_eq!(task_nice(&normal).await, own_nice);

        let (idle, _d3) = runner_with(|c| c.with_process_priority(ProcessPriority::Idle));
        assert_eq!(task_nice(&idle).await, own_nice.max(19));
    }

    #[tokio::test]
    async fn test_worker_secrets_are_not_inherited_by_task_processes() {
        const SENTINELS: [(&str, &str); 3] = [
            ("OXIDESWARM_SECRET", "sentinel-cluster-secret"),
            ("OXIDESWARM_SECRET_FILE", "/sentinel/secret/file"),
            ("OXIDE_MESH_TOKEN", "sentinel-mesh-token"),
        ];
        // Put the worker's "own" secrets in this process's environment, remove them afterwards.
        struct Restore;
        impl Drop for Restore {
            fn drop(&mut self) {
                for (name, _) in SENTINELS {
                    std::env::remove_var(name);
                }
            }
        }
        let _restore = Restore;
        for (name, value) in SENTINELS {
            std::env::set_var(name, value);
        }

        let (runner, _dir) = runner_with(|c| c);
        let probe = "echo \"[${OXIDESWARM_SECRET-unset}][${OXIDESWARM_SECRET_FILE-unset}][${OXIDE_MESH_TOKEN-unset}]\"";

        // Command task
        let task = Task::new(
            TaskSpec::command("sh", vec!["-c".into(), probe.into()]),
            TaskRequirements::generic(1, 10),
        );
        let res = runner.execute_task(&task, None, None).await;
        assert_eq!(res.stdout_str().trim(), "[unset][unset][unset]", "{res:?}");

        // ShellScript task
        let res = runner
            .execute_task(&script_task(probe.into(), 10), None, None)
            .await;
        assert_eq!(res.stdout_str().trim(), "[unset][unset][unset]", "{res:?}");

        // A value the task is explicitly given is its own business and still arrives.
        let mut env = HashMap::new();
        env.insert(
            "OXIDE_MESH_TOKEN".to_string(),
            "given-on-purpose".to_string(),
        );
        let task = Task::new(
            TaskSpec::Command {
                program: "sh".into(),
                args: vec!["-c".into(), probe.into()],
                env,
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, 10),
        );
        let res = runner.execute_task(&task, None, None).await;
        assert_eq!(
            res.stdout_str().trim(),
            "[unset][unset][given-on-purpose]",
            "{res:?}"
        );
    }
}
