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
//! threads). `kernel_name` is only a label echoed in the report: it is a benchmark workload, not a
//! general kernel launcher, and the input matrices are generated from `input_data` rather than
//! supplied by the caller.
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

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::warn;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::WorkerMessage;
use rusty_grid_core::task::{Bytes, CheckpointData, Task, TaskId, TaskResult, TaskSpec};

use crate::blob_cache::{BlobCache, DEFAULT_BLOB_CACHE_MAX_BYTES};
use crate::cpu_simd;
use crate::gpu::{check_gemm_request, DeviceLimits, GpuError};
use crate::isolation::{HostDirs, IsolationLevel};
use crate::process_tree::ProcessPriority;
use crate::sandbox::{SandboxConfig, SandboxError};
use crate::staging::OutputLimits;
#[cfg(windows)]
use crate::workspace::WorkspaceError;
use crate::workspace::{WorkspaceLimits, WorkspaceStore};

mod capture;
mod supervise;
mod workloads;
pub(crate) use workloads::base_refusal;

pub use capture::read_bounded_stream;
#[cfg(any(feature = "3d", test))]
pub(crate) use capture::read_output_file;
pub(crate) use capture::{append_note, cancellation, check_cancelled_now};
#[allow(unused_imports)]
pub(crate) use supervise::WorkspaceProcessCtx;

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
pub(super) const POST_KILL_DRAIN: Duration = Duration::from_millis(500);

/// How long to wait for the killed leader to be reaped.
pub(super) const REAP_TIMEOUT: Duration = Duration::from_secs(2);

/// Default maximum time a task waits to acquire its named workspace lock before failing.
pub const DEFAULT_WORKSPACE_LOCK_WAIT: Duration = Duration::from_secs(5);

/// Variables of the worker's own environment that task processes must never inherit: they hold
/// the cluster secret or the agent-mesh token. (A task can still be handed one explicitly through
/// its own `env`.)
pub use rusty_grid_core::cluster_auth::SECRET_ENV_VARS;

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
    /// Bound of the blob cache in `<sandbox base>/cache/blobs` (see [`crate::blob_cache`]).
    pub blob_cache_bytes: u64,
    /// Caps on the files a task's output patterns may bring back (see [`crate::staging`]).
    pub output_limits: OutputLimits,
    /// Optional explicit path to the host Blender executable (`--blender-path`).
    pub blender_path: Option<String>,
    /// How strongly task processes are confined (`--isolation`, design 0033). Process tasks
    /// only: in-process workloads are not affected.
    pub isolation: IsolationLevel,
    /// The host directories the isolation policy names (the owner's home, the Rust toolchain).
    pub host_dirs: HostDirs,
    /// Files denied to confined tasks wherever they are (the cluster secret file, the config
    /// file in use).
    pub protected_files: Vec<PathBuf>,
    /// Whether persistent named workspaces (`--workspaces`, design 0039) are enabled.
    pub workspaces: bool,
    /// Custom workspace root directory (`--workspace-root`). `None` uses [`crate::workspace::default_root`].
    pub workspace_root: Option<PathBuf>,
    /// Size and count bounds for the workspace store (`--workspace-limit-mb`, `--max-workspaces`).
    pub workspace_limits: WorkspaceLimits,
    /// Optional pre-opened workspace store (for tests with custom clock or limits).
    pub workspace_store: Option<WorkspaceStore>,
    /// Maximum time a task waits to acquire a workspace lock before failing as busy.
    pub workspace_lock_wait: Duration,
    /// Multiplier applied to `BuiltinTest` sleep durations in integration tests (`1` in production).
    #[doc(hidden)]
    pub test_sleep_multiplier: u32,
    /// Additive offset (ms) applied to `BuiltinTest` sleep durations in integration tests (`0` in production).
    #[doc(hidden)]
    pub test_sleep_offset_ms: u64,
    /// Optional in-process simulated task runner (`SCALE-1` load harness and simulation tests).
    pub fake_runner: Option<crate::sim::FakeRunnerConfig>,
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
            blob_cache_bytes: DEFAULT_BLOB_CACHE_MAX_BYTES,
            output_limits: OutputLimits::default(),
            blender_path: None,
            isolation: IsolationLevel::Off,
            host_dirs: HostDirs::detect(),
            protected_files: Vec::new(),
            workspaces: false,
            workspace_root: None,
            workspace_limits: WorkspaceLimits::default(),
            workspace_store: None,
            workspace_lock_wait: DEFAULT_WORKSPACE_LOCK_WAIT,
            test_sleep_multiplier: 1,
            test_sleep_offset_ms: 0,
            fake_runner: None,
        }
    }

    /// Scales `BuiltinTest` sleep durations by `multiplier.max(1)` (test hook for speed-aware
    /// scheduling integration tests). Exists for integration tests only.
    #[doc(hidden)]
    pub fn with_test_sleep_multiplier(mut self, multiplier: u32) -> Self {
        self.test_sleep_multiplier = multiplier.max(1);
        self
    }

    /// Adds `offset_ms` to `BuiltinTest` sleep durations (test hook for speed-aware scheduling
    /// integration tests). Exists for integration tests only.
    #[doc(hidden)]
    pub fn with_test_sleep_offset_ms(mut self, offset_ms: u64) -> Self {
        self.test_sleep_offset_ms = offset_ms;
        self
    }

    /// Confines task processes at `level`. The host directories come from this process's
    /// environment (see [`HostDirs::detect`]); use [`with_host_dirs`](Self::with_host_dirs) to
    /// name them.
    pub fn with_isolation(mut self, level: IsolationLevel) -> Self {
        self.isolation = level;
        self
    }

    pub fn with_protected_files(mut self, files: Vec<PathBuf>) -> Self {
        self.protected_files = files;
        self
    }

    pub fn with_host_dirs(mut self, host_dirs: HostDirs) -> Self {
        self.host_dirs = host_dirs;
        self
    }

    pub fn with_workspaces(mut self, enabled: bool) -> Self {
        self.workspaces = enabled;
        self
    }

    pub fn with_workspace_root(mut self, root: Option<PathBuf>) -> Self {
        self.workspace_root = root;
        self
    }

    pub fn with_workspace_limits(mut self, limits: WorkspaceLimits) -> Self {
        self.workspace_limits = limits;
        self
    }

    pub fn with_workspace_store(mut self, store: WorkspaceStore) -> Self {
        self.workspaces = true;
        self.workspace_root = Some(store.root().to_path_buf());
        self.workspace_limits = store.limits();
        self.workspace_store = Some(store);
        self
    }

    pub fn with_workspace_lock_wait(mut self, wait: Duration) -> Self {
        self.workspace_lock_wait = wait;
        self
    }

    pub fn with_blender_path(mut self, blender_path: Option<String>) -> Self {
        self.blender_path = blender_path;
        self
    }

    pub fn with_blob_cache_bytes(mut self, bytes: u64) -> Self {
        self.blob_cache_bytes = bytes;
        self
    }

    pub fn with_output_limits(mut self, limits: OutputLimits) -> Self {
        self.output_limits = limits;
        self
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

    /// Configures an in-process synthetic runner for load-harness simulations.
    pub fn with_fake_runner(mut self, fake: Option<crate::sim::FakeRunnerConfig>) -> Self {
        self.fake_runner = fake;
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

struct BlobCacheSlot {
    cache: std::sync::RwLock<Option<Arc<BlobCache>>>,
    init_lock: std::sync::Mutex<()>,
}

impl BlobCacheSlot {
    fn new() -> Self {
        Self {
            cache: std::sync::RwLock::new(None),
            init_lock: std::sync::Mutex::new(()),
        }
    }
}

/// Core computational workload execution engine for Worker nodes.
pub struct TaskRunner {
    pub(super) worker_id: Uuid,
    pub(super) capabilities: WorkerCapabilities,
    pub(super) config: RunnerConfig,
    /// The blob cache, opened lazily and retried on subsequent tasks if an earlier open failed
    /// (see [`TaskRunner::ensure_blob_cache`] and [`TaskRunner::blob_cache`]).
    blob_cache: Arc<BlobCacheSlot>,
    /// Persistent named workspace store, opened at worker start-up or on first workspace task
    /// when `config.workspaces` is true.
    workspace_store: std::sync::RwLock<Option<WorkspaceStore>>,
    /// Where GPU-class tasks get their engine; rebuilds it after a device loss.
    #[cfg(feature = "gpu-wgpu")]
    gpu_engine: Arc<crate::wgpu_engine::EngineProvider>,
}

impl TaskRunner {
    pub fn new(worker_id: Uuid, capabilities: WorkerCapabilities, config: RunnerConfig) -> Self {
        rusty_grid_core::capabilities::suppress_windows_error_dialogs();
        let initial_store = config.workspace_store.clone();
        Self {
            worker_id,
            capabilities,
            config,
            blob_cache: Arc::new(BlobCacheSlot::new()),
            workspace_store: std::sync::RwLock::new(initial_store),
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

    fn open_blob_cache_blocking(
        slot: &BlobCacheSlot,
        sandbox_config: &SandboxConfig,
        max_bytes: u64,
    ) -> Result<Arc<BlobCache>, String> {
        if let Some(cache) = slot.cache.read().unwrap_or_else(|e| e.into_inner()).clone() {
            return Ok(cache);
        }
        let _guard = slot.init_lock.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(cache) = slot.cache.read().unwrap_or_else(|e| e.into_inner()).clone() {
            return Ok(cache);
        }
        sandbox_config
            .ensure_private_root_recheck_blocking()
            .map_err(|e| e.to_string())?;
        let dir = sandbox_config.base_dir.join("cache").join("blobs");
        let opened = BlobCache::open(&dir, max_bytes)
            .map_err(|e| format!("cannot open the blob cache in '{}': {e}", dir.display()))?;
        *slot.cache.write().unwrap_or_else(|e| e.into_inner()) = Some(Arc::clone(&opened));
        Ok(opened)
    }

    /// Returns the blob cache if it has already been opened, without touching disk.
    pub fn blob_cache_if_open(&self) -> Option<Arc<BlobCache>> {
        self.blob_cache
            .cache
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// The worker's blob cache in `<sandbox base>/cache/blobs`, opened on first use (and retried
    /// on subsequent calls if an earlier open failed).
    pub fn blob_cache(&self) -> Result<Arc<BlobCache>, String> {
        Self::open_blob_cache_blocking(
            &self.blob_cache,
            &self.config.sandbox_config,
            self.config.blob_cache_bytes,
        )
    }

    /// Async counterpart to [`TaskRunner::blob_cache`]: returns the already-open cache immediately
    /// or runs the cold/retry open inside `spawn_blocking` so Win32/filesystem checks never block
    /// an async worker thread.
    pub async fn ensure_blob_cache(&self) -> Result<Arc<BlobCache>, String> {
        if let Some(cache) = self.blob_cache_if_open() {
            return Ok(cache);
        }
        let slot = Arc::clone(&self.blob_cache);
        let sandbox_config = self.config.sandbox_config.clone();
        let max_bytes = self.config.blob_cache_bytes;
        tokio::task::spawn_blocking(move || {
            Self::open_blob_cache_blocking(&slot, &sandbox_config, max_bytes)
        })
        .await
        .map_err(|e| e.to_string())?
    }

    /// Releases any output blobs pinned in the cache for `task_id`, without opening the cache if
    /// it was never used.
    pub fn release_task_outputs(&self, task_id: TaskId) {
        if let Some(cache) = self.blob_cache_if_open() {
            cache.release_task_outputs(task_id);
        }
    }

    /// Opens (creating and checking privacy on) the worker's persistent workspace store when
    /// `config.workspaces` is true, and caches it for subsequent tasks. Returns `Ok(None)` when
    /// `config.workspaces` is false.
    pub async fn ensure_workspace_store(&self) -> Result<Option<WorkspaceStore>, String> {
        if !self.config.workspaces {
            return Ok(None);
        }
        if let Some(store) = self
            .workspace_store
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        {
            return Ok(Some(store));
        }
        let root = self.config.workspace_root.clone();
        let limits = self.config.workspace_limits;
        #[cfg(windows)]
        let validate_for_isolation =
            (self.config.isolation != IsolationLevel::Off).then(|| self.config.host_dirs.clone());
        let store = tokio::task::spawn_blocking(move || {
            #[cfg(windows)]
            if let Some(ref host_dirs) = validate_for_isolation {
                if let Some(ref r) = root.clone().or_else(crate::workspace::default_root) {
                    crate::isolation::windows::validate_workspace_root_safe(r, host_dirs).map_err(
                        |reason| WorkspaceError::Root {
                            path: r.clone(),
                            reason,
                        },
                    )?;
                }
            }
            WorkspaceStore::open(root, limits)
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
        let mut slot = self
            .workspace_store
            .write()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = slot.clone() {
            return Ok(Some(existing));
        }
        *slot = Some(store.clone());
        Ok(Some(store))
    }

    /// Returns the workspace root directory if workspaces are enabled and the store is open (or a
    /// root path was configured).
    pub(super) fn workspace_root_if_open(&self) -> Option<PathBuf> {
        if let Some(store) = self
            .workspace_store
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            return Some(store.root().to_path_buf());
        }
        self.config
            .workspaces
            .then(|| self.config.workspace_root.clone())
            .flatten()
    }

    async fn execute_fake_task(
        &self,
        task: &Task,
        fake: &crate::sim::FakeRunnerConfig,
        timeout_duration: Duration,
        mut cancel_rx: Option<oneshot::Receiver<String>>,
        start_time: Instant,
    ) -> TaskResult {
        if let Some(ref cb) = fake.on_task_started {
            cb(task.id);
        }

        if let Some(reason) = check_cancelled_now(&mut cancel_rx) {
            return TaskResult::failure(
                self.worker_id,
                task.id,
                EXIT_CODE_CANCELLED,
                Bytes::new(),
                Bytes::new(),
                start_time.elapsed().as_millis() as u64,
                Some(format!("Cancelled: {reason}")),
            );
        }

        let delay = fake.effective_execution_delay();
        if !delay.is_zero() {
            tokio::select! {
                biased;
                reason = cancellation(&mut cancel_rx) => {
                    return TaskResult::failure(
                        self.worker_id,
                        task.id,
                        EXIT_CODE_CANCELLED,
                        Bytes::new(),
                        Bytes::new(),
                        start_time.elapsed().as_millis() as u64,
                        Some(format!("Cancelled: {reason}")),
                    );
                }
                _ = tokio::time::sleep(timeout_duration) => {
                    return TaskResult::failure(
                        self.worker_id,
                        task.id,
                        EXIT_CODE_TIMEOUT,
                        Bytes::new(),
                        Bytes::new(),
                        start_time.elapsed().as_millis() as u64,
                        Some(format!("Task timed out after {}s", timeout_duration.as_secs())),
                    );
                }
                _ = tokio::time::sleep(delay) => {}
            }
        }

        let elapsed_ms = start_time.elapsed().as_millis() as u64;
        if matches!(
            task.spec,
            TaskSpec::BuiltinTest {
                should_fail: true,
                ..
            }
        ) {
            return TaskResult::failure(
                self.worker_id,
                task.id,
                EXIT_CODE_GENERAL_ERROR,
                Bytes::new(),
                Bytes::from_static(b"Simulated BuiltinTest forced failure\n"),
                elapsed_ms,
                Some("BuiltinTest should_fail was true".to_string()),
            );
        }

        TaskResult::success(
            self.worker_id,
            task.id,
            fake.stdout_payload.clone(),
            elapsed_ms,
            matches!(task.spec, TaskSpec::GpuCompute { .. }),
        )
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
                    artifacts: Vec::new(),
                    outputs: Vec::new(),
                    cached: false,
                };
            }
        }

        if let Some(ref fake) = self.config.fake_runner {
            return self
                .execute_fake_task(task, fake, timeout_duration, cancel_rx, start_time)
                .await;
        }

        if let Some(ref ws) = task.workspace {
            if !matches!(
                task.spec,
                TaskSpec::Command { .. } | TaskSpec::ShellScript { .. }
            ) {
                let msg = "workspace tasks only support Command and ShellScript workloads";
                return TaskResult::failure(
                    self.worker_id,
                    task.id,
                    EXIT_CODE_GENERAL_ERROR,
                    "",
                    format!("Execution Error: {msg}"),
                    start_time.elapsed().as_millis() as u64,
                    Some(msg.to_string()),
                );
            }
            return self
                .execute_workspace_workload(
                    task,
                    ws,
                    timeout_duration,
                    child_pid,
                    cancel_rx,
                    start_time,
                )
                .await;
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

        let sleep_mult = u64::from(self.config.test_sleep_multiplier.max(1));
        let effective_sleep_ms = duration_ms
            .saturating_mul(sleep_mult)
            .saturating_add(self.config.test_sleep_offset_ms);
        let compute_fut = async {
            if cfg!(test) && test_name == "oversized_asset" {
                if duration_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(effective_sleep_ms)).await;
                }
                "engine stdout line\n".to_string()
            } else if test_name == "sleep"
                || test_name == "duration"
                || (duration_ms > 0
                    && test_name != "checkpoint"
                    && test_name != "checkpoint_iterative"
                    && test_name != "hash_compute")
            {
                tokio::time::sleep(Duration::from_millis(effective_sleep_ms)).await;
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
                let mut res =
                    TaskResult::success(self.worker_id, task_id, output, elapsed, require_gpu);
                if cfg!(test) && test_name == "oversized_asset" {
                    res.stderr = Bytes::from_static(b"engine stderr line\n");
                    res = res.with_asset_bytes(Bytes::from(vec![0u8; 70 * 1024 * 1024]));
                }
                res
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
                    asset_bytes: None,
                    artifacts: Vec::new(),
                    outputs: Vec::new(),
                    cached: false,
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
}

#[cfg(test)]
#[allow(unused_imports)]
pub(in crate::runner) use {
    crate::process_tree::{configure_command, ProcessTree},
    crate::sandbox::{sanitize_relative_path, set_permissions, Sandbox},
    crate::staging::{collect_outputs_from_dir_pinned, materialize_inputs},
    capture::TRUNCATION_NOTICE,
    rusty_grid_core::blob::{TreeManifest, MAX_MANIFEST_BYTES},
    rusty_grid_core::task::{
        sha256_digest, BytesMut, OutputArtifact, WorkspaceRef, WorkspaceSource,
        MAX_AGGREGATE_ARTIFACT_BYTES, MAX_OUTPUT_ARTIFACT_BYTES,
    },
    std::collections::HashMap,
    std::path::Path,
    std::sync::Mutex,
    supervise::{describe_exit, resolve_workspace_cwd},
    tracing::{debug, info},
};

#[cfg(all(test, not(windows)))]
#[allow(unused_imports)]
pub(in crate::runner) use {
    crate::workspace::WorkspaceError,
    supervise::{pick_shell, resolve_default_shell},
};

#[cfg(all(test, windows))]
#[allow(unused_imports)]
pub(in crate::runner) use supervise::resolve_windows_executable;

#[cfg(test)]
mod tests;

/// Tests of exit-status mapping, the cancellation channel and the default shell; no processes of
/// their own beyond `sh`, so they run everywhere `sh` does.
#[cfg(test)]
mod outcome_tests;

/// Real process trees: every test here spawns grandchildren (`sh -c` with a background `sleep`)
/// and checks that they are gone, and that nothing unrelated is touched.
#[cfg(all(test, unix))]
mod process_tree_tests;

#[cfg(test)]
mod workspace_runner_tests;
