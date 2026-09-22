//! Task definitions, specifications, requirements, results, and state machine transitions.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use uuid::Uuid;

/// Strongly-typed unique identifier for a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskId(pub Uuid);

impl TaskId {
    /// Generates a new random (v4) TaskId.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Creates a TaskId from an existing UUID.
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Returns a reference to the inner UUID.
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    /// Consumes self and returns the inner UUID.
    pub fn into_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for TaskId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(TaskId)
    }
}

/// Resource requirements and scheduling constraints for a task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRequirements {
    /// Minimum CPU cores required.
    pub cpu_cores: usize,
    /// Minimum system RAM required in MB.
    pub ram_mb: u64,
    /// Whether this task requires a GPU-capable worker.
    pub gpu_required: bool,
    /// Maximum allowable execution time in seconds before cancellation.
    pub timeout_secs: u64,
    /// Maximum allowable retries on worker failure. None inherits cluster default.
    #[serde(default)]
    pub max_retries: Option<u32>,
}

impl Default for TaskRequirements {
    fn default() -> Self {
        Self {
            cpu_cores: 1,
            ram_mb: 512,
            gpu_required: false,
            timeout_secs: 60,
            max_retries: None,
        }
    }
}

impl TaskRequirements {
    pub fn new(cpu_cores: usize, ram_mb: u64, gpu_required: bool, timeout_secs: u64) -> Self {
        Self {
            cpu_cores,
            ram_mb,
            gpu_required,
            timeout_secs,
            max_retries: None,
        }
    }

    /// Helper for generic CPU tasks.
    pub fn generic(cpu_cores: usize, timeout_secs: u64) -> Self {
        Self {
            cpu_cores,
            ram_mb: 512,
            gpu_required: false,
            timeout_secs,
            max_retries: None,
        }
    }

    /// Helper for GPU tasks.
    pub fn gpu(timeout_secs: u64) -> Self {
        Self {
            cpu_cores: 1,
            ram_mb: 1024,
            gpu_required: true,
            timeout_secs,
            max_retries: None,
        }
    }

    /// Configures maximum retry attempts on worker failure or crash.
    pub fn with_max_retries(mut self, retries: u32) -> Self {
        self.max_retries = Some(retries);
        self
    }

    /// Validates the requirements specification.
    pub fn validate(&self) -> Result<(), String> {
        if self.timeout_secs == 0 {
            return Err("Task timeout_secs must be greater than 0".to_string());
        }
        Ok(())
    }
}

fn default_work_group_size() -> u32 {
    16
}

fn default_matrix_dim() -> u32 {
    64
}

fn default_test_name() -> String {
    "hash_compute".to_string()
}

fn default_iterations() -> u32 {
    100_000
}

/// Concrete computation payload and execution specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskSpec {
    /// Direct command execution (e.g. `echo`, `python3`, CLI tools).
    Command {
        program: String,
        args: Vec<String>,
        env: HashMap<String, String>,
        working_dir: Option<PathBuf>,
        stdin: Option<Vec<u8>>,
    },
    /// Shell script execution via system shell.
    ShellScript {
        script: String,
        interpreter: Option<String>,
        env: HashMap<String, String>,
    },
    /// Distributed Rust compilation job.
    RustCompilation {
        crate_name: String,
        /// Relative file paths mapped to file contents (e.g., "Cargo.toml", "src/lib.rs").
        source_files: HashMap<String, String>,
        /// Flags passed to compiler / cargo (e.g., ["--crate-type", "lib"]).
        compiler_flags: Vec<String>,
        target_dir: Option<PathBuf>,
    },
    /// GPU compute workload (matrix multiplication, hash calculations, kernel execution).
    GpuCompute {
        kernel_name: String,
        input_data: Vec<u8>,
        work_group_size: u32,
        simulated_matrix_dim: u32,
        compute_intensity: u32,
    },
    /// In-memory built-in test task for rapid verification.
    BuiltinTest {
        test_name: String,
        iterations: u32,
        duration_ms: u64,
        should_fail: bool,
        require_gpu: bool,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum HumanTaskSpec {
    Command {
        #[serde(alias = "command")]
        program: String,
        args: Vec<String>,
        #[serde(default)]
        env: HashMap<String, String>,
        #[serde(default)]
        working_dir: Option<PathBuf>,
        #[serde(default)]
        stdin: Option<Vec<u8>>,
    },
    ShellScript {
        script: String,
        #[serde(default)]
        interpreter: Option<String>,
        #[serde(default)]
        env: HashMap<String, String>,
    },
    RustCompilation {
        crate_name: String,
        source_files: HashMap<String, String>,
        #[serde(default, alias = "cargo_args")]
        compiler_flags: Vec<String>,
        #[serde(default)]
        target_dir: Option<PathBuf>,
    },
    GpuCompute {
        kernel_name: String,
        #[serde(default)]
        input_data: Vec<u8>,
        #[serde(default = "default_work_group_size")]
        work_group_size: u32,
        #[serde(default = "default_matrix_dim")]
        simulated_matrix_dim: u32,
        #[serde(default)]
        compute_intensity: u32,
    },
    BuiltinTest {
        #[serde(default = "default_test_name")]
        test_name: String,
        #[serde(default = "default_iterations")]
        iterations: u32,
        #[serde(default)]
        duration_ms: u64,
        #[serde(default)]
        should_fail: bool,
        #[serde(default)]
        require_gpu: bool,
    },
}

#[derive(Serialize, Deserialize)]
enum BinaryTaskSpec {
    Command {
        program: String,
        args: Vec<String>,
        env: HashMap<String, String>,
        working_dir: Option<PathBuf>,
        stdin: Option<Vec<u8>>,
    },
    ShellScript {
        script: String,
        interpreter: Option<String>,
        env: HashMap<String, String>,
    },
    RustCompilation {
        crate_name: String,
        source_files: HashMap<String, String>,
        compiler_flags: Vec<String>,
        target_dir: Option<PathBuf>,
    },
    GpuCompute {
        kernel_name: String,
        input_data: Vec<u8>,
        work_group_size: u32,
        simulated_matrix_dim: u32,
        compute_intensity: u32,
    },
    BuiltinTest {
        test_name: String,
        iterations: u32,
        duration_ms: u64,
        should_fail: bool,
        require_gpu: bool,
    },
}

impl From<TaskSpec> for HumanTaskSpec {
    fn from(spec: TaskSpec) -> Self {
        match spec {
            TaskSpec::Command {
                program,
                args,
                env,
                working_dir,
                stdin,
            } => HumanTaskSpec::Command {
                program,
                args,
                env,
                working_dir,
                stdin,
            },
            TaskSpec::ShellScript {
                script,
                interpreter,
                env,
            } => HumanTaskSpec::ShellScript {
                script,
                interpreter,
                env,
            },
            TaskSpec::RustCompilation {
                crate_name,
                source_files,
                compiler_flags,
                target_dir,
            } => HumanTaskSpec::RustCompilation {
                crate_name,
                source_files,
                compiler_flags,
                target_dir,
            },
            TaskSpec::GpuCompute {
                kernel_name,
                input_data,
                work_group_size,
                simulated_matrix_dim,
                compute_intensity,
            } => HumanTaskSpec::GpuCompute {
                kernel_name,
                input_data,
                work_group_size,
                simulated_matrix_dim,
                compute_intensity,
            },
            TaskSpec::BuiltinTest {
                test_name,
                iterations,
                duration_ms,
                should_fail,
                require_gpu,
            } => HumanTaskSpec::BuiltinTest {
                test_name,
                iterations,
                duration_ms,
                should_fail,
                require_gpu,
            },
        }
    }
}

impl From<HumanTaskSpec> for TaskSpec {
    fn from(spec: HumanTaskSpec) -> Self {
        match spec {
            HumanTaskSpec::Command {
                program,
                args,
                env,
                working_dir,
                stdin,
            } => TaskSpec::Command {
                program,
                args,
                env,
                working_dir,
                stdin,
            },
            HumanTaskSpec::ShellScript {
                script,
                interpreter,
                env,
            } => TaskSpec::ShellScript {
                script,
                interpreter,
                env,
            },
            HumanTaskSpec::RustCompilation {
                crate_name,
                source_files,
                compiler_flags,
                target_dir,
            } => TaskSpec::RustCompilation {
                crate_name,
                source_files,
                compiler_flags,
                target_dir,
            },
            HumanTaskSpec::GpuCompute {
                kernel_name,
                input_data,
                work_group_size,
                simulated_matrix_dim,
                compute_intensity,
            } => TaskSpec::GpuCompute {
                kernel_name,
                input_data,
                work_group_size,
                simulated_matrix_dim,
                compute_intensity,
            },
            HumanTaskSpec::BuiltinTest {
                test_name,
                iterations,
                duration_ms,
                should_fail,
                require_gpu,
            } => TaskSpec::BuiltinTest {
                test_name,
                iterations,
                duration_ms,
                should_fail,
                require_gpu,
            },
        }
    }
}

impl From<TaskSpec> for BinaryTaskSpec {
    fn from(spec: TaskSpec) -> Self {
        match spec {
            TaskSpec::Command {
                program,
                args,
                env,
                working_dir,
                stdin,
            } => BinaryTaskSpec::Command {
                program,
                args,
                env,
                working_dir,
                stdin,
            },
            TaskSpec::ShellScript {
                script,
                interpreter,
                env,
            } => BinaryTaskSpec::ShellScript {
                script,
                interpreter,
                env,
            },
            TaskSpec::RustCompilation {
                crate_name,
                source_files,
                compiler_flags,
                target_dir,
            } => BinaryTaskSpec::RustCompilation {
                crate_name,
                source_files,
                compiler_flags,
                target_dir,
            },
            TaskSpec::GpuCompute {
                kernel_name,
                input_data,
                work_group_size,
                simulated_matrix_dim,
                compute_intensity,
            } => BinaryTaskSpec::GpuCompute {
                kernel_name,
                input_data,
                work_group_size,
                simulated_matrix_dim,
                compute_intensity,
            },
            TaskSpec::BuiltinTest {
                test_name,
                iterations,
                duration_ms,
                should_fail,
                require_gpu,
            } => BinaryTaskSpec::BuiltinTest {
                test_name,
                iterations,
                duration_ms,
                should_fail,
                require_gpu,
            },
        }
    }
}

impl From<BinaryTaskSpec> for TaskSpec {
    fn from(spec: BinaryTaskSpec) -> Self {
        match spec {
            BinaryTaskSpec::Command {
                program,
                args,
                env,
                working_dir,
                stdin,
            } => TaskSpec::Command {
                program,
                args,
                env,
                working_dir,
                stdin,
            },
            BinaryTaskSpec::ShellScript {
                script,
                interpreter,
                env,
            } => TaskSpec::ShellScript {
                script,
                interpreter,
                env,
            },
            BinaryTaskSpec::RustCompilation {
                crate_name,
                source_files,
                compiler_flags,
                target_dir,
            } => TaskSpec::RustCompilation {
                crate_name,
                source_files,
                compiler_flags,
                target_dir,
            },
            BinaryTaskSpec::GpuCompute {
                kernel_name,
                input_data,
                work_group_size,
                simulated_matrix_dim,
                compute_intensity,
            } => TaskSpec::GpuCompute {
                kernel_name,
                input_data,
                work_group_size,
                simulated_matrix_dim,
                compute_intensity,
            },
            BinaryTaskSpec::BuiltinTest {
                test_name,
                iterations,
                duration_ms,
                should_fail,
                require_gpu,
            } => TaskSpec::BuiltinTest {
                test_name,
                iterations,
                duration_ms,
                should_fail,
                require_gpu,
            },
        }
    }
}

impl Serialize for TaskSpec {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            HumanTaskSpec::from(self.clone()).serialize(serializer)
        } else {
            BinaryTaskSpec::from(self.clone()).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for TaskSpec {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            HumanTaskSpec::deserialize(deserializer).map(Into::into)
        } else {
            BinaryTaskSpec::deserialize(deserializer).map(Into::into)
        }
    }
}

impl TaskSpec {
    pub fn new_command(program: impl Into<String>, args: Vec<String>) -> Self {
        Self::Command {
            program: program.into(),
            args,
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        }
    }

    pub fn command(program: impl Into<String>, args: Vec<String>) -> Self {
        Self::new_command(program, args)
    }

    pub fn new_shell_script(script: impl Into<String>) -> Self {
        Self::ShellScript {
            script: script.into(),
            interpreter: None,
            env: HashMap::new(),
        }
    }

    pub fn shell_script(script: impl Into<String>) -> Self {
        Self::new_shell_script(script)
    }

    pub fn new_rust_compilation(
        crate_name: impl Into<String>,
        source_files: HashMap<String, String>,
        compiler_flags: Vec<String>,
    ) -> Self {
        Self::RustCompilation {
            crate_name: crate_name.into(),
            source_files,
            compiler_flags,
            target_dir: None,
        }
    }

    pub fn rust_compilation(
        crate_name: impl Into<String>,
        source_files: HashMap<String, String>,
        compiler_flags: Vec<String>,
    ) -> Self {
        Self::new_rust_compilation(crate_name, source_files, compiler_flags)
    }

    pub fn new_gpu_compute(kernel_name: impl Into<String>, simulated_matrix_dim: u32) -> Self {
        Self::GpuCompute {
            kernel_name: kernel_name.into(),
            input_data: Vec::new(),
            work_group_size: default_work_group_size(),
            simulated_matrix_dim,
            compute_intensity: 1,
        }
    }

    pub fn gpu_compute(kernel_name: impl Into<String>, simulated_matrix_dim: u32) -> Self {
        Self::new_gpu_compute(kernel_name, simulated_matrix_dim)
    }

    pub fn new_builtin_test(test_name: impl Into<String>, iterations: u32) -> Self {
        Self::BuiltinTest {
            test_name: test_name.into(),
            iterations,
            duration_ms: 0,
            should_fail: false,
            require_gpu: false,
        }
    }

    pub fn builtin_test(test_name: impl Into<String>, iterations: u32) -> Self {
        Self::new_builtin_test(test_name, iterations)
    }

    /// Validates internal consistency of the task specification.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            TaskSpec::Command { program, .. } => {
                if program.trim().is_empty() {
                    return Err("Command program cannot be empty".to_string());
                }
            }
            TaskSpec::ShellScript { script, .. } => {
                if script.trim().is_empty() {
                    return Err("Shell script cannot be empty".to_string());
                }
            }
            TaskSpec::RustCompilation {
                crate_name,
                source_files,
                ..
            } => {
                if crate_name.trim().is_empty() {
                    return Err("Rust compilation crate_name cannot be empty".to_string());
                }
                if source_files.is_empty() {
                    return Err("Rust compilation requires at least one source file".to_string());
                }
            }
            TaskSpec::GpuCompute { kernel_name, .. } => {
                if kernel_name.trim().is_empty() {
                    return Err("GPU kernel_name cannot be empty".to_string());
                }
            }
            TaskSpec::BuiltinTest { .. } => {}
        }
        Ok(())
    }

    /// Returns true if this specification inherently requires GPU execution.
    pub fn requires_gpu(&self) -> bool {
        match self {
            TaskSpec::GpuCompute { .. } => true,
            TaskSpec::BuiltinTest { require_gpu, .. } => *require_gpu,
            _ => false,
        }
    }
}

/// The top-level task representation submitted to Master and dispatched to Workers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    /// Unique task ID.
    pub id: TaskId,
    /// Computation payload.
    pub spec: TaskSpec,
    /// Resource requirements and timeouts.
    pub requirements: TaskRequirements,
    /// UTC timestamp of creation (seconds since UNIX epoch).
    pub created_at_utc: u64,
    /// User-defined tags or labels.
    #[serde(default)]
    pub tags: Vec<String>,
}

impl Task {
    /// Creates a new Task with generated TaskId and current timestamp.
    pub fn new(spec: TaskSpec, requirements: TaskRequirements) -> Self {
        let created_at_utc = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            id: TaskId::new(),
            spec,
            requirements,
            created_at_utc,
            tags: Vec::new(),
        }
    }

    /// Builder method to attach tags.
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    /// Validates the task and checks for specification/requirement mismatches.
    pub fn validate(&self) -> Result<(), String> {
        self.spec.validate()?;
        self.requirements.validate()?;

        if self.spec.requires_gpu() && !self.requirements.gpu_required {
            return Err(
                "TaskSpec requires GPU, but TaskRequirements.gpu_required is false".to_string(),
            );
        }

        Ok(())
    }
}

/// Execution outcome returned by a Worker upon task completion or termination.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskResult {
    /// Worker UUID that executed the task.
    pub worker_id: Uuid,
    /// Task ID that was executed.
    pub task_id: TaskId,
    /// Process exit code (0 indicates success).
    pub exit_code: i32,
    /// Captured standard output.
    pub stdout: String,
    /// Captured standard error.
    pub stderr: String,
    /// Execution duration in milliseconds.
    pub execution_time_ms: u64,
    /// Whether the task was executed on a physical or simulated GPU.
    pub is_gpu_executed: bool,
    /// Optional error message if execution or sandboxing encountered an error.
    pub error: Option<String>,
}

impl TaskResult {
    /// Returns true if execution completed with zero exit code and no framework error.
    #[inline]
    pub fn is_success(&self) -> bool {
        self.exit_code == 0 && self.error.is_none()
    }

    /// Convenience constructor for successful execution.
    pub fn success(
        worker_id: Uuid,
        task_id: TaskId,
        stdout: impl Into<String>,
        execution_time_ms: u64,
        is_gpu_executed: bool,
    ) -> Self {
        Self {
            worker_id,
            task_id,
            exit_code: 0,
            stdout: stdout.into(),
            stderr: String::new(),
            execution_time_ms,
            is_gpu_executed,
            error: None,
        }
    }

    /// Convenience constructor for failed execution.
    pub fn failure(
        worker_id: Uuid,
        task_id: TaskId,
        exit_code: i32,
        stdout: impl Into<String>,
        stderr: impl Into<String>,
        execution_time_ms: u64,
        error: Option<String>,
    ) -> Self {
        Self {
            worker_id,
            task_id,
            exit_code,
            stdout: stdout.into(),
            stderr: stderr.into(),
            execution_time_ms,
            is_gpu_executed: false,
            error,
        }
    }
}

/// Finite State Machine representation of a task's lifecycle on the Master.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TaskStatus {
    /// Submitted to Master and waiting in queue for available eligible worker.
    Queued,
    /// Selected by scheduler and dispatched to a worker.
    Assigned,
    /// Worker has acknowledged receipt and started execution.
    Running,
    /// Successfully finished execution (exit code 0).
    Completed,
    /// Execution failed (non-zero exit code or process error).
    Failed,
    /// Failed or orphaned by worker disconnect; re-queued for retry.
    Retried,
    /// Execution exceeded timeout threshold and was cancelled.
    TimedOut,
    /// Explicitly cancelled by user or client RPC.
    Cancelled,
}

impl TaskStatus {
    /// Returns true if this state is terminal (no further transitions possible).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskStatus::Completed
                | TaskStatus::Failed
                | TaskStatus::TimedOut
                | TaskStatus::Cancelled
        )
    }

    /// Returns true if the task is actively assigned or running on a worker.
    pub fn is_active(&self) -> bool {
        matches!(self, TaskStatus::Assigned | TaskStatus::Running)
    }

    /// Enforces the valid state transitions of the Master FSM.
    pub fn can_transition_to(&self, next: TaskStatus) -> bool {
        match self {
            TaskStatus::Queued => matches!(next, TaskStatus::Assigned | TaskStatus::Cancelled),
            TaskStatus::Assigned => matches!(
                next,
                TaskStatus::Running
                    | TaskStatus::Queued
                    | TaskStatus::Retried
                    | TaskStatus::Failed
                    | TaskStatus::TimedOut
                    | TaskStatus::Cancelled
            ),
            TaskStatus::Running => matches!(
                next,
                TaskStatus::Completed
                    | TaskStatus::Failed
                    | TaskStatus::Retried
                    | TaskStatus::TimedOut
                    | TaskStatus::Cancelled
            ),
            TaskStatus::Retried => matches!(next, TaskStatus::Queued | TaskStatus::Failed),
            // Terminal states cannot transition to anything
            TaskStatus::Completed
            | TaskStatus::Failed
            | TaskStatus::TimedOut
            | TaskStatus::Cancelled => false,
        }
    }
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_task_id_serde_transparent() {
        let raw_uuid = Uuid::new_v4();
        let task_id = TaskId::from_uuid(raw_uuid);

        let json = serde_json::to_string(&task_id).unwrap();
        // Because of #[serde(transparent)], it should be serialized directly as a quoted UUID string
        assert_eq!(json, format!("\"{}\"", raw_uuid));

        let deserialized: TaskId = serde_json::from_str(&json).unwrap();
        assert_eq!(task_id, deserialized);
        assert_eq!(task_id.to_string(), raw_uuid.to_string());
    }

    #[test]
    fn test_task_spec_validation() {
        // 1. Empty command
        let invalid_cmd = TaskSpec::Command {
            program: "".into(),
            args: vec![],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        };
        assert!(invalid_cmd.validate().is_err());

        // 2. Valid command
        let valid_cmd = TaskSpec::Command {
            program: "ls".into(),
            args: vec!["-la".into()],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        };
        assert!(valid_cmd.validate().is_ok());

        // 3. Empty Rust compilation crate name
        let invalid_rust = TaskSpec::RustCompilation {
            crate_name: "".into(),
            source_files: HashMap::new(),
            compiler_flags: vec![],
            target_dir: None,
        };
        assert!(invalid_rust.validate().is_err());

        // 4. Rust compilation without source files
        let no_sources_rust = TaskSpec::RustCompilation {
            crate_name: "my_crate".into(),
            source_files: HashMap::new(),
            compiler_flags: vec![],
            target_dir: None,
        };
        assert!(no_sources_rust.validate().is_err());

        // 5. Valid Rust compilation
        let mut sources = HashMap::new();
        sources.insert("Cargo.toml".into(), "[package]\nname = \"foo\"".into());
        let valid_rust = TaskSpec::RustCompilation {
            crate_name: "my_crate".into(),
            source_files: sources,
            compiler_flags: vec!["--release".into()],
            target_dir: None,
        };
        assert!(valid_rust.validate().is_ok());
    }

    #[test]
    fn test_task_gpu_mismatch_validation() {
        // Spec requires GPU, but requirements state gpu_required = false
        let gpu_spec = TaskSpec::GpuCompute {
            kernel_name: "matrix_mult".into(),
            input_data: vec![1, 2, 3],
            work_group_size: 16,
            simulated_matrix_dim: 64,
            compute_intensity: 10,
        };
        let bad_requirements = TaskRequirements::generic(1, 60);

        let task = Task::new(gpu_spec.clone(), bad_requirements);
        assert!(
            task.validate().is_err(),
            "Should error when GPU spec lacks gpu_required"
        );

        let good_requirements = TaskRequirements::gpu(60);
        let valid_task = Task::new(gpu_spec, good_requirements);
        assert!(valid_task.validate().is_ok());
    }

    #[test]
    fn test_task_spec_serde_aliases_and_defaults() {
        // Test "command" alias deserializes into `program`
        let json_cmd = r#"{"type":"Command","command":"echo","args":["hi"]}"#;
        let spec: TaskSpec = serde_json::from_str(json_cmd).unwrap();
        match spec {
            TaskSpec::Command {
                program,
                args,
                env,
                working_dir,
                stdin,
            } => {
                assert_eq!(program, "echo");
                assert_eq!(args, vec!["hi"]);
                assert!(env.is_empty());
                assert!(working_dir.is_none());
                assert!(stdin.is_none());
            }
            _ => panic!("Expected Command variant"),
        }

        // Test "cargo_args" alias deserializes into `compiler_flags`
        let json_rust = r#"{"type":"RustCompilation","crate_name":"foo","source_files":{"src/lib.rs":"fn f(){}"},"cargo_args":["--release"]}"#;
        let spec_rust: TaskSpec = serde_json::from_str(json_rust).unwrap();
        match spec_rust {
            TaskSpec::RustCompilation {
                crate_name,
                compiler_flags,
                target_dir,
                ..
            } => {
                assert_eq!(crate_name, "foo");
                assert_eq!(compiler_flags, vec!["--release"]);
                assert!(target_dir.is_none());
            }
            _ => panic!("Expected RustCompilation variant"),
        }

        // Test GpuCompute defaults
        let json_gpu = r#"{"type":"GpuCompute","kernel_name":"test"}"#;
        let spec_gpu: TaskSpec = serde_json::from_str(json_gpu).unwrap();
        match spec_gpu {
            TaskSpec::GpuCompute {
                kernel_name,
                work_group_size,
                simulated_matrix_dim,
                compute_intensity,
                ..
            } => {
                assert_eq!(kernel_name, "test");
                assert_eq!(work_group_size, 16);
                assert_eq!(simulated_matrix_dim, 64);
                assert_eq!(compute_intensity, 0);
            }
            _ => panic!("Expected GpuCompute variant"),
        }

        // Test BuiltinTest defaults
        let json_builtin = r#"{"type":"BuiltinTest"}"#;
        let spec_builtin: TaskSpec = serde_json::from_str(json_builtin).unwrap();
        match spec_builtin {
            TaskSpec::BuiltinTest {
                test_name,
                iterations,
                duration_ms,
                should_fail,
                require_gpu,
            } => {
                assert_eq!(test_name, "hash_compute");
                assert_eq!(iterations, 100_000);
                assert_eq!(duration_ms, 0);
                assert!(!should_fail);
                assert!(!require_gpu);
            }
            _ => panic!("Expected BuiltinTest variant"),
        }
    }

    #[test]
    fn test_task_status_transitions() {
        let q = TaskStatus::Queued;
        assert!(q.can_transition_to(TaskStatus::Assigned));
        assert!(q.can_transition_to(TaskStatus::Cancelled));
        assert!(!q.can_transition_to(TaskStatus::Running));
        assert!(!q.can_transition_to(TaskStatus::Completed));

        let a = TaskStatus::Assigned;
        assert!(a.can_transition_to(TaskStatus::Running));
        assert!(a.can_transition_to(TaskStatus::Retried));
        assert!(a.can_transition_to(TaskStatus::Failed));
        assert!(a.can_transition_to(TaskStatus::TimedOut));

        let r = TaskStatus::Running;
        assert!(r.can_transition_to(TaskStatus::Completed));
        assert!(r.can_transition_to(TaskStatus::Failed));
        assert!(r.can_transition_to(TaskStatus::TimedOut));
        assert!(!r.can_transition_to(TaskStatus::Assigned));

        let c = TaskStatus::Completed;
        assert!(c.is_terminal());
        assert!(!c.can_transition_to(TaskStatus::Queued));
        assert!(!c.can_transition_to(TaskStatus::Running));
    }

    #[test]
    fn test_task_result_helpers() {
        let worker_id = Uuid::new_v4();
        let task_id = TaskId::new();

        let success = TaskResult::success(worker_id, task_id, "output", 150, true);
        assert!(success.is_success());
        assert!(success.is_gpu_executed);
        assert_eq!(success.exit_code, 0);

        let failure = TaskResult::failure(
            worker_id,
            task_id,
            1,
            "",
            "syntax error",
            200,
            Some("Failed execution".into()),
        );
        assert!(!failure.is_success());
        assert_eq!(failure.exit_code, 1);
    }

    #[test]
    fn test_task_serialization_roundtrip() {
        let spec = TaskSpec::ShellScript {
            script: "echo 'hello grid'".into(),
            interpreter: None,
            env: HashMap::new(),
        };
        let req = TaskRequirements::generic(2, 30);
        let task = Task::new(spec, req).with_tags(vec!["test".into(), "smoke".into()]);

        let json = serde_json::to_string(&task).unwrap();
        let deserialized: Task = serde_json::from_str(&json).unwrap();

        assert_eq!(task, deserialized);
    }

    #[test]
    fn test_task_bincode_serialization_roundtrip() {
        let specs = vec![
            TaskSpec::new_command("cargo", vec!["build".into(), "--release".into()]),
            TaskSpec::new_shell_script("echo 'testing bincode'"),
            TaskSpec::RustCompilation {
                crate_name: "test_crate".into(),
                source_files: [("src/lib.rs".to_string(), "pub fn f() {}".to_string())]
                    .into_iter()
                    .collect(),
                compiler_flags: vec!["--crate-type".into(), "lib".into()],
                target_dir: None,
            },
            TaskSpec::GpuCompute {
                kernel_name: "gemm".into(),
                input_data: vec![1, 2, 3, 4, 5],
                work_group_size: 32,
                simulated_matrix_dim: 128,
                compute_intensity: 50,
            },
            TaskSpec::BuiltinTest {
                test_name: "quick_test".into(),
                iterations: 500,
                duration_ms: 10,
                should_fail: false,
                require_gpu: true,
            },
        ];

        for spec in specs {
            let req = TaskRequirements::generic(4, 60);
            let task = Task::new(spec, req).with_tags(vec!["bincode".into()]);

            let encoded = bincode::serialize(&task).expect("bincode serialization failed");
            let decoded: Task =
                bincode::deserialize(&encoded).expect("bincode deserialization failed");

            assert_eq!(task, decoded);
        }
    }

    #[test]
    fn test_task_requirements_max_retries() {
        let req_default = TaskRequirements::default();
        assert_eq!(req_default.max_retries, None);

        let req_custom = TaskRequirements::generic(2, 30).with_max_retries(5);
        assert_eq!(req_custom.max_retries, Some(5));

        // Test JSON roundtrip preserves max_retries
        let json = serde_json::to_string(&req_custom).unwrap();
        let decoded: TaskRequirements = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.max_retries, Some(5));

        // Test backward compatibility: JSON without max_retries field deserializes to None
        let legacy_json = r#"{"cpu_cores":1,"ram_mb":512,"gpu_required":false,"timeout_secs":60}"#;
        let decoded_legacy: TaskRequirements = serde_json::from_str(legacy_json).unwrap();
        assert_eq!(decoded_legacy.max_retries, None);
    }
}
