//! AgentGridBridge translating Layer 3 Agent Mesh envelopes into Layer 1 Master Grid tasks.
//!
//! Supports both an embedded in-process Master (`--grid-master` via [`InProcessBackend`]) and a
//! separately running remote Master (`--grid-connect` via [`crate::RemoteMasterBackend`]), enabling
//! compute offloading, distributed compilation, staged files/artifacts, 3D/AutoRig, and cluster
//! telemetry inspection.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::placement::{
    validate_task_user_tags, IsolationLevel, Placement, REQUIRE_PREFIX,
};
use rusty_grid_core::task::{
    validate_relative_sandbox_path, AutoRigSpec, Bytes, Engine3D, Generative3DSpec, StagedFile,
    Task, TaskId, TaskRequirements, TaskResult, TaskSpec, MAX_AGGREGATE_STAGED_BYTES,
    MAX_STAGED_FILE_BYTES,
};
use rusty_grid_master::server::MasterHandle;

use crate::grid_backend::{GridBackend, GridTaskResult, InProcessBackend};
use crate::protocol::{compute_sha256, AgentMeshEnvelope};

/// Bridge connecting `AgentMeshHub` to a Layer 1 Master Grid via [`GridBackend`].
#[derive(Clone)]
pub struct AgentGridBridge {
    backend: Arc<dyn GridBackend>,
    default_timeout: Duration,
    health: Arc<std::sync::Mutex<HealthCache>>,
}

/// How long a grid liveness result is reused by [`AgentGridBridge::catalog_health`].
pub const GRID_HEALTH_TTL: Duration = Duration::from_secs(5);

#[derive(Default)]
struct HealthCache {
    last: Option<(std::time::Instant, bool)>,
    refreshing: bool,
}

impl AgentGridBridge {
    /// Creates a new `AgentGridBridge` wrapping an in-process `MasterHandle`.
    pub fn new(master: MasterHandle) -> Self {
        Self::with_backend(Arc::new(InProcessBackend::new(master)))
    }

    /// Creates a new `AgentGridBridge` backed by an arbitrary [`GridBackend`].
    pub fn with_backend(backend: Arc<dyn GridBackend>) -> Self {
        Self {
            backend,
            default_timeout: Duration::from_secs(60),
            health: Arc::new(std::sync::Mutex::new(HealthCache::default())),
        }
    }

    /// Configures default execution timeout for bridged tasks.
    pub fn with_default_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = timeout;
        self
    }

    /// Returns a reference to the underlying [`GridBackend`].
    pub fn backend(&self) -> &Arc<dyn GridBackend> {
        &self.backend
    }

    /// Performs a bounded liveness check against the attached Master Grid backend.
    pub async fn health_check(&self) -> GridResult<()> {
        self.backend.health_check().await
    }

    /// Grid liveness for node catalogs (`/api/nodes`, `NodeList`), which must not wait on a
    /// master that is down (MESH-3, design 0104). The first call checks once; afterwards a result
    /// younger than [`GRID_HEALTH_TTL`] is reused, and a stale one is returned while a single
    /// background check refreshes it.
    pub async fn catalog_health(&self) -> bool {
        let now = std::time::Instant::now();
        let (known, refresh) = {
            let mut cache = self.health.lock().unwrap_or_else(|e| e.into_inner());
            let fresh = cache
                .last
                .is_some_and(|(at, _)| now.saturating_duration_since(at) < GRID_HEALTH_TTL);
            let refresh = !fresh && !cache.refreshing;
            if refresh {
                cache.refreshing = true;
            }
            (cache.last.map(|(_, ok)| ok), refresh)
        };
        let update = |health: &std::sync::Mutex<HealthCache>, ok: bool| {
            let mut cache = health.lock().unwrap_or_else(|e| e.into_inner());
            cache.last = Some((std::time::Instant::now(), ok));
            cache.refreshing = false;
        };
        match (known, refresh) {
            (None, true) => {
                let ok = self.backend.health_check().await.is_ok();
                update(&self.health, ok);
                ok
            }
            (Some(ok), true) => {
                let backend = Arc::clone(&self.backend);
                let health = Arc::clone(&self.health);
                tokio::spawn(async move {
                    let ok = backend.health_check().await.is_ok();
                    update(&health, ok);
                });
                ok
            }
            (Some(ok), false) => ok,
            // Another caller is running the first check: do not wait for it.
            (None, false) => false,
        }
    }

    /// Closes any persistent transport resources held by the backend.
    pub async fn close(&self) {
        self.backend.close().await;
    }

    /// Handles an incoming `CommandRequest` envelope, executing the grid task and returning `CommandResponse`.
    pub async fn handle_command(&self, req: &AgentMeshEnvelope) -> AgentMeshEnvelope {
        match req {
            AgentMeshEnvelope::CommandRequest {
                correlation_id,
                from,
                command,
                args,
                timeout_ms,
                ..
            } => {
                let timeout = timeout_ms
                    .map(Duration::from_millis)
                    .unwrap_or(self.default_timeout);
                self.dispatch_command(command, args, from, correlation_id, timeout)
                    .await
            }
            other => AgentMeshEnvelope::new_command_response(
                "grid",
                other.from_node(),
                other.correlation_id(),
                None,
                "failed",
                1,
                "",
                "Expected CommandRequest envelope",
                0,
            ),
        }
    }

    /// Dispatches a grid command by name and arguments.
    pub async fn execute_command(
        &self,
        command: &str,
        args: &Value,
        from: &str,
        correlation_id: &str,
    ) -> AgentMeshEnvelope {
        self.dispatch_command(command, args, from, correlation_id, self.default_timeout)
            .await
    }

    async fn dispatch_command(
        &self,
        command: &str,
        args: &Value,
        from: &str,
        correlation_id: &str,
        timeout: Duration,
    ) -> AgentMeshEnvelope {
        match command {
            "grid_compute" | "compute" => {
                self.handle_compute(command, args, from, correlation_id, timeout)
                    .await
            }
            "grid_compile" | "grid_compilation" | "compile" => {
                self.handle_compile(command, args, from, correlation_id, timeout)
                    .await
            }
            "grid_submit" | "submit" => {
                self.handle_submit(command, args, from, correlation_id, timeout)
                    .await
            }
            "grid_status" | "status" => {
                self.handle_status(command, args, from, correlation_id)
                    .await
            }
            "grid_result" | "result" => {
                self.handle_result(command, args, from, correlation_id, timeout)
                    .await
            }
            "grid_cancel" | "cancel" => {
                self.handle_cancel(command, args, from, correlation_id)
                    .await
            }
            "grid_3d_generate" | "3d_generate" | "generate_3d" => {
                self.handle_3d_generate(command, args, from, correlation_id, timeout)
                    .await
            }
            "grid_auto_rig" | "auto_rig" => {
                self.handle_auto_rig(command, args, from, correlation_id, timeout)
                    .await
            }
            unknown => make_error_response(
                from,
                correlation_id,
                unknown,
                127,
                &format!("Unsupported grid command: '{}'", unknown),
            ),
        }
    }

    async fn handle_compute(
        &self,
        command: &str,
        args: &Value,
        from: &str,
        correlation_id: &str,
        timeout: Duration,
    ) -> AgentMeshEnvelope {
        let timeout_secs = timeout.as_secs().max(1);

        let (spec, reqs) = if let Some(test_name) = args.get("test_name").and_then(|v| v.as_str()) {
            let iterations = args
                .get("iterations")
                .and_then(|v| v.as_u64())
                .unwrap_or(100_000) as u32;
            let duration_ms = args
                .get("duration_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let should_fail = args
                .get("should_fail")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let require_gpu = args
                .get("require_gpu")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            (
                TaskSpec::BuiltinTest {
                    test_name: test_name.to_string(),
                    iterations,
                    duration_ms,
                    should_fail,
                    require_gpu,
                },
                TaskRequirements {
                    cpu_cores: 1,
                    ram_mb: 512,
                    gpu_required: require_gpu,
                    timeout_secs,
                    max_retries: None,
                    vram_mb: None,
                    extensions: Vec::new(),
                },
            )
        } else {
            let kernel_name = args
                .get("kernel")
                .or_else(|| args.get("kernel_name"))
                .and_then(|v| v.as_str())
                .unwrap_or("gemm")
                .to_string();

            let simulated_matrix_dim = args
                .get("matrix_dim")
                .or_else(|| args.get("simulated_matrix_dim"))
                .and_then(|v| v.as_u64())
                .unwrap_or(64) as u32;

            let work_group_size = args
                .get("work_group_size")
                .and_then(|v| v.as_u64())
                .unwrap_or(16) as u32;

            let input_data = parse_input_bytes(args.get("input_data"));

            let gpu_required = args
                .get("gpu_required")
                .or_else(|| args.get("require_gpu"))
                .and_then(|v| v.as_bool())
                .unwrap_or(true);

            (
                TaskSpec::gpu_compute_with_params(
                    kernel_name,
                    input_data,
                    work_group_size,
                    simulated_matrix_dim,
                ),
                TaskRequirements {
                    cpu_cores: 1,
                    ram_mb: 1024,
                    gpu_required,
                    timeout_secs,
                    max_retries: None,
                    vram_mb: None,
                    extensions: Vec::new(),
                },
            )
        };

        let priority = args.get("priority").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
        let task = match apply_tags_and_placement(Task::new(spec, reqs), args, "grid_compute") {
            Ok(t) => t,
            Err(msg) => return make_error_response(from, correlation_id, command, 1, &msg),
        };

        match self.backend.submit_and_wait(task, priority, timeout).await {
            Ok(task_res) => map_task_result_to_response(task_res, from, correlation_id, command),
            Err(GridError::Timeout { .. }) => make_error_response(
                from,
                correlation_id,
                command,
                124,
                "Compute execution timed out",
            ),
            Err(e) => make_error_response(
                from,
                correlation_id,
                command,
                1,
                &format!("Compute task failed: {e}"),
            ),
        }
    }

    async fn handle_compile(
        &self,
        command: &str,
        args: &Value,
        from: &str,
        correlation_id: &str,
        timeout: Duration,
    ) -> AgentMeshEnvelope {
        let crate_name = args
            .get("crate_name")
            .and_then(|v| v.as_str())
            .unwrap_or("agent_crate")
            .to_string();

        let source_files = parse_source_files(args.get("source_files"));
        if source_files.is_empty() {
            return make_error_response(
                from,
                correlation_id,
                command,
                1,
                "Rust compilation requires at least one source file in 'source_files'",
            );
        }

        let compiler_flags = parse_compiler_flags(
            args.get("compiler_flags")
                .or_else(|| args.get("cargo_args")),
        );

        let timeout_secs = timeout.as_secs().max(1);
        let spec = TaskSpec::RustCompilation {
            crate_name,
            source_files,
            compiler_flags,
            target_dir: None,
        };
        let reqs = TaskRequirements::generic(2, timeout_secs);
        let priority = args.get("priority").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
        let task = match apply_tags_and_placement(Task::new(spec, reqs), args, "grid_compile") {
            Ok(t) => t,
            Err(msg) => return make_error_response(from, correlation_id, command, 1, &msg),
        };

        match self.backend.submit_and_wait(task, priority, timeout).await {
            Ok(task_res) => map_task_result_to_response(task_res, from, correlation_id, command),
            Err(GridError::Timeout { .. }) => make_error_response(
                from,
                correlation_id,
                command,
                124,
                "Compilation failed or timed out",
            ),
            Err(e) => make_error_response(
                from,
                correlation_id,
                command,
                1,
                &format!("Compilation task failed: {e}"),
            ),
        }
    }

    async fn handle_submit(
        &self,
        command: &str,
        args: &Value,
        from: &str,
        correlation_id: &str,
        timeout: Duration,
    ) -> AgentMeshEnvelope {
        if let Some(msg) = reject_unsupported_workspace(args) {
            return make_error_response(from, correlation_id, command, 1, &msg);
        }

        let timeout_secs = timeout.as_secs().max(1);

        let spec_res: Result<TaskSpec, _> = if let Some(spec_val) = args.get("spec") {
            serde_json::from_value(spec_val.clone())
        } else if let Some(prog) = args.get("program").and_then(|v| v.as_str()) {
            let cmd_args: Vec<String> = match args.get("args") {
                None | Some(Value::Null) => Vec::new(),
                Some(Value::Array(a)) => {
                    let mut collected = Vec::with_capacity(a.len());
                    for item in a {
                        match item.as_str() {
                            Some(s) => collected.push(s.to_string()),
                            None => {
                                return make_error_response(
                                    from,
                                    correlation_id,
                                    command,
                                    1,
                                    "Invalid task specification: 'args' elements must be strings",
                                );
                            }
                        }
                    }
                    collected
                }
                Some(_) => {
                    return make_error_response(
                        from,
                        correlation_id,
                        command,
                        1,
                        "Invalid task specification: 'args' must be an array of strings",
                    );
                }
            };
            Ok(TaskSpec::new_command(prog, cmd_args))
        } else if let Some(script) = args.get("script").and_then(|v| v.as_str()) {
            Ok(TaskSpec::new_shell_script(script))
        } else {
            Err(serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Missing 'spec', 'program', or 'script' argument",
            )))
        };

        let spec = match spec_res {
            Ok(s) => s,
            Err(e) => {
                return make_error_response(
                    from,
                    correlation_id,
                    command,
                    1,
                    &format!("Invalid task specification: {e}"),
                );
            }
        };

        if let Err(e) = spec.validate() {
            return make_error_response(
                from,
                correlation_id,
                command,
                1,
                &format!("Invalid task specification: {e}"),
            );
        }

        let reqs = if let Some(reqs_val) = args.get("requirements").filter(|v| !v.is_null()) {
            match serde_json::from_value::<TaskRequirements>(reqs_val.clone()) {
                Ok(r) => r,
                Err(e) => {
                    return make_error_response(
                        from,
                        correlation_id,
                        command,
                        1,
                        &format!("Invalid task requirements: {e}"),
                    );
                }
            }
        } else {
            TaskRequirements::generic(1, timeout_secs)
        };

        let staged_files =
            match parse_inline_staged_files(args.get("files").or_else(|| args.get("staged_files")))
            {
                Ok(f) => f,
                Err(msg) => return make_error_response(from, correlation_id, command, 1, &msg),
            };
        let expected_outputs = match parse_expected_outputs(
            args.get("outputs").or_else(|| args.get("expected_outputs")),
        ) {
            Ok(o) => o,
            Err(msg) => return make_error_response(from, correlation_id, command, 1, &msg),
        };
        if (!staged_files.is_empty() || !expected_outputs.is_empty())
            && !matches!(
                spec,
                TaskSpec::Command { .. }
                    | TaskSpec::ShellScript { .. }
                    | TaskSpec::RustCompilation { .. }
            )
        {
            return make_error_response(
                from,
                correlation_id,
                command,
                1,
                "Staged 'files' and 'outputs' are only supported for Command, ShellScript, and RustCompilation tasks",
            );
        }

        let wait = match parse_optional_bool(args.get("wait"), "wait") {
            Ok(b) => b.unwrap_or(true),
            Err(msg) => return make_error_response(from, correlation_id, command, 1, &msg),
        };
        let priority = args.get("priority").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
        let mut task = Task::new(spec, reqs);
        task.staged_files = staged_files;
        task.expected_outputs = expected_outputs;
        let task = match apply_tags_and_placement(task, args, "grid_submit") {
            Ok(t) => t,
            Err(msg) => return make_error_response(from, correlation_id, command, 1, &msg),
        };

        if wait {
            match self.backend.submit_and_wait(task, priority, timeout).await {
                Ok(task_res) => {
                    map_task_result_to_response(task_res, from, correlation_id, command)
                }
                Err(GridError::Timeout { .. }) => make_error_response(
                    from,
                    correlation_id,
                    command,
                    124,
                    "Task execution failed or timed out",
                ),
                Err(e) => make_error_response(
                    from,
                    correlation_id,
                    command,
                    1,
                    &format!("Task execution failed: {e}"),
                ),
            }
        } else {
            match self.backend.submit_task(task, priority).await {
                Ok(task_id) => {
                    let mut resp = AgentMeshEnvelope::new_command_response(
                        "grid",
                        from,
                        correlation_id,
                        Some(command.to_string()),
                        "success",
                        0,
                        format!("Task submitted successfully with ID: {task_id}"),
                        "",
                        0,
                    );
                    if let AgentMeshEnvelope::CommandResponse {
                        ref mut payload, ..
                    } = resp
                    {
                        *payload = Some(serde_json::json!({
                            "task_id": task_id.to_string(),
                            "status": "submitted"
                        }));
                    }
                    resp
                }
                Err(e) => make_error_response(
                    from,
                    correlation_id,
                    command,
                    1,
                    &format!("Failed to submit task: {e}"),
                ),
            }
        }
    }

    async fn handle_status(
        &self,
        command: &str,
        args: &Value,
        from: &str,
        correlation_id: &str,
    ) -> AgentMeshEnvelope {
        if let Some(task_id_str) = args.get("task_id").and_then(|v| v.as_str()) {
            match uuid::Uuid::parse_str(task_id_str) {
                Ok(uuid) => {
                    let task_id = TaskId::from_uuid(uuid);
                    match self.backend.get_task_status(task_id).await {
                        Ok(st) => {
                            let task_json = st.task_info.unwrap_or_else(|| {
                                serde_json::json!({
                                    "task_id": st.task_id.to_string(),
                                    "status": format!("{:?}", st.status),
                                    "state": st.state_name,
                                    "assigned_worker": st.assigned_worker.map(|u| u.to_string()),
                                    "error": st.error,
                                })
                            });
                            let payload = serde_json::json!({
                                "task": task_json,
                                "checkpoint": st.checkpoint,
                            });
                            let stdout = serde_json::to_string_pretty(&payload).unwrap_or_default();
                            let mut resp = AgentMeshEnvelope::new_command_response(
                                "grid",
                                from,
                                correlation_id,
                                Some(command.to_string()),
                                "success",
                                0,
                                stdout,
                                "",
                                0,
                            );
                            if let AgentMeshEnvelope::CommandResponse {
                                payload: ref mut p, ..
                            } = resp
                            {
                                *p = Some(payload);
                            }
                            resp
                        }
                        Err(GridError::TaskNotFound(_)) => make_error_response(
                            from,
                            correlation_id,
                            command,
                            1,
                            &format!("Task not found: {task_id}"),
                        ),
                        Err(e) => make_error_response(
                            from,
                            correlation_id,
                            command,
                            1,
                            &format!("Failed to retrieve task status: {e}"),
                        ),
                    }
                }
                Err(e) => make_error_response(
                    from,
                    correlation_id,
                    command,
                    1,
                    &format!("Invalid task UUID: {e}"),
                ),
            }
        } else {
            match self.backend.cluster_status().await {
                Ok(cs) => {
                    let payload = serde_json::json!({
                        "queue": cs.queue_json,
                        "workers": cs.workers_json,
                        "active_workers": cs.active_workers,
                        "total_workers": cs.total_workers,
                    });
                    let stdout = format!(
                        "Grid Cluster Status:\nActive Workers: {}\nQueued Tasks: {}\nRunning Tasks: {}\nCompleted Tasks: {}\nFailed Tasks: {}\nCancelled Tasks: {}\n",
                        cs.active_workers,
                        cs.queued_tasks,
                        cs.running_tasks,
                        cs.completed_tasks,
                        cs.failed_tasks,
                        cs.cancelled_tasks
                    );
                    let mut resp = AgentMeshEnvelope::new_command_response(
                        "grid",
                        from,
                        correlation_id,
                        Some(command.to_string()),
                        "success",
                        0,
                        stdout,
                        "",
                        0,
                    );
                    if let AgentMeshEnvelope::CommandResponse {
                        payload: ref mut p, ..
                    } = resp
                    {
                        *p = Some(payload);
                    }
                    resp
                }
                Err(e) => make_error_response(
                    from,
                    correlation_id,
                    command,
                    1,
                    &format!("Failed to retrieve cluster status: {e}"),
                ),
            }
        }
    }

    async fn handle_result(
        &self,
        command: &str,
        args: &Value,
        from: &str,
        correlation_id: &str,
        timeout: Duration,
    ) -> AgentMeshEnvelope {
        let task_id = match parse_task_id_arg(args) {
            Ok(id) => id,
            Err(msg) => return make_error_response(from, correlation_id, command, 1, &msg),
        };
        let wait = match parse_optional_bool(args.get("wait"), "wait") {
            Ok(b) => b.unwrap_or(false),
            Err(msg) => return make_error_response(from, correlation_id, command, 1, &msg),
        };

        if wait {
            match self.backend.wait_task(task_id, timeout).await {
                Ok(res) => map_task_result_to_response(res, from, correlation_id, command),
                Err(GridError::TaskNotFound(_)) => make_error_response(
                    from,
                    correlation_id,
                    command,
                    1,
                    &format!("Task not found: {task_id}"),
                ),
                Err(GridError::Timeout { .. }) => make_error_response(
                    from,
                    correlation_id,
                    command,
                    124,
                    &format!("Timed out waiting for task {task_id}"),
                ),
                Err(e) => make_error_response(
                    from,
                    correlation_id,
                    command,
                    1,
                    &format!("Failed to retrieve task result: {e}"),
                ),
            }
        } else {
            match self.backend.get_task_result(task_id).await {
                Ok(GridTaskResult::Completed(res)) => {
                    map_task_result_to_response(res, from, correlation_id, command)
                }
                Ok(GridTaskResult::Pending(st)) => {
                    let payload = serde_json::json!({
                        "task_id": st.task_id.to_string(),
                        "completed": false,
                        "status": format!("{:?}", st.status),
                        "state": st.state_name,
                        "assigned_worker": st.assigned_worker.map(|u| u.to_string()),
                        "error": st.error,
                    });
                    let stdout = format!("Task {} is {}", st.task_id, st.state_name);
                    let mut resp = AgentMeshEnvelope::new_command_response(
                        "grid",
                        from,
                        correlation_id,
                        Some(command.to_string()),
                        "success",
                        0,
                        stdout,
                        "",
                        0,
                    );
                    if let AgentMeshEnvelope::CommandResponse {
                        payload: ref mut p, ..
                    } = resp
                    {
                        *p = Some(payload);
                    }
                    resp
                }
                Err(GridError::TaskNotFound(_)) => make_error_response(
                    from,
                    correlation_id,
                    command,
                    1,
                    &format!("Task not found: {task_id}"),
                ),
                Err(e) => make_error_response(
                    from,
                    correlation_id,
                    command,
                    1,
                    &format!("Failed to retrieve task result: {e}"),
                ),
            }
        }
    }

    async fn handle_cancel(
        &self,
        command: &str,
        args: &Value,
        from: &str,
        correlation_id: &str,
    ) -> AgentMeshEnvelope {
        let task_id = match parse_task_id_arg(args) {
            Ok(id) => id,
            Err(msg) => return make_error_response(from, correlation_id, command, 1, &msg),
        };
        match self.backend.cancel_task(task_id).await {
            Ok(cancelled) => {
                let payload = serde_json::json!({
                    "task_id": task_id.to_string(),
                    "cancelled": cancelled,
                });
                let stdout = if cancelled {
                    format!("Task {task_id} cancelled")
                } else {
                    format!("Task {task_id} was not cancelled (already finished or unknown)")
                };
                let mut resp = AgentMeshEnvelope::new_command_response(
                    "grid",
                    from,
                    correlation_id,
                    Some(command.to_string()),
                    "success",
                    0,
                    stdout,
                    "",
                    0,
                );
                if let AgentMeshEnvelope::CommandResponse {
                    payload: ref mut p, ..
                } = resp
                {
                    *p = Some(payload);
                }
                resp
            }
            Err(e) => make_error_response(
                from,
                correlation_id,
                command,
                1,
                &format!("Failed to cancel task {task_id}: {e}"),
            ),
        }
    }

    async fn handle_3d_generate(
        &self,
        command: &str,
        args: &Value,
        from: &str,
        correlation_id: &str,
        timeout: Duration,
    ) -> AgentMeshEnvelope {
        let prompt = args
            .get("prompt")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let image_bytes = match parse_binary_data(
            args.get("image")
                .or_else(|| args.get("image_data"))
                .or_else(|| args.get("image_bytes")),
            "image",
        ) {
            Ok(bytes) => bytes,
            Err(msg) => return make_error_response(from, correlation_id, command, 1, &msg),
        };

        if prompt.is_none() && image_bytes.is_empty() {
            return make_error_response(
                from,
                correlation_id,
                command,
                1,
                "3D generation requires either non-empty 'prompt' or 'image'",
            );
        }

        let quality = args
            .get("quality")
            .and_then(|v| v.as_str())
            .unwrap_or("standard")
            .to_lowercase();

        if let Some(v) = args.get("engine") {
            if !v.is_string() {
                return make_error_response(
                    from,
                    correlation_id,
                    command,
                    1,
                    "Invalid 3D generation specification: 'engine' must be a string",
                );
            }
        }
        if let Some(v) = args.get("engine_preference") {
            if !v.is_string() {
                return make_error_response(
                    from,
                    correlation_id,
                    command,
                    1,
                    "Invalid 3D generation specification: 'engine_preference' must be a string",
                );
            }
        }

        let engine_preference = if let Some(raw) = args
            .get("engine")
            .or_else(|| args.get("engine_preference"))
            .and_then(|v| v.as_str())
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            match raw.parse::<Engine3D>() {
                Ok(Engine3D::Auto) => None,
                Ok(engine) => Some(engine.as_str().to_string()),
                Err(_) => {
                    return make_error_response(
                        from,
                        correlation_id,
                        command,
                        1,
                        &format!("Invalid 3D generation specification: unknown 3D engine '{raw}'"),
                    );
                }
            }
        } else {
            None
        };

        let export_texture = args
            .get("export_texture")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let spec = TaskSpec::Generative3D(Generative3DSpec {
            prompt,
            image_data: if image_bytes.is_empty() {
                None
            } else {
                Some(Bytes::from(image_bytes))
            },
            quality,
            engine_preference,
            export_texture,
        });

        if let Err(e) = spec.validate() {
            return make_error_response(
                from,
                correlation_id,
                command,
                1,
                &format!("Invalid 3D generation specification: {e}"),
            );
        }

        let timeout_secs = timeout.as_secs().max(1);
        let gpu_required = if let Some(req_gpu) = args
            .get("gpu_required")
            .or_else(|| args.get("require_gpu"))
            .and_then(|v| v.as_bool())
        {
            req_gpu
        } else {
            match spec {
                TaskSpec::Generative3D(ref s) => s.engine().requires_gpu(),
                _ => true,
            }
        };

        let reqs = TaskRequirements {
            cpu_cores: 2,
            ram_mb: if gpu_required { 4096 } else { 1024 },
            gpu_required,
            timeout_secs,
            max_retries: None,
            vram_mb: None,
            extensions: Vec::new(),
        };

        let priority = args.get("priority").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
        let wait = match parse_optional_bool(args.get("wait"), "wait") {
            Ok(b) => b.unwrap_or(true),
            Err(msg) => return make_error_response(from, correlation_id, command, 1, &msg),
        };
        let task = match apply_tags_and_placement(Task::new(spec, reqs), args, "grid_3d_generate") {
            Ok(t) => t,
            Err(msg) => return make_error_response(from, correlation_id, command, 1, &msg),
        };

        if wait {
            match self.backend.submit_and_wait(task, priority, timeout).await {
                Ok(task_res) => {
                    map_task_result_to_response(task_res, from, correlation_id, command)
                }
                Err(GridError::Timeout { .. }) => make_error_response(
                    from,
                    correlation_id,
                    command,
                    124,
                    "3D generation failed or timed out",
                ),
                Err(e) => make_error_response(
                    from,
                    correlation_id,
                    command,
                    1,
                    &format!("3D generation failed: {e}"),
                ),
            }
        } else {
            match self.backend.submit_task(task, priority).await {
                Ok(task_id) => {
                    let mut resp = AgentMeshEnvelope::new_command_response(
                        "grid",
                        from,
                        correlation_id,
                        Some(command.to_string()),
                        "success",
                        0,
                        format!("3D generation task submitted successfully with ID: {task_id}"),
                        "",
                        0,
                    );
                    if let AgentMeshEnvelope::CommandResponse {
                        ref mut payload, ..
                    } = resp
                    {
                        *payload = Some(serde_json::json!({
                            "task_id": task_id.to_string(),
                            "status": "submitted"
                        }));
                    }
                    resp
                }
                Err(e) => make_error_response(
                    from,
                    correlation_id,
                    command,
                    1,
                    &format!("Failed to submit 3D generation task: {e}"),
                ),
            }
        }
    }

    async fn handle_auto_rig(
        &self,
        command: &str,
        args: &Value,
        from: &str,
        correlation_id: &str,
        timeout: Duration,
    ) -> AgentMeshEnvelope {
        let input_mesh_bytes = match parse_binary_data(
            args.get("input_mesh")
                .or_else(|| args.get("input_mesh_bytes"))
                .or_else(|| args.get("mesh")),
            "input_mesh",
        ) {
            Ok(bytes) => bytes,
            Err(msg) => return make_error_response(from, correlation_id, command, 1, &msg),
        };

        if input_mesh_bytes.is_empty() {
            return make_error_response(
                from,
                correlation_id,
                command,
                1,
                "AutoRig requires non-empty 'input_mesh' (base64, hex string, or byte array)",
            );
        }

        let rig_preset = args
            .get("rig_preset")
            .and_then(|v| v.as_str())
            .unwrap_or("biped")
            .to_string();

        let embed_animations: Vec<String> = args
            .get("embed_animations")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let preferred_provider = args
            .get("preferred_provider")
            .or_else(|| args.get("provider"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let spec = TaskSpec::AutoRig(AutoRigSpec {
            input_mesh: Bytes::from(input_mesh_bytes),
            rig_preset,
            embed_animations,
            preferred_provider,
        });

        if let Err(e) = spec.validate() {
            return make_error_response(
                from,
                correlation_id,
                command,
                1,
                &format!("Invalid AutoRig specification: {e}"),
            );
        }

        let timeout_secs = timeout.as_secs().max(1);
        let reqs = TaskRequirements {
            cpu_cores: 2,
            ram_mb: 2048,
            gpu_required: false,
            timeout_secs,
            max_retries: None,
            vram_mb: None,
            extensions: Vec::new(),
        };

        let priority = args.get("priority").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
        let wait = match parse_optional_bool(args.get("wait"), "wait") {
            Ok(b) => b.unwrap_or(true),
            Err(msg) => return make_error_response(from, correlation_id, command, 1, &msg),
        };
        let task = match apply_tags_and_placement(Task::new(spec, reqs), args, "grid_auto_rig") {
            Ok(t) => t,
            Err(msg) => return make_error_response(from, correlation_id, command, 1, &msg),
        };

        if wait {
            match self.backend.submit_and_wait(task, priority, timeout).await {
                Ok(task_res) => {
                    map_task_result_to_response(task_res, from, correlation_id, command)
                }
                Err(GridError::Timeout { .. }) => make_error_response(
                    from,
                    correlation_id,
                    command,
                    124,
                    "AutoRig failed or timed out",
                ),
                Err(e) => make_error_response(
                    from,
                    correlation_id,
                    command,
                    1,
                    &format!("AutoRig failed: {e}"),
                ),
            }
        } else {
            match self.backend.submit_task(task, priority).await {
                Ok(task_id) => {
                    let mut resp = AgentMeshEnvelope::new_command_response(
                        "grid",
                        from,
                        correlation_id,
                        Some(command.to_string()),
                        "success",
                        0,
                        format!("AutoRig task submitted successfully with ID: {task_id}"),
                        "",
                        0,
                    );
                    if let AgentMeshEnvelope::CommandResponse {
                        ref mut payload, ..
                    } = resp
                    {
                        *payload = Some(serde_json::json!({
                            "task_id": task_id.to_string(),
                            "status": "submitted"
                        }));
                    }
                    resp
                }
                Err(e) => make_error_response(
                    from,
                    correlation_id,
                    command,
                    1,
                    &format!("Failed to submit AutoRig task: {e}"),
                ),
            }
        }
    }
}

fn parse_task_id_arg(args: &Value) -> Result<TaskId, String> {
    let raw = args
        .get("task_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Missing or invalid 'task_id' UUID string".to_string())?;
    let uuid = uuid::Uuid::parse_str(raw).map_err(|e| format!("Invalid task UUID: {e}"))?;
    Ok(TaskId::from_uuid(uuid))
}

fn parse_optional_bool(val: Option<&Value>, field: &str) -> Result<Option<bool>, String> {
    match val {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(*b)),
        Some(_) => Err(format!("'{field}' must be a boolean")),
    }
}

fn parse_string_list(val: Option<&Value>, field: &str) -> Result<Vec<String>, String> {
    match val {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(arr)) => {
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                let s = item
                    .as_str()
                    .ok_or_else(|| format!("'{field}' elements must be strings"))?;
                out.push(s.to_string());
            }
            Ok(out)
        }
        Some(_) => Err(format!("'{field}' must be an array of strings")),
    }
}

fn collect_placement_tags(
    obj: &serde_json::Map<String, Value>,
    allow_bare_tags_as_require_tags: bool,
    out: &mut Vec<String>,
) -> Result<(), String> {
    for (key, prefix) in [("os", "os"), ("arch", "arch"), ("worker", "worker")] {
        if let Some(v) = obj.get(key).filter(|v| !v.is_null()) {
            let s = v
                .as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| format!("placement '{key}' must be a non-empty string"))?;
            out.push(format!("{REQUIRE_PREFIX}{prefix}={s}"));
        }
    }

    if let Some(v) = obj.get("isolation").filter(|v| !v.is_null()) {
        let s = v
            .as_str()
            .ok_or_else(|| "placement 'isolation' must be a string".to_string())?;
        let level = IsolationLevel::parse_required(s)
            .map_err(|e| format!("Invalid placement 'isolation': {e}"))?;
        out.push(format!("{REQUIRE_PREFIX}iso={level}"));
    }

    for tag in parse_string_list(obj.get("require_tags"), "require_tags")? {
        out.push(format!("{REQUIRE_PREFIX}tag={tag}"));
    }
    if allow_bare_tags_as_require_tags {
        for tag in parse_string_list(obj.get("tags"), "placement.tags")? {
            out.push(format!("{REQUIRE_PREFIX}tag={tag}"));
        }
    }
    for tool in parse_string_list(obj.get("require_tools"), "require_tools")? {
        out.push(format!("{REQUIRE_PREFIX}tool={tool}"));
    }
    for tool in parse_string_list(obj.get("tools"), "tools")? {
        out.push(format!("{REQUIRE_PREFIX}tool={tool}"));
    }

    Ok(())
}

fn reject_unsupported_workspace(args: &Value) -> Option<String> {
    if args.get("workspace").is_some_and(|v| !v.is_null()) {
        Some(
            "Workspace attachment/sync ('workspace') is not supported by the Agent Mesh bridge; \
             use inline 'files' and 'outputs' instead"
                .to_string(),
        )
    } else {
        None
    }
}

fn apply_tags_and_placement(
    mut task: Task,
    args: &Value,
    default_command_tag: &str,
) -> Result<Task, String> {
    if let Some(msg) = reject_unsupported_workspace(args) {
        return Err(msg);
    }

    let mut tags = parse_string_list(args.get("tags"), "tags")?;

    if !tags.iter().any(|t| t == default_command_tag) {
        tags.push(default_command_tag.to_string());
    }
    if !tags.iter().any(|t| t == "agent_bridge") {
        tags.push("agent_bridge".to_string());
    }

    if let Some(placement_val) = args.get("placement").filter(|v| !v.is_null()) {
        let placement_obj = placement_val
            .as_object()
            .ok_or_else(|| "'placement' must be a JSON object".to_string())?;
        collect_placement_tags(placement_obj, true, &mut tags)?;
    }

    if let Some(top_obj) = args.as_object() {
        collect_placement_tags(top_obj, false, &mut tags)?;
    }

    validate_task_user_tags(&tags)?;
    Placement::from_task_tags(&tags)?;

    task.tags = tags;
    task.validate()?;
    Ok(task)
}

fn decode_inline_file_object(
    obj: &serde_json::Map<String, Value>,
    path_hint: Option<&str>,
) -> Result<(String, Vec<u8>, bool), String> {
    let raw_path = match path_hint {
        Some(p) => p,
        None => obj
            .get("path")
            .or_else(|| obj.get("name"))
            .and_then(Value::as_str)
            .ok_or_else(|| "Inline 'files' entry must include a string 'path'".to_string())?,
    };
    let norm_path = validate_relative_sandbox_path(raw_path)?;

    let executable = match obj.get("executable") {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(_) => return Err("'files[].executable' must be a boolean".to_string()),
    };

    let content_keys = [
        "text",
        "content",
        "base64",
        "data_base64",
        "bytes_base64",
        "data",
        "bytes",
    ];
    let present: Vec<&str> = content_keys
        .iter()
        .copied()
        .filter(|k| obj.get(*k).is_some_and(|v| !v.is_null()))
        .collect();

    if present.len() != 1 {
        return Err(format!(
            "Inline file '{norm_path}' must provide exactly one of 'text'/'content', \
             'base64'/'data_base64'/'bytes_base64', or 'data'/'bytes'"
        ));
    }

    let key = present[0];
    let val = &obj[key];
    let bytes = match key {
        "text" | "content" => val
            .as_str()
            .ok_or_else(|| format!("Inline file '{norm_path}' field '{key}' must be a string"))?
            .as_bytes()
            .to_vec(),
        "base64" | "data_base64" | "bytes_base64" => {
            let s = val.as_str().ok_or_else(|| {
                format!("Inline file '{norm_path}' field '{key}' must be a base64 string")
            })?;
            use base64::engine::general_purpose::STANDARD;
            use base64::Engine;
            STANDARD.decode(s.trim()).map_err(|_| {
                format!("Inline file '{norm_path}' field '{key}' is not valid base64")
            })?
        }
        "data" | "bytes" => {
            if !val.is_string() && !val.is_array() {
                return Err(format!(
                    "Inline file '{norm_path}' field '{key}' must be base64, hex, or a byte array"
                ));
            }
            parse_binary_data(Some(val), &format!("files[{norm_path}].{key}"))?
        }
        _ => unreachable!(),
    };

    Ok((norm_path, bytes, executable))
}

/// Parses inline staged files from `args["files"]` / `args["staged_files"]`.
///
/// Security invariant: NEVER reads hub-local disk files and refuses arrays of string paths.
fn parse_inline_staged_files(val: Option<&Value>) -> Result<Vec<StagedFile>, String> {
    let mut staged = Vec::new();
    let mut seen_paths = HashSet::new();
    let mut total_bytes: usize = 0;

    let mut push_file =
        |norm_path: String, bytes: Vec<u8>, executable: bool| -> Result<(), String> {
            if !seen_paths.insert(norm_path.clone()) {
                return Err(format!("Duplicate inline file path '{norm_path}'"));
            }
            if bytes.len() > MAX_STAGED_FILE_BYTES {
                return Err(format!(
                    "Inline file '{norm_path}' size ({} bytes) exceeds 32 MiB limit",
                    bytes.len()
                ));
            }
            total_bytes = total_bytes.saturating_add(bytes.len());
            if total_bytes > MAX_AGGREGATE_STAGED_BYTES {
                return Err("Aggregate inline 'files' size exceeds 32 MiB limit".to_string());
            }
            staged.push(StagedFile::new(norm_path, Bytes::from(bytes), executable));
            Ok(())
        };

    match val {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(arr)) => {
            for item in arr {
                match item {
                    Value::String(_) => {
                        return Err(
                            "Inline 'files' entries must be objects with 'path' and inline \
                             'text'/'content'/'base64'/'data'; hub-local file paths are not accepted"
                                .to_string(),
                        );
                    }
                    Value::Object(obj) => {
                        let (norm_path, bytes, executable) = decode_inline_file_object(obj, None)?;
                        push_file(norm_path, bytes, executable)?;
                    }
                    _ => {
                        return Err(
                            "Inline 'files' entries must be objects with 'path' and inline content"
                                .to_string(),
                        );
                    }
                }
            }
            Ok(staged)
        }
        Some(Value::Object(map)) => {
            for (raw_path, entry_val) in map {
                let norm_path = validate_relative_sandbox_path(raw_path)?;
                match entry_val {
                    Value::String(text) => {
                        push_file(norm_path, text.as_bytes().to_vec(), false)?;
                    }
                    Value::Object(obj) => {
                        let (resolved_path, bytes, executable) =
                            decode_inline_file_object(obj, Some(&norm_path))?;
                        push_file(resolved_path, bytes, executable)?;
                    }
                    _ => {
                        return Err(format!(
                            "Inline file '{norm_path}' value must be a UTF-8 string or inline file object"
                        ));
                    }
                }
            }
            Ok(staged)
        }
        Some(_) => Err(
            "'files' must be an array of inline file objects or a map of path to content".into(),
        ),
    }
}

fn parse_expected_outputs(val: Option<&Value>) -> Result<Vec<String>, String> {
    match val {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(arr)) => {
            let mut outputs = Vec::with_capacity(arr.len());
            let mut seen = HashSet::new();
            for item in arr {
                let raw = item
                    .as_str()
                    .ok_or_else(|| "'outputs' entries must be relative path strings".to_string())?;
                let norm = validate_relative_sandbox_path(raw)?;
                if seen.insert(norm.clone()) {
                    outputs.push(norm);
                }
            }
            Ok(outputs)
        }
        Some(_) => Err("'outputs' must be an array of relative sandbox file paths".into()),
    }
}

fn parse_input_bytes(val: Option<&Value>) -> Vec<u8> {
    match val {
        Some(Value::String(s)) => {
            if s.starts_with("0x") || s.starts_with("0X") {
                (2..s.len())
                    .step_by(2)
                    .filter_map(|i| {
                        if i + 2 <= s.len() {
                            u8::from_str_radix(&s[i..i + 2], 16).ok()
                        } else {
                            None
                        }
                    })
                    .collect()
            } else {
                s.as_bytes().to_vec()
            }
        }
        Some(Value::Array(arr)) => arr
            .iter()
            .filter_map(|v| v.as_u64().map(|n| n as u8))
            .collect(),
        _ => Vec::new(),
    }
}

/// Decode a binary payload (image or mesh) sent by a peer.
///
/// Order of attempts for a string value (after trimming):
/// 1. empty: `Ok(vec![])` (the caller reports its own "missing input" error);
/// 2. `0x` / `0X` prefix: strict hex (even length, hex digits only). Bare hex
///    without the prefix is not recognised, because it is also valid base64;
/// 3. path-like: a leading `/` other than the JPEG base64 magic `/9j/` is
///    refused, since strings such as `/tmp/abcd` are valid base64 characters.
///    Strings with `.`, `\`, `:` (`/tmp/x.glb`, `C:\x.glb`) are not base64
///    and are refused by step 4;
/// 4. base64 (`STANDARD` alphabet, padded);
/// 5. anything else is refused. The string's own bytes are never used as the
///    payload and the hub never reads files a peer names.
///
/// A JSON array of numbers is taken as raw bytes; other JSON types give an
/// empty result.
fn parse_binary_data(val: Option<&Value>, field: &str) -> Result<Vec<u8>, String> {
    let refusal =
        || format!("{field} must be base64, hex or a byte array; file paths are not accepted");
    match val {
        Some(Value::Array(arr)) => Ok(arr
            .iter()
            .filter_map(|v| v.as_u64().map(|n| n as u8))
            .collect()),
        Some(Value::String(s)) => {
            let s_trim = s.trim();
            if s_trim.is_empty() {
                return Ok(Vec::new());
            }
            if let Some(hex) = s_trim
                .strip_prefix("0x")
                .or_else(|| s_trim.strip_prefix("0X"))
            {
                if hex.len() % 2 != 0 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                    return Err(refusal());
                }
                return Ok((0..hex.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap_or(0))
                    .collect());
            }
            if s_trim.starts_with('/') && !s_trim.starts_with("/9j/") {
                return Err(refusal());
            }
            use base64::engine::general_purpose::STANDARD;
            use base64::Engine;
            match STANDARD.decode(s_trim) {
                Ok(bytes) if !bytes.is_empty() => Ok(bytes),
                _ => Err(refusal()),
            }
        }
        _ => Ok(Vec::new()),
    }
}

fn parse_source_files(val: Option<&Value>) -> HashMap<String, String> {
    let mut files = HashMap::new();
    if let Some(obj) = val.and_then(|v| v.as_object()) {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                files.insert(k.clone(), s.to_string());
            }
        }
    } else if let Some(arr) = val.and_then(|v| v.as_array()) {
        for item in arr {
            if let Some(pair) = item.as_array() {
                if pair.len() == 2 {
                    if let (Some(k), Some(v)) = (pair[0].as_str(), pair[1].as_str()) {
                        files.insert(k.to_string(), v.to_string());
                    }
                }
            } else if let Some(obj) = item.as_object() {
                if let (Some(path), Some(content)) = (
                    obj.get("path").and_then(|v| v.as_str()),
                    obj.get("content").and_then(|v| v.as_str()),
                ) {
                    files.insert(path.to_string(), content.to_string());
                }
            }
        }
    }
    files
}

fn parse_compiler_flags(val: Option<&Value>) -> Vec<String> {
    if let Some(arr) = val.and_then(|v| v.as_array()) {
        arr.iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect()
    } else if let Some(s) = val.and_then(|v| v.as_str()) {
        s.split_whitespace().map(String::from).collect()
    } else {
        Vec::new()
    }
}

fn map_task_result_to_response(
    res: TaskResult,
    from: &str,
    correlation_id: &str,
    command: &str,
) -> AgentMeshEnvelope {
    let status = if res.exit_code == 0 {
        "success"
    } else {
        "failed"
    };
    // Lossy, not `as_str()`: non-UTF-8 console output (e.g. an OEM code page on Windows) must
    // reach the agent as text with replacement characters, not as an empty string.
    let mut stdout = res.stdout_str().into_owned();
    let stderr = res.stderr_str().into_owned();

    // If compilation succeeded, ensure stdout contains informative completion banner
    if command.contains("compile")
        && res.exit_code == 0
        && !stdout.contains("Compilation successful")
        && !stdout.contains("Finished")
    {
        if !stdout.is_empty() {
            stdout.push_str("\nCompilation successful.\n");
        } else {
            stdout = "Compilation successful.\n".to_string();
        }
    }

    let mut payload = serde_json::json!({
        "task_id": res.task_id.to_string(),
        "worker_id": res.worker_id.to_string(),
        "is_gpu_executed": res.is_gpu_executed,
        "device_name": res.device_name,
        "execution_time_ms": res.execution_time_ms,
    });

    if let Some(ref asset) = res.asset_bytes {
        use base64::engine::general_purpose::STANDARD;
        use base64::Engine;
        let asset_slice = asset.as_slice();
        let b64 = STANDARD.encode(asset_slice);
        let sha256 = compute_sha256(asset_slice);
        if let Some(obj) = payload.as_object_mut() {
            obj.insert(
                "asset_bytes_len".to_string(),
                serde_json::json!(asset_slice.len()),
            );
            obj.insert("asset_sha256".to_string(), serde_json::json!(sha256));
            obj.insert("asset_base64".to_string(), serde_json::json!(b64));
        }
        if stdout.is_empty() {
            stdout = format!(
                "Asset generated successfully ({} bytes, sha256: {})",
                asset_slice.len(),
                sha256
            );
        }
    }

    if !res.artifacts.is_empty() {
        use base64::engine::general_purpose::STANDARD;
        use base64::Engine;
        let artifacts_json: Vec<Value> = res
            .artifacts
            .iter()
            .map(|art| {
                serde_json::json!({
                    "path": art.path,
                    "size": art.size,
                    "sha256": art.sha256_hex(),
                    "data_base64": STANDARD.encode(art.data.as_slice()),
                })
            })
            .collect();
        if let Some(obj) = payload.as_object_mut() {
            obj.insert("artifacts".to_string(), Value::Array(artifacts_json));
        }
    }

    let mut resp = AgentMeshEnvelope::new_command_response(
        "grid",
        from,
        correlation_id,
        Some(command.to_string()),
        status,
        res.exit_code,
        stdout,
        stderr,
        res.execution_time_ms,
    );

    if let AgentMeshEnvelope::CommandResponse {
        payload: ref mut p,
        error: ref mut e,
        ..
    } = resp
    {
        *p = Some(payload);
        *e = res.error;
    }

    resp
}

fn make_error_response(
    from: &str,
    correlation_id: &str,
    command: &str,
    exit_code: i32,
    err_msg: &str,
) -> AgentMeshEnvelope {
    let mut resp = AgentMeshEnvelope::new_command_response(
        "grid",
        from,
        correlation_id,
        Some(command.to_string()),
        "failed",
        exit_code,
        "",
        err_msg,
        0,
    );
    if let AgentMeshEnvelope::CommandResponse { ref mut error, .. } = resp {
        *error = Some(err_msg.to_string());
    }
    resp
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::task::Bytes;
    use uuid::Uuid;

    #[test]
    fn test_response_keeps_non_utf8_output_instead_of_blanking_it() {
        // Windows console output in an OEM code page: 0x82 is e-acute and invalid as UTF-8.
        let mut raw = b"Volume in drive C is Syst".to_vec();
        raw.push(0x82);
        raw.extend_from_slice(b"me\r\n");
        let mut res =
            TaskResult::success(Uuid::new_v4(), TaskId::new(), Bytes::from(raw), 5, false);
        res.stderr = Bytes::from(vec![b'w', 0xFF, b'n']);

        match map_task_result_to_response(res, "agent-1", "corr-1", "grid_submit") {
            AgentMeshEnvelope::CommandResponse {
                status,
                exit_code,
                stdout,
                stderr,
                ..
            } => {
                assert_eq!(status, "success");
                assert_eq!(exit_code, 0);
                assert_eq!(stdout, "Volume in drive C is Syst\u{fffd}me\r\n");
                assert_eq!(stderr, "w\u{fffd}n");
            }
            other => panic!("expected CommandResponse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_bridge_refuses_unknown_engine_envelope() {
        let master = rusty_grid_master::server::MasterServer::spawn(
            rusty_grid_master::server::ServerConfig::new("127.0.0.1:0".parse().unwrap()),
        )
        .await
        .expect("spawn master");
        let bridge = AgentGridBridge::new(master);

        let req = AgentMeshEnvelope::new_command_request(
            "agent-1",
            "grid",
            "grid_3d_generate",
            serde_json::json!({
                "prompt": "robot",
                "engine": "rodin",
            }),
            Some(5000),
        );

        let resp = bridge.handle_command(&req).await;
        match resp {
            AgentMeshEnvelope::CommandResponse {
                exit_code,
                stderr,
                status,
                ..
            } => {
                assert_eq!(status, "failed");
                assert_ne!(exit_code, 0);
                assert!(stderr.contains("unknown 3D engine 'rodin'"));
            }
            other => panic!("expected CommandResponse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_bridge_refuses_non_string_engine_envelope() {
        let master = rusty_grid_master::server::MasterServer::spawn(
            rusty_grid_master::server::ServerConfig::new("127.0.0.1:0".parse().unwrap()),
        )
        .await
        .expect("spawn master");
        let bridge = AgentGridBridge::new(master);

        // Non-string 'engine' (integer 5)
        let req_num = AgentMeshEnvelope::new_command_request(
            "agent-1",
            "grid",
            "grid_3d_generate",
            serde_json::json!({
                "prompt": "robot",
                "engine": 5,
            }),
            Some(5000),
        );

        let resp_num = bridge.handle_command(&req_num).await;
        match resp_num {
            AgentMeshEnvelope::CommandResponse {
                exit_code,
                stderr,
                status,
                ..
            } => {
                assert_eq!(status, "failed");
                assert_ne!(exit_code, 0);
                assert!(
                    stderr.contains("'engine' must be a string"),
                    "Expected non-string engine error, got: {stderr}"
                );
            }
            other => panic!("expected CommandResponse, got {other:?}"),
        }

        // Non-string 'engine_preference' (boolean true)
        let req_bool = AgentMeshEnvelope::new_command_request(
            "agent-1",
            "grid",
            "grid_3d_generate",
            serde_json::json!({
                "prompt": "robot",
                "engine_preference": true,
            }),
            Some(5000),
        );

        let resp_bool = bridge.handle_command(&req_bool).await;
        match resp_bool {
            AgentMeshEnvelope::CommandResponse {
                exit_code,
                stderr,
                status,
                ..
            } => {
                assert_eq!(status, "failed");
                assert_ne!(exit_code, 0);
                assert!(
                    stderr.contains("'engine_preference' must be a string"),
                    "Expected non-string engine_preference error, got: {stderr}"
                );
            }
            other => panic!("expected CommandResponse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_parse_binary_data_and_bridge_never_read_hub_local_files() {
        let temp_dir = tempfile::tempdir().expect("create tempdir");
        let secret_glb_path = temp_dir.path().join("secret_mesh.glb");
        let secret_png_path = temp_dir.path().join("secret_image.png");
        let secret_txt_path = temp_dir.path().join("secret_notes.txt");

        let mut secret_glb_bytes =
            b"glTF\x02\x00\x00\x00\x28\x00\x00\x00HUB_LOCAL_SECRET_GLB_DO_NOT_READ".to_vec();
        secret_glb_bytes.resize(65_536, b'G');
        let mut secret_png_bytes = b"\x89PNG\r\n\x1a\nHUB_LOCAL_SECRET_PNG_DO_NOT_READ".to_vec();
        secret_png_bytes.resize(65_536, b'P');
        let mut secret_txt_bytes = b"HUB_LOCAL_SECRET_TXT_DO_NOT_READ".to_vec();
        secret_txt_bytes.resize(65_536, b'T');

        std::fs::write(&secret_glb_path, &secret_glb_bytes).expect("write secret glb");
        std::fs::write(&secret_png_path, &secret_png_bytes).expect("write secret png");
        std::fs::write(&secret_txt_path, &secret_txt_bytes).expect("write secret txt");

        for (path, secret) in [
            (&secret_glb_path, secret_glb_bytes.as_slice()),
            (&secret_png_path, secret_png_bytes.as_slice()),
            (&secret_txt_path, secret_txt_bytes.as_slice()),
        ] {
            let path_val = serde_json::Value::String(path.to_string_lossy().to_string());
            let parsed = parse_binary_data(Some(&path_val), "input_mesh").unwrap_or_default();
            assert_ne!(
                parsed.as_slice(),
                secret,
                "parse_binary_data must never read hub-local file {}",
                path.display()
            );
            assert!(
                !String::from_utf8_lossy(&parsed).contains("HUB_LOCAL_SECRET_"),
                "parse_binary_data leaked hub-local file content from {}",
                path.display()
            );
        }

        let master = rusty_grid_master::server::MasterServer::spawn(
            rusty_grid_master::server::ServerConfig::new("127.0.0.1:0".parse().unwrap()),
        )
        .await
        .expect("spawn master");
        let bridge = AgentGridBridge::new(master.clone());

        // 1. Peer passes a hub-local .png path to grid_3d_generate
        let before_3d = master
            .queue_stats()
            .await
            .expect("queue stats")
            .active_bytes;
        let req_3d = AgentMeshEnvelope::new_command_request(
            "peer-agent",
            "grid",
            "grid_3d_generate",
            serde_json::json!({
                "image": secret_png_path.to_string_lossy().to_string(),
                "engine": "triposr",
                "wait": false,
            }),
            Some(5000),
        );
        let _resp_3d = bridge.handle_command(&req_3d).await;
        let after_3d = master
            .queue_stats()
            .await
            .expect("queue stats")
            .active_bytes;
        assert!(
            after_3d.saturating_sub(before_3d) < 8_192,
            "grid_3d_generate read the 64 KiB hub-local .png file into the queue (active_bytes delta = {})",
            after_3d.saturating_sub(before_3d)
        );

        // 2. Peer passes a hub-local .glb path to grid_auto_rig
        let before_rig = master
            .queue_stats()
            .await
            .expect("queue stats")
            .active_bytes;
        let req_rig = AgentMeshEnvelope::new_command_request(
            "peer-agent",
            "grid",
            "grid_auto_rig",
            serde_json::json!({
                "input_mesh": secret_glb_path.to_string_lossy().to_string(),
                "wait": false,
            }),
            Some(5000),
        );
        let _resp_rig = bridge.handle_command(&req_rig).await;
        let after_rig = master
            .queue_stats()
            .await
            .expect("queue stats")
            .active_bytes;
        assert!(
            after_rig.saturating_sub(before_rig) < 8_192,
            "grid_auto_rig read the 64 KiB hub-local .glb file into the queue (active_bytes delta = {})",
            after_rig.saturating_sub(before_rig)
        );

        // 3. Peer passes a hub-local .txt path to grid_auto_rig
        let before_txt = master
            .queue_stats()
            .await
            .expect("queue stats")
            .active_bytes;
        let req_txt = AgentMeshEnvelope::new_command_request(
            "peer-agent",
            "grid",
            "grid_auto_rig",
            serde_json::json!({
                "input_mesh": secret_txt_path.to_string_lossy().to_string(),
                "wait": false,
            }),
            Some(5000),
        );
        let _resp_txt = bridge.handle_command(&req_txt).await;
        let after_txt = master
            .queue_stats()
            .await
            .expect("queue stats")
            .active_bytes;
        assert!(
            after_txt.saturating_sub(before_txt) < 8_192,
            "grid_auto_rig read the 64 KiB hub-local .txt file into the queue (active_bytes delta = {})",
            after_txt.saturating_sub(before_txt)
        );

        let _ = master.shutdown();
    }

    #[test]
    fn parse_binary_data_accepts_array_base64_and_hex() {
        let arr = serde_json::json!([1, 2, 255]);
        assert_eq!(parse_binary_data(Some(&arr), "x").unwrap(), vec![1, 2, 255]);

        let b64 = serde_json::json!("Z2xURg==");
        assert_eq!(parse_binary_data(Some(&b64), "x").unwrap(), b"glTF");
        let b64_ws = serde_json::json!("  Z2xURg==\n");
        assert_eq!(parse_binary_data(Some(&b64_ws), "x").unwrap(), b"glTF");
        // JPEG base64 starts with '/', which must stay accepted.
        let jpeg = serde_json::json!("/9j/4AAQ");
        assert_eq!(
            parse_binary_data(Some(&jpeg), "x").unwrap(),
            vec![0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10]
        );

        let hex = serde_json::json!("0x676c5446");
        assert_eq!(parse_binary_data(Some(&hex), "x").unwrap(), b"glTF");
        let hex_up = serde_json::json!("0X676C5446");
        assert_eq!(parse_binary_data(Some(&hex_up), "x").unwrap(), b"glTF");
    }

    #[test]
    fn parse_binary_data_empty_values_stay_empty() {
        assert!(parse_binary_data(None, "x").unwrap().is_empty());
        assert!(parse_binary_data(Some(&serde_json::json!("")), "x")
            .unwrap()
            .is_empty());
        assert!(parse_binary_data(Some(&serde_json::json!("  ")), "x")
            .unwrap()
            .is_empty());
        assert!(parse_binary_data(Some(&serde_json::json!([])), "x")
            .unwrap()
            .is_empty());
        assert!(parse_binary_data(Some(&serde_json::json!(5)), "x")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn parse_binary_data_refuses_paths_and_garbage() {
        for bad in [
            "/tmp/x.glb",
            "/tmp/abc",
            "/tmp/abcd",
            "/Users/me/mesh",
            "C:\\x.glb",
            "C:\\Users\\me\\mesh.obj",
            "relative/mesh.glb",
            "mesh.glb",
            "~/mesh.glb",
            "not base64 at all",
            "0xzz",
            "0x123",
        ] {
            let err =
                parse_binary_data(Some(&serde_json::json!(bad)), "input_mesh").expect_err(bad);
            assert_eq!(
                err, "input_mesh must be base64, hex or a byte array; file paths are not accepted",
                "{bad}"
            );
        }
    }

    #[tokio::test]
    async fn bridge_refuses_path_valued_inputs_without_submitting() {
        let master = rusty_grid_master::server::MasterServer::spawn(
            rusty_grid_master::server::ServerConfig::new("127.0.0.1:0".parse().unwrap()),
        )
        .await
        .expect("spawn master");
        let bridge = AgentGridBridge::new(master.clone());
        let before = master.queue_stats().await.expect("queue stats").total;

        for (command, key) in [
            ("grid_auto_rig", "input_mesh"),
            ("grid_3d_generate", "image"),
        ] {
            for path in ["/tmp/x.glb", "C:\\x.glb"] {
                let req = AgentMeshEnvelope::new_command_request(
                    "peer-agent",
                    "grid",
                    command,
                    serde_json::json!({ key: path, "wait": false }),
                    Some(5000),
                );
                match bridge.handle_command(&req).await {
                    AgentMeshEnvelope::CommandResponse {
                        exit_code,
                        stderr,
                        status,
                        ..
                    } => {
                        assert_eq!(status, "failed");
                        assert_ne!(exit_code, 0);
                        assert!(
                            stderr.contains("file paths are not accepted"),
                            "{command} {path}: {stderr}"
                        );
                    }
                    other => panic!("expected CommandResponse, got {other:?}"),
                }
            }
        }

        let after = master.queue_stats().await.expect("queue stats").total;
        assert_eq!(before, after, "a refused input must not submit a task");
        let _ = master.shutdown();
    }

    #[test]
    fn test_apply_tags_and_placement_preserves_caller_tags_and_parses_constraints() {
        let task = Task::new(
            TaskSpec::new_shell_script("echo hi"),
            TaskRequirements::generic(1, 30),
        );
        let args = serde_json::json!({
            "tags": ["custom-tag", "team:ml"],
            "placement": {
                "os": "darwin",
                "tags": ["gpu-high"],
                "tools": ["cargo"],
            },
            "arch": "arm64",
            "isolation": "fs",
            "require_tags": ["zone-a"],
            "require_tools": ["python3"],
        });
        let updated = apply_tags_and_placement(task, &args, "grid_submit").unwrap();
        assert!(updated.tags.contains(&"custom-tag".to_string()));
        assert!(updated.tags.contains(&"team:ml".to_string()));
        assert!(updated.tags.contains(&"grid_submit".to_string()));
        assert!(updated.tags.contains(&"agent_bridge".to_string()));

        let placement = updated.placement().unwrap();
        assert_eq!(placement.os.as_deref(), Some("macos"));
        assert_eq!(placement.arch.as_deref(), Some("aarch64"));
        assert_eq!(placement.isolation, Some(IsolationLevel::Fs));
        assert!(placement.tags.contains(&"gpu-high".to_string()));
        assert!(placement.tags.contains(&"zone-a".to_string()));
        assert!(placement.tools.contains(&"cargo".to_string()));
        assert!(placement.tools.contains(&"python3".to_string()));
    }

    #[test]
    fn test_apply_tags_and_placement_rejects_isolation_off_and_reserved_tags() {
        let base = Task::new(
            TaskSpec::new_shell_script("echo hi"),
            TaskRequirements::generic(1, 30),
        );
        let err_off = apply_tags_and_placement(
            base.clone(),
            &serde_json::json!({ "isolation": "off" }),
            "grid_submit",
        )
        .expect_err("isolation=off must be refused");
        assert!(err_off.contains("fs"), "{err_off}");

        let err_reserved = apply_tags_and_placement(
            base,
            &serde_json::json!({ "tags": ["speed:fast"] }),
            "grid_submit",
        )
        .expect_err("reserved speed: tag must be refused");
        assert!(err_reserved.contains("speed:"), "{err_reserved}");
    }

    #[test]
    fn test_parse_inline_staged_files_and_refuses_hub_local_paths() {
        use base64::engine::general_purpose::STANDARD;
        use base64::Engine;

        // 1. Refuses string array paths like ["/etc/passwd"].
        let err = parse_inline_staged_files(Some(&serde_json::json!(["/etc/passwd"])))
            .expect_err("string paths must be refused");
        assert!(
            err.contains("hub-local file paths are not accepted"),
            "{err}"
        );

        // 2. Accepts inline array of objects (text + base64 + executable).
        let files = parse_inline_staged_files(Some(&serde_json::json!([
            {
                "path": "scripts/run.sh",
                "text": "#!/bin/sh\necho ok\n",
                "executable": true
            },
            {
                "path": "data/blob.bin",
                "base64": STANDARD.encode([0xde, 0xad, 0xbe, 0xef])
            }
        ])))
        .expect("inline objects should parse");
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].path, "scripts/run.sh");
        assert!(files[0].executable);
        assert_eq!(files[0].data.as_bytes(), b"#!/bin/sh\necho ok\n");
        assert_eq!(files[1].path, "data/blob.bin");
        assert!(!files[1].executable);
        assert_eq!(files[1].data.as_bytes(), &[0xde, 0xad, 0xbe, 0xef]);

        // 3. Rejects traversal paths and duplicate paths.
        assert!(parse_inline_staged_files(Some(&serde_json::json!([
            { "path": "../escape.txt", "text": "nope" }
        ])))
        .is_err());
        assert!(parse_inline_staged_files(Some(&serde_json::json!([
            { "path": "a.txt", "text": "1" },
            { "path": "a.txt", "text": "2" }
        ])))
        .is_err());
    }

    #[test]
    fn test_map_task_result_serializes_output_artifacts() {
        use rusty_grid_core::task::OutputArtifact;

        let mut res = TaskResult::success(
            Uuid::new_v4(),
            TaskId::new(),
            Bytes::from_static(b"done"),
            12,
            false,
        );
        res.artifacts.push(OutputArtifact::new(
            "out/result.txt",
            Bytes::from_static(b"hello-artifact"),
        ));

        let env = map_task_result_to_response(res, "agent-1", "corr-1", "grid_submit");
        match env {
            AgentMeshEnvelope::CommandResponse {
                payload: Some(payload),
                ..
            } => {
                let artifacts = payload["artifacts"].as_array().expect("artifacts array");
                assert_eq!(artifacts.len(), 1);
                assert_eq!(artifacts[0]["path"], "out/result.txt");
                assert_eq!(artifacts[0]["size"], 14);
                assert_eq!(artifacts[0]["sha256"], compute_sha256(b"hello-artifact"));
            }
            other => panic!("expected CommandResponse with payload, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_bridge_grid_submit_refuses_string_files_and_invalid_requirements_without_submitting(
    ) {
        let master = rusty_grid_master::server::MasterServer::spawn(
            rusty_grid_master::server::ServerConfig::new("127.0.0.1:0".parse().unwrap()),
        )
        .await
        .expect("spawn master");
        let bridge = AgentGridBridge::new(master.clone());
        let before = master.queue_stats().await.expect("queue stats").total;

        let bad_files_req = AgentMeshEnvelope::new_command_request(
            "peer-agent",
            "grid",
            "grid_submit",
            serde_json::json!({
                "script": "cat /etc/passwd",
                "files": ["/etc/passwd"],
                "wait": false
            }),
            Some(5000),
        );
        match bridge.handle_command(&bad_files_req).await {
            AgentMeshEnvelope::CommandResponse {
                status,
                exit_code,
                stderr,
                ..
            } => {
                assert_eq!(status, "failed");
                assert_ne!(exit_code, 0);
                assert!(
                    stderr.contains("hub-local file paths are not accepted"),
                    "{stderr}"
                );
            }
            other => panic!("expected CommandResponse, got {other:?}"),
        }

        let bad_reqs_req = AgentMeshEnvelope::new_command_request(
            "peer-agent",
            "grid",
            "grid_submit",
            serde_json::json!({
                "script": "echo hi",
                "requirements": "not-an-object",
                "wait": false
            }),
            Some(5000),
        );
        match bridge.handle_command(&bad_reqs_req).await {
            AgentMeshEnvelope::CommandResponse {
                status,
                exit_code,
                stderr,
                ..
            } => {
                assert_eq!(status, "failed");
                assert_ne!(exit_code, 0);
                assert!(stderr.contains("Invalid task requirements"), "{stderr}");
            }
            other => panic!("expected CommandResponse, got {other:?}"),
        }

        let after = master.queue_stats().await.expect("queue stats").total;
        assert_eq!(before, after, "refused submissions must not enqueue tasks");
        let _ = master.shutdown();
    }

    #[tokio::test]
    async fn test_bridge_grid_submit_refuses_workspace_without_submitting() {
        let master = rusty_grid_master::server::MasterServer::spawn(
            rusty_grid_master::server::ServerConfig::new("127.0.0.1:0".parse().unwrap()),
        )
        .await
        .expect("spawn master");
        let bridge = AgentGridBridge::new(master.clone());
        let before = master.queue_stats().await.expect("queue stats").total;
        assert_eq!(before, 0);

        let cases = [
            (
                "grid_submit",
                serde_json::json!({
                    "script": "echo should-be-rejected",
                    "workspace": "my-ws",
                    "wait": false
                }),
            ),
            (
                "grid_submit",
                serde_json::json!({
                    "script": "echo should-be-rejected",
                    "workspace": {"name": "my-ws"},
                    "wait": false
                }),
            ),
            (
                "grid_submit",
                serde_json::json!({
                    "script": "echo should-be-rejected",
                    "workspace": "my-ws",
                    "files": [{"path": "a.txt", "text": "x"}],
                    "outputs": ["out.txt"],
                    "wait": false
                }),
            ),
            (
                "grid_submit",
                serde_json::json!({
                    "script": "echo should-be-rejected",
                    "workspace": {"name": "my-ws"},
                    "files": [{"path": "a.txt", "text": "x"}],
                    "outputs": ["out.txt"],
                    "wait": false
                }),
            ),
            (
                "grid_compute",
                serde_json::json!({
                    "test_name": "noop",
                    "workspace": "my-ws"
                }),
            ),
        ];

        for (cmd, args) in cases {
            let req = AgentMeshEnvelope::new_command_request(
                "peer-agent",
                "grid",
                cmd,
                args.clone(),
                Some(5000),
            );
            match bridge.handle_command(&req).await {
                AgentMeshEnvelope::CommandResponse {
                    status,
                    exit_code,
                    stderr,
                    ..
                } => {
                    assert_eq!(status, "failed", "cmd={cmd} args={args}");
                    assert_eq!(exit_code, 1, "cmd={cmd} args={args}");
                    assert!(
                        stderr.contains("workspace"),
                        "expected workspace error for cmd={cmd} args={args}, got: {stderr}"
                    );
                }
                other => panic!("expected CommandResponse, got {other:?}"),
            }
        }

        let after_refused = master.queue_stats().await.expect("queue stats").total;
        assert_eq!(
            after_refused, 0,
            "workspace-rejected requests must not enqueue any task"
        );

        // Explicit `"workspace": null` must remain accepted.
        let null_ws_req = AgentMeshEnvelope::new_command_request(
            "peer-agent",
            "grid",
            "grid_submit",
            serde_json::json!({
                "script": "echo ok",
                "workspace": null,
                "wait": false
            }),
            Some(5000),
        );
        match bridge.handle_command(&null_ws_req).await {
            AgentMeshEnvelope::CommandResponse {
                status, exit_code, ..
            } => {
                assert_eq!(status, "success");
                assert_eq!(exit_code, 0);
            }
            other => panic!("expected CommandResponse, got {other:?}"),
        }
        assert_eq!(master.queue_stats().await.expect("queue stats").total, 1);

        let _ = master.shutdown();
    }

    #[tokio::test]
    async fn test_bridge_grid_submit_edge_cases_and_status_error_prefix() {
        use crate::grid_backend::{
            BoxGridFuture, GridBackend, GridClusterStatus, GridTaskResult, GridTaskStatus,
        };
        use futures::future::BoxFuture;
        use std::time::Duration;

        // 1. Inline `files` object-map syntax + duplicate normalized paths + traversal + >32 MiB
        let map_files = parse_inline_staged_files(Some(&serde_json::json!({
            "a/b.txt": "hello",
            "run.sh": { "text": "#!/bin/sh\necho hi\n", "executable": true }
        })))
        .expect("object-map files must parse");
        assert_eq!(map_files.len(), 2);
        let run_sh = map_files.iter().find(|f| f.path == "run.sh").unwrap();
        assert!(run_sh.executable);

        let dup_err = parse_inline_staged_files(Some(&serde_json::json!([
            { "path": "a.txt", "text": "1" },
            { "path": "./a.txt", "text": "2" }
        ])))
        .unwrap_err();
        assert!(dup_err.contains("Duplicate"), "{dup_err}");

        for bad_path in ["../escape.txt", "/abs/path.txt"] {
            let err = parse_inline_staged_files(Some(&serde_json::json!([
                { "path": bad_path, "text": "x" }
            ])))
            .unwrap_err();
            assert!(!err.is_empty());
        }

        let huge_text = "x".repeat(MAX_STAGED_FILE_BYTES + 1);
        let huge_err = parse_inline_staged_files(Some(&serde_json::json!([
            { "path": "big.bin", "text": huge_text }
        ])))
        .unwrap_err();
        assert!(huge_err.contains("32 MiB"), "{huge_err}");

        // 2. Conflicting placement tags + unknown UUID cancel + output traversal refusal
        let master = rusty_grid_master::server::MasterServer::spawn(
            rusty_grid_master::server::ServerConfig::new("127.0.0.1:0".parse().unwrap()),
        )
        .await
        .expect("spawn master");
        let bridge = AgentGridBridge::new(master.clone());

        let conflict_req = AgentMeshEnvelope::new_command_request(
            "peer-agent",
            "grid",
            "grid_submit",
            serde_json::json!({
                "script": "echo hi",
                "tags": ["require:os=linux"],
                "os": "macos",
                "wait": false
            }),
            Some(5000),
        );
        match bridge.handle_command(&conflict_req).await {
            AgentMeshEnvelope::CommandResponse {
                status, exit_code, ..
            } => {
                assert_eq!(status, "failed");
                assert_ne!(exit_code, 0);
            }
            other => panic!("expected CommandResponse, got {other:?}"),
        }

        let traversal_out_req = AgentMeshEnvelope::new_command_request(
            "peer-agent",
            "grid",
            "grid_submit",
            serde_json::json!({
                "script": "echo hi",
                "outputs": ["../escape.txt"],
                "wait": false
            }),
            Some(5000),
        );
        match bridge.handle_command(&traversal_out_req).await {
            AgentMeshEnvelope::CommandResponse {
                status, exit_code, ..
            } => {
                assert_eq!(status, "failed");
                assert_ne!(exit_code, 0);
            }
            other => panic!("expected CommandResponse, got {other:?}"),
        }

        let unknown_id = Uuid::new_v4().to_string();
        let cancel_unknown = AgentMeshEnvelope::new_command_request(
            "peer-agent",
            "grid",
            "grid_cancel",
            serde_json::json!({ "task_id": unknown_id }),
            Some(5000),
        );
        match bridge.handle_command(&cancel_unknown).await {
            AgentMeshEnvelope::CommandResponse {
                status,
                exit_code,
                payload: Some(p),
                ..
            } => {
                assert_eq!(status, "success");
                assert_eq!(exit_code, 0);
                assert_eq!(p["cancelled"], serde_json::json!(false));
            }
            other => panic!("expected CommandResponse, got {other:?}"),
        }
        let _ = master.shutdown();

        // 3. Mock backend verifying handle_status separates TaskNotFound from ConnectionFailed
        struct ErrorStatusBackend {
            return_not_found: bool,
        }
        impl GridBackend for ErrorStatusBackend {
            fn submit_task(&self, _: Task, _: u32) -> BoxGridFuture<'_, TaskId> {
                Box::pin(async { Err(GridError::Config("unused".into())) })
            }
            fn submit_and_wait(
                &self,
                _: Task,
                _: u32,
                _: Duration,
            ) -> BoxGridFuture<'_, TaskResult> {
                Box::pin(async { Err(GridError::Config("unused".into())) })
            }
            fn get_task_status(&self, tid: TaskId) -> BoxGridFuture<'_, GridTaskStatus> {
                let nf = self.return_not_found;
                Box::pin(async move {
                    if nf {
                        Err(GridError::TaskNotFound(tid.0))
                    } else {
                        Err(GridError::ConnectionFailed("connection refused".into()))
                    }
                })
            }
            fn get_task_result(&self, _: TaskId) -> BoxGridFuture<'_, GridTaskResult> {
                Box::pin(async { Err(GridError::Config("unused".into())) })
            }
            fn wait_task(&self, _: TaskId, _: Duration) -> BoxGridFuture<'_, TaskResult> {
                Box::pin(async { Err(GridError::Config("unused".into())) })
            }
            fn cancel_task(&self, _: TaskId) -> BoxGridFuture<'_, bool> {
                Box::pin(async { Ok(false) })
            }
            fn cluster_status(&self) -> BoxGridFuture<'_, GridClusterStatus> {
                Box::pin(async { Err(GridError::Config("unused".into())) })
            }
            fn list_workers(&self) -> BoxGridFuture<'_, serde_json::Value> {
                Box::pin(async { Ok(serde_json::json!([])) })
            }
            fn health_check(&self) -> BoxGridFuture<'_, ()> {
                Box::pin(async { Ok(()) })
            }
            fn close(&self) -> BoxFuture<'_, ()> {
                Box::pin(async {})
            }
        }

        let tid = Uuid::new_v4().to_string();
        let b_conn = AgentGridBridge::with_backend(Arc::new(ErrorStatusBackend {
            return_not_found: false,
        }));
        match b_conn
            .handle_command(&AgentMeshEnvelope::new_command_request(
                "peer",
                "grid",
                "grid_status",
                serde_json::json!({ "task_id": tid }),
                Some(2000),
            ))
            .await
        {
            AgentMeshEnvelope::CommandResponse { status, stderr, .. } => {
                assert_eq!(status, "failed");
                assert!(
                    stderr.starts_with("Failed to retrieve task status:"),
                    "{stderr}"
                );
            }
            other => panic!("expected CommandResponse, got {other:?}"),
        }

        let b_nf = AgentGridBridge::with_backend(Arc::new(ErrorStatusBackend {
            return_not_found: true,
        }));
        match b_nf
            .handle_command(&AgentMeshEnvelope::new_command_request(
                "peer",
                "grid",
                "grid_status",
                serde_json::json!({ "task_id": tid }),
                Some(2000),
            ))
            .await
        {
            AgentMeshEnvelope::CommandResponse { status, stderr, .. } => {
                assert_eq!(status, "failed");
                assert!(stderr.starts_with("Task not found:"), "{stderr}");
            }
            other => panic!("expected CommandResponse, got {other:?}"),
        }
    }
}
