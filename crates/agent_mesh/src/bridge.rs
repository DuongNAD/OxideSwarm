//! In-process AgentGridBridge translating Layer 3 Agent Mesh envelopes into Layer 1 Master Grid tasks.
//!
//! Provides turnkey ecosystem convergence between autonomous coding agents and the OxideSwarm
//! distributed supercomputing grid, enabling seamless compute shader offloading, distributed
//! compilation, and real-time cluster telemetry inspection.

use std::collections::HashMap;
use std::time::Duration;

use serde_json::Value;

use rusty_grid_core::task::{
    AutoRigSpec, Bytes, Generative3DSpec, Task, TaskId, TaskRequirements, TaskResult, TaskSpec,
};
use rusty_grid_master::registry::WorkerStatus;
use rusty_grid_master::server::MasterHandle;

use crate::protocol::{compute_sha256, AgentMeshEnvelope};

/// In-process bridge connecting `AgentMeshHub` to `MasterHandle`.
#[derive(Clone)]
pub struct AgentGridBridge {
    master: MasterHandle,
    default_timeout: Duration,
}

impl AgentGridBridge {
    /// Creates a new `AgentGridBridge` wrapping a running `MasterHandle`.
    pub fn new(master: MasterHandle) -> Self {
        Self {
            master,
            default_timeout: Duration::from_secs(60),
        }
    }

    /// Configures default execution timeout for bridged tasks.
    pub fn with_default_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = timeout;
        self
    }

    /// Returns a reference to the underlying `MasterHandle`.
    pub fn master(&self) -> &MasterHandle {
        &self.master
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
            let iterations = args.get("iterations").and_then(|v| v.as_u64()).unwrap_or(100_000) as u32;
            let duration_ms = args.get("duration_ms").and_then(|v| v.as_u64()).unwrap_or(0);
            let should_fail = args.get("should_fail").and_then(|v| v.as_bool()).unwrap_or(false);
            let require_gpu = args.get("require_gpu").and_then(|v| v.as_bool()).unwrap_or(false);
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

            let compute_intensity = args
                .get("compute_intensity")
                .and_then(|v| v.as_u64())
                .unwrap_or(1) as u32;

            let input_data = parse_input_bytes(args.get("input_data"));

            let gpu_required = args
                .get("gpu_required")
                .or_else(|| args.get("require_gpu"))
                .and_then(|v| v.as_bool())
                .unwrap_or(true);

            (
                TaskSpec::GpuCompute {
                    kernel_name,
                    input_data,
                    work_group_size,
                    simulated_matrix_dim,
                    compute_intensity,
                },
                TaskRequirements {
                    cpu_cores: 1,
                    ram_mb: 1024,
                    gpu_required,
                    timeout_secs,
                    max_retries: None,
                },
            )
        };

        let priority = args.get("priority").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
        let task = Task::new(spec, reqs).with_tags(vec!["grid_compute".into(), "agent_bridge".into()]);

        match self.master.submit_task_with_priority(task, priority).await {
            Ok(task_id) => {
                match self.master.wait_task(task_id, Some(timeout)).await {
                    Ok(task_res) => map_task_result_to_response(task_res, from, correlation_id, command),
                    Err(e) => make_error_response(
                        from,
                        correlation_id,
                        command,
                        124,
                        &format!("Compute execution failed or timed out: {e}"),
                    ),
                }
            }
            Err(e) => make_error_response(
                from,
                correlation_id,
                command,
                1,
                &format!("Failed to submit compute task: {e}"),
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
            args.get("compiler_flags").or_else(|| args.get("cargo_args")),
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
        let task = Task::new(spec, reqs).with_tags(vec!["grid_compile".into(), "agent_bridge".into()]);

        match self.master.submit_task_with_priority(task, priority).await {
            Ok(task_id) => {
                match self.master.wait_task(task_id, Some(timeout)).await {
                    Ok(task_res) => map_task_result_to_response(task_res, from, correlation_id, command),
                    Err(e) => make_error_response(
                        from,
                        correlation_id,
                        command,
                        124,
                        &format!("Compilation failed or timed out: {e}"),
                    ),
                }
            }
            Err(e) => make_error_response(
                from,
                correlation_id,
                command,
                1,
                &format!("Failed to submit compilation task: {e}"),
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
        let timeout_secs = timeout.as_secs().max(1);

        let spec_res: Result<TaskSpec, _> = if let Some(spec_val) = args.get("spec") {
            serde_json::from_value(spec_val.clone())
        } else if let Some(prog) = args.get("program").and_then(|v| v.as_str()) {
            let cmd_args: Vec<String> = args
                .get("args")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
                .unwrap_or_default();
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

        let reqs = if let Some(reqs_val) = args.get("requirements") {
            serde_json::from_value(reqs_val.clone()).unwrap_or_else(|_| TaskRequirements::generic(1, timeout_secs))
        } else {
            TaskRequirements::generic(1, timeout_secs)
        };

        let priority = args.get("priority").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
        let wait = args.get("wait").and_then(|v| v.as_bool()).unwrap_or(true);
        let task = Task::new(spec, reqs).with_tags(vec!["grid_submit".into(), "agent_bridge".into()]);

        match self.master.submit_task_with_priority(task, priority).await {
            Ok(task_id) => {
                if wait {
                    match self.master.wait_task(task_id, Some(timeout)).await {
                        Ok(task_res) => map_task_result_to_response(task_res, from, correlation_id, command),
                        Err(e) => make_error_response(
                            from,
                            correlation_id,
                            command,
                            124,
                            &format!("Task execution failed or timed out: {e}"),
                        ),
                    }
                } else {
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
                    if let AgentMeshEnvelope::CommandResponse { ref mut payload, .. } = resp {
                        *payload = Some(serde_json::json!({
                            "task_id": task_id.to_string(),
                            "status": "submitted"
                        }));
                    }
                    resp
                }
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
                    match self.master.get_task_info(task_id).await {
                        Ok(info) => {
                            let cp = self.master.get_checkpoint(&task_id).await;
                            let payload = serde_json::json!({
                                "task": info,
                                "checkpoint": cp,
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
                            if let AgentMeshEnvelope::CommandResponse { payload: ref mut p, .. } = resp {
                                *p = Some(payload);
                            }
                            resp
                        }
                        Err(e) => make_error_response(
                            from,
                            correlation_id,
                            command,
                            1,
                            &format!("Task not found: {e}"),
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
            let stats_res = self.master.queue_stats().await;
            let workers_res = self.master.list_workers().await;
            match (stats_res, workers_res) {
                (Ok(stats), Ok(workers)) => {
                    let active_workers = workers
                        .iter()
                        .filter(|w| w.status == WorkerStatus::Connected)
                        .count();
                    let payload = serde_json::json!({
                        "queue": stats,
                        "workers": workers,
                        "active_workers": active_workers,
                        "total_workers": workers.len(),
                    });
                    let stdout = format!(
                        "Grid Cluster Status:\nActive Workers: {}\nQueued Tasks: {}\nRunning Tasks: {}\nCompleted Tasks: {}\nFailed Tasks: {}\n",
                        active_workers, stats.queued, stats.running, stats.completed, stats.failed
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
                    if let AgentMeshEnvelope::CommandResponse { payload: ref mut p, .. } = resp {
                        *p = Some(payload);
                    }
                    resp
                }
                (Err(e), _) | (_, Err(e)) => make_error_response(
                    from,
                    correlation_id,
                    command,
                    1,
                    &format!("Failed to retrieve cluster status: {e}"),
                ),
            }
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

        let image_bytes = parse_binary_data(
            args.get("image")
                .or_else(|| args.get("image_data"))
                .or_else(|| args.get("image_bytes")),
        );

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

        let engine_preference = args
            .get("engine")
            .or_else(|| args.get("engine_preference"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_lowercase())
            .filter(|s| s != "auto");

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
                TaskSpec::Generative3D(ref s) => {
                    let is_sdf = s
                        .engine_preference
                        .as_deref()
                        .map(|e| e == "sdf" || e == "tier0_sdf" || e == "procedural")
                        .unwrap_or(false);
                    !is_sdf
                }
                _ => true,
            }
        };

        let reqs = TaskRequirements {
            cpu_cores: 2,
            ram_mb: if gpu_required { 4096 } else { 1024 },
            gpu_required,
            timeout_secs,
            max_retries: None,
        };

        let priority = args.get("priority").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
        let wait = args.get("wait").and_then(|v| v.as_bool()).unwrap_or(true);
        let task = Task::new(spec, reqs).with_tags(vec!["grid_3d_generate".into(), "agent_bridge".into()]);

        match self.master.submit_task_with_priority(task, priority).await {
            Ok(task_id) => {
                if wait {
                    match self.master.wait_task(task_id, Some(timeout)).await {
                        Ok(task_res) => map_task_result_to_response(task_res, from, correlation_id, command),
                        Err(e) => make_error_response(
                            from,
                            correlation_id,
                            command,
                            124,
                            &format!("3D generation failed or timed out: {e}"),
                        ),
                    }
                } else {
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
                    if let AgentMeshEnvelope::CommandResponse { ref mut payload, .. } = resp {
                        *payload = Some(serde_json::json!({
                            "task_id": task_id.to_string(),
                            "status": "submitted"
                        }));
                    }
                    resp
                }
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

    async fn handle_auto_rig(
        &self,
        command: &str,
        args: &Value,
        from: &str,
        correlation_id: &str,
        timeout: Duration,
    ) -> AgentMeshEnvelope {
        let input_mesh_bytes = parse_binary_data(
            args.get("input_mesh")
                .or_else(|| args.get("input_mesh_bytes"))
                .or_else(|| args.get("mesh")),
        );

        if input_mesh_bytes.is_empty() {
            return make_error_response(
                from,
                correlation_id,
                command,
                1,
                "AutoRig requires non-empty 'input_mesh' (base64, byte array, or file path)",
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
            .map(|arr| arr.iter().filter_map(|x| x.as_str().map(String::from)).collect())
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
        };

        let priority = args.get("priority").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
        let wait = args.get("wait").and_then(|v| v.as_bool()).unwrap_or(true);
        let task = Task::new(spec, reqs).with_tags(vec!["grid_auto_rig".into(), "agent_bridge".into()]);

        match self.master.submit_task_with_priority(task, priority).await {
            Ok(task_id) => {
                if wait {
                    match self.master.wait_task(task_id, Some(timeout)).await {
                        Ok(task_res) => map_task_result_to_response(task_res, from, correlation_id, command),
                        Err(e) => make_error_response(
                            from,
                            correlation_id,
                            command,
                            124,
                            &format!("AutoRig failed or timed out: {e}"),
                        ),
                    }
                } else {
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
                    if let AgentMeshEnvelope::CommandResponse { ref mut payload, .. } = resp {
                        *payload = Some(serde_json::json!({
                            "task_id": task_id.to_string(),
                            "status": "submitted"
                        }));
                    }
                    resp
                }
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

fn parse_binary_data(val: Option<&Value>) -> Vec<u8> {
    match val {
        Some(Value::Array(arr)) => arr
            .iter()
            .filter_map(|v| v.as_u64().map(|n| n as u8))
            .collect(),
        Some(Value::String(s)) => {
            let s_trim = s.trim();
            if s_trim.starts_with("0x") || s_trim.starts_with("0X") {
                (2..s_trim.len())
                    .step_by(2)
                    .filter_map(|i| {
                        if i + 2 <= s_trim.len() {
                            u8::from_str_radix(&s_trim[i..i + 2], 16).ok()
                        } else {
                            None
                        }
                    })
                    .collect()
            } else {
                use base64::engine::general_purpose::STANDARD;
                use base64::Engine;
                let p = std::path::Path::new(s_trim);
                if p.exists()
                    && (s_trim.ends_with(".glb")
                        || s_trim.ends_with(".gltf")
                        || s_trim.ends_with(".obj")
                        || s_trim.ends_with(".png")
                        || s_trim.ends_with(".jpg"))
                {
                    if let Ok(bytes) = std::fs::read(p) {
                        return bytes;
                    }
                }
                if let Ok(bytes) = STANDARD.decode(s_trim) {
                    if !bytes.is_empty() {
                        return bytes;
                    }
                }
                if let Ok(bytes) = std::fs::read(s_trim) {
                    bytes
                } else {
                    s_trim.as_bytes().to_vec()
                }
            }
        }
        _ => Vec::new(),
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
        arr.iter().filter_map(|v| v.as_str().map(String::from)).collect()
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
    let status = if res.exit_code == 0 { "success" } else { "failed" };
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
            obj.insert("asset_bytes_len".to_string(), serde_json::json!(asset_slice.len()));
            obj.insert("asset_sha256".to_string(), serde_json::json!(sha256));
            obj.insert("asset_base64".to_string(), serde_json::json!(b64));
        }
        if stdout.is_empty() {
            stdout = format!("Asset generated successfully ({} bytes, sha256: {})", asset_slice.len(), sha256);
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
    } = resp {
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
    if let AgentMeshEnvelope::CommandResponse {
        ref mut error,
        ..
    } = resp {
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
}
