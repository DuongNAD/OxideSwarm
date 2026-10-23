//! Model Context Protocol (MCP) server for OxideSwarm AI Agent integration.
//!
//! Provides a standard stdio JSON-RPC 2.0 server interface (and optional lightweight HTTP SSE mode)
//! allowing external AI coding agents (Claude Desktop, Cline, Cursor, custom agents) to directly
//! inspect, orchestrate, and execute workloads on an OxideSwarm cluster.
//!
//! # Protocol Compliance
//!
//! Follows the Model Context Protocol (MCP) specification (version `2024-11-05`):
//! - Lifecycle: `initialize`, `notifications/initialized`, `ping`
//! - Tools discovery: `tools/list`
//! - Tool execution: `tools/call`
//!
//! # Standard Tools
//!
//! 1. `status`: Cluster summary or task execution state for a specific task ID.
//! 2. `workers`: List connected workers with filtering by OS, architecture, and GPU.
//! 3. `submit`: Submit general commands, shell scripts, or GPU tasks with full placement constraints,
//!    environment variables, staged files, expected outputs, and synchronous wait options.
//! 4. `task_result`: Query task outcome, logs, exit code, and artifacts with optional blocking poll.
//! 5. `doctor`: Run cluster diagnostics (checks C1-C6) and return a structured report.
//!
//! # Stdout Hygiene
//!
//! Standard output (`stdout`) is strictly reserved for JSON-RPC 2.0 frames (`\n` terminated).
//! All diagnostics, tracing logs, and error messages are strictly routed to `stderr`.

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use uuid::Uuid;

use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::placement::{
    listed_worker_id, tag_value, Placement, ARCH_TAG_PREFIX, OS_TAG_PREFIX, REQUIRE_PREFIX,
    STATE_TAG_PREFIX,
};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse, MessageTransport};
use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskSpec};

use crate::config::ConfigFile;
use crate::connect::{
    connect_master, normalize_master_addr, prepare_staged_files, validate_output_paths,
    DEFAULT_MASTER_ADDR,
};
use crate::doctor::{run_doctor, DoctorOptions};
use crate::secret_cmd::SecretOpts;

/// Protocol version advertised in initialize handshake.
pub const MCP_PROTOCOL_VERSION: &str = "2024-11-05";
/// Server software name.
pub const MCP_SERVER_NAME: &str = "oxideswarm";
/// Server software version.
pub const MCP_SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// CLI arguments for the `oxideswarm mcp` command.
#[derive(Parser, Debug, Clone)]
pub struct McpArgs {
    #[arg(
        long,
        help = "Address of the Master's grid port (default 127.0.0.1:8088); a bare host gets :8088; 'auto' discovers it on the LAN"
    )]
    pub master: Option<String>,

    #[arg(
        long,
        help = "Run in SSE (Server-Sent Events) HTTP mode instead of stdio"
    )]
    pub sse: bool,

    #[arg(
        long,
        default_value = "127.0.0.1:8085",
        help = "HTTP bind address for SSE mode"
    )]
    pub sse_bind: String,

    #[arg(
        long,
        help = "Continue WITHOUT a cluster secret although the master is remote or auto-discovered"
    )]
    pub insecure: bool,
}

// ============================================================================
// JSON-RPC 2.0 Structures
// ============================================================================

#[derive(Debug, Clone, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    #[serde(default)]
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

impl JsonRpcResponse {
    pub fn success(id: Option<Value>, result: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        }
    }

    pub fn error(
        id: Option<Value>,
        code: i64,
        message: impl Into<String>,
        data: Option<Value>,
    ) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.into(),
                data,
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

// JSON-RPC 2.0 Error Codes
pub const PARSE_ERROR: i64 = -32700;
pub const INVALID_REQUEST: i64 = -32600;
pub const METHOD_NOT_FOUND: i64 = -32601;
pub const INVALID_PARAMS: i64 = -32602;
pub const INTERNAL_ERROR: i64 = -32603;

// ============================================================================
// MCP Tool Types
// ============================================================================

#[derive(Debug, Clone, Serialize)]
pub struct ToolCallResult {
    pub content: Vec<ToolContent>,
    #[serde(rename = "isError")]
    pub is_error: bool,
}

impl ToolCallResult {
    pub fn ok(text: impl Into<String>) -> Self {
        Self {
            content: vec![ToolContent::Text { text: text.into() }],
            is_error: false,
        }
    }

    pub fn ok_json(value: &Value) -> Self {
        let text = serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string());
        Self::ok(text)
    }

    pub fn error(text: impl Into<String>) -> Self {
        Self {
            content: vec![ToolContent::Text { text: text.into() }],
            is_error: true,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum ToolContent {
    #[serde(rename = "text")]
    Text { text: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct ToolDefinition {
    pub name: &'static str,
    pub description: &'static str,
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
}

// ============================================================================
// MCP State & Client Transport Manager
// ============================================================================

pub struct McpState {
    pub master_addr: String,
    pub secret: Option<ClusterSecret>,
    pub insecure: bool,
    transport: Mutex<Option<MessageTransport<TcpStream>>>,
}

impl McpState {
    pub fn new(master_addr: String, secret: Option<ClusterSecret>, insecure: bool) -> Self {
        Self {
            master_addr,
            secret,
            insecure,
            transport: Mutex::new(None),
        }
    }

    /// Sends a client request to the master, automatically reconnecting on dropped transport.
    pub async fn request(&self, message: &ClientMessage) -> Result<ClientResponse, String> {
        let mut guard = self.transport.lock().await;

        // Try existing transport
        if let Some(transport) = guard.as_mut() {
            if transport.send_msg(message).await.is_ok() {
                if let Ok(Some(resp)) = transport.recv_msg::<ClientResponse>().await {
                    return Ok(resp);
                }
            }
            // Transport failed; clear and reconnect
            *guard = None;
        }

        // Fresh connection
        let mut transport = connect_master(&self.master_addr, self.secret.as_ref())
            .await
            .map_err(|e| format!("Failed to connect to Master at {}: {e}", self.master_addr))?;

        transport
            .send_msg(message)
            .await
            .map_err(|e| format!("Failed to send request to Master: {e}"))?;

        let response = transport
            .recv_msg::<ClientResponse>()
            .await
            .map_err(|e| format!("Failed to receive response from Master: {e}"))?
            .ok_or_else(|| "Master closed connection unexpectedly".to_string())?;

        *guard = Some(transport);
        Ok(response)
    }

    /// Obtains a dedicated one-off connection (e.g. for long-running streaming/waiting operations).
    pub async fn connect_dedicated(&self) -> Result<MessageTransport<TcpStream>, String> {
        connect_master(&self.master_addr, self.secret.as_ref())
            .await
            .map_err(|e| format!("Failed to connect to Master at {}: {e}", self.master_addr))
    }
}

// ============================================================================
// Tool Registrations
// ============================================================================

pub fn available_tools() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "status",
            description: "Query cluster status, worker counts, or execution state of a specific task ID.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "task_id": {
                        "type": "string",
                        "description": "Optional UUID of a specific task to query"
                    },
                    "workers": {
                        "type": "boolean",
                        "description": "Whether to include full worker summaries in cluster status"
                    }
                }
            }),
        },
        ToolDefinition {
            name: "workers",
            description: "List worker nodes with filtering by connected state, OS, architecture, and GPU availability.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "connected_only": {
                        "type": "boolean",
                        "description": "Show only actively connected workers (default: true)"
                    },
                    "os": {
                        "type": "string",
                        "description": "Filter by OS (e.g. linux, windows, macos)"
                    },
                    "arch": {
                        "type": "string",
                        "description": "Filter by CPU architecture (e.g. x86_64, aarch64)"
                    },
                    "gpu_only": {
                        "type": "boolean",
                        "description": "Show only workers with GPU support"
                    }
                }
            }),
        },
        ToolDefinition {
            name: "submit",
            description: "Submit a compute workload (command, shell script, or GPU compute) to the grid cluster with placement constraints, environment variables, staged files, and artifact retrieval.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["command", "shell", "gpu"],
                        "description": "Task type (default: command)"
                    },
                    "command": {
                        "type": "string",
                        "description": "Command executable to run, or shell script string if type is shell"
                    },
                    "args": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Arguments for command execution"
                    },
                    "script": {
                        "type": "string",
                        "description": "Explicit script content for shell tasks"
                    },
                    "interpreter": {
                        "type": "string",
                        "description": "Interpreter for shell task (e.g. python3, bash)"
                    },
                    "stdin": {
                        "type": "string",
                        "description": "Standard input string to feed into task"
                    },
                    "env": {
                        "type": "object",
                        "additionalProperties": { "type": "string" },
                        "description": "Environment variables key-value map"
                    },
                    "cores": {
                        "type": "integer",
                        "description": "Minimum CPU cores required (default: 1)"
                    },
                    "ram_mb": {
                        "type": "integer",
                        "description": "Minimum RAM in MB required (default: 0)"
                    },
                    "gpu": {
                        "type": "boolean",
                        "description": "Require GPU execution (default: false)"
                    },
                    "os": {
                        "type": "string",
                        "description": "Target OS placement requirement (e.g. linux, windows, macos)"
                    },
                    "arch": {
                        "type": "string",
                        "description": "Target architecture requirement (e.g. x86_64, aarch64)"
                    },
                    "worker": {
                        "type": "string",
                        "description": "Target worker name or UUID"
                    },
                    "require_tags": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Required worker user tags"
                    },
                    "timeout_secs": {
                        "type": "integer",
                        "description": "Task timeout in seconds (default: 60)"
                    },
                    "max_retries": {
                        "type": "integer",
                        "description": "Maximum execution retries (default: 0)"
                    },
                    "wait": {
                        "type": "boolean",
                        "description": "Wait synchronously for task completion and return result (default: false)"
                    },
                    "matrix_dim": {
                        "type": "integer",
                        "description": "Square matrix dimension for GPU tasks (default: 64)"
                    },
                    "work_group_size": {
                        "type": "integer",
                        "description": "Workgroup tile size for GPU tasks (default: 16)"
                    },
                    "files": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Local files to stage into sandbox: 'LOCAL_PATH' or 'LOCAL_PATH:SANDBOX_PATH'"
                    },
                    "outputs": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Declared output artifact paths to retrieve upon completion"
                    }
                }
            }),
        },
        ToolDefinition {
            name: "task_result",
            description: "Fetch execution results, output logs, exit code, and artifacts for a task ID with optional blocking wait.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "task_id": {
                        "type": "string",
                        "description": "Task UUID to query"
                    },
                    "wait": {
                        "type": "boolean",
                        "description": "Wait until task finishes before returning (default: false)"
                    },
                    "timeout_secs": {
                        "type": "integer",
                        "description": "Max polling duration when waiting in seconds (default: 60)"
                    }
                },
                "required": ["task_id"]
            }),
        },
        ToolDefinition {
            name: "doctor",
            description: "Run diagnostic health checks (C1-C6) on the cluster and return a structured report.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "expect_workers": {
                        "type": "integer",
                        "description": "Minimum expected connected workers (default: 1)"
                    },
                    "timeout_secs": {
                        "type": "integer",
                        "description": "Probe timeout in seconds (default: 60)"
                    },
                    "gpu_dim": {
                        "type": "integer",
                        "description": "Matrix dimension for GPU probe (default: 512)"
                    },
                    "spread_tasks": {
                        "type": "integer",
                        "description": "Number of spread check tasks (default: 4 per worker)"
                    },
                    "skip_gpu": {
                        "type": "boolean",
                        "description": "Skip GPU probe C5 (default: false)"
                    },
                    "skip_spread": {
                        "type": "boolean",
                        "description": "Skip workload spread check C6 (default: false)"
                    }
                }
            }),
        },
    ]
}

// ============================================================================
// Tool Dispatcher
// ============================================================================

pub async fn call_tool(state: &McpState, name: &str, args: &Value) -> ToolCallResult {
    match name {
        "status" => tool_status(state, args).await,
        "workers" => tool_workers(state, args).await,
        "submit" => tool_submit(state, args).await,
        "task_result" => tool_task_result(state, args).await,
        "doctor" => tool_doctor(state, args).await,
        other => ToolCallResult::error(format!("Unknown tool: '{other}'")),
    }
}

// ----------------------------------------------------------------------------
// Tool: status
// ----------------------------------------------------------------------------

async fn tool_status(state: &McpState, args: &Value) -> ToolCallResult {
    let task_id_opt = args.get("task_id").and_then(Value::as_str);

    if let Some(task_id_str) = task_id_opt {
        let task_uuid = match Uuid::parse_str(task_id_str) {
            Ok(u) => u,
            Err(e) => {
                return ToolCallResult::error(format!("Invalid task_id UUID '{task_id_str}': {e}"))
            }
        };

        let msg = ClientMessage::GetTaskStatus {
            task_id: TaskId(task_uuid),
        };

        match state.request(&msg).await {
            Ok(ClientResponse::TaskStatusInfo {
                task_id,
                status,
                state_name,
                assigned_worker,
                error,
            }) => ToolCallResult::ok_json(&json!({
                "task_id": task_id.to_string(),
                "status": format!("{:?}", status),
                "state": state_name,
                "assigned_worker": assigned_worker,
                "error": error,
            })),
            Ok(ClientResponse::Error { message }) => ToolCallResult::error(message),
            Ok(other) => ToolCallResult::error(format!("Unexpected Master response: {other:?}")),
            Err(e) => ToolCallResult::error(e),
        }
    } else {
        // Query cluster workers & summary
        match state.request(&ClientMessage::ListWorkers).await {
            Ok(ClientResponse::WorkerList { workers }) => {
                let connected_count = workers
                    .iter()
                    .filter(|w| tag_value(&w.tags, STATE_TAG_PREFIX) != Some("disconnected"))
                    .count();

                let include_workers = args
                    .get("workers")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);

                let mut summary = json!({
                    "cluster_status": "ok",
                    "master": state.master_addr,
                    "total_workers": workers.len(),
                    "connected_workers": connected_count,
                });

                if include_workers {
                    let worker_list: Vec<Value> = workers
                        .iter()
                        .map(|w| {
                            let is_connected =
                                tag_value(&w.tags, STATE_TAG_PREFIX) != Some("disconnected");
                            json!({
                                "name": w.name,
                                "id": listed_worker_id(w),
                                "cpu_cores": w.cpu_cores,
                                "ram_mb": w.ram_mb,
                                "has_gpu": w.has_gpu,
                                "is_simulated_gpu": w.is_simulated_gpu,
                                "gpu_device_name": w.gpu_device_name,
                                "tags": w.tags,
                                "connected": is_connected,
                            })
                        })
                        .collect();
                    summary["workers"] = Value::Array(worker_list);
                }

                ToolCallResult::ok_json(&summary)
            }
            Ok(ClientResponse::Error { message }) => ToolCallResult::error(message),
            Ok(other) => ToolCallResult::error(format!("Unexpected Master response: {other:?}")),
            Err(e) => ToolCallResult::error(e),
        }
    }
}

// ----------------------------------------------------------------------------
// Tool: workers
// ----------------------------------------------------------------------------

async fn tool_workers(state: &McpState, args: &Value) -> ToolCallResult {
    let connected_only = args
        .get("connected_only")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let os_filter = args
        .get("os")
        .and_then(Value::as_str)
        .map(str::to_lowercase);
    let arch_filter = args
        .get("arch")
        .and_then(Value::as_str)
        .map(str::to_lowercase);
    let gpu_only = args
        .get("gpu_only")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    match state.request(&ClientMessage::ListWorkers).await {
        Ok(ClientResponse::WorkerList { workers }) => {
            let filtered: Vec<Value> = workers
                .into_iter()
                .filter(|w| {
                    let is_connected = tag_value(&w.tags, STATE_TAG_PREFIX) != Some("disconnected");
                    if connected_only && !is_connected {
                        return false;
                    }
                    if gpu_only && !w.has_gpu {
                        return false;
                    }
                    if let Some(target_os) = &os_filter {
                        let w_os = tag_value(&w.tags, OS_TAG_PREFIX).map(str::to_lowercase);
                        if w_os.as_deref() != Some(target_os.as_str()) {
                            return false;
                        }
                    }
                    if let Some(target_arch) = &arch_filter {
                        let w_arch = tag_value(&w.tags, ARCH_TAG_PREFIX).map(str::to_lowercase);
                        if w_arch.as_deref() != Some(target_arch.as_str()) {
                            return false;
                        }
                    }
                    true
                })
                .map(|w| {
                    let is_connected = tag_value(&w.tags, STATE_TAG_PREFIX) != Some("disconnected");
                    json!({
                        "name": w.name,
                        "id": listed_worker_id(&w),
                        "cpu_cores": w.cpu_cores,
                        "ram_mb": w.ram_mb,
                        "has_gpu": w.has_gpu,
                        "is_simulated_gpu": w.is_simulated_gpu,
                        "gpu_device_name": w.gpu_device_name,
                        "tags": w.tags,
                        "connected": is_connected,
                    })
                })
                .collect();

            ToolCallResult::ok_json(&Value::Array(filtered))
        }
        Ok(ClientResponse::Error { message }) => ToolCallResult::error(message),
        Ok(other) => ToolCallResult::error(format!("Unexpected Master response: {other:?}")),
        Err(e) => ToolCallResult::error(e),
    }
}

// ----------------------------------------------------------------------------
// Tool: submit
// ----------------------------------------------------------------------------

async fn tool_submit(state: &McpState, args: &Value) -> ToolCallResult {
    let task_type = args
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("command")
        .to_lowercase();
    let wait = args.get("wait").and_then(Value::as_bool).unwrap_or(false);

    // Environment variables
    let mut env_map = HashMap::new();
    if let Some(env_val) = args.get("env").and_then(Value::as_object) {
        for (k, v) in env_val {
            if let Some(val_str) = v.as_str() {
                env_map.insert(k.clone(), val_str.to_string());
            }
        }
    }

    // Build TaskSpec
    let spec = match task_type.as_str() {
        "gpu" => {
            let dim = args.get("matrix_dim").and_then(Value::as_u64).unwrap_or(64) as u32;
            let tile = args
                .get("work_group_size")
                .and_then(Value::as_u64)
                .unwrap_or(16) as u32;
            TaskSpec::GpuCompute {
                kernel_name: "matrix_multiply".into(),
                input_data: vec![1, 2, 3, 4],
                work_group_size: tile,
                simulated_matrix_dim: dim,
                compute_intensity: 10,
            }
        }
        "shell" => {
            let script = match args.get("script").and_then(Value::as_str) {
                Some(s) => s.to_string(),
                None => match args.get("command").and_then(Value::as_str) {
                    Some(c) => c.to_string(),
                    None => {
                        return ToolCallResult::error(
                            "Shell task requires 'script' or 'command' property",
                        )
                    }
                },
            };
            let interpreter = args
                .get("interpreter")
                .and_then(Value::as_str)
                .map(str::to_string);
            TaskSpec::ShellScript {
                script,
                interpreter,
                env: env_map,
            }
        }
        _ => {
            // Command
            let program = args
                .get("command")
                .and_then(Value::as_str)
                .unwrap_or("echo")
                .to_string();

            let cmd_args: Vec<String> = args
                .get("args")
                .and_then(Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_else(|| {
                    if program == "echo" {
                        vec!["Hello from OxideSwarm MCP".to_string()]
                    } else {
                        Vec::new()
                    }
                });

            let stdin = args
                .get("stdin")
                .and_then(Value::as_str)
                .map(|s| s.as_bytes().to_vec());

            TaskSpec::Command {
                program,
                args: cmd_args,
                env: env_map,
                working_dir: None,
                stdin,
            }
        }
    };

    // Requirements
    let cores = args.get("cores").and_then(Value::as_u64).unwrap_or(1) as usize;
    let ram_mb = args.get("ram_mb").and_then(Value::as_u64).unwrap_or(0);
    let gpu = args
        .get("gpu")
        .and_then(Value::as_bool)
        .unwrap_or(task_type == "gpu");
    let timeout_secs = args
        .get("timeout_secs")
        .and_then(Value::as_u64)
        .unwrap_or(60);
    let max_retries = args.get("max_retries").and_then(Value::as_u64).unwrap_or(0) as u32;

    let requirements =
        TaskRequirements::new(cores, ram_mb, gpu, timeout_secs).with_max_retries(max_retries);

    // Placement
    let mut placement_tags = Vec::new();
    if let Some(os) = args.get("os").and_then(Value::as_str) {
        placement_tags.push(format!("{REQUIRE_PREFIX}os={os}"));
    }
    if let Some(arch) = args.get("arch").and_then(Value::as_str) {
        placement_tags.push(format!("{REQUIRE_PREFIX}arch={arch}"));
    }
    if let Some(worker) = args.get("worker").and_then(Value::as_str) {
        placement_tags.push(format!("{REQUIRE_PREFIX}worker={worker}"));
    }
    if let Some(tags_arr) = args.get("require_tags").and_then(Value::as_array) {
        for t in tags_arr.iter().filter_map(Value::as_str) {
            placement_tags.push(format!("{REQUIRE_PREFIX}tag={t}"));
        }
    }

    let placement = match Placement::from_task_tags(&placement_tags) {
        Ok(p) => p,
        Err(e) => return ToolCallResult::error(format!("Invalid placement constraints: {e}")),
    };

    // Staged files
    let mut staged_files = Vec::new();
    if let Some(files_arr) = args.get("files").and_then(Value::as_array) {
        let string_paths: Vec<String> = files_arr
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect();
        if !string_paths.is_empty() {
            match prepare_staged_files(&string_paths) {
                Ok(files) => staged_files = files,
                Err(e) => return ToolCallResult::error(format!("Staged files error: {e}")),
            }
        }
    }

    // Expected outputs
    let mut expected_outputs = Vec::new();
    if let Some(outputs_arr) = args.get("outputs").and_then(Value::as_array) {
        let string_outputs: Vec<String> = outputs_arr
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect();
        if !string_outputs.is_empty() {
            match validate_output_paths(&string_outputs) {
                Ok(outs) => expected_outputs = outs,
                Err(e) => return ToolCallResult::error(format!("Expected outputs error: {e}")),
            }
        }
    }

    let mut task = Task::new(spec, requirements).with_placement(&placement);
    if !staged_files.is_empty() {
        task = task.with_staged_files(staged_files);
    }
    if !expected_outputs.is_empty() {
        task = task.with_expected_outputs(expected_outputs);
    }

    if wait {
        // Submit and wait synchronously on a dedicated connection
        let mut transport = match state.connect_dedicated().await {
            Ok(t) => t,
            Err(e) => return ToolCallResult::error(e),
        };

        let msg = ClientMessage::SubmitTask { task, wait: true };
        if let Err(e) = transport.send_msg(&msg).await {
            return ToolCallResult::error(format!("Failed to submit task: {e}"));
        }

        match transport.recv_msg::<ClientResponse>().await {
            Ok(Some(ClientResponse::TaskCompleted { result, .. })) => {
                let out_artifacts: Vec<Value> = result
                    .artifacts
                    .iter()
                    .map(|a| {
                        json!({
                            "path": a.path,
                            "size_bytes": a.size,
                            "checksum_sha256": a.sha256_hex(),
                            "data_preview": a.content().to_string_lossy(),
                        })
                    })
                    .collect();

                let status_str = if result.is_success() {
                    "completed"
                } else {
                    "failed"
                };

                ToolCallResult::ok_json(&json!({
                    "task_id": result.task_id.to_string(),
                    "status": status_str,
                    "exit_code": result.exit_code,
                    "stdout": result.stdout_str(),
                    "stderr": result.stderr_str(),
                    "duration_ms": result.execution_time_ms,
                    "error": result.error,
                    "artifacts": out_artifacts,
                }))
            }
            Ok(Some(ClientResponse::Error { message })) => ToolCallResult::error(message),
            Ok(Some(other)) => {
                ToolCallResult::error(format!("Unexpected Master response: {other:?}"))
            }
            Ok(None) => ToolCallResult::error("Master closed connection while waiting for task"),
            Err(e) => ToolCallResult::error(format!("Communication error: {e}")),
        }
    } else {
        // Submit asynchronously
        let msg = ClientMessage::SubmitTask { task, wait: false };
        match state.request(&msg).await {
            Ok(ClientResponse::TaskSubmitted { task_id }) => ToolCallResult::ok_json(&json!({
                "task_id": task_id.to_string(),
                "status": "submitted",
            })),
            Ok(ClientResponse::Error { message }) => ToolCallResult::error(message),
            Ok(other) => ToolCallResult::error(format!("Unexpected Master response: {other:?}")),
            Err(e) => ToolCallResult::error(e),
        }
    }
}

// ----------------------------------------------------------------------------
// Tool: task_result
// ----------------------------------------------------------------------------

async fn tool_task_result(state: &McpState, args: &Value) -> ToolCallResult {
    let task_id_str = match args.get("task_id").and_then(Value::as_str) {
        Some(s) => s,
        None => return ToolCallResult::error("Missing required property 'task_id'"),
    };

    let task_uuid = match Uuid::parse_str(task_id_str) {
        Ok(u) => u,
        Err(e) => {
            return ToolCallResult::error(format!("Invalid task_id UUID '{task_id_str}': {e}"))
        }
    };
    let task_id = TaskId(task_uuid);

    let wait = args.get("wait").and_then(Value::as_bool).unwrap_or(false);
    let timeout_secs = args
        .get("timeout_secs")
        .and_then(Value::as_u64)
        .unwrap_or(60);

    if wait {
        let deadline = Instant::now() + Duration::from_secs(timeout_secs);
        let mut transport = match state.connect_dedicated().await {
            Ok(t) => t,
            Err(e) => return ToolCallResult::error(e),
        };

        loop {
            let msg = ClientMessage::GetTaskResult { task_id };
            if let Err(e) = transport.send_msg(&msg).await {
                return ToolCallResult::error(format!("Failed to query task result: {e}"));
            }

            match transport.recv_msg::<ClientResponse>().await {
                Ok(Some(ClientResponse::TaskCompleted { result, .. })) => {
                    let out_artifacts: Vec<Value> = result
                        .artifacts
                        .iter()
                        .map(|a| {
                            json!({
                                "path": a.path,
                                "size_bytes": a.size,
                                "checksum_sha256": a.sha256_hex(),
                            })
                        })
                        .collect();

                    let status_str = if result.is_success() {
                        "completed"
                    } else {
                        "failed"
                    };

                    return ToolCallResult::ok_json(&json!({
                        "task_id": result.task_id.to_string(),
                        "status": status_str,
                        "exit_code": result.exit_code,
                        "stdout": result.stdout_str(),
                        "stderr": result.stderr_str(),
                        "duration_ms": result.execution_time_ms,
                        "error": result.error,
                        "artifacts": out_artifacts,
                    }));
                }
                Ok(Some(ClientResponse::TaskStatusInfo {
                    task_id: tid,
                    status,
                    state_name,
                    error,
                    ..
                })) => {
                    if Instant::now() >= deadline {
                        return ToolCallResult::ok_json(&json!({
                            "task_id": tid.to_string(),
                            "status": format!("{:?}", status),
                            "state": state_name,
                            "error": error,
                            "timed_out": true,
                        }));
                    }
                    tokio::time::sleep(Duration::from_millis(150)).await;
                }
                Ok(Some(ClientResponse::Error { message })) => {
                    return ToolCallResult::error(message)
                }
                Ok(Some(other)) => {
                    return ToolCallResult::error(format!("Unexpected Master response: {other:?}"))
                }
                Ok(None) => {
                    return ToolCallResult::error(
                        "Master closed connection while polling task result",
                    )
                }
                Err(e) => return ToolCallResult::error(format!("Communication error: {e}")),
            }
        }
    } else {
        match state
            .request(&ClientMessage::GetTaskResult { task_id })
            .await
        {
            Ok(ClientResponse::TaskCompleted { result, .. }) => {
                let out_artifacts: Vec<Value> = result
                    .artifacts
                    .iter()
                    .map(|a| {
                        json!({
                            "path": a.path,
                            "size_bytes": a.size,
                            "checksum_sha256": a.sha256_hex(),
                        })
                    })
                    .collect();

                let status_str = if result.is_success() {
                    "completed"
                } else {
                    "failed"
                };

                ToolCallResult::ok_json(&json!({
                    "task_id": result.task_id.to_string(),
                    "status": status_str,
                    "exit_code": result.exit_code,
                    "stdout": result.stdout_str(),
                    "stderr": result.stderr_str(),
                    "duration_ms": result.execution_time_ms,
                    "error": result.error,
                    "artifacts": out_artifacts,
                }))
            }
            Ok(ClientResponse::TaskStatusInfo {
                task_id,
                status,
                state_name,
                assigned_worker,
                error,
            }) => ToolCallResult::ok_json(&json!({
                "task_id": task_id.to_string(),
                "status": format!("{:?}", status),
                "state": state_name,
                "assigned_worker": assigned_worker,
                "error": error,
            })),
            Ok(ClientResponse::Error { message }) => ToolCallResult::error(message),
            Ok(other) => ToolCallResult::error(format!("Unexpected Master response: {other:?}")),
            Err(e) => ToolCallResult::error(e),
        }
    }
}

// ----------------------------------------------------------------------------
// Tool: doctor
// ----------------------------------------------------------------------------

async fn tool_doctor(state: &McpState, args: &Value) -> ToolCallResult {
    let mut options = DoctorOptions::default();

    if let Some(exp) = args.get("expect_workers").and_then(Value::as_u64) {
        options.expect_workers = exp as usize;
    }
    if let Some(to) = args.get("timeout_secs").and_then(Value::as_u64) {
        options.probe_timeout = Duration::from_secs(to);
    }
    if let Some(dim) = args.get("gpu_dim").and_then(Value::as_u64) {
        options.gpu_dim = dim as u32;
    }
    if let Some(spread) = args.get("spread_tasks").and_then(Value::as_u64) {
        options.spread_tasks = Some(spread as usize);
    }
    if let Some(skip_gpu) = args.get("skip_gpu").and_then(Value::as_bool) {
        options.skip_gpu = skip_gpu;
    }
    if let Some(skip_spread) = args.get("skip_spread").and_then(Value::as_bool) {
        options.skip_spread = skip_spread;
    }

    let report = run_doctor(&state.master_addr, state.secret.as_ref(), &options).await;
    match serde_json::to_value(&report) {
        Ok(val) => ToolCallResult::ok_json(&val),
        Err(e) => ToolCallResult::error(format!("Failed to serialize doctor report: {e}")),
    }
}

// ============================================================================
// Stdio Server Loop
// ============================================================================

/// Runs the MCP server in stdio mode, reading requests from stdin and responding to stdout.
pub async fn run_stdio_server(state: Arc<McpState>) -> ExitCode {
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin).lines();
    let mut stdout = tokio::io::stdout();

    eprintln!(
        "[mcp] OxideSwarm MCP Server v{} initialized (protocol: {}). Waiting for client frames on stdio...",
        MCP_SERVER_VERSION, MCP_PROTOCOL_VERSION
    );

    while let Ok(Some(line)) = reader.next_line().await {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let request: JsonRpcRequest = match serde_json::from_str(trimmed) {
            Ok(req) => req,
            Err(e) => {
                let err_resp = JsonRpcResponse::error(
                    None,
                    PARSE_ERROR,
                    format!("Invalid JSON-RPC frame: {e}"),
                    None,
                );
                send_response(&mut stdout, &err_resp).await;
                continue;
            }
        };

        if request.jsonrpc != "2.0" {
            let err_resp = JsonRpcResponse::error(
                request.id,
                INVALID_REQUEST,
                "Protocol version must be '2.0'",
                None,
            );
            send_response(&mut stdout, &err_resp).await;
            continue;
        }

        let id = request.id.clone();
        let method = request.method.as_str();

        match method {
            "initialize" => {
                let resp = JsonRpcResponse::success(
                    id,
                    json!({
                        "protocolVersion": MCP_PROTOCOL_VERSION,
                        "capabilities": {
                            "tools": {}
                        },
                        "serverInfo": {
                            "name": MCP_SERVER_NAME,
                            "version": MCP_SERVER_VERSION,
                        }
                    }),
                );
                send_response(&mut stdout, &resp).await;
            }
            "notifications/initialized" => {
                // Initialized notification has no response
                eprintln!("[mcp] Client handshake completed: notifications/initialized received");
            }
            "ping" => {
                let resp = JsonRpcResponse::success(id, json!({}));
                send_response(&mut stdout, &resp).await;
            }
            "tools/list" => {
                let tools = available_tools();
                let resp = JsonRpcResponse::success(id, json!({ "tools": tools }));
                send_response(&mut stdout, &resp).await;
            }
            "tools/call" => {
                let params = request.params.unwrap_or(Value::Null);
                let tool_name = params.get("name").and_then(Value::as_str);
                let arguments = params.get("arguments").cloned().unwrap_or(json!({}));

                match tool_name {
                    Some(name) => {
                        let tool_result = call_tool(&state, name, &arguments).await;
                        let resp = match serde_json::to_value(&tool_result) {
                            Ok(val) => JsonRpcResponse::success(id, val),
                            Err(e) => JsonRpcResponse::error(
                                id,
                                INTERNAL_ERROR,
                                format!("Serialization error: {e}"),
                                None,
                            ),
                        };
                        send_response(&mut stdout, &resp).await;
                    }
                    None => {
                        let resp = JsonRpcResponse::error(
                            id,
                            INVALID_PARAMS,
                            "Missing 'name' in tools/call parameters",
                            None,
                        );
                        send_response(&mut stdout, &resp).await;
                    }
                }
            }
            other => {
                // Unknown method
                let resp = JsonRpcResponse::error(
                    id,
                    METHOD_NOT_FOUND,
                    format!("Method not found: '{other}'"),
                    None,
                );
                send_response(&mut stdout, &resp).await;
            }
        }
    }

    eprintln!("[mcp] Stdio stream closed, exiting cleanly");
    ExitCode::SUCCESS
}

async fn send_response(stdout: &mut tokio::io::Stdout, response: &JsonRpcResponse) {
    if let Ok(serialized) = serde_json::to_string(response) {
        let _ = stdout.write_all(serialized.as_bytes()).await;
        let _ = stdout.write_all(b"\n").await;
        let _ = stdout.flush().await;
    }
}

// ============================================================================
// Lightweight SSE HTTP Server Mode (Optional)
// ============================================================================

pub async fn run_sse_server(state: Arc<McpState>, bind_addr: &str) -> ExitCode {
    eprintln!("[mcp] Starting MCP HTTP server on http://{bind_addr}...");
    let listener = match tokio::net::TcpListener::bind(bind_addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("[mcp] Failed to bind HTTP listener on {bind_addr}: {e}");
            return ExitCode::FAILURE;
        }
    };

    loop {
        let (mut socket, _peer_addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                eprintln!("[mcp] Accept error: {e}");
                continue;
            }
        };

        let state_clone = Arc::clone(&state);
        tokio::spawn(async move {
            let mut buf = vec![0u8; 8192];
            let n = match socket.read(&mut buf).await {
                Ok(n) if n > 0 => n,
                _ => return,
            };

            let req_str = String::from_utf8_lossy(&buf[..n]);
            // Simple HTTP header splitting
            if let Some((headers, body)) = req_str.split_once("\r\n\r\n") {
                if headers.starts_with("POST") {
                    let request_json: Result<JsonRpcRequest, _> = serde_json::from_str(body.trim());
                    let resp_body = match request_json {
                        Ok(req) => {
                            let id = req.id.clone();
                            let resp = match req.method.as_str() {
                                "initialize" => JsonRpcResponse::success(
                                    id,
                                    json!({
                                        "protocolVersion": MCP_PROTOCOL_VERSION,
                                        "capabilities": { "tools": {} },
                                        "serverInfo": { "name": MCP_SERVER_NAME, "version": MCP_SERVER_VERSION }
                                    }),
                                ),
                                "ping" => JsonRpcResponse::success(id, json!({})),
                                "tools/list" => JsonRpcResponse::success(
                                    id,
                                    json!({ "tools": available_tools() }),
                                ),
                                "tools/call" => {
                                    let params = req.params.unwrap_or(Value::Null);
                                    let name =
                                        params.get("name").and_then(Value::as_str).unwrap_or("");
                                    let args =
                                        params.get("arguments").cloned().unwrap_or(json!({}));
                                    let tool_res = call_tool(&state_clone, name, &args).await;
                                    JsonRpcResponse::success(
                                        id,
                                        serde_json::to_value(tool_res).unwrap(),
                                    )
                                }
                                other => JsonRpcResponse::error(
                                    id,
                                    METHOD_NOT_FOUND,
                                    format!("Method not found: '{other}'"),
                                    None,
                                ),
                            };
                            serde_json::to_string(&resp).unwrap_or_default()
                        }
                        Err(e) => serde_json::to_string(&JsonRpcResponse::error(
                            None,
                            PARSE_ERROR,
                            format!("Parse error: {e}"),
                            None,
                        ))
                        .unwrap_or_default(),
                    };

                    let http_resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        resp_body.len(),
                        resp_body
                    );
                    let _ = socket.write_all(http_resp.as_bytes()).await;
                } else if headers.starts_with("GET /sse") {
                    let sse_header = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\nevent: endpoint\r\ndata: /messages\r\n\r\n";
                    let _ = socket.write_all(sse_header.as_bytes()).await;
                } else {
                    let not_found =
                        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                    let _ = socket.write_all(not_found.as_bytes()).await;
                }
            }
        });
    }
}

// ============================================================================
// Top-Level Entrypoint
// ============================================================================

pub async fn run_mcp(
    args: McpArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let master_addr = normalize_master_addr(&crate::config::resolve_string(
        args.master,
        &["RUSTY_GRID_MASTER_ADDR", "RUSTY_GRID_MASTER"],
        None,
        DEFAULT_MASTER_ADDR,
    ));

    // Secret resolution: client role
    let secret =
        match secret_opts.resolve(config_file.as_ref(), crate::config::SecretSection::Status) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[mcp] Configuration error: {e}");
                return ExitCode::FAILURE;
            }
        };

    let state = Arc::new(McpState::new(master_addr, secret, args.insecure));

    if args.sse {
        run_sse_server(state, &args.sse_bind).await
    } else {
        run_stdio_server(state).await
    }
}
