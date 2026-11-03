//! Model Context Protocol (MCP) server for OxideSwarm AI Agent integration.
//!
//! Provides a standard stdio JSON-RPC 2.0 server interface
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
//! 5. `cancel`: Cancel a queued or running task by its UUID.
//! 6. `doctor`: Run cluster diagnostics (checks C1-C6) and return a structured report.
//!
//! # Stdout Hygiene
//!
//! Standard output (`stdout`) is strictly reserved for JSON-RPC 2.0 frames (`\n` terminated).
//! All diagnostics, tracing logs, and error messages are strictly routed to `stderr`.

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::JoinSet;
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

/// Default timeout for a single-frame Master RPC (`ListWorkers`, `GetTaskStatus`, `CancelTask`, etc.).
pub const DEFAULT_MASTER_RPC_TIMEOUT: Duration = Duration::from_secs(2);
/// Default margin added to `timeout_secs` when `wait_secs` is omitted on `submit {wait: true}`
/// so the worker's execution timeout (`exit_code: 124`) completes before the client wait deadline.
pub const DEFAULT_WAIT_MARGIN_SECS: u64 = 5;

pub struct McpState {
    pub master_addr: String,
    pub secret: Option<ClusterSecret>,
    resolved_master_addr: Mutex<Option<String>>,
    transport: Mutex<Option<MessageTransport<TcpStream>>>,
}

impl McpState {
    pub fn new(master_addr: String, secret: Option<ClusterSecret>) -> Self {
        Self {
            master_addr,
            secret,
            resolved_master_addr: Mutex::new(None),
            transport: Mutex::new(None),
        }
    }

    /// Resolves `self.master_addr` (running 3-tier LAN/HTTP/rendezvous discovery when `auto`)
    /// outside the per-RPC timeout, caching the resolved `host:port` so subsequent RPCs do not
    /// repeat discovery unless a connection fails.
    pub async fn resolve_target_addr(&self) -> Result<String, String> {
        if let Some(cached) = self.resolved_master_addr.lock().await.clone() {
            return Ok(cached);
        }
        let resolved = crate::connect::resolve_master_addr(&self.master_addr, self.secret.as_ref())
            .await
            .map_err(|e| format!("Failed to connect to Master at {}: {e}", self.master_addr))?;
        *self.resolved_master_addr.lock().await = Some(resolved.clone());
        Ok(resolved)
    }

    async fn invalidate_resolved_addr(&self) {
        *self.resolved_master_addr.lock().await = None;
    }

    /// Sends a client request to the master bounded by [`DEFAULT_MASTER_RPC_TIMEOUT`].
    pub async fn request(&self, message: &ClientMessage) -> Result<ClientResponse, String> {
        self.request_with_timeout(message, DEFAULT_MASTER_RPC_TIMEOUT)
            .await
    }

    /// Sends a client request to the master with progress bounded by `timeout`, without holding
    /// the pooled transport lock across network I/O and resolving `--master auto` outside the
    /// per-RPC timeout. Non-idempotent requests (`SubmitTask`) are never resent after the pooled
    /// write succeeds; only a failed pooled write falls back to a fresh connection.
    pub async fn request_with_timeout(
        &self,
        message: &ClientMessage,
        timeout: Duration,
    ) -> Result<ClientResponse, String> {
        let is_idempotent = !matches!(message, ClientMessage::SubmitTask { .. });
        let pooled = {
            let mut guard = self.transport.lock().await;
            guard.take()
        };

        // Try existing pooled transport outside the lock
        if let Some(mut transport) = pooled {
            if transport
                .send_msg_with_idle_timeout(message, timeout)
                .await
                .is_ok()
            {
                match transport
                    .recv_msg_with_idle_timeout::<ClientResponse>(timeout)
                    .await
                {
                    Ok(Some(resp)) => {
                        let mut guard = self.transport.lock().await;
                        if guard.is_none() {
                            *guard = Some(transport);
                        }
                        return Ok(resp);
                    }
                    Err(rusty_grid_core::protocol::ProtocolError::Io(ref io_err))
                        if io_err.kind() == std::io::ErrorKind::TimedOut =>
                    {
                        return Err(format!(
                            "Timed out waiting for response from Master at {}",
                            self.master_addr
                        ));
                    }
                    Ok(None) if !is_idempotent => {
                        return Err("Master closed connection unexpectedly".to_string());
                    }
                    Err(e) if !is_idempotent => {
                        return Err(format!("Failed to receive response from Master: {e}"));
                    }
                    Ok(None) | Err(_) => {
                        // Pooled idle connection was closed or reset before replying to an
                        // idempotent request; fall through to open a fresh connection.
                    }
                }
            }
        }

        // Fresh connection (discovery runs outside `timeout`, connect/handshake bounded by `timeout`)
        let mut transport = self.connect_dedicated_with_timeout(timeout).await?;

        transport
            .send_msg_with_idle_timeout(message, timeout)
            .await
            .map_err(|e| {
                if matches!(e, rusty_grid_core::protocol::ProtocolError::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::TimedOut) {
                    format!("Timed out sending request to Master at {}", self.master_addr)
                } else {
                    format!("Failed to send request to Master: {e}")
                }
            })?;

        let response = transport
            .recv_msg_with_idle_timeout::<ClientResponse>(timeout)
            .await
            .map_err(|e| {
                if matches!(e, rusty_grid_core::protocol::ProtocolError::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::TimedOut) {
                    format!(
                        "Timed out waiting for response from Master at {}",
                        self.master_addr
                    )
                } else {
                    format!("Failed to receive response from Master: {e}")
                }
            })?
            .ok_or_else(|| "Master closed connection unexpectedly".to_string())?;

        let mut guard = self.transport.lock().await;
        if guard.is_none() {
            *guard = Some(transport);
        }
        Ok(response)
    }

    /// Obtains a dedicated one-off connection (e.g. for long-running streaming/waiting operations).
    pub async fn connect_dedicated(&self) -> Result<MessageTransport<TcpStream>, String> {
        self.connect_dedicated_with_timeout(DEFAULT_MASTER_RPC_TIMEOUT)
            .await
    }

    /// Obtains a dedicated one-off connection with TCP connect + handshake bounded by `timeout`,
    /// resolving `--master auto` outside `timeout`.
    pub async fn connect_dedicated_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<MessageTransport<TcpStream>, String> {
        let had_cached = self.resolved_master_addr.lock().await.is_some();
        let target_addr = self.resolve_target_addr().await?;
        match tokio::time::timeout(timeout, connect_master(&target_addr, self.secret.as_ref()))
            .await
        {
            Ok(Ok(transport)) => Ok(transport),
            Ok(Err(e)) => {
                self.invalidate_resolved_addr().await;
                if had_cached {
                    if let Ok(fresh_addr) = self.resolve_target_addr().await {
                        if fresh_addr != target_addr {
                            if let Ok(Ok(t)) = tokio::time::timeout(
                                timeout,
                                connect_master(&fresh_addr, self.secret.as_ref()),
                            )
                            .await
                            {
                                return Ok(t);
                            }
                        }
                    }
                }
                Err(format!(
                    "Failed to connect to Master at {}: {e}",
                    self.master_addr
                ))
            }
            Err(_) => {
                self.invalidate_resolved_addr().await;
                Err(format!(
                    "Timed out connecting to Master at {}",
                    self.master_addr
                ))
            }
        }
    }

    /// Obtains a dedicated one-off connection bounded by `deadline`.
    pub async fn connect_dedicated_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<MessageTransport<TcpStream>, String> {
        let timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
        self.connect_dedicated_with_timeout(timeout).await
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
                    "wait_secs": {
                        "type": "integer",
                        "description": "Client wait deadline in seconds when wait is true (default: timeout_secs + 5)"
                    },
                    "max_retries": {
                        "type": "integer",
                        "description": "Maximum execution retries (default: cluster default)"
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
                    },
                    "wait_secs": {
                        "type": "integer",
                        "description": "Alias for timeout_secs: client wait deadline in seconds when waiting"
                    }
                },
                "required": ["task_id"]
            }),
        },
        ToolDefinition {
            name: "cancel",
            description: "Cancel a queued or running task by its UUID.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "task_id": {
                        "type": "string",
                        "description": "Task UUID to cancel"
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
// Tool Execution Context & Dispatcher
// ============================================================================

#[derive(Clone)]
pub struct ToolCallContext {
    pub cancel_tx: watch::Sender<bool>,
    pub cancel_rx: watch::Receiver<bool>,
    pub client_cancelled: Arc<std::sync::atomic::AtomicBool>,
    pub aborted_by_cancel: bool,
    pub submitted_task_id: Arc<Mutex<Option<TaskId>>>,
    pub progress_token: Option<Value>,
    pub out_tx: Option<mpsc::UnboundedSender<String>>,
}

impl Default for ToolCallContext {
    fn default() -> Self {
        let (tx, rx) = watch::channel(false);
        Self {
            cancel_tx: tx,
            cancel_rx: rx,
            client_cancelled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            aborted_by_cancel: false,
            submitted_task_id: Arc::new(Mutex::new(None)),
            progress_token: None,
            out_tx: None,
        }
    }
}

impl ToolCallContext {
    pub fn is_cancelled(&self) -> bool {
        *self.cancel_rx.borrow()
    }

    pub fn is_client_cancelled(&self) -> bool {
        self.client_cancelled
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn send_progress(&self, progress: u64, total: Option<u64>, message: impl Into<String>) {
        let (Some(token), Some(tx)) = (self.progress_token.as_ref(), self.out_tx.as_ref()) else {
            return;
        };
        let mut params = json!({
            "progressToken": token,
            "progress": progress,
            "message": message.into(),
        });
        if let Some(t) = total {
            params["total"] = json!(t);
        }
        let notification = json!({
            "jsonrpc": "2.0",
            "method": "notifications/progress",
            "params": params,
        });
        if let Ok(line) = serde_json::to_string(&notification) {
            let _ = tx.send(line);
        }
    }
}

pub async fn call_tool(state: &McpState, name: &str, args: &Value) -> ToolCallResult {
    let mut ctx = ToolCallContext::default();
    call_tool_with_context(state, name, args, &mut ctx).await
}

pub async fn call_tool_with_context(
    state: &McpState,
    name: &str,
    args: &Value,
    ctx: &mut ToolCallContext,
) -> ToolCallResult {
    match name {
        "status" => tool_status(state, args).await,
        "workers" => tool_workers(state, args).await,
        "submit" => tool_submit(state, args, ctx).await,
        "task_result" => tool_task_result(state, args, ctx).await,
        "cancel" => tool_cancel(state, args).await,
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

async fn check_matching_worker_warning(
    state: &McpState,
    placement: &Placement,
    requirements: &TaskRequirements,
    timeout: Duration,
) -> Option<String> {
    let Ok(ClientResponse::WorkerList { workers }) = state
        .request_with_timeout(&ClientMessage::ListWorkers, timeout)
        .await
    else {
        return None;
    };

    let any_matches = workers.iter().any(|w| {
        let connected = tag_value(&w.tags, STATE_TAG_PREFIX) != Some("disconnected");
        connected
            && placement.matches_listed(w)
            && (!requirements.gpu_required || w.has_gpu)
            && w.cpu_cores >= requirements.cpu_cores
            && w.ram_mb >= requirements.ram_mb
    });

    if any_matches {
        None
    } else {
        let msg = if !placement.is_empty() {
            format!(
                "Warning: no connected worker matches the placement constraints ({placement}); the task waits in the queue until one does"
            )
        } else {
            "Warning: no connected worker matches the task resource requirements; the task waits in the queue until one does".to_string()
        };
        eprintln!("[mcp] {msg}");
        Some(msg)
    }
}

async fn tool_submit(state: &McpState, args: &Value, ctx: &mut ToolCallContext) -> ToolCallResult {
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
    let wait_secs = args
        .get("wait_secs")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| timeout_secs.saturating_add(DEFAULT_WAIT_MARGIN_SECS));

    let mut requirements = TaskRequirements::new(cores, ram_mb, gpu, timeout_secs);
    if let Some(retries) = args.get("max_retries").and_then(Value::as_u64) {
        requirements = requirements.with_max_retries(retries as u32);
    }

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

    let mut task = Task::new(spec, requirements.clone()).with_placement(&placement);
    if !staged_files.is_empty() {
        task = task.with_staged_files(staged_files);
    }
    if !expected_outputs.is_empty() {
        task = task.with_expected_outputs(expected_outputs);
    }
    let task_id = task.id;
    *ctx.submitted_task_id.lock().await = Some(task_id);

    if ctx.is_cancelled() {
        ctx.aborted_by_cancel = true;
        return ToolCallResult::error(format!("Task {task_id} cancelled by client"));
    }

    if let Err(e) = state.resolve_target_addr().await {
        return ToolCallResult::error(e);
    }

    if ctx.is_cancelled() {
        ctx.aborted_by_cancel = true;
        return ToolCallResult::error(format!("Task {task_id} cancelled by client"));
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(wait_secs);
    let preflight_timeout =
        Duration::from_millis(500).min(Duration::from_secs(wait_secs.max(1)) / 2);
    let warning =
        check_matching_worker_warning(state, &placement, &requirements, preflight_timeout).await;

    if ctx.is_cancelled() {
        ctx.aborted_by_cancel = true;
        return ToolCallResult::error(format!("Task {task_id} cancelled by client"));
    }

    if wait {
        let submit_rpc_timeout =
            Duration::from_secs(wait_secs.max(1)).min(DEFAULT_MASTER_RPC_TIMEOUT);
        let mut transport = match state
            .connect_dedicated_with_timeout(submit_rpc_timeout)
            .await
        {
            Ok(t) => t,
            Err(e) => return ToolCallResult::error(e),
        };

        let msg = ClientMessage::SubmitTask { task, wait: true };
        if let Err(e) = transport
            .send_msg_with_idle_timeout(&msg, submit_rpc_timeout)
            .await
        {
            return if matches!(e, rusty_grid_core::protocol::ProtocolError::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::TimedOut)
            {
                ToolCallResult::error(format!(
                    "Timed out submitting task to Master at {}",
                    state.master_addr
                ))
            } else {
                ToolCallResult::error(format!("Failed to submit task: {e}"))
            };
        }

        ctx.send_progress(
            0,
            None,
            format!("Task {task_id} submitted; waiting for completion"),
        );

        let mut cancel_rx = ctx.cancel_rx.clone();
        let mut cancel_closed = false;
        let mut progress_tick = tokio::time::interval(Duration::from_millis(500));
        // Skip immediate first tick since we already sent progress 0 above.
        progress_tick.tick().await;
        let mut step = 0u64;

        let recv_outcome = loop {
            if *cancel_rx.borrow() {
                ctx.aborted_by_cancel = true;
                drop(transport);
                let _ = state
                    .request_with_timeout(
                        &ClientMessage::CancelTask { task_id },
                        Duration::from_millis(500),
                    )
                    .await;
                return ToolCallResult::error(format!("Task {task_id} cancelled by client"));
            }

            tokio::select! {
                biased;
                res = cancel_rx.changed(), if !cancel_closed => {
                    if res.is_err() {
                        cancel_closed = true;
                        continue;
                    }
                    if *cancel_rx.borrow() {
                        ctx.aborted_by_cancel = true;
                        drop(transport);
                        let _ = state
                            .request_with_timeout(
                                &ClientMessage::CancelTask { task_id },
                                Duration::from_millis(500),
                            )
                            .await;
                        return ToolCallResult::error(format!("Task {task_id} cancelled by client"));
                    }
                }
                res = transport.recv_msg::<ClientResponse>() => {
                    break Some(res);
                }
                _ = tokio::time::sleep_until(deadline) => {
                    break None;
                }
                _ = progress_tick.tick() => {
                    step += 1;
                    ctx.send_progress(step, None, format!("Waiting for task {task_id}"));
                }
            }
        };

        match recv_outcome {
            Some(Ok(Some(ClientResponse::TaskCompleted { result, .. }))) => {
                ctx.send_progress(
                    step + 1,
                    Some(step + 1),
                    format!("Task {task_id} completed"),
                );
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

                let mut body = json!({
                    "task_id": result.task_id.to_string(),
                    "status": status_str,
                    "exit_code": result.exit_code,
                    "stdout": result.stdout_str(),
                    "stderr": result.stderr_str(),
                    "duration_ms": result.execution_time_ms,
                    "error": result.error,
                    "artifacts": out_artifacts,
                });
                if let Some(w) = warning {
                    body["warning"] = Value::String(w);
                }
                ToolCallResult::ok_json(&body)
            }
            Some(Ok(Some(ClientResponse::Error { message }))) => ToolCallResult::error(message),
            Some(Ok(Some(other))) => {
                ToolCallResult::error(format!("Unexpected Master response: {other:?}"))
            }
            Some(Ok(None)) => {
                ToolCallResult::error("Master closed connection while waiting for task")
            }
            Some(Err(e)) => ToolCallResult::error(format!("Communication error: {e}")),
            None => {
                drop(transport);
                let (status_str, state_name) = match state
                    .request_with_timeout(
                        &ClientMessage::GetTaskStatus { task_id },
                        Duration::from_millis(500),
                    )
                    .await
                {
                    Ok(ClientResponse::TaskStatusInfo {
                        status, state_name, ..
                    }) => (format!("{:?}", status), state_name),
                    _ => ("Unknown".to_string(), "unknown".to_string()),
                };
                let mut body = json!({
                    "task_id": task_id.to_string(),
                    "status": status_str,
                    "state": state_name,
                    "timed_out": true,
                });
                if let Some(w) = warning {
                    body["warning"] = Value::String(w);
                }
                ToolCallResult::ok_json(&body)
            }
        }
    } else {
        // Submit asynchronously
        let msg = ClientMessage::SubmitTask { task, wait: false };
        let submit_timeout = args
            .get("wait_secs")
            .and_then(Value::as_u64)
            .map(Duration::from_secs)
            .unwrap_or(DEFAULT_MASTER_RPC_TIMEOUT);
        let remaining = deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .min(submit_timeout)
            .max(Duration::from_millis(100));

        match state.request_with_timeout(&msg, remaining).await {
            Ok(ClientResponse::TaskSubmitted { task_id }) => {
                if ctx.is_cancelled() {
                    ctx.aborted_by_cancel = true;
                    let _ = state
                        .request_with_timeout(
                            &ClientMessage::CancelTask { task_id },
                            Duration::from_millis(500),
                        )
                        .await;
                    return ToolCallResult::error(format!("Task {task_id} cancelled by client"));
                }
                ctx.send_progress(1, Some(1), format!("Task {task_id} submitted"));
                let mut body = json!({
                    "task_id": task_id.to_string(),
                    "status": "submitted",
                });
                if let Some(w) = warning {
                    body["warning"] = Value::String(w);
                }
                ToolCallResult::ok_json(&body)
            }
            Ok(ClientResponse::Error { message }) => ToolCallResult::error(message),
            Ok(other) => ToolCallResult::error(format!("Unexpected Master response: {other:?}")),
            Err(e) if e.starts_with("Timed out waiting for response") => {
                if ctx.is_cancelled() {
                    ctx.aborted_by_cancel = true;
                    let _ = state
                        .request_with_timeout(
                            &ClientMessage::CancelTask { task_id },
                            Duration::from_millis(500),
                        )
                        .await;
                    return ToolCallResult::error(format!("Task {task_id} cancelled by client"));
                }
                let (status_str, state_name) = match state
                    .request_with_timeout(
                        &ClientMessage::GetTaskStatus { task_id },
                        Duration::from_millis(500),
                    )
                    .await
                {
                    Ok(ClientResponse::TaskStatusInfo {
                        status, state_name, ..
                    }) => (format!("{:?}", status), state_name),
                    _ => ("Unknown".to_string(), "unknown".to_string()),
                };
                let mut body = json!({
                    "task_id": task_id.to_string(),
                    "status": status_str,
                    "state": state_name,
                    "timed_out": true,
                });
                if let Some(w) = warning {
                    body["warning"] = Value::String(w);
                }
                ToolCallResult::ok_json(&body)
            }
            Err(e) => ToolCallResult::error(e),
        }
    }
}

// ----------------------------------------------------------------------------
// Tool: cancel
// ----------------------------------------------------------------------------

async fn tool_cancel(state: &McpState, args: &Value) -> ToolCallResult {
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

    match state.request(&ClientMessage::CancelTask { task_id }).await {
        Ok(ClientResponse::TaskCancelled { task_id, success }) => ToolCallResult::ok_json(&json!({
            "task_id": task_id.to_string(),
            "cancelled": success,
        })),
        Ok(ClientResponse::Error { message }) => ToolCallResult::error(message),
        Ok(other) => ToolCallResult::error(format!("Unexpected Master response: {other:?}")),
        Err(e) => ToolCallResult::error(e),
    }
}

// ----------------------------------------------------------------------------
// Tool: task_result
// ----------------------------------------------------------------------------

async fn tool_task_result(
    state: &McpState,
    args: &Value,
    ctx: &mut ToolCallContext,
) -> ToolCallResult {
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
        .get("wait_secs")
        .or_else(|| args.get("timeout_secs"))
        .and_then(Value::as_u64)
        .unwrap_or(60);

    if wait {
        if let Err(e) = state.resolve_target_addr().await {
            return ToolCallResult::error(e);
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
        let mut transport = match state.connect_dedicated_until(deadline).await {
            Ok(t) => t,
            Err(e) => return ToolCallResult::error(e),
        };
        let mut cancel_rx = ctx.cancel_rx.clone();
        let mut cancel_closed = false;
        let mut step = 0u64;

        loop {
            if *cancel_rx.borrow() {
                ctx.aborted_by_cancel = true;
                return ToolCallResult::error(format!("Wait for task {task_id} cancelled"));
            }

            let msg = ClientMessage::GetTaskResult { task_id };
            let send_res = tokio::select! {
                biased;
                res = cancel_rx.changed(), if !cancel_closed => {
                    if res.is_err() {
                        cancel_closed = true;
                        continue;
                    }
                    if *cancel_rx.borrow() {
                        ctx.aborted_by_cancel = true;
                        return ToolCallResult::error(format!("Wait for task {task_id} cancelled"));
                    }
                    continue;
                }
                res = tokio::time::timeout_at(deadline, transport.send_msg(&msg)) => res,
            };
            match send_res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    return ToolCallResult::error(format!("Failed to query task result: {e}"))
                }
                Err(_) => {
                    return ToolCallResult::ok_json(&json!({
                        "task_id": task_id.to_string(),
                        "status": "Unknown",
                        "state": "unknown",
                        "timed_out": true,
                    }));
                }
            }

            let recv_res = tokio::select! {
                biased;
                res = cancel_rx.changed(), if !cancel_closed => {
                    if res.is_err() {
                        cancel_closed = true;
                        continue;
                    }
                    if *cancel_rx.borrow() {
                        ctx.aborted_by_cancel = true;
                        return ToolCallResult::error(format!("Wait for task {task_id} cancelled"));
                    }
                    continue;
                }
                res = tokio::time::timeout_at(deadline, transport.recv_msg::<ClientResponse>()) => res,
            };

            let resp = match recv_res {
                Ok(r) => r,
                Err(_) => {
                    return ToolCallResult::ok_json(&json!({
                        "task_id": task_id.to_string(),
                        "status": "Unknown",
                        "state": "unknown",
                        "timed_out": true,
                    }));
                }
            };

            match resp {
                Ok(Some(ClientResponse::TaskCompleted { result, .. })) => {
                    ctx.send_progress(
                        step + 1,
                        Some(step + 1),
                        format!("Task {task_id} completed"),
                    );
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
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        return ToolCallResult::ok_json(&json!({
                            "task_id": tid.to_string(),
                            "status": format!("{:?}", status),
                            "state": state_name,
                            "error": error,
                            "timed_out": true,
                        }));
                    }
                    step += 1;
                    ctx.send_progress(step, None, format!("Task {task_id} state: {state_name}"));
                    let sleep_until = (now + Duration::from_millis(150)).min(deadline);
                    tokio::select! {
                        biased;
                        res = cancel_rx.changed(), if !cancel_closed => {
                            if res.is_err() {
                                cancel_closed = true;
                            } else if *cancel_rx.borrow() {
                                ctx.aborted_by_cancel = true;
                                return ToolCallResult::error(format!("Wait for task {task_id} cancelled"));
                            }
                        }
                        _ = tokio::time::sleep_until(sleep_until) => {
                            if tokio::time::Instant::now() >= deadline {
                                return ToolCallResult::ok_json(&json!({
                                    "task_id": tid.to_string(),
                                    "status": format!("{:?}", status),
                                    "state": state_name,
                                    "error": error,
                                    "timed_out": true,
                                }));
                            }
                        }
                    }
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

    let target_addr = state
        .resolve_target_addr()
        .await
        .unwrap_or_else(|_| state.master_addr.clone());
    let report = run_doctor(&target_addr, state.secret.as_ref(), &options).await;
    match serde_json::to_value(&report) {
        Ok(val) => ToolCallResult::ok_json(&val),
        Err(e) => ToolCallResult::error(format!("Failed to serialize doctor report: {e}")),
    }
}

// ============================================================================
// Stdio Server Loop
// ============================================================================

struct InFlightCall {
    cancel_tx: watch::Sender<bool>,
    client_cancelled: Arc<std::sync::atomic::AtomicBool>,
    submitted_task_id: Arc<Mutex<Option<TaskId>>>,
}

fn request_id_key(id: &Value) -> Option<String> {
    if id.is_null() {
        None
    } else {
        serde_json::to_string(id).ok()
    }
}

fn extract_progress_token(params: &Value, arguments: &Value) -> Option<Value> {
    params
        .get("_meta")
        .and_then(|m| m.get("progressToken"))
        .or_else(|| params.get("progressToken"))
        .or_else(|| arguments.get("_meta").and_then(|m| m.get("progressToken")))
        .or_else(|| arguments.get("progressToken"))
        .filter(|v| !v.is_null())
        .cloned()
}

fn send_response_line(out_tx: &mpsc::UnboundedSender<String>, response: &JsonRpcResponse) {
    if let Ok(serialized) = serde_json::to_string(response) {
        let _ = out_tx.send(serialized);
    }
}

/// Runs the MCP server in stdio mode, using a reader loop, a dedicated writer task, and one
/// spawned task per `tools/call`.
pub async fn run_stdio_server(state: Arc<McpState>) -> ExitCode {
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin).lines();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();

    let writer_handle = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(line) = out_rx.recv().await {
            let _ = stdout.write_all(line.as_bytes()).await;
            let _ = stdout.write_all(b"\n").await;
            let _ = stdout.flush().await;
        }
    });

    let in_flight: Arc<Mutex<HashMap<String, InFlightCall>>> = Arc::new(Mutex::new(HashMap::new()));
    let active_cancels: Arc<Mutex<HashMap<u64, watch::Sender<bool>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let mut next_call_seq = 1u64;
    let mut call_tasks: JoinSet<()> = JoinSet::new();

    eprintln!(
        "[mcp] OxideSwarm MCP Server v{} initialized (protocol: {}). Waiting for client frames on stdio...",
        MCP_SERVER_VERSION, MCP_PROTOCOL_VERSION
    );

    loop {
        while call_tasks.try_join_next().is_some() {}

        let line = tokio::select! {
            line_res = reader.next_line() => {
                match line_res {
                    Ok(Some(line)) => line,
                    _ => break,
                }
            }
            Some(_) = call_tasks.join_next(), if !call_tasks.is_empty() => {
                continue;
            }
        };

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
                send_response_line(&out_tx, &err_resp);
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
            send_response_line(&out_tx, &err_resp);
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
                send_response_line(&out_tx, &resp);
            }
            "notifications/initialized" => {
                // Initialized notification has no response
                eprintln!("[mcp] Client handshake completed: notifications/initialized received");
            }
            "notifications/cancelled" => {
                let params = request.params.unwrap_or(Value::Null);
                if let Some(req_id) = params.get("requestId").or_else(|| params.get("id")) {
                    if let Some(key) = request_id_key(req_id) {
                        let maybe_tid = {
                            let guard = in_flight.lock().await;
                            if let Some(call) = guard.get(&key) {
                                call.client_cancelled
                                    .store(true, std::sync::atomic::Ordering::SeqCst);
                                let _ = call.cancel_tx.send(true);
                                Some(Arc::clone(&call.submitted_task_id))
                            } else {
                                None
                            }
                        };
                        if let Some(tid_mutex) = maybe_tid {
                            let maybe_tid_val = *tid_mutex.lock().await;
                            if let Some(task_id) = maybe_tid_val {
                                let state_clone = Arc::clone(&state);
                                tokio::spawn(async move {
                                    let _ = state_clone
                                        .request(&ClientMessage::CancelTask { task_id })
                                        .await;
                                });
                            }
                        }
                    }
                }
            }
            "ping" => {
                let resp = JsonRpcResponse::success(id, json!({}));
                send_response_line(&out_tx, &resp);
            }
            "tools/list" => {
                let tools = available_tools();
                let resp = JsonRpcResponse::success(id, json!({ "tools": tools }));
                send_response_line(&out_tx, &resp);
            }
            "tools/call" => {
                let params = request.params.unwrap_or(Value::Null);
                let tool_name = params
                    .get("name")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                let arguments = params.get("arguments").cloned().unwrap_or(json!({}));

                match tool_name {
                    Some(name) => {
                        let id_key = id.as_ref().and_then(request_id_key);
                        if let Some(ref key) = id_key {
                            let guard = in_flight.lock().await;
                            if guard.contains_key(key) {
                                drop(guard);
                                let resp = JsonRpcResponse::error(
                                    id,
                                    INVALID_REQUEST,
                                    format!("Duplicate in-flight request id: {key}"),
                                    None,
                                );
                                send_response_line(&out_tx, &resp);
                                continue;
                            }
                        }

                        let (cancel_tx, cancel_rx) = watch::channel(false);
                        let client_cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
                        let submitted_task_id = Arc::new(Mutex::new(None));
                        let call_seq = next_call_seq;
                        next_call_seq = next_call_seq.wrapping_add(1);
                        active_cancels
                            .lock()
                            .await
                            .insert(call_seq, cancel_tx.clone());

                        if let Some(ref key) = id_key {
                            in_flight.lock().await.insert(
                                key.clone(),
                                InFlightCall {
                                    cancel_tx: cancel_tx.clone(),
                                    client_cancelled: Arc::clone(&client_cancelled),
                                    submitted_task_id: Arc::clone(&submitted_task_id),
                                },
                            );
                        }

                        let progress_token = extract_progress_token(&params, &arguments);
                        let mut ctx = ToolCallContext {
                            cancel_tx,
                            cancel_rx,
                            client_cancelled,
                            aborted_by_cancel: false,
                            submitted_task_id,
                            progress_token,
                            out_tx: Some(out_tx.clone()),
                        };
                        let state_clone = Arc::clone(&state);
                        let in_flight_clone = Arc::clone(&in_flight);
                        let active_cancels_clone = Arc::clone(&active_cancels);
                        let out_tx_clone = out_tx.clone();

                        call_tasks.spawn(async move {
                            let tool_result =
                                call_tool_with_context(&state_clone, &name, &arguments, &mut ctx)
                                    .await;
                            active_cancels_clone.lock().await.remove(&call_seq);
                            if let Some(ref key) = id_key {
                                in_flight_clone.lock().await.remove(key);
                            }
                            if ctx.is_client_cancelled() || ctx.aborted_by_cancel {
                                return;
                            }
                            let resp = match serde_json::to_value(&tool_result) {
                                Ok(val) => JsonRpcResponse::success(id, val),
                                Err(e) => JsonRpcResponse::error(
                                    id,
                                    INTERNAL_ERROR,
                                    format!("Serialization error: {e}"),
                                    None,
                                ),
                            };
                            send_response_line(&out_tx_clone, &resp);
                        });
                    }
                    None => {
                        let resp = JsonRpcResponse::error(
                            id,
                            INVALID_PARAMS,
                            "Missing 'name' in tools/call parameters",
                            None,
                        );
                        send_response_line(&out_tx, &resp);
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
                send_response_line(&out_tx, &resp);
            }
        }
    }

    // Give in-flight calls (such as piped one-shot `doctor` or short `submit {wait: true}` calls)
    // a 2s grace window to finish naturally and emit their JSON-RPC replies, then cancel any
    // remaining long-running waits and bound cleanup to 500ms before aborting.
    let grace_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while !call_tasks.is_empty() {
        match tokio::time::timeout_at(grace_deadline, call_tasks.join_next()).await {
            Ok(Some(_)) => continue,
            _ => break,
        }
    }
    if !call_tasks.is_empty() {
        {
            let cancels = active_cancels.lock().await;
            for tx in cancels.values() {
                let _ = tx.send(true);
            }
        }
        let abort_deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        while !call_tasks.is_empty() {
            match tokio::time::timeout_at(abort_deadline, call_tasks.join_next()).await {
                Ok(Some(_)) => continue,
                _ => {
                    call_tasks.abort_all();
                    while call_tasks.join_next().await.is_some() {}
                    break;
                }
            }
        }
    }
    drop(out_tx);
    let _ = writer_handle.await;

    eprintln!("[mcp] Stdio stream closed, exiting cleanly");
    ExitCode::SUCCESS
}

// ============================================================================
// Top-Level Entrypoint
// ============================================================================

pub async fn run_mcp(
    args: McpArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let status_cfg = config_file.as_ref().and_then(|c| c.status.clone());
    let master_addr = normalize_master_addr(&crate::config::resolve_string(
        args.master,
        &["RUSTY_GRID_MASTER_ADDR", "RUSTY_GRID_MASTER"],
        status_cfg.as_ref().and_then(|s| s.master.clone()),
        DEFAULT_MASTER_ADDR,
    ));

    // Secret resolution and fail-closed client policy (same as `status`)
    let secret = match crate::secret_cmd::client_secret(
        &secret_opts,
        config_file.as_ref(),
        crate::config::SecretSection::Status,
        &master_addr,
        crate::secret_cmd::peer_insecure(
            args.insecure,
            status_cfg.as_ref().and_then(|s| s.insecure),
        ),
    ) {
        Ok(s) => s,
        Err(code) => return code,
    };

    let state = Arc::new(McpState::new(master_addr, secret));

    run_stdio_server(state).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_mcp_args_rejects_sse_flags() {
        assert!(McpArgs::try_parse_from(["mcp", "--sse"]).is_err());
        assert!(McpArgs::try_parse_from(["mcp", "--sse-bind", "127.0.0.1:8085"]).is_err());
        let ok = McpArgs::try_parse_from(["mcp", "--master", "127.0.0.1:8088"])
            .expect("valid mcp args should parse");
        assert_eq!(ok.master.as_deref(), Some("127.0.0.1:8088"));
        assert!(!ok.insecure);
    }
}
