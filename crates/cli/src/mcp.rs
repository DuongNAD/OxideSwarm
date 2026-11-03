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
    connect_master, normalize_master_addr, save_output_artifacts, DEFAULT_MASTER_ADDR,
};
use crate::doctor::{run_doctor, DoctorOptions};
use crate::secret_cmd::SecretOpts;

/// Protocol version advertised in initialize handshake.
pub const MCP_PROTOCOL_VERSION: &str = "2024-11-05";
/// Server software name.
pub const MCP_SERVER_NAME: &str = "oxideswarm";
/// Server software version.
pub const MCP_SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
/// Default inline output capture budget (16 KiB per stream, head + tail) returned to LLM callers.
pub const DEFAULT_MCP_MAX_OUTPUT_BYTES: usize = 16_384;

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
    /// per-RPC timeout. Idempotent requests fall back to a fresh connection if the pooled write
    /// or receive fails (including EOF or timeout); non-idempotent requests (`SubmitTask`) fall
    /// back only if the pooled write fails and are never resent after the pooled write succeeds.
    pub async fn request_with_timeout(
        &self,
        message: &ClientMessage,
        timeout: Duration,
    ) -> Result<ClientResponse, String> {
        self.request_with_retry_info(message, timeout)
            .await
            .map(|(resp, _)| resp)
    }

    /// Like [`McpState::request_with_timeout`], also returning `true` when the request was retried
    /// on a fresh connection after its write to the pooled connection had already succeeded.
    pub async fn request_with_retry_info(
        &self,
        message: &ClientMessage,
        timeout: Duration,
    ) -> Result<(ClientResponse, bool), String> {
        let is_idempotent = !matches!(message, ClientMessage::SubmitTask { .. });
        let mut retried_after_pooled_write = false;
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
                        return Ok((resp, false));
                    }
                    Err(rusty_grid_core::protocol::ProtocolError::Io(ref io_err))
                        if !is_idempotent && io_err.kind() == std::io::ErrorKind::TimedOut =>
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
                        // Pooled idle connection was closed, reset, or timed out before replying
                        // to an idempotent request; fall through to open a fresh connection.
                        retried_after_pooled_write = true;
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
        Ok((response, retried_after_pooled_write))
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
                        "description": "Local files or directories inside the working directory to stage into the sandbox via content-addressed blobs: 'LOCAL_PATH' or 'LOCAL_PATH:SANDBOX_PATH' (symbolic links and paths outside the working directory are refused)"
                    },
                    "outputs": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Declared output file paths or glob patterns to retrieve upon completion"
                    },
                    "output_dir": {
                        "type": "string",
                        "description": "Local directory to download declared output files into when wait is true"
                    },
                    "max_output_bytes": {
                        "type": "integer",
                        "description": "Maximum bytes of stdout and stderr to return inline (head + tail, default: 16384)"
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
                    },
                    "output_dir": {
                        "type": "string",
                        "description": "Local directory to download task output files into"
                    },
                    "max_output_bytes": {
                        "type": "integer",
                        "description": "Maximum bytes of stdout and stderr to return inline (head + tail, default: 16384)"
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
// MCP File Input Validation & Safe LLM Output Formatting
// ----------------------------------------------------------------------------

fn reject_symlinks_in_dir_tree(root: &std::path::Path) -> Result<(), String> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir)
            .map_err(|e| format!("cannot read directory '{}': {e}", dir.display()))?;
        for entry in entries {
            let entry =
                entry.map_err(|e| format!("cannot read directory '{}': {e}", dir.display()))?;
            let path = entry.path();
            let ft = entry
                .file_type()
                .map_err(|e| format!("cannot inspect '{}': {e}", path.display()))?;
            if ft.is_symlink() {
                return Err(format!(
                    "'{}' is a symbolic link; symbolic links are refused in MCP submit files",
                    path.display()
                ));
            }
            if ft.is_dir() {
                stack.push(path);
            }
        }
    }
    Ok(())
}

/// Validates that every `LOCAL[:SANDBOX]` spec in `specs` stays inside `cwd` and neither is nor
/// traverses a symbolic link, then hashes the inputs via [`crate::blobs::scan_inputs`].
pub fn scan_mcp_inputs_in_dir(
    cwd: &std::path::Path,
    specs: &[String],
) -> Result<crate::blobs::PreparedInputs, String> {
    use std::path::Component;

    let canon_cwd = std::fs::canonicalize(cwd)
        .map_err(|e| format!("cannot resolve working directory '{}': {e}", cwd.display()))?;

    let mut rewritten_specs = Vec::with_capacity(specs.len());
    for spec in specs {
        let (local, dest) = crate::blobs::parse_input_spec(spec)?;
        if local.as_os_str().is_empty() {
            return Err("input path cannot be empty".to_string());
        }
        if local
            .components()
            .any(|c| matches!(c, Component::ParentDir))
        {
            return Err(format!(
                "path '{}' must not contain '..' or leave the working directory",
                local.display()
            ));
        }

        let rel_path = if local.is_absolute() {
            local
                .strip_prefix(cwd)
                .or_else(|_| local.strip_prefix(&canon_cwd))
                .map_err(|_| {
                    format!(
                        "path '{}' is outside the working directory '{}'",
                        local.display(),
                        cwd.display()
                    )
                })?
                .to_path_buf()
        } else {
            local.clone()
        };

        let mut cur = cwd.to_path_buf();
        let mut saw_normal = false;
        for comp in rel_path.components() {
            match comp {
                Component::CurDir => {}
                Component::Normal(part) => {
                    saw_normal = true;
                    cur.push(part);
                    let meta = std::fs::symlink_metadata(&cur)
                        .map_err(|e| format!("'{}': {e}", local.display()))?;
                    if meta.file_type().is_symlink() {
                        return Err(format!(
                            "'{}' is or traverses a symbolic link ('{}'), which is refused in MCP submit files",
                            local.display(),
                            cur.display()
                        ));
                    }
                }
                _ => {
                    return Err(format!(
                        "path '{}' is not a relative path inside the working directory",
                        local.display()
                    ));
                }
            }
        }

        let target_meta =
            std::fs::symlink_metadata(&cur).map_err(|e| format!("'{}': {e}", local.display()))?;
        if target_meta.file_type().is_symlink() {
            return Err(format!(
                "'{}' is a symbolic link, which is refused in MCP submit files",
                local.display()
            ));
        }
        let canon_target =
            std::fs::canonicalize(&cur).map_err(|e| format!("'{}': {e}", local.display()))?;
        if !canon_target.starts_with(&canon_cwd) {
            return Err(format!(
                "path '{}' resolves outside the working directory '{}'",
                local.display(),
                cwd.display()
            ));
        }
        if target_meta.is_dir() {
            reject_symlinks_in_dir_tree(&cur)?;
        }

        let effective_dest = match dest {
            Some(d) => Some(d),
            None if saw_normal => Some(rel_path.to_string_lossy().replace('\\', "/")),
            None => None,
        };
        let rewritten = match effective_dest {
            Some(d) => format!("{}:{d}", cur.display()),
            None => cur.display().to_string(),
        };
        rewritten_specs.push(rewritten);
    }

    crate::blobs::scan_inputs(&rewritten_specs)
}

const WORKER_TRUNCATION_MARKER: &[u8] =
    b"[rusty_grid: output truncated after exceeding size limit]";

fn trim_utf8_end(slice: &[u8]) -> &[u8] {
    match std::str::from_utf8(slice) {
        Ok(_) => slice,
        Err(e) => {
            if e.error_len().is_none() {
                &slice[..e.valid_up_to()]
            } else {
                slice
            }
        }
    }
}

fn trim_utf8_start(slice: &[u8]) -> &[u8] {
    let mut start = 0;
    while start < slice.len() && start < 4 && (slice[start] & 0b1100_0000) == 0b1000_0000 {
        start += 1;
    }
    &slice[start..]
}

/// Formats a raw output stream (`stdout` or `stderr`) for an LLM tool reply, keeping head + tail
/// within `max_output_bytes` and reporting `(formatted_text, was_truncated, total_bytes)`.
pub fn truncate_stream_for_llm(bytes: &[u8], max_output_bytes: usize) -> (String, bool, usize) {
    let total_bytes = bytes.len();
    let has_worker_notice = bytes
        .windows(WORKER_TRUNCATION_MARKER.len())
        .any(|w| w == WORKER_TRUNCATION_MARKER);

    if total_bytes <= max_output_bytes {
        return (
            String::from_utf8_lossy(bytes).into_owned(),
            has_worker_notice,
            total_bytes,
        );
    }

    if max_output_bytes == 0 {
        return (String::new(), true, total_bytes);
    }

    let omitted = total_bytes.saturating_sub(max_output_bytes);
    let notice = format!("\n...[truncated {omitted} bytes]...\n");
    if max_output_bytes <= notice.len() + 2 {
        let head_len = max_output_bytes / 2;
        let tail_len = max_output_bytes - head_len;
        let head = String::from_utf8_lossy(trim_utf8_end(&bytes[..head_len]));
        let tail = String::from_utf8_lossy(trim_utf8_start(&bytes[total_bytes - tail_len..]));
        return (format!("{head}{tail}"), true, total_bytes);
    }

    let budget = max_output_bytes - notice.len();
    let head_len = budget / 2;
    let tail_len = budget - head_len;
    let head = String::from_utf8_lossy(trim_utf8_end(&bytes[..head_len]));
    let tail = String::from_utf8_lossy(trim_utf8_start(&bytes[total_bytes - tail_len..]));
    (format!("{head}{notice}{tail}"), true, total_bytes)
}

async fn format_completed_task_result(
    state: &McpState,
    result: &rusty_grid_core::task::TaskResult,
    args: &Value,
    warning: Option<String>,
) -> ToolCallResult {
    let output_dir_opt = args.get("output_dir").and_then(Value::as_str);
    let max_output_bytes = args
        .get("max_output_bytes")
        .and_then(Value::as_u64)
        .map(|n| n as usize)
        .unwrap_or(DEFAULT_MCP_MAX_OUTPUT_BYTES);

    let mut saved_outputs: Option<Vec<std::path::PathBuf>> = None;
    let mut saved_artifacts: Option<Vec<std::path::PathBuf>> = None;

    if let Some(out_dir_str) = output_dir_opt {
        let out_dir = std::path::Path::new(out_dir_str);
        if !result.outputs.is_empty() {
            let target_addr = match state.resolve_target_addr().await {
                Ok(addr) => addr,
                Err(e) => return ToolCallResult::error(e),
            };
            let conn =
                match crate::blobs::BlobConnection::connect(&target_addr, state.secret.as_ref())
                    .await
                {
                    Ok(c) => c,
                    Err(e) => {
                        return ToolCallResult::error(format!(
                            "Failed to connect to Master for output download: {e}"
                        ))
                    }
                };
            match crate::blobs::download_outputs(&conn, &result.outputs, out_dir).await {
                Ok(paths) => saved_outputs = Some(paths),
                Err(e) => {
                    return ToolCallResult::error(format!(
                        "Failed to download output files to '{}': {e}",
                        out_dir.display()
                    ))
                }
            }
        }
        if !result.artifacts.is_empty() {
            match save_output_artifacts(&result.artifacts, out_dir) {
                Ok(paths) => saved_artifacts = Some(paths),
                Err(e) => {
                    return ToolCallResult::error(format!(
                        "Failed to save output artifacts to '{}': {e}",
                        out_dir.display()
                    ))
                }
            }
        }
    }

    let mut out_artifacts: Vec<Value> = Vec::new();
    for (idx, output) in result.outputs.iter().enumerate() {
        let mut entry = json!({
            "path": output.path,
            "size_bytes": output.size,
            "blob_id": output.id.to_hex(),
            "checksum_blake3": output.id.to_hex(),
            "executable": output.executable,
        });
        if let Some(ref paths) = saved_outputs {
            if let Some(saved_path) = paths.get(idx) {
                entry["saved_path"] = Value::String(saved_path.display().to_string());
            }
        }
        out_artifacts.push(entry);
    }
    for (idx, artifact) in result.artifacts.iter().enumerate() {
        let mut entry = json!({
            "path": artifact.path,
            "size_bytes": artifact.size,
            "checksum_sha256": artifact.sha256_hex(),
        });
        if let Some(ref paths) = saved_artifacts {
            if let Some(saved_path) = paths.get(idx) {
                entry["saved_path"] = Value::String(saved_path.display().to_string());
            }
        }
        out_artifacts.push(entry);
    }

    let (stdout_str, stdout_truncated, stdout_total_bytes) =
        truncate_stream_for_llm(result.stdout.as_bytes(), max_output_bytes);
    let (stderr_str, stderr_truncated, stderr_total_bytes) =
        truncate_stream_for_llm(result.stderr.as_bytes(), max_output_bytes);
    let truncated = stdout_truncated || stderr_truncated;
    let total_bytes = stdout_total_bytes + stderr_total_bytes;

    let status_str = if result.is_success() {
        "completed"
    } else {
        "failed"
    };

    let mut body = json!({
        "task_id": result.task_id.to_string(),
        "status": status_str,
        "exit_code": result.exit_code,
        "stdout": stdout_str,
        "stderr": stderr_str,
        "truncated": truncated,
        "total_bytes": total_bytes,
        "stdout_truncated": stdout_truncated,
        "stdout_total_bytes": stdout_total_bytes,
        "stderr_truncated": stderr_truncated,
        "stderr_total_bytes": stderr_total_bytes,
        "duration_ms": result.execution_time_ms,
        "error": result.error,
        "artifacts": out_artifacts,
    });
    if let Some(w) = warning {
        body["warning"] = Value::String(w);
    }
    ToolCallResult::ok_json(&body)
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

    // Staged files (content-addressed blobs, restricted to non-symlink paths inside the working directory)
    let mut prepared_inputs = crate::blobs::PreparedInputs::default();
    if let Some(files_arr) = args.get("files").and_then(Value::as_array) {
        let string_paths: Vec<String> = files_arr
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect();
        if !string_paths.is_empty() {
            let cwd = match std::env::current_dir() {
                Ok(dir) => dir,
                Err(e) => {
                    return ToolCallResult::error(format!(
                        "Staged files error: cannot determine working directory: {e}"
                    ))
                }
            };
            match scan_mcp_inputs_in_dir(&cwd, &string_paths) {
                Ok(inputs) => prepared_inputs = inputs,
                Err(e) => return ToolCallResult::error(format!("Staged files error: {e}")),
            }
        }
    }

    // Expected outputs (blob output globs)
    let mut output_globs = Vec::new();
    if let Some(outputs_arr) = args.get("outputs").and_then(Value::as_array) {
        let string_outputs: Vec<String> = outputs_arr
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect();
        if !string_outputs.is_empty() {
            if let Err(e) = rusty_grid_core::blob::validate_output_patterns(&string_outputs) {
                return ToolCallResult::error(format!("Expected outputs error: {e}"));
            }
            output_globs = string_outputs;
        }
    }

    let mut task = Task::new(spec, requirements.clone()).with_placement(&placement);
    if !prepared_inputs.inputs.is_empty() {
        task = task.with_inputs(prepared_inputs.inputs.clone());
    }
    if !output_globs.is_empty() {
        task = task.with_output_globs(output_globs);
    }
    let task_id = task.id;
    *ctx.submitted_task_id.lock().await = Some(task_id);

    if ctx.is_cancelled() {
        ctx.aborted_by_cancel = true;
        return ToolCallResult::error(format!("Task {task_id} cancelled by client"));
    }

    let target_addr = match state.resolve_target_addr().await {
        Ok(addr) => addr,
        Err(e) => return ToolCallResult::error(e),
    };

    if ctx.is_cancelled() {
        ctx.aborted_by_cancel = true;
        return ToolCallResult::error(format!("Task {task_id} cancelled by client"));
    }

    if !prepared_inputs.inputs.is_empty() {
        let conn = match crate::blobs::BlobConnection::connect(&target_addr, state.secret.as_ref())
            .await
        {
            Ok(c) => c,
            Err(e) => {
                return ToolCallResult::error(format!(
                    "Failed to connect to Master for input blob upload: {e}"
                ))
            }
        };
        if let Err(e) = crate::blobs::upload_inputs(&conn, &prepared_inputs).await {
            return ToolCallResult::error(format!("Failed to upload input files: {e}"));
        }
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
                format_completed_task_result(state, &result, args, warning).await
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
            Err(e)
                if e.starts_with("Timed out waiting for response")
                    || e == "Master closed connection unexpectedly"
                    || e.starts_with("Failed to receive response from Master") =>
            {
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
                let (status_str, state_name, is_error, detail) = match state
                    .request_with_timeout(
                        &ClientMessage::GetTaskStatus { task_id },
                        Duration::from_millis(500),
                    )
                    .await
                {
                    Ok(ClientResponse::TaskStatusInfo {
                        status, state_name, ..
                    }) => (
                        format!("{:?}", status),
                        state_name,
                        false,
                        format!(
                            "Task {task_id} was submitted ({e}); query its outcome with task_result or status"
                        ),
                    ),
                    Ok(ClientResponse::Error { message }) if message.contains("not found") => {
                        // A frame written before a receive timeout may still be in flight or may
                        // have been lost when the pooled connection closed. Resend the same
                        // `SubmitTask` (with the same `task_id`) once on a fresh connection: the
                        // Master deduplicates by `task_id`, so treating `already exists` as
                        // submitted guarantees at most one task is queued.
                        match state
                            .request_with_timeout(&msg, DEFAULT_MASTER_RPC_TIMEOUT)
                            .await
                        {
                            Ok(ClientResponse::TaskSubmitted { task_id }) => {
                                if ctx.is_cancelled() {
                                    ctx.aborted_by_cancel = true;
                                    let _ = state
                                        .request_with_timeout(
                                            &ClientMessage::CancelTask { task_id },
                                            Duration::from_millis(500),
                                        )
                                        .await;
                                    return ToolCallResult::error(format!(
                                        "Task {task_id} cancelled by client"
                                    ));
                                }
                                ctx.send_progress(1, Some(1), format!("Task {task_id} submitted"));
                                let mut body = json!({
                                    "task_id": task_id.to_string(),
                                    "status": "submitted",
                                });
                                if let Some(w) = warning {
                                    body["warning"] = Value::String(w);
                                }
                                return ToolCallResult::ok_json(&body);
                            }
                            Ok(ClientResponse::Error { ref message })
                                if message.contains("already exists") =>
                            {
                                if ctx.is_cancelled() {
                                    ctx.aborted_by_cancel = true;
                                    let _ = state
                                        .request_with_timeout(
                                            &ClientMessage::CancelTask { task_id },
                                            Duration::from_millis(500),
                                        )
                                        .await;
                                    return ToolCallResult::error(format!(
                                        "Task {task_id} cancelled by client"
                                    ));
                                }
                                ctx.send_progress(1, Some(1), format!("Task {task_id} submitted"));
                                let mut body = json!({
                                    "task_id": task_id.to_string(),
                                    "status": "submitted",
                                });
                                if let Some(w) = warning {
                                    body["warning"] = Value::String(w);
                                }
                                return ToolCallResult::ok_json(&body);
                            }
                            Ok(ClientResponse::Error { message }) => {
                                return ToolCallResult::error(message);
                            }
                            _ => (
                                "Unknown".to_string(),
                                "unknown".to_string(),
                                false,
                                format!(
                                    "Task {task_id} may have been submitted ({e}); query its outcome with task_result or status"
                                ),
                            ),
                        }
                    }
                    _ => (
                        "Unknown".to_string(),
                        "unknown".to_string(),
                        false,
                        format!(
                            "Task {task_id} may have been submitted ({e}); query its outcome with task_result or status"
                        ),
                    ),
                };
                let mut body = json!({
                    "task_id": task_id.to_string(),
                    "status": status_str,
                    "state": state_name,
                    "timed_out": true,
                    "message": detail,
                });
                if let Some(w) = warning {
                    body["warning"] = Value::String(w);
                }
                ToolCallResult {
                    content: vec![ToolContent::Text {
                        text: serde_json::to_string_pretty(&body)
                            .unwrap_or_else(|_| body.to_string()),
                    }],
                    is_error,
                }
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

    match state
        .request_with_retry_info(
            &ClientMessage::CancelTask { task_id },
            DEFAULT_MASTER_RPC_TIMEOUT,
        )
        .await
    {
        Ok((ClientResponse::TaskCancelled { task_id, success }, retried_after_pooled_write)) => {
            let mut cancelled = success;
            if !cancelled && retried_after_pooled_write {
                if let Ok(ClientResponse::TaskStatusInfo { status, .. }) = state
                    .request_with_timeout(
                        &ClientMessage::GetTaskStatus { task_id },
                        Duration::from_millis(500),
                    )
                    .await
                {
                    if matches!(status, rusty_grid_core::TaskStatus::Cancelled) {
                        cancelled = true;
                    }
                }
            }
            ToolCallResult::ok_json(&json!({
                "task_id": task_id.to_string(),
                "cancelled": cancelled,
            }))
        }
        Ok((ClientResponse::Error { message }, _)) => ToolCallResult::error(message),
        Ok((other, _)) => ToolCallResult::error(format!("Unexpected Master response: {other:?}")),
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

    if wait && timeout_secs > 0 {
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
                    return format_completed_task_result(state, &result, args, None).await;
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
                format_completed_task_result(state, &result, args, None).await
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

    #[test]
    fn test_scan_mcp_inputs_refuses_outside_cwd_and_symlinks() {
        let outer = tempfile::tempdir().unwrap();
        let cwd = outer.path().join("workspace");
        std::fs::create_dir_all(cwd.join("sub")).unwrap();
        std::fs::write(cwd.join("sub/inside.txt"), b"hello-inside").unwrap();
        let secret_outside = outer.path().join("outside_secret.txt");
        std::fs::write(&secret_outside, b"do-not-exfiltrate").unwrap();

        // 1. Valid relative file inside cwd succeeds and preserves relative destination.
        let ok = scan_mcp_inputs_in_dir(&cwd, &["sub/inside.txt".to_string()])
            .expect("relative file inside cwd must succeed");
        assert_eq!(ok.inputs.len(), 1);
        assert_eq!(ok.inputs[0].path, "sub/inside.txt");

        // 2. Parent traversal (`..`) is refused.
        let err_parent = scan_mcp_inputs_in_dir(&cwd, &["../outside_secret.txt".to_string()])
            .err()
            .expect("parent traversal must be refused");
        assert!(err_parent.contains(".."), "{err_parent}");

        // 3. Absolute path outside cwd is refused.
        let err_abs = scan_mcp_inputs_in_dir(&cwd, &[secret_outside.to_string_lossy().to_string()])
            .err()
            .expect("absolute path outside cwd must be refused");
        assert!(
            err_abs.contains("outside the working directory"),
            "{err_abs}"
        );

        // 4. Symbolic link (both direct file symlink and symlink inside a directory) is refused.
        #[cfg(unix)]
        {
            let link_file = cwd.join("link_to_secret.txt");
            std::os::unix::fs::symlink(&secret_outside, &link_file).unwrap();
            let err_link = scan_mcp_inputs_in_dir(&cwd, &["link_to_secret.txt".to_string()])
                .err()
                .expect("symlink file must be refused");
            assert!(err_link.contains("symbolic link"), "{err_link}");

            let err_dir_with_link = scan_mcp_inputs_in_dir(&cwd, &[".".to_string()])
                .err()
                .expect("directory containing symlink must be refused");
            assert!(
                err_dir_with_link.contains("symbolic link"),
                "{err_dir_with_link}"
            );
        }
    }

    #[test]
    fn test_truncate_stream_for_llm_keeps_head_and_tail_within_budget() {
        let mut raw = Vec::new();
        raw.extend_from_slice(b"START_BANNER\n");
        raw.extend(std::iter::repeat_n(b'x', 20_000));
        raw.extend_from_slice(b"\nFINAL_SUMMARY_MARKER\n");

        let (formatted, truncated, total_bytes) = truncate_stream_for_llm(&raw, 4096);
        assert!(truncated);
        assert_eq!(total_bytes, raw.len());
        assert!(
            formatted.len() <= 4096,
            "formatted output ({} bytes) must fit within max_output_bytes (4096)",
            formatted.len()
        );
        assert!(formatted.starts_with("START_BANNER\n"));
        assert!(formatted.ends_with("\nFINAL_SUMMARY_MARKER\n"));
        assert!(formatted.contains("...[truncated "));
    }
}
