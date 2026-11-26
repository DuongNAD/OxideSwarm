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
//! 6. `doctor`: Run cluster diagnostics (checks C1-C8) and return a structured report.
//! 7. `workspaces`: List persistent agent workspaces and their worker affinities.
//! 8. `shard`: Run commands or `cargo nextest` build-once suites across parallel shards.
//! 9. `plan_snapshot`: Render the compact `<= 30`-line shared plan snapshot.
//!
//! # Stdout Hygiene
//!
//! Standard output (`stdout`) is strictly reserved for JSON-RPC 2.0 frames (`\n` terminated).
//! All diagnostics, tracing logs, and error messages are strictly routed to `stderr`.

use std::collections::{HashMap, VecDeque};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::JoinSet;
use uuid::Uuid;

use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::placement::{IsolationLevel, Placement, REQUIRE_PREFIX};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse, MessageTransport};
use rusty_grid_core::task::TaskId;
use rusty_grid_core::transport::GridStream;

use crate::config::ConfigFile;
use crate::connect::{connect_master, normalize_master_addr, DEFAULT_MASTER_ADDR};
use crate::secret_cmd::SecretOpts;

mod cancel;
mod doctor;
mod plan;
mod shard;
mod status;
pub(crate) mod submit;
mod submit_matrix;
mod submit_support;
mod task_result;
mod verify;
mod workers;
mod workspaces;

pub use submit::{
    prepare_mcp_output_dir_in_dir, scan_mcp_inputs_in_dir, scan_mcp_workspace_in_dir,
    stream_was_worker_truncated, truncate_stream_for_llm,
};
use submit_support::VerifyArgs;

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

    #[arg(
        long,
        help = "Maximum file count scanned when syncing a workspace via MCP submit (default 100000)"
    )]
    pub workspace_max_files: Option<usize>,

    #[arg(
        long,
        help = "Maximum total MiB scanned when syncing a workspace via MCP submit (default 2048)"
    )]
    pub workspace_max_mb: Option<u64>,
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

/// Maximum number of async task submission timestamps retained for `task_result` wait timing.
const MAX_SUBMITTED_TIMESTAMPS: usize = 4096;

pub struct McpState {
    pub master_addr: String,
    pub secret: Option<ClusterSecret>,
    pub workspace_limits: crate::blobs::WorkspaceLimits,
    resolved_master_addr: Mutex<Option<String>>,
    transport: Mutex<Option<MessageTransport<GridStream>>>,
    submitted_at: Mutex<(VecDeque<TaskId>, HashMap<TaskId, Instant>)>,
}

impl McpState {
    pub fn new(master_addr: String, secret: Option<ClusterSecret>) -> Self {
        Self {
            master_addr,
            secret,
            workspace_limits: crate::blobs::WorkspaceLimits::default(),
            resolved_master_addr: Mutex::new(None),
            transport: Mutex::new(None),
            submitted_at: Mutex::new((VecDeque::new(), HashMap::new())),
        }
    }

    pub(crate) async fn record_submitted(&self, task_id: TaskId, at: Instant) {
        let mut guard = self.submitted_at.lock().await;
        let (order, map) = &mut *guard;
        if let std::collections::hash_map::Entry::Occupied(mut e) = map.entry(task_id) {
            e.insert(at);
            return;
        }
        while order.len() >= MAX_SUBMITTED_TIMESTAMPS {
            if let Some(oldest) = order.pop_front() {
                map.remove(&oldest);
            }
        }
        order.push_back(task_id);
        map.insert(task_id, at);
    }

    pub(crate) async fn submitted_at(&self, task_id: TaskId) -> Option<Instant> {
        self.submitted_at.lock().await.1.get(&task_id).copied()
    }

    pub fn with_workspace_limits(mut self, limits: crate::blobs::WorkspaceLimits) -> Self {
        self.workspace_limits = limits;
        self
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
                        } else {
                            drop(guard);
                            transport.close().await;
                        }
                        return Ok((resp, false));
                    }
                    Err(rusty_grid_core::protocol::ProtocolError::Io(ref io_err))
                        if !is_idempotent && io_err.kind() == std::io::ErrorKind::TimedOut =>
                    {
                        transport.close().await;
                        return Err(format!(
                            "Timed out waiting for response from Master at {}",
                            self.master_addr
                        ));
                    }
                    Ok(None) if !is_idempotent => {
                        transport.close().await;
                        return Err("Master closed connection unexpectedly".to_string());
                    }
                    Err(e) if !is_idempotent => {
                        transport.close().await;
                        return Err(format!("Failed to receive response from Master: {e}"));
                    }
                    Ok(None) | Err(_) => {
                        // Pooled idle connection was closed, reset, or timed out before replying
                        // to an idempotent request; fall through to open a fresh connection.
                        transport.close().await;
                        retried_after_pooled_write = true;
                    }
                }
            } else {
                transport.close().await;
            }
        }

        // Fresh connection (discovery runs outside `timeout`, connect/handshake bounded by `timeout`)
        let mut transport = self.connect_dedicated_with_timeout(timeout).await?;

        if let Err(e) = transport.send_msg_with_idle_timeout(message, timeout).await {
            transport.close().await;
            return Err(
                if matches!(e, rusty_grid_core::protocol::ProtocolError::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::TimedOut)
                {
                    format!(
                        "Timed out sending request to Master at {}",
                        self.master_addr
                    )
                } else {
                    format!("Failed to send request to Master: {e}")
                },
            );
        }

        let response = match transport
            .recv_msg_with_idle_timeout::<ClientResponse>(timeout)
            .await
        {
            Ok(Some(resp)) => resp,
            Ok(None) => {
                transport.close().await;
                return Err("Master closed connection unexpectedly".to_string());
            }
            Err(e) => {
                transport.close().await;
                return Err(
                    if matches!(e, rusty_grid_core::protocol::ProtocolError::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::TimedOut)
                    {
                        format!(
                            "Timed out waiting for response from Master at {}",
                            self.master_addr
                        )
                    } else {
                        format!("Failed to receive response from Master: {e}")
                    },
                );
            }
        };

        let mut guard = self.transport.lock().await;
        if guard.is_none() {
            *guard = Some(transport);
        } else {
            drop(guard);
            transport.close().await;
        }
        Ok((response, retried_after_pooled_write))
    }

    /// Gracefully closes any pooled client connection and drains pending P2P endpoint closes.
    pub async fn close(&self) {
        let pooled = self.transport.lock().await.take();
        if let Some(mut transport) = pooled {
            transport.close().await;
        }
        rusty_grid_core::transport::drain_pending_p2p_client_closes().await;
    }

    /// Obtains a dedicated one-off connection (e.g. for long-running streaming/waiting operations).
    pub async fn connect_dedicated(&self) -> Result<MessageTransport<GridStream>, String> {
        self.connect_dedicated_with_timeout(DEFAULT_MASTER_RPC_TIMEOUT)
            .await
    }

    /// Obtains a dedicated one-off connection with TCP connect + handshake bounded by `timeout`,
    /// resolving `--master auto` outside `timeout`.
    pub async fn connect_dedicated_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<MessageTransport<GridStream>, String> {
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
    ) -> Result<MessageTransport<GridStream>, String> {
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
                },
                "required": []
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
                },
                "required": []
            }),
        },
        ToolDefinition {
            name: "submit",
            description: "Submit a compute workload (command, shell script, GPU compute, or Rust compilation) to the grid cluster with placement constraints, environment variables, staged files, and artifact retrieval.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["command", "shell", "gpu", "compile"],
                        "description": "Task type (default: command). Requires 'command' when 'command' (or omitted); requires 'script' or 'command' when 'shell'; requires 'source_files' or 'files' when 'compile'"
                    },
                    "command": {
                        "type": "string",
                        "description": "Command executable to run (required when type is 'command' or omitted; also accepted as shell script string when type is 'shell' and 'script' is omitted, or crate name when type is 'compile')"
                    },
                    "args": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Arguments for command execution (or compiler_flags when type is 'compile')"
                    },
                    "source_files": {
                        "type": "object",
                        "additionalProperties": { "type": "string" },
                        "description": "Source files map (relative path -> UTF-8 source text) for 'compile' tasks"
                    },
                    "script": {
                        "type": "string",
                        "description": "Explicit script content for shell tasks (when type is 'shell', either 'script' or 'command' is required)"
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
                    "vram_mb": {
                        "type": "integer",
                        "description": "Minimum GPU VRAM in MB required (default: 0; > 0 implies gpu=true)"
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
                    "isolation": {
                        "type": "string",
                        "enum": ["fs", "fs+net"],
                        "description": "Run only on a worker confined at least at this OS-level isolation (worker --isolation): fs or fs+net"
                    },
                    "require_tags": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Required worker user tags"
                    },
                    "tools": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Required worker host tools (e.g. [\"cargo\", \"rustc\", \"git\"]), matched against worker tool:<name> tags"
                    },
                    "timeout_secs": {
                        "type": "integer",
                        "description": "Task timeout in seconds (default: 60, or 1800 with workspace)"
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
                    "workspace": {
                        "type": "object",
                        "description": "Run the task inside a persistent named workspace on the worker (mutually exclusive with 'files')",
                        "properties": {
                            "name": {
                                "type": "string",
                                "description": "Workspace name ([A-Za-z0-9][A-Za-z0-9._-]{0,63})"
                            },
                            "sync_dir": {
                                "type": "string",
                                "description": "Directory inside the MCP server's working directory to sync into the workspace before execution (default: '.', or null for no sync)"
                            },
                            "cwd": {
                                "type": "string",
                                "description": "Relative working directory inside the workspace for command tasks"
                            }
                        },
                        "required": ["name"]
                    },
                    "outputs": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Declared output file paths or glob patterns to retrieve upon completion"
                    },
                    "verify": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Implies wait. After the task completes, run it again on this many other matching workers with the same operating system and CPU architecture as the first run (see 'verify_any_system') and compare the declared outputs by BLAKE3 (reply field 'verify': status verified|mismatch, workers with their os and arch, differing files; a mismatch is an error result). Needs 'outputs' or 'verify_file'; not with 'worker'. Only for deterministic tasks"
                    },
                    "verify_file": {
                        "type": "string",
                        "description": "With 'verify': compare only this sandbox file (e.g. checksums.sha256) byte for byte instead of every declared output"
                    },
                    "verify_any_system": {
                        "type": "boolean",
                        "description": "With 'verify': let the replicas run on matching workers with another operating system or CPU architecture than the first run (default false: the same deterministic program can give different results on two systems)"
                    },
                    "repeatable": {
                        "type": "boolean",
                        "description": "Mark this task as safe to re-run speculatively on another worker if a running attempt becomes a straggler (attaches opt:repeatable)"
                    },
                    "matrix": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Implies wait. Fan out the task over the Cartesian product of placement or env:<VAR> axes (default limit 64 cells, max 256), e.g. [\"os=macos,linux\", \"tag=fast,gpu\"]. Supported keys: os, arch, worker, iso, tool, tag, and env:<VAR> for command/shell tasks. Unplaced cells are reported immediately without hanging."
                    },
                    "max_matrix_cells": {
                        "type": "integer",
                        "description": "With 'matrix': maximum expanded cells allowed (default: 64, maximum: 256)"
                    },
                    "output_dir": {
                        "type": "string",
                        "description": "Local directory inside the working directory to download declared output files into when wait is true (symbolic links and paths outside the working directory are refused)"
                    },
                    "max_output_bytes": {
                        "type": "integer",
                        "description": "Maximum escaped bytes of stdout and stderr combined to return inline (head + tail, default: 16384)"
                    }
                },
                "required": []
            }),
        },
        ToolDefinition {
            name: "shard",
            description: "Run a command, shell script, or Cargo nextest build-once test suite across N parallel shards with 1-based OX_SHARD_INDEX and OX_SHARD_TOTAL environment injection, slot-aware worker spreading, fail-fast cancellation, and optional JUnit XML merging.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "count": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 256,
                        "description": "Number of parallel shards to run (1..=256)"
                    },
                    "type": {
                        "type": "string",
                        "enum": ["command", "shell"],
                        "description": "Task execution type per shard (default: command)"
                    },
                    "command": {
                        "type": "string",
                        "description": "Executable name (or shell command string when type is 'shell')"
                    },
                    "args": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Command arguments (or extra cargo nextest run arguments in build_once mode)"
                    },
                    "script": {
                        "type": "string",
                        "description": "Shell script body when type is 'shell'"
                    },
                    "interpreter": {
                        "type": "string",
                        "description": "Shell interpreter override (default: sh or cmd.exe)"
                    },
                    "stdin": {
                        "type": "string",
                        "description": "Standard input string passed to command shards"
                    },
                    "env": {
                        "type": "object",
                        "additionalProperties": { "type": "string" },
                        "description": "Environment variables for each shard (OX_SHARD_INDEX and OX_SHARD_TOTAL are always injected)"
                    },
                    "build_once": {
                        "type": "boolean",
                        "description": "Build once per (os, arch) via cargo nextest archive and pass the archive via Task::inputs"
                    },
                    "build_source": {
                        "type": "string",
                        "description": "Relative Rust project directory inside the working directory for build_once (default: '.')"
                    },
                    "fail_fast": {
                        "type": "boolean",
                        "description": "Cancel remaining in-flight shards via CancelTask on the first shard failure"
                    },
                    "junit_out": {
                        "type": "string",
                        "description": "Relative file path inside the working directory to write merged JUnit XML"
                    },
                    "junit_glob": {
                        "type": "string",
                        "description": "Sandbox-relative glob selecting per-shard JUnit XML files (default: '**/junit*.xml')"
                    },
                    "cores": { "type": "integer", "description": "Minimum CPU cores required per shard" },
                    "ram_mb": { "type": "integer", "description": "Minimum RAM in MB required per shard" },
                    "vram_mb": { "type": "integer", "description": "Minimum GPU VRAM in MB required per shard" },
                    "gpu": { "type": "boolean", "description": "Require GPU worker node" },
                    "os": { "type": "string", "description": "Required worker OS (macos, windows, linux, android)" },
                    "arch": { "type": "string", "description": "Required worker CPU arch (x86_64, aarch64)" },
                    "worker": { "type": "string", "description": "Target worker name or UUID" },
                    "isolation": { "type": "string", "description": "Minimum worker isolation level (fs, fs+net)" },
                    "require_tags": { "type": "array", "items": { "type": "string" }, "description": "Required worker user tags" },
                    "tools": { "type": "array", "items": { "type": "string" }, "description": "Required host tool tags" },
                    "timeout_secs": { "type": "integer", "description": "Per-shard execution timeout in seconds (default: 60, or 1800 with build_once)" },
                    "wait_secs": { "type": "integer", "description": "Alias for timeout_secs" },
                    "max_retries": { "type": "integer", "description": "Maximum retries per shard" },
                    "repeatable": { "type": "boolean", "description": "Attach opt:repeatable for speculative straggler re-run" },
                    "files": { "type": "array", "items": { "type": "string" }, "description": "Local files/directories inside working directory to stage as Task::inputs" },
                    "workspace": {
                        "type": "object",
                        "description": "Single-worker persistent workspace (mutually exclusive with files and build_once)",
                        "properties": {
                            "name": { "type": "string" },
                            "sync_dir": { "type": "string" },
                            "cwd": { "type": "string" }
                        },
                        "required": ["name"]
                    },
                    "outputs": { "type": "array", "items": { "type": "string" }, "description": "Sandbox output file paths or globs to retrieve" },
                    "output_dir": { "type": "string", "description": "Local directory inside working directory to download per-shard outputs into" },
                    "max_output_bytes": { "type": "integer", "description": "Maximum escaped bytes of stdout and stderr combined across the response (default: 16384)" }
                },
                "required": ["count"]
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
                        "description": "Local directory inside the working directory to download task output files into (symbolic links and paths outside the working directory are refused)"
                    },
                    "max_output_bytes": {
                        "type": "integer",
                        "description": "Maximum escaped bytes of stdout and stderr combined to return inline (head + tail, default: 16384)"
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
            description: "Run diagnostic health checks (C1-C8) on the cluster and return a structured report.",
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
                    },
                    "skip_throughput": {
                        "type": "boolean",
                        "description": "Skip the blob throughput probe C8 (default: false)"
                    }
                },
                "required": []
            }),
        },
        ToolDefinition {
            name: "workspaces",
            description: "List the master's workspace affinity map (does not list workspaces stored on workers).",
            input_schema: json!({
                "type": "object",
                "properties": {},
                "required": []
            }),
        },
        ToolDefinition {
            name: "plan_snapshot",
            description: "Render a compact snapshot (at most 30 lines) of .oxideswarm/plan.toml combined with git lease refs (refs/oxideswarm/lease/<task>), contract hashes, and recent base commits.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "plan_path": {
                        "type": "string",
                        "description": "Optional path inside the server working directory to the plan TOML file (default: .oxideswarm/plan.toml)"
                    },
                    "repo_dir": {
                        "type": "string",
                        "description": "Optional repository subdirectory inside the server working directory (default: current directory)"
                    },
                    "base": {
                        "type": "string",
                        "description": "Optional base git branch or ref overriding [project].base"
                    }
                },
                "required": []
            }),
        },
        ToolDefinition {
            name: "verify",
            description: "Sync the current worktree into worker workspaces, run the task's configured verify profiles across the grid, and return compact (<= 4 KiB) failure JSON.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "task": {
                        "type": "string",
                        "description": "Task id declared in .oxideswarm/plan.toml"
                    },
                    "profiles": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional subset of task verify profile names to run"
                    },
                    "plan_path": {
                        "type": "string",
                        "description": "Optional path inside the repository to .oxideswarm/plan.toml"
                    },
                    "repo_dir": {
                        "type": "string",
                        "description": "Optional repository subdirectory inside the working directory"
                    },
                    "base": {
                        "type": "string",
                        "description": "Optional git base ref overriding [project].base for owned_paths diff check"
                    },
                    "timeout_secs": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Execution timeout per verify batch in seconds (default 1800)"
                    }
                },
                "required": ["task"]
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

fn check_opt_bool(obj: &serde_json::Map<String, Value>, key: &str) -> Result<(), String> {
    if let Some(v) = obj.get(key) {
        if !v.is_null() && !v.is_boolean() {
            return Err(format!("Property '{key}' must be a boolean"));
        }
    }
    Ok(())
}

fn check_opt_str<'a>(
    obj: &'a serde_json::Map<String, Value>,
    key: &str,
) -> Result<Option<&'a str>, String> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v
            .as_str()
            .map(Some)
            .ok_or_else(|| format!("Property '{key}' must be a string")),
    }
}

fn check_opt_u64(obj: &serde_json::Map<String, Value>, key: &str) -> Result<Option<u64>, String> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v
            .as_u64()
            .map(Some)
            .ok_or_else(|| format!("Property '{key}' must be a non-negative integer")),
    }
}

fn check_opt_str_array(
    obj: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<Option<Vec<String>>, String> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => {
            let arr = v
                .as_array()
                .ok_or_else(|| format!("Property '{key}' must be an array of strings"))?;
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                let s = item
                    .as_str()
                    .ok_or_else(|| format!("Property '{key}' must be an array of strings"))?;
                out.push(s.to_string());
            }
            Ok(Some(out))
        }
    }
}

/// Validates tool name and arguments against the MCP tool input schemas before execution.
pub fn validate_tool_arguments(name: &str, args: &Value) -> Result<(), String> {
    let obj = args
        .as_object()
        .ok_or_else(|| "Tool 'arguments' must be a JSON object".to_string())?;

    match name {
        "status" => {
            if let Some(tid) = check_opt_str(obj, "task_id")? {
                Uuid::parse_str(tid).map_err(|e| format!("Invalid task_id UUID '{tid}': {e}"))?;
            }
            check_opt_bool(obj, "workers")?;
            Ok(())
        }
        "workers" => {
            check_opt_bool(obj, "connected_only")?;
            check_opt_str(obj, "os")?;
            check_opt_str(obj, "arch")?;
            check_opt_bool(obj, "gpu_only")?;
            Ok(())
        }
        "submit" => {
            let raw_type = check_opt_str(obj, "type")?.unwrap_or("command");
            let task_type = raw_type.to_lowercase();
            if !matches!(task_type.as_str(), "command" | "shell" | "gpu" | "compile") {
                return Err(format!(
                    "Invalid task 'type': '{raw_type}' (expected 'command', 'shell', 'gpu', or 'compile')"
                ));
            }

            let cmd_opt = check_opt_str(obj, "command")?;
            let script_opt = check_opt_str(obj, "script")?;
            let files_opt = check_opt_str_array(obj, "files")?;
            let mut has_source_files = false;
            if let Some(sf_val) = obj.get("source_files") {
                if !sf_val.is_null() {
                    let sf_obj = sf_val.as_object().ok_or_else(|| {
                        "Property 'source_files' must be an object mapping relative paths to strings"
                            .to_string()
                    })?;
                    for (k, v) in sf_obj {
                        if k.trim().is_empty() {
                            return Err(
                                "Source file path in 'source_files' must not be empty".to_string()
                            );
                        }
                        if !v.is_string() {
                            return Err(format!("Source file '{k}' content must be a string"));
                        }
                    }
                    has_source_files = !sf_obj.is_empty();
                }
            }
            match task_type.as_str() {
                "command" => {
                    if cmd_opt.is_none_or(|c| c.trim().is_empty()) {
                        return Err(
                            "Command task requires a non-empty 'command' property".to_string()
                        );
                    }
                }
                "shell" => {
                    let has_script = script_opt.is_some_and(|s| !s.trim().is_empty());
                    let has_cmd = cmd_opt.is_some_and(|c| !c.trim().is_empty());
                    if !has_script && !has_cmd {
                        return Err(
                            "Shell task requires a non-empty 'script' or 'command' property"
                                .to_string(),
                        );
                    }
                }
                "gpu" => {}
                "compile" => {
                    let has_files = files_opt.as_ref().is_some_and(|f| !f.is_empty());
                    if !has_source_files && !has_files {
                        return Err(
                            "Compile task requires a non-empty 'source_files' object or 'files' array"
                                .to_string(),
                        );
                    }
                }
                _ => unreachable!(),
            }

            check_opt_str_array(obj, "args")?;
            check_opt_str(obj, "interpreter")?;
            check_opt_str(obj, "stdin")?;
            if let Some(env_val) = obj.get("env") {
                if !env_val.is_null() {
                    let env_obj = env_val.as_object().ok_or_else(|| {
                        "Property 'env' must be an object mapping strings to strings".to_string()
                    })?;
                    for (k, v) in env_obj {
                        if !v.is_string() {
                            return Err(format!("Environment variable '{k}' must be a string"));
                        }
                    }
                }
            }
            check_opt_u64(obj, "cores")?;
            check_opt_u64(obj, "ram_mb")?;
            check_opt_u64(obj, "vram_mb")?;
            check_opt_bool(obj, "gpu")?;
            let os_opt = check_opt_str(obj, "os")?;
            let arch_opt = check_opt_str(obj, "arch")?;
            let worker_opt = check_opt_str(obj, "worker")?;
            let req_tags = check_opt_str_array(obj, "require_tags")?;
            let tools_opt = check_opt_str_array(obj, "tools")?;
            let isolation_opt = check_opt_str(obj, "isolation")?;

            let mut placement_tags = Vec::new();
            if let Some(level) = isolation_opt {
                let level = IsolationLevel::parse_required(level)
                    .map_err(|e| format!("Invalid placement constraints: {e}"))?;
                placement_tags.push(format!("{REQUIRE_PREFIX}iso={level}"));
            }
            if let Some(os) = os_opt {
                placement_tags.push(format!("{REQUIRE_PREFIX}os={os}"));
            }
            if let Some(arch) = arch_opt {
                placement_tags.push(format!("{REQUIRE_PREFIX}arch={arch}"));
            }
            if let Some(worker) = worker_opt {
                placement_tags.push(format!("{REQUIRE_PREFIX}worker={worker}"));
            }
            if let Some(tags) = req_tags {
                for t in tags {
                    placement_tags.push(format!("{REQUIRE_PREFIX}tag={t}"));
                }
            }
            if let Some(tools) = tools_opt {
                for t in tools {
                    placement_tags.push(format!("{REQUIRE_PREFIX}tool={t}"));
                }
            }
            let base_placement = Placement::from_task_tags(&placement_tags)
                .map_err(|e| format!("Invalid placement constraints: {e}"))?;

            check_opt_u64(obj, "timeout_secs")?;
            check_opt_u64(obj, "wait_secs")?;
            check_opt_u64(obj, "max_retries")?;
            check_opt_bool(obj, "wait")?;
            check_opt_bool(obj, "repeatable")?;
            check_opt_u64(obj, "matrix_dim")?;
            check_opt_u64(obj, "work_group_size")?;
            let max_matrix_cells = check_opt_u64(obj, "max_matrix_cells")?;
            let matrix_val = obj.get("matrix").filter(|v| !v.is_null());
            if max_matrix_cells.is_some() && matrix_val.is_none() {
                return Err("Property 'max_matrix_cells' requires 'matrix'".to_string());
            }
            if let Some(mval) = matrix_val {
                if obj.get("verify").is_some_and(|v| !v.is_null()) {
                    return Err(
                        "Property 'matrix' cannot be used together with 'verify'".to_string()
                    );
                }
                if obj.get("wait").and_then(Value::as_bool) == Some(false) {
                    return Err(
                        "Property 'matrix' requires 'wait' to be true (or omitted)".to_string()
                    );
                }
                let max_cells_u64 =
                    max_matrix_cells.unwrap_or(crate::matrix::DEFAULT_MAX_MATRIX_CELLS as u64);
                let max_cells = usize::try_from(max_cells_u64).map_err(|_| {
                    format!(
                        "Property 'max_matrix_cells' exceeds limit of {}",
                        crate::matrix::HARD_MAX_MATRIX_CELLS
                    )
                })?;
                let spec = crate::matrix::MatrixSpec::parse_mcp_value(mval, max_cells)?;
                let allow_env = matches!(task_type.as_str(), "command" | "shell");
                crate::matrix::expand_cells(&spec, &base_placement, allow_env)?;
            }
            let files_opt = check_opt_str_array(obj, "files")?;
            if let Some(ws_val) = obj.get("workspace") {
                if !ws_val.is_null() {
                    let ws_obj = ws_val
                        .as_object()
                        .ok_or_else(|| "Property 'workspace' must be an object".to_string())?;
                    if files_opt.is_some() {
                        return Err(
                            "Property 'workspace' cannot be used together with 'files'".to_string()
                        );
                    }
                    if task_type == "gpu" {
                        return Err(
                            "Property 'workspace' is only supported for 'command' or 'shell' tasks"
                                .to_string(),
                        );
                    }
                    for key in ws_obj.keys() {
                        if !matches!(key.as_str(), "name" | "sync_dir" | "cwd") {
                            return Err(format!("Unknown property 'workspace.{key}'"));
                        }
                    }
                    let ws_name = check_opt_str(ws_obj, "name")?
                        .ok_or_else(|| "Missing required property 'workspace.name'".to_string())?;
                    rusty_grid_core::task::validate_workspace_name(ws_name)?;
                    if let Some(sync_dir) = check_opt_str(ws_obj, "sync_dir")? {
                        if sync_dir.trim().is_empty() {
                            return Err("Property 'workspace.sync_dir' cannot be empty".to_string());
                        }
                        if std::path::Path::new(sync_dir)
                            .components()
                            .any(|c| matches!(c, std::path::Component::ParentDir))
                        {
                            return Err(format!(
                                "Property 'workspace.sync_dir' ('{sync_dir}') must not contain '..'"
                            ));
                        }
                    }
                    if let Some(cwd_rel) = check_opt_str(ws_obj, "cwd")? {
                        if task_type != "command" {
                            return Err(
                                "Property 'workspace.cwd' requires task type 'command'".to_string()
                            );
                        }
                        crate::cli_app::normalize_workspace_cwd(cwd_rel)?;
                    }
                }
            }
            if let Some(outputs) = check_opt_str_array(obj, "outputs")? {
                if !outputs.is_empty() {
                    rusty_grid_core::blob::validate_output_patterns(&outputs)
                        .map_err(|e| format!("Expected outputs error: {e}"))?;
                }
            }
            check_opt_str(obj, "output_dir")?;
            check_opt_u64(obj, "max_output_bytes")?;
            VerifyArgs::check(obj)?;
            Ok(())
        }
        "shard" => {
            let count_u64 = check_opt_u64(obj, "count")?
                .ok_or_else(|| "Missing required property 'count'".to_string())?;
            let count = usize::try_from(count_u64).map_err(|_| {
                format!(
                    "Property 'count' must be between 1 and {}",
                    crate::shard::HARD_MAX_SHARDS
                )
            })?;
            crate::shard::validate_shard_count(count)?;

            let build_once = obj
                .get("build_once")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            check_opt_bool(obj, "build_once")?;
            check_opt_bool(obj, "fail_fast")?;
            let build_source = check_opt_str(obj, "build_source")?;
            let raw_type = check_opt_str(obj, "type")?.unwrap_or("command");
            let task_type = raw_type.to_ascii_lowercase();
            if !matches!(task_type.as_str(), "command" | "shell") {
                return Err(format!(
                    "Invalid shard 'type': '{raw_type}' (expected 'command' or 'shell')"
                ));
            }
            let cmd_opt = check_opt_str(obj, "command")?;
            let script_opt = check_opt_str(obj, "script")?;
            let interp_opt = check_opt_str(obj, "interpreter")?;
            let stdin_opt = check_opt_str(obj, "stdin")?;

            if build_once {
                if task_type != "command" {
                    return Err(
                        "Property 'build_once' cannot be combined with type 'shell'".to_string()
                    );
                }
                if cmd_opt.is_some()
                    || script_opt.is_some()
                    || interp_opt.is_some()
                    || stdin_opt.is_some()
                {
                    return Err(
                        "Property 'build_once' cannot be combined with 'command', 'script', 'interpreter', or 'stdin'"
                            .to_string(),
                    );
                }
                if let Some(src) = build_source {
                    if src.trim().is_empty()
                        || std::path::Path::new(src)
                            .components()
                            .any(|c| matches!(c, std::path::Component::ParentDir))
                    {
                        return Err(format!(
                            "Property 'build_source' ('{src}') must be non-empty and must not contain '..'"
                        ));
                    }
                }
            } else {
                if build_source.is_some() {
                    return Err("Property 'build_source' requires 'build_once: true'".to_string());
                }
                match task_type.as_str() {
                    "command" => {
                        if cmd_opt.is_none_or(|c| c.trim().is_empty()) {
                            return Err(
                                "Command shard requires a non-empty 'command' property".to_string()
                            );
                        }
                    }
                    "shell" => {
                        let has_script = script_opt.is_some_and(|s| !s.trim().is_empty());
                        let has_cmd = cmd_opt.is_some_and(|c| !c.trim().is_empty());
                        if !has_script && !has_cmd {
                            return Err(
                                "Shell shard requires a non-empty 'script' or 'command' property"
                                    .to_string(),
                            );
                        }
                    }
                    _ => unreachable!(),
                }
            }

            check_opt_str_array(obj, "args")?;
            if let Some(env_val) = obj.get("env") {
                if !env_val.is_null() {
                    let env_obj = env_val.as_object().ok_or_else(|| {
                        "Property 'env' must be an object mapping strings to strings".to_string()
                    })?;
                    for (k, v) in env_obj {
                        if !v.is_string() {
                            return Err(format!("Environment variable '{k}' must be a string"));
                        }
                    }
                }
            }
            check_opt_u64(obj, "cores")?;
            check_opt_u64(obj, "ram_mb")?;
            check_opt_u64(obj, "vram_mb")?;
            check_opt_bool(obj, "gpu")?;

            let os_opt = check_opt_str(obj, "os")?;
            let arch_opt = check_opt_str(obj, "arch")?;
            let worker_opt = check_opt_str(obj, "worker")?;
            let req_tags = check_opt_str_array(obj, "require_tags")?;
            let tools_opt = check_opt_str_array(obj, "tools")?;
            let isolation_opt = check_opt_str(obj, "isolation")?;
            let mut placement_tags = Vec::new();
            if let Some(level) = isolation_opt {
                let level = IsolationLevel::parse_required(level)
                    .map_err(|e| format!("Invalid placement constraints: {e}"))?;
                placement_tags.push(format!("{REQUIRE_PREFIX}iso={level}"));
            }
            if let Some(os) = os_opt {
                placement_tags.push(format!("{REQUIRE_PREFIX}os={os}"));
            }
            if let Some(arch) = arch_opt {
                placement_tags.push(format!("{REQUIRE_PREFIX}arch={arch}"));
            }
            if let Some(worker) = worker_opt {
                placement_tags.push(format!("{REQUIRE_PREFIX}worker={worker}"));
            }
            if let Some(tags) = req_tags {
                for t in tags {
                    placement_tags.push(format!("{REQUIRE_PREFIX}tag={t}"));
                }
            }
            if let Some(tools) = tools_opt {
                for t in tools {
                    placement_tags.push(format!("{REQUIRE_PREFIX}tool={t}"));
                }
            }
            Placement::from_task_tags(&placement_tags)
                .map_err(|e| format!("Invalid placement constraints: {e}"))?;

            check_opt_u64(obj, "timeout_secs")?;
            check_opt_u64(obj, "wait_secs")?;
            check_opt_u64(obj, "max_retries")?;
            check_opt_bool(obj, "repeatable")?;

            let files_opt = check_opt_str_array(obj, "files")?;
            if let Some(ws_val) = obj.get("workspace") {
                if !ws_val.is_null() {
                    if build_once {
                        return Err(
                            "Property 'workspace' cannot be used together with 'build_once'"
                                .to_string(),
                        );
                    }
                    if files_opt.is_some() {
                        return Err(
                            "Property 'workspace' cannot be used together with 'files'".to_string()
                        );
                    }
                    let ws_obj = ws_val
                        .as_object()
                        .ok_or_else(|| "Property 'workspace' must be an object".to_string())?;
                    for key in ws_obj.keys() {
                        if !matches!(key.as_str(), "name" | "sync_dir" | "cwd") {
                            return Err(format!("Unknown property 'workspace.{key}'"));
                        }
                    }
                    let ws_name = check_opt_str(ws_obj, "name")?
                        .ok_or_else(|| "Missing required property 'workspace.name'".to_string())?;
                    rusty_grid_core::task::validate_workspace_name(ws_name)?;
                    if let Some(sync_dir) = check_opt_str(ws_obj, "sync_dir")? {
                        if sync_dir.trim().is_empty()
                            || std::path::Path::new(sync_dir)
                                .components()
                                .any(|c| matches!(c, std::path::Component::ParentDir))
                        {
                            return Err(format!(
                                "Property 'workspace.sync_dir' ('{sync_dir}') must be non-empty and must not contain '..'"
                            ));
                        }
                    }
                    if let Some(cwd_rel) = check_opt_str(ws_obj, "cwd")? {
                        if task_type != "command" {
                            return Err(
                                "Property 'workspace.cwd' requires task type 'command'".to_string()
                            );
                        }
                        crate::cli_app::normalize_workspace_cwd(cwd_rel)?;
                    }
                }
            }

            if let Some(outputs) = check_opt_str_array(obj, "outputs")? {
                if !outputs.is_empty() {
                    rusty_grid_core::blob::validate_output_patterns(&outputs)
                        .map_err(|e| format!("Expected outputs error: {e}"))?;
                }
            }
            let junit_out = check_opt_str(obj, "junit_out")?;
            if let Some(jout) = junit_out {
                if jout.trim().is_empty()
                    || std::path::Path::new(jout)
                        .components()
                        .any(|c| matches!(c, std::path::Component::ParentDir))
                {
                    return Err(format!(
                        "Property 'junit_out' ('{jout}') must be non-empty and must not contain '..'"
                    ));
                }
            }
            if let Some(jglob) = check_opt_str(obj, "junit_glob")? {
                if junit_out.is_none() {
                    return Err("Property 'junit_glob' requires 'junit_out'".to_string());
                }
                rusty_grid_core::blob::OutputPattern::parse(jglob)
                    .map_err(|e| format!("Invalid 'junit_glob': {e}"))?;
            }
            check_opt_str(obj, "output_dir")?;
            check_opt_u64(obj, "max_output_bytes")?;
            Ok(())
        }
        "task_result" => {
            let tid = check_opt_str(obj, "task_id")?
                .ok_or_else(|| "Missing required property 'task_id'".to_string())?;
            Uuid::parse_str(tid).map_err(|e| format!("Invalid task_id UUID '{tid}': {e}"))?;
            check_opt_bool(obj, "wait")?;
            check_opt_u64(obj, "timeout_secs")?;
            check_opt_u64(obj, "wait_secs")?;
            check_opt_str(obj, "output_dir")?;
            check_opt_u64(obj, "max_output_bytes")?;
            Ok(())
        }
        "cancel" => {
            let tid = check_opt_str(obj, "task_id")?
                .ok_or_else(|| "Missing required property 'task_id'".to_string())?;
            Uuid::parse_str(tid).map_err(|e| format!("Invalid task_id UUID '{tid}': {e}"))?;
            Ok(())
        }
        "doctor" => {
            check_opt_u64(obj, "expect_workers")?;
            check_opt_u64(obj, "timeout_secs")?;
            check_opt_u64(obj, "gpu_dim")?;
            check_opt_u64(obj, "spread_tasks")?;
            check_opt_bool(obj, "skip_gpu")?;
            check_opt_bool(obj, "skip_spread")?;
            check_opt_bool(obj, "skip_throughput")?;
            Ok(())
        }
        "workspaces" => Ok(()),
        "plan_snapshot" => {
            for field in ["plan_path", "repo_dir"] {
                if let Some(p) = check_opt_str(obj, field)? {
                    if p.trim().is_empty()
                        || std::path::Path::new(p)
                            .components()
                            .any(|c| matches!(c, std::path::Component::ParentDir))
                    {
                        return Err(format!(
                            "Property '{field}' ('{p}') must be non-empty and must not contain '..'"
                        ));
                    }
                }
            }
            if let Some(base) = check_opt_str(obj, "base")? {
                if base.trim().is_empty()
                    || base.chars().any(|c| c.is_whitespace() || c.is_control())
                {
                    return Err(format!(
                        "Property 'base' ('{base}') must be a non-empty git ref without whitespace"
                    ));
                }
            }
            Ok(())
        }
        "verify" => {
            let task = check_opt_str(obj, "task")?
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    "Missing or invalid required property 'task' (string)".to_string()
                })?;
            if task.chars().any(|c| c.is_whitespace() || c.is_control()) {
                return Err(format!(
                    "Property 'task' ('{task}') must not contain whitespace"
                ));
            }
            for field in ["plan_path", "repo_dir"] {
                if let Some(p) = check_opt_str(obj, field)? {
                    if p.trim().is_empty()
                        || std::path::Path::new(p)
                            .components()
                            .any(|c| matches!(c, std::path::Component::ParentDir))
                    {
                        return Err(format!(
                            "Property '{field}' ('{p}') must be non-empty and must not contain '..'"
                        ));
                    }
                }
            }
            if let Some(base) = check_opt_str(obj, "base")? {
                if base.trim().is_empty()
                    || base.chars().any(|c| c.is_whitespace() || c.is_control())
                {
                    return Err(format!(
                        "Property 'base' ('{base}') must be a non-empty git ref without whitespace"
                    ));
                }
            }
            let _ = check_opt_str_array(obj, "profiles")?;
            if let Some(t) = check_opt_u64(obj, "timeout_secs")? {
                if t == 0 {
                    return Err("Property 'timeout_secs' must be at least 1".to_string());
                }
            }
            Ok(())
        }
        other => Err(format!("Unknown tool: '{other}'")),
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
    if let Err(err) = validate_tool_arguments(name, args) {
        return ToolCallResult::error(err);
    }
    match name {
        "status" => Box::pin(status::tool_status(state, args)).await,
        "workers" => Box::pin(workers::tool_workers(state, args)).await,
        "submit" => Box::pin(submit::tool_submit(state, args, ctx)).await,
        "shard" => Box::pin(shard::tool_shard(state, args, ctx)).await,
        "task_result" => Box::pin(task_result::tool_task_result(state, args, ctx)).await,
        "cancel" => Box::pin(cancel::tool_cancel(state, args)).await,
        "doctor" => Box::pin(doctor::tool_doctor(state, args)).await,
        "workspaces" => Box::pin(workspaces::tool_workspaces(state)).await,
        "plan_snapshot" => Box::pin(plan::tool_plan_snapshot(args)).await,
        "verify" => Box::pin(verify::tool_verify(state, args, ctx)).await,
        other => ToolCallResult::error(format!("Unknown tool: '{other}'")),
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

/// An error response of the line reader, boxed: `JsonRpcResponse` is too large for an `Err`
/// variant (`clippy::result_large_err`).
fn error_reply(id: Option<Value>, code: i64, message: impl Into<String>) -> Box<JsonRpcResponse> {
    Box::new(JsonRpcResponse::error(id, code, message, None))
}

/// Reads one line of standard input as `run_stdio_server` does before it dispatches (public for
/// the `mcp_request_line` fuzz target, LV-18).
///
/// - `Ok(Some(request))`: a request, or a notification when `request.id` is `None` (the line has
///   no `id` member; `"id": null` gives `Some(Value::Null)`, a request answered with `"id":
///   null`). `jsonrpc` is `"2.0"`, `method` is not empty, and `params` is absent, null, an object
///   or an array.
/// - `Ok(None)`: no reply: a blank line, or a notification with a wrong `jsonrpc` or `params`.
/// - `Err(response)`: the error response the server writes.
#[doc(hidden)]
pub fn parse_request_line(line: &str) -> Result<Option<JsonRpcRequest>, Box<JsonRpcResponse>> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let raw_val: Value = match serde_json::from_str(trimmed) {
        Ok(v) => v,
        Err(e) => return Err(error_reply(None, PARSE_ERROR, format!("Invalid JSON: {e}"))),
    };

    let Value::Object(mut map) = raw_val else {
        return Err(error_reply(
            None,
            INVALID_REQUEST,
            "JSON-RPC request must be a JSON object",
        ));
    };

    let has_id = map.contains_key("id");
    let id = map.get("id").cloned();
    if has_id
        && !id
            .as_ref()
            .is_some_and(|v| v.is_string() || v.is_number() || v.is_null())
    {
        return Err(error_reply(
            None,
            INVALID_REQUEST,
            "Request 'id' must be a string, number, or null",
        ));
    }

    let method_opt = map
        .get("method")
        .and_then(Value::as_str)
        .filter(|m| !m.is_empty())
        .map(str::to_string);
    let valid_version = map.get("jsonrpc").and_then(Value::as_str) == Some("2.0");
    let valid_params = map
        .get("params")
        .is_none_or(|p| p.is_null() || p.is_object() || p.is_array());

    if !has_id {
        let Some(method) = method_opt else {
            return Err(error_reply(
                None,
                INVALID_REQUEST,
                "Missing or invalid 'method' field",
            ));
        };
        if !(valid_version && valid_params) {
            // Drop messages without an id silently (JSON-RPC 2.0 notifications).
            return Ok(None);
        }
        return Ok(Some(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: None,
            method,
            params: map.remove("params"),
        }));
    }

    if !valid_version {
        return Err(error_reply(
            id,
            INVALID_REQUEST,
            "Protocol version must be '2.0'",
        ));
    }

    let Some(method) = method_opt else {
        return Err(error_reply(
            id,
            INVALID_REQUEST,
            "Missing or invalid 'method' field",
        ));
    };

    if !valid_params {
        return Err(error_reply(
            id,
            INVALID_REQUEST,
            "Request 'params' must be an object or array",
        ));
    }

    Ok(Some(JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id,
        method,
        params: map.remove("params"),
    }))
}

/// The checks `run_stdio_server` applies to the `params` of a `tools/call` request (`id` is the
/// request's id) before it dispatches the call: the tool name, the arguments (absent or null is
/// `{}`) and [`validate_tool_arguments`]. `Ok` is the tool name and its arguments; `Err` is the
/// error response the server writes. Public for the `mcp_request_line` fuzz target (LV-18).
#[doc(hidden)]
pub fn parse_tool_call(
    id: &Option<Value>,
    params: &Value,
) -> Result<(String, Value), Box<JsonRpcResponse>> {
    let Some(params_obj) = params.as_object() else {
        return Err(error_reply(
            id.clone(),
            INVALID_PARAMS,
            "tools/call 'params' must be a JSON object",
        ));
    };
    let Some(name) = params_obj
        .get("name")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
    else {
        return Err(error_reply(
            id.clone(),
            INVALID_PARAMS,
            "Missing or invalid 'name' in tools/call parameters",
        ));
    };

    let arguments = match params_obj.get("arguments") {
        None | Some(Value::Null) => json!({}),
        Some(v) if v.is_object() => v.clone(),
        Some(_) => {
            return Err(error_reply(
                id.clone(),
                INVALID_PARAMS,
                "Tool 'arguments' must be a JSON object",
            ));
        }
    };

    if let Err(err_msg) = validate_tool_arguments(&name, &arguments) {
        return Err(error_reply(id.clone(), INVALID_PARAMS, err_msg));
    }
    Ok((name, arguments))
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
    let completed_submits: Arc<Mutex<std::collections::VecDeque<(String, TaskId)>>> =
        Arc::new(Mutex::new(std::collections::VecDeque::new()));
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

        let JsonRpcRequest {
            id, method, params, ..
        } = match parse_request_line(&line) {
            Ok(Some(request)) => request,
            Ok(None) => continue,
            Err(err_resp) => {
                send_response_line(&out_tx, &err_resp);
                continue;
            }
        };

        if id.is_none() {
            match method.as_str() {
                "notifications/initialized" => {
                    eprintln!(
                        "[mcp] Client handshake completed: notifications/initialized received"
                    );
                }
                "notifications/cancelled" => {
                    let params = params.unwrap_or(Value::Null);
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
                            let (task_to_cancel, was_in_flight) = if let Some(tid_mutex) = maybe_tid
                            {
                                (*tid_mutex.lock().await, true)
                            } else {
                                let mut guard = completed_submits.lock().await;
                                let tid = guard
                                    .iter()
                                    .rposition(|(k, _)| k == &key)
                                    .and_then(|pos| guard.remove(pos))
                                    .map(|(_, tid)| tid);
                                (tid, false)
                            };
                            if let Some(task_id) = task_to_cancel {
                                let state_clone = Arc::clone(&state);
                                tokio::spawn(async move {
                                    if !was_in_flight {
                                        // Post-reply cancel (design 0030): only cancel if the task is
                                        // still non-terminal on the master.
                                        let is_active = matches!(
                                            state_clone
                                                .request(&ClientMessage::GetTaskStatus {
                                                    task_id
                                                })
                                                .await,
                                            Ok(ClientResponse::TaskStatusInfo {
                                                status,
                                                ..
                                            }) if !status.is_terminal()
                                        );
                                        if !is_active {
                                            return;
                                        }
                                    }
                                    let _ = state_clone
                                        .request(&ClientMessage::CancelTask { task_id })
                                        .await;
                                });
                            }
                        }
                    }
                }
                _ => {}
            }
            // Drop messages without an id silently (JSON-RPC 2.0 notifications).
            continue;
        }

        if method != "notifications/cancelled" {
            if let Some(key) = id.as_ref().and_then(request_id_key) {
                completed_submits.lock().await.retain(|(k, _)| k != &key);
            }
        }

        match method.as_str() {
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
                let params = params.unwrap_or(Value::Null);
                let (name, arguments) = match parse_tool_call(&id, &params) {
                    Ok(call) => call,
                    Err(resp) => {
                        send_response_line(&out_tx, &resp);
                        continue;
                    }
                };

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
                let completed_submits_clone = Arc::clone(&completed_submits);
                let active_cancels_clone = Arc::clone(&active_cancels);
                let out_tx_clone = out_tx.clone();

                call_tasks.spawn(async move {
                    let tool_result =
                        call_tool_with_context(&state_clone, &name, &arguments, &mut ctx).await;
                    active_cancels_clone.lock().await.remove(&call_seq);
                    if let Some(ref key) = id_key {
                        if !ctx.is_client_cancelled() && !ctx.aborted_by_cancel {
                            if let Some(tid) = *ctx.submitted_task_id.lock().await {
                                let mut guard = completed_submits_clone.lock().await;
                                guard.retain(|(k, _)| k != key);
                                if guard.len() >= 256 {
                                    guard.pop_front();
                                }
                                guard.push_back((key.clone(), tid));
                            }
                        }
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
    // a 4s grace window to finish naturally and emit their JSON-RPC replies, then cancel any
    // remaining long-running waits and bound cleanup to 500ms before aborting.
    let grace_deadline = tokio::time::Instant::now() + Duration::from_secs(4);
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
    state.close().await;

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
        crate::config::CLIENT_MASTER_ENV_VARS,
        status_cfg.as_ref().and_then(|s| s.target_master()),
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

    let workspace_limits = crate::blobs::WorkspaceLimits {
        max_files: args
            .workspace_max_files
            .unwrap_or(crate::blobs::DEFAULT_WORKSPACE_MAX_FILES),
        max_bytes: args
            .workspace_max_mb
            .map(|mb| mb.saturating_mul(1024 * 1024))
            .unwrap_or(crate::blobs::DEFAULT_WORKSPACE_MAX_BYTES),
    };

    let state =
        Arc::new(McpState::new(master_addr, secret).with_workspace_limits(workspace_limits));

    run_stdio_server(state).await
}

#[cfg(test)]
mod tests {
    use super::submit::{format_completed_task_result, WORKER_TRUNCATION_NOTICE};
    use super::*;
    use clap::Parser;

    /// The reply line the server writes for `line`, or `None` when it writes nothing.
    fn reply_to(line: &str) -> Result<Option<JsonRpcRequest>, String> {
        parse_request_line(line).map_err(|resp| serde_json::to_string(&resp).expect("serialize"))
    }

    // LV-18: the expected lines are the replies of `oxideswarm mcp` built from `master` at
    // `3a0f7c5`, before `parse_request_line` was cut out of `run_stdio_server`.
    #[test]
    fn test_parse_request_line_matches_the_server_before_the_cut() {
        let request = reply_to(
            r#"{"jsonrpc":"2.0","id":3,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}"#,
        )
        .expect("a valid request")
        .expect("not dropped");
        assert_eq!(request.jsonrpc, "2.0");
        assert_eq!(request.id, Some(json!(3)));
        assert_eq!(request.method, "initialize");
        assert_eq!(
            request.params,
            Some(json!({"protocolVersion": "2024-11-05"}))
        );

        assert_eq!(
            reply_to("not json").unwrap_err(),
            r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Invalid JSON: expected ident at line 1 column 2"}}"#
        );
        assert_eq!(
            reply_to(r#"{"jsonrpc":"1.0","id":1,"method":"ping"}"#).unwrap_err(),
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"Protocol version must be '2.0'"}}"#
        );
        assert_eq!(
            reply_to(r#"{"jsonrpc":"2.0","id":"a","method":"ping","params":5}"#).unwrap_err(),
            r#"{"jsonrpc":"2.0","id":"a","error":{"code":-32600,"message":"Request 'params' must be an object or array"}}"#
        );
        assert_eq!(
            reply_to(r#"{"jsonrpc":"2.0","id":true,"method":"ping"}"#).unwrap_err(),
            r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"Request 'id' must be a string, number, or null"}}"#
        );
        assert_eq!(
            reply_to(r#"{"jsonrpc":"2.0","method":5}"#).unwrap_err(),
            r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"Missing or invalid 'method' field"}}"#
        );

        // No reply: a blank line, and a notification with a wrong version or params.
        assert!(reply_to("   ").unwrap().is_none());
        assert!(
            reply_to(r#"{"jsonrpc":"1.0","method":"notifications/initialized"}"#)
                .unwrap()
                .is_none()
        );
        assert!(reply_to(r#"{"jsonrpc":"2.0","method":"x","params":"s"}"#)
            .unwrap()
            .is_none());

        // A notification has no id; `"id": null` is a request answered with `"id": null`.
        let note = reply_to(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#)
            .unwrap()
            .unwrap();
        assert_eq!(note.id, None);
        let null_id = reply_to(r#"{"jsonrpc":"2.0","id":null,"method":"ping"}"#)
            .unwrap()
            .unwrap();
        assert_eq!(null_id.id, Some(Value::Null));
    }

    #[test]
    fn test_parse_tool_call_matches_the_server_before_the_cut() {
        let id = Some(json!(13));
        let reply = |params: Value| {
            parse_tool_call(&id, &params)
                .map_err(|resp| serde_json::to_string(&resp).expect("serialize"))
        };
        assert_eq!(
            reply(json!({"name": "status", "arguments": {"workers": true}})).unwrap(),
            ("status".to_string(), json!({"workers": true}))
        );
        assert_eq!(
            reply(json!({"name": "workspaces", "arguments": null})).unwrap(),
            ("workspaces".to_string(), json!({}))
        );
        assert_eq!(
            reply(Value::Null).unwrap_err(),
            r#"{"jsonrpc":"2.0","id":13,"error":{"code":-32602,"message":"tools/call 'params' must be a JSON object"}}"#
        );
        assert_eq!(
            reply(
                json!({"name": "submit", "arguments": {"isolation": "bogus", "command": "echo"}})
            )
            .unwrap_err(),
            r#"{"jsonrpc":"2.0","id":13,"error":{"code":-32602,"message":"Invalid placement constraints: unknown isolation level 'bogus' (expected off, fs or fs+net)"}}"#
        );
    }

    #[test]
    fn test_submit_isolation_argument_is_a_minimum_level_placement() {
        for level in ["fs", "fs+net"] {
            assert_eq!(
                validate_tool_arguments("submit", &json!({"command": "echo", "isolation": level})),
                Ok(()),
                "{level}"
            );
        }
        for bad in ["off", "strict"] {
            let err =
                validate_tool_arguments("submit", &json!({"command": "echo", "isolation": bad}))
                    .unwrap_err();
            assert!(
                err.contains("Invalid placement constraints"),
                "{bad}: {err}"
            );
        }
        assert!(
            validate_tool_arguments("submit", &json!({"command": "echo", "isolation": 3})).is_err()
        );
        let submit = available_tools()
            .into_iter()
            .find(|t| t.name == "submit")
            .unwrap();
        assert_eq!(
            submit.input_schema["properties"]["isolation"]["enum"],
            json!(["fs", "fs+net"])
        );
    }

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

        let (formatted, truncated, captured_bytes) = truncate_stream_for_llm(&raw, 4096);
        assert!(truncated);
        assert_eq!(captured_bytes, raw.len());
        assert!(
            formatted.len() <= 4096,
            "formatted output ({} bytes) must fit within max_output_bytes (4096)",
            formatted.len()
        );
        assert!(formatted.starts_with("START_BANNER\n"));
        assert!(formatted.ends_with("\nFINAL_SUMMARY_MARKER\n"));
        assert!(formatted.contains("...[truncated "));
    }

    #[test]
    fn test_scan_mcp_inputs_dot_maps_to_sandbox_root_and_handles_colon_cwd() {
        let outer = tempfile::tempdir().unwrap();
        let workspace = outer.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(
            workspace.join("Cargo.toml"),
            b"[package]\nname = \"demo\"\n",
        )
        .unwrap();

        let prepared = scan_mcp_inputs_in_dir(&workspace, &[".".to_string()])
            .expect("staging '.' inside workspace must succeed");
        assert_eq!(prepared.inputs.len(), 1);
        assert_eq!(prepared.inputs[0].path, ".");

        // On Unix, verify a working directory whose name contains ':' does not split at ':'
        // and never stages a sibling directory.
        #[cfg(unix)]
        {
            let sibling_proj = outer.path().join("proj");
            std::fs::create_dir_all(&sibling_proj).unwrap();
            let secret_hash = rusty_grid_core::blob::BlobId::of(b"sibling-secret-do-not-stage");
            std::fs::write(
                sibling_proj.join("secret.txt"),
                b"sibling-secret-do-not-stage",
            )
            .unwrap();

            let colon_cwd = outer.path().join("proj:v2");
            std::fs::create_dir_all(&colon_cwd).unwrap();
            let inside_hash = rusty_grid_core::blob::BlobId::of(b"only-inside-colon-cwd");
            std::fs::write(colon_cwd.join("inside.txt"), b"only-inside-colon-cwd").unwrap();

            let dot_in_colon = scan_mcp_inputs_in_dir(&colon_cwd, &[".".to_string()])
                .expect("staging '.' in cwd with ':' must succeed");
            assert_eq!(dot_in_colon.inputs.len(), 1);
            assert_eq!(dot_in_colon.inputs[0].path, ".");
            assert!(
                dot_in_colon.has_blob(&inside_hash),
                "expected inside.txt blob to be staged"
            );
            assert!(
                !dot_in_colon.has_blob(&secret_hash),
                "sibling /proj/secret.txt must never be staged"
            );

            let file_in_colon = scan_mcp_inputs_in_dir(&colon_cwd, &["inside.txt".to_string()])
                .expect("staging relative file in cwd with ':' must succeed");
            assert_eq!(file_in_colon.inputs.len(), 1);
            assert_eq!(file_in_colon.inputs[0].path, "inside.txt");
            assert_eq!(file_in_colon.inputs[0].blob_id(), inside_hash);
        }
    }

    #[test]
    fn test_prepare_mcp_output_dir_refuses_symlinks_parent_and_outside_cwd() {
        let outer = tempfile::tempdir().unwrap();
        let cwd = outer.path().join("workspace");
        std::fs::create_dir_all(&cwd).unwrap();
        let outside_dir = outer.path().join("outside_dir");
        std::fs::create_dir_all(&outside_dir).unwrap();

        // 1. Valid nested relative output_dir inside cwd is created one by one.
        let ok = prepare_mcp_output_dir_in_dir(&cwd, "nested/outputs")
            .expect("valid relative output_dir inside cwd must succeed");
        assert_eq!(ok, cwd.join("nested/outputs"));
        assert!(ok.is_dir());

        // 2. Parent traversal (`..`) is refused.
        let err_parent = prepare_mcp_output_dir_in_dir(&cwd, "../outside_dir")
            .expect_err("output_dir with '..' must be refused");
        assert!(err_parent.contains(".."), "{err_parent}");

        // 3. Absolute path outside cwd is refused.
        let err_abs = prepare_mcp_output_dir_in_dir(&cwd, &outside_dir.to_string_lossy())
            .expect_err("absolute output_dir outside cwd must be refused");
        assert!(
            err_abs.contains("outside the working directory"),
            "{err_abs}"
        );

        // 4. Symbolic link as output_dir or intermediate component is refused.
        #[cfg(unix)]
        {
            let link_out = cwd.join("out_link");
            std::os::unix::fs::symlink(&outside_dir, &link_out).unwrap();
            let err_link = prepare_mcp_output_dir_in_dir(&cwd, "out_link")
                .expect_err("symlink output_dir must be refused");
            assert!(err_link.contains("symbolic link"), "{err_link}");

            let err_trav = prepare_mcp_output_dir_in_dir(&cwd, "out_link/sub")
                .expect_err("output_dir traversing a symlink must be refused");
            assert!(err_trav.contains("symbolic link"), "{err_trav}");
        }
    }

    #[tokio::test]
    async fn test_max_output_bytes_bounds_jsonrpc_reply_for_0xff_0x01_and_escaped_chars() {
        use rusty_grid_core::task::{TaskId, TaskResult};

        let state = McpState::new("127.0.0.1:8088".to_string(), None);
        let args = json!({ "max_output_bytes": 4096 });
        let base = TaskResult::success(Uuid::new_v4(), TaskId::new(), "", 10, false);

        // Case 1: 20 KB of 0xFF on stdout.
        let res_ff = TaskResult {
            stdout: vec![0xFFu8; 20_000].into(),
            ..base.clone()
        };
        let call_ff = format_completed_task_result(&state, &res_ff, &args, None).await;
        let rpc_ff = serde_json::to_string(&JsonRpcResponse::success(
            Some(json!(1)),
            serde_json::to_value(&call_ff).unwrap(),
        ))
        .unwrap();
        assert!(
            rpc_ff.len() < 8 * 1024,
            "20 KB of 0xFF at max_output_bytes=4096 must give a JSON-RPC reply < 8 KiB, got {} bytes",
            rpc_ff.len()
        );
        let body_ff: Value = serde_json::from_str(match &call_ff.content[0] {
            ToolContent::Text { text } => text,
        })
        .unwrap();
        assert!(body_ff["stdout"].as_str().unwrap().len() <= 4096);
        assert_eq!(body_ff["captured_bytes"], 20_000);
        assert_eq!(body_ff["truncated"], true);
        assert_eq!(body_ff["worker_truncated"], false);

        // Case 2: 20 KB of 0x01 on BOTH stdout and stderr.
        let res_ctrl = TaskResult {
            stdout: vec![0x01u8; 20_000].into(),
            stderr: vec![0x01u8; 20_000].into(),
            ..base.clone()
        };
        let call_ctrl = format_completed_task_result(&state, &res_ctrl, &args, None).await;
        let rpc_ctrl = serde_json::to_string(&JsonRpcResponse::success(
            Some(json!(2)),
            serde_json::to_value(&call_ctrl).unwrap(),
        ))
        .unwrap();
        assert!(
            rpc_ctrl.len() < 8 * 1024,
            "0x01 on both streams at max_output_bytes=4096 must give a JSON-RPC reply < 8 KiB, got {} bytes",
            rpc_ctrl.len()
        );

        // Case 3: Quotes, backslashes, and newlines on BOTH stdout and stderr.
        let pat: Vec<u8> = b"\"\\\n".iter().copied().cycle().take(20_000).collect();
        let res_esc = TaskResult {
            stdout: pat.clone().into(),
            stderr: pat.into(),
            ..base.clone()
        };
        let call_esc = format_completed_task_result(&state, &res_esc, &args, None).await;
        let rpc_esc = serde_json::to_string(&JsonRpcResponse::success(
            Some(json!(3)),
            serde_json::to_value(&call_esc).unwrap(),
        ))
        .unwrap();
        assert!(
            rpc_esc.len() < 8 * 1024,
            "quotes, backslashes and newlines on both streams at max_output_bytes=4096 must give a JSON-RPC reply < 8 KiB, got {} bytes",
            rpc_esc.len()
        );

        // Case 4: Output that merely prints the worker marker text is neither `truncated` nor `worker_truncated`.
        let marker_echo = b"echo [rusty_grid: output truncated after exceeding size limit]\n";
        let res_marker = TaskResult {
            stdout: marker_echo.to_vec().into(),
            ..base.clone()
        };
        let call_marker = format_completed_task_result(&state, &res_marker, &args, None).await;
        let body_marker: Value = serde_json::from_str(match &call_marker.content[0] {
            ToolContent::Text { text } => text,
        })
        .unwrap();
        assert_eq!(body_marker["truncated"], false);
        assert_eq!(body_marker["worker_truncated"], false);
        assert_eq!(body_marker["stdout_truncated"], false);
        assert_eq!(body_marker["stdout_worker_truncated"], false);
        assert_eq!(body_marker["captured_bytes"], marker_echo.len());

        // Case 5: Worker configured with a smaller custom stream cap (e.g. 128 bytes: 64B head + notice + 64B tail)
        // still reports `worker_truncated: true` and `truncated: true`.
        let mut custom_cap_out = vec![b'H'; 64];
        custom_cap_out.extend_from_slice(WORKER_TRUNCATION_NOTICE);
        custom_cap_out.extend_from_slice(&[b'T'; 64]);
        let res_custom_cap = TaskResult {
            stdout: custom_cap_out.into(),
            ..base
        };
        let call_custom_cap =
            format_completed_task_result(&state, &res_custom_cap, &args, None).await;
        let body_custom_cap: Value = serde_json::from_str(match &call_custom_cap.content[0] {
            ToolContent::Text { text } => text,
        })
        .unwrap();
        assert_eq!(body_custom_cap["worker_truncated"], true);
        assert_eq!(body_custom_cap["stdout_worker_truncated"], true);
        assert_eq!(body_custom_cap["truncated"], true);
    }

    #[test]
    fn test_mcp3_tool_schemas_and_argument_validation() {
        let tools = available_tools();
        assert_eq!(tools.len(), 10);
        for t in &tools {
            assert!(
                t.input_schema.get("required").is_some_and(|r| r.is_array()),
                "tool '{}' schema must include a 'required' array",
                t.name
            );
            for forbidden in ["anyOf", "oneOf", "allOf"] {
                assert!(
                    t.input_schema.get(forbidden).is_none(),
                    "tool '{}' schema must not have top-level '{}' (rejected by Claude Messages API)",
                    t.name,
                    forbidden
                );
            }
        }

        // Empty submit (`submit {}`) is rejected without falling back to demo echo.
        assert!(validate_tool_arguments("submit", &json!({})).is_err());
        assert!(validate_tool_arguments("submit", &json!({"type": "command"})).is_err());
        assert!(validate_tool_arguments("submit", &json!({"command": "   "})).is_err());
        assert!(
            validate_tool_arguments("submit", &json!({"type": "unknown", "command": "echo"}))
                .is_err()
        );
        assert!(validate_tool_arguments("submit", &json!({"type": "shell"})).is_err());
        assert!(validate_tool_arguments("submit", &json!({"command": "echo"})).is_ok());
        assert!(
            validate_tool_arguments("submit", &json!({"type": "shell", "script": "echo ok"}))
                .is_ok()
        );
        assert!(validate_tool_arguments("submit", &json!({"type": "gpu"})).is_ok());

        // `verify` options: `verify_any_system` is a boolean (design 0089 D6).
        let verify = |extra: Value| {
            let mut args = json!({"command": "echo", "verify": 1, "outputs": ["o.txt"]});
            args.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            validate_tool_arguments("submit", &args)
        };
        assert!(verify(json!({})).is_ok());
        assert!(verify(json!({"verify_any_system": true})).is_ok());
        assert!(verify(json!({"verify_any_system": "yes"}))
            .unwrap_err()
            .contains("'verify_any_system' must be a boolean"));
        assert!(verify(json!({"verify": "two"})).is_err());
        let schema = available_tools()
            .into_iter()
            .find(|t| t.name == "submit")
            .unwrap()
            .input_schema;
        assert_eq!(schema["properties"]["verify_any_system"]["type"], "boolean");

        // Workspace validation in submit and workspaces tool
        assert!(validate_tool_arguments("workspaces", &json!({})).is_ok());
        assert!(validate_tool_arguments(
            "submit",
            &json!({
                "command": "cargo",
                "args": ["test"],
                "workspace": {
                    "name": "oxideswarm",
                    "sync_dir": ".",
                    "cwd": "crates/core"
                }
            })
        )
        .is_ok());

        // Shard validation
        assert!(validate_tool_arguments("shard", &json!({})).is_err());
        assert!(validate_tool_arguments("shard", &json!({"count": 0, "command": "echo"})).is_err());
        assert!(
            validate_tool_arguments("shard", &json!({"count": 257, "command": "echo"})).is_err()
        );
        assert!(validate_tool_arguments("shard", &json!({"count": 3})).is_err());
        assert!(validate_tool_arguments("shard", &json!({"count": 3, "command": "echo"})).is_ok());
        assert!(validate_tool_arguments("shard", &json!({"count": 3, "build_once": true})).is_ok());
        assert!(validate_tool_arguments(
            "shard",
            &json!({"count": 3, "build_once": true, "command": "echo"})
        )
        .is_err());
        assert!(validate_tool_arguments(
            "shard",
            &json!({"count": 3, "command": "echo", "junit_glob": "**/junit.xml"})
        )
        .is_err());
        assert!(validate_tool_arguments(
            "submit",
            &json!({
                "command": "cargo",
                "workspace": {
                    "name": "oxideswarm",
                    "sync_dir": null
                }
            })
        )
        .is_ok());
        // Combining workspace with files is rejected (-32602).
        assert!(validate_tool_arguments(
            "submit",
            &json!({
                "command": "echo",
                "files": ["a.txt"],
                "workspace": { "name": "oxideswarm" }
            })
        )
        .is_err());
        // Invalid workspace name, parent traversal in sync_dir/cwd, or cwd on shell/gpu rejected.
        assert!(validate_tool_arguments(
            "submit",
            &json!({
                "command": "echo",
                "workspace": { "name": "../bad" }
            })
        )
        .is_err());
        assert!(validate_tool_arguments(
            "submit",
            &json!({
                "command": "echo",
                "workspace": { "name": "ok", "sync_dir": "../outside" }
            })
        )
        .is_err());
        assert!(validate_tool_arguments(
            "submit",
            &json!({
                "command": "echo",
                "workspace": { "name": "ok", "cwd": "../outside" }
            })
        )
        .is_err());
        assert!(validate_tool_arguments(
            "submit",
            &json!({
                "type": "shell",
                "script": "echo hi",
                "workspace": { "name": "ok", "cwd": "sub" }
            })
        )
        .is_err());
        assert!(validate_tool_arguments(
            "submit",
            &json!({
                "type": "gpu",
                "workspace": { "name": "ok" }
            })
        )
        .is_err());

        // Matrix schema and validation in submit (design 0101, AX-1).
        assert_eq!(schema["properties"]["matrix"]["type"], "array");
        assert_eq!(schema["properties"]["matrix"]["items"]["type"], "string");
        assert_eq!(schema["properties"]["max_matrix_cells"]["type"], "integer");
        assert!(validate_tool_arguments(
            "submit",
            &json!({"command": "echo", "matrix": ["tag=a,b", "tag=c,d"]})
        )
        .is_ok());
        assert!(validate_tool_arguments(
            "submit",
            &json!({"command": "echo", "matrix": {"tag": ["a", "b"], "env:MODE": ["x", "y"]}})
        )
        .is_ok());
        assert!(
            validate_tool_arguments("submit", &json!({"command": "echo", "matrix": []})).is_err()
        );
        assert!(
            validate_tool_arguments("submit", &json!({"command": "echo", "matrix": {}})).is_err()
        );
        assert!(validate_tool_arguments(
            "submit",
            &json!({"command": "echo", "matrix": "not-valid"})
        )
        .is_err());
        assert!(validate_tool_arguments(
            "submit",
            &json!({"command": "echo", "matrix": ["os=macos"], "os": "linux"})
        )
        .is_err());
        assert!(validate_tool_arguments(
            "submit",
            &json!({"command": "echo", "matrix": ["tag=a"], "verify": 1, "outputs": ["o.txt"]})
        )
        .is_err());
        assert!(validate_tool_arguments(
            "submit",
            &json!({"command": "echo", "matrix": ["tag=a"], "wait": false})
        )
        .is_err());
        assert!(validate_tool_arguments(
            "submit",
            &json!({"type": "gpu", "matrix": ["env:SEED=1,2"]})
        )
        .is_err());
        let big_vals = (0..65)
            .map(|i| format!("t{i}"))
            .collect::<Vec<_>>()
            .join(",");
        assert!(validate_tool_arguments(
            "submit",
            &json!({"command": "echo", "matrix": [format!("tag={big_vals}")]})
        )
        .is_err());
        assert!(validate_tool_arguments(
            "submit",
            &json!({"command": "echo", "matrix": [format!("tag={big_vals}")], "max_matrix_cells": 100})
        )
        .is_ok());
        assert!(validate_tool_arguments(
            "submit",
            &json!({"command": "echo", "max_matrix_cells": 10})
        )
        .is_err());

        // Unknown tool, invalid UUIDs, and negative integers are rejected.
        assert!(validate_tool_arguments("unknown_tool", &json!({})).is_err());
        assert!(validate_tool_arguments("status", &json!({"task_id": "not-a-uuid"})).is_err());
        assert!(validate_tool_arguments("task_result", &json!({})).is_err());
        assert!(validate_tool_arguments("cancel", &json!({})).is_err());
        assert!(validate_tool_arguments("doctor", &json!({"expect_workers": -1})).is_err());
    }

    #[tokio::test]
    async fn test_mcp3_status_reports_no_workers_and_total_tasks_against_workerless_master() {
        use rusty_grid_master::reaper::ReaperConfig;
        use rusty_grid_master::scheduler::SchedulerConfig;
        use rusty_grid_master::server::{MasterServer, ServerConfig};

        let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_web_ui(false);
        let master = MasterServer::spawn_with_config(
            server_config,
            SchedulerConfig::default(),
            ReaperConfig::default(),
        )
        .await
        .expect("master starts");

        let state = McpState::new(master.server_addr().to_string(), None);

        // 1. Fresh master with 0 workers reports `"cluster_status": "no_workers"` and `total_tasks: 0`.
        let status_empty = call_tool(&state, "status", &json!({})).await;
        assert!(!status_empty.is_error);
        let empty_val: Value = serde_json::from_str(match &status_empty.content[0] {
            ToolContent::Text { text } => text,
        })
        .unwrap();
        assert_eq!(empty_val["cluster_status"], "no_workers");
        assert_eq!(empty_val["connected_workers"], 0);
        assert_eq!(empty_val["total_workers"], 0);
        assert_eq!(empty_val["total_tasks"], 0);
        assert_eq!(empty_val["pending_tasks"], 0);

        // 2. Submitting a task (`wait: false`) increments `total_tasks` and `pending_tasks` while
        // `cluster_status` remains `"no_workers"`.
        let submit_res = call_tool(
            &state,
            "submit",
            &json!({"type": "command", "command": "echo", "args": ["queued"], "wait": false}),
        )
        .await;
        assert!(!submit_res.is_error);

        let status_queued = call_tool(&state, "status", &json!({})).await;
        assert!(!status_queued.is_error);
        let queued_val: Value = serde_json::from_str(match &status_queued.content[0] {
            ToolContent::Text { text } => text,
        })
        .unwrap();
        assert_eq!(queued_val["cluster_status"], "no_workers");
        assert_eq!(queued_val["connected_workers"], 0);
        assert_eq!(queued_val["total_tasks"], 1);
        assert_eq!(queued_val["pending_tasks"], 1);

        let _ = master.shutdown();
    }

    #[tokio::test]
    async fn test_submit_tools_argument_validates_and_adds_require_tool_placement() {
        assert_eq!(
            validate_tool_arguments(
                "submit",
                &json!({"command": "echo", "tools": ["cargo", "rustc", "GIT"]})
            ),
            Ok(())
        );
        for bad in [
            json!({"command": "echo", "tools": [""]}),
            json!({"command": "echo", "tools": ["bad:tool"]}),
            json!({"command": "echo", "tools": "cargo"}),
        ] {
            assert!(
                validate_tool_arguments("submit", &bad).is_err(),
                "expected invalid tools arg {bad} to be rejected"
            );
        }

        let submit_schema = available_tools()
            .into_iter()
            .find(|t| t.name == "submit")
            .unwrap()
            .input_schema;
        assert_eq!(submit_schema["properties"]["tools"]["type"], "array");
        assert_eq!(
            submit_schema["properties"]["tools"]["items"]["type"],
            "string"
        );

        use rusty_grid_master::reaper::ReaperConfig;
        use rusty_grid_master::scheduler::SchedulerConfig;
        use rusty_grid_master::server::{MasterServer, ServerConfig};

        let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_web_ui(false);
        let master = MasterServer::spawn_with_config(
            server_config,
            SchedulerConfig::default(),
            ReaperConfig::default(),
        )
        .await
        .expect("master starts");

        let state = McpState::new(master.server_addr().to_string(), None);
        let submit_res = call_tool(
            &state,
            "submit",
            &json!({
                "type": "command",
                "command": "echo",
                "args": ["needs-cargo"],
                "tools": ["cargo", "git"],
                "wait": false
            }),
        )
        .await;
        assert!(!submit_res.is_error, "{submit_res:?}");

        let tasks = master.queue().list_tasks().await;
        assert_eq!(tasks.len(), 1);
        let task = master
            .queue()
            .get_task_spec(&tasks[0].task_id)
            .await
            .expect("task spec");
        let placement = task.placement().expect("valid placement");
        assert_eq!(
            placement.tools,
            vec!["cargo".to_string(), "git".to_string()]
        );

        // Compile task with Cargo.toml automatically adds `require:tool=cargo` when `tools` is omitted.
        let compile_cargo_res = call_tool(
            &state,
            "submit",
            &json!({
                "type": "compile",
                "source_files": {
                    "Cargo.toml": "[package]\nname = \"mcp_bin\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
                    "src/main.rs": "fn main() {}\n"
                },
                "wait": false
            }),
        )
        .await;
        assert!(!compile_cargo_res.is_error, "{compile_cargo_res:?}");
        let cargo_submit_val: Value = serde_json::from_str(match &compile_cargo_res.content[0] {
            ToolContent::Text { text } => text,
        })
        .unwrap();
        let cargo_tid = rusty_grid_core::TaskId::from_uuid(
            Uuid::parse_str(cargo_submit_val["task_id"].as_str().unwrap()).unwrap(),
        );
        let cargo_task = master
            .queue()
            .get_task_spec(&cargo_tid)
            .await
            .expect("cargo task");
        assert_eq!(
            cargo_task.placement().unwrap().tools,
            vec!["cargo".to_string()]
        );

        // Compile task without Cargo.toml automatically adds `require:tool=rustc` when `tools` is omitted.
        let compile_rustc_res = call_tool(
            &state,
            "submit",
            &json!({
                "type": "compile",
                "command": "mcp_lib",
                "source_files": {
                    "src/lib.rs": "pub fn answer() -> u32 { 42 }\n"
                },
                "wait": false
            }),
        )
        .await;
        assert!(!compile_rustc_res.is_error, "{compile_rustc_res:?}");
        let rustc_submit_val: Value = serde_json::from_str(match &compile_rustc_res.content[0] {
            ToolContent::Text { text } => text,
        })
        .unwrap();
        let rustc_tid = rusty_grid_core::TaskId::from_uuid(
            Uuid::parse_str(rustc_submit_val["task_id"].as_str().unwrap()).unwrap(),
        );
        let rustc_task = master
            .queue()
            .get_task_spec(&rustc_tid)
            .await
            .expect("rustc task");
        assert_eq!(
            rustc_task.placement().unwrap().tools,
            vec!["rustc".to_string()]
        );

        // Explicit `tools` on a compile task is preserved without duplication.
        let compile_explicit_res = call_tool(
            &state,
            "submit",
            &json!({
                "type": "compile",
                "source_files": {
                    "Cargo.toml": "[package]\nname = \"mcp_explicit\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
                    "src/main.rs": "fn main() {}\n"
                },
                "tools": ["cargo", "git"],
                "wait": false
            }),
        )
        .await;
        assert!(!compile_explicit_res.is_error, "{compile_explicit_res:?}");
        let explicit_submit_val: Value =
            serde_json::from_str(match &compile_explicit_res.content[0] {
                ToolContent::Text { text } => text,
            })
            .unwrap();
        let explicit_tid = rusty_grid_core::TaskId::from_uuid(
            Uuid::parse_str(explicit_submit_val["task_id"].as_str().unwrap()).unwrap(),
        );
        let explicit_task = master
            .queue()
            .get_task_spec(&explicit_tid)
            .await
            .expect("explicit task");
        assert_eq!(
            explicit_task.placement().unwrap().tools,
            vec!["cargo".to_string(), "git".to_string()]
        );

        let _ = master.shutdown();
    }

    #[tokio::test]
    async fn test_submit_vram_mb_argument_validates_sets_requirements_and_warns() {
        assert_eq!(
            validate_tool_arguments("submit", &json!({"command": "echo", "vram_mb": 4096})),
            Ok(())
        );
        for bad in [
            json!({"command": "echo", "vram_mb": "4096"}),
            json!({"command": "echo", "vram_mb": -1}),
            json!({"command": "echo", "vram_mb": 1.5}),
        ] {
            assert!(
                validate_tool_arguments("submit", &bad).is_err(),
                "expected invalid vram_mb arg {bad} to be rejected"
            );
        }

        let submit_schema = available_tools()
            .into_iter()
            .find(|t| t.name == "submit")
            .unwrap()
            .input_schema;
        assert_eq!(submit_schema["properties"]["vram_mb"]["type"], "integer");
        assert!(
            submit_schema["properties"]["vram_mb"]["description"]
                .as_str()
                .is_some_and(|s| s.contains("VRAM")),
            "expected VRAM description on vram_mb schema property"
        );

        use rusty_grid_master::reaper::ReaperConfig;
        use rusty_grid_master::scheduler::SchedulerConfig;
        use rusty_grid_master::server::{MasterServer, ServerConfig};

        let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_web_ui(false);
        let master = MasterServer::spawn_with_config(
            server_config,
            SchedulerConfig::default(),
            ReaperConfig::default(),
        )
        .await
        .expect("master starts");

        let state = McpState::new(master.server_addr().to_string(), None);
        let submit_res = call_tool(
            &state,
            "submit",
            &json!({
                "type": "command",
                "command": "echo",
                "args": ["needs-vram"],
                "vram_mb": 4096,
                "wait": false
            }),
        )
        .await;
        assert!(!submit_res.is_error, "{submit_res:?}");

        let body: Value = serde_json::from_str(match &submit_res.content[0] {
            ToolContent::Text { text } => text,
        })
        .unwrap();
        assert_eq!(
            body["warning"],
            "Warning: no connected worker matches the task resource requirements; the task waits in the queue until one does"
        );

        let tid = rusty_grid_core::TaskId::from_uuid(
            Uuid::parse_str(body["task_id"].as_str().unwrap()).unwrap(),
        );
        let task = master
            .queue()
            .get_task_spec(&tid)
            .await
            .expect("submitted task spec");
        assert_eq!(task.requirements.vram_mb, Some(4096));
        assert!(task.requirements.gpu_required);
        assert!(task.requirements.requires_gpu_or_vram());

        let _ = master.shutdown();
    }

    #[tokio::test]
    async fn test_submit_repeatable_argument_validates_and_attaches_opt_repeatable_tag() {
        assert_eq!(
            validate_tool_arguments("submit", &json!({"command": "echo", "repeatable": true})),
            Ok(())
        );
        assert!(validate_tool_arguments(
            "submit",
            &json!({"command": "echo", "repeatable": "yes"})
        )
        .is_err());

        let submit_schema = available_tools()
            .into_iter()
            .find(|t| t.name == "submit")
            .unwrap()
            .input_schema;
        assert_eq!(submit_schema["properties"]["repeatable"]["type"], "boolean");

        use rusty_grid_master::reaper::ReaperConfig;
        use rusty_grid_master::scheduler::SchedulerConfig;
        use rusty_grid_master::server::{MasterServer, ServerConfig};

        let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_web_ui(false);
        let master = MasterServer::spawn_with_config(
            server_config,
            SchedulerConfig::default(),
            ReaperConfig::default(),
        )
        .await
        .expect("master starts");

        let state = McpState::new(master.server_addr().to_string(), None);
        let submit_res = call_tool(
            &state,
            "submit",
            &json!({
                "type": "command",
                "command": "echo",
                "args": ["rep"],
                "repeatable": true,
                "wait": false
            }),
        )
        .await;
        assert!(!submit_res.is_error, "{submit_res:?}");

        let body: Value = serde_json::from_str(match &submit_res.content[0] {
            ToolContent::Text { text } => text,
        })
        .unwrap();
        let tid = rusty_grid_core::TaskId::from_uuid(
            Uuid::parse_str(body["task_id"].as_str().unwrap()).unwrap(),
        );
        let task = master
            .queue()
            .get_task_spec(&tid)
            .await
            .expect("submitted task spec");
        assert!(task.is_repeatable());
        assert!(task
            .tags
            .iter()
            .any(|t| t == rusty_grid_core::placement::OPT_REPEATABLE_TAG));

        let _ = master.shutdown();
    }

    #[tokio::test]
    async fn test_tool_shard_direct_defense_in_depth_refuses_workspace_with_build_once_or_files() {
        let state = McpState::new("127.0.0.1:0".to_string(), None);
        let mut ctx = ToolCallContext::default();

        let res_bo = shard::tool_shard(
            &state,
            &json!({
                "count": 1,
                "workspace": "ws",
                "build_once": true
            }),
            &mut ctx,
        )
        .await;
        assert!(res_bo.is_error, "{res_bo:?}");
        let msg_bo = match &res_bo.content[0] {
            ToolContent::Text { text } => text.as_str(),
        };
        assert!(msg_bo.contains("build_once"), "{msg_bo}");

        let res_files = shard::tool_shard(
            &state,
            &json!({
                "count": 1,
                "workspace": "ws",
                "files": ["a.txt"],
                "command": "echo"
            }),
            &mut ctx,
        )
        .await;
        assert!(res_files.is_error, "{res_files:?}");
        let msg_files = match &res_files.content[0] {
            ToolContent::Text { text } => text.as_str(),
        };
        assert!(msg_files.contains("files"), "{msg_files}");
    }

    #[test]
    fn test_call_tool_future_size_stays_bounded() {
        let state = McpState::new("127.0.0.1:0".to_string(), None);
        let args = json!({});
        let fut = call_tool(&state, "submit", &args);
        let fut_size = std::mem::size_of_val(&fut);
        assert!(
            fut_size <= 16 * 1024,
            "call_tool future must stay small enough for 2 MiB Linux test stacks, got {fut_size} bytes"
        );
    }
}
