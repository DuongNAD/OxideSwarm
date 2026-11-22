//! `submit --matrix` and MCP `submit` `matrix` fan-out (design 0101, AX-1).
//!
//! Expands a Cartesian product of placement (`os`, `arch`, `worker`, `iso`, `tool`, `tag`) and
//! process environment (`env:<VAR>`) axes client-side into individual `SubmitTask` requests on
//! Protocol 10. Pre-flight `ListWorkers` checks mark unfulfillable cells `unplaced` immediately
//! without submitting them or hanging, and shared input blobs or workspace trees are uploaded
//! at most once per submission.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Map, Value};
use tokio::sync::{mpsc, watch, Semaphore};
use uuid::Uuid;

use rusty_grid_core::placement::{
    listed_supports_workspaces, listed_worker_id, normalize_arch, normalize_os, tag_value,
    validate_tool_name, validate_user_tag, IsolationLevel, Placement, REQUIRE_PREFIX,
    STATE_TAG_PREFIX,
};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse, MessageTransport};
use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskResult, TaskSpec};
use rusty_grid_core::transport::GridStream;
use rusty_grid_core::WorkerCapabilities;

use crate::blobs::UploadReport;
use crate::mcp::submit::{
    escaped_str_bytes, split_stream_budgets, stream_was_worker_truncated, truncate_stream_for_llm,
};
use crate::run_summary::RunSummary;

/// Default maximum number of expanded matrix cells per submission.
pub const DEFAULT_MAX_MATRIX_CELLS: usize = 64;

/// Hard upper bound on expanded matrix cells (`--max-matrix-cells` / `max_matrix_cells`).
pub const HARD_MAX_MATRIX_CELLS: usize = 256;

/// Default per-cell inline stdout/stderr escaped-JSON byte budget for CLI `--json`.
pub const DEFAULT_CELL_OUTPUT_BYTES: usize = 16_384;

/// Cap concurrent TCP/Noise handshakes under the master's default `max_pending_handshakes` (64).
const MAX_CONCURRENT_HANDSHAKES: usize = 32;

/// Upper bound on wait deadlines so `u64::MAX` / `Duration::MAX` timeouts cannot overflow `Instant`.
const MAX_MATRIX_WAIT: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// Computes `Instant::now() + timeout`, capping at [`MAX_MATRIX_WAIT`] and using `checked_add`
/// so extreme user-supplied timeouts never panic.
pub(crate) fn safe_deadline(timeout: Duration) -> tokio::time::Instant {
    let now = tokio::time::Instant::now();
    let capped = timeout.min(MAX_MATRIX_WAIT);
    now.checked_add(capped)
        .or_else(|| now.checked_add(Duration::from_secs(60)))
        .unwrap_or(now)
}

/// Computes `std::time::Instant::now() + timeout`, capping at [`MAX_MATRIX_WAIT`] and using
/// `checked_add` so extreme user-supplied timeouts never panic.
pub(crate) fn safe_std_deadline(timeout: Duration) -> std::time::Instant {
    let now = std::time::Instant::now();
    let capped = timeout.min(MAX_MATRIX_WAIT);
    now.checked_add(capped)
        .or_else(|| now.checked_add(Duration::from_secs(60)))
        .unwrap_or(now)
}

/// Verifies that `<base_out_dir>/<slug>` is not an existing symbolic link or non-directory entry
/// before downloading matrix cell outputs or saving artifacts.
pub fn ensure_non_symlink_cell_dir(
    base_out_dir: &std::path::Path,
    slug: &str,
) -> Result<PathBuf, String> {
    let cell_dir = base_out_dir.join(slug);
    match std::fs::symlink_metadata(&cell_dir) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(format!(
                    "cell output directory '{}' is a symbolic link, which is refused",
                    cell_dir.display()
                ));
            }
            if !meta.is_dir() {
                return Err(format!(
                    "cell output path '{}' exists and is not a directory",
                    cell_dir.display()
                ));
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(format!(
                "cannot inspect cell output directory '{}': {e}",
                cell_dir.display()
            ));
        }
    }
    Ok(cell_dir)
}

/// Help text for `--matrix`.
pub const MATRIX_HELP: &str = "Fan out the task over the Cartesian product of KEY=V1,V2,... axes (repeatable; implies --wait). Placement keys: os, arch, worker, iso, tool, tag; process tasks also accept env:<VAR>. Unplaced cells fail upfront via ListWorkers without hanging";

/// Help text for `--max-matrix-cells`.
pub const MAX_MATRIX_CELLS_HELP: &str =
    "With --matrix: maximum expanded cells allowed (default: 64, max: 256)";

/// Supported matrix axis categories.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatrixAxisKind {
    Os,
    Arch,
    Worker,
    Iso,
    Tool,
    Tag,
    Env(String),
}

impl MatrixAxisKind {
    fn is_singular(&self) -> bool {
        !matches!(self, Self::Tool | Self::Tag)
    }

    fn key_label(&self) -> String {
        match self {
            Self::Os => "os".to_string(),
            Self::Arch => "arch".to_string(),
            Self::Worker => "worker".to_string(),
            Self::Iso => "iso".to_string(),
            Self::Tool => "tool".to_string(),
            Self::Tag => "tag".to_string(),
            Self::Env(var) => format!("env:{var}"),
        }
    }
}

/// One parsed axis of a matrix specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatrixAxis {
    pub key: String,
    pub kind: MatrixAxisKind,
    pub values: Vec<String>,
}

/// Validated matrix specification before Cartesian expansion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatrixSpec {
    pub axes: Vec<MatrixAxis>,
}

fn validate_max_cells(max_cells: usize) -> Result<(), String> {
    if max_cells == 0 || max_cells > HARD_MAX_MATRIX_CELLS {
        return Err(format!(
            "--max-matrix-cells / max_matrix_cells must be between 1 and {HARD_MAX_MATRIX_CELLS} (got {max_cells})"
        ));
    }
    Ok(())
}

fn parse_axis_key(raw_key: &str) -> Result<(String, MatrixAxisKind), String> {
    let key = raw_key.trim();
    if key.is_empty() {
        return Err("matrix axis key cannot be empty (expected KEY=V1,V2,...)".to_string());
    }
    if let Some(var) = key.strip_prefix("env:") {
        if var.is_empty()
            || var.trim() != var
            || var
                .chars()
                .any(|c| c.is_whitespace() || c == '=' || c == ',' || c == '\0')
        {
            return Err(format!(
                "invalid matrix env axis '{key}': expected env:<VAR> without spaces, '=', or ','"
            ));
        }
        return Ok((format!("env:{var}"), MatrixAxisKind::Env(var.to_string())));
    }
    match key.to_ascii_lowercase().as_str() {
        "os" => Ok(("os".to_string(), MatrixAxisKind::Os)),
        "arch" => Ok(("arch".to_string(), MatrixAxisKind::Arch)),
        "worker" => Ok(("worker".to_string(), MatrixAxisKind::Worker)),
        "iso" => Ok(("iso".to_string(), MatrixAxisKind::Iso)),
        "tool" => Ok(("tool".to_string(), MatrixAxisKind::Tool)),
        "tag" => Ok(("tag".to_string(), MatrixAxisKind::Tag)),
        _ => Err(format!(
            "unknown matrix axis '{key}' (supported: os, arch, worker, iso, tool, tag, env:<VAR>)"
        )),
    }
}

fn validate_axis_values(
    key: &str,
    kind: &MatrixAxisKind,
    raw_values: Vec<String>,
) -> Result<Vec<String>, String> {
    if raw_values.is_empty() {
        return Err(format!(
            "matrix axis '{key}' must specify at least one value"
        ));
    }
    let mut normalized = Vec::with_capacity(raw_values.len());
    let mut seen = BTreeSet::new();
    for raw in raw_values {
        if raw.contains(',') || raw.contains('=') {
            return Err(format!(
                "matrix axis '{key}' value '{raw}' must not contain ',' or '='"
            ));
        }
        let (val, dedup_key) = match kind {
            MatrixAxisKind::Os => {
                let t = raw.trim();
                if t.is_empty() || t.chars().any(char::is_whitespace) {
                    return Err(format!(
                        "matrix axis '{key}' contains an empty or whitespace value"
                    ));
                }
                let norm = normalize_os(t);
                (norm.clone(), norm)
            }
            MatrixAxisKind::Arch => {
                let t = raw.trim();
                if t.is_empty() || t.chars().any(char::is_whitespace) {
                    return Err(format!(
                        "matrix axis '{key}' contains an empty or whitespace value"
                    ));
                }
                let norm = normalize_arch(t);
                (norm.clone(), norm)
            }
            MatrixAxisKind::Worker => {
                let t = raw.trim();
                if t.is_empty() {
                    return Err(format!("matrix axis '{key}' contains an empty value"));
                }
                (t.to_string(), t.to_ascii_lowercase())
            }
            MatrixAxisKind::Iso => {
                let t = raw.trim();
                if t.is_empty() {
                    return Err(format!("matrix axis '{key}' contains an empty value"));
                }
                let level = IsolationLevel::parse_required(t)
                    .map_err(|e| format!("matrix axis '{key}': {e}"))?;
                let s = level.to_string();
                (s.clone(), s)
            }
            MatrixAxisKind::Tool => {
                let t = raw.trim();
                let norm =
                    validate_tool_name(t).map_err(|e| format!("matrix axis '{key}': {e}"))?;
                (norm.clone(), norm)
            }
            MatrixAxisKind::Tag => {
                let t = raw.trim();
                validate_user_tag(t).map_err(|e| format!("matrix axis '{key}': {e}"))?;
                (t.to_string(), t.to_string())
            }
            MatrixAxisKind::Env(_) => {
                if raw.is_empty() || raw.contains('\0') {
                    return Err(format!(
                        "matrix axis '{key}' contains an empty or NUL value"
                    ));
                }
                (raw.clone(), raw)
            }
        };
        if !seen.insert(dedup_key) {
            return Err(format!("duplicate value '{val}' in matrix axis '{key}'"));
        }
        normalized.push(val);
    }
    Ok(normalized)
}

fn validate_axes_list(axes: Vec<MatrixAxis>, max_cells: usize) -> Result<MatrixSpec, String> {
    validate_max_cells(max_cells)?;
    if axes.is_empty() {
        return Err("matrix must contain at least one axis".to_string());
    }
    let mut seen_singular = BTreeSet::new();
    let mut total_cells = 1usize;
    for axis in &axes {
        if axis.kind.is_singular() && !seen_singular.insert(axis.kind.key_label()) {
            return Err(format!("duplicate matrix axis '{}'", axis.key));
        }
        total_cells = total_cells
            .checked_mul(axis.values.len())
            .ok_or_else(|| format!("matrix cell count exceeds limit of {max_cells}"))?;
        if total_cells > max_cells {
            return Err(format!(
                "matrix expands to {total_cells} cells, exceeding the limit of {max_cells}"
            ));
        }
    }
    Ok(MatrixSpec { axes })
}

impl MatrixSpec {
    /// Parses CLI `--matrix KEY=V1,V2,...` specs and enforces `max_cells`.
    pub fn parse(raw_axes: &[String], max_cells: usize) -> Result<Self, String> {
        validate_max_cells(max_cells)?;
        if raw_axes.is_empty() {
            return Err("matrix must contain at least one KEY=V1,V2,... axis".to_string());
        }
        let mut axes = Vec::with_capacity(raw_axes.len());
        for raw in raw_axes {
            let Some((raw_key, raw_vals)) = raw.split_once('=') else {
                return Err(format!(
                    "invalid matrix axis '{raw}': expected KEY=V1,V2,..."
                ));
            };
            let (key, kind) = parse_axis_key(raw_key)?;
            if raw_vals.is_empty() {
                return Err(format!(
                    "matrix axis '{key}' must specify at least one value"
                ));
            }
            let raw_values: Vec<String> = raw_vals.split(',').map(str::to_string).collect();
            let values = validate_axis_values(&key, &kind, raw_values)?;
            axes.push(MatrixAxis { key, kind, values });
        }
        validate_axes_list(axes, max_cells)
    }

    /// Parses an MCP `matrix` parameter (either `["key=v1,v2"]` or `{"key": ["v1", "v2"]}`).
    pub fn parse_mcp_value(value: &Value, max_cells: usize) -> Result<Self, String> {
        validate_max_cells(max_cells)?;
        match value {
            Value::Array(arr) => {
                if arr.is_empty() {
                    return Err("Property 'matrix' must not be empty".to_string());
                }
                let mut raw_axes = Vec::with_capacity(arr.len());
                for item in arr {
                    let s = item.as_str().ok_or_else(|| {
                        "Property 'matrix' array elements must be 'key=v1,v2,...' strings"
                            .to_string()
                    })?;
                    raw_axes.push(s.to_string());
                }
                Self::parse(&raw_axes, max_cells)
            }
            Value::Object(obj) => {
                if obj.is_empty() {
                    return Err("Property 'matrix' object must not be empty".to_string());
                }
                let mut axes = Vec::with_capacity(obj.len());
                for (raw_key, vals_val) in obj {
                    let (key, kind) = parse_axis_key(raw_key)?;
                    let vals_arr = vals_val.as_array().ok_or_else(|| {
                        format!("matrix axis '{key}' in object form must be a non-empty array of strings")
                    })?;
                    let mut raw_values = Vec::with_capacity(vals_arr.len());
                    for item in vals_arr {
                        let s = item.as_str().ok_or_else(|| {
                            format!("matrix axis '{key}' values must all be strings")
                        })?;
                        raw_values.push(s.to_string());
                    }
                    let values = validate_axis_values(&key, &kind, raw_values)?;
                    axes.push(MatrixAxis { key, kind, values });
                }
                validate_axes_list(axes, max_cells)
            }
            _ => Err(
                "Property 'matrix' must be an array of 'key=v1,v2,...' strings or a non-empty axis map"
                    .to_string(),
            ),
        }
    }

    /// Total number of Cartesian cells.
    pub fn cell_count(&self) -> usize {
        self.axes
            .iter()
            .fold(1usize, |acc, a| acc.saturating_mul(a.values.len()))
    }
}

/// One expanded cell of a matrix submission.
#[derive(Debug, Clone)]
pub struct MatrixCell {
    pub index: usize,
    pub assignments: Vec<(String, String)>,
    pub label: String,
    pub slug: String,
    pub placement: Placement,
    pub env_overrides: BTreeMap<String, String>,
}

impl MatrixCell {
    /// Serializes this cell's axis assignments as a JSON object.
    ///
    /// Keys that appear once map to `"val"`; repeated keys (`tag`, `tool`) map to `["v1", "v2"]`.
    pub fn axes_json(&self) -> Value {
        let mut grouped: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for (k, v) in &self.assignments {
            grouped.entry(k.as_str()).or_default().push(v.as_str());
        }
        let mut map = Map::new();
        for (k, vals) in grouped {
            if vals.len() == 1 {
                map.insert(k.to_string(), Value::String(vals[0].to_string()));
            } else {
                map.insert(
                    k.to_string(),
                    Value::Array(
                        vals.into_iter()
                            .map(|s| Value::String(s.to_string()))
                            .collect(),
                    ),
                );
            }
        }
        Value::Object(map)
    }
}

fn sanitize_slug_component(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '.' || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    let trimmed = out.trim_matches(|c| c == '.' || c == '_');
    if trimmed.is_empty() {
        "val".to_string()
    } else {
        trimmed.to_string()
    }
}

fn build_cell_slug(index: usize, assignments: &[(String, String)]) -> String {
    let mut suffix = assignments
        .iter()
        .map(|(_, v)| sanitize_slug_component(v))
        .collect::<Vec<_>>()
        .join("-");
    if suffix.len() > 48 {
        suffix.truncate(48);
        suffix = suffix.trim_end_matches(['-', '_', '.']).to_string();
    }
    if suffix.is_empty() {
        format!("cell-{index:02}")
    } else {
        format!("cell-{index:02}-{suffix}")
    }
}

/// Expands `spec` into deterministic Cartesian cells merged onto `base_placement`.
pub fn expand_cells(
    spec: &MatrixSpec,
    base_placement: &Placement,
    allow_env_axes: bool,
) -> Result<Vec<MatrixCell>, String> {
    for axis in &spec.axes {
        match &axis.kind {
            MatrixAxisKind::Os if base_placement.os.is_some() => {
                return Err(
                    "matrix axis 'os' cannot be combined with an explicit --os / os constraint"
                        .to_string(),
                );
            }
            MatrixAxisKind::Arch if base_placement.arch.is_some() => {
                return Err(
                    "matrix axis 'arch' cannot be combined with an explicit --arch / arch constraint"
                        .to_string(),
                );
            }
            MatrixAxisKind::Worker if base_placement.worker.is_some() => {
                return Err(
                    "matrix axis 'worker' cannot be combined with an explicit --worker / worker constraint"
                        .to_string(),
                );
            }
            MatrixAxisKind::Iso if base_placement.isolation.is_some() => {
                return Err(
                    "matrix axis 'iso' cannot be combined with an explicit --require-isolation / isolation constraint"
                        .to_string(),
                );
            }
            MatrixAxisKind::Env(_) if !allow_env_axes => {
                return Err(format!(
                    "matrix axis '{}' is only supported for command and shell tasks",
                    axis.key
                ));
            }
            _ => {}
        }
    }

    let mut combos: Vec<Vec<(usize, String)>> = vec![Vec::new()];
    for (axis_idx, axis) in spec.axes.iter().enumerate() {
        let mut next = Vec::with_capacity(combos.len().saturating_mul(axis.values.len()));
        for prefix in &combos {
            for val in &axis.values {
                let mut item = prefix.clone();
                item.push((axis_idx, val.clone()));
                next.push(item);
            }
        }
        combos = next;
    }

    let base_tags = base_placement.to_task_tags();
    let mut cells = Vec::with_capacity(combos.len());
    for (index, combo) in combos.into_iter().enumerate() {
        let mut tags = base_tags.clone();
        let mut assignments = Vec::with_capacity(combo.len());
        let mut env_overrides = BTreeMap::new();
        for (axis_idx, val) in combo {
            let axis = &spec.axes[axis_idx];
            assignments.push((axis.key.clone(), val.clone()));
            match &axis.kind {
                MatrixAxisKind::Env(var) => {
                    env_overrides.insert(var.clone(), val);
                }
                MatrixAxisKind::Os => tags.push(format!("{REQUIRE_PREFIX}os={val}")),
                MatrixAxisKind::Arch => tags.push(format!("{REQUIRE_PREFIX}arch={val}")),
                MatrixAxisKind::Worker => tags.push(format!("{REQUIRE_PREFIX}worker={val}")),
                MatrixAxisKind::Iso => tags.push(format!("{REQUIRE_PREFIX}iso={val}")),
                MatrixAxisKind::Tool => tags.push(format!("{REQUIRE_PREFIX}tool={val}")),
                MatrixAxisKind::Tag => tags.push(format!("{REQUIRE_PREFIX}tag={val}")),
            }
        }
        let mut placement = Placement::from_task_tags(&tags)?;
        placement.extensions = base_placement.extensions.clone();
        let label = assignments
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",");
        let slug = build_cell_slug(index, &assignments);
        cells.push(MatrixCell {
            index,
            assignments,
            label,
            slug,
            placement,
            env_overrides,
        });
    }

    Ok(cells)
}

/// Whether a listed worker is connected (`state:` is not `disconnected`).
pub fn listed_as_connected(caps: &WorkerCapabilities) -> bool {
    tag_value(&caps.tags, STATE_TAG_PREFIX) != Some("disconnected")
}

/// True when `w` is connected and satisfies the cell's placement, task resources, and workspace
/// capability requirement.
pub fn worker_matches_cell(
    w: &WorkerCapabilities,
    placement: &Placement,
    reqs: &TaskRequirements,
    has_workspace: bool,
) -> bool {
    let req_vram = reqs.effective_vram_mb();
    listed_as_connected(w)
        && placement.matches_listed(w)
        && (!reqs.requires_gpu_or_vram() || w.can_execute_gpu())
        && w.cpu_cores >= reqs.cpu_cores
        && w.ram_mb >= reqs.ram_mb
        && (req_vram == 0 || w.vram_mb.is_some_and(|v| v >= req_vram))
        && (!has_workspace || listed_supports_workspaces(w))
}

/// Checks each cell against `workers`. Returns `None` for placeable cells and `Some(reason)` for
/// unplaced cells.
pub fn check_placeability(
    cells: &[MatrixCell],
    workers: &[WorkerCapabilities],
    reqs: &TaskRequirements,
    has_workspace: bool,
) -> Vec<Option<String>> {
    cells
        .iter()
        .map(|cell| {
            if workers
                .iter()
                .any(|w| worker_matches_cell(w, &cell.placement, reqs, has_workspace))
            {
                None
            } else if has_workspace
                && !workers
                    .iter()
                    .any(|w| listed_as_connected(w) && listed_supports_workspaces(w))
            {
                Some(format!(
                    "no connected worker supports workspaces for cell constraints ({})",
                    cell.placement
                ))
            } else if !workers
                .iter()
                .any(|w| listed_as_connected(w) && cell.placement.matches_listed(w))
            {
                Some(format!(
                    "no connected worker matches cell constraints ({})",
                    cell.placement
                ))
            } else {
                Some(format!(
                    "no connected worker matches cell constraints ({}) and task requirements",
                    cell.placement
                ))
            }
        })
        .collect()
}

/// Builds the `Task` for `cell` from `template`, keeping non-`require:` tags (e.g.
/// `opt:repeatable`) and injecting `OXIDESWARM_MATRIX_CELL`, `OXIDESWARM_MATRIX_INDEX`, and
/// `env:<VAR>` overrides for process tasks.
pub fn build_cell_task(template: &Task, cell: &MatrixCell) -> Task {
    let mut task = template.clone();
    task.id = TaskId::new();
    task.tags.retain(|t| !t.starts_with(REQUIRE_PREFIX));
    task.tags.extend(cell.placement.to_task_tags());
    match &mut task.spec {
        TaskSpec::Command { env, .. } | TaskSpec::ShellScript { env, .. } => {
            env.insert("OXIDESWARM_MATRIX_CELL".to_string(), cell.label.clone());
            env.insert(
                "OXIDESWARM_MATRIX_INDEX".to_string(),
                cell.index.to_string(),
            );
            for (k, v) in &cell.env_overrides {
                env.insert(k.clone(), v.clone());
            }
        }
        _ => {}
    }
    task
}

/// Lifecycle status of one matrix cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellStatus {
    Completed,
    Failed,
    Unplaced,
    TimedOut,
    Error,
    Cancelled,
}

impl CellStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Unplaced => "unplaced",
            Self::TimedOut => "timed_out",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Progress notification emitted by [`execute_matrix`].
#[derive(Debug, Clone)]
pub enum MatrixProgressEvent {
    Submitted {
        cell_index: usize,
        task_id: TaskId,
        label: String,
    },
    Finished {
        cell_index: usize,
        label: String,
        status: CellStatus,
    },
}

/// Raw execution outcome for one matrix cell.
#[derive(Debug, Clone)]
pub struct CellExecution {
    pub cell: MatrixCell,
    pub status: CellStatus,
    pub task_id: Option<TaskId>,
    pub worker_name: Option<String>,
    pub result: Option<TaskResult>,
    pub error: Option<String>,
    pub saved_outputs: Option<Vec<PathBuf>>,
    pub saved_artifacts: Option<Vec<PathBuf>>,
    pub download_error: Option<String>,
}

async fn send_best_effort_cancel<C, Fut>(connect: &C, task_id: TaskId)
where
    C: Fn() -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<MessageTransport<GridStream>, String>> + Send,
{
    let _ = tokio::time::timeout(Duration::from_millis(500), async {
        if let Ok(mut transport) = connect().await {
            let _ = transport
                .send_msg(&ClientMessage::CancelTask { task_id })
                .await;
            let _ = transport.recv_msg::<ClientResponse>().await;
            transport.close().await;
        }
    })
    .await;
}

fn cancelled_execution(cell: MatrixCell, task_id: TaskId) -> CellExecution {
    CellExecution {
        cell,
        status: CellStatus::Cancelled,
        task_id: Some(task_id),
        worker_name: None,
        result: None,
        error: Some(format!("Task {task_id} cancelled by client")),
        saved_outputs: None,
        saved_artifacts: None,
        download_error: None,
    }
}

async fn wait_for_cancel(cancel_rx: &mut Option<watch::Receiver<bool>>) {
    if let Some(rx) = cancel_rx.as_mut() {
        if *rx.borrow() {
            return;
        }
        while rx.changed().await.is_ok() {
            if *rx.borrow() {
                return;
            }
        }
    }
    std::future::pending::<()>().await
}

/// Executes all placeable cells concurrently via dedicated connections, returning one
/// [`CellExecution`] per cell in the original expansion order.
#[allow(clippy::too_many_arguments)]
pub async fn execute_matrix<C, Fut>(
    cells: Vec<MatrixCell>,
    workers: &[WorkerCapabilities],
    placeability: Vec<Option<String>>,
    template: &Task,
    timeout: Duration,
    cancel_rx: Option<watch::Receiver<bool>>,
    progress_tx: Option<mpsc::UnboundedSender<MatrixProgressEvent>>,
    connect: Arc<C>,
) -> Vec<CellExecution>
where
    C: Fn() -> Fut + Send + Sync + 'static,
    Fut:
        std::future::Future<Output = Result<MessageTransport<GridStream>, String>> + Send + 'static,
{
    let worker_names: Arc<BTreeMap<Uuid, String>> = Arc::new(
        workers
            .iter()
            .filter_map(|w| listed_worker_id(w).map(|id| (id, w.name.clone())))
            .collect(),
    );
    let total = cells.len();
    let mut outcomes: Vec<Option<CellExecution>> = vec![None; total];
    let handshake_sem = Arc::new(Semaphore::new(MAX_CONCURRENT_HANDSHAKES));
    let deadline = safe_deadline(timeout);
    let mut set = tokio::task::JoinSet::new();

    for (cell, unplaced_reason) in cells.into_iter().zip(placeability) {
        let idx = cell.index;
        if let Some(reason) = unplaced_reason {
            if let Some(ref tx) = progress_tx {
                let _ = tx.send(MatrixProgressEvent::Finished {
                    cell_index: idx,
                    label: cell.label.clone(),
                    status: CellStatus::Unplaced,
                });
            }
            outcomes[idx] = Some(CellExecution {
                cell,
                status: CellStatus::Unplaced,
                task_id: None,
                worker_name: None,
                result: None,
                error: Some(reason),
                saved_outputs: None,
                saved_artifacts: None,
                download_error: None,
            });
            continue;
        }

        let task = build_cell_task(template, &cell);
        let task_id = task.id;
        let connect = Arc::clone(&connect);
        let sem = Arc::clone(&handshake_sem);
        let names = Arc::clone(&worker_names);
        let mut cell_cancel = cancel_rx.clone();
        let tx = progress_tx.clone();

        set.spawn(async move {
            let label = cell.label.clone();
            if cell_cancel.as_ref().is_some_and(|rx| *rx.borrow()) {
                return cancelled_execution(cell, task_id);
            }

            let permit = tokio::select! {
                biased;
                () = wait_for_cancel(&mut cell_cancel) => {
                    return cancelled_execution(cell, task_id);
                }
                res = tokio::time::timeout_at(deadline, sem.acquire()) => match res {
                    Ok(Ok(p)) => p,
                    Ok(Err(_)) => {
                        return CellExecution {
                            cell,
                            status: CellStatus::Error,
                            task_id: Some(task_id),
                            worker_name: None,
                            result: None,
                            error: Some("internal handshake semaphore closed".to_string()),
                            saved_outputs: None,
                            saved_artifacts: None,
                            download_error: None,
                        };
                    }
                    Err(_) => {
                        return CellExecution {
                            cell,
                            status: CellStatus::TimedOut,
                            task_id: Some(task_id),
                            worker_name: None,
                            result: None,
                            error: Some(format!("timed out after {}s", timeout.as_secs())),
                            saved_outputs: None,
                            saved_artifacts: None,
                            download_error: None,
                        };
                    }
                }
            };

            let mut transport = tokio::select! {
                biased;
                () = wait_for_cancel(&mut cell_cancel) => {
                    drop(permit);
                    return cancelled_execution(cell, task_id);
                }
                res = tokio::time::timeout_at(deadline, connect()) => match res {
                    Ok(Ok(t)) => t,
                    Ok(Err(e)) => {
                        drop(permit);
                        return CellExecution {
                            cell,
                            status: CellStatus::Error,
                            task_id: Some(task_id),
                            worker_name: None,
                            result: None,
                            error: Some(e),
                            saved_outputs: None,
                            saved_artifacts: None,
                            download_error: None,
                        };
                    }
                    Err(_) => {
                        drop(permit);
                        return CellExecution {
                            cell,
                            status: CellStatus::TimedOut,
                            task_id: Some(task_id),
                            worker_name: None,
                            result: None,
                            error: Some(format!(
                                "timed out connecting to master after {}s",
                                timeout.as_secs()
                            )),
                            saved_outputs: None,
                            saved_artifacts: None,
                            download_error: None,
                        };
                    }
                }
            };
            drop(permit);

            if cell_cancel.as_ref().is_some_and(|rx| *rx.borrow()) {
                transport.close().await;
                return cancelled_execution(cell, task_id);
            }

            let msg = ClientMessage::SubmitTask { task, wait: true };
            let send_outcome = tokio::select! {
                biased;
                () = wait_for_cancel(&mut cell_cancel) => {
                    drop(transport);
                    send_best_effort_cancel(&*connect, task_id).await;
                    return cancelled_execution(cell, task_id);
                }
                res = tokio::time::timeout_at(deadline, transport.send_msg(&msg)) => match res {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(err)) => Err((CellStatus::Error, format!("could not submit cell: {err}"))),
                    Err(_) => Err((
                        CellStatus::TimedOut,
                        format!("timed out submitting cell after {}s", timeout.as_secs()),
                    )),
                }
            };
            if let Err((status, error)) = send_outcome {
                transport.close().await;
                return CellExecution {
                    cell,
                    status,
                    task_id: Some(task_id),
                    worker_name: None,
                    result: None,
                    error: Some(error),
                    saved_outputs: None,
                    saved_artifacts: None,
                    download_error: None,
                };
            }

            if let Some(ref tx) = tx {
                let _ = tx.send(MatrixProgressEvent::Submitted {
                    cell_index: idx,
                    task_id,
                    label: label.clone(),
                });
            }

            let mut cancel_closed = cell_cancel.is_none();
            let outcome = loop {
                if cell_cancel.as_ref().is_some_and(|rx| *rx.borrow()) {
                    drop(transport);
                    send_best_effort_cancel(&*connect, task_id).await;
                    break CellExecution {
                        cell,
                        status: CellStatus::Cancelled,
                        task_id: Some(task_id),
                        worker_name: None,
                        result: None,
                        error: Some(format!("Task {task_id} cancelled by client")),
                        saved_outputs: None,
                        saved_artifacts: None,
                        download_error: None,
                    };
                }
                tokio::select! {
                    biased;
                    res = async {
                        match cell_cancel.as_mut() {
                            Some(rx) => rx.changed().await,
                            None => std::future::pending().await,
                        }
                    }, if !cancel_closed => {
                        if res.is_err() {
                            cancel_closed = true;
                            continue;
                        }
                        if cell_cancel.as_ref().is_some_and(|rx| *rx.borrow()) {
                            drop(transport);
                            send_best_effort_cancel(&*connect, task_id).await;
                            break CellExecution {
                                cell,
                                status: CellStatus::Cancelled,
                                task_id: Some(task_id),
                                worker_name: None,
                                result: None,
                                error: Some(format!("Task {task_id} cancelled by client")),
                                saved_outputs: None,
                                saved_artifacts: None,
                                download_error: None,
                            };
                        }
                    }
                    res = transport.recv_msg::<ClientResponse>() => {
                        transport.close().await;
                        match res {
                            Ok(Some(ClientResponse::TaskCompleted { result, .. })) => {
                                let status = if result.is_success() {
                                    CellStatus::Completed
                                } else {
                                    CellStatus::Failed
                                };
                                let worker_name = names.get(&result.worker_id).cloned();
                                let error = result.error.clone();
                                break CellExecution {
                                    cell,
                                    status,
                                    task_id: Some(task_id),
                                    worker_name,
                                    result: Some(result),
                                    error,
                                    saved_outputs: None,
                                    saved_artifacts: None,
                                    download_error: None,
                                };
                            }
                            Ok(Some(ClientResponse::Error { message })) => {
                                break CellExecution {
                                    cell,
                                    status: CellStatus::Error,
                                    task_id: Some(task_id),
                                    worker_name: None,
                                    result: None,
                                    error: Some(message),
                                    saved_outputs: None,
                                    saved_artifacts: None,
                                    download_error: None,
                                };
                            }
                            Ok(Some(other)) => {
                                break CellExecution {
                                    cell,
                                    status: CellStatus::Error,
                                    task_id: Some(task_id),
                                    worker_name: None,
                                    result: None,
                                    error: Some(format!("unexpected master response: {other:?}")),
                                    saved_outputs: None,
                                    saved_artifacts: None,
                                    download_error: None,
                                };
                            }
                            Ok(None) => {
                                break CellExecution {
                                    cell,
                                    status: CellStatus::Error,
                                    task_id: Some(task_id),
                                    worker_name: None,
                                    result: None,
                                    error: Some("master closed the connection before replying".to_string()),
                                    saved_outputs: None,
                                    saved_artifacts: None,
                                    download_error: None,
                                };
                            }
                            Err(e) => {
                                break CellExecution {
                                    cell,
                                    status: CellStatus::Error,
                                    task_id: Some(task_id),
                                    worker_name: None,
                                    result: None,
                                    error: Some(format!("communication error: {e}")),
                                    saved_outputs: None,
                                    saved_artifacts: None,
                                    download_error: None,
                                };
                            }
                        }
                    }
                    _ = tokio::time::sleep_until(deadline) => {
                        transport.close().await;
                        break CellExecution {
                            cell,
                            status: CellStatus::TimedOut,
                            task_id: Some(task_id),
                            worker_name: None,
                            result: None,
                            error: Some(format!("timed out after {}s", timeout.as_secs())),
                            saved_outputs: None,
                            saved_artifacts: None,
                            download_error: None,
                        };
                    }
                }
            };

            if let Some(ref tx) = tx {
                let _ = tx.send(MatrixProgressEvent::Finished {
                    cell_index: idx,
                    label,
                    status: outcome.status,
                });
            }
            outcome
        });
    }

    drop(progress_tx);
    while let Some(joined) = set.join_next().await {
        if let Ok(exec) = joined {
            let idx = exec.cell.index;
            outcomes[idx] = Some(exec);
        }
    }

    outcomes
        .into_iter()
        .enumerate()
        .map(|(i, item)| item.unwrap_or_else(|| panic!("matrix cell {i} dropped without outcome")))
        .collect()
}

/// Structured per-cell report entry.
#[derive(Debug, Clone)]
pub struct CellRecord {
    pub index: usize,
    pub cell: String,
    pub slug: String,
    pub axes: Value,
    pub require_tags: Vec<String>,
    pub status: CellStatus,
    pub unplaced: bool,
    pub task_id: Option<String>,
    pub worker: Option<String>,
    pub worker_id: Option<String>,
    pub worker_name: Option<String>,
    pub exit_code: Option<i32>,
    pub duration_ms: u64,
    pub stdout: String,
    pub stderr: String,
    pub truncated: bool,
    pub worker_truncated: bool,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub stdout_captured_bytes: usize,
    pub stderr_captured_bytes: usize,
    pub captured_bytes: usize,
    pub error: Option<String>,
    pub outputs: Vec<Value>,
    pub saved_outputs: Option<Vec<String>>,
    pub saved_artifacts: Option<Vec<String>>,
}

impl CellRecord {
    pub fn to_json(&self) -> Value {
        let mut obj = json!({
            "index": self.index,
            "cell": self.cell,
            "label": self.cell,
            "slug": self.slug,
            "axes": self.axes,
            "require_tags": self.require_tags,
            "status": self.status.as_str(),
            "unplaced": self.unplaced,
            "task_id": self.task_id,
            "worker": self.worker,
            "worker_id": self.worker_id,
            "worker_name": self.worker_name,
            "exit_code": self.exit_code,
            "duration_ms": self.duration_ms,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "truncated": self.truncated,
            "worker_truncated": self.worker_truncated,
            "stdout_truncated": self.stdout_truncated,
            "stderr_truncated": self.stderr_truncated,
            "stdout_captured_bytes": self.stdout_captured_bytes,
            "stderr_captured_bytes": self.stderr_captured_bytes,
            "captured_bytes": self.captured_bytes,
            "error": self.error,
            "outputs": self.outputs,
        });
        if let Some(ref saved) = self.saved_outputs {
            obj["saved_outputs"] = json!(saved);
        }
        if let Some(ref saved) = self.saved_artifacts {
            obj["saved_artifacts"] = json!(saved);
        }
        obj
    }
}

/// Summary metrics for an executed matrix submission.
#[derive(Debug, Clone)]
pub struct MatrixSummary {
    pub total_cells: usize,
    pub completed_cells: usize,
    pub failed_cells: usize,
    pub unplaced_cells: usize,
    pub timed_out_cells: usize,
    pub error_cells: usize,
    pub cancelled_cells: usize,
    pub machines: usize,
    pub waited_ms: u64,
    pub task_time_sum_ms: u64,
}

/// Aggregated report for a matrix submission across CLI and MCP.
#[derive(Debug, Clone)]
pub struct MatrixReport {
    pub cells: Vec<CellRecord>,
    pub summary: MatrixSummary,
    pub input_upload: Option<UploadReport>,
    pub workspace: Option<Value>,
}

impl MatrixReport {
    /// Aggregates [`CellExecution`] outcomes into a bounded [`MatrixReport`].
    pub fn from_executions(
        executions: Vec<CellExecution>,
        waited_ms: u64,
        max_cell_output_bytes: usize,
        input_upload: Option<UploadReport>,
        workspace: Option<Value>,
    ) -> Self {
        let mut completed_cells = 0usize;
        let mut failed_cells = 0usize;
        let mut unplaced_cells = 0usize;
        let mut timed_out_cells = 0usize;
        let mut error_cells = 0usize;
        let mut cancelled_cells = 0usize;
        let mut results_for_summary = Vec::new();
        let mut records = Vec::with_capacity(executions.len());

        for exec in executions {
            let status = if exec.download_error.is_some() && exec.status == CellStatus::Completed {
                CellStatus::Error
            } else {
                exec.status
            };
            match status {
                CellStatus::Completed => completed_cells += 1,
                CellStatus::Failed => failed_cells += 1,
                CellStatus::Unplaced => unplaced_cells += 1,
                CellStatus::TimedOut => timed_out_cells += 1,
                CellStatus::Error => error_cells += 1,
                CellStatus::Cancelled => cancelled_cells += 1,
            }

            let axes = exec.cell.axes_json();
            let require_tags = exec.cell.placement.to_task_tags();
            let task_id = exec.task_id.map(|id| id.to_string());
            let saved_outputs_strs = exec.saved_outputs.as_ref().map(|paths| {
                paths
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
            });
            let saved_artifacts_strs = exec.saved_artifacts.as_ref().map(|paths| {
                paths
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
            });

            let record = if let Some(result) = exec.result {
                results_for_summary.push(result.clone());
                let worker_id_str = if result.worker_id.is_nil() {
                    None
                } else {
                    Some(result.worker_id.to_string())
                };
                let worker_display = exec.worker_name.clone().or_else(|| worker_id_str.clone());

                let stdout_raw = result.stdout.as_bytes();
                let stderr_raw = result.stderr.as_bytes();
                let stdout_worker_truncated = stream_was_worker_truncated(stdout_raw);
                let stderr_worker_truncated = stream_was_worker_truncated(stderr_raw);
                let worker_truncated = stdout_worker_truncated || stderr_worker_truncated;

                let stdout_need = escaped_str_bytes(&String::from_utf8_lossy(stdout_raw));
                let stderr_need = escaped_str_bytes(&String::from_utf8_lossy(stderr_raw));
                let (stdout_budget, stderr_budget) =
                    split_stream_budgets(stdout_need, stderr_need, max_cell_output_bytes);
                let (stdout, stdout_mcp_truncated, stdout_captured_bytes) =
                    truncate_stream_for_llm(stdout_raw, stdout_budget);
                let (stderr, stderr_mcp_truncated, stderr_captured_bytes) =
                    truncate_stream_for_llm(stderr_raw, stderr_budget);
                let stdout_truncated = stdout_mcp_truncated || stdout_worker_truncated;
                let stderr_truncated = stderr_mcp_truncated || stderr_worker_truncated;

                let mut outputs = Vec::new();
                for (i, output) in result.outputs.iter().enumerate() {
                    let mut entry = json!({
                        "path": output.path,
                        "size_bytes": output.size,
                        "blob_id": output.id.to_hex(),
                        "checksum_blake3": output.id.to_hex(),
                        "executable": output.executable,
                    });
                    if let Some(saved) = saved_outputs_strs.as_ref().and_then(|v| v.get(i)) {
                        entry["saved_path"] = Value::String(saved.clone());
                    }
                    outputs.push(entry);
                }
                for (i, artifact) in result.artifacts.iter().enumerate() {
                    let mut entry = json!({
                        "path": artifact.path,
                        "size_bytes": artifact.size,
                        "checksum_sha256": artifact.sha256_hex(),
                    });
                    if let Some(saved) = saved_artifacts_strs.as_ref().and_then(|v| v.get(i)) {
                        entry["saved_path"] = Value::String(saved.clone());
                    }
                    outputs.push(entry);
                }

                let error = exec.download_error.or(exec.error).or(result.error);

                CellRecord {
                    index: exec.cell.index,
                    cell: exec.cell.label,
                    slug: exec.cell.slug,
                    axes,
                    require_tags,
                    status,
                    unplaced: false,
                    task_id,
                    worker: worker_display,
                    worker_id: worker_id_str,
                    worker_name: exec.worker_name,
                    exit_code: Some(result.exit_code),
                    duration_ms: result.execution_time_ms,
                    stdout,
                    stderr,
                    truncated: stdout_truncated || stderr_truncated,
                    worker_truncated,
                    stdout_truncated,
                    stderr_truncated,
                    stdout_captured_bytes,
                    stderr_captured_bytes,
                    captured_bytes: stdout_captured_bytes + stderr_captured_bytes,
                    error,
                    outputs,
                    saved_outputs: saved_outputs_strs,
                    saved_artifacts: saved_artifacts_strs,
                }
            } else {
                CellRecord {
                    index: exec.cell.index,
                    cell: exec.cell.label,
                    slug: exec.cell.slug,
                    axes,
                    require_tags,
                    status,
                    unplaced: status == CellStatus::Unplaced,
                    task_id,
                    worker: None,
                    worker_id: None,
                    worker_name: None,
                    exit_code: None,
                    duration_ms: 0,
                    stdout: String::new(),
                    stderr: String::new(),
                    truncated: false,
                    worker_truncated: false,
                    stdout_truncated: false,
                    stderr_truncated: false,
                    stdout_captured_bytes: 0,
                    stderr_captured_bytes: 0,
                    captured_bytes: 0,
                    error: exec.download_error.or(exec.error),
                    outputs: Vec::new(),
                    saved_outputs: saved_outputs_strs,
                    saved_artifacts: saved_artifacts_strs,
                }
            };
            records.push(record);
        }

        let run_summary = RunSummary::from_results(&results_for_summary, waited_ms);
        let summary = MatrixSummary {
            total_cells: records.len(),
            completed_cells,
            failed_cells,
            unplaced_cells,
            timed_out_cells,
            error_cells,
            cancelled_cells,
            machines: run_summary.machines,
            waited_ms: run_summary.waited_ms,
            task_time_sum_ms: run_summary.task_time_sum_ms,
        };

        Self {
            cells: records,
            summary,
            input_upload,
            workspace,
        }
    }

    /// True if any cell did not complete with exit code 0.
    pub fn any_failed(&self) -> bool {
        self.summary.completed_cells < self.summary.total_cells
    }

    /// Serializes the full matrix report to JSON for CLI `--json` and MCP `submit`.
    pub fn to_json(&self) -> Value {
        let mut obj = Map::new();
        obj.insert("matrix".to_string(), Value::Bool(true));
        obj.insert(
            "status".to_string(),
            Value::String(if self.any_failed() {
                "failed".to_string()
            } else {
                "completed".to_string()
            }),
        );
        obj.insert("total_cells".to_string(), json!(self.summary.total_cells));
        obj.insert(
            "completed_cells".to_string(),
            json!(self.summary.completed_cells),
        );
        obj.insert("failed_cells".to_string(), json!(self.summary.failed_cells));
        obj.insert(
            "unplaced_cells".to_string(),
            json!(self.summary.unplaced_cells),
        );
        obj.insert(
            "timed_out_cells".to_string(),
            json!(self.summary.timed_out_cells),
        );
        obj.insert("error_cells".to_string(), json!(self.summary.error_cells));
        obj.insert(
            "cancelled_cells".to_string(),
            json!(self.summary.cancelled_cells),
        );
        obj.insert("machines".to_string(), json!(self.summary.machines));
        obj.insert("waited_ms".to_string(), json!(self.summary.waited_ms));
        obj.insert(
            "task_time_sum_ms".to_string(),
            json!(self.summary.task_time_sum_ms),
        );
        obj.insert(
            "cells".to_string(),
            Value::Array(self.cells.iter().map(CellRecord::to_json).collect()),
        );
        if let Some(ref upload) = self.input_upload {
            obj.insert("input_upload".to_string(), json!(upload));
        }
        if let Some(ref ws) = self.workspace {
            obj.insert("workspace".to_string(), ws.clone());
        }
        Value::Object(obj)
    }

    /// Formats a human-readable summary for CLI `oxideswarm submit --matrix`.
    pub fn text(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!(
            "Matrix: {} completed, {} failed, {} unplaced ({} total)",
            self.summary.completed_cells,
            self.summary.failed_cells
                + self.summary.timed_out_cells
                + self.summary.error_cells
                + self.summary.cancelled_cells,
            self.summary.unplaced_cells,
            self.summary.total_cells
        ));
        for c in &self.cells {
            let mut detail = format!("  [{}] {} -> {}", c.index, c.cell, c.status.as_str());
            if let Some(ref w) = c.worker {
                detail.push_str(&format!(" (worker {w}"));
                if let Some(code) = c.exit_code {
                    detail.push_str(&format!(", exit {code}"));
                }
                detail.push_str(&format!(", {}ms)", c.duration_ms));
            } else if let Some(ref err) = c.error {
                detail.push_str(&format!(": {err}"));
            }
            lines.push(detail);
            if !c.stdout.trim().is_empty() {
                lines.push(format!("      stdout: {}", c.stdout.trim()));
            }
            if !c.stderr.trim().is_empty() {
                lines.push(format!("      stderr: {}", c.stderr.trim()));
            }
            if c.worker.is_some() {
                if let Some(ref err) = c.error {
                    lines.push(format!("      error: {err}"));
                }
            }
        }
        let non_completed = self.summary.total_cells - self.summary.completed_cells;
        lines.push(format!(
            "Tasks: {} finished, {} failed, on {} machines",
            self.summary.completed_cells, non_completed, self.summary.machines
        ));
        lines.push(format!(
            "Waited: {:.3} s    Sum of task times: {:.3} s",
            self.summary.waited_ms as f64 / 1000.0,
            self.summary.task_time_sum_ms as f64 / 1000.0
        ));
        lines.join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::placement::FEAT_WORKSPACES_TAG;
    use rusty_grid_core::task::Bytes;

    fn worker(name: &str, tags: &[&str], supports_ws: bool) -> WorkerCapabilities {
        let mut all_tags: Vec<String> = tags.iter().map(|s| (*s).to_string()).collect();
        all_tags.push(format!("id:{}", Uuid::new_v4()));
        all_tags.push("state:idle".to_string());
        if supports_ws {
            all_tags.push(FEAT_WORKSPACES_TAG.to_string());
        }
        WorkerCapabilities {
            name: name.to_string(),
            cpu_cores: 4,
            ram_mb: 8192,
            has_gpu: false,
            is_simulated_gpu: false,
            gpu_device_name: None,
            tags: all_tags,
            mobile: None,
            has_blender: false,
            blender_version: None,
            vram_mb: None,
            has_cuda: false,
        }
    }

    #[test]
    fn parse_1d_and_2d_axes_and_normalize() {
        let spec = MatrixSpec::parse(
            &[
                "os=mac,Linux,win".to_string(),
                "env:MODE=fast,slow".to_string(),
            ],
            DEFAULT_MAX_MATRIX_CELLS,
        )
        .expect("parse 2D spec");
        assert_eq!(spec.cell_count(), 6);
        assert_eq!(spec.axes[0].values, vec!["macos", "linux", "windows"]);
        assert_eq!(spec.axes[1].values, vec!["fast", "slow"]);
    }

    #[test]
    fn allow_repeated_tag_and_tool_axes_but_reject_duplicate_singular_axes() {
        let spec = MatrixSpec::parse(
            &["tag=row-1,row-2".to_string(), "tag=col-1,col-2".to_string()],
            DEFAULT_MAX_MATRIX_CELLS,
        )
        .expect("repeated tag axes allowed");
        let cells = expand_cells(&spec, &Placement::default(), true).expect("expand 2D tags");
        assert_eq!(cells.len(), 4);
        assert_eq!(cells[0].label, "tag=row-1,tag=col-1");
        assert_eq!(cells[0].placement.tags, vec!["col-1", "row-1"]);
        assert_eq!(cells[3].label, "tag=row-2,tag=col-2");
        assert_eq!(cells[3].placement.tags, vec!["col-2", "row-2"]);
        assert_eq!(cells[0].axes_json(), json!({"tag": ["row-1", "col-1"]}));

        let dup_os = MatrixSpec::parse(
            &["os=macos".to_string(), "os=linux".to_string()],
            DEFAULT_MAX_MATRIX_CELLS,
        );
        assert!(dup_os.unwrap_err().contains("duplicate matrix axis 'os'"));

        let dup_env = MatrixSpec::parse(
            &["env:MODE=a".to_string(), "env:MODE=b".to_string()],
            DEFAULT_MAX_MATRIX_CELLS,
        );
        assert!(dup_env
            .unwrap_err()
            .contains("duplicate matrix axis 'env:MODE'"));
    }

    #[test]
    fn reject_invalid_specs_and_cell_cap() {
        assert!(MatrixSpec::parse(&[], 64).is_err());
        assert!(MatrixSpec::parse(&["not-an-axis".to_string()], 64).is_err());
        assert!(MatrixSpec::parse(&["=a,b".to_string()], 64).is_err());
        assert!(MatrixSpec::parse(&["tag=".to_string()], 64).is_err());
        assert!(MatrixSpec::parse(&["tag=a,,b".to_string()], 64).is_err());
        assert!(MatrixSpec::parse(&["tag=a,".to_string()], 64).is_err());
        assert!(MatrixSpec::parse(&["os=mac,macos".to_string()], 64).is_err());
        assert!(MatrixSpec::parse(&["unknown=a".to_string()], 64).is_err());
        assert!(MatrixSpec::parse(&["iso=bogus".to_string()], 64).is_err());
        assert!(MatrixSpec::parse(&["tag=os:linux".to_string()], 64).is_err());
        assert!(MatrixSpec::parse(&["tool=bad:tool".to_string()], 64).is_err());
        assert!(MatrixSpec::parse(&["tag=a".to_string()], 0).is_err());
        assert!(MatrixSpec::parse(&["tag=a".to_string()], HARD_MAX_MATRIX_CELLS + 1).is_err());

        let vals = (0..9)
            .map(|i| format!("t{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let over = MatrixSpec::parse(&[format!("tag={vals}"), format!("env:X={vals}")], 64);
        assert!(over.unwrap_err().contains("exceeding the limit of 64"));
    }

    #[test]
    fn parse_mcp_array_and_object_forms() {
        let arr_spec = MatrixSpec::parse_mcp_value(&json!(["tag=a,b", "tool=cargo"]), 64)
            .expect("array matrix");
        assert_eq!(arr_spec.cell_count(), 2);

        let obj_spec =
            MatrixSpec::parse_mcp_value(&json!({"tag": ["a", "b"], "env:MODE": ["x", "y"]}), 64)
                .expect("object matrix");
        assert_eq!(obj_spec.cell_count(), 4);

        assert!(MatrixSpec::parse_mcp_value(&json!([]), 64).is_err());
        assert!(MatrixSpec::parse_mcp_value(&json!({}), 64).is_err());
        assert!(MatrixSpec::parse_mcp_value(&json!("not-valid"), 64).is_err());
        assert!(MatrixSpec::parse_mcp_value(&json!({"os": ["macos,linux"]}), 64).is_err());
    }

    #[test]
    fn expand_rejects_base_placement_conflicts_and_non_process_env() {
        let spec = MatrixSpec::parse(&["os=macos,linux".to_string()], 64).unwrap();
        let base = Placement {
            os: Some("macos".to_string()),
            ..Placement::default()
        };
        assert!(expand_cells(&spec, &base, true)
            .unwrap_err()
            .contains("explicit --os"));

        let env_spec = MatrixSpec::parse(&["env:SEED=1,2".to_string()], 64).unwrap();
        assert!(expand_cells(&env_spec, &Placement::default(), false)
            .unwrap_err()
            .contains("only supported for command and shell"));
    }

    #[test]
    fn check_placeability_and_build_cell_task() {
        let w_a = worker("w-a", &["os:macos", "arch:aarch64", "cell-a"], true);
        let mut w_off = worker("w-b", &["os:macos", "arch:aarch64", "cell-b"], false);
        w_off.tags.retain(|t| t != "state:idle");
        w_off.tags.push("state:disconnected".to_string());

        let spec = MatrixSpec::parse(&["tag=cell-a,cell-b,cell-missing".to_string()], 64).unwrap();
        let cells = expand_cells(&spec, &Placement::default(), true).unwrap();
        let reqs = TaskRequirements::new(1, 0, false, 60);

        let res = check_placeability(&cells, &[w_a, w_off], &reqs, true);
        assert_eq!(res[0], None);
        assert!(res[1].is_some());
        assert!(res[2].is_some());

        let mut tpl = Task::new(
            TaskSpec::Command {
                program: "echo".to_string(),
                args: vec![],
                env: HashMap::from([("KEEP".to_string(), "1".to_string())]),
                working_dir: None,
                stdin: None,
            },
            reqs,
        );
        tpl.tags.push("opt:repeatable".to_string());
        tpl.tags.push("require:tag=old".to_string());

        let cell_task = build_cell_task(&tpl, &cells[0]);
        assert_ne!(cell_task.id, tpl.id);
        assert!(cell_task.tags.contains(&"opt:repeatable".to_string()));
        assert!(!cell_task.tags.contains(&"require:tag=old".to_string()));
        assert!(cell_task.tags.contains(&"require:tag=cell-a".to_string()));
        if let TaskSpec::Command { env, .. } = cell_task.spec {
            assert_eq!(env.get("KEEP").map(String::as_str), Some("1"));
            assert_eq!(
                env.get("OXIDESWARM_MATRIX_CELL").map(String::as_str),
                Some("tag=cell-a")
            );
            assert_eq!(
                env.get("OXIDESWARM_MATRIX_INDEX").map(String::as_str),
                Some("0")
            );
        } else {
            panic!("expected command spec");
        }
    }

    #[test]
    fn report_aggregates_completed_failed_and_unplaced() {
        let spec = MatrixSpec::parse(&["tag=a,b,c".to_string()], 64).unwrap();
        let cells = expand_cells(&spec, &Placement::default(), true).unwrap();
        let w1 = Uuid::new_v4();
        let execs = vec![
            CellExecution {
                cell: cells[0].clone(),
                status: CellStatus::Completed,
                task_id: Some(TaskId::new()),
                worker_name: Some("w1".to_string()),
                result: Some(TaskResult {
                    task_id: TaskId::new(),
                    worker_id: w1,
                    exit_code: 0,
                    stdout: Bytes::from("ok\n".as_bytes().to_vec()),
                    stderr: Bytes::new(),
                    execution_time_ms: 25,
                    is_gpu_executed: false,
                    device_name: None,
                    error: None,
                    asset_bytes: None,
                    artifacts: Vec::new(),
                    outputs: Vec::new(),
                }),
                error: None,
                saved_outputs: None,
                saved_artifacts: None,
                download_error: None,
            },
            CellExecution {
                cell: cells[1].clone(),
                status: CellStatus::Failed,
                task_id: Some(TaskId::new()),
                worker_name: Some("w1".to_string()),
                result: Some(TaskResult {
                    task_id: TaskId::new(),
                    worker_id: w1,
                    exit_code: 7,
                    stdout: Bytes::new(),
                    stderr: Bytes::from("boom".as_bytes().to_vec()),
                    execution_time_ms: 10,
                    is_gpu_executed: false,
                    device_name: None,
                    error: Some("exited 7".to_string()),
                    asset_bytes: None,
                    artifacts: Vec::new(),
                    outputs: Vec::new(),
                }),
                error: None,
                saved_outputs: None,
                saved_artifacts: None,
                download_error: None,
            },
            CellExecution {
                cell: cells[2].clone(),
                status: CellStatus::Unplaced,
                task_id: None,
                worker_name: None,
                result: None,
                error: Some("no connected worker matches cell constraints".to_string()),
                saved_outputs: None,
                saved_artifacts: None,
                download_error: None,
            },
        ];
        let report = MatrixReport::from_executions(execs, 50, 1024, None, None);
        assert!(report.any_failed());
        assert_eq!(report.summary.total_cells, 3);
        assert_eq!(report.summary.completed_cells, 1);
        assert_eq!(report.summary.failed_cells, 1);
        assert_eq!(report.summary.unplaced_cells, 1);
        assert_eq!(report.summary.machines, 1);
        assert_eq!(report.summary.task_time_sum_ms, 35);
        let j = report.to_json();
        assert_eq!(j["matrix"], true);
        assert_eq!(j["status"], "failed");
        assert_eq!(j["cells"][2]["unplaced"], true);
    }

    #[test]
    fn reject_delimiters_in_values_and_iso_off_consistently() {
        for bad in ["env:BAD=KEY=1", "tag=a=b", "iso=off"] {
            assert!(
                MatrixSpec::parse(&[bad.to_string()], 64).is_err(),
                "CLI parse should reject {bad}"
            );
            assert!(
                MatrixSpec::parse_mcp_value(&json!([bad]), 64).is_err(),
                "MCP array parse should reject {bad}"
            );
        }
        assert!(MatrixSpec::parse_mcp_value(&json!({"env:BAD": ["KEY=1"]}), 64).is_err());
        assert!(MatrixSpec::parse_mcp_value(&json!({"env:BAD=KEY": ["1"]}), 64).is_err());
        assert!(MatrixSpec::parse_mcp_value(&json!({"tag": ["a=b"]}), 64).is_err());
        assert!(MatrixSpec::parse_mcp_value(&json!({"iso": ["off"]}), 64).is_err());

        let valid_iso = MatrixSpec::parse(&["iso=fs,fs+net".to_string()], 64).unwrap();
        assert_eq!(valid_iso.axes[0].values, vec!["fs", "fs+net"]);
    }

    #[tokio::test]
    async fn execute_matrix_with_max_duration_does_not_panic() {
        let _ = safe_deadline(Duration::MAX);
        let _ = safe_deadline(Duration::from_secs(u64::MAX));

        let w = worker("w-1", &["cell-a"], false);
        let spec = MatrixSpec::parse(&["tag=cell-a,cell-missing".to_string()], 64).unwrap();
        let cells = expand_cells(&spec, &Placement::default(), true).unwrap();
        let reqs = TaskRequirements::new(1, 0, false, u64::MAX);
        let placeability = check_placeability(&cells, std::slice::from_ref(&w), &reqs, false);
        let tpl = Task::new(
            TaskSpec::Command {
                program: "echo".to_string(),
                args: vec![],
                env: HashMap::new(),
                working_dir: None,
                stdin: None,
            },
            reqs,
        );
        let connect = Arc::new(|| async { Err("simulated connect failure".to_string()) });
        let execs = execute_matrix(
            cells,
            &[w],
            placeability,
            &tpl,
            Duration::from_secs(u64::MAX),
            None,
            None,
            connect,
        )
        .await;
        assert_eq!(execs.len(), 2);
        assert_eq!(execs[0].status, CellStatus::Error);
        assert_eq!(execs[1].status, CellStatus::Unplaced);
    }

    #[tokio::test]
    async fn execute_matrix_cancels_during_slow_connect_before_submit() {
        let w = worker("w-1", &["cell-a"], false);
        let spec = MatrixSpec::parse(&["tag=cell-a".to_string()], 64).unwrap();
        let cells = expand_cells(&spec, &Placement::default(), true).unwrap();
        let reqs = TaskRequirements::new(1, 0, false, 60);
        let placeability = check_placeability(&cells, std::slice::from_ref(&w), &reqs, false);
        let tpl = Task::new(
            TaskSpec::Command {
                program: "echo".to_string(),
                args: vec![],
                env: HashMap::new(),
                working_dir: None,
                stdin: None,
            },
            reqs,
        );
        let (tx, rx) = watch::channel(false);
        let connect = Arc::new(|| async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Err("should have been cancelled before connect finished".to_string())
        });
        let cancel_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let _ = tx.send(true);
        });
        let execs = tokio::time::timeout(
            Duration::from_secs(2),
            execute_matrix(
                cells,
                &[w],
                placeability,
                &tpl,
                Duration::from_secs(60),
                Some(rx),
                None,
                connect,
            ),
        )
        .await
        .expect("cancellation should interrupt slow connect promptly");
        let _ = cancel_task.await;
        assert_eq!(execs.len(), 1);
        assert_eq!(execs[0].status, CellStatus::Cancelled);
    }

    use std::collections::HashMap;
}
