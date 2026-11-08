//! Shared workflow-mode identifiers, descriptors, result schema, and runner hook.
//!
//! The workflow-mode execution engine lives in `rusty_grid_cli::mode_cli` (`ox-mode`);
//! `rusty_grid_core::mode` contains only the pure data types, static descriptors, and
//! the process-local runner hook used by the Master Web UI (`/api/modes`).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use chrono::Utc;
use serde::{Deserialize, Serialize};

/// Supported top-level workflow modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowMode {
    /// Testing, linting, quality assurance, flaky test detection.
    Test,
    /// Fast build checks, local cluster management, health probes.
    Dev,
    /// Source doc generation, markdown specification verification, specification summary export.
    Doc,
    /// System profiling, micro-benchmarks, and synthetic scheduling simulation.
    Research,
}

impl WorkflowMode {
    /// Returns the canonical string identifier for the mode.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Test => "test",
            Self::Dev => "dev",
            Self::Doc => "doc",
            Self::Research => "research",
        }
    }

    /// Loose string parser supporting aliases (e.g. "bench" -> Research, "spec" -> Doc, "verify" -> Test).
    pub fn from_str_loose(s: &str) -> Option<Self> {
        let normalized = s.trim().to_lowercase();
        match normalized.as_str() {
            "test" | "tests" | "check-tests" | "verify" | "qa" => Some(Self::Test),
            "dev" | "develop" | "development" | "cluster" => Some(Self::Dev),
            "doc" | "docs" | "document" | "documentation" | "spec" | "specs" => Some(Self::Doc),
            "research" | "bench" | "benchmark" | "benchmarks" | "profile" | "perf" => {
                Some(Self::Research)
            }
            _ => None,
        }
    }

    /// All available workflow modes.
    pub fn all() -> &'static [WorkflowMode] {
        &[Self::Test, Self::Dev, Self::Doc, Self::Research]
    }

    /// Display title for user interfaces.
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Test => "🧪 Testing & Quality Verification",
            Self::Dev => "⚡ Development & Local Cluster",
            Self::Doc => "📚 Documentation & Architecture Spec",
            Self::Research => "🔬 Research & Performance Benchmarking",
        }
    }

    /// Description of the mode's capabilities.
    pub fn description(&self) -> &'static str {
        match self {
            Self::Test => {
                "Run workspace unit tests, lint checks (clippy/fmt), and sequential test passes."
            }
            Self::Dev => {
                "Fast incremental check, local dev cluster launcher (CLI), and HTTP health probe."
            }
            Self::Doc => {
                "Build rustdoc, verify markdown files and relative links, and export workspace spec summary."
            }
            Self::Research => {
                "Host resource profiling, in-memory wire/serde micro-benchmarks, and synthetic scheduling simulation."
            }
        }
    }

    /// List of standard actions available under this mode.
    pub fn available_actions(&self) -> &'static [&'static str] {
        match self {
            Self::Test => &["quick", "full", "lint", "flaky"],
            Self::Dev => &["fast", "watch", "cluster", "status", "stop"],
            Self::Doc => &["build", "verify", "export", "update"],
            Self::Research => &["profile", "bench", "distributed", "report"],
        }
    }
}

/// Unified execution summary and telemetry for any mode action.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModeExecutionResult {
    pub mode: WorkflowMode,
    pub action: String,
    pub success: bool,
    pub duration_ms: u64,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub metrics: HashMap<String, serde_json::Value>,
    pub timestamp: u64,
}

impl ModeExecutionResult {
    pub fn new(mode: WorkflowMode, action: impl Into<String>) -> Self {
        Self {
            mode,
            action: action.into(),
            success: false,
            duration_ms: 0,
            exit_code: None,
            stdout: String::new(),
            stderr: String::new(),
            metrics: HashMap::new(),
            timestamp: Utc::now().timestamp() as u64,
        }
    }

    pub fn with_success(mut self, success: bool) -> Self {
        self.success = success;
        self
    }

    pub fn with_output(mut self, stdout: impl Into<String>, stderr: impl Into<String>) -> Self {
        self.stdout = stdout.into();
        self.stderr = stderr.into();
        self
    }

    pub fn with_metric<T: Serialize>(mut self, key: &str, value: T) -> Self {
        if let Ok(v) = serde_json::to_value(value) {
            self.metrics.insert(key.to_string(), v);
        }
        self
    }
}

/// Metadata descriptor for exposing available modes via Web API and CLI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModeDescriptor {
    pub id: String,
    pub name: String,
    pub description: String,
    pub actions: Vec<ModeActionDescriptor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModeActionDescriptor {
    pub id: String,
    pub name: String,
    pub description: String,
    pub dangerous: bool,
}

/// Returns the descriptors of all supported modes and their actions.
pub fn get_mode_descriptors() -> Vec<ModeDescriptor> {
    WorkflowMode::all()
        .iter()
        .map(|m| match m {
            WorkflowMode::Test => ModeDescriptor {
                id: m.as_str().to_string(),
                name: m.display_name().to_string(),
                description: m.description().to_string(),
                actions: vec![
                    ModeActionDescriptor {
                        id: "quick".to_string(),
                        name: "Quick Unit Tests".to_string(),
                        description: "Run workspace library unit tests".to_string(),
                        dangerous: false,
                    },
                    ModeActionDescriptor {
                        id: "lint".to_string(),
                        name: "Lints & Format Check".to_string(),
                        description: "Run clippy and rustfmt checks across workspace".to_string(),
                        dangerous: false,
                    },
                    ModeActionDescriptor {
                        id: "flaky".to_string(),
                        name: "Flaky Test Detector".to_string(),
                        description: "Run library test suite 5 times sequentially".to_string(),
                        dangerous: false,
                    },
                    ModeActionDescriptor {
                        id: "full".to_string(),
                        name: "Full Test Suite".to_string(),
                        description: "Execute workspace unit and integration tests".to_string(),
                        dangerous: false,
                    },
                ],
            },
            WorkflowMode::Dev => ModeDescriptor {
                id: m.as_str().to_string(),
                name: m.display_name().to_string(),
                description: m.description().to_string(),
                actions: vec![
                    ModeActionDescriptor {
                        id: "fast".to_string(),
                        name: "Fast Check".to_string(),
                        description: "Run incremental workspace type-check".to_string(),
                        dangerous: false,
                    },
                    ModeActionDescriptor {
                        id: "watch".to_string(),
                        name: "Live Reload (Watch)".to_string(),
                        description: "Monitor workspace source files and re-check on changes"
                            .to_string(),
                        dangerous: false,
                    },
                    ModeActionDescriptor {
                        id: "cluster".to_string(),
                        name: "Launch Dev Cluster (CLI only)".to_string(),
                        description: "Launch background Master and local Workers from ox-mode CLI"
                            .to_string(),
                        dangerous: false,
                    },
                    ModeActionDescriptor {
                        id: "status".to_string(),
                        name: "Cluster Health Status".to_string(),
                        description: "Probe local master HTTP /api/status endpoint".to_string(),
                        dangerous: false,
                    },
                    ModeActionDescriptor {
                        id: "stop".to_string(),
                        name: "Stop Dev Cluster (CLI only)".to_string(),
                        description: "Terminate background dev cluster processes from ox-mode CLI"
                            .to_string(),
                        dangerous: true,
                    },
                ],
            },
            WorkflowMode::Doc => ModeDescriptor {
                id: m.as_str().to_string(),
                name: m.display_name().to_string(),
                description: m.description().to_string(),
                actions: vec![
                    ModeActionDescriptor {
                        id: "build".to_string(),
                        name: "Build Rustdoc".to_string(),
                        description: "Compile rustdoc documentation for workspace crates"
                            .to_string(),
                        dangerous: false,
                    },
                    ModeActionDescriptor {
                        id: "verify".to_string(),
                        name: "Verify Specs & Docs".to_string(),
                        description: concat!(
                            "Inspect markdown files for headings, ",
                            "balanced code fences, and relative links"
                        )
                        .to_string(),
                        dangerous: false,
                    },
                    ModeActionDescriptor {
                        id: "export".to_string(),
                        name: "Export Workspace Spec Summary".to_string(),
                        description: "Generate workspace crate and wire-protocol constants summary"
                            .to_string(),
                        dangerous: false,
                    },
                ],
            },
            WorkflowMode::Research => ModeDescriptor {
                id: m.as_str().to_string(),
                name: m.display_name().to_string(),
                description: m.description().to_string(),
                actions: vec![
                    ModeActionDescriptor {
                        id: "profile".to_string(),
                        name: "Hardware Resource Profiler".to_string(),
                        description: "Capture host CPU, RAM, and OS telemetry".to_string(),
                        dangerous: false,
                    },
                    ModeActionDescriptor {
                        id: "bench".to_string(),
                        name: "Micro-Benchmarks".to_string(),
                        description:
                            "Measure Task JSON serde and Bincode wire-codec roundtrips in ops/sec"
                                .to_string(),
                        dangerous: false,
                    },
                    ModeActionDescriptor {
                        id: "distributed".to_string(),
                        name: "Synthetic Scheduling Simulation".to_string(),
                        description: concat!(
                            "Run in-memory synthetic task-scheduling simulation ",
                            "(not a live cluster)"
                        )
                        .to_string(),
                        dangerous: false,
                    },
                    ModeActionDescriptor {
                        id: "report".to_string(),
                        name: "Research Report".to_string(),
                        description: concat!(
                            "Compile host profile, micro-benchmarks, ",
                            "and synthetic simulation into JSON"
                        )
                        .to_string(),
                        dangerous: false,
                    },
                ],
            },
        })
        .collect()
}

/// Callback signature registered by `rusty_grid_cli` so the Master Web UI can invoke workflow modes.
pub type ModeRunnerHook = fn(WorkflowMode, &str, &Path) -> ModeExecutionResult;

static MODE_RUNNER_HOOK: OnceLock<ModeRunnerHook> = OnceLock::new();

/// Registers the process-wide workflow-mode execution hook (`rusty_grid_cli::mode_cli::execute_mode_action`).
pub fn set_mode_runner_hook(hook: ModeRunnerHook) {
    let _ = MODE_RUNNER_HOOK.set(hook);
}

/// Dispatches a mode action through the registered CLI hook, or returns a failure result if no runner is installed.
pub fn dispatch_mode_action(
    mode: WorkflowMode,
    action: &str,
    workspace_root: &Path,
) -> ModeExecutionResult {
    if let Some(hook) = MODE_RUNNER_HOOK.get() {
        hook(mode, action, workspace_root)
    } else {
        let mut res = ModeExecutionResult::new(mode, action);
        res.success = false;
        res.exit_code = Some(1);
        res.stderr = concat!(
            "Workflow mode runner is not registered in this process; ",
            "run via `ox-mode` or `oxideswarm`."
        )
        .to_string();
        res
    }
}

/// Helper to discover the workspace root directory reliably.
pub fn find_workspace_root() -> PathBuf {
    if let Ok(cargo_dir) = std::env::var("CARGO_MANIFEST_DIR") {
        let p = PathBuf::from(cargo_dir);
        if p.join("../../Cargo.toml").exists() {
            return p.join("../..");
        }
        if p.join("Cargo.toml").exists() && p.join("crates").exists() {
            return p;
        }
    }

    let mut cur = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    for _ in 0..10 {
        if cur.join("Cargo.toml").exists() && cur.join("crates").exists() {
            return cur;
        }
        if let Some(parent) = cur.parent() {
            cur = parent.to_path_buf();
        } else {
            break;
        }
    }
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workflow_mode_parsing_and_aliases() {
        assert_eq!(
            WorkflowMode::from_str_loose("test"),
            Some(WorkflowMode::Test)
        );
        assert_eq!(
            WorkflowMode::from_str_loose("verify"),
            Some(WorkflowMode::Test)
        );
        assert_eq!(WorkflowMode::from_str_loose("dev"), Some(WorkflowMode::Dev));
        assert_eq!(
            WorkflowMode::from_str_loose("cluster"),
            Some(WorkflowMode::Dev)
        );
        assert_eq!(WorkflowMode::from_str_loose("doc"), Some(WorkflowMode::Doc));
        assert_eq!(
            WorkflowMode::from_str_loose("spec"),
            Some(WorkflowMode::Doc)
        );
        assert_eq!(
            WorkflowMode::from_str_loose("bench"),
            Some(WorkflowMode::Research)
        );
        assert_eq!(
            WorkflowMode::from_str_loose("research"),
            Some(WorkflowMode::Research)
        );
        assert_eq!(WorkflowMode::from_str_loose("unknown_mode_xyz"), None);
    }

    #[test]
    fn test_mode_descriptors_coverage() {
        let descriptors = get_mode_descriptors();
        assert_eq!(descriptors.len(), 4);
        for d in descriptors {
            assert!(!d.actions.is_empty(), "Mode {} should have actions", d.id);
        }
    }

    #[test]
    fn test_dispatch_mode_action_without_hook_fails() {
        let res = dispatch_mode_action(WorkflowMode::Research, "profile", Path::new("."));
        assert!(
            !res.success,
            "dispatch_mode_action without a registered hook must fail"
        );
        assert_eq!(res.exit_code, Some(1));
        assert!(res.stderr.contains("not registered"));
    }
}
