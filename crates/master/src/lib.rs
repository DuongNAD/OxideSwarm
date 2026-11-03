//! Master node server, worker registry, task queue, scheduler, and heartbeat reaper for `rusty_grid`.

pub mod auth;
pub mod blob_store;
#[cfg(feature = "dashboard")]
pub mod dashboard;
pub mod liveness;
pub mod mapreduce;
pub mod queue;
pub mod reaper;
pub mod registry;
pub mod scheduler;
pub mod server;
#[cfg(feature = "dashboard")]
pub mod web_ui;

pub use auth::{
    dashboard_admin_token, resolve_dashboard_auth, ApiToken, AuthConfig, AuthIdentity, AuthScope,
    DevBypass,
};

pub use blob_store::{
    BlobStore, BlobStoreConfig, BlobStoreStats, OutputPinGuard, DEFAULT_BLOB_STORE_MAX_BYTES,
    DEFAULT_OUTPUT_PIN_RETENTION,
};
pub use liveness::{RxClock, StampedStream};
pub use mapreduce::MapReduceEngine;
pub use queue::{
    is_worker_rejection, AssignedLoad, CancelOutcome, DelayedRetryKey, QueueOrderKey, QueueStats,
    ResultDisposition, ResultOutcome, RetryAttempt, RetryPolicy, TaskEntry, TaskInfo, TaskQueue,
    TaskState, TaskView, DEFAULT_MAX_ACTIVE_BYTES, DEFAULT_MAX_TERMINAL_BYTES,
    DEFAULT_MAX_TERMINAL_TASKS,
};
pub use reaper::{spawn_reaper, spawn_reaper_with_queue, ReaperConfig};
pub use registry::{
    HeartbeatOutcome, Registration, WorkerEntry, WorkerInfo, WorkerRegistry, WorkerStatus,
};
pub use scheduler::{
    ScheduleAssignment, ScheduleReport, ScheduleSkipReason, SchedulerConfig, SchedulingPolicy,
    WorkloadScheduler,
};
pub use server::{
    handle_task_progress, handle_task_result, handle_worker_disconnect, remove_port_file,
    write_port_file, MasterHandle, MasterServer, ServerConfig, WaiterMap,
};

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod manifest_tests {
    use std::collections::BTreeSet;
    use std::path::Path;

    fn find_unregistered_root_tests(workspace_root: &Path) -> Vec<String> {
        let tests_dir = workspace_root.join("tests");
        let mut test_files = BTreeSet::new();
        for entry in std::fs::read_dir(&tests_dir).expect("read workspace tests/ directory") {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    test_files.insert(name.to_string());
                }
            }
        }

        let mut registered = BTreeSet::new();
        let crates_dir = workspace_root.join("crates");
        for entry in std::fs::read_dir(&crates_dir).expect("read workspace crates/ directory") {
            let entry = entry.expect("crate dir entry");
            let manifest = entry.path().join("Cargo.toml");
            if !manifest.is_file() {
                continue;
            }
            let content = std::fs::read_to_string(&manifest).expect("read Cargo.toml");
            for line in content.lines() {
                let trimmed = line.trim();
                if let Some(rest) = trimmed.strip_prefix("path") {
                    let rest = rest.trim_start();
                    if let Some(rest) = rest.strip_prefix('=') {
                        let value = rest.trim().trim_matches('"').trim_matches('\'');
                        if let Some(file_name) = value.strip_prefix("../../tests/") {
                            registered.insert(file_name.to_string());
                        }
                    }
                }
            }
        }

        test_files.difference(&registered).cloned().collect()
    }

    #[test]
    fn test_every_root_integration_test_has_a_cargo_toml_test_entry() {
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("workspace root");
        let orphans = find_unregistered_root_tests(workspace_root);
        assert!(
            orphans.is_empty(),
            "tests/*.rs files without a [[test]] entry in crates/*/Cargo.toml: {orphans:?}"
        );
    }

    #[test]
    fn test_find_unregistered_root_tests_detects_orphan_test_file() {
        let tmp = tempfile::tempdir().unwrap();
        let tests_dir = tmp.path().join("tests");
        let crate_dir = tmp.path().join("crates").join("sample");
        std::fs::create_dir_all(&tests_dir).unwrap();
        std::fs::create_dir_all(&crate_dir).unwrap();

        std::fs::write(tests_dir.join("registered_e2e.rs"), "#[test] fn ok() {}\n").unwrap();
        std::fs::write(tests_dir.join("orphan_e2e.rs"), "#[test] fn orphan() {}\n").unwrap();
        std::fs::write(
            crate_dir.join("Cargo.toml"),
            r#"[package]
name = "sample"
version = "0.1.0"

[[test]]
name = "registered_e2e"
path = "../../tests/registered_e2e.rs"
"#,
        )
        .unwrap();

        let orphans = find_unregistered_root_tests(tmp.path());
        assert_eq!(orphans, vec!["orphan_e2e.rs".to_string()]);
    }

    fn strip_comments_and_docstrings(src: &str) -> String {
        let mut without_docstrings = String::with_capacity(src.len());
        let mut rest = src;
        while let Some(start) = rest.find("\"\"\"") {
            without_docstrings.push_str(&rest[..start]);
            let after_open = &rest[start + 3..];
            if let Some(end) = after_open.find("\"\"\"") {
                rest = &after_open[end + 3..];
            } else {
                rest = "";
                break;
            }
        }
        without_docstrings.push_str(rest);

        let mut out = String::with_capacity(without_docstrings.len());
        for line in without_docstrings.lines() {
            let trimmed = line.trim_start();
            if trimmed.starts_with('#') || trimmed.starts_with("//") {
                continue;
            }
            let code_part = line
                .split_once(" #")
                .map(|(code, _)| code)
                .or_else(|| line.split_once(" //").map(|(code, _)| code))
                .unwrap_or(line);
            out.push_str(code_part);
            out.push('\n');
        }
        out
    }

    fn tracked_tests_filenames(workspace_root: &Path) -> Option<BTreeSet<String>> {
        if !workspace_root.join(".git").exists() {
            return None;
        }
        let output = std::process::Command::new("git")
            .args(["ls-files", "tests"])
            .current_dir(workspace_root)
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let stdout = String::from_utf8(output.stdout).ok()?;
        let mut names = BTreeSet::new();
        for line in stdout.lines() {
            let path = Path::new(line.trim());
            if path.parent() == Some(Path::new("tests")) {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    names.insert(name.to_string());
                }
            }
        }
        Some(names)
    }

    fn find_orphan_non_rust_tests(workspace_root: &Path) -> Vec<String> {
        let tests_dir = workspace_root.join("tests");
        let tracked = tracked_tests_filenames(workspace_root);
        let mut non_rust_files = BTreeSet::new();
        for entry in std::fs::read_dir(&tests_dir).expect("read workspace tests/ directory") {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
            if !matches!(ext, "py" | "sh" | "ps1" | "cmd" | "vbs") {
                continue;
            }
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with('.') {
                    continue;
                }
                if let Some(ref tracked_set) = tracked {
                    if !tracked_set.contains(name) {
                        continue;
                    }
                }
                non_rust_files.insert(name.to_string());
            }
        }

        const RUNNER_FILES: &[(&str, Option<&str>)] = &[
            (".github/workflows/ci.yml", None),
            (
                "tests/run_mesh_simulation_tests.py",
                Some("run_mesh_simulation_tests.py"),
            ),
            (
                "tests/test_agent_mesh_rust_hub_cross_interop.py",
                Some("test_agent_mesh_rust_hub_cross_interop.py"),
            ),
            (
                "tests/test_ecosystem_convergence.rs",
                Some("test_ecosystem_convergence.rs"),
            ),
        ];

        let mut referenced = BTreeSet::new();
        for (rel_path, self_name) in RUNNER_FILES {
            let full_path = workspace_root.join(rel_path);
            let Ok(raw_content) = std::fs::read_to_string(&full_path) else {
                continue;
            };
            let content = strip_comments_and_docstrings(&raw_content);
            for candidate in &non_rust_files {
                if *self_name == Some(candidate.as_str()) {
                    continue;
                }
                let stem = candidate.strip_suffix(".py");
                let named_by_file = content.contains(candidate.as_str());
                let named_by_import = stem.is_some_and(|s| {
                    content.contains(&format!("from {s} import"))
                        || content.contains(&format!("from tests.{s} import"))
                        || content.contains(&format!("import {s}"))
                });
                if named_by_file || named_by_import {
                    referenced.insert(candidate.clone());
                }
            }
        }

        non_rust_files.difference(&referenced).cloned().collect()
    }

    #[test]
    fn test_every_non_rust_file_in_tests_is_named_by_a_runner() {
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("workspace root");
        let orphans = find_orphan_non_rust_tests(workspace_root);
        assert!(
            orphans.is_empty(),
            "non-Rust files in tests/ not named by any runner: {orphans:?}"
        );
    }

    #[test]
    fn test_find_orphan_non_rust_tests_detects_unreferenced_scripts() {
        let tmp = tempfile::tempdir().unwrap();
        let tests_dir = tmp.path().join("tests");
        let workflows_dir = tmp.path().join(".github").join("workflows");
        std::fs::create_dir_all(&tests_dir).unwrap();
        std::fs::create_dir_all(&workflows_dir).unwrap();

        std::fs::write(
            workflows_dir.join("ci.yml"),
            "steps:\n  # comment mentioning verify_p2p_persistence.sh\n  - run: python tests/run_mesh_simulation_tests.py\n",
        )
        .unwrap();
        std::fs::write(
            tests_dir.join("run_mesh_simulation_tests.py"),
            "\"\"\"Docstring mentioning test_m7_empirical_challenge.py\"\"\"\nfrom test_agent_mesh_simulation import run_simulation_tests\n",
        )
        .unwrap();
        std::fs::write(
            tests_dir.join("test_agent_mesh_simulation.py"),
            "def run_simulation_tests(): pass\n",
        )
        .unwrap();
        std::fs::write(
            tests_dir.join("test_m7_empirical_challenge.py"),
            "# File: tests/test_m7_empirical_challenge.py\n",
        )
        .unwrap();
        std::fs::write(
            tests_dir.join("verify_p2p_persistence.sh"),
            "#!/usr/bin/env bash\n",
        )
        .unwrap();
        std::fs::write(tests_dir.join("m7_challenger_2_results.json"), "{}\n").unwrap();

        let orphans = find_orphan_non_rust_tests(tmp.path());
        assert_eq!(
            orphans,
            vec![
                "test_m7_empirical_challenge.py".to_string(),
                "verify_p2p_persistence.sh".to_string(),
            ]
        );
    }
}
