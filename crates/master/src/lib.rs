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
}
