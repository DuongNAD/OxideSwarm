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
pub mod store;
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

    fn strip_rust_comments(src: &str) -> String {
        let mut out = String::with_capacity(src.len());
        let bytes = src.as_bytes();
        let mut i = 0;
        let mut in_string = false;
        while i < bytes.len() {
            if in_string {
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    out.push(bytes[i] as char);
                    out.push(bytes[i + 1] as char);
                    i += 2;
                    continue;
                }
                if bytes[i] == b'"' {
                    in_string = false;
                }
                out.push(bytes[i] as char);
                i += 1;
                continue;
            }
            if bytes[i] == b'"' {
                in_string = true;
                out.push('"');
                i += 1;
                continue;
            }
            if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'/' {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    if bytes[i] == b'\n' {
                        out.push('\n');
                    }
                    i += 1;
                }
                i = (i + 2).min(bytes.len());
                continue;
            }
            out.push(bytes[i] as char);
            i += 1;
        }
        out
    }

    fn strip_ignored_functions(src: &str) -> String {
        let mut out = String::with_capacity(src.len());
        let mut rest = src;
        while let Some(ig_idx) = rest.find("#[ignore") {
            out.push_str(&rest[..ig_idx]);
            let after_ig = &rest[ig_idx..];
            let Some(fn_offset) = after_ig.find("fn ") else {
                break;
            };
            let after_fn = &after_ig[fn_offset..];
            let Some(brace_offset) = after_fn.find('{') else {
                break;
            };
            let body_start = &after_fn[brace_offset..];
            let mut depth = 0usize;
            let mut in_string = false;
            let bytes = body_start.as_bytes();
            let mut i = 0;
            while i < bytes.len() {
                if in_string {
                    if bytes[i] == b'\\' && i + 1 < bytes.len() {
                        i += 2;
                        continue;
                    }
                    if bytes[i] == b'"' {
                        in_string = false;
                    }
                    i += 1;
                    continue;
                }
                match bytes[i] {
                    b'"' => in_string = true,
                    b'{' => depth += 1,
                    b'}' => {
                        depth = depth.saturating_sub(1);
                        if depth == 0 {
                            i += 1;
                            break;
                        }
                    }
                    _ => {}
                }
                i += 1;
            }
            rest = &body_start[i..];
        }
        out.push_str(rest);
        out
    }

    fn extract_fn_body<'a>(src: &'a str, fn_header: &str) -> Option<&'a str> {
        let idx = src.find(fn_header)?;
        let after = &src[idx..];
        let brace_offset = after.find('{')?;
        let body_start = &after[brace_offset..];
        let bytes = body_start.as_bytes();
        let mut depth = 0usize;
        let mut i = 0usize;
        let mut in_string = false;
        while i < bytes.len() {
            let b = bytes[i];
            if in_string {
                if b == b'\\' {
                    i += 2;
                    continue;
                }
                if b == b'"' {
                    in_string = false;
                }
                i += 1;
                continue;
            }
            match b {
                b'"' => in_string = true,
                b'{' => depth += 1,
                b'}' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        return Some(&body_start[..=i]);
                    }
                }
                _ => {}
            }
            i += 1;
        }
        None
    }

    fn check_test_source_for_non_minimal_p2p(
        label: &str,
        raw_src: &str,
        violations: &mut Vec<String>,
    ) {
        let uncommented = strip_rust_comments(raw_src);
        let active = strip_ignored_functions(&uncommented);
        if active.contains("presets::N0") {
            violations.push(format!(
                "{label}: uses presets::N0 in non-ignored test code"
            ));
        }
        let compact: String = active.chars().filter(|c| !c.is_whitespace()).collect();
        let compact_without_contains = compact
            .replace(".contains(\"--p2p\")", "")
            .replace(".contains(\"--p2p-ticket\")", "");
        if (compact_without_contains.contains("\"--p2p\"")
            || compact_without_contains.contains("\"--p2p-ticket\""))
            && (active.contains("fn cli(") || active.contains("Command::new"))
        {
            let has_minimal_env = if let Some(cli_body) = extract_fn_body(&active, "fn cli(") {
                cli_body.contains(".env(\"RUSTY_GRID_P2P_MINIMAL\"")
            } else {
                active.contains(".env(\"RUSTY_GRID_P2P_MINIMAL\"")
            };
            if !has_minimal_env {
                violations.push(format!(
                    "{label}: spawns --p2p/--p2p-ticket binary without .env(\"RUSTY_GRID_P2P_MINIMAL\", ...)"
                ));
            }
            if compact.contains(".env_remove(\"RUSTY_GRID_P2P_MINIMAL\")") {
                violations.push(format!(
                    "{label}: removes RUSTY_GRID_P2P_MINIMAL in non-ignored --p2p/--p2p-ticket test"
                ));
            }
            if compact.contains(".env_clear()") {
                violations.push(format!(
                    "{label}: calls .env_clear() in non-ignored --p2p/--p2p-ticket test"
                ));
            }
            let env_prefix = ".env(\"RUSTY_GRID_P2P_MINIMAL\",";
            let mut search_rest = compact.as_str();
            while let Some(idx) = search_rest.find(env_prefix) {
                let after = &search_rest[idx + env_prefix.len()..];
                let val = after.split(')').next().unwrap_or("");
                let normalized = val.to_ascii_lowercase();
                if !matches!(
                    normalized.as_str(),
                    "\"1\"" | "\"true\"" | "\"yes\"" | "\"on\""
                ) {
                    violations.push(format!(
                        "{label}: sets RUSTY_GRID_P2P_MINIMAL to non-enabled value {val} in non-ignored --p2p/--p2p-ticket test"
                    ));
                }
                search_rest = after;
            }
        }
        for stmt in active.split(';') {
            if stmt.contains(".with_p2p(true)") && !stmt.contains(".with_p2p_minimal_preset(true)")
            {
                violations.push(format!(
                    "{label}: uses .with_p2p(true) without .with_p2p_minimal_preset(true)"
                ));
            }
            if stmt.contains(".with_p2p_ticket(")
                && !stmt.contains(".with_p2p_minimal_preset(true)")
            {
                violations.push(format!(
                    "{label}: uses .with_p2p_ticket(...) without .with_p2p_minimal_preset(true)"
                ));
            }
            if stmt.contains("start_worker_impl(")
                && stmt.contains("Some(")
                && !active.contains("set_p2p_minimal_preset_for_testing(true)")
            {
                violations.push(format!(
                    "{label}: uses start_worker_impl(..., Some(...), ...) without set_p2p_minimal_preset_for_testing(true)"
                ));
            }
        }
    }

    fn collect_rs_files(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_rs_files(&path, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }

    fn find_non_minimal_p2p_in_tests(workspace_root: &Path) -> Vec<String> {
        let mut violations = Vec::new();
        let mut test_files = Vec::new();
        collect_rs_files(&workspace_root.join("tests"), &mut test_files);
        if let Ok(crates) = std::fs::read_dir(workspace_root.join("crates")) {
            for entry in crates.flatten() {
                collect_rs_files(&entry.path().join("tests"), &mut test_files);
            }
        }
        test_files.sort();
        for path in test_files {
            if let Ok(src) = std::fs::read_to_string(&path) {
                let rel = path
                    .strip_prefix(workspace_root)
                    .unwrap_or(&path)
                    .display()
                    .to_string()
                    .replace('\\', "/");
                check_test_source_for_non_minimal_p2p(&rel, &src, &mut violations);
            }
        }

        let mut src_files = Vec::new();
        if let Ok(crates) = std::fs::read_dir(workspace_root.join("crates")) {
            for entry in crates.flatten() {
                collect_rs_files(&entry.path().join("src"), &mut src_files);
            }
        }
        src_files.sort();
        for path in src_files {
            let rel = path
                .strip_prefix(workspace_root)
                .unwrap_or(&path)
                .display()
                .to_string()
                .replace('\\', "/");
            if rel == "crates/master/src/lib.rs" {
                continue;
            }
            if let Ok(src) = std::fs::read_to_string(&path) {
                if let Some((_, test_tail)) = src.split_once("#[cfg(test)]") {
                    check_test_source_for_non_minimal_p2p(&rel, test_tail, &mut violations);
                }
            }
        }

        violations
    }

    #[test]
    fn test_non_ignored_tests_use_minimal_p2p_preset() {
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("workspace root");
        let violations = find_non_minimal_p2p_in_tests(workspace_root);
        assert!(
            violations.is_empty(),
            "non-ignored test code must use presets::Minimal and .with_p2p_minimal_preset(true): {violations:?}"
        );
    }

    #[test]
    fn test_find_non_minimal_p2p_in_tests_detects_missing_preset_and_allows_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let tests_dir = tmp.path().join("tests");
        std::fs::create_dir_all(&tests_dir).unwrap();

        std::fs::write(
            tests_dir.join("sample_p2p.rs"),
            r#"
#[tokio::test]
#[ignore = "live relay test"]
async fn ignored_live_relay() {
    let _ep = iroh::Endpoint::builder(iroh::endpoint::presets::N0).bind().await;
    let _cfg = ServerConfig::new(addr).with_p2p(true);
    let _w = WorkerConfig::new("auto").with_p2p_ticket("t");
}

#[tokio::test]
async fn good_offline_test() {
    let _cfg = ServerConfig::new(addr)
        .with_p2p(true)
        .with_p2p_minimal_preset(true);
    let _w = WorkerConfig::new("auto")
        .with_p2p_ticket("t")
        .with_p2p_minimal_preset(true);
}

#[tokio::test]
async fn bad_missing_minimal() {
    let _cfg = ServerConfig::new(addr).with_p2p(true);
    let _cmd = Command::new("rusty-grid").arg("master").arg("--p2p");
    let _started = start_worker_impl("127.0.0.1:0".into(), Some(ticket), "w".into(), 2, 1024, false, 2);
}
"#,
        )
        .unwrap();

        std::fs::write(
            tests_dir.join("sample_env_override.rs"),
            r#"
fn cli() -> Command {
    let mut c = Command::new("rusty-grid");
    c.env("RUSTY_GRID_P2P_MINIMAL", "1");
    c
}

#[test]
fn bad_env_remove_clear_and_false() {
    let mut c1 = cli();
    c1.env_remove("RUSTY_GRID_P2P_MINIMAL").args(["master", "--p2p"]);
    let mut c2 = cli();
    c2.env("RUSTY_GRID_P2P_MINIMAL", "0").args(["master", "--p2p"]);
    let mut c3 = cli();
    c3.env_clear().args(["master", "--p2p"]);
}
"#,
        )
        .unwrap();

        std::fs::write(
            tests_dir.join("sample_ticket_only.rs"),
            r#"
#[test]
fn bad_ticket_only_worker_spawn() {
    let _cmd = Command::new("rusty-grid").args(["worker", "--p2p-ticket", "ticket123"]);
}
"#,
        )
        .unwrap();

        let violations = find_non_minimal_p2p_in_tests(tmp.path());
        assert_eq!(
            violations,
            vec![
                "tests/sample_env_override.rs: removes RUSTY_GRID_P2P_MINIMAL in non-ignored --p2p/--p2p-ticket test"
                    .to_string(),
                "tests/sample_env_override.rs: calls .env_clear() in non-ignored --p2p/--p2p-ticket test"
                    .to_string(),
                "tests/sample_env_override.rs: sets RUSTY_GRID_P2P_MINIMAL to non-enabled value \"0\" in non-ignored --p2p/--p2p-ticket test"
                    .to_string(),
                "tests/sample_p2p.rs: spawns --p2p/--p2p-ticket binary without .env(\"RUSTY_GRID_P2P_MINIMAL\", ...)"
                    .to_string(),
                "tests/sample_p2p.rs: uses .with_p2p(true) without .with_p2p_minimal_preset(true)"
                    .to_string(),
                "tests/sample_p2p.rs: uses start_worker_impl(..., Some(...), ...) without set_p2p_minimal_preset_for_testing(true)"
                    .to_string(),
                "tests/sample_ticket_only.rs: spawns --p2p/--p2p-ticket binary without .env(\"RUSTY_GRID_P2P_MINIMAL\", ...)"
                    .to_string(),
            ]
        );
    }
}
