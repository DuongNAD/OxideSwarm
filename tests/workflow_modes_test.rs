//! Integration tests for OxideSwarm Workflow Modes & Presets (`MODE-1`).

use rusty_grid_cli::mode::{
    execute_mode_action, export_specification, find_workspace_root, get_mode_descriptors,
    probe_cluster_health_with_root, run_distributed_benchmark, run_hardware_profile,
    run_micro_benchmarks, run_mode_cli, run_watch_check, verify_documentation, ModeCliArgs,
    WorkflowMode,
};
use rusty_grid_core::protocol::MAX_FRAME_SIZE;
use rusty_grid_master::auth::{AuthConfig, AuthScope};
use rusty_grid_master::server::{MasterServer, ServerConfig};

#[test]
fn test_workflow_modes_enum_and_descriptors() {
    let modes = WorkflowMode::all();
    assert_eq!(modes.len(), 4);

    let descriptors = get_mode_descriptors();
    assert_eq!(descriptors.len(), 4);

    for desc in descriptors {
        assert!(!desc.id.is_empty());
        assert!(!desc.name.is_empty());
        assert!(!desc.description.is_empty());
        assert!(!desc.actions.is_empty());
        for act in desc.actions {
            assert!(!act.id.is_empty());
            assert!(!act.name.is_empty());
            assert!(!act.description.is_empty());
        }
    }
}

#[test]
fn test_workflow_modes_aliases() {
    assert_eq!(
        WorkflowMode::from_str_loose("test"),
        Some(WorkflowMode::Test)
    );
    assert_eq!(
        WorkflowMode::from_str_loose("tests"),
        Some(WorkflowMode::Test)
    );
    assert_eq!(
        WorkflowMode::from_str_loose("verify"),
        Some(WorkflowMode::Test)
    );
    assert_eq!(WorkflowMode::from_str_loose("qa"), Some(WorkflowMode::Test));

    assert_eq!(WorkflowMode::from_str_loose("dev"), Some(WorkflowMode::Dev));
    assert_eq!(
        WorkflowMode::from_str_loose("develop"),
        Some(WorkflowMode::Dev)
    );
    assert_eq!(
        WorkflowMode::from_str_loose("cluster"),
        Some(WorkflowMode::Dev)
    );

    assert_eq!(WorkflowMode::from_str_loose("doc"), Some(WorkflowMode::Doc));
    assert_eq!(
        WorkflowMode::from_str_loose("docs"),
        Some(WorkflowMode::Doc)
    );
    assert_eq!(
        WorkflowMode::from_str_loose("spec"),
        Some(WorkflowMode::Doc)
    );

    assert_eq!(
        WorkflowMode::from_str_loose("research"),
        Some(WorkflowMode::Research)
    );
    assert_eq!(
        WorkflowMode::from_str_loose("bench"),
        Some(WorkflowMode::Research)
    );
    assert_eq!(
        WorkflowMode::from_str_loose("profile"),
        Some(WorkflowMode::Research)
    );

    assert_eq!(WorkflowMode::from_str_loose("unknown_invalid_xyz"), None);
}

#[test]
fn test_doc_mode_verify_and_export() {
    let root = find_workspace_root();
    assert!(
        root.join("Cargo.toml").exists(),
        "Root must have Cargo.toml"
    );

    // 1. Verify docs on real workspace
    let verify_res = verify_documentation(&root);
    assert!(
        verify_res.success,
        "Doc verification should pass: {:?}\n{}",
        verify_res.stderr, verify_res.stdout
    );
    assert!(verify_res
        .stdout
        .contains("Documentation & Specification Verification"));
    assert_eq!(verify_res.metrics["total_docs"], 5);
    assert!(
        verify_res.metrics["links_checked"].as_u64().unwrap_or(0) > 0,
        "verify_documentation must check relative links in markdown files"
    );
    assert_eq!(verify_res.metrics["broken_links"], 0);

    // 2. Verify broken relative link is detected and fails
    let temp = tempfile::tempdir().expect("tempdir");
    for doc_name in [
        "README.md",
        "CLAUDE.md",
        "PROJECT.md",
        "DEPLOYMENT.md",
        "ANDROID_ARCHITECTURE.md",
    ] {
        std::fs::write(
            temp.path().join(doc_name),
            format!("# {doc_name}\n\nValid text.\n"),
        )
        .expect("write doc");
    }
    std::fs::write(
        temp.path().join("README.md"),
        "# README\n\nSee [missing file](docs/does_not_exist.md) for details.\n",
    )
    .expect("write broken link README");
    let broken_res = verify_documentation(temp.path());
    assert!(
        !broken_res.success,
        "verify_documentation must fail when a relative link target is missing"
    );
    assert_eq!(broken_res.exit_code, Some(1));
    assert_eq!(broken_res.metrics["broken_links"], 1);
    assert!(broken_res.stderr.contains("does_not_exist.md"));

    // 3. Export spec and verify generated facts match real protocol constants
    let export_res = export_specification(&root);
    assert!(export_res.success, "Spec export should pass");
    let target_spec = root.join("target/docs/OXIDESWARM_SPECIFICATION.md");
    assert!(target_spec.exists(), "Exported spec file should exist");
    let spec_text = std::fs::read_to_string(&target_spec).expect("read exported spec");
    assert!(
        spec_text.contains(&MAX_FRAME_SIZE.to_string()),
        "Exported spec must report real MAX_FRAME_SIZE ({MAX_FRAME_SIZE})"
    );
    assert!(
        !spec_text.contains("16,777,216") && !spec_text.contains("max 16MB"),
        "Exported spec must not contain false 16 MB frame limit claims"
    );
    assert!(
        !spec_text.contains("[Payload: JSON]"),
        "Exported spec must not claim JSON-only wire framing"
    );
}

#[test]
fn test_research_mode_profile_and_bench() {
    // 1. Hardware profile
    let profile = run_hardware_profile();
    assert!(profile.success);
    assert!(profile.metrics.contains_key("cpu_cores"));
    assert!(profile.metrics.contains_key("ram_total_mb"));
    assert!(profile.metrics["cpu_cores"].as_u64().unwrap_or(0) > 0);

    // 2. Micro benchmarks (real Task JSON serde + Bincode wire codec roundtrip)
    let bench = run_micro_benchmarks();
    assert!(bench.success);
    assert!(bench.metrics.contains_key("serde_ops_per_sec"));
    assert!(bench.metrics.contains_key("codec_ops_per_sec"));
    assert!(bench.stdout.contains("Bincode Wire Codec Roundtrip"));
    let serde_ops = bench.metrics["serde_ops_per_sec"].as_u64().unwrap_or(0);
    assert!(
        serde_ops > 100,
        "Serde ops/sec should be > 100, got: {}",
        serde_ops
    );
    let codec_ops = bench.metrics["codec_ops_per_sec"].as_u64().unwrap_or(0);
    assert!(
        codec_ops > 100,
        "Codec ops/sec should be > 100, got: {}",
        codec_ops
    );
}

#[test]
fn test_research_mode_distributed_and_report() {
    let root = find_workspace_root();

    // 1. Distributed synthetic simulation benchmark
    let dist_res = run_distributed_benchmark(&root);
    assert!(dist_res.success, "Distributed benchmark should succeed");
    assert_eq!(dist_res.metrics["simulation_only"], true);
    assert!(dist_res.metrics.contains_key("throughput_tasks_per_sec"));
    assert!(dist_res.metrics.contains_key("latency_p50_us"));
    assert!(dist_res.metrics.contains_key("latency_p90_us"));
    assert!(dist_res.metrics.contains_key("latency_p99_us"));
    assert!(dist_res.metrics.contains_key("worker_allocations"));

    let throughput = dist_res.metrics["throughput_tasks_per_sec"]
        .as_f64()
        .unwrap_or(0.0);
    assert!(
        throughput > 0.0,
        "Throughput should be positive, got: {}",
        throughput
    );

    // 2. Full research report aggregation
    let report_res = rusty_grid_cli::mode::generate_research_report(&root);
    assert!(
        report_res.success,
        "Research report generation should succeed"
    );
    assert!(report_res.metrics.contains_key("hardware"));
    assert!(report_res.metrics.contains_key("micro_benchmarks"));
    assert!(report_res.metrics.contains_key("distributed_simulation"));

    let report_path = root.join("target/research_report.json");
    assert!(
        report_path.exists(),
        "target/research_report.json should exist"
    );
    let report_json = std::fs::read_to_string(&report_path).expect("read research_report.json");

    for text in [&dist_res.stdout, &report_res.stdout, &report_json] {
        assert!(!text.contains("OPTIMAL"), "output must not print OPTIMAL");
        assert!(
            !text.contains("RTX 4090"),
            "output must not print invented RTX 4090 hardware"
        );
        assert!(
            !text.contains("Snapdragon"),
            "output must not print invented Snapdragon hardware"
        );
        assert!(
            !text.contains("mac-orchestrator")
                && !text.contains("linux-worker-cpu")
                && !text.contains("android-mobile-edge"),
            "output must not print invented node names"
        );
    }
}

#[test]
fn test_dev_mode_watch_and_aliases() {
    let root = find_workspace_root();

    // 1. Test watch check
    let watch_res = run_watch_check(&root);
    assert!(watch_res.success, "Watch check should succeed");
    assert!(watch_res.metrics.contains_key("watchable_files"));
    assert!(watch_res.metrics.contains_key("watchable_dirs"));

    // 2. Test execute_mode_action with aliases
    let reload_res = execute_mode_action(WorkflowMode::Dev, "reload", &root);
    assert!(reload_res.success);

    let update_res = execute_mode_action(WorkflowMode::Doc, "update", &root);
    assert!(update_res.success);
}

#[test]
fn test_unknown_and_non_runnable_actions_fail() {
    let root = find_workspace_root();
    let res = execute_mode_action(WorkflowMode::Test, "non_existent_action", &root);
    assert!(!res.success);
    assert!(res.stderr.contains("Unknown action"));

    let cluster_res = execute_mode_action(WorkflowMode::Dev, "cluster", &root);
    assert!(
        !cluster_res.success,
        "dev cluster must fail in non-CLI execute_mode_action path"
    );
    assert_eq!(cluster_res.exit_code, Some(1));

    let stop_res = execute_mode_action(WorkflowMode::Dev, "stop", &root);
    assert!(
        !stop_res.success,
        "dev stop must fail in non-CLI execute_mode_action path"
    );
    assert_eq!(stop_res.exit_code, Some(1));
}

#[test]
fn test_core_has_no_cargo_invocation() {
    let root = find_workspace_root();
    let core_src_dir = root.join("crates/core/src");
    for entry in std::fs::read_dir(&core_src_dir).expect("read crates/core/src") {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            let content = std::fs::read_to_string(&path).expect("read rs file");
            assert!(
                !content.contains("Command::new(\"cargo\")"),
                "{} must not contain Command::new(\"cargo\")",
                path.display()
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dev_status_against_protected_master_exits_nonzero() {
    let auth_config = AuthConfig {
        enabled: true,
        dev_mode: false,
        loopback_bypass: false,
        tokens: Default::default(),
        allowed_origins: Vec::new(),
    }
    .with_token("secret-admin-token", "admin", &[AuthScope::Admin]);

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_auth_config(auth_config);
    let master = MasterServer::spawn(config)
        .await
        .expect("start protected master");
    let dash_port = master.dashboard_addr().expect("dashboard addr").port();

    let temp = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        temp.path().join(".dev_cluster.json"),
        serde_json::json!({ "web_ui_port": dash_port }).to_string(),
    )
    .expect("write .dev_cluster.json");

    // 1. Direct engine probe against protected master returns failure with HTTP 401
    let probe_res = probe_cluster_health_with_root(dash_port, temp.path());
    assert!(
        !probe_res.success,
        "dev status against protected master (401) must fail"
    );
    assert_eq!(probe_res.exit_code, Some(1));
    assert_eq!(probe_res.metrics["http_status"], 401);
    assert!(
        probe_res.stderr.contains("401"),
        "expected HTTP 401 in stderr, got: {}",
        probe_res.stderr
    );

    // 2. CLI runner exits non-zero against protected master
    let exit_code = run_mode_cli(ModeCliArgs {
        mode: Some("dev".to_string()),
        action: Some("status".to_string()),
        action_opt: None,
        iterations: 1,
        port: Some(dash_port),
        json: true,
        interactive: false,
    })
    .await;
    assert_ne!(
        exit_code, 0,
        "run_mode_cli dev status against protected master must return non-zero exit code"
    );

    // 3. Binary invocation (`ox-mode dev status --port <dash_port>`) exits non-zero
    let ox_mode_bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_ox-mode"));
    let out = std::process::Command::new(&ox_mode_bin)
        .args(["dev", "status", "--port", &dash_port.to_string(), "--json"])
        .output()
        .expect("spawn ox-mode dev status");
    assert!(
        !out.status.success(),
        "ox-mode dev status against protected master must exit non-zero"
    );

    let _ = master.shutdown();
}

#[test]
fn test_cli_binary_mode_subcommand() {
    let root = find_workspace_root();
    let ox_mode_bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_ox-mode"));

    let output = std::process::Command::new(&ox_mode_bin)
        .args(["doc", "verify", "--json"])
        .current_dir(&root)
        .output()
        .expect("Failed to execute ox-mode binary");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value =
        serde_json::from_str(&stdout).expect("Output should be valid JSON");
    assert_eq!(json["mode"], "doc");
    assert_eq!(json["action"], "verify");
    assert_eq!(json["success"], true);
}

fn resolve_functional_bash() -> Option<std::path::PathBuf> {
    #[cfg(windows)]
    let candidates = {
        let mut candidates = vec![
            std::path::PathBuf::from(r"C:\Program Files\Git\bin\bash.exe"),
            std::path::PathBuf::from(r"C:\Program Files\Git\usr\bin\bash.exe"),
            std::path::PathBuf::from(r"C:\Program Files (x86)\Git\bin\bash.exe"),
        ];
        if let Ok(local_app_data) = std::env::var("LOCALAPPDATA") {
            candidates
                .push(std::path::PathBuf::from(local_app_data).join(r"Programs\Git\bin\bash.exe"));
        }
        candidates.push(std::path::PathBuf::from("bash"));
        candidates
    };
    #[cfg(not(windows))]
    let candidates = vec![std::path::PathBuf::from("bash")];

    for cand in candidates {
        if let Ok(output) = std::process::Command::new(&cand)
            .args(["-c", "exit 0"])
            .output()
        {
            if output.status.success() {
                return Some(cand);
            }
        }
    }

    None
}

#[test]
fn test_bash_script_invocation() {
    let bash_bin = match resolve_functional_bash() {
        Some(bin) => bin,
        None => {
            eprintln!("Skipping test_bash_script_invocation: No functional bash interpreter found on host");
            return;
        }
    };

    let root = find_workspace_root();
    let script = root.join("scripts/mode.sh");
    if !script.exists() {
        return;
    }

    let output = std::process::Command::new(&bash_bin)
        .arg(&script)
        .args(["doc", "verify", "--json"])
        .current_dir(&root)
        .output()
        .expect("Failed to execute scripts/mode.sh");

    assert!(
        output.status.success(),
        "scripts/mode.sh doc verify should succeed: {:?}",
        output
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("Script output should be valid JSON");
    assert_eq!(json["mode"], "doc");
    assert_eq!(json["action"], "verify");
    assert_eq!(json["success"], true);
}
