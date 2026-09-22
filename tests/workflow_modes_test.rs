//! Integration tests for OxideSwarm Workflow Modes & Presets.

use rusty_grid_core::mode::{
    execute_mode_action, export_specification, find_workspace_root, get_mode_descriptors,
    run_hardware_profile, run_micro_benchmarks, verify_documentation, WorkflowMode,
};

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

    // 1. Verify docs
    let verify_res = verify_documentation(&root);
    assert!(
        verify_res.success,
        "Doc verification should pass: {:?}",
        verify_res.stderr
    );
    assert!(verify_res
        .stdout
        .contains("Documentation & Specification Verification"));
    assert!(verify_res.metrics.contains_key("total_docs"));
    assert_eq!(verify_res.metrics["total_docs"], 5);

    // 2. Export spec
    let export_res = export_specification(&root);
    assert!(export_res.success, "Spec export should pass");
    let target_spec = root.join("target/docs/OXIDESWARM_SPECIFICATION.md");
    assert!(target_spec.exists(), "Exported spec file should exist");
}

#[test]
fn test_research_mode_profile_and_bench() {
    // 1. Hardware profile
    let profile = run_hardware_profile();
    assert!(profile.success);
    assert!(profile.metrics.contains_key("cpu_cores"));
    assert!(profile.metrics.contains_key("ram_total_mb"));
    assert!(profile.metrics["cpu_cores"].as_u64().unwrap_or(0) > 0);

    // 2. Micro benchmarks
    let bench = run_micro_benchmarks();
    assert!(bench.success);
    assert!(bench.metrics.contains_key("serde_ops_per_sec"));
    assert!(bench.metrics.contains_key("codec_ops_per_sec"));
    let serde_ops = bench.metrics["serde_ops_per_sec"].as_u64().unwrap_or(0);
    assert!(
        serde_ops > 1000,
        "Serde ops/sec should be > 1000, got: {}",
        serde_ops
    );
}

#[test]
fn test_research_mode_distributed_and_report() {
    let root = find_workspace_root();

    // 1. Distributed simulation benchmark
    let dist_res = rusty_grid_core::mode::run_distributed_benchmark(&root);
    assert!(dist_res.success, "Distributed benchmark should succeed");
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
    let report_res = rusty_grid_core::mode::generate_research_report(&root);
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
}

#[test]
fn test_dev_mode_watch_and_aliases() {
    let root = find_workspace_root();

    // 1. Test watch check
    let watch_res = rusty_grid_core::mode::run_watch_check(&root);
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
fn test_unknown_action_rejection() {
    let root = find_workspace_root();
    let res = execute_mode_action(WorkflowMode::Test, "non_existent_action", &root);
    assert!(!res.success);
    assert!(res.stderr.contains("Unknown action"));
}

#[test]
fn test_cli_binary_mode_subcommand() {
    let root = find_workspace_root();
    let ox_mode_bin = root.join("target/debug/ox-mode");
    if !ox_mode_bin.exists() {
        return;
    }

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

#[test]
fn test_bash_script_invocation() {
    let root = find_workspace_root();
    let script = root.join("scripts/mode.sh");
    if !script.exists() {
        return;
    }

    // Run bash script doc verify --json
    let output = std::process::Command::new("bash")
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
