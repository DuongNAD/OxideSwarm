//! Command-line interface, interactive menu, and execution engine for OxideSwarm Workflow Modes (`ox-mode`).

use std::io::{self, Write};
use std::path::Path;

use clap::Parser;
use serde::{Deserialize, Serialize};

use rusty_grid_core::cluster_auth::ClusterSecret;
pub use rusty_grid_core::mode::{
    find_workspace_root, get_mode_descriptors, ModeActionDescriptor, ModeDescriptor,
    ModeExecutionResult, WorkflowMode,
};

mod dev;
// LV-4: cfg(unix) libc::killpg / signal calls for local dev cluster teardown.
#[allow(unsafe_code)]
mod dev_launch;
mod doc;
mod research;
mod test;

pub use dev::{
    launch_local_dev_cluster, launch_local_dev_cluster_with_ports, probe_cluster_health,
    probe_cluster_health_with_fallback_ports, probe_cluster_health_with_root,
    probe_cluster_health_with_secret, run_fast_check, run_watch_check, stop_local_dev_cluster,
    stop_local_dev_cluster_with_signaller, DEFAULT_FALLBACK_STATUS_PORTS,
};
pub use doc::{
    export_specification, finalize_doc_build_output, finalize_doc_build_output_with_target_dir,
    resolve_cargo_target_dir, run_doc_build, verify_documentation,
};
pub use research::{
    generate_research_report, run_distributed_benchmark, run_hardware_profile, run_micro_benchmarks,
};
pub use test::{
    evaluate_lint_outcomes, parse_cargo_test_counts, run_flaky_detection, run_flaky_detection_with,
    run_full_tests, run_lints, run_quick_tests,
};

use dev::{
    probe_cluster_health_with_bearer, resolve_dashboard_bearer_token, run_dev_menu,
    run_live_reload_watch,
};
use doc::run_doc_menu;
use research::run_research_menu;
use test::run_test_menu;

// ANSI terminal colors for CLI output
pub(super) const CLR_RESET: &str = "\x1b[0m";
pub(super) const CLR_BOLD: &str = "\x1b[1m";
pub(super) const CLR_CYAN: &str = "\x1b[36m";
pub(super) const CLR_GREEN: &str = "\x1b[32m";
pub(super) const CLR_YELLOW: &str = "\x1b[33m";
pub(super) const CLR_RED: &str = "\x1b[31m";
pub(super) const CLR_MAGENTA: &str = "\x1b[35m";
pub(super) const CLR_DIM: &str = "\x1b[2m";

#[derive(Parser, Debug, Clone, Serialize, Deserialize)]
#[command(
    name = "ox-mode",
    about = "OxideSwarm Workflow Modes & Presets: test, dev, doc, research",
    version
)]
pub struct ModeCliArgs {
    /// Workflow mode: test, dev, doc, research
    #[arg(index = 1)]
    pub mode: Option<String>,

    /// Mode action (e.g. quick, full, lint, flaky, fast, cluster, status, stop, build, verify, export, profile, bench, distributed, report)
    #[arg(index = 2)]
    pub action: Option<String>,

    /// Specific action override flag
    #[arg(long, short = 'a')]
    pub action_opt: Option<String>,

    /// Number of iterations for flaky test detection (default: 5)
    #[arg(long, default_value = "5")]
    pub iterations: usize,

    /// Optional dashboard HTTP port for `dev status`
    #[arg(long, env = "RUSTY_GRID_DASHBOARD_PORT")]
    pub port: Option<u16>,

    /// Format output as JSON
    #[arg(long)]
    pub json: bool,

    /// Open interactive menu
    #[arg(short = 'i', long)]
    pub interactive: bool,
}

// ==============================================================================
// 5. UNIFIED ENGINE DISPATCHER & CLI RUNNER
// ==============================================================================

/// Dispatches any workflow mode and action in the synchronous engine path.
///
/// Note: `dev cluster` and `dev stop` require process lifecycle management from the
/// `ox-mode` CLI (`execute_single_action`); calling them here (for example from the
/// Web UI dashboard) returns `success = false`.
pub fn execute_mode_action(
    mode: WorkflowMode,
    action: &str,
    workspace_root: &Path,
) -> ModeExecutionResult {
    let action_clean = action.trim().to_lowercase();
    match (mode, action_clean.as_str()) {
        // Test mode
        (WorkflowMode::Test, "quick") => run_quick_tests(workspace_root),
        (WorkflowMode::Test, "full") => run_full_tests(workspace_root),
        (WorkflowMode::Test, "lint") => run_lints(workspace_root),
        (WorkflowMode::Test, "size") => crate::size_check::run_size_check(workspace_root),
        (WorkflowMode::Test, "flaky") => run_flaky_detection(workspace_root, 5),

        // Dev mode
        (WorkflowMode::Dev, "fast") => run_fast_check(workspace_root),
        (WorkflowMode::Dev, "watch") | (WorkflowMode::Dev, "reload") => {
            run_watch_check(workspace_root)
        }
        (WorkflowMode::Dev, "status") => probe_cluster_health_with_root(None, workspace_root),
        (WorkflowMode::Dev, "cluster") => {
            let mut res = ModeExecutionResult::new(WorkflowMode::Dev, "cluster");
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = "Action 'dev cluster' cannot run in the non-CLI engine / dashboard path; run `ox-mode dev cluster` from the CLI.".to_string();
            res
        }
        (WorkflowMode::Dev, "stop") => {
            let mut res = ModeExecutionResult::new(WorkflowMode::Dev, "stop");
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = "Action 'dev stop' cannot run in the non-CLI engine / dashboard path; run `ox-mode dev stop` from the CLI.".to_string();
            res
        }

        // Doc mode
        (WorkflowMode::Doc, "build") => run_doc_build(workspace_root),
        (WorkflowMode::Doc, "verify") => verify_documentation(workspace_root),
        (WorkflowMode::Doc, "export") | (WorkflowMode::Doc, "update") => {
            export_specification(workspace_root)
        }

        // Research mode
        (WorkflowMode::Research, "profile") => run_hardware_profile(),
        (WorkflowMode::Research, "bench") => run_micro_benchmarks(),
        (WorkflowMode::Research, "distributed") => run_distributed_benchmark(workspace_root),
        (WorkflowMode::Research, "report") => generate_research_report(workspace_root),

        // Fallback for unknown action
        (m, act) => {
            let mut res = ModeExecutionResult::new(m, act);
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = format!(
                "Unknown action '{}' for mode '{:?}'. Available actions: {:?}",
                act,
                m,
                m.available_actions()
            );
            res
        }
    }
}

pub(super) fn extract_count(line: &str, keyword: &str) -> Option<usize> {
    if let Some(pos) = line.find(keyword) {
        let prefix = line[..pos].trim();
        if let Some(num_str) = prefix.split_whitespace().last() {
            return num_str.parse().ok();
        }
    }
    None
}

/// Executes the mode CLI workflow.
pub async fn run_mode_cli(args: ModeCliArgs) -> i32 {
    let workspace_root = find_workspace_root();
    let bearer_token = resolve_dashboard_bearer_token();
    run_mode_cli_with_bearer(args, &workspace_root, bearer_token.as_deref()).await
}

/// Executes the mode CLI workflow with an explicit workspace root and optional `ClusterSecret`.
pub async fn run_mode_cli_with_secret(
    args: ModeCliArgs,
    workspace_root: &Path,
    secret: Option<&ClusterSecret>,
) -> i32 {
    let bearer_token = secret.map(rusty_grid_master::dashboard_admin_token);
    run_mode_cli_with_bearer(args, workspace_root, bearer_token.as_deref()).await
}

async fn run_mode_cli_with_bearer(
    args: ModeCliArgs,
    workspace_root: &Path,
    bearer_token: Option<&str>,
) -> i32 {
    rusty_grid_core::mode::set_mode_runner_hook(execute_mode_action);

    if args.mode.is_none() || args.interactive {
        return run_interactive_menu(workspace_root).await;
    }

    let mode_str = args.mode.as_deref().unwrap_or("");
    let parsed_mode = match WorkflowMode::from_str_loose(mode_str) {
        Some(m) => m,
        None => {
            eprintln!(
                "{}Error: Unknown workflow mode '{}'. Available: test, dev, doc, research.{}",
                CLR_RED, mode_str, CLR_RESET
            );
            eprintln!("Run `ox-mode --help` or `ox-mode -i` for the interactive menu.");
            return 1;
        }
    };

    let action_str = args
        .action
        .or(args.action_opt)
        .unwrap_or_else(|| match parsed_mode {
            WorkflowMode::Test => "quick".to_string(),
            WorkflowMode::Dev => "fast".to_string(),
            WorkflowMode::Doc => "verify".to_string(),
            WorkflowMode::Research => "profile".to_string(),
        });

    let result =
        if parsed_mode == WorkflowMode::Dev && action_str.trim().eq_ignore_ascii_case("status") {
            probe_cluster_health_with_bearer(args.port, workspace_root, bearer_token)
        } else {
            execute_single_action(parsed_mode, &action_str, args.iterations, workspace_root).await
        };

    if args.json {
        if let Ok(json) = serde_json::to_string_pretty(&result) {
            println!("{}", json);
        }
    } else {
        render_execution_result(&result);
    }

    if result.success {
        0
    } else {
        result.exit_code.unwrap_or(1)
    }
}

/// Dispatches an action with CLI support for cluster launch/stop and live-reload.
pub async fn execute_single_action(
    mode: WorkflowMode,
    action: &str,
    iterations: usize,
    workspace_root: &Path,
) -> ModeExecutionResult {
    let action_clean = action.trim().to_lowercase();

    if mode == WorkflowMode::Test && action_clean == "flaky" {
        return run_flaky_detection(workspace_root, iterations);
    }

    if mode == WorkflowMode::Dev && (action_clean == "watch" || action_clean == "reload") {
        return run_live_reload_watch(workspace_root).await;
    }

    if mode == WorkflowMode::Dev && action_clean == "cluster" {
        return launch_local_dev_cluster(workspace_root).await;
    }

    if mode == WorkflowMode::Dev && action_clean == "stop" {
        return stop_local_dev_cluster(workspace_root).await;
    }

    execute_mode_action(mode, &action_clean, workspace_root)
}

/// Formats and renders execution result to stdout with ANSI decorations.
pub fn render_execution_result(res: &ModeExecutionResult) {
    let status_str = if res.success {
        format!("{}[OK - SUCCESS]{}", CLR_GREEN, CLR_RESET)
    } else {
        format!("{}[FAILED]{}", CLR_RED, CLR_RESET)
    };

    println!();
    println!(
        "{}────────────────────────────────────────────────────────────────────────────────{}",
        CLR_DIM, CLR_RESET
    );
    println!(
        "{}Mode:{} {:<12} {}Action:{} {:<12} {}Status:{} {} ({} ms)",
        CLR_BOLD,
        CLR_RESET,
        res.mode.as_str().to_uppercase(),
        CLR_BOLD,
        CLR_RESET,
        res.action,
        CLR_BOLD,
        CLR_RESET,
        status_str,
        res.duration_ms
    );
    println!(
        "{}────────────────────────────────────────────────────────────────────────────────{}",
        CLR_DIM, CLR_RESET
    );

    if !res.stdout.trim().is_empty() {
        println!("{}", res.stdout.trim());
    }

    if !res.stderr.trim().is_empty() {
        eprintln!("{}Error / Diagnostic Output:{}", CLR_YELLOW, CLR_RESET);
        eprintln!("{}", res.stderr.trim());
    }

    println!(
        "{}────────────────────────────────────────────────────────────────────────────────{}",
        CLR_DIM, CLR_RESET
    );
    println!();
}

/// Runs interactive terminal menu loop.
pub async fn run_interactive_menu(workspace_root: &Path) -> i32 {
    loop {
        println!();
        println!(
            "{}{}================================================================================",
            CLR_BOLD, CLR_CYAN
        );
        println!("       🚀 OXIDESWARM WORKFLOW MODES & PRESETS CONTROL CONSOLE (ox-mode)");
        println!(
            "================================================================================{}",
            CLR_RESET
        );
        println!("  Select a workflow mode:");
        println!(
            "    {}1.{} {}🧪 Test & QA Mode{}         (Unit tests, Clippy lints, Flaky detector)",
            CLR_BOLD, CLR_RESET, CLR_CYAN, CLR_RESET
        );
        println!(
            "    {}2.{} {}⚡ Development Mode{}       (Fast check, Local cluster dev, Health probe)",
            CLR_BOLD, CLR_RESET, CLR_YELLOW, CLR_RESET
        );
        println!(
            "    {}3.{} {}📚 Documentation Mode{}     (Cargo doc build, Spec verify, Export summary)",
            CLR_BOLD, CLR_RESET, CLR_MAGENTA, CLR_RESET
        );
        println!(
            "    {}4.{} {}🔬 Research & Bench Mode{}  (Hardware profile, Micro-benchmarks, Synthetic sim)",
            CLR_BOLD, CLR_RESET, CLR_GREEN, CLR_RESET
        );
        println!("    {}0.{} Exit", CLR_BOLD, CLR_RESET);
        println!(
            "{}--------------------------------------------------------------------------------{}",
            CLR_DIM, CLR_RESET
        );
        print!("Enter your choice [0-4]: ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        if io::stdin().read_line(&mut input).is_err() {
            return 0;
        }

        let choice = input.trim();
        match choice {
            "1" => {
                run_test_menu(workspace_root).await;
            }
            "2" => {
                run_dev_menu(workspace_root).await;
            }
            "3" => {
                run_doc_menu(workspace_root).await;
            }
            "4" => {
                run_research_menu(workspace_root).await;
            }
            "0" | "q" | "exit" => {
                println!("Goodbye!");
                return 0;
            }
            _ => {
                println!(
                    "{}Invalid selection. Please choose from 0 to 4.{}",
                    CLR_YELLOW, CLR_RESET
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_hardware_profiling_execution() {
        let profile = run_hardware_profile();
        assert!(profile.success);
        assert!(profile.metrics.contains_key("cpu_cores"));
        assert!(profile.metrics.contains_key("ram_total_mb"));
        assert!(profile.stdout.contains("OxideSwarm Hardware"));
    }

    #[test]
    fn test_micro_benchmarks_execution() {
        let bench = run_micro_benchmarks();
        assert!(bench.success);
        assert!(bench.metrics.contains_key("serde_ops_per_sec"));
        assert!(bench.metrics.contains_key("codec_ops_per_sec"));
        assert_eq!(
            bench.metrics.get("codec_wire_discriminator"),
            Some(&serde_json::json!(0x02))
        );
        let decoded_id = bench
            .metrics
            .get("codec_decoded_task_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let expected_id = bench
            .metrics
            .get("expected_task_id")
            .and_then(|v| v.as_str())
            .unwrap_or("mismatch");
        assert!(!decoded_id.is_empty());
        assert_eq!(decoded_id, expected_id);
        assert!(
            bench
                .metrics
                .get("codec_frame_bytes")
                .and_then(|v| v.as_u64())
                .unwrap_or(0)
                > 20
        );
        assert!(bench.stdout.contains("Bincode Wire Codec Roundtrip"));
    }

    #[test]
    fn test_distributed_simulation_is_honestly_labeled() {
        let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let dist = run_distributed_benchmark(&workspace_root);
        assert!(dist.success);
        assert_eq!(
            dist.metrics.get("simulation_only"),
            Some(&serde_json::json!(true))
        );
        let lower = dist.stdout.to_lowercase();
        assert!(!lower.contains("optimal"));
        assert!(!lower.contains("rtx 4090"));
        assert!(!lower.contains("rtx4090"));
        assert!(!lower.contains("worker-gpu-rtx4090"));
        assert!(!lower.contains("snapdragon"));

        let allocs = dist
            .metrics
            .get("worker_allocations")
            .and_then(|v| v.as_object())
            .expect("worker_allocations object");
        let allowed = [
            "sim-cpu-node-a",
            "sim-cpu-node-b",
            "sim-gpu-node",
            "sim-mobile-node",
        ];
        for key in allocs.keys() {
            assert!(
                allowed.contains(&key.as_str()),
                "unexpected synthetic worker name '{key}'"
            );
        }
    }

    #[test]
    fn test_parse_cargo_test_counts_two_passed_one_failed() {
        let fixture = "\
running 3 tests
test tests::test_alpha ... ok
test tests::test_beta ... ok
test tests::test_gamma ... FAILED

failures:

---- tests::test_gamma stdout ----
thread 'tests::test_gamma' panicked at 'assertion failed'

failures:
    tests::test_gamma

test result: FAILED. 2 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
";
        let (passed, failed) = parse_cargo_test_counts(fixture);
        assert_eq!(passed, 2);
        assert_eq!(failed, 1);

        let multi_crate_fixture = format!(
            "running 5 tests\ntest result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n{fixture}"
        );
        assert_eq!(parse_cargo_test_counts(&multi_crate_fixture), (7, 1));
    }

    #[test]
    fn test_lint_and_flaky_fail_when_only_part_passes() {
        let clippy_only = evaluate_lint_outcomes(true, "ok", "", false, "Needs formatting", "", 12);
        assert!(
            !clippy_only.success,
            "lint must fail when fmt fails even if clippy passed"
        );
        assert_eq!(clippy_only.exit_code, Some(1));

        let fmt_only = evaluate_lint_outcomes(false, "", "clippy warning", true, "", "", 12);
        assert!(
            !fmt_only.success,
            "lint must fail when clippy fails even if fmt passed"
        );
        assert_eq!(fmt_only.exit_code, Some(1));

        let both_ok = evaluate_lint_outcomes(true, "", "", true, "", "", 12);
        assert!(both_ok.success);
        assert_eq!(both_ok.exit_code, Some(0));

        let partial_flaky = run_flaky_detection_with(5, |iter| (iter != 3, 5));
        assert!(
            !partial_flaky.success,
            "flaky detection must fail when only 4 of 5 iterations pass"
        );
        assert_eq!(partial_flaky.exit_code, Some(1));
        assert_eq!(
            partial_flaky.metrics.get("passed"),
            Some(&serde_json::json!(4))
        );

        let all_flaky_pass = run_flaky_detection_with(5, |_| (true, 5));
        assert!(all_flaky_pass.success);
        assert_eq!(all_flaky_pass.exit_code, Some(0));
    }

    #[test]
    fn test_research_report_and_doc_build_fail_on_missing_or_unwritable_output() {
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(temp.path().join("target"), b"not a directory").expect("write target file");

        let report_res = generate_research_report(temp.path());
        assert!(
            !report_res.success,
            "generate_research_report must fail when target is a regular file"
        );
        assert_eq!(report_res.exit_code, Some(1));
        assert!(
            !report_res.stdout.contains("Saved Path:"),
            "failed report must not claim Saved Path"
        );
        assert!(report_res
            .stderr
            .contains("Failed to write research report"));

        let mut doc_res = ModeExecutionResult::new(WorkflowMode::Doc, "build");
        doc_res.success = true;
        doc_res.exit_code = Some(0);
        finalize_doc_build_output_with_target_dir(
            &temp.path().join("missing_target"),
            &mut doc_res,
        );
        assert!(
            !doc_res.success,
            "finalize_doc_build_output must fail when index.html was not produced"
        );
        assert_eq!(doc_res.exit_code, Some(1));

        let custom_target = temp.path().join("custom_cargo_target");
        let doc_dir = custom_target.join("doc/rusty_grid_core");
        std::fs::create_dir_all(&doc_dir).expect("create doc dir");
        std::fs::write(doc_dir.join("index.html"), "<html></html>").expect("write index.html");
        let mut custom_doc_res = ModeExecutionResult::new(WorkflowMode::Doc, "build");
        custom_doc_res.success = true;
        custom_doc_res.exit_code = Some(0);
        finalize_doc_build_output_with_target_dir(&custom_target, &mut custom_doc_res);
        assert!(
            custom_doc_res.success,
            "finalize_doc_build_output must succeed when index.html exists in custom CARGO_TARGET_DIR"
        );
        assert_eq!(custom_doc_res.exit_code, Some(0));
    }
}
