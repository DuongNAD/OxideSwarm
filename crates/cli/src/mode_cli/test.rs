//! `ox-mode test` workflow engine and interactive menu, split out of `mode_cli.rs`
//! (design 0080, MAINT-2).

use std::io::{self, Write};
use std::path::Path;
use std::process::Command;
use std::time::Instant;

use rusty_grid_core::mode::{ModeExecutionResult, WorkflowMode};

use super::{execute_single_action, extract_count, render_execution_result, CLR_BOLD, CLR_RESET};

/// Sums `passed;` and `failed;` counts across every `test result:` summary line in `cargo test` output.
pub fn parse_cargo_test_counts(stdout: &str) -> (usize, usize) {
    let mut passed_tests = 0;
    let mut failed_tests = 0;
    for line in stdout.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("test result:") {
            if let Some(passed) = extract_count(trimmed, "passed;") {
                passed_tests += passed;
            }
            if let Some(failed) = extract_count(trimmed, "failed;") {
                failed_tests += failed;
            }
        }
    }
    (passed_tests, failed_tests)
}

/// Runs quick workspace unit tests (`cargo test --workspace --lib`).
pub fn run_quick_tests(workspace_root: &Path) -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Test, "quick");

    let cmd_res = rusty_grid_core::process_spawn::output_std(
        Command::new("cargo")
            .args(["test", "--workspace", "--lib", "--", "--nocapture"])
            .current_dir(workspace_root),
    );

    res.duration_ms = start.elapsed().as_millis() as u64;

    match cmd_res {
        Ok(output) => {
            res.exit_code = output.status.code();
            res.success = output.status.success();
            res.stdout = String::from_utf8_lossy(&output.stdout).to_string();
            res.stderr = String::from_utf8_lossy(&output.stderr).to_string();

            let (passed_tests, failed_tests) = parse_cargo_test_counts(&res.stdout);
            res = res.with_metric("passed_tests", passed_tests);
            res = res.with_metric("failed_tests", failed_tests);
        }
        Err(e) => {
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = format!("Failed to spawn cargo: {}", e);
        }
    }
    res
}

/// Runs full workspace test suite (`cargo test --workspace`).
pub fn run_full_tests(workspace_root: &Path) -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Test, "full");

    let cmd_res = rusty_grid_core::process_spawn::output_std(
        Command::new("cargo")
            .args(["test", "--workspace", "--", "--nocapture"])
            .current_dir(workspace_root),
    );

    res.duration_ms = start.elapsed().as_millis() as u64;

    match cmd_res {
        Ok(output) => {
            res.exit_code = output.status.code();
            res.success = output.status.success();
            res.stdout = String::from_utf8_lossy(&output.stdout).to_string();
            res.stderr = String::from_utf8_lossy(&output.stderr).to_string();
        }
        Err(e) => {
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = format!("Failed to execute cargo: {}", e);
        }
    }
    res
}

/// Combines `cargo clippy` and `cargo fmt --check` outcomes into a `ModeExecutionResult`.
/// Both checks must succeed for `res.success` to be `true`.
pub fn evaluate_lint_outcomes(
    clippy_ok: bool,
    clippy_stdout: &str,
    clippy_stderr: &str,
    fmt_ok: bool,
    fmt_stdout: &str,
    fmt_stderr: &str,
    duration_ms: u64,
) -> ModeExecutionResult {
    let mut res = ModeExecutionResult::new(WorkflowMode::Test, "lint");
    res.duration_ms = duration_ms;
    res.success = clippy_ok && fmt_ok;
    res.exit_code = Some(if res.success { 0 } else { 1 });

    let mut combined_stdout = String::new();
    let mut combined_stderr = String::new();

    combined_stdout.push_str("=== 1. Cargo Clippy Lints ===\n");
    combined_stdout.push_str(clippy_stdout);
    combined_stderr.push_str(clippy_stderr);

    combined_stdout.push_str("\n=== 2. Rustfmt Code Style ===\n");
    if fmt_ok {
        combined_stdout.push_str("All source files conform to standard Rust formatting.\n");
    } else {
        combined_stderr.push_str(fmt_stderr);
        combined_stderr.push_str(fmt_stdout);
    }

    res.stdout = combined_stdout;
    res.stderr = combined_stderr;
    res = res.with_metric("clippy_passed", clippy_ok);
    res = res.with_metric("fmt_passed", fmt_ok);
    res
}

/// Runs clippy lint checks and rustfmt verification.
pub fn run_lints(workspace_root: &Path) -> ModeExecutionResult {
    let start = Instant::now();

    let clippy_out = rusty_grid_core::process_spawn::output_std(
        Command::new("cargo")
            .args([
                "clippy",
                "--workspace",
                "--all-targets",
                "--",
                "-D",
                "warnings",
            ])
            .current_dir(workspace_root),
    );

    let fmt_out = rusty_grid_core::process_spawn::output_std(
        Command::new("cargo")
            .args(["fmt", "--all", "--", "--check"])
            .current_dir(workspace_root),
    );

    let duration_ms = start.elapsed().as_millis() as u64;

    let (clippy_ok, clippy_stdout, clippy_stderr) = match clippy_out {
        Ok(o) => (
            o.status.success(),
            String::from_utf8_lossy(&o.stdout).into_owned(),
            String::from_utf8_lossy(&o.stderr).into_owned(),
        ),
        Err(e) => (
            false,
            String::new(),
            format!("Failed to spawn clippy: {e}\n"),
        ),
    };

    let (fmt_ok, fmt_stdout, fmt_stderr) = match fmt_out {
        Ok(o) => (
            o.status.success(),
            String::from_utf8_lossy(&o.stdout).into_owned(),
            String::from_utf8_lossy(&o.stderr).into_owned(),
        ),
        Err(e) => (
            false,
            String::new(),
            format!("Failed to spawn rustfmt: {e}\n"),
        ),
    };

    let res = evaluate_lint_outcomes(
        clippy_ok,
        &clippy_stdout,
        &clippy_stderr,
        fmt_ok,
        &fmt_stdout,
        &fmt_stderr,
        duration_ms,
    );

    crate::size_check::fold_into_lint(res, workspace_root)
}

/// Evaluates flaky test detection iterations using the supplied per-iteration runner `(iteration_1_based) -> (passed, elapsed_ms)`.
/// Every iteration must pass (`pass_count == iters`) for `res.success` to be `true`.
pub fn run_flaky_detection_with<F>(iterations: usize, mut run_one: F) -> ModeExecutionResult
where
    F: FnMut(usize) -> (bool, u64),
{
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Test, "flaky");
    let iters = if iterations == 0 { 5 } else { iterations };

    let mut iteration_results = Vec::new();
    let mut pass_count = 0;
    let mut total_duration_ms = 0u64;

    for i in 1..=iters {
        let (ok, elapsed) = run_one(i);
        total_duration_ms += elapsed;
        if ok {
            pass_count += 1;
        }
        iteration_results.push(serde_json::json!({
            "iteration": i,
            "success": ok,
            "duration_ms": elapsed
        }));
    }

    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = pass_count == iters;
    res.exit_code = Some(if res.success { 0 } else { 1 });

    let reliability_pct = (pass_count as f64 / iters as f64) * 100.0;
    let avg_duration_ms = total_duration_ms / iters as u64;

    let mut stdout = format!(
        "=== Flaky Test Detection Summary ===\nTotal Iterations: {}\nPassed: {} / {}\nPass Rate: {:.1}%\nAverage Duration: {} ms\n\nIterative Breakdown:\n",
        iters, pass_count, iters, reliability_pct, avg_duration_ms
    );
    for item in &iteration_results {
        stdout.push_str(&format!(
            "  - Run #{}: {} ({} ms)\n",
            item["iteration"],
            if item["success"].as_bool().unwrap_or(false) {
                "[PASS]"
            } else {
                "[FAIL]"
            },
            item["duration_ms"]
        ));
    }

    res.stdout = stdout;
    res = res.with_metric("iterations", iters);
    res = res.with_metric("passed", pass_count);
    res = res.with_metric("reliability_pct", reliability_pct);
    res = res.with_metric("avg_duration_ms", avg_duration_ms);
    res = res.with_metric("runs", iteration_results);

    res
}

/// Runs test iterations to detect flaky non-deterministic tests.
pub fn run_flaky_detection(workspace_root: &Path, iterations: usize) -> ModeExecutionResult {
    run_flaky_detection_with(iterations, |_i| {
        let iter_start = Instant::now();
        let cmd = rusty_grid_core::process_spawn::output_std(
            Command::new("cargo")
                .args(["test", "--workspace", "--lib", "--quiet"])
                .current_dir(workspace_root),
        );
        let elapsed = iter_start.elapsed().as_millis() as u64;
        let ok = cmd.as_ref().map(|o| o.status.success()).unwrap_or(false);
        (ok, elapsed)
    })
}

pub(super) async fn run_test_menu(workspace_root: &Path) {
    println!();
    println!(
        "{}=== 🧪 TEST & QUALITY ASSURANCE MODE ==={}",
        CLR_BOLD, CLR_RESET
    );
    println!("  [1] Quick Unit Tests (cargo test --workspace --lib)");
    println!("  [2] Lint Checks & Formatting (cargo clippy & fmt)");
    println!("  [3] Flaky Test Detector (5 iterations test loop)");
    println!("  [4] Full Integration & E2E Test Suite (cargo test --workspace)");
    println!("  [0] Back to Main Menu");
    print!("Select [0-4]: ");
    io::stdout().flush().unwrap();

    let mut input = String::new();
    let _ = io::stdin().read_line(&mut input);
    match input.trim() {
        "1" => {
            println!("Running Quick Unit Tests...");
            let res = execute_single_action(WorkflowMode::Test, "quick", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "2" => {
            println!("Running Lint & Format checks...");
            let res = execute_single_action(WorkflowMode::Test, "lint", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "3" => {
            println!("Running Flaky Test Detector (5 iterations)...");
            let res = execute_single_action(WorkflowMode::Test, "flaky", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "4" => {
            println!("Running Full Test Suite...");
            let res = execute_single_action(WorkflowMode::Test, "full", 5, workspace_root).await;
            render_execution_result(&res);
        }
        _ => {}
    }
}
