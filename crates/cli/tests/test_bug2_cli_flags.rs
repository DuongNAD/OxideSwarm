//! Integration tests for BUG-2: CLI flag validation for task types and output-dir.
//!
//! Validates that:
//! 1. `--type gpu --file Cargo.toml` fails before submission.
//! 2. `--type gpu --output out.txt` fails before submission.
//! 3. `--type 3d --output out.txt` fails before submission.
//! 4. `--type autorig --output out.txt` fails before submission.
//! 5. `--output-dir out/` without `--output` fails before submission.
//! 6. `--type gpu --output-dir out/` fails before submission.

use rusty_grid_cli::cli_app::{build_task_spec, SubmitArgs};
use std::path::PathBuf;

#[test]
fn test_gpu_task_refuses_file_flag() {
    let args = SubmitArgs {
        task_type: "gpu".to_string(),
        inputs: vec!["Cargo.toml".to_string()],
        ..Default::default()
    };
    let res = build_task_spec(&args);
    assert!(
        res.is_err(),
        "--type gpu with --file/--input must be rejected by build_task_spec"
    );
    let err = res.unwrap_err();
    assert!(
        err.contains("--input") || err.contains("--file"),
        "Error message should mention --input / --file, got: {err}"
    );
    assert!(
        err.contains("gpu"),
        "Error message should mention gpu, got: {err}"
    );
}

#[test]
fn test_gpu_task_refuses_output_flag() {
    let args = SubmitArgs {
        task_type: "gpu".to_string(),
        outputs: vec!["out.txt".to_string()],
        ..Default::default()
    };
    let res = build_task_spec(&args);
    assert!(
        res.is_err(),
        "--type gpu with --output must be rejected by build_task_spec"
    );
    let err = res.unwrap_err();
    assert!(
        err.contains("--output"),
        "Error message should mention --output, got: {err}"
    );
    assert!(
        err.contains("gpu"),
        "Error message should mention gpu, got: {err}"
    );
}

#[test]
fn test_3d_task_refuses_output_flag() {
    let args = SubmitArgs {
        task_type: "3d".to_string(),
        prompt: Some("a cube".to_string()),
        outputs: vec!["out.txt".to_string()],
        ..Default::default()
    };
    let res = build_task_spec(&args);
    assert!(
        res.is_err(),
        "--type 3d with --output must be rejected by build_task_spec"
    );
    let err = res.unwrap_err();
    assert!(
        err.contains("--output"),
        "Error message should mention --output, got: {err}"
    );
    assert!(
        err.contains("3d"),
        "Error message should mention 3d, got: {err}"
    );
}

#[test]
fn test_autorig_task_refuses_output_flag() {
    let args = SubmitArgs {
        task_type: "autorig".to_string(),
        input_mesh: Some(PathBuf::from("mesh.glb")),
        outputs: vec!["out.txt".to_string()],
        ..Default::default()
    };
    let res = build_task_spec(&args);
    assert!(
        res.is_err(),
        "--type autorig with --output must be rejected by build_task_spec"
    );
    let err = res.unwrap_err();
    assert!(
        err.contains("--output"),
        "Error message should mention --output, got: {err}"
    );
    assert!(
        err.contains("autorig"),
        "Error message should mention autorig, got: {err}"
    );
}

#[test]
fn test_output_dir_without_output_flag_fails() {
    let args = SubmitArgs {
        command: Some("echo".to_string()),
        args: vec!["hello".to_string()],
        output_dir: Some(PathBuf::from("out/")),
        outputs: vec![],
        ..Default::default()
    };
    let res = build_task_spec(&args);
    assert!(
        res.is_err(),
        "--output-dir without --output must be rejected by build_task_spec"
    );
    let err = res.unwrap_err();
    assert!(
        err.contains("--output-dir requires at least one --output pattern"),
        "Error message should mention requiring --output pattern, got: {err}"
    );
}

#[test]
fn test_output_dir_on_gpu_task_fails() {
    let args = SubmitArgs {
        task_type: "gpu".to_string(),
        output_dir: Some(PathBuf::from("out/")),
        ..Default::default()
    };
    let res = build_task_spec(&args);
    assert!(
        res.is_err(),
        "--type gpu with --output-dir must be rejected by build_task_spec"
    );
    let err = res.unwrap_err();
    assert!(
        err.contains("--output-dir"),
        "Error message should mention --output-dir, got: {err}"
    );
    assert!(
        err.contains("gpu"),
        "Error message should mention gpu, got: {err}"
    );
}
