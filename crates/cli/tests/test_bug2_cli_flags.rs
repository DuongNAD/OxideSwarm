//! Integration tests for BUG-2: CLI flag validation for task types and output-dir.
//!
//! Validates that:
//! 1. Unsupported task kinds (gpu, 3d, autorig, builtin-test, and all kebab/snake/continuous aliases)
//!    refuse staging flags: `--output`, `--file`/`--input`, and `--output-dir`.
//! 2. `--output-dir` requires at least one `--output` pattern.
//! 3. Valid command and shell tasks accept `--output` and `--output-dir`.
//! 4. Kebab-case `--type auto-rig` properly constructs `TaskSpec::AutoRig`.

use rusty_grid_cli::cli_app::{build_task_spec, SubmitArgs};
use rusty_grid_core::task::TaskSpec;
use std::path::PathBuf;

// =========================================================================
// 1. GPU Task Staging Flag Rejections
// =========================================================================

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
fn test_gpu_task_refuses_output_dir_flag() {
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

// =========================================================================
// 2. 3D Generative Task Staging Flag Rejections
// =========================================================================

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
fn test_3d_task_refuses_file_flag() {
    let args = SubmitArgs {
        task_type: "3d".to_string(),
        prompt: Some("a cube".to_string()),
        inputs: vec!["Cargo.toml".to_string()],
        ..Default::default()
    };
    let res = build_task_spec(&args);
    assert!(
        res.is_err(),
        "--type 3d with --file/--input must be rejected by build_task_spec"
    );
    let err = res.unwrap_err();
    assert!(
        err.contains("--input") || err.contains("--file"),
        "Error message should mention --input / --file, got: {err}"
    );
}

#[test]
fn test_3d_task_refuses_output_dir_flag() {
    let args = SubmitArgs {
        task_type: "3d".to_string(),
        prompt: Some("a cube".to_string()),
        output_dir: Some(PathBuf::from("out/")),
        ..Default::default()
    };
    let res = build_task_spec(&args);
    assert!(
        res.is_err(),
        "--type 3d with --output-dir must be rejected by build_task_spec"
    );
    let err = res.unwrap_err();
    assert!(
        err.contains("--output-dir"),
        "Error message should mention --output-dir, got: {err}"
    );
}

// =========================================================================
// 3. Auto-Rig Task Staging Flag Rejections (Kebab, Snake, Continuous)
// =========================================================================

#[test]
fn test_auto_rig_kebab_case_refuses_output_flag() {
    let args = SubmitArgs {
        task_type: "auto-rig".to_string(),
        outputs: vec!["out.txt".to_string()],
        ..Default::default()
    };
    let res = build_task_spec(&args);
    assert!(
        res.is_err(),
        "--type auto-rig with --output must be rejected by build_task_spec"
    );
    let err = res.unwrap_err();
    assert!(
        err.contains("--output"),
        "Error message should mention --output, got: {err}"
    );
    assert!(
        err.contains("auto-rig"),
        "Error message should mention auto-rig, got: {err}"
    );
}

#[test]
fn test_auto_rig_kebab_case_refuses_file_flag() {
    let args = SubmitArgs {
        task_type: "auto-rig".to_string(),
        inputs: vec!["Cargo.toml".to_string()],
        ..Default::default()
    };
    let res = build_task_spec(&args);
    assert!(
        res.is_err(),
        "--type auto-rig with --file/--input must be rejected by build_task_spec"
    );
    let err = res.unwrap_err();
    assert!(
        err.contains("--input") || err.contains("--file"),
        "Error message should mention --input / --file, got: {err}"
    );
    assert!(
        err.contains("auto-rig"),
        "Error message should mention auto-rig, got: {err}"
    );
}

#[test]
fn test_auto_rig_kebab_case_refuses_output_dir_flag() {
    let args = SubmitArgs {
        task_type: "auto-rig".to_string(),
        output_dir: Some(PathBuf::from("results/")),
        ..Default::default()
    };
    let res = build_task_spec(&args);
    assert!(
        res.is_err(),
        "--type auto-rig with --output-dir must be rejected by build_task_spec"
    );
    let err = res.unwrap_err();
    assert!(
        err.contains("--output-dir"),
        "Error message should mention --output-dir, got: {err}"
    );
    assert!(
        err.contains("auto-rig"),
        "Error message should mention auto-rig, got: {err}"
    );
}

#[test]
fn test_autorig_continuous_refuses_output_flag() {
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
fn test_auto_rig_snake_case_refuses_file_flag() {
    let args = SubmitArgs {
        task_type: "auto_rig".to_string(),
        inputs: vec!["Cargo.toml".to_string()],
        ..Default::default()
    };
    let res = build_task_spec(&args);
    assert!(
        res.is_err(),
        "--type auto_rig with --file/--input must be rejected"
    );
}

// =========================================================================
// 4. Builtin-Test Staging Flag Rejections (Kebab, Snake, Continuous)
// =========================================================================

#[test]
fn test_builtin_test_kebab_case_refuses_output_flag() {
    let args = SubmitArgs {
        task_type: "builtin-test".to_string(),
        outputs: vec!["out.txt".to_string()],
        ..Default::default()
    };
    let res = build_task_spec(&args);
    assert!(
        res.is_err(),
        "--type builtin-test with --output must be rejected by build_task_spec"
    );
    let err = res.unwrap_err();
    assert!(
        err.contains("--output"),
        "Error message should mention --output, got: {err}"
    );
    assert!(
        err.contains("builtin-test"),
        "Error message should mention builtin-test, got: {err}"
    );
}

#[test]
fn test_builtin_test_kebab_case_refuses_file_flag() {
    let args = SubmitArgs {
        task_type: "builtin-test".to_string(),
        inputs: vec!["Cargo.toml".to_string()],
        ..Default::default()
    };
    let res = build_task_spec(&args);
    assert!(
        res.is_err(),
        "--type builtin-test with --file/--input must be rejected by build_task_spec"
    );
    let err = res.unwrap_err();
    assert!(
        err.contains("--input") || err.contains("--file"),
        "Error message should mention --input / --file, got: {err}"
    );
    assert!(
        err.contains("builtin-test"),
        "Error message should mention builtin-test, got: {err}"
    );
}

#[test]
fn test_builtin_test_kebab_case_refuses_output_dir_flag() {
    let args = SubmitArgs {
        task_type: "builtin-test".to_string(),
        output_dir: Some(PathBuf::from("results/")),
        ..Default::default()
    };
    let res = build_task_spec(&args);
    assert!(
        res.is_err(),
        "--type builtin-test with --output-dir must be rejected by build_task_spec"
    );
    let err = res.unwrap_err();
    assert!(
        err.contains("--output-dir"),
        "Error message should mention --output-dir, got: {err}"
    );
    assert!(
        err.contains("builtin-test"),
        "Error message should mention builtin-test, got: {err}"
    );
}

#[test]
fn test_builtin_test_kebab_case_refuses_output_dir_with_output() {
    let args = SubmitArgs {
        task_type: "builtin-test".to_string(),
        outputs: vec!["out.txt".to_string()],
        output_dir: Some(PathBuf::from("results/")),
        ..Default::default()
    };
    let res = build_task_spec(&args);
    assert!(
        res.is_err(),
        "--type builtin-test with both --output and --output-dir must be rejected"
    );
    let err = res.unwrap_err();
    assert!(
        err.contains("builtin-test"),
        "Error message should mention builtin-test, got: {err}"
    );
}

#[test]
fn test_builtin_test_snake_case_refuses_output_flag() {
    let args = SubmitArgs {
        task_type: "builtin_test".to_string(),
        outputs: vec!["out.txt".to_string()],
        ..Default::default()
    };
    let res = build_task_spec(&args);
    assert!(
        res.is_err(),
        "--type builtin_test with --output must be rejected"
    );
    let err = res.unwrap_err();
    assert!(err.contains("--output"), "got: {err}");
}

#[test]
fn test_builtin_test_continuous_refuses_output_flag() {
    let args = SubmitArgs {
        task_type: "builtintest".to_string(),
        outputs: vec!["out.txt".to_string()],
        ..Default::default()
    };
    let res = build_task_spec(&args);
    assert!(
        res.is_err(),
        "--type builtintest with --output must be rejected"
    );
    let err = res.unwrap_err();
    assert!(err.contains("--output"), "got: {err}");
}

// =========================================================================
// 5. Output-Dir Coupling Rules
// =========================================================================

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

// =========================================================================
// 6. Comprehensive Parameterized Matrix for All Non-Command Aliases
// =========================================================================

#[test]
fn test_all_unsupported_task_type_aliases_reject_staging_flags() {
    let aliases = [
        "gpu",
        "3d",
        "gen3d",
        "3d_generate",
        "3d-generate",
        "generative_3d",
        "generative-3d",
        "autorig",
        "auto_rig",
        "auto-rig",
        "rig",
        "builtin_test",
        "builtin-test",
        "builtintest",
    ];

    for task_type in aliases {
        // 1. Must refuse --output
        let out_args = SubmitArgs {
            task_type: task_type.to_string(),
            outputs: vec!["file.txt".to_string()],
            ..Default::default()
        };
        let res = build_task_spec(&out_args);
        assert!(
            res.is_err(),
            "--type {task_type} with --output MUST be rejected"
        );
        let err = res.unwrap_err();
        assert!(
            err.contains("--output"),
            "--type {task_type} error must mention --output: {err}"
        );

        // 2. Must refuse --input / --file
        let in_args = SubmitArgs {
            task_type: task_type.to_string(),
            inputs: vec!["Cargo.toml".to_string()],
            ..Default::default()
        };
        let res = build_task_spec(&in_args);
        assert!(
            res.is_err(),
            "--type {task_type} with --input/--file MUST be rejected"
        );
        let err = res.unwrap_err();
        assert!(
            err.contains("--input") || err.contains("--file"),
            "--type {task_type} error must mention --input/--file: {err}"
        );

        // 3. Must refuse --output-dir
        let dir_args = SubmitArgs {
            task_type: task_type.to_string(),
            output_dir: Some(PathBuf::from("results/")),
            ..Default::default()
        };
        let res = build_task_spec(&dir_args);
        assert!(
            res.is_err(),
            "--type {task_type} with --output-dir MUST be rejected"
        );
        let err = res.unwrap_err();
        assert!(
            err.contains("--output-dir"),
            "--type {task_type} error must mention --output-dir: {err}"
        );
    }
}

// =========================================================================
// 7. Positive Controls (Valid Submissions Accepted)
// =========================================================================

#[test]
fn test_positive_control_command_with_output_and_output_dir() {
    let args = SubmitArgs {
        command: Some("echo".to_string()),
        args: vec!["hello".to_string()],
        outputs: vec!["out.txt".to_string()],
        output_dir: Some(PathBuf::from("./results")),
        ..Default::default()
    };
    let res = build_task_spec(&args);
    assert!(
        res.is_ok(),
        "Command task with valid output and output-dir must succeed: {:?}",
        res.err()
    );
    assert!(
        matches!(res.unwrap(), TaskSpec::Command { .. }),
        "Spec must be TaskSpec::Command"
    );
}

#[test]
fn test_positive_control_shell_with_output_and_output_dir() {
    let args = SubmitArgs {
        task_type: "shell".to_string(),
        command: Some("echo shell".to_string()),
        outputs: vec!["out.txt".to_string()],
        output_dir: Some(PathBuf::from("./results")),
        ..Default::default()
    };
    let res = build_task_spec(&args);
    assert!(
        res.is_ok(),
        "Shell task with valid output and output-dir must succeed: {:?}",
        res.err()
    );
    assert!(
        matches!(res.unwrap(), TaskSpec::ShellScript { .. }),
        "Spec must be TaskSpec::ShellScript"
    );
}

#[test]
fn test_auto_rig_kebab_case_valid_submission_creates_autorig_spec() {
    let temp_dir = tempfile::tempdir().unwrap();
    let dummy_mesh = temp_dir.path().join("model.glb");
    std::fs::write(&dummy_mesh, b"dummy mesh bytes for test").unwrap();

    let args = SubmitArgs {
        task_type: "auto-rig".to_string(),
        input_mesh: Some(dummy_mesh),
        ..Default::default()
    };
    let res = build_task_spec(&args);
    assert!(
        res.is_ok(),
        "Valid --type auto-rig with --input-mesh must succeed: {:?}",
        res.err()
    );
    assert!(
        matches!(res.unwrap(), TaskSpec::AutoRig(_)),
        "Expected TaskSpec::AutoRig, but got a different variant"
    );
}
