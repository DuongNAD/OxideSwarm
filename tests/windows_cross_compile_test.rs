//! Integration tests for the Windows cross-compilation harness (`build_windows_worker.sh`).
//!
//! Validates:
//! 1. Script existence and executable file permissions.
//! 2. `--check` execution: inspecting host environment for Cargo, rustup, target, MinGW toolchains,
//!    outputting diagnostic status summary, setup guidance, and exiting with code 0.
//! 3. `--dry-run` execution: outputting resolved environment variables, target, profile, and
//!    planned `cargo build` command line, and exiting with code 0.
//! 4. Build profile selection (`--release` vs `--debug`).
//! 5. Custom feature flags and target arguments.
//! 6. Custom environment variables overriding compiler and linker paths.
//! 7. `--help` / `-h` flag execution and usage documentation.
//! 8. Unknown argument rejection with descriptive error message and exit code 1.

use std::path::PathBuf;
use std::process::Command;

/// Locates `build_windows_worker.sh` dynamically across various test execution contexts.
fn get_script_path() -> PathBuf {
    let mut candidates = vec![
        PathBuf::from("build_windows_worker.sh"),
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../build_windows_worker.sh"),
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../build_windows_worker.sh"),
    ];
    if let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR") {
        let p = PathBuf::from(manifest_dir);
        if let Some(parent) = p.parent() {
            candidates.push(parent.join("build_windows_worker.sh"));
            if let Some(grandparent) = parent.parent() {
                candidates.push(grandparent.join("build_windows_worker.sh"));
            }
        }
    }
    for candidate in &candidates {
        if candidate.exists() {
            return candidate.canonicalize().unwrap_or_else(|_| candidate.clone());
        }
    }
    panic!("Could not locate build_windows_worker.sh in candidates: {candidates:?}");
}

fn script_cmd(script: &std::path::Path) -> Command {
    #[cfg(windows)]
    {
        let bash_candidates = [
            r"C:\Program Files\Git\bin\bash.exe",
            r"C:\Program Files\Git\usr\bin\bash.exe",
            r"C:\Program Files (x86)\Git\bin\bash.exe",
            r"C:\msys64\usr\bin\bash.exe",
        ];
        let script_str = script.to_string_lossy();
        let clean_path = script_str.strip_prefix(r"\\?\").unwrap_or(&script_str).replace('\\', "/");
        for candidate in &bash_candidates {
            if std::path::Path::new(candidate).exists() {
                let mut cmd = Command::new(candidate);
                cmd.arg(&clean_path);
                return cmd;
            }
        }
        let mut cmd = Command::new("bash");
        cmd.arg(&clean_path);
        cmd
    }
    #[cfg(not(windows))]
    {
        Command::new(script)
    }
}

#[test]
fn test_windows_cross_compile_script_exists_and_is_executable() {
    let script = get_script_path();
    assert!(script.exists(), "build_windows_worker.sh must exist at {}", script.display());

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let metadata = fs::metadata(&script).expect("failed to read script metadata");
        let permissions = metadata.permissions();
        assert!(
            permissions.mode() & 0o111 != 0,
            "build_windows_worker.sh must have executable permissions (mode: {:o})",
            permissions.mode()
        );
    }
}

#[test]
fn test_windows_cross_compile_check_flag() {
    let script = get_script_path();
    let output = script_cmd(&script)
        .arg("--check")
        .output()
        .expect("failed to execute build_windows_worker.sh --check");

    assert!(
        output.status.success(),
        "build_windows_worker.sh --check should exit with code 0, got status: {:?}",
        output.status
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Verifying host cross-compilation environment"),
        "stdout should report environment verification: {stdout}"
    );
    assert!(
        stdout.contains("Found Cargo:"),
        "stdout should verify Cargo: {stdout}"
    );
    assert!(
        stdout.contains("Diagnostic status summary:"),
        "stdout should provide diagnostic status summary: {stdout}"
    );
    assert!(
        stdout.contains("Target (x86_64-pc-windows-gnu):"),
        "stdout should report x86_64-pc-windows-gnu status: {stdout}"
    );
    assert!(
        stdout.contains("MinGW linker:"),
        "stdout should report MinGW linker status: {stdout}"
    );
    assert!(
        stdout.contains("Cross-compilation environment verification check completed successfully."),
        "stdout should confirm check completion: {stdout}"
    );
}

#[test]
fn test_windows_cross_compile_dry_run_default_release() {
    let script = get_script_path();
    let output = script_cmd(&script)
        .arg("--dry-run")
        .output()
        .expect("failed to execute build_windows_worker.sh --dry-run");

    assert!(
        output.status.success(),
        "build_windows_worker.sh --dry-run should exit with code 0, got status: {:?}",
        output.status
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("[DRY-RUN] Execution plan confirmed."),
        "stdout should confirm dry run plan: {stdout}"
    );
    assert!(
        stdout.contains("Planned Environment Variables:"),
        "stdout should display planned environment variables: {stdout}"
    );
    assert!(
        stdout.contains("CC_x86_64_pc_windows_gnu="),
        "stdout should display CC_x86_64_pc_windows_gnu: {stdout}"
    );
    assert!(
        stdout.contains("CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER="),
        "stdout should display CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER: {stdout}"
    );
    assert!(
        stdout.contains("Target: x86_64-pc-windows-gnu"),
        "stdout should display default target: {stdout}"
    );
    assert!(
        stdout.contains("Profile: release"),
        "stdout should default to release profile: {stdout}"
    );
    assert!(
        stdout.contains("Command: cargo build --target x86_64-pc-windows-gnu --bin rusty-grid --release"),
        "stdout should display the exact release cargo build command: {stdout}"
    );
}

#[test]
fn test_windows_cross_compile_dry_run_debug_profile() {
    let script = get_script_path();
    let output = script_cmd(&script)
        .args(["--dry-run", "--debug"])
        .output()
        .expect("failed to execute build_windows_worker.sh --dry-run --debug");

    assert!(
        output.status.success(),
        "build_windows_worker.sh --dry-run --debug should exit with code 0, got status: {:?}",
        output.status
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Profile: debug"),
        "stdout should show debug profile: {stdout}"
    );
    assert!(
        stdout.contains("Command: cargo build --target x86_64-pc-windows-gnu --bin rusty-grid"),
        "stdout should display debug cargo build command: {stdout}"
    );
    assert!(
        !stdout.contains("--bin rusty-grid --release"),
        "debug mode should not include --release flag: {stdout}"
    );
}

#[test]
fn test_windows_cross_compile_dry_run_custom_features() {
    let script = get_script_path();
    let output = script_cmd(&script)
        .args(["--dry-run", "--features", "p2p"])
        .output()
        .expect("failed to execute build_windows_worker.sh --dry-run --features p2p");

    assert!(
        output.status.success(),
        "build_windows_worker.sh --dry-run --features p2p should exit with code 0, got status: {:?}",
        output.status
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Command: cargo build --target x86_64-pc-windows-gnu --bin rusty-grid --release --features p2p"),
        "stdout should include custom features in the cargo command: {stdout}"
    );
}

#[test]
fn test_windows_cross_compile_dry_run_custom_env_overrides() {
    let script = get_script_path();
    let custom_cc = "/opt/custom/x86_64-w64-mingw32-gcc";
    let custom_ld = "/opt/custom/x86_64-w64-mingw32-gcc-ld";

    let output = script_cmd(&script)
        .arg("--dry-run")
        .env("CC_x86_64_pc_windows_gnu", custom_cc)
        .env("CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER", custom_ld)
        .output()
        .expect("failed to execute build_windows_worker.sh with env overrides");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(&format!("CC_x86_64_pc_windows_gnu={custom_cc}")),
        "stdout should reflect custom CC override: {stdout}"
    );
    assert!(
        stdout.contains(&format!("CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER={custom_ld}")),
        "stdout should reflect custom linker override: {stdout}"
    );
}

#[test]
fn test_windows_cross_compile_help_options() {
    let script = get_script_path();
    for flag in ["--help", "-h"] {
        let output = script_cmd(&script)
            .arg(flag)
            .output()
            .unwrap_or_else(|e| panic!("failed to execute with {flag}: {e}"));

        assert!(
            output.status.success(),
            "build_windows_worker.sh {flag} should exit with code 0"
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("Usage: build_windows_worker.sh [OPTIONS]"));
        assert!(stdout.contains("--check"));
        assert!(stdout.contains("--dry-run"));
        assert!(stdout.contains("--release"));
        assert!(stdout.contains("--debug"));
        assert!(stdout.contains("--target <TRIPLE>"));
        assert!(stdout.contains("CC_x86_64_pc_windows_gnu"));
        assert!(stdout.contains("CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER"));
    }
}

#[test]
fn test_windows_cross_compile_invalid_argument_fails() {
    let script = get_script_path();
    let output = script_cmd(&script)
        .arg("--invalid-flag-for-testing")
        .output()
        .expect("failed to execute build_windows_worker.sh with invalid argument");

    assert!(
        !output.status.success(),
        "build_windows_worker.sh should fail with non-zero exit code for invalid argument"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let combined = format!("{stdout}\n{stderr}");

    assert!(
        combined.contains("Unknown argument: --invalid-flag-for-testing"),
        "output should report unknown argument: {combined}"
    );
}

#[test]
fn test_windows_cross_compile_diagnostic_guidance_present() {
    let script = get_script_path();
    let output = script_cmd(&script)
        .arg("--check")
        .output()
        .expect("failed to execute build_windows_worker.sh --check");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("brew install mingw-w64")
            || stdout.contains("apt-get install gcc-mingw-w64-x86-64")
            || stdout.contains("dnf install mingw64-gcc"),
        "diagnostic output should provide package manager setup guidance: {stdout}"
    );
}
