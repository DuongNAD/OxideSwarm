//! Adversarial Stress Test Suite for Milestone M5B: Distributed File Staging & Output Artifact Collection.
//!
//! Author: Challenger 1 (critic, specialist)
//! Focus Areas:
//! 1. Path traversal fuzzing (.., ..\, foo/../../bar, ....//, UNC, colons, ADS, null bytes).
//! 2. Windows DOS device name fuzzing (CON, PRN, AUX, NUL, COM1..9, LPT1..9, case, extensions, subdirs).
//! 3. Size limit enforcement (staged file > 32 MiB, aggregate staged > 32 MiB, artifact > 10 MiB, aggregate artifacts > 32 MiB).
//! 4. Concurrency & memory exhaustion resilience.

use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::task::{
    validate_relative_sandbox_path, Bytes, StagedFile, Task, TaskId, TaskRequirements, TaskSpec,
    MAX_AGGREGATE_ARTIFACT_BYTES, MAX_STAGED_FILE_BYTES,
};
use rusty_grid_worker::runner::{RunnerConfig, TaskRunner};
use rusty_grid_worker::sandbox::{normalize_relative_path_str, Sandbox, SandboxConfig};

/// Helper to create an isolated TaskRunner with a temporary sandbox directory.
fn make_test_runner() -> (TaskRunner, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().expect("create temp dir for runner");
    let sandbox_cfg = SandboxConfig::new(temp_dir.path());
    let runner_cfg = RunnerConfig::new(sandbox_cfg);
    let capabilities = WorkerCapabilities {
        name: "challenger-1-stress-worker".into(),
        cpu_cores: 4,
        ram_mb: 8192,
        has_gpu: false,
        is_simulated_gpu: false,
        gpu_device_name: None,
        tags: vec!["challenger_stress".into()],
        mobile: None,
    };
    let runner = TaskRunner::new(Uuid::new_v4(), capabilities, runner_cfg);
    (runner, temp_dir)
}

// ============================================================================
// 1. Path Traversal Standard & Fuzzing Tests
// ============================================================================

#[test]
fn test_path_traversal_standard_matrix() {
    let standard_traversals = [
        // Basic traversal
        "..",
        "../",
        "..\\",
        "/..",
        "\\..",
        "../..",
        "..\\..",
        "../../..",
        "..\\..\\..",
        // Intermediate traversal
        "foo/../../bar",
        "foo\\..\\..\\bar",
        "a/b/c/../../../../etc/passwd",
        "a\\b\\c\\..\\..\\..\\..\\windows\\system32\\cmd.exe",
        "valid/path/../../../escaped",
        "nested/./../../escaped",
        "./../escaped",
        ".\\..\\escaped",
        // Absolute Unix paths
        "/",
        "//",
        "///",
        "/etc/shadow",
        "/var/run/docker.sock",
        "/tmp/evil",
        // Absolute Windows paths
        "\\",
        "\\\\",
        "\\\\server\\share",
        "\\\\?\\C:\\secret",
        "\\\\127.0.0.1\\c$\\windows",
        "\\Windows\\System32\\cmd.exe",
        "\\inetpub\\wwwroot",
        // Drive letters
        "C:",
        "C:\\",
        "C:/",
        "c:\\windows\\system32",
        "c:/windows/system32",
        "D:foo.txt",
        "D:nested/path.txt",
        "Z:\\backup",
        // Alternate Data Streams & Colons
        "foo:bar",
        "foo.txt:stream",
        "data.csv:$DATA",
        "sub/file.txt:hidden",
        "test::$INDEX_ALLOCATION",
        ":hidden",
        ":$DATA",
        // Null byte injection
        "normal.txt\0.exe",
        "\0",
        "data/\0/file.txt",
        "sub/file\0name.txt",
        // Empty paths
        "",
    ];

    let mut tested_count = 0;
    let mut failures = Vec::new();

    for path in standard_traversals {
        tested_count += 1;
        // Test validate_relative_sandbox_path
        if let Ok(accepted) = validate_relative_sandbox_path(path) {
            failures.push(format!(
                "validate_relative_sandbox_path ACCEPTED illegal path: '{path}' -> '{accepted}'"
            ));
        }

        // Test normalize_relative_path_str
        if let Ok(accepted) = normalize_relative_path_str(path) {
            failures.push(format!(
                "normalize_relative_path_str ACCEPTED illegal path: '{path}' -> '{accepted}'"
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "Standard traversal tests found {} bypasses out of {} tested paths:\n{}",
        failures.len(),
        tested_count,
        failures.join("\n")
    );
}

/// Adversarial stress test verifying multi-dot evasion (`....//`, `....`, `...`)
/// and whitespace traversal (`  ..  `, `  ../evil  `).
///
/// NOTE: This test reproduces known bypasses where `validate_relative_sandbox_path`
/// and `normalize_relative_path_str` fail to reject multi-dot and untrimmed components.
#[test]
fn test_path_traversal_adversarial_vulnerabilities() {
    let adversarial_traversals = [
        "....//",
        "..../",
        "....\\",
        "....\\\\",
        "foo/....//bar",
        "foo\\....\\\\bar",
        "....",
        "...",
        ".../",
        "...\\",
        ".....",
        "   ",
        "\t",
        "\r\n",
        "  ..  ",
        "  ../evil  ",
        "  ..\\evil  ",
    ];

    let mut bypasses = Vec::new();

    for path in adversarial_traversals {
        let core_res = validate_relative_sandbox_path(path);
        if let Ok(accepted) = core_res {
            bypasses.push(format!(
                "[VULNERABILITY] validate_relative_sandbox_path accepted '{path}' -> '{accepted}'"
            ));
        }

        let worker_res = normalize_relative_path_str(path);
        if let Ok(accepted) = worker_res {
            bypasses.push(format!(
                "[VULNERABILITY] normalize_relative_path_str accepted '{path}' -> '{accepted}'"
            ));
        }
    }

    assert!(
        bypasses.is_empty(),
        "Adversarial path traversal vulnerabilities detected ({} bypasses):\n{}",
        bypasses.len(),
        bypasses.join("\n")
    );
}

#[test]
fn test_path_traversal_combinatorial_generator() {
    let prefixes = [
        "",
        "/",
        "\\",
        "./",
        ".\\",
        "../",
        "..\\",
        "....//",
        "\\\\server\\share\\",
    ];
    let middles = [
        "foo",
        "bar/baz",
        "a/../b",
        "nested\\dir",
        "con",
        "COM1",
        "aux.txt",
    ];
    let suffixes = [
        "",
        "/",
        "\\",
        "/..",
        "\\..",
        "/../evil",
        "\\..\\evil",
        ":$DATA",
        "\0evil",
    ];

    let mut tested = 0;
    let mut violations = Vec::new();

    for p in prefixes {
        for m in middles {
            for s in suffixes {
                let candidate = format!("{p}{m}{s}");
                tested += 1;

                let should_reject = p.starts_with('/')
                    || p.starts_with('\\')
                    || p.contains("..")
                    || s.contains("..")
                    || m.contains("..")
                    || candidate.contains(':')
                    || candidate.contains('\0')
                    || m.to_ascii_uppercase().starts_with("CON")
                    || m.to_ascii_uppercase().starts_with("COM1")
                    || m.to_ascii_uppercase().starts_with("AUX");

                if should_reject {
                    if let Ok(res) = validate_relative_sandbox_path(&candidate) {
                        violations.push(format!(
                            "validate_relative_sandbox_path accepted '{candidate}' as '{res}'"
                        ));
                    }
                    if let Ok(res) = normalize_relative_path_str(&candidate) {
                        violations.push(format!(
                            "normalize_relative_path_str accepted '{candidate}' as '{res}'"
                        ));
                    }
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "Combinatorial traversal generator found {} violations out of {} variations:\n{}",
        violations.len(),
        tested,
        violations
            .iter()
            .take(20)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}

// ============================================================================
// 2. Windows DOS Device Name Adversarial Fuzzing
// ============================================================================

#[test]
fn test_windows_dos_device_names_standard_matrix() {
    let devices = [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];

    let extensions = [
        "", ".txt", ".csv", ".json", ".log", ".dat", ".bin", ".tar.gz", ".ext.sub",
    ];

    let casing_transforms: [fn(&str) -> String; 3] = [
        |s| s.to_ascii_uppercase(),
        |s| s.to_ascii_lowercase(),
        |s| {
            // Mixed case e.g. CoN, cOm1
            let mut c = s.chars();
            match c.next() {
                None => String::new(),
                Some(f) => f.to_uppercase().collect::<String>() + &c.as_str().to_lowercase(),
            }
        },
    ];

    let directory_prefixes = [
        "",
        "sub/",
        "nested/deep/directory/",
        "data\\",
        "config\\sub\\",
    ];

    let mut tested_count = 0;
    let mut failures = Vec::new();

    for dev in &devices {
        for ext in &extensions {
            for transform in &casing_transforms {
                let dev_name = transform(dev);
                let full_name = format!("{dev_name}{ext}");

                for dir in &directory_prefixes {
                    let path = format!("{dir}{full_name}");
                    tested_count += 1;

                    // 1. validate_relative_sandbox_path
                    if let Ok(res) = validate_relative_sandbox_path(&path) {
                        failures.push(format!(
                            "validate_relative_sandbox_path ALLOWED DOS device: '{path}' -> '{res}'"
                        ));
                    }

                    // 2. normalize_relative_path_str
                    if let Ok(res) = normalize_relative_path_str(&path) {
                        failures.push(format!(
                            "normalize_relative_path_str ALLOWED DOS device: '{path}' -> '{res}'"
                        ));
                    }
                }
            }
        }
    }

    // Trailing dot and space evasion
    let evasion_names = [
        "con ",
        "con.",
        "con. ",
        "con .",
        "aux.txt.",
        "aux.txt ",
        "COM1. ",
        "data/con ",
        "data\\prn.",
    ];

    for path in evasion_names {
        tested_count += 1;
        if let Ok(res) = validate_relative_sandbox_path(path) {
            failures.push(format!(
                "validate_relative_sandbox_path ALLOWED DOS evasion: '{path}' -> '{res}'"
            ));
        }
        if let Ok(res) = normalize_relative_path_str(path) {
            failures.push(format!(
                "normalize_relative_path_str ALLOWED DOS evasion: '{path}' -> '{res}'"
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "DOS device standard matrix found {} bypasses out of {} tested combinations:\n{}",
        failures.len(),
        tested_count,
        failures
            .iter()
            .take(20)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// Adversarial stress test verifying pre-extension space DOS device evasion (`nul .txt`, `lpt1 .dat`).
///
/// NOTE: Windows Win32 strips spaces before the dot extension when resolving device names,
/// so `nul .txt` targets the DOS device NUL.
#[test]
fn test_windows_dos_device_pre_dot_space_evasion() {
    let evasion_paths = [
        "nul .txt",
        "lpt1 .dat",
        "con .log",
        "aux .csv",
        "data/nul .json",
    ];

    let mut bypasses = Vec::new();

    for path in evasion_paths {
        if let Ok(res) = validate_relative_sandbox_path(path) {
            bypasses.push(format!(
                "[VULNERABILITY] validate_relative_sandbox_path accepted '{path}' -> '{res}'"
            ));
        }
        if let Ok(res) = normalize_relative_path_str(path) {
            bypasses.push(format!(
                "[VULNERABILITY] normalize_relative_path_str accepted '{path}' -> '{res}'"
            ));
        }
    }

    assert!(
        bypasses.is_empty(),
        "Windows DOS device pre-dot whitespace evasion detected ({} bypasses):\n{}",
        bypasses.len(),
        bypasses.join("\n")
    );
}

// ============================================================================
// 3. File & Artifact Size Limit Enforcement Tests
// ============================================================================

#[test]
fn test_staged_file_single_limit_rejected() {
    // 32 MiB + 1 byte
    let oversized_len = MAX_STAGED_FILE_BYTES + 1;
    let staged = StagedFile {
        path: "oversized.bin".to_string(),
        size: oversized_len as u64,
        sha256: [0u8; 32],
        executable: false,
        data: Bytes::from_static(b""),
    };

    let mut task = Task::new(
        TaskSpec::command("echo", vec!["test".into()]),
        TaskRequirements::generic(1, 10),
    );
    task.staged_files.push(staged);

    let val_res = task.validate();
    assert!(
        val_res.is_err(),
        "Task validation must reject staged file exceeding 32 MiB"
    );
}

#[tokio::test]
async fn test_sandbox_stage_files_single_size_limit() {
    let temp_sb_dir = tempfile::tempdir().unwrap();
    let sb_cfg = SandboxConfig::new(temp_sb_dir.path());
    let sandbox = Sandbox::create(&sb_cfg, TaskId::new()).await.unwrap();

    let oversized_staged = StagedFile {
        path: "huge.bin".to_string(),
        size: (MAX_STAGED_FILE_BYTES + 1024) as u64,
        sha256: [0u8; 32],
        executable: false,
        data: Bytes::new(),
    };

    let cache_dir = temp_sb_dir.path().join("cache");
    let res = sandbox.stage_files(&[oversized_staged], &cache_dir).await;
    assert!(
        res.is_err(),
        "Sandbox::stage_files must reject single file > 32 MiB"
    );
}

#[test]
fn test_staged_files_aggregate_limit_rejected() {
    // Two files: 20 MiB + 13 MiB = 33 MiB (> 32 MiB aggregate limit)
    let file1 = StagedFile {
        path: "part1.bin".to_string(),
        size: 20 * 1024 * 1024,
        sha256: [0u8; 32],
        executable: false,
        data: Bytes::new(),
    };
    let file2 = StagedFile {
        path: "part2.bin".to_string(),
        size: 13 * 1024 * 1024,
        sha256: [0u8; 32],
        executable: false,
        data: Bytes::new(),
    };

    let mut task = Task::new(
        TaskSpec::command("echo", vec!["test".into()]),
        TaskRequirements::generic(1, 10),
    );
    task.staged_files.push(file1);
    task.staged_files.push(file2);

    let val_res = task.validate();
    assert!(
        val_res.is_err(),
        "Task validation must reject aggregate staged files > 32 MiB"
    );
}

#[tokio::test]
async fn test_sandbox_stage_files_aggregate_limit() {
    let temp_sb_dir = tempfile::tempdir().unwrap();
    let sb_cfg = SandboxConfig::new(temp_sb_dir.path());
    let sandbox = Sandbox::create(&sb_cfg, TaskId::new()).await.unwrap();

    // Use valid payloads so checksum verification passes on first file,
    // ensuring aggregate limit is triggered on second file
    let data1 = vec![0x11u8; 17 * 1024 * 1024];
    let file1 = StagedFile::new("part1.bin", data1, false);

    let data2 = vec![0x22u8; 16 * 1024 * 1024];
    let file2 = StagedFile::new("part2.bin", data2, false);

    let cache_dir = temp_sb_dir.path().join("cache");
    let res = sandbox.stage_files(&[file1, file2], &cache_dir).await;
    assert!(
        res.is_err(),
        "Sandbox::stage_files must reject aggregate staged files > 32 MiB"
    );
    let err_str = res.unwrap_err().to_string();
    assert!(
        err_str.contains("Aggregate") || err_str.contains("32 MiB"),
        "Unexpected error: {err_str}"
    );
}

#[tokio::test]
async fn test_output_artifact_single_limit_harvesting() {
    let (runner, _tmp) = make_test_runner();
    let temp_sb_dir = tempfile::tempdir().unwrap();
    let sb_cfg = SandboxConfig::new(temp_sb_dir.path());
    let sandbox = Sandbox::create(&sb_cfg, TaskId::new()).await.unwrap();

    // Create an artifact file that is 10 MiB + 1 KiB
    let file_path = sandbox.path().join("oversized_artifact.dat");
    let chunk = vec![0xAAu8; 1024 * 1024]; // 1 MiB chunk
    let mut file = tokio::fs::File::create(&file_path).await.unwrap();
    use tokio::io::AsyncWriteExt;
    for _ in 0..10 {
        file.write_all(&chunk).await.unwrap();
    }
    file.write_all(&[0xBB; 1024]).await.unwrap(); // + 1 KiB => > 10 MiB
    file.flush().await.unwrap();
    drop(file);

    let collected = runner
        .harvest_output_artifacts(
            &sandbox,
            &["oversized_artifact.dat".to_string()],
            sandbox.task_id(),
        )
        .await;

    assert_eq!(
        collected.len(),
        0,
        "Single output artifact exceeding 10 MiB MUST NOT be collected"
    );
}

#[tokio::test]
async fn test_output_artifact_aggregate_limit_harvesting() {
    let (runner, _tmp) = make_test_runner();
    let temp_sb_dir = tempfile::tempdir().unwrap();
    let sb_cfg = SandboxConfig::new(temp_sb_dir.path());
    let sandbox = Sandbox::create(&sb_cfg, TaskId::new()).await.unwrap();

    // Create 4 files of 9 MiB each = 36 MiB total (> 32 MiB aggregate limit)
    let chunk = vec![0xCCu8; 1024 * 1024]; // 1 MiB chunk
    let mut expected_names = Vec::new();

    use tokio::io::AsyncWriteExt;
    for i in 1..=4 {
        let name = format!("chunk_{i}.bin");
        let path = sandbox.path().join(&name);
        let mut f = tokio::fs::File::create(&path).await.unwrap();
        for _ in 0..9 {
            f.write_all(&chunk).await.unwrap();
        }
        f.flush().await.unwrap();
        expected_names.push(name);
    }

    let collected = runner
        .harvest_output_artifacts(&sandbox, &expected_names, sandbox.task_id())
        .await;

    // First 3 files = 27 MiB (fits within 32 MiB). 4th file would be 36 MiB (> 32 MiB), so it must be stopped.
    assert!(
        collected.len() <= 3,
        "Aggregate collection must halt when exceeding 32 MiB aggregate limit. Collected: {}",
        collected.len()
    );

    let total_collected: u64 = collected.iter().map(|a| a.size).sum();
    assert!(
        total_collected <= MAX_AGGREGATE_ARTIFACT_BYTES as u64,
        "Total harvested artifacts bytes ({total_collected}) must not exceed 32 MiB ({MAX_AGGREGATE_ARTIFACT_BYTES})"
    );
}

// ============================================================================
// 4. Sanity Verification on Legitimate Paths
// ============================================================================

#[test]
fn test_legitimate_sandbox_paths_accepted() {
    let legitimate = [
        "file.txt",
        "data/input.csv",
        "models/weights/v1.0.bin",
        "configs/deep/nested/path/settings.json",
        "file_with_underscores_and-hyphens.dat",
        "README",
        "dir.with.dots/file.tar.gz",
    ];

    for path in legitimate {
        let norm_core = validate_relative_sandbox_path(path);
        assert!(
            norm_core.is_ok(),
            "Legitimate path '{}' was unexpectedly rejected by core: {:?}",
            path,
            norm_core
        );

        let norm_worker = normalize_relative_path_str(path);
        assert!(
            norm_worker.is_ok(),
            "Legitimate path '{}' was unexpectedly rejected by worker: {:?}",
            path,
            norm_worker
        );

        assert_eq!(
            norm_core.unwrap(),
            norm_worker.unwrap(),
            "Core and worker normalization must agree for legitimate path '{path}'"
        );
    }
}
