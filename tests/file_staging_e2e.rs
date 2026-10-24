//! End-to-End Integration Test Suite for Distributed File Staging and Output Artifact Collection (Milestone M5B).
//!
//! Validates:
//! 1. Single and multi-file staging into worker sandbox.
//! 2. Path traversal rejection (.., absolute paths, Windows drive/device prefixes).
//! 3. Windows DOS device names rejection (CON, PRN, AUX, NUL, COM1..9, LPT1..9).
//! 4. Content-hash deduplication cache at <sandbox_base>/cache/objects/<sha256>.
//! 5. Output artifact harvesting, size caps (10 MiB single, 32 MiB aggregate), and SHA-256 verification.
//! 6. Symlink escape defense (refusing to exfiltrate symlinks pointing outside sandbox).
//! 7. Wire codec roundtrips across JSON (0x01) and Bincode (0x02).
//! 8. Artifact size limits enforcement.
//! 9. Full end-to-end cluster submission with file staging and artifact retrieval.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::{
    deserialize_message, serialize_message, ClientMessage, ClientResponse, WireCodec, WorkerMessage,
};
use rusty_grid_core::task::{
    hex_encode, sha256_digest, Bytes, OutputArtifact, StagedFile, Task, TaskId, TaskRequirements,
    TaskResult, TaskSpec, MAX_AGGREGATE_ARTIFACT_BYTES, MAX_OUTPUT_ARTIFACT_BYTES,
};
use rusty_grid_master::{MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};
use rusty_grid_worker::runner::{RunnerConfig, TaskRunner};
use rusty_grid_worker::sandbox::{normalize_relative_path_str, Sandbox, SandboxConfig};

/// Test helper to create an isolated TaskRunner with a temporary sandbox directory.
fn make_test_runner() -> (TaskRunner, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().expect("create temp dir for runner");
    let sandbox_cfg = SandboxConfig::new(temp_dir.path());
    let runner_cfg = RunnerConfig::new(sandbox_cfg);
    let capabilities = WorkerCapabilities {
        name: "test-staging-worker".into(),
        cpu_cores: 4,
        ram_mb: 8192,
        has_gpu: false,
        is_simulated_gpu: false,
        gpu_device_name: None,
        tags: vec!["file_staging_test".into()],
        mobile: None,
        has_blender: false,
        blender_version: None,
        vram_mb: None,
        has_cuda: false,
    };
    let runner = TaskRunner::new(Uuid::new_v4(), capabilities, runner_cfg);
    (runner, temp_dir)
}

/// Helper: Cross-platform command to display file contents.
fn cat_file_spec(rel_path: &str) -> TaskSpec {
    #[cfg(windows)]
    {
        let win_path = rel_path.replace('/', "\\");
        TaskSpec::Command {
            program: "cmd.exe".into(),
            args: vec!["/C".into(), format!("type {win_path}")],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        }
    }
    #[cfg(not(windows))]
    {
        TaskSpec::Command {
            program: "cat".into(),
            args: vec![rel_path.into()],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        }
    }
}

/// Helper: Cross-platform command to create output files.
fn create_output_spec(filename: &str, content: &str) -> TaskSpec {
    #[cfg(windows)]
    {
        TaskSpec::Command {
            program: "cmd.exe".into(),
            args: vec!["/C".into(), format!("echo {content} > {filename}")],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        }
    }
    #[cfg(not(windows))]
    {
        TaskSpec::Command {
            program: "sh".into(),
            args: vec![
                "-c".into(),
                format!("printf '%s' '{content}' > '{filename}'"),
            ],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        }
    }
}

// ============================================================================
// 1. Single & Multi-File Staging Tests
// ============================================================================

#[tokio::test]
async fn test_single_file_staging_and_execution() {
    let (runner, _tmp) = make_test_runner();

    let content = b"OxideSwarm file staging verification content 12345";
    let staged = StagedFile::new("input.txt", Bytes::copy_from_slice(content), false);

    let mut task = Task::new(cat_file_spec("input.txt"), TaskRequirements::generic(1, 10));
    task.staged_files.push(staged);

    let result = runner.execute_task(&task, None, None).await;
    assert!(result.is_success(), "Task failed: {:?}", result.error);
    assert_eq!(result.exit_code, 0);
    let stdout = result.stdout_str();
    assert!(
        stdout.contains("OxideSwarm file staging verification"),
        "stdout was: {}",
        stdout
    );
}

#[tokio::test]
async fn test_multi_file_staging_nested_directories() {
    let (runner, _tmp) = make_test_runner();

    let file1 = StagedFile::new(
        "data/input.csv",
        Bytes::from_static(b"id,val\n1,100\n2,200\n"),
        false,
    );
    let file2 = StagedFile::new(
        "config/settings.json",
        Bytes::from_static(b"{\"debug\": true}"),
        false,
    );

    let mut task = Task::new(
        cat_file_spec("data/input.csv"),
        TaskRequirements::generic(1, 10),
    );
    task.staged_files.push(file1);
    task.staged_files.push(file2);

    let result = runner.execute_task(&task, None, None).await;
    assert!(result.is_success());
    let stdout = result.stdout_str();
    assert!(stdout.contains("id,val"));
    assert!(stdout.contains("1,100"));
}

// ============================================================================
// 2. Worker Content-Hash Cache & Deduplication Tests
// ============================================================================

#[tokio::test]
async fn test_worker_content_hash_cache_deduplication() {
    let (runner, tmp) = make_test_runner();

    let payload = vec![0x42u8; 64 * 1024]; // 64 KiB
    let digest = sha256_digest(&payload);
    let digest_hex = hex_encode(&digest);

    let staged1 = StagedFile::new("data1.bin", Bytes::copy_from_slice(&payload), false);
    let mut task1 = Task::new(
        TaskSpec::command("cmd.exe", vec!["/C".into(), "echo task1".into()]),
        TaskRequirements::generic(1, 10),
    );
    #[cfg(not(windows))]
    {
        task1.spec = TaskSpec::command("echo", vec!["task1".into()]);
    }
    task1.staged_files.push(staged1);

    let res1 = runner.execute_task(&task1, None, None).await;
    assert!(res1.is_success());

    // Verify cache entry was written to <sandbox_base>/cache/objects/<sha256>
    let cache_obj = tmp.path().join("cache").join("objects").join(&digest_hex);
    assert!(
        cache_obj.exists(),
        "Cache object must exist on disk: {:?}",
        cache_obj
    );
    let cached_bytes = tokio::fs::read(&cache_obj).await.unwrap();
    assert_eq!(cached_bytes.len(), 64 * 1024);
    assert_eq!(cached_bytes, payload);

    // Run Task 2 with identical payload
    let staged2 = StagedFile::new("data2.bin", Bytes::copy_from_slice(&payload), false);
    let mut task2 = Task::new(
        TaskSpec::command("cmd.exe", vec!["/C".into(), "echo task2".into()]),
        TaskRequirements::generic(1, 10),
    );
    #[cfg(not(windows))]
    {
        task2.spec = TaskSpec::command("echo", vec!["task2".into()]);
    }
    task2.staged_files.push(staged2);

    let res2 = runner.execute_task(&task2, None, None).await;
    assert!(res2.is_success());
}

// ============================================================================
// 3. Path Traversal & DOS Device Rejection Tests
// ============================================================================

#[test]
fn test_path_traversal_rejection_unit() {
    let bad_paths = [
        "../evil.txt",
        "..\\evil.txt",
        "sub/../../evil.txt",
        "sub\\..\\..\\evil.txt",
        "/etc/passwd",
        "\\Windows\\System32\\cmd.exe",
        "C:\\Windows\\cmd.exe",
        "c:/test.txt",
        "D:foo.txt",
        "data/null\0byte.txt",
        "",
        ".",
    ];

    for path in bad_paths {
        let res = normalize_relative_path_str(path);
        assert!(
            res.is_err(),
            "Path '{}' should be rejected, but was accepted: {:?}",
            path,
            res
        );
    }
}

#[test]
fn test_windows_dos_device_names_rejection() {
    let dos_devices = [
        "CON",
        "con",
        "con.txt",
        "CON.DAT",
        "PRN",
        "prn.log",
        "AUX",
        "aux.json",
        "NUL",
        "nul.txt",
        "nul ",
        "COM1",
        "com1.txt",
        "COM2",
        "COM9",
        "LPT1",
        "lpt1.txt",
        "LPT9",
        "sub/CON",
        "nested/dir/aux.txt",
    ];

    for dev in dos_devices {
        let res = normalize_relative_path_str(dev);
        assert!(
            res.is_err(),
            "Device name '{}' must be rejected: {:?}",
            dev,
            res
        );
        let err_msg = res.unwrap_err().to_string();
        assert!(
            err_msg.contains("reserved") || err_msg.contains("traversal"),
            "Error message should name violation: {}",
            err_msg
        );
    }
}

// ============================================================================
// 4. Output Artifact Harvesting Tests
// ============================================================================

#[tokio::test]
async fn test_output_artifact_harvesting_and_sha256() {
    let (runner, _tmp) = make_test_runner();

    let task_spec = create_output_spec("summary.txt", "ComputationResults2026");
    let mut task = Task::new(task_spec, TaskRequirements::generic(1, 10));
    task.expected_outputs.push("summary.txt".into());
    task.expected_outputs.push("non_existent.txt".into()); // should be ignored gracefully

    let result = runner.execute_task(&task, None, None).await;
    assert!(result.is_success());
    assert_eq!(
        result.artifacts.len(),
        1,
        "Should collect exactly 1 artifact"
    );

    let artifact = &result.artifacts[0];
    assert_eq!(artifact.path, "summary.txt");
    assert!(artifact.size > 0);
    assert_eq!(artifact.size, artifact.data.len() as u64);

    let expected_digest = sha256_digest(artifact.data.as_bytes());
    assert_eq!(artifact.sha256, expected_digest);
    assert!(artifact.verify_checksum());
}

#[tokio::test]
async fn test_output_artifact_symlink_defense() {
    #[cfg(unix)]
    {
        let (runner, tmp) = make_test_runner();

        // Create an external secret file outside the sandbox
        let external_secret = tmp.path().join("external_secret.txt");
        tokio::fs::write(&external_secret, b"CLASSIFIED_DATA")
            .await
            .unwrap();

        // Task creates a symlink pointing to the external file
        let ext_str = external_secret.to_string_lossy().to_string();
        let script = format!("ln -s {ext_str} leak.txt");
        let task_spec = TaskSpec::ShellScript {
            script,
            interpreter: None,
            env: HashMap::new(),
        };

        let mut task = Task::new(task_spec, TaskRequirements::generic(1, 10));
        task.expected_outputs.push("leak.txt".into());

        let result = runner.execute_task(&task, None, None).await;
        assert_eq!(
            result.artifacts.len(),
            0,
            "Symlink to external file MUST NOT be collected as an artifact"
        );
    }
}

// ============================================================================
// 5. Artifact Size Limits Enforcement Tests
// ============================================================================

#[tokio::test]
async fn test_artifact_size_limits_enforcement() {
    let (runner, _tmp) = make_test_runner();

    // Verify limit constants match requirement specification
    assert_eq!(MAX_OUTPUT_ARTIFACT_BYTES, 10 * 1024 * 1024);
    assert_eq!(MAX_AGGREGATE_ARTIFACT_BYTES, 32 * 1024 * 1024);

    // Test harvesting ignores oversized file
    let temp_sb_dir = tempfile::tempdir().unwrap();
    let sb_cfg = SandboxConfig::new(temp_sb_dir.path());
    let sandbox = Sandbox::create(&sb_cfg, TaskId::new()).await.unwrap();

    // Create a 11 MiB file (exceeds 10 MiB limit)
    let oversized_data = vec![0u8; 11 * 1024 * 1024];
    let oversized_path = sandbox.path().join("big_file.bin");
    tokio::fs::write(&oversized_path, &oversized_data)
        .await
        .unwrap();

    let collected = runner
        .harvest_output_artifacts(&sandbox, &["big_file.bin".to_string()], sandbox.task_id())
        .await;

    assert_eq!(
        collected.len(),
        0,
        "File exceeding 10 MiB must be skipped during harvesting"
    );
}

// ============================================================================
// 6. Wire Codec Roundtrip Compatibility Tests
// ============================================================================

#[test]
fn test_wire_codecs_roundtrip_staged_files_and_artifacts() {
    let mut task = Task::new(
        TaskSpec::command("echo", vec!["codec_test".into()]),
        TaskRequirements::generic(2, 60),
    );
    task.staged_files.push(StagedFile::new(
        "model.bin",
        Bytes::copy_from_slice(b"WIDGET_WEIGHTS_BINARY"),
        false,
    ));
    task.expected_outputs.push("metrics.json".into());

    let mut result = TaskResult::success(Uuid::new_v4(), task.id, "ok", 123, false);
    result.artifacts.push(OutputArtifact::new(
        "metrics.json",
        Bytes::copy_from_slice(b"{\"accuracy\": 0.99}"),
    ));

    // 1. JSON Codec (0x01)
    let json_worker_msg = WorkerMessage::from(result.clone());
    let encoded_json = serialize_message(&json_worker_msg, WireCodec::Json).unwrap();
    assert_eq!(encoded_json[0], 0x01);
    let (decoded_json, codec) = deserialize_message::<WorkerMessage>(&encoded_json).unwrap();
    assert_eq!(codec, WireCodec::Json);
    match decoded_json {
        WorkerMessage::TaskResult { artifacts, .. } => {
            assert_eq!(artifacts.len(), 1);
            assert_eq!(artifacts[0].path, "metrics.json");
            assert_eq!(artifacts[0].data.as_bytes(), b"{\"accuracy\": 0.99}");
            assert!(artifacts[0].verify_checksum());
        }
        _ => panic!("Expected TaskResult"),
    }

    // 2. Bincode Codec (0x02)
    let encoded_bin = serialize_message(&json_worker_msg, WireCodec::Bincode).unwrap();
    assert_eq!(encoded_bin[0], 0x02);
    let (decoded_bin, codec) = deserialize_message::<WorkerMessage>(&encoded_bin).unwrap();
    assert_eq!(codec, WireCodec::Bincode);
    match decoded_bin {
        WorkerMessage::TaskResult { artifacts, .. } => {
            assert_eq!(artifacts.len(), 1);
            assert_eq!(artifacts[0].path, "metrics.json");
            assert_eq!(artifacts[0].data.as_bytes(), b"{\"accuracy\": 0.99}");
            assert!(artifacts[0].verify_checksum());
        }
        _ => panic!("Expected TaskResult"),
    }
}

// ============================================================================
// 7. CLI Helper Unit Tests
// ============================================================================

#[test]
fn test_cli_parse_file_spec() {
    use rusty_grid_cli::connect::{parse_file_spec, validate_output_paths};

    // Bare relative
    let (local, sandbox) = parse_file_spec("data.csv").unwrap();
    assert_eq!(local, PathBuf::from("data.csv"));
    assert_eq!(sandbox, "data.csv");

    // Explicit mapping
    let (local, sandbox) = parse_file_spec("local/file.txt:remote/dir/file.txt").unwrap();
    assert_eq!(local, PathBuf::from("local/file.txt"));
    assert_eq!(sandbox, "remote/dir/file.txt");

    // Windows drive letter
    let (local, sandbox) = parse_file_spec("C:\\data\\test.csv:target.csv").unwrap();
    assert_eq!(local, PathBuf::from("C:\\data\\test.csv"));
    assert_eq!(sandbox, "target.csv");

    // Output paths validation
    let valid = validate_output_paths(&["result.txt".into(), "nested/output.bin".into()]).unwrap();
    assert_eq!(valid, vec!["result.txt", "nested/output.bin"]);

    assert!(validate_output_paths(&["../leak.txt".into()]).is_err());
    assert!(validate_output_paths(&["CON.txt".into()]).is_err());
}

#[tokio::test]
async fn test_cli_save_output_artifacts() {
    use rusty_grid_cli::connect::save_output_artifacts;

    let temp_dest = tempfile::tempdir().unwrap();
    let artifacts = vec![
        OutputArtifact::new(
            "reports/summary.txt",
            Bytes::from_static(b"All Tests Passed"),
        ),
        OutputArtifact::new("data.bin", Bytes::from_static(b"\x00\x01\x02\x03")),
    ];

    let saved = save_output_artifacts(&artifacts, temp_dest.path()).unwrap();
    assert_eq!(saved.len(), 2);

    let summary_path = temp_dest.path().join("reports").join("summary.txt");
    assert!(summary_path.exists());
    let summary_content = tokio::fs::read_to_string(&summary_path).await.unwrap();
    assert_eq!(summary_content, "All Tests Passed");

    let data_path = temp_dest.path().join("data.bin");
    assert!(data_path.exists());
    let data_content = tokio::fs::read(&data_path).await.unwrap();
    assert_eq!(data_content, b"\x00\x01\x02\x03");
}

#[tokio::test]
async fn test_e2e_full_cluster_file_staging_and_artifact_retrieval() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr();

    let temp_worker_dir = tempfile::tempdir().expect("worker tempdir");
    let worker_cfg = WorkerConfig::new(master_addr.to_string())
        .with_name("e2e-staging-worker")
        .with_cores(2)
        .with_ram_mb(4096)
        .with_sandbox_base_dir(temp_worker_dir.path());

    let mut client = WorkerClient::new(worker_cfg);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let worker_handle = tokio::spawn(async move {
        let _ = client.run(shutdown_rx).await;
    });

    // Wait briefly for worker registration
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Connect client
    let mut transport = rusty_grid_cli::connect_master(&master_addr.to_string(), None)
        .await
        .expect("connect to master");

    // Build task with staged input file and expected output
    let staged_input = StagedFile::new("numbers.txt", Bytes::from_static(b"10\n20\n30\n"), false);
    let task_spec = create_output_spec("processed.txt", "PROCESSED_SUM_60");
    let mut task = Task::new(task_spec, TaskRequirements::generic(1, 10));
    task.staged_files.push(staged_input);
    task.expected_outputs.push("processed.txt".into());

    let submit_msg = ClientMessage::SubmitTask { task, wait: true };
    transport.send_msg(&submit_msg).await.unwrap();

    let resp = transport.recv_msg::<ClientResponse>().await.unwrap();
    match resp {
        Some(ClientResponse::TaskCompleted { task_id: _, result }) => {
            assert!(result.is_success(), "Task failed: {:?}", result.error);
            assert_eq!(result.artifacts.len(), 1);
            let artifact = &result.artifacts[0];
            assert_eq!(artifact.path, "processed.txt");
            assert!(artifact.verify_checksum());
            let data_str = String::from_utf8_lossy(artifact.data.as_bytes());
            assert!(
                data_str.contains("PROCESSED_SUM_60"),
                "artifact content was: {}",
                data_str
            );

            // Test saving artifact via CLI helper
            let temp_client_dir = tempfile::tempdir().unwrap();
            let saved = rusty_grid_cli::connect::save_output_artifacts(
                &result.artifacts,
                temp_client_dir.path(),
            )
            .unwrap();
            assert_eq!(saved.len(), 1);
            assert!(saved[0].exists());
            let content = tokio::fs::read_to_string(&saved[0]).await.unwrap();
            assert!(content.contains("PROCESSED_SUM_60"));
        }
        other => panic!("Expected TaskCompleted, got {other:?}"),
    }

    let _ = shutdown_tx.send(true);
    let _ = worker_handle.await;
    let _ = master.shutdown();
}
