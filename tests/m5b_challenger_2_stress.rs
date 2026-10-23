//! Adversarial Stress Test Suite for Milestone M5B (Challenger 2).
//!
//! Stress-tests:
//! 1. Worker content-hash deduplication, cache object directory layout, hardlinks, and distinct content isolation.
//! 2. Concurrent cache staging under race conditions.
//! 3. Symlink escape defense, canonicalization checks, and path traversal rejection in `harvest_output_artifacts`.
//! 4. Multi-codec wire serialization across JSON (0x01), Bincode (0x02), and AEAD (0x04) with tamper detection.
//! 5. Master Queue LRU retention accounting for staged files, expected outputs, and artifacts, enforcing budget eviction.

use std::sync::Arc;

use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::{
    derive_aead_keys, deserialize_message, serialize_message, EndpointRole, MasterMessage,
    MessageTransport, WireCodec, WorkerMessage, WIRE_FORMAT_BINCODE, WIRE_FORMAT_ENCRYPTED,
    WIRE_FORMAT_JSON,
};
use rusty_grid_core::task::{
    hex_encode, sha256_digest, Bytes, OutputArtifact, StagedFile, Task, TaskId, TaskRequirements,
    TaskResult, TaskSpec,
};
use rusty_grid_master::queue::{RetryPolicy, TaskQueue, TaskState, DEFAULT_MAX_TERMINAL_BYTES};
use rusty_grid_worker::runner::{RunnerConfig, TaskRunner};
use rusty_grid_worker::sandbox::{normalize_relative_path_str, Sandbox, SandboxConfig};

/// Helper to create a test runner with temporary directory.
fn make_test_runner() -> (TaskRunner, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().expect("create temp dir for runner");
    let sandbox_cfg = SandboxConfig::new(temp_dir.path());
    let runner_cfg = RunnerConfig::new(sandbox_cfg);
    let capabilities = WorkerCapabilities {
        name: "challenger-2-worker".into(),
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
// 1. Cache Deduplication, Hardlink Acceleration & Content Isolation
// ============================================================================

#[tokio::test]
async fn test_cache_deduplication_and_hardlink_acceleration() {
    let temp_dir = tempfile::tempdir().expect("create temp dir for cache test");
    let base_dir = temp_dir.path();
    let cache_dir = base_dir.join("cache");
    let objects_dir = cache_dir.join("objects");

    let sb_cfg = SandboxConfig::new(base_dir);

    // Payload A
    let content_a = b"OXIDE_SWARM_IMMUTABLE_MODEL_WEIGHTS_V1_TEST_DATA";
    let sha256_a = sha256_digest(content_a);
    let sha256_a_hex = hex_encode(&sha256_a);

    let staged_a = StagedFile::new(
        "models/weights.bin",
        Bytes::copy_from_slice(content_a),
        false,
    );

    // --- Task 1: First staging creates cache object ---
    let task_id_1 = TaskId::new();
    let sandbox_1 = Sandbox::create(&sb_cfg, task_id_1)
        .await
        .expect("create sandbox 1");
    sandbox_1
        .stage_files(std::slice::from_ref(&staged_a), &cache_dir)
        .await
        .expect("stage files task 1");

    let cache_obj_a = objects_dir.join(&sha256_a_hex);
    assert!(
        cache_obj_a.exists(),
        "Cache object must exist at <sandbox_base>/cache/objects/<sha256>"
    );
    let cached_bytes = tokio::fs::read(&cache_obj_a)
        .await
        .expect("read cached object A");
    assert_eq!(cached_bytes, content_a);

    let staged_file_1 = sandbox_1.path().join("models").join("weights.bin");
    assert!(
        staged_file_1.exists(),
        "Staged file must exist in sandbox 1"
    );

    // --- Task 2: Consecutive staging with identical content reuses cache ---
    let task_id_2 = TaskId::new();
    let sandbox_2 = Sandbox::create(&sb_cfg, task_id_2)
        .await
        .expect("create sandbox 2");
    sandbox_2
        .stage_files(std::slice::from_ref(&staged_a), &cache_dir)
        .await
        .expect("stage files task 2");

    let staged_file_2 = sandbox_2.path().join("models").join("weights.bin");
    assert!(
        staged_file_2.exists(),
        "Staged file must exist in sandbox 2"
    );
    let read_file_2 = tokio::fs::read(&staged_file_2)
        .await
        .expect("read staged file 2");
    assert_eq!(read_file_2, content_a);

    // Verify hardlink or copy fallback:
    // If the filesystem supports hardlinks, the link count of the cache object should be >= 2.
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let meta = std::fs::metadata(&cache_obj_a).expect("metadata of cache obj");
        if meta.nlink() >= 2 {
            println!(
                "Confirmed hardlink deduplication on Unix (nlink = {})",
                meta.nlink()
            );
        }
    }
    #[cfg(windows)]
    {
        // On Windows, verify cache object metadata exists and matches expected size
        if let Ok(meta) = std::fs::metadata(&cache_obj_a) {
            assert_eq!(meta.len(), content_a.len() as u64);
        }
    }

    // --- Task 3: Modified content produces distinct SHA-256 and separate cache entry ---
    let content_b = b"OXIDE_SWARM_IMMUTABLE_MODEL_WEIGHTS_V2_MUTATED_DATA";
    let sha256_b = sha256_digest(content_b);
    let sha256_b_hex = hex_encode(&sha256_b);
    assert_ne!(
        sha256_a_hex, sha256_b_hex,
        "Mutated content must produce distinct SHA-256"
    );

    let staged_b = StagedFile::new(
        "models/weights.bin",
        Bytes::copy_from_slice(content_b),
        false,
    );

    let task_id_3 = TaskId::new();
    let sandbox_3 = Sandbox::create(&sb_cfg, task_id_3)
        .await
        .expect("create sandbox 3");
    sandbox_3
        .stage_files(&[staged_b], &cache_dir)
        .await
        .expect("stage files task 3");

    let cache_obj_b = objects_dir.join(&sha256_b_hex);
    assert!(
        cache_obj_b.exists(),
        "Mutated content must create a distinct cache entry at <sandbox_base>/cache/objects/<sha256_b>"
    );
    let cached_bytes_b = tokio::fs::read(&cache_obj_b)
        .await
        .expect("read cached object B");
    assert_eq!(cached_bytes_b, content_b);

    // Verify object A remains untouched and uncorrupted
    let cached_bytes_a_after = tokio::fs::read(&cache_obj_a)
        .await
        .expect("read cached object A after B");
    assert_eq!(cached_bytes_a_after, content_a);
}

#[tokio::test]
async fn test_concurrent_cache_staging_race_conditions() {
    let temp_dir = tempfile::tempdir().expect("create temp dir for concurrent cache");
    let base_dir = Arc::new(temp_dir.path().to_path_buf());
    let cache_dir = base_dir.join("cache");

    let content = b"CONCURRENT_CONTENT_SHARED_ACROSS_WORKERS_1234567890";
    let staged = StagedFile::new("shared.dat", Bytes::copy_from_slice(content), false);

    // Concurrently stage the exact same file in 10 different sandboxes
    let mut handles = Vec::new();
    for i in 0..10 {
        let base = Arc::clone(&base_dir);
        let c_dir = cache_dir.clone();
        let file = staged.clone();
        let handle = tokio::spawn(async move {
            let sb_cfg = SandboxConfig::new(&*base);
            let task_id = TaskId::new();
            let sandbox = Sandbox::create(&sb_cfg, task_id)
                .await
                .expect("create sandbox");
            sandbox
                .stage_files(&[file], &c_dir)
                .await
                .expect("stage file");

            let target = sandbox.path().join("shared.dat");
            let read_back = tokio::fs::read(&target).await.expect("read back file");
            assert_eq!(read_back, content);
            i
        });
        handles.push(handle);
    }

    for handle in handles {
        let i = handle.await.expect("task thread join");
        assert!(i < 10);
    }

    // Verify cache object exists and is intact
    let sha256_hex = hex_encode(&sha256_digest(content));
    let cache_obj = cache_dir.join("objects").join(&sha256_hex);
    assert!(cache_obj.exists());
    let cached_content = tokio::fs::read(&cache_obj).await.expect("read cache obj");
    assert_eq!(cached_content, content);
}

// ============================================================================
// 2. Symlink Escape Defense & Path Traversal Prevention
// ============================================================================

#[tokio::test]
async fn test_symlink_escape_prevention_in_harvest() {
    let (runner, tmp) = make_test_runner();
    let sandbox_cfg = SandboxConfig::new(tmp.path());
    let task_id = TaskId::new();
    let sandbox = Sandbox::create(&sandbox_cfg, task_id)
        .await
        .expect("create sandbox");

    // 1. Create a host secret file outside the sandbox root
    let external_secret_dir = tmp.path().join("host_secrets");
    tokio::fs::create_dir_all(&external_secret_dir)
        .await
        .expect("create external secret dir");
    let external_secret_file = external_secret_dir.join("confidential_keys.pem");
    tokio::fs::write(
        &external_secret_file,
        b"TOP_SECRET_CLUSTER_PRIVATE_KEY_DATA",
    )
    .await
    .expect("write external secret");

    // 2. Attempt to create a symlink inside the sandbox pointing to the external file
    let symlink_path = sandbox.path().join("stolen_secret.pem");

    #[cfg(unix)]
    let symlink_created =
        { std::os::unix::fs::symlink(&external_secret_file, &symlink_path).is_ok() };

    #[cfg(windows)]
    let symlink_created =
        { std::os::windows::fs::symlink_file(&external_secret_file, &symlink_path).is_ok() };

    if symlink_created {
        // Symlink was created: harvest_output_artifacts MUST detect and reject it!
        let collected = runner
            .harvest_output_artifacts(
                &sandbox,
                &["stolen_secret.pem".to_string()],
                sandbox.task_id(),
            )
            .await;

        assert_eq!(
            collected.len(),
            0,
            "Security vulnerability: harvest_output_artifacts harvested an external symlink!"
        );
    } else {
        println!(
            "Note: Symlink creation skipped due to OS security privileges (Windows non-admin)"
        );
    }

    // 3. Attempt direct path traversal in expected_outputs
    let adversarial_traversal_paths = vec![
        "../host_secrets/confidential_keys.pem".to_string(),
        "../../confidential_keys.pem".to_string(),
        "/etc/passwd".to_string(),
        "C:\\Windows\\win.ini".to_string(),
        "CON".to_string(),
        "NUL".to_string(),
        "sub/../CON".to_string(),
        "data:stream.txt".to_string(),
    ];

    let collected_traversal = runner
        .harvest_output_artifacts(&sandbox, &adversarial_traversal_paths, sandbox.task_id())
        .await;

    assert_eq!(
        collected_traversal.len(),
        0,
        "All path traversal attempts in expected_outputs must be rejected"
    );

    // 4. Verify legitimate regular file is still harvested correctly
    let legit_path = sandbox.path().join("results.json");
    tokio::fs::write(&legit_path, b"{\"computation\": 42}")
        .await
        .expect("write legit output");

    let collected_legit = runner
        .harvest_output_artifacts(&sandbox, &["results.json".to_string()], sandbox.task_id())
        .await;

    assert_eq!(collected_legit.len(), 1);
    assert_eq!(collected_legit[0].path, "results.json");
    assert_eq!(collected_legit[0].data.as_bytes(), b"{\"computation\": 42}");
    assert!(collected_legit[0].verify_checksum());
}

#[test]
fn test_adversarial_path_sanitizer_edge_cases() {
    let malicious = [
        "../secret.txt",
        "..\\secret.txt",
        "/etc/shadow",
        "\\Windows\\System32",
        "C:/autoexec.bat",
        "D:\\data.bin",
        "file/with/null\0byte",
        "nested/../../escape",
        "dir/CON",
        "AUX.json",
        "PRN",
        "COM1",
        "LPT9.txt",
        ":alternate_stream",
        "stream:data",
        "",
        ".",
        "./.",
        "./",
        "/",
        "\\",
    ];

    for path in malicious {
        let res = normalize_relative_path_str(path);
        assert!(
            res.is_err(),
            "Path '{}' MUST be rejected as an illegal sandbox path",
            path
        );
    }

    // Valid paths must be accepted and normalized
    let valid_pairs = [
        ("simple.txt", "simple.txt"),
        ("nested/dir/data.csv", "nested/dir/data.csv"),
        ("windows\\style\\path.bin", "windows/style/path.bin"),
        ("./current/dir/file.txt", "current/dir/file.txt"),
    ];

    for (raw, expected) in valid_pairs {
        let normalized = normalize_relative_path_str(raw).expect("valid path");
        assert_eq!(normalized, expected);
    }
}

// ============================================================================
// 3. Multi-Codec Wire Roundtrips (JSON 0x01, Bincode 0x02, AEAD 0x04)
// ============================================================================

#[tokio::test]
async fn test_multicodec_wire_roundtrips_json_bincode_aead() {
    // Construct rich, realistic Task with multiple staged files
    let mut task = Task::new(
        TaskSpec::command(
            "python3",
            vec![
                "-u".into(),
                "pipeline.py".into(),
                "--batch-size".into(),
                "64".into(),
            ],
        ),
        TaskRequirements::generic(4, 16384),
    );
    task.tags = vec![
        "high_priority".into(),
        "require:gpu_vendor=nvidia".into(),
        "tag_with_special_chars=!@#$%^&*()_+=-`~".into(),
    ];

    // File 1: Text configuration
    task.staged_files.push(StagedFile::new(
        "config/pipeline.json",
        Bytes::copy_from_slice(b"{\"learning_rate\": 0.001, \"epochs\": 50}"),
        false,
    ));

    // File 2: Binary dataset
    let binary_data: Vec<u8> = (0..2048).map(|i| (i % 256) as u8).collect();
    task.staged_files.push(StagedFile::new(
        "data/dataset.bin",
        Bytes::copy_from_slice(&binary_data),
        false,
    ));

    // File 3: Executable script
    task.staged_files.push(StagedFile::new(
        "scripts/run.sh",
        Bytes::copy_from_slice(b"#!/bin/bash\nexec python3 pipeline.py\n"),
        true,
    ));

    task.expected_outputs = vec![
        "output/model.onnx".into(),
        "output/metrics.csv".into(),
        "logs/run.log".into(),
    ];

    // Construct rich TaskResult with multiple output artifacts
    let worker_id = Uuid::new_v4();
    let mut task_result = TaskResult::success(
        worker_id,
        task.id,
        "Epoch 50/50 - accuracy: 0.985 - loss: 0.034\n",
        4520,
        true,
    );
    task_result.device_name = Some("NVIDIA RTX 4090 D".into());

    let artifact_data_1: Vec<u8> = (0..4096).map(|i| ((i * 7) % 256) as u8).collect();
    task_result.artifacts.push(OutputArtifact::new(
        "output/model.onnx",
        Bytes::copy_from_slice(&artifact_data_1),
    ));

    task_result.artifacts.push(OutputArtifact::new(
        "output/metrics.csv",
        Bytes::copy_from_slice(b"epoch,loss,acc\n50,0.034,0.985\n"),
    ));

    task_result.artifacts.push(OutputArtifact::new(
        "logs/run.log",
        Bytes::copy_from_slice(b"[INFO] Training complete without warnings.\n"),
    ));

    let worker_msg = WorkerMessage::from(task_result.clone());
    let master_msg = MasterMessage::AssignTask { task: task.clone() };

    // --- Wire Codec 1: JSON (0x01) ---
    {
        // Test WorkerMessage (TaskResult) over JSON
        let encoded_w =
            serialize_message(&worker_msg, WireCodec::Json).expect("serialize worker msg json");
        assert_eq!(encoded_w[0], WIRE_FORMAT_JSON);
        let (decoded_w, codec_w) =
            deserialize_message::<WorkerMessage>(&encoded_w).expect("deserialize worker msg json");
        assert_eq!(codec_w, WireCodec::Json);
        assert_eq!(decoded_w, worker_msg);

        // Test MasterMessage (AssignTask with staged files) over JSON
        let encoded_m =
            serialize_message(&master_msg, WireCodec::Json).expect("serialize master msg json");
        assert_eq!(encoded_m[0], WIRE_FORMAT_JSON);
        let (decoded_m, codec_m) =
            deserialize_message::<MasterMessage>(&encoded_m).expect("deserialize master msg json");
        assert_eq!(codec_m, WireCodec::Json);
        assert_eq!(decoded_m, master_msg);
    }

    // --- Wire Codec 2: Bincode (0x02) ---
    {
        // Test WorkerMessage (TaskResult) over Bincode
        let encoded_w = serialize_message(&worker_msg, WireCodec::Bincode)
            .expect("serialize worker msg bincode");
        assert_eq!(encoded_w[0], WIRE_FORMAT_BINCODE);
        let (decoded_w, codec_w) = deserialize_message::<WorkerMessage>(&encoded_w)
            .expect("deserialize worker msg bincode");
        assert_eq!(codec_w, WireCodec::Bincode);
        assert_eq!(decoded_w, worker_msg);

        // Test MasterMessage (AssignTask with staged files) over Bincode
        let encoded_m = serialize_message(&master_msg, WireCodec::Bincode)
            .expect("serialize master msg bincode");
        assert_eq!(encoded_m[0], WIRE_FORMAT_BINCODE);
        let (decoded_m, codec_m) = deserialize_message::<MasterMessage>(&encoded_m)
            .expect("deserialize master msg bincode");
        assert_eq!(codec_m, WireCodec::Bincode);
        assert_eq!(decoded_m, master_msg);
    }

    // --- Wire Codec 3: AEAD Encrypted (0x04) ---
    {
        let session_key = [0x5au8; 32];
        let (mut client_writer, mut client_reader) =
            derive_aead_keys(&session_key, EndpointRole::Client);
        let (mut server_writer, mut server_reader) =
            derive_aead_keys(&session_key, EndpointRole::Server);

        // Client -> Server: serialize WorkerMessage, encrypt, decrypt, deserialize
        let plaintext_w =
            serialize_message(&worker_msg, WireCodec::Bincode).expect("bincode plaintext");
        let encrypted_frame_w = client_writer
            .encrypt_frame(&plaintext_w)
            .expect("encrypt client frame");
        assert_eq!(encrypted_frame_w[0], WIRE_FORMAT_ENCRYPTED);

        let decrypted_frame_w = server_reader
            .decrypt_frame(&encrypted_frame_w)
            .expect("decrypt server frame");
        assert_eq!(decrypted_frame_w, plaintext_w);
        let (decoded_aead_w, codec) = deserialize_message::<WorkerMessage>(&decrypted_frame_w)
            .expect("deserialize decrypted frame");
        assert_eq!(codec, WireCodec::Bincode);
        assert_eq!(decoded_aead_w, worker_msg);

        // Server -> Client: serialize MasterMessage, encrypt, decrypt, deserialize
        let plaintext_m = serialize_message(&master_msg, WireCodec::Json).expect("json plaintext");
        let encrypted_frame_m = server_writer
            .encrypt_frame(&plaintext_m)
            .expect("encrypt server frame");
        assert_eq!(encrypted_frame_m[0], WIRE_FORMAT_ENCRYPTED);

        let decrypted_frame_m = client_reader
            .decrypt_frame(&encrypted_frame_m)
            .expect("decrypt client frame");
        assert_eq!(decrypted_frame_m, plaintext_m);
        let (decoded_aead_m, codec) = deserialize_message::<MasterMessage>(&decrypted_frame_m)
            .expect("deserialize decrypted master frame");
        assert_eq!(codec, WireCodec::Json);
        assert_eq!(decoded_aead_m, master_msg);

        // Tamper test: modifying a byte in ciphertext must cause decryption error
        let mut tampered_frame = encrypted_frame_m.to_vec();
        let tamper_idx = tampered_frame.len() - 5;
        tampered_frame[tamper_idx] ^= 0xff;
        let tamper_res = client_reader.decrypt_frame(&tampered_frame);
        assert!(
            tamper_res.is_err(),
            "AEAD decryption must fail on tampered frame"
        );
    }

    // --- Duplex Transport Stream Verification ---
    {
        let session_key = [0x77u8; 32];
        let (client_io, server_io) = tokio::io::duplex(64 * 1024 * 1024);

        let mut client_transport = MessageTransport::with_codec(client_io, WireCodec::Bincode);
        let mut server_transport = MessageTransport::with_codec(server_io, WireCodec::Bincode);

        client_transport.enable_encryption(&session_key, EndpointRole::Client);
        server_transport.enable_encryption(&session_key, EndpointRole::Server);

        // Send worker message over encrypted duplex stream
        client_transport
            .send_msg(&worker_msg)
            .await
            .expect("send msg over transport");
        let received = server_transport
            .recv_msg::<WorkerMessage>()
            .await
            .expect("recv msg over transport")
            .expect("message received");

        assert_eq!(received, worker_msg);
    }
}

// ============================================================================
// 4. Master Queue LRU Retention & 256 MiB Budget Eviction
// ============================================================================

#[tokio::test]
async fn test_master_queue_lru_retention_accounting_and_eviction() {
    // 1. Verify default 256 MiB constant
    assert_eq!(
        DEFAULT_MAX_TERMINAL_BYTES,
        256 * 1024 * 1024,
        "Default terminal retention budget must be exactly 256 MiB"
    );

    // 2. Setup a queue with a compact budget of 60 KiB to precisely verify eviction
    //    Each task with a 30 KiB staged file + 10 KiB artifact charges > 40 KiB.
    //    Thus, two completed tasks exceed 60 KiB, forcing the oldest to be evicted!
    let budget_bytes = 60 * 1024;
    let queue = TaskQueue::with_retention_limits(RetryPolicy::default(), None, 1000, budget_bytes);
    let worker_id = Uuid::new_v4();

    // Task 1: Staged file (25 KB) + Artifact (15 KB) => ~41 KB
    let mut task_1 = Task::new(
        TaskSpec::command("echo", vec!["task_1".into()]),
        TaskRequirements::generic(1, 10),
    );
    let file_data_1 = vec![0x11u8; 25 * 1024];
    task_1.staged_files.push(StagedFile::new(
        "data/input_1.bin",
        Bytes::copy_from_slice(&file_data_1),
        false,
    ));
    task_1.expected_outputs.push("output/result_1.bin".into());

    let tid_1 = queue.submit(task_1).await.expect("submit task 1");
    let _ = queue
        .pop_and_schedule(worker_id, |_| true)
        .await
        .expect("schedule task 1");
    queue
        .mark_running(&tid_1, worker_id)
        .await
        .expect("mark running task 1");

    let mut res_1 = TaskResult::success(worker_id, tid_1, "ok", 10, false);
    let art_data_1 = vec![0x22u8; 15 * 1024];
    res_1.artifacts.push(OutputArtifact::new(
        "output/result_1.bin",
        Bytes::copy_from_slice(&art_data_1),
    ));
    queue
        .record_result(res_1)
        .await
        .expect("record result task 1");

    // Task 1 completed, must be retained
    assert_eq!(queue.get_state(&tid_1).await, Some(TaskState::Completed));
    assert!(queue.get_task(&tid_1).await.is_some());
    assert!(queue.get_result(&tid_1).await.is_some());

    // Task 2: Staged file (25 KB) + Artifact (15 KB) => ~41 KB
    let mut task_2 = Task::new(
        TaskSpec::command("echo", vec!["task_2".into()]),
        TaskRequirements::generic(1, 10),
    );
    let file_data_2 = vec![0x33u8; 25 * 1024];
    task_2.staged_files.push(StagedFile::new(
        "data/input_2.bin",
        Bytes::copy_from_slice(&file_data_2),
        false,
    ));
    task_2.expected_outputs.push("output/result_2.bin".into());

    let tid_2 = queue.submit(task_2).await.expect("submit task 2");
    let _ = queue
        .pop_and_schedule(worker_id, |_| true)
        .await
        .expect("schedule task 2");
    queue
        .mark_running(&tid_2, worker_id)
        .await
        .expect("mark running task 2");

    let mut res_2 = TaskResult::success(worker_id, tid_2, "ok", 10, false);
    let art_data_2 = vec![0x44u8; 15 * 1024];
    res_2.artifacts.push(OutputArtifact::new(
        "output/result_2.bin",
        Bytes::copy_from_slice(&art_data_2),
    ));
    queue
        .record_result(res_2)
        .await
        .expect("record result task 2");

    // Total terminal bytes ~82 KB > 60 KB budget!
    // Task 1 (oldest terminal task) MUST be evicted to respect byte budget.
    // Task 2 (newest task) MUST be retained.
    assert!(
        queue.get_task(&tid_1).await.is_none(),
        "Task 1 must be evicted because retained bytes exceeded the 60 KiB budget"
    );
    assert!(
        queue.get_result(&tid_1).await.is_none(),
        "Evicted task results must no longer be retained"
    );
    assert_eq!(
        queue.get_state(&tid_2).await,
        Some(TaskState::Completed),
        "Task 2 must be retained"
    );
    assert!(
        queue.get_task(&tid_2).await.is_some(),
        "Task 2 metadata must be retained"
    );
    assert!(
        queue.get_result(&tid_2).await.is_some(),
        "Task 2 result and artifacts must be retained"
    );

    // Task 3: Another task pushing Task 2 out
    let mut task_3 = Task::new(
        TaskSpec::command("echo", vec!["task_3".into()]),
        TaskRequirements::generic(1, 10),
    );
    task_3.staged_files.push(StagedFile::new(
        "data/input_3.bin",
        Bytes::copy_from_slice(&file_data_2),
        false,
    ));
    let tid_3 = queue.submit(task_3).await.expect("submit task 3");
    let _ = queue
        .pop_and_schedule(worker_id, |_| true)
        .await
        .expect("schedule task 3");
    queue
        .mark_running(&tid_3, worker_id)
        .await
        .expect("mark running task 3");

    let mut res_3 = TaskResult::success(worker_id, tid_3, "ok", 10, false);
    res_3.artifacts.push(OutputArtifact::new(
        "output/result_3.bin",
        Bytes::copy_from_slice(&art_data_2),
    ));
    queue
        .record_result(res_3)
        .await
        .expect("record result task 3");

    // Task 2 must now be evicted, and Task 3 retained
    assert!(
        queue.get_task(&tid_2).await.is_none(),
        "Task 2 must be evicted upon Task 3 completion"
    );
    assert!(
        queue.get_task(&tid_3).await.is_some(),
        "Task 3 must be retained"
    );
}

#[tokio::test]
async fn test_active_tasks_never_evicted_by_terminal_budget() {
    // Verify that active queue tasks (Queued, Scheduled, Running) are NEVER evicted
    // even when terminal budget is exhausted.
    let queue = TaskQueue::with_retention_limits(
        RetryPolicy::default(),
        None,
        1, // only 1 terminal task retained
        1000,
    );
    let worker_id = Uuid::new_v4();

    // 1. Submit an active task that stays Queued
    let active_task = Task::new(
        TaskSpec::command("sleep", vec!["100".into()]),
        TaskRequirements::generic(1, 10),
    );
    let active_id = queue.submit(active_task).await.expect("submit active task");

    // 2. Submit and complete 5 quick tasks
    for i in 0..5 {
        let task = Task::new(
            TaskSpec::command("echo", vec![format!("{i}")]),
            TaskRequirements::generic(1, 10),
        );
        let tid = queue.submit(task).await.expect("submit quick task");
        let _ = queue.schedule_task(&tid, worker_id).await;
        let _ = queue.mark_running(&tid, worker_id).await;
        let res = TaskResult::success(worker_id, tid, "ok", 1, false);
        let _ = queue.record_result(res).await;
    }

    // Active task must STILL be in Queued state and NOT evicted
    assert_eq!(
        queue.get_state(&active_id).await,
        Some(TaskState::Queued),
        "Active Queued task must NEVER be evicted by terminal LRU retention"
    );
    let schedulable = queue.get_schedulable_tasks().await;
    assert!(
        schedulable.iter().any(|t| t.id == active_id),
        "Active task must remain schedulable in ready_queue"
    );
}
