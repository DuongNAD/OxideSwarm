//! Adversarial Stress & Edge Case Test Suite for Android Bridge Cluster Authentication
//! Milestone 2 Challenger 1 verification.
//!
//! Specifically validates:
//! 1. Adversarial secret text inputs to `set_cluster_secret_impl`:
//!    - Bad hex characters (non-hex characters, interior whitespace, invalid chars at edges and middle)
//!    - Truncated hex (1 char, 2 chars, 10 chars, 30 chars/15 bytes, 31 chars)
//!    - 65-character hex (odd character count exceeding standard 64-char SHA256 hex)
//!    - Non-existent file paths (absolute, relative, invalid directory structures)
//!    - Corrupt and malformed secret files (empty file, comments only, non-hex file contents)
//! 2. Error reporting & state preservation:
//!    - Return value is false for all invalid inputs
//!    - `get_worker_status_impl` surfaces `ERROR: Invalid cluster secret:` without panicking
//!    - State cleanly recovers upon clearing or providing valid secret
//! 3. Rapid sequential and concurrent worker connect/disconnect cycles against an authenticated MasterServer:
//!    - Valid secret connects, authenticates via HMAC handshake, and registers
//!    - Rogue worker with invalid secret is rejected by master handshake without crashing master
//!    - Repeated rapid start/stop cycles operate cleanly without resource leak or deadlocks

use oxideworker::{
    get_worker_status_impl, set_cluster_secret_impl, set_sandbox_dir_impl,
    start_worker_with_secret_impl, stop_worker_impl,
};
use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_master::server::{MasterServer, ServerConfig};
use std::time::Duration;
use tempfile::tempdir;

static TEST_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn test_adversarial_auth_bad_hex_characters() {
    let _guard = TEST_MUTEX.lock().await;
    stop_worker_impl();

    let bad_hex_inputs = [
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789zzzzzz",
        "not_a_hex_secret_key_at_all_!@#$%^&*()_+~`",
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeX",
        "0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEG", // G is not hex
        "1234567890abcdefghijklmnopqrstuvwxyz",
        "01234567 89abcdef 01234567 89abcdef 01234567 89abcdef 01234567 89abcdef", // interior whitespace
        "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", // 0x prefix is not raw hex
        "😊0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", // unicode emoji
    ];

    for input in &bad_hex_inputs {
        let result = set_cluster_secret_impl(input);
        assert!(!result, "Bad hex input must return false: {input}");

        let status = get_worker_status_impl();
        assert!(
            status.contains("ERROR: Invalid cluster secret:"),
            "Status must indicate invalid cluster secret for input '{input}': got '{status}'"
        );
    }

    // State recovery: resetting to empty must succeed and clear error
    assert!(set_cluster_secret_impl(""));
}

#[tokio::test]
async fn test_adversarial_auth_truncated_hex() {
    let _guard = TEST_MUTEX.lock().await;
    stop_worker_impl();

    let truncated_inputs = [
        "a",                               // 1 char
        "01",                              // 2 chars = 1 byte (min is 16 bytes = 32 chars)
        "0123456789",                      // 10 chars = 5 bytes
        "0123456789abcdef0123456789",      // 26 chars = 13 bytes
        "0123456789abcdef0123456789abcd",  // 30 chars = 15 bytes (1 byte short of minimum)
        "0123456789abcdef0123456789abcde", // 31 chars = odd length
    ];

    for input in &truncated_inputs {
        let result = set_cluster_secret_impl(input);
        assert!(!result, "Truncated hex input must return false: {input}");

        let status = get_worker_status_impl();
        assert!(
            status.contains("ERROR: Invalid cluster secret:"),
            "Status must report error for truncated hex '{input}': got '{status}'"
        );
    }

    assert!(set_cluster_secret_impl(""));
}

#[tokio::test]
async fn test_adversarial_auth_65_char_hex() {
    let _guard = TEST_MUTEX.lock().await;
    stop_worker_impl();

    // Exactly 65 valid hex characters (odd length: 64 + 1 extra char)
    let hex_65 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0";
    assert_eq!(hex_65.len(), 65);

    let result = set_cluster_secret_impl(hex_65);
    assert!(
        !result,
        "65-character hex input (odd length) must return false"
    );

    let status = get_worker_status_impl();
    assert!(
        status.contains("ERROR: Invalid cluster secret:"),
        "Status must report error for 65-char hex: got '{status}'"
    );

    // Also with surrounding whitespace
    let padded_65 = format!("   \t\n{hex_65}\n  ");
    assert!(!set_cluster_secret_impl(&padded_65));

    assert!(set_cluster_secret_impl(""));
}

#[tokio::test]
async fn test_adversarial_auth_nonexistent_and_corrupt_file_paths() {
    let _guard = TEST_MUTEX.lock().await;
    stop_worker_impl();

    let nonexistent_paths = [
        "nonexistent_cluster_secret_file_12345.secret",
        "/var/empty/does_not_exist/cluster.secret",
        "C:\\nonexistent_dir_9999\\missing_secret.secret",
        "subdir/missing_file.txt",
    ];

    for path in &nonexistent_paths {
        let result = set_cluster_secret_impl(path);
        assert!(!result, "Nonexistent file path must return false: {path}");

        let status = get_worker_status_impl();
        assert!(
            status.contains("ERROR: Invalid cluster secret:"),
            "Status must report error for nonexistent file path: got '{status}'"
        );
    }

    // Now test files that DO exist on disk, but contain invalid secret formats
    let temp_dir = tempdir().unwrap();

    // 1. Empty file
    let empty_file = temp_dir.path().join("empty.secret");
    std::fs::write(&empty_file, b"").unwrap();
    assert!(!set_cluster_secret_impl(&empty_file.display().to_string()));
    assert!(get_worker_status_impl().contains("ERROR: Invalid cluster secret:"));

    // 2. Only comment lines
    let comment_file = temp_dir.path().join("comments_only.secret");
    std::fs::write(&comment_file, b"# line 1 comment\n# line 2 comment\n").unwrap();
    assert!(!set_cluster_secret_impl(
        &comment_file.display().to_string()
    ));

    // 3. Corrupt/non-hex characters in file
    let corrupt_file = temp_dir.path().join("corrupt.secret");
    std::fs::write(
        &corrupt_file,
        b"this_file_contains_invalid_hex_characters\n",
    )
    .unwrap();
    assert!(!set_cluster_secret_impl(
        &corrupt_file.display().to_string()
    ));

    // 4. Truncated hex in file (10 bytes = 20 chars)
    let truncated_file = temp_dir.path().join("truncated.secret");
    std::fs::write(&truncated_file, b"0123456789abcdef0123\n").unwrap();
    assert!(!set_cluster_secret_impl(
        &truncated_file.display().to_string()
    ));

    // 5. 65-character hex in file
    let file_65 = temp_dir.path().join("hex65.secret");
    let hex_65 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0";
    std::fs::write(&file_65, format!("{hex_65}\n")).unwrap();
    assert!(!set_cluster_secret_impl(&file_65.display().to_string()));

    // Clear secret cleanly
    assert!(set_cluster_secret_impl(""));
}

#[tokio::test]
async fn test_adversarial_auth_state_recovery_and_rotation() {
    let _guard = TEST_MUTEX.lock().await;
    stop_worker_impl();

    // 1. Start with invalid input
    assert!(!set_cluster_secret_impl("invalid_garbage_hex_12345"));
    assert!(get_worker_status_impl().contains("ERROR: Invalid cluster secret:"));

    // 2. Clear with empty string -> should succeed
    assert!(set_cluster_secret_impl(""));

    // 3. Provide valid generated secret -> should succeed
    let secret1 = ClusterSecret::generate().unwrap();
    assert!(set_cluster_secret_impl(&secret1.to_hex()));

    // 4. Inject another invalid input -> should fail
    assert!(!set_cluster_secret_impl("corrupted_again_not_hex"));

    // 5. Rotate to a new valid secret file -> should succeed
    let temp_dir = tempdir().unwrap();
    let secret2 = ClusterSecret::generate().unwrap();
    let secret_file = temp_dir.path().join("new_cluster.secret");
    std::fs::write(&secret_file, format!("{}\n", secret2.to_hex())).unwrap();
    assert!(set_cluster_secret_impl(&secret_file.display().to_string()));

    // 6. Reset
    assert!(set_cluster_secret_impl(""));
}

#[tokio::test]
async fn test_adversarial_rapid_worker_connect_disconnect_against_authenticated_master() {
    let _guard = TEST_MUTEX.lock().await;
    stop_worker_impl();

    let temp_dir = tempdir().unwrap();
    let sandbox_dir = temp_dir.path().join("sandboxes");
    std::fs::create_dir_all(&sandbox_dir).unwrap();
    assert!(set_sandbox_dir_impl(sandbox_dir.display().to_string()));

    // Spawn authenticated MasterServer
    let secret = ClusterSecret::generate().unwrap();
    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_cluster_secret(secret.clone())
        .with_handshake_timeout(3);
    let master = MasterServer::spawn(server_config)
        .await
        .expect("Spawn master");
    let master_addr = master.server_addr();

    // 1. Run 5 rapid connect/disconnect cycles
    for cycle in 0..5 {
        let worker_name = format!("CycleWorker_{cycle}");
        let started = start_worker_with_secret_impl(
            master_addr.to_string(),
            None,
            Some(secret.to_hex()),
            worker_name.clone(),
            2,
            2048,
            false,
            2,
        );
        assert!(started, "Worker supervisor should spawn for cycle {cycle}");

        // Wait for worker to register
        let mut registered = false;
        for _ in 0..60 {
            if let Ok(workers) = master.list_workers().await {
                if workers.iter().any(|w| w.capabilities.name == worker_name) {
                    registered = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(registered, "Worker must register in cycle {cycle}");

        // Stop worker immediately
        assert!(stop_worker_impl(), "Stop worker in cycle {cycle}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // 2. Adversarial rogue worker: start with wrong secret
    let wrong_secret = ClusterSecret::generate().unwrap();
    let started = start_worker_with_secret_impl(
        master_addr.to_string(),
        None,
        Some(wrong_secret.to_hex()),
        "RogueWorker".to_string(),
        2,
        2048,
        false,
        2,
    );
    assert!(started, "Worker supervisor spawns");

    // Verify rogue worker is rejected by Master handshake and never registered
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let workers = master.list_workers().await.unwrap_or_default();
        assert!(
            !workers.iter().any(|w| w.capabilities.name == "RogueWorker"),
            "Rogue worker with wrong secret must NEVER be admitted into registry"
        );
    }
    assert!(stop_worker_impl());

    // 3. Clean master shutdown
    let _ = master.shutdown();
    assert!(set_cluster_secret_impl(""));
}
