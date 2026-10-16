//! Integration tests verifying Android JNI bridge cluster authentication against live Master nodes.

use oxideworker::{
    get_worker_status_impl, set_cluster_secret_impl, set_sandbox_dir_impl, start_worker_impl,
    start_worker_with_secret_impl, stop_worker_impl,
};
use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_master::server::{MasterServer, ServerConfig};
use std::time::Duration;

static TEST_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn test_bridge_set_cluster_secret_hex_and_file_parsing() {
    let _guard = TEST_MUTEX.lock().await;
    stop_worker_impl();

    // 1. Valid 64-char hex secret
    let raw_secret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    assert!(set_cluster_secret_impl(raw_secret));

    // 2. Whitespace trimming
    let padded = format!("   \n\t{raw_secret}   \n");
    assert!(set_cluster_secret_impl(&padded));

    // 3. Clear secret with empty string
    assert!(set_cluster_secret_impl("   "));

    // 4. Load from secret file
    let temp_dir = tempfile::tempdir().unwrap();
    let secret_file = temp_dir.path().join("test_cluster.secret");
    std::fs::write(&secret_file, format!("{raw_secret}\n")).unwrap();
    assert!(set_cluster_secret_impl(&secret_file.display().to_string()));

    // 5. Invalid secret (invalid hex characters and nonexistent file path)
    assert!(!set_cluster_secret_impl(
        "invalid_secret_non_hex_12345_nonexistent_path"
    ));
    let status_json = get_worker_status_impl();
    assert!(
        status_json.contains("ERROR: Invalid cluster secret:"),
        "Status must report error for invalid secret: {status_json}"
    );

    // Reset secret
    assert!(set_cluster_secret_impl(""));
}

#[tokio::test]
async fn test_bridge_worker_connects_to_authenticated_master() {
    let _guard = TEST_MUTEX.lock().await;
    stop_worker_impl();

    let temp_dir = tempfile::tempdir().unwrap();
    let sandbox_dir = temp_dir.path().join("sandboxes");
    std::fs::create_dir_all(&sandbox_dir).unwrap();
    assert!(set_sandbox_dir_impl(sandbox_dir.display().to_string()));

    // 1. Generate secret and spawn secret-protected Master
    let secret = ClusterSecret::generate().unwrap();
    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_cluster_secret(secret.clone())
        .with_handshake_timeout(3);
    let master = MasterServer::spawn(server_config)
        .await
        .expect("Spawn master");
    let master_addr = master.server_addr();

    // 2. Configure secret on Android Bridge
    assert!(set_cluster_secret_impl(&secret.to_hex()));

    // 3. Start worker via bridge
    let started = start_worker_impl(
        master_addr.to_string(),
        None,
        "AuthAndroidWorker".to_string(),
        2,
        2048,
        false,
        2,
    );
    assert!(started, "Worker supervisor should spawn");

    // 4. Wait for worker to complete client_handshake and register with Master
    let mut registered = false;
    for _ in 0..100 {
        if let Ok(workers) = master.list_workers().await {
            if !workers.is_empty() {
                registered = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        registered,
        "Worker must successfully complete handshake and register with authenticated Master"
    );

    let workers = master.list_workers().await.expect("list workers");
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].capabilities.name, "AuthAndroidWorker");

    // 5. Clean teardown
    assert!(stop_worker_impl());
    let _ = master.shutdown();
    assert!(set_cluster_secret_impl(""));
}

#[tokio::test]
async fn test_bridge_worker_without_secret_rejected_by_authenticated_master() {
    let _guard = TEST_MUTEX.lock().await;
    stop_worker_impl();

    let temp_dir = tempfile::tempdir().unwrap();
    let sandbox_dir = temp_dir.path().join("sandboxes");
    std::fs::create_dir_all(&sandbox_dir).unwrap();
    assert!(set_sandbox_dir_impl(sandbox_dir.display().to_string()));

    // 1. Spawn secret-protected Master
    let secret = ClusterSecret::generate().unwrap();
    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_cluster_secret(secret.clone())
        .with_handshake_timeout(2);
    let master = MasterServer::spawn(server_config)
        .await
        .expect("Spawn master");
    let master_addr = master.server_addr();

    // 2. Ensure NO secret is configured on the bridge
    assert!(set_cluster_secret_impl(""));

    // 3. Start unauthenticated worker
    let started = start_worker_impl(
        master_addr.to_string(),
        None,
        "UnauthAndroidWorker".to_string(),
        2,
        2048,
        false,
        2,
    );
    assert!(started, "Worker supervisor starts");

    // 4. Verify worker is NOT registered with master
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let count = master.list_workers().await.map(|w| w.len()).unwrap_or(0);
        assert_eq!(count, 0, "Unauthenticated worker must not be registered");
    }

    // 5. Clean teardown
    assert!(stop_worker_impl());
    let _ = master.shutdown();
}

#[tokio::test]
async fn test_bridge_worker_with_secret_impl_convenience_function() {
    let _guard = TEST_MUTEX.lock().await;
    stop_worker_impl();

    let temp_dir = tempfile::tempdir().unwrap();
    let sandbox_dir = temp_dir.path().join("sandboxes");
    std::fs::create_dir_all(&sandbox_dir).unwrap();
    assert!(set_sandbox_dir_impl(sandbox_dir.display().to_string()));

    // 1. Spawn secret-protected Master
    let secret = ClusterSecret::generate().unwrap();
    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_cluster_secret(secret.clone())
        .with_handshake_timeout(3);
    let master = MasterServer::spawn(server_config)
        .await
        .expect("Spawn master");
    let master_addr = master.server_addr();

    // 2. Start worker using start_worker_with_secret_impl directly
    let started = start_worker_with_secret_impl(
        master_addr.to_string(),
        None,
        Some(secret.to_hex()),
        "DirectSecretWorker".to_string(),
        2,
        2048,
        false,
        2,
    );
    assert!(started, "Worker supervisor should spawn");

    let mut registered = false;
    for _ in 0..100 {
        if let Ok(workers) = master.list_workers().await {
            if !workers.is_empty() {
                registered = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        registered,
        "Direct-secret worker must register successfully"
    );

    assert!(stop_worker_impl());
    let _ = master.shutdown();
    assert!(set_cluster_secret_impl(""));
}
