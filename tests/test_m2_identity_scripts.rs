//! Verification tests for Milestone 2 (R2: Stable Identity Pairing & 1-Click Automation Scripts).
//!
//! Tests:
//! 1. 0-Config Master SecretKey persistence to ~/.oxideswarm/master_key.bin
//! 2. Worker auto-reconnect (<3s) upon master restart
//! 3. connect_remote.sh syntax and dry-run validation
//! 4. connect_remote.cmd elevation and bypass verification
//! 5. install_windows_service.ps1 -P2pTicket integration

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;
use tokio::sync::watch;

use rusty_grid_master::server::{default_master_key_file, MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

/// Test 1: Verifies that when P2P is enabled without an explicit key file, Master
/// automatically defaults to ~/.oxideswarm/master_key.bin and produces identical tickets.
#[tokio::test]
async fn test_0_config_master_secret_key_persistence() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let mock_home = temp_dir.path().join("mock_user_home");
    std::fs::create_dir_all(&mock_home).expect("create mock home");

    // Override HOME and USERPROFILE for isolated test execution
    std::env::set_var("HOME", &mock_home);
    std::env::set_var("USERPROFILE", &mock_home);

    let default_key = default_master_key_file().expect("default key path resolved");
    assert_eq!(
        default_key,
        mock_home.join(".oxideswarm").join("master_key.bin"),
        "Default key must point to ~/.oxideswarm/master_key.bin"
    );

    let ticket_file1 = temp_dir.path().join("master1.ticket");
    let master_bind: SocketAddr = "127.0.0.1:0".parse().unwrap();

    // Run 1: 0-config P2P (no p2p_key_file specified)
    let config1 = ServerConfig::new(master_bind)
        .with_p2p(true)
        .with_p2p_ticket_file(&ticket_file1);

    let master1 = MasterServer::spawn(config1)
        .await
        .expect("spawn master 1");
    let ticket1 = tokio::fs::read_to_string(&ticket_file1)
        .await
        .expect("read ticket 1");

    // Verify master_key.bin was created and is 32 bytes
    assert!(
        default_key.exists(),
        "Default master_key.bin must exist after master startup"
    );
    let key_data = tokio::fs::read(&default_key)
        .await
        .expect("read default key");
    assert_eq!(
        key_data.len(),
        32,
        "Secret key must be a genuine 32-byte Ed25519 binary key"
    );

    master1.shutdown().expect("shutdown master 1");
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Run 2: Restart Master with 0-config P2P
    let ticket_file2 = temp_dir.path().join("master2.ticket");
    let config2 = ServerConfig::new(master_bind)
        .with_p2p(true)
        .with_p2p_ticket_file(&ticket_file2);

    let master2 = MasterServer::spawn(config2)
        .await
        .expect("spawn master 2");
    let ticket2 = tokio::fs::read_to_string(&ticket_file2)
        .await
        .expect("read ticket 2");

    assert_eq!(
        ticket1, ticket2,
        "P2P ticket must be 100% deterministic across master restarts using default key persistence"
    );

    master2.shutdown().expect("shutdown master 2");
}

/// Test 2: Verifies that upon Master restart, the Worker automatically reconnects
/// and re-registers in < 3.0 seconds without manual intervention.
#[tokio::test]
async fn test_worker_auto_reconnect_under_3_seconds() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let key_file = temp_dir.path().join("fast_reconnect_master.key");
    let ticket_file = temp_dir.path().join("fast_reconnect_master.ticket");

    let master_bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let config1 = ServerConfig::new(master_bind)
        .with_p2p(true)
        .with_p2p_key_file(&key_file)
        .with_p2p_ticket_file(&ticket_file);

    // 1. Spawn Master Run 1
    let master1 = MasterServer::spawn(config1)
        .await
        .expect("spawn master 1");
    let ticket1 = tokio::fs::read_to_string(&ticket_file)
        .await
        .expect("read ticket 1");

    // 2. Start Worker with Ticket 1
    let worker_sandbox = temp_dir.path().join("worker_reconnect_sb");
    let worker_config = WorkerConfig::new("auto")
        .with_name("fast-reconnect-worker")
        .with_cores(2)
        .with_sandbox_base_dir(&worker_sandbox)
        .with_p2p_ticket(&ticket1);

    let mut worker = WorkerClient::new(worker_config);
    let worker_id = worker.worker_id();
    let (worker_shutdown_tx, worker_shutdown_rx) = watch::channel(false);

    let worker_task = tokio::spawn(async move {
        let _ = worker.run(worker_shutdown_rx).await;
    });

    // Wait for initial registration on Master 1
    let init_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut registered_master1 = false;
    while tokio::time::Instant::now() < init_deadline {
        if master1
            .registry()
            .list_workers()
            .await
            .iter()
            .any(|w| w.worker_id == worker_id)
        {
            registered_master1 = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        registered_master1,
        "Worker must register with Master 1 initially"
    );

    // 3. Stop Master 1
    master1.shutdown().expect("shutdown master 1");
    tokio::time::sleep(Duration::from_millis(400)).await;

    // 4. Start Master Run 2 with same persistent key
    let config2 = ServerConfig::new(master_bind)
        .with_p2p(true)
        .with_p2p_key_file(&key_file)
        .with_p2p_ticket_file(&ticket_file);

    let restart_start = std::time::Instant::now();
    let master2 = MasterServer::spawn(config2)
        .await
        .expect("spawn master 2");

    // 5. Measure reconnect time: Worker must re-register within 3.0 seconds
    let mut reconnected = false;
    let max_allowed = Duration::from_millis(3000);
    let timeout_deadline = tokio::time::Instant::now() + max_allowed;

    while tokio::time::Instant::now() < timeout_deadline {
        if master2
            .registry()
            .list_workers()
            .await
            .iter()
            .any(|w| w.worker_id == worker_id)
        {
            reconnected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let elapsed = restart_start.elapsed();
    assert!(
        reconnected,
        "Worker failed to auto-reconnect within 3.0s (elapsed: {elapsed:?})"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "Worker auto-reconnect must be < 3.0s, but took {elapsed:?}"
    );

    // Teardown
    let _ = worker_shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;
    let _ = master2.shutdown();
}

/// Test 3: Verifies connect_remote.sh script syntax, help message, and dry-run execution.
#[test]
fn test_connect_remote_sh_validation_and_dry_run() {
    let script_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("connect_remote.sh");

    assert!(script_path.exists(), "connect_remote.sh must exist at repo root");

    // Verify syntax with bash -n
    let status_syntax = Command::new("bash")
        .arg("-n")
        .arg(&script_path)
        .status()
        .expect("run bash -n");
    assert!(
        status_syntax.success(),
        "bash -n connect_remote.sh must succeed with exit code 0"
    );

    // Verify --help
    let output_help = Command::new("bash")
        .arg(&script_path)
        .arg("--help")
        .output()
        .expect("run connect_remote.sh --help");
    assert!(output_help.status.success());
    let stdout_help = String::from_utf8_lossy(&output_help.stdout);
    assert!(
        stdout_help.contains("Usage:"),
        "Help message must contain Usage:"
    );
    assert!(
        stdout_help.contains("--service"),
        "Help message must document --service"
    );
    assert!(
        stdout_help.contains("--foreground"),
        "Help message must document --foreground"
    );

    // Verify dry-run with valid ticket
    let output_dry = Command::new("bash")
        .arg(&script_path)
        .arg("{\"id\":\"00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff\",\"addrs\":[]}")
        .arg("--dry-run")
        .output()
        .expect("run connect_remote.sh --dry-run");
    assert!(
        output_dry.status.success(),
        "connect_remote.sh --dry-run must exit successfully"
    );
    let stdout_dry = String::from_utf8_lossy(&output_dry.stdout);
    assert!(
        stdout_dry.contains("Dry-Run Completed Successfully"),
        "Must output dry-run completion"
    );

    // Verify rejection of invalid ticket
    let output_invalid = Command::new("bash")
        .arg(&script_path)
        .arg("not-a-valid-ticket")
        .arg("--dry-run")
        .output()
        .expect("run connect_remote.sh with invalid ticket");
    assert!(
        !output_invalid.status.success(),
        "connect_remote.sh must reject invalid ticket format"
    );
}

/// Test 4: Verifies connect_remote.cmd has UAC elevation, ExecutionPolicy bypass, and service invocation.
#[test]
fn test_connect_remote_cmd_integrity() {
    let script_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("connect_remote.cmd");

    assert!(script_path.exists(), "connect_remote.cmd must exist at repo root");
    let content = std::fs::read_to_string(&script_path).expect("read connect_remote.cmd");

    assert!(
        content.contains("net session"),
        "connect_remote.cmd must check for Administrator privileges via 'net session'"
    );
    assert!(
        content.contains("-Verb RunAs"),
        "connect_remote.cmd must self-elevate via '-Verb RunAs'"
    );
    assert!(
        content.contains("-ExecutionPolicy Bypass"),
        "connect_remote.cmd must bypass restricted ExecutionPolicy"
    );
    assert!(
        content.contains("install_windows_service.ps1"),
        "connect_remote.cmd must invoke packaging\\windows\\install_windows_service.ps1"
    );
    assert!(
        content.contains("-P2pTicket"),
        "connect_remote.cmd must pass -P2pTicket to installer"
    );
}

/// Test 5: Verifies packaging/windows/install_windows_service.ps1 has -P2pTicket parameter.
#[test]
fn test_install_windows_service_ps1_p2p_ticket_support() {
    let ps1_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("packaging")
        .join("windows")
        .join("install_windows_service.ps1");

    assert!(ps1_path.exists(), "install_windows_service.ps1 must exist");
    let content = std::fs::read_to_string(&ps1_path).expect("read install_windows_service.ps1");

    assert!(
        content.contains("[string]$P2pTicket = \"\""),
        "install_windows_service.ps1 must declare [string]$P2pTicket parameter"
    );
    assert!(
        content.contains("p2p_ticket = '$P2pTicket'"),
        "install_windows_service.ps1 must persist p2p_ticket in generated TOML configuration"
    );
    assert!(
        content.contains("--p2p-ticket"),
        "install_windows_service.ps1 must configure service arguments to include --p2p-ticket"
    );
}
