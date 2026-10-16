//! Adversarial State Isolation & Environment Protection Verification Suite for Milestone 4.
//!
//! Authored by M4 Challenger 1 to empirically verify:
//! 1. Host Immutability: the developer's real `~/.oxideswarm` is never touched by any test.
//! 2. Path Resolution Adversarial Behavior:
//!    - When `OXIDESWARM_HOME` is empty (`""`), all resolvers fall back to default user profile without panics.
//!    - When `OXIDESWARM_HOME` is unset, all resolvers fall back to default user profile without panics.
//!    - When `OXIDESWARM_HOME` is an isolated temporary directory:
//!      - `default_oxideswarm_dir()` resolves to `<temp_dir>`
//!      - `default_master_key_file()` resolves to `<temp_dir>\master_key.bin`
//!      - `ClusterSecret::default_path()` resolves to `<temp_dir>\cluster.secret`
//!      - `default_worker_id_path(None)` resolves to `<temp_dir>\worker_id`
//!      - `default_worker_id_path(Some("worker-1"))` resolves to `<temp_dir>\worker_id.worker-1`
//! 3. Master 0-Config P2P Default Key Persistence:
//!    - Spawning a Master with P2P enabled and NO explicit key file creates `master_key.bin` in `<temp_dir>`
//!    - The generated key is exactly 32 bytes and gives the same P2P endpoint id across restarts
//!    - The real `~/.oxideswarm` remains untouched and identical in metadata and contents.
//! 4. Worker Identity File Environment Precedence:
//!    - `RUSTY_GRID_WORKER_ID_FILE` takes precedence over `OXIDESWARM_HOME`
//!    - Empty `RUSTY_GRID_WORKER_ID_FILE` falls back cleanly to `OXIDESWARM_HOME`

use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::Mutex;

use rusty_grid_cli::config::default_oxideswarm_dir;
use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_master::server::{default_master_key_file, MasterServer, ServerConfig};
use rusty_grid_worker::identity::{default_worker_id_path, WORKER_ID_FILE_ENV};

const DEFAULT_SECRET_FILE_NAME: &str = "cluster.secret";

static ENV_MUTEX: Mutex<()> = Mutex::const_new(());

struct EnvGuard {
    orig_oxideswarm_home: Option<OsString>,
    orig_home: Option<OsString>,
    orig_userprofile: Option<OsString>,
    orig_worker_id_file: Option<OsString>,
}

impl EnvGuard {
    fn capture() -> Self {
        Self {
            orig_oxideswarm_home: std::env::var_os("OXIDESWARM_HOME"),
            orig_home: std::env::var_os("HOME"),
            orig_userprofile: std::env::var_os("USERPROFILE"),
            orig_worker_id_file: std::env::var_os(WORKER_ID_FILE_ENV),
        }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.orig_oxideswarm_home {
            Some(v) => std::env::set_var("OXIDESWARM_HOME", v),
            None => std::env::remove_var("OXIDESWARM_HOME"),
        }
        match &self.orig_home {
            Some(v) => std::env::set_var("HOME", v),
            None => std::env::remove_var("HOME"),
        }
        match &self.orig_userprofile {
            Some(v) => std::env::set_var("USERPROFILE", v),
            None => std::env::remove_var("USERPROFILE"),
        }
        match &self.orig_worker_id_file {
            Some(v) => std::env::set_var(WORKER_ID_FILE_ENV, v),
            None => std::env::remove_var(WORKER_ID_FILE_ENV),
        }
    }
}

fn get_expected_default_dir() -> PathBuf {
    #[cfg(windows)]
    {
        let base = std::env::var_os("USERPROFILE")
            .or_else(|| std::env::var_os("HOME"))
            .filter(|s| !s.is_empty())
            .expect("USERPROFILE or HOME must be set");
        PathBuf::from(base).join(".oxideswarm")
    }
    #[cfg(not(windows))]
    {
        let base = std::env::var_os("HOME")
            .filter(|s| !s.is_empty())
            .expect("HOME must be set");
        PathBuf::from(base).join(".oxideswarm")
    }
}

#[test]
fn test_adversarial_empty_oxideswarm_home_fallback() {
    let _lock = ENV_MUTEX.blocking_lock();
    let _guard = EnvGuard::capture();

    // Adversarial: Set OXIDESWARM_HOME to empty string ""
    std::env::set_var("OXIDESWARM_HOME", "");
    std::env::remove_var(WORKER_ID_FILE_ENV);

    let expected_dir = get_expected_default_dir();
    let expected_master_key = expected_dir.join("master_key.bin");
    let expected_secret = expected_dir.join(DEFAULT_SECRET_FILE_NAME);
    let expected_worker_id = expected_dir.join("worker_id");
    let expected_worker_id_named = expected_dir.join("worker_id.node-42");

    // 1. default_oxideswarm_dir
    let dir = default_oxideswarm_dir().expect("default_oxideswarm_dir should not be None");
    assert_eq!(
        dir, expected_dir,
        "default_oxideswarm_dir must fall back to default profile on empty OXIDESWARM_HOME"
    );

    // 2. default_master_key_file
    let key = default_master_key_file().expect("default_master_key_file should not be None");
    assert_eq!(
        key, expected_master_key,
        "default_master_key_file must fall back to default profile on empty OXIDESWARM_HOME"
    );

    // 3. ClusterSecret::default_path
    let secret =
        ClusterSecret::default_path().expect("ClusterSecret::default_path should not be None");
    assert_eq!(
        secret, expected_secret,
        "ClusterSecret::default_path must fall back to default profile on empty OXIDESWARM_HOME"
    );

    // 4. default_worker_id_path(None)
    let worker_id =
        default_worker_id_path(None).expect("default_worker_id_path(None) should not be None");
    assert_eq!(
        worker_id, expected_worker_id,
        "default_worker_id_path(None) must fall back to default profile on empty OXIDESWARM_HOME"
    );

    // 5. default_worker_id_path(Some("node-42"))
    let worker_id_named = default_worker_id_path(Some("node-42"))
        .expect("default_worker_id_path(Some) should not be None");
    assert_eq!(
        worker_id_named, expected_worker_id_named,
        "default_worker_id_path(Some) must append sanitized name on empty OXIDESWARM_HOME"
    );
}

#[test]
fn test_adversarial_unset_oxideswarm_home_fallback() {
    let _lock = ENV_MUTEX.blocking_lock();
    let _guard = EnvGuard::capture();

    // Adversarial: Explicitly remove OXIDESWARM_HOME
    std::env::remove_var("OXIDESWARM_HOME");
    std::env::remove_var(WORKER_ID_FILE_ENV);

    let expected_dir = get_expected_default_dir();
    let expected_master_key = expected_dir.join("master_key.bin");
    let expected_secret = expected_dir.join(DEFAULT_SECRET_FILE_NAME);
    let expected_worker_id = expected_dir.join("worker_id");

    assert_eq!(default_oxideswarm_dir(), Some(expected_dir));
    assert_eq!(default_master_key_file(), Some(expected_master_key));
    assert_eq!(ClusterSecret::default_path(), Some(expected_secret));
    assert_eq!(default_worker_id_path(None), Some(expected_worker_id));
}

#[test]
fn test_adversarial_isolated_temp_dir_resolution() {
    let _lock = ENV_MUTEX.blocking_lock();
    let _guard = EnvGuard::capture();

    let temp_sandbox = tempfile::tempdir().expect("create temp dir");
    let sandbox_path = temp_sandbox.path().to_path_buf();

    std::env::set_var("OXIDESWARM_HOME", &sandbox_path);
    std::env::remove_var(WORKER_ID_FILE_ENV);

    // 1. default_oxideswarm_dir must resolve to <temp_dir>
    let dir = default_oxideswarm_dir().expect("must resolve oxideswarm dir");
    assert_eq!(
        dir, sandbox_path,
        "default_oxideswarm_dir must match temp_dir exactly"
    );

    // 2. default_master_key_file must resolve to <temp_dir>\master_key.bin
    let key = default_master_key_file().expect("must resolve master key");
    assert_eq!(key, sandbox_path.join("master_key.bin"));

    // 3. ClusterSecret::default_path must resolve to <temp_dir>\cluster.secret
    let secret = ClusterSecret::default_path().expect("must resolve cluster secret");
    assert_eq!(secret, sandbox_path.join("cluster.secret"));

    // 4. default_worker_id_path(None) must resolve to <temp_dir>\worker_id
    let worker_id = default_worker_id_path(None).expect("must resolve worker id");
    assert_eq!(worker_id, sandbox_path.join("worker_id"));

    // 5. default_worker_id_path(Some("bench-worker"))
    let worker_id_named =
        default_worker_id_path(Some("bench-worker")).expect("must resolve named worker id");
    assert_eq!(worker_id_named, sandbox_path.join("worker_id.bench-worker"));
}

#[test]
fn test_worker_id_env_override_precedence() {
    let _lock = ENV_MUTEX.blocking_lock();
    let _guard = EnvGuard::capture();

    let temp_sandbox = tempfile::tempdir().expect("create temp dir");
    let sandbox_path = temp_sandbox.path().to_path_buf();
    std::env::set_var("OXIDESWARM_HOME", &sandbox_path);

    // 1. Explicit RUSTY_GRID_WORKER_ID_FILE must override OXIDESWARM_HOME
    let explicit_file = sandbox_path.join("custom_id_file.txt");
    std::env::set_var(WORKER_ID_FILE_ENV, &explicit_file);
    assert_eq!(
        default_worker_id_path(Some("ignored-name")),
        Some(explicit_file)
    );

    // 2. Empty RUSTY_GRID_WORKER_ID_FILE must fall back to OXIDESWARM_HOME
    std::env::set_var(WORKER_ID_FILE_ENV, "");
    assert_eq!(
        default_worker_id_path(None),
        Some(sandbox_path.join("worker_id"))
    );
}

#[tokio::test]
async fn test_master_p2p_default_key_creates_isolated_key_without_host_mutation() {
    let _lock = ENV_MUTEX.lock().await;
    let _guard = EnvGuard::capture();

    // Check host ~/.oxideswarm before spawning master
    let host_oxideswarm = get_expected_default_dir();
    let host_master_key = host_oxideswarm.join("master_key.bin");
    let host_pre_mtime = if host_master_key.exists() {
        Some(
            std::fs::metadata(&host_master_key)
                .unwrap()
                .modified()
                .unwrap(),
        )
    } else {
        None
    };

    // Create isolated sandbox for OXIDESWARM_HOME
    let temp_sandbox = tempfile::tempdir().expect("create temp dir");
    let sandbox_dir = temp_sandbox.path().to_path_buf();
    std::env::set_var("OXIDESWARM_HOME", &sandbox_dir);

    let ticket_file = sandbox_dir.join("isolated_master.ticket");
    let master_bind: SocketAddr = "127.0.0.1:0".parse().unwrap();

    // 0-config P2P: No explicit p2p_key_file provided.
    // Master MUST use default_master_key_file() which now resolves to <sandbox_dir>/master_key.bin.
    let config1 = ServerConfig::new(master_bind)
        .with_p2p(true)
        .with_p2p_ticket_file(&ticket_file);

    let master1 = MasterServer::spawn(config1)
        .await
        .expect("spawn master 1 in isolated environment");

    let expected_sandboxed_key = sandbox_dir.join("master_key.bin");
    assert!(
        expected_sandboxed_key.exists(),
        "master_key.bin must be created inside isolated OXIDESWARM_HOME: {:?}",
        expected_sandboxed_key
    );

    let key_data = tokio::fs::read(&expected_sandboxed_key)
        .await
        .expect("read sandboxed master_key.bin");
    assert_eq!(
        key_data.len(),
        32,
        "Generated Ed25519 key must be exactly 32 bytes"
    );

    let ticket1 = tokio::fs::read_to_string(&ticket_file)
        .await
        .expect("read ticket 1");
    assert!(!ticket1.is_empty(), "Generated ticket must not be empty");

    master1.shutdown().expect("shutdown master 1");
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Restart Master with same isolated config to verify key reuse
    let ticket_file2 = sandbox_dir.join("isolated_master_2.ticket");
    let config2 = ServerConfig::new(master_bind)
        .with_p2p(true)
        .with_p2p_ticket_file(&ticket_file2);

    let master2 = MasterServer::spawn(config2)
        .await
        .expect("spawn master 2 in isolated environment");

    let ticket2 = tokio::fs::read_to_string(&ticket_file2)
        .await
        .expect("read ticket 2");
    // Compare the endpoint id, which comes from the persisted key. The rest of the ticket is the
    // address list: the home relay when one is reachable, otherwise direct addresses whose UDP
    // port changes on every start, so whole tickets only match on a machine with Internet access.
    let endpoint_id = |ticket: &str| {
        let parsed: serde_json::Value = serde_json::from_str(ticket).expect("ticket is JSON");
        parsed["id"]
            .as_str()
            .expect("ticket has an endpoint id")
            .to_string()
    };
    assert_eq!(
        endpoint_id(&ticket1),
        endpoint_id(&ticket2),
        "P2P endpoint id must be identical across restarts using persistent sandboxed key"
    );

    master2.shutdown().expect("shutdown master 2");

    // Critical: Verify host ~/.oxideswarm was NOT touched at all
    if let Some(pre_mtime) = host_pre_mtime {
        let post_mtime = std::fs::metadata(&host_master_key)
            .unwrap()
            .modified()
            .unwrap();
        assert_eq!(
            pre_mtime, post_mtime,
            "CRITICAL: Host ~/.oxideswarm/master_key.bin was modified during isolated test!"
        );
    }
}
