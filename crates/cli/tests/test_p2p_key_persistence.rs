//! End-to-end P2P secret key persistence, CLI flag/env/config precedence, corrupt-key
//! rejection, hex-key parsing, ephemeral identity variance, and shutdown file cleanup tests.
//!
//! Every spawned `rusty-grid` child process runs with an isolated temporary `HOME` and
//! `USERPROFILE` passed via `Command::env`, so the developer's `~/.oxideswarm` is never touched.

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use rusty_grid_master::server::{MasterServer, ServerConfig};

const BIN: &str = env!("CARGO_BIN_EXE_rusty-grid");

/// Builds a `Command` for `rusty-grid` with a clean, isolated home directory.
fn cli(home: &Path) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.env("HOME", home)
        .env("USERPROFILE", home)
        .env_remove("OXIDESWARM_HOME")
        .current_dir(home)
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("OXIDESWARM_SECRET_FILE")
        .env_remove("RUSTY_GRID_CONFIG")
        .env_remove("RUSTY_GRID_MASTER")
        .env_remove("RUSTY_GRID_MASTER_ADDR")
        .env_remove("RUSTY_GRID_LISTEN")
        .env_remove("RUSTY_GRID_MASTER_LISTEN")
        .env_remove("RUSTY_GRID_INSECURE")
        .env_remove("RUSTY_GRID_P2P")
        .env_remove("RUSTY_GRID_P2P_KEY_FILE")
        .env_remove("RUSTY_GRID_P2P_TICKET_FILE")
        .env_remove("RUSTY_GRID_EPHEMERAL_KEY")
        .env("RUSTY_GRID_DISABLE_DISCOVERY", "1")
        .env("RUSTY_GRID_DISABLE_WEB_UI", "1")
        .env("NO_COLOR", "1")
        .stdin(Stdio::null());
    cmd
}

struct RunningMaster {
    child: Child,
}

impl RunningMaster {
    fn spawn(mut cmd: Command) -> Self {
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
        let child = cmd.spawn().expect("spawn rusty-grid master");
        Self { child }
    }

    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for RunningMaster {
    fn drop(&mut self) {
        self.stop();
    }
}

fn wait_for_non_empty_file(path: &Path, timeout: Duration) -> Option<String> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(content) = fs::read_to_string(path) {
            let trimmed = content.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
        std::thread::sleep(Duration::from_millis(40));
    }
    None
}

fn ticket_endpoint_id(ticket_json: &str) -> String {
    let parsed: serde_json::Value =
        serde_json::from_str(ticket_json).expect("ticket must be valid JSON");
    parsed["id"]
        .as_str()
        .expect("ticket JSON must contain string 'id'")
        .to_string()
}

#[test]
fn test_corrupt_p2p_key_file_exits_1_without_port_or_ticket_file() {
    let cases: &[(&str, Option<&[u8]>)] = &[
        ("0_bytes_empty", Some(b"")),
        ("1_byte_minimal", Some(&[0x00])),
        ("15_bytes_too_short", Some(b"0123456789abcde")),
        ("31_bytes_boundary_short", Some(&[b'B'; 31])),
        ("33_bytes_boundary_long", Some(&[b'C'; 33])),
        ("64_invalid_hex_chars", Some(&[b'z'; 64])),
        ("directory_as_key_file", None),
    ];

    for &(name, payload) in cases {
        let tmp = tempfile::tempdir().expect("tempdir");
        let home = tmp.path();
        let port_file = home.join("master.port");
        let ticket_file = home.join("ticket.txt");
        let key_file = home.join(format!("{name}.key"));

        match payload {
            Some(bytes) => fs::write(&key_file, bytes).expect("write corrupt key"),
            None => fs::create_dir_all(&key_file).expect("create key dir"),
        }

        let mut cmd = cli(home);
        cmd.args([
            "master",
            "--listen",
            "127.0.0.1:0",
            "--web-ui-addr",
            "127.0.0.1:0",
            "--port-file",
        ])
        .arg(&port_file)
        .args(["--p2p", "--p2p-key-file"])
        .arg(&key_file)
        .arg("--p2p-ticket-file")
        .arg(&ticket_file);

        let output = cmd
            .output()
            .expect("run rusty-grid master with corrupt key");
        assert_eq!(
            output.status.code(),
            Some(1),
            "case {name}: corrupt key file must exit with code 1, stderr={}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("Failed to start master server:"),
            "case {name}: expected diagnostic on stderr, got: {stderr}"
        );
        assert!(
            !port_file.exists(),
            "case {name}: port file must not be written when key is corrupt"
        );
        assert!(
            !ticket_file.exists(),
            "case {name}: ticket file must not be written when key is corrupt"
        );
    }
}

#[test]
fn test_hex_encoded_p2p_key_file_produces_stable_endpoint_identity() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let home = tmp.path();
    let key_file = home.join("hex_key.txt");
    let hex_key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n";
    fs::write(&key_file, hex_key).expect("write hex key");

    let mut endpoint_ids = Vec::new();
    for cycle in 1..=2 {
        let port_file = home.join(format!("hex_{cycle}.port"));
        let ticket_file = home.join(format!("hex_{cycle}.ticket"));

        let mut cmd = cli(home);
        cmd.args([
            "master",
            "--listen",
            "127.0.0.1:0",
            "--web-ui-addr",
            "127.0.0.1:0",
            "--port-file",
        ])
        .arg(&port_file)
        .args(["--p2p", "--p2p-key-file"])
        .arg(&key_file)
        .arg("--p2p-ticket-file")
        .arg(&ticket_file);

        let mut master = RunningMaster::spawn(cmd);
        let ticket = wait_for_non_empty_file(&ticket_file, Duration::from_secs(10))
            .unwrap_or_else(|| panic!("cycle {cycle}: hex key master failed to write ticket"));
        endpoint_ids.push(ticket_endpoint_id(&ticket));
        master.stop();
    }

    assert_eq!(
        endpoint_ids[0], endpoint_ids[1],
        "64-char hex key file must produce identical P2P endpoint ID across restarts"
    );
    assert_eq!(
        fs::read_to_string(&key_file).expect("read hex key"),
        hex_key,
        "hex key file content must be preserved untouched"
    );
}

#[test]
fn test_key_file_cli_alias_and_env_and_config_precedence() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let home = tmp.path();

    // 1. `--key-file` CLI alias
    let alias_key = home.join("alias_key.bin");
    let alias_ticket = home.join("alias.ticket");
    {
        let mut cmd = cli(home);
        cmd.args([
            "master",
            "--listen",
            "127.0.0.1:0",
            "--web-ui-addr",
            "127.0.0.1:0",
            "--key-file",
        ])
        .arg(&alias_key)
        .arg("--p2p-ticket-file")
        .arg(&alias_ticket);

        let mut master = RunningMaster::spawn(cmd);
        let ticket = wait_for_non_empty_file(&alias_ticket, Duration::from_secs(10))
            .expect("--key-file alias must publish ticket");
        assert!(!ticket_endpoint_id(&ticket).is_empty());
        master.stop();
    }
    assert_eq!(
        fs::metadata(&alias_key).expect("alias_key exists").len(),
        32,
        "--key-file must create a 32-byte key"
    );

    // 2. `RUSTY_GRID_P2P_KEY_FILE` env var when CLI flag is absent
    let env_key = home.join("env_key.bin");
    let env_ticket = home.join("env.ticket");
    {
        let mut cmd = cli(home);
        cmd.env("RUSTY_GRID_P2P_KEY_FILE", &env_key)
            .args([
                "master",
                "--listen",
                "127.0.0.1:0",
                "--web-ui-addr",
                "127.0.0.1:0",
                "--p2p-ticket-file",
            ])
            .arg(&env_ticket);

        let mut master = RunningMaster::spawn(cmd);
        let ticket = wait_for_non_empty_file(&env_ticket, Duration::from_secs(10))
            .expect("RUSTY_GRID_P2P_KEY_FILE must publish ticket");
        assert!(!ticket_endpoint_id(&ticket).is_empty());
        master.stop();
    }
    assert_eq!(
        fs::metadata(&env_key).expect("env_key exists").len(),
        32,
        "RUSTY_GRID_P2P_KEY_FILE must create a 32-byte key"
    );

    // 3. Precedence: `--p2p-key-file` CLI flag overrides `RUSTY_GRID_P2P_KEY_FILE`
    let cli_win_key = home.join("cli_win_key.bin");
    let env_lose_key = home.join("env_lose_key.bin");
    let prec_ticket = home.join("prec.ticket");
    {
        let mut cmd = cli(home);
        cmd.env("RUSTY_GRID_P2P_KEY_FILE", &env_lose_key)
            .args([
                "master",
                "--listen",
                "127.0.0.1:0",
                "--web-ui-addr",
                "127.0.0.1:0",
                "--p2p-key-file",
            ])
            .arg(&cli_win_key)
            .arg("--p2p-ticket-file")
            .arg(&prec_ticket);

        let mut master = RunningMaster::spawn(cmd);
        let _ = wait_for_non_empty_file(&prec_ticket, Duration::from_secs(10))
            .expect("CLI precedence master must publish ticket");
        master.stop();
    }
    assert_eq!(
        fs::metadata(&cli_win_key)
            .expect("cli_win_key exists")
            .len(),
        32
    );
    assert!(
        !env_lose_key.exists(),
        "--p2p-key-file must override RUSTY_GRID_P2P_KEY_FILE without creating the env key"
    );

    // 4. TOML config `p2p_key_file` and CLI override over config
    let cfg_key = home.join("cfg_key.bin");
    let cfg_ticket = home.join("cfg.ticket");
    let config_toml = home.join("master.toml");
    let toml_body = format!(
        "[master]\nlisten = \"127.0.0.1:0\"\np2p_ticket_file = \"{}\"\np2p_key_file = \"{}\"\n",
        cfg_ticket.display().to_string().replace('\\', "/"),
        cfg_key.display().to_string().replace('\\', "/"),
    );
    fs::write(&config_toml, toml_body).expect("write master.toml");
    {
        let mut cmd = cli(home);
        cmd.args(["master", "--web-ui-addr", "127.0.0.1:0", "--config"])
            .arg(&config_toml);

        let mut master = RunningMaster::spawn(cmd);
        let _ = wait_for_non_empty_file(&cfg_ticket, Duration::from_secs(10))
            .expect("config p2p_key_file master must publish ticket");
        master.stop();
    }
    assert_eq!(
        fs::metadata(&cfg_key).expect("cfg_key exists").len(),
        32,
        "config p2p_key_file must create a 32-byte key"
    );
}

#[test]
fn test_ephemeral_key_produces_distinct_identities_without_writing_default_key() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let home = tmp.path();

    let mut ids = Vec::new();
    for run in 1..=2 {
        let ticket_file = home.join(format!("eph_{run}.ticket"));
        let mut cmd = cli(home);
        cmd.args([
            "master",
            "--listen",
            "127.0.0.1:0",
            "--web-ui-addr",
            "127.0.0.1:0",
            "--p2p",
            "--ephemeral-key",
            "--p2p-ticket-file",
        ])
        .arg(&ticket_file);

        let mut master = RunningMaster::spawn(cmd);
        let ticket = wait_for_non_empty_file(&ticket_file, Duration::from_secs(10))
            .unwrap_or_else(|| panic!("ephemeral run {run} failed to publish ticket"));
        ids.push(ticket_endpoint_id(&ticket));
        master.stop();
    }

    assert_ne!(
        ids[0], ids[1],
        "--ephemeral-key runs must generate distinct P2P endpoint identities"
    );
    assert!(
        !home.join(".oxideswarm").join("master_key.bin").exists(),
        "--ephemeral-key must not create ~/.oxideswarm/master_key.bin"
    );
}

#[tokio::test]
async fn test_port_and_ticket_files_removed_on_shutdown() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();

    // 1. Ephemeral key mode: both port_file and p2p_ticket_file are removed on shutdown.
    let eph_port = dir.join("eph_shutdown.port");
    let eph_ticket = dir.join("eph_shutdown.ticket");
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let eph_cfg = ServerConfig::new(bind)
        .with_port_file(&eph_port)
        .with_p2p(true)
        .with_ephemeral_key(true)
        .with_p2p_ticket_file(&eph_ticket);

    let eph_master = MasterServer::spawn(eph_cfg)
        .await
        .expect("spawn ephemeral master");
    assert!(
        eph_port.exists(),
        "port file must exist while master is running"
    );
    assert!(
        eph_ticket.exists(),
        "ticket file must exist while master is running"
    );

    eph_master.shutdown().expect("shutdown ephemeral master");
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && (eph_port.exists() || eph_ticket.exists()) {
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    assert!(
        !eph_port.exists(),
        "port file must be removed on master shutdown"
    );
    assert!(
        !eph_ticket.exists(),
        "ephemeral p2p_ticket_file must be removed on master shutdown"
    );

    // 2. Persistent key mode: port_file is removed on shutdown while p2p_key_file is preserved.
    let persist_port = dir.join("persist_shutdown.port");
    let persist_key = dir.join("persist_shutdown.key");
    let persist_ticket = dir.join("persist_shutdown.ticket");
    let persist_cfg = ServerConfig::new(bind)
        .with_port_file(&persist_port)
        .with_p2p(true)
        .with_p2p_key_file(&persist_key)
        .with_p2p_ticket_file(&persist_ticket);

    let persist_master = MasterServer::spawn(persist_cfg)
        .await
        .expect("spawn persistent master");
    assert!(persist_port.exists());
    assert!(persist_key.exists());
    assert!(persist_ticket.exists());

    persist_master
        .shutdown()
        .expect("shutdown persistent master");
    let deadline2 = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline2 && persist_port.exists() {
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    assert!(
        !persist_port.exists(),
        "port file must be removed on persistent master shutdown"
    );
    assert_eq!(
        fs::metadata(&persist_key)
            .expect("persistent key file must remain after shutdown")
            .len(),
        32,
        "persistent key file must remain 32 bytes after shutdown"
    );

    let _ = PathBuf::from(&persist_ticket);
}
