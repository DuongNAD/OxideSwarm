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
        .env("RUSTY_GRID_P2P_MINIMAL", "1")
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

fn run_with_timeout(mut cmd: Command, timeout: Duration) -> std::process::Output {
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().expect("spawn rusty-grid");
    let mut stdout_pipe = child.stdout.take().expect("take child stdout");
    let mut stderr_pipe = child.stderr.take().expect("take child stderr");

    let stdout_reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = std::io::Read::read_to_end(&mut stdout_pipe, &mut buf);
        buf
    });
    let stderr_reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = std::io::Read::read_to_end(&mut stderr_pipe, &mut buf);
        buf
    });

    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait().expect("try_wait on rusty-grid child") {
            Some(status) => {
                let _ = child.kill();
                let _ = child.wait();
                break status;
            }
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "rusty-grid child did not exit within {:?}; killed child",
                    timeout
                );
            }
            None => std::thread::sleep(Duration::from_millis(25)),
        }
    };

    let stdout = stdout_reader.join().expect("join stdout reader");
    let stderr = stderr_reader.join().expect("join stderr reader");
    std::process::Output {
        status,
        stdout,
        stderr,
    }
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

        let output = run_with_timeout(cmd, Duration::from_secs(10));

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

    // 4. Precedence: `RUSTY_GRID_P2P_KEY_FILE` env var overrides TOML config `p2p_key_file`
    let env_over_cfg_key = home.join("env_over_cfg_key.bin");
    let cfg_lose_env_key = home.join("cfg_lose_env_key.bin");
    let env_over_cfg_ticket = home.join("env_over_cfg.ticket");
    let cfg_lose_env_toml = home.join("cfg_lose_env.toml");
    fs::write(
        &cfg_lose_env_toml,
        format!(
            "[master]\nlisten = \"127.0.0.1:0\"\np2p_ticket_file = \"{}\"\np2p_key_file = \"{}\"\n",
            env_over_cfg_ticket.display().to_string().replace('\\', "/"),
            cfg_lose_env_key.display().to_string().replace('\\', "/"),
        ),
    )
    .expect("write cfg_lose_env.toml");
    {
        let mut cmd = cli(home);
        cmd.env("RUSTY_GRID_P2P_KEY_FILE", &env_over_cfg_key)
            .args(["master", "--web-ui-addr", "127.0.0.1:0", "--config"])
            .arg(&cfg_lose_env_toml);

        let mut master = RunningMaster::spawn(cmd);
        let _ = wait_for_non_empty_file(&env_over_cfg_ticket, Duration::from_secs(10))
            .expect("env-over-config master must publish ticket");
        master.stop();
    }
    assert_eq!(
        fs::metadata(&env_over_cfg_key)
            .expect("env_over_cfg_key exists")
            .len(),
        32
    );
    assert!(
        !cfg_lose_env_key.exists(),
        "RUSTY_GRID_P2P_KEY_FILE must override config p2p_key_file without creating config key"
    );

    // 5. Precedence: `--p2p-key-file` CLI flag overrides TOML config `p2p_key_file`
    let cli_over_cfg_key = home.join("cli_over_cfg_key.bin");
    let cfg_lose_cli_key = home.join("cfg_lose_cli_key.bin");
    let cli_over_cfg_ticket = home.join("cli_over_cfg.ticket");
    let cfg_lose_cli_toml = home.join("cfg_lose_cli.toml");
    fs::write(
        &cfg_lose_cli_toml,
        format!(
            "[master]\nlisten = \"127.0.0.1:0\"\np2p_ticket_file = \"{}\"\np2p_key_file = \"{}\"\n",
            cli_over_cfg_ticket.display().to_string().replace('\\', "/"),
            cfg_lose_cli_key.display().to_string().replace('\\', "/"),
        ),
    )
    .expect("write cfg_lose_cli.toml");
    {
        let mut cmd = cli(home);
        cmd.args(["master", "--web-ui-addr", "127.0.0.1:0", "--p2p-key-file"])
            .arg(&cli_over_cfg_key)
            .arg("--config")
            .arg(&cfg_lose_cli_toml);

        let mut master = RunningMaster::spawn(cmd);
        let _ = wait_for_non_empty_file(&cli_over_cfg_ticket, Duration::from_secs(10))
            .expect("CLI-over-config master must publish ticket");
        master.stop();
    }
    assert_eq!(
        fs::metadata(&cli_over_cfg_key)
            .expect("cli_over_cfg_key exists")
            .len(),
        32
    );
    assert!(
        !cfg_lose_cli_key.exists(),
        "--p2p-key-file must override config p2p_key_file without creating config key"
    );

    // 6. TOML config `p2p_key_file` when CLI flag and env var are absent
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
        .with_p2p_minimal_preset(true)
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
        .with_p2p_minimal_preset(true)
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

#[test]
fn test_p2p_minimal_env_warns_on_stderr_and_writes_ticket_under_3s() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let ticket_file = dir.join("minimal_cli.ticket");
    let rust_log = std::env::var("RUST_LOG").unwrap_or_else(|_| "iroh=debug,info".to_string());

    // 1. With RUSTY_GRID_P2P_MINIMAL=1 (set by `cli(dir)`), `master --listen 127.0.0.1:0 --p2p
    //    --ephemeral-key` writes its P2P ticket in under 3 s, emits the minimal preset warning
    //    once on stderr, and makes no relay or pkarr connection attempts.
    let mut m_cmd = cli(dir);
    m_cmd
        .env("RUST_LOG", &rust_log)
        .args([
            "master",
            "--listen",
            "127.0.0.1:0",
            "--p2p",
            "--ephemeral-key",
            "--insecure",
            "--p2p-ticket-file",
        ])
        .arg(&ticket_file)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let started = Instant::now();
    let mut m_child = m_cmd.spawn().expect("spawn master with minimal preset");
    let m_stdout_pipe = m_child.stdout.take().expect("take master stdout");
    let m_stderr_pipe = m_child.stderr.take().expect("take master stderr");
    let m_stdout_reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = std::io::Read::read_to_end(&mut { m_stdout_pipe }, &mut buf);
        String::from_utf8_lossy(&buf).into_owned()
    });
    let m_stderr_reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = std::io::Read::read_to_end(&mut { m_stderr_pipe }, &mut buf);
        String::from_utf8_lossy(&buf).into_owned()
    });

    let ticket = wait_for_non_empty_file(&ticket_file, Duration::from_secs(3))
        .expect("ticket must be written in under 3 s when RUSTY_GRID_P2P_MINIMAL=1");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(3),
        "ticket write took {:?} (>= 3 s); CLI minimal preset wiring was not applied",
        elapsed
    );
    let _ = m_child.kill();
    let _ = m_child.wait();
    let m_stdout = m_stdout_reader.join().expect("join master stdout reader");
    let m_stderr = m_stderr_reader.join().expect("join master stderr reader");
    eprint!("{m_stdout}{m_stderr}");
    assert_eq!(
        m_stderr
            .matches("P2P minimal preset is enabled (p2p_minimal_preset=true)")
            .count(),
        1,
        "master must emit the P2P minimal preset warning exactly once on stderr, got: {m_stderr:?}"
    );
    for forbidden in ["relay.n0.iroh.link", "dns.iroh.link", "pkarr"] {
        assert!(
            !m_stdout.contains(forbidden) && !m_stderr.contains(forbidden),
            "master with RUSTY_GRID_P2P_MINIMAL=1 must not contact {forbidden}, stdout={m_stdout:?}, stderr={m_stderr:?}"
        );
    }

    // 2. With RUSTY_GRID_P2P_MINIMAL=1, `worker --p2p-ticket <ticket>` also forwards the preset
    //    into WorkerConfig, emits the warning once on stderr, and makes no relay or pkarr
    //    connection attempts.
    let mut w_cmd = cli(dir);
    w_cmd
        .env("RUST_LOG", &rust_log)
        .args([
            "worker",
            "--p2p-ticket",
            &ticket,
            "--insecure",
            "--no-gpu",
            "--no-cuda",
            "--no-blender",
            "--no-portal",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut w_child = w_cmd.spawn().expect("spawn worker with minimal preset");
    let mut w_stdout_pipe = w_child.stdout.take().expect("take worker stdout");
    let w_stderr_pipe = w_child.stderr.take().expect("take worker stderr");
    let w_stderr_reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = std::io::Read::read_to_end(&mut { w_stderr_pipe }, &mut buf);
        String::from_utf8_lossy(&buf).into_owned()
    });
    let w_stdout_reader = std::thread::spawn(move || {
        use std::io::BufRead;
        let mut collected = String::new();
        let reader = std::io::BufReader::new(&mut w_stdout_pipe);
        for line in reader.lines().map_while(Result::ok) {
            collected.push_str(&line);
            collected.push('\n');
            if line.contains("RustyGrid Worker started") {
                break;
            }
        }
        collected
    });
    let w_deadline = Instant::now() + Duration::from_secs(3);
    while !w_stdout_reader.is_finished() && Instant::now() < w_deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    let _ = w_child.kill();
    let _ = w_child.wait();
    let w_stdout = w_stdout_reader.join().expect("join worker stdout reader");
    let w_stderr = w_stderr_reader.join().expect("join worker stderr reader");
    eprint!("{w_stdout}{w_stderr}");
    assert_eq!(
        w_stderr
            .matches("P2P minimal preset is enabled (p2p_minimal_preset=true)")
            .count(),
        1,
        "worker must emit the P2P minimal preset warning exactly once on stderr, got: {w_stderr:?}"
    );
    for forbidden in ["relay.n0.iroh.link", "dns.iroh.link", "pkarr"] {
        assert!(
            !w_stdout.contains(forbidden) && !w_stderr.contains(forbidden),
            "worker with RUSTY_GRID_P2P_MINIMAL=1 must not contact {forbidden}, stdout={w_stdout:?}, stderr={w_stderr:?}"
        );
    }
}

#[test]
#[ignore = "requires outbound network to relay.n0.iroh.link"]
fn test_p2p_default_env_omits_minimal_preset_warning_on_stderr() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();

    // Without RUSTY_GRID_P2P_MINIMAL, `master --p2p` uses `presets::N0` and does not emit the
    // minimal preset warning on stderr.
    let mut no_min_cmd = cli(dir);
    no_min_cmd
        .env_remove("RUSTY_GRID_P2P_MINIMAL")
        .args([
            "master",
            "--listen",
            "127.0.0.1:0",
            "--p2p",
            "--ephemeral-key",
            "--insecure",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut no_min_child = no_min_cmd
        .spawn()
        .expect("spawn master without minimal preset");
    let mut no_min_stdout = no_min_child
        .stdout
        .take()
        .expect("take no_min master stdout");
    let no_min_stderr_pipe = no_min_child
        .stderr
        .take()
        .expect("take no_min master stderr");
    let no_min_stderr_reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = std::io::Read::read_to_end(&mut { no_min_stderr_pipe }, &mut buf);
        String::from_utf8_lossy(&buf).into_owned()
    });
    let no_min_stdout_reader = std::thread::spawn(move || {
        use std::io::BufRead;
        let reader = std::io::BufReader::new(&mut no_min_stdout);
        for line in reader.lines().map_while(Result::ok) {
            if line.contains("Starting RustyGrid Master") {
                break;
            }
        }
    });
    let no_min_deadline = Instant::now() + Duration::from_secs(3);
    while !no_min_stdout_reader.is_finished() && Instant::now() < no_min_deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    std::thread::sleep(Duration::from_millis(250));
    let _ = no_min_child.kill();
    let _ = no_min_child.wait();
    let _ = no_min_stdout_reader.join();
    let no_min_stderr = no_min_stderr_reader
        .join()
        .expect("join no_min stderr reader");
    assert!(
        !no_min_stderr.contains("P2P minimal preset is enabled"),
        "without RUSTY_GRID_P2P_MINIMAL, master must not emit the minimal preset warning, got: {no_min_stderr:?}"
    );
}
