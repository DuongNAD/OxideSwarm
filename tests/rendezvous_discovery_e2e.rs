//! End-to-end integration tests for usable Tier 3 rendezvous discovery (`FEAT-7`).
//!
//! All tests isolate `HOME` / `USERPROFILE` into temporary directories (`OXIDESWARM_HOME`
//! removed), bind loopback ephemeral ports (`127.0.0.1:0` with `--port-file`), and disable UDP
//! LAN discovery and `:8080` dashboards/portals so the real workspace `~/.oxideswarm` and default
//! ports are never touched.

use std::fs;
use std::net::UdpSocket;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::discovery::{
    derive_rendezvous_key, discover_master_rendezvous, parse_rendezvous_url,
    register_rendezvous_lease, RendezvousLease, RENDEZVOUS_ENV,
};
use rusty_grid_master::{spawn_rendezvous_server, KeyedRendezvousConfig};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const BIN: &str = env!("CARGO_BIN_EXE_rusty-grid");

fn isolated_cli(home: &Path) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.env("HOME", home)
        .env("USERPROFILE", home)
        .current_dir(home)
        .env_remove("OXIDESWARM_HOME")
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("OXIDESWARM_SECRET_FILE")
        .env_remove("OXIDESWARM_MASTER")
        .env_remove("OXIDESWARM_RENDEZVOUS")
        .env_remove("OXIDESWARM_RENDEZVOUS_LISTEN")
        .env_remove("RUSTY_GRID_RENDEZVOUS")
        .env_remove("RUSTY_GRID_CONFIG")
        .env_remove("RUSTY_GRID_MASTER")
        .env_remove("RUSTY_GRID_MASTER_ADDR")
        .env_remove("RUSTY_GRID_LISTEN")
        .env_remove("RUSTY_GRID_MASTER_LISTEN")
        .env_remove("RUSTY_GRID_INSECURE")
        .env_remove("RUSTY_GRID_P2P")
        .env_remove("RUSTY_GRID_P2P_TICKET")
        .env("RUSTY_GRID_P2P_MINIMAL", "1")
        .env("RUSTY_GRID_DISABLE_DISCOVERY", "1")
        .env("RUSTY_GRID_DISABLE_WEB_UI", "1")
        .env("OXIDESWARM_DISCOVERY_CANDIDATES", "127.0.0.1:9")
        .env("RUSTY_GRID_NO_BATTERY", "1")
        .env("NO_COLOR", "1")
        .stdin(Stdio::null());
    cmd
}

struct ChildService {
    child: Child,
    log: PathBuf,
}

impl ChildService {
    fn spawn(mut cmd: Command, log: PathBuf) -> Self {
        let out = fs::File::create(&log).expect("create child log file");
        let err = out.try_clone().expect("clone child log handle");
        cmd.stdout(Stdio::from(out)).stderr(Stdio::from(err));
        let child = cmd.spawn().expect("spawn rusty-grid subprocess");
        Self { child, log }
    }

    fn log(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }
}

impl Drop for ChildService {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn wait_for_port_file(path: &Path, service: &ChildService, timeout: Duration) -> u16 {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Ok(raw) = fs::read_to_string(path) {
            if let Ok(port) = raw.trim().parse::<u16>() {
                return port;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!(
        "Timed out waiting for port file {} after {:?}.\nService log:\n{}",
        path.display(),
        timeout,
        service.log()
    );
}

fn run_with_timeout(mut cmd: Command, timeout: Duration) -> Output {
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().expect("spawn command");
    let start = Instant::now();
    loop {
        if child.try_wait().expect("poll child").is_some() {
            return child.wait_with_output().expect("collect output");
        }
        if start.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            panic!("Command timed out after {:?}", timeout);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn test_worker_auto_rendezvous_registers_with_tiers_1_and_2_disabled() {
    let root = TempDir::new().expect("tempdir root");
    let rv_home = root.path().join("rendezvous_home");
    let master_home = root.path().join("master_home");
    let worker_home = root.path().join("worker_home");
    let client_home = root.path().join("client_home");
    for dir in [&rv_home, &master_home, &worker_home, &client_home] {
        fs::create_dir_all(dir).expect("create isolated home");
    }

    // Generate one shared secret file in an isolated location.
    let secret_path = root.path().join("cluster.secret");
    let mut gen_cmd = isolated_cli(&master_home);
    gen_cmd.args(["secret", "generate", "--out"]);
    gen_cmd.arg(&secret_path);
    let gen_out = run_with_timeout(gen_cmd, Duration::from_secs(10));
    assert!(
        gen_out.status.success(),
        "secret generate failed: {}",
        String::from_utf8_lossy(&gen_out.stderr)
    );

    // Hold a silent UDP socket on loopback so Tier 1 (`auto:<silent_port>`) never receives a
    // beacon, while `OXIDESWARM_DISCOVERY_CANDIDATES=127.0.0.1:9` + `RUSTY_GRID_DISABLE_WEB_UI=1`
    // ensure Tier 2 HTTP probing cannot find the master.
    let silent_udp = UdpSocket::bind("127.0.0.1:0").expect("bind silent UDP socket");
    let silent_udp_port = silent_udp.local_addr().unwrap().port();

    // 1. Start standalone `oxideswarm rendezvous` WITHOUT `--secret-file`.
    let rv_port_file = root.path().join("rendezvous.port");
    let mut rv_cmd = isolated_cli(&rv_home);
    rv_cmd.args(["rendezvous", "--listen", "127.0.0.1:0", "--port-file"]);
    rv_cmd.arg(&rv_port_file);
    let rv_service = ChildService::spawn(rv_cmd, root.path().join("rendezvous.log"));
    let rv_port = wait_for_port_file(&rv_port_file, &rv_service, Duration::from_secs(10));
    let rv_url = format!("http://127.0.0.1:{rv_port}");

    // 2. Start Master configured with `--rendezvous <URL>`.
    let master_port_file = root.path().join("master.port");
    let mut master_cmd = isolated_cli(&master_home);
    master_cmd.args(["master", "--listen", "127.0.0.1:0", "--port-file"]);
    master_cmd.arg(&master_port_file);
    master_cmd.args(["--rendezvous", &rv_url, "--secret-file"]);
    master_cmd.arg(&secret_path);
    let master_service = ChildService::spawn(master_cmd, root.path().join("master.log"));
    let _master_port =
        wait_for_port_file(&master_port_file, &master_service, Duration::from_secs(15));

    // 3. Start Worker using `--master auto:<silent_udp_port> --rendezvous <URL>`.
    let mut worker_cmd = isolated_cli(&worker_home);
    worker_cmd.args([
        "worker",
        "--master",
        &format!("auto:{silent_udp_port}"),
        "--rendezvous",
        &rv_url,
        "--secret-file",
    ]);
    worker_cmd.arg(&secret_path);
    worker_cmd.args([
        "--no-portal",
        "--no-gpu",
        "--name",
        "rv-worker",
        "--max-concurrency",
        "1",
    ]);
    let worker_service = ChildService::spawn(worker_cmd, root.path().join("worker.log"));

    // 4. Poll `workers --master auto:<silent_udp_port>` via Tier 3 until `rv-worker` registers.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last_workers_stderr = String::new();
    let mut registered = false;
    while Instant::now() < deadline {
        let mut workers_cmd = isolated_cli(&client_home);
        workers_cmd.env(RENDEZVOUS_ENV, &rv_url);
        workers_cmd.args([
            "workers",
            "--master",
            &format!("auto:{silent_udp_port}"),
            "--secret-file",
        ]);
        workers_cmd.arg(&secret_path);
        let out = run_with_timeout(workers_cmd, Duration::from_secs(10));
        let stdout = String::from_utf8_lossy(&out.stdout);
        last_workers_stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        if out.status.success() && stdout.contains("rv-worker") {
            registered = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(400));
    }
    assert!(
        registered,
        "Worker must register via Tier 3 rendezvous with Tiers 1 and 2 disabled.\nLast stderr: {last_workers_stderr}\nRendezvous log:\n{}\nMaster log:\n{}\nWorker log:\n{}",
        rv_service.log(),
        master_service.log(),
        worker_service.log()
    );

    // 5. Submit and wait on a command discovered purely via Tier 3 rendezvous.
    let mut submit_cmd = isolated_cli(&client_home);
    submit_cmd.env(RENDEZVOUS_ENV, &rv_url);
    submit_cmd.args([
        "submit",
        "--master",
        &format!("auto:{silent_udp_port}"),
        "--secret-file",
    ]);
    submit_cmd.arg(&secret_path);
    submit_cmd.args(["--wait", "--", "echo", "rendezvous-ok"]);
    let submit_out = run_with_timeout(submit_cmd, Duration::from_secs(20));
    let submit_stdout = String::from_utf8_lossy(&submit_out.stdout);
    let submit_stderr = String::from_utf8_lossy(&submit_out.stderr);
    assert!(
        submit_out.status.success() && submit_stdout.contains("rendezvous-ok"),
        "submit over auto + Tier 3 rendezvous failed.\nstdout: {submit_stdout}\nstderr: {submit_stderr}"
    );
}

async fn raw_http_get(addr: std::net::SocketAddr, path: &str) -> u16 {
    let mut stream = TcpStream::connect(addr).await.expect("connect rendezvous");
    let req = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.expect("write GET");
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf).await;
    let text = String::from_utf8_lossy(&buf);
    text.lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or(0)
}

#[tokio::test]
async fn test_keyed_rendezvous_multi_cluster_isolation_and_anti_harvesting() {
    let handle = spawn_rendezvous_server(
        "127.0.0.1:0".parse().unwrap(),
        KeyedRendezvousConfig::default(),
    )
    .await
    .expect("spawn standalone keyed rendezvous server");
    let addr = handle.local_addr();
    let rv_url = format!("http://{addr}");

    let secret_a = ClusterSecret::generate().unwrap();
    let secret_b = ClusterSecret::generate().unwrap();
    let secret_wrong = ClusterSecret::generate().unwrap();

    let mut lease_a = RendezvousLease::new(
        "127.0.0.1:19101",
        "http://127.0.0.1:18101",
        "master-a",
        Some("ticket-cluster-a".to_string()),
        1,
        120,
    );
    lease_a.sign(&secret_a);

    let mut lease_b = RendezvousLease::new(
        "127.0.0.1:19102",
        "http://127.0.0.1:18102",
        "master-b",
        Some("ticket-cluster-b".to_string()),
        2,
        120,
    );
    lease_b.sign(&secret_b);

    register_rendezvous_lease(&rv_url, &lease_a)
        .await
        .expect("register cluster A lease");
    register_rendezvous_lease(&rv_url, &lease_b)
        .await
        .expect("register cluster B lease");

    // Each cluster retrieves only its own master address and verified p2p_ticket.
    let beacon_a = discover_master_rendezvous(&rv_url, Some(&secret_a), Duration::from_secs(2))
        .await
        .expect("discover cluster A");
    assert_eq!(beacon_a.cluster_addr, "127.0.0.1:19101");
    assert_eq!(beacon_a.p2p_ticket.as_deref(), Some("ticket-cluster-a"));

    let beacon_b = discover_master_rendezvous(&rv_url, Some(&secret_b), Duration::from_secs(2))
        .await
        .expect("discover cluster B");
    assert_eq!(beacon_b.cluster_addr, "127.0.0.1:19102");
    assert_eq!(beacon_b.p2p_ticket.as_deref(), Some("ticket-cluster-b"));

    // Wrong or missing secret cannot harvest leases or read keyed slots.
    assert!(
        discover_master_rendezvous(&rv_url, Some(&secret_wrong), Duration::from_secs(1))
            .await
            .is_none()
    );
    assert!(
        discover_master_rendezvous(&rv_url, None, Duration::from_secs(1))
            .await
            .is_none()
    );
    let key_a = derive_rendezvous_key(&secret_a);
    let unauth_status = raw_http_get(addr, &format!("/api/rendezvous/master?key={key_a}")).await;
    assert_eq!(
        unauth_status, 401,
        "Unauthenticated read of keyed slot without X-OxideSwarm-Lease-Auth must return 401"
    );

    // Unsigned lease registration is rejected fail-closed by default.
    let unsigned = RendezvousLease::new("127.0.0.1:19999", "", "rogue", None, 0, 120);
    assert!(register_rendezvous_lease(&rv_url, &unsigned).await.is_err());

    // Tampering with p2p_ticket after signing is rejected on registration and verification.
    let mut tampered = lease_a.clone();
    tampered.p2p_ticket = Some("ticket-tampered".to_string());
    assert!(!tampered.verify(&secret_a, 30));
    assert!(!tampered.verify_key_signature(30));
    assert!(register_rendezvous_lease(&rv_url, &tampered).await.is_err());

    handle.shutdown().await;
}

#[test]
fn test_cli_rejects_https_rendezvous_url_and_non_loopback_unsigned() {
    let https_err = parse_rendezvous_url("https://rendezvous.example.com:8090")
        .expect_err("https:// URL must be refused");
    assert!(https_err.contains("https://"));

    let home = TempDir::new().expect("temp home");

    let mut master_cmd = isolated_cli(home.path());
    master_cmd.args([
        "master",
        "--rendezvous",
        "https://rendezvous.example.com:8090",
    ]);
    let master_out = run_with_timeout(master_cmd, Duration::from_secs(10));
    assert!(!master_out.status.success());
    let master_err = String::from_utf8_lossy(&master_out.stderr);
    assert!(
        master_err.contains("https://"),
        "master stderr must explain why https:// was rejected: {master_err}"
    );

    let mut worker_cmd = isolated_cli(home.path());
    worker_cmd.args([
        "worker",
        "--rendezvous",
        "https://rendezvous.example.com:8090",
    ]);
    let worker_out = run_with_timeout(worker_cmd, Duration::from_secs(10));
    assert!(!worker_out.status.success());
    let worker_err = String::from_utf8_lossy(&worker_out.stderr);
    assert!(
        worker_err.contains("https://"),
        "worker stderr must explain why https:// was rejected: {worker_err}"
    );

    let mut rv_cmd = isolated_cli(home.path());
    rv_cmd.args(["rendezvous", "--listen", "0.0.0.0:0", "--allow-unsigned"]);
    let rv_out = run_with_timeout(rv_cmd, Duration::from_secs(10));
    assert!(!rv_out.status.success());
    let rv_err = String::from_utf8_lossy(&rv_out.stderr);
    assert!(
        rv_err.contains("--insecure"),
        "non-loopback --allow-unsigned without --insecure must require --insecure: {rv_err}"
    );
}
