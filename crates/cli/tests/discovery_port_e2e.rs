//! CLI-2: `master --discovery-port` moves or turns off the UDP discovery beacon (design 0084).
//!
//! Every port is picked from the OS and handed to the child, never 8088-8090, so the test runs next
//! to a real master. Two masters run at the same time: one with its beacon moved, one with it off.

use std::net::{TcpListener, TcpStream, UdpSocket};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

const BIN: &str = env!("CARGO_BIN_EXE_rusty-grid");

struct Master(Child);

impl Drop for Master {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// Bind-and-drop kept (TEST-52): `--listen 127.0.0.1:0` turns the beacon off (`ServerConfig::new`).
fn free_tcp_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

// Bind-and-drop kept (TEST-52): `--discovery-port 0` turns the beacon off; no port is published.
fn free_udp_port() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Starts `rusty-grid master` on loopback in a private HOME, with `extra` arguments.
fn start_master(home: &Path, tcp_port: u16, extra: &[&str], env: &[(&str, &str)]) -> Master {
    let mut cmd = Command::new(BIN);
    cmd.arg("master")
        .arg("--listen")
        .arg(format!("127.0.0.1:{tcp_port}"))
        .args(extra)
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env("OXIDESWARM_HOME", home.join(".oxideswarm"))
        .env("RUSTY_GRID_DISABLE_WEB_UI", "1")
        .env("NO_COLOR", "1")
        .env_remove("RUSTY_GRID_DISABLE_DISCOVERY")
        .env_remove("RUSTY_GRID_DISCOVERY_PORT")
        .env_remove("RUSTY_GRID_CONFIG")
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("OXIDESWARM_SECRET_FILE")
        .current_dir(home)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for (k, v) in env {
        cmd.env(k, v);
    }
    let master = Master(cmd.spawn().expect("spawn master"));
    let deadline = Instant::now() + Duration::from_secs(20);
    while TcpStream::connect(("127.0.0.1", tcp_port)).is_err() {
        assert!(Instant::now() < deadline, "master never listened");
        std::thread::sleep(Duration::from_millis(50));
    }
    master
}

async fn probe(udp_port: u16) -> Option<rusty_grid_core::MasterBeacon> {
    rusty_grid_core::discover_master(Duration::from_millis(2500), udp_port).await
}

#[tokio::test]
async fn beacon_moves_with_the_flag_and_a_second_master_runs_with_it_off() {
    let home_a = TempDir::new().unwrap();
    let home_b = TempDir::new().unwrap();
    let (tcp_a, tcp_b) = (free_tcp_port(), free_tcp_port());
    let (udp_a, udp_b) = (free_udp_port(), free_udp_port());

    let _a = start_master(
        home_a.path(),
        tcp_a,
        &["--discovery-port", &udp_a.to_string()],
        &[],
    );
    let _b = start_master(home_b.path(), tcp_b, &["--discovery-port", "0"], &[]);

    let found = probe(udp_a).await.expect("master A answers on its port");
    assert!(
        found.cluster_addr.ends_with(&format!(":{tcp_a}")),
        "beacon on {udp_a} must name master A, got {}",
        found.cluster_addr
    );
    assert!(
        probe(udp_b).await.is_none(),
        "master B has its beacon off, nothing answers on {udp_b}"
    );
    // Master B is up and serving even though it has no beacon.
    assert!(TcpStream::connect(("127.0.0.1", tcp_b)).is_ok());
}

#[tokio::test]
async fn the_environment_variable_moves_the_beacon_and_the_flag_outranks_it() {
    let home = TempDir::new().unwrap();
    let tcp = free_tcp_port();
    let (env_port, flag_port) = (free_udp_port(), free_udp_port());
    let _m = start_master(
        home.path(),
        tcp,
        &["--discovery-port", &flag_port.to_string()],
        &[("RUSTY_GRID_DISCOVERY_PORT", &env_port.to_string())],
    );
    assert!(
        probe(flag_port).await.is_some(),
        "the flag outranks the environment"
    );
    assert!(probe(env_port).await.is_none());

    let home2 = TempDir::new().unwrap();
    let tcp2 = free_tcp_port();
    let env_only = free_udp_port();
    let _m2 = start_master(
        home2.path(),
        tcp2,
        &[],
        &[("RUSTY_GRID_DISCOVERY_PORT", &env_only.to_string())],
    );
    assert!(probe(env_only).await.is_some(), "the environment moves it");
}

#[tokio::test]
async fn the_disable_variable_still_wins_over_a_port() {
    let home = TempDir::new().unwrap();
    let tcp = free_tcp_port();
    let udp = free_udp_port();
    let _m = start_master(
        home.path(),
        tcp,
        &["--discovery-port", &udp.to_string()],
        &[("RUSTY_GRID_DISABLE_DISCOVERY", "1")],
    );
    assert!(probe(udp).await.is_none());
}
