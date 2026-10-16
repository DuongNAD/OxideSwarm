//! End-to-end checks of the CLI master's dashboard policy and of `mapreduce`'s exit status, driving
//! the real `rusty-grid` binary as separate processes.
//!
//! * A bare loopback master has a usable local dashboard (no login) and says so on startup.
//! * A master listening beyond loopback keeps its dashboard on loopback unless `--web-ui-addr` names
//!   another interface, and then it needs the dashboard token.
//! * `--dev` is ignored, loudly, when the dashboard listener is not on a loopback address, and a
//!   non-loopback dashboard prints a plain-HTTP warning.
//! * `mapreduce` exits non-zero when the job failed.
//!
//! Every process gets a private `HOME`, so nothing here touches the developer's `~/.oxideswarm`.

#![cfg(unix)]

use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tempfile::TempDir;

const BIN: &str = env!("CARGO_BIN_EXE_rusty-grid");

static SEQ: AtomicUsize = AtomicUsize::new(0);

fn cli(home: &Path) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.env("HOME", home)
        .env("USERPROFILE", home)
        .env_remove("OXIDESWARM_HOME")
        .current_dir(home)
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("OXIDESWARM_SECRET_FILE")
        .env_remove("OXIDE_DEV_MODE")
        .env_remove("RUSTY_GRID_CONFIG")
        .env_remove("RUSTY_GRID_MASTER")
        .env_remove("RUSTY_GRID_MASTER_ADDR")
        .env_remove("RUSTY_GRID_LISTEN")
        .env_remove("RUSTY_GRID_MASTER_LISTEN")
        .env_remove("RUSTY_GRID_INSECURE")
        .env_remove("RUSTY_GRID_P2P")
        .env_remove("RUSTY_GRID_ENABLE_WEB_UI")
        .env_remove("RUSTY_GRID_DISABLE_WEB_UI")
        .env("RUSTY_GRID_DISABLE_DISCOVERY", "1")
        .env("NO_COLOR", "1")
        .stdin(Stdio::null());
    cmd
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn wait_for(timeout: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if condition() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    condition()
}

struct Service {
    child: Child,
    log: PathBuf,
}

impl Service {
    fn spawn(mut cmd: Command, log: PathBuf) -> Self {
        let out = fs::File::create(&log).expect("create log");
        let err = out.try_clone().expect("clone log handle");
        cmd.stdout(Stdio::from(out)).stderr(Stdio::from(err));
        let child = cmd.spawn().expect("spawn rusty-grid service");
        Self { child, log }
    }

    fn log(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Master {
    service: Service,
    grid: SocketAddr,
    dashboard: SocketAddr,
}

/// Starts `rusty-grid master --listen <listen> ...` and waits until both ports are published.
fn start_master(home: &Path, listen: &str, args: &[&str]) -> Master {
    let id = SEQ.fetch_add(1, Ordering::SeqCst);
    let port_file = home.join(format!("master-{id}.port"));
    let dash_file = home.join(format!("dashboard-{id}.port"));
    let mut cmd = cli(home);
    cmd.args(["master", "--listen", listen, "--port-file"])
        .arg(&port_file)
        .arg("--dashboard-port-file")
        .arg(&dash_file)
        .args(args);
    let service = Service::spawn(cmd, home.join(format!("master-{id}.log")));
    let read_port = |file: &Path| {
        fs::read_to_string(file)
            .ok()
            .and_then(|p| p.trim().parse::<u16>().ok())
    };
    assert!(
        wait_for(Duration::from_secs(20), || {
            read_port(&port_file).is_some() && read_port(&dash_file).is_some()
        }),
        "master did not publish its ports; log:\n{}",
        service.log()
    );
    // Give the banner a moment to be flushed after the ports appear.
    wait_for(Duration::from_secs(5), || {
        service.log().contains("Embedded Dashboard available")
    });
    Master {
        grid: SocketAddr::from(([127, 0, 0, 1], read_port(&port_file).unwrap())),
        dashboard: SocketAddr::from(([127, 0, 0, 1], read_port(&dash_file).unwrap())),
        service,
    }
}

/// A minimal HTTP/1.1 GET; returns the status code.
fn http_status(addr: SocketAddr, path: &str, bearer: Option<&str>) -> u16 {
    let mut stream =
        TcpStream::connect_timeout(&addr, Duration::from_secs(5)).expect("connect dashboard");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let auth = bearer
        .map(|t| format!("Authorization: Bearer {t}\r\n"))
        .unwrap_or_default();
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\n{auth}Connection: close\r\n\r\n"
    )
    .unwrap();
    let mut response = Vec::new();
    let _ = stream.read_to_end(&mut response);
    let head = String::from_utf8_lossy(&response);
    head.split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no HTTP status in {head:?}"))
}

fn home() -> TempDir {
    tempfile::tempdir().expect("tempdir")
}

#[cfg(unix)]
#[test]
fn test_a_bare_loopback_master_serves_a_local_dashboard_without_a_login() {
    let home = home();
    let master = start_master(home.path(), "127.0.0.1:0", &["--dashboard-port", "0"]);
    let log = master.service.log();

    assert!(master.dashboard.ip().is_loopback());
    assert_eq!(
        http_status(master.dashboard, "/api/status", None),
        200,
        "a bare local master must have a usable dashboard; log:\n{log}"
    );
    assert!(
        log.contains("Dashboard access: this machine only, no login"),
        "the banner must say that local access is open; log:\n{log}"
    );
    assert!(
        !log.contains("WARNING"),
        "a loopback dashboard needs no plain-HTTP warning; log:\n{log}"
    );
}

#[cfg(unix)]
#[test]
fn test_an_explicit_token_keeps_even_a_loopback_dashboard_locked() {
    let home = home();
    let master = start_master(
        home.path(),
        "127.0.0.1:0",
        &[
            "--dashboard-port",
            "0",
            "--auth-token",
            "ops-token:ops:admin",
        ],
    );
    assert_eq!(http_status(master.dashboard, "/api/status", None), 401);
    assert_eq!(
        http_status(master.dashboard, "/api/status", Some("ops-token")),
        200
    );
    assert!(
        !master.service.log().contains("this machine only, no login"),
        "{}",
        master.service.log()
    );
}

#[cfg(unix)]
#[test]
fn test_a_master_listening_on_all_interfaces_keeps_its_dashboard_on_loopback_and_locked() {
    let home = home();
    // `--listen 0.0.0.0` brings in the cluster secret policy (a secret is generated in HOME)...
    let master = start_master(home.path(), "0.0.0.0:0", &["--dashboard-port", "0"]);
    let log = master.service.log();

    // ...but does not drag the dashboard onto the network.
    assert!(
        log.contains("Embedded Dashboard available at http://127.0.0.1:"),
        "the dashboard must stay on loopback; log:\n{log}"
    );
    assert_eq!(
        http_status(master.dashboard, "/api/status", None),
        401,
        "log:\n{log}"
    );

    let token = {
        let mut cmd = cli(home.path());
        cmd.args(["secret", "show", "--dashboard-token"]);
        let out: Output = cmd.output().expect("secret show");
        assert!(out.status.success(), "{}", text(&out.stderr));
        text(&out.stdout).trim().to_string()
    };
    assert_eq!(
        http_status(master.dashboard, "/api/status", Some(&token)),
        200
    );
    assert!(log.contains("secret show --dashboard-token"), "{log}");
}

#[cfg(unix)]
#[test]
fn test_dev_mode_is_ignored_and_a_plain_http_warning_printed_for_a_non_loopback_dashboard() {
    let home = home();
    // The operator explicitly publishes the dashboard on every interface and also asks for --dev.
    let master = start_master(
        home.path(),
        "127.0.0.1:0",
        &["--web-ui-addr", "0.0.0.0:0", "--dev"],
    );
    let log = master.service.log();

    assert!(log.contains("WARNING"), "log:\n{log}");
    assert!(log.contains("plain HTTP"), "log:\n{log}");
    assert!(
        log.contains("--dev / OXIDE_DEV_MODE ignored"),
        "the refusal must be visible; log:\n{log}"
    );
    // A request that would pass every dev-bypass check is still anonymous, so it is denied.
    assert_eq!(
        http_status(master.dashboard, "/api/status", None),
        401,
        "log:\n{log}"
    );
}

#[cfg(unix)]
#[test]
fn test_a_non_loopback_dashboard_without_dev_or_secret_is_not_open_either() {
    let home = home();
    let master = start_master(home.path(), "127.0.0.1:0", &["--web-ui-addr", "0.0.0.0:0"]);
    assert_eq!(
        http_status(master.dashboard, "/api/status", None),
        401,
        "implicit local access applies to loopback listeners only; log:\n{}",
        master.service.log()
    );
    assert!(master.service.log().contains("plain HTTP"));
}

#[cfg(unix)]
#[test]
fn test_the_default_dashboard_address_is_loopback_even_when_the_grid_listens_everywhere() {
    // No --dashboard-port and no --web-ui-addr: the dashboard takes its built-in address
    // (127.0.0.1:8080), not the interface the grid listens on. Port 8080 may be taken on a
    // developer machine, so both outcomes of that bind attempt are accepted; what is checked is
    // the address the master tried to publish the dashboard on.
    let home = home();
    let port_file = home.path().join("grid.port");
    let mut cmd = cli(home.path());
    cmd.env("RUSTY_GRID_ENABLE_WEB_UI", "1")
        .args(["master", "--listen", "0.0.0.0:0", "--port-file"])
        .arg(&port_file);
    let service = Service::spawn(cmd, home.path().join("master-default-dashboard.log"));
    assert!(
        wait_for(Duration::from_secs(20), || port_file.is_file()),
        "master did not start; log:\n{}",
        service.log()
    );
    assert!(
        wait_for(Duration::from_secs(10), || {
            let log = service.log();
            log.contains("Dashboard listening") || log.contains("Web UI port binding failed")
        }),
        "the master never tried to start the dashboard; log:\n{}",
        service.log()
    );
    let log = service.log();
    assert!(log.contains("127.0.0.1:8080"), "log:\n{log}");
    assert!(
        !log.contains("0.0.0.0:8080"),
        "the dashboard must not follow the grid listener onto every interface; log:\n{log}"
    );
}

#[cfg(unix)]
#[test]
fn test_web_ui_addr_and_dashboard_port_flags_cannot_be_combined() {
    // Both name the dashboard's port; silently preferring one would hide the other.
    let home = home();
    let mut cmd = cli(home.path());
    cmd.args([
        "master",
        "--listen",
        "127.0.0.1:0",
        "--web-ui-addr",
        "127.0.0.1:0",
        "--dashboard-port",
        "0",
    ]);
    // A master that wrongly accepts the combination would run forever: do not wait for it.
    let mut service = Service::spawn(cmd, home.path().join("master-conflict.log"));
    let exited = wait_for(Duration::from_secs(20), || {
        service.child.try_wait().ok().flatten().is_some()
    });
    let log = service.log();
    assert!(
        exited,
        "the master started although both flags were given; log:\n{log}"
    );
    assert!(!service.child.wait().unwrap().success(), "log:\n{log}");
    assert!(
        log.contains("--web-ui-addr") && log.contains("--dashboard-port"),
        "the error must name both flags: {log}"
    );
}

#[cfg(unix)]
#[test]
fn test_web_ui_addr_outranks_a_dashboard_port_from_the_environment() {
    let home = home();
    // The environment asks for a loopback dashboard port, the flag for every interface: the flag
    // is the more specific word and must not be overridden by the port alone.
    let mut cmd = cli(home.path());
    cmd.env("RUSTY_GRID_DASHBOARD_PORT", "0");
    let port_file = home.path().join("grid.port");
    cmd.args([
        "master",
        "--listen",
        "127.0.0.1:0",
        "--web-ui-addr",
        "0.0.0.0:0",
        "--port-file",
    ])
    .arg(&port_file);
    let service = Service::spawn(cmd, home.path().join("master-env-port.log"));
    assert!(
        wait_for(Duration::from_secs(20), || {
            service.log().contains("Embedded Dashboard available")
        }),
        "log:\n{}",
        service.log()
    );
    let log = service.log();
    assert!(
        log.contains("Embedded Dashboard available at http://0.0.0.0:"),
        "--web-ui-addr must be honoured; log:\n{log}"
    );
    assert!(
        log.contains("ignoring dashboard_port"),
        "the override must be announced; log:\n{log}"
    );
}

#[cfg(unix)]
#[test]
fn test_mapreduce_exits_non_zero_when_the_job_fails() {
    let home = home();
    let master = start_master(home.path(), "127.0.0.1:0", &["--dashboard-port", "0"]);
    let grid = master.grid.to_string();
    let sandbox = home.path().join("sandbox");
    let mut worker_cmd = cli(home.path());
    worker_cmd
        .args([
            "worker",
            "--master",
            &grid,
            "--name",
            "mr-worker",
            "--cores",
            "2",
        ])
        .arg("--sandbox-base-dir")
        .arg(&sandbox);
    let _worker = Service::spawn(worker_cmd, home.path().join("worker.log"));
    assert!(
        wait_for(Duration::from_secs(20), || {
            let mut cmd = cli(home.path());
            cmd.args(["workers", "--master", &grid, "--json"]);
            cmd.output()
                .ok()
                .and_then(|o| serde_json::from_slice::<serde_json::Value>(&o.stdout).ok())
                .and_then(|v| v.as_array().map(Vec::len))
                == Some(1)
        }),
        "worker never registered"
    );

    let mapreduce = |extra: &[&str]| -> Output {
        let mut cmd = cli(home.path());
        cmd.args(["mapreduce", "--master", &grid, "--input", "a b a"])
            .args(extra);
        cmd.output().expect("run mapreduce")
    };

    // A job that works: exit 0 and the counts.
    let ok = mapreduce(&["--chunks", "1"]);
    assert!(ok.status.success(), "stderr: {}", text(&ok.stderr));
    assert!(text(&ok.stdout).contains("a: 2"), "{}", text(&ok.stdout));

    // A mapper that cannot run (its script does not exist on the worker): the master answers with a
    // Failed job, and the command must say so and exit non-zero instead of printing "Completed".
    let failed = mapreduce(&["--mapper", "./no-such-mapper.sh"]);
    assert!(
        !failed.status.success(),
        "a failed job must not exit 0; stdout: {} stderr: {}",
        text(&failed.stdout),
        text(&failed.stderr)
    );
    let stderr = text(&failed.stderr);
    assert!(stderr.contains("FAILED"), "{stderr}");
    assert!(
        stderr.contains("Map partition"),
        "the reason must be shown: {stderr}"
    );
    assert!(
        !text(&failed.stdout).contains("Job Completed"),
        "{}",
        text(&failed.stdout)
    );

    // --chunks 0 used to panic with a division by zero; it is a usage error now.
    let zero = mapreduce(&["--chunks", "0"]);
    assert!(!zero.status.success());
    assert!(
        text(&zero.stderr).contains("--chunks must be at least 1"),
        "{}",
        text(&zero.stderr)
    );
}
