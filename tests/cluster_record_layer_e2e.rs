//! End-to-end tests of the AEAD record layer on the cluster port (cluster authentication v2),
//! driving the real `rusty-grid` binary (master, worker, submit, workers) as separate processes.
//!
//! Workers and clients reach the master through an in-test TCP proxy that understands the
//! length-delimited framing. It records every frame, and on the first connection it relays it can
//! play the on-path attacker on the master -> worker direction: inject a plaintext `AssignTask`,
//! or flip one bit of a record. Controls on an open (secret-less) cluster show that the recording
//! and the injection really work there, so the assertions are not vacuous. (Tampering in the
//! worker -> master direction is covered by `cluster_auth_e2e` and
//! `m3_challenger_1_wire_encryption_stress` against an in-process master.)
//!
//! Every spawned process gets a private `HOME` / `USERPROFILE` and working directory, so nothing
//! here reads or writes the developer's real `~/.oxideswarm`, and no `OXIDESWARM_*` /
//! `RUSTY_GRID_*` variable from the test environment leaks in.
#![cfg(unix)]

use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use tempfile::TempDir;

use rusty_grid_core::cluster_auth::WIRE_FORMAT_AUTH;
use rusty_grid_core::protocol::{
    serialize_message, MasterMessage, WireCodec, MAX_FRAME_SIZE, WIRE_FORMAT_ENCRYPTED,
};
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};

const BIN: &str = env!("CARGO_BIN_EXE_rusty-grid");

/// Makes port-file and log names unique within one test.
static SEQ: AtomicUsize = AtomicUsize::new(0);

/// A clean command environment rooted in `home`.
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
        .env("RUSTY_GRID_DISABLE_DISCOVERY", "1")
        .env("RUSTY_GRID_DISABLE_WEB_UI", "1")
        .env("NO_COLOR", "1")
        .stdin(Stdio::null());
    cmd
}

fn run(mut cmd: Command) -> Output {
    cmd.output().expect("run rusty-grid")
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
        thread::sleep(Duration::from_millis(50));
    }
    condition()
}

/// A long-running child (master / worker) whose output goes to a log file. Killed on drop.
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

/// One cluster under test: a temp home, an optional secret, and a master.
struct Cluster {
    home: TempDir,
    /// `--secret-file <path>` for every process, or nothing for an open cluster.
    secret_args: Vec<String>,
    master: Service,
    master_addr: String,
}

impl Cluster {
    /// Starts a master on an ephemeral loopback port, with a fresh secret unless `open`.
    fn start(open: bool) -> Self {
        let home = tempfile::tempdir().expect("tempdir");
        let mut secret_args = Vec::new();
        if !open {
            let secret = home.path().join("cluster.secret");
            let mut cmd = cli(home.path());
            cmd.args(["secret", "generate", "--out"]).arg(&secret);
            let out = run(cmd);
            assert!(
                out.status.success(),
                "secret generate: {}",
                text(&out.stderr)
            );
            secret_args = vec!["--secret-file".into(), secret.display().to_string()];
        }

        let id = SEQ.fetch_add(1, Ordering::SeqCst);
        let port_file = home.path().join(format!("master-{id}.port"));
        let mut cmd = cli(home.path());
        cmd.args([
            "master",
            "--listen",
            "127.0.0.1:0",
            "--heartbeat-interval",
            "1",
        ])
        .arg("--port-file")
        .arg(&port_file)
        .args(&secret_args);
        let master = Service::spawn(cmd, home.path().join(format!("master-{id}.log")));
        let mut port = None;
        assert!(
            wait_for(Duration::from_secs(20), || {
                port = fs::read_to_string(&port_file)
                    .ok()
                    .and_then(|p| p.trim().parse::<u16>().ok());
                port.is_some()
            }),
            "master did not publish its port; log:\n{}",
            master.log()
        );
        if !open {
            assert!(
                master.log().contains("Cluster authentication: enabled"),
                "master log:\n{}",
                master.log()
            );
        }
        Self {
            home,
            secret_args,
            master,
            master_addr: format!("127.0.0.1:{}", port.unwrap()),
        }
    }

    fn path(&self) -> &Path {
        self.home.path()
    }

    /// Starts a worker that dials `master` (the master itself or a proxy in front of it).
    fn start_worker(&self, master: &str, name: &str) -> Service {
        let sandbox = self.path().join(format!("sandbox-{name}"));
        let mut cmd = cli(self.path());
        cmd.args(["worker", "--master", master, "--name", name, "--cores", "1"])
            .arg("--sandbox-base-dir")
            .arg(&sandbox)
            .args(&self.secret_args);
        Service::spawn(cmd, self.path().join(format!("worker-{name}.log")))
    }

    /// Number of workers `rusty-grid workers` reports when asked through `via`.
    fn worker_count(&self, via: &str) -> Option<usize> {
        let mut cmd = cli(self.path());
        cmd.args(["workers", "--master", via, "--json"])
            .args(&self.secret_args);
        let out = run(cmd);
        if !out.status.success() {
            return None;
        }
        let json: serde_json::Value = serde_json::from_slice(&out.stdout).ok()?;
        Some(json.as_array()?.len())
    }

    fn wait_for_workers(&self, via: &str, count: usize, worker: &Service) {
        assert!(
            wait_for(Duration::from_secs(30), || self.worker_count(via)
                == Some(count)),
            "expected {count} worker(s).\nworker log:\n{}\nmaster log:\n{}",
            worker.log(),
            self.master.log()
        );
    }

    /// `rusty-grid submit --wait -- <command...>` through `via`.
    fn submit_wait(&self, via: &str, command: &[&str]) -> Output {
        let mut cmd = cli(self.path());
        cmd.args(["submit", "--master", via])
            .args(&self.secret_args)
            .arg("--wait")
            .arg("--")
            .args(command);
        run(cmd)
    }
}

// ---------------------------------------------------------------------------------------------
// The on-path proxy
// ---------------------------------------------------------------------------------------------

/// Which way a frame travelled through the proxy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Dir {
    /// Worker / client to master.
    Up,
    /// Master to worker / client.
    Down,
}

/// What the proxy does to the first connection it relays; later connections pass unchanged, so
/// a worker that dropped the attacked connection can reconnect.
#[derive(Clone)]
enum Attack {
    None,
    /// Send this frame to the worker right after the first master-to-worker message, as if the
    /// master had sent it.
    InjectDown(Vec<u8>),
    /// Flip one bit in the first master-to-worker message of at least this many bytes.
    FlipDown {
        min_len: usize,
    },
}

#[derive(Default)]
struct Traffic {
    /// Every relayed frame: connection index, direction, bytes (as forwarded).
    frames: Vec<(usize, Dir, Vec<u8>)>,
    /// Connections in the order their two directions ended: `Up` when the worker / client side
    /// closed, `Down` when the master side did.
    closed: Vec<(usize, Dir)>,
    /// Connections the attack was carried out on.
    attacked: Vec<usize>,
    /// Number of accepted connections.
    connections: usize,
}

struct Proxy {
    addr: String,
    traffic: Arc<Mutex<Traffic>>,
}

impl Proxy {
    fn start(upstream: &str, attack: Attack) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind proxy");
        let addr = listener.local_addr().unwrap().to_string();
        let traffic = Arc::new(Mutex::new(Traffic::default()));
        let upstream = upstream.to_string();
        let shared = traffic.clone();
        thread::spawn(move || {
            for (conn, peer) in listener.incoming().enumerate() {
                let Ok(peer) = peer else { return };
                let Ok(master) = TcpStream::connect(&upstream) else {
                    continue;
                };
                shared.lock().unwrap().connections += 1;
                let attack = if conn == 0 {
                    attack.clone()
                } else {
                    Attack::None
                };
                let (peer_in, master_out) =
                    (peer.try_clone().unwrap(), master.try_clone().unwrap());
                let (up_attack, up_traffic) = (attack.clone(), shared.clone());
                thread::spawn(move || {
                    pump(conn, Dir::Up, peer_in, master_out, up_attack, up_traffic)
                });
                let down_traffic = shared.clone();
                thread::spawn(move || pump(conn, Dir::Down, master, peer, attack, down_traffic));
            }
        });
        Self { addr, traffic }
    }

    fn snapshot<T>(&self, f: impl FnOnce(&Traffic) -> T) -> T {
        f(&self.traffic.lock().unwrap())
    }

    /// Which side closed connection `conn` first, if it is closed.
    fn first_to_close(&self, conn: usize) -> Option<Dir> {
        self.snapshot(|t| t.closed.iter().find(|(c, _)| *c == conn).map(|(_, d)| *d))
    }

    /// Whether a connection after the first one carried a master-to-peer message after its
    /// handshake, i.e. the master accepted the worker again on a new connection.
    fn reconnected(&self) -> bool {
        self.snapshot(|t| {
            t.frames.iter().any(|(conn, dir, frame)| {
                *conn >= 1 && *dir == Dir::Down && frame.first() != Some(&WIRE_FORMAT_AUTH)
            })
        })
    }

    /// Whether `needle` appears anywhere in the relayed bytes.
    fn saw_bytes(&self, needle: &[u8]) -> bool {
        self.snapshot(|t| {
            t.frames
                .iter()
                .any(|(_, _, f)| f.windows(needle.len()).any(|w| w == needle))
        })
    }
}

fn read_frame(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).ok()?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME_SIZE {
        return None;
    }
    let mut frame = vec![0u8; len];
    stream.read_exact(&mut frame).ok()?;
    Some(frame)
}

fn write_frame(stream: &mut TcpStream, frame: &[u8]) -> bool {
    stream
        .write_all(&(frame.len() as u32).to_be_bytes())
        .and_then(|()| stream.write_all(frame))
        .is_ok()
}

/// Relays frames from `from` to `to` until either side fails, applying `attack` once.
fn pump(
    conn: usize,
    dir: Dir,
    mut from: TcpStream,
    mut to: TcpStream,
    attack: Attack,
    traffic: Arc<Mutex<Traffic>>,
) {
    let mut done = false;
    while let Some(mut frame) = read_frame(&mut from) {
        let mut extra = None;
        if frame.first() != Some(&WIRE_FORMAT_AUTH) {
            let hit = !done
                && match (&attack, dir) {
                    (Attack::InjectDown(inject), Dir::Down) => {
                        extra = Some(inject.clone());
                        true
                    }
                    (Attack::FlipDown { min_len }, Dir::Down) => frame.len() >= *min_len,
                    _ => false,
                };
            if hit {
                if extra.is_none() {
                    let middle = frame.len() / 2;
                    frame[middle] ^= 0x01;
                }
                done = true;
                traffic.lock().unwrap().attacked.push(conn);
            }
        }
        traffic
            .lock()
            .unwrap()
            .frames
            .push((conn, dir, frame.clone()));
        if !write_frame(&mut to, &frame) {
            break;
        }
        if let Some(extra) = extra {
            if !write_frame(&mut to, &extra) {
                break;
            }
        }
    }
    traffic.lock().unwrap().closed.push((conn, dir));
    let _ = to.shutdown(Shutdown::Both);
    let _ = from.shutdown(Shutdown::Both);
}

/// A unique string that cannot appear on the wire by accident.
fn marker(label: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("oxs-{label}-{}-{nanos}", std::process::id())
}

// ---------------------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------------------

#[test]
fn test_master_worker_and_cli_run_a_task_with_every_frame_encrypted() {
    let cluster = Cluster::start(false);
    let proxy = Proxy::start(&cluster.master_addr, Attack::None);
    let worker = cluster.start_worker(&proxy.addr, "sealed-worker");
    cluster.wait_for_workers(&proxy.addr, 1, &worker);

    // The command, its argument and its output all cross the proxy: client -> master (submit),
    // master -> worker (assignment), worker -> master (result), master -> client (reply).
    let secret_text = marker("payload");
    let out = cluster.submit_wait(&proxy.addr, &["echo", &secret_text]);
    assert!(
        out.status.success(),
        "submit failed: {}\nworker log:\n{}",
        text(&out.stderr),
        worker.log()
    );
    assert!(
        text(&out.stdout).contains(&secret_text),
        "{}",
        text(&out.stdout)
    );

    assert!(
        !proxy.saw_bytes(secret_text.as_bytes()),
        "the task payload crossed the network in clear text"
    );
    proxy.snapshot(|t| {
        assert!(
            t.connections >= 3,
            "worker, workers and submit connections expected"
        );
        let mut records = [0usize; 2];
        for (conn, dir, frame) in &t.frames {
            match frame.first() {
                Some(&WIRE_FORMAT_AUTH) => {}
                Some(&WIRE_FORMAT_ENCRYPTED) => records[(*dir == Dir::Down) as usize] += 1,
                other => panic!(
                    "connection {conn} {dir:?}: frame with format {other:?} after the handshake"
                ),
            }
        }
        assert!(
            records[0] >= 3 && records[1] >= 3,
            "records seen: {records:?}"
        );
    });

    // Control: without a secret the same traffic is plain, so the check above means something.
    let open = Cluster::start(true);
    let open_proxy = Proxy::start(&open.master_addr, Attack::None);
    let open_worker = open.start_worker(&open.master_addr, "open-worker");
    open.wait_for_workers(&open.master_addr, 1, &open_worker);
    let plain_text = marker("plain");
    let out = open.submit_wait(&open_proxy.addr, &["echo", &plain_text]);
    assert!(out.status.success(), "open submit: {}", text(&out.stderr));
    assert!(open_proxy.saw_bytes(plain_text.as_bytes()));
}

#[test]
fn test_worker_refuses_an_injected_plaintext_assignment_and_drops_the_connection() {
    // The attack the record layer exists for: an on-path host injects a well-formed `AssignTask`
    // into an authenticated master -> worker connection.
    fn injected_assignment(marker_file: &Path) -> Vec<u8> {
        let task = Task::new(
            TaskSpec::new_shell_script(format!("touch '{}'", marker_file.display())),
            TaskRequirements::generic(1, 30),
        );
        serialize_message(&MasterMessage::AssignTask { task }, WireCodec::Bincode)
            .expect("encode AssignTask")
            .to_vec()
    }

    let cluster = Cluster::start(false);
    let pwned = cluster.path().join("injected-task-ran");
    let proxy = Proxy::start(
        &cluster.master_addr,
        Attack::InjectDown(injected_assignment(&pwned)),
    );
    let worker = cluster.start_worker(&proxy.addr, "target-worker");

    // The worker drops the attacked connection itself and comes back on a clean one.
    assert!(
        wait_for(Duration::from_secs(20), || proxy
            .first_to_close(0)
            .is_some()),
        "the attacked connection stayed open.\nworker log:\n{}",
        worker.log()
    );
    assert_eq!(proxy.snapshot(|t| t.attacked.clone()), vec![0]);
    assert_eq!(
        proxy.first_to_close(0),
        Some(Dir::Up),
        "the worker must be the one that drops the connection.\nworker log:\n{}",
        worker.log()
    );
    assert!(
        wait_for(Duration::from_secs(30), || proxy.reconnected()),
        "the worker did not register again.\nworker log:\n{}",
        worker.log()
    );
    assert!(
        worker.log().contains("Expected encrypted wire format"),
        "worker log:\n{}",
        worker.log()
    );
    thread::sleep(Duration::from_millis(500));
    assert!(!pwned.exists(), "the injected task was executed");

    // Control: on an open cluster the very same injection runs the command on the worker.
    let open = Cluster::start(true);
    let open_pwned = open.path().join("injected-task-ran");
    let open_proxy = Proxy::start(
        &open.master_addr,
        Attack::InjectDown(injected_assignment(&open_pwned)),
    );
    let open_worker = open.start_worker(&open_proxy.addr, "open-target");
    assert!(
        wait_for(Duration::from_secs(20), || open_pwned.exists()),
        "control: the injected task should have run on an open cluster.\nworker log:\n{}",
        open_worker.log()
    );
}

#[test]
fn test_worker_drops_a_tampered_assignment_and_the_task_runs_once_after_reconnecting() {
    let cluster = Cluster::start(false);
    // The assignment is the only master -> worker message this large (acks are tiny).
    let padding = "x".repeat(4000);
    let proxy = Proxy::start(&cluster.master_addr, Attack::FlipDown { min_len: 3000 });
    let worker = cluster.start_worker(&proxy.addr, "tampered-worker");
    cluster.wait_for_workers(&cluster.master_addr, 1, &worker);

    let output_marker = marker("result");
    let out = cluster.submit_wait(&cluster.master_addr, &["echo", &output_marker, &padding]);
    assert!(
        out.status.success(),
        "submit failed: {}\nworker log:\n{}\nmaster log:\n{}",
        text(&out.stderr),
        worker.log(),
        cluster.master.log()
    );
    assert_eq!(
        text(&out.stdout).matches(&output_marker).count(),
        1,
        "{}",
        text(&out.stdout)
    );

    assert_eq!(proxy.snapshot(|t| t.attacked.clone()), vec![0]);
    assert_eq!(
        proxy.first_to_close(0),
        Some(Dir::Up),
        "the worker must drop the connection that carried the tampered record"
    );
    assert!(
        proxy.snapshot(|t| t.connections) >= 2,
        "the worker reconnects"
    );
    assert!(
        worker
            .log()
            .contains("Ciphertext integrity verification failed"),
        "worker log:\n{}",
        worker.log()
    );
}
