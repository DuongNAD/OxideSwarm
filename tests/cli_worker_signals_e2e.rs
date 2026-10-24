//! End-to-end tests of what the real `rusty-grid worker` process does with the tasks it runs when
//! it is told to stop (SIGTERM, the signal `kill`, systemd, launchd and Termux send), and of the
//! `--task-priority` flag.
//!
//! Every spawned process gets a private `HOME` and working directory, as in `cli_secret_e2e`.
#![cfg(unix)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

const BIN: &str = env!("CARGO_BIN_EXE_rusty-grid");

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
        .env_remove("RUSTY_GRID_TASK_PRIORITY")
        .env_remove("RUSTY_GRID_INSECURE")
        .env("RUSTY_GRID_DISABLE_DISCOVERY", "1")
        .env("RUSTY_GRID_DISABLE_WEB_UI", "1")
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

/// A long-running child whose output goes to a log file. Killed on drop.
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
    _service: Service,
    addr: String,
}

fn start_master(home: &Path) -> Master {
    let port_file = home.join("master.port");
    let mut cmd = cli(home);
    cmd.args(["master", "--listen", "127.0.0.1:0", "--port-file"])
        .arg(&port_file);
    let service = Service::spawn(cmd, home.join("master.log"));
    let mut port = None;
    assert!(
        wait_for(Duration::from_secs(20), || {
            port = fs::read_to_string(&port_file)
                .ok()
                .and_then(|p| p.trim().parse::<u16>().ok());
            port.is_some()
        }),
        "master did not publish its port; log:\n{}",
        service.log()
    );
    Master {
        _service: service,
        addr: format!("127.0.0.1:{}", port.unwrap()),
    }
}

fn start_worker(home: &Path, master: &str, name: &str, args: &[&str]) -> Service {
    let mut cmd = cli(home);
    cmd.args(["worker", "--master", master, "--name", name, "--cores", "1"])
        .arg("--sandbox-base-dir")
        .arg(home.join(format!("sandbox-{name}")))
        .args(args);
    Service::spawn(cmd, home.join(format!("worker-{name}.log")))
}

fn worker_count(home: &Path, master: &str) -> Option<usize> {
    let out = cli(home)
        .args(["workers", "--master", master, "--json"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).ok()?;
    Some(json.as_array()?.len())
}

fn wait_for_worker(home: &Path, master: &Master, worker: &Service) {
    assert!(
        wait_for(Duration::from_secs(30), || worker_count(home, &master.addr)
            == Some(1)),
        "worker never registered.\nworker log:\n{}",
        worker.log()
    );
}

fn alive(pid: u32) -> bool {
    // A zombie still answers `kill -0`; ask ps for its state instead.
    let out = Command::new("ps")
        .args(["-o", "stat=", "-p", &pid.to_string()])
        .output()
        .unwrap();
    let state = text(&out.stdout);
    let state = state.trim();
    !state.is_empty() && !state.starts_with('Z')
}

fn recorded_pid(file: &Path) -> u32 {
    let mut pid = None;
    assert!(
        wait_for(Duration::from_secs(20), || {
            pid = fs::read_to_string(file)
                .ok()
                .and_then(|t| t.trim().parse::<u32>().ok());
            pid.is_some()
        }),
        "{} was never written",
        file.display()
    );
    pid.unwrap()
}

/// Kills the listed processes when dropped, so a failing test does not leave `sleep 300` behind.
struct KillOnDrop(Vec<u32>);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        for pid in &self.0 {
            let _ = Command::new("kill")
                .args(["-9", &pid.to_string()])
                .stderr(Stdio::null())
                .status();
        }
    }
}

fn tempdir() -> TempDir {
    tempfile::tempdir().expect("tempdir")
}

#[test]
fn test_sigterm_stops_the_worker_gracefully_and_takes_its_task_trees_with_it() {
    let home = tempdir();
    let master = start_master(home.path());
    let mut worker = start_worker(home.path(), &master.addr, "sigterm-worker", &[]);
    wait_for_worker(home.path(), &master, &worker);

    // A task with a background child and a nested shell with a grandchild, running for minutes.
    let child_file = home.path().join("child.pid");
    let grandchild_file = home.path().join("grandchild.pid");
    let script = format!(
        "sleep 300 & echo $! > '{}'; sh -c 'sleep 300 & echo $! > \"$0\"; wait' '{}' & wait",
        child_file.display(),
        grandchild_file.display()
    );
    let out: Output = cli(home.path())
        .args([
            "submit",
            "--master",
            &master.addr,
            "--timeout-secs",
            "600",
            "--",
            "sh",
            "-c",
            &script,
        ])
        .output()
        .unwrap();
    assert!(out.status.success(), "submit failed: {}", text(&out.stderr));
    let child = recorded_pid(&child_file);
    let grandchild = recorded_pid(&grandchild_file);
    let _cleanup = KillOnDrop(vec![child, grandchild]);
    assert!(alive(child) && alive(grandchild));

    // What `kill`, systemd and launchd send.
    let status = Command::new("kill")
        .args(["-TERM", &worker.child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());

    let mut exit = None;
    assert!(
        wait_for(Duration::from_secs(20), || {
            exit = worker.child.try_wait().unwrap();
            exit.is_some()
        }),
        "the worker did not stop on SIGTERM; log:\n{}",
        worker.log()
    );
    // A clean exit through the shutdown path, not death by the default signal action.
    assert_eq!(
        exit.unwrap().code(),
        Some(0),
        "worker log:\n{}",
        worker.log()
    );
    assert!(
        worker.log().contains("finished cleanly"),
        "worker log:\n{}",
        worker.log()
    );

    for (what, pid) in [
        ("background child", child),
        ("nested grandchild", grandchild),
    ] {
        assert!(
            wait_for(Duration::from_secs(10), || !alive(pid)),
            "the {what} (pid {pid}) outlived the worker that was asked to stop"
        );
    }
}

#[test]
fn test_task_priority_flag_sets_the_nice_value_of_task_processes() {
    let home = tempdir();
    let master = start_master(home.path());
    let worker = start_worker(
        home.path(),
        &master.addr,
        "idle-worker",
        &["--task-priority", "idle"],
    );
    wait_for_worker(home.path(), &master, &worker);

    let out = cli(home.path())
        .args([
            "submit",
            "--master",
            &master.addr,
            "--wait",
            "--",
            "sh",
            "-c",
            "ps -o ni= -p $$",
        ])
        .output()
        .unwrap();
    assert!(out.status.success(), "submit failed: {}", text(&out.stderr));
    assert!(
        text(&out.stdout).lines().any(|line| line.trim() == "19"),
        "an idle-priority task should run at nice 19: {}",
        text(&out.stdout)
    );
}

#[test]
fn test_misspelt_task_priority_is_refused_instead_of_running_tasks_at_full_priority() {
    let home = tempdir();
    let out = cli(home.path())
        .args([
            "worker",
            "--master",
            "127.0.0.1:1",
            "--task-priority",
            "realtime",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let stderr = text(&out.stderr);
    assert!(
        stderr.contains("--task-priority") && stderr.contains("below-normal"),
        "{stderr}"
    );
}
