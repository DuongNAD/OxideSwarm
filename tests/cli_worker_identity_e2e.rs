//! End-to-end tests of the worker id the real `rusty-grid worker` process uses.
//!
//! A restarted worker is the same machine, so a *named* worker keeps its id (`worker_id.<name>`
//! under `~/.oxideswarm`). Workers that have no name must not: they would all share one id file,
//! the master takes the same id for one machine reconnecting and closes the live session of the
//! other, and two terminals running `rusty-grid worker` would kick each other forever.
//!
//! Every spawned process gets a private `HOME` and working directory (never the developer's own
//! `~/.oxideswarm`). The worker only has to print its banner, so the master address is a closed
//! port.
#![cfg(unix)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;
use uuid::Uuid;

const BIN: &str = env!("CARGO_BIN_EXE_rusty-grid");

fn cli(home: &Path) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.env("HOME", home)
        .env("USERPROFILE", home)
        .current_dir(home)
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("OXIDESWARM_SECRET_FILE")
        .env_remove("RUSTY_GRID_CONFIG")
        .env_remove("RUSTY_GRID_MASTER")
        .env_remove("RUSTY_GRID_MASTER_ADDR")
        .env_remove("RUSTY_GRID_WORKER_NAME")
        .env_remove("RUSTY_GRID_NAME")
        .env_remove("RUSTY_GRID_WORKER_ID_FILE")
        .env_remove("RUSTY_GRID_INSECURE")
        .env("RUSTY_GRID_DISABLE_DISCOVERY", "1")
        .env("RUSTY_GRID_DISABLE_WEB_UI", "1")
        .env("NO_COLOR", "1")
        .stdin(Stdio::null());
    cmd
}

/// A running worker whose output goes to a log file. Killed on drop.
struct Worker {
    child: Child,
    log: PathBuf,
}

impl Worker {
    fn log(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// The id in the worker's banner (`RustyGrid Worker <uuid> initializing ...`).
    fn id(&self) -> Uuid {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(30) {
            let log = self.log();
            if let Some(line) = log.lines().find(|l| l.starts_with("RustyGrid Worker ")) {
                let word = line.split_whitespace().nth(2).unwrap_or_default();
                return Uuid::parse_str(word).unwrap_or_else(|_| panic!("no id in {line:?}"));
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("the worker printed no banner; log:\n{}", self.log());
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Starts `rusty-grid worker` against a closed port; `tag` keeps the log files apart.
fn start_worker(home: &Path, tag: &str, name: Option<&str>, env: &[(&str, &Path)]) -> Worker {
    let log = home.join(format!("worker-{tag}.log"));
    let out = fs::File::create(&log).expect("create log");
    let err = out.try_clone().expect("clone log handle");
    let mut cmd = cli(home);
    cmd.args(["worker", "--master", "127.0.0.1:9", "--cores", "1"])
        .arg("--sandbox-base-dir")
        .arg(home.join(format!("sandbox-{tag}")));
    if let Some(name) = name {
        cmd.args(["--name", name]);
    }
    for (key, value) in env {
        cmd.env(key, value);
    }
    cmd.stdout(Stdio::from(out)).stderr(Stdio::from(err));
    let child = cmd.spawn().expect("spawn rusty-grid worker");
    Worker { child, log }
}

fn id_file(home: &Path, name: Option<&str>) -> PathBuf {
    let file = match name {
        Some(name) => format!("worker_id.{name}"),
        None => "worker_id".to_string(),
    };
    home.join(".oxideswarm").join(file)
}

#[test]
fn test_a_named_worker_keeps_its_id_when_it_is_restarted() {
    let home = TempDir::new().unwrap();
    let first = start_worker(home.path(), "first", Some("gpu-pc"), &[]).id();
    // (the first process was killed when its handle was dropped)
    let second = start_worker(home.path(), "second", Some("gpu-pc"), &[]).id();
    assert_eq!(
        first, second,
        "a restart of the same named worker is the same machine"
    );
    assert!(id_file(home.path(), Some("gpu-pc")).is_file());

    let other = start_worker(home.path(), "other", Some("mac"), &[]).id();
    assert_ne!(other, first, "another name is another machine");
}

#[test]
fn test_workers_without_a_name_never_share_an_id() {
    let home = TempDir::new().unwrap();
    // Two terminals, one HOME: both alive at once, as in a hand-started local cluster.
    let a = start_worker(home.path(), "a", None, &[]);
    let b = start_worker(home.path(), "b", None, &[]);
    assert_ne!(
        a.id(),
        b.id(),
        "the master would treat equal ids as one machine reconnecting and kick one of them"
    );
    assert!(
        !id_file(home.path(), None).exists(),
        "nothing says which machine an unnamed worker is, so no shared id file is written"
    );
    drop((a, b));
    let again = start_worker(home.path(), "c", None, &[]);
    let _ = again.id();
}

#[test]
fn test_an_explicit_id_file_keeps_the_id_of_a_worker_without_a_name() {
    let home = TempDir::new().unwrap();
    let file = home.path().join("my-worker-id");
    let env = [("RUSTY_GRID_WORKER_ID_FILE", file.as_path())];
    let first = start_worker(home.path(), "first", None, &env).id();
    let second = start_worker(home.path(), "second", None, &env).id();
    assert_eq!(first, second);
    assert_eq!(fs::read_to_string(&file).unwrap().trim(), first.to_string());
}
