//! T2 integration tests for `WorkerSupervisor` (`TRAY-1`, `design 0112`).

use oxideswarm_tray::state::SupervisorPhase;
use oxideswarm_tray::supervisor::{SupervisorConfig, WorkerSupervisor};
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const HELPER_MODE_ENV: &str = "OXIDESWARM_TRAY_FAKE_WORKER_MODE";
const COUNTER_FILE_ENV: &str = "OXIDESWARM_TRAY_FAKE_WORKER_COUNTER";
const PID_FILE_ENV: &str = "OXIDESWARM_TRAY_FAKE_WORKER_PID";

/// Helper test invoked as a fake worker child process when `OXIDESWARM_TRAY_FAKE_WORKER_MODE` is set.
#[test]
fn fake_worker_helper() {
    let Ok(mode) = std::env::var(HELPER_MODE_ENV) else {
        return;
    };
    if mode == "crash_then_run" {
        let counter_path =
            PathBuf::from(std::env::var(COUNTER_FILE_ENV).expect("counter file env var required"));
        let prev: u32 = fs::read_to_string(&counter_path)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0);
        let current = prev + 1;
        fs::write(&counter_path, current.to_string()).expect("write counter");
        if current < 3 {
            std::process::exit(1);
        }
    }
    if let Ok(pid_path) = std::env::var(PID_FILE_ENV) {
        fs::write(pid_path, std::process::id().to_string()).expect("write pid file");
    }
    loop {
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(windows)]
fn pid_is_alive(pid: u32) -> bool {
    let out = std::process::Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/NH", "/FO", "CSV"])
        .output();
    match out {
        Ok(o) => {
            let stdout = String::from_utf8_lossy(&o.stdout);
            stdout.contains(&format!("\"{pid}\""))
        }
        Err(_) => false,
    }
}

#[cfg(unix)]
fn pid_is_alive(pid: u32) -> bool {
    std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

struct TempDirGuard(PathBuf);

impl TempDirGuard {
    fn new(prefix: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("{prefix}{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).expect("create tempdir");
        Self(dir)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn supervisor_restarts_after_crash_with_backoff_and_stops_on_quit() {
    let tmp = TempDirGuard::new("oxideswarm_tray_t2_");
    let counter_path = tmp.path().join("attempts.txt");
    let pid_path = tmp.path().join("stable_pid.txt");
    let log_path = tmp.path().join("worker.log");

    let exe = std::env::current_exe().expect("current test binary");
    let mut cfg = SupervisorConfig::new(
        exe,
        vec![
            "--exact".to_string(),
            "fake_worker_helper".to_string(),
            "--nocapture".to_string(),
        ],
    );
    cfg.prepend_worker_subcommand = false;
    cfg.log_file = Some(log_path.clone());
    cfg.initial_backoff = Duration::from_millis(40);
    cfg.max_backoff = Duration::from_millis(160);
    cfg.stable_reset_after = Duration::from_secs(5);
    cfg.extra_env = vec![
        (HELPER_MODE_ENV.to_string(), "crash_then_run".to_string()),
        (
            COUNTER_FILE_ENV.to_string(),
            counter_path.to_string_lossy().into_owned(),
        ),
        (
            PID_FILE_ENV.to_string(),
            pid_path.to_string_lossy().into_owned(),
        ),
    ];

    let mut sup = WorkerSupervisor::new(cfg);
    let mut saw_backoff_attempts = Vec::new();
    let mut stable_pid: Option<u32> = None;

    for _ in 0..200 {
        let phase = sup.step(Instant::now());
        if let SupervisorPhase::Backoff { attempt, .. } = &phase {
            if !saw_backoff_attempts.contains(attempt) {
                saw_backoff_attempts.push(*attempt);
            }
        }
        if let SupervisorPhase::Running { pid, .. } = phase {
            if let Ok(recorded) = fs::read_to_string(&pid_path) {
                if recorded.trim() == pid.to_string() {
                    stable_pid = Some(pid);
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let pid = stable_pid.expect("supervisor should stabilize on 3rd attempt after two crashes");
    assert_eq!(
        fs::read_to_string(&counter_path)
            .expect("read counter")
            .trim(),
        "3"
    );
    assert!(
        saw_backoff_attempts.contains(&1) && saw_backoff_attempts.contains(&2),
        "expected backoff attempts 1 and 2, saw {saw_backoff_attempts:?}"
    );
    assert!(sup.has_child());
    assert!(log_path.exists());

    // Simulate Quit: stop_and_wait terminates the child and keeps supervisor stopped.
    sup.stop_and_wait().await;
    assert!(!sup.has_child());
    assert_eq!(sup.step(Instant::now()), SupervisorPhase::StoppedByUser);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !pid_is_alive(pid),
        "stopped child PID {pid} should not remain alive"
    );

    // Re-enabling starts a fresh child cleanly.
    sup.start_if_stopped();
    let phase = sup.step(Instant::now());
    assert!(matches!(phase, SupervisorPhase::Running { .. }));
    sup.stop_and_wait().await;
}
