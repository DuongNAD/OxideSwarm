//! Task placement and `oxideswarm doctor` against a real in-process cluster (design record
//! `docs/design/0001-two-node-readiness.md`): constrained tasks reach only matching workers through
//! the master, worker listings carry the `id:` / `state:` tags clients need, and the doctor passes
//! on a healthy cluster and fails where it should.
//!
//! Everything binds `127.0.0.1:0`; workers run without the standby portal and with private sandbox
//! directories, so nothing touches fixed ports or the developer's `~/.oxideswarm`.

use std::time::{Duration, Instant};

use tempfile::TempDir;
use tokio::sync::watch;
use uuid::Uuid;

use rusty_grid_cli::doctor::{run_doctor, DoctorOptions, Outcome};
use rusty_grid_core::placement::{listed_worker_id, tag_value, Placement, STATE_TAG_PREFIX};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse};
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::registry::WorkerStatus;
use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

struct Cluster {
    master: MasterHandle,
    addr: String,
    workers: Vec<(watch::Sender<bool>, tokio::task::JoinHandle<()>)>,
    _dirs: Vec<TempDir>,
}

impl Cluster {
    async fn new() -> Self {
        let master = MasterServer::spawn_with_config(
            ServerConfig::new("127.0.0.1:0".parse().unwrap()),
            SchedulerConfig::default(),
            ReaperConfig::default(),
        )
        .await
        .expect("master starts");
        let addr = master.server_addr().to_string();
        Self {
            master,
            addr,
            workers: Vec::new(),
            _dirs: Vec::new(),
        }
    }

    /// Starts a worker and returns its id.
    fn spawn_worker(&mut self, name: &str, tags: &[&str], simulate_gpu: bool) -> Uuid {
        let dir = tempfile::tempdir().expect("sandbox dir");
        let config = WorkerConfig::new(self.addr.clone())
            .with_name(name)
            .with_cores(2)
            .with_ram_mb(2048)
            .with_simulate_gpu(simulate_gpu)
            .with_tags(tags.iter().map(|t| t.to_string()).collect())
            .with_sandbox_base_dir(dir.path())
            .with_redirection_portal(false);
        self._dirs.push(dir);
        let mut client = WorkerClient::new(config);
        let id = client.worker_id();
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let _ = client.run(rx).await;
        });
        self.workers.push((tx, handle));
        id
    }

    async fn wait_for_workers(&self, count: usize) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if let Ok(workers) = self.master.list_workers().await {
                if workers
                    .iter()
                    .filter(|w| w.status != WorkerStatus::Disconnected)
                    .count()
                    >= count
                {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("{count} workers did not register");
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for (tx, handle) in &self.workers {
            let _ = tx.send(true);
            handle.abort();
        }
    }
}

fn pinned(spec: TaskSpec, constraints: &[String]) -> Task {
    Task::new(spec, TaskRequirements::generic(1, 30)).with_tags(constraints.to_vec())
}

#[tokio::test]
async fn test_constrained_tasks_reach_only_matching_workers() {
    let mut cluster = Cluster::new().await;
    let a = cluster.spawn_worker("node-a", &["laptop"], false);
    let b = cluster.spawn_worker("node-b", &[], false);
    cluster.wait_for_workers(2).await;

    for _ in 0..3 {
        for (constraint, expected) in [
            (format!("require:worker={a}"), a),
            (format!("require:worker={b}"), b),
            ("require:worker=NODE-B".to_string(), b),
            ("require:tag=laptop".to_string(), a),
        ] {
            let task = pinned(
                TaskSpec::builtin_test("hash_compute", 100),
                std::slice::from_ref(&constraint),
            );
            let id = cluster.master.submit_task(task).await.expect("accepted");
            let result = cluster
                .master
                .wait_task(id, Some(Duration::from_secs(20)))
                .await
                .expect("completes");
            assert_eq!(result.worker_id, expected, "{constraint}");
        }
    }

    // Both workers run the test's own OS: a constraint naming it lands anywhere, another OS waits.
    let this_os = Placement {
        os: Some(std::env::consts::OS.to_string()),
        ..Placement::default()
    };
    let task = Task::new(
        TaskSpec::builtin_test("hash_compute", 100),
        TaskRequirements::generic(1, 30),
    )
    .with_placement(&this_os);
    let id = cluster.master.submit_task(task).await.unwrap();
    let result = cluster
        .master
        .wait_task(id, Some(Duration::from_secs(20)))
        .await
        .unwrap();
    assert!(result.worker_id == a || result.worker_id == b);

    let elsewhere = if std::env::consts::OS == "plan9" {
        "linux"
    } else {
        "plan9"
    };
    let waiting = pinned(
        TaskSpec::builtin_test("hash_compute", 100),
        &[format!("require:os={elsewhere}")],
    );
    let id = cluster.master.submit_task(waiting).await.unwrap();
    assert!(
        cluster
            .master
            .wait_task(id, Some(Duration::from_millis(800)))
            .await
            .is_err(),
        "no worker runs {elsewhere}: the task must wait"
    );
}

#[tokio::test]
async fn test_invalid_constraints_are_refused_at_submission() {
    let cluster = Cluster::new().await;
    for bad in [
        "require:gpu_vendor=nvidia",
        "require:os",
        "require:tag=os:macos",
    ] {
        let task = pinned(
            TaskSpec::builtin_test("hash_compute", 1),
            &[bad.to_string()],
        );
        let err = cluster
            .master
            .submit_task(task)
            .await
            .expect_err("an invalid constraint must be refused");
        assert!(
            err.to_string().contains("placement constraint"),
            "{bad}: {err}"
        );
    }
}

#[tokio::test]
async fn test_worker_listing_carries_system_id_and_state_tags() {
    let mut cluster = Cluster::new().await;
    let a = cluster.spawn_worker("node-a", &["laptop"], false);
    cluster.wait_for_workers(1).await;

    let mut transport = rusty_grid_cli::connect_master(&cluster.addr, None)
        .await
        .expect("connects");
    transport
        .send_msg(&ClientMessage::ListWorkers)
        .await
        .unwrap();
    let workers = match transport.recv_msg::<ClientResponse>().await.unwrap() {
        Some(ClientResponse::WorkerList { workers }) => workers,
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(workers.len(), 1);
    let w = &workers[0];
    assert_eq!(listed_worker_id(w), Some(a));
    assert_eq!(tag_value(&w.tags, STATE_TAG_PREFIX), Some("connected"));
    assert_eq!(tag_value(&w.tags, "os:"), Some(std::env::consts::OS));
    assert_eq!(tag_value(&w.tags, "arch:"), Some(std::env::consts::ARCH));
    assert!(w.tags.iter().any(|t| t == "laptop"));
    assert!(w.tags.iter().any(|t| t.starts_with("link:")));
}

#[tokio::test]
async fn test_doctor_passes_on_a_healthy_two_worker_cluster() {
    let mut cluster = Cluster::new().await;
    cluster.spawn_worker("node-a", &["laptop"], false);
    cluster.spawn_worker("node-b", &[], true);
    cluster.wait_for_workers(2).await;

    let options = DoctorOptions {
        expect_workers: 2,
        probe_timeout: Duration::from_secs(30),
        gpu_dim: 64,
        spread_tasks: Some(8),
        ..DoctorOptions::default()
    };
    let report = run_doctor(&cluster.addr, None, &options).await;
    let rendered = report.render();
    assert!(!report.failed(), "{rendered}");
    assert_eq!(report.workers.len(), 2, "{rendered}");

    let passed = |id: &str| {
        report
            .checks
            .iter()
            .filter(|c| c.id == id && c.outcome == Outcome::Pass)
            .count()
    };
    assert_eq!(passed("C1"), 1, "{rendered}");
    assert_eq!(passed("C2"), 1, "{rendered}");
    assert_eq!(
        passed("C3"),
        2,
        "one in-process probe per worker: {rendered}"
    );
    assert_eq!(passed("C4"), 2, "one process probe per worker: {rendered}");
    assert_eq!(
        passed("C5"),
        1,
        "only node-b has a (simulated) GPU: {rendered}"
    );
    let spread = report.checks.iter().find(|c| c.id == "C6").expect("C6");
    assert_ne!(spread.outcome, Outcome::Fail, "{rendered}");
    assert!(
        rendered.contains("node-a") && rendered.contains("node-b"),
        "{rendered}"
    );
}

#[tokio::test]
async fn test_doctor_fails_when_workers_are_missing_or_the_master_is_unreachable() {
    let mut cluster = Cluster::new().await;
    cluster.spawn_worker("node-a", &[], false);
    cluster.wait_for_workers(1).await;

    let options = DoctorOptions {
        expect_workers: 2,
        probe_timeout: Duration::from_secs(30),
        skip_spread: true,
        ..DoctorOptions::default()
    };
    let report = run_doctor(&cluster.addr, None, &options).await;
    let rendered = report.render();
    assert!(report.failed(), "{rendered}");
    let c2 = report.checks.iter().find(|c| c.id == "C2").expect("C2");
    assert_eq!(c2.outcome, Outcome::Fail, "{rendered}");
    assert!(
        c2.detail.contains("1 connected, at least 2 expected"),
        "{rendered}"
    );
    assert!(
        report
            .checks
            .iter()
            .any(|c| c.id == "C3" && c.outcome == Outcome::Pass),
        "the worker that is there is still probed: {rendered}"
    );

    // Port 1 never listens.
    let report = run_doctor("127.0.0.1:1", None, &DoctorOptions::default()).await;
    assert!(report.failed());
    assert!(!report.auth_failed);
    assert_eq!(report.checks[0].id, "C1");
    assert_eq!(report.checks[0].outcome, Outcome::Fail);
}
