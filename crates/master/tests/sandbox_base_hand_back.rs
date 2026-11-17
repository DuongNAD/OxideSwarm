//! BUG-25 / design 0090: a worker whose sandbox base became unusable after it registered hands its
//! tasks back, and the master runs them on another worker without charging an execution retry.

use std::time::Duration;

use tokio::sync::watch;
use uuid::Uuid;

use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_master::queue::TaskState;
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

struct TestWorker {
    id: Uuid,
    _shutdown: watch::Sender<bool>,
    handle: tokio::task::JoinHandle<()>,
}

fn spawn_worker(master_addr: &str, name: &str, base: &std::path::Path) -> TestWorker {
    let (shutdown, rx) = watch::channel(false);
    let config = WorkerConfig::new(master_addr.to_string())
        .with_no_battery(true)
        .with_name(name)
        .with_cores(4)
        .with_sandbox_base_dir(base);
    let mut client = WorkerClient::new(config);
    let id = client.worker_id();
    let handle = tokio::spawn(async move {
        let _ = client.run(rx).await;
    });
    TestWorker {
        id,
        _shutdown: shutdown,
        handle,
    }
}

fn echo_task(i: usize) -> Task {
    Task::new(
        TaskSpec::Command {
            program: "echo".into(),
            args: vec![format!("hand-back-{i}")],
            env: Default::default(),
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 30).with_max_retries(3),
    )
}

#[tokio::test]
async fn tasks_refused_for_a_broken_sandbox_base_complete_on_the_other_worker() {
    let dir = tempfile::tempdir().expect("tempdir");
    let good_base = dir.path().join("good");
    let bad_base = dir.path().join("bad");

    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("spawn master");
    let addr = master.server_addr().to_string();
    let good = spawn_worker(&addr, "good", &good_base);
    let bad = spawn_worker(&addr, "bad", &bad_base);

    let mut registered = false;
    for _ in 0..100 {
        if master.list_workers().await.map(|w| w.len()).unwrap_or(0) == 2 {
            registered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(registered, "both workers must register");

    // The base breaks while the worker runs: a plain file takes its place, so it cannot be
    // re-created (a recreated directory would just be used again).
    std::fs::remove_dir_all(&bad_base).expect("remove bad base");
    std::fs::write(&bad_base, b"not a directory").expect("block the base");

    let mut ids = Vec::new();
    for i in 0..8 {
        ids.push(master.submit_task(echo_task(i)).await.expect("submit"));
    }
    for id in &ids {
        let result = master
            .wait_task(*id, Some(Duration::from_secs(30)))
            .await
            .expect("task result");
        assert!(result.is_success(), "task must complete, got {result:?}");
        let info = master.get_task_info(*id).await.expect("info");
        assert_eq!(info.state, TaskState::Completed);
        assert_eq!(
            info.assigned_worker_id,
            Some(good.id),
            "the broken worker cannot have run it"
        );
    }
    assert_ne!(good.id, bad.id);
    good.handle.abort();
    bad.handle.abort();
}
