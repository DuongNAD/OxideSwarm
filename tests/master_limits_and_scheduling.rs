//! End-to-end checks of the master's resource limits and scheduling quality, with real workers:
//!
//! * the grid port keeps a budget of open connections and of connections still in their
//!   pre-authentication handshake, closes surplus peers, and gives authenticated peers' handshake
//!   slots back;
//! * a backlog of tasks nobody can run (GPU work with no GPU worker online) does not starve CPU work
//!   queued behind it;
//! * Map/Reduce tasks run on all workers at once, not one after another.

use std::time::{Duration, Instant};

use tempfile::TempDir;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::sync::watch;

use rusty_grid_core::mapreduce::{MapFunctionSpec, MapReduceJobSpec, ReduceFunctionSpec};
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

/// A worker running in this process; stopped on drop.
struct TestWorker {
    shutdown_tx: watch::Sender<bool>,
    handle: tokio::task::JoinHandle<()>,
    _sandbox: TempDir,
}

impl Drop for TestWorker {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        self.handle.abort();
    }
}

fn spawn_worker(master: &MasterHandle, name: &str, cores: usize) -> TestWorker {
    let sandbox = tempfile::tempdir().expect("sandbox");
    let config = WorkerConfig::new(master.server_addr().to_string())
        .with_name(name)
        .with_cores(cores)
        .with_sandbox_base_dir(sandbox.path());
    let mut worker = WorkerClient::new(config);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = tokio::spawn(async move {
        let _ = worker.run(shutdown_rx).await;
    });
    TestWorker {
        shutdown_tx,
        handle,
        _sandbox: sandbox,
    }
}

async fn wait_for_worker_count(master: &MasterHandle, count: usize) -> bool {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        let connected = master
            .list_workers()
            .await
            .map(|w| {
                w.iter()
                    .filter(|w| w.status != rusty_grid_master::registry::WorkerStatus::Disconnected)
                    .count()
            })
            .unwrap_or(0);
        if connected >= count {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// True if the server has closed `stream` (EOF or reset) within `within`.
async fn closed_by_server(stream: &mut TcpStream, within: Duration) -> bool {
    let mut buf = [0u8; 16];
    match tokio::time::timeout(within, stream.read(&mut buf)).await {
        Ok(Ok(0)) | Ok(Err(_)) => true,
        Ok(Ok(_)) => false,
        Err(_) => false,
    }
}

async fn spawn_master(config: ServerConfig) -> MasterHandle {
    MasterServer::spawn(config).await.expect("spawn master")
}

fn grid_config() -> ServerConfig {
    ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(false)
        .with_discovery(false)
}

// ---------------------------------------------------------------------------------------------
// Connection budgets
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_surplus_peers_are_closed_and_closing_one_frees_its_slot() {
    let master = spawn_master(grid_config().with_max_connections(2)).await;
    let addr = master.server_addr();

    let mut first = TcpStream::connect(addr).await.unwrap();
    let mut second = TcpStream::connect(addr).await.unwrap();
    // Both silent connections hold a slot (the master waits for their first frame).
    assert!(!closed_by_server(&mut first, Duration::from_millis(300)).await);
    assert!(!closed_by_server(&mut second, Duration::from_millis(100)).await);

    let mut third = TcpStream::connect(addr).await.unwrap();
    assert!(
        closed_by_server(&mut third, Duration::from_secs(2)).await,
        "a third peer exceeds max_connections = 2 and must be closed at once"
    );
    // The first two were not disturbed by the refusal.
    assert!(!closed_by_server(&mut first, Duration::from_millis(100)).await);

    // Closing one makes room.
    drop(first);
    let start = Instant::now();
    loop {
        let mut retry = TcpStream::connect(addr).await.unwrap();
        if !closed_by_server(&mut retry, Duration::from_millis(300)).await {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "a closed connection must give its slot back"
        );
    }

    let _ = master.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_pre_auth_budget_holds_stalled_peers_and_is_returned_by_authenticated_ones() {
    // Only one connection may sit in the handshake at a time.
    let master = spawn_master(grid_config().with_max_pending_handshakes(1)).await;
    let addr = master.server_addr();

    // A stalled peer (connects, sends nothing) occupies the single pre-auth slot...
    let mut stalled = TcpStream::connect(addr).await.unwrap();
    assert!(!closed_by_server(&mut stalled, Duration::from_millis(300)).await);
    // ...so the next one is refused immediately instead of also waiting out the handshake timeout.
    let mut refused = TcpStream::connect(addr).await.unwrap();
    assert!(closed_by_server(&mut refused, Duration::from_secs(2)).await);

    // When the stalled peer goes away, the slot comes back and real workers get through, one after
    // another: a worker that has registered no longer counts as "pending".
    drop(stalled);
    let _w1 = spawn_worker(&master, "first", 1);
    assert!(
        wait_for_worker_count(&master, 1).await,
        "first worker must register"
    );
    let _w2 = spawn_worker(&master, "second", 1);
    assert!(
        wait_for_worker_count(&master, 2).await,
        "a registered worker must not keep holding the pre-auth slot"
    );

    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// Scheduling
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_gpu_backlog_without_a_gpu_worker_does_not_starve_cpu_work_behind_it() {
    let master = spawn_master(grid_config()).await;
    let _worker = spawn_worker(&master, "cpu-only", 2);
    assert!(wait_for_worker_count(&master, 1).await);

    // Far more unplaceable tasks than the old 128-task scheduling window.
    for _ in 0..400 {
        let task = Task::new(TaskSpec::gpu_compute("gemm", 8), TaskRequirements::gpu(30));
        master.submit_task(task).await.unwrap();
    }
    let cpu_task = Task::new(
        TaskSpec::command("echo", vec!["behind the backlog".into()]),
        TaskRequirements::generic(1, 30),
    );
    let id = master.submit_task(cpu_task).await.unwrap();

    let result = master
        .wait_task(id, Some(Duration::from_secs(15)))
        .await
        .expect("the CPU task must run although 400 GPU tasks sit in front of it");
    assert_eq!(result.exit_code, 0);
    assert!(result.stdout_str().contains("behind the backlog"));
    assert_eq!(
        master.queue_stats().await.unwrap().queued,
        400,
        "the GPU backlog is untouched"
    );

    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// Map/Reduce
// ---------------------------------------------------------------------------------------------

fn sleepy_job(partitions: usize) -> MapReduceJobSpec {
    // Each mapper and each reducer takes about one second.
    MapReduceJobSpec::new(
        "parallelism",
        (0..partitions).map(|i| format!("word{i} shared")).collect(),
        MapFunctionSpec::ShellScript {
            script: r#"sleep 1; while IFS= read -r line || [ -n "$line" ]; do for w in $line; do printf '%s\t1\n' "$w"; done; done"#.into(),
        },
        ReduceFunctionSpec::ShellScript {
            script: r#"sleep 1; python3 -c "import sys, json; d=json.load(sys.stdin); print(sum(int(x) for x in d['values']))""#.into(),
        },
        partitions,
        1,
        60,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mapreduce_tasks_run_on_all_workers_at_once() {
    if std::process::Command::new("python3")
        .arg("--version")
        .output()
        .is_err()
    {
        eprintln!("python3 not available; skipping");
        return;
    }
    let master = spawn_master(grid_config()).await;
    let _w1 = spawn_worker(&master, "w1", 2);
    let _w2 = spawn_worker(&master, "w2", 2);
    assert!(wait_for_worker_count(&master, 2).await);

    // 4 map tasks of ~1 s and 5 reduce tasks of ~1 s (4 words + "shared") on 4 slots: about 2 s
    // of map plus reduce waves when concurrent, at least 9 s when run one after another.
    let started = Instant::now();
    let result = master.execute_mapreduce(sleepy_job(4)).await.unwrap();
    let elapsed = started.elapsed();

    assert_eq!(result.status, "Completed", "{:?}", result.error);
    assert_eq!(result.map_tasks_completed, 4);
    assert_eq!(result.reduce_tasks_total, 5);
    assert_eq!(result.output["shared"].as_i64(), Some(4));
    assert!(
        elapsed < Duration::from_secs(7),
        "9 one-second tasks on 4 slots took {elapsed:?}: tasks are not running concurrently"
    );

    let _ = master.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mapreduce_parallelism_is_configurable_and_a_failed_job_reports_failure() {
    let master = spawn_master(grid_config().with_mapreduce_parallelism(1)).await;
    let _w1 = spawn_worker(&master, "w1", 4);
    assert!(wait_for_worker_count(&master, 1).await);

    // Parallelism 1 forces strictly sequential dispatch: 3 one-second mappers need >= 3 s.
    let job = MapReduceJobSpec::new(
        "serial",
        vec!["a".into(), "b".into(), "c".into()],
        MapFunctionSpec::ShellScript {
            script: r#"sleep 1; while IFS= read -r line || [ -n "$line" ]; do printf '%s\t1\n' "$line"; done"#.into(),
        },
        ReduceFunctionSpec::Builtin {
            operator: "sum".into(),
        },
        3,
        1,
        60,
    );
    let started = Instant::now();
    let result = master.execute_mapreduce(job).await.unwrap();
    assert_eq!(result.status, "Completed");
    assert!(
        started.elapsed() >= Duration::from_millis(2800),
        "parallelism = 1 must run the partitions one at a time, took {:?}",
        started.elapsed()
    );

    // A mapper that exits non-zero fails the job and says why.
    let failing = MapReduceJobSpec::new(
        "failing",
        vec!["a".into(), "b".into()],
        MapFunctionSpec::ShellScript {
            script: "echo mapper-broke >&2; exit 3".into(),
        },
        ReduceFunctionSpec::Builtin {
            operator: "sum".into(),
        },
        2,
        1,
        60,
    );
    let result = master.execute_mapreduce(failing).await.unwrap();
    assert_eq!(result.status, "Failed");
    let error = result.error.expect("a failed job carries its reason");
    assert!(
        error.contains("exit code 3") && error.contains("mapper-broke"),
        "{error}"
    );

    let _ = master.shutdown();
}
