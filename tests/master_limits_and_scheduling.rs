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
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{
    MasterHandle, MasterServer, ServerConfig, DEFAULT_MASTER_MAX_BATCH_SIZE,
};
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
        .with_no_battery(true)
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

async fn wait_for_worker_count_within(
    master: &MasterHandle,
    count: usize,
    within: Duration,
) -> usize {
    let start = Instant::now();
    let mut connected = 0;
    while start.elapsed() < within {
        connected = master
            .list_workers()
            .await
            .map(|w| {
                w.iter()
                    .filter(|w| w.status != rusty_grid_master::registry::WorkerStatus::Disconnected)
                    .count()
            })
            .unwrap_or(0);
        if connected >= count {
            return connected;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    connected
}

async fn wait_for_worker_count(master: &MasterHandle, count: usize) -> bool {
    wait_for_worker_count_within(master, count, Duration::from_secs(10)).await >= count
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
    let sched_cfg = SchedulerConfig::default()
        .with_max_batch_size(DEFAULT_MASTER_MAX_BATCH_SIZE)
        .without_host_cpu_backpressure();
    MasterServer::spawn_with_config(config, sched_cfg, ReaperConfig::default())
        .await
        .expect("spawn master")
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
            println!("closed connection slot released in {:?}", start.elapsed());
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(60),
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

fn barrier_job(
    barrier_dir: &std::path::Path,
    partitions: usize,
    mapper_timeout_secs: u64,
) -> MapReduceJobSpec {
    // Each mapper announces itself into `barrier_dir` and waits until all `partitions` mappers
    // are active at once, so serial execution deadlocks on the barrier and fails the test rather
    // than relying on a wall-clock elapsed window (TEST-63).
    let barrier = barrier_dir.to_string_lossy().replace('\\', "/");
    let map_script = format!(
        r#"barrier='{barrier}'
printf '%s\n' "$RUSTY_GRID_WORKER_ID" > "$barrier/active-$RUSTY_GRID_TASK_ID"
while :; do
  set -- "$barrier"/active-*
  [ "$#" -ge {partitions} ] && break
  sleep 0.05
done
while IFS= read -r line || [ -n "$line" ]; do for w in $line; do printf '%s\t1\n' "$w"; done; done"#
    );
    MapReduceJobSpec::new(
        "parallelism",
        (0..partitions).map(|i| format!("word{i} shared")).collect(),
        MapFunctionSpec::ShellScript { script: map_script },
        ReduceFunctionSpec::ShellScript {
            script: r#"python3 -c "import sys, json; d=json.load(sys.stdin); print(sum(int(x) for x in d['values']))""#.into(),
        },
        partitions,
        1,
        mapper_timeout_secs,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mapreduce_tasks_run_on_all_workers_at_once() {
    const WAIT: Duration = Duration::from_secs(180);
    if std::process::Command::new("python3")
        .arg("--version")
        .output()
        .is_err()
    {
        eprintln!("python3 not available; skipping");
        return;
    }
    let master = spawn_master(grid_config()).await;
    let mut w1 = spawn_worker(&master, "w1", 2);
    let mut w2 = spawn_worker(&master, "w2", 2);
    let connected = wait_for_worker_count_within(&master, 2, WAIT).await;
    assert!(
        connected >= 2,
        "workers did not register within {WAIT:?} (connected={connected})"
    );

    let barrier_dir = tempfile::tempdir().expect("barrier tempdir");
    let mapper_timeout_secs = (WAIT * 3).as_secs();
    let started = Instant::now();
    let result = match tokio::time::timeout(
        WAIT,
        master.execute_mapreduce(barrier_job(barrier_dir.path(), 4, mapper_timeout_secs)),
    )
    .await
    {
        Ok(outcome) => outcome.unwrap(),
        Err(_) => {
            let stats = master.queue_stats().await;
            let active_count = std::fs::read_dir(barrier_dir.path())
                .map(|entries| {
                    entries
                        .filter_map(Result::ok)
                        .filter(|e| e.file_name().to_string_lossy().starts_with("active-"))
                        .count()
                })
                .unwrap_or(0);
            panic!(
                "concurrent mapreduce barrier job did not finish within {WAIT:?} (active_mappers={active_count}/4, queue_stats={stats:?})"
            );
        }
    };
    let elapsed = started.elapsed();
    println!("concurrent mapreduce job finished in {elapsed:?}");

    assert_eq!(result.status, "Completed", "{:?}", result.error);
    assert_eq!(result.map_tasks_completed, 4);
    assert_eq!(result.reduce_tasks_total, 5);
    assert_eq!(result.output["shared"].as_i64(), Some(4));

    let mut total_files = 0usize;
    let mut worker_counts: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    for entry in std::fs::read_dir(barrier_dir.path()).expect("read barrier dir") {
        let entry = entry.expect("barrier entry");
        let name = entry.file_name();
        if !name.to_string_lossy().starts_with("active-") {
            continue;
        }
        total_files += 1;
        let worker_id = std::fs::read_to_string(entry.path())
            .expect("read active file")
            .trim()
            .to_string();
        assert!(
            !worker_id.is_empty(),
            "empty worker id in {}",
            entry.path().display()
        );
        *worker_counts.entry(worker_id).or_insert(0) += 1;
    }
    assert_eq!(total_files, 4, "expected 4 barrier marker files");
    assert_eq!(
        worker_counts.len(),
        2,
        "expected map tasks across both workers, got {worker_counts:?}"
    );
    assert!(
        worker_counts.values().all(|&count| count == 2),
        "expected 2 concurrent map tasks on each 2-slot worker, got {worker_counts:?}"
    );

    let _ = w1.shutdown_tx.send(true);
    let _ = w2.shutdown_tx.send(true);
    let _ = tokio::time::timeout(WAIT, &mut w1.handle).await;
    let _ = tokio::time::timeout(WAIT, &mut w2.handle).await;
    let _ = master.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mapreduce_parallelism_is_configurable_and_a_failed_job_reports_failure() {
    // Every wait below ends as soon as its condition holds; `WAIT` only turns a hang into a
    // failure, and `mapper_timeout_secs` sits above it so a slow run under CPU starvation fails
    // on `WAIT` with queue diagnostics rather than inside the engine's 60 s mapper timeout
    // (TEST-54).
    const WAIT: Duration = Duration::from_secs(180);
    let mapper_timeout_secs = (WAIT * 3).as_secs();
    let test_started = Instant::now();

    let master = spawn_master(grid_config().with_mapreduce_parallelism(1)).await;
    let mut w1 = spawn_worker(&master, "w1", 4);
    let connected = wait_for_worker_count_within(&master, 1, WAIT).await;
    assert!(
        connected >= 1,
        "worker did not register within {WAIT:?} (connected={connected})"
    );
    println!("worker registered after {:?}", test_started.elapsed());

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
        mapper_timeout_secs,
    );
    let started = Instant::now();
    let result = match tokio::time::timeout(WAIT, master.execute_mapreduce(job)).await {
        Ok(outcome) => outcome.unwrap(),
        Err(_) => {
            let stats = master.queue_stats().await;
            panic!("serial mapreduce job did not finish within {WAIT:?}; queue_stats={stats:?}");
        }
    };
    let serial_elapsed = started.elapsed();
    println!("serial job finished in {serial_elapsed:?}");
    assert_eq!(result.status, "Completed");
    assert!(
        serial_elapsed >= Duration::from_millis(2800),
        "parallelism = 1 must run the partitions one at a time, took {serial_elapsed:?}"
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
        mapper_timeout_secs,
    );
    let failing_started = Instant::now();
    let result = match tokio::time::timeout(WAIT, master.execute_mapreduce(failing)).await {
        Ok(outcome) => outcome.unwrap(),
        Err(_) => {
            let stats = master.queue_stats().await;
            panic!("failing mapreduce job did not finish within {WAIT:?}; queue_stats={stats:?}");
        }
    };
    println!("failing job finished in {:?}", failing_started.elapsed());
    assert_eq!(result.status, "Failed");
    let error = result.error.expect("a failed job carries its reason");
    assert!(
        error.contains("exit code 3") && error.contains("mapper-broke"),
        "{error}"
    );

    let _ = w1.shutdown_tx.send(true);
    let _ = tokio::time::timeout(WAIT, &mut w1.handle).await;
    let _ = master.shutdown();
}
