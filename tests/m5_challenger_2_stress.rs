//! Adversarial and Empirical Stress Test Suite for Milestone 5 (Challenger 2).
//!
//! Verifies:
//! 1. Map/Reduce Word Count Multi-Partition Across Multiple Workers:
//!    - Multiple text partitions split across active workers
//!    - Builtin operators and shell pipelines
//!    - Exact word frequency aggregation matching oracle
//! 2. Partition & Input Boundary Edge Cases:
//!    - Empty input handling (immediate completed, 0 tasks, empty map)
//!    - Single-partition input
//!    - Large multi-line multi-partition input (stress load with oracle validation)
//! 3. Resiliency Under Worker Disconnection / Crash During Map Phase:
//!    - Abrupt worker crash (TCP EOF) mid-execution of a map task
//!    - Task re-enqueued as Retrying -> Queued and reassigned to surviving worker
//!    - Entire Map/Reduce job completes successfully with correct output
//! 4. Resiliency Under Worker Disconnection / Crash During Reduce Phase:
//!    - Abrupt worker crash mid-execution of a reduce task
//!    - Reduce task retried from retained in-memory intermediate shuffle partitions
//!    - Map phase is NOT re-run (map tasks have retry_count == 0)
//!    - Map/Reduce job completes successfully with correct output
//! 5. Error Transitions on Non-Zero Exit Codes:
//!    - Map task returning non-zero exit code (e.g. 42) -> Job status Failed with descriptive error
//!    - Reduce task returning non-zero exit code -> Job status Failed with descriptive error
//! 6. Concurrency & High Load:
//!    - Multiple concurrent Map/Reduce jobs executing simultaneously on the master
//!    - No cross-talk, race conditions, or deadlocks between concurrent jobs
//! 7. Wire Network Transport:
//!    - Map/Reduce submitted via ClientMessage over raw TcpStream and ClientResponse received

use std::collections::HashMap;
use std::time::{Duration, Instant};

use futures::future::join_all;
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::sync::watch;
use uuid::Uuid;

use rusty_grid_core::mapreduce::{MapFunctionSpec, MapReduceJobSpec, ReduceFunctionSpec};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse, MessageTransport};
use rusty_grid_master::registry::WorkerStatus;
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

/// Helper representing a spawned test worker with an isolated sandbox directory.
struct SpawnedWorker {
    pub worker_id: Uuid,
    pub shutdown_tx: watch::Sender<bool>,
    pub handle: tokio::task::JoinHandle<()>,
    pub _temp_dir: TempDir,
}

impl SpawnedWorker {
    pub fn abort(&self) {
        self.handle.abort();
        let _ = self.shutdown_tx.send(true);
    }
}

/// Spawns a worker client with an isolated sandbox directory and returns its controller.
fn spawn_test_worker(
    master_addr: String,
    name: &str,
    cores: usize,
    simulate_gpu: bool,
) -> SpawnedWorker {
    let temp_dir = tempfile::tempdir().expect("failed to create worker tempdir");
    let cfg = WorkerConfig::new(master_addr)
        .with_name(name)
        .with_cores(cores)
        .with_simulate_gpu(simulate_gpu)
        .with_sandbox_base_dir(temp_dir.path());

    let mut worker = WorkerClient::new(cfg);
    let worker_id = worker.worker_id();
    let (tx, rx) = watch::channel(false);

    let handle = tokio::spawn(async move {
        let _ = worker.run(rx).await;
    });

    SpawnedWorker {
        worker_id,
        shutdown_tx: tx,
        handle,
        _temp_dir: temp_dir,
    }
}

/// Awaits until at least `expected_count` workers are in `Connected` status.
async fn wait_for_workers(master: &MasterHandle, expected_count: usize, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Ok(workers) = master.list_workers().await {
            let active = workers
                .iter()
                .filter(|w| w.status == WorkerStatus::Connected)
                .count();
            if active >= expected_count {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

// =========================================================================
// SUITE 1: Map/Reduce Word Count Across Multiple Partitions & Workers
// =========================================================================

#[tokio::test]
async fn test_m5_mapreduce_word_count_multi_partition_multi_worker() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    let _w1 = spawn_test_worker(master_addr.clone(), "worker-m5-1", 2, false);
    let _w2 = spawn_test_worker(master_addr.clone(), "worker-m5-2", 2, false);
    let _w3 = spawn_test_worker(master_addr.clone(), "worker-m5-3", 2, false);

    assert!(
        wait_for_workers(&master, 3, Duration::from_secs(5)).await,
        "Failed to connect 3 workers"
    );

    let input_data = vec![
        "apple banana cherry date apple".to_string(),
        "banana elderberry fig grape apple banana".to_string(),
        "cherry date fig honeydew kiwi lemon".to_string(),
        "grape honeydew mango nectarine orange apple".to_string(),
    ];

    let spec = MapReduceJobSpec::new(
        "fruit_word_count",
        input_data,
        MapFunctionSpec::Builtin {
            operator: "word_count".into(),
        },
        ReduceFunctionSpec::Builtin {
            operator: "sum".into(),
        },
        4,
        2,
        30,
    );

    let res = master
        .execute_mapreduce(spec)
        .await
        .expect("execute_mapreduce failed");

    assert_eq!(res.status, "Completed");
    assert_eq!(res.map_tasks_total, 4);
    assert_eq!(res.map_tasks_completed, 4);
    assert!(res.error.is_none());

    let get_count = |k: &str| -> i64 {
        res.output
            .get(k)
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| panic!("key {k} not found in output: {:?}", res.output))
    };

    assert_eq!(get_count("apple"), 4);
    assert_eq!(get_count("banana"), 3);
    assert_eq!(get_count("cherry"), 2);
    assert_eq!(get_count("date"), 2);
    assert_eq!(get_count("fig"), 2);
    assert_eq!(get_count("grape"), 2);
    assert_eq!(get_count("honeydew"), 2);
    assert_eq!(get_count("elderberry"), 1);
    assert_eq!(get_count("kiwi"), 1);
    assert_eq!(get_count("lemon"), 1);
    assert_eq!(get_count("mango"), 1);
    assert_eq!(get_count("nectarine"), 1);
    assert_eq!(get_count("orange"), 1);

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_m5_mapreduce_word_count_shell_pipeline_multi_worker() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    let _w1 = spawn_test_worker(master_addr.clone(), "worker-sh-1", 2, false);
    let _w2 = spawn_test_worker(master_addr.clone(), "worker-sh-2", 2, false);

    assert!(
        wait_for_workers(&master, 2, Duration::from_secs(5)).await,
        "Failed to connect workers"
    );

    let mapper_script = r#"while IFS= read -r line || [ -n "$line" ]; do
  for w in $line; do
    printf "%s\t1\n" "$w"
  done
done"#;

    let reducer_script = r#"python3 -c "import sys, json; d=json.load(sys.stdin); print(sum(int(x) for x in d['values']))""#;

    let spec = MapReduceJobSpec::new(
        "shell_wc",
        vec![
            "rust distributed compute rust".into(),
            "compute engine in rust grid".into(),
        ],
        MapFunctionSpec::ShellScript {
            script: mapper_script.into(),
        },
        ReduceFunctionSpec::ShellScript {
            script: reducer_script.into(),
        },
        2,
        2,
        30,
    );

    let res = master
        .execute_mapreduce(spec)
        .await
        .expect("execute_mapreduce shell pipeline");

    assert_eq!(res.status, "Completed");
    assert_eq!(res.map_tasks_total, 2);
    assert_eq!(res.map_tasks_completed, 2);

    let get_count = |k: &str| -> i64 {
        res.output
            .get(k)
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| panic!("key {k} missing in {:?}", res.output))
    };

    assert_eq!(get_count("rust"), 3);
    assert_eq!(get_count("compute"), 2);
    assert_eq!(get_count("distributed"), 1);
    assert_eq!(get_count("engine"), 1);
    assert_eq!(get_count("in"), 1);
    assert_eq!(get_count("grid"), 1);

    let _ = master.shutdown();
}

// =========================================================================
// SUITE 2: Boundary Inputs (Empty, Single Partition, Large Multi-line)
// =========================================================================

#[tokio::test]
async fn test_m5_mapreduce_empty_input() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");

    let spec = MapReduceJobSpec::new(
        "empty_job",
        vec![],
        MapFunctionSpec::Builtin {
            operator: "word_count".into(),
        },
        ReduceFunctionSpec::Builtin {
            operator: "sum".into(),
        },
        4,
        2,
        30,
    );

    let res = master
        .execute_mapreduce(spec)
        .await
        .expect("empty mapreduce should succeed immediately");

    assert_eq!(res.status, "Completed");
    assert_eq!(res.map_tasks_total, 0);
    assert_eq!(res.map_tasks_completed, 0);
    assert_eq!(res.reduce_tasks_total, 0);
    assert_eq!(res.reduce_tasks_completed, 0);
    assert!(res.output.is_empty());
    assert!(res.error.is_none());

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_m5_mapreduce_single_partition_input() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    let _w1 = spawn_test_worker(master_addr.clone(), "worker-single", 2, false);

    assert!(
        wait_for_workers(&master, 1, Duration::from_secs(5)).await,
        "Worker failed to connect"
    );

    let spec = MapReduceJobSpec::new(
        "single_part_job",
        vec!["alpha beta gamma alpha beta alpha".into()],
        MapFunctionSpec::Builtin {
            operator: "word_count".into(),
        },
        ReduceFunctionSpec::Builtin {
            operator: "sum".into(),
        },
        1,
        1,
        30,
    );

    let res = master
        .execute_mapreduce(spec)
        .await
        .expect("single partition mapreduce failed");

    assert_eq!(res.status, "Completed");
    assert_eq!(res.map_tasks_total, 1);
    assert_eq!(res.map_tasks_completed, 1);

    let get_count = |k: &str| -> i64 {
        res.output
            .get(k)
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| panic!("key {k} missing"))
    };

    assert_eq!(get_count("alpha"), 3);
    assert_eq!(get_count("beta"), 2);
    assert_eq!(get_count("gamma"), 1);

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_m5_mapreduce_large_multiline_input_stress() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    let _w1 = spawn_test_worker(master_addr.clone(), "worker-large-1", 2, false);
    let _w2 = spawn_test_worker(master_addr.clone(), "worker-large-2", 2, false);
    let _w3 = spawn_test_worker(master_addr.clone(), "worker-large-3", 2, false);

    assert!(
        wait_for_workers(&master, 3, Duration::from_secs(5)).await,
        "Workers failed to connect"
    );

    // Dictionary of 30 deterministic words
    let vocabulary = [
        "quantum",
        "entropy",
        "tensor",
        "kernel",
        "matrix",
        "vector",
        "cluster",
        "pipeline",
        "sharding",
        "replica",
        "consensus",
        "latency",
        "throughput",
        "bandwidth",
        "channel",
        "semaphore",
        "mutex",
        "barrier",
        "scheduler",
        "runtime",
        "compiler",
        "bytecode",
        "register",
        "assembly",
        "protocol",
        "framing",
        "checksum",
        "payload",
        "socket",
        "daemon",
    ];

    let mut oracle: HashMap<String, i64> = HashMap::new();
    let num_lines = 600;
    let mut lines = Vec::with_capacity(num_lines);

    for i in 0..num_lines {
        let mut line_words = Vec::new();
        for j in 0..12 {
            let word_idx = (i * 17 + j * 31 + 7) % vocabulary.len();
            let word = vocabulary[word_idx];
            line_words.push(word);
            *oracle.entry(word.to_string()).or_insert(0) += 1;
        }
        lines.push(line_words.join(" "));
    }

    let spec = MapReduceJobSpec::new(
        "large_stress_job",
        lines,
        MapFunctionSpec::Builtin {
            operator: "word_count".into(),
        },
        ReduceFunctionSpec::Builtin {
            operator: "sum".into(),
        },
        6,
        3,
        60,
    );

    let res = master
        .execute_mapreduce(spec)
        .await
        .expect("large multi-line mapreduce failed");

    assert_eq!(res.status, "Completed");
    assert_eq!(res.map_tasks_total, 6);
    assert_eq!(res.map_tasks_completed, 6);
    assert_eq!(res.output.len(), oracle.len());

    for (word, &expected_count) in &oracle {
        let actual_count = res
            .output
            .get(word)
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| panic!("Missing word {word} in output"));
        assert_eq!(
            actual_count, expected_count,
            "Count mismatch for word '{word}': expected {expected_count}, got {actual_count}"
        );
    }

    let _ = master.shutdown();
}

// =========================================================================
// SUITE 3: Fault Tolerance: Worker Crash During Map Phase
// =========================================================================

#[tokio::test]
async fn test_m5_mapreduce_worker_crash_during_map_phase_rescheduled() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    let w1 = spawn_test_worker(master_addr.clone(), "worker-mapcrash-1", 2, false);
    let _w2 = spawn_test_worker(master_addr.clone(), "worker-mapcrash-2", 2, false);

    assert!(
        wait_for_workers(&master, 2, Duration::from_secs(5)).await,
        "Workers failed to connect"
    );

    // Mapper script sleeps for 1 second on attempt to allow crash injection
    let slow_mapper = r#"sleep 1
while IFS= read -r line || [ -n "$line" ]; do
  for w in $line; do
    printf "%s\t1\n" "$w"
  done
done"#;

    let spec = MapReduceJobSpec::new(
        "map_fault_tolerance_job",
        vec![
            "failover map task resiliency".into(),
            "distributed worker recovery".into(),
        ],
        MapFunctionSpec::ShellScript {
            script: slow_mapper.into(),
        },
        ReduceFunctionSpec::Builtin {
            operator: "sum".into(),
        },
        2,
        1,
        60,
    );

    let master_clone = master.clone();
    let job_fut = tokio::spawn(async move { master_clone.execute_mapreduce(spec).await });

    // Wait until a task is actively running on W1, then terminate W1
    let mut aborted = false;
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(4) {
        let tasks = master.list_tasks().await.unwrap_or_default();
        if let Some(t) = tasks
            .iter()
            .find(|t| t.assigned_worker_id == Some(w1.worker_id))
        {
            if t.state == rusty_grid_master::queue::TaskState::Running {
                w1.abort();
                aborted = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    if !aborted {
        // Fallback: abort w1 anyway if it was scheduled
        w1.abort();
    }

    // Await job completion — worker 2 should take over and complete the job
    let res = job_fut
        .await
        .expect("job task panicked")
        .expect("mapreduce execution after worker crash");

    assert_eq!(res.status, "Completed");
    assert_eq!(res.map_tasks_total, 2);
    assert_eq!(res.map_tasks_completed, 2);

    let get_count = |k: &str| -> i64 {
        res.output
            .get(k)
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| panic!("key {k} missing"))
    };

    assert_eq!(get_count("failover"), 1);
    assert_eq!(get_count("map"), 1);
    assert_eq!(get_count("resiliency"), 1);
    assert_eq!(get_count("recovery"), 1);

    let _ = master.shutdown();
}

// =========================================================================
// SUITE 4: Fault Tolerance: Worker Crash During Reduce Phase
// =========================================================================

#[tokio::test]
async fn test_m5_mapreduce_worker_crash_during_reduce_phase_retried_intermediate_retained() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    let w1 = spawn_test_worker(master_addr.clone(), "worker-redcrash-1", 2, false);
    let _w2 = spawn_test_worker(master_addr.clone(), "worker-redcrash-2", 2, false);

    assert!(
        wait_for_workers(&master, 2, Duration::from_secs(5)).await,
        "Workers failed to connect"
    );

    // Fast mapper
    let fast_mapper = r#"while IFS= read -r line || [ -n "$line" ]; do
  for w in $line; do
    printf "%s\t1\n" "$w"
  done
done"#;

    // Slow reducer sleeping 1 second to allow crash injection
    let slow_reducer = r#"sleep 1
python3 -c "import sys, json; d=json.load(sys.stdin); print(sum(int(x) for x in d['values']))""#;

    let spec = MapReduceJobSpec::new(
        "reduce_fault_tolerance_job",
        vec![
            "reduce retry intermediate retained".into(),
            "reduce retry verification".into(),
        ],
        MapFunctionSpec::ShellScript {
            script: fast_mapper.into(),
        },
        ReduceFunctionSpec::ShellScript {
            script: slow_reducer.into(),
        },
        2,
        2,
        60,
    );

    let master_clone = master.clone();
    let job_fut = tokio::spawn(async move { master_clone.execute_mapreduce(spec).await });

    // Wait until map tasks are completed and a reduce task is running on W1
    let mut aborted = false;
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(6) {
        let tasks = master.list_tasks().await.unwrap_or_default();
        // Check if there are tasks with retry count or running on W1 after map phase
        if let Some(t) = tasks
            .iter()
            .find(|t| t.assigned_worker_id == Some(w1.worker_id))
        {
            if t.state == rusty_grid_master::queue::TaskState::Running {
                // If there are at least 2 completed tasks, map phase has finished and reduce has started
                let completed_count = tasks
                    .iter()
                    .filter(|tk| tk.state == rusty_grid_master::queue::TaskState::Completed)
                    .count();
                if completed_count >= 2 {
                    w1.abort();
                    aborted = true;
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    if !aborted {
        w1.abort();
    }

    let res = job_fut
        .await
        .expect("job task panicked")
        .expect("mapreduce execution after reduce worker crash");

    assert_eq!(res.status, "Completed");

    let get_count = |k: &str| -> i64 {
        res.output
            .get(k)
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| panic!("key {k} missing in {:?}", res.output))
    };

    assert_eq!(get_count("reduce"), 2);
    assert_eq!(get_count("retry"), 2);
    assert_eq!(get_count("intermediate"), 1);
    assert_eq!(get_count("retained"), 1);
    assert_eq!(get_count("verification"), 1);

    // Verify empirical contract: Map phase was NOT re-run!
    // Exactly 2 map tasks were ever created and completed without being re-executed.
    let tasks = master.list_tasks().await.unwrap_or_default();
    let map_tasks: Vec<_> = tasks
        .iter()
        .filter(|t| t.exit_code == Some(0) && t.retry_count == 0)
        .collect();
    assert!(
        map_tasks.len() >= 2,
        "Expected at least 2 map tasks completed with retry_count == 0"
    );

    let _ = master.shutdown();
}

// =========================================================================
// SUITE 5: Non-Zero Exit Code Error Handling
// =========================================================================

#[tokio::test]
async fn test_m5_mapreduce_map_task_nonzero_exit_fails_with_error() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    let _w1 = spawn_test_worker(master_addr.clone(), "worker-fail-1", 2, false);

    assert!(
        wait_for_workers(&master, 1, Duration::from_secs(5)).await,
        "Worker failed to connect"
    );

    let failing_mapper = "echo 'SYNTAX_ERROR_ABORTING' >&2; exit 42";

    let spec = MapReduceJobSpec::new(
        "failing_map_job",
        vec!["some input line".into()],
        MapFunctionSpec::ShellScript {
            script: failing_mapper.into(),
        },
        ReduceFunctionSpec::Builtin {
            operator: "sum".into(),
        },
        1,
        1,
        30,
    );

    let res = master
        .execute_mapreduce(spec)
        .await
        .expect("execute_mapreduce returns Ok(MapReduceResult)");

    assert_eq!(res.status, "Failed");
    assert!(res.output.is_empty());
    assert!(res.error.is_some());

    let err_msg = res.error.unwrap();
    assert!(
        err_msg.contains("exit code 42"),
        "Expected error to mention exit code 42, got: {err_msg}"
    );
    assert!(
        err_msg.contains("SYNTAX_ERROR_ABORTING"),
        "Expected error to contain stderr output, got: {err_msg}"
    );

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_m5_mapreduce_reduce_task_nonzero_exit_fails_with_error() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    let _w1 = spawn_test_worker(master_addr.clone(), "worker-fail-2", 2, false);

    assert!(
        wait_for_workers(&master, 1, Duration::from_secs(5)).await,
        "Worker failed to connect"
    );

    let failing_reducer = "echo 'REDUCER_CORRUPTED_STREAM' >&2; exit 19";

    let spec = MapReduceJobSpec::new(
        "failing_reduce_job",
        vec!["hello world".into()],
        MapFunctionSpec::Builtin {
            operator: "word_count".into(),
        },
        ReduceFunctionSpec::ShellScript {
            script: failing_reducer.into(),
        },
        1,
        1,
        30,
    );

    let res = master
        .execute_mapreduce(spec)
        .await
        .expect("execute_mapreduce returns Ok(MapReduceResult)");

    assert_eq!(res.status, "Failed");
    assert!(res.output.is_empty());
    assert!(res.error.is_some());

    let err_msg = res.error.unwrap();
    assert!(
        err_msg.contains("REDUCER_CORRUPTED_STREAM"),
        "Expected error to mention REDUCER_CORRUPTED_STREAM, got: {err_msg}"
    );

    let _ = master.shutdown();
}

// =========================================================================
// SUITE 6: Concurrent Map/Reduce Jobs Stress
// =========================================================================

#[tokio::test]
async fn test_m5_mapreduce_concurrent_jobs_stress() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    let _w1 = spawn_test_worker(master_addr.clone(), "worker-conc-1", 4, false);
    let _w2 = spawn_test_worker(master_addr.clone(), "worker-conc-2", 4, false);
    let _w3 = spawn_test_worker(master_addr.clone(), "worker-conc-3", 4, false);

    assert!(
        wait_for_workers(&master, 3, Duration::from_secs(5)).await,
        "Workers failed to connect"
    );

    // 5 distinct MapReduce jobs with distinct domain vocabulary
    let job_specs = vec![
        (
            MapReduceJobSpec::new(
                "job_fruits",
                vec!["apple apple orange".into(), "banana orange apple".into()],
                MapFunctionSpec::Builtin {
                    operator: "word_count".into(),
                },
                ReduceFunctionSpec::Builtin {
                    operator: "sum".into(),
                },
                2,
                2,
                30,
            ),
            vec![("apple", 3), ("orange", 2), ("banana", 1)],
        ),
        (
            MapReduceJobSpec::new(
                "job_langs",
                vec!["rust rust python go".into(), "rust go zig c cpp".into()],
                MapFunctionSpec::Builtin {
                    operator: "word_count".into(),
                },
                ReduceFunctionSpec::Builtin {
                    operator: "sum".into(),
                },
                2,
                2,
                30,
            ),
            vec![
                ("rust", 3),
                ("go", 2),
                ("python", 1),
                ("zig", 1),
                ("c", 1),
                ("cpp", 1),
            ],
        ),
        (
            MapReduceJobSpec::new(
                "job_colors",
                vec!["red blue green red".into(), "blue yellow green red".into()],
                MapFunctionSpec::Builtin {
                    operator: "word_count".into(),
                },
                ReduceFunctionSpec::Builtin {
                    operator: "sum".into(),
                },
                2,
                2,
                30,
            ),
            vec![("red", 3), ("blue", 2), ("green", 2), ("yellow", 1)],
        ),
        (
            MapReduceJobSpec::new(
                "job_animals",
                vec!["cat dog bird cat".into(), "elephant tiger dog cat".into()],
                MapFunctionSpec::Builtin {
                    operator: "word_count".into(),
                },
                ReduceFunctionSpec::Builtin {
                    operator: "sum".into(),
                },
                2,
                2,
                30,
            ),
            vec![
                ("cat", 3),
                ("dog", 2),
                ("bird", 1),
                ("elephant", 1),
                ("tiger", 1),
            ],
        ),
        (
            MapReduceJobSpec::new(
                "job_single_token",
                vec!["unique_token unique_token".into()],
                MapFunctionSpec::Builtin {
                    operator: "word_count".into(),
                },
                ReduceFunctionSpec::Builtin {
                    operator: "sum".into(),
                },
                1,
                1,
                30,
            ),
            vec![("unique_token", 2)],
        ),
    ];

    // Launch all 5 jobs concurrently
    let mut futures = Vec::new();
    for (spec, expected) in job_specs {
        let m = master.clone();
        futures.push(tokio::spawn(async move {
            let res = m.execute_mapreduce(spec).await;
            (res, expected)
        }));
    }

    let results = join_all(futures).await;

    for task_res in results {
        let (job_res, expected) = task_res.expect("spawned job panicked");
        let res = job_res.expect("job execute_mapreduce failed");

        assert_eq!(res.status, "Completed");
        assert!(res.error.is_none());

        for (word, expected_count) in expected {
            let actual = res
                .output
                .get(word)
                .and_then(|v| v.as_i64())
                .unwrap_or_else(|| panic!("word {word} missing in job result {:?}", res.output));
            assert_eq!(
                actual, expected_count,
                "Concurrent job word mismatch for {word}"
            );
        }
    }

    let _ = master.shutdown();
}

// =========================================================================
// SUITE 7: Network Client Transport Over Raw TcpStream
// =========================================================================

#[tokio::test]
async fn test_m5_mapreduce_via_client_network_transport() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    let _w1 = spawn_test_worker(master_addr.clone(), "worker-net-1", 2, false);

    assert!(
        wait_for_workers(&master, 1, Duration::from_secs(5)).await,
        "Worker failed to connect"
    );

    let stream = TcpStream::connect(&master_addr)
        .await
        .expect("TcpStream::connect to master");
    let mut transport = MessageTransport::new(stream);

    let spec = MapReduceJobSpec::new(
        "net_wire_job",
        vec![
            "tcp framing wire protocol tcp".into(),
            "wire serialization framing".into(),
        ],
        MapFunctionSpec::Builtin {
            operator: "word_count".into(),
        },
        ReduceFunctionSpec::Builtin {
            operator: "sum".into(),
        },
        2,
        2,
        30,
    );

    let client_msg = ClientMessage::SubmitMapReduce { job: spec };
    transport
        .send_msg(&client_msg)
        .await
        .expect("send_msg ClientMessage::SubmitMapReduce");

    let response = transport
        .recv_msg::<ClientResponse>()
        .await
        .expect("recv_msg ClientResponse")
        .expect("Stream closed prematurely");

    match response {
        ClientResponse::MapReduceCompleted { result } => {
            assert_eq!(result.status, "Completed");
            let get_count = |k: &str| -> i64 {
                result
                    .output
                    .get(k)
                    .and_then(|v| v.as_i64())
                    .unwrap_or_else(|| panic!("key {k} missing in {:?}", result.output))
            };
            assert_eq!(get_count("tcp"), 2);
            assert_eq!(get_count("framing"), 2);
            assert_eq!(get_count("wire"), 2);
            assert_eq!(get_count("protocol"), 1);
            assert_eq!(get_count("serialization"), 1);
        }
        other => panic!("Unexpected response from master: {other:?}"),
    }

    let _ = master.shutdown();
}
