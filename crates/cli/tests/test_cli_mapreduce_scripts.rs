//! Integration tests for MR-1: MapReduce CLI script shipping, `--map-cmd` / `--reduce-cmd`,
//! float `max` / `min` built-in reducers, and clear error message when a worker lacks `sh`.

use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{watch, Notify, RwLock};
use uuid::Uuid;

use rusty_grid_core::mapreduce::{MapFunctionSpec, MapReduceJobSpec, ReduceFunctionSpec};
use rusty_grid_core::task::TaskResult;
use rusty_grid_master::mapreduce::MapReduceEngine;
use rusty_grid_master::queue::TaskQueue;
use rusty_grid_master::server::{MasterServer, ServerConfig, WaiterMap};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

const BIN: &str = env!("CARGO_BIN_EXE_oxideswarm");

fn cli_cmd(home: &Path) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.current_dir(home)
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env("OXIDESWARM_HOME", home.join(".oxideswarm"))
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("OXIDESWARM_SECRET_FILE")
        .env_remove("RUSTY_GRID_CONFIG")
        .env_remove("RUSTY_GRID_MASTER")
        .env_remove("RUSTY_GRID_MASTER_ADDR")
        .env_remove("RUSTY_GRID_INSECURE");
    cmd
}

async fn start_cluster() -> (
    rusty_grid_master::MasterHandle,
    String,
    watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
    tempfile::TempDir,
) {
    let temp = tempfile::tempdir().expect("tempdir");
    let sandbox_dir = temp.path().join("worker_sandboxes");
    std::fs::create_dir_all(&sandbox_dir).expect("create sandbox_dir");

    let server_cfg = ServerConfig::from_addr("127.0.0.1:0")
        .expect("server config")
        .with_discovery(false);
    let master = MasterServer::spawn(server_cfg).await.expect("spawn master");
    let master_addr = master.server_addr().to_string();

    let worker_cfg = WorkerConfig::new(&master_addr)
        .with_name("mr1-worker")
        .with_cores(4)
        .with_ram_mb(4096)
        .with_sandbox_base_dir(&sandbox_dir);
    let mut worker = WorkerClient::new(worker_cfg);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker_handle = tokio::spawn(async move {
        let _ = worker.run(shutdown_rx).await;
    });

    let deadline = Instant::now() + Duration::from_secs(5);
    while master.registry().list_workers().await.is_empty() {
        assert!(Instant::now() < deadline, "worker did not register in time");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    (master, master_addr, shutdown_tx, worker_handle, temp)
}

#[tokio::test]
async fn test_cli_mapreduce_ships_local_mapper_and_reducer_scripts() {
    let (_master, master_addr, shutdown_tx, worker_handle, _cluster_temp) = start_cluster().await;

    // Create a client directory completely separate from the worker's sandbox directory.
    let client_dir = tempfile::tempdir().expect("client tempdir");
    let input_path = client_dir.path().join("input.txt");
    std::fs::write(&input_path, "apple\nbanana\napple\ncherry\napple\nbanana\n")
        .expect("write input");

    let mapper_path = client_dir.path().join("local.sh");
    std::fs::write(
        &mapper_path,
        "while IFS= read -r line || [ -n \"$line\" ]; do\n  if [ -n \"$line\" ]; then\n    printf \"%s\\t1\\n\" \"$line\"\n  fi\ndone\n",
    )
    .expect("write local.sh");

    // Reducer reads {"key":..., "values":[1,1,...]} from stdin and counts elements in `values`
    let reducer_path = client_dir.path().join("sum.sh");
    std::fs::write(
        &reducer_path,
        "input=$(cat)\nvals=\"${input#*\\[}\"\nvals=\"${vals%\\]*}\"\ncount=$(printf \"%s\" \"$vals\" | tr -cd ',' | wc -c | tr -d ' ')\nprintf \"%d\\n\" $((count + 1))\n",
    )
    .expect("write sum.sh");

    let client_path = client_dir.path().to_path_buf();
    let output = tokio::task::spawn_blocking(move || {
        cli_cmd(&client_path)
            .args([
                "mapreduce",
                "--master",
                &master_addr,
                "--json",
                "--input",
                "input.txt",
                "--mapper",
                "./local.sh",
                "--reducer",
                "./sum.sh",
                "--chunks",
                "2",
            ])
            .output()
            .expect("spawn oxideswarm mapreduce")
    })
    .await
    .unwrap();

    assert!(
        output.status.success(),
        "mapreduce --mapper ./local.sh --reducer ./sum.sh failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let parsed: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse mapreduce json");
    assert_eq!(parsed["status"], "Completed");
    assert_eq!(parsed["output"]["apple"], 3);
    assert_eq!(parsed["output"]["banana"], 2);
    assert_eq!(parsed["output"]["cherry"], 1);

    let _ = shutdown_tx.send(true);
    worker_handle.abort();
}

#[tokio::test]
async fn test_cli_mapreduce_map_cmd_and_reduce_cmd_and_float_max_min() {
    let (_master, master_addr, shutdown_tx, worker_handle, _cluster_temp) = start_cluster().await;

    let client_dir = tempfile::tempdir().expect("client tempdir");
    let input_path = client_dir.path().join("floats.txt");
    std::fs::write(
        &input_path,
        "sensor\t1.5\nsensor\t9.75\nsensor\t-2.25\nsensor\t4.0\n",
    )
    .expect("write floats.txt");

    let client_path = client_dir.path().to_path_buf();

    // 1. Test --map-cmd with built-in max reducer on floating-point numbers
    let c_path = client_path.clone();
    let m_arg = master_addr.clone();
    let max_out = tokio::task::spawn_blocking(move || {
        cli_cmd(&c_path)
            .args([
                "mapreduce",
                "--master",
                &m_arg,
                "--json",
                "--input",
                "floats.txt",
                "--map-cmd",
                "sh -c 'cat'",
                "--reducer",
                "max",
                "--chunks",
                "2",
            ])
            .output()
            .expect("spawn oxideswarm mapreduce max")
    })
    .await
    .unwrap();

    assert!(
        max_out.status.success(),
        "mapreduce --map-cmd max failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&max_out.stdout),
        String::from_utf8_lossy(&max_out.stderr)
    );
    let max_json: serde_json::Value = serde_json::from_slice(&max_out.stdout).unwrap();
    assert_eq!(max_json["status"], "Completed");
    assert_eq!(max_json["output"]["sensor"].as_f64(), Some(9.75));

    // 2. Test --map-cmd with built-in min reducer on floating-point numbers
    let c_path = client_path.clone();
    let m_arg = master_addr.clone();
    let min_out = tokio::task::spawn_blocking(move || {
        cli_cmd(&c_path)
            .args([
                "mapreduce",
                "--master",
                &m_arg,
                "--json",
                "--input",
                "floats.txt",
                "--map-cmd",
                "sh -c 'cat'",
                "--reducer",
                "min",
                "--chunks",
                "2",
            ])
            .output()
            .expect("spawn oxideswarm mapreduce min")
    })
    .await
    .unwrap();

    assert!(min_out.status.success());
    let min_json: serde_json::Value = serde_json::from_slice(&min_out.stdout).unwrap();
    assert_eq!(min_json["status"], "Completed");
    assert_eq!(min_json["output"]["sensor"].as_f64(), Some(-2.25));

    // 3. Test --reduce-cmd
    let c_path = client_path.clone();
    let m_arg = master_addr.clone();
    let cmd_out = tokio::task::spawn_blocking(move || {
        cli_cmd(&c_path)
            .args([
                "mapreduce",
                "--master",
                &m_arg,
                "--json",
                "--input",
                "floats.txt",
                "--map-cmd",
                "sh -c 'cat'",
                "--reduce-cmd",
                "sh -c 'cat >/dev/null && printf \"42\\n\"'",
                "--chunks",
                "1",
            ])
            .output()
            .expect("spawn oxideswarm mapreduce --reduce-cmd")
    })
    .await
    .unwrap();

    assert!(cmd_out.status.success());
    let cmd_json: serde_json::Value = serde_json::from_slice(&cmd_out.stdout).unwrap();
    assert_eq!(cmd_json["status"], "Completed");
    assert_eq!(cmd_json["output"]["sensor"], 42);

    let _ = shutdown_tx.send(true);
    worker_handle.abort();
}

#[tokio::test]
async fn test_mapreduce_missing_sh_returns_clear_error() {
    let queue = TaskQueue::new();
    let notify = Arc::new(Notify::new());
    let waiters: WaiterMap = Arc::new(RwLock::new(HashMap::new()));
    let engine = MapReduceEngine::new(queue.clone(), notify.clone(), waiters.clone());

    let q_clone = queue.clone();
    let w_clone = Arc::clone(&waiters);
    let n_clone = Arc::clone(&notify);
    let worker_sim = tokio::spawn(async move {
        loop {
            n_clone.notified().await;
            let ready = q_clone.get_schedulable_tasks().await;
            if let Some(task) = ready.into_iter().next() {
                let worker_id = Uuid::new_v4();
                let _ = q_clone.mark_running(&task.id, worker_id).await;
                let res = TaskResult::failure(
                    worker_id,
                    task.id,
                    1,
                    "",
                    "Failed to spawn process 'sh': program not found",
                    5,
                    Some("Process spawn error: program not found".to_string()),
                );
                let _ = q_clone.record_result(res.clone()).await;
                if let Some(senders) = w_clone.write().await.remove(&task.id) {
                    for tx in senders {
                        let _ = tx.send(res.clone());
                    }
                }
                return;
            }
        }
    });

    let spec = MapReduceJobSpec::new(
        "missing_sh_job",
        vec!["hello world".to_string()],
        MapFunctionSpec::Builtin {
            operator: "word_count".to_string(),
        },
        ReduceFunctionSpec::Builtin {
            operator: "sum".to_string(),
        },
        1,
        1,
        10,
    );

    let result = engine.execute(spec).await.expect("execute returns result");
    worker_sim.abort();

    assert_eq!(result.status, "Failed");
    let err = result.error.as_deref().unwrap_or("");
    assert!(
        err.contains("worker does not have 'sh' on PATH")
            && err.contains("--map-cmd / --reduce-cmd"),
        "expected clear missing-sh guidance in error, got: {err}"
    );
}
