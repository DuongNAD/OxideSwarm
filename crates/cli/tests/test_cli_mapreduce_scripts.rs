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
    let m_addr = master_addr.clone();
    let output = tokio::task::spawn_blocking(move || {
        cli_cmd(&client_path)
            .args([
                "mapreduce",
                "--master",
                &m_addr,
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

    // F33-1: Even if files named `word_count` and `sum` exist in the working directory,
    // default flags (`--mapper word_count --reducer sum`) MUST run the built-in operators
    // and never read or execute those working-directory files. Only explicit `./word_count`
    // ships the local file.
    let marker_path = client_dir.path().join("hijack_marker.txt");
    let planted_word_count = client_dir.path().join("word_count");
    let planted_sum = client_dir.path().join("sum");
    std::fs::write(
        &planted_word_count,
        format!(
            "echo pwned > '{}'\nprintf \"hijacked\\t999\\n\"\n",
            marker_path.display()
        ),
    )
    .expect("write planted word_count");
    std::fs::write(
        &planted_sum,
        format!(
            "echo pwned_reduce > '{}'\nprintf \"999\\n\"\n",
            marker_path.display()
        ),
    )
    .expect("write planted sum");

    let client_path = client_dir.path().to_path_buf();
    let m_addr = master_addr.clone();
    let default_out = tokio::task::spawn_blocking(move || {
        cli_cmd(&client_path)
            .args([
                "mapreduce",
                "--master",
                &m_addr,
                "--json",
                "--input",
                "input.txt",
                "--chunks",
                "2",
            ])
            .output()
            .expect("spawn oxideswarm mapreduce default")
    })
    .await
    .unwrap();

    assert!(
        default_out.status.success(),
        "default mapreduce failed: stderr={}",
        String::from_utf8_lossy(&default_out.stderr)
    );
    assert!(
        !marker_path.exists(),
        "default mapreduce must NOT execute ./word_count or ./sum in cwd"
    );
    let default_json: serde_json::Value = serde_json::from_slice(&default_out.stdout).unwrap();
    assert_eq!(default_json["status"], "Completed");
    assert_eq!(default_json["output"]["apple"], 3);
    assert_eq!(default_json["output"]["hijacked"], serde_json::Value::Null);

    // Passing `--mapper ./word_count` explicitly DOES ship the file.
    let client_path = client_dir.path().to_path_buf();
    let m_addr = master_addr.clone();
    let explicit_out = tokio::task::spawn_blocking(move || {
        cli_cmd(&client_path)
            .args([
                "mapreduce",
                "--master",
                &m_addr,
                "--json",
                "--input",
                "input.txt",
                "--mapper",
                "./word_count",
                "--chunks",
                "1",
            ])
            .output()
            .expect("spawn oxideswarm mapreduce --mapper ./word_count")
    })
    .await
    .unwrap();
    assert!(explicit_out.status.success());
    let explicit_json: serde_json::Value = serde_json::from_slice(&explicit_out.stdout).unwrap();
    assert_eq!(explicit_json["output"]["hijacked"], 999);

    // F33-3: Oversized mapper script (> 64 KiB) is rejected on the client with a size error.
    let huge_mapper = client_dir.path().join("huge.sh");
    std::fs::write(&huge_mapper, "a".repeat(70 * 1024)).expect("write huge.sh");
    let client_path = client_dir.path().to_path_buf();
    let m_addr = master_addr.clone();
    let huge_out = tokio::task::spawn_blocking(move || {
        cli_cmd(&client_path)
            .args([
                "mapreduce",
                "--master",
                &m_addr,
                "--input",
                "input.txt",
                "--mapper",
                "./huge.sh",
            ])
            .output()
            .expect("spawn oxideswarm mapreduce --mapper ./huge.sh")
    })
    .await
    .unwrap();
    assert!(!huge_out.status.success());
    let huge_stderr = String::from_utf8_lossy(&huge_out.stderr);
    assert!(
        huge_stderr.contains("exceeds maximum inline MapReduce script size")
            && huge_stderr.contains("use --map-cmd"),
        "expected size error with --map-cmd for oversized mapper script, got: {huge_stderr}"
    );
    assert!(!huge_stderr.contains("--mapper-cmd"), "{huge_stderr}");

    // N33-1: Oversized reducer script (> 64 KiB) names `--reduce-cmd` (not `--reducer-cmd`).
    let client_path = client_dir.path().to_path_buf();
    let m_addr = master_addr.clone();
    let huge_reduce_out = tokio::task::spawn_blocking(move || {
        cli_cmd(&client_path)
            .args([
                "mapreduce",
                "--master",
                &m_addr,
                "--input",
                "input.txt",
                "--reducer",
                "./huge.sh",
            ])
            .output()
            .expect("spawn oxideswarm mapreduce --reducer ./huge.sh")
    })
    .await
    .unwrap();
    assert!(!huge_reduce_out.status.success());
    let huge_reduce_stderr = String::from_utf8_lossy(&huge_reduce_out.stderr);
    assert!(
        huge_reduce_stderr.contains("exceeds maximum inline MapReduce script size")
            && huge_reduce_stderr.contains("use --reduce-cmd"),
        "expected size error with --reduce-cmd for oversized reducer script, got: {huge_reduce_stderr}"
    );
    assert!(
        !huge_reduce_stderr.contains("--reducer-cmd"),
        "{huge_reduce_stderr}"
    );

    let _ = shutdown_tx.send(true);
    worker_handle.abort();
}

#[tokio::test]
async fn test_cli_mapreduce_map_cmd_and_reduce_cmd_and_float_max_min() {
    let (_master, master_addr, shutdown_tx, worker_handle, _cluster_temp) = start_cluster().await;

    let client_dir = tempfile::tempdir().expect("client tempdir");
    let input_path = client_dir.path().join("floats.txt");
    // Include JSON-encoded string float values ("12.5" and "-5.5") to exercise the string-float
    // branches in `reduce_builtin` (m12), non-finite "inf", "-inf", and "NaN" strings (F33-5),
    // and a key `all_skipped` whose values ("inf", "NaN", "1e999", "abc") are all skipped (N33-3)
    // to verify that key is omitted from `output` rather than reporting i64::MIN / i64::MAX / 0.
    std::fs::write(
        &input_path,
        "sensor\t1.5\nsensor\t9.75\n{\"key\":\"sensor\",\"value\":\"12.5\"}\n{\"key\":\"sensor\",\"value\":\"-5.5\"}\nsensor\t-2.25\nsensor\t4.0\nsensor\tinf\nsensor\t-inf\nsensor\tNaN\nall_skipped\tinf\nall_skipped\tNaN\nall_skipped\t1e999\nall_skipped\tabc\n",
    )
    .expect("write floats.txt");

    let client_path = client_dir.path().to_path_buf();

    // 1. Test --map-cmd with built-in max reducer on floating-point numbers & string floats
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
    assert_eq!(max_json["output"]["sensor"].as_f64(), Some(12.5));
    assert!(
        max_json["output"].get("all_skipped").is_none(),
        "key with only skipped values must be omitted from max output, got: {:?}",
        max_json["output"]
    );

    // 2. Test --map-cmd with built-in min reducer on floating-point numbers & string floats
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
    assert_eq!(min_json["output"]["sensor"].as_f64(), Some(-5.5));
    assert!(
        min_json["output"].get("all_skipped").is_none(),
        "key with only skipped values must be omitted from min output, got: {:?}",
        min_json["output"]
    );

    // 2b. Test --map-cmd with built-in sum reducer omits `all_skipped` too
    let c_path = client_path.clone();
    let m_arg = master_addr.clone();
    let sum_out = tokio::task::spawn_blocking(move || {
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
                "sum",
                "--chunks",
                "2",
            ])
            .output()
            .expect("spawn oxideswarm mapreduce sum")
    })
    .await
    .unwrap();
    assert!(sum_out.status.success());
    let sum_json: serde_json::Value = serde_json::from_slice(&sum_out.stdout).unwrap();
    assert_eq!(sum_json["status"], "Completed");
    assert_eq!(sum_json["output"]["sensor"].as_f64(), Some(20.0));
    assert!(
        sum_json["output"].get("all_skipped").is_none(),
        "key with only skipped values must be omitted from sum output, got: {:?}",
        sum_json["output"]
    );

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

    // 4. F33-4: Verify Windows backslashes in `--map-cmd` are preserved literally when unquoted.
    let c_path = client_path.clone();
    let m_arg = master_addr.clone();
    let win_out = tokio::task::spawn_blocking(move || {
        cli_cmd(&c_path)
            .args([
                "mapreduce",
                "--master",
                &m_arg,
                "--input",
                "floats.txt",
                "--map-cmd",
                r"C:\Tools\mapper.exe --flag",
                "--chunks",
                "1",
            ])
            .output()
            .expect("spawn oxideswarm mapreduce windows path")
    })
    .await
    .unwrap();
    assert!(!win_out.status.success());
    let win_stderr = String::from_utf8_lossy(&win_out.stderr);
    assert!(
        win_stderr.contains(r"C:\Tools\mapper.exe"),
        "expected unquoted Windows path backslashes to be preserved, got: {win_stderr}"
    );

    // 5. N33-2: Verify single-quoted Windows UNC path `'\\srv\share\m.exe'` preserves its leading double backslash.
    let c_path = client_path.clone();
    let m_arg = master_addr.clone();
    let unc_out = tokio::task::spawn_blocking(move || {
        cli_cmd(&c_path)
            .args([
                "mapreduce",
                "--master",
                &m_arg,
                "--input",
                "floats.txt",
                "--map-cmd",
                r"'\\srv\share\m.exe' --flag",
                "--chunks",
                "1",
            ])
            .output()
            .expect("spawn oxideswarm mapreduce unc path")
    })
    .await
    .unwrap();
    assert!(!unc_out.status.success());
    let unc_stderr = String::from_utf8_lossy(&unc_out.stderr);
    assert!(
        unc_stderr.contains(r"\\srv\share\m.exe"),
        "expected single-quoted UNC path leading double backslash to be preserved, got: {unc_stderr}"
    );

    let _ = shutdown_tx.send(true);
    worker_handle.abort();
}

async fn run_simulated_mapreduce(
    spec: MapReduceJobSpec,
    mut outcomes: Vec<Result<&'static str, (&'static str, &'static str)>>,
) -> rusty_grid_core::mapreduce::MapReduceResult {
    let queue = TaskQueue::new();
    let notify = Arc::new(Notify::new());
    let waiters: WaiterMap = Arc::new(RwLock::new(HashMap::new()));
    let engine = MapReduceEngine::new(queue.clone(), notify.clone(), waiters.clone());

    let q_clone = queue.clone();
    let w_clone = Arc::clone(&waiters);
    let n_clone = Arc::clone(&notify);
    let worker_sim = tokio::spawn(async move {
        let mut handled = std::collections::HashSet::new();
        while !outcomes.is_empty() {
            n_clone.notified().await;
            loop {
                let ready = q_clone.get_schedulable_tasks().await;
                let Some(task) = ready.into_iter().find(|t| !handled.contains(&t.id)) else {
                    break;
                };
                if outcomes.is_empty() {
                    return;
                }
                handled.insert(task.id);
                let outcome = outcomes.remove(0);
                let worker_id = Uuid::new_v4();
                let _ = q_clone.mark_running(&task.id, worker_id).await;
                let res = match outcome {
                    Ok(stdout) => TaskResult::success(worker_id, task.id, stdout, 5, false),
                    Err((stderr, err_msg)) => TaskResult::failure(
                        worker_id,
                        task.id,
                        1,
                        "",
                        stderr,
                        5,
                        Some(err_msg.to_string()),
                    ),
                };
                let _ = q_clone.record_result(res.clone()).await;
                if let Some(senders) = w_clone.write().await.remove(&task.id) {
                    for tx in senders {
                        let _ = tx.send(res.clone());
                    }
                }
            }
        }
    });

    let result = engine.execute(spec).await.expect("execute returns result");
    worker_sim.abort();
    result
}

#[tokio::test]
async fn test_mapreduce_missing_sh_returns_clear_error() {
    // 1. Builtin / ShellScript map stage failing with not-found gets the missing-sh hint.
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
    let result = run_simulated_mapreduce(
        spec,
        vec![Err((
            "Failed to spawn process 'sh': program not found",
            "Process spawn error: program not found",
        ))],
    )
    .await;

    assert_eq!(result.status, "Failed");
    let err = result.error.as_deref().unwrap_or("");
    assert!(
        err.contains("worker does not have 'sh' on PATH")
            && err.contains("--map-cmd / --reduce-cmd"),
        "expected clear missing-sh guidance in error, got: {err}"
    );

    // 2. (m9): ReduceFunctionSpec::ShellScript failing with not-found ALSO gets the missing-sh hint.
    let reduce_spec = MapReduceJobSpec::new(
        "missing_sh_reduce_job",
        vec!["k".to_string()],
        MapFunctionSpec::Command {
            program: "custom-mapper".to_string(),
            args: vec![],
        },
        ReduceFunctionSpec::ShellScript {
            script: "cat".to_string(),
        },
        1,
        1,
        10,
    );
    let reduce_res = run_simulated_mapreduce(
        reduce_spec,
        vec![
            Ok("k\t1\n"),
            Err((
                "Failed to spawn process 'sh': No such file or directory (os error 2)",
                "Process spawn error: No such file or directory (os error 2)",
            )),
        ],
    )
    .await;
    assert_eq!(reduce_res.status, "Failed");
    let reduce_err = reduce_res.error.as_deref().unwrap_or("");
    assert!(
        reduce_err.contains("Reducer for key 'k' failed")
            && reduce_err.contains("worker does not have 'sh' on PATH"),
        "expected missing-sh hint on ShellScript reducer failure (m9), got: {reduce_err}"
    );

    // 3. (m10): MapFunctionSpec::Command failing with not-found must NOT get the missing-sh hint.
    let cmd_spec = MapReduceJobSpec::new(
        "cmd_missing_job",
        vec!["hello".to_string()],
        MapFunctionSpec::Command {
            program: "custom-mapper".to_string(),
            args: vec![],
        },
        ReduceFunctionSpec::Builtin {
            operator: "sum".to_string(),
        },
        1,
        1,
        10,
    );
    let cmd_res = run_simulated_mapreduce(
        cmd_spec,
        vec![Err((
            "Failed to spawn process 'custom-mapper': No such file or directory (os error 2)",
            "Process spawn error: No such file or directory (os error 2)",
        ))],
    )
    .await;
    assert_eq!(cmd_res.status, "Failed");
    let cmd_err = cmd_res.error.as_deref().unwrap_or("");
    assert!(
        !cmd_err.contains("worker does not have 'sh' on PATH"),
        "Command stage failure must NOT claim 'sh' is missing (m10), got: {cmd_err}"
    );

    // 4. (m11): Builtin/ShellScript stage failing with E2BIG (os error 7) or normal non-zero exit
    // must NOT get the missing-sh hint.
    let e2big_spec = MapReduceJobSpec::new(
        "e2big_job",
        vec!["hello".to_string()],
        MapFunctionSpec::ShellScript {
            script: "echo hi".to_string(),
        },
        ReduceFunctionSpec::Builtin {
            operator: "sum".to_string(),
        },
        1,
        1,
        10,
    );
    let e2big_res = run_simulated_mapreduce(
        e2big_spec,
        vec![Err((
            "Failed to spawn process 'sh': Argument list too long (os error 7)",
            "Process spawn error: Argument list too long (os error 7)",
        ))],
    )
    .await;
    assert_eq!(e2big_res.status, "Failed");
    let e2big_err = e2big_res.error.as_deref().unwrap_or("");
    assert!(
        !e2big_err.contains("worker does not have 'sh' on PATH"),
        "E2BIG spawn error must NOT claim 'sh' is missing (m11), got: {e2big_err}"
    );
}
