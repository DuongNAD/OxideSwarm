//! Integration tests for the `ADOPT-2` run summary in `submit --wait` and `submit --wait --json`
//! (design 0059 D5).

use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};
use tokio::sync::watch;

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

async fn spawn_test_cluster() -> (
    rusty_grid_master::MasterHandle,
    String,
    watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
    tempfile::TempDir,
) {
    let temp = tempfile::tempdir().expect("tempdir");
    let blob_dir = temp.path().join("master_blobs");
    let sandbox_dir = temp.path().join("worker_sandboxes");
    std::fs::create_dir_all(&blob_dir).expect("create blob_dir");
    std::fs::create_dir_all(&sandbox_dir).expect("create sandbox_dir");

    let server_cfg = ServerConfig::from_addr("127.0.0.1:0")
        .expect("server config")
        .with_discovery_port(0)
        .with_blob_store(blob_dir, 64 * 1024 * 1024);
    let master = MasterServer::spawn(server_cfg).await.expect("spawn master");
    let master_addr = master.server_addr().to_string();

    let worker_cfg = WorkerConfig::new(&master_addr)
        .with_no_battery(true)
        .with_name("adopt2-worker")
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
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    (master, master_addr, shutdown_tx, worker_handle, temp)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_submit_wait_prints_summary_lines_and_json_fields() {
    let (master, master_addr, shutdown_tx, mut worker_handle, temp) = spawn_test_cluster().await;
    let home = temp.path();

    // 1. Human `submit --wait` prints the two D5 summary lines immediately after Execution Time.
    let ok_out = cli_cmd(home)
        .args([
            "submit",
            "--master",
            &master_addr,
            "--wait",
            "--",
            "echo",
            "adopt2-ok",
        ])
        .output()
        .expect("run submit --wait");
    assert!(
        ok_out.status.success(),
        "submit --wait failed: {}",
        String::from_utf8_lossy(&ok_out.stderr)
    );
    let ok_stdout = String::from_utf8_lossy(&ok_out.stdout);
    let lines: Vec<&str> = ok_stdout.lines().collect();
    let exec_idx = lines
        .iter()
        .position(|line| line.starts_with("Execution Time:") && line.ends_with("ms"))
        .unwrap_or_else(|| panic!("missing Execution Time line in stdout:\n{ok_stdout}"));
    assert!(
        lines.len() > exec_idx + 2,
        "expected two summary lines after Execution Time:\n{ok_stdout}"
    );
    assert_eq!(
        lines[exec_idx + 1],
        "Tasks: 1 finished, 0 failed, on 1 machines",
        "unexpected Tasks summary line:\n{ok_stdout}"
    );
    let waited_line = lines[exec_idx + 2];
    assert!(
        waited_line.starts_with("Waited: ")
            && waited_line.contains(" s    Sum of task times: ")
            && waited_line.ends_with(" s"),
        "unexpected Waited summary line:\n{ok_stdout}"
    );
    let (waited_part, sum_part) = waited_line
        .split_once(" s    Sum of task times: ")
        .unwrap_or_else(|| panic!("missing summary separator in:\n{waited_line}"));
    let waited_secs: f64 = waited_part
        .strip_prefix("Waited: ")
        .unwrap_or_else(|| panic!("missing Waited prefix in:\n{waited_part}"))
        .parse()
        .unwrap_or_else(|e| panic!("invalid waited seconds in '{waited_part}': {e}"));
    let sum_secs: f64 = sum_part
        .strip_suffix(" s")
        .unwrap_or_else(|| panic!("missing seconds suffix in:\n{sum_part}"))
        .parse()
        .unwrap_or_else(|e| panic!("invalid task time sum in '{sum_part}': {e}"));
    assert!(
        waited_secs >= sum_secs,
        "expected Waited ({waited_secs} s) >= Sum of task times ({sum_secs} s) for one task:\n{ok_stdout}"
    );
    assert!(
        !ok_stdout.contains('%'),
        "unexpected percentage in:\n{ok_stdout}"
    );
    assert!(
        !ok_stdout.contains("your machine"),
        "forbidden phrase in:\n{ok_stdout}"
    );

    // 2. Failed waited run reports `0 finished, 1 failed, on 1 machines`.
    let fail_out = cli_cmd(home)
        .args([
            "submit",
            "--master",
            &master_addr,
            "--type",
            "shell",
            "--command",
            "exit 3",
            "--wait",
        ])
        .output()
        .expect("run failed submit --wait");
    assert_eq!(fail_out.status.code(), Some(3));
    let fail_stdout = String::from_utf8_lossy(&fail_out.stdout);
    assert!(
        fail_stdout.contains("Tasks: 0 finished, 1 failed, on 1 machines"),
        "missing failed summary line in:\n{fail_stdout}"
    );
    assert!(
        fail_stdout.contains("Waited: ") && fail_stdout.contains(" s    Sum of task times: "),
        "missing waited summary line in failed run:\n{fail_stdout}"
    );

    // 3. `submit --wait --json` carries waited_ms, task_time_sum_ms, and machines.
    let json_out = cli_cmd(home)
        .args([
            "submit",
            "--master",
            &master_addr,
            "--wait",
            "--json",
            "--",
            "echo",
            "adopt2-json-ok",
        ])
        .output()
        .expect("run submit --wait --json");
    assert!(
        json_out.status.success(),
        "submit --wait --json failed: {}",
        String::from_utf8_lossy(&json_out.stderr)
    );
    let json_val: serde_json::Value =
        serde_json::from_slice(&json_out.stdout).expect("parse submit --json output");
    assert_eq!(
        json_val["exit_code"], 0,
        "unexpected exit_code in: {json_val}"
    );
    let worker_id_str = json_val["worker_id"]
        .as_str()
        .unwrap_or_else(|| panic!("missing worker_id in: {json_val}"));
    let parsed_worker = uuid::Uuid::parse_str(worker_id_str)
        .unwrap_or_else(|e| panic!("invalid worker_id UUID '{worker_id_str}': {e}"));
    assert!(
        !parsed_worker.is_nil(),
        "expected non-nil worker_id in: {json_val}"
    );
    assert_eq!(
        json_val["worker_name"], "adopt2-worker",
        "unexpected worker_name in: {json_val}"
    );
    assert!(
        json_val["stdout"]
            .as_str()
            .is_some_and(|s| s.contains("adopt2-json-ok")),
        "missing expected stdout in: {json_val}"
    );
    let waited_ms = json_val["waited_ms"]
        .as_u64()
        .unwrap_or_else(|| panic!("missing waited_ms in: {json_val}"));
    let task_time_sum_ms = json_val["task_time_sum_ms"]
        .as_u64()
        .unwrap_or_else(|| panic!("missing task_time_sum_ms in: {json_val}"));
    assert_eq!(
        json_val["task_time_sum_ms"], json_val["execution_time_ms"],
        "unexpected task_time_sum_ms in: {json_val}"
    );
    assert!(
        waited_ms >= task_time_sum_ms,
        "expected waited_ms ({waited_ms}) >= task_time_sum_ms ({task_time_sum_ms}): {json_val}"
    );
    assert_eq!(
        json_val["machines"], 1,
        "unexpected machines count in: {json_val}"
    );

    let _ = shutdown_tx.send(true);
    if tokio::time::timeout(Duration::from_secs(30), &mut worker_handle)
        .await
        .is_err()
    {
        worker_handle.abort();
        let _ = worker_handle.await;
    }
    let _ = master.shutdown();
    drop(temp);
}
