//! End-to-end tests for `oxideswarm cancel <ID>` and `oxideswarm result <ID>` (CLI-1).

use std::path::{Path, PathBuf};
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
        .with_blob_store(blob_dir, 64 * 1024 * 1024);
    let master = MasterServer::spawn(server_cfg).await.expect("spawn master");
    let master_addr = master.server_addr().to_string();

    let worker_cfg = WorkerConfig::new(&master_addr)
        .with_name("cli1-worker")
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
async fn test_cli_cancel_stops_running_sleep_task() {
    let (master, master_addr, shutdown_tx, worker_handle, temp) = spawn_test_cluster().await;
    let home = temp.path();

    let submit_out = cli_cmd(home)
        .args([
            "submit",
            "--master",
            &master_addr,
            "--type",
            "shell",
            "--interpreter",
            "python3",
            "--command",
            "import time; time.sleep(30)",
            "--json",
        ])
        .output()
        .expect("spawn submit");
    assert!(
        submit_out.status.success(),
        "submit failed: {}",
        String::from_utf8_lossy(&submit_out.stderr)
    );
    let submit_json: serde_json::Value =
        serde_json::from_slice(&submit_out.stdout).expect("parse submit json");
    let task_id_str = submit_json["task_id"]
        .as_str()
        .expect("task_id string")
        .to_string();

    // Wait until the sleep task is Running on the worker.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let tasks = master.queue().list_tasks().await;
        if let Some(info) = tasks.first() {
            if format!("{:?}", info.state) == "Running" {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "sleep task did not start running"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    let cancel_start = Instant::now();
    let cancel_out = cli_cmd(home)
        .args(["cancel", &task_id_str, "--master", &master_addr])
        .output()
        .expect("spawn cancel");
    assert!(
        cancel_out.status.success(),
        "cancel failed: {}",
        String::from_utf8_lossy(&cancel_out.stderr)
    );
    let cancel_stdout = String::from_utf8_lossy(&cancel_out.stdout);
    assert!(
        cancel_stdout.contains(&format!("Task {task_id_str} cancelled.")),
        "unexpected cancel stdout: {cancel_stdout}"
    );

    // Repeating cancel on an already cancelled task exits non-zero.
    let second_cancel = cli_cmd(home)
        .args(["cancel", &task_id_str, "--master", &master_addr, "--json"])
        .output()
        .expect("spawn second cancel");
    assert!(!second_cancel.status.success());
    let second_json: serde_json::Value =
        serde_json::from_slice(&second_cancel.stdout).expect("parse second cancel json");
    assert_eq!(second_json["cancelled"], false);

    // `result --wait` on the cancelled task reports failure without waiting 30s.
    let res_out = cli_cmd(home)
        .args(["result", &task_id_str, "--master", &master_addr, "--wait"])
        .output()
        .expect("spawn result --wait");
    assert!(!res_out.status.success());
    assert!(
        cancel_start.elapsed() < Duration::from_secs(10),
        "cancelled sleep task must stop well before 30s"
    );

    let _ = shutdown_tx.send(true);
    worker_handle.abort();
    let _ = master.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cli_result_reads_finished_task_and_downloads_outputs() {
    let (master, master_addr, shutdown_tx, worker_handle, temp) = spawn_test_cluster().await;
    let home = temp.path();

    // 1. Submit without `--wait` (so `submit` returns immediately with the task_id).
    let async_out = cli_cmd(home)
        .args([
            "submit",
            "--master",
            &master_addr,
            "--type",
            "shell",
            "--interpreter",
            "python3",
            "--command",
            "print('hello-from-async-submit')",
            "--json",
        ])
        .output()
        .expect("spawn async submit");
    assert!(
        async_out.status.success(),
        "async submit failed: {}",
        String::from_utf8_lossy(&async_out.stderr)
    );
    let async_json: serde_json::Value =
        serde_json::from_slice(&async_out.stdout).expect("parse async submit json");
    let async_id = async_json["task_id"]
        .as_str()
        .expect("async task_id")
        .to_string();

    // Wait for completion via `result --wait`, then read again without `--wait` in JSON mode.
    let wait_out = cli_cmd(home)
        .args(["result", &async_id, "--master", &master_addr, "--wait"])
        .output()
        .expect("spawn result --wait");
    assert!(
        wait_out.status.success(),
        "result --wait failed: {}",
        String::from_utf8_lossy(&wait_out.stderr)
    );
    let wait_stdout = String::from_utf8_lossy(&wait_out.stdout);
    assert!(
        wait_stdout.contains("completed successfully.")
            && wait_stdout.contains("hello-from-async-submit"),
        "unexpected result stdout: {wait_stdout}"
    );

    let read_out = cli_cmd(home)
        .args(["result", &async_id, "--master", &master_addr, "--json"])
        .output()
        .expect("spawn result --json");
    assert!(read_out.status.success());
    let read_json: serde_json::Value =
        serde_json::from_slice(&read_out.stdout).expect("parse result json");
    assert_eq!(read_json["exit_code"], 0);
    assert!(
        read_json["stdout"]
            .as_str()
            .unwrap_or("")
            .contains("hello-from-async-submit"),
        "unexpected json result: {read_json}"
    );

    // 2. Submit a task that produces a blob output and verify `result --output-dir` downloads it.
    let submit_out_dir = home.join("submit_out");
    let out_submit = cli_cmd(home)
        .args([
            "submit",
            "--master",
            &master_addr,
            "--type",
            "shell",
            "--interpreter",
            "python3",
            "--command",
            "import pathlib; print('hello-from-async-result'); pathlib.Path('async_out.txt').write_text('payload-42')",
            "--output",
            "async_out.txt",
            "--output-dir",
            submit_out_dir.to_str().unwrap(),
            "--json",
        ])
        .output()
        .expect("spawn submit with output");
    assert!(
        out_submit.status.success(),
        "submit with output failed: {}",
        String::from_utf8_lossy(&out_submit.stderr)
    );
    let out_submit_json: serde_json::Value =
        serde_json::from_slice(&out_submit.stdout).expect("parse submit with output json");
    let task_id_str = out_submit_json["task_id"]
        .as_str()
        .expect("task_id")
        .to_string();

    let out_dir: PathBuf = home.join("downloaded_results");
    let result_out = cli_cmd(home)
        .args([
            "result",
            &task_id_str,
            "--master",
            &master_addr,
            "--output-dir",
            out_dir.to_str().unwrap(),
            "--json",
        ])
        .output()
        .expect("spawn result --output-dir");
    assert!(
        result_out.status.success(),
        "result --output-dir failed: {}",
        String::from_utf8_lossy(&result_out.stderr)
    );

    let downloaded = std::fs::read_to_string(out_dir.join("async_out.txt"))
        .expect("async_out.txt must be downloaded by `oxideswarm result --output-dir`");
    assert_eq!(downloaded, "payload-42");

    let _ = shutdown_tx.send(true);
    worker_handle.abort();
    let _ = master.shutdown();
}
