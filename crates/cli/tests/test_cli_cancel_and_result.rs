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
        .with_no_battery(true)
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

    // F30-4: `result <ID>` without `--wait` on a still-running task exits with code 3
    // (distinct from exit 1 for a failed task).
    let running_res = cli_cmd(home)
        .args(["result", &task_id_str, "--master", &master_addr])
        .output()
        .expect("spawn result without --wait on running task");
    assert_eq!(
        running_res.status.code(),
        Some(3),
        "expected exit code 3 for still-running task, got {:?}",
        running_res.status
    );

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

    // `result --wait` on the cancelled task reports cancellation without waiting 30s.
    let res_out = cli_cmd(home)
        .args(["result", &task_id_str, "--master", &master_addr, "--wait"])
        .output()
        .expect("spawn result --wait");
    assert!(!res_out.status.success());
    assert!(
        cancel_start.elapsed() < Duration::from_secs(10),
        "cancelled sleep task must stop well before 30s"
    );
    let res_stdout = String::from_utf8_lossy(&res_out.stdout);
    assert!(
        res_stdout.contains("cancelled") || res_stdout.contains("Cancelled"),
        "expected cancelled message in result stdout, got: {res_stdout}"
    );
    assert!(
        !res_stdout.contains("00000000-0000-0000-0000-000000000000"),
        "must not print nil worker UUID: {res_stdout}"
    );

    // F30-a: Submit a task that stays Queued (`--require-tag absent`), cancel it while queued
    // (so `result.worker_id` is `Uuid::nil()`), and assert `result` prints no `Worker:` line.
    let queued_sub = cli_cmd(home)
        .args([
            "submit",
            "--master",
            &master_addr,
            "--require-tag",
            "absent",
            "--json",
            "--",
            "echo",
            "never-runs",
        ])
        .output()
        .expect("spawn queued submit");
    assert!(queued_sub.status.success());
    let queued_sub_json: serde_json::Value =
        serde_json::from_slice(&queued_sub.stdout).expect("parse queued_sub json");
    let queued_id = queued_sub_json["task_id"].as_str().expect("queued_id");

    let queued_cancel = cli_cmd(home)
        .args(["cancel", queued_id, "--master", &master_addr])
        .output()
        .expect("cancel queued task");
    assert!(queued_cancel.status.success());

    let queued_res = cli_cmd(home)
        .args(["result", queued_id, "--master", &master_addr])
        .output()
        .expect("result of queued-cancelled task");
    assert!(!queued_res.status.success());
    let queued_res_stdout = String::from_utf8_lossy(&queued_res.stdout);
    assert!(
        !queued_res_stdout.contains("Worker:"),
        "result of a task cancelled while queued (nil worker_id) must print no Worker: line, got: {queued_res_stdout}"
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
            && wait_stdout.contains("hello-from-async-submit")
            && wait_stdout.contains("Worker: "),
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

    // 3. F30-5: Verify that a failed blob download (e.g. `--output-dir` is a regular file)
    // exits non-zero and reports `output_download_error` in `--json`.
    let not_a_dir = home.join("regular_file_not_dir");
    std::fs::write(&not_a_dir, "blocker").expect("write regular_file_not_dir");
    let bad_blob_dl = cli_cmd(home)
        .args([
            "result",
            &task_id_str,
            "--master",
            &master_addr,
            "--output-dir",
            not_a_dir.to_str().unwrap(),
            "--json",
        ])
        .output()
        .expect("spawn result with bad --output-dir for blob");
    assert!(
        !bad_blob_dl.status.success(),
        "failed blob download must exit non-zero"
    );
    let bad_blob_json: serde_json::Value =
        serde_json::from_slice(&bad_blob_dl.stdout).expect("parse bad_blob_dl json");
    assert!(
        bad_blob_json.get("output_download_error").is_some(),
        "expected output_download_error in json: {bad_blob_json}"
    );

    // 4. F30-5: Verify that `result` preserves the task's non-1 exit code (e.g. exit 7).
    let exit7_sub = cli_cmd(home)
        .args([
            "submit",
            "--master",
            &master_addr,
            "--type",
            "shell",
            "--interpreter",
            "python3",
            "--command",
            "import sys; sys.exit(7)",
            "--json",
        ])
        .output()
        .expect("spawn exit 7 submit");
    let exit7_json: serde_json::Value =
        serde_json::from_slice(&exit7_sub.stdout).expect("parse exit7_sub json");
    let exit7_id = exit7_json["task_id"].as_str().expect("exit7_id");

    let exit7_res = cli_cmd(home)
        .args(["result", exit7_id, "--master", &master_addr, "--wait"])
        .output()
        .expect("spawn result --wait for exit 7");
    assert_eq!(
        exit7_res.status.code(),
        Some(7),
        "expected exit code 7 from task, got {:?}",
        exit7_res.status
    );

    // 4b. N1: Verify that a task exiting with code 3 maps to process exit code 1 in `result`
    // (so exit code 3 is reserved unambiguously for a still-running/non-terminal task),
    // while `--json` preserves `"exit_code": 3`.
    let exit3_sub = cli_cmd(home)
        .args([
            "submit",
            "--master",
            &master_addr,
            "--type",
            "shell",
            "--interpreter",
            "python3",
            "--command",
            "import sys; sys.exit(3)",
            "--json",
        ])
        .output()
        .expect("spawn exit 3 submit");
    let exit3_json: serde_json::Value =
        serde_json::from_slice(&exit3_sub.stdout).expect("parse exit3_sub json");
    let exit3_id = exit3_json["task_id"].as_str().expect("exit3_id");

    let exit3_res = cli_cmd(home)
        .args([
            "result",
            exit3_id,
            "--master",
            &master_addr,
            "--wait",
            "--json",
        ])
        .output()
        .expect("spawn result --wait --json for exit 3");
    assert_eq!(
        exit3_res.status.code(),
        Some(1),
        "task exit code 3 must map to process exit code 1 so 3 stays reserved for in-progress tasks"
    );
    let exit3_res_json: serde_json::Value =
        serde_json::from_slice(&exit3_res.stdout).expect("parse exit3_res json");
    assert_eq!(exit3_res_json["exit_code"], 3);

    // 5. F30-1, F30-2, F30-5: Submit a task producing an inline `OutputArtifact` (`expected_outputs`),
    // verify saving it to a directory succeeds, refusing to write through a symlink leaves the victim
    // untouched and exits non-zero, and failing to save to a regular-file `--output-dir` exits non-zero
    // with `artifact_save_error` in `--json`.
    let inline_task = rusty_grid_core::Task::new(
        rusty_grid_core::TaskSpec::ShellScript {
            script: "import pathlib; pathlib.Path('rep.txt').write_text('TASK_PAYLOAD')".into(),
            interpreter: Some("python3".into()),
            env: Default::default(),
        },
        rusty_grid_core::TaskRequirements::default(),
    )
    .with_expected_outputs(vec!["rep.txt".into()]);
    let inline_id = inline_task.id.to_string();
    let mut transport = rusty_grid_cli::connect::connect_master(&master_addr, None)
        .await
        .expect("connect_master");
    transport
        .send_msg(&rusty_grid_core::protocol::ClientMessage::SubmitTask {
            task: inline_task,
            wait: true,
        })
        .await
        .expect("send inline task");
    let _ = transport
        .recv_msg::<rusty_grid_core::protocol::ClientResponse>()
        .await
        .expect("recv inline task completion");

    let inline_ok_dir = home.join("inline_ok");
    let inline_ok = cli_cmd(home)
        .args([
            "result",
            &inline_id,
            "--master",
            &master_addr,
            "--output-dir",
            inline_ok_dir.to_str().unwrap(),
            "--json",
        ])
        .output()
        .expect("spawn result for inline artifact");
    assert!(
        inline_ok.status.success(),
        "inline artifact save failed: {}",
        String::from_utf8_lossy(&inline_ok.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(inline_ok_dir.join("rep.txt")).expect("read rep.txt"),
        "TASK_PAYLOAD"
    );

    #[cfg(unix)]
    {
        let symlink_dir = home.join("symlink_dir");
        std::fs::create_dir_all(&symlink_dir).expect("create symlink_dir");
        let victim = home.join("victim.txt");
        std::fs::write(&victim, "SAFE_ORIGINAL").expect("write victim");
        std::os::unix::fs::symlink(&victim, symlink_dir.join("rep.txt")).expect("create symlink");

        let symlink_res = cli_cmd(home)
            .args([
                "result",
                &inline_id,
                "--master",
                &master_addr,
                "--output-dir",
                symlink_dir.to_str().unwrap(),
                "--json",
            ])
            .output()
            .expect("spawn result into symlink dir");
        assert!(
            !symlink_res.status.success(),
            "result must exit non-zero when artifact target is a symlink"
        );
        assert_eq!(
            std::fs::read_to_string(&victim).expect("read victim after symlink refusal"),
            "SAFE_ORIGINAL",
            "victim file targeted by symlink must remain untouched"
        );
        let symlink_json: serde_json::Value =
            serde_json::from_slice(&symlink_res.stdout).expect("parse symlink_res json");
        assert!(
            symlink_json.get("artifact_save_error").is_some(),
            "expected artifact_save_error in json: {symlink_json}"
        );

        // N2: Verify that a symlinked `--output-dir` root directory (such as `/tmp` -> `private/tmp`
        // on macOS) IS accepted for inline artifacts as long as its target is a directory and no
        // component below `--output-dir` is a symlink.
        let real_root_dir = home.join("real_output_root");
        std::fs::create_dir_all(&real_root_dir).expect("create real_output_root");
        let symlink_root_dir = home.join("symlink_output_root");
        std::os::unix::fs::symlink(&real_root_dir, &symlink_root_dir)
            .expect("create symlink_output_root");

        let symlink_root_res = cli_cmd(home)
            .args([
                "result",
                &inline_id,
                "--master",
                &master_addr,
                "--output-dir",
                symlink_root_dir.to_str().unwrap(),
                "--json",
            ])
            .output()
            .expect("spawn result into symlinked root output-dir");
        assert!(
            symlink_root_res.status.success(),
            "symlinked --output-dir root directory must be accepted: {}",
            String::from_utf8_lossy(&symlink_root_res.stderr)
        );
        assert_eq!(
            std::fs::read_to_string(real_root_dir.join("rep.txt"))
                .expect("read rep.txt through symlinked root output-dir"),
            "TASK_PAYLOAD"
        );
    }

    let bad_inline_dl = cli_cmd(home)
        .args([
            "result",
            &inline_id,
            "--master",
            &master_addr,
            "--output-dir",
            not_a_dir.to_str().unwrap(),
            "--json",
        ])
        .output()
        .expect("spawn result with file as --output-dir for inline artifact");
    assert!(
        !bad_inline_dl.status.success(),
        "failed inline artifact save must exit non-zero"
    );
    let bad_inline_json: serde_json::Value =
        serde_json::from_slice(&bad_inline_dl.stdout).expect("parse bad_inline_dl json");
    assert!(
        bad_inline_json.get("artifact_save_error").is_some(),
        "expected artifact_save_error in json: {bad_inline_json}"
    );

    // 6. F30-b: Verify that `submit --wait` also exits non-zero (`ExitCode::FAILURE`) and
    // reports `artifact_save_error` when saving an inline `OutputArtifact` fails (e.g. when
    // `--output-dir` is a regular file).
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock master for submit artifact_save_error");
    let mock_addr = mock_listener.local_addr().expect("mock_addr").to_string();
    let mock_handle = tokio::spawn(async move {
        if let Ok((stream, _)) = mock_listener.accept().await {
            let mut tx = rusty_grid_core::protocol::MessageTransport::new(stream);
            while let Ok(Some(msg)) = tx
                .recv_msg::<rusty_grid_core::protocol::ClientMessage>()
                .await
            {
                match msg {
                    rusty_grid_core::protocol::ClientMessage::SubmitTask { task, .. } => {
                        let mut res = rusty_grid_core::TaskResult::success(
                            uuid::Uuid::new_v4(),
                            task.id,
                            "ok",
                            5,
                            false,
                        );
                        res.artifacts
                            .push(rusty_grid_core::task::OutputArtifact::new(
                                "rep.txt",
                                b"INLINE_DATA".to_vec(),
                            ));
                        let _ = tx
                            .send_msg(&rusty_grid_core::protocol::ClientResponse::TaskCompleted {
                                task_id: task.id,
                                result: res,
                            })
                            .await;
                    }
                    rusty_grid_core::protocol::ClientMessage::ClusterStatus => {
                        let _ = tx
                            .send_msg(&rusty_grid_core::protocol::ClientResponse::ClusterStatus {
                                total_tasks: 0,
                                pending_tasks: 0,
                                running_tasks: 0,
                                completed_tasks: 0,
                                failed_tasks: 0,
                                active_bytes: 0,
                                max_active_bytes: 0,
                                store_error: None,
                                workers: Vec::new(),
                            })
                            .await;
                    }
                    rusty_grid_core::protocol::ClientMessage::ListWorkers => {
                        let _ = tx
                            .send_msg(&rusty_grid_core::protocol::ClientResponse::WorkerList {
                                workers: Vec::new(),
                            })
                            .await;
                    }
                    _ => break,
                }
            }
        }
    });

    let bad_submit_inline = cli_cmd(home)
        .args([
            "submit",
            "--master",
            &mock_addr,
            "--wait",
            "--output",
            "rep.txt",
            "--output-dir",
            not_a_dir.to_str().unwrap(),
            "--json",
            "--",
            "echo",
            "inline-fail",
        ])
        .output()
        .expect("spawn submit with regular file as --output-dir for inline artifact");
    assert!(
        !bad_submit_inline.status.success(),
        "submit must exit non-zero when saving inline OutputArtifact fails"
    );
    let bad_submit_inline_json: serde_json::Value =
        serde_json::from_slice(&bad_submit_inline.stdout).expect("parse bad_submit_inline json");
    assert!(
        bad_submit_inline_json.get("artifact_save_error").is_some(),
        "expected artifact_save_error in submit json: {bad_submit_inline_json}"
    );
    mock_handle.abort();

    let _ = shutdown_tx.send(true);
    worker_handle.abort();
    let _ = master.shutdown();
}
