//! End-to-end integration tests for master task store durability and startup replay
//! (`docs/design/0046-master-durability.md` D1–D3).

use std::time::{Duration, Instant};

use tokio::sync::watch;

use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_master::queue::TaskState;
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_master::store::HEADER_LEN;
use rusty_grid_worker::client::WorkerClient;

async fn wait_for<F, Fut>(timeout: Duration, step: Duration, mut condition: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if condition().await {
            return true;
        }
        tokio::time::sleep(step).await;
    }
    false
}

fn builtin_task(name: &str, duration_ms: u64, max_retries: Option<u32>) -> Task {
    let mut req = TaskRequirements::generic(1, 16);
    req.max_retries = max_retries;
    Task::new(
        TaskSpec::BuiltinTest {
            test_name: name.to_string(),
            iterations: 4,
            duration_ms,
            should_fail: false,
            require_gpu: false,
        },
        req,
    )
}

#[tokio::test]
async fn test_master_restart_recovers_completed_queued_uncharged_running_and_fails_zero_retry_running(
) {
    let dir = tempfile::tempdir().expect("tempdir");
    let state_dir = dir.path().join("master-state");

    let (t_done_id, t_run_retryable_id, t_run_zero_retry_id, t_q1_id, t_q2_id) = {
        let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(false)
            .with_discovery(false)
            .with_state_dir(&state_dir);
        let master1 = MasterServer::spawn(config).await.expect("spawn master1");
        let master1_addr = master1.server_addr().to_string();

        let (w1_shutdown_tx, w1_shutdown_rx) = watch::channel(false);
        let (w2_shutdown_tx, w2_shutdown_rx) = watch::channel(false);

        let mut w1 =
            WorkerClient::for_testing(master1_addr.clone(), Some("dur-w1".into()), Some(1), false);
        let mut w2 =
            WorkerClient::for_testing(master1_addr.clone(), Some("dur-w2".into()), Some(1), false);

        let w1_handle = tokio::spawn(async move {
            let _ = w1.run(w1_shutdown_rx).await;
        });

        assert!(
            wait_for(
                Duration::from_secs(5),
                Duration::from_millis(25),
                || async {
                    master1
                        .list_workers()
                        .await
                        .map(|w| w.len() == 1)
                        .unwrap_or(false)
                }
            )
            .await,
            "worker 1 must register with master1"
        );

        // 1. `t_done`: finishes before master1 stops.
        let t_done = builtin_task("t_done", 10, Some(2));
        let t_done_id = master1.submit_task(t_done).await.expect("submit t_done");
        let done_res = master1
            .wait_task(t_done_id, Some(Duration::from_secs(10)))
            .await
            .expect("t_done completion");
        assert!(done_res.is_success());

        // 2. `t_run_retryable`: enters Running on w1 (occupying w1's single concurrency slot).
        let t_run_retryable = builtin_task("t_run_retryable", 3_000, Some(2));
        let t_run_retryable_id = master1
            .submit_task(t_run_retryable)
            .await
            .expect("submit t_run_retryable");
        assert!(
            wait_for(
                Duration::from_secs(5),
                Duration::from_millis(20),
                || async {
                    master1
                        .get_task_info(t_run_retryable_id)
                        .await
                        .map(|i| i.state == TaskState::Running)
                        .unwrap_or(false)
                }
            )
            .await,
            "t_run_retryable must enter Running on w1"
        );

        // 3. Connect w2 (1 slot) and start `t_run_zero_retry` (`max_retries == Some(0)`).
        let w2_handle = tokio::spawn(async move {
            let _ = w2.run(w2_shutdown_rx).await;
        });
        assert!(
            wait_for(
                Duration::from_secs(5),
                Duration::from_millis(25),
                || async {
                    master1
                        .list_workers()
                        .await
                        .map(|w| w.len() == 2)
                        .unwrap_or(false)
                }
            )
            .await,
            "worker 2 must register with master1"
        );

        let t_run_zero_retry = builtin_task("t_run_zero_retry", 3_000, Some(0));
        let t_run_zero_retry_id = master1
            .submit_task(t_run_zero_retry)
            .await
            .expect("submit t_run_zero_retry");
        assert!(
            wait_for(
                Duration::from_secs(5),
                Duration::from_millis(20),
                || async {
                    master1
                        .get_task_info(t_run_zero_retry_id)
                        .await
                        .map(|i| i.state == TaskState::Running)
                        .unwrap_or(false)
                }
            )
            .await,
            "t_run_zero_retry must enter Running on w2"
        );

        // 4. `t_q1` and `t_q2`: remain Queued in FIFO order behind the two saturated 1-slot workers.
        let t_q1 = builtin_task("t_q1", 15, Some(2));
        let t_q2 = builtin_task("t_q2", 15, Some(2));
        let t_q1_id = master1.submit_task(t_q1).await.expect("submit t_q1");
        let t_q2_id = master1.submit_task(t_q2).await.expect("submit t_q2");
        assert_eq!(
            master1.get_task_info(t_q1_id).await.unwrap().state,
            TaskState::Queued
        );
        assert_eq!(
            master1.get_task_info(t_q2_id).await.unwrap().state,
            TaskState::Queued
        );

        // Shut down master1 cleanly while both running tasks are still in flight (before workers
        // disconnect), compacting state into `snapshot.bin` and resetting `journal.bin`.
        master1.shutdown().expect("shutdown master1");
        let _ = w1_shutdown_tx.send(true);
        let _ = w2_shutdown_tx.send(true);
        w1_handle.abort();
        w2_handle.abort();

        (
            t_done_id,
            t_run_retryable_id,
            t_run_zero_retry_id,
            t_q1_id,
            t_q2_id,
        )
    };

    assert!(
        state_dir.join("snapshot.bin").exists(),
        "clean master shutdown must write snapshot.bin"
    );
    assert_eq!(
        std::fs::metadata(state_dir.join("journal.bin"))
            .expect("journal.bin metadata")
            .len(),
        HEADER_LEN as u64,
        "clean master shutdown must truncate journal.bin back to its 24-byte header"
    );

    // Start master2 on the same `--state-dir` with session-resume hold disabled (`0 s`) so
    // unresumed `max_retries == 0` fails immediately and `ready_task_ids` is populated at startup.
    let config2 = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(false)
        .with_discovery(false)
        .with_state_dir(&state_dir)
        .with_session_resume_hold(Duration::ZERO);
    let master2 = MasterServer::spawn(config2).await.expect("spawn master2");

    // Verify recovered states before any worker connects to master2:
    // - `t_done` is immediately readable as `Completed` without re-execution.
    let recovered_done = master2
        .wait_task(t_done_id, Some(Duration::from_secs(1)))
        .await
        .expect("recovered t_done result");
    assert!(recovered_done.is_success());

    // - `t_run_zero_retry` (`Running` with `max_retries == 0`) is immediately `Failed`.
    let zero_info = master2
        .get_task_info(t_run_zero_retry_id)
        .await
        .expect("t_run_zero_retry info");
    assert_eq!(zero_info.state, TaskState::Failed);
    assert_eq!(
        zero_info.error_message.as_deref(),
        Some("master restarted while the task ran; not re-run because max_retries is 0")
    );
    let zero_res = master2
        .wait_task(t_run_zero_retry_id, Some(Duration::from_secs(1)))
        .await
        .expect("wait_task on failed zero-retry task");
    assert!(!zero_res.is_success());

    // - `t_run_retryable` transitioned from `Running` to `Queued` with uncharged counters.
    let retryable_before = master2
        .get_task_info(t_run_retryable_id)
        .await
        .expect("t_run_retryable info before worker connects");
    assert_eq!(retryable_before.state, TaskState::Queued);
    assert_eq!(retryable_before.retry_count, 0);
    assert_eq!(retryable_before.failover_count, 0);
    assert_eq!(retryable_before.evacuation_count, 0);

    // - Queued tasks kept their FIFO order: `t_run_retryable`, then `t_q1`, then `t_q2`.
    let ready_ids = master2.queue().ready_task_ids(10).await;
    assert_eq!(ready_ids, vec![t_run_retryable_id, t_q1_id, t_q2_id]);

    // Connect a worker to master2 and verify `t_run_retryable`, `t_q1`, and `t_q2` all execute
    // and complete with uncharged counters.
    let master2_addr = master2.server_addr().to_string();
    let (w3_shutdown_tx, w3_shutdown_rx) = watch::channel(false);
    let mut w3 = WorkerClient::for_testing(master2_addr, Some("dur-w3".into()), Some(2), false);
    let w3_handle = tokio::spawn(async move {
        let _ = w3.run(w3_shutdown_rx).await;
    });

    for tid in [t_run_retryable_id, t_q1_id, t_q2_id] {
        let res = master2
            .wait_task(tid, Some(Duration::from_secs(15)))
            .await
            .unwrap_or_else(|e| panic!("task {tid} failed to finish after restart: {e}"));
        assert!(res.is_success(), "task {tid} must succeed after replay");
    }

    let retryable_after = master2
        .get_task_info(t_run_retryable_id)
        .await
        .expect("t_run_retryable info after completion");
    assert_eq!(retryable_after.state, TaskState::Completed);
    assert_eq!(retryable_after.retry_count, 0);
    assert_eq!(retryable_after.failover_count, 0);
    assert_eq!(retryable_after.evacuation_count, 0);

    let _ = w3_shutdown_tx.send(true);
    w3_handle.abort();
    master2.shutdown().expect("shutdown master2");
}

fn gated_marker_task(
    marker_path: &std::path::Path,
    gate_path: &std::path::Path,
    done_marker_path: Option<&std::path::Path>,
    output_file: Option<(&str, &str)>,
    max_retries: Option<u32>,
) -> Task {
    let mut env = std::collections::HashMap::new();
    env.insert(
        "MARKER_PATH".to_string(),
        marker_path.to_string_lossy().to_string(),
    );
    env.insert(
        "GATE_PATH".to_string(),
        gate_path.to_string_lossy().to_string(),
    );
    if let Some(done) = done_marker_path {
        env.insert(
            "DONE_MARKER_PATH".to_string(),
            done.to_string_lossy().to_string(),
        );
    }
    if let Some((out_name, out_body)) = output_file {
        env.insert("OUT_FILE".to_string(), out_name.to_string());
        env.insert("OUT_BODY".to_string(), out_body.to_string());
    }

    let mut req = TaskRequirements::generic(1, 16);
    req.max_retries = max_retries;
    req.timeout_secs = 20;

    #[cfg(unix)]
    let spec = TaskSpec::ShellScript {
        script: concat!(
            "printf 'run\\n' >> \"$MARKER_PATH\"\n",
            "while [ ! -f \"$GATE_PATH\" ]; do sleep 0.03; done\n",
            "if [ -n \"${OUT_FILE:-}\" ]; then printf '%s' \"$OUT_BODY\" > \"$OUT_FILE\"; fi\n",
            "if [ -n \"${DONE_MARKER_PATH:-}\" ]; then printf 'done\\n' >> \"$DONE_MARKER_PATH\"; fi\n"
        )
        .to_string(),
        interpreter: None,
        env,
    };

    #[cfg(windows)]
    let spec = TaskSpec::Command {
        program: "powershell".to_string(),
        args: vec![
            "-NoProfile".to_string(),
            "-NonInteractive".to_string(),
            "-Command".to_string(),
            concat!(
                "Add-Content -Path $env:MARKER_PATH -Value 'run'; ",
                "while (-not (Test-Path $env:GATE_PATH)) { Start-Sleep -Milliseconds 30 }; ",
                "if ($env:OUT_FILE) { [System.IO.File]::WriteAllText($env:OUT_FILE, $env:OUT_BODY) }; ",
                "if ($env:DONE_MARKER_PATH) { Add-Content -Path $env:DONE_MARKER_PATH -Value 'done' }"
            )
            .to_string(),
        ],
        env,
        working_dir: None,
        stdin: None,
    };

    let mut task = Task::new(spec, req);
    if let Some((out_name, _)) = output_file {
        task.output_globs = vec![out_name.to_string()];
    }
    task
}

fn timed_sleep_marker_task(
    started_marker: &std::path::Path,
    completed_marker: &std::path::Path,
    sleep_ms: u64,
    max_retries: Option<u32>,
) -> Task {
    let mut env = std::collections::HashMap::new();
    env.insert(
        "STARTED_MARKER".to_string(),
        started_marker.to_string_lossy().to_string(),
    );
    env.insert(
        "COMPLETED_MARKER".to_string(),
        completed_marker.to_string_lossy().to_string(),
    );

    let mut req = TaskRequirements::generic(1, 16);
    req.max_retries = max_retries;
    req.timeout_secs = 20;

    #[cfg(unix)]
    let spec = TaskSpec::ShellScript {
        script: format!(
            "printf 'started\\n' >> \"$STARTED_MARKER\"\nsleep {:.2}\nprintf 'completed\\n' >> \"$COMPLETED_MARKER\"\n",
            sleep_ms as f64 / 1000.0
        ),
        interpreter: None,
        env,
    };

    #[cfg(windows)]
    let spec = TaskSpec::Command {
        program: "powershell".to_string(),
        args: vec![
            "-NoProfile".to_string(),
            "-NonInteractive".to_string(),
            "-Command".to_string(),
            format!(
                "Add-Content -Path $env:STARTED_MARKER -Value 'started'; Start-Sleep -Milliseconds {sleep_ms}; Add-Content -Path $env:COMPLETED_MARKER -Value 'completed'"
            ),
        ],
        env,
        working_dir: None,
        stdin: None,
    };

    Task::new(spec, req)
}

async fn spawn_rebound_master(config: ServerConfig, timeout: Duration) -> MasterHandle {
    let deadline = Instant::now() + timeout;
    loop {
        match MasterServer::spawn(config.clone()).await {
            Ok(m) => return m,
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(e) => panic!("failed to rebind master on {}: {e}", config.bind_addr),
        }
    }
}

/// `docs/design/0046-master-durability.md` G4 / Test 1:
/// Start a master with `--state-dir` and one connected 1-slot worker.
/// Submit 4 tasks: `t_done` (completes before kill), `t_run` (writes an execution counter to a
/// marker file and runs across the master restart), and `t_q1`, `t_q2` (queued behind `t_run`).
/// Kill the first master and restart a second master on the same port and `--state-dir`.
/// Verify `t_done` reads back immediately, `t_run` is adopted via `ResumeSession` and finishes
/// with marker count == 1 (ran **exactly once**), and `t_q1`, `t_q2` both complete.
#[tokio::test]
async fn test_master_restart_recovers_finished_queued_and_adopts_running_task_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let state_dir = dir.path().join("master-state");
    let marker_path = dir.path().join("t_run.marker");
    let gate_path = dir.path().join("t_run.gate");

    let config1 = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(false)
        .with_discovery(false)
        .with_state_dir(&state_dir);
    let master1 = MasterServer::spawn(config1).await.expect("spawn master1");
    let master_addr = master1.server_addr();

    let (w1_shutdown_tx, w1_shutdown_rx) = watch::channel(false);
    let mut w1 = WorkerClient::for_testing(
        master_addr.to_string(),
        Some("resume-w1".into()),
        Some(1),
        false,
    );
    w1.set_backoff_config(rusty_grid_worker::backoff::BackoffConfig {
        initial: Duration::from_millis(80),
        max: Duration::from_millis(200),
        factor: 1.5,
        jitter_ratio: 0.0,
    });
    let w1_id = w1.worker_id();
    let w1_handle = tokio::spawn(async move {
        let _ = w1.run(w1_shutdown_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(25),
            || async {
                master1
                    .list_workers()
                    .await
                    .map(|w| w.len() == 1)
                    .unwrap_or(false)
            }
        )
        .await,
        "worker 1 must register with master1"
    );

    // 1. `t_done`: finishes before master1 stops.
    let t_done = builtin_task("t_done", 10, Some(2));
    let t_done_id = master1.submit_task(t_done).await.expect("submit t_done");
    let done_res = master1
        .wait_task(t_done_id, Some(Duration::from_secs(10)))
        .await
        .expect("t_done completion");
    assert!(done_res.is_success());

    // 2. `t_run`: writes to `marker_path` and waits on `gate_path` while master1 restarts.
    let t_run = gated_marker_task(
        &marker_path,
        &gate_path,
        None,
        Some(("out.txt", "adopted-output")),
        Some(2),
    );
    let t_run_id = master1.submit_task(t_run).await.expect("submit t_run");

    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(25),
            || async {
                let is_running = master1
                    .get_task_info(t_run_id)
                    .await
                    .map(|i| i.state == TaskState::Running)
                    .unwrap_or(false);
                let marker_written = std::fs::read_to_string(&marker_path)
                    .map(|s| s.lines().count() == 1)
                    .unwrap_or(false);
                is_running && marker_written
            }
        )
        .await,
        "t_run must be Running on master1 and have written its marker line"
    );

    // 3. `t_q1` and `t_q2`: remain Queued behind `t_run`.
    let t_q1_id = master1
        .submit_task(builtin_task("t_q1", 15, Some(2)))
        .await
        .expect("submit t_q1");
    let t_q2_id = master1
        .submit_task(builtin_task("t_q2", 15, Some(2)))
        .await
        .expect("submit t_q2");
    assert_eq!(
        master1.get_task_info(t_q1_id).await.unwrap().state,
        TaskState::Queued
    );
    assert_eq!(
        master1.get_task_info(t_q2_id).await.unwrap().state,
        TaskState::Queued
    );

    // 4. Shut down master1 while `t_run` is still running on `w1`, then start master2 on the same
    // address and `--state-dir`.
    master1.shutdown().expect("shutdown master1");
    drop(master1);

    let config2 = ServerConfig::new(master_addr)
        .with_web_ui(false)
        .with_discovery(false)
        .with_state_dir(&state_dir);
    let master2 = spawn_rebound_master(config2, Duration::from_secs(5)).await;

    // 5. `t_done` is immediately readable from master2 before `t_run` finishes.
    let recovered_done = master2
        .wait_task(t_done_id, Some(Duration::from_secs(1)))
        .await
        .expect("recovered t_done result");
    assert!(recovered_done.is_success());

    // 6. Wait for `w1` to reconnect to master2 and adopt `t_run` back into `TaskState::Running`.
    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(25),
            || async {
                let task_adopted = master2
                    .get_task_info(t_run_id)
                    .await
                    .map(|i| i.state == TaskState::Running && i.assigned_worker_id == Some(w1_id))
                    .unwrap_or(false);
                let worker_credited = master2
                    .list_workers()
                    .await
                    .map(|ws| {
                        ws.iter()
                            .any(|w| w.worker_id == w1_id && w.active_tasks == 1)
                    })
                    .unwrap_or(false);
                task_adopted && worker_credited
            }
        )
        .await,
        "w1 must reconnect to master2 and adopt t_run into Running with active_tasks == 1"
    );

    // 7. Release `gate_path` so `t_run` finishes, uploads `out.txt`, and frees `w1` to run `t_q1` and `t_q2`.
    std::fs::write(&gate_path, b"go").expect("write gate file");

    let run_res = master2
        .wait_task(t_run_id, Some(Duration::from_secs(10)))
        .await
        .expect("t_run completion after adoption");
    assert!(run_res.is_success(), "t_run must succeed: {:?}", run_res);
    assert_eq!(run_res.outputs.len(), 1);
    assert_eq!(run_res.outputs[0].path, "out.txt");

    for tid in [t_q1_id, t_q2_id] {
        let res = master2
            .wait_task(tid, Some(Duration::from_secs(10)))
            .await
            .unwrap_or_else(|e| panic!("queued task {tid} failed to finish: {e}"));
        assert!(res.is_success());
    }

    // 8. Verify `t_run` ran **exactly once** with zero retries/failovers.
    let marker_contents = std::fs::read_to_string(&marker_path).expect("read marker file");
    assert_eq!(
        marker_contents.lines().count(),
        1,
        "t_run must have executed exactly once across the master restart"
    );
    let run_info = master2.get_task_info(t_run_id).await.expect("t_run info");
    assert_eq!(run_info.state, TaskState::Completed);
    assert_eq!(run_info.retry_count, 0);
    assert_eq!(run_info.failover_count, 0);
    assert_eq!(run_info.evacuation_count, 0);

    let _ = w1_shutdown_tx.send(true);
    w1_handle.abort();
    master2.shutdown().expect("shutdown master2");
}

/// Verifies `D4.3` disconnected outbox + output blob upload:
/// When a task finishes *while the master is down*, the worker retains its output blob in
/// `BlobCache` (pinned by `CachePin`) and buffers the `TaskResult` in `pending_resume_outbox`.
/// Upon reconnecting and sending `ResumeSession`, the worker uploads the output blob to the
/// restarted master's `BlobStore` before flushing `TaskResult`.
#[tokio::test]
async fn test_worker_disconnected_outbox_uploads_output_blobs_and_flushes_result_on_resume() {
    let dir = tempfile::tempdir().expect("tempdir");
    let state_dir = dir.path().join("master-state");
    let marker_path = dir.path().join("outbox.marker");
    let gate_path = dir.path().join("outbox.gate");
    let done_marker_path = dir.path().join("outbox.done");

    let config1 = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(false)
        .with_discovery(false)
        .with_state_dir(&state_dir);
    let master1 = MasterServer::spawn(config1).await.expect("spawn master1");
    let master_addr = master1.server_addr();

    let (w1_shutdown_tx, w1_shutdown_rx) = watch::channel(false);
    let mut w1 = WorkerClient::for_testing(
        master_addr.to_string(),
        Some("outbox-w1".into()),
        Some(1),
        false,
    );
    w1.set_backoff_config(rusty_grid_worker::backoff::BackoffConfig {
        initial: Duration::from_millis(80),
        max: Duration::from_millis(200),
        factor: 1.5,
        jitter_ratio: 0.0,
    });
    let w1_handle = tokio::spawn(async move {
        let _ = w1.run(w1_shutdown_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(25),
            || async {
                master1
                    .list_workers()
                    .await
                    .map(|w| w.len() == 1)
                    .unwrap_or(false)
            }
        )
        .await,
        "worker 1 must register with master1"
    );

    let t_outbox = gated_marker_task(
        &marker_path,
        &gate_path,
        Some(&done_marker_path),
        Some(("artifact.txt", "disconnected-outbox-blob-payload")),
        Some(2),
    );
    let t_outbox_id = master1
        .submit_task(t_outbox)
        .await
        .expect("submit t_outbox");

    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(25),
            || async {
                let is_running = master1
                    .get_task_info(t_outbox_id)
                    .await
                    .map(|i| i.state == TaskState::Running)
                    .unwrap_or(false);
                let marker_written = std::fs::read_to_string(&marker_path)
                    .map(|s| s.lines().count() == 1)
                    .unwrap_or(false);
                is_running && marker_written
            }
        )
        .await,
        "t_outbox must be Running on master1"
    );

    // Stop master1, then allow `t_outbox` to finish while master1 is down so the worker buffers
    // its output blob and `TaskResult` in `pending_resume_outbox`.
    master1.shutdown().expect("shutdown master1");
    drop(master1);

    std::fs::write(&gate_path, b"go").expect("write gate file while master is down");
    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(25),
            || async { done_marker_path.exists() }
        )
        .await,
        "t_outbox subprocess must finish while master is down"
    );
    // Give the worker's spawned task a moment to harvest `artifact.txt` and buffer `TaskResult`.
    tokio::time::sleep(Duration::from_millis(120)).await;

    // Now start master2 on the same address and `--state-dir`.
    let config2 = ServerConfig::new(master_addr)
        .with_web_ui(false)
        .with_discovery(false)
        .with_state_dir(&state_dir);
    let master2 = spawn_rebound_master(config2, Duration::from_secs(5)).await;

    let res = master2
        .wait_task(t_outbox_id, Some(Duration::from_secs(10)))
        .await
        .expect("t_outbox completion from disconnected outbox");
    assert!(res.is_success(), "t_outbox must succeed: {:?}", res);
    assert_eq!(res.outputs.len(), 1);
    assert_eq!(res.outputs[0].path, "artifact.txt");

    let store = master2.blob_store().expect("master2 blob store");
    let blob_id = res.outputs[0].id;
    assert!(
        store.contains(&blob_id),
        "output blob must be uploaded to master2 blob store before TaskResult"
    );
    let hex = blob_id.to_hex();
    let blob_path = state_dir.join("blobs/objects").join(&hex[..2]).join(&hex);
    let blob_bytes = std::fs::read(blob_path).expect("read uploaded blob bytes");
    assert_eq!(blob_bytes, b"disconnected-outbox-blob-payload");

    let marker_contents = std::fs::read_to_string(&marker_path).expect("read marker");
    assert_eq!(marker_contents.lines().count(), 1);

    let _ = w1_shutdown_tx.send(true);
    w1_handle.abort();
    master2.shutdown().expect("shutdown master2");
}

/// `docs/design/0046-master-durability.md` G4 / Test 2:
/// Configure a short worker resume grace (`1 s`) and delay the first worker's reconnect (`4 s`)
/// until after the restarted master's `2 s` hold has expired. Verify via the marker files that
/// the first worker killed its local task when its `1 s` grace expired (before `hold` expired),
/// and only the post-expiry execution completed (`completed_marker` line count == 1).
#[tokio::test]
async fn test_worker_reconnecting_after_master_hold_expires_runs_task_only_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let state_dir = dir.path().join("master-state");
    let started_marker = dir.path().join("started.marker");
    let completed_marker = dir.path().join("completed.marker");

    // Master 1: heartbeat = 1 s (`dead_peer_timeout` = 4 s, `margin` = 3 s) and hold = 8 s so
    // `max_safe_grace` = 8 - 4 - 3 = 1 s.
    let config1 = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(false)
        .with_discovery(false)
        .with_heartbeat_interval(1)
        .with_state_dir(&state_dir)
        .with_session_resume_hold(Duration::from_secs(8));
    let master1 = MasterServer::spawn(config1).await.expect("spawn master1");
    let master_addr = master1.server_addr();

    let (w1_shutdown_tx, w1_shutdown_rx) = watch::channel(false);
    let mut w1 = WorkerClient::for_testing(
        master_addr.to_string(),
        Some("expired-w1".into()),
        Some(1),
        false,
    );
    // Delay w1's reconnect attempt to 4 s so its 1 s grace expires while disconnected, and
    // master2's 2 s hold also expires before w1 reconnects.
    w1.set_backoff_config(rusty_grid_worker::backoff::BackoffConfig {
        initial: Duration::from_secs(4),
        max: Duration::from_secs(4),
        factor: 1.0,
        jitter_ratio: 0.0,
    });
    let w1_handle = tokio::spawn(async move {
        let _ = w1.run(w1_shutdown_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(25),
            || async {
                master1
                    .list_workers()
                    .await
                    .map(|w| w.len() == 1)
                    .unwrap_or(false)
            }
        )
        .await,
        "w1 must register with master1"
    );

    // Submit a task that takes 2.2 s to write `completed.marker`. Since w1's grace is 1.0 s, w1
    // kills its first attempt at ~1.0 s after master1 stops, before `completed.marker` is written
    // and before master2's 2.0 s hold expires.
    let t_task = timed_sleep_marker_task(&started_marker, &completed_marker, 2_200, Some(2));
    let t_task_id = master1.submit_task(t_task).await.expect("submit t_task");

    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(25),
            || async {
                let is_running = master1
                    .get_task_info(t_task_id)
                    .await
                    .map(|i| i.state == TaskState::Running)
                    .unwrap_or(false);
                let started_once = std::fs::read_to_string(&started_marker)
                    .map(|s| s.lines().count() == 1)
                    .unwrap_or(false);
                is_running && started_once
            }
        )
        .await,
        "t_task must be Running on w1 and have written its first started line"
    );

    // Stop master1 and start master2 with a 2 s adoption hold.
    master1.shutdown().expect("shutdown master1");
    drop(master1);

    let config2 = ServerConfig::new(master_addr)
        .with_web_ui(false)
        .with_discovery(false)
        .with_heartbeat_interval(1)
        .with_state_dir(&state_dir)
        .with_session_resume_hold(Duration::from_secs(2));
    let master2 = spawn_rebound_master(config2, Duration::from_secs(5)).await;

    // Wait 1.3 s: w1's 1.0 s grace has expired (killing the first subprocess before 2.2 s), while
    // master2's 2.0 s hold has not expired yet.
    tokio::time::sleep(Duration::from_millis(1_300)).await;
    assert!(
        !completed_marker.exists(),
        "w1 must have killed its local task when its 1 s grace expired"
    );

    // Connect w2 to master2; once master2's 2 s hold expires, w2 picks up `t_task` and finishes it.
    let (w2_shutdown_tx, w2_shutdown_rx) = watch::channel(false);
    let mut w2 = WorkerClient::for_testing(
        master_addr.to_string(),
        Some("expired-w2".into()),
        Some(1),
        false,
    );
    let w2_handle = tokio::spawn(async move {
        let _ = w2.run(w2_shutdown_rx).await;
    });

    let res = master2
        .wait_task(t_task_id, Some(Duration::from_secs(15)))
        .await
        .expect("t_task completion after hold expiry");
    assert!(res.is_success(), "t_task failed: {res:?}");

    let completed_contents =
        std::fs::read_to_string(&completed_marker).expect("read completed_marker");
    assert_eq!(
        completed_contents.lines().count(),
        1,
        "t_task must have completed exactly once (no duplicate completion from w1)"
    );
    let started_contents = std::fs::read_to_string(&started_marker).expect("read started_marker");
    assert_eq!(
        started_contents.lines().count(),
        2,
        "first attempt started on w1 (killed on grace expiry) and second attempt ran on w2 after hold expiry"
    );

    let _ = w1_shutdown_tx.send(true);
    let _ = w2_shutdown_tx.send(true);
    w1_handle.abort();
    w2_handle.abort();
    master2.shutdown().expect("shutdown master2");
}

/// Reviewer Probe P3b (`158-fix` H2): a task that finishes right as the master shuts down (after
/// `close_and_compact_store_sync`, or while its `TaskResult` is in flight on the dying socket)
/// retains its result in `pending_resume_outbox` until the master acknowledges it, so the
/// restarted master adopts and records the result and the task runs **exactly once**.
#[tokio::test]
async fn rv158_probe_result_during_master_shutdown_is_lost_and_task_reruns() {
    let mut twice = 0usize;
    let rounds = 12u64;
    for round in 0..rounds {
        let dir = tempfile::tempdir().expect("tempdir");
        let state_dir = dir.path().join("master-state");
        let marker_path = dir.path().join("p3.marker");
        let gate_path = dir.path().join("p3.gate");
        let done_path = dir.path().join("p3.done");
        let config1 = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_web_ui(false)
            .with_discovery(false)
            .with_state_dir(&state_dir);
        let master1 = MasterServer::spawn(config1).await.expect("spawn master1");
        let master_addr = master1.server_addr();
        let (w_tx, w_rx) = watch::channel(false);
        let mut w1 = WorkerClient::for_testing(
            master_addr.to_string(),
            Some(format!("p3-{round}")),
            Some(1),
            false,
        );
        w1.set_backoff_config(rusty_grid_worker::backoff::BackoffConfig {
            initial: Duration::from_millis(80),
            max: Duration::from_millis(200),
            factor: 1.5,
            jitter_ratio: 0.0,
        });
        let wh = tokio::spawn(async move {
            let _ = w1.run(w_rx).await;
        });
        assert!(
            wait_for(
                Duration::from_secs(5),
                Duration::from_millis(25),
                || async {
                    master1
                        .list_workers()
                        .await
                        .map(|w| w.len() == 1)
                        .unwrap_or(false)
                }
            )
            .await
        );
        let t = gated_marker_task(&marker_path, &gate_path, Some(&done_path), None, Some(2));
        let tid = master1.submit_task(t).await.expect("submit");
        assert!(
            wait_for(
                Duration::from_secs(5),
                Duration::from_millis(10),
                || async {
                    let running = master1
                        .get_task_info(tid)
                        .await
                        .map(|i| i.state == TaskState::Running)
                        .unwrap_or(false);
                    running
                        && std::fs::read_to_string(&marker_path)
                            .map(|s| s.lines().count() == 1)
                            .unwrap_or(false)
                }
            )
            .await
        );
        std::fs::write(&gate_path, b"go").unwrap();
        while !done_path.exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        tokio::time::sleep(Duration::from_millis(round % 6)).await;
        master1.shutdown().expect("shutdown master1");
        drop(master1);
        assert!(
            wait_for(
                Duration::from_secs(5),
                Duration::from_millis(10),
                || async { done_path.exists() }
            )
            .await
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        let config2 = ServerConfig::new(master_addr)
            .with_web_ui(false)
            .with_discovery(false)
            .with_state_dir(&state_dir);
        let master2 = spawn_rebound_master(config2, Duration::from_secs(5)).await;
        let res = master2
            .wait_task(tid, Some(Duration::from_secs(10)))
            .await
            .expect("task completion on master2");
        assert!(res.is_success(), "round {round} task failed: {res:?}");
        tokio::time::sleep(Duration::from_millis(300)).await;
        let runs = std::fs::read_to_string(&marker_path)
            .map(|s| s.lines().count())
            .unwrap_or(0);
        let info = master2.get_task_info(tid).await.expect("task info");
        eprintln!(
            "RV158 P3 round={round} delay_ms={} runs={runs} state={:?} retry={} failover={}",
            round % 6,
            info.state,
            info.retry_count,
            info.failover_count
        );
        if runs > 1 {
            twice += 1;
        }
        let _ = w_tx.send(true);
        wh.abort();
        master2.shutdown().expect("shutdown master2");
    }
    eprintln!("RV158 P3: rounds={rounds} ran_twice={twice}");
    assert_eq!(twice, 0, "task ran twice in {twice}/{rounds} rounds");
}

/// `158-fix` M3 (live connection drop while master stays up):
/// A worker connected through a TCP relay has its connection severed while the master stays up
/// and two tasks (one normal, one `max_retries = 0`) are `Running`. The master places both tasks
/// in `adoptable_by` hold, the worker reconnects within the hold and sends `ResumeSession`, and
/// both tasks finish with 1 execution and 0 retries/failovers.
#[tokio::test]
async fn test_worker_connection_drop_adopts_running_task_while_master_stays_up() {
    let dir = tempfile::tempdir().expect("tempdir");
    let marker_normal = dir.path().join("drop_normal.marker");
    let marker_zero = dir.path().join("drop_zero.marker");
    let gate_path = dir.path().join("drop.gate");

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(false)
        .with_discovery(false)
        .with_session_resume_hold(Duration::from_secs(30));
    let master = MasterServer::spawn(config).await.expect("spawn master");
    let master_addr = master.server_addr();

    // TCP relay that can sever the active connection on demand while accepting subsequent reconnects.
    let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind proxy");
    let proxy_addr = proxy_listener.local_addr().expect("proxy addr");
    let (sever_tx, sever_rx) = watch::channel(0u64);
    let proxy_task = tokio::spawn(async move {
        loop {
            let Ok((mut client_sock, _)) = proxy_listener.accept().await else {
                break;
            };
            let Ok(mut upstream_sock) = tokio::net::TcpStream::connect(master_addr).await else {
                continue;
            };
            let mut conn_sever_rx = sever_rx.clone();
            let gen_at_start = *conn_sever_rx.borrow_and_update();
            tokio::spawn(async move {
                tokio::select! {
                    _ = tokio::io::copy_bidirectional(&mut client_sock, &mut upstream_sock) => {}
                    _ = conn_sever_rx.changed() => {
                        if *conn_sever_rx.borrow() != gen_at_start {
                            // Sever both TCP legs immediately.
                        }
                    }
                }
            });
        }
    });

    let (w_shutdown_tx, w_shutdown_rx) = watch::channel(false);
    let mut w1 = WorkerClient::for_testing(
        proxy_addr.to_string(),
        Some("drop-live-w1".into()),
        Some(2),
        false,
    );
    w1.set_backoff_config(rusty_grid_worker::backoff::BackoffConfig {
        initial: Duration::from_millis(80),
        max: Duration::from_millis(200),
        factor: 1.5,
        jitter_ratio: 0.0,
    });
    let w1_id = w1.worker_id();
    let w1_handle = tokio::spawn(async move {
        let _ = w1.run(w_shutdown_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(25),
            || async {
                master
                    .list_workers()
                    .await
                    .map(|w| w.len() == 1)
                    .unwrap_or(false)
            }
        )
        .await,
        "worker must register through proxy"
    );

    let t_normal = gated_marker_task(&marker_normal, &gate_path, None, None, Some(2));
    let t_zero = gated_marker_task(&marker_zero, &gate_path, None, None, Some(0));
    let t_normal_id = master.submit_task(t_normal).await.expect("submit normal");
    let t_zero_id = master.submit_task(t_zero).await.expect("submit zero");

    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(25),
            || async {
                let n_ok = master
                    .get_task_info(t_normal_id)
                    .await
                    .map(|i| i.state == TaskState::Running)
                    .unwrap_or(false)
                    && std::fs::read_to_string(&marker_normal)
                        .map(|s| s.lines().count() == 1)
                        .unwrap_or(false);
                let z_ok = master
                    .get_task_info(t_zero_id)
                    .await
                    .map(|i| i.state == TaskState::Running)
                    .unwrap_or(false)
                    && std::fs::read_to_string(&marker_zero)
                        .map(|s| s.lines().count() == 1)
                        .unwrap_or(false);
                n_ok && z_ok
            }
        )
        .await,
        "both normal and max_retries=0 tasks must be Running before connection drop"
    );
    let session_before = master.list_workers().await.unwrap()[0].session_id;

    // Sever the TCP connection while the master stays up.
    let _ = sever_tx.send(1);

    // Wait for the worker to reconnect on a new session_id and re-adopt both tasks into Running.
    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(25),
            || async {
                let reconnected = master
                    .list_workers()
                    .await
                    .map(|ws| {
                        ws.iter().any(|w| {
                            w.worker_id == w1_id
                                && w.session_id > session_before
                                && w.active_tasks == 2
                        })
                    })
                    .unwrap_or(false);
                let both_running = master
                    .get_task_info(t_normal_id)
                    .await
                    .map(|i| i.state == TaskState::Running)
                    .unwrap_or(false)
                    && master
                        .get_task_info(t_zero_id)
                        .await
                        .map(|i| i.state == TaskState::Running)
                        .unwrap_or(false);
                reconnected && both_running
            }
        )
        .await,
        "worker must reconnect on a new session and re-adopt both tasks"
    );

    std::fs::write(&gate_path, b"go").expect("write gate");

    for tid in [t_normal_id, t_zero_id] {
        let res = master
            .wait_task(tid, Some(Duration::from_secs(10)))
            .await
            .expect("task completion after live reconnect");
        assert!(res.is_success(), "task {tid} failed: {res:?}");
        let info = master.get_task_info(tid).await.expect("task info");
        assert_eq!(info.retry_count, 0);
        assert_eq!(info.failover_count, 0);
    }

    assert_eq!(
        std::fs::read_to_string(&marker_normal)
            .unwrap()
            .lines()
            .count(),
        1
    );
    assert_eq!(
        std::fs::read_to_string(&marker_zero)
            .unwrap()
            .lines()
            .count(),
        1
    );

    let _ = w_shutdown_tx.send(true);
    w1_handle.abort();
    proxy_task.abort();
    master.shutdown().expect("shutdown master");
}

/// Helper entrypoint invoked as a child process by
/// `test_master_sigkill_restart_adopts_running_normal_and_zero_retry_tasks_once` so the parent test
/// can `SIGKILL` (`Child::kill()`) a real master process while tasks are `Running`, ensuring
/// `close_and_compact_store_sync` never runs and the restarted master replays from `journal.bin`.
#[tokio::test]
async fn sigkill_child_master_entrypoint() {
    let Ok(state_dir) = std::env::var("OXIDESWARM_TEST_SIGKILL_MASTER_STATE_DIR") else {
        return;
    };
    let port_file =
        std::env::var("OXIDESWARM_TEST_SIGKILL_MASTER_PORT_FILE").expect("PORT_FILE env");
    let ready_file =
        std::env::var("OXIDESWARM_TEST_SIGKILL_MASTER_READY_FILE").expect("READY_FILE env");
    let marker_normal = std::path::PathBuf::from(
        std::env::var("OXIDESWARM_TEST_SIGKILL_MARKER_NORMAL").expect("MARKER_NORMAL env"),
    );
    let marker_zero = std::path::PathBuf::from(
        std::env::var("OXIDESWARM_TEST_SIGKILL_MARKER_ZERO").expect("MARKER_ZERO env"),
    );
    let gate_path =
        std::path::PathBuf::from(std::env::var("OXIDESWARM_TEST_SIGKILL_GATE").expect("GATE env"));

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(false)
        .with_discovery(false)
        .with_state_dir(&state_dir);
    let master = MasterServer::spawn(config)
        .await
        .expect("spawn child master");
    std::fs::write(&port_file, master.server_addr().to_string()).expect("write port_file");

    let t_normal_id = master
        .submit_task(gated_marker_task(
            &marker_normal,
            &gate_path,
            None,
            None,
            Some(2),
        ))
        .await
        .expect("submit normal task");
    let t_zero_id = master
        .submit_task(gated_marker_task(
            &marker_zero,
            &gate_path,
            None,
            None,
            Some(0),
        ))
        .await
        .expect("submit zero-retry task");

    while master
        .get_task_info(t_normal_id)
        .await
        .map(|i| i.state != TaskState::Running)
        .unwrap_or(true)
        || master
            .get_task_info(t_zero_id)
            .await
            .map(|i| i.state != TaskState::Running)
            .unwrap_or(true)
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Ensure `TaskRunning` journal records are flushed before signalling parent.
    tokio::time::sleep(Duration::from_millis(25)).await;
    std::fs::write(&ready_file, format!("{t_normal_id}\n{t_zero_id}\n")).expect("write ready_file");
    std::future::pending::<()>().await;
}

fn locate_rusty_grid_bin() -> Option<std::path::PathBuf> {
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_rusty-grid") {
        let pb = std::path::PathBuf::from(p);
        if pb.exists() {
            return Some(pb);
        }
    }
    let exe = std::env::current_exe().ok()?;
    let target_dir = exe.parent()?.parent()?;
    let bin = target_dir.join(format!("rusty-grid{}", std::env::consts::EXE_SUFFIX));
    bin.exists().then_some(bin)
}

async fn submit_task_via_tcp(
    master_addr: std::net::SocketAddr,
    task: Task,
) -> rusty_grid_core::task::TaskId {
    use rusty_grid_core::protocol::{ClientMessage, ClientResponse, MessageTransport};
    let stream = tokio::net::TcpStream::connect(master_addr)
        .await
        .expect("connect to master for submit");
    let mut transport = MessageTransport::new(stream);
    transport
        .send_msg(&ClientMessage::SubmitTask { task, wait: false })
        .await
        .expect("send SubmitTask");
    match transport
        .recv_msg::<ClientResponse>()
        .await
        .expect("recv SubmitTask response")
    {
        Some(ClientResponse::TaskSubmitted { task_id }) => task_id,
        other => panic!("expected TaskSubmitted, got {other:?}"),
    }
}

async fn is_task_running_via_tcp(
    master_addr: std::net::SocketAddr,
    task_id: rusty_grid_core::task::TaskId,
) -> bool {
    use rusty_grid_core::protocol::{ClientMessage, ClientResponse, MessageTransport};
    let Ok(stream) = tokio::net::TcpStream::connect(master_addr).await else {
        return false;
    };
    let mut transport = MessageTransport::new(stream);
    if transport
        .send_msg(&ClientMessage::GetTaskStatus { task_id })
        .await
        .is_err()
    {
        return false;
    }
    matches!(
        transport.recv_msg::<ClientResponse>().await,
        Ok(Some(ClientResponse::TaskStatusInfo { ref state_name, .. })) if state_name == "Running"
    )
}

/// `158-fix` M3 (unclean `SIGKILL` master crash with uncompacted journal replay):
/// Spawns the `rusty-grid` CLI master process on `--state-dir` (falling back to the test-binary
/// child entrypoint only when `rusty-grid` has not been built in the target directory yet), runs
/// both a normal task and a `max_retries = 0` task on a 2-slot worker, kills the child master with
/// `Child::kill()` (`SIGKILL`) so `close_and_compact_store_sync` does not run, starts `master2` on
/// the same `--state-dir` (replaying `journal.bin` directly), and verifies both tasks are adopted
/// and finish with 1 run and 0 retries/failovers.
#[tokio::test]
async fn test_master_sigkill_restart_adopts_running_normal_and_zero_retry_tasks_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let state_dir = dir.path().join("master-state");
    let home_dir = dir.path().join("home");
    std::fs::create_dir_all(&home_dir).expect("create home_dir");
    let port_file = dir.path().join("master.addr");
    let ready_file = dir.path().join("master.ready");
    let marker_normal = dir.path().join("sigkill_normal.marker");
    let marker_zero = dir.path().join("sigkill_zero.marker");
    let gate_path = dir.path().join("sigkill.gate");

    let cli_bin = locate_rusty_grid_bin();
    let mut child = if let Some(ref bin) = cli_bin {
        std::process::Command::new(bin)
            .args([
                "master",
                "--listen",
                "127.0.0.1:0",
                "--port-file",
                port_file.to_str().unwrap(),
                "--dashboard-port",
                "0",
                "--state-dir",
                state_dir.to_str().unwrap(),
                "--session-resume-hold-secs",
                "30",
            ])
            .env("HOME", &home_dir)
            .env("USERPROFILE", &home_dir)
            .env_remove("OXIDESWARM_SECRET")
            .env_remove("RUSTY_GRID_SECRET")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn rusty-grid master process")
    } else {
        std::process::Command::new(std::env::current_exe().expect("current_exe"))
            .arg("--exact")
            .arg("sigkill_child_master_entrypoint")
            .arg("--nocapture")
            .env("OXIDESWARM_TEST_SIGKILL_MASTER_STATE_DIR", &state_dir)
            .env("OXIDESWARM_TEST_SIGKILL_MASTER_PORT_FILE", &port_file)
            .env("OXIDESWARM_TEST_SIGKILL_MASTER_READY_FILE", &ready_file)
            .env("OXIDESWARM_TEST_SIGKILL_MARKER_NORMAL", &marker_normal)
            .env("OXIDESWARM_TEST_SIGKILL_MARKER_ZERO", &marker_zero)
            .env("OXIDESWARM_TEST_SIGKILL_GATE", &gate_path)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn child master process")
    };

    assert!(
        wait_for(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                std::fs::read_to_string(&port_file)
                    .ok()
                    .and_then(|s| {
                        let trimmed = s.trim();
                        trimmed.parse::<std::net::SocketAddr>().ok().or_else(|| {
                            trimmed
                                .parse::<u16>()
                                .ok()
                                .map(|p| std::net::SocketAddr::from(([127, 0, 0, 1], p)))
                        })
                    })
                    .is_some()
            }
        )
        .await,
        "child master process must write its bound address/port"
    );
    let port_str = std::fs::read_to_string(&port_file).unwrap();
    let trimmed = port_str.trim();
    let master_addr: std::net::SocketAddr = trimmed.parse().unwrap_or_else(|_| {
        let port: u16 = trimmed.parse().expect("parse port");
        std::net::SocketAddr::from(([127, 0, 0, 1], port))
    });

    let (w_shutdown_tx, w_shutdown_rx) = watch::channel(false);
    let mut w1 = WorkerClient::for_testing(
        master_addr.to_string(),
        Some("sigkill-w1".into()),
        Some(2),
        false,
    );
    w1.set_backoff_config(rusty_grid_worker::backoff::BackoffConfig {
        initial: Duration::from_millis(80),
        max: Duration::from_millis(200),
        factor: 1.5,
        jitter_ratio: 0.0,
    });
    let w1_id = w1.worker_id();
    let w1_handle = tokio::spawn(async move {
        let _ = w1.run(w_shutdown_rx).await;
    });

    let (t_normal_id, t_zero_id) = if cli_bin.is_some() {
        let tn = submit_task_via_tcp(
            master_addr,
            gated_marker_task(&marker_normal, &gate_path, None, None, Some(2)),
        )
        .await;
        let tz = submit_task_via_tcp(
            master_addr,
            gated_marker_task(&marker_zero, &gate_path, None, None, Some(0)),
        )
        .await;
        assert!(
            wait_for(
                Duration::from_secs(10),
                Duration::from_millis(25),
                || async {
                    is_task_running_via_tcp(master_addr, tn).await
                        && is_task_running_via_tcp(master_addr, tz).await
                        && std::fs::read_to_string(&marker_normal)
                            .map(|s| s.lines().count() == 1)
                            .unwrap_or(false)
                        && std::fs::read_to_string(&marker_zero)
                            .map(|s| s.lines().count() == 1)
                            .unwrap_or(false)
                }
            )
            .await,
            "both normal and max_retries=0 tasks must start running on rusty-grid CLI master before SIGKILL"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
        (tn, tz)
    } else {
        assert!(
            wait_for(
                Duration::from_secs(10),
                Duration::from_millis(25),
                || async {
                    ready_file.exists()
                        && std::fs::read_to_string(&marker_normal)
                            .map(|s| s.lines().count() == 1)
                            .unwrap_or(false)
                        && std::fs::read_to_string(&marker_zero)
                            .map(|s| s.lines().count() == 1)
                            .unwrap_or(false)
                }
            )
            .await,
            "both normal and max_retries=0 tasks must start running before SIGKILL"
        );
        let ready_text = std::fs::read_to_string(&ready_file).expect("read ready_file");
        let mut lines = ready_text.lines();
        let tn: rusty_grid_core::task::TaskId =
            lines.next().unwrap().parse().expect("parse t_normal_id");
        let tz: rusty_grid_core::task::TaskId =
            lines.next().unwrap().parse().expect("parse t_zero_id");
        (tn, tz)
    };

    // Uncleanly kill (`SIGKILL`) the child master process so `close_and_compact_store_sync` never runs.
    let _ = child.kill();
    let _ = child.wait();

    let journal_len = std::fs::metadata(state_dir.join("journal.bin"))
        .expect("journal.bin metadata")
        .len();
    assert!(
        journal_len > 24,
        "journal.bin must hold uncompacted records after SIGKILL (len={journal_len})"
    );

    let config2 = ServerConfig::new(master_addr)
        .with_web_ui(false)
        .with_discovery(false)
        .with_state_dir(&state_dir);
    let master2 = spawn_rebound_master(config2, Duration::from_secs(5)).await;

    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(25),
            || async {
                let n_adopted = master2
                    .get_task_info(t_normal_id)
                    .await
                    .map(|i| i.state == TaskState::Running && i.assigned_worker_id == Some(w1_id))
                    .unwrap_or(false);
                let z_adopted = master2
                    .get_task_info(t_zero_id)
                    .await
                    .map(|i| i.state == TaskState::Running && i.assigned_worker_id == Some(w1_id))
                    .unwrap_or(false);
                n_adopted && z_adopted
            }
        )
        .await,
        "w1 must reconnect to master2 after SIGKILL and adopt both normal and max_retries=0 tasks"
    );

    std::fs::write(&gate_path, b"go").expect("write gate");

    for tid in [t_normal_id, t_zero_id] {
        let res = master2
            .wait_task(tid, Some(Duration::from_secs(10)))
            .await
            .expect("task completion after SIGKILL restart");
        assert!(res.is_success(), "task {tid} failed: {res:?}");
        let info = master2.get_task_info(tid).await.expect("task info");
        assert_eq!(info.retry_count, 0);
        assert_eq!(info.failover_count, 0);
    }

    assert_eq!(
        std::fs::read_to_string(&marker_normal)
            .unwrap()
            .lines()
            .count(),
        1
    );
    assert_eq!(
        std::fs::read_to_string(&marker_zero)
            .unwrap()
            .lines()
            .count(),
        1
    );

    let _ = w_shutdown_tx.send(true);
    w1_handle.abort();
    master2.shutdown().expect("shutdown master2");
}
