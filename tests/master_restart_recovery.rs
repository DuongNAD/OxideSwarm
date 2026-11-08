//! End-to-end integration tests for master task store durability and startup replay
//! (`docs/design/0046-master-durability.md` D1–D3).

use std::time::{Duration, Instant};

use tokio::sync::watch;

use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_master::queue::TaskState;
use rusty_grid_master::server::{MasterServer, ServerConfig};
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

    // Start master2 on the same `--state-dir`.
    let config2 = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(false)
        .with_discovery(false)
        .with_state_dir(&state_dir);
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
