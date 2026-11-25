//! E2E integration tests for FEAT-5: Process task checkpoints (`OXCP1`, `$OXIDESWARM_CHECKPOINT_DIR`).
//!
//! Exercises the full Master <-> Worker Protocol 10 lifecycle:
//! 1. `Command` task periodic `OXCP1` checkpoint emission, abrupt worker drop, and resume on a second worker.
//! 2. `ShellScript` task checkpoint emission/resume and `--output '**/*'` checkpoint exclusion.
//! 3. `Workspace` task checkpoint emission/resume inside `<ws>/tmp/.oxideswarm_checkpoint` with `--output '**/*'`.
//! 4. macOS `--isolation fs` (`sandbox-exec` Seatbelt) writable `$OXIDESWARM_CHECKPOINT_DIR` across failover.
//! 5. Graceful worker shutdown (`SIGTERM` / `shutdown_tx`) draining active task and flushing final checkpoint.

#![cfg(unix)]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::sync::watch;

use rusty_grid_core::blob::{BlobFile, TreeManifest};
#[cfg(target_os = "macos")]
use rusty_grid_core::placement::IsolationLevel;
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec, WorkspaceRef, WorkspaceSource};
use rusty_grid_master::queue::TaskState;
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{MasterServer, ServerConfig, DEFAULT_MASTER_MAX_BATCH_SIZE};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};
#[cfg(target_os = "macos")]
use rusty_grid_worker::isolation::verify_isolation_support;

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

fn base_worker_config(
    master_addr: &str,
    name: &str,
    sandbox_base: &std::path::Path,
) -> WorkerConfig {
    WorkerConfig::new(master_addr)
        .with_name(name)
        .with_cores(2)
        .with_ram_mb(4096)
        .with_no_battery(true)
        .with_no_gpu(true)
        .with_no_blender(true)
        .with_no_cuda(true)
        .with_redirection_portal(false)
        .with_sandbox_base_dir(sandbox_base)
}

#[tokio::test]
async fn test_feat5_command_periodic_checkpoint_abort_and_resume() {
    let tmp = tempfile::tempdir().unwrap();
    let w1_sb = tmp.path().join("w1_sb");
    let w2_sb = tmp.path().join("w2_sb");
    let step1_marker = tmp.path().join("step1.count");

    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(3),
        SchedulerConfig::default()
            .with_max_batch_size(DEFAULT_MASTER_MAX_BATCH_SIZE)
            .without_host_cpu_backpressure(),
        ReaperConfig::default(),
    )
    .await
    .expect("master spawn");
    let master_addr = master.server_addr().to_string();

    let (_w1_tx, w1_rx) = watch::channel(false);
    let mut w1 = WorkerClient::new(base_worker_config(&master_addr, "f5-cmd-w1", &w1_sb));
    let w1_id = w1.worker_id();
    let w1_handle = tokio::spawn(async move {
        let _ = w1.run(w1_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                master
                    .list_workers()
                    .await
                    .map(|ws| ws.iter().any(|w| w.worker_id == w1_id))
                    .unwrap_or(false)
            }
        )
        .await
    );

    let script = format!(
        r#"
set -e
if [ -f "$OXIDESWARM_CHECKPOINT_DIR/step1.done" ]; then
  echo "SKIPPED_STEP1"
  cat "$OXIDESWARM_CHECKPOINT_DIR/step1.done"
  echo "STEP2_COMPLETE"
  exit 0
fi
echo "1" >> '{}'
echo "checkpoint_state_v1" > "$OXIDESWARM_CHECKPOINT_DIR/step1.done"
sleep 15
"#,
        step1_marker.display()
    );
    let task = Task::new(
        TaskSpec::command("sh", vec!["-c".to_string(), script]),
        TaskRequirements::generic(1, 30),
    );
    let task_id = master.submit_task(task).await.expect("submit");

    // Wait for periodic OXCP1 checkpoint to arrive on Master.
    assert!(
        wait_for(
            Duration::from_secs(10),
            Duration::from_millis(50),
            || async {
                master.get_checkpoint(&task_id).await.is_some_and(|cp| {
                    cp.sequence >= 1 && cp.delta_state.as_slice().starts_with(b"OXCP1\x01")
                })
            }
        )
        .await,
        "Master must receive periodic OXCP1 checkpoint from Worker 1"
    );

    // Abruptly kill Worker 1.
    w1_handle.abort();

    assert!(
        wait_for(
            Duration::from_secs(10),
            Duration::from_millis(30),
            || async {
                master
                    .get_task_info(task_id)
                    .await
                    .map(|i| i.state == TaskState::Queued && i.assigned_worker_id.is_none())
                    .unwrap_or(false)
            }
        )
        .await
    );

    // Connect Worker 2 to resume.
    let (w2_tx, w2_rx) = watch::channel(false);
    let mut w2 = WorkerClient::new(base_worker_config(&master_addr, "f5-cmd-w2", &w2_sb));
    let w2_handle = tokio::spawn(async move {
        let _ = w2.run(w2_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(15),
            Duration::from_millis(50),
            || async {
                master
                    .get_task_info(task_id)
                    .await
                    .map(|i| i.state == TaskState::Completed)
                    .unwrap_or(false)
            }
        )
        .await
    );

    let result = master
        .wait_task(task_id, Some(Duration::from_secs(2)))
        .await
        .expect("result");
    assert!(result.is_success(), "{:?}", result.error);
    let stdout = result.stdout_str();
    assert!(stdout.contains("SKIPPED_STEP1"), "stdout={stdout}");
    assert!(stdout.contains("checkpoint_state_v1"), "stdout={stdout}");
    assert!(stdout.contains("STEP2_COMPLETE"), "stdout={stdout}");
    let lines = std::fs::read_to_string(&step1_marker).unwrap();
    assert_eq!(lines.lines().count(), 1, "step 1 must run only once");

    let _ = w2_tx.send(true);
    let _ = w2_handle.await;
    let _ = master.shutdown();
}

#[tokio::test]
async fn test_feat5_shell_script_checkpoint_resume_and_output_exclusion() {
    let tmp = tempfile::tempdir().unwrap();
    let blob_dir = tmp.path().join("blobs");
    let w1_sb = tmp.path().join("w1_sb");
    let w2_sb = tmp.path().join("w2_sb");

    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_max_retries(3)
            .with_blob_store(&blob_dir, 64 * 1024 * 1024),
        SchedulerConfig::default()
            .with_max_batch_size(DEFAULT_MASTER_MAX_BATCH_SIZE)
            .without_host_cpu_backpressure(),
        ReaperConfig::default(),
    )
    .await
    .expect("master spawn");
    let master_addr = master.server_addr().to_string();

    let (_w1_tx, w1_rx) = watch::channel(false);
    let mut w1 = WorkerClient::new(base_worker_config(&master_addr, "f5-sh-w1", &w1_sb));
    let w1_id = w1.worker_id();
    let w1_handle = tokio::spawn(async move {
        let _ = w1.run(w1_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                master
                    .list_workers()
                    .await
                    .map(|ws| ws.iter().any(|w| w.worker_id == w1_id))
                    .unwrap_or(false)
            }
        )
        .await
    );

    let script = r#"
set -e
if [ -f "$OXIDESWARM_CHECKPOINT_DIR/nested/phase1.dat" ]; then
  echo "resumed=true" > result.txt
  cat "$OXIDESWARM_CHECKPOINT_DIR/nested/phase1.dat" >> result.txt
  exit 0
fi
mkdir -p "$OXIDESWARM_CHECKPOINT_DIR/nested"
echo "phase1_data" > "$OXIDESWARM_CHECKPOINT_DIR/nested/phase1.dat"
sleep 15
"#;
    let mut task = Task::new(
        TaskSpec::shell_script(script),
        TaskRequirements::generic(1, 30),
    );
    task.output_globs = vec!["**/*".to_string()];
    let task_id = master.submit_task(task).await.expect("submit");

    assert!(
        wait_for(
            Duration::from_secs(10),
            Duration::from_millis(50),
            || async { master.get_checkpoint(&task_id).await.is_some() }
        )
        .await
    );

    w1_handle.abort();

    assert!(
        wait_for(
            Duration::from_secs(10),
            Duration::from_millis(30),
            || async {
                master
                    .get_task_info(task_id)
                    .await
                    .map(|i| i.state == TaskState::Queued && i.assigned_worker_id.is_none())
                    .unwrap_or(false)
            }
        )
        .await
    );

    let (w2_tx, w2_rx) = watch::channel(false);
    let mut w2 = WorkerClient::new(base_worker_config(&master_addr, "f5-sh-w2", &w2_sb));
    let w2_handle = tokio::spawn(async move {
        let _ = w2.run(w2_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(15),
            Duration::from_millis(50),
            || async {
                master
                    .get_task_info(task_id)
                    .await
                    .map(|i| i.state == TaskState::Completed)
                    .unwrap_or(false)
            }
        )
        .await
    );

    let result = master
        .wait_task(task_id, Some(Duration::from_secs(2)))
        .await
        .expect("result");
    assert!(result.is_success(), "{:?}", result.error);
    let paths: Vec<&str> = result.outputs.iter().map(|o| o.path.as_str()).collect();
    assert!(
        paths.contains(&"result.txt"),
        "expected result.txt in outputs: {paths:?}"
    );
    assert!(
        !paths.iter().any(|p| p.contains(".oxideswarm_checkpoint")),
        "--output '**/*' must exclude .oxideswarm_checkpoint: {paths:?}"
    );

    let _ = w2_tx.send(true);
    let _ = w2_handle.await;
    let _ = master.shutdown();
}

#[tokio::test]
async fn test_feat5_workspace_checkpoint_resume_and_output_exclusion() {
    let tmp = tempfile::tempdir().unwrap();
    let blob_dir = tmp.path().join("blobs");
    let w1_sb = tmp.path().join("w1_sb");
    let w2_sb = tmp.path().join("w2_sb");
    let w1_ws = tmp.path().join("w1_ws");
    let w2_ws = tmp.path().join("w2_ws");

    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_max_retries(3)
            .with_blob_store(&blob_dir, 64 * 1024 * 1024),
        SchedulerConfig::default()
            .with_max_batch_size(DEFAULT_MASTER_MAX_BATCH_SIZE)
            .without_host_cpu_backpressure(),
        ReaperConfig::default(),
    )
    .await
    .expect("master spawn");
    let master_addr = master.server_addr().to_string();

    let store = master.blob_store().expect("blob store");
    let seed_bytes = b"seed_input\n";
    let seed_id = store.put(seed_bytes).await.unwrap();
    let manifest = TreeManifest::new(vec![BlobFile {
        path: "input.txt".to_string(),
        id: seed_id,
        size: seed_bytes.len() as u64,
        executable: false,
    }])
    .unwrap();
    let manifest_bytes = manifest.encode();
    let manifest_id = store.put(&manifest_bytes).await.unwrap();

    let (_w1_tx, w1_rx) = watch::channel(false);
    let mut w1 = WorkerClient::new(
        base_worker_config(&master_addr, "f5-ws-w1", &w1_sb)
            .with_workspaces(true)
            .with_workspace_root(Some(w1_ws)),
    );
    let w1_id = w1.worker_id();
    let w1_handle = tokio::spawn(async move {
        let _ = w1.run(w1_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                master
                    .list_workers()
                    .await
                    .map(|ws| ws.iter().any(|w| w.worker_id == w1_id))
                    .unwrap_or(false)
            }
        )
        .await
    );

    let script = r#"
set -e
test -f input.txt
if [ -f "$OXIDESWARM_CHECKPOINT_DIR/ws_step1.txt" ]; then
  cat "$OXIDESWARM_CHECKPOINT_DIR/ws_step1.txt" > final.txt
  echo "ws_resumed"
  exit 0
fi
echo "ws_cp_payload" > "$OXIDESWARM_CHECKPOINT_DIR/ws_step1.txt"
sleep 15
"#;
    let mut task = Task::new(
        TaskSpec::shell_script(script),
        TaskRequirements::generic(1, 30),
    );
    task.workspace = Some(WorkspaceRef {
        name: "f5-ws".to_string(),
        source: Some(WorkspaceSource::Tree {
            manifest: manifest_id,
            manifest_size: manifest_bytes.len() as u64,
        }),
    });
    task.output_globs = vec!["**/*".to_string()];

    let task_id = master.submit_task(task).await.expect("submit");

    assert!(
        wait_for(
            Duration::from_secs(10),
            Duration::from_millis(50),
            || async { master.get_checkpoint(&task_id).await.is_some() }
        )
        .await
    );

    w1_handle.abort();

    assert!(
        wait_for(
            Duration::from_secs(10),
            Duration::from_millis(30),
            || async {
                master
                    .get_task_info(task_id)
                    .await
                    .map(|i| i.state == TaskState::Queued && i.assigned_worker_id.is_none())
                    .unwrap_or(false)
            }
        )
        .await
    );

    let (w2_tx, w2_rx) = watch::channel(false);
    let mut w2 = WorkerClient::new(
        base_worker_config(&master_addr, "f5-ws-w2", &w2_sb)
            .with_workspaces(true)
            .with_workspace_root(Some(w2_ws)),
    );
    let w2_handle = tokio::spawn(async move {
        let _ = w2.run(w2_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(15),
            Duration::from_millis(50),
            || async {
                master
                    .get_task_info(task_id)
                    .await
                    .map(|i| i.state == TaskState::Completed)
                    .unwrap_or(false)
            }
        )
        .await
    );

    let result = master
        .wait_task(task_id, Some(Duration::from_secs(2)))
        .await
        .expect("result");
    assert!(result.is_success(), "{:?}", result.error);
    assert!(result.stdout_str().contains("ws_resumed"));
    let paths: Vec<&str> = result.outputs.iter().map(|o| o.path.as_str()).collect();
    assert_eq!(paths, ["final.txt", "input.txt"]);

    let _ = w2_tx.send(true);
    let _ = w2_handle.await;
    let _ = master.shutdown();
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn test_feat5_macos_seatbelt_fs_isolation_checkpoint_resume() {
    let tmp = tempfile::tempdir().unwrap();
    let w1_sb = tmp.path().join("w1_sb");
    let w2_sb = tmp.path().join("w2_sb");
    std::fs::create_dir_all(&w1_sb).unwrap();
    if verify_isolation_support(IsolationLevel::Fs, &w1_sb)
        .await
        .is_err()
    {
        return;
    }

    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(3),
        SchedulerConfig::default()
            .with_max_batch_size(DEFAULT_MASTER_MAX_BATCH_SIZE)
            .without_host_cpu_backpressure(),
        ReaperConfig::default(),
    )
    .await
    .expect("master spawn");
    let master_addr = master.server_addr().to_string();

    let (_w1_tx, w1_rx) = watch::channel(false);
    let mut w1 = WorkerClient::new(
        base_worker_config(&master_addr, "f5-iso-w1", &w1_sb).with_isolation(IsolationLevel::Fs),
    );
    let w1_id = w1.worker_id();
    let w1_handle = tokio::spawn(async move {
        let _ = w1.run(w1_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                master
                    .list_workers()
                    .await
                    .map(|ws| ws.iter().any(|w| w.worker_id == w1_id))
                    .unwrap_or(false)
            }
        )
        .await
    );

    let script = r#"
set -e
if [ -f "$OXIDESWARM_CHECKPOINT_DIR/seatbelt.ok" ]; then
  cat "$OXIDESWARM_CHECKPOINT_DIR/seatbelt.ok"
  echo "SEATBELT_RESUMED"
  exit 0
fi
echo "seatbelt_state_1" > "$OXIDESWARM_CHECKPOINT_DIR/seatbelt.ok"
sleep 15
"#;
    let task = Task::new(
        TaskSpec::shell_script(script),
        TaskRequirements::generic(1, 30),
    );
    let task_id = master.submit_task(task).await.expect("submit");

    assert!(
        wait_for(
            Duration::from_secs(10),
            Duration::from_millis(50),
            || async { master.get_checkpoint(&task_id).await.is_some() }
        )
        .await
    );

    w1_handle.abort();

    assert!(
        wait_for(
            Duration::from_secs(10),
            Duration::from_millis(30),
            || async {
                master
                    .get_task_info(task_id)
                    .await
                    .map(|i| i.state == TaskState::Queued && i.assigned_worker_id.is_none())
                    .unwrap_or(false)
            }
        )
        .await
    );

    let (w2_tx, w2_rx) = watch::channel(false);
    let mut w2 = WorkerClient::new(
        base_worker_config(&master_addr, "f5-iso-w2", &w2_sb).with_isolation(IsolationLevel::Fs),
    );
    let w2_handle = tokio::spawn(async move {
        let _ = w2.run(w2_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(15),
            Duration::from_millis(50),
            || async {
                master
                    .get_task_info(task_id)
                    .await
                    .map(|i| i.state == TaskState::Completed)
                    .unwrap_or(false)
            }
        )
        .await
    );

    let result = master
        .wait_task(task_id, Some(Duration::from_secs(2)))
        .await
        .expect("result");
    assert!(
        result.is_success(),
        "{:?} stderr={}",
        result.error,
        result.stderr_str()
    );
    let stdout = result.stdout_str();
    assert!(stdout.contains("seatbelt_state_1"), "stdout={stdout}");
    assert!(stdout.contains("SEATBELT_RESUMED"), "stdout={stdout}");

    let _ = w2_tx.send(true);
    let _ = w2_handle.await;
    let _ = master.shutdown();
}

#[tokio::test]
async fn test_feat5_graceful_worker_shutdown_flushes_final_checkpoint() {
    let tmp = tempfile::tempdir().unwrap();
    let w1_sb = tmp.path().join("w1_sb");
    let w2_sb = tmp.path().join("w2_sb");
    let started_flag = tmp.path().join("started.flag");

    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(3),
        SchedulerConfig::default()
            .with_max_batch_size(DEFAULT_MASTER_MAX_BATCH_SIZE)
            .without_host_cpu_backpressure(),
        ReaperConfig::default(),
    )
    .await
    .expect("master spawn");
    let master_addr = master.server_addr().to_string();

    let (w1_tx, w1_rx) = watch::channel(false);
    let mut w1 = WorkerClient::new(base_worker_config(&master_addr, "f5-drain-w1", &w1_sb));
    let w1_id = w1.worker_id();
    let w1_handle = tokio::spawn(async move {
        let _ = w1.run(w1_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                master
                    .list_workers()
                    .await
                    .map(|ws| ws.iter().any(|w| w.worker_id == w1_id))
                    .unwrap_or(false)
            }
        )
        .await
    );

    // Script writes checkpoint ONLY inside its SIGTERM handler (no periodic checkpoint before SIGTERM!).
    let script = format!(
        r#"
if [ -f "$OXIDESWARM_CHECKPOINT_DIR/graceful.ckpt" ]; then
  cat "$OXIDESWARM_CHECKPOINT_DIR/graceful.ckpt"
  echo "DRAIN_RESUMED"
  exit 0
fi
trap 'echo "flushed_on_sigterm" > "$OXIDESWARM_CHECKPOINT_DIR/graceful.ckpt"; exit 0' TERM
echo "started" > '{}'
while :; do sleep 0.05; done
"#,
        started_flag.display()
    );
    let mut env = HashMap::new();
    env.insert("LC_ALL".to_string(), "C".to_string());
    let task = Task::new(
        TaskSpec::ShellScript {
            script,
            interpreter: None,
            env,
        },
        TaskRequirements::generic(1, 30),
    );
    let task_id = master.submit_task(task).await.expect("submit");

    assert!(
        wait_for(Duration::from_secs(10), Duration::from_millis(20), || {
            let p = started_flag.clone();
            async move { p.exists() }
        })
        .await,
        "task must start on Worker 1"
    );
    assert!(
        master.get_checkpoint(&task_id).await.is_none(),
        "no checkpoint should exist before SIGTERM drain"
    );

    // Initiate graceful worker shutdown -> must drain task, pack final checkpoint, and flush before Disconnecting.
    let _ = w1_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), w1_handle)
        .await
        .expect("worker 1 must finish graceful drain");

    assert!(
        master
            .get_checkpoint(&task_id)
            .await
            .is_some_and(|cp| cp.delta_state.as_slice().starts_with(b"OXCP1\x01")),
        "graceful shutdown must flush final OXCP1 checkpoint to Master before Disconnecting"
    );

    // Connect Worker 2 and verify it restores the flushed checkpoint.
    let (w2_tx, w2_rx) = watch::channel(false);
    let mut w2 = WorkerClient::new(base_worker_config(&master_addr, "f5-drain-w2", &w2_sb));
    let w2_handle = tokio::spawn(async move {
        let _ = w2.run(w2_rx).await;
    });

    assert!(
        wait_for(
            Duration::from_secs(15),
            Duration::from_millis(50),
            || async {
                master
                    .get_task_info(task_id)
                    .await
                    .map(|i| i.state == TaskState::Completed)
                    .unwrap_or(false)
            }
        )
        .await
    );

    let result = master
        .wait_task(task_id, Some(Duration::from_secs(2)))
        .await
        .expect("result");
    assert!(result.is_success(), "{:?}", result.error);
    let stdout = result.stdout_str();
    assert!(stdout.contains("flushed_on_sigterm"), "stdout={stdout}");
    assert!(stdout.contains("DRAIN_RESUMED"), "stdout={stdout}");

    let _ = w2_tx.send(true);
    let _ = w2_handle.await;
    let _ = master.shutdown();
}
