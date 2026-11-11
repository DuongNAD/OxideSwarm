//! Integration tests for `SCHED-4` (`docs/design/0060-speed-aware-placement.md`, D1 & D2):
//! measured worker speed history (`SpeedBook`) and speed-aware task placement.

use std::time::{Duration, Instant};

use futures::future::join_all;
use tempfile::TempDir;
use tokio::sync::watch;
use uuid::Uuid;

use rusty_grid_core::task::{Task, TaskRequirements, TaskResult, TaskSpec};
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::registry::WorkerStatus;
use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

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

fn sleep_spec(duration_ms: u64) -> TaskSpec {
    TaskSpec::BuiltinTest {
        test_name: "sleep".to_string(),
        iterations: 1,
        duration_ms,
        should_fail: false,
        require_gpu: false,
    }
}

struct TwoSpeedCluster {
    master: MasterHandle,
    fast_id: Uuid,
    slow_id: Option<Uuid>,
    fast_shutdown: watch::Sender<bool>,
    slow_shutdown: Option<watch::Sender<bool>>,
    _sandbox_dir: TempDir,
}

impl TwoSpeedCluster {
    async fn spawn(sched_config: SchedulerConfig, include_slow: bool) -> Self {
        let sandbox_dir = TempDir::new().expect("tempdir for worker sandboxes");
        let server_cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap());
        let master =
            MasterServer::spawn_with_config(server_cfg, sched_config, ReaperConfig::default())
                .await
                .expect("MasterServer::spawn_with_config failed");
        let master_addr = master.server_addr().to_string();

        let fast_cfg = WorkerConfig::new(master_addr.clone())
            .with_name("worker-fast")
            .with_cores(4)
            .with_ram_mb(16_384)
            .with_no_gpu(true)
            .with_max_concurrency(4)
            .with_sandbox_base_dir(sandbox_dir.path().join("fast"));
        let mut fast = WorkerClient::new(fast_cfg);
        let fast_id = fast.worker_id();
        let (fast_tx, fast_rx) = watch::channel(false);
        tokio::spawn(async move {
            let _ = fast.run(fast_rx).await;
        });

        let (slow_id, slow_shutdown, expected_workers) = if include_slow {
            let slow_cfg = WorkerConfig::new(master_addr)
                .with_name("worker-slow")
                .with_cores(4)
                .with_ram_mb(16_384)
                .with_no_gpu(true)
                .with_max_concurrency(4)
                .with_sandbox_base_dir(sandbox_dir.path().join("slow"));
            // Test hook: sleep 5x longer per task than the fast worker.
            let mut slow = WorkerClient::new(slow_cfg).with_test_sleep_multiplier(5);
            let sid = slow.worker_id();
            let (slow_tx, slow_rx) = watch::channel(false);
            tokio::spawn(async move {
                let _ = slow.run(slow_rx).await;
            });
            (Some(sid), Some(slow_tx), 2usize)
        } else {
            (None, None, 1usize)
        };

        let ready = wait_for(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                let workers = master.list_workers().await.unwrap_or_default();
                workers.len() == expected_workers
                    && workers.iter().all(|w| w.status == WorkerStatus::Connected)
            },
        )
        .await;
        assert!(ready, "workers failed to register within timeout");

        Self {
            master,
            fast_id,
            slow_id,
            fast_shutdown: fast_tx,
            slow_shutdown,
            _sandbox_dir: sandbox_dir,
        }
    }

    async fn run_batch(&self, count: usize, duration_ms: u64) -> (Duration, Vec<TaskResult>) {
        let spec = sleep_spec(duration_ms);
        let start = Instant::now();
        let mut ids = Vec::with_capacity(count);
        for _ in 0..count {
            let task = Task::new(spec.clone(), TaskRequirements::generic(1, 30));
            let id = self
                .master
                .submit_task(task)
                .await
                .expect("submit_task failed");
            ids.push(id);
        }
        let waits = ids
            .into_iter()
            .map(|id| self.master.wait_task(id, Some(Duration::from_secs(20))));
        let results: Vec<TaskResult> = join_all(waits)
            .await
            .into_iter()
            .map(|r| r.expect("wait_task failed"))
            .collect();
        let elapsed = start.elapsed();
        assert!(results.iter().all(TaskResult::is_success));
        (elapsed, results)
    }

    async fn teardown(self) {
        let _ = self.fast_shutdown.send(true);
        if let Some(tx) = self.slow_shutdown {
            let _ = tx.send(true);
        }
        let _ = self.master.shutdown();
    }
}

#[tokio::test]
async fn sched4_batch_finishes_within_1_5x_fast_worker_and_zero_factor_restores_old_timing() {
    const BASE_SLEEP_MS: u64 = 220;
    const BATCH_TASKS: usize = 8; // 2 waves of 4 slots on the fast worker alone (~440 ms)

    // 1. Measure the fast worker's own time on the 8-task batch (`fast` alone, 4 slots).
    let solo = TwoSpeedCluster::spawn(SchedulerConfig::default(), false).await;
    let (fast_own_time, solo_results) = solo.run_batch(BATCH_TASKS, BASE_SLEEP_MS).await;
    assert_eq!(solo_results.len(), BATCH_TASKS);
    solo.teardown().await;

    // 2. Two in-process workers (`fast` at 1x and `slow` at 5x) under default `SchedulerConfig`.
    //    Run a 4-task unpinned warmup so both workers record >= 2 samples naturally in `SpeedBook`.
    let d2_cluster = TwoSpeedCluster::spawn(SchedulerConfig::default(), true).await;
    let (_, warmup_results) = d2_cluster.run_batch(4, BASE_SLEEP_MS).await;
    let slow_id = d2_cluster.slow_id.expect("slow worker present");
    assert_eq!(
        warmup_results
            .iter()
            .filter(|r| r.worker_id == d2_cluster.fast_id)
            .count(),
        2
    );
    assert_eq!(
        warmup_results
            .iter()
            .filter(|r| r.worker_id == slow_id)
            .count(),
        2
    );

    // Measured batch with D2 active: all 8 tasks stay off the 5x-slower worker and finish within
    // `1.5 * fast_own_time`.
    let (d2_elapsed, d2_results) = d2_cluster.run_batch(BATCH_TASKS, BASE_SLEEP_MS).await;
    let d2_slow_count = d2_results.iter().filter(|r| r.worker_id == slow_id).count();
    d2_cluster.teardown().await;

    assert_eq!(
        d2_slow_count, 0,
        "D2 must hold overflow tasks for the faster worker instead of spilling onto the 5x slower worker"
    );
    let cap = fast_own_time.mul_f64(1.5);
    assert!(
        d2_elapsed <= cap,
        "D2 batch elapsed {d2_elapsed:?} exceeded 1.5x fast worker's own time ({fast_own_time:?}, cap={cap:?})"
    );

    // 3. Same two workers and warmup, but `speed_hold_factor = 0.0`: D2 turns off and the old slow
    //    timing returns (`slow` receives half of Wave 1 and drags the batch to ~5x single-task time).
    let off_cluster =
        TwoSpeedCluster::spawn(SchedulerConfig::default().with_speed_hold_factor(0.0), true).await;
    let _ = off_cluster.run_batch(4, BASE_SLEEP_MS).await;
    let off_slow_id = off_cluster.slow_id.expect("slow worker present");
    let (off_elapsed, off_results) = off_cluster.run_batch(BATCH_TASKS, BASE_SLEEP_MS).await;
    let off_slow_count = off_results
        .iter()
        .filter(|r| r.worker_id == off_slow_id)
        .count();
    off_cluster.teardown().await;

    assert!(
        off_slow_count >= 4,
        "with speed_hold_factor = 0, slow worker should receive Wave 1 tasks (got {off_slow_count})"
    );
    assert!(
        off_elapsed > cap,
        "with speed_hold_factor = 0, old slow timing should return (> {cap:?}, got {off_elapsed:?})"
    );
}
