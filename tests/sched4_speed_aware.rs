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

/// How many times longer the slow worker of `TwoSpeedCluster` sleeps per task. 10 rather than 5
/// (TEST-44): D2 holds an overflow task for a full fast worker whenever the slow average is at least
/// `1.5 * (ready + fast average)`, at worst three fast averages. With 5x, a starved runner that
/// stretched each fast task by more than 220 ms let D2 send a task to the slow worker at once (1 run
/// in 125 on 2 cores next to 4 spinning processes); with 10x the fast tasks must stretch by 770 ms.
const SLOW_SLEEP_MULTIPLIER: u32 = 10;

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
        let master = MasterServer::spawn_with_config(
            server_cfg,
            sched_config.without_host_cpu_backpressure(),
            ReaperConfig::default(),
        )
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
            // Test hook: sleep `SLOW_SLEEP_MULTIPLIER` times longer per task than the fast worker.
            let mut slow =
                WorkerClient::new(slow_cfg).with_test_sleep_multiplier(SLOW_SLEEP_MULTIPLIER);
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

    /// Submits `count` sleep tasks and waits for all of them. Returns the batch time and, per task,
    /// its result and when the test saw it (time since the batch started, never earlier than the
    /// result itself).
    async fn run_batch(
        &self,
        count: usize,
        duration_ms: u64,
    ) -> (Duration, Vec<(TaskResult, Duration)>) {
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
        let waits = ids.into_iter().map(|id| async move {
            let result = self
                .master
                .wait_task(id, Some(Duration::from_secs(20)))
                .await;
            (result, start.elapsed())
        });
        let results: Vec<(TaskResult, Duration)> = join_all(waits)
            .await
            .into_iter()
            .map(|(r, seen)| (r.expect("wait_task failed"), seen))
            .collect();
        let elapsed = start.elapsed();
        assert!(results.iter().all(|(r, _)| r.is_success()));
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

/// TEST-44: what design 0060 D2 decides is placement (the batch stays on the fast worker and the
/// slow one is held back), so that is what this test asserts. The batch times are printed: the old
/// `d2_elapsed <= 1.5 * fast_own_time` and `off_elapsed > 1.5 * fast_own_time` compared wall-clock
/// times of a few hundred milliseconds measured at different moments, which a loaded runner breaks
/// without any change in placement (`766 ms > 697 ms` on `ubuntu-latest`). With `speed_hold_factor = 0`
/// the old timing is asserted by a bound load cannot break: a task on the slow worker sleeps
/// `SLOW_SLEEP_MULTIPLIER * BASE_SLEEP_MS`, and the batch waits for it.
#[tokio::test]
async fn sched4_d2_keeps_the_batch_on_the_fast_worker_and_zero_factor_restores_old_timing() {
    const BASE_SLEEP_MS: u64 = 220;
    const BATCH_TASKS: usize = 8; // 2 waves of 4 slots on the fast worker alone (~440 ms)

    // 1. Measure the fast worker's own time on the 8-task batch (`fast` alone, 4 slots).
    let solo = TwoSpeedCluster::spawn(SchedulerConfig::default(), false).await;
    let (fast_own_time, solo_results) = solo.run_batch(BATCH_TASKS, BASE_SLEEP_MS).await;
    assert_eq!(solo_results.len(), BATCH_TASKS);
    solo.teardown().await;

    // 2. Two in-process workers (`fast` at 1x and `slow` at 10x) under default `SchedulerConfig`.
    //    Run an 8-task concurrent warmup so both workers saturate slots and record >= 2 samples
    //    naturally in `SpeedBook` without depending on single-task CPU telemetry ordering.
    let d2_cluster = TwoSpeedCluster::spawn(SchedulerConfig::default(), true).await;
    let (_, warmup_results) = d2_cluster.run_batch(8, BASE_SLEEP_MS).await;
    let slow_id = d2_cluster.slow_id.expect("slow worker present");
    let fast_warmup = warmup_results
        .iter()
        .filter(|(r, _)| r.worker_id == d2_cluster.fast_id)
        .count();
    let slow_warmup = warmup_results
        .iter()
        .filter(|(r, _)| r.worker_id == slow_id)
        .count();
    assert!(
        fast_warmup >= 2 && slow_warmup >= 2,
        "concurrent warmup must give both workers >= 2 samples (got fast={fast_warmup}, slow={slow_warmup})"
    );

    // Measured batch with D2 active. D2 holds overflow tasks for the fast worker (step 4) and lets a
    // task go to the slow worker only after it has waited `min_expected` (step 5), here the slow
    // worker's measured average, at least `slow_task_time`. So a task that ran on the slow worker
    // after that wait cannot have finished before `2 * slow_task_time` (its wait plus its own slow
    // sleep), however loaded the runner is. D2 would send a task to the slow worker without the wait
    // only if the slow average fell below `1.5 * (ready + fast average)`, which with a 10x sleep
    // needs more than `(SLOW_SLEEP_MULTIPLIER - 3) * BASE_SLEEP_MS / 2` (770 ms) of extra time in
    // every fast task. Without load every task runs on the fast worker.
    let slow_task_time = Duration::from_millis(u64::from(SLOW_SLEEP_MULTIPLIER) * BASE_SLEEP_MS);
    let (d2_elapsed, d2_results) = d2_cluster.run_batch(BATCH_TASKS, BASE_SLEEP_MS).await;
    let d2_fast_id = d2_cluster.fast_id;
    let d2_spilled: Vec<Duration> = d2_results
        .iter()
        .filter(|(r, _)| r.worker_id == slow_id)
        .map(|(_, seen)| *seen)
        .collect();
    let d2_fast_count = d2_results
        .iter()
        .filter(|(r, _)| r.worker_id == d2_fast_id)
        .count();
    d2_cluster.teardown().await;

    assert_eq!(
        d2_fast_count + d2_spilled.len(),
        BATCH_TASKS,
        "every task of the batch must run on one of the two workers"
    );
    for seen in &d2_spilled {
        assert!(
            *seen >= slow_task_time * 2,
            "D2 must hold overflow tasks for the faster worker: a task reached the 10x slower worker \
             and finished {seen:?} after the batch started, before its wait cap plus its own run \
             ({:?})",
            slow_task_time * 2
        );
    }

    // 3. Same two workers and warmup, but `speed_hold_factor = 0.0`: D2 turns off and the old slow
    //    timing returns (`slow` receives Wave 1 work, and the batch waits for its slow tasks).
    let off_cluster =
        TwoSpeedCluster::spawn(SchedulerConfig::default().with_speed_hold_factor(0.0), true).await;
    let _ = off_cluster.run_batch(8, BASE_SLEEP_MS).await;
    let off_slow_id = off_cluster.slow_id.expect("slow worker present");
    let (off_elapsed, off_results) = off_cluster.run_batch(BATCH_TASKS, BASE_SLEEP_MS).await;
    let off_slow_count = off_results
        .iter()
        .filter(|(r, _)| r.worker_id == off_slow_id)
        .count();
    off_cluster.teardown().await;

    assert!(
        off_slow_count >= 4,
        "with speed_hold_factor = 0, slow worker should receive Wave 1 tasks (got {off_slow_count})"
    );
    assert!(
        off_elapsed >= slow_task_time,
        "with speed_hold_factor = 0, the batch must wait for a slow task ({slow_task_time:?}), got {off_elapsed:?}"
    );

    eprintln!(
        "SCHED-4 D2 timing (printed, not asserted; TEST-44): fast worker alone {fast_own_time:?}, \
         D2 batch {d2_elapsed:?} ({:.2}x), speed_hold_factor = 0 batch {off_elapsed:?} ({:.2}x)",
        d2_elapsed.as_secs_f64() / fast_own_time.as_secs_f64(),
        off_elapsed.as_secs_f64() / fast_own_time.as_secs_f64()
    );
}

#[tokio::test]
async fn sched4_near_equal_workers_spread_after_warmup() {
    const BASE_SLEEP_MS: u64 = 180;
    const WARMUP_TASKS: usize = 16;
    const MEASURED_TASKS: usize = 8;

    let sandbox_dir = TempDir::new().expect("tempdir for worker sandboxes");
    let server_cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap());
    let master = MasterServer::spawn_with_config(
        server_cfg,
        SchedulerConfig::default().without_host_cpu_backpressure(),
        ReaperConfig::default(),
    )
    .await
    .expect("MasterServer::spawn_with_config failed");
    let master_addr = master.server_addr().to_string();

    let mut ids = Vec::new();
    let mut shutdowns = Vec::new();
    // Both workers expose 8 concurrent slots (16 total) so an 8-task measured batch never
    // overflows onto the second worker due to slot saturation alone. `worker-b` sleeps 25 ms
    // longer (~1.14x vs 1.0x): well inside `speed_hold_factor = 1.5`, so D2 must spread tasks
    // across both whereas packing onto the lowest-EWMA worker sends all 8 tasks to `worker-a`.
    for (name, offset_ms) in [("worker-a", 0u64), ("worker-b", 25u64)] {
        let cfg = WorkerConfig::new(master_addr.clone())
            .with_name(name)
            .with_cores(8)
            .with_ram_mb(16_384)
            .with_no_gpu(true)
            .with_max_concurrency(8)
            .with_sandbox_base_dir(sandbox_dir.path().join(name));
        let mut client = WorkerClient::new(cfg).with_test_sleep_offset_ms(offset_ms);
        ids.push(client.worker_id());
        let (tx, rx) = watch::channel(false);
        shutdowns.push(tx);
        tokio::spawn(async move {
            let _ = client.run(rx).await;
        });
    }

    let ready = wait_for(
        Duration::from_secs(10),
        Duration::from_millis(25),
        || async {
            let workers = master.list_workers().await.unwrap_or_default();
            workers.len() == 2 && workers.iter().all(|w| w.status == WorkerStatus::Connected)
        },
    )
    .await;
    assert!(ready, "both near-equal workers failed to connect");

    let spec = sleep_spec(BASE_SLEEP_MS);
    // Warmup of 16 tasks so both 8-slot workers record >= 2 samples in SpeedBook.
    let mut warm_ids = Vec::with_capacity(WARMUP_TASKS);
    for _ in 0..WARMUP_TASKS {
        warm_ids.push(
            master
                .submit_task(Task::new(spec.clone(), TaskRequirements::generic(1, 30)))
                .await
                .unwrap(),
        );
    }
    let warm_results: Vec<TaskResult> = join_all(
        warm_ids
            .into_iter()
            .map(|id| master.wait_task(id, Some(Duration::from_secs(20)))),
    )
    .await
    .into_iter()
    .map(|r| r.unwrap())
    .collect();
    assert!(warm_results.iter().all(TaskResult::is_success));
    let warm_a = warm_results
        .iter()
        .filter(|r| r.worker_id == ids[0])
        .count();
    let warm_b = warm_results
        .iter()
        .filter(|r| r.worker_id == ids[1])
        .count();
    assert!(
        warm_a >= 2 && warm_b >= 2,
        "warmup must give both near-equal workers >= 2 samples (a={warm_a}, b={warm_b})"
    );

    // Measured 8-task batch with SpeedBook populated on 16 total slots: both 1.0x/1.14x workers
    // still share work instead of packing all 8 onto `worker-a`.
    let mut batch_ids = Vec::with_capacity(MEASURED_TASKS);
    for _ in 0..MEASURED_TASKS {
        batch_ids.push(
            master
                .submit_task(Task::new(spec.clone(), TaskRequirements::generic(1, 30)))
                .await
                .unwrap(),
        );
    }
    let batch_results: Vec<TaskResult> = join_all(
        batch_ids
            .into_iter()
            .map(|id| master.wait_task(id, Some(Duration::from_secs(20)))),
    )
    .await
    .into_iter()
    .map(|r| r.unwrap())
    .collect();
    assert!(batch_results.iter().all(TaskResult::is_success));

    let count_a = batch_results
        .iter()
        .filter(|r| r.worker_id == ids[0])
        .count();
    let count_b = batch_results
        .iter()
        .filter(|r| r.worker_id == ids[1])
        .count();

    for tx in shutdowns {
        let _ = tx.send(true);
    }
    let _ = master.shutdown();

    assert!(
        count_a >= 2 && count_b >= 2,
        "near-equal workers must spread work instead of packing onto one (a={count_a}, b={count_b})"
    );
}

#[tokio::test]
async fn sched4c_repeatable_straggler_rescued_non_repeatable_control_and_sibling_fail_tolerated() {
    use rusty_grid_core::capabilities::WorkerCapabilities;
    use rusty_grid_core::placement::{tag_value, OPT_REPEATABLE_TAG, SPEED_TAG_PREFIX};
    use rusty_grid_core::protocol::{
        MasterMessage, MessageTransport, ProtocolCapabilities, WorkerMessage,
        PROTOCOL_VERSION_CURRENT,
    };
    use tokio::net::TcpStream;
    use tokio::sync::mpsc;

    async fn connect_mock(
        addr: std::net::SocketAddr,
        caps: WorkerCapabilities,
    ) -> (
        Uuid,
        mpsc::Sender<WorkerMessage>,
        mpsc::Receiver<MasterMessage>,
        tokio::task::JoinHandle<()>,
    ) {
        let wid = Uuid::new_v4();
        let (tx_up, mut rx_up) = mpsc::channel::<WorkerMessage>(32);
        let (tx_down, rx_down) = mpsc::channel::<MasterMessage>(32);
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut transport = MessageTransport::new(stream);
        transport
            .send_msg(&WorkerMessage::Register {
                worker_id: wid,
                capabilities: caps,
                protocol_version: PROTOCOL_VERSION_CURRENT,
                features: ProtocolCapabilities::all_supported(),
            })
            .await
            .unwrap();
        let ack: Option<MasterMessage> = transport.recv_msg().await.unwrap();
        assert!(matches!(
            ack,
            Some(MasterMessage::RegisterAck { accepted: true, .. })
        ));
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg = transport.recv_msg::<MasterMessage>() => {
                        match msg {
                            Ok(Some(m)) => {
                                if tx_down.send(m).await.is_err() {
                                    break;
                                }
                            }
                            _ => break,
                        }
                    }
                    up = rx_up.recv() => {
                        match up {
                            Some(m) => {
                                if transport.send_msg(&m).await.is_err() {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        });
        (wid, tx_up, rx_down, handle)
    }

    let reaper_cfg = ReaperConfig::default()
        .with_scan_interval(Duration::from_millis(80))
        .with_straggler_min_elapsed(Duration::from_millis(400))
        .with_straggler_multiplier(3.0);
    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()),
        SchedulerConfig::default().without_host_cpu_backpressure(),
        reaper_cfg,
    )
    .await
    .unwrap();
    let addr = master.server_addr();

    let (fast_id, fast_tx, mut fast_rx, fast_h) = connect_mock(
        addr,
        WorkerCapabilities::new("w-fast", 1, 8192, false, false, None)
            .with_tags(vec!["tier-fast".into()]),
    )
    .await;
    let (slow_id, slow_tx, mut slow_rx, slow_h) = connect_mock(
        addr,
        WorkerCapabilities::new("w-slow", 1, 8192, false, false, None)
            .with_tags(vec!["tier-slow".into()]),
    )
    .await;
    assert!(
        wait_for(
            Duration::from_secs(10),
            Duration::from_millis(20),
            || async {
                master.list_workers().await.is_ok_and(|ws| {
                    ws.iter()
                        .filter(|w| w.status == WorkerStatus::Connected)
                        .count()
                        == 2
                })
            }
        )
        .await
    );

    let spec = TaskSpec::command("work", vec!["job".into()]);

    // Prime SpeedBook with 2 fast samples on w-fast (50 ms) and 2 slower samples on w-slow (150 ms).
    for _ in 0..2 {
        for (wid, tx, rx, tag, dur_ms) in [
            (
                fast_id,
                &fast_tx,
                &mut fast_rx,
                "require:tag=tier-fast",
                50u64,
            ),
            (
                slow_id,
                &slow_tx,
                &mut slow_rx,
                "require:tag=tier-slow",
                150u64,
            ),
        ] {
            let tid = master
                .submit_task(
                    Task::new(spec.clone(), TaskRequirements::generic(1, 30))
                        .with_tags(vec![tag.into()]),
                )
                .await
                .unwrap();
            let assigned = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if let Some(MasterMessage::AssignTask { task }) = rx.recv().await {
                        if task.id == tid {
                            break;
                        }
                    }
                }
            })
            .await;
            assert!(assigned.is_ok());
            tx.send(WorkerMessage::from(TaskResult::success(
                wid, tid, "ok", dur_ms, false,
            )))
            .await
            .unwrap();
            let res = master
                .wait_task(tid, Some(Duration::from_secs(10)))
                .await
                .unwrap();
            assert!(res.is_success());
        }
    }

    // SCHED-4d check: list_workers exposes speed:1.0 on w-fast and speed:3.0 on w-slow.
    let listed = master.list_workers().await.unwrap();
    let fast_listed = listed.iter().find(|w| w.worker_id == fast_id).unwrap();
    let slow_listed = listed.iter().find(|w| w.worker_id == slow_id).unwrap();
    assert_eq!(
        tag_value(&fast_listed.capabilities.tags, SPEED_TAG_PREFIX),
        Some("1.0")
    );
    assert_eq!(
        tag_value(&slow_listed.capabilities.tags, SPEED_TAG_PREFIX),
        Some("3.0")
    );

    // Occupy w-fast with a short blocker so the repeatable task lands on w-slow first.
    let blocker_id = master
        .submit_task(
            Task::new(
                TaskSpec::command("block", vec![]),
                TaskRequirements::generic(1, 30),
            )
            .with_tags(vec!["require:tag=tier-fast".into()]),
        )
        .await
        .unwrap();
    let got_blocker = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(MasterMessage::AssignTask { task }) = fast_rx.recv().await {
                if task.id == blocker_id {
                    break;
                }
            }
        }
    })
    .await;
    assert!(got_blocker.is_ok());
    fast_tx
        .send(WorkerMessage::TaskProgress {
            worker_id: fast_id,
            task_id: blocker_id,
            status: rusty_grid_core::task::TaskStatus::Running,
        })
        .await
        .unwrap();

    // Submit repeatable task -> lands on w-slow and marks Running.
    let rep_id = master
        .submit_task(
            Task::new(spec.clone(), TaskRequirements::generic(1, 30))
                .with_tags(vec![OPT_REPEATABLE_TAG.into()]),
        )
        .await
        .unwrap();
    let got_rep_on_slow = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(MasterMessage::AssignTask { task }) = slow_rx.recv().await {
                if task.id == rep_id {
                    break;
                }
            }
        }
    })
    .await;
    assert!(got_rep_on_slow.is_ok());
    slow_tx
        .send(WorkerMessage::TaskProgress {
            worker_id: slow_id,
            task_id: rep_id,
            status: rusty_grid_core::task::TaskStatus::Running,
        })
        .await
        .unwrap();

    // Wait 550ms (> 400ms floor and > 3 * 50ms), then free w-fast.
    tokio::time::sleep(Duration::from_millis(550)).await;
    fast_tx
        .send(WorkerMessage::from(TaskResult::success(
            fast_id, blocker_id, "done", 600, false,
        )))
        .await
        .unwrap();
    master
        .wait_task(blocker_id, Some(Duration::from_secs(10)))
        .await
        .unwrap();

    // Reaper straggler pass dispatches speculative copy of rep_id onto w-fast.
    let got_spec_on_fast = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(MasterMessage::AssignTask { task }) = fast_rx.recv().await {
                if task.id == rep_id {
                    break;
                }
            }
        }
    })
    .await;
    assert!(
        got_spec_on_fast.is_ok(),
        "w-fast must receive speculative copy"
    );

    // Slow primary fails AFTER speculative copy is running -> task does NOT fail!
    slow_tx
        .send(WorkerMessage::from(TaskResult::failure(
            slow_id,
            rep_id,
            2,
            "",
            "slow failed",
            700,
            None,
        )))
        .await
        .unwrap();

    // Fast sibling completes cleanly -> task succeeds on w-fast.
    fast_tx
        .send(WorkerMessage::from(TaskResult::success(
            fast_id, rep_id, "rescued", 45, false,
        )))
        .await
        .unwrap();
    let rep_res = master
        .wait_task(rep_id, Some(Duration::from_secs(10)))
        .await
        .unwrap();
    assert!(rep_res.is_success());
    assert_eq!(rep_res.worker_id, fast_id);
    assert_eq!(rep_res.stdout_str(), "rescued");

    // Now test sibling cancellation when w-fast wins while w-slow is still running:
    let b2_id = master
        .submit_task(
            Task::new(
                TaskSpec::command("block2", vec![]),
                TaskRequirements::generic(1, 30),
            )
            .with_tags(vec!["require:tag=tier-fast".into()]),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(MasterMessage::AssignTask { task }) = fast_rx.recv().await {
                if task.id == b2_id {
                    break;
                }
            }
        }
    })
    .await
    .unwrap();

    let rep3_id = master
        .submit_task(
            Task::new(spec.clone(), TaskRequirements::generic(1, 30))
                .with_tags(vec![OPT_REPEATABLE_TAG.into()]),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(MasterMessage::AssignTask { task }) = slow_rx.recv().await {
                if task.id == rep3_id {
                    break;
                }
            }
        }
    })
    .await
    .unwrap();
    slow_tx
        .send(WorkerMessage::TaskProgress {
            worker_id: slow_id,
            task_id: rep3_id,
            status: rusty_grid_core::task::TaskStatus::Running,
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(550)).await;
    fast_tx
        .send(WorkerMessage::from(TaskResult::success(
            fast_id, b2_id, "ok", 600, false,
        )))
        .await
        .unwrap();
    master
        .wait_task(b2_id, Some(Duration::from_secs(10)))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(MasterMessage::AssignTask { task }) = fast_rx.recv().await {
                if task.id == rep3_id {
                    break;
                }
            }
        }
    })
    .await
    .unwrap();
    fast_tx
        .send(WorkerMessage::from(TaskResult::success(
            fast_id, rep3_id, "win3", 40, false,
        )))
        .await
        .unwrap();
    let rep3_res = master
        .wait_task(rep3_id, Some(Duration::from_secs(10)))
        .await
        .unwrap();
    assert_eq!(rep3_res.worker_id, fast_id);

    // Slow sibling must receive CancelTask with "no longer assigned to this worker".
    let cancelled_on_slow = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(MasterMessage::CancelTask { task_id, reason }) = slow_rx.recv().await {
                if task_id == rep3_id {
                    break reason;
                }
            }
        }
    })
    .await
    .expect("w-slow must receive CancelTask when fast sibling completes");
    assert!(cancelled_on_slow
        .as_deref()
        .is_some_and(|r| r.contains("no longer assigned to this worker")));

    // Non-repeatable control: never duplicated onto w-fast even past 400ms.
    let b3_id = master
        .submit_task(
            Task::new(
                TaskSpec::command("block3", vec![]),
                TaskRequirements::generic(1, 30),
            )
            .with_tags(vec!["require:tag=tier-fast".into()]),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(MasterMessage::AssignTask { task }) = fast_rx.recv().await {
                if task.id == b3_id {
                    break;
                }
            }
        }
    })
    .await
    .unwrap();
    let ctrl_id = master
        .submit_task(Task::new(spec, TaskRequirements::generic(1, 30)))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(MasterMessage::AssignTask { task }) = slow_rx.recv().await {
                if task.id == ctrl_id {
                    break;
                }
            }
        }
    })
    .await
    .unwrap();
    slow_tx
        .send(WorkerMessage::TaskProgress {
            worker_id: slow_id,
            task_id: ctrl_id,
            status: rusty_grid_core::task::TaskStatus::Running,
        })
        .await
        .unwrap();
    fast_tx
        .send(WorkerMessage::from(TaskResult::success(
            fast_id, b3_id, "ok", 100, false,
        )))
        .await
        .unwrap();
    master
        .wait_task(b3_id, Some(Duration::from_secs(10)))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(650)).await;
    assert!(
        fast_rx.try_recv().is_err(),
        "non-repeatable control task must never be duplicated onto idle fast worker"
    );
    slow_tx
        .send(WorkerMessage::from(TaskResult::success(
            slow_id, ctrl_id, "ctrl", 700, false,
        )))
        .await
        .unwrap();
    let ctrl_res = master
        .wait_task(ctrl_id, Some(Duration::from_secs(10)))
        .await
        .unwrap();
    assert_eq!(ctrl_res.worker_id, slow_id);

    fast_h.abort();
    slow_h.abort();
    let _ = master.shutdown();
}
