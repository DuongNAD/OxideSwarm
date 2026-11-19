//! Integration tests for `NET-9` local contributor controls ([design 0095](../../docs/design/0095-contributor-controls.md)).
//!
//! Runs against a real loopback `MasterServer` and `WorkerClient`s using injected clock, host CPU,
//! power-source, and network-cost probes. Waits strictly by condition with generous deadlines (no
//! wall-clock window assertions).

use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse, MessageTransport, WorkerMessage};
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_core::CONTRIBUTOR_CONTROL_REFUSAL_PREFIX;
use rusty_grid_master::{MasterHandle, MasterServer, ServerConfig, TaskState, WorkerStatus};
use rusty_grid_worker::{
    write_pause_file, AllowHoursSchedule, BackoffConfig, Clock, ContributorControlsConfig,
    FakeRunnerConfig, HostCpuProbe, NetworkCost, NetworkCostProbe, PowerProbe, PowerSource,
    WorkerClient, WorkerConfig, WorkerControls, WorkerControlsHandle, EXIT_CODE_SUCCESS,
};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

fn real_test_worker_config(
    master_addr: impl Into<String>,
    name: &str,
    sandbox_dir: &std::path::Path,
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
        .with_sandbox_base_dir(sandbox_dir)
}

fn default_injected_controls(control_file: Option<std::path::PathBuf>) -> WorkerControlsHandle {
    WorkerControls::with_probes(
        ContributorControlsConfig {
            control_file,
            ..Default::default()
        },
        InjectedClock::from_local(2026, 11, 19, 10, 0),
        InjectedCpuProbe::new(10.0),
        InjectedPowerProbe::new(PowerSource::Ac),
        InjectedNetworkProbe::new(NetworkCost::Unmetered),
    )
}

struct InjectedClock(AtomicI64);

impl InjectedClock {
    fn from_local(year: i32, month: u32, day: u32, hour: u32, min: u32) -> Arc<Self> {
        let ndt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(year, month, day).unwrap(),
            NaiveTime::from_hms_opt(hour, min, 0).unwrap(),
        );
        let dt = Local.from_local_datetime(&ndt).single().unwrap();
        Arc::new(Self(AtomicI64::new(dt.timestamp())))
    }

    fn set_local(&self, year: i32, month: u32, day: u32, hour: u32, min: u32) {
        let ndt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(year, month, day).unwrap(),
            NaiveTime::from_hms_opt(hour, min, 0).unwrap(),
        );
        let dt = Local.from_local_datetime(&ndt).single().unwrap();
        self.0.store(dt.timestamp(), Ordering::Release);
    }
}

impl Clock for InjectedClock {
    fn local_now(&self) -> chrono::DateTime<Local> {
        Local
            .timestamp_opt(self.0.load(Ordering::Acquire), 0)
            .single()
            .unwrap()
    }
}

struct InjectedCpuProbe(AtomicU32);

impl InjectedCpuProbe {
    fn new(pct: f32) -> Arc<Self> {
        Arc::new(Self(AtomicU32::new(pct.to_bits())))
    }

    fn set(&self, pct: f32) {
        self.0.store(pct.to_bits(), Ordering::Release);
    }
}

impl HostCpuProbe for InjectedCpuProbe {
    fn cpu_usage_pct(&self) -> f32 {
        f32::from_bits(self.0.load(Ordering::Acquire))
    }
}

struct InjectedPowerProbe(AtomicU8);

impl InjectedPowerProbe {
    fn new(src: PowerSource) -> Arc<Self> {
        Arc::new(Self(AtomicU8::new(src as u8)))
    }

    fn set(&self, src: PowerSource) {
        self.0.store(src as u8, Ordering::Release);
    }
}

impl PowerProbe for InjectedPowerProbe {
    fn power_source(&self) -> PowerSource {
        match self.0.load(Ordering::Acquire) {
            0 => PowerSource::Ac,
            1 => PowerSource::Battery,
            _ => PowerSource::Unknown,
        }
    }
}

struct InjectedNetworkProbe(AtomicU8);

impl InjectedNetworkProbe {
    fn new(cost: NetworkCost) -> Arc<Self> {
        Arc::new(Self(AtomicU8::new(cost as u8)))
    }

    fn set(&self, cost: NetworkCost) {
        self.0.store(cost as u8, Ordering::Release);
    }
}

impl NetworkCostProbe for InjectedNetworkProbe {
    fn network_cost(&self) -> NetworkCost {
        match self.0.load(Ordering::Acquire) {
            0 => NetworkCost::Unmetered,
            1 => NetworkCost::Metered,
            _ => NetworkCost::Unsupported,
        }
    }
}

async fn wait_until<F, Fut>(deadline_secs: u64, desc: &str, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(deadline_secs);
    loop {
        if cond().await {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for: {desc}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn spawn_test_master() -> MasterHandle {
    let cfg = ServerConfig::from_addr("127.0.0.1:0")
        .expect("loopback")
        .with_web_ui(false)
        .with_implicit_local_dashboard(false)
        .with_discovery(false)
        .with_heartbeat_interval(1)
        .with_session_resume_hold(Duration::ZERO);
    MasterServer::spawn(cfg).await.expect("spawn master")
}

async fn submit_and_wait(
    master_addr: std::net::SocketAddr,
    task: Task,
) -> rusty_grid_core::TaskResult {
    let mut transport = MessageTransport::new(
        TcpStream::connect(master_addr)
            .await
            .expect("connect submitter"),
    );
    transport
        .send_msg(&ClientMessage::SubmitTask { task, wait: true })
        .await
        .expect("send SubmitTask");
    match tokio::time::timeout(
        Duration::from_secs(30),
        transport.recv_msg::<ClientResponse>(),
    )
    .await
    .expect("task wait timeout")
    .expect("recv")
    {
        Some(ClientResponse::TaskCompleted { result, .. }) => result,
        other => panic!("unexpected response from master: {other:?}"),
    }
}

fn simulated_worker(
    master_addr: String,
    name: &str,
    controls: Option<WorkerControlsHandle>,
    task_delay: Duration,
) -> WorkerClient {
    let mut cfg = WorkerConfig::simulated(
        master_addr,
        Some(name.to_string()),
        Some(4),
        FakeRunnerConfig::new(task_delay),
        None,
    );
    if let Some(ctrl) = controls {
        cfg = cfg.with_controls_handle(ctrl);
    }
    let mut client = WorkerClient::new(cfg);
    client.set_backoff_config(
        BackoffConfig::default()
            .with_initial(Duration::from_millis(50))
            .with_max(Duration::from_millis(200)),
    );
    client
}

fn pin_worker_task(worker_id: Uuid) -> Task {
    Task::new(
        TaskSpec::BuiltinTest {
            test_name: "net9-pinned".to_string(),
            iterations: 1,
            duration_ms: 0,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30),
    )
    .with_tags(vec![format!("require:worker={worker_id}")])
}

fn unpinned_task() -> Task {
    Task::new(
        TaskSpec::BuiltinTest {
            test_name: "net9-any".to_string(),
            iterations: 1,
            duration_ms: 0,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30),
    )
}

#[tokio::test]
async fn test_net9_injected_probes_gate_cpu_schedule_daily_cap_battery_and_metered() {
    let master = spawn_test_master().await;
    let master_addr = master.server_addr();
    let master_str = master_addr.to_string();

    let clock = InjectedClock::from_local(2026, 11, 19, 10, 0); // Thu 10:00
    let cpu = InjectedCpuProbe::new(15.0);
    let power = InjectedPowerProbe::new(PowerSource::Ac);
    let net = InjectedNetworkProbe::new(NetworkCost::Unmetered);
    let w1_controls = WorkerControls::with_probes(
        ContributorControlsConfig {
            max_host_cpu_pct: Some(50.0),
            allow_hours: Some(AllowHoursSchedule::parse("Thu 09:00-18:00").unwrap()),
            daily_task_hours: Some(1e-9), // First completed task (>= 1 ms) exhausts today's cap.
            pause_on_battery: true,
            pause_on_metered: true,
            control_file: None,
        },
        Arc::clone(&clock) as Arc<dyn Clock>,
        Arc::clone(&cpu) as Arc<dyn HostCpuProbe>,
        Arc::clone(&power) as Arc<dyn PowerProbe>,
        Arc::clone(&net) as Arc<dyn NetworkCostProbe>,
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut w1 = simulated_worker(
        master_str.clone(),
        "w1-controlled",
        Some(Arc::clone(&w1_controls)),
        Duration::from_millis(10),
    );
    let mut w2 = simulated_worker(
        master_str,
        "w2-open",
        Some(default_injected_controls(None)),
        Duration::ZERO,
    );
    let w1_id = w1.worker_id();
    let w2_id = w2.worker_id();
    let h1 = tokio::spawn({
        let rx = shutdown_rx.clone();
        async move { w1.run(rx).await }
    });
    let h2 = tokio::spawn({
        let rx = shutdown_rx.clone();
        async move { w2.run(rx).await }
    });

    wait_until(15, "both workers connected", || async {
        let ws = master.registry().list_workers().await;
        ws.iter()
            .filter(|w| w.status == WorkerStatus::Connected)
            .count()
            == 2
    })
    .await;

    // 1. CPU above threshold holds back w1 via heartbeat host_busy -> unpinned runs on w2.
    cpu.set(85.0);
    w1_controls.state_notify().notify_waiters();
    wait_until(10, "master observes w1 host_busy on high CPU", || async {
        master
            .registry()
            .get_worker(w1_id)
            .await
            .is_some_and(|w| w.cpu_usage_pct > 90.0)
    })
    .await;
    let res = submit_and_wait(master_addr, unpinned_task()).await;
    assert_eq!(res.exit_code, EXIT_CODE_SUCCESS);
    assert_eq!(res.worker_id, w2_id, "high-CPU worker must be skipped");

    cpu.set(10.0);
    w1_controls.state_notify().notify_waiters();
    wait_until(10, "master observes w1 unblocked CPU", || async {
        master
            .registry()
            .get_worker(w1_id)
            .await
            .is_some_and(|w| w.cpu_usage_pct < 85.0)
    })
    .await;

    // 2. Outside --allow-hours blocks w1; inside window accepts pinned task.
    clock.set_local(2026, 11, 19, 20, 0);
    w1_controls.state_notify().notify_waiters();
    wait_until(
        10,
        "master observes w1 blocked outside schedule",
        || async {
            master
                .registry()
                .get_worker(w1_id)
                .await
                .is_some_and(|w| w.cpu_usage_pct > 90.0)
        },
    )
    .await;
    let res_sched = submit_and_wait(master_addr, unpinned_task()).await;
    assert_eq!(res_sched.worker_id, w2_id);

    clock.set_local(2026, 11, 19, 12, 0);
    w1_controls.state_notify().notify_waiters();
    wait_until(10, "master observes w1 unblocked in schedule", || async {
        master
            .registry()
            .get_worker(w1_id)
            .await
            .is_some_and(|w| w.cpu_usage_pct < 85.0)
    })
    .await;

    // 3. On battery blocks w1.
    power.set(PowerSource::Battery);
    w1_controls.state_notify().notify_waiters();
    wait_until(10, "master observes w1 blocked on battery", || async {
        master
            .registry()
            .get_worker(w1_id)
            .await
            .is_some_and(|w| w.cpu_usage_pct > 90.0)
    })
    .await;
    let res_bat = submit_and_wait(master_addr, unpinned_task()).await;
    assert_eq!(res_bat.worker_id, w2_id);
    power.set(PowerSource::Ac);
    w1_controls.state_notify().notify_waiters();
    wait_until(
        10,
        "master observes w1 unblocked after AC return",
        || async {
            master
                .registry()
                .get_worker(w1_id)
                .await
                .is_some_and(|w| w.cpu_usage_pct < 85.0)
        },
    )
    .await;

    // 4. Metered network blocks w1.
    net.set(NetworkCost::Metered);
    w1_controls.state_notify().notify_waiters();
    wait_until(
        10,
        "master observes w1 blocked on metered network",
        || async {
            master
                .registry()
                .get_worker(w1_id)
                .await
                .is_some_and(|w| w.cpu_usage_pct > 90.0)
        },
    )
    .await;
    let res_net = submit_and_wait(master_addr, unpinned_task()).await;
    assert_eq!(res_net.worker_id, w2_id);
    net.set(NetworkCost::Unmetered);
    w1_controls.state_notify().notify_waiters();
    wait_until(
        10,
        "master observes w1 unblocked on unmetered AC",
        || async {
            master
                .registry()
                .get_worker(w1_id)
                .await
                .is_some_and(|w| w.cpu_usage_pct < 85.0)
        },
    )
    .await;

    // 5. Run one pinned task on w1 (consumes daily_task_hours), then daily cap blocks until midnight reset.
    let res_w1 = submit_and_wait(master_addr, pin_worker_task(w1_id)).await;
    assert_eq!(res_w1.exit_code, EXIT_CODE_SUCCESS);
    assert_eq!(res_w1.worker_id, w1_id);
    wait_until(10, "master observes w1 blocked after daily cap", || async {
        master
            .registry()
            .get_worker(w1_id)
            .await
            .is_some_and(|w| w.cpu_usage_pct > 90.0)
    })
    .await;
    let res_capped = submit_and_wait(master_addr, unpinned_task()).await;
    assert_eq!(res_capped.worker_id, w2_id);

    // Advance injected clock into next Thursday (2026-11-26 10:00) -> resets daily cap.
    clock.set_local(2026, 11, 26, 10, 0);
    w1_controls.state_notify().notify_waiters();
    wait_until(
        10,
        "master observes w1 unblocked on next local day",
        || async {
            master
                .registry()
                .get_worker(w1_id)
                .await
                .is_some_and(|w| w.cpu_usage_pct < 85.0)
        },
    )
    .await;
    let res_reset = submit_and_wait(master_addr, pin_worker_task(w1_id)).await;
    assert_eq!(res_reset.exit_code, EXIT_CODE_SUCCESS);
    assert_eq!(res_reset.worker_id, w1_id);

    let _ = shutdown_tx.send(true);
    let _ = h1.await;
    let _ = h2.await;
    let _ = master.shutdown();
}

#[tokio::test]
async fn test_net9_paused_worker_refuses_direct_assignment() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = real_test_worker_config("127.0.0.1:1", "paused-direct", dir.path());
    let client = WorkerClient::new(cfg);
    client.controls().pause();

    let (tx, mut rx) = mpsc::channel(4);
    let task = unpinned_task();
    client.handle_assign_task(task.clone(), tx).await;

    let msg = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("refusal timeout")
        .expect("TaskResult");
    match msg {
        WorkerMessage::TaskResult {
            task_id,
            exit_code,
            execution_time_ms,
            stdout,
            error,
            ..
        } => {
            assert_eq!(task_id, task.id);
            assert_eq!(exit_code, 1);
            assert_eq!(execution_time_ms, 0);
            assert!(stdout.is_empty());
            assert!(
                error
                    .as_deref()
                    .is_some_and(|e| e.starts_with(CONTRIBUTOR_CONTROL_REFUSAL_PREFIX)),
                "unexpected error: {:?}",
                error
            );
        }
        other => panic!("expected TaskResult refusal, got {other:?}"),
    }
}

#[tokio::test]
async fn test_net9_worker_pause_stops_running_task_within_5s_and_hands_back_uncharged() {
    let dir = tempfile::tempdir().unwrap();
    let ctrl_file = dir.path().join("worker_pause.w1");
    let started_marker = dir.path().join("task_started.marker");
    let sb1 = dir.path().join("sb1");
    let sb2 = dir.path().join("sb2");
    std::fs::create_dir_all(&sb1).unwrap();
    std::fs::create_dir_all(&sb2).unwrap();

    let master = spawn_test_master().await;
    let master_addr = master.server_addr();
    let master_str = master_addr.to_string();

    let w1_cfg = real_test_worker_config(master_str.clone(), "w1-pause", &sb1)
        .with_controls_handle(default_injected_controls(Some(ctrl_file.clone())));
    let mut w1 = WorkerClient::new(w1_cfg);
    let w1_id = w1.worker_id();
    let w1_controls = Arc::clone(w1.controls());

    // Start w2 paused initially so the long task MUST land on w1 first.
    let w2_cfg = real_test_worker_config(master_str, "w2-failover", &sb2)
        .with_controls_handle(default_injected_controls(None));
    let mut w2 = WorkerClient::new(w2_cfg);
    let w2_id = w2.worker_id();
    let w2_controls = Arc::clone(w2.controls());
    w2_controls.pause();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let h1 = tokio::spawn({
        let rx = shutdown_rx.clone();
        async move { w1.run(rx).await }
    });
    let h2 = tokio::spawn({
        let rx = shutdown_rx.clone();
        async move { w2.run(rx).await }
    });

    wait_until(
        15,
        "both workers connected and w2 initially paused",
        || async {
            let w1_ok = master
                .registry()
                .get_worker(w1_id)
                .await
                .is_some_and(|w| w.status == WorkerStatus::Connected && w.cpu_usage_pct < 85.0);
            let w2_paused = master
                .registry()
                .get_worker(w2_id)
                .await
                .is_some_and(|w| w.status == WorkerStatus::Connected && w.cpu_usage_pct > 90.0);
            w1_ok && w2_paused
        },
    )
    .await;

    // On run 1 (w1): creates started_marker and sleeps until killed.
    // On run 2 (w2 after handback): sees started_marker already exists and exits 0 immediately.
    #[cfg(unix)]
    let script = format!(
        "if [ ! -f '{0}' ]; then echo started > '{0}'; while :; do sleep 0.2; done; else echo resumed_done; fi",
        started_marker.display()
    );
    #[cfg(windows)]
    let script = format!(
        "if not exist \"{0}\" (echo started > \"{0}\" & ping -n 61 127.0.0.1 >NUL) else (echo resumed_done)",
        started_marker.display()
    );

    let task = Task::new(
        TaskSpec::shell_script(script),
        TaskRequirements::generic(1, 60),
    );
    let task_id = task.id;
    let submit_fut = tokio::spawn(submit_and_wait(master_addr, task));

    wait_until(20, "worker 1 creates started_marker", || {
        let p = started_marker.clone();
        async move { p.exists() }
    })
    .await;
    wait_until(10, "master records task Running on w1", || async {
        master
            .queue()
            .get_task(&task_id)
            .await
            .is_some_and(|t| t.state == TaskState::Running && t.assigned_worker_id == Some(w1_id))
    })
    .await;

    // Trigger contributor pause via the real control file (`worker pause` mechanism).
    write_pause_file(&ctrl_file).expect("write pause file");
    wait_until(
        15,
        "w1 stops active task and becomes idle+paused on master",
        || async {
            let worker_idle_and_busy_gated = master
                .registry()
                .get_worker(w1_id)
                .await
                .is_some_and(|w| w.status == WorkerStatus::Connected && w.cpu_usage_pct > 90.0);
            let handback_seen = master
                .queue()
                .get_task(&task_id)
                .await
                .is_some_and(|t| t.failover_count >= 1);
            worker_idle_and_busy_gated && handback_seen
        },
    )
    .await;

    assert!(
        w1_controls.is_paused(),
        "w1 must be in paused state after control file write"
    );

    // Unpause w2 so it completes the handed-back task.
    w2_controls.resume();
    let result = tokio::time::timeout(Duration::from_secs(20), submit_fut)
        .await
        .expect("handed-back task finish timeout")
        .expect("submit join");
    assert_eq!(result.exit_code, EXIT_CODE_SUCCESS);
    assert_eq!(
        result.worker_id, w2_id,
        "handed-back task must complete on w2"
    );

    let info = master
        .queue()
        .get_task(&task_id)
        .await
        .expect("task info in queue");
    assert_eq!(info.state, TaskState::Completed);
    assert_eq!(
        info.failover_count, 1,
        "worker pause must count as 1 failover"
    );
    assert_eq!(
        info.retry_count.saturating_sub(info.failover_count),
        0,
        "contributor pause must charge zero execution retries (retry={}, failover={})",
        info.retry_count,
        info.failover_count
    );

    // w1 remained connected the whole time; removing pause file lets it accept tasks again.
    rusty_grid_worker::remove_pause_file(&ctrl_file).expect("remove pause file");
    w2_controls.pause();
    wait_until(10, "w1 clears host_busy and w2 is paused", || async {
        let w1_ready = master
            .registry()
            .get_worker(w1_id)
            .await
            .is_some_and(|w| w.status == WorkerStatus::Connected && w.cpu_usage_pct < 85.0);
        let w2_paused = master
            .registry()
            .get_worker(w2_id)
            .await
            .is_some_and(|w| w.status == WorkerStatus::Connected && w.cpu_usage_pct > 90.0);
        w1_ready && w2_paused
    })
    .await;
    let followup = submit_and_wait(master_addr, unpinned_task()).await;
    assert_eq!(followup.exit_code, EXIT_CODE_SUCCESS);
    assert_eq!(followup.worker_id, w1_id);

    let _ = shutdown_tx.send(true);
    let _ = h1.await;
    let _ = h2.await;
    let _ = master.shutdown();
}
