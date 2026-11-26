//! Loopback integration tests for NET-9b (`design 0113` D1/T1/T4, #481 over #478):
//!
//! * Fake battery (`BatteryProbe`) and fake metered network (`MeteredNetworkProbe`) holds:
//!   worker transitions to `WorkerControlState::Held` (reported on `worker status` with reason and
//!   detail), Master observes `state:paused`, refused tasks requeue uncharged (`max_retries = 0`),
//!   and clearing the holding limit restores normal scheduling.
//! * `--hold-ends-tasks` hands running tasks back cleanly when an automatic limit triggers.
//! * Without `--hold-ends-tasks`, a running task finishes normally; if a rejection occurred while
//!   held and the hold lifts before the running task ends, the worker auto-reconnects once idle.
//! * Live `limits set` via the control channel (`ControlEvent::LimitsUpdated`) enforces `hours`,
//!   `local_max_cpu_pct`, and `max_task_hours_per_day` (after real completed tasks record runtime),
//!   and manual `Resume` while a limit still holds keeps the worker in `Held`.

use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::NaiveDateTime;
use tempfile::TempDir;
use tokio::sync::watch;
use uuid::Uuid;

use rusty_grid_core::placement::STATE_TAG_PREFIX;
use rusty_grid_core::protocol::{ClientMessage, ClientResponse, MessageTransport};
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec, TaskStatus};
use rusty_grid_master::queue::TaskState;
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::registry::WorkerStatus;
use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};
use rusty_grid_worker::control::{
    send_control_request, ControlEndpoint, ControlRequest, ControlResponse, ControlStatusSnapshot,
    WorkerControlState, WorkerLimitsConfig,
};
use rusty_grid_worker::limits::{
    BatteryProbe, BatteryStatus, HostCpuProbe, LocalClock, MeteredNetworkProbe, MeteredStatus,
};

const WAIT_BOUND: Duration = Duration::from_secs(60);

struct AtomicBatteryProbe(Arc<AtomicU8>);
impl BatteryProbe for AtomicBatteryProbe {
    fn probe_battery(&self) -> BatteryStatus {
        match self.0.load(Ordering::Acquire) {
            1 => BatteryStatus::OnBattery,
            2 => BatteryStatus::OnAc,
            _ => BatteryStatus::Unknown,
        }
    }
}

struct AtomicMeteredProbe(Arc<AtomicU8>);
impl MeteredNetworkProbe for AtomicMeteredProbe {
    fn probe_metered(&self) -> MeteredStatus {
        match self.0.load(Ordering::Acquire) {
            1 => MeteredStatus::Metered,
            2 => MeteredStatus::Unmetered,
            _ => MeteredStatus::Unknown,
        }
    }
}

struct AtomicCpuProbe(Arc<AtomicU32>);
impl HostCpuProbe for AtomicCpuProbe {
    fn probe_cpu_pct(&self) -> Option<f32> {
        Some(f32::from_bits(self.0.load(Ordering::Acquire)))
    }
}

struct FixedClock(NaiveDateTime);
impl LocalClock for FixedClock {
    fn local_now(&self) -> NaiveDateTime {
        self.0
    }
}

#[derive(Default)]
struct ProbeHandles {
    battery: Option<Arc<AtomicU8>>,
    metered: Option<Arc<AtomicU8>>,
    cpu_bits: Option<Arc<AtomicU32>>,
    fixed_clock: Option<NaiveDateTime>,
    hold_ends_tasks: bool,
}

struct Net9bCluster {
    master: MasterHandle,
    addr: String,
    workers: Vec<(watch::Sender<bool>, tokio::task::JoinHandle<()>)>,
    _dirs: Vec<TempDir>,
}

impl Net9bCluster {
    async fn new() -> Self {
        let blob_dir = tempfile::tempdir().expect("blob dir");
        let master = MasterServer::spawn_with_config(
            ServerConfig::new("127.0.0.1:0".parse().unwrap())
                .with_blob_store(blob_dir.path(), 64 * 1024 * 1024),
            SchedulerConfig::default().without_host_cpu_backpressure(),
            ReaperConfig::default(),
        )
        .await
        .expect("master starts");
        let addr = master.server_addr().to_string();
        Self {
            master,
            addr,
            workers: Vec::new(),
            _dirs: vec![blob_dir],
        }
    }

    fn spawn_worker(
        &mut self,
        name: &str,
        cores: usize,
        control_endpoint: Option<ControlEndpoint>,
        initial_limits: WorkerLimitsConfig,
        probes: ProbeHandles,
    ) -> Uuid {
        let dir = tempfile::tempdir().expect("sandbox dir");
        let mut config = WorkerConfig::new(self.addr.clone())
            .with_no_battery(true)
            .with_name(name)
            .with_cores(cores)
            .with_max_concurrency(cores)
            .with_ram_mb(2048)
            .with_sandbox_base_dir(dir.path())
            .with_redirection_portal(false)
            .with_control_endpoint(control_endpoint)
            .with_initial_limits(initial_limits)
            .with_hold_ends_tasks(probes.hold_ends_tasks)
            .with_limit_eval_interval(Duration::from_millis(80));

        if let Some(b) = probes.battery {
            config = config.with_battery_probe(Some(Arc::new(AtomicBatteryProbe(b))));
        }
        if let Some(m) = probes.metered {
            config = config.with_metered_probe(Some(Arc::new(AtomicMeteredProbe(m))));
        }
        if let Some(c) = probes.cpu_bits {
            config = config.with_cpu_probe(Some(Arc::new(AtomicCpuProbe(c))));
        }
        if let Some(dt) = probes.fixed_clock {
            config = config.with_local_clock(Some(Arc::new(FixedClock(dt))));
        }

        self._dirs.push(dir);
        let mut client = WorkerClient::new(config);
        let id = client.worker_id();
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let _ = client.run(rx).await;
        });
        self.workers.push((tx, handle));
        id
    }

    async fn wait_for_workers(&self, count: usize) {
        let deadline = Instant::now() + WAIT_BOUND;
        while Instant::now() < deadline {
            if let Ok(workers) = self.master.list_workers().await {
                if workers
                    .iter()
                    .filter(|w| w.status != WorkerStatus::Disconnected)
                    .count()
                    >= count
                {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("{count} workers did not register within {WAIT_BOUND:?}");
    }

    async fn wait_until_paused(&self, worker_id: Uuid, want_paused: bool) {
        let deadline = Instant::now() + WAIT_BOUND;
        while Instant::now() < deadline {
            if let Ok(workers) = self.master.list_workers().await {
                if workers.iter().any(|w| {
                    w.worker_id == worker_id
                        && w.status != WorkerStatus::Disconnected
                        && w.paused == want_paused
                }) {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!(
            "worker {worker_id} paused={want_paused} not observed on master within {WAIT_BOUND:?}"
        );
    }
}

impl Drop for Net9bCluster {
    fn drop(&mut self) {
        for (tx, handle) in &self.workers {
            let _ = tx.send(true);
            handle.abort();
        }
    }
}

fn unique_endpoint(dir: &Path, label: &str) -> ControlEndpoint {
    #[cfg(unix)]
    {
        ControlEndpoint::UnixSocket(dir.join(format!("{label}-{}.control.sock", Uuid::new_v4())))
    }
    #[cfg(windows)]
    {
        let _ = dir;
        ControlEndpoint::NamedPipe(format!(
            r"\\.\pipe\oxideswarm-net9b-{label}-{}",
            Uuid::new_v4()
        ))
    }
}

async fn poll_control_status<F>(ep: &ControlEndpoint, mut pred: F) -> ControlStatusSnapshot
where
    F: FnMut(&ControlStatusSnapshot) -> bool,
{
    let deadline = Instant::now() + WAIT_BOUND;
    let mut last = None;
    while Instant::now() < deadline {
        if let Ok(ControlResponse::Status { snapshot }) =
            send_control_request(ep, &ControlRequest::Status).await
        {
            if pred(&snapshot) {
                return snapshot;
            }
            last = Some(snapshot);
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    panic!("control status condition not met within {WAIT_BOUND:?}; last={last:?}");
}

async fn wait_for_file(path: &Path) {
    let deadline = Instant::now() + WAIT_BOUND;
    while Instant::now() < deadline {
        if path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!(
        "marker file {} not created within {WAIT_BOUND:?}",
        path.display()
    );
}

fn gated_task_spec(started_marker: &Path, release_flag: &Path, output_tag: &str) -> TaskSpec {
    #[cfg(unix)]
    {
        let cmd = format!(
            "touch '{started}' && while [ ! -f '{release}' ]; do sleep 0.05; done && echo '{tag}'",
            started = started_marker.display(),
            release = release_flag.display(),
            tag = output_tag,
        );
        TaskSpec::command("sh", vec!["-c".into(), cmd])
    }
    #[cfg(windows)]
    {
        let ps = format!(
            "$null = New-Item -ItemType File -Force -Path '{started}'; while (-not (Test-Path '{release}')) {{ Start-Sleep -Milliseconds 50 }}; Write-Output '{tag}'",
            started = started_marker.display(),
            release = release_flag.display(),
            tag = output_tag,
        );
        TaskSpec::command(
            "powershell",
            vec!["-NoProfile".into(), "-Command".into(), ps],
        )
    }
}

fn echo_task(text: &str) -> Task {
    let req = TaskRequirements::generic(1, 60).with_max_retries(0);
    #[cfg(unix)]
    let spec = TaskSpec::command("sh", vec!["-c".into(), format!("echo '{text}'")]);
    #[cfg(windows)]
    let spec = TaskSpec::command("cmd", vec!["/C".into(), format!("echo {text}")]);
    Task::new(spec, req)
}

#[tokio::test]
async fn net9b_fake_battery_and_metered_holds_hand_back_and_recover() {
    let mut cluster = Net9bCluster::new().await;
    let dir = tempfile::tempdir().expect("ipc dir");
    let ep_lim = unique_endpoint(dir.path(), "lim");

    let battery_state = Arc::new(AtomicU8::new(2)); // OnAc
    let metered_state = Arc::new(AtomicU8::new(2)); // Unmetered

    let w_lim = cluster.spawn_worker(
        "lim-box",
        1,
        Some(ep_lim.clone()),
        WorkerLimitsConfig {
            no_tasks_on_battery: Some(true),
            no_tasks_on_metered: Some(true),
            ..Default::default()
        },
        ProbeHandles {
            battery: Some(Arc::clone(&battery_state)),
            metered: Some(Arc::clone(&metered_state)),
            hold_ends_tasks: true,
            ..Default::default()
        },
    );
    cluster.wait_for_workers(1).await;

    // 1. Flip fake battery probe to OnBattery: worker status must enter Held("on battery").
    battery_state.store(1, Ordering::Release);
    let snap = poll_control_status(&ep_lim, |s| s.state == WorkerControlState::Held).await;
    assert_eq!(snap.reason.as_deref(), Some("on battery"));
    assert!(
        snap.detail
            .as_deref()
            .is_some_and(|d| d.contains("battery")),
        "status detail should explain holding limit: {snap:?}"
    );

    // Submit a max_retries=0 task while `lim-box` is sole worker: it must reject uncharged,
    // Master enters `state:paused`, then joining `peer-box` completes the requeued task!
    let tid = cluster
        .master
        .submit_task(echo_task("battery-requeue-ok"))
        .await
        .expect("submit");
    cluster.wait_until_paused(w_lim, true).await;
    let listed = cluster.master.list_workers().await.unwrap();
    assert!(listed
        .iter()
        .any(|w| { w.worker_id == w_lim && w.status != WorkerStatus::Disconnected && w.paused }));
    let wire_stream = tokio::net::TcpStream::connect(&cluster.addr).await.unwrap();
    let mut wire = MessageTransport::new(wire_stream);
    wire.send_msg(&ClientMessage::ClusterStatus).await.unwrap();
    match wire.recv_msg::<ClientResponse>().await.unwrap() {
        Some(ClientResponse::ClusterStatus { workers, .. }) => {
            assert!(
                workers.iter().any(|w| {
                    w.name == "lim-box"
                        && w.tags
                            .iter()
                            .any(|t| t == &format!("{STATE_TAG_PREFIX}paused"))
                }),
                "wire ClusterStatus must advertise state:paused: {workers:?}"
            );
        }
        other => panic!("expected ClientResponse::ClusterStatus, got {other:?}"),
    }

    let w_peer = cluster.spawn_worker(
        "peer-box",
        1,
        None,
        WorkerLimitsConfig::default(),
        ProbeHandles::default(),
    );
    cluster.wait_for_workers(2).await;
    let res = cluster
        .master
        .wait_task(tid, Some(WAIT_BOUND))
        .await
        .expect("rejected battery task must requeue uncharged onto peer-box");
    assert_eq!(res.exit_code, 0, "requeued task error: {:?}", res.error);
    assert_eq!(res.worker_id, w_peer);
    assert!(res.stdout_str().contains("battery-requeue-ok"));

    // 2. Flip fake metered probe to Metered BEFORE recovering battery -> still Held("metered network")
    //    if battery recovers while metered stays Held.
    metered_state.store(1, Ordering::Release);
    battery_state.store(2, Ordering::Release);
    let snap = poll_control_status(&ep_lim, |s| {
        s.state == WorkerControlState::Held && s.reason.as_deref() == Some("metered network")
    })
    .await;
    assert!(snap
        .detail
        .as_deref()
        .is_some_and(|d| d.contains("metered")));

    // 3. Clear metered hold: `lim-box` leaves Held -> Running and clears `paused` on Master.
    metered_state.store(2, Ordering::Release);
    poll_control_status(&ep_lim, |s| s.state == WorkerControlState::Running).await;
    cluster.wait_until_paused(w_lim, false).await;

    // 4. Test `--hold-ends-tasks` on metered flip while `lim-box` is actively running a task.
    //    Since `peer-box` is busy on a long gated task of its own, `lim-box` picks up the 2nd task.
    let markers = tempfile::tempdir().expect("markers");
    let p_started = markers.path().join("peer.started");
    let p_release = markers.path().join("peer.release");
    let peer_tid = cluster
        .master
        .submit_task(Task::new(
            gated_task_spec(&p_started, &p_release, "peer-slot-occupier"),
            TaskRequirements::generic(1, 60).with_max_retries(0),
        ))
        .await
        .unwrap();
    wait_for_file(&p_started).await;

    // Now whichever worker ran peer_tid left the other free; wait? Better: release peer once
    // BOTH are busy so `lim-box` is definitely holding the second task.
    let l_started = markers.path().join("lim.started");
    let l_release = markers.path().join("lim.release");
    let target_tid = cluster
        .master
        .submit_task(Task::new(
            gated_task_spec(&l_started, &l_release, "metered-handback-ok"),
            TaskRequirements::generic(1, 60).with_max_retries(0),
        ))
        .await
        .unwrap();
    wait_for_file(&l_started).await;

    // Identify which task is on `w_lim` and which is on `w_peer`.
    let tasks = cluster.master.list_tasks().await.unwrap();
    let lim_tid = tasks
        .iter()
        .find(|t| t.assigned_worker_id == Some(w_lim) && t.state == TaskState::Running)
        .expect("w_lim running a task")
        .task_id;
    let other_tid = if lim_tid == target_tid {
        peer_tid
    } else {
        target_tid
    };
    let other_release = if lim_tid == target_tid {
        &p_release
    } else {
        &l_release
    };
    let lim_release = if lim_tid == target_tid {
        &l_release
    } else {
        &p_release
    };

    // Free `w_peer` first so it can immediately accept the handed-back task from `w_lim`.
    std::fs::write(other_release, b"go").unwrap();
    let other_res = cluster
        .master
        .wait_task(other_tid, Some(WAIT_BOUND))
        .await
        .unwrap();
    assert_eq!(other_res.exit_code, 0);

    // Flip `lim-box` to Metered with `hold_ends_tasks = true`: running task hands back uncharged!
    metered_state.store(1, Ordering::Release);
    poll_control_status(&ep_lim, |s| {
        s.state == WorkerControlState::Held && s.active_tasks == 0
    })
    .await;
    cluster.wait_until_paused(w_lim, true).await;

    std::fs::write(lim_release, b"go").unwrap();
    let handed_res = cluster
        .master
        .wait_task(lim_tid, Some(WAIT_BOUND))
        .await
        .unwrap();
    assert_eq!(handed_res.exit_code, 0);
    assert_eq!(handed_res.worker_id, w_peer);
}

#[tokio::test]
async fn net9b_mid_task_lift_without_hold_ends_tasks_reconnects_when_idle() {
    let mut cluster = Net9bCluster::new().await;
    let dir = tempfile::tempdir().expect("ipc dir");
    let ep_lim = unique_endpoint(dir.path(), "nolimhandback");
    let battery_state = Arc::new(AtomicU8::new(2)); // OnAc

    let w_lim = cluster.spawn_worker(
        "steady-box",
        2,
        Some(ep_lim.clone()),
        WorkerLimitsConfig {
            no_tasks_on_battery: Some(true),
            ..Default::default()
        },
        ProbeHandles {
            battery: Some(Arc::clone(&battery_state)),
            hold_ends_tasks: false,
            ..Default::default()
        },
    );
    cluster.wait_for_workers(1).await;

    let markers = tempfile::tempdir().expect("markers");
    let started = markers.path().join("steady.started");
    let release = markers.path().join("steady.release");
    let running_tid = cluster
        .master
        .submit_task(Task::new(
            gated_task_spec(&started, &release, "finished-while-held"),
            TaskRequirements::generic(1, 60).with_max_retries(0),
        ))
        .await
        .unwrap();
    wait_for_file(&started).await;
    poll_control_status(&ep_lim, |s| s.active_tasks == 1).await;

    // Trigger battery hold with `hold_ends_tasks = false`: active task MUST keep running!
    battery_state.store(1, Ordering::Release);
    poll_control_status(&ep_lim, |s| s.state == WorkerControlState::Held).await;
    let st = cluster.master.get_task_status(running_tid).await.unwrap();
    assert_eq!(st, TaskStatus::Running);

    // Submit a 2nd task: worker refuses it while active task is still running -> master enters paused.
    let second_tid = cluster
        .master
        .submit_task(echo_task("after-reconnect"))
        .await
        .unwrap();
    cluster.wait_until_paused(w_lim, true).await;

    // Lift the hold BEFORE the active task finishes. While `active_tasks == 1`, worker stays
    // Running locally and does not bounce the in-flight task, then reconnects as soon as idle!
    battery_state.store(2, Ordering::Release);
    poll_control_status(&ep_lim, |s| {
        s.state == WorkerControlState::Running && s.active_tasks == 1
    })
    .await;

    // Release active task: once it completes, worker must auto-reconnect, clear `paused`, and run `second_tid`!
    std::fs::write(&release, b"go").unwrap();
    let r1 = cluster
        .master
        .wait_task(running_tid, Some(WAIT_BOUND))
        .await
        .unwrap();
    assert_eq!(r1.exit_code, 0);
    assert!(r1.stdout_str().contains("finished-while-held"));

    cluster.wait_until_paused(w_lim, false).await;
    let r2 = cluster
        .master
        .wait_task(second_tid, Some(WAIT_BOUND))
        .await
        .unwrap();
    assert_eq!(r2.exit_code, 0);
    assert!(r2.stdout_str().contains("after-reconnect"));
}

#[tokio::test]
async fn net9b_live_limits_set_hours_cpu_daily_budget_and_manual_resume_while_held() {
    let mut cluster = Net9bCluster::new().await;
    let dir = tempfile::tempdir().expect("ipc dir");
    let ep_lim = unique_endpoint(dir.path(), "dyn");

    let cpu_bits = Arc::new(AtomicU32::new(15.0f32.to_bits()));
    // Fixed clock: Thu 2026-11-26 14:00:00
    let fixed_dt =
        NaiveDateTime::parse_from_str("2026-11-26 14:00:00", "%Y-%m-%d %H:%M:%S").unwrap();

    let w_lim = cluster.spawn_worker(
        "dyn-box",
        1,
        Some(ep_lim.clone()),
        WorkerLimitsConfig::default(),
        ProbeHandles {
            cpu_bits: Some(Arc::clone(&cpu_bits)),
            fixed_clock: Some(fixed_dt),
            ..Default::default()
        },
    );
    cluster.wait_for_workers(1).await;

    // 1. Live `limits set hours "mon-fri 18:00-08:00"` (outside 14:00 Thu) -> Held immediately!
    let resp = send_control_request(
        &ep_lim,
        &ControlRequest::LimitsSet {
            key: "hours".into(),
            value: "mon-fri 18:00-08:00".into(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(resp, ControlResponse::Ok { .. }));
    let snap = poll_control_status(&ep_lim, |s| s.state == WorkerControlState::Held).await;
    assert_eq!(snap.reason.as_deref(), Some("outside allowed hours"));

    // While paused AND held, manual Resume transitions to Held (not Running) without clearing hold!
    let _ = send_control_request(
        &ep_lim,
        &ControlRequest::Pause {
            until_epoch_ms: None,
            reason: Some("manual".into()),
        },
    )
    .await
    .unwrap();
    let resume_resp = send_control_request(&ep_lim, &ControlRequest::Resume)
        .await
        .unwrap();
    match resume_resp {
        ControlResponse::Ok { snapshot, .. } => {
            assert_eq!(snapshot.state, WorkerControlState::Held);
            assert_eq!(snapshot.reason.as_deref(), Some("outside allowed hours"));
        }
        other => panic!("unexpected resume response: {other:?}"),
    }

    // Clear hours -> back to Running.
    let _ = send_control_request(
        &ep_lim,
        &ControlRequest::LimitsSet {
            key: "hours".into(),
            value: "none".into(),
        },
    )
    .await
    .unwrap();
    poll_control_status(&ep_lim, |s| s.state == WorkerControlState::Running).await;

    // 2. Set `local_max_cpu_pct = 50` and raise fake CPU above threshold -> Held("host cpu above limit").
    let _ = send_control_request(
        &ep_lim,
        &ControlRequest::LimitsSet {
            key: "local_max_cpu_pct".into(),
            value: "50".into(),
        },
    )
    .await
    .unwrap();
    cpu_bits.store(90.0f32.to_bits(), Ordering::Release);
    let snap = poll_control_status(&ep_lim, |s| {
        s.state == WorkerControlState::Held && s.reason.as_deref() == Some("host cpu above limit")
    })
    .await;
    assert_eq!(snap.reason.as_deref(), Some("host cpu above limit"));

    // Lower fake CPU to 10% (poll/clear cap by unsetting CPU cap or waiting 30s window — here unset cap).
    let _ = send_control_request(
        &ep_lim,
        &ControlRequest::LimitsSet {
            key: "local_max_cpu_pct".into(),
            value: "none".into(),
        },
    )
    .await
    .unwrap();
    poll_control_status(&ep_lim, |s| s.state == WorkerControlState::Running).await;

    // 3. Complete a real task (>= 100 ms), then set `max_task_hours_per_day = 0.00001` (36 ms)
    //    -> `record_completed_task` has already recorded the runtime so Held("daily task-hours cap reached")!
    let tid = cluster
        .master
        .submit_task(Task::new(
            TaskSpec::BuiltinTest {
                test_name: "budget-check".into(),
                iterations: 1,
                duration_ms: 120,
                should_fail: false,
                require_gpu: false,
            },
            TaskRequirements::generic(1, 60).with_max_retries(0),
        ))
        .await
        .unwrap();
    let res = cluster
        .master
        .wait_task(tid, Some(WAIT_BOUND))
        .await
        .unwrap();
    assert_eq!(res.exit_code, 0);
    let _ = w_lim;

    let _ = send_control_request(
        &ep_lim,
        &ControlRequest::LimitsSet {
            key: "max_task_hours_per_day".into(),
            value: "0.00001".into(),
        },
    )
    .await
    .unwrap();
    let snap = poll_control_status(&ep_lim, |s| {
        s.state == WorkerControlState::Held
            && s.reason.as_deref() == Some("daily task-hours cap reached")
    })
    .await;
    assert_eq!(snap.reason.as_deref(), Some("daily task-hours cap reached"));
}
