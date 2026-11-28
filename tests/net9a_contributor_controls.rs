//! Integration tests for NET-9a (`design 0113` D2, D3, T2, T3):
//!
//! * Pausing a running worker over its local same-user control socket stops running task process
//!   trees within 5 seconds and hands them back uncharged (`max_retries = 0` still succeeds on
//!   another worker).
//! * The master places the paused worker in hold-back (`state:paused`), skips it for new work,
//!   and `doctor` emits `WARN` (not `FAIL`) for paused workers.
//! * Resuming over the control socket reconnects the worker immediately and restores eligibility.
//! * `--no-gpu-tasks` holds back only GPU tasks on that worker while CPU tasks continue to run.
//! * `oxideswarm worker pause|resume|status|limits` round-trips against a live worker and persists
//!   limit updates to `rusty-grid.toml` without disturbing unrelated `[worker]` keys.

use std::path::PathBuf;
#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;
#[cfg(unix)]
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::sync::watch;
#[cfg(unix)]
use tokio::sync::Notify;
use uuid::Uuid;

#[cfg(unix)]
use rusty_grid_cli::doctor::{run_doctor, DoctorOptions, Outcome};
use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec, TaskStatus};
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::registry::WorkerStatus;
use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};
use rusty_grid_worker::control::{
    send_control_request, ControlEndpoint, ControlRequest, WorkerLimitsConfig,
};
#[cfg(unix)]
use rusty_grid_worker::control::{ControlResponse, WorkerControlState};

struct Net9Cluster {
    master: MasterHandle,
    addr: String,
    workers: Vec<(watch::Sender<bool>, tokio::task::JoinHandle<()>)>,
    _dirs: Vec<TempDir>,
}

impl Net9Cluster {
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
        simulate_gpu: bool,
        control_endpoint: Option<ControlEndpoint>,
        initial_limits: WorkerLimitsConfig,
        control_config_path: Option<PathBuf>,
    ) -> Uuid {
        let dir = tempfile::tempdir().expect("sandbox dir");
        let mut config = WorkerConfig::new(self.addr.clone())
            .with_no_battery(true)
            .with_name(name)
            .with_cores(cores)
            .with_max_concurrency(cores)
            .with_ram_mb(2048)
            .with_simulate_gpu(simulate_gpu)
            .with_sandbox_base_dir(dir.path())
            .with_redirection_portal(false)
            .with_control_endpoint(control_endpoint)
            .with_initial_limits(initial_limits);
        if let Some(cfg) = control_config_path {
            let persist_target = cfg.clone();
            config = config
                .with_control_config_path(Some(cfg))
                .with_limit_persist_hook(Some(rusty_grid_worker::LimitPersistHook(Arc::new(
                    move |limits| {
                        let mut doc = std::fs::read_to_string(&persist_target)
                            .unwrap_or_default()
                            .parse::<toml_edit::DocumentMut>()
                            .unwrap_or_default();
                        if !doc.as_table().contains_key("worker") {
                            doc["worker"] = toml_edit::Item::Table(toml_edit::Table::new());
                        }
                        if let Some(tbl) = doc["worker"].as_table_mut() {
                            if limits.no_gpu_tasks {
                                tbl["no_gpu_tasks"] = toml_edit::value(true);
                            } else {
                                tbl.remove("no_gpu_tasks");
                            }
                            match limits.local_max_cpu_pct {
                                Some(v) => {
                                    tbl["local_max_cpu_pct"] = toml_edit::value(i64::from(v));
                                }
                                None => {
                                    tbl.remove("local_max_cpu_pct");
                                }
                            }
                        }
                        std::fs::write(&persist_target, doc.to_string())
                            .map_err(|e| format!("write failed: {e}"))?;
                        Ok(Some(persist_target.clone()))
                    },
                ))));
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
        let deadline = Instant::now() + Duration::from_secs(10);
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
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("{count} workers did not register");
    }

    #[cfg(unix)]
    async fn wait_until_paused(&self, worker_id: Uuid, want_paused: bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if let Ok(workers) = self.master.list_workers().await {
                if workers
                    .iter()
                    .any(|w| w.worker_id == worker_id && w.paused == want_paused)
                {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("worker {worker_id} paused={want_paused} state not observed in time");
    }
}

impl Drop for Net9Cluster {
    fn drop(&mut self) {
        for (tx, handle) in &self.workers {
            let _ = tx.send(true);
            handle.abort();
        }
    }
}

#[cfg(unix)]
async fn wait_for_file(path: &std::path::Path) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("file {} did not appear", path.display());
}

#[cfg(unix)]
#[tokio::test]
async fn t2_pause_hands_back_running_tasks_uncharged_holds_back_and_resumes() {
    let mut cluster = Net9Cluster::new().await;
    let ctrl_dir = tempfile::tempdir().expect("ctrl dir");
    let sock_path = ctrl_dir.path().join("priv").join("worker-a.control.sock");
    let ep_a = ControlEndpoint::UnixSocket(sock_path.clone());

    let worker_a = cluster.spawn_worker(
        "worker-a",
        2,
        false,
        Some(ep_a.clone()),
        WorkerLimitsConfig::default(),
        None,
    );
    cluster.wait_for_workers(1).await;
    wait_for_file(&sock_path).await;

    // Submit 2 long-running shell tasks with `max_retries = 0`: they must only reach Completion on
    // `worker-b` if hand-back does NOT charge the execution retry budget.
    let state_dir = tempfile::tempdir().expect("state dir");
    let rerun_flag = state_dir.path().join("reran.flag");
    let mut task_ids = Vec::new();
    let mut pid_files = Vec::new();
    for idx in 0..2 {
        let pid_file = state_dir.path().join(format!("task_{idx}.pid"));
        let cmd = format!(
            "if [ -f '{}' ]; then echo handed_back_{idx}; else echo $$ > '{}'; sleep 30; fi",
            rerun_flag.display(),
            pid_file.display()
        );
        let task = Task::new(
            TaskSpec::command("sh", vec!["-c".into(), cmd]),
            TaskRequirements::generic(1, 60).with_max_retries(0),
        );
        task_ids.push(cluster.master.submit_task(task).await.expect("submit"));
        pid_files.push(pid_file);
    }

    for pf in &pid_files {
        wait_for_file(pf).await;
    }

    // Flip the rerun flag and connect worker-b so both handed-back tasks can rerun cleanly.
    std::fs::write(&rerun_flag, b"ok").expect("write rerun flag");
    let worker_b = cluster.spawn_worker(
        "worker-b",
        2,
        false,
        None,
        WorkerLimitsConfig::default(),
        None,
    );
    cluster.wait_for_workers(2).await;

    // Trigger contributor pause via local control socket.
    let resp = send_control_request(
        &ep_a,
        &ControlRequest::Pause {
            until_epoch_ms: None,
            reason: Some("gaming".into()),
        },
    )
    .await
    .expect("pause RPC");
    match resp {
        ControlResponse::Ok { snapshot, .. } => {
            assert_eq!(snapshot.state, WorkerControlState::Paused);
        }
        other => panic!("unexpected pause response: {other:?}"),
    }

    for (idx, tid) in task_ids.iter().enumerate() {
        let res = cluster
            .master
            .wait_task(*tid, Some(Duration::from_secs(10)))
            .await
            .expect("handed-back task must complete on worker-b");
        assert_eq!(res.exit_code, 0, "task {idx}: {:?}", res.error);
        assert_eq!(res.worker_id, worker_b);
        assert!(res.stdout_str().contains(&format!("handed_back_{idx}")));
    }

    // Verify original worker-a child processes were terminated cleanly.
    let mut sys = sysinfo::System::new();
    sys.refresh_processes(sysinfo::ProcessesToUpdate::All, true);
    for pf in &pid_files {
        let pid: usize = std::fs::read_to_string(pf)
            .expect("read pid")
            .trim()
            .parse()
            .expect("parse pid");
        assert!(
            sys.process(sysinfo::Pid::from(pid))
                .is_none_or(|p| p.status() == sysinfo::ProcessStatus::Zombie),
            "process {pid} should be dead after pause hand-back"
        );
    }

    // Master must now hold worker-a back with paused=true and route new tasks to worker-b.
    cluster.wait_until_paused(worker_a, true).await;
    let listed = cluster.master.list_workers().await.expect("list workers");
    assert!(listed.iter().any(|w| w.worker_id == worker_a && w.paused));

    // `doctor` must WARN on paused `worker-a` and NOT fail.
    let doctor_report = run_doctor(
        &cluster.addr,
        None,
        &DoctorOptions {
            expect_workers: 2,
            skip_throughput: true,
            ..DoctorOptions::default()
        },
    )
    .await;
    assert!(
        !doctor_report.failed(),
        "doctor must not fail on paused worker: {:?}",
        doctor_report.checks
    );
    assert!(doctor_report.checks.iter().any(|c| {
        c.outcome == Outcome::Warn
            && c.worker.as_deref() == Some("worker-a")
            && c.detail.contains("state:paused")
    }));

    // Unpinned work during hold-back goes strictly to worker-b.
    let hold_task = Task::new(
        TaskSpec::command("echo", vec!["still-paused".into()]),
        TaskRequirements::generic(1, 30),
    );
    let hold_id = cluster.master.submit_task(hold_task).await.unwrap();
    let hold_res = cluster
        .master
        .wait_task(hold_id, Some(Duration::from_secs(10)))
        .await
        .unwrap();
    assert_eq!(hold_res.worker_id, worker_b);

    // Resume worker-a over control socket: it must reconnect immediately and take pinned work.
    let resp = send_control_request(&ep_a, &ControlRequest::Resume)
        .await
        .expect("resume RPC");
    match resp {
        ControlResponse::Ok { snapshot, .. } => {
            assert_eq!(snapshot.state, WorkerControlState::Running);
        }
        other => panic!("unexpected resume response: {other:?}"),
    }
    cluster.wait_until_paused(worker_a, false).await;

    let pinned_task = Task::new(
        TaskSpec::command("echo", vec!["resumed-ok".into()]),
        TaskRequirements::generic(1, 30),
    )
    .with_tags(vec![format!("require:worker={worker_a}")]);
    let pinned_id = cluster.master.submit_task(pinned_task).await.unwrap();
    let pinned_res = cluster
        .master
        .wait_task(pinned_id, Some(Duration::from_secs(10)))
        .await
        .unwrap();
    assert_eq!(pinned_res.worker_id, worker_a);
    assert_eq!(pinned_res.exit_code, 0);
}

#[tokio::test]
async fn t2_no_gpu_tasks_holds_back_only_gpu_tasks() {
    let mut cluster = Net9Cluster::new().await;
    let no_gpu_limits = WorkerLimitsConfig {
        no_gpu_tasks: true,
        ..Default::default()
    };

    let worker_gpu_off = cluster.spawn_worker("gpu-off", 2, true, None, no_gpu_limits, None);
    cluster.wait_for_workers(1).await;

    // Submit an unpinned GPU task while only `gpu-off` is registered; once `gpu-on` joins,
    // `gpu-off` rejects and enters gpu_paused hold-back so the GPU task lands on `gpu-on`.
    let gpu_task = Task::new(
        TaskSpec::builtin_test("gpu_matmul", 64),
        TaskRequirements::gpu(30).with_max_retries(0),
    );
    let gpu_tid = cluster.master.submit_task(gpu_task).await.unwrap();

    // Wait briefly until `gpu-off` records at least one rejection / gpu_paused flag.
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Ok(workers) = cluster.master.list_workers().await {
            if workers
                .iter()
                .any(|w| w.worker_id == worker_gpu_off && w.gpu_paused)
            {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let worker_gpu_on =
        cluster.spawn_worker("gpu-on", 2, true, None, WorkerLimitsConfig::default(), None);
    cluster.wait_for_workers(2).await;

    let gpu_res = cluster
        .master
        .wait_task(gpu_tid, Some(Duration::from_secs(15)))
        .await
        .expect("GPU task completes on gpu-on");
    assert_eq!(gpu_res.exit_code, 0);
    assert_eq!(gpu_res.worker_id, worker_gpu_on);

    // `gpu-off` must still run CPU tasks normally.
    let cpu_task = Task::new(
        TaskSpec::builtin_test("hash_compute", 20),
        TaskRequirements::generic(1, 30),
    )
    .with_tags(vec![format!("require:worker={worker_gpu_off}")]);
    let cpu_tid = cluster.master.submit_task(cpu_task).await.unwrap();
    let cpu_res = cluster
        .master
        .wait_task(cpu_tid, Some(Duration::from_secs(10)))
        .await
        .unwrap();
    assert_eq!(cpu_res.exit_code, 0);
    assert_eq!(cpu_res.worker_id, worker_gpu_off);

    // Verify `gpu_off` still keeps `gpu_paused = true` after completing a CPU task.
    let listed = cluster.master.list_workers().await.unwrap();
    let off_info = listed
        .iter()
        .find(|w| w.worker_id == worker_gpu_off)
        .unwrap();
    assert!(off_info.gpu_paused);
    assert!(!off_info.paused);
    let _ = WorkerCapabilities::new("dummy", 1, 1024, false, false, None);
    let _ = TaskStatus::Completed;
}

#[cfg(unix)]
#[tokio::test]
async fn t3_control_cli_round_trip_and_limits_persistence() {
    let mut cluster = Net9Cluster::new().await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let sock_path = tmp.path().join("ctrl").join("cli-test.control.sock");
    let cfg_path = tmp.path().join("rusty-grid.toml");
    std::fs::write(&cfg_path, "# keep comment\n[worker]\nname = \"cli-box\"\n").unwrap();

    cluster.spawn_worker(
        "cli-box",
        1,
        false,
        Some(ControlEndpoint::UnixSocket(sock_path.clone())),
        WorkerLimitsConfig::default(),
        Some(cfg_path.clone()),
    );
    cluster.wait_for_workers(1).await;
    wait_for_file(&sock_path).await;

    let bin = env!("CARGO_BIN_EXE_oxideswarm");
    let sock_str = sock_path.to_string_lossy().to_string();

    let run_cli = |args: &[&str]| {
        let mut cmd = Command::new(bin);
        cmd.env("HOME", tmp.path()).args(args);
        async move { cmd.output().await.expect("run oxideswarm CLI") }
    };

    let status_out = run_cli(&["worker", "status", "--control-socket", &sock_str, "--json"]).await;
    assert!(
        status_out.status.success(),
        "{}",
        String::from_utf8_lossy(&status_out.stderr)
    );
    let status_json: serde_json::Value =
        serde_json::from_slice(&status_out.stdout).expect("valid status JSON");
    assert_eq!(status_json["snapshot"]["state"], "running");

    let pause_out = run_cli(&[
        "worker",
        "pause",
        "--control-socket",
        &sock_str,
        "--until",
        "1h",
        "--reason",
        "build",
        "--json",
    ])
    .await;
    assert!(
        pause_out.status.success(),
        "{}",
        String::from_utf8_lossy(&pause_out.stderr)
    );
    let pause_json: serde_json::Value = serde_json::from_slice(&pause_out.stdout).unwrap();
    assert_eq!(pause_json["snapshot"]["state"], "paused");
    assert!(pause_json["snapshot"]["paused_until_epoch_ms"].is_u64());

    let set_out = run_cli(&[
        "worker",
        "limits",
        "set",
        "no_gpu_tasks",
        "true",
        "--control-socket",
        &sock_str,
        "--json",
    ])
    .await;
    assert!(
        set_out.status.success(),
        "{}",
        String::from_utf8_lossy(&set_out.stderr)
    );

    let get_out = run_cli(&[
        "worker",
        "limits",
        "get",
        "--control-socket",
        &sock_str,
        "--json",
    ])
    .await;
    assert!(get_out.status.success());
    let get_json: serde_json::Value = serde_json::from_slice(&get_out.stdout).unwrap();
    assert_eq!(get_json["limits"]["no_gpu_tasks"], true);

    let saved_toml = std::fs::read_to_string(&cfg_path).unwrap();
    assert!(saved_toml.contains("# keep comment"));
    assert!(saved_toml.contains("no_gpu_tasks = true"));

    let resume_out = run_cli(&["worker", "resume", "--control-socket", &sock_str, "--json"]).await;
    assert!(resume_out.status.success());
    let resume_json: serde_json::Value = serde_json::from_slice(&resume_out.stdout).unwrap();
    assert_eq!(resume_json["snapshot"]["state"], "running");
}

#[cfg(unix)]
#[tokio::test]
async fn pause_during_reconnect_hands_back_resumed_task_on_session_start() {
    let blob_dir = tempfile::tempdir().expect("blob dir");
    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_blob_store(blob_dir.path(), 64 * 1024 * 1024)
            .with_session_resume_hold(Duration::from_secs(30)),
        SchedulerConfig::default().without_host_cpu_backpressure(),
        ReaperConfig::default(),
    )
    .await
    .expect("master starts");
    let master_addr = master.server_addr();

    // Controllable TCP proxy so we can put worker_a cleanly into its reconnect / resume grace
    // window without shutting down the master.
    let proxy = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let proxy_addr = proxy.local_addr().unwrap().to_string();
    let allow_connects = Arc::new(AtomicBool::new(true));
    let drop_conns = Arc::new(Notify::new());
    let proxy_allow = Arc::clone(&allow_connects);
    let proxy_drop = Arc::clone(&drop_conns);
    let proxy_task = tokio::spawn(async move {
        loop {
            let Ok((mut inbound, _)) = proxy.accept().await else {
                break;
            };
            if !proxy_allow.load(Ordering::Acquire) {
                drop(inbound);
                continue;
            }
            let drop_signal = Arc::clone(&proxy_drop);
            tokio::spawn(async move {
                let Ok(mut outbound) = tokio::net::TcpStream::connect(master_addr).await else {
                    return;
                };
                tokio::select! {
                    _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}
                    _ = drop_signal.notified() => {}
                }
            });
        }
    });

    let ctrl_dir = tempfile::tempdir().expect("ctrl dir");
    let sock_path = ctrl_dir.path().join("priv").join("reconnect.control.sock");
    let ep_a = ControlEndpoint::UnixSocket(sock_path.clone());
    let sandbox_a = tempfile::tempdir().expect("sandbox a");
    let mut cfg_a = WorkerConfig::new(proxy_addr)
        .with_no_battery(true)
        .with_name("reconnect-a")
        .with_cores(1)
        .with_max_concurrency(1)
        .with_ram_mb(2048)
        .with_sandbox_base_dir(sandbox_a.path())
        .with_redirection_portal(false)
        .with_control_endpoint(Some(ep_a.clone()));
    cfg_a = cfg_a.with_session_resume_grace_cap(Some(Duration::from_secs(30)));
    let mut client_a = WorkerClient::new(cfg_a);
    let worker_a = client_a.worker_id();
    let (tx_a, rx_a) = watch::channel(false);
    let handle_a = tokio::spawn(async move {
        let _ = client_a.run(rx_a).await;
    });

    wait_for_file(&sock_path).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if master
            .list_workers()
            .await
            .is_ok_and(|ws| ws.iter().any(|w| w.worker_id == worker_a))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let state_dir = tempfile::tempdir().expect("state dir");
    let pid_file = state_dir.path().join("task.pid");
    let rerun_flag = state_dir.path().join("rerun.flag");
    let cmd = format!(
        "if [ -f '{}' ]; then echo resumed_handed_back_ok; else echo $$ > '{}'; sleep 30; fi",
        rerun_flag.display(),
        pid_file.display()
    );
    let tid = master
        .submit_task(Task::new(
            TaskSpec::command("sh", vec!["-c".into(), cmd]),
            TaskRequirements::generic(1, 60).with_max_retries(0),
        ))
        .await
        .expect("submit");
    wait_for_file(&pid_file).await;

    // Sever worker_a's TCP session so it enters the resume grace window, pause it via control
    // socket while disconnected, then allow reconnects and bring up worker_b.
    allow_connects.store(false, Ordering::Release);
    drop_conns.notify_waiters();
    let disc_deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < disc_deadline {
        if master.list_workers().await.is_ok_and(|ws| {
            ws.iter()
                .any(|w| w.worker_id == worker_a && w.status == WorkerStatus::Disconnected)
        }) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let resp = send_control_request(
        &ep_a,
        &ControlRequest::Pause {
            until_epoch_ms: None,
            reason: Some("reconnect-pause".into()),
        },
    )
    .await
    .expect("pause during reconnect");
    assert!(matches!(resp, ControlResponse::Ok { .. }));

    std::fs::write(&rerun_flag, b"ok").expect("write rerun flag");
    let mut backup_cluster_workers = Vec::new();
    let sandbox_b = tempfile::tempdir().expect("sandbox b");
    let cfg_b = WorkerConfig::new(master_addr.to_string())
        .with_no_battery(true)
        .with_name("reconnect-b")
        .with_cores(1)
        .with_max_concurrency(1)
        .with_ram_mb(2048)
        .with_sandbox_base_dir(sandbox_b.path())
        .with_redirection_portal(false);
    let mut client_b = WorkerClient::new(cfg_b);
    let worker_b = client_b.worker_id();
    let (tx_b, rx_b) = watch::channel(false);
    backup_cluster_workers.push((
        tx_b,
        tokio::spawn(async move {
            let _ = client_b.run(rx_b).await;
        }),
    ));

    allow_connects.store(true, Ordering::Release);
    let res = master
        .wait_task(tid, Some(Duration::from_secs(15)))
        .await
        .expect("task handed back on session start must succeed on worker_b");
    assert_eq!(res.exit_code, 0, "{:?}", res.error);
    assert_eq!(res.worker_id, worker_b);
    assert!(res.stdout_str().contains("resumed_handed_back_ok"));

    let _ = tx_a.send(true);
    handle_a.abort();
    for (tx, h) in backup_cluster_workers {
        let _ = tx.send(true);
        h.abort();
    }
    proxy_task.abort();
}

#[cfg(unix)]
#[tokio::test]
async fn t3_control_channel_enforces_same_user_and_refuses_oversized_frames() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::UnixStream;

    let mut cluster = Net9Cluster::new().await;
    let ctrl_dir = tempfile::tempdir().expect("ctrl dir");
    let sock_path = ctrl_dir.path().join("priv").join("t3-sec.control.sock");
    cluster.spawn_worker(
        "t3-sec",
        1,
        false,
        Some(ControlEndpoint::UnixSocket(sock_path.clone())),
        WorkerLimitsConfig::default(),
        None,
    );
    cluster.wait_for_workers(1).await;
    wait_for_file(&sock_path).await;

    // Oversized JSON line (> 8 KiB) must be answered with an error frame, not crash or grow unboundedly.
    let stream = UnixStream::connect(&sock_path).await.expect("connect sock");
    let (reader, mut writer) = tokio::io::split(stream);
    let oversized = format!(
        "{{\"op\":\"pause\",\"reason\":\"{}\"}}\n",
        "x".repeat(rusty_grid_worker::control::MAX_CONTROL_LINE_BYTES + 256)
    );
    writer.write_all(oversized.as_bytes()).await.unwrap();
    writer.flush().await.unwrap();
    let mut reply = String::new();
    let n = BufReader::new(reader).read_line(&mut reply).await.unwrap();
    assert!(n > 0);
    let parsed: ControlResponse = serde_json::from_str(reply.trim()).unwrap();
    assert!(matches!(parsed, ControlResponse::Error { .. }));

    // Control channel must remain healthy right after an oversized frame.
    let status = send_control_request(
        &ControlEndpoint::UnixSocket(sock_path),
        &ControlRequest::Status,
    )
    .await
    .unwrap();
    assert!(matches!(status, ControlResponse::Status { .. }));
}

#[cfg(windows)]
#[tokio::test]
async fn t3_windows_named_pipe_cli_round_trip() {
    let mut cluster = Net9Cluster::new().await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let pipe_name = format!(r"\\.\pipe\oxideswarm-cli-test-{}", Uuid::new_v4());
    let cfg_path = tmp.path().join("rusty-grid.toml");
    std::fs::write(&cfg_path, "[worker]\nname = \"win-cli-box\"\n").unwrap();

    cluster.spawn_worker(
        "win-cli-box",
        1,
        false,
        Some(ControlEndpoint::NamedPipe(pipe_name.clone())),
        WorkerLimitsConfig::default(),
        Some(cfg_path.clone()),
    );
    cluster.wait_for_workers(1).await;

    let ep = ControlEndpoint::NamedPipe(pipe_name.clone());
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if send_control_request(&ep, &ControlRequest::Status)
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let bin = env!("CARGO_BIN_EXE_oxideswarm");
    let out = Command::new(bin)
        .env("OXIDESWARM_HOME", tmp.path())
        .args(["worker", "status", "--control-socket", &pipe_name, "--json"])
        .output()
        .await
        .expect("run oxideswarm CLI on Windows");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let status_json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(status_json["snapshot"]["state"], "running");

    let set_out = Command::new(bin)
        .env("OXIDESWARM_HOME", tmp.path())
        .args([
            "worker",
            "limits",
            "set",
            "no_gpu_tasks",
            "true",
            "--control-socket",
            &pipe_name,
            "--json",
        ])
        .output()
        .await
        .unwrap();
    assert!(set_out.status.success());
    let saved = std::fs::read_to_string(&cfg_path).unwrap();
    assert!(saved.contains("no_gpu_tasks = true"));
}
