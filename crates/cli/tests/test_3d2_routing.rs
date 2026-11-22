#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use serde_json::Value;
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::sync::watch;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::{
    MasterMessage, MessageTransport, ProtocolCapabilities, WorkerMessage, PROTOCOL_VERSION_CURRENT,
};
use rusty_grid_core::task::TaskId;
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{MasterServer, ServerConfig};

const BIN: &str = env!("CARGO_BIN_EXE_oxideswarm");

struct ChildGuard(Option<Child>);

impl ChildGuard {
    fn wait_with_output(mut self) -> std::io::Result<Output> {
        let child = self.0.take().expect("child already taken");
        child.wait_with_output()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn setup_fake_pmset() -> TempDir {
    let dir = TempDir::new().unwrap();
    let pmset = dir.path().join("pmset");
    std::fs::write(&pmset, "#!/bin/sh\necho \"Now drawing from 'AC Power'\"\n").unwrap();
    let mut perms = std::fs::metadata(&pmset).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&pmset, perms).unwrap();
    dir
}

fn cli_cmd(home: &Path, tmpdir: &Path, fake_bin: &Path) -> Command {
    let base_path = std::env::var("PATH").unwrap_or_default();
    let new_path = format!("{}:{base_path}", fake_bin.display());
    let mut cmd = Command::new(BIN);
    cmd.env("HOME", home)
        .env("USERPROFILE", home)
        .env("OXIDESWARM_HOME", home.join(".oxideswarm"))
        .env("TMPDIR", tmpdir)
        .env("PATH", new_path)
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("OXIDESWARM_SECRET_FILE")
        .env_remove("RUSTY_GRID_SECRET")
        .env_remove("RUSTY_GRID_CONFIG")
        .env_remove("RUSTY_GRID_MASTER")
        .env_remove("RUSTY_GRID_MASTER_ADDR")
        .env_remove("RUSTY_GRID_INSECURE")
        .env_remove("RUSTY_GRID_WORKER_NAME")
        .env_remove("RUSTY_GRID_NAME")
        .env_remove("RUSTY_GRID_WORKER_ID_FILE")
        .env("RUSTY_GRID_DISABLE_DISCOVERY", "1")
        .env("RUSTY_GRID_DISABLE_WEB_UI", "1")
        .env("RUSTY_GRID_NO_BATTERY", "1")
        .env("NO_COLOR", "1")
        .current_dir(home)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd
}

async fn run_cli(home: &Path, tmpdir: &Path, fake_bin: &Path, args: &[&str]) -> Output {
    let mut cmd = cli_cmd(home, tmpdir, fake_bin);
    cmd.args(args);
    let fut = tokio::task::spawn_blocking(move || {
        let child = ChildGuard(Some(cmd.spawn().expect("spawn oxideswarm")));
        child.wait_with_output().expect("wait oxideswarm")
    });
    tokio::time::timeout(Duration::from_secs(15), fut)
        .await
        .expect("oxideswarm CLI timed out")
        .expect("join CLI task")
}

struct MockWorkerHandle {
    worker_id: Uuid,
    assigned_count: Arc<AtomicUsize>,
    shutdown_tx: watch::Sender<bool>,
    join: tokio::task::JoinHandle<()>,
}

impl MockWorkerHandle {
    async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.join.await;
    }
}

async fn spawn_mock_worker(
    addr: std::net::SocketAddr,
    caps: WorkerCapabilities,
) -> MockWorkerHandle {
    let worker_id = Uuid::new_v4();
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut transport = MessageTransport::new(stream);
    transport
        .send_msg(&WorkerMessage::Register {
            worker_id,
            capabilities: caps,
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
        })
        .await
        .unwrap();
    match transport.recv_msg::<MasterMessage>().await.unwrap() {
        Some(MasterMessage::RegisterAck { accepted: true, .. }) => {}
        other => panic!("expected RegisterAck, got {other:?}"),
    }

    let assigned_count = Arc::new(AtomicUsize::new(0));
    let assigned_ref = Arc::clone(&assigned_count);
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    let join = tokio::spawn(async move {
        let mut hb = tokio::time::interval(Duration::from_millis(500));
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
                _ = hb.tick() => {
                    let _ = transport.send_msg(&WorkerMessage::Heartbeat {
                        worker_id,
                        timestamp: 0,
                        active_tasks: 0,
                        cpu_usage_pct: 5.0,
                        ram_available_mb: 8192,
                    }).await;
                }
                msg = transport.recv_msg::<MasterMessage>() => {
                    let Ok(Some(msg)) = msg else { break; };
                    let task_id: Option<TaskId> = match msg {
                        MasterMessage::AssignTask { task } => Some(task.id),
                        MasterMessage::AssignTaskWithCheckpoint { task, .. } => Some(task.id),
                        _ => None,
                    };
                    if let Some(tid) = task_id {
                        assigned_ref.fetch_add(1, Ordering::SeqCst);
                        let _ = transport.send_msg(&WorkerMessage::TaskResult {
                            worker_id,
                            task_id: tid,
                            exit_code: 0,
                            stdout: b"ok".to_vec().into(),
                            stderr: Vec::new().into(),
                            execution_time_ms: 5,
                            is_gpu_executed: false,
                            device_name: Some("MockWorker".into()),
                            error: None,
                            asset_bytes: None,
                            artifacts: Vec::new(),
                            outputs: Vec::new(),
                        }).await;
                    }
                }
            }
        }
    });

    MockWorkerHandle {
        worker_id,
        assigned_count,
        shutdown_tx,
        join,
    }
}

#[tokio::test]
async fn cli_autorig_blender_routes_only_to_blender_worker_and_warns_when_absent() {
    let home = TempDir::new().unwrap();
    let tmp = TempDir::new().unwrap();
    let fake_bin = setup_fake_pmset();

    let mesh_path = home.path().join("mesh.glb");
    std::fs::write(&mesh_path, b"glTF\x02\x00\x00\x00\x0c\x00\x00\x00").unwrap();
    let mesh_str = mesh_path.to_string_lossy().to_string();

    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_discovery_port(0),
        SchedulerConfig::default().without_host_cpu_backpressure(),
        ReaperConfig::default(),
    )
    .await
    .unwrap();
    let addr = master.server_addr();
    let addr_str = addr.to_string();

    // 1. Only a non-Blender worker connected -> preflight warns on stderr, task stays queued.
    let no_blender = spawn_mock_worker(
        addr,
        WorkerCapabilities::new("no-blender", 4, 8192, false, false, None)
            .with_blender(false, None),
    )
    .await;

    let warn_out = run_cli(
        home.path(),
        tmp.path(),
        fake_bin.path(),
        &[
            "submit",
            "--master",
            &addr_str,
            "--type",
            "autorig",
            "--input-mesh",
            &mesh_str,
            "--preferred-provider",
            "blender",
        ],
    )
    .await;
    assert!(
        warn_out.status.success(),
        "fire-and-forget submit should succeed"
    );
    let warn_stderr = String::from_utf8_lossy(&warn_out.stderr);
    assert!(
        warn_stderr.contains("Warning: no connected worker has Blender available"),
        "expected Blender preflight warning on stderr, got: {warn_stderr}"
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        no_blender.assigned_count.load(Ordering::SeqCst),
        0,
        "non-Blender worker must never receive queued blender task"
    );

    // 2. Connect a Blender worker -> new --wait --json submit lands strictly on Blender worker,
    //    with zero Blender warning on stderr.
    let blender = spawn_mock_worker(
        addr,
        WorkerCapabilities::new("has-blender", 4, 8192, false, false, None)
            .with_blender(true, Some("4.2.0".into())),
    )
    .await;

    let wait_out = run_cli(
        home.path(),
        tmp.path(),
        fake_bin.path(),
        &[
            "submit",
            "--master",
            &addr_str,
            "--type",
            "autorig",
            "--input-mesh",
            &mesh_str,
            "--preferred-provider",
            "blender",
            "--wait",
            "--json",
        ],
    )
    .await;
    assert!(
        wait_out.status.success(),
        "submit --wait must succeed once Blender worker connects; stderr={}",
        String::from_utf8_lossy(&wait_out.stderr)
    );
    let wait_stderr = String::from_utf8_lossy(&wait_out.stderr);
    assert!(
        !wait_stderr.contains("Warning: no connected worker has Blender available"),
        "must not warn when a Blender worker is connected; got: {wait_stderr}"
    );
    let json: Value = serde_json::from_slice(&wait_out.stdout).expect("valid JSON output");
    assert_eq!(
        json.get("worker_id").and_then(Value::as_str),
        Some(blender.worker_id.to_string().as_str())
    );
    assert_eq!(
        no_blender.assigned_count.load(Ordering::SeqCst),
        0,
        "non-Blender worker must never receive any blender task"
    );

    blender.shutdown().await;
    no_blender.shutdown().await;
    let _ = master.shutdown();
}

#[tokio::test]
async fn cli_gen3d_high_skips_2gb_worker_lands_on_8gb_and_warns_when_absent() {
    let home = TempDir::new().unwrap();
    let tmp = TempDir::new().unwrap();
    let fake_bin = setup_fake_pmset();

    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_discovery_port(0),
        SchedulerConfig::default().without_host_cpu_backpressure(),
        ReaperConfig::default(),
    )
    .await
    .unwrap();
    let addr = master.server_addr();
    let addr_str = addr.to_string();

    // 1. Only a 2 GB GPU worker is connected -> preflight warns >= 6144 MB VRAM, and 2 GB worker
    //    never gets the `high` task.
    let gpu_2g = spawn_mock_worker(
        addr,
        WorkerCapabilities::new("gpu-2g", 4, 8192, true, false, None).with_vram_mb(Some(2048)),
    )
    .await;

    let warn_out = run_cli(
        home.path(),
        tmp.path(),
        fake_bin.path(),
        &[
            "submit",
            "--master",
            &addr_str,
            "--type",
            "3d",
            "--prompt",
            "robot",
            "--engine",
            "triposr",
            "--quality",
            "high",
        ],
    )
    .await;
    assert!(warn_out.status.success());
    let warn_stderr = String::from_utf8_lossy(&warn_out.stderr);
    assert!(
        warn_stderr.contains("Warning: no connected GPU worker advertises >= 6144 MB VRAM"),
        "expected 6144 MB VRAM preflight warning, got: {warn_stderr}"
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        gpu_2g.assigned_count.load(Ordering::SeqCst),
        0,
        "2 GB worker must never receive a high-quality 3D task"
    );

    // 2. Connect an 8 GB GPU worker -> new `--quality high --wait --json` lands on 8 GB worker.
    let gpu_8g = spawn_mock_worker(
        addr,
        WorkerCapabilities::new("gpu-8g", 4, 16384, true, false, None).with_vram_mb(Some(8192)),
    )
    .await;

    let wait_out = run_cli(
        home.path(),
        tmp.path(),
        fake_bin.path(),
        &[
            "submit",
            "--master",
            &addr_str,
            "--type",
            "3d",
            "--prompt",
            "robot",
            "--engine",
            "triposr",
            "--quality",
            "high",
            "--wait",
            "--json",
        ],
    )
    .await;
    assert!(
        wait_out.status.success(),
        "submit --quality high --wait must succeed on 8 GB worker; stderr={}",
        String::from_utf8_lossy(&wait_out.stderr)
    );
    let wait_stderr = String::from_utf8_lossy(&wait_out.stderr);
    assert!(
        !wait_stderr.contains("MB VRAM"),
        "must not emit VRAM warning when 8 GB worker is connected; got: {wait_stderr}"
    );
    let json: Value = serde_json::from_slice(&wait_out.stdout).expect("valid JSON output");
    assert_eq!(
        json.get("worker_id").and_then(Value::as_str),
        Some(gpu_8g.worker_id.to_string().as_str())
    );
    assert_eq!(
        gpu_2g.assigned_count.load(Ordering::SeqCst),
        0,
        "2 GB worker must never receive high-quality task"
    );

    gpu_8g.shutdown().await;
    gpu_2g.shutdown().await;
    let _ = master.shutdown();
}

#[tokio::test]
async fn cli_gen3d_cpu_defaults_emit_zero_false_vram_warnings_and_run_on_cpu_worker() {
    let home = TempDir::new().unwrap();
    let tmp = TempDir::new().unwrap();
    let fake_bin = setup_fake_pmset();

    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_discovery_port(0),
        SchedulerConfig::default().without_host_cpu_backpressure(),
        ReaperConfig::default(),
    )
    .await
    .unwrap();
    let addr = master.server_addr();
    let addr_str = addr.to_string();

    let cpu = spawn_mock_worker(
        addr,
        WorkerCapabilities::new("cpu-only", 4, 8192, false, false, None),
    )
    .await;

    for args in [
        vec![
            "submit", "--master", &addr_str, "--prompt", "robot", "--wait",
        ],
        vec![
            "submit",
            "--master",
            &addr_str,
            "--type",
            "3d",
            "--prompt",
            "robot",
            "--engine",
            "sdf",
            "--quality",
            "high",
            "--wait",
        ],
        vec![
            "submit", "--master", &addr_str, "--type", "3d", "--prompt", "robot", "--engine",
            "triposr", "--wait",
        ],
    ] {
        let out = run_cli(home.path(), tmp.path(), fake_bin.path(), &args).await;
        assert!(
            out.status.success(),
            "CPU default submit {args:?} failed; stderr={}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            !stderr.contains("MB VRAM") && !stderr.contains("Blender"),
            "unexpected preflight warning for {args:?}: {stderr}"
        );
    }

    assert_eq!(cpu.assigned_count.load(Ordering::SeqCst), 3);

    cpu.shutdown().await;
    let _ = master.shutdown();
}

#[cfg(feature = "3d")]
#[tokio::test]
async fn cli_real_worker_autorig_blender_routing_with_fake_blender() {
    use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

    let home = TempDir::new().unwrap();
    let tmp = TempDir::new().unwrap();
    let fake_bin = setup_fake_pmset();

    // Fake blender binary that copies --input to --output.
    let fake_blender_dir = TempDir::new().unwrap();
    let fake_blender = fake_blender_dir.path().join("fake_blender.sh");
    std::fs::write(
        &fake_blender,
        "#!/bin/sh\nin_p=\"\"\nout_p=\"\"\nwhile [ \"$#\" -gt 0 ]; do\n  case \"$1\" in\n    --input) in_p=\"$2\"; shift 2 ;;\n    --output) out_p=\"$2\"; shift 2 ;;\n    *) shift ;;\n  esac\ndone\ncp \"$in_p\" \"$out_p\"\n",
    )
    .unwrap();
    let mut perms = std::fs::metadata(&fake_blender).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&fake_blender, perms).unwrap();

    // Generate valid input GLB using SDF.
    let gen_spec = rusty_grid_core::task::Generative3DSpec::new_prompt("cylinder")
        .with_engine("sdf")
        .with_quality("fast");
    let (mesh_bytes, _) = rusty_grid_worker::gen3d::generate_procedural_3d(&gen_spec).unwrap();
    let input_mesh = home.path().join("input.glb");
    std::fs::write(&input_mesh, &mesh_bytes).unwrap();
    let input_str = input_mesh.to_string_lossy().to_string();

    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_discovery_port(0),
        SchedulerConfig::default().without_host_cpu_backpressure(),
        ReaperConfig::default(),
    )
    .await
    .unwrap();
    let addr = master.server_addr().to_string();

    let sb_no = TempDir::new().unwrap();
    let mut c_no = WorkerClient::new(
        WorkerConfig::new(addr.clone())
            .with_no_battery(true)
            .with_name("worker-no-blender")
            .with_cores(2)
            .with_ram_mb(4096)
            .with_no_blender(true)
            .with_sandbox_base_dir(sb_no.path()),
    );
    let (tx_no, rx_no) = watch::channel(false);
    let h_no = tokio::spawn(async move {
        let _ = c_no.run(rx_no).await;
    });

    let sb_yes = TempDir::new().unwrap();
    let mut c_yes = WorkerClient::new(
        WorkerConfig::new(addr.clone())
            .with_no_battery(true)
            .with_name("worker-blender")
            .with_cores(2)
            .with_ram_mb(4096)
            .with_blender(true)
            .with_blender_version("4.2.0")
            .with_blender_path(fake_blender.to_string_lossy().into_owned())
            .with_sandbox_base_dir(sb_yes.path()),
    );
    let (tx_yes, rx_yes) = watch::channel(false);
    let h_yes = tokio::spawn(async move {
        let _ = c_yes.run(rx_yes).await;
    });

    // Wait for both workers to register.
    for _ in 0..50 {
        if master.list_workers().await.map(|w| w.len()).unwrap_or(0) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let out_glb = home.path().join("rigged.glb");
    let out_str = out_glb.to_string_lossy().to_string();
    let out = run_cli(
        home.path(),
        tmp.path(),
        fake_bin.path(),
        &[
            "submit",
            "--master",
            &addr,
            "--type",
            "autorig",
            "--input-mesh",
            &input_str,
            "--preferred-provider",
            "blender",
            "-o",
            &out_str,
            "--wait",
            "--json",
        ],
    )
    .await;
    assert!(
        out.status.success(),
        "real WorkerClient autorig blender submit failed; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let json: Value = serde_json::from_slice(&out.stdout).expect("valid JSON output");
    assert_eq!(
        json.get("worker_name").and_then(Value::as_str),
        Some("worker-blender")
    );
    assert!(out_glb.is_file());

    let _ = tx_no.send(true);
    let _ = tx_yes.send(true);
    let _ = h_no.await;
    let _ = h_yes.await;
    let _ = master.shutdown();
}
