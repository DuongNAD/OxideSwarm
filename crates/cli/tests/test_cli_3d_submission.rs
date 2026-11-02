use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;
use tokio::process::Command;
use tokio::sync::watch;

use rusty_grid_cli::cli_app::validate_glb_header;
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

const BIN: &str = env!("CARGO_BIN_EXE_rusty-grid");

fn cli_command(home: &Path) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.env("HOME", home)
        .env("USERPROFILE", home)
        .env("OXIDESWARM_HOME", home.join(".oxideswarm"))
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
        .env("NO_COLOR", "1")
        .current_dir(home);
    cmd
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cli_3d_sdf_submission_and_export() {
    let temp_home = TempDir::new().expect("create isolated test home");
    let out_glb_path = temp_home.path().join("exported_cube.glb");

    // 1. Spawn in-process Master
    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()),
        SchedulerConfig::default(),
        ReaperConfig::default(),
    )
    .await
    .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr();

    // 2. Spawn in-process Worker
    let worker_temp = TempDir::new().unwrap();
    let worker_cfg = WorkerConfig::new(master_addr.to_string())
        .with_name("worker-3d")
        .with_cores(4)
        .with_ram_mb(8192)
        .with_sandbox_base_dir(worker_temp.path());

    let mut client = WorkerClient::new(worker_cfg);
    let (tx, rx) = watch::channel(false);
    let worker_handle = tokio::spawn(async move {
        let _ = client.run(rx).await;
    });

    // Wait briefly for worker registration
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 3. Execute 3D Generation via spawned CLI binary
    let output = cli_command(temp_home.path())
        .arg("submit")
        .arg("--master")
        .arg(master_addr.to_string())
        .arg("--type")
        .arg("3d")
        .arg("--prompt")
        .arg("a solid cube")
        .arg("--quality")
        .arg("fast")
        .arg("--engine")
        .arg("sdf")
        .arg("--export-texture")
        .arg("--wait")
        .arg("--json")
        .arg("-o")
        .arg(&out_glb_path)
        .output()
        .await
        .expect("spawn submit command");

    assert!(
        output.status.success(),
        "CLI submit failed (exit {:?}): {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );

    // 4. Verify output file exists and is valid glTF
    assert!(out_glb_path.is_file());
    let data = std::fs::read(&out_glb_path).unwrap();
    assert!(data.len() > 1000);
    assert!(validate_glb_header(&data).is_ok());

    // 5. Cleanup
    let _ = tx.send(true);
    let _ = worker_handle.await;
    let _ = master.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cli_autorig_submission_and_export() {
    let temp_home = TempDir::new().expect("create isolated test home");
    let input_mesh_path = temp_home.path().join("input_cylinder.glb");
    let out_rigged_path = temp_home.path().join("rigged_cylinder.glb");

    // Generate input mesh
    let gen_spec = rusty_grid_core::task::Generative3DSpec::new_prompt("cylinder".to_string())
        .with_engine("sdf")
        .with_quality("fast");
    let (mesh_bytes, _) = rusty_grid_worker::gen3d::generate_procedural_3d(&gen_spec).unwrap();
    std::fs::write(&input_mesh_path, &mesh_bytes).unwrap();

    // 1. Spawn in-process Master
    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()),
        SchedulerConfig::default(),
        ReaperConfig::default(),
    )
    .await
    .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr();

    // 2. Spawn in-process Worker
    let worker_temp = TempDir::new().unwrap();
    let worker_cfg = WorkerConfig::new(master_addr.to_string())
        .with_name("worker-autorig")
        .with_cores(4)
        .with_ram_mb(8192)
        .with_sandbox_base_dir(worker_temp.path());

    let mut client = WorkerClient::new(worker_cfg);
    let (tx, rx) = watch::channel(false);
    let worker_handle = tokio::spawn(async move {
        let _ = client.run(rx).await;
    });

    // Wait briefly for worker registration
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 3. Execute Auto-Rig via spawned CLI binary
    let output = cli_command(temp_home.path())
        .arg("submit")
        .arg("--master")
        .arg(master_addr.to_string())
        .arg("--type")
        .arg("autorig")
        .arg("--input-mesh")
        .arg(&input_mesh_path)
        .arg("--rig-preset")
        .arg("biped")
        .arg("--embed-animations")
        .arg("idle")
        .arg("--preferred-provider")
        .arg("builtin")
        .arg("--wait")
        .arg("--json")
        .arg("-o")
        .arg(&out_rigged_path)
        .output()
        .await
        .expect("spawn autorig submit command");

    assert!(
        output.status.success(),
        "CLI autorig submit failed (exit {:?}): {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );

    // 4. Verify output file exists and is valid glTF
    assert!(out_rigged_path.is_file());
    let data = std::fs::read(&out_rigged_path).unwrap();
    assert!(data.len() > 1000);
    assert!(validate_glb_header(&data).is_ok());

    // 5. Cleanup
    let _ = tx.send(true);
    let _ = worker_handle.await;
    let _ = master.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cli_negative_validations() {
    let temp_home = TempDir::new().expect("create isolated test home");

    // Non-existent image file
    let output1 = cli_command(temp_home.path())
        .arg("submit")
        .arg("--master")
        .arg("127.0.0.1:8080")
        .arg("--type")
        .arg("3d")
        .arg("--image")
        .arg("non_existent_image_12345.png")
        .arg("--wait")
        .output()
        .await
        .expect("spawn submit command");
    assert!(
        !output1.status.success(),
        "Submission with non-existent image must fail"
    );

    // Empty input mesh file
    let empty_mesh = temp_home.path().join("empty.glb");
    std::fs::write(&empty_mesh, b"").unwrap();

    let output2 = cli_command(temp_home.path())
        .arg("submit")
        .arg("--master")
        .arg("127.0.0.1:8080")
        .arg("--type")
        .arg("autorig")
        .arg("--input-mesh")
        .arg(&empty_mesh)
        .arg("--wait")
        .output()
        .await
        .expect("spawn autorig submit command");
    assert!(
        !output2.status.success(),
        "Submission with empty input mesh must fail"
    );
}
