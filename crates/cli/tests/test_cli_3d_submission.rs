use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::watch;

use rusty_grid_cli::cli_app::validate_glb_header;
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

const BIN: &str = env!("CARGO_BIN_EXE_rusty-grid");

/// Constructs a clean, isolated CLI command rooted in a temporary home directory.
/// Ensures developer's real ~/.oxideswarm is never read or written.
fn cli(home: &Path) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.env("HOME", home)
        .env("USERPROFILE", home)
        .env_remove("OXIDESWARM_HOME")
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("OXIDESWARM_SECRET_FILE")
        .env_remove("RUSTY_GRID_CONFIG")
        .env_remove("RUSTY_GRID_MASTER")
        .env_remove("RUSTY_GRID_MASTER_ADDR")
        .stdin(Stdio::null());
    cmd
}

#[tokio::test]
async fn test_cli_3d_sdf_submission_and_export() {
    let temp_dir = TempDir::new().unwrap();
    let out_glb_path = temp_dir.path().join("exported_cube.glb");

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

    // 3. Execute submit command via isolated child process
    let submit_future = tokio::task::spawn_blocking({
        let home = temp_dir.path().to_path_buf();
        let master_str = master_addr.to_string();
        let out_path = out_glb_path.clone();
        move || {
            cli(&home)
                .arg("submit")
                .arg("--master")
                .arg(&master_str)
                .arg("--type")
                .arg("3d")
                .arg("--prompt")
                .arg("a solid cube")
                .arg("--engine")
                .arg("sdf")
                .arg("--quality")
                .arg("fast")
                .arg("-o")
                .arg(&out_path)
                .arg("--wait")
                .status()
                .expect("run submit CLI")
        }
    });

    let status = tokio::time::timeout(Duration::from_secs(15), submit_future)
        .await
        .expect("SDF submit timed out!")
        .expect("Task join error");

    assert!(status.success(), "Submit CLI command must succeed");

    // 4. Verify output file exists and is valid glTF
    assert!(out_glb_path.is_file(), "Exported glTF file must exist");
    let data = std::fs::read(&out_glb_path).unwrap();
    assert!(data.len() > 1000);
    assert!(validate_glb_header(&data).is_ok());

    // 5. Cleanup
    let _ = tx.send(true);
    let _ = worker_handle.await;
    let _ = master.shutdown();
}

#[tokio::test]
async fn test_cli_autorig_submission_and_export() {
    let temp_dir = TempDir::new().unwrap();
    let input_mesh_path = temp_dir.path().join("input_cylinder.glb");
    let out_rigged_path = temp_dir.path().join("rigged_cylinder.glb");

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

    tokio::time::sleep(Duration::from_millis(200)).await;

    // 3. Execute submit command via isolated child process
    let submit_future = tokio::task::spawn_blocking({
        let home = temp_dir.path().to_path_buf();
        let master_str = master_addr.to_string();
        let in_mesh = input_mesh_path.clone();
        let out_path = out_rigged_path.clone();
        move || {
            cli(&home)
                .arg("submit")
                .arg("--master")
                .arg(&master_str)
                .arg("--type")
                .arg("autorig")
                .arg("--input-mesh")
                .arg(&in_mesh)
                .arg("--rig-preset")
                .arg("biped")
                .arg("--embed-animations")
                .arg("idle")
                .arg("--preferred-provider")
                .arg("builtin")
                .arg("-o")
                .arg(&out_path)
                .arg("--wait")
                .status()
                .expect("run autorig submit CLI")
        }
    });

    let status = tokio::time::timeout(Duration::from_secs(15), submit_future)
        .await
        .expect("Autorig submit timed out!")
        .expect("Task join error");

    assert!(status.success(), "Autorig submit CLI command must succeed");

    // 4. Verify output file exists and is valid glTF
    assert!(out_rigged_path.is_file(), "Rigged output asset must exist");
    let data = std::fs::read(&out_rigged_path).unwrap();
    assert!(data.len() > 1000);
    assert!(validate_glb_header(&data).is_ok());

    // 5. Cleanup
    let _ = tx.send(true);
    let _ = worker_handle.await;
    let _ = master.shutdown();
}

#[tokio::test]
async fn test_cli_negative_validations() {
    let temp_dir = TempDir::new().unwrap();

    // Non-existent image file
    let submit_image = tokio::task::spawn_blocking({
        let home = temp_dir.path().to_path_buf();
        move || {
            cli(&home)
                .arg("submit")
                .arg("--master")
                .arg("127.0.0.1:8080")
                .arg("--type")
                .arg("3d")
                .arg("--image")
                .arg("non_existent_image_12345.png")
                .arg("--wait")
                .output()
                .expect("run submit CLI")
        }
    });

    let output_image = tokio::time::timeout(Duration::from_secs(15), submit_image)
        .await
        .expect("Negative validation image timed out!")
        .expect("Task join error");
    assert!(!output_image.status.success());

    // Empty input mesh file
    let empty_mesh = temp_dir.path().join("empty.glb");
    std::fs::write(&empty_mesh, b"").unwrap();

    let submit_mesh = tokio::task::spawn_blocking({
        let home = temp_dir.path().to_path_buf();
        move || {
            cli(&home)
                .arg("submit")
                .arg("--master")
                .arg("127.0.0.1:8080")
                .arg("--type")
                .arg("autorig")
                .arg("--input-mesh")
                .arg(&empty_mesh)
                .arg("--wait")
                .output()
                .expect("run submit CLI")
        }
    });

    let output_mesh = tokio::time::timeout(Duration::from_secs(15), submit_mesh)
        .await
        .expect("Negative validation mesh timed out!")
        .expect("Task join error");
    assert!(!output_mesh.status.success());
}

#[tokio::test]
async fn test_cli_3d_default_engine_with_image_fails_before_submit() {
    let temp_dir = TempDir::new().unwrap();
    let img_path = temp_dir.path().join("input.png");
    std::fs::write(&img_path, b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDRtest").unwrap();

    // Default engine (auto) with --image must fail validation before any network attempt
    let submit = tokio::task::spawn_blocking({
        let home = temp_dir.path().to_path_buf();
        let img = img_path.clone();
        move || {
            cli(&home)
                .arg("submit")
                .arg("--master")
                .arg("127.0.0.1:9999")
                .arg("--type")
                .arg("3d")
                .arg("--prompt")
                .arg("robot")
                .arg("--image")
                .arg(&img)
                .arg("--wait")
                .output()
                .expect("run submit CLI")
        }
    });

    let output = tokio::time::timeout(Duration::from_secs(15), submit)
        .await
        .expect("Submit timed out!")
        .expect("Task join error");

    assert!(
        !output.status.success(),
        "--image with default/auto engine must fail fast before submission"
    );
    let combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains("Image input is only supported with neural 3D engines"),
        "Expected validation error, got: {combined}"
    );
}
