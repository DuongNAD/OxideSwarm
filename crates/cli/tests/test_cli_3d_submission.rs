use std::process::ExitCode;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::watch;

use rusty_grid_cli::cli_app::{run_submit, validate_glb_header, SubmitArgs};
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

static TEST_ENV_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[allow(dead_code)]
struct TestEnvGuard {
    _lock: tokio::sync::MutexGuard<'static, ()>,
    temp_dir: TempDir,
    orig_home: Option<std::ffi::OsString>,
    orig_userprofile: Option<std::ffi::OsString>,
    orig_oxideswarm_home: Option<std::ffi::OsString>,
    orig_secret: Option<std::ffi::OsString>,
    orig_secret_file: Option<std::ffi::OsString>,
    orig_config: Option<std::ffi::OsString>,
}

#[allow(dead_code)]
impl TestEnvGuard {
    async fn isolate() -> Self {
        let lock = TEST_ENV_MUTEX.lock().await;
        let temp_dir = TempDir::new().expect("create test temp dir");
        let orig_home = std::env::var_os("HOME");
        let orig_userprofile = std::env::var_os("USERPROFILE");
        let orig_oxideswarm_home = std::env::var_os("OXIDESWARM_HOME");
        let orig_secret = std::env::var_os("OXIDESWARM_SECRET");
        let orig_secret_file = std::env::var_os("OXIDESWARM_SECRET_FILE");
        let orig_config = std::env::var_os("RUSTY_GRID_CONFIG");

        std::env::set_var("HOME", temp_dir.path());
        std::env::set_var("USERPROFILE", temp_dir.path());
        std::env::set_var("OXIDESWARM_HOME", temp_dir.path().join(".oxideswarm"));
        std::env::remove_var("OXIDESWARM_SECRET");
        std::env::remove_var("OXIDESWARM_SECRET_FILE");
        std::env::remove_var("RUSTY_GRID_CONFIG");

        Self {
            _lock: lock,
            temp_dir,
            orig_home,
            orig_userprofile,
            orig_oxideswarm_home,
            orig_secret,
            orig_secret_file,
            orig_config,
        }
    }

    fn path(&self) -> &std::path::Path {
        self.temp_dir.path()
    }
}

impl Drop for TestEnvGuard {
    fn drop(&mut self) {
        match &self.orig_home {
            Some(v) => std::env::set_var("HOME", v),
            None => std::env::remove_var("HOME"),
        }
        match &self.orig_userprofile {
            Some(v) => std::env::set_var("USERPROFILE", v),
            None => std::env::remove_var("USERPROFILE"),
        }
        match &self.orig_oxideswarm_home {
            Some(v) => std::env::set_var("OXIDESWARM_HOME", v),
            None => std::env::remove_var("OXIDESWARM_HOME"),
        }
        match &self.orig_secret {
            Some(v) => std::env::set_var("OXIDESWARM_SECRET", v),
            None => std::env::remove_var("OXIDESWARM_SECRET"),
        }
        match &self.orig_secret_file {
            Some(v) => std::env::set_var("OXIDESWARM_SECRET_FILE", v),
            None => std::env::remove_var("OXIDESWARM_SECRET_FILE"),
        }
        match &self.orig_config {
            Some(v) => std::env::set_var("RUSTY_GRID_CONFIG", v),
            None => std::env::remove_var("RUSTY_GRID_CONFIG"),
        }
    }
}

#[tokio::test]
async fn test_cli_3d_sdf_submission_and_export() {
    let env_guard = TestEnvGuard::isolate().await;
    let out_glb_path = env_guard.path().join("exported_cube.glb");

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

    // 3. Construct SubmitArgs for 3D Generation
    let args = SubmitArgs {
        master: Some(master_addr.to_string()),
        task_type: "3d".to_string(),
        command: None,
        cores: 1,
        ram_mb: 0,
        gpu: false,
        timeout_secs: 30,
        max_retries: None,
        wait: true,
        json: true,
        prompt: Some("a solid cube".to_string()),
        image: None,
        quality: Some("fast".to_string()),
        engine: Some("sdf".to_string()),
        export_texture: true,
        no_export_texture: false,
        input_mesh: None,
        rig_preset: None,
        embed_animations: Vec::new(),
        preferred_provider: None,
        output: Some(out_glb_path.clone()),
        args: Vec::new(),
        ..Default::default()
    };

    let exit_code = run_submit(args, None).await;
    assert_eq!(exit_code, ExitCode::SUCCESS);

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

#[tokio::test]
async fn test_cli_autorig_submission_and_export() {
    let env_guard = TestEnvGuard::isolate().await;
    let input_mesh_path = env_guard.path().join("input_cylinder.glb");
    let out_rigged_path = env_guard.path().join("rigged_cylinder.glb");

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

    // 3. Construct SubmitArgs for Auto-Rig
    let args = SubmitArgs {
        master: Some(master_addr.to_string()),
        task_type: "autorig".to_string(),
        command: None,
        cores: 1,
        ram_mb: 0,
        gpu: false,
        timeout_secs: 30,
        max_retries: None,
        wait: true,
        json: true,
        prompt: None,
        image: None,
        quality: None,
        engine: None,
        export_texture: false,
        no_export_texture: false,
        input_mesh: Some(input_mesh_path),
        rig_preset: Some("biped".to_string()),
        embed_animations: vec!["idle".to_string()],
        preferred_provider: Some("builtin".to_string()),
        output: Some(out_rigged_path.clone()),
        args: Vec::new(),
        ..Default::default()
    };

    let exit_code = run_submit(args, None).await;
    assert_eq!(exit_code, ExitCode::SUCCESS);

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

#[tokio::test]
async fn test_cli_negative_validations() {
    let _env_guard = TestEnvGuard::isolate().await;
    // Non-existent image file
    let args = SubmitArgs {
        master: Some("127.0.0.1:8080".to_string()),
        task_type: "3d".to_string(),
        command: None,
        cores: 1,
        ram_mb: 0,
        gpu: false,
        timeout_secs: 30,
        max_retries: None,
        wait: true,
        json: false,
        prompt: None,
        image: Some("non_existent_image_12345.png".into()),
        quality: None,
        engine: None,
        export_texture: false,
        no_export_texture: false,
        input_mesh: None,
        rig_preset: None,
        embed_animations: Vec::new(),
        preferred_provider: None,
        output: None,
        args: Vec::new(),
        ..Default::default()
    };
    let exit_code = run_submit(args, None).await;
    assert_eq!(exit_code, ExitCode::FAILURE);

    // Empty input mesh file
    let temp_dir = TempDir::new().unwrap();
    let empty_mesh = temp_dir.path().join("empty.glb");
    std::fs::write(&empty_mesh, b"").unwrap();

    let args_mesh = SubmitArgs {
        master: Some("127.0.0.1:8080".to_string()),
        task_type: "autorig".to_string(),
        command: None,
        cores: 1,
        ram_mb: 0,
        gpu: false,
        timeout_secs: 30,
        max_retries: None,
        wait: true,
        json: false,
        prompt: None,
        image: None,
        quality: None,
        engine: None,
        export_texture: false,
        no_export_texture: false,
        input_mesh: Some(empty_mesh),
        rig_preset: None,
        embed_animations: Vec::new(),
        preferred_provider: None,
        output: None,
        args: Vec::new(),
        ..Default::default()
    };
    let exit_code = run_submit(args_mesh, None).await;
    assert_eq!(exit_code, ExitCode::FAILURE);
}
