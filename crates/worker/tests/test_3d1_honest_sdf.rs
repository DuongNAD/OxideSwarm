//! Integration tests for Stage 1 Item 4: 3D-1 ("Enforce Default D3 honest failure for 3D SDF tier")
//! and absolute path resolution extensions.

use std::path::PathBuf;
use tokio::sync::Mutex;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::task::{Bytes, Generative3DSpec, Task, TaskRequirements, TaskSpec};
use rusty_grid_worker::gen3d::runner::{
    resolve_entrypoint_script, resolve_python_runtime, to_clean_absolute_path,
};
use rusty_grid_worker::sandbox::SandboxConfig;
use rusty_grid_worker::{RunnerConfig, TaskRunner};

static ENV_MUTEX: Mutex<()> = Mutex::const_new(());

fn create_test_runner() -> TaskRunner {
    let temp_dir = tempfile::tempdir().expect("Failed to create tempdir");
    let sandbox_config = SandboxConfig::new(temp_dir.path().to_path_buf());
    let runner_config = RunnerConfig::new(sandbox_config);
    let mut caps = WorkerCapabilities::new("test_worker", 4, 8192, false, false, None);
    caps.has_cuda = false;
    TaskRunner::new(uuid::Uuid::new_v4(), caps, runner_config)
}

/// Helper struct that restores environment variables on drop
struct EnvGuard {
    saved: Vec<(&'static str, Option<String>)>,
}

impl EnvGuard {
    fn new(keys: &[&'static str]) -> Self {
        let mut saved = Vec::new();
        for &k in keys {
            saved.push((k, std::env::var(k).ok()));
        }
        Self { saved }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (k, v) in &self.saved {
            match v {
                Some(val) => std::env::set_var(k, val),
                None => std::env::remove_var(k),
            }
        }
    }
}

// ============================================================================
// Test 1: Explicit --engine triposr without model checkpoint fails honestly
// ============================================================================
#[tokio::test]
async fn test_3d1_triposr_without_model_fails_honestly() {
    let _lock = ENV_MUTEX.lock().await;
    let _guard = EnvGuard::new(&[
        "OXIDESWARM_TRIPOSR_CHECKPOINT",
        "TRIPOSR_CHECKPOINT",
        "OXIDESWARM_GEN3D_SCRIPT",
    ]);

    std::env::remove_var("OXIDESWARM_TRIPOSR_CHECKPOINT");
    std::env::remove_var("TRIPOSR_CHECKPOINT");
    std::env::remove_var("OXIDESWARM_GEN3D_SCRIPT");

    let runner = create_test_runner();
    let spec = Generative3DSpec::new_prompt("a combat mech")
        .with_quality("standard")
        .with_engine("triposr");

    let task = Task::new(TaskSpec::Generative3D(spec), TaskRequirements::default());
    let result = runner.execute_task(&task, None, None).await;

    // Must fail honestly
    assert!(
        !result.is_success(),
        "Task under --engine triposr without model must fail"
    );
    assert_ne!(result.exit_code, 0, "Exit code must be non-zero");
    assert!(
        result.asset_bytes.is_none(),
        "Asset bytes must be None on honest failure"
    );

    // Must report descriptive diagnostic error from entrypoint.py or missing runtime
    let stderr_str = String::from_utf8_lossy(result.stderr.as_slice());
    let err_str = result.error.as_deref().unwrap_or("");
    assert!(
        stderr_str.contains("TripoSR model not available")
            || err_str.contains("Missing Python")
            || err_str.contains("3D engine subprocess failed"),
        "Failure must name cause; stderr: {stderr_str}, error: {err_str}"
    );

    // Device name must not falsely claim TripoSR ran
    let device_name = result.device_name.as_deref().unwrap_or("");
    assert!(
        !device_name.contains("TripoSR"),
        "Device name must not claim TripoSR on failure: {:?}",
        device_name
    );
}

// ============================================================================
// Test 2: Default --engine auto routes directly to Tier 0 procedural SDF tier
// ============================================================================
#[tokio::test]
async fn test_3d1_auto_engine_defaults_to_sdf_tier() {
    let _lock = ENV_MUTEX.lock().await;
    let _guard = EnvGuard::new(&["OXIDESWARM_GEN3D_SCRIPT"]);
    std::env::remove_var("OXIDESWARM_GEN3D_SCRIPT");

    let runner = create_test_runner();
    let spec = Generative3DSpec::new_prompt("procedural humanoid drone")
        .with_quality("fast")
        .with_engine("auto");

    let task = Task::new(TaskSpec::Generative3D(spec), TaskRequirements::default());
    let result = runner.execute_task(&task, None, None).await;

    assert!(result.is_success(), "Task failed: {:?}", result.error);
    assert!(result.asset_bytes.is_some(), "Asset bytes missing");

    let asset = result.asset_bytes.unwrap();
    assert!(asset.len() >= 12, "glTF asset too short");
    let bytes = asset.as_slice();
    assert_eq!(&bytes[0..4], b"glTF", "Magic header must be glTF");
    assert_eq!(
        u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
        2,
        "Version must be glTF 2.0"
    );

    // Device name must be Procedural-SDF-PureRust
    assert_eq!(
        result.device_name.as_deref(),
        Some("Procedural-SDF-PureRust"),
        "Default auto must use Procedural-SDF-PureRust"
    );
    assert!(
        !result.is_gpu_executed,
        "Tier 0 SDF must not claim GPU execution"
    );
}

// ============================================================================
// Test 3: SDF tier rejects image input honestly at both validation and runtime
// ============================================================================
#[tokio::test]
async fn test_3d1_sdf_tier_rejects_image_input_honestly() {
    let _lock = ENV_MUTEX.lock().await;

    let mut spec = Generative3DSpec::new_prompt("stylized creature").with_engine("sdf");
    spec.image_data = Some(Bytes::from_static(b"fake_image_data_bytes"));

    // 1. Client/Master TaskSpec::validate validation rejection
    let task_spec = TaskSpec::Generative3D(spec.clone());
    let val_err = task_spec
        .validate()
        .expect_err("TaskSpec::validate must reject image on SDF tier");
    assert_eq!(
        val_err,
        "Tier 0 procedural SDF engine does not support image input (text prompt only)"
    );

    // Also test with "tier0_sdf" engine name
    let mut tier0_spec = Generative3DSpec::new_prompt("stylized creature").with_engine("tier0_sdf");
    tier0_spec.image_data = Some(Bytes::from_static(b"fake_image_data_bytes"));
    let tier0_task_spec = TaskSpec::Generative3D(tier0_spec);
    let val_err2 = tier0_task_spec
        .validate()
        .expect_err("TaskSpec::validate must reject image on tier0_sdf");
    assert_eq!(
        val_err2,
        "Tier 0 procedural SDF engine does not support image input (text prompt only)"
    );

    // 2. Worker runner execute_tier0_sdf honest rejection
    let runner = create_test_runner();
    let task = Task::new(TaskSpec::Generative3D(spec), TaskRequirements::default());
    let result = runner.execute_task(&task, None, None).await;

    assert!(
        !result.is_success(),
        "Runner must reject image input on SDF tier"
    );
    assert!(result.asset_bytes.is_none(), "Asset bytes must be None");
    let err_str = result.error.as_deref().unwrap_or("");
    assert!(
        err_str.contains(
            "Tier 0 procedural SDF engine does not support image input (text prompt only)"
        ),
        "Unexpected error: {err_str}"
    );
}

// ============================================================================
// Test 4: resolve_entrypoint_script returns absolute path without \\?\ prefix
// ============================================================================
#[tokio::test]
async fn test_3d1_resolve_entrypoint_script_returns_absolute_path() {
    let _lock = ENV_MUTEX.lock().await;
    let _guard = EnvGuard::new(&["OXIDESWARM_GEN3D_SCRIPT"]);
    std::env::remove_var("OXIDESWARM_GEN3D_SCRIPT");

    let script = resolve_entrypoint_script();
    assert!(script.is_some(), "Entrypoint script must be resolvable");
    let path = script.unwrap();

    assert!(
        path.is_absolute(),
        "Resolved entrypoint script path must be absolute: {:?}",
        path
    );
    assert!(
        path.is_file(),
        "Resolved entrypoint script path must exist on disk: {:?}",
        path
    );

    #[cfg(windows)]
    {
        let s = path.to_string_lossy();
        assert!(
            !s.starts_with(r"\\?\"),
            "Resolved script path must not start with verbatim \\\\?\\ prefix: {:?}",
            s
        );
    }
}

// ============================================================================
// Test 5: resolve_python_runtime returns absolute path without \\?\ prefix
// ============================================================================
#[tokio::test]
async fn test_3d1_resolve_python_runtime_returns_absolute_path() {
    let _lock = ENV_MUTEX.lock().await;

    if let Some(py_path) = resolve_python_runtime() {
        assert!(
            py_path.is_absolute(),
            "Resolved python runtime path must be absolute: {:?}",
            py_path
        );
        assert!(
            py_path.is_file(),
            "Resolved python runtime path must exist on disk: {:?}",
            py_path
        );

        #[cfg(windows)]
        {
            let s = py_path.to_string_lossy();
            assert!(
                !s.starts_with(r"\\?\"),
                "Resolved python path must not start with verbatim \\\\?\\ prefix: {:?}",
                s
            );
        }
    }
}

// ============================================================================
// Test 6: No device_name contains TripoSR unless genuine model inference ran
// ============================================================================
#[tokio::test]
async fn test_3d1_no_device_name_contains_triposr_unless_model_ran() {
    let _lock = ENV_MUTEX.lock().await;
    let _guard = EnvGuard::new(&[
        "OXIDESWARM_TRIPOSR_CHECKPOINT",
        "TRIPOSR_CHECKPOINT",
        "OXIDESWARM_GEN3D_SCRIPT",
    ]);
    std::env::remove_var("OXIDESWARM_TRIPOSR_CHECKPOINT");
    std::env::remove_var("TRIPOSR_CHECKPOINT");
    std::env::remove_var("OXIDESWARM_GEN3D_SCRIPT");

    let runner = create_test_runner();

    // 1. auto engine
    let spec_auto = Generative3DSpec::new_prompt("torus").with_engine("auto");
    let task_auto = Task::new(
        TaskSpec::Generative3D(spec_auto),
        TaskRequirements::default(),
    );
    let res_auto = runner.execute_task(&task_auto, None, None).await;
    let dev_auto = res_auto.device_name.as_deref().unwrap_or("");
    assert!(
        !dev_auto.contains("TripoSR"),
        "Auto engine device name must not contain TripoSR: {dev_auto}"
    );

    // 2. sdf engine
    let spec_sdf = Generative3DSpec::new_prompt("sphere").with_engine("sdf");
    let task_sdf = Task::new(
        TaskSpec::Generative3D(spec_sdf),
        TaskRequirements::default(),
    );
    let res_sdf = runner.execute_task(&task_sdf, None, None).await;
    let dev_sdf = res_sdf.device_name.as_deref().unwrap_or("");
    assert!(
        !dev_sdf.contains("TripoSR"),
        "SDF engine device name must not contain TripoSR: {dev_sdf}"
    );

    // 3. triposr engine without model checkpoint
    let spec_triposr = Generative3DSpec::new_prompt("cylinder").with_engine("triposr");
    let task_triposr = Task::new(
        TaskSpec::Generative3D(spec_triposr),
        TaskRequirements::default(),
    );
    let res_triposr = runner.execute_task(&task_triposr, None, None).await;
    let dev_triposr = res_triposr.device_name.as_deref().unwrap_or("");
    assert!(
        !dev_triposr.contains("TripoSR"),
        "Failed triposr engine device name must not contain TripoSR: {dev_triposr}"
    );
}

// ============================================================================
// Test 7: agent_node.py engine options are trimmed to auto, sdf, triposr
// ============================================================================
#[tokio::test]
async fn test_3d1_agent_node_engine_options_trimmed() {
    let script_path = PathBuf::from("scripts/agent_node.py");
    let full_path = if script_path.is_file() {
        script_path
    } else {
        PathBuf::from("../../scripts/agent_node.py")
    };

    assert!(full_path.is_file(), "scripts/agent_node.py must exist");
    let content = std::fs::read_to_string(&full_path).expect("Failed to read agent_node.py");

    // Verify removed placeholder engines are NOT present in choices
    assert!(
        !content.contains("choices=[\"auto\", \"rodin\""),
        "Must not contain rodin choice"
    );
    assert!(
        !content.contains("choices=[\"auto\", \"trellis\""),
        "Must not contain trellis choice"
    );
    assert!(
        !content.contains("choices=[\"auto\", \"hunyuan\""),
        "Must not contain hunyuan choice"
    );

    // Verify trimmed choices are present
    assert!(
        content.contains("choices=[\"auto\", \"sdf\", \"triposr\"]"),
        "agent_node.py must restrict --engine choices to [auto, sdf, triposr]"
    );

    // If python is available, test that CLI rejects uninstalled engines
    if let Some(py_bin) = resolve_python_runtime() {
        let output = std::process::Command::new(&py_bin)
            .arg(&full_path)
            .args(["--engine", "rodin"])
            .output();

        if let Ok(out) = output {
            assert!(
                !out.status.success(),
                "agent_node.py must reject --engine rodin"
            );
            let stderr = String::from_utf8_lossy(&out.stderr);
            assert!(
                stderr.contains("invalid choice: 'rodin'"),
                "stderr must report invalid choice: {stderr}"
            );
        }
    }
}

// ============================================================================
// Test 8: to_clean_absolute_path strips Windows verbatim prefixes
// ============================================================================
#[tokio::test]
async fn test_3d1_to_clean_absolute_path_prefix_stripping() {
    let cargo_toml = PathBuf::from("Cargo.toml");
    if let Some(clean) = to_clean_absolute_path(&cargo_toml) {
        assert!(clean.is_absolute());
        #[cfg(windows)]
        {
            let s = clean.to_string_lossy();
            assert!(!s.starts_with(r"\\?\"));
            assert!(!s.starts_with(r"\\?\UNC\"));
        }
    }
}
