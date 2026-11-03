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
    if resolve_python_runtime().is_some() && resolve_entrypoint_script().is_some() {
        assert!(
            stderr_str.contains("TripoSR model not available"),
            "Failure must explicitly name cause 'TripoSR model not available'; got stderr: {stderr_str}"
        );
        assert!(
            err_str.contains("TripoSR model not available"),
            "result.error must contain diagnostic cause; got: {err_str}"
        );
    } else {
        assert!(
            stderr_str.contains("TripoSR model not available: missing Python"),
            "Failure must explicitly name missing runtime 'TripoSR model not available: missing Python'; got stderr: {stderr_str}"
        );
        assert!(
            err_str.contains("TripoSR model not available: missing Python"),
            "Failure must name missing runtime 'TripoSR model not available: missing Python'; got error: {err_str}"
        );
    }

    // Device name must not falsely claim TripoSR ran
    let device_name = result.device_name.as_deref().unwrap_or("");
    assert!(
        !device_name.contains("TripoSR"),
        "Device name must not claim TripoSR on failure: {:?}",
        device_name
    );
}

#[tokio::test]
async fn test_3d1_triposr_with_arbitrary_checkpoint_still_fails_honestly() {
    let _lock = ENV_MUTEX.lock().await;
    let _guard = EnvGuard::new(&["OXIDESWARM_TRIPOSR_CHECKPOINT", "OXIDESWARM_GEN3D_SCRIPT"]);
    std::env::set_var("OXIDESWARM_TRIPOSR_CHECKPOINT", "Cargo.toml");
    std::env::remove_var("OXIDESWARM_GEN3D_SCRIPT");

    let runner = create_test_runner();
    let spec = Generative3DSpec::new_prompt("test model").with_engine("triposr");
    let task = Task::new(TaskSpec::Generative3D(spec), TaskRequirements::default());
    let result = runner.execute_task(&task, None, None).await;

    assert!(!result.is_success());
    assert_ne!(result.exit_code, 0);
    assert!(result.asset_bytes.is_none());
    let dev = result.device_name.as_deref().unwrap_or("");
    assert!(!dev.contains("TripoSR"));
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

#[tokio::test]
async fn test_3d1_auto_engine_never_invokes_python_runtime() {
    let _lock = ENV_MUTEX.lock().await;
    let temp_dir = tempfile::tempdir().expect("Failed to create tempdir");
    let sentinel_file = temp_dir.path().join("sentinel_python_ran.txt");
    let trap_script = temp_dir.path().join("trap_entrypoint.py");

    let script_code = format!(
        "import sys, pathlib\npathlib.Path(r'{}').write_text('TRAP_EXECUTED')\nsys.exit(1)\n",
        sentinel_file.display()
    );
    std::fs::write(&trap_script, script_code).unwrap();

    let _guard = EnvGuard::new(&["OXIDESWARM_GEN3D_SCRIPT"]);
    std::env::set_var("OXIDESWARM_GEN3D_SCRIPT", &trap_script);

    let runner = create_test_runner();
    let spec = Generative3DSpec::new_prompt("drone").with_engine("auto");
    let task = Task::new(TaskSpec::Generative3D(spec), TaskRequirements::default());

    let result = runner.execute_task(&task, None, None).await;

    // 1. Task must succeed via procedural SDF
    assert!(
        result.is_success(),
        "Auto engine must succeed directly via pure Rust SDF: {:?}",
        result.error
    );
    assert_eq!(
        result.device_name.as_deref(),
        Some("Procedural-SDF-PureRust"),
        "Device label must be Procedural-SDF-PureRust"
    );

    // 2. Python trap script must NEVER have been executed
    assert!(
        !sentinel_file.exists(),
        "Python entrypoint must NEVER be executed for auto engine"
    );
}

#[tokio::test]
async fn test_3d1_auto_engine_defaults_to_sdf_tier_even_with_custom_script() {
    let _lock = ENV_MUTEX.lock().await;
    let temp_dir = tempfile::tempdir().expect("Failed to create tempdir");
    let dummy_script = temp_dir.path().join("mock_engine.py");
    std::fs::write(&dummy_script, "import sys\nsys.exit(0)\n").unwrap();

    let _guard = EnvGuard::new(&["OXIDESWARM_GEN3D_SCRIPT"]);
    std::env::set_var("OXIDESWARM_GEN3D_SCRIPT", dummy_script.to_str().unwrap());

    let runner = create_test_runner();
    let spec = Generative3DSpec::new_prompt("procedural humanoid drone")
        .with_quality("fast")
        .with_engine("auto");

    let task = Task::new(TaskSpec::Generative3D(spec), TaskRequirements::default());
    let result = runner.execute_task(&task, None, None).await;

    assert!(result.is_success());
    assert_eq!(
        result.device_name.as_deref(),
        Some("Procedural-SDF-PureRust"),
        "Auto must always use Procedural-SDF-PureRust even if OXIDESWARM_GEN3D_SCRIPT is set"
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

    // Also test with "auto" engine name
    let mut auto_spec = Generative3DSpec::new_prompt("stylized creature").with_engine("auto");
    auto_spec.image_data = Some(Bytes::from_static(b"fake_image_data_bytes"));
    let auto_task_spec = TaskSpec::Generative3D(auto_spec);
    let val_err_auto = auto_task_spec
        .validate()
        .expect_err("TaskSpec::validate must reject image on auto");
    assert_eq!(
        val_err_auto,
        "Image input is only supported with neural 3D engines (e.g. triposr); the auto engine does not accept images"
    );

    // Also test all remaining Engine3D::Sdf aliases ("procedural", "tier0", "tier-0", "procedural_sdf")
    for alias in ["procedural", "tier0", "tier-0", "procedural_sdf"] {
        let mut alias_spec = Generative3DSpec::new_prompt("stylized creature").with_engine(alias);
        alias_spec.image_data = Some(Bytes::from_static(b"fake_image_data_bytes"));
        let alias_task_spec = TaskSpec::Generative3D(alias_spec);
        let val_err_alias = alias_task_spec.validate().expect_err(&format!(
            "TaskSpec::validate must reject image on SDF alias '{alias}'"
        ));
        assert_eq!(
            val_err_alias,
            "Tier 0 procedural SDF engine does not support image input (text prompt only)",
            "SDF alias '{alias}' must return SDF image rejection error"
        );
    }

    // Also test with default engine (None)
    let mut default_spec = Generative3DSpec::new_prompt("stylized creature");
    default_spec.image_data = Some(Bytes::from_static(b"fake_image_data_bytes"));
    let default_task_spec = TaskSpec::Generative3D(default_spec);
    let val_err_def = default_task_spec
        .validate()
        .expect_err("TaskSpec::validate must reject image on default engine");
    assert_eq!(
        val_err_def,
        "Image input is only supported with neural 3D engines (e.g. triposr); the auto engine does not accept images"
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
            let stdout = String::from_utf8_lossy(&out.stdout);
            let combined = format!("{stderr}\n{stdout}");
            assert!(
                combined.contains("invalid choice: 'rodin'"),
                "Output must report invalid choice: {combined}"
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
    let clean = to_clean_absolute_path(&cargo_toml)
        .expect("Cargo.toml must resolve to clean absolute path");
    assert!(clean.is_absolute());
    #[cfg(windows)]
    {
        let s = clean.to_string_lossy();
        assert!(!s.starts_with(r"\\?\"));
        assert!(!s.starts_with(r"\\?\UNC\"));
    }
}

// ============================================================================
// Test 9: Success path dynamically extracts engine label from subprocess stdout
// ============================================================================
#[tokio::test]
async fn test_3d1_success_path_device_name_comes_from_engine() {
    let _lock = ENV_MUTEX.lock().await;
    let temp_dir = tempfile::tempdir().expect("Failed to create tempdir");
    let mock_script = temp_dir.path().join("mock_success_engine.py");

    let python_code = r#"
import sys, argparse
parser = argparse.ArgumentParser()
parser.add_argument("--output", required=True)
parser.add_argument("--input", default=None)
parser.add_argument("--prompt", default=None)
parser.add_argument("--device", default="cpu")
parser.add_argument("--quality", default="standard")
parser.add_argument("--export-texture", action="store_true")
parser.add_argument("--no-texture", action="store_false", dest="export_texture")
parser.add_argument("--offline", action="store_true")
args = parser.parse_args()

print("[DEVICE: Custom-Mock-Engine-v2]", flush=True)

with open(args.output, "wb") as f:
    f.write(b"glTF\x02\x00\x00\x00\x14\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")
sys.exit(0)
"#;
    std::fs::write(&mock_script, python_code).unwrap();

    let _guard = EnvGuard::new(&["OXIDESWARM_GEN3D_SCRIPT"]);
    std::env::set_var("OXIDESWARM_GEN3D_SCRIPT", mock_script.to_str().unwrap());

    let runner = create_test_runner();
    let spec = Generative3DSpec::new_prompt("test prompt")
        .with_quality("standard")
        .with_engine("triposr");

    let task = Task::new(TaskSpec::Generative3D(spec), TaskRequirements::default());
    let result = runner.execute_task(&task, None, None).await;

    if resolve_python_runtime().is_some() {
        assert!(
            result.is_success(),
            "Task should succeed: {:?}",
            result.error
        );
        assert_eq!(
            result.device_name.as_deref(),
            Some("Custom-Mock-Engine-v2"),
            "Device name must come dynamically from the engine output"
        );
    }
}

// ============================================================================
// Test 10: resolve_python_runtime preserves virtualenv symlink sys.prefix
// ============================================================================
#[tokio::test]
async fn test_3d1_resolve_python_preserves_venv_sys_prefix() {
    let _lock = ENV_MUTEX.lock().await;

    let base_py = match resolve_python_runtime() {
        Some(p) => p,
        None => return,
    };

    let out_raw = std::process::Command::new(&base_py)
        .args(["-c", "import sys; print(sys.prefix)"])
        .output();
    let raw_prefix = match out_raw {
        Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout).trim().to_string(),
        _ => return,
    };

    let temp_dir = tempfile::tempdir().expect("Failed to create tempdir");
    let link_name = if cfg!(windows) {
        "python_sym.exe"
    } else {
        "python_sym"
    };
    let symlink_path = temp_dir.path().join(link_name);

    let created_symlink = {
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&base_py, &symlink_path).is_ok()
        }
        #[cfg(windows)]
        {
            std::os::windows::fs::symlink_file(&base_py, &symlink_path).is_ok()
        }
    };

    if created_symlink && symlink_path.is_file() {
        let _guard = EnvGuard::new(&["OXIDESWARM_PYTHON"]);
        std::env::set_var("OXIDESWARM_PYTHON", &symlink_path);

        let resolved = resolve_python_runtime().expect("Should resolve OXIDESWARM_PYTHON");
        // Must NOT resolve symlink to base_py target path
        assert_eq!(
            resolved, symlink_path,
            "Resolved path must preserve symlink, not canonical target"
        );

        let out_resolved = std::process::Command::new(&resolved)
            .args(["-c", "import sys; print(sys.prefix)"])
            .output()
            .expect("Failed to run resolved symlink");
        if out_resolved.status.success() {
            let resolved_prefix = String::from_utf8_lossy(&out_resolved.stdout)
                .trim()
                .to_string();
            assert_eq!(
                resolved_prefix, raw_prefix,
                "sys.prefix before ({raw_prefix}) and after ({resolved_prefix}) resolving must match"
            );
        }
    }
}
