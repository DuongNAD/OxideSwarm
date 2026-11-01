//! Feature F16: Headless Blender 4.5.4 LTS Auto-Rig Provider.
//!
//! Executes host Blender in headless mode supervised by the hardened runner
//! (`ProcessTree` with `CREATE_SUSPENDED` on Windows / process group on Unix),
//! with automatic fallback to the built-in Rust rigging engine.

use std::path::{Path, PathBuf};
use tracing::{debug, info};

pub const EMBEDDED_BLENDER_RIG_SCRIPT: &str =
    include_str!("../../../../engines/autorig/blender_rig.py");

/// Resolves the file path to host Blender executable.
pub fn resolve_blender_binary() -> Option<PathBuf> {
    // 1. Environment variables
    for env_var in &[
        "RUSTY_GRID_BLENDER_PATH",
        "OXIDE_SWARM_BLENDER_PATH",
        "BLENDER_PATH",
    ] {
        if let Ok(env_path) = std::env::var(env_var) {
            let p = PathBuf::from(env_path.trim());
            if p.is_file() {
                return Some(p);
            }
        }
    }

    // 2. Standard Windows installation paths
    #[cfg(windows)]
    {
        let mut search_dirs = Vec::new();
        if let Ok(pf) = std::env::var("ProgramFiles") {
            search_dirs.push(PathBuf::from(pf).join("Blender Foundation"));
        }
        if let Ok(pf86) = std::env::var("ProgramFiles(x86)") {
            search_dirs.push(PathBuf::from(pf86).join("Blender Foundation"));
        }
        search_dirs.push(PathBuf::from(r"C:\Program Files\Blender Foundation"));

        for base_dir in search_dirs {
            if base_dir.is_dir() {
                if let Ok(entries) = std::fs::read_dir(&base_dir) {
                    let mut version_dirs: Vec<PathBuf> = entries
                        .flatten()
                        .map(|e| e.path())
                        .filter(|p| {
                            p.is_dir()
                                && p.file_name()
                                    .unwrap_or_default()
                                    .to_string_lossy()
                                    .starts_with("Blender")
                        })
                        .collect();
                    version_dirs.sort_by(|a, b| b.cmp(a)); // Descending version order (prefer 4.5 over older)
                    for dir in version_dirs {
                        let exe = dir.join("blender.exe");
                        if exe.is_file() {
                            return Some(exe);
                        }
                    }
                }
            }
        }
    }

    // 3. Fallback check for "blender" on PATH
    if probe_blender("blender") {
        return Some(PathBuf::from("blender"));
    }

    None
}

/// Probes whether a Blender executable candidate is runnable by invoking `<program> --version`
/// with sensitive environment variables stripped.
pub fn probe_blender<P: AsRef<std::ffi::OsStr>>(program: P) -> bool {
    rusty_grid_core::cluster_auth::probe_command(program)
        .arg("--version")
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false)
}

/// Resolves the path to `engines/autorig/blender_rig.py`.
pub fn resolve_blender_rig_script() -> Option<PathBuf> {
    let candidates = [
        PathBuf::from("engines/autorig/blender_rig.py"),
        PathBuf::from("../engines/autorig/blender_rig.py"),
        PathBuf::from("../../engines/autorig/blender_rig.py"),
    ];

    for c in &candidates {
        if c.is_file() {
            return Some(c.clone());
        }
    }

    None
}

/// Headless Blender provider executing volumetric heat-diffusion auto-weighting.
pub struct BlenderRigProvider;

impl BlenderRigProvider {
    /// Executes automated rigging via host Blender in headless mode.
    ///
    /// Supervised under the hardened runner (`ProcessTree`).
    pub async fn execute_rigging(
        runner: &crate::runner::TaskRunner,
        task_id: rusty_grid_core::task::TaskId,
        blender_bin: &Path,
        input_mesh_bytes: &[u8],
        preset: &str,
        animations: &[String],
        timeout_secs: u64,
    ) -> Result<Vec<u8>, String> {
        let temp_dir = tempfile::tempdir()
            .map_err(|e| format!("Failed to create temporary directory for Blender: {e}"))?;

        let input_path = temp_dir.path().join("input_mesh.glb");
        let output_path = temp_dir.path().join("rigged_output.glb");
        let script_path = temp_dir.path().join("blender_rig.py");

        // Write input mesh
        tokio::fs::write(&input_path, input_mesh_bytes)
            .await
            .map_err(|e| format!("Failed to write input mesh to temp dir: {e}"))?;

        // Write driver script (using local file or embedded script)
        let script_content = if let Some(local_script) = resolve_blender_rig_script() {
            tokio::fs::read_to_string(&local_script)
                .await
                .unwrap_or_else(|_| EMBEDDED_BLENDER_RIG_SCRIPT.to_string())
        } else {
            EMBEDDED_BLENDER_RIG_SCRIPT.to_string()
        };

        tokio::fs::write(&script_path, script_content)
            .await
            .map_err(|e| format!("Failed to write blender_rig.py: {e}"))?;

        let anim_arg = animations.join(",");

        let cmd_args = vec![
            "--background".to_string(),
            "--factory-startup".to_string(),
            "--python".to_string(),
            script_path.to_string_lossy().to_string(),
            "--".to_string(),
            "--input".to_string(),
            input_path.to_string_lossy().to_string(),
            "--output".to_string(),
            output_path.to_string_lossy().to_string(),
            "--preset".to_string(),
            preset.to_string(),
            "--animations".to_string(),
            anim_arg,
        ];

        let res = runner
            .execute_process_supervised(
                task_id,
                temp_dir.path(),
                &blender_bin.to_string_lossy(),
                &cmd_args,
                temp_dir.path(),
                &std::collections::HashMap::new(),
                None,
                std::time::Duration::from_secs(timeout_secs),
                None,
                None,
                std::time::Instant::now(),
            )
            .await;

        let stdout_str = String::from_utf8_lossy(res.stdout.as_slice());
        for line in stdout_str.lines() {
            debug!("[BLENDER] {line}");
            if let Some((progress, msg)) = crate::gen3d::runner::parse_progress_marker(line) {
                info!(progress = progress, message = %msg, "Blender auto-rigging progress");
            }
        }

        if res.is_success() {
            match crate::runner::read_output_file(&output_path).await {
                Ok(Some(bytes)) => Ok(bytes),
                Ok(None) => Err("Blender output .glb missing".to_string()),
                Err(e) => Err(format!("Failed to read Blender output .glb: {e}")),
            }
        } else {
            Err(res.error.unwrap_or_else(|| {
                format!(
                    "Blender process exited with error status: {}",
                    res.exit_code
                )
            }))
        }
    }
}
