//! Feature F16: Headless Blender 4.5.4 LTS Auto-Rig Provider.
//!
//! Executes host Blender in headless mode supervised by Win32 Job Object
//! (`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`), with automatic fallback to the built-in Rust rigging engine.

use crate::job_object::JobObject;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::{debug, info, warn};

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
    if let Ok(output) = std::process::Command::new("blender")
        .arg("--version")
        .output()
    {
        if output.status.success() {
            return Some(PathBuf::from("blender"));
        }
    }

    None
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
    /// Supervised under Win32 Job Object with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`.
    pub async fn execute_rigging(
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

        // Initialize Win32 Job Object for process tree containment
        let mut job_object = match JobObject::create() {
            Ok(job) => Some(job),
            Err(e) => {
                warn!("Failed to create Win32 Job Object for Blender: {e}");
                None
            }
        };

        let anim_arg = animations.join(",");

        let mut cmd = tokio::process::Command::new(blender_bin);
        cmd.arg("--background");
        cmd.arg("--factory-startup");
        cmd.arg("--python");
        cmd.arg(&script_path);
        cmd.arg("--");
        cmd.arg("--input");
        cmd.arg(&input_path);
        cmd.arg("--output");
        cmd.arg(&output_path);
        cmd.arg("--preset");
        cmd.arg(preset);
        cmd.arg("--animations");
        cmd.arg(&anim_arg);

        cmd.current_dir(temp_dir.path());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        cmd.stdin(Stdio::null());
        cmd.kill_on_drop(true);

        #[cfg(unix)]
        {
            cmd.process_group(0);
        }

        let mut child = cmd
            .spawn()
            .map_err(|e| format!("Failed to spawn Blender subprocess: {e}"))?;

        // Assign PID to Job Object immediately
        if let Some(pid) = child.id() {
            if let Some(ref mut job) = job_object {
                let _ = job.assign_pid(pid);
            }
        }

        // Stream output
        let stdout = child
            .stdout
            .take()
            .ok_or("Failed to capture Blender stdout")?;
        let mut reader = BufReader::new(stdout).lines();

        let execution = async {
            while let Ok(Some(line)) = reader.next_line().await {
                debug!("[BLENDER] {line}");
                if let Some((progress, msg)) = crate::gen3d::runner::parse_progress_marker(&line) {
                    info!(progress = progress, message = %msg, "Blender auto-rigging progress");
                }
            }
            child.wait().await
        };

        match tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), execution).await {
            Ok(Ok(status)) if status.success() => {
                if output_path.is_file() {
                    let glb_bytes = tokio::fs::read(&output_path)
                        .await
                        .map_err(|e| format!("Failed to read Blender output .glb: {e}"))?;
                    Ok(glb_bytes)
                } else {
                    Err(
                        "Blender process finished with success but output .glb is missing"
                            .to_string(),
                    )
                }
            }
            Ok(Ok(status)) => Err(format!(
                "Blender process exited with error status: {status}"
            )),
            Ok(Err(e)) => Err(format!("I/O error during Blender execution: {e}")),
            Err(_) => {
                // Timeout: enforce termination
                if let Some(ref job) = job_object {
                    let _ = job.terminate(1);
                }
                Err(format!(
                    "Blender auto-rigging timed out after {timeout_secs}s"
                ))
            }
        }
    }
}
