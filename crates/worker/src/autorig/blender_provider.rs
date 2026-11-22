//! Feature F16: Headless Blender 4.5.4 LTS Auto-Rig Provider.
//!
//! Executes host Blender in headless mode supervised by the hardened runner
//! (`ProcessTree` with `CREATE_SUSPENDED` on Windows / process group on Unix),
//! with automatic fallback to the built-in Rust rigging engine.

use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tracing::{debug, info};

use super::mesh_loader::{detect_mesh_format, strip_leading_boms_and_ws, MeshFormat};
use super::types::WorkerError;

pub const EMBEDDED_BLENDER_RIG_SCRIPT: &str =
    include_str!("../../../../engines/autorig/blender_rig.py");

/// Resolves the file path to host Blender executable.
///
/// Delegates to [`rusty_grid_core::capabilities::resolve_blender_binary`] so capability
/// detection (`detect_blender`) and task execution share one resolution rule and search path list.
pub fn resolve_blender_binary(explicit_path: Option<&str>) -> Option<PathBuf> {
    rusty_grid_core::capabilities::resolve_blender_binary(explicit_path)
}

/// Probes whether a Blender executable candidate is runnable by invoking `<program> --version`
/// with sensitive environment variables stripped.
pub fn probe_blender<P: AsRef<std::ffi::OsStr>>(program: P) -> bool {
    rusty_grid_core::capabilities::probe_blender_binary(program).0
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
        Self::execute_rigging_supervised(
            runner,
            task_id,
            blender_bin,
            input_mesh_bytes,
            preset,
            animations,
            Duration::from_secs(timeout_secs),
            None,
            None,
            Instant::now(),
        )
        .await
        .map_err(|e| e.to_string())
    }

    /// Executes headless Blender auto-rigging with full timeout, PID, and cancel propagation.
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_rigging_supervised(
        runner: &crate::runner::TaskRunner,
        task_id: rusty_grid_core::task::TaskId,
        blender_bin: &Path,
        input_mesh_bytes: &[u8],
        preset: &str,
        animations: &[String],
        timeout: Duration,
        child_pid: Option<Arc<AtomicU32>>,
        cancel_rx: Option<oneshot::Receiver<String>>,
        start_time: Instant,
    ) -> Result<Vec<u8>, WorkerError> {
        let format = detect_mesh_format(input_mesh_bytes).map_err(WorkerError::AutoRig)?;
        let staged_input_bytes = match format {
            MeshFormat::Glb => input_mesh_bytes,
            MeshFormat::Gltf | MeshFormat::Obj => strip_leading_boms_and_ws(input_mesh_bytes),
        };

        let temp_dir = tempfile::tempdir().map_err(|e| {
            WorkerError::AutoRig(format!(
                "Failed to create temporary directory for Blender: {e}"
            ))
        })?;

        let input_path = temp_dir
            .path()
            .join(format!("input_mesh.{}", format.extension()));
        let output_path = temp_dir.path().join("rigged_output.glb");
        let script_path = temp_dir.path().join("blender_rig.py");

        // Write input mesh (BOM/leading-whitespace stripped for text .obj/.gltf)
        tokio::fs::write(&input_path, staged_input_bytes)
            .await
            .map_err(|e| {
                WorkerError::AutoRig(format!("Failed to write input mesh to temp dir: {e}"))
            })?;

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
            .map_err(|e| WorkerError::AutoRig(format!("Failed to write blender_rig.py: {e}")))?;

        let anim_arg = animations.join(",");

        let cmd_args = vec![
            "--background".to_string(),
            "--factory-startup".to_string(),
            "--python-exit-code".to_string(),
            "1".to_string(),
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
                timeout,
                child_pid,
                cancel_rx,
                start_time,
            )
            .await;

        let stdout_str = String::from_utf8_lossy(res.stdout.as_slice());
        for line in stdout_str.lines() {
            debug!("[BLENDER] {line}");
            if let Some((progress, msg)) = crate::gen3d::runner::parse_progress_marker(line) {
                info!(progress = progress, message = %msg, "Blender auto-rigging progress");
            }
        }

        if res.exit_code == crate::runner::EXIT_CODE_CANCELLED {
            return Err(WorkerError::Cancelled(
                res.error.unwrap_or_else(|| "Task cancelled".to_string()),
            ));
        }
        if res.exit_code == crate::runner::EXIT_CODE_TIMEOUT {
            if let Some(err) = res.error {
                return Err(WorkerError::TimedOut(err));
            }
        }

        if res.is_success() {
            match crate::runner::read_output_file(&output_path).await {
                Ok(Some(bytes)) => {
                    if bytes.len() >= 12 && bytes.starts_with(b"glTF") {
                        Ok(bytes)
                    } else {
                        Err(WorkerError::AutoRig(
                            "Blender output .glb has invalid glTF 2.0 header".to_string(),
                        ))
                    }
                }
                Ok(None) => Err(WorkerError::AutoRig(
                    "Blender output .glb missing".to_string(),
                )),
                Err(e) => Err(WorkerError::AutoRig(format!(
                    "Failed to read Blender output .glb: {e}"
                ))),
            }
        } else {
            let err_msg = res.error.unwrap_or_else(|| {
                let last_err_line = String::from_utf8_lossy(res.stderr.as_slice())
                    .lines()
                    .map(str::trim)
                    .rfind(|l| !l.is_empty())
                    .map(str::to_string);
                match last_err_line {
                    Some(line) => format!(
                        "Blender process exited with status {}: {line}",
                        res.exit_code
                    ),
                    None => format!(
                        "Blender process exited with error status: {}",
                        res.exit_code
                    ),
                }
            });
            Err(WorkerError::AutoRig(err_msg))
        }
    }
}
