//! Automated Rigging & Skeletal Animation Pipeline for OxideSwarm (Milestone 3).
//!
//! Provides pure-Rust automated skeleton rigging, landmark extraction (F11),
//! DAG armature hierarchy and Inverse Bind Matrices (F12), geodesic skin weighting (F13),
//! procedural TRS animation keyframing (F14), compliant binary glTF 2.0 export (F15),
//! and supervised headless Blender fallback (F16).

pub mod anim_synth;
pub mod armature;
pub mod blender_provider;
pub mod gltf_export;
pub mod landmark;
pub mod math;
pub mod mesh_loader;
pub mod skinning;
pub mod types;

pub use anim_synth::{AnimationTargetPath, AnimationTrack, ClipSynthesizer, ProceduralClip};
pub use armature::{build_bone_segments, build_standard_armature};
pub use blender_provider::BlenderRigProvider;
pub use gltf_export::{
    export_rigged_glb, export_rigged_glb_cancellable, CHUNK_TYPE_BIN, CHUNK_TYPE_JSON, GLB_MAGIC,
};
pub use landmark::{extract_landmarks, extract_landmarks_cancellable};
pub use math::{
    eigen_decomposition_3x3, eigen_symmetric_3x3, EigenDecomposition3, Mat4, Quat, Vec3,
};
pub use mesh_loader::{
    detect_mesh_format, load_mesh_from_bytes, load_mesh_from_bytes_cancellable, MeshFormat,
};
pub use skinning::{compute_skin_weights, compute_skin_weights_cancellable, SkinningConfig};
pub use types::{
    AnatomicalLandmarks, Armature, BoneSegment, Joint, LimbSide, QuadrupedLandmarks, RigPreset,
    SkinWeights, WorkerError,
};

use rusty_grid_core::task::{AutoRigProvider, AutoRigSpec};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tracing::{info, warn};

/// Concrete backend that actually executed an AutoRig workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoRigUsedProvider {
    Blender,
    Builtin,
}

impl AutoRigUsedProvider {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Blender => "blender",
            Self::Builtin => "builtin",
        }
    }

    pub fn device_name(&self) -> &'static str {
        match self {
            Self::Blender => "OxideSwarm-AutoRig-Blender",
            Self::Builtin => "OxideSwarm-AutoRig-Builtin",
        }
    }
}

/// Output bytes paired with the provider that produced them.
#[derive(Debug, Clone)]
pub struct AutoRigOutcome {
    pub glb_bytes: Vec<u8>,
    pub provider: AutoRigUsedProvider,
}

struct AutoRigStopOnDrop(Arc<AtomicBool>);

impl Drop for AutoRigStopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

fn map_stage_err(stage: &str, err: String) -> WorkerError {
    if err == "Cancelled" {
        WorkerError::Cancelled("Task cancelled".to_string())
    } else {
        WorkerError::AutoRig(format!("{stage}: {err}"))
    }
}

/// Executes the built-in pure-Rust auto-rigging engine with progressive milestone callback reporting:
/// - 25%: `landmark_extraction`
/// - 50%: `armature_ibm`
/// - 75%: `geodesic_skinning`
/// - 100%: `animation_export`
pub fn auto_rig_mesh_with_progress<F>(
    spec: &AutoRigSpec,
    on_progress: F,
) -> Result<Vec<u8>, WorkerError>
where
    F: FnMut(f32, &'static str, &'static str),
{
    auto_rig_mesh_with_progress_cancellable(spec, on_progress, &|| false)
}

/// Executes the built-in pure-Rust auto-rigging engine with cooperative cancellation polling.
pub fn auto_rig_mesh_with_progress_cancellable<F>(
    spec: &AutoRigSpec,
    mut on_progress: F,
    should_stop: &dyn Fn() -> bool,
) -> Result<Vec<u8>, WorkerError>
where
    F: FnMut(f32, &'static str, &'static str),
{
    if should_stop() {
        return Err(WorkerError::Cancelled("Task cancelled".to_string()));
    }

    // 0. Validate AutoRig options upfront (zero silent no-ops)
    let provider = spec.parse_provider().map_err(WorkerError::AutoRig)?;
    if provider == AutoRigProvider::Blender {
        return Err(WorkerError::AutoRig(
            "preferred_provider 'blender' requires async TaskRunner execution".to_string(),
        ));
    }
    let preset = RigPreset::try_from_str(&spec.rig_preset).map_err(WorkerError::AutoRig)?;
    AutoRigSpec::validate_animation_clips(&spec.embed_animations).map_err(WorkerError::AutoRig)?;

    // 1. Parse input mesh geometry
    let raw_mesh = load_mesh_from_bytes_cancellable(spec.input_mesh.as_bytes(), should_stop)
        .map_err(|e| map_stage_err("Failed to parse input mesh", e))?;
    if should_stop() {
        return Err(WorkerError::Cancelled("Task cancelled".to_string()));
    }

    // 2. Feature F11: Extract anatomical landmarks and symmetry (25%)
    let landmarks = extract_landmarks_cancellable(&raw_mesh, preset, should_stop)
        .map_err(|e| map_stage_err("Landmark extraction failed", e))?;
    on_progress(
        0.25,
        "landmark_extraction",
        "PCA symmetry and anatomical landmarks extracted",
    );
    if should_stop() {
        return Err(WorkerError::Cancelled("Task cancelled".to_string()));
    }

    // 3. Feature F12: Build DAG armature hierarchy & IBMs (50%)
    let armature = build_standard_armature(&landmarks, preset);
    let bone_segments = build_bone_segments(&armature, &landmarks);
    on_progress(
        0.50,
        "armature_ibm",
        "DAG armature hierarchy and inverse bind matrices computed",
    );
    if should_stop() {
        return Err(WorkerError::Cancelled("Task cancelled".to_string()));
    }

    // 4. Feature F13: Geodesic skin weighting & top-4 pruning (75%)
    let skin_weights = compute_skin_weights_cancellable(
        &raw_mesh,
        &bone_segments,
        &SkinningConfig::default(),
        should_stop,
    )
    .map_err(|e| map_stage_err("Skin weighting failed", e))?;
    on_progress(
        0.75,
        "geodesic_skinning",
        "Geodesic surface skin weights computed with topological barrier",
    );
    if should_stop() {
        return Err(WorkerError::Cancelled("Task cancelled".to_string()));
    }

    // 5. Feature F14: Procedural TRS animation track synthesis
    let clips = ClipSynthesizer::synthesize_all_cancellable(
        &spec.embed_animations,
        landmarks.height,
        &armature,
        should_stop,
    )
    .map_err(|e| map_stage_err("Animation synthesis failed", e))?;

    // 6. Feature F15: Rigged glTF 2.0 Binary (.glb) serialization (100%)
    let glb_bytes = export_rigged_glb_cancellable(
        &raw_mesh,
        &armature,
        &skin_weights,
        &clips,
        None,
        should_stop,
    )
    .map_err(|e| map_stage_err("glTF binary export failed", e))?;
    on_progress(
        1.00,
        "animation_export",
        "TRS keyframes synthesized and rigged glTF 2.0 container exported",
    );

    Ok(glb_bytes)
}

/// Synchronous entry point: executes the built-in pure-Rust auto-rigging engine.
pub fn auto_rig_mesh(spec: &AutoRigSpec) -> Result<Vec<u8>, WorkerError> {
    auto_rig_mesh_with_progress(spec, |_, _, _| {})
}

/// Asynchronous entry point preserving backwards-compatible signature (60s default timeout).
pub async fn auto_rig_mesh_async<F>(
    runner: &crate::runner::TaskRunner,
    task_id: rusty_grid_core::task::TaskId,
    spec: &AutoRigSpec,
    on_progress: F,
) -> Result<Vec<u8>, WorkerError>
where
    F: FnMut(f32, &'static str, &str) + Send + 'static,
{
    auto_rig_mesh_async_supervised(
        runner,
        task_id,
        spec,
        Duration::from_secs(60),
        None,
        None,
        Instant::now(),
        on_progress,
    )
    .await
    .map(|outcome| outcome.glb_bytes)
}

/// Supervised asynchronous entry point with honest provider routing, timeout, PID, and cancellation.
#[allow(clippy::too_many_arguments)]
pub async fn auto_rig_mesh_async_supervised<F>(
    runner: &crate::runner::TaskRunner,
    task_id: rusty_grid_core::task::TaskId,
    spec: &AutoRigSpec,
    timeout: Duration,
    child_pid: Option<Arc<AtomicU32>>,
    mut cancel_rx: Option<oneshot::Receiver<String>>,
    start_time: Instant,
    mut on_progress: F,
) -> Result<AutoRigOutcome, WorkerError>
where
    F: FnMut(f32, &'static str, &str) + Send + 'static,
{
    if let Some(reason) = crate::runner::check_cancelled_now(&mut cancel_rx) {
        return Err(WorkerError::Cancelled(reason));
    }

    // Validate options upfront before probing/spawning any provider.
    let provider = spec.parse_provider().map_err(WorkerError::AutoRig)?;
    RigPreset::try_from_str(&spec.rig_preset).map_err(WorkerError::AutoRig)?;
    AutoRigSpec::validate_animation_clips(&spec.embed_animations).map_err(WorkerError::AutoRig)?;
    if spec.input_mesh.is_empty() {
        return Err(WorkerError::AutoRig(
            "AutoRig input_mesh cannot be empty".to_string(),
        ));
    }

    let formatted_timeout_secs = timeout.as_secs().max(u64::from(!timeout.is_zero()));
    let deadline = tokio::time::Instant::now() + timeout;

    if matches!(provider, AutoRigProvider::Blender | AutoRigProvider::Auto) {
        if let Some(blender_bin) =
            blender_provider::resolve_blender_binary(runner.config().blender_path.as_deref())
        {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(WorkerError::TimedOut(format!(
                    "Task timed out after {formatted_timeout_secs}s",
                )));
            }

            info!(bin = ?blender_bin, "Attempting Blender headless auto-rigging provider");
            on_progress(
                0.10,
                "analyzing_mesh",
                "Executing Blender headless auto-rigging provider",
            );

            let (blender_cancel_tx, blender_cancel_rx) = oneshot::channel::<String>();
            let mut blender_cancel_tx = Some(blender_cancel_tx);
            let blender_fut = BlenderRigProvider::execute_rigging_supervised(
                runner,
                task_id,
                &blender_bin,
                spec.input_mesh.as_bytes(),
                &spec.rig_preset,
                &spec.embed_animations,
                remaining,
                child_pid.clone(),
                Some(blender_cancel_rx),
                start_time,
            );
            tokio::pin!(blender_fut);

            let blender_res = tokio::select! {
                biased;
                reason = crate::runner::cancellation(&mut cancel_rx) => {
                    if let Some(tx) = blender_cancel_tx.take() {
                        let _ = tx.send(reason);
                    }
                    blender_fut.await
                }
                res = &mut blender_fut => res,
            };

            match blender_res {
                Ok(glb) => {
                    info!("Blender auto-rigging completed successfully");
                    on_progress(
                        1.00,
                        "completed",
                        "Blender auto-rigging completed successfully",
                    );
                    return Ok(AutoRigOutcome {
                        glb_bytes: glb,
                        provider: AutoRigUsedProvider::Blender,
                    });
                }
                Err(e @ WorkerError::Cancelled(_)) => {
                    return Err(e);
                }
                Err(WorkerError::TimedOut(_)) => {
                    return Err(WorkerError::TimedOut(format!(
                        "Task timed out after {formatted_timeout_secs}s",
                    )));
                }
                Err(e) => {
                    if provider == AutoRigProvider::Blender {
                        return Err(e);
                    }
                    warn!(
                        "Blender auto-rigging failed ({e}); falling back to built-in pure-Rust engine"
                    );
                }
            }
        } else {
            if provider == AutoRigProvider::Blender {
                return Err(WorkerError::AutoRig(
                    "Blender auto-rigging provider was requested explicitly, but no Blender executable was found on this worker"
                        .to_string(),
                ));
            }
            warn!("Blender executable not found on host system; falling back to built-in pure-Rust engine");
        }
    }

    // Built-in pure-Rust rigging engine (for Builtin / None, or Auto fallback)
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return Err(WorkerError::TimedOut(format!(
            "Task timed out after {formatted_timeout_secs}s",
        )));
    }

    let stop = Arc::new(AtomicBool::new(false));
    let _stop_on_drop = AutoRigStopOnDrop(Arc::clone(&stop));
    let stop_clone = Arc::clone(&stop);
    let spec_clone = spec.clone();

    let mut blocking_handle = tokio::task::spawn_blocking(move || {
        auto_rig_mesh_with_progress_cancellable(&spec_clone, on_progress, &|| {
            stop_clone.load(Ordering::Relaxed)
        })
    });

    tokio::select! {
        biased;
        reason = crate::runner::cancellation(&mut cancel_rx) => {
            stop.store(true, Ordering::Release);
            let _ = tokio::time::timeout(Duration::from_millis(200), &mut blocking_handle).await;
            Err(WorkerError::Cancelled(reason))
        }
        _ = tokio::time::sleep(remaining) => {
            stop.store(true, Ordering::Release);
            let _ = tokio::time::timeout(Duration::from_millis(200), &mut blocking_handle).await;
            Err(WorkerError::TimedOut(format!(
                "Task timed out after {formatted_timeout_secs}s",
            )))
        }
        joined = &mut blocking_handle => {
            match joined {
                Ok(Ok(glb_bytes)) => Ok(AutoRigOutcome {
                    glb_bytes,
                    provider: AutoRigUsedProvider::Builtin,
                }),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(WorkerError::AutoRig(format!("Join error: {e}"))),
            }
        }
    }
}
