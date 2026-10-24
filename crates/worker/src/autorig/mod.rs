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
pub use gltf_export::{export_rigged_glb, CHUNK_TYPE_BIN, CHUNK_TYPE_JSON, GLB_MAGIC};
pub use landmark::extract_landmarks;
pub use math::{eigen_decomposition_3x3, eigen_symmetric_3x3, EigenDecomposition3, Mat4, Quat, Vec3};
pub use mesh_loader::load_mesh_from_bytes;
pub use skinning::{compute_skin_weights, SkinningConfig};
pub use types::{
    AnatomicalLandmarks, Armature, BoneSegment, Joint, LimbSide, RigPreset, SkinWeights, WorkerError,
};

use rusty_grid_core::task::AutoRigSpec;
use tracing::{info, warn};

/// Executes the built-in pure-Rust auto-rigging engine with progressive milestone callback reporting:
/// - 25%: `landmark_extraction`
/// - 50%: `armature_ibm`
/// - 75%: `geodesic_skinning`
/// - 100%: `animation_export`
pub fn auto_rig_mesh_with_progress<F>(
    spec: &AutoRigSpec,
    mut on_progress: F,
) -> Result<Vec<u8>, WorkerError>
where
    F: FnMut(f32, &'static str, &'static str),
{
    // 1. Parse input mesh geometry
    let raw_mesh = load_mesh_from_bytes(spec.input_mesh.as_bytes())
        .map_err(|e| WorkerError::AutoRig(format!("Failed to parse input mesh: {e}")))?;

    // 2. Resolve skeletal rig preset
    let preset = RigPreset::from_str(&spec.rig_preset);

    // 3. Feature F11: Extract anatomical landmarks and symmetry (25%)
    let landmarks = extract_landmarks(&raw_mesh, preset)
        .map_err(|e| WorkerError::AutoRig(format!("Landmark extraction failed: {e}")))?;
    on_progress(0.25, "landmark_extraction", "PCA symmetry and 15 anatomical landmarks extracted");

    // 4. Feature F12: Build standard DAG armature hierarchy & IBMs (50%)
    let armature = build_standard_armature(&landmarks, preset);
    let bone_segments = build_bone_segments(&armature, &landmarks);
    on_progress(0.50, "armature_ibm", "Standard 15-joint DAG armature and inverse bind matrices computed");

    // 5. Feature F13: Geodesic skin weighting & top-4 pruning (75%)
    let skin_weights = compute_skin_weights(&raw_mesh, &bone_segments, &SkinningConfig::default())
        .map_err(|e| WorkerError::AutoRig(format!("Skin weighting failed: {e}")))?;
    on_progress(0.75, "geodesic_skinning", "Geodesic surface skin weights computed with topological barrier");

    // 6. Feature F14: Procedural TRS animation track synthesis
    let clips = ClipSynthesizer::synthesize_all(&spec.embed_animations, landmarks.height, &armature);

    // 7. Feature F15: Rigged glTF 2.0 Binary (.glb) serialization (100%)
    let glb_bytes = export_rigged_glb(&raw_mesh, &armature, &skin_weights, &clips, None)
        .map_err(|e| WorkerError::AutoRig(format!("glTF binary export failed: {e}")))?;
    on_progress(1.00, "animation_export", "TRS keyframes synthesized and rigged glTF 2.0 container exported");

    Ok(glb_bytes)
}

/// Synchronous entry point: executes the built-in pure-Rust auto-rigging engine.
///
/// 1. Parses input 3D mesh (GLB or OBJ) into `RawMesh`.
/// 2. Performs PCA orientation and 50-slab vertical slicing to extract 15 anatomical landmarks.
/// 3. Builds DAG joint hierarchy and calculates column-major 4x4 Inverse Bind Matrices (IBM).
/// 4. Computes geodesic surface gated skin weights with sagittal barrier and top-4 unity normalization.
/// 5. Synthesizes requested procedural TRS animation tracks (`idle`, `walk`, `turnaround`).
/// 6. Packages and serializes all structures into a compliant binary glTF 2.0 (.glb) container.
pub fn auto_rig_mesh(spec: &AutoRigSpec) -> Result<Vec<u8>, WorkerError> {
    auto_rig_mesh_with_progress(spec, |_, _, _| {})
}

/// Asynchronous entry point with provider routing:
/// Supports headless Blender 4.5.4 LTS fallback (F16) when requested,
/// with automatic deterministic fallback to the built-in pure-Rust rigging engine.
pub async fn auto_rig_mesh_async(spec: &AutoRigSpec) -> Result<Vec<u8>, WorkerError> {
    let pref = spec.preferred_provider.as_deref().unwrap_or("builtin").to_lowercase();

    if pref == "blender" {
        if let Some(blender_bin) = blender_provider::resolve_blender_binary() {
            info!(bin = ?blender_bin, "Attempting Blender headless auto-rigging provider");
            match BlenderRigProvider::execute_rigging(
                &blender_bin,
                spec.input_mesh.as_bytes(),
                &spec.rig_preset,
                &spec.embed_animations,
                60,
            )
            .await
            {
                Ok(glb) => {
                    info!("Blender auto-rigging completed successfully");
                    return Ok(glb);
                }
                Err(e) => {
                    warn!("Blender auto-rigging failed ({e}); falling back to built-in pure-Rust engine");
                }
            }
        } else {
            warn!("Blender executable not found on host system; falling back to built-in pure-Rust engine");
        }
    }

    // Default or fallback: Built-in pure-Rust rigging engine
    auto_rig_mesh(spec)
}
