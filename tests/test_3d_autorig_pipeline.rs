//! End-to-End Integration, glTF 2.0 Validation & Coverage Hardening Test Suite (Feature F22).
//!
//! Validates:
//! 1. `test_e2e_generative_3d_master_to_worker`: Submits TaskSpec::Generative3D, checks progressive checkpoints (25%, 50%, 75%, 100%), extracts zero-copy Bytes asset, runs glTF validator.
//! 2. `test_e2e_auto_rig_master_to_worker`: Chains mesh into TaskSpec::AutoRig, checks progressive checkpoints, extracts rigged .glb, runs deep glTF 2.0 rigging and animation validator.
//! 3. `test_e2e_agent_mesh_bridge_3d_and_autorig`: Executes grid_3d_generate and grid_auto_rig via AgentMeshClient / AgentGridBridge, verifies intermediate ACK, Base64 decoding, and SHA-256 integrity.
//! 4. `test_e2e_cli_submit_3d_and_disk_export`: Exercises run_submit with --prompt, --quality fast, --engine sdf, --wait, --output <temp_dir>/output.glb, verifies file written to disk, validate_glb_header pass, and full glTF validation pass.
//! 5. `test_e2e_checkpoint_monotonic_progress`: Verifies checkpoints strictly progress [1, 2, 3, 4] and [25%, 50%, 75%, 100%].
//! 6. `test_boundary_and_corrupted_mesh_rejection`: Corrupted input bytes properly rejected with error, no panics.
//! 7. `test_adversarial_single_bit_tamper_detection`: 1-bit flip in binary payload triggers SHA-256 verification failure.
//! 8. `test_adversarial_worker_crash_rescheduling`: Simulated worker disconnect triggers task rescheduling by Master.
//! 9. `test_adversarial_validator_structural_robustness`: Adversarially stresses glTF 2.0 validator against singular/invalid IBMs, unnormalized skin weights, non-monotonic animation timestamps, and corrupted geometries.

#![allow(
    unknown_lints,
    clippy::manual_is_multiple_of,
    clippy::needless_borrows_for_generic_args,
    clippy::needless_range_loop
)]

use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;
use tempfile::TempDir;
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

use agent_mesh::bridge::AgentGridBridge;
use agent_mesh::client::AgentMeshClient;
use agent_mesh::hub::AgentMeshHub;
use agent_mesh::protocol::{compute_sha256, verify_sha256, AgentMeshEnvelope};

use rusty_grid_cli::cli_app::{run_submit, validate_glb_header, SubmitArgs};
use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::WorkerMessage;
use rusty_grid_core::task::{AutoRigSpec, Generative3DSpec, Task, TaskRequirements, TaskSpec};
use rusty_grid_master::queue::TaskState;
use rusty_grid_master::registry::WorkerStatus;
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};
use rusty_grid_worker::runner::{
    RunnerConfig, TaskRunner, EXIT_CODE_GENERAL_ERROR, EXIT_CODE_SUCCESS,
};

// =========================================================================
// MODULE 1: STANDALONE ZERO-DEPENDENCY GLTF 2.0 BINARY VALIDATOR
// =========================================================================

pub const GLB_MAGIC: u32 = 0x46546C67; // "glTF" in little-endian
pub const GLB_VERSION: u32 = 2;
pub const CHUNK_TYPE_JSON: u32 = 0x4E4F534A; // "JSON"
pub const CHUNK_TYPE_BIN: u32 = 0x004E4942; // "BIN\0"

pub const COMPONENT_TYPE_UNSIGNED_BYTE: u32 = 5121;
pub const COMPONENT_TYPE_UNSIGNED_SHORT: u32 = 5123;
pub const COMPONENT_TYPE_UNSIGNED_INT: u32 = 5125;
pub const COMPONENT_TYPE_FLOAT: u32 = 5126;

/// Parsed glTF 2.0 Binary Container.
#[derive(Debug)]
pub struct ParsedGlb<'a> {
    pub total_length: usize,
    pub json: Value,
    pub bin_data: &'a [u8],
}

/// Verification report for mesh geometry.
#[derive(Debug, Clone)]
pub struct MeshValidationReport {
    pub vertex_count: usize,
    pub triangle_count: usize,
    pub has_normals: bool,
    pub has_uvs: bool,
    pub bounding_box_min: [f32; 3],
    pub bounding_box_max: [f32; 3],
}

/// Verification report for skeletal skinning and rigging.
#[derive(Debug, Clone)]
pub struct SkinValidationReport {
    pub joint_count: usize,
    pub joint_names: Vec<String>,
    pub inverse_bind_matrices_count: usize,
    pub weights_verified_count: usize,
    pub max_weight_sum_error: f32,
}

/// Verification report for animation clips and tracks.
#[derive(Debug, Clone)]
pub struct AnimationValidationReport {
    pub clip_names: Vec<String>,
    pub total_channels: usize,
    pub total_samplers: usize,
    pub durations: Vec<f32>,
}

/// Complete glTF validation summary.
#[derive(Debug, Clone)]
pub struct RiggedGlbSummary {
    pub mesh: MeshValidationReport,
    pub skin: SkinValidationReport,
    pub animations: AnimationValidationReport,
}

/// Parses and validates the glTF 2.0 binary container structure.
pub fn parse_and_validate_glb_container(raw: &[u8]) -> Result<ParsedGlb<'_>, String> {
    if raw.len() < 12 {
        return Err(format!(
            "GLB buffer truncated: length {} < 12 header bytes",
            raw.len()
        ));
    }

    // 1. Header Magic: 0x46546C67 ("glTF")
    let magic = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
    if magic != GLB_MAGIC {
        return Err(format!(
            "Invalid glTF magic: expected 0x{GLB_MAGIC:08X}, got 0x{magic:08X}"
        ));
    }

    // 2. Version: 2
    let version = u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]);
    if version != GLB_VERSION {
        return Err(format!(
            "Unsupported glTF version: expected {GLB_VERSION}, got {version}"
        ));
    }

    // 3. Total Length
    let total_len = u32::from_le_bytes([raw[8], raw[9], raw[10], raw[11]]) as usize;
    if total_len != raw.len() {
        return Err(format!(
            "GLB container length mismatch: header specifies {total_len} bytes, actual buffer is {} bytes",
            raw.len()
        ));
    }

    // 4. Chunk 0: JSON
    if raw.len() < 20 {
        return Err("GLB buffer truncated before Chunk 0 header".into());
    }
    let chunk0_len = u32::from_le_bytes([raw[12], raw[13], raw[14], raw[15]]) as usize;
    let chunk0_type = u32::from_le_bytes([raw[16], raw[17], raw[18], raw[19]]);
    if chunk0_type != CHUNK_TYPE_JSON {
        return Err(format!(
            "Invalid Chunk 0 type: expected JSON 0x{CHUNK_TYPE_JSON:08X}, got 0x{chunk0_type:08X}"
        ));
    }
    if chunk0_len % 4 != 0 {
        return Err(format!(
            "Chunk 0 length ({chunk0_len}) is not 4-byte aligned"
        ));
    }
    let chunk0_end = 20 + chunk0_len;
    if chunk0_end > raw.len() {
        return Err(format!(
            "Chunk 0 exceeds container bounds: offset {chunk0_end} > total {}",
            raw.len()
        ));
    }

    let json_slice = &raw[20..chunk0_end];
    let json_val: Value = serde_json::from_slice(json_slice)
        .map_err(|e| format!("Failed to parse Chunk 0 JSON metadata: {e}"))?;

    let asset_ver = json_val["asset"]["version"].as_str().unwrap_or("");
    if asset_ver != "2.0" {
        return Err(format!(
            "Invalid glTF asset version '{asset_ver}': expected '2.0'"
        ));
    }

    // 5. Chunk 1: BIN
    let bin_data = if chunk0_end < raw.len() {
        if chunk0_end + 8 > raw.len() {
            return Err("GLB buffer truncated before Chunk 1 header".into());
        }
        let chunk1_len = u32::from_le_bytes([
            raw[chunk0_end],
            raw[chunk0_end + 1],
            raw[chunk0_end + 2],
            raw[chunk0_end + 3],
        ]) as usize;
        let chunk1_type = u32::from_le_bytes([
            raw[chunk0_end + 4],
            raw[chunk0_end + 5],
            raw[chunk0_end + 6],
            raw[chunk0_end + 7],
        ]);
        if chunk1_type != CHUNK_TYPE_BIN {
            return Err(format!(
                "Invalid Chunk 1 type: expected BIN 0x{CHUNK_TYPE_BIN:08X}, got 0x{chunk1_type:08X}"
            ));
        }
        if chunk1_len % 4 != 0 {
            return Err(format!(
                "Chunk 1 length ({chunk1_len}) is not 4-byte aligned"
            ));
        }
        let chunk1_start = chunk0_end + 8;
        let chunk1_end = chunk1_start + chunk1_len;
        if chunk1_end != raw.len() {
            return Err(format!(
                "Chunk 1 bounds ({chunk1_end}) do not match total container length ({})",
                raw.len()
            ));
        }
        &raw[chunk1_start..chunk1_end]
    } else {
        &[][..]
    };

    // 6. BufferViews 4-byte alignment and bounds
    if let Some(bvs) = json_val.get("bufferViews").and_then(|v| v.as_array()) {
        for (i, bv) in bvs.iter().enumerate() {
            let offset = bv.get("byteOffset").and_then(|o| o.as_u64()).unwrap_or(0) as usize;
            let length = bv.get("byteLength").and_then(|l| l.as_u64()).unwrap_or(0) as usize;
            if offset % 4 != 0 {
                return Err(format!(
                    "BufferView {i} byteOffset ({offset}) is not 4-byte aligned"
                ));
            }
            if offset + length > bin_data.len() {
                return Err(format!(
                    "BufferView {i} extent ({} bytes) exceeds BIN chunk length ({} bytes)",
                    offset + length,
                    bin_data.len()
                ));
            }
        }
    }

    Ok(ParsedGlb {
        total_length: total_len,
        json: json_val,
        bin_data,
    })
}

/// Validates geometric attributes of mesh primitives (POSITION, NORMAL, TEXCOORD_0, indices, bounds).
pub fn validate_glb_mesh(glb: &ParsedGlb<'_>) -> Result<MeshValidationReport, String> {
    let meshes = glb
        .json
        .get("meshes")
        .and_then(|m| m.as_array())
        .ok_or("glTF missing 'meshes' array")?;
    if meshes.is_empty() {
        return Err("'meshes' array is empty".into());
    }

    let primitives = meshes[0]
        .get("primitives")
        .and_then(|p| p.as_array())
        .ok_or("Mesh[0] missing 'primitives' array")?;
    if primitives.is_empty() {
        return Err("Mesh[0] 'primitives' array is empty".into());
    }

    let prim = &primitives[0];
    let attributes = prim
        .get("attributes")
        .and_then(|a| a.as_object())
        .ok_or("Primitive missing 'attributes' object")?;

    let mode = prim.get("mode").and_then(|m| m.as_u64()).unwrap_or(4);
    if mode != 4 {
        return Err(format!(
            "Unsupported primitive mode {mode}; expected 4 (TRIANGLES)"
        ));
    }

    // 1. POSITION Attribute
    let pos_acc_idx = attributes
        .get("POSITION")
        .and_then(|p| p.as_u64())
        .ok_or("Primitive missing 'POSITION' attribute")? as usize;
    let (pos_count, pos_floats) =
        read_accessor_floats(glb, pos_acc_idx, "VEC3", COMPONENT_TYPE_FLOAT)?;
    if pos_count < 3 {
        return Err(format!("POSITION vertex count ({pos_count}) < 3"));
    }

    // Verify finite values and compute actual bounding box
    let mut actual_min = [f32::INFINITY; 3];
    let mut actual_max = [f32::NEG_INFINITY; 3];
    for chunk in pos_floats.chunks_exact(3) {
        for k in 0..3 {
            let v = chunk[k];
            if !v.is_finite() {
                return Err(format!("POSITION contains non-finite float {v}"));
            }
            actual_min[k] = actual_min[k].min(v);
            actual_max[k] = actual_max[k].max(v);
        }
    }

    // Compare with declared min/max in accessor
    let pos_acc = &glb.json["accessors"][pos_acc_idx];
    if let (Some(decl_min_arr), Some(decl_max_arr)) = (
        pos_acc.get("min").and_then(|m| m.as_array()),
        pos_acc.get("max").and_then(|m| m.as_array()),
    ) {
        for k in 0..3 {
            let decl_min = decl_min_arr.get(k).and_then(|v| v.as_f64()).unwrap_or(0.0) as f32;
            let decl_max = decl_max_arr.get(k).and_then(|v| v.as_f64()).unwrap_or(0.0) as f32;
            if (decl_min - actual_min[k]).abs() > 1e-3 {
                return Err(format!(
                    "POSITION accessor min[{k}] mismatch: declared {decl_min}, actual {}",
                    actual_min[k]
                ));
            }
            if (decl_max - actual_max[k]).abs() > 1e-3 {
                return Err(format!(
                    "POSITION accessor max[{k}] mismatch: declared {decl_max}, actual {}",
                    actual_max[k]
                ));
            }
        }
    }

    // 2. NORMAL Attribute
    let has_normals = if let Some(norm_idx) = attributes.get("NORMAL").and_then(|n| n.as_u64()) {
        let (norm_count, norm_floats) =
            read_accessor_floats(glb, norm_idx as usize, "VEC3", COMPONENT_TYPE_FLOAT)?;
        if norm_count != pos_count {
            return Err(format!(
                "NORMAL count ({norm_count}) != POSITION count ({pos_count})"
            ));
        }
        for chunk in norm_floats.chunks_exact(3) {
            let len_sq = chunk[0] * chunk[0] + chunk[1] * chunk[1] + chunk[2] * chunk[2];
            let len = len_sq.sqrt();
            if (len - 1.0).abs() > 0.08 {
                return Err(format!(
                    "NORMAL vector [{:?}] has non-unit length {len}",
                    chunk
                ));
            }
        }
        true
    } else {
        false
    };

    // 3. TEXCOORD_0 Attribute
    let has_uvs = if let Some(uv_idx) = attributes.get("TEXCOORD_0").and_then(|u| u.as_u64()) {
        let (uv_count, uv_floats) =
            read_accessor_floats(glb, uv_idx as usize, "VEC2", COMPONENT_TYPE_FLOAT)?;
        if uv_count != pos_count {
            return Err(format!(
                "TEXCOORD_0 count ({uv_count}) != POSITION count ({pos_count})"
            ));
        }
        for &u in &uv_floats {
            if !u.is_finite() {
                return Err(format!("TEXCOORD_0 contains non-finite float {u}"));
            }
        }
        true
    } else {
        false
    };

    // 4. Triangle Indices
    let triangle_count = if let Some(idx_acc_val) = prim.get("indices").and_then(|i| i.as_u64()) {
        let indices = read_accessor_indices(glb, idx_acc_val as usize)?;
        if indices.len() % 3 != 0 {
            return Err(format!(
                "Indices count ({}) is not a multiple of 3",
                indices.len()
            ));
        }
        for &idx in &indices {
            if idx as usize >= pos_count {
                return Err(format!(
                    "Index {idx} out of bounds for vertex count {pos_count}"
                ));
            }
        }
        indices.len() / 3
    } else {
        pos_count / 3
    };

    Ok(MeshValidationReport {
        vertex_count: pos_count,
        triangle_count,
        has_normals,
        has_uvs,
        bounding_box_min: actual_min,
        bounding_box_max: actual_max,
    })
}

/// Validates skinning, 15-joint armature DAG hierarchy, IBM matrices, and normalized vertex weights.
pub fn validate_glb_rigging_and_skin(
    glb: &ParsedGlb<'_>,
    expected_joint_count: usize,
) -> Result<SkinValidationReport, String> {
    // 1. Skins array
    let skins = glb
        .json
        .get("skins")
        .and_then(|s| s.as_array())
        .ok_or("Missing 'skins' array in glTF")?;
    if skins.len() != 1 {
        return Err(format!("Expected 1 skin, got {}", skins.len()));
    }

    let skin = &skins[0];
    let joints_arr = skin
        .get("joints")
        .and_then(|j| j.as_array())
        .ok_or("Skin missing 'joints' array")?;
    if joints_arr.len() != expected_joint_count {
        return Err(format!(
            "Skin joints count ({}) != expected ({expected_joint_count})",
            joints_arr.len()
        ));
    }

    // Verify joint nodes in nodes array
    let nodes = glb
        .json
        .get("nodes")
        .and_then(|n| n.as_array())
        .ok_or("Missing 'nodes' array in glTF")?;
    let mut joint_names = Vec::with_capacity(joints_arr.len());
    for (i, j_val) in joints_arr.iter().enumerate() {
        let node_idx = j_val
            .as_u64()
            .ok_or_else(|| format!("Invalid joint node index at {i}"))?
            as usize;
        let node = nodes
            .get(node_idx)
            .ok_or_else(|| format!("Joint node {node_idx} out of bounds"))?;
        let name = node
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or("UnnamedJoint");
        joint_names.push(name.to_string());
    }

    if expected_joint_count == 15 {
        let expected_names = [
            "Hips",
            "Spine",
            "Chest",
            "Neck",
            "Head",
            "UpperLeg_L",
            "LowerLeg_L",
            "Foot_L",
            "UpperLeg_R",
            "LowerLeg_R",
            "Foot_R",
            "UpperArm_L",
            "LowerArm_L",
            "UpperArm_R",
            "LowerArm_R",
        ];
        for (i, &exp_name) in expected_names.iter().enumerate() {
            if joint_names[i] != exp_name {
                return Err(format!(
                    "Joint {i} name mismatch: expected '{exp_name}', got '{}'",
                    joint_names[i]
                ));
            }
        }
    }

    // 2. Inverse Bind Matrices (MAT4, FLOAT 5126)
    let ibm_acc_idx = skin
        .get("inverseBindMatrices")
        .and_then(|i| i.as_u64())
        .ok_or("Skin missing 'inverseBindMatrices' accessor")? as usize;
    let (ibm_count, ibm_floats) =
        read_accessor_floats(glb, ibm_acc_idx, "MAT4", COMPONENT_TYPE_FLOAT)?;
    if ibm_count != expected_joint_count {
        return Err(format!(
            "InverseBindMatrices count ({ibm_count}) != joint count ({expected_joint_count})"
        ));
    }

    // Validate 4x4 column-major affine matrices (bottom row entries [0, 0, 0, 1] within 1e-4)
    for (j_idx, mat_chunk) in ibm_floats.chunks_exact(16).enumerate() {
        let r30 = mat_chunk[3];
        let r31 = mat_chunk[7];
        let r32 = mat_chunk[11];
        let r33 = mat_chunk[15];

        if r30.abs() > 1e-4 || r31.abs() > 1e-4 || r32.abs() > 1e-4 || (r33 - 1.0).abs() > 1e-4 {
            return Err(format!(
                "Joint {j_idx} IBM bottom row is not affine [0,0,0,1]: [{r30}, {r31}, {r32}, {r33}]"
            ));
        }

        for (k, &f) in mat_chunk.iter().enumerate() {
            if !f.is_finite() {
                return Err(format!("Joint {j_idx} IBM element {k} is non-finite: {f}"));
            }
        }

        // Verify non-singular / invertible matrix (det of 3x3 rotational submatrix != 0)
        let m0 = mat_chunk[0];
        let m1 = mat_chunk[1];
        let m2 = mat_chunk[2];
        let m4 = mat_chunk[4];
        let m5 = mat_chunk[5];
        let m6 = mat_chunk[6];
        let m8 = mat_chunk[8];
        let m9 = mat_chunk[9];
        let m10 = mat_chunk[10];

        let det = m0 * (m5 * m10 - m6 * m9) - m4 * (m1 * m10 - m2 * m9) + m8 * (m1 * m6 - m2 * m5);
        if det.abs() < 1e-6 {
            return Err(format!(
                "Joint {j_idx} IBM matrix is singular or non-invertible (det = {det})"
            ));
        }
    }

    // 3. Primitive Skinned Attributes: JOINTS_0 and WEIGHTS_0
    let prim = &glb.json["meshes"][0]["primitives"][0];
    let attributes = prim
        .get("attributes")
        .and_then(|a| a.as_object())
        .ok_or("Missing primitive attributes")?;

    let joints_acc_idx = attributes
        .get("JOINTS_0")
        .and_then(|j| j.as_u64())
        .ok_or("Primitive missing 'JOINTS_0' attribute")? as usize;
    let weights_acc_idx = attributes
        .get("WEIGHTS_0")
        .and_then(|w| w.as_u64())
        .ok_or("Primitive missing 'WEIGHTS_0' attribute")? as usize;

    let joints_vec = read_accessor_vec4_u16(glb, joints_acc_idx)?;
    let (weights_count, weights_floats) =
        read_accessor_floats(glb, weights_acc_idx, "VEC4", COMPONENT_TYPE_FLOAT)?;

    if joints_vec.len() != weights_count {
        return Err(format!(
            "JOINTS_0 count ({}) != WEIGHTS_0 count ({weights_count})",
            joints_vec.len()
        ));
    }

    // Verify joint index range (< expected_joint_count)
    for (v_idx, &j_arr) in joints_vec.iter().enumerate() {
        for k in 0..4 {
            if j_arr[k] as usize >= expected_joint_count {
                return Err(format!(
                    "Vertex {v_idx} JOINTS_0[{k}] ({}) >= expected joint count ({expected_joint_count})",
                    j_arr[k]
                ));
            }
        }
    }

    // Verify WEIGHTS_0 unity normalization: sum(w_k) == 1.000000 +- 1e-4
    let mut max_err = 0.0f32;
    for (v_idx, chunk) in weights_floats.chunks_exact(4).enumerate() {
        let sum = chunk[0] + chunk[1] + chunk[2] + chunk[3];
        let err = (sum - 1.0).abs();
        max_err = max_err.max(err);
        if err > 1e-4 {
            return Err(format!(
                "Vertex {v_idx} skin weights [{:?}] sum to {sum} (error {err} > 1e-4)",
                chunk
            ));
        }
        for k in 0..4 {
            if chunk[k] < -1e-5 || chunk[k] > 1.0 + 1e-5 {
                return Err(format!(
                    "Vertex {v_idx} weight[{k}] ({}) out of range [0.0, 1.0]",
                    chunk[k]
                ));
            }
        }
    }

    Ok(SkinValidationReport {
        joint_count: expected_joint_count,
        joint_names,
        inverse_bind_matrices_count: ibm_count,
        weights_verified_count: joints_vec.len(),
        max_weight_sum_error: max_err,
    })
}

/// Validates animation clips, channels, monotonic timestamps, and unit quaternions.
pub fn validate_glb_animations(
    glb: &ParsedGlb<'_>,
    expected_clips: &[&str],
) -> Result<AnimationValidationReport, String> {
    let anims = glb
        .json
        .get("animations")
        .and_then(|a| a.as_array())
        .ok_or("Missing 'animations' array in glTF")?;
    if anims.is_empty() {
        return Err("'animations' array is empty".into());
    }

    let mut clip_names = Vec::new();
    let mut durations = Vec::new();
    let mut total_channels = 0;
    let mut total_samplers = 0;

    for exp_clip in expected_clips {
        let anim = anims
            .iter()
            .find(|a| a.get("name").and_then(|n| n.as_str()) == Some(*exp_clip))
            .ok_or_else(|| format!("Expected animation clip '{exp_clip}' not found"))?;

        clip_names.push(exp_clip.to_string());

        let channels = anim
            .get("channels")
            .and_then(|c| c.as_array())
            .ok_or_else(|| format!("Clip '{exp_clip}' missing channels"))?;
        let samplers = anim
            .get("samplers")
            .and_then(|s| s.as_array())
            .ok_or_else(|| format!("Clip '{exp_clip}' missing samplers"))?;

        total_channels += channels.len();
        total_samplers += samplers.len();

        let mut clip_duration = 0.0f32;

        for (s_idx, sampler) in samplers.iter().enumerate() {
            let input_idx = sampler
                .get("input")
                .and_then(|i| i.as_u64())
                .ok_or_else(|| format!("Sampler {s_idx} missing 'input' accessor"))?
                as usize;
            let output_idx = sampler
                .get("output")
                .and_then(|o| o.as_u64())
                .ok_or_else(|| format!("Sampler {s_idx} missing 'output' accessor"))?
                as usize;

            // 1. Verify monotonic timestamps
            let (t_count, timestamps) =
                read_accessor_floats(glb, input_idx, "SCALAR", COMPONENT_TYPE_FLOAT)?;
            if t_count < 2 {
                return Err(format!(
                    "Clip '{exp_clip}' sampler {s_idx} timestamps count ({t_count}) < 2"
                ));
            }
            for i in 0..timestamps.len() - 1 {
                if timestamps[i + 1] <= timestamps[i] {
                    return Err(format!(
                        "Clip '{exp_clip}' sampler {s_idx} non-monotonic timestamps at frame {i}: t[{i}]={}, t[{}]={}",
                        timestamps[i],
                        i + 1,
                        timestamps[i + 1]
                    ));
                }
            }
            clip_duration = clip_duration.max(*timestamps.last().unwrap());

            // 2. Verify output keyframes
            let out_acc = &glb.json["accessors"][output_idx];
            let out_type = out_acc.get("type").and_then(|t| t.as_str()).unwrap_or("");
            let (out_count, out_floats) =
                read_accessor_floats(glb, output_idx, out_type, COMPONENT_TYPE_FLOAT)?;
            if out_count != t_count {
                return Err(format!(
                    "Clip '{exp_clip}' sampler {s_idx} keyframe count mismatch: input {t_count} != output {out_count}"
                ));
            }

            // Verify unit quaternions for rotation channels
            if out_type == "VEC4" {
                for (k_idx, q_chunk) in out_floats.chunks_exact(4).enumerate() {
                    let len_sq = q_chunk[0] * q_chunk[0]
                        + q_chunk[1] * q_chunk[1]
                        + q_chunk[2] * q_chunk[2]
                        + q_chunk[3] * q_chunk[3];
                    let len = len_sq.sqrt();
                    if (len - 1.0).abs() > 1e-3 {
                        return Err(format!(
                            "Clip '{exp_clip}' sampler {s_idx} keyframe {k_idx} rotation quaternion [{:?}] has non-unit norm {len}",
                            q_chunk
                        ));
                    }
                }
            }
        }

        // Verify channel targets
        for (c_idx, channel) in channels.iter().enumerate() {
            let target = channel
                .get("target")
                .and_then(|t| t.as_object())
                .ok_or_else(|| format!("Channel {c_idx} missing target"))?;
            let target_node = target
                .get("node")
                .and_then(|n| n.as_u64())
                .ok_or_else(|| format!("Channel {c_idx} missing target node"))?
                as usize;
            let target_path = target
                .get("path")
                .and_then(|p| p.as_str())
                .ok_or_else(|| format!("Channel {c_idx} missing target path"))?;

            if !["translation", "rotation", "scale"].contains(&target_path) {
                return Err(format!("Invalid channel target path '{target_path}'"));
            }
            if target_node >= 15 {
                return Err(format!("Channel {c_idx} target node {target_node} >= 15"));
            }
        }

        durations.push(clip_duration);
    }

    Ok(AnimationValidationReport {
        clip_names,
        total_channels,
        total_samplers,
        durations,
    })
}

/// Top-level validator for unrigged static meshes.
pub fn validate_unrigged_mesh(bytes: &[u8]) -> Result<MeshValidationReport, String> {
    let glb = parse_and_validate_glb_container(bytes)?;
    validate_glb_mesh(&glb)
}

/// Top-level validator for complete rigged and animated glTF assets.
pub fn validate_complete_rigged_glb(
    bytes: &[u8],
    expected_clips: &[&str],
) -> Result<RiggedGlbSummary, String> {
    let glb = parse_and_validate_glb_container(bytes)?;
    let mesh = validate_glb_mesh(&glb)?;
    let skin = validate_glb_rigging_and_skin(&glb, 15)?;
    let animations = validate_glb_animations(&glb, expected_clips)?;

    Ok(RiggedGlbSummary {
        mesh,
        skin,
        animations,
    })
}

// ---------------------------------------------------------------------------
// Accessor Binary Helper Decoders
// ---------------------------------------------------------------------------

fn read_accessor_floats(
    glb: &ParsedGlb<'_>,
    acc_idx: usize,
    expected_type: &str,
    expected_comp_type: u32,
) -> Result<(usize, Vec<f32>), String> {
    let accessors = glb
        .json
        .get("accessors")
        .and_then(|a| a.as_array())
        .ok_or("Missing accessors array")?;
    let acc = accessors
        .get(acc_idx)
        .ok_or_else(|| format!("Accessor index {acc_idx} out of bounds"))?;

    let count = acc.get("count").and_then(|c| c.as_u64()).unwrap_or(0) as usize;
    let actual_type = acc.get("type").and_then(|t| t.as_str()).unwrap_or("");
    let comp_type = acc
        .get("componentType")
        .and_then(|c| c.as_u64())
        .unwrap_or(0) as u32;

    if actual_type != expected_type {
        return Err(format!(
            "Accessor {acc_idx} type mismatch: expected {expected_type}, got {actual_type}"
        ));
    }
    if comp_type != expected_comp_type {
        return Err(format!(
            "Accessor {acc_idx} componentType mismatch: expected {expected_comp_type}, got {comp_type}"
        ));
    }

    let comp_count = match expected_type {
        "SCALAR" => 1,
        "VEC2" => 2,
        "VEC3" => 3,
        "VEC4" => 4,
        "MAT4" => 16,
        _ => return Err(format!("Unsupported type {expected_type}")),
    };

    let bv_idx = acc.get("bufferView").and_then(|b| b.as_u64()).unwrap_or(0) as usize;
    let acc_byte_offset = acc.get("byteOffset").and_then(|o| o.as_u64()).unwrap_or(0) as usize;

    let buffer_views = glb
        .json
        .get("bufferViews")
        .and_then(|b| b.as_array())
        .ok_or("Missing bufferViews array")?;
    let bv = buffer_views
        .get(bv_idx)
        .ok_or_else(|| format!("BufferView index {bv_idx} out of bounds"))?;
    let bv_byte_offset = bv.get("byteOffset").and_then(|o| o.as_u64()).unwrap_or(0) as usize;
    let stride = bv
        .get("byteStride")
        .and_then(|s| s.as_u64())
        .map(|s| s as usize)
        .unwrap_or(comp_count * 4);

    let total_offset = bv_byte_offset + acc_byte_offset;
    let total_elements = count * comp_count;
    let mut out = Vec::with_capacity(total_elements);

    for i in 0..count {
        let elem_start = total_offset + i * stride;
        for c in 0..comp_count {
            let offset = elem_start + c * 4;
            if offset + 4 > glb.bin_data.len() {
                return Err(format!(
                    "Buffer overrun at accessor {acc_idx} element {i}, component {c}"
                ));
            }
            let val = f32::from_le_bytes([
                glb.bin_data[offset],
                glb.bin_data[offset + 1],
                glb.bin_data[offset + 2],
                glb.bin_data[offset + 3],
            ]);
            out.push(val);
        }
    }

    Ok((count, out))
}

fn read_accessor_vec4_u16(glb: &ParsedGlb<'_>, acc_idx: usize) -> Result<Vec<[u16; 4]>, String> {
    let accessors = glb
        .json
        .get("accessors")
        .and_then(|a| a.as_array())
        .ok_or("Missing accessors array")?;
    let acc = accessors
        .get(acc_idx)
        .ok_or_else(|| format!("Accessor index {acc_idx} out of bounds"))?;
    let count = acc.get("count").and_then(|c| c.as_u64()).unwrap_or(0) as usize;
    let comp_type = acc
        .get("componentType")
        .and_then(|c| c.as_u64())
        .unwrap_or(0) as u32;

    let bv_idx = acc.get("bufferView").and_then(|b| b.as_u64()).unwrap_or(0) as usize;
    let acc_byte_offset = acc.get("byteOffset").and_then(|o| o.as_u64()).unwrap_or(0) as usize;

    let buffer_views = glb
        .json
        .get("bufferViews")
        .and_then(|b| b.as_array())
        .ok_or("Missing bufferViews array")?;
    let bv = buffer_views
        .get(bv_idx)
        .ok_or_else(|| format!("BufferView index {bv_idx} out of bounds"))?;
    let bv_byte_offset = bv.get("byteOffset").and_then(|o| o.as_u64()).unwrap_or(0) as usize;

    let total_offset = bv_byte_offset + acc_byte_offset;
    let mut out = Vec::with_capacity(count);

    if comp_type == COMPONENT_TYPE_UNSIGNED_SHORT {
        for i in 0..count {
            let offset = total_offset + i * 8;
            if offset + 8 > glb.bin_data.len() {
                return Err(format!("Buffer overrun in JOINTS_0 u16 at vertex {i}"));
            }
            let j0 = u16::from_le_bytes([glb.bin_data[offset], glb.bin_data[offset + 1]]);
            let j1 = u16::from_le_bytes([glb.bin_data[offset + 2], glb.bin_data[offset + 3]]);
            let j2 = u16::from_le_bytes([glb.bin_data[offset + 4], glb.bin_data[offset + 5]]);
            let j3 = u16::from_le_bytes([glb.bin_data[offset + 6], glb.bin_data[offset + 7]]);
            out.push([j0, j1, j2, j3]);
        }
    } else if comp_type == COMPONENT_TYPE_UNSIGNED_BYTE {
        for i in 0..count {
            let offset = total_offset + i * 4;
            if offset + 4 > glb.bin_data.len() {
                return Err(format!("Buffer overrun in JOINTS_0 u8 at vertex {i}"));
            }
            out.push([
                glb.bin_data[offset] as u16,
                glb.bin_data[offset + 1] as u16,
                glb.bin_data[offset + 2] as u16,
                glb.bin_data[offset + 3] as u16,
            ]);
        }
    } else {
        return Err(format!("Unsupported JOINTS_0 componentType {comp_type}"));
    }

    Ok(out)
}

fn read_accessor_indices(glb: &ParsedGlb<'_>, acc_idx: usize) -> Result<Vec<u32>, String> {
    let accessors = glb
        .json
        .get("accessors")
        .and_then(|a| a.as_array())
        .ok_or("Missing accessors array")?;
    let acc = accessors
        .get(acc_idx)
        .ok_or_else(|| format!("Accessor index {acc_idx} out of bounds"))?;
    let count = acc.get("count").and_then(|c| c.as_u64()).unwrap_or(0) as usize;
    let comp_type = acc
        .get("componentType")
        .and_then(|c| c.as_u64())
        .unwrap_or(5123) as u32;

    let bv_idx = acc.get("bufferView").and_then(|b| b.as_u64()).unwrap_or(0) as usize;
    let acc_byte_offset = acc.get("byteOffset").and_then(|o| o.as_u64()).unwrap_or(0) as usize;

    let buffer_views = glb
        .json
        .get("bufferViews")
        .and_then(|b| b.as_array())
        .ok_or("Missing bufferViews array")?;
    let bv = buffer_views
        .get(bv_idx)
        .ok_or_else(|| format!("BufferView index {bv_idx} out of bounds"))?;
    let bv_byte_offset = bv.get("byteOffset").and_then(|o| o.as_u64()).unwrap_or(0) as usize;

    let total_offset = bv_byte_offset + acc_byte_offset;
    let mut out = Vec::with_capacity(count);

    if comp_type == COMPONENT_TYPE_UNSIGNED_SHORT {
        for i in 0..count {
            let offset = total_offset + i * 2;
            if offset + 2 > glb.bin_data.len() {
                return Err(format!("Buffer overrun in u16 indices at index {i}"));
            }
            out.push(u16::from_le_bytes([glb.bin_data[offset], glb.bin_data[offset + 1]]) as u32);
        }
    } else if comp_type == COMPONENT_TYPE_UNSIGNED_INT {
        for i in 0..count {
            let offset = total_offset + i * 4;
            if offset + 4 > glb.bin_data.len() {
                return Err(format!("Buffer overrun in u32 indices at index {i}"));
            }
            out.push(u32::from_le_bytes([
                glb.bin_data[offset],
                glb.bin_data[offset + 1],
                glb.bin_data[offset + 2],
                glb.bin_data[offset + 3],
            ]));
        }
    } else {
        return Err(format!("Unsupported index componentType {comp_type}"));
    }

    Ok(out)
}

// =========================================================================
// MODULE 2: TEST CLUSTER HARNESS (Master, Workers, Agent Mesh Hub & Bridge)
// =========================================================================

struct PipelineCluster {
    pub master: MasterHandle,
    pub master_addr: SocketAddr,
    pub hub: AgentMeshHub,
    pub hub_url: String,
    pub workers: Vec<SpawnedWorker>,
}

struct SpawnedWorker {
    pub _worker_id: Uuid,
    pub shutdown_tx: watch::Sender<bool>,
    pub handle: tokio::task::JoinHandle<()>,
    pub _temp_dir: TempDir,
}

impl PipelineCluster {
    pub async fn spawn(num_workers: usize) -> Self {
        // 1. Master Coordinator on ephemeral loopback
        let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
            .await
            .expect("MasterServer::spawn failed");
        let master_addr = master.server_addr();

        // 2. Agent Mesh Gateway on ephemeral loopback with attached AgentGridBridge
        let hub = AgentMeshHub::new();
        let bridge = Arc::new(AgentGridBridge::new(master.clone()));
        hub.attach_bridge(bridge).await;
        let hub_port = hub
            .start("127.0.0.1:0")
            .await
            .expect("AgentMeshHub start failed");
        let hub_url = format!("ws://127.0.0.1:{hub_port}/ws");

        // 3. Spawn Workers with simulated GPU and isolated sandboxes
        let mut workers = Vec::with_capacity(num_workers);
        for i in 0..num_workers {
            let temp_dir = TempDir::new().expect("worker tempdir");
            let worker_cfg = WorkerConfig::new(master_addr.to_string())
                .with_name(&format!("e2e-worker-{}", i + 1))
                .with_cores(4)
                .with_ram_mb(8192)
                .with_simulate_gpu(true)
                .with_redirection_portal(false)
                .with_sandbox_base_dir(temp_dir.path());

            let mut worker = WorkerClient::new(worker_cfg);
            let worker_id = worker.worker_id();
            let (shutdown_tx, rx) = watch::channel(false);

            let handle = tokio::spawn(async move {
                let _ = worker.run(rx).await;
            });

            workers.push(SpawnedWorker {
                _worker_id: worker_id,
                shutdown_tx,
                handle,
                _temp_dir: temp_dir,
            });
        }

        // 4. Await connected workers in Master registry
        let start = Instant::now();
        let timeout = Duration::from_secs(5);
        let mut ready = false;
        while start.elapsed() < timeout {
            if let Ok(list) = master.list_workers().await {
                let connected_count = list
                    .iter()
                    .filter(|w| w.status == WorkerStatus::Connected)
                    .count();
                if connected_count >= num_workers {
                    ready = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            ready,
            "Timed out waiting for {num_workers} workers to connect to Master coordinator"
        );

        Self {
            master,
            master_addr,
            hub,
            hub_url,
            workers,
        }
    }

    pub async fn shutdown(mut self) {
        for w in self.workers.drain(..) {
            let _ = w.shutdown_tx.send(true);
            let _ = w.handle.await;
        }
        self.hub.stop().await;
        let _ = self.master.shutdown();
    }
}

/// Helper to create a local in-memory TaskRunner for testing execution.
fn make_test_runner(gpu: bool) -> TaskRunner {
    let caps = WorkerCapabilities::new("runner-worker-1", 4, 8192, gpu, false, None);
    TaskRunner::new(Uuid::new_v4(), caps, RunnerConfig::default())
}

// =========================================================================
// MODULE 3: THE 8 FULL BATTERY TEST CASES (FEATURE F22)
// =========================================================================

/// 1. `test_e2e_generative_3d_master_to_worker`:
/// Submits TaskSpec::Generative3D, checks progressive checkpoints (25%, 50%, 75%, 100%),
/// extracts zero-copy Bytes asset, runs glTF validator.
#[tokio::test]
async fn test_e2e_generative_3d_master_to_worker() {
    let cluster = PipelineCluster::spawn(1).await;

    let spec = Generative3DSpec::new_prompt("cybernetic guardian".to_string())
        .with_quality("fast")
        .with_engine("sdf")
        .with_export_texture(true);

    let task = Task::new(
        TaskSpec::Generative3D(spec),
        TaskRequirements::generic(1, 30),
    );
    let task_id = cluster
        .master
        .submit_task(task)
        .await
        .expect("submit Generative3D");

    let result = cluster
        .master
        .wait_task(task_id, Some(Duration::from_secs(15)))
        .await
        .expect("Generative3D execution failed or timed out");

    assert!(
        result.is_success(),
        "TaskResult must be success: {:?}",
        result.error
    );
    assert_eq!(result.exit_code, EXIT_CODE_SUCCESS);

    // Verify checkpoint recorded by Master
    let cp = cluster
        .master
        .get_checkpoint(&task_id)
        .await
        .expect("Master must retain checkpoint for Generative3D");
    assert_eq!(cp.sequence, 4, "Must reach sequence 4 checkpoint");
    let cp_val: Value =
        serde_json::from_slice(cp.delta_state.as_slice()).expect("Checkpoint JSON valid");
    assert_eq!(cp_val["stage"], "gltf_pack");
    assert_eq!(cp_val["progress"], 1.0);

    // Extract zero-copy asset_bytes and validate unrigged glTF
    let asset = result
        .asset_bytes
        .expect("Generative3D must produce asset_bytes");
    assert!(
        asset.len() > 1000,
        "Asset size must be non-trivial (>1000 bytes)"
    );

    let report =
        validate_unrigged_mesh(asset.as_slice()).expect("glTF 2.0 unrigged mesh validation failed");
    assert!(report.vertex_count >= 3);
    assert!(report.triangle_count >= 1);
    assert!(report.has_normals, "Must have normals");
    assert!(report.has_uvs, "Must have UVs");

    cluster.shutdown().await;
}

/// 2. `test_e2e_auto_rig_master_to_worker`:
/// Chains mesh into TaskSpec::AutoRig, checks progressive checkpoints,
/// extracts rigged .glb, runs deep glTF 2.0 rigging and animation validator.
#[tokio::test]
async fn test_e2e_auto_rig_master_to_worker() {
    let cluster = PipelineCluster::spawn(1).await;

    // Step 1: Generate source mesh via Tier-0 SDF
    let gen_spec = Generative3DSpec::new_prompt("bipedal character base".to_string())
        .with_quality("fast")
        .with_engine("sdf");
    let gen_task = Task::new(
        TaskSpec::Generative3D(gen_spec),
        TaskRequirements::generic(1, 30),
    );
    let gen_id = cluster
        .master
        .submit_task(gen_task)
        .await
        .expect("submit gen");
    let gen_result = cluster
        .master
        .wait_task(gen_id, Some(Duration::from_secs(15)))
        .await
        .expect("gen wait");
    assert!(gen_result.is_success());
    let source_mesh = gen_result.asset_bytes.expect("source mesh bytes");

    // Step 2: Chain into AutoRig
    let rig_spec = AutoRigSpec::new(source_mesh.as_slice().to_vec(), "biped")
        .with_animations(vec![
            "idle".to_string(),
            "walk".to_string(),
            "turnaround".to_string(),
        ])
        .with_provider("builtin");

    let rig_task = Task::new(
        TaskSpec::AutoRig(rig_spec),
        TaskRequirements::generic(1, 30),
    );
    let rig_id = cluster
        .master
        .submit_task(rig_task)
        .await
        .expect("submit AutoRig");
    let rig_result = cluster
        .master
        .wait_task(rig_id, Some(Duration::from_secs(15)))
        .await
        .expect("AutoRig execution failed or timed out");

    assert!(
        rig_result.is_success(),
        "AutoRig must succeed: {:?}",
        rig_result.error
    );
    assert_eq!(rig_result.exit_code, EXIT_CODE_SUCCESS);

    // Verify checkpoint recorded by Master
    let cp = cluster
        .master
        .get_checkpoint(&rig_id)
        .await
        .expect("Master must retain checkpoint for AutoRig");
    assert_eq!(cp.sequence, 4, "Must reach sequence 4 checkpoint");
    let cp_val: Value =
        serde_json::from_slice(cp.delta_state.as_slice()).expect("Checkpoint JSON valid");
    assert_eq!(cp_val["stage"], "animation_export");
    assert_eq!(cp_val["progress"], 1.0);

    // Deep glTF 2.0 Rigging and Animation validation
    let rigged_asset = rig_result
        .asset_bytes
        .expect("AutoRig must produce asset_bytes");
    let summary =
        validate_complete_rigged_glb(rigged_asset.as_slice(), &["idle", "walk", "turnaround"])
            .expect("glTF 2.0 rigged mesh validation failed");

    assert_eq!(summary.skin.joint_count, 15);
    assert_eq!(summary.skin.inverse_bind_matrices_count, 15);
    assert!(summary.skin.max_weight_sum_error <= 1e-4);
    assert_eq!(
        summary.animations.clip_names,
        vec!["idle", "walk", "turnaround"]
    );
    assert_eq!(summary.animations.durations.len(), 3);

    cluster.shutdown().await;
}

/// 3. `test_e2e_agent_mesh_bridge_3d_and_autorig`:
/// Executes grid_3d_generate and grid_auto_rig via AgentMeshClient / AgentGridBridge,
/// verifies intermediate ACK, Base64 decoding, and SHA-256 integrity.
#[tokio::test]
async fn test_e2e_agent_mesh_bridge_3d_and_autorig() {
    let cluster = PipelineCluster::spawn(1).await;

    let client = AgentMeshClient::new(&cluster.hub_url, "test-coding-agent", "windows");
    client
        .connect()
        .await
        .expect("AgentMeshClient connection failed");

    // 1. Execute grid_3d_generate
    let resp_3d = client
        .generate_3d_from_prompt(
            "crystalline sentinel",
            Some("fast"),
            Some("sdf"),
            Some(30000),
        )
        .await
        .expect("generate_3d_from_prompt failed");

    let mesh_bytes = AgentMeshClient::extract_asset_bytes(&resp_3d)
        .expect("extract_asset_bytes with SHA-256 validation failed for 3D generation");
    assert!(mesh_bytes.len() > 1000);
    validate_unrigged_mesh(&mesh_bytes).expect("unrigged mesh validation failed");

    // 2. Execute grid_auto_rig
    let resp_rig = client
        .auto_rig(
            &mesh_bytes,
            Some("biped"),
            Some(vec![
                "idle".to_string(),
                "walk".to_string(),
                "turnaround".to_string(),
            ]),
            Some("builtin"),
            Some(30000),
        )
        .await
        .expect("auto_rig failed");

    let rigged_bytes = AgentMeshClient::extract_asset_bytes(&resp_rig)
        .expect("extract_asset_bytes with SHA-256 validation failed for AutoRig");
    assert!(rigged_bytes.len() > 1000);

    let summary = validate_complete_rigged_glb(&rigged_bytes, &["idle", "walk", "turnaround"])
        .expect("rigged glb validation failed");
    assert_eq!(summary.skin.joint_count, 15);
    assert_eq!(
        summary.animations.clip_names,
        vec!["idle", "walk", "turnaround"]
    );

    cluster.shutdown().await;
}

/// 4. `test_e2e_cli_submit_3d_and_disk_export`:
/// Exercises run_submit with --prompt, --quality fast, --engine sdf, --wait, --output <temp_dir>/output.glb,
/// verifies file written to disk, validate_glb_header pass, and full glTF validation pass.
#[tokio::test]
async fn test_e2e_cli_submit_3d_and_disk_export() {
    let cluster = PipelineCluster::spawn(1).await;
    let temp_dir = TempDir::new().expect("tempdir");

    let out_mesh_path = temp_dir.path().join("cli_exported_mesh.glb");
    let out_rigged_path = temp_dir.path().join("cli_exported_rigged.glb");

    // 1. Submit 3D generation via CLI
    let args_3d = SubmitArgs {
        master: Some(cluster.master_addr.to_string()),
        task_type: "3d".to_string(),
        command: None,
        cores: 1,
        ram_mb: 0,
        gpu: false,
        timeout_secs: 30,
        max_retries: None,
        wait: true,
        json: true,
        prompt: Some("humanoid automaton".to_string()),
        image: None,
        quality: Some("fast".to_string()),
        engine: Some("sdf".to_string()),
        export_texture: true,
        no_export_texture: false,
        input_mesh: None,
        rig_preset: None,
        embed_animations: Vec::new(),
        preferred_provider: None,
        output: Some(out_mesh_path.clone()),
        args: Vec::new(),
        ..Default::default()
    };

    let exit_code = run_submit(args_3d, None).await;
    assert_eq!(exit_code, ExitCode::SUCCESS);
    assert!(
        out_mesh_path.is_file(),
        "Mesh output file must exist on disk"
    );

    let mesh_data = std::fs::read(&out_mesh_path).expect("read exported mesh");
    assert!(mesh_data.len() > 1000);
    assert!(validate_glb_header(&mesh_data).is_ok());
    validate_unrigged_mesh(&mesh_data).expect("full glTF 2.0 unrigged mesh validation failed");

    // 2. Submit AutoRig via CLI chaining the mesh file from disk
    let args_rig = SubmitArgs {
        master: Some(cluster.master_addr.to_string()),
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
        input_mesh: Some(out_mesh_path),
        rig_preset: Some("biped".to_string()),
        embed_animations: vec![
            "idle".to_string(),
            "walk".to_string(),
            "turnaround".to_string(),
        ],
        preferred_provider: Some("builtin".to_string()),
        output: Some(out_rigged_path.clone()),
        args: Vec::new(),
        ..Default::default()
    };

    let exit_code_rig = run_submit(args_rig, None).await;
    assert_eq!(exit_code_rig, ExitCode::SUCCESS);
    assert!(
        out_rigged_path.is_file(),
        "Rigged output file must exist on disk"
    );

    let rigged_data = std::fs::read(&out_rigged_path).expect("read exported rigged mesh");
    assert!(rigged_data.len() > 1000);
    assert!(validate_glb_header(&rigged_data).is_ok());

    let summary = validate_complete_rigged_glb(&rigged_data, &["idle", "walk", "turnaround"])
        .expect("full glTF 2.0 rigged mesh validation failed");
    assert_eq!(summary.skin.joint_count, 15);
    assert_eq!(
        summary.animations.clip_names,
        vec!["idle", "walk", "turnaround"]
    );

    cluster.shutdown().await;
}

/// 5. `test_e2e_checkpoint_monotonic_progress`:
/// Verifies checkpoints strictly progress [1, 2, 3, 4] and [25%, 50%, 75%, 100%].
#[tokio::test]
async fn test_e2e_checkpoint_monotonic_progress() {
    let runner = make_test_runner(false);

    // 1. Generative 3D Checkpoint Monotonic Progression
    let sdf_spec = Generative3DSpec::new_prompt("monotonic_guardian".to_string())
        .with_quality("fast")
        .with_engine("sdf");
    let sdf_task = Task::new(
        TaskSpec::Generative3D(sdf_spec),
        TaskRequirements::generic(1, 30),
    );

    let (tx_sdf, mut rx_sdf) = mpsc::channel(16);
    let sdf_task_clone = sdf_task.clone();
    let runner_sdf = make_test_runner(false);

    let handle_sdf = tokio::spawn(async move {
        runner_sdf
            .execute_task_with_checkpoint(&sdf_task_clone, None, Some(tx_sdf), None, None)
            .await
    });

    let mut sdf_cps = Vec::new();
    while let Some(msg) = rx_sdf.recv().await {
        if let WorkerMessage::Checkpoint {
            sequence,
            delta_state,
            ..
        } = msg
        {
            sdf_cps.push((sequence, delta_state));
        }
    }

    let sdf_res = handle_sdf.await.expect("join sdf");
    assert!(sdf_res.is_success());

    assert_eq!(sdf_cps.len(), 4, "SDF must emit exactly 4 checkpoints");
    let expected_sdf_stages = ["sdf_geometry", "manifold_uv", "texture_baking", "gltf_pack"];
    let expected_sdf_progress = [0.25, 0.50, 0.75, 1.00];

    for (i, (seq, delta)) in sdf_cps.iter().enumerate() {
        assert_eq!(*seq, (i + 1) as u64, "Sequence must be strictly monotonic");
        let val: Value = serde_json::from_slice(delta.as_slice()).expect("JSON delta");
        assert_eq!(val["stage"], expected_sdf_stages[i]);
        assert_eq!(val["progress"], expected_sdf_progress[i]);
    }

    // 2. AutoRig Checkpoint Monotonic Progression
    let mesh_bytes = sdf_res.asset_bytes.expect("sdf produced mesh");
    let rig_spec = AutoRigSpec::new(mesh_bytes.as_slice().to_vec(), "biped")
        .with_animations(vec!["idle".to_string()]);
    let rig_task = Task::new(
        TaskSpec::AutoRig(rig_spec),
        TaskRequirements::generic(1, 30),
    );

    let (tx_rig, mut rx_rig) = mpsc::channel(16);
    let rig_task_clone = rig_task.clone();

    let handle_rig = tokio::spawn(async move {
        runner
            .execute_task_with_checkpoint(&rig_task_clone, None, Some(tx_rig), None, None)
            .await
    });

    let mut rig_cps = Vec::new();
    while let Some(msg) = rx_rig.recv().await {
        if let WorkerMessage::Checkpoint {
            sequence,
            delta_state,
            ..
        } = msg
        {
            rig_cps.push((sequence, delta_state));
        }
    }

    let rig_res = handle_rig.await.expect("join rig");
    assert!(rig_res.is_success());

    assert_eq!(rig_cps.len(), 4, "AutoRig must emit exactly 4 checkpoints");
    let expected_rig_stages = [
        "landmark_extraction",
        "armature_ibm",
        "geodesic_skinning",
        "animation_export",
    ];
    let expected_rig_progress = [0.25, 0.50, 0.75, 1.00];

    for (i, (seq, delta)) in rig_cps.iter().enumerate() {
        assert_eq!(*seq, (i + 1) as u64, "Sequence must be strictly monotonic");
        let val: Value = serde_json::from_slice(delta.as_slice()).expect("JSON delta");
        assert_eq!(val["stage"], expected_rig_stages[i]);
        assert_eq!(val["progress"], expected_rig_progress[i]);
    }
}

/// 6. `test_boundary_and_corrupted_mesh_rejection`:
/// Corrupted input bytes properly rejected with error, no panics.
#[tokio::test]
async fn test_boundary_and_corrupted_mesh_rejection() {
    let runner = make_test_runner(false);

    // Case 1: Empty mesh bytes
    let empty_spec = AutoRigSpec::new(vec![], "biped");
    let empty_task = Task::new(
        TaskSpec::AutoRig(empty_spec),
        TaskRequirements::generic(1, 30),
    );
    let res1 = runner.execute_task(&empty_task, None, None).await;
    assert!(!res1.is_success());
    assert_eq!(res1.exit_code, EXIT_CODE_GENERAL_ERROR);
    assert!(res1.asset_bytes.is_none());

    // Case 2: Random noise bytes
    let noise_spec = AutoRigSpec::new(
        vec![0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04],
        "biped",
    );
    let noise_task = Task::new(
        TaskSpec::AutoRig(noise_spec),
        TaskRequirements::generic(1, 30),
    );
    let res2 = runner.execute_task(&noise_task, None, None).await;
    assert!(!res2.is_success());
    assert_eq!(res2.exit_code, EXIT_CODE_GENERAL_ERROR);
    assert!(res2.asset_bytes.is_none());

    // Case 3: Truncated GLB header (claims 100 bytes, only provides 16)
    let mut trunc_glb = Vec::new();
    trunc_glb.extend_from_slice(b"glTF");
    trunc_glb.extend_from_slice(&2u32.to_le_bytes());
    trunc_glb.extend_from_slice(&100u32.to_le_bytes());
    trunc_glb.extend_from_slice(&20u32.to_le_bytes());

    let trunc_spec = AutoRigSpec::new(trunc_glb, "biped");
    let trunc_task = Task::new(
        TaskSpec::AutoRig(trunc_spec),
        TaskRequirements::generic(1, 30),
    );
    let res3 = runner.execute_task(&trunc_task, None, None).await;
    assert!(!res3.is_success());
    assert_eq!(res3.exit_code, EXIT_CODE_GENERAL_ERROR);

    // Case 4: Invalid JSON chunk inside valid container framing
    let bad_json = b"{ invalid json content !@#$%^ }";
    let mut bad_json_bytes = bad_json.to_vec();
    while bad_json_bytes.len() % 4 != 0 {
        bad_json_bytes.push(b' ');
    }
    let total_len = 12 + 8 + bad_json_bytes.len();
    let mut bad_glb = Vec::new();
    bad_glb.extend_from_slice(b"glTF");
    bad_glb.extend_from_slice(&2u32.to_le_bytes());
    bad_glb.extend_from_slice(&(total_len as u32).to_le_bytes());
    bad_glb.extend_from_slice(&(bad_json_bytes.len() as u32).to_le_bytes());
    bad_glb.extend_from_slice(&CHUNK_TYPE_JSON.to_le_bytes());
    bad_glb.extend_from_slice(&bad_json_bytes);

    let bad_spec = AutoRigSpec::new(bad_glb, "biped");
    let bad_task = Task::new(
        TaskSpec::AutoRig(bad_spec),
        TaskRequirements::generic(1, 30),
    );
    let res4 = runner.execute_task(&bad_task, None, None).await;
    assert!(!res4.is_success());
    assert_eq!(res4.exit_code, EXIT_CODE_GENERAL_ERROR);

    // Case 5: Zero-byte file to CLI
    let temp_dir = TempDir::new().unwrap();
    let empty_file = temp_dir.path().join("zero.glb");
    std::fs::write(&empty_file, b"").unwrap();

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
        input_mesh: Some(empty_file),
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

/// 7. `test_adversarial_single_bit_tamper_detection`:
/// 1-bit flip in binary payload triggers SHA-256 verification failure.
#[tokio::test]
async fn test_adversarial_single_bit_tamper_detection() {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;

    let runner = make_test_runner(false);
    let sdf_spec = Generative3DSpec::new_prompt("tamper_test".to_string())
        .with_quality("fast")
        .with_engine("sdf");
    let sdf_task = Task::new(
        TaskSpec::Generative3D(sdf_spec),
        TaskRequirements::generic(1, 30),
    );
    let res = runner.execute_task(&sdf_task, None, None).await;
    assert!(res.is_success());
    let original_bytes = res.asset_bytes.expect("original bytes").as_slice().to_vec();
    let original_sha = compute_sha256(&original_bytes);

    // Verify baseline passes
    assert!(verify_sha256(&original_bytes, &original_sha));

    // Attack 1: Single bit flip at byte 0
    let mut tampered_0 = original_bytes.clone();
    tampered_0[0] ^= 0x01;
    assert!(!verify_sha256(&tampered_0, &original_sha));

    let env_tampered_0: AgentMeshEnvelope = serde_json::from_value(serde_json::json!({
        "type": "command_response",
        "from": "grid",
        "to": "client",
        "correlation_id": "corr-1",
        "status": "success",
        "exit_code": 0,
        "payload": {
            "asset_sha256": original_sha,
            "asset_base64": STANDARD.encode(&tampered_0),
        }
    }))
    .expect("deserialize envelope");
    let err_0 = AgentMeshClient::extract_asset_bytes(&env_tampered_0);
    assert!(err_0.is_err());
    assert!(err_0
        .unwrap_err()
        .to_string()
        .contains("SHA-256 verification failed"));

    // Attack 2: Single bit flip at middle byte
    let mid_idx = original_bytes.len() / 2;
    let mut tampered_mid = original_bytes.clone();
    tampered_mid[mid_idx] ^= 0x10;
    assert!(!verify_sha256(&tampered_mid, &original_sha));

    // Attack 3: Single bit flip at last byte
    let last_idx = original_bytes.len() - 1;
    let mut tampered_last = original_bytes.clone();
    tampered_last[last_idx] ^= 0x80;
    assert!(!verify_sha256(&tampered_last, &original_sha));

    // Attack 4: Single bit corruption in glTF header container validation
    let mut bad_magic = original_bytes.clone();
    bad_magic[0] ^= 0x01;
    let magic_err = parse_and_validate_glb_container(&bad_magic);
    assert!(magic_err.is_err());
    assert!(magic_err.unwrap_err().contains("Invalid glTF magic"));

    let mut bad_version = original_bytes.clone();
    bad_version[4] = 3;
    let ver_err = parse_and_validate_glb_container(&bad_version);
    assert!(ver_err.is_err());
    assert!(ver_err.unwrap_err().contains("Unsupported glTF version"));
}

/// 8. `test_adversarial_worker_crash_rescheduling`:
/// Simulated worker disconnect triggers task rescheduling by Master.
#[tokio::test]
async fn test_adversarial_worker_crash_rescheduling() {
    let master =
        MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(3))
            .await
            .expect("MasterServer::spawn failed");
    let master_addr = master.server_addr().to_string();

    let temp1 = TempDir::new().unwrap();
    let temp2 = TempDir::new().unwrap();

    let (_tx1, rx1) = watch::channel(false);
    let (_tx2, rx2) = watch::channel(false);

    let mut w1 = WorkerClient::new(
        WorkerConfig::new(master_addr.clone())
            .with_name("crash-w1")
            .with_cores(4)
            .with_ram_mb(8192)
            .with_simulate_gpu(true)
            .with_redirection_portal(false)
            .with_sandbox_base_dir(temp1.path()),
    );
    let w1_id = w1.worker_id();

    let mut w2 = WorkerClient::new(
        WorkerConfig::new(master_addr.clone())
            .with_name("crash-w2")
            .with_cores(4)
            .with_ram_mb(8192)
            .with_simulate_gpu(true)
            .with_redirection_portal(false)
            .with_sandbox_base_dir(temp2.path()),
    );
    let w2_id = w2.worker_id();

    let handle1 = tokio::spawn(async move {
        let _ = w1.run(rx1).await;
    });
    let _handle2 = tokio::spawn(async move {
        let _ = w2.run(rx2).await;
    });

    // Wait for both workers to connect
    let mut both_connected = false;
    for _ in 0..50 {
        if let Ok(list) = master.list_workers().await {
            if list
                .iter()
                .filter(|w| w.status == WorkerStatus::Connected)
                .count()
                == 2
            {
                both_connected = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(both_connected, "Both workers must connect");

    // Submit long enough task so we can catch it running
    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "crash_rescheduling".into(),
            iterations: 100,
            duration_ms: 1500,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 30),
    );
    let task_id = master.submit_task(task).await.expect("submit task");

    // Wait until running
    let mut is_running = false;
    for _ in 0..100 {
        if let Ok(info) = master.get_task_info(task_id).await {
            if info.state == TaskState::Running {
                is_running = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(is_running, "Task must enter Running state");

    let initial_info = master.get_task_info(task_id).await.unwrap();
    let assigned_worker = initial_info.assigned_worker_id.expect("assigned worker");

    // Abruptly kill the assigned worker (simulating sudden crash / SIGKILL)
    if assigned_worker == w1_id {
        handle1.abort();
    } else {
        _handle2.abort();
    }

    // Await completion on survivor worker
    let mut completed = false;
    for _ in 0..100 {
        if let Ok(info) = master.get_task_info(task_id).await {
            if info.state == TaskState::Completed {
                completed = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        completed,
        "Task must complete on surviving worker after crash"
    );

    let final_info = master.get_task_info(task_id).await.unwrap();
    assert_eq!(final_info.state, TaskState::Completed);
    assert_eq!(
        final_info.retry_count, 1,
        "Task must have been retried once"
    );
    assert_eq!(final_info.exit_code, Some(0));

    let survivor = if assigned_worker == w1_id {
        w2_id
    } else {
        w1_id
    };
    assert_eq!(final_info.assigned_worker_id, Some(survivor));

    // Confirm that the surviving worker can execute 3D generative tasks cleanly
    let sdf_spec = Generative3DSpec::new_prompt("post_recovery_mesh".to_string())
        .with_quality("fast")
        .with_engine("sdf");
    let sdf_task = Task::new(
        TaskSpec::Generative3D(sdf_spec),
        TaskRequirements::generic(1, 30),
    );
    let sdf_id = master
        .submit_task(sdf_task)
        .await
        .expect("submit post-recovery 3d");
    let sdf_res = master
        .wait_task(sdf_id, Some(Duration::from_secs(15)))
        .await
        .expect("post recovery wait");
    assert!(sdf_res.is_success());
    assert!(sdf_res.asset_bytes.is_some());

    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------
// Adversarial Challenger Helpers & Suite (Challenger M5 3D #2)
// ---------------------------------------------------------------------------

/// Reconstructs a valid glTF 2.0 binary container from JSON Value and binary slice.
fn rebuild_glb(json_val: &Value, bin_data: &[u8]) -> Vec<u8> {
    let mut json_bytes = serde_json::to_vec(json_val).expect("serialize json");
    let pad = (4 - (json_bytes.len() % 4)) % 4;
    json_bytes.extend(vec![b' '; pad]);

    let chunk0_len = json_bytes.len() as u32;

    let mut padded_bin = bin_data.to_vec();
    let bin_pad = (4 - (padded_bin.len() % 4)) % 4;
    padded_bin.extend(vec![0u8; bin_pad]);
    let chunk1_len = padded_bin.len() as u32;

    let total_len = 12 + 8 + chunk0_len as usize + 8 + chunk1_len as usize;

    let mut out = Vec::with_capacity(total_len);
    out.extend_from_slice(&GLB_MAGIC.to_le_bytes());
    out.extend_from_slice(&GLB_VERSION.to_le_bytes());
    out.extend_from_slice(&(total_len as u32).to_le_bytes());

    out.extend_from_slice(&chunk0_len.to_le_bytes());
    out.extend_from_slice(&CHUNK_TYPE_JSON.to_le_bytes());
    out.extend_from_slice(&json_bytes);

    out.extend_from_slice(&chunk1_len.to_le_bytes());
    out.extend_from_slice(&CHUNK_TYPE_BIN.to_le_bytes());
    out.extend_from_slice(&padded_bin);

    out
}

/// Computes the exact byte offset of an accessor in bin_data.
fn get_accessor_byte_offset(glb: &ParsedGlb<'_>, acc_idx: usize) -> usize {
    let acc = &glb.json["accessors"][acc_idx];
    let bv_idx = acc["bufferView"].as_u64().expect("bufferView index") as usize;
    let acc_byte_offset = acc.get("byteOffset").and_then(|o| o.as_u64()).unwrap_or(0) as usize;
    let bv_byte_offset = glb.json["bufferViews"][bv_idx]["byteOffset"]
        .as_u64()
        .unwrap_or(0) as usize;
    bv_byte_offset + acc_byte_offset
}

/// 9. `test_adversarial_validator_structural_robustness`:
/// Adversarially stresses structural validator against invalid IBM matrices (singular matrices,
/// non-affine bottom row, invalid dimensions), unnormalized & out-of-range skin weights,
/// non-monotonic animation timestamps, non-unit quaternions, and corrupt geometries.
#[tokio::test]
async fn test_adversarial_validator_structural_robustness() {
    let runner = make_test_runner(false);

    // Step 1: Generate clean source mesh and complete rigged GLB
    let sdf_spec = Generative3DSpec::new_prompt("adversarial_rig_subject".to_string())
        .with_quality("fast")
        .with_engine("sdf");
    let gen_task = Task::new(
        TaskSpec::Generative3D(sdf_spec),
        TaskRequirements::generic(1, 30),
    );
    let gen_res = runner.execute_task(&gen_task, None, None).await;
    assert!(gen_res.is_success());
    let source_mesh = gen_res.asset_bytes.expect("source mesh");

    let rig_spec = AutoRigSpec::new(source_mesh.as_slice().to_vec(), "biped")
        .with_animations(vec![
            "idle".to_string(),
            "walk".to_string(),
            "turnaround".to_string(),
        ])
        .with_provider("builtin");
    let rig_task = Task::new(
        TaskSpec::AutoRig(rig_spec),
        TaskRequirements::generic(1, 30),
    );
    let rig_res = runner.execute_task(&rig_task, None, None).await;
    assert!(rig_res.is_success());
    let clean_glb_bytes = rig_res
        .asset_bytes
        .expect("clean rigged glb")
        .as_slice()
        .to_vec();

    // Baseline validation must pass 100%
    let base_summary =
        validate_complete_rigged_glb(&clean_glb_bytes, &["idle", "walk", "turnaround"])
            .expect("Baseline glTF must pass");
    assert_eq!(base_summary.skin.joint_count, 15);
    assert_eq!(base_summary.animations.clip_names.len(), 3);

    let parsed_clean = parse_and_validate_glb_container(&clean_glb_bytes).unwrap();
    let ibm_acc_idx = parsed_clean.json["skins"][0]["inverseBindMatrices"]
        .as_u64()
        .unwrap() as usize;
    let ibm_offset = get_accessor_byte_offset(&parsed_clean, ibm_acc_idx);

    // =========================================================================
    // CATEGORY 1: IBM MATRIX STRESS (Singular, Non-Affine, Non-Finite, Dimensions)
    // =========================================================================

    // 1A. Singular IBM Matrix (det(3x3) == 0.0)
    let mut bin_singular = parsed_clean.bin_data.to_vec();
    for elem_idx in [0, 1, 2, 4, 5, 6, 8, 9, 10] {
        let f_offset = ibm_offset + elem_idx * 4;
        bin_singular[f_offset..f_offset + 4].copy_from_slice(&0.0f32.to_le_bytes());
    }
    let glb_singular = rebuild_glb(&parsed_clean.json, &bin_singular);
    let parsed_singular = parse_and_validate_glb_container(&glb_singular).unwrap();
    let err_singular = validate_glb_rigging_and_skin(&parsed_singular, 15);
    assert!(
        err_singular.is_err(),
        "Singular IBM matrix must be rejected"
    );
    assert!(
        err_singular
            .unwrap_err()
            .contains("singular or non-invertible"),
        "Error message must specify singular matrix"
    );

    // 1B. Rank-Deficient IBM Matrix (row 0 == row 1 in 3x3 rotation)
    let mut bin_rank_def = parsed_clean.bin_data.to_vec();
    for (src, dst) in [(0, 1), (4, 5), (8, 9)] {
        let src_off = ibm_offset + src * 4;
        let dst_off = ibm_offset + dst * 4;
        bin_rank_def.copy_within(src_off..src_off + 4, dst_off);
    }
    let glb_rank_def = rebuild_glb(&parsed_clean.json, &bin_rank_def);
    let parsed_rank_def = parse_and_validate_glb_container(&glb_rank_def).unwrap();
    let err_rank_def = validate_glb_rigging_and_skin(&parsed_rank_def, 15);
    assert!(err_rank_def.is_err(), "Rank deficient IBM must be rejected");
    assert!(err_rank_def
        .unwrap_err()
        .contains("singular or non-invertible"));

    // 1C. Non-Affine Bottom Row in IBM (element 15 is 2.5 instead of 1.0)
    let mut bin_non_affine = parsed_clean.bin_data.to_vec();
    let elem15_off = ibm_offset + 15 * 4;
    bin_non_affine[elem15_off..elem15_off + 4].copy_from_slice(&2.5f32.to_le_bytes());
    let glb_non_affine = rebuild_glb(&parsed_clean.json, &bin_non_affine);
    let parsed_non_affine = parse_and_validate_glb_container(&glb_non_affine).unwrap();
    let err_non_affine = validate_glb_rigging_and_skin(&parsed_non_affine, 15);
    assert!(
        err_non_affine.is_err(),
        "Non-affine IBM bottom row must be rejected"
    );
    assert!(err_non_affine.unwrap_err().contains("not affine [0,0,0,1]"));

    // 1D. Non-Finite Float (NaN) in IBM
    let mut bin_nan = parsed_clean.bin_data.to_vec();
    bin_nan[ibm_offset..ibm_offset + 4].copy_from_slice(&f32::NAN.to_le_bytes());
    let glb_nan = rebuild_glb(&parsed_clean.json, &bin_nan);
    let parsed_nan = parse_and_validate_glb_container(&glb_nan).unwrap();
    let err_nan = validate_glb_rigging_and_skin(&parsed_nan, 15);
    assert!(err_nan.is_err(), "Non-finite IBM element must be rejected");
    assert!(err_nan.unwrap_err().contains("is non-finite"));

    // 1E. Invalid IBM Count (14 matrices instead of 15)
    let mut bad_json_ibm_count = parsed_clean.json.clone();
    bad_json_ibm_count["accessors"][ibm_acc_idx]["count"] = serde_json::json!(14);
    let glb_bad_ibm_count = rebuild_glb(&bad_json_ibm_count, parsed_clean.bin_data);
    let parsed_bad_ibm_count = parse_and_validate_glb_container(&glb_bad_ibm_count).unwrap();
    let err_bad_ibm_count = validate_glb_rigging_and_skin(&parsed_bad_ibm_count, 15);
    assert!(
        err_bad_ibm_count.is_err(),
        "IBM count != joint count must be rejected"
    );
    assert!(err_bad_ibm_count
        .unwrap_err()
        .contains("InverseBindMatrices count (14) != joint count (15)"));

    // 1F. Invalid Joint Hierarchy Count (14 joints instead of 15)
    let mut bad_json_joints = parsed_clean.json.clone();
    bad_json_joints["skins"][0]["joints"]
        .as_array_mut()
        .unwrap()
        .pop();
    let glb_bad_joints = rebuild_glb(&bad_json_joints, parsed_clean.bin_data);
    let parsed_bad_joints = parse_and_validate_glb_container(&glb_bad_joints).unwrap();
    let err_bad_joints = validate_glb_rigging_and_skin(&parsed_bad_joints, 15);
    assert!(
        err_bad_joints.is_err(),
        "Skin joint count != 15 must be rejected"
    );
    assert!(err_bad_joints
        .unwrap_err()
        .contains("Skin joints count (14) != expected (15)"));

    // =========================================================================
    // CATEGORY 2: SKIN WEIGHT STRESS (Unnormalized, Negative, Excessive, Bounds)
    // =========================================================================

    let weights_acc_idx = parsed_clean.json["meshes"][0]["primitives"][0]["attributes"]["WEIGHTS_0"]
        .as_u64()
        .unwrap() as usize;
    let weights_offset = get_accessor_byte_offset(&parsed_clean, weights_acc_idx);

    // 2A. Unnormalized Weights: Sum = 0.75 != 1.0 (error 0.25 > 1e-4)
    let mut bin_unnorm = parsed_clean.bin_data.to_vec();
    for (k, &w) in [0.25f32, 0.25, 0.25, 0.0].iter().enumerate() {
        let off = weights_offset + k * 4;
        bin_unnorm[off..off + 4].copy_from_slice(&w.to_le_bytes());
    }
    let glb_unnorm = rebuild_glb(&parsed_clean.json, &bin_unnorm);
    let parsed_unnorm = parse_and_validate_glb_container(&glb_unnorm).unwrap();
    let err_unnorm = validate_glb_rigging_and_skin(&parsed_unnorm, 15);
    assert!(
        err_unnorm.is_err(),
        "Unnormalized weights (sum 0.75) must be rejected"
    );
    assert!(err_unnorm.unwrap_err().contains("sum to 0.75"));

    // 2B. Unnormalized Weights: Sum = 1.05 != 1.0 (error 0.05 > 1e-4)
    let mut bin_unnorm2 = parsed_clean.bin_data.to_vec();
    for (k, &w) in [0.35f32, 0.35, 0.35, 0.0].iter().enumerate() {
        let off = weights_offset + k * 4;
        bin_unnorm2[off..off + 4].copy_from_slice(&w.to_le_bytes());
    }
    let glb_unnorm2 = rebuild_glb(&parsed_clean.json, &bin_unnorm2);
    let parsed_unnorm2 = parse_and_validate_glb_container(&glb_unnorm2).unwrap();
    let err_unnorm2 = validate_glb_rigging_and_skin(&parsed_unnorm2, 15);
    assert!(
        err_unnorm2.is_err(),
        "Unnormalized weights (sum 1.05) must be rejected"
    );

    // 2C. Out-of-Range Negative Skin Weight (sum == 1.0, but w < 0.0)
    let mut bin_neg_weight = parsed_clean.bin_data.to_vec();
    for (k, &w) in [-0.2f32, 0.6, 0.6, 0.0].iter().enumerate() {
        let off = weights_offset + k * 4;
        bin_neg_weight[off..off + 4].copy_from_slice(&w.to_le_bytes());
    }
    let glb_neg_weight = rebuild_glb(&parsed_clean.json, &bin_neg_weight);
    let parsed_neg_weight = parse_and_validate_glb_container(&glb_neg_weight).unwrap();
    let err_neg_weight = validate_glb_rigging_and_skin(&parsed_neg_weight, 15);
    assert!(
        err_neg_weight.is_err(),
        "Negative skin weight must be rejected"
    );
    assert!(err_neg_weight
        .unwrap_err()
        .contains("out of range [0.0, 1.0]"));

    // 2D. Out-of-Range Excessive Skin Weight (w > 1.0)
    let mut bin_high_weight = parsed_clean.bin_data.to_vec();
    for (k, &w) in [1.5f32, 0.0, 0.0, -0.5].iter().enumerate() {
        let off = weights_offset + k * 4;
        bin_high_weight[off..off + 4].copy_from_slice(&w.to_le_bytes());
    }
    let glb_high_weight = rebuild_glb(&parsed_clean.json, &bin_high_weight);
    let parsed_high_weight = parse_and_validate_glb_container(&glb_high_weight).unwrap();
    let err_high_weight = validate_glb_rigging_and_skin(&parsed_high_weight, 15);
    assert!(
        err_high_weight.is_err(),
        "Skin weight > 1.0 must be rejected"
    );
    assert!(err_high_weight
        .unwrap_err()
        .contains("out of range [0.0, 1.0]"));

    // 2E. Out-of-Bounds Joint Index in JOINTS_0 (vertex 0 joint 0 set to 15)
    let joints_acc_idx = parsed_clean.json["meshes"][0]["primitives"][0]["attributes"]["JOINTS_0"]
        .as_u64()
        .unwrap() as usize;
    let joints_offset = get_accessor_byte_offset(&parsed_clean, joints_acc_idx);
    let mut bin_bad_joint = parsed_clean.bin_data.to_vec();
    bin_bad_joint[joints_offset..joints_offset + 2].copy_from_slice(&15u16.to_le_bytes());
    let glb_bad_joint = rebuild_glb(&parsed_clean.json, &bin_bad_joint);
    let parsed_bad_joint = parse_and_validate_glb_container(&glb_bad_joint).unwrap();
    let err_bad_joint = validate_glb_rigging_and_skin(&parsed_bad_joint, 15);
    assert!(err_bad_joint.is_err(), "Joint index >= 15 must be rejected");
    assert!(err_bad_joint
        .unwrap_err()
        .contains("JOINTS_0[0] (15) >= expected joint count (15)"));

    // =========================================================================
    // CATEGORY 3: ANIMATION TIMESTAMPS, QUATERNIONS & TARGET ROUTING
    // =========================================================================

    let time_acc_idx = parsed_clean.json["animations"][0]["samplers"][0]["input"]
        .as_u64()
        .unwrap() as usize;
    let time_offset = get_accessor_byte_offset(&parsed_clean, time_acc_idx);

    // 3A. Non-Monotonic Duplicate Timestamps (t1 == t0)
    let mut bin_dup_time = parsed_clean.bin_data.to_vec();
    let t0 = bin_dup_time[time_offset..time_offset + 4].to_vec();
    bin_dup_time[time_offset + 4..time_offset + 8].copy_from_slice(&t0);
    let glb_dup_time = rebuild_glb(&parsed_clean.json, &bin_dup_time);
    let parsed_dup_time = parse_and_validate_glb_container(&glb_dup_time).unwrap();
    let err_dup_time = validate_glb_animations(&parsed_dup_time, &["idle", "walk", "turnaround"]);
    assert!(
        err_dup_time.is_err(),
        "Duplicate animation timestamps must be rejected"
    );
    assert!(err_dup_time
        .unwrap_err()
        .contains("non-monotonic timestamps"));

    // 3B. Non-Monotonic Decreasing Timestamps (t1 < t0)
    let mut bin_dec_time = parsed_clean.bin_data.to_vec();
    bin_dec_time[time_offset..time_offset + 4].copy_from_slice(&1.5f32.to_le_bytes());
    bin_dec_time[time_offset + 4..time_offset + 8].copy_from_slice(&0.5f32.to_le_bytes());
    let glb_dec_time = rebuild_glb(&parsed_clean.json, &bin_dec_time);
    let parsed_dec_time = parse_and_validate_glb_container(&glb_dec_time).unwrap();
    let err_dec_time = validate_glb_animations(&parsed_dec_time, &["idle", "walk", "turnaround"]);
    assert!(
        err_dec_time.is_err(),
        "Decreasing animation timestamps must be rejected"
    );
    assert!(err_dec_time
        .unwrap_err()
        .contains("non-monotonic timestamps"));

    // 3C. Non-Unit Rotation Quaternion (norm = 0.5 != 1.0)
    let samplers = parsed_clean.json["animations"][0]["samplers"]
        .as_array()
        .unwrap();
    let mut rot_out_acc_idx = None;
    for sampler in samplers {
        let out_idx = sampler["output"].as_u64().unwrap() as usize;
        let out_type = parsed_clean.json["accessors"][out_idx]["type"]
            .as_str()
            .unwrap();
        if out_type == "VEC4" {
            rot_out_acc_idx = Some(out_idx);
            break;
        }
    }
    let rot_out_acc_idx = rot_out_acc_idx.expect("Must find a rotation sampler in animations[0]");
    let rot_offset = get_accessor_byte_offset(&parsed_clean, rot_out_acc_idx);

    let mut bin_bad_quat = parsed_clean.bin_data.to_vec();
    for (k, &q) in [0.0f32, 0.0, 0.0, 0.5].iter().enumerate() {
        let off = rot_offset + k * 4;
        bin_bad_quat[off..off + 4].copy_from_slice(&q.to_le_bytes());
    }
    let glb_bad_quat = rebuild_glb(&parsed_clean.json, &bin_bad_quat);
    let parsed_bad_quat = parse_and_validate_glb_container(&glb_bad_quat).unwrap();
    let err_bad_quat = validate_glb_animations(&parsed_bad_quat, &["idle", "walk", "turnaround"]);
    assert!(
        err_bad_quat.is_err(),
        "Non-unit rotation quaternion must be rejected"
    );
    assert!(err_bad_quat.unwrap_err().contains("has non-unit norm"));

    // 3D. Channel Target Node Out-of-Bounds (node 25 >= 15)
    let mut bad_json_target_node = parsed_clean.json.clone();
    bad_json_target_node["animations"][0]["channels"][0]["target"]["node"] = serde_json::json!(25);
    let glb_bad_target_node = rebuild_glb(&bad_json_target_node, parsed_clean.bin_data);
    let parsed_bad_target_node = parse_and_validate_glb_container(&glb_bad_target_node).unwrap();
    let err_bad_target_node =
        validate_glb_animations(&parsed_bad_target_node, &["idle", "walk", "turnaround"]);
    assert!(
        err_bad_target_node.is_err(),
        "Target node 25 >= 15 must be rejected"
    );
    assert!(err_bad_target_node
        .unwrap_err()
        .contains("target node 25 >= 15"));

    // 3E. Invalid Channel Target Path
    let mut bad_json_target_path = parsed_clean.json.clone();
    bad_json_target_path["animations"][0]["channels"][0]["target"]["path"] =
        serde_json::json!("morph_weights");
    let glb_bad_target_path = rebuild_glb(&bad_json_target_path, parsed_clean.bin_data);
    let parsed_bad_target_path = parse_and_validate_glb_container(&glb_bad_target_path).unwrap();
    let err_bad_target_path =
        validate_glb_animations(&parsed_bad_target_path, &["idle", "walk", "turnaround"]);
    assert!(
        err_bad_target_path.is_err(),
        "Invalid channel target path must be rejected"
    );
    assert!(err_bad_target_path
        .unwrap_err()
        .contains("Invalid channel target path 'morph_weights'"));

    // =========================================================================
    // CATEGORY 4: GEOMETRY ATTRIBUTES (Non-unit Normals, Out-of-Bounds Indices)
    // =========================================================================

    // 4A. Non-Unit Normal Vector (norm = 2.0 != 1.0)
    let norm_acc_idx = parsed_clean.json["meshes"][0]["primitives"][0]["attributes"]["NORMAL"]
        .as_u64()
        .unwrap() as usize;
    let norm_offset = get_accessor_byte_offset(&parsed_clean, norm_acc_idx);
    let mut bin_bad_norm = parsed_clean.bin_data.to_vec();
    for (k, &n) in [0.0f32, 0.0, 2.0].iter().enumerate() {
        let off = norm_offset + k * 4;
        bin_bad_norm[off..off + 4].copy_from_slice(&n.to_le_bytes());
    }
    let glb_bad_norm = rebuild_glb(&parsed_clean.json, &bin_bad_norm);
    let parsed_bad_norm = parse_and_validate_glb_container(&glb_bad_norm).unwrap();
    let err_bad_norm = validate_glb_mesh(&parsed_bad_norm);
    assert!(err_bad_norm.is_err(), "Non-unit normal must be rejected");
    assert!(err_bad_norm.unwrap_err().contains("has non-unit length"));

    // 4B. Out-of-Bounds Triangle Index
    let idx_acc_idx = parsed_clean.json["meshes"][0]["primitives"][0]["indices"]
        .as_u64()
        .unwrap() as usize;
    let idx_offset = get_accessor_byte_offset(&parsed_clean, idx_acc_idx);
    let mut bin_bad_idx = parsed_clean.bin_data.to_vec();
    bin_bad_idx[idx_offset..idx_offset + 2].copy_from_slice(&0xFFFFu16.to_le_bytes());
    let glb_bad_idx = rebuild_glb(&parsed_clean.json, &bin_bad_idx);
    let parsed_bad_idx = parse_and_validate_glb_container(&glb_bad_idx).unwrap();
    let err_bad_idx = validate_glb_mesh(&parsed_bad_idx);
    assert!(err_bad_idx.is_err(), "Out-of-bounds index must be rejected");
    assert!(err_bad_idx
        .unwrap_err()
        .contains("out of bounds for vertex count"));

    // =========================================================================
    // CATEGORY 5: BUFFERVIEW ALIGNMENT & BIN BOUNDS STRESS
    // =========================================================================

    // 5A. BufferView Unaligned Byte Offset
    let mut bad_json_bv = parsed_clean.json.clone();
    bad_json_bv["bufferViews"][0]["byteOffset"] = serde_json::json!(1);
    let glb_bad_bv = rebuild_glb(&bad_json_bv, parsed_clean.bin_data);
    let err_bv_align = parse_and_validate_glb_container(&glb_bad_bv);
    assert!(err_bv_align.is_err());
    assert!(err_bv_align.unwrap_err().contains("is not 4-byte aligned"));

    // 5B. BufferView Extent Exceeds BIN Chunk Length
    let mut bad_json_bv_len = parsed_clean.json.clone();
    bad_json_bv_len["bufferViews"][0]["byteLength"] =
        serde_json::json!(parsed_clean.bin_data.len() + 100);
    let glb_bad_bv_len = rebuild_glb(&bad_json_bv_len, parsed_clean.bin_data);
    let err_bv_len = parse_and_validate_glb_container(&glb_bad_bv_len);
    assert!(err_bv_len.is_err());
    assert!(err_bv_len.unwrap_err().contains("exceeds BIN chunk length"));
}
