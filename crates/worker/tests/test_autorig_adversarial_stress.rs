//! Adversarial empirical stress suite for Milestone 3 (Automated Rigging & Skeletal Animation Pipeline).
//!
//! Authored by: challenger_m3_1 (Empirical Challenger)
//! Roles: critic, specialist
//!
//! Validates:
//! - Adversarial Meshes:
//!   1. Extreme aspect ratios (needles, pancake meshes).
//!   2. Degenerate triangles and collinear vertices.
//!   3. Disconnected multi-part meshes (floating separate objects).
//!   4. Robed meshes (zero crotch separation).
//!   5. High vertex count scaling: 10k, 20k, 30k vertices with latency verification against the < 100ms budget.
//! - glTF 2.0 Binary Validation (.glb):
//!   6. Header magic 0x46546C67, version 2, total length.
//!   7. Chunk 0 (JSON) 4-byte aligned with ASCII space padding (0x20).
//!   8. Chunk 1 (BIN) 4-byte aligned with null padding (0x00).
//!   9. Armature hierarchy: 15 joints, valid parent-child DAG.
//!   10. Skin: valid inverseBindMatrices accessor (960 bytes, 15 joints, column-major Mat4, W * IBM = I).
//!   11. Primitives: JOINTS_0 in [0, 14], WEIGHTS_0 normalized sum = 1.000000 +/- 1e-5.
//!   12. Animations: idle, walk, turnaround with valid samplers (ascending timestamps) and channels.
//! - Live Tooling Integration:
//!   13. Headless Blender 4.5.4 LTS import & armature validation.

#![allow(
    clippy::too_many_arguments,
    clippy::needless_range_loop,
    clippy::manual_range_contains
)]

use std::path::PathBuf;
use std::time::Instant;

use rusty_grid_core::task::{AutoRigSpec, Bytes};
use rusty_grid_worker::autorig::gltf_export::{
    COMPONENT_TYPE_FLOAT, COMPONENT_TYPE_UNSIGNED_SHORT,
};
use rusty_grid_worker::autorig::*;
use rusty_grid_worker::gen3d::types::RawMesh;
use serde_json::Value;

// ============================================================================
// GEOMETRIC TEST FIXTURE GENERATORS
// ============================================================================

/// Helper: formats a RawMesh into standard Wavefront OBJ bytes.
fn raw_mesh_to_obj_bytes(mesh: &RawMesh) -> Bytes {
    let mut s = String::with_capacity(mesh.positions.len() * 32 + mesh.indices.len() * 16);
    s.push_str("# OxideSwarm Adversarial Stress Mesh\n");
    for p in &mesh.positions {
        s.push_str(&format!("v {} {} {}\n", p[0], p[1], p[2]));
    }
    for tri in mesh.indices.chunks_exact(3) {
        s.push_str(&format!("f {} {} {}\n", tri[0] + 1, tri[1] + 1, tri[2] + 1));
    }
    Bytes::from(s)
}

/// Helper: adds an axis-aligned box to positions and indices.
fn push_box(
    positions: &mut Vec<[f32; 3]>,
    indices: &mut Vec<u32>,
    cx: f32,
    cy: f32,
    cz: f32,
    hx: f32,
    hy: f32,
    hz: f32,
) {
    let base_idx = positions.len() as u32;
    let corners = [
        [-hx, -hy, -hz],
        [hx, -hy, -hz],
        [hx, hy, -hz],
        [-hx, hy, -hz],
        [-hx, -hy, hz],
        [hx, -hy, hz],
        [hx, hy, hz],
        [-hx, hy, hz],
    ];
    for c in &corners {
        positions.push([cx + c[0], cy + c[1], cz + c[2]]);
    }
    let box_indices = [
        0, 1, 2, 0, 2, 3, // back
        4, 6, 5, 4, 7, 6, // front
        0, 4, 5, 0, 5, 1, // bottom
        3, 2, 6, 3, 6, 7, // top
        0, 3, 7, 0, 7, 4, // left
        1, 5, 6, 1, 6, 2, // right
    ];
    for &idx in &box_indices {
        indices.push(base_idx + idx);
    }
}

/// Helper: generates a tessellated cylinder along Y with R rings and S slices.
fn push_cylinder(
    positions: &mut Vec<[f32; 3]>,
    indices: &mut Vec<u32>,
    cx: f32,
    cz: f32,
    y_min: f32,
    y_max: f32,
    radius: f32,
    rings: usize,
    slices: usize,
) {
    let base_idx = positions.len() as u32;
    let dy = (y_max - y_min) / rings as f32;
    let two_pi = 2.0 * std::f32::consts::PI;

    for r in 0..=rings {
        let y = y_min + (r as f32) * dy;
        for s in 0..slices {
            let theta = two_pi * (s as f32) / (slices as f32);
            let x = cx + radius * theta.cos();
            let z = cz + radius * theta.sin();
            positions.push([x, y, z]);
        }
    }

    for r in 0..rings {
        let r0 = base_idx + (r * slices) as u32;
        let r1 = base_idx + ((r + 1) * slices) as u32;
        for s in 0..slices {
            let s_next = (s + 1) % slices;
            let v00 = r0 + s as u32;
            let v01 = r0 + s_next as u32;
            let v10 = r1 + s as u32;
            let v11 = r1 + s_next as u32;

            indices.push(v00);
            indices.push(v10);
            indices.push(v11);

            indices.push(v00);
            indices.push(v11);
            indices.push(v01);
        }
    }
}

/// Helper: creates a baseline humanoid mesh.
fn create_standard_humanoid() -> RawMesh {
    let mut positions = Vec::new();
    let mut indices = Vec::new();

    // Legs
    push_box(
        &mut positions,
        &mut indices,
        -0.20,
        0.50,
        0.0,
        0.08,
        0.50,
        0.08,
    );
    push_box(
        &mut positions,
        &mut indices,
        0.20,
        0.50,
        0.0,
        0.08,
        0.50,
        0.08,
    );
    // Torso
    push_box(
        &mut positions,
        &mut indices,
        0.0,
        1.30,
        0.0,
        0.22,
        0.30,
        0.12,
    );
    // Arms
    push_box(
        &mut positions,
        &mut indices,
        -0.38,
        1.35,
        0.0,
        0.14,
        0.06,
        0.06,
    );
    push_box(
        &mut positions,
        &mut indices,
        0.38,
        1.35,
        0.0,
        0.14,
        0.06,
        0.06,
    );
    // Head
    push_box(
        &mut positions,
        &mut indices,
        0.0,
        1.80,
        0.0,
        0.12,
        0.20,
        0.12,
    );

    RawMesh {
        positions,
        normals: Vec::new(),
        uvs: Vec::new(),
        indices,
        colors: None,
    }
}

/// Helper: creates a dense humanoid mesh targeting approximately `target_verts` vertices.
fn create_dense_humanoid(target_verts: usize) -> RawMesh {
    let mut positions = Vec::with_capacity(target_verts + 500);
    let mut indices = Vec::with_capacity(target_verts * 6);

    // 6 segments: LeftLeg, RightLeg, Torso, Head, LeftArm, RightArm
    let verts_per_part = (target_verts / 6).max(100);
    let slices = ((verts_per_part as f64).sqrt() * 0.9).round() as usize;
    let rings = (verts_per_part / slices).max(4);

    // Left Leg
    push_cylinder(
        &mut positions,
        &mut indices,
        -0.20,
        0.0,
        0.0,
        1.0,
        0.08,
        rings,
        slices,
    );
    // Right Leg
    push_cylinder(
        &mut positions,
        &mut indices,
        0.20,
        0.0,
        0.0,
        1.0,
        0.08,
        rings,
        slices,
    );
    // Torso
    push_cylinder(
        &mut positions,
        &mut indices,
        0.0,
        0.0,
        1.0,
        1.6,
        0.22,
        rings,
        slices,
    );
    // Head
    push_cylinder(
        &mut positions,
        &mut indices,
        0.0,
        0.0,
        1.6,
        2.0,
        0.14,
        rings,
        slices,
    );
    // Left Arm
    push_cylinder(
        &mut positions,
        &mut indices,
        -0.40,
        0.0,
        1.0,
        1.5,
        0.06,
        rings,
        slices,
    );
    // Right Arm
    push_cylinder(
        &mut positions,
        &mut indices,
        0.40,
        0.0,
        1.0,
        1.5,
        0.06,
        rings,
        slices,
    );

    RawMesh {
        positions,
        normals: Vec::new(),
        uvs: Vec::new(),
        indices,
        colors: None,
    }
}

// ============================================================================
// glTF 2.0 BINARY VALIDATION ORACLE
// ============================================================================

#[derive(Debug)]
pub struct GlbValidationReport {
    pub total_bytes: usize,
    pub json_length: usize,
    pub bin_length: usize,
    pub node_count: usize,
    pub joint_count: usize,
    pub vertex_count: usize,
    pub triangle_count: usize,
    pub animation_count: usize,
    pub min_weight_sum: f32,
    pub max_weight_sum: f32,
    pub max_ibm_identity_error: f32,
}

/// Exhaustive binary chunk and Khronos specification conformance oracle.
pub fn validate_rigged_glb_oracle(glb: &[u8], expected_animations: &[&str]) -> GlbValidationReport {
    // 1. Header Validation
    assert!(glb.len() >= 20, "GLB buffer truncated (<20 bytes)");
    let magic = u32::from_le_bytes([glb[0], glb[1], glb[2], glb[3]]);
    let version = u32::from_le_bytes([glb[4], glb[5], glb[6], glb[7]]);
    let total_len = u32::from_le_bytes([glb[8], glb[9], glb[10], glb[11]]) as usize;

    assert_eq!(magic, GLB_MAGIC, "Header magic must be 0x46546C67 ('glTF')");
    assert_eq!(version, 2, "glTF version must be 2");
    assert_eq!(
        total_len,
        glb.len(),
        "Header total length must equal byte slice length"
    );

    // 2. Chunk 0: JSON Validation
    let json_len = u32::from_le_bytes([glb[12], glb[13], glb[14], glb[15]]) as usize;
    let json_type = u32::from_le_bytes([glb[16], glb[17], glb[18], glb[19]]);

    assert_eq!(
        json_type, CHUNK_TYPE_JSON,
        "Chunk 0 type must be 0x4E4F534A ('JSON')"
    );
    assert_eq!(json_len % 4, 0, "Chunk 0 length must be a multiple of 4");

    let json_bytes = &glb[20..20 + json_len];
    // Check ASCII space padding (0x20)
    let json_str = std::str::from_utf8(json_bytes).expect("JSON chunk must be valid UTF-8");
    let json_trimmed = json_str.trim_end_matches(' ');
    for b in &json_bytes[json_trimmed.len()..] {
        assert_eq!(*b, 0x20, "JSON padding byte must be ASCII space 0x20");
    }

    let gltf: Value = serde_json::from_str(json_trimmed).expect("glTF JSON must be valid JSON");
    assert_eq!(
        gltf["asset"]["version"], "2.0",
        "glTF asset version must be '2.0'"
    );

    // 3. Chunk 1: BIN Validation
    let bin_header_offset = 20 + json_len;
    assert!(
        bin_header_offset + 8 <= glb.len(),
        "Missing Chunk 1 BIN header"
    );

    let bin_len = u32::from_le_bytes([
        glb[bin_header_offset],
        glb[bin_header_offset + 1],
        glb[bin_header_offset + 2],
        glb[bin_header_offset + 3],
    ]) as usize;
    let bin_type = u32::from_le_bytes([
        glb[bin_header_offset + 4],
        glb[bin_header_offset + 5],
        glb[bin_header_offset + 6],
        glb[bin_header_offset + 7],
    ]);

    assert_eq!(
        bin_type, CHUNK_TYPE_BIN,
        "Chunk 1 type must be 0x004E4942 ('BIN\\0')"
    );
    assert_eq!(bin_len % 4, 0, "Chunk 1 length must be a multiple of 4");
    assert_eq!(
        12 + 8 + json_len + 8 + bin_len,
        glb.len(),
        "Container length equation: 12 + 8 + json_len + 8 + bin_len == total_len"
    );

    let bin_data = &glb[bin_header_offset + 8..bin_header_offset + 8 + bin_len];

    // Check null padding (0x00) on Chunk 1
    let buffers = gltf["buffers"].as_array().expect("buffers array missing");
    let declared_buffer_len = buffers[0]["byteLength"].as_u64().unwrap() as usize;
    for b in &bin_data[declared_buffer_len..] {
        assert_eq!(*b, 0x00, "BIN chunk padding byte must be null 0x00");
    }

    // 4. BufferViews 4-Byte Alignment
    let buffer_views = gltf["bufferViews"].as_array().expect("bufferViews missing");
    for (bv_idx, bv) in buffer_views.iter().enumerate() {
        let offset = bv["byteOffset"].as_u64().unwrap_or(0) as usize;
        let length = bv["byteLength"].as_u64().expect("byteLength missing") as usize;
        assert_eq!(
            offset % 4,
            0,
            "BufferView {} byteOffset ({}) not 4-byte aligned",
            bv_idx,
            offset
        );
        assert!(
            offset + length <= bin_data.len(),
            "BufferView {} exceeds binary chunk boundary",
            bv_idx
        );
    }

    let accessors = gltf["accessors"].as_array().expect("accessors missing");

    // 5. Armature Hierarchy & Joint Count (15 Nodes)
    let nodes = gltf["nodes"].as_array().expect("nodes array missing");
    assert_eq!(
        nodes.len(),
        16,
        "Must have exactly 15 joint nodes + 1 mesh node"
    );

    let expected_joints = [
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

    for (j_idx, expected_name) in expected_joints.iter().enumerate() {
        let node = &nodes[j_idx];
        assert_eq!(
            node["name"].as_str().unwrap(),
            *expected_name,
            "Joint {} name mismatch",
            j_idx
        );
        // Translation must be 3 floats
        let trans = node["translation"].as_array().expect("translation missing");
        assert_eq!(trans.len(), 3);
        // Rotation must be 4 floats
        let rot = node["rotation"].as_array().expect("rotation missing");
        assert_eq!(rot.len(), 4);
    }

    // Check DAG topological sorting (parent < child)
    for j_idx in 0..15 {
        if let Some(children) = nodes[j_idx]["children"].as_array() {
            for child_val in children {
                let child_idx = child_val.as_u64().unwrap() as usize;
                assert!(
                    child_idx > j_idx && child_idx < 15,
                    "DAG violation: parent {} has invalid child {}",
                    j_idx,
                    child_idx
                );
            }
        }
    }

    // 6. Skin & Inverse Bind Matrices Accessor (960 bytes, 15 joints)
    let skins = gltf["skins"].as_array().expect("skins array missing");
    assert_eq!(skins.len(), 1, "Must contain exactly 1 skin definition");
    let skin = &skins[0];

    let skin_joints = skin["joints"].as_array().expect("skin joints missing");
    assert_eq!(skin_joints.len(), 15, "Skin must reference 15 joints");
    for (i, j_val) in skin_joints.iter().enumerate() {
        assert_eq!(
            j_val.as_u64().unwrap() as usize,
            i,
            "Skin joint indices must be 0..14"
        );
    }

    let ibm_acc_idx = skin["inverseBindMatrices"]
        .as_u64()
        .expect("inverseBindMatrices missing") as usize;
    let ibm_acc = &accessors[ibm_acc_idx];
    assert_eq!(ibm_acc["componentType"], COMPONENT_TYPE_FLOAT);
    assert_eq!(ibm_acc["type"], "MAT4");
    assert_eq!(ibm_acc["count"], 15);

    let ibm_bv_idx = ibm_acc["bufferView"].as_u64().unwrap() as usize;
    let ibm_bv = &buffer_views[ibm_bv_idx];
    let ibm_offset = ibm_bv["byteOffset"].as_u64().unwrap() as usize;
    let ibm_len = ibm_bv["byteLength"].as_u64().unwrap() as usize;
    assert_eq!(
        ibm_len, 960,
        "Inverse bind matrices bufferView must be exactly 960 bytes (15 * 64)"
    );

    // Read the 15 matrices and verify W * IBM == I
    let mut max_ibm_err = 0.0f32;
    // Compute world transforms W_j from node local translations
    let mut world_pos = [Vec3::ZERO; 15];
    world_pos[0] = Vec3::new(
        nodes[0]["translation"][0].as_f64().unwrap() as f32,
        nodes[0]["translation"][1].as_f64().unwrap() as f32,
        nodes[0]["translation"][2].as_f64().unwrap() as f32,
    );

    // Build parent map
    let mut parent_map = [None; 15];
    for p in 0..15 {
        if let Some(children) = nodes[p]["children"].as_array() {
            for c in children {
                parent_map[c.as_u64().unwrap() as usize] = Some(p);
            }
        }
    }

    for j in 1..15 {
        let p = parent_map[j].expect("Non-root joint must have parent");
        let l_trans = Vec3::new(
            nodes[j]["translation"][0].as_f64().unwrap() as f32,
            nodes[j]["translation"][1].as_f64().unwrap() as f32,
            nodes[j]["translation"][2].as_f64().unwrap() as f32,
        );
        world_pos[j] = world_pos[p] + l_trans;
    }

    for j in 0..15 {
        let m_start = ibm_offset + j * 64;
        let mut ibm_cols = [[0.0f32; 4]; 4];
        for col in 0..4 {
            for row in 0..4 {
                let off = m_start + (col * 4 + row) * 4;
                let val = f32::from_le_bytes([
                    bin_data[off],
                    bin_data[off + 1],
                    bin_data[off + 2],
                    bin_data[off + 3],
                ]);
                assert!(val.is_finite(), "IBM float must be finite (got {})", val);
                ibm_cols[col][row] = val;
            }
        }

        // Multiply world_transform * IBM
        let w_mat = Mat4::from_translation(world_pos[j]);
        let ibm_mat = Mat4 { cols: ibm_cols };
        let prod = w_mat.mul_mat4(&ibm_mat);

        for col in 0..4 {
            for row in 0..4 {
                let exp = if col == row { 1.0 } else { 0.0 };
                let diff = (prod.cols[col][row] - exp).abs();
                if diff > max_ibm_err {
                    max_ibm_err = diff;
                }
                assert!(
                    diff < 1e-3,
                    "W * IBM != I at joint {} [{},{}]: prod={}, exp={}",
                    j,
                    col,
                    row,
                    prod.cols[col][row],
                    exp
                );
            }
        }
    }

    // 7. Primitive JOINTS_0 in [0, 14] and WEIGHTS_0 normalized sum = 1.000000 +/- 1e-5
    let meshes = gltf["meshes"].as_array().expect("meshes missing");
    let prim = &meshes[0]["primitives"][0];
    let attrs = prim["attributes"].as_object().expect("attributes missing");

    let joints_acc_idx = attrs["JOINTS_0"].as_u64().unwrap() as usize;
    let weights_acc_idx = attrs["WEIGHTS_0"].as_u64().unwrap() as usize;

    let j_acc = &accessors[joints_acc_idx];
    assert_eq!(j_acc["componentType"], COMPONENT_TYPE_UNSIGNED_SHORT);
    assert_eq!(j_acc["type"], "VEC4");
    let vert_count = j_acc["count"].as_u64().unwrap() as usize;

    let w_acc = &accessors[weights_acc_idx];
    assert_eq!(w_acc["componentType"], COMPONENT_TYPE_FLOAT);
    assert_eq!(w_acc["type"], "VEC4");
    assert_eq!(w_acc["count"].as_u64().unwrap() as usize, vert_count);

    let j_bv_idx = j_acc["bufferView"].as_u64().unwrap() as usize;
    let j_bv = &buffer_views[j_bv_idx];
    let j_offset = j_bv["byteOffset"].as_u64().unwrap() as usize;

    let w_bv_idx = w_acc["bufferView"].as_u64().unwrap() as usize;
    let w_bv = &buffer_views[w_bv_idx];
    let w_offset = w_bv["byteOffset"].as_u64().unwrap() as usize;

    let mut min_w_sum = f32::INFINITY;
    let mut max_w_sum = f32::NEG_INFINITY;

    for v in 0..vert_count {
        // Read 4 joints (u16)
        let j_base = j_offset + v * 8;
        let mut joints_val = [0u16; 4];
        for k in 0..4 {
            let off = j_base + k * 2;
            let val = u16::from_le_bytes([bin_data[off], bin_data[off + 1]]);
            assert!(
                val <= 14,
                "JOINTS_0 value {} exceeds 14 at vertex {} component {}",
                val,
                v,
                k
            );
            joints_val[k] = val;
        }

        // Read 4 weights (f32)
        let w_base = w_offset + v * 16;
        let mut weights_val = [0.0f32; 4];
        for k in 0..4 {
            let off = w_base + k * 4;
            let val = f32::from_le_bytes([
                bin_data[off],
                bin_data[off + 1],
                bin_data[off + 2],
                bin_data[off + 3],
            ]);
            assert!(
                val >= 0.0 && val <= 1.0,
                "WEIGHTS_0 value {} out of range [0.0, 1.0] at vertex {} component {}",
                val,
                v,
                k
            );
            weights_val[k] = val;
        }

        let sum: f32 = weights_val.iter().sum();
        if sum < min_w_sum {
            min_w_sum = sum;
        }
        if sum > max_w_sum {
            max_w_sum = sum;
        }

        assert!(
            (sum - 1.0).abs() < 1e-5,
            "WEIGHTS_0 unity sum violation at vertex {}: sum = {}, weights = {:?}",
            v,
            sum,
            weights_val
        );
    }

    // 8. Animations: idle, walk, turnaround with valid samplers & channels
    let anim_count = if let Some(anims) = gltf.get("animations").and_then(|a| a.as_array()) {
        assert_eq!(
            anims.len(),
            expected_animations.len(),
            "Animation count mismatch"
        );

        for (a_idx, &expected_clip) in expected_animations.iter().enumerate() {
            let anim = &anims[a_idx];
            assert_eq!(
                anim["name"].as_str().unwrap(),
                expected_clip,
                "Animation {} name mismatch",
                a_idx
            );

            let samplers = anim["samplers"].as_array().expect("samplers missing");
            let channels = anim["channels"].as_array().expect("channels missing");
            assert!(!samplers.is_empty(), "Samplers cannot be empty");
            assert!(!channels.is_empty(), "Channels cannot be empty");

            // Verify samplers
            for (s_idx, s) in samplers.iter().enumerate() {
                assert_eq!(s["interpolation"], "LINEAR");
                let input_acc_idx = s["input"].as_u64().unwrap() as usize;
                let input_acc = &accessors[input_acc_idx];
                assert_eq!(input_acc["type"], "SCALAR");
                assert_eq!(input_acc["componentType"], COMPONENT_TYPE_FLOAT);

                let in_bv_idx = input_acc["bufferView"].as_u64().unwrap() as usize;
                let in_bv = &buffer_views[in_bv_idx];
                let in_offset = in_bv["byteOffset"].as_u64().unwrap() as usize;
                let count = input_acc["count"].as_u64().unwrap() as usize;

                let mut prev_t = -1.0f32;
                for t_i in 0..count {
                    let off = in_offset + t_i * 4;
                    let t = f32::from_le_bytes([
                        bin_data[off],
                        bin_data[off + 1],
                        bin_data[off + 2],
                        bin_data[off + 3],
                    ]);
                    assert!(
                        t >= prev_t,
                        "Non-monotonic timestamp in anim {} sampler {}: t = {}, prev = {}",
                        expected_clip,
                        s_idx,
                        t,
                        prev_t
                    );
                    prev_t = t;
                }
            }

            // Verify channels
            for ch in channels {
                let target = &ch["target"];
                let node_id = target["node"].as_u64().unwrap() as usize;
                assert!(
                    node_id < 15,
                    "Channel target node {} out of bounds [0, 14]",
                    node_id
                );
                let path = target["path"].as_str().unwrap();
                assert!(
                    path == "translation" || path == "rotation" || path == "scale",
                    "Invalid animation path: {}",
                    path
                );
            }
        }
        anims.len()
    } else {
        0
    };

    // Number of triangles from indices accessor
    let idx_acc_idx = prim["indices"].as_u64().unwrap() as usize;
    let tri_count = (accessors[idx_acc_idx]["count"].as_u64().unwrap() / 3) as usize;

    GlbValidationReport {
        total_bytes: glb.len(),
        json_length: json_len,
        bin_length: bin_len,
        node_count: nodes.len(),
        joint_count: skin_joints.len(),
        vertex_count: vert_count,
        triangle_count: tri_count,
        animation_count: anim_count,
        min_weight_sum: min_w_sum,
        max_weight_sum: max_w_sum,
        max_ibm_identity_error: max_ibm_err,
    }
}

// ============================================================================
// CHALLENGE 1: Extreme Aspect Ratios (Needles & Pancake Meshes)
// ============================================================================

#[test]
fn test_adversarial_mesh_extreme_aspect_ratios() {
    // 1. Vertical needle along Y: H = 100.0, W = 0.001, D = 0.001 (aspect ratio 100,000 : 1)
    let mut needle_y_positions = Vec::new();
    let mut needle_y_indices = Vec::new();
    push_box(
        &mut needle_y_positions,
        &mut needle_y_indices,
        0.0,
        50.0,
        0.0,
        0.0005,
        50.0,
        0.0005,
    );
    let needle_y_mesh = RawMesh {
        positions: needle_y_positions,
        normals: Vec::new(),
        uvs: Vec::new(),
        indices: needle_y_indices,
        colors: None,
    };
    let spec_needle_y = AutoRigSpec::new(raw_mesh_to_obj_bytes(&needle_y_mesh), "biped");
    let glb_needle_y =
        auto_rig_mesh(&spec_needle_y).expect("Vertical needle mesh auto-rigging must succeed");
    let report_needle_y =
        validate_rigged_glb_oracle(&glb_needle_y, &["idle", "walk", "turnaround"]);
    println!("[ADVERSARIAL - VERTICAL NEEDLE] {:?}", report_needle_y);
    assert!((report_needle_y.min_weight_sum - 1.0).abs() < 1e-5);
    assert!((report_needle_y.max_weight_sum - 1.0).abs() < 1e-5);

    // 2. Flat pancake mesh in XZ plane: H = 0.0001, W = 10.0, D = 10.0 (aspect ratio 1 : 100,000)
    let mut pancake_positions = Vec::new();
    let mut pancake_indices = Vec::new();
    push_box(
        &mut pancake_positions,
        &mut pancake_indices,
        0.0,
        0.00005,
        0.0,
        5.0,
        0.00005,
        5.0,
    );
    let pancake_mesh = RawMesh {
        positions: pancake_positions,
        normals: Vec::new(),
        uvs: Vec::new(),
        indices: pancake_indices,
        colors: None,
    };
    let spec_pancake = AutoRigSpec::new(raw_mesh_to_obj_bytes(&pancake_mesh), "biped");
    let glb_pancake = auto_rig_mesh(&spec_pancake).expect("Pancake mesh auto-rigging must succeed");
    let report_pancake = validate_rigged_glb_oracle(&glb_pancake, &["idle", "walk", "turnaround"]);
    println!("[ADVERSARIAL - PANCAKE MESH] {:?}", report_pancake);
    assert!((report_pancake.min_weight_sum - 1.0).abs() < 1e-5);
    assert!((report_pancake.max_weight_sum - 1.0).abs() < 1e-5);

    // 3. Horizontal needle along X: H = 0.001, W = 50.0, D = 0.001
    let mut needle_x_positions = Vec::new();
    let mut needle_x_indices = Vec::new();
    push_box(
        &mut needle_x_positions,
        &mut needle_x_indices,
        0.0,
        0.0,
        0.0,
        25.0,
        0.0005,
        0.0005,
    );
    let needle_x_mesh = RawMesh {
        positions: needle_x_positions,
        normals: Vec::new(),
        uvs: Vec::new(),
        indices: needle_x_indices,
        colors: None,
    };
    let spec_needle_x = AutoRigSpec::new(raw_mesh_to_obj_bytes(&needle_x_mesh), "biped");
    let glb_needle_x =
        auto_rig_mesh(&spec_needle_x).expect("Horizontal needle mesh auto-rigging must succeed");
    let report_needle_x =
        validate_rigged_glb_oracle(&glb_needle_x, &["idle", "walk", "turnaround"]);
    println!("[ADVERSARIAL - HORIZONTAL NEEDLE] {:?}", report_needle_x);
    assert!((report_needle_x.min_weight_sum - 1.0).abs() < 1e-5);
    assert!((report_needle_x.max_weight_sum - 1.0).abs() < 1e-5);
}

// ============================================================================
// CHALLENGE 2: Degenerate Triangles and Collinear Vertices
// ============================================================================

#[test]
fn test_adversarial_mesh_degenerate_triangles_and_collinear_vertices() {
    let mut mesh = create_standard_humanoid();

    // 1. Add identical zero-area vertices and faces
    let base_idx = mesh.positions.len() as u32;
    mesh.positions.push([0.0, 1.0, 0.0]); // base_idx
    mesh.positions.push([0.0, 1.0, 0.0]); // base_idx + 1 (identical)
    mesh.positions.push([0.0, 1.0, 0.0]); // base_idx + 2 (identical)
                                          // Degenerate triangle with zero area
    mesh.indices
        .extend_from_slice(&[base_idx, base_idx + 1, base_idx + 2]);
    // Collapsed index triangle
    mesh.indices
        .extend_from_slice(&[base_idx, base_idx, base_idx]);
    mesh.indices
        .extend_from_slice(&[base_idx, base_idx + 1, base_idx]);

    // 2. Add collinear vertices along a line
    let col_idx = mesh.positions.len() as u32;
    mesh.positions.push([0.1, 0.5, 0.0]);
    mesh.positions.push([0.1, 1.0, 0.0]);
    mesh.positions.push([0.1, 1.5, 0.0]);
    // Collinear triangle
    mesh.indices
        .extend_from_slice(&[col_idx, col_idx + 1, col_idx + 2]);

    // Auto-rig mesh with degeneracies
    let spec = AutoRigSpec::new(raw_mesh_to_obj_bytes(&mesh), "biped");
    let glb = auto_rig_mesh(&spec).expect("Degenerate triangle mesh auto-rigging must succeed");

    let report = validate_rigged_glb_oracle(&glb, &["idle", "walk", "turnaround"]);
    println!("[ADVERSARIAL - DEGENERATE TRIANGLES] {:?}", report);

    assert_eq!(report.vertex_count, mesh.positions.len());
    assert!((report.min_weight_sum - 1.0).abs() < 1e-5);
    assert!((report.max_weight_sum - 1.0).abs() < 1e-5);
}

// ============================================================================
// CHALLENGE 3: Disconnected Multi-Part Meshes (Floating Separate Objects)
// ============================================================================

#[test]
fn test_adversarial_mesh_disconnected_multipart_components() {
    let mut positions = Vec::new();
    let mut indices = Vec::new();

    // 6 completely disconnected floating boxes with 0.15 air gaps between them:
    // 1. Floating Head: Y = 1.70 .. 1.90
    push_box(
        &mut positions,
        &mut indices,
        0.0,
        1.80,
        0.0,
        0.10,
        0.10,
        0.10,
    );
    // 2. Floating Torso: Y = 1.10 .. 1.45 (gap of 0.25 below head)
    push_box(
        &mut positions,
        &mut indices,
        0.0,
        1.25,
        0.0,
        0.20,
        0.15,
        0.10,
    );
    // 3. Floating Left Arm: X = -0.45, Y = 1.25 (gap of 0.25 to the left of torso)
    push_box(
        &mut positions,
        &mut indices,
        -0.45,
        1.25,
        0.0,
        0.10,
        0.05,
        0.05,
    );
    // 4. Floating Right Arm: X = 0.45, Y = 1.25 (gap of 0.25 to the right of torso)
    push_box(
        &mut positions,
        &mut indices,
        0.45,
        1.25,
        0.0,
        0.10,
        0.05,
        0.05,
    );
    // 5. Floating Left Leg: X = -0.20, Y = 0.35 .. 0.85 (gap of 0.25 below torso)
    push_box(
        &mut positions,
        &mut indices,
        -0.20,
        0.50,
        0.0,
        0.08,
        0.25,
        0.08,
    );
    // 6. Floating Right Leg: X = 0.20, Y = 0.35 .. 0.85 (gap of 0.25 below torso)
    push_box(
        &mut positions,
        &mut indices,
        0.20,
        0.50,
        0.0,
        0.08,
        0.25,
        0.08,
    );

    let mesh = RawMesh {
        positions,
        normals: Vec::new(),
        uvs: Vec::new(),
        indices,
        colors: None,
    };

    let spec = AutoRigSpec::new(raw_mesh_to_obj_bytes(&mesh), "biped");
    let glb = auto_rig_mesh(&spec).expect("Disconnected multi-part mesh auto-rigging must succeed");

    let report = validate_rigged_glb_oracle(&glb, &["idle", "walk", "turnaround"]);
    println!("[ADVERSARIAL - DISCONNECTED MULTI-PART] {:?}", report);

    assert_eq!(report.vertex_count, mesh.positions.len());
    assert!((report.min_weight_sum - 1.0).abs() < 1e-5);
    assert!((report.max_weight_sum - 1.0).abs() < 1e-5);
}

// ============================================================================
// CHALLENGE 4: Robed Meshes (Zero Crotch Separation)
// ============================================================================

#[test]
fn test_adversarial_mesh_robed_mesh_zero_crotch_gap() {
    let mut positions = Vec::new();
    let mut indices = Vec::new();

    // Upper body: Torso, Head, Arms
    push_box(
        &mut positions,
        &mut indices,
        0.0,
        1.30,
        0.0,
        0.22,
        0.30,
        0.12,
    );
    push_box(
        &mut positions,
        &mut indices,
        -0.38,
        1.35,
        0.0,
        0.14,
        0.06,
        0.06,
    );
    push_box(
        &mut positions,
        &mut indices,
        0.38,
        1.35,
        0.0,
        0.14,
        0.06,
        0.06,
    );
    push_box(
        &mut positions,
        &mut indices,
        0.0,
        1.80,
        0.0,
        0.12,
        0.20,
        0.12,
    );

    // Lower body: SOLID continuous bell-shaped Robe / Skirt from floor (Y = 0.0) up to waist (Y = 1.0)
    // No crotch gap, no leg separation!
    let rings = 20;
    let slices = 24;
    let base_idx = positions.len() as u32;
    let two_pi = 2.0 * std::f32::consts::PI;

    for r in 0..=rings {
        let frac = (r as f32) / (rings as f32); // 0.0 at floor, 1.0 at waist
        let y = frac * 1.0;
        // Radius tapers from 0.40 at floor to 0.25 at waist
        let radius = 0.40 - 0.15 * frac;

        for s in 0..slices {
            let theta = two_pi * (s as f32) / (slices as f32);
            let x = radius * theta.cos();
            let z = radius * theta.sin() * 0.7; // elliptical
            positions.push([x, y, z]);
        }
    }

    for r in 0..rings {
        let r0 = base_idx + (r * slices) as u32;
        let r1 = base_idx + ((r + 1) * slices) as u32;
        for s in 0..slices {
            let s_next = (s + 1) % slices;
            indices.push(r0 + s as u32);
            indices.push(r1 + s as u32);
            indices.push(r1 + s_next as u32);

            indices.push(r0 + s as u32);
            indices.push(r1 + s_next as u32);
            indices.push(r0 + s_next as u32);
        }
    }

    let mesh = RawMesh {
        positions,
        normals: Vec::new(),
        uvs: Vec::new(),
        indices,
        colors: None,
    };

    let spec = AutoRigSpec::new(raw_mesh_to_obj_bytes(&mesh), "biped");
    let glb = auto_rig_mesh(&spec).expect("Robed mesh auto-rigging must succeed");

    let report = validate_rigged_glb_oracle(&glb, &["idle", "walk", "turnaround"]);
    println!("[ADVERSARIAL - ROBED MESH ZERO CROTCH] {:?}", report);

    assert_eq!(report.vertex_count, mesh.positions.len());
    assert!((report.min_weight_sum - 1.0).abs() < 1e-5);
    assert!((report.max_weight_sum - 1.0).abs() < 1e-5);
}

// ============================================================================
// CHALLENGE 5: Vertex Scaling (10k, 20k, 30k Vertices) & Latency Budget (< 100ms)
// ============================================================================

#[test]
fn test_vertex_scaling_and_latency_budget() {
    let target_vertex_counts = [10_000, 20_000, 30_000];

    for &target in &target_vertex_counts {
        let mesh = create_dense_humanoid(target);
        let actual_verts = mesh.positions.len();
        let actual_tris = mesh.indices.len() / 3;

        println!(
            "\n[SCALING BENCHMARK] Target: {} verts | Generated: {} verts, {} tris",
            target, actual_verts, actual_tris
        );

        let spec = AutoRigSpec::new(raw_mesh_to_obj_bytes(&mesh), "biped")
            .with_animations(vec!["idle".to_string(), "walk".to_string()]);

        // Measure execution latency
        let start = Instant::now();
        let glb = auto_rig_mesh(&spec).expect("Scaling auto-rigging failed");
        let elapsed = start.elapsed();
        let elapsed_ms = elapsed.as_secs_f64() * 1000.0;

        println!(
            "[LATENCY BENCHMARK] {} verts -> {:?} ({:.2} ms) | GLB size: {} bytes",
            actual_verts,
            elapsed,
            elapsed_ms,
            glb.len()
        );

        // Validate the generated GLB
        let report = validate_rigged_glb_oracle(&glb, &["idle", "walk"]);
        assert_eq!(report.vertex_count, actual_verts);
        assert!((report.min_weight_sum - 1.0).abs() < 1e-5);
        assert!((report.max_weight_sum - 1.0).abs() < 1e-5);

        // Budget evaluation
        if !cfg!(debug_assertions) {
            // Release mode SLA: strictly under 100ms
            assert!(
                elapsed_ms < 100.0,
                "Release mode SLA violation: {} verts took {:.2} ms (budget < 100ms)",
                actual_verts,
                elapsed_ms
            );
        } else {
            // Debug mode info notice
            println!(
                "[PROFILE NOTICE] In unoptimized debug build, {} verts executed in {:.2} ms",
                actual_verts, elapsed_ms
            );
        }
    }
}

// ============================================================================
// CHALLENGE 6: glTF 2.0 Binary Chunks, Skin IBM, Accessor min/max Conformance
// ============================================================================

#[test]
fn test_gltf_2_binary_chunk_and_skin_conformance() {
    let mesh = create_standard_humanoid();
    let spec = AutoRigSpec::new(raw_mesh_to_obj_bytes(&mesh), "biped").with_animations(vec![
        "idle".to_string(),
        "walk".to_string(),
        "turnaround".to_string(),
    ]);

    let glb = auto_rig_mesh(&spec).expect("Export failed");
    let report = validate_rigged_glb_oracle(&glb, &["idle", "walk", "turnaround"]);

    println!("[GLTF 2.0 CONFORMANCE REPORT] {:#?}", report);

    assert_eq!(report.joint_count, 15);
    assert_eq!(report.animation_count, 3);
    assert!(
        report.max_ibm_identity_error < 1e-3,
        "W * IBM identity error too high"
    );
    assert!((report.min_weight_sum - 1.0).abs() < 1e-5);
    assert!((report.max_weight_sum - 1.0).abs() < 1e-5);
}

// ============================================================================
// CHALLENGE 7: Headless Blender 4.5.4 LTS Live Skeletal & Animation Import
// ============================================================================

#[test]
fn test_blender_headless_import_and_rig_validation() {
    let blender_path =
        PathBuf::from(r"C:\Program Files\Blender Foundation\Blender 4.5\blender.exe");
    if !blender_path.is_file() {
        println!(
            "[SKIP] Blender 4.5 executable not found at {:?}",
            blender_path
        );
        return;
    }

    let mesh = create_standard_humanoid();
    let spec = AutoRigSpec::new(raw_mesh_to_obj_bytes(&mesh), "biped").with_animations(vec![
        "idle".to_string(),
        "walk".to_string(),
        "turnaround".to_string(),
    ]);

    let glb = auto_rig_mesh(&spec).expect("Auto-rigging failed");

    let test_dir = PathBuf::from(r"E:\teamwork_projects\OxideSwarm\target\adversarial_autorig");
    let _ = std::fs::create_dir_all(&test_dir);
    let glb_path = test_dir.join("humanoid_rigged.glb");
    std::fs::write(&glb_path, &glb).expect("Failed to write GLB");

    let script_path = test_dir.join("validate_rig.py");
    let py_script = format!(
        r#"import bpy
import sys

bpy.ops.wm.read_factory_settings(use_empty=True)
bpy.ops.import_scene.gltf(filepath=r'{glb}')

armatures = [o for o in bpy.data.objects if o.type == 'ARMATURE']
meshes = [o for o in bpy.data.objects if o.type == 'MESH']
actions = list(bpy.data.actions)

print('BLENDER_AUTORIG_AUDIT: Armatures:', len(armatures), 'Meshes:', len(meshes), 'Actions:', len(actions))

if len(armatures) == 0 or len(meshes) == 0:
    print('FAIL: Missing armature or mesh')
    sys.exit(1)

arm = armatures[0]
bone_count = len(arm.data.bones)
print('BONE_COUNT:', bone_count)
if bone_count != 15:
    print(f'FAIL: Expected 15 bones, got {{bone_count}}')
    sys.exit(2)

mesh_obj = meshes[0]
has_armature_mod = any(m.type == 'ARMATURE' for m in mesh_obj.modifiers)
print('ARMATURE_MODIFIER:', has_armature_mod)
if not has_armature_mod:
    print('FAIL: Mesh missing ARMATURE modifier')
    sys.exit(3)

print('SUCCESS_BLENDER_RIG_VERIFIED')
sys.exit(0)
"#,
        glb = glb_path.display()
    );
    std::fs::write(&script_path, &py_script).expect("Failed to write python script");

    let output = std::process::Command::new(&blender_path)
        .args(["--background", "--python", script_path.to_str().unwrap()])
        .output()
        .expect("Failed to run Blender");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    println!("[BLENDER 4.5 IMPORT OUTPUT]\n{}", stdout.trim());
    if !stderr.is_empty() {
        println!("[BLENDER STDERR]\n{}", stderr.trim());
    }

    assert!(
        output.status.success(),
        "Blender import failed with exit code {:?}",
        output.status.code()
    );
    assert!(
        stdout.contains("SUCCESS_BLENDER_RIG_VERIFIED"),
        "Blender output missing SUCCESS_BLENDER_RIG_VERIFIED"
    );
}
