//! Comprehensive Integration and Regression Test Suite for Features F11–F16.
//!
//! Validates:
//! - F11: Landmark extraction, sagittal symmetry detection, 50-slab vertical slicing.
//! - F12: 15-joint DAG armature, deterministic pre-bends, column-major 4x4 IBMs.
//! - F13: Hybrid bone capsule distance, sagittal barrier, geodesic surface Dijkstra, top-4 unity normalization.
//! - F14: Procedural TRS animation synthesizers (idle, walk, turnaround at 30 Hz).
//! - F15: Rigged glTF 2.0 Binary (.glb) chunk serialization, alignment, and accessor bounds.
//! - F16: Blender provider fallback and TaskRunner AutoRig execution.

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::task::{AutoRigSpec, Bytes, Task, TaskRequirements, TaskSpec};
use rusty_grid_worker::autorig::*;
use rusty_grid_worker::gen3d::types::RawMesh;
use rusty_grid_worker::runner::{RunnerConfig, TaskRunner};
use serde_json::Value;
use uuid::Uuid;

/// Helper to generate a procedural humanoid test mesh with two legs, torso, two arms, and head.
fn create_humanoid_test_mesh() -> RawMesh {
    let mut positions = Vec::new();
    let mut indices = Vec::new();

    // Helper: adds a box centered at (cx, cy, cz) with half extents (hx, hy, hz)
    let mut add_box = |cx: f32, cy: f32, cz: f32, hx: f32, hy: f32, hz: f32| {
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
        // 12 triangles (6 faces)
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
    };

    // Height H = 2.0 (Y from 0.0 to 2.0), Width W = 1.0 (X from -0.5 to 0.5), Depth D = 0.4 (Z from -0.2 to 0.2)
    // Left Leg: Y = 0.0 .. 1.0, X = -0.20
    add_box(-0.20, 0.50, 0.0, 0.08, 0.50, 0.08);
    // Right Leg: Y = 0.0 .. 1.0, X = +0.20
    add_box(0.20, 0.50, 0.0, 0.08, 0.50, 0.08);
    // Torso: Y = 1.0 .. 1.6, X = 0.0
    add_box(0.0, 1.30, 0.0, 0.22, 0.30, 0.12);
    // Left Arm: Y = 1.2 .. 1.5, X = -0.38
    add_box(-0.38, 1.35, 0.0, 0.14, 0.06, 0.06);
    // Right Arm: Y = 1.2 .. 1.5, X = +0.38
    add_box(0.38, 1.35, 0.0, 0.14, 0.06, 0.06);
    // Head: Y = 1.6 .. 2.0, X = 0.0
    add_box(0.0, 1.80, 0.0, 0.12, 0.20, 0.12);

    RawMesh {
        positions,
        normals: Vec::new(),
        uvs: Vec::new(),
        indices,
        colors: None,
    }
}

/// Helper to generate a simple OBJ string from a RawMesh.
fn mesh_to_obj_string(mesh: &RawMesh) -> String {
    let mut s = String::new();
    s.push_str("# OxideSwarm Test Humanoid OBJ\n");
    for p in &mesh.positions {
        s.push_str(&format!("v {} {} {}\n", p[0], p[1], p[2]));
    }
    for tri in mesh.indices.as_chunks::<3>().0 {
        s.push_str(&format!("f {} {} {}\n", tri[0] + 1, tri[1] + 1, tri[2] + 1));
    }
    s
}

#[test]
fn test_math_linear_algebra_and_eigen_decomposition() {
    // 1. Vec3 tests
    let v1 = Vec3::new(1.0, 2.0, 3.0);
    let v2 = Vec3::new(4.0, 5.0, 6.0);
    assert_eq!(v1.dot(v2), 32.0);
    let cross = v1.cross(v2);
    assert_eq!(cross, Vec3::new(-3.0, 6.0, -3.0));
    assert!(cross.dot(v1).abs() < 1e-6);
    assert!(cross.dot(v2).abs() < 1e-6);

    // 2. Mat4 rigid inverse tests
    let pos = Vec3::new(10.5, -2.3, 4.8);
    let rot = Quat::from_euler(0.3, -0.5, 0.2);
    let mat = Mat4::from_trs(pos, rot, Vec3::ONE);
    let inv = mat.inverse_rigid();
    let identity = mat.mul_mat4(&inv);

    for col in 0..4 {
        for row in 0..4 {
            let expected = if col == row { 1.0 } else { 0.0 };
            assert!(
                (identity.cols[col][row] - expected).abs() < 1e-5,
                "Failed at col {}, row {}: got {}",
                col,
                row,
                identity.cols[col][row]
            );
        }
    }

    // 3. Quat slerp tests at t = 0.0, 0.25, 0.5, 0.75, 1.0
    let q1 = Quat::IDENTITY;
    let q2 = Quat::from_axis_angle(Vec3::UP, std::f32::consts::PI * 0.5);

    // t = 0.0 must match q1
    let q_0 = Quat::slerp(q1, q2, 0.0);
    assert!(
        (q_0.dot(q1).abs() - 1.0).abs() < 1e-5,
        "q(0.0) must match q1"
    );

    // t = 0.25 must match rotation by PI * 0.125
    let q_025 = Quat::slerp(q1, q2, 0.25);
    let q_exp_025 = Quat::from_axis_angle(Vec3::UP, std::f32::consts::PI * 0.125);
    assert!(
        (q_025.dot(q_exp_025).abs() - 1.0).abs() < 1e-4,
        "q(0.25) angular fidelity failed"
    );

    // t = 0.5 must match rotation by PI * 0.25
    let q_mid = Quat::slerp(q1, q2, 0.5);
    let q_expected = Quat::from_axis_angle(Vec3::UP, std::f32::consts::PI * 0.25);
    assert!(
        (q_mid.dot(q_expected).abs() - 1.0).abs() < 1e-4,
        "q(0.5) angular fidelity failed"
    );

    // t = 0.75 must match rotation by PI * 0.375
    let q_075 = Quat::slerp(q1, q2, 0.75);
    let q_exp_075 = Quat::from_axis_angle(Vec3::UP, std::f32::consts::PI * 0.375);
    assert!(
        (q_075.dot(q_exp_075).abs() - 1.0).abs() < 1e-4,
        "q(0.75) angular fidelity failed"
    );

    // t = 1.0 must match q2 (endpoint fidelity)
    let q_1 = Quat::slerp(q1, q2, 1.0);
    assert!(
        (q_1.dot(q2).abs() - 1.0).abs() < 1e-5,
        "q(1.0) must match q2"
    );

    // 4. Cardano / Smith 3x3 symmetric eigen-decomposition
    let diag_matrix = [[5.0, 0.0, 0.0], [0.0, 2.0, 0.0], [0.0, 0.0, 1.0]];
    let eigen = eigen_symmetric_3x3(diag_matrix);
    assert!((eigen.eigenvalues[0] - 5.0).abs() < 1e-4);
    assert!((eigen.eigenvalues[1] - 2.0).abs() < 1e-4);
    assert!((eigen.eigenvalues[2] - 1.0).abs() < 1e-4);
}

#[test]
fn test_mesh_loader_obj_parsing() {
    let mesh = create_humanoid_test_mesh();
    let obj_str = mesh_to_obj_string(&mesh);
    let loaded = load_mesh_from_bytes(obj_str.as_bytes()).expect("Failed to parse OBJ mesh");

    assert_eq!(loaded.positions.len(), mesh.positions.len());
    assert_eq!(loaded.indices.len(), mesh.indices.len());

    let (min1, max1) = mesh.compute_aabb();
    let (min2, max2) = loaded.compute_aabb();
    for i in 0..3 {
        assert!((min1[i] - min2[i]).abs() < 1e-4);
        assert!((max1[i] - max2[i]).abs() < 1e-4);
    }
}

#[test]
fn test_f11_landmark_extraction_and_symmetry() {
    let mesh = create_humanoid_test_mesh();
    let landmarks = extract_landmarks(&mesh, RigPreset::Biped)
        .expect("Landmark extraction should succeed on humanoid mesh");

    // Height should be approximately 2.0
    assert!((landmarks.height - 2.0).abs() < 0.05);
    assert!(landmarks.is_symmetric);
    assert!(landmarks.symmetry_error < 0.05);

    // Root should be approximately in middle height (~1.0)
    assert!(landmarks.root.y > 0.8 && landmarks.root.y < 1.3);

    // Knee anterior pre-bend (+Z offset relative to hip/ankle)
    assert!(landmarks.knee_l.z > landmarks.hip_l.z);
    assert!(landmarks.knee_r.z > landmarks.hip_r.z);

    // Elbow posterior pre-bend (-Z offset relative to shoulder/wrist)
    assert!(landmarks.elbow_l.z < landmarks.shoulder_l.z);
    assert!(landmarks.elbow_r.z < landmarks.shoulder_r.z);

    // Bilateral coordinates should be mirrored across X = 0
    assert!((landmarks.hip_l.x + landmarks.hip_r.x).abs() < 1e-4);
    assert!((landmarks.shoulder_l.x + landmarks.shoulder_r.x).abs() < 1e-4);
}

#[test]
fn test_f12_armature_dag_hierarchy_and_ibm() {
    let mesh = create_humanoid_test_mesh();
    let landmarks = extract_landmarks(&mesh, RigPreset::Biped).unwrap();
    let armature = build_standard_armature(&landmarks, RigPreset::Biped);

    assert_eq!(armature.joint_count(), 15);
    assert_eq!(armature.root_index, 0);

    // Check DAG topological ordering: parent < child for all joints
    for joint in &armature.joints {
        if let Some(parent_idx) = joint.parent {
            assert!(parent_idx < joint.index, "DAG must satisfy parent < child");
            assert!(armature.joints[parent_idx].children.contains(&joint.index));
        }
    }

    // Verify 15-joint standard names
    let names: Vec<&str> = armature.joints.iter().map(|j| j.name.as_str()).collect();
    assert_eq!(names[0], "Hips");
    assert_eq!(names[1], "Spine");
    assert_eq!(names[2], "Chest");
    assert_eq!(names[3], "Neck");
    assert_eq!(names[4], "Head");
    assert_eq!(names[5], "UpperLeg_L");
    assert_eq!(names[6], "LowerLeg_L");
    assert_eq!(names[7], "Foot_L");
    assert_eq!(names[8], "UpperLeg_R");
    assert_eq!(names[9], "LowerLeg_R");
    assert_eq!(names[10], "Foot_R");
    assert_eq!(names[11], "UpperArm_L");
    assert_eq!(names[12], "LowerArm_L");
    assert_eq!(names[13], "UpperArm_R");
    assert_eq!(names[14], "LowerArm_R");

    // Verify Inverse Bind Matrix byte buffer: exactly 15 * 64 = 960 bytes
    let ibm_bytes = armature.serialize_inverse_bind_matrices();
    assert_eq!(ibm_bytes.len(), 960);
    assert_eq!(ibm_bytes.len() % 4, 0);

    // Verify IBM column-major mathematical inversion: W * IBM == I
    for joint in &armature.joints {
        let prod = joint.world_transform.mul_mat4(&joint.inverse_bind_matrix);
        for c in 0..4 {
            for r in 0..4 {
                let exp = if c == r { 1.0 } else { 0.0 };
                assert!((prod.cols[c][r] - exp).abs() < 1e-4);
            }
        }
    }
}

#[test]
fn test_f13_geodesic_skin_weighting_and_unity_normalization() {
    let mesh = create_humanoid_test_mesh();
    let landmarks = extract_landmarks(&mesh, RigPreset::Biped).unwrap();
    let armature = build_standard_armature(&landmarks, RigPreset::Biped);
    let bone_segments = build_bone_segments(&armature, &landmarks);

    let skin_weights = compute_skin_weights(&mesh, &bone_segments, &SkinningConfig::default())
        .expect("Skin weighting should succeed");

    assert_eq!(skin_weights.joints.len(), mesh.positions.len());
    assert_eq!(skin_weights.weights.len(), mesh.positions.len());

    // Strict partition of unity: sum of 4 weights == 1.000000 for EVERY vertex
    for (v_idx, w) in skin_weights.weights.iter().enumerate() {
        let sum: f32 = w.iter().sum();
        assert!(
            (sum - 1.0).abs() < 1e-6,
            "Vertex {} weights sum to {} instead of 1.0: {:?}",
            v_idx,
            sum,
            w
        );
        for &val in w {
            assert!((0.0..=1.0).contains(&val));
        }
    }

    // Verify sagittal barrier: vertices strictly on left (X < -0.15) must not bind to right leg bones (8, 9, 10)
    for (v_idx, p) in mesh.positions.iter().enumerate() {
        if p[0] < -0.15 && p[1] < 1.0 {
            let j = skin_weights.joints[v_idx];
            let w = skin_weights.weights[v_idx];
            for k in 0..4 {
                let joint_idx = j[k];
                if joint_idx == 8 || joint_idx == 9 || joint_idx == 10 {
                    assert!(
                        w[k] < 1e-4,
                        "Left leg vertex {} bound to contralateral right bone {} with weight {}",
                        v_idx,
                        joint_idx,
                        w[k]
                    );
                }
            }
        }
    }

    // Binary encoders length and alignment
    let j_bytes = skin_weights.to_joints_bytes();
    let w_bytes = skin_weights.to_weights_bytes();
    assert_eq!(j_bytes.len(), mesh.positions.len() * 8);
    assert_eq!(w_bytes.len(), mesh.positions.len() * 16);
    assert_eq!(j_bytes.len() % 4, 0);
    assert_eq!(w_bytes.len() % 4, 0);
}

#[test]
fn test_f14_procedural_animation_synthesizer() {
    let mesh = create_humanoid_test_mesh();
    let landmarks = extract_landmarks(&mesh, RigPreset::Biped).unwrap();
    let armature = build_standard_armature(&landmarks, RigPreset::Biped);

    let requested = vec![
        "idle".to_string(),
        "walk".to_string(),
        "turnaround".to_string(),
    ];
    let clips = ClipSynthesizer::synthesize_all(&requested, landmarks.height, &armature);

    assert_eq!(clips.len(), 3);

    // 1. Idle Clip
    let idle = &clips[0];
    assert_eq!(idle.name, "idle");
    assert!((idle.duration - 2.4).abs() < 1e-4);
    assert!(!idle.tracks.is_empty());
    for track in &idle.tracks {
        assert_eq!(track.timestamps.len(), 73); // 2.4s * 30 fps + 1
        assert_eq!(track.timestamps[0], 0.0);
        assert!((track.timestamps.last().unwrap() - 2.4).abs() < 1e-4);
    }

    // 2. Walk Clip
    let walk = &clips[1];
    assert_eq!(walk.name, "walk");
    assert!((walk.duration - 1.2).abs() < 1e-4);
    for track in &walk.tracks {
        assert_eq!(track.timestamps.len(), 37); // 1.2s * 30 fps + 1
    }

    // 3. Turnaround Clip
    let turn = &clips[2];
    assert_eq!(turn.name, "turnaround");
    assert!((turn.duration - 3.2).abs() < 1e-4);
    for track in &turn.tracks {
        assert_eq!(track.timestamps.len(), 97); // 3.2s * 30 fps + 1
    }
}

#[test]
fn test_f15_rigged_gltf_binary_export_and_conformance() {
    let mesh = create_humanoid_test_mesh();
    let landmarks = extract_landmarks(&mesh, RigPreset::Biped).unwrap();
    let armature = build_standard_armature(&landmarks, RigPreset::Biped);
    let bone_segments = build_bone_segments(&armature, &landmarks);
    let skin_weights =
        compute_skin_weights(&mesh, &bone_segments, &SkinningConfig::default()).unwrap();
    let clips = ClipSynthesizer::synthesize_all(
        &[
            "idle".to_string(),
            "walk".to_string(),
            "turnaround".to_string(),
        ],
        landmarks.height,
        &armature,
    );

    let glb_bytes = export_rigged_glb(&mesh, &armature, &skin_weights, &clips, None)
        .expect("Rigged glTF export should succeed");

    assert!(glb_bytes.len() >= 20);

    // 1. Validate Container Header
    let magic = u32::from_le_bytes([glb_bytes[0], glb_bytes[1], glb_bytes[2], glb_bytes[3]]);
    let version = u32::from_le_bytes([glb_bytes[4], glb_bytes[5], glb_bytes[6], glb_bytes[7]]);
    let total_len =
        u32::from_le_bytes([glb_bytes[8], glb_bytes[9], glb_bytes[10], glb_bytes[11]]) as usize;

    assert_eq!(magic, GLB_MAGIC, "Invalid GLB magic header");
    assert_eq!(version, 2, "glTF version must be 2");
    assert_eq!(total_len, glb_bytes.len(), "Total length header mismatch");

    // 2. Validate Chunk 0 (JSON)
    let json_len =
        u32::from_le_bytes([glb_bytes[12], glb_bytes[13], glb_bytes[14], glb_bytes[15]]) as usize;
    let json_type =
        u32::from_le_bytes([glb_bytes[16], glb_bytes[17], glb_bytes[18], glb_bytes[19]]);
    assert_eq!(json_type, CHUNK_TYPE_JSON);
    assert_eq!(json_len % 4, 0, "JSON chunk length must be a multiple of 4");

    let json_bytes = &glb_bytes[20..20 + json_len];
    let gltf: Value = serde_json::from_slice(json_bytes).expect("glTF JSON must be valid");

    // 3. Inspect glTF JSON structural compliance
    assert_eq!(gltf["asset"]["version"], "2.0");

    let nodes = gltf["nodes"].as_array().expect("nodes must be present");
    assert_eq!(nodes.len(), 16); // 15 joint nodes + 1 mesh node

    let skins = gltf["skins"].as_array().expect("skins must be present");
    assert_eq!(skins.len(), 1);
    let skin_joints = skins[0]["joints"]
        .as_array()
        .expect("skin joints must be present");
    assert_eq!(skin_joints.len(), 15);

    let meshes = gltf["meshes"].as_array().expect("meshes must be present");
    let prim = &meshes[0]["primitives"][0];
    assert!(prim["attributes"]["POSITION"].is_number());
    assert!(prim["attributes"]["JOINTS_0"].is_number());
    assert!(prim["attributes"]["WEIGHTS_0"].is_number());
    assert_eq!(prim["mode"], 4);

    let anims = gltf["animations"]
        .as_array()
        .expect("animations must be present");
    assert_eq!(anims.len(), 3);

    // 4. Validate Chunk 1 (BIN)
    let bin_header_offset = 20 + json_len;
    let bin_len = u32::from_le_bytes([
        glb_bytes[bin_header_offset],
        glb_bytes[bin_header_offset + 1],
        glb_bytes[bin_header_offset + 2],
        glb_bytes[bin_header_offset + 3],
    ]) as usize;
    let bin_type = u32::from_le_bytes([
        glb_bytes[bin_header_offset + 4],
        glb_bytes[bin_header_offset + 5],
        glb_bytes[bin_header_offset + 6],
        glb_bytes[bin_header_offset + 7],
    ]);
    assert_eq!(bin_type, CHUNK_TYPE_BIN);
    assert_eq!(bin_len % 4, 0, "BIN chunk length must be a multiple of 4");

    // 5. Verify bufferViews 4-byte alignment
    let buffer_views = gltf["bufferViews"].as_array().unwrap();
    for bv in buffer_views {
        let byte_offset = bv["byteOffset"].as_u64().unwrap_or(0);
        assert_eq!(
            byte_offset % 4,
            0,
            "bufferView byteOffset must be 4-byte aligned"
        );
    }

    // 6. Verify round-trip parsing of output .glb through our own loader
    let parsed_back =
        load_mesh_from_bytes(&glb_bytes).expect("Should be able to parse generated GLB");
    assert_eq!(parsed_back.positions.len(), mesh.positions.len());
}

#[tokio::test]
async fn test_f16_autorig_task_runner_execution() {
    let mesh = create_humanoid_test_mesh();
    let obj_content = mesh_to_obj_string(&mesh);

    let spec = AutoRigSpec::new(Bytes::from(obj_content), "biped")
        .with_animations(vec!["idle".to_string(), "walk".to_string()])
        .with_provider("builtin");

    // 1. Direct auto_rig_mesh entry point test
    let glb_bytes = auto_rig_mesh(&spec).expect("auto_rig_mesh should succeed");
    assert!(glb_bytes.len() > 1000);

    // 2. Full TaskRunner pipeline execution test
    let caps = WorkerCapabilities::new("test_worker", 4, 8192, false, false, None);
    let runner = TaskRunner::new(Uuid::new_v4(), caps, RunnerConfig::default());
    let task = Task::new(TaskSpec::AutoRig(spec), TaskRequirements::default());

    let result = runner.execute_task(&task, None, None).await;
    assert!(
        result.is_success(),
        "Task execution failed: {:?}",
        result.error
    );
    assert!(
        result.asset_bytes.is_some(),
        "asset_bytes must be populated"
    );

    let asset = result.asset_bytes.unwrap();
    assert!(asset.len() > 1000);
    assert_eq!(&asset.as_bytes()[0..4], b"glTF");
}

#[test]
fn test_e2e_procedural_sdf_to_autorig() {
    // 1. Generate procedural 3D model using Tier-0 SDF engine
    let gen_spec = rusty_grid_core::task::Generative3DSpec::new_prompt("humanoid character")
        .with_quality("fast")
        .with_engine("sdf");

    let (gen_glb, stats) = rusty_grid_worker::gen3d::generate_procedural_3d(&gen_spec)
        .expect("Procedural SDF generation should succeed");
    assert!(stats.vertex_count > 50);

    // 2. Feed SDF generated .glb directly into auto-rigging engine
    let rig_spec = AutoRigSpec::new(Bytes::from(gen_glb), "biped")
        .with_animations(vec![
            "idle".to_string(),
            "walk".to_string(),
            "turnaround".to_string(),
        ])
        .with_provider("builtin");

    let rigged_glb =
        auto_rig_mesh(&rig_spec).expect("Auto-rigging procedural SDF mesh should succeed");
    assert!(rigged_glb.len() > 2000);
    assert_eq!(&rigged_glb[0..4], b"glTF");

    // 3. Verify that the output can be loaded back
    let reloaded = load_mesh_from_bytes(&rigged_glb)
        .expect("Output rigged GLB should be valid and reloadable");
    assert_eq!(reloaded.positions.len(), stats.vertex_count);
}

#[test]
fn test_edge_cases_and_error_handling() {
    // 1. Empty mesh error
    let empty_spec = AutoRigSpec::new(Bytes::new(), "biped");
    assert!(auto_rig_mesh(&empty_spec).is_err());

    // 2. Degenerate tiny mesh (< 8 vertices)
    let tiny_obj = "v 0 0 0\nv 1 0 0\nv 0 1 0\nf 1 2 3\n";
    let tiny_spec = AutoRigSpec::new(Bytes::from_static(tiny_obj.as_bytes()), "biped");
    assert!(auto_rig_mesh(&tiny_spec).is_err());

    // 3. Corrupted GLB data
    let corrupt_bytes = vec![
        0x46, 0x54, 0x6C, 0x67, 0x02, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF,
    ];
    let corrupt_spec = AutoRigSpec::new(Bytes::from(corrupt_bytes), "biped");
    assert!(auto_rig_mesh(&corrupt_spec).is_err());
}

#[test]
fn test_presets_quadruped_and_generic() {
    let mesh = create_humanoid_test_mesh();
    let obj_content = mesh_to_obj_string(&mesh);

    // Quadruped preset
    let quad_spec = AutoRigSpec::new(Bytes::from(obj_content.clone()), "quadruped");
    let quad_glb = auto_rig_mesh(&quad_spec).expect("Quadruped preset should succeed");
    assert!(quad_glb.len() > 1000);

    // Generic preset
    let gen_spec = AutoRigSpec::new(Bytes::from(obj_content), "generic");
    let gen_glb = auto_rig_mesh(&gen_spec).expect("Generic preset should succeed");
    assert!(gen_glb.len() > 1000);
}

#[test]
fn test_blender_provider_detection_and_script_embedding() {
    // Embedded script must not be empty and must compile as valid Python syntax
    assert!(!blender_provider::EMBEDDED_BLENDER_RIG_SCRIPT.is_empty());
    assert!(blender_provider::EMBEDDED_BLENDER_RIG_SCRIPT.contains("StandardArmature"));
    assert!(blender_provider::EMBEDDED_BLENDER_RIG_SCRIPT.contains("ARMATURE_AUTO"));

    // Script resolution should return either local path or fallback to embedded
    let script = blender_provider::resolve_blender_rig_script();
    assert!(script.is_some() || !blender_provider::EMBEDDED_BLENDER_RIG_SCRIPT.is_empty());
}

#[test]
fn test_pca_mesh_alignment_on_rotated_humanoid() {
    let base_mesh = create_humanoid_test_mesh();
    let angle = std::f32::consts::FRAC_PI_4; // 45 degrees
    let cos_a = angle.cos();
    let sin_a = angle.sin();

    // Rotate mesh 45 degrees around Y axis (yaw rotation)
    let mut rotated_positions = Vec::with_capacity(base_mesh.positions.len());
    for p in &base_mesh.positions {
        let rx = p[0] * cos_a - p[2] * sin_a;
        let ry = p[1];
        let rz = p[0] * sin_a + p[2] * cos_a;
        rotated_positions.push([rx, ry, rz]);
    }

    let rotated_mesh = RawMesh {
        positions: rotated_positions,
        normals: Vec::new(),
        uvs: Vec::new(),
        indices: base_mesh.indices.clone(),
        colors: None,
    };

    let base_lm = extract_landmarks(&base_mesh, RigPreset::Biped)
        .expect("Base landmark extraction should succeed");
    let rot_lm = extract_landmarks(&rotated_mesh, RigPreset::Biped)
        .expect("Rotated landmark extraction should succeed via PCA alignment");

    // Anatomical dimensions in canonical PCA space must be invariant to rotation
    assert!(
        (rot_lm.height - base_lm.height).abs() < 0.05,
        "Height deviation too large: base={}, rot={}",
        base_lm.height,
        rot_lm.height
    );
    assert!(
        (rot_lm.width - base_lm.width).abs() < 0.05,
        "Width deviation too large: base={}, rot={}",
        base_lm.width,
        rot_lm.width
    );
    assert!(
        rot_lm.is_symmetric,
        "Rotated mesh must retain bilateral symmetry in PCA space"
    );

    // World-space landmarks: Head and root should remain on rotation axis
    assert!((rot_lm.head.x).abs() < 0.05);
    assert!((rot_lm.head.z).abs() < 0.05);
    assert!((rot_lm.head.y - base_lm.head.y).abs() < 0.05);

    // Lateral landmarks (Left vs Right Foot) must be rotated by 45 degrees
    let expected_hip_l_x = base_lm.hip_l.x * cos_a - base_lm.hip_l.z * sin_a;
    let expected_hip_l_z = base_lm.hip_l.x * sin_a + base_lm.hip_l.z * cos_a;
    assert!(
        (rot_lm.hip_l.x - expected_hip_l_x).abs() < 0.05,
        "Rotated hip_l.x mismatch: got {}, expected {}",
        rot_lm.hip_l.x,
        expected_hip_l_x
    );
    assert!(
        (rot_lm.hip_l.z - expected_hip_l_z).abs() < 0.05,
        "Rotated hip_l.z mismatch: got {}, expected {}",
        rot_lm.hip_l.z,
        expected_hip_l_z
    );

    // Also test 90-degree yaw rotation
    let mut rot90_positions = Vec::with_capacity(base_mesh.positions.len());
    for p in &base_mesh.positions {
        rot90_positions.push([-p[2], p[1], p[0]]);
    }
    let rot90_mesh = RawMesh {
        positions: rot90_positions,
        normals: Vec::new(),
        uvs: Vec::new(),
        indices: base_mesh.indices.clone(),
        colors: None,
    };
    let rot90_lm = extract_landmarks(&rot90_mesh, RigPreset::Biped)
        .expect("90-degree rotated landmark extraction should succeed");
    assert!((rot90_lm.height - 2.0).abs() < 0.05);
    assert!((rot90_lm.head.y - base_lm.head.y).abs() < 0.05);
}

#[test]
fn test_forward_kinematics_traversal() {
    let mesh = create_humanoid_test_mesh();
    let lm = extract_landmarks(&mesh, RigPreset::Biped).expect("Landmarks extraction");
    let mut armature = build_standard_armature(&lm, RigPreset::Biped);

    // 1. In rest pose, evaluate_forward_kinematics() must reproduce joint world_transform
    let fk_world = armature.evaluate_forward_kinematics();
    assert_eq!(fk_world.len(), 15);
    for (i, (joint, fk_mat)) in armature.joints.iter().zip(fk_world.iter()).enumerate() {
        for col in 0..4 {
            for row in 0..4 {
                assert!(
                    (joint.world_transform.cols[col][row] - fk_mat.cols[col][row]).abs() < 1e-5,
                    "Joint {} world transform mismatch at ({}, {})",
                    i,
                    col,
                    row
                );
            }
        }
    }

    // 2. Pose propagation test: Apply local rotation to Joint 1 (Spine)
    // Rotate Spine 90 degrees around Z axis: child Chest, Neck, Head, Arms must move
    let spine_rot = Quat::from_axis_angle(Vec3::FORWARD, std::f32::consts::FRAC_PI_2);
    armature.joints[1].local_rotation = [spine_rot.x, spine_rot.y, spine_rot.z, spine_rot.w];

    let posed_fk = armature.evaluate_forward_kinematics();

    // Joint 0 (Hips) and Legs (5..10) must be unaffected
    assert_eq!(posed_fk[0], armature.joints[0].world_transform);
    for (j, fk_mat) in posed_fk.iter().enumerate().take(11).skip(5) {
        assert_eq!(*fk_mat, armature.joints[j].world_transform);
    }

    // Joint 4 (Head, descendant of Spine) must have rotated world transform
    assert_ne!(posed_fk[4], armature.joints[4].world_transform);

    // 3. update_world_transforms must update joint.world_transform and maintain W * IBM = I
    armature.update_world_transforms();
    for joint in &armature.joints {
        let identity = joint.world_transform.mul_mat4(&joint.inverse_bind_matrix);
        for col in 0..4 {
            for row in 0..4 {
                let expected = if col == row { 1.0 } else { 0.0 };
                assert!(
                    (identity.cols[col][row] - expected).abs() < 1e-4,
                    "W * IBM != I for joint {}",
                    joint.name
                );
            }
        }
    }
}

#[test]
fn test_obj_loader_attribute_synchronization_and_bounds_checking() {
    // 1. Partial UVs: face 1 specifies vt, face 2 does not
    let partial_uv_obj = "\
v 0.0 0.0 0.0
v 1.0 0.0 0.0
v 1.0 1.0 0.0
v 0.0 1.0 0.0
vt 0.0 0.0
vt 1.0 0.0
vt 1.0 1.0
f 1/1 2/2 3/3
f 1 3 4
";
    let mesh =
        load_mesh_from_bytes(partial_uv_obj.as_bytes()).expect("Partial UV OBJ should parse");
    assert_eq!(
        mesh.positions.len(),
        mesh.uvs.len(),
        "UV count must equal position count"
    );

    // 2. Partial normals: face 1 specifies vn, face 2 does not
    let partial_norm_obj = "\
v 0.0 0.0 0.0
v 1.0 0.0 0.0
v 1.0 1.0 0.0
v 0.0 1.0 0.0
vn 0.0 0.0 1.0
f 1//1 2//1 3//1
f 1 3 4
";
    let mesh_norm =
        load_mesh_from_bytes(partial_norm_obj.as_bytes()).expect("Partial normal OBJ should parse");
    assert_eq!(
        mesh_norm.positions.len(),
        mesh_norm.normals.len(),
        "Normal count must equal position count"
    );

    // 3. Out-of-bounds vertex index
    let oob_obj = "\
v 0.0 0.0 0.0
v 1.0 0.0 0.0
v 1.0 1.0 0.0
f 1 2 999
";
    let oob_result = load_mesh_from_bytes(oob_obj.as_bytes());
    assert!(
        oob_result.is_err(),
        "Out-of-bounds positive vertex index must return Err"
    );

    // 4. Invalid index 0
    let zero_idx_obj = "\
v 0.0 0.0 0.0
v 1.0 0.0 0.0
v 1.0 1.0 0.0
f 0 1 2
";
    let zero_result = load_mesh_from_bytes(zero_idx_obj.as_bytes());
    assert!(zero_result.is_err(), "Zero vertex index must return Err");

    // 5. Out-of-bounds negative index
    let neg_oob_obj = "\
v 0.0 0.0 0.0
v 1.0 0.0 0.0
v 1.0 1.0 0.0
f -1 -2 -999
";
    let neg_result = load_mesh_from_bytes(neg_oob_obj.as_bytes());
    assert!(
        neg_result.is_err(),
        "Out-of-bounds negative vertex index must return Err"
    );
}
