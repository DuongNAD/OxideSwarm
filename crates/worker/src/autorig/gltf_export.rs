//! Feature F15: Rigged glTF 2.0 Binary (.glb) Exporter.
//!
//! Packages skinned meshes, 15-joint armature hierarchy, Inverse Bind Matrices,
//! glTF vertex attributes (`JOINTS_0`, `WEIGHTS_0`), and procedural animation clips
//! into a strictly compliant Khronos glTF 2.0 binary (.glb) container.

use super::anim_synth::{AnimationTargetPath, ProceduralClip};
use super::types::{Armature, SkinWeights};
use crate::gen3d::types::{BakedMaterial, RawMesh};
use serde_json::json;

pub const GLB_MAGIC: u32 = 0x46546C67; // "glTF"
pub const GLB_VERSION: u32 = 2;
pub const CHUNK_TYPE_JSON: u32 = 0x4E4F534A; // "JSON"
pub const CHUNK_TYPE_BIN: u32 = 0x004E4942; // "BIN\0"

pub const TARGET_ARRAY_BUFFER: u32 = 34962;
pub const TARGET_ELEMENT_ARRAY_BUFFER: u32 = 34963;

pub const COMPONENT_TYPE_UNSIGNED_SHORT: u32 = 5123;
pub const COMPONENT_TYPE_UNSIGNED_INT: u32 = 5125;
pub const COMPONENT_TYPE_FLOAT: u32 = 5126;

/// Exports a skinned mesh, armature, and animation clips to glTF 2.0 Binary (.glb).
pub fn export_rigged_glb(
    mesh: &RawMesh,
    armature: &Armature,
    skin_weights: &SkinWeights,
    animations: &[ProceduralClip],
    material: Option<&BakedMaterial>,
) -> Result<Vec<u8>, String> {
    if mesh.positions.is_empty() || mesh.indices.is_empty() {
        return Err("Cannot export empty skinned mesh".to_string());
    }
    if skin_weights.joints.len() != mesh.positions.len() {
        return Err("Skin weights vertex count does not match mesh vertex count".to_string());
    }

    let mut bin_data = Vec::new();
    let mut buffer_views = Vec::new();
    let mut accessors = Vec::new();

    let (min_pos, max_pos) = mesh.compute_aabb();

    // 1. POSITION BufferView & Accessor
    let pos_offset = bin_data.len();
    let pos_len = mesh.positions.len() * 12;
    for p in &mesh.positions {
        bin_data.extend_from_slice(&p[0].to_le_bytes());
        bin_data.extend_from_slice(&p[1].to_le_bytes());
        bin_data.extend_from_slice(&p[2].to_le_bytes());
    }
    pad_to_4(&mut bin_data);
    let pos_bv_idx = buffer_views.len();
    buffer_views.push(json!({
        "buffer": 0,
        "byteOffset": pos_offset,
        "byteLength": pos_len,
        "target": TARGET_ARRAY_BUFFER
    }));
    let pos_acc_idx = accessors.len();
    accessors.push(json!({
        "bufferView": pos_bv_idx,
        "byteOffset": 0,
        "componentType": COMPONENT_TYPE_FLOAT,
        "count": mesh.positions.len(),
        "type": "VEC3",
        "min": min_pos,
        "max": max_pos
    }));

    // 2. NORMAL BufferView & Accessor (Optional)
    let normal_acc_idx = if !mesh.normals.is_empty() {
        let norm_offset = bin_data.len();
        let norm_len = mesh.normals.len() * 12;
        for n in &mesh.normals {
            bin_data.extend_from_slice(&n[0].to_le_bytes());
            bin_data.extend_from_slice(&n[1].to_le_bytes());
            bin_data.extend_from_slice(&n[2].to_le_bytes());
        }
        pad_to_4(&mut bin_data);
        let bv_idx = buffer_views.len();
        buffer_views.push(json!({
            "buffer": 0,
            "byteOffset": norm_offset,
            "byteLength": norm_len,
            "target": TARGET_ARRAY_BUFFER
        }));
        let acc_idx = accessors.len();
        accessors.push(json!({
            "bufferView": bv_idx,
            "byteOffset": 0,
            "componentType": COMPONENT_TYPE_FLOAT,
            "count": mesh.normals.len(),
            "type": "VEC3"
        }));
        Some(acc_idx)
    } else {
        None
    };

    // 3. TEXCOORD_0 BufferView & Accessor (Optional)
    let uv_acc_idx = if !mesh.uvs.is_empty() {
        let uv_offset = bin_data.len();
        let uv_len = mesh.uvs.len() * 8;
        for uv in &mesh.uvs {
            bin_data.extend_from_slice(&uv[0].to_le_bytes());
            bin_data.extend_from_slice(&uv[1].to_le_bytes());
        }
        pad_to_4(&mut bin_data);
        let bv_idx = buffer_views.len();
        buffer_views.push(json!({
            "buffer": 0,
            "byteOffset": uv_offset,
            "byteLength": uv_len,
            "target": TARGET_ARRAY_BUFFER
        }));
        let acc_idx = accessors.len();
        accessors.push(json!({
            "bufferView": bv_idx,
            "byteOffset": 0,
            "componentType": COMPONENT_TYPE_FLOAT,
            "count": mesh.uvs.len(),
            "type": "VEC2"
        }));
        Some(acc_idx)
    } else {
        None
    };

    // 4. JOINTS_0 BufferView & Accessor (UNSIGNED_SHORT, VEC4)
    let joints_bytes = skin_weights.to_joints_bytes();
    let joints_offset = bin_data.len();
    let joints_len = joints_bytes.len();
    bin_data.extend_from_slice(&joints_bytes);
    pad_to_4(&mut bin_data);
    let joints_bv_idx = buffer_views.len();
    buffer_views.push(json!({
        "buffer": 0,
        "byteOffset": joints_offset,
        "byteLength": joints_len,
        "target": TARGET_ARRAY_BUFFER
    }));
    let (joints_min, joints_max) = skin_weights.compute_joints_bounds();
    let joints_acc_idx = accessors.len();
    accessors.push(json!({
        "bufferView": joints_bv_idx,
        "byteOffset": 0,
        "componentType": COMPONENT_TYPE_UNSIGNED_SHORT,
        "count": skin_weights.joints.len(),
        "type": "VEC4",
        "min": joints_min,
        "max": joints_max
    }));

    // 5. WEIGHTS_0 BufferView & Accessor (FLOAT, VEC4)
    let weights_bytes = skin_weights.to_weights_bytes();
    let weights_offset = bin_data.len();
    let weights_len = weights_bytes.len();
    bin_data.extend_from_slice(&weights_bytes);
    pad_to_4(&mut bin_data);
    let weights_bv_idx = buffer_views.len();
    buffer_views.push(json!({
        "buffer": 0,
        "byteOffset": weights_offset,
        "byteLength": weights_len,
        "target": TARGET_ARRAY_BUFFER
    }));
    let (weights_min, weights_max) = skin_weights.compute_weights_bounds();
    let weights_acc_idx = accessors.len();
    accessors.push(json!({
        "bufferView": weights_bv_idx,
        "byteOffset": 0,
        "componentType": COMPONENT_TYPE_FLOAT,
        "count": skin_weights.weights.len(),
        "type": "VEC4",
        "min": weights_min,
        "max": weights_max
    }));

    // 6. INDICES BufferView & Accessor
    let idx_offset = bin_data.len();
    let max_v = mesh.positions.len() as u32;
    let (idx_component_type, idx_len) = if max_v <= 65535 {
        for &idx in &mesh.indices {
            bin_data.extend_from_slice(&(idx as u16).to_le_bytes());
        }
        (COMPONENT_TYPE_UNSIGNED_SHORT, mesh.indices.len() * 2)
    } else {
        for &idx in &mesh.indices {
            bin_data.extend_from_slice(&idx.to_le_bytes());
        }
        (COMPONENT_TYPE_UNSIGNED_INT, mesh.indices.len() * 4)
    };
    pad_to_4(&mut bin_data);
    let idx_bv_idx = buffer_views.len();
    buffer_views.push(json!({
        "buffer": 0,
        "byteOffset": idx_offset,
        "byteLength": idx_len,
        "target": TARGET_ELEMENT_ARRAY_BUFFER
    }));
    let idx_acc = accessors.len();
    accessors.push(json!({
        "bufferView": idx_bv_idx,
        "byteOffset": 0,
        "componentType": idx_component_type,
        "count": mesh.indices.len(),
        "type": "SCALAR"
    }));

    // 7. INVERSE BIND MATRICES BufferView & Accessor (MAT4, FLOAT, no target)
    let ibm_bytes = armature.serialize_inverse_bind_matrices();
    let ibm_offset = bin_data.len();
    let ibm_len = ibm_bytes.len();
    bin_data.extend_from_slice(&ibm_bytes);
    pad_to_4(&mut bin_data);
    let ibm_bv_idx = buffer_views.len();
    buffer_views.push(json!({
        "buffer": 0,
        "byteOffset": ibm_offset,
        "byteLength": ibm_len
    }));
    let ibm_acc_idx = accessors.len();
    accessors.push(json!({
        "bufferView": ibm_bv_idx,
        "byteOffset": 0,
        "componentType": COMPONENT_TYPE_FLOAT,
        "count": armature.joints.len(),
        "type": "MAT4"
    }));

    // 8. ANIMATIONS Samplers & Channels
    let mut gltf_animations = Vec::new();
    for clip in animations {
        let mut samplers = Vec::new();
        let mut channels = Vec::new();

        for track in &clip.tracks {
            // A. Timestamp Input Accessor (SCALAR, FLOAT)
            let time_offset = bin_data.len();
            let time_len = track.timestamps.len() * 4;
            for &t in &track.timestamps {
                bin_data.extend_from_slice(&t.to_le_bytes());
            }
            pad_to_4(&mut bin_data);
            let time_bv_idx = buffer_views.len();
            buffer_views.push(json!({
                "buffer": 0,
                "byteOffset": time_offset,
                "byteLength": time_len
            }));
            let min_t = track.timestamps.first().copied().unwrap_or(0.0);
            let max_t = track.timestamps.last().copied().unwrap_or(0.0);
            let time_acc_idx = accessors.len();
            accessors.push(json!({
                "bufferView": time_bv_idx,
                "byteOffset": 0,
                "componentType": COMPONENT_TYPE_FLOAT,
                "count": track.timestamps.len(),
                "type": "SCALAR",
                "min": [min_t],
                "max": [max_t]
            }));

            // B. Values Output Accessor
            let (vec_type, comp_count) = match track.path {
                AnimationTargetPath::Translation | AnimationTargetPath::Scale => ("VEC3", 3),
                AnimationTargetPath::Rotation => ("VEC4", 4),
            };
            let val_offset = bin_data.len();
            let val_len = track.values.len() * 4;
            for &v in &track.values {
                bin_data.extend_from_slice(&v.to_le_bytes());
            }
            pad_to_4(&mut bin_data);
            let val_bv_idx = buffer_views.len();
            buffer_views.push(json!({
                "buffer": 0,
                "byteOffset": val_offset,
                "byteLength": val_len
            }));
            let val_acc_idx = accessors.len();
            accessors.push(json!({
                "bufferView": val_bv_idx,
                "byteOffset": 0,
                "componentType": COMPONENT_TYPE_FLOAT,
                "count": track.values.len() / comp_count,
                "type": vec_type
            }));

            // Sampler & Channel
            let sampler_idx = samplers.len();
            samplers.push(json!({
                "input": time_acc_idx,
                "interpolation": "LINEAR",
                "output": val_acc_idx
            }));

            channels.push(json!({
                "sampler": sampler_idx,
                "target": {
                    "node": track.target_joint, // Joint nodes are nodes 0..M-1
                    "path": track.path.as_str()
                }
            }));
        }

        gltf_animations.push(json!({
            "name": clip.name,
            "samplers": samplers,
            "channels": channels
        }));
    }

    // 9. Materials & Textures
    let mut materials = Vec::new();
    let mut textures = Vec::new();
    let mut images = Vec::new();
    let mut samplers = Vec::new();

    if let Some(mat) = material {
        if !mat.base_color_png.is_empty() {
            let img_offset = bin_data.len();
            let img_len = mat.base_color_png.len();
            bin_data.extend_from_slice(&mat.base_color_png);
            pad_to_4(&mut bin_data);

            let img_bv_idx = buffer_views.len();
            buffer_views.push(json!({
                "buffer": 0,
                "byteOffset": img_offset,
                "byteLength": img_len
            }));

            images.push(json!({
                "bufferView": img_bv_idx,
                "mimeType": "image/png"
            }));

            samplers.push(json!({
                "magFilter": 9729, // LINEAR
                "minFilter": 9987, // LINEAR_MIPMAP_LINEAR
                "wrapS": 10497,    // REPEAT
                "wrapT": 10497
            }));

            textures.push(json!({
                "sampler": 0,
                "source": 0
            }));

            materials.push(json!({
                "name": "CharacterMaterial",
                "pbrMetallicRoughness": {
                    "baseColorTexture": { "index": 0 },
                    "baseColorFactor": mat.base_color_factor,
                    "metallicFactor": mat.metallic_factor,
                    "roughnessFactor": mat.roughness_factor
                },
                "doubleSided": false
            }));
        }
    }

    if materials.is_empty() {
        materials.push(json!({
            "name": "DefaultCharacterMaterial",
            "pbrMetallicRoughness": {
                "baseColorFactor": [0.85, 0.85, 0.90, 1.0],
                "metallicFactor": 0.1,
                "roughnessFactor": 0.5
            },
            "doubleSided": false
        }));
    }

    // 10. Node Hierarchy Construction
    // Nodes 0..M-1: Joint Nodes
    let mut nodes = Vec::with_capacity(armature.joints.len() + 1);
    for joint in &armature.joints {
        let mut node = json!({
            "name": joint.name,
            "translation": joint.local_translation.to_array(),
            "rotation": joint.local_rotation,
            "scale": joint.local_scale
        });
        if !joint.children.is_empty() {
            node.as_object_mut().unwrap().insert("children".to_string(), json!(joint.children));
        }
        nodes.push(node);
    }

    // Node M: Skinned Mesh Node
    let mesh_node_idx = nodes.len();
    nodes.push(json!({
        "name": "CharacterMeshNode",
        "mesh": 0,
        "skin": 0
    }));

    // 11. Primitive Attributes
    let mut attributes = json!({
        "POSITION": pos_acc_idx,
        "JOINTS_0": joints_acc_idx,
        "WEIGHTS_0": weights_acc_idx
    });
    if let Some(norm_idx) = normal_acc_idx {
        attributes.as_object_mut().unwrap().insert("NORMAL".to_string(), json!(norm_idx));
    }
    if let Some(uv_idx) = uv_acc_idx {
        attributes.as_object_mut().unwrap().insert("TEXCOORD_0".to_string(), json!(uv_idx));
    }

    let primitive = json!({
        "attributes": attributes,
        "indices": idx_acc,
        "material": 0,
        "mode": 4 // TRIANGLES
    });

    // 12. Skin Definition
    let joint_indices: Vec<usize> = (0..armature.joints.len()).collect();
    let skins = vec![json!({
        "name": "StandardArmatureSkin",
        "inverseBindMatrices": ibm_acc_idx,
        "joints": joint_indices,
        "skeleton": 0
    })];

    // 13. Assemble glTF JSON Object
    let mut gltf_json = json!({
        "asset": {
            "version": "2.0",
            "generator": "OxideSwarm Rigged glTF 2.0 Binary Packager"
        },
        "scene": 0,
        "scenes": [{ "nodes": [0, mesh_node_idx] }],
        "nodes": nodes,
        "skins": skins,
        "meshes": [{
            "name": "CharacterMesh",
            "primitives": [primitive]
        }],
        "accessors": accessors,
        "bufferViews": buffer_views,
        "materials": materials,
        "buffers": [{ "byteLength": bin_data.len() }]
    });

    if !gltf_animations.is_empty() {
        gltf_json.as_object_mut().unwrap().insert("animations".to_string(), json!(gltf_animations));
    }
    if !textures.is_empty() {
        gltf_json.as_object_mut().unwrap().insert("textures".to_string(), json!(textures));
        gltf_json.as_object_mut().unwrap().insert("images".to_string(), json!(images));
        gltf_json.as_object_mut().unwrap().insert("samplers".to_string(), json!(samplers));
    }

    let mut json_bytes = serde_json::to_vec(&gltf_json)
        .map_err(|e| format!("Failed to serialize glTF JSON: {e}"))?;

    // Pad JSON Chunk with space characters (0x20) to 4-byte boundary
    while json_bytes.len() % 4 != 0 {
        json_bytes.push(0x20);
    }

    // Pad BIN Chunk with null bytes (0x00) to 4-byte boundary
    pad_to_4(&mut bin_data);

    // Calculate Total Container Length:
    // 12 (Header) + 8 (Chunk 0 Header) + json_len + 8 (Chunk 1 Header) + bin_len
    let total_len = 12 + 8 + json_bytes.len() + 8 + bin_data.len();

    let mut glb = Vec::with_capacity(total_len);

    // Header (12 bytes)
    glb.extend_from_slice(&GLB_MAGIC.to_le_bytes());
    glb.extend_from_slice(&GLB_VERSION.to_le_bytes());
    glb.extend_from_slice(&(total_len as u32).to_le_bytes());

    // Chunk 0: JSON
    glb.extend_from_slice(&(json_bytes.len() as u32).to_le_bytes());
    glb.extend_from_slice(&CHUNK_TYPE_JSON.to_le_bytes());
    glb.extend_from_slice(&json_bytes);

    // Chunk 1: BIN
    glb.extend_from_slice(&(bin_data.len() as u32).to_le_bytes());
    glb.extend_from_slice(&CHUNK_TYPE_BIN.to_le_bytes());
    glb.extend_from_slice(&bin_data);

    Ok(glb)
}

#[inline(always)]
fn pad_to_4(buf: &mut Vec<u8>) {
    while buf.len() % 4 != 0 {
        buf.push(0);
    }
}
