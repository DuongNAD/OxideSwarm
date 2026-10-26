//! Compliant Khronos glTF 2.0 Binary (.glb) packager (Feature F9).

use super::types::{BakedMaterial, RawMesh};
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

/// Packages a `RawMesh` and optional `BakedMaterial` into a compliant glTF 2.0 Binary (.glb) byte buffer.
pub fn pack_glb(mesh: &RawMesh, material: Option<&BakedMaterial>) -> Result<Vec<u8>, String> {
    if mesh.positions.is_empty() || mesh.indices.is_empty() {
        return Err("Cannot pack empty mesh into glTF".to_string());
    }

    let mut bin_data = Vec::new();
    let mut buffer_views = Vec::new();
    let mut accessors = Vec::new();

    let (min_pos, max_pos) = mesh.compute_aabb();

    // 1. POSITION BufferView & Accessor (0)
    let pos_offset = bin_data.len();
    let pos_len = mesh.positions.len() * 12;
    for p in &mesh.positions {
        bin_data.extend_from_slice(&p[0].to_le_bytes());
        bin_data.extend_from_slice(&p[1].to_le_bytes());
        bin_data.extend_from_slice(&p[2].to_le_bytes());
    }
    pad_to_4(&mut bin_data);
    buffer_views.push(json!({
        "buffer": 0,
        "byteOffset": pos_offset,
        "byteLength": pos_len,
        "target": TARGET_ARRAY_BUFFER
    }));
    accessors.push(json!({
        "bufferView": 0,
        "byteOffset": 0,
        "componentType": COMPONENT_TYPE_FLOAT,
        "count": mesh.positions.len(),
        "type": "VEC3",
        "min": min_pos,
        "max": max_pos
    }));

    // 2. NORMAL BufferView & Accessor (1)
    let has_normals = !mesh.normals.is_empty();
    let normal_accessor_idx = if has_normals {
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

    // 3. TEXCOORD_0 BufferView & Accessor (2)
    let has_uvs = !mesh.uvs.is_empty();
    let uv_accessor_idx = if has_uvs {
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

    // 4. INDICES BufferView & Accessor (3)
    let idx_offset = bin_data.len();
    let use_u16 = mesh.positions.len() <= 65535;
    let (idx_len, comp_type) = if use_u16 {
        for &idx in &mesh.indices {
            bin_data.extend_from_slice(&(idx as u16).to_le_bytes());
        }
        (mesh.indices.len() * 2, COMPONENT_TYPE_UNSIGNED_SHORT)
    } else {
        for &idx in &mesh.indices {
            bin_data.extend_from_slice(&idx.to_le_bytes());
        }
        (mesh.indices.len() * 4, COMPONENT_TYPE_UNSIGNED_INT)
    };
    pad_to_4(&mut bin_data);
    let idx_bv = buffer_views.len();
    buffer_views.push(json!({
        "buffer": 0,
        "byteOffset": idx_offset,
        "byteLength": idx_len,
        "target": TARGET_ELEMENT_ARRAY_BUFFER
    }));
    let idx_acc = accessors.len();
    accessors.push(json!({
        "bufferView": idx_bv,
        "byteOffset": 0,
        "componentType": comp_type,
        "count": mesh.indices.len(),
        "type": "SCALAR"
    }));

    // 5. Embedded Material & PNG Texture
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

            let img_view_idx = buffer_views.len();
            buffer_views.push(json!({
                "buffer": 0,
                "byteOffset": img_offset,
                "byteLength": img_len
            }));

            images.push(json!({
                "bufferView": img_view_idx,
                "mimeType": "image/png"
            }));

            samplers.push(json!({
                "magFilter": 9729, // LINEAR
                "minFilter": 9987, // LINEAR_MIPMAP_LINEAR
                "wrapS": 10497,    // REPEAT
                "wrapT": 10497     // REPEAT
            }));

            textures.push(json!({
                "sampler": 0,
                "source": 0
            }));

            materials.push(json!({
                "name": "PbrBaseMaterial",
                "pbrMetallicRoughness": {
                    "baseColorTexture": { "index": 0 },
                    "baseColorFactor": mat.base_color_factor,
                    "metallicFactor": mat.metallic_factor,
                    "roughnessFactor": mat.roughness_factor
                },
                "doubleSided": false
            }));
        }
    } else {
        // Fallback standard material if no texture provided
        materials.push(json!({
            "name": "DefaultMaterial",
            "pbrMetallicRoughness": {
                "baseColorFactor": [0.8, 0.8, 0.85, 1.0],
                "metallicFactor": 0.1,
                "roughnessFactor": 0.5
            },
            "doubleSided": false
        }));
    }

    // Assemble glTF JSON Object
    let mut attributes = json!({
        "POSITION": 0
    });
    if let Some(norm_idx) = normal_accessor_idx {
        attributes
            .as_object_mut()
            .unwrap()
            .insert("NORMAL".to_string(), json!(norm_idx));
    }
    if let Some(uv_idx) = uv_accessor_idx {
        attributes
            .as_object_mut()
            .unwrap()
            .insert("TEXCOORD_0".to_string(), json!(uv_idx));
    }

    let primitive = json!({
        "attributes": attributes,
        "indices": idx_acc,
        "material": 0,
        "mode": 4 // TRIANGLES
    });

    let mut gltf_json = json!({
        "asset": {
            "version": "2.0",
            "generator": "OxideSwarm Direct glTF 2.0 Binary Packager"
        },
        "scene": 0,
        "scenes": [{ "nodes": [0] }],
        "nodes": [{ "name": "GeneratedMeshNode", "mesh": 0 }],
        "meshes": [{
            "name": "GeneratedMesh",
            "primitives": [primitive]
        }],
        "accessors": accessors,
        "bufferViews": buffer_views,
        "materials": materials,
        "buffers": [{ "byteLength": bin_data.len() }]
    });

    if !textures.is_empty() {
        gltf_json
            .as_object_mut()
            .unwrap()
            .insert("textures".to_string(), json!(textures));
        gltf_json
            .as_object_mut()
            .unwrap()
            .insert("images".to_string(), json!(images));
        gltf_json
            .as_object_mut()
            .unwrap()
            .insert("samplers".to_string(), json!(samplers));
    }

    let mut json_bytes = serde_json::to_vec(&gltf_json)
        .map_err(|e| format!("Failed to serialize glTF JSON: {e}"))?;

    // Pad JSON Chunk with space characters (0x20) to a 4-byte boundary
    while json_bytes.len() % 4 != 0 {
        json_bytes.push(0x20);
    }

    // Calculate Total Container Length:
    // 12 (Header) + 8 (Chunk 0 Header) + json_len + 8 (Chunk 1 Header) + bin_len
    let total_len = 12 + 8 + json_bytes.len() + 8 + bin_data.len();

    let mut glb = Vec::with_capacity(total_len);

    // 1. Header (12 bytes)
    glb.extend_from_slice(&GLB_MAGIC.to_le_bytes());
    glb.extend_from_slice(&GLB_VERSION.to_le_bytes());
    glb.extend_from_slice(&(total_len as u32).to_le_bytes());

    // 2. Chunk 0: JSON
    glb.extend_from_slice(&(json_bytes.len() as u32).to_le_bytes());
    glb.extend_from_slice(&CHUNK_TYPE_JSON.to_le_bytes());
    glb.extend_from_slice(&json_bytes);

    // 3. Chunk 1: BIN
    glb.extend_from_slice(&(bin_data.len() as u32).to_le_bytes());
    glb.extend_from_slice(&CHUNK_TYPE_BIN.to_le_bytes());
    glb.extend_from_slice(&bin_data);

    Ok(glb)
}

#[inline(always)]
#[allow(unknown_lints, clippy::manual_is_multiple_of)]
fn pad_to_4(buf: &mut Vec<u8>) {
    while buf.len() % 4 != 0 {
        buf.push(0);
    }
}
