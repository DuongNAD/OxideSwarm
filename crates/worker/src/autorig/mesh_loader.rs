//! Mesh loader capable of parsing glTF 2.0 Binary (.glb) and Wavefront OBJ into `RawMesh`.

use crate::gen3d::types::RawMesh;
use serde_json::Value;

pub const GLB_MAGIC: u32 = 0x46546C67; // "glTF"
pub const CHUNK_TYPE_JSON: u32 = 0x4E4F534A; // "JSON"
pub const CHUNK_TYPE_BIN: u32 = 0x004E4942; // "BIN\0"

/// Loads and normalizes a 3D mesh from arbitrary input bytes (GLB or OBJ).
pub fn load_mesh_from_bytes(data: &[u8]) -> Result<RawMesh, String> {
    if data.len() < 12 {
        return Err("Input mesh data is too small to be valid GLB or OBJ".to_string());
    }

    // Check for glTF Binary magic (0x46546C67)
    let magic = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    if magic == GLB_MAGIC {
        load_glb_mesh(data)
    } else {
        // Assume text-based Wavefront OBJ
        load_obj_mesh(data)
    }
}

/// Parses a binary glTF 2.0 (.glb) container into a `RawMesh`.
fn load_glb_mesh(data: &[u8]) -> Result<RawMesh, String> {
    if data.len() < 20 {
        return Err("Truncated GLB container header".to_string());
    }

    let version = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
    if version != 2 {
        return Err(format!("Unsupported glTF version: {version}"));
    }

    let total_len = u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize;
    if data.len() < total_len {
        return Err("GLB byte length mismatch".to_string());
    }

    // Chunk 0: JSON
    let mut offset = 12;
    if offset + 8 > data.len() {
        return Err("Missing Chunk 0 in GLB".to_string());
    }
    let json_len = u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]) as usize;
    let json_type = u32::from_le_bytes([data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7]]);
    if json_type != CHUNK_TYPE_JSON {
        return Err("Expected JSON Chunk 0 in GLB".to_string());
    }
    offset += 8;

    if offset + json_len > data.len() {
        return Err("Truncated JSON chunk in GLB".to_string());
    }
    let json_slice = &data[offset..offset + json_len];
    offset += json_len;

    // Chunk 1: BIN
    let bin_slice = if offset + 8 <= data.len() {
        let bin_len = u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]) as usize;
        let bin_type = u32::from_le_bytes([data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7]]);
        if bin_type == CHUNK_TYPE_BIN {
            offset += 8;
            if offset + bin_len <= data.len() {
                &data[offset..offset + bin_len]
            } else {
                &data[offset..]
            }
        } else {
            &[]
        }
    } else {
        &[]
    };

    let gltf: Value = serde_json::from_slice(json_slice)
        .map_err(|e| format!("Failed to parse glTF JSON: {e}"))?;

    let meshes = gltf.get("meshes").and_then(|m| m.as_array())
        .ok_or("No meshes found in glTF JSON")?;
    if meshes.is_empty() {
        return Err("glTF meshes array is empty".to_string());
    }

    let primitives = meshes[0].get("primitives").and_then(|p| p.as_array())
        .ok_or("No primitives found in glTF mesh")?;
    if primitives.is_empty() {
        return Err("glTF primitives array is empty".to_string());
    }

    let prim = &primitives[0];
    let attributes = prim.get("attributes").and_then(|a| a.as_object())
        .ok_or("No attributes found in primitive")?;

    let pos_acc_idx = attributes.get("POSITION").and_then(|idx| idx.as_u64())
        .ok_or("Primitive missing POSITION attribute")? as usize;

    let accessors = gltf.get("accessors").and_then(|a| a.as_array())
        .ok_or("No accessors found in glTF")?;
    let buffer_views = gltf.get("bufferViews").and_then(|b| b.as_array())
        .ok_or("No bufferViews found in glTF")?;

    // Read positions
    let positions = read_vec3_accessor(pos_acc_idx, accessors, buffer_views, bin_slice)?;

    // Read optional normals
    let normals = if let Some(norm_acc_idx) = attributes.get("NORMAL").and_then(|idx| idx.as_u64()) {
        read_vec3_accessor(norm_acc_idx as usize, accessors, buffer_views, bin_slice).unwrap_or_default()
    } else {
        Vec::new()
    };

    // Read optional UVs
    let uvs = if let Some(uv_acc_idx) = attributes.get("TEXCOORD_0").and_then(|idx| idx.as_u64()) {
        read_vec2_accessor(uv_acc_idx as usize, accessors, buffer_views, bin_slice).unwrap_or_default()
    } else {
        Vec::new()
    };

    // Read indices
    let indices = if let Some(idx_acc_val) = prim.get("indices").and_then(|i| i.as_u64()) {
        read_indices_accessor(idx_acc_val as usize, accessors, buffer_views, bin_slice)?
    } else {
        (0..positions.len() as u32).collect()
    };

    Ok(RawMesh {
        positions,
        normals,
        uvs,
        indices,
        colors: None,
    })
}

fn read_vec3_accessor(
    acc_idx: usize,
    accessors: &[Value],
    buffer_views: &[Value],
    bin: &[u8],
) -> Result<Vec<[f32; 3]>, String> {
    let acc = accessors.get(acc_idx).ok_or("Invalid accessor index")?;
    let count = acc.get("count").and_then(|c| c.as_u64()).ok_or("Missing count")? as usize;
    let bv_idx = acc.get("bufferView").and_then(|b| b.as_u64()).ok_or("Missing bufferView")? as usize;
    let acc_byte_offset = acc.get("byteOffset").and_then(|o| o.as_u64()).unwrap_or(0) as usize;

    let bv = buffer_views.get(bv_idx).ok_or("Invalid bufferView index")?;
    let bv_byte_offset = bv.get("byteOffset").and_then(|o| o.as_u64()).unwrap_or(0) as usize;

    let total_offset = bv_byte_offset + acc_byte_offset;
    let byte_stride = bv.get("byteStride").and_then(|s| s.as_u64()).map(|s| s as usize).unwrap_or(12);

    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let start = total_offset + i * byte_stride;
        if start + 12 > bin.len() {
            return Err("Buffer overrun while reading Vec3 accessor".to_string());
        }
        let x = f32::from_le_bytes([bin[start], bin[start + 1], bin[start + 2], bin[start + 3]]);
        let y = f32::from_le_bytes([bin[start + 4], bin[start + 5], bin[start + 6], bin[start + 7]]);
        let z = f32::from_le_bytes([bin[start + 8], bin[start + 9], bin[start + 10], bin[start + 11]]);
        out.push([x, y, z]);
    }
    Ok(out)
}

fn read_vec2_accessor(
    acc_idx: usize,
    accessors: &[Value],
    buffer_views: &[Value],
    bin: &[u8],
) -> Result<Vec<[f32; 2]>, String> {
    let acc = accessors.get(acc_idx).ok_or("Invalid accessor index")?;
    let count = acc.get("count").and_then(|c| c.as_u64()).ok_or("Missing count")? as usize;
    let bv_idx = acc.get("bufferView").and_then(|b| b.as_u64()).ok_or("Missing bufferView")? as usize;
    let acc_byte_offset = acc.get("byteOffset").and_then(|o| o.as_u64()).unwrap_or(0) as usize;

    let bv = buffer_views.get(bv_idx).ok_or("Invalid bufferView index")?;
    let bv_byte_offset = bv.get("byteOffset").and_then(|o| o.as_u64()).unwrap_or(0) as usize;

    let total_offset = bv_byte_offset + acc_byte_offset;
    let byte_stride = bv.get("byteStride").and_then(|s| s.as_u64()).map(|s| s as usize).unwrap_or(8);

    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let start = total_offset + i * byte_stride;
        if start + 8 > bin.len() {
            return Err("Buffer overrun while reading Vec2 accessor".to_string());
        }
        let u = f32::from_le_bytes([bin[start], bin[start + 1], bin[start + 2], bin[start + 3]]);
        let v = f32::from_le_bytes([bin[start + 4], bin[start + 5], bin[start + 6], bin[start + 7]]);
        out.push([u, v]);
    }
    Ok(out)
}

fn read_indices_accessor(
    acc_idx: usize,
    accessors: &[Value],
    buffer_views: &[Value],
    bin: &[u8],
) -> Result<Vec<u32>, String> {
    let acc = accessors.get(acc_idx).ok_or("Invalid accessor index")?;
    let count = acc.get("count").and_then(|c| c.as_u64()).ok_or("Missing count")? as usize;
    let comp_type = acc.get("componentType").and_then(|t| t.as_u64()).unwrap_or(5123) as u32;
    let bv_idx = acc.get("bufferView").and_then(|b| b.as_u64()).ok_or("Missing bufferView")? as usize;
    let acc_byte_offset = acc.get("byteOffset").and_then(|o| o.as_u64()).unwrap_or(0) as usize;

    let bv = buffer_views.get(bv_idx).ok_or("Invalid bufferView index")?;
    let bv_byte_offset = bv.get("byteOffset").and_then(|o| o.as_u64()).unwrap_or(0) as usize;

    let total_offset = bv_byte_offset + acc_byte_offset;
    let mut out = Vec::with_capacity(count);

    if comp_type == 5123 {
        // UNSIGNED_SHORT (16-bit)
        for i in 0..count {
            let start = total_offset + i * 2;
            if start + 2 > bin.len() {
                return Err("Buffer overrun while reading u16 indices".to_string());
            }
            let idx = u16::from_le_bytes([bin[start], bin[start + 1]]) as u32;
            out.push(idx);
        }
    } else if comp_type == 5125 {
        // UNSIGNED_INT (32-bit)
        for i in 0..count {
            let start = total_offset + i * 4;
            if start + 4 > bin.len() {
                return Err("Buffer overrun while reading u32 indices".to_string());
            }
            let idx = u32::from_le_bytes([bin[start], bin[start + 1], bin[start + 2], bin[start + 3]]);
            out.push(idx);
        }
    } else {
        return Err(format!("Unsupported index component type: {comp_type}"));
    }

    Ok(out)
}

/// Parses a Wavefront OBJ text buffer into a `RawMesh`.
fn load_obj_mesh(data: &[u8]) -> Result<RawMesh, String> {
    let text = std::str::from_utf8(data)
        .map_err(|e| format!("OBJ data contains invalid UTF-8: {e}"))?;

    let mut raw_positions: Vec<[f32; 3]> = Vec::new();
    let mut raw_normals: Vec<[f32; 3]> = Vec::new();
    let mut raw_uvs: Vec<[f32; 2]> = Vec::new();
    let mut raw_faces: Vec<Vec<String>> = Vec::new();

    // Pass 1: Collect raw attributes and face lines
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let mut parts = line.split_whitespace();
        let tag = match parts.next() {
            Some(t) => t,
            None => continue,
        };

        match tag {
            "v" => {
                let x: f32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                let y: f32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                let z: f32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                raw_positions.push([x, y, z]);
            }
            "vn" => {
                let x: f32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                let y: f32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                let z: f32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                raw_normals.push([x, y, z]);
            }
            "vt" => {
                let u: f32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                let v: f32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                raw_uvs.push([u, v]);
            }
            "f" => {
                let face_tokens: Vec<String> = parts.map(|s| s.to_string()).collect();
                if face_tokens.len() >= 3 {
                    raw_faces.push(face_tokens);
                }
            }
            _ => {}
        }
    }

    if raw_positions.is_empty() {
        return Err("No vertices found in OBJ mesh".to_string());
    }

    let has_uvs = !raw_uvs.is_empty();
    let has_normals = !raw_normals.is_empty();

    let mut final_positions: Vec<[f32; 3]> = Vec::new();
    let mut final_normals: Vec<[f32; 3]> = Vec::new();
    let mut final_uvs: Vec<[f32; 2]> = Vec::new();
    let mut final_indices: Vec<u32> = Vec::new();

    let mut vert_map: std::collections::HashMap<(usize, Option<usize>, Option<usize>), u32> =
        std::collections::HashMap::new();

    // Pass 2: Triangulate faces and synchronize attributes
    for face_tokens in &raw_faces {
        let mut face_verts = Vec::with_capacity(face_tokens.len());
        for token in face_tokens {
            let mut subtokens = token.split('/');
            let v_idx: i32 = subtokens.next().and_then(|s| s.parse().ok()).unwrap_or(0);
            let vt_idx: Option<i32> = subtokens.next().and_then(|s| if s.is_empty() { None } else { s.parse().ok() });
            let vn_idx: Option<i32> = subtokens.next().and_then(|s| if s.is_empty() { None } else { s.parse().ok() });

            let pos_idx = resolve_obj_index(v_idx, raw_positions.len()).ok_or_else(|| {
                format!(
                    "OBJ face references vertex index {} out of range (total {})",
                    v_idx,
                    raw_positions.len()
                )
            })?;
            let uv_idx = vt_idx.and_then(|idx| resolve_obj_index(idx, raw_uvs.len()));
            let norm_idx = vn_idx.and_then(|idx| resolve_obj_index(idx, raw_normals.len()));

            face_verts.push((pos_idx, uv_idx, norm_idx));
        }

        for i in 1..face_verts.len() - 1 {
            let tri = [face_verts[0], face_verts[i], face_verts[i + 1]];
            for &vert_key in &tri {
                let idx = if let Some(&existing) = vert_map.get(&vert_key) {
                    existing
                } else {
                    let (p_i, uv_i, n_i) = vert_key;
                    let new_idx = final_positions.len() as u32;
                    final_positions.push(raw_positions[p_i]);

                    if has_uvs {
                        if let Some(uv_idx) = uv_i {
                            if uv_idx < raw_uvs.len() {
                                final_uvs.push(raw_uvs[uv_idx]);
                            } else {
                                final_uvs.push([0.0, 0.0]);
                            }
                        } else {
                            final_uvs.push([0.0, 0.0]);
                        }
                    }

                    if has_normals {
                        if let Some(norm_idx) = n_i {
                            if norm_idx < raw_normals.len() {
                                final_normals.push(raw_normals[norm_idx]);
                            } else {
                                final_normals.push([0.0, 1.0, 0.0]);
                            }
                        } else {
                            final_normals.push([0.0, 1.0, 0.0]);
                        }
                    }

                    vert_map.insert(vert_key, new_idx);
                    new_idx
                };
                final_indices.push(idx);
            }
        }
    }

    if final_positions.is_empty() {
        return Err("No valid faces found in OBJ mesh".to_string());
    }

    Ok(RawMesh {
        positions: final_positions,
        normals: final_normals,
        uvs: final_uvs,
        indices: final_indices,
        colors: None,
    })
}

#[inline(always)]
fn resolve_obj_index(idx: i32, len: usize) -> Option<usize> {
    if idx > 0 {
        let u = (idx as usize).checked_sub(1)?;
        if u < len {
            Some(u)
        } else {
            None
        }
    } else if idx < 0 {
        let offset = len as i64 + idx as i64;
        if offset >= 0 && (offset as usize) < len {
            Some(offset as usize)
        } else {
            None
        }
    } else {
        None
    }
}
