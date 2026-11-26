//! Mesh loader capable of parsing glTF 2.0 Binary (.glb) and Wavefront OBJ into `RawMesh`.

use crate::gen3d::types::RawMesh;
use serde_json::Value;

pub const GLB_MAGIC: u32 = 0x46546C67; // "glTF"
pub const CHUNK_TYPE_JSON: u32 = 0x4E4F534A; // "JSON"
pub const CHUNK_TYPE_BIN: u32 = 0x004E4942; // "BIN\0"

/// Detected 3D mesh container format from magic bytes and content sniffing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeshFormat {
    Glb,
    Gltf,
    Obj,
}

impl MeshFormat {
    /// File extension corresponding to this mesh format.
    pub fn extension(&self) -> &'static str {
        match self {
            Self::Glb => "glb",
            Self::Gltf => "gltf",
            Self::Obj => "obj",
        }
    }
}

/// Strips any leading UTF-8 BOMs (`0xEF 0xBB 0xBF`) and ASCII whitespace repeatedly.
pub(crate) fn strip_leading_boms_and_ws(mut data: &[u8]) -> &[u8] {
    loop {
        let trimmed = data.trim_ascii_start();
        if let Some(rest) = trimmed.strip_prefix(&[0xEF, 0xBB, 0xBF]) {
            data = rest;
        } else {
            return trimmed;
        }
    }
}

/// Detects the mesh format from raw bytes without trusting file extensions.
pub fn detect_mesh_format(data: &[u8]) -> Result<MeshFormat, String> {
    if data.is_empty() {
        return Err("Input mesh data is empty".to_string());
    }

    if data.starts_with(b"glTF") {
        if data.len() < 12 {
            return Err("Truncated or invalid GLB container".to_string());
        }
        let version = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        if version != 2 {
            return Err(format!("Unsupported glTF version: {version}"));
        }
        let total_len = u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize;
        if total_len < 12 || data.len() < total_len {
            return Err("Truncated or invalid GLB container".to_string());
        }
        return Ok(MeshFormat::Glb);
    }

    // Strip optional UTF-8 BOM(s) and leading ASCII whitespace before sniffing `.gltf` or `.obj`.
    let trimmed = strip_leading_boms_and_ws(data);

    if trimmed.starts_with(b"{") {
        let json: Value =
            serde_json::from_slice(trimmed).map_err(|e| format!("Invalid glTF JSON input: {e}"))?;
        let obj = json
            .as_object()
            .ok_or_else(|| "Invalid glTF JSON: root must be an object".to_string())?;
        if !obj.contains_key("asset") && !obj.contains_key("meshes") {
            return Err(
                "Invalid or unsupported glTF JSON: missing 'asset' or 'meshes'".to_string(),
            );
        }
        return Ok(MeshFormat::Gltf);
    }

    // Otherwise validate ASCII Wavefront OBJ structure.
    let text = std::str::from_utf8(trimmed).map_err(|_| {
        "Unsupported or corrupt input mesh format (expected .glb, .gltf, or .obj)".to_string()
    })?;
    if trimmed
        .iter()
        .any(|&b| (b < 0x20 && b != b'\n' && b != b'\r' && b != b'\t') || b == 0x7F)
    {
        return Err(
            "Unsupported or corrupt input mesh format (unexpected binary control bytes)"
                .to_string(),
        );
    }

    let mut has_vertex = false;
    let mut has_face = false;
    for line in text.lines() {
        let line = line.trim_matches(|c: char| c.is_whitespace() || c == '\u{FEFF}');
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        match parts.next() {
            Some("v") => {
                let coords: Vec<f32> = parts
                    .take(3)
                    .filter_map(|s| s.parse::<f32>().ok().filter(|v| v.is_finite()))
                    .collect();
                if coords.len() == 3 {
                    has_vertex = true;
                }
            }
            Some("f") if parts.count() >= 3 => {
                has_face = true;
            }
            _ => {}
        }
        if has_vertex && has_face {
            return Ok(MeshFormat::Obj);
        }
    }

    Err(
        "Unsupported or corrupt input mesh format (expected binary .glb, JSON .gltf, or valid ASCII .obj)"
            .to_string(),
    )
}

/// Loads and normalizes a 3D mesh from arbitrary input bytes (GLB or OBJ).
pub fn load_mesh_from_bytes(data: &[u8]) -> Result<RawMesh, String> {
    load_mesh_from_bytes_cancellable(data, &|| false)
}

/// Loads and normalizes a 3D mesh from raw bytes with cooperative cancellation checks.
pub fn load_mesh_from_bytes_cancellable(
    data: &[u8],
    should_stop: &dyn Fn() -> bool,
) -> Result<RawMesh, String> {
    if should_stop() {
        return Err("Cancelled".to_string());
    }
    match detect_mesh_format(data)? {
        MeshFormat::Glb => load_glb_mesh_cancellable(data, should_stop),
        MeshFormat::Gltf => Err(
            "Text .gltf JSON format is not supported by the built-in pure-Rust loader; convert to .glb/.obj or use preferred_provider='blender'"
                .to_string(),
        ),
        MeshFormat::Obj => load_obj_mesh_cancellable(data, should_stop),
    }
}

/// Parses a binary glTF 2.0 (.glb) container into a `RawMesh`.
fn load_glb_mesh_cancellable(
    data: &[u8],
    should_stop: &dyn Fn() -> bool,
) -> Result<RawMesh, String> {
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
    let json_len = u32::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ]) as usize;
    let json_type = u32::from_le_bytes([
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ]);
    if json_type != CHUNK_TYPE_JSON {
        return Err("Expected JSON Chunk 0 in GLB".to_string());
    }
    offset += 8;

    if offset
        .checked_add(json_len)
        .is_none_or(|end| end > data.len())
    {
        return Err("Truncated JSON chunk in GLB".to_string());
    }
    let json_slice = &data[offset..offset + json_len];
    offset += json_len;

    // Chunk 1: BIN
    let bin_slice = if offset.checked_add(8).is_some_and(|end| end <= data.len()) {
        let bin_len = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        let bin_type = u32::from_le_bytes([
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]);
        if bin_type == CHUNK_TYPE_BIN {
            offset += 8;
            if let Some(bin_end) = offset.checked_add(bin_len) {
                if bin_end <= data.len() {
                    &data[offset..bin_end]
                } else {
                    &data[offset..]
                }
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

    let meshes = gltf
        .get("meshes")
        .and_then(|m| m.as_array())
        .ok_or("No meshes found in glTF JSON")?;
    if meshes.is_empty() {
        return Err("glTF meshes array is empty".to_string());
    }

    let primitives = meshes[0]
        .get("primitives")
        .and_then(|p| p.as_array())
        .ok_or("No primitives found in glTF mesh")?;
    if primitives.is_empty() {
        return Err("glTF primitives array is empty".to_string());
    }

    let prim = &primitives[0];
    let attributes = prim
        .get("attributes")
        .and_then(|a| a.as_object())
        .ok_or("No attributes found in primitive")?;

    let pos_acc_idx = attributes
        .get("POSITION")
        .and_then(|idx| idx.as_u64())
        .and_then(|idx| usize::try_from(idx).ok())
        .ok_or("Primitive missing POSITION attribute")?;

    let accessors = gltf
        .get("accessors")
        .and_then(|a| a.as_array())
        .ok_or("No accessors found in glTF")?;
    let buffer_views = gltf
        .get("bufferViews")
        .and_then(|b| b.as_array())
        .ok_or("No bufferViews found in glTF")?;

    // Read positions
    let positions =
        read_vec3_accessor(pos_acc_idx, accessors, buffer_views, bin_slice, should_stop)?;

    // Read optional normals
    let normals = if let Some(norm_acc_idx) = attributes
        .get("NORMAL")
        .and_then(|idx| idx.as_u64())
        .and_then(|idx| usize::try_from(idx).ok())
    {
        read_vec3_accessor(
            norm_acc_idx,
            accessors,
            buffer_views,
            bin_slice,
            should_stop,
        )
        .unwrap_or_default()
    } else {
        Vec::new()
    };

    // Read optional UVs
    let uvs = if let Some(uv_acc_idx) = attributes
        .get("TEXCOORD_0")
        .and_then(|idx| idx.as_u64())
        .and_then(|idx| usize::try_from(idx).ok())
    {
        read_vec2_accessor(uv_acc_idx, accessors, buffer_views, bin_slice, should_stop)
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    // Read indices
    let indices = if let Some(idx_acc_val) = prim.get("indices").and_then(|i| i.as_u64()) {
        let idx_acc = usize::try_from(idx_acc_val).map_err(|_| "Invalid indices accessor index")?;
        read_indices_accessor(idx_acc, accessors, buffer_views, bin_slice, should_stop)?
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

fn validate_accessor_range(
    total_offset: usize,
    count: usize,
    byte_stride: usize,
    elem_size: usize,
    bin_len: usize,
    label: &str,
) -> Result<(), String> {
    if byte_stride < elem_size {
        return Err(format!(
            "Invalid bufferView byteStride ({byte_stride} < {elem_size}) while reading {label}"
        ));
    }
    if count == 0 {
        if total_offset > bin_len {
            return Err(format!("Buffer overrun while reading {label}"));
        }
        return Ok(());
    }
    let last_offset = (count - 1)
        .checked_mul(byte_stride)
        .and_then(|span| total_offset.checked_add(span))
        .and_then(|end| end.checked_add(elem_size))
        .ok_or_else(|| format!("Buffer offset overflow while reading {label}"))?;
    if last_offset > bin_len {
        return Err(format!("Buffer overrun while reading {label}"));
    }
    Ok(())
}

fn json_u64_to_usize(val: Option<&Value>, default: usize, label: &str) -> Result<usize, String> {
    match val {
        None => Ok(default),
        Some(v) => {
            let raw = v.as_u64().ok_or_else(|| format!("Invalid {label}"))?;
            usize::try_from(raw).map_err(|_| format!("{label} exceeds usize range"))
        }
    }
}

fn read_vec3_accessor(
    acc_idx: usize,
    accessors: &[Value],
    buffer_views: &[Value],
    bin: &[u8],
    should_stop: &dyn Fn() -> bool,
) -> Result<Vec<[f32; 3]>, String> {
    let acc = accessors.get(acc_idx).ok_or("Invalid accessor index")?;
    let count_raw = acc
        .get("count")
        .and_then(|c| c.as_u64())
        .ok_or("Missing count")?;
    let count = usize::try_from(count_raw).map_err(|_| "Accessor count exceeds usize range")?;
    let bv_raw = acc
        .get("bufferView")
        .and_then(|b| b.as_u64())
        .ok_or("Missing bufferView")?;
    let bv_idx = usize::try_from(bv_raw).map_err(|_| "bufferView index exceeds usize range")?;
    let acc_byte_offset = json_u64_to_usize(acc.get("byteOffset"), 0, "accessor.byteOffset")?;

    let bv = buffer_views.get(bv_idx).ok_or("Invalid bufferView index")?;
    let bv_byte_offset = json_u64_to_usize(bv.get("byteOffset"), 0, "bufferView.byteOffset")?;

    let total_offset = bv_byte_offset
        .checked_add(acc_byte_offset)
        .ok_or("Buffer offset overflow while reading Vec3 accessor")?;
    let byte_stride = json_u64_to_usize(bv.get("byteStride"), 12, "bufferView.byteStride")?;

    validate_accessor_range(
        total_offset,
        count,
        byte_stride,
        12,
        bin.len(),
        "Vec3 accessor",
    )?;

    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        if i % 8192 == 0 && should_stop() {
            return Err("Cancelled".to_string());
        }
        let start = total_offset + i * byte_stride;
        let x = f32::from_le_bytes([bin[start], bin[start + 1], bin[start + 2], bin[start + 3]]);
        let y = f32::from_le_bytes([
            bin[start + 4],
            bin[start + 5],
            bin[start + 6],
            bin[start + 7],
        ]);
        let z = f32::from_le_bytes([
            bin[start + 8],
            bin[start + 9],
            bin[start + 10],
            bin[start + 11],
        ]);
        out.push([x, y, z]);
    }
    Ok(out)
}

fn read_vec2_accessor(
    acc_idx: usize,
    accessors: &[Value],
    buffer_views: &[Value],
    bin: &[u8],
    should_stop: &dyn Fn() -> bool,
) -> Result<Vec<[f32; 2]>, String> {
    let acc = accessors.get(acc_idx).ok_or("Invalid accessor index")?;
    let count_raw = acc
        .get("count")
        .and_then(|c| c.as_u64())
        .ok_or("Missing count")?;
    let count = usize::try_from(count_raw).map_err(|_| "Accessor count exceeds usize range")?;
    let bv_raw = acc
        .get("bufferView")
        .and_then(|b| b.as_u64())
        .ok_or("Missing bufferView")?;
    let bv_idx = usize::try_from(bv_raw).map_err(|_| "bufferView index exceeds usize range")?;
    let acc_byte_offset = json_u64_to_usize(acc.get("byteOffset"), 0, "accessor.byteOffset")?;

    let bv = buffer_views.get(bv_idx).ok_or("Invalid bufferView index")?;
    let bv_byte_offset = json_u64_to_usize(bv.get("byteOffset"), 0, "bufferView.byteOffset")?;

    let total_offset = bv_byte_offset
        .checked_add(acc_byte_offset)
        .ok_or("Buffer offset overflow while reading Vec2 accessor")?;
    let byte_stride = json_u64_to_usize(bv.get("byteStride"), 8, "bufferView.byteStride")?;

    validate_accessor_range(
        total_offset,
        count,
        byte_stride,
        8,
        bin.len(),
        "Vec2 accessor",
    )?;

    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        if i % 8192 == 0 && should_stop() {
            return Err("Cancelled".to_string());
        }
        let start = total_offset + i * byte_stride;
        let u = f32::from_le_bytes([bin[start], bin[start + 1], bin[start + 2], bin[start + 3]]);
        let v = f32::from_le_bytes([
            bin[start + 4],
            bin[start + 5],
            bin[start + 6],
            bin[start + 7],
        ]);
        out.push([u, v]);
    }
    Ok(out)
}

fn read_indices_accessor(
    acc_idx: usize,
    accessors: &[Value],
    buffer_views: &[Value],
    bin: &[u8],
    should_stop: &dyn Fn() -> bool,
) -> Result<Vec<u32>, String> {
    let acc = accessors.get(acc_idx).ok_or("Invalid accessor index")?;
    let count_raw = acc
        .get("count")
        .and_then(|c| c.as_u64())
        .ok_or("Missing count")?;
    let count = usize::try_from(count_raw).map_err(|_| "Accessor count exceeds usize range")?;
    let comp_type = acc
        .get("componentType")
        .and_then(|t| t.as_u64())
        .unwrap_or(5123) as u32;
    let bv_raw = acc
        .get("bufferView")
        .and_then(|b| b.as_u64())
        .ok_or("Missing bufferView")?;
    let bv_idx = usize::try_from(bv_raw).map_err(|_| "bufferView index exceeds usize range")?;
    let acc_byte_offset = json_u64_to_usize(acc.get("byteOffset"), 0, "accessor.byteOffset")?;

    let bv = buffer_views.get(bv_idx).ok_or("Invalid bufferView index")?;
    let bv_byte_offset = json_u64_to_usize(bv.get("byteOffset"), 0, "bufferView.byteOffset")?;

    let total_offset = bv_byte_offset
        .checked_add(acc_byte_offset)
        .ok_or("Buffer offset overflow while reading indices accessor")?;

    if comp_type == 5123 {
        // UNSIGNED_SHORT (16-bit)
        validate_accessor_range(total_offset, count, 2, 2, bin.len(), "u16 indices")?;
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            if i % 8192 == 0 && should_stop() {
                return Err("Cancelled".to_string());
            }
            let start = total_offset + i * 2;
            let idx = u16::from_le_bytes([bin[start], bin[start + 1]]) as u32;
            out.push(idx);
        }
        Ok(out)
    } else if comp_type == 5125 {
        // UNSIGNED_INT (32-bit)
        validate_accessor_range(total_offset, count, 4, 4, bin.len(), "u32 indices")?;
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            if i % 8192 == 0 && should_stop() {
                return Err("Cancelled".to_string());
            }
            let start = total_offset + i * 4;
            let idx =
                u32::from_le_bytes([bin[start], bin[start + 1], bin[start + 2], bin[start + 3]]);
            out.push(idx);
        }
        Ok(out)
    } else {
        Err(format!("Unsupported index component type: {comp_type}"))
    }
}

/// Parses a Wavefront OBJ text buffer into a `RawMesh`.
fn load_obj_mesh_cancellable(
    data: &[u8],
    should_stop: &dyn Fn() -> bool,
) -> Result<RawMesh, String> {
    let data = strip_leading_boms_and_ws(data);
    let text =
        std::str::from_utf8(data).map_err(|e| format!("OBJ data contains invalid UTF-8: {e}"))?;

    let mut raw_positions: Vec<[f32; 3]> = Vec::new();
    let mut raw_normals: Vec<[f32; 3]> = Vec::new();
    let mut raw_uvs: Vec<[f32; 2]> = Vec::new();
    let mut raw_faces: Vec<Vec<String>> = Vec::new();

    // Pass 1: Collect raw attributes and face lines
    for (line_idx, line) in text.lines().enumerate() {
        if line_idx % 4096 == 0 && should_stop() {
            return Err("Cancelled".to_string());
        }
        let line = line.trim_matches(|c: char| c.is_whitespace() || c == '\u{FEFF}');
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
                if !x.is_finite() || !y.is_finite() || !z.is_finite() {
                    return Err(format!(
                        "Non-finite vertex coordinate on OBJ line {}",
                        line_idx + 1
                    ));
                }
                raw_positions.push([x, y, z]);
            }
            "vn" => {
                let x: f32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                let y: f32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                let z: f32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                if !x.is_finite() || !y.is_finite() || !z.is_finite() {
                    return Err(format!(
                        "Non-finite vertex normal on OBJ line {}",
                        line_idx + 1
                    ));
                }
                raw_normals.push([x, y, z]);
            }
            "vt" => {
                let u: f32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                let v: f32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                if !u.is_finite() || !v.is_finite() {
                    return Err(format!(
                        "Non-finite UV coordinate on OBJ line {}",
                        line_idx + 1
                    ));
                }
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
    for (face_idx, face_tokens) in raw_faces.iter().enumerate() {
        if face_idx % 4096 == 0 && should_stop() {
            return Err("Cancelled".to_string());
        }
        let mut face_verts = Vec::with_capacity(face_tokens.len());
        for token in face_tokens {
            let mut subtokens = token.split('/');
            let v_idx: i32 = subtokens.next().and_then(|s| s.parse().ok()).unwrap_or(0);
            let vt_idx: Option<i32> =
                subtokens
                    .next()
                    .and_then(|s| if s.is_empty() { None } else { s.parse().ok() });
            let vn_idx: Option<i32> =
                subtokens
                    .next()
                    .and_then(|s| if s.is_empty() { None } else { s.parse().ok() });

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
