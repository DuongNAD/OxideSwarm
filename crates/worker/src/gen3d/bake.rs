//! Barycentric PBR texture atlas baking and pure-Rust PNG encoder (Feature F8).

use super::types::{BakedMaterial, RawMesh};

/// Trait defining a surface color and material sampler.
pub trait SurfaceSampler: Send + Sync {
    fn sample(&self, position: [f32; 3], normal: [f32; 3]) -> [u8; 4];
}

/// Default procedural PBR surface sampler with directional lighting and subtle height tinting.
pub struct ProceduralPbrSampler;

impl SurfaceSampler for ProceduralPbrSampler {
    fn sample(&self, pos: [f32; 3], norm: [f32; 3]) -> [u8; 4] {
        // Directional light from top-right-front
        let light_dir = [0.577f32, 0.577, 0.577];
        let ndotl =
            (norm[0] * light_dir[0] + norm[1] * light_dir[1] + norm[2] * light_dir[2]).max(0.0);
        let ambient = 0.35 + 0.15 * (norm[1] * 0.5 + 0.5);
        let factor = (ambient + ndotl * 0.5).clamp(0.0, 1.0);

        // High-tech slate / titanium base color with subtle height gradient
        let height_tint = (pos[1] * 0.5 + 0.5).clamp(0.0, 1.0);
        let r = ((50.0 + 35.0 * height_tint) * factor).clamp(0.0, 255.0) as u8;
        let g = ((60.0 + 40.0 * height_tint) * factor).clamp(0.0, 255.0) as u8;
        let b = ((75.0 + 45.0 * height_tint) * factor).clamp(0.0, 255.0) as u8;

        [r, g, b, 255]
    }
}

/// Bakes a procedural surface sampler onto a UV-parameterized mesh.
pub fn bake_texture_atlas(
    mesh: &RawMesh,
    width: u32,
    height: u32,
    sampler: &dyn SurfaceSampler,
) -> BakedMaterial {
    let mut buffer = vec![0u8; (width * height * 4) as usize];
    let mut mask = vec![false; (width * height) as usize];

    let w_f = width as f32;
    let h_f = height as f32;

    for chunk in mesh.indices.chunks_exact(3) {
        let i0 = chunk[0] as usize;
        let i1 = chunk[1] as usize;
        let i2 = chunk[2] as usize;

        if i0 >= mesh.uvs.len() || i1 >= mesh.uvs.len() || i2 >= mesh.uvs.len() {
            continue;
        }

        let uv0 = mesh.uvs[i0];
        let uv1 = mesh.uvs[i1];
        let uv2 = mesh.uvs[i2];

        let p0 = [uv0[0] * w_f, (1.0 - uv0[1]) * h_f];
        let p1 = [uv1[0] * w_f, (1.0 - uv1[1]) * h_f];
        let p2 = [uv2[0] * w_f, (1.0 - uv2[1]) * h_f];

        let min_x = (p0[0].min(p1[0]).min(p2[0]).floor() as i32).max(0);
        let max_x = (p0[0].max(p1[0]).max(p2[0]).ceil() as i32).min(width as i32 - 1);
        let min_y = (p0[1].min(p1[1]).min(p2[1]).floor() as i32).max(0);
        let max_y = (p0[1].max(p1[1]).max(p2[1]).ceil() as i32).min(height as i32 - 1);

        let det = (p1[1] - p2[1]) * (p0[0] - p2[0]) + (p2[0] - p1[0]) * (p0[1] - p2[1]);
        if det.abs() < 1e-9 {
            continue;
        }
        let inv_det = 1.0 / det;

        for py in min_y..=max_y {
            let y = py as f32 + 0.5;
            for px in min_x..=max_x {
                let x = px as f32 + 0.5;
                let l0 = ((p1[1] - p2[1]) * (x - p2[0]) + (p2[0] - p1[0]) * (y - p2[1])) * inv_det;
                let l1 = ((p2[1] - p0[1]) * (x - p2[0]) + (p0[0] - p2[0]) * (y - p2[1])) * inv_det;
                let l2 = 1.0 - l0 - l1;

                if l0 >= -1e-3 && l1 >= -1e-3 && l2 >= -1e-3 {
                    let w0 = l0.max(0.0);
                    let w1 = l1.max(0.0);
                    let w2 = l2.max(0.0);
                    let sum = w0 + w1 + w2;
                    let (w0, w1, w2) = if sum > 1e-9 {
                        (w0 / sum, w1 / sum, w2 / sum)
                    } else {
                        (0.333, 0.333, 0.333)
                    };

                    let v0 = mesh.positions[i0];
                    let v1 = mesh.positions[i1];
                    let v2 = mesh.positions[i2];
                    let pos = [
                        w0 * v0[0] + w1 * v1[0] + w2 * v2[0],
                        w0 * v0[1] + w1 * v1[1] + w2 * v2[1],
                        w0 * v0[2] + w1 * v1[2] + w2 * v2[2],
                    ];

                    let n0 = mesh.normals.get(i0).copied().unwrap_or([0.0, 1.0, 0.0]);
                    let n1 = mesh.normals.get(i1).copied().unwrap_or([0.0, 1.0, 0.0]);
                    let n2 = mesh.normals.get(i2).copied().unwrap_or([0.0, 1.0, 0.0]);
                    let raw_n = [
                        w0 * n0[0] + w1 * n1[0] + w2 * n2[0],
                        w0 * n0[1] + w1 * n1[1] + w2 * n2[1],
                        w0 * n0[2] + w1 * n1[2] + w2 * n2[2],
                    ];
                    let n_len = (raw_n[0].powi(2) + raw_n[1].powi(2) + raw_n[2].powi(2)).sqrt();
                    let norm = if n_len > 1e-9 {
                        [raw_n[0] / n_len, raw_n[1] / n_len, raw_n[2] / n_len]
                    } else {
                        [0.0, 1.0, 0.0]
                    };

                    let rgba = sampler.sample(pos, norm);
                    let pixel_idx = (py as usize * width as usize + px as usize) * 4;
                    buffer[pixel_idx..pixel_idx + 4].copy_from_slice(&rgba);
                    mask[py as usize * width as usize + px as usize] = true;
                }
            }
        }
    }

    // Seam margin dilation (2 passes to prevent texture bleeding across charts)
    for _ in 0..2 {
        let mut next_buffer = buffer.clone();
        let mut next_mask = mask.clone();
        for py in 0..height as usize {
            for px in 0..width as usize {
                let idx = py * width as usize + px;
                if !mask[idx] {
                    let mut found = None;
                    'neighbor: for dy in -1..=1 {
                        for dx in -1..=1 {
                            let nx = px as isize + dx;
                            let ny = py as isize + dy;
                            if nx >= 0 && nx < width as isize && ny >= 0 && ny < height as isize {
                                let n_idx = ny as usize * width as usize + nx as usize;
                                if mask[n_idx] {
                                    let p_idx = n_idx * 4;
                                    found = Some([
                                        buffer[p_idx],
                                        buffer[p_idx + 1],
                                        buffer[p_idx + 2],
                                        buffer[p_idx + 3],
                                    ]);
                                    break 'neighbor;
                                }
                            }
                        }
                    }
                    if let Some(rgba) = found {
                        let p_idx = idx * 4;
                        next_buffer[p_idx..p_idx + 4].copy_from_slice(&rgba);
                        next_mask[idx] = true;
                    }
                }
            }
        }
        buffer = next_buffer;
        mask = next_mask;
    }

    let png_bytes = encode_png_rgba(&buffer, width, height);

    BakedMaterial {
        width,
        height,
        base_color_png: png_bytes,
        roughness_factor: 0.6,
        metallic_factor: 0.1,
        base_color_factor: [1.0, 1.0, 1.0, 1.0],
    }
}

/// Pure-Rust uncompressed DEFLATE PNG encoder with Adler-32 and CRC-32 checksums.
pub fn encode_png_rgba(pixels: &[u8], width: u32, height: u32) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]); // PNG Header

    // 1. IHDR Chunk
    let mut ihdr = Vec::with_capacity(13);
    ihdr.extend_from_slice(&width.to_be_bytes());
    ihdr.extend_from_slice(&height.to_be_bytes());
    ihdr.push(8); // Bit depth: 8
    ihdr.push(6); // Color type: RGBA
    ihdr.push(0); // Compression method: 0 (DEFLATE)
    ihdr.push(0); // Filter method: 0 (None)
    ihdr.push(0); // Interlace method: 0 (No interlace)
    write_chunk(&mut out, b"IHDR", &ihdr);

    // 2. IDAT Chunk (zlib stream wrapping scanlines with filter 0)
    let scanline_len = (width as usize * 4) + 1;
    let mut raw_data = Vec::with_capacity(scanline_len * height as usize);
    for y in 0..height as usize {
        raw_data.push(0); // Filter type None
        let start = y * (width as usize * 4);
        raw_data.extend_from_slice(&pixels[start..start + width as usize * 4]);
    }

    let mut zlib = Vec::new();
    zlib.push(0x78); // CMF: Deflate, 32K window
    zlib.push(0x01); // FLG: FCHECK

    // Compute Adler-32 checksum
    let mut adler_a = 1u32;
    let mut adler_b = 0u32;
    for &byte in &raw_data {
        adler_a = (adler_a + byte as u32) % 65521;
        adler_b = (adler_b + adler_a) % 65521;
    }
    let adler = (adler_b << 16) | adler_a;

    // Deflate uncompressed blocks (max 65535 bytes per block)
    let chunk_size = 65535;
    let total_chunks = (raw_data.len() + chunk_size - 1).max(1) / chunk_size;
    for (i, slice) in raw_data.chunks(chunk_size).enumerate() {
        let is_last = i == total_chunks.saturating_sub(1);
        zlib.push(if is_last { 0x01 } else { 0x00 });
        let len = slice.len() as u16;
        zlib.extend_from_slice(&len.to_le_bytes());
        zlib.extend_from_slice(&(!len).to_le_bytes());
        zlib.extend_from_slice(slice);
    }
    zlib.extend_from_slice(&adler.to_be_bytes());
    write_chunk(&mut out, b"IDAT", &zlib);

    // 3. IEND Chunk
    write_chunk(&mut out, b"IEND", &[]);

    out
}

fn write_chunk(out: &mut Vec<u8>, chunk_type: &[u8; 4], data: &[u8]) {
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    out.extend_from_slice(chunk_type);
    out.extend_from_slice(data);
    let mut crc = crc32_update(0xFFFFFFFF, chunk_type);
    crc = crc32_update(crc, data);
    out.extend_from_slice(&(crc ^ 0xFFFFFFFF).to_be_bytes());
}

fn crc32_update(mut crc: u32, data: &[u8]) -> u32 {
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
        }
    }
    crc
}
