//! Conformal multi-chart UV parameterization and gutter-padded atlas packing (Feature F7).

use std::collections::HashMap;
use super::types::RawMesh;

pub const DEFAULT_GUTTER_PIXELS: u32 = 4;
pub const DEFAULT_ATLAS_SIZE: u32 = 1024;

/// Trait defining a UV unwrapping engine.
pub trait UvUnwrapper: Send + Sync {
    fn unwrap(&self, mesh: &RawMesh, gutter_pixels: u32, atlas_size: u32) -> RawMesh;
}

/// Built-in pure-Rust multi-chart box unwrapper with 3x2 grid packing.
pub struct MultiChartBoxUnwrapper;

impl UvUnwrapper for MultiChartBoxUnwrapper {
    fn unwrap(&self, mesh: &RawMesh, gutter_pixels: u32, atlas_size: u32) -> RawMesh {
        if mesh.positions.is_empty() || mesh.indices.is_empty() {
            return mesh.clone();
        }

        let num_faces = mesh.indices.len() / 3;
        let mut face_charts = Vec::with_capacity(num_faces);

        // 1. Assign each face to one of 6 cardinal charts (+-X, +-Y, +-Z)
        for chunk in mesh.indices.chunks_exact(3) {
            let n0 = mesh.normals.get(chunk[0] as usize).copied().unwrap_or([0.0, 1.0, 0.0]);
            let n1 = mesh.normals.get(chunk[1] as usize).copied().unwrap_or([0.0, 1.0, 0.0]);
            let n2 = mesh.normals.get(chunk[2] as usize).copied().unwrap_or([0.0, 1.0, 0.0]);
            let avg_n = [
                n0[0] + n1[0] + n2[0],
                n0[1] + n1[1] + n2[1],
                n0[2] + n1[2] + n2[2],
            ];

            let ax = avg_n[0].abs();
            let ay = avg_n[1].abs();
            let az = avg_n[2].abs();

            let chart = if ax >= ay && ax >= az {
                if avg_n[0] > 0.0 { 0 } else { 1 } // +X, -X
            } else if ay >= ax && ay >= az {
                if avg_n[1] > 0.0 { 2 } else { 3 } // +Y, -Y
            } else {
                if avg_n[2] > 0.0 { 4 } else { 5 } // +Z, -Z
            };
            face_charts.push(chart);
        }

        // 2. Compute local 2D bounds per chart
        let mut chart_bounds = vec![
            ([f32::INFINITY, f32::INFINITY], [f32::NEG_INFINITY, f32::NEG_INFINITY]);
            6
        ];
        for (f_idx, chunk) in mesh.indices.chunks_exact(3).enumerate() {
            let chart = face_charts[f_idx];
            for &v_idx in chunk {
                let p = mesh.positions[v_idx as usize];
                let (u, v) = project_2d(p, chart);
                let (min, max) = &mut chart_bounds[chart];
                if u < min[0] { min[0] = u; }
                if v < min[1] { min[1] = v; }
                if u > max[0] { max[0] = u; }
                if v > max[1] { max[1] = v; }
            }
        }

        // 3. Configure 3x2 Atlas Grid with Gutter Padding
        let gutter = (gutter_pixels as f32) / (atlas_size as f32);
        let cell_w = ((1.0 - 4.0 * gutter) / 3.0).max(0.01);
        let cell_h = ((1.0 - 3.0 * gutter) / 2.0).max(0.01);

        // 4. Seam splitting and vertex assembly
        let mut new_positions = Vec::new();
        let mut new_normals = Vec::new();
        let mut new_uvs = Vec::new();
        let mut new_indices = Vec::with_capacity(mesh.indices.len());
        let mut vertex_map: HashMap<(u32, usize), u32> = HashMap::new();

        for (f_idx, chunk) in mesh.indices.chunks_exact(3).enumerate() {
            let chart = face_charts[f_idx];
            let (min, max) = chart_bounds[chart];
            let span_u = (max[0] - min[0]).max(1e-5);
            let span_v = (max[1] - min[1]).max(1e-5);

            let col = (chart % 3) as f32;
            let row = (chart / 3) as f32;
            let offset_u = gutter + col * (cell_w + gutter);
            let offset_v = gutter + row * (cell_h + gutter);

            for &v_idx in chunk {
                let key = (v_idx, chart);
                if let Some(&new_id) = vertex_map.get(&key) {
                    new_indices.push(new_id);
                } else {
                    let p = mesh.positions[v_idx as usize];
                    let norm = mesh.normals.get(v_idx as usize).copied().unwrap_or([0.0, 1.0, 0.0]);
                    let (raw_u, raw_v) = project_2d(p, chart);

                    let norm_u = (raw_u - min[0]) / span_u;
                    let norm_v = (raw_v - min[1]) / span_v;

                    let final_u = (offset_u + norm_u * cell_w).clamp(0.0, 1.0);
                    let final_v = (offset_v + norm_v * cell_h).clamp(0.0, 1.0);

                    let new_id = new_positions.len() as u32;
                    new_positions.push(p);
                    new_normals.push(norm);
                    new_uvs.push([final_u, final_v]);
                    vertex_map.insert(key, new_id);
                    new_indices.push(new_id);
                }
            }
        }

        RawMesh {
            positions: new_positions,
            normals: new_normals,
            uvs: new_uvs,
            indices: new_indices,
            colors: mesh.colors.clone(),
        }
    }
}

#[inline(always)]
fn project_2d(p: [f32; 3], chart: usize) -> (f32, f32) {
    match chart {
        0 => (-p[2], p[1]), // +X
        1 => (p[2], p[1]),  // -X
        2 => (p[0], -p[2]), // +Y
        3 => (p[0], p[2]),  // -Y
        4 => (p[0], p[1]),  // +Z
        _ => (-p[0], p[1]), // -Z
    }
}
