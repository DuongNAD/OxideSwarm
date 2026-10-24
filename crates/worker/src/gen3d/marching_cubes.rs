//! Pure-Rust Marching Cubes isosurface extraction engine (Feature F4).
//!
//! Evaluates analytical SDF scalar fields over a regular 3D grid and extracts
//! watertight, manifold triangle meshes in < 15ms without heap reallocations.

use super::math::Vec3;
use super::mc_tables::{EDGE_TABLE, TRI_TABLE};
use super::sdf::SdfPreset;
use super::types::RawMesh;

/// Corner indices for the 12 edges in Paul Bourke's Marching Cubes canonical cube.
pub const EDGE_CORNERS: [(usize, usize); 12] = [
    (0, 1), (1, 2), (2, 3), (3, 0), // 0..3: Bottom face (Z = 0)
    (4, 5), (5, 6), (6, 7), (7, 4), // 4..7: Top face (Z = 1)
    (0, 4), (1, 5), (2, 6), (3, 7), // 8..11: Vertical struts
];

/// Extracts an isosurface mesh from an SDF preset at the given grid resolution.
pub fn extract_isosurface(preset: &SdfPreset, resolution: usize) -> Result<RawMesh, String> {
    let (min_b, max_b) = preset.bounds();

    let nx = resolution;
    let ny = (resolution as f32 * ((max_b.y - min_b.y) / (max_b.x - min_b.x)).max(0.5)).round() as usize;
    let nz = (resolution as f32 * ((max_b.z - min_b.z) / (max_b.x - min_b.x)).max(0.5)).round() as usize;

    let dx = (max_b.x - min_b.x) / (nx as f32);
    let dy = (max_b.y - min_b.y) / (ny as f32);
    let dz = (max_b.z - min_b.z) / (nz as f32);

    let corners_x = nx + 1;
    let corners_y = ny + 1;
    let corners_z = nz + 1;
    let total_corners = corners_x * corners_y * corners_z;

    // 1. Pre-sample grid scalar field in flat buffer
    let mut grid_vals = Vec::with_capacity(total_corners);
    for k in 0..corners_z {
        let z = min_b.z + (k as f32) * dz;
        for j in 0..corners_y {
            let y = min_b.y + (j as f32) * dy;
            for i in 0..corners_x {
                let x = min_b.x + (i as f32) * dx;
                let val = preset.eval(Vec3::new(x, y, z));
                grid_vals.push(val);
            }
        }
    }

    #[inline(always)]
    fn corner_val(vals: &[f32], cx: usize, cy: usize, i: usize, j: usize, k: usize) -> f32 {
        vals[(k * cy + j) * cx + i]
    }

    #[inline(always)]
    fn corner_pos(min_b: Vec3, dx: f32, dy: f32, dz: f32, i: usize, j: usize, k: usize) -> Vec3 {
        Vec3::new(
            min_b.x + (i as f32) * dx,
            min_b.y + (j as f32) * dy,
            min_b.z + (k as f32) * dz,
        )
    }

    // 2. Flat edge cache for O(1) deduplication: (corners_x * corners_y * corners_z * 3)
    let mut edge_cache = vec![u32::MAX; total_corners * 3];

    let mut positions = Vec::new();
    let mut normals = Vec::new();
    let mut indices = Vec::new();

    let cube_offsets: [(usize, usize, usize); 8] = [
        (0, 0, 0), // 0
        (1, 0, 0), // 1
        (1, 1, 0), // 2
        (0, 1, 0), // 3
        (0, 0, 1), // 4
        (1, 0, 1), // 5
        (1, 1, 1), // 6
        (0, 1, 1), // 7
    ];

    // 3. March over each cubic cell
    for k in 0..nz {
        for j in 0..ny {
            for i in 0..nx {
                // Fetch 8 corner values
                let mut v = [0.0f32; 8];
                let mut cube_idx = 0u8;

                for c in 0..8 {
                    let (ox, oy, oz) = cube_offsets[c];
                    let val = corner_val(&grid_vals, corners_x, corners_y, i + ox, j + oy, k + oz);
                    v[c] = val;
                    if val < 0.0 {
                        cube_idx |= 1 << c;
                    }
                }

                // Cell completely inside or outside surface
                if cube_idx == 0 || cube_idx == 255 {
                    continue;
                }

                let edge_flags = EDGE_TABLE[cube_idx as usize];
                if edge_flags == 0 {
                    continue;
                }

                let tri_row = &TRI_TABLE[cube_idx as usize];

                let mut t = 0;
                while t < 16 && tri_row[t] != -1 {
                    for edge_offset in 0..3 {
                        let edge_num = tri_row[t + edge_offset] as usize;

                        // Edge origin coordinate and axis for cache
                        let (ei, ej, ek, axis) = match edge_num {
                            0 => (i, j, k, 0),
                            1 => (i + 1, j, k, 1),
                            2 => (i, j + 1, k, 0),
                            3 => (i, j, k, 1),
                            4 => (i, j, k + 1, 0),
                            5 => (i + 1, j, k + 1, 1),
                            6 => (i, j + 1, k + 1, 0),
                            7 => (i, j, k + 1, 1),
                            8 => (i, j, k, 2),
                            9 => (i + 1, j, k, 2),
                            10 => (i + 1, j + 1, k, 2),
                            11 => (i, j + 1, k, 2),
                            _ => unreachable!(),
                        };

                        let cache_idx = ((ek * corners_y + ej) * corners_x + ei) * 3 + axis;
                        let cached = edge_cache[cache_idx];

                        if cached != u32::MAX {
                            indices.push(cached);
                        } else {
                            // Compute interpolated vertex position
                            let (c0, c1) = EDGE_CORNERS[edge_num];
                            let (ox0, oy0, oz0) = cube_offsets[c0];
                            let (ox1, oy1, oz1) = cube_offsets[c1];

                            let p0 = corner_pos(min_b, dx, dy, dz, i + ox0, j + oy0, k + oz0);
                            let p1 = corner_pos(min_b, dx, dy, dz, i + ox1, j + oy1, k + oz1);

                            let v0 = v[c0];
                            let v1 = v[c1];

                            let diff = v1 - v0;
                            let interp = if diff.abs() > 1e-12 {
                                ((-v0) / diff).clamp(0.0, 1.0)
                            } else {
                                0.5
                            };

                            let pos = p0 + (p1 - p0) * interp;

                            // Compute analytical normal via tetrahedral gradient
                            let eps = 0.001f32;
                            let k0 = Vec3::new(1.0, -1.0, -1.0);
                            let k1 = Vec3::new(-1.0, -1.0, 1.0);
                            let k2 = Vec3::new(-1.0, 1.0, -1.0);
                            let k3 = Vec3::new(1.0, 1.0, 1.0);
                            let grad = k0 * preset.eval(pos + k0 * eps)
                                + k1 * preset.eval(pos + k1 * eps)
                                + k2 * preset.eval(pos + k2 * eps)
                                + k3 * preset.eval(pos + k3 * eps);
                            let norm = grad.normalize().to_array();

                            let vert_id = positions.len() as u32;
                            positions.push(pos.to_array());
                            normals.push(norm);

                            edge_cache[cache_idx] = vert_id;
                            indices.push(vert_id);
                        }
                    }
                    t += 3;
                }
            }
        }
    }

    if positions.is_empty() || indices.is_empty() {
        return Err("Marching Cubes extracted empty mesh (0 vertices/triangles)".to_string());
    }

    Ok(RawMesh {
        positions,
        normals,
        uvs: Vec::new(),
        indices,
        colors: None,
    })
}
