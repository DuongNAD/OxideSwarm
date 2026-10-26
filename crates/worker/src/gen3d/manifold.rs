//! Manifold mesh repair and area-weighted smooth normal calculation (Feature F6).

use super::types::RawMesh;
use std::collections::HashMap;

pub const DEDUPLICATION_EPSILON: f32 = 1e-5;

/// Sanitizes mesh topology, deduplicates coincident vertices, prunes zero-area triangles,
/// compacts the vertex buffer, and calculates area-weighted smooth normals.
pub fn repair_manifold_mesh(raw: &RawMesh) -> RawMesh {
    if raw.positions.is_empty() || raw.indices.is_empty() {
        return raw.clone();
    }

    // 1. Spatial hashing vertex deduplication
    let mut unique_positions: Vec<[f32; 3]> = Vec::new();
    let mut remap = Vec::with_capacity(raw.positions.len());
    let mut grid: HashMap<(i64, i64, i64), Vec<u32>> = HashMap::new();
    let eps = DEDUPLICATION_EPSILON;
    let eps_sq = eps * eps;

    for p in &raw.positions {
        let kx = (p[0] / eps).round() as i64;
        let ky = (p[1] / eps).round() as i64;
        let kz = (p[2] / eps).round() as i64;

        let mut matched_idx = None;
        'search: for dx in -1..=1 {
            for dy in -1..=1 {
                for dz in -1..=1 {
                    if let Some(candidates) = grid.get(&(kx + dx, ky + dy, kz + dz)) {
                        for &cand_idx in candidates {
                            let q = unique_positions[cand_idx as usize];
                            let dist_sq = (p[0] - q[0]).powi(2)
                                + (p[1] - q[1]).powi(2)
                                + (p[2] - q[2]).powi(2);
                            if dist_sq < eps_sq {
                                matched_idx = Some(cand_idx);
                                break 'search;
                            }
                        }
                    }
                }
            }
        }

        if let Some(idx) = matched_idx {
            remap.push(idx);
        } else {
            let new_idx = unique_positions.len() as u32;
            unique_positions.push(*p);
            grid.entry((kx, ky, kz)).or_default().push(new_idx);
            remap.push(new_idx);
        }
    }

    // 2. Initial index remapping and collapsed face pruning
    let mut current_indices = Vec::with_capacity(raw.indices.len());
    for chunk in raw.indices.chunks_exact(3) {
        let i0 = remap[chunk[0] as usize];
        let i1 = remap[chunk[1] as usize];
        let i2 = remap[chunk[2] as usize];
        if i0 != i1 && i1 != i2 && i2 != i0 {
            current_indices.push(i0);
            current_indices.push(i1);
            current_indices.push(i2);
        }
    }

    // 3. Iterative Edge Collapse for degenerate and sliver triangles (norm_sq < 1e-14)
    // Collapsing the shortest edge preserves the closed 2-manifold property and eliminates open boundary holes.
    let mut iteration = 0;
    while iteration < 500 {
        iteration += 1;
        let mut sliver_edge = None;

        for chunk in current_indices.chunks_exact(3) {
            let i0 = chunk[0] as usize;
            let i1 = chunk[1] as usize;
            let i2 = chunk[2] as usize;

            let p0 = unique_positions[i0];
            let p1 = unique_positions[i1];
            let p2 = unique_positions[i2];

            let u = [p1[0] - p0[0], p1[1] - p0[1], p1[2] - p0[2]];
            let v = [p2[0] - p0[0], p2[1] - p0[1], p2[2] - p0[2]];

            let cx = u[1] * v[2] - u[2] * v[1];
            let cy = u[2] * v[0] - u[0] * v[2];
            let cz = u[0] * v[1] - u[1] * v[0];
            let norm_sq = cx * cx + cy * cy + cz * cz;

            if norm_sq < 1e-14 {
                let d01 =
                    (p1[0] - p0[0]).powi(2) + (p1[1] - p0[1]).powi(2) + (p1[2] - p0[2]).powi(2);
                let d12 =
                    (p2[0] - p1[0]).powi(2) + (p2[1] - p1[1]).powi(2) + (p2[2] - p1[2]).powi(2);
                let d20 =
                    (p0[0] - p2[0]).powi(2) + (p0[1] - p2[1]).powi(2) + (p0[2] - p2[2]).powi(2);

                let (keep_v, drop_v) = if d01 <= d12 && d01 <= d20 {
                    (i0 as u32, i1 as u32)
                } else if d12 <= d01 && d12 <= d20 {
                    (i1 as u32, i2 as u32)
                } else {
                    (i2 as u32, i0 as u32)
                };

                sliver_edge = Some((keep_v, drop_v));
                break;
            }
        }

        if let Some((keep_v, drop_v)) = sliver_edge {
            for idx in &mut current_indices {
                if *idx == drop_v {
                    *idx = keep_v;
                }
            }
            let mut pruned = Vec::with_capacity(current_indices.len());
            for chunk in current_indices.chunks_exact(3) {
                if chunk[0] != chunk[1] && chunk[1] != chunk[2] && chunk[2] != chunk[0] {
                    pruned.push(chunk[0]);
                    pruned.push(chunk[1]);
                    pruned.push(chunk[2]);
                }
            }
            current_indices = pruned;
        } else {
            break;
        }
    }

    let valid_indices = current_indices;
    let mut referenced = vec![false; unique_positions.len()];
    for &idx in &valid_indices {
        referenced[idx as usize] = true;
    }

    // 3. Compact vertex buffer (prune unreferenced vertices)
    let mut compact_positions = Vec::new();
    let mut compact_map = vec![0u32; unique_positions.len()];
    for (idx, &is_ref) in referenced.iter().enumerate() {
        if is_ref {
            compact_map[idx] = compact_positions.len() as u32;
            compact_positions.push(unique_positions[idx]);
        }
    }

    let final_indices: Vec<u32> = valid_indices
        .iter()
        .map(|&idx| compact_map[idx as usize])
        .collect();

    // 4. Calculate Area-Weighted Smooth Normals
    let mut normal_acc = vec![[0.0f32; 3]; compact_positions.len()];
    for chunk in final_indices.chunks_exact(3) {
        let i0 = chunk[0] as usize;
        let i1 = chunk[1] as usize;
        let i2 = chunk[2] as usize;

        let p0 = compact_positions[i0];
        let p1 = compact_positions[i1];
        let p2 = compact_positions[i2];

        let u = [p1[0] - p0[0], p1[1] - p0[1], p1[2] - p0[2]];
        let v = [p2[0] - p0[0], p2[1] - p0[1], p2[2] - p0[2]];

        let cx = u[1] * v[2] - u[2] * v[1];
        let cy = u[2] * v[0] - u[0] * v[2];
        let cz = u[0] * v[1] - u[1] * v[0];

        for &idx in &[i0, i1, i2] {
            normal_acc[idx][0] += cx;
            normal_acc[idx][1] += cy;
            normal_acc[idx][2] += cz;
        }
    }

    let normals: Vec<[f32; 3]> = normal_acc
        .into_iter()
        .map(|acc| {
            let len = (acc[0] * acc[0] + acc[1] * acc[1] + acc[2] * acc[2]).sqrt();
            if len > 1e-12 {
                [acc[0] / len, acc[1] / len, acc[2] / len]
            } else {
                [0.0, 1.0, 0.0]
            }
        })
        .collect();

    RawMesh {
        positions: compact_positions,
        normals,
        uvs: Vec::new(),
        indices: final_indices,
        colors: raw.colors.clone(),
    }
}
