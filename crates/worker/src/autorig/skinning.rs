//! Feature F13: Geodesic Skin Weighting & Top-4 Pruning.
//!
//! Implements hybrid bone capsule proximity, sagittal topological barrier (X = 0),
//! multi-source geodesic surface Dijkstra gating, zero-allocation top-4 stack insertion sort,
//! and float32 residual compensation guaranteeing sum(w_k) == 1.000000.

use super::math::Vec3;
use super::types::{BoneSegment, LimbSide, SkinWeights};
use crate::gen3d::types::RawMesh;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Configuration options for the skinning weight calculation.
#[derive(Debug, Clone)]
pub struct SkinningConfig {
    /// Sagittal midline transition half-width (fraction of bbox width). Default 0.02.
    pub sagittal_transition_width: f32,
    /// Geodesic shortcut threshold ratio (tau_gap). Default 2.2.
    pub geodesic_gap_ratio: f32,
    /// Gaussian falloff sigma multiplier. Default 0.85.
    pub gaussian_sigma: f32,
}

impl Default for SkinningConfig {
    fn default() -> Self {
        Self {
            sagittal_transition_width: 0.02,
            geodesic_gap_ratio: 2.2,
            gaussian_sigma: 0.85,
        }
    }
}

/// Primary entry point: computes glTF 2.0 compliant skin weights for a mesh and armature bones.
pub fn compute_skin_weights(
    mesh: &RawMesh,
    bones: &[BoneSegment],
    config: &SkinningConfig,
) -> Result<SkinWeights, String> {
    let num_verts = mesh.positions.len();
    if num_verts == 0 {
        return Err("Cannot compute skin weights on empty mesh".to_string());
    }
    if bones.is_empty() {
        return Err("Cannot compute skin weights without bones".to_string());
    }

    let (min_bb, max_bb) = mesh.compute_aabb();
    let bbox_width = (max_bb[0] - min_bb[0]).max(1e-4);
    let bbox_height = (max_bb[1] - min_bb[1]).max(1e-4);
    let center_x = (min_bb[0] + max_bb[0]) * 0.5;

    // 1. Build mesh edge adjacency graph for surface geodesic calculations
    let adj = build_mesh_adjacency(num_verts, &mesh.indices);

    // 2. Identify bone surface anchors on mesh (nearest vertex to bone midpoint)
    let anchors: Vec<usize> = bones
        .iter()
        .map(|b| {
            let mid = (b.start + b.end) * 0.5;
            find_nearest_vertex(&mesh.positions, mid)
        })
        .collect();

    // 3. Multi-source Dijkstra geodesic surface flood for each bone
    let mut geo_dist: Vec<Vec<f32>> = Vec::with_capacity(bones.len());
    for &anchor in &anchors {
        geo_dist.push(dijkstra_shortest_paths(anchor, &adj, &mesh.positions));
    }

    let mut out_joints = Vec::with_capacity(num_verts);
    let mut out_weights = Vec::with_capacity(num_verts);

    let transition_width = config.sagittal_transition_width * bbox_width;
    let sigma_topo = 0.08 * bbox_height;

    // 4. Compute weights per vertex
    let mut raw_weights = vec![0.0f32; bones.len()];

    for (v_idx, p_arr) in mesh.positions.iter().enumerate() {
        let v = Vec3::from_array(*p_arr);

        for (b_idx, b) in bones.iter().enumerate() {
            // A. Point-to-segment bone capsule projection
            let u = b.end - b.start;
            let len_sq = u.length_sq();
            let (raw_dist, t_clamped, t_raw) = if len_sq > 1e-8 {
                let t = (v - b.start).dot(u) / len_sq;
                let tc = t.clamp(0.0, 1.0);
                let proj = b.start + u * tc;
                ((v - proj).length(), tc, t)
            } else {
                ((v - b.start).length(), 0.0, 0.0)
            };
            let _ = t_clamped;

            // B. Longitudinal axial overhang penalty
            let delta_ext = (-t_raw).max(0.0) + (t_raw - 1.0).max(0.0);
            let bone_len = len_sq.sqrt();
            let dist_long = raw_dist + 2.5 * bone_len * delta_ext;
            let norm_dist = dist_long / b.radius.max(1e-4);

            // C. Sagittal Barrier (X = 0)
            let beta_sag = match b.side {
                LimbSide::Axial => 1.0,
                LimbSide::Left => {
                    let s = -(v.x - center_x); // Left is negative relative X
                    smooth_sagittal_barrier(s, transition_width)
                }
                LimbSide::Right => {
                    let s = v.x - center_x; // Right is positive relative X
                    smooth_sagittal_barrier(s, transition_width)
                }
            };

            if beta_sag <= 0.0 {
                raw_weights[b_idx] = 0.0;
                continue;
            }

            // D. Geodesic topological gating (Armpit & gap protection)
            let d_surface = geo_dist[b_idx][v_idx];
            let gamma_topo = if d_surface.is_finite() {
                let gap_excess = d_surface - config.geodesic_gap_ratio * raw_dist;
                if gap_excess > 0.0 {
                    (-gap_excess * gap_excess / (2.0 * sigma_topo * sigma_topo)).exp()
                } else {
                    1.0
                }
            } else {
                1.0 // Fallback for disconnected mesh shells
            };

            // E. Gaussian heat diffusion kernel
            let sigma = config.gaussian_sigma;
            let phi = (-norm_dist * norm_dist / (2.0 * sigma * sigma)).exp();

            raw_weights[b_idx] = beta_sag * gamma_topo * phi;
        }

        // 5. Zero-allocation Top-4 influence pruning
        let mut top_j: [u16; 4] = [0; 4];
        let mut top_w: [f32; 4] = [0.0; 4];

        for (b_idx, &rw) in raw_weights.iter().enumerate() {
            if rw <= top_w[3] {
                continue;
            }
            let j = bones[b_idx].joint_index;
            if rw > top_w[0] {
                top_w[3] = top_w[2]; top_j[3] = top_j[2];
                top_w[2] = top_w[1]; top_j[2] = top_j[1];
                top_w[1] = top_w[0]; top_j[1] = top_j[0];
                top_w[0] = rw;       top_j[0] = j;
            } else if rw > top_w[1] {
                top_w[3] = top_w[2]; top_j[3] = top_j[2];
                top_w[2] = top_w[1]; top_j[2] = top_j[1];
                top_w[1] = rw;       top_j[1] = j;
            } else if rw > top_w[2] {
                top_w[3] = top_w[2]; top_j[3] = top_j[2];
                top_w[2] = rw;       top_j[2] = j;
            } else {
                top_w[3] = rw;       top_j[3] = j;
            }
        }

        // 6. Float32 roundoff compensation for strict unity sum = 1.000000
        let sum = top_w[0] + top_w[1] + top_w[2] + top_w[3];
        if sum > 1e-8 {
            let inv_s = 1.0 / sum;
            top_w[0] *= inv_s;
            top_w[1] *= inv_s;
            top_w[2] *= inv_s;
            top_w[3] *= inv_s;
            // Residual correction applied to largest weight
            top_w[0] = 1.0 - (top_w[1] + top_w[2] + top_w[3]);
        } else {
            // Defense against orphan vertex: bind 100% to Root joint
            top_j = [0, 0, 0, 0];
            top_w = [1.0, 0.0, 0.0, 0.0];
        }

        out_joints.push(top_j);
        out_weights.push(top_w);
    }

    Ok(SkinWeights {
        joints: out_joints,
        weights: out_weights,
    })
}

#[inline(always)]
fn smooth_sagittal_barrier(signed_dist: f32, half_width: f32) -> f32 {
    if signed_dist >= half_width {
        1.0
    } else if signed_dist <= -half_width {
        0.0
    } else {
        let u = (signed_dist + half_width) / (2.0 * half_width);
        u * u * (3.0 - 2.0 * u) // Hermite smoothstep
    }
}

#[derive(Copy, Clone, PartialEq)]
struct DijkstraEntry {
    vertex: usize,
    cost: f32,
}

impl Eq for DijkstraEntry {}

impl Ord for DijkstraEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other.cost.partial_cmp(&self.cost).unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for DijkstraEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn build_mesh_adjacency(num_verts: usize, indices: &[u32]) -> Vec<Vec<usize>> {
    let mut adj = vec![Vec::with_capacity(6); num_verts];
    for tri in indices.chunks_exact(3) {
        let i0 = tri[0] as usize;
        let i1 = tri[1] as usize;
        let i2 = tri[2] as usize;
        if i0 < num_verts && i1 < num_verts && i2 < num_verts {
            adj[i0].push(i1); adj[i0].push(i2);
            adj[i1].push(i0); adj[i1].push(i2);
            adj[i2].push(i0); adj[i2].push(i1);
        }
    }
    // Deduplicate neighbors
    for neighbors in &mut adj {
        neighbors.sort_unstable();
        neighbors.dedup();
    }
    adj
}

fn find_nearest_vertex(positions: &[[f32; 3]], point: Vec3) -> usize {
    let mut best_idx = 0;
    let mut best_dist_sq = f32::INFINITY;
    for (idx, p) in positions.iter().enumerate() {
        let dx = p[0] - point.x;
        let dy = p[1] - point.y;
        let dz = p[2] - point.z;
        let d_sq = dx * dx + dy * dy + dz * dz;
        if d_sq < best_dist_sq {
            best_dist_sq = d_sq;
            best_idx = idx;
        }
    }
    best_idx
}

fn dijkstra_shortest_paths(
    start: usize,
    adj: &[Vec<usize>],
    positions: &[[f32; 3]],
) -> Vec<f32> {
    if start >= adj.len() {
        return vec![f32::INFINITY; adj.len()];
    }

    let mut dist = vec![f32::INFINITY; adj.len()];
    let mut heap = BinaryHeap::new();

    dist[start] = 0.0;
    heap.push(DijkstraEntry { vertex: start, cost: 0.0 });

    while let Some(DijkstraEntry { vertex, cost }) = heap.pop() {
        if cost > dist[vertex] {
            continue;
        }
        let p_u = positions[vertex];

        for &neighbor in &adj[vertex] {
            if neighbor >= positions.len() {
                continue;
            }
            let p_v = positions[neighbor];
            let edge_len = ((p_u[0] - p_v[0]).powi(2)
                + (p_u[1] - p_v[1]).powi(2)
                + (p_u[2] - p_v[2]).powi(2))
            .sqrt();

            let next_cost = cost + edge_len;
            if next_cost < dist[neighbor] {
                dist[neighbor] = next_cost;
                heap.push(DijkstraEntry {
                    vertex: neighbor,
                    cost: next_cost,
                });
            }
        }
    }
    dist
}
