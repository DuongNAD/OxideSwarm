//! Feature F11: Mesh Landmark & Symmetry Extraction.
//!
//! Provides PCA orientation analysis, sagittal plane symmetry detection (X = 0),
//! 50-slab vertical slicing, and extraction of 15 anatomical skeletal joints.

use super::math::{eigen_decomposition_3x3, Vec3};
use super::types::{AnatomicalLandmarks, RigPreset};
use crate::gen3d::types::RawMesh;

/// Analyzes mesh geometry and extracts 3D anatomical landmarks using PCA alignment.
pub fn extract_landmarks(mesh: &RawMesh, _preset: RigPreset) -> Result<AnatomicalLandmarks, String> {
    if mesh.positions.len() < 8 {
        return Err("Mesh has insufficient vertices for landmark extraction (minimum 8 required)".to_string());
    }

    let n = mesh.positions.len() as f32;
    let inv_n = 1.0 / n;

    // 1. Compute vertex centroid p_bar = 1/N sum(p_i)
    let centroid = mesh
        .positions
        .iter()
        .fold(Vec3::ZERO, |acc, p| acc + Vec3::new(p[0], p[1], p[2]))
        * inv_n;

    // 2. Compute 3x3 symmetric sample covariance tensor:
    // C = 1/N sum (p_i - p_bar)(p_i - p_bar)^T
    let mut c00 = 0.0f32;
    let mut c01 = 0.0f32;
    let mut c02 = 0.0f32;
    let mut c11 = 0.0f32;
    let mut c12 = 0.0f32;
    let mut c22 = 0.0f32;

    for p in &mesh.positions {
        let dx = p[0] - centroid.x;
        let dy = p[1] - centroid.y;
        let dz = p[2] - centroid.z;

        c00 += dx * dx;
        c01 += dx * dy;
        c02 += dx * dz;
        c11 += dy * dy;
        c12 += dy * dz;
        c22 += dz * dz;
    }

    let cov_matrix = [
        [c00 * inv_n, c01 * inv_n, c02 * inv_n],
        [c01 * inv_n, c11 * inv_n, c12 * inv_n],
        [c02 * inv_n, c12 * inv_n, c22 * inv_n],
    ];

    // 3. Decompose C via eigen_decomposition_3x3 to extract principal axes
    let eigen = eigen_decomposition_3x3(cov_matrix);

    // 4. Identify dominant vertical axis, secondary lateral axis, and front axis
    let mut up_idx = 0;
    // Prefer eigenvector aligned with world UP if it possesses significant variance
    for i in 0..3 {
        let dot_up = eigen.eigenvectors[i].dot(Vec3::UP).abs();
        if dot_up > 0.85 && eigen.eigenvalues[i] > 0.4 * eigen.eigenvalues[0] {
            up_idx = i;
            break;
        }
    }

    let other_indices: Vec<usize> = (0..3).filter(|&idx| idx != up_idx).collect();
    let (lat_idx, _depth_idx) = if eigen.eigenvalues[other_indices[0]] >= eigen.eigenvalues[other_indices[1]] {
        (other_indices[0], other_indices[1])
    } else {
        (other_indices[1], other_indices[0])
    };

    let mut axis_y = eigen.eigenvectors[up_idx];
    let mut axis_x = eigen.eigenvectors[lat_idx];

    // Check vertical orientation: if axis_y dot UP < 0, flip so +Y points towards the head
    if axis_y.dot(Vec3::UP) < -0.05 {
        axis_y = -axis_y;
    } else if axis_y.dot(Vec3::UP).abs() <= 0.05 {
        // If mesh is oriented horizontally, use anatomical vertex density asymmetry:
        // Torso/head contains more vertices than legs, so positive projection should have higher vertex count
        let mut pos_count = 0usize;
        let mut neg_count = 0usize;
        for p in &mesh.positions {
            let dp = Vec3::new(p[0] - centroid.x, p[1] - centroid.y, p[2] - centroid.z);
            let proj = dp.dot(axis_y);
            if proj > 0.0 {
                pos_count += 1;
            } else if proj < 0.0 {
                neg_count += 1;
            }
        }
        if pos_count < neg_count {
            axis_y = -axis_y;
        }
    }

    // Ensure axis_x is strictly perpendicular to axis_y (Gram-Schmidt)
    axis_x = (axis_x - axis_y * axis_x.dot(axis_y)).normalize();

    // axis_z via cross product: axis_x x axis_y = axis_z
    let mut axis_z = axis_x.cross(axis_y).normalize();

    // If axis_z points backwards relative to world FORWARD, flip both axis_z and axis_x
    if axis_z.dot(Vec3::FORWARD) < -0.1 {
        axis_x = -axis_x;
        axis_z = -axis_z;
    }

    // 5. Transform vertices into canonical aligned frame (+Y up, +Z forward, X lateral)
    let mut canonical_positions = Vec::with_capacity(mesh.positions.len());
    let mut min_pos = [f32::INFINITY; 3];
    let mut max_pos = [f32::NEG_INFINITY; 3];

    for p in &mesh.positions {
        let dp = Vec3::new(p[0] - centroid.x, p[1] - centroid.y, p[2] - centroid.z);
        let xc = dp.dot(axis_x);
        let yc = dp.dot(axis_y);
        let zc = dp.dot(axis_z);

        min_pos[0] = min_pos[0].min(xc);
        min_pos[1] = min_pos[1].min(yc);
        min_pos[2] = min_pos[2].min(zc);

        max_pos[0] = max_pos[0].max(xc);
        max_pos[1] = max_pos[1].max(yc);
        max_pos[2] = max_pos[2].max(zc);

        canonical_positions.push([xc, yc, zc]);
    }

    let height = (max_pos[1] - min_pos[1]).max(1e-4);
    let width = (max_pos[0] - min_pos[0]).max(1e-4);
    let depth = (max_pos[2] - min_pos[2]).max(1e-4);

    let center_x = (min_pos[0] + max_pos[0]) * 0.5;
    let center_z = (min_pos[2] + max_pos[2]) * 0.5;

    // 6. Bilateral symmetry check across sagittal plane X = center_x in canonical frame
    let sample_count = canonical_positions.len().min(400);
    let step = (canonical_positions.len() / sample_count).max(1);
    let mut sym_err_acc = 0.0f32;

    for i in (0..canonical_positions.len()).step_by(step).take(sample_count) {
        let p = canonical_positions[i];
        let p_refl = [2.0 * center_x - p[0], p[1], p[2]];
        let mut min_d = f32::INFINITY;
        for j in (0..canonical_positions.len()).step_by(step * 2).take(sample_count / 2) {
            let q = canonical_positions[j];
            let d = ((p_refl[0] - q[0]).powi(2) + (p_refl[1] - q[1]).powi(2) + (p_refl[2] - q[2]).powi(2)).sqrt();
            if d < min_d {
                min_d = d;
            }
        }
        sym_err_acc += min_d;
    }
    let mean_sym_err = sym_err_acc / (sample_count as f32 * height);
    let is_symmetric = mean_sym_err < 0.06;

    // 7. Partition canonical mesh vertices into K = 50 vertical slabs
    const K: usize = 50;
    let delta_y = height / (K as f32);
    let mut slice_counts = [0usize; K];
    let mut slice_min_x = [f32::INFINITY; K];
    let mut slice_max_x = [f32::NEG_INFINITY; K];
    let mut slice_left_count = [0usize; K];
    let mut slice_right_count = [0usize; K];

    for p in &canonical_positions {
        let rel_y = (p[1] - min_pos[1]).clamp(0.0, height - 1e-4);
        let k = ((rel_y / delta_y) as usize).min(K - 1);
        slice_counts[k] += 1;

        if p[0] < slice_min_x[k] { slice_min_x[k] = p[0]; }
        if p[0] > slice_max_x[k] { slice_max_x[k] = p[0]; }

        let rel_x = p[0] - center_x;
        if rel_x < -0.02 * width {
            slice_left_count[k] += 1;
        } else if rel_x > 0.02 * width {
            slice_right_count[k] += 1;
        }
    }

    // 8. Pelvic Bifurcation (Crotch level detection)
    let mut crotch_k = 24; // fallback ~0.48 * 50
    for k in (10..30).rev() {
        if slice_left_count[k] > 4 && slice_right_count[k] > 4 {
            crotch_k = k;
            break;
        }
    }
    let y_crotch = min_pos[1] + (crotch_k as f32) * delta_y;

    // 9. Neck Constriction Detection
    let mut neck_k = 41; // fallback ~0.82 * 50
    let mut min_neck_width = f32::INFINITY;
    for k in 36..46 {
        if slice_counts[k] > 4 {
            let w = slice_max_x[k] - slice_min_x[k];
            if w < min_neck_width && w > 0.02 * width {
                min_neck_width = w;
                neck_k = k;
            }
        }
    }
    let y_neck = min_pos[1] + (neck_k as f32) * delta_y;

    // 10. Construct Key Landmark Coordinates in Canonical Frame
    let y_root = y_crotch + 0.04 * height;
    let y_chest = y_crotch + 0.65 * (y_neck - y_crotch);
    let y_spine = (y_root + y_chest) * 0.5;
    let y_head = y_neck + 0.55 * (max_pos[1] - y_neck);

    let leg_x_offset = 0.11 * width;
    let arm_x_offset = 0.22 * width;
    let elbow_x_offset = 0.38 * width;
    let wrist_x_offset = 0.46 * width;

    let y_knee = min_pos[1] + 0.26 * height;
    let y_ankle = min_pos[1] + 0.06 * height;
    let y_elbow = y_root + 0.12 * height;
    let y_wrist = y_root - 0.04 * height;

    // Biomechanical Pre-Bends (anterior knee +Z, posterior elbow -Z)
    let knee_prebend_z = center_z + 0.02 * height;
    let elbow_prebend_z = center_z - 0.02 * height;

    // 11. Helper to transform canonical coordinates back to mesh world space
    let to_world = |cx: f32, cy: f32, cz: f32| -> Vec3 {
        centroid + axis_x * cx + axis_y * cy + axis_z * cz
    };

    Ok(AnatomicalLandmarks {
        height,
        width,
        depth,
        is_symmetric,
        symmetry_error: mean_sym_err,
        root: to_world(center_x, y_root, center_z),
        spine: to_world(center_x, y_spine, center_z),
        chest: to_world(center_x, y_chest, center_z),
        neck: to_world(center_x, y_neck, center_z),
        head: to_world(center_x, y_head, center_z),
        hip_l: to_world(center_x - leg_x_offset, y_crotch, center_z),
        knee_l: to_world(center_x - leg_x_offset, y_knee, knee_prebend_z),
        ankle_l: to_world(center_x - leg_x_offset, y_ankle, center_z - 0.01 * depth),
        hip_r: to_world(center_x + leg_x_offset, y_crotch, center_z),
        knee_r: to_world(center_x + leg_x_offset, y_knee, knee_prebend_z),
        ankle_r: to_world(center_x + leg_x_offset, y_ankle, center_z - 0.01 * depth),
        shoulder_l: to_world(center_x - arm_x_offset, y_chest, center_z),
        elbow_l: to_world(center_x - elbow_x_offset, y_elbow, elbow_prebend_z),
        wrist_l: to_world(center_x - wrist_x_offset, y_wrist, center_z),
        shoulder_r: to_world(center_x + arm_x_offset, y_chest, center_z),
        elbow_r: to_world(center_x + elbow_x_offset, y_elbow, elbow_prebend_z),
        wrist_r: to_world(center_x + wrist_x_offset, y_wrist, center_z),
    })
}
