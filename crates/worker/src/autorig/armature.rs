//! Feature F12: Standard Hierarchical Armature & Inverse Bind Matrix (IBM) Generation.
//!
//! Builds the 15-joint skeletal DAG hierarchy, local/world transforms,
//! deterministic pre-bends, and column-major 4x4 Inverse Bind Matrices.

use super::math::{Mat4, Quat, Vec3};
use super::types::{AnatomicalLandmarks, Armature, BoneSegment, Joint, LimbSide, RigPreset};

impl Armature {
    /// Evaluates forward kinematics traversal through the skeletal DAG in topological order (0..15),
    /// computing cumulative world transforms:
    /// W_j = W_parent * L_j
    /// where L_j = Mat4::from_trs(local_translation, local_rotation, local_scale).
    pub fn evaluate_forward_kinematics(&self) -> Vec<Mat4> {
        let mut world_transforms = vec![Mat4::IDENTITY; self.joints.len()];
        for (i, joint) in self.joints.iter().enumerate() {
            let rot = Quat::new(
                joint.local_rotation[0],
                joint.local_rotation[1],
                joint.local_rotation[2],
                joint.local_rotation[3],
            );
            let scale = Vec3::new(
                joint.local_scale[0],
                joint.local_scale[1],
                joint.local_scale[2],
            );
            let local_transform = Mat4::from_trs(joint.local_translation, rot, scale);
            let world_transform = match joint.parent {
                Some(p_idx) => world_transforms[p_idx].mul_mat4(&local_transform),
                None => local_transform,
            };
            world_transforms[i] = world_transform;
        }
        world_transforms
    }

    /// Updates joint.world_transform and joint.inverse_bind_matrix for all joints
    /// using explicit forward kinematics traversal.
    pub fn update_world_transforms(&mut self) {
        let computed = self.evaluate_forward_kinematics();
        for (joint, world_mat) in self.joints.iter_mut().zip(computed) {
            joint.world_transform = world_mat;
            joint.inverse_bind_matrix = world_mat.inverse_rigid();
        }
    }
}

/// Builds the hierarchical skeletal armature from anatomical landmarks for the chosen preset.
pub fn build_standard_armature(lm: &AnatomicalLandmarks, preset: RigPreset) -> Armature {
    let mut joints: Vec<Joint> = Vec::with_capacity(match preset {
        RigPreset::Biped => 15,
        RigPreset::Quadruped => 19,
        RigPreset::Generic => 5,
    });

    macro_rules! add_joint {
        ($idx:expr, $name:expr, $parent:expr, $pos:expr) => {
            let parent_opt: Option<usize> = $parent;
            let local_trans = match parent_opt {
                Some(p_idx) => $pos - joints[p_idx].rest_world_pos,
                None => $pos,
            };

            joints.push(Joint {
                index: $idx,
                name: $name.to_string(),
                parent: parent_opt,
                children: Vec::new(),
                rest_world_pos: $pos,
                local_translation: local_trans,
                local_rotation: [0.0, 0.0, 0.0, 1.0],
                local_scale: [1.0, 1.0, 1.0],
                world_transform: Mat4::IDENTITY,
                inverse_bind_matrix: Mat4::IDENTITY,
            });

            if let Some(p_idx) = parent_opt {
                joints[p_idx].children.push($idx);
            }
        };
    }

    match preset {
        RigPreset::Biped => {
            // Joint 0: Hips (Root)
            add_joint!(0, "Hips", None, lm.root);
            // Joint 1: Spine
            add_joint!(1, "Spine", Some(0), lm.spine);
            // Joint 2: Chest
            add_joint!(2, "Chest", Some(1), lm.chest);
            // Joint 3: Neck
            add_joint!(3, "Neck", Some(2), lm.neck);
            // Joint 4: Head
            add_joint!(4, "Head", Some(3), lm.head);

            // Left Leg: UpperLeg_L (5) -> LowerLeg_L (6) -> Foot_L (7)
            add_joint!(5, "UpperLeg_L", Some(0), lm.hip_l);
            add_joint!(6, "LowerLeg_L", Some(5), lm.knee_l);
            add_joint!(7, "Foot_L", Some(6), lm.ankle_l);

            // Right Leg: UpperLeg_R (8) -> LowerLeg_R (9) -> Foot_R (10)
            add_joint!(8, "UpperLeg_R", Some(0), lm.hip_r);
            add_joint!(9, "LowerLeg_R", Some(8), lm.knee_r);
            add_joint!(10, "Foot_R", Some(9), lm.ankle_r);

            // Left Arm: UpperArm_L (11) -> LowerArm_L (12)
            add_joint!(11, "UpperArm_L", Some(2), lm.shoulder_l);
            add_joint!(12, "LowerArm_L", Some(11), lm.elbow_l);

            // Right Arm: UpperArm_R (13) -> LowerArm_R (14)
            add_joint!(13, "UpperArm_R", Some(2), lm.shoulder_r);
            add_joint!(14, "LowerArm_R", Some(13), lm.elbow_r);
        }
        RigPreset::Quadruped => {
            let q = lm.quadruped.as_ref();
            let tail_base = q
                .map(|ql| ql.tail_base)
                .unwrap_or_else(|| lm.root - Vec3::new(0.0, 0.0, 0.15 * lm.depth));
            let tail_tip = q
                .map(|ql| ql.tail_tip)
                .unwrap_or_else(|| tail_base - Vec3::new(0.0, 0.08 * lm.height, 0.15 * lm.depth));

            add_joint!(0, "Pelvis", None, q.map(|ql| ql.pelvis).unwrap_or(lm.root));
            add_joint!(
                1,
                "Spine",
                Some(0),
                q.map(|ql| ql.spine).unwrap_or(lm.spine)
            );
            add_joint!(
                2,
                "Chest",
                Some(1),
                q.map(|ql| ql.chest).unwrap_or(lm.chest)
            );
            add_joint!(3, "Neck", Some(2), q.map(|ql| ql.neck).unwrap_or(lm.neck));
            add_joint!(4, "Head", Some(3), q.map(|ql| ql.head).unwrap_or(lm.head));

            // Tail chain (5 -> 6)
            add_joint!(5, "Tail_01", Some(0), tail_base);
            add_joint!(6, "Tail_02", Some(5), tail_tip);

            // Hind Left Leg (7 -> 8 -> 9)
            add_joint!(
                7,
                "HindUpperLeg_L",
                Some(0),
                q.map(|ql| ql.hind_hip_l).unwrap_or(lm.hip_l)
            );
            add_joint!(
                8,
                "HindLowerLeg_L",
                Some(7),
                q.map(|ql| ql.hind_knee_l).unwrap_or(lm.knee_l)
            );
            add_joint!(
                9,
                "HindPaw_L",
                Some(8),
                q.map(|ql| ql.hind_ankle_l).unwrap_or(lm.ankle_l)
            );

            // Hind Right Leg (10 -> 11 -> 12)
            add_joint!(
                10,
                "HindUpperLeg_R",
                Some(0),
                q.map(|ql| ql.hind_hip_r).unwrap_or(lm.hip_r)
            );
            add_joint!(
                11,
                "HindLowerLeg_R",
                Some(10),
                q.map(|ql| ql.hind_knee_r).unwrap_or(lm.knee_r)
            );
            add_joint!(
                12,
                "HindPaw_R",
                Some(11),
                q.map(|ql| ql.hind_ankle_r).unwrap_or(lm.ankle_r)
            );

            // Front Left Leg (13 -> 14 -> 15)
            add_joint!(
                13,
                "FrontUpperLeg_L",
                Some(2),
                q.map(|ql| ql.front_shoulder_l).unwrap_or(lm.shoulder_l)
            );
            add_joint!(
                14,
                "FrontLowerLeg_L",
                Some(13),
                q.map(|ql| ql.front_elbow_l).unwrap_or(lm.elbow_l)
            );
            add_joint!(
                15,
                "FrontPaw_L",
                Some(14),
                q.map(|ql| ql.front_wrist_l).unwrap_or(lm.wrist_l)
            );

            // Front Right Leg (16 -> 17 -> 18)
            add_joint!(
                16,
                "FrontUpperLeg_R",
                Some(2),
                q.map(|ql| ql.front_shoulder_r).unwrap_or(lm.shoulder_r)
            );
            add_joint!(
                17,
                "FrontLowerLeg_R",
                Some(16),
                q.map(|ql| ql.front_elbow_r).unwrap_or(lm.elbow_r)
            );
            add_joint!(
                18,
                "FrontPaw_R",
                Some(17),
                q.map(|ql| ql.front_wrist_r).unwrap_or(lm.wrist_r)
            );
        }
        RigPreset::Generic => {
            // 5-joint universal axial hierarchy
            add_joint!(0, "Root", None, lm.root);
            add_joint!(1, "Spine", Some(0), lm.spine);
            add_joint!(2, "Chest", Some(1), lm.chest);
            add_joint!(3, "Neck", Some(2), lm.neck);
            add_joint!(4, "Head", Some(3), lm.head);
        }
    }

    let mut armature = Armature {
        preset,
        joints,
        root_index: 0,
    };

    // Forward Kinematics traversal to compute W_j = W_parent * L_j and IBMs
    armature.update_world_transforms();

    armature
}

/// Constructs the list of anatomical bone segments used by the skinning engine (Feature F13).
pub fn build_bone_segments(armature: &Armature, lm: &AnatomicalLandmarks) -> Vec<BoneSegment> {
    let h = lm.height;
    let mut segments = Vec::new();

    // Helper closure
    let mut add_segment =
        |joint_idx: usize, start: Vec3, end: Vec3, r_fraction: f32, side: LimbSide| {
            segments.push(BoneSegment {
                joint_index: joint_idx as u16,
                start,
                end,
                radius: r_fraction * h,
                side,
            });
        };

    // Axial Spine Segments (shared across all presets: joints 0..=4)
    add_segment(
        0,
        armature.joints[0].rest_world_pos,
        armature.joints[1].rest_world_pos,
        0.16,
        LimbSide::Axial,
    );
    add_segment(
        1,
        armature.joints[1].rest_world_pos,
        armature.joints[2].rest_world_pos,
        0.14,
        LimbSide::Axial,
    );
    add_segment(
        2,
        armature.joints[2].rest_world_pos,
        armature.joints[3].rest_world_pos,
        0.15,
        LimbSide::Axial,
    );
    add_segment(
        3,
        armature.joints[3].rest_world_pos,
        armature.joints[4].rest_world_pos,
        0.08,
        LimbSide::Axial,
    );
    add_segment(
        4,
        armature.joints[4].rest_world_pos,
        armature.joints[4].rest_world_pos + Vec3::new(0.0, 0.08 * h, 0.0),
        0.12,
        LimbSide::Axial,
    );

    match armature.preset {
        RigPreset::Biped => {
            // Left Leg Segments
            add_segment(
                5,
                armature.joints[5].rest_world_pos,
                armature.joints[6].rest_world_pos,
                0.10,
                LimbSide::Left,
            );
            add_segment(
                6,
                armature.joints[6].rest_world_pos,
                armature.joints[7].rest_world_pos,
                0.08,
                LimbSide::Left,
            );
            add_segment(
                7,
                armature.joints[7].rest_world_pos,
                armature.joints[7].rest_world_pos + Vec3::new(0.0, 0.0, 0.08 * h),
                0.07,
                LimbSide::Left,
            );

            // Right Leg Segments
            add_segment(
                8,
                armature.joints[8].rest_world_pos,
                armature.joints[9].rest_world_pos,
                0.10,
                LimbSide::Right,
            );
            add_segment(
                9,
                armature.joints[9].rest_world_pos,
                armature.joints[10].rest_world_pos,
                0.08,
                LimbSide::Right,
            );
            add_segment(
                10,
                armature.joints[10].rest_world_pos,
                armature.joints[10].rest_world_pos + Vec3::new(0.0, 0.0, 0.08 * h),
                0.07,
                LimbSide::Right,
            );

            // Left Arm Segments
            add_segment(
                11,
                armature.joints[11].rest_world_pos,
                armature.joints[12].rest_world_pos,
                0.08,
                LimbSide::Left,
            );
            add_segment(
                12,
                armature.joints[12].rest_world_pos,
                lm.wrist_l,
                0.06,
                LimbSide::Left,
            );

            // Right Arm Segments
            add_segment(
                13,
                armature.joints[13].rest_world_pos,
                armature.joints[14].rest_world_pos,
                0.08,
                LimbSide::Right,
            );
            add_segment(
                14,
                armature.joints[14].rest_world_pos,
                lm.wrist_r,
                0.06,
                LimbSide::Right,
            );
        }
        RigPreset::Quadruped => {
            // Tail segments (5 -> 6 -> tip)
            add_segment(
                5,
                armature.joints[5].rest_world_pos,
                armature.joints[6].rest_world_pos,
                0.07,
                LimbSide::Axial,
            );
            let tail_dir =
                (armature.joints[6].rest_world_pos - armature.joints[5].rest_world_pos).normalize();
            add_segment(
                6,
                armature.joints[6].rest_world_pos,
                armature.joints[6].rest_world_pos + tail_dir * (0.08 * h),
                0.05,
                LimbSide::Axial,
            );

            // Hind Left (7, 8, 9) & Hind Right (10, 11, 12)
            for &(u, l, p, side) in &[
                (7, 8, 9, LimbSide::Left),
                (10, 11, 12, LimbSide::Right),
                (13, 14, 15, LimbSide::Left),
                (16, 17, 18, LimbSide::Right),
            ] {
                add_segment(
                    u,
                    armature.joints[u].rest_world_pos,
                    armature.joints[l].rest_world_pos,
                    0.10,
                    side,
                );
                add_segment(
                    l,
                    armature.joints[l].rest_world_pos,
                    armature.joints[p].rest_world_pos,
                    0.08,
                    side,
                );
                add_segment(
                    p,
                    armature.joints[p].rest_world_pos,
                    armature.joints[p].rest_world_pos + Vec3::new(0.0, 0.0, 0.06 * h),
                    0.07,
                    side,
                );
            }
        }
        RigPreset::Generic => {
            // Axial segments 0..=4 already added above.
        }
    }

    segments
}
