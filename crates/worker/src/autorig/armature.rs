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

/// Builds the 15-joint standard hierarchical skeletal armature from anatomical landmarks.
pub fn build_standard_armature(lm: &AnatomicalLandmarks, preset: RigPreset) -> Armature {
    let mut joints: Vec<Joint> = Vec::with_capacity(15);

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

    // Axial Spine Segments
    // 0: Hips -> Spine
    add_segment(
        0,
        armature.joints[0].rest_world_pos,
        armature.joints[1].rest_world_pos,
        0.16,
        LimbSide::Axial,
    );
    // 1: Spine -> Chest
    add_segment(
        1,
        armature.joints[1].rest_world_pos,
        armature.joints[2].rest_world_pos,
        0.14,
        LimbSide::Axial,
    );
    // 2: Chest -> Neck
    add_segment(
        2,
        armature.joints[2].rest_world_pos,
        armature.joints[3].rest_world_pos,
        0.15,
        LimbSide::Axial,
    );
    // 3: Neck -> Head
    add_segment(
        3,
        armature.joints[3].rest_world_pos,
        armature.joints[4].rest_world_pos,
        0.08,
        LimbSide::Axial,
    );
    // 4: Head endcap
    add_segment(
        4,
        armature.joints[4].rest_world_pos,
        armature.joints[4].rest_world_pos + Vec3::new(0.0, 0.08 * h, 0.0),
        0.12,
        LimbSide::Axial,
    );

    // Left Leg Segments
    // 5: UpperLeg_L (Hip -> Knee)
    add_segment(
        5,
        armature.joints[5].rest_world_pos,
        armature.joints[6].rest_world_pos,
        0.10,
        LimbSide::Left,
    );
    // 6: LowerLeg_L (Knee -> Ankle)
    add_segment(
        6,
        armature.joints[6].rest_world_pos,
        armature.joints[7].rest_world_pos,
        0.08,
        LimbSide::Left,
    );
    // 7: Foot_L (Ankle -> Toe terminal)
    add_segment(
        7,
        armature.joints[7].rest_world_pos,
        armature.joints[7].rest_world_pos + Vec3::new(0.0, 0.0, 0.08 * h),
        0.07,
        LimbSide::Left,
    );

    // Right Leg Segments
    // 8: UpperLeg_R (Hip -> Knee)
    add_segment(
        8,
        armature.joints[8].rest_world_pos,
        armature.joints[9].rest_world_pos,
        0.10,
        LimbSide::Right,
    );
    // 9: LowerLeg_R (Knee -> Ankle)
    add_segment(
        9,
        armature.joints[9].rest_world_pos,
        armature.joints[10].rest_world_pos,
        0.08,
        LimbSide::Right,
    );
    // 10: Foot_R (Ankle -> Toe terminal)
    add_segment(
        10,
        armature.joints[10].rest_world_pos,
        armature.joints[10].rest_world_pos + Vec3::new(0.0, 0.0, 0.08 * h),
        0.07,
        LimbSide::Right,
    );

    // Left Arm Segments
    // 11: UpperArm_L (Shoulder -> Elbow)
    add_segment(
        11,
        armature.joints[11].rest_world_pos,
        armature.joints[12].rest_world_pos,
        0.08,
        LimbSide::Left,
    );
    // 12: LowerArm_L (Elbow -> Wrist terminal)
    add_segment(
        12,
        armature.joints[12].rest_world_pos,
        lm.wrist_l,
        0.06,
        LimbSide::Left,
    );

    // Right Arm Segments
    // 13: UpperArm_R (Shoulder -> Elbow)
    add_segment(
        13,
        armature.joints[13].rest_world_pos,
        armature.joints[14].rest_world_pos,
        0.08,
        LimbSide::Right,
    );
    // 14: LowerArm_R (Elbow -> Wrist terminal)
    add_segment(
        14,
        armature.joints[14].rest_world_pos,
        lm.wrist_r,
        0.06,
        LimbSide::Right,
    );

    segments
}
