//! Feature F14: Procedural Animation Track Synthesizer.
//!
//! Synthesizes cyclic, continuous TRS keyframe tracks at 30 Hz for standard humanoid motions:
//! - `idle`: natural breathing, root micro-sway, chest expansion.
//! - `walk`: cyclic biped locomotion, double-frequency pelvic bounce, antiphase arm/leg swing.
//! - `turnaround`: 360-degree yaw rotation with alternating foot steps.

use super::math::Quat;
use super::types::{Armature, RigPreset};

/// glTF 2.0 animation channel target path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnimationTargetPath {
    Translation,
    Rotation,
    Scale,
}

impl AnimationTargetPath {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Translation => "translation",
            Self::Rotation => "rotation",
            Self::Scale => "scale",
        }
    }
}

/// Sampled animation track for a single joint and target path.
#[derive(Debug, Clone, PartialEq)]
pub struct AnimationTrack {
    /// Index of the target joint node in the armature.
    pub target_joint: usize,
    pub path: AnimationTargetPath,
    /// Strictly monotonically increasing timestamp keyframes in seconds.
    pub timestamps: Vec<f32>,
    /// Flattened keyframe values:
    /// - 3 floats per keyframe for Translation and Scale ([x, y, z])
    /// - 4 floats per keyframe for Rotation ([x, y, z, w])
    pub values: Vec<f32>,
}

/// A complete procedural animation clip containing multiple joint tracks.
#[derive(Debug, Clone, PartialEq)]
pub struct ProceduralClip {
    pub name: String,
    pub duration: f32,
    pub tracks: Vec<AnimationTrack>,
}

pub struct ClipSynthesizer;

impl ClipSynthesizer {
    /// Synthesizes all requested animation clips for the given armature, failing on unknown clips.
    pub fn synthesize_all(
        requested: &[String],
        character_height: f32,
        armature: &Armature,
    ) -> Vec<ProceduralClip> {
        Self::synthesize_all_checked(requested, character_height, armature)
            .expect("Unsupported AutoRig animation clip")
    }

    /// Validated animation clip synthesis returning `Err` on unknown or empty clip names.
    pub fn synthesize_all_checked(
        requested: &[String],
        character_height: f32,
        armature: &Armature,
    ) -> Result<Vec<ProceduralClip>, String> {
        Self::synthesize_all_cancellable(requested, character_height, armature, &|| false)
    }

    /// Validated animation clip synthesis with cooperative cancellation checks.
    pub fn synthesize_all_cancellable(
        requested: &[String],
        character_height: f32,
        armature: &Armature,
        should_stop: &dyn Fn() -> bool,
    ) -> Result<Vec<ProceduralClip>, String> {
        let mut clips = Vec::with_capacity(requested.len());
        let mut seen = Vec::with_capacity(requested.len());
        for name in requested {
            if should_stop() {
                return Err("Cancelled".to_string());
            }
            let n = name.trim().to_ascii_lowercase();
            if matches!(n.as_str(), "idle" | "walk" | "turnaround") {
                if seen.contains(&n) {
                    return Err(format!(
                        "Duplicate AutoRig animation clip '{n}' in embed_animations"
                    ));
                }
                seen.push(n.clone());
            }
            match n.as_str() {
                "idle" => clips.push(Self::synthesize_idle(character_height, armature)),
                "walk" => clips.push(Self::synthesize_walk(character_height, armature)),
                "turnaround" => clips.push(Self::synthesize_turnaround(character_height, armature)),
                "" => {
                    return Err(
                        "Unsupported empty AutoRig animation clip; valid clips are: idle, walk, turnaround"
                            .to_string(),
                    )
                }
                other => {
                    return Err(format!(
                        "Unsupported AutoRig animation clip '{other}'; valid clips are: idle, walk, turnaround"
                    ))
                }
            }
        }
        Ok(clips)
    }

    /// Synthesizes the `idle` breathing animation clip (T = 2.4s, 73 frames at 30 Hz).
    pub fn synthesize_idle(h: f32, armature: &Armature) -> ProceduralClip {
        let duration = 2.4f32;
        let fps = 30.0f32;
        let frame_count = (duration * fps).round() as usize + 1; // 73 frames
        let omega = 2.0 * std::f32::consts::PI / duration;

        let mut timestamps = Vec::with_capacity(frame_count);
        for i in 0..frame_count {
            timestamps.push(i as f32 / fps);
        }

        let mut tracks = Vec::new();

        // 1. Root / Hips (Index 0): Subtle vertical and lateral micro-bob
        let rest_root_t = armature.joints[0].local_translation;
        let mut root_t_vals = Vec::with_capacity(frame_count * 3);
        let mut root_r_vals = Vec::with_capacity(frame_count * 4);

        for &t in &timestamps {
            let dy = -0.006 * h * (omega * t).sin();
            let dx = 0.003 * h * (0.5 * omega * t).sin();
            root_t_vals.extend_from_slice(&[rest_root_t.x + dx, rest_root_t.y + dy, rest_root_t.z]);

            let roll = 0.015 * (0.5 * omega * t).sin();
            let q = Quat::from_euler(0.0, 0.0, roll);
            root_r_vals.extend_from_slice(&q.to_array());
        }

        tracks.push(AnimationTrack {
            target_joint: 0,
            path: AnimationTargetPath::Translation,
            timestamps: timestamps.clone(),
            values: root_t_vals,
        });
        tracks.push(AnimationTrack {
            target_joint: 0,
            path: AnimationTargetPath::Rotation,
            timestamps: timestamps.clone(),
            values: root_r_vals,
        });

        // 2. Spine (Index 1) & Chest (Index 2): Thoracic breathing expansion
        if armature.joints.len() > 2 {
            let mut spine_r_vals = Vec::with_capacity(frame_count * 4);
            let mut chest_r_vals = Vec::with_capacity(frame_count * 4);
            let mut chest_s_vals = Vec::with_capacity(frame_count * 3);

            for &t in &timestamps {
                let pitch_spine = 0.020 * (omega * t).sin();
                let q_spine = Quat::from_euler(pitch_spine, 0.0, 0.0);
                spine_r_vals.extend_from_slice(&q_spine.to_array());

                let pitch_chest = 0.035 * (omega * t).sin();
                let q_chest = Quat::from_euler(pitch_chest, 0.0, 0.0);
                chest_r_vals.extend_from_slice(&q_chest.to_array());

                // Non-uniform lung scale expansion
                let s_breath = (omega * t).sin();
                chest_s_vals.extend_from_slice(&[
                    1.0 + 0.025 * s_breath,
                    1.0 + 0.015 * s_breath,
                    1.0 + 0.030 * s_breath,
                ]);
            }

            tracks.push(AnimationTrack {
                target_joint: 1,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: spine_r_vals,
            });
            tracks.push(AnimationTrack {
                target_joint: 2,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: chest_r_vals,
            });
            tracks.push(AnimationTrack {
                target_joint: 2,
                path: AnimationTargetPath::Scale,
                timestamps: timestamps.clone(),
                values: chest_s_vals,
            });
        }

        // 3. Head (Index 4): Counter-pitch to stabilize forward gaze
        if armature.joints.len() > 4 {
            let mut head_r_vals = Vec::with_capacity(frame_count * 4);
            for &t in &timestamps {
                let pitch_head = -0.025 * (omega * t).sin();
                let q_head = Quat::from_euler(pitch_head, 0.0, 0.0);
                head_r_vals.extend_from_slice(&q_head.to_array());
            }
            tracks.push(AnimationTrack {
                target_joint: 4,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: head_r_vals,
            });
        }

        // 4. Preset-specific secondary idle tracks
        if armature.preset == RigPreset::Biped && armature.joints.len() > 13 {
            let mut l_arm_r = Vec::with_capacity(frame_count * 4);
            let mut r_arm_r = Vec::with_capacity(frame_count * 4);

            for &t in &timestamps {
                let roll_l = -0.015 * (omega * t - 0.25).sin();
                let roll_r = 0.015 * (omega * t - 0.25).sin();
                l_arm_r.extend_from_slice(&Quat::from_euler(0.0, 0.0, roll_l).to_array());
                r_arm_r.extend_from_slice(&Quat::from_euler(0.0, 0.0, roll_r).to_array());
            }

            tracks.push(AnimationTrack {
                target_joint: 11,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: l_arm_r,
            });
            tracks.push(AnimationTrack {
                target_joint: 13,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: r_arm_r,
            });
        } else if armature.preset == RigPreset::Quadruped && armature.joints.len() > 6 {
            let mut tail1_r = Vec::with_capacity(frame_count * 4);
            let mut tail2_r = Vec::with_capacity(frame_count * 4);
            for &t in &timestamps {
                let yaw1 = 0.08 * (omega * t).sin();
                let yaw2 = 0.12 * (omega * t - 0.4).sin();
                tail1_r.extend_from_slice(&Quat::from_euler(0.0, yaw1, 0.0).to_array());
                tail2_r.extend_from_slice(&Quat::from_euler(0.0, yaw2, 0.0).to_array());
            }
            tracks.push(AnimationTrack {
                target_joint: 5,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: tail1_r,
            });
            tracks.push(AnimationTrack {
                target_joint: 6,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: tail2_r,
            });
        }

        ProceduralClip {
            name: "idle".to_string(),
            duration,
            tracks,
        }
    }

    /// Synthesizes the `walk` cyclic gait animation clip (T = 1.2s, 37 frames at 30 Hz).
    pub fn synthesize_walk(h: f32, armature: &Armature) -> ProceduralClip {
        let duration = 1.2f32;
        let fps = 30.0f32;
        let frame_count = (duration * fps).round() as usize + 1; // 37 frames
        let two_pi = 2.0 * std::f32::consts::PI;

        let mut timestamps = Vec::with_capacity(frame_count);
        for i in 0..frame_count {
            timestamps.push(i as f32 / fps);
        }

        let mut tracks = Vec::new();

        // 1. Root / Hips: 6-DOF cyclic gait (vertical bounce 2phi, lateral shift phi, pelvic yaw/roll)
        let rest_root_t = armature.joints[0].local_translation;
        let mut root_t = Vec::with_capacity(frame_count * 3);
        let mut root_r = Vec::with_capacity(frame_count * 4);

        for &t in &timestamps {
            let phi = two_pi * (t / duration);
            // Double frequency bounce: reaches minimum at heel strike (phi = 0, pi)
            let dy = -0.018 * h * (1.0 - (2.0 * phi).cos());
            let dx = 0.022 * h * phi.sin();
            root_t.extend_from_slice(&[rest_root_t.x + dx, rest_root_t.y + dy, rest_root_t.z]);

            let roll = 0.055 * phi.sin();
            let yaw = -0.075 * phi.sin();
            let q = Quat::from_euler(0.0, yaw, roll);
            root_r.extend_from_slice(&q.to_array());
        }

        tracks.push(AnimationTrack {
            target_joint: 0,
            path: AnimationTargetPath::Translation,
            timestamps: timestamps.clone(),
            values: root_t,
        });
        tracks.push(AnimationTrack {
            target_joint: 0,
            path: AnimationTargetPath::Rotation,
            timestamps: timestamps.clone(),
            values: root_r,
        });

        // 2. Chest (Index 2): Upper torso counter-rotation
        if armature.joints.len() > 2 {
            let mut chest_r = Vec::with_capacity(frame_count * 4);
            for &t in &timestamps {
                let phi = two_pi * (t / duration);
                let yaw = 0.065 * phi.sin(); // Antiphase to pelvic yaw
                let pitch = 0.040; // Forward athletic lean
                let q = Quat::from_euler(pitch, yaw, 0.0);
                chest_r.extend_from_slice(&q.to_array());
            }
            tracks.push(AnimationTrack {
                target_joint: 2,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: chest_r,
            });
        }

        // 3. Preset-specific gait tracks
        if armature.preset == RigPreset::Biped && armature.joints.len() > 14 {
            let mut l_thigh_r = Vec::with_capacity(frame_count * 4);
            let mut l_shin_r = Vec::with_capacity(frame_count * 4);
            let mut l_foot_r = Vec::with_capacity(frame_count * 4);

            let mut r_thigh_r = Vec::with_capacity(frame_count * 4);
            let mut r_shin_r = Vec::with_capacity(frame_count * 4);
            let mut r_foot_r = Vec::with_capacity(frame_count * 4);

            for &t in &timestamps {
                let phi_l = two_pi * (t / duration);
                let phi_r = phi_l + std::f32::consts::PI;

                // Thigh flexion/extension (hip pitch)
                let th_l = 0.38 * phi_l.sin();
                let th_r = 0.38 * phi_r.sin();
                l_thigh_r.extend_from_slice(&Quat::from_euler(th_l, 0.0, 0.0).to_array());
                r_thigh_r.extend_from_slice(&Quat::from_euler(th_r, 0.0, 0.0).to_array());

                // Knee flexion: unilateral hinge flexing backward (pitch < 0 in local coords)
                let flex_l = 0.08 + 0.75 * (-((phi_l - 0.20).sin())).max(0.0).powf(1.8);
                let flex_r = 0.08 + 0.75 * (-((phi_r - 0.20).sin())).max(0.0).powf(1.8);
                l_shin_r.extend_from_slice(&Quat::from_euler(-flex_l, 0.0, 0.0).to_array());
                r_shin_r.extend_from_slice(&Quat::from_euler(-flex_r, 0.0, 0.0).to_array());

                // Ankle flexion
                let ank_l = 0.22 * (phi_l - 0.45).sin();
                let ank_r = 0.22 * (phi_r - 0.45).sin();
                l_foot_r.extend_from_slice(&Quat::from_euler(ank_l, 0.0, 0.0).to_array());
                r_foot_r.extend_from_slice(&Quat::from_euler(ank_r, 0.0, 0.0).to_array());
            }

            tracks.push(AnimationTrack {
                target_joint: 5,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: l_thigh_r,
            });
            tracks.push(AnimationTrack {
                target_joint: 6,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: l_shin_r,
            });
            tracks.push(AnimationTrack {
                target_joint: 7,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: l_foot_r,
            });

            tracks.push(AnimationTrack {
                target_joint: 8,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: r_thigh_r,
            });
            tracks.push(AnimationTrack {
                target_joint: 9,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: r_shin_r,
            });
            tracks.push(AnimationTrack {
                target_joint: 10,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: r_foot_r,
            });

            // 4. Arms (UpperArm_L: 11, LowerArm_L: 12; UpperArm_R: 13, LowerArm_R: 14)
            let mut l_arm_r = Vec::with_capacity(frame_count * 4);
            let mut l_forearm_r = Vec::with_capacity(frame_count * 4);
            let mut r_arm_r = Vec::with_capacity(frame_count * 4);
            let mut r_forearm_r = Vec::with_capacity(frame_count * 4);

            for &t in &timestamps {
                let phi = two_pi * (t / duration);
                // Antiphase arm swing: Left arm moves with Right leg (phi + pi)
                let p_l = -0.32 * phi.sin();
                let p_r = 0.32 * phi.sin();
                l_arm_r.extend_from_slice(&Quat::from_euler(p_l, 0.0, 0.0).to_array());
                r_arm_r.extend_from_slice(&Quat::from_euler(p_r, 0.0, 0.0).to_array());

                // Elbow flexion during forward swing
                let el_l = 0.20 + 0.28 * phi.sin().max(0.0);
                let el_r = 0.20 + 0.28 * (-phi.sin()).max(0.0);
                l_forearm_r.extend_from_slice(&Quat::from_euler(el_l, 0.0, 0.0).to_array());
                r_forearm_r.extend_from_slice(&Quat::from_euler(el_r, 0.0, 0.0).to_array());
            }

            tracks.push(AnimationTrack {
                target_joint: 11,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: l_arm_r,
            });
            tracks.push(AnimationTrack {
                target_joint: 12,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: l_forearm_r,
            });
            tracks.push(AnimationTrack {
                target_joint: 13,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: r_arm_r,
            });
            tracks.push(AnimationTrack {
                target_joint: 14,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: r_forearm_r,
            });
        } else if armature.preset == RigPreset::Quadruped && armature.joints.len() > 18 {
            // Quadruped diagonal trot gait + tail wag:
            // Hind L (7,8) + Front R (16,17) move together; Hind R (10,11) + Front L (13,14) move antiphase.
            let mut tail1_r = Vec::with_capacity(frame_count * 4);
            let mut hl_up = Vec::with_capacity(frame_count * 4);
            let mut hl_lo = Vec::with_capacity(frame_count * 4);
            let mut hr_up = Vec::with_capacity(frame_count * 4);
            let mut hr_lo = Vec::with_capacity(frame_count * 4);
            let mut fl_up = Vec::with_capacity(frame_count * 4);
            let mut fl_lo = Vec::with_capacity(frame_count * 4);
            let mut fr_up = Vec::with_capacity(frame_count * 4);
            let mut fr_lo = Vec::with_capacity(frame_count * 4);

            for &t in &timestamps {
                let phi_a = two_pi * (t / duration);
                let phi_b = phi_a + std::f32::consts::PI;

                tail1_r
                    .extend_from_slice(&Quat::from_euler(0.0, 0.18 * phi_a.sin(), 0.0).to_array());

                let swing_a = 0.34 * phi_a.sin();
                let swing_b = 0.34 * phi_b.sin();
                let bend_a = 0.08 + 0.55 * (-phi_a.sin()).max(0.0);
                let bend_b = 0.08 + 0.55 * (-phi_b.sin()).max(0.0);

                hl_up.extend_from_slice(&Quat::from_euler(swing_a, 0.0, 0.0).to_array());
                hl_lo.extend_from_slice(&Quat::from_euler(-bend_a, 0.0, 0.0).to_array());
                fr_up.extend_from_slice(&Quat::from_euler(swing_a, 0.0, 0.0).to_array());
                fr_lo.extend_from_slice(&Quat::from_euler(bend_a * 0.7, 0.0, 0.0).to_array());

                hr_up.extend_from_slice(&Quat::from_euler(swing_b, 0.0, 0.0).to_array());
                hr_lo.extend_from_slice(&Quat::from_euler(-bend_b, 0.0, 0.0).to_array());
                fl_up.extend_from_slice(&Quat::from_euler(swing_b, 0.0, 0.0).to_array());
                fl_lo.extend_from_slice(&Quat::from_euler(bend_b * 0.7, 0.0, 0.0).to_array());
            }

            for (joint, vals) in [
                (5, tail1_r),
                (7, hl_up),
                (8, hl_lo),
                (10, hr_up),
                (11, hr_lo),
                (13, fl_up),
                (14, fl_lo),
                (16, fr_up),
                (17, fr_lo),
            ] {
                tracks.push(AnimationTrack {
                    target_joint: joint,
                    path: AnimationTargetPath::Rotation,
                    timestamps: timestamps.clone(),
                    values: vals,
                });
            }
        } else if armature.preset == RigPreset::Generic && armature.joints.len() > 4 {
            let mut spine_r = Vec::with_capacity(frame_count * 4);
            let mut head_r = Vec::with_capacity(frame_count * 4);
            for &t in &timestamps {
                let phi = two_pi * (t / duration);
                spine_r
                    .extend_from_slice(&Quat::from_euler(0.025, 0.04 * phi.sin(), 0.0).to_array());
                head_r.extend_from_slice(
                    &Quat::from_euler(-0.025, -0.05 * phi.sin(), 0.0).to_array(),
                );
            }
            tracks.push(AnimationTrack {
                target_joint: 1,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: spine_r,
            });
            tracks.push(AnimationTrack {
                target_joint: 4,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: head_r,
            });
        }

        ProceduralClip {
            name: "walk".to_string(),
            duration,
            tracks,
        }
    }

    /// Synthesizes the `turnaround` 360-degree rotation animation clip (T = 3.2s, 97 frames at 30 Hz).
    pub fn synthesize_turnaround(h: f32, armature: &Armature) -> ProceduralClip {
        let duration = 3.2f32;
        let fps = 30.0f32;
        let frame_count = (duration * fps).round() as usize + 1; // 97 frames
        let two_pi = 2.0 * std::f32::consts::PI;

        let mut timestamps = Vec::with_capacity(frame_count);
        for i in 0..frame_count {
            timestamps.push(i as f32 / fps);
        }

        let mut tracks = Vec::new();

        // 1. Root / Hips: 360-degree yaw rotation with stepping modulation
        let rest_root_t = armature.joints[0].local_translation;
        let mut root_t = Vec::with_capacity(frame_count * 3);
        let mut root_r = Vec::with_capacity(frame_count * 4);

        for &t in &timestamps {
            let u = t / duration;
            // Modulated turn rate with 4 discrete step propulsions
            let yaw = two_pi * (u - (0.12 / two_pi) * (8.0 * std::f32::consts::PI * u).sin());
            let dy = -0.015 * h * (4.0 * std::f32::consts::PI * u).sin().abs();

            root_t.extend_from_slice(&[rest_root_t.x, rest_root_t.y + dy, rest_root_t.z]);

            // Quaternion rotating around +Y axis
            let half_yaw = yaw * 0.5;
            let q = Quat::new(0.0, half_yaw.sin(), 0.0, half_yaw.cos()).normalize();
            root_r.extend_from_slice(&q.to_array());
        }

        tracks.push(AnimationTrack {
            target_joint: 0,
            path: AnimationTargetPath::Translation,
            timestamps: timestamps.clone(),
            values: root_t,
        });
        tracks.push(AnimationTrack {
            target_joint: 0,
            path: AnimationTargetPath::Rotation,
            timestamps: timestamps.clone(),
            values: root_r,
        });

        // 2. Preset-specific turnaround stepping / balance tracks
        if armature.preset == RigPreset::Biped && armature.joints.len() > 10 {
            let mut l_thigh_r = Vec::with_capacity(frame_count * 4);
            let mut l_shin_r = Vec::with_capacity(frame_count * 4);
            let mut r_thigh_r = Vec::with_capacity(frame_count * 4);
            let mut r_shin_r = Vec::with_capacity(frame_count * 4);

            for &t in &timestamps {
                let step_phase = (t % 0.8) / 0.8;
                let (lift_r, lift_l) = if step_phase < 0.5 {
                    // Right leg lifts and turns
                    let tau = step_phase / 0.5;
                    ((std::f32::consts::PI * tau).sin() * 0.45, 0.0f32)
                } else {
                    // Left leg lifts and turns
                    let tau = (step_phase - 0.5) / 0.5;
                    (0.0f32, (std::f32::consts::PI * tau).sin() * 0.45)
                };

                r_thigh_r.extend_from_slice(&Quat::from_euler(lift_r * 0.6, 0.0, 0.0).to_array());
                r_shin_r.extend_from_slice(&Quat::from_euler(-lift_r * 1.2, 0.0, 0.0).to_array());

                l_thigh_r.extend_from_slice(&Quat::from_euler(lift_l * 0.6, 0.0, 0.0).to_array());
                l_shin_r.extend_from_slice(&Quat::from_euler(-lift_l * 1.2, 0.0, 0.0).to_array());
            }

            tracks.push(AnimationTrack {
                target_joint: 5,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: l_thigh_r,
            });
            tracks.push(AnimationTrack {
                target_joint: 6,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: l_shin_r,
            });

            tracks.push(AnimationTrack {
                target_joint: 8,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: r_thigh_r,
            });
            tracks.push(AnimationTrack {
                target_joint: 9,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: r_shin_r,
            });
        } else if armature.preset == RigPreset::Quadruped && armature.joints.len() > 17 {
            let mut tail_r = Vec::with_capacity(frame_count * 4);
            let mut hl_r = Vec::with_capacity(frame_count * 4);
            let mut hr_r = Vec::with_capacity(frame_count * 4);
            let mut fl_r = Vec::with_capacity(frame_count * 4);
            let mut fr_r = Vec::with_capacity(frame_count * 4);

            for &t in &timestamps {
                let step_phase = (t % 0.8) / 0.8;
                let (lift_a, lift_b) = if step_phase < 0.5 {
                    let tau = step_phase / 0.5;
                    ((std::f32::consts::PI * tau).sin() * 0.35, 0.0f32)
                } else {
                    let tau = (step_phase - 0.5) / 0.5;
                    (0.0f32, (std::f32::consts::PI * tau).sin() * 0.35)
                };
                tail_r.extend_from_slice(
                    &Quat::from_euler(0.0, 0.14 * (two_pi * t / duration).sin(), 0.0).to_array(),
                );
                hl_r.extend_from_slice(&Quat::from_euler(lift_a * 0.5, 0.0, 0.0).to_array());
                fr_r.extend_from_slice(&Quat::from_euler(lift_a * 0.5, 0.0, 0.0).to_array());
                hr_r.extend_from_slice(&Quat::from_euler(lift_b * 0.5, 0.0, 0.0).to_array());
                fl_r.extend_from_slice(&Quat::from_euler(lift_b * 0.5, 0.0, 0.0).to_array());
            }

            for (joint, vals) in [(5, tail_r), (7, hl_r), (10, hr_r), (13, fl_r), (16, fr_r)] {
                tracks.push(AnimationTrack {
                    target_joint: joint,
                    path: AnimationTargetPath::Rotation,
                    timestamps: timestamps.clone(),
                    values: vals,
                });
            }
        } else if armature.preset == RigPreset::Generic && armature.joints.len() > 2 {
            let mut chest_r = Vec::with_capacity(frame_count * 4);
            for &t in &timestamps {
                let u = t / duration;
                let sway = 0.04 * (4.0 * std::f32::consts::PI * u).sin();
                chest_r.extend_from_slice(&Quat::from_euler(sway, 0.0, 0.0).to_array());
            }
            tracks.push(AnimationTrack {
                target_joint: 2,
                path: AnimationTargetPath::Rotation,
                timestamps: timestamps.clone(),
                values: chest_r,
            });
        }

        ProceduralClip {
            name: "turnaround".to_string(),
            duration,
            tracks,
        }
    }
}
