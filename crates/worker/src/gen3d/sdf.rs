//! Analytical Signed Distance Functions (SDF) and procedural 3D presets (Feature F4).

use super::math::Vec3;

// --- Analytical Primitives ---

#[inline(always)]
pub fn sd_sphere(p: Vec3, r: f32) -> f32 {
    p.length() - r
}

#[inline(always)]
pub fn sd_box(p: Vec3, b: Vec3) -> f32 {
    let q = p.abs() - b;
    let outside = q.component_max(Vec3::ZERO).length();
    let inside = q.max_elem().min(0.0);
    outside + inside
}

#[inline(always)]
pub fn sd_round_box(p: Vec3, b: Vec3, r: f32) -> f32 {
    sd_box(p, b - Vec3::new(r, r, r)) - r
}

#[inline(always)]
pub fn sd_capped_cylinder(p: Vec3, r: f32, h: f32) -> f32 {
    let d_radial = (p.x * p.x + p.z * p.z).sqrt() - r;
    let d_axial = p.y.abs() - h;
    let outside = (d_radial.max(0.0).powi(2) + d_axial.max(0.0).powi(2)).sqrt();
    let inside = d_radial.max(d_axial).min(0.0);
    outside + inside
}

#[inline(always)]
pub fn sd_capsule(p: Vec3, a: Vec3, b: Vec3, r: f32) -> f32 {
    let pa = p - a;
    let ba = b - a;
    let denom = ba.dot(ba);
    let h = if denom > 1e-12 {
        (pa.dot(ba) / denom).clamp(0.0, 1.0)
    } else {
        0.0
    };
    (pa - ba * h).length() - r
}

#[inline(always)]
pub fn sd_torus(p: Vec3, major_r: f32, minor_r: f32) -> f32 {
    let q_x = (p.x * p.x + p.z * p.z).sqrt() - major_r;
    let q_y = p.y;
    (q_x * q_x + q_y * q_y).sqrt() - minor_r
}

#[inline(always)]
pub fn sd_ellipsoid(p: Vec3, r: Vec3) -> f32 {
    let scaled = Vec3::new(p.x / r.x, p.y / r.y, p.z / r.z);
    (scaled.length() - 1.0) * r.min_elem()
}

// --- CSG & Smooth Blending Operators ---

#[inline(always)]
pub fn op_union(d1: f32, d2: f32) -> f32 {
    d1.min(d2)
}

#[inline(always)]
pub fn op_intersect(d1: f32, d2: f32) -> f32 {
    d1.max(d2)
}

#[inline(always)]
pub fn op_subtract(d1: f32, d2: f32) -> f32 {
    d1.max(-d2)
}

#[inline(always)]
pub fn op_smin(d1: f32, d2: f32, k: f32) -> f32 {
    let h = (0.5 + 0.5 * (d2 - d1) / k).clamp(0.0, 1.0);
    d2 * (1.0 - h) + d1 * h - k * h * (1.0 - h)
}

#[inline(always)]
pub fn op_ssub(d1: f32, d2: f32, k: f32) -> f32 {
    let h = (0.5 - 0.5 * (d1 + d2) / k).clamp(0.0, 1.0);
    d1 * (1.0 - h) + (-d2) * h + k * h * (1.0 - h)
}

// --- Procedural Presets ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SdfPreset {
    HumanoidDummy,
    RobotTorso,
    StylizedCharacter,
    TestOrb,
}

impl SdfPreset {
    /// Evaluates the signed distance to the surface at point `p`.
    #[inline(always)]
    pub fn eval(&self, p: Vec3) -> f32 {
        match self {
            Self::HumanoidDummy => eval_humanoid_dummy(p),
            Self::RobotTorso => eval_robot_torso(p),
            Self::StylizedCharacter => eval_stylized_character(p),
            Self::TestOrb => eval_test_orb(p),
        }
    }

    /// Returns the tight world-space bounding box `(min, max)` for grid sampling.
    pub fn bounds(&self) -> (Vec3, Vec3) {
        match self {
            Self::HumanoidDummy => (
                Vec3::new(-0.95, -0.05, -0.45),
                Vec3::new(0.95, 1.85, 0.45),
            ),
            Self::RobotTorso => (
                Vec3::new(-0.85, -0.05, -0.45),
                Vec3::new(0.85, 1.80, 0.45),
            ),
            Self::StylizedCharacter => (
                Vec3::new(-0.75, -0.05, -0.60),
                Vec3::new(0.75, 1.35, 0.60),
            ),
            Self::TestOrb => (
                Vec3::new(-0.85, -0.05, -0.85),
                Vec3::new(0.85, 1.65, 0.85),
            ),
        }
    }
}

/// Resolves the optimal procedural preset from a natural language prompt.
pub fn resolve_preset(prompt: Option<&str>) -> SdfPreset {
    let text = match prompt {
        Some(s) => s.to_lowercase(),
        None => return SdfPreset::HumanoidDummy,
    };

    if text.contains("robot") || text.contains("mech") || text.contains("droid") || text.contains("cyber") || text.contains("armor") {
        SdfPreset::RobotTorso
    } else if text.contains("chibi") || text.contains("cute") || text.contains("mascot") || text.contains("creature") || text.contains("stylized") {
        SdfPreset::StylizedCharacter
    } else if text.contains("orb") || text.contains("sphere") || text.contains("math") || text.contains("test") {
        SdfPreset::TestOrb
    } else {
        SdfPreset::HumanoidDummy
    }
}

// --- Preset Evaluators ---

/// Humanoid biped standing upright with Y-up, ground at Y=0, height ~1.8m, sagittal symmetry at X=0.
#[inline(always)]
fn eval_humanoid_dummy(p: Vec3) -> f32 {
    // 1. Head (centered at Y=1.62)
    let head = sd_ellipsoid(
        p - Vec3::new(0.0, 1.62, 0.0),
        Vec3::new(0.11, 0.14, 0.12),
    );

    // 2. Neck
    let neck = sd_capsule(
        p,
        Vec3::new(0.0, 1.42, 0.0),
        Vec3::new(0.0, 1.54, 0.0),
        0.06,
    );
    let head_neck = op_smin(head, neck, 0.04);

    // 3. Torso (upper chest and abdomen/pelvis)
    let upper_chest = sd_capsule(
        p,
        Vec3::new(0.0, 1.15, 0.0),
        Vec3::new(0.0, 1.40, 0.0),
        0.17,
    );
    let abdomen = sd_capsule(
        p,
        Vec3::new(0.0, 0.85, 0.0),
        Vec3::new(0.0, 1.15, 0.0),
        0.15,
    );
    let torso = op_smin(upper_chest, abdomen, 0.06);
    let upper_body = op_smin(head_neck, torso, 0.06);

    // 4. Bilateral symmetry for arms and legs across sagittal plane (X=0)
    let sym_p = Vec3::new(p.x.abs(), p.y, p.z);

    // Shoulder & Arm
    let shoulder = sd_sphere(sym_p - Vec3::new(0.22, 1.34, 0.0), 0.08);
    let upper_arm = sd_capsule(
        sym_p,
        Vec3::new(0.22, 1.34, 0.0),
        Vec3::new(0.48, 1.12, -0.02),
        0.06,
    );
    let forearm = sd_capsule(
        sym_p,
        Vec3::new(0.48, 1.12, -0.02),
        Vec3::new(0.72, 0.90, 0.02),
        0.05,
    );
    let hand = sd_capsule(
        sym_p,
        Vec3::new(0.72, 0.90, 0.02),
        Vec3::new(0.82, 0.80, 0.04),
        0.04,
    );
    let arm = op_smin(op_smin(shoulder, upper_arm, 0.04), op_smin(forearm, hand, 0.03), 0.04);

    // Legs
    let thigh = sd_capsule(
        sym_p,
        Vec3::new(0.12, 0.85, 0.0),
        Vec3::new(0.13, 0.45, 0.03),
        0.09,
    );
    let calf = sd_capsule(
        sym_p,
        Vec3::new(0.13, 0.45, 0.03),
        Vec3::new(0.14, 0.08, 0.0),
        0.07,
    );
    let leg_limb = op_smin(thigh, calf, 0.05);

    // Foot
    let foot_p = sym_p - Vec3::new(0.14, 0.04, 0.06);
    let foot = sd_round_box(foot_p, Vec3::new(0.05, 0.035, 0.11), 0.015);
    let leg = op_smin(leg_limb, foot, 0.04);

    // Combine all
    op_smin(upper_body, op_union(arm, leg), 0.06)
}

/// Robot torso mech with hard-surface features, visor, and articulated limbs.
#[inline(always)]
fn eval_robot_torso(p: Vec3) -> f32 {
    // Torso rounded box
    let torso_p = p - Vec3::new(0.0, 1.05, 0.0);
    let torso = sd_round_box(torso_p, Vec3::new(0.25, 0.32, 0.17), 0.03);

    // Chest vent subtraction
    let vent_p = p - Vec3::new(0.0, 1.15, 0.16);
    let vent = sd_capped_cylinder(vent_p, 0.08, 0.04);
    let torso_vent = op_ssub(torso, vent, 0.02);

    // Head
    let head_p = p - Vec3::new(0.0, 1.55, 0.0);
    let head_box = sd_round_box(head_p, Vec3::new(0.13, 0.13, 0.13), 0.02);
    // Visor groove
    let visor_p = head_p - Vec3::new(0.0, 0.02, 0.12);
    let visor = sd_box(visor_p, Vec3::new(0.11, 0.03, 0.04));
    let head = op_ssub(head_box, visor, 0.01);

    let neck = sd_capped_cylinder(p - Vec3::new(0.0, 1.40, 0.0), 0.05, 0.04);
    let body = op_union(op_union(torso_vent, neck), head);

    // Bilateral limbs
    let sym_p = Vec3::new(p.x.abs(), p.y, p.z);
    let shoulder = sd_sphere(sym_p - Vec3::new(0.32, 1.30, 0.0), 0.07);
    let arm = sd_capped_cylinder(sym_p - Vec3::new(0.42, 1.05, 0.0), 0.05, 0.20);
    let thigh = sd_capped_cylinder(sym_p - Vec3::new(0.15, 0.55, 0.0), 0.07, 0.18);
    let calf = sd_capped_cylinder(sym_p - Vec3::new(0.15, 0.22, 0.0), 0.06, 0.15);
    let foot = sd_box(sym_p - Vec3::new(0.15, 0.04, 0.05), Vec3::new(0.06, 0.04, 0.12));

    let limbs = op_union(
        op_union(shoulder, arm),
        op_union(op_union(thigh, calf), foot),
    );

    op_union(body, limbs)
}

/// Stylized mascot with chubby body, large ears, and friendly proportions.
#[inline(always)]
fn eval_stylized_character(p: Vec3) -> f32 {
    let body = sd_sphere(p - Vec3::new(0.0, 0.65, 0.0), 0.45);

    let sym_p = Vec3::new(p.x.abs(), p.y, p.z);
    // Ears
    let ear = sd_sphere(sym_p - Vec3::new(0.30, 1.05, 0.0), 0.14);
    // Short stubby legs
    let leg = sd_capsule(
        sym_p,
        Vec3::new(0.18, 0.35, 0.0),
        Vec3::new(0.18, 0.07, 0.0),
        0.09,
    );
    // Tiny arms
    let arm = sd_capsule(
        sym_p,
        Vec3::new(0.42, 0.65, 0.0),
        Vec3::new(0.55, 0.50, 0.08),
        0.07,
    );

    let with_ears = op_smin(body, ear, 0.08);
    let with_limbs = op_smin(with_ears, op_union(leg, arm), 0.06);
    with_limbs
}

/// Sci-fi test orb with 3 intersecting toruses for topological testing.
#[inline(always)]
fn eval_test_orb(p: Vec3) -> f32 {
    let center = p - Vec3::new(0.0, 0.80, 0.0);
    let sphere = sd_sphere(center, 0.48);

    // Torus in XZ plane
    let torus_xz = sd_torus(center, 0.62, 0.05);

    // Torus in XY plane
    let rot_xy = Vec3::new(center.x, center.z, center.y);
    let torus_xy = sd_torus(rot_xy, 0.62, 0.05);

    // Torus in YZ plane
    let rot_yz = Vec3::new(center.y, center.x, center.z);
    let torus_yz = sd_torus(rot_yz, 0.62, 0.05);

    let toruses = op_union(op_union(torus_xz, torus_xy), torus_yz);
    op_smin(sphere, toruses, 0.05)
}
