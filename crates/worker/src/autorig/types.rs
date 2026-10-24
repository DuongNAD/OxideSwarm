//! Data types, skeletal armature definitions, landmarks, and skin weight containers.

use super::math::{Mat4, Vec3};

/// Skeletal armature preset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RigPreset {
    Biped,
    Quadruped,
    Generic,
}

impl RigPreset {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().trim() {
            "quadruped" => Self::Quadruped,
            "generic" => Self::Generic,
            _ => Self::Biped, // default
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Biped => "biped",
            Self::Quadruped => "quadruped",
            Self::Generic => "generic",
        }
    }
}

/// Single joint in the skeletal DAG hierarchy.
#[derive(Debug, Clone, PartialEq)]
pub struct Joint {
    /// 0-based joint index in the armature.
    pub index: usize,
    /// Human-readable joint name (e.g. "Hips", "Spine", "LeftUpperLeg").
    pub name: String,
    /// Parent joint index, or None if root.
    pub parent: Option<usize>,
    /// Child joint indices.
    pub children: Vec<usize>,
    /// Global rest position in model space.
    pub rest_world_pos: Vec3,
    /// Local translation relative to parent in rest pose.
    pub local_translation: Vec3,
    /// Local rotation quaternion [x, y, z, w].
    pub local_rotation: [f32; 4],
    /// Local scale [sx, sy, sz].
    pub local_scale: [f32; 3],
    /// Global world transform matrix W_j.
    pub world_transform: Mat4,
    /// Inverse Bind Matrix (IBM) = W_j^(-1).
    pub inverse_bind_matrix: Mat4,
}

/// Complete hierarchical skeletal armature.
#[derive(Debug, Clone, PartialEq)]
pub struct Armature {
    pub preset: RigPreset,
    /// Topologically sorted joint list (parent < child).
    pub joints: Vec<Joint>,
    pub root_index: usize,
}

impl Armature {
    pub fn joint_count(&self) -> usize {
        self.joints.len()
    }

    /// Serializes all Inverse Bind Matrices into a tightly packed column-major byte buffer (64 bytes per joint).
    pub fn serialize_inverse_bind_matrices(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.joints.len() * 64);
        for joint in &self.joints {
            let arr = joint.inverse_bind_matrix.to_column_major_array();
            for f in arr {
                bytes.extend_from_slice(&f.to_le_bytes());
            }
        }
        bytes
    }
}

/// Extracted 3D anatomical landmarks from mesh geometric analysis.
#[derive(Debug, Clone, PartialEq)]
pub struct AnatomicalLandmarks {
    pub height: f32,
    pub width: f32,
    pub depth: f32,
    pub is_symmetric: bool,
    pub symmetry_error: f32,
    pub root: Vec3,
    pub spine: Vec3,
    pub chest: Vec3,
    pub neck: Vec3,
    pub head: Vec3,
    pub hip_l: Vec3,
    pub knee_l: Vec3,
    pub ankle_l: Vec3,
    pub hip_r: Vec3,
    pub knee_r: Vec3,
    pub ankle_r: Vec3,
    pub shoulder_l: Vec3,
    pub elbow_l: Vec3,
    pub wrist_l: Vec3,
    pub shoulder_r: Vec3,
    pub elbow_r: Vec3,
    pub wrist_r: Vec3,
}

/// Lateral body side classification for bone segments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimbSide {
    Axial,
    Left,
    Right,
}

/// Anatomical bone segment for capsule distance calculation.
#[derive(Debug, Clone, PartialEq)]
pub struct BoneSegment {
    /// Index of the joint in the armature hierarchy that this bone segment drives.
    pub joint_index: u16,
    /// Proximal joint position (start point).
    pub start: Vec3,
    /// Distal joint position (end point).
    pub end: Vec3,
    /// Characteristic anatomical radius of influence.
    pub radius: f32,
    /// Lateral side classification.
    pub side: LimbSide,
}

/// Output skinning weights container compliant with glTF 2.0.
#[derive(Debug, Clone, PartialEq)]
pub struct SkinWeights {
    /// glTF JOINTS_0 stream: exactly 4 joint indices per vertex.
    pub joints: Vec<[u16; 4]>,
    /// glTF WEIGHTS_0 stream: exactly 4 normalized weights per vertex, sum = 1.0.
    pub weights: Vec<[f32; 4]>,
}

impl SkinWeights {
    /// Serializes JOINTS_0 into little-endian bytes for the glTF binary chunk.
    pub fn to_joints_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.joints.len() * 8);
        for j in &self.joints {
            bytes.extend_from_slice(&j[0].to_le_bytes());
            bytes.extend_from_slice(&j[1].to_le_bytes());
            bytes.extend_from_slice(&j[2].to_le_bytes());
            bytes.extend_from_slice(&j[3].to_le_bytes());
        }
        bytes
    }

    /// Serializes WEIGHTS_0 into little-endian bytes for the glTF binary chunk.
    pub fn to_weights_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.weights.len() * 16);
        for w in &self.weights {
            bytes.extend_from_slice(&w[0].to_le_bytes());
            bytes.extend_from_slice(&w[1].to_le_bytes());
            bytes.extend_from_slice(&w[2].to_le_bytes());
            bytes.extend_from_slice(&w[3].to_le_bytes());
        }
        bytes
    }

    /// Computes accessor min and max bounds for JOINTS_0.
    pub fn compute_joints_bounds(&self) -> ([u16; 4], [u16; 4]) {
        if self.joints.is_empty() {
            return ([0; 4], [0; 4]);
        }
        let mut min = [u16::MAX; 4];
        let mut max = [u16::MIN; 4];
        for j in &self.joints {
            for k in 0..4 {
                min[k] = min[k].min(j[k]);
                max[k] = max[k].max(j[k]);
            }
        }
        (min, max)
    }

    /// Computes accessor min and max bounds for WEIGHTS_0.
    pub fn compute_weights_bounds(&self) -> ([f32; 4], [f32; 4]) {
        if self.weights.is_empty() {
            return ([0.0; 4], [0.0; 4]);
        }
        let mut min = [f32::INFINITY; 4];
        let mut max = [f32::NEG_INFINITY; 4];
        for w in &self.weights {
            for k in 0..4 {
                min[k] = min[k].min(w[k]);
                max[k] = max[k].max(w[k]);
            }
        }
        (min, max)
    }
}

/// Unified error type for the worker autorig pipeline.
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("AutoRig error: {0}")]
    AutoRig(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

impl From<String> for WorkerError {
    fn from(s: String) -> Self {
        WorkerError::AutoRig(s)
    }
}

impl From<&str> for WorkerError {
    fn from(s: &str) -> Self {
        WorkerError::AutoRig(s.to_string())
    }
}
