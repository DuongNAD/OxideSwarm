//! Core geometry, mesh, and material types for the 3D generation engine.

/// Basic 3D triangle mesh representation with optional normals, UVs, and vertex colors.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RawMesh {
    /// 3D vertex positions [x, y, z] in model space.
    pub positions: Vec<[f32; 3]>,
    /// Per-vertex surface normal unit vectors [nx, ny, nz].
    pub normals: Vec<[f32; 3]>,
    /// Per-vertex 2D texture coordinates [u, v] in range 0.0..=1.0.
    pub uvs: Vec<[f32; 2]>,
    /// Triangle face vertex indices. Length is always a multiple of 3.
    pub indices: Vec<u32>,
    /// Optional per-vertex RGBA colors (0.0..=1.0).
    pub colors: Option<Vec<[f32; 4]>>,
}

impl RawMesh {
    pub fn new(positions: Vec<[f32; 3]>, indices: Vec<u32>) -> Self {
        Self {
            positions,
            normals: Vec::new(),
            uvs: Vec::new(),
            indices,
            colors: None,
        }
    }

    pub fn triangle_count(&self) -> usize {
        self.indices.len() / 3
    }

    pub fn vertex_count(&self) -> usize {
        self.positions.len()
    }

    /// Computes the exact axis-aligned bounding box [min, max].
    pub fn compute_aabb(&self) -> ([f32; 3], [f32; 3]) {
        if self.positions.is_empty() {
            return ([0.0, 0.0, 0.0], [0.0, 0.0, 0.0]);
        }
        let mut min = [f32::INFINITY; 3];
        let mut max = [f32::NEG_INFINITY; 3];
        for p in &self.positions {
            for i in 0..3 {
                if p[i] < min[i] {
                    min[i] = p[i];
                }
                if p[i] > max[i] {
                    max[i] = p[i];
                }
            }
        }
        (min, max)
    }
}

/// Baked PBR texture map and material properties.
#[derive(Debug, Clone)]
pub struct BakedMaterial {
    /// Texture width in pixels (e.g. 512, 1024, 2048).
    pub width: u32,
    /// Texture height in pixels.
    pub height: u32,
    /// Raw PNG-encoded image bytes of the Albedo base color map.
    pub base_color_png: Vec<u8>,
    /// PBR roughness factor (0.0 = mirror, 1.0 = diffuse). Default 0.6.
    pub roughness_factor: f32,
    /// PBR metallic factor (0.0 = dielectric, 1.0 = pure metal). Default 0.1.
    pub metallic_factor: f32,
    /// Base color multiplier [R, G, B, A].
    pub base_color_factor: [f32; 4],
}

impl Default for BakedMaterial {
    fn default() -> Self {
        Self {
            width: 1024,
            height: 1024,
            base_color_png: Vec::new(),
            roughness_factor: 0.6,
            metallic_factor: 0.1,
            base_color_factor: [1.0, 1.0, 1.0, 1.0],
        }
    }
}

/// Telemetry and execution statistics for 3D mesh generation.
#[derive(Debug, Clone, PartialEq)]
pub struct GenerationStats {
    pub vertex_count: usize,
    pub triangle_count: usize,
    pub execution_time_ms: u64,
    pub aabb_min: [f32; 3],
    pub aabb_max: [f32; 3],
}
