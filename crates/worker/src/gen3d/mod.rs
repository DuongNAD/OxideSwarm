//! Local 3D Mesh Generation Engine for OxideSwarm (Milestone 2).
//!
//! Provides Tier-0 pure-Rust Procedural Signed Distance Function (SDF) Marching Cubes,
//! manifold topology repair, conformal UV unwrapping, barycentric PBR baking, and
//! direct glTF 2.0 binary (.glb) packing.

pub mod bake;
pub mod gltf_pack;
pub mod manifold;
pub mod marching_cubes;
pub mod math;
pub mod mc_tables;
pub mod runner;
pub mod sdf;
pub mod types;
pub mod uv;

use bake::{bake_texture_atlas, ProceduralPbrSampler};
use gltf_pack::pack_glb;
use manifold::repair_manifold_mesh;
use rusty_grid_core::task::Generative3DSpec;
use std::time::Instant;
pub use types::{BakedMaterial, GenerationStats, RawMesh};
use uv::{MultiChartBoxUnwrapper, UvUnwrapper};

/// Executes the full Tier-0 procedural 3D generation pipeline in pure Rust (< 50ms)
/// with progressive milestone callback reporting (25%, 50%, 75%, 100%).
pub fn generate_procedural_3d_with_progress<F>(
    spec: &Generative3DSpec,
    mut on_progress: F,
) -> Result<(Vec<u8>, GenerationStats), String>
where
    F: FnMut(f32, &'static str, &'static str),
{
    let start = Instant::now();

    // 1. Resolve preset from prompt
    let preset = sdf::resolve_preset(spec.prompt.as_deref());

    // 2. Select grid resolution based on quality tier
    let resolution = match spec.quality.as_str() {
        "fast" => 32,
        "high" => 64,
        _ => 48, // "standard"
    };

    // Stage 1: Geometry Extraction (25%)
    let raw_mesh = marching_cubes::extract_isosurface(&preset, resolution)?;
    on_progress(0.25, "sdf_geometry", "Isosurface extracted via Marching Cubes");

    // Stage 2: Manifold Mesh Cleanup & Conformal UV Parameterization (50%)
    let clean_mesh = repair_manifold_mesh(&raw_mesh);
    let atlas_size = match spec.quality.as_str() {
        "fast" => 512,
        "high" => 2048,
        _ => 1024,
    };
    let unwrapper = MultiChartBoxUnwrapper;
    let uv_mesh = unwrapper.unwrap(&clean_mesh, 4, atlas_size);
    on_progress(0.50, "manifold_uv", "Manifold topology repaired and conformal UV charts mapped");

    // Stage 3: Barycentric PBR Texture Atlas Baking (75%)
    let material = if spec.export_texture {
        let sampler = ProceduralPbrSampler;
        Some(bake_texture_atlas(&uv_mesh, atlas_size, atlas_size, &sampler))
    } else {
        None
    };
    on_progress(0.75, "texture_baking", "PBR albedo texture atlas baked");

    // Stage 4: Binary glTF 2.0 Packaging (100%)
    let glb_bytes = pack_glb(&uv_mesh, material.as_ref())?;
    on_progress(1.00, "gltf_pack", "Binary glTF 2.0 container assembled");

    let elapsed = start.elapsed().as_millis() as u64;

    let (aabb_min, aabb_max) = uv_mesh.compute_aabb();

    let stats = GenerationStats {
        vertex_count: uv_mesh.positions.len(),
        triangle_count: uv_mesh.indices.len() / 3,
        execution_time_ms: elapsed,
        aabb_min,
        aabb_max,
    };

    Ok((glb_bytes, stats))
}

/// Executes the full Tier-0 procedural 3D generation pipeline in pure Rust (< 50ms).
///
/// 1. Evaluates analytical SDF primitives and smooth boolean CSG blending.
/// 2. Extracts watertight 2-manifold isosurface via Paul Bourke Marching Cubes.
/// 3. Repairs topology: vertex deduplication, degenerate face removal, area-weighted normals.
/// 4. Generates non-overlapping conformal box UV coordinates with gutter padding.
/// 5. Bakes PBR albedo texture atlas and encodes to pure-Rust PNG (if `spec.export_texture` is true).
/// 6. Packs geometry, accessors, materials, and textures into compliant glTF 2.0 `.glb` bytes.
pub fn generate_procedural_3d(
    spec: &Generative3DSpec,
) -> Result<(Vec<u8>, GenerationStats), String> {
    generate_procedural_3d_with_progress(spec, |_, _, _| {})
}
