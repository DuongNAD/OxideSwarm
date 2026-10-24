//! Comprehensive test suite for OxideSwarm Milestone 2 Local 3D Generation Engine (Features F4–F10).

use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::task::{Generative3DSpec, Task, TaskRequirements, TaskSpec};
use rusty_grid_worker::gen3d::bake::{bake_texture_atlas, ProceduralPbrSampler};
use rusty_grid_worker::gen3d::gltf_pack::{pack_glb, GLB_MAGIC, GLB_VERSION};
use rusty_grid_worker::gen3d::manifold::repair_manifold_mesh;
use rusty_grid_worker::gen3d::marching_cubes::extract_isosurface;
use rusty_grid_worker::gen3d::math::Vec3;
use rusty_grid_worker::gen3d::sdf::{
    op_smin, op_subtract, op_union, sd_box, sd_capsule, sd_sphere, sd_torus, SdfPreset,
};
use rusty_grid_worker::gen3d::types::RawMesh;
use rusty_grid_worker::gen3d::uv::{MultiChartBoxUnwrapper, UvUnwrapper};
use rusty_grid_worker::job_object::JobObject;
use rusty_grid_worker::sandbox::SandboxConfig;
use rusty_grid_worker::{RunnerConfig, TaskRunner, EXIT_CODE_CANCELLED};

fn create_test_runner() -> TaskRunner {
    let temp_dir = tempfile::tempdir().expect("Failed to create tempdir");
    let sandbox_config = SandboxConfig::new(temp_dir.path().to_path_buf());
    let runner_config = RunnerConfig::new(sandbox_config);
    let mut caps = WorkerCapabilities::new("test_worker", 4, 8192, false, false, None);
    caps.has_cuda = false;
    TaskRunner::new(uuid::Uuid::new_v4(), caps, runner_config)
}

// ============================================================================
// Feature F4: Tier 0 Analytical SDF Primitives & Marching Cubes Tests
// ============================================================================

#[test]
fn test_sdf_primitives_analytical_correctness() {
    // 1. Sphere radius 1.0 at origin
    assert_eq!(sd_sphere(Vec3::ZERO, 1.0), -1.0);
    assert!((sd_sphere(Vec3::new(1.0, 0.0, 0.0), 1.0)).abs() < 1e-6);
    assert!((sd_sphere(Vec3::new(2.0, 0.0, 0.0), 1.0) - 1.0).abs() < 1e-6);

    // 2. Box half-extents (1, 1, 1)
    assert_eq!(sd_box(Vec3::ZERO, Vec3::ONE), -1.0);
    assert!((sd_box(Vec3::new(1.0, 0.0, 0.0), Vec3::ONE)).abs() < 1e-6);
    assert!((sd_box(Vec3::new(2.0, 0.0, 0.0), Vec3::ONE) - 1.0).abs() < 1e-6);

    // 3. Torus major radius 2.0, minor radius 0.5 in XZ plane
    assert!((sd_torus(Vec3::new(2.0, 0.0, 0.0), 2.0, 0.5) - (-0.5)).abs() < 1e-6);
    assert!((sd_torus(Vec3::new(2.5, 0.0, 0.0), 2.0, 0.5)).abs() < 1e-6);

    // 4. Capsule from (0, 0, 0) to (0, 2, 0) with radius 0.5
    assert!((sd_capsule(Vec3::new(0.0, 1.0, 0.0), Vec3::ZERO, Vec3::new(0.0, 2.0, 0.0), 0.5) - (-0.5)).abs() < 1e-6);
    assert!((sd_capsule(Vec3::new(0.5, 1.0, 0.0), Vec3::ZERO, Vec3::new(0.0, 2.0, 0.0), 0.5)).abs() < 1e-6);

    // 5. CSG Operators
    assert_eq!(op_union(1.0, -0.5), -0.5);
    assert_eq!(op_subtract(1.0, -0.5), 1.0);

    // Smooth min should be strictly less than sharp min
    let smin_val = op_smin(0.0, 0.0, 0.2);
    assert!((smin_val - (-0.05)).abs() < 1e-5);
}

#[test]
fn test_marching_cubes_watertightness_and_speed() {
    let preset = SdfPreset::HumanoidDummy;
    let start = std::time::Instant::now();
    let mesh = extract_isosurface(&preset, 48).expect("Marching Cubes extraction failed");
    let elapsed = start.elapsed();

    println!(
        "Marching Cubes 48^3 extracted: {} vertices, {} triangles in {:?}",
        mesh.vertex_count(),
        mesh.triangle_count(),
        elapsed
    );

    // Latency budget check (< 50ms target)
    assert!(
        elapsed < Duration::from_millis(50),
        "Marching Cubes took too long: {:?}",
        elapsed
    );

    assert!(mesh.vertex_count() > 100);
    assert!(mesh.triangle_count() > 200);

    // Check that vertices and normals contain no NaNs or Infinities
    for p in &mesh.positions {
        assert!(p[0].is_finite() && p[1].is_finite() && p[2].is_finite());
    }
    for n in &mesh.normals {
        assert!(n[0].is_finite() && n[1].is_finite() && n[2].is_finite());
        let len = (n[0] * n[0] + n[1] * n[1] + n[2] * n[2]).sqrt();
        assert!((len - 1.0).abs() < 1e-2, "Normal vector not normalized: len={len}");
    }
}

// ============================================================================
// Feature F6: Manifold Mesh Repair & Normals Tests
// ============================================================================

#[test]
fn test_manifold_deduplication_and_zero_area_filtering() {
    // Construct a test mesh with duplicate vertices and a zero-area triangle
    let positions = vec![
        [0.0, 0.0, 0.0], // 0
        [1.0, 0.0, 0.0], // 1
        [0.0, 1.0, 0.0], // 2
        [0.0, 0.0, 0.0], // 3 (duplicate of 0)
        [1.0, 0.0, 0.0], // 4 (duplicate of 1)
        [0.0, 0.0, 1.0], // 5
        [0.5, 0.0, 0.0], // 6 (collinear with 0 and 1)
    ];

    let indices = vec![
        0, 1, 2, // Valid triangle 1
        3, 4, 5, // Valid triangle 2 (uses duplicates of 0 and 1)
        0, 6, 1, // Degenerate collinear triangle (zero area)
        2, 2, 5, // Degenerate collapsed index triangle
    ];

    let raw = RawMesh::new(positions, indices);
    let repaired = repair_manifold_mesh(&raw);

    // Degenerate triangles must be eliminated
    assert_eq!(repaired.triangle_count(), 2);
    // Duplicate vertices 3 and 4 must be merged
    assert_eq!(repaired.vertex_count(), 4);

    // Normals must be populated and normalized
    assert_eq!(repaired.normals.len(), repaired.vertex_count());
    for n in &repaired.normals {
        let len = (n[0] * n[0] + n[1] * n[1] + n[2] * n[2]).sqrt();
        assert!((len - 1.0).abs() < 1e-3);
    }
}

// ============================================================================
// Feature F7: UV Parameterization & Gutter Packing Tests
// ============================================================================

#[test]
fn test_uv_unwrapper_bounds_and_split() {
    let preset = SdfPreset::TestOrb;
    let raw = extract_isosurface(&preset, 32).expect("Extraction failed");
    let clean = repair_manifold_mesh(&raw);

    let unwrapper = MultiChartBoxUnwrapper;
    let uv_mesh = unwrapper.unwrap(&clean, 4, 1024);

    assert!(!uv_mesh.uvs.is_empty());
    assert_eq!(uv_mesh.positions.len(), uv_mesh.uvs.len());
    assert_eq!(uv_mesh.positions.len(), uv_mesh.normals.len());

    // All UVs must lie strictly within [0.0, 1.0]
    for uv in &uv_mesh.uvs {
        assert!(uv[0] >= 0.0 && uv[0] <= 1.0, "UV u out of bounds: {}", uv[0]);
        assert!(uv[1] >= 0.0 && uv[1] <= 1.0, "UV v out of bounds: {}", uv[1]);
    }
}

// ============================================================================
// Feature F8: Barycentric Texture Atlas Baking & PNG Encoding Tests
// ============================================================================

#[test]
fn test_texture_baking_and_png_encoding() {
    let preset = SdfPreset::StylizedCharacter;
    let raw = extract_isosurface(&preset, 32).expect("Extraction failed");
    let clean = repair_manifold_mesh(&raw);
    let unwrapper = MultiChartBoxUnwrapper;
    let uv_mesh = unwrapper.unwrap(&clean, 4, 512);

    let sampler = ProceduralPbrSampler;
    let mat = bake_texture_atlas(&uv_mesh, 256, 256, &sampler);

    assert_eq!(mat.width, 256);
    assert_eq!(mat.height, 256);
    assert!(!mat.base_color_png.is_empty());

    // Validate standard 8-byte PNG header: \x89PNG\r\n\x1a\n
    let expected_header = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
    assert_eq!(&mat.base_color_png[0..8], &expected_header);
}

// ============================================================================
// Feature F9: glTF 2.0 Binary Container (.glb) Packager Tests
// ============================================================================

#[test]
fn test_glb_container_compliance() {
    let preset = SdfPreset::RobotTorso;
    let raw = extract_isosurface(&preset, 32).expect("Extraction failed");
    let clean = repair_manifold_mesh(&raw);
    let unwrapper = MultiChartBoxUnwrapper;
    let uv_mesh = unwrapper.unwrap(&clean, 4, 512);
    let sampler = ProceduralPbrSampler;
    let mat = bake_texture_atlas(&uv_mesh, 128, 128, &sampler);

    let glb = pack_glb(&uv_mesh, Some(&mat)).expect("GLB packing failed");

    // 1. Length must be at least 12 bytes and match container header
    assert!(glb.len() >= 12);
    let magic = u32::from_le_bytes(glb[0..4].try_into().unwrap());
    let version = u32::from_le_bytes(glb[4..8].try_into().unwrap());
    let length = u32::from_le_bytes(glb[8..12].try_into().unwrap());

    assert_eq!(magic, GLB_MAGIC);
    assert_eq!(version, GLB_VERSION);
    assert_eq!(length as usize, glb.len());

    // 2. Chunk 0 must be JSON (type 0x4E4F534A)
    let json_len = u32::from_le_bytes(glb[12..16].try_into().unwrap()) as usize;
    let json_type = u32::from_le_bytes(glb[16..20].try_into().unwrap());
    assert_eq!(json_type, 0x4E4F534A);
    assert_eq!(json_len % 4, 0, "JSON chunk length must be 4-byte aligned");

    let json_str = std::str::from_utf8(&glb[20..20 + json_len]).expect("Invalid JSON UTF-8");
    let parsed: serde_json::Value = serde_json::from_str(json_str).expect("Failed to parse glTF JSON");

    // 3. Mandatory min and max on POSITION accessor
    let accessors = parsed["accessors"].as_array().expect("accessors not an array");
    let pos_acc = &accessors[0];
    assert!(pos_acc["min"].is_array(), "POSITION accessor missing min bounding box");
    assert!(pos_acc["max"].is_array(), "POSITION accessor missing max bounding box");

    // 4. Chunk 1 must be BIN (type 0x004E4942)
    let bin_offset = 20 + json_len;
    let bin_len = u32::from_le_bytes(glb[bin_offset..bin_offset + 4].try_into().unwrap()) as usize;
    let bin_type = u32::from_le_bytes(glb[bin_offset + 4..bin_offset + 8].try_into().unwrap());
    assert_eq!(bin_type, 0x004E4942);
    assert_eq!(bin_len % 4, 0, "BIN chunk length must be 4-byte aligned");
    assert_eq!(12 + 8 + json_len + 8 + bin_len, glb.len());

    let out_path = std::path::PathBuf::from(r"E:\teamwork_projects\OxideSwarm\target\tier0_test.glb");
    if let Some(parent) = out_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    std::fs::write(&out_path, &glb).expect("Failed to write tier0_test.glb");
}

// ============================================================================
// Feature F10: Win32 Job Object Supervision Tests
// ============================================================================

#[test]
fn test_job_object_creation_and_lifecycle() {
    let job = JobObject::create().expect("Failed to create JobObject");

    // Spawn a dummy sleep process
    #[cfg(windows)]
    let mut cmd = std::process::Command::new("powershell");
    #[cfg(windows)]
    cmd.args(["-Command", "Start-Sleep -Seconds 10"]);

    #[cfg(not(windows))]
    let mut cmd = std::process::Command::new("sleep");
    #[cfg(not(windows))]
    cmd.arg("10");

    let mut child = cmd.spawn().expect("Failed to spawn dummy child process");
    let pid = child.id();

    let mut job = job;
    job.assign_pid(pid).expect("Failed to assign PID to JobObject");

    // Terminate via JobObject
    job.terminate(130).expect("Failed to terminate JobObject");
    let status = child.wait().expect("Failed to wait on child");
    assert!(!status.success());
}

// ============================================================================
// End-to-End TaskRunner Generative3D Integration Tests
// ============================================================================

#[tokio::test]
async fn test_task_runner_generative3d_sdf_execution() {
    let runner = create_test_runner();
    let spec = Generative3DSpec::new_prompt("cybernetic mechanical warrior")
        .with_quality("fast")
        .with_engine("sdf");

    let task = Task::new(TaskSpec::Generative3D(spec), TaskRequirements::default());

    let result = runner.execute_task(&task, None, None).await;

    assert!(result.is_success(), "Task failed: {:?}", result.error);
    assert!(result.asset_bytes.is_some(), "Asset bytes missing from TaskResult");

    let asset = result.asset_bytes.unwrap();
    assert!(asset.len() >= 12);
    let bytes = asset.as_slice();
    assert_eq!(&bytes[0..4], b"glTF");
    assert_eq!(u32::from_le_bytes(bytes[4..8].try_into().unwrap()), 2);
}

#[tokio::test]
async fn test_task_runner_generative3d_auto_degradation() {
    let runner = create_test_runner();
    let spec = Generative3DSpec::new_prompt("cute low-poly mascot")
        .with_quality("standard")
        .with_engine("auto");

    let task = Task::new(TaskSpec::Generative3D(spec), TaskRequirements::default());

    let result = runner.execute_task(&task, None, None).await;

    assert!(result.is_success(), "Task failed: {:?}", result.error);
    assert!(result.asset_bytes.is_some(), "Asset bytes missing");
    let asset = result.asset_bytes.unwrap();
    let bytes = asset.as_slice();
    assert_eq!(&bytes[0..4], b"glTF");
}

#[tokio::test]
async fn test_task_runner_generative3d_cancellation() {
    let runner = create_test_runner();
    let spec = Generative3DSpec::new_prompt("futuristic titan")
        .with_quality("high")
        .with_engine("triposr");

    let task = Task::new(TaskSpec::Generative3D(spec), TaskRequirements::default());

    let (cancel_tx, cancel_rx) = oneshot::channel();
    let child_pid = Arc::new(AtomicU32::new(0));

    // Cancel immediately
    let _ = cancel_tx.send("User cancellation requested".to_string());

    let result = runner
        .execute_task(&task, Some(child_pid), Some(cancel_rx))
        .await;

    assert_eq!(result.exit_code, EXIT_CODE_CANCELLED);
}
