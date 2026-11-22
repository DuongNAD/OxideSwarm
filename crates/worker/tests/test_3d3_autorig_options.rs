#![cfg(feature = "3d")]
#![allow(clippy::field_reassign_with_default)]

//! Comprehensive integration tests for Milestone M4-3D3 (`3D-3`):
//! - Honest AutoRig options validation (`rig_preset`, `embed_animations`, `preferred_provider`)
//! - Distinct `biped` (15), `quadruped` (19), and `generic` (5) skeletons & animations
//! - `engines/autorig/blender_rig.py` option validation & real `turnaround` keyframes
//! - Magic-byte mesh format detection (`.glb`, `.gltf`, `.obj`) and Blender routing
//! - Explicit `"blender"` refusal vs `"auto"` fallback & `device_name`/stdout provider reporting
//! - `<= 1 s` timeout & cancellation propagation across Blender, Builtin AutoRig, and Tier-0 SDF

use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;
use tempfile::TempDir;
use tokio::sync::oneshot;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::task::{
    AutoRigSpec, Bytes, Generative3DSpec, Task, TaskRequirements, TaskSpec,
};
use rusty_grid_worker::autorig::{
    auto_rig_mesh, detect_mesh_format, load_mesh_from_bytes, ClipSynthesizer, MeshFormat, RigPreset,
};
use rusty_grid_worker::sandbox::SandboxConfig;
use rusty_grid_worker::{
    RunnerConfig, TaskRunner, EXIT_CODE_CANCELLED, EXIT_CODE_GENERAL_ERROR, EXIT_CODE_TIMEOUT,
};

mod common;

use common::{
    assert_all_alive, assert_all_dead_async, create_fake_blender_executable, get_python_exe,
    poll_pids_async, EnvLock,
};

fn create_isolated_runner(blender_path: Option<String>) -> (TaskRunner, TempDir) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let sandbox_config = SandboxConfig::new(temp_dir.path().to_path_buf());
    let runner_config = RunnerConfig::new(sandbox_config).with_blender_path(blender_path);
    let mut caps = WorkerCapabilities::new("3d3_test_worker", 4, 8192, false, false, None);
    caps.has_blender = runner_config.blender_path.is_some();
    let runner = TaskRunner::new(uuid::Uuid::new_v4(), caps, runner_config);
    (runner, temp_dir)
}

fn minimal_valid_obj() -> Vec<u8> {
    // 10 vertices & 8 faces forming an elongated prism suitable for landmark extraction
    let obj = "\
v -0.3 0.0 -0.6
v 0.3 0.0 -0.6
v 0.3 0.0 0.6
v -0.3 0.0 0.6
v -0.3 1.0 -0.6
v 0.3 1.0 -0.6
v 0.3 1.0 0.6
v -0.3 1.0 0.6
v 0.0 1.4 0.8
v 0.0 0.8 -0.9
f 1 2 3
f 1 3 4
f 5 6 7
f 5 7 8
f 1 2 6
f 1 6 5
f 2 3 7
f 3 4 8
";
    obj.as_bytes().to_vec()
}

fn generate_sdf_glb() -> Vec<u8> {
    let spec = Generative3DSpec::new_prompt("quadruped wolf")
        .with_quality("fast")
        .with_engine("sdf");
    let (glb, _) = rusty_grid_worker::gen3d::generate_procedural_3d(&spec).expect("sdf glb");
    glb
}

fn minimal_valid_gltf_json() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "asset": { "version": "2.0" },
        "meshes": [{ "primitives": [] }]
    }))
    .unwrap()
}

fn parse_glb_json(glb: &[u8]) -> Value {
    assert!(glb.len() >= 20, "GLB too short");
    assert_eq!(&glb[0..4], b"glTF", "GLB magic mismatch");
    let json_len = u32::from_le_bytes([glb[12], glb[13], glb[14], glb[15]]) as usize;
    serde_json::from_slice(&glb[20..20 + json_len]).expect("valid glTF JSON")
}

fn extract_skin_joint_names(gltf: &Value) -> Vec<String> {
    let nodes = gltf["nodes"].as_array().expect("nodes");
    let skin_joints = gltf["skins"][0]["joints"].as_array().expect("skin joints");
    skin_joints
        .iter()
        .map(|idx| {
            nodes[idx.as_u64().unwrap() as usize]["name"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect()
}

fn create_fake_blender_outputting_glb(
    dir: &Path,
    recorded_args_path: &Path,
    valid_glb: &[u8],
) -> std::path::PathBuf {
    let glb_src = dir.join("canned_rigged.glb");
    std::fs::write(&glb_src, valid_glb).expect("write canned glb");

    let args_str = recorded_args_path.display().to_string().replace('\\', "/");
    let glb_src_str = glb_src.display().to_string().replace('\\', "/");
    let script = format!(
        r#"
import json, shutil, sys

args = sys.argv[1:]
if "--version" in args:
    print("Blender 4.5.4 LTS", flush=True)
    sys.exit(0)

with open("{args_str}", "w") as f:
    json.dump(args, f)

out_path = None
for i, a in enumerate(args):
    if a == "--output" and i + 1 < len(args):
        out_path = args[i + 1]

if out_path:
    shutil.copyfile("{glb_src_str}", out_path)
print("[PROGRESS: 1.00] Fake Blender finished", flush=True)
sys.exit(0)
"#
    );
    create_fake_blender_executable(dir, &script)
}

#[tokio::test]
async fn test_upfront_refusal_of_invalid_options_in_core_and_worker() {
    let obj = minimal_valid_obj();

    // 1. Unknown/empty rig_preset refused in TaskSpec::validate(), auto_rig_mesh, and TaskRunner
    for bad_preset in ["dragon", "", "   ", "centaur"] {
        let spec = AutoRigSpec::new(Bytes::from(obj.clone()), bad_preset);
        let err = TaskSpec::AutoRig(spec.clone())
            .validate()
            .expect_err("TaskSpec::validate must refuse invalid rig_preset");
        assert!(
            err.to_lowercase().contains("rig_preset"),
            "Unexpected error message: {err}"
        );

        assert!(
            auto_rig_mesh(&spec).is_err(),
            "auto_rig_mesh must refuse invalid preset '{bad_preset}'"
        );

        let (runner, _dir) = create_isolated_runner(None);
        let task = Task::new(TaskSpec::AutoRig(spec), TaskRequirements::generic(1, 10));
        let res = runner.execute_task(&task, None, None).await;
        assert_eq!(res.exit_code, EXIT_CODE_GENERAL_ERROR);
    }

    // 2. Unknown/empty embed_animations clip refused everywhere; empty Vec allowed
    for bad_clip in ["run", "jump", "", "  "] {
        let spec = AutoRigSpec::new(Bytes::from(obj.clone()), "biped")
            .with_animations(vec!["idle".into(), bad_clip.into()]);
        assert!(TaskSpec::AutoRig(spec.clone()).validate().is_err());
        assert!(auto_rig_mesh(&spec).is_err());

        let (runner, _dir) = create_isolated_runner(None);
        let task = Task::new(TaskSpec::AutoRig(spec), TaskRequirements::generic(1, 10));
        let res = runner.execute_task(&task, None, None).await;
        assert_eq!(res.exit_code, EXIT_CODE_GENERAL_ERROR);
    }
    let zero_clips_spec =
        AutoRigSpec::new(Bytes::from(obj.clone()), "biped").with_animations(vec![]);
    assert!(TaskSpec::AutoRig(zero_clips_spec.clone())
        .validate()
        .is_ok());
    assert!(auto_rig_mesh(&zero_clips_spec).is_ok());

    // 3. Unknown/empty preferred_provider refused everywhere; "blender" refused in sync-only auto_rig_mesh
    for bad_prov in ["maya", "", "  ", "rig_net"] {
        let spec = AutoRigSpec::new(Bytes::from(obj.clone()), "biped").with_provider(bad_prov);
        assert!(TaskSpec::AutoRig(spec.clone()).validate().is_err());
        assert!(auto_rig_mesh(&spec).is_err());

        let (runner, _dir) = create_isolated_runner(None);
        let task = Task::new(TaskSpec::AutoRig(spec), TaskRequirements::generic(1, 10));
        let res = runner.execute_task(&task, None, None).await;
        assert_eq!(res.exit_code, EXIT_CODE_GENERAL_ERROR);
    }

    let sync_blender_spec = AutoRigSpec::new(Bytes::from(obj), "biped").with_provider("blender");
    assert!(
        auto_rig_mesh(&sync_blender_spec).is_err(),
        "Sync-only auto_rig_mesh must refuse 'blender' provider instead of silently running builtin"
    );
}

#[test]
fn test_distinct_biped_quadruped_generic_skeletons_and_animations() {
    let obj = minimal_valid_obj();
    let clips = vec!["idle".into(), "walk".into(), "turnaround".into()];

    let biped_glb = auto_rig_mesh(
        &AutoRigSpec::new(Bytes::from(obj.clone()), "biped").with_animations(clips.clone()),
    )
    .expect("biped glb");
    let quad_glb = auto_rig_mesh(
        &AutoRigSpec::new(Bytes::from(obj.clone()), "quadruped").with_animations(clips.clone()),
    )
    .expect("quadruped glb");
    let gen_glb =
        auto_rig_mesh(&AutoRigSpec::new(Bytes::from(obj), "generic").with_animations(clips))
            .expect("generic glb");

    let biped_json = parse_glb_json(&biped_glb);
    let quad_json = parse_glb_json(&quad_glb);
    let gen_json = parse_glb_json(&gen_glb);

    let biped_names = extract_skin_joint_names(&biped_json);
    let quad_names = extract_skin_joint_names(&quad_json);
    let gen_names = extract_skin_joint_names(&gen_json);

    assert_eq!(biped_names.len(), 15);
    assert_eq!(biped_names[0], "Hips");

    assert_eq!(quad_names.len(), 19);
    assert_eq!(quad_names[0], "Pelvis");
    assert!(quad_names.contains(&"Tail_01".to_string()));
    assert!(quad_names.contains(&"Tail_02".to_string()));
    assert!(quad_names.contains(&"HindUpperLeg_L".to_string()));
    assert!(quad_names.contains(&"HindPaw_R".to_string()));
    assert!(quad_names.contains(&"FrontUpperLeg_L".to_string()));
    assert!(quad_names.contains(&"FrontPaw_R".to_string()));

    assert_eq!(gen_names, vec!["Root", "Spine", "Chest", "Neck", "Head"]);

    // Verify all 3 clips have non-empty channels targeting valid joint indices
    for (preset_name, json, max_j) in [
        ("biped", &biped_json, 15usize),
        ("quadruped", &quad_json, 19usize),
        ("generic", &gen_json, 5usize),
    ] {
        let anims = json["animations"].as_array().expect("animations");
        assert_eq!(anims.len(), 3, "{preset_name} must embed 3 clips");
        for anim in anims {
            let channels = anim["channels"].as_array().unwrap();
            assert!(
                !channels.is_empty(),
                "{preset_name} clip must have channels"
            );
            for ch in channels {
                let node = ch["target"]["node"].as_u64().unwrap() as usize;
                assert!(
                    node < max_j,
                    "{preset_name} animation channel targeted out-of-range joint {node}"
                );
            }
        }
    }

    // Direct ClipSynthesizer::synthesize_all_checked must also refuse unknown clips
    let raw = load_mesh_from_bytes(&minimal_valid_obj()).unwrap();
    let lm = rusty_grid_worker::autorig::extract_landmarks(&raw, RigPreset::Quadruped).unwrap();
    let arm = rusty_grid_worker::autorig::build_standard_armature(&lm, RigPreset::Quadruped);
    assert!(ClipSynthesizer::synthesize_all_checked(&["run".into()], lm.height, &arm).is_err());
}

#[test]
fn test_blender_rig_py_validates_options_and_builds_distinct_rigs_and_turnaround() {
    let dir = tempfile::tempdir().expect("tempdir");
    let py = get_python_exe();
    let script_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("engines/autorig/blender_rig.py");
    assert!(script_path.is_file(), "blender_rig.py must exist");

    // Mock `bpy` and `mathutils` to exercise `blender_rig.py` end-to-end without host Blender.
    let mock_dir = dir.path().join("mock_py");
    std::fs::create_dir_all(&mock_dir).unwrap();
    std::fs::write(
        mock_dir.join("mathutils.py"),
        r#"
class Vector:
    def __init__(self, coords):
        self.x, self.y, self.z = float(coords[0]), float(coords[1]), float(coords[2])
    def __rmatmul__(self, other):
        return self
"#,
    )
    .unwrap();
    std::fs::write(
        mock_dir.join("bpy.py"),
        r#"
import json, os

class _Bone:
    def __init__(self, name):
        self.name = name
        self.head = (0.0, 0.0, 0.0)
        self.tail = (0.0, 0.0, 1.0)
        self.parent = None

class _EditBones(list):
    def new(self, name):
        b = _Bone(name)
        self.append(b)
        return b

class _PoseBone:
    def __init__(self, name):
        self.name = name
        self.location = (0.0, 0.0, 0.0)
        self.rotation_mode = 'XYZ'
        self.rotation_euler = (0.0, 0.0, 0.0)
    def keyframe_insert(self, data_path, frame):
        cur = _STATE["active_action"]
        cur["keyframes"].append((self.name, data_path, frame, tuple(self.location), tuple(self.rotation_euler)))

class _Pose:
    def __init__(self, names):
        self.bones = {n: _PoseBone(n) for n in names}

class _Obj:
    def __init__(self, name, obj_type, data=None):
        self.name = name
        self.type = obj_type
        self.data = data
        self.bound_box = [(-0.5,-0.5,0.0),(0.5,-0.5,0.0),(0.5,0.5,0.0),(-0.5,0.5,0.0),
                          (-0.5,-0.5,2.0),(0.5,-0.5,2.0),(0.5,0.5,2.0),(-0.5,0.5,2.0)]
        self.matrix_world = 1
        self.animation_data = None
        self.pose = None
    def select_set(self, _): pass
    def animation_data_create(self):
        self.animation_data = type("Anim", (), {"action": None})()

_STATE = {"objects": [], "actions": [], "active_action": None}

class _Actions(list):
    def new(self, name):
        act = {"name": name, "keyframes": []}
        self.append(act)
        _STATE["active_action"] = act
        return act

class _Armatures(list):
    def new(self, name):
        arm = type("Arm", (), {"name": name, "edit_bones": _EditBones()})()
        self.append(arm)
        return arm

class _Objects(list):
    def new(self, name, data):
        obj = _Obj(name, 'ARMATURE', data)
        return obj
    def link(self, obj):
        _STATE["objects"].append(obj)

data = type("Data", (), {
    "armatures": _Armatures(),
    "objects": _Objects(),
    "actions": _Actions(),
})()

class _ViewLayer:
    def __init__(self):
        self.objects = type("VLO", (), {"active": None})()

class _Scene:
    @property
    def objects(self):
        return _STATE["objects"]
    @property
    def collection(self):
        return type("Col", (), {"objects": data.objects})()

context = type("Ctx", (), {"scene": _Scene(), "view_layer": _ViewLayer()})()

def _reset(use_empty=True):
    _STATE["objects"] = []
    data.actions.clear()
    data.armatures.clear()

def _import_mesh(filepath):
    _STATE["objects"].append(_Obj("Imported", 'MESH'))

def _mode_set(mode):
    obj = context.view_layer.objects.active
    if mode == 'OBJECT' and obj and obj.type == 'ARMATURE':
        obj.pose = _Pose([b.name for b in obj.data.edit_bones])

def _export_gltf(filepath, **kwargs):
    report = {
        "bones": [b.name for b in data.armatures[0].edit_bones],
        "actions": {a["name"]: len(a["keyframes"]) for a in data.actions},
        "turnaround_samples": next((a["keyframes"] for a in data.actions if a["name"] == "turnaround"), []),
    }
    with open(filepath + ".report.json", "w") as f:
        json.dump(report, f)
    with open(filepath, "wb") as f:
        f.write(b"glTF\x02\x00\x00\x00\x14\x00\x00\x00")

ops = type("Ops", (), {
    "wm": type("WM", (), {"read_factory_settings": staticmethod(_reset), "obj_import": staticmethod(_import_mesh)})(),
    "import_scene": type("IS", (), {"gltf": staticmethod(_import_mesh), "obj": staticmethod(_import_mesh)})(),
    "object": type("ObjOps", (), {
        "select_all": staticmethod(lambda action='DESELECT': None),
        "join": staticmethod(lambda: None),
        "mode_set": staticmethod(_mode_set),
        "parent_set": staticmethod(lambda type='ARMATURE_AUTO': None),
    })(),
    "export_scene": type("ES", (), {"gltf": staticmethod(_export_gltf)})(),
})()
"#,
    )
    .unwrap();

    let run_driver = |preset: &str, animations: &str, out_file: &Path| {
        let mut cmd = Command::new(&py);
        cmd.arg(&script_path)
            .arg("--")
            .arg("--input")
            .arg("mesh.obj")
            .arg("--output")
            .arg(out_file)
            .arg("--preset")
            .arg(preset)
            .arg("--animations")
            .arg(animations)
            .env("PYTHONPATH", &mock_dir);
        cmd.output().expect("run blender_rig.py")
    };

    // 1. Unknown preset rejected with non-zero exit
    let bad_preset_out = run_driver("dragon", "idle", &dir.path().join("bad_p.glb"));
    assert!(!bad_preset_out.status.success());
    let stderr_p = String::from_utf8_lossy(&bad_preset_out.stderr);
    assert!(stderr_p.contains("rig_preset"), "stderr: {stderr_p}");

    // 2. Unknown clip rejected with non-zero exit
    let bad_clip_out = run_driver("biped", "idle,run", &dir.path().join("bad_c.glb"));
    assert!(!bad_clip_out.status.success());
    let stderr_c = String::from_utf8_lossy(&bad_clip_out.stderr);
    assert!(stderr_c.contains("animation clip"), "stderr: {stderr_c}");

    // 3. Quadruped + turnaround produces 19 quadruped bones & non-empty turnaround keyframes
    let quad_glb = dir.path().join("quad.glb");
    let quad_out = run_driver("quadruped", "idle,walk,turnaround", &quad_glb);
    assert!(
        quad_out.status.success(),
        "quadruped blender_rig.py failed: {}",
        String::from_utf8_lossy(&quad_out.stderr)
    );
    let quad_report: Value = serde_json::from_str(
        &std::fs::read_to_string(format!("{}.report.json", quad_glb.display())).unwrap(),
    )
    .unwrap();
    let bones = quad_report["bones"].as_array().unwrap();
    assert_eq!(bones.len(), 19);
    assert_eq!(bones[0], "Pelvis");
    assert!(bones.iter().any(|b| b == "Tail_01"));
    assert!(bones.iter().any(|b| b == "HindUpperLeg_L"));
    assert!(bones.iter().any(|b| b == "FrontUpperLeg_R"));
    assert!(quad_report["actions"]["idle"].as_u64().unwrap() > 0);
    assert!(quad_report["actions"]["walk"].as_u64().unwrap() > 0);
    assert!(quad_report["actions"]["turnaround"].as_u64().unwrap() >= 97);

    // Verify turnaround keyframes actually rotate root around Z up to ~2*pi
    let samples = quad_report["turnaround_samples"].as_array().unwrap();
    let max_yaw = samples
        .iter()
        .filter(|s| s[0] == "Pelvis" && s[1] == "rotation_euler")
        .map(|s| s[4][2].as_f64().unwrap())
        .fold(0.0f64, f64::max);
    assert!(
        max_yaw > 5.5,
        "Expected ~2*pi turnaround yaw rotation, got {max_yaw}"
    );

    // 4. Generic preset produces 5 axial bones
    let gen_glb = dir.path().join("gen.glb");
    let gen_out = run_driver("generic", "turnaround", &gen_glb);
    assert!(gen_out.status.success());
    let gen_report: Value = serde_json::from_str(
        &std::fs::read_to_string(format!("{}.report.json", gen_glb.display())).unwrap(),
    )
    .unwrap();
    assert_eq!(
        gen_report["bones"],
        serde_json::json!(["Root", "Spine", "Chest", "Neck", "Head"])
    );
    assert!(gen_report["actions"]["turnaround"].as_u64().unwrap() >= 97);
}

#[tokio::test]
async fn test_magic_byte_detection_and_blender_extension_routing() {
    let glb = generate_sdf_glb();
    let obj = minimal_valid_obj();
    let gltf = minimal_valid_gltf_json();
    let mut bom_gltf = vec![0xEF, 0xBB, 0xBF, b' ', b'\n'];
    bom_gltf.extend_from_slice(&gltf);

    assert_eq!(detect_mesh_format(&glb).unwrap(), MeshFormat::Glb);
    assert_eq!(detect_mesh_format(&obj).unwrap(), MeshFormat::Obj);
    assert_eq!(detect_mesh_format(&gltf).unwrap(), MeshFormat::Gltf);
    assert_eq!(detect_mesh_format(&bom_gltf).unwrap(), MeshFormat::Gltf);

    // Reject empty, truncated GLB, bogus JSON, binary garbage, and 0-face OBJ
    assert!(detect_mesh_format(b"").is_err());
    assert!(detect_mesh_format(b"glTF\x02\x00").is_err());
    assert!(detect_mesh_format(b"{ \"foo\": 1 }").is_err());
    assert!(detect_mesh_format(b"{ broken json").is_err());
    assert!(detect_mesh_format(b"\x00\x01\x02\x03\xff\xfe").is_err());
    assert!(detect_mesh_format(b"v 0 0 0\nv 1 0 0\n").is_err());

    // Pure-Rust loader explicitly rejects text .gltf JSON with helpful diagnostic
    let gltf_err = load_mesh_from_bytes(&gltf).expect_err("pure-Rust loader must reject .gltf");
    assert!(gltf_err.contains(".gltf") && gltf_err.contains("blender"));

    // Route .obj, .gltf, and .glb through fake Blender and verify --input extension + device_name + stdout
    let mut env = EnvLock::lock().await;
    let iso_home = tempfile::tempdir().unwrap();
    env.set("HOME", &iso_home.path().to_string_lossy());
    env.set("USERPROFILE", &iso_home.path().to_string_lossy());
    env.set("TMPDIR", &iso_home.path().to_string_lossy());

    let dir = tempfile::tempdir().unwrap();
    let args_log = dir.path().join("recorded_args.json");
    let fake_blender = create_fake_blender_outputting_glb(dir.path(), &args_log, &glb);
    let (runner, _sbox) = create_isolated_runner(Some(fake_blender.to_string_lossy().to_string()));

    for (label, bytes, expected_ext) in [
        ("obj", obj.as_slice(), ".obj"),
        ("gltf", gltf.as_slice(), ".gltf"),
        ("glb", glb.as_slice(), ".glb"),
    ] {
        let _ = std::fs::remove_file(&args_log);
        let spec = AutoRigSpec::new(Bytes::copy_from_slice(bytes), "quadruped")
            .with_animations(vec!["idle".into(), "turnaround".into()])
            .with_provider("blender");
        let task = Task::new(TaskSpec::AutoRig(spec), TaskRequirements::generic(1, 15));
        let res = runner.execute_task(&task, None, None).await;
        assert!(
            res.is_success(),
            "Fake Blender failed on {label}: {:?}",
            res.error
        );
        assert_eq!(
            res.device_name.as_deref(),
            Some("OxideSwarm-AutoRig-Blender")
        );
        let stdout_str = String::from_utf8_lossy(res.stdout.as_slice());
        assert!(
            stdout_str.contains("provider: blender"),
            "stdout missing provider: blender -> {stdout_str}"
        );

        let recorded: Vec<String> =
            serde_json::from_str(&std::fs::read_to_string(&args_log).unwrap()).unwrap();
        assert!(recorded.contains(&"--python-exit-code".to_string()));
        let input_arg = recorded
            .windows(2)
            .find(|w| w[0] == "--input")
            .map(|w| w[1].as_str())
            .expect("--input arg");
        assert!(
            input_arg.ends_with(expected_ext),
            "Expected --input ending with {expected_ext}, got {input_arg}"
        );
        let preset_arg = recorded
            .windows(2)
            .find(|w| w[0] == "--preset")
            .map(|w| w[1].as_str())
            .expect("--preset arg");
        assert_eq!(preset_arg, "quadruped");
    }
}

#[tokio::test]
async fn test_provider_reporting_and_invalid_blender_output_refusal() {
    let mut env = EnvLock::lock().await;
    let iso_home = tempfile::tempdir().unwrap();
    env.set("HOME", &iso_home.path().to_string_lossy());
    env.set("USERPROFILE", &iso_home.path().to_string_lossy());
    env.set("TMPDIR", &iso_home.path().to_string_lossy());

    let dir = tempfile::tempdir().unwrap();
    let corrupt_glb_script = r#"
import sys
args = sys.argv[1:]
if "--version" in args:
    print("Blender 4.5.4 LTS", flush=True)
    sys.exit(0)
for i, a in enumerate(args):
    if a == "--output" and i + 1 < len(args):
        with open(args[i + 1], "wb") as f:
            f.write(b"NOT_A_VALID_GLB_FILE")
sys.exit(0)
"#;
    let bad_blender = create_fake_blender_executable(dir.path(), corrupt_glb_script);
    let (runner, _sbox) = create_isolated_runner(Some(bad_blender.to_string_lossy().to_string()));
    let obj = minimal_valid_obj();

    // 1. None & "builtin" must use Builtin and report Builtin device_name/stdout (never invoke Blender)
    for prov in [None, Some("builtin")] {
        let mut spec = AutoRigSpec::new(Bytes::from(obj.clone()), "biped");
        spec.preferred_provider = prov.map(str::to_string);
        let task = Task::new(TaskSpec::AutoRig(spec), TaskRequirements::generic(1, 15));
        let res = runner.execute_task(&task, None, None).await;
        assert!(res.is_success(), "builtin failed: {:?}", res.error);
        assert_eq!(
            res.device_name.as_deref(),
            Some("OxideSwarm-AutoRig-Builtin")
        );
        assert!(String::from_utf8_lossy(res.stdout.as_slice()).contains("provider: builtin"));
    }

    // 2. Explicit "blender" must fail if Blender writes invalid GLB bytes
    let strict_spec = AutoRigSpec::new(Bytes::from(obj.clone()), "biped").with_provider("blender");
    let strict_res = runner
        .execute_task(
            &Task::new(
                TaskSpec::AutoRig(strict_spec),
                TaskRequirements::generic(1, 15),
            ),
            None,
            None,
        )
        .await;
    assert!(!strict_res.is_success());
    assert_ne!(strict_res.exit_code, 0);

    // 3. "auto" falls back to Builtin when Blender produces invalid GLB and reports Builtin
    let auto_spec = AutoRigSpec::new(Bytes::from(obj), "biped").with_provider("auto");
    let auto_res = runner
        .execute_task(
            &Task::new(
                TaskSpec::AutoRig(auto_spec),
                TaskRequirements::generic(1, 15),
            ),
            None,
            None,
        )
        .await;
    assert!(auto_res.is_success());
    assert_eq!(
        auto_res.device_name.as_deref(),
        Some("OxideSwarm-AutoRig-Builtin")
    );
    assert!(String::from_utf8_lossy(auto_res.stdout.as_slice()).contains("provider: builtin"));
}

#[tokio::test]
async fn test_blender_timeout_and_cancellation_within_1s_and_no_fallback() {
    let mut env = EnvLock::lock().await;
    let iso_home = tempfile::tempdir().unwrap();
    env.set("HOME", &iso_home.path().to_string_lossy());
    env.set("USERPROFILE", &iso_home.path().to_string_lossy());
    env.set("TMPDIR", &iso_home.path().to_string_lossy());
    env.set("RUSTY_GRID_BLENDER_PATH", "");
    let dir = tempfile::tempdir().unwrap();
    let pids_timeout = dir.path().join("pids_timeout.txt");
    let pids_cancel = dir.path().join("pids_cancel.txt");

    let make_sleeping_blender = |pids_path: &Path| {
        let p = pids_path.display().to_string().replace('\\', "/");
        format!(
            r#"
import os, subprocess, sys, time
if "--version" in sys.argv[1:]:
    print("Blender 4.5.4 LTS", flush=True)
    sys.exit(0)
with open("{p}", "a") as f:
    f.write(f"PID:{{os.getpid()}}\n")
    f.flush()
gc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
with open("{p}", "a") as f:
    f.write(f"PID:{{gc.pid}}\n")
    f.flush()
time.sleep(30.0)
"#
        )
    };

    // A. Non-60s timeout propagation (timeout_secs = 1) with "auto" must time out quickly and NOT fall back
    let dir_a = dir.path().join("a");
    std::fs::create_dir_all(&dir_a).unwrap();
    let fake_a = create_fake_blender_executable(&dir_a, &make_sleeping_blender(&pids_timeout));
    let (runner_a, _sbox_a) = create_isolated_runner(Some(fake_a.to_string_lossy().to_string()));

    let spec_a = AutoRigSpec::new(Bytes::from(minimal_valid_obj()), "biped").with_provider("auto");
    let mut req_a = TaskRequirements::generic(1, 1);
    req_a.timeout_secs = 1;
    let task_a = Task::new(TaskSpec::AutoRig(spec_a), req_a);
    let child_pid_a = Arc::new(AtomicU32::new(0));
    let t0 = Instant::now();
    let res_a = runner_a
        .execute_task_with_checkpoint(&task_a, None, None, Some(Arc::clone(&child_pid_a)), None)
        .await;
    let elapsed_a = t0.elapsed();

    assert_eq!(res_a.exit_code, EXIT_CODE_TIMEOUT);
    assert!(
        elapsed_a < Duration::from_secs(3),
        "1s Blender timeout must NOT wait 60s (took {elapsed_a:?})"
    );
    assert!(child_pid_a.load(Ordering::Relaxed) > 0);
    let pids_a = poll_pids_async(&pids_timeout, 2, Duration::from_secs(2)).await;
    assert_all_dead_async(&pids_a, Duration::from_secs(3)).await;

    // B. Mid-flight cancel with "auto" must settle <= 1s after signal and NOT fall back to Builtin
    let dir_b = dir.path().join("b");
    std::fs::create_dir_all(&dir_b).unwrap();
    let fake_b = create_fake_blender_executable(&dir_b, &make_sleeping_blender(&pids_cancel));
    let (runner_b, _sbox_b) = create_isolated_runner(Some(fake_b.to_string_lossy().to_string()));

    let spec_b = AutoRigSpec::new(Bytes::from(minimal_valid_obj()), "biped").with_provider("auto");
    let task_b = Task::new(TaskSpec::AutoRig(spec_b), TaskRequirements::generic(1, 60));
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let child_pid_b = Arc::new(AtomicU32::new(0));
    let child_pid_b_clone = Arc::clone(&child_pid_b);

    let handle = tokio::spawn(async move {
        runner_b
            .execute_task_with_checkpoint(
                &task_b,
                None,
                None,
                Some(child_pid_b_clone),
                Some(cancel_rx),
            )
            .await
    });

    let pids_b = poll_pids_async(&pids_cancel, 2, Duration::from_secs(10)).await;
    let mut sys = sysinfo::System::new();
    assert_all_alive(&mut sys, &pids_b);

    let t_cancel = Instant::now();
    let _ = cancel_tx.send("cancel_3d3_test".to_string());
    let res_b = handle.await.expect("join");
    let cancel_elapsed = t_cancel.elapsed();

    assert_eq!(res_b.exit_code, EXIT_CODE_CANCELLED);
    assert!(
        cancel_elapsed <= Duration::from_millis(1500),
        "Blender cancel should settle within ~1s (took {cancel_elapsed:?})"
    );
    assert_all_dead_async(&pids_b, Duration::from_secs(3)).await;
}

#[tokio::test]
async fn test_tier0_sdf_and_builtin_autorig_timeout_cancel_within_1s() {
    let (runner, _sbox) = create_isolated_runner(None);
    let runner = Arc::new(runner);

    // 1. Dropping cancel_tx without sending must NOT falsely cancel Tier-0 SDF or Builtin AutoRig
    {
        let (tx, rx) = oneshot::channel::<String>();
        drop(tx);
        let sdf_fast = Generative3DSpec::new_prompt("cube")
            .with_engine("sdf")
            .with_quality("fast");
        let task = Task::new(
            TaskSpec::Generative3D(sdf_fast),
            TaskRequirements::generic(1, 30),
        );
        let res = runner
            .execute_task_with_checkpoint(&task, None, None, None, Some(rx))
            .await;
        assert!(
            res.is_success(),
            "Dropped cancel_tx must not cancel SDF: {:?}",
            res.error
        );
    }
    {
        let (tx, rx) = oneshot::channel::<String>();
        drop(tx);
        let rig_spec =
            AutoRigSpec::new(Bytes::from(minimal_valid_obj()), "biped").with_provider("builtin");
        let task = Task::new(
            TaskSpec::AutoRig(rig_spec),
            TaskRequirements::generic(1, 30),
        );
        let res = runner
            .execute_task_with_checkpoint(&task, None, None, None, Some(rx))
            .await;
        assert!(
            res.is_success(),
            "Dropped cancel_tx must not cancel AutoRig: {:?}",
            res.error
        );
    }

    // 2. Tier-0 SDF "high" quality mid-flight cancel triggered at 1st checkpoint settles <= 1 s
    {
        let sdf_high = Generative3DSpec::new_prompt("robot")
            .with_engine("sdf")
            .with_quality("high");
        let task = Task::new(
            TaskSpec::Generative3D(sdf_high),
            TaskRequirements::generic(1, 60),
        );
        let (cp_tx, mut cp_rx) = tokio::sync::mpsc::channel(8);
        let (cancel_tx, cancel_rx) = oneshot::channel();
        let runner_clone = Arc::clone(&runner);
        let handle = tokio::spawn(async move {
            runner_clone
                .execute_task_with_checkpoint(&task, None, Some(cp_tx), None, Some(cancel_rx))
                .await
        });
        let _ = tokio::time::timeout(Duration::from_secs(5), cp_rx.recv())
            .await
            .expect("expected first SDF checkpoint");
        let t_cancel = Instant::now();
        let _ = cancel_tx.send("cancel_sdf_high".to_string());
        let res = handle.await.expect("join");
        let elapsed = t_cancel.elapsed();
        assert_eq!(res.exit_code, EXIT_CODE_CANCELLED);
        assert!(
            elapsed <= Duration::from_secs(1),
            "Tier-0 SDF cancel took {elapsed:?} (> 1s)"
        );
    }

    // 3. Tier-0 SDF "high" quality short timeout via execute_generative_3d_workload settles <= 1 s
    {
        let sdf_high = Generative3DSpec::new_prompt("robot")
            .with_engine("sdf")
            .with_quality("high");
        let task = Task::new(
            TaskSpec::Generative3D(sdf_high.clone()),
            TaskRequirements::generic(1, 1),
        );
        let t0 = Instant::now();
        let res = runner
            .execute_generative_3d_workload(
                &task,
                &sdf_high,
                None,
                None,
                Duration::from_millis(40),
                None,
                None,
                Instant::now(),
            )
            .await;
        let elapsed = t0.elapsed();
        assert_eq!(res.exit_code, EXIT_CODE_TIMEOUT);
        assert!(
            elapsed <= Duration::from_secs(1),
            "Tier-0 SDF timeout took {elapsed:?} (> 1s)"
        );
    }

    // 4. Builtin AutoRig cancel & timeout on dense "high" SDF mesh settle <= 1 s
    let dense_glb = {
        let sdf_high_notex = Generative3DSpec::new_prompt("mech")
            .with_engine("sdf")
            .with_quality("high")
            .with_export_texture(false);
        rusty_grid_worker::gen3d::generate_procedural_3d(&sdf_high_notex)
            .unwrap()
            .0
    };
    {
        let spec =
            AutoRigSpec::new(Bytes::from(dense_glb.clone()), "quadruped").with_provider("builtin");
        let task = Task::new(TaskSpec::AutoRig(spec), TaskRequirements::generic(1, 60));
        let (cp_tx, mut cp_rx) = tokio::sync::mpsc::channel(8);
        let (cancel_tx, cancel_rx) = oneshot::channel();
        let runner_clone = Arc::clone(&runner);
        let handle = tokio::spawn(async move {
            runner_clone
                .execute_task_with_checkpoint(&task, None, Some(cp_tx), None, Some(cancel_rx))
                .await
        });
        let _ = tokio::time::timeout(Duration::from_secs(5), cp_rx.recv())
            .await
            .expect("expected first AutoRig checkpoint");
        let t_cancel = Instant::now();
        let _ = cancel_tx.send("cancel_builtin_autorig".to_string());
        let res = handle.await.expect("join");
        let elapsed = t_cancel.elapsed();
        assert_eq!(res.exit_code, EXIT_CODE_CANCELLED);
        assert!(
            elapsed <= Duration::from_secs(1),
            "Builtin AutoRig cancel took {elapsed:?} (> 1s)"
        );
    }
    {
        let spec = AutoRigSpec::new(Bytes::from(dense_glb), "quadruped").with_provider("builtin");
        let task = Task::new(
            TaskSpec::AutoRig(spec.clone()),
            TaskRequirements::generic(1, 1),
        );
        let t0 = Instant::now();
        let res = runner
            .execute_auto_rig_workload(
                &task,
                &spec,
                None,
                None,
                Duration::from_millis(15),
                None,
                None,
                Instant::now(),
            )
            .await;
        let elapsed = t0.elapsed();
        assert_eq!(res.exit_code, EXIT_CODE_TIMEOUT);
        assert!(
            elapsed <= Duration::from_secs(1),
            "Builtin AutoRig timeout took {elapsed:?} (> 1s)"
        );
    }
}

#[test]
fn test_utf8_bom_obj_preserves_first_vertex_and_rigs_cleanly() {
    // 1. Minimal 8-vertex cube OBJ with UTF-8 BOM: must not drop vertex 1 or fail with index 8 out of range
    let cube_obj = "\
v -0.5 0.0 -0.5
v 0.5 0.0 -0.5
v 0.5 0.0 0.5
v -0.5 0.0 0.5
v -0.5 1.8 -0.5
v 0.5 1.8 -0.5
v 0.5 1.8 0.5
v -0.5 1.8 0.5
f 1 2 3
f 1 3 4
f 5 6 7
f 5 7 8
f 1 2 6
f 1 6 5
f 2 3 7
f 2 7 6
f 3 4 8
f 3 8 7
f 4 1 5
f 4 5 8
";
    let mut bom_cube = vec![0xEF, 0xBB, 0xBF];
    bom_cube.extend_from_slice(cube_obj.as_bytes());
    assert_eq!(detect_mesh_format(&bom_cube).unwrap(), MeshFormat::Obj);

    let cube_mesh =
        load_mesh_from_bytes(&bom_cube).expect("8-vertex UTF-8 BOM cube OBJ must load all 8 verts");
    assert_eq!(cube_mesh.positions.len(), 8);
    assert_eq!(cube_mesh.positions[0], [-0.5, 0.0, -0.5]);

    let rigged_glb = auto_rig_mesh(&AutoRigSpec::new(Bytes::from(bom_cube), "biped"))
        .expect("8-vertex UTF-8 BOM cube OBJ must rig cleanly via auto_rig_mesh");
    let gltf = parse_glb_json(&rigged_glb);
    assert_eq!(extract_skin_joint_names(&gltf).len(), 15);

    // 2. 10-vertex UTF-8 BOM OBJ with trailing unreferenced vertices: positions[0] must not shift to vertex 2
    let mut bom_minimal_10 = vec![0xEF, 0xBB, 0xBF];
    bom_minimal_10.extend_from_slice(&minimal_valid_obj());
    assert_eq!(
        detect_mesh_format(&bom_minimal_10).unwrap(),
        MeshFormat::Obj
    );
    let minimal_mesh =
        load_mesh_from_bytes(&bom_minimal_10).expect("10-vertex UTF-8 BOM minimal OBJ must load");
    assert_eq!(minimal_mesh.positions[0], [-0.3, 0.0, -0.6]);

    let ten_vert_obj = "\
v 999.0 999.0 999.0
v 0.0 0.0 0.0
v 1.0 0.0 0.0
v 1.0 1.0 0.0
v 0.0 1.0 0.0
v 0.0 0.0 1.0
v 1.0 0.0 1.0
v 1.0 1.0 1.0
v 0.0 1.0 1.0
v 0.5 0.5 0.5
f 1 2 3
f 2 3 4
f 3 4 5
f 4 5 6
f 5 6 7
f 6 7 8
";
    let mut bom_ten = vec![0xEF, 0xBB, 0xBF];
    bom_ten.extend_from_slice(ten_vert_obj.as_bytes());
    let ten_mesh =
        load_mesh_from_bytes(&bom_ten).expect("10-vertex UTF-8 BOM OBJ must preserve vertex 1");
    assert_eq!(ten_mesh.positions[0], [999.0, 999.0, 999.0]);
}

#[test]
fn test_double_bom_and_spaces_before_bom_obj_preserves_first_vertex() {
    let cube_obj = "\
v -0.5 0.0 -0.5
v 0.5 0.0 -0.5
v 0.5 0.0 0.5
v -0.5 0.0 0.5
v -0.5 1.8 -0.5
v 0.5 1.8 -0.5
v 0.5 1.8 0.5
v -0.5 1.8 0.5
f 1 2 3
f 1 3 4
f 5 6 7
f 5 7 8
f 1 2 6
f 1 6 5
f 2 3 7
f 2 7 6
f 3 4 8
f 3 8 7
f 4 1 5
f 4 5 8
";
    let ten_vert_obj = "\
v 999.0 999.0 999.0
v 0.0 0.0 0.0
v 1.0 0.0 0.0
v 1.0 1.0 0.0
v 0.0 1.0 0.0
v 0.0 0.0 1.0
v 1.0 0.0 1.0
v 1.0 1.0 1.0
v 0.0 1.0 1.0
v 0.5 0.5 0.5
f 1 2 3
f 2 3 4
f 3 4 5
f 4 5 6
f 5 6 7
f 6 7 8
";

    let prefixes: &[(&str, &[u8])] = &[
        ("double_bom", b"\xEF\xBB\xBF\xEF\xBB\xBF"),
        ("spaces_before_bom", b"  \t\n\xEF\xBB\xBF"),
        ("mixed_ws_and_double_bom", b"  \xEF\xBB\xBF \n\xEF\xBB\xBF"),
    ];

    for (label, prefix) in prefixes {
        let mut cube = prefix.to_vec();
        cube.extend_from_slice(cube_obj.as_bytes());
        assert_eq!(
            detect_mesh_format(&cube).unwrap(),
            MeshFormat::Obj,
            "{label} cube must detect as Obj"
        );
        let loaded_cube = load_mesh_from_bytes(&cube)
            .unwrap_or_else(|e| panic!("{label} cube must load cleanly: {e}"));
        assert_eq!(loaded_cube.positions.len(), 8, "{label} cube vertex count");
        assert_eq!(
            loaded_cube.positions[0],
            [-0.5, 0.0, -0.5],
            "{label} cube must preserve vertex 1"
        );
        let rigged = auto_rig_mesh(&AutoRigSpec::new(Bytes::from(cube), "biped"))
            .unwrap_or_else(|e| panic!("{label} cube must rig cleanly: {e}"));
        let gltf = parse_glb_json(&rigged);
        assert_eq!(extract_skin_joint_names(&gltf).len(), 15);

        let mut ten = prefix.to_vec();
        ten.extend_from_slice(ten_vert_obj.as_bytes());
        assert_eq!(detect_mesh_format(&ten).unwrap(), MeshFormat::Obj);
        let loaded_ten = load_mesh_from_bytes(&ten)
            .unwrap_or_else(|e| panic!("{label} 10-vert OBJ must load cleanly: {e}"));
        assert_eq!(loaded_ten.positions.len(), 8);
        assert_eq!(
            loaded_ten.positions[0],
            [999.0, 999.0, 999.0],
            "{label} 10-vert OBJ must preserve vertex 1"
        );
    }

    // BOM-only / whitespace+BOM-only inputs must still be rejected cleanly.
    let bom_only = b"\xEF\xBB\xBF  \n\xEF\xBB\xBF";
    assert!(detect_mesh_format(bom_only).is_err());
    assert!(load_mesh_from_bytes(bom_only).is_err());
}

#[tokio::test]
async fn test_blender_provider_strips_utf8_boms_from_staged_input_files() {
    let mut env = EnvLock::lock().await;
    let iso_home = tempfile::tempdir().unwrap();
    env.set("HOME", &iso_home.path().to_string_lossy());
    env.set("USERPROFILE", &iso_home.path().to_string_lossy());
    env.set("TMPDIR", &iso_home.path().to_string_lossy());

    let dir = tempfile::tempdir().unwrap();
    let glb = generate_sdf_glb();
    let glb_src = dir.path().join("canned_rigged.glb");
    std::fs::write(&glb_src, &glb).expect("write canned glb");
    let staged_copy = dir.path().join("staged_input_copy.bin");

    let glb_src_str = glb_src.display().to_string().replace('\\', "/");
    let staged_copy_str = staged_copy.display().to_string().replace('\\', "/");
    let script = format!(
        r#"
import shutil, sys

args = sys.argv[1:]
if "--version" in args:
    print("Blender 4.5.4 LTS", flush=True)
    sys.exit(0)

in_path = None
out_path = None
for i, a in enumerate(args):
    if a == "--input" and i + 1 < len(args):
        in_path = args[i + 1]
    elif a == "--output" and i + 1 < len(args):
        out_path = args[i + 1]

if in_path:
    shutil.copyfile(in_path, "{staged_copy_str}")
if out_path:
    shutil.copyfile("{glb_src_str}", out_path)
print("[PROGRESS: 1.00] Fake Blender inspected staged input", flush=True)
sys.exit(0)
"#
    );
    let fake_blender = create_fake_blender_executable(dir.path(), &script);
    let (runner, _sbox) = create_isolated_runner(Some(fake_blender.to_string_lossy().to_string()));

    let cube_obj = b"\
v -0.5 0.0 -0.5
v 0.5 0.0 -0.5
v 0.5 0.0 0.5
v -0.5 0.0 0.5
v -0.5 1.8 -0.5
v 0.5 1.8 -0.5
v 0.5 1.8 0.5
v -0.5 1.8 0.5
f 1 2 3
f 1 3 4
f 5 6 7
f 5 7 8
f 1 2 6
f 1 6 5
f 2 3 7
f 2 7 6
f 3 4 8
f 3 8 7
f 4 1 5
f 4 5 8
";

    let mut single_bom_obj = b"\xEF\xBB\xBF".to_vec();
    single_bom_obj.extend_from_slice(cube_obj);

    let mut double_bom_obj = b"\xEF\xBB\xBF\xEF\xBB\xBF".to_vec();
    double_bom_obj.extend_from_slice(cube_obj);

    let mut spaces_before_bom_obj = b"  \n\xEF\xBB\xBF".to_vec();
    spaces_before_bom_obj.extend_from_slice(cube_obj);

    let mut bom_comment_obj = b"\xEF\xBB\xBF# cube header\n".to_vec();
    bom_comment_obj.extend_from_slice(cube_obj);

    let gltf_json = minimal_valid_gltf_json();
    let mut double_bom_gltf = b"\xEF\xBB\xBF  \xEF\xBB\xBF\n".to_vec();
    double_bom_gltf.extend_from_slice(&gltf_json);

    for (label, input_bytes, expected_prefix) in [
        (
            "single_bom_obj",
            single_bom_obj.as_slice(),
            b"v ".as_slice(),
        ),
        (
            "double_bom_obj",
            double_bom_obj.as_slice(),
            b"v ".as_slice(),
        ),
        (
            "spaces_before_bom_obj",
            spaces_before_bom_obj.as_slice(),
            b"v ".as_slice(),
        ),
        (
            "bom_comment_obj",
            bom_comment_obj.as_slice(),
            b"# ".as_slice(),
        ),
        (
            "double_bom_gltf",
            double_bom_gltf.as_slice(),
            b"{".as_slice(),
        ),
    ] {
        let _ = std::fs::remove_file(&staged_copy);
        let spec =
            AutoRigSpec::new(Bytes::copy_from_slice(input_bytes), "biped").with_provider("blender");
        let task = Task::new(TaskSpec::AutoRig(spec), TaskRequirements::generic(1, 15));
        let res = runner.execute_task(&task, None, None).await;
        assert!(
            res.is_success(),
            "Fake Blender failed on {label}: {:?}",
            res.error
        );
        assert_eq!(
            res.device_name.as_deref(),
            Some("OxideSwarm-AutoRig-Blender")
        );

        let staged_bytes = std::fs::read(&staged_copy).expect("staged input copy");
        assert!(
            !staged_bytes.starts_with(b"\xEF\xBB\xBF"),
            "{label}: staged file on disk must not start with UTF-8 BOM"
        );
        assert!(
            !staged_bytes.windows(3).any(|w| w == [0xEF, 0xBB, 0xBF]),
            "{label}: staged file on disk must not contain UTF-8 BOM"
        );
        assert!(
            staged_bytes.starts_with(expected_prefix),
            "{label}: expected staged input to start with {:?}, got {:?}",
            String::from_utf8_lossy(expected_prefix),
            String::from_utf8_lossy(&staged_bytes[..staged_bytes.len().min(16)])
        );
    }

    // Binary .glb bytes must remain completely untouched when staged.
    let _ = std::fs::remove_file(&staged_copy);
    let glb_spec = AutoRigSpec::new(Bytes::from(glb.clone()), "biped").with_provider("blender");
    let glb_task = Task::new(
        TaskSpec::AutoRig(glb_spec),
        TaskRequirements::generic(1, 15),
    );
    let glb_res = runner.execute_task(&glb_task, None, None).await;
    assert!(glb_res.is_success(), "GLB failed: {:?}", glb_res.error);
    let staged_glb = std::fs::read(&staged_copy).expect("staged glb copy");
    assert_eq!(staged_glb, glb, "Binary GLB must be staged verbatim");
}

fn build_raw_glb_for_test(json_val: &Value, bin: &[u8]) -> Vec<u8> {
    let mut json_bytes = serde_json::to_vec(json_val).unwrap();
    while json_bytes.len() % 4 != 0 {
        json_bytes.push(b' ');
    }
    let mut bin_padded = bin.to_vec();
    while bin_padded.len() % 4 != 0 {
        bin_padded.push(0);
    }
    let total_len = 12 + 8 + json_bytes.len() + 8 + bin_padded.len();
    let mut out = Vec::with_capacity(total_len);
    out.extend_from_slice(b"glTF");
    out.extend_from_slice(&2u32.to_le_bytes());
    out.extend_from_slice(&(total_len as u32).to_le_bytes());
    out.extend_from_slice(&(json_bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(&0x4E4F534Au32.to_le_bytes()); // JSON
    out.extend_from_slice(&json_bytes);
    out.extend_from_slice(&(bin_padded.len() as u32).to_le_bytes());
    out.extend_from_slice(&0x004E4942u32.to_le_bytes()); // BIN\0
    out.extend_from_slice(&bin_padded);
    out
}

#[test]
fn glb_malicious_byte_offset_and_huge_count_return_err_without_panic() {
    let bin = vec![0u8; 64];

    // 1. POSITION accessor.byteOffset + bufferView.byteOffset u64/usize overflow
    let overflow_pos = build_raw_glb_for_test(
        &serde_json::json!({
            "asset": { "version": "2.0" },
            "meshes": [{ "primitives": [{ "attributes": { "POSITION": 0 } }] }],
            "accessors": [{ "bufferView": 0, "count": 1, "byteOffset": u64::MAX - 5 }],
            "bufferViews": [{ "buffer": 0, "byteOffset": 8, "byteLength": 12 }]
        }),
        &bin,
    );
    assert!(
        load_mesh_from_bytes(&overflow_pos).is_err(),
        "overflowing POSITION byteOffset must return Err without panicking"
    );

    // 2. Huge count preallocation DoS (count = u64::MAX and 1_000_000_000)
    for bad_count in [1_000_000_000u64, u64::MAX] {
        let huge_count = build_raw_glb_for_test(
            &serde_json::json!({
                "asset": { "version": "2.0" },
                "meshes": [{ "primitives": [{ "attributes": { "POSITION": 0 } }] }],
                "accessors": [{ "bufferView": 0, "count": bad_count }],
                "bufferViews": [{ "buffer": 0, "byteOffset": 0, "byteLength": 64 }]
            }),
            &bin,
        );
        assert!(
            load_mesh_from_bytes(&huge_count).is_err(),
            "huge count={bad_count} must return Err before Vec::with_capacity"
        );
    }

    // 3. Zero byteStride paired with huge count must be rejected before Vec::with_capacity
    let zero_stride = build_raw_glb_for_test(
        &serde_json::json!({
            "asset": { "version": "2.0" },
            "meshes": [{ "primitives": [{ "attributes": { "POSITION": 0 } }] }],
            "accessors": [{ "bufferView": 0, "count": 1_000_000_000u64 }],
            "bufferViews": [{ "buffer": 0, "byteOffset": 0, "byteLength": 64, "byteStride": 0 }]
        }),
        &bin,
    );
    assert!(load_mesh_from_bytes(&zero_stride).is_err());

    // 4. Malicious indices accessor (u16 and u32) overflow & huge count
    for comp_type in [5123u32, 5125u32] {
        let bad_idx_offset = build_raw_glb_for_test(
            &serde_json::json!({
                "asset": { "version": "2.0" },
                "meshes": [{ "primitives": [{ "attributes": { "POSITION": 0 }, "indices": 1 }] }],
                "accessors": [
                    { "bufferView": 0, "count": 3 },
                    { "bufferView": 1, "count": 3, "componentType": comp_type, "byteOffset": u64::MAX - 3 }
                ],
                "bufferViews": [
                    { "buffer": 0, "byteOffset": 0, "byteLength": 36 },
                    { "buffer": 0, "byteOffset": 8, "byteLength": 16 }
                ]
            }),
            &bin,
        );
        assert!(
            load_mesh_from_bytes(&bad_idx_offset).is_err(),
            "overflowing indices byteOffset (comp_type={comp_type}) must return Err"
        );

        let huge_idx_count = build_raw_glb_for_test(
            &serde_json::json!({
                "asset": { "version": "2.0" },
                "meshes": [{ "primitives": [{ "attributes": { "POSITION": 0 }, "indices": 1 }] }],
                "accessors": [
                    { "bufferView": 0, "count": 3 },
                    { "bufferView": 1, "count": 1_000_000_000u64, "componentType": comp_type }
                ],
                "bufferViews": [
                    { "buffer": 0, "byteOffset": 0, "byteLength": 36 },
                    { "buffer": 0, "byteOffset": 36, "byteLength": 12 }
                ]
            }),
            &bin,
        );
        assert!(
            load_mesh_from_bytes(&huge_idx_count).is_err(),
            "huge indices count (comp_type={comp_type}) must return Err"
        );
    }

    // 5. Optional TEXCOORD_0 overflow / huge count must not panic or OOM
    let bad_uv = build_raw_glb_for_test(
        &serde_json::json!({
            "asset": { "version": "2.0" },
            "meshes": [{ "primitives": [{ "attributes": { "POSITION": 0, "TEXCOORD_0": 1 } }] }],
            "accessors": [
                { "bufferView": 0, "count": 3 },
                { "bufferView": 1, "count": 1_000_000_000u64, "byteOffset": u64::MAX - 3 }
            ],
            "bufferViews": [
                { "buffer": 0, "byteOffset": 0, "byteLength": 36 },
                { "buffer": 0, "byteOffset": 8, "byteLength": 16 }
            ]
        }),
        &bin,
    );
    let uv_res = load_mesh_from_bytes(&bad_uv).expect("valid POSITION with invalid optional UV");
    assert_eq!(uv_res.positions.len(), 3);
    assert!(uv_res.uvs.is_empty());
}

#[test]
fn truncated_glb_utf8_bom_comment_obj_and_nan_obj_handling() {
    // 1. Truncated GLB headers
    let trunc12 = b"glTF\x02\x00\x00\x00\x20\x00\x00\x00";
    assert!(detect_mesh_format(trunc12).is_err());
    assert!(load_mesh_from_bytes(trunc12).is_err());

    let trunc20_badlen = b"glTF\x02\x00\x00\x00\xff\x00\x00\x00\x08\x00\x00\x00JSON";
    assert!(detect_mesh_format(trunc20_badlen).is_err());
    assert!(load_mesh_from_bytes(trunc20_badlen).is_err());

    // 2. UTF-8 BOM + # comment OBJ succeeds
    let mut bom_comment = b"\xEF\xBB\xBF# leading comment\n".to_vec();
    bom_comment.extend_from_slice(&minimal_valid_obj());
    assert_eq!(detect_mesh_format(&bom_comment).unwrap(), MeshFormat::Obj);
    let loaded = load_mesh_from_bytes(&bom_comment).expect("BOM + comment OBJ must parse");
    assert_eq!(loaded.positions.len(), 8);

    // 3. Uppercase V/F OBJ is rejected honestly (case-sensitive Wavefront tags)
    let upper_obj = b"V 0 0 0\nV 1 0 0\nV 0 1 0\nF 1 2 3\n";
    assert!(detect_mesh_format(upper_obj).is_err());
    assert!(load_mesh_from_bytes(upper_obj).is_err());

    // 4. NaN / Inf / -Inf vertex coordinates mixed with finite vertices must return Err
    for bad_coord in ["NaN", "inf", "-inf"] {
        let mut obj = format!("v {bad_coord} 0.0 0.0\n");
        obj.push_str(std::str::from_utf8(&minimal_valid_obj()).unwrap());
        let err = load_mesh_from_bytes(obj.as_bytes())
            .expect_err(&format!("OBJ with {bad_coord} vertex must return Err"));
        assert!(
            err.contains("Non-finite"),
            "unexpected error for {bad_coord}: {err}"
        );
    }
}

#[tokio::test]
async fn autorig_blender_pre_cancel_and_1s_timeout_never_fallback_or_leak_grandchildren() {
    let mut env = EnvLock::lock().await;
    let iso_home = tempfile::tempdir().unwrap();
    env.set("HOME", &iso_home.path().to_string_lossy());
    env.set("USERPROFILE", &iso_home.path().to_string_lossy());
    env.set("TMPDIR", &iso_home.path().to_string_lossy());
    env.set("RUSTY_GRID_BLENDER_PATH", "");

    let dir = tempfile::tempdir().unwrap();
    let pids_file = dir.path().join("pids_1s.txt");
    let p_str = pids_file.display().to_string().replace('\\', "/");
    let sleeping_blender = format!(
        r#"
import os, subprocess, sys, time
if "--version" in sys.argv[1:]:
    print("Blender 4.5.4 LTS", flush=True)
    sys.exit(0)
with open("{p_str}", "a") as f:
    f.write(f"PID:{{os.getpid()}}\n")
    f.flush()
gc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
with open("{p_str}", "a") as f:
    f.write(f"PID:{{gc.pid}}\n")
    f.flush()
time.sleep(30.0)
"#
    );
    let fake_blender = create_fake_blender_executable(dir.path(), &sleeping_blender);
    let (runner, _sbox) = create_isolated_runner(Some(fake_blender.to_string_lossy().to_string()));

    // 1. Pre-cancelled oneshot on AutoRig(preferred_provider="blender") returns EXIT_CODE_CANCELLED with no fallback
    let (cancel_tx, cancel_rx) = oneshot::channel();
    cancel_tx.send("pre_cancelled".to_string()).unwrap();
    let spec_pre = AutoRigSpec::new(Bytes::from(minimal_valid_obj()), "biped")
        .with_provider("blender")
        .with_animations(vec!["idle".into()]);
    let task_pre = Task::new(
        TaskSpec::AutoRig(spec_pre),
        TaskRequirements::generic(1, 30),
    );
    let t_pre = Instant::now();
    let res_pre = runner
        .execute_task_with_checkpoint(&task_pre, None, None, None, Some(cancel_rx))
        .await;
    assert_eq!(res_pre.exit_code, EXIT_CODE_CANCELLED);
    assert!(!res_pre.is_success());
    assert!(t_pre.elapsed() < Duration::from_secs(1));
    assert!(
        !pids_file.exists(),
        "pre-cancelled AutoRig must not spawn Blender"
    );

    // 2. 1s timeout kills Blender + grandchild, reports "after 1s" (never "0s"), and never falls back
    let spec_to = AutoRigSpec::new(Bytes::from(minimal_valid_obj()), "biped")
        .with_provider("blender")
        .with_animations(vec!["idle".into()]);
    let mut req_to = TaskRequirements::generic(1, 1);
    req_to.timeout_secs = 1;
    let task_to = Task::new(TaskSpec::AutoRig(spec_to), req_to);
    let child_pid = Arc::new(AtomicU32::new(0));
    let t_to = Instant::now();
    let res_to = runner
        .execute_task_with_checkpoint(&task_to, None, None, Some(Arc::clone(&child_pid)), None)
        .await;
    let elapsed_to = t_to.elapsed();

    assert_eq!(res_to.exit_code, EXIT_CODE_TIMEOUT);
    assert!(!res_to.is_success());
    assert!(
        elapsed_to < Duration::from_secs(3),
        "1s Blender timeout took {elapsed_to:?}"
    );
    let err_msg = res_to.error.as_deref().unwrap_or("");
    assert!(
        err_msg.contains("after 1s"),
        "1s Blender timeout must report 'after 1s', got: {err_msg:?}"
    );
    assert!(
        !err_msg.contains("after 0s"),
        "1s timeout must never report 'after 0s': {err_msg:?}"
    );
    let pids = poll_pids_async(&pids_file, 2, Duration::from_secs(2)).await;
    assert_all_dead_async(&pids, Duration::from_secs(3)).await;
}

#[tokio::test]
async fn autorig_device_name_and_stdout_attribution_across_auto_builtin_blender() {
    let mut env = EnvLock::lock().await;
    let iso_home = tempfile::tempdir().unwrap();
    env.set("HOME", &iso_home.path().to_string_lossy());
    env.set("USERPROFILE", &iso_home.path().to_string_lossy());
    env.set("TMPDIR", &iso_home.path().to_string_lossy());
    env.set("RUSTY_GRID_BLENDER_PATH", "");

    let glb = generate_sdf_glb();
    let obj = minimal_valid_obj();

    // 1. "auto" fallback when Blender is missing (invalid override path -> None) -> OxideSwarm-AutoRig-Builtin + "provider: builtin"
    let missing_blender = iso_home.path().join("missing_blender_bin");
    let (runner_noblen, _sbox1) =
        create_isolated_runner(Some(missing_blender.to_string_lossy().to_string()));
    let auto_fb_task = Task::new(
        TaskSpec::AutoRig(
            AutoRigSpec::new(Bytes::from(obj.clone()), "biped")
                .with_provider("auto")
                .with_animations(vec!["idle".into()]),
        ),
        TaskRequirements::generic(1, 30),
    );
    let res_auto_fb = runner_noblen.execute_task(&auto_fb_task, None, None).await;
    assert!(res_auto_fb.is_success(), "{:?}", res_auto_fb.error);
    assert_eq!(
        res_auto_fb.device_name.as_deref(),
        Some("OxideSwarm-AutoRig-Builtin")
    );
    assert!(String::from_utf8_lossy(res_auto_fb.stdout.as_slice()).contains("provider: builtin"));

    // 2. "builtin" even when Blender IS configured -> OxideSwarm-AutoRig-Builtin + "provider: builtin"
    let dir = tempfile::tempdir().unwrap();
    let args_log = dir.path().join("args.json");
    let fake_blender = create_fake_blender_outputting_glb(dir.path(), &args_log, &glb);
    let (runner_blen, _sbox2) =
        create_isolated_runner(Some(fake_blender.to_string_lossy().to_string()));

    let builtin_task = Task::new(
        TaskSpec::AutoRig(
            AutoRigSpec::new(Bytes::from(obj.clone()), "biped")
                .with_provider("builtin")
                .with_animations(vec!["idle".into()]),
        ),
        TaskRequirements::generic(1, 30),
    );
    let res_builtin = runner_blen.execute_task(&builtin_task, None, None).await;
    assert!(res_builtin.is_success(), "{:?}", res_builtin.error);
    assert_eq!(
        res_builtin.device_name.as_deref(),
        Some("OxideSwarm-AutoRig-Builtin")
    );
    assert!(String::from_utf8_lossy(res_builtin.stdout.as_slice()).contains("provider: builtin"));
    assert!(
        !args_log.exists(),
        "builtin must never invoke configured Blender"
    );

    // 3. "auto" when Blender succeeds -> OxideSwarm-AutoRig-Blender + "provider: blender"
    let auto_blen_task = Task::new(
        TaskSpec::AutoRig(
            AutoRigSpec::new(Bytes::from(obj), "biped")
                .with_provider("auto")
                .with_animations(vec!["idle".into()]),
        ),
        TaskRequirements::generic(1, 30),
    );
    let res_auto_blen = runner_blen.execute_task(&auto_blen_task, None, None).await;
    assert!(res_auto_blen.is_success(), "{:?}", res_auto_blen.error);
    assert_eq!(
        res_auto_blen.device_name.as_deref(),
        Some("OxideSwarm-AutoRig-Blender")
    );
    assert!(String::from_utf8_lossy(res_auto_blen.stdout.as_slice()).contains("provider: blender"));
}

#[tokio::test]
async fn autorig_duplicate_animation_clips_behavior() {
    let obj = minimal_valid_obj();

    // Duplicate animation clips (including case/whitespace variations) are rejected across
    // TaskSpec::validate(), auto_rig_mesh, ClipSynthesizer, TaskRunner, and blender_rig.py.
    for dup_clips in [
        vec!["idle".to_string(), "idle".to_string()],
        vec!["idle".to_string(), " IDLE ".to_string(), "walk".to_string()],
    ] {
        let spec = AutoRigSpec::new(Bytes::from(obj.clone()), "biped").with_animations(dup_clips);
        let err = TaskSpec::AutoRig(spec.clone())
            .validate()
            .expect_err("duplicate embed_animations must be rejected in validate()");
        assert!(err.contains("Duplicate"), "unexpected error: {err}");

        assert!(auto_rig_mesh(&spec).is_err());

        let (runner, _dir) = create_isolated_runner(None);
        let task = Task::new(TaskSpec::AutoRig(spec), TaskRequirements::generic(1, 10));
        let res = runner.execute_task(&task, None, None).await;
        assert_eq!(res.exit_code, EXIT_CODE_GENERAL_ERROR);
    }

    let raw = load_mesh_from_bytes(&obj).unwrap();
    let lm = rusty_grid_worker::autorig::extract_landmarks(&raw, RigPreset::Biped).unwrap();
    let arm = rusty_grid_worker::autorig::build_standard_armature(&lm, RigPreset::Biped);
    assert!(ClipSynthesizer::synthesize_all_checked(
        &["idle".into(), "idle".into()],
        lm.height,
        &arm
    )
    .is_err());

    // blender_rig.py rejects duplicate --animations entries cleanly while preserving empty "".
    let py = get_python_exe();
    let script_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("engines/autorig/blender_rig.py");
    let dir = tempfile::tempdir().unwrap();
    let dup_out = Command::new(&py)
        .arg(&script_path)
        .args([
            "--",
            "--input",
            "mesh.obj",
            "--output",
            dir.path().join("dup.glb").to_str().unwrap(),
            "--preset",
            "biped",
            "--animations",
            "idle,idle",
        ])
        .output()
        .unwrap();
    assert!(!dup_out.status.success());
    let dup_stderr = String::from_utf8_lossy(&dup_out.stderr);
    assert!(dup_stderr.contains("Duplicate"), "stderr: {dup_stderr}");
}
