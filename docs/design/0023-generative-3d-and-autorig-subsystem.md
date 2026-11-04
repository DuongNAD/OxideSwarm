# 0023: Generative 3D and Auto-Rigging Subsystem

Status: Partial
Created: 2026-11-04    Updated: 2026-11-04

## Summary

Documents the local 3D mesh generation (`TaskSpec::Generative3D`, `crates/worker/src/gen3d`,
`engines/generative_3d/entrypoint.py`) and skeletal auto-rigging (`TaskSpec::AutoRig`,
`crates/worker/src/autorig`, `engines/autorig/blender_rig.py`) subsystem, its current boundaries
(`3D-2`, `3D-3`), and updates the macOS, Windows, and Android Termux worker installers to deploy
`engines/` and set `OXIDESWARM_GEN3D_SCRIPT`.

## Context

OxideSwarm includes two 3D asset pipeline task variants in `crates/core/src/task.rs`:
- `TaskSpec::Generative3D(Generative3DSpec)` (`submit --type 3d` or `submit --prompt <TEXT>` /
  `--image <PATH>`), executed by `TaskRunner::execute_generative_3d_workload` in
  `crates/worker/src/runner.rs` (`:1912-2185`).
- `TaskSpec::AutoRig(AutoRigSpec)` (`submit --type autorig` or `submit --input-mesh <PATH>`),
  executed by `TaskRunner::execute_auto_rig_workload` in `crates/worker/src/runner.rs`
  (`:2289-2445`).

Before this change (`DOC-5` in `docs/BACKLOG.md`):
1. While `docs/design/0015-engine-names-and-cpu-fallback.md` and
   `docs/design/0017-honest-3d-sdf-fallback.md` recorded specific fixes to engine validation and
   honest TripoSR failure, there was no design record or architecture section in `README.md` or
   `CLAUDE.md` covering the end-to-end 3D and auto-rigging subsystem (`crates/worker/src/gen3d`,
   `crates/worker/src/autorig`, `engines/`).
2. `crates/worker/src/gen3d/runner.rs:117-141` (`resolve_entrypoint_script`) resolves
   `engines/generative_3d/entrypoint.py` via `OXIDESWARM_GEN3D_SCRIPT` first and then relative to
   the worker's current working directory (`engines/generative_3d/entrypoint.py`,
   `../engines/generative_3d/entrypoint.py`, `../../engines/generative_3d/entrypoint.py`), but none
   of the background service installers (`packaging/macos/install_mac_daemon.sh`,
   `packaging/windows/install_windows_service.ps1`,
   `packaging/android/termux/install_termux_daemon.sh`) copied `engines/` or set
   `OXIDESWARM_GEN3D_SCRIPT`.

## Goals

- **G1:** Document the architecture, wire specifications, execution tiers, environment variables,
  and current limits (`3D-2`, `3D-3`) of `TaskSpec::Generative3D` and `TaskSpec::AutoRig` in a
  design record (`0023`), `README.md`, and `CLAUDE.md`.
- **G2:** Update the macOS, Windows, and Android Termux worker installers so installed background
  workers have `engines/` deployed in their working/install directory and `OXIDESWARM_GEN3D_SCRIPT`
  configured.

## Non-goals

- Bundling a pretrained neural 3D model checkpoint (`TripoSR` / `Trellis`).
- Changing the wire format or fixing `3D-2` (reporting Blender fallback in task output) or `3D-3`
  (preset-only SDF geometry) in this change.

## Design

### D1 Generative 3D pipeline (`TaskSpec::Generative3D`, `crates/worker/src/gen3d`)

`Generative3DSpec` (`crates/core/src/task.rs:733-887`) carries:
- `prompt: Option<String>` — natural-language prompt (`submit --prompt <TEXT>`).
- `image_data: Option<Bytes>` — raw PNG/JPEG/WebP image bytes (`submit --image <PATH>`).
- `quality: String` — `"fast"`, `"standard"` (default), or `"high"` (`submit --quality`).
- `engine_preference: Option<String>` — `"auto"` (default), `"sdf"`, `"triposr"`, or `"trellis"`
  (`submit --engine`, parsed by `Generative3DSpec::try_engine` into `Engine3D`).
- `export_texture: bool` — whether to bake a PBR albedo PNG texture into the `.glb` (`true` by
  default; disabled with `submit --no-export-texture`).

Client-side validation (`crates/cli/src/cli_app.rs`, designs `0015` and `0017`) rejects unknown
`--engine` names, rejects `--engine trellis`, and rejects `--image` when the effective engine is
`sdf` or `auto` before connecting to the master.

On the worker, `TaskRunner::execute_generative_3d_workload` (`crates/worker/src/runner.rs:1912`)
routes by `Engine3D`:
1. **Tier 0 — Pure-Rust Procedural SDF (`Engine3D::Sdf` and `Engine3D::Auto`):**
   - Runs in-process on CPU via `generate_procedural_3d_with_progress`
     (`crates/worker/src/gen3d/mod.rs:28-98`) without requiring a GPU (`requires_gpu = false`).
   - `sdf::resolve_preset` (`crates/worker/src/gen3d/sdf.rs:125-154`) maps prompt keywords to one
     of four analytical SDF presets: `SdfPreset::RobotTorso` (`robot`, `mech`, `droid`, `cyber`,
     `armor`), `SdfPreset::StylizedCharacter` (`chibi`, `cute`, `mascot`, `creature`, `stylized`),
     `SdfPreset::TestOrb` (`orb`, `sphere`, `math`, `test`), or `SdfPreset::HumanoidDummy`
     (default).
   - `marching_cubes::extract_isosurface` samples a $32^3$ (`fast`), $48^3$ (`standard`), or
     $64^3$ (`high`) grid (`crates/worker/src/gen3d/mod.rs:41-48`).
   - `manifold::repair_manifold_mesh` deduplicates vertices, removes degenerate faces, and computes
     area-weighted normals; `uv::MultiChartBoxUnwrapper` unwraps non-overlapping UV charts (`512`,
     `1024`, or `2048` px); `bake::bake_texture_atlas` rasterizes a procedural PBR albedo PNG when
     `export_texture` is `true`; and `gltf_pack::pack_glb` serializes a binary glTF 2.0 (`.glb`)
     container returned in `TaskResult::asset_bytes` with `device_name = Some("Procedural-SDF-PureRust")`.
   - Emits four `WorkerMessage::Checkpoint` stages (`0.25` `sdf_geometry`, `0.50` `manifold_uv`,
     `0.75` `texture_baking`, `1.00` `gltf_pack`).
2. **Tier 1 — Python Subprocess (`Engine3D::TripoSr`):**
   - Resolves Python via `gen3d::runner::resolve_python_runtime`
     (`crates/worker/src/gen3d/runner.rs:67-114`: `OXIDESWARM_PYTHON` -> `RUSTY_GRID_PYTHON` ->
     workspace `.venv` -> `python` / `python3` on `PATH`, using `std::path::absolute` so venv
     symlinks keep their `sys.prefix`) and the script via `resolve_entrypoint_script`
     (`OXIDESWARM_GEN3D_SCRIPT` -> `engines/generative_3d/entrypoint.py` in `.`, `..`, `../..`).
   - Runs `engines/generative_3d/entrypoint.py` inside a per-task `Sandbox` under
     `execute_process_supervised` with `HF_HUB_OFFLINE=1` and `TRANSFORMERS_OFFLINE=1`.
   - Because OxideSwarm does not bundle a pretrained TripoSR neural checkpoint (`0017`),
     `engines/generative_3d/entrypoint.py` exits with code `1` and writes
     `"TripoSR model not available: local AI 3D model checkpoint is not installed"` to `stderr`.
3. **`Engine3D::Trellis`:**
   - Refused immediately with exit code `1` (`"Trellis 3D neural engine is not supported; use sdf or triposr"`).

### D2 Automated rigging and animation pipeline (`TaskSpec::AutoRig`, `crates/worker/src/autorig`)

`AutoRigSpec` (`crates/core/src/task.rs:891-900`) carries:
- `input_mesh: Bytes` — raw `.glb`/`.gltf` or `.obj` mesh bytes (`submit --input-mesh <PATH>`).
- `rig_preset: String` — `"biped"` (default), `"quadruped"`, or `"generic"` (`submit --rig-preset`).
- `embed_animations: Vec<String>` — procedural clip names (`submit --embed-animations`, e.g.
  `idle,walk,turnaround`).
- `preferred_provider: Option<String>` — `"builtin"` (default) or `"blender"`
  (`submit --preferred-provider`).

On the worker, `TaskRunner::execute_auto_rig_workload` (`crates/worker/src/runner.rs:2289`) routes
by `preferred_provider`:
1. **Built-in Pure-Rust Rigging Engine (`preferred_provider = "builtin"` or fallback):**
   - `mesh_loader::load_mesh_from_bytes` parses binary `.glb` or ASCII `.obj` geometry.
   - `landmark::extract_landmarks` performs 3x3 symmetric eigendecomposition PCA orientation and
     50-slab vertical slicing to extract 15 anatomical landmarks (`0.25` `landmark_extraction`).
   - `armature::build_standard_armature` builds a 15-joint topological DAG (`Hips`, `Spine`,
     `Chest`, `Neck`, `Head`, `UpperLeg_L/R`, `LowerLeg_L/R`, `Foot_L/R`, `UpperArm_L/R`,
     `LowerArm_L/R`) and computes column-major $4\times 4$ Inverse Bind Matrices (`0.50`
     `armature_ibm`).
   - `skinning::compute_skin_weights` computes surface geodesic bone weights with a sagittal
     symmetry barrier and top-4 `JOINTS_0` / `WEIGHTS_0` unity normalization (`0.75`
     `geodesic_skinning`).
   - `anim_synth::ClipSynthesizer::synthesize_all` generates procedural quaternion/translation TRS
     keyframe tracks (`idle`, `walk`, `turnaround`), and `gltf_export::export_rigged_glb` packs the
     skinned mesh, `Skin`, `Node` hierarchy, and `Animation` channels into a binary `.glb` returned
     in `TaskResult::asset_bytes` (`1.00` `animation_export`).
2. **Supervised Headless Blender Provider (`preferred_provider = "blender"`):**
   - `autorig::blender_provider::resolve_blender_binary` delegates to
     `rusty_grid_core::capabilities::resolve_blender_binary` (`--blender-path`,
     `RUSTY_GRID_BLENDER_PATH`, `OXIDE_SWARM_BLENDER_PATH`, `BLENDER_PATH`, `PATH`, and standard
     macOS/Linux/Windows install paths).
   - `BlenderRigProvider::execute_rigging` (`crates/worker/src/autorig/blender_provider.rs:51-141`)
     materializes the input mesh and `engines/autorig/blender_rig.py` (reading the on-disk script
     if found via `resolve_blender_rig_script`, or falling back to `EMBEDDED_BLENDER_RIG_SCRIPT`
     compiled in via `include_str!`) into a temporary directory and invokes
     `blender --background --factory-startup --python blender_rig.py -- --input ... --output ...`
     under `execute_process_supervised`.
   - If Blender is not installed or exits non-zero, `execute_auto_rig_workload` logs a warning and
     falls back to `auto_rig_mesh_with_progress`.

### D3 Current subsystem limits (`3D-2`, `3D-3`)

| Backlog ID | Component | Current limitation |
|---|---|---|
| `3D-2` | `crates/worker/src/runner.rs:2370-2376`, `crates/worker/src/gen3d/runner.rs:128-132` | When `--preferred-provider blender` is requested and Blender is missing or fails, the worker logs a warning and falls back to the built-in Rust auto-rigger while returning `stdout = "Auto-rigged glTF 2.0 binary generated (... bytes)\n"` and `device_name = Some("OxideSwarm-AutoRig")`, so the caller cannot tell from `TaskResult` alone whether Blender or the built-in fallback ran. For `Generative3D`, `resolve_entrypoint_script` only checks relative paths unless `OXIDESWARM_GEN3D_SCRIPT` is set (mitigated for installed services by D4). |
| `3D-3` | `crates/worker/src/gen3d/sdf.rs:91-154`, `crates/worker/src/gen3d/mod.rs:37-48`, `engines/generative_3d/entrypoint.py` | Tier 0 procedural SDF selects among four fixed analytical presets (`HumanoidDummy`, `RobotTorso`, `StylizedCharacter`, `TestOrb`) by prompt substring rather than open-vocabulary text-to-3D, and rejects `--image` inputs. Tier 1 (`triposr`) has no bundled neural weights checkpoint and exits `1`. |

### D4 Worker service installer deployment of `engines/` and `OXIDESWARM_GEN3D_SCRIPT`

To ensure background worker daemons started outside a repository working directory can locate
`engines/generative_3d/entrypoint.py` and `engines/autorig/blender_rig.py`:
- **`packaging/macos/install_mac_daemon.sh`:** copies `${WORKSPACE_ROOT}/engines` (when present) to
  `${WORK_DIR}/engines` owned by `${RUN_USER}:${RUN_GROUP}`, and sets `OXIDESWARM_GEN3D_SCRIPT` to
  `${WORK_DIR}/engines/generative_3d/entrypoint.py` in the LaunchDaemon `.plist`
  `EnvironmentVariables` dictionary.
- **`packaging/windows/install_windows_service.ps1`:** copies `..\..\engines` (when present) to
  `$InstallDir\engines`, sets machine environment variable `OXIDESWARM_GEN3D_SCRIPT` to
  `$InstallDir\engines\generative_3d\entrypoint.py`, and includes `<env name="OXIDESWARM_GEN3D_SCRIPT" .../>`
  in generated WinSW XML.
- **`packaging/android/termux/install_termux_daemon.sh` & `start_worker.sh`:** copies `engines/`
  (when present next to the installer or repo root) to `${HOME}/.local/share/oxideswarm/engines`
  and exports `OXIDESWARM_GEN3D_SCRIPT="${HOME}/.local/share/oxideswarm/engines/generative_3d/entrypoint.py"`
  in `start_worker.sh` and the generated Termux:Boot hook when the script is present.

## Compatibility

No wire protocol or CLI flag changes. Existing `TaskSpec::Generative3D` and `TaskSpec::AutoRig`
payloads remain binary- and JSON-compatible.

## Security

- External 3D (`entrypoint.py`) and Blender (`blender_rig.py`) subprocesses run through
  `TaskRunner::execute_process_supervised`, which strips cluster/mesh secrets
  (`OXIDESWARM_SECRET`, `OXIDESWARM_SECRET_FILE`, `OXIDE_MESH_TOKEN`), isolates child processes in
  process groups / Windows Job Objects, and enforces task timeouts.
- Installed `engines/` directories are owned by root/Administrators or the service account and do
  not contain secrets.

## Test plan

- `cargo fmt --all -- --check`
- `python tests/test_launcher_scripts.py` (verifies line endings, installer safety, and dry-run
  behaviour).

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Write design record `0023` for the 3D and auto-rigging subsystem | `docs/design/0023-generative-3d-and-autorig-subsystem.md`, `docs/README.md` | Architecture, specs, env vars, and `3D-2`/`3D-3` limits documented | Implemented |
| W2 | Document the 3D subsystem in `README.md` and `CLAUDE.md` | `README.md`, `CLAUDE.md` | `README.md` has a 3D subsystem section and `CLAUDE.md` `worker` entry lists `gen3d` and `autorig` | Implemented |
| W3 | Deploy `engines/` and set `OXIDESWARM_GEN3D_SCRIPT` in worker installers | `packaging/macos/install_mac_daemon.sh`, `packaging/windows/install_windows_service.ps1`, `packaging/android/termux/install_termux_daemon.sh`, `packaging/android/termux/start_worker.sh` | Installers copy `engines/` when available and configure `OXIDESWARM_GEN3D_SCRIPT` | Implemented |
| W4 | Mark `DOC-5` fixed in `docs/BACKLOG.md` | `docs/BACKLOG.md` | `DOC-5` marked fixed with changelog entry | Implemented |

## Status log

- 2026-11-04: Design record created and `DOC-5` implemented on branch `win/doc-5-3d-subsystem`.
