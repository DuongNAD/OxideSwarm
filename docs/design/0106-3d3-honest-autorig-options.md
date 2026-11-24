# 0106: Honest AutoRig Options, Format Detection & Cancellation (`3D-3`)

Status: Implemented
Created: 2026-11-22    Updated: 2026-11-22

## Summary

`3D-3` eliminates every silent no-op and unpropagated timeout/cancellation path in AutoRig and Tier-0 SDF execution. `TaskSpec::validate()`, the worker AutoRig pipeline, and `engines/autorig/blender_rig.py` now validate `rig_preset`, `embed_animations`, and `preferred_provider` consistently, implement distinct `biped` (15-joint), `quadruped` (19-joint), and `generic` (5-joint axial) skeletons and real `turnaround` keyframes, detect `.glb` / `.gltf` / `.obj` inputs from magic bytes, refuse failures on explicit `"blender"` instead of silently falling back, report the actual provider in `TaskResult::device_name` and stdout, and propagate task `timeout`, `child_pid`, and cooperative cancellation into both Blender and blocking Rust 3D/AutoRig workloads so timeout or cancellation settles within `<= 1 s`.

## Context

- Previously, `TaskSpec::validate()` (`crates/core/src/task.rs`) only checked `!input_mesh.is_empty()` and `!rig_preset.trim().is_empty()`. Unknown presets, unknown animation clips (`_ => {}` in `anim_synth.rs`), and unknown `preferred_provider` values were silently accepted and coerced to `biped` / empty clips / `builtin`.
- `engines/autorig/blender_rig.py` accepted `--preset` without ever reading it, produced empty actions for `turnaround` and unknown clips, and lacked a top-level `try/except ... sys.exit(1)` guard.
- `BlenderRigProvider::execute_rigging` hardcoded `input_mesh.glb` (breaking `.obj`/`.gltf`), omitted `--python-exit-code 1`, hardcoded a `60` s timeout, and passed `child_pid: None, cancel_rx: None` into `execute_process_supervised`.
- `auto_rig_mesh_async` silently fell back to built-in Rust even when `preferred_provider == "blender"` was explicitly requested, and `TaskResult::device_name` always reported `"OxideSwarm-AutoRig-Engine"`.
- `execute_tier0_sdf` dropped `timeout` and `cancel_rx` completely, `execute_auto_rig_workload` had a channel-drop bug (`rx.await.ok()`), and neither `gen3d` nor `autorig` `spawn_blocking` loops polled a cooperative `AtomicBool` stop flag.

## Goals

- G1. Validate `rig_preset` (`biped`, `quadruped`, `generic`), `embed_animations` (`idle`, `walk`, `turnaround`, or empty `Vec`), and `preferred_provider` (`None`, `auto`, `builtin`, `blender`) upfront in `TaskSpec::validate()`, worker, and `blender_rig.py`.
- G2. Implement distinct `quadruped` (19-joint) and `generic` (5-joint axial) skeletons and animations in both pure-Rust AutoRig and `blender_rig.py`, plus real 360-degree `turnaround` keyframes in `blender_rig.py`.
- G3. Detect `.glb` / `.gltf` / `.obj` via magic bytes and content sniffing (`detect_mesh_format`) so `BlenderRigProvider` writes the right input file extension and rejects corrupt/empty payloads before spawning Blender.
- G4. Enforce honest provider semantics: `None`/`"builtin"` -> Builtin only; `"auto"` -> try Blender and fall back to Builtin on non-cancel/non-timeout failure; `"blender"` -> fail explicitly on missing/failing/invalid Blender; report `"OxideSwarm-AutoRig-Blender"` vs `"OxideSwarm-AutoRig-Builtin"` in `TaskResult::device_name` and `provider: blender|builtin` in stdout.
- G5. Propagate task `timeout`, `child_pid`, and `cancel_rx` into Blender supervision and cooperative `AtomicBool` checks across `gen3d/*` and `autorig/*` so timeout and cancellation settle within `<= 1 s` without leaking CPU threads or Blender child processes.

## Non-goals

- Modifying `crates/core` wire structs/enums or bumping `PROTOCOL_VERSION_CURRENT` (`10`), `STORE_FORMAT_VERSION` (`4`), or `EXTENSION_REGISTRY` (`&[]`).
- Changing `preferred_provider: None` away from `Builtin` (existing 4-checkpoint pure-Rust tests depend on deterministic `Builtin` execution when no provider is specified).

## Design

### D1: Upfront Validation Without Wire Changes (`crates/core/src/task.rs`)

- Added `AutoRigProvider` (`Auto`, `Builtin`, `Blender`) and validation helpers `AutoRigSpec::parse_provider`, `AutoRigSpec::validate_rig_preset`, and `AutoRigSpec::validate_animation_clip` without touching any serialized fields of `AutoRigSpec`, `HumanAutoRigSpec`, or `BinaryAutoRigSpec`.
- `TaskSpec::validate()` validates `rig_preset`, every element of `embed_animations` (allowing `vec![]`), and `preferred_provider` (`None` -> `Builtin`).

### D2: Distinct Skeletons & Animations (`crates/worker/src/autorig/*`, `engines/autorig/blender_rig.py`)

- `RigPreset::try_from_str` / `FromStr` rejects unknown/empty presets instead of defaulting to `Biped`.
- `landmark.rs` extracts `QuadrupedLandmarks` using longitudinal spine orientation (`Z` front-to-back, `X` lateral) when `preset == RigPreset::Quadruped`.
- `armature.rs` and `blender_rig.py` construct:
  - `biped`: 15 joints (`Hips`..`LowerArm_R`).
  - `quadruped`: 19 joints (`Pelvis`, `Spine`, `Chest`, `Neck`, `Head`, `Tail_01`, `Tail_02`, `HindUpperLeg_L/R`, `HindLowerLeg_L/R`, `HindPaw_L/R`, `FrontUpperLeg_L/R`, `FrontLowerLeg_L/R`, `FrontPaw_L/R`).
  - `generic`: 5 axial joints (`Root`, `Spine`, `Chest`, `Neck`, `Head`).
- `anim_synth.rs` replaces `_ => {}` with `ClipSynthesizer::synthesize_all_checked` / `synthesize_all_cancellable`, and synthesizes preset-specific `idle`, `walk` (diagonal trot for quadruped), and `turnaround` tracks. `blender_rig.py` validates `--preset` and `--animations`, implements real 97-frame `turnaround` keyframes, and wraps `main()` in `try/except ... sys.exit(1)`.

### D3: Magic-Byte Detection & Honest Provider Routing (`mesh_loader.rs`, `blender_provider.rs`, `autorig/mod.rs`, `workloads.rs`)

- `detect_mesh_format` identifies binary `.glb` (`b"glTF"` header + version 2 + length bounds), JSON `.gltf` (optional UTF-8 BOM + `{` with `"asset"` or `"meshes"`), and ASCII `.obj` (UTF-8 without control bytes, containing valid `v` and `f` lines). Pure-Rust `load_mesh_from_bytes_cancellable` rejects `.gltf` with an actionable error directing callers to `.glb`/`.obj` or `preferred_provider='blender'`.
- `BlenderRigProvider::execute_rigging_supervised` writes `input_mesh.<ext>`, passes `--python-exit-code 1`, validates output `.glb` (`b"glTF"`, len >= 12), and preserves `res.stderr` diagnostics.
- `auto_rig_mesh_async_supervised` returns `AutoRigOutcome { glb_bytes, provider: AutoRigUsedProvider }`, setting `TaskResult::device_name` to `"OxideSwarm-AutoRig-Blender"` or `"OxideSwarm-AutoRig-Builtin"` and printing `provider: blender` / `provider: builtin` in stdout.

### D4: `<= 1 s` Timeout & Cancellation Propagation (`workloads.rs`, `autorig/*`, `gen3d/*`)

- Added `WorkerError::Cancelled(String)` and `WorkerError::TimedOut(String)` so Blender cancellation/timeout is never caught by `"auto"` fallback.
- `auto_rig_mesh_async_supervised` forwards remaining `timeout`, `child_pid`, and a bridged `blender_cancel_rx` into `BlenderRigProvider::execute_rigging_supervised` -> `execute_process_supervised`, triggering `stop_tree` on cancel or timeout.
- `execute_tier0_sdf` and the Builtin AutoRig branch use `WorkloadStopOnDrop(Arc<AtomicBool>)` / `AutoRigStopOnDrop`, `tokio::select!` with `cancellation(&mut cancel_rx)` (fixing the dropped-sender bug) and `tokio::time::sleep(timeout)`, a 200 ms bounded blocking join, and fine-grained `should_stop()` checks across `marching_cubes.rs`, `manifold.rs`, `uv.rs`, `bake.rs`, `gltf_pack.rs`, `mesh_loader.rs`, `landmark.rs`, `skinning.rs`, `anim_synth.rs`, and `gltf_export.rs`.

## Compatibility

- `PROTOCOL_VERSION_CURRENT == 10`, `PROTOCOL_VERSION_MIN_COMPATIBLE == 9`, `STORE_FORMAT_VERSION == 4`, and `EXTENSION_REGISTRY == &[]` remain unchanged. `wire_layout_tests` passes without modification.

## Security

- Magic-byte format sniffing validates GLB header bounds, JSON structure, and ASCII OBJ control-byte absence before writing temp files or spawning Blender.
- Blender receives `--python-exit-code 1` and `stop_tree` kills the entire process group / Job Object on cancellation or timeout.

## Test plan

- `crates/core/src/task.rs` unit tests + `crates/core/tests/m1_challenger_1_3d_serialization.rs`
- `crates/worker/src/client/tests.rs` & `crates/worker/tests/test_challenger_m3_blender_job_object.rs` (`"auto"` fallback vs `"blender"` strict failure, `device_name`)
- `crates/worker/tests/test_autorig_pipeline.rs` (`biped`, `quadruped`, `generic` distinct joint topologies)
- `crates/worker/tests/test_3d3_autorig_options.rs` (validation refusals, distinct skeletons/animations, `blender_rig.py` validation & `turnaround` under mock `bpy`, `.obj`/`.gltf`/`.glb` magic-byte routing, provider reporting, and `<= 1 s` timeout/cancellation + dropped-sender checks for Blender, Builtin AutoRig, and Tier-0 SDF)
- `python3 -m py_compile engines/autorig/blender_rig.py`

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| 3D-3 | Make AutoRig options real or refuse them; magic-byte format detection; <=1s timeout/cancel propagation | `crates/core/src/task.rs`, `crates/worker/src/autorig/*`, `crates/worker/src/gen3d/*`, `crates/worker/src/runner/workloads.rs`, `engines/autorig/blender_rig.py`, tests | All workspace `3d` tests, `wire_layout_tests`, `clippy`, `fmt`, and `ox-mode test size` pass | Implemented |

## Status log

- 2026-11-22: Created design record `0106` and implemented `3D-3` across `crates/core`, `crates/worker`, and `engines/autorig/blender_rig.py`.
