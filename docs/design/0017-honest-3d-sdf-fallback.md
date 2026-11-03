# 0017: Honest 3D Procedural SDF Fallback, Neural Model Refusal, and Environment Isolation

Status: Implemented
Created: 2026-11-01    Updated: 2026-11-03

## Summary

Enforces honest execution and failure semantics for generative 3D workloads across OxideSwarm:
- Default (`auto`) and procedural (`sdf`) 3D generation routes exclusively to pure Rust Tier 0 Signed Distance Functions (`Procedural-SDF-PureRust`), eliminating Python subprocess overhead and unverified fallbacks.
- Removes hardcoded octahedron export stubs (`export_valid_glb`) and fake progress lines; neural 3D model requests (`triposr`) without installed checkpoint weights exit non-zero with explicit diagnostic reasons.
- Client-side validation rejects `--image` inputs immediately before submission when using default or procedural SDF engines.
- Subprocess execution failures surface the terminal `stderr` diagnostic line in `result.error`.
- Python runtime discovery preserves virtual environment symlinks using `std::path::absolute`.
- Engine device labels (`device_name`) are dynamically parsed from engine outputs rather than hardcoded.

## Context

Behaviour before this change:

| Area | Behaviour | Reference |
|---|---|---|
| Octahedron Stub | When `OXIDESWARM_TRIPOSR_CHECKPOINT` was set to an arbitrary file, `entrypoint.py` exported a fixed octahedron with fake progress and exited 0, claiming neural inference occurred. | `engines/generative_3d/entrypoint.py:45+` |
| Fake Device Label | Worker unconditionally labeled any successful 3D subprocess run as `TripoSR-CUDA` or `TripoSR-CPU`. | `crates/worker/src/runner.rs:2099` |
| Auto Engine Purity | If `OXIDESWARM_GEN3D_SCRIPT` was set, `auto` routed to Python instead of SDF. If Python failed, it silently fell back to SDF, hiding configuration defects. | `crates/worker/src/runner.rs:1870, 2142` |
| Delayed Image Rejection | `submit --image x.png --wait` with default engine passed client validation and failed only after worker assignment. | `crates/core/src/task.rs:1472`, `crates/cli/src/cli_app.rs:2028` |
| Subprocess Error Reporting | Subprocess failures left `result.error` with generic text `3D engine subprocess failed with exit code 1`, hiding the cause from API callers. | `crates/worker/src/runner.rs:2146` |
| Python Venv Resolution | `resolve_python_runtime` canonicalized paths, resolving venv symlinks to base interpreters and breaking virtualenv site-packages. | `crates/worker/src/gen3d/runner.rs:10` |

## Goals

- **G1** Eliminate fake generative stubs: require real neural weights for neural engines; fail non-zero with exact cause (`TripoSR model not available`) when weights are missing.
- **G2** Enforce pure Rust procedural SDF execution for `auto` and `sdf`, independent of environment variables or Python availability.
- **G3** Reject `--image` inputs client-side before task submission when using procedural SDF engines.
- **G4** Deliver actionable diagnostic messages in `result.error` by extracting the terminal `stderr` line on subprocess exit.
- **G5** Preserve virtual environment integrity during Python discovery by avoiding symlink canonicalization.
- **G6** Derive `device_name` dynamically from engine telemetry rather than static hardcoding.

## Non-goals

- Distributing pre-trained multi-gigabyte neural network weights in the repository.
- Implementing image-to-SDF neural reconstruction in pure Rust.
- Changing wire protocol framing format.

## Design

### D1. Genuine Neural Model Execution or Honest Refusal
- In `engines/generative_3d/entrypoint.py`:
  - Remove `export_valid_glb` and dummy octahedron vertex buffers.
  - Remove artificial `[PROGRESS: ...]` print statements preceding weight verification.
  - When model checkpoint weights are absent, write explicit diagnostic to stderr and exit with non-zero exit code:
    `TripoSR model not available: local AI 3D model checkpoint is not installed`.

### D2. Deterministic Procedural SDF Routing
- In `crates/worker/src/runner.rs`:
  - `engine == Engine3D::Sdf || engine == Engine3D::Auto` immediately executes `execute_tier0_sdf`.
  - Remove `!is_custom_script` condition so `auto` never branches to Python.
  - Remove unreachable `Engine3D::Auto` fallback branches when Python is missing, when `output_model.glb` is missing, and on non-zero subprocess exit.

### D3. Fast Pre-submission Validation for Image Inputs
- In `crates/core/src/task.rs`:
  - `Generative3DSpec::new_image` defaults `engine_preference` to `Some("triposr".to_string())` (`crates/core/src/task.rs:852`).
  - `TaskSpec::validate()` parses the engine into `Engine3D` via `spec.try_engine()?` and matches on it when `has_image` is true:
    - For `Engine3D::Auto` (`None`, `""`, `"auto"`), returns `"Image input is only supported with neural 3D engines (e.g. triposr); auto and procedural engines do not accept images"`.
    - For `Engine3D::Sdf` (`"sdf"`, `"tier0_sdf"`, `"tier0"`, `"tier-0"`, `"procedural"`, `"procedural_sdf"`), returns `"Tier 0 procedural SDF engine does not support image input (text prompt only)"`.
  - In `crates/cli/src/cli_app.rs`: `build_task_spec` invokes `spec.validate()?` before connecting to master.

### D4. Subprocess Diagnostic Stderr Extraction
- In `crates/worker/src/runner.rs`:
  - On non-zero child process exit, extract the last non-empty line of `stderr`.
  - Set `result.error = Some(err_msg)` where `err_msg` incorporates the last non-empty stderr line.

### D5. Symlink-Preserving Absolute Path Resolution
- In `crates/worker/src/gen3d/runner.rs`:
  - Replace `std::fs::canonicalize` with `std::path::absolute` (preserving verbatim prefix stripping on Windows without symlink traversal).
  - Preserves `sys.prefix` and virtual environment module resolution.
  - Python on `PATH` (`probe_executable_in_path`) is found by walking `PATH` directories, probing each candidate with `probe_python` (`rusty_grid_core::cluster_auth::probe_command`), and returning a clean absolute path (`to_clean_absolute_path`) instead of a bare `"python"` or `"python3"`.

### D6. Dynamic Engine Device Reporting
- In `crates/worker/src/runner.rs` and `gen3d/runner.rs`:
  - Scan subprocess stdout for `[ENGINE: <label>]`, `[DEVICE: <label>]`, `[ENGINE] <label>`, or `[DEVICE] <label>`.
  - Report the extracted label in `result.device_name`. If absent, do not fabricate `TripoSR-*`.

## Compatibility

- **Wire Compatibility**: Zero wire protocol schema changes.
- **Client Compatibility**: Existing CLI workflows using prompt-based generation continue working unchanged. Submissions attempting image reconstruction without specifying an image-capable engine receive fast, informative errors.

## Security

- **Path Traversal & Integrity**: Scripts and Python runtimes are resolved to verified absolute paths.
- **Fail-Closed Diagnostics**: Missing checkpoints fail immediately with clear audit trails.

## Test Plan

- `crates/worker/tests/test_3d1_honest_sdf.rs`:
  - `test_3d1_triposr_without_model_fails_honestly`: asserts exact failure text.
  - `test_3d1_triposr_with_arbitrary_checkpoint_still_fails_honestly`: asserts non-zero exit without model asset.
  - `test_3d1_auto_engine_defaults_to_sdf_tier`: asserts `Procedural-SDF-PureRust`.
  - `test_3d1_auto_engine_never_invokes_python_runtime`: trap script verification.
  - `test_3d1_auto_engine_defaults_to_sdf_tier_even_with_custom_script`: env override verification.
  - `test_3d1_sdf_tier_rejects_image_input_honestly`: asserts rejection on `auto` and all `Engine3D::Sdf` aliases (`sdf`, `tier0_sdf`, `tier0`, `tier-0`, `procedural`, `procedural_sdf`).
  - `test_3d1_success_path_device_name_comes_from_engine`: verifies dynamic device label.
  - `test_3d1_resolve_python_preserves_venv_sys_prefix`: verifies `sys.prefix` consistency.
- `crates/core/src/task.rs`:
  - `test_validate_generative_3d_sdf_aliases_and_auto_reject_image`: verifies `TaskSpec::validate()` rejects image input on `Engine3D::Auto` and all `Engine3D::Sdf` aliases with their respective diagnostic errors.
- `crates/cli/tests/test_cli_3d_submission.rs`:
  - `test_cli_3d_default_engine_with_image_fails_before_submit`: fast pre-submission failure.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Fast CLI `--image` validation for auto/default engine | `crates/core/src/task.rs`, `crates/cli/tests/` | `--image` with default engine fails before submit | Implemented |
| W2 | Exact failure cause assertion in tests | `crates/worker/tests/test_3d1_honest_sdf.rs` | Mutation removing reason text decisively fails test | Implemented |
| W3 | Ensure Python never runs for `auto` | `crates/worker/src/runner.rs`, `test_3d1_honest_sdf.rs` | Mutation removing auto routing decisively fails test | Implemented |
| W4 | Dynamic engine device reporting on success path | `crates/worker/src/runner.rs`, `test_3d1_honest_sdf.rs` | Restoring hardcoded label decisively fails test | Implemented |
| W5 | Virtual environment Python path resolution | `crates/worker/src/gen3d/runner.rs` | Venv Python retains `sys.prefix` | Implemented |
| W6 | Extract last stderr line into `result.error` | `crates/worker/src/runner.rs` | `result.error` contains exact diagnostic cause | Implemented |
| W7 | Add Design Record 0017 and update `README.md` | `docs/design/0017-*.md`, `README.md` | DR 0017 created and indexed in README | Implemented |

## Status log

- 2026-11-02: Design record 0017 drafted and implemented for PR #22 remediation.
- 2026-11-03: Updated D3 to reflect `TaskSpec::validate()` matching on parsed `Engine3D` across all `Engine3D::Sdf` aliases (`sdf`, `tier0_sdf`, `tier0`, `tier-0`, `procedural`, `procedural_sdf`), aligned missing-Python assertion in `test_3d1_honest_sdf.rs`, and removed obsolete `auto` fallback half from `test_sec5_missing_output_file_fails_under_explicit_engine`.
