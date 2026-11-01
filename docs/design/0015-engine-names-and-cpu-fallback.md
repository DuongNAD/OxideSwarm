# 0015: 3D Engine Name Validation, Strict Fallback Removal, and Default CPU Execution

Status: Implemented
Created: 2026-11-01    Updated: 2026-11-01

## Summary

Enforces strict 3D generation engine name validation and changes default 3D task semantics to CPU execution:
- Default 3D submissions (`submit --prompt <PROMPT>`) execute on CPU workers via procedural SDF without requiring a GPU or `--gpu`.
- The `--engine` flag and core task spec strictly validate engine names (`sdf`, `triposr`, `auto`), refusing unknown names (e.g. `rodin`) with explicit errors across CLI, core builders, Agent Mesh bridge envelopes, and wire codecs.
- Handles unsupported `Trellis` engine with clean fail-fast diagnostic error (`Trellis 3D neural engine is not supported; use sdf or triposr`).
- Ensures wire codec parity between JSON and Bincode for `Engine3D` representations.
- Eliminates process-wide environment variable mutations in tests via child process isolation.

## Context

Behaviour before this change:

| Area | Behaviour | Reference |
|---|---|---|
| Default 3D Engine | Default 3D submission routed to `TripoSr` with `requires_gpu = true`. On CPU-only clusters, tasks queued indefinitely or timed out. | `crates/core/src/task.rs:641`, `crates/cli/src/cli_app.rs:2386` |
| Unknown Engine Fallback | Unknown engine names (e.g. `rodin`) silently fell back to `Engine3D::Auto` via `From<&str>`, producing SDF meshes instead of reporting an error. | `crates/core/src/task.rs:704`, `crates/agent_mesh/src/bridge.rs:569` |
| Codec Parity | JSON and Bincode diverged when deserializing unknown or uppercase engine names: JSON produced parse errors while Bincode produced `None` / `Auto`. | `crates/core/src/task.rs:803` |
| Dead Engine References | `Trellis` was exposed as an available engine in `Engine3D` and CLI help, but had no backend implementation and routed to TripoSR. | `crates/core/src/task.rs:629`, `scripts/agent_node.py:721` |
| Test State Isolation | Integration tests in `test_cli_3d_submission.rs` invoked `std::env::set_var("OXIDESWARM_HOME", ...)` to prevent reading `~/.oxideswarm/cluster.secret`, causing race conditions. | `crates/cli/tests/test_cli_3d_submission.rs:14,88,196` |

## Goals

- **G1** Make default 3D generation CPU-capable so `submit --prompt <PROMPT> -o <OUT> --wait` succeeds immediately on CPU-only worker nodes via procedural SDF without `--gpu`.
- **G2** Strictly reject unknown or unsupported engine names across all boundaries: CLI argument parsing, core task builder (`Generative3DSpec::with_engine`), Agent Mesh bridge envelopes (`grid_submit`), and wire serialization codecs.
- **G3** Enforce codec parity: ensure JSON and Bincode reject unknown engine names with identical failure semantics.
- **G4** Cleanly intercept unsupported `Trellis` requests and report explicit diagnostic failures without silent fallback.
- **G5** Prevent queue hangs when `--engine triposr` is requested on CPU-only clusters: execute on CPU workers to report `TripoSR model not available` or fail cleanly with an informative diagnostic.
- **G6** Isolate CLI integration tests from host credentials using child process `Command::env` instead of process-wide `std::env::set_var`.

## Non-goals

- Implementing full TripoSR neural weight checkpoints or deep learning dependencies in the core repository.
- Implementing Trellis sparse-structure diffusion model generation.
- Changing wire protocol framing format or length-delimited discriminators.

## Design

### D1. CPU-Capable Default 3D Execution Semantics
In `crates/core/src/task.rs` and `crates/cli/src/cli_app.rs`:
- `Generative3DSpec::default()` selects `Engine3D::Auto`, which maps to CPU procedural SDF generation.
- `requires_gpu()` returns `false` for `Engine3D::Auto`, `Engine3D::Sdf`, and `Engine3D::TripoSr` (CPU mode supported).
- In `crates/cli/src/cli_app.rs`, `requirements.gpu_required` is `false` unless explicitly requested via `--gpu` or `--type gpu`, or required by spec (`Engine3D::Trellis`).
- Default 3D tasks are immediately matched and executed by standard CPU workers, producing a valid glTF 2.0 binary asset.

### D2. Strict Engine Name Validation and Fallback Elimination
- Remove `impl From<&str> for Engine3D` that defaulted unmatched strings to `Auto`.
- Implement `std::str::FromStr` and `TryFrom<&str>` for `Engine3D`:
  - Valid strings: `"auto"`, `"sdf"`, `"triposr"` (case-insensitive ASCII).
  - Invalid strings (e.g. `"rodin"`): return `Err("unknown 3D engine '{name}'; valid engines are: auto, sdf, triposr")`.
- Store `engine_preference: Option<String>` in `Generative3DSpec`, preserving raw input string so validation fails with explicit error.
- Update `TaskSpec::validate()` to ensure engine names are verified before queue admission.

### D3. Wire Codec Parity Between JSON and Bincode
- Both human-readable (JSON) and binary (Bincode) codecs serialize `Engine3D` consistently.
- Invalid engine names in incoming frames fail deserialization immediately, preventing corrupted tasks from entering the queue or silently changing engine semantics.

### D4. Interception and Diagnostics for Unsupported Engines
- Intercept `Engine3D::Trellis` at worker execution and return explicit failure: `Trellis 3D neural engine is not supported; use sdf or triposr`.
- CLI help strings in `crates/cli/src/cli_app.rs`: `--engine: auto, sdf, triposr (default: auto)`.

### D5. CPU Execution & Diagnostic Reporting for TripoSR
- Update worker runner so that `Engine3D::TripoSr` tasks routed to CPU workers run `entrypoint.py` in CPU mode.
- If checkpoint weights are absent, the worker fails with `TripoSR model not available`, fulfilling the 3D-1 requirement within seconds rather than hanging.

### D6. Child Process Environment Isolation for CLI Tests
- In `crates/cli/tests/test_cli_3d_submission.rs`, remove `std::env::set_var("OXIDESWARM_HOME", ...)`.
- Drive CLI tests through `Command::new(BIN)` with `.env("HOME", temp_dir.path())`, `.env("USERPROFILE", temp_dir.path())`, and `.env_remove("OXIDESWARM_HOME")`.
- Wrap CLI test futures in 15-second timeouts to catch potential hangs early.

## Compatibility

- **Wire Compatibility**: Positional indices for remaining variants are preserved. `Auto` is 0, `Sdf` is 1, `TripoSr` is 2, `Trellis` is 3.
- **Client Impact**: Clients passing unknown engine strings like `rodin` receive an immediate configuration error instead of an unexpected SDF mesh.
- **Upgrade Order**: Standard coordinator-first rollout.

## Security

- **Input Validation**: Strict engine validation prevents unverified engine names from propagating to execution environments.
- **Credential Isolation**: Removing `set_var` ensures parallel tests cannot inadvertently expose or overwrite host secrets in `~/.oxideswarm`.

## Test Plan

- `crates/core/src/task.rs` unit tests:
  - `test_engine_from_str_valid`: verifies `sdf`, `triposr`, `auto`.
  - `test_engine_from_str_invalid`: asserts `rodin` returns error.
  - `test_engine_codec_parity`: verifies JSON and Bincode agree on valid variants and reject unknown variants.
- `crates/cli/tests/test_cli_3d_submission.rs`:
  - `test_cli_3d_sdf_submission_and_export` (child process CLI).
  - `test_cli_autorig_submission_and_export` (child process CLI).
  - `test_cli_negative_validations` (rejection of bad inputs).
  - `test_cli_3d_default_submit_succeeds_on_cpu_worker` (wrapped in 15s timeout).
  - `test_cli_3d_triposr_fails_honestly_on_cpu_worker` (fails fast on CPU cluster).
  - `test_cli_3d_explicit_gpu_engine_without_gpu_flag` (trellis triggers GPU requirement).
- `crates/agent_mesh/src/bridge.rs` integration tests:
  - `test_bridge_refuses_unknown_engine_envelope`: verifies `grid_submit` with `rodin` returns error.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Implement strict `parse` / `TryFrom` for `Engine3D` and remove silent fallback to `Auto` | `crates/core/src/task.rs` | `Engine3D::parse("rodin")` returns error | Implemented |
| W2 | Enforce wire codec parity for `Engine3D` in JSON and Bincode | `crates/core/src/task.rs` | Codec tests show identical rejection across codecs | Implemented |
| W3 | Allow `TripoSr` on CPU worker to report model status | `crates/core/src/task.rs`, `crates/worker/src/runner.rs` | `--engine triposr` fails quickly with diagnostic on CPU cluster | Implemented |
| W4 | Intercept unsupported `Trellis` engine with informative failure | `crates/worker/src/runner.rs` | Trellis submission fails with clear diagnostic | Implemented |
| W5 | Eliminate `set_var` in `test_cli_3d_submission.rs` | `crates/cli/tests/test_cli_3d_submission.rs` | 0 `set_var` calls; child process env isolated | Implemented |
| W6 | Add timeout guards to CLI submit tests | `crates/cli/tests/test_cli_3d_submission.rs` | Tests bounded by 15-second timeouts | Implemented |
| W7 | Add Design Record 0015 and update `README.md` | `docs/design/0015-*.md`, `README.md` | DR 0015 created and README indexed | Implemented |

## Status log

- 2026-11-01: Design record 0015 drafted and implemented for PR 21 remediation.
