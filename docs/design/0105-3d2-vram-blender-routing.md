# 0105: Master VRAM and Blender Capability Routing (`3D-2`)

Status: Implemented
Created: 2026-11-22    Updated: 2026-11-22

## Summary

Wires two missing capability gates into the master scheduler, speculative straggler reaper, and CLI preflight checks on Protocol 10 and Store v4:

1. **Blender capability routing**: `AutoRig` tasks requesting `preferred_provider == "blender"` (case-insensitive, trimmed) set `TaskView::requires_blender = true` and route exclusively to workers advertising `WorkerCapabilities::has_blender == true`.
2. **Preset VRAM default routing**: `SchedulerConfig::required_vram_mb` (`min_vram_fast_mb = 2048`, `min_vram_standard_mb = 4096`, `min_vram_high_mb = 6144`) is wired into scheduling, `AssignedLoad` reservations, `Shape` caching, `hold_back`, `gpu_demand`, `reaper::run_straggler_pass`, and CLI preflight checks when `--vram-mb` is omitted, while preserving CPU-only execution for `Auto`, `Sdf`, and non-GPU `"fast"`/`"standard"` `Generative3D` defaults (`BUG-3`).

## Context

- `WorkerCapabilities` (`crates/core/src/capabilities.rs`) already advertised `has_blender: bool` and `vram_mb: Option<u64>`, and `SchedulerConfig` (`crates/master/src/scheduler.rs`) already held `min_vram_fast_mb` (2048), `min_vram_standard_mb` (4096), and `min_vram_high_mb` (6144).
- However, `TaskView` (`crates/master/src/queue.rs`), `WorkerAttrIndex` / `RegisteredIndex` (`crates/master/src/worker_index.rs`), `Engine` (`crates/master/src/scheduler.rs`), and `run_straggler_pass` (`crates/master/src/reaper.rs`) never checked `has_blender`, allowing `--preferred-provider blender` tasks to land on workers without Blender.
- `SchedulerConfig::required_vram_mb` had zero callers and checked `spec.engine().is_cpu_capable()`, which after `BUG-3` returns `true` for all `Engine3D` variants (`Auto`, `Sdf`, `TripoSr`, `Trellis`). Consequently, `high`-quality `Generative3D` tasks without explicit `--vram-mb` bypassed VRAM filtering and could land on 2 GB GPU workers or CPU-only nodes.
- Meanwhile, existing `BUG-3` integration guarantees (`test_cli_3d_default_submit_succeeds_on_cpu_worker` and `test_cli_3d_triposr_fails_honestly_on_cpu_worker`) require default `Auto` and non-GPU `TripoSr` `"standard"` submissions without `--gpu` / `--vram-mb` to continue reaching CPU workers without stalling in the queue.

## Goals

- **G1**: Ensure `AutoRig` tasks with `preferred_provider` equal to `"blender"` (case-insensitive, trimmed) only schedule (including speculative straggler copies) onto workers advertising `has_blender == true`, and report clear skip / unfulfillable diagnostics when no Blender worker exists.
- **G2**: Enforce `SchedulerConfig` preset VRAM thresholds (`2048` / `4096` / `6144` MB) across placement, `Shape` memoization, `AssignedLoad` cross-pass accounting, `hold_back`, Stage 6/7 GPU preservation/priority, `reaper::run_straggler_pass`, and CLI preflight warnings so a `high` task skips a 2 GB (or 4 GB) GPU worker even when `--vram-mb` is omitted.
- **G3**: Preserve `BUG-3` behavior for pure-Rust `Sdf` (all qualities) and non-GPU `"fast"`/`"standard"` `Auto` and `TripoSr` defaults, and preserve `0.4.x` Protocol 10 / Store v4 wire and store compatibility.

## Non-goals

- Changing `crates/core` wire structs (`TaskRequirements`, `Generative3DSpec`, `AutoRigSpec`, `WorkerCapabilities`) or bumping `PROTOCOL_VERSION_CURRENT` (`10`) or `STORE_FORMAT_VERSION` (`4`).
- Implementing `3D-3` AutoRig option validation or refusal (`preferred_provider` fallback vs hard error on the worker side is handled separately in `3D-3`).
- Requiring a host `blender` installation or discrete GPU to compile or run `3D-2` routing tests.

## Design

### D1: Blender Capability Routing (`has_blender` / `requires_blender`)

- **`crates/master/src/queue.rs`**:
  - `TaskView` adds `pub requires_blender: bool`, populated by `task_requires_blender(task)` (`true` for `TaskSpec::AutoRig(spec)` when `spec.preferred_provider.as_deref().is_some_and(|p| p.trim().eq_ignore_ascii_case("blender"))`).
- **`crates/master/src/worker_index.rs`**:
  - `WorkerAttrIndex` builds a `blender: BitSet` from `worker.capabilities.has_blender`, exposes `any_has_blender()`, and intersects `self.blender` inside `apply_caps_and_features` whenever `CapRequirements::requires_blender` is `true`.
  - Bundling `(requires_gpu, requires_blender, required_features)` into `pub(crate) struct CapRequirements` keeps `free_candidate_mask`, `unblocked_static_mask`, `matching_static_mask`, and `RegisteredIndex::any_fits_all` concise and enforced across all scheduling and `hold_back` paths.
- **`crates/master/src/scheduler.rs`**:
  - `Shape` includes `requires_blender: bool` so a held or unfulfillable `"blender"` task in `Engine::dead` never blocks a `"builtin"` `AutoRig` task behind it.
  - `Engine::skip_reason` emits `ScheduleSkipReason::ClusterLacksCapability("cluster lacks workers with Blender capability".into())` when `view.requires_blender && !self.worker_index.attrs().any_has_blender()`.
  - `Engine::place` (`fail_unfulfillable_tasks`) emits `Verdict::Unfulfillable("Cluster lacks workers with Blender capability".into())` when no registered worker has Blender, and appends `, blender=true` to the fallback resource mismatch message only when `view.requires_blender` is `true` (preserving exact existing diagnostic strings for all non-Blender tasks).
- **`crates/master/src/reaper.rs`**:
  - `run_straggler_pass` filters both `connected_workers` and `schedulable` candidates with `(!cand.view.requires_blender || w.capabilities.has_blender)`.

### D2: Preset VRAM Default Routing (`SchedulerConfig::required_vram_mb`)

- **Stateless effective VRAM rule** (implemented identically in `SchedulerConfig::required_vram_mb`, `TaskView::gen3d_vram_tier`, and CLI preflight):
  1. If `task.requirements.effective_vram_mb() > 0`, use the explicit `vram_mb`.
  2. Else if `TaskSpec::Generative3D(spec)`:
     - `Engine3D::Sdf` -> `0` MB (Tier 0 CPU procedural engine uses no GPU VRAM unless `--vram-mb` is passed explicitly).
     - Else if `task.requirements.gpu_required || spec.quality.trim().eq_ignore_ascii_case("high")`:
       - `"fast"` -> `config.min_vram_fast_mb` (default `2048` MB)
       - `"high"` -> `config.min_vram_high_mb` (default `6144` MB)
       - `_` (`"standard"`) -> `config.min_vram_standard_mb` (default `4096` MB)
     - Else (`!gpu_required` and `"fast"`/`"standard"`) -> `0` MB, keeping `Auto` and `TripoSr` `"standard"` defaults CPU-eligible (`BUG-3`).
  3. Else -> `0` MB.
- **Implied GPU requirement**: Whenever `req_vram > 0`, `effective_requires_gpu = view.requires_gpu || req_vram > 0` is `true` in `Shape`, `Engine::place`, `Engine::skip_reason`, `Engine::apply_stage6_and_7`, `Engine::rank`, `matchmake_inner`, `gpu_demand`, `schedule_batch`, and `reaper::run_straggler_pass`. This prevents Stage 6 (`preserve_gpu_for_gpu_tasks`) and `prefer_cpu_only` from steering `high` tasks away from GPU workers onto CPU workers.
- **Cross-pass VRAM reservation**: `TaskQueue::assigned_loads_with<F: Fn(&Task) -> u64>(&self, vram_for_task: F)` (in `crates/master/src/queue.rs`) lets `WorkloadScheduler::schedule_batch` and `reaper::run_straggler_pass` accumulate preset VRAM for active (`Scheduled`, `Running`) and `adoptable_by`-held tasks without mutating `TaskRequirements` or breaking `TaskQueue::assigned_loads()`.

### D3: CLI Preflight Checks (`crates/cli/src/cli_app/submit.rs`)

- Adds `any_connected_worker_has_blender` and `preflight_required_vram_mb` in `crates/cli/src/cli_app/submit.rs`.
- `oxideswarm submit` fetches `ClientMessage::ListWorkers` when `requires_blender` or `preflight_vram_mb.is_some()` (alongside existing placement/workspace triggers) and emits non-blocking `Warning: ...` lines to `stderr` if no connected worker advertises Blender or sufficient GPU VRAM. CPU-only `Sdf`/`Auto`/`TripoSr` `"standard"` defaults emit zero false VRAM warnings.

## Compatibility

- `PROTOCOL_VERSION_CURRENT == 10`, `PROTOCOL_VERSION_MIN_COMPATIBLE == 9`, `STORE_FORMAT_VERSION == 4`, and `EXTENSION_REGISTRY == &[]` remain untouched.
- `wire_layout_tests` and `store_layout_is_frozen` pass with zero wire or journal layout changes.
- `crates/master` continues to compile under `--no-default-features`, default features, and `--features 3d`.

## Security

- Saturating arithmetic (`saturating_add` / `saturating_sub`) is preserved in `assigned_loads_with`, `Seat::placed.vram_mb`, and `run_straggler_pass`, preventing integer overflow on large `vram_mb` values.
- No new network messages, filesystem paths, or privilege boundaries are introduced.

## Test plan

- **`crates/master/tests/test_3d2_routing.rs`** (runs under both default features and `--features 3d` using `WorkerCapabilities::with_blender` / `with_vram_mb` and `WorkerMessage::Register`):
  - `--preferred-provider blender` (`AutoRig`, including mixed case/whitespace) routes only to `has_blender == true` workers, skips with `ClusterLacksCapability` when absent, fails immediately under `fail_unfulfillable_tasks`, and does not taint `builtin` `AutoRig` tasks in `Shape` negative-caching.
  - `Generative3D` `high` (`Auto` and `TripoSr`) skips 2 GB (`2048 MB`), 4 GB (`4096 MB`), and `6143 MB` workers and lands on `6144+ MB`, preserves CPU-only `Sdf` `high` and `Auto`/`TripoSr` `standard` defaults, honors custom `SchedulerConfig::min_vram_*_mb`, separates `high` vs `fast` in `Shape`, reserves preset VRAM across passes via `assigned_loads_with`, holds back saturated 8 GB GPU workers, and checks `reaper::run_straggler_pass` for both `requires_blender` and preset VRAM.
- **`crates/cli/tests/test_3d2_routing.rs`** (default features + `--features 3d`):
  - End-to-end `oxideswarm submit` against `MasterServer` + mock `WorkerMessage::Register` workers verifying `AutoRig --preferred-provider blender` landing + stderr preflight warning, `Generative3D --quality high` skipping a 2 GB worker and landing on an 8 GB worker + stderr VRAM warning, and zero false VRAM warnings on `Auto` / `Sdf` defaults.
- **Regression suites**: `test_cli_3d_submission` (`--features 3d`), `test_challenger_m4_scheduler_runner` (`--features 3d`), `wire_layout_tests`, `ox-mode test size`, and `ox-mode doc verify`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| 3D-2 | Wire VRAM and Blender capability routing into Master scheduler, reaper, and CLI preflight | `crates/master/src/{queue.rs,worker_index.rs,scheduler.rs,reaper.rs}`, `crates/cli/src/cli_app/submit.rs`, `crates/master/tests/test_3d2_routing.rs`, `crates/cli/tests/test_3d2_routing.rs`, `docs/design/0105-3d2-vram-blender-routing.md` | `--preferred-provider blender` routes only to Blender workers; `high` non-SDF `Generative3D` skips 2 GB workers while preserving `BUG-3` CPU defaults; all verification gates pass | Implemented |

## Status log

- 2026-11-22: Created design record `0105` and implemented Blender capability routing and preset VRAM default routing (`3D-2`).
