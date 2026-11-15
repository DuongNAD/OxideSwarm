# 0067: Task video memory requirements and scheduler accounting

Status: Implemented
Created: 2026-11-15    Updated: 2026-11-15

## Summary

Adds optional `vram_mb: Option<u64>` to `TaskRequirements` (protocol version `9`, store format `3`),
`--vram-mb <MB>` on `oxideswarm submit`, `vram_mb` on the MCP `submit` tool and Web UI `POST /api/tasks`,
and per-worker VRAM budget tracking in `TaskQueue` and `WorkloadScheduler` (`FEAT-9a`, decision
`Q56`).

## Context

Previously (`FEAT-9` and decision `Q56` in `docs/design/0064-research-of-2026-11-13-and-route.md`):

- Workers advertised `WorkerCapabilities::vram_mb: Option<u64>` (`crates/core/src/capabilities.rs`)
  via hardware detection (`detect_gpu_vram_mb`) or `oxideswarm worker --gpu --vram-mb <MB>`, while
  `TaskRequirements` (`crates/core/src/task.rs`) carried `cpu_cores`, `ram_mb`, and `gpu_required`,
  with no field for video memory.
- `SchedulerConfig::required_vram_mb` (`crates/master/src/scheduler.rs`) returned heuristic defaults
  only for `TaskSpec::Generative3D` and had no caller in placement or admission.
- On multi-slot GPU workers (`--slots > 1`), concurrent GPU jobs could overcommit physical VRAM and
  fail with out-of-memory errors.
- Spike `R20` (`2026-11-15`, Hub records `test_runs/mac_r20_gpu_probe.md` and
  `test_runs/win_r20_gpu_probe.md`) confirmed native GPU compute works inside macOS Seatbelt and
  Windows AppContainer under `--isolation fs`, motivating immediate multi-slot VRAM accounting.

## Goals

- **G1**: Let tasks declare a minimum video memory requirement in MB (`TaskRequirements::vram_mb`)
  via CLI (`--vram-mb`), MCP (`vram_mb`), and Web UI (`vram_mb`), implying GPU execution whenever
  set above zero.
- **G2**: Enforce static VRAM capability checks (`WorkerCapabilities::satisfies`, `WorkerIndex`, and
  scheduler unfulfillable reporting) so tasks with a positive `vram_mb` only match GPU workers that
  report at least that many MB of VRAM.
- **G3**: Track active (`Scheduled`, `Running`, and active `adoptable_by`-held) and same-pass
  placed VRAM reservations per worker so concurrent multi-slot scheduling never overcommits a
  worker's advertised VRAM and large-VRAM waiting tasks hold back saturated workers rather than
  starving behind smaller tasks.
- **G4**: Follow `docs/design/0040-protocol-versioning.md` (`FEAT-8`) cleanly by bumping
  `PROTOCOL_VERSION_CURRENT` and `PROTOCOL_VERSION_MIN_COMPATIBLE` to `9`, bumping
  `STORE_FORMAT_VERSION` to `3`, keeping clear `Incompatible protocol version` errors for v5 and
  v6 to v8 bincode `SubmitTask` frames, and regenerating `crates/core/tests/data/wire_layout.txt`.

## Non-goals

- Live per-process free VRAM polling via NVML, DXGI, or Metal on heartbeats. Because heartbeats
  report no dynamic free VRAM metric, the master reserves declared `vram_mb` against total
  advertised `WorkerCapabilities::vram_mb` across all active (`Scheduled`, `Running`, and held)
  tasks.
- Automatic unified-memory VRAM auto-detection on macOS Apple Silicon or synthetic capacity on
  `--simulate-gpu` workers without `--vram-mb`; those workers report `vram_mb = None`, take GPU
  tasks that omit `vram_mb`, and require `oxideswarm worker --gpu --vram-mb <MB>` to claim fixed
  VRAM capacity.
- Multi-GPU per-device selection or CUDA `CUDA_VISIBLE_DEVICES` partitioning within a single host.

## Design

### D1 Wire representation, protocol version 9, and store format 3

- `TaskRequirements` (`crates/core/src/task.rs`) appends `#[serde(default)] pub vram_mb: Option<u64>`
  as its final field. No `skip_serializing_if` attribute is used because `bincode` encoding is
  positional (enforced by `skip_serializing_if_is_kept_off_bincode_types` in
  `crates/core/src/wire_layout_tests.rs`).
- `TaskRequirements::with_vram_mb(vram_mb)` normalizes `0` to `None` and for positive values sets
  `vram_mb = Some(vram_mb)` and `gpu_required = true`. Helper methods `effective_vram_mb()` and
  `requires_gpu_or_vram()` ensure direct struct initializers that set `vram_mb: Some(n)` with `n > 0`
  and `gpu_required: false` still require GPU capability during routing and capability checks.
- `PROTOCOL_VERSION_CURRENT` and `PROTOCOL_VERSION_MIN_COMPATIBLE` (`crates/core/src/protocol.rs`)
  bump from `8` to `9`. `LegacyPreV9TaskRequirements` freezes the v3 to v8 `TaskRequirements`
  bincode layout inside `LegacyV5Task` and a new `LegacyV6ToV8Task` /
  `LegacyV6ToV8BinaryClientMessage::SubmitTask`, so a v9 master answers v5 and v6 to v8 bincode
  `SubmitTask` frames with `ProtocolError::IncompatibleVersion` instead of a generic bincode EOF
  error.
- `crates/core/src/wire_layout_tests.rs` sets `vram_mb: Some(6_144)` in `sample_task()` and
  updates `crates/core/tests/data/wire_layout.txt`.
- `STORE_FORMAT_VERSION` (`crates/master/src/store.rs`) bumps from `2` to `3` while keeping
  `MIN_STORE_FORMAT_VERSION = 1`. Clean-shutdown snapshots (JSON) and empty format-1 or format-2
  journals carry over to format 3 at startup, while uncompacted records of older `Task` layouts
  are refused cleanly.

### D2 Static capability eligibility and unfulfillable tasks

- `WorkerCapabilities::satisfies` (`crates/core/src/capabilities.rs`),
  `worker_index::statically_fits_caps` (`crates/master/src/worker_index.rs`), and
  `scheduler::statically_fits` (`crates/master/src/scheduler.rs`) share the same static predicate:
  when `effective_vram_mb() == 0`, any GPU worker (`vram_mb: None` or `Some(_)`) fits; when
  `effective_vram_mb() > 0`, the worker must advertise `can_execute_gpu()` and
  `vram_mb >= Some(req_vram)`.
- When `--fail-unfulfillable-tasks` is enabled, `Engine::place` includes `vram_mb=<N>` in the
  unfulfillable error message whenever `effective_vram_mb() > 0` (leaving existing messages for
  `vram_mb == 0` unchanged).

### D3 Master VRAM reservation and hold-back

- `AssignedLoad` (`crates/master/src/queue.rs`) adds `pub vram_mb: u64`. Unlike
  `unstarted_ram_mb` (which only counts `Scheduled` tasks because heartbeats report live free
  RAM), `TaskQueue::assigned_loads` sums `effective_vram_mb()` across every active (`Scheduled` or
  `Running`) task on the worker and every task held under an active `adoptable_by` window.
- `TaskView::requires_gpu` uses `task.requirements.requires_gpu_or_vram() || task.spec.requires_gpu()`.
  `Shape` (`crates/master/src/scheduler.rs`) includes `vram_mb: u64`, preventing negative-shape
  memoization from incorrectly skipping a small-VRAM task after a larger-VRAM task fails to place.
- `Seat` tracks `used_vram_mb() = base.vram_mb + placed.vram_mb`. During scheduling,
  `is_seat_vram_short(seat, req_vram_mb)` rejects a worker whenever
  `cap.saturating_sub(seat.used_vram_mb()) < req_vram_mb`, and placing a task increments
  `seat.placed.vram_mb`.
- When a GPU worker statically fits `req_vram_mb` and is currently carrying work
  (`seat.used_vram_mb() > 0`), dynamic VRAM exhaustion adds that worker to `full` so
  `hold_back` reserves it and prevents narrower tasks later in the queue from starving large-VRAM
  GPU jobs. `hold_back` breaks ties by minimizing `(slots_to_free, vram_to_free, idx)`.
- `WorkloadScheduler::gpu_demand` treats `requires_gpu_or_vram()` as GPU demand so
  `preserve_gpu_for_gpu_tasks` protects GPU workers even when a user submits a `--command` or
  `--shell` job with `--vram-mb <MB>` and no explicit `--gpu`.

### D4 CLI, MCP, and Web UI submission

- `oxideswarm submit` (`crates/cli/src/cli_app.rs`) adds `--vram-mb <MB>` (alias `--vram`),
  normalized with `> 0`, implies `--gpu` when non-zero, populates `TaskRequirements::vram_mb`, and
  emits a preflight stderr warning if no connected GPU worker advertises at least `<MB>` MB VRAM.
- MCP `submit` (`crates/cli/src/mcp.rs`) adds an integer `vram_mb` schema field, rejects negative
  or non-integer values upfront, sets `TaskRequirements::with_vram_mb`, and updates
  `check_matching_worker_warning` to verify GPU capability and `vram_mb` against `WorkerList`.
- Web UI `POST /api/tasks` (`crates/master/src/web_ui.rs`) accepts optional `vram_mb: Option<u64>`
  and maps non-zero values through the same semantics.

## Compatibility

- Wire breaking change guarded by `FEAT-8`: `PROTOCOL_VERSION_CURRENT` and
  `PROTOCOL_VERSION_MIN_COMPATIBLE` advance from `8` to `9` (`MIN == CURRENT`). Upgrade the
  master first, then restart workers and CLI/MCP clients (`DEPLOYMENT.md` section 3.5).
- `STORE_FORMAT_VERSION` advances from `2` to `3`. Cleanly shut down masters running protocol `8`
  with an empty journal upgrade automatically on start.

## Security

- `vram_mb` is a scheduler capacity hint bounded by `u64` with saturating addition (`saturating_add`
  and `saturating_sub`), preventing integer wraparound on adversarial task requirements or worker
  advertisements.
- MCP validates `vram_mb` as a non-negative integer before constructing a `Task`.
- No filesystem permissions or isolation rules change in this record.

## Test plan

- `crates/core`: `TaskRequirements` (`with_vram_mb`, legacy JSON default, bincode/JSON round-trip),
  `WorkerCapabilities::satisfies` VRAM cases, `protocol` v9 round-trip + v5/v8 refusal error tests,
  and regenerated `wire_layout_tests`.
- `crates/master`: `store` format-1 and format-2 clean upgrade tests, `queue` `assigned_loads`
  VRAM tracking (`Scheduled`, `Running`, `adoptable_by`, terminal release), `worker_index` static
  VRAM filtering, and `scheduler` unit + `WorkloadScheduler` async tests covering single-pass
  multi-slot packing, cross-pass `Running` reservation, `hold_back` anti-starvation, static
  `--fail-unfulfillable-tasks`, and `preserve_gpu_for_gpu_tasks` (`m4_scheduling`).
- `crates/cli`: CLI `--vram-mb` flag parsing, MCP `submit` `vram_mb` validation and warning tests
  (`mcp_server_e2e`, `test_mcp_concurrent_and_deadlines`), and `test_hello_probe_e2e` including
  v8-client-to-v9-master refusal.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| FEAT-9a | Task `vram_mb` field (protocol v9, store v3), CLI `--vram-mb`, MCP `vram_mb`, Web UI `vram_mb`, and master scheduler VRAM budget tracking (`Q56`) | `crates/core/src/task.rs`, `crates/core/src/capabilities.rs`, `crates/core/src/protocol.rs`, `crates/core/src/wire_layout_tests.rs`, `crates/core/tests/data/wire_layout.txt`, `crates/master/src/store.rs`, `crates/master/src/queue.rs`, `crates/master/src/worker_index.rs`, `crates/master/src/scheduler.rs`, `crates/master/src/web_ui.rs`, `crates/cli/src/cli_app.rs`, `crates/cli/src/mcp.rs`, `crates/cli/tests/test_hello_probe_e2e.rs` | Wire layout golden test, workspace tests, `m4_scheduling`, `mcp_server_e2e`, `test_hello_probe_e2e`, and `ox-mode doc verify` pass | Implemented |

## Status log

- 2026-11-15: Written and implemented for owner decision `Q56` (`FEAT-9a`), bumping protocol
  version `8` to `9` and store format `2` to `3` across `core`, `master`, and `cli`.
