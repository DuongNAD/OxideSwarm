# 0103: Sharded runs and Rust build-once mode (`AX-2`)

Status: Implemented
Created: 2026-11-22    Updated: 2026-11-22

## Summary

`oxideswarm shard --count <N>` and the MCP `shard` tool fan out `N` parallel shard tasks over Protocol 10 without changing the wire format or master store. A `ListWorkers` pre-flight check fails fast when zero eligible workers match, Smooth Weighted Round-Robin spreads shards proportionally to `slots:<n>` tags while injecting 1-based `OX_SHARD_INDEX` and `OX_SHARD_TOTAL`, and `--build-once` builds one `cargo nextest archive` per `(os, arch)` and passes the output `BlobFile` in shard `Task::inputs` alongside a pure-`std` JUnit XML merger (`--junit-out`).

## Context

- [Design 0056](0056-frontier-program.md) (`D2` part `A2`, `R2`, `M1`) specifies client-side sharded runs (`oxideswarm shard --count <n>`), runner recipes, `--build-once` via `cargo nextest archive` / `--workspace-remap`, and JUnit XML merging with zero wire changes.
- [Design 0101](0101-matrix-fanout.md) (`AX-1`) introduced `crates/cli/src/matrix.rs` primitives (`listed_as_connected`, `worker_matches_cell`, `safe_deadline`, `ensure_non_symlink_cell_dir`, `CellStatus`) and `RunSummary` (`crates/cli/src/run_summary.rs`).
- Master's `WeightedLeastLoaded` scheduler (`crates/master/src/scheduler.rs`) ranks candidates by `load / cpu_cores` rather than `advertised_slots`, so client-side SWRR worker pinning ensures proportional `slots:<n>` spreading (`crates/core/src/placement.rs`) and `(os, arch)` archive affinity.
- `crates/worker/src/runner/workloads.rs` forbids combining `Task::workspace` with `Task::inputs` or `Task::staged_files`, and `WorkspaceGuard` serializes per workspace, so `--build-once` shard tasks use `Task::inputs` (`TreeManifest` + archive `BlobFile`) instead of `Task::workspace`.
- Master `pin_result_outputs` (`crates/master/src/server/tasks_and_blobs.rs`) retains `TaskResult::outputs` blobs for `DEFAULT_OUTPUT_PIN_RETENTION` (`600` s), and `admit_blob_task` re-pins `Task::inputs` directly from `BlobStore` without client download or re-upload.

## Goals

- G1. Run `N` (`1..=256`) parallel shard tasks from `oxideswarm shard` and MCP `shard`, injecting 1-based `OX_SHARD_INDEX` (`1..=N`) and `OX_SHARD_TOTAL` (`N`).
- G2. Fail immediately via `ListWorkers` pre-flight when zero eligible workers match, before scanning or uploading blobs.
- G3. Spread shards across eligible workers proportionally to `slots:<n>` tags (falling back to `cpu_cores`) via Smooth Weighted Round-Robin (`spread_shards_swrr`).
- G4. Support Rust `--build-once`: scan source once with `.gitignore` awareness (skipping `.git` and top-level `target/`), build one `cargo nextest archive` per distinct `(os, arch)`, abort remaining builders on failure before dispatching shards, and attach the archive `BlobFile` + source `TreeManifest` via `Task::inputs`.
- G5. Support `--fail-fast` (`CancelTask` to sibling in-flight shards), per-shard `--output-dir/<shard_slug>/` symlink-safe isolation, and pure-`std` `--junit-out` JUnit XML merging.
- G6. Preserve Protocol 10 (`PROTOCOL_VERSION_CURRENT = 10`) and Store v4 (`STORE_FORMAT_VERSION = 4`) invariance.

## Non-goals

- Modifying wire structs, adding batch RPCs, or altering Master durable journal layout (`AX-3` and `CACHE-1` are separate items).
- Cross-run persistent build caching across separate invocations (`CACHE-1`).
- Combining `--workspace` with `--build-once`, `--input`, or multi-worker sharding.

## Design

### D1: CLI and MCP surface (`crates/cli/src/cli_app/shard.rs`, `crates/cli/src/mcp/shard.rs`)

- **CLI**: `Commands::Shard(ShardArgs)` in `crates/cli/src/cli_app.rs` exposes `--count`, `--build-once`, `--build-source`, `--junit-out`, `--junit-glob`, `--output-dir`, `--fail-fast`, `--json`, placement/resource/env/input/output/workspace/auth flags matching `SubmitArgs`, and trailing `-- <ARGS...>`.
- **MCP**: `crates/cli/src/mcp.rs` registers a flat-schema `shard` tool (`required: ["count"]`, no top-level `anyOf`/`oneOf`/`allOf`) with `-32602` validation in `validate_tool_arguments`, `ctx.cancel_rx` cancellation, `state.record_submitted`, bounded per-shard output (`split_stream_budgets` + `truncate_stream_for_llm`), and working-directory confinement for `files`, `workspace`, `output_dir`, `build_source`, and `junit_out`.

### D2: `OX_SHARD_INDEX`, `OX_SHARD_TOTAL`, and `slots:` SWRR spreading (`crates/cli/src/shard.rs`)

- `filter_eligible_workers` filters `ListWorkers` results using `listed_as_connected` and `worker_matches_cell` (plus `tool:cargo-nextest*` / `tool:cargo` resolution in `--build-once` mode), failing fast if zero workers match.
- `spread_shards_swrr` runs Smooth Weighted Round-Robin weighted by `advertised_slots(w).unwrap_or(w.cpu_cores.max(1)).max(1)` and pins `placement.worker` to each selected worker's `id:` (or name fallback).
- `inject_shard_env` writes `OX_SHARD_INDEX` (`1..=N`) and `OX_SHARD_TOTAL` (`N`) into `TaskSpec::Command` / `TaskSpec::ShellScript`, overriding any conflicting user `--env OX_SHARD_*` entries.

### D3: Rust `--build-once` dataflow (`crates/cli/src/shard.rs`)

1. `validate_build_source` verifies `Cargo.toml` exists in `--build-source` (default `.`).
2. `scan_build_source` walks `--build-source` with `ignore::WalkBuilder`, honors `.gitignore`, skips `.git` and top-level `target/`, builds a clean `TreeManifest` v1 (`TreeManifest::new`), and `upload_build_source` uploads missing blobs once.
3. `normalize_build_once_workers` aligns each `(os, arch)` group to a consistent `tool:cargo-nextest@<ver>` tag (by highest aggregate `slots`), and `plan_build_once_groups` reserves non-colliding archive/config paths per `crates/worker/src/staging.rs`.
4. `execute_build_once` runs one `cargo nextest archive --archive-file <archive_rel>` builder per distinct `(os, arch)` concurrently; any builder error or missing archive output cancels remaining builders via `CancelTask` and aborts before any shard is submitted.
5. `build_shard_task` constructs shard `Task::inputs` with `[TaskInput::tree(".", manifest_id, manifest_len), TaskInput::file(archive_rel, archive.id, archive.size, archive.executable)]` (never `Task::workspace`) and runs `cargo nextest run --archive-file <archive_rel> --workspace-remap . --partition slice:<idx>/<total>` (plus staged `.oxideswarm-nextest.toml` via `--tool-config-file` when `--junit-out` is set).

### D4: Concurrent execution, `--fail-fast`, and JUnit XML merge (`crates/cli/src/shard.rs::junit`)

- `execute_shards` dispatches shards concurrently via `tokio::task::JoinSet` bounded by `Semaphore(32)` handshakes, reacting to `fail_fast` and `watch::Receiver<bool>` cancellation by sending best-effort `CancelTask`.
- `ShardReport` embeds `RunSummary::from_results` parity (`machines`, `waited_ms`, `task_time_sum_ms`, `tasks_line()`, `waited_line()`).
- `--output-dir` downloads each shard's outputs into `<output_dir>/<shard_slug>/` via `ensure_non_symlink_cell_dir` (refusing symlinks). When `--junit-out` is set without `--output-dir`, JUnit XML blobs stream into memory via `download_blob` so the current working directory stays clean.
- `shard::junit` parses `<testsuites>` / `<testsuite>`, handles XML entities and `<![CDATA[...]]>`, aggregates `tests`/`failures`/`errors`/`skipped`/`time`, synthesizes error `<testsuite>` entries for non-completed or malformed-XML shards, rejects all-completed runs with zero JUnit XML files, and writes atomically via `write_junit_atomic` (refusing symlinks).

## Compatibility

- Zero changes to `crates/core/*`, `crates/master/*`, `crates/worker/*`, or `Cargo.toml` / `Cargo.lock`.
- `PROTOCOL_VERSION_CURRENT == 10`, `PROTOCOL_VERSION_MIN_COMPATIBLE == 9`, `STORE_FORMAT_VERSION == 4`, and `EXTENSION_REGISTRY == &[]` remain invariant.

## Security

- `--count` is capped at `HARD_MAX_SHARDS = 256` and concurrent handshakes at `MAX_CONCURRENT_HANDSHAKES = 32`.
- `ensure_non_symlink_cell_dir` and `junit::write_junit_atomic` refuse symbolic links along output paths.
- `shard::junit` enforces a `64 MiB` XML input cap, `256`-level nesting depth cap, and entity/control-character sanitization.
- MCP `shard` confines `files`, `workspace`, `output_dir`, `build_source`, and `junit_out` within the server working directory without `..` or symlinks.

## Test plan

- Unit tests in `crates/cli/src/shard.rs` (`#[cfg(test)] mod tests`) and `crates/cli/src/mcp.rs` covering SWRR slot spreading, `ListWorkers` filtering, `(os, arch)` grouping, path collision avoidance, `.git`/`target/` exclusion, JUnit XML parsing/merging/error synthesis, symlink refusal, and MCP schema validation (`-32602`).
- Integration tests in `crates/cli/tests/test_ax2_shard.rs` covering `OX_SHARD_INDEX`/`OX_SHARD_TOTAL`, unequal `slots:` spreading, 0-worker fast-fail, partial failure & `--fail-fast` cancellation, `--output-dir` isolation & symlink refusal, `--junit-out` merge, full `--build-once` via `SubprocessCluster`, MCP `shard` stdio execution, and offline CLI refusals.
- Pure-helper tests for the R2/M1 benchmark in `benchmarks/grid/test_bench_shard.py` and dated loopback benchmark record in `benchmarks/grid/results/`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| AX-2 | Sharded runs, Rust `--build-once`, JUnit XML merge, MCP `shard`, R2/M1 benchmark, and guide | `crates/cli/src/shard.rs`, `crates/cli/src/cli_app/shard.rs`, `crates/cli/src/cli_app.rs`, `crates/cli/src/mcp/shard.rs`, `crates/cli/src/mcp.rs`, `crates/cli/src/lib.rs`, `crates/cli/tests/test_ax2_shard.rs`, `benchmarks/grid/bench_shard_r2_m1.py`, `benchmarks/grid/test_bench_shard.py`, `docs/guides/sharded-runs.md`, `docs/design/0103-ax2-sharded-runs.md` | Unit, integration, hygiene, and R2/M1 loopback benchmark checks pass | Implemented |

## Status log

- 2026-11-22: Implemented `oxideswarm shard`, MCP `shard`, `shard::junit`, `--build-once`, `test_ax2_shard.rs`, R2/M1 loopback benchmark, and `docs/guides/sharded-runs.md`.
- 2026-11-22: Recorded single-host loopback Spike R2 (`51.26 %` savings across 3 workers, clearing the `30 %` stop rule) and Measure M1 (`0.5061x` median, `0.466x` min across 725 unit tests on Apple M5 loopback) in `benchmarks/grid/results/2026-11-22-r2-m1-macos-arm64-apple-m5.json` (`benchmarks/grid/bench_shard_r2_m1.py`).
