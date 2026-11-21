# 0101: Matrix fan-out in `submit` and MCP (`AX-1`)

Status: Implemented
Created: 2026-11-21    Updated: 2026-11-21

## Summary

`oxideswarm submit --matrix <KEY=V1,V2,...>` and the MCP `submit` tool `matrix` parameter expand a Cartesian product of placement and environment axes client-side into individual `ClientMessage::SubmitTask` calls on Protocol 10 without changing the wire format or master store. A pre-flight `ClientMessage::ListWorkers` check identifies unplaced cells immediately without hanging, shared `--input` blobs and `--workspace` trees are uploaded once per submission, and placed cells execute concurrently and return structured per-cell records and summary counts.

## Context

- `crates/cli/src/cli_app/submit.rs` previously submitted a single `Task` per `submit` invocation (plus optional `--verify` replicas via `crates/cli/src/verify.rs`).
- `crates/core/src/placement.rs` already models task constraints as `require:<key>=<value>` tags (`os`, `arch`, `worker`, `iso`, `tool`, `tag`) parsed by `Placement::from_task_tags` and matched against `WorkerCapabilities` via `Placement::matches_listed`.
- Master `fail_unfulfillable_tasks` defaults to `false` ([design 0034](0034-fail-unfulfillable-tasks.md)), so submitting a task whose constraints match no connected worker with `wait: true` blocks until `timeout_secs`.
- [Design 0056](0056-frontier-program.md) (`D2`, `AX-1`) specifies client-side matrix fan-out across `submit` and MCP `submit` with zero wire changes.

## Goals

- G1. Expand 1D and multi-axis Cartesian products of `os`, `arch`, `worker`, `iso`, `tool`, `tag`, and `env:<VAR>` (for process tasks) in both `oxideswarm submit --matrix` and MCP `submit`.
- G2. Detect unplaced cells upfront using `ClientMessage::ListWorkers` so unfulfillable cells report `unplaced` immediately without submitting to the master or waiting for `timeout_secs`.
- G3. Scan and upload shared `--input` blobs and `--workspace` trees at most once per matrix submission, skipping uploads completely when zero cells are placeable.
- G4. Execute placeable cells concurrently with per-cell `--output-dir` isolation, bounded inline head/tail output, structured JSON and human cell records, and non-zero / `isError: true` propagation when any cell fails, times out, or is unplaced.
- G5. Keep Protocol 10 (`PROTOCOL_VERSION_CURRENT = 10`) and Store v4 (`STORE_FORMAT_VERSION = 4`) unchanged.

## Non-goals

- Changing wire structs or adding a server-side batch/matrix RPC (`AX-2` sharding, `AX-3` live streaming, and `CACHE-1` action cache are separate items).
- Combining `--matrix` with `--verify` or `-o` / `--output-asset` in a single invocation.
- Multi-task cross-process deduplication beyond single-submission blob and workspace upload reuse.

## Design

### D1: Matrix specification and Cartesian expansion (`crates/cli/src/matrix.rs`)

- **CLI flags (`crates/cli/src/cli_app.rs`)**: repeatable `--matrix <KEY=V1,V2>` (`Vec<String>`) and `--max-matrix-cells <N>` (requires `--matrix`, default `64`, hard cap `256`). `--matrix` implies `--wait`.
- **MCP schema (`crates/cli/src/mcp.rs`)**: `submit.inputSchema.properties` exposes `matrix` as `type: "array"`, `items: { "type": "string" }` (avoiding top-level `anyOf`/`oneOf`/`allOf`) and `max_matrix_cells` as `type: "integer"`. At runtime, `validate_tool_arguments` and `MatrixSpec::parse_mcp_value` accept both `key=v1,v2` string arrays and non-empty axis objects `{"key": ["v1", "v2"]}`.
- **Supported axes and conflicts**:
  - Placement axes: `os`, `arch`, `worker`, `iso`, `tool`, `tag` (mapped to `require:<key>=<value>` and merged with base placement constraints via `Placement::from_task_tags`).
  - Process environment axis: `env:<VAR>` (supported on `Command` and `ShellScript` tasks, rejected on `gpu` and `compile` tasks).
  - Singular axes (`os`, `arch`, `worker`, `iso`, and duplicate `env:<VAR>`) reject duplicates and reject conflicts with base singular placement options (`--os`, `--arch`, `--worker`, `--require-isolation` or MCP equivalents).
  - Multi-valued axes (`tag` and `tool`) may repeat across `--matrix` entries (for example `["tag=row-1,row-2", "tag=col-1,col-2"]`) to form a 2D Cartesian product of tags or tools.
  - Empty specs, missing `=`, empty keys or values (`a,,b`, `a,`), duplicate values within the same axis, unknown keys, invalid tag/tool/isolation values, and cell counts exceeding `max_matrix_cells` (checked with `checked_mul`) are rejected before connecting.

### D2: Pre-flight `ListWorkers` unplaced-cell detection

- Before uploading blobs or dispatching tasks, the client issues a single bounded `ClientMessage::ListWorkers` RPC. If listing workers fails, the submission fails cleanly without hanging.
- Each expanded cell is checked against connected workers (`state:` not `disconnected`) for `Placement::matches_listed`, CPU cores, RAM, GPU/VRAM requirements, and `feat:workspaces` when a workspace is attached.
- Unplaced cells are never sent to the master. They receive `status: "unplaced"`, `unplaced: true`, and an explanatory `error` naming the unsatisfied placement or resource constraints. Placeable cells in the same matrix still execute.

### D3: Deduplicated staging and concurrent cell execution

- When zero cells are placeable, input blob and workspace uploads are skipped completely.
- When at least one cell is placeable, `scan_inputs` / `upload_inputs` (or workspace scan and upload) runs once before cell dispatch, and every cell `Task` reuses the resulting `BlobFile` inputs or `WorkspaceRef`.
- Each placeable cell receives a fresh `TaskId::new()`, merged `require:` tags, and process environment injections `OXIDESWARM_MATRIX_CELL`, `OXIDESWARM_MATRIX_INDEX`, and any `env:<VAR>` overrides.
- Placeable cells run concurrently in a `tokio::task::JoinSet` over dedicated connections (`connect_master` / `McpState::connect_dedicated_until`, handshake concurrency bounded to 32 under the master's 64-pending-handshake limit), respecting the task timeout and MCP `cancel_rx` (`CancelTask` best-effort on cancellation).

### D4: Output isolation, bounded streams, and reporting

- Declared `--output` / `outputs` files for each completed cell download into `<output_dir>/<cell_slug>/` (defaulting `<output_dir>` to `.` on CLI when `--output` is set without `--output-dir`) so cells never overwrite each other.
- Inline `stdout` and `stderr` are bounded per cell with `truncate_stream_for_llm` and `split_stream_budgets` (escaping JSON bytes accurately so MCP replies remain compact single-line JSON-RPC 2.0 responses).
- `MatrixReport::to_json` emits `matrix: true`, `status` (`"completed"` or `"failed"`), cell count fields (`total_cells`, `completed_cells`, `failed_cells`, `unplaced_cells`, `timed_out_cells`, `error_cells`, `cancelled_cells`), `RunSummary` fields (`machines`, `waited_ms`, `task_time_sum_ms`), ordered `cells` records (`index`, `cell`, `label`, `slug`, `axes`, `require_tags`, `status`, `unplaced`, `task_id`, `worker`, `worker_id`, `worker_name`, `exit_code`, `duration_ms`, `stdout`, `stderr`, truncation metadata, `error`, `outputs`, `saved_outputs`, `saved_artifacts`), and optional `input_upload` / `workspace` summaries.
- CLI exits non-zero and MCP sets `isError: true` whenever any cell fails, times out, errors, is cancelled, or is unplaced. Invalid MCP `matrix` arguments fail validation in `validate_tool_arguments` with JSON-RPC `-32602` while keeping the stdio server loop alive.

## Compatibility

- Zero wire protocol changes (`PROTOCOL_VERSION_CURRENT = 10`, `PROTOCOL_VERSION_MIN_COMPATIBLE = 9`).
- Zero durable store changes (`STORE_FORMAT_VERSION = 4`) and zero changes to `EXTENSION_REGISTRY` or `crates/core/tests/data/wire_layout.txt`.
- Existing `submit` invocations without `--matrix` or MCP `matrix` are unchanged.

## Security

- Cell expansion is capped at `64` cells by default and `256` maximum (`HARD_MAX_MATRIX_CELLS`), bounding client-side fan-out and master queue load.
- Placement axes enforce `Placement::from_task_tags` rules, rejecting reserved worker/master tag prefixes (`os:`, `opt:`, `speed:`, and similar).
- MCP file, workspace, and `output_dir` path confinement and symlink rejections remain enforced before any upload or download.

## Test plan

- Unit tests in `crates/cli/src/matrix.rs` and `crates/cli/src/mcp.rs` covering axis parsing, Cartesian expansion, cap enforcement, conflict rejection, `env:<VAR>` validation, placeability filtering, and MCP schema/validation (`-32602`).
- Integration tests in `crates/cli/tests/test_ax1_matrix.rs` covering 1D fan-out across 3 tagged workers, 2D Cartesian fan-out, prompt unplaced-cell detection without hanging, single-pass `--input` and `--workspace` upload deduplication, per-cell `--output-dir` isolation, partial cell failure, CLI validation refusals, and MCP stdio `matrix` workflows (including `ping` liveness, large stdout framing, and stdin EOF grace).
- Verification gates: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test -p rusty_grid_cli --lib --locked`, `cargo test -p rusty_grid_cli --test test_ax1_matrix --locked`, `ox-mode test size`, `ox-mode doc verify`, and `tests/test_m1_challenger_hygiene.py`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| AX-1 | Matrix fan-out in `submit` CLI and MCP | `crates/cli/src/matrix.rs`, `crates/cli/src/lib.rs`, `crates/cli/src/cli_app.rs`, `crates/cli/src/cli_app/submit.rs`, `crates/cli/src/mcp.rs`, `crates/cli/src/mcp/submit.rs`, `crates/cli/src/mcp/submit_matrix.rs`, `crates/cli/tests/test_ax1_matrix.rs`, `README.md`, `docs/reference.md`, `docs/guides/mcp-client-setup.md` | Unit and E2E tests pass for 3 tagged workers, 2D matrix, unplaced cell without hanging, upload deduplication, and MCP stdio matrix | Implemented |

## Status log

- 2026-11-21: Created design record `0101` and implemented client-side matrix fan-out across CLI `submit` and MCP `submit` (`AX-1`).
