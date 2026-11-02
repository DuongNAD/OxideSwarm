# 0013: Task Output Validation, Mandatory Harvesting Failure Semantics, and Staging Flag Enforcement

Status: Implemented
Created: 2026-11-01    Updated: 2026-11-01

## Summary

Enforces strict task output validation and failure semantics across worker execution engines:
- Missing declared output artifacts fail the task with non-zero exit code (`EXIT_CODE_GENERAL_ERROR = 1`), populating `result.error` and `result.stderr` with the missing path.
- Inline harvest size limits (10 MiB per artifact, 32 MiB aggregate per task) strictly fail the task on overflow, naming the offending file.
- Prohibits symlinked output artifacts and rejects path traversal (`..`, absolute paths, Windows reserved names) before dereferencing.
- Enforces CLI staging flag mutual exclusivity across task types (`command`, `shell`, `gpu`, `3d`, `autorig`, `compile`), pointing 3D/rigging asset outputs to `--output-asset` / `-o`.
- Clarifies the architectural distinction between inline harvest (MCP & library callers) and content-addressed blob transfer (CLI).

## Context

Behaviour before this change:

| Area | Behaviour | Code |
|---|---|---|
| Missing Outputs | When a declared output was missing from the sandbox, `harvest_output_artifacts` silently skipped it or returned an empty list. Tasks exited with code 0 (`SUCCESS`), leaving clients unaware of missing artifacts. | `crates/worker/src/runner.rs:1255` |
| Oversize Outputs | Single artifacts > 10 MiB or aggregate outputs > 32 MiB were logged as warnings and skipped, returning truncated artifact sets with exit code 0. | `crates/worker/src/runner.rs:1282,1289` |
| Symlinks & Traversal | Symlinks were skipped without explicit task failure. Error results did not distinguish between missing files and hostile traversal attempts. | `crates/worker/src/runner.rs:1266` |
| CLI Flag Validation | Incompatible staging flags (e.g. `--file` or `--output` on `--type gpu` or `--type auto-rig`) were silently ignored or led to confusing error messages like `--output applies to command and shell tasks, not to --type generic`. Single `--output` on 3D tasks implicitly redirected to asset outputs. | `crates/cli/src/cli_app.rs:1976` |
| Inline vs Blob Paths | Documentation conflated inline artifact collection (design 0003) with content-addressed blob streaming (design 0006). | `README.md`, `CLAUDE.md` |

## Goals

- **G1** A task whose execution finishes with exit code 0 must fail with exit code 1 (`EXIT_CODE_GENERAL_ERROR`) if any declared expected output artifact is missing, oversized, a symlink, or traverses outside the sandbox root.
- **G2** Preserve original non-zero process failure exit codes when a declared output is not produced by a failed process, recording diagnostic notes in `stderr` and `error`.
- **G3** Enforce strict single-file (10 MiB) and aggregate (32 MiB) limits on inline output artifacts, explicitly naming the file that crossed the limit.
- **G4** Provide unambiguous, actionable CLI error messages when staging flags are passed to incompatible task kinds, explicitly suggesting `--output-asset` / `-o` for 3D generation and auto-rigging.
- **G5** Formally document the separation between inline artifact harvesting (`Task::expected_outputs`) and blob transfer (`Task::output_globs`).

## Non-goals

- Applying the 10 MiB inline limit to the content-addressed blob data plane (`--output PATTERN`), which is governed separately by `--output-limit-mb` (default 1 GiB per task, `staging.rs:29`, `:249`) under design 0006.
- Supporting wildcard glob patterns for inline harvest (`expected_outputs` are literal sandbox relative paths; glob patterns are supported exclusively by the blob plane).
- Allowing symlinks inside inline task artifacts.

## Design

### D1. Output Harvesting Result & Task Result Mutation (`harvest_and_apply_artifacts`)

Output harvesting in `TaskRunner` returns `Result<Vec<OutputArtifact>, String>`.
A shared helper `harvest_and_apply_artifacts` deduplicates the post-execution artifact processing across all three execution workloads (`Command`, `ShellScript`, `RustCompilation`):

1. If `task.expected_outputs` is empty, the original `TaskResult` is returned untouched.
2. If `harvest_output_artifacts` succeeds, `result.artifacts` is populated with the harvested items.
3. If `harvest_output_artifacts` returns `Err(e)`:
   - Diagnostic message `"Output files: {e}"` is logged at `WARN` level.
   - If `result.exit_code == EXIT_CODE_SUCCESS`, `result.exit_code` is set to `EXIT_CODE_GENERAL_ERROR` (1).
   - If the task process already failed with a non-zero exit code (e.g. 42), that exit code is preserved.
   - `result.stderr` appends `"[rusty_grid: {message}]\n"`.
   - `result.error` is populated or appended with `{message}`.

### D2. Rejection of Hostile Traversal and Security Boundaries

In `harvest_output_artifacts`:
1. `sanitize_relative_path(sandbox.path(), Path::new(expected))` validates relative components before filesystem access. Any parent directory traversal (`..`), absolute path, leading slash, Windows drive letter or DOS device name immediately returns `Err("output artifact '{expected}' resolves outside sandbox root")`.
2. `tokio::fs::symlink_metadata` queries path metadata without dereferencing symlinks. If `meta.is_symlink()`, it immediately returns `Err("output artifact '{expected}' is a symbolic link (symbolic links are prohibited for security isolation)")`.
3. `tokio::fs::canonicalize` verifies that the resolved path is rooted within the canonical sandbox directory.
4. When a file is missing, if `process_failed` is false, it returns `Err("the task did not produce '{expected}'")`.

### D3. Inline Harvest vs. Content-Addressed Blob Data Plane

OxideSwarm maintains two distinct output retrieval mechanisms:

| Dimension | Inline Harvesting (Design 0003 & 0013) | Blob Data Plane (Design 0006) |
|---|---|---|
| Trigger / Field | `Task::expected_outputs` / `TaskResult::artifacts` | `Task::output_globs` / `BlobClient` |
| Primary Callers | MCP Server (`oxideswarm mcp`), direct library callers | CLI (`oxideswarm submit --output PATTERN --output-dir DIR`) |
| Transport Framing | Serialized directly within wire protocol frames (`MAX_FRAME_SIZE = 64 MiB`) | Streamed in 4 MiB content-addressed chunks over QUIC data stream or TCP |
| Selection Semantics | Exact literal relative path | Glob patterns (`*`, `?`, `**`) |
| Per-File Limit | 10 MiB (`MAX_OUTPUT_ARTIFACT_BYTES`) | No separate per-file limit (governed by 1 GiB per-task limit) |
| Aggregate Limit | 32 MiB (`MAX_AGGREGATE_ARTIFACT_BYTES`) | 1 GiB per task (`--output-limit-mb`, `staging.rs:29`, `:249`) |
| Missing File Behavior | Task fails with exit code 1 naming missing path | Staging warns / skips non-matching patterns; exits 1 if declared exact blob missing |

### D4. CLI Staging Flag Validation and Suggestion

In `crates/cli/src/cli_app.rs`:
- `--input` (and `--file`), `--output`, and `--output-dir` are accepted only on `command` and `shell` tasks.
- If `--output` is supplied on a 3D generation (`--type 3d`, or when `--prompt` / `--image` is present) or auto-rigging task, `build_task_spec` returns:
  `"--output applies to command and shell tasks, not to 3d or rigging tasks (use --output-asset or -o for 3D/rigging assets)"`.
- Passing `--output-dir` without `--output` is rejected: `"--output-dir requires at least one --output pattern"`.

## Compatibility

- **Wire Compatibility**: No changes to wire format or message enums. `Task` and `TaskResult` fields remain identical.
- **Client Impact**: Clients relying on implicit exit 0 when declared outputs were missing will now correctly receive exit 1 with an error message naming the missing file.

## Security

- **Path Traversal Defense**: All declared paths are sanitized and checked against canonical sandbox roots before any file read.
- **Symlink Exfiltration Prevention**: Symlinks are rejected via `symlink_metadata` without dereferencing, preventing exfiltration of host files (e.g. `/etc/passwd` or cluster secrets).
- **Resource Exhaustion**: Hard limits of 10 MiB per artifact and 32 MiB per task prevent memory exhaustion in the coordinator and client from oversized inline payloads.

## Test Plan

- `crates/worker/tests/test_bug2_output_failure.rs`:
  - `test_missing_expected_output_fails_task` (command task missing output)
  - `test_missing_expected_output_fails_shell_task` (shell task missing output)
  - `test_missing_expected_output_fails_compile_task` (Rust compilation task missing output)
  - `test_oversize_11mib_output_fails_task` (single file > 10 MiB)
  - `test_aggregate_oversize_output_fails_task` (aggregate files > 32 MiB, asserts named file)
  - `test_symlink_output_fails_task` (symlink rejection)
  - `test_process_failure_preserves_exit_code` (preservation of original non-zero exit code)
- `crates/cli/tests/test_bug2_cli_flags.rs` (23 tests):
  - Validation that unsupported task types and kebab/snake aliases reject staging flags.
  - Verification that `--output` on 3D tasks explicitly recommends `--output-asset` / `-o`.
- `tests/m5b_challenger_2_stress.rs`:
  - `test_symlink_escape_prevention_in_harvest`: iterates over 8 hostile paths, asserting exact sandbox traversal refusal rather than generic missing-file errors.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Implement mandatory failure on missing/oversize/symlink output in runner | `crates/worker/src/runner.rs` | Missing output causes exit code 1 with path in error | Implemented |
| W2 | Deduplicate output harvest handling across command, shell, compile | `crates/worker/src/runner.rs` | Single helper `harvest_and_apply_artifacts` used in all 3 workloads | Implemented |
| W3 | Add shell and compile missing output integration tests | `crates/worker/tests/test_bug2_output_failure.rs` | Mutations in shell and compile copies fail tests | Implemented |
| W4 | Refine error messages (remove wildcard advice, name aggregate file) | `crates/worker/src/runner.rs` | Tests verify named file crossing aggregate limit | Implemented |
| W5 | CLI staging flag mutual exclusivity & 3D asset flag suggestion | `crates/cli/src/cli_app.rs` | CLI rejects staging flags and suggests `-o` on 3D | Implemented |
| W6 | Strengthen traversal assertion in challenger stress test | `tests/m5b_challenger_2_stress.rs` | Bypassing sanitize/canonical checks fails test | Implemented |
| W7 | Add design record 0013 and README entry | `docs/design/0013-*.md`, `README.md` | Record and index entry committed | Implemented |

## Status log

- 2026-11-01: Design record 0013 drafted, reviewed, and implemented for PR 20 review fixes.
