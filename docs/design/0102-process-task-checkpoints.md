# 0102: Process task checkpoints (`FEAT-5`)

Status: Implemented
Created: 2026-11-21    Updated: 2026-11-21

## Summary

Process workloads (`Command`, `ShellScript`, and `Workspace` tasks) receive a private writable `$OXIDESWARM_CHECKPOINT_DIR` inside their sandbox, excluded from `--output` globs and `expected_outputs`. The worker restores prior `OXCP1` state before spawn, polls periodically and emits a final changed snapshot on exit/cancel/timeout/graceful drain using monotonic sequence numbers (`S + 1`) inside `CheckpointData.delta_state` on Protocol 10 and Store v4.

## Context

- Previously, `TaskRunner::execute_task_with_checkpoint` (`crates/worker/src/runner.rs`) only forwarded `checkpoint` and `checkpoint_tx` to `BuiltinTest`, `Generative3D`, and `AutoRig`. `Command`, `ShellScript`, `RustCompilation`, and `Workspace` workloads never received a checkpoint directory, periodic emitter, or pre-spawn restore.
- `BuiltinTest` emits 16-byte binary or JSON checkpoints, and `Generative3D` / `AutoRig` emit JSON progress objects (`{"progress": ...}`). Reusing `WorkerMessage::Checkpoint` and `MasterMessage::AssignTaskWithCheckpoint` for arbitrary process state requires an explicit in-band payload discriminator so process-task restore never misinterprets JSON/binary progress frames.
- Under `--isolation fs|fs+net` (`crates/worker/src/isolation.rs`, `crates/worker/src/isolation/macos.rs`), `cmd.env_clear()` strips `OXIDESWARM_*` from the worker environment and OS confinement restricts writes exclusively to `SANDBOX_DIR` and `EXTRA_WRITABLE_DIR`.
- `OutputPattern` (`crates/core/src/blob.rs`) treats `*` as matching dot-prefixed entries, so `staging::walk` (`crates/worker/src/staging.rs`) would collect a checkpoint directory inside `sandbox.path()` under `--output '**/*'` unless explicitly excluded.
- When `shutdown_rx` triggered graceful worker shutdown (`crates/worker/src/client/session.rs`), `lane_writer` exited immediately on `closing_tx` and `expire_resume_window("FAST_SHUTDOWN")` aborted active tasks before they could flush a final checkpoint.

## Goals

- G1. Provide every `Command`, `ShellScript`, and `Workspace` task with a private, writable `OXIDESWARM_CHECKPOINT_DIR` inside its confined sandbox (including macOS Seatbelt `--isolation fs|fs+net`).
- G2. Separate process-task directory snapshots from `BuiltinTest` / `Generative3D` / `AutoRig` payloads using a deterministic, integrity-checked `OXCP1` binary format on Protocol 10 (`PROTOCOL_VERSION_CURRENT = 10`) and Store v4 (`STORE_FORMAT_VERSION = 4`).
- G3. Exclude `.oxideswarm_checkpoint` from `--output` globs (including `--output '**/*'`), sandbox root diagnostics, and `expected_outputs` artifacts.
- G4. Enforce strict symlink, reparse-point, special-file, path-traversal, and size/file-count safety on both pack and restore, failing gracefully on oversized or corrupt payloads without crashing the task or worker.
- G5. Restore prior `OXCP1` state before spawning, initialize outbound sequence numbers to `S + 1` for Master monotonicity (`TaskQueue::is_newer_checkpoint`), emit deduplicated checkpoints periodically and on non-zero exit/cancellation/timeout, and drain active tasks on graceful worker shutdown before closing the transport.

## Non-goals

- Changing wire structs or bumping `PROTOCOL_VERSION_CURRENT`, `STORE_FORMAT_VERSION`, or `EXTENSION_REGISTRY`.
- Adding `OXIDESWARM_CHECKPOINT_DIR` to `RustCompilation` (`cargo build` / `rustc` builds) or modifying `Generative3D` / `AutoRig` / `BuiltinTest` checkpoint formats.
- External compression codecs (`zstd` / `flate2`) or multi-frame streaming checkpoints exceeding `MAX_PROCESS_CHECKPOINT_BYTES`.

## Design

### D1: In-band `OXCP1` binary format (`crates/worker/src/runner/checkpoint.rs`)

- **Magic & header (44 bytes)**: `[magic: b"OXCP1\x01" (6 B)][flags: u16 LE = 0][entry_count: u32 LE][blake3_digest: 32 B]`.
- **Canonical entry stream**: Sorted lexicographically by canonical forward-slash `rel_path`. Each entry encodes `[kind: u8 (0 = regular file, 1 = leaf empty dir)][path_len: u32 LE][rel_path UTF-8][executable: u8][data_len: u64 LE][data_bytes]`. `blake3_digest` hashes the canonical entry stream via `rusty_grid_core::blob::BlobId::of`, enabling both decode integrity checks and `O(1)` change-detection (`last_emitted_hash`).
- **Bounds**: `MAX_PROCESS_CHECKPOINT_BYTES = 12 * 1024 * 1024` (12 MiB), `MAX_PROCESS_CHECKPOINT_FILES = 4096`, `MAX_PROCESS_CHECKPOINT_PATH_BYTES = 4096`. Because `AssignTaskWithCheckpoint` bundles `Task` (`staged_files` up to 32 MiB) and `latest_checkpoint` in a single `64 MiB` `MAX_FRAME_SIZE` frame and `snapshot.bin` JSON `bytes_serde` can expand non-UTF-8 bytes up to `4x`, `12 MiB` keeps the combined frame within `64 MiB`, JSON expansion `<= 48 MiB`, and master memory well within `256 MiB` `DEFAULT_MAX_ACTIVE_BYTES`.
- **Payload separation**: Restore silently returns `Ok(None)` for `None` or any `delta_state` not starting with `b"OXCP1"`. Corrupt, truncated, or hash-mismatched `OXCP1` payloads log a warning, clean any partial files in `checkpoint_dir`, and return `None` without failing the task or crashing the worker.

### D2: Sandbox placement, Seatbelt confinement & env injection

- **Non-workspace process tasks (`Command`, `ShellScript`)**: `<sandbox_dir>/.oxideswarm_checkpoint` (mode `0o700`, verified real owned directory). Lies inside `SANDBOX_DIR`, so macOS Seatbelt (`crates/worker/src/isolation/macos.rs`), Linux Landlock, and Windows AppContainer grant RW access automatically.
- **Workspace tasks (`Workspace`)**: `<ws>/tmp/.oxideswarm_checkpoint` (mode `0o700`, created after `guard.prepare_tmp()` and `guard.verify_run_dirs()`). Lies inside `EXTRA_WRITABLE_DIR`, granting RW under isolation while keeping persistent `<ws>/tree` clean of checkpoint artifacts.
- **Environment injection (`crates/worker/src/runner/supervise.rs`)**: `OXIDESWARM_CHECKPOINT_DIR` is injected after `cmd.env_clear()`, `inherited_environment`, user `env`, `isolation::prepare_environment`, and `workspace_ctx` variables so isolation cannot strip it and task `--env` cannot spoof it.

### D3: `--output` & artifact exclusion (`crates/worker/src/staging.rs`, `crates/worker/src/runner/capture.rs`)

- `staging::walk` skips `.oxideswarm_checkpoint` (never descending or collecting files, even under `--output '**/*'`).
- `is_reserved_checkpoint_rel` normalizes leading `./` / `.\` segments and compares the first path segment ASCII case-insensitively (`eq_ignore_ascii_case`) plus Unicode case/NFD-folded via `fold_blob_path` for non-ASCII segments (blocking e.g. `U+212A` `K` and `U+017F` `ſ` on APFS/NTFS), preventing bypasses on case-insensitive APFS/NTFS volumes. `staging::collect_outputs_inner` refuses literal patterns targeting `.oxideswarm_checkpoint` and skips wildcard patterns prefixed with it; `staging::describe_sandbox_root` uses `is_reserved_checkpoint_rel` to omit `.oxideswarm_checkpoint` (and its case/Unicode-folded variants) from missing-output diagnostics.
- `capture::harvest_output_artifacts_from_dir` rejects `expected_outputs` entries targeting `.oxideswarm_checkpoint` (including mixed-case and `./` forms).

### D4: Pre-spawn restore, periodic polling, final emission & graceful drain

- **Restore & sequence monotonicity**: Pre-spawn restore initializes `last_emitted_hash` to the restored `OXCP1` digest (or `empty_checkpoint_digest()` when absent/ignored/corrupt) and initializes `next_sequence = checkpoint.map_or(1, |c| c.sequence.saturating_add(1))`, satisfying `TaskQueue::is_newer_checkpoint` (`sequence > existing.sequence` in `crates/master/src/queue/lifecycle.rs`).
- **Periodic polling (`supervise.rs`)**: `execute_process_supervised_with_checkpoint` polls `checkpoint_dir` every `RunnerConfig::checkpoint_poll_interval` (default `1s`) via non-overlapping `tokio::task::spawn_blocking` packs, emitting `WorkerMessage::Checkpoint` only when the canonical BLAKE3 digest changes. Oversized directories log a warning and skip emission without failing the task.
- **Final emission & graceful drain (`supervise.rs`, `client/tasks.rs`, `client/session.rs`)**: Non-zero exits, `Ended::Cancelled` (after `stop_tree`), and `Ended::TimedOut` (after `stop_tree`) emit one final changed snapshot if `checkpoint_tx` is open, and `forward_checkpoint` performs a bounded `200 ms` send retry on final emission if `session.data_tx` is momentarily full outside a resume window. On `shutdown_rx` in `run_session`, if active tasks exist and no urgent evacuation override is active, `drain_active_tasks_for_shutdown` grants a bounded `2s` cancel/checkpoint drain and flushes `session.data_tx` before `session.close(Some(Disconnecting))`; idle workers exit with `0 ms` drain overhead.

## Compatibility

- Protocol 10 (`PROTOCOL_VERSION_CURRENT = 10`, `PROTOCOL_VERSION_MIN_COMPATIBLE = 9`) and Store v4 (`STORE_FORMAT_VERSION = 4`) remain unchanged; `EXTENSION_REGISTRY` and `crates/core/tests/data/wire_layout.txt` are untouched.
- Existing `BuiltinTest`, `Generative3D`, and `AutoRig` checkpoint payloads do not start with `b"OXCP1"` and continue working unchanged.

## Security

- **Pack**: On Unix, walks exclusively via directory-descriptor-pinned `PinnedDir` (`open` / `openat` with `O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC`, `fdopendir` + `readdir` on a `F_DUPFD_CLOEXEC` descriptor, `fstatat(..., AT_SYMLINK_NOFOLLOW)`, `openat(..., O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC)`, and `fstat` ownership/type verification) in `crates/worker/src/sandbox.rs`, eliminating live parent-directory symlink TOCTOU races while keeping `checkpoint.rs` safe under `#![deny(unsafe_code)]`. On non-Unix targets, checks `symlink_metadata`, `is_real_owned_dir`, canonical containment, and `open_regular_file_nofollow`. Rejects symlinks, reparse points, FIFOs/sockets/devices, non-UTF-8 paths, invalid blob paths, and case/Unicode-NFD collisions (`fold_blob_path`). Bounds file reads with `.take(remaining + 1)`.
- **Unpack**: Verifies `OXCP1\x01` magic, `flags == 0`, BLAKE3 content digest, exact cursor consumption, `validate_blob_path`, `normalize_relative_path_str`, `sanitize_relative_path`, `fold_blob_path` uniqueness, ancestor file/dir disjointness, and canonical parent confinement inside `checkpoint_dir` before creating files (`create_new`, `O_NOFOLLOW` on Unix).

## Test plan

- **Unit tests (`crates/worker/src/runner/checkpoint.rs`, `staging.rs`, `capture.rs`, `runner/tests.rs`)**: `OXCP1` pack/unpack roundtrip (nested dirs, empty dirs, executable bit, deterministic digest), non-`OXCP1` ignore (JSON, raw 16B), corrupt/truncated/bad-hash rejection, symlink & traversal rejection on pack and unpack, size & file-count limits, `S + 1` sequence monotonicity, `--output '**/*'` and `expected_outputs` exclusion, `OXIDESWARM_CHECKPOINT_DIR` env injection under `IsolationLevel::Fs` and `Off`.
- **Integration / E2E tests (`crates/worker/tests/test_feat5_process_checkpoints.rs`)**: `Command`, `ShellScript`, `Workspace` (with `--output '**/*'`), macOS `--isolation fs` Seatbelt kill-and-resume, and worker `SIGTERM` graceful drain final checkpoint delivery.
- **Verification gates**: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo check -p rusty_grid_master --no-default-features --locked`, `cargo test -p rusty_grid_core --lib wire_layout_tests --locked`, `cargo test --workspace --lib --locked`, `cargo test -p rusty_grid_worker --test test_feat3_os_isolation --locked`, `cargo test -p rusty_grid_worker --test test_feat5_process_checkpoints --locked`, `ox-mode test size`, `ox-mode doc verify`, and `python3 -m unittest tests/test_m1_challenger_hygiene.py`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| FEAT-5 | Process task checkpoints (`OXCP1`, `OXIDESWARM_CHECKPOINT_DIR`, restore, periodic + drain/cancel emission) | `crates/worker/src/runner/checkpoint.rs`, `crates/worker/src/runner.rs`, `crates/worker/src/runner/workloads.rs`, `crates/worker/src/runner/supervise.rs`, `crates/worker/src/runner/capture.rs`, `crates/worker/src/staging.rs`, `crates/worker/src/client/tasks.rs`, `crates/worker/src/client/session.rs`, `crates/worker/tests/test_feat5_process_checkpoints.rs`, `docs/reference.md`, `docs/README.md` | Unit and E2E tests pass for `Command`, `ShellScript`, `Workspace`, `--isolation fs`, `--output '**/*'`, and `SIGTERM` drain | Implemented |

## Status log

- 2026-11-21: Created design record `0102` and implemented process task checkpoints (`FEAT-5`) across `crates/worker`.
