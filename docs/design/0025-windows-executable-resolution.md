# 0025: Windows task executable resolution and PE validation

Status: Implemented
Created: 2026-11-04    Updated: 2026-11-04

## Summary

Validate Windows task executables in `resolve_windows_executable` (`crates/worker/src/runner.rs`) by requiring a Windows executable extension from the fixed list `[".com", ".exe", ".bat", ".cmd"]` (in `PATHEXT`'s default order) and a 2-byte `MZ` header on `.com` and `.exe` files, keeping the Git-for-Windows-first search order for bare names, returning explicit paths unchanged when validation fails, and logging both skipped candidates (`debug!`) and the resolved path on `cmd.spawn()` failures.

## Context

On Windows, `TaskRunner::execute_process_supervised` (`crates/worker/src/runner.rs`) calls `resolve_windows_executable(program, &effective_path)` before passing the program to `tokio::process::Command::new`. Before this change:

| Area | Behaviour before this change | Code |
|---|---|---|
| Extensionless files in Git or `PATH` | `resolve_windows_executable` checked `for ext in &[".exe", ".cmd", ".bat", ""]` in `C:\Program Files\Git\usr\bin` and `C:\Program Files\Git\bin`, and `let candidate = dir.join(bare_name); if candidate.is_file()` in `effective_path`. Any extensionless file (`cmd`, `sh` wrapper, or data file) matching `bare_name` was selected and passed directly to `CreateProcessW`, failing with `os error 193` (`%1 is not a valid Win32 application`). | `crates/worker/src/runner.rs` |
| 0-byte or non-PE `.exe` / `.com` files | `candidate.is_file()` accepted 0-byte or non-PE `.exe` and `.com` files ahead of valid PE binaries on `PATH`, also failing `CreateProcessW` with `os error 193`. | `crates/worker/src/runner.rs` |
| Silent refusal and opaque spawn errors | `cmd.spawn()` failure messages reported only the requested `'program'` name (`Failed to spawn process 'cmd': ...` and `Process spawn error: ...`), never the resolved path selected by `resolve_windows_executable`, and skipped candidates were not logged. | `crates/worker/src/runner.rs` |

In Windows CI runs (`workspace tests (windows-latest)`), 5 tests in `tests/blob_transfer_e2e.rs` (`test_input_directory_round_trip_and_second_submit_sends_nothing`, `test_output_patterns_never_follow_symlinked_parent_directories_or_steal_external_files`, `test_output_blobs_stay_pinned_for_delayed_client_download`, `test_output_blobs_can_be_evicted_after_output_pin_retention_expires`, and `cli::test_cli_submit_input_and_output_round_trip_with_dedup`) and `tests/cli_doctor_e2e.rs::test_doctor_passes_on_a_healthy_two_worker_cluster` (along with production `oxideswarm doctor` check C4 in `crates/cli/src/doctor.rs`) failed with `os error 193` when spawning `cmd /C ...`.

## Goals

- **G1** Accept only files with a recognized Windows executable extension from the fixed list `[".com", ".exe", ".bat", ".cmd"]` (in `PATHEXT`'s default order).
- **G2** Verify that `.com` and `.exe` candidate files start with the 2-byte DOS/PE `MZ` signature (`[0x4D, 0x5A]`) before selecting them.
- **G3** Preserve the search order for bare command names (`C:\Program Files\Git\usr\bin` and `C:\Program Files\Git\bin` first, then `effective_path`, then `std::process::Command` fallback) and never let an explicit path with a path separator fall through to a `PATH` lookup for its base name.
- **G4** Emit a `tracing::debug!` event whenever an existing candidate file is skipped, and include `resolved_program` in both `stderr` and `result.error` when `cmd.spawn()` fails.

## Non-goals

- Changing Unix command resolution (this resolver is `#[cfg(windows)]` only).
- Executing extensionless POSIX shebang scripts directly without an explicit interpreter (`sh` or `bash`).

## Design

### D1 Accepted extensions and `MZ` header check (`WINDOWS_EXEC_EXTS`, `is_valid_windows_executable`)

1. `WINDOWS_EXEC_EXTS` is the fixed list `[".com", ".exe", ".bat", ".cmd"]`, matching Windows `PATHEXT`'s default precedence (`.COM;.EXE;.BAT;.CMD`).
2. `is_valid_windows_executable(path)` returns `false` immediately if `!path.is_file()`.
3. If `path` exists as a file but its extension (case-insensitive ASCII) is not `com`, `exe`, `bat`, or `cmd`, `is_valid_windows_executable` logs `debug!(path = %path.display(), "Skipping Windows executable candidate without .com/.exe/.bat/.cmd extension")` and returns `false`.
4. If the extension is `com` or `exe`, `is_valid_windows_executable` opens `path` and reads its first 2 bytes; if the read fails or the bytes are not `b"MZ"`, it logs `debug!(path = %path.display(), "Skipping Windows PE candidate without valid MZ header")` and returns `false`.
5. `.bat` and `.cmd` files that exist as regular files return `true` (the Rust standard library spawns batch files via `cmd.exe /c`).

### D2 Search order and explicit-path handling (`resolve_windows_executable`)

1. **Explicit path (`has_path_sep`):** When `program` contains `/`, `\`, or `:`, `resolve_windows_executable` checks `is_valid_windows_executable(Path::new(program))` and then `format!("{program}{ext}")` for each `ext` in `WINDOWS_EXEC_EXTS` (`.com`, `.exe`, `.bat`, `.cmd`). If none match, it returns `program.to_string()` unchanged at the end of the `has_path_sep` block so an explicit path never falls through to a `PATH` search for its base name.
2. **Git for Windows fast path (bare names):** When `!has_path_sep`, `resolve_windows_executable` extracts `bare_name` and probes `C:\Program Files\Git\usr\bin` and `C:\Program Files\Git\bin` first via `probe_candidate_in_dir`. Checking Git first ensures bare Unix tool names (`find`, `sort`, `timeout`, `tar`, `ssh`, `sh`) resolve to the Unix utilities on Windows workers just as on macOS and Linux workers, even when Git's directories appear after `C:\Windows\System32` in `PATH`.
3. **Task `effective_path` walk (bare names):** Next, `resolve_windows_executable` iterates `std::env::split_paths(effective_path)` in order and calls `probe_candidate_in_dir(&dir, bare_name)`, which checks `dir.join(bare_name)` followed by `dir.join(format!("{bare_name}{ext}"))` across `WINDOWS_EXEC_EXTS`.
4. **Standard library fallback:** If no candidate passes validation, `resolve_windows_executable` returns `program.to_string()` unchanged so `tokio::process::Command::new` delegates to the standard library's OS lookup.

### D3 Spawn failure diagnostics (`execute_process_supervised`)

When `cmd.spawn()` returns `Err(e)` in `crates/worker/src/runner.rs`, `TaskResult::failure` records on all platforms:
- `stderr`: `Failed to spawn process '{program}' (resolved: '{resolved_program}'): {e}`
- `error`: `Some(format!("Process spawn error for '{program}' (resolved: '{resolved_program}'): {e}"))`

Both strings preserve the `"Failed to spawn process '<program>'"` and `"Process spawn error"` substrings expected by `crates/master/src/mapreduce.rs` while always naming the resolved path.

## Compatibility

- Wire protocol, CLI flags, and Unix command resolution are unchanged.
- Spawn failure messages (`stderr` and `result.error` in `crates/worker/src/runner.rs`) now include `(resolved: '<resolved_program>')` on every platform; `crates/master/src/mapreduce.rs`'s substring checks (`"Failed to spawn process 'sh'"` and `"Process spawn error"`) still match the new text.
- On Windows, tasks invoking bare commands (`cmd`, `powershell`, `sh`, `python`, `find`, `tar`) or explicit paths with or without an extension continue to work while skipping extensionless or non-`MZ` candidate files.

## Security

- Preventing an explicit path (`has_path_sep`) from falling back to `PATH` when validation fails ensures that `<abs>/tool` cannot unintentionally execute an unrelated `tool.exe` from another directory on `PATH`.
- Reading only 2 bytes (`[0u8; 2]`) from candidate `.com`/`.exe` files bounds file I/O during path resolution.

## Test plan

- `runner::outcome_tests::test_resolve_windows_executable_skips_extensionless_and_invalid_pe_files` in `crates/worker/src/runner.rs`.
- `runner::outcome_tests::test_resolve_windows_executable_explicit_path_and_pathext_order` in `crates/worker/src/runner.rs`.
- `cargo test -p rusty_grid_cli --test blob_transfer_e2e --locked` (10/10 passing on Windows).
- `cargo test -p rusty_grid_cli --test cli_doctor_e2e --locked` (5/5 passing on Windows).

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Require `.com`/`.exe`/`.bat`/`.cmd` and `MZ` header in `resolve_windows_executable`, keep Git-first search order for bare names, and keep explicit paths from falling through to `PATH` | `crates/worker/src/runner.rs` | Unit tests and `blob_transfer_e2e` / `cli_doctor_e2e` pass on Windows | Implemented |
| W2 | Log skipped candidates with `debug!` and include `resolved_program` in spawn failure messages | `crates/worker/src/runner.rs` | Spawn error unit test asserts `resolved_program` in `stderr` and `result.error` when `program != resolved_program` | Implemented |
| W3 | Add design record 0025 and update `README.md` and `CLAUDE.md` | `docs/design/0025-windows-executable-resolution.md`, `README.md`, `CLAUDE.md` | Hygiene checks pass | Implemented |

## Status log

- 2026-11-04: Implemented `WINDOWS_EXEC_EXTS` (`.com`, `.exe`, `.bat`, `.cmd`), `has_windows_exec_ext`, `is_valid_windows_executable` (with 2-byte `MZ` PE check and `debug!` logging), `probe_candidate_in_dir`, and `resolve_windows_executable` in `crates/worker/src/runner.rs`; included `resolved_program` in spawn error messages and verified with unit and end-to-end tests on Windows.
