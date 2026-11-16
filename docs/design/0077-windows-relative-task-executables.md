# 0077: Resolve relative staged executables against the task working directory on Windows (`BUG-21`)

Status: Implemented
Created: 2026-11-16    Updated: 2026-11-16

## Summary

Windows task executable resolution now resolves relative paths containing path separators (`./prog.exe`, `.\prog.exe`, `sub/prog.exe`, `sub\prog.exe`) against the task's working directory inside the sandbox (`--cwd` or the sandbox root), refuses traversal and sandbox escapes (`..\prog.exe`, `../prog.exe`, `sub\..\..\prog.exe`, `\prog.exe`, `/prog.exe`, `C:prog.exe`), keeps [design 0025](0025-windows-executable-resolution.md) `.com`/`.exe`/`.bat`/`.cmd` and `MZ` header validation, wraps `.bat`/`.cmd` via `%SystemRoot%\System32\cmd.exe /d /s /c "..."`, and passes an absolute `lpApplicationName` to `CreateProcessW` / `CreateProcessAsUserW` across `--isolation off`, `fs`, and `fs+net` (`BUG-21`).

## Context

In the `R20` Windows probe (`OxideSwarm_Agent_Hub` `test_runs/win_r20_gpu_probe.md`, row `W2_relpath_direct`, section 6 Gap 2), staging an executable with `oxideswarm submit --input <exe>:gemm_bench.exe --command ./gemm_bench.exe -- 1024` worked on macOS and Linux workers but failed on Windows workers:

- Under `--isolation off`, `resolve_windows_executable` (`crates/core/src/capabilities.rs`) saw a path separator (`/` or `\`) and checked `Path::new("./gemm_bench.exe").is_file()` against the worker process's own current directory instead of the task's working directory inside the sandbox, returning `./gemm_bench.exe` unchanged. `tokio::process::Command` then passed that relative path to `CreateProcessW`, failing with `os error 2` unless the executable happened to sit in the worker's working directory.
- Under `--isolation fs` and `--isolation fs+net`, `resolve_windows_executable_for_isolation` (`crates/core/src/capabilities.rs`) and `windows::resolve_program` (`crates/worker/src/isolation.rs`) likewise probed `./gemm_bench.exe` against the worker's current directory and returned `None` / `"program './gemm_bench.exe' was not found in PATH"`.
- In `spawn_with_security_capabilities` (`crates/worker/src/isolation.rs`), `CreateProcessAsUserW` / `CreateProcessW` passed `std::ptr::null()` for `lpApplicationName`, leaving Win32 command-line resolution sensitive to the worker's current directory and batch quoting rules.

## Goals

- **G1**: Resolve `./x.exe`, `.\x.exe`, `sub/x.exe`, and `sub\x.exe` against the task's working directory (sandbox root or `--cwd <REL>` inside the sandbox) under `--isolation off`, `fs`, and `fs+net`.
- **G2**: Keep [design 0025](0025-windows-executable-resolution.md) extension precedence (`.com`, `.exe`, `.bat`, `.cmd`) and 2-byte `MZ` header checks on `.com` and `.exe` files, without falling back to `PATH` or the worker's own current directory when a relative path with separators does not exist.
- **G3**: Refuse relative program paths that escape the task sandbox (`..\x.exe`, `../x.exe`, `sub\..\..\x.exe`, drive-relative `C:x.exe`, root-relative `\x.exe`, NUL bytes, or symlinks/junctions targeting outside the sandbox).
- **G4**: Pass a verified absolute `lpApplicationName` to `CreateProcessW` and `CreateProcessAsUserW` (and execute `.bat` / `.cmd` targets via `%SystemRoot%\System32\cmd.exe /d /s /c "..."`) so the worker process's current directory never affects task spawn.

## Non-goals

- Changing Unix command resolution (`Command::current_dir` and `PATH` lookup already resolve `./x` on Unix; Linux/macOS behavior is unchanged).
- Changing bare command resolution order (`Git\usr\bin` fast path under `--isolation off`, native `System32` / `Git\cmd` before MSYS2 under `--isolation fs|fs+net` from [design 0055](0055-usb-kit-windows-isolation.md)).

## Design

### D1 Relative program confinement and resolution (`crates/core/src/capabilities.rs`)

- Added `resolve_relative_windows_program_in_dir(program, working_dir, sandbox_root)` and shared helper `resolve_explicit_windows_program`:
  1. Rejects NUL bytes, `is_absolute()`, any `..` segment on `/` or `\`, `Component::ParentDir`, `Component::RootDir`, `Component::Prefix` (e.g., drive-relative `C:prog.exe` or `\prog.exe`), and empty clean segment lists.
  2. Joins the normalized relative segments onto absolute `working_dir` to form `base_candidate`, then validates via `resolve_canonical_prefix` + `win_path_starts_with_ci` that both `base_candidate` and any resolved executable (following junctions/symlinks via `std::fs::canonicalize`) remain inside `sandbox_root`.
  3. Probes `[""]` followed by `WINDOWS_EXEC_EXTS` (`[".com", ".exe", ".bat", ".cmd"]`) with `is_valid_windows_executable` (requiring regular file + `MZ` signature for `.com`/`.exe`).
- Added `resolve_windows_executable_in_dir` and `resolve_windows_executable_for_isolation_in_dir` (accepting `working_dir: Option<&Path>` and `sandbox_root: Option<&Path>`), keeping the 2-argument `resolve_windows_executable` and `resolve_windows_executable_for_isolation` wrappers compatible. Also resolves empty/relative `PATH` entries via `resolve_path_dir` against `working_dir` when present.

### D2 Worker runner wiring (`crates/worker/src/runner/supervise.rs`)

- `TaskRunner::execute_process_supervised_with_workspace` computes `(eff_sandbox_path, eff_cwd, eff_sandbox_base)` before program resolution (converting paths via `std::path::absolute`, plus 8.3 short-name normalization under AppContainer isolation).
- On Windows, resolves `program` through `resolve_windows_executable_for_isolation_in_dir` (when isolated) or `resolve_windows_executable_in_dir` (when `--isolation off`), passing `Some(&eff_cwd)` and `Some(&eff_sandbox_path)`. If `Err(e)` is returned (traversal, drive/root prefix, or sandbox escape), returns `TaskResult::failure` immediately without spawning.
- Passes `&eff_cwd` through `crate::isolation::wrap_program_in_dir` so AppContainer resolution uses the same task working directory and sandbox boundary.

### D3 AppContainer resolver & `lpApplicationName` hardening (`crates/worker/src/isolation.rs`)

- Added `wrap_program_in_dir(jail, working_dir, program, args, search_path)` (with `wrap_program` delegating with `working_dir = jail.sandbox_dir`).
- Updated `windows::resolve_program(program, search_path, working_dir, sandbox_dir)` to delegate non-`/bin/`/`/usr/bin/` separator paths to `resolve_relative_windows_program_in_dir`, verify `.com`/`.exe`/`.bat`/`.cmd` and `MZ` headers, normalize non-reparse 8.3 paths via `normalize_win_path`, and resolve relative `PATH` entries against `abs_cwd`.
- Updated `windows::spawn_with_security_capabilities`:
  1. Checks `Path::new(std_cmd.get_program()).is_absolute()` before allocating spawn pipe handles and returns `ErrorKind::InvalidInput` if a non-absolute path reaches AppContainer spawn.
  2. Passes a non-null wide `lpApplicationName` (`app_name_wide.as_ptr()`) to both `CreateProcessAsUserW` and `CreateProcessW`.
  3. When the resolved target has extension `.bat` or `.cmd`, sets `lpApplicationName` to `%SystemRoot%\System32\cmd.exe` and wraps `lpCommandLine` as `"<cmd.exe>" /d /s /c "\"<target>\" <args>"`, avoiding Win32 `CreateProcessW` batch-path dispatch quirks when directories contain spaces.

## Security

- Relative executable paths cannot escape `sandbox_dir` through `..` segments, drive-relative prefixes (`C:prog.exe`), root-relative paths (`\prog.exe`), or junctions/symlinks inside the sandbox pointing outside `sandbox_dir`.
- Passing an explicit absolute `lpApplicationName` eliminates worker-CWD ambiguity at Win32 spawn time even if another thread or launcher starts the worker from an arbitrary working directory.

## Compatibility

- Wire protocol (`PROTOCOL_VERSION_CURRENT = 9`), CLI flags, configuration files, and Unix behavior are unchanged.
- Existing callers of `resolve_windows_executable`, `resolve_windows_executable_for_isolation`, and `wrap_program` remain source-compatible.

## Test plan

- **Unit (`crates/core/src/capabilities.rs`):** `capabilities::tests::test_windows_relative_task_executable_resolution_and_sandbox_confinement` checks `./prog.exe`, `.\prog.exe`, `sub/prog.exe`, `sub\prog.exe`, `--cwd sub`, `.com`/`.exe`/`.bat`/`.cmd` + `MZ` rules, no-`PATH`-fallback for missing relative paths, refusal of `..\prog.exe`, `../prog.exe`, `sub\..\..\prog.exe`, `\prog.exe`, `/prog.exe`, `C:prog.exe`, NUL bytes, and junction escape outside `sandbox_root`.
- **Runner (`crates/worker/src/runner/outcome_tests.rs`):** `runner::outcome_tests::test_task_runner_relative_windows_program_paths_and_refused_escape` runs staged `./prog.cmd`, `.\prog.cmd`, `sub/prog.cmd`, `sub\prog.cmd`, and `working_dir: Some("sub")` under `TaskRunner`, and asserts `..\decoy.cmd` / `../decoy.cmd` are refused without executing the parent decoy.
- **Isolation (`crates/worker/tests/test_feat3_os_isolation.rs`):** `windows::test_windows_relative_staged_executables_across_isolation_modes` compiles real Win32 `.exe` binaries via `rustc`, runs `./prog.exe`, `.\prog.exe`, `sub/prog.exe`, `sub\prog.exe`, `working_dir = Some("sub")` + `./prog.exe`, and `.\dir with space\run me.bat` under both `IsolationLevel::Off` and `IsolationLevel::Fs`, and asserts `..\decoy.exe`, `../decoy.exe`, `..\other_task\decoy.exe`, `../other_task/decoy.exe`, and `..\..\decoy.exe` never run outside decoys.
- **CLI E2E (`tests/cli_isolation_e2e.rs`):** `test_cli_submit_staged_relative_executable_under_off_and_fs_isolation` stages a compiled `.exe` with `submit --input` and runs `--command ./prog.exe`, `.\prog.exe`, `sub/prog.exe`, and `sub\prog.exe` against real `master` + `worker --isolation off` and `worker --isolation fs` processes with decoy executables in the worker CWD and outside the sandbox, and verifies `..\prog.exe`, `..\other_task\prog.exe`, and `..\..\prog.exe` fail without executing decoys.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| BUG-21 | Resolve relative staged task executables against task working directory across all Windows isolation modes and refuse sandbox escapes | `crates/core/src/capabilities.rs`, `crates/worker/src/isolation.rs`, `crates/worker/src/runner/supervise.rs`, `crates/worker/src/runner/outcome_tests.rs`, `crates/worker/tests/test_feat3_os_isolation.rs`, `tests/cli_isolation_e2e.rs`, `docs/design/0077-windows-relative-task-executables.md`, `docs/README.md`, `CHANGELOG.md`, `docs/BACKLOG.md` | Windows unit, `TaskRunner`, `test_feat3_os_isolation`, and `cli_isolation_e2e` pass under `off` and `fs`, and CI `workspace tests (windows-latest)` passes twice | Implemented |

## Status log

- 2026-11-16: Implemented relative Windows task executable resolution and sandbox confinement across `crates/core/src/capabilities.rs`, `crates/worker/src/isolation.rs`, and `crates/worker/src/runner/supervise.rs`, added unit/`TaskRunner`/AppContainer/CLI E2E regression tests (`BUG-21`).
