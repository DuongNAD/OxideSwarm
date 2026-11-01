# 0013 — Ship Local Mapper/Reducer Scripts and Command Stages in `oxideswarm mapreduce`

- **Status:** Implemented (`MR-1`)
- **Date:** 2026-11-02
- **Scope:** `crates/cli/src/cli_app.rs`, `crates/master/src/mapreduce.rs`, `crates/cli/tests/test_cli_mapreduce_scripts.rs`

## Summary

Fixes `oxideswarm mapreduce` (`MR-1`) so that local file paths passed to `--mapper` and `--reducer` are read on the client machine and shipped inline as script text rather than sent as literal path strings, adds `--map-cmd` and `--reduce-cmd` for `Command` map/reduce stages, removes the unsupported `uppercase` operator from `--mapper --help`, supports floating-point numbers in the built-in `max` and `min` reducers, and surfaces a clear error when a worker lacks `sh`.

## Context & Problem

1. In `crates/cli/src/cli_app.rs` (`run_mapreduce`), when `--mapper ./local.sh` or `--reducer ./sum.sh` was passed, the CLI constructed `MapFunctionSpec::ShellScript { script: "./local.sh" }` without reading the file. The worker then executed `sh -c ./local.sh` inside an empty sandbox directory and failed unless `./local.sh` happened to exist on the worker.
2. `MapFunctionSpec::Command` and `ReduceFunctionSpec::Command` existed on the wire (`crates/core/src/mapreduce.rs`), but `oxideswarm mapreduce` had no `--map-cmd` or `--reduce-cmd` flags to use them.
3. `--mapper --help` advertised `uppercase`, which `builtin_map_script` (`crates/master/src/mapreduce.rs:449`) does not implement.
4. `reduce_builtin` (`crates/master/src/mapreduce.rs:604-621`) only inspected `val.as_i64()` for `"max"` and `"min"`, ignoring floating-point values (`3.5`, `"2.75"`).
5. When a worker lacked `sh` on `PATH`, `TaskRunner` failed to spawn `sh` and recorded the error in `result.error` with an empty `result.stderr`, so `MapReduceEngine` reported an empty failure reason.

## Decisions

### D1 — Read Local Mapper and Reducer Script Files on the Client
- When `--mapper` or `--reducer` contains a path separator (`/`, `\`), ends with `.sh`, or names an existing file on the client machine, `run_mapreduce` reads the file with `std::fs::read_to_string` and populates `ShellScript { script }` with the file's contents. If reading fails, the CLI prints an error to `stderr` and exits with code `1`.

### D2 — Add `--map-cmd` and `--reduce-cmd`
- Adds `--map-cmd <CMD>` (mutually exclusive with `--mapper`) and `--reduce-cmd <CMD>` (mutually exclusive with `--reducer`) to `MapReduceArgs`.
- Splits the command string into `(program, args)` (honouring single and double quotes) and constructs `MapFunctionSpec::Command { program, args }` / `ReduceFunctionSpec::Command { program, args }`.

### D3 — Floating-Point Support in `max` and `min` Built-in Reducers
- `reduce_builtin("max", ...)` and `reduce_builtin("min", ...)` parse `i64`, `f64`, and numeric string values, returning an integer JSON number when all numeric inputs are integers and a floating-point JSON number when any numeric input is a float.

### D4 — Clear Error When a Worker Lacks `sh`
- `MapReduceEngine` formats map and reduce task failures using `result.stderr_str()` when non-empty and falls back to `result.error`. When a `sh` stage fails to spawn (`exit_code == -1` or spawn error on `'sh'`), the error explains that the worker lacks `sh` on `PATH` and suggests installing a POSIX shell or using `--map-cmd` / `--reduce-cmd`.

## Status Log

- 2026-11-02: Implemented in `crates/cli/src/cli_app.rs` and `crates/master/src/mapreduce.rs`, verified by `crates/cli/tests/test_cli_mapreduce_scripts.rs` (`MR-1`).
