# 0016 — Ship Local Mapper/Reducer Scripts and Command Stages in `oxideswarm mapreduce`

**Status:** Implemented (`MR-1`)  
**Created:** 2026-11-02  
**Updated:** 2026-11-02  

## Summary

Fixes `oxideswarm mapreduce` (`MR-1`) so that local file paths passed to `--mapper` and `--reducer` (containing `/`, `\`, or ending in `.sh`) are read on the client machine and shipped inline as script text rather than sent as literal path strings, adds `--map-cmd` and `--reduce-cmd` for `Command` map/reduce stages, removes the unsupported `uppercase` operator from `--mapper --help`, supports finite floating-point numbers in the built-in `max`, `min`, and `sum` reducers, and surfaces a clear error when a worker lacks `sh`.

## Context

1. In `crates/cli/src/cli_app.rs` (`run_mapreduce`), when `--mapper ./local.sh` or `--reducer ./sum.sh` was passed, the CLI constructed `MapFunctionSpec::ShellScript { script: "./local.sh" }` without reading the file. The worker then executed `sh -c ./local.sh` inside an empty sandbox directory and failed unless `./local.sh` happened to exist on the worker.
2. `MapFunctionSpec::Command` and `ReduceFunctionSpec::Command` existed on the wire (`crates/core/src/mapreduce.rs`), but `oxideswarm mapreduce` had no `--map-cmd` or `--reduce-cmd` flags to use them.
3. `--mapper --help` advertised `uppercase`, which `builtin_map_script` (`crates/master/src/mapreduce.rs`) does not implement.
4. `reduce_builtin` (`crates/master/src/mapreduce.rs`) only inspected `val.as_i64()` for `"max"` and `"min"`, ignoring floating-point values (`3.5`, `"2.75"`), and did not guard against non-finite float strings (`"inf"`, `"NaN"`), which `serde_json::json!` serializes as `null`.
5. When a worker lacked `sh` on `PATH`, `TaskRunner` failed to spawn `sh` and recorded the error in `result.error` (`Process spawn error: ...`) with a spawn failure on `stderr`, and `MapReduceEngine` gave no hint about why `sh` failed or how to use `--map-cmd` / `--reduce-cmd`.

## Goals

- Read local script files on the client when `--mapper` or `--reducer` is a script path (`/` or `\` separator or `.sh` suffix) after checking built-in operator names first, enforcing a 64 KiB size cap on inline scripts.
- Expose `--map-cmd <CMD>` and `--reduce-cmd <CMD>` on `oxideswarm mapreduce`, preserving unquoted Windows path backslashes.
- Support finite floating-point numbers (both JSON numbers and numeric strings) in built-in `max`, `min`, and `sum` reducers without emitting `null` on `inf`/`NaN`.
- Append a clear missing-`sh` diagnostic hint when a `sh`-backed stage fails to spawn because `sh` is not found on the worker.

## Non-goals

- Automatic rescheduling or placement filtering of `sh`-backed MapReduce stages away from workers that lack `sh`.
- Executing `#!` shebang interpreters for `--mapper` / `--reducer` scripts (use `--map-cmd` / `--reduce-cmd` for non-`sh` interpreters such as `python3`).
- Shipping multi-megabyte scripts or binary executables via `MapFunctionSpec::ShellScript` (use `oxideswarm submit --input` or stage binaries onto workers and invoke them via `--map-cmd` / `--reduce-cmd`).

## Design

### 1. Client-Side Script Detection and Size Cap (`crates/cli/src/cli_app.rs`)

- Built-in mapper names (`word_count`, `line_count`, `identity`) and built-in reducer names (`sum`, `count`, `max`, `min`, `concat`) are matched first so that local files named `word_count` or `sum` in the current working directory are never read or executed when default or built-in operator names are used.
- A non-built-in `--mapper` or `--reducer` value is treated as a local script path if and only if `looks_like_script_path` is true: the value contains `/`, contains `\`, or ends with `.sh`. `Path::exists()` is never consulted for bare names.
- `read_mapreduce_script` reads the local file on the client machine and enforces `MAX_MAPREDUCE_SCRIPT_BYTES` (`64 KiB`). If the file cannot be read or exceeds `64 KiB` (which could otherwise hit OS `E2BIG` argv limits when passed to `sh -c`), the CLI prints a clear error to `stderr` and exits with code `1`.
- Shipped `ShellScript` stages are executed on workers as `sh -c <script>`. Because the script text is passed inline to `sh -c`, any `#!` shebang line inside the script is treated as a comment by `sh` and ignored. To run Python or another interpreter, pass `--map-cmd "python3 ..."` or `--reduce-cmd "python3 ..."`.

### 2. `--map-cmd` and `--reduce-cmd` (`crates/cli/src/cli_app.rs`)

- Adds `--map-cmd <CMD>` (mutually exclusive with `--mapper`) and `--reduce-cmd <CMD>` (mutually exclusive with `--reducer`) to `MapReduceArgs`.
- `split_command_line` splits the command string into `(program, args)` honouring single and double quotes. Outside single quotes, a backslash `\` acts as an escape only before `"`, `'`, `\`, or (when unquoted) whitespace; ordinary unquoted Windows path backslashes (e.g. `C:\Tools\mapper.exe --flag`) are preserved literally.

### 3. Finite Floating-Point Support in Built-in Reducers (`crates/master/src/mapreduce.rs`)

- `reduce_builtin` for `"sum"`, `"max"`, and `"min"` accepts integer (`i64`), finite floating-point (`f64`), and numeric string values (`s.parse::<i64>()` / `s.parse::<f64>()` with `f.is_finite()`).
- Non-finite floating-point values (`inf`, `-inf`, `NaN`) are ignored so `serde_json::json!` never serializes a `null` output for a completed key.

### 4. Missing-`sh` Diagnostic and Worker Placement (`crates/master/src/mapreduce.rs`)

- `task_failure_detail(result, uses_sh)` inspects `result.stderr_str()` and `result.error`. When `uses_sh` is `true` (`MapFunctionSpec::Builtin`, `MapFunctionSpec::ShellScript`, or `ReduceFunctionSpec::ShellScript`), the failure is a process spawn error, and the error text indicates the executable was not found (`not found`, `no such file`, `cannot find the file`, or `os error 2`), it appends:
  `(worker does not have 'sh' on PATH; install a POSIX shell such as Git for Windows or pass --map-cmd / --reduce-cmd)`.
- Other spawn errors (such as `os error 7` / `Argument list too long`) and `Command` stages (`uses_sh == false`) do not receive the missing-`sh` hint.
- MapReduce stages use `TaskRequirements::generic` without OS placement constraints; if any worker in the cluster lacks `sh` and is assigned a partition of a `sh`-backed job, that partition fails and therefore fails the job.

## Compatibility

- **Inline shell snippets in `--mapper` / `--reducer`:** Previously, a string containing `/` such as `--mapper 'sed s/one/1/'` was sent verbatim to the master and run under `sh -c` on the worker. Because values containing `/` are now read as local script files on the client, inline shell commands containing `/` or ending in `.sh` fail on the client with `Failed to read mapper script`. Use `--map-cmd "sh -c 'sed s/one/1/'"` (or `--reduce-cmd`) instead.
- **Worker-only script paths:** Previously, `--mapper /opt/worker_only.sh` ran `/opt/worker_only.sh` on the worker without reading it on the client. Now `--mapper` reads `/opt/worker_only.sh` on the client machine; to invoke a script or binary that exists only on workers, pass `--map-cmd /opt/worker_only.sh` or `--reduce-cmd /opt/worker_only.sh`.

## Security

- **Execution authority:** The client reads `--mapper` / `--reducer` script files with the CLI user's permissions and sends their contents in `ClientMessage::SubmitMapReduce` over the authenticated cluster transport. The master never executes mapper or reducer scripts (built-in reducers are pure in-memory arithmetic on the master). Workers execute `ShellScript` (`sh -c <script>`) and `Command` stages as the worker's OS user inside a per-task working directory (`sandbox`). As documented in [docs/SECURITY.md](../SECURITY.md), the per-task directory is not a container: a shipped script or command can read any file readable by the worker's OS user, including `~/.oxideswarm/cluster.secret`, which is the same authority as `oxideswarm submit --command`.
- **Working-directory hijacking prevention (`F33-1`):** Built-in operator names (`word_count`, `line_count`, `identity`, `sum`, `count`, `max`, `min`, `concat`) take precedence over file paths, and `looks_like_script_path` requires `/`, `\`, or a `.sh` suffix rather than `Path::exists()`. Running `oxideswarm mapreduce --input in.txt` inside an untrusted directory containing `./word_count` or `./sum` never reads or executes those files unless the user explicitly passes `--mapper ./word_count` or `--reducer ./sum`.

## Test plan

- `cargo test -p rusty_grid_cli --test test_cli_mapreduce_scripts`
  - `test_cli_mapreduce_ships_local_mapper_and_reducer_scripts` (local `./local.sh` and `./sum.sh` shipped from a separate client directory; planted `word_count` and `sum` in cwd ignored by default flags; oversized `> 64 KiB` script rejected with size error)
  - `test_cli_mapreduce_map_cmd_and_reduce_cmd_and_float_max_min` (`--map-cmd`, `--reduce-cmd`, float and string-float `max`/`min`, `inf`/`NaN` skipped without `null`, Windows backslash path preserved in `--map-cmd`)
  - `test_mapreduce_missing_sh_returns_clear_error` (missing-`sh` hint on `Builtin` map and `ShellScript` reduce; no hint on `Command` map or `E2BIG` spawn failure)
- `cargo test -p rusty_grid_cli --test cli_dashboard_policy_e2e` (`test_mapreduce_exits_non_zero_when_the_job_fails`)

## Work items

- `MR-1`: Ship local mapper and reducer scripts from `oxideswarm mapreduce`, add `--map-cmd` and `--reduce-cmd`, support float `max`/`min`, and surface missing-`sh` diagnostics.

## Status log

- 2026-11-02: Implemented `MR-1` in `crates/cli/src/cli_app.rs` and `crates/master/src/mapreduce.rs`; addressed review findings `F33-1` through `F33-8` and renumbered design record to `0016`.
