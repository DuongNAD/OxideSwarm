# 0001: Two-node readiness: task placement, task inputs and a cluster check

Status: Implemented (the macOS + Windows run is still to be done, see W7)
Created: 2026-10-23    Updated: 2026-10-23

## Summary

Make a cluster of two different machines (for example a MacBook as master and GPU worker, and a
Windows laptop as CPU worker) usable and verifiable:

- pin a task to an operating system, a CPU architecture, a worker tag or one named worker;
- send a script, standard input and environment variables with a task;
- choose the size of a GPU task from the CLI;
- check a running cluster end to end with one command, `oxideswarm doctor`.

The wire format does not change: placement data travels in the `tags` fields that `Task` and
`WorkerCapabilities` already carry.

## Context

Behaviour before this change (commit `5103e53`):

| Area | Behaviour | Code |
|---|---|---|
| Placement | A task can ask only for CPU cores, RAM, a GPU, a timeout and retries. Nothing selects an operating system or a worker, so a mixed macOS/Windows cluster can only run commands that behave the same on both. | `TaskRequirements` in `crates/core/src/task.rs`; `Engine::candidates` in `crates/master/src/scheduler.rs` |
| Tags | `Task.tags` is carried and counted for memory, nothing else. `WorkerCapabilities.tags` holds `low_battery` / `thermal_throttled` (read by the scheduler) and `link:*` (added by the master). Workers add no tags of their own, and the CLI cannot set any. | `crates/master/src/queue.rs`, `scheduler.rs`, `server.rs`; `WorkerConfig::tags` in `crates/worker/src/client.rs` |
| Inputs | `TaskSpec::Command` has `stdin` and `env`, `TaskSpec::ShellScript` has `interpreter` and `env`; `submit` sets none of them. Every file a command reads must already exist on the worker. | `build_task_spec` in `crates/cli/src/cli_app.rs` |
| GPU | `submit --type gpu` always sends a 64x64 GEMM with 16x16 tiles. Workers accept up to 8192 on a real GPU (device limits permitting) and 2048 on the simulated one. | `build_task_spec`; `GPU_MAX_DIM`, `SIMULATOR_MAX_DIM` in `crates/worker/src/gpu.rs` |
| Compile | `submit --type compile` sends no source files, and `TaskSpec::validate` rejects a compilation without sources, so it always fails on the master. | `build_task_spec`; `TaskSpec::validate` |
| Results | `submit --wait` prints exit code, time and output, but not which worker ran the task. | `run_submit` |
| Verification | `test_integration.sh` and `run_cluster_benchmark.sh` start every process on one machine; `ox-mode research distributed` is an in-memory simulation. Nothing checks a real multi-machine cluster. | |

Every submit path (client, dashboard, agent-mesh bridge) reaches `TaskQueue::submit_with_priority`,
which calls `Task::validate`. Validation added there covers all paths.

## Goals

- **G1** Run a task only on workers of a given OS, architecture or tag, or on one named worker.
- **G2** Send a script file, standard input and environment variables with a task.
- **G3** Choose the GEMM size and tile of a GPU task from the CLI.
- **G4** One command checks connection, authentication, every worker, process start-up on every OS,
  every GPU and parallel spreading, and exits non-zero when a check fails.
- **G5** Documentation follows [docs/README.md](../README.md); a guide covers a macOS + Windows pair.

## Non-goals

- Copying arbitrary files to workers (`--file`) or a distributed file system: needs a new `TaskSpec`
  field and a protocol version bump.
- Pooling memory or splitting one process across machines.
- Changes to discovery, transports, authentication or the dashboard UI.
- Scheduling by measured worker speed.

## Design

### D1 Worker tags

- At start-up the worker adds `os:<std::env::consts::OS>` and `arch:<std::env::consts::ARCH>` to
  `WorkerCapabilities.tags`, for example `os:macos` + `arch:aarch64`, `os:windows` + `arch:x86_64`.
- User tags: `worker --tag <TAG>` (repeatable), else `RUSTY_GRID_WORKER_TAGS` (comma-separated),
  else `tags = [...]` in the `[worker]` section of the config file.
- In `ListWorkers` and `ClusterStatus` answers the master adds `link:<path>` (as before),
  `id:<worker uuid>` and `state:<connected|busy|disconnected>`. Clients receive only
  `WorkerCapabilities`, so the `id:` tag is how they learn the id a `worker` constraint can name.
- A user tag is 1 to 64 bytes with no whitespace, `=` or `,`. The prefixes `os:`, `arch:`, `link:`,
  `id:` and `state:` are reserved; a worker given such a tag refuses to start with a configuration
  error (`WorkerClient::run`, and the CLI before that).

### D2 Placement constraints

A task carries constraints in `Task.tags` as `require:<key>=<value>`:

| Key | A worker matches when | Values |
|---|---|---|
| `os` | it has the tag `os:<value>` | `macos`, `windows`, `linux`, `android`, ... Aliases: `mac`, `darwin`, `osx` = `macos`; `win` = `windows` |
| `arch` | it has the tag `arch:<value>` | `x86_64`, `aarch64`, ... Aliases: `amd64`, `x64` = `x86_64`; `arm64` = `aarch64` |
| `worker` | its id equals the value, or its name does (ignoring case) | worker UUID or name |
| `tag` | it has exactly this tag | a user tag (D1) |

- All constraints must hold. `os`, `arch` and `worker` appear at most once; `tag` may repeat.
- One module, `crates/core/src/placement.rs`, parses, normalises, formats and matches constraints for
  the CLI and the master.
- `Task::validate` rejects an unknown key, a repeated single-value key or an empty value, so every
  submit path answers with an error. Tags without the `require:` prefix keep their meaning (none).
- The scheduler checks constraints in its static stage, before free slots, so `hold_back` only ever
  reserves a matching worker. `Shape` includes the constraints, so tasks with different constraints
  are never treated as interchangeable.
- With no matching worker connected the task waits in the queue, like a GPU task without a GPU
  worker, and the scheduling report gives `ClusterLacksCapability` as the reason. With
  `fail_unfulfillable_tasks` the report lists it as unfulfillable instead, exactly as it does for a
  GPU task (the master does not fail either automatically).
- A queued task whose constraints cannot be read (only possible for a task that bypassed
  `Task::validate`) is placed nowhere and reported as unfulfillable.

### D3 `submit` options

| Option | Task type | Effect |
|---|---|---|
| `--os <OS>`, `--arch <ARCH>`, `--worker <NAME\|ID>`, `--require-tag <TAG>` (repeatable) | all | D2 constraints |
| `--stdin-file <PATH>` | `command` (default) | the file content is the process's standard input |
| `--script-file <PATH>` | `shell` | the file content is the script; cannot be combined with `--command` or words after `--` |
| `--interpreter <PROG>` | `shell` | run the script with `PROG` (for example `python3`) instead of `sh` or `cmd.exe` |
| `--env <KEY=VALUE>` (repeatable) | `command`, `shell` | environment variable of the task |
| `--matrix-dim <N>`, `--work-group-size <N>` | `gpu` | GEMM edge (default 64) and tile edge (default 16) |

- Standard input plus script content is limited to 32 MiB per task (frames are capped at 64 MiB,
  `MAX_FRAME_SIZE`); a larger file is refused before connecting.
- `--wait` output adds `Worker: <name> (<id>)`; `--json` output adds a `worker_name` field.
- Before submitting a task with constraints, the CLI fetches the worker list and warns when no
  connected worker matches. The task is still submitted and waits.
- `--type compile` fails in the CLI with a message that compile tasks need source files, which only
  the agent-mesh bridge (`grid_compile`) supplies.

### D4 `oxideswarm doctor`

Checks a running master, with the same address and secret resolution as `submit`:

| ID | Check | Passes when |
|---|---|---|
| C1 | Connect | TCP connection and cluster handshake succeed |
| C2 | Workers | at least `--expect-workers <N>` (default 1) workers are registered; each is listed with name, id, OS, architecture, cores, RAM, GPU, link |
| C3 | In-process probe, per worker | a `BuiltinTest hash_compute` pinned with `require:worker=<id>` completes on that worker |
| C4 | Process probe, per worker | a pinned `echo` (`cmd /C` on Windows, `sh -c` elsewhere) completes and prints the expected token; skipped for a worker without an `os:` tag |
| C5 | GPU probe, per GPU worker | a pinned GEMM of `--gpu-dim <N>` (default 512, at most 2048 on a simulated GPU) reports `Status: VERIFIED_OK`; time and GFLOP/s are printed |
| C6 | Spread | `--spread-tasks <N>` unpinned 500 ms `sleep` tasks (default 4 per worker) complete; with two or more workers, at least two of them ran some (otherwise a warning) |

- A probe that completes on another worker than the one it was pinned to fails C3 with "the master
  ignores placement constraints; upgrade the master".
- Probes are submitted without `wait` and collected with `GetTaskResult`, so one slow worker does
  not serialise the run. Each probe times out after `--probe-timeout-secs` (default 60).
- `--skip-gpu` and `--skip-spread` leave out C5 and C6. `--json` prints one JSON object.
- Exit code: 0 when no check failed, 1 when one failed, 6 when authentication failed.
- `doctor` runs only built-in tasks, `echo` and GEMM; nothing is written on workers outside the
  per-task sandbox.

### D5 Several workers on one host

`worker --no-portal` (env `RUSTY_GRID_NO_PORTAL`, config `no_portal`) leaves out the standby portal
that otherwise binds `0.0.0.0:8080`, so a second worker on one machine (or a test) opens no port to
the network. Default behaviour is unchanged.

### D6 Documentation

- `docs/README.md`: index and writing standard (added with this record).
- `docs/guides/two-machine-cluster.md`: macOS + Windows guide covering Wi-Fi, a direct Ethernet cable
  with a USB-C adapter and static addresses, P2P across networks, and verification with `doctor`.
- Stale statements fixed: `DEPLOYMENT.md` section 3.5 says the grid has no version negotiation, but
  workers send `protocol_version` and feature bits at registration and the master refuses versions
  below `PROTOCOL_VERSION_MIN_COMPATIBLE` (`crates/master/src/server.rs`); the README environment
  variable heading carries a count; a heading of
  `benchmarks/interconnect/COMPARATIVE_INTERCONNECT_BENCHMARK.md` is not in English.
- `README.md`: CLI reference and environment variables for D1 to D4. `CLAUDE.md`: new modules.

## Compatibility

- The message layout does not change and `PROTOCOL_VERSION_CURRENT` stays 1: both `tags` fields
  already exist in the public structs, in their `Human*` / `Binary*` mirrors and in all `From`
  conversions.
- Mixed versions:
  - old master, new CLI: `require:` tags are ignored and the task may run on any worker. `doctor`
    detects this (C3).
  - new master, old worker: the worker has no `os:` / `arch:` tags, so `os` and `arch` constraints
    never match it; `worker` constraints still do.
  - new master, old CLI: behaviour unchanged.
- Upgrade rule (documented in the guide): build every node from the same commit and restart the
  master first.

## Security

- No new port, listener or trust relationship. An authenticated client can already run any command
  on a worker; sending a script or standard input with the task does not widen that.
- Script and input travel inside the existing authenticated connection, encrypted after the
  handshake (format `0x04`).
- The 32 MiB input limit keeps a task below the 64 MiB frame limit; workers keep their output limit
  (`DEFAULT_MAX_OUTPUT_BYTES`, 10 MiB).
- The worker still removes its own `OXIDESWARM_SECRET`, `OXIDESWARM_SECRET_FILE` and
  `OXIDE_MESH_TOKEN` before it applies the task's environment (`SECRET_ENV_VARS` in
  `crates/worker/src/runner.rs`); `--env` values are passed as given.

## Test plan

- **Unit:** placement parsing, aliases, formatting, matching and every rejection case; worker tag
  validation and system tags; scheduler filtering (pinned, OS, tag, combined, `Shape` separation,
  `hold_back` with constraints, unfulfillable); `build_task_spec` for every new option; input limit.
- **Integration:** in-process master and two workers with different tags; pinned tasks land on the
  right worker; an unknown key is rejected; `doctor` passes on a healthy cluster and fails C3 when a
  pinned task cannot run.
- **Live, Windows host:** master and two workers on loopback, one built with `gpu-wgpu` on the local
  GPU; `doctor --expect-workers 2` passes every check.
- **Live, macOS + Windows:** master and GPU worker on the Mac, CPU worker on the Windows laptop, over
  Wi-Fi and over Ethernet; `doctor --expect-workers 2` passes every check.
- **Gates:** `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo check -p rusty_grid_master --no-default-features`, `cargo test --workspace`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Placement module | `crates/core/src/placement.rs`, `crates/core/src/lib.rs`, `crates/core/src/task.rs` (`Task::validate`) | D2 parsing, formatting, matching and validation, with unit tests | Implemented |
| W2 | Worker tags | `crates/worker/src/client.rs`, `crates/master/src/server.rs` (`listed_worker`), `crates/cli/src/cli_app.rs`, `crates/cli/src/config.rs` | registered workers carry `os:`, `arch:` and user tags; listings add `id:` / `state:`; reserved prefixes refused; `--no-portal` (D5) | Implemented |
| W3 | Master enforcement | `crates/master/src/queue.rs` (`TaskView`), `crates/master/src/scheduler.rs` | constrained tasks reach only matching workers; unit tests from the test plan pass | Implemented |
| W4 | `submit` options | `crates/cli/src/cli_app.rs` | D3 options with unit tests; worker shown in results; no-match warning; compile message | Implemented |
| W5 | `doctor` command | `crates/cli/src/doctor.rs`, `crates/cli/src/cli_app.rs`, `tests/cli_doctor_e2e.rs` | D4 checks and exit codes; integration test passes | Implemented |
| W6 | Documentation | `docs/guides/two-machine-cluster.md`, `README.md`, `DEPLOYMENT.md`, `CLAUDE.md`, `PROJECT.md`, benchmark report | D6 done; each new flag and variable documented once, in `README.md` | Implemented |
| W7 | Verification | none | all gates green; live Windows run recorded in the status log; macOS + Windows run | Partial: the macOS + Windows run is open |

## Status log

- 2026-10-23: Created and accepted.
- 2026-10-23: W1 to W6 implemented on branch `feat/two-node-readiness`. Gates on the Windows host:
  `cargo test --workspace` 1132 passed, 0 failed (state after W4); `cargo test -p rusty_grid_cli`
  passed after W5, including `tests/cli_doctor_e2e.rs` (5 tests); `cargo clippy --workspace
  --all-targets -- -D warnings`, `cargo fmt --all -- --check` and
  `cargo check -p rusty_grid_master --no-default-features` clean.
- 2026-10-23: Live run on the Windows host (i5-14600KF, RTX 5060 Ti): an authenticated master on
  loopback, one worker with the wgpu engine and one with `--no-gpu`, release build with
  `--features gpu-wgpu`. `doctor --expect-workers 2 --gpu-dim 1024`: 8 passed, 0 failed; C5
  verified a 1024 x 1024 GEMM on Vulkan; C6 ran 8 tasks of 500 ms in about 545 ms, 4 per worker.
  `submit --os`, `--worker`, `--require-tag`, `--env`, `--stdin-file`, `--script-file` (Git for
  Windows `sh`) and `--type gpu --matrix-dim 2048` (`VERIFIED_OK`) behaved as specified; the
  no-match warning and the CLI-side rejection of a reserved tag worked.
- 2026-10-23: Found in the live run and fixed: the wgpu crates logged every device poll at INFO
  (dozens of lines per GPU task); without `RUST_LOG` they now log at WARN (`default_log_filter`).
- Open: the macOS (master + Metal worker) + Windows laptop run, `doctor --expect-workers 2`.
