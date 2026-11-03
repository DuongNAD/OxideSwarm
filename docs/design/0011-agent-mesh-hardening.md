# 0011: Agent Mesh Command Hardening, Process Controls, and Launcher Integration

Status: Implemented
Created: 2026-11-01    Updated: 2026-11-02

## Summary

Hardens the agent mesh command execution layer across Rust (`agent_mesh`) and Python (`scripts/agent_node.py`)
runtimes (SEC-6). Restricts nodes to 5 advertised commands (`echo`, `ping`, `system_info`, `sha256_verify`,
`shell_exec`), rejecting unadvertised commands with exit code 127. Adds `--no-shell` flag to suppress
`shell_exec` advertising and execution. Strips 5 sensitive environment variables from child command
inheritance, detaches stdin (`DEVNULL`), caps output streams at 1 MiB per stream with truncation markers,
and exposes `--no-shell` across all platform launchers.

## Context

Prior to SEC-6, mesh nodes allowed arbitrary commands to be executed via raw binary names or shells
without restriction (`crates/agent_mesh/src/executor.rs`, `scripts/agent_node.py`). Child processes
inherited all environment variables from the parent node process (including `OXIDESWARM_SECRET`,
`OXIDE_MESH_TOKEN`, and `RUSTY_GRID_SECRET`), inherited stdin (causing commands like `cat` to hang
until timeout), and had unbounded stdout/stderr capture that could exceed WebSocket message limits
(16 MiB). In addition, platform launcher scripts (`packaging/agent_mesh/*`) did not allow operators
to configure `--no-shell`.

## Goals

- G1: Enforce advertised command restriction: only 5 commands (`echo`, `ping`, `system_info`,
  `sha256_verify`, `shell_exec`) are accepted; all unadvertised binaries/commands return `exit_code: 127`
  and `status: "failed"`.
- G2: Provide `--no-shell` mode: omits `shell_exec` from capability registration and refuses incoming
  `shell_exec` with `exit_code: 127` while keeping safe built-ins operational.
- G3: Environment variable sanitization: strip 5 sensitive variables (`OXIDESWARM_SECRET`,
  `OXIDESWARM_SECRET_FILE`, `OXIDE_MESH_TOKEN`, `RUSTY_GRID_SECRET`, `RUSTY_GRID_SECRET_FILE`) from
  parent process inheritance before child execution, while preserving user-provided `env` overrides.
- G4: Process I/O controls: detach stdin (`DEVNULL` / null) so interactive commands terminate immediately;
  cap stdout and stderr streams at 1 MiB (1,048,576 bytes) with an explicit truncation notice
  (`[agent_mesh: output truncated after 1048576 bytes]` / `[agent_node: ...]`).
- G5: Process tree termination: guarantee that timed-out commands cleanly terminate the leader and all
  descendants/grandchildren, across Unix and Windows.
- G6: Platform launcher support: allow operators to activate `--no-shell` across all 4 OS launchers
  (`ubuntu`, `macos`, `android`, `windows`) via CLI flag or `OXIDE_MESH_NO_SHELL=1`.

## Non-goals

- Host OS containerization or virtualization: this is not a sandbox. Commands run under the user account
  of the node, with access to host filesystem and network.
- Mitigating file-system reads of secrets: if a sender has `shell_exec` permissions, it can read
  `~/.oxideswarm/cluster.secret` or inspect `/proc` / process lists; env stripping prevents ambient
  inheritance, not deliberate local credential theft. Only `--no-shell` eliminates arbitrary command execution.
- Restricting `grid_submit` on embedded grid masters: `--no-shell` applies to mesh node execution; if a
  hub is started with `--grid-master`, compute tasks dispatched through the grid bridge run on grid workers
  governed by worker process controls.

## Design

### D1: Command Whitelist & 127 Exit Code Protocol
Both runtimes check the requested command against the set of 5 supported commands. If the command is not
in `["echo", "ping", "system_info", "sha256_verify", "shell_exec"]`, execution is refused immediately
without spawning a process, returning `exit_code: 127`, `status: "failed"`, and a descriptive `stderr` message.

### D2: `--no-shell` Policy & Capability Negotiation
When `--no-shell` is enabled:
- The node omits `"shell_exec"` from the `supported_commands` list inside its registration capabilities payload.
- If a `shell_exec` request is received, the node returns `exit_code: 127`, `status: "failed"`, and
  `stderr: "shell_exec is disabled by --no-shell"`.
- Built-in safe commands (`echo`, `ping`, `system_info`, `sha256_verify`) remain fully functional.

### D3: Environment Sanitization
Before spawning a subprocess, the node removes `SECRET_ENV_VARS`:
`OXIDESWARM_SECRET`, `OXIDESWARM_SECRET_FILE`, `OXIDE_MESH_TOKEN`, `RUSTY_GRID_SECRET`, `RUSTY_GRID_SECRET_FILE`.
User-provided environment variables passed in `args.env` are applied after sanitization, allowing callers
to explicitly supply task-specific variables.

### D4: Stream Capping & Truncation Marker
Stdout and stderr reader tasks read up to 1,048,576 bytes (1 MiB). Any excess stream bytes are drained and
discarded to prevent pipe blocking. When truncated, the stream is appended with
`\n[agent_mesh: output truncated after 1048576 bytes]` (or `[agent_node: ...]`).

### D5: Stdin Detachment
Child processes are spawned with `stdin` set to null / `asyncio.subprocess.DEVNULL`, preventing commands
like `cat` from hanging indefinitely.

### D6: Process Tree Termination
On Unix, commands are spawned with `process_group(0)`, and timeouts signal the group via `killpg(SIGKILL)`
followed by descendant cleanup. On Windows, `taskkill /F /T` terminates the process tree.

### D7: Platform Launcher Integration
All launchers (`run_agent_node.sh` on Ubuntu/macOS/Termux and `run_agent_node.cmd` on Windows) inspect the
environment variable `OXIDE_MESH_NO_SHELL` (value `1`) or the CLI argument `--no-shell`, appending
`--no-shell` to the native binary or fallback Python client invocation.

### D8: CLI Process Exit vs Protocol Exit Code (F11)
`agent-mesh send` acts as a message router client. When the target node answers with `exit_code: 127`,
`agent-mesh send` prints `Exit Code: 127` and exits process code 0 because message delivery succeeded.
Process code non-zero is reserved for transport or delivery failure (`DeliveryNack`).

### D9: Variable List Parity (F12)
`RUSTY_GRID_SECRET` and `RUSTY_GRID_SECRET_FILE` are retained defensively to shield potential legacy
grid variables. A shared test validates that Rust and Python lists remain identical.

### D10: Parity Matrix for Non-Uniform Runtime Behaviors (F10)
- `timeout_ms: null`: Both runtimes safely default to 15000 ms.
- `cwd` of wrong type: Both runtimes ignore non-string values and safely run in the default working directory.
- `command` vs `cmd` keys: Rust requires canonical `cmd` property in dictionary; Python accepts `cmd` or `command` alias for backwards compatibility.
- `args: null`: Rust converts null Value to string `"null"` (exit 127); Python treats empty args as empty command string `""` (exit 0).
- `env` non-string values (`true`, `null`): Rust formats JSON strings `true` and `null`; Python stringifies to `True` and `None`.
- Truncation markers: Rust uses `[agent_mesh: output truncated after 1048576 bytes]`; Python uses `[agent_node: output truncated after 1048576 bytes]` to identify runtime origin.

## Compatibility

- Wire envelopes (`CommandRequest`, `CommandResponse`) are unchanged.
- Nodes without `--no-shell` maintain existing execution capabilities.
- Hubs route commands without inspecting capabilities, and the mesh node itself inspects capabilities and refuses unauthorized commands (such as `shell_exec`).

## Security

Stripping environment variables prevents child processes from inadvertently reading secrets via `env` or
`printenv`. However, if `shell_exec` is active, the process runs as the node's OS user and can read
accessible secret files (e.g. `~/.oxideswarm/cluster.secret`). To completely eliminate remote command execution,
operators must activate `--no-shell`.

## Test Plan

- `crates/agent_mesh/src/executor.rs`: unit tests for stream capping and timeout handling.
- `crates/agent_mesh/tests/test_sec6_mesh_sandbox.rs`: integration tests for command restriction, `--no-shell`,
  and environment sanitization.
- `tests/test_sec6_mesh_sandbox.py`: Python node verification suite.
- `tests/test_launcher_scripts.py`: tests verifying `--no-shell` propagation from launchers.
- `.github/workflows/ci.yml`: automated verification in CI.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| SEC6-1 | Command restriction & 127 code | `executor.rs`, `agent_node.py` | Unadvertised commands return 127 | Implemented |
| SEC6-2 | `--no-shell` flag & filter | `client.rs`, `main.rs`, `agent_node.py` | Capability filtered and execution refused | Implemented |
| SEC6-3 | Env sanitization & 5 vars | `executor.rs`, `agent_node.py` | 5 vars absent from child env | Implemented |
| SEC6-4 | Output cap at 1 MiB | `executor.rs`, `agent_node.py` | Stream truncated with marker | Implemented |
| SEC6-5 | Launcher `--no-shell` support | `packaging/agent_mesh/*` | All launchers accept flag / env var | Implemented |
| SEC6-6 | CI integration | `.github/workflows/ci.yml`, `run_all_mesh_tests.py` | Tests run in CI | Implemented |

## Status log

- 2026-11-01: SEC-6 implementation landed in branch `win/sec-6-mesh-sandbox` (commit `a95e668`).
- 2026-11-02: Review fixes applied: design record added, terminology clarified, launchers updated, CI enabled.
