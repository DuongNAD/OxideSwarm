# 0079: Make the Agent Mesh launchers safe by default (`SEC-15`)

Status: Implemented
Created: 2026-11-16    Updated: 2026-11-16

## Summary

The Windows PowerShell launcher gains `-NoShell` and honours `OXIDE_MESH_NO_SHELL=1`, like the `.cmd`, macOS, Ubuntu and Termux launchers. The Android `adb` launcher hands the hub token to the phone on standard input, so no `adb` argument holds it. A node (the Rust client and `scripts/agent_node.py`) logs a warning when it sends a token over `ws://` to a host that is not on loopback. No wire format, node option or default changes.

## Context

- A mesh node advertises and runs `shell_exec` unless it starts with `--no-shell` (`crates/agent_mesh/src/client.rs`), so one hub token gives a shell on every node.
- `packaging/agent_mesh/windows/run_agent_node.ps1` had no way to pass `--no-shell`. Its `.cmd` sibling and the macOS launcher read `OXIDE_MESH_NO_SHELL=1` and a `--no-shell` argument.
- `packaging/agent_mesh/android/run_agent_node_adb.sh` built `OXIDE_MESH_TOKEN='<token>' agent-mesh node ...` as the argument of `adb shell`, so the token sat in the process list of the host and of the phone.
- The hub serves `ws://` only (TLS goes through a proxy, [design 0011](0011-agent-mesh-hardening.md)), so a token sent to a hub beyond loopback crosses the network in clear text. The launchers already printed a notice; the nodes themselves said nothing.

## Goals

- **G1**: `run_agent_node.ps1 -NoShell` and `OXIDE_MESH_NO_SHELL=1` pass `--no-shell` to the native binary and to the Python fallback.
- **G2**: The `adb shell` command line holds no token.
- **G3**: A node warns once per connection attempt when a token goes over `ws://` to a non-loopback host, never prints the token, and behaves as before.

## Non-goals

- Making `--no-shell` the default. That would break existing deployments that rely on `shell_exec`; it needs its own decision.
- Serving TLS from the hub.
- Adding `--no-shell` to the Android `adb` launcher.

## Design

### D1 One variable name

All launchers use `OXIDE_MESH_NO_SHELL` (value `1`) plus the option `--no-shell` (`-NoShell` in PowerShell). The PowerShell launcher builds `$NoShellArgs` once and splats it into both launch paths (`@NoShellArgs`).

### D2 Token on standard input for `adb`

When `OXIDE_MESH_TOKEN` is set, `run_agent_node_adb.sh` runs `printf '%s\n' "$OXIDE_MESH_TOKEN" | adb shell "IFS= read -r OXIDE_MESH_TOKEN; export OXIDE_MESH_TOKEN; exec <agent> node ..."`. The remote shell reads the token and exports it to the agent, which reads `OXIDE_MESH_TOKEN` as before. Nothing is written to the phone, so there is no file to remove. Without a token the command runs unchanged and reads nothing.

### D3 Warning in the nodes

`token_sent_in_clear_to_remote_host(hub_url, has_token)` (Rust, `crates/agent_mesh/src/client.rs`; Python, `scripts/agent_node.py`) is true when a token is set, the scheme is `ws`, and the host is not `localhost`, `*.localhost` or a loopback IP address (user-info before `@` is ignored, so `ws://127.0.0.1@192.0.2.1/` counts as remote). `connect` logs a warning (`tracing::warn!`, `logging.warning`) naming the URL but not the token. `wss://` and tokenless nodes do not warn.

## Security

- Disabling `shell_exec` stays opt-in; this change only makes the opt-in reachable on every launcher.
- The token no longer appears in `adb` arguments or the phone's process list while the launcher runs. It is still present in the agent's environment on the phone, as on every other platform.
- The warning does not stop the connection: a trusted network or tunnel is the operator's call.

## Compatibility

Wire protocol, hub, node CLI and defaults are unchanged. A launcher run without the new flag or variable behaves as before. The `adb` launcher needs a remote shell with `read` and `exec` (every Android `sh`).

## Test plan

- `tests/test_launcher_scripts.py`: `StaticRepositoryChecks.test_windows_ps1_agent_launcher_supports_no_shell` (`-NoShell`, `OXIDE_MESH_NO_SHELL`, both launch paths, CRLF kept) and `test_agent_mesh_launchers_support_no_shell`; `ShellLauncherTests.test_android_adb_launcher_keeps_token_off_the_adb_command_line` (a fake `adb` records its arguments and standard input: the token is in stdin only); `AgentNodeCleartextTokenWarningTests` (predicate, a warning on `connect` without the token, no warning on loopback).
- `cargo test -p agent_mesh`: `client::tests::test_cleartext_token_warning_predicate`.
- The `.ps1` is not executed on macOS; Windows CI covers the text checks.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| SEC-15 | `-NoShell` for the PowerShell launcher, token on stdin for `adb`, `ws://` token warning in both nodes | `packaging/agent_mesh/windows/run_agent_node.ps1`, `packaging/agent_mesh/android/run_agent_node_adb.sh`, `crates/agent_mesh/src/client.rs`, `scripts/agent_node.py`, `tests/test_launcher_scripts.py` | The tests above pass | Implemented |

## Status log

- 2026-11-16: Implemented (`SEC-15`).
