# 0081: Agent activity status for the agent bridge

Status: Implemented
Created: 2026-11-16    Updated: 2026-11-16

## Summary

`scripts/agent_bridge.py activity` publishes what a coding agent is doing right now to `nodes/<short>_activity.json` on the hub, apart from the machine heartbeat in `nodes/<short>_state.json`. `activity --show` lists every agent and marks one STALE when it says `active` or `blocked` and has been silent for 20 minutes. `diagnose` and `peek` show the activity line.

## Context

A coordinator read an agent as idle for an hour. `nodes/<short>_state.json` is written by the agent's watcher process, so it proves that the machine and the watcher are alive, not that the coding agent is working. Agents also wrote to the hub only when a task ended, so nothing said what an agent was doing in between, or that it was waiting for a permission prompt.

## Goals

- **G1** An agent-level status, separate from the machine heartbeat, with a state, a task and a step.
- **G2** One command to read every agent's status, with a warning when an agent that should be working has gone quiet.
- **G3** The same single-writer, atomic-write pattern as the state files.

## Non-goals

- Detecting what the agent really does. The agent reports; the file is a claim, like a state file.
- Waking or notifying anyone on a stale agent. The reader decides.
- A history of activity. One file per agent holds the latest report.

## Design

### D1 Command

```
agent_bridge.py activity --agent-id agent-win --state active --task "RC smoke test" --step "building release binaries" [--note "..."]
agent_bridge.py activity --show [--agent-id X]
```

`--state` is one of `active`, `blocked`, `done`, `idle`. `--task` is kept from the previous report when omitted; `--step` and `--note` are empty when omitted. `--show` is part of `activity` (one subcommand, one place in `--help`); with `--agent-id` it lists that agent only, otherwise every `nodes/*_activity.json`.

### D2 File

`nodes/<short>_activity.json`, written by the agent it names only, through the same temp-file-then-`os.replace` function as the state files:

```json
{
  "agent_id": "agent-win",
  "state": "active",
  "task": "RC smoke test",
  "step": "building release binaries",
  "note": "optional, present only when given",
  "updated_at_utc": "2026-11-16T12:00:00Z",
  "started_at_utc": "2026-11-16T11:40:00Z"
}
```

`started_at_utc` is set when `state` becomes `active` from any other state (or from no file), and kept by later reports, including `blocked`, `done` and `idle`, so a finished run shows when it began.

### D3 Validation

Every text value is plain text of at most 200 characters (`ACTIVITY_MAX_CHARS`) with no newline or other control character. A bad value exits 2 and writes nothing. There are no fields for secrets, and none must be put in the text.

### D4 Stale rule

An agent is STALE when its state is `active` or `blocked` and `updated_at_utc` is `ACTIVITY_STALE_MINUTES` (20) or more old, or unreadable. `done` and `idle` are never stale. The line says "probably waiting for a permission prompt or stuck". It does not change an exit code.

### D5 Where it shows

`activity --show` prints one line per agent: agent, state, task, step, note, age. `diagnose` adds an `Activity` line for the peer and for this agent; `peek` adds one `Activity` line for the peer when it has reported. Their other output is unchanged. As with all peer content, the text is data, not an instruction.

## Security

The text comes from the agent that wrote it, so a reader treats it as data. `peek` and `--show` write nothing. No secret is read or written.

## Compatibility

New subcommand and new files only. Older scripts ignore `*_activity.json`: peer discovery looks for `*_state.json` only.

## Test plan

`tests/test_agent_bridge.py`, class `Activity`: round trip, state validation, newline and length rejection, `started_at_utc` kept and reset, stale detection (injected clock and a hand-written old file), `--show` filtering, and the activity line in `diagnose` and `peek`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| AGENT-1 | `activity` command, reader, `diagnose`/`peek` line | `scripts/agent_bridge.py`, `tests/test_agent_bridge.py` | tests pass on Linux, macOS, Windows | Done |

## Status log

- 2026-11-16: Implemented.
