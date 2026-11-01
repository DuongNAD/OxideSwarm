# 0007: Message trigger for the agent bridge

Status: Implemented
Created: 2026-11-01    Updated: 2026-11-01

## Summary

Two coding agents talk through `OxideSwarm_Agent_Hub` on Google Drive. Until now an agent saw a
message only when it remembered to look. `scripts/agent_bridge.py watch` now delivers each message
once and survives a Drive outage. A Claude Code session wakes by itself when the peer writes, and
other agents can use a background watcher, a desktop notification or a command.

## Context

`watch --once-on-message` already existed, and the hub rules told each agent to restart it after
every turn. It had these problems:

- It kept no delivery state, so restarting it fired again for every unacknowledged message, and it
  missed incident updates made while it was stopped.
- An error from Drive, or from printing Vietnamese to a Windows pipe, ended the process.
- It rewrote `nodes/<self>_state.json` on every poll, every 10 s, through the Drive sync.
- `--auto-pull` ran `git pull --rebase origin master` on whatever branch was checked out.
- An agent that forgot to restart it heard nothing, and neither did the owner.

## Goals

- G1: Each message reaches one consumer on a machine exactly once, across restarts.
- G2: A running Claude Code session wakes without any action from the owner.
- G3: Other agents and the owner have a way to be told.
- G4: The watcher survives Drive outages and non-UTF-8 consoles.
- G5: A checkout without a hub is untouched by the hooks.

## Non-goals

- A push transport. Latency stays the Drive sync plus the poll interval.
- Waking a session that is not running. `peek` reports the mail when the next session starts.
- Starting an agent unattended. An owner can do that with `--on-event`, at their own risk.

## Design

### D1: Delivery state

`claim_event` creates one file per delivered event under
`~/.oxideswarm/agent_bridge/claims/<consumer>/`, or under `OXIDESWARM_HOME` when it is set. `O_EXCL`
makes the claim atomic, so two watchers of one consumer deliver a message once. Claims stay on the
machine, because Drive sync is neither atomic nor immediate. Claims older than 30 days are removed.

A message is keyed by its file name. An incident is keyed by name and modification time, so an
update fires again. A first run claims the existing incidents without delivering them. It delivers
unread messages. Incidents reported by this machine do not fire.

### D2: Claude Code wake

`.claude/settings.json` holds two hooks, through `scripts/agent_bridge_hook.sh`, which picks a usable
Python:

- `SessionStart` runs `peek`. It prints a few lines when mail is unread, and nothing otherwise.
- `Stop` runs `watch --hook` with `asyncRewake`. On an event it writes a notice to stderr and exits
  with code 2, which wakes the model. The end of that turn runs the hook again.

`SessionStart` is not used for the wait, because `asyncRewake` does not wake the model from that
event. It was tested on Claude Code 2.1.308.

The `Stop` hook fires after every turn, so `acquire_watch_lock` keeps one watcher per session. It
uses an OS file lock, which ends with the process. Later hooks exit at once.

### D3: Other agents and the owner

- `watch --once-on-message` ends on an event, for tools that react when a background command exits.
- `watch --notify` shows a desktop notification.
- `watch --on-event "<COMMAND>"` runs a command with `OXIDESWARM_EVENT_KIND`, `_FILE`, `_TITLE`,
  `_PRIORITY` and `_FROM` in its environment.
- `--consumer` separates delivery groups, so a notifier does not take a message from an agent.

### D4: Robustness

- The main loop treats `OSError` as an outage, reports it once and keeps polling.
- `main` sets stdout and stderr to UTF-8 with replacement.
- `--heartbeat-every` (default 60 s) separates writes to the hub from polling.
- `--auto-pull` runs `git pull --ff-only` on the checked-out branch.
- `send` reports how long ago the peer published state, because a stale peer has no watcher.
- `read_hook_session_id` polls stdin and leaves no read pending. An earlier version read in a
  background thread, and it hung on Windows, past `--max-wait`, when stdin stayed open.
- `get_git_info` runs git without stdin and with a limit of 20 s, so a stuck git cannot outlast
  `--max-wait`.
- `watch --hook` checks the age of `nodes/<self>_state.json` from `updated_at_utc` alone. It writes
  the full state file only when the file is missing (leaving an existing unreadable or truncated
  state file untouched); when `updated_at_utc` is older than 150 s (or in the future) it updates
  only `updated_at_utc` and keeps the existing `git` block and all other fields, so a hook watcher
  in another worktree never overwrites the active agent's branch. On a machine running only Claude
  Code hooks (with no full `heartbeat`, `diagnose`, or non-hook `watch`), `updated_at_utc` shows
  liveness while `git`, `lan_ip`, and `security_and_config` (the secret fingerprint) stay frozen at
   the values from the last full write.

## Compatibility

The message format is unchanged, so an older peer still reads every message. `watch` keeps its
existing flags. The first run with this version delivers unread messages once more, because no
claims exist yet. `scripts/agent_bridge.py` is mirrored in the hub, so that copy needs an update.

## Security

- The hub is writable by anyone with access to the Drive account. Its content is not authenticated.
- The hook notice carries the sender, the title and the priority, not the body. The title is limited
  to `MAX_TITLE_CHARS` and stripped of control characters. The notice tells the agent that the
  content comes from a peer, is data, and needs the owner's approval for anything outside the task.
- `desktop_notify` and `run_event_command` pass peer text through arguments or variables, never
  through a script or a shell string. An `--on-event` command must quote those variables itself.
- `peek` and `watch --hook` create nothing on a machine without a hub, because `.claude/settings.json`
  applies to every clone of this public repository.

## Test plan

- `tests/test_agent_bridge.py` runs the real script against a temporary hub on Linux, macOS and
  Windows in CI: delivery, restart, concurrent watchers, consumers, incidents, outage, code page,
  empty files, heartbeat rate, `--on-event` injection, hook mode, a stdin left open, a stuck git,
  inert behaviour without a hub.
- Targeted mutations of the script were caught by those tests.
- A headless Claude Code 2.1.308 session on macOS was woken twice while idle, by two files written to
  a temporary hub. The watcher had rearmed itself after the first wake.
- Not tested: a real exchange through Google Drive between two machines, the Windows toast, and the
  hooks on Windows.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Delivery state and watch loop | `scripts/agent_bridge.py` | D1 and D4 pass in `tests/test_agent_bridge.py` | Implemented |
| W2 | Claude Code hooks | `.claude/settings.json`, `scripts/agent_bridge_hook.sh` | An idle session wakes on a new message | Implemented |
| W3 | Notification and command | `scripts/agent_bridge.py` | `--on-event` test passes; `--notify` runs on macOS | Partial |
| W4 | CI | `.github/workflows/ci.yml` | The bridge tests run on all three systems | Implemented |
| W5 | Two-machine run | none | A message from each machine wakes the other through Drive | Planned |

## Status log

- 2026-11-01: W1, W2 and W4 implemented. W3 is Partial because the Windows toast has not run. W5
  waits for the two machines.
- 2026-11-01: `watch --hook` no longer leaves a read pending on stdin, and git calls have a time
  limit, after `agent bridge (windows-latest)` timed out on pull request 7.
- 2026-11-01: `watch --hook` now publishes `nodes/<self>_state.json` only when the state file is
  missing or older than 150 s (`BRIDGE-2`), preventing a hook watcher in another worktree from
  overwriting the active agent watcher's state.
- 2026-11-01: `watch --hook` now measures state age from `updated_at_utc` alone and updates only
  `updated_at_utc` (preserving the `git` block) when refreshing an existing state file (`BRIDGE-3`).
