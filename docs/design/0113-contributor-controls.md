# 0113: Contributor controls: pause, limits the worker enforces, and a local control channel (NET-9)

Status: Draft
Created: 2026-11-26    Updated: 2026-11-26

## Summary

A person who lends a machine sets limits on it, and the worker enforces them whatever the master asks:
a pause by hand, a CPU threshold of its own, hours, no new task on battery or on a metered network, and
no GPU task. Paused or limited, the worker stays connected; running tasks go back to the master within
5 s without a retry charged, and the master stops offering work to that worker for a while instead of
bouncing tasks off it. A same-user local control channel (`oxideswarm worker pause|resume|status|limits`)
serves the command line and the tray app of [design 0112](0112-tray-app-for-contributors.md). No wire
change: release 0.4.7. Item NET-9 ([design 0094](0094-road-to-a-shared-research-grid.md)), split into
NET-9a (pause, hand-back, control channel, master hold-back) and NET-9b (the limits).

## Context

- **Limits today.** `--max-concurrency`, `--task-priority below-normal` (task processes only),
  `--isolation`, and the master's `--max-host-cpu-pct` (85 by default, `crates/master/src/scheduler.rs`).
  A laptop reports its battery and the master lowers its concurrency by battery tier
  (`calculate_effective_concurrency`, `crates/core/src/capabilities.rs`); a phone refuses tasks on a
  critically low battery (`WorkerClient::mobile_telemetry`, `crates/worker/src/client.rs`). The worker
  reads no network cost and has no schedule. The only local stop is ending the process.
- **Refusals exist and are cheap.** A worker that cannot run a task answers with a result the master
  recognises (`is_worker_rejection`, `WORKER_REJECTION_MARKERS`, `crates/master/src/queue.rs`): exit 1,
  no output, zero run time and text containing `rejected by worker`. The task goes to another worker and
  no retry is charged ([design 0090](0090-sandbox-base-refusal-hand-back.md)).
- **The trap.** The same code notes that a refusing worker "looks permanently idle, which is how one
  stale-snapshot phone ends up absorbing a large share of a cluster's work": the scheduler keeps choosing
  it. A pause built on refusals alone would make every paused machine bounce tasks.
- **Why now.** The friend trial of 2026-11-26 ran a worker in the background on someone else's laptop;
  the owner asked for a way to keep it from getting in the way (design 0112), and GPU tasks do not yield
  to a game.

## Goals

- G1. Pause and resume by hand, from the command line or the tray, acting within 5 s; a paused worker
  stays connected and listed.
- G2. Limits set by the contributor in their own configuration and enforced by the worker: a CPU
  threshold, hours (a weekly schedule and a daily cap of task hours), no new task on battery, no new task
  on a metered network, no GPU task.
- G3. A held-back task runs elsewhere with no retry charged; the master does not keep offering work to a
  paused or limited worker.
- G4. A log line and `status` name the control that holds work back.
- G5. No wire change; a 0.4.x master and worker of either side of the change keep working together.

## Non-goals

- Policies across several worker processes of one machine, per-campaign opt-in and task classes (NET-4).
- A rule for an idle screen (GREEN-1 of design 0056); the full-screen pause is TRAY-4 of design 0112.
- An explicit availability message in the protocol; it may come with protocol 11 (0.5.0) if the
  hold-back of D3 proves too coarse.

## Design

### D1 Settings (NET-9b)

Flags, `RUSTY_GRID_*` variables and `[worker]` keys of the contributor's configuration, the usual
precedence (`crates/cli/src/config.rs`):

| Flag | Meaning |
|---|---|
| `--local-max-cpu-pct <P>` | No new task while host CPU use, averaged over 30 s, is above P (the worker's own threshold, beside the master's) |
| `--hours "<spec>"` | Weekly schedule in local time, for example `"mon-fri 18:00-08:00; sat-sun 00:00-24:00"`; outside it, no new task |
| `--max-task-hours-per-day <H>` | Daily cap of summed task run time, reset at local midnight |
| `--no-tasks-on-battery` | No new task while on battery |
| `--no-tasks-on-metered` | No new task while the operating system reports a metered connection |
| `--no-gpu-tasks` | Refuse GPU tasks (CPU tasks still run) |

Metered detection: Windows `INetworkCostManager` (`NLM_CONNECTION_COST_*`), macOS `NWPath.isExpensive` /
`isConstrained`, Linux NetworkManager's `Metered` property; where none answers, the worker logs once at
start that it cannot tell and treats the network as unmetered.

### D2 Pause, hand-back and the control channel (NET-9a)

1. **States.** `running`, `paused` (by hand, with an optional end time), `held` (a limit applies, with its
   reason). The worker re-evaluates limits every 10 s and on events (power, network change).
2. **New tasks.** In `paused` or `held`, every assignment is refused at once with
   `Task rejected by worker: paused: <reason>` (or `...: no gpu tasks` for a GPU task under
   `--no-gpu-tasks`), exit 1, zero run time: an older master already hands it back without a retry.
3. **Running tasks.** A hand pause ends running tasks within 5 s (process tree kill,
   `crates/worker/src/process_tree.rs`) and reports each with the same refusal text, so they rerun
   elsewhere uncharged. A limit (battery, hours, metered, CPU) only stops new tasks and lets running ones
   finish, unless the contributor sets `--hold-ends-tasks`.
4. **Control channel.** The worker serves a local channel only the same user can open: a named pipe
   `\\.\pipe\oxideswarm-<user>-<worker-name>` with a DACL for the user on Windows, a Unix socket
   `<state dir>/control.sock` with mode 600 elsewhere. Requests are one JSON line each: `status`,
   `pause [until]`, `resume`, `limits get`, `limits set <key> <value>` (written back to the configuration
   file). `oxideswarm worker pause|resume|status|limits` are thin clients; the tray app (design 0112) uses
   the same channel.

### D3 Master hold-back (NET-9a, master side)

1. A refusal whose text holds `rejected by worker: paused` puts that worker session in a hold-back set
   for 60 s (doubling to at most 10 minutes while refusals continue; cleared by a heartbeat that shows a
   finished task or by a new session). The scheduler skips held-back workers in `view_fits`; their tasks
   go to other workers or wait in the queue.
2. `--no-gpu-tasks` refusals hold back only GPU tasks on that worker.
3. `workers` and `ClusterStatus` show `state:paused` for a held-back worker (a master-added tag, so no
   wire change); `doctor` reports a paused worker as a warning, not a failure.
4. An older master without D3 still hands tasks back; it may offer them to the paused worker again, which
   costs a round trip per offer, not a failed task. Release notes say to upgrade the master first.

## Compatibility

- No wire change: refusals are `TaskResult`s, the hold-back and the `state:paused` tag are master-side,
  the control channel is local. Protocol stays 10.
- `state:` is already a master-added reserved prefix; `paused` is a new value.
- Defaults keep today's behaviour: no limit is on unless set.

## Security

- The control channel is local and same-user only; it never carries the cluster secret, and a request
  from another account is refused by the operating system before the worker reads it.
- `limits set` writes only the keys of D1 into the user's own configuration file.
- A refusal text never includes the contributor's schedule or location; `status` shows it only locally.

## Test plan

- T1. Unit: hours parser (overnight ranges, days, daylight saving change), daily cap reset, the CPU
  average, metered detection stubs per system, refusal texts recognised by `is_worker_rejection`.
- T2. Integration on loopback: pause with two tasks running, both rerun on another worker within 5 s
  with zero retries charged; a paused worker gets no new assignment during its hold-back; resume and the
  next task lands on it; `--no-gpu-tasks` refuses only GPU tasks.
- T3. Control channel: `worker pause|resume|status|limits` round trips; another user cannot connect
  (Windows and Unix).
- T4. Live: a Windows laptop on battery with `--no-tasks-on-battery` holds work back and takes it again on
  AC; a metered Wi-Fi network holds work back with `--no-tasks-on-metered`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| NET-9a | Pause/resume, hand-back of running tasks, local control channel, master hold-back and `state:paused` | `crates/worker/src/{client.rs,control.rs (new)}`, `crates/master/src/{queue.rs,scheduler.rs}`, `crates/cli/src/cli_app/worker.rs` | T2, T3 pass | Planned |
| NET-9b | Limits: CPU threshold, hours, daily cap, battery, metered network, no GPU tasks | `crates/worker/src/limits.rs` (new), `crates/cli/src/config.rs`, `docs/reference.md` | T1 and T4 pass | Planned |

## Status log

- 2026-11-26: Draft, the design record that NET-9 asks for, written for release 0.4.7 (owner's plan of
  the 0.4.x milestones the same day: 0.4.7 = the developer toolset of design 0110 and contributor
  controls). It is the base of the tray app of design 0112.
