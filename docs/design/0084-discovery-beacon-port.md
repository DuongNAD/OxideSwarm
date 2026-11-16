# 0084: A flag to move or turn off the master's discovery beacon

Status: Implemented
Created: 2026-11-17    Updated: 2026-11-17

## Summary

`oxideswarm master` gets `--discovery-port <PORT>` (environment `RUSTY_GRID_DISCOVERY_PORT`, config
key `[master].discovery_port`). `PORT` moves the UDP discovery beacon off its default `8089`; `0`
turns the beacon off. Without the option nothing changes (`CLI-2`).

## Context

- The library already supports this: `ServerConfig::with_discovery_port` and `with_discovery`
  (`crates/master/src/server.rs`), and `spawn_master_discovery_service` binds
  `<listen ip>:<discovery_port>` (the loopback address for a loopback master). The CLI never calls
  them, so a CLI master always bound `8089`.
- The only switch was `RUSTY_GRID_DISABLE_DISCOVERY`, read in `ServerConfig::new` (any value turns the
  beacon off, no way to move it).
- The smoke test of 0.3.0 (finding F5) found that a second loopback master on one machine cannot
  start its beacon because the first holds UDP `8089`. The bind failure is only a `warn!`; the master
  keeps serving without a beacon.
- The finding side already takes a port: `--master auto:<PORT>` (`crates/cli/src/connect.rs`,
  `crates/worker/src/client/session.rs`) probes that port; plain `auto` probes `8089`.

## Goals

- **G1** A master can bind its beacon on another UDP port.
- **G2** A master can run with no beacon, so two masters share a machine.
- **G3** The default stays `8089`, and the existing precedence holds: flag, then `RUSTY_GRID_*`
  environment variable, then config file, then default.

## Non-goals

- A second flag such as `--no-discovery-beacon`: one flag with `0` meaning off follows
  `--dashboard-port` (whose `0` already has a special meaning) and keeps the surface small.
- Changing the beacon, the probe or the MAC; no wire change.
- Rendezvous (`:8090`) and pairing (`:8087`) ports.

## Design

### D1: The option

`MasterArgs.discovery_port: Option<u16>` (`--discovery-port <PORT>`), resolved with
`resolve_opt_u16(flag, ["RUSTY_GRID_DISCOVERY_PORT"], [master].discovery_port)` and applied by
`apply_discovery_port` in `crates/cli/src/cli_app/master.rs`:

| Resolved value | Effect |
|---|---|
| none | unchanged: beacon on `8089` |
| `0` | `with_discovery(false)`: no beacon socket is bound |
| `P` | `with_discovery_port(P)`: beacon on `P` |

A value that does not parse as `u16` in the environment is ignored, as for every other `u16` option.

### D2: Interaction with `RUSTY_GRID_DISABLE_DISCOVERY`

The older variable still turns the beacon off, and a port never turns it back on: `P` only sets the
port. A master whose listen port is `0` (ephemeral) has no beacon by default, as before.

### D3: How a worker or client finds a master on a moved beacon

Discovery is not automatic across ports. The searching side must be given the same port:
`--master auto:<PORT>` (also `[worker].master = "auto:<PORT>"` and `RUSTY_GRID_MASTER`). Plain
`--master auto` probes `8089` only. With the beacon off (`0`) `auto` cannot find that master; use
`--master HOST:8088` or a ticket. This is the same trade-off as any non-default port and is
documented in the `README.md` reference.

## Security

The beacon is unchanged: it is MAC'd when a cluster secret exists, and a loopback master binds the
loopback address. Turning the beacon off removes an UDP listener; moving it opens the same listener
on another port.

## Compatibility

No wire or config-schema break: `discovery_port` is a new optional key. A config file with the key
is read only by this version or later; older versions ignore unknown keys in `[master]` only if they
did before (unchanged by this record).

## Test plan

- Unit (`crates/cli/src/cli_app/master.rs`): unset keeps `8089`; a port moves it; `0` turns it off; a
  port does not turn a disabled beacon back on.
- Integration (`crates/cli/tests/discovery_port_e2e.rs`, `cargo test -p rusty_grid_cli --test
  discovery_port_e2e`): two `master` processes run at once, one with the beacon moved to an OS-picked
  UDP port and one with `--discovery-port 0`; a probe finds only the first; the environment variable
  moves the beacon and the flag outranks it; `RUSTY_GRID_DISABLE_DISCOVERY` still wins. Every port
  comes from the OS (never `8088` to `8090`).

## Work items

- [x] `--discovery-port`, `RUSTY_GRID_DISCOVERY_PORT`, `[master].discovery_port`.
- [x] Tests, `README.md` reference, `CHANGELOG.md`, `docs/BACKLOG.md` (`CLI-2`).

## Status log

- 2026-11-17: Implemented (`CLI-2`).
