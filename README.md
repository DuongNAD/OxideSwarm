<div align="center">

<img src="assets/logo.svg" alt="OxideSwarm" width="420">

**One Rust binary that turns the machines you own into a compute grid for builds, tests and coding agents.**

[![Release](https://img.shields.io/github/v/release/DuongNAD/OxideSwarm?style=flat-square&label=release)](https://github.com/DuongNAD/OxideSwarm/releases)
[![CI](https://img.shields.io/github/actions/workflow/status/DuongNAD/OxideSwarm/ci.yml?branch=master&style=flat-square&label=CI)](https://github.com/DuongNAD/OxideSwarm/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg?style=flat-square)](#license)
[![Platforms](https://img.shields.io/badge/platforms-macOS%20%7C%20Windows%20%7C%20Linux-lightgrey.svg?style=flat-square)](docs/reference.md#platform-support)

</div>

## What it does

- **Runs builds and tests on your other machines.** `oxideswarm submit --workspace <NAME> -- cargo test`
  syncs the project to a worker and runs it there. The next run sends only the files that changed,
  and the worker reuses its `target/` directory.
- **Serves coding agents.** `oxideswarm mcp` is a Model Context Protocol server with the tools
  `status`, `workers`, `submit`, `task_result`, `cancel`, `doctor` and `workspaces`.
- **Moves files in and out.** `submit --input` sends files and directories, and `--output` brings
  back the files its patterns select. A file the other side already holds is not sent again.
- **Works across networks.** On a LAN, workers connect to the master over TCP. With `master --p2p`,
  machines on other networks connect through NAT or a relay (iroh QUIC), with no router ports to
  open. Traffic beyond loopback is authenticated and encrypted.
- **Isolates tasks.** With `worker --isolation fs`, a task can write only its own sandbox and cannot
  read the cluster secret, on macOS, Linux and Windows.

## Install

Download the archive for your system and `SHA256SUMS` from the
[Releases page](https://github.com/DuongNAD/OxideSwarm/releases) (the latest is `v0.4.4`).
Check both, unpack, and put the folder on your `PATH`:

```bash
# macOS / Linux (on Linux: sha256sum -c SHA256SUMS --ignore-missing)
shasum -a 256 -c SHA256SUMS --ignore-missing
gh attestation verify oxideswarm-0.4.4-aarch64-apple-darwin.tar.gz --repo DuongNAD/OxideSwarm
tar -xzf oxideswarm-0.4.4-aarch64-apple-darwin.tar.gz
```

```powershell
# Windows: compare the hash with its line in SHA256SUMS
(Get-FileHash .\oxideswarm-0.4.4-x86_64-pc-windows-msvc.zip -Algorithm SHA256).Hash
gh attestation verify .\oxideswarm-0.4.4-x86_64-pc-windows-msvc.zip --repo DuongNAD/OxideSwarm
Expand-Archive .\oxideswarm-0.4.4-x86_64-pc-windows-msvc.zip -DestinationPath C:\oxideswarm
```

The binaries are not signed. On macOS, remove the quarantine flag before the first start:
`xattr -dr com.apple.quarantine oxideswarm-0.4.4-aarch64-apple-darwin`. Targets, the `-gpu` archives,
what the attestation proves and the service installers: [docs/reference.md](docs/reference.md#install).

Build from source (Rust 1.91 or newer; add `--features gpu-wgpu` for a worker whose GPU should do work):

```bash
cargo build --release --bin oxideswarm
```

## Quick start

On one machine, in three terminals. A master on loopback needs no secret.

```bash
oxideswarm master                         # cluster port 127.0.0.1:8088, dashboard http://127.0.0.1:8080
oxideswarm worker --workspaces            # connects to 127.0.0.1:8088
oxideswarm submit --wait -- hostname      # runs on the worker and prints its output
```

From the root of a Rust project, run its tests on a worker:

```bash
oxideswarm submit --workspace myproject --require-tool cargo --wait -- cargo test
```

Run it again: only changed files travel. The walk-through for two machines, with the expected output,
is [docs/guides/offload-cargo-test.md](docs/guides/offload-cargo-test.md).

## Add your other machines

Start the master with `--listen 0.0.0.0:8088`. The first start creates `~/.oxideswarm/cluster.secret`.
Then add each machine in one of three ways:

- **Same LAN:** run `oxideswarm invite --code` on the master and `oxideswarm join --code <N>-<word>-<word>`
  on the new machine, then compare the 6-digit check on both ([guide](docs/guides/add-a-machine.md)).
- **Another network:** start the master with `--p2p` and run `oxideswarm invite` on it. Send the
  `oxs1-invite:...` string by chat, paste it into `oxideswarm join` on the new machine, and approve it
  on the master with `oxideswarm members approve <PENDING_ID> --code <NNNNNN>`
  ([design 0051](docs/design/0051-machine-identity-and-invites.md)).
- **Without a terminal:** `oxideswarm usb create <DIR> --machines <N>` writes an encrypted USB setup kit.
  One double-click on macOS or Windows installs, joins and starts a worker ([packaging/usb/README.md](packaging/usb/README.md)).

After a join, start the worker with `oxideswarm worker`, and check the whole cluster with
`oxideswarm doctor`. Other ways to connect (one join token, launchers, Windows service, Termux) are in
[docs/reference.md](docs/reference.md#set-up-a-cluster).

## For coding agents

Add `oxideswarm mcp` as a stdio MCP server in Claude Code, Codex or Cursor:
[docs/guides/mcp-client-setup.md](docs/guides/mcp-client-setup.md).

## Status and limits

- Tested: CI runs the full test suite on macOS, Windows and Linux for every pull request, and live
  runs between a Mac and a Windows PC are recorded. Intel Macs get a release build but no tests.
- The release binaries are not signed or notarized.
- Delivery is at least once: after a worker is lost, a task can run twice. One master, with no
  automatic failover.
- Anyone who holds the cluster secret can run commands on every worker. No independent security
  review has been published yet.
- Android: the app does not build yet (AND-1), and the Termux worker has not run on a device.
- No language model serving yet: release `0.5.0` is Design only.
- Mixed versions: upgrade the master first; it accepts peers one protocol version back.

The full list is in [docs/reference.md](docs/reference.md#status-and-limits), the plan in
[docs/ROADMAP.md](docs/ROADMAP.md).

## Documentation

- [docs/README.md](docs/README.md): index of every document and the writing standard
- [docs/reference.md](docs/reference.md): CLI flags, ports, environment variables, platform support,
  troubleshooting, measured results, development and tests
- [docs/guides/](docs/guides/): step-by-step guides (two machines, add a machine, MCP, GPU work, relay)
- [DEPLOYMENT.md](DEPLOYMENT.md): Agent Mesh deployment and upgrading a running grid
- [docs/SECURITY.md](docs/SECURITY.md): threat model, cluster secret, ports, who runs tasks
- [CHANGELOG.md](CHANGELOG.md) and [docs/ROADMAP.md](docs/ROADMAP.md)

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) first. A change to behaviour, a CLI flag, an environment
variable or the wire format comes with a design record in [docs/design/](docs/design/) and an update
of [docs/reference.md](docs/reference.md), in the same pull request.

## License

OxideSwarm is dual-licensed under:
* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
* MIT License ([LICENSE-MIT](LICENSE-MIT))

You may choose either license at your option.
