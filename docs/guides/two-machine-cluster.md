# Run a macOS machine and a Windows machine as one cluster

## Goal

A Mac (master and GPU worker) and a Windows PC (CPU worker) form one OxideSwarm cluster, and
`oxideswarm doctor` passes every check. The same steps work with the roles swapped, or with Linux
on either side.

## Prerequisites

- On both machines: the Rust toolchain (1.75 or later) and a checkout of the **same commit** of this
  repository. Each machine builds its own binary; nothing is cross-compiled.
- On Windows: [Git for Windows](https://git-scm.com/download/win). Shell tasks then run under its
  `sh`, as they do on macOS (without it they fall back to `cmd.exe`).
- A network path between the machines (step 1).
- To copy the secret with `scp`: Remote Login enabled on the Mac (System Settings > General >
  Sharing > Remote Login). A USB stick works as well.

## Steps

### 1. Connect the machines

| Link | Use it when | Setup |
|---|---|---|
| Same Wi-Fi or router | default | Nothing. It fails when the network keeps clients apart (guest Wi-Fi, many offices and cafés). |
| Ethernet cable | lower, steadier latency | The Mac needs a USB-C to Ethernet adapter. Plug both machines into the router, or cable them directly and give each a static address on a subnet the Wi-Fi does not use, for example `10.99.0.1` (Mac) and `10.99.0.2` (Windows), mask `255.255.255.0`, no router or gateway, so Internet traffic stays on Wi-Fi. |
| Different networks | the machines are in different places | P2P through the master's ticket (step 3, second command). |

A plain USB-C to USB-C cable between two laptops gives no network link: both ends are USB hosts,
and IP over a cable needs Thunderbolt or USB4 on both sides.

### 2. Build

On the Mac (Terminal, repository root):

```bash
cargo build --release --bin oxideswarm --bin rusty-grid --features gpu-wgpu
```

On Windows (PowerShell, repository root):

```powershell
cargo build --release --bin oxideswarm --bin rusty-grid
```

Build both binaries: the launchers use `oxideswarm` first, and an older copy without the GPU engine
would win. Add `--features gpu-wgpu` on Windows too if its GPU should take GPU tasks.

### 3. Start the master on the Mac

On the Mac:

```bash
./switch_role.sh master
ipconfig getifaddr en0
```

The master listens on `0.0.0.0:8088`, keeps the dashboard on the Mac, starts a local worker, and on
its first start creates `~/.oxideswarm/cluster.secret`. The second command prints the Mac's Wi-Fi
address, `<MAC_IP>` below; on a direct cable use the static address from step 1. If macOS asks
whether `rusty-grid` may accept incoming connections, allow it.

Machines on different networks: start the master with `./switch_role.sh master --p2p` instead, and
connect the Windows machine with `connect_remote.cmd` and the ticket from
`~/.oxideswarm/master_ticket.txt` ([README, section D](../../README.md#d-remote-wan-connection-cross-network--internet--requires-ticket-and-secret));
then continue with step 6.

### 4. Copy the cluster secret to Windows

Copy `~/.oxideswarm/cluster.secret` from the Mac to `%USERPROFILE%\.oxideswarm\cluster.secret` on
Windows, by USB stick or, on Windows (PowerShell):

```powershell
New-Item -ItemType Directory -Force "$env:USERPROFILE\.oxideswarm" | Out-Null
scp <USER>@<MAC_IP>:.oxideswarm/cluster.secret "$env:USERPROFILE\.oxideswarm\cluster.secret"
```

Never send the secret through chat, e-mail or a cloud-synced folder ([SECURITY.md](../SECURITY.md)).

### 5. Start the worker on Windows

On Windows (PowerShell, repository root):

```powershell
.\windows\switch_role.cmd worker <MAC_IP>:8088 --no-gpu --tag laptop
```

- The worker connects out to the master, so Windows needs no inbound firewall rule.
- `--no-gpu` keeps GPU tasks on the Mac, so `submit --gpu` always means "run on the Mac". Leave it
  out to let a Windows GPU take GPU tasks as well.
- `--tag laptop` is optional: it lets `submit --require-tag laptop` select this machine.

### 6. Send work

On the Mac:

```bash
# Only on the Windows worker
./target/release/oxideswarm submit --os windows --wait --command cmd -- /C ver
# A local Python script, shipped with the task, run on the Mac
./target/release/oxideswarm submit --type shell --os macos --interpreter python3 --script-file job.py --wait
# Local input data as standard input
./target/release/oxideswarm submit --os macos --stdin-file data.txt --wait --command wc -- -l
# A 2048 x 2048 GEMM on the Mac's GPU, checked against the CPU
./target/release/oxideswarm submit --type gpu --matrix-dim 2048 --wait
# One text file split across both machines
./target/release/oxideswarm mapreduce --input big.txt --mapper word_count --reducer sum --chunks 8
```

A task runs wherever its program exists: without `--os` or `--worker`, use only commands both
machines have. On Windows the Python launcher is usually `python` or `py`, not `python3`.
`submit --wait` prints the worker that ran the task.

## Verify

On the Mac:

```bash
./target/release/oxideswarm doctor --expect-workers 2
```

Expected: both workers listed (`macos/aarch64` with the Mac's GPU, `windows/x86_64`), every check
`PASS`, exit code 0. The report looks like this (recorded on one Windows host running two workers,
so both say `windows/x86_64`):

```text
Workers (2 connected, 2 registered)
  NAME                 ID        OS/ARCH          CORES    RAM MB  GPU                          STATE      TAGS
  live-cpu             d483e4d4  windows/x86_64      20     48941  none                         connected
  live-gpu             a23a24e5  windows/x86_64      20     48941  NVIDIA GeForce RTX 5060 Ti   connected  desk

Checks
  PASS  C1 Connect                                 connected and authenticated in 1 ms
  PASS  C2 Workers                                 2 connected (at least 2 expected)
  PASS  C3 In-process probe   live-cpu             completed in 0 ms
  PASS  C4 Process probe      live-cpu             started a process and read its output in 17 ms
  PASS  C3 In-process probe   live-gpu             completed in 0 ms
  PASS  C4 Process probe      live-gpu             started a process and read its output in 17 ms
  PASS  C5 GPU probe          live-gpu             1024x1024 GEMM verified in 14 ms, 153.4 GFLOP/s on NVIDIA GeForce RTX 5060 Ti
  PASS  C6 Spread                                  8 tasks of 500 ms finished in 543 ms on 2 worker(s): live-cpu 4, live-gpu 4

Summary: 8 passed, 0 warnings, 0 failed, 0 skipped
```

The GFLOP/s figure comes from the task's reported execution time at a small matrix size: a rough
figure, not the GPU's peak (`--gpu-dim 2048` gives a better one). What each check proves:

| Check | Proves |
|---|---|
| C1 Connect | the master is reachable and accepts this machine's secret |
| C2 Workers | both workers are registered and connected |
| C3 In-process probe | each worker receives tasks pinned to it and reports results |
| C4 Process probe | each worker starts processes on its OS and returns their output |
| C5 GPU probe | each GPU worker multiplies matrices and the result passes the CPU check |
| C6 Spread | unpinned work is shared between the workers |

`doctor --json` prints the same report as JSON. A MacBook Air has no fan: long jobs slow down as it
warms up, and that is expected.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| C1 fails with `Authentication required` or `Authentication failed` (exit code 6) | This machine has no copy of the secret, or a different one | Repeat step 4, restart the worker |
| C1 fails: connection refused or timed out | Master not listening on the LAN, wrong port, macOS firewall, client isolation | Start the master with `switch_role.sh master`; use port 8088, never 8080; allow incoming connections on the Mac; try Ethernet |
| C2 fails: 1 connected, 2 expected | The Windows worker did not register | On Windows: `.\windows\switch_role.cmd status`, and the log in `%USERPROFILE%\.oxideswarm\run` |
| C3 fails: "ran on worker ... instead" | The master is older than the client | Build the same commit everywhere; restart the master first |
| C5 fails: `GPU unavailable` | Binary built without `gpu-wgpu`, or a driver problem | Rebuild with `--features gpu-wgpu`; the worker log must show `GPU compute enabled on ...` |
| C6 warns: all tasks ran on one worker | The other worker is busy, above the master's CPU limit (85 % by default), or kept free for GPU work | Run again when idle; on a busy laptop this is expected |
| `--master auto` finds nothing | UDP broadcast does not cross the link (client isolation, direct cable, VPN) | Use `<MAC_IP>:8088` |
| `submit` warns that no connected worker matches | The constraint names an OS, tag or worker that is not connected | The task waits in the queue; start that worker or fix the constraint |
