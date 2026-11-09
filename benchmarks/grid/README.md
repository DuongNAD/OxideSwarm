# Grid benchmarks (BENCH-1)

`bench_grid.py` measures the OxideSwarm grid on loopback with the binary you built. It uses only the
Python 3 standard library and runs on macOS, Linux and Windows (all three are recorded below).

```bash
cargo build --release --locked --bin oxideswarm
python3 benchmarks/grid/bench_grid.py --bin target/release/oxideswarm          # (a) and (b), about 10 minutes
python3 benchmarks/grid/bench_grid.py --bin target/release/oxideswarm --quick  # smoke run, under a minute
python3 benchmarks/grid/bench_grid.py --bin target/release/oxideswarm \
    --offload-master <HOST:PORT_OR_TICKET> --secret-file ~/.oxideswarm/cluster.secret  # (c) two-machine offload
python3 tests/test_bench_grid.py                                               # helper checks, no binary
```

Every run starts its own master and workers on `127.0.0.1` with ephemeral ports, gives each process a
private `HOME` and `USERPROFILE`, stops them through the process handles it holds (never by name) and
deletes its temporary directory. It never reads or writes the real `~/.oxideswarm`. It fails with the
reason when the master does not publish a port or a worker does not register. It writes one JSON record
to `results/<UTC date>-<os>-<arch>-<cpu>.json (no host or user name is recorded)` with the date, git commit (and whether the tree had other changes),
binary version and profile, CPU, cores, RAM, OS and kernel, network, power state, load averages before
and after, parameters, the median, minimum and maximum over the runs, and every raw run.

## What is measured

**(a) No-op task overhead, 1 to 4 workers.** The task is `true` (`cmd /c exit 0` on Windows). Each
worker runs on this machine with default settings and `RUSTY_GRID_NO_BATTERY=1`. For each worker count
a fresh cluster is started, warmed with 5 tasks, and then:

- *Latency:* 100 `oxideswarm submit --wait` processes one after the other. The figure is the client
  wall clock of that process, from spawn to exit. It includes starting the CLI, the authenticated
  connect, the submit, the master's scheduling, the worker's sandbox and process start, the result, and
  the CLI's follow-up worker-list request. It is not a master-side timestamp: the CLI does not expose one.
- *Throughput:* 300 such processes with 16 in flight, divided by the wall time of the whole batch.
- *Client floor:* the same two measurements with `oxideswarm status` instead of `submit`, which is a
  connect plus one request and no task. It shows how much of the time is the CLI and the connection
  rather than the grid. Subtract it as an estimate, not as an exact split.

Each is repeated in 3 fresh clusters per worker count; the table gives the median of the 3 runs and the
range. p99 of 100 samples is the second slowest sample, so it moves a lot.

**(b) First and repeated upload of one input tree.** A tree of 500 files (1 to 64 KiB of seeded random
bytes each, 10 directories, 16833795 bytes, seed 20261109) is submitted with `--input` and a no-op
command to one worker, three times in one cluster: first, the same tree again, and the tree with one file
changed by one byte. "Blobs sent" is the count the CLI prints (it uploads only blobs the master lacks,
design 0006). "Store growth" is the byte growth of the master's blob directory, which is the data that
crossed the connection for the upload (plus none for blobs it already held); the bytes on the wire itself
are not counted, so they include a small framing overhead on top. Wall time is the whole `submit --wait`
process: hashing, upload, the worker's fetch from the master, and the task.

## Recorded run

Record: [`results/2026-11-08-macos-arm64-apple-m5.json`](results/2026-11-08-macos-arm64-apple-m5.json). 2026-11-08 (UTC), Apple M5, 10 cores, 32 GiB, macOS
(Darwin 27.2.0), AC power, release build of `433ba81` plus this script. Loopback.

**The machine was not idle.** Other build and test agents were running: the load averages were 103, 52
and 50 before and 127, 100 and 73 after, on 10 cores. Treat every number as a pessimistic figure from a
loaded machine. The ranges are wide for that reason, and the p99 in particular includes scheduling noise
from other processes. A rerun on an idle machine is welcome and goes next to this record.

| Workers | Throughput, tasks/s (median, range) | p50 ms (median, range) | p99 ms (median, range) | Client floor: calls/s, p50 ms |
|---|---|---|---|---|
| 1 | 17.6 (17.4 to 37.0) | 26.2 (23.2 to 43.1) | 57.1 (32.7 to 821.3) | 300.2, 5.3 |
| 2 | 24.4 (23.8 to 32.6) | 29.7 (27.6 to 47.8) | 54.8 (53.3 to 107.9) | 271.5, 9.8 |
| 3 | 34.0 (10.5 to 52.4) | 33.3 (31.7 to 53.5) | 88.4 (70.4 to 141.6) | 279.4, 10.6 |
| 4 | 33.9 (19.9 to 35.0) | 31.0 (26.9 to 53.8) | 72.9 (50.2 to 288.7) | 287.7, 6.9 |

| Upload of the 500-file tree | Wall time, s (median, range) | Blobs sent | Store growth, bytes |
|---|---|---|---|
| First | 3.89 (2.10 to 5.23) | 501 of 501 | 16864821 |
| Same tree again | 0.126 (0.122 to 0.994) | 0 of 501 | 0 |
| One file changed | 0.142 (0.136 to 19.4) | 2 of 501 | 55102 |

The 501st blob is the tree manifest. Changing one file sends that file and the new manifest only.

Record: [`results/2026-11-09-windows-amd64-13th-gen-intel-core-i7-1355u.json`](results/2026-11-09-windows-amd64-13th-gen-intel-core-i7-1355u.json). 2026-11-09 (UTC), 13th Gen Intel(R) Core(TM) i7-1355U (10 cores, 12 threads), 16 GiB, Windows 11 Home (10.0.26300), AC power (`Win32_Battery` `BatteryStatus=2`, 100 %), release build of `4181a6d`. Loopback.

| Workers | Throughput, tasks/s (median, range) | p50 ms (median, range) | p99 ms (median, range) | Client floor: calls/s, p50 ms |
|---|---|---|---|---|
| 1 | 50.9 (48.9 to 52.4) | 69.0 (68.5 to 77.3) | 113.5 (101.1 to 116.8) | 146.7, 30.9 |
| 2 | 45.4 (38.4 to 50.1) | 73.7 (66.1 to 91.8) | 146.9 (128.7 to 170.7) | 138.6, 26.5 |
| 3 | 47.9 (42.9 to 50.4) | 88.3 (62.6 to 90.9) | 213.6 (106.5 to 237.5) | 145.1, 28.0 |
| 4 | 48.4 (45.8 to 52.1) | 88.7 (77.5 to 93.0) | 169.6 (136.4 to 273.0) | 170.2, 25.8 |

| Upload of the 500-file tree | Wall time, s (median, range) | Blobs sent | Store growth, bytes |
|---|---|---|---|
| First | 10.85 (10.73 to 13.66) | 501 of 501 | 16864821 |
| Same tree again | 1.790 (1.565 to 1.889) | 0 of 501 | 0 |
| One file changed | 1.722 (1.421 to 1.808) | 2 of 501 | 55102 |

Record: [`results/2026-11-09-linux-arm64-docker-on-apple-m5.json`](results/2026-11-09-linux-arm64-docker-on-apple-m5.json). 2026-11-09 (UTC). Linux arm64 (kernel `7.0.14-linuxkit`) **in the Docker Desktop VM on the Apple M5 Mac** of the macOS record: 10 virtual CPUs, 8 GiB. This is not a dedicated Linux machine. The run used a release build of `92d329a`, built in a `rust:1.96-bookworm` container, and was on loopback. The load average inside the VM was 3.6 before and 2.7 after. A build was running on the Mac at the same time.

| Workers | Throughput, tasks/s (median, range) | p50 ms (median, range) | p99 ms (median, range) | Client floor: calls/s, p50 ms |
|---|---|---|---|---|
| 1 | 866.4 (767.8 to 906.5) | 6.3 (6.2 to 6.5) | 8.8 (7.9 to 17.7) | 1978.2, 1.4 |
| 2 | 1088.4 (1086.6 to 1089.7) | 6.3 (6.2 to 6.4) | 8.3 (8.1 to 11.8) | 2105.8, 1.4 |
| 3 | 1054.9 (1053.2 to 1083.9) | 6.4 (6.1 to 6.4) | 7.8 (7.1 to 8.0) | 2097.0, 1.3 |
| 4 | 1106.4 (1041.0 to 1108.9) | 6.3 (6.3 to 6.4) | 7.8 (7.3 to 9.1) | 2121.7, 1.3 |

| Upload of the 500-file tree | Wall time, s (median, range) | Blobs sent | Store growth, bytes |
|---|---|---|---|
| First | 0.222 (0.216 to 0.309) | 501 of 501 | 16864821 |
| Same tree again | 0.047 (0.045 to 0.048) | 0 of 501 | 0 |
| One file changed | 0.047 (0.047 to 0.049) | 2 of 501 | 55102 |

**Why the three records differ.** Linux starts a process and accepts a connection in about 1 ms (the
client floor). Under its loaded run, macOS took 5 to 11 ms for the same, and Windows took 26 to 31
ms. The no-op task, the CLI process per call and the worker's process start all pay that cost, so
the task overhead follows the client floor. The records also differ in hardware and in load, so they
compare machines in the state they were in, not operating systems in general.

**(c) Two-machine `cargo test` workspace offload (`--offload-master`).** Connects from the client machine
to a running two-machine cluster (`--offload-master <HOST:PORT_OR_TICKET>`, authenticating with
`--secret-file <PATH>` or `OXIDESWARM_SECRET` inside a private temporary `HOME`). For each repetition it
picks a fresh workspace name (`<prefix>-r<N>-<hex>`) and runs `oxideswarm submit --workspace <name> --wait
-- cargo <offload-cargo-args>` (default: `test -p rusty_grid_core --lib --offline`) twice from
`--offload-dir` (default: repository root):

- *Cold:* first run in the fresh workspace (syncs the tree, compiles from scratch, and runs the tests).
- *Warm:* second run in the same workspace (syncs 0 new file blobs, reuses `target/`, and prints no
  `Compiling` line).

The JSON record captures client wall clock (`wall_s`), worker-side execution time
(`worker_execution_time_ms` from `Execution Time: <N>ms`), `blobs_sent` and `blobs_total` from the
`Workspace '<name>': ...` summary line, `reused_target` on the warm run, and sanitized worker hardware/OS/link
capabilities from `oxideswarm workers --json` (omitting worker names and IDs so no host names are recorded).

**(c) Recorded run.** Record: [`results/2026-11-09-c-macos-arm64-to-windows-x86_64.json`](results/2026-11-09-c-macos-arm64-to-windows-x86_64.json).
2026-11-09 (UTC). Client and master: Apple M5, 10 cores, 32 GiB, macOS (Darwin 27.2.0), AC power, load
average about 2.5 before and after. Worker: Intel Core i7-1355U (12 threads), 16 GB, Windows 11 Home
(build 26300), `--workspaces`, `--isolation off`, a direct TCP link on one home LAN. Both binaries were
release builds of `5c9e126`; the synced tree was the local commit `8743ed1` (404 files, 10.1 MiB), run
with the default `test -p rusty_grid_core --lib --offline`, 3 repetitions.

| Run | Client wall clock, s (median, range) | Worker time, ms (median, range) | Blobs sent | `target/` reused |
|---|---|---|---|---|
| Cold | 209.6 (209.4 to 224.2) | 209171 (209092 to 223881) | 0 of 405 | no |
| Warm | 7.6 (7.3 to 7.7) | 7248 (7085 to 7279) | 0 of 405 | yes, in all 3 runs |

Every `submit` exited with code 0, which the harness requires. The cold runs sent no blobs because the master already held the tree from
the CLI and MCP runs of the [offload guide record](../../docs/guides/offload-cargo-test.md#recorded-two-machine-run)
made just before (its first run sent 405 of 405 blobs, 10.2 MiB, in a 210.7 s submit). So the cold
figure is the worker writing a fresh tree from its blob cache and compiling the dependencies from
scratch; the warm figure is a sync that changes nothing plus an incremental `cargo test`. Each warm run
took about 3.6 % of its cold run.

## Limits

- (a) and (b) run on one machine over loopback: no network cost, no NAT, no relay, no cluster secret (loopback runs
  unauthenticated and unencrypted, so no handshake or AEAD cost). It says nothing about a LAN or the Internet.
- Throughput is measured through separate CLI processes, so it is limited by process start, not only by
  the master. A client that keeps one connection open would show a different, probably higher, figure.
  The "client floor" columns bound that effect; they do not remove it.
- Throughput does not rise cleanly with workers here: the clients, the master and the workers all share
  the same cores, and the machine was loaded. Do not read the table as a scaling curve.
- Workers use the default below-normal task priority and a fresh sandbox per task; a no-op task is the
  cheapest possible task. Real tasks add their own run time.
- No other scheduler was run, so these figures are not a comparison with any other system.
- (c) is one pair of machines on one LAN, a single worker and one crate: it shows what a reused `target/` saves, not how the grid scales, and a
  relayed or Internet link would add transfer time to a first run.
