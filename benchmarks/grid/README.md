# Grid benchmarks (BENCH-1)

`bench_grid.py` measures the OxideSwarm grid on loopback with the binary you built. It uses only the
Python 3 standard library and runs on macOS, Linux and Windows (the Windows path is written but has
not been run yet; only macOS is recorded below).

```bash
cargo build --release --locked --bin oxideswarm
python3 benchmarks/grid/bench_grid.py --bin target/release/oxideswarm          # about 10 minutes
python3 benchmarks/grid/bench_grid.py --bin target/release/oxideswarm --quick  # smoke run, under a minute
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

## Limits

- One machine, loopback: no network cost, no NAT, no relay, no cluster secret (loopback runs
  unauthenticated and unencrypted, so no handshake or AEAD cost). It says nothing about a LAN or the Internet.
- Throughput is measured through separate CLI processes, so it is limited by process start, not only by
  the master. A client that keeps one connection open would show a different, probably higher, figure.
  The "client floor" columns bound that effect; they do not remove it.
- Throughput does not rise cleanly with workers here: the clients, the master and the workers all share
  the same cores, and the machine was loaded. Do not read the table as a scaling curve.
- Workers use the default below-normal task priority and a fresh sandbox per task; a no-op task is the
  cheapest possible task. Real tasks add their own run time.
- No other scheduler was run, so these figures are not a comparison with any other system.
- (c), the offload of `cargo test` between two machines, is not measured yet; it waits for the workspace
  design (BACKLOG, BENCH-1 step 2).
