# Sweep benchmarks (BENCH-2)

This directory holds the harness and the synthetic workload that the benchmark records of design 0093
(`docs/design/0093-benchmarks-worth-showing.md`) use. It answers one question: does running a sweep of independent tasks
through OxideSwarm finish sooner than running it on the fastest machine alone?

| File | What it is |
|---|---|
| `bench_sweep.py` | The harness: runs one list of tasks locally and through a cluster, and writes one JSON record |
| `workload_cpu.py` | W1, a deterministic CPU-bound task in the Python standard library |
| `results/` | The dated records, `<UTC date>-<slug>.json` |

Both scripts use the Python standard library only.

## The method (design 0093 D1)

1. **Plan first.** A record is written only with `--plan <file>`. The file states the question, the workload, the sizes and the
   expected result, and it is committed before the run. The harness copies it into the record's `plan` field. A poor
   result is published like a good one.
2. **Baseline: the fastest machine alone.** Each machine runs the same tasks locally, without OxideSwarm, one process per task,
   at 1, half and all logical cores. The best of the three is that machine's baseline, and the record keeps all three.
3. **Metrics.** Throughput in tasks per second. Speedup = grid throughput / the fastest machine's best local throughput.
   Parallel efficiency = grid throughput / the sum of each machine's best local throughput; it is the fair figure for
   machines of different speed.
4. **Runs.** One warm-up run, recorded and not counted, then `--runs` measured runs (3 by default). The record gives the
   median, the minimum and the maximum.
5. **Record.** The date, the OxideSwarm commit, version and build profile, the workload file and its SHA-256, each
   machine's CPU, logical cores, RAM, OS, architecture and power, each worker's slots, isolation mode and link, the link
   facts of `doctor --json`, the load averages before and after, the plan, every raw run and the count of failed tasks.
6. **Harness rules.** The harness starts its own processes, gives each a private `HOME` and `USERPROFILE`, uses its own
   ephemeral ports with the discovery beacon off, and stops the processes by handle. Records hold no host name, user name,
   worker name, worker id, path or IP address; workers appear as `worker-1`, `worker-2`, and so on.
7. **Results are checked.** Every grid task's checksum is compared with the local run of the same task, and the record counts
   matches, mismatches and missing results. W1's checksum depends only on its seed and its units, so it is the same on
   every system. A workload such as the Anima Engine runner gives different checksums on different systems (design 0089 D6),
   and is checked only against a machine of the same OS and architecture.

## W1

```text
python3 benchmarks/sweep/workload_cpu.py --seed 7 --units 1000
W1 seed=7 units=1000 checksum=98aaab6eec02b5f265d2a3e6d560f2aaea7b557d78aa991be5ace2cdea358d26
python3 benchmarks/sweep/workload_cpu.py --calibrate 10     # units that take about 10 s here
```

A task chains SHA-256 over a 32-byte state `--units` times. Without `--units`, the harness calibrates the units for
`--task-seconds` on the machine that runs it. Pass `--units` explicitly when machines are to be compared, because every
record of one comparison must use the same units, and `--local-results` refuses a record with other units or another
version of `workload_cpu.py`.

## Running it

```text
# Smoke run: 8 tasks of about 0.05 s, a loopback cluster, nothing written.
python3 benchmarks/sweep/bench_sweep.py --quick --no-write

# A record on one machine: the local baseline and a loopback cluster on the same machine.
python3 benchmarks/sweep/bench_sweep.py --plan plan.json --tasks 200 --units <U>

# Two machines (BENCH-3): first the local baseline on each machine...
python3 benchmarks/sweep/bench_sweep.py --plan plan.json --local-only --tasks 200 --units <U> --slug w1-local-<machine>
# ...then the sweep through a master for the run, with the other machine's local record.
python3 benchmarks/sweep/bench_sweep.py --plan plan.json --tasks 200 --units <U> \
    --master <HOST:PORT> --secret-file <the run's secret> --worker-python python3 \
    --local-results benchmarks/sweep/results/<date>-w1-local-<other machine>.json
```

- `--bin` names the `oxideswarm` binary. The default is `target/release/oxideswarm`.
- Without `--master`, the harness starts a loopback cluster: one master and `--workers` workers, each with `--slots` slots.
  The default slot count is the best local parallelism.
- With `--master`, it only submits. It never starts, stops or reconfigures the master or the workers.
- The grid runs every task as `submit --json --wait --input workload_cpu.py --command <python> -- workload_cpu.py --seed <S> --units <U>`.
  It keeps `--client-concurrency` submits in flight; the default is the cluster's total slots.
- `--worker-python` is the interpreter command on the workers: `python3` on macOS and Linux, `python` (or `py`) on Windows.

A loopback run measures OxideSwarm against the same machine, so its speedup is below 1. It shows the per-task overhead on
one machine, not scaling. Scaling needs at least two machines and the local record of each.

The loopback workers keep their sandboxes under the system temporary directory. A worker refuses a sandbox base that other
accounts can write. If the harness reports that a worker exited early, point `TEMP` and `TMP` (or `TMPDIR`) at a private
directory.

## Tests

`tests/test_bench_sweep.py` covers the helpers, W1 and the local half of the harness without a binary. The CI job
`agent bridge` runs it on Linux, macOS and Windows.

## Task process time and slot use (record format 2, BENCH-9)

From `record_format: 2`, every record adds process timing and slot utilization alongside the format-1 throughput
and latency metrics (`--local-results` still accepts format-1 local records; those fields return `None`):

| Scope | Field | Meaning |
|---|---|---|
| Local run and level | `task_time_mean_s`, `task_time_median_s` | Mean and median wall-clock seconds around `subprocess.run` over succeeded local tasks in that run or across measured runs at that parallelism level |
| Grid run | `task_time_sum_s` | Sum of worker-reported `execution_time_ms / 1000` over all tasks in the run that returned a time (includes failed tasks, since their slots were occupied) |
| Grid run | `task_time_missing` | Count of tasks in the run without a numeric `execution_time_ms` |
| Grid run | `slot_seconds` | `total_slots * wall_s` — total slot-seconds the cluster offered during the run |
| Grid run | `utilization` | `task_time_sum_s / slot_seconds` (fraction of offered slot-seconds spent inside task execution, or `null` when every row lacks `execution_time_ms`) |
| Grid run and `grid` | `per_worker.<label>` | `tasks` (all rows for `<label>`), plus `mean_s` and `median_s` of worker-reported execution time over succeeded tasks (`checksum != null`) per neutral worker label (`worker-1`, `worker-2`, ...; across measured runs at top-level `grid.per_worker`) |
| Grid `grid` | `utilization` | `summarize` (`median`, `min`, `max`, `runs`) of per-run `utilization` across the measured runs (`null` if no run had a timed task) |
| Local and grid run | `competing_processes` | Low-rate background sampler (every 2 s) watching for `cargo` / `rustc` process names on the harness host only (a `cargo` run on a remote worker machine is not visible to it); records `watched`, `interval_s`, `samples`, `detections`, and `sightings` (`utc`, `processes` base names only) |

Grid runs also print a second summary line right below the unchanged first summary line:

```text
slot utilization <pct> % (median of <runs>); mean task time: worker-1 <x> s, ...
```

**What these numbers show.** Comparing a worker label's grid `mean_s` (or `median_s`) with that machine's local
`task_time_mean_s` at the same parallelism separates **per-task process slowdown** (for instance below-normal worker
priority, CPU contention on the host, or thermal throttling) from **slot idle time** (`1 - utilization`).

**What these numbers do not show.** `execution_time_ms` starts at the top of the worker's runner
(`crates/worker/src/runner.rs`, `start_time`), so it includes `Sandbox::create` and staged/blob input placement
into the sandbox (`crates/worker/src/runner/workloads.rs`) in addition to the child process, whereas fetching missing
blobs from the master, harvesting artifacts and collecting output blobs, destroying the sandbox, uploading outputs,
and sending `TaskResult` back sit outside `execution_time_ms`. `utilization` does not split non-execution
slot-seconds among those stages, client `submit` CLI startup, master dispatch, or end-of-sweep tail drain. Both
`execution_time_ms` and local `task_time_s` measure wall-clock seconds (including interpreter startup), not kernel
user+sys CPU seconds.
