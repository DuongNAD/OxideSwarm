# Run your test matrix on several machines

## Goal

Fan out a Rust test matrix across multiple isolated workers with a single `oxideswarm submit --matrix` command (`crates/cli/src/matrix.rs`, [design 0101](../design/0101-matrix-fanout.md)), read human and JSON cell results, and re-run only a failed cell.

## Prerequisites

- The built `oxideswarm` binary (`cargo build --release --bin oxideswarm` or an archive release).
- On the worker machines: `cargo` and `rustc` in `PATH` (probed at worker start-up as `tool:cargo` and `tool:rustc` in `crates/core/src/capabilities.rs`), plus OS task isolation (`--isolation fs`, [design 0033](../design/0033-os-level-task-isolation.md)).
- On the client machine: a Rust repository checkout without a large ignored `target/` directory inside the uploaded `--input` path (or copy the `oxideswarm` binary out of `target/` and clean `target/` before passing `--input .:.`, because `scan_inputs` uploads everything under `--input` paths).
- Full flag, port, and environment-variable reference: [docs/reference.md](../reference.md). For multi-machine setup, see [two-machine-cluster.md](two-machine-cluster.md). For test sharding with `--build-once` and JUnit merging, see [sharded-runs.md](sharded-runs.md).

## Steps

### 1. Start a cluster with isolated state and `--isolation fs`

To practice on one machine without touching `~/.oxideswarm` or default ports, set `OXIDESWARM_HOME` to a temporary directory, generate a cluster secret first (always keep cluster authentication enabled), and use ephemeral ports (`127.0.0.1:0` with `--port-file`).

On the master machine (Terminal 1):

```bash
export OXIDESWARM_HOME="$(mktemp -d)"
oxideswarm secret generate
oxideswarm master \
  --listen 127.0.0.1:0 \
  --port-file "$OXIDESWARM_HOME/master.port" \
  --discovery-port 0 \
  --dashboard-port 0
```

Read the bound master port printed on stdout or in `"$OXIDESWARM_HOME/master.port"`, then start two workers with distinct `--name`, `--tag`, and `--sandbox-base-dir` values (`--no-portal` avoids port `8080` collisions when two workers share one host).

On worker machine A (Terminal 2, using the same `OXIDESWARM_HOME` or copy of `cluster.secret`):

```bash
export OXIDESWARM_HOME="<RUN_HOME>"
oxideswarm worker \
  --master "127.0.0.1:<MASTER_PORT>" \
  --isolation fs \
  --no-portal \
  --no-gpu \
  --max-concurrency 2 \
  --name worker-a \
  --tag lane-a \
  --sandbox-base-dir "$OXIDESWARM_HOME/sb-worker-a"
```

On worker machine B (Terminal 3, using the same `OXIDESWARM_HOME` or copy of `cluster.secret`):

```bash
export OXIDESWARM_HOME="<RUN_HOME>"
oxideswarm worker \
  --master "127.0.0.1:<MASTER_PORT>" \
  --isolation fs \
  --no-portal \
  --no-gpu \
  --max-concurrency 2 \
  --name worker-b \
  --tag lane-b \
  --sandbox-base-dir "$OXIDESWARM_HOME/sb-worker-b"
```

Confirm both workers advertise `state:connected`, `iso:fs`, and `tool:cargo` before submitting.

On the client machine (from the repository root):

```bash
export OXIDESWARM_HOME="<RUN_HOME>"
oxideswarm workers --master "127.0.0.1:<MASTER_PORT>" --json
```

### 2. Fan out a 2D test matrix with `submit --matrix`

`--matrix` takes `KEY=V1,V2,...` axes (`os`, `arch`, `worker`, `iso`, `tool`, `tag`, and `env:<VAR>`) and submits the Cartesian product concurrently (`--matrix` implies `--wait`). Pass `--timeout-secs 300` so fresh per-task Cargo builds in `--input` sandboxes do not hit the `60` s default `submit` timeout. Use `--input .:.` rather than `--workspace` when fanning out across multiple workers (`--workspace` serializes onto one workspace affinity worker at a time).

On the client machine (from the repository root):

```bash
oxideswarm submit \
  --master "127.0.0.1:<MASTER_PORT>" \
  --require-tool cargo \
  --timeout-secs 300 \
  --input .:. \
  --matrix tag=lane-a,lane-b \
  --matrix 'env:TEST_FILTER=placement::tests::test_aliases,placement::tests::test_matches' \
  --type shell \
  -- 'cargo test --offline -p rusty_grid_core --lib "$TEST_FILTER"'
```

To fan out independent test slices with `OX_SHARD_INDEX` / `OX_SHARD_TOTAL` instead of a Cartesian grid, use `oxideswarm shard` ([sharded-runs.md](sharded-runs.md)):

On the client machine (from the repository root):

```bash
oxideswarm shard \
  --master "127.0.0.1:<MASTER_PORT>" \
  --count 2 \
  --require-tool cargo \
  --timeout-secs 300 \
  --input .:. \
  --shell \
  -- 'if [ "$OX_SHARD_INDEX" = "1" ]; then cargo test --offline -p rusty_grid_core --lib placement::tests::test_aliases; else cargo test --offline -p rusty_grid_core --lib placement::tests::test_matches; fi'
```

### 3. Read machine-readable results (`--json`)

Add `--json` to inspect summary counts (`total_cells`, `completed_cells`, `failed_cells`, `unplaced_cells`, `machines`, `waited_ms`, `task_time_sum_ms`) and per-cell `task_id`, `worker_name`, `exit_code`, and `duration_ms` fields (`MatrixReport::to_json` in `crates/cli/src/matrix.rs`). Repeating the submission with unchanged files uploads `0` blobs because the master already holds them.

On the client machine (from the repository root):

```bash
oxideswarm submit \
  --master "127.0.0.1:<MASTER_PORT>" \
  --require-tool cargo \
  --timeout-secs 300 \
  --input .:. \
  --matrix tag=lane-a,lane-b \
  --matrix 'env:TEST_FILTER=placement::tests::test_aliases,placement::tests::test_matches' \
  --json \
  --type shell \
  -- 'cargo test --offline -p rusty_grid_core --lib "$TEST_FILTER"'
```

### 4. Inspect and re-run when one cell fails (or is unplaced)

`oxideswarm submit --matrix` waits for all placed cells and exits non-zero (`1`) if any cell fails, times out, or is unplaced (`unplaced` cells are detected upfront via `ListWorkers` without waiting for `--timeout-secs`).

When one cell fails, identify its tag/environment (and its `task_id` from `--json` or `oxideswarm status --json`), optionally fetch its full output with `oxideswarm result <TASK_ID>`, then re-run only that cell with `submit --wait`.

On the client machine (re-running only the `lane-b` cell):

```bash
oxideswarm result <TASK_ID> --master "127.0.0.1:<MASTER_PORT>"
oxideswarm submit \
  --master "127.0.0.1:<MASTER_PORT>" \
  --require-tool cargo \
  --require-tag lane-b \
  --timeout-secs 300 \
  --input .:. \
  --wait \
  --type shell \
  -- 'cargo test --offline -p rusty_grid_core --lib placement::tests::test_aliases'
```

When finished, stop the workers and master cleanly and remove the temporary `OXIDESWARM_HOME` directory.

## Verify

Run a fast 2-cell check across the matrix (including an intentionally unplaced cell `missing-lane` to verify pre-flight rejection returns immediately instead of hanging) or verify the passing 2-worker `rusty_grid_core` matrix from Step 2.

On the client machine:

```bash
oxideswarm submit \
  --master "127.0.0.1:<MASTER_PORT>" \
  --require-tool cargo \
  --matrix tag=lane-a,lane-b \
  --type shell \
  -- 'cargo --version'
```

Expected output:

```text
Matrix: 2 completed, 0 failed, 0 unplaced (2 total)
  [0] tag=lane-a -> completed (worker worker-a, exit 0, <MS>ms)
      stdout: cargo ...
  [1] tag=lane-b -> completed (worker worker-b, exit 0, <MS>ms)
      stdout: cargo ...
Tasks: 2 finished, 0 failed, on 2 machines
Waited: <W> s    Sum of task times: <S> s
```

### Recorded run

Recorded on `2026-11-27` on `rusty-grid 0.4.5` (commit `07caa5ae`) on one Apple Silicon M5 Mac (`10` cores, `32768` MiB RAM, `macOS 27.2.1`, `aarch64`) running one loopback master (`127.0.0.1:59585`, generated cluster secret, ephemeral port, authentication enabled) and two `--isolation fs --no-portal --no-gpu --max-concurrency 2` workers (`worker-a` tagged `lane-a`, `worker-b` tagged `lane-b`):

| Check | Outcome (from this one run) | Recorded summary and timings (from this one run) |
|---|---|---|
| Passing 2D matrix (human, 4 cells: `tag=lane-a,lane-b` x `env:TEST_FILTER=test_aliases,test_matches`) | `exit 0` (`4 completed, 0 failed, 0 unplaced`) | `Inputs: 855 files (21.0 MiB); sent 853 of 853 blobs (21.0 MiB), 0 already on the master`; `Tasks: 4 finished, 0 failed, on 2 machines`; `Waited: 108.921 s`, `Sum of task times: 394.024 s` (`wall 109.074 s`) |
| Passing 2D matrix (`--json`, 4 cells, warm blob cache) | `exit 0` (`"status": "completed"`, `"machines": 2`) | `"uploaded": 0` (`853 already on the master`); `"waited_ms": 93447`, `"task_time_sum_ms": 371089` (`wall 93.558 s`) |
| One failing cell (`tag=lane-a,lane-b`, `lane-b` exits `1`) | `exit 1` (`1 completed, 1 failed, 0 unplaced`) | `[0]` `worker-a` `exit 0` (`45307ms`); `[1]` `worker-b` `exit 1` (`45317ms`); `Waited: 46.789 s`, `Sum of task times: 90.624 s` (`wall 46.894 s`) |
| Single-cell rerun (`--require-tag lane-b --wait`) | `exit 0` (`1 finished, 0 failed, on 1 machines`) | `Execution Time: 27583ms`; `Waited: 27.847 s`, `Sum of task times: 27.583 s` (`wall 27.894 s`) |
| Unplaced cell pre-flight (`tag=lane-a,missing-lane`) | `exit 1` (`1 completed, 0 failed, 1 unplaced`) | `[0]` `worker-a` `exit 0` (`27ms`); `[1] tag=missing-lane -> unplaced: no connected worker matches cell constraints (tool=cargo, tag=missing-lane)`; `Waited: 0.029 s`, `Sum of task times: 0.027 s` (`wall 0.036 s`) |
| Companion `oxideswarm shard --count 2` (`test_aliases` / `test_matches`) | `exit 0` (`Shards: 2 completed, 0 failed (2 total)`) | `[1/2]` `worker-a` (`46358ms`), `[2/2]` `worker-b` (`46366ms`); `Tasks: 2 finished, 0 failed, on 2 machines`; `Waited: 46.783 s`, `Sum of task times: 92.724 s` (`wall 46.790 s`) |

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `timed out: timed out after 60s` on fresh Cargo cells | Default `submit` task timeout is `60` s while clean per-task `--input` sandboxes compile dependencies from scratch | Pass `--timeout-secs 300` (or higher for larger crates) |
| `--input .:.` uploads hundreds of MiB of build artifacts | `scan_inputs` sends all files under `--input` including `target/` if it exists in `.` | Run `cargo clean` before submitting (keeping the `oxideswarm` binary outside `.`), or pass source directories explicitly (`--input Cargo.toml --input crates:crates`) |
| `unplaced: no connected worker matches cell constraints` | Pre-flight `ListWorkers` found no connected worker satisfying that cell's `--os`, `--arch`, `--require-tool`, or `--tag` | Run `oxideswarm workers --json` and fix the axis values or worker `--tag` flags |
| All matrix cells run sequentially on one worker | `--workspace` pins every task in that workspace onto one worker | Switch to `--input .:.` when you want concurrent fan-out across workers |
| `invalid matrix axis` or conflict error before connecting | Duplicate singular axis (`os`, `arch`, `worker`, `iso`), reserved tag prefix (`os:`, `tool:`, `state:`, ...), or count above `--max-matrix-cells` (default `64`, max `256`) | Remove conflicting base flags (`--os`, `--worker`) and raise `--max-matrix-cells` up to `256` if needed |
