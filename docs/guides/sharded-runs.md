# Run sharded tests across workers

## Goal

Split a test suite across `N` OxideSwarm workers with `oxideswarm shard`, injecting `OX_SHARD_INDEX` and `OX_SHARD_TOTAL` and optionally compiling Rust suites once per `(os, arch)` via `--build-once`.

## Prerequisites

- A running master and one or more connected workers ([two-machine-cluster.md](two-machine-cluster.md)); set `--max-concurrency <SLOTS>` on each worker so `slots:<n>` reflects how many parallel shards that machine should take.
- The `oxideswarm` CLI and cluster secret (`~/.oxideswarm/cluster.secret` or `%USERPROFILE%\.oxideswarm\cluster.secret`).
- For Rust `--build-once`, `cargo` and `cargo-nextest` installed on the worker machines matching each target `(os, arch)`, and a Cargo project containing `Cargo.toml`.
- Flag and environment-variable summaries are in [docs/reference.md](../reference.md).

## Steps

### 1. Understand `OX_SHARD_INDEX` and `OX_SHARD_TOTAL`

`oxideswarm shard --count <N>` injects two environment variables into every shard task (`TaskSpec::Command` and `TaskSpec::ShellScript`), overriding any conflicting `--env` values:

- `OX_SHARD_INDEX`: 1-based shard number from `1` to `N` (`1..=OX_SHARD_TOTAL`), matching `cargo nextest`, `jest`, and `pytest-split` directly.
- `OX_SHARD_TOTAL`: total shard count `N`.

For 0-based tools (`0..N-1`), compute `IDX0=$((OX_SHARD_INDEX - 1))` inside a `--shell` task or wrapper script. Note that without `--shell`, command arguments after `--` are passed directly to `execve` without shell variable expansion.

### 2. Rust with `cargo nextest` (`--build-once`)

On the client machine, inside a Cargo workspace root:

```bash
./target/release/oxideswarm shard \
  --master <MASTER_IP>:8088 \
  --count 3 \
  --build-once \
  --junit-out target/shard-junit.xml
```

What happens:

1. `oxideswarm shard` checks `ListWorkers` upfront and fails fast if zero eligible workers match.
2. `scan_build_source` scans `--build-source` (default `.`) using `.gitignore` rules, skipping `.git` and top-level `target/`, and uploads a `TreeManifest` once.
3. One builder task per distinct `(os, arch)` runs `cargo nextest archive --archive-file <ARCHIVE>` and returns the archive `BlobFile` in `TaskResult::outputs`. If any builder fails, remaining builders are cancelled and no shard tasks are submitted.
4. Each of the `3` shard tasks receives the source tree and archive `BlobFile` via `Task::inputs` (never `Task::workspace`), runs `cargo nextest run --archive-file <ARCHIVE> --workspace-remap . --partition slice:<M>/<N>`, and merges the per-shard JUnit XML into `target/shard-junit.xml`.

To run `cargo nextest` without `--build-once` (for example, inside an existing image or custom command):

On the client machine:

```bash
./target/release/oxideswarm shard \
  --master <MASTER_IP>:8088 \
  --count 3 \
  --require-tool cargo \
  --input .:. \
  --shell \
  -- 'cargo nextest run --partition slice:$OX_SHARD_INDEX/$OX_SHARD_TOTAL'
```

### 3. Python with `pytest`

With `pytest-split` installed in the worker environment (1-based `--group` matches `OX_SHARD_INDEX` directly):

On the client machine:

```bash
./target/release/oxideswarm shard \
  --master <MASTER_IP>:8088 \
  --count 4 \
  --input .:. \
  --junit-out reports/pytest-junit.xml \
  --shell \
  -- 'pytest --splits "$OX_SHARD_TOTAL" --group "$OX_SHARD_INDEX" --junitxml=junit.xml'
```

Without plugins (stdlib-only 0-based modulo fallback over `pytest --collect-only -q`):

On the client machine:

```bash
./target/release/oxideswarm shard \
  --master <MASTER_IP>:8088 \
  --count 4 \
  --input .:. \
  --junit-out reports/pytest-junit.xml \
  --shell \
  -- 'IDX0=$((OX_SHARD_INDEX - 1))
TESTS=$(pytest --collect-only -q | grep "::" | awk -v i="$IDX0" -v n="$OX_SHARD_TOTAL" "NR % n == i")
[ -z "$TESTS" ] && exit 0
pytest --junitxml=junit.xml $TESTS'
```

### 4. JavaScript and TypeScript with `jest`

`jest --shard=<M>/<N>` uses 1-based indices matching `OX_SHARD_INDEX` / `OX_SHARD_TOTAL`:

On the client machine:

```bash
./target/release/oxideswarm shard \
  --master <MASTER_IP>:8088 \
  --count 3 \
  --input .:. \
  --shell \
  -- 'npx jest --shard="$OX_SHARD_INDEX/$OX_SHARD_TOTAL"'
```

### 5. Go with `go test`

Partition Go package paths using 0-based modulo `IDX0=$((OX_SHARD_INDEX - 1))`:

On the client machine:

```bash
./target/release/oxideswarm shard \
  --master <MASTER_IP>:8088 \
  --count 3 \
  --input .:. \
  --shell \
  -- 'IDX0=$((OX_SHARD_INDEX - 1))
PKGS=$(go list ./... | awk -v i="$IDX0" -v n="$OX_SHARD_TOTAL" "(NR - 1) % n == i")
[ -z "$PKGS" ] && exit 0
go test $PKGS'
```

### 6. Slot-aware spreading, `--fail-fast`, and `--output-dir`

- **Slot-aware spreading**: `oxideswarm shard` weights Smooth Weighted Round-Robin by each worker's advertised `slots:<n>` (`worker --max-concurrency <n>`, falling back to `cpu_cores`) and pins each shard to its selected worker so heavy nodes receive proportionally more shards.
- **Fail fast**: Pass `--fail-fast` to send best-effort `CancelTask` to remaining in-flight shards as soon as any shard fails, times out, or errors.
- **Per-shard outputs**: Pass `--output-dir <DIR>` with `--output <PATTERN>` to save each shard's files under `<DIR>/shard-01/`, `<DIR>/shard-02/`, and so on. Pre-existing symbolic links at shard output directories are refused.

## Verify

On the client machine, run a 3-shard shell check against connected workers:

```bash
./target/release/oxideswarm shard \
  --master <MASTER_IP>:8088 \
  --count 3 \
  --shell \
  -- 'printf "shard %s of %s\n" "$OX_SHARD_INDEX" "$OX_SHARD_TOTAL"'
```

Expected output:

```text
Shards: 3 completed, 0 failed (3 total)
  [1/3] shard-01 -> completed (worker ..., exit 0, ...ms)
      stdout: shard 1 of 3
  [2/3] shard-02 -> completed (worker ..., exit 0, ...ms)
      stdout: shard 2 of 3
  [3/3] shard-03 -> completed (worker ..., exit 0, ...ms)
      stdout: shard 3 of 3
Tasks: 3 finished, 0 failed, on ... machines
Waited: ... s    Sum of task times: ... s
```

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `no connected workers match shard placement` | `ListWorkers` pre-flight found zero eligible workers | Check `oxideswarm workers --json` and adjust `--os`, `--arch`, `--require-tag`, `--require-tool`, or `--cores` |
| `$OX_SHARD_INDEX` prints literally | `--shell` was omitted, so the client or `execve` did not expand shell variables | Add `--shell` (or `--type shell`) when referencing `$OX_SHARD_INDEX` in command arguments |
| `--workspace cannot be combined with --build-once` | `Task::workspace` forbids `Task::inputs` and serializes onto one worker | Remove `--workspace` when using `--build-once` or multi-worker `--input` sharding |
| `all shard(s) completed without producing any JUnit XML files` | `--junit-out` was set but shards did not write files matching `--junit-glob` | Pass `--junit-glob <PATTERN>` matching your runner's JUnit output path |
| `cell output directory '...' is a symbolic link` | A symlink already exists under `--output-dir` or `--junit-out` | Remove the symlink or choose a clean non-symlink directory |
