# Endurance benchmarks (`TEST-24`, design 0065)

`benchmarks/endurance/endurance.py` drives the built `oxideswarm` binary to measure large file
and many-file transfers, memory flatness across sizes, output-limit enforcement, upload
deduplication, mid-transfer disconnects, throttled TCP behavior, and blob-store capacity
refusals. It uses only the Python 3 standard library.

The `soak` subcommand comes in `TEST-25`, and the scheduled CI workflow comes in `TEST-26`.

## Usage

```bash
cargo build --release --locked --bin oxideswarm
python3 tests/test_endurance_harness.py
python3 benchmarks/endurance/endurance.py transfer --bin target/release/oxideswarm
```

Windows PowerShell example using a private work directory outside `AppData`:

```powershell
cargo build --release --locked --bin oxideswarm
python tests\test_endurance_harness.py
python benchmarks\endurance\endurance.py transfer `
  --bin target\release\oxideswarm.exe `
  --work-dir C:\ox_tmp\endurance `
  --scenarios T1,T2,T3,T4,T6,T7,T8 `
  --t1-sizes-gib 1,3 `
  --t2-files 20000
```

Subcommands in `TEST-24`:

- `transfer`: runs selected transfer scenarios (`T1`–`T9`) and writes one JSON record under
  `benchmarks/endurance/results/<UTC date>-<link>-<os>-<arch>-<cpu>.json`.
- `throttle`: runs a standalone user-space TCP proxy bound strictly to `127.0.0.1` (`--listen`,
  `--target`, `--rate-mbps`, `--delay-ms`, `--port-file`). Used in `T6`, `T7`, and `T8`.
- `sample`: polls resident memory (`rss_bytes`), open handles (`handles`), threads (`threads`),
  CPU time (`cpu_seconds`), Windows real-time AV state, and store/cache directory sizes
  (`--pid ROLE=PID`, `--blob-dir`, `--cache-dir`, `--sandbox-dir`, `--state-dir`). Open handles
  come from `Get-Process` on Windows, `lsof -n -P -p` on macOS, and `/proc/<pid>/fd` on Linux.

Every run follows the rules of `benchmarks/grid/bench_grid.py`:

- Starts its own master (`127.0.0.1:0`, `--port-file`, `--dashboard-port 0`,
  `--discovery-port 0`) and worker(s).
- Gives every child process (`master`, `worker`, `submit`) its own private `HOME`, `USERPROFILE`,
  `OXIDESWARM_HOME`, `TEMP`, and `TMP` (never touching `~/.oxideswarm` or any installed trial
  worker).
- Stops child processes strictly via the `Popen` handles it holds (never by process name) and
  removes its temporary directory afterwards.
- Refuses to start unless free disk space is at least `3 * largest_file + 10 GiB`.
- Redacts host names, user names, drive/home paths, and non-loopback IPv4 addresses before
  writing any JSON record.

## Data generation and integrity verification

Python's standard library `hashlib` does not include BLAKE3. The harness verifies bytes end-to-end
with streaming **SHA-256** independently of the code under test, without creating a second copy on
disk, while recording `oxideswarm`'s on-disk BLAKE3 store object names:

1. **Client generation & upload (`oxideswarm submit --input`):** `endurance.py` streams seeded
   random bytes (`random.Random(seed)`) in `4 MiB` chunks directly into the payload file while
   computing a streaming `sha256` digest. Independently, `oxideswarm submit` hashes every file with
   `blake3::Hasher` (`BlobHasher`), announces the 64-hex BLAKE3 digest as the `BlobId`, and
   uploads chunks to the master.
2. **Master & worker store inspection (object names, not recomputed):** `BlobStore` on the master
   and `BlobCache` on the worker verify BLAKE3 internally inside the binary (`crates/master/src/blob_store.rs`
   and `crates/worker/src/blob_cache.rs`). For loopback runs, the harness records the resulting
   `<hh>/<64-hex>` entries from `<blob_dir>/objects` and `cache/blobs/objects` whose byte size
   matches the payload (`master_blake3_ids`, `worker_cache_blake3_ids`) — it does not recompute
   BLAKE3 in Python.
3. **Worker & client end-to-end SHA-256 check:** the worker task script streams the materialized file
   or tree in its sandbox without copying it and checks exact byte size and streaming `sha256`
   against the client's expected digest independently of `oxideswarm`. For outputs (`T3`, `T7`), it
   generates seeded output files whose BLAKE3 `id` is returned in `submit --wait --json` `outputs[]`
   and verified again on download by `oxideswarm submit --output-dir` plus independent streaming
   `sha256`.

## Scope and deferred runs (`TEST-24` step 1 vs step 2)

Loopback baseline results on a Windows `x86_64` desktop (`T1` at `1` and `3 GiB`, `T2` at
`20,000` files, `T3`, `T4`, `T6`, `T7`, `T8`) are recorded below.

Deferred to the owner-present two-machine session (`TEST-24` step 2):

- `T1` at `8 GiB` and `T2` at `100,000` files
- `T5` (half-cut upload over a real/throttled two-machine upload)
- `T9` (3 sequential tasks with a `3 GiB` input > `2 GiB` worker cache)
- Two-machine links: `lan-tcp`, `lan-quic`, and `quic-relay`

## Recorded runs

Record: [`results/2026-11-19-loopback-windows-amd64-intel-core-i5-14600kf.json`](results/2026-11-19-loopback-windows-amd64-intel-core-i5-14600kf.json).
`2026-11-19` (`04:53:33Z`), Windows `10.0.26300` (`AMD64`), `Intel(R) Core(TM) i5-14600KF` (`20` logical
cores, `47.79 GiB` / `51,318,800,384 B` RAM), Windows Defender real-time protection `enabled = true`,
release build `rusty-grid 0.4.3` at `a3e5b71` (`dirty_tree = true` while adding the harness), loopback
(`127.0.0.1`, measured TCP connect RTT `0.377 ms`, theoretical 4 MiB/RTT single-blob bound `10,610.08 MiB/s`).

All 7 requested loopback scenarios passed (`7/7`, total run `2,648.17 s`):

| Scenario | Check & Key Parameters | Result | Wall time | Key Metrics & Resource Summary |
|---|---|---|---|---|
| `T1` (`1 GiB`) | `1,073,741,824 B` uploaded, worker SHA-256 verified, Master & Worker BLAKE3 `9b7296ce...c3c` | `PASS` | `10.033 s` | `102.061 MiB/s`; peak RSS: master `27.3 MiB`, worker `26.7 MiB`, client `29.3 MiB` |
| `T1` (`3 GiB`) | `3,221,225,472 B` uploaded, worker SHA-256 verified, Master BLAKE3 `01fa0722...e3c`, evicted from `2 GiB` worker cache after task | `PASS` | `67.899 s` | `45.244 MiB/s`; peak RSS: master `23.6 MiB` (`-3.9 MiB`), worker `24.0 MiB` (`-2.8 MiB`), client `29.9 MiB` (`+0.64 MiB`) — within `128 MiB` flatness bound |
| `T2` | `20,000` files (`173,998,510 B` data, `20,001` blobs / `175,278,536 B` uploaded) | `PASS` | `233.124 s` | `master_blobs_stored = 20,001`; peak RSS: master `26.0 MiB`, worker `32.7 MiB`, client `29.8 MiB`; handles `<= 312` |
| `T3` | Default `--output-limit-mb` (`1 GiB`): `1,000 MiB` (`1,048,576,000 B`) pass vs `1,100 MiB` (`1,153,433,600 B`) refusal | `PASS` | `10.757 s` / `3.679 s` | `1,000 MiB` BLAKE3 `60d45ecd...151` downloaded & verified; `1,100 MiB` failed with `--output-limit-mb`, `0` tmp leftovers |
| `T4` | `3 GiB` (`3,221,225,472 B`) submitted twice | `PASS` | `56.469 s` / `20.488 s` | Second submit uploaded `0` blobs / `0 B` (`master_blob_store_growth_bytes = 0`) |
| `T6` | `64 MiB` input fetch cut mid-transfer via loopback proxy (`12 Mbit/s`, `cut_links(3 s)`) | `PASS` | `4.440 s` | Worker disconnected (`os error 10054`), failed over and completed cleanly; `0` `.part` files left in `cache/blobs/tmp` |
| `T7` | `256 MiB` input + `256 MiB` output over throttled TCP (`2.0 Mbit/s`, `50 ms` RTT, `2,165 s` vs `0.25 MiB/s` cap) | `PASS` | `2,164.622 s` | `worker_reaped = false` (`0` reap log entries across `406` samples over `36.1 min`); output BLAKE3 `1e9191a6...7af` verified; peak RSS: master `23.6 MiB`, worker `35.9 MiB` |
| `T8` | `--blob-store-mb 64`: `72 MiB` single blob + two concurrent `40 MiB` uploads | `PASS` | `0.050 s` (`8a`) | Both refused naming `--blob-store-mb` (`0` tmp leftovers after finish) |
