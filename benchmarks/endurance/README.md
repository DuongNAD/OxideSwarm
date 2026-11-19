# Endurance benchmarks (`TEST-24` & `TEST-25`, design 0065)

`benchmarks/endurance/endurance.py` drives the built `oxideswarm` binary to measure large file
and many-file transfers, memory flatness across sizes, output-limit enforcement, upload
deduplication, mid-transfer disconnects, throttled TCP behavior, blob-store capacity
refusals, and multi-worker fault-injection soak runs. It uses only the Python 3 standard library.

The scheduled CI workflow comes in `TEST-26`.

## Usage

```bash
cargo build --release --locked --bin oxideswarm
python3 tests/test_endurance_harness.py
python3 benchmarks/endurance/endurance.py transfer --bin target/release/oxideswarm
python3 benchmarks/endurance/endurance.py soak --bin target/release/oxideswarm --duration 45m --fault-scale 0.25
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
python benchmarks\endurance\endurance.py soak `
  --bin target\release\oxideswarm.exe `
  --duration 45m `
  --fault-scale 0.25 `
  --load-scale 1.0 `
  --sample-interval 60s `
  --workers 3 `
  --slots 4 `
  --max-queue-size 64 `
  --work-dir C:\ox_tmp\soak
```

Subcommands in `TEST-24` and `TEST-25`:

- `transfer`: runs selected transfer scenarios (`T1`–`T9`) and writes one JSON record under
  `benchmarks/endurance/results/<UTC date>-<link>-<os>-<arch>-<cpu>.json`.
- `soak`: runs one master (`--state-dir`, `--max-queue-size`, `--blob-store-mb`) and 2 or 3 workers
  (`--workers`, `--slots`, `--workspaces`/`--no-workspaces`, `--blob-cache-mb`) across per-worker
  `127.0.0.1` `ThrottleProxy` instances under seeded per-minute load (`--load-scale`) and scaled
  faults (`--duration`, `--fault-scale`, `--sample-interval`: worker kill/restart, master restart
  on the same state dir, proxy link cuts, and queue-cap bursts), checks every task via `TaskLedger`
  and computes all five D3 item 6 pass/fail checks into
  `benchmarks/endurance/results/<UTC date>-soak-<duration>-<link>-<os>-<arch>-<cpu>.json`.
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

## Scope and deferred runs (`TEST-24` & `TEST-25` step 1 vs step 2)

Loopback baseline results on a Windows `x86_64` desktop are recorded below (`TEST-24` step 1
`T1`–`T8`, and `TEST-25` step 1 45-minute soak).

Deferred to owner multi-machine sessions (`TEST-24` & `TEST-25` step 2):

- `TEST-24` step 2: `T1` at `8 GiB`, `T2` at `100,000` files, `T5`, `T9`, and two-machine links
  (`lan-tcp`, `lan-quic`, `quic-relay`)
- `TEST-25` step 2: full `24h` (`--fault-scale 1.0`) three-machine soak (Mac master + two Windows
  workers on mains power, Q57)

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

### `TEST-25` step 1 (`45m` Windows loopback soak)

Record: [`results/2026-11-19-soak-45m-loopback-windows-amd64-intel-core-i5-14600kf.json`](results/2026-11-19-soak-45m-loopback-windows-amd64-intel-core-i5-14600kf.json).
`2026-11-19` (`12:30:08Z`), Windows `10.0.26300` (`AMD64`), `Intel(R) Core(TM) i5-14600KF` (`20` logical
cores, `47.79 GiB` / `51,318,800,384 B` RAM), Windows Defender real-time protection `enabled = true`,
release build `rusty-grid 0.4.4` at `4c11a45` (`dirty_tree = false`), loopback (`127.0.0.1`,
measured TCP connect RTT `0.363 ms`), `--duration 45m --fault-scale 0.25 --load-scale 1.0 --sample-interval 60s --workers 3 --slots 4 --workspaces --max-queue-size 64 --blob-store-mb 4096 --blob-cache-mb 2048`.

All 6 checks passed (`6/6`, 5 D3 item 6 checks plus fault completeness, actual elapsed `2,704.22 s`; faults injected: `8` `worker_kill`, `5` `link_cut`, `2` `queue_burst` [80 submits each into `max_queue_size=64`; spawning 24 CLI subprocesses spread submits over several seconds as the 12 worker slots drained, so `0` `QueueFull` refusals occurred in this run], `1` `master_restart`):

| Check | Result | Key Metrics & Resource Summary |
|---|---|---|
| `1_accounting` | `PASS` | `2,185` submitted (`1,350` `noop`, `225` `new_input`, `225` `reused_input`, `90` `output`, `45` `workspace_sync`, `45` `cancel`, `45` `probe`, `160` `queue_burst`), `2,185` terminal (`2,140` `ok`, `45` `cancelled`), `0` submit failures, `0` lost/duplicate/hash mismatch/unexpected, `master_audit`: `2,185` unique task results logged (`0` multi-result tasks, `0` queue errors, `12` failover sweeps), `4` retries / `4` orphan requeues (`GET /api/tasks` + master `INFO` logs; debug-only `stale_results_ignored` / `late_results_ignored` unmeasured at default `INFO`) |
| `2_resource_plateau` | `PASS` | Early (`423.2–1,208.6 s`) vs late (`1,934.2–2,702.0 s`) means across restarts: master `31.2 -> 38.4 MiB` (`198.4 -> 201.1` handles), `w0 28.6 -> 29.2 MiB`, `w1 28.3 -> 28.2 MiB`, `w2 26.6 -> 30.3 MiB`. Per-process lifetimes: master gen 0 `16.0 -> 35.5 MiB` (`+19.6 MiB`), master gen 1 `29.2 -> 40.3 MiB` (`+11.1 MiB`); worker lifetimes stayed `15.2–31.7 MiB` peak across restarts |
| `3_disk_and_journal` | `PASS` | Blob store peak `4,293,920,667 B`, idle `4,285,531,715 B` (`<= 4 GiB`); worker cache mid-run peaks `w0 2,146,435,072 B` / `w1 2,228,226,815 B` (`2,125.0 MiB`, 1 sample above `2 GiB` while pinned inputs were active, within `2 GiB + 564 MiB` pinned headroom) / `w2 2,140,143,616 B` and idle `w0 2,128,610,709 B` / `w1 2,107,639,671 B` / `w2 2,090,860,544 B` (`<= 2 GiB`); sandboxes clean (`0` tmp/lock leftovers); `journal.bin` shrank `1,525,821 -> 24 B` (restart) and `801,453 -> 24 B` (final stop) |
| `4_probe_p99` | `PASS` | `45/45` probes ok; `8` probes overlapping fault windows excluded (`[t_s, end_s + 15 s]`), leaving `14` baseline (`p99 = 130.852 ms`) and `16` late (`p99 = 146.509 ms`) clean probes (`ratio = 1.1197 <= 1.5`) |
| `5_clean_stop` | `PASS` | `leftover_pids = []`, `leftover_task_trees = []` |
| `6_fault_injection` | `PASS` | `16/16` planned faults executed cleanly, `0` fault errors |

## Remote-worker soak mode (`soak --master-only` and `worker-agent`, D4.3)

For the three-machine 24-hour soak (design 0065 D4.3: one Mac master plus two Windows workers),
`endurance.py` splits cluster coordination across an authenticated master/worker split:

- **`soak --master-only` (master machine):** starts the master with `--secret-file`, hosts the
  HMAC-SHA256 control HTTP server (`--control-listen`), waits for `N` named `worker-agent` nodes to
  register on the control server and connect to the master (`--expect-workers`,
  `--register-timeout`), runs the D3 task load, timed probe loop, master sampling, and fault
  schedule (`queue_burst`, `master_restart` rebinding the fixed master port, and remote
  `worker_kill` / `link_cut`), then pulls each remote worker's samples and idle disk snapshot
  before writing the merged JSON record.
- **`worker-agent` (each remote machine):** runs inside a private `--work-dir` outside `AppData`
  and `~/.oxideswarm` (with `tmp` as a sibling of `home`), opens a local `127.0.0.1` `ThrottleProxy`
  pointed at `--master` so `link_cut` works identically across machines, launches
  `oxideswarm worker --name <NAME> --secret-file <PATH>`, samples `worker_<name>` RSS/handles/threads/CPU
  and worker cache/sandbox bytes aligned to the master's soak start clock, executes authenticated
  faults (`kill_restart`, `link_cut`), and returns idle disk and process-tree clean-stop telemetry.

### Control-channel security model

1. **Out-of-band run secret only:** generate a fresh run secret (`oxideswarm secret generate --out <RUN_SECRET_FILE>`) and copy that file by hand to each remote worker machine. The secret never travels over the hub, in a PR, in a commit, or in CLI arguments; the harness accepts `--secret-file <PATH>` only.
2. **Domain-separated mutual HMAC-SHA256 authentication:** both sides derive a dedicated key via `HMAC-SHA256(secret, "oxideswarm-endurance-control-v1")`. Every request (`X-Soak-Worker`, `X-Soak-Ts`, `X-Soak-Nonce`, `X-Soak-Sig`) and every server response (`X-Soak-Ts`, `X-Soak-Nonce`, `X-Soak-Sig`) is verified over canonical bytes (`method`, `path` / `status`, `worker`, timestamp, nonce, raw body bytes) with `hmac.compare_digest` before JSON parsing.
3. **Replay protection and body bounds:** requests older than 300 s or carrying a reused nonce are rejected (`401`), HMAC verification runs before recording nonces (preventing unauthenticated replay-cache poisoning), and request bodies are bounded to 32 MiB (`413`).
4. **Fixed 4-op command allowlist:** `worker-agent` accepts only verified responses from `SoakControlServer` and executes only `kill_restart`, `link_cut`, `idle_snapshot`, and `shutdown` — never arbitrary shell commands or unauthenticated requests.
5. **Privacy scrubbing:** the secret text, derived dashboard bearer token, `--secret-file` path, local work directories, hostnames, user names, and non-loopback IPv4 addresses are redacted before error logging or writing any JSON record.

### Three-machine 24-hour commands (design 0065 D4.3)

Use your own non-trial ports (never `8080`, `8087`, `8088`, `8089`), a dedicated `--secret-file`, and pass `--worker-python python` when the master OS (`python3`) differs from Windows workers (`python`):

Master machine (macOS):

```bash
python3 benchmarks/endurance/endurance.py soak \
  --bin <MAC_OXIDESWARM_BIN> \
  --worker-python python \
  --master-only \
  --master-listen 0.0.0.0:18500 \
  --control-listen 0.0.0.0:18501 \
  --secret-file <RUN_SECRET_FILE> \
  --expect-workers win-a,win-b \
  --workers 2 --slots 4 --max-queue-size 64 \
  --link lan-tcp --duration 24h --fault-scale 1.0 --load-scale 1.0 --sample-interval 60s \
  --work-dir <PRIVATE_MAC_WORKDIR>
```

Each remote worker machine (Windows PowerShell, private NTFS folder outside `AppData`):

```powershell
python benchmarks\endurance\endurance.py worker-agent `
  --bin <WIN_OXIDESWARM_EXE> `
  --master <MAC_LAN_OR_TAILSCALE_IP>:18500 `
  --control <MAC_LAN_OR_TAILSCALE_IP>:18501 `
  --secret-file <RUN_SECRET_FILE> `
  --name win-a --slots 4 --sample-interval 60s `
  --work-dir <PRIVATE_WIN_WORKDIR>
```

### Loopback two-worker-agent verification (no cross-machine traffic)

Run the Python harness test suite (mocked + optional real-binary two-`worker-agent` loopback test):

```powershell
python tests\test_endurance_harness.py
$env:OXIDESWARM_TEST_BIN = '<PATH_TO_OXIDESWARM_EXE>'; python tests\test_endurance_harness.py
```

