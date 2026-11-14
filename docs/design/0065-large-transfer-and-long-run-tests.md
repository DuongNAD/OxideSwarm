# 0065: Tests for large transfers and long runs

Status: Draft (plan; builds nothing; Q57 to Q59 are open)
Created: 2026-11-14    Updated: 2026-11-14

## Summary

This record plans two test harnesses and the records they produce. The first moves large files and
large trees between real machines. It checks that they arrive intact, how long they take, and that
memory does not grow with the size of a file. The second keeps a cluster busy for 6 to 24 hours
while it injects faults. It checks that no task is lost or run twice, and that memory, handles, disk
and latency level off. A short version of both runs on a schedule in CI. Nothing in the product
changes in this record.

## Context

### What the tests cover today

1. **Size.** A search of the Rust tests on 2026-11-14 at `master` `f486885` found these largest
   payloads:
   - In the end-to-end blob tests, the largest file is 5 MiB plus 123 bytes
     (`tests/blob_transfer_e2e.rs:183`).
   - The largest tree is 10,000 small files (`tests/blob_upload_pipelining_e2e.rs`).
   - An 11 MiB output is used only to check that the output limit fails the task
     (`crates/worker/tests/test_bug2_output_failure.rs:195`).
   - Codec tests send single frames of up to the 64 MB frame limit
     (`crates/core/tests/memory_bench.rs`). That is the framing path, not the blob path.
2. **Time.** Every CI job has a timeout of at most 90 minutes (`.github/workflows/ci.yml`).
   The longest script outside CI, `benchmarks/grid/bench_grid.py`, takes about 10 minutes (its
   README). No committed record measures the memory, handles or disk of a cluster over hours.
3. **Neighbouring items.**
   - BENCH-1 measures no-op task overhead and a repeated upload on loopback.
   - Check C8 of `oxideswarm doctor` sends an 8 MiB blob to each worker (LINK-1).
   - SCALE-1 (Planned) covers many simulated workers. It does not cover large data or long time.

### What the code does with large data

| Part | Behaviour | Where |
|---|---|---|
| Chunks | A blob moves in chunks of 4 MiB. Each chunk is one request. The next chunk of the same blob waits for the reply to the last one. Eight blobs move at a time | `crates/core/src/blob_transfer.rs`: `BLOB_CHUNK_BYTES`, `upload_blob`, `download_blob`, `TRANSFER_CONCURRENCY` |
| Missing query | One query names at most 65,536 blobs | `blob_transfer.rs`: `MAX_MISSING_QUERY` |
| Master store | It holds 4 GiB by default (`master --blob-store-mb`). It refuses a blob larger than the whole store, with a message that names the flag. An upload reserves its whole size when it starts | `crates/master/src/blob_store.rs`: `DEFAULT_BLOB_STORE_MAX_BYTES`, `write_chunk` |
| Interrupted upload | An upload in progress belongs to one connection. A new connection must start at offset 0, so a dropped upload sends the whole blob again. `tmp/` is emptied when the master starts | `blob_store.rs`: `write_chunk`, module docs |
| Worker cache | It holds 2 GiB by default. It evicts least recently used blobs after each task and never evicts a pinned one. An input is copied into the sandbox, so during a task it takes about twice its size on the worker's disk | `crates/worker/src/blob_cache.rs`: `DEFAULT_BLOB_CACHE_MAX_BYTES`, module docs |
| Worker fetch | Each reply waits at most 120 s. A cancelled fetch removes its partial file. Inputs are fetched in `client.rs` before `TaskRunner` starts the task's timeout | `crates/worker/src/blob_client.rs`: `BLOB_REPLY_TIMEOUT`, test `cancelling_fetch_task_inputs_removes_partial_temp_file`; `crates/worker/src/client.rs` |
| Outputs | A task may bring back 1 GiB of output files by default (`worker --output-limit-mb`). More fails the task | `crates/worker/src/staging.rs`: `DEFAULT_OUTPUT_LIMIT_BYTES` |
| Heartbeats | The master's defaults are a heartbeat every 3 s and a timeout of 10 s. On QUIC, blob traffic uses the data stream. On TCP, the worker writes queued control messages before the next data frame, but a frame already on the wire is not interrupted | `crates/cli/src/cli_app.rs`; `blob_transfer.rs` module docs; `client.rs`: `lane_writer` |

One consequence follows from the chunk rule, by arithmetic and not by measurement. A single blob
moves at most 4 MiB per round trip. At 100 ms that is at most 40 MiB/s. At 300 ms it is at most
about 13 MiB/s. This limits one large file over a relay. It does not limit many small files, which
move eight at a time.

### Why now

- [Design 0062](0062-model-weights-into-remote-memory.md) plans to move model weights of several
  GB from one machine into the memory of another. [Design 0061](0061-first-application-model-tunnel.md)
  plans a model tunnel. Both need measured numbers for large transfers first.
- The USB kit ([design 0053](0053-usb-setup-kit.md)) runs workers for days on other people's
  machines. A slow leak would show there first.

### Questions the code does not answer

1. Does the memory of the client, the master and the worker stay flat when a file grows from
   1 GiB to 8 GiB?
2. How long does a 3 GiB input take over LAN TCP, LAN QUIC and a relay? How close is it to the
   bound above?
3. What happens to an input larger than the worker cache? The code suggests it is used, evicted
   after the task and fetched again by every later task. This is not measured.
4. How many bytes go again after a dropped connection? The code says the whole blob.
5. Can a slow TCP link delay a heartbeat past the 10 s timeout during a transfer, so that a busy
   worker is reaped?
6. Over hours, do memory, open handles, disk use (store, cache, sandboxes, journal) and latency
   level off?

## Goals

- G1: Answer questions 1 to 5 with dated records from real machines, on LAN and through a relay.
- G2: Show that memory per process does not grow with the size of a file.
- G3: Show that a cluster under load and faults for 24 hours accounts for every task. No task may
  be lost or have two results.
- G4: Show that memory, handles, disk use and latency level off during that run.
- G5: Turn every defect the runs find into a regression test in the normal suite.

## Non-goals

- Many workers. That is SCALE-1.
- Faster or resumable transfers. A finding that needs one gets its own design record (Q58).
- Any change to the wire format, a CLI flag or a default.
- A required check on pull requests.
- A comparison of network products. That is `benchmarks/interconnect/`.

## Design

### D1: One harness, two scenarios

1. Add `benchmarks/endurance/endurance.py` with the subcommands `transfer`, `soak`, `throttle`
   and `sample`. It uses only the Python 3 standard library.
2. It follows the rules of `benchmarks/grid/bench_grid.py`:
   - It starts its own master and workers on ephemeral ports.
   - Every process it starts gets a private `HOME` and `USERPROFILE`.
   - It stops processes through the handles it holds, never by name.
   - It deletes its temporary directory.
   - It writes one JSON record under `benchmarks/endurance/results/`. The record holds the date,
     commit, binary version, hardware, operating system, network, parameters and every raw sample.
     It holds no host name, IP address or user name.
3. **Two machines.** The master runs on one machine, started for the run with a temporary `HOME`
   and ports that differ from any other cluster on the machine. Workers on other machines join it
   with a secret created for the run. The harness never uses `~/.oxideswarm` or the trial cluster.
4. **Data.** Files are random bytes from a seeded generator. Both sides check them by BLAKE3
   without keeping a second copy.
5. **Samples.** `sample` runs on each machine. For each process the harness started, it records
   resident memory, open handles, threads and CPU time. It also records the sizes of the blob
   store, the worker cache, the sandbox base and the state directory. It reads open handles from
   `/proc/<pid>/fd` on Linux, `lsof -p` on macOS and `Get-Process` on Windows. On Windows it also
   records whether real-time antivirus scanning is on.
6. **Throttle.** `throttle` is a TCP proxy on `127.0.0.1` with a set bandwidth and delay. It runs
   in user space and needs no root. It sits between a worker and the master. QUIC is not
   throttled; it is measured on real links only.
7. **Disk check.** Before a run the harness refuses to start when a machine has less free space
   than three times the largest file plus 10 GiB. That covers the store, the cache and the
   sandbox copy.

### D2: Transfer scenarios (`endurance.py transfer`)

| ID | Scenario | Pass |
|---|---|---|
| T1 | One input file of 1, 3 and 8 GiB. The 8 GiB run raises `--blob-store-mb` | The BLAKE3 hash matches at the far end. For each process, peak memory at 8 GiB is within 128 MiB of peak memory at 1 GiB |
| T2 | A tree of 100,000 files of 1 to 16 KiB, which needs more than one missing query | Every file arrives with its hash. Peak memory is recorded |
| T3 | Outputs of 1,000 MiB and 1,100 MiB with the default output limit | The first task completes. The second fails with the output-limit message |
| T4 | The same 3 GiB input submitted twice | The second submission uploads no blob bytes |
| T5 | A client upload cut at about half, then submitted again | The task completes. The bytes sent again are recorded |
| T6 | A worker's link cut during an input fetch | The task is requeued and completes. No partial file is left in the cache's `tmp/` |
| T7 | A TCP link throttled to 2 Mbit/s with 50 ms delay, a 256 MiB input and a 256 MiB output | No worker is reaped during the transfer, and the task completes |
| T8 | An input larger than the store, and two uploads that together exceed it | Both are refused with a message that names `--blob-store-mb`. Nothing is left in the store's `tmp/` |
| T9 | A 3 GiB input, larger than the worker cache, used by three tasks in a row | All three complete. The bytes fetched per task and the worker's peak disk use are recorded |

Each scenario that moves data runs on four links: loopback, LAN over TCP, LAN over direct QUIC, and
QUIC through a relay. The record holds the throughput and the round-trip time of each link. It
compares them with the bound of 4 MiB per round trip.

### D3: Soak scenario (`endurance.py soak`)

1. **Cluster.** One master with `--state-dir` and two or three workers, at least one of them on
   Windows. A short run lasts 1 hour. A long run lasts 24 hours.
2. **Load per minute.** The load comes from a seeded generator, so a run can be repeated:
   - 30 no-op tasks;
   - 5 tasks with a new input of 10 to 100 MiB, and 5 with an input used before;
   - 2 tasks with a 50 MiB output;
   - 1 workspace sync task, when the workers run with `--workspaces`;
   - 1 cancel of a running task.
3. **Faults.**
   - Every 20 minutes, kill one worker and start it again after 60 s.
   - Every 2 hours, restart the master on the same state directory.
   - Every 30 minutes, cut one worker's link for 30 s through `throttle`.
   - Once an hour, submit a burst up to the queue cap (`master --max-queue-size`, set for the
     run).
4. **Samples.** Every 60 s, the samples of D1 and one probe task, timed from submit to result.
5. **Ledger.** The harness records every task it submits and the result it expects. At the end it
   checks that each task reached exactly one terminal state and counts the retries.
6. **Pass.** These thresholds are starting values. The first record may change them, with the
   reason in the status log.
   1. Every task is accounted for. It completed with the expected hash, or it failed or was
      cancelled for a reason the harness caused. No task is lost, and none has two results.
   2. For each process, mean memory in the last hour is within 10 % or 32 MiB, whichever is
      larger, of mean memory in the second hour. Open handles are within 10 %.
   3. The blob store stays within its bound plus pinned bytes. The worker cache is within its
      bound after each task. When the cluster is idle, the sandbox base holds only the cache. The
      journal shrinks after each compaction.
   4. The probe's p99 in the last hour is at most 1.5 times its p99 in the second hour.
   5. After the stop, no process the harness started is left, and no task process tree remains.

### D4: Where it runs

1. **In the normal suite.** Each defect a run finds gets a regression test that fails without the
   fix (rule 4 of [BACKLOG.md](../BACKLOG.md)).
2. **Short run in CI.** A job `endurance` on `ubuntu-latest` runs on `workflow_dispatch` and on
   the weekly schedule, like the job `p2p-wan-relay`. It never runs on pull requests and is not a required
   check. It runs T1 at 1 GiB, T2 at 20,000 files, T4, T6, T7 and T8 on loopback, and a soak of
   45 minutes. It uploads the JSON record as an artifact.
3. **Full run on the owner's machines.** A Mac master and two Windows workers, with temporary
   homes and their own ports. T1 to T9 over LAN and through a relay, and a soak of 24 hours. The
   records are committed under `benchmarks/endurance/results/`. The machines must stay awake on
   mains power (Q57).

### D5: Order

1. After release 0.2.0. The harness does not depend on protocol 8.
2. Before LLM-12 and LLM-13 of design 0062, and before the USB kit goes to more people.
3. TEST-24 (transfers) comes first. TEST-25 (soak) reuses its sampler and throttle. TEST-26 (CI)
   comes last, because it runs short versions of both.

## Compatibility

Nothing changes in the wire format, the CLI or a default. The harness uses existing commands only:
`submit` with `--input` and `--output`, `workers`, `status`, `cancel` and `result`. A record names
the binary version it ran.

## Security

- The data is random. Records hold no secret, host name, IP address or user name.
- Every process gets a private home. The harness never reads or writes `~/.oxideswarm`.
- The two-machine run uses a secret created for the run. It never joins the trial cluster.
- `throttle` binds `127.0.0.1` only. It forwards to the one address given on its command line.
- No run uses a machine of the trial users.

## Test plan

- `tests/test_endurance_harness.py` checks the helpers without a binary, like
  `tests/test_bench_grid.py`:
  - the seeded generator gives the same bytes twice;
  - the ledger finds a lost task and a task with two results;
  - the threshold arithmetic of D3;
  - `throttle` forwards every byte and keeps to its rate within 10 %.
- The CI job `agent-bridge` runs it with the other Python tests.
- The harness is accepted when the short run is green twice on the schedule and the first full
  record is committed.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| TEST-24 | Large-transfer harness and first records | `benchmarks/endurance/endurance.py` (`transfer`, `throttle`, `sample`), `benchmarks/endurance/README.md`, `tests/test_endurance_harness.py` | Records of T1 to T9 are committed for loopback and for two machines over LAN and a relay. Every failed check has its own item or regression test | Planned |
| TEST-25 | Soak harness and a first 24-hour record | `benchmarks/endurance/endurance.py` (`soak`) | A 24-hour record on a Mac and two Windows machines is committed. It passes D3, or every failed check has its own item | Planned |
| TEST-26 | Short run on a schedule in CI | `.github/workflows/ci.yml` | The job is green on two scheduled runs | Planned |

## Decisions for the owner

The right column is a recommended default, not a decision. They are listed in
[ROADMAP.md](../ROADMAP.md) as Q57 to Q59.

| ID | Question | Recommended default |
|---|---|---|
| Q57 | Does the 24-hour soak run on the owner's three machines (a Mac and two Windows machines, awake on mains power), apart from the trial cluster? | Yes, one run over a weekend after 0.2.0 |
| Q58 | If T5 shows that a dropped transfer sends the whole blob again, do uploads resume from the last chunk? That is a change to the master store and needs its own design record | Decide from the T5 and T9 records over a relay. Change nothing before them |
| Q59 | Is 8 GiB the largest file of T1, or the size of a model the owner plans to run (design 0062)? | 8 GiB first. Add the model's size when design 0062 names one |

## Risks

- A full run needs a lot of free disk on top of build directories. The disk check of D1 refuses to
  start rather than fill a disk.
- `throttle` shapes TCP only. QUIC results come from real links and vary with them.
- A 24-hour run on laptops can be cut by sleep or updates. The record then says where it stopped,
  and the run counts only when it finished.

## Status log

- 2026-11-14: Draft created at the owner's request, after a progress check found that no test
  moves more than a few MiB through the blob path or runs for hours. It builds nothing. It adds
  TEST-24 to TEST-26 (Planned) and decisions Q57 to Q59.
