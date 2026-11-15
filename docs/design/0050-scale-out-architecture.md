# 0050: Scale-out architecture (`SCALE-1` to `SCALE-6`, `ECO-3`, `ECO-4`)

Status: Partial
Created: 2026-11-09    Updated: 2026-11-15

## Summary

Today one master runs the whole grid for one owner or one small team. This record states the limits
of that design with evidence, and a staged architecture that grows it to many workers and many
submitters: a load harness first, then a faster scheduler, worker-to-worker blob distribution, a
highly available master, autoscaling providers and, last, federation of sites. It also covers
multi-tenancy and observability, which a shared grid needs. D1 (`SCALE-1`, with baseline records at 100, 500 and 1000 simulated workers), D2 (`SCALE-2`, with post-change records at 500 and 1000 simulated workers) and D7 (`ECO-4`) are built; the remaining stages are planned. Each open choice lists its options, and the owner decides in [ROADMAP.md](../ROADMAP.md) (Q8, Q9, Q12).

## Context

The limits below are read from the code at `master` `09a8990` on 2026-11-09. None is a defect for
the purpose of [ROADMAP.md](../ROADMAP.md) (one owner, a few machines). Each becomes one when the
grid grows.

1. **One master is a single point of failure and a capacity ceiling.** All scheduling, session
   state and the blob store live in one `MasterServer` process (`crates/master/src/server.rs`).
   `--state-dir` (design 0046, `crates/master/src/store.rs`) makes a restart lose nothing, but while
   the master is down nobody submits and nothing is placed. `ClaimMasterRole` is refused (design
   0026, `FEAT-6`), so no standby exists.
2. **Blobs flow through the master.** A worker fetches input blobs from, and uploads output blobs
   to, the master's `BlobStore` (`crates/master/src/blob_store.rs`, `crates/worker/src/blob_client.rs`,
   `crates/core/src/blob_transfer.rs`). Ten workers that need the same 1 GiB tree make the master
   send it ten times. The master's disk and uplink bound the whole grid.
3. **The queue lives in master memory.** `TaskQueue` (`crates/master/src/queue.rs`) holds every task
   in process memory, capped by `--max-active-mb` (256 MiB default), optional `--max-queue-size`
   and the retained-task bounds. The journal is for recovery; it is not a second place to read.
4. **There is no autoscaling.** The grid has the workers that someone started. Nothing starts or
   stops a worker on queue depth.
5. **State that decides placement is one process's memory.** The workspace affinity map
   (`workspace_affinity` in `crates/master/src/queue.rs`, bounded by `MAX_WORKSPACE_AFFINITIES`),
   the resume holds of design 0046 and the placement `Engine` with `hold_back`
   (`crates/master/src/scheduler.rs`) all assume exactly one scheduler. A second master would need
   them replicated or partitioned.
6. **Fairness is by task order, not by submitter.** `WorkloadScheduler::schedule_batch` batches
   ready tasks and `hold_back` stops wide tasks from starving. Nothing shares capacity between
   submitters, because the cluster has one trust domain: whoever holds the cluster secret is an
   admin of the grid. `AuthScope` (`crates/master/src/auth.rs`) covers the dashboard only.
7. **No metrics or traces for operators.** No Prometheus or OpenTelemetry code exists in `crates/`.
   The dashboard (`crates/master/src/web_ui.rs`) shows live state and `doctor` checks one grid.
8. **Single-machine baseline extended to simulated fleets.** `benchmarks/grid/bench_grid.py` (BENCH-1) runs one to four workers on loopback. `SCALE-1` (`benchmarks/grid/scale_harness.py`, `crates/cli/examples/scale_load_driver.rs`) now measures 100, 500 and 1000 simulated workers plus an impaired-link scenario (summarized under D1).

## Goals

1. G1: measure the grid at 100, 500 and 1000 workers, and publish the numbers with the scripts that
   measured them (writing standard rule 6).
2. G2: survive the loss of the master host without losing an accepted task, and without running a
   task twice more often than today.
3. G3: stop the master from being the bottleneck for blob bytes.
4. G4: let several submitters share a grid with limits, identities and an audit trail.
5. G5: let the grid grow and shrink with queue depth through pluggable providers.
6. G6: give operators metrics and traces they already know how to read.

## Non-goals

1. A replacement for Kubernetes, Nomad or Bazel remote execution servers. OxideSwarm stays a
   one-binary grid; D5 only launches workers through them.
2. Exactly-once execution. Delivery stays at least once (`ROADMAP.md`, "What is missing").
3. Untrusted tenants running arbitrary code. `--isolation` (design 0033) limits what a task reads;
   it is not a security boundary against a hostile kernel exploit. D6 states what multi-tenancy
   does and does not promise.
4. A new wire protocol family. Every change here uses the versioning rules of design 0040.

## What must stay true

Every stage below keeps these. A stage that cannot keep one is not accepted.

1. **Security model.** Every new path runs behind the cluster handshake (`cluster_auth.rs`,
   `AUTH_VERSION` 2) and encrypted records. Master-to-master and worker-to-worker links use it too;
   there is no unauthenticated peer port. `cluster.secret` is never carried by a ticket, a log or a
   blob.
2. **Protocol versioning.** `PROTOCOL_VERSION_MIN_COMPATIBLE == PROTOCOL_VERSION_CURRENT` stays
   the rule, with `ProtocolCapabilities` bits for optional abilities. A change to a message adds
   the variant to the public enum, both mirrors and all four `From` impls, and to the
   golden-bytes layout tests (`crates/core/src/wire_layout_tests.rs`).
3. **Durability.** A task the master acknowledged is in the journal before the acknowledgement
   (design 0046). Replication, where used, acknowledges after a quorum, not after one disk.
4. **Single-machine mode.** `oxideswarm master` with no flags on a laptop works as it does today. Every
   scale-out feature is opt-in and compiles out or stays idle by default.
5. **Honest status.** A number needs its script and date. A feature is Planned until a test or a
   dated run shows it.

## Design

### D1: Load harness (`SCALE-1`, Implemented)

A simulated worker is `WorkerClient::simulated` (`crates/worker/src/client.rs`, `crates/worker/src/sim.rs`)
paired with `FakeRunnerConfig` (`crates/worker/src/runner.rs`) and `SimulatedLinkStream`, so 1000 of
them run in `crates/cli/examples/scale_load_driver.rs` against one real `oxideswarm master`
subprocess (`OXS-AUTH` v2 handshake, ChaCha20-Poly1305 record layer, protocol v8). Each worker
registers, heartbeats, receives tasks, and answers after a configurable delay with optional link
base latency, FIFO jitter, packet-loss stalls, and `disconnect_all()` mass reconnect injection.
`benchmarks/grid/scale_harness.py` drives the scenarios under BENCH-1 rules and writes sanitized JSON
records with date, commit, hardware, OS, network, and 3 raw runs.

The `2026-11-14` baseline (`3d6b35eb`, Core i5-14600KF, 20 logical CPUs, `47.79` GiB RAM, Windows
`amd64`, [benchmarks/grid/README.md](../../benchmarks/grid/README.md)) decides which of D2 and D3
matter first:

- Control plane, heartbeats (`336.8 acks/s`, `42.7 KB/s` at 1000 workers), mass reconnect (`641.5 ms`
  for 1000 workers), steady master RSS (`25.7` / `55.7` / `87.6 MiB` at 100 / 500 / 1000 workers),
  and journal replay (`19.5 ms` in-process for `1.37 MB`) stay well within single-master bounds up
  to 1000 workers.
- Simultaneous `100 x 10 MiB` input-blob fan-out spikes master RSS from `25.7 to 87.6 MiB` to
  `507.4 to 676.7 MiB` median (up to `845.3 MiB` max) and drops to `39.23 MiB/s` (`25.5 s`,
  `1537.5 %` peak master CPU) over a `25 ms + 10 ms`, `1 %`-loss simulated WAN link, pointing
  directly to `SCALE-3` (D3 worker-to-worker blob distribution and bounded master transfer buffers)
  as the primary data-plane bottleneck, alongside `SCALE-2` (D2 scheduler indexing, cold terminal
  result eviction, and fair share) for multi-submitter queues.

### D2: Scheduler and queue at scale (`SCALE-2`)

Open options, in order of cost:

| Option | What changes | Trade-off |
|---|---|---|
| A. Index and batch | Keep one scheduler. Index workers by OS, arch, tag and free slots so a pass does not compare every task with every worker (the harness shows whether it does today). Raise the micro-batch size. Move cold terminal results out of memory into the store | Smallest change. One process stays the limit |
| B. Fair share | A queue per submitter (D6 identity) with weighted round robin before placement | Needs D6. Prevents one submitter from filling the queue |
| C. Partitioned schedulers | Shard workers and queues by key across several master processes | Breaks the single-process assumptions of Context 5. Only if A and B miss the Q8 target |

Recommended order: A, then B with D6, then C only when the harness shows A and B fall short.
Option A keeps the 8-state machine (`TaskQueue::can_transition_to`) and the `Engine::hold_back`
rule that keeps wide tasks from starving.

Implemented (`SCALE-2`): `WorkerIndex` and `RegisteredIndex` in `crates/master/src/worker_index.rs`
maintain `O(W / 64)` `u64` bitsets over OS, architecture, worker id/name, isolation level, tools,
tags, GPU capability, protocol features (including `WORKSPACES` and `TREE_V2`), free slots, and
mobile thermal/battery buckets for fast candidate filtering and `fail_unfulfillable_tasks` checks.
`WorkloadScheduler` (`crates/master/src/scheduler.rs`) scans `SCAN_PAGE = 512` ready tasks per step,
`MasterServer::spawn` raises the default batch cap to `DEFAULT_MASTER_MAX_BATCH_SIZE = 1024`
(configurable via `master --max-batch-size`, `RUSTY_GRID_MAX_BATCH_SIZE`, and
`[master].max_batch_size`), and both scheduler loops use adaptive micro-batch debounce plus a
bounded multi-batch drain (`MAX_CONSECUTIVE_BATCHES = 16`). `FairShareQueues` in
`crates/master/src/fair_share.rs` interleaves queued submissions per `SubmitterId` with weighted
round-robin while preserving priority ordering, wide-task hold-back, and `SpeedBook`
hold-back. Dashboard broadcast sites guard on `b_tx.receiver_count() > 0` before calling
`get_task`/`stats`, and `ColdResultStore` (`crates/master/src/cold_results.rs`, stored under
`<state_dir>/cold_results/<xx>/<task_id>.bin` in `OXSWCLD1` format) spills terminal `TaskResult`
payloads past the hot limits (`DEFAULT_MAX_HOT_TERMINAL_TASKS = 1024` /
`DEFAULT_MAX_HOT_TERMINAL_BYTES = 4 MiB`) via single-flight `spawn_cold_maintenance` / `put_batch`
with parallel staging, per-shard directory caching, and byte-exact file reuse on `journal.bin`
replay while keeping `GetTaskStatus` RAM-only via `TaskStatusDetail`.
The `2026-11-15` post-`SCALE-2` runs on the same Core i5-14600KF host (`ec228a01`,
[benchmarks/grid/README.md](../../benchmarks/grid/README.md)) achieve `409.1` tasks/s at `500w`
(`+9.0 %` over `375.2` baseline, unloaded assign p50 `16.69 ms` vs `31.01 ms`, steady RSS
`57.2 MiB`, `51.36 ms` replay) and `414.4` tasks/s at `1000w` (unloaded assign p50 `17.67 ms`,
steady RSS `91.8 MiB`, `55.52 ms` replay) with `0` failed tasks.

### D3: Worker-to-worker blob distribution (`SCALE-3`)

Workers already keep a bounded BLAKE3 cache (`crates/worker/src/blob_cache.rs`). The master becomes
a tracker and a fallback:

1. A worker advertises the blob ids it holds, as a delta, through a `ProtocolCapabilities` bit and
   a new message pair (`BlobHave`, `BlobWant`), sent on the control stream.
2. When a task needs a blob the worker lacks, the master replies with a short list of peers that
   hold it. The worker fetches from a peer over a direct authenticated link in 4 MiB chunks, verifies
   the BLAKE3 id as today, and falls back to the master when no peer answers.
3. A peer link reuses the cluster handshake with a per-peer key derived from the secret, and the
   master never reveals a peer address to a worker that is not registered.

Open choice: peer links over iroh (NAT traversal, one more dependency path) or plain TCP inside one
site. Recommended: iroh when the master is P2P, TCP otherwise. Trade-off: peer links open worker
listeners, which Windows and mobile workers may not allow; they stay clients only and use the
master fallback. Output blobs still go to the master first; a later step may keep them on the
producing worker and record only the id.

### D4: Master high availability (`SCALE-4`, supersedes the Design-only `FEAT-6`)

`FEAT-6` is Implemented as a refusal (design 0026). This item replaces that note with a design and
then code.

| Option | How | Trade-off |
|---|---|---|
| A. Active-passive with a fencing epoch | One leader writes the journal. A standby follows it (journal shipping) and takes over when a lease expires. Every task assignment carries the epoch, and a worker refuses a lower one | Small. Failover time is the lease length. Needs one agreed lease holder (a shared store, or a third voter), or split brain is possible |
| B. Raft-replicated journal | The journal of design 0046 becomes a Raft log across three or five masters. A commit needs a majority | No split brain, and a quorum survives one loss. Adds a consensus dependency and the cost of a log write across a network on every state change |

Both build on design 0046: the journal already records submissions, transitions, checkpoints and
results, and replay returns `Scheduled` and `Running` tasks to `Queued`. Workers already resume a
session after a short disconnect (`ResumeSession`); they would also learn the epoch and the list of
master addresses, and dial the next address when one fails.

Recommended default: A first, to reach a working failover with a small change, with the journal
format and the epoch fixed so B can replace the replication layer later. The owner decides in Q9.
Either way: a standby never serves submissions as a second leader, a deposed leader stops placing
tasks the moment it sees a higher epoch, and the harness (D1) includes a kill-the-leader scenario
whose pass condition is no lost task and bounded duplicate runs.

### D5: Autoscaling providers (`SCALE-5`)

A provider is a small interface: `start(n, labels)`, `stop(worker_id)`, `list()`. The master (or a
separate `oxideswarm scaler` process, so the master stays small) reads queue depth and the free
capacity per tag, and asks a provider to add or remove workers within `min`, `max` and a cooldown.
A started worker receives a join token (design 0048) that is scoped and short-lived.

Providers, in the order the owner can choose (Q12):

| Provider | Starts a worker by | Note |
|---|---|---|
| SSH pool | `ssh host oxideswarm worker ...` on machines the owner lists | No new infrastructure. First |
| Cloud VM | One cloud API call and a boot script | Per cloud; costs money, so a hard `max` and a budget line are required |
| Kubernetes | A `Deployment` or `Job` whose replica count the scaler sets | The grid runs inside a cluster the owner already has |

Scale-down only picks idle workers and drains them with `WorkerMessage::Disconnecting` (design 0036),
which does not charge retries.

### D6: Multi-tenancy and quotas (`ECO-3`)

Today the cluster secret is one trust domain. Tenants add identity above it:

1. A **tenant** and a **project** name every task (`Task` fields, protocol bit). Clients present a
   scoped token derived from the secret or issued by the master, not the secret itself.
2. **Quotas** per tenant: concurrent tasks, queued bytes, CPU-seconds per day, and the blob store
   share. Admission refuses with a typed error, as `--max-queue-size` does.
3. An **audit log**: who submitted, cancelled or read what, written to the journal store and
   exported as append-only lines.
4. Placement constraints per tenant (a tenant may be limited to workers with a `tenant:` tag).

What it does not promise: isolation between mutually hostile tenants. A task still runs as the
worker's OS user, limited by `--isolation`. Tenants share a trust level of "not hostile", and the
guide says so. Fair share (D2 option B) uses the tenant as the submitter key.

### D7: Observability (`ECO-4`)

`GET /metrics` on the dashboard router (`crates/master/src/web_ui.rs`, `metrics.rs`) exposes
Prometheus text exposition (`text/plain; version=0.0.4; charset=utf-8`), guarded by
`AuthScope::Observer` in `crates/master/src/auth.rs`:

- Queue & throughput: `oxideswarm_queue_tasks{state}` (`queued`, `scheduled`, `running`,
  `retrying`, `completed`, `failed`, `cancelled`), `oxideswarm_queue_active_bytes`,
  `oxideswarm_queue_max_active_bytes`, `oxideswarm_tasks_submitted_total`,
  `oxideswarm_tasks_terminal_total{state}` (`completed`, `failed`, `cancelled`),
  `oxideswarm_tasks_per_second` (60-second sliding window anchored to creation `Instant`), and
  `oxideswarm_scheduling_latency_seconds` histogram (`_bucket`, `_sum`, `_count`).
- Workers (`state` × `link`): `oxideswarm_workers{state,link}` (`connected`, `busy`,
  `disconnected` × `tcp_lan`, `quic_direct`, `quic_relay`, plus `other` when present; live
  `iroh::endpoint::Connection` inspection overrides tags for P2P workers).
- Blob store & durable journal (`--state-dir`): `oxideswarm_blob_bytes_in_total`,
  `oxideswarm_blob_bytes_out_total`, `oxideswarm_blob_evictions_total`,
  `oxideswarm_blob_store_bytes`, `oxideswarm_blob_store_max_bytes`,
  `oxideswarm_blob_store_objects`, `oxideswarm_journal_bytes`, `oxideswarm_journal_records`,
  `oxideswarm_journal_fsync_duration_seconds` histogram, `oxideswarm_store_configured` (`1`/`0`),
  and `oxideswarm_store_healthy` (`1` healthy, `0` after any write failure).

Opt-in OpenTelemetry tracing (`--features otel` on `rusty_grid_master` / `rusty_grid_cli`, zero
new external packages in `Cargo.lock`) exports `oxideswarm::otel` lifecycle spans (`submit`,
`place`, `assign`, `run`, `result`) over OTLP/HTTP JSON (`POST /v1/traces`, configured via
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` or `OTEL_EXPORTER_OTLP_ENDPOINT` plus `OTEL_SERVICE_NAME`)
in `crates/master/src/otel.rs` and `crates/cli/src/cli_app.rs`. `traceId` and `parentSpanId` are
deterministically derived from `task_id` via keyed BLAKE3, and only a closed attribute allowlist
(`task_id`, `worker_id`, `priority`, `requires_gpu`, `verdict`, `outcome`, `exit_code`, `attempt`)
is captured. Endpoint parsing rejects embedded URL credentials without echoing secrets, and cluster
secrets, bearer tokens, worker IPs, task commands/inputs, and store error strings never appear in
Prometheus labels or OTLP attributes.

### D8: Federation of sites (`SCALE-6`, last, optional)

Several sites, each a grid with its own master, with a thin layer that forwards a task to another
site when the local one lacks a capability or capacity. Open options: a coordinator that holds only
metadata and routes (small), or masters that peer and forward directly (no extra component, more
protocol). Blobs move site to site by id, so a blob a site already has is not sent. This waits for
D2, D3 and D4, because it needs a stable master identity, an epoch and a blob tracker. It is built
only if the owner's Q8 target needs more than one site.

## Compatibility

- Each stage adds protocol feature bits and a version bump when a message changes
  (`PROTOCOL_VERSION_CURRENT`), with the upgrade order of `DEPLOYMENT.md` 3.5: master first.
- D4 changes the master address list in `Register`/`RegisterAck`; an old worker keeps one address and
  works with a single leader.
- Journal format changes (D4, D6) are versioned in `crates/master/src/store.rs`; a master refuses a
  journal newer than it understands rather than discarding it.
- `/metrics`, providers and the scaler are additive and off by default.

## Security

1. Peer links (D3) and master links (D4) use the cluster handshake and AEAD. A new peer port is
   never unauthenticated.
2. Scoped tokens (D6) are derived with a new HMAC label (`oxs1-...` style, one per purpose) so a
   token for one purpose is useless for another.
3. A scaler holds cloud credentials or SSH keys. They stay in the scaler's own files at mode 600,
   not in `cluster.secret`, not in a task environment, and not in the join token it hands out.
4. A federated site (D8) is a separate trust domain: it gets its own secret and a narrow forward
   permission.
5. A scheduler or quota bug can starve a tenant; D1 includes a fairness scenario so it is tested.

## Test plan

- Unit: placement index (D2), quota admission (D6), epoch comparison (D4), peer ordering (D3).
- Integration: kill the leader mid-run (D4) with no lost task; two workers fetch one blob and the
  master sends it once (D3); a tenant over quota is refused (D6).
- Load: the harness (D1) at 100, 500 and 1000 workers, recorded with date and hardware. A claim in
  `ROADMAP.md` or `README.md` links its record.
- Live: a scale-up and a scale-down through the SSH pool provider on two real machines, recorded.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| SCALE-1 | Load harness with simulated workers (D1) | `benchmarks/grid/scale_harness.py`, `crates/cli/examples/scale_load_driver.rs`, `crates/worker/src/sim.rs` | Dated records at 100, 500 and 1000 workers are committed | Implemented |
| SCALE-2 | Scheduler and queue at scale (D2) | `crates/master/src/{worker_index,fair_share,cold_results,scheduler,queue,server,store}.rs` | The SCALE-1 numbers improve by the Q8 target, with no change in placement results | Implemented |
| SCALE-3 | Worker-to-worker blob distribution (D3) | `crates/core/src/blob_transfer.rs`, `crates/worker/src/blob_client.rs`, `crates/master/src/blob_store.rs` | A test shows the master sends one blob once to N workers | Planned |
| SCALE-4 | Master high availability (D4) | `crates/master/src/store.rs`, `server.rs`, `crates/worker/src/client.rs` | Killing the leader loses no accepted task | Planned |
| SCALE-5 | Autoscaling providers (D5) | new `crates/` module or bin | A queue backlog starts workers and idle time drains them, recorded live | Planned |
| SCALE-6 | Multi-site federation (D8) | to be designed | A task reaches a worker in another site | Planned |
| ECO-3 | Multi-tenancy and quotas (D6) | `crates/core/src/task.rs`, `crates/master/src/auth.rs`, `queue.rs` | An over-quota submit is refused and the audit log shows it | Planned |
| ECO-4 | Observability (D7) | `crates/master/src/metrics.rs`, `otel.rs`, `web_ui.rs`, `auth.rs`, `queue.rs`, `scheduler.rs`, `store.rs`, `crates/cli/src/cli_app.rs` | A Prometheus scrape returns the listed series | Implemented |

Order: SCALE-1 first, then SCALE-2 and SCALE-3 as the numbers direct, SCALE-4, SCALE-5, and
SCALE-6 only on need. ECO-3 and ECO-4 can start after SCALE-1.

## Status log

- 2026-11-09: Created as Draft at the owner's request to grow OxideSwarm into a large project.
  Nothing is built. The owner's decisions Q8, Q9 and Q12 in `ROADMAP.md` fix the target scale, the
  HA approach and the first providers.
- 2026-11-14: Implemented ECO-4 (D7): added authenticated Prometheus `GET /metrics`
  (`AuthScope::Observer`) in `crates/master/src/metrics.rs`, `web_ui.rs`, `auth.rs`, `queue.rs`,
  and `store.rs`, plus opt-in zero-SDK `--features otel` OTLP/HTTP JSON trace export across
  `submit`, `place`, `assign`, `run`, and `result` lifecycle spans seeded deterministically from
  `task_id` in `crates/master/src/otel.rs`, `scheduler.rs`, and `crates/cli/src/cli_app.rs`
  (`tests/observability_metrics_otel_test.rs`).
- 2026-11-14: Implemented SCALE-1 (D1) in pull request 222: WorkerClient::simulated, FakeRunnerConfig, SimulatedLinkStream (crates/worker/src/sim.rs, crates/worker/tests/scale1_simulated_worker.rs), scale_load_driver (crates/cli/examples/scale_load_driver.rs), benchmarks/grid/scale_harness.py (tests/test_scale_harness.py), and 3-run baseline records at 100, 500 and 1000 loopback workers plus 100 workers on a 25 ms + 10 ms, 1 % loss simulated link (benchmarks/grid/results/2026-11-14-scale1-windows-amd64-*.json). Status changed from Draft to Partial.
- 2026-11-14: Implemented D2 (`SCALE-2`) scheduler bitset index (`WorkerIndex` and `RegisteredIndex`), `SCAN_PAGE = 512` and `DEFAULT_MASTER_MAX_BATCH_SIZE = 1024` (`--max-batch-size`), adaptive micro-batch debounce and multi-batch drain, per-submitter `FairShareQueues`, dashboard receiver guards, and `ColdResultStore` disk spill. Marked Partial until the 500-worker / 20-submitter `SCALE-1` harness run is recorded after PR #222 lands.
- 2026-11-15: Finalized D2 (`SCALE-2`) in pull request 227: added `TREE_V2` bitset indexing across `WorkerIndex` / `RegisteredIndex` and `ready_views_fair`, raised `DEFAULT_MAX_HOT_TERMINAL_TASKS` to `1024`, replaced overlapping cold-flush tasks with single-flight `spawn_cold_maintenance` + batched `ColdResultStore::put_batch` + uncompacted `journal.bin` replay file reuse, and recorded post-`SCALE-2` 3-run benchmarks at `500w` (`409.1 tasks/s`, `16.69 ms` unloaded assign p50, `57.2 MiB` steady RSS, `51.36 ms` replay, `benchmarks/grid/results/2026-11-15-scale2-windows-amd64-500w.json`) and `1000w` (`414.4 tasks/s`, `17.67 ms` unloaded assign p50, `91.8 MiB` steady RSS, `55.52 ms` replay, `benchmarks/grid/results/2026-11-15-scale2-windows-amd64-1000w.json`). Marked `SCALE-2` Implemented.
