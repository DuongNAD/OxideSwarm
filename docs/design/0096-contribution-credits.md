# 0096: Contribution credits per participant, display only (NET-10)

Status: Accepted (Option B implemented on draft pull request 379; Wire change: deferred to the protocol 11 train with BUG-22 and ECO-9)
Created: 2026-11-19    Updated: 2026-11-19

## Summary

Adds a bounded, append-only contribution ledger on the master (`crates/master/src/credits.rs`) so
each participant in a circle sees the tasks, core-seconds, and GPU-seconds their machines **ran
for others**, their tasks **used from others**, and their **self-run** work over the last day,
the last week, and all time (`NET-10`, Stage 1 of [design 0094](0094-road-to-a-shared-research-grid.md)).
Double-entry conservation stays internal (`sum(ran_for_others) == sum(used_from_others)` across all
participants, with self-run isolated separately and not counted as "for others"); no signed net
column or `"balance"`/`"debited"` terminology is shown (Q96). The ledger keys persisted records on
immutable member identities (`member_id` hex, or `"organiser"` for v2/local peers) and resolves
participant labels at display time so `NET-10` does not wait for `ECO-9` and [design 0094](0094-road-to-a-shared-research-grid.md)
needs no amendment.

Per coordinator decision (2026-11-19, **Option B**), the master ledger, `GET /api/credits`
(`AuthScope::Observer` showing labels only; `AuthScope::Admin` seeing `member_ids`),
`--max-credit-records` / `--max-credit-bytes`, and the dashboard Q96 panel ship with zero wire or
store-format change on draft pull request 379 (holding as a draft for post-stability merge). Wire
change: deferred to the protocol 11 train with BUG-22 and ECO-9.

## Context

Read against `master` (`f6554a72`) on 2026-11-19:

- **Task execution facts:** When a task finishes on a worker, the worker returns `TaskResult`
  (`crates/core/src/task.rs`: `worker_id`, `task_id`, `exit_code`, `execution_time_ms`), while the
  task carries `TaskRequirements` (`cpu_cores`, `gpu_required`, `vram_mb`). The master commits
  terminal state once in `handle_task_result` when `queue.record_result_from_enqueued` returns
  `ResultDisposition::Terminal` (`crates/master/src/server/tasks_and_blobs.rs`).
- **Terminal eviction:** `TaskQueue` (`crates/master/src/queue.rs`) evicts finished tasks beyond
  `--max-retained-tasks` and `--max-retained-mb`, so the task table cannot serve as the account.
- **Worker ownership:** `MembersFile::worker_owners` (`crates/master/src/members.rs`) and
  `WorkerRegistry::check_worker_owner` (`crates/master/src/registry.rs`) map a v3 worker's `Uuid`
  to its owning `member_id`.
- **Submitter identity:** `handle_client_connection` (`crates/master/src/server/client_and_members.rs`)
  creates a per-connection `SubmitterId::Client(Uuid::new_v4())` for fair share and holds
  `Option<AuthenticatedV3Peer>` (which carries `member_id`). `TaskEntry.submitted_by_member` keeps
  that identity in memory; persisting it in `TaskStore` (`STORE_FORMAT_VERSION` `4 -> 5`) stays in
  `ECO-9` / `ENROLL-1c` step 2.
- **Wire surface today:**
  - `oxideswarm members list` (`crates/cli/src/members_cmd.rs`) sends `ClientMessage::ListMembers`
    and receives `ClientResponse::MembersList { members: Vec<MemberEntry> }`
    (`crates/core/src/protocol.rs`, `MemberEntry` in `crates/core/src/noise_auth.rs`).
  - `oxideswarm status` (`crates/cli/src/cli_app/tasks.rs`) sends `ClientMessage::ClusterStatus` and
    receives `ClientResponse::ClusterStatus`, which carries counts and `Vec<WorkerCapabilities>` and
    has no credit fields or per-member list.
  - Both enums use positional bincode (`crates/core/src/protocol/client_resp_mirrors.rs`) frozen at
    `PROTOCOL_VERSION_CURRENT = 10` / `PROTOCOL_VERSION_MIN_COMPATIBLE = 9` by
    `crates/core/src/wire_layout_tests.rs`. `BUG-22` (separate `cancelled_tasks` count in
    `ClientResponse::ClusterStatus`) is already waiting for the next planned wire change.
  - Note on roles: `check_v3_client_message_role` (`crates/master/src/server/handshakes.rs`) permits
    `ClientMessage::ClusterStatus` for any enrolled role (`member.has_any_role()`), whereas
    `ClientMessage::ListMembers` is restricted to `admin`.
- **Dashboard:** `crates/master/src/web_ui.rs` (`api_status` and `/ws`, `/api/stream/sse`) and
  `crates/master/src/dashboard/dto.rs` serve local HTTP JSON behind RBAC `AuthScope`
  (`crates/master/src/auth.rs`) and are independent of the binary wire protocol.

## Goals

- **G1:** Keep an accurate, bounded, append-only ledger on the master (in `--state-dir` when
  configured, and in memory otherwise) that survives master restarts without losing totals when old
  raw entries age out, and never stops or blocks the master on disk or compaction errors.
- **G2:** Preserve exact double-entry conservation (`sum(ran_for_others) == sum(used_from_others)`
  for tasks, core-milliseconds/seconds, and GPU-milliseconds/seconds, with self-run isolated) over
  `24h`, `7d`, and `all_time`.
- **G3:** Share one ledger record schema with model-tunnel usage records (`LLM-10`, `LLM-11` of
  [design 0061](0061-first-application-model-tunnel.md)).
- **G4:** Enforce the display-only rule of Q96 and Q39: contribution records never affect placement,
  admission, fair share, or worker limits, cannot be moved or spent, show no signed net column or
  monetary/balance wording, and never leave the master automatically.
- **G5:** Surface participant contributions in the web dashboard, `oxideswarm members list`, and
  `oxideswarm status` cleanly without violating [design 0076](0076-protocol-compatibility-and-release-train.md)
  or [design 0083](0083-one-version-window.md).

## Non-goals

- Transferring, spending, selling, buying, or valuing credits in currency or showing signed "Net"
  or "balance" indicators (Q96).
- Enforcing quotas, throttling, or rejecting submissions based on contribution history (those belong
  to `ECO-9` quotas).
- Inter-cluster signed receipts (`NET-2` of [design 0054](0054-open-compute-network.md)).
- Replacing `ECO-9`'s participant label or `ENROLL-1c`'s journal submitter field; this design keys
  records by `member_id` so participant labels can be applied at display time without migration.

## Design

### D1 Shared ledger format (`NET-10` and `LLM-10`)

A single master-owned module `crates/master/src/credits.rs` maintains the usage ledger and exposes
read-only window aggregations (`CreditsSnapshot`).

Each appended entry is a self-contained compact JSON line (avoiding pretty-printing and avoiding
`serde_json::Value` DOM trees) with one shared outer envelope covering both compute tasks (`NET-10`)
and model tunnels (`LLM-10`):

```rust
pub struct UsageRecord {
    pub record_id: u64,               // Monotonic master-assigned sequence starting at 1
    pub recorded_at_utc: u64,         // Unix epoch seconds when recorded on the master
    pub runner_member_key: String,    // Runner owner's member_id hex, or "organiser"
    pub submitter_member_key: String, // Submitter's member_id hex, or "organiser"
    pub kind: UsageKind,
}

#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UsageKind {
    Task {
        task_id: TaskId,
        worker_id: Uuid,
        duration_ms: u64,   // Master-clamped <= min(reported, elapsed, timeout, 7d)
        cpu_cores: usize,   // Reserved cores (clamped 1..=4096)
        gpu_reserved: bool, // GPU-seconds count only when reserved
        exit_code: i32,
        verified: Option<bool>, // None in Stage 1; Some(true/false) from Stage 2
    },
    ModelTunnel {
        tunnel_id: Uuid,
        service: String,
        worker_id: Uuid,
        duration_ms: u64,
        bytes_to_worker: u64,
        bytes_from_worker: u64,
        close_reason: String,
    },
}
```

- **Commit point and ordering (exactly once, non-blocking after ACK and notify):**
  `apply_task_result` calls `prepare_terminal_credit` only when `queue.record_result_from_enqueued`
  returns `ResultDisposition::Terminal` and `!outcome.rejected`. Stale results, late duplicates,
  worker rejections, evacuations, cancellations, and intermediate failovers are never booked. The
  entry is passed to `CreditsLedger::enqueue_task` strictly **after** durable store `ack.await`,
  `send_result_ack`, and `notify_result_waiters`, handing off to a single batching OS writer thread
  (5 ms / 256-record group commit) so disk sync and compaction never delay worker ACKs or client
  notifications and no `std::sync::Mutex` is ever held across `.await` or disk I/O.
- **Clamping & GPU-reserved-only rule:** Master clamps worker-reported `execution_time_ms` against
  master observed elapsed time since submit (`max(monotonic_elapsed, utc_elapsed + 1s)`), declared
  `timeout_secs`, and `MAX_CREDIT_DURATION_MS` (7 days). GPU-seconds count **only** when
  `gpu_reserved` is true (`design 0094 D8`), ignoring unverified worker `is_gpu_executed` claims.
  Counters accumulate in `u128` so extreme values cannot saturate one side and break conservation.
- **Cross-participant conservation & self-run isolation (Q96 / design 0054 D6):**
  After resolving `runner_member_key` and `submitter_member_key` at display time to participant groups:
  - When `runner_group == submitter_group`: the work goes exclusively into `self_run` (not
    `ran_for_others`, not `used_from_others`).
  - When `runner_group != submitter_group`: `runner_group.ran_for_others` and
    `submitter_group.used_from_others` increment by the identical `SideCounters` vector
    (`tasks = 1`, `core_ms = clamped_ms * cores`, `gpu_ms = if gpu_reserved { clamped_ms } else { 0 }`),
    guaranteeing $\sum_p \text{ran\_for\_others}_p == \sum_p \text{used\_from\_others}_p$ identically
    over `last_24h`, `last_7d`, and `all_time`.
- **Stage 1 vs Stage 2 counting:** In Stage 1 (`verified: None`), every terminal task counts per
  [design 0094](0094-road-to-a-shared-research-grid.md) D8. From Stage 2 (`NET-3`), entries with
  `verified == Some(false)` are excluded from counters.
- **Privacy (`Q39`, `LLM-10`):** No command arguments, environment variables, file paths, stdout,
  stderr, or prompt/response bytes are ever stored in `UsageRecord`.

### D2 Member-id keying and display-time label resolution

- Persisted `UsageRecord` and archived totals key strictly on `member_id` (or `"organiser"` for v2
  shared-secret / local / dashboard submissions).
- `CreditsSnapshotFile` stores pairwise archived totals `archived_pairs: BTreeMap<String, BTreeMap<String, SideCounters>>`
  (`[runner_member_key][submitter_member_key]`) rather than pre-flattened label totals. When
  `ECO-9` adds participant labels or an admin assigns/renames a label covering one or more machine
  keys, `snapshot_for_display` regroups both `archived_pairs` and `retained` at display time:
  history never splits across label edits, and cross-machine tasks between keys under one label
  reclassify automatically into `self_run` while keeping global conservation exact.
- **RBAC display filtering (D9(c)):**
  - `AuthScope::Admin` callers on `GET /api/credits` receive resolved participant labels (or
    `MemberEntry.name` / `member_id` for unlabeled members) plus `member_ids: Vec<String>`.
  - `AuthScope::Observer` callers receive labels only (`member_ids` omitted): `"organiser"`,
    explicit participant labels, or neutral deterministic `"participant-N"` placeholders for unlabeled
    members, never revealing raw `member_id` hex or `MemberEntry.name` machine names.

### D3 Bounded storage, low-water compaction, and fail-open recovery

Inside `--state-dir`, `CreditsLedger` writes:
- `credits.snapshot.json`: compact JSON envelope `{ "version": 1, "blake3": ..., "payload": ... }`
  written atomically via temp file + `sync_all` + `rename` + `store::sync_dir` before resetting the
  journal (`0600` Unix mode / owner-only Windows DACL per [design 0047](0047-windows-private-files.md)).
- `credits.journal.jsonl`: append-only compact NDJSON opened read+write (without `.append(true)` so
  `set_len` works on Windows during both compaction and torn-tail recovery).

Checksum scope: the per-line BLAKE3 digest in `credits.journal.jsonl` (and `credits.snapshot.json`)
is a local integrity checksum, not a hash chain: it detects torn or accidental writes, not tampering
by anyone who already has write access to `--state-dir`.

Bounding & fail-open rules:
- Defaults: `--max-credit-records 100000`, `--max-credit-bytes 33554432` (32 MiB). Values of `0` on
  either flag/config field are refused at CLI validation.
- **Low-water compaction (~50%):** When `retained.len() > max_credit_records` or
  `snapshot_bytes + journal_bytes > max_credit_bytes`, the writer thread folds oldest records into
  `archived_pairs` down to `max_records / 2` (and half of `max_bytes`), writes the compact snapshot
  with `store::sync_dir`, then truncates `credits.journal.jsonl` to `0` (`set_len(0)` + `sync_all`)
  and updates the in-memory cutoff without blocking readers.
- **Fail-open guarantee:** A display-only ledger never stops the master:
  - On journal write/sync failure (e.g. full disk), the writer truncates `credits.journal.jsonl` back
    to the last clean byte offset (`set_len(saved_len)`), seeks cleanly, enforces RAM bounds, logs a
    warning, and keeps running.
  - On compaction error after journal commit, the writer logs a warning and returns `Ok` to any
    record waiter so callers never fail after a record was journaled.
  - On startup `open()`, torn trailing lines truncate cleanly; corrupt snapshot or mid-file corrupt
    journal files are quarantined (`*.corrupt.<epoch>`) with a warning and replaced by a fresh empty
    ledger (while symlinks on credit files remain refused by `open()`; if `MasterServer` startup
    still gets an error opening the ledger, it warns and falls back to an in-memory ledger).

### D4 Display-only enforcement (Q96, Q39)

- `WorkloadScheduler` (`crates/master/src/scheduler.rs`), `TaskQueue` admission/fair-share, and
  `WorkerClient` never query `credits.rs`.
- No RPC, CLI verb, or HTTP endpoint mutates counters (no `transfer`, `grant`, `spend`, or `reset`;
  no signed `Net` column, no balance icon, no `"debited"` or `"balance"` names).
- No telemetry exporter (`otel.rs` / `discovery.rs`) leaves the master with credit data.

### D5 Display surfaces and why `ClientResponse` needs a wire change

1. **Web dashboard (`crates/master/src/web_ui.rs`, `dashboard.html`) — no wire change:**
   - Adds `GET /api/credits` (protected by `AuthScope::Observer`, redacting member ids/names from
     non-Admin observers).
   - `dashboard.html` renders "Contribution Credits (Display Only)" with columns `Participant`,
     `Ran for others`, `Used from others`, and `Self-run` across `24h`, `7d`, and `All Time` tabs.
2. **`oxideswarm members list` and `oxideswarm status` — requires a `ClientResponse` wire change:**
   - Both use positional bincode (`crates/core/src/protocol/client_resp_mirrors.rs`) frozen at
     `PROTOCOL_VERSION_CURRENT = 10` (`BUG-22` is already waiting here). Adding fields or a
     `ListCredits` variant stops at this design until the protocol 11 train.

### D6 Wire-change decision (Option B shipped, protocol 11 deferred)

Protocol `10` stays untouched now: `credits.rs`, `--max-credit-records`, `--max-credit-bytes`,
`GET /api/credits`, and `dashboard.html` ship on draft pull request 379 (holding for post-stability
merge). CLI `ClientResponse` fields (`ListCredits` / `ClusterStatus` / `MembersList`) defer to the
protocol 11 train alongside `BUG-22` and `ECO-9` (`design 0076 D1`, `design 0083`).

## Compatibility

- **Option B on draft pull request 379:** Keeps protocol version (`10`), `TaskStore` format (`4`),
  and `Cargo.lock` completely unchanged.
- **Wire change deferred to protocol 11:** Bumps `PROTOCOL_VERSION_CURRENT` to `11`, updates wire
  mirrors and `tests/test_release_skew.py`, and serves `ClientMessage::ListCredits` / trailing
  fields via `serialize_message_for_peer`.

## Security

- **Local files:** `credits.snapshot.json` and `credits.journal.jsonl` under `--state-dir` use `0600`
  Unix mode / owner-only Windows DACL (`design 0047`), refusing symlinks on open/compaction.
- **Observer redaction (D9(c)):** `GET /api/credits` never exposes raw `member_id` hex or machine
  names to non-Admin `AuthScope::Observer` tokens.
- **Clamping & overflow immunity:** Master clamps task duration (`min(reported, elapsed, timeout, 7d)`)
  and `cpu_cores` (`1..=4096`), ignores worker `is_gpu_executed` when `!gpu_reserved`, and sums in
  `u128` so extreme inputs never break zero-sum conservation.
- **Bounded memory & disk:** `--max-credit-records` and `--max-credit-bytes` cap both RAM and disk.

## Test plan

- **Unit (`crates/master/src/credits/tests.rs` & `web_ui.rs` & `crates/cli`):**
  - Q96 conservation & self-run isolation (`sum(ran_for_others) == sum(used_from_others)`).
  - Member-id keying, display-time label grouping/renaming, and Observer redaction vs Admin `member_ids`.
  - Duration clamping, `gpu_reserved`-only billing, and `u128` extreme-value conservation.
  - Time window filtering (`24h`, `7d`, `all_time`) with injected clock and shared `ModelTunnel` envelope.
  - Low-water (~50%) compaction count at cap, compact JSON format, `mode 0600` permissions, torn-tail
    truncation (`set_len(valid_len)`), compaction (`set_len(0)`), failed-append rollback truncation,
    corrupt snapshot/journal quarantine (`*.corrupt.*`), symlink refusal, and CLI `0` flag rejection.
- **Integration (`crates/master/tests/test_net10_contribution_credits.rs`):**
  - Two enrolled v3 participants on loopback: cross-participant + self-run conservation, Observer vs
    Admin redaction on `GET /api/credits`, restart survival in `--state-dir`, low-water rollover
    compaction, worker rejection, failover, late/stale duplicate results, queued & running
    cancellations, inflated duration/GPU clamping, and master startup resilience with corrupt credit
    files.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| NET-10 | Contribution credits per participant, display only (Stage 1 of design 0094, Option B) | `crates/master/src/credits.rs`, `crates/master/src/web_ui.rs`, `crates/master/src/dashboard.html`, `crates/cli/src/cli_app.rs`, `docs/reference.md` | Master ledger, `--state-dir` persistence + low-water compaction, fail-open quarantine, `GET /api/credits`, dashboard Q96 table, and loopback/unit tests pass (CLI `ClientResponse` fields deferred to protocol 11 with `BUG-22` and `ECO-9`) | Partial |

## Decisions (resolved 2026-11-19)

- **Wire-change bundling:** Decided **Option B** — ship master ledger, `GET /api/credits`, and
  dashboard card first with zero wire or store-format change on draft pull request 379; wire change
  deferred to the protocol 11 train with `BUG-22` and `ECO-9`.
- **Member-id keying (coordinator decision):** Key ledger by `member_id` and resolve participant
  labels at display time so history never splits when labels change and `NET-10` does not wait for
  `ECO-9` or amend `design 0094`.
- **Q96 wording (coordinator decision):** Keep double-entry internal; display `"Ran for others"`,
  `"Used from others"`, and `"Self-run"` with no signed `Net`, no balance icon, and no
  `"debited"`/`"balance"` names.
- **Integrity scope (coordinator decision):** Per-line BLAKE3 in the journal is a checksum catching
  torn or accidental writes, not a tamper-proof chain.
- **Dashboard RBAC:** `/api/credits` requires `AuthScope::Observer` (labels only for Observer;
  `member_ids` and member names for Admin), rejecting mutating methods (`405`/`403`).

## Status log

- 2026-11-19: Claimed `design 0096` on the hub and wrote initial `Draft` design for `NET-10` on branch `mac/net-10-contribution-credits` (draft pull request 379).
- 2026-11-19: Implemented Option B on draft pull request 379 (`crates/master/src/credits.rs`, `--max-credit-records` / `--max-credit-bytes`, `GET /api/credits`, and `dashboard.html` panel, zero wire change); wire change deferred to the protocol 11 train with `BUG-22` and `ECO-9`.
- 2026-11-19: Applied all coordinator review fixes on draft pull request 379 (Windows read+write journal open, partial-append rollback + corrupt-file quarantine + fail-open startup, low-water compact-JSON compaction + single batching writer after ACK/notify + `store::sync_dir`, master duration clamping + `gpu_reserved`-only billing, member-id keying + display-time label resolution, Q96 UI/API wording, Observer redaction, `--max-credit-records 0` validation, and added tests).
