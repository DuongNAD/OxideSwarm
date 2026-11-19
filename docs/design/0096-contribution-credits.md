# 0096: Contribution credits per participant, display only (NET-10)

Status: Draft (stops after the design record; awaits coordinator decision on bundling the `ClientResponse` wire change with BUG-22 and ECO-9)
Created: 2026-11-19    Updated: 2026-11-19

## Summary

Adds a bounded, append-only contribution ledger on the master (`crates/master/src/credits.rs`) so
each participant in a circle sees the tasks, core-seconds, and GPU-seconds their machines ran for
others and their tasks used over the last day, the last week, and all time (`NET-10`, Stage 1 of
[design 0094](0094-road-to-a-shared-research-grid.md)). Every entry credits the runner and debits the
submitter by the same amount, is strictly display-only (Q96), and shares one record format with
model-tunnel usage records (`LLM-10`, `LLM-11` of [design 0061](0061-first-application-model-tunnel.md)).
Showing credits in `oxideswarm members list` and `oxideswarm status` requires a wire change to
`ClientResponse` (`ClientMessage`/`ClientResponse` bincode layouts are frozen at protocol `10`), so
implementation stops here pending coordinator approval on wire-change bundling.

## Context

Read against `master` (`f6554a72`) on 2026-11-19:

- **Task execution facts:** When a task finishes on a worker, the worker returns `TaskResult`
  (`crates/core/src/task.rs`: `worker_id`, `task_id`, `exit_code`, `execution_time_ms`,
  `is_gpu_executed`), while the task carries `TaskRequirements` (`cpu_cores`, `gpu_required`,
  `vram_mb`). The master commits terminal state once in `handle_task_result` when
  `queue.record_result_from_enqueued` returns `ResultDisposition::Terminal`
  (`crates/master/src/server/tasks_and_blobs.rs`).
- **Terminal eviction:** `TaskQueue` (`crates/master/src/queue.rs`) evicts finished tasks beyond
  `--max-retained-tasks` and `--max-retained-mb`, so the task table cannot serve as the account.
- **Worker ownership:** `MembersFile::worker_owners` (`crates/master/src/members.rs`) and
  `WorkerRegistry::check_worker_owner` (`crates/master/src/registry.rs`) map a v3 worker's `Uuid`
  to its owning `member_id`.
- **Submitter identity:** `handle_client_connection` (`crates/master/src/server/client_and_members.rs`)
  creates a per-connection `SubmitterId::Client(Uuid::new_v4())` for fair share and holds
  `Option<AuthenticatedV3Peer>` (which carries `member_id`), but `TaskQueue` and `TaskStore`
  (`STORE_FORMAT_VERSION = 4`, `crates/master/src/store.rs`) do not yet record or persist the
  submitting member or participant label (planned in `ECO-9` step 2 together with `ENROLL-1c` step 2).
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
  raw entries age out.
- **G2:** Preserve exact double-entry conservation (`contributed - used` sums to zero across all
  participants for tasks, core-seconds, and GPU-seconds) over `1d`, `7d`, and `all_time`.
- **G3:** Share one ledger record schema with model-tunnel usage records (`LLM-10`, `LLM-11` of
  [design 0061](0061-first-application-model-tunnel.md)).
- **G4:** Enforce the display-only rule of Q96 and Q39: credits never affect placement, admission,
  fair share, or worker limits, cannot be moved or spent, and never leave the master automatically.
- **G5:** Surface participant credits in the web dashboard, `oxideswarm members list`, and
  `oxideswarm status` cleanly without violating [design 0076](0076-protocol-compatibility-and-release-train.md)
  or [design 0083](0083-one-version-window.md).

## Non-goals

- Transferring, spending, selling, buying, or valuing credits in currency (Q96).
- Enforcing quotas, throttling, or rejecting submissions based on credit balance (those belong to
  `ECO-9` quotas, never the credit balance).
- Inter-cluster signed receipts (`NET-2` of [design 0054](0054-open-compute-network.md)).
- Replacing `ECO-9`'s participant label or `ENROLL-1c`'s journal submitter field; this design uses
  them when present and states how pre-`ECO-9` masters attribute work.

## Design

### D1 Shared ledger format (`NET-10` and `LLM-10`)

A single master-owned module `crates/master/src/credits.rs` maintains the usage ledger and exposes
read-only window aggregations (`LedgerSnapshot`).

Each appended entry is a self-contained JSON line (or length+BLAKE3-framed record matching the framing
discipline of [design 0046](0046-master-durability.md), avoiding bincode layout coupling across minor
releases) with one shared outer envelope covering both compute tasks (`NET-10`) and model tunnels
(`LLM-10`):

```rust
pub struct UsageRecord {
    pub record_id: u64,               // Monotonic master-assigned sequence
    pub recorded_at_utc: u64,         // Unix epoch seconds when committed
    pub credited_participant: String, // Runner's participant label or fallback id
    pub debited_participant: String,  // Submitter's participant label or fallback id
    pub credited_member_id: Option<String>, // None for v2 / local-host worker
    pub debited_member_id: Option<String>,  // None for v2 / dashboard / local submitter
    pub kind: UsageKind,
}

#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UsageKind {
    Task {
        task_id: TaskId,
        worker_id: Uuid,
        duration_ms: u64,
        cpu_cores: usize,
        gpu_reserved: bool,
        is_gpu_executed: bool,
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

- **Commit point (exactly once per terminal task):** Recorded in `handle_task_result` only when
  `queue.record_result_from_enqueued` returns `ResultDisposition::Terminal`. Stale results, late
  duplicates, worker rejections, evacuations, and transient retries are never booked.
- **Conservation law (design 0054 D6):** A `UsageKind::Task` entry credits `credited_participant` and
  debits `debited_participant` by the identical vector:
  - `tasks = 1`
  - `core_ms = duration_ms.saturating_mul(cpu_cores as u64)` (reported as both exact `core_ms` in
    JSON and `core_secs = core_ms as f64 / 1000.0`)
  - `gpu_ms = if gpu_reserved || is_gpu_executed { duration_ms } else { 0 }`
  When `credited_participant == debited_participant` (self-run work), both contributed and used sides
  increase by the same amount, so net (`contributed - used`) is `0`, and across the whole ledger
  $\sum_p (\text{contributed}_p - \text{used}_p) = 0$ always holds identically.
- **Stage 1 vs Stage 2 counting:** In Stage 1 (`verified: None`), every terminal task counts per
  [design 0094](0094-road-to-a-shared-research-grid.md) D8 (`verified` can be omitted on Stage 1 task
  records; dashboard/CLI still display `exit_code == 0` vs non-zero counts if helpful, without
  breaking zero-sum). From Stage 2 (`NET-3`), entries with `verified == Some(false)` are excluded
  from verified totals.
- **Privacy (`Q39`, `LLM-10`):** No command arguments, environment variables, file paths, stdout,
  stderr, or prompt/response bytes are ever stored in `UsageRecord`.

### D2 Grouping by participant label and member attribution

- **Grouping rule:**
  1. When `ECO-9` adds `participant: Option<String>` to `MemberEntry`, every member maps to its
     non-empty trimmed `participant` label; unlabeled members fall back to `member:<member_id>` (or
     `"organiser"` for v2 peers and the local master host, matching `ECO-9` step 1).
  2. Before `ECO-9` merges: v3 peers group by their enrolled `member_id` (display helper can resolve
     the current `MemberEntry.name` alongside `member_id` when local to the master), and v2
     shared-secret / unauthenticated loopback peers group under the reserved key `"organiser"`.
- **Submitter attribution (`debited_participant`):**
  - `handle_client_connection` knows `v3_peer.as_ref().map(|p| p.member_id.clone())`. That value is
    stored alongside the queued task (`submitted_by_member: Option<String>` in `TaskQueue`, and
    persisted across restarts once `ECO-9` step 2 / `ENROLL-1c` step 2 updates
    `STORE_FORMAT_VERSION`). Web UI `POST /api/tasks` submissions carry `"organiser"` (or the RBAC
    identity if applicable).
  - If `NET-10` runs before `ECO-9` persists submitters in `journal.bin`/`snapshot.bin`, an in-memory
    map tracks submitters cleanly during runtime, while in-flight tasks replayed after a crash fallback
    to `"organiser"` until `TaskStore` format `5` lands; this tradeoff should be weighed when
    ordering `ECO-9` and `NET-10`.

### D3 Bounded storage, restart survival, and rollover conservation

Inside `--state-dir`, `LedgerStore` writes:
- `credits.snapshot.json`: `{ version: 1, next_record_id, archived_totals: BTreeMap<String, WindowCounters>, retained: Vec<UsageRecord> }`, compacted atomically (`write tmp -> sync_all -> rename`, plus BLAKE3 integrity tag header or envelope, `0600` mode on Unix / owner-only DACL on Windows per [design 0047](0047-windows-private-files.md)).
- `credits.journal.jsonl`: append-only NDJSON (each line carrying a sequence id and per-line BLAKE3 tag over the JSON payload; torn trailing lines are truncated on recovery like `store.rs`).

Bounding rules (configurable with sane defaults):
- Defaults: `--max-credit-records 100000`, `--max-credit-bytes 33554432` (32 MiB).
- When `retained.len()` exceeds `max_credit_records` or journal+snapshot exceeds `max_credit_bytes`,
  the oldest records are folded into `archived_totals` (`contributed` for `credited_participant`,
  `used` for `debited_participant`) and compacted out of the journal.
- Folding evicted records into `archived_totals` guarantees:
  1. Disk and memory stay strictly bounded regardless of cluster lifetime.
  2. `all_time` totals and $\sum (\text{contributed} - \text{used}) = 0$ stay exact forever.
  3. `last_24h` (`now - 86_400`) and `last_7d` (`now - 604_800`) compute over `retained` records; if
     retained capacity was exhausted inside the last 7 days, the response marks
     `window_truncated: true` honestly instead of claiming completeness.

### D4 Display-only enforcement (Q96, Q39)

- `WorkloadScheduler` (`crates/master/src/scheduler.rs`), `TaskQueue` admission/fair-share, and
  `WorkerClient` never query `credits.rs`.
- No RPC, CLI verb, or HTTP endpoint mutates balances (no `transfer`, `grant`, `spend`, or `reset`).
- No telemetry exporter (`otel.rs` / `discovery.rs`) sends credit balances off the master.

### D5 Display surfaces and why `ClientResponse` needs a wire change

1. **Web dashboard (`crates/master/src/web_ui.rs`, `dashboard.html`) — no wire change:**
   - Adds `GET /api/credits` (protected by `AuthScope::Observer`) and an optional `credits`
     summary in `/api/status` or `ClusterSnapshotDto`.
   - `dashboard.html` renders a "Contribution Credits (Display Only)" table showing per-participant
     `Last 24h`, `Last 7d`, and `All Time` (`Tasks`, `Core-s`, `GPU-s` contributed / used / net),
     with a footer note confirming Q96 (`display only; net sum = 0`).
2. **`oxideswarm members list` and `oxideswarm status` — requires a `ClientResponse` wire change:**
   - `oxideswarm members list` sends `ClientMessage::ListMembers` and decodes ONLY
     `ClientResponse::MembersList { members: Vec<MemberEntry> }`. Neither `MemberEntry` nor
     `ClientResponse::MembersList` contains credit counters, and positional bincode rejects any added
     field at deserialization unless guarded by a protocol bump.
   - `oxideswarm status` sends `ClientMessage::ClusterStatus` and decodes ONLY
     `ClientResponse::ClusterStatus`, which likewise carries fixed task bucket counts and
     `Vec<WorkerCapabilities>` with no per-member/participant credit slice (adding a single field
     here is the exact reason `BUG-22` is held for the next wire change).
   - **Role access consideration:** `ClientMessage::ListMembers` is currently `admin`-only
     (`crates/master/src/server/handshakes.rs:163`). A contributor holding only `worker` or `client`
     roles should still be able to view cluster contribution credits via `oxideswarm status` or a
     dedicated read-only query (`ListCredits` under `member.has_any_role()`), just like
     `ClusterStatus`.

### D6 Wire-change options for coordinator decision (stop point)

Because exposing credits in `oxideswarm members list` and `oxideswarm status` modifies the
client-master wire protocol, implementation pauses here for a coordinator choice among three options
under [design 0076](0076-protocol-compatibility-and-release-train.md) and
[design 0083](0083-one-version-window.md):

| Option | Wire shape (Protocol `11`) | Compatibility window | Timing vs `BUG-22` & `ECO-9` |
|---|---|---|---|
| **A (Recommended): Bundle CLI credits into the next protocol `11` train alongside `BUG-22` (and `ECO-9` if ready)** | Append `ClientMessage::ListCredits` + `ClientResponse::CreditsList { summary }` (available to `has_any_role()`), add optional `participant: Option<String>` / credit summary support in `MembersList`, and add `cancelled_tasks` (`BUG-22`) + optional credits summary in `ClusterStatus`. | `serialize_message_for_peer` encodes v10 `ClusterStatus` / `MembersList` cleanly for v10 peers, keeping the one-version window (`10..=11`) open per [design 0083](0083-one-version-window.md), OR closes window at `11` if bundled with `ECO-9`. `members list` / `status` query `ListCredits` when master protocol >= `11`. | One protocol bump serves `BUG-22`, `NET-10`, and `ECO-9` together (satisfies design 0076 D1). |
| **B: Ship master ledger + dashboard `/api/credits` now (zero wire change), defer CLI `ClientResponse` fields** | No protocol change now (`10` stays untouched); `credits.rs` + `GET /api/credits` + `dashboard.html` land after the stability release. | Protocol stays `10` with zero skew. | `oxideswarm status` and `oxideswarm members list` still lack credits until Option A's wire bump lands. |
| **C: Hold all `NET-10` code until `ECO-9` + `BUG-22` wire & store format `5` train** | Same wire & store train as Option A, plus `TaskStore` format `5` for persisted task submitter (`ECO-9` step 2 / `ENROLL-1c` step 2). | Single protocol `11` + store `5` release after the `0.4.x` stability release. | Avoids temporary in-flight submitter fallback on master restart prior to `ECO-9`. |

## Compatibility

- **Design-only right now:** Changes protocol version (`10`), store format (`4`), and `Cargo.lock`
  **not at all**.
- **At implementation time (under Option A or C):**
  - Bumps `PROTOCOL_VERSION_CURRENT` to `11`, updates both `HumanClient*` and `BinaryClient*`
    mirrors + all four `From` impls, refreshes `crates/core/tests/data/wire_layout.txt` (and moves
    v10 layout to `wire_layout_previous.txt` if keeping the `10..=11` window), and updates
    `tests/test_release_skew.py`.
  - If `ListCredits` / `CreditsList` is appended as new enum variants while `ClusterStatus` /
    `MembersList` add v11-only trailing fields formatted conditionally via `serialize_message_for_peer`
    by peer version (`last_peer_version`), v10 CLI clients continue working seamlessly against a v11
    master without seeing credit fields.
  - Note: a v11 CLI `members list` / `status` calling `ListCredits` after `ClusterStatus`/`MembersList`
    must check `Hello`'s version or peer server version (`>= 11`, cached on `connect_master`) so it
    never sends an unknown `ListCredits` variant to a v10 master.

## Security

- **Local files:** `credits.snapshot.json` and `credits.journal.jsonl` under `--state-dir` are created
  with `0600` Unix permissions / owner-only Windows DACL (`std::fs::OpenOptions`), refusing symlinks on
  open/recovery.
- **No external leakage (Q39):** Ledger endpoints require RBAC `AuthScope::Observer` (with existing
  loopback `--dev` bypass rules, never off loopback); worker nodes and external clusters never pull or
  receive the ledger.
- **Saturating integer math:** `core_ms` and `gpu_ms` use saturating arithmetic (`u128` or
  `u64::saturating_*`) to prevent overflow even on crafted/corrupted task durations, plus per-record
  BLAKE3 verification on journal replay so corrupted trailing bytes are safely truncated without
  corrupting balances.
- **Bounded memory & disk:** `--max-credit-records` and `--max-credit-bytes` cap ledger growth so
  long-lived masters cannot exhaust disk or RAM.

## Test plan

Once approved for implementation:
- **Unit (`crates/master/src/credits.rs`):**
  - Terminal-only booking (stale, late, rejection, evacuation, failover attempts ignored; single
    terminal completion booked once).
  - Double-entry conservation check across mixed self-run and cross-participant tasks (`sum(contributed - used) == 0` for tasks, core-ms, and gpu-ms).
  - Time window filtering (`24h`, `7d`, `all_time`) with injected clock.
  - Rollover compaction past `--max-credit-records` preserving exact `all_time` totals and zero-sum
    invariant while bounding retained records and journal size; torn trailing journal line recovery.
- **Wire & protocol (`crates/core`):** `wire_layout_tests` + `window_tests` covering v10 <-> v11
  compatibility when `ClientResponse` changes are included.
- **Integration (loopback cluster, private `HOME`):**
  - Two enrolled v3 participants (and organiser/worker mix): run tasks across both workers, verify
    contributed vs used core-seconds match task `execution_time_ms * cpu_cores`, sum to `0` across
    all participants, persist across master restart with `--state-dir`, and appear identically in
    `oxideswarm members list --json`, `oxideswarm status --json`, and `GET /api/credits`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| NET-10 | Contribution credits per participant, display only (Stage 1 of design 0094) | `crates/master/src/credits.rs`, `crates/master/src/web_ui.rs`, `crates/master/src/dashboard.html`, `crates/core/src/protocol.rs`, `crates/cli/src/members_cmd.rs`, `crates/cli/src/cli_app/tasks.rs`, `docs/reference.md` | On a 2-participant loopback cluster, contributed/used core-seconds match task run times, net sums to zero, survive master `--state-dir` restart, stay within ledger bounds, and render in CLI and dashboard | Planned |

## Open decisions

- **Wire-change bundling (for coordinator):** Choose between **Option A** (bundle `NET-10` `ClientResponse` fields / `ListCredits` with `BUG-22` and `ECO-9` into protocol `11`), **Option B** (build master ledger + `/api/credits` dashboard view first with zero wire change, defer CLI fields to protocol `11`), or **Option C** (hold implementation until post-stability `BUG-22` + `ECO-9` + store format `5` train).
- **Dashboard unauthenticated loopback view:** Confirm whether `/api/credits` follows `/api/status`'s exact `AuthScope::Observer` rule (recommended yes, since participant labels reveal cluster usage activity).

## Status log

- 2026-11-19: Claimed `design 0096` on the hub and wrote initial `Draft` design for `NET-10` on branch `mac/net-10-contribution-credits`. Verified that showing credits in `oxideswarm members list` and `oxideswarm status` requires a `ClientResponse` wire change (`PROTOCOL_VERSION_CURRENT` currently `10`), and stopped before Rust/wire implementation awaiting coordinator decision.
