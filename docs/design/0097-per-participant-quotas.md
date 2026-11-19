# 0097: Per-participant quotas and participant labels (`ECO-9` and `ENROLL-1c` step 2)

Status: Draft (Master-side quotas, store format 5 + ENROLL-1c step 2, and dashboard API/UI implemented on draft PR 384; Wire change: deferred to the protocol 11 train with BUG-22 and NET-10)
Created: 2026-11-19    Updated: 2026-11-19

## Summary

Adds **participant labels** and **per-participant resource quotas** (`max_running_tasks`,
`max_slots`, `max_queued_tasks`, `priority_band`, and `fair_share_weight`) on the master, keys
weighted fair-share scheduling on the resolved participant rather than individual client TCP/QUIC
connections, and completes step 2 of `ENROLL-1c` (`TaskStore` format `4 -> 5`, persisting
`submitted_by_member` so `RevokeMember` cancels active tasks submitted by a revoked key).

Per coordinator instruction (`2026-11-19`), this keeps `PROTOCOL_VERSION_CURRENT` at `10`
with zero changes to `ClientMessage`, `ClientResponse`, or `MemberEntry`: all remote CLI wire
additions (`members label`, `invite --participant`, `members list` label column, `status`
wait/quota fields) stop at this design record and join the **protocol 11 train** with `BUG-22`
and `NET-10`. Quotas are a cooperative sharing mechanism for trusted circle members (not a
boundary against a hostile participant; [design 0050](0050-scale-out-architecture.md) D6).

## Context

Read against `master` (`435fa586`) on 2026-11-19:

- **Member vs person gap:** An enrolled identity in `MemberStore` (`MemberEntry` in
  `crates/core/src/noise_auth.rs`) is one machine key (`id`, `name`, `public_key`, `roles`,
  timestamps, `via_invite`), serialized directly inside `ClientResponse::MembersList` (`protocol 10`).
  One person in a trusted circle often enrolls several machines, while `MembersFile`
  (`crates/master/src/members.rs`) is local JSON on the master (`members.json`).
- **Connection-scoped fair share:** `handle_client_connection`
  (`crates/master/src/server/client_and_members.rs`) currently generates a fresh
  `SubmitterId::Client(Uuid::new_v4())` per network connection, and all TCP/QUIC `SubmitTask` calls
  pass hardcoded priority `0`. A participant running 20 short `submit` connections receives 20x the
  scheduling turns of a participant running 1 connection.
- **No per-participant caps:** Only cluster-wide caps exist today (`--max-queue-size` and
  `--max-active-mb` in `TaskQueue`, `crates/master/src/queue.rs`).
- **Durable task store (`ENROLL-1c` step 2 gap):** `TaskStore` (`crates/master/src/store.rs`) is
  at `STORE_FORMAT_VERSION = 4` and `MIN_STORE_FORMAT_VERSION = 1`. Neither `TaskEntry`,
  `PersistedTaskEntry`, nor `JournalRecord::TaskSubmitted` records which member submitted a task,
  leaving step 2 of revocation ([design 0051](0051-machine-identity-and-invites.md) D5) Planned.
  On `2026-11-19`, the coordinator approved bumping `STORE_FORMAT_VERSION` (`4 -> 5`) together
  with `ENROLL-1c` step 2 for `ECO-9`.

## Goals

- **G1:** Group a participant's enrolled machine keys (and local/v2 `"organiser"` peers) under one
  canonical participant identity persisted in `members.json` without modifying `MemberEntry` on the
  wire at protocol `10`.
- **G2:** Enforce configurable per-participant limits (`max_running_tasks`, `max_slots`,
  `max_queued_tasks`, `priority_band`, `fair_share_weight`) on the master without head-of-line
  starvation of other participants.
- **G3:** Key `FairShareQueues` (`crates/master/src/fair_share.rs`) on the resolved participant so
  1-connection vs N-connection submitters share slots fairly (e.g. 1:1 under equal weights), with
  priority tiers (`High > Normal > Low`) respected across queued work.
- **G4:** Persist `submitted_by_member` in `TaskStore` (`STORE_FORMAT_VERSION = 5`), document and
  enforce the one-version clean-shutdown store upgrade rule from format `4`, and complete
  `ENROLL-1c` step 2 (`RevokeMember` cancels active tasks submitted by the revoked member).
- **G5:** Expose participant quota configuration via master CLI/TOML and local RBAC dashboard
  endpoints (`GET /api/quotas` + Admin label/quota mutations), and specify the deferred protocol 11
  CLI wire surface cleanly.

## Non-goals

- Defending against a malicious/hostile holder of a valid key or cluster secret (`design 0050 D6`
  and [design 0094](0094-road-to-a-shared-research-grid.md) Stage 2 `NET-1`/`NET-3`/`NET-4` handle
  untrusted peers).
- Breaking wire protocol `10` compatibility before the planned protocol 11 release train
  ([design 0076](0076-protocol-compatibility-and-release-train.md) D1,
  [design 0083](0083-one-version-window.md)).
- Ledger accounting / display-only credits (handled in `NET-10`, where member-id keying allows
  display-time grouping by these participant labels without rewriting ledger history).

## Design

### D1 Participant identity and local `members.json` storage (zero wire change)

Because `MemberEntry` is transmitted positionally inside `ClientResponse::MembersList`, adding a
field to `MemberEntry` would alter protocol `10` bincode layout. Instead, participant mappings and
quota overrides live in local `MembersFile` (`crates/master/src/members.rs`, `MEMBERS_FILE_VERSION = 1`
with `#[serde(default, skip_serializing_if = "...")]`):

```rust
pub struct MembersFile {
    // ... existing fields ...
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub member_participants: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub participant_quotas: BTreeMap<String, ParticipantQuota>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_participant_quota: Option<ParticipantQuota>,
}
```

- **Canonical participant resolution:**
  - Any v2 shared-secret client or local/dashboard host submission resolves to the reserved
    participant `"organiser"` (`ORGANISER_PARTICIPANT`).
  - An enrolled OXS-AUTH v3 member (`member_id`) resolves to its assigned label in
    `member_participants`, or falls back to `member:<member_id>` so unlabeled v3 members remain
    isolated per member rather than collapsing into a single anonymous bucket.
- **Label validation:**
  - Trimmed lowercase ASCII, length `1..=64` characters, allowed bytes `[a-z0-9._@-]`.
  - Refuses the `"member:"` prefix so no label can impersonate an unlabeled member's fallback key.
  - Note: `"organiser"` is reserved for v2/host default identity, though explicit quota overrides may
    target `"organiser"`, any valid label, or a specific `"member:<member_id>"`.

### D2 Per-participant quotas and priority band

`crates/master/src/quotas.rs` defines `ParticipantQuota`:

- `max_running_tasks: Option<usize>` (`>= 1`): caps tasks owned by the participant in active execution
  states (`Scheduled`, `Running`, or queued with an active `adoptable_by` session-resume hold).
- `max_slots: Option<usize>` (`>= 1`): caps total CPU core slots (`max(1, requirements.cpu_cores)`)
  held concurrently across active execution states. At submission time, if a single task's requested
  slots already exceed `max_slots`, admission refuses it immediately with `QuotaExceeded {
  limit_kind: "max_slots", ... }` (non-transient) instead of leaving an unschedulable task queued
  forever.
- `max_queued_tasks: Option<usize>` (`>= 1`): caps tasks owned by the participant in waiting states
  (`Submitted`, `Queued`, `Retrying`). Checked atomically under the `TaskQueue` write lock during
  submission; refused immediately with `GridError::QuotaExceeded { limit_kind: "max_queued_tasks",
  .. }` (transient `is_transient() == true`, HTTP `429 Too Many Requests` on `POST /api/tasks`,
  `ClientResponse::Error` over protocol `10`).
- `priority_band: Option<PriorityBand>`: `Low` (`default = 0`, `max = 33`), `Normal` (`default = 50`,
  `max = 66`), or `High` (`default = 100`, `max = 100`).
  - When a participant submits without an explicit priority (`ClientMessage::SubmitTask` carries none
    at protocol `10`), the master assigns `band.default_priority()` (or `0` when no band is set).
  - When an explicit priority exceeds `band.max_priority()` (e.g., via dashboard/internal APIs or
    future protocol `11` messages), admission refuses with `GridError::QuotaExceeded {
    limit_kind: "priority_band", .. }` (non-transient, HTTP `400`).
- `fair_share_weight: Option<u32>` (`>= 1`, default `1`): WRR weight in `FairShareQueues`.

**Precedence (most specific wins per field):**
`members.json` per-participant override > CLI/TOML `--participant-quota` override >
`members.json` default quota > CLI/TOML `--default-participant-*` default.

### D3 Weighted fair share keyed on participant and anti-starvation scheduling

- `SubmitterId` (`crates/master/src/fair_share.rs`) gains `SubmitterId::Participant(String)` while
  preserving `Default` and `Client(Uuid)`.
- Every v3 connection maps to `SubmitterId::Participant(participant)` at submission (and on
  journal recovery for member-submitted tasks via `QuotaController::participant_for_member`), while
  v2 secret/unauthenticated connections under `"organiser"` keep per-connection
  `SubmitterId::Client(Uuid)` fair-share buckets **only when no member is enrolled**
  (`members.is_empty()`) and are attributed to `SubmitterId::Participant("organiser")` once any
  member is enrolled so v2/HTTP connections cannot multiply weight against enrolled members. Local
  internal `TaskQueue::submit` tests without quota context keep `SubmitterId::Default`.
- **Priority-tiered WRR (`walk_excluding`):** `FairShareQueues::walk_excluding(cursor, limit,
  excluded)` skips excluded (running/slot-capped) participants without mutating `active_ring`, selects
  the highest ready task priority across remaining active submitters, and walks submitters at that
  tier in WRR order before falling through to lower priority tiers.
- **Unbanded priority clamping (`QuotaController::resolve_submission_priority`):** When the resolved
  participant has no explicit `priority_band`, HTTP `POST /api/tasks` and `agent-mesh` priority
  requests are clamped to `PriorityBand::Normal.default_priority()` (`50`) if any priority band is
  configured elsewhere on the cluster (and default to `50`), or clamped to `0` if members or
  other quotas are active without bands, preventing unbanded traffic from starving enrolled
  members. Only completely unconfigured clusters pass arbitrary numeric priorities through unchanged.
- **MapReduce attribution (`crates/master/src/mapreduce.rs`):** Client connections pass
  `MapReduceAttribution` (`quota_controller`, `submitted_by_member`, `client_submitter`) to
  `MapReduceEngine::with_attribution`, routing mappers and reducers through
  `TaskQueue::try_submit_slot_attributed` so `max_slots`, `max_running_tasks`, `max_queued_tasks`,
  `priority_band`, and `ENROLL-1c` cancellation apply equally to MapReduce jobs (with transient
  `max_queued_tasks` errors retrying under the existing queue-room backoff loop).
- **Scheduler gating, zero-overhead bypass, & no starvation (`crates/master/src/scheduler.rs`):**
  1. When `QuotaController::has_any_quota_configured()` is `false`, the scheduler skips
     `participant_usage()` / `sync_participant_weights()` entirely, clears any stale wait reasons only
     if non-empty, and uses the fair walk only when `active_submitter_count() > 1`.
  2. When quotas are configured (`quota_qc` active), Step 2.5 computes per-participant active
     running tasks & slots across `active_sizes` (never scanning retained terminal tasks), updates
     in-memory scheduling weights, and fails any queued tasks whose required `cpu_cores` exceed the
     participant's current `max_slots` (e.g., after an admin lowered `max_slots`, relabeled a member,
     or restarted with tighter `--participant-quota`) as `Unfulfillable` (`TaskState::Failed`) with
     the quota reason, waking `wait: true` clients even with busy or zero workers.
  3. In Step 3, `ready_views_fair_excluding` skips participants excluded by `max_running_tasks` or
     `max_slots` so capped `High`-band tasks cannot exhaust `max_scan_per_pass` or hold back
     uncapped work; quota-blocked GPU tasks are ignored when checking `engine.note_gpu_waiting()`, so
     they do not hold GPU reserve slots idle; and any queued task with `task_slots > max_slots`
     likewise fails as `Unfulfillable`.
  4. The fair-share cursor scans via `ready_views_fair_excluding` whenever at least 2 submitters
     have queued work **or** `quota_qc` is active (`has_any_quota_configured()`).
- **Live re-labeling (`rekey_member_participant`):** When an admin assigns or edits a member's
  participant label, active non-terminal tasks belonging to that member (`active_sizes`) re-key
  cleanly between `FairShareQueues` buckets (removing and reinserting their `QueueOrderKey` under
  the write lock) so scheduling, fair share, and queued counts reflect the new participant
  immediately.

### D4 `TaskStore` format `4 -> 5`, `ENROLL-1c` step 2, and one-version store upgrade rule

- **Schema additions (`crates/master/src/store.rs`):**
  - Bump `STORE_FORMAT_VERSION` from `4` to `5` (protocol `10` stays untouched).
  - Add `#[serde(default, skip_serializing_if = "Option::is_none")] pub submitted_by_member: Option<String>`
    to `PersistedTaskEntry` and `submitted_by_member: Option<String>` to `JournalRecord::TaskSubmitted`
    (re-blessing `crates/master/tests/data/journal_layout.txt`).
  - Mirror `pub submitted_by_member: Option<String>` on `TaskEntry` (`crates/master/src/queue.rs`).
- **Revocation write-lock race guard (`ENROLL-1c`):** `TaskQueue::submit_attributed_checked` and
  `try_submit_slot_attributed` verify under `inner.write().await` (`MemberStore::contains_member_id`)
  that an enrolled submitting member has not been revoked between handshake and enqueue, rejecting a
  concurrent `SubmitTask` / `SubmitMapReduce` handler that ran after `RevokeMember`'s cancel scan.
- **One-version & clean-shutdown store upgrade rule ([design 0083](0083-one-version-window.md) D6,
  `DEPLOYMENT.md` section 3.5):**
  - `MIN_STORE_FORMAT_VERSION` remains `1` specifically to support the clean-shutdown snapshot
    upgrade path documented in `DEPLOYMENT.md` 3.5.
  - Both `Header` and `LoadedSnapshot` validate `format` alongside `protocol`:
    - If `snapshot.bin` was written by clean shutdown under format `1..=4` (including format `4` at
      protocol `10`!) and `journal.bin` contains **no uncompacted records** (header-only at `HEADER_LEN`),
      `TaskStore::open_with` deserializes the JSON snapshot cleanly (`submitted_by_member` defaults to
      `None` for pre-upgrade tasks) and rewrites `snapshot.bin` and `journal.bin` headers to
      `STORE_FORMAT_VERSION = 5`. Verified against real `v0.4.4` binary fixtures in
      `crates/master/tests/data/store_v0_4_4_{clean,dirty}/`.
    - If `journal.bin` holds **any uncompacted records** from an older `store_format != STORE_FORMAT_VERSION`
      or older `protocol_version != PROTOCOL_VERSION_CURRENT`, `parse_journal` **refuses to open**
      before rewriting anything, with an explicit `GridError::Storage` message instructing the operator
      to start the previous binary and stop it cleanly (`SIGINT`/`SIGTERM`) first.
    - Future store formats (`> 5`) and `0` are refused immediately. Downgrading a format `5` store
      back to `0.4.x` (format `4`) is not supported; operators must back up `--state-dir` before
      upgrading (`DEPLOYMENT.md` 3.5).
- **`--verify N` replica accounting (`crates/cli/src/verify.rs`):** Each `submit --verify N` (or
  MCP `verify`) replica counts against the submitter's participant quota. The CLI and MCP server
  attach a non-constraint `oxs:verify-replicas=<N>` tag to the initial task (`replica_task` strips
  it from replicas) so `submit_slot_inner` refuses `N > max_queued_tasks` with `QuotaExceeded` up
  front before running the first copy (zero wire change).
- **`ENROLL-1c` step 2 (`RevokeMember` cancels submitted tasks):**
  - When `ClientMessage::RevokeMember` revokes `member_id` (and also on admin revoke cleanup), after
    closing connections (step 1) and taking back its workers (step 3), the master calls
    `TaskQueue::cancel_tasks_submitted_by(&entry.id, "Submitting member revoked by admin")`:
    - Atomically transitions every non-terminal task (`Submitted`, `Queued`, `Retrying`, `Scheduled`,
      `Running`) whose `submitted_by_member == Some(member_id)` to `TaskState::Cancelled` (clearing
      ready queue / fair share / adoption holds and appending `JournalRecord::TaskTerminal`).
    - Dispatches `MasterMessage::CancelTask` to each executing worker and wakes any open waiter
      channels. Re-run `scheduler_notify.notify_one()` afterwards so freed worker slots start queued
      tasks of remaining members promptly.

### D5 Non-wire admin surface vs Protocol 11 wire-change boundary

- **Ships now (protocol `10`, no wire change):**
  - Master CLI/env/TOML (`crates/cli/src/cli_app.rs`, `master.rs`, `config.rs`, `docs/reference.md`):
    - `--default-participant-max-running <N>` (`RUSTY_GRID_DEFAULT_PARTICIPANT_MAX_RUNNING`)
    - `--default-participant-max-slots <N>` (`RUSTY_GRID_DEFAULT_PARTICIPANT_MAX_SLOTS`)
    - `--default-participant-max-queued <N>` (`RUSTY_GRID_DEFAULT_PARTICIPANT_MAX_QUEUED`)
    - `--default-participant-priority <low|normal|high>` (`RUSTY_GRID_DEFAULT_PARTICIPANT_PRIORITY`)
    - Repeatable `--participant-quota <PARTICIPANT:SPEC>` (and `[master].participant_quotas` map in TOML)
  - Master HTTP RBAC API & Cockpit UI (`crates/master/src/web_ui.rs`, `dashboard.html`):
    - `GET /api/quotas` (`AuthScope::Observer`): returns per-participant configured limits, live
      `running_tasks`, `running_slots`, `queued_tasks`, `priority_band`, `fair_share_weight`, and
      `wait_reason`. Follows `design 0094 D9(c)`: non-Admin Observers see labels only (or anonymized
      `"participant-N"` for unlabeled members), never raw `member_ids` or `MemberEntry.name`;
      Admins also receive enrolled member IDs and label assignment metadata.
    - `POST /api/quotas/default` (`AuthScope::Admin`): persists default `ParticipantQuota` in `members.json`.
    - `POST /api/quotas/participants/{participant}` (`AuthScope::Admin`): persists per-participant `ParticipantQuota` in `members.json`.
    - `POST /api/members/{member_id}/participant` (`AuthScope::Admin`): sets or clears a member's participant label in `members.json` and re-keys queued tasks live.
- **Stops at this design — Wire change: deferred to the protocol 11 train with `BUG-22` and `NET-10`:**
  - Adding `participant: Option<String>` to `MemberEntry` so `oxideswarm members list` displays the participant column over TCP/QUIC.
  - Extending `ClientResponse::ClusterStatus` with per-participant quota usage / wait reasons for `oxideswarm status`.
  - Adding `ClientMessage` variants for remote CLI administration (`oxideswarm members label <target> <label>`, `oxideswarm invite --participant <label>`, `oxideswarm quotas ...`) and an optional priority field on `ClientMessage::SubmitTask`.

## Compatibility

- **Wire protocol:** `PROTOCOL_VERSION_CURRENT` remains `10` (`PROTOCOL_VERSION_MIN_COMPATIBLE = 9`).
  Existing v9/v10 workers and CLI clients connect and run without changes, with participant quota
  enforcement and priority defaults applied transparently on the master.
- **Task store (`4 -> 5`):** Existing `--state-dir` deployments upgrading across format `4 -> 5` must
  stop the previous `0.4.x` master cleanly first (`SIGINT`/`SIGTERM`, compacting `journal.bin` into
  `snapshot.bin`) and keep a pre-upgrade `--state-dir` backup if rollback is needed.
- **`members.json`:** Keeps `version = 1` with `#[serde(default)]` optional maps so existing
  `members.json` files load seamlessly with zero migration step.

## Security

- **Trusted-circle scope ([design 0050](0050-scale-out-architecture.md) D6):** Quotas prevent accidental
  flooding and fair-share skew among trusted circle participants. A participant intentionally holding
  multiple keys without shared labels cannot be blocked cryptographically at Stage 1 (Sybil
  resistance waits for Stage 2 `NET-3`/`NET-4`).
- **RBAC & D9(c) privacy:** All mutating dashboard quota/label endpoints require `AuthScope::Admin`
  and refuse invalid labels / `"member:*"` spoofing. `GET /api/quotas` redacts raw `member_ids` and
  machine names for non-Admin `AuthScope::Observer` tokens.
- **Validation:** Zero caps (`0`) and empty/control-char/oversized labels are refused with `400`/exit
  error at CLI, TOML, HTTP, and `members.json` boundaries.

## Test plan

- **Unit tests:**
  - `crates/core/src/error.rs`: `GridError::QuotaExceeded` error code and transience rules.
  - `crates/core/src/placement.rs` & `crates/cli/src/verify.rs`: `VERIFY_REPLICAS_TAG_PREFIX` parsing and stripping from verification replicas.
  - `crates/master/src/store.rs`: format `4 -> 5` clean-shutdown JSON upgrade (`submitted_by_member`
    defaults to `None`, headers rewritten to `5`), refusal of uncompacted format-4 journal even at
    protocol `10`, real `v0.4.4` binary clean & dirty store fixtures (`tests/data/store_v0_4_4_{clean,dirty}/`),
    and re-blessed `tests/data/journal_layout.txt`.
  - `crates/master/src/quotas.rs`: label validation, `"member:*"` rejection, priority bands, unbanded
    priority clamping, `has_any_quota_configured`, quota spec parsing, and precedence matrix.
  - `crates/master/src/members.rs`: participant label & quota persistence across reload, revoke cleanup,
    and legacy `members.json` load compatibility.
  - `crates/master/src/fair_share.rs`: participant-keyed WRR, priority-tier ordering, and `walk_excluding`
    skipping capped submitters without starving others.
  - `crates/master/src/queue/recovery_tests.rs`: revocation write-lock guard (`submit_attributed_checked`)
    refusing a revoked member ID after revoke scan.
- **Loopback integration tests (`crates/master/tests/test_eco9_participant_quotas.rs`):**
  1. Running cap of 2 tasks never exceeded under burst submission; queued tasks wait with wait reason
     and finish once running tasks complete while uncapped participants suffer zero starvation.
  2. Slot cap (`max_slots`) bounds concurrent `sum(cpu_cores)` across multi-core tasks and rejects
     oversized single-task slot requests immediately (`QuotaExceeded`).
  3. Contention fair share: participant A submitting over 1 connection vs participant B submitting over
     20 connections share slots 1:1 (each gets half).
  4. Multi-machine grouping & re-labeling: two enrolled v3 keys sharing participant `"team-alice"`
     share one running/queued/slot cap; admin re-labeling via `/api/members/{id}/participant` updates
     caps and re-keys queued work cleanly.
  5. Queued cap (`max_queued_tasks`) returns typed `QuotaExceeded` error; priority bands order
     `High > Normal > Low` without starving lower-band work when capped.
  6. Persistence across master restart with `--state-dir`: `submitted_by_member`, participant labels,
     and configured caps survive restart.
  7. `ENROLL-1c` step 2: `RevokeMember` cancels active queued and running tasks submitted by the revoked
     member (and notifies executing workers) while leaving other members' tasks intact.
  8. MapReduce attribution & quota enforcement (`SubmitMapReduce` capped by `max_running_tasks` and
     visible on `/api/quotas`).
  9. Lowering `max_slots` fails stranded queued wide tasks as `Failed` (`Unfulfillable`) and wakes
     `wait: true` clients.
  10. Unbanded HTTP `POST /api/tasks` and `MasterHandle::submit_task_with_priority` clamp priority so
      they cannot starve `High`-band members.
  11. `--verify N` (`oxs:verify-replicas=N`) exceeding `max_queued_tasks` is refused up front.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| ECO-9 | Per-participant quotas: running tasks, slots, queued tasks, and priority band | `crates/core/src/error.rs`, `crates/master/src/quotas.rs`, `crates/master/src/members.rs`, `crates/master/src/fair_share.rs`, `crates/master/src/queue.rs`, `crates/master/src/scheduler.rs`, `crates/master/src/store.rs`, `crates/master/src/server.rs`, `crates/master/src/web_ui.rs`, `crates/master/src/dashboard.html`, `crates/cli/src/`, `README.md`, `docs/SECURITY.md` | Master quota enforcement, participant fair share, priority bands, store format `5` + upgrade test, dashboard API/UI, and loopback tests pass (CLI wire fields deferred to protocol 11 with `BUG-22` and `NET-10`) | Partial |
| ENROLL-1c | Step 2: persist submitting member and cancel revoked member's submitted tasks | `crates/master/src/store.rs`, `crates/master/src/queue.rs`, `crates/master/src/server/client_and_members.rs` | Format `4 -> 5` store upgrade test and loopback revocation-cancels-submitted-tasks test pass | Partial |

## Decisions (resolved 2026-11-19)

- **Store format bump & `ENROLL-1c` step 2 approved:** Coordinator approved bumping `TaskStore` format
  (`4 -> 5`) together with `ENROLL-1c` step 2 for `ECO-9` on `2026-11-19`.
- **Protocol 10 preserved:** All `ClientMessage` / `ClientResponse` / `MemberEntry` wire additions for
  CLI `members list` / `status` / `members label` stop at this design record and defer to the
  protocol 11 train with `BUG-22` and `NET-10`.

## Status log

- 2026-11-19: Claimed `design 0097` on the hub and wrote initial design record for `ECO-9` and `ENROLL-1c` step 2 on branch `mac/eco-9-participant-quotas`.
- 2026-11-19: Implemented master participant labels/quotas (`ECO-9`), `TaskStore` format `4 -> 5` and `ENROLL-1c` step 2 revocation cancellation, master CLI/TOML flags, RBAC HTTP `/api/quotas*` + `/api/members/{member_id}/participant` endpoints, Cockpit card, MapReduce attribution, lowered `max_slots` `Unfulfillable` failure, revocation write-lock guard, unbanded priority clamping, unconfigured quota controller bypass, `--verify N` upfront cap, real `v0.4.4` store fixtures, and 11-test loopback suite (`test_eco9_participant_quotas` passed `3/3` consecutive runs on macOS) on draft pull request 384; protocol 11 CLI RPC fields remain deferred with `BUG-22` and `NET-10`.
