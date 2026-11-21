# 0098: Protocol 11 train (BUG-22 base, NET-10, ECO-9)

Status: Draft
Created: 2026-11-19    Updated: 2026-11-19

## Summary

Establishes the base of the shared protocol 11 train: bumps `PROTOCOL_VERSION_CURRENT` to `11` and `PROTOCOL_VERSION_MIN_COMPATIBLE` to `10`, promotes release `v0.4.4`'s wire layout to `wire_layout_previous.txt`, implements `BUG-22` (`cancelled_tasks` in `ClientResponse::ClusterStatus`, `oxideswarm status`, and MCP `status`) with v10 response folding, and holds as a draft pull request until the `0.4.x` stability release ships and follow-on wire pull requests for `NET-10` and `ECO-9` land on top.

## Context

- [Design 0076](0076-protocol-compatibility-and-release-train.md) (D1) requires a release within 48 hours of any `PROTOCOL_VERSION_CURRENT` bump and before the next wire change.
- [Design 0083](0083-one-version-window.md) keeps a one-version compatibility window (`PROTOCOL_VERSION_MIN_COMPATIBLE = PROTOCOL_VERSION_CURRENT - 1`) governed by golden layout files (`crates/core/src/wire_layout_tests.rs`, `crates/core/src/window_tests.rs`).
- Today, three open items need additive wire changes on `ClientMessage` / `ClientResponse`:
  - `BUG-22`: `TaskQueue::cluster_buckets` (`crates/master/src/queue.rs`) adds cancelled tasks into `failed_tasks` in `ClientResponse::ClusterStatus` (`crates/core/src/protocol.rs`), while the web dashboard (`crates/master/src/web_ui.rs`) and `/metrics` already count `failed` and `cancelled` separately.
  - `NET-10` (Design 0096, Option B on [draft pull request 379](https://github.com/DuongNAD/OxideSwarm/pull/379)): deferred CLI display-only credit RPC/fields.
  - `ECO-9` (Design 0097, [draft pull request 384](https://github.com/DuongNAD/OxideSwarm/pull/384)): deferred remote participant label, quota usage, and priority fields.
- Rather than spending separate protocol bumps (`11`, `12`, `13`) back-to-back during the stability release cycle, all three share protocol version `11`.

## Goals

- **G1:** Separate `cancelled_tasks` from `failed_tasks` in `ClientResponse::ClusterStatus`, `oxideswarm status`, `oxideswarm status --json`, and MCP `status` (`BUG-22`).
- **G2:** Preserve the one-version compatibility window (`10..=11`) so `v0.4.x` (`protocol 10`) workers and clients stay compatible while upgrading to protocol `11`, while refusing `v0.3.0` (`protocol 9`) peers cleanly.
- **G3:** Name every **Planned** `NET-10` and `ECO-9` wire field scheduled for the protocol `11` train so follow-on pull requests share protocol `11` without ad-hoc wire drift.
- **G4:** Enforce [Design 0076](0076-protocol-compatibility-and-release-train.md) D1 and `DEPLOYMENT.md` section 3.5 upgrade ordering by keeping this base pull request in draft until the stability release ships and the train pull requests coordinate.

## Non-goals

- Implementing `NET-10` or `ECO-9` wire fields, ledger state, or store migrations in this base pull request (those remain **Planned** on the protocol `11` train and land in their own pull requests).
- Merging protocol `11` to `master` before the `0.4.x` stability release is tagged.
- Allowing protocol `9` (`v0.3.0`) peers onto a protocol `11` master or breaking the `is_master_echo_acceptable` echo rule (`PROTOCOL_VERSION_MIN_COMPATIBLE` advances to `10`).

## Design

### D1 Protocol 11 version window and BUG-22 wire change

- **`crates/core/src/protocol.rs`:**
  - `PROTOCOL_VERSION_CURRENT` moves from `10` to `11`.
  - `PROTOCOL_VERSION_MIN_COMPATIBLE` moves from `9` to `10`.
  - `ClientResponse::ClusterStatus` appends `cancelled_tasks: usize` after `failed_tasks`.
- **`crates/core/src/protocol/client_resp_mirrors.rs`:**
  - `HumanClientResponse::ClusterStatus` and `BinaryClientResponse::ClusterStatus` append `cancelled_tasks: usize`, updated across all four `From` conversions.
  - `HumanClientResponse::ClusterStatus.cancelled_tasks` carries `#[serde(default)]`; `BinaryClientResponse` has no `#[serde(skip_serializing_if)]` (banned on bincode mirror types by `crates/core/src/lib.rs`).
- **Master and CLI (`BUG-22`):**
  - `TaskQueue::cluster_buckets` (`crates/master/src/queue.rs`) returns `(pending, running, completed, failed, cancelled)` separately; `crates/master/src/server/client_and_members.rs` and `crates/master/src/web_ui.rs` populate `failed_tasks` and `cancelled_tasks` independently. (`/api/status` and Prometheus `/metrics` already keep both counts separate.)
  - `crates/cli/src/cli_app/tasks.rs` adds `"cancelled_tasks"` to `oxideswarm status --json` and prints `Cancelled Tasks:  <n>` in plain `oxideswarm status`.
  - `crates/cli/src/mcp/status.rs` adds `"cancelled_tasks"` to the MCP `status` tool payload.

### D2 Planned follow-on fields on the protocol 11 train

The fields below are **Planned** and must be added by their own pull requests on top of protocol `11` (not implemented in this base pull request):

- **`NET-10` (Design 0096, [draft pull request 379](https://github.com/DuongNAD/OxideSwarm/pull/379)) — Planned:**
  - Display-only contribution credits (`ran_for_others`, `used_from_others`, `self_run` across `24h`, `7d`, and `all_time` windows covering tasks, core-ms/core-s, and GPU-ms/GPU-s; strictly non-monetary with no signed net or `"balance"` / `"debited"` wording):
    - Remote query/response variants appended last: `ClientMessage::ListCredits` and `ClientResponse::CreditsList`.
    - Optional trailing credit summary fields appended to `ClientResponse::ClusterStatus` and `ClientResponse::MembersList`.
  - That pull request must extend `ClientResponse::for_peer(version)` and the v10 legacy mirrors so v10 peers reset/drop trailing fields cleanly, and must refresh `crates/core/tests/data/wire_layout.txt` under `# protocol_version: 11`.
- **`ECO-9` (Design 0097, [draft pull request 384](https://github.com/DuongNAD/OxideSwarm/pull/384)) — Planned:**
  - `MemberEntry::participant: Option<String>` inside `ClientResponse::MembersList` (for `oxideswarm members list`).
  - Trailing nested participant quota usage field (for example `participant_usage: Option<...>`, holding per-participant `running_tasks`, `running_slots`, `queued_tasks`, `priority_band`, `fair_share_weight`, and `wait_reason`, avoiding collision with `ClusterStatus.running_tasks`) appended to `ClientResponse::ClusterStatus`.
  - Remote admin `ClientMessage` and `ClientResponse` variants appended last for `oxideswarm members label`, `oxideswarm invite --participant`, and `oxideswarm quotas`.
  - Task submission priority encoded as a task extension ([Design 0082](0082-task-extension-slot.md)) rather than a typed field on `ClientMessage::SubmitTask`.
  - That pull request must extend `ClientResponse::for_peer(version)` and the v10 legacy mirrors (including `MembersList` and a v10 mirror of `MemberEntry`) so protocol `10` peers omit/reset trailing fields cleanly, and must refresh `crates/core/tests/data/wire_layout.txt` under `# protocol_version: 11`.

### D2.1 Guardrails for NET-10 and ECO-9 follow-on pull requests

When `NET-10` and `ECO-9` land their wire additions on top of protocol `11`, both pull requests must satisfy all six rules below:

1. **Use constants, not literal `11`, and handle every changed variant:** `ClientResponse::for_peer` (`crates/core/src/protocol.rs:881`) and `serialize_client_response_for_peer` (`crates/core/src/protocol/client_resp_mirrors.rs:1473`) currently check `< 11` literally and special-case only `ClusterStatus`. Replace literal `11` with `PROTOCOL_VERSION_CURRENT` (and `PROTOCOL_VERSION_MIN_COMPATIBLE`) and handle every changed variant (`ClusterStatus`, `MembersList`, etc.).
2. **Reset trailing `ClusterStatus` fields in `for_peer(10)`:** Every new trailing `ClusterStatus` field added by `NET-10` or `ECO-9` must be reset by `ClientResponse::for_peer(10)` to the exact default that a v10 reader (`deserialize_message_as_v10`) produces (`None`, empty, `0`, etc.); otherwise `window_tests.rs` D7.2's `read == resp.for_peer(10)` check fails.
3. **`MembersList` v10 legacy mirror and positional indices:** When `ECO-9` (or `NET-10`) modifies `ClientResponse::MembersList` / `MemberEntry`, `LegacyV10BinaryClientResponse` and `HumanLegacyV10ClientResponse` need a `MembersList` variant that lists every preceding variant up to `MembersList`'s positional bincode index, together with a v10 mirror of `MemberEntry` without `participant`, an extended tag check in `deserialize_message_as_v10`, and `MembersList` entries in `DIFFERS_FROM_PREVIOUS` (`crates/core/src/window_tests.rs`).
4. **Avoid `ClusterStatus.running_tasks` name collision:** `ClientResponse::ClusterStatus` already has a top-level `running_tasks: usize` field. `ECO-9` must nest per-participant quota usage (e.g., `participant_usage: Option<...>`) or rename its fields rather than introducing a duplicate top-level `running_tasks` name.
5. **Carry `SubmitTask` priority as a task extension ([Design 0082](0082-task-extension-slot.md)):** Do not add a typed priority field to `ClientMessage::SubmitTask`; a typed field would require re-introducing a v10 `SubmitTask` decoder and the stamp-decides-layout logic that this base pull request removed.
6. **Variant ordering, bincode attribute rule, and frozen baseline:** Append new `ClientMessage` and `ClientResponse` variants strictly last; never use `#[serde(skip_serializing_if)]` on `Option` fields of `Binary*` mirror structs/enums; and keep `crates/core/tests/data/wire_layout_previous.txt` pinned verbatim to `v0.4.4`.

### D3 Serving v10 peers across the 10..=11 window

- **Inbound decoding (`crates/core/src/protocol/codec.rs`):**
  - Protocol `10` `ClientMessage` and `WorkerMessage` frames already carry the v10 `Task` layout with extensions, so v10 requests decode directly through the current decoder with no `SubmitTask` shape shim needed.
  - Protocol `9` peers are outside `10..=11` and are refused with `IncompatibleVersion(9)`. `LegacyV9TaskRequirements`, `LegacyV9Task`, and `LegacyV9BinaryClientMessage` (`crates/core/src/protocol/client_msg_mirrors.rs`) remain as refusal-only `Deserialize` decoders so v9 `SubmitTask` frames return `Incompatible protocol version 9` instead of an opaque decode error.
  - `BinaryMasterMessage` in `crates/core/src/protocol/worker_master_mirrors.rs` uses `Task` directly (unused v9 bincode helpers removed while keeping `master_message_for_peer`).
- **Outbound encoding (`ClientResponse::for_peer` and `serialize_message_for_peer`):**
  - `ClientResponse::for_peer(version)`: for `version < 11` (`10`), folds `cancelled_tasks` into `failed_tasks` (`failed_tasks.saturating_add(cancelled_tasks)`) and resets `cancelled_tasks = 0`, preserving total terminal non-succeeded counts for v10 clients.
  - `serialize_client_response_for_peer` translates v10 replies through `LegacyV10BinaryClientResponse` (bincode, positional without `cancelled_tasks`) and `HumanLegacyV10ClientResponse` (JSON without `cancelled_tasks`), stamping `protocol_version: 10`.
  - `decode_bincode_client_response` uses `LegacyV10BinaryClientResponse` as a refusal check for un-bumped replies.
  - `WorkerSession` (`crates/master/src/session.rs`) logs a once-per-session warning and tags `proto:10` on protocol `10` workers across control and data lanes.

### D4 Golden wire layout and cross-version tests

- `crates/core/tests/data/wire_layout_previous.txt` is promoted verbatim (LF line endings) from release `v0.4.4`'s `crates/core/tests/data/wire_layout.txt` (`# protocol_version: 10`, `# protocol_version_min_compatible: 9`).
- `crates/core/tests/data/wire_layout.txt` is regenerated for `# protocol_version: 11` and `# protocol_version_min_compatible: 10`, with `wire_layout_tests.rs` setting the `ClusterStatus` sample to `failed_tasks: 0, cancelled_tasks: 1` deliberately so the folded v10 sum (`0 + 1 = 1`) equals `v0.4.4`'s recorded `1` (`window_tests.rs` D7.2 can therefore compare the folded v10 bytes directly against `wire_layout_previous.txt`).
- `crates/core/src/window_tests.rs` sets `PREVIOUS = 10` (`DIFFERS_FROM_PREVIOUS` = `ClusterStatus`, `Hello`) and replays `wire_layout_previous.txt` against `serialize_v10_client_message`, `deserialize_message_as_v10`, and `serialize_message_for_peer`.
- `tests/window_v10_peers.rs` (registered in `crates/master/Cargo.toml`, replacing `tests/window_v9_peers.rs`) exercises v10 workers, v10 JSON/bincode clients, `(failed: 1, cancelled: 1)` on v11 vs `(failed: 2)` folded on v10, and v9 refusal.

### D5 Release obligation and upgrade order

- **Hold rule ([Design 0076](0076-protocol-compatibility-and-release-train.md) D1):** Keep this pull request in **Draft** until the `0.4.x` stability release is tagged and the protocol `11` train pull requests (`NET-10`, `ECO-9`) are coordinated onto this base.
- **48-hour release rule:** Once protocol `11` merges to `master`, cut a release within 48 hours and before the next wire change (`protocol 12`).
- **Master-first cluster upgrade order (`DEPLOYMENT.md` section 3.5):**
  1. Verify `oxideswarm workers` shows no remaining `proto:9` (`v0.3.0`) workers, upgrading those to `v0.4.x` first because protocol `11` refuses protocol `9`.
  2. Stop the master cleanly (so `--state-dir` compacts) and upgrade/restart the master to protocol `11` first.
  3. Upgrade `proto:10` (`v0.4.x`) workers and clients in rolling order; they stay connected against the protocol `11` master until upgraded. A protocol `10` master still refuses protocol `11` peers (`is_master_echo_acceptable`).

## Compatibility

- **Window:** `PROTOCOL_VERSION_MIN_COMPATIBLE = 10`, `PROTOCOL_VERSION_CURRENT = 11`.
- **Upgrading master (v10 -> v11) with v10 workers/clients:** Supported for `10`; `ClusterStatus` folds `cancelled_tasks` into `failed_tasks` for v10 callers; v9 (`v0.3.0`) peers are refused cleanly with `Incompatible protocol version 9 (server accepts 10..=11)`.
- **Newer peer against older master:** Refused cleanly on `Hello` or `Register` because an un-upgraded v10 master enforces `9..=10` and `is_master_echo_acceptable` rejects mismatched echoes.
- **Follow-on PR coordination:** Any **Planned** `NET-10` or `ECO-9` additions re-run `UPDATE_WIRE_LAYOUT=1` for `wire_layout.txt` (keeping `wire_layout_previous.txt` pinned at `v0.4.4` unless an incompatible breaking change closes the window).

## Security

- Protocol `9` refusal preserves the explicit `IncompatibleVersion` rejection path on untrusted/stale sockets without silent fallthrough.
- `ClientResponse::ClusterStatus.cancelled_tasks` is an aggregate cluster counter only and introduces no secret, identity, or credential exposure (and future `NET-10` / `ECO-9` train PRs enforce [Design 0094](0094-road-to-a-shared-research-grid.md) D9(c) observer redaction in their own PRs).
- Legacy bincode refusal structs remain strict and keep `skip_serializing_if` forbidden on binary mirror types.

## Test plan

- **Unit (`crates/core`):** `cargo +1.96.0 test --locked -p rusty_grid_core --lib` (`wire_layout_tests`, `window_tests`, `version_and_aead_tests`, `skip_serializing_if_is_kept_off_bincode_types`).
- **Workspace unit & clippy matrix:** `cargo +1.96.0 clippy` across default, `--features 3d`, `--features otel`, CLI `--features gpu-wgpu`, `master --no-default-features`, and `cargo +1.96.0 test --workspace --lib --locked`.
- **Cross-version & queue/failover (`crates/master`):** `window_v10_peers` (v10 workers/clients, folded `(2, 0)` vs split `(1, 1)` `ClusterStatus`, v9 refusal), `master_failover_cancel_liveness`, and `session::tests::test_multiplexed_v10_worker_gets_v10_on_both_lanes`.
- **CLI & MCP E2E (`crates/cli`):** `test_status_cancelled_tasks` (1 failed + 1 cancelled verified on `status --json`, plain `status`, and MCP `status`), `test_hello_probe_e2e`, `test_cli_cancel_and_result`, and `mcp_server_e2e`.
- **Release skew script:** `python tests/test_release_skew.py`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| BUG-22 | Count cancelled tasks apart from failed tasks in `status --json` (protocol 11 base) | `crates/core/src/protocol.rs`, `crates/core/src/protocol/client_resp_mirrors.rs`, `crates/core/src/protocol/client_msg_mirrors.rs`, `crates/core/src/protocol/worker_master_mirrors.rs`, `crates/core/src/protocol/codec.rs`, `crates/core/src/wire_layout_tests.rs`, `crates/core/src/window_tests.rs`, `crates/core/tests/data/wire_layout.txt`, `crates/core/tests/data/wire_layout_previous.txt`, `crates/master/src/queue.rs`, `crates/master/src/server/client_and_members.rs`, `crates/master/src/web_ui.rs`, `crates/master/src/session.rs`, `crates/cli/src/cli_app/tasks.rs`, `crates/cli/src/mcp/status.rs`, `tests/window_v10_peers.rs`, `crates/cli/tests/test_status_cancelled_tasks.rs` | `1` failed and `1` cancelled task report `1` and `1` across `ClusterStatus`, `status --json`, `status`, and MCP `status`, v10 peers fold cleanly, and protocol 11 merges and releases | Partial |
| NET-10 (wire follow-on) | Protocol 11 remote `ListCredits` / summary fields (Design 0096, [draft PR #379](https://github.com/DuongNAD/OxideSwarm/pull/379)) | `crates/core/src/protocol.rs`, `crates/core/src/protocol/client_resp_mirrors.rs`, `crates/core/src/protocol/codec.rs`, `crates/master/`, `crates/cli/` | Follow-on PR adds `NET-10` wire variants/fields on protocol `11`, extends `for_peer(10)`, refreshes `wire_layout.txt`, and passes CLI/cross-version tests | Planned |
| ECO-9 (wire follow-on) | Protocol 11 participant/quota/priority CLI wire fields (Design 0097, [draft PR #384](https://github.com/DuongNAD/OxideSwarm/pull/384)) | `crates/core/src/protocol.rs`, `crates/core/src/noise_auth.rs`, `crates/core/src/protocol/client_resp_mirrors.rs`, `crates/core/src/protocol/codec.rs`, `crates/master/`, `crates/cli/` | Follow-on PR adds `MemberEntry::participant`, nested `ClusterStatus` participant usage/wait fields, admin RPCs, and priority task extension on protocol `11`, extends `for_peer(10)`, and passes tests | Planned |
| CACHE-1 (wire follow-on) | Protocol 11 `TaskResult::cached: bool` and `action-cache` task extension `id = 1` ([Design 0100](0100-action-cache.md)) | `crates/core/src/task/extension.rs`, `crates/core/src/task/action_cache.rs`, `crates/core/src/task.rs`, `crates/core/src/protocol.rs`, `crates/core/src/protocol/client_resp_mirrors.rs`, `crates/core/src/protocol/codec.rs`, `crates/core/src/window_tests.rs`, `crates/master/`, `crates/cli/` | Follow-on PR adds `TaskResult::cached` and `action-cache` extension `id = 1` on protocol `11`, preserves v10 `TaskCompleted` decoding via `LegacyV10BinaryTaskResult` / `HumanLegacyV10TaskResult`, refreshes `wire_layout.txt`, and passes unit, `window_v10_peers`, and `action_cache_e2e` tests | Partial |

## Status log

- 2026-11-19: Claimed `design 0098` on the hub and wrote initial draft for the protocol `11` train base on branch `win/protocol-11-train` (`BUG-22` implemented on draft pull request; `NET-10` and `ECO-9` wire additions **Planned** on the same protocol `11` train).
- 2026-11-19: Applied review fixes for draft PR #386: replaced links to draft-only designs 0096 and 0097 with PR URLs, clarified why `wire_layout_tests.rs` sets `failed_tasks: 0, cancelled_tasks: 1` for D7.2 folded byte comparison with `v0.4.4`, added `D2.1` guardrails for `NET-10` and `ECO-9` (use constants not literal `11`, reset trailing fields in `for_peer(10)`, `MembersList` legacy mirror + positional indices + `DIFFERS_FROM_PREVIOUS`, nest participant usage to avoid `running_tasks` collision, encode `SubmitTask` priority as a design 0082 task extension, append new variants last without `skip_serializing_if` on `Binary*`, keep `wire_layout_previous.txt` pinned at `v0.4.4`), and updated `CHANGELOG.md` for `0.3.0`/protocol 9 refusal and master-first upgrade order.
- 2026-11-22: Added follow-on coordination entry for `CACHE-1` ([Design 0100](0100-action-cache.md)) appending `TaskResult::cached: bool` and registering task extension `id = 1` (`action-cache`) on the protocol `11` train.
