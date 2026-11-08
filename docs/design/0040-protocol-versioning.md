# 0040: Real protocol versioning (`FEAT-8`)

Status: Implemented
Created: 2026-11-08    Updated: 2026-11-08

## Summary

Bump the wire protocol version from `2` to `3`, enforce `PROTOCOL_VERSION_CURRENT` and
`PROTOCOL_VERSION_MIN_COMPATIBLE` bumps in `wire_layout_tests.rs` whenever recorded bincode bytes
change, gate worker runtime capabilities on the `ProtocolCapabilities` bits acknowledged in
`MasterMessage::RegisterAck`, and add `protocol_version: u32` to `ClientMessage` and
`ClientResponse` so mismatched clients, workers, and masters refuse each other with a clear version
error on both sides.

## Context

Before this change:

1. `crates/core/src/wire_layout_tests.rs` pinned canonical bincode encodings in
   `crates/core/tests/data/wire_layout.txt`, but regenerating the file with `UPDATE_WIRE_LAYOUT=1`
   did not verify that `PROTOCOL_VERSION_CURRENT` (`crates/core/src/protocol.rs:31`) or
   `PROTOCOL_VERSION_MIN_COMPATIBLE` (`crates/core/src/protocol.rs:33`) had been incremented.
2. Both Master (`crates/master/src/server.rs`) and Worker (`crates/worker/src/client.rs`) checked
   only `protocol_version < PROTOCOL_VERSION_MIN_COMPATIBLE`, accepting peers with a newer,
   incompatible `protocol_version > PROTOCOL_VERSION_CURRENT`.
3. `WorkerClient::perform_handshake` ignored the `features` field of `MasterMessage::RegisterAck`,
   never intersecting or checking acknowledged `ProtocolCapabilities` bits before sending
   `MobileTelemetry`, `Checkpoint`, or `BlobRequest` frames, scaling keepalives with
   `ADAPTIVE_KEEPALIVE`, or running tasks with `staged_files` or `inputs`.
4. `ClientMessage` and `ClientResponse` carried no `protocol_version` field in either
   `BinaryClientMessage` / `BinaryClientResponse` or `HumanClientMessage` / `HumanClientResponse`.
   A client and Master from different protocol versions failed with an opaque bincode decode or EOF
   error.

## Goals

- **G1:** Changing `Task` or any protocol message's bincode layout fails `wire_layout_tests` (even
  under `UPDATE_WIRE_LAYOUT=1`) unless `PROTOCOL_VERSION_CURRENT` (and
  `PROTOCOL_VERSION_MIN_COMPATIBLE` when an existing variant's bytes change) is bumped.
- **G2:** A worker and Master with incompatible `protocol_version` values (`< PROTOCOL_VERSION_MIN_COMPATIBLE`
  or `> PROTOCOL_VERSION_CURRENT`) refuse registration with an explicit version message on both
  sides.
- **G3:** A client and Master with incompatible `protocol_version` values refuse the request with
  an explicit version message on both sides rather than an opaque bincode error.
- **G4:** `WorkerClient` stores the acknowledged `RegisterAck.features` intersection and checks the
  relevant `ProtocolCapabilities` bit before using each feature.
- **G5:** Record the migration plan off unmaintained `bincode` 1.x (`RUSTSEC-2025-0141`).

## Non-goals

- Rolling multi-version wire compatibility across different protocol versions: because both sides
  enforce `PROTOCOL_VERSION_MIN_COMPATIBLE..=PROTOCOL_VERSION_CURRENT` (`3..=3`), **every bump of
  `PROTOCOL_VERSION_CURRENT` breaks older peers** (an older v3 peer will refuse a v4 peer even if
  the change was purely additive). All nodes in a cluster must run matching builds.
- Replacing `bincode` 1.x in this pull request (see D4 for the migration plan).

## Design

### D1: Golden-bytes version-bump enforcement (`crates/core/src/wire_layout_tests.rs`, `.github/workflows/ci.yml`)

`render_wire_layout` writes `# protocol_version: 3` and `# protocol_version_min_compatible: 3` at
the top of `crates/core/tests/data/wire_layout.txt`. Both header lines are mandatory in normal and
`UPDATE_WIRE_LAYOUT=1` runs (`try_parse_wire_layout_versions` fails if either header or the file
itself is missing in update mode, preventing bypass via `rm wire_layout.txt` or header deletion).
`verify_version_bump_for_layout_change` compares the recorded layout and versions against
`current_wire_layout()`, `PROTOCOL_VERSION_CURRENT`, and `PROTOCOL_VERSION_MIN_COMPATIBLE` during
normal test runs, in `UPDATE_WIRE_LAYOUT=1` mode, and against the git baseline copy in
`wire_layout_file_bumps_version_when_body_differs_from_origin_master`:

- In CI (`.github/workflows/ci.yml`), the baseline step fetches `+${{ github.event.before }}` on a
  `push` run and `+${{ github.base_ref || 'master' }}` on a `pull_request` run into the dedicated
  ref `refs/remotes/ci/wire-layout-base` (`WIRE_LAYOUT_BASELINE_REF`), so a `push` to `master`
  compares with the previous `master` commit and a pull request into a non-`master` branch compares
  with its target branch.
- Under `UPDATE_WIRE_LAYOUT=1`, `wire_layout_file_bumps_version_when_body_differs_from_origin_master`
  skips its standalone run so it never reads a half-written file, and
  `bincode_encoding_matches_the_recorded_wire_layout` runs `check_wire_layout_against_git_baseline`
  after writing `crates/core/tests/data/wire_layout.txt`.
- If any canonical encoding differs from `wire_layout.txt` (or the baseline ref's copy, named as
  `<baseline_ref>:crates/core/tests/data/wire_layout.txt` in the failure message) and
  `PROTOCOL_VERSION_CURRENT <= recorded_version`, the test fails and refuses to overwrite
  `wire_layout.txt`.
- If any previously recorded variant changed its bytes or was removed and
  `PROTOCOL_VERSION_MIN_COMPATIBLE < PROTOCOL_VERSION_CURRENT`, the test fails and requires bumping
  `PROTOCOL_VERSION_MIN_COMPATIBLE` to `PROTOCOL_VERSION_CURRENT`.

### D2: Acknowledged feature-bit enforcement (`crates/worker/src/client.rs`)

`WorkerClient` stores `(features & ProtocolCapabilities::all_supported()).bits()` from an accepted
`MasterMessage::RegisterAck` in `negotiated_features: AtomicU64`, exposed via
`WorkerClient::negotiated_features(&self) -> ProtocolCapabilities`:

- `MOBILE_TELEMETRY`: required (alongside `master_features::MOBILE_TELEMETRY`) before sending
  session `WorkerMessage::MobileTelemetry` frames.
- `BLOB_TRANSFER`: required (alongside `master_features::BLOBS`) before creating the session
  `BlobClient`, and checked in `handle_assign_task_with_checkpoint` before running tasks with
  `inputs` or `output_globs`.
- `FILE_STAGING`: checked in `handle_assign_task_with_checkpoint` before running tasks with
  `staged_files` or `expected_outputs`.
- `CHECKPOINT_DELTA`: gates passing `Some(outbound_tx)` as `checkpoint_tx` to
  `TaskRunner::execute_task_with_checkpoint`.
- `ADAPTIVE_KEEPALIVE`: gates cellular/idle keepalive scaling in `run_session` and
  `spawn_adaptive_heartbeat_task`.

### D3: Client-path protocol versioning (`crates/core/src/protocol.rs`, `crates/master/src/server.rs`, `crates/cli/src/connect.rs`)

- Every variant of `BinaryClientMessage` and `BinaryClientResponse` appends
  `protocol_version: u32`, and every variant of `HumanClientMessage` and `HumanClientResponse`
  carries `#[serde(default = "default_protocol_version")] protocol_version: u32`.
- `default_protocol_version()` returns `LEGACY_UNVERSIONED_PROTOCOL_VERSION = 2`, so any JSON peer
  that omits `"protocol_version"` — whether in `ClientMessage`, `ClientResponse`,
  `WorkerMessage::Register`, or `MasterMessage::RegisterAck` — is treated as protocol version `2`
  and refused (`ProtocolError::IncompatibleVersion { peer_version: 2, .. }` on the client path, or
  `!is_protocol_version_compatible(2)` during the worker registration handshake).
- `deserialize_message` uses `bincode_exact_deserialize` (`.with_fixint_encoding().reject_trailing_bytes()`)
  and checks `is_protocol_version_compatible(ver)` on `ClientMessage`, `ClientResponse` (except
  `ClientResponse::Error`, which is always decoded so refusal messages reach the caller), and
  `InboundMessage`.
- `LegacyV2BinaryClientMessage` and `LegacyV2BinaryClientResponse` allow a v3 Master or v3 client
  to recognize an unversioned v2 bincode frame and return
  `ProtocolError::IncompatibleVersion { peer_version: 2, current_version: 3, min_compatible_version: 3 }`
  (mapped to `GridError::IncompatibleVersion` without the worker `"Registration handshake failed:"`
  prefix) instead of a raw bincode EOF error.
- In `connect_master` (`crates/cli/src/connect.rs`), before returning the connected transport to
  any command (`submit`, `cancel`, `status`, `doctor`, `mcp`, ...), the client sends a
  side-effect-free `ClientMessage::ClusterStatus` probe and verifies the `ClientResponse::ClusterStatus`
  reply. Because a legacy v2 Master's bincode fallback ignores trailing bytes on `SubmitTask` and
  `CancelTask`, probing `ClusterStatus` inside `connect_master` guarantees a v3 client refuses a v2
  Master before any state-changing frame is ever transmitted. Because `ClusterStatus` builds the
  worker list on every connection (including blob and MCP pool connections, and twice for `status`),
  `W1` of [design 0039](0039-agent-workspaces.md) will replace this probe with a small
  `ClientMessage::Hello` / `ClientResponse::Hello` version variant appended last in the same release
  as `0039` W1's protocol version bump.
- When `authenticate_and_read_first_message` or `handle_client_connection` on the Master receives
  `ProtocolError::IncompatibleVersion { peer_version, .. }`, it replies with
  `ClientResponse::Error { message: format_incompatible_version_error("client", peer_version) }`
  in the client's detected wire codec before closing the connection.

### D4: Migration plan off `bincode` 1.x (`RUSTSEC-2025-0141`)

`bincode` 1.3.3 is unmaintained (`RUSTSEC-2025-0141`, currently listed in `deny.toml` `ignore`).
Because `WireCodec` and the 1-byte frame discriminator (`0x01` JSON, `0x02` Bincode, `0x03` Auth,
`0x04` AEAD) isolate payload serialization inside `serialize_message` and `deserialize_message` in
`crates/core/src/protocol.rs`, the workspace can migrate off `bincode` 1.x in a single crate
(`rusty_grid_core`):

1. **Drop-in wire-compatible upgrade (`bincode` 2.x):** Replace `bincode = "1.3"` in
   `crates/core/Cargo.toml` with `bincode` 2.x configured with
   `bincode::config::standard().with_fixed_int_encoding()`. In fixed-int little-endian mode,
   `bincode` 2.x produces the exact byte layout pinned in `crates/core/tests/data/wire_layout.txt`,
   verified by `bincode_encoding_matches_the_recorded_wire_layout`.
2. **Alternative compact format (`postcard`):** If replacing `bincode` with `postcard`, bump
   `PROTOCOL_VERSION_CURRENT` and `PROTOCOL_VERSION_MIN_COMPATIBLE` and regenerate
   `wire_layout.txt`. With D1–D3 in place, any mixed-version peer is refused at handshake with an
   explicit version mismatch error.

## Compatibility

- `PROTOCOL_VERSION_CURRENT` and `PROTOCOL_VERSION_MIN_COMPATIBLE` are bumped from `2` to `3`.
  Every bump of `PROTOCOL_VERSION_CURRENT` breaks older peers (`MIN == CURRENT`); all nodes in a
  cluster must run the same protocol version.
- **Upgrade order — master first:** Always upgrade and restart the Master before upgrading workers
  and clients:
  - An upgraded v3 Master immediately refuses v2 workers (`RegisterAck { accepted: false }`) and v2
    clients (`ClientResponse::Error`) before registering the worker or accepting any client task.
  - Conversely, a v3 worker connecting to an un-upgraded v2 Master sends `Register { protocol_version: 3 }`,
    which the v2 Master accepts (`protocol_version >= 2`) before replying with
    `RegisterAck { accepted: true, protocol_version: 2 }`. The v3 worker then rejects version `2`
    in `perform_handshake`, drops the connection, and retries with backoff; during that window the
    worker briefly appears in the v2 Master's `workers` list and any task the v2 Master assigns to
    it is requeued when the socket closes.
- A v2 worker connecting to a v3 Master (or a v3 worker connecting to a v2/v4 Master) is refused
  with `Incompatible protocol version <N> from worker/Master (supported: 3..=3; upgrade both master and worker to matching builds)`.
- A v2 client connecting to a v3 Master (or a v3 client connecting to a v2/v4 Master) is refused
  during `connect_master`'s pre-flight `ClusterStatus` check with `ClientResponse::Error` /
  `GridError::IncompatibleVersion` naming both the peer version and the supported version range
  `3..=3`, before any state-changing request (`SubmitTask`, `CancelTask`) is sent.

## Security

- `bincode_exact_deserialize` enforces `.reject_trailing_bytes()` and `.with_limit(slice.len())`,
  removing the trailing-bytes fallback in `InboundMessage` that could misclassify truncated worker
  frames as `ClientMessage`.
- Unauthenticated malformed or truncated frames continue to be dropped immediately without sending
  a response body.

## Test plan

- `crates/core/src/wire_layout_tests.rs`: `changed_task_layout_fails_without_version_bump` and
  `bincode_encoding_matches_the_recorded_wire_layout`.
- `crates/core/src/protocol.rs`: unit tests for client/response version skew in Bincode and JSON,
  legacy v2 bincode client/response recognition, and `ClientResponse::Error` pass-through.
- `crates/worker/src/client.rs`: unit tests for worker handshake refusal on lower (`1`) and higher
  (`99`) Master protocol versions and for `RegisterAck` feature-bit gating (`FILE_STAGING`,
  `BLOB_TRANSFER`, `CHECKPOINT_DELTA`, `MOBILE_TELEMETRY`).
- `crates/master/src/server.rs`: integration tests for bilateral worker/Master and client/Master
  protocol version mismatch refusal.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| `FEAT-8` | Real protocol versioning, golden-bytes version-bump check, `RegisterAck` feature-bit gating, client-path versioning | `crates/core/src/protocol.rs`, `crates/core/src/error.rs`, `crates/core/src/wire_layout_tests.rs`, `crates/core/tests/data/wire_layout.txt`, `crates/worker/src/client.rs`, `crates/master/src/server.rs`, `crates/cli/src/connect.rs` | All 4 `FEAT-8` checks in `docs/BACKLOG.md` pass | Implemented |

## Status log

- 2026-11-08: Implemented in protocol version 3 (`FEAT-8`).
- 2026-11-08: `FEAT-8b` follow-ups: fetch `+${{ github.event.before }}` (push) or
  `+${{ github.base_ref || 'master' }}` (PR) into `refs/remotes/ci/wire-layout-base` in CI, run the
  baseline check after regeneration under `UPDATE_WIRE_LAYOUT=1` and name the baseline ref in
  failure messages, document JSON `Register` / `RegisterAck` refusal when `"protocol_version"` is
  omitted, and schedule the lightweight `Hello` version probe variant for `0039` W1.
