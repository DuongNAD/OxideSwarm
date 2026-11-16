# 0083: A one-version compatibility window

Status: Implemented by draft pull request PRNUM (PROTO-4 of design 0076 D4; based on draft pull request 272, design 0082)
Created: 2026-11-17    Updated: 2026-11-17

## Summary

From protocol 10 on, a master accepts workers and clients that speak `PROTOCOL_VERSION_CURRENT - 1`:
`PROTOCOL_VERSION_MIN_COMPATIBLE` becomes `PROTOCOL_VERSION_CURRENT - 1` (10 accepts 9). The master
learns each peer's version, encodes everything it sends to that peer in that peer's layout, and
decodes the older layout it receives. Only three messages differ between 9 and 10, because only
`Task` and `TaskRequirements` changed: `ClientMessage::SubmitTask`, `MasterMessage::AssignTask` and
`MasterMessage::AssignTaskWithCheckpoint`. A newer worker or client is still refused by an older
master, so the upgrade order is master first. A change that cannot be additive closes the window for
its version, and its pull request says so.

## Context

- Versions: `PROTOCOL_VERSION_CURRENT` and `PROTOCOL_VERSION_MIN_COMPATIBLE` are both `10` on the
  PROTO-3 branch (`crates/core/src/protocol.rs:42-44`). `is_protocol_version_compatible` is the one
  range check (`protocol.rs:55-57`). The refusal text is `Incompatible protocol version <n> from
  <role> (supported: MIN..=CUR; ...)` (`protocol.rs:61-72`); the skew job parses it.
- Every `ClientMessage` and `ClientResponse` carries a `protocol_version`, stamped with
  `PROTOCOL_VERSION_CURRENT` by the `From` impls (`crates/core/src/protocol/client_msg_mirrors.rs:675`,
  `:762`; `client_resp_mirrors.rs:950`, `:1099`). The public enums have no such field, so the decoder
  checks the stamp and drops it (`codec.rs:307-317`). A response stamp is checked by the receiver
  except on `ClientResponse::Error` (`codec.rs:349-357`). A worker sends its version in
  `WorkerMessage::Register` (`protocol.rs:277-282`), and the master answers with
  `MasterMessage::RegisterAck.protocol_version`, always `PROTOCOL_VERSION_CURRENT`
  (`crates/master/src/session.rs:891`), which the worker range-checks
  (`crates/worker/src/client/session.rs:1326-1330`).
- The master refuses an out-of-range worker in `register` (`session.rs:723-734`) and an out-of-range
  client by answering `ClientResponse::Error` and closing (`crates/master/src/server/client_and_members.rs:166-177`,
  `server/handshakes.rs:593`). `connect_master` sends `ClientMessage::Hello` first on every client
  connection and maps an error reply that contains "Incompatible protocol version" to
  `GridError::IncompatibleVersion` (`crates/cli/src/connect.rs:606-638`). The master answers `Hello`
  with `ClientResponse::Hello { min_compatible_version }` (`client_and_members.rs:386-391`).
- Which messages changed in 10: `Task` and `TaskRequirements` gained `extensions` last (design 0082
  D1). They are embedded without a mirror, so the messages that carry them change: `SubmitTask`
  (`protocol.rs:655`), `AssignTask` and `AssignTaskWithCheckpoint` (`protocol.rs:434-439`).
  `RoleHandoverAck` also holds `Vec<Task>` (`protocol.rs:451-456`), but the master only sends it
  with an empty list (`crates/master/src/server/handshakes.rs:773-781`), and an empty `Vec` encodes
  the same in both versions. Comparing `crates/core/tests/data/wire_layout.txt` on this branch with
  `origin/master` (protocol 9), the encoded length of exactly `ClientMessage::SubmitTask`,
  `MasterMessage::AssignTask`, `MasterMessage::AssignTaskWithCheckpoint` and
  `MasterMessage::RoleHandoverAck` differs; every other entry differs only in the stamp (`09000000`
  to `0a000000`) on `ClientMessage` and `ClientResponse`. No `ClientResponse` variant carries a
  `Task`, so the replies are the same bytes apart from the stamp.
- The frozen v9 decoders of PROTO-3: `LegacyV9TaskRequirements`, `LegacyV9Task` and
  `LegacyV9BinaryClientMessage` (`client_msg_mirrors.rs:292-360`). They are `Deserialize` only and
  `decode_bincode_client_message` uses them only to refuse a v9 `SubmitTask` (`codec.rs:336-343`).
  They sit beside the v5 and v6 to v8 ones (`client_msg_mirrors.rs:244-291`, `:329-350`).
- Where bytes are made: `serialize_message` is generic over `M: Serialize` and has no version
  argument (`codec.rs:250-279`); `MessageTransport` and `MessageWriter` keep a codec but no peer
  version (`codec.rs:592-600`, `:1060-1068`). `last_detected_codec` is the existing per-connection
  read-side fact (`codec.rs:602`).
- Placement for extensions (PROTO-3): `Task::strip_master_extensions` removes every `Master`-scope
  extension from the worker's copy in the one place that builds `AssignTask`
  (`crates/master/src/scheduler.rs:2091-2104`, `crates/core/src/task/extension.rs:370-376`); a task
  with a known critical `Worker`/`Both` extension needs the worker tag `ext:<id>`
  (`crates/core/src/placement.rs:48`, `:52`).
- The wire layout test records the bytes of every canonical message and enforces version bumps in
  `verify_version_bump_for_layout_change` (`crates/core/src/wire_layout_tests.rs:1392-1440`): a
  changed or removed variant requires `MIN == CURRENT`. The stamp makes every `ClientMessage` and
  `ClientResponse` entry change on each bump, so today that rule always fires.
- Store: `STORE_FORMAT_VERSION = 4`, `MIN_STORE_FORMAT_VERSION = 1` (`crates/master/src/store.rs:97`,
  `:102`). A journal with records written by another protocol version is refused
  (`store.rs:1158-1166`); an empty journal and the JSON snapshot carry over (`store.rs:372-375`,
  `:1155`).
- Handshake and record layer: the v2 cluster handshake has `AUTH_VERSION = 2` and carries no protocol
  version (`crates/core/src/cluster_auth.rs:92`). The v3 (Noise) hello carries the protocol version,
  checks it with `is_protocol_version_compatible` and uses the hello frame as the Noise prologue
  (`crates/core/src/noise_auth.rs:1076-1124`). The QUIC ALPN is `rusty-grid/v1`
  (`crates/core/src/transport.rs:11`). No JSON type has `deny_unknown_fields` (a search of
  `crates/core/src` finds none), so a JSON peer ignores a field it does not know.
- Design 0040 says every bump breaks older peers and lists rolling compatibility as a non-goal
  (`docs/design/0040-protocol-versioning.md:49-55`). Design 0082 D6 states the rule this record builds
  (`docs/design/0082-task-extension-slot.md`, D6) and PROTO-3d waits for it.

## Goals

- **G1** A v10 master registers a v9 worker, runs its tasks, and serves a v9 client, with no change
  to the v9 peer.
- **G2** A v9 peer never receives bytes or a version stamp it would refuse, and the master never
  sends it a restriction it cannot enforce.
- **G3** A rule that CI enforces: the window is exactly one version, and a pull request that cannot
  keep it must say so and close it.
- **G4** The upgrade order stays master first, and the v9 side of a mixed cluster is visible to the
  operator.

## Non-goals

- A window of more than one version, or one that reaches back across a closed window.
- A newer worker or client talking to an older master. The older master refuses it (D3).
- Reading a v9 journal with uncompacted records (D6).
- Changing `workspace` or `vram_mb`, the AEAD record layer, the handshake, or QUIC.
- A rolling downgrade of a master that has written a format 4 store.

## Design

### D1: The rule and its enforcement

- `PROTOCOL_VERSION_MIN_COMPATIBLE = PROTOCOL_VERSION_CURRENT - 1` while the window is open, so the
  constants become 10 and 9. The range check `is_protocol_version_compatible` stays as it is
  (`protocol.rs:55-57`).
- **Closing the window.** A pull request that changes the wire in a way the window cannot carry
  (a removed or reshaped variant, a field that cannot be dropped or defaulted for the older peer,
  a security fix) sets `MIN = CURRENT`, says "wire change, window closed" in its description, and
  deletes the frozen previous-version data of D7. Design 0076 D4 already says this; this record makes
  the test demand it.
- **The layout test as a range.** `verify_version_bump_for_layout_change`
  (`wire_layout_tests.rs:1392-1440`) changes in four ways:
  1. `CURRENT - 1 <= MIN <= CURRENT` always (the test today only checks `MIN <= CURRENT`,
     `:1411`).
  2. The version stamp leaves the recorded bytes: the canonical `ClientMessage` and `ClientResponse`
     samples are encoded with a fixed stamp `0` through the existing stamping helpers
     (`client_msg_mirrors.rs:870`, `client_resp_mirrors.rs:1267`, made `pub(crate)` for tests), so a
     bump no longer rewrites every entry. The header lines still record the two versions.
  3. A changed or removed entry no longer forces `MIN == CURRENT` by itself. It requires either
     `MIN == CURRENT` (window closed) or a passing window test (D7), which proves the old peer is
     still carried. The test message names both ways out.
  4. When `MIN < CURRENT`, `crates/core/tests/data/wire_layout_previous.txt` must exist and its
     header `# protocol_version` must equal `MIN` (D7). When `MIN == CURRENT`, the file must not exist.
- `DEPLOYMENT.md` 3.5, `CLAUDE.md` (the protocol paragraph) and design 0040 change in the same
  pull request (Compatibility).

### D2: How the master learns each peer's version

- **Worker.** `register` reads `protocol_version` from `WorkerMessage::Register`
  (`session.rs:715-723`) after the range check. It stores it in the session as
  `peer_version: u32` (the registry session entry and the writer tasks). It is fixed for the
  session; a reconnect is a new `Register` and a new session. Both QUIC lanes and the single TCP
  stream use the same value (the writers are created at `session.rs:952`, `:1105` and `:1136`).
- **Client.** Every `ClientMessage` carries the stamp. The decoders (`codec.rs:307-346`,
  `:427-435`) return it with the message, and `MessageTransport` and `MessageReader` keep it as
  `last_peer_version()`, next to `last_detected_codec` (`codec.rs:555`, `:602`). The master's client
  loop sets the outbound version of its transport from that value before every reply
  (`MessageTransport::set_peer_version`, beside `set_outbound_codec`, `codec.rs:597`). The version
  is therefore per frame: no connection state can go stale, and a client that sends a different
  stamp on a later frame gets that layout, still inside the range check.
- **Hello.** `Hello` carries only the stamp. The master answers with the peer's stamp (D3), and
  `min_compatible_version` is its own `PROTOCOL_VERSION_MIN_COMPATIBLE`.
- **Ambiguity of a v9 `SubmitTask`.** A v9 frame ends after `workspace`; read as v10, the first
  extra field is a `u64` list length taken from `created_at_utc`. A task whose `created_at_utc` is
  `0` can therefore parse as v10 by accident. The rule: decode the v10 layout; if it succeeds and
  the stamp is below `CURRENT`, decode again with the layout of that stamp and use that result; if
  that fails, the frame is an error. A frame is never accepted under a layout its stamp contradicts.
  A test sends a v9 `SubmitTask` with `created_at_utc = 0`.

### D3: What a v10 master sends to a v9 peer

**Stamps.** The master echoes the peer's version: `RegisterAck.protocol_version` for a v9 worker is
`9`, and every `ClientResponse` to a v9 client is stamped `9`. The v9 peer's own range check
(`9..=9`, the checks quoted in Context) then accepts it. Without the echo a v9
worker refuses the ack and a v9 client refuses every reply. The ack of a refusal and the
`Error` reply keep stamping `CURRENT`; a v9 peer already reads them (`codec.rs:353` skips `Error`,
the refusal is read before the version check, `worker/src/client/session.rs:1318-1323`).

**Encoding.** `serialize_message` gets a `peer_version: u32` parameter beside the codec
(`MessageTransport`, `MessageWriter` hold it; default `CURRENT`). For `peer_version == CURRENT`
nothing changes. For an older version a message type opts in:

| Message | v9 form |
|---|---|
| `MasterMessage::RegisterAck` | `protocol_version` field set to the peer's version |
| `MasterMessage::AssignTask`, `AssignTaskWithCheckpoint` | the `Task` goes through `Task::for_peer(9)` (below), then encodes as `LegacyV9Task` (no lists) |
| `MasterMessage::RoleHandoverAck` | encoded as v9; the list is empty today, and `Task`s in it get the same rule |
| `ClientResponse` (all) | `BinaryClientResponse::from_client_response(msg, 9)` / the Human equivalent: same layout, stamp 9 |
| all other `MasterMessage` | unchanged, same bytes |

`BinaryMasterMessage` becomes generic over the task type (`BinaryMasterMessage<T = Task>`, with
`AssignTask { task: T }`) so the v10 and the v9 form are one definition and cannot disagree on
variant order. `LegacyV9Task` and `LegacyV9TaskRequirements` get `Serialize` and a borrowed form
`LegacyV9TaskRef<'a>` so a copy of the task is not needed. The JSON codec needs no layout change (a
v9 JSON peer ignores `extensions`), but takes the same `for_peer` step so a JSON and a bincode v9
peer see the same task. The type dispatch inside `serialize_message` uses `TypeId`, as
`deserialize_message` does (`codec.rs:471-545`); `M` gets `'static`.

**`Task::for_peer(version)`** (new, `crates/core/src/task/extension.rs` beside
`strip_master_extensions`) returns the task a peer of that version may receive:

1. `Master`-scope extensions are removed (already done for every worker, `scheduler.rs:2096`).
2. For `version < 10`: both extension lists are cleared. The extensions that remain at that point are
   non-critical, or critical with scope `Worker` or `Both`.
3. The stored task and the journal keep every extension. Only the copy for the wire changes.

**Why dropping is safe.** A v9 worker never advertises `ext:<id>`, so no task with a critical
`Worker` or `Both` extension is placed on it (PROTO-3, `placement.rs:48`, design 0082 D4). What is
left to drop is non-critical, which is defined as "ignorable", and `Master`-scope ones, which the
master enforces itself. The worker-side refusal of an unknown critical extension
(`worker/src/client/tasks.rs`, design 0082 D2.3) stays as a second line: a v9 worker has no such
code, which is why placement is the real guard. A test pins it (Test plan 4).

**Relay between 10 and later.** "Kept byte for byte on relay" (design 0082 D2.2) holds between peers
of version 10 or later. A v9 peer receives the task without the extensions that the receiver did not
need to know.

**Placement stays unchanged.** The master adds no new rule: a v9 worker is an ordinary worker without
`ext:` tags. It is chosen for every task that has no critical `Worker`/`Both` extension.

### D4: What a v10 master does with a v9 client and a v9 worker

**v9 client.**

- `SubmitTask`: decoded through `LegacyV9BinaryClientMessage`, converted by a new
  `From<LegacyV9Task> for Task` into a `Task` with `extensions` empty in both lists and
  `ClientMessage::SubmitTask { task, wait }` with the version recorded by D2 (today the decoder
  refuses it, `codec.rs:336-343`). The conversion is total: every field of the v9 layout exists in
  v10. Then it is an ordinary task.
- Every other `ClientMessage` has the same layout in 9 and 10 (the wire layout comparison above), so
  `BinaryClientMessage` decodes it, and the range check now accepts the stamp 9.
- Replies: encoded per D3 with stamp 9. `TaskCompleted`, `TaskStatusInfo`, `WorkerList`,
  `ClusterStatus`, `BlobReply`, `Hello` and the rest have the v10 layout, so only the stamp changes.
  `WorkerList` and `ClusterStatus` carry worker tags as strings, including `ext:<id>`; a v9 client
  prints them as it prints any tag. `ClientResponse::Hello` reports `min_compatible_version = 9`
  (the master's own); a v9 client ignores the number (`connect.rs:632`).
- A v9 client cannot send an extension, so a task from it never carries one. The master does not
  guess.

**v9 worker.**

- `Register` has the same layout, and a v9 worker advertises `all_supported()` features and
  `tool:`, `iso:`, `slots:` tags, and no `ext:` tag. The ack is stamped 9 (D3).
- Every `WorkerMessage` it sends (`Heartbeat`, `TaskResult`, `Checkpoint`, `BlobRequest`, ...) has
  the same layout in 9 and 10 (the wire layout comparison above), so the existing decoder reads them.
- Dispatch: `AssignTask` and `AssignTaskWithCheckpoint` are encoded by D3. Blob transfer, checkpoints
  and workspaces use messages that did not change, so `BLOB_TRANSFER`, `FILE_STAGING` and the rest of
  `ProtocolCapabilities` negotiate as today.
- The master shows the peer's version to the operator: a master-added tag `proto:<n>` in
  `ListWorkers` and `ClusterStatus`, next to `link:`, `id:` and `state:`
  (`client_and_members.rs:67-83`), and a `warn!` once per session at registration when
  `n < CURRENT` ("worker <name> speaks protocol 9; upgrade it before the master moves to 11"). The
  tag is the eleventh reserved prefix (Q78) and needs no wire field.

### D5: A v10 worker or client against a v9 master

It is refused, as today, and the window does not change that. A window protects a newer master from
older peers and never the reverse:

- Worker: the v9 master sees `Register { protocol_version: 10 }`, answers `RegisterAck { accepted:
  false, message: "Incompatible protocol version 10 from worker (supported: 9..=9; ...)" }` (v9
  `session.rs:723-734`), and the v10 worker returns `GridError::RegistrationRejected` before it looks
  at the version (`worker/src/client/session.rs:1318-1323`).
- Client: the v9 master answers the stamped `Hello` with `ClientResponse::Error` and closes
  (`client_and_members.rs:166-177`); `connect_master` maps it to `GridError::IncompatibleVersion`
  (`connect.rs:633-639`) before any state-changing message goes out. A v10 client never sends a v10
  `SubmitTask` to a v9 master.
- **A client and worker accept only an echo of their own version** (new, D2/D3): a v10 worker takes a
  `RegisterAck` whose `protocol_version == PROTOCOL_VERSION_CURRENT`, and a v10 client takes a
  non-error reply whose stamp equals its own. With a window the generic range check also lets 9
  through, and without this rule a v10 client could accept a master that answers at 9 and then send
  it a layout it cannot read. Today's v9 master never does that, but a later master with a different
  window could. `is_protocol_version_compatible` keeps its job on the master side; the peer side
  calls a new `is_master_echo_acceptable(v) = v == PROTOCOL_VERSION_CURRENT`.
- **Upgrade order:** master first, then workers and clients, in `DEPLOYMENT.md` 3.5. After the master
  moves from 9 to 10 the v9 workers keep running and registering; upgrade them one at a time, then
  the clients. When the master later moves to 11, every worker still on 9 is refused: finish the
  worker upgrade before the master follows, which is what `proto:<n>` shows.

### D6: The journal and the store

- Format 4 versus 3 is a disk format, not a wire one, and the window does not apply to it. A v10 master
  reads a v9 store when the v9 master was stopped cleanly: the snapshot is JSON and the empty journal
  resets (`store.rs:372-375`, `:1155`; the clean-shutdown upgrade of `DEPLOYMENT.md` 3.5, tests at
  `store.rs:1650-1700`). An uncompacted v9 journal (format 3, protocol header 9) is refused with
  the message "start the previous binary and stop it cleanly" (`store.rs:1158-1166`). That is the path
  from 0.3.0: stop the v9 master with `SIGINT` or `SIGTERM`, start the v10 master.
- The window does not change this on purpose (Q75). Reading format 3 journal records needs a frozen v9
  `JournalRecord` and a decoder for every type inside it (`Task`, `TaskState`, `RetryAttempt`,
  `CheckpointData`, `TaskResult`), a second copy of the largest layout in the project, for a case the
  clean stop already covers.
- A v9 master cannot open a v10 store (it reads formats 1 to 3, `store.rs:914-918`). Rolling a master
  back after the upgrade needs the pre-upgrade state directory. `DEPLOYMENT.md` 3.5 says: copy
  `--state-dir` before starting the new master.
- While v9 peers are attached, a task may be stored with extensions the v9 worker never saw. The
  result comes back like any other.

### D7: Tests keep the window honest

`crates/core/tests/data/wire_layout_previous.txt` is the `wire_layout.txt` of the latest release that
speaks `CURRENT - 1`, copied from its tag (for 0.3.0, `git show v0.3.0:crates/core/tests/data/wire_layout.txt`,
protocol 9). It is read-only data. A cross-version test in `crates/core` (`window_tests.rs`)
uses it as the oracle, so no hand-written v9 encoder can drift from what 0.3.0 really sent:

1. **Peer to master:** every `WorkerMessage`, `ClientMessage` (sent by a v9 peer) in the previous file is
   decoded by the current decoder; the result equals the canonical sample of the current file with
   `extensions` empty. `SubmitTask` is decoded through `LegacyV9BinaryClientMessage`.
2. **Master to peer:** for every `MasterMessage` and `ClientResponse` in the previous file, the
   current sample passed through `for_peer(9)` and the v9 encoder produces exactly the recorded
   bytes (with the stamp 9). Samples that carry extensions are compared after `for_peer`.
3. **A new frozen record:** each entry of the current file that differs from the previous file must
   be in a short list in the test (`DIFFERS_FROM_PREVIOUS`) with the reason. Today: the three
   `Task` carriers and `RoleHandoverAck`. A fourth entry that changes without being listed fails the
   test, and so does a listed one that no longer differs.
4. The range rule of D1 as unit tests on `verify_version_bump_for_layout_change`: `MIN == CURRENT -
   1` with no previous file fails, `MIN == CURRENT` with a previous file fails, `MIN < CURRENT - 1`
   fails.

At the next wire change the pull request replaces the previous file with the file of the release at
`CURRENT` (the release train guarantees one exists, design 0076 D1), and the window moves from
`9..=10` to `10..=11`.

## Compatibility

- **Rule change (design 0040).** The non-goal "every bump breaks older peers" no longer holds from
  protocol 10: the master accepts `CURRENT - 1`. Design 0040 gets a status-log entry and its
  Non-goals and D1 text a pointer to this record. The wire layout rules in design 0040 D1 (the last
  two bullets about `MIN`) are replaced by D1 above.
- **Documents:** `DEPLOYMENT.md` 3.5 (the paragraph that says `MIN == CURRENT` and that v10 refuses
  v9), `CLAUDE.md` (the protocol version paragraph, the `Legacy*` decoders, the `proto:` tag and
  "ten" to "eleven" reserved prefixes) and `README.md` where it names protocol versions.
- **Wire:** no new message and no new field. `PROTOCOL_VERSION_CURRENT` stays 10, so the pull request
  is not itself a wire change; it makes a v10 master speak v9 as well. It merges after, and is
  released with, pull request 272 (design 0076 D1: one release per wire change, within 48 hours).
- **Mixed cluster** (v10 master): v9 workers and clients work; v10 ones work; a v9 master refuses v10
  peers (upgrade the master first). Two masters of different versions in one cluster are not a case:
  there is one master.
- **JSON peers:** a v9 JSON peer works with the same stamps, and the `extensions` field is not sent.
- **Handshake, AEAD and QUIC:** unchanged. The v2 handshake has no protocol version (`cluster_auth.rs:92`).
  The v3 Noise hello already goes through the range check (`noise_auth.rs:1123`), so with the window
  it accepts a hello stamped 9; the hello frame is the Noise prologue, so the stamp is
  authenticated. The record layer is chosen by the handshake version, not by the protocol version
  (`protocol.rs:79-82`), and the ALPN stays `rusty-grid/v1`. QUIC worker connections still carry
  `STREAM_CONTROL` and `STREAM_DATA`, and client connections one `STREAM_CLIENT` stream; both lanes
  get the same peer version (D2).
- **Skew job (PROTO-2).** `scripts/release_skew.py` runs three pairs
  (`scripts/release_skew.py:1-28`). It reads the release and pull request protocol constants (CI
  passes them from `git show <tag>:crates/core/src/protocol.rs` and the working tree) and derives the
  expected verdict of each pair. With the pull request at `CURRENT = 10`, the release at 9:

  | Pair | `MIN` 9 (window open) | `MIN` 10 (window closed) |
  |---|---|---|
  | 1 release master, pull request worker and client | refused with the version error | refused with the version error |
  | 2 pull request master, release worker and client | works | refused with the version error |
  | 3 pull request master, pull request worker and client | works | works |

  A pair that differs from its expected verdict fails the job. Pair 2 `works` is the proof for
  "a worker one version back registers and runs a task". The existing check that a refusal inside
  the refuser's range fails the job (`classify`) already covers a regression of pair 2.

## Security

- **A master talks to older code.** The v9 layouts and stamps are the only new input paths: one
  frozen `SubmitTask` decoder (already present for refusal) and the stamp echo. Both are bounded by
  the 64 MiB frame limit and the same `Task::validate` as a v10 task.
- **A claimed version is not trusted for more than layout.** The version in `Register` and in a
  client frame is inside the AEAD record layer (or inside the Noise prologue for v3), so an
  on-path attacker cannot lower it. A peer on loopback without a secret can claim 9, but gets only
  the v9 copy of a task: no restriction is lost (D3), and it can send nothing it could not send as 10.
- **No restriction is dropped for an older peer.** Critical `Worker`/`Both` extensions keep tasks off
  workers without `ext:` tags, and a v9 worker has none (D3). A security fix that needs the wire
  changed must close the window (D1), so a vulnerable v9 path does not stay open for a release.
- **Downgrade of a master is not supported** (D6), so a window cannot be used to roll a fixed master
  back onto an old store.
- The `proto:<n>` tag is master-added; a worker cannot claim it (reserved prefix, `placement.rs:52`).

## Test plan

Unit and in-process tests (no live cluster), plus the skew job. All use frozen data from D7, not an
encoder written in the test.

1. D7 tests 1 to 4 (`crates/core/src/window_tests.rs`, `wire_layout_tests.rs`).
2. `decode_v9_submit_task_is_accepted_and_has_empty_lists`: the v9 `SubmitTask` from the previous
   file becomes a `Task` with both lists empty and equal fields; with `created_at_utc = 0` it still
   decodes as v9 (D2); a v9 frame with stamp 8 or a v10 frame with stamp 9 is refused.
3. `v9_worker_registers_and_runs_a_task` (`crates/master/tests/`): an in-process master at 10 and a
   test worker that speaks v9 bytes (the previous file's `Register` and `AssignTask` shapes): the
   ack is stamped 9, a task is assigned in the v9 layout (bytes equal the previous sample), the
   result completes it, the stored task keeps its extensions (`TEST_EXTENSION_REGISTRY` entries).
4. `critical_worker_extension_never_reaches_a_v9_worker`: the task waits with
   `ClusterLacksCapability` naming the id while only a v9 worker is connected, and is placed when a
   v10 worker with `ext:<id>` joins. A non-critical extension reaches the v9 worker without it. A
   `Master`-scope extension is absent for both.
5. `v9_client_round_trip`: `Hello`, `SubmitTask --wait`, `GetTaskResult`, `ListWorkers` and
   `ClusterStatus` from a v9 client against a v10 master; every reply is stamped 9 and decodes with
   the frozen v9 reader; `proto:<n>` appears in `ListWorkers`.
6. `v10_peers_refuse_an_older_master`: a fake master that answers `Hello` and `RegisterAck` stamped
   9 is refused by the v10 client and worker (D5); a fake v9 master that refuses is reported as
   `IncompatibleVersion` and `RegistrationRejected`.
7. Both codecs: tests 3 and 5 run for bincode and JSON; the JSON v9 task has no `extensions`.
8. QUIC: the two-lane worker test (`crates/master/src/session.rs` tests) runs once with a v9
   worker and checks that control and data lanes both use v9.
9. Store: a v9 clean-shutdown directory opens in 10 (exists, `store.rs:1650-1670`); an uncompacted v9
   journal is refused with the message of D6 (exists, `store.rs:1680-1715`).
10. Skew job: `tests/test_release_skew.py` gets fake binaries for the table above, including a pair
    that must fail the job when pair 2 is refused with `MIN` 9. Then the CI job `release skew` shows
    the table with pair 2 `works` against `v0.3.0`.
11. `ox-mode test lint`, `cargo test -p rusty_grid_core --lib`, `-p rusty_grid_master`,
    `-p rusty_grid_worker --lib`, `-p rusty_grid_cli`, and PROTO-3d of design 0082 (its test 10).

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| PROTO-4a | Constants, range rule, stamp-neutral samples, previous file (D1, D7 tests 3 and 4) | `crates/core/src/protocol.rs`, `crates/core/src/wire_layout_tests.rs`, `crates/core/tests/data/wire_layout_previous.txt` | Test 1.4 passes; `MIN` is 9 | Implemented |
| PROTO-4b | Peer version: `last_peer_version`, `set_peer_version`, `serialize_message` with a version, `Task::for_peer`, generic `BinaryMasterMessage`, `Serialize` v9 types, v9 decode (D2, D3) | `crates/core/src/protocol/codec.rs`, `crates/core/src/protocol/client_msg_mirrors.rs`, `crates/core/src/protocol/client_resp_mirrors.rs`, `crates/core/src/protocol/worker_master_mirrors.rs`, `crates/core/src/task/extension.rs` | Tests 1 to 2 pass | Implemented |
| PROTO-4c | Master: `peer_version` in the session, stamps, `proto:` tag, warning; peer-side echo check (D3, D4, D5) | `crates/master/src/session.rs`, `crates/master/src/server/client_and_members.rs`, `crates/master/src/server/handshakes.rs`, `crates/core/src/placement.rs`, `crates/worker/src/client/session.rs`, `crates/cli/src/connect.rs` | Tests 3 to 8 pass | Implemented |
| PROTO-4d | Documents and the skew job: design 0040, `DEPLOYMENT.md` 3.5, `CLAUDE.md`, `release_skew.py`, its test (Compatibility, test 10) | `docs/design/0040-protocol-versioning.md`, `DEPLOYMENT.md`, `CLAUDE.md`, `scripts/release_skew.py`, `tests/test_release_skew.py`, `.github/workflows/ci.yml` | Test 10 passes; the CI table shows pair 2 `works` | Implemented |
| PROTO-3d | Cross-version tests of design 0082 (its test 10) | see design 0082 | Tests 3 to 5 of this record | Implemented |

The four items fit one pull request (L, as in the backlog), based on pull request 272, which merges
first. They are released together: protocol 10 with the window is the first version people outside
the owner's machines install (design 0076 Q66).

## Open decisions

| ID | Question | Recommended default |
|---|---|---|
| Q73 | Ship the window in the same release as protocol 10 (pull request 272 and this one together)? | Yes. Protocol 10 without the window would refuse every 0.3.0 worker on the first upgrade |
| Q74 | A non-critical extension on a task for a v9 worker: drop it from the copy, or refuse to place the task? | Drop. "Ignorable" is the definition of non-critical, and the stored task keeps it |
| Q75 | Read an uncompacted v9 journal (format 3 records)? | No. A clean stop already carries state across (D6); a frozen `JournalRecord` is not worth it |
| Q76 | Record the previous release's layout file and drop the stamp from the recorded bytes (D1, D7)? | Yes. The oracle is then what 0.3.0 really sent |
| Q77 | Workers and clients accept only an echo of their own version from the master (D5)? | Yes. It keeps "upgrade the master first" true by construction |
| Q78 | Add a master-added `proto:<n>` tag, the eleventh reserved prefix, and a once-per-session warning for peers below `CURRENT`? | Yes. Operators need to see who is still on the old version before the master moves on |
| Q79 | Keep v9 JSON peers inside the window, or support bincode only? | Keep both: the code path is the same `for_peer` step and the cost is one test |

## Status log

- 2026-11-17: Draft written for PROTO-4 (design 0076 D4, Q66). Statements about code were checked on
  the PROTO-3 branch (`origin/claude/proto-3-extension-slot`, pull request 272). Builds nothing.
- 2026-11-17: The owner decided Q73 to Q79 in chat, all on the recommended defaults: the window ships
  in the same release as protocol 10 (Q73); a non-critical extension is dropped from a v9 worker's
  copy (Q74); an uncompacted format 3 journal is not read (Q75); the previous release's layout file
  is the oracle and the stamp is dropped from the recorded bytes (Q76); workers and clients accept
  only an echo of their own version (Q77); the master-added `proto:<n>` tag, the eleventh reserved
  prefix, and a once-per-session warning (Q78); v9 JSON and bincode peers are both kept (Q79).
- 2026-11-17: Built by draft pull request PRNUM (PROTO-4a to PROTO-4d and PROTO-3d of design 0082).
  `PROTOCOL_VERSION_MIN_COMPATIBLE` is 9 at `PROTOCOL_VERSION_CURRENT` 10. The pull request is not a
  wire change by itself (no new message or field) and merges with pull request 272 (design 0076 D1).
  Deviations from this record, where the code forced or allowed a different shape (the line numbers
  quoted in Context were approximate and were checked against the code):
  - **Echo check in the decoders (D5).** The client side of "accept only an echo of the own version"
    sits in the `ClientResponse` decoders of `crates/core/src/protocol/codec.rs`
    (`is_master_echo_acceptable`, both codecs), not in `connect_master`, so every reader of a
    `ClientResponse` gets it and `connect_master` maps the resulting version error as before. The
    worker check is in `crates/worker/src/client/session.rs`.
  - **`proto:<n>` is added in `WorkerEntry::to_info` (D4)**, not in `listed_worker`, so the
    dashboard and the scheduler's `WorkerInfo` carry the tag too; `WorkerInfo` has no new field
    (a new public field would have broken every struct literal of the integration tests). The tag
    reads the registry's per-session `peer_version`, set by `WorkerRegistry::set_peer_version`.
  - **Where the client reply version is set (D2).** `send_client_response`
    (`crates/master/src/server/tasks_and_blobs.rs`) copies `MessageTransport::last_peer_version()`
    into `set_peer_version` before each reply, which covers every reply path of the client loop
    (map-reduce and the blob pipeline included) in one place. The first frame, which the handshake
    decodes outside the transport, is recorded with `note_peer_version`.
  - **Owned v9 task instead of `LegacyV9TaskRef` (D3).** `Task::for_peer(9)` returns an owned copy
    and `From<Task> for LegacyV9Task` moves its fields, so there is one clone per assignment and no
    borrowed form. `serialize_message` keeps its signature; the new `serialize_message_for_peer`
    needs `M: 'static` (a `TypeId` dispatch), so `send_msg` and `send_msg_with_idle_timeout` carry
    that bound. `deserialize_message_with_version` returns the stamp of a client message.
  - **Layout test (D1).** The recorded `ClientMessage` and `ClientResponse` entries are encoded with
    the stamp `0`. `wire_layout_previous.txt` is the `wire_layout.txt` of `v0.3.0` verbatim (its
    entries still carry the stamp 9, and its `RegisterAck` and `Register` samples carry the marker
    `0x0a0b0c0d`); `window_tests.rs` normalises them when it compares. "A changed entry needs
    `MIN == CURRENT` or a passing window test" is enforced as a range check
    (`verify_window_range`, `CURRENT - 1 <= MIN <= CURRENT`) plus the separate window tests, which
    fail the suite on their own; the two are not wired into one function. The previous-file rule is
    `verify_previous_layout_file`.
  - **Frozen readers and writers for tests.** `deserialize_message_as_v9` (a frozen v9 reader of
    `MasterMessage` and `ClientResponse`) and `serialize_v9_client_message` sit behind the core
    feature `test-extensions` that the master and worker dev-dependencies already enable. The
    in-process v9 peers of `tests/window_v9_peers.rs` encode messages whose layout did not change
    (`Register`, `TaskResult`, ...) with the current encoder, which `window_tests.rs` proves is the
    same bytes as `wire_layout_previous.txt`; the changed ones (`AssignTask`, `SubmitTask`) go
    through the v9 types that the same test replays against the recorded bytes. The real `v0.3.0`
    binaries run in `scripts/release_skew.py`.
  - **Test plan.** Test 6 is split: the worker side is in `crates/worker/src/client/tests.rs`, the
    client side is the probe test and `a_previous_version_hello_is_answered_with_the_clients_own_stamp`
    in `crates/cli/tests/test_hello_probe_e2e.rs`. Test 4 asserts that the task stays `Queued` and
    never reaches the v9 worker, not the `ClusterLacksCapability` text. Test 8 is
    `test_multiplexed_v9_worker_gets_v9_on_both_lanes` on the in-memory two-lane session (a real
    iroh connection is not started). Test 9 is the existing store tests, unchanged.
  - **Skew job.** `scripts/release_skew.py` gets `--release-protocol-rs` (CI passes
    `gh api .../contents/crates/core/src/protocol.rs?ref=<tag>`, so the checkout needs no tags) and
    `--pr-protocol-rs` (default: the working tree); without the first nothing is derived and the job
    behaves as before.
  - Unverified here: Windows and Linux runs, a real QUIC (iroh) v9 worker, and the CI job itself.
