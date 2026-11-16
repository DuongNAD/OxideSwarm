# 0082: An extension slot for optional task fields

Status: Implemented (PROTO-3 of design 0076 D3; draft pull request 272, which merges together with PROTO-4)
Created: 2026-11-17    Updated: 2026-11-17

## Summary

Adds `extensions: Vec<Extension>` at the end of `Task` and of `TaskRequirements`, where `Extension` is `{ id: u16, critical: bool, bytes: Vec<u8> }`, as protocol version 10. After that, an optional task field travels as an extension and no longer forces `PROTOCOL_VERSION_MIN_COMPATIBLE` up. A receiver ignores an unknown non-critical extension and keeps it when it relays the task. It refuses a task with an unknown critical extension, and the scheduler never places such a task on a worker that does not advertise the id. Existing fields (`workspace`, `vram_mb`) stay where they are.

## Context

- `Task` ends with `workspace` (`crates/core/src/task.rs:1965`, appended in v6) and `TaskRequirements` ends with `vram_mb` (`crates/core/src/task.rs:472`, appended in v9, design 0068 D1). Both are plain serde structs with `#[serde(default)]`. Bincode is positional and ignores the default, so each added field changes the bytes of every message that carries a task (`SubmitTask`, `AssignTask`, `AssignTaskWithCheckpoint`).
- `Task` and `TaskRequirements` have no `Human*`/`Binary*` mirror: the mirror enums embed `Task` as it is (`crates/core/src/protocol/client_msg_mirrors.rs:12`, `:126`). A field on `Task` therefore needs no new variant, no mirror field and no `From` impl. The cost of a field is the frozen legacy layouts, which exist only to refuse old frames with a clear error (`LegacyV5Task`, `LegacyV6ToV8Task`, `crates/core/src/protocol/client_msg_mirrors.rs:256-291`; the wrapper enums at `:293-315`).
- Versions: `PROTOCOL_VERSION_CURRENT` and `PROTOCOL_VERSION_MIN_COMPATIBLE` are both `9` (`crates/core/src/protocol.rs:42-44`). The wire layout test compares encoded bytes with `crates/core/tests/data/wire_layout.txt` (`crates/core/src/wire_layout_tests.rs:1566`, baseline check at `:1844`). Its samples set `vram_mb` at `:144` and `:173`.
- `skip_serializing_if` is banned on bincode types (`crates/core/src/lib.rs:168`).
- Submission validates the task in the queue (`crates/master/src/queue/scheduling.rs:76`, `:266`). `admit_blob_task` calls `Task::validate` too, but only for tasks with blobs (`crates/master/src/server/tasks_and_blobs.rs:188`), so the queue is the place where every submitted task is checked. `Task::validate` is at `crates/core/src/task.rs:2177` and `TaskRequirements::validate` at `:552`.
- The worker receives a task in `MasterMessage::AssignTask` / `AssignTaskWithCheckpoint` (`crates/worker/src/client/session.rs:1080-1090`) and already refuses one whose requirements exceed its capabilities (`crates/worker/src/client/tasks.rs:374-376`).
- Placement needs on a worker are matched in the scheduler: `required_features(task)` (`crates/master/src/queue.rs:553`) turns a task into the `ProtocolCapabilities` a worker must negotiate, and a task no worker can serve waits with `ClusterLacksCapability` (`crates/master/src/scheduler.rs:745-753`). Worker tags `os:`, `arch:`, `slots:`, `iso:` and `tool:` are added by the worker (`crates/worker/src/client.rs:875-901`), and nine prefixes are reserved (`crates/core/src/placement.rs:49-59`).
- The master journal stores `Task` in bincode and its format is `STORE_FORMAT_VERSION = 3` (`crates/master/src/store.rs:96`; the layout rule is at `:20`).

## Goals

- **G1** One field at the end of `Task` and of `TaskRequirements` that carries optional data of any later feature, with defined behaviour for a receiver that does not know it.
- **G2** A restriction (a limit, an isolation need, a placement need) can never be silently dropped by a worker that does not understand it.
- **G3** A later optional field is an extension: `PROTOCOL_VERSION_CURRENT` may move, `PROTOCOL_VERSION_MIN_COMPATIBLE` does not.
- **G4** Bounded count and size, inside `MAX_FRAME_SIZE` (`crates/core/src/protocol.rs:27`).

## Non-goals

- Moving `workspace` or `vram_mb` into extensions (D5).
- A user-facing way to attach an extension (no CLI flag, no MCP field, no Web UI field). Extensions are for features of the project, each with its own design record.
- Extensions on messages other than `Task` and `TaskRequirements` (`Register`, `TaskResult`, ...). Those use `ProtocolCapabilities` bits or a later record.
- The one-version window (PROTO-4, design 0076 D4). D6 states only the rule it must follow.

## Design

### D1: Types and wire shape

In `crates/core/src/task.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Extension {
    pub id: u16,          // see the registry in D3; 0 is invalid
    pub critical: bool,   // true: a receiver that does not know `id` must refuse the task
    #[serde(with = "extension_bytes")]
    pub bytes: Vec<u8>,   // the value; its layout belongs to the registry entry of `id`
}
```

- `Task` gets `#[serde(default)] pub extensions: Vec<Extension>` as its last field, after `workspace`. `TaskRequirements` gets the same field after `vram_mb`. Both default to empty. No `skip_serializing_if` (`crates/core/src/lib.rs:168` enforces this).
- **Bincode:** an empty list is 8 bytes (a `u64` length), so every task grows by 16 bytes. A non-empty list is the length, then per entry `u16` id, `u8` bool and a `u64` length plus the bytes. `bytes` use `serialize_bytes`, which bincode 1.x writes exactly like `Vec<u8>`; the helper exists only for JSON.
- **JSON** (the tagged JSON codec, and the store snapshot): `"extensions": [{"id": 7, "critical": false, "bytes": "00ff"}]`, with `bytes` as lowercase hex (`serde_with`-style module `extension_bytes`, using `hex_encode` / `hex_decode` from `crates/core/src/cluster_auth.rs`, as `blob.rs` does). It is human-readable and survives the numeric-array trap of `Vec<u8>` in JSON. A JSON peer may omit `extensions`; `#[serde(default)]` gives an empty list.
- **Mirrors:** none to change (Context). The "four `From` impls" rule of `CLAUDE.md` is not touched, and the build item says so in its pull request so a reviewer does not look for them.
- **Why two lists.** `Task.extensions` carries what the worker does with the task; `TaskRequirements.extensions` carries what the scheduler matches on, next to `vram_mb`. The registry (D3) names the list of each id. An id in the wrong list is refused as malformed, not treated as unknown. If the owner prefers one list, Q70 drops the requirements list; the rest of this record is unchanged.
- **Protocol version 10.** Adding the lists changes the bytes of existing variants, so this is the next wire change: `PROTOCOL_VERSION_CURRENT = 10` and, until PROTO-4 lands, `PROTOCOL_VERSION_MIN_COMPATIBLE = 10` (design 0040 lines 80 to 85). The PROTO-1 rule applies: the pull request says "wire change", a release follows within 48 hours, and if another wire change is already waiting for a release, this one extends its bump and its `wire_layout.txt` entry instead of taking a new number (design 0076 D1).
- **Store format.** `Task` is in the journal in bincode, so `STORE_FORMAT_VERSION` goes from 3 to 4 with `MIN_STORE_FORMAT_VERSION` unchanged, as design 0068 D1 did for `vram_mb`. Snapshots (JSON) carry over; uncompacted older journal records are refused cleanly.

### D2: Rules for a receiver

Let `K` be the set of ids the receiving build knows (D3).

1. **Known id:** the build decodes `bytes` by the registry entry. A value that does not decode is refused as invalid (error names the id).
2. **Unknown id, `critical = false`:** ignored. The extension is not removed: it stays in the stored `Task`, in the journal, and in every `Task` the master relays (to a worker, in a status or result path that carries the task), unchanged byte for byte. A later build that knows the id sees it.
3. **Unknown id, `critical = true`:**
   - **Master, at submit:** `Task::validate` fails with `Task <id> needs extension <n>, which this master does not support`, so `SubmitTask` gets `ClientResponse::Error` (`crates/master/src/server/client_and_members.rs:240-271`) and nothing is queued or stored. A task that bypasses `validate` is placed nowhere, like a task with unreadable placement tags (`crates/master/src/queue.rs:520-522`).
   - **Worker, at assignment:** before the capability check at `crates/worker/src/client/tasks.rs:374`, the worker refuses the task with a failed `TaskResult` that names the id. This is defence in depth: with D4 the master does not send such a task.
4. **Limits (D4 bounds)** are checked first, on both sides, so a malformed list never reaches rules 1 to 3.
5. **Duplicate ids** in one list are refused (a duplicate would make "which one wins" a wire question).
6. **A critical extension must be one that restricts.** The rule for authors of a new extension: anything that limits where or how a task runs (isolation, placement, resource limits, output limits) is `critical = true` in its registry entry. A hint (a label, a priority hint, a cache key) is `critical = false`. The registry entry fixes the value of `critical`, and a known id that arrives with the other value is refused as invalid. The encoder cannot choose it.

### D3: Registry

One table in `crates/core/src/task.rs`, next to `Extension`, as a `const` slice that the code reads and the wire layout test records:

| Field | Meaning |
|---|---|
| `id: u16` | `0` invalid. `1..=0x7FFF` assigned by a design record, in order. `0x8000..=0xFFFF` reserved for experiments and tests: no release build assigns one. |
| `name` | Short lowercase name, used in logs and error messages. |
| `list` | `Task` or `Requirements` (D1). |
| `critical` | The value this id must carry (D2.6). |
| `scope` | `Master` (only the master's scheduler or admission reads it), `Worker` (only the worker reads it) or `Both`. |
| `decode` | A function that checks `bytes` and returns the typed value. |

The registry starts **empty** in the pull request that builds the slot, plus one `#[cfg(test)]` entry per case the tests need (a known critical and a known non-critical id from the experimental range). The first real id comes with the feature that needs it. `docs/README.md` gets no table: the registry in `task.rs` is the single source, and each feature's design record names its id (writing standard rule 5).

### D4: Bounds and placement

**Bounds** (constants beside the registry; `MAX_FRAME_SIZE` is 64 MiB, so all are far below it):

| Constant | Value | Checked by |
|---|---|---|
| `MAX_EXTENSIONS_PER_LIST` | 16 | `Task::validate`, per list |
| `MAX_EXTENSION_BYTES` | 64 KiB | `Task::validate`, per entry |
| `MAX_TASK_EXTENSION_BYTES` | 256 KiB | `Task::validate`, both lists together, counting `bytes` only |

The worker repeats the three checks at assignment (D2.4). A bincode decoder reads the list length from the frame, but the frame cap and `Vec` growth by reading keep a lying length from allocating more than the frame holds. Tasks are already retained under the queue's byte budget; extension bytes count into it with the rest of the `Task` (the build item verifies this in a test). The sizes are Q71.

**Placement.** The scheduler must understand every critical extension that restricts placement, or the task does not run. The master decides as follows:

1. At submit the master refuses a critical id it does not know (D2.3). So every critical extension of a queued task is known to the master.
2. A known critical extension with `scope` `Worker` or `Both` needs workers that understand it. A worker advertises each id it implements as a worker tag `ext:<id>` (decimal), set at start-up by the worker beside `tool:<name>` (`crates/worker/src/client.rs:875-901`). The tag is a tenth reserved prefix (`ext:`) in `crates/core/src/placement.rs:49-59`, and `CLAUDE.md` changes "nine" to "ten". It needs no wire field: tags are already in `Register`.
3. `TaskView` (`crates/master/src/queue.rs:516`) gets `required_extensions: Vec<u16>`, the ids of the task's known critical extensions with `scope` `Worker` or `Both`. The static stage that checks `required_features` (`crates/master/src/scheduler.rs:745-753`) also checks that the worker carries `ext:<id>` for each, and the id list is part of the placement `Shape`. A task no worker matches waits with `ClusterLacksCapability` and fails with the usual message under `--fail-unfulfillable-tasks` (design 0034). The wait message names the id.
4. A critical extension with `scope = Master` needs no worker tag: the master's own scheduler or admission code enforces it. The master removes such an extension from the copy of the task it sends to a worker, because the worker cannot enforce it and would otherwise refuse it as unknown (D2.3); the stored task keeps it. This is the one place a v10 relay drops a critical extension, and it is safe because the master is the enforcer.
5. A non-critical extension never changes placement.

This is "decide and state": **a critical extension that restricts placement is enforced by the master's scheduler through `ext:` tags; a worker that lacks the tag is never chosen; the task waits or fails as unfulfillable, and it is never run without the restriction.**

### D5: Existing fields stay

`Task::workspace` and `TaskRequirements::vram_mb` would have been extensions. **Do not migrate them in this change.**

- They are part of the v9 layout that released v0.3.0 will speak; moving them changes bytes and legacy decoders again, for no gain to any user.
- `workspace` drives feature negotiation (`WORKSPACES`), the master's affinity index, the `Task::validate` rules at `crates/core/src/task.rs:2177` and the store; `vram_mb` is read on the scheduler hot path (`crates/master/src/scheduler.rs`, design 0068 D3). Typed fields keep those checks at compile time.
- The next wire change, protocol 10, happens anyway, so a migration would be cheap in version numbers, but it would be the largest part of the pull request and carries scheduler and store risk with no benefit.

From protocol 10 on, the rule is: **a new optional field of `Task` or `TaskRequirements` is an extension, unless a design record shows that it must be typed.** The two old fields are the exceptions already made. Q68 asks the owner to confirm.

### D6: A v10 master and a v9 peer (after PROTO-4)

This record builds no window. It states the rule PROTO-4 follows so the slot does not block it.

- **Until PROTO-4:** a v10 build refuses a v9 peer by the version check. For a v9 `SubmitTask` it answers with `IncompatibleVersion`, which needs a `LegacyV9Task` and `LegacyV9BinaryClientMessage` in `client_msg_mirrors.rs` beside the v5 and v6 to v8 ones (the existing `LegacyV6ToV8Task` ends at `workspace` with `LegacyPreV9` requirements, so v9 needs its own struct with `vram_mb`).
- **After PROTO-4**, with the master at 10 and a peer at 9 (the peer's version is in `Register` and in every client message):
  - **v9 client to v10 master:** the master decodes the v9 `Task` through `LegacyV9Task` into a `Task` with both lists empty.
  - **v10 master to a v9 worker:** the master encodes the v9 layout, which has no lists. Non-critical extensions are dropped from that copy (the rule "ignored" holds, they are only not forwarded); the stored task keeps them. A v9 worker has no `ext:` tags, so by D4 no task with a critical `Worker`/`Both` extension is placed on it. A critical `Master`-scope extension is stripped from the copy as for any worker (D4.4).
  - **v10 master to a v9 client:** a response that echoes a task omits the lists.
  - "Preserved on relay" therefore holds between peers of version 10 or later. Dropping for a v9 peer is not a loss of a restriction, because restrictions are critical and D4 keeps those tasks away from v9 workers.
- A v9 peer never sends an extension, so a v10 master never has to guess what one means.

## Compatibility

- Wire breaking change under design 0040 and design 0076 D1: `PROTOCOL_VERSION_CURRENT` and `PROTOCOL_VERSION_MIN_COMPATIBLE` both become 10. Upgrade the master first, then workers and clients (`DEPLOYMENT.md` section 3.5). Frozen legacy decoders for v9 are added (D6).
- `STORE_FORMAT_VERSION` 3 to 4. A cleanly shut down v9 master upgrades at start; an uncompacted v9 journal is refused cleanly (as design 0068).
- From then on an added extension changes `PROTOCOL_VERSION_CURRENT` only when a build wants the bump for its own reasons (a release marker). It need not change `PROTOCOL_VERSION_MIN_COMPATIBLE`, which is the point of G3.
- JSON peers may omit `extensions`.

## Security

- **New untrusted input.** A client or a compromised peer controls the list. Bounds in D4 are enforced on both master and worker; the extension bytes are never interpreted by code that does not know the id.
- **The critical bit is the safety rule.** A worker that cannot enforce a restriction refuses the task or is never sent it (D2, D4). No path runs a task with a restriction dropped, including a v9 worker after PROTO-4 (D6).
- **A client cannot lower a restriction by flipping the bit.** For a known id, the registry fixes `critical` (D2.6) and a mismatch is refused. For an unknown id the sender's value is used, and `critical = false` there means "no one has promised to enforce this", so the sender asked for nothing.
- **No code execution.** Decoders are plain data; each registry entry's decoder is bounded by its own size limit and returns an error, never panics (fuzzable, see Test plan).
- **No secret or ticket in extensions.** Extensions are stored in the journal and shown in task listings; nothing sensitive goes in `bytes` unless its design record covers that, as for any task field.
- **Resource use.** Journal and queue budgets already apply to the whole `Task`; at most 256 KiB per task is added.
- The `ext:` worker tag is self-declared. A worker that lies about it can only receive tasks it is trusted with already (the same trust as the `tool:` and `iso:` tags).

## Test plan

Unit tests in `crates/core/src/task.rs` and `crates/core/src/protocol/tests.rs`, with `#[cfg(test)]` registry entries (experimental range):

1. Round trip of a `Task` with extensions through bincode and JSON; the hex form; an absent `extensions` in JSON gives an empty list.
2. Unknown non-critical: decodes, `Task::validate` passes, the list is unchanged after a master queue round trip and a journal replay, and after an `AssignTask` encode and decode.
3. Unknown critical: `Task::validate` fails with the id in the message; the worker refusal returns a failed `TaskResult` naming the id.
4. Known critical with `critical = false` on the wire is refused (D2.6); a known id in the wrong list is refused; a duplicate id is refused; id `0` is refused.
5. Limits: 16 accepted and 17 refused; 64 KiB accepted and 64 KiB + 1 refused; 256 KiB total accepted and over refused; a `SubmitTask` over the limit gets `ClientResponse::Error` and nothing is queued.
6. Scheduler: a task with a known critical `Worker`-scope extension waits (`ClusterLacksCapability` names the id) when no worker has `ext:<id>`, and is placed when one has it; a `Master`-scope extension does not restrict workers and is absent from the copy sent to the worker; `Shape` differs between tasks with different required ids.
7. A decoder fuzz-style test: random bytes for each registered decoder never panic.
8. `crates/core/tests/data/wire_layout.txt`: regenerate under `UPDATE_WIRE_LAYOUT=1`. `sample_task()` (`crates/core/src/wire_layout_tests.rs:144`, `:173`) gets one non-critical and one critical extension in `Task` and one in `TaskRequirements`, so the bytes of the lists are recorded. The test still has to fail when the lists change without a version bump. The registry table is recorded in the same file (one `Extension::<id> <name> <list> <critical> <scope>` line per entry, sorted by id), so a changed or removed id is a visible diff.
9. `legacy_v9_submit_task_is_refused_with_a_version_error` (D6, before PROTO-4; replaced by `decode_v9_submit_task_is_accepted_and_has_empty_lists` when PROTO-4 opens the window); store test that a format-3 snapshot loads in format 4.
10. **Cross-version (after PROTO-4):** the skew job of design 0076 D2 and an in-process test with a v9 peer: a v9 `SubmitTask` becomes a task with empty lists; a v10 task with a non-critical extension reaches a v9 worker without it and runs; a v10 task with a critical `Worker` extension never reaches a v9 worker.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| PROTO-3a | `Extension`, the two fields, `extension_bytes`, registry, bounds and `Task::validate` rules (D1 to D3) | `crates/core/src/task.rs` | Tests 1 to 5 and 7 pass | Implemented |
| PROTO-3b | Protocol 10: version constants, `LegacyV9Task` and wrapper, `wire_layout.txt`, `STORE_FORMAT_VERSION` 4 (D1, D6) | `crates/core/src/protocol.rs`, `crates/core/src/protocol/client_msg_mirrors.rs`, `crates/core/src/wire_layout_tests.rs`, `crates/core/tests/data/wire_layout.txt`, `crates/master/src/store.rs`, `docs/design/0040-protocol-versioning.md`, `DEPLOYMENT.md` | Test 8 and 9 pass; "wire change" pull request, release within 48 hours | Implemented (the release is pending) |
| PROTO-3c | `ext:` worker tag, `TaskView::required_extensions`, scheduler check, worker refusal (D2.3, D4) | `crates/core/src/placement.rs`, `crates/worker/src/client.rs`, `crates/worker/src/client/tasks.rs`, `crates/master/src/queue.rs`, `crates/master/src/scheduler.rs`, `CLAUDE.md` | Tests 3 and 6 pass | Implemented |
| PROTO-3d | Cross-version tests | `tests/window_v9_peers.rs`, `crates/core/src/window_tests.rs`, `scripts/release_skew.py` | Test 10 passes (needs PROTO-4) | Implemented (pull request 274, design 0083) |

The three first items fit one pull request (L, as in the backlog). They land at the next wire change (Q65).

## Open decisions

| ID | Question | Recommended default |
|---|---|---|
| Q68 | Leave `workspace` and `vram_mb` as typed fields (D5)? | Yes. Extensions start with the next new optional field |
| Q69 | How does a worker advertise the extensions it understands (D4)? | A worker tag `ext:<id>`, the tenth reserved prefix. The alternative is a new field in `Register`, which is another wire change |
| Q70 | Two lists (`Task` and `TaskRequirements`) or one on `Task`? | Two, as design 0076 D3 says. Requirements stay the place the scheduler reads |
| Q71 | Limits: 16 per list, 64 KiB each, 256 KiB per task? | Yes. Raise a limit with the first feature that needs more |
| Q72 | Should the registry fix `critical` per id (D2.6)? | Yes. It keeps a sender from lowering a restriction |

## Status log

- 2026-11-17: Draft written for PROTO-3 (design 0076 D3, Q65). Statements about code were checked on `master` `619b609`. Builds nothing.
- 2026-11-17: The owner decided Q68 to Q72 in chat, all on the recommended defaults: `workspace` and
  `vram_mb` stay typed (Q68); workers advertise `ext:<id>` tags, a tenth reserved prefix (Q69); two
  lists (Q70); limits 16 per list, 64 KiB per extension, 256 KiB per task (Q71); the registry fixes
  `critical` per id (Q72).
- 2026-11-17: Built by draft pull request 272 (PROTO-3a, PROTO-3b, PROTO-3c): protocol 10, store
  format 4, `Extension` and the empty registry, the frozen v9 `SubmitTask` decoder, the `ext:` tag,
  the scheduler check and the worker refusal. The pull request is a wire change and holds until
  PROTO-4 (design 0076 D1). PROTO-3d (test 10, the cross-version tests with a v9 peer) waits for
  PROTO-4; before it, the skew job (design 0076 D2) shows both cross pairs refused with the version
  error and the pull request pair working. Deviations from this record, where the code forced or
  allowed a different shape:
  - **Test registry (D3).** The test entries sit in `TEST_EXTENSION_REGISTRY` behind the core
    feature `test-extensions`, which only the dev-dependencies of `rusty_grid_master` and
    `rusty_grid_worker` enable, so their tests can use known ids. A plain `#[cfg(test)]` entry is
    invisible to the other crates. A release build has an empty registry and no test entry. The wire
    layout file records `EXTENSION_REGISTRY` only (one `Extension::<id, 5 digits>` line per entry,
    none today), so the file does not depend on the feature. As a consequence of the existing layout
    test, adding or editing a registry id needs a `PROTOCOL_VERSION_CURRENT` bump with the
    regeneration, and changing or removing one needs `MIN` too; that is stricter than D1's "may
    move", and it is the visible diff D3 asked for.
  - **Location (D1, D3).** The types, registry, limits and receiver rules sit in
    `crates/core/src/task/extension.rs`, re-exported by `crates/core/src/task.rs` (so the path
    `rusty_grid_core::task::Extension` is as written): `task.rs` would have passed the 3,000-line
    check of design 0080.
  - **`decode` (D3)** is `fn(&[u8]) -> Result<(), String>`: it checks the value. The typed value
    arrives with the first real id, whose record fixes its type.
  - **Placement (D4.3).** There is no `TaskView::required_extensions` and no new `Shape` field.
    `Placement` gains `extensions: Vec<u16>`, which is not readable from `require:` tags;
    `Task::scheduling_placement` adds the ids of the task's known critical `Worker`/`Both`
    extensions to the placement, `TaskView::placement` carries it, the worker index matches it
    against the `ext:<id>` tag, and `Shape` already contains the placement. The effect is the one D4
    states, and the wait and unfulfillable message is "no worker matches the placement constraints
    (ext=<id>)".
  - **Stripping (D4.4)** removes every extension of scope `Master` from the worker's copy, critical
    or not, in the one place that builds `AssignTask` (`WorkloadScheduler`); D4.4 spoke of critical
    ones only. The stored task keeps all of them.
  - **Messages.** Bound and registry errors read `Task <id>: ...`; the unknown critical one reads
    `Task <id> needs extension <n>, which this master does not support` (`worker` at assignment).
  - The worker's refusal is a plain failed `TaskResult`, not one of the `WORKER_REJECTION_MARKERS`
    texts, so the master does not requeue the task onto another worker: no other worker would run
    it either.
  - `Task::validate` checks the extension lists after placement; the queue charges extension bytes
    (`3 + bytes.len()` per entry) in `task_active_bytes`, with a test.
  - Unverified here: Windows and Linux runs of the new tests, and test 10 (PROTO-3d).
- 2026-11-17: PROTO-3d built with PROTO-4 (pull request 274, [design 0083](0083-one-version-window.md)): test 10 is `tests/window_v9_peers.rs` (a v9 worker registers and runs a task; a non-critical extension is dropped from its copy and a critical `Worker` extension never reaches it; a v9 client submits, polls and lists, in both codecs) plus the skew job. The D6 rule "a v10 master refuses a v9 `SubmitTask`" is replaced by the window: a v9 `SubmitTask` is read through `LegacyV9Task` (D2 of design 0083), and `legacy_v9_submit_task_is_refused_with_a_version_error` became `decode_v9_submit_task_is_accepted_and_has_empty_lists`.
