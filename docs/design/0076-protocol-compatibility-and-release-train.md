# 0076: Protocol compatibility and a release train

Status: Draft (plan; PROTO-1 is built; Q64 to Q66 are decided, Q67 is open)
Created: 2026-11-16    Updated: 2026-11-16

## Summary

Every change to the wire format breaks every older peer, and the format changes often. In 31 days
the protocol went from version 1 to version 9, four times in one day. A binary from the newest
release cannot join a master built from `master` on the day of that release. This record plans four steps,
from cheap to costly: tie each protocol bump to a release, detect skew in CI, give optional task
fields a slot that does not change the layout, and, before 1.0, accept peers one version back.
Nothing in the product changes in this record.

## Context

### How versions are checked today

- A worker's `Register`, and every `ClientMessage` and `ClientResponse`, carries
  `protocol_version`. Both sides refuse a peer outside
  `PROTOCOL_VERSION_MIN_COMPATIBLE..=PROTOCOL_VERSION_CURRENT` (design 0040).
- Messages are encoded positionally with bincode. Adding a field to `Task` or
  `TaskRequirements` changes the bytes of existing variants.
- The wire layout test (`crates/core/src/wire_layout_tests.rs`) compares encoded bytes with
  `crates/core/tests/data/wire_layout.txt`. A new variant needs a bump of
  `PROTOCOL_VERSION_CURRENT`. A changed or removed variant also forces
  `PROTOCOL_VERSION_MIN_COMPATIBLE` up to `PROTOCOL_VERSION_CURRENT` (design 0040, lines 80 to 85).
- Design 0040 lists "rolling multi-version wire compatibility" as a non-goal: "All nodes in a
  cluster must run matching builds."
- `DEPLOYMENT.md` section 3.5 tells operators to upgrade and restart the master first.
- Some messages were added without a layout change: `WorkerMessage::BlobRequest` and
  `MasterMessage::BlobReply` sit after `UnknownVariant`, and optional abilities are announced as
  `ProtocolCapabilities` bits and `RegisterAck.master_features`.
- `LegacyV5BinaryClientMessage` and `LegacyV6ToV8BinaryClientMessage` decode old frames only to
  return a clear `Incompatible protocol version` error.

### How often it changed

The value of `PROTOCOL_VERSION_CURRENT` on `master`, from `git log -G` on
`crates/core/src/protocol.rs` at `96ce571`. `PROTOCOL_VERSION_MIN_COMPATIBLE` was equal to it at
every step.

| Version | Date | Commit | Cause |
|---|---|---|---|
| 1 | 2026-10-16 | `58c0eb0` | First versioned protocol |
| 2 | 2026-11-01 | `e238708` | Refuse mismatched peers (INT-5) |
| 3 | 2026-11-08 | `aee4173` | Versioning enforced, golden bytes (FEAT-8) |
| 4 | 2026-11-08 | `0f956ae` | Session resume, `compute_intensity` removed (FEAT-1) |
| 5 | 2026-11-08 | `c930e04` | Review fixes of pull request 147 |
| 6 | 2026-11-08 | `44e4366` | `Task::workspace` and `Hello` (design 0039 W1) |
| 7 | 2026-11-09 | `8a4f7d9` | Workspace flags and the `workspaces` tool (design 0039 W5) |
| 8 | 2026-11-10 | `98fee71` | Invites and members (ENROLL-1b) |
| 9 | 2026-11-15 | `1b159b0` | `TaskRequirements::vram_mb` (FEAT-9a, design 0068) |

Releases: `v0.1.0` (2026-11-09), `v0.1.1` (2026-11-10) and `v0.1.2` (2026-11-12) speak version 7.
`v0.2.0` (2026-11-15) speaks version 8. Pull request 232 moved `master` to version 9 on the same day.

### What this costs

1. **Released binaries go stale at once.** No release speaks version 9, so a person who installs
   `v0.2.0` cannot join a master built from `master`, and the reverse.
2. **Every machine moves together.** On 2026-11-16 the GPU run between a Mac and a Windows
   machine (`benchmarks/gpu-run/`, pull request 244) needed both binaries built from one commit.
   A trial cluster on a released version must stay on that release until every member upgrades.
3. **Operators cannot upgrade one machine at a time.** The master goes first, and every worker and
   client is refused until it follows.
4. **Each bump costs code that only refuses.** Each step adds or extends a `Legacy*` decoder that
   exists to print a clear error.

## Goals

- G1: Every published release speaks the protocol of the `master` commit it was cut from, and no
  protocol version exists on `master` for more than a short, stated time without a release.
- G2: CI shows, on every pull request that changes the wire layout, how the newest release and the
  new build behave together.
- G3: Adding an optional field to a task or its requirements no longer forces
  `PROTOCOL_VERSION_MIN_COMPATIBLE` up.
- G4: Before 1.0, a master accepts workers and clients one protocol version older than itself.

## Non-goals

- Compatibility with more than one older version.
- Replacing bincode. Design 0040 D4 holds that plan.
- Compatibility of the Agent Mesh protocol, which is separate from the grid's.

## Design

### D1: A release train for wire changes

A pull request that bumps `PROTOCOL_VERSION_CURRENT` is a release event:

1. The pull request description says "wire change" and names the version.
2. A release is cut within a stated time after it merges (Q64 proposes 48 hours), or before the
   next wire change merges, whichever comes first.
3. Two wire changes may land between releases only if they share one version bump. The second
   one then extends the first one's `wire_layout.txt` entry instead of bumping again.

This needs no code. It needs one line in `CLAUDE.md`, one in `docs/README.md`, and a pull request
template, which the repository does not have yet (`.github/pull_request_template.md`).

### D2: A skew job in CI

A CI job downloads the newest release archive for Linux (`SHA256SUMS` checked) and runs three
pairs against the binary of the pull request:

| Master | Worker or client | Expected today |
|---|---|---|
| release | pull request | Refused with the version error, or works |
| pull request | release | Refused with the version error, or works |
| pull request | pull request | Works |

The job does not demand compatibility. It fails only when a refused pair does not print the version
error, or when the release and the pull request speak the same version but a pair does not work.
Its output, a three-row table, goes into the job summary so a reviewer sees the skew.

### D3: An extension slot for optional task fields

The next wire change adds one field at the end of `Task` and of `TaskRequirements`:
`extensions: Vec<Extension>`, where `Extension` is `{ id: u16, critical: bool, bytes: Vec<u8> }`.
After that, an optional field travels as an extension instead of as a new struct field:

- The receiver decodes the extensions it knows and keeps the others.
- An unknown extension with `critical = false` is ignored.
- An unknown extension with `critical = true` makes the receiver refuse the task with a clear
  error. A field that restricts where or how a task runs (isolation, placement, limits) is always
  critical, so an older worker can never run a task while silently dropping one of its limits.
- Extension ids are listed in one table in `crates/core/src/task.rs`, and the wire layout test
  records them.

With this slot, the changes of versions 6 and 9 (`Task::workspace`, `TaskRequirements::vram_mb`)
would have been additive: `PROTOCOL_VERSION_CURRENT` would still move, and
`PROTOCOL_VERSION_MIN_COMPATIBLE` would not.

### D4: A window of one version before 1.0

Once D3 is in, the master accepts peers of `PROTOCOL_VERSION_CURRENT - 1`:

- `PROTOCOL_VERSION_MIN_COMPATIBLE` becomes `PROTOCOL_VERSION_CURRENT - 1`, and the wire layout
  test enforces that range instead of equality.
- A change that cannot be additive (a removed or reshaped variant) still raises the minimum. The
  pull request then states that the window is closed for that version, and D1 applies.
- The skew job of D2 then expects "works" for the release one version back.

## Compatibility

This record changes nothing. D3 is a wire change and follows design 0040 when it is built. D4
changes the rule of design 0040 and its non-goal, and needs that record updated.

## Security

- D3 adds a new input. Extensions are bounded in count and size by the existing frame limit, and a
  limit per task is part of the build item.
- The `critical` bit is the safety rule of D3: a worker that does not understand a restriction
  refuses the task instead of running it without the restriction.
- D4 means a master talks to older code. A security fix that changes the wire closes the window
  for that version, as in D4.

## Test plan

No code. D2 is itself a test. D3 and D4 each get a design record with tests before code.

## Work items

Sizes are the backlog's S, M and L. The items are in Phase 12 of [BACKLOG.md](../BACKLOG.md) under the same IDs. REL-6 is the step before that phase (pull request 260).

| ID | Item | Size | Needs | Files | Done when | Status |
|---|---|---|---|---|---|---|
| REL-6 | Release `v0.3.0` at protocol 9 | S | Owner (a tag) | `CHANGELOG.md` | A release speaks the protocol of `master` | Planned (pull request 260) |
| PROTO-1 | The release train rule of D1 | S | Q64 | `CLAUDE.md`, `.github/pull_request_template.md` (new), `docs/README.md` | The rule is written where agents and contributors read it | Implemented by the pull request that plans 0.4.0 |
| PROTO-2 | The skew job of D2 | M | PROTO-1 | `.github/workflows/ci.yml`, `scripts/release_skew.py` | Each pull request shows the release-to-build table | Implemented (pull request 269) |
| PROTO-3 | The extension slot of D3 | L | Q65, a design record | `crates/core/src/task.rs`, `crates/core/src/protocol.rs` | An optional field can be added with no change to `PROTOCOL_VERSION_MIN_COMPATIBLE` | Planned |
| PROTO-4 | The one-version window of D4 | L | Q66, PROTO-3 | `crates/core/src/protocol.rs`, `crates/core/src/wire_layout_tests.rs`, design 0040 | A worker one version back registers and runs a task | Planned |

## Open decisions

| ID | Question | Recommended default |
|---|---|---|
| Q64 | How long may `master` speak a protocol version that no release speaks? | 48 hours, and never across two wire changes |
| Q65 | Build the extension slot (D3) at the next wire change? | Yes. It costs one bump and saves the minimum bump of every later optional field |
| Q66 | When does the one-version window (D4) start? | After D3, and before the first release that people outside the owner's machines install |
| Q67 | Cut `v0.3.0` now, at protocol 9 (REL-6)? | Yes, after the open fixes of the audit of 2026-11-15 merge |

## Status log

- 2026-11-16: Draft written from the evaluation of `master` `96ce571`. The version table comes
  from `git log -G 'pub const PROTOCOL_VERSION_CURRENT: u32 = [0-9]+'` on
  `crates/core/src/protocol.rs`, and the release versions from the same file at each tag.
- 2026-11-16: Q64, Q65 and Q66 take the recommended defaults (owner's go-ahead in chat on 2026-11-16 to build 0076 for 0.4.0): 48 hours and never across two wire changes; build the extension slot at the next wire change; the one-version window starts after D3. PROTO-1 to PROTO-4 are in Phase 12 of the backlog, and PROTO-1 is Implemented by the pull request that plans 0.4.0.
- 2026-11-17: D2 built (PROTO-2, pull request 269): `scripts/release_skew.py` and the CI job `release skew (ubuntu-latest)`. A pair that is refused must print `Incompatible protocol version N from <role> (supported: MIN..=CUR; ...)`, which also gives both protocol versions for the table. Run locally against the `v0.3.0` macOS archive, all three pairs work at protocol 9; with the pull request build temporarily at protocol 10 the two cross pairs are refused with the version error.
- 2026-11-17: The design record for PROTO-3 (D3) is [0082](0082-task-extension-slot.md), Draft. It fixes the types, bounds, the placement rule and the v9 rule for PROTO-4, and asks Q68 to Q72.
- 2026-11-17: D3 built (PROTO-3, draft pull request PRNUM, design 0082): protocol 10 with the extension slot. It holds until PROTO-4 and a release follows within 48 hours of the merge.
