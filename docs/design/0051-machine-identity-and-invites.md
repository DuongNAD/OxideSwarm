# 0051: A key per machine, invites and revocation

Status: Accepted (direction chosen by the owner on 2026-11-09, decision Q13); Partial: ENROLL-1a and ENROLL-1b built; ENROLL-1c and ENROLL-1d planned
Created: 2026-11-09    Updated: 2026-11-11

## Summary

Each machine gets its own key. The master keeps a list of the machines it admits, called members.
A new machine joins by redeeming an **invite**. An invite works once or N times, expires, needs the
owner's approval by default, and does not contain the cluster secret. Revoking one member closes
every connection it has and stops its work, without a new secret for every other machine. The
handshake becomes Noise `IK`. It adds forward secrecy, and a joining machine can authenticate the
master before it sends anything. Clusters that use the shared secret keep working. Item
`ENROLL-1`.

## Context

- Today one shared secret is the only credential (`crates/core/src/cluster_auth.rs`, OXS-AUTH v2).
  Every worker, every client and the master hold the same 32 bytes. Whoever holds them can run
  commands on every worker (`docs/SECURITY.md`).
- Adding a machine means moving the secret, by a file copy or with the join token of
  [design 0048](0048-join-token.md), which is the secret in another form. It must not travel
  through chat or e-mail.
- Removing one machine needs a new secret everywhere (`docs/SECURITY.md` section 5).
- A worker id is "an identity, not a credential" (`docs/SECURITY.md` section 8). Two workers with
  the same id displace each other at the master.
- The v2 handshake has no forward secrecy. Its keys derive from the long-term secret and from
  nonces sent in the clear.
- On 2026-11-09 the owner chose ecosystem model A (decision Q13 in `docs/ROADMAP.md`): each person
  or team runs a cluster, and trusted people add their machines to it. One shared secret cannot
  carry that.

## Goals

- **G1** No machine except the master's host needs the cluster secret. The other machines
  authenticate with a key that never leaves them.
- **G2** An unused invite may travel through chat or e-mail. It works once by default, expires
  after 24 hours by default, and needs the owner's approval by default. Once it is used or
  expired, it is worthless.
- **G3** The joining machine authenticates the master pinned in the invite. Before the machine
  saves anything or starts work, its user confirms that the owner's master shows the same
  pairing check. Joining any cluster gives that cluster code execution on the joining machine,
  so `join` says this before it asks.
- **G4** `oxideswarm members revoke <NAME|ID>` removes one member. All its connections close at
  once, on every lane. Tasks it submitted are cancelled, and tasks running on it are requeued at
  once, without the resume hold and without its checkpoints. Its later handshakes are refused.
- **G5** Sessions have forward secrecy.
- **G6** Clusters that use the shared secret keep working. A master can admit secret peers and
  member peers side by side.
- **G7** The same handshake runs on TCP and on iroh streams.

## Non-goals

- Permissions per user or per project, and quotas. That is ECO-3 (design 0050 D6). A member with
  the `client` role can still run commands on every worker, within `--isolation`.
- Strangers running code on each other's machines (model B), which Q13 defers.
- A server run by the project (Q11).
- Android (`nativeSetClusterSecret`), the system-service installers that run under another home
  directory, and rotation of the master's key. They keep the shared secret under `--auth both` and
  get follow-up items.
- Short codes and USB kits ([0052](0052-short-code-pairing.md), [0053](0053-usb-setup-kit.md)).

## Design

### D1 Keys

- Every machine has one static X25519 key pair for the cluster handshake, `~/.oxideswarm/node.key`.
  It is a private file (`crates/core/src/private_file.rs`), created on first use, and it never
  leaves the machine.
- The master has its own static key, `~/.oxideswarm/master_noise.key`, separate from the iroh
  identity `master_key.bin`, so that no key serves two protocols. With `--state-dir`, both keys
  live in the state directory.
- A member's ID is the first 16 hex characters of the SHA-256 of its public key. A key's
  fingerprint, shown to people, is the same value in groups of four.

### D2 Handshake: OXS-AUTH v3 (Noise `IK`)

1. **Cleartext HELLO** (`0x03` frame, as in v2): auth version `3`, the client's protocol version
   and the purpose, `member` or `enroll`. The master checks the protocol version here, before any
   Noise message, so a version mismatch still gets the explicit error of design 0040.
2. **Noise:** `Noise_IK_25519_ChaChaPoly_BLAKE2s` through the `snow` crate. The prologue is the
   exact HELLO bytes. The initiator, a worker or a client, knows the master's static key from its
   invite or its config. The initiator's static key travels encrypted in message 1.
3. **Record layer:** after message 2 both sides extract the two raw directional `Split()` cipher
   keys `(k_i2r, k_r2i)` via `snow::HandshakeState::dangerously_get_raw_split()`, derive
   `session_key = HMAC-SHA256(key = "oxs3-split-session-key", k_i2r || k_r2i)` (volatile-zeroing
   `k_i2r` and `k_r2i`), and feed `session_key` into `MessageTransport::enable_encryption` so the
   existing AEAD record layer (`0x04`, `AeadReaderSession`) derives `k_c2s` and `k_s2c` strictly
   from the Noise `Split()` output (never from public handshake values). Every later message,
   including `ENROLL`, `ENROLLED`, `REJECT` and the pending-approval messages, is an encrypted
   record. A failure after this point closes the connection without a cleartext answer.
4. **Authenticated only after the first record.** IK message 1 can be replayed, so the master
   treats the peer as authenticated only after the first transport record from it decrypts.
5. **Admission:** a key in the member list goes on with its roles (D6). With purpose `enroll`, any
   initiator key may complete the Noise `IK` handshake, but the first encrypted record within the
   5-second handshake watchdog must be `ClientMessage::Enroll` (D4); no pre-`Enroll` `Hello` round
   trip or idle waiter pool exists. The frame limit stays `PRE_AUTH_MAX_FRAME` (1 KiB) until the key
   is a member. Anything else closes the connection.
6. **Limits:** a v3 responder does four X25519 operations and one key generation per attempt. v3
   shares the global `DEFAULT_MAX_PENDING_HANDSHAKES` (64) and `FAILURE_DELAY` with v2. There are
   no per-address limits, because iroh node ids and NAT make addresses cheap.
7. **Choosing a version:** a machine with an enrolment (a pinned master key in its config and a
   `node.key`) uses v3. A machine with only a secret uses v2. `master_requires_secret`
   (`crates/cli/src/connect.rs`) becomes "requires a credential": a secret or an enrolment.

### D3 Invites

```
oxideswarm invite [--roles worker[,client]] [--uses N] [--expires 24h] [--no-approval]
                  [--master HOST:PORT] [--p2p-ticket-file PATH] [--list | --revoke ID]
```

- An admin (D6) runs it. It prints `oxs1-invite:<base64url payload>.<check>`.
- The payload is a JSON object with these fields:
  - the master's addresses and P2P ticket, as in join tokens;
  - the master's static public key;
  - `invite_id` (16 random bytes) and `invite_secret` (32 random bytes);
  - the expiry time and the roles.
- `<check>` is 8 bytes of SHA-256 over the payload. It catches typos only. It is not a MAC, so
  nothing in the invite is trusted until the handshake and the pairing check (D4) have passed.
- The master stores each invite (D5): the id, the secret, the roles, the uses left, the expiry,
  whether approval is needed, and the creator.
- The defaults are 1 use, 24 hours, approval required and the role `worker`. An invite that
  grants `admin` always needs approval and has exactly one use.

### D4 Enrolment and approval

1. **Start.** The new machine runs `oxideswarm join` with the invite on standard input, as with join
   tokens, verifies that a controlling terminal is available, and creates `node.key` in memory if it
   is missing on disk.
   - **No-approval invites (`--no-approval`):** before opening any network connection, `join`
     flushes pending terminal input, prints `Master key fingerprint: xxxx-xxxx-xxxx-xxxx` (omitting
     the pairing check, since no admin compares one) and the G3 warning, and prompts `[y/N]`. Only
     if the user confirms does `join` open the connection, run v3 with purpose `enroll` against the
     pinned `master_key`, and send `ENROLL`. Declining neither touches the network nor writes files.
   - **Approval-required invites (default):** `join` opens the connection and runs v3 with purpose
     `enroll` and the pinned master key. A master whose key does not match the pin is refused.
2. **Request.** The new machine sends `ENROLL { invite_id, proof, name, os, arch }`. The proof is
   `HMAC-SHA256(invite_secret, "oxs3-enroll" || handshake_hash)`, which binds it to this session and
   to the pinned master key. For a `--no-approval` invite, a valid request deducts one use and
   answers `ENROLLED` at once.
3. **Pending (approval-required invites).** The master checks the invite (known, not expired, not
   revoked, uses left) and the proof. It then adds a **pending request** tied to this live
   connection, which sends keepalives.
   - The use is not taken yet.
   - Several requests may be pending for the same invite. `members pending` marks them, for example
     "2 requests for a 1-use invite".
   - A pending request ends when its connection closes, when 10 minutes pass, or when the master
     restarts. In each case the use is not taken, and the new machine can try again.
   - At most 32 requests may be pending.
4. **Pairing check (approval-required invites).** Both sides derive a 6-digit pairing check from the
   handshake hash. Then:
   - the master shows the request in `members pending` (with the remote peer address and invite
     contention count; human output omits `pairing_code` and `master_fingerprint` so the admin
     obtains the 6-digit code and optional `--fingerprint` out-of-band from the joining machine,
     while `members pending --json` includes `master_fingerprint`) and the dashboard;
   - the new machine flushes pending terminal input and prints the pairing check on the same line as
     the master's fingerprint (`Pairing code: NNNNNN  (Master key fingerprint: xxxx-xxxx-xxxx-xxxx)`),
     followed by the warning of G3.
5. **Approval.** An admin approves with `oxideswarm members approve <PENDING_ID> --code <NNNNNN> [--fingerprint <XXXX-XXXX-XXXX-XXXX>]`,
   from the master's host, from any admin member, or in the dashboard as Admin. The admin compares
   the pairing check, master key fingerprint, name, OS and remote address with what the new machine
   shows, and passes the 6-digit code shown on the joining machine so the master verifies it in
   constant time before taking a use or adding the member. Approval takes one use, persists the
   member in `members.json` with the invite's roles, and answers `ENROLLED`. `members reject`
   refuses the request.
6. **Confirmation on the new machine.** For an approval-required invite, the user confirms that the
   owner sees the same pairing check (`[y/N]`; `--yes` only with `--invite-file` and a kit
   passphrase, design 0053), and `join` sends `ClientMessage::Hello` (`ClientResponse::Hello`) to
   confirm on the enrollment stream.
   - After sending `ENROLLED`, the master waits up to 120 seconds (`ENROLL_CONFIRM_TIMEOUT_SECS`)
     for that `Hello` confirmation. If the joining machine declines, disconnects, or lets the
     120-second window expire (`enrollment confirmation timed out`), the master rolls back the
     unconfirmed approval (using generation-checked compare-and-swap that preserves concurrent
     admin renames, restores the invite use only when the public key is no longer a member, and
     disconnects any worker sessions).
   - Because `members approve` persists to `members.json` immediately while the unconfirmed
     rollback tracker lives in memory only, a master restart inside that 120-second confirmation
     window leaves the approved member enrolled permanently.
   - Only after both the approval (if required) and the `[y/N]` confirmation succeed does `join`:
     - save `node.key` (if newly created) and the master's address, ticket and public key in
       `~/.oxideswarm/rusty-grid.toml`;
     - run the checks that its roles allow (D6).
   A refusal on either side leaves nothing written on the joining machine.
7. **Errors.** A wrong proof or an unknown, expired or revoked invite gets one generic `REJECT`. The
   master's log names the case.

### D5 Member store, worker ids and revocation

- **Store.** `members.json` lives in `--state-dir`, or else in `~/.oxideswarm/`, as a private file
  that is written atomically. A secret-only master (`--auth secret`) never creates `members.json`.
  When members are enabled, `members.json` holds:
  - the members: public key, ID, name, roles, created, last seen, and which invite enrolled it;
  - the invites;
  - the **worker ids each member owns** and the bounded `v2_worker_seq` map of seen v2 worker ids.
- **Commands.** `oxideswarm members list | pending | approve | reject | revoke | rename | release-worker`
  reach the master as `ClientMessage` variants (`ReleaseWorkerId { worker_id }` ->
  `WorkerIdReleased { worker_id, released }`).
- **Worker ids.** The first `Register` of a worker id from a member records that member as its
  owner (up to 64 worker ids per member, with persisted LRU eviction of non-live claims via
  `worker_last_seq`; reconnects of an already-owned id update in-memory LRU order without rewriting
  `members.json`). A `Register` with an id owned by another member is refused with a generic wire
  error (never exposing local master paths). Without this rule, member A could take over member B's
  worker id, displace B's session and receive B's `require:worker=<id>` tasks.
  - Under `--auth secret`, v2 peers share worker ids without touching `members.json`.
  - Under `--auth both`, v2 peers record newly seen worker ids in `v2_worker_seq` (persisting only
    when a new id is added or an entry past 1,024 is evicted, logging a local warning without
    refusing `Register` if the disk write fails) and can reclaim a non-live worker id from a member
    claim across master restarts.
  - Under `--auth members`, `v2_worker_seq` is ignored so former v2 workers can register directly as
    enrolled members; under `--auth both`, an admin releases a stopped worker's id with
    `oxideswarm members release-worker <WORKER_ID>` (D6).
  - Known limits of `v2_worker_seq`:
    1. it protects only v2 worker ids that have registered at least once since upgrading the master;
    2. it is capped at 1,024 entries (`MAX_TRACKED_V2_WORKER_IDS`), and unnamed v2 workers generate a
       new UUID per restart, so 1,024 unnamed v2 restarts can evict a stopped named v2 worker's
       entry;
    3. `ListWorkers` is permitted to any member (D6), so any member can read active worker ids.
- **Revocation** removes the member and then:
  1. closes every connection the member has, from a registry of connections per member that
     covers worker sessions (both QUIC lanes), client connections, `BlobConnection`, P2P
     `STREAM_CLIENT` and MCP;
  2. cancels the tasks the member submitted, whether queued or running. The master records the
     submitting member of each task in its queue and journal; this needs no wire change;
  3. requeues the tasks running on the member's workers at once, skipping the session-resume hold
     and dropping checkpoints those workers wrote;
  4. refuses the member's key at its next handshake.

  Under `--auth both`, a revoked machine that also holds the cluster secret can come back through
  v2, so `revoke` warns and points at secret rotation.

### D6 Roles and the master's host

- A member has a set of roles: `worker`, `client` and `admin`. `admin` includes `client`. The owner's
  laptop can be `worker,client`.
- Each message has a required role, checked by the master per message and enforced by tests:

| Messages | Role |
|---|---|
| `Hello`, `ClusterStatus`, `ListWorkers`, `ListWorkspaces` (read-only) | any member |
| `Register` and the other worker messages | `worker` |
| `SubmitTask`, `CancelTask`, `GetTaskResult`, map/reduce, blob uploads | `client` |
| Invites (`CreateInvite`, `ListInvites`, `RevokeInvite`), members (`ListMembers`, `RevokeMember`, `RenameMember`, `ReleaseWorkerId` -> `WorkerIdReleased`), approval (`ListPendingEnrollments`, `ApproveEnrollment`, `RejectEnrollment`) | `admin` |

- `join` and the installers run only the checks that the roles allow. A worker-only member waits
  until `workers` lists its own id as connected (read-only). The doctor's task probes need
  `client`.
- **The master's host:** at first start with `--auth both` or `members`, the master enrols the host's
  own `node.key` as an `admin` member. The CLI on the master's host is therefore admin under every
  `--auth` mode. A v2 secret peer is admin under `secret` and `both`, as today.
- **`master --auth secret|members|both`** (`RUSTY_GRID_AUTH`, `[master].auth`), default `both`:
  - `members` refuses v2 peers, with the distinct reason "this master admits enrolled machines
    only; join with an invite", and ignores `v2_worker_seq` so former v2 workers can register
    directly as enrolled members with their persisted named `worker_id`;
  - while running `--auth both`, if a named worker (`worker --name <NAME>`, persisted
    `~/.oxideswarm/worker_id.<name>`) moves from shared-secret v2 to an enrolled v3 member, stop
    the v2 worker and run `oxideswarm members release-worker <WORKER_ID>` as an admin (the refusal
    message prints this command) so the enrolled member can claim that `worker_id`;
  - once every machine has joined with an invite, the owner can switch to `members` and delete the
    secret everywhere except the master's host.
- Loopback runs without a secret keep working unauthenticated.

### D7 Other networks

The invite carries the P2P ticket when the master runs `--p2p`, so a machine on another network
joins through iroh, through the owner's relay if one is set (`--relay-url`,
[design 0049](0049-custom-relay.md)). v3 runs on the iroh stream as v2 does now.

## Compatibility

- The protocol version goes to 8 (new `ClientMessage` and `ClientResponse` variants for invites,
  members and approval). `PROTOCOL_VERSION_MIN_COMPATIBLE` follows, so upgrade the master first
  (`DEPLOYMENT.md` 3.5). The release is `0.2.0`.
- `AUTH_VERSION` 3 is added, and 2 stays under `--auth secret|both`.
- Join tokens (`oxs1-join:`) keep working where the master admits secret peers.

## Security

- **Trust model (model A).** Joining a cluster gives it code execution on the joining machine. A
  `client` member can run any command on any worker of the cluster. Members are people the owner
  trusts. The guide and `docs/SECURITY.md` say both, and recommend `--isolation fs` where it is
  supported.
- **A stolen unused invite.** The thief can make a request, but it stays pending next to the real
  machine's request and takes no use. The admin sees two requests and a pairing check that does not
  match the real machine's screen, and rejects the wrong one. `--no-approval` removes this
  protection, so `invite` warns, and the USB kit (0053) allows it only with a passphrase and a
  short expiry.
- **A forged or altered invite** (one that pins an attacker's master). The joining user sees a
  fingerprint and a pairing check that the owner's master does not show, and refuses. `join`
  writes nothing and starts nothing before that confirmation.
- **A stolen node key.** Revoke the member (G4). The key cannot impersonate the master, whose key is
  pinned.
- **A stolen master host** is a full compromise, as today.
- **Logging.** No invite secret, node private key, proof or pairing check is logged, put into an
  error, or shown by `Debug`. The redaction rules of `JoinToken` apply.

## Test plan

- **Unit:**
  - invite encoding and parsing, typos, expiry and uses;
  - a proof bound to another handshake hash is refused, and so is a wrong pinned master key;
  - a replayed IK message 1 never becomes an authenticated session;
  - v3 round trip, and the `Split()` keys in both directions;
  - pre-auth frames over 1 KiB are refused, and the role table per message is enforced.
- **Integration** (`tests/cli_enroll_e2e.rs`, temporary home directories, loopback master):
  - invite, join, approve, confirm, and the worker runs a task;
  - two requests for a one-use invite: rejecting one and approving the other takes one use;
  - a pending request whose connection closes takes no use;
  - a worker-only member cannot submit;
  - a member cannot register another member's worker id;
  - a v2 secret worker and a v3 member worker run side by side;
  - `--auth members` refuses the v2 worker with the distinct reason, and the master's host CLI
    stays admin;
  - an expired invite and a rejected request write nothing.
- **Revocation** (`tests/cli_revoke_e2e.rs`): a revoked client loses its open `submit --wait`, MCP
  and blob connections. Its queued and running tasks are cancelled. A task on a revoked worker is
  requeued without the resume hold and without its checkpoint.
- **P2P:** the join flow over an iroh ticket with the minimal test preset.
- **Live:** a machine on another network joins through `--p2p` with an invite sent by chat. The run
  is recorded with its date in the add-a-machine guide.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| ENROLL-1a | Node and master keys; OXS-AUTH v3 (Noise `IK`, cleartext HELLO, `Split()` into the record layer); a member list read at start | `crates/core/src/cluster_auth.rs`, `crates/core/src/noise_auth.rs` (new), `crates/master/src/members.rs` (new), `crates/master/src/server.rs`, `crates/worker/src/client.rs`, `crates/cli/src/connect.rs`, `crates/cli/src/config.rs`, `crates/cli/src/cli_app.rs`, `crates/cli/src/secret_cmd.rs`, `tests/enroll_v3_e2e.rs` (new) | v3 unit tests pass, and a worker whose key is in a hand-written `members.json` runs a task next to a v2 worker | Implemented |
| ENROLL-1b | Invites, `ENROLL`, pending requests, pairing check, approval, `invite`, `members`, `join` with invites, `--auth`, admin enrolment of the master's host, protocol 8 | `crates/core/src/noise_auth.rs`, `crates/core/src/protocol.rs`, `crates/core/src/wire_layout_tests.rs`, `crates/core/tests/data/wire_layout.txt`, `crates/master/src/members.rs`, `crates/master/src/server.rs`, `crates/master/src/session.rs`, `crates/cli/src/members_cmd.rs` (new), `crates/cli/src/join.rs`, `crates/cli/src/secret_cmd.rs`, `crates/cli/src/cli_app.rs`, `tests/cli_enroll_e2e.rs` (new) | `cli_enroll_e2e` passes on Linux, macOS and Windows | Implemented |
| ENROLL-1c | Roles per message, worker-id ownership, revocation across all lanes, submitter of each task | `crates/master/src/members.rs`, `crates/master/src/server.rs`, `crates/master/src/session.rs`, `crates/master/src/queue.rs`, `crates/master/src/store.rs` | `cli_revoke_e2e` and the role tests pass on Linux, macOS and Windows | Planned |
| ENROLL-1d | Dashboard members page with approval, `docs/SECURITY.md`, `README.md`, guide `docs/guides/add-a-machine.md` | `crates/master/src/web_ui.rs`, `dashboard.html`, docs | A dashboard API test lists, approves and revokes; the guide's live run from another network is recorded | Planned |

## Status log

- 2026-11-09: Accepted. The owner chose ecosystem model A (Q13). Not built.
- 2026-11-09: Revised after an independent review, before any code. Changes:
  - admin enrolment of the master's host;
  - roles as a set, with a role per message;
  - worker ids owned by their member;
  - revocation across all lanes, with task cancellation;
  - the joiner confirms the pairing check, and a use is taken at approval, not on request;
  - the HELLO is the Noise prologue, records are encrypted after `Split()`, and limits are global;
  - migration notes, and ENROLL-1b split into 1b and 1c.
- 2026-11-09: `ENROLL-1a` implemented (`crates/core/src/noise_auth.rs`, `crates/master/src/members.rs`, `crates/master/src/server.rs`, `crates/worker/src/client.rs`, `crates/cli`, `tests/enroll_v3_e2e.rs`). Per-machine X25519 keys (`node.key`, `master_noise.key`), OXS-AUTH v3 Noise `IK` (`Noise_IK_25519_ChaChaPoly_BLAKE2s` with exact cleartext `HELLO` bytes as prologue and `Split()` key derivation into `0x04` AEAD records), `members.json` loaded at master startup, `--master-key` (`RUSTY_GRID_MASTER_KEY`, `master_key`), and `master --auth secret|members|both` (`RUSTY_GRID_AUTH`, `[master].auth`).
- 2026-11-09: `ENROLL-1a` security review fixes: enforce v3 `client`/`admin` role on `InboundMessage::Client`, cap the first encrypted v3 record at 64 KiB until decrypted, pass `MasterAuthContext` explicitly without task-local fallback, harden `members.json` (worker-only default roles, `id`/duplicate-key validation, and private file/dir permissions), require explicit `node_key` in `WorkerClient::perform_handshake`, and expand unit/e2e coverage with neutral handshake failure wording.
- 2026-11-10: `ENROLL-1b` implemented (`crates/core/src/noise_auth.rs`, `crates/core/src/protocol.rs`, `crates/master/src/members.rs`, `crates/master/src/server.rs`, `crates/master/src/session.rs`, `crates/cli/src/members_cmd.rs`, `crates/cli/src/join.rs`, `crates/cli/src/secret_cmd.rs`, `crates/cli/src/cli_app.rs`, `tests/cli_enroll_e2e.rs`). Added `oxs1-invite:` invites, `oxideswarm invite` (`--roles`, `--uses`, `--expires`, `--no-approval`, `--list`, `--revoke`), `oxideswarm members` (`list`, `pending`, `approve`, `reject`, `revoke`, `rename`), `ENROLL` over OXS-AUTH v3 (`HandshakePurpose::Enroll`) with session-bound proof (`HMAC-SHA256(invite_secret, "oxs3-enroll" || handshake_hash)`) and 6-digit pairing check derived from `handshake_hash`, live connection-bound pending requests (up to 32, 10-minute expiry, keepalives, use deducted at approval), `oxideswarm join` invite redemption with pairing check and G3 warning confirmation (`[y/N]`) and role-aware post-join checks, auto-enrolment of the master's host `node.key` as `admin`, worker-id ownership per member, and protocol version 8 (`PROTOCOL_VERSION_CURRENT = 8`, `PROTOCOL_VERSION_MIN_COMPATIBLE = 8`).
- 2026-11-10: `ENROLL-1b` round-1 review hardening on the pairing check against forged/MITM invites. Because the Noise `IK` responder chooses its ephemeral key after receiving message 1, a rogue responder that rewrote an invite with its own address and static key could open a single `ENROLL` to the real master, read that session's 20-bit (6-digit) pairing code, and grind ~$10^6$ (~$2^{20}$) responder ephemerals **offline** against the victim's message 1 until the victim's 6-digit code matched. Hardened in three ways: (a) `PendingEnrollmentInfo` omits `pairing_code` and `oxideswarm members pending` human output omits both `pairing_code` and `master_fingerprint` (`--json` includes `master_fingerprint`) so the admin must obtain the 6-digit code and fingerprint from the joining machine; (b) `oxideswarm members approve <ID> --code <NNNNNN> [--fingerprint <XXXX-XXXX-XXXX-XXXX>]` (`ClientMessage::ApproveEnrollment { pending_id, pairing_code }`) requires the admin to enter the 6-digit code shown on the joining machine, verified in constant time on the master before consuming an invite use; and (c) `oxideswarm join` prints the 64-bit master key fingerprint on the same line as the pairing code (`Pairing code: NNNNNN  (Master key fingerprint: xxxx-xxxx-xxxx-xxxx)`), requiring ~$2^{64}$ offline work to collide the 64-bit master key fingerprint when `--fingerprint` (or out-of-band fingerprint verification) is used and ~$2^{20}$ offline ephemeral grinding against message 1 when only the 6-digit pairing code is checked.
- 2026-11-10: `ENROLL-1b` round-2 review fixes (`N1..N13`): LRU eviction of non-live `worker_id` claims past `MAX_WORKERS_PER_MEMBER` (64) (`N1`); require v2 cluster-secret or v3 `admin` authentication on all member/invite administration messages (`N2`); check for a controlling terminal before opening `ENROLL`, confirm `[y/N]` before sending `Enroll` on `--no-approval` invites, and require post-approval `Hello` confirmation (rolling back on decline or disconnect) on approval invites (`N3`); omit `pairing_code` from `PendingEnrollmentInfo` / `members pending` and add `--fingerprint` verification to `members approve` (`N4`); skip `ensure_local_host_admin` when the host key is in `revoked_keys` (`N5`); verify existing `master_noise.key` matches `self.master_noise_key` in `ensure_v3_key_persisted` (`N6`); check and claim `worker_id` ownership atomically (`N7`, `N13`); preserve existing member `name` and `via_invite` on re-enrollment (`N8`); strip Unicode `Cf` format/BiDi characters (`is_control_or_format_char`) (`N9`); and clarify multi-line piped input errors and loopback no-auth documentation (`N10..N12`).
- 2026-11-10: `ENROLL-1b` round-3 review fixes (`1..9` + nits): compare-and-swap post-approval rollback with generation tracking, `revoked_keys` check in `contains`/`get`/`rollback`, 120 s confirmation timeout, and worker session disconnect on rollback (`1`, `5`); revert unnamed `~/.oxideswarm/worker_id` persistence so multiple unnamed workers in one `HOME` keep distinct ephemeral UUIDs (`2`); complete `ClientMessage::Hello` -> `ClientResponse::Hello` before the `--no-approval` `[y/N]` prompt with a 32-waiter cap and 120 s prompt idle timeout (`3`); flush pending TTY input before the `[y/N]` prompt so a multi-line paste (`<invite>\ny\n`) cannot auto-confirm (`4`); hold the `WorkerRegistry` write lock across the `MemberStore` worker-id claim/eviction check, persist `worker_last_seq` and `v2_worker_seq`, allow v2 workers to reclaim non-live squatted worker ids, and prune `owner_member_ids` on `remove_worker`/`prune_stale_workers` (`6`, `7`, `8`); match `--fingerprint` against `<PENDING_ID>` on both client and server; and align `README.md` and design 0051 (`9`).
- 2026-11-11: `ENROLL-1b` round-4 review fixes (`N1..N5` + nits): avoid per-`Register` `members.json` writes by keeping secret-only masters (`--auth secret`) purely in memory, persisting `worker_last_seq` / `v2_worker_seq` only on new ids or LRU evictions outside the `WorkerRegistry` write lock, and logging persistence warnings locally without refusing worker registration or leaking filesystem paths (`N1`, `NIT 4`, `NIT 7`); ignore `v2_worker_seq` under `--auth members` and add `oxideswarm members release-worker <WORKER_ID>` (`ClientMessage::ReleaseWorkerId` -> `ClientResponse::WorkerIdReleased`, `admin` role, refuses live workers) for migrating named workers from v2 to v3 under `--auth both` (`N2`); close and drain `decision_rx` on joiner disconnect/timeout and roll back inline in `approve_pending` when `decision_tx.send` fails (`N3`); prompt `[y/N]` on the pinned master key fingerprint (without a pairing code) before opening any connection on `--no-approval` invites, removing the pre-`Enroll` `Hello` waiter pool (`N4`, `NIT 6`); carry concurrent renames into `UnconfirmedMemberTracker` and restore invite uses only when the key is no longer a member (`N5`); and log rollback persistence errors, distinguish the 120 s confirmation timeout wire message, and document the in-memory rollback tracker and `v2_worker_seq` bounds (`NIT 1..3`, `NIT 5`).
