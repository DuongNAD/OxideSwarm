# 0005: Cluster authentication v2 for the encrypted record layer

Status: Implemented
Created: 2026-10-26    Updated: 2026-10-26

## Summary

The AEAD record layer on authenticated cluster connections (`0x04`, ChaCha20-Poly1305) already
existed, but the handshake still announced version 1, so peers with and without it could not tell
each other apart. This change:

- bumps the cluster handshake to `AUTH_VERSION = 2` and binds the version into every MAC;
- refuses version 1 peers with a clear message on both sides, with no unencrypted fallback;
- makes the record reader fail closed, keeps a refused oversized frame from using up a nonce, and
  removes sequence numbers from the error texts that callers log.

## Context

Behaviour before this change:

| Area | Behaviour | Code |
|---|---|---|
| Handshake version | HELLO carried version 1 both in builds without the record layer (`055ca3f`, which added the handshake) and in builds with it (from `58c0eb0`). The version byte was not covered by any MAC. | `crates/core/src/cluster_auth.rs` |
| Mixed versions | An old peer and a new peer completed the handshake, then failed on the first frame with a misleading framing error (`Expected encrypted wire format (0x04), received 0x02`, or `Unsupported wire format discriminator: 0x04`). | `crates/core/src/protocol.rs` |
| Error texts | A refused record logged the expected and the received sequence number, which is the nonce. | `AeadReaderSession::decrypt_frame` |
| Oversized frame | `encrypt_frame` consumed a sequence number before it checked the 64 MB limit, so a refused frame desynchronised the direction. | `AeadWriterSession::encrypt_frame` |
| After a refusal | The reader kept its state; only the callers' `break` on a read error ended the connection. | `AeadReaderSession` |
| Module docs | `cluster_auth.rs` still said that nothing used the session key and that TCP was not encrypted. | `crates/core/src/cluster_auth.rs` |
| Tests | No test covered a tampered or injected frame received by a worker, a reflected record, or the counter limit; one tamper test opened records with the wrong key. | `tests/m3_challenger_1_wire_encryption_stress.rs` |

## Goals

- **G1** A peer knows from the handshake whether the other side runs the record layer.
- **G2** A rewritten version byte cannot produce a handshake that succeeds.
- **G3** A version 1 peer is refused with an actionable message on both sides.
- **G4** The first refused record ends the session, whatever the caller does next.
- **G5** Nothing the record layer logs contains a key, nonce, sequence number or plaintext.
- **G6** Tests show that a worker drops a connection that carries a tampered or injected frame.

## Non-goals

- Forward secrecy (an ephemeral key exchange in the handshake).
- Changing the record format, the record keys or the iroh QUIC transport.
- A compatibility mode for version 1 peers.

## Design

### D1 Version 2 handshake

`AUTH_VERSION` is 2. The message flow and frame layout are unchanged. The labels name the
version: `server_mac = HMAC(secret, "oxs2-srv" || client_nonce || server_nonce)`,
`client_mac = HMAC(secret, "oxs2-cli" || server_nonce || client_nonce)` and the session key is
`HMAC(secret, "oxs2-key" || client_nonce || server_nonce)`. A HELLO rewritten to another version on
the way therefore yields a CHALLENGE that the client does not accept. The record keys are still
`HMAC(session_key, "oxs1-aead-c2s")` and `HMAC(session_key, "oxs1-aead-s2c")`; they change with the
session key.

### D2 Version 1 peers

`server_handshake` answers a version 1 HELLO with the generic `REJECT("unsupported auth
version")` after the usual failure delay. The server log says that the peer speaks version 1, which
does not guarantee an encrypted connection, and must be upgraded. A version 2 client that receives this REJECT reports "the
master speaks another cluster authentication version ... run the same OxideSwarm release on the
master and on this node". There is no fallback and no flag for one: `--insecure` keeps its meaning
(it only waives the requirement to have a secret).

### D3 Record layer fixes

- `AeadReaderSession` sets a `failed` flag on the first refused record and refuses every later one
  with `AEAD session closed after an earlier record failure`.
- `AeadWriterSession::encrypt_frame` checks `plaintext + 25 bytes <= MAX_FRAME_SIZE` first and
  advances the counter only after the record is sealed. `u64::MAX` is never used as a sequence
  number.
- Error texts are fixed strings (`AEAD_ERR_*` in `protocol.rs`).
- `ProtocolCapabilities::AEAD_ENCRYPTION` is documented as informational: it travels after the
  handshake and never selects the record layer.

## Compatibility

- Wire: the HELLO version byte and the MAC labels change. The record format does not.
- Mixed versions with a secret do not interoperate: a version 1 peer is refused. A worker that is
  refused retries with the authentication backoff (30 s) until it is upgraded.
- Upgrade order: as for any wire change, restart the master first, then the workers and CLIs
  (`DEPLOYMENT.md` section 3.5).
- Secret-less loopback clusters do not run the handshake and are not affected.

## Security

- No new inputs. The version byte is now bound to the MACs.
- The record layer keeps confidentiality and integrity against an on-path attacker; it does not
  hide frame sizes or timing, cannot prevent the connection from being cut, and has no forward
  secrecy (see `docs/SECURITY.md`).

## Test plan

| Level | Test | What it shows |
|---|---|---|
| Unit | `protocol::tests::test_aead_*`, `test_encrypted_transport_drops_a_tampered_frame_for_good`, `test_encrypted_split_reader_refuses_an_injected_plaintext_assignment` | Every flipped bit is refused and closes the session; replay, reorder, drop, reflection and foreign records are refused; the counter never wraps; an oversized frame uses no nonce; error texts are fixed. |
| Unit | `cluster_auth::tests::v1_hello_is_refused_with_an_upgrade_hint`, `challenge_made_with_version_1_labels_is_not_accepted` | D1 and D2. |
| E2E | `tests/cluster_record_layer_e2e.rs` (`rusty_grid_cli`) | Real master, worker and CLI behind a recording proxy: every frame after the handshake is a record and no payload byte appears on the wire; an injected plaintext `AssignTask` and a record with one flipped bit both make the worker drop the connection, and nothing runs from them. Controls on an open cluster show that the recording and the injection work there. |
| Gate | `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings` | No new failures or warnings. |

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Handshake version 2, version-bound labels, version 1 refusal | `crates/core/src/cluster_auth.rs` | Unit tests pass | Implemented |
| W2 | Fail-closed reader, nonce-safe size check, fixed error texts | `crates/core/src/protocol.rs` | Unit tests pass | Implemented |
| W3 | End-to-end tests through an on-path proxy | `tests/cluster_record_layer_e2e.rs`, `crates/cli/Cargo.toml` | The tests pass | Implemented |
| W4 | Documentation | `docs/SECURITY.md`, `README.md`, `CLAUDE.md`, `DEPLOYMENT.md` | Updated in the same commit | Implemented |

## Status log

- 2026-10-26: Implemented.
