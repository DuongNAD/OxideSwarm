# 0052: Join on a LAN with a short code

Status: Implemented
Created: 2026-11-09    Updated: 2026-11-14

## Summary

On an admin machine, `oxideswarm invite --code` prints a code such as `7-rocket-violin`. On a new
machine on the same LAN, `oxideswarm join --code 7-rocket-violin` finds the inviting machine. The
two run a password-authenticated key exchange (SPAKE2) with the code, and the agreed key carries an
invite of [design 0051](0051-machine-identity-and-invites.md). Enrolment then goes on as in 0051,
with approval and the pairing check. The code is short enough to read aloud. An attacker gets one
guess per code, and every failed or abandoned exchange cancels the code. Item `JOIN-2`.

## Context

- An invite (0051) is a long base64 string. Pasting it works between machines that share a
  clipboard or a chat app. Typing it is impractical on a fresh machine, at a console, or from
  another screen.
- Magic Wormhole has shown that a short code with SPAKE2 moves a secret safely between two people
  who can talk to each other.
- The master binds UDP port 8089 without address reuse (`crates/master/src/server.rs`,
  `spawn_master_discovery_service`), so pairing cannot share that port.

## Goals

- **G1** Joining a machine on a LAN takes two short commands and no copy and paste.
- **G2** A passive listener learns nothing about the invite. An attacker without the code gets
  exactly one guess at 22 bits per code, and the owner sees that the code was burned.
- **G3** The running master changes nowhere. Pairing runs in the short-lived `invite --code`
  process and ends after one success, one failure or 10 minutes.
- **G4** Approval and the pairing check of 0051 stay.

## Non-goals

- Short codes across networks. A machine on another network joins with the invite string of 0051,
  which may be sent by chat or e-mail. A rendezvous mailbox on the owner's relay may come later, as
  FEAT-7.
- Pairing a phone, which needs a QR code (`JOIN-1b`, after `AND-1`).

## Design

### D1 The code

- The form is `<nameplate>-<word>-<word>`. The nameplate is a random number from 1 to 999. Each word
  comes from a 2048-entry English word list, which gives 22 bits.
- The word list ships in the binary and must carry a licence compatible with `MIT OR Apache-2.0`,
  such as a public-domain list. `crates/cli/src/wordlist.rs` embeds the 2048-word Bitcoin BIP-0039
  English wordlist copied verbatim from `rust-bitcoin/rust-bip39` v3.0.0 (`CC0-1.0` Public Domain
  dedication). USB-1 ([0053](0053-usb-setup-kit.md)) shares `wordlist.rs`.
- A word that is not in the list is caught before any network traffic, and `join` suggests the
  closest word.

### D2 Finding each other

- The inviting process binds the dedicated **UDP port 8087** and an ephemeral TCP port for as long as
  the code is valid. No other component uses port 8087.
- `join --code` sends a broadcast query `{"kind":"oxs-pair-query","v":1,"nameplate":N}` from an
  ephemeral UDP port to port 8087, once a second for at most 30 s. Only an inviter whose
  nameplate matches answers by unicast with `{"kind":"oxs-pair","v":1,"nameplate":N,"port":P}`.
- The joiner therefore needs no inbound firewall rule. Windows admits a unicast reply to a
  broadcast by default. The inviting machine needs inbound UDP 8087 and the TCP port; on Windows
  the first run may raise a firewall prompt, and the guide says so.
- `join --code <CODE> --pair-host HOST:PORT` skips discovery where broadcasts are blocked.

### D3 Exchange

Each step has a 5-second timeout, and the session runs over one TCP connection.

1. The joiner sends its SPAKE2 message (the `spake2` crate, Ed25519 group, password
   `oxs-pair-v1:<w1>-<w2>`, identities `oxs-pair-inviter` and `oxs-pair-joiner`). The inviter answers
   with its own message.
2. **The joiner confirms first:** it sends `HMAC(K, "oxs-pair-confirm-joiner")`. The inviter checks it.
   Only if it matches does the inviter send `HMAC(K, "oxs-pair-confirm-inviter")`.
3. **Burn rule:** a session that has received the inviter's SPAKE2 message ends without a valid
   joiner confirmation in one of three ways: a wrong confirmation, a timeout, or a closed
   connection. Each one **cancels the invite**, and the process exits with "the code was used by
   someone who did not know it; nothing was shared; make a new code". An attacker thus never
   receives anything that tests a guess, and gets one guess per code. A connection that stalls
   before sending its SPAKE2 message holds the single slot for at most 5 seconds.
4. The inviter derives directional keys via HKDF-SHA256 (`salt = "oxs-pair-v1-aead-salt"`,
   info `"oxs-pair-v1-key-i2j"` and `"oxs-pair-v1-key-j2i"`) and sends the invite string encrypted
   with ChaCha20-Poly1305 (`seq = 0`, AAD `"oxs-pair-v1-invite"`). When `--master` was omitted (or
   loopback/unspecified) while the pairing TCP socket used a non-loopback interface, the inviter
   puts that interface's IP with the master port into the invite so the joiner reaches the LAN
   master without extra flags. The joiner then goes on as `join` does with an invite (0051 D4),
   including the pairing check and its own confirmation.
5. Once the master answers `EnrollPending`, the joiner sends an encrypted `PairPendingNotice`
   (`pending_id`, 6-digit `pairing_code`, `master_fingerprint`) back over `oxs-pair-v1-key-j2i`
   (`seq = 1`, AAD `"oxs-pair-v1-pending"`). The inviting process verifies that `pending_id` on the
   master via `ListPendingEnrollments` (0051), checks its `invite_id` and `master_fingerprint`,
   shows `<name> (<os>/<arch>, <address>) wants to join. Pairing check 418 302. Approve? [y/N]`,
   and sends `ApproveEnrollment` (or `RejectEnrollment` and `RevokeInvite`) on confirmation.

### D4 Limits and command line

- Each `invite --code` process runs one pairing, for 10 minutes, with one TCP connection at a time.
  Messages are at most 4 KiB.
- `invite --code` needs the `admin` role (0051 D6).
- The code is a flag value, `join --code <CODE>`, and must match the grammar of D1. `join` keeps the
  rule of design 0048 G3: any other positional or unexpected argument is refused without echoing it,
  so an invite or token pasted onto the command line never appears in the output.

## Compatibility

- No wire change beyond 0051: the inviting process uses the invite, pending and approve messages
  of 0051.
- The pairing protocol carries its version in the `v` field and in the `oxs-pair-v1` password
  prefix.
- Port 8087 (UDP) is new. `README.md` lists it with the other ports, and so does `CLAUDE.md`.

## Security

- **Passive listener:** sees the nameplate, the port and the SPAKE2 messages. SPAKE2 reveals nothing
  about the password to a passive listener.
- **Active attacker:** can make one guess at 22 bits, and the code is dead afterwards (D3 step 3). A
  right guess, 1 in about 4 million, still meets approval and the pairing check of 0051.
- **Forged announcements** only divert the joiner to a host that does not know the code. The joiner
  sends nothing but SPAKE2 messages and its confirmation before the exchange has succeeded, and the
  confirmation reveals nothing without `K`.
- **Denial of service:** anyone on the LAN can burn a code by connecting. The owner then makes a
  new one. This is accepted on a LAN of people the owner knows.

## Test plan

- **Unit:**
  - code grammar and word suggestions;
  - a SPAKE2 round trip;
  - each burn case (wrong confirmation, timeout, closed connection) cancels the invite, and the
    inviter never sends its confirmation first;
  - message limits and per-step timeouts;
  - a token pasted as a positional argument is refused without echo.
- **Integration** (`tests/cli_pair_e2e.rs`, loopback, `--pair-host` instead of broadcast in CI):
  - `invite --code` and `join --code` give an enrolled worker after approval through standard input;
  - a wrong code leaves no invite and writes nothing on the joiner.
- **Live:** a Windows and a macOS machine on one LAN, recorded in the add-a-machine guide.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| JOIN-2a | Word list and code grammar, SPAKE2 exchange with the burn rule, `invite --code`, `join --code`, discovery on UDP 8087 | `crates/cli/src/pair.rs`, `crates/cli/src/wordlist.rs`, `crates/cli/src/members_cmd.rs`, `crates/cli/src/join.rs`, `tests/cli_pair_e2e.rs`, `README.md`, `CLAUDE.md`, `docs/SECURITY.md` | `cli_pair_e2e` passes on Linux, macOS and Windows | Implemented |
| JOIN-2b | Guide section and live record | `docs/guides/add-a-machine.md`, `docs/README.md` | The live LAN run is recorded | Implemented |

## Status log

- 2026-11-09: Accepted with Q13. It starts after ENROLL-1b.
- 2026-11-09: Revised after an independent review, before any code. Changes:
  - the joiner confirms first, and the burn rule covers every unfinished session;
  - per-step timeouts;
  - a dedicated UDP port 8087 with a query from the joiner and a unicast answer, instead of
    port 8089;
  - `join --code`, which keeps the argument rule of 0048;
  - the 0051 pending messages are named.
- 2026-11-14: Implemented `JOIN-2a` and wrote `docs/guides/add-a-machine.md` (`JOIN-2b` stays
  Partial pending the live Windows/macOS LAN run): BIP-0039 `CC0-1.0` wordlist (`wordlist.rs`),
  SPAKE2 + burn rule + UDP 8087 discovery + HKDF/ChaCha20-Poly1305 invite & `PairPendingNotice`
  handoff (`pair.rs`), `invite --code`, `join --code [--pair-host]`, and `tests/cli_pair_e2e.rs`.
- 2026-11-14: Completed `JOIN-2b` and verified `tests/cli_pair_e2e.rs` on Linux, macOS and Windows in CI (run `43590921426`). Recorded the live two-machine LAN run in `docs/guides/add-a-machine.md` (macOS `arm64` master + inviter and Windows 11 `x86_64` joiner at `917c032`: UDP `8087` broadcast discovery with `422-tool-humor`, matching pairing check `711 627`, enrolled member `269b134d8914e3de`, `doctor` `3 passed, 0 warnings, 0 failed, 2 skipped`, and `submit --wait -- echo paired-ok` returning `paired-ok` in `348 ms`).
