# Fuzz targets (LV-1, LV-18)

`cargo fuzz` (libFuzzer) targets for the parsers that read bytes from another machine, another
person or a file. LV-1 (`docs/BACKLOG.md`, spike R12 of design 0058) added the first three, which
read what a peer sends before authentication completes; LV-18 added three more for the parsers
design 0058 LV-1 lists as later targets. The crate is outside the Cargo workspace (`exclude` in the
root `Cargo.toml`) and has a `Cargo.lock` of its own; a normal `cargo build --workspace` does not
see it.

The targets call public functions, with one exception: the readers of the master's task store are
private, so `rusty_grid_master` has a Cargo feature `fuzzing`, off by default, that compiles
`store::fuzzing` (the journal and snapshot readers on bytes in memory, and the encoders the store
writes with). Only this crate turns it on; a normal build does not compile it. `rusty_grid_core`
and `rusty_grid_cli` have no `fuzzing` feature.

| Target | Input | What runs |
|---|---|---|
| `inbound_frame` | a byte stream | A `MessageTransport` over an in-memory stream reads it as `InboundMessage`, once per `WireCodec` (bincode, JSON), with a frame limit of 4096 bytes and at most 64 frames: the length prefix, the frame limit, the format byte dispatch and both decoders |
| `server_handshake` | one mode byte (even: v2 shared-secret handshake, odd: v3 Noise IK handshake), then the byte stream of the peer | `server_handshake` or `v3_server_handshake` against an in-memory stream, with a fixed test secret and a fixed test key. The clock is paused, so the 250 ms failure delay of a rejected peer costs no time |
| `tree_manifest` | the bytes of a manifest | `TreeManifest::decode`; a manifest that decodes must decode again after `encode` |
| `aead_record` | one mode byte, then (even) the byte stream of a peer without the key, or (odd) chunks `[len: u16 LE][op][plaintext]` | The encrypted record layer (format byte `0x04`) with record keys derived from a fixed test session key. Even: a server `MessageTransport` with encryption reads the stream as `InboundMessage` (frame limit 4096, at most 64 frames). Odd: the client side seals each plaintext, `op` keeps, damages, drops, replays or cuts the record, and `AeadReaderSession::decrypt_frame` must accept exactly the records that arrive intact and in order before the first bad one, return each plaintext unchanged, and refuse everything after the first bad one; the same records then go through the `MessageReader` of a split transport, which decodes them |
| `join_invite` | one mode byte (modulo 4), then the rest | 0: the text piped into `oxideswarm join` (`read_token`, then `InviteToken::decode` or `JoinToken::decode` by prefix). 1: the JSON payload of a join token; the target adds the base64url encoding and the integrity tag made with the secret the payload names. 2: the JSON payload of an invite; the target adds its checksum. 3: an `invite.oxs` file: `validate_invite_oxs_header`, then `decrypt_invite_oxs` with a fixed test passphrase when the header asks for at most 256 KiB and two passes of Argon2id. A token or invite that decodes must decode to the same value after `encode` |
| `store_journal` | one mode byte, then the rest | The readers of `master --state-dir` through `store::fuzzing`. Bit 0 picks `journal.bin` or `snapshot.bin`; bit 1 picks the whole file, or only what the frames carry, with the target writing the header, lengths and BLAKE3 hashes so that the bincode and JSON decoders behind the hashes are reached; bits 2 and 3 place the snapshot's generation against the journal's. The valid prefix a journal read reports must read again to the same records with no torn tail, accepted records written again by the store's encoder must read back equal, and so must an accepted snapshot |

The in-memory stream, the paused-clock runtime, the chunk splitter and the fixed test secret, keys
and passphrase are in `src/lib.rs`. None of them is a real secret.

## Run them

`cargo-fuzz` needs `cargo install cargo-fuzz`. From the repository root:

```bash
cd fuzz
cargo fuzz build -s none                                   # builds the six targets
cargo fuzz run inbound_frame -s none -- -max_total_time=60 -max_len=4096
cargo fuzz run aead_record -s none -- -max_total_time=60 -max_len=4096
```

`-s none` turns the sanitizer off: the code is safe Rust, and it builds with a stable toolchain
(the local runs below used `cargo-fuzz` 0.13.2 with Rust 1.96.0 and no `RUSTC_BOOTSTRAP`). CI pins a
dated nightly instead, because that is what `cargo fuzz` documents (see the `fuzz` job in
`.github/workflows/ci.yml`).

On macOS the build of `iroh-relay` (a dependency of `rusty_grid_cli` and of `rusty_grid_master`
with its default features) fails since LV-18: it also declares a `cdylib`, and linking that
`cdylib` with the coverage instrumentation of `cargo fuzz` stops on undefined symbols of `core`
and `std`. The fuzz targets do not use that `cdylib`. Let the macOS linker accept it, with the
same flag for every `cargo fuzz` command (a different `RUSTFLAGS` rebuilds everything):

```bash
export RUSTFLAGS="-Clink-arg=-Wl,-undefined,dynamic_lookup"   # macOS only
```

The six target binaries built this way have no undefined Rust symbols (`nm -u`). Linux links
shared libraries with undefined symbols by default and needs no flag.

`cargo fuzz run` writes the inputs it finds into `fuzz/corpus/<target>/`, next to the committed
seeds. To keep the seeds clean, pass a work directory first: the first corpus directory receives new
inputs, the others are only read.

```bash
cargo fuzz run tree_manifest -s none /tmp/work/tree_manifest corpus/tree_manifest -- -max_total_time=3600
```

## Seeds and regressions

- `corpus/<target>/` holds the seeds: valid encodings, written by
  `cd fuzz && cargo run --example gen_seeds` (every target) or
  `cargo run --example gen_seeds -- <target>...` (only those named): framed `WorkerMessage` and
  `ClientMessage` values in both codecs, the HELLO and the first Noise message of a real client, v1
  and v2 manifests, records sealed with the test session key (as a stream, and as chunks for each
  `op`), a join token with an address and one with a ticket, an invite, their JSON payloads, an
  `invite.oxs` at the default cost and one at a cheap cost made with the test passphrase, a journal
  with one record of each kind (whole, torn, header only, stale, and as payloads only), and
  snapshots (whole, empty, and as JSON only).
- `regressions/<target>/` holds inputs that must never fail: hand-made malformed inputs, and every
  crash input a run finds, minimised. The CI job runs them first with
  `cargo fuzz run <target> fuzz/regressions/<target> -- -runs=0`. For the first three targets:
  frames with a length prefix far over the limit, a truncated frame, an unknown format byte, an
  authentication frame or an AEAD frame where a message is expected, a vector length that claims
  more than the input has. For `aead_record`: a record too short to hold a tag, a plain frame where
  a record is expected, a record with a changed tag, the second record first, a replayed and a cut
  record. For `join_invite`: a bare prefix, a token without a tag or with bad base64, an invite with
  a short checksum or two lines, payloads with a short secret, a master address with a space, no
  roles, an unknown role or a malformed id, and `invite.oxs` files that are truncated, have another
  magic, a cost out of bounds, more Argon2id lanes than the memory allows, or a changed tag. For
  `store_journal`: empty, short, all-zero and foreign headers, a store format of 99, records of an
  older protocol, a damaged first frame with valid frames after it (refused), a damaged last length
  check (cut as a torn tail), a frame over 64 MiB, a journal newer than its snapshot, payloads that
  are not records or claim `u64::MAX` entries, and snapshots with a wrong length, a length over the
  bound, a changed checksum, a cut frame header, or JSON that is not a snapshot.

## CI

The `fuzz` job (Linux) builds the six targets on a pinned nightly (`nightly-2026-11-10`), runs the
inputs of `regressions/`, then fuzzes each target for 60 seconds on pull requests and pushes, and
for 10 minutes on the weekly schedule. A finding fails the job and uploads the crash input as the
artifact `fuzz-artifacts-<target>`: minimise it locally with `cargo fuzz tmin`, and add it to
`regressions/`. Since LV-18 the crate also builds `rusty_grid_master` and `rusty_grid_cli`, iroh
included, so the job's timeout is 40 minutes.

## One-hour record

Spike R12 of design 0058: each target for one hour, the three in parallel, `-jobs` and `-workers`
1 each, seeded with `corpus/<target>/`, `-max_len=4096`.

Date: 2026-11-17, 00:15 to 01:15 (UTC+7), `master` at `5d5b9df` plus this change. Machine: macOS
arm64 (Apple M5, 10 cores, on AC power); the three runs shared it. Tool: `cargo-fuzz` 0.13.2,
Rust 1.96.0 stable, `-s none`, no `RUSTC_BOOTSTRAP`. Command per target:
`cargo fuzz run <target> -s none <work dir> fuzz/corpus/<target> -- -max_total_time=3600 -max_len=4096`.

| Target | Executions | Per second | Coverage (`cov`, edges) | Features (`ft`) | Corpus at the end | Peak RSS | Crashes |
|---|---|---|---|---|---|---|---|
| `inbound_frame` | 24,456,793 | 6,791 | 3,401 | 8,052 | 2,424 inputs, 347 KB | 41 MB | 0 |
| `server_handshake` | 7,664,331 | 2,128 | 1,084 | 1,539 | 64 inputs, 2,276 B | 34 MB | 0 |
| `tree_manifest` | 34,622,030 | 9,614 | 665 | 1,711 | 368 inputs, 55 KB | 35 MB | 0 |

No crash, panic, timeout or out-of-memory in any run. `server_handshake` is the slowest because
every accepted HELLO runs a Noise `IK` step (X25519). Its coverage stops growing early: the proof
of the v2 handshake and the second Noise message need a MAC or key the fuzzer cannot guess
(by design: that is the authentication), so the bytes after the HELLO of an unauthenticated peer
are what these runs cover.

## Five-minute record (LV-18)

These are 5-minute runs, not one-hour runs: each new target for 300 seconds, the three in
parallel, `-jobs` and `-workers` 1 each, seeded with `corpus/<target>/`, `-max_len=4096`.

Date: 2026-11-18, 12:38:10 to 12:43:14 (UTC+7), `master` at `b486137` plus this change. Machine:
macOS 27.2.1 arm64 (Apple M5, 10 cores, 32 GB, on AC power); the three runs shared it with other
builds and tests (load average 8 to 21 when they started), so the rates are lower than an idle
machine gives. Tool: `cargo-fuzz` 0.13.2, Rust 1.96.0 stable, `-s none`, no `RUSTC_BOOTSTRAP`, with
the macOS `RUSTFLAGS` above. Command per target, from `fuzz/`:
`cargo fuzz run <target> -s none <work dir> corpus/<target> -- -max_total_time=300 -max_len=4096`.

| Target | Executions | Per second | Coverage (`cov`, edges) | Features (`ft`) | Corpus at the end | Peak RSS | Crashes |
|---|---|---|---|---|---|---|---|
| `aead_record` | 967,987 | 3,215 | 2,770 | 6,234 | 1,466 inputs, 159 KB | 49 MB | 0 |
| `join_invite` | 682,365 | 2,266 | 2,140 | 5,776 | 1,859 inputs, 110 KB | 51 MB | 0 |
| `store_journal` | 910,478 | 3,024 | 3,785 | 7,598 | 2,207 inputs, 596 KB | 51 MB | 0 |

No crash, panic, failed assertion, timeout or out-of-memory in any run, and no file in
`artifacts/`. The round-trip and record-layer assertions held for every input.

## Findings

None in the first hour of each LV-1 target, and none in the five minutes of each LV-18 target.
`regressions/` therefore holds only the hand-made malformed inputs listed above, which are read
without a panic. A longer run (the weekly CI job runs 10 minutes per target) and the open targets
below may find more.

## Open targets

Design 0058 LV-1 names more parsers than these six. They are open:

1. The JSON-RPC reader of `oxideswarm mcp`. The line reader and the request checks (JSON, object,
   `id`, `method`, `jsonrpc`, `params`) are inline in `run_stdio_server` in
   `crates/cli/src/mcp.rs`, which reads the process's standard input, so no public function takes
   one line. A target needs one, for example `pub fn parse_request_line(line: &str) ->
   Result<JsonRpcRequest, JsonRpcResponse>` cut out of that loop (and a dispatch of the methods
   that need no master: `initialize`, `ping`, `tools/list`). It waits for MAINT-2, which is
   splitting `mcp.rs` into submodules. `validate_tool_arguments`, the check of `tools/call`
   arguments, is public already.
2. The pairing exchange of `pair.rs` (`invite --code` and `join --code`): the UDP discovery
   messages and the SPAKE2 exchange on the TCP stream (`run_inviter_session_on_stream`,
   `run_joiner_exchange_on_stream`) are private.
3. The v3 handshake after the first frame (`v3_server_confirm_first_record`, the `ENROLL` record),
   which needs a session key from a completed handshake.
