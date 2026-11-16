# Fuzz targets (LV-1)

`cargo fuzz` (libFuzzer) targets for the parsers that read bytes from another machine, the first
two of them before authentication completes. This is item `LV-1` of `docs/BACKLOG.md` and spike R12
of design 0058. The crate is outside the Cargo workspace (`exclude` in the root `Cargo.toml`) and
has a `Cargo.lock` of its own; a normal `cargo build --workspace` does not see it. Every function a
target calls is public, so `rusty_grid_core` has no `fuzzing` feature and a normal build is
unchanged.

| Target | Input | What runs |
|---|---|---|
| `inbound_frame` | a byte stream | A `MessageTransport` over an in-memory stream reads it as `InboundMessage`, once per `WireCodec` (bincode, JSON), with a frame limit of 4096 bytes and at most 64 frames: the length prefix, the frame limit, the format byte dispatch and both decoders |
| `server_handshake` | one mode byte (even: v2 shared-secret handshake, odd: v3 Noise IK handshake), then the byte stream of the peer | `server_handshake` or `v3_server_handshake` against an in-memory stream, with a fixed test secret and a fixed test key. The clock is paused, so the 250 ms failure delay of a rejected peer costs no time |
| `tree_manifest` | the bytes of a manifest | `TreeManifest::decode`; a manifest that decodes must decode again after `encode` |

The in-memory stream and the paused-clock runtime are in `src/lib.rs`.

## Run them

`cargo-fuzz` needs `cargo install cargo-fuzz`. From the repository root:

```bash
cargo fuzz build -s none                                   # builds the three targets
cargo fuzz run inbound_frame -s none -- -max_total_time=60 -max_len=4096
cargo fuzz run server_handshake -s none -- -max_total_time=60 -max_len=4096
cargo fuzz run tree_manifest -s none -- -max_total_time=60 -max_len=4096
```

`-s none` turns the sanitizer off: the code is safe Rust, and it builds with a stable toolchain
(the local runs below used `cargo-fuzz` 0.13.2 with Rust 1.96.0 and no `RUSTC_BOOTSTRAP`). CI pins a
dated nightly instead, because that is what `cargo fuzz` documents (see the `fuzz` job in
`.github/workflows/ci.yml`).

`cargo fuzz run` writes the inputs it finds into `fuzz/corpus/<target>/`, next to the committed
seeds. To keep the seeds clean, pass a work directory first: the first corpus directory receives new
inputs, the others are only read.

```bash
cargo fuzz run tree_manifest -s none /tmp/work/tree_manifest fuzz/corpus/tree_manifest -- -max_total_time=3600
```

## Seeds and regressions

- `corpus/<target>/` holds the seeds: valid encodings, written by
  `cd fuzz && cargo run --example gen_seeds` (framed `WorkerMessage` and `ClientMessage` values in
  both codecs, the HELLO and the first Noise message of a real client, v1 and v2 manifests).
- `regressions/<target>/` holds inputs that must never fail: hand-made malformed frames (a length
  prefix far over the limit, a truncated frame, an unknown format byte, an authentication frame or
  an AEAD frame where a message is expected, a vector length that claims more than the input has),
  and every crash input a run finds, minimised. The CI job runs them first with
  `cargo fuzz run <target> fuzz/regressions/<target> -- -runs=0`.

## CI

The `fuzz` job (Linux) builds the three targets on a pinned nightly (`nightly-2026-11-10`), runs the
inputs of `regressions/`, then fuzzes each target for 60 seconds on pull requests and pushes, and
for 10 minutes on the weekly schedule. A finding fails the job and uploads the crash input as the
artifact `fuzz-artifacts-<target>`: minimise it locally with `cargo fuzz tmin`, and add it to `regressions/`.

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

## Findings

None in the first hour of each target. `regressions/` therefore holds only the hand-made
malformed inputs listed above, which are read without a panic. A longer run (the weekly CI job runs
10 minutes per target) and the open targets below may find more.

## Open targets

Design 0058 LV-1 names more parsers than these three. They are open, in this order:

1. The encrypted record reader (`AeadReaderSession::decrypt_frame`, `MessageReader` with
   encryption) with a fixed session key.
2. The join token and the invite (`oxideswarm join`, `invite.oxs` and the pairing messages of
   `pair.rs`).
3. The journal reader of the master state store (`--state-dir`).
4. The JSON-RPC reader of `oxideswarm mcp`.
5. The v3 handshake after the first frame (`v3_server_confirm_first_record`, the `ENROLL` record),
   which needs a session key from a completed handshake.
