# 0048: Join a machine with one token

Status: Implemented
Created: 2026-11-09    Updated: 2026-11-09

## Summary

`oxideswarm secret join-token` (on the master machine) prints one string that carries the master's address, optionally its P2P ticket, and the cluster secret. `oxideswarm join` (on the new machine) reads that string from standard input, saves the secret in `~/.oxideswarm/cluster.secret`, saves the address in `~/.oxideswarm/rusty-grid.toml`, and runs the `doctor` checks. It replaces the two hand-made steps "copy the secret file at mode 600" and "find out the address or ticket" (`JOIN-1`). The QR code form for the Android app is later work (it needs `AND-1`).

## Context

A master tells the owner to copy `cluster.secret` to every machine by hand (`secret_cmd.rs`, `share_instruction`), and a worker then also needs the master's address or a P2P ticket (`--master`, `--p2p-ticket`). Two values travel over two channels, and a wrong file mode, a truncated copy or a stale ticket shows up only as a refused handshake. Tools users compare a join against ask for a login and nothing else.

## Goals

- **G1** One copy-pasteable string joins a machine; the new machine needs no flag, no environment variable and no second file.
- **G2** The token is printed only when the owner asks (`secret join-token`). It is never logged, never written to a file by OxideSwarm, never put in a config file.
- **G3** `join` reads the token from standard input only. A token given as an argument is refused and not echoed.
- **G4** A token with one changed, lost or added character is refused before anything is written.
- **G5** `join` never replaces a *different* existing secret unless `--replace-secret` is given, and never leaves a half-joined machine: the secret is settled first, the address second.
- **G6** No secret and no token is printed to stdout or stderr, by `join` or by anything it runs. The one deliberate exception is `secret join-token`, whose stdout is the token.

## Non-goals

- A QR code (`JOIN-1` step 4, after `AND-1`).
- Making the token expire or single-use. It carries the long-lived cluster secret, so it is exactly as sensitive as the secret file; rotating the secret (`docs/SECURITY.md` section 5) revokes every token.
- Authenticating the master to the joiner. The token only transports the secret; the handshake that follows proves the master holds it.

## Design

### D1 Token format

```
oxs1-join:<payload>.<tag>
```

- `oxs1-join:` marks the kind and the version (the same `oxs1` family as the other secret-derived labels). A future format gets a new prefix; a decoder refuses a prefix it does not know.
- `<payload>` is base64url without padding (RFC 4648 section 5, alphabet `A-Z a-z 0-9 - _`, so it survives copying through a terminal, a mail client or a chat tool without quoting) of a JSON object `{"m": "<host:port>", "t": "<p2p ticket>", "s": "<secret hex>"}`. `m` and `t` are optional, at least one must be present; `s` is the cluster secret as lowercase hex.
- `<tag>` is 32 hex characters: the first 16 bytes of `HMAC-SHA256(secret, "oxs1-join-token:" || <payload text>)`, computed with `ClusterSecret::derive`.

**Integrity check.** The tag turns "a character changed" (a typo, a line wrap, a lost last character, a stale half of two tokens) into a clear refusal instead of a refused handshake later. The MAC key is the secret that the token itself carries, so the tag is an integrity check against accidents and not authentication against an attacker: anyone who can change a token can also read the secret in it and re-sign. It is a MAC rather than a plain checksum because the machinery exists (`ClusterSecret::derive`, no new dependency), the output is not guessable without the payload, and it binds the address and ticket to the secret, so an address cannot be swapped between two tokens by splicing. Authentication of the transport channel stays the owner's job (SECURITY.md section 8).

Fields are bounded: the address and the ticket are 1 to 2048 characters without whitespace, and `join` reads at most 8 KiB from standard input.

### D2 The master side: `oxideswarm secret join-token`

```
oxideswarm secret join-token --master HOST:PORT [--p2p-ticket T | --p2p-ticket-file PATH]
```

- The secret is the one the machine already uses (`secret show` resolution: flags, environment, config, `~/.oxideswarm/cluster.secret`); without one the command fails and points at `secret generate`.
- The address is `--master` (a bare host gets `:8088`). Without it, the `[master] listen` of the config file is used when other machines could dial it, which excludes a wildcard (`0.0.0.0`, `[::]`) and port 0; otherwise the command fails and says to pass `--master`. The running master is deliberately not asked: a master that listens on `0.0.0.0` cannot know which of its addresses the new machine reaches.
- The ticket is `--p2p-ticket`, the content of `--p2p-ticket-file`, or the content of the config's `[master] p2p_ticket_file` if that file exists.
- stdout is the token and a newline, nothing else. stderr carries one warning that the token is as sensitive as the secret. The token is never logged through `tracing`.

### D3 The new machine: `oxideswarm join`

```
pbpaste | oxideswarm join [--replace-secret] [--replace-master] [--expect-workers N] [--no-doctor]
```

1. **Input.** Piped standard input is read whole (at most 8 KiB). If standard input is a terminal, `join` prompts on stderr (`Paste the join token (it will not be shown):`) and reads one line with echo off (`tcsetattr` without `ECHO` on Unix, `ENABLE_ECHO_INPUT` cleared in the console mode on Windows), restoring the mode afterwards; if echo cannot be turned off it refuses instead of reading with echo. A terminal's line editor limits one line (1024 bytes on macOS), which is enough for a token with an address and for most tickets; a longer token should be piped. A positional argument is refused with a message that does not repeat it (clap would print an unexpected argument back, so the command declares a hidden catch-all argument to take control of the message).
2. **Verify.** The prefix, the base64, the JSON, the hex secret, the tag and the field rules are checked. Nothing has been written yet; error messages describe the fault and never contain the token or the secret.
3. **Secret.** The target is `~/.oxideswarm/cluster.secret` (`OXIDESWARM_HOME` is honoured, like everywhere else). Missing: created through `ClusterSecret::save_file`, which is mode 600 on Unix and a protected owner-only DACL on Windows (`private_file`, design 0047), in a directory that is 700 / owner-only when it is created. Present and equal: left alone. Present and different, or unreadable: refused with the `--replace-secret` hint, nothing changed. With the flag: replaced atomically through a private temporary file.
4. **Address.** `~/.oxideswarm/rusty-grid.toml` gets `master` in `[worker]`, `[submit]` and `[status]`, so `worker`, `submit`, `status`, `workers`, `cancel`, `result`, `doctor` and `mapreduce` need no `--master`. A ticket goes to `[worker] p2p_ticket`; a token without a ticket removes an existing `p2p_ticket`, because the worker prefers a ticket from the config file over its address and a ticket of a previous master would win. The file is edited in place with `toml_edit`: only these keys change, and comments, order and formatting are kept. Before anything is written, a symbolic link is refused, a file that is not valid TOML is refused, and a key that already holds a *different* master or ticket (or a ticket that would be removed) is a conflict: `join` stops, with nothing written (not even the secret), unless `--replace-master` is given. When the file changes, a private backup (`rusty-grid.toml.bak-<unix seconds>`, mode 600 or the owner-only DACL, never overwriting an older backup) is written first, the new text goes through a temporary file and a rename that keeps the file's permissions, and stderr lists exactly which keys changed (`[worker].master`, ...; never values). No secret is ever written to this file. A `./rusty-grid.toml`, `RUSTY_GRID_CONFIG` or `--config` still takes precedence, as for any setting (`config.rs`).
5. **Doctor.** Unless `--no-doctor` or the token has no address, the `doctor` checks run against the master with the token's secret, and `join` exits with doctor's status (0 passed, 1 a check failed, 6 authentication failed). `--expect-workers` defaults to 0, not doctor's 1: the usual next step is to start the worker on this machine, so "no worker yet" is not a failed join. A token with only a P2P ticket cannot be checked (`doctor` connects to a TCP address); `join` says so and exits 0.
6. **Output.** `join` prints what it did (paths, the master address) and the doctor report. It prints neither the token nor the secret.

### D4 Behaviour on repeated use

Joining twice with the same token changes nothing and passes. Joining with a token of another cluster fails at step 3 until `--replace-secret` is given.

## Compatibility

No wire or protocol change. `secret` gains a subcommand and the CLI gains `join`; `base64` moves from a dev dependency to a dependency of `rusty_grid_cli`, and `toml_edit` (already in `Cargo.lock` through `toml`), `libc` (Unix) and `windows-sys` with `Win32_System_Console` (Windows; already in the tree) become direct dependencies. No new package enters the lock file except the `toml_writer` edge of `toml_edit`. Existing setups (a secret copied by hand) keep working and are not touched.

## Security

- The token **is** the cluster secret in another form. Whoever reads it can join the cluster and drive it (run commands on every worker). It must travel over a channel the owner trusts and must not be pasted into chat, e-mail, tickets or logs; `docs/SECURITY.md` lists it with the other secrets.
- A token printed to a terminal stays in the scrollback of that terminal, and a token piped through a clipboard stays in the clipboard. `join` therefore accepts it only on standard input (not in shell history, not in the process list), and on a terminal reads it with echo off, so a pasted token is not shown (and is not left in the scrollback).
- Neither command logs the token, formats it into an error, or derives `Debug` output from it (`JoinToken` redacts the secret and the ticket). Tasks that the new machine's worker runs cannot read the token: it is never stored, only the secret file, which is already protected (SECURITY.md section 7).
- The address in `rusty-grid.toml` and the ticket are not credentials (the ticket is an address; the secret decides who may join).
- Replacing a secret is explicit (`--replace-secret`), because a join with the wrong token would otherwise silently cut a machine off from the cluster it uses.

## Test plan

- Unit tests (`crates/cli/src/join.rs`): round trip; every single-character change of the token is refused, as are truncation and extension; a token without a target, with a malformed address, and with only a ticket; errors and `Debug` never show the token or the secret; the stdin reader trims and bounds; secret creation, equal, different, replace; the config plan keeps comments and order, reports a different master or ticket as a conflict, and refuses a file that is not TOML or a symbolic link; the backup is private and never overwrites an older one; the echo-off helper clears and restores `ECHO` on a pseudo-terminal and fails on a descriptor that is not a terminal (Unix; the Windows console-mode branch is compiled only on Windows and is untested).
- Integration test `tests/cli_join_e2e.rs` (`cli_join_e2e`, real binaries, two temporary `HOME`/`USERPROFILE` directories passed with `Command::env`, loopback master): create the token on the master side, join with it on standard input, assert the secret file is mode 600 (Windows: protected owner-only DACL through `private_file::read_dacl`) and equals the master's, start a worker with no flags and see `doctor` pass; a token with one changed byte (payload and tag) is refused with nothing written; a token as an argument is refused and not echoed; an existing config with comments and a different master is refused without `--replace-master`, and with it keeps the comments, lists the changed keys, writes one private backup; a symbolic-link config is refused; a different existing secret is refused without `--replace-secret` and replaced with it; the generated secrets and the token appear in no stdout or stderr and no log of any process.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| JOIN-1 | `JoinToken`, `secret join-token`, `join` | `crates/cli/src/join.rs`, `secret_cmd.rs`, `cli_app.rs` | `cli_join_e2e` passes | Implemented |
| JOIN-1b | QR code form for the Android app | `packaging/android` | after `AND-1` | Planned |

## Status log

- 2026-11-09: implemented `JOIN-1` steps 1, 2, 3 and 5 (steps: design record, token, join, docs). Step 4 (QR code) waits for `AND-1`.
- 2026-11-09: review changes: in-place config edit with backup, conflict flag `--replace-master`, symbolic-link refusal, and a no-echo prompt on a terminal.
