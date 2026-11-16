# 0080: Fail CI when a Rust file's production part exceeds 3,000 lines (`MAINT-1` step 3)

Status: Implemented
Created: 2026-11-16    Updated: 2026-11-16

## Summary

`ox-mode test size` measures every Rust source file and fails when the production part of a file is longer than 3,000 lines, except for the files in `scripts/rust-file-size-allowlist.txt`, whose recorded maximum may only shrink. `ox-mode test lint` runs it as its third part, and the Linux `clippy` job of CI runs it on every pull request.

## Context

`MAINT-1` split the six largest files (`queue.rs`, `server.rs`, `client.rs`, `runner.rs`, `protocol.rs`, `cli_app.rs`) because files of 4,000 to 8,000 lines made review slow and pull requests collide. Nothing stopped a file from growing back. Measured at `03ace60`, three other files already have a production part above 3,000 lines: `crates/worker/src/isolation.rs` (5,343), `crates/cli/src/mcp.rs` (3,692) and `crates/cli/src/mode_cli.rs` (3,239).

## Goals

- **G1** One implementation, in Rust, used by `ox-mode` and by CI.
- **G2** A new file or a growing file cannot cross 3,000 production lines without a failing check.
- **G3** The existing large files are recorded, not hidden, and the record can only go down.

## Non-goals

- Splitting `isolation.rs`, `mcp.rs` or `mode_cli.rs` (separate work).
- Limiting test code: only the production part counts.

## Design

### D1 What is measured

`crates/cli/src/size_check.rs` walks the workspace root and measures each `*.rs` file (skipping `target/`, `node_modules/` and hidden directories such as `.git/` and `.claude/`). The production part is the number of lines before the first line that starts, after indentation, with `#[cfg(test)]`; a file without one counts whole. Files that hold only tests are not measured: anything under a `tests/` directory, `tests.rs` and `*_tests.rs` (the test modules that `MAINT-1` moved into files of their own).

### D2 The allowlist

`scripts/rust-file-size-allowlist.txt` holds one `<path> <max_lines>` per line, `#` comments, and a header that states the rule. The maximum is the current production line count. A malformed line, a duplicate, or a maximum not above 3,000 makes the check fail. Entries at `03ace60` plus this change:

| File | Maximum |
|---|---|
| `crates/worker/src/isolation.rs` | 5,377 (5,343 at `03ace60`, grown by SEC-12) |
| `crates/cli/src/mcp.rs` | 3,692 |
| `crates/cli/src/mode_cli.rs` | 3,242 (3,239 plus the `size` dispatch and lint hook) |

### D3 Verdicts

- An unlisted file above 3,000 lines fails.
- A listed file above its maximum fails: the allowlist may only shrink, and adding an entry for a new file is not the way out.
- A listed file below its maximum, a listed file at or below 3,000 lines, or a listed path that no longer exists produces a warning, not a failure, naming the line to lower or delete. A warning does not fail so that a pull request that happens to shrink a file never turns a sibling pull request red; the next change to the allowlist cleans it up.

### D4 Where it runs

- `ox-mode test size` (new action; `size` is listed in `WorkflowMode::available_actions` and the descriptors) prints the files above 2,500 lines, the verdicts, and exits 1 on a failure.
- `ox-mode test lint` appends the same result as part 3 and fails when it fails. The size check needs no build of the workspace, so it is fast.
- CI: one step in the Linux leg of the `clippy` job, `cargo run --locked -p rusty_grid_cli --bin ox-mode -- test size`. No job is added and no required check name changes.
- A unit test (`size_check::tests::workspace_allowlist_holds_for_this_checkout`) runs the check against the checkout, so `cargo test --workspace --lib` also catches a violation.

### D5 Known limit

The production part ends at the first `#[cfg(test)]` line anywhere in the file, so a `#[cfg(test)]` attribute on an item or statement inside production code cuts the count early (for example `#[cfg(test)] let ...` lines in a function make the file measure shorter than it is). A later change can count up to the first `#[cfg(test)]` that is followed by `mod`.

## Security

No network, no process spawn, read-only access to the working tree.

## Compatibility

No change to the grid, the wire format or any existing `ox-mode` action. `ox-mode test` has one more action, `size`.

## Test plan

- Unit tests in `crates/cli/src/size_check.rs`: first-`#[cfg(test)]` rule, test-only file names, allowlist parsing errors, the exact 3,000 / 3,001 boundary, growth fails, shrink warns, stale entry warns, and the real checkout passes.
- Manual proof that it fails: with `mcp.rs` set to 3,600 in the allowlist, and with a scratch file of 3,001 lines, `ox-mode test size` exits 1 (recorded in the pull request).

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| MAINT-1 | Size check in `ox-mode test lint` and CI | `crates/cli/src/size_check.rs`, `crates/cli/src/mode_cli.rs`, `crates/core/src/mode.rs`, `scripts/rust-file-size-allowlist.txt`, `.github/workflows/ci.yml` | The check runs on every pull request and fails above 3,000 production lines | Implemented |

## Status log

- 2026-11-16: Implemented. Allowlist: `isolation.rs` 5,377, `mcp.rs` 3,692, `mode_cli.rs` 3,242.
