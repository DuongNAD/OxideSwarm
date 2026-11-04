# 0003: Distributed file staging and output artifact collection

Status: Implemented
Created: 2026-10-23    Updated: 2026-11-04

## Summary

Distribute input files to worker execution sandboxes and collect output artifacts:

- stage local files into worker task sandboxes via `submit --file <local>[:<sandbox>]`;
- declare expected output files and directories via `submit --output <path>` and `submit --output-dir <dir>`;
- define `StagedFile` and `OutputArtifact` data models with SHA-256 content verification;
- write staged files directly into each worker task sandbox (`nlink == 1`) so in-place edits by one task never affect subsequent tasks;
- isolate sandboxes against directory traversal (`..`), leading slashes, Windows DOS device names, and symlink escapes;
- account for file staging payloads within the master queue 256 MiB oldest-first retention budget.

## Context

Behaviour before this change:

| Area | Behaviour | Code |
|---|---|---|
| Input Files | Tasks could only run binaries and read files already present on the worker host filesystem. The CLI `submit` command had no option to send files alongside a task. | `crates/cli/src/cli_app.rs` |
| Output Files | Tasks captured only `stdout` and `stderr` streams up to 10 MiB. Output files generated inside worker sandboxes were deleted upon task completion without retrieval. | `crates/worker/src/runner.rs` |
| Wire Protocol | `TaskSpec` carried command arguments, script strings, and environment variables. The wire protocol lacked data structures for binary file payloads and artifact envelopes. | `crates/core/src/task.rs`; `crates/core/src/protocol.rs` |
| Caching | No content-addressed object store existed on workers; re-running tasks with identical inputs required re-transmitting all data across the network. | `crates/worker/src/sandbox.rs` |
| Queue Budget | The master queue counted only task descriptor struct sizes against its memory budget, ignoring attached payloads. | `crates/master/src/queue.rs` |

## Goals

- **G1** Stage input files into worker task sandboxes with customizable destination relative paths.
- **G2** Collect declared output files and directories from worker sandboxes back to the client.
- **G3** Verify staged file payloads by SHA-256 digest and write an independent copy (`nlink == 1`) into each task sandbox.
- **G4** Enforce strict sandbox isolation, rejecting traversal components, root paths, reserved Windows device names, and symlinks resolving outside the sandbox.
- **G5** Maintain wire compatibility across JSON (`0x01`), Bincode (`0x02`), and AEAD (`0x04`) codecs via protocol capability negotiation.
- **G6** Bound master queue memory at 256 MiB for active tasks (`--max-active-mb` / `RUSTY_GRID_MAX_ACTIVE_MB`) and 256 MiB for retained finished tasks (`--max-retained-mb` / `RUSTY_GRID_MAX_RETAINED_MB`, using oldest-first eviction).

## Non-goals

- Implementing a distributed POSIX file system or network filesystem mount.
- Streaming large gigabyte-scale datasets; individual tasks are bounded by frame limits (64 MiB max frame size).
- Bidirectional real-time file synchronisation during active process execution.

## Design

### D1 Distributed file staging and artifact collection CLI syntax

The CLI `submit` subcommand adds options to declare input and output files:

| Option | Argument | Description |
|---|---|---|
| `--file` | `<LOCAL_PATH>[:<SANDBOX_PATH>]` | Stage a local file into the worker sandbox (repeatable). If `<SANDBOX_PATH>` is omitted, the local file name is used. |
| `--output` | `<SANDBOX_PATH>` | Declare a file artifact to collect upon completion (repeatable). |
| `--output-dir` | `<LOCAL_DIR>` | Local directory where collected output artifacts are written upon task completion. |

Example:

On the client machine:
```bash
oxideswarm submit --file input.csv:data/input.csv --output results.json --output-dir ./out -- python3 process.py
```

Implemented in `crates/cli/src/cli_app.rs` (`SubmitArgs`).

### D2 Data models: `StagedFile` and `OutputArtifact`

`crates/core/src/task.rs` introduces two serializable structures:

```rust
pub struct StagedFile {
    pub path: String,
    pub sha256: [u8; 32],
    pub size: u64,
    pub executable: bool,
    pub data: Vec<u8>,
}

pub struct OutputArtifact {
    pub path: String,
    pub sha256: [u8; 32],
    pub size: u64,
    pub data: Vec<u8>,
}
```

- `path`: Normalized relative destination path inside the sandbox directory.
- `sha256`: Cryptographic digest computed over `data`.
- `executable`: Unix executable permission bit (`0o755` vs `0o644`).
- `data`: Raw file content payload.

Tasks carry `staged_files: Vec<StagedFile>` and `expected_outputs: Vec<String>`. Completed task outcomes return `artifacts: Vec<OutputArtifact>`.

### D3 Worker staged file placement

Inline `StagedFile` payloads arrive in memory inside `Task::staged_files`. Each task sandbox receives its own independent file (`nlink == 1`):

```text
<sandbox_base_dir>/
└── tasks/
    └── task-<uuid>/
        ├── data/
        │   └── input.csv (independent copy written from StagedFile::data)
        └── results.json
```

Placement flow:
1. When receiving a `StagedFile`, the worker verifies that `sha256(data) == staged_file.sha256`.
2. Creates the destination directory tree inside the task sandbox.
3. Writes `staged.data` directly to the sandbox target path (`nlink == 1`), avoiding shared hard-linked inodes in `cache/objects` that an in-place edit or `chmod` could corrupt for later tasks.
4. Sets Unix file permissions (`0o755` if `executable`, otherwise `0o644`).

Implemented in `crates/worker/src/sandbox.rs` (`Sandbox::stage_files`).

### D4 Strict sandbox path sanitization and isolation

To prevent directory traversal and host compromises, paths are validated by `validate_relative_sandbox_path` (`crates/core/src/task.rs`) and `normalize_relative_path_str` (`crates/worker/src/sandbox.rs`):

1. **Rejection of absolute paths:** Rejects leading slashes (`/`), backslashes (`\`), and drive letters (`C:`).
2. **Rejection of traversal tokens:** Strips current-directory markers (`.`). Rejects parent traversal (`..`) and multi-dot evasion patterns (`...`, `....//`).
3. **Rejection of reserved DOS device names:** Rejects Windows reserved device stems (`CON`, `PRN`, `AUX`, `NUL`, `COM1`–`COM9`, `LPT1`–`LPT9`), including pre-dot and post-dot whitespace evasion attempts (`nul .txt`, `aux .log`).
4. **Symlink escape prevention:** Rejects paths pointing to symlinks whose targets resolve outside the sandbox boundary.

### D5 Wire format and capability negotiation

- Protocol capability flag `ProtocolCapabilities::FILE_STAGING = 1 << 6` is exchanged during handshake.
- Supported across all codecs:
  - JSON (`0x01`): Base64-encoded file byte streams in JSON frames.
  - Bincode (`0x02`): Compact length-prefixed binary byte arrays.
  - AEAD (`0x04`): ChaCha20-Poly1305 stream-encrypted frames.
- Payloads exceeding `MAX_FRAME_SIZE` (64 MiB) or cumulative task input limits (32 MiB) are refused before transmission.

Implemented in `crates/core/src/protocol.rs` and `crates/core/src/task.rs`.

### D6 Master queue memory accounting and oldest-first retention budget

The master queue tracks active and retained payload memory in `crates/master/src/queue.rs` (see also `docs/design/0008-oversized-task-results-and-active-queue-budget.md`):
- Active (non-terminal) task memory defaults to 256 MiB (`DEFAULT_MAX_ACTIVE_BYTES`), configurable via `--max-active-mb`, `RUSTY_GRID_MAX_ACTIVE_MB`, or `max_active_mb` in `[master]`. Submissions and checkpoint increases that would exceed this cap are refused with `GridError::QueueByteLimitExceeded`.
- Retained finished-task memory defaults to 256 MiB (`DEFAULT_MAX_TERMINAL_BYTES`), configurable via `--max-retained-mb`, `RUSTY_GRID_MAX_RETAINED_MB`, or `max_retained_mb` in `[master]`.
- Each task entry accounts for the size of its stored `TaskSpec` (including `StagedFile` bytes), checkpoint, and execution `TaskResult` (including `OutputArtifact` and `asset_bytes`).
- When retained terminal memory exceeds `max_terminal_bytes`, finished tasks are evicted oldest-first until memory falls within budget (always keeping at least the newest finished task).

## Compatibility

- Workers advertising `ProtocolCapabilities::FILE_STAGING` receive tasks with staged files.
- `TaskView::required_features` (`crates/master/src/queue.rs`) requires `ProtocolCapabilities::FILE_STAGING` whenever `task.staged_files` or `task.expected_outputs` is non-empty, and `WorkloadScheduler` (`crates/master/src/scheduler.rs`) skips workers lacking `required_features`, leaving the task queued with `ScheduleSkipReason::ClusterLacksCapability(String)` (`crates/master/src/scheduler.rs:203-208`) until a capable worker connects.
- Existing tasks without staged files incur zero wire overhead (empty vectors serialize to single byte prefixes).

## Security

- Sanitization prevents reading or writing outside the task sandbox directory.
- SHA-256 digest validation guarantees that tampered or corrupted file payloads are rejected prior to execution.
- File staging operates entirely within the mutual HMAC-SHA256 authenticated and encrypted stream (`0x04`).

## Test plan

- **Unit:** path normalization, traversal rejection, DOS device name evasion, SHA-256 digest validation, independent per-sandbox file placement (`nlink == 1`), and legacy staging cache removal at worker start-up (`crates/core`, `crates/worker`).
- **Stress:** adversarial path traversal test suites, whitespace evasion generators, and combinatorial fuzzing (`tests/m5b_challenger_1_stress.rs`, `tests/m5b_challenger_2_stress.rs`).
- **Integration:** end-to-end file staging, execution, output artifact collection, and oldest-first queue eviction over JSON and Bincode wire codecs (`tests/file_staging_e2e.rs`).
- **Gates:** `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Staging data models and sanitization | `crates/core/src/task.rs` | `StagedFile`, `OutputArtifact`, and `validate_relative_sandbox_path` implemented | Implemented |
| W2 | Worker sandbox staging | `crates/worker/src/sandbox.rs` | SHA-256 verification, independent per-sandbox placement (`nlink == 1`), and artifact collection | Implemented |
| W3 | CLI staging options | `crates/cli/src/cli_app.rs` | `--file`, `--output`, `--output-dir` parsing and payload building | Implemented |
| W4 | Master queue retention budget | `crates/master/src/queue.rs` | 256 MiB oldest-first retention budget and artifact eviction | Implemented |
| W5 | End-to-end integration tests | `tests/file_staging_e2e.rs` | Staging and artifact round-trip passes on JSON and Bincode | Implemented |
| W6 | Adversarial path stress tests | `tests/m5b_challenger_1_stress.rs`, `tests/m5b_challenger_2_stress.rs` | Traversal fuzzing and DOS device evasion tests pass | Implemented |

## Status log

- 2026-10-23: Created and accepted for Milestone M5B.
- 2026-10-23: Implemented W1–W5 on branch `feat/m5b-file-staging`.
- 2026-10-23: Resolved Challenger 1 and 2 path traversal edge cases (pre-dot whitespace DOS device evasion, multi-dot components).
- 2026-10-23: Verified via `cargo test -p rusty_grid_cli --test file_staging_e2e`, `m5b_challenger_1_stress`, and `m5b_challenger_2_stress` (31 passing tests). Status marked Implemented.
- 2026-10-26: The CLI moved to the content-addressed files of design record 0006: `submit --file` is an alias of `--input` and `--output` takes patterns, both through the master's blob store. The inline staging described here (`Task::staged_files`, `expected_outputs`, `TaskResult::artifacts`) is unchanged and still used by the MCP server and library callers.
- 2026-11-01: Removed `cache/objects` hard-linking from `Sandbox::stage_files` (BUG-1) so each task sandbox gets an independent file (`nlink == 1`) written from `staged.data`, preventing in-place mutations or permission changes from affecting subsequent tasks.
- 2026-11-01: Corrected G6 and D6 to name `DEFAULT_MAX_TERMINAL_BYTES` (`--max-retained-mb` / `RUSTY_GRID_MAX_RETAINED_MB`) and added the 256 MiB active task byte cap `DEFAULT_MAX_ACTIVE_BYTES` (`--max-active-mb` / `RUSTY_GRID_MAX_ACTIVE_MB`) from design record 0008 (BUG-4).
- 2026-11-04: Removed the unused `_cache_base_dir` parameter from `Sandbox::stage_files` and added worker start-up cleanup of legacy `<sandbox_base>/cache/objects` and `<sandbox_base>/cache/tmp` directories while leaving `<sandbox_base>/cache/blobs` intact (BUG-8).
