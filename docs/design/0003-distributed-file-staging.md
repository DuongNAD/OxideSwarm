# 0003: Distributed file staging and output artifact collection

Status: Implemented
Created: 2026-10-23    Updated: 2026-10-23

## Summary

Distribute input files to worker execution sandboxes and collect output artifacts:

- stage local files into worker task sandboxes via `submit --file <local>[:<sandbox>]`;
- declare expected output files and directories via `submit --output <path>` and `submit --output-dir <dir>`;
- define `StagedFile` and `OutputArtifact` data models with SHA-256 content verification;
- cache staged files by content hash with atomic writes and hardlink placement on workers;
- isolate sandboxes against directory traversal (`..`), leading slashes, Windows DOS device names, and symlink escapes;
- account for file staging payloads within the master queue 256 MiB LRU retention budget.

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
- **G3** Deduplicate and cache staged files on workers by SHA-256 digest with atomic placement.
- **G4** Enforce strict sandbox isolation, rejecting traversal components, root paths, reserved Windows device names, and symlinks resolving outside the sandbox.
- **G5** Maintain wire compatibility across JSON (`0x01`), Bincode (`0x02`), and AEAD (`0x04`) codecs via protocol capability negotiation.
- **G6** Cap total master retained memory for staged files and artifacts at 256 MiB using LRU eviction.

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

### D3 Worker content-hash cache layout and placement

Workers maintain an immutable content-addressed cache:

```text
<sandbox_base_dir>/
├── cache/
│   └── objects/
│       ├── <sha256_hex_1>
│       └── <sha256_hex_2>
└── tasks/
    └── task-<uuid>/
        ├── data/
        │   └── input.csv -> link to cache/objects/<sha256>
        └── results.json
```

Placement flow:
1. When receiving a `StagedFile`, the worker verifies that `sha256(data) == staged_file.sha256`.
2. Checks if `<sandbox_base>/cache/objects/<sha256>` exists.
3. If absent, writes payload to a temporary file (`.tmp.<uuid>`) in the cache directory, fsyncs, and renames atomically to `<sandbox_base>/cache/objects/<sha256>`.
4. Creates destination directory tree inside the task sandbox.
5. Attempts to create a hardlink (`std::fs::hard_link`) from the cache object into the sandbox target path. If hardlinking fails (e.g. cross-device link), falls back to copying.
6. Sets Unix file permissions (`0o755` if `executable`, otherwise `0o644`).

Implemented in `crates/worker/src/sandbox.rs` (`SandboxManager::stage_file`).

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

### D6 Master queue memory accounting and LRU budget

The master queue tracks retained payload memory in `crates/master/src/queue.rs`:
- Retained memory budget defaults to 256 MiB (`DEFAULT_MAX_RETAINED_MB = 256`), configurable via `--max-retained-mb` or `OXIDESWARM_MAX_RETAINED_MB`.
- Each completed task accounts for the size of its stored `TaskSpec` (including `StagedFile` bytes) and execution `TaskResult` (including `OutputArtifact` bytes).
- When total memory exceeds 256 MiB, terminal tasks are evicted in LRU order (oldest finished tasks first) until memory falls within budget.

## Compatibility

- Workers advertising `ProtocolCapabilities::FILE_STAGING` receive tasks with staged files.
- If a client attempts to submit staged files to an older master or worker lacking the capability, the master rejects submission with `CapabilityMismatch`.
- Existing tasks without staged files incur zero wire overhead (empty vectors serialize to single byte prefixes).

## Security

- Sanitization prevents reading or writing outside the task sandbox directory.
- SHA-256 digest validation guarantees that tampered or corrupted file payloads are rejected prior to execution.
- File staging operates entirely within the mutual HMAC-SHA256 authenticated and encrypted stream (`0x04`).

## Test plan

- **Unit:** path normalization, traversal rejection, DOS device name evasion, SHA-256 digest validation, and cache atomic placement (`crates/core`, `crates/worker`).
- **Stress:** adversarial path traversal test suites, whitespace evasion generators, and combinatorial fuzzing (`tests/m5b_challenger_1_stress.rs`, `tests/m5b_challenger_2_stress.rs`).
- **Integration:** end-to-end file staging, execution, output artifact collection, and LRU queue eviction over JSON and Bincode wire codecs (`tests/file_staging_e2e.rs`).
- **Gates:** `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Staging data models and sanitization | `crates/core/src/task.rs` | `StagedFile`, `OutputArtifact`, and `validate_relative_sandbox_path` implemented | Implemented |
| W2 | Worker cache and sandbox staging | `crates/worker/src/sandbox.rs` | SHA-256 cache objects, hardlink placement, and artifact collection | Implemented |
| W3 | CLI staging options | `crates/cli/src/cli_app.rs` | `--file`, `--output`, `--output-dir` parsing and payload building | Implemented |
| W4 | Master queue LRU budget | `crates/master/src/queue.rs` | 256 MiB retention budget and artifact eviction | Implemented |
| W5 | End-to-end integration tests | `tests/file_staging_e2e.rs` | Staging and artifact round-trip passes on JSON and Bincode | Implemented |
| W6 | Adversarial path stress tests | `tests/m5b_challenger_1_stress.rs`, `tests/m5b_challenger_2_stress.rs` | Traversal fuzzing and DOS device evasion tests pass | Implemented |

## Status log

- 2026-10-23: Created and accepted for Milestone M5B.
- 2026-10-23: Implemented W1–W5 on branch `feat/m5b-file-staging`.
- 2026-10-23: Resolved Challenger 1 and 2 path traversal edge cases (pre-dot whitespace DOS device evasion, multi-dot components).
- 2026-10-23: Verified via `cargo test -p rusty_grid_cli --test file_staging_e2e`, `m5b_challenger_1_stress`, and `m5b_challenger_2_stress` (31 passing tests). Status marked Implemented.
