# 0100: Content-Addressed Action Cache (CACHE-1)

Status: Partial
Created: 2026-11-22    Updated: 2026-11-22

## Summary

`CACHE-1` adds an opt-in, content-addressed action cache on the master so identical deterministic tasks reuse previously produced output blobs and stdout/stderr instead of re-executing on workers. A task enables caching via `--cache` (CLI) or `cache: true` (MCP), keyed by a domain-separated BLAKE3 digest of the normalized `TaskSpec`, declared inputs, staged files, workspace source, expected output specs, placement constraints, and GPU requirements. Cached responses return `TaskResult::cached = true` over Protocol 11 while staying wire-compatible with Protocol 10 peers via legacy response mirrors.

## Context

Before this change, every `oxideswarm submit` or MCP `submit` executes from scratch (`crates/master/src/server/client_and_members.rs`), even when a previous run executed the exact same command, environment, declared inputs, and placement constraints and all of its output blobs are already stored on the master (`crates/master/src/blob_store.rs`). Repeat builds, identical test runs, and unchanged pipeline stages waste worker capacity and round-trip latency.

At the same time, blind caching would be unsafe: tasks without declared inputs can depend on external clocks, random generators, or ambient state, and output blobs stored for a cached result could otherwise be evicted by `BlobStore` LRU pressure (`BlobStore::evict_to_fit` in `crates/master/src/blob_store.rs`). Furthermore, adding `cached: bool` to `TaskResult` touches wire serialization, so this change rides on the Protocol 11 release train (`docs/design/0098-protocol-11-train.md`, PR `#386`) and follows the one-version compatibility window (`docs/design/0083-one-version-window.md`).

## Goals

- **G1. Opt-in deterministic hits:** Tasks submitted with `--cache` / `cache: true` hit on a matching canonical BLAKE3 key and return immediately from the master without scheduling a worker.
- **G2. Complete and non-volatile cache keys:** Volatile per-submission fields (`task.id`, `created_at_utc`, `timeout_secs`, scheduling-only resource hints, `opt:repeatable`, and cache opt-in metadata) do not affect cache identity; changes to spec, ordered env, inputs, staged files, workspace source, output specs, placement, or GPU needs miss cleanly.
- **G3. Fail-safe declared-inputs guard & verifier exclusion:** Reject `--cache` when a task has no declared inputs (and forbid combining `--cache` with `--verify`), while stripping cache extensions on verification replicas.
- **G4. Pin-protected bounded storage:** Cache entries pin their referenced output blobs in `BlobStore`, bound cache footprint via `--action-cache-mb` (default `512` MiB, `0` disables), evict LRU entries cleanly (releasing unreferenced pins), and persist across master restarts when `--state-dir` is enabled.
- **G5. Mixed-version protocol compatibility:** Protocol 11 peers receive `TaskResult::cached`; Protocol 10 clients still encode/decode `TaskCompleted` without `cached`, and Protocol 10/11 workers keep the unchanged `WorkerMessage::TaskResult` wire shape.

## Non-goals

- Caching tasks that fail (`!result.success || result.exit_code != 0`).
- Tracking undeclared filesystem reads or intercepting syscalls to discover hidden dependencies automatically.
- Cross-cluster or remote CAS sharing between independent masters.
- Separate cache read-only vs write-only CLI modes (extension payload reserves `0x01` read-write; future flags may add modes without breaking the extension registry entry).

## Design

### D1. Canonical action cache key (`crates/core/src/task/action_cache.rs`)

`compute_action_cache_key(task: &Task) -> ActionCacheKey` (`[u8; 32]`) computes a length-prefixed BLAKE3 digest prefixed by `"oxideswarm-action-cache-v1"`. Every sequence, map, string, and byte slice is encoded with a `u64` little-endian length prefix, and every `Option` is prefixed with a `0u8`/`1u8` tag so boundaries cannot collide.

Included fields:
1. `TaskSpec` execution definition: spec variant (`Command`, `ShellScript`, `RustCompilation`, `GpuCompute`, `BuiltinTest`, `Generative3D`, `AutoRig`) including command/program/script/source/payload fields, args/compiler flags, normalized `env` and `source_files` maps sorted by key, `working_dir`, and `stdin`.
2. Declared `inputs` (`TaskInput` with `InputSource::File` or `InputSource::Tree`): sorted canonically by `(path, kind, blob_id, size, executable)` so CLI `--input` flag ordering does not cause spurious misses.
3. Inline `staged_files` (`StagedFile`): sorted by `(path, executable, size, sha256, data)`, hashing `StagedFile::path`, executable bit, digest, and bytes.
4. `workspace` source (`workspace.source: Option<WorkspaceSource>` on `WorkspaceRef`): hashes `WorkspaceSource::Tree { manifest, manifest_size }` when present.
5. Outputs specification: sorted and deduplicated `output_globs` and `expected_outputs`.
6. Execution-affecting placement and hardware constraints: `task.scheduling_placement()` (`Placement::{os, arch, worker, isolation, tools, tags, extensions}` — strips non-placement labels such as `opt:repeatable`), `requirements.gpu_required`, and `requirements.vram_mb`.
7. Any future non-cache task and requirement extensions sorted by `(id, critical, bytes)`, excluding `ACTION_CACHE_EXTENSION_ID`.

Explicitly excluded fields:
- `task.id`, `task.created_at_utc`, `task.timeout_secs`
- `requirements.cpu_cores`, `requirements.ram_mb`, `requirements.max_retries`
- `opt:repeatable` placement hint
- The `action-cache` extension itself (so enabling/querying cache does not alter the key)

### D2. Opt-in contract, validation, and stale-result semantics (`crates/core/src/task/extension.rs`, `crates/core/src/task.rs`)

- **Extension slot entry:** `ACTION_CACHE_EXTENSION_ID = 1` is registered in `EXTENSION_REGISTRY` as `name: "action-cache"`, `list: ExtensionList::Task`, `critical: false`, `scope: ExtensionScope::Master`, with payload `[0x01]` (`ACTION_CACHE_EXT_PAYLOAD_READ_WRITE`). Because scope is `Master`, `Task::for_peer` and `strip_master_extensions` drop it before forwarding to workers so worker execution never rejects the extension.
- **Helpers on `Task`:** `with_action_cache()`, `is_action_cache_enabled()`, `strip_action_cache()`, and `has_declared_cache_inputs()`.
- **Validation:** `Task::validate()` rejects `is_action_cache_enabled()` when `!has_declared_cache_inputs()` (no declared blob `inputs`, no `staged_files`, and no declared `workspace.source`), or when `workspace.as_ref().is_some_and(|ws| ws.source.is_none())` (an unsynced persistent workspace whose on-worker files are mutable and undeclared).
- **Verifier incompatibility:** `oxideswarm submit` and MCP `submit` reject combining `cache` and `verify`. Additionally, `crates/cli/src/verify.rs` calls `replica.strip_action_cache()` when constructing spot-check replicas so replica runs always execute on workers and never consult or populate the cache.
- **Stale-result boundary:** Because caching relies strictly on declared inputs, tasks that fetch unpinned network URLs, read wall-clock time, consume randomness, or read undeclared host paths (for example under `--isolation off` or external runtime directories) can receive stale cached hits. This assumption is surfaced in `docs/reference.md`, CLI `--help`, and the MCP `submit` tool schema description.

### D3. Wire compatibility (`crates/core/src/task.rs`, `protocol.rs`, `client_resp_mirrors.rs`, `codec.rs`)

- `TaskResult`, `HumanTaskResult`, and `BinaryTaskResult` gain `pub cached: bool` as their final field (`#[serde(default)]` on `HumanTaskResult` JSON only; never `skip_serializing_if` on bincode types, enforced by `skip_serializing_if_is_kept_off_bincode_types`).
- Standard constructors (`TaskResult::success`, `TaskResult::failure`, `From<WorkerTaskResult>`) initialize `cached: false`.
- `WorkerMessage::TaskResult` carries individual fields rather than `TaskResult` directly and remains unchanged; the master always forces `cached = false` on incoming worker results.
- `TaskResult::from_bincode_bytes` decodes Protocol 11 `BinaryTaskResult`, falling back to `LegacyV10BinaryTaskResult` (up to `outputs`) and `LegacyBinaryTaskResult` (up to `error`) when decoding historical snapshots/cold results.
- **Protocol 10 peers:**
  - `ClientResponse::for_peer(v)` for `v < PROTOCOL_VERSION_CURRENT` folds `cached: false` on `TaskCompleted`.
  - `LegacyV2BinaryClientResponse`, `LegacyV3V4BinaryClientResponse`, and `LegacyV10BinaryClientResponse` use `LegacyV10BinaryTaskResult` inside `TaskCompleted`.
  - `HumanLegacyV10ClientResponse` adds `TaskCompleted` carrying `HumanLegacyV10TaskResult` so JSON v10 peers never receive `"cached"`.
  - `deserialize_message_as_v10::<ClientResponse>` routes binary tag `1` (`TaskCompleted`) through `LegacyV10BinaryClientResponse` and rejects v10 JSON `TaskCompleted` payloads containing `"cached"`.
  - `window_tests.rs` updates `DIFFERS_FROM_PREVIOUS` for `ClientResponse::TaskCompleted` and `Extension::00001` without altering `crates/core/tests/data/wire_layout_previous.txt`.

### D4. Master cache, blob pinning, eviction, and durability (`crates/master/src/action_cache.rs`, `blob_store.rs`, `store.rs`)

- **`BlobStore` cache pinning:** `BlobStore` tracks `cache_pins: HashMap<[u8; 32], Vec<BlobId>>` alongside task pins (`pins` / `output_pins`). `BlobStore::is_pinned_locked` treats any blob present in `cache_pins` as non-evictable during `evict_to_fit`. `pin_cache_entry(key, blobs)` returns `false` if any referenced output blob is missing rather than partially pinning.
- **LRU + byte cap:** `ActionCache` maintains entries indexed by `ActionCacheKey` in an LRU order, storing the canonical `TaskResult` (with `cached` normalized to `false` at rest, set to `true` when returned as a hit), entry size (`result.outputs` sum of blob byte lengths), and timestamps. Inserts exceeding `max_bytes` (default `512 MiB`, configurable via `--action-cache-mb`; `0` disables caching and clears runtime use) evict oldest entries via `unpin_cache_entry` until within budget; single results larger than `max_bytes` are not cached.
- **Lookup & insert points:**
  - In `ClientMessage::SubmitTask` (`crates/master/src/server/client_and_members.rs`) and `MasterHandle::submit_task` (`crates/master/src/server.rs`): after `task.validate()` succeeds and auth/queue checks pass, if `cache_enabled` and `task.is_action_cache_enabled()`, compute `key = compute_action_cache_key(&task)`. On hit where every output blob still exists and can be pinned/retained, skip queueing/dispatch, record a completed task entry via `TaskQueue::record_cached_completion` and the durability store, emit `ClientResponse::TaskCompleted` with `cached: true`, and update cache hit metrics.
  - In `apply_task_result_inner` (`crates/master/src/server/tasks_and_blobs.rs`): only when `original_task.is_action_cache_enabled()`, `result.success && result.exit_code == 0`, and cache is enabled, insert `(key, result)` into `ActionCache` after pinning output blobs.
- **Durability:**
  - `STORE_FORMAT_VERSION` increments `4 -> 5` (`crates/master/src/store.rs`), and `ColdResultStore::get` (`crates/master/src/cold_results.rs`) uses `TaskResult::from_bincode_bytes` so persisted v4 results decode cleanly alongside v5 results with `cached`.
  - When `--state-dir` is configured, `ActionCache` loads `<state_dir>/action_cache.json` on startup (pruning any entries whose output blobs are no longer present in `BlobStore` and enforcing `max_bytes`), and persists updates atomically (`action_cache.json.tmp` -> `fsync` -> rename).

### D5. CLI, MCP, and observability (`crates/cli`, `crates/master/src/metrics.rs`)

- **Master flag / env / TOML:** `oxideswarm master --action-cache-mb <MB>`, `RUSTY_GRID_ACTION_CACHE_MB`, `[master].action_cache_mb` (default `512`, `0` disables).
- **Submit flag & MCP:** `oxideswarm submit --cache` and MCP `submit` `cache: boolean` (default `false`). Both print/return cached status (`Cached: yes` in CLI `submit` and `result`; `"cached": result.cached` in MCP `submit` and `task_result`).
- **Metrics:** Prometheus metrics track `action_cache_hits_total`, `action_cache_misses_total`, `action_cache_entries`, and `action_cache_bytes`.

## Compatibility

- **Wire protocol:** Rides on Protocol 11 (`PROTOCOL_VERSION_CURRENT = 11`, `PROTOCOL_VERSION_MIN_COMPATIBLE = 10` per design `0098`). Protocol 10 clients still submit uncached tasks and receive `TaskCompleted` decoded without `cached`. Protocol 10 and 11 workers exchange unchanged `WorkerMessage::TaskResult` frames.
- **Journal & cold results:** `STORE_FORMAT_VERSION = 5` (`crates/master/tests/data/journal_layout.txt` re-blessed). `ColdResultStore` reads v4 (`LegacyV10BinaryTaskResult`) and v5 (`BinaryTaskResult`) records transparently.
- **Upgrade order:** Upgrade master first (masters accept v10 and v11 peers); once master runs Protocol 11, upgrading clients unlocks `--cache` and `TaskResult::cached` reporting.

## Security

- **Isolation:** Cache lookup runs after authentication/RBAC task-submission checks and `Task::validate()`. Cross-user or arbitrary unauthenticated submits cannot query cache hits.
- **Trust boundary:** Only deterministic declared-input tasks (`!inputs.is_empty() || !staged_files.is_empty() || has_workspace_source`) are eligible; `--verify` strips the cache extension on replica runs so a compromised worker poisoning an uncached task cannot poison spot-check verification.
- **Bounds:** Total cached output footprint is capped by `--action-cache-mb` (default `512 MiB`), `ActionCacheKey` uses domain-separated BLAKE3 with explicit length prefixes, and extension parsing enforces bounded 1-byte payload `[0x01]`.

## Test plan

- **Unit (`crates/core`):** Canonical key stability (map/input/output order independence), sensitivity to spec/env/inputs/staged files/workspace/placement/GPU, insensitivity to `task.id`/timestamp/timeout/cpu/ram/retries/`opt:repeatable`/cache flag, validation rejection without declared inputs, `TaskResult::cached` bincode/JSON round-trips, v10 mirror & window byte-for-byte replay (`window_tests`, `wire_layout_tests`, `skip_serializing_if`).
- **Unit (`crates/master`):** `BlobStore` cache pinning prevents eviction and unpin restores eviction; `ActionCache` hit/miss, failure exclusion, LRU eviction, zero-disable, atomic save/reload pruning missing blobs, `store_layout_is_frozen`.
- **Integration (`crates/cli/tests/action_cache_e2e.rs`, `tests/window_v10_peers.rs`):** End-to-end hit (`Cached: yes`, worker runs once), input mutation miss, failed task not cached, CLI `--cache` without inputs rejected, `--cache --verify` rejected, LRU eviction, restart persistence across `--state-dir`, `--action-cache-mb 0` disable, MCP `submit` `cache: true` + `"cached": true`, v10 client skew compatibility in `window_v10_peers`.
- **Live (2 machines):** Follow-up verification on Windows + macOS cluster before marking `CACHE-1` `Implemented`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| `CACHE-1a` | Extension id `1`, canonical key, validation, `TaskResult::cached`, v10 mirrors & window tests | `crates/core/src/task/extension.rs`, `crates/core/src/task/action_cache.rs`, `crates/core/src/task.rs`, `crates/core/src/protocol*.rs`, `crates/core/src/window_tests.rs` | Core unit, wire layout, and window replay tests pass | Implemented |
| `CACHE-1b` | `BlobStore` cache pinning, master `ActionCache`, LRU eviction, `state_dir` persistence, `STORE_FORMAT_VERSION = 5`, server wiring & metrics | `crates/master/src/blob_store.rs`, `crates/master/src/action_cache.rs`, `crates/master/src/store.rs`, `crates/master/src/cold_results.rs`, `crates/master/src/server*.rs`, `crates/master/src/queue*.rs`, `crates/master/src/metrics.rs` | Master unit & `store_layout_is_frozen` pass | Implemented |
| `CACHE-1c` | CLI flags (`--cache`, `--action-cache-mb`), verifier strip, MCP support, e2e & `window_v10_peers` tests, docs | `crates/cli/src/**`, `crates/cli/tests/action_cache_e2e.rs`, `tests/window_v10_peers.rs`, `docs/reference.md`, `CLAUDE.md` | `action_cache_e2e`, `window_v10_peers`, `mcp_server_e2e`, and repo gates pass | Implemented |
| `CACHE-1d` | Two-machine live cluster verification (macOS + Windows) | `docs/BACKLOG.md`, `docs/design/0100-action-cache.md` | Live hit/miss/eviction/restart run recorded against two real machines | Planned |

## Status log

- **2026-11-22:** Initial design record authored for `CACHE-1` on Protocol 11 train (`0098`, PR `#386`); work items `CACHE-1a`..`1c` implemented in progress, `CACHE-1d` live run queued after merge.
- **2026-11-22:** Completed `CACHE-1a`..`1c` locally and marked them `Implemented` (keeping `CACHE-1` `Partial` until `CACHE-1d` two-machine live verification): verified `rusty_grid_core` (`305` lib tests), `rusty_grid_master` (`397` lib tests + `--no-default-features` check + `window_v10_peers` `8/8`), `action_cache_e2e` (`6/6`), and repository lint/hygiene gates.
