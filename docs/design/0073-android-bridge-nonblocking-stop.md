# 0073: Android bridge non-blocking stop and start synchronization

Status: Implemented
Created: 2026-11-15    Updated: 2026-11-15

## Summary

Makes `stop_worker_impl` in `crates/android_bridge` return without blocking the calling thread by
joining the worker thread on a separate helper thread, closes the concurrent `start_worker_impl`
race with `compare_exchange`, replaces mutex `lock().unwrap()` calls in `crates/android_bridge` with
poison-tolerant locking, and checks `rusty_grid_android_bridge` for `aarch64-linux-android` in CI.

## Context

Previously (at `c2462e3`, `AND-5` in `docs/BACKLOG.md`):

- `stop_worker_impl` (`crates/android_bridge/src/lib.rs`) sent `true` on `shutdown_tx`, set
  `running = false`, and called `h.join()` on the calling thread. Because `WorkerClient` drains
  cancelled tasks for up to `TASK_DRAIN_TIMEOUT` (`10 s`, `crates/worker/src/client.rs`), calling
  `OxideWorkerBridge.stop()` (`nativeStopWorker`) from `MainActivity.kt` or `OxideWorkerService.kt`
  could block the Android UI thread long enough to trigger an "Application Not Responding" dialog.
- `start_worker_impl` (`crates/android_bridge/src/lib.rs`) checked `state.running.load(...)` at the
  top of the function and stored `true` only after configuring the worker and checking the sandbox
  directory, allowing two concurrent start calls to both pass the guard and spawn two workers.
- Mutex accesses in `crates/android_bridge/src/lib.rs` used `lock().unwrap()`, which could panic
  across the JNI boundary if a lock was poisoned.
- `.github/workflows/ci.yml` never compiled `rusty_grid_android_bridge` for an Android target.

## Goals

- **G1**: Return from `stop_worker_impl` (`nativeStopWorker`) immediately without blocking the
  calling thread while the previous worker thread finishes draining.
- **G2**: Guarantee that two concurrent calls to `start_worker_impl` start at most one worker, and
  handle a start call while a previous worker is still draining without blocking the caller.
- **G3**: Avoid introducing panic paths across JNI by using poison-tolerant mutex locking in
  `crates/android_bridge`.
- **G4**: Compile-check `rusty_grid_android_bridge` for `aarch64-linux-android` in the `clippy` CI
  job on `ubuntu-latest`.

## Non-goals

- Changing the wire protocol or modifying `crates/worker/src/{client,runner,process_tree}.rs`.
- Modifying the Kotlin app (`packaging/android/app/`), whose existing status strings remain
  compatible.
- Running Android emulator or device tests in CI.

## Design

### D1 Non-blocking stop, draining state, and lifecycle serialization (`crates/android_bridge/src/lib.rs`)

- `WorkerState` tracks `lifecycle: Mutex<()>`, `running: AtomicBool`, `stopping: AtomicBool`, and
  `generation: AtomicU64`. Both `start_worker_impl` and `stop_worker_impl` hold `lifecycle` across
  their state transitions so Start and Stop cannot interleave or clobber one another's fields.
- `stop_worker_impl` acquires `lifecycle`, sets `stopping = true` (`swap(true)`), takes
  `thread_handle` and `shutdown_tx`, sends `true` on `shutdown_tx`, sets `status = "STOPPING"` when
  a worker thread handle was present, and only then transitions `running` from `true` to `false`. If
  a handle was taken, it spawns a detached join thread (`std::thread::spawn`) that waits on
  `h.join()` and then, under `lifecycle`, sets `status = "STOPPED"` and `stopping = false`. If no
  handle was present, it sets `status = "STOPPED"` and clears `stopping` only when no earlier stop
  is still draining (`!was_stopping`).
- Before acquiring `lifecycle`, if `stopping` is `true`, `start_worker_impl` polls `stopping` for up
  to `30 ms` so an already-idle worker runtime can finish shutting down. After acquiring `lifecycle`
  and claiming `running`, it re-checks `stopping`: if the previous worker is still draining tasks,
  it sets `status = "STOPPING"`, releases its `running` claim, and returns `false` without blocking
  the caller.
- `start_worker_impl` configures the in-process mobile worker with `.with_no_gpu(!simulate_gpu)`,
  `.with_no_blender(true)`, and `.with_no_cuda(true)` to skip host subprocess hardware probes, and
  constructs `WorkerClient::new(config)` inside `tokio::task::spawn_blocking` raced against
  `shutdown_rx.changed()` so stop requests during client initialization take effect immediately.

### D2 Atomic start claim and generation-guarded self-exit (`crates/android_bridge/src/lib.rs`)

- Under `lifecycle`, `start_worker_impl` claims `state.running` with
  `compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)` and holds `lifecycle` through
  checking the sandbox directory, storing `worker_id`, `p2p_ticket`, `shutdown_tx`, and
  `status = "STARTING"`, incrementing `generation`, spawning the worker thread, and storing
  `thread_handle`. If `running` is already `true`, it returns `true` immediately without spawning a
  second worker thread.
- Every early-return error path (`stopping` re-check, `check_sandbox_usable` failure, and thread
  spawn failure) resets `running` to `false` before releasing `lifecycle`.
- Each spawned worker thread captures its `my_generation` and scopes both `WorkerThreadGuard`
  (`debug_assert!(live <= 1)`) and the Tokio runtime inside an inner block so `rt.shutdown_background()`
  (which consumes `rt`) and `drop(_worker_guard)` complete before any self-exit state update. On
  self-exit (runtime build failure, uncancelled `WorkerClient::new` failure, or `client.run`
  returning when not stopped), the thread locks `lifecycle` and updates `status` and
  `running = false` only if `state.generation == my_generation && !state.stopping`.

### D3 Poison-tolerant mutex locking (`crates/android_bridge/src/lib.rs`)

- `lock_unpoisoned` wraps `mutex.lock().unwrap_or_else(|e| e.into_inner())` and replaces
  `lock().unwrap()` across `crates/android_bridge/src/lib.rs`.

### D4 Android target check in CI (`.github/workflows/ci.yml`)

- In the `clippy` job on `ubuntu-latest`, right after `master compiles without default features
  (dashboard, p2p)`, a step gated with `if: matrix.os == 'ubuntu-latest'` installs the
  `aarch64-linux-android` target via `rustup target add aarch64-linux-android`, sets
  `CC_aarch64_linux_android`, `AR_aarch64_linux_android`, and
  `CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER` to the preinstalled NDK toolchain under
  `$ANDROID_NDK_LATEST_HOME/toolchains/llvm/prebuilt/linux-x86_64/bin`, and runs
  `cargo check -p rusty_grid_android_bridge --target aarch64-linux-android --locked`.

## Compatibility

- No wire protocol messages or CLI flags change.
- Kotlin callers (`OxideWorkerBridge.kt`, `MainActivity.kt`, `OxideWorkerService.kt`) continue to
  work without source changes: `nativeStopWorker` still returns `true`, `nativeIsRunning` returns
  `false` as soon as stop is requested, and `get_worker_status_impl` reports `"STOPPING"` while the
  old worker drains and `"STOPPED"` once the join completes.

## Security

- No new network inputs, credentials, or file paths are introduced.
- Replacing `lock().unwrap()` with `unwrap_or_else(|e| e.into_inner())` prevents a poisoned mutex
  from unwinding toward the `extern "system"` JNI boundary.

## Test plan

- Host-side unit and integration tests in `crates/android_bridge` (`cargo test -p
  rusty_grid_android_bridge --locked`, including `--test mobile_coordination_test`):
  - `test_stop_worker_returns_in_under_100ms_while_worker_drains`: verifies `stop_worker_impl`
    returns in `< 500 ms` while the worker thread is still draining, `start_worker_impl` returns
    `false` with status `STOPPING` during the drain, and status becomes `STOPPED` once the join
    finishes.
  - `test_concurrent_start_calls_start_only_one_worker`: synchronizes two threads on a barrier and
    verifies that concurrent `start_worker_impl` calls connect only one worker to a local TCP
    listener.
  - `test_stop_then_start_again_works`: verifies stopping a worker and starting a fresh worker
    succeeds and assigns a new worker UUID.
  - `test_interleaved_stop_start_at_most_one_worker_thread`: runs 200 barrier-synchronized
    iterations of interleaved `start_worker_impl` and `stop_worker_impl` across two threads and
    verifies that at most one worker thread is ever live simultaneously.
  - `test_cancelled_start_does_not_clobber_stop_status`: drives `Start1 -> Stop -> Start2` using a
    test hook after Start1's running claim and verifies that exactly one worker thread is live,
    Start2's `worker_id` and `p2p_ticket` are preserved, and the status is not left stuck at
    `STARTING`.
  - `test_self_exiting_worker_then_immediate_start_does_not_panic`: starts a worker that self-exits
    immediately (invalid P2P ticket) and immediately restarts in a 50-iteration loop, verifying no
    panic and at most one live worker thread at any time.
- CI step `cargo check -p rusty_grid_android_bridge --target aarch64-linux-android --locked`:
  checked on GitHub Actions (`ubuntu-latest` with `$ANDROID_NDK_LATEST_HOME`), not locally on hosts
  without the Android NDK installed.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| AND-5 | Non-blocking `stop_worker_impl`, atomic `start_worker_impl` claim, poison-tolerant locks, host tests, and `aarch64-linux-android` CI check | `crates/android_bridge/src/lib.rs`, `.github/workflows/ci.yml`, `ANDROID_ARCHITECTURE.md`, `CHANGELOG.md`, `docs/README.md`, `docs/design/0073-android-bridge-nonblocking-stop.md` | `cargo test -p rusty_grid_android_bridge --locked` passes and CI checks `--target aarch64-linux-android` | Implemented |

## Status log

- 2026-11-15: Implemented `AND-5` in `crates/android_bridge` and `.github/workflows/ci.yml`.
