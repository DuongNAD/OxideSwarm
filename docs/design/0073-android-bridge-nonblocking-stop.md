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

### D1 Non-blocking stop and draining state (`crates/android_bridge/src/lib.rs`)

- `WorkerState` tracks `stopping: AtomicBool` alongside `running: AtomicBool`.
- `stop_worker_impl` atomically transitions `running` from `true` to `false`, sets
  `stopping = true` and `status = "STOPPING"`, sends `true` on `shutdown_tx`, takes `thread_handle`,
  and spawns a background join thread (`std::thread::spawn`).
- The join thread waits on `h.join()`, then sets `status = "STOPPED"` and clears
  `stopping = false`.
- If `start_worker_impl` is called while `stopping` is `true`, it polls `stopping` for up to `30 ms`
  so an already-idle worker runtime can finish shutting down, and if `stopping` is still `true`
  (because the previous worker is draining tasks), sets `status = "STOPPING"`, releases its
  `running` claim, and returns `false` without blocking the caller.

### D2 Atomic start claim (`crates/android_bridge/src/lib.rs`)

- `start_worker_impl` claims `state.running` at entry with
  `compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)`. If `running` is already
  `true`, it returns `true` immediately without spawning a second worker thread.
- Every early-return error path (`stopping` check, `check_sandbox_usable` failure, thread spawn
  failure, and runtime build failure inside the worker thread) resets `running` to `false`.

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
    returns in `< 100 ms` while the worker thread is still draining, `start_worker_impl` returns
    `false` with status `STOPPING` during the drain, and status becomes `STOPPED` once the join
    finishes.
  - `test_concurrent_start_calls_start_only_one_worker`: synchronizes two threads on a barrier and
    verifies that concurrent `start_worker_impl` calls connect only one worker to a local TCP
    listener.
  - `test_stop_then_start_again_works`: verifies stopping a worker and starting a fresh worker
    succeeds and assigns a new worker UUID.
- CI step `cargo check -p rusty_grid_android_bridge --target aarch64-linux-android --locked`:
  checked on GitHub Actions (`ubuntu-latest` with `$ANDROID_NDK_LATEST_HOME`), not locally on hosts
  without the Android NDK installed.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| AND-5 | Non-blocking `stop_worker_impl`, atomic `start_worker_impl` claim, poison-tolerant locks, host tests, and `aarch64-linux-android` CI check | `crates/android_bridge/src/lib.rs`, `.github/workflows/ci.yml`, `ANDROID_ARCHITECTURE.md`, `CHANGELOG.md`, `docs/README.md`, `docs/design/0073-android-bridge-nonblocking-stop.md` | `cargo test -p rusty_grid_android_bridge --locked` passes and CI checks `--target aarch64-linux-android` | Implemented |

## Status log

- 2026-11-15: Implemented `AND-5` in `crates/android_bridge` and `.github/workflows/ci.yml`.
