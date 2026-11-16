# 0072: Collect outputs without following links

Status: Implemented
Created: 2026-11-15    Updated: 2026-11-15

## Summary

The worker collects each task output through a single file descriptor opened without following a
final symbolic link (`O_NOFOLLOW | O_NONBLOCK` on Unix, `FILE_FLAG_OPEN_REPARSE_POINT` on Windows)
and verified by `fstat` to be a regular file. The BLAKE3 blob id, the output size check, and any
copied bytes all come from that descriptor (`SEC-13`).

## Context

Previously:

- `collect_outputs_inner` (`crates/worker/src/staging.rs`) walked the sandbox with
  `DirEntry::file_type()`, which does not follow symbolic links, recorded `Found { path, size }`,
  checked `is_executable(&file.path)` via `std::fs::metadata`, and passed `&file.path` to
  `BlobCache::insert_file` or `BlobCache::insert_file_pinned`.
- `BlobCache::insert_file_inner` (`crates/worker/src/blob_cache.rs`) called `hash_file(path)`
  (`crates/core/src/blob.rs`, plain `File::open`, which follows symbolic links), then
  `single_link(path)` and `std::fs::rename(path, &tmp)` or `copy_verified(path, &tmp, id)` (plain
  `File::open`).
- On macOS, and on Linux without a delegated cgroup, a task process that calls `setsid()` and drops
  `RUSTY_GRID_RUN_TOKEN` can outlive the task (`crates/worker/src/process_tree.rs`). Between the
  directory walk and the file open, it could replace a selected output with a symbolic link to a
  file that only the worker can read (for example under `--isolation fs`), causing the worker to
  hash and upload that target.

## Goals

- **G1**: Open every collected output file without following a final symbolic link or blocking on a
  FIFO, and refuse anything whose open descriptor is not a regular file.
- **G2**: Derive the BLAKE3 `BlobId`, the executable bit, the size checked against
  `--output-limit-mb`, and any copied cache bytes from that single open descriptor.
- **G3**: Preserve the single-link move fast path (`allow_move`) only when `symlink_metadata(tmp)`
  after `rename(path, tmp)` is a regular file with the same `dev`/`ino` (Unix) or volume serial and
  file index (Windows) as the open descriptor, falling back to copying from the descriptor and
  removing the moved entry otherwise.
- **G4**: Keep hard-linked regular files working (copied, never moved), as required by `cargo` build
  artifacts and `RustCompilation`.

## Non-goals

- Changing the wire protocol or CLI flags.
- Refusing hard links inside the sandbox (`nlink > 1` continues to copy the regular file).

## Design

### D1 `open_regular_file_nofollow` (`crates/core/src/blob.rs`)

- On Unix, `open_regular_file_nofollow` opens `path` with
  `OpenOptionsExt::custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)` so symbolic links fail with
  `ELOOP` (mapped to `ErrorKind::InvalidInput`) and FIFOs never block `open(2)`.
- On Windows, it opens `path` with `FILE_FLAG_OPEN_REPARSE_POINT` (`0x0020_0000`) and inspects
  `symlink_metadata(path)` if opening a directory or directory reparse point fails with
  `PermissionDenied`.
- It inspects `file.metadata()` (`fstat` / `GetFileInformationByHandle`) and refuses any handle
  whose metadata reports a symbolic link, reparse point, or non-regular file.
- `hash_file(path)` also delegates to `open_regular_file_nofollow(path)`.

### D2 Descriptor-backed `BlobCache::insert_open_file` (`crates/worker/src/blob_cache.rs`)

- `BlobCache::insert_file_inner` opens `(file, meta)` via `open_regular_file_nofollow(path)` and
  calls `BlobCache::insert_open_file(path, file, &meta, allow_move, pin)`.
- `insert_open_file` hashes `&mut file` with `hash_reader`.
- When `allow_move` holds and `descriptor_is_single_link(&file, meta)` is true (`meta.nlink() == 1`
  on Unix, `nNumberOfLinks == 1` via `GetFileInformationByHandle` on Windows), it renames `path` to
  `tmp` and checks `moved_entry_matches_descriptor(&tmp, &file, meta, size)`:
  - `symlink_metadata(&tmp)` must be a regular file (never a symlink or reparse point) of length
    `size`;
  - on Unix, `tmp_meta.dev() == meta.dev()`, `tmp_meta.ino() == meta.ino()`, and
    `tmp_meta.nlink() == 1`;
  - on Windows, `GetFileInformationByHandle` on `open_regular_file_nofollow(&tmp)` must match the
    descriptor's volume serial number and 64-bit file index with `nNumberOfLinks == 1`.
- If the post-rename check fails, `remove_moved_entry(&tmp)` removes `tmp` and
  `copy_descriptor_verified(&mut file, &tmp, id, size)` rewinds `file` to offset `0` and copies and
  verifies the bytes from the descriptor into `tmp`.

### D3 Output collection (`crates/worker/src/staging.rs`)

- `insert_found_outputs` opens each selected `Found` path with `open_regular_file_nofollow`,
  updates the entry's size from the descriptor's `meta.len()`, re-checks `OutputLimits` before
  hashing, reads the executable bit from `meta`, and inserts the open descriptor via
  `cache.insert_open_file`.
- An output that is a symbolic link or FIFO at open time fails collection with a clear message
  naming the relative sandbox path (`output '<rel>' is not a regular file or directory (symbolic
  links are never returned): ...`).

## Compatibility

No wire-format, CLI, or configuration changes.

## Security

- **Single descriptor for hash and bytes**: the master verifies every uploaded blob against its
  BLAKE3 `BlobId`. Because the worker computes the `BlobId` and reads or verifies the cached bytes
  from the same `O_NOFOLLOW` regular-file descriptor, a post-walk symlink swap cannot cause the
  worker to read, cache, or upload a symlink target outside the sandbox, nor leave a symlink inside
  the blob cache.
- **FIFOs**: `O_NONBLOCK` prevents a task that places or swaps in a named pipe from wedging the
  worker's blocking collection thread on `open(2)`.
- **Hard links**: hard links inside the sandbox are copied rather than refused because `cargo` and
  `RustCompilation` hard-link build artifacts. Preventing a sandboxed task from hard-linking host
  files into the sandbox depends on Landlock `REFER` confinement on Linux (`crates/worker/src/isolation/linux.rs`)
  and is tested for macOS Seatbelt under `SEC-12`.

## Test plan

- `crates/core/src/blob.rs`:
  `open_regular_file_nofollow_refuses_symlinks_and_fifos_without_blocking`.
- `crates/worker/src/blob_cache.rs`:
  - `insert_file_refuses_symlink_outside_sandbox_and_never_caches_target`
  - `insert_file_refuses_fifo_without_blocking`
  - `insert_open_file_falls_back_to_descriptor_copy_when_path_is_swapped_for_symlink_before_rename`
  - `moving_an_output_in_never_steals_a_file_with_other_links`
- `crates/worker/src/staging.rs`:
  - `swapping_selected_output_for_symlink_after_walk_is_refused`
  - `fifo_output_does_not_block_and_is_refused`
  - `hard_linked_outputs_and_descriptor_size_limits_work`
- Windows (`#[cfg(windows)]`): `open_regular_file_nofollow_refuses_ntfs_junction`, `insert_found_outputs_refuses_ntfs_junction`, and `insert_open_file_falls_back_to_descriptor_copy_when_path_is_replaced_before_rename`.
- End-to-end suites: `blob_transfer_e2e` and `file_staging_e2e`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| SEC-13 | Open and collect task outputs through an `O_NOFOLLOW` descriptor verified to be a regular file | `crates/core/src/blob.rs`, `crates/worker/src/blob_cache.rs`, `crates/worker/src/staging.rs`, `docs/design/0072-collect-outputs-without-following-links.md`, `docs/README.md`, `CHANGELOG.md` | Unit tests in `blob`, `blob_cache`, and `staging` plus `blob_transfer_e2e` and `file_staging_e2e` pass | Implemented |

## Status log

- 2026-11-15: Implemented `SEC-13` in `crates/core/src/blob.rs`, `crates/worker/src/blob_cache.rs`,
  and `crates/worker/src/staging.rs`.
