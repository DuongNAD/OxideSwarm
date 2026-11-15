# 0074: External read-only runtime allowlist and sandboxed GPU workload route

Status: Implemented
Created: 2026-11-16    Updated: 2026-11-16

## Summary

Adds opt-in read-only external runtime directory allowlisting on workers (`--isolation-read-dir`,
alias `--allow-read-dir`, `RUSTY_GRID_ISOLATION_READ_DIRS`, `[worker].isolation_read_dirs`) across
macOS Seatbelt, Linux Landlock, and Windows AppContainer while preserving home and secret isolation,
records macOS and Windows `R20` GPU sandbox results, and adds `docs/guides/gpu-workloads.md`.

## Context

Spike `R20` ([design 0064](0064-research-of-2026-11-13-and-route.md) D3) tested native GPU
subprocesses under `--isolation off`, `--isolation fs`, and `--isolation fs+net` on macOS
(`Apple M5` Metal) and Windows 11 (`NVIDIA GeForce RTX 5060 Ti` Vulkan, CUDA Driver API, `nvcc`,
and Blender 4.5.4 LTS Cycles `CUDA`/`OPTIX`):

1. Native GPU compute already worked under `--isolation fs` and `--isolation fs+net` on both
   operating systems (`wgpu` Metal and `MTLCompilerService` shader compilation under macOS
   `sandbox-exec`; `wgpu` Vulkan, `nvcuda.dll` PTX JIT, `nvidia-smi.exe`, `nvcc.exe`, and Blender
   Cycles `CUDA` and `OPTIX` under Windows AppContainer), while task probes confirmed cluster
   secrets and home listings remained denied (`DENIED` on macOS; `ERROR_ACCESS_DENIED` / `os_err=5`
   on Windows).
2. Both systems blocked the same practical class of ML workloads (`W4`): Python/PyTorch toolchains
   installed under user-home subdirectories (`~/.pyenv/versions/<PY_VER>` failed on macOS with
   exit code `71` under `sandbox-exec`, and `%LOCALAPPDATA%\Programs\Python\Python311` was refused at
   Windows worker preflight in `crates/worker/src/isolation.rs` as lying outside the sandbox,
   worker toolchain, and system directories).
3. On Linux, `crates/worker/src/isolation/linux.rs` only permits `/dev/null`, `/dev/zero`,
   `/dev/urandom`, and `/dev/random` in its Landlock ruleset and opens no `/dev/nvidia*` or
   `/dev/dri/*` device nodes yet (`R20` Linux host probing remains `Planned`).

Exposing a user's Python, conda, or CUDA toolkit directory by relaxing `USER_HOME`, `%APPDATA%`, or
`%LOCALAPPDATA%` broadly would undermine home isolation (`design 0033`). Instead, the worker needs an
explicit operator-configured read-only directory allowlist with strict boundary checks.

## Goals

- **G1**: Let worker operators allowlist specific external runtime directories read-only under
  `--isolation fs` and `--isolation fs+net` via CLI flag, environment variable, and `rusty-grid.toml`.
- **G2**: Refuse broad, overlapping, secret-bearing, missing, non-directory, or symlink/junction paths
  before accepting work or granting AppContainer ACLs.
- **G3**: Enforce identical read-only + execute confinement semantics across macOS Seatbelt, Linux
  Landlock, and Windows AppContainer while preserving read-only denial of writes, cluster secrets,
  SSH keys, sibling sandboxes, and workspaces.
- **G4**: Document end-to-end GPU workload commands (`docs/guides/gpu-workloads.md`) and update
  `README.md` status/flags to match recorded behavior on all three operating systems.

## Non-goals

- Implicitly allowing `$HOME`, `%USERPROFILE%`, `%APPDATA%`, or `%LOCALAPPDATA%`.
- Letting tasks or remote submitters request arbitrary host read paths over the wire protocol.
- Adding unverified Linux `/dev/nvidia*` or `/dev/dri/*` Landlock rules without a Linux GPU host run.
- Changing Windows relative-path resolution for staged `./prog.exe` commands (`W2_relpath_direct`).

## Design

### D1 CLI, environment, and configuration plumbing

- Added repeatable `--isolation-read-dir <DIR>` (alias `--allow-read-dir`) on `WorkerArgs` and
  `RUSTY_GRID_ISOLATION_READ_DIRS` (`ISOLATION_READ_DIRS_ENV`, parsed with `std::env::split_paths`)
  in `crates/cli/src/cli_app.rs` and `crates/worker/src/lib.rs`.
- Added `WorkerFileConfig::isolation_read_dirs: Option<Vec<String>>` in `crates/cli/src/config.rs`.
- Precedence in `resolve_isolation_read_dirs`: non-empty CLI flags win over non-empty
  `RUSTY_GRID_ISOLATION_READ_DIRS`, which wins over `[worker].isolation_read_dirs`, deduplicated in
  order.
- Added `WorkerConfig::isolation_read_dirs` (`with_isolation_read_dirs`) and
  `HostDirs::extra_read_only_dirs` (`with_extra_read_only_dirs`), passing configured paths into
  `WorkerClient::validate_isolation_config()` (called early in `run_worker` and `WorkerClient::run`).
- When `--isolation off` is combined with non-empty allowlist paths, start-up logs a warning noting
  that allowlist entries have no confinement effect under `off`.

### D2 Path boundary and symlink validation (`validate_extra_read_only_dirs`)

`validate_extra_read_only_dirs` (`crates/worker/src/isolation.rs`) validates every configured entry
before any policy rule or Windows ACL is applied:

1. Rejects empty strings, relative paths, non-existent targets, and non-directory paths.
2. Refuses symbolic links (`symlink_metadata`) and Windows reparse points/NTFS junctions
   (`FILE_ATTRIBUTE_REPARSE_POINT`) on the target itself and along every ancestor strictly inside
   `user_home`.
3. Canonicalizes the target and rejects exact equality or container ancestors of `user_home`
   (including `Desktop`, `Documents`, `Downloads`, `.config`, `.local`, `.cache`, `Library`, `AppData`,
   `AppData/Local`, `AppData/Local/Programs`, `AppData/Roaming`, and `temp_dir()`).
4. Rejects any parent or child subtree overlap (bidirectional containment) with `OXIDESWARM_HOME`,
   `~/.oxideswarm`, `.ssh`, `.aws`, `CARGO_HOME`, `sandbox_base`, `workspace_root`, or any configured
   `protected_files` (`cluster.secret`, `node.key`, `p2p_secret.key`, `master_noise.key`, config).

### D3 Platform confinement integration

- **macOS (`macos::profile`, `macos::arguments`)**: Emits `(allow file-read* (subpath (param "EXTRA_READ_<i>")))`
  and `file-read-metadata` ancestor rules (`ANCESTOR_*`) after all user-home/cache/temp denies
  (last-match-wins) and before `CARGO_CRED` and `PROTECTED_*` denials.
- **Linux (`crates/worker/src/isolation/linux.rs`)**: Appends `host.extra_read_only_dirs` to
  `read_only_roots` (read+execute Landlock rights only, no write right) after validation, with
  `carve` continuing to exclude any protected file inside granted trees.
- **Windows (`mod windows` in `crates/worker/src/isolation.rs`)**:
  - `prepare_worker_toolchains` validates paths via `validate_extra_read_only_dirs`, strips any
    `oxideswarm.toolchain` ACE from `protected_files`, grants `oxideswarm.toolchain` + `CurrentUser`
    `(OI)(CI)(RX)` via `grant_rx_recursive`, and appends each directory to the ACL manifest
    (`acl_grant_manifest_path`) for reversible removal by `windows/revoke-isolation-acls.ps1`.
  - `WindowsAppContainerGuard::create_blocking` runs `validate_extra_read_only_dirs` per task and
    permits executables located under `host.extra_read_only_dirs` (`in_extra_read`) during program
    preflight without relaxing `sandbox_dir` or protected-file checks.
  - `probe_with_config` confirms inside an ephemeral AppContainer probe that configured
    `protected_files` remain unreadable (`ACCESS_DENIED`).

### D4 Recorded GPU probe evidence (`R20`)

- **macOS (`Apple M5`, Metal, 2026-11-15)**: Under `--isolation fs` and `--isolation fs+net`,
  in-process `wgpu`, out-of-process `gemm_bench`, and native Swift Metal (`MTLCompilerService`)
  succeeded (`rc=0`) while canary/account-home reads were denied; a `~/.pyenv` Python runtime failed
  with `rc=71` before allowlisting and outside-home `/opt/homebrew` Python succeeded (`rc=0`).
- **Windows 11 (`NVIDIA GeForce RTX 5060 Ti`, 2026-11-15/16)**:
  - Host baseline probe (`win_r20_gpu_probe.md`): `wgpu` Vulkan (`19/19` compute, `10/10`
    adversarial), `nvcuda.dll` PTX JIT, `nvidia-smi.exe`, `nvcc.exe`, and Blender 4.5.4 LTS Cycles
    (`CUDA` and `OPTIX`) passed under `--isolation fs` and `--isolation fs+net` with secrets denied,
    while user-profile `%LOCALAPPDATA%\Programs\Python\Python311` was blocked before launch.
  - Post-fix live loopback AppContainer CUDA probe (isolated temporary `HOME` and `OXIDESWARM_HOME`
    on the `R20` branch, `cuda_probe.exe` under `...\AppData\Local\Programs\Python\Python311`):
    without `--isolation-read-dir`, `submit --gpu --vram-mb 512 --require-isolation fs` failed
    (`Exit Code: 1`, `11 ms`, preflight refused path outside sandbox/toolchain/system dirs); with
    `--isolation-read-dir`, the exact same task succeeded (`Exit Code: 0`, `141 ms`,
    `CUDA_DEVICE=NVIDIA GeForce RTX 5060 Ti driver_ver=13050 total_mib=16283 CUDA_VRAM_ROUNDTRIP_OK`),
    writes inside the allowlisted folder failed (`os_err=Some(5)`), `cluster.secret` / `node.key` /
    `.ssh/id_ed25519` reads failed (`os_err=Some(5)`), and `windows/revoke-isolation-acls.ps1`
    removed the granted `oxideswarm.toolchain` ACE cleanly (`rc=0`).

## Compatibility

Worker-local feature only; `PROTOCOL_VERSION_CURRENT` remains `9` and wire messages are unchanged.
Workers without `--isolation-read-dir` retain identical default confinement behavior.

## Security

- Allowlist paths originate strictly from the worker operator's local CLI flags, environment, or
  `rusty-grid.toml`; remote submitters cannot pass or override `isolation_read_dirs`.
- Validation blocks user-home roots, OS profile containers, credential folders, sandbox/workspace
  subtrees, and symlink/junction redirects before policy construction or Windows DACL updates.
- Writes remain denied on all allowlisted trees across Seatbelt, Landlock, and AppContainer.

## Test plan

- **Unit tests**:
  - `crates/worker/src/isolation.rs`: `test_validate_extra_read_only_dirs_allows_narrow_runtime_and_refuses_broad_or_secret_paths`,
    `test_validate_extra_read_only_dirs_refuses_symlinks_and_junctions`, and updated Seatbelt
    `profile`/`arguments` tests.
  - `crates/worker/src/isolation/linux.rs`: `test_read_only_grants_include_extra_read_only_dirs_and_carve_protected_files`.
  - `crates/worker/src/client/tests.rs`: `test_worker_client_plumbs_and_validates_isolation_read_dirs`.
  - `crates/cli/src/config.rs` and `crates/cli/src/cli_app.rs`:
    `test_worker_config_parses_isolation_read_dirs_from_toml`,
    `test_worker_isolation_read_dir_flags_and_precedence`.
- **Integration & E2E tests**:
  - `crates/worker/tests/test_feat3_os_isolation.rs`:
    `test_extra_read_only_dir_inside_home_is_readable_while_secrets_stay_denied` on macOS
    (`fs` and `fs+net`), Linux (`fs` and `fs+net` when Landlock is available), and Windows
    (AppContainer).
  - `tests/cli_isolation_e2e.rs`: `test_isolated_worker_refuses_broad_or_secret_isolation_read_dir`.
- **CI / hygiene gates**:
  - `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`,
    `cargo clippy --workspace --all-targets --features gpu-wgpu,3d --locked -- -D warnings`,
    `ox-mode doc verify`, `tests/test_m1_challenger_hygiene.py`, `tests/test_launcher_scripts.py`,
    and live temporary-`HOME` Windows AppContainer CUDA loopback verification.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| `R20` | Spike: a GPU inside the sandbox on each system (`design 0064` D3) | `docs/design/0074-external-runtime-allowlist-and-gpu-workloads.md`, `docs/design/0064-research-of-2026-11-13-and-route.md` | Recorded for macOS, Windows, and Linux (`--isolation off` vs `fs` / `fs+net` plus minimal rule where needed) | Partial (macOS and Windows recorded; Linux Planned) |
| `FEAT-3e` | External read-only runtime directory allowlist (`--isolation-read-dir`, `RUSTY_GRID_ISOLATION_READ_DIRS`, `[worker].isolation_read_dirs`) and GPU workload guide | `crates/worker/src/{isolation.rs, isolation/linux.rs, client.rs, lib.rs, client/tests.rs}`, `crates/worker/tests/test_feat3_os_isolation.rs`, `crates/cli/src/{config.rs, cli_app.rs}`, `tests/cli_isolation_e2e.rs`, `docs/guides/gpu-workloads.md`, `README.md`, `docs/README.md`, `CHANGELOG.md` | Unit, AppContainer/Seatbelt/Landlock integration, CLI E2E, and live Windows AppContainer CUDA loopback probes pass with home/secret reads and runtime writes denied | Implemented |

## Status log

- 2026-11-16: Authored design `0074` (`FEAT-3e`), added `--isolation-read-dir` /
  `RUSTY_GRID_ISOLATION_READ_DIRS` / `[worker].isolation_read_dirs` across worker and CLI, added
  `docs/guides/gpu-workloads.md`, updated `README.md` / `docs/design/0033` / `docs/design/0064`,
  and recorded live `NVIDIA GeForce RTX 5060 Ti` AppContainer CUDA pass (`Exit Code: 0`) under
  `--isolation fs --isolation-read-dir` alongside control failure (`Exit Code: 1`) without it.
