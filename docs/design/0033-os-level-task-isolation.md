# 0033: OS-level task isolation

Status: Draft
Created: 2026-11-08    Updated: 2026-11-08

## Summary

- Adds `--isolation <off|fs|fs+net>` (`RUSTY_GRID_ISOLATION`, `[worker].isolation`, default `off`) to `oxideswarm worker` (`FEAT-3` in [BACKLOG.md](../BACKLOG.md) and Phase B of [ROADMAP.md](../ROADMAP.md)).
- Under `fs`, the worker restricts each spawned task process using Landlock (`landlock_restrict_self`) on Linux, `sandbox-exec` on macOS, and an AppContainer / Restricted Token with explicit deny ACLs on Windows so the task can read and write its own sandbox directory while reads of `cluster.secret`, other tasks' sandboxes, and the worker blob cache fail, and writes outside the sandbox fail.
- Under `fs+net`, the worker applies all `fs` restrictions and additionally blocks outbound and inbound network sockets using `CLONE_NEWNET` / Landlock TCP + `seccomp` socket filtering on Linux, `(deny network*)` in `sandbox-exec` on macOS, and AppContainer zero network capabilities on Windows.
- Every worker advertises its active isolation level as a reserved `iso:<off|fs|fs+net>` tag (`crates/core/src/placement.rs`), tasks can require a minimum level via `require:iso=<fs|fs+net>`, and `oxideswarm doctor` reports each worker's isolation level.

## Context

What exists at `master` `01a9786`:

| Area | Today | Code |
|---|---|---|
| Sandbox directory | `Sandbox::create` creates `<base>/<task_id>` with mode `0700` on Unix, and `sanitize_relative_path` keeps staged inputs and declared outputs inside `<base>/<task_id>` | `crates/worker/src/sandbox.rs:119-160, 405-520` |
| Task process credentials | `execute_process_supervised` strips `SECRET_ENV_VARS` from the child's environment (`crates/worker/src/runner.rs:2607-2609`), but the child runs as the worker's OS user with no Landlock, `seccomp`, `sandbox-exec`, AppContainer, or restricted token | `crates/worker/src/process_tree.rs:121-151`, `crates/worker/src/runner.rs:2558-2666` |
| Host secrets and sibling state | A task command (`Command` or `ShellScript`) can open `~/.oxideswarm/cluster.secret`, `~/.oxideswarm/p2p_secret.key`, sibling task directories under `<base>`, or `<base>/cache/blobs`, and can open arbitrary outbound TCP/UDP connections | `crates/core/src/cluster_auth.rs`, `crates/worker/src/blob_cache.rs` |
| Worker tags and `doctor` | `RESERVED_TAG_PREFIXES` has six prefixes (`os:`, `arch:`, `slots:`, `link:`, `id:`, `state:`). Neither `WorkerSummary` nor `oxideswarm doctor` reports task isolation | `crates/core/src/placement.rs:24-44`, `crates/cli/src/doctor.rs:108-160, 201-251` |

This matters before Phase C ([design 0039](0039-agent-workspaces.md)) enables persistent agent workspaces: a coding agent submits arbitrary build and test commands, and without OS-level isolation a task can read `cluster.secret` or mutate files outside its sandbox.

## Goals

- **G1** With `--isolation fs`, a task process on Linux, macOS, or Windows cannot read `~/.oxideswarm` (`cluster.secret`, `p2p_secret.key`), other tasks' sandboxes under `<base>`, or the worker's blob cache `<base>/cache/blobs`, and cannot write any file outside its own sandbox directory (`<base>/<task_id>`) and task-scoped temporary directory.
- **G2** With `--isolation fs`, normal build and test tools (`sh`, `bash`, `cmd.exe`, `powershell`, `cargo`, `rustc`, `python3`) can still read system executables, shared libraries, and toolchains, and can create, read, and write files inside the task's sandbox.
- **G3** With `--isolation fs+net`, all `fs` rules hold and any attempt by the task to open an outbound TCP or UDP connection fails on Linux, macOS, and Windows.
- **G4** If an operator starts a worker with `--isolation fs` or `--isolation fs+net` on a host where the OS kernel cannot enforce that level, the worker fails fast at start-up with a clear error rather than running tasks unsandboxed.
- **G5** Every worker advertises `iso:off`, `iso:fs`, or `iso:fs+net`, placement supports `require:iso=<fs|fs+net>`, and `oxideswarm doctor` displays the level for each worker.

## Non-goals

- Hardware virtualization (KVM, Hyper-V, Virtualization.framework) or container image management.
- Enforcing isolation on in-process `BuiltinTest` or in-process `GpuCompute` workloads (which execute inside the worker process rather than spawning a child process).
- Changing the default (`off`) in this item before workspace defaults are switched in Phase C.

## Design

### D1 Isolation levels, configuration, and start-up self-check (`crates/worker/src/isolation.rs`)

1. New type `IsolationLevel` in `crates/worker/src/isolation.rs` (re-exported in `rusty_grid_core::placement`):
   - `IsolationLevel::Off` (`"off"`, default)
   - `IsolationLevel::Fs` (`"fs"`)
   - `IsolationLevel::FsNet` (`"fs+net"`)
2. `WorkerConfig` (`crates/worker/src/client.rs`) gains `pub isolation: IsolationLevel`.
3. `WorkerArgs` (`crates/cli/src/cli_app.rs`) gains `--isolation <off|fs|fs+net>`, `RUSTY_GRID_ISOLATION` environment variable support, and `WorkerFileConfig::isolation` (`[worker].isolation` in `oxideswarm.toml`), resolved with standard CLI > env > config > `off` precedence.
4. Inside `execute_process_supervised` (`crates/worker/src/runner.rs:2620-2645`), when `isolation != IsolationLevel::Off`, the worker also sets `HOME`, `USERPROFILE`, `TMPDIR`, `TEMP`, and `TMP` in the child environment to `sandbox_path` so standard libraries and compilers place temporary and dot-directory state inside the sandbox.
5. At worker start-up (`WorkerClient::run`), when `isolation != IsolationLevel::Off`, `verify_isolation_support(isolation, &sandbox_base_dir)` runs a one-shot self-check probe in a temporary sandbox directory before registering with the master. If the host kernel lacks the required primitive, start-up returns `Err(GridError::Config(...))`.

### D2 Linux implementation: Landlock, `seccomp`, and namespaces (`crates/worker/src/isolation.rs`)

Applied inside `configure_command`'s `pre_exec` hook (`crates/worker/src/process_tree.rs:126-137`) after `PR_SET_PDEATHSIG` and priority setup:

1. **No new privileges:** Calls `libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0)`.
2. **Filesystem isolation (`fs` and `fs+net`) via Landlock ABI v1+ (`SYS_landlock_create_ruleset`, `SYS_landlock_add_rule`, `SYS_landlock_restrict_self`) plus mount/path masking:**
   - Creates a Landlock ruleset handling all supported `LANDLOCK_ACCESS_FS_*` bits for the running kernel ABI (v1 through v5).
   - Adds read-write-execute rules (`FS_READ_WRITE | FS_EXECUTE`) for:
     - The canonical `sandbox_path` (and, when `0039` workspaces are active, the workspace's `tree/` and `tmp/` directories).
     - `/dev/null`, `/dev/zero`, `/dev/urandom`, `/dev/random`.
   - Adds read-only + execute rules (`FS_READ_ONLY | FS_EXECUTE`) for system and toolchain directories that exist on the host:
     - `/usr`, `/bin`, `/sbin`, `/lib`, `/lib32`, `/lib64`, `/etc/ld.so.cache`, `/etc/ld.so.conf`, `/etc/ld.so.conf.d`, `/etc/Alternatives`, `/etc/ssl`, `/etc/ca-certificates`, `/etc/resolv.conf`, `/etc/hosts`, `/etc/nsswitch.conf`, `/proc`, `/sys/devices/system/cpu`.
     - Read-only toolchain directories (`$RUSTUP_HOME` or `~/.rustup`, and `~/.cargo/bin`) when present.
   - Notably omitted from the Landlock allowlist: `~/.oxideswarm` (so `~/.oxideswarm/cluster.secret` and `p2p_secret.key` cannot be opened), `<sandbox_base>` (so sibling sandboxes `<sandbox_base>/<other_task_id>` and `<base>/cache/blobs` cannot be listed or opened), and the rest of the user's home directory.
   - Calls `SYS_landlock_restrict_self`.
3. **Network isolation (`fs+net`):**
   - First attempts `libc::unshare(libc::CLONE_NEWUSER | libc::CLONE_NEWNET)` to place the task in an unconfigured network namespace with no routes or interfaces.
   - Regardless of whether `CLONE_NEWUSER` is permitted by `kernel.unprivileged_userns_clone`, installs a strict `seccomp` BPF filter (`SECCOMP_SET_MODE_FILTER`) that returns `SECCOMP_RET_ERRNO | EACCES` for `socket(domain, ...)` when `domain` is `AF_INET` (`2`) or `AF_INET6` (`10`) (and on Landlock ABI v4+, also restricts `LANDLOCK_ACCESS_NET_BIND_TCP | LANDLOCK_ACCESS_NET_CONNECT_TCP`), guaranteeing that outbound IPv4/IPv6 TCP and UDP connections fail with `EACCES`.

### D3 macOS implementation: `sandbox-exec` Seatbelt profile (`crates/worker/src/isolation.rs`)

On macOS (`#[cfg(target_os = "macos")]`), when `isolation` is `Fs` or `FsNet`, `configure_command` / `execute_process_supervised` invokes `/usr/bin/sandbox-exec -p <profile> -D SANDBOX_DIR=<canonical_sandbox> -D SECRET_DIR=<canonical_oxideswarm_dir> -D SANDBOX_BASE=<canonical_sandbox_base> -D CACHE_DIR=<canonical_cache_dir> -- <resolved_program> <args...>`:

1. **`fs` Seatbelt profile:**
   ```text
   (version 1)
   (allow default)
   (deny file-write* (subpath "/"))
   (allow file-write*
       (subpath (param "SANDBOX_DIR"))
       (literal "/dev/null")
       (literal "/dev/zero")
       (literal "/dev/random")
       (literal "/dev/urandom")
       (literal "/dev/dtracehelper"))
   (deny file-read*
       (subpath (param "SECRET_DIR"))
       (subpath (param "CACHE_DIR"))
       (subpath (param "SANDBOX_BASE")))
   (allow file-read*
       (subpath (param "SANDBOX_DIR")))
   ```
   Because Seatbelt evaluates later matching rules with higher precedence, `(allow file-read* (subpath (param "SANDBOX_DIR")))` permits reading the task's own sandbox while `(deny file-read* (subpath (param "SANDBOX_BASE")))` blocks reading sibling sandboxes and `(deny file-read* (subpath (param "SECRET_DIR")))` blocks reading `~/.oxideswarm/cluster.secret`.
2. **`fs+net` Seatbelt profile:**
   Appends `(deny network*)` to the `fs` profile, blocking all outbound and inbound IPv4, IPv6, and non-sandbox Unix domain socket traffic.

### D4 Windows implementation: AppContainer and explicit deny ACLs (`crates/worker/src/isolation.rs`)

On Windows (`#[cfg(windows)]`), when `isolation` is `Fs` or `FsNet`:

1. **Filesystem isolation (`fs` and `fs+net`):**
   - At worker start-up (or per worker instance), the worker creates or derives a dedicated AppContainer SID (`CreateAppContainerProfile` / `DeriveAppContainerSidFromAppContainerName` in `userenv.dll`, with fallback to a Restricted Token `CreateRestrictedToken` holding a dedicated restriction SID and Low Mandatory Integrity Level `S-1-16-4096`).
   - On `~/.oxideswarm` (and any explicit `--secret-file` path), `<sandbox_base>`, and `<base>/cache`, the worker sets an explicit `EXPLICIT_ACCESS_W` `DENY_ACCESS` ACE for that isolation SID (inheritable to files and subdirectories) and marks those directories Medium Integrity (`NO_READ_UP | NO_WRITE_UP`).
   - On the task's own `sandbox_path` (`<sandbox_base>/<task_id>`), `Sandbox::create` grants `GENERIC_ALL` (`FILE_ALL_ACCESS`) to the task's isolation SID and sets Low Mandatory Integrity Level on `sandbox_path` so the isolated process can create, read, write, and execute files inside `sandbox_path`, while opening `cluster.secret`, sibling sandboxes, or the blob cache fails with `ERROR_ACCESS_DENIED` (`os error 5`), and writing outside `sandbox_path` fails with `ERROR_ACCESS_DENIED`.
2. **Network isolation (`fs+net`):**
   - In `AppContainer` mode (`UpdateProcThreadAttribute` with `PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES`), `SecurityCapabilities.Capabilities` is passed as empty (`CapabilityCount = 0`) for `fs+net` ( blocking `internetClient` and `privateNetworkClientServer` in the Windows kernel so outbound TCP/UDP `connect`/`sendto` fail with `WSAEACCES`), whereas `fs` mode includes `SID_INTERNET_CLIENT` (`S-1-15-3-1`) and `SID_PRIVATE_NETWORK_CLIENT_SERVER` (`S-1-15-3-3`) so only filesystem isolation is enforced under `fs`.

### D5 Reserved `iso:` worker tag and placement constraint (`crates/core/src/placement.rs`)

1. `crates/core/src/placement.rs`:
   - Add `pub const ISO_TAG_PREFIX: &str = "iso:";` and include `ISO_TAG_PREFIX` in `RESERVED_TAG_PREFIXES` (`[&str; 7]`).
   - Every worker advertises `iso:off`, `iso:fs`, or `iso:fs+net` in `WorkerCapabilities::tags`.
   - `Placement` gains `pub isolation: Option<IsolationLevel>`, parsed from `require:iso=off`, `require:iso=fs`, or `require:iso=fs+net`.
   - In `Placement::matches` (`crates/core/src/placement.rs`), a worker with `iso:<w_level>` satisfies `require:iso=<req_level>` when `w_level >= req_level` (`Off < Fs < FsNet`). A worker from an older build without an `iso:` tag is treated as `IsolationLevel::Off`.
2. `crates/cli/src/cli_app.rs` (`SubmitArgs`) gains `--require-isolation <fs|fs+net>` (emitting `require:iso=<level>`), and MCP `submit` (`crates/cli/src/mcp.rs`) gains optional `isolation: "fs" | "fs+net"`.

### D6 `oxideswarm doctor` reporting (`crates/cli/src/doctor.rs`)

1. `WorkerSummary` (`crates/cli/src/doctor.rs:108`) gains `pub isolation: String`, populated from `tag_value(&caps.tags, ISO_TAG_PREFIX).unwrap_or("off")`.
2. `DoctorReport::render` (`crates/cli/src/doctor.rs:201`) adds an `ISO` column to the `Workers` table (`off`, `fs`, or `fs+net`) and includes `iso:<level>` in the `C4 Process probe` detail string so both human output and `--json` output report the active isolation level of every worker.

## Compatibility

- Wire format: unchanged. `iso:<level>` and `require:iso=<level>` travel in the existing `WorkerCapabilities::tags` and `Task::tags` `Vec<String>` fields.
- Mixed versions: a worker without an `iso:` tag is treated as `iso:off`. Existing tasks without `require:iso=...` run on any worker regardless of its isolation level.

## Security

- Blocks untrusted or agent-submitted task commands from reading `~/.oxideswarm/cluster.secret`, `~/.oxideswarm/p2p_secret.key`, sibling task sandboxes, and the worker blob cache under `fs` and `fs+net`.
- Blocks task commands from modifying files outside their sandbox directory under `fs` and `fs+net`.
- Blocks data exfiltration over outbound TCP/UDP sockets under `fs+net`.
- `ISO_TAG_PREFIX` (`iso:`) is added to `RESERVED_TAG_PREFIXES` so an operator cannot spoof `iso:fs` via `--tag`.

## Test plan

- **Unit tests (`crates/core/src/placement.rs`, `crates/worker/src/isolation.rs`, `crates/cli/src/doctor.rs`):**
  - `iso:` is rejected by `validate_user_tag`.
  - `Placement::from_task_tags` parses `require:iso=fs` and `require:iso=fs+net`, rejects unknown values, and matches `iso:fs+net` against `require:iso=fs`.
  - `DoctorReport` renders the `ISO` column and serializes `isolation` in JSON.
- **Cross-platform integration tests (`crates/worker/tests/test_feat3_os_isolation.rs`, "Done when" of `FEAT-3`):**
  1. **`fs` isolation test (runs on Linux, macOS, and Windows in CI):**
     - Creates a dummy `cluster.secret` in a temporary `~/.oxideswarm/cluster.secret` and a sibling sandbox file `<sandbox_base>/other_task/secret.txt`.
     - Starts a worker (or `TaskRunner`) with `IsolationLevel::Fs`.
     - Submits a task that (a) writes `allowed.txt` inside its own sandbox (`$RUSTY_GRID_SANDBOX_DIR/allowed.txt`) and reads it back, and (b) attempts to read `~/.oxideswarm/cluster.secret`, `<sandbox_base>/other_task/secret.txt`, and write outside its sandbox.
     - Asserts that writing and reading `allowed.txt` inside the sandbox succeeds, while reading `cluster.secret`, reading the sibling sandbox file, and writing outside the sandbox all fail with permission/access denied.
  2. **`fs+net` isolation test (runs on Linux, macOS, and Windows in CI):**
     - Binds a local `TcpListener` on `127.0.0.1:0` in the test process.
     - Runs a task under `IsolationLevel::FsNet` that attempts to open a TCP connection to that listener's `127.0.0.1:<port>`.
     - Asserts that the outbound connection fails inside the task and the listener accepts no connection.
  3. **`doctor` reporting test (`tests/doctor_e2e.rs`):**
     - Starts a worker with `--isolation fs`, runs `oxideswarm doctor`, and asserts that the output and JSON report `iso:fs`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| `FEAT-3` | Add `--isolation off\|fs\|fs+net`, Linux Landlock/`seccomp`/namespaces, macOS `sandbox-exec`, Windows AppContainer/Restricted Token + ACLs, `iso:` tag, and `doctor` reporting | `crates/core/src/placement.rs`, `crates/worker/src/isolation.rs`, `crates/worker/src/process_tree.rs`, `crates/worker/src/runner.rs`, `crates/worker/src/sandbox.rs`, `crates/cli/src/cli_app.rs`, `crates/cli/src/config.rs`, `crates/cli/src/doctor.rs`, `crates/cli/src/mcp.rs`, `crates/worker/tests/test_feat3_os_isolation.rs`, `README.md`, `docs/README.md` | With `fs`, a probe that reads `cluster.secret` fails on each OS and writing the sandbox works; with `fs+net`, an outbound connection fails; `doctor` reports the level | Planned |

## Status log

- 2026-11-08: Initial draft written for `FEAT-3` (Phase B of `docs/ROADMAP.md`).
