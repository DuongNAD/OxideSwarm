# 0033: OS-level task isolation

Status: Accepted
Created: 2026-11-08    Updated: 2026-11-08

## Summary

- Adds `--isolation <off|fs|fs+net>` (`RUSTY_GRID_ISOLATION`, `[worker].isolation`, default `off`) to `oxideswarm worker` (`FEAT-3` in [BACKLOG.md](../BACKLOG.md) and Phase B of [ROADMAP.md](../ROADMAP.md)).
- Under `fs`, the worker enforces one uniform filesystem policy across Linux (Landlock), macOS (`sandbox-exec`), and Windows (a per-task AppContainer SID): system directories and the host's `$RUSTUP_HOME` / `$CARGO_HOME` toolchains are read-only, the task's own sandbox is writable, and the rest of the user's home directory (`~/.oxideswarm`, `~/.ssh`, `~/.aws`), sibling sandboxes, and the blob cache are denied.
- Under `fs+net`, the worker applies all `fs` rules and blocks outbound and inbound network sockets using `CLONE_NEWNET` + Landlock TCP + `seccomp` (every socket domain but `AF_NETLINK`, and `io_uring_*`) on Linux, `(deny network*)` in `sandbox-exec` on macOS, and AppContainer zero network capabilities on Windows.
- Every worker advertises its active isolation level as a reserved `iso:<off|fs|fs+net>` tag (`crates/core/src/placement.rs`), tasks can require a minimum level via `require:iso=<fs|fs+net>`, and `oxideswarm doctor` reports each worker's isolation level.

## Context

What exists at `master` `01a9786`:

| Area | Today | Code |
|---|---|---|
| Sandbox directory | `Sandbox::create` creates `<base>/<task_id>` with mode `0700` on Unix, and `sanitize_relative_path` keeps staged inputs and declared outputs inside `<base>/<task_id>` | `crates/worker/src/sandbox.rs:119-160, 405-520` |
| Task process credentials | `execute_process_supervised` strips `SECRET_ENV_VARS` from the child's environment (`crates/worker/src/runner.rs:2607-2609`), but the child runs as the worker's OS user with no Landlock, `seccomp`, `sandbox-exec`, AppContainer, or restricted token | `crates/worker/src/process_tree.rs:121-151`, `crates/worker/src/runner.rs:2558-2666` |
| Host secrets and sibling state | A task command (`Command` or `ShellScript`) can open `~/.oxideswarm/cluster.secret`, `~/.oxideswarm/p2p_secret.key`, `~/.ssh/id_*`, sibling task directories under `<base>`, or `<base>/cache/blobs`, can read `/proc/<worker_pid>/environ` on Linux, and can open arbitrary outbound TCP/UDP or `SSH_AUTH_SOCK` connections | `crates/core/src/cluster_auth.rs`, `crates/worker/src/blob_cache.rs` |
| Worker tags and `doctor` | `RESERVED_TAG_PREFIXES` has six prefixes (`os:`, `arch:`, `slots:`, `link:`, `id:`, `state:`). Neither `WorkerSummary` nor `oxideswarm doctor` reports task isolation | `crates/core/src/placement.rs:24-44`, `crates/cli/src/doctor.rs:108-160, 201-251` |

This matters before Phase C ([design 0039](0039-agent-workspaces.md)) enables persistent agent workspaces: a coding agent submits arbitrary build and test commands, and without OS-level isolation a task can read `cluster.secret`, `~/.ssh`, or another task's sandbox.

## Goals

- **G1** With `--isolation fs`, all three platforms (Linux, macOS, Windows) enforce the same filesystem policy:
  - **Writable:** only the task's own sandbox directory (`<base>/<task_id>`, plus `<workspace>/tree` and `<workspace>/tmp` when [design 0039](0039-agent-workspaces.md) workspaces are active) and standard null/random character devices.
  - **Readable and executable:** system OS directories and the host's Rust toolchain and Cargo cache (`$RUSTUP_HOME` and `$CARGO_HOME`, excluding `$CARGO_HOME/credentials` and `$CARGO_HOME/credentials.toml`).
  - **Denied (both read and write):** the rest of the user's home directory (`$HOME` / `%USERPROFILE%`, including `~/.oxideswarm`, `~/.ssh`, `~/.aws`, `~/.gnupg`, `~/.config`, and other repositories), sibling task sandboxes under `<base>`, the worker blob cache `<base>/cache/blobs`, and `/proc/<worker_pid>` on Linux.
- **G2** With `--isolation fs`, normal build and test tools (`sh`, `bash`, `cmd.exe`, `powershell`, `cargo build --offline`, `rustc`, `python3`) run on a default `rustup` installation and can build a crate whose dependencies are already in `$CARGO_HOME/registry`.
- **G3** With `--isolation fs+net`, all `fs` rules hold and any attempt by the task to open an outbound IPv4 or IPv6 TCP or UDP connection (including via Linux `io_uring`) fails on Linux, macOS, and Windows.
- **G4** If an operator starts a worker with `--isolation fs` or `--isolation fs+net` on a host where the OS kernel cannot enforce that level, the worker fails fast at start-up with a clear error rather than running tasks unsandboxed.
- **G5** Every worker advertises `iso:off`, `iso:fs`, or `iso:fs+net`, placement supports `require:iso=<fs|fs+net>`, and `oxideswarm doctor` displays the level for each worker.

## Non-goals

- Hardware virtualization (KVM, Hyper-V, Virtualization.framework) or container image management.
- Enforcing isolation on in-process `BuiltinTest` or in-process `GpuCompute` workloads (which execute inside the worker process rather than spawning a child process).
- Changing the default (`off`) in this item before workspace defaults are switched in Phase C.

## Design

### D1 Isolation levels, environment scrubbing, toolchain preservation, and start-up self-check (`crates/worker/src/isolation.rs`)

1. New type `IsolationLevel`, defined in `crates/core/src/placement.rs` because placement needs it and `core` cannot depend on `worker`, and re-exported from `crates/worker/src/isolation.rs` (FEAT-3a implementation note; the design first put it in the worker):
   - `IsolationLevel::Off` (`"off"`, default)
   - `IsolationLevel::Fs` (`"fs"`)
   - `IsolationLevel::FsNet` (`"fs+net"`)
2. `WorkerConfig` (`crates/worker/src/client.rs`) gains `pub isolation: IsolationLevel`.
3. `WorkerArgs` (`crates/cli/src/cli_app.rs`) gains `--isolation <off|fs|fs+net>`, `RUSTY_GRID_ISOLATION` environment variable support, and `WorkerFileConfig::isolation` (`[worker].isolation` in `oxideswarm.toml`), resolved with standard CLI > env > config > `off` precedence.
4. **Environment preparation under `fs` and `fs+net` (`crates/worker/src/runner.rs:2605-2630`):**
   - **Preserve `RUSTUP_HOME` and `CARGO_HOME` before moving `HOME`:** `~/.cargo/bin/cargo` is a `rustup` proxy that locates toolchains via `$RUSTUP_HOME` (falling back to `$HOME/.rustup`) and reads `$CARGO_HOME` (falling back to `$HOME/.cargo`). Before changing `HOME` / `USERPROFILE`, the worker resolves the host's real `RUSTUP_HOME` (`$RUSTUP_HOME` if set, else `<real_home>/.rustup`) and real `CARGO_HOME` (`$CARGO_HOME` if set, else `<real_home>/.cargo`) and sets `RUSTUP_HOME` and `CARGO_HOME` explicitly in the task's environment if the task did not override them.
   - **Read-only `$CARGO_HOME` policy (Option a):** Under `fs` and `fs+net`, the host's `$RUSTUP_HOME` and `$CARGO_HOME` (`bin/`, `registry/`, `git/`, `config.toml`, excluding `credentials` and `credentials.toml`) are mounted/granted **read-only + execute**. Offline builds (`cargo build --offline` or `cargo test --offline`) against dependencies already cached and unpacked in `$CARGO_HOME/registry` succeed without write access and cannot poison another task's shared Cargo cache. A task under `fs` that wants to download new crates can set `CARGO_HOME=$RUSTY_GRID_SANDBOX_DIR/.cargo`.
   - **Move `HOME` and temp directories into the sandbox:** Set `HOME`, `USERPROFILE`, `TMPDIR`, `TEMP`, and `TMP` in the child environment to `sandbox_path`.
   - **Strip local IPC agent sockets:** Remove `SSH_AUTH_SOCK`, `GPG_AGENT_INFO`, `DBUS_SESSION_BUS_ADDRESS`, and `DOCKER_HOST` from the child environment in addition to `SECRET_ENV_VARS`.
   - **Allowlist the inherited environment (security review of FEAT-3a):** the child starts from an empty environment, not a copy of the worker's, so a `GITHUB_TOKEN` or cloud credential in the worker's environment never reaches a task. Inherited from the worker: `PATH`, `LANG`, `LC_*`, `TERM`, `TZ`, `USER`, `LOGNAME`, `SHELL`, `COLORTERM`, `NO_COLOR`, `DEVELOPER_DIR`, `SDKROOT`, `RUSTUP_TOOLCHAIN` (`INHERITED_ENV_VARS`). Added by the worker: the task's own `env`, the moved `HOME` / `USERPROFILE` / `TMPDIR` / `TEMP` / `TMP`, `RUSTUP_HOME`, `CARGO_HOME`, `RUSTY_GRID_TASK_ID`, `RUSTY_GRID_WORKER_ID`, `RUSTY_GRID_SANDBOX_DIR` and the containment variable `RUSTY_GRID_RUN_TOKEN` (design 0031).
   - **No secret through the environment or argv:** a worker with `fs` or `fs+net` refuses to start (exit code 6) when the cluster secret is given as `OXIDESWARM_SECRET` or `--secret`; see Security.
5. **Start-up self-check (`WorkerClient::run`):** When `isolation != IsolationLevel::Off`, `verify_isolation_support(isolation, &sandbox_base_dir)` runs a one-shot self-check probe in a temporary sandbox directory before registering with the master. If the host kernel lacks the required primitive, start-up returns `Err(GridError::Config(...))`.

### D2 Linux implementation: Landlock, `seccomp`, and namespaces (`crates/worker/src/isolation/linux.rs`)

There is no wrapper program on Linux: `wrap_program` returns the program unchanged and `confine_command` registers a `pre_exec` hook. The policy is built **before `spawn`** (path descriptors opened with `O_PATH | O_CLOEXEC`, the BPF program as a `Vec<SockFilter>`, the id-map texts) and the hook only makes raw system calls: no allocation, no locks, no logging. The runner drops the `Command` right after `spawn`, which closes the descriptors.

**Ordering with design 0031 (`FEAT-2`) in `Command::pre_exec`:** `confine_command` is called right after `configure_command`, so its hook runs after FEAT-2's (hooks run in registration order):

1. FEAT-2's hook sets the CPU `nice`, double-forks the process-group supervisor C and reaps B, and moves the task leader A into its cgroup v2 leaf.
2. The isolation hook runs in A, right before `execve`. Because it runs **after** step 1, supervisor C is never restricted by the task's Landlock domain or `seccomp` filter and keeps full access to `/proc` and `<leaf>/cgroup.kill`.

**What the hook does, in order:**

1. `prctl(PR_SET_NO_NEW_PRIVS, 1)`.
2. `fs+net` only, best effort: `unshare(CLONE_NEWUSER | CLONE_NEWNET)` (an empty network namespace), then it writes `deny` to `/proc/self/setgroups` and the identity maps `<uid> <uid> 1` / `<gid> <gid> 1`, so the task keeps its numeric ids (`git` refuses repositories owned by another uid). Failures are ignored.
3. Landlock (`landlock_create_ruleset`, `landlock_add_rule` for each prepared descriptor, `landlock_restrict_self`). The ruleset handles every filesystem right of the running ABI (v1 `EXECUTE`..`MAKE_SYM`, v2 `REFER`, v3 `TRUNCATE`, v5 `IOCTL_DEV`); under `fs+net` also `BIND_TCP | CONNECT_TCP` from ABI 4 with no rule; from ABI 6 also the `ABSTRACT_UNIX_SOCKET` and `SIGNAL` scopes. Rules:
   - **Read-write-execute (everything the ABI handles, directories; for a file only the file rights):** the canonical task sandbox.
   - **Read-write on devices:** `/dev/null`, `/dev/zero`, `/dev/urandom`, `/dev/random`.
   - **Read and execute (`EXECUTE | READ_FILE | READ_DIR`, files without `READ_DIR`):** `/usr`, `/bin`, `/sbin`, `/lib`, `/lib32`, `/lib64`, `/libx32`, `/opt`, `/etc/{ld.so.cache,ld.so.conf,ld.so.conf.d,alternatives,ssl,ca-certificates,pki,resolv.conf,hosts,host.conf,nsswitch.conf,gai.conf,services,protocols,passwd,group,localtime,os-release,terminfo}`, `/sys/devices/system/cpu`, `/proc/{cpuinfo,meminfo,stat,version,loadavg}`, `/proc/sys/kernel`, `$RUSTUP_HOME`, and `$CARGO_HOME/{bin,registry,git,config.toml,config}` (never `$CARGO_HOME` itself, so `credentials` and `credentials.toml` stay out). Paths that do not exist are skipped. (`/etc/passwd`, `/etc/group` and `/etc/localtime` were added to the first draft's list: `ls -l`, `whoami` and `git` need them.)
   - **The task leader's own `/proc/<pid>`** (`FS_READ_FILE | FS_READ_DIR`): the hook opens `/proc/self` *in the child*, so it names the task and not the worker (opened by the parent it would grant the worker's `/proc/<worker_pid>`).
   - **Protected files are cut out of granted directories.** Landlock can only allow, never carve a hole, so for each read-only root that contains a protected file (`WorkerConfig::protected_files`: the cluster secret file and the config file in use) the policy grants the root's other entries one by one, recursively; symbolic links are skipped. A secret file inside `$RUSTUP_HOME` or `/opt` stays unreadable while its siblings stay readable.
   - Everything else is denied for read and write: `$HOME` (`~/.oxideswarm`, `~/.ssh`, `~/.aws`, `~/.config`), `<sandbox_base>` (sibling sandboxes), `<base>/cache/blobs`, `/tmp`, `/run`, `/var`, `/home`, `/root`, `/dev` (but the four devices) and `/proc` (but the entries above). A task that reaches a denied file through a symbolic link or a hard link is denied too (Landlock checks the resolved file; linking needs `REFER` on the source).
4. `seccomp` (`prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER)`), a hand-built classic-BPF array (about 20 instructions, no dependency), the same at both levels except for sockets:
   1. `seccomp_data.arch` must be the native `AUDIT_ARCH_X86_64` / `AUDIT_ARCH_AARCH64`, and on `x86_64` the x32 bit must be clear, otherwise `SECCOMP_RET_KILL_PROCESS`. A 32-bit executable is therefore killed (`SIGSYS`) at its first system call; the filter matches native numbers only.
   2. `io_uring_setup`, `io_uring_enter` and `io_uring_register` fail with `ENOSYS`, at both levels: `IORING_OP_SOCKET` (Linux 5.19+) could otherwise create the sockets the next rule refuses.
   3. `ioctl(_, TIOCSTI, _)` fails with `EPERM` (Linux 6.2+ disables it by default, a sysctl can re-enable it; keystroke injection into the controlling terminal).
   4. `socket(domain, ...)` fails with `EACCES` for `AF_UNIX` at `fs` and for **every domain except `AF_NETLINK`** at `fs+net`. `socketpair` stays allowed: it cannot reach anything outside. The first draft closed `AF_INET`/`AF_INET6` only and relied on omitting `/run` and `/tmp` for Unix sockets; see the correction below.
5. The Landlock ABI is read at start-up with `landlock_create_ruleset(NULL, 0, LANDLOCK_CREATE_RULESET_VERSION)` (`rusty_grid_worker::isolation::landlock_abi`).

**Start-up self-check on Linux (`verify_isolation_support`):** the worker refuses to start when Landlock is missing (`ENOSYS`: kernel older than 5.13 or built without it; `EOPNOTSUPP`: built in but not in the `lsm=` list) or the architecture is not `x86_64`/`aarch64`. It then runs `/bin/sh` through the real hook in a temporary directory below the sandbox base: the probe must write inside the directory, fail to write beside it, and read `Seccomp: 2` from `/proc/self/status`. A host where the policy cannot be applied therefore never registers. The hook failing for a task fails that task (`Cannot run '<program>' with --isolation ...`); it never runs plain.

**Kernel features and what a lower ABI loses** (only ABI 1 is required; each later ABI adds a protection):

| Linux | Landlock ABI | Adds | Without it |
|---|---|---|---|
| 5.13 | 1 | file access rights, `ptrace` restriction | start-up refuses below this |
| 5.19 | 2 | `REFER` (rename and link across directories) | cross-directory `rename` inside the sandbox fails with `EXDEV` |
| 6.2 | 3 | `TRUNCATE` | `truncate(2)` of a file the task owns inside a read-only toolchain directory is possible (start-up logs a warning) |
| 6.7 | 4 | TCP bind/connect rules | none: `seccomp` does the network work |
| 6.10 | 5 | `IOCTL_DEV` | none that matters: devices are not opened; `TIOCSTI` is in `seccomp` |
| 6.12 | 6 | signal and abstract-Unix-socket scopes | a task can signal the worker and every process of the same user (start-up logs a warning) |

`seccomp` filters and `PR_SET_NO_NEW_PRIVS` are older than any Landlock kernel. `unshare` needs `CONFIG_USER_NS`; hosts that forbid it keep `seccomp` and Landlock.

**What was verified on `ubuntu-latest` (FEAT-3b, 2026-11-08, Linux 7.0 azure, Landlock ABI 8, `kernel.apparmor_restrict_unprivileged_userns = 1`, `kernel.io_uring_disabled = 0`), and what it changed:**

- *Landlock does not mediate `connect` to a pathname Unix socket* (it hooks file opens and path operations, and no right of the ABIs up to 8 covers the lookup a Unix `connect` does). Omitting `/run` and `/tmp` from the allowlist, as the first draft said, does **not** stop a task from reaching `docker.sock`, an ssh-agent or the D-Bus bus. The test `test_a_listening_unix_socket_outside_the_sandbox_cannot_be_reached` shows an unconfined task connecting and a confined one refused; the refusal is `seccomp` denying `socket(AF_UNIX)`. The cost: a confined task cannot create `AF_UNIX` sockets of its own (a test server on a Unix socket, `multiprocessing.Manager`, `systemd-notify`); pipes and `socketpair` work.
- *Descendants do not get `/proc/self`.* The rule names the leader's directory, so `sh` can read `/proc/self/status` but `rustc` (a grandchild) cannot read its own `/proc/self/maps`; programs treat that as the absence of an optional file (`cargo build --offline` works).
- *The `/proc/<worker_pid>/environ` of the worker is protected twice*: by the narrow `/proc` rule, and by Landlock's `ptrace` hook, which refuses `ptrace`, `process_vm_readv` and the `/proc/<pid>/{environ,mem,fd,...}` files of any process outside the task's domain. `/proc/<pid>/cmdline` of other processes is denied by the narrow rule (it is world-readable otherwise).
- *AppArmor's userns restriction (Ubuntu 24.04+)* lets `unshare(CLONE_NEWUSER | CLONE_NEWNET)` succeed but strips the capabilities in the new namespace, so the id maps cannot be written: the task is in an empty network namespace and sees its ids as `65534`. With the sysctl off (or on other distributions) the identity map works and the task keeps its ids. Both paths pass the whole test suite, and CI runs it both ways.
- *Unconfined control tests* for every case above (`LEAK_*`) succeed on the same runner, so each denial is the policy and not the environment.
- *Signals* are scoped on ABI 6+: a task gets `EPERM` for `kill` on the worker and on foreign processes of the user, and can still signal its own children; on ABI < 6 the test only checks the children.

### D3 macOS implementation: `sandbox-exec` Seatbelt profile (`crates/worker/src/isolation.rs`)

On macOS (`#[cfg(target_os = "macos")]`), when `isolation` is `Fs` or `FsNet`, `execute_process_supervised` invokes `/usr/bin/sandbox-exec -p <profile>` with canonicalized `-D` parameters (`SANDBOX_DIR`, `USER_HOME`, `SANDBOX_BASE`, `CACHE_DIR`, `RUSTUP_HOME`, `CARGO_HOME`, `CARGO_CRED`, `CARGO_CRED_TOML`):

1. **`fs` Seatbelt profile (same read and write policy as Linux and Windows):**
   ```text
   (version 1)
   (allow default)
   (deny file-write* (subpath "/"))
   (allow file-write*
       (subpath (param "SANDBOX_DIR"))
       (literal "/dev/null") (literal "/dev/zero") (literal "/dev/random")
       (literal "/dev/urandom") (literal "/dev/dtracehelper")
       (literal "/dev/stdout") (literal "/dev/stderr") (literal "/dev/tty")
       (subpath "/dev/fd"))
   (deny file-read*
       (subpath (param "USER_HOME"))
       (subpath (param "SANDBOX_BASE"))
       (subpath (param "CACHE_DIR")))
   (allow file-read*
       (subpath (param "SANDBOX_DIR"))
       (subpath (param "RUSTUP_HOME"))
       (subpath (param "CARGO_HOME")))
   (deny file-read*
       (literal (param "CARGO_CRED"))
       (literal (param "CARGO_CRED_TOML")))
   (allow file-read-metadata
       (literal (param "ANCESTOR_0")) (literal (param "ANCESTOR_1")) ...)
   ```
   The `ANCESTOR_<n>` parameters are generated per task (see the second correction below). `RUSTUP_HOME` and `CARGO_HOME` in the profile are always the host's own directories, never a value from the task's environment.
2. **`fs+net` Seatbelt profile:** appends `(deny network*)` to the `fs` profile.
3. **Invocation:** `/usr/bin/sandbox-exec -p <profile> -D NAME=VALUE ... -- <program> <args>`, with the program resolved against the task's `PATH` first (a missing program fails the task with a clear message). `sandbox-exec` applies the profile and `exec`s the program in place, so the PID the worker tracks is the task itself.
4. **Start-up probe:** `verify_isolation_support` runs `/bin/sh` under the profile in a temporary directory below the sandbox base. It must write inside the sandbox and must fail to write next to it, so a host where `sandbox-exec` is missing, refuses to nest (the worker itself runs inside a sandbox) or does not enforce the profile stops at start-up.

**What was verified on macOS (FEAT-3a, 2026-11-08, Darwin 27.2, Apple silicon), and what it changed in this section:**

- *Rule precedence is last-match-wins, as the design assumed.* With the profile order above, `cat` of `$USER_HOME/.ssh/key`, of a sibling sandbox file and of `$CARGO_HOME/credentials` fails with `Operation not permitted`, while `$SANDBOX_DIR` and `$CARGO_HOME/registry` read fine. With the `allow` placed *before* the `deny` the sandbox's own file becomes unreadable. The order deny, allow, deny is therefore required, not stylistic.
- *Correction 1: `/dev/stdout`, `/dev/stderr`, `/dev/tty` and `/dev/fd/*` need their own write rules.* With only the five device nodes of the first draft, `echo x > /dev/stderr` (common in build scripts) fails, because the check is on the `/dev/stderr` path, not on the descriptor it resolves to.
- *Correction 2: the directories on the way to an allowed path need `file-read-metadata`.* A `subpath` rule also matches the directory itself, so `(deny file-read* (subpath SANDBOX_BASE))` made `stat(SANDBOX_BASE)` fail, and then `realpath()` of anything below it failed too: `cargo build --offline` printed `could not canonicalize path` and died with `could not parse/generate dep info`. `stat` of `USER_HOME` fails likewise, which broke compiling a dependency from `$CARGO_HOME/registry/src` when the toolchain lives under the home (the normal case). The profile therefore re-allows `file-read-metadata` for the literal ancestors of `SANDBOX_DIR`, `RUSTUP_HOME` and `CARGO_HOME` that the deny rules cover, plus `SANDBOX_BASE`. Their contents stay unreadable (`ls` of the base or of the home fails; `stat` of `~/.ssh` fails; only `stat` of the ancestors themselves succeeds). Without this change both cargo tests in `test_feat3_os_isolation.rs` fail.
- *The parameters must be canonical.* Rules match the real path, so a path given through a symlink (`/tmp/...` for `/private/tmp/...`) is denied like the real one, and a `-D` value through a symlink would never match. `/var/folders/...` temp bases are `/private/var/folders/...`; every parameter is canonicalized (its longest existing prefix, when the path does not exist yet).
- *`(deny network*)` blocks a loopback TCP connect:* `nc -z 127.0.0.1 <port>` succeeds under `fs` and the listener accepts it; under `fs+net` it fails and the listener sees nothing.
- *`--` ends the `sandbox-exec` options,* and `-p` takes the profile text, so no profile file is written.
- *Security review of PR 133 (2026-11-08) found escapes that are not file accesses; each is closed by a rule and has a regression test in `test_feat3_os_isolation.rs`:*
  - `(deny lsopen)` and `(deny appleevent-send)`: a task built a background-only `.app` in its sandbox and started it with `open -g -j` or `NSWorkspace.openURL`; launchd ran it unconfined and it copied a denied file. Both routes fail with LaunchServices error `-54`.
  - `(deny user-preference-read user-preference-write)` with `(allow user-preference-read (preference-domain "kCFPreferencesAnyApplication"))`: `defaults read` returned the owner's values and `defaults write` created plists in `~/Library/Preferences`, because cfprefsd acts for the task. (The deny alone did not stop it in an isolated experiment without the home file deny; the combination with the file rules does, and the test covers the combination.)
  - `(deny network-outbound (remote unix-socket))` with an allow for `/private/var/run/mDNSResponder`: the SSH agent socket is listed under `/private/var/run/com.apple.launchd.*/Listeners`, so unsetting `SSH_AUTH_SOCK` was cosmetic. DNS keeps working.
  - `(deny mach-lookup (global-name-prefix "com.apple.pasteboard"))`: `pbpaste` read the clipboard.
  - `(deny file-ioctl (literal "/dev/tty") (regex #"^/dev/ttys[0-9]+$"))`: a task injected keystrokes into the parent shell with `TIOCSTI`. The test runs the task under a pty and makes the task ignore `SIGTTOU` as an attacker would.
  - `(deny signal)` then `(allow signal (target same-sandbox))`: a task could `SIGSTOP` the worker or any process of the user. The task can still signal its own children; FEAT-2's supervisor is forked before `sandbox-exec` and is not restricted.
  - The account's real home (`getpwuid_r`) is denied as well as `$HOME`, so a worker started with another `HOME` does not expose the real one.
  - Reads wider than the first draft said are now denied: the per-user temp directory (`confstr(_CS_DARWIN_USER_TEMP_DIR)`, which also hides Blender's temp directories from other tasks; a Blender task's own temp directory is its `SANDBOX_DIR` and stays usable), the per-user cache directory (`confstr(_CS_DARWIN_USER_CACHE_DIR)`, `/var/folders/../C`, which was readable and listable), `/private/tmp`, `/private/var/tmp`, `/Users/Shared` and `/Volumes`.
- *Still readable on macOS:* the system (`(allow default)`), other users' directories where their permissions allow it, and `$CARGO_HOME/git` (it is inside the re-allowed `$CARGO_HOME`; the credentials files are denied).
- *Not covered:* `~/Library` and `DARWIN_USER_TEMP_DIR` (`/var/folders/...`) are outside `USER_HOME` when the home is elsewhere, and a tool that writes there (a compiler cache) is refused by the write rule. Reads of system paths stay open (`(allow default)`), as on Linux.

### D4 Windows implementation: per-task AppContainer SID (`crates/worker/src/isolation.rs`)

On Windows (`#[cfg(windows)]`), when `isolation` is `Fs` or `FsNet`:

1. **One unique AppContainer SID per task (prevents cross-task sandbox reads):**
   - In `Sandbox::create` (or before spawning the task in `execute_process_supervised`), the worker creates a per-task AppContainer profile named `oxideswarm.task.<task_id>` via `CreateAppContainerProfile` (`userenv.dll`), obtaining a unique per-task SID `S-1-15-2-...`.
   - When the task finishes and its `Sandbox` is destroyed (as well as during start-up cleanup of leftover sandboxes), the worker deletes the profile via `DeleteAppContainerProfile`.
   - Because an AppContainer token does not carry the user's SID in its access group list, the Windows kernel denies access by default to the entire `%USERPROFILE%` tree (`~/.oxideswarm/cluster.secret`, `~/.ssh/test_key`, `%APPDATA%`), `<sandbox_base>`, and `<base>/cache`.
   - `Sandbox::create` grants `FILE_ALL_ACCESS` on `<sandbox_base>/<task_id>` **only** to that task's specific AppContainer SID (`SetNamedSecurityInfoW`). Two tasks running concurrently on the same Windows worker have distinct AppContainer SIDs and cannot read or write each other's sandbox directories.
   - At worker start-up under `fs` or `fs+net`, the worker ensures `ALL APPLICATION PACKAGES` (`S-1-15-2-1`) has read-and-execute (`GENERIC_READ | GENERIC_EXECUTE`) access on `$RUSTUP_HOME` and `$CARGO_HOME` (`bin`, `registry`, `git`, `config.toml`), and sets an explicit `DENY_ACCESS` ACE for `S-1-15-2-1` on `$CARGO_HOME\credentials` and `$CARGO_HOME\credentials.toml` and on `~/.oxideswarm`.
2. **Network isolation (`fs+net` vs `fs`):**
   - The child is spawned with `UpdateProcThreadAttribute` (`PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES`):
     - Under `fs`, `SecurityCapabilities` includes `WinCapabilityInternetClientSid` (`S-1-15-3-1`) and `WinCapabilityPrivateNetworkClientServerSid` (`S-1-15-3-3`) so only filesystem isolation is enforced.
     - Under `fs+net`, `SecurityCapabilities` passes `Capabilities = NULL, CapabilityCount = 0`, which causes the Windows kernel (WFP / AFD) to reject all outbound and inbound IPv4/IPv6 TCP and UDP operations with `WSAEACCES`.

### D5 Reserved `iso:` worker tag and placement constraint (`crates/core/src/placement.rs`)

1. `crates/core/src/placement.rs`:
   - Add `pub const ISO_TAG_PREFIX: &str = "iso:";` and include `ISO_TAG_PREFIX` in `RESERVED_TAG_PREFIXES` (`[&str; 7]`).
   - Every worker advertises `iso:off`, `iso:fs`, or `iso:fs+net` in `WorkerCapabilities::tags`.
   - `Placement` gains `pub isolation: Option<IsolationLevel>`, parsed from `require:iso=off`, `require:iso=fs`, or `require:iso=fs+net`.
   - In `Placement::matches` (`crates/core/src/placement.rs`), a worker with `iso:<w_level>` satisfies `require:iso=<req_level>` when `w_level >= req_level` (`Off < Fs < FsNet`). A worker without an `iso:` tag is treated as `IsolationLevel::Off`.
2. `crates/cli/src/cli_app.rs` (`SubmitArgs`) gains `--require-isolation <fs|fs+net>` (emitting `require:iso=<level>`), and MCP `submit` (`crates/cli/src/mcp.rs`) gains optional `isolation: "fs" | "fs+net"`. Both refuse `off` (`IsolationLevel::parse_required`): every worker is at least `off`, so the constraint would match everything; the placement parser itself accepts `require:iso=off`.

### D6 `oxideswarm doctor` reporting (`crates/cli/src/doctor.rs`)

1. `WorkerSummary` (`crates/cli/src/doctor.rs:108`) gains `pub isolation: String`, populated from `tag_value(&caps.tags, ISO_TAG_PREFIX).unwrap_or("off")`.
2. `DoctorReport::render` (`crates/cli/src/doctor.rs:201`) adds an `ISO` column to the `Workers` table (`off`, `fs`, or `fs+net`) and includes `iso:<level>` in the `C4 Process probe` detail string so both human output and `--json` output report the active isolation level of every worker.

## Compatibility

- Wire format: unchanged. `iso:<level>` and `require:iso=<level>` travel in the existing `WorkerCapabilities::tags` and `Task::tags` `Vec<String>` fields.
- Effects of moving `HOME` / `USERPROFILE` to `sandbox_path` under `fs` and `fs+net` (D1.4):
  - **Rust / Cargo:** `RUSTUP_HOME` and `CARGO_HOME` remain pointed at the host's real toolchain and registry directories (read-only), so `cargo build --offline` and `rustc` work without reconfiguration. Tasks that write to `$CARGO_HOME` (for example `cargo install` or downloading new crates under `fs`) must set `CARGO_HOME=$RUSTY_GRID_SANDBOX_DIR/.cargo`.
  - **Git:** Git reads `$HOME/.gitconfig` inside the sandbox rather than the user's `~/.gitconfig` or `~/.config/git/config`. Tasks that run `git commit` or need `safe.directory` must pass `-c` flags or write `.gitconfig` inside the sandbox.
  - **SSH:** OpenSSH looks for `$HOME/.ssh/config` and `$HOME/.ssh/known_hosts` inside the sandbox, and `SSH_AUTH_SOCK` is unset.
  - **Python:** Python's user site-packages directory (`~/.local/lib/python*/site-packages` on Unix, `%APPDATA%\Python` on Windows) resolves inside the sandbox unless `PYTHONUSERBASE` or `PYTHONPATH` is set; system and virtualenv site-packages under `/usr` or `/opt` remain readable.

## Security

- **Filesystem (`fs` and `fs+net`):** Blocks task processes on Linux, macOS, and Windows from reading or writing the user's home directory (`~/.oxideswarm/cluster.secret`, `~/.oxideswarm/p2p_secret.key`, `~/.ssh`, `~/.aws`, `~/.config`, `$CARGO_HOME/credentials*`), other tasks' sandboxes (including concurrent tasks on Windows via per-task AppContainer SIDs), the worker blob cache, and `/proc/<worker_pid>/environ` on Linux.
- **Network (`fs+net`):** Blocks IPv4 and IPv6 TCP/UDP sockets across Linux (`CLONE_NEWNET` when the host allows it + Landlock TCP from ABI 4 + `seccomp` refusing every socket domain but `AF_NETLINK` and `io_uring_*`, with `AUDIT_ARCH` verification), macOS (`(deny network*)`), and Windows (AppContainer with zero network capabilities).
- **Local IPC coverage per OS:**
  - **Linux:** `seccomp` refuses `socket(AF_UNIX)` under both `fs` and `fs+net`, so `/var/run/docker.sock`, `/tmp/ssh-*` and `/run/user/*/bus` are unreachable and so are abstract sockets. Landlock itself does not stop a `connect` to a pathname socket (D2). `SSH_AUTH_SOCK`, `DBUS_SESSION_BUS_ADDRESS` and `DOCKER_HOST` are also stripped from the environment. `socketpair` is allowed. The task cannot create Unix sockets of its own.
  - **macOS:** Under `fs+net`, `(deny network*)` blocks all `AF_UNIX` socket `connect` and `bind` calls; under `fs`, `(deny file-write*)` outside `SANDBOX_DIR` and stripping `SSH_AUTH_SOCK` / `DOCKER_HOST` blocks connecting to sockets that require write permission or environment discovery, while Mach bootstrap IPC to system services stays governed by the default Seatbelt rules.
  - **Windows:** AppContainer places each task in its own isolated named-pipe and ALPC object directory (`\Sessions\<id>\AppContainerNamedObjects\<TaskAppContainerSid>`), blocking access to host named pipes (`\\.\pipe\docker_engine`, `\\.\pipe\openssh-ssh-agent`) under both `fs` and `fs+net`.
- **macOS: a task can read the environment and command line of the worker and of every other process of the same user.** `sysctl KERN_PROCARGS2` (and `ps eww`) is allowed for the same user and no Seatbelt rule blocks it (`process-info*`, `(target others)` and `(deny sysctl-read)` were all tried). A confined task read `OXIDESWARM_SECRET` from a worker that had it in its environment. Mitigations in FEAT-3a: a worker with isolation refuses to start when the cluster secret comes from `OXIDESWARM_SECRET` or `--secret`, and a confined task gets an allowlisted environment. The secret file, `OXIDESWARM_SECRET_FILE` (a path) and the default file are not exposed that way, and the worker passes the paths of the secret file (`--secret-file`, `OXIDESWARM_SECRET_FILE`, a config key, the default file) and of the config file in use (it can hold `secret =`) to the policy as `(deny file-read* (literal <canonical path>))`, so they stay unreadable even when they live outside the denied home (for example in `/etc`, `/opt` or a toolchain directory). The worker does not scrub its original environment or argv in place (unsound in a multi-threaded process). Residual risk: any other secret in the worker's environment or argv, and the environment and argv of the user's other processes (a shell with a token, an IDE), stay readable to tasks; only a dedicated task user (future work) closes this. On Linux the equivalent is `/proc/<pid>/environ` and `cmdline`, closed by the narrow `/proc` rule and Landlock's `ptrace` hook (FEAT-3b).
- **Linux residual risks (FEAT-3b).** (1) Landlock does not restrict `stat`, path lookup or `readlink`: a task can learn that a denied file exists and its size, owner and times, and the names it can guess; it cannot read, list or write it. (2) On Landlock ABI < 6 (Linux < 6.12) a task can signal the worker and any process of the same user; on ABI < 3 it can truncate files it owns inside the read-only toolchain directories. Start-up logs both. (3) `seccomp` is a targeted deny list, not an allow list: other powerful system calls (`keyctl`, `bpf`, `perf_event_open`, `userfaultfd`) stay as the kernel and the distribution configure them. (4) At `fs` the network stays open (as designed) apart from Unix sockets. (5) A 32-bit executable is killed by the filter. (6) Without unprivileged user namespaces `fs+net` rests on `seccomp` (the private network namespace is a second layer, not the first). (7) A host where the worker runs inside a container whose own `seccomp` profile blocks the `landlock_*` calls refuses to start, by design.
- **Tag integrity:** `ISO_TAG_PREFIX` (`iso:`) is in `RESERVED_TAG_PREFIXES` so an operator cannot spoof `iso:fs` via `--tag`.

## Test plan

- **Unit tests (`crates/core/src/placement.rs`, `crates/worker/src/isolation.rs`, `crates/cli/src/doctor.rs`):**
  - `iso:` is rejected by `validate_user_tag`.
  - `Placement::from_task_tags` parses `require:iso=fs` and `require:iso=fs+net`, rejects unknown values, and matches `iso:fs+net` against `require:iso=fs`.
  - `DoctorReport` renders the `ISO` column and serializes `isolation` in JSON.
- **Cross-platform integration tests (`crates/worker/tests/test_feat3_os_isolation.rs`):**
  1. **`fs` secret, home-directory, sibling-sandbox, and `/proc` isolation test (Linux, macOS, Windows):**
     - Under a temporary `HOME` / `USERPROFILE`, creates `$HOME/.oxideswarm/cluster.secret`, `$HOME/.ssh/test_key`, and a sibling sandbox file `<sandbox_base>/other_task/secret.txt`.
     - Runs a task under `IsolationLevel::Fs` that (a) writes and reads back `$RUSTY_GRID_SANDBOX_DIR/allowed.txt`, and (b) attempts to read `$HOME/.oxideswarm/cluster.secret`, `$HOME/.ssh/test_key`, `<sandbox_base>/other_task/secret.txt`, and (on Linux) `/proc/<worker_pid>/environ`, and attempts to write a file outside its sandbox.
     - Asserts that writing/reading `allowed.txt` inside the sandbox succeeds, while every forbidden read (`cluster.secret`, `.ssh/test_key`, `other_task/secret.txt`, and `/proc/<worker_pid>/environ` on Linux) and outside write fails with permission/access denied.
  2. **Concurrent tasks cross-sandbox isolation test (Linux, macOS, Windows — Item 3 of review):**
     - Runs two tasks concurrently under `IsolationLevel::Fs` on the same worker, synchronizes after both write `secret_a.txt` and `secret_b.txt` in their respective sandboxes, and asserts that neither task can read or list the other task's sandbox directory.
  3. **Offline `cargo build --offline` with cached dependency under `fs` (Linux, macOS, Windows — Item 1 of review):**
     - Pre-populates a tiny local dependency crate in the test's Cargo cache (`CARGO_HOME`), then runs `cargo build --offline` inside a task under `IsolationLevel::Fs` and asserts that `cargo` and `rustc` succeed (`exit_code == 0`) and produce the binary in the sandbox `target/`.
  4. **`fs+net` socket and `io_uring` isolation test (Linux, macOS, Windows — Item 4 of review):**
     - Binds a local `TcpListener` on `127.0.0.1:0` in the test process and runs a task under `IsolationLevel::FsNet` that attempts an outbound TCP connection to `127.0.0.1:<port>` (and on Linux, also invokes `SYS_io_uring_setup` / `IORING_OP_SOCKET`, skipping the `io_uring` sub-assertion only if an unisolated probe shows the kernel lacks `io_uring`). Asserts that both attempts fail inside the task and the listener accepts no connection.
  5. **`doctor` reporting test (`tests/doctor_e2e.rs`):**
     - Starts a worker with `--isolation fs`, runs `oxideswarm doctor`, and asserts that the output and JSON report `iso:fs`.

On Linux the same tests (`mod linux` in `test_feat3_os_isolation.rs`) add: the `/proc/<worker_pid>/{environ,cmdline}` denial, symbolic- and hard-link escapes, a protected file inside a granted directory, a listening pathname Unix socket outside the sandbox, the socket domains per level, `io_uring_setup` (`ENOSYS`; skipped when the unconfined control has no usable io_uring), `TIOCSTI` (`EPERM` instead of `ENOTTY`), signals to a foreign process (ABI 6+), the private network namespace and its id map, and the tracked PID, cancel and timeout with the leader inside its namespaces. A kernel without Landlock skips them with a notice unless `RUSTY_GRID_REQUIRE_LANDLOCK=1` (CI sets it); `RUSTY_GRID_REQUIRE_USERNS=1` demands the namespace.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| `FEAT-3` | Add `--isolation off\|fs\|fs+net`, Linux Landlock/`seccomp`/namespaces, macOS `sandbox-exec`, Windows per-task AppContainer, `RUSTUP_HOME`/`CARGO_HOME` preservation, `iso:` tag, and `doctor` reporting. Split into the three rows below | `crates/core/src/placement.rs`, `crates/worker/src/isolation.rs`, `crates/worker/src/process_tree.rs`, `crates/worker/src/runner.rs`, `crates/worker/src/sandbox.rs`, `crates/cli/src/cli_app.rs`, `crates/cli/src/config.rs`, `crates/cli/src/doctor.rs`, `crates/cli/src/mcp.rs`, `crates/worker/tests/test_feat3_os_isolation.rs`, `README.md`, `docs/README.md` | Under `fs`, reading `cluster.secret`, `$HOME/.ssh/test_key`, a concurrent task's sandbox, or `/proc/<worker_pid>/environ` fails and `cargo build --offline` of a crate with a cached dependency succeeds on each OS; under `fs+net`, outbound TCP and Linux `io_uring` socket attempts fail; `doctor` reports the level | Partial |
| `FEAT-3a` | The platform-independent core and macOS: `IsolationLevel`, `--isolation` / `RUSTY_GRID_ISOLATION` / `[worker].isolation`, start-up self-check, environment preparation, `iso:` tag, `require:iso=`, `submit --require-isolation`, MCP `isolation`, `doctor` ISO column, macOS `sandbox-exec` profile | `crates/core/src/placement.rs`, `crates/worker/src/isolation.rs`, `crates/worker/src/runner.rs`, `crates/worker/src/client.rs`, `crates/cli/src/{cli_app,config,doctor,mcp}.rs`, `crates/worker/tests/test_feat3_os_isolation.rs` | On macOS the `fs` and `fs+net` tests in `test_feat3_os_isolation.rs` pass (secrets, sibling sandbox and outside writes denied, TCP refused, `cargo build --offline` works, PID, cancel and timeout still work); on Linux (until `FEAT-3b`) and Windows an `fs` worker refuses to start with "not implemented yet" | Implemented |
| `FEAT-3b` | Linux: Landlock, `seccomp`, namespaces, `/proc` narrowing; adds the concurrent-task and `io_uring` tests | `crates/worker/src/isolation.rs`, `crates/worker/src/isolation/linux.rs`, `crates/worker/src/runner.rs`, `crates/worker/tests/test_feat3_os_isolation.rs`, `.github/workflows/ci.yml` | The Linux rows of the Test plan pass on `ubuntu-latest` (with and without unprivileged user namespaces); the self-check refuses a kernel without Landlock | Implemented |
| `FEAT-3c` | Windows: per-task AppContainer profile and ACLs | `crates/worker/src/isolation.rs`, `crates/worker/src/sandbox.rs` | The Windows rows of the Test plan pass | Planned |

## Status log

- 2026-11-08: Initial draft written for `FEAT-3` (Phase B of `docs/ROADMAP.md`).
- 2026-11-08: Addressed review `117-fix`: preserved `RUSTUP_HOME` and read-only `CARGO_HOME` (with `credentials*` denied) so `cargo build --offline` works under `fs`; unified the `fs` read policy across Linux, macOS (`(deny file-read* (subpath (param "USER_HOME")))` before toolchain/sandbox allows), and Windows; switched Windows to a per-task AppContainer profile (`oxideswarm.task.<task_id>`) so concurrent tasks cannot read each other's sandboxes; added `AUDIT_ARCH` validation and `io_uring_*` blocking to the Linux `seccomp` filter and stripped `SSH_AUTH_SOCK` / `DBUS_SESSION_BUS_ADDRESS` / `DOCKER_HOST`; narrowed Linux `/proc` rules to `/proc/self` and system info files; specified `pre_exec` ordering after design 0031's supervisor fork; and documented the effects of moving `HOME` in Compatibility.
- 2026-11-08: `FEAT-3a` implemented (core and macOS). Set Status to Accepted. `IsolationLevel` lives in `rusty_grid_core::placement` (re-exported by the worker). Corrected D3 after checking Seatbelt on macOS: last-match-wins precedence confirmed; `/dev/stdout`, `/dev/stderr`, `/dev/tty`, `/dev/fd/*` added to the writable devices; `file-read-metadata` re-allowed for the ancestors of the allowed paths (needed for `realpath()` and so for cargo); parameters must be canonical; added the start-up probe. Linux (`FEAT-3b`) and Windows (`FEAT-3c`) still make an `fs` worker refuse to start.
- 2026-11-08: Security review of PR 133 fixed in `FEAT-3a`: D3 gained the non-file rules (LaunchServices, Apple events, preferences, Unix sockets, pasteboard, `TIOCSTI`, signals), the account home and the wider read denies; D1.4 gained the environment allowlist and the refusal of a secret in `OXIDESWARM_SECRET` / `--secret`; Security names the macOS process-environment residual risk.
- 2026-11-08: Re-verification of PR 133 found that a secret file or config file outside the denied home was readable, and so was the per-user cache directory. The worker now passes the secret file and config file paths to the policy (`WorkerConfig::protected_files`) and denies the cache directory; Security no longer calls the secret file safe because of the home deny.
- 2026-11-08: `FEAT-3b` implemented (Linux). Corrected D2 after running it on `ubuntu-latest` (Linux 7.0, Landlock ABI 8): Landlock does not stop `connect` to a pathname Unix socket, so `seccomp` refuses `socket(AF_UNIX)` at both levels (and, under `fs+net`, every domain but `AF_NETLINK`); the `/proc/self` rule is opened in the child and covers the task leader only; protected files are cut out of granted directories; `TIOCSTI` and the x32/foreign ABI are in the filter at both levels; `/etc/passwd`, `/etc/group` and `/etc/localtime` joined the read-only list; AppArmor's userns restriction lets `unshare` succeed without capabilities, so the id map fails and the task sees uid 65534 (both paths tested). Lower ABIs lose truncate protection (< 3) and signal scoping (< 6); start-up warns.
