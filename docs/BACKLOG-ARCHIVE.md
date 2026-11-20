# Completion backlog archive

Status: created on 2026-11-18 by DOC-11. An item's text moves here when the item becomes Implemented.

This file holds the text of every Implemented item of [BACKLOG.md](BACKLOG.md), moved here unchanged
so that BACKLOG.md shows the open work first
([design 0088](design/0088-evaluation-of-2026-11-17-and-what-finished-means.md), E2). The items stand
under the phase headings of BACKLOG.md, in the same order, and each phase keeps its introduction. Each
text describes the code when the item was written; if a line has moved, search for the named symbol.
The Index in BACKLOG.md is the list of every item, open or Implemented, with its status and pull
requests, and the status log there records each change.

## Phase 0: integrate open work

Do this phase first. Later items assume its result.

### INT-1: Merge pull request 3

- **Evidence:** Reproduced. All checks are green in run 40269419930.
- **Problem:** `master` has no CI, and its unit tests fail on any machine with a battery.
- **Do:** Merge pull request 3 (`claude/ci-github-actions`).
- **Done when:** The CI run on `master` passes.

### INT-2: Close superseded pull requests and branches

- **Evidence:** Reproduced.
- **Problem:** Pull request 1 (`fix/review-blockers`) has a tree identical to `master`.
  Pull request 2 (`claude/trusting-wing-d9a27c`) is an older CI attempt, 21 commits behind.
  `origin/feat/cross-machine-network-sync` and `origin/feat/two-node-readiness` are fully merged.
- **Do:**
  1. Copy what pull request 2 has and `ci.yml` lacks into the description of TEST-6.
  2. Ask the owner to close both pull requests and delete the merged remote branches.
- **Done when:** `gh pr list` shows neither pull request, and the merged branches are gone.

### INT-3: Land the AEAD hardening branch

- **Evidence:** Reproduced. The trial merge with INT-1 is clean.
- **Problem:** Commit `d0dddb8` on branch `claude/compassionate-mestorf-b58176` has no pull request
  yet. It adds auth version 2, a fail-closed record reader, and tamper and injection tests.
- **Do:**
  1. Rebase it on `master`, and open a pull request.
  2. State in the pull request that a version 1 peer is refused, so all nodes upgrade together.
- **Done when:** The pull request is merged and the gates pass. `tests/cluster_record_layer_e2e.rs`
  passes.

### INT-4: Land the content-addressed file branch

- **Evidence:** Reproduced. A trial merge of INT-1, INT-3 and INT-4 passed clippy, the
  `--no-default-features` check, all unit tests, `blob_transfer_e2e`, `cluster_record_layer_e2e` and
  `file_staging_e2e`. The full suite was not run on the merged tree.
- **Problem:** Commit `fae2622` on branch `claude/dreamy-kilby-6adcea` has no pull request yet. It
  adds BLAKE3 blobs, a master blob store, a worker blob cache, `submit --input` and glob outputs. It adds about
  6,400 lines in one commit.
- **Do:**
  1. Rebase it on `master` after INT-3.
  2. Resolve `crates/cli/Cargo.toml` by keeping the `[[test]]` entries of both branches.
  3. Both branches add a design record numbered 0005. Rename this branch's record to
     `docs/design/0006-content-addressed-data-plane.md` and update the references to it in
     `README.md` and `PROJECT.md`.
  4. Review the change by subsystem, and split it if a reviewer asks.
  5. State in the pull request that `Task` and `TaskResult` gained fields, so every component runs
     the same build and the master restarts first.
- **Done when:** The pull request is merged, the gates pass, and `tests/blob_transfer_e2e.rs`
  passes.

### INT-5: Fix the review findings of pull request 6

- **Evidence:** Reproduced at commit `e1215ee`, except F5, which is Read. The runs used real
  binaries on loopback with a temporary `HOME`. Line numbers in this item refer to that commit.
- **Problem:** Pull request 6 is not ready to merge. CI fails on Windows, and a task can move files
  from outside its sandbox. F1 to F6 describe each problem.
- **Do:** Work through F1 to F6 in order. Commit every fix on branch
  `int-4-content-addressed-data-plane`, so pull request 6 carries it. This item has no pull request
  of its own. Write the failing test first for F2 to F6. Do not merge, and do not start SEC-1.
- **Done when:** Each fix meets its check. `cargo test -p rusty_grid_cli --test blob_transfer_e2e`
  passes. Every check of pull request 6 is green. The owner has then been asked to approve the
  merge under INT-4.

#### F1: Make clippy pass on Windows

- **Evidence:** Reproduced. CI run `41277852801`, job `clippy (windows-latest)`, fails with
  `large_enum_variant` at `crates/core/src/protocol.rs:1867`, `:1899` and `:1925`. On Windows
  `SubmitTask` holds 385 bytes and the next variant 184 bytes. The same check passes on Linux and
  macOS.
- **Problem:** Clippy stopped at `rusty_grid_core`, so the other crates were not linted on Windows.
- **Do:** Add `#[allow(clippy::large_enum_variant)]` to `ClientMessage`, `HumanClientMessage` and
  `BinaryClientMessage`, as `crates/cli/src/cli_app.rs:124` does. Do not box `Task`, because that
  changes every caller. Then fix whatever the next CI run reports for the other crates.
- **Done when:** `clippy (windows-latest)` is green on pull request 6.

#### F2: Never follow a symbolic link in an output path

- **Evidence:** Reproduced. A task ran `ln -s <OUTSIDE> link` with `--output link/important.txt`.
  The task succeeded, `<OUTSIDE>/important.txt` was gone, and the client received the file.
  `--output 'link/sub/*.txt'` moved every matching file of `<OUTSIDE>/sub` in the same way. A trial
  patch left both directories unchanged.
- **Problem:** `collect_outputs` (`crates/worker/src/staging.rs:150`) calls `symlink_metadata` on the
  whole path, so links in parent directories are followed. `insert_file(path, true)` then renames
  the file into the cache. This breaks goal G6 of design 0006 and the module documentation of
  `staging.rs`. A task that links a shared directory loses files there.
- **Do:**
  1. Before `symlink_metadata(&start)`, check every directory on the way to the literal prefix,
     one component at a time, from the canonical sandbox root. Use `file_type().is_symlink()`.
  2. A link on the way fails a literal pattern with an error that names the pattern.
  3. A link on the way makes a wildcard pattern select nothing.
  4. Add unit tests in `staging.rs` and an end-to-end test in `tests/blob_transfer_e2e.rs`. A Windows
     test skips when the system refuses to create a link.
- **Done when:** Both commands above leave `<OUTSIDE>` unchanged. The literal form fails the task
  with a message that names the pattern. `--output '**/*.txt'` still returns the files of the
  sandbox and nothing from `<OUTSIDE>`.

#### F3: Keep output blobs until the client can fetch them

- **Evidence:** Reproduced. With `master --blob-store-mb 3`, task A wrote a 2 MiB output while its
  client was stopped with `SIGSTOP`. A second task then stored a 2 MiB output. The resumed client of
  task A failed with `output 'a.bin' ... is no longer on the master`.
- **Problem:** `apply_task_result` (`crates/master/src/server.rs:1963`) checks that the outputs exist,
  but nothing pins them. Any later upload can evict them before the client downloads them.
- **Do:**
  1. Pin the output blobs in the same store call that checks them, so the check cannot go stale.
  2. Keep that pin for 10 minutes after the task reaches a terminal state. Release it from
     `spawn_blob_store_sweeper` (`crates/master/src/server.rs:1839`), which today unpins every
     terminal task at once.
  3. Keep the input pins of a task that is retried. Drop the new pin for a stale result.
  4. Put the duration in `BlobStoreConfig`, so a test can shorten it. Add no CLI flag.
  5. State in design 0006 D3 that pinned outputs can hold the store above its bound for that time.
- **Done when:** A test runs the scenario above and the client receives the file. A second test
  shows that the blob can be evicted after the pin ends.

#### F4: Pin output files from the moment they enter the worker cache

- **Evidence:** Reproduced. With `worker --blob-cache-mb 1 --max-concurrency 8`, one task wrote 4000
  files of 1 KiB with `--output 'out/*'` while four clients kept running tasks with small outputs.
  The first attempt failed with `output blob ... is no longer in the worker's cache`.
- **Problem:** `collect_outputs` moves files into the cache (`crates/worker/src/staging.rs:210`). The
  pin comes later, in `upload_task_outputs` (`crates/worker/src/blob_client.rs:236`). Between the
  two, `evict_to_fit` of another task can delete them, and the sandbox copy is already gone.
  `crates/worker/src/blob_cache.rs:14` promises that this never happens.
- **Do:**
  1. Hold a pin from the moment a file enters the cache until its upload ends or is skipped.
  2. `TaskResult` is a wire type and cannot carry the pin, so keep it in the cache or the runner,
     keyed by task id.
  3. Release it on every path, including a suppressed result, a failed collection and a failed
     upload.
- **Done when:** A unit test calls `evict_to_fit` between collection and upload, and the files
  survive. The scenario above passes 20 runs in a row.

#### F5: Keep the blob queue from blocking the master's inbound loop

- **Evidence:** Read. No run showed a visible failure, so this is the lowest risk of the six.
- **Problem:** `serve_worker_blob_request` (`crates/master/src/server.rs:1814`) awaits a send into a
  queue of 32 requests. A worker with five transferring tasks can have 40 requests in flight,
  because `TRANSFER_CONCURRENCY` is 8. The inbound loop then waits for the disk, and so do the
  heartbeats and results of that worker. The wait is short on a healthy disk and long on a stalled
  one. The comment at `crates/master/src/server.rs:1775` says this never happens.
- **Do:**
  1. Add a limit of requests in flight per session to `crates/core/src/blob_transfer.rs`, smaller
     than the queue. Build the master queue from that constant.
  2. Enforce the limit in `BlobClient` with a semaphore. Start `BLOB_REPLY_TIMEOUT` after the permit
     is taken.
  3. Keep the bounded queue for a peer that ignores the limit, and correct the comment.
- **Done when:** A unit test starts 100 concurrent `BlobClient` calls against a fake session. It
  never sees more requests in flight than the limit, and every call finishes.

#### F6: Refuse a mismatched worker and master at registration

- **Evidence:** Reproduced with binaries from `e1215ee` and `ee2d13b`. A new worker on an old master
  registered, and the first task ended the connection with
  `Bincode error: io error: unexpected end of file`. The task failed and the worker reconnected. A
  trial patch with both constants at 2 made each side refuse the other with a message that names
  the versions. The unit tests of `rusty_grid_core` passed on that patch.
- **Problem:** `Task` and `TaskResult` changed layout, but `PROTOCOL_VERSION_CURRENT` and
  `PROTOCOL_VERSION_MIN_COMPATIBLE` (`crates/core/src/protocol.rs:31`) stay at 1. `DEPLOYMENT.md` 3.5
  still says that a worker newer than its master keeps working. An old worker on a new master ran a
  plain command only because bincode ignores trailing bytes. `AssignTaskWithCheckpoint` has a field
  after `task`, so that worker can misread a checkpoint (Read).
- **Do:** Set both constants to 2. Correct `DEPLOYMENT.md` 3.5 and the Compatibility section of
  design 0006: all nodes run one build. Update FEAT-8 to match, and leave the rest of it for later.
- **Done when:** A test shows that the master refuses a version 1 worker. A test shows that the
  worker refuses a `RegisterAck` with version 1.

## Phase 1: stop the harm

These items remove ways to lose data or to run code without authorisation.

### SEC-1: Remove `mcp --sse`

- **Evidence:** Reproduced. A `POST` with `Origin: https://evil.example` and
  `Content-Type: text/plain` to an arbitrary path ran a task through `mcp --sse`.
- **Problem:** `run_sse_server` (`crates/cli/src/mcp.rs:1224`) accepts any `POST` on any path. It
  checks no `Origin`, `Host`, `Content-Type` or token. While the mode runs, a web page in the
  owner's browser can run commands on the cluster with the owner's secret. The mode appears in no
  document. Its default port 8085 is also the `sync_network` probe port.
- **Do:** Delete `--sse`, `--sse-bind` and `run_sse_server`.
- **Done when:** Clap rejects `mcp --sse`, and `git grep run_sse_server` finds nothing.

### SEC-2: Make MCP refuse a remote master without a secret

- **Evidence:** Reproduced. With an empty `HOME`, `mcp --master <REMOTE_IP>:8088` answers
  `initialize` and exits 0. `status` with the same arguments exits 6.
- **Problem:** `run_mcp` (`crates/cli/src/mcp.rs:1339`) resolves the secret but never applies the
  refusal policy. `McpState.insecure` (`mcp.rs:216`) is stored and never read. `submit` therefore
  sends the command, environment, stdin and file contents in clear text. MCP also ignores
  `RUSTY_GRID_INSECURE` and the `[status]` configuration.
- **Do:**
  1. Move `client_secret` and `peer_insecure` from `crates/cli/src/cli_app.rs` to
     `crates/cli/src/secret_cmd.rs`, and call them from `run_mcp`.
  2. Pass the `[status]` configuration to `run_mcp`, and remove `McpState.insecure`.
- **Done when:** With an empty `HOME`, `mcp --master <REMOTE_IP>:8088` exits 6 before it reads
  stdin. With `--insecure` it starts and warns on stderr. `tests/mcp_server_e2e.rs` covers both.

### SEC-3: Delete the Android stub master

- **Evidence:** Read. The stub and the dependency list were confirmed. `rusty_grid_master` is only a
  dev-dependency of `crates/android_bridge`.
- **Problem:** `start_master_impl` (`crates/android_bridge/src/lib.rs:556`) binds `0.0.0.0`.
  `handle_coordinator_stream` (`crates/android_bridge/src/lib.rs:816`) runs no handshake and acknowledges registrations. It
  has no queue, no scheduler and no task dispatch. The "dashboard" returns one hard-coded string.
  `CLAUDE.md`, `PROJECT.md` and `ANDROID_ARCHITECTURE.md` call this a master.
- **Do (default of D2):**
  1. Delete `start_master_impl`, `handle_coordinator_stream`, `nativeStartMaster`,
     `nativeStopMaster`, and the Kotlin `startMaster` and `stopMaster`.
  2. Remove or rewrite the tests that use them. `adversarial_challenge_tests.rs` can start a real
     `MasterServer` through the dev-dependency.
  3. State in the three documents that the app is a worker only.
- **Done when:** `git grep nativeStartMaster` finds nothing, and the gates pass.

### SEC-4: Delete the plain-HTTP secret transfer

- **Evidence:** Read.
- **Problem:** `serve-secret` in `scripts/agent_bridge.py` serves `cluster.secret` over plain HTTP on
  `0.0.0.0:8086` for up to 600 s. Without peer state it serves any client. The secret then crosses
  the LAN unencrypted. Port 8086 and this command are missing from `docs/SECURITY.md` and the
  `README.md` port table.
- **Do (default of D7):** Delete `serve-secret` and `pull-secret`. Point
  `docs/guides/multi-agent-collaboration.md` at `scp` or another channel the owner trusts.
- **Done when:** `git grep serve-secret` finds nothing.

### SEC-5: Run Python and Blender through the hardened runner

- **Evidence:** Read. The missing `env_remove` was confirmed.
- **Problem:** The Python tier (`crates/worker/src/runner.rs:1809-1825`) and Blender
  (`crates/worker/src/autorig/blender_provider.rs:149-173`) build their own commands. Compared with
  `execute_process_supervised`:
  - `OXIDESWARM_SECRET`, `OXIDESWARM_SECRET_FILE` and `OXIDE_MESH_TOKEN` reach the child.
  - They get no nice value, no `PR_SET_PDEATHSIG`, and no `CREATE_SUSPENDED`.
  - `terminate_all_task_process_trees` does not see them.
  - `crates/worker/src/job_object.rs` is a second containment implementation, and its `Drop` does
    nothing on Unix, so an aborted future leaves grandchildren alive.
  - Blender's stderr is piped and never read, so it can block.
- **Do:** Run both through `execute_process_supervised`, extended with an output-file step. Delete
  `job_object.rs`.
- **Done when:** A 3D task and a fake-Blender task report the secret variables as unset and run at
  the task nice value. Their grandchildren die on abort on Linux and macOS.

### SEC-6: Restrict Agent Mesh node commands

- **Evidence:** Read.
- **Problem:**
  - The Rust node spawns any `command` string, not only `shell_exec`
    (`crates/agent_mesh/src/executor.rs:174`).
  - The Python node passes no `env=` (`scripts/agent_node.py:359-364`), so `shell_exec` children
    inherit `OXIDE_MESH_TOKEN` and `OXIDESWARM_SECRET`. Its output has no size cap.
  - Any token holder can run commands on every node.
- **Do:**
  1. Restrict the Rust node to the five commands it advertises.
  2. Add `--no-shell` to both runtimes.
  3. In `agent_node.py`, remove the secrets from the environment, cap output at 1 MiB, and honour
     `cwd` and `env`.
- **Done when:** `send --command /bin/ls` returns 127 on both runtimes. `shell_exec env` on a Python
  node shows neither secret. `--no-shell` refuses `shell_exec`.

### SEC-7: Correct public route claims and redact the lease

- **Evidence:** Read.
- **Problem:** `docs/SECURITY.md:25` says every dashboard route needs a token. `GET /`,
  `/api/discovery/probe`, `GET /api/rendezvous/master` and `POST /api/rendezvous/register` need none
  (`crates/master/src/auth.rs:374-377`). The lease read exposes the P2P ticket and addresses. A
  master without a secret accepts unsigned lease writes (`crates/master/src/web_ui.rs:1636-1649`).
  `docs/SECURITY.md:78` says the CLI has no switch for the portal, but `--no-portal` exists.
- **Do:** Remove the ticket from unauthenticated reads. List every public route in
  `docs/SECURITY.md`.
- **Done when:** A test asserts that the anonymous lease response has no ticket.

### SEC-9: Private files on Windows

- **Evidence:** Read; the ACL as reported from the owner's Windows machine. `C:\Users\<name>` grants
  `Everyone:(RX)` and `BUILTIN\Users:(RX)` with inheritance.
- **Problem:** On Unix the cluster secret and `master_key.bin` are created with mode 0600 and
  `ClusterSecret::load_file` warns about a wider mode. On Windows the files inherit the DACL of their
  directory, so every local account can read the secret and the master identity, and nothing warns.
- **Do:** Create each private file (cluster secret, master key, ticket file), the task store
  directory (`--state-dir`) and the `~\.oxideswarm` directory with a protected DACL for the current user, `SYSTEM` and
  `Administrators`, applied by `CreateFileW` at creation. Warn at load, with the `icacls` fix, when
  the DACL grants anyone else. Design 0047.
- **Done when:** A Windows test creates a private file under a parent that grants `Everyone` read
  and reads back a protected DACL with exactly the three allowed SIDs. A pure test covers the
  warning check.

### BUG-1: Stop hard-linking the staging cache

- **Evidence:** Reproduced. Three tasks with the same `--file`: the second appended to it, and the
  third read `original` plus the appended line.
- **Problem:** On a cache hit `Sandbox::stage_files` hard-links the cache object into the sandbox
  (`crates/worker/src/sandbox.rs:483`). The object is not hashed again. A task that edits its input
  in place rewrites the cache, and every later task with that hash gets the changed bytes.
  `set_permissions` at `crates/worker/src/sandbox.rs:510` also changes the shared inode. The payload is already in
  memory, so the link saves no network traffic.
- **Do:** Write `staged.data` to the target, and remove the inline `cache/objects` use. Update
  `docs/design/0003-distributed-file-staging.md`.
- **Done when:** A test stages one file for three tasks, task 2 appends, task 3 reads the original
  bytes, and the file in the sandbox has `nlink` 1.

### BUG-2: Fail a task when a declared output is missing

- **Evidence:** Reproduced. `submit --output nonexistent.txt --output-dir out --wait --command true`
  exits 0 and prints nothing about the file.
- **Problem:** Missing, symlinked, over-size and over-aggregate outputs are skipped with a worker log
  only (`crates/worker/src/runner.rs:1204-1231`). `--file` and `--output` are ignored by GPU,
  `BuiltinTest`, 3D and rig tasks. `README.md:280` shows `--output-dir` without `--output`, which
  collects nothing.
- **Do:** Fail the task with the path and the reason, as design 0006 does for the blob path. Refuse
  these flags on task kinds that ignore them. Correct `README.md:280`.
- **Done when:** A missing or 11 MiB `--output` gives a non-zero result naming the path.
  `--type gpu --file <PATH>` fails before submit.

### BUG-3: Fix the default 3D submit

- **Evidence:** Reproduced. `submit --prompt robot --wait` fails with
  `TaskSpec requires GPU, but TaskRequirements.gpu_required is false`.
- **Problem:** The default engine leaves `engine_preference` as `None`, so `requires_gpu()` is true
  (`crates/core/src/task.rs:1485-1491`). The CLI sets `gpu_required` only from `--gpu` or
  `--type gpu` (`crates/cli/src/cli_app.rs:2302-2309`). CLI tests always pass `--engine sdf`.
  Engine names also disagree across `crates/worker/src/runner.rs:1697`,
  `crates/core/src/task.rs:1489` and `crates/agent_mesh/src/bridge.rs:611`.
- **Do:** Make `auto` CPU-capable. Use one engine-name enum in `crates/core`.
- **Done when:** `submit --prompt robot --wait` succeeds on a CPU worker, with a test.

### BUG-4: Fail only the task when a result is too large

- **Evidence:** Read.
- **Problem:** A result frame above 64 MiB ends the session and cancels every task on that worker
  (`crates/core/src/protocol.rs:177`, `crates/worker/src/client.rs:654`). `asset_bytes` is not
  charged to the retention budget (`crates/master/src/queue.rs:601-610`). The byte budget covers
  terminal tasks only.
- **Do:** Fail the oversized result as one task. Charge `asset_bytes`. Add a byte cap for active
  tasks.
- **Done when:** An oversized result fails only its task, and other tasks on that worker finish.

### 3D-1: Stop reporting the stub as TripoSR

- **Evidence:** Read. `engines/generative_3d/entrypoint.py` was confirmed.
- **Problem:** `entrypoint.py` prints "neural volume" progress with `time.sleep`, loads no model, and
  writes a fixed mesh (`entrypoint.py:45-57`). With the default `auto` it is chosen before the SDF
  tier when Python and the working-directory-relative script resolve
  (`crates/worker/src/runner.rs:1704-1728`). The result is labelled `TripoSR-CUDA` or `TripoSR-CPU`.
  The same request therefore gives an octahedron or a humanoid, depending on the worker's directory.
  The prompt selects one of four fixed shapes, and the image is ignored by the SDF tier
  (`crates/worker/src/gen3d/mod.rs:38`). `scripts/agent_node.py:721` offers engines that have no
  code.
- **Do (default of D3):** Make `entrypoint.py` exit non-zero with the reason. Make `auto` use the SDF
  tier. Take the device label from what ran. Reject `--image` on the SDF tier. Trim
  `agent_node.py:721`.
- **Done when:** `--engine triposr` without a model fails and names the cause.
  `--engine sdf --image <PATH>` fails clearly. A test asserts that no `device_name` contains TripoSR
  unless a model ran.

### BUG-5: Close the remaining gaps in the blob store and cache

- **Evidence:** Read at commit `e1215ee`, except defect 6, which is Reproduced. If a line has moved,
  search for the named symbol.
- **Problem:** The review of pull request 6 found these smaller defects. None of them blocks the
  merge.
  1. Eviction removes a blob from the index under the lock and deletes its file after the lock
     (`delete_objects`, `crates/master/src/blob_store.rs:563`). A concurrent commit of the same id
     can lose its new file while the index still lists it.
  2. Uploads in `tmp/` do not count towards `max_bytes` (`crates/master/src/blob_store.rs:9`), so
     concurrent uploads can exceed the bound.
  3. `walk` skips selected names that are not UTF-8 or not valid blob paths, and it hides read
     errors (`crates/worker/src/staging.rs:234`). The task still succeeds.
  4. A cancelled fetch leaves its `.part` file in the cache `tmp/` directory
     (`crates/worker/src/client.rs:1449`). The file is outside the size accounting, and only a
     worker start removes it, after 24 hours.
  5. `abort_uploads` (`crates/master/src/blob_store.rs:623`) cannot see an upload whose chunk is
     being written, and `append` puts that upload back afterwards. Its file stays until the
     10-minute sweep.
  6. When an output download fails, `submit` returns before it prints the exit code, stdout and
     stderr of the task (`run_submit_with_secret`, `crates/cli/src/cli_app.rs`).
- **Do:** Confirm each defect against the code. Write a failing test for it, then fix it.
- **Done when:** Each defect has a test that failed before the fix.
  `cargo test -p rusty_grid_cli --test blob_transfer_e2e` passes.

## Phase 2: make the tests trustworthy

The full suite must pass on a clean machine before CI can require it.

### TEST-1: Gate host-bound tests

- **Evidence:** Reproduced. See the test baseline.
- **Problem:**
  All paths below are in `crates/worker/tests/`.
  - Tests in `test_challenger_m3_blender_job_object.rs` assert `C:\Program Files\...\blender.exe`.
  - `test_gen3d_pipeline.rs:277`, `test_autorig_adversarial_stress.rs:1226` and
    `test_gen3d_adversarial_stress.rs:693` hard-code an `E:\` path. On Unix the first writes a stray
    file into `crates/worker/`.
  - `test_gen3d_pipeline.rs:99` and `test_gen3d_adversarial_stress.rs:173` use a 50 ms budget in
    debug builds.
  - `test_gen3d_pipeline.rs:289` and `test_gen3d_adversarial_stress.rs:633` spawn a child without its
    own process group, so `kill -9 -pid` misses it.
  - `test_challenger_m2_job_object.rs` needs `python` on `PATH`.
- **Do:**
  1. Use `tempfile` for all output paths.
  2. Gate Windows tests with `#[cfg(windows)]`.
  3. Skip Blender tests without Blender, and add `RUSTY_GRID_REQUIRE_BLENDER`, like the GPU switch.
  4. Use `process_group(0)` on Unix spawns and a `python3` fallback.
  5. Run speed budgets in release builds only, or move them to a benchmark.
- **Done when:** `cargo test -p rusty_grid_worker` passes on macOS and Linux without Blender or an
  `E:` drive. `git grep 'E:.teamwork_projects'` finds nothing in the test files. `git status` is
  clean after the run.

### TEST-2: Isolate tests from the host battery

- **Evidence:** Reproduced. Three tests pass only when `pmset` reports no battery.
- **Problem:** `WorkerClient::new` reads the host battery. On a laptop that gives a 60 s heartbeat.
  Below 15 % the worker refuses every task, so any test that runs a task fails.
  `crates/worker/src/client.rs:2848` and other unit tests share this dependency.
- **Do:** Add a `WorkerConfig` option that disables the host probe, and use it in every test that
  starts a worker. Spawned binaries need a matching environment variable.
- **Done when:** The full suite gives the same result with a fake `pmset` that reports no battery and
  with one that reports `5%; discharging`.

### TEST-3: Repair stale, starved and flaky tests

- **Evidence:** Reproduced failures. The causes below are from reading, so confirm them.
- **Problem:**
  - `test_challenger_cascading_worker_crashes_exhaust_retries`: `ChallengerCluster::spawn` sets only
    `with_max_retries`. A worker loss consumes `max_failovers` (default 10,
    `crates/master/src/queue.rs:183`), so after three crashes the task waits with no workers.
  - `test_capability_queries_under_connect_disconnect_stress`: each query loop ran once in 1.8 s.
    The likely cause is `#[tokio::test]` on one thread, plus blocking hardware probes in
    `WorkerClient::new` (`crates/core/src/capabilities.rs:694`).
  - `test_adversarial_worker_connect_via_master_p2p_ticket`: it allows 5 s for a P2P connection
    through public relays. It also failed on Ubuntu and Windows in the only CI run of the full suite.
  - `test_aead_sessions_roundtrip_tamper_and_replay` in `crates/core/src/protocol.rs` was reported to
    pass for the wrong reason, because it decrypts with the wrong key.
- **Do:**
  1. Set `with_max_failovers` in the cascading test, or define what exhausting retries means.
  2. Time `WorkerClient::new`. If it blocks, move the probes to `spawn_blocking`, and use a
     multi-thread test runtime.
  3. Give the P2P test a longer budget or a local relay.
  4. Make the tamper test fail only because of the tampering.
- **Done when:** Each test passes ten times in a row on an idle machine.

### TEST-4: Stop tests from reading the real home directory

- **Evidence:** Reproduced. These tests pass with an empty `HOME` and fail with a dummy
  `cluster.secret` in `HOME`:
  - `crates/cli/tests/test_cli_3d_submission.rs:13` and `test_cli_3d_submission.rs:87`
  - `crates/cli/tests/test_challenger_m4_cli_adversarial.rs:498`
  - `crates/core/tests/m1_3d_adversarial_stress.rs:740` and `m1_3d_adversarial_stress.rs:852`
  - `tests/test_3d_autorig_pipeline.rs:1306`
- **Problem:** These tests start a client or worker without a private `HOME`. They read the
  developer's real `~/.oxideswarm/cluster.secret`. The two in `m1_3d_adversarial_stress.rs` also
  write `worker_id.*` files there, which breaks the test isolation rule in `CLAUDE.md`.
- **Do:** Give every spawned binary and every in-process CLI run a temporary `HOME` and
  `USERPROFILE`.
- **Done when:** The full suite gives the same result with a dummy `cluster.secret` in `HOME` and
  with an empty `HOME`.

### TEST-5: Register or delete the orphan Rust test

- **Evidence:** Reproduced. A script compared `tests/*.rs` with the `[[test]]` entries.
- **Problem:** `tests/test_challenger_m4_scheduler_runner.rs` is the only file in `tests/` without a
  `[[test]]` entry, so it never runs. Its `WorkerInfo` literal also lacks the `features` field.
- **Do:** Repair and register it in `crates/master/Cargo.toml`, or delete it after 3D-2. Add a check
  that compares `tests/*.rs` with the `[[test]]` entries, and run it in CI.
- **Done when:** The check passes, and CI fails if a file is added without an entry.

### TEST-6: Extend CI to the full suite

- **Evidence:** Reproduced. `.github/workflows/ci.yml` runs unit tests only.
- **Problem:** CI never runs `tests/`, `crates/*/tests/`, `--features gpu-wgpu`, the Python suites, or
  the launcher tests. The only CI run of the full suite was in pull request 2, at an old base, and
  it failed.
- **Do:**
  1. Add a job for `cargo test --workspace --locked --no-fail-fast` on Linux, macOS and Windows (`timeout-minutes: 90`).
  2. Add a launcher job on Ubuntu and macOS that does not build anything, because
     `tests/test_launcher_scripts.py` also scans `target/`. On macOS it sets `export PATH="/bin:$PATH"` so it runs under `/bin/bash` 3.2 instead of Homebrew bash.
  3. Add `actions/setup-python@v7` (`python-version: '3.12'`) with `python -m pip install --disable-pip-version-check websockets` for `tests/test_ecosystem_convergence.rs`.
  4. Add `CARGO_PROFILE_DEV_DEBUG=line-tables-only` to keep test binaries small, and configure `Swatinem/rust-cache@v2` with `save-if: ${{ github.ref == 'refs/heads/master' }}` and `cache-on-failure: true`.
  5. Set `concurrency` to `group: ${{ github.workflow }}-${{ github.event_name == 'pull_request' && github.ref || github.sha }}` and `cancel-in-progress: ${{ github.event_name == 'pull_request' }}` so pushes to `master` are never cancelled.
  6. Add a clippy and `--lib` job with `-p rusty_grid_cli --features gpu-wgpu`.
  7. Add `tests/test_agent_mesh_rust_hub_cross_interop.py`, which tests the Rust hub.
  8. Delete `origin/claude/trusting-wing-d9a27c` once the full CI file is in place.
- **Done when:** All jobs are required and green on `master`.

### TEST-7: Run or delete the orphan Python tests

- **Evidence:** Read.
- **Problem:** Seventeen Python files in `tests/` have no runner and no reference, for example
  `test_m7_empirical_challenge.py` and `test_challenger_m5_rapid_disconnect.py`.
  `tests/run_all_mesh_tests.py` calls itself authoritative and tests a Python reference hub only.
  `tests/verify_p2p_persistence.sh` is unreferenced.
- **Do:** Run each file. Add those with value to a runner, and delete the rest. Rename the mesh
  runner to say that it is a simulation.
- **Done when:** Every file in `tests/` is named by a runner or a `[[test]]` entry.

### TEST-19: Fix flaky required tests

- **Evidence:** CI failures of 2026-11-09 and full-suite runs on macOS; each test passed when rerun.
- **Problem:**
  - `queue::tests::rvb_drop_submit_future_mid_ack_loses_task_from_ready_queue` slept 20 ms and
    then aborted the spawned `submit`, assuming it had reached its durable-ack wait. On a loaded
    host the task had not run yet, so the abort left no task at all. It then slept a fixed 300 ms
    for the detached ack task to publish the task.
  - `store::tests::classifying_a_damaged_tail_is_linear` asserted a wall-clock bound (1 s) on
    opening an 8 MiB damaged journal, which fails when the machine is busy.
  - `e2e_cluster::test_tier2_bva_large_output_stream_truncation` waited 5 s for MSYS2 `sh` on a
    Windows runner; the work is tiny, the process start is slow under load.
- **Do:** The first test waits for the task to appear before it aborts, and polls (20 s bound) for
  it to reach the ready queue. The second counts probes and hashed bytes (`ScanWork`) on inputs of
  N and 4N bytes and asserts the algorithm's own bounds. The third waits up to 60 s, with a 40 s
  task timeout, and keeps every assertion. No production code changed.
- **Done when:** Each test passes 50 times in a row under a CPU load, and the Windows jobs pass
  on three runs.

### TEST-20: Fix Windows disconnect stress timeouts

- **Evidence:** CI failures on `windows-latest` in `tests/m4_challenger_2_stress.rs` (for example `master` `5c9e126`, job `129153917725`): `test_adversarial_worker_disconnect_multi_tasks_in_flight` and `test_adversarial_cascading_worker_disconnects_failover` timed out after 12 s in `wait_task` while all nine other tests in the binary finished in under 5 s.
- **Problem:**
  1. `WorkerClient::handle_assign_task_with_checkpoint` emits `TaskProgress { status: TaskStatus::Running }` before `Sandbox::create` and before `configured.spawn`. When a disconnect test aborts the worker as soon as `TaskState::Running` is observed, `WorkerClient::drop` sends `"WorkerClient dropped"` on the task cancellation channel, but `execute_command_workload`, `execute_shell_script_workload`, `execute_rust_compilation_workload`, and `execute_process_supervised_with_workspace` did not check `cancel_rx` before spawning the child process. The dropped worker therefore still spawned `sh.exe` and immediately terminated its Job Object milliseconds later.
  2. When `sh.exe -c "sleep ..."` was already running at abort time, `TreeInner::kill_and_release_once` waited only `MIN_LIVE_KILL_AGE = 80 ms` (and `TreeInner::drop` passed `child_already_exited = true`, skipping the wait altogether). Under 11-way parallel test load on Windows, `80 ms` lands while `sh.exe` is inside MSYS2 `child_info_spawn` starting `sleep.exe` (`msys-2.0.dll` `dll_crt0_1`), stalling MSYS2's global shared-memory initialization lock for 15 s and freezing the surviving `sh.exe` children on the failover workers past the 12 s `wait_task` budget.
  3. `spawn_test_worker` in `tests/m4_challenger_2_stress.rs` and `WorkerClient::for_testing` did not pass `.with_ram_mb(...)`, so every worker spawn ran `sysinfo::System::new()` + `refresh_memory()` synchronously on the single-threaded test runtime.
- **Do:**
  1. Check `cancel_rx` via `check_cancelled_now` before `Sandbox::create` in `execute_command_workload`, `execute_shell_script_workload`, and `execute_rust_compilation_workload`, and both at entry and immediately before `configured.spawn` in `execute_process_supervised_with_workspace`.
  2. Raise `MIN_LIVE_KILL_AGE` in `crates/worker/src/process_tree.rs` from `80 ms` to `250 ms` so both `sh.exe` and its immediate MSYS2 child finish `dll_crt0_1` before `TerminateJobObject`, and pass `child_already_exited = false` in `TreeInner::drop`.
  3. Set `.with_ram_mb(4096)` in `WorkerClient::for_testing` and `spawn_test_worker`.
- **Done when:** `cargo test -p rusty_grid_master --test m4_challenger_2_stress --locked` passes 20 times in a row on Windows inside the full-suite lock, and `runner::tests::test_runner_pre_cancelled_task_does_not_spawn_child` verifies that a pre-cancelled task does not spawn a child process.

### TEST-22: Ignore UUID and metric `4090` tokens in `workflow_modes_test`

- **Evidence:** CI failure on `macos-latest` (run `42690928796`, job `129410135311`) in `tests/workflow_modes_test.rs:313` (`test_research_mode_distributed_and_report`): `output must not print invented RTX / worker-gpu-rtx4090 hardware or node names`.
- **Problem:** `generate_research_report` writes `expected_task_id` and `codec_decoded_task_id` (random UUIDv4 strings `xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx`) plus integer RAM/timing metrics into `target/research_report.json`. `contains_standalone_number` only checked `c.is_ascii_digit() || c == '.'` on the characters immediately before and after `"4090"`, so a UUIDv4 with `-4090-` in its third hyphen-delimited group (or `4090` adjacent to hex digits `'a'..='f'`), or a JSON/unit-labelled metric equal to `4090`, was mistaken for an `"RTX 4090"` GPU model name.
- **Do:** Teach `contains_standalone_number` in `tests/workflow_modes_test.rs` to ignore `"4090"` when adjacent to ASCII hex digits, inside a hyphen-delimited UUID hex group (`-4090-`), or formatted as a JSON numeric value (`: 4090`) or unit-suffixed measurement (`4090 ms`, `4090 mb`, `4090 ops/sec`, `4090 tasks`), while still matching standalone GPU model strings (`"gpu: geforce 4090 ti"`, `"nvidia rtx 4090"`, `"worker-gpu-rtx4090"`, `"4090"`).
- **Done when:** `test_standalone_number_check_ignores_digits_inside_measurements` and `test_research_mode_distributed_and_report` pass in `cargo test -p rusty_grid_cli --test workflow_modes_test --locked`.

## Phase 3: make the documents true

The standard in `docs/README.md` requires "only what the code does". These items apply it.

### TEST-27: Find why the `mesh_auth` slow-consumer test times out on macOS

- **Evidence:** Reproduced once in CI. The `master` push of `6a88eff` on 2026-11-15 failed in
  `workspace tests (macos-latest)` (run `43735264003`, job `132876345396`):
  `test_slow_consumer_is_disconnected_instead_of_buffering_forever` in
  `crates/agent_mesh/tests/mesh_auth.rs` printed "victim disconnected after 20 x 100 KiB sends",
  then the `ping` to the healthy node, a `send_command` with a 3000 ms deadline, ended in
  `Elapsed(())` (line 873). The runs of the next two pushes (`3413fb2`, `96ce571`) passed.
- **Problem:** The test checks that a slow consumer does not hold up other peers. A timeout at this
  line is either a busy runner or the delay the test exists to catch, and nothing in the output
  tells the two apart.
- **Do:**
  1. Run the test 50 times on `macos-latest` and record how often it fails.
  2. Print the time from the victim's removal to the healthy node's answer, so a failure shows
     whether the time went to the flooder's socket, the hub's writer to the healthy node, or the
     healthy node itself.
  3. If the delay is in the hub, fix it there. If it is the runner, raise the deadline by the
     measured margin, with a comment that names this item.
- **Done when:** 50 runs in a row pass on `macos-latest`, and the test prints that latency.
- **Partial (2026-11-16, pull request 263):** not reproduced in 60 local runs on macOS (30 plain, 30
  with 12 CPU-burning processes). Steps 2 and 3 are done: the healthy-node ping now goes through a
  separate client with a 10 s deadline, the dead-peer NACK check has 30 s, and both latencies since
  the victim's removal are printed (at most about 8.7 ms under load). Step 1, 50 runs on
  `macos-latest`, is not done.
- **Implemented (2026-11-17, pull request 303):** cause found, and it was a hub defect, not the
  runner. The second failure (pull request 293, job `134020949727`) showed the healthy node answering
  in 21 ms, so the hub was not stalled; the panic was on the flooder's own `send_command` ("channel
  closed"). After the victim is removed, every 100 KiB message still in the flooder's socket gets a
  NACK queued on the flooder's own outbound queue (8 messages in this test). The hub's read loop can
  run through that backlog faster than its write pump drains the queue, the queue overflows, and the
  hub kicks the flooder as a slow consumer although it reads its replies. Fix in
  `crates/agent_mesh/src/hub.rs`: the connection loop waits for 2 free slots in the sender's own
  queue before it handles a request frame (a peer that never drains is still dropped by the kick, a
  closed writer or the idle timeout). New regression test
  `test_burst_of_nacks_does_not_disconnect_a_healthy_sender` (60 messages to an unknown id with a
  queue of 8) failed on the old code every time and passes now. Local loops of the old slow-consumer
  test did not reproduce the CI failure (0 of 100 under 2 x CPU load before the fix), so the
  regression test is the evidence; after the fix 0 of 100 under load and 0 of 50 plain fail.

### DOC-1: Correct false status claims

- **Evidence:** Reproduced or read, per row.
- **Problem:** `README.md` and `PROJECT.md` use statuses outside the fixed vocabulary, and several
  are false.

| Location | Claim | Correction |
|---|---|---|
| `README.md:103` | Automatic failover: Completed | Design only. `README.md:81` already says there is none |
| `README.md:94`, `README.md:104` | Tools `oxideswarm_status` and others, including cancel | The tools are `status`, `workers`, `submit`, `task_result`, `doctor` |
| `README.md:106`, `README.md:179`, `CLAUDE.md:86`, `DEPLOYMENT.md:256`, `PROJECT.md:96`, `ANDROID_ARCHITECTURE.md:20` | `nativeStartWorkerWithSecret` | This export does not exist. The path is `nativeSetClusterSecret` |
| `README.md:110`, `docs/SECURITY.md:152`, `PROJECT.md:93` | Containment guarantees cleanup | True for Windows Job Objects. On Linux it covers the direct child. macOS has nothing |
| `README.md:87`, `README.md:109` | Reassigned workers resume | `BuiltinTest` only |
| `README.md:111` | Version negotiation prevents frame drops across all components | Partial, see FEAT-8 |
| `README.md:82`, `README.md:108` | Discovery fallback: Completed | Partial, see FEAT-7 |
| `README.md:79`, `README.md:80` | Hardware-verified, launcher tests | No PowerShell, cmd or VBS file runs in any test. Replace with a dated per-platform table |
| `README.md:78` | Your own mapper and reducer | Not reachable from the CLI, see MR-1 |
| `PROJECT.md:89` | LRU and TTL eviction | Finish-order eviction with count and size bounds. TTL is off |
| `PROJECT.md:88` | Least-loaded and round-robin | Only weighted least-loaded runs |
| `PROJECT.md:98` | Bridge routes to a live master | It starts its own embedded master |
| `PROJECT.md:113` | 100 % test pass | Remove |
| `CLAUDE.md:75`, `docs/design/0002-mobile-coordination.md:81` | `AdaptiveKeepaliveController` | No such type exists |
| `packaging/android/README.md:39` | Production-grade, turnkey app | The app does not compile |

- **Do:**
  1. Apply the table.
  2. Map every status to Implemented, Partial, Planned or Design only.
  3. Restore a limitation row that says delivery is at-least-once and that a task can run twice.
- **Done when:** A grep for `Completed`, `Done`, `Hardened` and `COMPLETED` in status cells finds
  nothing. No document contradicts `README.md:81`.

### DOC-2: Remove unbacked performance numbers

- **Evidence:** Reproduced. The source report is outside the repository.
- **Problem:** `README.md:85` calls 341.3 MB/s and "1.55x scaling" an empirical baseline. The source
  compared the cluster only with the phone alone. The same report shows the cluster at 0.37x of the
  Mac alone, at 30 % parallel efficiency. No log or report is in the repository. `README.md:32` and
  `windows/README.md:11` give memory figures without a script.
- **Do:** Delete the numbers, or commit a generated report under `benchmarks/` with hardware, commit
  and date, compared with the fastest single node.
- **Done when:** `git grep 341.3` finds nothing, or it points at a committed report.

### DOC-3: Align design records with the code

- **Evidence:** Read.
- **Problem:**
  - `docs/design/0002-mobile-coordination.md` names `evaluate_effective_concurrency`,
    `compare_node_authority` and `evacuate_before_sleep`, which do not exist. Its battery tiers
    (30, 15, 5) differ from the code (50, 30, 15). Its weights and tie-break differ from
    `crates/core/src/arbitrage.rs`.
  - `docs/design/0003-distributed-file-staging.md` names `SandboxManager::stage_file`,
    `OXIDESWARM_MAX_RETAINED_MB` and `CapabilityMismatch`, which do not exist. It calls inline
    staging content-addressed, but the payload travels in every task message.
  - `docs/design/0004-mcp-and-discovery.md` names arguments the server ignores.
  - `packaging/android/README.md:222` gives 75 degrees C, while the code uses 37, 40 and 42.
    `packaging/android/README.md:126` still gives 3 s heartbeats.
  - All four records say Implemented.
- **Do:** Correct each record from the code. Set the record status and work items to what is true.
- **Done when:** Every symbol in `docs/design` resolves in the code.

### DOC-4: Complete the CLI and environment reference

- **Evidence:** Read.
- **Problem:** `docs/README.md` says flags and variables are listed in `README.md`. About 58 of 96
  long flags and 26 environment variables are missing. Port 8086 is missing. `scripts/mode.sh:275`
  and `windows/switch_role.cmd:50` are not in English.
- **Do:** Complete the tables. Add a test that walks `Cli::command()` and fails if a long flag is
  missing from `README.md`.
- **Done when:** The test passes.

### DOC-5: Document the 3D subsystem

- **Status:** Implemented (`win/doc-5-3d-subsystem`, design `0023`)
- **Evidence:** Reproduced. A grep of all tracked Markdown finds no mention.
- **Problem:** `crates/worker/src/gen3d`, `crates/worker/src/autorig`, `TaskSpec::Generative3D`,
  `TaskSpec::AutoRig` and `engines/` have no design record and no `README.md` section. No installer
  ships `engines/`, and `crates/worker/src/gen3d/runner.rs:128-132` finds the script by a relative path.
- **Do:** Add a design record, a `README.md` section and a `CLAUDE.md` entry. State the limits. Ship
  `engines/` or set `OXIDESWARM_GEN3D_SCRIPT` in the installers.
- **Done when:** `CLAUDE.md` and a design record describe both modules.

### DOC-6: Correct the minimum Rust version

- **Evidence:** Reproduced. `iroh` 1.2.0 and 1.4.1 both declare `rust-version = "1.91"`.
- **Problem:** `Cargo.toml:17`, `CLAUDE.md:7` and `docs/guides/two-machine-cluster.md:11` say 1.75.
  No crate inherits the value, so nothing enforces it.
- **Do:** Set `rust-version = "1.91"`, inherit it in all crates, and correct the documents.
- **Done when:** `cargo metadata` reports `rust_version` 1.91 for every workspace package.

### DOC-7: Add an MCP client setup guide

- **Evidence:** Read. A search for `mcpServers` and `claude mcp add` finds nothing relevant.
- **Problem:** No document shows how to connect a client. `docs/SECURITY.md` does not mention MCP.
- **Do:** Add `docs/guides/mcp-client-setup.md` with snippets for Claude Code, Codex and Cursor. Add
  an MCP row to `docs/SECURITY.md`.
- **Done when:** Each snippet was run once, and the guide follows the guide template.

### DOC-8: Update `CLAUDE.md` and `SECURITY.md` for Windows isolation

- **Evidence:** Read after pull request 152 (`FEAT-3c`) merged.
- **Problem:** Two documents still described the state before pull request 152 (`FEAT-3c`):
  - `CLAUDE.md` omitted that `cargo` under the Windows AppContainer (`--isolation fs|fs+net`) is
    `Planned` as `FEAT-3d` and that a Windows worker refuses `--workspaces` with isolation inside the
    `isolation.rs` summary;
  - `docs/SECURITY.md` listed task sandboxing only under "Not considered / not provided" (section 2)
    and omitted `--isolation off|fs|fs+net` (`crates/worker/src/isolation.rs`, design 0033) from
    section 7 ("Who runs the tasks").
- **Do:** Update `CLAUDE.md` and `docs/SECURITY.md` to match `crates/worker/src/isolation.rs`:
  Windows `--isolation fs|fs+net` uses a per-task Win32 AppContainer (`FEAT-3c`), `cargo` under it is
  `Planned` as `FEAT-3d`, and a Windows worker refuses `--workspaces` with `--isolation fs|fs+net`.
- **Done when:** Neither `CLAUDE.md` nor `docs/SECURITY.md` claims that Windows AppContainer
  isolation is unimplemented or that task sandboxing is not provided when `--isolation fs|fs+net` is
  enabled, and both state the `FEAT-3d` (`cargo`) and `--workspaces` limits on Windows.

- **Note (2026-11-10):** FEAT-3d (pull request 183) merged before DOC-8, so the merged documents
  state the FEAT-3d behaviour (cargo and workspaces under the Windows AppContainer on a worker that
  is not elevated) instead of the limits above.

### REL-1: Restore the Apache licence text

- **Evidence:** Reproduced. `LICENSE-APACHE` is 948 bytes, and sections 2 to 9 are missing.
- **Problem:** The repository is public and offers Apache-2.0 without the licence terms.
- **Do:** Replace the file with the verbatim text from `apache.org/licenses/LICENSE-2.0.txt`.
- **Done when:** The file matches that text.

### REL-2: Remove owner data from tracked files

- **Evidence:** Read.
- **Problem:** `docs/README.md` forbids real addresses and hostnames. The public tree contains owner
  paths, LAN addresses and device identifiers, including in
  `scripts/sync_network/ping_success.txt`, `scripts/run_milestones_1_and_2.ps1:174`,
  `scripts/agent_bridge.py:175`, `tests/test_launcher_scripts.py:416` and
  `ANDROID_ARCHITECTURE.md:654`. The guard test scans only shell, cmd, PowerShell and VBS files.
- **Do (default of D5):**
  1. Delete the two script files, or sanitise them.
  2. Store hashes in the guard instead of the values, and extend it to all tracked text.
  3. Replace addresses with placeholders or RFC 5737 values.
- **Done when:** The extended guard passes.

### REL-3: Small repository hygiene

- **Status:** Implemented (`win/rel-3-repo-hygiene`)
- **Evidence:** Reproduced.
- **Problem:** `run_chaos_test.sh` writes reports to `test_reports/`, which is not in `.gitignore`.
  `.so` files under `jniLibs` are not ignored.
- **Do:** Ignore both paths.
- **Done when:** `git status --short` is empty after `./run_chaos_test.sh`.

## Phase 4: finish partial features

### CLI-1: Add `cancel` and `result` commands

- **Evidence:** Reproduced. The help output has neither. After `submit` without `--wait`, `status`
  shows no stdout.
- **Problem:** `ClientMessage::CancelTask` and `GetTaskResult` exist, but only `doctor` uses them. A
  user cannot cancel a task or read the output of a task submitted without `--wait`.
- **Do:** Add `oxideswarm cancel <ID>` and `oxideswarm result <ID> [--wait] [--json] [--output-dir]`.
- **Done when:** An end-to-end test cancels a running `sleep`, and another reads a finished result.

### MCP-1: Make the MCP server concurrent, with deadlines

- **Evidence:** Read.
- **Problem:** `run_stdio_server` awaits `call_tool` inside its read loop (`crates/cli/src/mcp.rs:1101`,
  `mcp.rs:1172`). While a tool runs, `ping` and `notifications/cancelled` are not read. The wait path
  has no deadline (`mcp.rs:844`). `submit {wait:true}` with no matching worker blocks the server.
  There is no `cancel` tool. Tool defaults set `max_retries` to 0 (`mcp.rs:765`), so a worker drop
  fails the task.
- **Do:**
  1. Use a reader task, a writer task and one spawned task per `tools/call`.
  2. Make `submit` wait against a client deadline, and return `timed_out: true` with the task id.
  3. Add a `cancel` tool, and honour `notifications/cancelled`.
  4. Warn when no worker matches.
  5. Send `notifications/progress` when the client passes a token.
- **Done when:** A test sends `submit {wait:true, require_tags:["absent"], wait_secs:2}` and `ping`.
  `ping` is answered first, and `submit` returns a task id with `timed_out: true`.

### MCP-2: Return outputs safely to an LLM

- **Evidence:** Read.
- **Problem:** `task_result` returns only path, size and hash for artifacts. `data_preview` is the
  whole file through `to_string_lossy` (`mcp.rs:854`), so binaries are corrupted. stdout and stderr
  return up to 10 MiB each, and there is no truncation field. The worker keeps the first 10 MiB
  (`crates/worker/src/runner.rs:278`), so a `cargo test` summary at the end is lost. MCP still sends
  inline files after INT-4.
- **Do:**
  1. Add `output_dir` and `max_output_bytes`, with a default near 16 KiB, head plus tail.
  2. Return `truncated` and `total_bytes`, and remove `data_preview`.
  3. Make `StreamCapture` keep head and tail.
  4. Move MCP file transfer to blobs.
- **Done when:** A task writes a 1 MiB binary and 20 MiB of stdout ending in a marker.
  `task_result {output_dir, max_output_bytes:4096}` gives a file with the right hash, a reply under
  8 KiB, and the marker.

### MCP-3: Correct JSON-RPC and argument handling

- **Evidence:** Read.
- **Problem:** An error is sent in reply to a notification. Valid JSON that is not a request returns
  -32700. An unknown tool returns `isError` instead of -32602. A missing `command` runs a demo echo
  and reports success (`mcp.rs:716`). An unknown `type` becomes a command. `status` hard-codes
  `"cluster_status":"ok"` (`mcp.rs:545`). A failed `doctor` is `isError:false`.
- **Do:** Drop messages without an id silently. Separate -32700 from -32600. Return -32602 for bad
  tools and arguments. Add `required` to the schemas. Use `ClusterStatus`. Align design 0004.
- **Done when:** A test covers `notifications/cancelled` (no output), a missing `method` (-32600) and
  `submit {}` (an error, nothing queued). Update `tests/m5c_challenger_1_stress.rs:326`, which pins
  the old code.

### SCHED-1: Respect `--max-concurrency` on the master

- **Evidence:** Read.
- **Problem:** The worker limits a semaphore (`crates/worker/src/client.rs:874`), but the master
  counts `cpu_cores` (`crates/master/src/scheduler.rs:127`). A worker with `--max-concurrency 1` on 8
  cores receives up to 8 tasks. Seven wait in state `Scheduled` and cannot move to an idle worker.
- **Do:** Add a reserved `slots:<n>` worker tag, which needs no wire change. Use it in
  `effective_max_concurrency`.
- **Done when:** A scheduler test with `slots:2` and an idle peer sends the overflow to the peer.

### SCHED-2: Wire or delete dead flags

- **Evidence:** Read. Confirmed by grep.
- **Problem:**
  - `--max-queue-size` and `--scheduler-policy` are parsed and never read. Queue capacity is
    hard-coded to `None`, so `QueueFull` is unreachable.
  - `RUSTY_GRID_WIRE_CODEC` and config `wire_codec` can never win over the clap default.
  - `[submit] timeout_secs` is never read.
  - TTL eviction code exists, but nothing enables it (`crates/master/src/reaper.rs:45`).
  - A config `p2p_ticket` beats an explicit `--master`.
- **Do:** Wire each one or delete it. Make `wire_codec` optional with a hard error on bad values.
- **Done when:** `--max-queue-size 2` rejects the third submit in a test.
  `RUSTY_GRID_WIRE_CODEC=json` takes effect in a test.

### SCHED-3: Give impossible tasks an outcome

- **Evidence:** Reproduced. `submit --require-tag nothing --wait` warns and then waits without limit.
- **Problem:** `fail_unfulfillable_tasks` has no flag, and it only fills a report that the server
  discards (`crates/master/src/scheduler.rs:111`).
- **Do:** Add a queue timeout, or fail unplaceable tasks when configured.
- **Done when:** An unplaceable task ends Failed when configured, with a test.

### MOB-1: Fix the adaptive keepalive

- **Evidence:** Reproduced. A MacBook worker ignored a 1 s master interval.
- **Problem:** `recommended_keepalive_interval` returns 60 s for any non-cellular link, even with
  active tasks (`crates/core/src/capabilities.rs:180`). Design 0002 says 10 s when active. Laptops
  have no network type, so every battery-powered worker heartbeats at 60 s. The master's dead-worker
  window becomes 240 s. Backpressure reads CPU load from heartbeats, so a laptop can stay
  unschedulable for up to 60 s.
- **Do:** Return 10 s when `active_tasks > 0` on any link. Keep the negotiated interval when the
  network type is unknown. Update `crates/worker/src/heartbeat.rs`,
  `crates/worker/src/client.rs:1937` and `crates/master/src/registry.rs`.
- **Done when:** Unit tests cover Wi-Fi with one task (10 s) and a laptop (negotiated interval). A
  reaper test reaps a battery worker at four times its real interval.

### MOB-2: Keep thermal state on the master

- **Evidence:** Read. `with_live_status` and `live_status` were confirmed.
- **Problem:** `with_live_status` keeps three fields (`crates/core/src/capabilities.rs:71`), and
  `live_status` returns temperature, tiers and network as `None` (`capabilities.rs:82`). The master
  therefore discards what a phone reports, and thermal tiers above Warm can never apply. A phone at
  45 degrees C keeps getting work. A cellular phone's interval resets to 60 s at its next heartbeat.
  `detect_battery` has no Windows branch.
- **Do:** Store the last `MobileLiveStatus` on `WorkerEntry` and `WorkerInfo`, and use it in
  `Engine::new` and `candidates`. Add or document Windows and macOS sensing.
- **Done when:** Telemetry with `thermal_tier: Critical` yields no assignment, `Hot` yields at most
  one slot, and a cellular interval stays 25 s across heartbeats.

### MOB-3: Do not charge evacuation to retry budgets

- **Evidence:** Read.
- **Problem:** Design 0002 says an evacuation does not increase retry counts. `immediate_reschedule`
  removes only the delay, and both counters still increase (`crates/master/src/queue.rs:1669`).
  `tests/mobile_coordination_test.rs:946` asserts the opposite of the design.
- **Do:** Add an uncharged path for announced drains. Correct the test.
- **Done when:** Repeated evacuations leave `failover_count` at 0.

### MR-1: Let a CLI user ship a mapper and reducer

- **Evidence:** Read.
- **Problem:** A path passed to `mapreduce` becomes `ShellScript { script: <the path> }`
  (`crates/cli/src/cli_app.rs:2778`). The file is never read, so it must already exist on every
  worker. `--help` names `uppercase`, which is unsupported. `max` and `min` ignore floats.
- **Do:** Read the file into `script`. Add `--map-cmd` and `--reduce-cmd`. Handle workers without `sh`
  with a clear error.
- **Done when:** `mapreduce --mapper ./local.sh --reducer ./sum.sh` succeeds on a worker that lacks
  those files, in a registered end-to-end test.

### MESH-1: Make `wss://` work in the Rust mesh client

- **Evidence:** Read. `Cargo.toml:33` enables no TLS feature for `tokio-tungstenite`.
- **Problem:** `README.md:467`, `DEPLOYMENT.md:9` and `crates/agent_mesh/src/main.rs:294` advise
  `wss://`, but the Rust client cannot open it. The hub token then travels in clear text.
- **Do:** Enable a rustls feature in `Cargo.toml`, or state that Rust nodes need a VPN or SSH tunnel.
- **Done when:** A test connects the Rust client through a TLS-terminating proxy, and `cargo audit`
  passes.

### MODE-1: Make `ox-mode` report only what happened

- **Evidence:** Read.
- **Problem:**
  - `research distributed` is an in-memory simulation with invented hardware names
    (`run_distributed_benchmark`, `crates/core/src/mode.rs:1083`). It prints
    `PASSED - OPTIMAL DISPATCH`.
  - `research bench` runs a `Vec` loop as its "codec" benchmark.
  - `dev cluster` and `dev stop` return success with canned text in the dashboard path.
  - `dev status` reports healthy against a master that answers 401.
  - `doc export` is a template with false facts (16 MB frames, JSON payload).
  - `doc verify` promises link checks that do not exist.
- **Do:** Label or remove the simulation. Return failure for actions that cannot run. Check HTTP
  status. Generate or delete `doc export`. Move the engine out of `crates/core`.
- **Done when:** `rusty_grid_core` has no `Command::new("cargo")`, no output prints `OPTIMAL` or
  invented hardware, and `dev status` against a protected master exits non-zero.

### BRIDGE-1: Repair `agent_bridge.py diagnose`

- **Evidence:** Read.
- **Problem:** `diagnose` prints `HEALTHY` when peer state is missing, never checks its age, and
  always exits 0. Identity comes from the platform, so two Macs overwrite one state file. Owner
  paths and a private gateway address are hard-coded. `tests/test_agent_bridge.py` covers `watch`,
  `peek` and `send`, but not `diagnose`.
- **Do:** Report `UNKNOWN` for missing or stale peers, and exit non-zero on problems. Add
  `--agent-id`. Remove owner paths. Add tests for `diagnose`.
- **Done when:** `diagnose` without a peer exits non-zero, and two simulated agents on one platform
  do not collide.

## Phase 5: larger designs

Each item below needs a design record before code.

### FEAT-1: Master durability

- **Evidence:** Read. No journal or store exists in `crates/master`.
- **Problem:** The queue and registry are in-memory (`crates/master/src/queue.rs:516`). A master
  restart loses queued tasks, running tasks and finished results. `Register` carries no task list.
  Workers cancel all running tasks when any session ends (`crates/worker/src/client.rs:1144`), so a
  short network drop restarts tasks from zero.
- **Do:**
  1. Add `crates/master/src/store.rs` with an append-only journal for submit, state changes,
     checkpoints and terminal results, and `--state-dir`.
  2. Replay it at start. Scheduled and Running tasks become Queued without charge.
  3. Add a worker grace period and a `ResumeSession { running }` message, gated by a feature
     announcement, so a master can adopt running tasks.
- **Done when:** `tests/master_restart_recovery.rs` kills a master holding one running, two queued
  and one finished task, and restarts it. The result reads back, queued tasks finish, and the running
  task finishes exactly once.

### FEAT-2: Containment after a worker crash

- **Evidence:** Read. `run_chaos_test.sh:309` states that the orphaned shell keeps running.
- **Problem:** `PR_SET_PDEATHSIG` is compiled for `target_os = "linux"` only
  (`crates/worker/src/process_tree.rs:128`), and it covers the direct child. macOS and Android have
  nothing. After `kill -9` of a worker, the task keeps running while the master runs it elsewhere. A
  process that calls `setsid()` also escapes `killpg`.
- **Do:** Add a small supervisor in each task group that holds a pipe from the worker and runs
  `killpg` on end of file. On Linux add `PR_SET_CHILD_SUBREAPER` or cgroup v2.
- **Done when:** A test starts the real worker, runs a script with a child and a grandchild, sends
  `kill -9`, and asserts that all are dead within 5 s on Linux and macOS. `run_chaos_test.sh` fails
  if the victim writes `done:` after the kill.

### FEAT-3: OS-level task isolation

- **Evidence:** Read. The worker uses none of `setuid`, Landlock, seccomp, `sandbox-exec`,
  AppContainer or cgroups.
- **Problem:** The sandbox is a directory with mode 0700. A task can read `cluster.secret`, other
  sandboxes and the cache. Network, memory, disk and process count are unrestricted. This matters
  more when coding agents submit tasks.
- **Do:** Add `--isolation off|fs|fs+net` in `configure_command`. Use Landlock and namespaces on Linux,
  `sandbox-exec` on macOS, and AppContainer or a restricted token on Windows. Advertise an `iso:` tag.
- **Done when:** With `fs`, a probe that reads `cluster.secret` fails on each OS and writing the
  sandbox works. With `fs+net`, an outbound connection fails. `doctor` reports the level.

### FEAT-3d: cargo under Windows isolation

- **Evidence:** Reproduced on `windows-latest` (`Windows Server 2025`, CI run `42608546025`, job
  `129130623354`, commit `c9891fe`). Round 10/11 diagnostics inside the confined AppContainer task
  (`tiny`) proved:
  1. `icacls` on `<rustup_home>\toolchains\staged-x86_64-pc-windows-msvc\bin` and `bin\rustc.exe` shows
     both `oxideswarm.toolchain` (`S-1-15-3-1024-...`) and `CurrentUser` carry `(I)(OI)(CI)(RX)` on
     `bin` and `(I)(RX)` on `rustc.exe` after `SetNamedSecurityInfoW` with
     `UNPROTECTED_DACL_SECURITY_INFORMATION`.
  2. Job object basic limits queried via `QueryInformationJobObject` are `LimitFlags=0x2000`
     (`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` only) and `ActiveProcessLimit=0`.
  3. The spawn working directory `tiny` is readable and traversable (`DIAG_READ_DIR_CWD=Ok(2)`), and
     anonymous pipe creation succeeds (`DIAG_ANON_PIPE=Ok("OK")`).
  4. Spawning `rustc.exe -vV` directly from inside the AppContainer succeeds (`ExitStatus(0)`, prints
     `rustc 1.96.0 (ac68faa20 2026-05-25)`) when `stdin` is `Stdio::inherit()`
     (`DIAG_SPAWN_INHERIT=Ok(ExitStatus(0))` and `DIAG_SPAWN_STDIO_PIPED=Ok(ExitStatus(0))`).
  5. Opening `"NUL"` (`\Device\Null` / `\\.\GLOBALROOT\Device\Null`) from inside the AppContainer
     under an elevated administrator session (`runneradmin` on `windows-latest`) fails Stage 1 of
     `SepAccessCheck` with `ERROR_ACCESS_DENIED (os error 5)`
     (`DIAG_OPEN_NUL_READ=Err(Os { code: 5 })`, `DIAG_OPEN_NUL_WRITE=Err(Os { code: 5 })`,
     `DIAG_OPEN_GLOBALROOT_NULL=Err(Os { code: 5 })`). Because Rust's `std::process::Command::output()`
     (used by `cargo` when running `rustc -vV`, even with `RUSTC` and `CARGO_BUILD_JOBS=1` set) opens
     `"NUL"` (`Stdio::null()`) for `stdin` inside `cargo.exe` before calling `CreateProcessW`, every
     spawn that uses `Stdio::null()` (`DIAG_SPAWN_STDIN_NULL`, `DIAG_SPAWN_FULL_OUTPUT`, `cargo -vV`,
     `cargo build --offline`) fails with `Access is denied. (os error 5)` (`never executed`).
- **Problem:** On Windows, `cargo` builds under `--isolation fs|fs+net` fail when `cargo` spawns
  `rustc -vV` with `Stdio::null()`, and `--workspaces` combined with `--isolation fs|fs+net` is
  refused on Windows (design 0033 D4, design 0039 W2b).
- **Do:** Enable `cargo` builds (and `--workspaces` with `--isolation fs|fs+net`) inside a Windows
  AppContainer without editing Object Manager namespace DACLs (`\??`, `\GLOBAL??`, `\Device\Null`) or
  adding `BUILTIN\Users` / `Everyone` to user or toolchain files, and un-ignore
  `test_cargo_build_offline_works_under_fs` and
  `test_cargo_build_offline_with_a_cached_dependency_under_a_denied_home` on Windows.
- **Done when:** Both `test_cargo_build_offline_works_under_fs` and
  `test_cargo_build_offline_with_a_cached_dependency_under_a_denied_home` pass un-ignored in
  `test_feat3_os_isolation` on `windows-latest`, and workspaces with `--isolation fs|fs+net` work on
  Windows.
  - *Verified (PR 183, CI run `42678349017`):* On a non-elevated Windows worker (`TokenElevation == 0`), `\Sessions\<S>\DosDevices\<UserLogonId>` carries `(A;;CCDCLCSWSDRCWDWO;;;<CurrentUser>)` and `cargo build --offline` (including cached registry dependencies under a denied `USERPROFILE`) and `--workspaces` (`<ws>/tree` and `<ws>/tmp` confined to the per-attempt AppContainer SID `oxideswarm.task.<worker8>.<run_token>`, with non-inherited traverse-only for `oxideswarm.toolchain` on `<workspace_root>` and `<workspace_root>/<ws_name>`) pass under `--isolation fs` and `--isolation fs+net`, both locally and on `windows-latest` (via a non-elevated scheduled task `InteractiveToken` / `LeastPrivilege`), and a worker with default sandbox settings (`%TEMP%\rusty_grid\sandboxes` inside `host.user_home`) starts cleanly.
  - *Residual limitation:* On an **elevated** Windows worker (`TokenElevation != 0`), UAC creates `\Sessions\<S>\DosDevices\<AdminLogonId>` with `TokenOwner = BUILTIN\Administrators` (`O:BA D:(A;OICI;...;;;SY)(A;OICI;...;;;BA)`) and no ACE for `CurrentUser`, and `ntoskrnl.exe!SeIsTokenAssignableToProcess` refuses switching to a fresh logon session via `CreateProcessAsUserW` without `SeAssignPrimaryTokenPrivilege` (`os error 1314`), so `cargo` and other programs that open `NUL` fail with `os error 5` inside the AppContainer. Workers must run non-elevated; when an elevated Windows worker starts with `--isolation fs|fs+net`, it logs a startup `WARN` and drops `tool:cargo` from its advertised capability tags.

### FEAT-4: Remote workspaces for coding agents

- **Evidence:** Read.
- **Problem:** An agent cannot run `cargo test` on another machine through MCP. Staging takes single
  files up to 32 MiB. The sandbox is per task, so `target/` is never reused. `working_dir` is
  hard-coded to `None`. All clients use TCP only, so a client on another network cannot reach a
  P2P-only master. The default timeout is 60 s.
- **Do:** Design a named persistent workspace per worker and a git or bundle source. Add P2P to
  `connect_master` for every client.
- **Done when:** An agent on one machine runs `cargo test -p rusty_grid_core --lib` over MCP on another
  machine at a commit that machine did not have. The second run reuses `target/`.

### FEAT-6: Master failover

- **Evidence:** Read. A grep confirms that nothing constructs `ClaimMasterRole`.
- **Problem:** The three handlers in `crates/master/src/server.rs` (`server.rs:2771`, `server.rs:3106`,
  `server.rs:3718`) fabricate an incumbent ticket with a hard-coded score of 3500. On accept the
  master logs a step-down and keeps running. `calculate_authority_score`, `resolve_election_tie` and
  `check_mobile_master_eligibility` have no production caller. Without a secret, a claim is accepted
  unsigned.
- **Do (default of D1):** Answer every claim with `accepted: false` from one helper. Remove
  `ROLE_ARBITRAGE` from `all_supported()`. Mark the design as Design only. Real failover needs
  replicated state, renewed leases and a fencing epoch, and it starts after FEAT-1.
- **Done when:** A test shows that a claim is refused, and no document says failover is finished.
- **Note (2026-11-15):** Implemented means the refusal of decision D1 (pull request 81, design
  0026). The master still has no failover. Master high availability is SCALE-4, which is Planned
  (DOC-9).

### FEAT-7: Usable rendezvous discovery

- **Evidence:** Read.
- **Problem:** Tier 2 lives on the dashboard listener, which is loopback by default. Tier 3 has no
  host: the registry is a single overwritable slot on a master's own dashboard. `p2p_ticket` is
  outside the MAC (`crates/core/src/discovery.rs:237`) and dropped by `to_beacon`. `https://` URLs
  silently use plain TCP (`discovery.rs:789`). No test reaches Tier 3 through
  `discover_master_tiered`.
- **Do:** Add a standalone registry command with keyed leases. Cover the ticket with the MAC. Reject
  `https://` or implement TLS. State the real preconditions in `README.md`.
- **Done when:** A worker with `--master auto --rendezvous <URL>` registers in a test with Tiers 1 and
  2 blocked. A tampered ticket fails verification.

### FEAT-8: Real protocol versioning

- **Evidence:** Read. `PROTOCOL_VERSION_CURRENT` and `PROTOCOL_VERSION_MIN_COMPATIBLE` were bumped to 2
  in INT-5 F6.
- **Problem:** Before INT-5 F6, `Task` and `TaskResult` changed layout without a version bump. There are
  no golden-bytes tests to catch future unbumped layout changes. The worker ignores the acknowledged
  feature bits, and only `FILE_STAGING` is read. `ClientMessage` has no version, so a client cannot
  learn the master's version. A client from `e1215ee` on a master from `ee2d13b` lost `--wait` without
  an error. A client from `ee2d13b` on a master from `e1215ee` was disconnected with a hint about the
  cluster secret (both reproduced). `bincode` 1.x is unmaintained (RUSTSEC-2025-0141).
- **Do:** Bump on every layout change, enforced by golden-bytes tests. Use the acknowledged bits. Add
  a version to the client path. Plan the move away from `bincode` 1.x.
- **Done when:** A changed `Task` layout fails CI without a bump. Mismatched peers refuse with a clear
  message.

### FEAT-10: Compile crates with cargo

- **Evidence:** Read.
- **Problem:** `RustCompilation` runs bare `rustc` on one entry file, with no dependencies
  (`crates/worker/src/runner.rs:1604`). `Cargo.toml` is written and ignored. `submit --type compile`
  is refused (`crates/cli/src/cli_app.rs:2010`). No capability says that a worker has `rustc`.
- **Do:** Run `cargo build --offline` when `Cargo.toml` is present. Add a `tool:rustc` tag and a CLI
  source option.
- **Done when:** A crate with a local dependency builds from `submit`, and the binary reaches the
  client.

### FEAT-11: Remove dead code and split large modules

- **Evidence:** Read.
- **Problem:**
  - Unused: `dashboard/index.html`, `shaders/hash.wgsl`, `DashboardState`, `prepare_spec`,
    `probe_physical_adapter`, `auto_rig_mesh_async`, `compute_intensity`.
  - `crates/core/src/arbitrage.rs` has almost no production caller.
  - `crates/master/src/server.rs` holds two long connection loops, one for TCP and one for QUIC.
  - The Android bridge exports 13 functions under a `service` package that has no Kotlin class.
  - Leftover sandboxes after a crash have no start-up sweep.
- **Do:** Give each item a production caller or delete it. Merge the two connection loops into one
  session state machine. Add the sweep.
- **Done when:** Each listed symbol has a production caller or is gone.

### MAINT-1: Split the six largest source files and cap file size in CI

- **Evidence:** Measured on `master` `0354b28` on 2026-11-11 (lines; the test part is everything
  from the file's first `#[cfg(test)]`; commits are those on `master` since 2026-10-11):

  | File | Lines | Production | Tests | Commits |
  |---|---|---|---|---|
  | `crates/master/src/queue.rs` | 8,639 | 5,044 | 3,595 | 36 |
  | `crates/master/src/server.rs` | 8,593 | 4,941 | 3,652 | 73 |
  | `crates/worker/src/client.rs` | 8,252 | 4,361 | 3,891 | 87 |
  | `crates/cli/src/cli_app.rs` | 8,099 | 5,149 | 2,950 | 101 |
  | `crates/worker/src/runner.rs` | 6,293 | 4,141 | 2,152 | 79 |
  | `crates/core/src/protocol.rs` | 6,184 | 3,232 | 2,952 | 41 |

  Each holds 120 to 243 functions. FEAT-11 merged the two connection loops into
  `crates/master/src/session.rs` and removed dead code, but split none of these files.
- **Problem:** Each of these files mixes several jobs, so a reviewer has to read thousands of lines
  to judge one change, and parallel pull requests are likely to collide in them: `cli_app.rs`
  changed 101 times in a month. ENROLL-1b adds more to `server.rs`. Nothing stops a file from growing further.
- **Do:** one pull request per file, with no change in behaviour, starting each only after the open
  pull requests that touch that file have merged (ENROLL-1b for `server.rs` and `cli_app.rs`,
  SCHED-4 for `scheduler.rs`):
  1. Move each file's `#[cfg(test)]` module to a file of its own next to it (`mod tests;` with the
     tests in `<module>/tests.rs`). This alone removes about 40 % of the lines.
  2. Split the production code by job, keeping the public API through `pub use` re-exports:
     - `cli_app.rs`: one module per subcommand (`master`, `worker`, `submit`, `status`/`result`/
       `cancel`, `mapreduce`, `workspaces`), with the clap types and `run_cli` dispatch left in
       `cli_app.rs`;
     - `client.rs`: connection, registration and reconnect; the active-task table and assignment
       handling; blob and staging glue;
     - `runner.rs`: process spawning and supervision; output capture and limits; Windows executable
       resolution;
     - `server.rs`: listeners and handshakes; client request handlers; enrollment and members;
     - `queue.rs`: the task state machine; retention and eviction; checkpoints;
     - `protocol.rs`: the message enums; the `Human*`/`Binary*` mirrors and their `From` impls; the
       codec.
  3. Add a size check to `cargo xtask`/`ox-mode test lint` and CI: a Rust source file whose
     production part exceeds 3,000 lines fails, except files on an allowlist that may only shrink.
- **Done when:** none of the six files has a production part above 3,000 lines, the allowlist is
  empty or only shrinks, the CI size check runs on every pull request, and the full test suite and
  `crates/core/tests/data/wire_layout.txt` are unchanged by the split.
- **Note (2026-11-15):** At `c2462e3` the production parts (lines before each file's `mod tests`)
  have grown in five of the six files since the table above: `server.rs` 6,275, `queue.rs` 5,915,
  `cli_app.rs` 5,463, `protocol.rs` 5,269 and `runner.rs` 4,333. `client.rs` is 4,540 after its
  tests moved to `client/tests.rs`. Each new item that touches these files makes the split harder.
- **Note (2026-11-16):** All six files are split (pull requests 230, 231, 242, 249, 250 and 253).
  At `03ace60` their tests live in files of their own, and the whole of each file is at most:
  `server.rs` 2,569 lines, `client.rs` 1,684, `cli_app.rs` 1,629, `queue.rs` 1,469, `runner.rs`
  1,336 and `protocol.rs` 864. The largest new non-test submodule is `queue/lifecycle.rs` with
  1,536 lines. The CI size check of step 3 is built (pull request 259, design 0080): `ox-mode test
  size`, part of `ox-mode test lint` and a step of the Linux `clippy` job, fails when a Rust file's
  production part exceeds 3,000 lines. Three other files were above the limit and are on the
  allowlist at their current size: `crates/worker/src/isolation.rs` 5,377, `crates/cli/src/mcp.rs`
  3,692 and `crates/cli/src/mode_cli.rs` 3,242. Every Done-when point holds, so the item is
  Implemented.
- **Implemented (2026-11-16, pull request 259, [design 0080](design/0080-source-file-size-check.md)):**
  `crates/cli/src/size_check.rs` measures every `*.rs` file (test-only files excepted) and compares
  it with `scripts/rust-file-size-allowlist.txt`: an unlisted file above 3,000 production lines or a
  listed file above its maximum fails; a shrunk entry is a warning. `ox-mode test size` runs it,
  `ox-mode test lint` includes it, and the Linux `clippy` job runs it on every pull request.
  `crates/core/tests/data/wire_layout.txt` is unchanged. The allowlist can now only shrink; splitting
  `isolation.rs`, `mcp.rs` and `mode_cli.rs` would empty it and is not part of this item.

## Phase 6: release

### REL-4: Release workflow and install path

- **Evidence:** Reproduced. `git tag --list` and `gh release list` are empty.
- **Problem:** A user can get a binary only by building from source.
  `packaging/macos/install_mac_daemon.sh` expects a binary that nothing provides. Path dependencies
  have no version, 5 of 6 crates have no description, and `authors` differs from the licence.
- **Do:**
  1. Add `.github/workflows/release.yml` on `v*` tags, with Linux, macOS and Windows archives, with
     and without `gpu-wgpu`, and `SHA256SUMS`.
  2. Add an install section to `README.md`.
  3. Set `publish = false` (default of D4), and align `authors`.
- **Done when:** A tagged release has archives and checksums. A machine without Rust installs from the
  README alone.

### REL-5: Contributor and security files

- **Evidence:** Reproduced.
- **Problem:** No `CONTRIBUTING`, `CHANGELOG`, issue template or dependabot configuration exists.
  `docs/SECURITY.md:172` offers only a public issue for security reports.
- **Do:** Add the files, and add a private reporting route.
- **Done when:** The files exist, and `docs/SECURITY.md` names a private route.

### BENCH-1: Publish benchmarks with the scripts that measured them

- **Evidence:** Read, on 2026-11-08 at `master` `fec164e`. `run_cluster_benchmark.sh` measures
  chunk hashing and matrix products on two nodes. `benchmarks/interconnect/` holds a one-off WAN
  dataset. `ox-mode research` benchmarks the wire codec. Nothing measures per-task scheduling
  overhead, the bytes a repeated blob upload saves, or the user story end to end. HyperQueue, the
  nearest Rust task scheduler, states a per-task overhead below 0.1 ms in its README without a
  method ([HyperQueue](https://github.com/It4innovations/hyperqueue)).
- **Problem:** Writing standard rule 6 forbids a performance number without its script, so the
  project cannot yet state how fast it is.
- **Do:**
  1. Add `benchmarks/grid/` with a script that runs on loopback and measures: (a) no-op task
     throughput and the p50 and p99 time from submit to result, with one to four workers; (b) a
     first and a repeated upload of the same input tree, in bytes sent and time.
  2. After WS-7, add (c): the end-to-end offload of `cargo test` for a fixed public crate at a fixed
     commit, cold and warm, between two machines.
  3. Write each run to a results file with its date, commit, hardware, operating systems and
     network.
  4. Link the results from `README.md`. Compare with HyperQueue only with a run of both on the same
     machine.
- **Done when:** The script runs on macOS, Linux and Windows. Dated records of (a) and (b) on one
  machine, and of (c) on two machines, are committed, and `README.md` links them.

## Phase 6b: machines anywhere

Added on 2026-11-09 after the owner chose ecosystem model A (Q13): each person or team runs a
cluster, and trusted people add their machines to it from any network. Order: ENROLL-1 first,
then JOIN-2 and USB-1 in parallel.

### JOIN-2: Join on a LAN with a short code

- **Evidence:** Read. An invite (ENROLL-1) is a long string, which is hard to type on a fresh
  machine.
- **Problem:** On a LAN, the owner wants to read a code aloud or type it from another screen.
- **Do:** [Design 0052](design/0052-short-code-pairing.md): `invite --code` prints
  `<nameplate>-<word>-<word>`, `join --code <CODE>` finds the inviter with a broadcast query to UDP
  port 8087, and SPAKE2 delivers an invite. The joiner confirms first, and any session that does
  not confirm cancels the code. Approval stays.
- **Done when:** `tests/cli_pair_e2e.rs` passes on Linux, macOS and Windows, and a live LAN run
  between Windows and macOS is recorded.

## Phase 7: scale out

Draft, added on 2026-11-09 with Phase F of [ROADMAP.md](ROADMAP.md). The design is
[design 0050](design/0050-scale-out-architecture.md). Every item starts after the Phase D exit check
of the roadmap, and SCALE-1 comes first because it measures the limits the other items remove. The
owner's decisions Q8, Q9 and Q12 are open.

### SCALE-1: Load harness with simulated workers

- **Evidence:** Read, on 2026-11-09 at `master` `09a8990`. `benchmarks/grid/bench_grid.py` starts one
  to four real workers on loopback on one machine (BENCH-1). No run has more than four workers,
  more than one submitter or a network between master and workers. `docs/README.md` rule 6 forbids
  a scale claim without a script.
- **Problem:** The project cannot say how many workers one master handles, where the limit is, or
  what a mass reconnect costs. Every later item needs that number.
- **Do:**
  1. Add a simulated worker: a `WorkerClient` with a fake runner that answers after a set delay,
     with optional link delay and loss, so 1000 of them run on one host.
  2. Extend `benchmarks/grid/` with scenarios at 100, 500 and 1000 workers and several submitters:
     submit-to-assign p50 and p99, throughput, master CPU and memory, journal bytes, replay time
     and the time to settle after every worker reconnects at once.
  3. Write each run to a results file under the BENCH-1 rules: date, commit, hardware, operating
     system, network and raw runs.
- **Done when:** Dated records at 100, 500 and 1000 simulated workers are committed on one named
  machine, and `README.md` links them.

### SCALE-2: Scheduler and queue at scale

- **Evidence:** Read. `WorkloadScheduler::schedule_batch` and the placement `Engine`
  (`crates/master/src/scheduler.rs`) run in one process. `TaskQueue` holds all tasks in memory
  (`crates/master/src/queue.rs`), bounded by `--max-active-mb` and the retained-task limits. No code
  shares capacity between submitters.
- **Problem:** A placement pass, the memory of finished tasks and one submitter's backlog can each
  limit a grid with hundreds of workers. Nothing measures which does.
- **Do:**
  1. After SCALE-1, profile the pass and fix the largest cost first: index workers by OS,
     architecture, tag and free slots, and raise the micro-batch size where the profile supports it.
  2. Move cold terminal results out of memory into the store.
  3. Add fair share: a queue per submitter (the ECO-3 identity) with weighted round robin before
     placement. Until ECO-3 exists, the submitter key is the connection's client id.
  4. Keep the 8-state machine and `Engine::hold_back`.
- **Done when:** The SCALE-1 numbers improve by the bound the owner sets (Q8) with identical
  placement results on the existing scheduler tests, and a test shows a second submitter's task
  placed while a first submitter's backlog is larger than the free slots.
- **Implemented (`2026-11-15`):** Pull request 227 (`ec228a01`, [design 0050](design/0050-scale-out-architecture.md))
  added `WorkerIndex` / `RegisteredIndex` capability and resource bitsets (`crates/master/src/worker_index.rs`,
  including `WORKSPACES` and `TREE_V2`), `SCAN_PAGE = 512`, `DEFAULT_MASTER_MAX_BATCH_SIZE = 1024`
  (`--max-batch-size`), adaptive debounce and 16-batch drain, weighted round-robin per-submitter
  `FairShareQueues` (`crates/master/src/fair_share.rs`, `queue.rs`), dashboard broadcast receiver guards,
  and single-flight batched `ColdResultStore` disk spill (`crates/master/src/cold_results.rs`), with
  post-`SCALE-2` 3-run `scale_harness.py` records at `500w` (`409.1` vs `375.2 tasks/s`, `16.69` vs
  `31.01 ms` unloaded assign p50) and `1000w` (`414.4 tasks/s`, `91.8 MiB` steady RSS, `0` failures).
- **Note (2026-11-15):** The 1000-worker run is the stretch run of Q8 and shows no gain: the burst
  rate went from `419.7` (SCALE-1) to `414.4 tasks/s`, and the burst assign p50 from `17.4` to
  `18.9 ms` (`benchmarks/grid/results/2026-11-14-scale1-windows-amd64-1000w.json`,
  `2026-11-15-scale2-windows-amd64-1000w.json`). The status rests on the 500-worker target
  (DOC-9).

## Phase 8: product and ecosystem

Draft, added on 2026-11-09 with Phase G of [ROADMAP.md](ROADMAP.md). The items start after the
Phase D exit check and run in parallel with Phase 7 where they do not depend on it. The owner's
decisions Q10 and Q11 are open. Items that overlap an existing one say which they extend.

### ECO-4: Observability

- **Evidence:** Read. No Prometheus or OpenTelemetry code is in `crates/`. The dashboard
  (`crates/master/src/web_ui.rs`) shows live state to a person.
- **Problem:** An operator cannot alert on queue depth or scheduling latency with the tools they
  already use.
- **Do:**
  1. Add `/metrics` in Prometheus text format on the dashboard listener, behind the Observer scope:
     queue depth by state, tasks per second, scheduling latency histogram, workers by state and
     link, blob bytes, journal size, store health.
  2. Add OpenTelemetry traces behind an opt-in Cargo feature, exported over OTLP.
  3. List new dependencies for the owner before adding them. Never put the secret or task inputs in
     a metric label or span.
- **Done when:** A test scrapes `/metrics` from a running master and finds each listed series, a
  request without the Observer scope is refused, and `cargo check -p rusty_grid_master
  --no-default-features` still passes.

### LOG-1: Close P2P client endpoints cleanly and send client logs to stderr

- **Evidence:** Reproduced on 2026-11-09 during two-machine P2P runs and the Windows USB kit check.
- **Problem:**
  1. Every CLI client command (`doctor`, `workers`, `submit`, `status`, `mcp`) that connects over a
     P2P ticket logs `ERROR iroh::socket: Endpoint dropped without calling Endpoint::close.
     Aborting ungracefully.` on exit because `P2pClientGuard::drop` spawns a detached
     `endpoint.close()` task that is aborted when the CLI drops the Tokio runtime, and
     `BlobConnection::drop` only aborts its reader task without awaiting endpoint shutdown.
  2. `run_cli_with` in `crates/cli/src/cli_app.rs` routed `INFO` tracing events to `stdout` for
     every command except `mcp`, so `iroh` `INFO` lines printed before `workers --json` output over
     a P2P ticket and broke JSON parsing.
- **Do:**
  1. Add explicit `close` / `start_close` and `drain_pending_p2p_client_closes` in
     `crates/core/src/transport.rs`, close client transports and `BlobConnection`s in
     `crates/cli/src/blobs.rs`, `crates/cli/src/doctor.rs`, `crates/cli/src/mcp.rs` and
     `crates/cli/src/cli_app.rs`, and drain pending client endpoint closes before `run_doctor`,
     `McpState::close` and `run_cli_with` return.
  2. Route all tracing of client commands (`submit`, `status`, `result`, `cancel`, `workers`,
     `workspaces`, `mapreduce`, `doctor`, `join`, `secret`, `mcp`) to `stderr` in `run_cli_with`,
     keeping `master`, `worker` and `mode` unchanged.
- **Done when:** `test_log_1_cli_client_commands_over_p2p_ticket_close_endpoints_without_stderr_error`
  in `tests/test_p2p_wan_fallback.rs` runs `status`, `workers --json` and `submit` (with input and
  output blob transfers) over a P2P ticket with `RUST_LOG=info,iroh=info`, finds no
  `Endpoint dropped without calling` line in `stderr`, and parses the `stdout` of `workers --json`
  as JSON.

### BUG-18: Resolve bare commands under Windows isolation against `System32` before `Git\usr\bin`

- **Evidence:** Reproduced on 2026-11-10 during the Windows USB kit isolation check: `oxideswarm submit -- whoami` against a worker started with `--isolation fs` failed with exit code `-1073741502` (`0xC0000142` / `STATUS_DLL_INIT_FAILURE`).
- **Problem:** `augment_windows_path` and `resolve_windows_executable` (`crates/core/src/capabilities.rs:1602,1699`) checked `C:\Program Files\Git\usr\bin` ahead of `%SystemRoot%\System32`. Bare `whoami` therefore resolved to MSYS2's `C:\Program Files\Git\usr\bin\whoami.exe`, which links `msys-2.0.dll`. Inside a Win32 AppContainer, `msys-2.0.dll` fails during `DllMain(DLL_PROCESS_ATTACH)` Cygwin shared-heap initialization (`0xC0000142`), whereas `C:\Windows\System32\whoami.exe` is a native Win32 binary that exits `0`.
- **Do:** Add `augment_windows_path_for_isolation` and `resolve_windows_executable_for_isolation` in `crates/core/src/capabilities.rs` and call them from `TaskRunner::execute_process_supervised_with_workspace` (`crates/worker/src/runner.rs`) when `self.config.isolation != IsolationLevel::Off`, so native directories (`%SystemRoot%\System32`, `Git\cmd`, `Git\bin`) are searched before MSYS2 `usr\bin` directories (`Git\usr\bin`, `msys64\usr\bin`).
- **Done when:** `test_windows_executable_resolution_for_isolation_prefers_system32_over_msys_usr_bin` in `crates/core/src/capabilities.rs` and `test_bare_whoami_succeeds_under_isolation_fs` in `crates/worker/tests/test_feat3_os_isolation.rs` pass, and a live `oxideswarm submit -- whoami` against a Windows worker running with `--isolation fs` exits `0`.

### USB-2: Opt-in Windows isolation in the USB kit

- **Evidence:** Reproduced on 2026-11-10 ([design 0055](design/0055-usb-kit-windows-isolation.md)).
- **Problem:**
  1. `windows::acl_grant_manifest_path` in `crates/worker/src/isolation.rs` checked only `RUSTY_GRID_ACL_GRANT_LOG` and then `<user_home>/.oxideswarm/windows_acl_grants.txt`, ignoring `OXIDESWARM_HOME`. Starting a USB-kit worker with `--isolation fs` on Windows therefore wrote `windows_acl_grants.txt` into the machine owner's `%USERPROFILE%\.oxideswarm`.
  2. `packaging/usb/install.ps1` had no `-Isolation` flag, `packaging/usb/make_kit.sh` could not bake an isolation mode into the kit, `packaging/usb/uninstall.ps1` deleted `$InstallRoot` without revoking `oxideswarm.toolchain` ACL grants on `%RUSTUP_HOME%` and `%CARGO_HOME%`, and neither `install.ps1` nor `uninstall.ps1` had `[CmdletBinding()]` to reject unknown parameters.
- **Do:**
  1. Update `windows::acl_grant_manifest_path` (and `windows/revoke-isolation-acls.ps1`) to use precedence `RUSTY_GRID_ACL_GRANT_LOG > <OXIDESWARM_HOME>/windows_acl_grants.txt > ~/.oxideswarm/windows_acl_grants.txt`, with unit test `test_acl_grant_manifest_path_precedence_honours_oxideswarm_home`.
  2. Add `[CmdletBinding()]` to `packaging/usb/install.ps1` and `packaging/usb/uninstall.ps1`; add `-Isolation off|fs` (default `off`) to `install.ps1`, `--isolation off|fs` (default `fs`) to `install.sh`, and `--isolation off|fs` to `make_kit.sh` (writing `$KIT/isolation` and copying `windows/revoke-isolation-acls.ps1` into `$KIT/revoke-isolation-acls.ps1`).
  3. Update `packaging/usb/uninstall.ps1` to stop the worker and run `revoke-isolation-acls.ps1` against `$InstallRoot\home\windows_acl_grants.txt` before deleting `$InstallRoot`.
- **Done when:** `tests/test_launcher_scripts.py` passes, and a live Windows install with `-Isolation fs` runs the trial-guide checks, revokes all `oxideswarm.toolchain` ACEs on `uninstall.ps1`, and leaves `%USERPROFILE%\.oxideswarm` untouched.

### USB-3: Keep the Windows USB install folder apart from the default workspace root

- **Evidence:** Found by reading on 2026-11-10 during the review of pull request 204. On Windows the
  USB kit installs into `%LOCALAPPDATA%\OxideSwarm` (`packaging/usb/install.ps1` and
  `uninstall.ps1`, parameter `-InstallRoot`), and `worker::workspace::default_root_with`
  (`crates/worker/src/workspace.rs`) puts the default workspace root at
  `%LOCALAPPDATA%\oxideswarm\workspaces`. NTFS compares names without case, so both are the same
  folder.
- **Problem:** `uninstall.ps1` removes `$InstallRoot` recursively. On a user account that also runs
  a worker from a normal install with `--workspaces` and the default root, removing the USB kit
  deletes that worker's workspaces. The reverse also holds: the other worker's workspace store sits
  inside the kit's private install folder and its lock and state files can make the kit's checks
  refuse the folder or the uninstall fail half way.
- **Do:**
  1. Give the kit an install folder name that no other component uses (for example
     `%LOCALAPPDATA%\OxideSwarm-USB`), and keep reading the old name for upgrades and uninstall.
  2. Make the kit always pass `--workspace-root <install folder>\workspaces`, also for the default
     folder, so the kit never shares the default root.
  3. Make `uninstall.ps1` refuse to delete a folder that holds a `workspaces` directory the kit did
     not create, and say which worker uses it.
- **Done when:** a test in `tests/test_launcher_scripts.py` shows that the kit's install folder and
  workspace root never equal `default_root_with(Platform::Windows, ..)` for any case spelling, and a
  live Windows run with a normal worker using the default root plus the USB kit installed and then
  uninstalled leaves the normal worker's workspaces intact.

### USB-4: Size the kit's task slots to the machine's fast cores

- **Evidence:** Measured on 2026-11-10 (see SCHED-4): the kit gives a worker half of the logical
  processors as task slots (`packaging/usb/install.ps1`, `[Environment]::ProcessorCount / 2`, and
  the same rule in `install.sh`). On an Intel Core i7-1355U laptop that is 6 slots, and 6 concurrent
  CPU tasks took 39.6 s each against 2.9 s for 2.
- **Problem:** Laptop processors with few performance cores, a low power limit and the owner's own
  programs running slow down far more than linearly when every slot is busy, so the kit's default
  makes a borrowed laptop both slow for the cluster and sluggish for its owner.
- **Do:**
  1. On Windows, count the performance cores (`GetSystemCpuSetInformation`, highest
     `EfficiencyClass`) and on macOS the performance level (`sysctl hw.perflevel0.logicalcpu`); use
     their logical processors, at most half of all, as the default slots. Fall back to a quarter of
     the logical processors when the count is unavailable and the machine has a battery.
  2. Let `-Slots N` / `--slots N` override it, keep it on `-Update`, and print the chosen value.
- **Done when:** `tests/test_launcher_scripts.py` covers the rule with fake processor data, and on
  the i7-1355U laptop the kit picks 4 slots or fewer.

## Phase 10: lasting value

Added on 2026-11-10 from [design 0058](design/0058-lasting-value.md), at the owner's word, for its
first items. The record plans assurance, a written specification, citable evidence and stewardship,
and adds no feature. Its other items (LV-2, LV-3, LV-6 to LV-9, LV-11 to LV-13, LV-16 and
LV-17) stay in the design record until the owner adds them here. LV-4 and LV-15 are in Phase 13
since 2026-11-17. The four items below do not depend
on each other and can run in parallel. None changes the wire format, a CLI flag or a default, so
none needs a design record of its own.

### LV-1: Fuzz targets for the parsers of untrusted bytes (first targets)

- **Evidence:** Read on 2026-11-10 at `master` `2f22c48`. The repository has no `fuzz/` directory
  and no property-testing dependency. Bytes from another machine reach these parsers, the first two
  before authentication completes: the frame reader and format dispatch of `MessageTransport`
  (`crates/core/src/protocol.rs`: `recv_raw_frame_max`, `recv_msg_with_codec_max`, frames up to
  `MAX_FRAME_SIZE`, 64 MiB), the handshake frames (`crates/core/src/cluster_auth.rs`,
  `crates/core/src/noise_auth.rs`), and `TreeManifest::decode` (`crates/core/src/blob.rs`). The
  `audit` job of CI reports `bincode` 1.x as unmaintained.
- **Problem:** Nothing has fed these parsers malformed input at scale. A panic or an unbounded
  allocation in one of them is a remote denial of service, and before the handshake it needs no
  secret.
- **Do:**
  1. Add a `fuzz/` crate for `cargo fuzz` (libFuzzer), outside the workspace members (add it to
     `exclude` in the root `Cargo.toml`), with a `Cargo.lock` of its own. List its dependencies for
     the owner in the pull request.
  2. Write three targets:
     - `inbound_frame`: the input is a byte stream that a `MessageTransport` over an in-memory
       stream reads as `InboundMessage`, once per `WireCodec`, with a small frame limit. The frame
       reader, the format byte dispatch and both decoders are then in the path.
     - `server_handshake`: the input is what a peer sends first. Run the server side of the
       handshake against it over an in-memory stream, with a fixed test secret and a fixed test
       key.
     - `tree_manifest`: the input goes to `TreeManifest::decode`.

     If a parser is only reachable through private functions, expose it behind a Cargo feature
     `fuzzing` of `rusty_grid_core` that is off by default. Do not change a normal build.
  3. Seed each target's corpus with valid encodings, for example the messages that
     `crates/core/src/wire_layout_tests.rs` builds.
  4. Run each target for one hour locally. Record the date, the machine, the number of executions
     and every finding in `fuzz/README.md`. This is spike R12 of design 0058.
  5. For each finding follow rule 4: a failing test first, then the fix. A finding that a remote
     peer can trigger goes through the private route of `docs/SECURITY.md` before any public pull
     request.
  6. Add a CI job on Linux that builds the targets and runs each for 60 seconds on pull requests,
     and for 10 minutes each on the weekly schedule. The job pins a dated nightly toolchain,
     because `cargo fuzz` needs one. Keep crash inputs as files under `fuzz/regressions/`, and run
     them in the same job.
- **Done when:** The three targets build and run in CI. `fuzz/README.md` holds the one-hour record.
  A malformed frame placed in `fuzz/regressions/` is read without a panic. The later targets of
  design 0058 LV-1 (the encrypted record reader, the join token and the invite, the journal reader,
  the JSON-RPC reader of `oxideswarm mcp`) are listed in `fuzz/README.md` as open.
- **Status:** Partial (2026-11-17, pull request 270). Built: `fuzz/` with the targets `inbound_frame`,
  `server_handshake` (v2 and v3) and `tree_manifest`, seed corpora, `fuzz/regressions/`, `fuzz/README.md`
  with the one-hour record (no finding; 24.5 million, 7.7 million and 34.6 million executions) and the
  `fuzz` CI job on a pinned nightly. Every function the targets call is public, so no `fuzzing`
  feature was needed. The local runs used a stable toolchain with `-s none`. Open: the `fuzz` job has
  not run yet on GitHub (Done when: the three targets build and run in CI); Implemented once it is green.
- **Implemented (2026-11-18):** the `fuzz` CI job runs the three targets green on `master`
  (first green run on `master` at `b486137`); the later targets are LV-18.

### LV-18: Fuzz targets for the remaining parsers of untrusted bytes

- **Evidence:** Read on 2026-11-18 at `master` `b486137`. `fuzz/` (LV-1, pull request 270) has three
  targets (`inbound_frame`, `server_handshake`, `tree_manifest`), and `fuzz/README.md` lists the later
  targets of design 0058 LV-1 as open: the encrypted record reader (`AeadReaderSession::decrypt_frame`
  in `crates/core/src/protocol/codec.rs`), the join token and the invite (`JoinToken::decode` in
  `crates/cli/src/join.rs`, `InviteToken::decode` in `crates/cli/src/members_cmd.rs`, the
  `invite.oxs` file in `crates/cli/src/usb_kit.rs`), the journal reader of the master state store
  (`read_journal` and `read_snapshot` in `crates/master/src/store.rs`, both private) and the JSON-RPC
  reader of `oxideswarm mcp` (inline in `run_stdio_server`, `crates/cli/src/mcp.rs`). Exit check E4
  of [design 0088](design/0088-evaluation-of-2026-11-17-and-what-finished-means.md) asks for a fuzz
  target in CI for every parser of untrusted bytes.
- **Problem:** These parsers read bytes that another machine or person wrote: every frame after the
  handshake, a pasted token, a file on a USB stick, the state directory after a crash. Nothing has
  fed them malformed input at scale. A Poly1305 tag guards the record layer and BLAKE3 hashes guard
  the journal and the snapshot, so random bytes never reach the decoders behind them.
- **Do:**
  1. `aead_record`: the record layer with keys derived from a fixed test session key: a byte stream
     of a peer without the key, and records sealed by the client side that the fuzzer keeps,
     damages, drops, replays or cuts. The reader must accept exactly the intact records in order
     before the first bad one, return each plaintext unchanged, and refuse everything after it.
  2. `join_invite`: the text piped into `oxideswarm join`, join token and invite payloads with a valid
     tag or checksum added by the target, and `invite.oxs` files (header, and decryption with a test
     passphrase when the Argon2id cost is small). Decode, encode, decode gives the same value.
  3. `store_journal`: the journal and snapshot readers on bytes in memory, raw or with the target
     writing headers, lengths and hashes around the fuzzer's payloads. The readers are private, so
     `rusty_grid_master` gets a Cargo feature `fuzzing`, off by default, with `store::fuzzing`;
     `read_journal` and `read_snapshot` are split into a file read and a parse, with no change in
     behaviour. The valid prefix must read again without a torn tail, and accepted records and
     snapshots written again must read back equal.
  4. The JSON-RPC reader of `oxideswarm mcp`, through a public function that takes one line.
  5. Seeds, regression inputs, the three targets in the `fuzz` CI matrix (60 s on pull requests,
     10 minutes weekly), a 5-minute local run of each recorded in `fuzz/README.md`.
- **Done when:** The four targets build and run in the `fuzz` CI job, and `fuzz/README.md` holds a
  local run of each with its findings.
- **Status:** Partial (2026-11-18, pull request 326). Built: `aead_record`, `join_invite` and
  `store_journal` (steps 1 to 3 and 5), with no finding in 5 minutes each. Open: step 4. No public
  function takes one line of the MCP reader, and MAINT-2 is splitting `mcp.rs` into submodules now;
  the target needs a function such as `parse_request_line(line) -> Result<JsonRpcRequest,
  JsonRpcResponse>` cut out of `run_stdio_server`. Also open until it runs: the `fuzz` CI job of the
  three new targets.
- **Implemented (2026-11-18, pull request 335):** step 4. `run_stdio_server` now calls
  `parse_request_line(line) -> Result<Option<JsonRpcRequest>, Box<JsonRpcResponse>>` (`Ok(None)`: a
  line that gets no reply) and `parse_tool_call(id, params) -> Result<(String, Value),
  Box<JsonRpcResponse>>` (the `tools/call` checks, `validate_tool_arguments` included), both
  `#[doc(hidden)] pub`; the
  replies to 48 test lines are byte for byte those of `master` `3a0f7c5`. The target
  `mcp_request_line` with seeds, regression inputs and its `fuzz` CI job; a 5-minute local run
  (1,752,586 executions, no finding) is in `fuzz/README.md`. The `fuzz` job runs the four LV-18
  targets on pull request 335.

## Phase 11: findings of the audit of 2026-11-15

These items come from a read-only audit of `master` at `c2462e3` (after 0.2.0 and SCALE-2). Line
numbers refer to that commit. Evidence "Read" means the code was read and nothing was run, so
confirm each item before you fix it (rule 5). On that commit `cargo clippy --workspace
--all-targets -- -D warnings`, `cargo fmt --all -- --check` and `cargo test --workspace --lib`
(1139 passed, 0 failed) passed on an Apple Silicon Mac.

### BUG-19: Bound the data-lane backlog of a QUIC worker session

- **Evidence:** Read.
- **Problem:** In the QUIC branch of `WorkerSession`, `router_task` moves `AssignTask`,
  `AssignTaskWithCheckpoint`, `CancelTask` and `BlobReply` into `pending_data` whenever the 64-slot
  `data_out_tx` is full (`crates/master/src/session.rs:854-925`). `pending_data` is a `VecDeque`
  with no bound. The router keeps draining the 128-slot `outbound_rx` (`session.rs:647`) into it,
  so that channel no longer limits memory. The data writer awaits `send_msg` with no write
  timeout (`session.rs:957-983`). A worker that keeps its control lane alive but stops reading its
  data lane makes the master hold every assignment and every `BlobReply` chunk (up to 4 MiB each)
  for it. The comment on `BlobService` (`crates/master/src/server.rs:2321-2324`) says the bounded
  channel caps this buffering.
- **Do:**
  1. Cap `pending_data` by message count and by bytes.
  2. At the cap, stop reading `outbound_rx`, so its bound pushes back on the senders again.
  3. Add a write timeout on the data lane. When it expires, end the session with a logged reason,
     so the failover sweep requeues its tasks.
- **Done when:** A test with a QUIC worker that heartbeats but never reads its data lane shows the
  master's buffered bytes for that session stay under the cap. The session ends within the
  timeout, and its tasks are requeued.
- **Implemented (2026-11-15, pull request 236, [design 0069](design/0069-bounded-quic-data-lane.md)):**
  the data-lane backlog of a QUIC session is capped by count and bytes, and the data lane has a
  write timeout (`crates/master/src/session.rs`).

### BUG-20: Keep one slow worker from stalling every dispatch

- **Evidence:** Read.
- **Problem:** `schedule_batch` delivers its assignments one after another
  (`crates/master/src/scheduler.rs:1956-1969`). Each delivery awaits `sender.reserve()` on the
  worker's channel with no timeout (`scheduler.rs:2019`). The comment above it says a slow worker
  can keep the pass there for a long time. While one worker's channel is full, no other worker
  receives an assignment from that pass. The reaper does not end such a session, because any
  received byte counts as proof of life and the timeout is 10 s (`crates/master/src/reaper.rs:29`).
- **Do:** Deliver without blocking the pass on one worker. Either reserve with `try_reserve` and
  requeue the task for that worker when the channel is full (`requeue_dispatch_failure`), or
  dispatch to different workers concurrently with a per-worker deadline. Keep the check under the
  queue lock (`TaskQueue::deliver_if_scheduled`) that a cancelled task is never assigned.
- **Done when:** A test with one worker whose channel stays full and one healthy worker shows the
  healthy worker receives its assignments within one scheduling interval. The existing scheduler
  tests and the `Engine::hold_back` tests pass unchanged.
- **Implemented (2026-11-15, pull request 241, [design 0075](design/0075-non-blocking-dispatch.md)):**
  `WorkloadScheduler::dispatch` tries `try_reserve` first and defers a full channel instead of
  holding the pass (`crates/master/src/scheduler.rs`).

### BUG-21: Resolve relative staged executables against the task working directory on Windows

- **Evidence:** Reproduced in the `R20` Windows probe (`OxideSwarm_Agent_Hub`
  `test_runs/win_r20_gpu_probe.md`, row `W2_relpath_direct`). Staging `gemm_bench.exe` with
  `submit --input` and running `--command ./gemm_bench.exe -- 1024` fails on Windows under
  `--isolation off` with `os error 2` at spawn and under `--isolation fs|fs+net` with
  `"program './gemm_bench.exe' was not found in PATH"`.
- **Problem:** `resolve_windows_executable` and `resolve_windows_executable_for_isolation`
  (`crates/core/src/capabilities.rs`) and `windows::resolve_program`
  (`crates/worker/src/isolation.rs`) checked relative paths containing `/` or `\` against the
  worker process's current directory instead of the task's working directory (`eff_cwd` inside
  the sandbox), and `CreateProcessW` / `CreateProcessAsUserW` passed `NULL` for `lpApplicationName`.
- **Do:** Resolve `./`, `.\`, `sub/`, and `sub\` against the task working directory inside the
  sandbox across `--isolation off`, `fs`, and `fs+net`, preserve `.com`/`.exe`/`.bat`/`.cmd` and
  `MZ` rules of [design 0025](design/0025-windows-executable-resolution.md), refuse paths that
  escape the sandbox (`..`, drive/root prefixes, junctions targeting outside), wrap `.bat`/`.cmd`
  via `cmd.exe /d /s /c`, and pass an absolute `lpApplicationName` to `CreateProcessW` /
  `CreateProcessAsUserW`.
- **Done when:** Windows unit, `TaskRunner`, AppContainer (`test_feat3_os_isolation`), and CLI E2E
  (`cli_isolation_e2e`) tests stage a small `.exe` and run `./prog.exe`, `.\prog.exe`, and
  `sub/prog.exe` under `--isolation off` and `fs`, and refuse `..\prog.exe` without running
  outside decoys.
- **Implemented (2026-11-16, [design 0077](design/0077-windows-relative-task-executables.md)):**
  implemented relative task executable resolution and sandbox confinement in
  `crates/core/src/capabilities.rs`, `crates/worker/src/runner/supervise.rs`, and
  `crates/worker/src/isolation.rs`.

### SEC-10: Stop a working-directory config file from configuring `worker` and `submit`

- **Evidence:** Read.
- **Problem:** Every command except `mcp` loads `./rusty-grid.toml` or `./rusty-grid.json` from
  the working directory before `~/.oxideswarm/rusty-grid.toml`, and prints nothing
  (`crates/cli/src/config.rs:466-477`, called from `crates/cli/src/cli_app.rs:1239`). That file
  can set `master`, `secret` and `insecure` (`config.rs:190-210`). The quick start tells users to
  run `submit --workspace` from a project root (`README.md:81-85`). A checkout from someone else
  that ships a `rusty-grid.toml` could point `submit` at another master, or start `worker`
  against a master that then runs its commands on this machine. `mcp` already skips these files
  and prints a notice (`config.rs:452-455`).
- **Do (default of D8):** Skip a working-directory config file for every command, with the same
  stderr notice that `mcp` prints. `--config <PATH>` and `RUSTY_GRID_CONFIG` still load any file.
  Update `README.md`, `CLAUDE.md` (the `config.rs` precedence line) and a design record, in the
  same pull request (rule 7).
- **Done when:** A test runs `submit` and `worker` in a directory with a `rusty-grid.toml` that
  names another master. Neither connects to that master, and both print the notice.

### SEC-11: Refuse instead of falling back to the elevated token under Windows isolation

- **Evidence:** Read.
- **Problem:** On an elevated Windows worker, the AppContainer child starts from a restricted
  (LUA) token through `CreateProcessAsUserW` (`crates/worker/src/isolation.rs:4646-4670`). If
  `get_or_init_worker_lua_token` returns `None` (`isolation.rs:2321-2381`), or
  `CreateProcessAsUserW` fails, the code calls `CreateProcessW` with the worker's own elevated
  token (`isolation.rs:4674-4689`). It logs nothing. The child still runs in the AppContainer, but
  its token derives from the elevated token that the LUA path exists to avoid.
- **Do:** When the worker is elevated and the LUA path fails, fail the task with the Win32 error,
  and log the reason once. Keep `CreateProcessW` for a worker that is not elevated.
- **Done when:** A Windows test that forces the LUA token to fail sees the task fail with a
  message naming the cause, and no task process starts.
- **Implemented (2026-11-16, pull request 256, [design 0078](design/0078-no-elevated-token-fallback.md)):**
  an elevated worker whose LUA token is missing, or whose `CreateProcessAsUserW` fails, fails the
  spawn with the Win32 error and logs the reason once (`crates/worker/src/isolation.rs`). The
  Windows tests `test_windows_elevated_worker_refuses_when_lua_token_is_missing` and
  `test_windows_elevated_worker_refuses_when_create_process_as_user_fails` cover it.

### SEC-12: Test and narrow the macOS isolation profile

- **Evidence:** Read. The escape below is inferred and has not been tried.
- **Problem:** The Seatbelt profile starts with `(allow default)` and then denies file writes
  outside the sandbox and reads of selected directories (`crates/worker/src/isolation.rs:640-667`).
  Mach and XPC service lookups stay open apart from the pasteboard, which design 0033 states. A
  task might ask `launchd` or another XPC service to start a process outside the sandbox, for
  example with `launchctl submit`. Linux uses an allowlist (Landlock and seccomp) instead.
- **Do:**
  1. Add tests under `--isolation fs` that try `launchctl submit`, `open -a`, and `osascript`, and
     that hard-link a file from the owner's home into the sandbox.
  2. Deny `mach-lookup` by default, with an allowlist of the services that `cargo`, `rustc`,
     `git` and the system linker need.
  3. Record the result and any remaining limit in design 0033 and `docs/SECURITY.md`.
- **Done when:** The tests above pass on `macos-latest` in CI, and `cargo build` of a workspace
  still works under `--isolation fs` on macOS.
- **Implemented (2026-11-16, pull request 257, [design 0033](design/0033-os-level-task-isolation.md)):**
  the macOS profile denies `mach-lookup` and `job-creation`; the allowlist has one service,
  `com.apple.MTLCompilerService` for Metal GPU tasks; `cargo`, `rustc`, `git`, `cc` and `ld` needed none. `launchctl submit` was already refused
  (`deny(1) job-creation`). Tests for `launchctl submit`, `open -a`, `osascript` and a hard link
  to a secret were added. The profile is still a denylist inside `(allow default)`.

### SEC-13: Collect outputs through the descriptor that was checked

- **Evidence:** Read. The attack needs a task process that outlives the task.
- **Problem:** Output collection checks each entry's type without following links
  (`crates/worker/src/staging.rs:329-352`). The worker then opens the path again with
  `File::open`, which follows a symbolic link (`hash_file` in `crates/core/src/blob.rs:250`,
  called from `BlobCache::insert_file_inner` in `crates/worker/src/blob_cache.rs:377-395`, and
  `copy_verified` at `blob_cache.rs:606`). On macOS, and on Linux without a delegated cgroup, a
  task process that calls `setsid` and drops `RUSTY_GRID_RUN_TOKEN` survives the end of the task
  (`crates/worker/src/process_tree.rs:513-545`, `1210-1231`). Between the check and the open it
  can replace an output with a link to a file that only the worker can read. The worker would then
  upload that file. Under `--isolation off` the task can read such files anyway, so this matters
  under `--isolation fs|fs+net`.
- **Do:** Open each selected output once with `O_NOFOLLOW` (on Windows,
  `FILE_FLAG_OPEN_REPARSE_POINT` and a reparse-point check), check type and size on that
  descriptor, and hash and copy from it.
- **Done when:** A test replaces an output with a symbolic link between the check and the upload,
  and the task fails or the link is skipped. The target's content never reaches the master.

### SEC-14: Take the `agy` chat route out of the master dashboard

- **Evidence:** Read.
- **Problem:** `POST /api/chat` (`crates/master/src/web_ui.rs:333`) starts an `agy -p <prompt>`
  process on the master host (`run_agy` at `web_ui.rs:1255`, binary search at `web_ui.rs:1440-1476`).
  The route needs Submitter scope only (`crates/master/src/auth.rs:389-390`). The prompt is one
  argument, a timeout applies and the process group is killed, but what `agy` may do depends on
  that tool's own configuration. This is developer tooling inside the production master.
- **Do (default of D9):** Remove the route and its dashboard panel from the master. If the owner
  keeps it, require Admin scope and an opt-in master flag, documented in `README.md` and
  `docs/SECURITY.md`.
- **Done when:** `POST /api/chat` answers 404 on a default master, or 403 to a Submitter token and
  404 without the opt-in flag, with a test for each case.
- **Implemented (2026-11-15, pull request 237, [design 0071](design/0071-remove-dashboard-chat.md)):**
  the route, `run_agy` and the dashboard panel were removed from the master
  (`crates/master/src/web_ui.rs`, `tests/dashboard_security_test.rs`).

### SEC-15: Make the Agent Mesh launchers safe by default

- **Evidence:** Read.
- **Problem:** A mesh node runs `shell_exec` unless it is started with `--no-shell`
  (`crates/agent_mesh/src/client.rs:140`), so one hub token gives a shell on every node.
  `packaging/agent_mesh/windows/run_agent_node.ps1` has no way to pass `--no-shell`, while its
  `.cmd` sibling (`run_agent_node.cmd:24-27`) and the macOS launcher
  (`packaging/agent_mesh/macos/run_agent_node.sh:15-21`) do. The Android launcher puts the token
  on the `adb shell` command line (`packaging/agent_mesh/android/run_agent_node_adb.sh:56`). The
  hub serves `ws://` only, so a token sent to a hub that is not on loopback travels in clear.
- **Do:**
  1. Add `-NoShell` and `OXIDE_MESH_NO_SHELL` to `run_agent_node.ps1`.
  2. Pass the token to the phone through a file with mode 600 or through standard input.
  3. Make a node print a warning when it sends a token over `ws://` to a host that is not on
     loopback.
- **Done when:** `tests/test_launcher_scripts.py` checks `-NoShell` in the `.ps1` launcher. The
  adb launcher's command line holds no token. A test shows the `ws://` warning.
- **Implemented (2026-11-16, pull request 258, [design 0079](design/0079-agent-mesh-launchers-safe-by-default.md)):**
  `run_agent_node.ps1` has `-NoShell` and honours `OXIDE_MESH_NO_SHELL=1`, `run_agent_node_adb.sh`
  sends the token on standard input, and the Rust node and `scripts/agent_node.py` log a warning
  for a token over `ws://` to a non-loopback host. Tests are in `tests/test_launcher_scripts.py`
  and `client::tests::test_cleartext_token_warning_predicate`.

### AND-5: Stop the Android worker without blocking the calling thread, and build the bridge in CI

- **Evidence:** Read.
- **Problem:** `stop_worker_impl` joins the worker thread on the calling thread
  (`crates/android_bridge/src/lib.rs:353-355`). The worker can drain for up to 10 s
  (`TASK_DRAIN_TIMEOUT`, `crates/worker/src/client.rs:143`). The Kotlin app calls this from its
  activity and service (`MainActivity.kt`, `OxideWorkerService.kt`), so the user interface can
  freeze long enough for an "application not responding" dialog. `running` is read at `lib.rs:187`
  and set at `lib.rs:243`, so two quick starts can both pass the check. No CI job compiles the
  bridge for an Android target.
- **Do:**
  1. Make the stop call return at once, and join the worker thread on a thread of its own.
  2. Claim `running` with `compare_exchange` at the start.
  3. Add a CI step that runs `cargo check -p rusty_grid_android_bridge --target aarch64-linux-android`.
- **Done when:** A host-side test shows the stop call returns in under 100 ms while a task drains.
  Two concurrent start calls start one worker. The CI step runs on every pull request.
- **Implemented (2026-11-16, pull request 240, [design 0073](design/0073-android-bridge-nonblocking-stop.md)):**
  `stop_worker_impl` returns at once and joins the worker thread on a thread of its own, and a start
  claims `running` with `compare_exchange` (`crates/android_bridge/src/lib.rs`). The host tests
  `test_stop_worker_returns_in_under_100ms_while_worker_drains` and
  `test_interleaved_stop_start_at_most_one_worker_thread` cover both, and the CI step "android
  bridge compiles for aarch64-linux-android" runs in `.github/workflows/ci.yml`.

### DOC-9: Correct the statuses and statements that the audit found wrong

- **Evidence:** Read. Each line below was checked against the code at `c2462e3`.
- **Problem:** These statements disagree with the code or with git:
  1. The Index row of FEAT-6 is titled "Master failover" and set to Implemented. What was built is
     the refusal of `ClaimMasterRole` (design 0026). The item's own "Done when" says no document
     may say failover is finished. Master high availability is SCALE-4.
  2. The SCALE-2 note quotes the 1000-worker result without its baseline. At 500 workers, the
     target of Q8, the burst rate rose from 375.2 to 409.1 tasks/s. At 1000 workers, the stretch
     run of Q8, it fell from 419.7 to 414.4 tasks/s, and the burst assign p50 rose from 17.4 to
     18.9 ms (`benchmarks/grid/results/2026-11-14-scale1-windows-amd64-1000w.json`,
     `2026-11-15-scale2-windows-amd64-1000w.json`). The status Implemented stands on the
     500-worker target.
  3. Six Index rows mark merged pull requests "(open)": TEST-21 (198), TEST-22 (200), DOC-8 (196),
     LOG-1 (194), FEAT-3d (183) and FEAT-4 (186).
  4. `docs/ROADMAP.md`, "Where the project stands", describes `3ce2a65` of 2026-11-09. Its table
     "What is missing" still lists FEAT-3d and design 0039 W11, which both shipped in 0.2.0.
  5. `CLAUDE.md` lists six MCP tools (`mcp.rs` serves seven, with `workspaces`). It names doctor
     checks C1-C6 (`crates/cli/src/doctor.rs` runs C1-C8). It says the project is "verified across"
     Android ARM64 devices, while `README.md` says the worker has not run on a device.
  6. `PROJECT.md` cites `check_mobile_master_eligibility`, which FEAT-11 removed, and marks
     discovery Partial after FEAT-7 shipped.
  7. The help text of `worker --isolation` says "macOS only for now"
     (`crates/cli/src/cli_app.rs:453`). Linux and Windows are implemented.
  8. `README.md` says revoking a member "does not yet cut active lanes". The revoke handler does
     disconnect the member's worker sessions and requeues their tasks
     (`crates/master/src/server.rs:4654-4676`). Client connections stay open until ENROLL-1c.
  9. `README.md` does not say that the network namespace of Linux `fs+net` is best effort. Where
     user namespaces are forbidden, seccomp and Landlock remain
     (`crates/worker/src/isolation/linux.rs:26-28`).
  10. `docs/README.md` shows design 0044 as Planned (the record says Implemented) and design 0039
      as Accepted. It has no entry for designs 0001 to 0022 and 0025. Records 0031 and 0033 still
      say Accepted, though both are built. Records use both the template vocabulary (Draft,
      Accepted) and the status vocabulary (Implemented, Partial, Planned, Design only).
  11. The table "Is the project finished?" at the top of this file shows the snapshot of
      2026-11-01 (AEAD "on a branch", restart "Design only") without a date on the table itself.
- **Do:** Correct each statement in its own document. Rename the FEAT-6 row to "Refuse the
  master-role claim" and keep it Implemented, or set it to Design only; name SCALE-4 for real
  failover. Add the 1000-worker baseline to the SCALE-2 note, so the stretch run reads as no
  gain. Date the snapshot table. Pick one status vocabulary for design records and state it in
  `docs/README.md`.
- **Done when:** Each of the eleven points is corrected or marked as decided by the owner, and a
  search for "macOS only for now", "C1-C6" and "(open)" next to a merged pull request finds
  nothing.
- **Implemented (2026-11-15, pull request 234):** each point, in order:
  1. FEAT-6 row renamed, with a note.
  2. A 1000-worker note was added to SCALE-2.
  3. The six "(open)" markers were removed.
  4. Two rows left the table "What is missing" in `docs/ROADMAP.md`, its summary no longer
     counts Android among the running workers, and a dated note points to DOC-10.
  5. `CLAUDE.md` names seven MCP tools and checks C1 to C8, and states the Android limit.
  6. `PROJECT.md` drops `check_mobile_master_eligibility`, sets discovery to Implemented with
     design 0066, and names checks C1 to C8.
  7. The `--isolation` help text and the MCP `doctor` description were corrected.
  8. The revocation row of `README.md` now says what `members revoke` does.
  9. The Linux row of the platform table states that the network namespace is best effort.
  10. In `docs/README.md`:
      - rows for designs 0001 to 0022 and 0025 were added;
      - 0031, 0033, 0039 and 0044 now show Implemented;
      - the unused numbers are named;
      - rule 7 states the design status vocabulary.
      The records 0031 and 0033 say Implemented.
  11. The snapshot section is dated.

  The remaining "C1 to C6" and "(open)" strings are in the dated snapshot section and in this
  item's own text.

### DOC-10: Keep one roadmap

- **Evidence:** Read.
- **Problem:** Four documents describe direction and status: `docs/ROADMAP.md` (updated
  2026-11-13, before 0.2.0), section 5 of `PROJECT.md`, the preamble of this file, and the untracked
  `docs/ROADMAP_AND_VISION.md`. The last calls itself "Approved for Implementation". Several of its
  statements disagree with the code: the Agent Mesh hub port (it says 8765; the default is
  `127.0.0.1:8088`, `crates/agent_mesh/src/main.rs:35`), the checkpoint type (`CheckpointData` in
  `crates/core/src/task.rs`), the chunk size and hash (4 MiB and BLAKE3,
  `crates/core/src/blob_transfer.rs:29`), the tree manifest version (`tree/2`,
  `crates/core/src/blob.rs:54`) and "no Prometheus `/metrics`" (ECO-4 added it).
- **Do:** Keep `docs/ROADMAP.md` as the only roadmap and bring "Where the project stands" up to
  0.2.0. Replace section 5 of `PROJECT.md` with a link to it. The owner decides whether
  `docs/ROADMAP_AND_VISION.md` is deleted, or corrected and merged into `docs/ROADMAP.md`; it is
  not committed as it stands.
- **Done when:** One roadmap document exists, `PROJECT.md` links to it, and no committed document
  calls a plan "Approved for Implementation" without a decision in `docs/ROADMAP.md`.
- **Implemented (2026-11-15, pull request 235):**
  - "Where the project stands" in `docs/ROADMAP.md` now describes `96ce571` (2026-11-16).
  - Section 5 of `PROJECT.md` is a link to `docs/ROADMAP.md`.
  - The owner left the choice to Claude. Claude moved the untracked
    `docs/ROADMAP_AND_VISION.md` out of the repository into an archive folder of the owner's, with
    a note, instead of deleting it. Its six sound points had already landed through pull request
    206.
  - A search of the tracked files finds no "Approved for Implementation".

## Phase 12: 0.4.0, upgrade without stopping the cluster, and stability

The goal of 0.4.0 is that a cluster can be upgraded without stopping all of it, and that it runs
stably. The first half is [design 0076](design/0076-protocol-compatibility-and-release-train.md);
the second half is the test and CI fixes below and the endurance and fuzz items that already
exist. LLM serving across machines ([design 0067](design/0067-multi-node-llm-serving-and-admission.md)) is 0.5.0.

Order:

1. PROTO-1 and the test fixes TEST-28 to TEST-32, first.
2. PROTO-2.
3. PROTO-3: a design record first, then code.
4. PROTO-4, after PROTO-3 and an update of [design 0040](design/0040-protocol-versioning.md).
5. TEST-24, TEST-25, TEST-26 and LV-1 alongside. They are already in the Index (TEST-23 and
   TEST-27 too) and are not repeated here.

REL-6 (release `v0.3.0` at protocol 9, pull request 260) is the step before this phase.

The Windows smoke test of 0.3.0 passed 9 of 9 steps at `3ecc926`, and the macOS one passed 10 of
10 at `9acc06e`. SEC-16, TEST-32, BUG-22 and CLI-2 come from the Windows report.

### PROTO-1: The release-train rule for wire changes

- **Evidence:** Read. Design 0076 D1.
- **Problem:** A pull request that bumps `PROTOCOL_VERSION_CURRENT` makes `master` speak a version
  that no release speaks, and nothing says when a release must follow.
- **Do:** Add `.github/pull_request_template.md` with a "Wire change" checkbox, one line in
  `CLAUDE.md` where the protocol-version rules are, and one line in `docs/README.md` where the
  design-record rules are.
- **Done when:** The three files carry the rule.
- **Implemented (2026-11-16, this pull request):** the template, the `CLAUDE.md` line and the
  `docs/README.md` line exist. Q64 takes its recommended default: a release within 48 hours, and
  never across two wire changes.

### PROTO-2: A CI job that runs the newest release against the pull request's build

- **Evidence:** Read. Design 0076 D2.
- **Problem:** Nothing in CI shows whether the build of a pull request still works with the
  newest release.
- **Do:** Add a job to `.github/workflows/ci.yml` that runs the newest release against the
  pull request's build in three pairs (as design 0076 D2 lists them) and writes a table of the
  results to the job summary.
- **Done when:** Each pull request shows the release-to-build table.
- **Implemented (2026-11-17, pull request 269):** `scripts/release_skew.py` (Python standard library, unit tests in
  `tests/test_release_skew.py`) runs the three pairs on loopback with private HOMEs and prints the table; the CI job
  `release skew (ubuntu-latest)` builds the pull request's `oxideswarm`, downloads the newest release archive with
  `gh release download`, checks `SHA256SUMS` and runs the script. It is not a required check, and it passes with a
  notice when no release exists.

### PROTO-3: An extension slot for optional wire fields

- **Evidence:** Read. Design 0076 D3.
- **Problem:** Every added field moves `PROTOCOL_VERSION_MIN_COMPATIBLE`, because bincode is
  positional.
- **Do:** Write a design record first (done: [design 0082](design/0082-task-extension-slot.md), 2026-11-17). Then build the slot at the next wire change (Q65), in
  `crates/core/src/task.rs` and `crates/core/src/protocol.rs`.
- **Done when:** An optional field can be added with no change to
  `PROTOCOL_VERSION_MIN_COMPATIBLE`.
- **Status:** Implemented by pull request 272 (draft: it merges together with PROTO-4, one-version
  window, by the release-train rule of design 0076 D1). Protocol 10, store format 4,
  `Extension` and the empty registry in `crates/core/src/task.rs`, the `ext:` worker tag and the
  scheduler check, and the frozen v9 `SubmitTask` decoder. Q68 to Q72 decided by the owner on
  2026-11-17 (the recommended defaults).

### PROTO-4: A one-version compatibility window

- **Evidence:** Read. Design 0076 D4.
- **Problem:** Today `PROTOCOL_VERSION_MIN_COMPATIBLE` equals `PROTOCOL_VERSION_CURRENT`, so a
  master upgrade stops every older worker.
- **Do:** After PROTO-3 (Q66), make the master accept the previous version, update design 0040
  and `crates/core/src/wire_layout_tests.rs`.
- **Done when:** A worker one version back registers and runs a task.
- **Design:** [design 0083](design/0083-one-version-window.md) (2026-11-17, based on pull request 272): Q73 to Q79 decided by the owner in chat on 2026-11-17, all on the recommended defaults.
- **Status:** Implemented by draft pull request 274 (it merges together with PROTO-3, pull request 272, and a release follows within 48 hours, design 0076 D1). `PROTOCOL_VERSION_MIN_COMPATIBLE` is 9 at protocol 10; the master reads each peer's version, encodes for it (`serialize_message_for_peer`, `Task::for_peer`), serves protocol 9 workers and clients, and tags them `proto:9`; the layout test is a range check with `wire_layout_previous.txt` (v0.3.0) as the oracle; the skew job derives its expected table. PROTO-3d (the cross-version tests of design 0082) is built with it.

### TEST-28: Make the group-commit timing test meaningful on slow runners

- **Evidence:** Reproduced once. `test_concurrent_submissions_share_group_commit_and_recover_across_restart`
  (`crates/master/src/server/persistence_tests.rs:906`) asserts that 1,000 durable submissions
  finish in under 2 s (line 935). On `windows-latest` (pull request 260, job `133322408308`,
  2026-11-16) they took 2.0422361 s.
- **Problem:** A wall-clock bound of 2 s says more about the runner than about group commit.
- **Do:** Measure group commit by the count of fsyncs or commits, or use a much wider bound. Keep
  what the test proves: submissions share commits, and the state survives a restart.
- **Done when:** The test passes on a slow runner and fails if every submission commits alone.
- **Implemented (2026-11-16, pull request 265):** the 2 s wall-clock bound is gone; a 60 s guard
  catches a hang, and the existing `store_sync_count()` assertion (more than 0 and fewer than 200
  syncs for 1,000 submissions) fails if every submission commits alone. 20 of 20 local runs
  passed, one of them with 12 CPU-burning processes.

### TEST-29: Find why `workspace tests (windows-latest)` loses its runner at 60 minutes

- **Evidence:** Reproduced twice on 2026-11-16: pull request 251 at `72b05c5` (job
  `133129128530`) and pull request 256 at `ede7d01` (job `133256035744`). GitHub annotated both
  "The hosted runner lost communication with the server". Reruns of the same heads passed in
  about 30 minutes.
- **Problem:** Some test hangs or starves the runner until the job limit. Nothing in the log says
  which.
- **Do:** Find the test: per-test timeouts, a step timeout below the job limit that prints the
  running tests, or running the suite under the Windows agent. Then fix it.
- **Done when:** The job finishes in its usual time on ten runs in a row, or the test that hung is
  fixed and named here.
- **Partial (2026-11-17, pull request 283):** investigated on a Windows 11 machine with toolchain
  1.96.0 (`OxideSwarm_Agent_Hub` `test_runs/win_test29_2026-11-17.md`). In both lost jobs steps 1 to 7
  had succeeded and step 8 (`cargo test --workspace`) was still running. Three CI-like runs found no
  hang and no memory or handle exhaustion, only three timing flakes under CPU starvation (TEST-36 to
  TEST-38). The cause of the runner loss is not known. The workspace step now stops at
  `timeout-minutes: 45`, prints a heartbeat line with the last output line once a minute
  (`scripts/ci_heartbeat.py`), and an `if: always()` step prints the process tree, memory and the
  last 120 lines of the test output, so the next occurrence names the running test.
- **Implemented (2026-11-19, pull request 358):** The job finished in its usual time on far more than ten runs
  in a row. Read through the GitHub API for the CI runs created on 2026-11-18 (UTC): in their latest attempt, 82
  `workspace tests (windows-latest)` jobs passed (median 30.3 min, longest 43.6 min), one failed to compile (a
  Dependabot bump of `sysinfo`), 31 were cancelled by a newer push and five were still queued. No attempt lost its
  runner; the one failed earlier attempt (run `44308601329`, attempt 1) failed the stress test
  `test_capability_queries_under_connect_disconnect_stress` after 33 min. A count of 35 of these jobs on the
  Windows machine gave the same picture (34 passed, median 30.2 min, longest 43.1 min). The cause of the two
  losses of 2026-11-16 stays unknown, and the diagnostics of pull request 283 stay. Making the job faster is
  TEST-56.

### TEST-30: Stop `test_agent_bridge.py` from depending on the host's open ports

- **Evidence:** Reproduced on a machine where a real master listens on port 8088.
- **Problem:** `Diagnose.test_two_simulated_agents_on_one_platform_do_not_collide`
  (`tests/test_agent_bridge.py:745`) fails there, because the test probes the real port.
- **Do:** Make the test use a port it binds itself, or stub the probe.
- **Done when:** The test passes with a listener on 8088.
- **Implemented (2026-11-16, pull request 263):** `agent_bridge.py` reads the environment variable
  `OXIDESWARM_BRIDGE_ASSUME_LISTENING` (ports reported as listening; unset means probe as before),
  the tests set it to empty, and a new test sets it to 8088 and still sees `[SPLIT-BRAIN MASTER]`.
  The whole file passed (45, then 53 tests with pull request 261) with a real master on 8088.

### TEST-31: Remove the race in the detached-grandchild Blender probe test

- **Evidence:** Failed once on the ubuntu unit tests (pull request 238, before 2026-11-16).
- **Problem:** The test in `crates/core/src/capabilities.rs` (script near line 3737) kills the
  process group before the grandchild has called `setsid`, so the grandchild may still be in the
  group.
- **Do:** Have the script write a second file after `setsid`, and wait for it before the kill.
- **Done when:** The test passes 200 times in a loop on Linux.
- **Partial (2026-11-16, pull request 265):** the grandchild writes a ready marker after `setsid`
  and the child waits for it (at most 5 s) before it exits; the Unix timeouts were widened
  (2.5 s, 4 s, 10 s) because under load the script had not written its PID file within 800 ms.
  50 local runs on macOS passed (30 with 12 CPU-burning processes). The 200-run loop on Linux of
  "Done when" is not done.
- **Implemented (2026-11-19, pull request 363, open):** Passed 200/200 runs (0 failures; 1,343.1 s elapsed across the two batches; plus a 1/1 smoke run beforehand in 6.65 s) on Linux (`aarch64`, 10 logical cores, `Linux 7.0.14-linuxkit`, `Debian GNU/Linux 12 (bookworm)` in the Docker Desktop container `rust:1.96-bookworm`, `rustc 1.96.1 (31fca3adb 2026-06-26)`, `cargo 1.96.1 (356927216 2026-06-26)`, `Python 3.11.2`, `--locked`, fresh temporary `HOME` per run) at commit `ccafa90` (`ccafa9023`): 100 alone runs (100/100 passed, 668.84 s) and 100 runs under 6 Python busy loops on 10 logical cores (about 60 %, 100/100 passed, 674.26 s). Together with the macOS runs (pull request 265), "Done when" passes on both Unix hosts.

### TEST-32: Make the cancel-during-upload worker test independent of `NoDefaultCurrentDirectoryInExePath`

- **Evidence:** Reproduced 5 of 5 times on Windows (smoke test, finding F2).
  `client::tests::test_cancel_during_output_upload_reports_result_and_decrements_master_active_tasks`
  (`crates/worker/src/client/tests.rs`) panicked with "task 2 did not write exit marker in time".
  With `Remove-Item Env:NoDefaultCurrentDirectoryInExePath` the same commit passes (248 passed).
  Claude Code sessions set that variable.
- **Problem:** The test starts `cmd.exe /C drain.cmd` with a bare name. With the variable set,
  `cmd.exe` does not search the current directory, so the script never runs. The product is not at
  fault: under `--isolation off` a task inherits the worker's environment, as designed.
- **Do:** Start `.\drain.cmd`, or clear the variable for the child.
- **Done when:** The test passes with the variable set to 1.
- **Implemented (pull request 267, row corrected on 2026-11-18):** the test starts `.\drain.cmd`
  (`crates/worker/src/client/tests.rs`). agent-win ran it 3 of 3 times with
  `NoDefaultCurrentDirectoryInExePath=1` on `master` `fb38aee`.

### TEST-33: Make the worker-churn stress test deterministic on Windows

- **Evidence:** Reproduced once on 2026-11-16: pull request 263 at `282de6f` (run `43879650203`, job
  `133341802791`, `workspace tests (windows-latest)`): `test_stress_high_volume_worker_churn`
  panicked at `tests/m9_stress_challenge_1.rs:763` with "All 30 tasks must complete despite rapid
  worker churn". The rerun passed. The pull request did not touch the master or the worker.
- **Problem:** The test waits a fixed time for 30 tasks while workers connect and drop; on a slow
  runner the requeue and backoff can take longer than the wait.
- **Do:** Find the wait or backoff the test depends on, and wait for the condition (all tasks
  terminal) with a bound that does not depend on runner speed, or count requeues.
- **Done when:** The test passes 50 times in a loop on Windows, one run with CPU load.
- **Partial (2026-11-17, pull request 286):** not reproduced on Windows 11 in 125 runs (exact and whole binary, no load,
  2 cores with 4 CPU spinners, 1 core with 6 spinners); the drain after the churn took at most 2.4 s against the
  20 s bound, and the CI log of the failing job stops before this binary. An aborted worker cannot linger: `Drop for
  Session` (`crates/worker/src/client/session.rs`) stops its lane tasks, so the socket closes. The drain wait is now
  60 s and the registration wait 30 s (both end as soon as the condition holds), and a timeout lists every task that
  is not `Completed` with its state, counts, worker and error. The changed test passed 50 runs on 1 core with 6
  spinners and 50 without load. On 1 core two other tests of the file failed: `test_stress_reaper_detection_and_failover`
  (2 of 20, line 1151) and `test_stress_immediate_failover_latency_under_load` (1 of 20, `129.9476ms` against `<100ms`).
- **Implemented (2026-11-17, pull request 289):** cause found in CI (pull request 288, run `44044328538`, job
  `133906405213`, windows-latest, with the diagnostics of pull request 286): one of the 30 tasks was `Failed` with
  `retries=1 failovers=1 evacuations=1` and `error: Sandbox base: The sandbox base directory '...\.tmpZk4loM' is
  not writable: The system cannot find the file specified. (os error 2)`. The churn loop dropped each retired
  `ManagedWorker` right after `graceful_disconnect()` or `abort()`, and a `ManagedWorker` owns the `TempDir` that is
  the worker's `--sandbox-base-dir`; the directory was deleted while the worker could still be given a task, the
  worker refused it (`client/tasks.rs`, `ensure_private_root_recheck`), and the master, which retries only the
  refusals it recognises (see BUG-25), failed the task for good although `with_max_retries(10)`. The test now
  keeps every retired worker in a `retired` list until the end, so no sandbox base disappears under a worker
  that is still alive; every assertion is unchanged. The other tests of the file keep their workers in a vector
  until the end and need no change. Loop: 50 runs of the exact test on macOS, 25 without load and 25 with four `yes` CPU burners, all passed, then the whole binary once (6 passed). The Windows 50-run loop of "Done when" is not run (the failure was seen only on windows-latest). This was a defect of the test; the product side (a worker
  with an unusable base fails the task terminally instead of handing it back) is BUG-25.

### TEST-34: Show and fix the Agent Mesh simulation Suite 2 failure on macOS

- **Evidence:** Reproduced once on 2026-11-16: pull request 262 at `eb68ed9` (job `133354770430`,
  `agent bridge (macos-latest)`): `tests/run_mesh_simulation_tests.py` printed "[FAILED] Suite 2 raised
  exception:" with no exception text, then "Suite 2: Bidirectional Zero-Data-Loss & Burst ... FAILED".
  The rerun passed. The pull request changed documents only.
- **Problem:** The runner prints no exception text, so the cause is unknown.
- **Do:** Print the exception type, message and traceback for a failing suite; then find the cause.
- **Done when:** The cause is written here and fixed, or the suite passes 50 runs on `macos-latest`.
- **Implemented (2026-11-17, pull request 277):** the runner now prints the type, `repr` and
  traceback of a failing suite on stderr. With that, the failure reproduced locally on macOS (6 of
  about 20 plain runs, while other jobs loaded the machine): `AssertionError` with no message at
  `tests/test_agent_mesh_data_loss.py` Phase 4, `assert len(bytes_128k) == payload_size_128k`. The cause
  is in the test, not in the Python reference hub: after sending a payload the test slept a fixed 0.1 s
  and then read `received_data_payloads[-1]`; when the hub had not yet delivered the 128 KB frame, that
  was the earlier 64 KB payload. The fixed sleeps of Phases 1 to 5 are now a bounded wait
  (`wait_for_payload_count`, 10 s) for the expected count, and every count and every payload is still
  asserted exactly (Phase 5 keeps a 0.1 s settle so a duplicate frame is still counted). After the fix:
  50 runs of `tests/run_mesh_simulation_tests.py` and 50 with 12 `yes` CPU burners, all passed. The
  `macos-latest` 50-run loop of "Done when" is not run.

### TEST-35: Make the `switch_role` launcher test stable on macOS

- **Evidence:** Reproduced twice: pull request 206 on 2026-11-12, and pull request 260 at `9d8b9cc` on
  2026-11-16 (job `133322408476`, `launcher scripts (macos-latest)`):
  `ShellLauncherTests.test_switch_role_master_flow_and_tracked_stop` failed with "AssertionError: 1 != 0"
  on the launcher's return code. Reruns passed.
- **Problem:** The launcher returned 1 once in a while; the test does not print why.
- **Do:** Print the launcher's output on failure, then find the cause.
- **Done when:** The cause is written here and fixed, or the test passes 50 runs on `macos-latest`.
- **Partial (2026-11-17, pull request 277):** diagnostics added, cause not found. A failing launcher
  run now fails with its command, return code, combined output, `master.log` / `worker.log` and the
  fake binary's recorded calls. The failure did not reproduce locally: 30 runs plain and 30 with 12
  `yes` CPU burners, each with the default `PATH` and with `PATH="/bin:$PATH"` (120 runs, 0 failures).
  The launcher returns 1 only when the master port is in use, the master process is not recognised
  right after it starts, or its log shows no "RustyGrid Master started" within 10 s; one suspected
  race (a process still showing the launcher's command line just after the fork) was not seen in 300
  tries. The launcher is unchanged. The next failure on `macos-latest` will print which of these it was.
- **Implemented (2026-11-18, pull request 338):** the cause is the race that pull request 277 suspected.
  The diagnostics caught it on `macos-latest` on 2026-11-18 (pull request 337, `launcher scripts
  (macos-latest)`): `switch_role.sh master` printed "[ERROR] Master failed to start" with an empty log
  tail, then "[INFO] Stopping master (PID 1431)...", and `master.log` held "RustyGrid Master started on
  fake". `start_tracked` writes the PID right after the fork. Until the child has exec'd the binary, `ps -p
  PID -o command=` shows the launcher's own command line, so `pid_is_ours` was false and the start loop
  gave up on its first pass, before the binary had written anything; a moment later `stop_tracked` found
  the binary under the same PID. The master start loop now ends early only when the process is gone
  (`kill -0`) and confirms the PID with `pid_is_ours` once the log line is there, so a reused PID still
  fails the start. The worker checks after `sleep 1` (the master's local worker) and `sleep 2` (the
  worker role) use the new `wait_until_ours`, which waits within the same 10 s budget (50 checks 0.2 s
  apart) and fails at once when the process is gone. `pid_is_ours`, used by `stop` and `status`, is
  unchanged. Three tests in `tests/test_launcher_scripts.py` put a `ps` on the launcher's `PATH` that shows
  the launcher's command line until the fake binary has run for 3 s: the master and worker tests failed on
  the old launcher with the CI symptom and pass now; the third checks that a PID whose command line names
  another program fails the start and is not signalled. With 8 `yes` CPU burners and `PATH="/bin:$PATH"`
  (bash 3.2.57), `test_switch_role_master_flow_and_tracked_stop` passed 30 of 30 runs with the fix; the
  old launcher passed 30 of 30 there too, as in the tries of pull request 277, so the window is shown by
  the `ps` shim, not by load. The `macos-latest` 50-run loop of "Done when" is not run. No other shell
  launcher checks a command line right after starting a process (`packaging/usb/install.sh` checks only
  `kill -0` after `sleep 4`).

### TEST-36: Remove the 50 ms bound from the symlink-chain manifest test

- **Evidence:** Reproduced on the owner's Windows machine under CPU starvation (4 cores, then 2 cores
  plus 4 CPU spinners; hub report `test_runs/win_test29_2026-11-17.md`):
  `manifest_v2_symlinks_and_empty_dirs_round_trip_and_refuse_escapes` in `crates/core/src/blob.rs`
  asserted that refusing the chained-link manifest takes under 50 ms and measured 52.19 ms and 78.52 ms.
- **Problem:** The test wants to prove that validation is bounded (40 chained links and 1,000 compound
  links must not expand exponentially), but a 50 ms limit also measures the runner's load.
- **Do:** Keep the refusal assertion and bound the time at 10 s: the unbounded expansion never finishes,
  so the bound still tells the two apart.
- **Done when:** Implemented by this PR. The test passed 50 of 50 runs next to 12 `yes` processes on a
  Mac, and 50 of 50 next to 48 (the old code also passed there: this Mac did not reproduce the failure).

### TEST-37: Remove the 100 ms window from the port-file reader test

- **Evidence:** Reproduced on the same Windows machine and report as TEST-36:
  `test_atomic_port_file_publication_concurrent_client_reads` in `tests/m2_challenger_2_stress.rs` let
  ten readers poll the port file for a fixed 100 ms and required at least one successful read.
- **Problem:** Readers add their reads to the total only when they stop, and under starvation they may not
  be scheduled inside 100 ms, so the total can be 0 although no read was ever corrupt.
- **Do:** Count each successful read as it happens and wait (60 s deadline against a hang) until 50 reads
  have been made before stopping the readers. Every read is still checked to be non-empty and a valid
  port.
- **Done when:** Implemented by this PR. 50 of 50 runs next to 12 `yes` processes and 50 of 50 next to 48.

### TEST-38: Remove the 800 ms window from the re-dispatched task id test

- **Evidence:** Reproduced on the same Windows machine and report as TEST-36:
  `test_reassigned_task_id_stops_the_old_run_silently_and_the_new_run_owns_the_slot` in
  `crates/worker/src/client/tests.rs` collected results for 800 ms after the re-dispatch and found none
  when `echo second` had not run yet.
- **Problem:** The wait for the new run's result was a fixed window, and the final 100 ms sleep decided
  when the slots were free.
- **Do:** Wait up to 60 s for the first result, then until the active table is empty, then drain the
  channel and still require exactly one result; poll for the two slots instead of sleeping.
- **Done when:** Implemented by this PR. 50 of 50 runs next to 12 `yes` processes and 50 of 50 next to 48.

### TEST-39: Wait for the `--no-shell` mesh node to register before calling it

- **Evidence:** `test_sec6_real_cli_no_shell_flag_execution` (`crates/agent_mesh/tests/test_sec6_mesh_sandbox.rs`)
  failed on `macos-latest` in pull request 274 with `ERR_NODE_NOT_FOUND` and passed on a rerun.
- **Problem:** The test spawned `agent-mesh node --no-shell` and slept 600 ms before sending `shell_exec`; a
  slow start left the node unregistered. Locally the old test failed 30 of 30 runs next to 24 `yes`
  processes, and once with no load on a cold binary.
- **Do:** Poll `AgentMeshClient::query_node_list` until the node is listed (30 s bound), then send.
- **Done when:** Implemented by pull request 278: 30 of 30 runs next to 24 `yes` processes.

### TEST-40: Wait for each payload in the Agent Mesh large-payload test

- **Evidence:** `test_adv_large_payload_boundaries` (`crates/agent_mesh/tests/adversarial_mesh_challenge.rs:520`)
  failed on `macos-latest` in pull request 275 (run `43957863018`, job `133617088614`): "Chunk size
  mismatch for size 131072", left `65536`. It passed on a rerun.
- **Problem:** Same cause as TEST-34: the test slept 150 ms after each send and checked the last payload
  received, which was still the 64 KiB one when the 128 KiB frame had not arrived.
- **Do:** Wait (20 ms polls, 30 s bound) until the receiver holds as many payloads as were sent, then run
  the same assertions. Pull request 278 also replaced the fixed sleeps before the burst counts of
  `adversarial_mesh_challenge.rs`, `adversarial_challenge.rs` and `mesh_integration.rs` with the same wait.
- **Done when:** Implemented by pull request 278. The old test did not fail locally (30 of 30 under load),
  so the fix rests on the cause seen in CI.

### TEST-42: Make the reaper boundary test independent of scheduler delay

- **Evidence:** `test_reaper_accuracy_silent_worker_boundary_timing` (`tests/m2_challenger_2_stress.rs:96`)
  failed on `windows-latest` on 2026-11-16 (run `43944560507`, job `133570626681`):
  `Reaper must NOT reap worker before timeout (at 100ms < 200ms)`, `left: Disconnected`, `right: Connected`.
  The same job failed the port-file reader test that TEST-37 fixed. ROADMAP.md "Stability" lists it.
- **Problem:** The test checks the worker 100 ms into a 200 ms timeout. A likely cause, not verified: on a
  starved runner more than 200 ms passed before the check, so the reaper acted as designed.
- **Do:** Measure the time that really passed before the check (or drive the reaper with a paused clock) and
  assert on that, as TEST-36 to TEST-38 did. If the reaper fired early, fix the reaper instead.
- **Done when:** The cause is named here, and the test passes 200 runs in a row under CPU starvation on Windows.
- **Cause:** Confirmed on the owner's Windows desktop (1 core, 6 CPU spinners): the check meant for 100 ms
  ran after the 200 ms timeout had really passed, and the reaper acted as designed. The old test also timed
  from after the ack, later than the moment the master stamps the worker's last heartbeat, and bounded the
  reap at 450 ms. The old test failed 22 of 100 runs there (all on that 450 ms bound; 3 of 100 on 2 cores with
  4 spinners). The new version printed the boundary check running after the timeout in 71 of 200 runs, once
  with the worker already Disconnected at `378.9 ms`, which is the failure of run `43944560507`; it saw the
  reap at up to `1021 ms`.
- **Status:** Implemented by pull request 302. Times are measured from just before the Register is sent, so
  a correct reaper can never be seen Disconnected with less than the timeout elapsed: the boundary check
  asserts "Connected, or the check came after the timeout" and the reap must be seen at or after the
  timeout. The reap must be seen within 5 s (the configured 200 ms, not the 10 s default, and not never);
  the time is printed. 200 of 200 runs passed in a row under 1 core and 6 spinners. Control: with the
  reaper at a quarter of the timeout the test fails (3 of 3, Disconnected at about 110 ms).

### TEST-43: Show and fix the late dashboard port file of the in-process dev cluster test

- **Evidence:** `test_dev_cluster_in_process_launch_then_stop_and_signal_failure_handling`
  (`tests/workflow_modes_test.rs:1017`) failed on `ubuntu-latest` on 2026-11-16 (run `43952618376`, job
  `133598809232`): `Dashboard port file (<tmp>/.dev_cluster.dashboard_port) did not appear within 8s for
  127.0.0.1:<port>`. ROADMAP.md "Stability" lists it.
- **Problem:** `ox-mode dev` waits a fixed 8 s for the dashboard port file. The log does not say whether the
  in-process dashboard started late or did not start.
- **Do:** Print the in-process master's output when the wait fails, find which of the two happened, then fix
  the start-up or wait on the condition with a bounded but generous limit.
- **Done when:** The cause is named here, and the test passes 200 runs in a row on Linux.
- **Cause:** The dashboard did not start late and was not missing: the test gave the master one port for both
  listeners. It picked the grid port and then the dashboard port by binding `127.0.0.1:0` twice, closing each
  listener at once, so the second bind could be handed the port the first had just released. The master took
  that port for its grid listener, logged `Web UI port binding failed; continuing master RPC without Web UI
  ... (os error 10048) addr=127.0.0.1:51840`, and never wrote the port file. Seen on the owner's Windows
  desktop in 1 of 30 runs of the whole test binary (4 cores, 4 spinning processes), with the master's log that
  this item added. `test_dev_cluster_never_sends_token_when_web_ui_port_is_held` picked its ports the same way.
- **Status:** Implemented by pull request 307. The two tests keep the first listener open while they pick the
  second port (`two_free_ports` in `tests/workflow_modes_test.rs`). `launch_local_dev_cluster_with_ports`
  refuses the same port for both at once, with a message, and a new test covers that. The master's stdout and
  stderr go to `.dev_cluster.master.log` in the workspace (covered by `.dev_cluster.*` in `.gitignore`), and a
  failed wait quotes its last 40 lines. The 8 s wait stays. The helpers moved to `crates/cli/src/mode_cli/dev_launch.rs`
  so that `mode_cli.rs` shrank (allowlist 3240 to 3213 lines). On 4 cores next to 4 spinning processes,
  the whole test binary (18 tests in parallel, `--test-threads=4`; the two tests that need `cargo test`'s
  environment skipped) passed 200 of 200 runs in a row. Not run on Linux here.

### TEST-44: Remove the 1.5x wall-clock cap from the speed-aware batch test

- **Evidence:** `sched4_batch_finishes_within_1_5x_fast_worker_and_zero_factor_restores_old_timing`
  (`tests/sched4_speed_aware.rs:194`) failed on `ubuntu-latest` on 2026-11-17, in pull request 282 at
  `3783987` (run `44021280672`, job `133834113011`): `D2 batch elapsed 766.662836ms exceeded 1.5x fast
  worker's own time (464.419518ms, cap=696.629277ms)`. Pull request 282 changes only `windows/oxctl.ps1`, a
  Python launcher test and docs.
- **Problem:** The test compares two wall-clock times of a few hundred milliseconds, so a loaded runner
  can break the 1.5x ratio without any change in placement.
- **Do:** Assert what design 0060 D2 decides (which worker ran each task, and that the slow worker was held
  back), and keep the timing as a printed measurement or as a much wider bound, as TEST-36 to TEST-38 did.
- **Done when:** The test passes 200 runs in a row under CPU starvation on Linux.
- **Status:** Implemented by pull request 304. The test, renamed
  `sched4_d2_keeps_the_batch_on_the_fast_worker_and_zero_factor_restores_old_timing`, asserts placement:
  every task of the measured batch runs on one of the two workers, and a task that ran on the slow worker
  finished no earlier than twice the slow task time after the batch started (the wait cap of D2 step 5, at
  least the slow worker's measured average, plus the task's own run). The slow worker now sleeps 10x instead
  of 5x: D2 holds a task for a full fast worker while the slow average is at least `1.5 * (ready + fast
  average)`, at worst three fast averages, and with 5x a run on 2 cores next to 4 spinning processes let D2
  send a task to the slow worker at once (run 125 of the first loop: a slow task finished `1.31 s` after the
  batch started). With 10x the fast tasks must stretch by 770 ms for that. With `speed_hold_factor = 0` the slow
  worker must still get at least 4 tasks and the batch must last at least one slow task. Both batch times and
  their ratios to the fast worker's own time are printed, not asserted. On the owner's Windows desktop, 2 cores
  next to 4 spinning processes: 200 of 200 runs in a row passed (the D2 ratio reached `2.61x`, above the old
  `1.5x` cap in 5 runs). Control: with D2 turned off for the measured batch the assertion fails (2 of 2).
  Not run on Linux here.

### TEST-45: Keep the second worker of the reaper failover stress test alive

- **Evidence:** Reported by agent-win with pull request 286 on 2026-11-17: on its Windows desktop with one
  core and six spinning processes, `test_stress_reaper_detection_and_failover`
  (`tests/m9_stress_challenge_1.rs:1151`, "Task must complete on Worker 2 after Worker 1 is reaped") failed
  in 2 of 20 runs of the whole test binary. Not seen in CI yet.
- **Problem:** The agent's reading, not proven: Worker 2 is a raw connection that sends no heartbeats while
  the test's reaper timeout is short, so the reaper can take Worker 2 too before its `TaskResult` counts.
- **Do:** Confirm the cause from the master's log in a failing run, then make Worker 2 send heartbeats (or
  give it a timeout it cannot reach) without weakening the assertion about Worker 1.
- **Done when:** The cause is named here, and the test passes 200 runs in a row under the same starvation.
- **Cause:** Confirmed: the reaper took Worker 2. With failure messages that name what the master holds, the
  test alone failed 19 of 100 runs on 1 core next to 6 spinning processes: 18 times Worker 2 got no
  `AssignTask` (`got None, Worker 2 is Some(Disconnected)`), once the master had already requeued the task
  after Worker 2's result (`Worker 2 is Some(Disconnected), task: state Queued, retries 2`). Worker 2 was a raw
  connection that sent no heartbeat, and the reaper timeout was 80 ms, so any 80 ms stall between its
  registration and its result was enough. The whole binary failed this test in 5 of 20 runs there.
- **Status:** Implemented by pull request 308. The reaper timeout is 2 s instead of 80 ms, and Worker 2
  sends a heartbeat every 50 ms from its own task (the transport is split; the reader skips `HeartbeatAck`)
  until the end of the test. Worker 1 is unchanged: it must still be reaped, end `Disconnected` with no
  active task, and the task must complete on Worker 2 with `retry_count` 1. The waits for the reap and for
  the completion are liveness bounds (8 s and 15 s). Failures now print Worker 2's status and the task's
  state. On 1 core next to 6 spinning processes the test passed 200 of 200 runs in a row.

### TEST-46: Remove the 100 ms bound from the failover latency stress test

- **Evidence:** Reported by agent-win with pull request 286 on 2026-11-17: under the same starvation,
  `test_stress_immediate_failover_latency_under_load` (`tests/m9_stress_challenge_1.rs:637`) failed in 1 of
  20 runs: `129.9476ms` measured against a strict `< 100ms`. Not seen in CI yet.
- **Problem:** A wall-clock bound of 100 ms, like the ones TEST-36 to TEST-38 removed.
- **Do:** Assert the order of events (the task was re-queued after the disconnect, without waiting for the
  reaper timeout) and print the latency, or use a bound that a starved runner cannot reach.
- **Done when:** The test passes 200 runs in a row under the same starvation.
- **Cause:** Two wall-clock windows, not one. On 1 core next to 6 spinning processes the whole test binary
  failed this test in 12 of 20 runs: 9 times the evicted task was not yet reassigned when the 200 ms wait
  ended, 3 times the latency was over 100 ms (`116.7 ms`, `152.7 ms`, `166.2 ms`). The same loop failed
  `test_stress_tcp_rst_vs_graceful_disconnect` once on its 500 ms window for the same graceful failover.
- **Status:** Implemented by pull request 309. Both tests assert the order of events: after the graceful
  disconnect the task is reassigned to the survivor within 5 s, which is half of the 10 s reaper timeout of
  these masters (`ReaperConfig::default()`), so the reassignment came from the disconnect and not from the
  reaper; the second test still requires `retry_count` and `failover_count` 0. The latency is printed. The
  wait for the 4 tasks to complete is 30 s (was 8 s), a liveness bound. On 1 core next to 6 spinning
  processes the two tests passed 200 of 200 runs in a row; the printed latency reached `99.4 ms`.

### TEST-47: Count only this test's handles in the capability-detection leak test

- **Evidence:** `test_rapid_repeated_detection_sequential_leak_and_stability`
  (`crates/core/tests/m1_3d_adversarial_stress.rs:696`) failed on `windows-latest` on 2026-11-17, in pull
  request 286 at `c1238c6` (run `44027023301`, job `133849820395`): `File handle leak detected! Handle count
  grew by 8 (initial: 144, final: 152)` after 200 detections in 257 ms. The rerun of the job passed. Pull
  request 286 changes only `tests/m9_stress_challenge_1.rs` and docs.
- **Problem:** The test compares the handle count of the whole test process before and after its loop, while
  other tests of the same binary (for example `test_rapid_repeated_detection_concurrent_stress`) run on other
  threads and open handles of their own. A likely cause, not verified: those tests, not a leak.
- **Do:** Run the test alone in its process (a separate test binary, or a serial guard shared by the tests of
  this file that open processes), or compare several rounds and fail only on growth that keeps rising. If a
  real leak shows up alone, fix it in capability detection.
- **Done when:** The cause is named here, and the test passes 200 runs in a row on Windows next to the other
  tests of its binary.
- **Cause:** Two parts, both measured on the owner's Windows desktop. Alone in its process the count
  grew by exactly 2 in 5 of 5 runs (147 to 149): handles the first detections allocate once. Next to the
  other tests of its binary the starting count moved between 169 and 173 from run to run, so the count
  included other tests' handles. The `+8` of CI was not reproduced exactly here.
- **Status:** Implemented by pull request 310. A `tokio::sync::RwLock` in the test file
  (`handle_count_lock`) is taken exclusively by the two tests that count handles
  (`test_rapid_repeated_detection_sequential_leak_and_stability` and
  `test_rapid_repeated_detection_concurrent_stress`) and shared by every other test of the file that opens
  handles (the ten `detect*` override tests and the two CLI end-to-end tests); the serde tests take no lock.
  Both counting tests run a warm-up detection before the first count. The limits (5 and 15) are unchanged.
  Afterwards the growth was 0 alone (149 to 149) and next to the other tests (154 to 154, 156 to 156), and
  the binary takes about 37 s instead of 24 s. The whole binary (all 21 tests, the default parallel
  harness) passed 200 of 200 runs in a row on Windows, with a handle growth of 0 in every run of both tests.

### TEST-48: Give each Android bridge test its own worker state

- **Evidence:** `test_bridge_set_cluster_secret_hex_and_file_parsing`
  (`crates/android_bridge/tests/cluster_auth_test.rs:40`) failed on `ubuntu-latest` on 2026-11-17, in pull
  request 289 at `16313f5` (run `44050351817`, job `133926449804`): `Status must report error for invalid
  secret: {... "status":"STOPPED" ...}`. The rerun passed. Pull request 289 changes only a test of the master
  crate and docs.
- **Problem:** The bridge keeps its worker in process-global state (`OnceLock<Arc<WorkerState>>`, see
  `ANDROID_ARCHITECTURE.md`), and the tests of this binary run in parallel. A likely cause, not verified:
  another test started or stopped the worker between this test's call and its status read, so the status no
  longer shows the error this test caused.
- **Do:** Serialise the tests of the binary that touch the global worker (one lock shared by them, or one
  test per process), or give the bridge a way to reset its state between tests. Keep every assertion.
- **Done when:** The cause is named here, and the binary passes 200 runs in a row on Linux.
- **Implemented (2026-11-17, pull request 300):** The cause is not another test's call. All tests of the
  file already hold one `TEST_MUTEX`. `stop_worker_impl` returns while the worker still drains: it sets the
  status `STOPPING` and a detached thread writes `STOPPED` after it has joined the worker thread. That late
  write ran after the previous test released the lock, so it could land between this test's failing
  `set_cluster_secret_impl` (which writes `ERROR: Invalid cluster secret: ...`) and its status read. The
  tests now call `common::stop_and_settle()` (`crates/android_bridge/tests/common/mod.rs`), which stops the
  worker and waits (30 s bound) until the status is no longer `STOPPING`; the four test files that read the
  status use it. No change to the bridge, every assertion kept.

### TEST-49: Wait for the master by condition in the sched2 runtime test

- **Evidence:** `cli_app::runtime_tests::test_sched2_max_queue_size_scheduler_policy_wire_codec_timeout_and_p2p_ticket`
  (`crates/cli/src/cli_app/runtime_tests.rs:886`, "round-robin master bound") failed in `gpu-wgpu (ubuntu-latest)`
  on 2026-11-17, in pull request 294 at `c5dfc08` (run `44086935350`, job `134051751538`). The rerun passed.
- **Problem:** The test tries to connect to the in-process master 50 times, 20 ms apart: one second. The
  `gpu-wgpu` job builds and runs heavier code on the same runner, so the master can take longer to bind.
- **Do:** Wait for the master's port file or for a successful connect with a bounded but generous limit
  (for example 30 s), as TEST-36 to TEST-38 did, and keep every assertion.
- **Done when:** The test passes 200 runs in a row under CPU starvation on Linux.
- **Implemented (2026-11-17, pull request 301):** Cause as in the Problem: both in-process masters of the
  test (the JSON-codec master and the round-robin master) were waited for with `for _ in 0..50` connects,
  20 ms apart, so one second to build the master's state and bind. Both waits now call
  `wait_for_master_bind`, a helper in `runtime_tests.rs` that retries the connect every 20 ms for up to 30 s
  and returns as soon as it succeeds. Every assertion is kept; no other fixed bind wait is left in the file.

### TEST-50: Wait for the dashboard banner lines before asserting in cli_dashboard_policy_e2e

- **Evidence:** `test_dev_mode_is_ignored_and_a_plain_http_warning_printed_for_a_non_loopback_dashboard`
  (`tests/cli_dashboard_policy_e2e.rs:258`, `assert!(log.contains("WARNING"))`) failed in `workspace tests
  (macos-latest)` on 2026-11-17, in pull request 309 (run `44192627449`, attempt 1, job `134425718401`). The
  log in the panic message ends at `Embedded Dashboard available at http://0.0.0.0:50311`: it has the
  master's tracing `WARN` about plain HTTP, but neither the banner line `Dashboard access: --dev /
  OXIDE_DEV_MODE ignored ...` nor the `WARNING: the dashboard is listening on ...` line.
- **Problem:** `run_master` (`crates/cli/src/cli_app/master.rs`) prints, after `MasterServer::spawn_with_config`
  has published both port files, `RustyGrid Master started`, the v3 public key, `Embedded Dashboard
  available`, the `dashboard_banner` lines (`println!`) and then the plain-HTTP `WARNING` (`eprintln!`), one
  write per line. The test's `start_master` waited for the ports and then (without asserting) up to 5 s for
  `Embedded Dashboard available`, and each test read the log once. On a slow runner that read can come
  between two of those writes. A race in the test, not a product defect.
- **Do:** In every test of the file that checks master log text, wait by condition (20 s bound) until every
  line it checks is present, print the log on timeout, and keep every assertion.
- **Done when:** The test binary passes 100 runs in a row under CPU load.
- **Implemented (2026-11-18, pull request 315):** `Service::wait_for_log(&[needles])` polls the log until it holds every needle,
  fails with the missing needles and the whole log after `LOG_WAIT` (20 s), and returns the log the test then
  asserts on. Five tests use it: the bare loopback master (banner line), the explicit token (waits for the
  `Dashboard access:` line before it checks that the open-access line is absent), the master on all
  interfaces (dashboard address and `secret show --dashboard-token`), `--dev` on a non-loopback dashboard
  (`WARNING`, `plain HTTP`, `--dev / OXIDE_DEV_MODE ignored`) and the non-loopback dashboard without `--dev`
  (`plain HTTP`). The unasserted 5 s wait in `start_master` is gone; no fixed sleep is added. On an Apple
  Silicon Mac (10 cores), two copies of the test binary at once, 100 runs each: 200 of 200 passed next to 12
  spinning processes and 200 of 200 next to 40. The old file did not fail here either (200 of 200 at each
  load), so the race was not reproduced locally; the CI log above is the evidence. No other test file reads
  the master's banner lines once after start-up.

### TEST-51: Let the run_master unit tests pick their own port instead of bind-and-drop

- **Evidence:** `cli_app::tests::test_master_with_larger_max_active_bytes_accepts_what_default_refuses_and_reports_bytes`
  (`crates/cli/src/cli_app/tests.rs:792`, "run_master bound and accepted connection") failed in `unit tests
  (ubuntu-latest)` on 2026-11-18, in the step `cargo test -p rusty_grid_cli --lib --features otel` of pull
  request 317 (docs only; run `44209756700`, attempt 1, job `134484800892`). The test printed `Failed to start
  master server: I/O error: Address already in use (os error 98)` before the panic.
- **Problem:** The test picked the master's port by binding `127.0.0.1:0`, reading the port and dropping the
  listener, and then started `run_master` on that port. The binary runs its 199 tests in parallel, and
  another test can take the port in between. Its connect loop was also a one-second window (50 tries, 20 ms
  apart). The sched2 runtime test (`crates/cli/src/cli_app/runtime_tests.rs`, TEST-49) picked the ports of
  its two in-process masters the same way.
- **Do:** Start each in-process master with `--listen 127.0.0.1:0 --port-file <tempdir>/master.port`, wait by
  condition (30 s bound) until the file holds a port, then wait by condition until `connect_master`
  succeeds; keep every assertion.
- **Done when:** The lib test binary of `rusty_grid_cli` passes 50 runs in a row under CPU load.
- **Implemented (2026-11-18, pull request 318):** Two helpers in `crates/cli/src/cli_app/tests.rs`, shared
  with `runtime_tests.rs`: `wait_for_master_port` polls the port file every 20 ms and fails with what it
  waited for when `run_master` ends first or `MASTER_START_BOUND` (30 s) passes, and
  `connect_master_within_bound` retries `connect_master` until it succeeds or the same bound passes, then
  fails with the last error. The `--max-active-mb` test and both masters of the sched2 test use them;
  TEST-49's `wait_for_master_bind` is replaced. Every assertion is kept. On an Apple Silicon Mac (10 cores),
  the lib test binary of `rusty_grid_cli` (199 tests, default parallel harness) passed 50 of 50 runs in a row
  next to 4 spinning `yes` processes. The old code was not looped, so the race was not reproduced locally;
  the CI log above is the evidence. Other tests that pick a port by bind-and-drop and then start a master, a
  worker or its portal on it were not changed:
  `crates/cli/tests/discovery_port_e2e.rs` (`free_tcp_port`, `free_udp_port`), `tests/workflow_modes_test.rs`
  (`two_free_ports`, `free_master_port`, `free_web_port`), `tests/m1_challenger_2_stress.rs` (two dashboard
  ports), `crates/master/src/server/tests.rs` (the discovery UDP port), `tests/cluster_auth_e2e.rs` (the
  discovery UDP port) and `crates/worker/src/client/tests.rs` (the standby portal port).

### TEST-52: Let the remaining tests pick ports through the process that binds them, not bind-and-drop

- **Evidence:** Found while fixing TEST-51 (pull request 318): six more test files pick a port by binding
  `127.0.0.1:0`, reading the port and dropping the socket, and then hand that port to a master, a worker portal or
  a dev cluster that binds it later. The TEST-51 failure (`Address already in use`, run `44209756700`) shows that
  another test can take such a port in between. None of the six has failed in CI so far.
- **Problem:** The same race as TEST-51 wherever the process that binds the port is told a number instead of
  picking one.
- **Do:** Where the product can pick the port and report it (port 0 with a port file, or a bound-address
  accessor), let it, and wait by condition with a 30 s bound. Where it cannot, keep the pick, give the reason in a
  one-line comment at the site, and name the product change that would remove it.
- **Done when:** Every site is changed or carries its reason, and each changed test binary passes 20 runs in a row
  under CPU load.
- **Partial (2026-11-18, pull request 320):**
  - Changed: both Web UI tests of `tests/m1_challenger_2_stress.rs` start the dashboard on `127.0.0.1:0` and read
    `MasterHandle::dashboard_addr()`. The reuse test spawns the second master on the same port again until it holds
    it (30 s bound, the "must be freed upon master shutdown" message kept) instead of binding a probe listener and
    dropping it first; its reachability waits are 30 s bounds (were 3 s). The portal part of
    `test_loopback_targeting_worker_portal_binds_loopback_not_all_interfaces` (`crates/worker/src/client/tests.rs`)
    binds on `portal_bind_addr_for_target(0, ..)`, asserts that the listener is on 127.0.0.1 and runs `run_portal`
    on it, as `portal_fixture` already does; its 600 ms connect loop is gone, because the listener is bound before
    the portal runs. It no longer goes through `spawn_worker_redirection_portal_with_secret`, which is that bind
    followed by `run_portal`. Every assertion is kept.
  - Kept, with the reason at the site: `crates/cli/tests/discovery_port_e2e.rs` (the TCP ports, because
    `ServerConfig::new` turns the beacon off for listen port 0 and `--discovery-port N` does not turn it back on,
    so a master whose beacon the test checks needs a fixed listen port; the UDP ports, because `--discovery-port 0`
    turns the beacon off and the master never publishes the port its beacon binds),
    `crates/master/src/server/tests.rs` and `tests/cluster_auth_e2e.rs` (the discovery UDP port, same reason), and
    `two_free_ports` and `free_master_port` in `tests/workflow_modes_test.rs` (`launch_local_dev_cluster_with_ports`
    gives the master port to its workers as `--master 127.0.0.1:<port>` and accepts the dashboard only when its
    port file holds the port it was given, so neither can be 0). `free_web_port` there is never bound: the master
    fails on the held grid port before it binds its dashboard, so a reused port cannot change that test's outcome.
    `test_dev_cluster_refuses_one_port_for_master_and_dashboard` binds its port in no process either.
  - Product changes that would remove the rest (not made here, each changes behaviour or a flag and needs a design
    record): let the dev cluster take 0 for both ports (master on `--listen 127.0.0.1:0 --port-file`, workers
    started once the port is read, the dashboard port taken from `--dashboard-port-file`), which would also remove
    the bind-and-drop in `launch_local_dev_cluster` (`crates/cli/src/mode_cli.rs`); bind the discovery socket before
    `MasterServer::spawn_with_config` returns and publish its address on `MasterHandle` and in a port file, with a
    way to ask for an ephemeral beacon port; let an explicit `--discovery-port N` start the beacon of a master that
    listens on port 0.
  - Not in this item (fixed ports, a different pattern): `test_beacon_of_a_master_with_a_loopback_dashboard_advertises_no_dashboard`
    in `crates/master/src/server/tests.rs` (UDP 18091) and `tests/discovery_integration.rs` (29003, 29004, 29006,
    each with a 150 ms sleep before the first connect).
  - On an Apple Silicon Mac (10 cores), next to 4 spinning `yes` processes, 20 runs in a row of each test binary
    the item touches passed 20 of 20: `m1_challenger_2_stress`, the `rusty_grid_worker` lib, the `rusty_grid_master`
    lib, `cluster_auth_e2e`, `discovery_port_e2e` and `workflow_modes_test` (120 of 120). The old code was not
    looped, and the race has not been reproduced locally.
- **Implemented (2026-11-19, pull request 364, open, owner decision 2026-11-19):** Option (a) only:
  `launch_local_dev_cluster` (`crates/cli/src/mode_cli/dev.rs`) delegates `(0, 0)` directly to
  `launch_local_dev_cluster_with_ports` without bind-and-drop probing of `9090` / `8080`, and
  `launch_local_dev_cluster_with_ports` (with `dev_launch.rs`) accepts port `0` for master and dashboard,
  passes `--port-file .dev_cluster.master_port` and `--dashboard-port-file .dev_cluster.dashboard_port`,
  waits for both port files before spawning workers, and polls `/api/status` up to 30 s.
  `tests/workflow_modes_test.rs` drops `two_free_ports`, `free_master_port`, and `free_web_port` and passes
  `0` instead. Options (b) and (c) are declined (owner decision 2026-11-19): the four sites carrying
  `// Bind-and-drop kept (TEST-52)` — `crates/cli/tests/discovery_port_e2e.rs` (TCP and UDP ports),
  `crates/master/src/server/tests.rs:147`, and `tests/cluster_auth_e2e.rs:564` — remain accepted exceptions
  because `--discovery-port 0` keeps its single meaning ("beacon off") and listen port `0` keeps the beacon
  off under [design 0084](design/0084-discovery-beacon-port.md). Updated [design 0027](design/0027-ox-mode-honest-reporting.md),
  [design 0084](design/0084-discovery-beacon-port.md), `README.md`, and `CHANGELOG.md`. On Apple Silicon (10 cores),
  next to 6 Python busy-loop processes, 20 sequential runs of `workflow_modes_test` (`--locked`) passed
  20/20 (`420/420` test cases, `692.74 s`).

### TEST-53: Wait by condition in the oversized task result worker test

- **Evidence:** `client::tests::test_oversized_task_result_fails_only_its_task_while_other_tasks_on_worker_finish`
  (`crates/worker/src/client/tests.rs`) failed once on the owner's Apple Silicon Mac on 2026-11-18, in a full
  `cargo test --workspace --lib` while other builds ran, with `both tasks finish on the same session:
  Elapsed(())`: its 5 s window for both task results ran out. It passed alone and on two reruns.
- **Problem:** The test gave the worker 5 s to connect and then 5 s in all for both results. Most of that window is
  CPU work on the test's single runtime thread: the worker's task runner computes
  `master_features::result_ack_token` for every result, which hashes the oversized task's whole 70 MiB
  `asset_bytes` with BLAKE3, unoptimized in the `test` profile. A `sample` of one run put 1,341 samples (about
  1.3 s) in `result_ack_token`, during which the session loop, the lane writer and the sibling task wait; both
  results then arrive within a millisecond of each other. A loaded machine stretches that past 5 s. The order of
  the two results does not depend on the clock: the session loop handles the two assignments one after the other,
  so the oversized task's 20 ms timer is set before the sibling's 200 ms one and its result enters the data lane
  first. A wall-clock window in the test, not an ordering defect.
- **Do:** Wait by condition (60 s bound) for the connection, the Register and both results; set the worker's
  dead-peer timeout and both task timeouts past that bound, because the test master is silent after the
  assignments and the default 20 s watchdog or the 10 s task timeouts would fail a slow run with another message;
  on failure print what was waited for and the state reached; keep every assertion.
- **Done when:** The test passes 100 runs in a row next to 6 spinning processes.
- **Implemented (2026-11-18, pull request 327):** `WAIT` (60 s) bounds the connection, the Register, both results
  and the worker's stop at the end (so the sandbox base outlives the worker, TEST-33); every wait ends as soon as
  its condition holds. The worker runs with `with_dead_peer_timeout(WAIT * 5)` and both tasks with a 300 s
  timeout. When the results do not arrive, the panic says why (the bound ran out, the worker disconnected, or a
  protocol error), after how long, which results arrived with their exit code and error, and which other messages
  came; the test prints the times. The assertions are unchanged: the oversized task fails with `exceeds` and
  `dropped asset_bytes` and keeps its engine stdout and stderr, the sibling exits 0, both on the one session in
  that order, and the worker never disconnects. On an Apple Silicon Mac (10 cores) next to 6 spinning `yes`
  processes: the old test passed 100 of 100 (1.45 s to 3.49 s per run), so the failure was not reproduced
  locally; the new test passed 100 of 100 (1.42 s to 2.51 s per run, both results 0.96 s to 1.85 s after the
  assignments). Not changed: `result_ack_token` is computed for every result, also when the session has no
  resume window and the token is not used. Other tests of the file with a window of 5 s or less that decides the
  verdict, on other helpers, were not changed: the lane writer tests (5 s and 2 s per frame), `wait_running` and
  the cancel and leftover sandbox tests (5 s, 2 s), `messages_after_a_battery_change` (5 s, and a collection window of two
  `MOBILE_TELEMETRY_CHECK_INTERVAL`s), the four `test_run_refuses_*` tests (5 s), the portal tests
  (`portal_get` 5 s, a 1 s read, and `started.elapsed() < 2 s` in
  `test_portal_request_reader_gives_up_on_a_silent_client`),
  `test_cancel_during_output_upload_reports_result_and_decrements_master_active_tasks` (5 s, 5 s, 2 s),
  `test_shutdown_requested_resolves_on_flag_or_dropped_sender_only` (2 s) and the Windows-only junk executable
  test (5 s).

### TEST-54: Wait by condition in the mapreduce parallelism test

- **Evidence:** `test_mapreduce_parallelism_is_configurable_and_a_failed_job_reports_failure`
  (`tests/master_limits_and_scheduling.rs`, `rusty_grid_master`) failed twice under heavy load on the owner's
  Apple Silicon Mac on 2026-11-18 with a 60 s mapper timeout, then passed 8 of 8 runs.
- **Problem:** The test waited up to 10 s for the worker to register and gave each `MapReduceJobSpec` a 60 s
  `timeout_secs`, with no outer condition wait. Under heavy CPU contention on the same machine, spawning `sh` and
  scheduling three sequential 1 s map tasks stalled past the 60 s mapper timeout, failing inside the MapReduce
  engine without queue diagnostics.
- **Do:** Replace each wait with a condition wait with a generous bound (`180 s`) that returns as soon as the
  condition holds, raise the job mapper timeouts strictly above that bound (`540 s`), keep every assertion
  (including the sequential execution lower bound and error message checks), and stop and join the worker before
  its sandbox `TempDir` drops.
- **Done when:** `master_limits_and_scheduling` passes and 50 runs in a row of
  `test_mapreduce_parallelism_is_configurable_and_a_failed_job_reports_failure` pass next to 6 `yes > /dev/null`
  CPU burners.
- **Implemented (2026-11-18, pull request 333):** `WAIT` (`180 s`) bounds `wait_for_worker_count_within`, the
  `"serial"` `execute_mapreduce` call, the `"failing"` `execute_mapreduce` call, and worker shutdown before
  `TempDir` drop (`TEST-33`); every wait ends as soon as its condition holds. Both `MapReduceJobSpec` jobs pass
  `mapper_timeout_secs = 540` (`WAIT * 3`), and a timed-out `WAIT` prints `master.queue_stats()`. Every assertion
  stays intact, and the test prints registration and job timings. Verification on an Apple Silicon Mac (10 cores):
  `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, full
  `master_limits_and_scheduling` (5 passed in 4.55 s), and 50 of 50 runs of
  `test_mapreduce_parallelism_is_configurable_and_a_failed_job_reports_failure` next to 6 `yes > /dev/null`
  processes (`min=3.72s`, `max=27.73s`). Not changed in `tests/master_limits_and_scheduling.rs`:
  `wait_for_worker_count` (10 s default kept for the other tests), `closed_by_server` (`2 s` / `100–300 ms`
  probes in connection-budget tests), `wait_task` (`15 s` in GPU backlog test), and
  `test_mapreduce_tasks_run_on_all_workers_at_once` (`60 s` mapper timeout and `elapsed < 7 s`).

### TEST-55: Windows Home: sandbox s1 grant test without the Remote Desktop Users group

- **Evidence:** `sandbox::windows_private_dir_tests::s1_write_grants_are_refused_and_read_only_grants_warn_once_with_working_remedy`
  (`crates/worker/src/sandbox.rs:2911`) failed on a Windows 11 Home laptop with
  `*S-1-5-32-555: No mapping between account names and security IDs was done` at
  `crates\worker\src\sandbox.rs:2927:9`.
- **Problem:** Windows Home editions have no `BUILTIN\Remote Desktop Users` local group
  (`S-1-5-32-555`), so `icacls /grant "*S-1-5-32-555:..."` fails `LookupAccountSidW` before
  `ensure_private_dir` is tested. Production code in `rusty_grid_core::private_file` only formats
  `S-1-5-32-555` by string comparison in `sid_label` and does not require the group to exist on
  the host.
- **Do:** Replace `SID_REMOTE_DESKTOP_USERS` in the on-disk `icacls` test with `SID_GUESTS`
  (`S-1-5-32-546`, `BUILTIN\Guests`, present on every Windows edition) while keeping `NETWORK`,
  `BUILTIN\Users`, `ANONYMOUS LOGON`, and `ALL APPLICATION PACKAGES` and every assertion of
  [design 0021](design/0021-private-default-sandbox-base.md).
- **Done when:** `cargo test -p rusty_grid_worker --lib --locked` passes in full on Windows 11 Home.
- **Implemented (2026-11-19, pull request 356):** Replaced `SID_REMOTE_DESKTOP_USERS` with
  `SID_GUESTS` (`BUILTIN\Guests`) in
  `s1_write_grants_are_refused_and_read_only_grants_warn_once_with_working_remedy`
  (`crates/worker/src/sandbox.rs`), keeping the in-memory `SID_REMOTE_DESKTOP_USERS` coverage in
  `crates/core/src/private_file.rs`; the full `rusty_grid_worker` lib suite passes on Windows 11
  Home.

### TEST-56: Cut the time of `workspace tests (windows-latest)` without losing coverage

- **Evidence:** `OxideSwarm_Agent_Hub` `test_runs/win2_test29_2026-11-19.md` (sections 4 and 5): across
  35 completed `workspace tests (windows-latest)` CI jobs created on 2026-11-18, 34 passed (1
  unrelated Dependabot `sysinfo` compile failure; 0 lost), with total job durations min `1,424 s`
  (`23.7 min`), median `1,812.5 s` (`30.2 min`), max `2,586 s` (`43.1 min`), and Step 8 max `2,216 s`
  (`36.9 min`).
- **Problem:** Four test/workflow bottlenecks inflated Windows CI wall time: `test_dev_mode_watch_and_aliases`
  ran `cargo check --workspace` over the whole repository twice; `serialization_benchmark` and
  `serialization_benchmark_multi_size` ran thousands of `opt-level=0` loops and wall-clock throughput
  asserts by default; `blob_upload_pipelining_e2e` and `blob_transfer_e2e` created/copied `10,000`
  and `4,000` tiny files on NTFS; and Step 8 re-ran all 30 Windows `test_feat3_os_isolation` tests in
  parallel after Step 7 had already executed them serially (`--test-threads=1`).
- **Do:** Make the four targeted optimizations in order (`workflow_modes_test` single workspace check;
  `OXIDESWARM_RUN_BENCHMARKS=1` gating on the serialization benchmarks; `1,600`-file `#[cfg(windows)]`
  counts in the two blob e2e tests; and step-scoped `RUSTY_GRID_SKIP_FEAT3_OS_ISOLATION_STEP8`
  deduplication in Step 8 without `cargo-nextest` or new CI actions), then measure both local
  binaries and the PR's `workspace tests (windows-latest)` job time.
- **Done when:** The PR's `workspace tests (windows-latest)` job is faster than the `30.2 min` median
  baseline with no test removed.
- **Implemented (2026-11-19, pull request 361, open):**
  1. `tests/workflow_modes_test.rs`: keeps `run_watch_check(&root)` once on the real workspace root
     and exercises the `Dev` `"reload"` alias on a minimal 2-file temporary crate (`Cargo.toml` +
     `src/lib.rs`). Local `workflow_modes_test` (21 tests): `59.08 s -> 29.98 s` `libtest`
     (`64.80 s -> 37.23 s` warm wall-clock).
  2. `crates/core/tests/serialization_benchmark.rs` and `serialization_benchmark_multi_size.rs`: keep
     every round-trip, payload-size, and wire-discriminator assertion unconditional with compact
     default iterations (`5`/`25` and `[20, 10, 5, 2]`), gating full loops (`5,000` and
     `[2000, 1000, 500, 50]`) and wall-clock assertions behind `OXIDESWARM_RUN_BENCHMARKS=1`. Local:
     `23.84 s -> 0.14 s` (`74.48 s -> 2.72 s` wall) and `37.64 s -> 1.77 s` (`47.75 s -> 4.14 s`
     wall); both pass with `OXIDESWARM_RUN_BENCHMARKS=1` (`23.89 s` and `35.79 s` `libtest`).
  3. `tests/blob_upload_pipelining_e2e.rs` and `tests/blob_transfer_e2e.rs`: keep `10,000` and `4,000`
     under `#[cfg(not(windows))]` and use `1,600` under `#[cfg(windows)]` (`100x`
     `MAX_IN_FLIGHT_REQUESTS = 16` with `proxy.peak() > 1`; `1,600 KiB` > `1 MiB` worker cache by
     `576 KiB`). Local: `14.09 s -> 2.02 s` (`141.70 s -> 7.56 s` wall) and `68.73 s -> 30.19 s`
     (`87.03 s -> 35.49 s` wall, 11/11 passed; pin test `64.34 s -> 28.43 s` `libtest`).
  4. `crates/worker/tests/test_feat3_os_isolation.rs`, `.github/workflows/ci.yml`, and
     `tests/test_launcher_scripts.py`: sets `RUSTY_GRID_SKIP_FEAT3_OS_ISOLATION_STEP8` ONLY on
     Windows CI Step 8 (never containing `"OXIDE"`, so `test_sec5_version_probes_strip_secrets` stays
     clean) so all 30 `#[cfg(windows)]` tests print `skipped:` in Step 8 after Step 7 runs them
     serially, while newly added binaries or unannotated tests still run; statically guarded in
     `test_windows_ci_feat3_dedup_cannot_drop_step7_or_new_tests`. Local Step 8 mode: `66.85 s ->
     0.00 s` `libtest` (`123.72 s -> 3.83 s` wall, 30/30 `skipped:`).
  - **PR #361 CI result (`44437998772`, `windows-latest` job `135271584175`, green across all 3 OSes):** total job duration `1,416 s` (`23.60 min`, vs `30.2 min` median / `43.1 min` max baseline — `-6.6 min`), with Step 7 in `165 s` (`2.75 min`) and Step 8 in `1,149 s` (`19.15 min`).

### TEST-58: Find why the keep_sandboxes sweep test fails on macOS

- **Evidence:** `sandbox::tests::test_ensure_usable_keep_sandboxes_retains_and_subsequent_default_sweeps`
  (`crates/worker/src/sandbox.rs`, `-p rusty_grid_worker --lib`) failed twice on `macos-latest`: run
  `44184956339` (`unit tests (macos-latest)`, 2026-11-17) and run `44250010401` (`workspace tests
  (macos-latest)`, 2026-11-18), both panicking at `!sb_path.exists()` (`subsequent ensure_usable() with
  keep_sandboxes == false must sweep retained unlocked sandboxes`, `crates/worker/src/sandbox.rs:2645`).
  Reproduced locally on Apple Silicon (10 cores) at `origin/master` (`f0989363`): `0/200` alone, `0/200`
  within `sandbox::tests::`, and `2/50` failures (iterations 3 and 16, both at `crates/worker/src/sandbox.rs:2645:9`)
  when run in parallel with six `runner::*` subprocess tests in the same `rusty_grid_worker --lib` binary.
- **Problem:** `Sandbox::create` holds an exclusive `flock` (`file.try_lock()`) on `<base_dir>/.locks/<task_id>.lock`.
  On Unix, `flock(2)` locks attach to the open file description rather than the descriptor. While other
  tests in the same `rusty_grid_worker` test process concurrently spawn child tasks via
  `crates/worker/src/process_tree.rs` (`pre_exec` double-forks a detached lifeline supervisor), the forked
  children inherit duplicate descriptors pointing to the open file description (`O_CLOEXEC` only closes on
  `execve`, not `fork`). On developer shells with `ulimit -n` at `1,048,576` (not macOS's default: `launchd`
  defaults to `256` and `packaging/macos/com.oxideswarm.worker.plist` sets `65,536`), each detached
  supervisor closes descriptors in ascending order (`0, 1, 2, ...`) up to `1,048,575` (`process_tree.rs:691-732`,
  `1034-1050`); while ascending `close` drops the low-numbered lock fd early, iterating up to the soft limit
  burns CPU across cores and widens the scheduling window during which newly forked children still hold the
  inherited lock descriptor (confirmed by the PR 366 review run on `f0989363`: `4/30` failures at soft limit
  `1,048,576`, `0/30` at `1,024`, and `0/40` after the fix). Because `Sandbox::destroy` (`keep_sandboxes == true`
  and `false`) and `Drop for Sandbox` only set `self.lock_file = None` (`close(fd)`) without calling
  `File::unlock()` (`LOCK_UN`), `close` left the `flock` held until the last child closed its duplicate,
  causing `sweep_leftover_sandboxes_sync` -> `open_and_try_lock_task` -> `try_lock()` to see `WouldBlock`
  and leave the retained sandbox unswept.
- **Do:** Reproduce in loops on macOS, add a deterministic unit test modeling an inherited duplicate lock
  descriptor (`try_clone()`), explicitly unlock task lock files (`File::unlock()`) before closing in
  `crates/worker/src/sandbox.rs`, and keep `test_ensure_usable_keep_sandboxes_retains_and_subsequent_default_sweeps`
  and its `!sb_path.exists()` assertion intact.
- **Done when:** Cause is named, the fix passes deterministically and in 200-run alone, 200-run `sandbox::tests`,
  and 200-run concurrent loops on macOS without weakening assertions.
- **Implemented (2026-11-19, pull request 366, open):** Added `release_task_lock` in
  `crates/worker/src/sandbox.rs` (calls `lock_file.unlock()` before `drop(lock_file)`) and used it across
  `Sandbox::destroy`, `Drop for Sandbox`, `Sandbox::create` error cleanup, `open_and_try_lock_task_with`,
  and `sweep_leftover_sandboxes_sync`, plus deterministic unit test
  `sandbox::tests::test_keep_sandboxes_destroy_unlocks_even_with_inherited_lock_fd` (confirmed to fail before
  the fix at `sandbox.rs:2690:9` and pass after). The `process_tree.rs` macOS ascending close-loop CPU/wall
  burn is tracked separately as `BUG-33`. Post-fix verification on Apple Silicon (10 cores, private `HOME`): `0/200` failures alone, `0/200` across `sandbox::tests::`, `0/200` alongside the six
  concurrent `runner::*` tests, and `0/20` across the full `rusty_grid_worker --lib` binary (304 tests/run).

### TEST-57: m2 capability stress test: wait for reconnects by condition, not a fixed 1.8 s window

- **Evidence:** `test_capability_queries_under_connect_disconnect_stress` (`tests/m2_challenger_2_stress.rs:735`)
  failed on `windows-latest` in run `44308601329` (attempt 1, after 33 minutes) with
  `The churn workers must reconnect during the run (observed 8 registrations from 8 workers)`.
- **Problem:** The test slept for a fixed `1800 ms` (`tests/m2_challenger_2_stress.rs:698`) and then
  asserted `registered >= 16` and `> 50` iterations in each of the four query loops. Under a starved
  CI runner, all 8 churn workers completed their initial registration (`observed 8 registrations`),
  leaving too little of the fixed `1.8 s` window for any worker to reconnect before shutdown.
- **Do:** Replace the fixed `1.8 s` sleep with a `50 ms` polling loop up to a `60 s` deadline until
  `registered >= 16` and all four query counts exceed `50`, stopping early if any query task finishes,
  and keep both final assertions with the elapsed time and counts printed on failure.
- **Done when:** Both assertions stay, idle run time remains about `2 s`, and the test passes `50/50`
  (`25` alone and `25` under 12 CPU busy loops on 20 logical cores) on Windows.
- **Implemented (2026-11-19, pull request 365):** Polls `registry.counts()` and all four atomic
  query counters every `50 ms` up to `60 s` in `tests/m2_challenger_2_stress.rs` and prints
  `stress_elapsed` and counts on completion or deadline expiry. On a Windows x86_64 machine (20 logical cores, `rustc 1.98.1`,
  a private folder), `cargo test --locked -p rusty_grid_master --test m2_challenger_2_stress test_capability_queries_under_connect_disconnect_stress -- --exact`
  passed `50/50` (`0` failures): `25/25` alone (wall `5.0398 / 5.1959 / 6.0501 s`, libtest
  `0.85 / 0.86 / 1.01 s`) and `25/25` under 12 CPU busy loops (`82.0%` host CPU; wall
  `5.6473 / 5.8747 / 6.6347 s`, libtest `1.00 / 1.02 / 1.45 s`). Workspace `cargo +1.96.0 fmt --all -- --check`
  and `cargo +1.96.0 clippy --workspace --all-targets --locked -- -D warnings` passed with `0` warnings.

### TEST-59: Zero-duration task test: drop the 100 ms wall-clock window

- **Evidence:** `test_tier2_bva_zero_duration_builtin_task_instant_completion`
  (`tests/e2e_cluster.rs:1078:5`) failed in `workspace tests (macos-latest)` on 2026-11-18 at `be6c1e9`
  (run `44209594539`, job `134484257136`) with `0-duration task must complete nearly instantly`.
- **Problem:** The test asserted `elapsed < Duration::from_millis(100)` across a full master-worker
  round trip (`submit_and_wait`), which depends on runner scheduling rather than task behaviour.
- **Do:** Raise the cluster/task safety timeouts (`30 s`), print `elapsed` and `res.execution_time_ms`
  instead of asserting `elapsed < 100 ms`, and assert task success (`res.is_success()`, `exit_code == 0`,
  `error.is_none()`), the `0 ms` builtin sleep stdout, and bounded worker `execution_time_ms`.
- **Done when:** No wall-clock `< 100 ms` assertion remains, `elapsed` is printed, and the test passes
  `50/50` (`25` alone and `25` under 12 CPU busy loops on 20 logical cores) on Windows.
- **Implemented (2026-11-19, pull request 365):** Updated
  `test_tier2_bva_zero_duration_builtin_task_instant_completion` in `tests/e2e_cluster.rs` as planned.
  On a Windows x86_64 machine (20 logical cores, `rustc 1.98.1`, a private folder),
  `cargo test --locked -p rusty_grid_cli --test e2e_cluster test_tier2_bva_zero_duration_builtin_task_instant_completion -- --exact`
  passed `50/50` (`0` failures): `25/25` alone (wall `6.5930 / 6.8475 / 9.2592 s`, libtest
  `0.58 / 0.60 / 1.15 s`) and `25/25` under 12 CPU busy loops (`90.0–97.0%` host CPU; wall
  `7.0569 / 13.5401 / 27.8546 s`, libtest `0.67 / 0.99 / 1.63 s`). Workspace `cargo +1.96.0 fmt --all -- --check`
  and `cargo +1.96.0 clippy --workspace --all-targets --locked -- -D warnings` passed with `0` warnings.

### TEST-60: Windows: worker client::tests time out at 5 s under parallel load

- **Evidence:** During full `cargo test -p rusty_grid_worker --lib --locked` under parallel load on
  Windows 11 Home (12 logical threads), four `client::tests` cases failed at `5 s`
  `Elapsed(())` timeouts (reproduced `9/20` failures in full `--lib` under 6 CPU busy loops on
  `origin/master` `4b9be26`, while filtered `client::tests` alone passed `20/20` alone and `20/20`
  under 6 busy loops):
  - `test_handle_cancel_task_stops_task_waiting_for_a_permit_and_reports_130`: `9/20` (`client/tests.rs:1369`, `running task did not finish: Err(Elapsed(()))`),
  - `test_cancel_during_output_upload_reports_result_and_decrements_master_active_tasks`: `5/20` (`client/tests.rs:3077`, `task must reach upload_task_outputs and send BlobRequest: Elapsed(())`),
  - `test_a_leftover_sandbox_directory_does_not_fail_the_assignment`: `3/20` (`client/tests.rs:1429`, `no result: Err(Elapsed(()))`),
  - `test_windows_worker_task_running_junk_exe_fails_fast_without_error_dialog`: `3/20` (`client/tests.rs:3962`, `child process running junk .exe must fail fast without hanging: Elapsed(())`),
  alongside an unrelated `1/20` MSYS2 `sh.exe` `add_item` fork/mount collision on 1 run.
- **Problem:** Multiple async waits in `crates/worker/src/client/tests.rs` used tight `1–5 s`
  timeouts (and a `< 2 s` elapsed check in `test_portal_request_reader_gives_up_on_a_silent_client`),
  per-recv `5 s` timeouts inside loops instead of a total deadline, and tight `5–10 s` mock task
  timeouts, tripping when the full `273`-test `--lib` binary scheduled dozens of subprocesses in parallel.
- **Do:** Replace those `1–5 s` waits and the `< 2 s` wall-clock check with generous condition waits
  (`TEST_WAIT_TIMEOUT = 60 s` using total `timeout_at` deadlines in loops, `30 s` on `junk .exe`
  modal-dialog checks and Unix hung-Blender probe, `5 s` grandchild poll), raise tight mock task
  timeouts (`5–30 s` -> `30–60 s`, Case 2 background child `sleep 30 &` / `ping -n 31`), replace the
  unbounded loop in `test_with_keep_sandboxes_a_leftover_directory_is_moved_aside_not_deleted` with a
  bounded `60 s` deadline, and name the awaited condition and observed message variants on failure
  without weakening any behaviour assertion.
- **Done when:** All assertions remain intact, `client::tests` passes `20/20` alone and `20/20` under
  `6` busy loops, and full `rusty_grid_worker --lib` under `6` busy loops records `0` `client::tests`
  `Elapsed(())` timeouts.
- **Implemented (2026-11-19, pull request 378, open):** Updated `crates/worker/src/client/tests.rs` as planned (keeping
  negative-assertion `50 ms` and `300 ms` waits untouched). On Windows 11 Home (12 logical threads,
  isolated env `%USERPROFILE%\ox_c_env\test60_env`), post-fix verification passed:
  - `cargo test -p rusty_grid_worker --lib --locked client::tests` (`81` tests): `20/20` passed
    alone (`4.23–4.46 s` libtest / `5.08–6.32 s` wall) and `20/20` passed under `6` Python busy loops
    (`4.48–8.24 s` libtest / `5.70–10.25 s` wall);
  - `cargo test -p rusty_grid_worker --lib --locked` (`273` tests): `20/20` passed (`0` failures)
    under `6` Python busy loops (`12.11–30.99 s` libtest / `13.57–32.73 s` wall, vs `11/20` pass /
    `9/20` fail baseline).
  Workspace `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`,
  `ox-mode test size`, and `ox-mode doc verify` all passed.

### TEST-63: master_limits_and_scheduling: prove MapReduce concurrency with a barrier instead of a 7 s window

- **Evidence:** In loaded workspace Run 5 on a 10-core Apple Silicon Mac (`origin/master` `fc694126` under
  `6` Python CPU busy loops, private `HOME`), `test_mapreduce_tasks_run_on_all_workers_at_once`
  (`tests/master_limits_and_scheduling.rs:256:5`) failed `1/5` runs at
  `9 one-second tasks on 4 slots took 20.682465417s: tasks are not running concurrently`.
- **Problem:** The test used `sleep 1` in 4 mappers and 5 reducers and checked `elapsed < Duration::from_secs(7)`
  with a `60 s` mapper timeout (left unchanged by `TEST-54` in pull request `333`); under heavy host CPU
  contention, spawning 9 sequential/wave shell + Python subprocesses stretched past `7 s` even when scheduled
  concurrently, without directly proving all 4 slots ran mappers at the same time.
- **Do:** Replace `sleepy_job` with `barrier_job`: each mapper writes `$RUSTY_GRID_WORKER_ID` into a temporary
  barrier directory (`$barrier/active-$RUSTY_GRID_TASK_ID`) and polls until all `4` `active-*` files exist
  simultaneously (impossible unless both 2-slot workers hold 2 active mappers at once), assert `4` markers
  distributed `2`/`2` across the `2` workers, use `WAIT = 180 s` (`mapper_timeout_secs = 540`) with queue +
  barrier diagnostics on timeout, drop `elapsed < Duration::from_secs(7)`, and shut down `w1`/`w2` cleanly
  before `TempDir` drop.
- **Done when:** Concurrency across both workers is proven directly by the barrier, all outcome assertions
  stay intact, and `cargo test -p rusty_grid_master --test master_limits_and_scheduling --locked -- --exact test_mapreduce_tasks_run_on_all_workers_at_once`
  passes `20/20` under `6` busy loops after the fix.
- **Implemented (2026-11-19, pull request 389, open):** Updated `tests/master_limits_and_scheduling.rs` as planned.
  On a 10-core Apple Silicon Mac under `6` Python CPU busy loops (`python3 -c 'while True: pass'`), private
  `HOME`, running `test_mapreduce_tasks_run_on_all_workers_at_once` (`--locked`):
  - Before fix (on `origin/master` `8641b67d`): `20/20` passed (`120.02 s` total, `5.05–7.33 s` per run;
    reproduced `1/5` in full workspace Run 5).
  - After fix: `20/20` passed (`57.08 s` total, `1.90–5.25 s` per run).
  Full `master_limits_and_scheduling`, workspace `fmt`, `clippy --workspace --all-targets --locked -- -D warnings`,

### TEST-66: worker_cluster_auth: drop wall-clock upper bounds on handshake timeout and shutdown tests

- **Evidence:** In loaded workspace Run 5 on a 10-core Apple Silicon Mac (`origin/master` `fc694126` under
  `6` Python CPU busy loops, private `HOME`), `test_unanswered_handshake_times_out_with_a_secret`
  (`tests/worker_cluster_auth.rs:382:5`) failed `1/5` runs at `assertion failed: started.elapsed() < Duration::from_secs(3)`.
- **Problem:** `tests/worker_cluster_auth.rs` checked wall-clock upper bounds (`< 3 s` around a `300 ms`
  handshake timeout in `test_unanswered_handshake_times_out_with_a_secret`, `< 2 s` in
  `test_shutdown_interrupts_a_handshake_with_a_silent_master` after a fixed `400 ms` sleep, and `< 2 s` /
  `5 s` in `test_auth_failure_backs_off_instead_of_hot_looping_and_shutdown_interrupts_it` alongside a
  `10 s` `RunningWorker::stop` bound), which trip on a CPU-starved host.
- **Do:** Raise `RunningWorker::stop` to `TEST_WAIT_TIMEOUT` (`60 s`), wait by condition for the initial
  auth attempt / silent-master TCP accept (`tokio::sync::Notify`) up to `60 s`, bound the `300 ms`
  unanswered-handshake test by `tokio::time::timeout(TEST_WAIT_TIMEOUT, ...)` without `< 3 s` or `< 2 s`
  wall-clock assertions, and assert `shutdown` ends the `30 s`-floored auth backoff within `25 s` with
  `attempts == 1`.
- **Done when:** All outcome assertions stay intact and `cargo test -p rusty_grid_worker --test worker_cluster_auth --locked`
  passes `20/20` under `6` busy loops after the fix.
- **Implemented (2026-11-19, pull request 387, open):** Updated `tests/worker_cluster_auth.rs` as planned. On
  a 10-core Apple Silicon Mac under `6` Python CPU busy loops (`python3 -c 'while True: pass'`), private
  `HOME`, running `cargo test -p rusty_grid_worker --test worker_cluster_auth --locked`:
  - Before fix (on `origin/master` `fc694126`): `20/20` passed (`188.76 s` total, `8.11–10.36 s` per run;
    reproduced `1/5` in full workspace Run 5).
  - After fix: `20/20` passed (`198.06 s` total, `8.54–11.57 s` per run).
  Workspace `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`,
  `worker_cluster_auth`, and `python3 tests/test_m1_challenger_hygiene.py` all passed.

### TEST-62: blob_transfer_e2e: print CLI submit output on failure and harden loaded timeouts

- **Evidence:** In loaded workspace Run 5 on a 10-core Apple Silicon Mac (`origin/master` `fc694126` under
  `6` Python CPU busy loops, private `HOME`), `cli::test_cli_submit_input_and_output_round_trip_with_dedup`
  (`tests/blob_transfer_e2e.rs:1073:9`) failed `1/5` runs at `assertion left == right failed: left: Some(1), right: Some(7)`
  when the `submit --wait --timeout-secs 30` subcheck for `exit 7` exited `1`, without printing stdout/stderr.
- **Problem:** Inside `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]`, synchronous
  `std::process::Command::output()` and `std::thread::sleep` calls blocked Tokio runtime threads hosting
  the in-process `WorkerClient`, while `rusty-grid master` kept its `10 s` default `--heartbeat-timeout-secs`,
  `port_file`/registration polls used `20 s`, `submit` used `30–60 s`, and failing CLI assertions suppressed
  stdout, stderr, and `master.log`.
- **Do:** Run `rusty-grid` CLI subprocesses via `tokio::task::spawn_blocking`, replace synchronous polling
  with `60 s` async loops, pass `--heartbeat-timeout-secs 60` to `rusty-grid master`, raise `submit`
  `--timeout-secs` to `120`, include stdout, stderr, and `master.log` in all CLI submit failure messages,
  and shut down the in-process worker cleanly (`stop_worker` + `60 s` join timeout).
- **Done when:** All assertions stay intact, failure messages include full CLI and master diagnostics, and
  `cargo test -p rusty_grid_cli --test blob_transfer_e2e --locked -- --exact cli::test_cli_submit_input_and_output_round_trip_with_dedup`
  passes `20/20` under `6` busy loops after the fix (plus a full-binary run).
- **Implemented (2026-11-19, pull request 390, open):** Updated `tests/blob_transfer_e2e.rs` as planned. On a
  10-core Apple Silicon Mac under `6` Python CPU busy loops (`python3 -c 'while True: pass'`), private
  `HOME`:
  - Before fix (on `origin/master` `8641b67d`, with diagnostic capture added): test alone passed `20/20`
    (`98.11 s` total, `3.61–6.98 s` per run; full binary `1/1` loaded probe `30.08 s`; reproduced `1/5`
    in full workspace Run 5).
  - After fix (test alone): `20/20` passed (`98.23 s` total, `3.88–6.83 s` per run).
  Full `blob_transfer_e2e`, workspace `fmt`, `clippy --workspace --all-targets --locked -- -D warnings`,
  and `python3 tests/test_m1_challenger_hygiene.py` all passed.

### TEST-64: worker lib process_tree_tests: wait for partial stdout by condition before cancelling

- **Evidence:** In loaded workspace Run 5 on a 10-core Apple Silicon Mac (`origin/master` `fc694126` under
  `6` Python CPU busy loops, private `HOME`),
  `runner::process_tree_tests::test_partial_output_survives_timeout_and_cancellation`
  (`crates/worker/src/runner/process_tree_tests.rs:313:5`) failed `1/5` runs with empty stdout (`""`) on
  the cancellation subtest.
- **Problem:** The cancellation subtest slept a fixed `600 ms` (`tokio::time::sleep(Duration::from_millis(600))`)
  after `tokio::spawn(execute_task(...))` and immediately sent `cancel_tx`; under CPU contention `sh` had
  not yet flushed `echo partial-out` before being cancelled. The preceding timeout subtest also used a
  tight `1 s` task timeout.
- **Do:** Raise the timeout subtest to `timeout_secs = 3`; in the cancellation subtest, write a temporary
  marker file immediately after `echo partial-out`, poll every `20 ms` up to a `60 s` deadline (panicking
  early if the task future exits beforehand) before sending cancellation, bound awaiting `run` with `60 s`,
  and print `exit_code`/`error`/`stdout`/`stderr` on failure.
- **Done when:** All assertions remain intact and
  `cargo test -p rusty_grid_worker --lib --locked -- --exact runner::process_tree_tests::test_partial_output_survives_timeout_and_cancellation`
  passes `20/20` under `6` busy loops after the fix.
- **Implemented (2026-11-19, pull request 388, open):** Updated `crates/worker/src/runner/process_tree_tests.rs`.
  On a 10-core Apple Silicon Mac under `6` Python CPU busy loops (`python3 -c 'while True: pass'`), private
  `HOME`, running `runner::process_tree_tests::test_partial_output_survives_timeout_and_cancellation` (`--locked`):
  - Before fix (on `origin/master` `8641b67d`): `20/20` passed (`62.54 s` total, `2.04–4.11 s` per run;
    reproduced `1/5` in full workspace Run 5).
  - After fix: `20/20` passed (`86.09 s` total, `3.45–5.45 s` per run).
  All `runner::process_tree_tests` unit tests, workspace `fmt`, `clippy --workspace --all-targets --locked -- -D warnings`,
  and `python3 tests/test_m1_challenger_hygiene.py` passed.

### TEST-65: test_sec5_hardened_runner: condition waits for short-task pipe EOF and setsid survivor kill

- **Evidence:** Two failures on `test_sec5_hardened_runner`:
  - On 10-core Apple Silicon macOS (loaded workspace Run 5 at `origin/master` `fc694126`),
    `test_feat2_short_task_finishes_under_one_second_without_pipe_note`
    (`crates/worker/tests/test_sec5_hardened_runner.rs:1772:5`) failed `1/5` runs at
    `elapsed < Duration::from_secs(1)` (`took 2.179053666s`).
  - On `ubuntu-latest` (CI run `44525741499`, PR #383 docs-only, job `135565717998`),
    `test_feat2_normal_task_completion_kills_setsid_survivor_within_one_second` panicked at
    `crates/worker/tests/common/mod.rs:261:5` (`Orphaned descendant processes survived termination: [2532]`)
    after `assert_all_dead_async(&pids, Duration::from_secs(1))`.
- **Problem:** `test_feat2_short_task_finishes_under_one_second_without_pipe_note` checked `elapsed < 1 s`
  instead of relying on the product invariant (`execute_process_supervised` drains stdout/stderr up to
  `pipe_drain_grace` *before* `tree.kill_async` dismisses the lifeline supervisor, so pipe EOF arriving
  without `"kept the output pipes open"` proves the supervisor closed inherited pipe descriptors).
  Meanwhile, `test_feat2_normal_task_completion_kills_setsid_survivor_within_one_second` and its immediate
  siblings used `1–2 s` / `15 s` timeouts on PID polling, process termination, and supervisor exit; `is_pid_alive`
  only skipped `ProcessStatus::Zombie` (not `Dead`); and Linux `kill_cgroup_leaf` / `kill_escaped_by_env`
  did not retry `open`/`read`/`SYS_getdents64` on `EINTR` if `SIGCHLD` arrived mid-sweep.
- **Do:** Keep the legacy test names, configure `pipe_drain_grace = 10 s` and remove `< 1 s` in
  `test_feat2_short_task_finishes_under_one_second_without_pipe_note`, raise the condition-wait deadlines to
  `FEAT2_WAIT_TIMEOUT = 60 s` (logging elapsed kill/exit times) in the `setsid` survivor, concurrent-attempt,
  and supervisor-exit tests, treat `ProcessStatus::Dead` as dead alongside `Zombie` with survivor `(pid, status, name)`
  diagnostics in `crates/worker/tests/common/mod.rs`, and retry `EINTR` in Linux `kill_cgroup_leaf` and
  `kill_escaped_by_env` (`crates/worker/src/process_tree.rs`).
- **Done when:** All outcome invariants stay intact and `cargo test -p rusty_grid_worker --test test_sec5_hardened_runner --locked`
  passes `20/20` under `6` busy loops on macOS, with Linux verified by CI (`sec5 hardened runner (ubuntu-latest)`
  and `workspace tests (ubuntu-latest)`).
- **Implemented (2026-11-19, pull request 388, open):** Updated `crates/worker/tests/test_sec5_hardened_runner.rs`,
  `crates/worker/tests/common/mod.rs`, and `crates/worker/src/process_tree.rs`. On 10-core Apple Silicon macOS
  under `6` Python busy loops, private `HOME`, running `cargo test -p rusty_grid_worker --test test_sec5_hardened_runner --locked`:
  - Before fix (on `origin/master` `8641b67d`): `20/20` passed (`84.62 s` total, `3.03–6.03 s` per run;
    reproduced `1/5` in full workspace Run 5).
  - After fix: `20/20` passed (`76.26 s` total, `3.02–4.08 s` per run).
  Linux (`ubuntu-latest`) is covered by CI (`sec5 hardened runner (ubuntu-latest)` and `workspace tests (ubuntu-latest)`);
  Docker is not installed on this Mac. Workspace `fmt`, `clippy --workspace --all-targets --locked -- -D warnings`,
  and `python3 tests/test_m1_challenger_hygiene.py` all passed.

### TEST-67: Campaign driver tests flake on Windows log appends and macOS 0.2 s submit timeout

- **Evidence:** `tests/test_campaign_driver.py` failed intermittently in the `agent bridge` CI job on
  `master` commit `4888dd5` and pull requests 385, 388, 389, 391, and 393 across Windows and macOS
  runners:
  - Windows (`agent bridge (windows-latest)`, PR 393 run `44567880864`): `test_verified_run_and_privacy_scrubbing`
    line 668 failed `AssertionError: 2 != 3` on `len(submit_calls)` while `record["summary"]["verified"] == 3`
    already proved `benchmarks/campaigns/driver.py` completed all three units.
  - macOS (`agent bridge (macos-latest)`, PR 393 run `44567880864`): `test_timeout_records_failed_and_cancels_task`
    line 843 failed `KeyError: 'cancelled_on_master'`.
- **Problem:** Both causes live in `tests/test_campaign_driver.py` (`benchmarks/campaigns/driver.py` is
  right):
  1. `FAKE_CLI_SOURCE` appended JSON lines to a single shared file with `open("a")` across
     `--concurrency 2` subprocesses, which is not atomic on Windows and dropped one line cleanly while
     `driver.py` saw all three verified completions.
  2. `test_timeout_records_failed_and_cancels_task` passed `--submit-timeout-secs 0.2`, letting
     `proc.communicate(timeout=0.2)` `SIGKILL` the fake Python process on loaded CI runners before
     Python started and flushed `timeout_json` (`u001`) or `task_id=` (`u002`); without a task id in
     stdout or stderr, `execute_submit_attempt` (`benchmarks/campaigns/driver.py:1299`) legitimately
     left `cancelled_on_master` unset.
- **Do:** Replace `FAKE_OXIDESWARM_LOG` append logging with `FAKE_OXIDESWARM_LOG_DIR`, writing each
  invocation atomically via temp file + `os.replace` to `<ts_ns>-<pid>-<uuid4>.json` (and using
  atomic writes for `.workers_calls`, `.count.<seed>`, and marker files) plus a shared chronological
  reader `read_fake_cli_logs`; raise `--submit-timeout-secs` in
  `test_timeout_records_failed_and_cancels_task` from `0.2` to `5`, flush `timeout_json` stdout, keep
  `u002`'s `sleep_secs` at `30.0` (six times the `5 s` watchdog) rather than `600.0` because
  `driver.py`'s Windows `proc.kill()` ends only the `.cmd` wrapper (`oxideswarm.cmd`) and does not
  reach its child processes (leaving stdout/stderr pipes open until sleep ends; fine for the pilot,
  which runs `oxideswarm.exe` directly), raise `wait_for_file`, `proc.communicate`, and interrupt
  `sleep_secs` in `test_interrupt_records_interrupted_cancels_and_resumes` to `60.0 s` / `600.0 s`,
  and attach full attempt records to the assertions without asserting elapsed time.
- **Done when:** All 13 tests in `tests/test_campaign_driver.py` keep full behaviour and cancellation
  assertions without sub-second startup windows or shared append races, and pass `20/20` under 6 CPU
  busy loops.
- **Implemented (2026-11-19, pull request 394, open):** Updated `tests/test_campaign_driver.py` (`driver.py`
  unchanged) and verified on Apple M5 (10 cores, private `HOME`, 6 Python CPU busy loops):
  - Before fix (`origin/master` `b0786ed8`): `20/20` passed (`74.61 s` total, `3.64–3.84 s` per run;
    Apple M5 starts Python under `0.2 s` while slower CI runners hit the startup race and Windows
    non-atomic append race).
  - After fix: `python3 tests/test_campaign_driver.py -v` passed `13/13` in `8.054 s`, and the 20-run
    loop under 6 Python busy loops passed `20/20` (`173.79 s` total, `8.52–9.21 s` per run, `0`
    failures).

### TEST-79: scale1_simulated_worker: widen 5-20 s burst, reconnect, and drain deadlines to 60 s

- **Evidence:** In `agent-win-2`'s loaded Windows 11 Home flake hunt (`OxideSwarm_Agent_Hub`
  `test_runs/win2_flakehunt_2026-11-20.md`, Item 11 on `435fa58`, 12 logical threads under 6 CPU busy
  loops), `hundred_simulated_workers_execute_burst_survive_mass_disconnect_and_recover` failed at
  `crates/worker/tests/scale1_simulated_worker.rs:303` on task 41 of 200 across 4 submitters and
  100 simulated workers: `submitter ... task 41 failed/timed out` hitting the `5 s` per-task response
  timeout at `:295`.
- **Problem:** `crates/worker/tests/scale1_simulated_worker.rs` used tight deadlines (`5 s` per-task
  response timeout at `:295` during the 200-task 100-worker burst, `20 s` registration and
  reconnection deadlines at `:253` and `:331`, `15 s` post-reconnect response and worker-shutdown
  timeouts at `:374` and `:396`, `2 s` idle disconnect read timeout at `:197`, and `30 s` / `5 s`
  non-timeout fake-runner bounds at `:60`, `:79`, `:80`, `:289`, `:368`).
- **Do:** Add `TEST_WAIT_TIMEOUT = 60 s`; raise the registration, burst per-task, reconnection,
  post-reconnect per-task, `wrapped_idle.read`, and shared `tokio::time::timeout_at` worker-shutdown
  drain deadlines to `TEST_WAIT_TIMEOUT`; raise non-timeout `builtin_task(..., 60)` bounds and the
  cancellation target delay (`60 s` / `builtin_task(false, 120)`); and keep every behaviour assertion
  and the `1 s` `EXIT_CODE_TIMEOUT` check intact.
- **Done when:** All 3 tests keep every latency/FIFO/impairment, fake-runner cancellation/timeout,
  100-worker registration, 200-task burst, mass-disconnect convergence, and 50-task post-reconnect
  assertion, and pass `20/20` under 6 CPU busy loops.
- **Implemented (2026-11-19, pull request 401, open):** Updated `crates/worker/tests/scale1_simulated_worker.rs`
  and verified on Apple M5 (10 cores, private `HOME`, 6 Python CPU busy loops; `0/20` local failures
  before the fix is expected since the `5 s` flake occurred on a 12-thread Windows laptop):
  - Before fix (`origin/master` `d321d97f`): `20/20` passed (`37.29 s` total, `1.72–3.59 s` per run).
  - After fix: `3/3` passed in `1.64 s`, and the 20-run loaded loop passed `20/20` (`35.51 s` total,
    `1.70–1.85 s` per run, `0` failures).

### TEST-78: rendezvous_discovery_e2e: widen 10-30 s command and registration deadlines to 60 s

- **Evidence:** In `agent-win-2`'s loaded Windows 11 Home flake hunt (`OxideSwarm_Agent_Hub`
  `test_runs/win2_flakehunt_2026-11-20.md`, Item 10 on `435fa58`, 12 logical threads under 6 CPU busy
  loops), `test_worker_auto_rendezvous_registers_with_tiers_1_and_2_disabled` failed at
  `tests/rendezvous_discovery_e2e.rs:111` (`run_with_timeout(submit_cmd, Duration::from_secs(20))` at
  `:229`): `Command timed out after 20s`, after Tier 1 UDP + Tier 2 HTTP timeouts + rendezvous lookup
  + auth handshake + worker spawn.
- **Problem:** `tests/rendezvous_discovery_e2e.rs` used `10–30 s` timeouts for subprocess setup
  (`secret generate`, `wait_for_port_file` at `10 s` / `15 s`), worker registration polling (`30 s`),
  `workers --master auto` (`10 s`), `submit --master auto --wait` (`20 s`), and HTTPS/unsigned
  rejection commands (`10 s`), plus `1–2 s` `discover_master_rendezvous` checks, and
  `run_with_timeout` panicked without child `stdout`/`stderr` or rendezvous/master/worker service
  logs.
- **Do:** Add `TEST_WAIT_TIMEOUT = 60 s` and `run_with_timeout_with_logs` (draining child
  `stdout`/`stderr` in background threads and printing captured child output plus service logs on
  timeout without blocking on thread joins if a child leaves a pipe open); raise all
  `wait_for_port_file`, registration deadline, `run_with_timeout` / `run_with_timeout_with_logs`,
  and `discover_master_rendezvous` deadlines to `TEST_WAIT_TIMEOUT`; and include rendezvous, master,
  and worker logs on `submit_out` assertion failure.
- **Done when:** All 3 tests keep every Tier 3 rendezvous discovery, keyed isolation,
  anti-harvesting, and CLI URL rejection assertion, print service + child diagnostics on failure, and
  pass `20/20` under 6 CPU busy loops.
- **Implemented (2026-11-19, pull request 400, open):** Updated `tests/rendezvous_discovery_e2e.rs` and
  verified on Apple M5 (10 cores, private `HOME`, 6 Python CPU busy loops; `0/20` local failures
  before the fix is expected since the `20 s` timeout occurred on a 12-thread Windows laptop):
  - Before fix (`origin/master` `d321d97f`): `20/20` passed (`195.42 s` total, `9.54–11.58 s` per
    run).
  - After fix: `3/3` passed in `9.16 s`, and the 20-run loaded loop passed `20/20` (`193.82 s` total,
    `9.39–10.00 s` per run, `0` failures).

### TEST-74: m5_challenger_2_stress: wait 60 s for reduce Running on w1 and use TEST-54 MapReduce timeouts

- **Evidence:** In `agent-win-2`'s loaded Windows 11 Home flake hunt (`OxideSwarm_Agent_Hub`
  `test_runs/win2_flakehunt_2026-11-20.md`, Run 1 on `435fa58`, 12 logical threads under 6 CPU busy
  loops), `test_m5_mapreduce_worker_crash_during_reduce_phase_retried_intermediate_retained` failed at
  `tests/m5_challenger_2_stress.rs:623`: `mapreduce execution after reduce worker crash: Timeout { operation: "MapReduce task ...", duration_secs: 60 }`.
  Reproduced locally on Apple M5 (`98570ab9`, 6 CPU busy loops) in `2/20` runs (runs 11 and 12 of
  20, both at `:623:10` after `67.40 s` / `67.42 s`, with the other 18 runs splitting into `~5.1 s` vs
  `~7.6 s`).
- **Problem:** In `test_m5_mapreduce_worker_crash_during_reduce_phase_retried_intermediate_retained`,
  `tasks.iter().find(|t| t.assigned_worker_id == Some(w1.worker_id))` returned `w1`'s first task in
  `HashMap` iteration order (`TaskQueue::list_tasks`): when hash order put `w1`'s already-`Completed`
  map task first instead of a `Running` reduce task, the loop spun for its full `6 s` deadline before
  falling back to `w1.abort()`. The `60 s` failure itself is the engine's idle timeout; the old test
  printed no diagnostics, so which task stalled is unknown (the two consecutive `~67.4 s` local runs
  point to a load burst and 85 % host-CPU backpressure), and the post-job assertion only checked
  `retry_count == 0` on `>= 2` tasks without verifying `tasks.len() == 7` or reduce failover on the
  surviving worker. Sibling `test_m5_mapreduce_worker_crash_during_map_phase_rescheduled` also used
  a `4 s` fallback loop, `wait_for_workers` used `5 s`, `MapReduceJobSpec` used `30–60 s` without an
  outer `WAIT`, and `spawn_test_worker` omitted `.with_ram_mb(4096)` / `.with_no_gpu` /
  `.with_no_blender(true)` / `.with_no_cuda(true)` / `.with_redirection_portal(false)` (`TEST-20`).
- **Do:** Poll up to `TEST_WAIT_TIMEOUT = 60 s` for a `TaskState::Running` task on either `w1` or
  `w2` (plus `completed_count >= 2` in the reduce-crash test), crash whichever worker holds that
  `Running` task, and panic with `task_summary` diagnostics instead of falling back after `4–6 s`
  (this repairs crash injection separately from the `60 s` stall); protect against the `60 s` idle
  stall with the `TEST-54` MapReduce timeout pairing (`MR_WAIT = 180 s` outer `tokio::time::timeout`
  dumping `queue_stats` + `task_summary`, and `MR_TASK_TIMEOUT_SECS = 540` strictly above `MR_WAIT`
  across all specs); assert `tasks.len() == 7` (2 map + 5 reduce, proving no map recomputation) and
  at least one `Completed` task with `failover_count >= 1` assigned to the surviving worker (not the
  aborted worker); widen all `wait_for_workers` calls (`5 s` -> `60 s`); and apply the `TEST-20`
  worker config flags in `spawn_test_worker`.
- **Done when:** All 11 tests keep every MapReduce, intermediate shuffle retention, reduce failover,
  and `retry_count == 0` map-phase invariant, and pass `20/20` under 6 CPU busy loops.
- **Implemented (2026-11-19, pull request 398, open):** Updated `tests/m5_challenger_2_stress.rs` and
  verified on Apple M5 (10 cores, private `HOME`, 6 Python CPU busy loops):
  - Before fix (`origin/master` `98570ab9`): `18/20` passed, **`2/20` failed** at `:623:10`
    (`252.31 s` total, `4.97–67.79 s` per run).
  - After fix: `11/11` passed in `3.51 s`, and the 20-run loaded loop passed `20/20` (`74.45 s` total,
    `3.61–3.86 s` per run, `0` failures).

### TEST-77: e2e_cluster: widen 3-5 s worker and wait_task deadlines to 60 s

- **Evidence:** In `agent-win-2`'s loaded Windows 11 Home flake hunt (`OxideSwarm_Agent_Hub`
  `test_runs/win2_flakehunt_2026-11-20.md`, Run 1 on `435fa58`, 12 logical threads under 6 CPU busy
  loops), `test_tier2_bva_max_concurrency_limit_throttles_worker` failed at `tests/e2e_cluster.rs:1142`:
  `called Result::unwrap() on an Err value: Timeout { operation: "wait_task(...)", duration_secs: 5 }`.
- **Problem:** Across `tests/e2e_cluster.rs`, `wait_for_workers`, `poll_until`, `submit_and_wait`,
  and dozens of direct `wait_task`, `Instant::now() + 2–3 s`, and `tokio::time::timeout(3 s)` calls
  used `2–15 s` deadlines (paired with `5–15 s` task timeouts), and `TestCluster::spawn_worker` plus
  `test_tier2_bva_max_concurrency_limit_throttles_worker` omitted `TEST-20` no-Blender/no-CUDA/no-portal
  flags.
- **Do:** Add `TEST_WAIT_TIMEOUT = 60 s`, floor `wait_for_workers`, `submit_and_wait`, and
  `poll_until` at `TEST_WAIT_TIMEOUT`, raise all direct `wait_task` (`5–15 s` -> `60 s`),
  registration (`3–30 s` -> `60 s`), cancellation/routing deadlines (`2–3 s` -> `60 s`), `sleep 2`/`10`
  cancellation targets (`sleep 120`, `timeout_secs = 180`), MapReduce specs (`20–30 s` -> `180 s`), and
  non-timeout `TaskRequirements` (`10–30 s` -> `60 s`), and apply `TEST-20` worker config flags in
  `spawn_worker` and `test_tier2_bva_max_concurrency_limit_throttles_worker` (keeping every routing,
  serial `>= 250 ms`, negative `Queued`, and `1 s` timeout test assertion intact).
- **Done when:** All 65 tests keep full assertions (including `elapsed >= 250 ms` serialization and the
  `1 s` timeout test) and pass `20/20` under 6 CPU busy loops.
- **Implemented (2026-11-19, pull request 399, open):** Updated `tests/e2e_cluster.rs` and verified on
  Apple M5 (10 cores, private `HOME`, 6 Python CPU busy loops; `0/20` local failures before the fix is
  expected since the `5 s` timeout occurred on a 12-thread Windows laptop):
  - Before fix (`origin/master` `98570ab9`): `20/20` passed (`162.16 s` total, `7.52–9.50 s` per run).
  - After fix: `65/65` passed in `2.50 s`, and the 20-run loaded loop passed `20/20` (`63.93 s` total,
    `2.99–3.38 s` per run, `0` failures).

### TEST-73: m4_scheduling: prove batch concurrency with a barrier and drop 1.2 s / 1.5 s wall-clock windows

- **Evidence:** In `agent-win-2`'s loaded Windows 11 Home flake hunt (`OxideSwarm_Agent_Hub`
  `test_runs/win2_flakehunt_2026-11-20.md`, Run 1 on `435fa58`, 12 logical threads under 6 CPU busy
  loops), `test_m4_parallel_batch_spread_scheduling` failed at `tests/m4_scheduling.rs:433`:
  `Batch of 5 tasks should execute in parallel across 3 workers in < 1.2s, took 1.242767s`. Sibling
  `test_m4_in_flight_task_cancellation` also enforced `cancel_duration < 1500 ms` (`:630`), and all
  five tests in `tests/m4_scheduling.rs` used fixed `3–8 s` waits (`5 s` / `12 s` registration,
  `3–6 s` waits / disconnects, `8 s` failover wait against `sleep 0.8`, `3–4 s` cancel/quick waits).
- **Problem:** Wall-clock upper bounds (`1.2 s` / `1.5 s`) and tight `3–8 s` waits flake when a
  loaded test runner schedules child shells slower than nominal desktop latency.
- **Do:** Replace `wait_for` with `TEST_WAIT_TIMEOUT = 60 s` helpers (`wait_for_workers` and
  `wait_for_task_running` with observed worker/status diagnostics); in
  `test_m4_parallel_batch_spread_scheduling`, replace `sleep 0.2` and the `< 800 ms` / `< 1200 ms`
  wall-clock assertion with a cross-platform `sh -c` concurrency barrier in `tempfile::tempdir()`
  (`started_<i>` markers + host-released `release` gate after observing `>= 3` concurrent tasks
  within `60 s`, printing both time-to-3 and total batch time) while keeping all 5 exit-0 and 3-worker
  spread assertions; in `test_m4_worker_disconnect_during_execution_triggers_retry`, gate the first
  attempt on `first_started` / `first_aborted` markers instead of `sleep 0.8`; and in
  `test_m4_in_flight_task_cancellation`, sleep `120 s` (`timeout_secs = 180`), drop the `< 1.5 s`
  wall-clock cap while printing `cancel_duration`, and widen all remaining `wait_task` / registration /
  task timeouts to `60 s` (keeping the `400 ms` `Queued` negative check untouched).
- **Done when:** All 5 tests keep every routing, spread, retry, and cancellation assertion with no
  wall-clock window, compile cleanly, and pass `20/20` under 6 CPU busy loops.
- **Implemented (2026-11-19, pull request 397, open):** Updated `tests/m4_scheduling.rs` and verified on
  Apple M5 (10 cores, private `HOME`, 6 Python CPU busy loops; `0/20` failures before the fix on macOS
  is expected because the `< 1.2 s` flake hit a 12-thread Windows laptop):
  - Before fix (`origin/master` `98570ab9`): `20/20` passed (`53.70 s` total, `2.56–3.88 s` per run).
  - After fix: `5/5` passed in `1.39 s` (`m4 parallel batch: 5 tasks running concurrently after 21.459708ms, total batch 81.544416ms`),
    and the 20-run loaded loop under 6 busy loops passed `20/20` (`34.18 s` total, `1.60–1.81 s` per
    run, `0` failures).

### TEST-68: gpu_wgpu_compute_test: replace short 3-8 s waits with 60 s condition deadlines

- **Evidence:** During CI on pull request 387 (`workspace tests (windows-latest)`, run `44597494223`,
  `cargo test --workspace --locked --no-fail-fast`), `test_adversarial_scheduling_isolation_mixed_cluster`
  panicked at `tests/gpu_wgpu_compute_test.rs:1218:10`:
  `quick wait: Timeout { operation: "wait_task(...)", duration_secs: 3 }`.
- **Problem:** `tests/gpu_wgpu_compute_test.rs` used fixed `3–8 s` condition-wait and `wait_task`
  timeouts (`5 s` worker registration and `5 s` `wait_task` with `15 s` task timeout in Scenario 7
  `test_e2e_cluster_gpu_task_distribution`; `5 s` registration, `8 s` × 12 `wait_task` calls with
  `10 s`/`15 s` task timeouts, `3 s` Worker 3 `Disconnected` wait, and `3 s` `quick_cpu` `wait_task`
  with `5 s` task timeout in Scenario 11 `test_adversarial_scheduling_isolation_mixed_cluster`), causing
  false timeouts on loaded CI runners.
- **Do:** Define `TEST_WAIT_TIMEOUT = 60 s` and shared helper `wait_for_workers` polling
  `master.list_workers()` until the predicate passes or dumping observed `(worker_id, status)` pairs on
  timeout; raise the worker registration (`5 s` -> `60 s`), Worker 3 disconnect (`3 s` -> `60 s`),
  `wait_task` (`3–8 s` -> `60 s`), and `TaskRequirements` task execution timeouts (`5–15 s` -> `60 s`)
  with descriptive failure panics naming the awaited condition and task/worker IDs; keep every strict
  capability routing assertion and the `500 ms` negative check before asserting `stranded_id` stays
  `Queued`, and strengthen the negative check by re-checking `master.get_task_status(stranded_id)` is
  still `Queued` after `quick_cpu` completes on a free CPU worker.
- **Done when:** All 14 tests in `tests/gpu_wgpu_compute_test.rs` keep full routing and negative checks,
  compile with and without `--features gpu-wgpu`, and pass `20/20` under 6 CPU busy loops.
- **Implemented (2026-11-19, pull request 396, open):** Updated `tests/gpu_wgpu_compute_test.rs` and verified on
  Apple M5 (10 cores, private `HOME`, 6 Python CPU busy loops):
  - Before fix (`origin/master` `98570ab9`): `20/20` passed (`97.83 s` total, `4.71–6.37 s` per run).
  - After fix: `cargo test -p rusty_grid_worker --test gpu_wgpu_compute_test --features gpu-wgpu --no-run --locked`
    compiled cleanly, default `14/14` passed in `4.41 s`, and the 20-run loop under 6 Python busy loops
    passed `20/20` (`95.01 s` total, `4.69–4.91 s` per run, `0` failures).

### TEST-80: test_sec5_hardened_runner: drop 5 s wall-clock upper bounds in oversized sparse output test

- **Evidence:** In CI run `44660865104` on pull request 391 (`sec5 hardened runner (macos-latest)`,
  `cargo test -p rusty_grid_worker --test test_sec5_hardened_runner --features 3d --locked`),
  `test_bug10_3d_and_blender_refuse_oversized_sparse_output_without_reading` failed at
  `crates/worker/tests/test_sec5_hardened_runner.rs:1388`:
  `Oversized sparse file must fail without reading into memory, took 8.590504708s`.
- **Problem:** The test asserted `elapsed < Duration::from_secs(5)` for both the Generative3D subprocess
  run (`:1389`) and `BlenderRigProvider::execute_rigging` (`:1426`) as a proxy for "refused without
  reading into memory", even though wall-clock elapsed includes Python startup and sandbox setup on a
  loaded runner, and used a `15 s` rig timeout (`:1420`). Meanwhile, the structural proof was already
  in place: `capture.rs:359-369` checks `meta.len()` before allocating or reading and formats
  `"is {size} bytes"`, whereas the capped-read fallback formats `"is more than {max_bytes} bytes"`, so
  asserting `"is {sparse_bytes} bytes"`, `!"is more than"`, and `res.asset_bytes.is_none()` already
  proves the file was never read.
- **Do:** Drop both `elapsed < Duration::from_secs(5)` assertions (printing elapsed durations with
  `eprintln!` instead), raise the `BlenderRigProvider::execute_rigging` timeout argument from `15` to
  `60`, and keep all exit-code, error-message, and `asset_bytes.is_none()` assertions.
- **Done when:** All 28 tests in `test_sec5_hardened_runner` (`--features 3d`) keep every structural
  metadata-refusal assertion without wall-clock upper bounds on oversized sparse output, and pass `20/20`
  under 6 CPU busy loops.
- **Implemented (2026-11-20, pull request 402, open):** Updated `crates/worker/tests/test_sec5_hardened_runner.rs`
  and verified on Apple M5 (10 cores, private `HOME`, 6 Python CPU busy loops, `--features 3d`; `0/20`
  local failures before is expected since the `8.59 s` wall-clock failure occurred on a shared
  `macos-latest` GitHub runner):
  - Before fix (`origin/master` `347e2a90`): `20/20` passed (`12.88 s` total, `0.54–1.91 s` per run).
  - After fix: full binary `28/28` passed in `2.83 s`, and the 20-run loaded loop of the test passed
    `20/20` (`11.47 s` total, `0.54–0.61 s` per run, `0` failures).

### TEST-69: Windows AppContainer isolation tests wait too briefly under load

- **Evidence:** During the 5-run Windows flake hunt (`435fa58` and `b0786ed`, `cargo test --workspace --locked --no-fail-fast` under 6 Python CPU busy loops with `RUSTY_GRID_REQUIRE_APPCONTAINER=1`), `test_attack_junctions_and_hardlinks_inside_sandbox_cannot_expose_denied_secret` in `crates/worker/tests/test_feat3_os_isolation.rs` failed `5/5` runs on `task2 must signal ready.txt`.
- **Problem:** Five wait paths inside `mod windows` of `crates/worker/tests/test_feat3_os_isolation.rs` relied on short fixed sleeps or tight 5–30 s budgets under load:
  1. `test_attack_junctions_and_hardlinks_inside_sandbox_cannot_expose_denied_secret` polled `ready.txt` and `go.txt` for `200 * 25 ms` (`5 s`) while passing `60` to `win_task`, which capped task timeout down to `35 s` (`win_task` lines 3102–3106).
  2. `test_concurrent_tasks_under_fs_cannot_read_or_list_each_others_sandboxes` polled `go.txt` / `exit.txt` and `secret.txt` / `done.txt` for `300 * 50 ms` (`15 s`) with the same `60 -> 35 s` `win_task` cap.
  3. `wait_for_pid` (`10 s`), cancel `timeout` (`20 s`), timeout elapsed check (`30 s`), and `test_self_check_accepts_both_levels_on_windows` (`20 s` per isolation level) used tight bounds.
  4. `test_named_pipes_and_kernel_events_are_isolated` slept a fixed `800 ms` before probing the `30 s` host pipe/event PowerShell process and killed it only at test end.
  5. `test_per_attempt_appcontainer_profile_is_unique_and_cleaned_up_and_sweep_is_scoped` polled `!pkg_dir.exists()` for `20 * 25 ms` (`500 ms`).
- **Do:** Introduce `WIN_WAIT_TIMEOUT = 60 s`, `ChildKillGuard`, and `wait_for_barrier` (with `AtomicBool` task-completion early-exit detection and `format_result_diag` diagnostics) in `mod windows`; pass `90` (`90 s`) instead of `60` to `win_task` in the junction/hard-link and concurrent-sibling tests, move their PowerShell loops to `60 s` UTC deadlines (`tokio::join!` + `spawn_blocking` for host junction creation), and emit/assert `GO_SEEN` in the sibling-isolation test so tasks never pass by probing before `go.txt` is written; widen `wait_for_pid`, cancel, timeout elapsed (logging elapsed time), and `verify_isolation_support` checks to `WIN_WAIT_TIMEOUT`; wait for host pipe `"READY"` over piped stdout with a `180 s` host sleep and `ChildKillGuard`; and poll AppContainer profile directory removal up to a `30 s` `Instant` deadline. Keep all security, SDDL, AppContainer, and control assertions intact.
- **Done when:** All `mod windows` AppContainer tests pass `20/20` sequentially (`-- --test-threads=1`) under 4 CPU busy loops with `RUSTY_GRID_REQUIRE_APPCONTAINER=1`, `cargo fmt --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `ox-mode test size`, and `ox-mode doc verify` pass, and confirmation in the failing configuration (`test_feat3_os_isolation` with default parallel test threads under `6` busy loops and `RUSTY_GRID_REQUIRE_APPCONTAINER=1`, `5` runs) passes on `agent-win`.
- **Partial (2026-11-20, pull request 403, open):** Updated `crates/worker/tests/test_feat3_os_isolation.rs` (`mod windows` and `#[cfg(..., windows)]` on `format_result_diag`) and verified on Windows (`12` threads, `16 GB` RAM, `RUSTY_GRID_REQUIRE_APPCONTAINER=1`, private `HOME`/`USERPROFILE`/`TEMP`/`TMP`):
  - Pre-shutdown loaded loops (`origin/master` `98570ab`, `-j 4 -- --test-threads=1` under `4` Python CPU busy loops, before the 2026-11-19 ~22:38 UTC shutdown): before fix `20/20` passed (`31` passed per run; `150.03–169.37 s`, median `154.22 s`, total `3,106.57 s`), compared to `5/5` failures of `test_attack_junctions_and_hardlinks_inside_sandbox_cannot_expose_denied_secret` during full loaded workspace runs under `6` busy loops; after fix `20/20` passed (`31` passed per run; `148.63–157.44 s`, median `151.07 s`, total `3,029.12 s`), and `cargo clippy --workspace --all-targets --locked -j 4 -- -D warnings` passed.
  - Post-restart verification: after rebasing onto `origin/master` (`8513b79`), rebuilt once under the memory rules (`CARGO_BUILD_JOBS=2`, `CARGO_INCREMENTAL=0`, `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`) and verified `1/1` (`31 passed, 0 failed` in `211.33 s`) with `RUSTY_GRID_REQUIRE_APPCONTAINER=1 --test-threads=1`; after merging `origin/master` (`fd0df6a`) and adding `GO_SEEN`, `cargo fmt --all -- --check`, `ox-mode test size`, and `ox-mode doc verify` passed locally while `windows-latest` CI verifies `GO_SEEN` (local `1/1` post-`GO_SEEN` check to be added as a PR comment once free RAM `>= 6 GiB`).
  - Why `Partial`: the failing configuration (full loaded workspace under `6` busy loops) crashed this laptop twice (`0x1a MEMORY_MANAGEMENT` and an OOM shutdown), so it was not run here; the fix was looped only as this binary alone (`--test-threads=1`, `4` busy loops), where the old code also passed `20/20`. Confirmation in the failing configuration runs on the case PC (`agent-win`) after its 24-hour soak, and `TEST-69` becomes `Implemented` in that follow-up.

### TEST-81: Test masters keep 85 % host-CPU backpressure, stalling queued tasks under load

- **Evidence:** In Q80 confirmation Run 2 on `origin/master` `347e2a90` (10-core Apple M5 under 6 Python
  CPU busy loops alongside the workspace test run), `-p rusty_grid_cli --test blob_transfer_e2e`
  failed in `cli::test_cli_submit_input_and_output_round_trip_with_dedup` (`120 s` silent task timeout
  after 2 earlier tasks completed) and `test_worker_pins_output_files_from_collection_through_upload`
  (`60 s` `Elapsed(())`). Reproducing with `RUST_LOG=rusty_grid_master::scheduler=debug,info` on
  `origin/master` `fd0df6a0` under 6 busy loops plus `cargo test -p rusty_grid_cli` completed submits 1
  and 2 in `16–18 ms` and stalled submit 3 for `5 s`, emitting `51` `DEBUG` lines verbatim:
  `2026-11-20T05:41:27.557055Z DEBUG rusty_grid_master::scheduler: Scheduling pass skipped queued tasks while worker(s) are held back by host CPU backpressure skipped=1 max_host_cpu_pct=85.0 busy_workers=[(73f372f4-c5e6-425e-84f1-ce1b419fb050, 99.60392, 0)]`.
- **Problem:** `SchedulerConfig::max_host_cpu_pct` defaults to `85.0` so real masters protect the host,
  and most integration/E2E tests started masters without raising it (even `WorkerClient::for_testing`
  and `WorkerClient::simulated` report live `sysinfo` CPU usage). Under loaded CI runners or local
  stress loops, host CPU exceeds `85 %` and unrelated task-dispatch tests stall with no indication of
  why tasks remain `Queued`. This `85 %` host-CPU backpressure is probably behind several Windows-hunt
  flakes (`TEST-69` to `TEST-79`) and the `TEST-68` echo stall as well.
  Open product question for later (now tracked in `SCHED-5` in `docs/BACKLOG.md`): when every worker
  sits above `85 %` host CPU, queued tasks wait silently without any hint to the user.
- **Do:** Confirm host-CPU backpressure as the cause from a debug master log, add
  `SchedulerConfig::with_max_host_cpu_pct` and `SchedulerConfig::without_host_cpu_backpressure`
  (leaving user-facing backpressure reporting to `SCHED-5`), opt non-backpressure test masters out
  (`without_host_cpu_backpressure()` / `--max-host-cpu-pct 100`) across the suite, and keep
  production default `85.0` and all CPU-backpressure / config-default tests untouched.
- **Done when:** Non-backpressure test masters opt out of `85 %` CPU backpressure, host-CPU
  backpressure and config tests remain at `85.0 %` and pass, `blob_transfer_e2e` passes `20/20` under
  6 busy loops, and `cargo test --workspace --locked` passes cleanly.
- **Implemented (2026-11-20, pull request 404, open):** Added `SchedulerConfig::with_max_host_cpu_pct`,
  `SchedulerConfig::without_host_cpu_backpressure`, and unit test
  `test_with_max_host_cpu_pct_100_allows_saturated_test_host` in `crates/master/src/scheduler.rs`
  (removing the temporary per-pass `DEBUG` log per PR 404 review and moving user-facing reporting to
  `SCHED-5`); opted `57` non-backpressure test files out (`without_host_cpu_backpressure()` /
  `--max-host-cpu-pct 100`) alongside the `3` already opted-out files
  (`tests/m4_challenger_2_stress.rs`, `tests/m6_challenger_1_stress.rs`,
  `crates/cli/tests/test_mcp_concurrent_and_deadlines.rs`), while keeping production default `85.0` and
  all backpressure/config tests (`scheduler::tests`,
  `e2e_cluster::test_tier3_pairwise_dynamic_load_backpressure_high_cpu`, `tests/m6_challenger_2_stress.rs`,
  `tests/m5_challenger_1_stress.rs`, `crates/cli/src/cli_app/{tests,runtime_tests}.rs`) unchanged:
  - `blob_transfer_e2e` loaded 20-run loop (Apple M5, 6 busy loops, private `HOME`): `20/20` before
    (`576.66 s`, `26.18–36.63 s`; Q80 Run 2 reproduced the stall under workspace load) and `20/20` after
    (`627.62 s`, `29.01–33.98 s`).
  - Targeted backpressure checks: `scheduler::tests` (`82 passed`),
    `e2e_cluster::test_tier3_pairwise_dynamic_load_backpressure_high_cpu` (`1 passed`),
    `m6_challenger_2_stress` (`11 passed`), `m5_challenger_1_stress` (`32 passed`).
  - Full gates: `cargo test ... --features 3d --no-run --locked`, `cargo clippy --workspace --all-targets
    --locked -- -D warnings`, and `cargo test --workspace --locked` (`137` binaries, `2,228 passed;
    0 failed; 5 ignored`).

### TEST-82: `rusty_grid_worker`: replace wall-clock upper bounds and bound `scale1` status queries

- **Evidence:** Read-only audit `OxideSwarm_Agent_Hub` `test_runs/mac_wallclock_audit_2026-11-20.md`
  (coordinator follow-up `TEST-82`) identified `31` Category 1 wall-clock upper bounds in
  `rusty_grid_worker` and `3` Category 3 unbounded `recv_msg` awaits inside `TEST_WAIT_TIMEOUT`
  deadline loops in `crates/worker/tests/scale1_simulated_worker.rs`.
- **Problem:** Under overloaded CI or local stress loops, wall-clock upper bounds (`< 40 ms`, `< 400 ms`,
  `< 500 ms`, `< 1–30 s`) fail when an OS scheduler slice delays a task or `IoClock` read, while unbounded
  `query.recv_msg::<ClientResponse>().await` calls in `scale1_simulated_worker.rs` can hang past
  `TEST_WAIT_TIMEOUT` if the master stalls a status reply.
- **Do:** Replace proof proxies with deterministic structural or ordering checks and print elapsed times,
  widen hang guards to `60 s` (raising inner task/lock timeouts above `60 s`), leave release-only
  `!cfg!(debug_assertions)` SLAs untouched, skip `tests/worker_connection_lifecycle.rs` (`TEST-76`) and
  `crates/worker/tests/test_feat3_os_isolation.rs` `mod windows` (`TEST-69`), and bound every status
  query in `scale1_simulated_worker.rs` by the remaining deadline budget:
  1. `tests/gpu_wgpu_compute_test.rs:1000` (with `:981`, `gpu_class_compute_does_not_block_the_async_runtime`):
     replaced the 600-tick loop and `worst_gap < 400 ms` assertion with a `compute_done` `AtomicBool` and a
     `current_thread` ticker counter (`yield_now`/`sleep`) asserting `ticks_during_compute >= 1` inside
     `TEST_WAIT_TIMEOUT` (`60 s`), logging compute wall time, ticks, and worst gap.
  2. `tests/gpu_wgpu_compute_test.rs:1021, :1029` (`gpu_class_timeout_fires_and_stops_the_multiplication`):
     added a deterministic `cpu_simd::matmul_simd_cancellable` row-stop proof (`!completed`, `polls == 3 < 64`),
     added `runner::tests::test_spawn_simulated_gpu_job_stops_when_stop_flag_raised` in `crates/worker/src/runner/tests.rs`
     (verifying `StopOnDrop` -> `spawn_simulated_gpu_job` returns `Err(GpuError::Interrupted(_))` while an unstopped control returns `Ok`),
     widened `execute_task` (`< 10 s`) and `drop(runtime)` (`< 2500 ms`) guards to `TEST_WAIT_TIMEOUT`
     (`60 s`), and logged elapsed times.
  3. `tests/gpu_wgpu_compute_test.rs:1049` (`gpu_class_cancel_is_honoured_promptly`):
     added the `"user requested cancel"` error assertion, logged elapsed time, and widened `< 10 s` to `TEST_WAIT_TIMEOUT` (`60 s`).
  4. `crates/worker/src/heartbeat.rs:760, :766, :787, :803` (`tests::test_io_clock_counts_received_bytes_and_bulk_write_progress_only`,
     `tests::test_stamped_stream_stamps_reads_and_only_bulk_writes`):
     added test-only `IoClock::with_fake_clock(Arc<AtomicU64>)` and asserted exact `Duration::ZERO` /
     `60 ms` silence transitions (keeping a lower-bound `IoClock::new()` check) instead of `< 40 ms` real-clock windows.
  5. `crates/worker/src/runner/process_tree_tests.rs:189, :274` (`test_a_background_process_holding_the_pipes_does_not_turn_exit_7_into_a_timeout`,
     `test_a_helper_that_escaped_the_process_group_cannot_hold_the_run_hostage`):
     raised task timeouts `20` -> `90`, logged elapsed times, and widened `< 10 s` hang guards to `< 60 s`.
  6. `crates/worker/src/runner/workspace_runner_tests.rs:293` (`test_workspace_task_error_cases_and_over_limit_warning`):
     set `.with_workspace_lock_wait(Duration::from_secs(90))` and task timeout `90 s`, asserted
     `error == Some("Cancelled: cancelled during lock wait")` and `!stdout.contains("should-not-run")`,
     logged `wait_elapsed`, and widened `< 1 s` to `< 60 s`.
  7. `crates/worker/src/wgpu_engine.rs:1055, :1073` (`tests::waiting_for_the_gpu_stops_on_cancel_and_on_deadline`):
     widened both `< 5 s` hang guards (`never_sent` still alive) to `< 60 s`, logging elapsed times.
  8. `tests/m3_challenger_1_stress.rs:407, :463` (`test_pipe_buffer_deadlock_resistance_simultaneous_large_output`):
     added `TEST_WAIT_TIMEOUT = 60 s`, raised task timeouts `15` -> `90`, logged elapsed times, and
     widened `< 10 s` guards to `< TEST_WAIT_TIMEOUT`.
  9. `tests/m3_challenger_1_stress.rs:545` (`test_infinite_stream_resistance_yes_command_watchdog`):
     logged elapsed time, kept `>= 990 ms` and PID reap checks, and widened `< 3000 ms` to `< TEST_WAIT_TIMEOUT`.
  10. `tests/m3_challenger_1_stress.rs:583, :614` (`test_timeout_watchdog_precision_and_os_reaping`):
      changed `sleep 30` -> `sleep 120`, logged elapsed times, kept `>= 990 ms` / `>= 1980 ms` and PID reap
      checks, and widened `<= 2500 ms` / `<= 3500 ms` guards to `< TEST_WAIT_TIMEOUT`.
  11. `tests/m3_challenger_1_stress.rs:954` (with `:920`, `test_cancellation_of_queued_task_behind_saturated_worker`):
      replaced `< 500 ms` (and inner `5 s` loop) with a `release_task1` file barrier (`ReleaseOnDrop` guard)
      proving `task2` receives `EXIT_CODE_CANCELLED` (`130`) *before* `task1` is released to exit (`0`),
      using `TEST_WAIT_TIMEOUT` (`60 s`) `timeout_at` deadlines and printing cancellation time.
  12. `tests/m3_execution.rs:279` (`tests::test_timeout_watchdog_terminates_runaway`):
      changed `sleep 60` -> `sleep 120`, logged elapsed time, kept `>= 1 s` and timeout-message checks, and
      widened `< 5 s` to `< 60 s`.
  13. `crates/worker/tests/test_feat3_os_isolation.rs:1214, :2939` (`macos` and `linux` `test_tracked_pid_is_the_task_and_cancel_and_timeout_still_kill_it`):
      widened `< 30 s` timeout checks and `20 s` cancel guards to `60 s` and logged elapsed times (Linux branch verified by CI `ubuntu-latest`; `mod windows` skipped per `TEST-69`).
  14. `crates/worker/tests/test_sec5_hardened_runner.rs:292, :1239` (`test_sec5_blender_large_stderr_does_not_block`,
      `test_sec5_fifo_or_non_regular_output_fails_for_both_engines`):
      raised Blender rig timeout `10` -> `60` and removed `< 8 s`; raised 3D FIFO task timeout `5` -> `60`,
      wrapped `execute_task` in a `90 s` `tokio::time::timeout` hang guard, and removed `< 4 s` while keeping
      `"Failed to read generated .glb asset"` and `asset_bytes.is_none()` and logging elapsed times.
  15. `crates/worker/tests/scale1_simulated_worker.rs:268, :313, :343` (`hundred_simulated_workers_execute_burst_survive_mass_disconnect_and_recover`):
      wrapped all three `ClusterStatus` `send_msg`/`recv_msg` awaits (registration, submitter-failure diagnostic,
      and reconnect convergence) in `tokio::time::timeout(remaining, ...)` against their deadlines.
  16. **Untouched release-only `!cfg!(debug_assertions)` SLAs (never run in CI):**
      `crates/worker/tests/test_autorig_adversarial_stress.rs:1157` (`elapsed_ms < 100.0`),
      `crates/worker/tests/test_gen3d_adversarial_stress.rs:173, :203` (`avg_mc_ms < 50.0`, `avg_geom_ms < 50.0`),
      `crates/worker/tests/test_gen3d_pipeline.rs:100` (`elapsed < Duration::from_millis(50)`).
  17. **Skipped per coordinator split:** `tests/worker_connection_lifecycle.rs:351, :515, :780` (`TEST-76`).
- **Done when:** All `24` fixed Cat 1 bounds and `3` bounded Cat 3 awaits keep structural/outcome assertions,
  `cargo fmt --all -- --check` and clippy pass with `-D warnings`, and every touched `rusty_grid_worker`
  binary passes `20/20` under 6 CPU busy loops.
- **Implemented (2026-11-20, pull request 405, open):** Applied all `TEST-82` changes across the 10 worker files and
  verified on Apple M5 (`10` cores, private `HOME`, `--locked`, 6 Python CPU busy loops):
  - Cheap before-loops (`origin/master` `f485533e`; skipped `scale1_simulated_worker` [looped in `TEST-79`] and `test_feat3_os_isolation`):
    - `gpu_wgpu_compute_test` (`gpu_class_` 3 tests): `20/20` (`96.21 s` total, `4.60–7.97 s`)
    - `rusty_grid_worker --lib` (5 targeted tests): `20/20` (`29.43 s` total, `1.09–5.53 s`)
    - `m3_execution` (`test_timeout_watchdog_terminates_runaway`): `20/20` (`34.03 s` total, `1.28–6.06 s`)
    - `m3_challenger_1_stress` (`test_cancellation_of_queued_task_behind_saturated_worker`): `20/20` (`45.64 s` total, `1.84–6.21 s`)
    - `test_sec5_hardened_runner --features 3d` (2 targeted tests): `20/20` (`20.55 s` total, `0.68–4.05 s`)
  - Gates: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`,
    `cargo clippy -p rusty_grid_worker --all-targets --features 3d,gpu-wgpu --locked -- -D warnings`, and
    `python3 tests/test_m1_challenger_hygiene.py`.
  - After-loops (full touched binaries, `20/20` each under 6 busy loops; review follow-up re-run `313 passed` single `--lib` plus `20/20` each on `test_spawn_simulated_gpu_job_stops_when_stop_flag_raised` [`5.07 s`], `macos::test_tracked_pid_is_the_task_and_cancel_and_timeout_still_kill_it` [`50.05 s`], and `test_sec5_fifo_or_non_regular_output_fails_for_both_engines` [`10.62 s`]):
    - `rusty_grid_worker --lib --locked` (`312 passed` pre-review; `313 passed` post-review): `20/20` (`208.35 s` total, `9.63–10.80 s`)
    - `rusty_grid_worker --lib --features gpu-wgpu --locked -- wgpu_engine::tests::waiting_for_the_gpu_stops_on_cancel_and_on_deadline` (`1 passed`): `20/20` (`132.23 s` total, `5.89–11.71 s`)
    - `gpu_wgpu_compute_test --locked` (`14 passed`; single `--features gpu-wgpu` run `19 passed` in `6.90 s`): `20/20` (`73.06 s` total, `3.17–9.46 s`)
    - `m3_challenger_1_stress --locked` (`16 passed`): `20/20` (`81.48 s` total, `3.64–10.37 s`)
    - `m3_execution --locked` (`7 passed`): `20/20` (`37.41 s` total, `1.50–7.66 s`)
    - `test_sec5_hardened_runner --features 3d --locked` (`28 passed`): `20/20` (`83.75 s` total, `3.67–11.26 s`)
    - `scale1_simulated_worker --locked` (`3 passed`): `20/20` (`43.57 s` total, `1.89–6.49 s`)
    - `test_feat3_os_isolation --locked` (`28 passed; 1 ignored`): `20/20` (`585.75 s` total, `27.90–33.59 s`)

### TEST-83: Replace wall-clock upper bounds in master and core tests with structural proofs and 60 s hang guards

- **Evidence:** Read-only wall-clock audit on `origin/master` (`2026-11-20`) found `36` Category 1
  wall-clock upper-bound and SLA assertions in `rusty_grid_core` and `rusty_grid_master` (excluding
  `m2_challenger_1_stress` reserved for `TEST-70`, `m4_challenger_1_stress` reserved for `TEST-71`, and
  `master_restart_recovery` reserved for `TEST-75`), plus `4` adjacent Category 1 upper bounds in the
  same modified functions (`40` upper bounds updated across `18` files / `15` binaries). Zero
  `!cfg!(debug_assertions)` release-only SLAs exist in `crates/core`, `crates/master`, or `tests/`.
- **Problem:** Under loaded CI runners or concurrent suites, millisecond/single-digit-second proof-proxy
  ceilings (`< 50 ms`, `< 200 ms`, `< 500 ms`, `< 1500 ms`, `< 1–6 s`) and throughput floors (`> 1000`,
  `> 5000 msgs/s`) flake on CPU scheduling jitter even when the underlying non-blocking, fast-path,
  complexity, or flow-control behavior is working properly.
- **Do:** Replace proof proxies with structural or ordering checks and print elapsed times, remove debug
  throughput floors, widen hang guards to `60 s` (and inner sleep targets past `60 s` to `120 s`), skip
  `m2_challenger_1_stress` (`TEST-70`), `m4_challenger_1_stress` (`TEST-71`), and `master_restart_recovery`
  (`TEST-75`), and verify every touched binary `20/20` under 6 CPU busy loops.
- **Done when:** Every audited Category 1 bound in `rusty_grid_core` and `rusty_grid_master` (outside the
  three skipped files) is structural or guarded at `60 s`, `cargo fmt`, workspace `clippy`, and doc
  hygiene pass, and all `15` binaries pass `20/20` under 6 CPU busy loops.
- **Implemented (2026-11-20, pull request 406, open):** Updated `36` audited Category 1 hits plus `4`
  adjacent upper bounds across `18` files / `15` test binaries (`m2_challenger_1_stress`,
  `m4_challenger_1_stress`, and `master_restart_recovery` untouched):
  - `crates/core/src/capabilities.rs` (`rusty_grid_core --lib`, `5` hits): in
    `test_probe_blender_with_timeout_handles_version_non_version_failure_and_timeout` (`:3755` `< 8 s / 10 s`,
    adjacent Unix `det_elapsed < 6 s`, `:3844` `< 15 s / 20 s`),
    `test_hung_blender_probe_via_override_times_out_and_reports_no_blender` (`:3895` `HOST_PROBE_TIMEOUT + 5 s / 10 s`),
    and `test_probe_tool_with_timeout_and_host_tool_tags` (`:3942` `< 15 s`), raised inner background/hung
    sleeps from `60 s` to `120 s`, widened upper bounds and `max_overhead` to `60 s` (preserving
    `assert_pid_file_process_dead` and `:3879` `elapsed >= 2 s` lower bound), and printed elapsed durations.
  - `crates/core/tests/m1_3d_adversarial_stress.rs` (`1` hit, `:722`): in
    `test_rapid_repeated_detection_sequential_leak_and_stability`, widened `median < 1500 ms` to a `60 s`
    hang guard while preserving latency percentile prints, `per_call_guard`, and `handle_growth <= 5`.
  - `crates/core/tests/m1_stress_challenge.rs` (`2` hits, `:295`, `:360`): in
    `test_high_throughput_10k_sequential` and `test_high_throughput_10k_pipelined`, removed
    `msgs_per_sec > 1000.0` and `> 5000.0` throughput SLAs, wrapped both loops in `60 s`
    `tokio::time::timeout` hang guards, and preserved all round-trip/FIFO assertions and rate prints.
  - `crates/master/src/mapreduce.rs` (`rusty_grid_master --lib`, `1` hit, `:1600`): in
    `test_mapreduce_job_waits_for_queue_room_and_completes_when_room_is_freed`, raised `impossible_job`
    timeout `10 -> 120`, wrapped `engine.execute` in a `60 s` timeout, added structural assertion
    `queue.get_schedulable_tasks().await.is_empty()` alongside `QueueByteLimitExceeded`, logged elapsed,
    and widened `< 1 s` to `< 60 s`.
  - `crates/master/src/queue/recovery_tests.rs` (`rusty_grid_master --lib`, `1` hit, `:801`): in
    `test_release_expired_adoption_holds_fast_path_with_10k_tasks`, replaced `< 50 ms` with a structural
    zero-lock O(1) proof holding `q.inner.write().await` while polling 1,000 `release_expired_adoption_holds()`
    calls via biased `tokio::select!` (`_ = yield_now() => panic!`, plus `adoption_holds == 0`), logged
    elapsed, and widened the hang guard to `60 s`.
  - `crates/master/src/scheduler.rs` (`rusty_grid_master --lib`, `2` hits, `:6445` [audit `:6384`],
    `:7024` [audit `:6963`]): added `#[cfg(test)]` `Cell<usize>` counters on `Engine` and `ScheduleReport`,
    asserted `candidate_scans == 1` and `dead_speed_hits == 19_999` in
    `test_sched4_dead_speed_cache_and_cap_bypass` (`< 5 s` -> `< 60 s`) and `workers_inspected < 100_000`
    in `schedule_1000_heterogeneous_workers_scales_cleanly` (`< 2 s` -> `< 60 s`), and logged elapsed plus
    counters.
  - `crates/master/src/server/persistence_tests.rs` (`rusty_grid_master --lib`, `2` hits, `:479`, `:546`):
    in `test_p2p_handshake_refusals_delivered_over_real_iroh`, widened `closed()` timeouts (`5 s -> 60 s`)
    and upper elapsed guards (`<= 3 s -> < 60 s`) for both silent and flow-control-blocked refused peers
    while keeping `elapsed >= 900 ms` and subsequent normal-peer admission, and logged elapsed.
  - `tests/cluster_auth_e2e.rs` (`2` hits, `:515`, `:541`): configured the oversized-first-frame master with
    `with_handshake_timeout(120)` so immediate EOF/generic-reject proof does not depend on timeout, widened
    stream read timeouts and upper elapsed guards from `3–4 s` to `60 s` in
    `test_oversized_first_frame_is_cut_off_and_master_stays_healthy` and
    `test_silent_unauthenticated_peer_is_dropped_after_the_handshake_timeout` (keeping a `>= 900 ms` lower
    bound around the `1 s` handshake timeout), and logged elapsed.
  - `tests/m10_streaming_stress.rs` (`1` hit, `:748`): in
    `test_stress_master_graceful_shutdown_drains_telemetry_streams`, applied a shared `60 s`
    `drain_deadline` (`timeout_at`) across WebSocket readers, widened `< 4 s` to `< 60 s`, and logged elapsed.
  - `tests/m2_challenger_2_stress.rs` (`4` audited + `1` coupled `poll_until` at `:297`): in
    `test_reaper_fast_path_abrupt_socket_drop_without_disconnecting` (`:297`, `:321`), raised reaper timeout
    `60 s -> 120 s` and widened poll/elapsed guard `200 ms -> 60 s` (fast-path EOF beats `120 s` reaper);
    in `test_adversarial_quic_stream_multiplexing_heavy_saturation_anti_hol` (`:1042`, `:1047`, `:1052`),
    replaced `< 100 ms` / `< 200 ms` / `< 1500 ms` SLAs with structural flow-control backpressure isolation
    (`stream_receive_window(64 KiB)`, `receive_window(4 MiB)`, `Notify`-paused `data_sink_task`, `16 KiB`
    write chunks firing `data_started`, `60 s` `timeout_at` over 30 heartbeats, asserting
    `!data_producer_task.is_finished()` before releasing the sink and verifying all 5 MiB arrive).
  - `tests/m2_stress_challenge.rs` (`3` audited + `3` coupled `max_r1` bounds at `:325/:338/:351`): in
    `test_challenge_handshake_timeout_resilience_and_non_blocking` (`:277`, `:282`), raised handshake
    timeout `5 -> 10 s`, verified all three Slowloris sockets still return `WouldBlock` on `try_read` after
    legit registration completes, and widened `poll_until`/`rogue_timeout`/`max_r1` to `60 s` (keeping
    `>= 9 s` lower bound); in `test_challenge_malformed_handshake_immediate_rejection` (`:717`), raised
    handshake timeout `5 -> 120 s` (`RegisterAck(accepted=false)` proves malformed rejection independent of
    timeout) and widened `recv_msg` + elapsed guard `< 500 ms` to `60 s`.
  - `tests/m4_challenger_2_stress.rs` (`1` hit, `:808`): in
    `test_adversarial_cancel_mid_execution_and_permit_reuse`, raised task `sleep 10 -> 120` / timeout
    `20 -> 120`, widened `wait_for`/`wait_task`/elapsed guard (`< 1500 ms -> < 60 s`), and logged elapsed.
  - `tests/m9_stress_challenge_2.rs` (`3` hits, `:850`, `:930`, `:1136`): in
    `test_client_wait_task_unblocks_promptly_on_crash`,
    `test_client_wait_task_unblocks_on_reaper_eviction_latency`, and
    `test_multiple_concurrent_client_waiters_fanout`, raised `BuiltinTest` `duration_ms` (`3000`/`5000` ->
    `120_000`) and task timeouts (`30 -> 120`), widened `wait_for`/`wait_task`/elapsed guards (`500–600 ms`
    -> `60 s`), and logged elapsed.
  - `tests/master_limits_and_scheduling.rs` (`1` hit, `:136`): in
    `test_surplus_peers_are_closed_and_closing_one_frees_its_slot`, widened slot-release loop guard
    `< 5 s -> < 60 s` and logged release elapsed.
  - `tests/master_quic_limits.rs` (`3` hits, `:73`, `:105`, `:175`): widened idle-peer close (`:73`, keeping a
    `>= 900 ms` lower bound around its `1 s` handshake timeout) and
    3-worker registration (`:175`) guards to `60 s`; in
    `test_quic_peers_count_against_the_connection_budget_and_free_it_when_gone` (`:105`), raised handshake
    timeout `20 -> 90 s` (`deadline = 90 + 25`), widened refusal wait/guard to `60 s`, and added
    `first.close_reason().is_none()` proving the first peer still holds the single connection slot.
  - `tests/test_challenger_m4_scheduler_runner.rs` (`1` hit, `:553`): in
    `test_stress_high_throughput_matchmaking_burst_100_tasks`, widened `< 50 ms` to `< 60 s` while keeping
    latency printing, determinism, and constraint invariants.
  - `tests/worker_crash_rescheduling_test.rs` (`2` audited + `1` coupled `wait_for` at `:272`): in
    `test_immediate_graceful_disconnect` (`:272`, `:274`), raised reaper timeout `10 -> 120 s`, widened
    `wait_for`/elapsed guard `< 300 ms -> < 60 s`, accepted `Scheduled | Running | Completed` on survivor, and
    asserted `evacuation_count == 1`, `retry_count == 0`, and `failover_count == 0` after reassignment to
    prove the uncharged `0 ms`-backoff graceful evacuation path;
    in `test_reaper_waiters_resolution` (`:549`), raised `duration_ms` `5000 -> 120_000` / timeout
    `30 -> 120`, widened `wait_task`/elapsed guard (`< 2 s -> < 60 s`), and logged elapsed.
  - `tests/zero_copy_quic_test.rs` (`1` hit, `:406`): in
    `test_quic_stream_multiplexing_isolation_and_anti_hol_blocking`, replaced `avg_latency < 50 ms` with the
    same structural QUIC stream flow-control pause check (`stream_receive_window(64 KiB)`, `receive_window(4
    MiB)`, paused `data_sink_task`, `Notify`-gated start, `60 s` `timeout_at` over 10 heartbeats, and
    `!data_producer_task.is_finished()` before sink release).
  - **Cheap targeted 20-run loaded before-loops (`origin/master` `f485533e`, Apple M5, 6 busy loops, private `HOME`; expensive `capabilities` / `m1_3d_adversarial_stress` / full master integration before-loops skipped):**
    - `m1_stress_challenge` (`test_high_throughput_10k_`): `20/20` (`total=10.18s`, `min=0.32s`, `max=3.56s`)
    - `rusty_grid_master --lib` (4 targeted tests: `mapreduce`, `recovery_tests`, `scheduler` x2): `20/20` (`total=15.99s`, `min=0.62s`, `max=3.90s`)
    - `test_challenger_m4_scheduler_runner` (`test_stress_high_throughput_matchmaking_burst_100_tasks`): `20/20` (`total=8.33s`, `min=0.24s`, `max=3.22s`)
    - `zero_copy_quic_test` (`test_quic_stream_multiplexing_isolation_and_anti_hol_blocking`): `20/20` (`total=13.03s`, `min=0.34s`, `max=6.19s`)
    - `worker_crash_rescheduling_test` (`test_immediate_graceful_disconnect` + `test_reaper_waiters_resolution`): `20/20` (`total=53.36s`, `min=2.46s`, `max=6.13s`)
  - **Single-run counts and full-binary 20-run loaded after-loops (Apple M5, 6 busy loops, private `HOME`, `--locked`; all `15/15` binaries passed `20/20`):**
    - `rusty_grid_core --lib`: single `295/295` (`8.51 s`), after `20/20` (`total=165.40s`, `min=8.11s`, `max=9.83s`)
    - `core::m1_3d_adversarial_stress`: single `21/21` (`88.03 s`), after `20/20` (`total=3640.83s`, `min=158.45s`, `max=272.14s`)
    - `core::m1_stress_challenge`: single `15/15` (`1.18 s`), after `20/20` (`total=48.74s`, `min=2.28s`, `max=3.81s`)
    - `rusty_grid_master --lib`: single `394/394` (`22.80 s`), after `20/20` (`total=485.30s`, `min=23.21s`, `max=25.85s`)
    - `master::cluster_auth_e2e`: single `18/18` (`2.57 s`), post-review after `20/20` (`total=62.22s`, `min=2.88s`, `max=6.99s`; initial `62.43 s`, `2.88–7.07 s`)
    - `master::m10_streaming_stress`: single `6/6` (`0.44 s`), after `20/20` (`total=18.92s`, `min=0.72s`, `max=4.64s`)
    - `master::m2_challenger_2_stress`: single `12/12` (`0.63 s`), after `20/20` (`total=22.97s`, `min=0.92s`, `max=4.84s`)
    - `master::m2_stress_challenge`: single `6/6` (`10.06 s`), after `20/20` (`total=211.20s`, `min=10.38s`, `max=12.27s`)
    - `master::m4_challenger_2_stress`: single `11/11` (`4.51 s`), after `20/20` (`total=103.60s`, `min=4.81s`, `max=11.41s`)
    - `master::m9_stress_challenge_2`: single `10/10` (`1.90 s`), after `20/20` (`total=47.88s`, `min=2.18s`, `max=5.75s`)
    - `master::master_limits_and_scheduling`: single `5/5` (`3.48 s`), after `20/20` (`total=87.71s`, `min=4.06s`, `max=6.37s`)
    - `master::master_quic_limits`: single `3/3` (`1.56 s`), post-review after `20/20` (`total=73.76s`, `min=1.99s`, `max=31.99s`; initial `45.28 s`, `2.07–4.54 s`)
    - `master::test_challenger_m4_scheduler_runner`: single `3/3` (`0.00 s`), after `20/20` (`total=10.05s`, `min=0.29s`, `max=4.26s`)
    - `master::worker_crash_rescheduling_test`: single `4/4` (`2.22 s`), post-review after `20/20` (`total=53.08s`, `min=2.47s`, `max=5.18s`; initial `54.84 s`, `2.50–6.72 s`)
    - `master::zero_copy_quic_test`: single `8/8` (`0.33 s`), after `20/20` (`total=20.48s`, `min=0.89s`, `max=2.94s`)
  - **Quality gates:** `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `python3 tests/test_m1_challenger_hygiene.py` passed.

### BUG-23: Stop the Windows master gracefully in `oxctl.ps1`

- **Evidence:** Read on 2026-11-17 while preparing 0.4.0: `windows/oxctl.ps1` starts the master with
  `Start-Process -WindowStyle Hidden` and stops it with `taskkill.exe /F /T /PID` (line 135). A forced end
  skips the master's Ctrl+C, Ctrl+Break and close handlers (`wait_for_worker_shutdown_signal` in
  `crates/cli/src/cli_app.rs`), so a master with `--state-dir` never compacts `journal.bin`. Confirmed by
  the Windows smoke test of 0.4.0 on 2026-11-17 (`OxideSwarm_Agent_Hub` `test_runs/win_smoke_0.4.0_118fd88.md`,
  S1): after `taskkill /F /T /PID` the 0.4.0 master refuses the journal with the documented message. The way
  out also works there (S2): the 0.3.0 master in a visible console, Ctrl+C, exit code 0, journal compacted,
  and 0.4.0 then shows the 3 tasks.
- **Problem:** Across a protocol bump the next master refuses an uncompacted journal (DEPLOYMENT.md
  section 3.5), and a hidden master has no console to press Ctrl+C in. The launchers do not pass
  `--state-dir`, so only a master whose `state_dir` was set by hand (flag, environment or config file) is
  affected. `switch_role.sh` already sends SIGTERM and waits before SIGKILL.
- **Do:** Ask the master to stop first (for example a console control event to its process group, or a
  local stop request), wait a grace period, and use `taskkill /F /T` only after it; print which way it
  stopped.
- **Done when:** `oxctl.ps1 stop` on a master with `--state-dir` leaves a compacted journal (`journal.bin`
  with 0 records), shown by a launcher test or a recorded Windows run.
- **Implemented (2026-11-17, pull request 282, [design 0086](design/0086-graceful-windows-stop.md)):**
  `Stop-Tracked` first sends a console Ctrl+C from a separate hidden `powershell` (`FreeConsole`,
  `AttachConsole`, `SetConsoleCtrlHandler(NULL, TRUE)`, `GenerateConsoleCtrlEvent(CTRL_C_EVENT, 0)`), waits up
  to 15 s and only then runs `taskkill /F /T`, and prints which way the process stopped; the worker gets the
  same stop (its clean stop cancels its tasks and ends their trees). Recorded run with the 0.4.0 binary and a
  private state dir: before, `journal.bin` 2157 B and no `snapshot.bin`; after, `master stopped cleanly after
  Ctrl+C in 0.7 s`, `journal.bin` 24 B, `snapshot.bin` 3331 B, and a restart lists the 3 tasks. A worker with a
  running `Start-Sleep 60` task stopped cleanly in 0.7 s with no task process left. `WindowsOxctlStopTests` in
  `tests/test_launcher_scripts.py` covers the clean path and the `taskkill` fallback on Windows.

### BUG-24: Let a master restart with `--state-dir` on a file system without permissions

- **Evidence:** Windows smoke test of 0.4.0 on 2026-11-17 (`OxideSwarm_Agent_Hub`
  `test_runs/win_smoke_0.4.0_118fd88.md`, finding G1). With `--state-dir` on an exFAT drive the master writes
  `members.json` and `master_noise.key` on its first start. On the next start `MemberStore::load_file`
  (`crates/master/src/members.rs`) runs `check_members_file_permissions`, which reads the DACL; exFAT has no
  ACLs, every account counts as having full control, and the master exits with code 1:
  `members file '<state>\members.json' is accessible by other accounts (Everyone); to make it private run: icacls ...`.
  The 0.3.0 and 0.4.0 masters behave the same, so it is not an upgrade regression. The 0.3.0 master also logged
  the same warning for `node.key` in a private HOME on exFAT; 0.4.0 and the other files under the private-file
  check were not tried. macOS and Linux (mode bits from the mount options) were not tried.
- **Problem:** A master whose state lives on an exFAT or FAT stick or SSD works once and then never starts again,
  and the message suggests an `icacls` command that cannot work on that file system.
- **Do:** Detect a file system without permissions (Windows: `GetVolumeInformationW` flags without
  `FILE_PERSISTENT_ACLS`; Unix: the mode cannot be narrowed) at the first start, before the master writes
  anything there, and refuse with a message that names the file system and asks for a directory on NTFS, APFS
  or ext4. Say the same instead of the `icacls` command when a later start finds it.
- **Done when:** A test (or a recorded Windows run) shows that a master pointed at a `--state-dir` without
  permissions refuses at the first start with that message and writes nothing there, and that NTFS behaves as
  before.
- **Implemented on Windows (2026-11-17, pull request 284, [design 0085](design/0085-state-dir-without-permissions.md)):**
  the CLI master (right after the state directory is resolved) and `MasterServer::bind_full` read the volume flags
  of the state directory, or of its nearest existing ancestor (`GetVolumeInformationByHandleW`, nothing created),
  and refuse a volume without `FILE_PERSISTENT_ACLS` with `state directory '<dir>' is on a file system that keeps
  no file permissions (exFAT), so every account on this machine could read or replace it; use a directory on an
  NTFS drive instead`. `members.json` and the private-file warnings (cluster secret, `node.key`,
  `master_noise.key`, the P2P key, the task store directory) use the same text on such a volume instead of the
  `icacls` hint. Recorded run: exFAT refused at the first start with nothing created, an exFAT state directory
  written by 0.4.0 refused and unchanged, NTFS start, stop and start as before. macOS and Linux are not covered.

### BUG-25: Hand a task back to the master when the worker's sandbox base is unusable at assignment

- **Evidence:** Found while closing TEST-33 (pull request 288, run `44044328538`): a task with
  `with_max_retries(10)` ended `Failed` after one failover with `error: Sandbox base: The sandbox base
  directory '...' is not writable: ... (os error 2)`. A worker that cannot use its sandbox base at assignment
  answers with a failed `TaskResult` (`crates/worker/src/client/tasks.rs`, around line 520: `ensure_private_root_recheck`
  fails, `TaskResult::failure(.., exit code 1, "Sandbox base: {e}")`, run time 0). The master treats a result as
  "the worker refused the task" only when its text holds one of the markers of `WORKER_REJECTION_MARKERS`
  (`crates/master/src/queue.rs`: "rejected by worker", "exceed worker capabilities", "capabilities insufficient",
  "lacks gpu capability"; `is_worker_rejection`). The sandbox message has none, so it is an execution failure,
  and `RetryPolicy::retry_on_execution_failure` is `false` by default (`queue.rs`, `queue/lifecycle.rs`), so the
  task becomes `Failed` with retries left. The same holds for the input-fetch check before the blob cache
  and for `Sandbox::create` failing on the base.
- **Problem:** The task is lost although another worker could run it, and the worker keeps looking idle, so it
  can absorb further tasks the same way (the situation `is_worker_rejection` was written for). Scope: the base
  is checked at start-up (`WorkerClient::run`, `ensure_usable`), so this needs a base that breaks while the worker
  runs. The default base (`<temp>/rusty_grid-<euid>/sandboxes`) is re-created on the next task (the per-task
  re-check recreates the private root, and `Sandbox::create` and the lock directory use `create_dir_all`), so a
  temp cleaner that removes it is survived on Unix; on Windows the same re-check recreates the base, yet the CI
  run above saw `os error 2` while the directory was being deleted. An explicit `--sandbox-base-dir` is checked
  the same way on Windows (any base) and, on Unix, re-created by `create_dir_all` but not re-checked for safety.
  What is not recreated: a base whose parent is gone and cannot be re-created, a full or read-only disk, or
  changed permissions. In those cases every assigned task fails for good.
- **Do:** Make a `BaseDirUnusable` or `BaseDirUnsafe` refusal at assignment, at the input fetch and in
  `Sandbox::create` a worker refusal: add a marker the master recognises (for example "rejected by worker:
  sandbox base") to the failure text, or a typed flag, keeping `execution_time_ms == 0`. Consider retrying the
  re-check once after a short delay, since the Windows case looked like a removal in progress.
- **Done when:** A test removes (or makes unwritable) the base of one of two workers after it registered, submits
  tasks with `max_retries` above 0, and all complete on the other worker; `is_worker_rejection` has a unit test
  with the exact message.
- **Implemented (2026-11-17, pull request 299):** The three refusals answer `Task rejected by worker: sandbox base
  unusable: <reason>` (`rusty_grid_core::sandbox_base_refusal`), which the master already matches ("rejected by
  worker") and now lists in full; no wire change, a 0.4.x master hands it back too. The assignment and input-fetch
  re-checks repeat once after 250 ms; `Sandbox::create` refusals cover the base errors and I/O errors under the base.
  The existing `avoid_workers` and `MAX_WORKER_REJECTIONS_PER_TASK` logic prevents a loop, so no new guard. Test:
  `crates/master/tests/sandbox_base_hand_back.rs`; design 0090.

### BUG-26: Keep the `ox-mode dev cluster` master off the default discovery port

- **Evidence:** Seen with the master log that TEST-43 (pull request 307) adds: the master that
  `launch_local_dev_cluster_with_ports` starts (`crates/cli/src/mode_cli.rs`) logged `Master UDP Discovery
  beacon & query responder active bind_addr=127.0.0.1:8089 discovery_port=8089`. It is started without
  `--discovery-port`, so it takes the default port 8089, on a developer machine that may also run a real
  cluster, and in the tests that start a dev cluster.
- **Problem:** The dev cluster's two workers connect with `--master 127.0.0.1:<port>`, so nothing uses its
  beacon, but the beacon still answers discovery queries on 8089 and can collide with a master that uses the
  default port on the same machine.
- **Do:** Start the dev cluster's master with `--discovery-port 0`, which turns the beacon off.
- **Done when:** The dev cluster's master binds no UDP discovery port, and the dev cluster tests pass.
- **Status:** Implemented by pull request 311. Both branches of the launcher (isolated home and workspace)
  pass `--discovery-port 0`; the change adds no line to `mode_cli.rs`, which is at its allowlisted size.
  While the four dev cluster tests ran on Windows, a watcher listed the UDP endpoints of every `rusty-grid.exe`
  they started: none with this change; `master 127.0.0.1:8089` with the build before it.

### BUG-28: Refuse a P2P ticket passed to `worker --master` with a `--p2p-ticket` hint

- **Evidence:** Starting `oxideswarm worker --master <ticket>` with an iroh P2P ticket left `p2p_ticket`
  unset and logged plain TCP `TcpStream::connect` `"invalid port value"` errors, while `--p2p-ticket <ticket>`
  worked. `README.md` (`worker --master <ADDR|auto>`) and `worker --help` document only `HOST:8088`, bare `HOST`,
  and `auto` for `--master`, with `--p2p-ticket <TICKET>` as the worker's P2P ticket flag.
- **Problem:** `resolve_worker_target` (`crates/cli/src/cli_app/worker.rs`) called `normalize_master_addr`,
  which returns `is_p2p_ticket` strings verbatim, and cleared `p2p_ticket` whenever an explicit master string
  was supplied, so the ticket flowed into `WorkerConfig::master_addr` and `WorkerClient::establish_tcp` tried to
  dial it over TCP.
- **Do:** Reject P2P tickets (`is_p2p_ticket`) upfront in `resolve_worker_target_from` (`--master`,
  `RUSTY_GRID_MASTER_ADDR`, `RUSTY_GRID_MASTER`, `[worker].master`) with a clear error pointing to `--p2p-ticket`
  without echoing the raw ticket, write a failing unit test first generating a fresh ticket from `iroh::SecretKey`
  at runtime, and document the hint in `worker --master` help and `README.md`.
- **Done when:** `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`,
  and `cargo test -p rusty_grid_cli --locked --lib` pass, with the new ticket unit tests confirmed failing
  before the fix.
- **Implemented (2026-11-18, pull request 334):** `resolve_worker_target_from` checks `is_p2p_ticket` on the
  selected target source before calling `normalize_master_addr` and returns
  `"this looks like a P2P ticket; use --p2p-ticket ..."` without interpolating the ticket; `run_worker` exits with
  `ExitCode::FAILURE` before credential resolution or networking. Both `worker::tests` tests (testing `--master`,
  `RUSTY_GRID_MASTER_ADDR`, `[worker].master`, `--p2p-ticket`, and `run_worker` with an `EndpointAddr` JSON ticket
  generated at runtime from `iroh::SecretKey::generate()`) failed before the fix and pass after (`201 passed; 0
  failed` in `rusty_grid_cli --lib`). Added the hint to `WorkerArgs::master` in `crates/cli/src/cli_app.rs` and
  `README.md`.

### BUG-27: Compute the result acknowledgement token only when result resume is on

- **Evidence:** Found in TEST-53 (pull request 327): a `sample` of
  `test_oversized_task_result_fails_only_its_task_while_other_tasks_on_worker_finish` put about 1.3 s (1,341
  samples) of the test's single runtime thread in `master_features::result_ack_token`, which hashes the 70 MiB
  `asset_bytes` of the oversized result with BLAKE3, unoptimized in the `test` profile. That test's worker has no
  resume window, so the token it computed was never used.
- **Problem:** The worker's task runner (`crates/worker/src/client/tasks.rs`) computed the token for every
  result. Only the session-resume outbox uses it (`PendingResumeOutbox`: the push, `update_result` after an output
  upload, and `ack_result`), and all three calls already ran only when the session has a resume window
  (`resume_enabled`: the master announced `session-resume=<secs>` and the worker's grace cap is not 0), so
  without one the hash was thrown away. With one, `BufferedTaskResult::new` hashed the same result a second time,
  under the outbox's mutex. The master (`crates/master/src/server/tasks_and_blobs.rs`) also computed the token for
  every result and sent it (`CancelTask` with `oxs:result-ack:<token>`) only when its session-resume hold is above 0.
  The worker sends its `TaskResult` the same way with or without the token; the token is never on the wire in it.
- **Do:** Compute the token on the worker only when the session has a resume window, once, and hand it to
  `BufferedTaskResult::new`; compute it on the master only when the hold is above 0. Keep the token bytes and
  every message unchanged.
- **Done when:** A unit test shows that the token is not computed when resume is off, and that it is the same
  token, computed once, when resume is on; the worker unit tests and the result resume integration tests pass.
- **Implemented (2026-11-18, pull request 332):** `resume_ack_token(resume_enabled, &result, token)` in
  `tasks.rs` returns `None` without calling `token` when resume is off; the runner keeps the token as an
  `Option<u64>` that is `Some` exactly when `resume_enabled`, and the three outbox calls take it from there.
  `BufferedTaskResult::new` takes the token from its caller instead of hashing again. The master's `ack_token` is
  an `Option` computed under the same `session_resume_hold_secs() > 0` test that decided whether to send it. No
  wire change. Test: `client::tests::test_result_ack_token_is_computed_only_when_resume_is_on` (a counting
  closure: no call with resume off; one call, returning `master_features::result_ack_token`, with resume on).
  Not changed: with resume on the hash still runs on the async runtime thread, once after the run, again in
  `update_result` after an output upload and in the session's resume flush (`session.rs`), both under the
  outbox's mutex, and once on the master. Moving it to `spawn_blocking` would add an await between the end of the
  run and the outbox push and would touch all four places, so it is left out. A `master` started from the CLI
  holds sessions for 30 s by default (`--session-resume-hold-secs`), so resume is on there unless the hold is set
  to 0; `ServerConfig` (library and tests) defaults to 0.

### BUG-29: Offer in the USB installer's folder menu only drives whose root the install accepts

- **Evidence:** On 2026-11-18, on the owner's Windows 11 laptop with kit 0.4.3, "Install OxideSwarm.cmd" offered
  `2) E:\OxideSwarm-USB` in its folder menu. Choosing it failed with "cannot use E:\OxideSwarm-USB: another
  account (NT AUTHORITY\Authenticated Users) can delete or re-permission folders at the root of E:\"
  (`Test-RootSafety`, `packaging/usb/install.ps1`). The refusal itself is correct.
- **Problem:** The menu (`[IO.DriveInfo]::GetDrives()` filtered by `Test-InstallDrive`) checked only the drive
  type, file system and bus, not the root check that the install then runs, so it offered a drive the install
  refuses.
- **Do:** Build the menu from folders that pass the same check, hiding a failing one or listing it as not
  usable with the reason; keep the default folder first.
- **Done when:** A Windows test shows a refused folder listed as not usable (or absent), and the default first.
- **Implemented (2026-11-18, pull request 341):** The menu runs `Test-RootSafety` on each candidate folder and
  lists a refused one without a number, as `not usable: <reason>`, so the person at the keyboard learns why a
  drive is missing; the default stays `1)`, and with only the default usable nothing is asked. Test hooks
  `OXIDESWARM_USB_LIST_FOLDERS=1` (print the menu and stop) and `OXIDESWARM_USB_DRIVE_CHOICES` (replace the other
  drives' folders). Tests: `UsbKitTests.test_windows_drive_menu_offers_only_folders_the_install_accepts` (static)
  and `StaticRepositoryChecks.test_windows_usb_drive_menu_old_install_uninstall_and_failed_start` (Windows CI).

### BUG-30: Let the USB uninstaller remove an install from a kit older than the workspaces marker

- **Evidence:** On the same laptop, "Uninstall OxideSwarm.cmd" refused the old `E:\OxideSwarm` install:
  "E:\OxideSwarm\workspaces was not created by the USB kit (missing .oxideswarm-usb-workspaces marker; used by
  another OxideSwarm worker or tool)" (`packaging/usb/uninstall.ps1`). The old kit predates that marker; the
  owner created it by hand.
- **Problem:** An install from an older kit could not be removed with the current uninstaller.
- **Do:** Accept a `workspaces` folder without the marker when the install folder carries
  `.oxideswarm-usb-install`, `workspaces` sits directly inside it and neither it nor anything inside it is a
  link, and no other worker or tool uses it (`Get-WorkspaceDirUsers`); refuse every other case. The same in
  `uninstall.sh` if it has the marker logic.
- **Done when:** A test removes an old-style install without the marker and still refuses a foreign one.
- **Implemented (2026-11-18, pull request 341):** `uninstall.ps1` accepts such a folder under those conditions
  and one more: it is not `%LOCALAPPDATA%\oxideswarm\workspaces`, the default workspace root of a non-USB worker,
  which a stopped worker may still own. It says so ("an install from an older kit"). `uninstall.sh` has no marker
  check and is unchanged. Tests: `UsbKitTests.test_windows_uninstaller_removes_an_install_from_an_older_kit`
  (static) and the Windows test above (an old install is removed; one used by another running worker and one
  without `.oxideswarm-usb-install` are refused).

### BUG-31: Report a USB-kit worker that stops after its start, with its log, and keep that log

- **Evidence:** On the same laptop, a fresh install into `%LOCALAPPDATA%\OxideSwarm-USB` printed "ERROR: the
  worker runs but the master does not list it yet; see ...\logs\worker.log", then rolled back and deleted the
  folder, log included. Reproduced with the same binary: the worker exited about 6 s after its start, because its
  sandbox base `%TEMP%\oxideswarm-usb\sandboxes` sits in a Temp folder that grants Modify to three leftover
  accounts, and the worker's sandbox base check refuses it. GPU start-up took about 5 s; `install.ps1` checked the
  worker only at about 3 s (`install.sh` at 4 s).
- **Problem:** A worker that stops after the one liveness check is misreported, and the log the error names no
  longer exists.
- **Do:** In both installers, check the worker every second during the whole wait for the listing and report
  "the worker stopped" with its exit code; on any failed start print the last 30 lines of `worker.log` under a
  header before the rollback; keep the log (or a private copy) through the rollback, so the error names a file
  that exists. Decide whether the default Windows install still needs `%TEMP%` for its sandbox base.
- **Done when:** A fake worker that exits at 5 s is reported as stopped, with its log tail printed, and its log
  exists after the rollback (at least for `install.sh`).
- **Implemented (2026-11-18, pull request 341):** Both installers watch the worker every second while the
  listing runs (`install.sh` in the background, `install.ps1` in a runspace) and fail with "the worker stopped N
  s after it started (exit code C)" (`wait` in bash, a process handle in PowerShell), print the last 30 lines of
  `worker.log` under a header, and keep `<folder>/logs` through the rollback once the worker has been started
  (`install.sh` makes it mode 700); `install.sh` keeps its isolation fallback. Sandbox base: a Windows install
  now uses `<folder>\work` except under `%LOCALAPPDATA%` with `-Isolation fs` (the isolated worker accepts no
  other folder inside the profile), an update keeps a usable base, and `install.ps1` runs the worker's base check
  before it joins, naming the folder and accounts and printing, never running, the `icacls /remove:g` command
  (design 0053 status log). Tests: `UsbKitTests.test_a_worker_that_stops_after_the_start_is_reported_with_its_log`,
  `test_install_falls_back_to_isolation_off_when_the_isolated_worker_stops`,
  `test_windows_installer_watches_the_worker_and_keeps_its_log`,
  `test_windows_task_folders_stay_in_the_install_folder_unless_isolation_needs_temp`, and the Windows test above.

### BUG-32: Allow the USB kit's workspace root under -Isolation fs

- **Evidence:** Reproduced live on 2026-11-18 on a Windows 11 laptop (`win2/bug-32-workspace-root-fs`
  over `origin/master` `d90e1e5`): a worker started with `--isolation fs --workspaces` and
  `--workspace-root <profile>\AppData\Local\OxideSwarm-USB\workspaces` exited at startup with
  `refusing to rewrite the DACL of user folder '...' as workspace_root`, and passing a
  `%TEMP%\...\workspaces` root holding `.oxideswarm-usb-workspaces` also exited at startup with
  `containing non-workspace entry '.oxideswarm-usb-workspaces'` (`validate_workspace_root_safe`,
  `crates/worker/src/isolation/windows/toolchain.rs`).
- **Problem:** Under `-Isolation fs` with `--workspaces`, `packaging/usb/install.ps1` passes
  `--workspace-root <install>\workspaces` (or `<install>\usb-kit\workspaces` when updating a
  legacy `%LOCALAPPDATA%\OxideSwarm` install) and creates `.oxideswarm-usb-workspaces` inside it.
  The worker's `validate_workspace_root_safe` rejected both the USB kit paths under `%LOCALAPPDATA%`
  and the marker file.
- **Do:** Statically allow `<home>\AppData\Local\OxideSwarm-USB\workspaces` and
  `<home>\AppData\Local\OxideSwarm\usb-kit\workspaces` in `is_allowed_under_home`, accept
  `.oxideswarm-usb-workspaces` only when `std::fs::symlink_metadata` confirms a regular non-reparse
  file, refuse custom `-InstallRoot` paths inside `%USERPROFILE%` under `-Isolation fs` with
  workspaces before installing in `packaging/usb/install.ps1`, and cover both in tests.
- **Done when:** Both paths and the regular marker file are accepted, directory/symlink/junction/upper-case-name
  markers and sibling/`workspaces-evil`/`..`-escape user-profile paths are refused, `install.ps1` refuses
  custom profile roots before creating directories and installs cleanly in `<LOCALAPPDATA>\OxideSwarm-USB`
  under `-Isolation fs` with workspaces, `uninstall.ps1` requires `.oxideswarm-usb-workspaces` to be a
  regular file (`-PathType Leaf`), and the worker/isolation/launcher checks pass on Windows (with known
  unrelated host caveats recorded separately).
- **Implemented (2026-11-19, pull request 352):** Added `<home>\AppData\Local\OxideSwarm-USB\workspaces`
  and `<home>\AppData\Local\OxideSwarm\usb-kit\workspaces` to `is_allowed_under_home` and accepted
  regular non-reparse lowercase `.oxideswarm-usb-workspaces` files (`symlink_metadata`) in
  `validate_workspace_root_safe` (`crates/worker/src/isolation/windows/toolchain.rs`); added unit
  tests in `crates/worker/src/isolation.rs` (including `workspaces-evil`, `workspaces\..\other`, and
  `.OXIDESWARM-USB-WORKSPACES`); added the pre-install profile check in `packaging/usb/install.ps1`
  and `-PathType Leaf` marker check in `packaging/usb/uninstall.ps1`; and covered default-root
  `-Isolation fs` + workspaces install/uninstall plus refusal cases in `tests/test_launcher_scripts.py`.

### BUG-33: macOS: the lifeline supervisor closes descriptors one by one up to the soft limit for every task

- **Evidence:** Found in the review of pull request 366 (`TEST-58`, `OxideSwarm_Agent_Hub`
  `test_runs/review_pr366.md`): `crates/worker/src/process_tree.rs` (`close_fds_except`) looped `libc::close(fd)`
  up to `RLIMIT_NOFILE` on macOS (`1,048,575` under a developer shell with `ulimit -n 1,048,576`, `65,536`
  under `packaging/macos/com.oxideswarm.worker.plist`, `256` under default `launchd`). A 20-fork `wait4`
  probe on Apple M5 before the fix measured per-task `close_fds_except` median wall / CPU times of
  `111.889 ms` / `111.095 ms` (`mean 111.560 ms` / `110.751 ms`) at `1,048,576` and `7.683 ms` / `7.248 ms`
  (`mean 7.714 ms` / `7.228 ms`) at `65,536`.
- **Problem:** Even though the task child only waits for intermediate fork `child_b` to exit, the detached
  lifeline supervisor burned CPU across cores closing hundreds of thousands of non-existent descriptors on
  every task spawn when `RLIMIT_NOFILE` was high, amplifying contention on other threads.
- **Do:** On macOS, list the calling process's open descriptors via `libc::proc_pidinfo(getpid(),
  libc::PROC_PIDLISTFDS, ...)` into a fixed stack buffer (async-signal-safe after `fork`: no allocation, no
  locks) and close only those not in `keep`, falling back to the bounded `RLIMIT_NOFILE` loop if
  `proc_pidinfo` fails or the buffer overflows; keep Linux on `SYS_close_range` and Android unchanged.
  Add a unit test opening a high-numbered descriptor (`>= 5000`) and proving the supervisor closes it.
- **Done when:** High-fd unit test and `test_sec5_hardened_runner` pass, per-task close time is measured
  before/after at `1,048,576` and `65,536`, and every `unsafe` block carries a `// SAFETY:` comment.
- **Implemented (2026-11-19, pull request 367, open):** Added `close_open_fds_macos` (fixed 1,024-entry
  `[libc::proc_fdinfo; 1024]` stack buffer, up to 8 rounds) and `close_fds_range_fallback` in
  `crates/worker/src/process_tree.rs` ([design 0031](design/0031-containment-after-worker-crash.md)), plus
  `process_tree::tests::test_supervisor_closes_high_numbered_fd` (`F_DUPFD_CLOEXEC` at `>= 5000`, polling
  for pipe EOF while child and supervisor remain alive). After the fix on Apple M5 (20 forks via `wait4`):
  - `ulimit -n 1,048,576`: median wall `0.688 ms` (`mean 0.681 ms`, `min 0.447 ms`), median CPU `0.183 ms`
    (`mean 0.223 ms`, `min 0.166 ms`) — down from `111.889 ms` / `111.095 ms`.
  - `ulimit -n 65,536`: median wall `0.678 ms` (`mean 0.669 ms`, `min 0.549 ms`), median CPU `0.181 ms`
    (`mean 0.196 ms`, `min 0.161 ms`) — down from `7.683 ms` / `7.248 ms`.
  `rusty_grid_worker --lib` and `test_sec5_hardened_runner` (with and without `--features 3d`) pass.

### CLI-2: Add a flag to move or disable the UDP discovery beacon

- **Evidence:** Read in the smoke test (finding F5): the master binds UDP `127.0.0.1:8089`, and
  the CLI has no flag to move it. It was free during the run and released when the master stopped.
- **Problem:** A second loopback master on the same machine cannot start its beacon.
- **Do:** Add a flag and an environment variable (`RUSTY_GRID_*`) to set the beacon port or turn
  the beacon off. Update the `README.md` reference.
- **Done when:** Two masters on one machine run at the same time, one with the beacon off.
- **Implemented (2026-11-17, pull request 275):** `master --discovery-port <PORT>` (`RUSTY_GRID_DISCOVERY_PORT`,
  `[master].discovery_port`) moves the beacon and `0` turns it off; design 0084. A worker finds a moved
  beacon only with `--master auto:<PORT>`. `crates/cli/tests/discovery_port_e2e.rs` runs two masters at
  once, one with the beacon off.

### SEC-17: Windows workspace root: refuse a reparse point in any folder between home and the root, and hard-linked marker or lock files

- **Evidence:** Read in the review of pull request 352 (BUG-32). `path_starts_with_ci` canonicalises both
  paths (`normalize_win_path`), so a junction in an ancestor folder (for example
  `AppData\Local\OxideSwarm-USB` pointing elsewhere) is followed, and `is_reparse_point` checks only the
  last folder (`crates/worker/src/isolation/windows/mod.rs`, `toolchain.rs`). `SetNamedSecurityInfoW`
  then rewrites the DACL of the target. The same holds for the allowed entries that existed before
  BUG-32, so it is not a regression. The marker `.oxideswarm-usb-workspaces` and `store.lock` are not
  checked for hard links.
- **Problem:** Only processes of the same user can create such a junction or link, and the
  `oxideswarm.toolchain` capability gets only non-inherited traverse, so the impact is low. But the rule
  "the worker rewrites a DACL only on its own folders" depends on it.
- **Do:**
  1. Before the rewrite, refuse a reparse point in every folder from the user home to the workspace
     root.
  2. Require `nlink == 1` for the marker and `store.lock` (`windows_file_identity`,
     `crates/worker/src/blob_cache.rs`).
  3. Add tests with a junction in an ancestor folder and with a hard-linked marker. Where the host
     cannot create them, skip with a printed reason.
- **Done when:** Those tests pass on `windows-latest` in CI.
- **Implemented (2026-11-19, pull request 360, open):** `validate_workspace_root_safe` (and a second
  `validate_workspace_root_ancestors_safe` check under `PREPARE_LOCK` right before `protect_sandbox_base`)
  refuses `--workspace-root` itself and any symlink or NTFS junction along the raw, lexical, or canonical
  paths whenever `--workspace-root` is under or resolves into `host.user_home` or `%USERPROFILE%`
  (covering `BUG-32` USB-kit roots, pre-`BUG-32` allowed roots, redirected user home / `AppData\Local`
  junctions with the `--workspace-root` target workaround documented in the refusal message,
  `CHANGELOG.md`, and [design 0033](design/0033-os-level-task-isolation.md), and external junctions above
  or pointing into home, while leaving roots completely outside home without an ancestor check), and
  `check_single_link_regular_file` / `validate_single_link_file_info` open `.oxideswarm-usb-workspaces` and
  `store.lock` with `FILE_FLAG_OPEN_REPARSE_POINT` and read `nNumberOfLinks == 1` directly from
  `GetFileInformationByHandle` (so ReFS / Dev Drive / network volumes with zero volume serial or 64-bit file
  index stay accepted and read failures report the OS cause); unit tests in
  `crates/worker/src/isolation.rs` cover ancestor junctions, home/`AppData\Local` junctions and target
  workarounds, external junctions above and into home, `nlink != 1` hard links, and zero-serial/index
  handling (`SEC-18` tracks the remaining path-based `SetNamedSecurityInfoW` TOCTOU window, other root
  entries, and separate-walk / `RUSTY_GRID_REQUIRE_*` proof tests).

### SEC-18: Windows workspace root: check every entry the DACL rewrite reaches, write the DACL through a handle, and tests that prove each walk

- **Evidence:** Read in the review of pull request 360 (SEC-17), 2026-11-19
  (`OxideSwarm_Agent_Hub` `test_runs/review_pr360.md`, items 5 and 6).
  - Any top-level regular file of the workspace root whose name passes `validate_workspace_name`, or a
    file named `.locks`, is accepted without a type or `nlink` check (`toolchain.rs`), and
    `SetNamedSecurityInfoW` pushes the root's inheritable entries onto it, so a hard link changes the
    DACL of the file it is linked to. `<ws>\state.json` and `<ws>\lock` get a protected DACL written
    directly (`crates/worker/src/isolation/windows/mod.rs`) with only a reparse check.
  - The DACL is written by path (`protect_sandbox_base`, `acl.rs`), so a folder swapped for a junction
    between the check and the write is followed (the window that SEC-17 narrows but cannot close).
  - On CI the test's temporary folder sits under the real `%USERPROFILE%`, so the SEC-17 tests pass even
    without the `host.user_home` walk; the guard assertion checks only `ErrorKind::PermissionDenied`, and
    a skipped test shows as `ok`.
- **Problem:** The rule "the worker rewrites a DACL only on its own folders and files" still has these
  gaps. Only processes of the same user can create the links, so the impact is low.
- **Do:**
  1. Refuse, or skip without rewriting, every top-level entry that is not a folder the worker made,
     and require `nlink == 1` for every file whose DACL is written.
  2. Write the DACL through a handle: open the root with `FILE_FLAG_BACKUP_SEMANTICS` and
     `FILE_FLAG_OPEN_REPARSE_POINT`, compare `GetFinalPathNameByHandleW` with the lexical path, and call
     `SetSecurityInfo` on that handle.
  3. Make the tests prove each walk on its own (a home that is not an ancestor of `%USERPROFILE%` in the
     test), assert the refusal text, and add a `RUSTY_GRID_REQUIRE_*` switch so CI fails when a test
     skips.
- **Done when:** Those tests pass on `windows-latest` in CI with the switch set.
- **Implemented (2026-11-19, pull request 372, open):** `validate_workspace_root_entries_safe` and
  `validate_workspace_dir_entries_safe` refuse unexpected top-level files, reparse entries, non-directory
  `.locks` / workspace folders, and hard-linked (`nNumberOfLinks != 1`) `.locks/*` and
  `<ws>/{state.json,lock,state.json.tmp}` files before any inheritable DACL rewrite runs;
  `open_verified_dacl_handle`, `set_protected_dacl_on_handle`, `set_protected_dacl_on_path`,
  `set_protected_dacl_on_single_link_file_if_exists`, and `protect_sandbox_base` open every modified
  directory and file with `FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT`, verify non-reparse
  type and `nNumberOfLinks == 1` via `GetFileInformationByHandle`, check `GetFinalPathNameByHandleW` against
  the lexical long path (`GetLongPathNameW`), and apply `SetSecurityInfo` on that handle (with `<ws>` and
  metadata DACL failures propagated instead of skipped); `check_chain_from_home` matches home only on
  `Normal` components so a bare drive prefix (`C:`) no longer resolves via CWD; and the tests in
  `crates/worker/src/isolation.rs` prove the `host.user_home` and `%USERPROFILE%` walks independently,
  verify bare-prefix exclusion under home CWD, assert refusal texts, exercise `PREPARE_LOCK` / handle /
  metadata race hooks, and fail under `RUSTY_GRID_REQUIRE_APPCONTAINER=1` instead of skipping.

## Phase 13: the path to finished

Added on 2026-11-17 from [design 0088](design/0088-evaluation-of-2026-11-17-and-what-finished-means.md),
after the owner accepted Q82. That record states ten exit checks that together mean "finished" for
the purpose of Q1. This phase holds the items those checks need that had no row in the Index. Nine
of them have their full text in designs 0056, 0058 and 0059; the item here names the files as they
are at `master` `16b2a38` and the check. Three are new: CI-1, MAINT-2 and DOC-11.

A row in the Index starts no item:

- ADOPT-1 is the owner's (Q37). AX-1 starts after its first round, and so do ADOPT-3 and ADOPT-4, as
  Q38 recommends. Q36, Q38 and Q55 stay open.
- AX-1 to AX-3 may start before release `0.5.0` ships (Q83).

Order:

1. CI-1, DOC-11 and ADOPT-1, beside the test items of Phase 12.
2. ADOPT-2, LV-4 and MAINT-2. None of them waits for the study.
3. AX-1, then AX-2, AX-3, ADOPT-3 and ADOPT-4.
4. LV-15, after LV-5.

Every "Evidence" below was read on 2026-11-17 at `master` `16b2a38`, unless it says otherwise.

### MAINT-2: Bring the last three files under the size cap

- **Evidence:** `scripts/rust-file-size-allowlist.txt` lists three files whose production part
  (every line before the first `#[cfg(test)]`) exceeds 3,000 lines: `crates/worker/src/isolation.rs`
  (5,346), `crates/cli/src/mcp.rs` (3,692) and `crates/cli/src/mode_cli.rs` (3,240). MAINT-1 split
  six other files and left these at their size. `isolation.rs` holds the macOS code in an inline
  `mod macos` and the Windows code in an inline `pub mod windows`. Only the Linux code is in a file
  of its own (`crates/worker/src/isolation/linux.rs`). It also holds 125 of the 290 lines of `crates/*/src`
  that contain `unsafe`.
- **Problem:** The code that confines a task is the hardest file to review, and it is the largest
  production file. Two platforms cannot be changed without a merge conflict in one file.
- **Do:** One pull request per file. Move code, and change no behaviour, no public name and no
  message text.
  1. `isolation.rs`: move `mod macos` to `isolation/macos.rs` and `pub mod windows` to
     `isolation/windows.rs`, beside `isolation/linux.rs`. Keep the shared types, the level
     parsing and the start-up check in `isolation.rs`. Do LV-4 for these files in the same pull
     request or the next one, so each `unsafe` site is read once.
  2. `mcp.rs`: move each tool (`status`, `workers`, `submit`, `task_result`, `cancel`, `doctor`,
     `workspaces`) to a file under `crates/cli/src/mcp/`, and keep the JSON-RPC loop in `mcp.rs`.
  3. `mode_cli.rs`: move each workflow mode (`test`, `dev`, `doc`, `research`) to a file under
     `crates/cli/src/mode_cli/`.
  4. After each split, delete the file's line from the allowlist.
- **Done when:** The allowlist has no entry, `ox-mode test size` passes,
  `cargo test -p rusty_grid_worker --test test_feat3_os_isolation` passes on Linux, macOS and
  Windows, `cargo test -p rusty_grid_cli --test mcp_server_e2e` passes, and
  `crates/core/tests/data/wire_layout.txt` is unchanged.
- **Progress:** step 1 (`isolation.rs`): pull request 331.
- **Progress:** step 3 (`mode_cli.rs`): pull request 325.
- **Progress:** step 2 (`mcp.rs`): pull request 323.
- **Implemented (2026-11-18, pull requests 323, 325 and 331):** `scripts/rust-file-size-allowlist.txt` has no
  entry: each pull request deleted its file's line. At `master` `74a46c72` the production parts are 1,077 lines
  (`isolation.rs`, beside `isolation/macos.rs` and `isolation/windows/`), 1,708 (`mcp.rs`, beside
  `crates/cli/src/mcp/`) and 394 (`mode_cli.rs`, beside `crates/cli/src/mode_cli/`). Each pull request passed
  every CI check, `ox-mode test size` and the workspace tests on Linux, macOS and Windows among them, and none
  changed `crates/core/tests/data/wire_layout.txt`.

### DOC-11: Make the backlog show open work first

- **Evidence:** This file has more than 3,600 lines. Its Index has 178 rows after this phase, and
  106 of them are Implemented. The text of every Implemented item stays in the phases below, so an
  agent that looks for open work reads the finished work first. No column says whether an open
  item is inside the stop line of design 0088 (D1).
- **Problem:** The length hides the open work, and the Index counts growth work (Phases 7 to 9) as
  open work for completion.
- **Do:**
  1. Add a column "Line" to the Index: "In" for an open item that an exit check of design 0088
     (D2) names, "Out" for an open item that its D1 puts outside the stop line, and empty for an
     Implemented item. An open item that neither names is "In" when it fixes a defect or a test
     (BUG, SEC, TEST), and otherwise goes to the owner as a question in the pull request.
  2. Move the text of every Implemented item to `docs/BACKLOG-ARCHIVE.md`, under the same phase
     headings and in the same order. Keep the Index whole, and keep the status log here.
  3. Add both files to the index of `docs/README.md`, and fix every link that pointed at a moved
     heading.
  4. Do this in one pull request, at a time when no other pull request that edits this file is
     open, and say so on the agent hub first.
- **Done when:** No item with status Implemented has its text in this file, every Index row of an
  open item has "In" or "Out", `python3 tests/test_m1_challenger_hygiene.py` reports no broken
  link, and a line count in the pull request shows that the two files together hold the item
  texts unchanged.

### ADOPT-2: Run summary in `submit --wait`, its JSON output and the MCP results

- **Evidence:** [Design 0059](design/0059-first-users.md), D5. `submit --wait` prints
  `Execution Time` per task (`crates/cli/src/cli_app/submit.rs`). No line or JSON field gives the
  time the caller waited, the sum of the task times or the number of machines: a search for
  `waited_ms` and `task_time_sum_ms` in `crates/cli/src` finds nothing.
- **Problem:** A user cannot see what the grid cost or gained on a run without a stopwatch.
- **Do:** As D5 of design 0059, for one task first:
  1. After a waited run, print the two lines of D5 (`Tasks:` and `Waited:`) next to
     `Execution Time`. `Waited` is measured on the client, and the sum adds
     `TaskResult::execution_time_ms`.
  2. Add `waited_ms`, `task_time_sum_ms` and `machines` to the JSON output of `submit` and to the
     result of the MCP tools `submit` and `task_result` (`crates/cli/src/mcp.rs`).
  3. Print no ratio and no percentage, and never call the sum the time on the user's machine.
  4. Update `README.md` and `CHANGELOG.md`. The values come from fields that exist, so the wire
     format does not change.
- **Done when:** The ADOPT-2 tests of the test plan of design 0059 pass: the unit cases (no task,
  one task, one failed task, two tasks on one worker), a waited run on loopback that prints both
  lines and carries the three fields under `--json`, and `mcp_server_e2e` finds the fields while
  standard output stays JSON-RPC only.
- **Status (2026-11-18, Implemented):** Pull request 350: adds `crates/cli/src/run_summary.rs`
  (with the four unit cases of design 0059 plus a two-worker case), prints the two D5 summary lines
  after `Execution Time` in `submit --wait`, adds `waited_ms`, `task_time_sum_ms`, and `machines`
  to `submit --json` and the MCP `submit` and `task_result` tools (covering the submitted task only,
  before `--verify` replicas; for a task submitted in the same MCP
  session, `task_result` measures `waited_ms` from `submit` to the `task_result` reply so it
  includes caller time between calls and increases on repeat queries, and returns `null` when that
  session did not record the submission), covers loopback CLI and MCP runs in
  `crates/cli/tests/test_submit_run_summary.rs` and `tests/mcp_server_e2e.rs`, and updates
  `README.md`, `CHANGELOG.md`, `docs/guides/offload-cargo-test.md`, and `docs/design/0059-first-users.md`.

## Phase 15: coordination between agents

From [design 0087](design/0087-shared-coordination-state.md) (accepted on 2026-11-17, Q84 to Q86). Several
agents and Claude Code sessions change this repository at once; these items keep them from taking the
same id or changing the same lines unawares, and share what one of them learned. Nothing in the grid
changes.

### COORD-1: Claim design numbers, Q ids, phases and item ids on the hub

- **Evidence:** On 2026-11-17 two Claude Code sessions each added `## Phase 13` (pull requests 292 and
  293), and design numbers were reserved only in one session's private memory (design 0087, Context).
- **Problem:** Nothing tells an agent which id another agent is about to use.
- **Do:** As D1 of design 0087: `agent_bridge.py claim design|q|phase|item`, create-only claim files on
  the hub with a settle check, `--list` and `--release`; the rule of Q85 in `CLAUDE.md` and `AGENT_RULES.md`.
- **Done when:** The unit tests pass in CI, and two near-simultaneous claims on the real hub end with two
  different values (a dated record).
- **Implemented (2026-11-17, pull request 295):** `claim` with create-only files
  (`<value>__<agent>__<time>__<nonce>.json`), a 20 s settle check and the order (time, agent, nonce);
  `--list`, `--release`. Live on the real hub (`OxideSwarm_Agent_Hub` `test_runs/coord_live_2026-11-17.md`):
  two claims 0.2 s apart as agent `owner` got `LIVETEST-1` and `LIVETEST-2`; the two-machine race is covered
  by the unit tests.

### COORD-2: Changed files on the activity record, with an overlap check

- **Evidence:** Pull requests 282, 284 and 286 added lines at the same places in `CHANGELOG.md`,
  `docs/BACKLOG.md` and `docs/README.md`; the conflicts showed only after the first merged.
- **Problem:** An agent cannot see before it starts that another agent changes the same files.
- **Do:** As D2 of design 0087: `activity --branch --pr --files --files-from-git` and
  `activity --overlap`.
- **Done when:** The unit tests pass in CI, and `activity --overlap` names a shared file in a run with
  two agents on the real hub.
- **Implemented (2026-11-17, pull request 295):** `activity --branch --pr --files --files-from-git` and
  `activity --overlap`; live on the real hub it printed the shared-file line for `CHANGELOG.md` and a
  warning for `scripts/agent_bridge.py` (same record).

### COORD-3: One shared place for process lessons

- **Evidence:** Process lessons lived in the coordinator's private Claude memory, which the Windows agents
  cannot read (design 0087, Context).
- **Do:** As D3 of design 0087.
- **Implemented (2026-11-17, pull request 294):** the hub's `AGENT_RULES.md` has section 3 "Lessons"
  (fifteen dated lessons; section 0 tells every agent to read it), and `CLAUDE.md` "Conventions and
  pitfalls" has the lessons that hold without the hub.

## Phase 16: Anima Engine as the first workload

From [design 0089](design/0089-anima-engine-first-workload.md). The owner asked on 2026-11-17 that
OxideSwarm support the owner's simulation project, Anima Engine, as far as it can. Anima Engine runs sweeps
of independent trials that each end in a checksum, and a Rust build too heavy for the owner's development
machine. Order: APP-4 (needs nothing built), VERIFY-1, then AX-1 and AX-2 of Phase 13 with Anima Engine as
their first user (Q88), then APP-5.

### VERIFY-1: Run a task again on other workers and compare its outputs

- **Evidence:** Design 0089 D3. The grid has no way to check a result.
- **Do:** `submit --verify <N>` and `--verify-file`, and the same in the MCP `submit` tool, client side with
  no wire change (Q89).
- **Done when:** The tests of design 0089 D3 pass in CI.
- **Implemented (2026-11-17, pull request 296):** `submit --verify <N>` picks N other workers that fit the task,
  submits it to each pinned with `require:worker=<id>`, and compares the BLAKE3 ids of the declared outputs (or one
  `--verify-file`); `verified` or `mismatch` (exit 4), a `verify` object with `--json`, and the same in MCP `submit`.
  `tests/verify_e2e.rs` (Unix) covers verified, mismatch, too few workers, `--verify-file`, the refusals and MCP.

### VERIFY-2: Replicas of `submit --verify` on the same operating system and architecture by default

- **Evidence:** Design 0089 D6. On 2026-11-17 APP-4 ran the same deterministic Anima Engine trial (same commit,
  same rustc, byte-identical inputs, on the CPU) on Windows x86_64 and on macOS arm64: the final checksums differ
  in all three arms (control: Windows 2546342991, macOS 1859816024), while five repeat runs on Windows agree. A
  replica on the other system reports a false mismatch (exit 4).
- **Do:** By default `submit --verify` picks replicas only among workers whose `os:` and `arch:` tags equal those of
  the worker that ran the first copy (a tag that worker lacks is not compared, with one notice); the too-few error
  names the system and `--verify-any-system`; `--verify-any-system` (MCP `verify_any_system`) allows any matching
  worker. The report names each worker's system.
- **Done when:** Unit tests of `pick_targets` with mixed `os:`/`arch:` tags pass and `tests/verify_e2e.rs` still
  passes.
- **Implemented (2026-11-18, pull request 312):** `crates/cli/src/verify.rs` (`System`, `pick_targets(.., any_system)`), the
  `--verify-any-system` flag, `verify_any_system` in the MCP `submit` tool, and `os` / `arch` per worker in the
  `verify` JSON object.

## Phase 17: benchmarks worth showing

From [design 0093](design/0093-benchmarks-worth-showing.md). The owner asked on 2026-11-18 whether the
benchmarks are strong enough to show. The records are scripted and honest, but none of them measures a
second machine against the fastest machine alone, offload against the laptop itself, or two machines on
different networks. Every record follows the method of design 0093 D1: the plan is committed before the run,
the baseline is the fastest machine alone, and efficiency is measured against the sum of what each machine
does alone. Order: BENCH-2, BENCH-5, BENCH-3, BENCH-4, BENCH-6, then BENCH-7 before LAUNCH-4.

### BENCH-2: The method and the sweep harness for benchmark records

- **Evidence:** Design 0093 D1. `benchmarks/grid/bench_grid.py` measures loopback only and has no local
  baseline; DOC-2 shows a report that compared the cluster with its slowest machine.
- **Do:** Add `benchmarks/sweep/bench_sweep.py` and the synthetic workload `benchmarks/sweep/workload_cpu.py`
  (W1, standard library, work per task as a parameter), and `benchmarks/sweep/README.md` with the method of
  D1. The harness runs a workload locally at 1, half and all logical cores, and through a cluster with
  `submit --json`; it writes the record of D1 rule 5. Add `tests/test_bench_sweep.py` to the CI job
  `agent-bridge`.
- **Done when:** `bench_sweep.py --quick --no-write` runs W1 locally and through a loopback cluster, and the
  helper tests pass in CI on Linux, macOS and Windows.
- **Implemented (pull request 314):** `benchmarks/sweep/bench_sweep.py`, `benchmarks/sweep/workload_cpu.py`
  (W1, a SHA-256 chain whose checksum depends only on seed and units) and `benchmarks/sweep/README.md`;
  `tests/test_bench_sweep.py` (16 helper tests) runs in the CI job `agent-bridge`. The quick run passed on
  Windows loopback (8 of 8 checksums).

### BENCH-8: Find why the two-machine W1 sweep reached 73 % efficiency, not 85 %

- **Evidence:** BENCH-3's record `benchmarks/sweep/results/2026-11-18-w1-two-machines-lan.json`: efficiency
  0.73 (median run 53.7 s against 39.4 s for the sum of both machines alone); runs 2.48, 2.23 and 2.23
  tasks/s. The Windows worker took 60, 70 and 70 of the 120 tasks; the sum of the local rates gives it 64.
  `submit_to_result_p50_s` is 10.9 to 12.2 s, while a task under full load takes about 7.1 s on the Mac
  (10 / 1.412) and 12.3 s on Windows (20 / 1.631).
- **Problem:** The plan expected at least 0.85, and the record does not say where the rest went. Three causes
  fit and none is tested:
  1. Few waves: the plan sized tasks at about 3 s from one process on the Mac, but under full load a task
     takes 7 to 12 s, so 120 tasks are about four waves on 30 slots. When the queue runs dry, the Mac waits
     while the last Windows tasks run, up to one Windows task length (12 s against a 39 s ideal).
  2. No queue for the scheduler: the harness keeps as many submits in flight as there are slots (30), so the
     master never holds a waiting task and the speed-aware placement of design 0060 never has a choice.
  3. Per-task overhead: one CLI process per task (BENCH-1 (a): 26 to 69 ms on loopback), the LAN round trip,
     the sandbox and the Python start; BENCH-5 measures it.
- **Do:** With the same two workers and units, under a plan committed first: (a) 480 tasks instead of 120;
  (b) 120 tasks with `--client-concurrency 120`; (c) each worker alone through the grid (BENCH-3's controls).
  Compare each with the record above; if (b) helps, look at the placement of the last tasks in the master
  log.
- **Done when:** The records are committed and this section names the cause or causes with their share of
  the gap, or the change that closes it.
- **Implemented (2026-11-18, pull request 355):** Plan
  `benchmarks/sweep/plans/2026-11-18-bench-8-w1-efficiency-gap.json`, committed before the first run, with
  two amendments committed before the steps they add. Same units, release 0.4.3, master for the run on the
  Mac; nine records in `benchmarks/sweep/results/` (`2026-11-18-w1-*`), every checksum equal.

  | Step | Record | Tasks/s | Efficiency |
  |---|---|---|---|
  | BENCH-3: both, 120 tasks, 30 in flight | `w1-two-machines-lan` | 2.233 | 0.734 |
  | (b) both, 120 tasks, 120 in flight | `w1-two-machines-lan-cc120` | 2.349 | 0.772 |
  | (a) both, 480 tasks, 30 in flight | `w1-two-machines-lan-480` | 2.301 | 0.756 |
  | (c) Mac worker alone, master on the same Mac | `w1-mac-worker-only-lan` | 1.108 | 0.785 |
  | 4b: the same, `--task-priority normal` | `w1-mac-worker-only-normal-priority-lan` | 1.180 | 0.836 |
  | (c) Windows worker alone, master on the Mac over the LAN | `w1-win-worker-only-lan` | 1.269 | 0.778 |
  | 1c: Windows alone, master on the same machine | `w1-win-loopback-default-priority` | 1.535 | 0.941 |
  | 1d: the same, `--task-priority normal` | `w1-win-loopback-normal-priority` | 1.539 | 0.944 |

  Efficiency of a one-worker step is against that machine alone. Findings:
  1. Combining the machines costs little: both through the grid against the sum of each worker alone
     through the grid (1.108 + 1.269 = 2.377 tasks/s) is 0.94 for BENCH-3, 0.99 for (b) and 0.97 for (a).
  2. Causes 1 and 2 are below the plan's 0.05 threshold: (a) adds 0.022 and (b) adds 0.038 to BENCH-3's
     0.734. The Windows worker took 60 to 70 of 120 tasks in (b) and 300 to 320 of 480 in (a).
  3. Cause 3 is the gap: each worker alone through the grid reaches about 0.78 of its machine alone.
     - Windows: with the master on the same machine it reaches 0.94 with either priority, so the default
       below-normal priority costs nothing there, and the master on another machine over the home LAN costs
       0.16 (median submit-to-result 15.3 to 17.7 s against 12.0 to 13.8 s, about 3 s more per task).
     - Mac: the master is on the same Mac, so the LAN is not involved. `--task-priority normal` adds 0.051,
       at the threshold; the other 0.16 is open. In both Mac steps the warm-up run was faster than the
       measured runs (1.19 then 1.11, and 1.42 then 1.18 tasks/s), which the heat of a fanless machine under
       seven minutes of full load could explain; not tested. BENCH-9 splits the cost with per-task times;
       Q93 asks whether the default priority should change.
  Deviations: step 1b (the Windows worker with `--task-priority normal` over the LAN) was run once and is
  void, because the worker restarted during its runs; a second attempt was refused because a cargo test of
  another project had started on the Mac. Amendment 2 replaced it with 1c and 1d on the Windows machine
  alone. Steps 2 and 3 ended their grid runs before that cargo test started. A second attempt of 4b was
  needed for the same reason; the void records are not committed.
