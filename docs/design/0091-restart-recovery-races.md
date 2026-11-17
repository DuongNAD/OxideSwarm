# 0091: Two races in worker recovery that made `master_restart_recovery` flaky (`TEST-23`)

Status: Implemented
Created: 2026-11-17    Updated: 2026-11-17

## Summary

Looping the `master_restart_recovery` tests under CPU load found two races in the worker. First, a task whose output upload was cut by the master going away was buffered as failed ("uploading output files to the master failed: the connection to the master ended") instead of keeping its outputs for the resume. Second, the lock that keeps a sandbox from being swept could be won on a lock file that had just been unlinked, so a second opener could also win it. Both are fixed in the worker; no wire, CLI or file-format change.

## Context

`TEST-23` started from a Windows CI failure whose result said the task did not produce `out.txt`. On macOS, 192 runs of the outbox and restart tests with eight copies in parallel and twenty busy loops gave 3 failures of `test_worker_disconnected_outbox_uploads_output_blobs_and_flushes_result_on_resume` (all "the connection to the master ended"). The earlier lead (pull request 288) saw one "out.txt: No such file or directory" in 140 runs of the whole binary. The tests do not set a sandbox base, so all copies, and the workers inside one copy, share `<tmp>/rusty_grid-<uid>/sandboxes`.

## Design

### D1 A session that ended during an upload keeps the buffered result

In `client/tasks.rs`, a finished task with outputs first buffers its result for a resume (`pending_resume_outbox`), then uploads the outputs. When the connection ends during that upload, `BlobClient` is closed and `upload_task_outputs` rewrites the local result into a failure. The code then replaced the buffered result with it, and the resume sent the failure. Now the buffered result is replaced only when the blob client is still open; otherwise the buffered result (with its outputs) stays and the resume uploads them, as design 0044 intended.

### D2 A task lock is only kept when its file still has its name

`open_and_try_lock_task` opens `<base>/.locks/<task>.lock` and then locks it. The orphan-lock sweep and `Sandbox::destroy` unlink the file while they hold the lock. An opener that had opened the file just before the unlink then locked a file with no name; the next opener created a new file and locked it too, and a sweep that found the sandbox directory with a free lock removed the directory of a running task. Now, after the lock is taken, the path must still name the same file (device and inode on Unix, existence on Windows); otherwise the file is closed and opened again. This was reproduced deterministically with a hook between open and lock (`open_and_try_lock_task_with`); it was not shown to be the cause of the one Windows failure.

## Test plan

- `sandbox::tests::test_task_lock_taken_on_an_unlinked_file_is_not_kept` fails without D2.
- The outbox and restart tests in `tests/master_restart_recovery.rs` looped under load (see the pull request for counts). D1 has no deterministic test of its own: it needs a connection that ends inside an upload.

## Alternatives considered

- Give every test worker its own sandbox base. It hides D2 and leaves two real workers on one machine exposed, so it was not done.
