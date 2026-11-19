//! Bounded stream capture, cancellation helpers, output artifact harvesting, and blob staging.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use rusty_grid_core::task::{
    sha256_digest, Bytes, BytesMut, OutputArtifact, Task, TaskId, TaskResult,
    MAX_AGGREGATE_ARTIFACT_BYTES, MAX_OUTPUT_ARTIFACT_BYTES,
};

use crate::sandbox::{sanitize_relative_path, Sandbox, SandboxError};
use crate::staging::{collect_outputs_from_dir_pinned, materialize_inputs};

use super::{TaskRunner, EXIT_CODE_CANCELLED, EXIT_CODE_GENERAL_ERROR, EXIT_CODE_SUCCESS};

/// The notice inserted between the preserved head and tail when a capture exceeds its size limit.
pub(super) const TRUNCATION_NOTICE: &[u8] =
    b"\n[rusty_grid: output truncated after exceeding size limit]\n";

/// Bytes captured from one output stream so far. Shared with the reader so that what has been
/// read is still there when the reader has to be abandoned (a timeout, or a background process
/// that never closes the pipe).
///
/// Keeps the first `max_bytes / 2` bytes in `head` and a rolling ring buffer of the last
/// `max_bytes - max_bytes / 2` bytes in `tail`, so both startup diagnostics and final test/build
/// summaries are preserved when a stream overflows `max_bytes`.
#[derive(Default)]
struct StreamCapture {
    head: BytesMut,
    tail: std::collections::VecDeque<u8>,
    truncated: bool,
}

impl StreamCapture {
    fn push(&mut self, chunk: &[u8], max_bytes: usize) {
        if chunk.is_empty() {
            return;
        }
        let head_cap = max_bytes / 2;
        let tail_cap = max_bytes - head_cap;

        let head_room = head_cap.saturating_sub(self.head.len());
        let to_head = chunk.len().min(head_room);
        if to_head > 0 {
            self.head.extend_from_slice(&chunk[..to_head]);
        }

        let rest = &chunk[to_head..];
        if rest.is_empty() {
            return;
        }
        if tail_cap == 0 {
            self.truncated = true;
            return;
        }
        if rest.len() >= tail_cap {
            if self.tail.len() + rest.len() > tail_cap {
                self.truncated = true;
            }
            self.tail.clear();
            self.tail.extend(&rest[rest.len() - tail_cap..]);
        } else {
            let overflow = (self.tail.len() + rest.len()).saturating_sub(tail_cap);
            if overflow > 0 {
                self.truncated = true;
                self.tail.drain(..overflow);
            }
            self.tail.extend(rest);
        }
    }

    fn finish(mut self) -> (Bytes, bool) {
        if self.truncated {
            self.head.extend_from_slice(TRUNCATION_NOTICE);
        }
        let (s1, s2) = self.tail.as_slices();
        self.head.extend_from_slice(s1);
        self.head.extend_from_slice(s2);
        (self.head.freeze().into(), self.truncated)
    }
}

type SharedCapture = Arc<Mutex<StreamCapture>>;

/// Whether a failed metadata query means the declared output is simply not there.
fn output_is_missing(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
    )
}

/// The error for a declared output whose metadata could not be read. Only a file that is not
/// there is "not produced"; any other error (access denied, a file being deleted, an invalid
/// name) says what the system answered, because the file may well exist.
fn output_stat_error_message(expected: &str, e: &std::io::Error) -> String {
    if output_is_missing(e) {
        format!("the task did not produce '{expected}'")
    } else {
        format!("could not examine '{expected}': {e}")
    }
}

fn lock_capture(capture: &SharedCapture) -> std::sync::MutexGuard<'_, StreamCapture> {
    capture
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Reads `reader` to EOF into `capture`, keeping at most `max_bytes` and draining the rest.
async fn read_into_capture<R: tokio::io::AsyncRead + Unpin>(
    mut reader: R,
    max_bytes: usize,
    capture: SharedCapture,
) {
    use tokio::io::AsyncReadExt;

    let mut scratch = [0u8; 8192];
    loop {
        match reader.read(&mut scratch).await {
            Ok(0) => break, // EOF reached
            Ok(n) => lock_capture(&capture).push(&scratch[..n], max_bytes),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => {
                warn!(error = %e, "Task output stream read ended with error");
                break;
            }
        }
    }
}

/// Reads an async stream into a bounded memory buffer without pipe deadlocks.
///
/// Continues draining the pipe to EOF even after the buffer is filled, ensuring
/// the child process does not block on write operations.
pub async fn read_bounded_stream<R: tokio::io::AsyncRead + Unpin>(
    reader: R,
    max_bytes: usize,
) -> (Bytes, bool) {
    let capture = SharedCapture::default();
    read_into_capture(reader, max_bytes, Arc::clone(&capture)).await;
    let captured = std::mem::take(&mut *lock_capture(&capture));
    captured.finish()
}

/// The two output readers of a supervised process, with the buffers they fill.
pub(super) struct OutputReaders {
    stdout: Option<JoinHandle<()>>,
    stderr: Option<JoinHandle<()>>,
    stdout_capture: SharedCapture,
    stderr_capture: SharedCapture,
}

impl OutputReaders {
    pub(super) fn spawn<Out, Err>(
        stdout: Option<Out>,
        stderr: Option<Err>,
        max_bytes: usize,
    ) -> Self
    where
        Out: tokio::io::AsyncRead + Unpin + Send + 'static,
        Err: tokio::io::AsyncRead + Unpin + Send + 'static,
    {
        let stdout_capture = SharedCapture::default();
        let stderr_capture = SharedCapture::default();
        Self {
            stdout: stdout.map(|pipe| {
                tokio::spawn(read_into_capture(
                    pipe,
                    max_bytes,
                    Arc::clone(&stdout_capture),
                ))
            }),
            stderr: stderr.map(|pipe| {
                tokio::spawn(read_into_capture(
                    pipe,
                    max_bytes,
                    Arc::clone(&stderr_capture),
                ))
            }),
            stdout_capture,
            stderr_capture,
        }
    }

    /// Waits until both pipes reached EOF, for at most `grace`. Returns whether they did.
    pub(super) async fn wait(&mut self, grace: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + grace;
        for slot in [&mut self.stdout, &mut self.stderr] {
            if let Some(reader) = slot.as_mut() {
                if tokio::time::timeout_at(deadline, reader).await.is_err() {
                    return false;
                }
                *slot = None;
            }
        }
        true
    }

    /// Stops the readers (a reader that is still waiting for EOF is abandoned) and returns what
    /// they captured: `(stdout, stderr)`.
    pub(super) fn finish(mut self) -> (Bytes, Bytes) {
        self.abort();
        let take = |capture: &SharedCapture| std::mem::take(&mut *lock_capture(capture)).finish().0;
        (take(&self.stdout_capture), take(&self.stderr_capture))
    }

    pub(super) fn abort(&mut self) {
        for reader in [self.stdout.take(), self.stderr.take()]
            .into_iter()
            .flatten()
        {
            reader.abort();
        }
    }
}

impl Drop for OutputReaders {
    /// A run that is dropped mid-flight must not leave readers behind: a daemon that escaped the
    /// process tree could hold the pipes (and these tasks) open forever.
    fn drop(&mut self) {
        self.abort();
    }
}

/// Resolves with the reason once the task is cancelled (by the Master's `CancelTask` or by the
/// worker itself). It never resolves when there is no cancel channel, or when the sender was
/// dropped without sending: a sender that vanishes is not a cancellation, and treating it as one
/// used to kill healthy tasks and report them as cancelled.
pub(crate) async fn cancellation(slot: &mut Option<oneshot::Receiver<String>>) -> String {
    let Some(receiver) = slot.as_mut() else {
        return std::future::pending().await;
    };
    let outcome = receiver.await;
    // A completed receiver must not be polled again.
    *slot = None;
    match outcome {
        Ok(reason) => reason,
        Err(_) => std::future::pending().await,
    }
}

/// Checks whether `slot` has already received a cancellation signal without blocking.
/// As with [`cancellation`], a sender dropped without sending is ignored and clears `slot`.
pub(crate) fn check_cancelled_now(slot: &mut Option<oneshot::Receiver<String>>) -> Option<String> {
    let receiver = slot.as_mut()?;
    match receiver.try_recv() {
        Ok(reason) => {
            *slot = None;
            Some(reason)
        }
        Err(oneshot::error::TryRecvError::Closed) => {
            *slot = None;
            None
        }
        Err(oneshot::error::TryRecvError::Empty) => None,
    }
}

/// Aborts the wrapped task when dropped.
pub(super) struct AbortOnDrop(pub(super) JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// `bytes` followed by `note`; a newline first when `bytes` is non-empty and does not end in one
/// and `note` does not start with one.
pub(crate) fn append_note(bytes: Bytes, note: &str) -> Bytes {
    let mut combined = BytesMut::from(bytes.as_bytes());
    if !combined.is_empty() && !combined.ends_with(b"\n") && !note.starts_with('\n') {
        combined.extend_from_slice(b"\n");
    }
    combined.extend_from_slice(note.as_bytes());
    combined.freeze().into()
}

/// Reads an output file produced by a task process.
///
/// Opens the path first without following symbolic links or blocking on FIFOs
/// (`O_NOFOLLOW | O_NONBLOCK` on Unix; `FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS`
/// on Windows), inspects `file.metadata()` on the open handle to refuse symbolic links/reparse
/// points, non-regular files (such as FIFOs, character devices, or directories), and files whose
/// size exceeds `MAX_FRAME_SIZE` (64 MiB = 67,108,864 bytes) before reading, and bounds the read
/// with `AsyncReadExt::take(MAX_FRAME_SIZE as u64 + 1)` so a file that grows past the limit while
/// being read is also refused.
///
/// Returns `Ok(Some(Vec<u8>))` if the regular file exists, is within the frame limit,
/// and is read successfully.
/// Returns `Ok(None)` only if the file was not found (`ErrorKind::NotFound`).
/// Returns `Err(io::Error)` if reading failed for any other reason (e.g. symbolic link,
/// not a regular file, exceeds `MAX_FRAME_SIZE`, or permission denied).
#[cfg(any(feature = "3d", test))]
pub(crate) async fn read_output_file(path: &Path) -> std::io::Result<Option<Vec<u8>>> {
    let mut opts = tokio::fs::OpenOptions::new();
    opts.read(true);
    #[cfg(unix)]
    {
        opts.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
        opts.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS);
    }

    let file = match opts.open(path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        #[cfg(unix)]
        Err(e) if e.raw_os_error() == Some(libc::ELOOP) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "output path is a symbolic link (symbolic links are prohibited for security isolation): {}",
                    path.display()
                ),
            ));
        }
        Err(e) => return Err(e),
    };

    let meta = file.metadata().await?;

    #[cfg(windows)]
    let is_reparse = {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
        (meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT) != 0
    };
    #[cfg(not(windows))]
    let is_reparse = false;

    if meta.file_type().is_symlink() || is_reparse {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "output path is a symbolic link (symbolic links are prohibited for security isolation): {}",
                path.display()
            ),
        ));
    }

    if !meta.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("output path is not a regular file: {}", path.display()),
        ));
    }

    let max_bytes = rusty_grid_core::protocol::MAX_FRAME_SIZE as u64;
    let size = meta.len();
    if size > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "output file '{}' is {} bytes, exceeding frame limit of {} bytes (64 MiB)",
                path.display(),
                size,
                max_bytes
            ),
        ));
    }

    let mut data = Vec::with_capacity(size as usize);
    use tokio::io::AsyncReadExt;
    file.take(max_bytes + 1).read_to_end(&mut data).await?;

    if (data.len() as u64) > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "output file '{}' is more than {} bytes, exceeding frame limit of {} bytes (64 MiB)",
                path.display(),
                max_bytes,
                max_bytes
            ),
        ));
    }

    Ok(Some(data))
}

impl TaskRunner {
    /// Extracts declared expected output artifacts from the sandbox before destruction.
    pub async fn harvest_output_artifacts(
        &self,
        sandbox: &Sandbox,
        expected_outputs: &[String],
        task_id: TaskId,
        process_failed: bool,
    ) -> Result<Vec<OutputArtifact>, String> {
        self.harvest_output_artifacts_from_dir(
            sandbox.path(),
            expected_outputs,
            task_id,
            process_failed,
        )
        .await
    }

    /// Extracts declared expected output artifacts from `root_dir` (either a task sandbox or a
    /// workspace tree directory).
    pub(super) async fn harvest_output_artifacts_from_dir(
        &self,
        root_dir: &Path,
        expected_outputs: &[String],
        task_id: TaskId,
        process_failed: bool,
    ) -> Result<Vec<OutputArtifact>, String> {
        let mut collected = Vec::new();
        let mut total_bytes: usize = 0;

        for expected in expected_outputs {
            let target = match sanitize_relative_path(root_dir, Path::new(expected)) {
                Ok(p) => p,
                Err(err) => {
                    warn!(task_id = %task_id, path = %expected, error = %err, "Ignoring illegal expected output path");
                    return Err(format!(
                        "output artifact '{expected}' resolves outside sandbox root"
                    ));
                }
            };

            // Query symlink metadata without following links
            let meta = match tokio::fs::symlink_metadata(&target).await {
                Ok(m) => m,
                Err(e) => {
                    if !output_is_missing(&e) {
                        warn!(task_id = %task_id, path = %expected, error = %e, "Could not examine a declared output artifact");
                    }
                    if !process_failed {
                        return Err(output_stat_error_message(expected, &e));
                    }
                    debug!(task_id = %task_id, path = %expected, error = %e, "Declared output artifact was not produced by failed task");
                    continue;
                }
            };

            // Strictly reject symlinks to prevent sandbox exfiltration
            if meta.is_symlink() {
                warn!(task_id = %task_id, path = %expected, "Skipping output artifact: symlinks are prohibited for security isolation");
                return Err(format!(
                    "output artifact '{expected}' is a symbolic link (symbolic links are prohibited for security isolation)"
                ));
            }

            if !meta.is_file() {
                warn!(task_id = %task_id, path = %expected, "Skipping output artifact: path is not a regular file");
                return Err(format!(
                    "output artifact '{expected}' is not a regular file"
                ));
            }

            let size = meta.len() as usize;
            if size > MAX_OUTPUT_ARTIFACT_BYTES {
                warn!(task_id = %task_id, path = %expected, size = size, "Skipping output artifact: exceeds single file limit");
                return Err(format!(
                    "output artifact '{expected}' is {size} bytes; exceeds single file limit of 10 MiB"
                ));
            }

            if total_bytes + size > MAX_AGGREGATE_ARTIFACT_BYTES {
                warn!(task_id = %task_id, path = %expected, size = size, "Halting artifact collection: aggregate size exceeds 32 MiB limit");
                return Err(format!(
                    "output artifact '{expected}' causes aggregate size to exceed limit of 32 MiB"
                ));
            }

            // Ensure canonical path resolves strictly within the sandbox root
            if let (Ok(canonical_target), Ok(canonical_sandbox)) = (
                tokio::fs::canonicalize(&target).await,
                tokio::fs::canonicalize(root_dir).await,
            ) {
                if !canonical_target.starts_with(&canonical_sandbox) {
                    warn!(task_id = %task_id, path = %expected, "Skipping output artifact: path resolves outside sandbox root");
                    return Err(format!(
                        "output artifact '{expected}' resolves outside sandbox root"
                    ));
                }
            }

            // Read artifact content
            match tokio::fs::read(&target).await {
                Ok(data) => {
                    let sha256 = sha256_digest(&data);
                    total_bytes += size;
                    collected.push(OutputArtifact {
                        path: expected.clone(),
                        size: meta.len(),
                        sha256,
                        data: Bytes::from(data),
                    });
                }
                Err(err) => {
                    warn!(task_id = %task_id, path = %expected, error = %err, "Failed to read output artifact file");
                    return Err(format!(
                        "failed to read output artifact '{expected}': {err}"
                    ));
                }
            }
        }

        Ok(collected)
    }

    /// Harvests declared expected output artifacts from the sandbox and updates `TaskResult`.
    ///
    /// If harvesting fails (e.g. missing artifact, oversize, symlink, traversal), marks `result`
    /// as failed with `EXIT_CODE_GENERAL_ERROR`, appends diagnostic notes to stderr and error.
    pub(super) async fn harvest_and_apply_artifacts(
        &self,
        sandbox: &Sandbox,
        task: &Task,
        result: TaskResult,
        process_failed: bool,
    ) -> TaskResult {
        self.harvest_and_apply_artifacts_from_dir(sandbox.path(), task, result, process_failed)
            .await
    }

    pub(super) async fn harvest_and_apply_artifacts_from_dir(
        &self,
        root_dir: &Path,
        task: &Task,
        mut result: TaskResult,
        process_failed: bool,
    ) -> TaskResult {
        if task.expected_outputs.is_empty() {
            return result;
        }

        match self
            .harvest_output_artifacts_from_dir(
                root_dir,
                &task.expected_outputs,
                task.id,
                process_failed,
            )
            .await
        {
            Ok(artifacts) => {
                result.artifacts = artifacts;
            }
            Err(e) => {
                let message = format!("Output files: {e}");
                warn!(task_id = %task.id, error = %e, "Harvesting output artifacts failed");
                if result.exit_code == EXIT_CODE_SUCCESS {
                    result.exit_code = EXIT_CODE_GENERAL_ERROR;
                }
                result.stderr = append_note(result.stderr, &format!("[rusty_grid: {message}]\n"));
                result.error = Some(match result.error.take() {
                    Some(error) => format!("{error}; {message}"),
                    None => message,
                });
            }
        }
        result
    }

    /// Helper to stage files into sandbox before task execution.
    pub async fn prepare_staged_files(
        &self,
        sandbox: &Sandbox,
        task: &Task,
    ) -> Result<(), SandboxError> {
        if task.staged_files.is_empty() {
            return Ok(());
        }
        sandbox.stage_files(&task.staged_files).await
    }

    /// Places the task's blob inputs (`Task::inputs`) into the sandbox from the blob cache. The
    /// worker fetched them before the run; a blob that is not cached fails the task.
    pub async fn prepare_blob_inputs(&self, sandbox: &Sandbox, task: &Task) -> Result<(), String> {
        if task.inputs.is_empty() {
            return Ok(());
        }
        let cache = self.ensure_blob_cache().await?;
        let root = sandbox.path().to_path_buf();
        let inputs = task.inputs.clone();
        let placed =
            tokio::task::spawn_blocking(move || materialize_inputs(&root, &inputs, &cache))
                .await
                .map_err(|e| e.to_string())??;
        debug!(task_id = %task.id, files = placed, "Placed input files into the sandbox");
        Ok(())
    }

    /// Collects the files `Task::output_globs` and `extra_patterns` select into the blob cache
    /// and lists them in `result.outputs`. A cancelled run brings nothing back; a failure to
    /// collect fails the task, with the reason in its error and stderr.
    pub async fn collect_blob_outputs(
        &self,
        sandbox: &Sandbox,
        task: &Task,
        result: TaskResult,
        extra_patterns: &[String],
    ) -> TaskResult {
        self.collect_blob_outputs_from_dir(sandbox.path(), task, result, extra_patterns, true)
            .await
    }

    pub(super) async fn collect_blob_outputs_from_dir(
        &self,
        root_dir: &Path,
        task: &Task,
        mut result: TaskResult,
        extra_patterns: &[String],
        allow_move: bool,
    ) -> TaskResult {
        let patterns: Vec<String> = task
            .output_globs
            .iter()
            .chain(extra_patterns)
            .cloned()
            .collect();
        if patterns.is_empty() || result.exit_code == EXIT_CODE_CANCELLED {
            return result;
        }
        let collected = match self.ensure_blob_cache().await {
            Ok(cache) => {
                let root = root_dir.to_path_buf();
                let limits = self.config.output_limits;
                let process_failed = !result.is_success();
                let task_id = task.id;
                tokio::task::spawn_blocking(move || {
                    let (outputs, pin) = collect_outputs_from_dir_pinned(
                        &root,
                        &patterns,
                        &cache,
                        limits,
                        process_failed,
                        allow_move,
                    )?;
                    cache.keep_task_outputs(task_id, pin);
                    Ok(outputs)
                })
                .await
                .unwrap_or_else(|e| Err(e.to_string()))
            }
            Err(e) => Err(e),
        };
        match collected {
            Ok(outputs) => result.outputs = outputs,
            Err(e) => {
                let message = format!("Output files: {e}");
                warn!(task_id = %task.id, error = %e, "Collecting output files failed");
                if result.exit_code == EXIT_CODE_SUCCESS {
                    result.exit_code = EXIT_CODE_GENERAL_ERROR;
                }
                result.stderr = append_note(result.stderr, &format!("[rusty_grid: {message}]\n"));
                result.error = Some(match result.error.take() {
                    Some(error) => format!("{error}; {message}"),
                    None => message,
                });
            }
        }
        result
    }
}

#[cfg(test)]
mod output_stat_error_tests {
    use super::*;
    use std::io::{Error, ErrorKind};

    #[test]
    fn a_file_that_is_not_there_is_not_produced() {
        for kind in [ErrorKind::NotFound, ErrorKind::NotADirectory] {
            let e = Error::from(kind);
            assert!(output_is_missing(&e), "{kind:?}");
            assert_eq!(
                output_stat_error_message("out.txt", &e),
                "the task did not produce 'out.txt'"
            );
        }
    }

    #[test]
    fn any_other_error_names_what_the_system_answered() {
        for kind in [ErrorKind::PermissionDenied, ErrorKind::InvalidInput] {
            let e = Error::new(kind, "the system said no");
            assert!(!output_is_missing(&e), "{kind:?}");
            let message = output_stat_error_message("out.txt", &e);
            assert_eq!(message, "could not examine 'out.txt': the system said no");
            assert!(
                !message.contains("did not produce"),
                "the file may exist: {message}"
            );
        }
    }

    #[tokio::test]
    async fn a_declared_output_that_is_missing_still_fails_a_successful_task_only() {
        use crate::runner::RunnerConfig;
        use crate::sandbox::SandboxConfig;
        use rusty_grid_core::capabilities::WorkerCapabilities;
        use rusty_grid_core::task::TaskId;

        let dir = tempfile::tempdir().unwrap();
        let runner = TaskRunner::new(
            uuid::Uuid::new_v4(),
            WorkerCapabilities::default(),
            RunnerConfig::new(SandboxConfig::new(dir.path().join("base"))),
        );
        let id = TaskId::from_uuid(uuid::Uuid::new_v4());
        let wanted = vec!["out.txt".to_string()];

        let err = runner
            .harvest_output_artifacts_from_dir(dir.path(), &wanted, id, false)
            .await
            .unwrap_err();
        assert_eq!(err, "the task did not produce 'out.txt'");
        let none = runner
            .harvest_output_artifacts_from_dir(dir.path(), &wanted, id, true)
            .await
            .unwrap();
        assert!(none.is_empty());
    }
}
