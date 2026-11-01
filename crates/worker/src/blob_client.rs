//! The worker's side of blob transfer (`rusty_grid_core::blob_transfer`): fetching the input
//! blobs of a task that its cache lacks before the run, and uploading the output blobs the Master
//! lacks after it. Design record: `docs/design/0006-content-addressed-data-plane.md`.
//!
//! A [`BlobClient`] belongs to one session: it exists only while the worker is connected to a
//! Master that announced `master_features::BLOBS`, its requests go out on the session's data
//! queue (the data stream of a QUIC connection, so transfers never delay heartbeats on the
//! control stream), and the session loop hands it every `MasterMessage::BlobReply`. When the
//! session ends, waiting requests fail at once.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, Semaphore};
use tracing::debug;

use rusty_grid_core::blob::{BlobId, InputSource, TreeManifest, MAX_MANIFEST_BYTES};
use rusty_grid_core::blob_transfer::{
    download_blob, for_each_concurrently, missing_blobs, upload_blob, BlobChannel, BlobError,
    BlobReply, BlobRequest, MAX_IN_FLIGHT_REQUESTS,
};
use rusty_grid_core::protocol::WorkerMessage;
use rusty_grid_core::task::{Task, TaskResult};

use crate::blob_cache::{BlobCache, CachePin};
use crate::runner::{append_note, EXIT_CODE_GENERAL_ERROR};

/// How long one blob request may wait for its reply before the transfer is given up.
pub const BLOB_REPLY_TIMEOUT: Duration = Duration::from_secs(120);

/// Sends blob requests to the Master over one session and matches the replies.
pub struct BlobClient {
    outbound: mpsc::Sender<WorkerMessage>,
    in_flight: Semaphore,
    pending: Mutex<HashMap<u64, oneshot::Sender<BlobReply>>>,
    next_id: AtomicU64,
    closed: AtomicBool,
}

impl BlobClient {
    /// A client that sends its requests into `outbound` (the session's data queue).
    pub fn new(outbound: mpsc::Sender<WorkerMessage>) -> Arc<Self> {
        Arc::new(Self {
            outbound,
            in_flight: Semaphore::new(MAX_IN_FLIGHT_REQUESTS),
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            closed: AtomicBool::new(false),
        })
    }

    fn pending(&self) -> std::sync::MutexGuard<'_, HashMap<u64, oneshot::Sender<BlobReply>>> {
        self.pending.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Hands the reply to request `request_id` to whoever waits for it. False when nobody does
    /// (it timed out, or the id is unknown).
    pub fn deliver(&self, request_id: u64, reply: BlobReply) -> bool {
        match self.pending().remove(&request_id) {
            Some(waiter) => waiter.send(reply).is_ok(),
            None => false,
        }
    }

    /// Fails every request still waiting, and every later one (the session ended).
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.in_flight.close();
        self.pending().clear();
    }
}

impl BlobChannel for BlobClient {
    async fn call(&self, request: BlobRequest) -> Result<BlobReply, BlobError> {
        let _permit =
            self.in_flight.acquire().await.map_err(|_| {
                BlobError::Transport("the connection to the master ended".to_string())
            })?;
        if self.closed.load(Ordering::Acquire) {
            return Err(BlobError::Transport(
                "the connection to the master ended".to_string(),
            ));
        }
        let request_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending().insert(request_id, tx);
        let message = WorkerMessage::BlobRequest {
            request_id,
            request,
        };
        if self.outbound.send(message).await.is_err() {
            self.pending().remove(&request_id);
            return Err(BlobError::Transport(
                "the connection to the master ended".to_string(),
            ));
        }
        match tokio::time::timeout(BLOB_REPLY_TIMEOUT, rx).await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(_)) => Err(BlobError::Transport(
                "the connection to the master ended".to_string(),
            )),
            Err(_) => {
                self.pending().remove(&request_id);
                Err(BlobError::Transport(format!(
                    "the master did not answer a blob request within {}s",
                    BLOB_REPLY_TIMEOUT.as_secs()
                )))
            }
        }
    }
}

/// Makes sure every blob `task`'s inputs need is in `cache`, fetching the missing ones through
/// `client`, and pins them all for the run (the returned guard). Tree manifests are fetched
/// first, then the files they list.
pub async fn fetch_task_inputs(
    task: &Task,
    client: Option<&BlobClient>,
    cache: &Arc<BlobCache>,
) -> Result<CachePin, String> {
    let mut pin = cache.pin([]);
    let top: Vec<(BlobId, u64)> = task
        .inputs
        .iter()
        .map(|input| match input.source {
            InputSource::File { id, size, .. } => (id, size),
            InputSource::Tree {
                manifest,
                manifest_size,
            } => (manifest, manifest_size),
        })
        .collect();
    pin.extend(top.iter().map(|(id, _)| *id));
    fetch_missing(top, client, cache).await?;

    for input in &task.inputs {
        let InputSource::Tree { manifest, .. } = input.source else {
            continue;
        };
        let reader = Arc::clone(cache);
        let bytes = tokio::task::spawn_blocking(move || {
            reader.read_verified(&manifest, MAX_MANIFEST_BYTES)
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| format!("input '{}': directory manifest: {e}", input.path))?;
        let tree =
            TreeManifest::decode(&bytes).map_err(|e| format!("input '{}': {e}", input.path))?;
        let files: Vec<(BlobId, u64)> = tree.files().iter().map(|f| (f.id, f.size)).collect();
        pin.extend(files.iter().map(|(id, _)| *id));
        fetch_missing(files, client, cache)
            .await
            .map_err(|e| format!("input '{}': {e}", input.path))?;
    }
    Ok(pin)
}

async fn fetch_missing(
    blobs: Vec<(BlobId, u64)>,
    client: Option<&BlobClient>,
    cache: &Arc<BlobCache>,
) -> Result<(), String> {
    let mut seen = HashSet::new();
    let missing: Vec<(BlobId, u64)> = blobs
        .into_iter()
        .filter(|(id, _)| seen.insert(*id) && !cache.contains(id))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    let Some(client) = client else {
        return Err(format!(
            "{} input blobs are not in this worker's cache, and it is not connected to a master with a blob store to fetch them from",
            missing.len()
        ));
    };
    let count = missing.len();
    let bytes = for_each_concurrently(missing, |(id, size)| fetch_one(client, cache, id, size))
        .await
        .map_err(|e| format!("fetching inputs from the master failed: {e}"))?;
    debug!(blobs = count, bytes, "Fetched input blobs from the master");
    Ok(())
}

async fn fetch_one(
    client: &BlobClient,
    cache: &Arc<BlobCache>,
    id: BlobId,
    size: u64,
) -> Result<u64, BlobError> {
    let tmp = cache.temp_path();
    let outcome = async {
        let mut file = tokio::fs::File::create(&tmp).await?;
        let bytes = download_blob(client, id, Some(size), &mut file).await?;
        file.sync_all().await?;
        drop(file);
        cache.adopt(&tmp, id, size)?;
        Ok(bytes)
    }
    .await;
    if outcome.is_err() {
        let _ = tokio::fs::remove_file(&tmp).await;
    }
    outcome
}

/// Uploads the output blobs of `result` that the Master lacks, from `cache`. Without a client
/// (the Master has no blob store) outputs the task asked for fail it, and outputs it did not ask
/// for (a `RustCompilation`'s artifacts) are dropped with a note in stderr. A failed upload fails
/// the task: its outputs could not be handed to the client.
pub async fn upload_task_outputs(
    task: &Task,
    result: &mut TaskResult,
    client: Option<&BlobClient>,
    cache: &Arc<BlobCache>,
) {
    let _task_pin = cache.take_task_outputs(task.id);
    if result.outputs.is_empty() {
        return;
    }
    let Some(client) = client else {
        let count = result.outputs.len();
        result.outputs.clear();
        let note = format!(
            "{count} output files were not returned: the master has no blob store (start it with `oxideswarm master`, which keeps one)"
        );
        if task.output_globs.is_empty() {
            result.stderr = append_note(std::mem::take(&mut result.stderr), &note);
        } else {
            fail(result, note);
        }
        return;
    };

    let mut seen = HashSet::new();
    let blobs: Vec<(BlobId, u64)> = result
        .outputs
        .iter()
        .filter(|f| seen.insert(f.id))
        .map(|f| (f.id, f.size))
        .collect();
    let _pin = cache.pin(blobs.iter().map(|(id, _)| *id));
    let ids: Vec<BlobId> = blobs.iter().map(|(id, _)| *id).collect();
    let uploaded = async {
        let missing: HashSet<BlobId> = missing_blobs(client, &ids).await?.into_iter().collect();
        let todo: Vec<(BlobId, u64)> = blobs
            .into_iter()
            .filter(|(id, _)| missing.contains(id))
            .collect();
        for_each_concurrently(todo, |(id, size)| upload_one(client, cache, id, size)).await
    }
    .await;
    match uploaded {
        Ok(bytes) => debug!(task_id = %task.id, bytes, "Uploaded output blobs to the master"),
        Err(e) => {
            result.outputs.clear();
            fail(
                result,
                format!("uploading output files to the master failed: {e}"),
            );
        }
    }
}

async fn upload_one(
    client: &BlobClient,
    cache: &BlobCache,
    id: BlobId,
    size: u64,
) -> Result<u64, BlobError> {
    let file = tokio::fs::File::open(cache.object_path(&id))
        .await
        .map_err(|e| {
            BlobError::Transport(format!(
                "output blob {id} is no longer in the worker's cache ({e}); raise `worker --blob-cache-mb`"
            ))
        })?;
    upload_blob(client, id, size, file).await
}

fn fail(result: &mut TaskResult, message: String) {
    if result.exit_code == 0 {
        result.exit_code = EXIT_CODE_GENERAL_ERROR;
    }
    result.stderr = append_note(std::mem::take(&mut result.stderr), &message);
    result.error = Some(match result.error.take() {
        Some(error) => format!("{error}; {message}"),
        None => message,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::blob::{BlobFile, TaskInput};
    use rusty_grid_core::task::{TaskRequirements, TaskSpec};
    use uuid::Uuid;

    /// Serves the client's requests from an in-memory map, the way the Master's store would.
    fn spawn_master(
        mut requests: mpsc::Receiver<WorkerMessage>,
        client: Arc<BlobClient>,
        blobs: Arc<Mutex<HashMap<BlobId, Vec<u8>>>>,
        reads: Arc<AtomicU64>,
    ) {
        tokio::spawn(async move {
            let mut uploads: HashMap<BlobId, Vec<u8>> = HashMap::new();
            while let Some(WorkerMessage::BlobRequest {
                request_id,
                request,
            }) = requests.recv().await
            {
                let reply = match request {
                    BlobRequest::Missing { ids } => {
                        let blobs = blobs.lock().unwrap();
                        BlobReply::Missing {
                            ids: ids
                                .into_iter()
                                .filter(|id| !blobs.contains_key(id))
                                .collect(),
                        }
                    }
                    BlobRequest::Read {
                        id,
                        offset,
                        max_len,
                    } => {
                        reads.fetch_add(1, Ordering::Relaxed);
                        match blobs.lock().unwrap().get(&id) {
                            Some(data) => {
                                let end = (offset as usize + max_len as usize).min(data.len());
                                BlobReply::Chunk {
                                    id,
                                    offset,
                                    total_size: data.len() as u64,
                                    data: data[offset as usize..end].to_vec().into(),
                                }
                            }
                            None => BlobReply::NotFound { id },
                        }
                    }
                    BlobRequest::Write {
                        id,
                        total_size,
                        data,
                        ..
                    } => {
                        let buf = uploads.entry(id).or_default();
                        buf.extend_from_slice(data.as_bytes());
                        let received = buf.len() as u64;
                        if received == total_size {
                            blobs
                                .lock()
                                .unwrap()
                                .insert(id, uploads.remove(&id).unwrap());
                        }
                        BlobReply::Written {
                            id,
                            received,
                            complete: received == total_size,
                        }
                    }
                };
                client.deliver(request_id, reply);
            }
        });
    }

    fn task_with(inputs: Vec<TaskInput>) -> Task {
        Task::new(
            TaskSpec::command("true", vec![]),
            TaskRequirements::generic(1, 10),
        )
        .with_inputs(inputs)
    }

    #[tokio::test]
    async fn inputs_are_fetched_once_then_served_from_the_cache() {
        let dir = tempfile::tempdir().unwrap();
        let cache = BlobCache::open(dir.path(), 1 << 30).unwrap();
        let blobs = Arc::new(Mutex::new(HashMap::new()));
        let reads = Arc::new(AtomicU64::new(0));
        let (tx, rx) = mpsc::channel(16);
        let client = BlobClient::new(tx);
        spawn_master(
            rx,
            Arc::clone(&client),
            Arc::clone(&blobs),
            Arc::clone(&reads),
        );

        let main = b"fn main() {}".to_vec();
        let tree = TreeManifest::new(vec![BlobFile {
            path: "src/main.rs".into(),
            id: BlobId::of(&main),
            size: main.len() as u64,
            executable: false,
        }])
        .unwrap();
        let encoded = tree.encode();
        let manifest = BlobId::of(&encoded);
        blobs
            .lock()
            .unwrap()
            .insert(BlobId::of(&main), main.clone());
        blobs.lock().unwrap().insert(manifest, encoded.clone());
        let task = task_with(vec![TaskInput::tree(
            "proj",
            manifest,
            encoded.len() as u64,
        )]);

        let pin = fetch_task_inputs(&task, Some(&client), &cache)
            .await
            .unwrap();
        assert!(cache.contains(&manifest) && cache.contains(&BlobId::of(&main)));
        let first_reads = reads.load(Ordering::Relaxed);
        assert_eq!(first_reads, 2);
        drop(pin);

        // Same inputs again: nothing crosses the wire.
        fetch_task_inputs(&task, Some(&client), &cache)
            .await
            .unwrap();
        assert_eq!(reads.load(Ordering::Relaxed), first_reads);

        // A cached input needs no master at all; a missing one does.
        fetch_task_inputs(&task, None, &cache).await.unwrap();
        let other = task_with(vec![TaskInput::file("x", BlobId::of(b"x"), 1, false)]);
        assert!(fetch_task_inputs(&other, None, &cache)
            .await
            .unwrap_err()
            .contains("not connected"));
    }

    #[tokio::test]
    async fn outputs_the_master_lacks_are_uploaded_and_requested_outputs_need_a_store() {
        let dir = tempfile::tempdir().unwrap();
        let cache = BlobCache::open(dir.path(), 1 << 30).unwrap();
        let blobs = Arc::new(Mutex::new(HashMap::new()));
        let (tx, rx) = mpsc::channel(16);
        let client = BlobClient::new(tx);
        spawn_master(
            rx,
            Arc::clone(&client),
            Arc::clone(&blobs),
            Arc::new(AtomicU64::new(0)),
        );

        let produced = dir.path().join("artifact.bin");
        std::fs::write(&produced, b"compiled artifact").unwrap();
        let (id, size) = cache.insert_file(&produced, true).unwrap();
        let task = task_with(vec![]).with_output_glob("artifact.bin");
        let output = BlobFile {
            path: "artifact.bin".into(),
            id,
            size,
            executable: false,
        };

        let mut result = TaskResult::success(Uuid::new_v4(), task.id, "", 1, false)
            .with_outputs(vec![output.clone()]);
        upload_task_outputs(&task, &mut result, Some(&client), &cache).await;
        assert!(result.is_success(), "{:?}", result.error);
        assert_eq!(
            blobs.lock().unwrap().get(&id).unwrap(),
            b"compiled artifact"
        );

        let mut no_store = TaskResult::success(Uuid::new_v4(), task.id, "", 1, false)
            .with_outputs(vec![output.clone()]);
        upload_task_outputs(&task, &mut no_store, None, &cache).await;
        assert!(
            !no_store.is_success(),
            "requested outputs that cannot be returned fail the task"
        );
        assert!(no_store.outputs.is_empty());

        let implicit = task_with(vec![]);
        let mut compiled = TaskResult::success(Uuid::new_v4(), implicit.id, "", 1, false)
            .with_outputs(vec![output]);
        upload_task_outputs(&implicit, &mut compiled, None, &cache).await;
        assert!(
            compiled.is_success(),
            "unrequested artifacts are only noted"
        );
        assert!(compiled.stderr_str().contains("not returned"));
    }

    #[tokio::test]
    async fn a_closed_session_fails_waiting_requests_at_once() {
        let (tx, mut rx) = mpsc::channel(4);
        let client = BlobClient::new(tx);
        let waiter = {
            let client = Arc::clone(&client);
            tokio::spawn(async move { client.call(BlobRequest::Missing { ids: vec![] }).await })
        };
        assert!(rx.recv().await.is_some());
        client.close();
        let outcome = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("no wait for the reply timeout")
            .unwrap();
        assert!(outcome.is_err());
        let later = tokio::time::timeout(
            Duration::from_secs(5),
            client.call(BlobRequest::Missing { ids: vec![] }),
        )
        .await
        .expect("a closed client answers at once");
        assert!(later.is_err());
    }

    #[tokio::test]
    async fn output_files_remain_pinned_between_collection_and_upload() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = Arc::new(Mutex::new(HashMap::new()));
        let (tx, rx) = mpsc::channel(32);
        let client = BlobClient::new(tx);
        spawn_master(
            rx,
            Arc::clone(&client),
            Arc::clone(&blobs),
            Arc::new(AtomicU64::new(0)),
        );

        let mut runner_cfg = crate::runner::RunnerConfig::default();
        runner_cfg.sandbox_config.base_dir = dir.path().to_path_buf();
        // Bound the worker cache at 1 KiB while the task produces 4 files of 512 B (2 KiB total).
        runner_cfg.blob_cache_bytes = 1024;
        let runner = crate::runner::TaskRunner::new(
            Uuid::new_v4(),
            rusty_grid_core::capabilities::WorkerCapabilities::new(
                "test", 1, 1024, false, false, None,
            ),
            runner_cfg.clone(),
        );
        let cache = runner.blob_cache().unwrap();

        let task = task_with(vec![]).with_output_glob("out/*");
        let mut sandbox = crate::sandbox::Sandbox::create(&runner_cfg.sandbox_config, task.id)
            .await
            .unwrap();
        std::fs::create_dir_all(sandbox.path().join("out")).unwrap();
        for i in 0..4u8 {
            std::fs::write(
                sandbox.path().join("out").join(format!("f{i}.bin")),
                vec![i + 1; 512],
            )
            .unwrap();
        }

        let initial = TaskResult::success(runner.worker_id(), task.id, "", 1, false);
        let mut result = runner
            .collect_blob_outputs(&sandbox, &task, initial, &[])
            .await;
        let _ = sandbox.destroy().await;
        assert_eq!(result.outputs.len(), 4);

        // Another task finishing on the worker calls `evict_to_fit` before `upload_task_outputs`.
        assert_eq!(
            cache.evict_to_fit(),
            0,
            "collected output files must stay pinned until their upload finishes"
        );

        upload_task_outputs(&task, &mut result, Some(&client), &cache).await;
        assert!(result.is_success(), "{:?}", result.error);
        assert_eq!(blobs.lock().unwrap().len(), 4);

        // Once the upload is complete, the output pin is released and `evict_to_fit` can reclaim space.
        assert!(
            cache.evict_to_fit() >= 2,
            "output pin must be released after upload finishes"
        );

        // A failed upload (closed session) also releases the output pin on return.
        let failed_task = task_with(vec![]).with_output_glob("out/*");
        let mut sandbox =
            crate::sandbox::Sandbox::create(&runner_cfg.sandbox_config, failed_task.id)
                .await
                .unwrap();
        std::fs::create_dir_all(sandbox.path().join("out")).unwrap();
        for i in 0..4u8 {
            std::fs::write(
                sandbox.path().join("out").join(format!("g{i}.bin")),
                vec![i + 10; 512],
            )
            .unwrap();
        }
        let initial = TaskResult::success(runner.worker_id(), failed_task.id, "", 1, false);
        let mut failed_res = runner
            .collect_blob_outputs(&sandbox, &failed_task, initial, &[])
            .await;
        let _ = sandbox.destroy().await;
        cache.evict_to_fit();
        for out in &failed_res.outputs {
            assert!(
                cache.contains(&out.id),
                "pinned output of failed_task must survive evict_to_fit"
            );
        }
        client.close();
        upload_task_outputs(&failed_task, &mut failed_res, Some(&client), &cache).await;
        assert!(!failed_res.is_success());
        assert!(
            cache.evict_to_fit() >= 2,
            "output pin must be released even when upload fails"
        );

        // A suppressed result (where upload is skipped) releases the pin via `release_task_outputs`.
        let suppressed_task = task_with(vec![]).with_output_glob("out/*");
        let mut sandbox =
            crate::sandbox::Sandbox::create(&runner_cfg.sandbox_config, suppressed_task.id)
                .await
                .unwrap();
        std::fs::create_dir_all(sandbox.path().join("out")).unwrap();
        for i in 0..4u8 {
            std::fs::write(
                sandbox.path().join("out").join(format!("h{i}.bin")),
                vec![i + 20; 512],
            )
            .unwrap();
        }
        let initial = TaskResult::success(runner.worker_id(), suppressed_task.id, "", 1, false);
        let suppressed_res = runner
            .collect_blob_outputs(&sandbox, &suppressed_task, initial, &[])
            .await;
        let _ = sandbox.destroy().await;
        cache.evict_to_fit();
        for out in &suppressed_res.outputs {
            assert!(
                cache.contains(&out.id),
                "pinned output of suppressed_task must survive evict_to_fit"
            );
        }
        runner.release_task_outputs(suppressed_task.id);
        assert!(
            cache.evict_to_fit() >= 2,
            "output pin must be released when result is suppressed"
        );
    }

    #[tokio::test]
    async fn session_limits_concurrent_requests_in_flight() {
        use std::sync::atomic::AtomicUsize;

        let (tx, mut rx) = mpsc::channel(128);
        let client = BlobClient::new(tx);
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak_in_flight = Arc::new(AtomicUsize::new(0));

        let responder_client = Arc::clone(&client);
        let responder_in_flight = Arc::clone(&in_flight);
        let responder_peak = Arc::clone(&peak_in_flight);
        tokio::spawn(async move {
            while let Some(WorkerMessage::BlobRequest { request_id, .. }) = rx.recv().await {
                let current = responder_in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                responder_peak.fetch_max(current, Ordering::SeqCst);
                let client = Arc::clone(&responder_client);
                let in_flight = Arc::clone(&responder_in_flight);
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    client.deliver(request_id, BlobReply::Missing { ids: vec![] });
                });
            }
        });

        let mut handles = Vec::with_capacity(100);
        for _ in 0..100 {
            let client = Arc::clone(&client);
            handles.push(tokio::spawn(async move {
                client.call(BlobRequest::Missing { ids: vec![] }).await
            }));
        }
        for handle in handles {
            let reply = handle.await.unwrap().expect("request finishes");
            assert_eq!(reply, BlobReply::Missing { ids: vec![] });
        }

        let peak = peak_in_flight.load(Ordering::SeqCst);
        assert!(
            peak <= MAX_IN_FLIGHT_REQUESTS,
            "saw {peak} concurrent requests in flight, limit is {MAX_IN_FLIGHT_REQUESTS}"
        );
    }
}
