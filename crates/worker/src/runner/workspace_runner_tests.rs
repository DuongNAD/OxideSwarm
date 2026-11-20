use super::*;
use rusty_grid_core::task::TaskRequirements;

fn store_manifest_in_cache(
    cache: &crate::blob_cache::BlobCache,
    files: &[(&str, &[u8], bool)],
) -> (rusty_grid_core::blob::BlobId, u64) {
    use rusty_grid_core::blob::{BlobFile, BlobId, TreeManifest};
    let mut entries = Vec::with_capacity(files.len());
    for (path, data, executable) in files {
        let id = BlobId::of(data);
        let tmp = cache.temp_path();
        std::fs::write(&tmp, data).expect("write file blob");
        cache
            .adopt(&tmp, id, data.len() as u64)
            .expect("adopt file blob");
        entries.push(BlobFile {
            path: (*path).to_string(),
            id,
            size: data.len() as u64,
            executable: *executable,
        });
    }
    let manifest = TreeManifest::new(entries).expect("valid manifest");
    let manifest_bytes = manifest.encode();
    let manifest_id = BlobId::of(&manifest_bytes);
    let tmp = cache.temp_path();
    std::fs::write(&tmp, &manifest_bytes).expect("write manifest blob");
    cache
        .adopt(&tmp, manifest_id, manifest_bytes.len() as u64)
        .expect("adopt manifest blob");
    (manifest_id, manifest_bytes.len() as u64)
}

#[tokio::test]
async fn test_workspace_task_syncs_persists_build_output_deletes_removed_files_and_uses_tmp() {
    use rusty_grid_core::task::WorkspaceRef;

    let tmp = tempfile::tempdir().expect("tempdir");
    let home_dir = tmp.path().join("home");
    let sandbox_dir = tmp.path().join("sandboxes");
    let ws_root = tmp.path().join("workspaces");
    std::fs::create_dir_all(&home_dir).expect("create fake home");

    let real = crate::isolation::HostDirs::detect();
    let host = crate::isolation::HostDirs::from_home(
        home_dir,
        Some(real.rustup_home),
        Some(real.cargo_home),
    );
    let cfg = RunnerConfig::new(SandboxConfig::new(&sandbox_dir))
        .with_host_dirs(host)
        .with_workspaces(true)
        .with_workspace_root(Some(ws_root.clone()));
    let runner = TaskRunner::new(Uuid::new_v4(), WorkerCapabilities::default(), cfg);
    let cache = runner.blob_cache().expect("blob cache");

    // Task 1: syncs initial manifest (`src/a.txt`, `src/old.txt`), checks env & tmpdir,
    // creates untracked build output `target/artifact.txt`, and collects it via `output_globs`.
    let (m1_id, m1_size) = store_manifest_in_cache(
        &cache,
        &[
            ("src/a.txt", b"v1-alpha", false),
            ("src/old.txt", b"remove-me", false),
        ],
    );
    let script1 = r#"
set -e
echo "WS=$RUSTY_GRID_WORKSPACE"
echo "WS_DIR=$RUSTY_GRID_WORKSPACE_DIR"
echo "TMP=$TMPDIR"
echo "A=$(cat src/a.txt)"
echo "OLD=$(cat src/old.txt)"
mkdir -p target
printf 'built-artifact-v1' > target/artifact.txt
printf 'temp-scratch' > "$TMPDIR/scratch.tmp"
[ -f "$TMPDIR/scratch.tmp" ] && echo "TMP_WRITE_OK"
"#;
    let task1 = Task::new(
        TaskSpec::shell_script(script1),
        TaskRequirements::generic(1, 30),
    )
    .with_workspace(WorkspaceRef::new("my_proj").with_tree(m1_id, m1_size))
    .with_output_globs(vec!["target/artifact.txt".to_string()]);

    let res1 = runner.execute_task(&task1, None, None).await;
    assert!(
        res1.is_success(),
        "task 1 failed: {:?} / {}",
        res1.error,
        res1.stderr_str()
    );
    let out1 = res1.stdout_str();
    assert!(out1.contains("WS=my_proj"), "stdout: {out1}");
    assert!(out1.contains("A=v1-alpha"), "stdout: {out1}");
    assert!(out1.contains("OLD=remove-me"), "stdout: {out1}");
    assert!(out1.contains("TMP_WRITE_OK"), "stdout: {out1}");
    assert_eq!(res1.outputs.len(), 1);
    assert_eq!(res1.outputs[0].path, "target/artifact.txt");
    assert_eq!(
        std::fs::read_to_string(ws_root.join("my_proj/tree/target/artifact.txt")).unwrap(),
        "built-artifact-v1",
        "output collection from a workspace must copy, not move/delete files out of the tree"
    );
    runner.release_task_outputs(task1.id);

    // Task 2: syncs updated manifest (`src/a.txt` updated, `src/old.txt` removed).
    // Untracked `target/artifact.txt` must survive, `src/old.txt` must be gone, and `$TMPDIR`
    // must have been wiped fresh by `prepare_tmp`.
    let (m2_id, m2_size) = store_manifest_in_cache(&cache, &[("src/a.txt", b"v2-beta", false)]);
    let script2 = r#"
set -e
echo "A=$(cat src/a.txt)"
[ ! -e src/old.txt ] && echo "OLD_DELETED"
echo "KEPT=$(cat target/artifact.txt)"
[ ! -e "$TMPDIR/scratch.tmp" ] && echo "TMP_WIPED"
"#;
    let task2 = Task::new(
        TaskSpec::shell_script(script2),
        TaskRequirements::generic(1, 30),
    )
    .with_workspace(WorkspaceRef::new("my_proj").with_tree(m2_id, m2_size));
    let res2 = runner.execute_task(&task2, None, None).await;
    assert!(
        res2.is_success(),
        "task 2 failed: {:?} / {}",
        res2.error,
        res2.stderr_str()
    );
    let out2 = res2.stdout_str();
    assert!(out2.contains("A=v2-beta"), "stdout: {out2}");
    assert!(out2.contains("OLD_DELETED"), "stdout: {out2}");
    assert!(out2.contains("KEPT=built-artifact-v1"), "stdout: {out2}");
    assert!(out2.contains("TMP_WIPED"), "stdout: {out2}");

    // Task 3: runs in the already-ready workspace with no `source` and relative `working_dir`.
    let cmd3 = TaskSpec::Command {
        program: "sh".into(),
        args: vec![
            "-c".into(),
            "cat a.txt && cat ../target/artifact.txt".into(),
        ],
        env: HashMap::new(),
        working_dir: Some("src".into()),
        stdin: None,
    };
    let task3 = Task::new(cmd3, TaskRequirements::generic(1, 30))
        .with_workspace(WorkspaceRef::new("my_proj"));
    let res3 = runner.execute_task(&task3, None, None).await;
    assert!(
        res3.is_success(),
        "task 3 failed: {:?} / {}",
        res3.error,
        res3.stderr_str()
    );
    assert!(res3.stdout_str().contains("v2-beta"));
    assert!(res3.stdout_str().contains("built-artifact-v1"));
}

#[tokio::test]
async fn test_workspace_task_error_cases_and_over_limit_warning() {
    use crate::workspace::WorkspaceLimits;
    use rusty_grid_core::task::WorkspaceRef;

    let tmp = tempfile::tempdir().expect("tempdir");
    let home_dir = tmp.path().join("home");
    let sandbox_dir = tmp.path().join("sandboxes");
    let ws_root = tmp.path().join("workspaces");
    std::fs::create_dir_all(&home_dir).expect("create fake home");

    let real = crate::isolation::HostDirs::detect();
    let host = crate::isolation::HostDirs::from_home(
        home_dir,
        Some(real.rustup_home),
        Some(real.cargo_home),
    );

    // 1. Runner without `--workspaces` refuses a workspace task clearly.
    let disabled_runner = TaskRunner::new(
        Uuid::new_v4(),
        WorkerCapabilities::default(),
        RunnerConfig::new(SandboxConfig::new(&sandbox_dir)).with_host_dirs(host.clone()),
    );
    let ws_task = Task::new(
        TaskSpec::shell_script("echo hi"),
        TaskRequirements::generic(1, 30),
    )
    .with_workspace(WorkspaceRef::new("proj"));
    let disabled_res = disabled_runner.execute_task(&ws_task, None, None).await;
    assert!(!disabled_res.is_success());
    assert!(
        disabled_res.stderr_str().contains("--workspaces"),
        "stderr: {}",
        disabled_res.stderr_str()
    );

    // 2. Runner with `--workspaces` refuses an uninitialized workspace when no tree source is given.
    let cfg = RunnerConfig::new(SandboxConfig::new(&sandbox_dir))
        .with_host_dirs(host)
        .with_workspaces(true)
        .with_workspace_root(Some(ws_root.clone()))
        .with_workspace_limits(WorkspaceLimits {
            limit_mb: 1,
            max_workspaces: 4,
            ..WorkspaceLimits::default()
        })
        .with_workspace_lock_wait(Duration::from_secs(90));
    let runner = TaskRunner::new(Uuid::new_v4(), WorkerCapabilities::default(), cfg);
    let unready_res = runner.execute_task(&ws_task, None, None).await;
    assert!(!unready_res.is_success());
    assert!(
        unready_res.stderr_str().contains("has no synced tree"),
        "stderr: {}",
        unready_res.stderr_str()
    );

    // 3. Relative working_dir escaping the workspace tree is refused (both via path traversal
    // and via a symlink inside the workspace tree).
    let cache = runner.blob_cache().expect("blob cache");
    let (m_id, m_size) = store_manifest_in_cache(&cache, &[("hello.txt", b"ok", false)]);
    let bad_cwd_task = Task::new(
        TaskSpec::Command {
            program: "echo".into(),
            args: vec!["hi".into()],
            env: HashMap::new(),
            working_dir: Some("../outside".into()),
            stdin: None,
        },
        TaskRequirements::generic(1, 30),
    )
    .with_workspace(WorkspaceRef::new("proj").with_tree(m_id, m_size));
    let bad_cwd_res = runner.execute_task(&bad_cwd_task, None, None).await;
    assert!(!bad_cwd_res.is_success());
    assert!(
        bad_cwd_res.stderr_str().contains("Path traversal")
            || bad_cwd_res
                .stderr_str()
                .contains("Invalid workspace working_dir"),
        "stderr: {}",
        bad_cwd_res.stderr_str()
    );
    assert!(resolve_workspace_cwd(&ws_root, Path::new("../outside")).is_err());
    assert!(resolve_workspace_cwd(&ws_root, Path::new("/abs/path")).is_err());

    // 4. Exceeding `limit_mb` (1 MiB) appends `finish()`'s warning to the task's stderr.
    let big = vec![b'x'; 1_200_000];
    let (big_m_id, big_m_size) = store_manifest_in_cache(&cache, &[("big.bin", &big, false)]);
    let over_limit_task = Task::new(
        TaskSpec::shell_script("echo done"),
        TaskRequirements::generic(1, 30),
    )
    .with_workspace(WorkspaceRef::new("proj").with_tree(big_m_id, big_m_size));
    let over_res = runner.execute_task(&over_limit_task, None, None).await;
    assert!(
        over_res.is_success(),
        "over-limit workspace task must still succeed: {:?} / {}",
        over_res.error,
        over_res.stderr_str()
    );
    assert!(
        over_res
            .stderr_str()
            .contains("above the limit of 1 MiB; it was evicted"),
        "stderr should include finish() warning: {}",
        over_res.stderr_str()
    );

    // 5. Cancellation while waiting on a busy workspace lock finishes promptly instead of
    //    blocking for the full `workspace_lock_wait` (90s).
    let store = runner
        .ensure_workspace_store()
        .await
        .expect("open store")
        .expect("store enabled");
    let held = store
        .acquire("busy_ws", Duration::ZERO)
        .expect("acquire busy_ws");
    let (cancel_tx, cancel_rx) = oneshot::channel();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(60)).await;
        let _ = cancel_tx.send("cancelled during lock wait".to_string());
    });
    let wait_start = Instant::now();
    let blocked_task = Task::new(
        TaskSpec::shell_script("echo should-not-run"),
        TaskRequirements::generic(1, 90),
    )
    .with_workspace(WorkspaceRef::new("busy_ws").with_tree(m_id, m_size));
    let cancel_res = runner
        .execute_task(&blocked_task, None, Some(cancel_rx))
        .await;
    let wait_elapsed = wait_start.elapsed();
    println!("workspace lock-wait cancellation took {wait_elapsed:?}");
    assert_eq!(cancel_res.exit_code, EXIT_CODE_CANCELLED);
    assert_eq!(
        cancel_res.error.as_deref(),
        Some("Cancelled: cancelled during lock wait")
    );
    assert!(!cancel_res.stdout_str().contains("should-not-run"));
    assert!(
        wait_elapsed < Duration::from_secs(60),
        "cancellation during lock wait hung: {wait_elapsed:?}"
    );
    drop(held);
}

#[tokio::test]
async fn test_runner_pre_cancelled_task_does_not_spawn_child() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let runner = TaskRunner::new(
        Uuid::new_v4(),
        WorkerCapabilities::default(),
        RunnerConfig::new(SandboxConfig::new(tmp.path())),
    );
    let task = Task::new(
        TaskSpec::shell_script("sleep 5; echo should-not-spawn"),
        TaskRequirements::generic(1, 30),
    );
    let child_pid = Arc::new(AtomicU32::new(0));
    let (cancel_tx, cancel_rx) = oneshot::channel();
    cancel_tx
        .send("WorkerClient dropped".to_string())
        .expect("send cancel");

    let res = runner
        .execute_task(&task, Some(Arc::clone(&child_pid)), Some(cancel_rx))
        .await;
    assert_eq!(res.exit_code, EXIT_CODE_CANCELLED);
    assert_eq!(
        child_pid.load(Ordering::Acquire),
        0,
        "pre-cancelled task must not spawn a child process"
    );
    assert_eq!(
        res.error.as_deref(),
        Some("Cancelled: WorkerClient dropped")
    );
}

#[tokio::test]
async fn test_runner_blob_cache_retries_after_initial_base_failure() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let base_path = tmp.path().join("sandboxes");
    std::fs::write(&base_path, b"blocking file").expect("write file");

    let runner = TaskRunner::new(
        Uuid::new_v4(),
        WorkerCapabilities::default(),
        RunnerConfig::new(SandboxConfig::new(&base_path)),
    );
    assert!(
        runner.ensure_blob_cache().await.is_err(),
        "blob cache open must fail while base_dir is a regular file"
    );
    assert!(runner.blob_cache_if_open().is_none());

    std::fs::remove_file(&base_path).expect("remove blocking file");
    let cache = runner
        .ensure_blob_cache()
        .await
        .expect("blob cache open must retry and succeed once base_dir is fixed");
    assert!(runner.blob_cache_if_open().is_some());
    let _ = cache;
}
