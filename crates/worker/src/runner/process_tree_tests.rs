use super::*;
use rusty_grid_core::task::TaskRequirements;

fn runner_with(
    configure: impl FnOnce(RunnerConfig) -> RunnerConfig,
) -> (Arc<TaskRunner>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let caps = WorkerCapabilities {
        name: "tree-test-worker".into(),
        cpu_cores: 4,
        ram_mb: 8192,
        has_gpu: false,
        is_simulated_gpu: false,
        gpu_device_name: None,
        tags: vec![],
        mobile: None,
        ..Default::default()
    };
    let config = configure(RunnerConfig::new(SandboxConfig::new(dir.path())));
    (Arc::new(TaskRunner::new(Uuid::new_v4(), caps, config)), dir)
}

fn script_task(script: String, timeout_secs: u64) -> Task {
    Task::new(
        TaskSpec::shell_script(script),
        TaskRequirements::generic(1, timeout_secs),
    )
}

/// Alive means running: a zombie (killed, not yet reaped by its new parent) does not count.
fn alive(pid: u32) -> bool {
    let mut sys = sysinfo::System::new();
    sys.refresh_processes();
    sys.process(sysinfo::Pid::from(pid as usize))
        .is_some_and(|p| p.status() != sysinfo::ProcessStatus::Zombie)
}

fn kill_stray(pid: u32) {
    // SAFETY: SIGKILL to a PID this test recorded from its own leftover process.
    unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
}

/// The PID a script recorded in `file` (waits for the script to get that far).
async fn recorded_pid(file: &Path) -> u32 {
    for _ in 0..200 {
        if let Some(pid) = std::fs::read_to_string(file)
            .ok()
            .and_then(|text| text.trim().parse::<u32>().ok())
        {
            return pid;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("{} was never written", file.display());
}

async fn assert_dies(what: &str, pid: u32) {
    for _ in 0..100 {
        if !alive(pid) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    kill_stray(pid);
    panic!("{what} (pid {pid}) survived the end of its task");
}

/// A script that leaves two descendants running: a background `sleep` of its own and, one level
/// deeper, another one under a nested `sh`.
fn tree_script(pids: &Path) -> String {
    format!(
        "sleep 300 &\necho $! > '{child}'\nsh -c 'sleep 300 & echo $! > \"$0\"; wait' '{grandchild}' &\nwait\n",
        child = pids.join("child.pid").display(),
        grandchild = pids.join("grandchild.pid").display(),
    )
}

#[tokio::test]
async fn test_timeout_kills_the_whole_tree_including_grandchildren() {
    let (runner, _dir) = runner_with(|c| c);
    let pids = tempfile::tempdir().unwrap();
    let task = script_task(tree_script(pids.path()), 2);

    let res = runner.execute_task(&task, None, None).await;

    assert_eq!(res.exit_code, EXIT_CODE_TIMEOUT);
    assert_dies(
        "background child",
        recorded_pid(&pids.path().join("child.pid")).await,
    )
    .await;
    assert_dies(
        "nested grandchild",
        recorded_pid(&pids.path().join("grandchild.pid")).await,
    )
    .await;
}

#[tokio::test]
async fn test_cancellation_kills_the_whole_tree_including_grandchildren() {
    let (runner, _dir) = runner_with(|c| c);
    let pids = tempfile::tempdir().unwrap();
    let task = script_task(tree_script(pids.path()), 60);
    let (cancel_tx, cancel_rx) = oneshot::channel();

    let run = tokio::spawn({
        let runner = Arc::clone(&runner);
        async move { runner.execute_task(&task, None, Some(cancel_rx)).await }
    });
    let child = recorded_pid(&pids.path().join("child.pid")).await;
    let grandchild = recorded_pid(&pids.path().join("grandchild.pid")).await;
    assert!(
        alive(child) && alive(grandchild),
        "the tree must be running before the cancel"
    );

    cancel_tx.send("stop".into()).unwrap();
    let res = run.await.unwrap();

    assert_eq!(res.exit_code, EXIT_CODE_CANCELLED);
    assert!(res.error.unwrap().contains("Cancelled: stop"));
    assert_dies("background child", child).await;
    assert_dies("nested grandchild", grandchild).await;
}

#[tokio::test]
async fn test_dropping_the_run_kills_the_tree() {
    // This is what `abort_handle.abort()` (a cancel the channel could not deliver) and a
    // shut-down runtime do to a running task.
    let (runner, _dir) = runner_with(|c| c);
    let pids = tempfile::tempdir().unwrap();
    let task = script_task(tree_script(pids.path()), 60);

    let run = tokio::spawn({
        let runner = Arc::clone(&runner);
        async move { runner.execute_task(&task, None, None).await }
    });
    let child = recorded_pid(&pids.path().join("child.pid")).await;
    let grandchild = recorded_pid(&pids.path().join("grandchild.pid")).await;

    run.abort();
    assert!(run.await.unwrap_err().is_cancelled());

    assert_dies("background child", child).await;
    assert_dies("nested grandchild", grandchild).await;
}

#[tokio::test]
async fn test_a_daemon_left_behind_by_a_finished_task_is_killed_with_it() {
    // The shell exits at once; the helper redirected its output, so no pipe keeps the task
    // open. It must still not outlive the task (the sandbox is deleted right after).
    let (runner, _dir) = runner_with(|c| c);
    let pids = tempfile::tempdir().unwrap();
    let pid_file = pids.path().join("daemon.pid");
    let task = script_task(
        format!(
            "( sleep 300 >/dev/null 2>&1 </dev/null & echo $! > '{}' )\necho finished\n",
            pid_file.display()
        ),
        30,
    );

    let res = runner.execute_task(&task, None, None).await;

    assert!(res.is_success(), "{res:?}");
    assert_eq!(res.stdout_str().trim(), "finished");
    assert_dies("daemon", recorded_pid(&pid_file).await).await;
}

#[tokio::test]
async fn test_a_background_process_holding_the_pipes_does_not_turn_exit_7_into_a_timeout() {
    let (runner, _dir) = runner_with(|c| c.with_pipe_drain_grace(Duration::from_millis(300)));
    let pids = tempfile::tempdir().unwrap();
    let pid_file = pids.path().join("holder.pid");
    // `sleep` inherits stdout and stderr, so the pipes stay open for five minutes.
    let task = script_task(
        format!(
            "sleep 300 &\necho $! > '{}'\necho done-output\necho some-warning >&2\nexit 7\n",
            pid_file.display()
        ),
        20,
    );

    let t0 = Instant::now();
    let res = runner.execute_task(&task, None, None).await;
    let elapsed = t0.elapsed();

    assert!(
        elapsed < Duration::from_secs(10),
        "must not wait for the pipes or the timeout: {elapsed:?}"
    );
    assert_eq!(
        res.exit_code, 7,
        "the real exit status, not a timeout: {res:?}"
    );
    assert_ne!(res.exit_code, EXIT_CODE_TIMEOUT);
    assert!(
        res.stdout_str().contains("done-output"),
        "output must be kept: {:?}",
        res.stdout_str()
    );
    assert!(res.stderr_str().contains("some-warning"));
    assert!(
        res.stderr_str().contains("kept the output pipes open"),
        "the result should explain the terminated helpers: {:?}",
        res.stderr_str()
    );
    assert_dies("pipe holder", recorded_pid(&pid_file).await).await;
}

#[tokio::test]
async fn test_a_background_process_holding_the_pipes_does_not_fail_a_successful_task() {
    let (runner, _dir) = runner_with(|c| c.with_pipe_drain_grace(Duration::from_millis(300)));
    let pids = tempfile::tempdir().unwrap();
    let pid_file = pids.path().join("holder.pid");
    let task = script_task(
        format!(
            "sleep 300 &\necho $! > '{}'\necho built\n",
            pid_file.display()
        ),
        20,
    );

    let res = runner.execute_task(&task, None, None).await;

    assert!(res.is_success(), "{res:?}");
    assert_eq!(res.exit_code, 0);
    assert!(res.stdout_str().contains("built"));
    assert_dies("pipe holder", recorded_pid(&pid_file).await).await;
}

#[tokio::test]
async fn test_a_helper_that_escaped_the_process_group_cannot_hold_the_run_hostage() {
    // A helper in its own session is out of reach of the group kill and holds the pipes. The
    // run must still end after the drain grace, with the real status and the output so far.
    if std::process::Command::new("python3")
        .arg("--version")
        .output()
        .is_err()
    {
        eprintln!("python3 not available; skipping");
        return;
    }
    let (runner, _dir) = runner_with(|c| c.with_pipe_drain_grace(Duration::from_millis(300)));
    let task = Task::new(
        TaskSpec::command(
            "python3",
            vec![
                "-c".into(),
                "import subprocess; p = subprocess.Popen(['sleep', '300'], start_new_session=True); print(p.pid, flush=True)".into(),
            ],
        ),
        TaskRequirements::generic(1, 20),
    );

    let t0 = Instant::now();
    let res = runner.execute_task(&task, None, None).await;
    let elapsed = t0.elapsed();

    let escaped: Option<u32> = res
        .stdout_str()
        .lines()
        .next()
        .and_then(|l| l.trim().parse().ok());
    if let Some(pid) = escaped {
        kill_stray(pid);
    }
    assert!(res.is_success(), "{res:?}");
    assert!(
        escaped.is_some(),
        "the pid line was captured: {:?}",
        res.stdout_str()
    );
    assert!(elapsed < Duration::from_secs(10), "{elapsed:?}");
}

#[tokio::test]
async fn test_partial_output_survives_timeout_and_cancellation() {
    let (runner, _dir) = runner_with(|c| c);
    let task = script_task(
        "echo partial-out\necho partial-err >&2\nsleep 300\n".into(),
        3,
    );
    let res = runner.execute_task(&task, None, None).await;
    assert_eq!(res.exit_code, EXIT_CODE_TIMEOUT);
    assert!(
        res.stdout_str().contains("partial-out"),
        "exit_code={} error={:?} stdout={:?} stderr={:?}",
        res.exit_code,
        res.error,
        res.stdout_str(),
        res.stderr_str()
    );
    assert!(
        res.stderr_str().contains("partial-err"),
        "{:?}",
        res.stderr_str()
    );
    assert!(
        res.stderr_str().contains("Task timed out after 3s"),
        "{:?}",
        res.stderr_str()
    );
    assert!(res.error.as_deref().unwrap().contains("timed out after 3s"));

    let marker_dir = tempfile::tempdir().unwrap();
    let ready = marker_dir.path().join("printed");
    let task = script_task(
        format!("echo partial-out\n: > '{}'\nsleep 300\n", ready.display()),
        60,
    );
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let run = tokio::spawn({
        let runner = Arc::clone(&runner);
        async move { runner.execute_task(&task, None, Some(cancel_rx)).await }
    });
    tokio::time::timeout(Duration::from_secs(60), async {
        while !ready.exists() {
            assert!(
                !run.is_finished(),
                "task exited before printing first line / marker"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("task did not print first line within 60 s");
    cancel_tx.send("enough".into()).unwrap();
    let res = tokio::time::timeout(Duration::from_secs(60), run)
        .await
        .expect("cancelled task did not finish within 60 s")
        .unwrap();
    assert_eq!(res.exit_code, EXIT_CODE_CANCELLED);
    assert!(
        res.stdout_str().contains("partial-out"),
        "exit_code={} error={:?} stdout={:?} stderr={:?}",
        res.exit_code,
        res.error,
        res.stdout_str(),
        res.stderr_str()
    );
}

#[tokio::test]
async fn test_ending_a_tree_leaves_an_unrelated_process_alone() {
    let mut bystander = std::process::Command::new("sleep")
        .arg("300")
        .spawn()
        .unwrap();
    let bystander_pid = bystander.id();

    let (runner, _dir) = runner_with(|c| c);
    let pids = tempfile::tempdir().unwrap();
    let res = runner
        .execute_task(&script_task(tree_script(pids.path()), 1), None, None)
        .await;
    assert_eq!(res.exit_code, EXIT_CODE_TIMEOUT);
    let finished = runner
        .execute_task(&script_task("echo fine".into(), 10), None, None)
        .await;
    assert!(finished.is_success());

    let still_there = alive(bystander_pid);
    let _ = bystander.kill();
    let _ = bystander.wait();
    assert!(
        still_there,
        "an unrelated process was killed along with a task's tree"
    );
}

#[tokio::test]
async fn test_task_processes_run_at_lower_priority_by_default_and_it_is_configurable() {
    /// The nice value `ps` reports for the shell that runs the task.
    async fn task_nice(runner: &TaskRunner) -> i32 {
        let task = Task::new(
            TaskSpec::command("sh", vec!["-c".into(), "ps -o ni= -p $$".into()]),
            TaskRequirements::generic(1, 10),
        );
        let res = runner.execute_task(&task, None, None).await;
        assert!(res.is_success(), "{res:?}");
        res.stdout_str()
            .trim()
            .parse()
            .unwrap_or_else(|_| panic!("no nice value in {:?}", res.stdout_str()))
    }

    let own_nice: i32 = {
        let out = std::process::Command::new("ps")
            .args(["-o", "ni=", "-p", &std::process::id().to_string()])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().parse().unwrap()
    };

    let (default_runner, _d1) =
        runner_with(|c| c.with_process_priority(ProcessPriority::default()));
    assert_eq!(ProcessPriority::default(), ProcessPriority::BelowNormal);
    assert_eq!(task_nice(&default_runner).await, own_nice.max(10));

    let (normal, _d2) = runner_with(|c| c.with_process_priority(ProcessPriority::Normal));
    assert_eq!(task_nice(&normal).await, own_nice);

    let (idle, _d3) = runner_with(|c| c.with_process_priority(ProcessPriority::Idle));
    assert_eq!(task_nice(&idle).await, own_nice.max(19));
}

#[tokio::test]
async fn test_worker_secrets_are_not_inherited_by_task_processes() {
    const SENTINELS: [(&str, &str); 3] = [
        ("OXIDESWARM_SECRET", "sentinel-cluster-secret"),
        ("OXIDESWARM_SECRET_FILE", "/sentinel/secret/file"),
        ("OXIDE_MESH_TOKEN", "sentinel-mesh-token"),
    ];
    // Put the worker's "own" secrets in this process's environment, remove them afterwards.
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            for (name, _) in SENTINELS {
                std::env::remove_var(name);
            }
        }
    }
    let _restore = Restore;
    for (name, value) in SENTINELS {
        std::env::set_var(name, value);
    }

    let (runner, _dir) = runner_with(|c| c);
    let probe = "echo \"[${OXIDESWARM_SECRET-unset}][${OXIDESWARM_SECRET_FILE-unset}][${OXIDE_MESH_TOKEN-unset}]\"";

    // Command task
    let task = Task::new(
        TaskSpec::command("sh", vec!["-c".into(), probe.into()]),
        TaskRequirements::generic(1, 10),
    );
    let res = runner.execute_task(&task, None, None).await;
    assert_eq!(res.stdout_str().trim(), "[unset][unset][unset]", "{res:?}");

    // ShellScript task
    let res = runner
        .execute_task(&script_task(probe.into(), 10), None, None)
        .await;
    assert_eq!(res.stdout_str().trim(), "[unset][unset][unset]", "{res:?}");

    // A value the task is explicitly given is its own business and still arrives.
    let mut env = HashMap::new();
    env.insert(
        "OXIDE_MESH_TOKEN".to_string(),
        "given-on-purpose".to_string(),
    );
    let task = Task::new(
        TaskSpec::Command {
            program: "sh".into(),
            args: vec!["-c".into(), probe.into()],
            env,
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 10),
    );
    let res = runner.execute_task(&task, None, None).await;
    assert_eq!(
        res.stdout_str().trim(),
        "[unset][unset][given-on-purpose]",
        "{res:?}"
    );
}

#[tokio::test]
async fn test_read_bounded_stream_keeps_head_and_tail_when_truncated() {
    let mut input = Vec::new();
    input.extend_from_slice(b"HEAD-0123456789");
    input.extend(std::iter::repeat_n(b'M', 1024));
    input.extend_from_slice(b"TAIL-9876543210");

    let (captured, truncated) = read_bounded_stream(&input[..], 30).await;
    assert!(truncated);
    let text = String::from_utf8(captured.0.to_vec()).expect("utf8");
    assert!(
        text.starts_with("HEAD-0123456789"),
        "expected head preserved, got {text:?}"
    );
    assert!(
        text.ends_with("TAIL-9876543210"),
        "expected tail preserved, got {text:?}"
    );
    assert!(
        text.contains("[rusty_grid: output truncated after exceeding size limit]"),
        "expected truncation notice in {text:?}"
    );

    // Exact fit does not mark truncated and preserves the full stream in order.
    let exact = b"0123456789abcdef";
    let (captured_exact, truncated_exact) = read_bounded_stream(&exact[..], exact.len()).await;
    assert!(!truncated_exact);
    assert_eq!(captured_exact.as_bytes(), exact);
}

#[tokio::test]
async fn test_read_bounded_stream_chunked_boundaries_exact_plus_one_and_many_wraps() {
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, ReadBuf};

    struct ChunkedSlice<'a> {
        data: &'a [u8],
        pos: usize,
        chunk_size: usize,
    }

    impl AsyncRead for ChunkedSlice<'_> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if self.pos >= self.data.len() {
                return Poll::Ready(Ok(()));
            }
            let avail = self.data.len() - self.pos;
            let n = avail.min(self.chunk_size).min(buf.remaining());
            buf.put_slice(&self.data[self.pos..self.pos + n]);
            self.pos += n;
            Poll::Ready(Ok(()))
        }
    }

    let max_len = 4096 * 4 + 37;
    let master_input: Vec<u8> = (0..max_len).map(|i| ((i * 73 + 19) & 0xFF) as u8).collect();

    for cap in [1usize, 2, 15, 16, 1024, 1025, 4096] {
        let head_cap = cap / 2;
        let tail_cap = cap - head_cap;
        let lengths = [
            cap.saturating_sub(1),
            cap,
            cap + 1,
            head_cap + 1 + tail_cap,
            cap * 4 + 37,
        ];
        for len in lengths {
            let input = &master_input[..len];
            for chunk_size in [1usize, 2, 7, 64, 511, 512, 513, 1024, 1025, 4096, 8192] {
                let reader = ChunkedSlice {
                    data: input,
                    pos: 0,
                    chunk_size,
                };
                let (captured, truncated) = read_bounded_stream(reader, cap).await;
                let bytes = captured.as_bytes();
                if len <= cap {
                    assert!(
                        !truncated,
                        "cap={cap} len={len} chunk_size={chunk_size}: must not truncate"
                    );
                    assert_eq!(
                        bytes, input,
                        "cap={cap} len={len} chunk_size={chunk_size}: exact bytes must match"
                    );
                } else {
                    assert!(
                        truncated,
                        "cap={cap} len={len} chunk_size={chunk_size}: must flag truncated"
                    );
                    assert_eq!(
                        bytes.len(),
                        cap + TRUNCATION_NOTICE.len(),
                        "cap={cap} len={len} chunk_size={chunk_size}: truncated len mismatch"
                    );
                    assert_eq!(
                        &bytes[..head_cap],
                        &input[..head_cap],
                        "cap={cap} len={len} chunk_size={chunk_size}: head mismatch"
                    );
                    assert_eq!(
                        &bytes[head_cap..head_cap + TRUNCATION_NOTICE.len()],
                        TRUNCATION_NOTICE,
                        "cap={cap} len={len} chunk_size={chunk_size}: notice mismatch"
                    );
                    assert_eq!(
                        &bytes[head_cap + TRUNCATION_NOTICE.len()..],
                        &input[len - tail_cap..],
                        "cap={cap} len={len} chunk_size={chunk_size}: tail mismatch"
                    );
                }
            }
        }
    }
}
