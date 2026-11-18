use super::*;
use rusty_grid_core::task::TaskRequirements;

fn runner_in(dir: &Path) -> TaskRunner {
    let caps = WorkerCapabilities {
        name: "outcome-test-worker".into(),
        cpu_cores: 4,
        ram_mb: 8192,
        has_gpu: false,
        is_simulated_gpu: false,
        gpu_device_name: None,
        tags: vec![],
        mobile: None,
        ..Default::default()
    };
    TaskRunner::new(
        Uuid::new_v4(),
        caps,
        RunnerConfig::new(SandboxConfig::new(dir)),
    )
}

#[cfg(unix)]
#[test]
fn test_describe_exit_keeps_130_for_cancellation_only() {
    use std::os::unix::process::ExitStatusExt;
    let exited = |code: i32| std::process::ExitStatus::from_raw(code << 8);
    let signalled = |sig: i32| std::process::ExitStatus::from_raw(sig);

    // Ordinary exit codes pass through untouched, with no error text.
    assert_eq!(describe_exit(&exited(1)), (1, None));
    assert_eq!(describe_exit(&exited(42)), (42, None));
    assert_eq!(describe_exit(&exited(124)), (124, None));

    // A script that ends with `exit 130` is a failure, never the Master's "cancelled" verdict.
    let (code, error) = describe_exit(&exited(130));
    assert_eq!(code, EXIT_CODE_INTERRUPTED);
    assert_ne!(code, EXIT_CODE_CANCELLED);
    assert!(error.unwrap().contains("130"));

    // SIGINT collides with it in the shell's 128+signal convention: distinct as well.
    let (code, error) = describe_exit(&signalled(2));
    assert_eq!(code, EXIT_CODE_INTERRUPTED);
    assert!(error.unwrap().contains("SIGINT"));

    // Every other signal keeps the conventional 128+signal and says which one it was.
    let (code, error) = describe_exit(&signalled(9));
    assert_eq!(code, 137);
    assert!(error.unwrap().contains("signal 9 (SIGKILL)"));
    let (code, error) = describe_exit(&signalled(15));
    assert_eq!(code, 143);
    assert!(error.unwrap().contains("SIGTERM"));
    let (code, error) = describe_exit(&signalled(31));
    assert_eq!(code, 128 + 31);
    assert!(error.unwrap().contains("signal 31"));
}

#[cfg(unix)]
#[tokio::test]
async fn test_process_exiting_130_is_a_failure_not_a_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let runner = runner_in(dir.path());
    let task = Task::new(
        TaskSpec::command("sh", vec!["-c".into(), "exit 130".into()]),
        TaskRequirements::generic(1, 10),
    );

    let res = runner.execute_task(&task, None, None).await;
    assert!(!res.is_success());
    assert_ne!(
        res.exit_code, EXIT_CODE_CANCELLED,
        "130 is reserved for cancellation"
    );
    assert_eq!(res.exit_code, EXIT_CODE_INTERRUPTED);
    assert!(!res.error.as_deref().unwrap_or("").starts_with("Cancelled"));
}

#[cfg(unix)]
#[tokio::test]
async fn test_process_killed_by_a_signal_reports_it() {
    let dir = tempfile::tempdir().unwrap();
    let runner = runner_in(dir.path());
    let task = Task::new(
        TaskSpec::command("sh", vec!["-c".into(), "kill -TERM $$".into()]),
        TaskRequirements::generic(1, 10),
    );

    let res = runner.execute_task(&task, None, None).await;
    assert_eq!(res.exit_code, 128 + 15);
    let error = res.error.expect("a signalled process must say so");
    assert!(
        error.contains("signal 15") && error.contains("SIGTERM"),
        "{error}"
    );
}

#[tokio::test]
async fn test_dropped_cancel_sender_is_not_a_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let runner = runner_in(dir.path());

    // A process task...
    #[cfg(unix)]
    {
        let (tx, rx) = oneshot::channel::<String>();
        drop(tx);
        let task = Task::new(
            TaskSpec::command("sh", vec!["-c".into(), "sleep 0.3; echo survived".into()]),
            TaskRequirements::generic(1, 10),
        );
        let res = runner.execute_task(&task, None, Some(rx)).await;
        assert!(
            res.is_success(),
            "a vanished cancel sender must not kill the task: {res:?}"
        );
        assert_eq!(res.stdout_str().trim(), "survived");
    }

    // ...and an in-memory one.
    let (tx, rx) = oneshot::channel::<String>();
    drop(tx);
    let task = Task::new(
        TaskSpec::BuiltinTest {
            test_name: "sleep".into(),
            iterations: 0,
            duration_ms: 200,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 10),
    );
    let res = runner.execute_task(&task, None, Some(rx)).await;
    assert!(res.is_success(), "{res:?}");
}

#[tokio::test]
async fn test_cancellation_helper_ignores_a_closed_channel_but_delivers_a_reason() {
    let mut none: Option<oneshot::Receiver<String>> = None;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), cancellation(&mut none))
            .await
            .is_err()
    );

    let (tx, rx) = oneshot::channel::<String>();
    drop(tx);
    let mut closed = Some(rx);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), cancellation(&mut closed))
            .await
            .is_err()
    );

    let (tx, rx) = oneshot::channel::<String>();
    tx.send("because".into()).unwrap();
    let mut sent = Some(rx);
    assert_eq!(cancellation(&mut sent).await, "because");
}

#[cfg(unix)]
#[test]
fn test_default_shell_lookup_order() {
    let dir = tempfile::tempdir().unwrap();
    let path_dir = dir.path().join("bin");
    std::fs::create_dir_all(&path_dir).unwrap();
    let path_sh = path_dir.join("sh");
    std::fs::write(&path_sh, "#!/bin/sh\n").unwrap();
    let preferred = dir.path().join("preferred_sh");
    let fallback = dir.path().join("fallback_sh");
    std::fs::write(&preferred, "").unwrap();
    std::fs::write(&fallback, "").unwrap();
    let (preferred, fallback) = (preferred.to_str().unwrap(), fallback.to_str().unwrap());
    let path = path_dir.to_str().unwrap();
    let missing = dir.path().join("does_not_exist");
    let missing = missing.to_str().unwrap();

    // /bin/sh (the preferred location) wins where it exists...
    assert_eq!(pick_shell(&[preferred], path, &[fallback]), preferred);
    // ...Termux-style: no /bin/sh, but an `sh` on PATH...
    assert_eq!(
        pick_shell(&[missing], path, &[fallback]),
        path_sh.to_str().unwrap()
    );
    // ...Android: no /bin/sh and none on PATH, the system shell...
    assert_eq!(pick_shell(&[missing], "", &[fallback]), fallback);
    assert_eq!(pick_shell(&[missing], missing, &[fallback]), fallback);
    // ...and the bare name as the last resort, never a hard-coded path that does not exist.
    assert_eq!(pick_shell(&[missing], "", &[missing]), "sh");
}

#[cfg(unix)]
#[test]
fn test_default_shell_exists_on_this_host() {
    let shell = resolve_default_shell(&std::env::var("PATH").unwrap_or_default());
    assert!(
        Path::new(&shell).is_file(),
        "resolved shell {shell:?} must exist"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn test_shell_script_without_interpreter_runs_via_the_resolved_shell() {
    let dir = tempfile::tempdir().unwrap();
    let runner = runner_in(dir.path());
    let task = Task::new(
        TaskSpec::shell_script("echo via-default-shell"),
        TaskRequirements::generic(1, 10),
    );
    let res = runner.execute_task(&task, None, None).await;
    assert!(res.is_success(), "{res:?}");
    assert_eq!(res.stdout_str().trim(), "via-default-shell");
}

#[tokio::test]
async fn test_read_output_file_regular_missing_and_directory() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let file_path = temp_dir.path().join("model.glb");
    let non_existent = temp_dir.path().join("missing.glb");
    let dir_path = temp_dir.path().join("subdir");
    tokio::fs::create_dir(&dir_path).await.expect("create dir");

    // Missing file -> Ok(None)
    let res = read_output_file(&non_existent).await.expect("read missing");
    assert!(res.is_none());

    // Regular file -> Ok(Some(data))
    tokio::fs::write(&file_path, b"glTF test content")
        .await
        .expect("write file");
    let res = read_output_file(&file_path).await.expect("read file");
    assert_eq!(res, Some(b"glTF test content".to_vec()));

    // Directory -> Err
    let err = read_output_file(&dir_path).await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert!(err
        .to_string()
        .contains("output path is not a regular file"));
}

#[cfg(unix)]
#[tokio::test]
async fn test_read_output_file_fifo_returns_error_immediately() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let fifo_path = temp_dir.path().join("fifo_output.glb");
    let c_path = std::ffi::CString::new(fifo_path.to_str().expect("utf8 path")).expect("cstring");
    // SAFETY: `c_path` is a NUL-terminated path in a private temp dir, live across the call.
    let rc = unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) };
    assert_eq!(rc, 0, "mkfifo failed");

    let start = std::time::Instant::now();
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_output_file(&fifo_path),
    )
    .await
    .expect("read_output_file must not hang on FIFO")
    .expect_err("FIFO output must be refused");
    assert!(
        start.elapsed() < std::time::Duration::from_millis(500),
        "FIFO check took {:?}",
        start.elapsed()
    );
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        err.to_string()
            .contains("output path is not a regular file"),
        "unexpected FIFO error: {err}"
    );
}

#[tokio::test]
async fn test_read_output_file_refuses_symlink_and_oversized_or_sparse_file() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let target_file = temp_dir.path().join("target.glb");
    tokio::fs::write(&target_file, b"SECRET_SYMLINK_TARGET_BYTES")
        .await
        .expect("write target file");

    // 1. Symbolic link / junction must be refused via open-without-follow
    let link_path = temp_dir.path().join("link_model.glb");
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&target_file, &link_path).expect("create unix symlink");
    }
    #[cfg(windows)]
    {
        if std::os::windows::fs::symlink_file(&target_file, &link_path).is_err() {
            let target_dir = temp_dir.path().join("target_dir");
            std::fs::create_dir(&target_dir).expect("create target_dir");
            let status = std::process::Command::new("cmd")
                .args(["/C", "mklink", "/J"])
                .arg(&link_path)
                .arg(&target_dir)
                .status()
                .expect("run mklink /J");
            assert!(status.success(), "mklink /J must succeed");
        }
    }

    let link_err = read_output_file(&link_path)
        .await
        .expect_err("symlink output must be refused");
    assert_eq!(link_err.kind(), std::io::ErrorKind::InvalidData);
    let link_msg = link_err.to_string();
    assert!(
        link_msg.contains("symbolic link") && link_msg.contains(&link_path.display().to_string()),
        "expected symlink error naming path '{}', got: '{link_msg}'",
        link_path.display()
    );

    // 2. Concurrent swap between regular file and symlink must never read symlink target
    let swap_path = temp_dir.path().join("swap_model.glb");
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_clone = stop.clone();
    let swap_path_bg = swap_path.clone();
    let target_bg = target_file.clone();
    let temp_bg = temp_dir.path().to_path_buf();
    let swapper = std::thread::spawn(move || {
        let mut i = 0u64;
        while !stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
            let tmp_reg = temp_bg.join(format!("tmp_reg_{i}.glb"));
            let _ = std::fs::write(&tmp_reg, b"REGULAR_GLTF_BYTES");
            let _ = std::fs::rename(&tmp_reg, &swap_path_bg);
            #[cfg(unix)]
            {
                let tmp_lnk = temp_bg.join(format!("tmp_lnk_{i}.glb"));
                let _ = std::os::unix::fs::symlink(&target_bg, &tmp_lnk);
                let _ = std::fs::rename(&tmp_lnk, &swap_path_bg);
            }
            #[cfg(windows)]
            {
                let _ = std::fs::remove_file(&swap_path_bg);
                let _ = std::os::windows::fs::symlink_file(&target_bg, &swap_path_bg);
            }
            i = i.wrapping_add(1);
        }
    });
    for _ in 0..80 {
        if let Ok(Some(bytes)) = read_output_file(&swap_path).await {
            assert_ne!(
                bytes.as_slice(),
                b"SECRET_SYMLINK_TARGET_BYTES",
                "read_output_file must never follow a swapped symlink"
            );
            assert_eq!(bytes.as_slice(), b"REGULAR_GLTF_BYTES");
        }
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = swapper.join();

    // 3. File at smallest size above MAX_FRAME_SIZE (64 MiB + 1 byte) must be refused without reading
    let max_bytes = rusty_grid_core::protocol::MAX_FRAME_SIZE as u64;
    let oversized_path = temp_dir.path().join("oversized.glb");
    let oversized_len = max_bytes + 1;
    let lock_file = {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&oversized_path)
            .expect("create oversized file");
        #[cfg(windows)]
        {
            let _ = std::process::Command::new("fsutil")
                .args(["sparse", "setflag"])
                .arg(&oversized_path)
                .output();
        }
        f.set_len(oversized_len).expect("set_len oversized");
        #[cfg(windows)]
        {
            // Hold a mandatory exclusive lock on Windows so any attempted read fails with OS error 33
            f.lock().expect("lock oversized file");
        }
        f
    };
    let over_err = read_output_file(&oversized_path)
        .await
        .expect_err("file above MAX_FRAME_SIZE must be refused before reading");
    drop(lock_file);
    assert_eq!(over_err.kind(), std::io::ErrorKind::InvalidData);
    let over_msg = over_err.to_string();
    assert!(
        over_msg.contains(&oversized_path.display().to_string())
            && over_msg.contains(&format!("is {oversized_len} bytes"))
            && !over_msg.contains("is more than"),
        "expected pre-read error naming path '{}' and exact size 'is {oversized_len} bytes', got: '{over_msg}'",
        oversized_path.display()
    );

    // 4. File that grows past MAX_FRAME_SIZE while being read is caught by the capped read ("is more than")
    let growing_path = temp_dir.path().join("growing.glb");
    {
        let f = std::fs::File::create(&growing_path).expect("create growing file");
        #[cfg(windows)]
        {
            let _ = std::process::Command::new("fsutil")
                .args(["sparse", "setflag"])
                .arg(&growing_path)
                .output();
        }
        f.set_len(max_bytes).expect("set_len max_bytes");
    }
    let mut observed_capped_err = false;
    for delay_us in [
        0u64, 50, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 40_000,
    ] {
        let writer = std::fs::OpenOptions::new()
            .write(true)
            .open(&growing_path)
            .expect("open growing file for write");
        writer.set_len(max_bytes).expect("reset to max_bytes");

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_writer = Arc::clone(&stop);
        let max_sparse_len = 16 * 1024 * 1024 * 1024u64;
        let step_bytes = 128 * 1024 * 1024u64;
        let handle = std::thread::spawn(move || {
            if delay_us > 0 {
                std::thread::sleep(std::time::Duration::from_micros(delay_us));
            }
            let mut target_len = max_bytes + step_bytes;
            let _ = writer.set_len(target_len);
            // Keep advancing the sparse EOF until `read_output_file` returns.
            // With `.take(max_bytes + 1)`, the reader stops immediately at `max_bytes + 1`
            // bytes; if `.take(max_bytes + 1)` is removed, `read_to_end` chases the
            // growing sparse file past `max_bytes + 1` until the timeout below fires.
            while !stop_writer.load(std::sync::atomic::Ordering::Relaxed)
                && target_len < max_sparse_len
            {
                target_len = (target_len + step_bytes).min(max_sparse_len);
                let _ = writer.set_len(target_len);
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        });

        let read_res = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            read_output_file(&growing_path),
        )
        .await;
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = handle.join();

        match read_res.expect(
            "read_output_file timed out on a growing sparse file; read must be capped by .take(MAX_FRAME_SIZE + 1)",
        ) {
            Ok(Some(data)) => {
                assert!(
                    (data.len() as u64) <= max_bytes,
                    "read_output_file must never return more than MAX_FRAME_SIZE bytes (got {})",
                    data.len()
                );
            }
            Ok(None) => panic!("growing file must exist"),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains(&format!("is more than {max_bytes} bytes")) {
                    assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
                    assert!(msg.contains(&growing_path.display().to_string()));
                    observed_capped_err = true;
                    break;
                }
            }
        }
    }
    assert!(
        observed_capped_err,
        "capped read check ('is more than {max_bytes} bytes') must trigger when file grows during read"
    );
}

#[cfg(windows)]
#[tokio::test]
async fn test_resolve_windows_executable_skips_extensionless_and_invalid_pe_files() {
    let fake_dir = tempfile::tempdir().unwrap();
    let sandbox_dir = tempfile::tempdir().unwrap();
    // Place an extensionless 'cmd' file and a 0-byte 'xcopy.exe' stub ahead of System32 on PATH.
    // Without executable-extension and MZ-header validation, resolving bare 'cmd' or 'xcopy'
    // picks these non-Win32 files and fails CreateProcessW with os error 193.
    std::fs::write(fake_dir.path().join("cmd"), b"#!/bin/sh\necho not-win32\n").unwrap();
    std::fs::write(fake_dir.path().join("xcopy.exe"), b"").unwrap();

    let sys_path = std::env::var("PATH").unwrap_or_default();
    let poisoned_path = format!("{};{sys_path}", fake_dir.path().display());

    let resolved_cmd = resolve_windows_executable("cmd", &poisoned_path);
    assert!(
        resolved_cmd.to_ascii_lowercase().ends_with("cmd.exe"),
        "bare 'cmd' must resolve to cmd.exe, not extensionless file: {resolved_cmd}"
    );
    assert_ne!(
        Path::new(&resolved_cmd),
        fake_dir.path().join("cmd"),
        "extensionless file must never be selected on Windows"
    );

    let resolved_xcopy = resolve_windows_executable("xcopy", &poisoned_path);
    assert_ne!(
        Path::new(&resolved_xcopy),
        fake_dir.path().join("xcopy.exe"),
        "0-byte .exe stub without MZ header must be skipped"
    );

    let runner = runner_in(sandbox_dir.path());
    let mut env = HashMap::new();
    env.insert("PATH".to_string(), poisoned_path);
    let task = Task::new(
        TaskSpec::Command {
            program: "cmd".into(),
            args: vec!["/C".into(), "echo".into(), "win32-spawn-ok".into()],
            env,
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 10),
    );
    let res = runner.execute_task(&task, None, None).await;
    assert!(
        res.is_success(),
        "cmd must spawn cleanly even when an extensionless 'cmd' precedes System32 on PATH: {res:?}"
    );
    assert_eq!(res.stdout_str().trim(), "win32-spawn-ok");
}

#[cfg(windows)]
#[tokio::test]
async fn test_resolve_windows_executable_explicit_path_and_pathext_order() {
    let fake_dir = tempfile::tempdir().unwrap();
    let sandbox_dir = tempfile::tempdir().unwrap();

    // 1. An explicit path (with path separator) that fails validation must return the
    //    given path unchanged instead of falling through to PATH for its base name.
    let bad_explicit_cmd = fake_dir.path().join("cmd");
    std::fs::write(&bad_explicit_cmd, b"#!/bin/sh\necho not-win32\n").unwrap();
    let bad_explicit_str = bad_explicit_cmd.to_string_lossy().to_string();
    let sys_path = std::env::var("PATH").unwrap_or_default();

    assert_eq!(
        resolve_windows_executable(&bad_explicit_str, &sys_path),
        bad_explicit_str,
        "explicit path that fails validation must not fall through to System32\\cmd.exe on PATH"
    );

    // 2. When spawning a bare command whose resolved path differs from `program` fails
    //    (e.g. a 2-byte 'MZ' stub that passes the 2-byte header check in fake_dir but is
    //    not a complete PE image), both stderr and result.error must include the resolved
    //    full path in fake_dir, not merely the bare program name 'brokenpe'.
    let broken_pe = fake_dir.path().join("brokenpe.exe");
    std::fs::write(&broken_pe, b"MZ").unwrap();
    let broken_pe_str = broken_pe.to_string_lossy().to_string();
    let runner = runner_in(sandbox_dir.path());
    let mut broken_env = HashMap::new();
    broken_env.insert(
        "PATH".to_string(),
        fake_dir.path().to_string_lossy().to_string(),
    );
    let task = Task::new(
        TaskSpec::Command {
            program: "brokenpe".into(),
            args: vec![],
            env: broken_env,
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 10),
    );
    let res = runner.execute_task(&task, None, None).await;
    assert!(!res.is_success(), "2-byte MZ stub must fail CreateProcessW");
    assert!(
        res.stderr_str().contains(&broken_pe_str),
        "stderr must include resolved path '{broken_pe_str}' (distinct from program 'brokenpe'), got: {}",
        res.stderr_str()
    );
    let err_msg = res.error.as_deref().unwrap_or_default();
    assert!(
        err_msg.contains(&broken_pe_str),
        "result.error must include resolved path '{broken_pe_str}' (distinct from program 'brokenpe'), got: {err_msg}"
    );

    // 3. PATHEXT order: .com (with MZ) before .exe (with MZ) before .bat before .cmd.
    std::fs::write(fake_dir.path().join("prio.com"), b"MZfake-com").unwrap();
    std::fs::write(fake_dir.path().join("prio.exe"), b"MZfake-exe").unwrap();
    std::fs::write(fake_dir.path().join("prio.bat"), b"@echo bat\r\n").unwrap();
    std::fs::write(fake_dir.path().join("prio.cmd"), b"@echo cmd\r\n").unwrap();
    let only_fake_path = fake_dir.path().to_string_lossy().to_string();
    assert_eq!(
        Path::new(&resolve_windows_executable("prio", &only_fake_path)),
        fake_dir.path().join("prio.com")
    );
    std::fs::remove_file(fake_dir.path().join("prio.com")).unwrap();
    assert_eq!(
        Path::new(&resolve_windows_executable("prio", &only_fake_path)),
        fake_dir.path().join("prio.exe")
    );
    std::fs::remove_file(fake_dir.path().join("prio.exe")).unwrap();
    assert_eq!(
        Path::new(&resolve_windows_executable("prio", &only_fake_path)),
        fake_dir.path().join("prio.bat")
    );
    std::fs::remove_file(fake_dir.path().join("prio.bat")).unwrap();
    assert_eq!(
        Path::new(&resolve_windows_executable("prio", &only_fake_path)),
        fake_dir.path().join("prio.cmd")
    );
}

#[cfg(windows)]
#[tokio::test]
async fn test_task_runner_relative_windows_program_paths_and_refused_escape() {
    use rusty_grid_core::task::StagedFile;

    let base_dir = tempfile::tempdir().unwrap();
    let sandboxes = base_dir.path().join("sandboxes");
    std::fs::create_dir_all(&sandboxes).unwrap();
    let canary = base_dir.path().join("escape_canary.txt");

    // Place a decoy script directly under sandboxes/ (parent of any task's sandbox dir) so
    // `..\decoy.cmd` would hit it if relative traversal were not refused.
    let parent_decoy = sandboxes.join("decoy.cmd");
    std::fs::write(
        &parent_decoy,
        format!("@echo PWNED > \"{}\"\r\n", canary.display()),
    )
    .unwrap();

    let runner = runner_in(&sandboxes);
    let root_cmd = StagedFile::new("prog.cmd", b"@echo ROOT_CMD:%1\r\n".to_vec(), true);
    let sub_cmd = StagedFile::new("sub/prog.cmd", b"@echo SUB_CMD:%1\r\n".to_vec(), true);

    for (prog, working_dir, expected) in [
        ("./prog.cmd", None, "ROOT_CMD:42"),
        (r".\prog.cmd", None, "ROOT_CMD:42"),
        ("sub/prog.cmd", None, "SUB_CMD:42"),
        (r"sub\prog.cmd", None, "SUB_CMD:42"),
        (r".\prog.cmd", Some(PathBuf::from("sub")), "SUB_CMD:42"),
    ] {
        let task = Task::new(
            TaskSpec::Command {
                program: prog.into(),
                args: vec!["42".into()],
                env: HashMap::new(),
                working_dir,
                stdin: None,
            },
            TaskRequirements::generic(1, 10),
        )
        .with_staged_files(vec![root_cmd.clone(), sub_cmd.clone()]);
        let res = runner.execute_task(&task, None, None).await;
        assert!(
            res.is_success(),
            "relative program '{prog}' failed: {:?} / {} / {}",
            res.error,
            res.stdout_str(),
            res.stderr_str()
        );
        assert_eq!(res.stdout_str().trim(), expected, "program={prog}");
    }

    for escaping in [r"..\decoy.cmd", "../decoy.cmd"] {
        let task = Task::new(
            TaskSpec::Command {
                program: escaping.into(),
                args: Vec::new(),
                env: HashMap::new(),
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, 10),
        );
        let res = runner.execute_task(&task, None, None).await;
        assert!(
            !res.is_success(),
            "escaping relative program '{escaping}' must fail"
        );
        let err_msg = res.error.as_deref().unwrap_or_default();
        assert!(
            err_msg.contains("Invalid program path") && err_msg.contains("escapes"),
            "expected escape refusal in error for '{escaping}', got: {err_msg}"
        );
        assert!(
            !canary.exists(),
            "escaping relative program '{escaping}' must never execute parent decoy"
        );
    }
}
