//! Taking down every running task tree at once, for a shutdown that cannot unwind (a SIGTERM or
//! console-close handler): `terminate_all_task_process_trees`.
//!
//! This is the only test in its binary on purpose: the call kills *every* live task tree of the
//! process, which would be a race against any other test that has a task running.
#![cfg(unix)]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_worker::{
    terminate_all_task_process_trees, RunnerConfig, SandboxConfig, TaskRunner, EXIT_CODE_TIMEOUT,
};
use uuid::Uuid;

fn alive(pid: u32) -> bool {
    let mut sys = sysinfo::System::new();
    sys.refresh_processes();
    sys.process(sysinfo::Pid::from(pid as usize))
        .is_some_and(|p| p.status() != sysinfo::ProcessStatus::Zombie)
}

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

#[tokio::test]
async fn test_terminate_all_task_process_trees_kills_every_running_tree_and_nothing_else() {
    let mut bystander = std::process::Command::new("sleep")
        .arg("300")
        .spawn()
        .unwrap();
    let bystander_pid = bystander.id();

    let sandboxes = tempfile::tempdir().unwrap();
    let pids = tempfile::tempdir().unwrap();
    let caps = WorkerCapabilities {
        name: "shutdown-test-worker".into(),
        cpu_cores: 4,
        ram_mb: 8192,
        has_gpu: false,
        is_simulated_gpu: false,
        gpu_device_name: None,
        tags: vec![],
        mobile: None,
    };
    let runner = Arc::new(TaskRunner::new(
        Uuid::new_v4(),
        caps,
        RunnerConfig::new(SandboxConfig::new(sandboxes.path())),
    ));

    // Two tasks, each with a background child and a nested grandchild.
    let mut runs = Vec::new();
    let mut expected_dead = Vec::new();
    for n in 0..2 {
        let child = pids.path().join(format!("child{n}.pid"));
        let grandchild = pids.path().join(format!("grandchild{n}.pid"));
        let script = format!(
            "sleep 300 &\necho $! > '{}'\nsh -c 'sleep 300 & echo $! > \"$0\"; wait' '{}' &\nwait\n",
            child.display(),
            grandchild.display()
        );
        let task = Task::new(
            TaskSpec::shell_script(script),
            TaskRequirements::generic(1, 120),
        );
        let runner = Arc::clone(&runner);
        runs.push(tokio::spawn(async move {
            runner.execute_task(&task, None, None).await
        }));
        expected_dead.push(child);
        expected_dead.push(grandchild);
    }
    let mut pids_to_check = Vec::new();
    for file in &expected_dead {
        let pid = recorded_pid(file).await;
        assert!(alive(pid));
        pids_to_check.push(pid);
    }

    let killed = terminate_all_task_process_trees();
    assert_eq!(killed, 2, "both running trees were registered");

    for run in runs {
        let res = tokio::time::timeout(Duration::from_secs(10), run)
            .await
            .expect("a run whose tree was killed must end")
            .unwrap();
        assert!(!res.is_success());
        assert_ne!(
            res.exit_code, EXIT_CODE_TIMEOUT,
            "killed, not timed out: {res:?}"
        );
    }
    for pid in pids_to_check {
        let mut dead = false;
        for _ in 0..100 {
            if !alive(pid) {
                dead = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if !dead {
            unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
        }
        assert!(
            dead,
            "process {pid} survived terminate_all_task_process_trees"
        );
    }

    // Once the runs have ended nothing is registered any more, and the bystander was never touched.
    assert_eq!(terminate_all_task_process_trees(), 0);
    let bystander_alive = alive(bystander_pid);
    let _ = bystander.kill();
    let _ = bystander.wait();
    assert!(bystander_alive, "an unrelated process was killed");
}
