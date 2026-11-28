use super::*;
use rusty_grid_core::error::GridError;
use rusty_grid_core::task::{CheckpointData, TaskRequirements, TaskSpec};

#[tokio::test]
async fn test_fsm_valid_transitions() {
    assert!(TaskState::Submitted.can_transition_to(TaskState::Queued));
    assert!(TaskState::Queued.can_transition_to(TaskState::Scheduled));
    assert!(TaskState::Scheduled.can_transition_to(TaskState::Running));
    assert!(TaskState::Scheduled.can_transition_to(TaskState::Completed)); // Fast finish
    assert!(TaskState::Running.can_transition_to(TaskState::Completed));
    assert!(TaskState::Running.can_transition_to(TaskState::Failed));
    assert!(TaskState::Running.can_transition_to(TaskState::Retrying));
    assert!(TaskState::Retrying.can_transition_to(TaskState::Queued));
}

#[tokio::test]
async fn test_fsm_terminal_invariance() {
    assert!(!TaskState::Completed.can_transition_to(TaskState::Queued));
    assert!(!TaskState::Failed.can_transition_to(TaskState::Running));
    assert!(!TaskState::Cancelled.can_transition_to(TaskState::Completed));
}

#[tokio::test]
async fn test_priority_fifo_ordering() {
    let queue = TaskQueue::new();

    let t1 = Task::new(
        TaskSpec::command("echo", vec!["t1".into()]),
        TaskRequirements::generic(1, 10),
    );
    let t2 = Task::new(
        TaskSpec::command("echo", vec!["t2".into()]),
        TaskRequirements::generic(1, 10),
    );
    let t3 = Task::new(
        TaskSpec::command("echo", vec!["t3".into()]),
        TaskRequirements::generic(1, 10),
    );

    // Submit t1 priority 0, t2 priority 10, t3 priority 0
    let id1 = queue.submit_with_priority(t1, 0).await.unwrap();
    let id2 = queue.submit_with_priority(t2, 10).await.unwrap();
    let id3 = queue.submit_with_priority(t3, 0).await.unwrap();

    let w_id = Uuid::new_v4();

    // 1st pop must be t2 (highest priority = 10)
    let pop1 = queue.pop_and_schedule(w_id, |_| true).await.unwrap();
    assert_eq!(pop1.id, id2);

    // 2nd pop must be t1 (priority 0, earlier sequence than t3)
    let pop2 = queue.pop_and_schedule(w_id, |_| true).await.unwrap();
    assert_eq!(pop2.id, id1);

    // 3rd pop must be t3
    let pop3 = queue.pop_and_schedule(w_id, |_| true).await.unwrap();
    assert_eq!(pop3.id, id3);

    assert!(queue.pop_and_schedule(w_id, |_| true).await.is_none());
}

#[tokio::test]
async fn test_fast_finishing_task_scheduled_to_completed() {
    let queue = TaskQueue::new();
    let task = Task::new(
        TaskSpec::command("echo", vec!["fast".into()]),
        TaskRequirements::generic(1, 10),
    );
    let task_id = queue.submit(task).await.unwrap();
    let worker_id = Uuid::new_v4();

    let scheduled_task = queue.pop_and_schedule(worker_id, |_| true).await.unwrap();
    assert_eq!(scheduled_task.id, task_id);
    assert_eq!(
        queue.get_state(&task_id).await.unwrap(),
        TaskState::Scheduled
    );

    // Record successful result directly without mark_running
    let result = TaskResult::success(worker_id, task_id, "fast\n", 5, false);
    let state = queue.record_result(result).await.unwrap();
    assert_eq!(state, TaskState::Completed);
    assert_eq!(
        queue.get_state(&task_id).await.unwrap(),
        TaskState::Completed
    );
}

#[tokio::test]
async fn test_worker_disconnect_reaping_and_backoff_advancement() {
    let policy = RetryPolicy {
        initial_backoff_ms: 10,
        ..Default::default()
    };
    let queue = TaskQueue::with_config(policy, None);

    let task = Task::new(
        TaskSpec::command("echo", vec!["retry".into()]),
        TaskRequirements::generic(1, 10),
    );
    let task_id = queue.submit(task).await.unwrap();
    let worker_id = Uuid::new_v4();

    let _ = queue.pop_and_schedule(worker_id, |_| true).await.unwrap();
    queue.mark_running(&task_id, worker_id).await.unwrap();

    // Worker drops
    let affected = queue
        .handle_worker_disconnected(&worker_id, "Connection severed", false)
        .await;
    assert_eq!(affected, vec![task_id]);
    assert_eq!(
        queue.get_state(&task_id).await.unwrap(),
        TaskState::Retrying
    );

    // Immediately before backoff elapsed: 0 ready
    assert_eq!(queue.process_delayed_retries().await, 0);

    // Wait 15ms for backoff to pass
    tokio::time::sleep(Duration::from_millis(15)).await;
    assert_eq!(queue.process_delayed_retries().await, 1);
    assert_eq!(queue.get_state(&task_id).await.unwrap(), TaskState::Queued);

    // Schedulable again to survivor worker
    let survivor_id = Uuid::new_v4();
    let reassigned = queue.pop_and_schedule(survivor_id, |_| true).await.unwrap();
    assert_eq!(reassigned.id, task_id);
}

#[tokio::test]
async fn test_cancel_task_populates_entry_result() {
    let queue = TaskQueue::new();
    let task = Task::new(
        TaskSpec::command("echo", vec!["cancel_me".into()]),
        TaskRequirements::generic(1, 10),
    );
    let task_id = queue.submit(task).await.unwrap();
    queue
        .cancel_task(&task_id, Some("user requested".into()))
        .await
        .unwrap();

    assert_eq!(
        queue.get_state(&task_id).await.unwrap(),
        TaskState::Cancelled
    );
    let res = queue
        .get_result(&task_id)
        .await
        .expect("result must be present");
    assert_eq!(res.exit_code, 130);
    assert_eq!(res.task_id, task_id);
    assert_eq!(res.error.as_deref(), Some("user requested"));
}

#[tokio::test]
async fn test_worker_disconnect_exhaustion_populates_entry_result() {
    let policy = RetryPolicy {
        max_retries: 0,
        ..Default::default()
    };
    let queue = TaskQueue::with_config(policy, None);
    let task = Task::new(
        TaskSpec::command("echo", vec!["exhaust".into()]),
        TaskRequirements::generic(1, 10),
    );
    let task_id = queue.submit(task).await.unwrap();
    let worker_id = Uuid::new_v4();
    let _ = queue.pop_and_schedule(worker_id, |_| true).await.unwrap();

    let affected = queue
        .handle_worker_disconnected(&worker_id, "crash", false)
        .await;
    assert_eq!(affected, vec![task_id]);
    assert_eq!(queue.get_state(&task_id).await.unwrap(), TaskState::Failed);
    let res = queue
        .get_result(&task_id)
        .await
        .expect("result must be present on failure");
    assert_eq!(res.exit_code, 1);
    assert!(res
        .error
        .as_deref()
        .unwrap_or("")
        .contains("retries exhausted"));
}

#[tokio::test]
async fn test_immediate_graceful_disconnect_and_per_task_max_retries() {
    // Policy allows 5 retries by default
    let policy = RetryPolicy {
        max_retries: 5,
        initial_backoff_ms: 10_000, // Large backoff to verify immediate bypasses it
        ..Default::default()
    };
    let queue = TaskQueue::with_config(policy, None);

    // Task with per-task max_retries = 1 override
    let task = Task::new(
        TaskSpec::command("echo", vec!["per_task".into()]),
        TaskRequirements::generic(1, 10).with_max_retries(1),
    );
    let task_id = queue.submit(task).await.unwrap();
    let worker_1 = Uuid::new_v4();
    let _ = queue.pop_and_schedule(worker_1, |_| true).await.unwrap();
    queue.mark_running(&task_id, worker_1).await.unwrap();

    // Graceful disconnect (immediate_reschedule = true)
    let affected = queue
        .handle_worker_disconnected(&worker_1, "Graceful shutdown", true)
        .await;
    assert_eq!(affected, vec![task_id]);

    // State must immediately be Queued (ready for re-scheduling, 0ms delay)
    assert_eq!(
        queue.get_state(&task_id).await.unwrap(),
        TaskState::Queued,
        "Immediate reschedule must transition directly to Queued"
    );

    let info = queue.get_task(&task_id).await.unwrap();
    assert_eq!(info.retry_count, 1);

    // Schedule to worker 2
    let worker_2 = Uuid::new_v4();
    let sched = queue.pop_and_schedule(worker_2, |_| true).await;
    assert!(sched.is_some(), "Task must be immediately schedulable");

    // Worker 2 disconnects -> should exceed per-task max_retries (1) and fail!
    let affected2 = queue
        .handle_worker_disconnected(&worker_2, "Worker 2 crash", false)
        .await;
    assert_eq!(affected2, vec![task_id]);
    assert_eq!(queue.get_state(&task_id).await.unwrap(), TaskState::Failed);

    let res = queue.get_result(&task_id).await.unwrap();
    assert_eq!(res.exit_code, 1);
    let err = res.error.unwrap();
    assert!(
        err.contains("maximum retry limit (1) reached"),
        "Must record max retry limit 1: {err}"
    );
}

#[tokio::test]
async fn test_terminal_task_lru_retention_and_eviction() {
    let queue = TaskQueue::with_retention(RetryPolicy::default(), None, 3);
    let worker_id = Uuid::new_v4();

    let mut ids = Vec::new();
    for i in 0..5 {
        let task = Task::new(
            TaskSpec::command("echo", vec![format!("{i}")]),
            TaskRequirements::generic(1, 10),
        );
        let tid = queue.submit(task).await.unwrap();
        ids.push(tid);
        let _ = queue.pop_and_schedule(worker_id, |_| true).await;
        queue.mark_running(&tid, worker_id).await.unwrap();
        let res = TaskResult::success(worker_id, tid, "ok", 10, false);
        queue.record_result(res).await.unwrap();
    }

    // We submitted and completed 5 tasks with retention limit = 3.
    // Oldest 2 tasks (ids[0] and ids[1]) should have been evicted.
    assert!(queue.get_task(&ids[0]).await.is_none());
    assert!(queue.get_result(&ids[0]).await.is_none());
    assert!(queue.get_task(&ids[1]).await.is_none());
    assert!(queue.get_result(&ids[1]).await.is_none());

    // Newest 3 tasks (ids[2], ids[3], ids[4]) must be retained.
    assert!(queue.get_task(&ids[2]).await.is_some());
    assert!(queue.get_task(&ids[3]).await.is_some());
    assert!(queue.get_task(&ids[4]).await.is_some());

    // Dynamic eviction down to 1
    let evicted = queue.evict_terminal_tasks(1).await;
    assert_eq!(evicted, 2);
    assert!(queue.get_task(&ids[2]).await.is_none());
    assert!(queue.get_task(&ids[3]).await.is_none());
    assert!(queue.get_task(&ids[4]).await.is_some());
}

pub(super) fn echo_task(label: &str) -> Task {
    Task::new(
        TaskSpec::command("echo", vec![label.to_string()]),
        TaskRequirements::generic(1, 10),
    )
}

/// Submits `task`, assigns it to `(worker, session)` and returns its id.
async fn submit_and_assign(queue: &TaskQueue, task: Task, worker: Uuid, session: u64) -> TaskId {
    let id = queue.submit(task).await.unwrap();
    let scheduled = queue
        .schedule_tasks_batch_in_sessions(&[(id, worker, session)])
        .await;
    assert_eq!(scheduled.len(), 1);
    id
}

#[tokio::test]
async fn test_duplicate_task_id_is_rejected_and_live_entry_untouched() {
    let queue = TaskQueue::new();
    let worker = Uuid::new_v4();

    // Case A: the original is Running; a resubmit must not reset it to Queued.
    let task = echo_task("dup-running");
    let clone_of_task = task.clone();
    let id = submit_and_assign(&queue, task, worker, 1).await;
    queue.mark_running_from(worker, Some(1), &id).await.unwrap();
    let err = queue.submit(clone_of_task).await.unwrap_err();
    assert!(err.to_string().contains("already exists"), "{err}");
    assert_eq!(queue.get_state(&id).await, Some(TaskState::Running));
    assert!(queue.get_schedulable_tasks().await.is_empty());

    // Case B: the original is Queued; a resubmit must not add a second ready-queue key.
    let queued = echo_task("dup-queued");
    let queued_clone = queued.clone();
    let qid = queue.submit(queued).await.unwrap();
    assert!(queue.submit(queued_clone).await.is_err());
    assert_eq!(queue.get_schedulable_tasks().await.len(), 1);
    let popped = queue.pop_and_schedule(worker, |_| true).await.unwrap();
    assert_eq!(popped.id, qid);
    assert!(
        queue.pop_and_schedule(worker, |_| true).await.is_none(),
        "no stale key may offer the same task twice"
    );

    // Case C: the original is terminal; it must not be resurrected.
    let done = echo_task("dup-done");
    let done_clone = done.clone();
    let did = submit_and_assign(&queue, done, worker, 1).await;
    queue
        .record_result_from(
            worker,
            Some(1),
            TaskResult::success(worker, did, "ok", 5, false),
        )
        .await
        .unwrap();
    assert!(queue.submit(done_clone).await.is_err());
    assert_eq!(queue.get_state(&did).await, Some(TaskState::Completed));
}

#[tokio::test]
async fn test_stale_result_after_failover_is_dropped_and_leaves_no_poison_key() {
    let queue = TaskQueue::new();
    let (w1, w2) = (Uuid::new_v4(), Uuid::new_v4());
    let id = submit_and_assign(&queue, echo_task("stale"), w1, 1).await;
    queue.mark_running_from(w1, Some(1), &id).await.unwrap();

    // w1's session is lost; the task goes back to the queue
    let affected = queue
        .handle_session_disconnected(&w1, Some(1), "lost", true)
        .await;
    assert_eq!(affected, vec![id]);
    assert_eq!(queue.get_state(&id).await, Some(TaskState::Queued));

    // The zombie finishes anyway: both a failure and a success must be ignored.
    for zombie in [
        TaskResult::failure(w1, id, 1, "", "boom", 50, None),
        TaskResult::success(w1, id, "late", 50, false),
        TaskResult::failure(w1, id, 130, "", "", 0, None),
    ] {
        let outcome = queue.record_result_from(w1, Some(1), zombie).await.unwrap();
        assert_eq!(outcome.disposition, ResultDisposition::Stale);
        assert_eq!(outcome.state, TaskState::Queued);
    }
    assert_eq!(queue.get_state(&id).await, Some(TaskState::Queued));
    assert_eq!(
        queue.get_schedulable_tasks().await.len(),
        1,
        "exactly one live ready-queue key, no poisoned duplicates"
    );

    // It is still schedulable, and only the new owner can finish it.
    assert_eq!(
        queue
            .schedule_tasks_batch_in_sessions(&[(id, w2, 7)])
            .await
            .len(),
        1
    );
    let wrong_session = queue
        .record_result_from(w2, Some(1), TaskResult::success(w2, id, "x", 1, false))
        .await
        .unwrap();
    assert_eq!(wrong_session.disposition, ResultDisposition::Stale);
    let right = queue
        .record_result_from(w2, Some(7), TaskResult::success(w2, id, "ok", 1, false))
        .await
        .unwrap();
    assert_eq!(right.disposition, ResultDisposition::Terminal);
    assert_eq!(queue.get_state(&id).await, Some(TaskState::Completed));
}

#[tokio::test]
async fn test_result_from_foreign_worker_cannot_complete_fail_or_cancel_a_task() {
    let queue = TaskQueue::new();
    let (owner, intruder) = (Uuid::new_v4(), Uuid::new_v4());
    let id = submit_and_assign(&queue, echo_task("owned"), owner, 1).await;
    queue.mark_running_from(owner, Some(1), &id).await.unwrap();

    // The intruder spoofs the owner's id in the body; the sender is the connection, not the body.
    for forged in [
        TaskResult::success(owner, id, "forged", 1, false),
        TaskResult::failure(owner, id, 1, "", "forged failure", 1, None),
        TaskResult::failure(owner, id, 130, "", "", 0, None),
    ] {
        let outcome = queue
            .record_result_from(intruder, Some(9), forged)
            .await
            .unwrap();
        assert_eq!(outcome.disposition, ResultDisposition::Stale);
    }
    assert_eq!(queue.get_state(&id).await, Some(TaskState::Running));

    // A failure result for a task that was never assigned (still Queued) must not fail it either.
    let queued = queue.submit(echo_task("never-assigned")).await.unwrap();
    let outcome = queue
        .record_result_from(
            intruder,
            Some(9),
            TaskResult::failure(intruder, queued, 1, "", "x", 1, None),
        )
        .await
        .unwrap();
    assert_eq!(outcome.disposition, ResultDisposition::Stale);
    assert_eq!(queue.get_state(&queued).await, Some(TaskState::Queued));
    assert_eq!(queue.get_schedulable_tasks().await.len(), 1);

    // Progress cannot reassign the task and checkpoints from the intruder are not stored.
    assert!(queue
        .mark_running_from(intruder, Some(9), &id)
        .await
        .is_err());
    assert_eq!(
        queue.get_task(&id).await.unwrap().assigned_worker_id,
        Some(owner)
    );
    // Same for a task that has not reported Running yet: a foreign "Running" report must neither
    // advance it nor take it over.
    let pending = submit_and_assign(&queue, echo_task("not-yet-running"), owner, 1).await;
    assert!(queue
        .mark_running_from(intruder, Some(9), &pending)
        .await
        .is_err());
    let info = queue.get_task(&pending).await.unwrap();
    assert_eq!(info.state, TaskState::Scheduled);
    assert_eq!(info.assigned_worker_id, Some(owner));
    queue
        .mark_running_from(owner, Some(1), &pending)
        .await
        .unwrap();
    let poison = CheckpointData::new(
        u64::MAX,
        rusty_grid_core::task::Bytes::copy_from_slice(b"junk"),
    );
    assert!(!queue
        .record_checkpoint_from(intruder, Some(9), id, poison)
        .await
        .unwrap());
    assert!(queue.get_checkpoint(&id).await.is_none());
    let genuine = CheckpointData::new(1, rusty_grid_core::task::Bytes::copy_from_slice(b"ok"));
    assert!(queue
        .record_checkpoint_from(owner, Some(1), id, genuine)
        .await
        .unwrap());
    assert_eq!(queue.get_checkpoint(&id).await.unwrap().sequence, 1);

    // The genuine result still goes through.
    let done = queue
        .record_result_from(
            owner,
            Some(1),
            TaskResult::success(owner, id, "real", 3, false),
        )
        .await
        .unwrap();
    assert_eq!(done.disposition, ResultDisposition::Terminal);
    assert_eq!(queue.get_result(&id).await.unwrap().stdout_str(), "real");
}

#[tokio::test]
async fn test_late_result_from_owner_of_cancelled_task_is_not_stale() {
    let queue = TaskQueue::new();
    let w = Uuid::new_v4();
    let id = submit_and_assign(&queue, echo_task("cancel-late"), w, 3).await;
    queue.mark_running_from(w, Some(3), &id).await.unwrap();
    let cancel = queue
        .cancel_task_detailed(&id, Some("stop".into()))
        .await
        .unwrap();
    assert!(cancel.newly_cancelled);
    assert_eq!(cancel.worker_to_notify, Some(w));
    assert_eq!(cancel.result.as_ref().unwrap().exit_code, 130);

    // The worker acknowledges with its own cancelled result: from the owner, so the caller
    // must still release the slot, but the state stays Cancelled.
    let outcome = queue
        .record_result_from(w, Some(3), TaskResult::failure(w, id, 130, "", "", 1, None))
        .await
        .unwrap();
    assert_eq!(outcome.disposition, ResultDisposition::Late);
    assert_eq!(outcome.state, TaskState::Cancelled);

    let again = queue.cancel_task_detailed(&id, None).await.unwrap();
    assert!(!again.newly_cancelled);
    assert!(queue
        .cancel_task_detailed(&TaskId::new(), None)
        .await
        .is_err());
}

#[tokio::test]
async fn test_session_scoped_failover_spares_the_newer_session() {
    let queue = TaskQueue::new();
    let w = Uuid::new_v4();
    let old = submit_and_assign(&queue, echo_task("old-session"), w, 1).await;
    let new = submit_and_assign(&queue, echo_task("new-session"), w, 2).await;

    let affected = queue
        .handle_session_disconnected(&w, Some(1), "superseded", true)
        .await;
    assert_eq!(
        affected,
        vec![old],
        "only the superseded session is failed over"
    );
    assert_eq!(queue.get_state(&old).await, Some(TaskState::Queued));
    assert_eq!(queue.get_state(&new).await, Some(TaskState::Scheduled));
    assert_eq!(queue.get_task(&new).await.unwrap().retry_count, 0);

    // Failing over the same session twice is a no-op: exactly once.
    assert!(queue
        .handle_session_disconnected(&w, Some(1), "again", true)
        .await
        .is_empty());
    assert_eq!(queue.get_task(&old).await.unwrap().retry_count, 1);

    // A session-less sweep (legacy callers) takes everything the worker still has.
    assert_eq!(
        queue.handle_worker_disconnected(&w, "gone", true).await,
        vec![new]
    );
}

#[tokio::test]
async fn test_sweep_of_a_later_session_recovers_tasks_stranded_on_an_older_one() {
    // Session 1's sweep never ran (its handler was aborted by session 2's registration, and
    // session 2's by session 3's): the sweep of session 2 must still take session 1's tasks.
    let queue = TaskQueue::new();
    let w = Uuid::new_v4();
    let first = submit_and_assign(&queue, echo_task("stranded-1"), w, 1).await;
    queue.mark_running_from(w, Some(1), &first).await.unwrap();
    let second = submit_and_assign(&queue, echo_task("stranded-2"), w, 2).await;
    let third = submit_and_assign(&queue, echo_task("live-3"), w, 3).await;

    let mut affected = queue
        .handle_session_disconnected(&w, Some(2), "superseded", true)
        .await;
    affected.sort_by_key(|id| id.0);
    let mut expected = vec![first, second];
    expected.sort_by_key(|id| id.0);
    assert_eq!(affected, expected, "sessions 1 and 2 are both dead");
    assert_eq!(queue.get_state(&first).await, Some(TaskState::Queued));
    assert_eq!(queue.get_state(&second).await, Some(TaskState::Queued));
    assert_eq!(
        queue.get_state(&third).await,
        Some(TaskState::Scheduled),
        "the newest session keeps what it was given"
    );
    assert_eq!(queue.get_task(&first).await.unwrap().retry_count, 1);
}

#[tokio::test]
async fn test_deliver_if_scheduled_only_runs_for_the_exact_owner_of_a_scheduled_task() {
    let queue = TaskQueue::new();
    let w = Uuid::new_v4();
    let id = submit_and_assign(&queue, echo_task("deliver"), w, 4).await;

    assert_eq!(queue.deliver_if_scheduled(&id, &w, 4, || 7).await, Some(7));
    // Another session of the same worker, another worker, an unknown task: nothing runs.
    assert_eq!(queue.deliver_if_scheduled(&id, &w, 5, || 7).await, None);
    assert_eq!(
        queue
            .deliver_if_scheduled(&id, &Uuid::new_v4(), 4, || 7)
            .await,
        None
    );
    assert_eq!(
        queue
            .deliver_if_scheduled(&TaskId::new(), &w, 4, || 7)
            .await,
        None
    );

    // Once the task is running, or cancelled, it is no longer the dispatcher's to deliver.
    queue.mark_running_from(w, Some(4), &id).await.unwrap();
    assert_eq!(queue.deliver_if_scheduled(&id, &w, 4, || 7).await, None);
    let cancelled = submit_and_assign(&queue, echo_task("deliver-cancelled"), w, 4).await;
    queue.cancel_task_detailed(&cancelled, None).await.unwrap();
    assert_eq!(
        queue.deliver_if_scheduled(&cancelled, &w, 4, || 7).await,
        None
    );
}

#[tokio::test]
async fn test_failover_has_its_own_budget_and_keeps_execution_retries_intact() {
    // Defaults: max_retries = 3 but max_failovers = 10 -> far more than 3 network handoffs survive.
    let queue = TaskQueue::new();
    let id = queue.submit(echo_task("roaming")).await.unwrap();
    for round in 1..=6u32 {
        let w = Uuid::new_v4();
        queue.schedule_task(&id, w).await.expect("schedulable");
        let affected = queue.handle_worker_disconnected(&w, "handoff", true).await;
        assert_eq!(affected, vec![id]);
        assert_eq!(
            queue.get_state(&id).await,
            Some(TaskState::Queued),
            "round {round}"
        );
    }
    assert_eq!(queue.get_task(&id).await.unwrap().retry_count, 6);

    // Execution retries are budgeted separately: 6 failovers did not use up max_retries = 1.
    let policy = RetryPolicy {
        max_retries: 1,
        retry_on_execution_failure: true,
        initial_backoff_ms: 1,
        ..Default::default()
    };
    let queue = TaskQueue::with_config(policy, None);
    let id = queue.submit(echo_task("flaky")).await.unwrap();
    for _ in 0..6 {
        let w = Uuid::new_v4();
        queue.schedule_task(&id, w).await.unwrap();
        queue.handle_worker_disconnected(&w, "handoff", true).await;
    }
    let w = Uuid::new_v4();
    queue.schedule_task(&id, w).await.unwrap();
    let first_failure = queue
        .record_result(TaskResult::failure(w, id, 2, "", "nope", 10, None))
        .await
        .unwrap();
    assert_eq!(
        first_failure,
        TaskState::Retrying,
        "one execution retry is still available"
    );
    tokio::time::sleep(Duration::from_millis(150)).await; // backoff doubles per re-dispatch
    queue.process_delayed_retries().await;
    queue.schedule_task(&id, w).await.unwrap();
    let second_failure = queue
        .record_result(TaskResult::failure(w, id, 2, "", "nope", 10, None))
        .await
        .unwrap();
    assert_eq!(
        second_failure,
        TaskState::Failed,
        "execution budget (1) is now spent"
    );
}

#[tokio::test]
async fn test_failover_budget_is_bounded_and_zero_retries_means_never_rerun() {
    let policy = RetryPolicy {
        max_retries: 1,
        max_failovers: 2,
        ..Default::default()
    };
    let queue = TaskQueue::with_config(policy, None);
    let id = queue.submit(echo_task("poison")).await.unwrap();
    for _ in 0..2 {
        let w = Uuid::new_v4();
        queue.schedule_task(&id, w).await.unwrap();
        queue.handle_worker_disconnected(&w, "crash", true).await;
        assert_eq!(queue.get_state(&id).await, Some(TaskState::Queued));
    }
    let w = Uuid::new_v4();
    queue.schedule_task(&id, w).await.unwrap();
    queue.handle_worker_disconnected(&w, "crash", true).await;
    assert_eq!(queue.get_state(&id).await, Some(TaskState::Failed));
    let err = queue.get_result(&id).await.unwrap().error.unwrap();
    assert!(err.contains("maximum retry limit (2) reached"), "{err}");
    assert!(err.contains("(2/2)"), "{err}");

    // max_retries = 0 on the policy: never run a task twice, so no failover either.
    let queue = TaskQueue::with_config(
        RetryPolicy {
            max_retries: 0,
            ..Default::default()
        },
        None,
    );
    let id = queue.submit(echo_task("once")).await.unwrap();
    let w = Uuid::new_v4();
    queue.schedule_task(&id, w).await.unwrap();
    queue.handle_worker_disconnected(&w, "crash", true).await;
    assert_eq!(queue.get_state(&id).await, Some(TaskState::Failed));
}

#[tokio::test]
async fn test_repeated_evacuations_leave_failover_and_retry_counts_at_zero_and_preserve_budgets() {
    // 1. Drains 1..=max_evacuations (3) leave retry_count == 0 and failover_count == 0,
    //    increment evacuation_count, and record each drain in retry_history.
    //    Past max_evacuations, drain 4 takes the charged path (retry_count = 1,
    //    failover_count = 1), and drain 5 (N + 1 after both uncharged and charged caps)
    //    transitions the task to Failed.
    let policy = RetryPolicy {
        max_retries: 1,
        max_failovers: 1,
        max_evacuations: 3,
        ..Default::default()
    };
    let queue = TaskQueue::with_config(policy, None);
    let id = queue
        .submit(Task::new(
            TaskSpec::command("echo", vec!["evac".into()]),
            TaskRequirements::generic(1, 10).with_max_retries(1),
        ))
        .await
        .unwrap();

    for round in 1..=3u64 {
        let w = Uuid::new_v4();
        let scheduled = queue
            .schedule_tasks_batch_in_sessions(&[(id, w, round)])
            .await;
        assert_eq!(scheduled.len(), 1, "round {round} must schedule");
        queue.mark_running_from(w, Some(round), &id).await.unwrap();

        let affected = if round % 2 == 1 {
            queue
                .handle_session_evacuated(&w, Some(round), "FAST_PRE_SLEEP_BATTERY_LOW")
                .await
        } else {
            queue
                .handle_worker_evacuated(&w, "Graceful worker shutdown")
                .await
        };
        assert_eq!(affected, vec![id]);

        let info = queue.get_task(&id).await.unwrap();
        assert_eq!(info.state, TaskState::Queued, "round {round}");
        assert_eq!(info.retry_count, 0, "round {round}");
        assert_eq!(info.failover_count, 0, "round {round}");
        assert_eq!(info.evacuation_count, round as u32, "round {round}");
        assert_eq!(info.assigned_worker_id, None, "round {round}");
        let history = queue.get_retry_history(&id).await.unwrap();
        assert_eq!(history.len(), round as usize, "round {round}");
        assert!(
            history
                .last()
                .unwrap()
                .error
                .as_deref()
                .unwrap_or("")
                .contains(&format!("uncharged evacuation {round}/3")),
            "history entry must record uncharged evacuation: {history:?}"
        );
    }

    // Drain 4: past max_evacuations (3), so it takes the charged path (retry_count = 1, failover_count = 1).
    let w4 = Uuid::new_v4();
    assert_eq!(
        queue
            .schedule_tasks_batch_in_sessions(&[(id, w4, 4)])
            .await
            .len(),
        1
    );
    queue.mark_running_from(w4, Some(4), &id).await.unwrap();
    queue
        .handle_session_evacuated(&w4, Some(4), "FAST_PRE_SLEEP_BATTERY_LOW")
        .await;
    let info4 = queue.get_task(&id).await.unwrap();
    assert_eq!(info4.state, TaskState::Queued);
    assert_eq!(info4.evacuation_count, 3);
    assert_eq!(info4.retry_count, 1);
    assert_eq!(info4.failover_count, 1);
    assert_eq!(queue.get_retry_history(&id).await.unwrap().len(), 4);

    // Drain 5 (N + 1): both max_evacuations (3) and charged budget (1) are exhausted -> Failed.
    let w5 = Uuid::new_v4();
    assert_eq!(
        queue
            .schedule_tasks_batch_in_sessions(&[(id, w5, 5)])
            .await
            .len(),
        1
    );
    queue.mark_running_from(w5, Some(5), &id).await.unwrap();
    queue
        .handle_session_evacuated(&w5, Some(5), "FAST_PRE_SLEEP_BATTERY_LOW")
        .await;
    let info5 = queue.get_task(&id).await.unwrap();
    assert_eq!(info5.state, TaskState::Failed);
    assert_eq!(info5.evacuation_count, 3);
    assert_eq!(info5.retry_count, 1);

    // 2. When a task's charged budget is already spent (or N = max_evacuations = 3 while
    //    Scheduled with 0 charged budget), N drains succeed and the (N + 1)-th drain fails.
    let sched_id = queue
        .submit(Task::new(
            TaskSpec::command("echo", vec!["sched-zero".into()]),
            TaskRequirements::generic(1, 10).with_max_retries(0),
        ))
        .await
        .unwrap();
    for round in 1..=3u64 {
        let w = Uuid::new_v4();
        queue
            .schedule_tasks_batch_in_sessions(&[(sched_id, w, round)])
            .await;
        queue
            .handle_session_evacuated(&w, Some(round), "drain before start")
            .await;
        assert_eq!(queue.get_state(&sched_id).await, Some(TaskState::Queued));
    }
    let w_n_plus_1 = Uuid::new_v4();
    queue
        .schedule_tasks_batch_in_sessions(&[(sched_id, w_n_plus_1, 4)])
        .await;
    queue
        .handle_session_evacuated(&w_n_plus_1, Some(4), "drain 4")
        .await;
    assert_eq!(
        queue.get_state(&sched_id).await,
        Some(TaskState::Failed),
        "N + 1 (4th) drain with 0 charged budget must end in Failed"
    );

    // 3. A Running task with max_retries(0) (per-task or policy-level) is never run again:
    //    the very first drain transitions it to Failed.
    let run_once_id = queue
        .submit(Task::new(
            TaskSpec::command("echo", vec!["run-once".into()]),
            TaskRequirements::generic(1, 10).with_max_retries(0),
        ))
        .await
        .unwrap();
    let w_once = Uuid::new_v4();
    queue
        .schedule_tasks_batch_in_sessions(&[(run_once_id, w_once, 1)])
        .await;
    queue
        .mark_running_from(w_once, Some(1), &run_once_id)
        .await
        .unwrap();
    queue
        .handle_session_evacuated(&w_once, Some(1), "FAST_PRE_SLEEP_BATTERY_LOW")
        .await;
    let once_info = queue.get_task(&run_once_id).await.unwrap();
    assert_eq!(
        once_info.state,
        TaskState::Failed,
        "Running task with max_retries(0) must not be run again on drain"
    );
    assert_eq!(once_info.evacuation_count, 0);
    assert_eq!(once_info.retry_count, 0);

    let policy_zero_queue = TaskQueue::with_config(
        RetryPolicy {
            max_retries: 0,
            max_evacuations: 10,
            ..Default::default()
        },
        None,
    );
    let policy_zero_id = policy_zero_queue
        .submit(echo_task("policy-zero-once"))
        .await
        .unwrap();
    let w_pz = Uuid::new_v4();
    policy_zero_queue
        .schedule_tasks_batch_in_sessions(&[(policy_zero_id, w_pz, 1)])
        .await;
    policy_zero_queue
        .mark_running_from(w_pz, Some(1), &policy_zero_id)
        .await
        .unwrap();
    policy_zero_queue
        .handle_session_evacuated(&w_pz, Some(1), "Graceful worker shutdown")
        .await;
    assert_eq!(
        policy_zero_queue.get_state(&policy_zero_id).await,
        Some(TaskState::Failed),
        "Running task under policy max_retries = 0 must fail on first drain"
    );
}

/// The exact texts `WorkerClient` and the task runner emit when they refuse to run a task.
fn worker_rejection(worker: Uuid, task: TaskId, stderr: &str, error: &str) -> TaskResult {
    TaskResult::failure(worker, task, 1, "", stderr, 0, Some(error.to_string()))
}

#[test]
fn test_worker_rejection_detection_matches_worker_messages_only() {
    let (w, t) = (Uuid::new_v4(), TaskId::new());
    assert!(is_worker_rejection(&worker_rejection(
        w,
        t,
        "Task rejected by worker: Critical battery preservation (<15% discharging)",
        "Battery critically low (<15% discharging)"
    )));
    assert!(is_worker_rejection(&worker_rejection(
        w,
        t,
        "Task requirements exceed worker capabilities (CPU, RAM, or GPU)",
        "Worker capabilities insufficient for task requirements"
    )));
    assert!(is_worker_rejection(&worker_rejection(
        w,
        t,
        "Execution Error: BuiltinTest requires GPU, but worker has no GPU capability (Worker lacks GPU capability).",
        "Worker lacks GPU capability"
    )));

    // A sandbox base the worker cannot use (design 0090), exactly as the worker words it.
    let refusal = rusty_grid_core::sandbox_base_refusal(
        "The sandbox base directory '/x/base' is not writable: Not a directory (os error 20)",
    );
    assert!(is_worker_rejection(&worker_rejection(
        w, t, &refusal, &refusal
    )));
    assert!(is_worker_rejection(&TaskResult::failure(
        w,
        t,
        1,
        "",
        refusal.clone(),
        0,
        Some(refusal.clone())
    )));

    // A task that genuinely ran and failed is not a rejection, however fast it was.
    assert!(!is_worker_rejection(&TaskResult::failure(
        w, t, 1, "", "boom", 0, None
    )));
    assert!(!is_worker_rejection(&TaskResult::failure(
        w,
        t,
        1,
        "",
        "BuiltinTest: intentional failure requested",
        0,
        Some("Intentional failure".into())
    )));
    assert!(
        !is_worker_rejection(&TaskResult::failure(
            w,
            t,
            1,
            "",
            "Task rejected by worker: x",
            250,
            Some("x".into())
        )),
        "a refusal is produced before anything runs, so it takes no measurable time"
    );
    assert!(!is_worker_rejection(&TaskResult::success(
        w, t, "ok", 0, false
    )));
}

#[tokio::test]
async fn test_rejected_task_goes_to_another_worker_and_is_bounded() {
    let queue = TaskQueue::new();
    let phone = Uuid::new_v4();
    let id = submit_and_assign(&queue, echo_task("battery"), phone, 4).await;
    let rejection = worker_rejection(
        phone,
        id,
        "Task rejected by worker: Critical battery preservation (<15% discharging)",
        "Battery critically low (<15% discharging)",
    );
    let outcome = queue
        .record_result_from(phone, Some(4), rejection.clone())
        .await
        .unwrap();
    assert_eq!(outcome.disposition, ResultDisposition::Requeued);
    assert!(outcome.rejected);
    assert_eq!(outcome.state, TaskState::Queued);
    let info = queue.get_task(&id).await.unwrap();
    assert_eq!(info.assigned_worker_id, None);
    assert_eq!(info.retry_count, 1);
    assert_eq!(queue.avoided_workers_for(&[id]).await[&id], vec![phone]);
    assert_eq!(queue.get_schedulable_tasks().await.len(), 1);

    // Keeps being rejected: bounded, then the real reason is reported.
    let mut last = outcome;
    for round in 2..=MAX_WORKER_REJECTIONS_PER_TASK + 1 {
        let w = Uuid::new_v4();
        queue
            .schedule_tasks_batch_in_sessions(&[(id, w, round as u64)])
            .await;
        last = queue
            .record_result_from(
                w,
                Some(round as u64),
                worker_rejection(
                    w,
                    id,
                    "Task rejected by worker: Critical battery",
                    "Battery critically low",
                ),
            )
            .await
            .unwrap();
    }
    assert_eq!(last.disposition, ResultDisposition::Terminal);
    assert_eq!(last.state, TaskState::Failed);
    assert!(queue
        .get_result(&id)
        .await
        .unwrap()
        .error
        .unwrap()
        .contains("Battery critically low"));
}

#[tokio::test]
async fn test_requeue_dispatch_failure_is_guarded_and_uncharged() {
    let queue = TaskQueue::new();
    let w = Uuid::new_v4();
    let id = submit_and_assign(&queue, echo_task("undelivered"), w, 5).await;

    // Wrong session / wrong worker: nothing happens.
    assert!(!queue.requeue_dispatch_failure(&id, &w, 6).await.unwrap());
    assert!(!queue
        .requeue_dispatch_failure(&id, &Uuid::new_v4(), 5)
        .await
        .unwrap());
    assert_eq!(queue.get_state(&id).await, Some(TaskState::Scheduled));

    assert!(queue.requeue_dispatch_failure(&id, &w, 5).await.unwrap());
    let info = queue.get_task(&id).await.unwrap();
    assert_eq!(info.state, TaskState::Queued);
    assert_eq!(
        info.retry_count, 0,
        "an undelivered task was never tried, so nothing is charged"
    );
    assert_eq!(queue.get_schedulable_tasks().await.len(), 1);

    // A task cancelled in the meantime is never resurrected, by either requeue entry point.
    let id2 = submit_and_assign(&queue, echo_task("cancelled-meanwhile"), w, 5).await;
    queue.cancel_task(&id2, None).await.unwrap();
    assert!(!queue.requeue_dispatch_failure(&id2, &w, 5).await.unwrap());
    assert!(queue.requeue_task(&id2).await.is_err());
    assert_eq!(queue.get_state(&id2).await, Some(TaskState::Cancelled));
}

#[tokio::test]
async fn test_cluster_buckets_cover_every_state() {
    let policy = RetryPolicy {
        initial_backoff_ms: 60_000,
        ..Default::default()
    };
    let queue = TaskQueue::with_config(policy, None);
    let w = Uuid::new_v4();

    let _queued = queue.submit(echo_task("q")).await.unwrap();
    let scheduled = submit_and_assign(&queue, echo_task("s"), w, 1).await;
    let running = submit_and_assign(&queue, echo_task("r"), w, 1).await;
    queue.mark_running_from(w, Some(1), &running).await.unwrap();
    let completed = submit_and_assign(&queue, echo_task("c"), w, 1).await;
    queue
        .record_result_from(w, Some(1), TaskResult::success(w, completed, "", 1, false))
        .await
        .unwrap();
    let failed = submit_and_assign(&queue, echo_task("f"), w, 1).await;
    queue
        .record_result_from(
            w,
            Some(1),
            TaskResult::failure(w, failed, 2, "", "", 1, None),
        )
        .await
        .unwrap();
    let cancelled = queue.submit(echo_task("x")).await.unwrap();
    queue.cancel_task(&cancelled, None).await.unwrap();
    let retrying = submit_and_assign(&queue, echo_task("t"), Uuid::new_v4(), 1).await;
    let retrying_worker = queue
        .get_task(&retrying)
        .await
        .unwrap()
        .assigned_worker_id
        .unwrap();
    queue
        .handle_worker_disconnected(&retrying_worker, "lost", false)
        .await;
    let _ = scheduled;

    let stats = queue.stats().await;
    assert_eq!(stats.total, 7);
    assert_eq!(
        (stats.queued, stats.scheduled, stats.running, stats.retrying),
        (1, 1, 1, 1)
    );
    let (pending, running_bucket, completed_bucket, failed_bucket) = stats.cluster_buckets();
    assert_eq!(pending, 2, "queued + retrying");
    assert_eq!(running_bucket, 2, "scheduled + running");
    assert_eq!(completed_bucket, 1);
    assert_eq!(failed_bucket, 2, "failed + cancelled");
    assert_eq!(
        pending + running_bucket + completed_bucket + failed_bucket,
        stats.total
    );
}

#[tokio::test]
async fn test_stale_ready_keys_are_never_offered_and_get_pruned() {
    // Whatever leaks a ready-queue key for a task that is no longer Queued (the unguarded failure
    // paths used to), that key must not take scheduling slots or be dispatched a second time.
    let queue = TaskQueue::new();
    let w = Uuid::new_v4();
    let live = queue.submit(echo_task("live")).await.unwrap();
    let done = submit_and_assign(&queue, echo_task("done"), w, 1).await;
    queue
        .record_result_from(w, Some(1), TaskResult::success(w, done, "ok", 1, false))
        .await
        .unwrap();
    let stale_key = {
        let mut inner = queue.inner.write().await;
        let entry = &inner.tasks[&done];
        let key = QueueOrderKey {
            priority_rev: Reverse(entry.priority),
            sequence: entry.sequence,
            task_id: done,
        };
        inner.ready_queue.insert(key);
        key
    };

    let offered: Vec<TaskId> = queue
        .get_schedulable_tasks()
        .await
        .iter()
        .map(|t| t.id)
        .collect();
    assert_eq!(
        offered,
        vec![live],
        "a finished task must not be offered to the scheduler"
    );

    // Even if a scheduler pass names it, it is not re-dispatched, its state is untouched and the
    // key is dropped instead of being re-offered forever.
    assert!(queue
        .schedule_tasks_batch_in_sessions(&[(done, w, 2)])
        .await
        .is_empty());
    assert_eq!(queue.get_state(&done).await, Some(TaskState::Completed));
    assert!(!queue.inner.read().await.ready_queue.contains(&stale_key));

    // The one-by-one path skips stale keys too.
    let popped = queue.pop_and_schedule(w, |_| true).await.unwrap();
    assert_eq!(popped.id, live);
    assert!(queue.pop_and_schedule(w, |_| true).await.is_none());
}

// --- Step B: byte-bounded retention, scheduling views, loads, listing ---

/// Runs `task` on `worker` (session 1) and finishes it with `output_bytes` of stdout.
async fn finish_with_output(
    queue: &TaskQueue,
    task: Task,
    worker: Uuid,
    output_bytes: usize,
) -> TaskId {
    let id = submit_and_assign(queue, task, worker, 1).await;
    let result = TaskResult::success(worker, id, "x".repeat(output_bytes), 1, false);
    queue
        .record_result_from(worker, Some(1), result)
        .await
        .unwrap();
    id
}

#[tokio::test]
async fn test_terminal_retention_is_bounded_by_bytes_oldest_first() {
    const MIB: usize = 1024 * 1024;
    // Plenty of room by count, 2.5 MiB by bytes: only the two newest 1 MiB results fit.
    let queue = TaskQueue::with_retention_limits(
        RetryPolicy::default(),
        None,
        10_000,
        5 * MIB / 2,
        DEFAULT_MAX_ACTIVE_BYTES,
    );
    let worker = Uuid::new_v4();
    let mut ids = Vec::new();
    for i in 0..5 {
        ids.push(finish_with_output(&queue, echo_task(&format!("bulky-{i}")), worker, MIB).await);
    }

    for old in &ids[..3] {
        assert!(
            queue.get_task(old).await.is_none(),
            "the oldest finished tasks must be evicted once the byte budget is exceeded"
        );
    }
    for kept in &ids[3..] {
        assert!(queue.get_task(kept).await.is_some());
        assert!(queue.get_result(kept).await.is_some());
    }
    assert!(
        queue.terminal_retained_bytes().await <= 5 * MIB / 2,
        "retained bytes must stay within the budget"
    );
    assert!(queue.terminal_retained_bytes().await >= 2 * MIB);
}

#[tokio::test]
async fn test_the_newest_result_survives_even_when_larger_than_the_whole_budget() {
    const MIB: usize = 1024 * 1024;
    let queue = TaskQueue::with_retention_limits(
        RetryPolicy::default(),
        None,
        10_000,
        MIB / 4,
        DEFAULT_MAX_ACTIVE_BYTES,
    );
    let worker = Uuid::new_v4();
    let small = finish_with_output(&queue, echo_task("small"), worker, 10).await;
    let huge = finish_with_output(&queue, echo_task("huge"), worker, 2 * MIB).await;

    // The client that just got `huge` must still be able to read it back...
    assert_eq!(
        queue.get_result(&huge).await.unwrap().stdout.0.len(),
        2 * MIB
    );
    // ...at the price of everything older.
    assert!(queue.get_task(&small).await.is_none());
}

#[tokio::test]
async fn test_lowering_the_byte_budget_evicts_and_eviction_releases_the_charge() {
    const MIB: usize = 1024 * 1024;
    let queue = TaskQueue::new();
    let worker = Uuid::new_v4();
    let mut ids = Vec::new();
    for i in 0..4 {
        ids.push(finish_with_output(&queue, echo_task(&format!("t-{i}")), worker, MIB).await);
    }
    assert!(queue.terminal_retained_bytes().await >= 4 * MIB);

    queue.set_max_terminal_bytes(2 * MIB + MIB / 2).await;
    assert!(queue.get_task(&ids[0]).await.is_none());
    assert!(queue.get_task(&ids[1]).await.is_none());
    assert!(queue.get_task(&ids[3]).await.is_some());
    assert!(queue.terminal_retained_bytes().await <= 2 * MIB + MIB / 2);

    // Count-based and TTL eviction release the byte charge as well.
    queue.set_max_terminal_tasks(0).await;
    assert_eq!(queue.terminal_retained_bytes().await, 0);
    assert_eq!(queue.stats().await.total, 0);
}

#[tokio::test]
async fn test_payload_of_a_finished_task_counts_toward_the_budget() {
    // A task that carries a large stdin and prints nothing still holds that stdin.
    let queue = TaskQueue::new();
    let worker = Uuid::new_v4();
    let task = Task::new(
        TaskSpec::Command {
            program: "cat".into(),
            args: vec![],
            env: Default::default(),
            working_dir: None,
            stdin: Some(vec![0u8; 3 * 1024 * 1024]),
        },
        TaskRequirements::generic(1, 10),
    );
    finish_with_output(&queue, task, worker, 1).await;
    assert!(queue.terminal_retained_bytes().await >= 3 * 1024 * 1024);
}

#[tokio::test]
async fn test_asset_bytes_are_charged_to_terminal_retention_budget() {
    const MIB: usize = 1024 * 1024;
    let queue = TaskQueue::with_retention_limits(
        RetryPolicy::default(),
        None,
        10_000,
        5 * MIB / 2,
        DEFAULT_MAX_ACTIVE_BYTES,
    );
    let worker = Uuid::new_v4();
    let mut ids = Vec::new();
    for i in 0..4 {
        let id = submit_and_assign(&queue, echo_task(&format!("asset-{i}")), worker, 1).await;
        let mut result = TaskResult::success(worker, id, "", 1, false);
        result.asset_bytes = Some(rusty_grid_core::task::Bytes::from(vec![0u8; MIB]));
        queue
            .record_result_from(worker, Some(1), result)
            .await
            .unwrap();
        ids.push(id);
    }

    assert!(
        queue.get_task(&ids[0]).await.is_none(),
        "oldest task with 1 MiB asset_bytes must be evicted once the 2.5 MiB budget is exceeded"
    );
    assert!(
        queue.get_task(&ids[1]).await.is_none(),
        "second oldest task with 1 MiB asset_bytes must also be evicted"
    );
    assert!(queue.get_task(&ids[2]).await.is_some());
    assert!(queue.get_task(&ids[3]).await.is_some());
    assert!(queue.terminal_retained_bytes().await >= 2 * MIB);
    assert!(queue.terminal_retained_bytes().await <= 5 * MIB / 2);
}

#[tokio::test]
async fn test_active_task_byte_cap_bounds_non_terminal_tasks_and_releases_on_completion() {
    const MIB: usize = 1024 * 1024;
    let queue = TaskQueue::with_retention_limits(
        RetryPolicy::default(),
        None,
        DEFAULT_MAX_TERMINAL_TASKS,
        DEFAULT_MAX_TERMINAL_BYTES,
        5 * MIB / 2,
    );
    let worker = Uuid::new_v4();

    let make_heavy_task = |label: &str, bytes: usize| {
        Task::new(
            TaskSpec::Command {
                program: "cat".into(),
                args: vec![label.to_string()],
                env: Default::default(),
                working_dir: None,
                stdin: Some(vec![b'a'; bytes]),
            },
            TaskRequirements::generic(1, 10),
        )
    };

    let id1 = queue.submit(make_heavy_task("t1", MIB)).await.unwrap();
    let id2 = queue.submit(make_heavy_task("t2", MIB)).await.unwrap();
    assert!(
        queue.active_retained_bytes().await >= 2 * MIB,
        "active tasks must be charged toward active_retained_bytes"
    );

    let err = queue.submit(make_heavy_task("t3", MIB)).await.unwrap_err();
    assert!(
        matches!(err, GridError::QueueByteLimitExceeded { .. }),
        "third 1 MiB task must be rejected when active byte cap is 2.5 MiB, got {err:?}"
    );

    // Schedule id1 and verify an oversized checkpoint is also rejected by the active byte cap.
    let scheduled = queue
        .schedule_tasks_batch_in_sessions(&[(id1, worker, 1)])
        .await;
    assert_eq!(scheduled.len(), 1);
    let cp_err = queue
        .record_checkpoint_from(
            worker,
            Some(1),
            id1,
            CheckpointData::new(1, rusty_grid_core::task::Bytes::from(vec![0u8; MIB])),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(cp_err, GridError::QueueByteLimitExceeded { .. }),
        "checkpoint exceeding active byte cap must be rejected, got {cp_err:?}"
    );

    // Finishing id1 moves its charge from active to terminal, freeing room for t3.
    queue
        .record_result_from(
            worker,
            Some(1),
            TaskResult::success(worker, id1, "done", 1, false),
        )
        .await
        .unwrap();
    assert!(queue.active_retained_bytes().await < 2 * MIB);

    let id3 = queue
        .submit(make_heavy_task("t3-after-finish", MIB))
        .await
        .expect("room freed after id1 completed");

    // Cancelling id2 and id3 releases the remaining active byte charge completely.
    queue.cancel_task(&id2, None).await.unwrap();
    queue.cancel_task(&id3, None).await.unwrap();
    assert_eq!(queue.active_retained_bytes().await, 0);
}

#[tokio::test]
async fn test_checkpoint_over_active_cap_accepts_equal_or_smaller_and_refuses_larger() {
    // Cap is 3_000 bytes. Submit a task with a 1_000-byte initial checkpoint (~2_037 bytes
    // including entry overhead) so it fits at submission, then push the queue's active total
    // over the 3_000-byte cap via retry_history growth on worker eviction.
    let queue = TaskQueue::with_retention_limits(
        RetryPolicy::default(),
        None,
        DEFAULT_MAX_TERMINAL_TASKS,
        DEFAULT_MAX_TERMINAL_BYTES,
        3_000,
    );
    let worker_1 = Uuid::new_v4();
    let worker_2 = Uuid::new_v4();

    let mut task = echo_task("cp-growth");
    task.update_checkpoint(CheckpointData::new(
        1,
        rusty_grid_core::task::Bytes::from(vec![b'a'; 1_000]),
    ));
    let task_id = queue.submit(task).await.unwrap();
    queue
        .schedule_tasks_batch_in_sessions(&[(task_id, worker_1, 1)])
        .await;

    // Evicting worker_1 with a long reason pushes `active_retained_bytes()` above the 3_000 cap.
    queue
        .handle_session_disconnected(&worker_1, Some(1), &"r".repeat(1_500), true)
        .await;
    assert!(
        queue.active_retained_bytes().await > 3_000,
        "retry_history growth pushed active_retained_bytes ({}) over the 3_000 cap",
        queue.active_retained_bytes().await
    );

    queue
        .schedule_tasks_batch_in_sessions(&[(task_id, worker_2, 2)])
        .await;

    // Equal-size checkpoint (1_000 bytes) does not grow the entry and must be stored.
    let stored_equal = queue
        .record_checkpoint_from(
            worker_2,
            Some(2),
            task_id,
            CheckpointData::new(2, rusty_grid_core::task::Bytes::from(vec![b'b'; 1_000])),
        )
        .await
        .expect("equal-size checkpoint must be accepted even when total is over cap");
    assert!(stored_equal);
    assert_eq!(queue.get_checkpoint(&task_id).await.unwrap().sequence, 2);

    // Smaller checkpoint (600 bytes) reduces the entry and must be stored.
    let stored_smaller = queue
        .record_checkpoint_from(
            worker_2,
            Some(2),
            task_id,
            CheckpointData::new(3, rusty_grid_core::task::Bytes::from(vec![b'c'; 600])),
        )
        .await
        .expect("smaller checkpoint must be accepted even when total is over cap");
    assert!(stored_smaller);
    assert_eq!(queue.get_checkpoint(&task_id).await.unwrap().sequence, 3);

    // Larger checkpoint (700 bytes > 600 bytes while total > 2_000) must be refused.
    let err = queue
        .record_checkpoint_from(
            worker_2,
            Some(2),
            task_id,
            CheckpointData::new(4, rusty_grid_core::task::Bytes::from(vec![b'd'; 700])),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, GridError::QueueByteLimitExceeded { .. }),
        "growing checkpoint over active cap must be refused, got {err:?}"
    );
    assert_eq!(queue.get_checkpoint(&task_id).await.unwrap().sequence, 3);
}

#[tokio::test]
async fn test_ready_views_page_through_the_queue_in_order_without_cloning_tasks() {
    let queue = TaskQueue::new();
    let mut ids = Vec::new();
    for i in 0..10 {
        ids.push(queue.submit(echo_task(&format!("v-{i}"))).await.unwrap());
    }
    // One of them is a GPU task: the view must say so.
    let gpu = Task::new(TaskSpec::gpu_compute("gemm", 8), TaskRequirements::gpu(30));
    let gpu_id = queue.submit(gpu).await.unwrap();

    let mut seen = Vec::new();
    let mut cursor = None;
    let mut pages = 0;
    loop {
        let (views, next) = queue.ready_views(cursor, 4).await;
        pages += 1;
        for v in &views {
            if v.task_id == gpu_id {
                assert!(v.requires_gpu && v.heavy);
            } else {
                assert!(!v.requires_gpu && !v.heavy);
            }
        }
        seen.extend(views.into_iter().map(|v| v.task_id));
        match next {
            Some(key) => cursor = Some(key),
            None => break,
        }
    }
    let mut expected = ids.clone();
    expected.push(gpu_id);
    assert_eq!(
        seen, expected,
        "pages must cover the queue exactly once, in FIFO order"
    );
    assert!(pages >= 3);
}

#[tokio::test]
async fn test_ready_views_skip_tasks_that_are_no_longer_queued_and_carry_avoid_lists() {
    let queue = TaskQueue::new();
    let worker = Uuid::new_v4();
    let assigned = submit_and_assign(&queue, echo_task("gone"), worker, 1).await;
    let waiting = queue.submit(echo_task("waiting")).await.unwrap();
    // Hand-feed a rejection so the entry has an avoid list.
    {
        let mut inner = queue.inner.write().await;
        inner
            .tasks
            .get_mut(&waiting)
            .unwrap()
            .avoid_workers
            .push(worker);
    }

    let (views, next) = queue.ready_views(None, 10).await;
    assert!(next.is_none());
    assert_eq!(views.len(), 1);
    assert_eq!(views[0].task_id, waiting);
    assert_eq!(views[0].avoid, vec![worker]);
    assert_ne!(views[0].task_id, assigned);
    assert_eq!(queue.ready_task_ids(10).await, vec![waiting]);
}

#[tokio::test]
async fn test_assigned_loads_come_from_the_assignment_set_with_slots_and_unstarted_ram() {
    let queue = TaskQueue::new();
    let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
    let big = Task::new(
        TaskSpec::command("make", vec![]),
        TaskRequirements::new(8, 4096, false, 60),
    );
    let big_id = submit_and_assign(&queue, big, a, 1).await;
    let small_id = submit_and_assign(&queue, echo_task("small"), a, 1).await;
    let other_id = submit_and_assign(&queue, echo_task("other"), b, 1).await;

    let loads = queue.assigned_loads().await;
    assert_eq!(loads[&a].tasks, 2);
    assert_eq!(loads[&a].slots, 8 + 1, "an 8-core task occupies 8 slots");
    assert_eq!(loads[&a].unstarted_ram_mb, 4096 + 512);
    assert_eq!(loads[&b].tasks, 1);

    // Once the worker reports a task running its RAM is in the worker's own figure.
    queue.mark_running_from(a, Some(1), &big_id).await.unwrap();
    assert_eq!(queue.assigned_loads().await[&a].unstarted_ram_mb, 512);

    // A finished task leaves the books; a worker with nothing left disappears from the map.
    queue
        .record_result_from(b, Some(1), TaskResult::success(b, other_id, "", 1, false))
        .await
        .unwrap();
    queue
        .record_result_from(a, Some(1), TaskResult::success(a, small_id, "", 1, false))
        .await
        .unwrap();
    let loads = queue.assigned_loads().await;
    assert!(!loads.contains_key(&b));
    assert_eq!(loads[&a].tasks, 1);
}

#[tokio::test]
async fn test_assigned_loads_track_vram_across_scheduled_running_and_finished() {
    let queue = TaskQueue::new();
    let worker = Uuid::new_v4();
    let gpu_a = Task::new(
        TaskSpec::command("train-a", vec![]),
        TaskRequirements::new(1, 1024, true, 60).with_vram_mb(4096),
    );
    let gpu_b = Task::new(
        TaskSpec::command("train-b", vec![]),
        TaskRequirements::new(1, 512, true, 60).with_vram_mb(2048),
    );
    let id_a = submit_and_assign(&queue, gpu_a, worker, 1).await;
    let id_b = submit_and_assign(&queue, gpu_b, worker, 1).await;

    let loads = queue.assigned_loads().await;
    assert_eq!(loads[&worker].unstarted_ram_mb, 1024 + 512);
    assert_eq!(loads[&worker].vram_mb, 4096 + 2048);

    // Once `gpu_a` transitions to Running, host RAM leaves `unstarted_ram_mb`,
    // but VRAM stays reserved because heartbeats report static total VRAM.
    queue
        .mark_running_from(worker, Some(1), &id_a)
        .await
        .unwrap();
    let loads = queue.assigned_loads().await;
    assert_eq!(loads[&worker].unstarted_ram_mb, 512);
    assert_eq!(loads[&worker].vram_mb, 4096 + 2048);

    queue
        .record_result_from(
            worker,
            Some(1),
            TaskResult::success(worker, id_a, "", 1, false),
        )
        .await
        .unwrap();
    let loads = queue.assigned_loads().await;
    assert_eq!(loads[&worker].vram_mb, 2048);

    queue
        .record_result_from(
            worker,
            Some(1),
            TaskResult::success(worker, id_b, "", 1, false),
        )
        .await
        .unwrap();
    assert!(!queue.assigned_loads().await.contains_key(&worker));
}

#[tokio::test]
async fn test_recent_tasks_are_newest_first_with_offset_limit_and_filter() {
    let queue = TaskQueue::new();
    let worker = Uuid::new_v4();
    let mut ids = Vec::new();
    for i in 0..30 {
        ids.push(queue.submit(echo_task(&format!("r-{i}"))).await.unwrap());
    }
    // Finish the 5 oldest.
    for id in &ids[..5] {
        let scheduled = queue
            .schedule_tasks_batch_in_sessions(&[(*id, worker, 1)])
            .await;
        assert_eq!(scheduled.len(), 1);
        queue
            .record_result_from(
                worker,
                Some(1),
                TaskResult::success(worker, *id, "", 1, false),
            )
            .await
            .unwrap();
    }

    let newest: Vec<TaskId> = queue
        .recent_tasks(|_| true, 0, 3)
        .await
        .into_iter()
        .map(|t| t.task_id)
        .collect();
    assert_eq!(newest, vec![ids[29], ids[28], ids[27]]);

    let page2: Vec<TaskId> = queue
        .recent_tasks(|_| true, 3, 3)
        .await
        .into_iter()
        .map(|t| t.task_id)
        .collect();
    assert_eq!(page2, vec![ids[26], ids[25], ids[24]]);

    let done: Vec<TaskId> = queue
        .recent_tasks(|e| e.state == TaskState::Completed, 0, 100)
        .await
        .into_iter()
        .map(|t| t.task_id)
        .collect();
    assert_eq!(done, vec![ids[4], ids[3], ids[2], ids[1], ids[0]]);

    assert_eq!(queue.recent_tasks(|_| true, 0, 0).await.len(), 0);
    assert_eq!(queue.recent_tasks(|_| true, 100, 10).await.len(), 0);
    assert_eq!(queue.recent_tasks(|_| true, 0, usize::MAX).await.len(), 30);
    assert_eq!(queue.recent_tasks(|_| true, usize::MAX, 5).await.len(), 0);
}

#[test]
fn test_task_state_spelling_matches_its_serde_representation() {
    for state in [
        TaskState::Submitted,
        TaskState::Queued,
        TaskState::Scheduled,
        TaskState::Running,
        TaskState::Completed,
        TaskState::Failed,
        TaskState::Cancelled,
        TaskState::Retrying,
    ] {
        assert_eq!(
            serde_json::to_value(state).unwrap(),
            serde_json::Value::String(state.as_str().to_string()),
            "TaskState::as_str must be the serde spelling every endpoint uses"
        );
    }
}

#[tokio::test]
async fn test_task_active_bytes_counts_working_dir_target_dir_and_preferred_provider() {
    use rusty_grid_core::task::AutoRigSpec;

    let base_cmd = Task::new(
        TaskSpec::Command {
            program: "echo".into(),
            args: vec![],
            env: Default::default(),
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 30),
    );
    let with_wdir = Task::new(
        TaskSpec::Command {
            program: "echo".into(),
            args: vec![],
            env: Default::default(),
            working_dir: Some("/custom/working/directory/path".into()),
            stdin: None,
        },
        TaskRequirements::generic(1, 30),
    );
    assert_eq!(
        task_active_bytes(&with_wdir) - task_active_bytes(&base_cmd),
        "/custom/working/directory/path".len()
    );

    let base_compile = Task::new(
        TaskSpec::RustCompilation {
            crate_name: "proj".into(),
            source_files: Default::default(),
            compiler_flags: vec![],
            target_dir: None,
        },
        TaskRequirements::generic(1, 30),
    );
    let with_tdir = Task::new(
        TaskSpec::RustCompilation {
            crate_name: "proj".into(),
            source_files: Default::default(),
            compiler_flags: vec![],
            target_dir: Some("/tmp/custom-target-dir".into()),
        },
        TaskRequirements::generic(1, 30),
    );
    assert_eq!(
        task_active_bytes(&with_tdir) - task_active_bytes(&base_compile),
        "/tmp/custom-target-dir".len()
    );

    let base_rig = Task::new(
        TaskSpec::AutoRig(AutoRigSpec {
            input_mesh: "mesh.glb".into(),
            rig_preset: "humanoid".into(),
            embed_animations: vec![],
            preferred_provider: None,
        }),
        TaskRequirements::generic(1, 30),
    );
    let with_prov = Task::new(
        TaskSpec::AutoRig(AutoRigSpec {
            input_mesh: "mesh.glb".into(),
            rig_preset: "humanoid".into(),
            embed_animations: vec![],
            preferred_provider: Some("rig_net_provider".into()),
        }),
        TaskRequirements::generic(1, 30),
    );
    assert_eq!(
        task_active_bytes(&with_prov) - task_active_bytes(&base_rig),
        "rig_net_provider".len()
    );

    let queue = TaskQueue::with_retention_limits(
        RetryPolicy::default(),
        None,
        DEFAULT_MAX_TERMINAL_TASKS,
        DEFAULT_MAX_TERMINAL_BYTES,
        2_000,
    );
    let expected_bytes = task_active_bytes(&with_wdir);
    queue.submit(with_wdir).await.unwrap();
    let stats = queue.stats().await;
    assert_eq!(stats.active_bytes, expected_bytes);
    assert_eq!(stats.max_active_bytes, 2_000);
}

#[tokio::test]
async fn test_three_d_tasks_require_three_d_protocol_capability_for_placement() {
    use crate::registry::{WorkerInfo, WorkerStatus};
    use crate::scheduler::{ScheduleSkipReason, SchedulerConfig, WorkloadScheduler};
    use rusty_grid_core::capabilities::WorkerCapabilities;
    use rusty_grid_core::task::{AutoRigSpec, Generative3DSpec};

    let gen3d_task = Task::new(
        TaskSpec::Generative3D(
            Generative3DSpec::new_prompt("obsidian crystal").with_quality("fast"),
        ),
        TaskRequirements::generic(1, 30),
    );
    let autorig_task = Task::new(
        TaskSpec::AutoRig(AutoRigSpec::new(vec![1, 2, 3, 4], "biped")),
        TaskRequirements::generic(1, 30),
    );

    assert!(
        TaskView::of(&gen3d_task)
            .required_features
            .contains(ProtocolCapabilities::THREE_D),
        "Generative3D TaskView must require ProtocolCapabilities::THREE_D"
    );
    assert!(
        TaskView::of(&autorig_task)
            .required_features
            .contains(ProtocolCapabilities::THREE_D),
        "AutoRig TaskView must require ProtocolCapabilities::THREE_D"
    );

    let mut features_without_3d = ProtocolCapabilities::all_supported();
    features_without_3d.remove(ProtocolCapabilities::THREE_D);

    let worker_no_3d = WorkerInfo {
        worker_id: Uuid::new_v4(),
        session_id: 1,
        capabilities: WorkerCapabilities::new("worker-no-3d", 4, 8192, true, true, None),
        features: features_without_3d,
        status: WorkerStatus::Connected,
        last_heartbeat_timestamp: 1000,
        registered_timestamp: 1000,
        active_tasks: 0,
        remote_addr: "127.0.0.1:9000".into(),
        cpu_usage_pct: 0.0,
        ram_available_mb: 8192,
        mobile_status: None,
        paused: false,
        gpu_paused: false,
    };

    let tasks = vec![gen3d_task.clone(), autorig_task.clone()];
    let gen3d_id = gen3d_task.id;
    let autorig_id = autorig_task.id;

    // 1. Default scheduler: both 3D tasks wait with ClusterLacksCapability("... THREE_D ...").
    let default_cfg = SchedulerConfig::default();
    let report =
        WorkloadScheduler::matchmake(&default_cfg, &tasks, std::slice::from_ref(&worker_no_3d));
    assert!(
        report.assignments.is_empty(),
        "3D tasks must not be placed on a worker without THREE_D"
    );
    assert_eq!(report.skipped.len(), 2);
    for (_, reason) in &report.skipped {
        match reason {
            ScheduleSkipReason::ClusterLacksCapability(msg) => {
                assert!(
                    msg.contains("THREE_D"),
                    "expected THREE_D capability skip reason, got: {msg}"
                );
            }
            other => panic!("expected ClusterLacksCapability, got {other:?}"),
        }
    }

    // 2. With --fail-unfulfillable-tasks: both 3D tasks are reported unfulfillable.
    let strict_cfg = SchedulerConfig {
        fail_unfulfillable_tasks: true,
        ..SchedulerConfig::default()
    };
    let strict_report =
        WorkloadScheduler::matchmake(&strict_cfg, &tasks, std::slice::from_ref(&worker_no_3d));
    assert!(strict_report.assignments.is_empty());
    assert_eq!(strict_report.unfulfillable.len(), 2);
    for (_, msg) in &strict_report.unfulfillable {
        assert!(
            msg.contains("THREE_D"),
            "expected THREE_D in unfulfillable error, got: {msg}"
        );
    }

    // 3. Once a worker advertising ProtocolCapabilities::THREE_D registers, both are placed on it.
    let mut worker_with_3d = worker_no_3d.clone();
    worker_with_3d.worker_id = Uuid::new_v4();
    worker_with_3d.capabilities.name = "worker-with-3d".into();
    worker_with_3d.features = ProtocolCapabilities::all_supported();
    let workers = vec![worker_no_3d, worker_with_3d.clone()];

    let placed_report = WorkloadScheduler::matchmake(&default_cfg, &tasks, &workers);
    assert_eq!(placed_report.assignments.len(), 2);
    assert_eq!(placed_report.assignments[0].task_id, gen3d_id);
    assert_eq!(
        placed_report.assignments[0].worker_id,
        worker_with_3d.worker_id
    );
    assert_eq!(placed_report.assignments[1].task_id, autorig_id);
    assert_eq!(
        placed_report.assignments[1].worker_id,
        worker_with_3d.worker_id
    );
}

#[tokio::test]
async fn sched4c_speculative_lifecycle_cancel_sibling_and_failure_tolerance() {
    use rusty_grid_core::placement::OPT_REPEATABLE_TAG;

    let queue = TaskQueue::new();
    let w_primary = Uuid::new_v4();
    let w_spec = Uuid::new_v4();

    // 1. Speculative winner completes first, cancels primary sibling, and ignores stale primary result.
    let t1 = Task::new(
        TaskSpec::command("echo", vec!["rep-1".into()]),
        TaskRequirements::generic(2, 128),
    )
    .with_tags(vec![OPT_REPEATABLE_TAG.to_string()]);
    let id1 = queue.submit(t1).await.unwrap();
    queue
        .schedule_tasks_batch_in_sessions(&[(id1, w_primary, 10)])
        .await;
    queue
        .mark_running_from(w_primary, Some(10), &id1)
        .await
        .unwrap();

    assert!(queue.try_start_speculative(id1, w_spec, 20).await.is_some());
    assert!(queue
        .try_start_speculative(id1, Uuid::new_v4(), 30)
        .await
        .is_none());

    // Loads and active running tasks charge both primary and speculative workers.
    let loads = queue.assigned_loads().await;
    assert_eq!(loads[&w_primary].slots, 2);
    assert_eq!(loads[&w_spec].slots, 2);
    assert_eq!(
        loads[&w_spec].unstarted_ram_mb,
        TaskRequirements::generic(2, 128).ram_mb
    );
    let active = queue.active_running_tasks().await;
    assert_eq!(active[&w_primary].len(), 1);
    assert_eq!(active[&w_spec].len(), 1);

    queue
        .mark_running_from(w_spec, Some(20), &id1)
        .await
        .unwrap();
    assert_eq!(queue.assigned_loads().await[&w_spec].unstarted_ram_mb, 0);

    let win = queue
        .record_result_from(
            w_spec,
            Some(20),
            TaskResult::success(w_spec, id1, "fast\n", 50, false),
        )
        .await
        .unwrap();
    assert_eq!(win.disposition, ResultDisposition::Terminal);
    assert_eq!(win.state, TaskState::Completed);
    let (sib_wid, sib_sid, sib_live) = win.cancel_sibling.expect("expected cancel_sibling");
    assert_eq!((sib_wid, sib_sid), (w_primary, Some(10)));
    assert!(sib_live.is_none());
    assert_eq!(
        queue.get_task(&id1).await.unwrap().assigned_worker_id,
        Some(w_spec)
    );
    assert!(queue.assigned_loads().await.is_empty());

    let late_primary = queue
        .record_result_from(
            w_primary,
            Some(10),
            TaskResult::success(w_primary, id1, "slow\n", 900, false),
        )
        .await
        .unwrap();
    assert_eq!(late_primary.disposition, ResultDisposition::Stale);

    // 2. Primary fails while speculative is running -> speculative is promoted and completes task.
    let t2 = Task::new(
        TaskSpec::command("echo", vec!["rep-2".into()]),
        TaskRequirements::generic(1, 64),
    )
    .with_tags(vec![OPT_REPEATABLE_TAG.to_string()]);
    let id2 = queue.submit(t2).await.unwrap();
    queue
        .schedule_tasks_batch_in_sessions(&[(id2, w_primary, 11)])
        .await;
    queue
        .mark_running_from(w_primary, Some(11), &id2)
        .await
        .unwrap();
    assert!(queue.try_start_speculative(id2, w_spec, 21).await.is_some());
    queue
        .mark_running_from(w_spec, Some(21), &id2)
        .await
        .unwrap();

    let primary_fail = queue
        .record_result_from(
            w_primary,
            Some(11),
            TaskResult::failure(w_primary, id2, 2, "", "boom", 100, None),
        )
        .await
        .unwrap();
    assert_eq!(primary_fail.disposition, ResultDisposition::SiblingFailed);
    assert_eq!(primary_fail.state, TaskState::Running);
    assert_eq!(
        queue.get_task(&id2).await.unwrap().assigned_worker_id,
        Some(w_spec)
    );

    let spec_win = queue
        .record_result_from(
            w_spec,
            Some(21),
            TaskResult::success(w_spec, id2, "ok\n", 80, false),
        )
        .await
        .unwrap();
    assert_eq!(spec_win.disposition, ResultDisposition::Terminal);
    assert_eq!(spec_win.state, TaskState::Completed);

    // 3. Cancel notifies both primary and speculative workers.
    let t3 = Task::new(
        TaskSpec::command("sleep", vec!["60".into()]),
        TaskRequirements::generic(1, 64),
    )
    .with_tags(vec![OPT_REPEATABLE_TAG.to_string()]);
    let id3 = queue.submit(t3).await.unwrap();
    queue
        .schedule_tasks_batch_in_sessions(&[(id3, w_primary, 12)])
        .await;
    queue
        .mark_running_from(w_primary, Some(12), &id3)
        .await
        .unwrap();
    assert!(queue.try_start_speculative(id3, w_spec, 22).await.is_some());

    let cancel = queue
        .cancel_task_detailed(&id3, Some("user abort".into()))
        .await
        .unwrap();
    assert!(cancel.newly_cancelled);
    assert_eq!(cancel.worker_to_notify, Some(w_primary));
    let (extra_wid, extra_sid, extra_live) = cancel
        .extra_worker_to_notify
        .expect("expected extra_worker_to_notify");
    assert_eq!((extra_wid, extra_sid), (w_spec, 22));
    assert!(extra_live.is_some());
    assert!(queue.assigned_loads().await.is_empty());
}

#[tokio::test]
async fn tier5_session_resume_hold_mid_speculative_primary_disconnect() {
    use rusty_grid_core::placement::OPT_REPEATABLE_TAG;

    let queue = TaskQueue::new();
    queue.set_session_resume_hold(Duration::from_secs(30)).await;
    let w_primary = Uuid::new_v4();
    let w_spec = Uuid::new_v4();

    let t1 = Task::new(
        TaskSpec::command("sleep", vec!["60".into()]),
        TaskRequirements::generic(1, 64),
    )
    .with_tags(vec![OPT_REPEATABLE_TAG.to_string()]);
    let id1 = queue.submit(t1).await.unwrap();
    queue
        .schedule_tasks_batch_in_sessions(&[(id1, w_primary, 10)])
        .await;
    queue
        .mark_running_from(w_primary, Some(10), &id1)
        .await
        .unwrap();
    assert!(queue.try_start_speculative(id1, w_spec, 20).await.is_some());
    queue
        .mark_running_from(w_spec, Some(20), &id1)
        .await
        .unwrap();

    // Primary disconnects while speculative copy is running: speculative promotes immediately
    // rather than holding the task for primary resume.
    queue
        .handle_session_disconnected(&w_primary, Some(10), "disconnect", false)
        .await;
    let task_after = queue.get_task(&id1).await.unwrap();
    assert_eq!(task_after.assigned_worker_id, Some(w_spec));
    assert_eq!(task_after.state, TaskState::Running);

    // Old primary reconnects and offers id1 in ResumeSession: must be in stale_to_cancel.
    let adopt = queue.adopt_session_tasks(w_primary, 11, &[id1]).await;
    assert!(adopt.newly_adopted.is_empty());
    assert!(adopt.already_owned.is_empty());
    assert_eq!(adopt.stale_to_cancel, vec![id1]);
    assert_eq!(
        queue.get_task(&id1).await.unwrap().assigned_worker_id,
        Some(w_spec)
    );
}

#[tokio::test]
async fn tier5_session_resume_hold_mid_speculative_secondary_disconnect() {
    use rusty_grid_core::placement::OPT_REPEATABLE_TAG;

    let queue = TaskQueue::new();
    queue.set_session_resume_hold(Duration::from_secs(30)).await;
    let w_primary = Uuid::new_v4();
    let w_spec = Uuid::new_v4();

    let t1 = Task::new(
        TaskSpec::command("sleep", vec!["60".into()]),
        TaskRequirements::generic(1, 64),
    )
    .with_tags(vec![OPT_REPEATABLE_TAG.to_string()]);
    let id1 = queue.submit(t1).await.unwrap();
    queue
        .schedule_tasks_batch_in_sessions(&[(id1, w_primary, 10)])
        .await;
    queue
        .mark_running_from(w_primary, Some(10), &id1)
        .await
        .unwrap();
    assert!(queue.try_start_speculative(id1, w_spec, 20).await.is_some());
    queue
        .mark_running_from(w_spec, Some(20), &id1)
        .await
        .unwrap();

    // Speculative worker disconnects: speculative attempt clears and primary keeps ownership.
    queue
        .handle_session_disconnected(&w_spec, Some(20), "disconnect", false)
        .await;
    let task_after = queue.get_task(&id1).await.unwrap();
    assert_eq!(task_after.assigned_worker_id, Some(w_primary));
    assert_eq!(task_after.state, TaskState::Running);

    // Speculative worker reconnects and offers id1 in ResumeSession: must cancel orphaned copy.
    let adopt = queue.adopt_session_tasks(w_spec, 21, &[id1]).await;
    assert!(adopt.newly_adopted.is_empty());
    assert!(adopt.already_owned.is_empty());
    assert_eq!(adopt.stale_to_cancel, vec![id1]);
    assert_eq!(
        queue.get_task(&id1).await.unwrap().assigned_worker_id,
        Some(w_primary)
    );
}

#[tokio::test]
async fn revoked_primary_checkpoint_is_dropped_when_speculative_copy_is_promoted() {
    use rusty_grid_core::placement::OPT_REPEATABLE_TAG;

    let queue = TaskQueue::new();
    let w_primary = Uuid::new_v4();
    let w_spec = Uuid::new_v4();
    let task = Task::new(
        TaskSpec::command("sleep", vec!["60".into()]),
        TaskRequirements::generic(1, 64),
    )
    .with_tags(vec![OPT_REPEATABLE_TAG.to_string()]);
    let id = queue.submit(task).await.unwrap();
    queue
        .schedule_tasks_batch_in_sessions(&[(id, w_primary, 10)])
        .await;
    queue
        .mark_running_from(w_primary, Some(10), &id)
        .await
        .unwrap();
    let checkpoint = CheckpointData::new(
        1,
        rusty_grid_core::task::Bytes::copy_from_slice(b"from-revoked-member"),
    );
    assert!(queue
        .record_checkpoint_from(w_primary, Some(10), id, checkpoint)
        .await
        .unwrap());
    assert!(queue.get_checkpoint(&id).await.is_some());
    assert!(queue.try_start_speculative(id, w_spec, 20).await.is_some());

    // The primary's member is revoked: the speculative copy is promoted, and the checkpoint the
    // revoked member wrote must not survive for a later resume.
    queue.handle_workers_revoked(&[w_primary], "revoked").await;
    let after = queue.get_task(&id).await.unwrap();
    assert_eq!(after.assigned_worker_id, Some(w_spec));
    assert!(queue.get_checkpoint(&id).await.is_none());
}

#[tokio::test]
async fn large_checkpoints_are_rate_limited_and_leave_headroom_for_submissions() {
    let queue = TaskQueue::new();
    let worker = Uuid::new_v4();
    let task = Task::new(
        TaskSpec::command("sleep", vec!["60".into()]),
        TaskRequirements::generic(1, 64),
    );
    let id = queue.submit(task).await.unwrap();
    queue
        .schedule_tasks_batch_in_sessions(&[(id, worker, 1)])
        .await;
    queue.mark_running_from(worker, Some(1), &id).await.unwrap();

    let big = |seq: u64| {
        CheckpointData::new(
            seq,
            rusty_grid_core::task::Bytes::from(vec![7u8; crate::queue::LARGE_CHECKPOINT_BYTES]),
        )
    };
    assert!(queue
        .record_checkpoint_from(worker, Some(1), id, big(1))
        .await
        .unwrap());
    // A second large checkpoint right away is dropped, not stored or journaled.
    assert!(!queue
        .record_checkpoint_from(worker, Some(1), id, big(2))
        .await
        .unwrap());
    assert_eq!(queue.get_checkpoint(&id).await.unwrap().sequence, 1);
    // Small checkpoints are not rate limited.
    let small = CheckpointData::new(3, rusty_grid_core::task::Bytes::copy_from_slice(b"ok"));
    assert!(queue
        .record_checkpoint_from(worker, Some(1), id, small)
        .await
        .unwrap());
}

#[tokio::test]
async fn paused_refusals_do_not_exhaust_rejection_cap_or_charge_retries() {
    let queue = TaskQueue::new();
    let worker = Uuid::new_v4();
    let task = Task::new(
        TaskSpec::command("echo", vec!["ok".into()]),
        TaskRequirements::generic(1, 64).with_max_retries(0),
    );
    let id = queue.submit(task).await.unwrap();

    for round in 1..=10u64 {
        let scheduled = queue
            .schedule_tasks_batch_in_sessions(&[(id, worker, round)])
            .await;
        assert_eq!(
            scheduled.len(),
            1,
            "round {round} must schedule queued task"
        );
        let rejection = worker_rejection(
            worker,
            id,
            "Task rejected by worker: paused: gaming",
            "Task rejected by worker: paused: gaming",
        );
        let outcome = queue
            .record_result_from(worker, Some(round), rejection)
            .await
            .unwrap();
        assert_eq!(
            outcome.disposition,
            ResultDisposition::Requeued,
            "round {round} paused refusal must requeue uncharged"
        );
        assert_eq!(outcome.state, TaskState::Queued);
        let info = queue.get_task(&id).await.unwrap();
        assert_eq!(info.state, TaskState::Queued);
        assert_eq!(
            info.retry_count, 0,
            "round {round} must keep retry_count == 0"
        );
        assert_eq!(
            info.failover_count, 0,
            "round {round} must keep failover_count == 0"
        );
        let rej_count = queue.inner.read().await.tasks[&id].rejection_count;
        assert_eq!(rej_count, 0, "round {round} must keep rejection_count == 0");
    }
}

#[tokio::test]
async fn no_gpu_tasks_refusals_do_not_exhaust_rejection_cap_or_charge_retries() {
    let queue = TaskQueue::new();
    let worker = Uuid::new_v4();
    let task = Task::new(
        TaskSpec::command("gpu-job", vec![]),
        TaskRequirements::new(1, 512, true, 60).with_max_retries(0),
    );
    let id = queue.submit(task).await.unwrap();

    for round in 1..=10u64 {
        let scheduled = queue
            .schedule_tasks_batch_in_sessions(&[(id, worker, round)])
            .await;
        assert_eq!(
            scheduled.len(),
            1,
            "round {round} must schedule queued GPU task"
        );
        let rejection = worker_rejection(
            worker,
            id,
            "Task rejected by worker: no gpu tasks",
            "Task rejected by worker: no gpu tasks",
        );
        let outcome = queue
            .record_result_from(worker, Some(round), rejection)
            .await
            .unwrap();
        assert_eq!(
            outcome.disposition,
            ResultDisposition::Requeued,
            "round {round} no-gpu refusal must requeue uncharged"
        );
        assert_eq!(outcome.state, TaskState::Queued);
        let info = queue.get_task(&id).await.unwrap();
        assert_eq!(info.state, TaskState::Queued);
        assert_eq!(
            info.retry_count, 0,
            "round {round} must keep retry_count == 0"
        );
        assert_eq!(
            info.failover_count, 0,
            "round {round} must keep failover_count == 0"
        );
        let rej_count = queue.inner.read().await.tasks[&id].rejection_count;
        assert_eq!(rej_count, 0, "round {round} must keep rejection_count == 0");
    }
}
