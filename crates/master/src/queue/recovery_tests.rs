use super::tests::echo_task;
use super::*;
use crate::fair_share::SubmitterId;
use crate::store::TaskStore;
use rusty_grid_core::error::GridError;
use rusty_grid_core::task::{CheckpointData, TaskRequirements, TaskSpec};

#[tokio::test]
async fn test_attach_store_and_replay_recovers_all_d2_5_states_and_preserves_counters() {
    let dir = tempfile::tempdir().unwrap();
    let worker_id = Uuid::new_v4();

    let (
        submitted_id,
        queued_id,
        scheduled_id,
        running_retryable_id,
        running_zero_retry_id,
        retrying_id,
        completed_id,
        failed_id,
        cancelled_id,
    ) = {
        let (store, recovered) = TaskStore::open(dir.path()).unwrap();
        let queue = TaskQueue::with_config(
            RetryPolicy {
                retry_on_execution_failure: true,
                ..RetryPolicy::default()
            },
            None,
        );
        queue
            .attach_store_and_replay(store, recovered, Duration::ZERO)
            .await
            .unwrap();

        let submitted_task = echo_task("submitted");
        let submitted_id = submitted_task.id;

        let queued_id = queue.submit(echo_task("queued")).await.unwrap();

        let scheduled_id = queue.submit(echo_task("scheduled")).await.unwrap();
        queue
            .schedule_tasks_batch_in_sessions(&[(scheduled_id, worker_id, 1)])
            .await;

        let running_retryable_id = queue.submit(echo_task("running-retryable")).await.unwrap();
        queue
            .schedule_tasks_batch_in_sessions(&[(running_retryable_id, worker_id, 1)])
            .await;
        queue
            .mark_running_from(worker_id, Some(1), &running_retryable_id)
            .await
            .unwrap();
        queue
            .record_checkpoint_from(
                worker_id,
                Some(1),
                running_retryable_id,
                CheckpointData::new(3, vec![9, 8, 7]),
            )
            .await
            .unwrap();

        let mut zero_retry_task = echo_task("running-zero-retry");
        zero_retry_task.requirements.max_retries = Some(0);
        let running_zero_retry_id = queue.submit(zero_retry_task).await.unwrap();
        queue
            .schedule_tasks_batch_in_sessions(&[(running_zero_retry_id, worker_id, 1)])
            .await;
        queue
            .mark_running_from(worker_id, Some(1), &running_zero_retry_id)
            .await
            .unwrap();

        let retrying_id = queue.submit(echo_task("retrying")).await.unwrap();
        queue
            .schedule_tasks_batch_in_sessions(&[(retrying_id, worker_id, 1)])
            .await;
        queue
            .mark_running_from(worker_id, Some(1), &retrying_id)
            .await
            .unwrap();
        let retry_outcome = queue
            .record_result_from(
                worker_id,
                Some(1),
                TaskResult::failure(worker_id, retrying_id, 2, "", "transient", 10, None),
            )
            .await
            .unwrap();
        assert_eq!(retry_outcome.state, TaskState::Retrying);

        let completed_id = queue.submit(echo_task("completed")).await.unwrap();
        queue
            .schedule_tasks_batch_in_sessions(&[(completed_id, worker_id, 1)])
            .await;
        queue
            .record_result_from(
                worker_id,
                Some(1),
                TaskResult::success(worker_id, completed_id, "done-out", 5, false),
            )
            .await
            .unwrap();

        let other_worker = Uuid::new_v4();
        let mut fail_once_task = echo_task("failed");
        fail_once_task.requirements.max_retries = Some(0);
        let failed_id = queue.submit(fail_once_task).await.unwrap();
        queue
            .schedule_tasks_batch_in_sessions(&[(failed_id, other_worker, 99)])
            .await;
        queue
            .mark_running_from(other_worker, Some(99), &failed_id)
            .await
            .unwrap();
        queue
            .handle_session_disconnected(&other_worker, Some(99), "boom", false)
            .await;

        // Inject a snapshot entry with `TaskState::Submitted` to cover all 9 D2.5 states.
        {
            let mut inner = queue.inner.write().await;
            let seq = inner.next_sequence;
            inner.next_sequence += 1;
            let submitted_key = crate::speed::TaskKey::for_task(&submitted_task);
            let now_sub = Instant::now();
            inner.tasks.insert(
                submitted_id,
                TaskEntry {
                    task: submitted_task,
                    task_key: submitted_key,
                    priority: 0,
                    submitter: SubmitterId::Default,
                    participant: None,
                    submitted_by_member: None,
                    sequence: seq,
                    state: TaskState::Submitted,
                    assigned_worker_id: None,
                    submitted_at_utc: 100,
                    submitted_instant: now_sub,
                    queued_instant: now_sub,
                    scheduled_instant: None,
                    started_instant: None,
                    retry_after_instant: None,
                    retry_count: 0,
                    failover_count: 0,
                    evacuation_count: 0,
                    rejection_count: 0,
                    avoid_workers: Vec::new(),
                    assigned_session: None,
                    resumed_from_checkpoint: false,
                    adoptable_by: None,
                    fail_if_not_adopted: false,
                    pending_failover_charge: false,
                    pending_failover_reason: None,
                    retry_history: Vec::new(),
                    result: None,
                    cold_result: None,
                    error_message: None,
                    requires_tree_v2: false,
                },
            );
        }

        let cancelled_id = queue.submit(echo_task("cancelled")).await.unwrap();
        queue
            .cancel_task(&cancelled_id, Some("user abort".into()))
            .await
            .unwrap();

        queue.close_and_compact_store().await;

        (
            submitted_id,
            queued_id,
            scheduled_id,
            running_retryable_id,
            running_zero_retry_id,
            retrying_id,
            completed_id,
            failed_id,
            cancelled_id,
        )
    };

    let (store2, recovered2) = TaskStore::open(dir.path()).unwrap();
    let recovered_queue = TaskQueue::new();
    recovered_queue
        .attach_store_and_replay(store2, recovered2, Duration::ZERO)
        .await
        .unwrap();

    assert_eq!(
        recovered_queue.get_state(&submitted_id).await,
        Some(TaskState::Queued)
    );
    assert_eq!(
        recovered_queue.get_state(&queued_id).await,
        Some(TaskState::Queued)
    );

    let sched_info = recovered_queue.get_task(&scheduled_id).await.unwrap();
    assert_eq!(sched_info.state, TaskState::Queued);
    assert_eq!(sched_info.retry_count, 0);
    assert_eq!(sched_info.failover_count, 0);
    assert_eq!(sched_info.evacuation_count, 0);

    let run_info = recovered_queue
        .get_task(&running_retryable_id)
        .await
        .unwrap();
    assert_eq!(run_info.state, TaskState::Queued);
    assert_eq!(run_info.retry_count, 0);
    assert_eq!(run_info.failover_count, 0);
    assert_eq!(run_info.evacuation_count, 0);
    assert_eq!(
        recovered_queue
            .get_checkpoint(&running_retryable_id)
            .await
            .unwrap()
            .sequence,
        3
    );

    let zero_info = recovered_queue
        .get_task(&running_zero_retry_id)
        .await
        .unwrap();
    assert_eq!(zero_info.state, TaskState::Failed);
    assert_eq!(
        zero_info.error_message.as_deref(),
        Some("master restarted while the task ran; not re-run because max_retries is 0")
    );

    let retry_info = recovered_queue.get_task(&retrying_id).await.unwrap();
    assert_eq!(retry_info.state, TaskState::Queued);
    assert_eq!(retry_info.retry_count, 1);

    assert_eq!(
        recovered_queue.get_state(&completed_id).await,
        Some(TaskState::Completed)
    );
    assert_eq!(
        recovered_queue
            .get_result(&completed_id)
            .await
            .unwrap()
            .stdout
            .as_str(),
        "done-out"
    );
    assert_eq!(
        recovered_queue.get_state(&failed_id).await,
        Some(TaskState::Failed)
    );
    assert_eq!(
        recovered_queue.get_state(&cancelled_id).await,
        Some(TaskState::Cancelled)
    );

    recovered_queue.close_and_compact_store().await;
}

#[tokio::test]
async fn test_attach_store_and_replay_adoptable_by_hold_and_adopt_session_tasks() {
    let dir = tempfile::tempdir().unwrap();
    let worker_id = Uuid::new_v4();

    let (adopted_id, omitted_zero_retry_id, omitted_scheduled_id) = {
        let (store, recovered) = TaskStore::open(dir.path()).unwrap();
        let queue = TaskQueue::with_config(
            RetryPolicy {
                retry_on_execution_failure: true,
                ..RetryPolicy::default()
            },
            None,
        );
        queue
            .attach_store_and_replay(store, recovered, Duration::ZERO)
            .await
            .unwrap();

        let adopted_id = queue.submit(echo_task("adopted")).await.unwrap();
        let mut zero_task = echo_task("omitted-zero");
        zero_task.requirements.max_retries = Some(0);
        let omitted_zero_retry_id = queue.submit(zero_task).await.unwrap();
        let omitted_scheduled_id = queue.submit(echo_task("omitted-scheduled")).await.unwrap();

        queue
            .schedule_tasks_batch_in_sessions(&[
                (adopted_id, worker_id, 1),
                (omitted_zero_retry_id, worker_id, 1),
                (omitted_scheduled_id, worker_id, 1),
            ])
            .await;
        queue
            .mark_running_from(worker_id, Some(1), &adopted_id)
            .await
            .unwrap();
        queue
            .mark_running_from(worker_id, Some(1), &omitted_zero_retry_id)
            .await
            .unwrap();

        queue.close_and_compact_store().await;
        (adopted_id, omitted_zero_retry_id, omitted_scheduled_id)
    };

    let (store2, recovered2) = TaskStore::open(dir.path()).unwrap();
    let queue2 = TaskQueue::new();
    queue2
        .attach_store_and_replay(store2, recovered2, Duration::from_secs(30))
        .await
        .unwrap();

    // While held in `adoptable_by`, none of the 3 tasks is offered to a normal scheduling pass.
    assert!(queue2.get_schedulable_tasks().await.is_empty());
    assert!(queue2.ready_views(None, 10).await.0.is_empty());

    let stale_unknown = TaskId::new();
    let outcome = queue2
        .adopt_session_tasks(worker_id, 7, &[adopted_id, stale_unknown])
        .await;

    assert_eq!(outcome.adopted, vec![adopted_id]);
    assert_eq!(outcome.stale_to_cancel, vec![stale_unknown]);
    assert_eq!(outcome.newly_failed, vec![omitted_zero_retry_id]);

    assert_eq!(
        queue2.get_state(&adopted_id).await,
        Some(TaskState::Running)
    );
    let zero_info = queue2.get_task(&omitted_zero_retry_id).await.unwrap();
    assert_eq!(zero_info.state, TaskState::Failed);
    assert_eq!(
        zero_info.error_message.as_deref(),
        Some("master restarted while the task ran; not re-run because max_retries is 0")
    );

    // `omitted_scheduled_id` had its hold released when `worker_id` resumed without it.
    let schedulable = queue2.get_schedulable_tasks().await;
    assert_eq!(schedulable.len(), 1);
    assert_eq!(schedulable[0].id, omitted_scheduled_id);

    queue2.close_and_compact_store().await;
}

#[tokio::test]
async fn test_release_expired_adoption_holds_fails_zero_retry_and_frees_retryable() {
    let dir = tempfile::tempdir().unwrap();
    let worker_id = Uuid::new_v4();

    let (retryable_id, zero_retry_id) = {
        let (store, recovered) = TaskStore::open(dir.path()).unwrap();
        let queue = TaskQueue::with_config(
            RetryPolicy {
                retry_on_execution_failure: true,
                ..RetryPolicy::default()
            },
            None,
        );
        queue
            .attach_store_and_replay(store, recovered, Duration::ZERO)
            .await
            .unwrap();

        let retryable_id = queue.submit(echo_task("retryable")).await.unwrap();
        let mut zero_task = echo_task("zero");
        zero_task.requirements.max_retries = Some(0);
        let zero_retry_id = queue.submit(zero_task).await.unwrap();

        queue
            .schedule_tasks_batch_in_sessions(&[
                (retryable_id, worker_id, 1),
                (zero_retry_id, worker_id, 1),
            ])
            .await;
        queue
            .mark_running_from(worker_id, Some(1), &retryable_id)
            .await
            .unwrap();
        queue
            .mark_running_from(worker_id, Some(1), &zero_retry_id)
            .await
            .unwrap();

        queue.close_and_compact_store().await;
        (retryable_id, zero_retry_id)
    };

    let (store2, recovered2) = TaskStore::open(dir.path()).unwrap();
    let queue2 = TaskQueue::new();
    queue2
        .attach_store_and_replay(store2, recovered2, Duration::from_millis(20))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(35)).await;
    let newly_failed = queue2.release_expired_adoption_holds().await;
    assert_eq!(newly_failed, vec![zero_retry_id]);

    let zero_info = queue2.get_task(&zero_retry_id).await.unwrap();
    assert_eq!(zero_info.state, TaskState::Failed);
    assert_eq!(
        zero_info.error_message.as_deref(),
        Some("master restarted while the task ran; not re-run because max_retries is 0")
    );

    let retryable_info = queue2.get_task(&retryable_id).await.unwrap();
    assert_eq!(retryable_info.state, TaskState::Queued);
    assert_eq!(retryable_info.retry_count, 0);
    assert_eq!(retryable_info.failover_count, 0);
    assert_eq!(retryable_info.evacuation_count, 0);
    assert_eq!(queue2.get_schedulable_tasks().await.len(), 1);

    queue2.close_and_compact_store().await;
}

#[derive(Debug, Clone, PartialEq)]
struct ReplaySnap {
    state: TaskState,
    retry: u32,
    failover: u32,
    evac: u32,
    history: usize,
    has_result: bool,
    err: Option<String>,
    ckpt: Option<u64>,
}

async fn replay_snap(q: &TaskQueue, id: &TaskId) -> Option<ReplaySnap> {
    let info = q.get_task(id).await?;
    Some(ReplaySnap {
        state: info.state,
        retry: info.retry_count,
        failover: info.failover_count,
        evac: info.evacuation_count,
        history: q.get_retry_history(id).await.map_or(0, |h| h.len()),
        has_result: q.get_result(id).await.is_some(),
        err: info.error_message,
        ckpt: q.get_checkpoint(id).await.map(|c| c.sequence),
    })
}

fn replay_policy() -> RetryPolicy {
    RetryPolicy {
        retry_on_execution_failure: true,
        initial_backoff_ms: 60_000,
        ..RetryPolicy::default()
    }
}

async fn drive_all_transitions(q: &TaskQueue) -> Vec<(TaskId, u32)> {
    let w = Uuid::new_v4();
    let w2 = Uuid::new_v4();
    let mut ids = Vec::new();
    for i in 0..14u32 {
        let prio = [0, 5, 1, 5, 0, 2, 0, 0, 3, 0, 1, 0, 0, 4][i as usize];
        let id = q
            .submit_with_priority(echo_task(&format!("t{i}")), prio)
            .await
            .unwrap();
        ids.push((id, prio));
    }
    let v: Vec<TaskId> = ids.iter().map(|(t, _)| *t).collect();
    let id = |i: usize| v[i];
    // Finish a few early tasks so retention eviction happens inside later sweeps.
    for i in [10usize, 11, 12] {
        q.schedule_tasks_batch_in_sessions(&[(id(i), w, 1)]).await;
        q.record_result_from(w, Some(1), TaskResult::success(w, id(i), "early", 1, false))
            .await
            .unwrap();
    }
    q.schedule_tasks_batch_in_sessions(&(1..=6).map(|i| (id(i), w, 1)).collect::<Vec<_>>())
        .await;
    for i in 2..=6 {
        q.mark_running_from(w, Some(1), &id(i)).await.unwrap();
    }
    q.record_checkpoint_from(w, Some(1), id(2), CheckpointData::new(3, vec![1, 2, 3]))
        .await
        .unwrap();
    q.record_result_from(w, Some(1), TaskResult::success(w, id(3), "ok", 5, false))
        .await
        .unwrap();
    // Execution failure -> Retrying (long backoff).
    q.record_result_from(
        w,
        Some(1),
        TaskResult::failure(w, id(4), 2, "", "x", 10, None),
    )
    .await
    .unwrap();
    // Worker rejection -> Queued, rejection_count 1.
    q.record_result_from(
        w,
        Some(1),
        TaskResult::failure(
            w,
            id(5),
            1,
            "",
            "",
            0,
            Some("capabilities insufficient".into()),
        ),
    )
    .await
    .unwrap();
    q.cancel_task(&id(6), Some("user abort running".into()))
        .await
        .unwrap();
    q.cancel_task(&id(7), Some("user abort queued".into()))
        .await
        .unwrap();
    q.fail_unfulfillable_task(&id(8), "no worker can".into())
        .await;
    // Tighten retention so the next sweep evicts while it runs.
    q.set_max_terminal_tasks(3).await;
    // Evacuation on w2.
    q.schedule_tasks_batch_in_sessions(&[(id(9), w2, 2)]).await;
    q.mark_running_from(w2, Some(2), &id(9)).await.unwrap();
    q.handle_session_evacuated(&w2, Some(2), "drain").await;
    // Disconnect of w: t1 Scheduled, t2 Running with checkpoint -> charged failover.
    let mut zero = echo_task("zero");
    zero.requirements.max_retries = Some(0);
    let zid = q.submit(zero).await.unwrap();
    ids.push((zid, 0));
    q.schedule_tasks_batch_in_sessions(&[(zid, w, 1)]).await;
    q.mark_running_from(w, Some(1), &zid).await.unwrap();
    q.handle_session_disconnected(&w, Some(1), "boom", true)
        .await;
    // Leave one Scheduled and one Running (with retry budget > 0) for replay.
    q.schedule_tasks_batch_in_sessions(&[(id(0), w2, 3), (id(13), w2, 3)])
        .await;
    q.mark_running_from(w2, Some(3), &id(13)).await.unwrap();
    ids
}

fn expected_replayed_state(live: TaskState) -> TaskState {
    match live {
        TaskState::Scheduled | TaskState::Running | TaskState::Retrying | TaskState::Submitted => {
            TaskState::Queued
        }
        s => s,
    }
}

async fn compare_replay_against_live(label: &str, opts: crate::store::StoreOptions) -> Vec<String> {
    compare_replay_against_live_with_retention(label, opts, DEFAULT_MAX_TERMINAL_TASKS).await
}

async fn compare_replay_against_live_with_retention(
    label: &str,
    opts: crate::store::StoreOptions,
    retention: usize,
) -> Vec<String> {
    let dir = tempfile::tempdir().unwrap();
    let (live, ids) = {
        let (store, rec) = TaskStore::open_with(dir.path(), opts.clone()).unwrap();
        let q = TaskQueue::with_retention(replay_policy(), None, retention);
        q.attach_store_and_replay(store, rec, Duration::ZERO)
            .await
            .unwrap();
        let ids = drive_all_transitions(&q).await;
        let mut live = Vec::new();
        for (id, _) in &ids {
            live.push(replay_snap(&q, id).await);
        }
        // Crash-equivalent: drop without calling close_and_compact_store (writer flushes queued records).
        drop(q);
        (live, ids)
    };
    let (store, rec) = TaskStore::open_with(dir.path(), opts).unwrap();
    if label == "journal-only" || label == "evict-journal-only" {
        assert!(
            rec.snapshot.is_none(),
            "journal-only replay must have no snapshot.bin"
        );
        assert!(
            rec.records.len() > 15,
            "journal-only replay must recover > 15 journal records, got {}",
            rec.records.len()
        );
    }
    let q2 = TaskQueue::with_retention(replay_policy(), None, retention);
    q2.attach_store_and_replay(store, rec, Duration::ZERO)
        .await
        .unwrap();
    let mut diffs = Vec::new();
    let mut expect_ready = Vec::new();
    for (i, ((id, prio), l)) in ids.iter().zip(live.iter()).enumerate() {
        let got = replay_snap(&q2, id).await;
        let want = l.clone().map(|mut s| {
            s.state = expected_replayed_state(s.state);
            s
        });
        if let Some(ref w) = want {
            if w.state == TaskState::Queued {
                expect_ready.push((std::cmp::Reverse(*prio), i, *id));
            }
        }
        if got != want {
            diffs.push(format!(
                "[{label}] t{i}: live->expected {want:?}\n            replayed {got:?}"
            ));
        }
    }
    expect_ready.sort_by_key(|(p, i, _)| (*p, *i));
    let expect_ready: Vec<TaskId> = expect_ready.into_iter().map(|(_, _, id)| id).collect();
    let ready = q2.ready_task_ids(100).await;
    if ready != expect_ready {
        diffs.push(format!(
            "[{label}] ready order differs:\n want {expect_ready:?}\n got  {ready:?}"
        ));
    }
    // next_sequence restored: a new submission sorts after every recovered one of equal priority.
    let fresh = q2.submit(echo_task("fresh")).await.unwrap();
    let r = q2.ready_task_ids(100).await;
    if r.last() != Some(&fresh) {
        diffs.push(format!("[{label}] new task not last: {r:?}"));
    }
    diffs
}

#[tokio::test]
async fn test_journal_replay_without_compaction_matches_live() {
    let diffs =
        compare_replay_against_live("journal-only", crate::store::StoreOptions::default()).await;
    assert!(
        diffs.is_empty(),
        "replay differences:\n{}",
        diffs.join("\n")
    );
}

#[tokio::test]
async fn test_replay_with_frequent_compaction_matches_live() {
    let mut all = Vec::new();
    for n in [1u64, 2, 3, 5, 8, 13] {
        let opts = crate::store::StoreOptions {
            compact_max_records: n,
            ..crate::store::StoreOptions::default()
        };
        all.extend(compare_replay_against_live(&format!("compact@{n}"), opts).await);
    }
    assert!(
        all.is_empty(),
        "frequent compaction replay differences:\n{}",
        all.join("\n")
    );
}

#[tokio::test]
async fn test_scheduled_zero_retry_task_fails_on_restart_and_persists_fail_if_not_adopted_across_compaction(
) {
    let dir = tempfile::tempdir().unwrap();
    let worker_id = Uuid::new_v4();

    // 1. Crash while a max_retries == 0 task is in `Scheduled` (before `mark_running_from`),
    //    with no snapshot compaction (journal-only).
    let scheduled_zero_id = {
        let (store, recovered) = TaskStore::open(dir.path()).unwrap();
        let queue = TaskQueue::with_config(replay_policy(), None);
        queue
            .attach_store_and_replay(store, recovered, Duration::ZERO)
            .await
            .unwrap();

        let mut zero_task = echo_task("scheduled-zero");
        zero_task.requirements.max_retries = Some(0);
        let tid = queue.submit(zero_task).await.unwrap();
        queue
            .schedule_tasks_batch_in_sessions(&[(tid, worker_id, 1)])
            .await;
        assert_eq!(queue.get_state(&tid).await, Some(TaskState::Scheduled));
        drop(queue);
        tid
    };

    // Reopen with a 30s adoption hold, then trigger snapshot compaction while held, and drop.
    {
        let opts = crate::store::StoreOptions {
            compact_max_records: 1,
            ..crate::store::StoreOptions::default()
        };
        let (store2, recovered2) = TaskStore::open_with(dir.path(), opts).unwrap();
        let queue2 = TaskQueue::with_config(replay_policy(), None);
        queue2
            .attach_store_and_replay(store2, recovered2, Duration::from_secs(30))
            .await
            .unwrap();

        // Must be held for adoption, not offered to the normal scheduler.
        assert!(queue2.get_schedulable_tasks().await.is_empty());

        // Submit another task to trigger snapshot compaction while `scheduled_zero_id` is held.
        let _other = queue2.submit(echo_task("trigger-compact")).await.unwrap();
        queue2.close_and_compact_store().await;
    }

    // Reopen after compaction with a 20ms hold; when the hold expires, the task must fail
    // rather than re-running because `fail_if_not_adopted` and `adoptable_by_worker` were persisted.
    let (store3, recovered3) = TaskStore::open(dir.path()).unwrap();
    assert!(recovered3.snapshot.is_some());
    let queue3 = TaskQueue::with_config(replay_policy(), None);
    queue3
        .attach_store_and_replay(store3, recovered3, Duration::from_millis(20))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(35)).await;
    let newly_failed = queue3.release_expired_adoption_holds().await;
    assert_eq!(newly_failed, vec![scheduled_zero_id]);
    let info = queue3.get_task(&scheduled_zero_id).await.unwrap();
    assert_eq!(info.state, TaskState::Failed);
    assert_eq!(
        info.error_message.as_deref(),
        Some("master restarted while the task ran; not re-run because max_retries is 0")
    );
    queue3.close_and_compact_store().await;
}

#[tokio::test]
async fn test_storage_failure_rolls_back_submit_and_preserves_in_flight_result_transition() {
    let dir = tempfile::tempdir().unwrap();
    let (store, rec) = TaskStore::open(dir.path()).unwrap();
    let q = TaskQueue::with_config(replay_policy(), None);
    q.attach_store_and_replay(store, rec, Duration::ZERO)
        .await
        .unwrap();

    let ok_id = q.submit(echo_task("before")).await.unwrap();
    let w = Uuid::new_v4();
    q.schedule_tasks_batch_in_sessions(&[(ok_id, w, 1)]).await;
    let baseline_active_bytes = q.active_retained_bytes().await;

    // Inject sync_data failure on the next batch.
    q.inject_store_sync_failure(1).await;

    let t = echo_task("during-failure");
    let tid = t.id;
    let res = q.submit(t).await;
    assert!(
        matches!(res, Err(GridError::Storage(_))),
        "expected Err(GridError::Storage), got {res:?}"
    );
    assert_eq!(
        q.get_state(&tid).await,
        None,
        "failed submission must be rolled back from tasks table"
    );
    assert!(
        !q.ready_task_ids(10).await.contains(&tid),
        "failed submission must not be in ready_queue"
    );
    assert_eq!(
        q.active_retained_bytes().await,
        baseline_active_bytes,
        "failed submission must not leak active_bytes"
    );

    // A second submission while the store is already marked failed is refused before mutating tasks.
    let t2 = echo_task("after-failure");
    let tid2 = t2.id;
    let res2 = q.submit(t2).await;
    assert!(matches!(res2, Err(GridError::Storage(_))));
    assert_eq!(q.get_state(&tid2).await, None);
    assert!(!q.ready_task_ids(10).await.contains(&tid2));

    // Recording a result for an already-admitted task commits in memory and returns Ok
    // so the server can release the worker's slot, unpin blobs, and wake completion waiters.
    let outcome = q
        .record_result_from(w, Some(1), TaskResult::success(w, ok_id, "o", 1, false))
        .await
        .expect("record_result_from must succeed in memory even if store is failed");
    assert_eq!(outcome.disposition, ResultDisposition::Terminal);
    assert_eq!(q.get_state(&ok_id).await, Some(TaskState::Completed));
}

#[tokio::test]
async fn test_release_expired_adoption_holds_fast_path_with_10k_tasks() {
    let q = TaskQueue::with_config(replay_policy(), None);
    let w = Uuid::new_v4();
    let mut ids = Vec::new();
    for i in 0..10_000 {
        ids.push(q.submit(echo_task(&format!("c{i}"))).await.unwrap());
    }
    let assign: Vec<_> = ids.iter().map(|id| (*id, w, 1u64)).collect();
    q.schedule_tasks_batch_in_sessions(&assign).await;
    for id in &ids {
        q.record_result_from(w, Some(1), TaskResult::success(w, *id, "o", 1, false))
            .await
            .unwrap();
    }
    let start = Instant::now();
    for _ in 0..1_000 {
        assert!(q.release_expired_adoption_holds().await.is_empty());
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(50),
        "1,000 calls to release_expired_adoption_holds with 10,000 retained tasks took {elapsed:?} (expected fast-path < 50ms)"
    );
}

#[tokio::test]
async fn test_submit_holds_off_ready_queue_until_durable_ack_resolves() {
    let dir = tempfile::tempdir().unwrap();
    let opts = crate::store::StoreOptions {
        group_commit_window: Duration::from_millis(30),
        ..crate::store::StoreOptions::default()
    };
    let (store, rec) = TaskStore::open_with(dir.path(), opts).unwrap();
    let q = TaskQueue::with_config(replay_policy(), None);
    q.attach_store_and_replay(store, rec, Duration::ZERO)
        .await
        .unwrap();

    let t = echo_task("hold-off-ready");
    let tid = t.id;
    let q_clone = q.clone();
    let submit_fut = tokio::spawn(async move { q_clone.submit(t).await });

    // Wait briefly so `submit` enters the group-commit wait before inserting into `ready_queue`.
    tokio::time::sleep(Duration::from_millis(3)).await;
    assert!(
        !q.ready_task_ids(10).await.contains(&tid),
        "task awaiting DurableAck must not be in ready_queue"
    );
    assert!(
        q.get_schedulable_tasks().await.is_empty(),
        "task awaiting DurableAck must not be schedulable"
    );

    let returned_id = submit_fut.await.unwrap().unwrap();
    assert_eq!(returned_id, tid);
    assert_eq!(q.get_state(&tid).await, Some(TaskState::Queued));
    assert!(q.ready_task_ids(10).await.contains(&tid));
}

#[tokio::test]
async fn rvb_drop_submit_future_mid_ack_loses_task_from_ready_queue() {
    let dir = tempfile::tempdir().unwrap();
    let opts = crate::store::StoreOptions {
        group_commit_window: Duration::from_millis(150),
        ..crate::store::StoreOptions::default()
    };
    let (store, rec) = TaskStore::open_with(dir.path(), opts).unwrap();
    let q = TaskQueue::with_config(replay_policy(), None);
    q.attach_store_and_replay(store, rec, Duration::ZERO)
        .await
        .unwrap();

    let t = echo_task("dropped");
    let id = t.id;
    let q2 = q.clone();
    let h = tokio::spawn(async move { q2.submit(t).await });
    // Abort only once `submit` has really entered its durable-ack wait (the task is in the
    // map; it only reaches `ready_queue` after the commit window). A fixed
    // sleep was a race: on a loaded CI host the spawned submit may not have run at all
    // after 20 ms, and aborting it first leaves no task (state `None`).
    let entered = poll_test_condition(|| async { q.get_state(&id).await.is_some() }).await;
    assert!(entered, "submit never reached its durable-ack wait");
    h.abort();
    let _ = h.await;

    // The detached ack task must still publish the task. Wait for that with a generous
    // bound instead of guessing how long the commit window plus scheduling takes.
    let published =
        poll_test_condition(|| async { q.ready_task_ids(10).await.contains(&id) }).await;
    let st = q.get_state(&id).await;
    assert!(
        published,
        "ZOMBIE: task is {st:?} but not in ready_queue (never scheduled)"
    );
    assert_eq!(st, Some(TaskState::Queued));
}

/// Polls `cond` every 5 ms until it holds or 20 s pass (a bound, not an expected delay).
async fn poll_test_condition<F, Fut>(mut cond: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if cond().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    false
}

#[tokio::test]
async fn rvb_rollback_cancel_race() {
    // Case 1: cancel while submit awaits its ack and the ack succeeds -> stays Cancelled, not in ready_queue.
    let dir = tempfile::tempdir().unwrap();
    let opts = crate::store::StoreOptions {
        group_commit_window: Duration::from_millis(150),
        ..crate::store::StoreOptions::default()
    };
    let (store, rec) = TaskStore::open_with(dir.path(), opts.clone()).unwrap();
    let q = TaskQueue::with_config(replay_policy(), None);
    q.attach_store_and_replay(store, rec, Duration::ZERO)
        .await
        .unwrap();

    let t = echo_task("cancel-race-ok");
    let id = t.id;
    let q2 = q.clone();
    let h = tokio::spawn(async move { q2.submit(t).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let c = q.cancel_task(&id, None).await;
    assert!(c.is_ok());
    let r = h.await.unwrap();
    assert!(r.is_ok());
    assert_eq!(q.get_state(&id).await, Some(TaskState::Cancelled));
    assert!(!q.ready_task_ids(10).await.contains(&id));

    // Case 2: cancel while submit awaits its ack and the store sync fails -> rolled back cleanly.
    let dir2 = tempfile::tempdir().unwrap();
    let (store2, rec2) = TaskStore::open_with(dir2.path(), opts).unwrap();
    let q_fail = TaskQueue::with_config(replay_policy(), None);
    q_fail
        .attach_store_and_replay(store2, rec2, Duration::ZERO)
        .await
        .unwrap();
    q_fail.inject_store_sync_failure(1).await;

    let t2 = echo_task("cancel-race-fail");
    let id2 = t2.id;
    let q_fail2 = q_fail.clone();
    let h2 = tokio::spawn(async move { q_fail2.submit(t2).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let _ = q_fail.cancel_task(&id2, None).await;
    let r2 = h2.await.unwrap();
    assert!(r2.is_err());
    assert_eq!(q_fail.get_state(&id2).await, None);
    assert!(!q_fail.ready_task_ids(10).await.contains(&id2));
    assert_eq!(q_fail.active_retained_bytes().await, 0);
    assert_eq!(q_fail.terminal_retained_bytes().await, 0);
}

#[tokio::test]
async fn rvb_zero_retry_scheduled_after_crash() {
    let dir = tempfile::tempdir().unwrap();
    let (store, rec) = TaskStore::open(dir.path()).unwrap();
    let q = TaskQueue::with_config(replay_policy(), None);
    q.attach_store_and_replay(store, rec, Duration::ZERO)
        .await
        .unwrap();
    let mut t = echo_task("zero");
    t.requirements.max_retries = Some(0);
    let id = t.id;
    q.submit(t).await.unwrap();
    let mut t2 = echo_task("nonzero");
    t2.requirements.max_retries = Some(3);
    let id2 = t2.id;
    q.submit(t2).await.unwrap();
    let w = Uuid::new_v4();
    q.schedule_tasks_batch_in_sessions(&[(id, w, 1), (id2, w, 1)])
        .await;
    drop(q);
    let (store, rec) = TaskStore::open(dir.path()).unwrap();
    let q2 = TaskQueue::with_config(replay_policy(), None);
    q2.attach_store_and_replay(store, rec, Duration::ZERO)
        .await
        .unwrap();
    let a = q2.get_task(&id).await.unwrap();
    let b = q2.get_task(&id2).await.unwrap();
    assert_eq!(a.state, TaskState::Failed);
    assert_eq!(b.state, TaskState::Queued);
}

#[tokio::test]
async fn rvb_compaction_boundaries_many() {
    let mut all = Vec::new();
    for n in 1u64..=24 {
        let opts = crate::store::StoreOptions {
            compact_max_records: n,
            ..crate::store::StoreOptions::default()
        };
        all.extend(compare_replay_against_live(&format!("compact@{n}"), opts).await);
    }
    assert!(
        all.is_empty(),
        "compaction boundary differences:\n{}",
        all.join("\n")
    );
}

#[tokio::test]
async fn rvb_compaction_with_eviction() {
    let mut all = Vec::new();
    for n in 1u64..=20 {
        let opts = crate::store::StoreOptions {
            compact_max_records: n,
            ..crate::store::StoreOptions::default()
        };
        all.extend(
            compare_replay_against_live_with_retention(&format!("evict-compact@{n}"), opts, 3)
                .await,
        );
    }
    all.extend(
        compare_replay_against_live_with_retention(
            "evict-journal-only",
            crate::store::StoreOptions::default(),
            3,
        )
        .await,
    );
    assert!(
        all.is_empty(),
        "compaction with eviction differences:\n{}",
        all.join("\n")
    );
}

#[tokio::test]
async fn test_session_disconnect_with_session_resume_defers_failover_until_unadopted_or_expired() {
    let queue = TaskQueue::with_config(
        RetryPolicy {
            max_retries: 2,
            max_failovers: 2,
            retry_on_worker_disconnect: true,
            initial_backoff_ms: 0,
            ..RetryPolicy::default()
        },
        None,
    );
    queue
        .set_session_resume_hold(Duration::from_millis(50))
        .await;

    let worker_id = Uuid::new_v4();
    queue.set_worker_session_resume(worker_id, true);

    let t_adopted = queue.submit(echo_task("adopted")).await.unwrap();
    let t_omitted = queue.submit(echo_task("omitted")).await.unwrap();
    let mut zero_task = echo_task("zero-retry");
    zero_task.requirements.max_retries = Some(0);
    let t_zero = queue.submit(zero_task).await.unwrap();

    queue
        .schedule_tasks_batch_in_sessions(&[
            (t_adopted, worker_id, 1),
            (t_omitted, worker_id, 1),
            (t_zero, worker_id, 1),
        ])
        .await;
    for tid in [t_adopted, t_omitted, t_zero] {
        queue
            .mark_running_from(worker_id, Some(1), &tid)
            .await
            .unwrap();
    }

    // Disconnect session 1: all 3 tasks are held in `Queued` with `adoptable_by` and
    // `pending_failover_charge = true`, with 0 failovers charged yet.
    let affected = queue
        .handle_session_disconnected(&worker_id, Some(1), "Worker dropped", true)
        .await;
    assert_eq!(affected.len(), 3);
    assert!(queue.ready_task_ids(10).await.is_empty());

    for tid in [t_adopted, t_omitted, t_zero] {
        let info = queue.get_task(&tid).await.unwrap();
        assert_eq!(info.state, TaskState::Queued);
        assert_eq!(info.retry_count, 0);
        assert_eq!(info.failover_count, 0);
    }

    // Reconnect as session 2 and adopt only `t_adopted`:
    // - `t_adopted` returns to `Running` with `failover_count == 0`
    // - `t_omitted` is released immediately with its deferred failover charge (`failover_count == 1`)
    // - `t_zero` fails immediately because `max_retries == 0`
    let outcome = queue.adopt_session_tasks(worker_id, 2, &[t_adopted]).await;
    assert_eq!(outcome.adopted, vec![t_adopted]);
    assert!(outcome.stale_to_cancel.is_empty());
    assert_eq!(outcome.newly_failed, vec![t_zero]);

    let adopted_info = queue.get_task(&t_adopted).await.unwrap();
    assert_eq!(adopted_info.state, TaskState::Running);
    assert_eq!(adopted_info.retry_count, 0);
    assert_eq!(adopted_info.failover_count, 0);

    let omitted_info = queue.get_task(&t_omitted).await.unwrap();
    assert_eq!(omitted_info.state, TaskState::Queued);
    assert_eq!(omitted_info.retry_count, 1);
    assert_eq!(omitted_info.failover_count, 1);

    let zero_info = queue.get_task(&t_zero).await.unwrap();
    assert_eq!(zero_info.state, TaskState::Failed);

    // Now disconnect session 2 and let the hold expire for `t_adopted`.
    queue
        .handle_session_disconnected(&worker_id, Some(2), "Second drop", true)
        .await;
    tokio::time::sleep(Duration::from_millis(65)).await;
    let expired_failed = queue.release_expired_adoption_holds().await;
    assert!(expired_failed.is_empty());

    let adopted_after_expiry = queue.get_task(&t_adopted).await.unwrap();
    assert_eq!(adopted_after_expiry.state, TaskState::Queued);
    assert_eq!(adopted_after_expiry.retry_count, 1);
    assert_eq!(adopted_after_expiry.failover_count, 1);

    // Late `ResumeSession` after the hold expired must report `t_adopted` in `stale_to_cancel`.
    let late = queue.adopt_session_tasks(worker_id, 3, &[t_adopted]).await;
    assert!(late.adopted.is_empty());
    assert_eq!(late.stale_to_cancel, vec![t_adopted]);
}

#[tokio::test]
async fn test_workspace_tasks_are_not_placed_on_workers_without_the_workspaces_bit() {
    use crate::registry::{WorkerInfo, WorkerStatus};
    use crate::scheduler::{ScheduleSkipReason, SchedulerConfig, WorkloadScheduler};
    use rusty_grid_core::capabilities::WorkerCapabilities;
    use rusty_grid_core::task::WorkspaceRef;

    let ws_task = Task::new(
        TaskSpec::new_command("cargo", vec!["test".into()]),
        TaskRequirements::generic(1, 30),
    )
    .with_workspace(WorkspaceRef::new("agent-1"));
    let plain_task = Task::new(
        TaskSpec::new_command("echo", vec![]),
        TaskRequirements::generic(1, 30),
    );
    assert!(TaskView::of(&ws_task)
        .required_features
        .contains(ProtocolCapabilities::WORKSPACES));
    assert!(!TaskView::of(&plain_task)
        .required_features
        .contains(ProtocolCapabilities::WORKSPACES));

    let worker = |features| WorkerInfo {
        worker_id: Uuid::new_v4(),
        session_id: 1,
        capabilities: WorkerCapabilities::new("w", 4, 8192, false, false, None),
        features,
        status: WorkerStatus::Connected,
        last_heartbeat_timestamp: 1000,
        registered_timestamp: 1000,
        active_tasks: 0,
        remote_addr: "127.0.0.1:9000".into(),
        cpu_usage_pct: 0.0,
        ram_available_mb: 8192,
        mobile_status: None,
    };
    let cfg = SchedulerConfig::default();
    let tasks = vec![ws_task.clone(), plain_task.clone()];

    // `all_supported()` does not carry the bit: the workspace task waits, the plain one runs.
    let w = worker(ProtocolCapabilities::all_supported());
    let report = WorkloadScheduler::matchmake(&cfg, &tasks, std::slice::from_ref(&w));
    assert_eq!(report.assignments.len(), 1);
    assert_eq!(report.assignments[0].task_id, plain_task.id);
    assert_eq!(report.skipped.len(), 1);
    match &report.skipped[0].1 {
        ScheduleSkipReason::ClusterLacksCapability(msg) => {
            assert!(msg.contains("WORKSPACES"), "{msg}")
        }
        other => panic!("expected ClusterLacksCapability, got {other:?}"),
    }

    // A worker that advertises the bit gets both.
    let mut features = ProtocolCapabilities::all_supported();
    features.insert(ProtocolCapabilities::WORKSPACES);
    let w = worker(features);
    let report = WorkloadScheduler::matchmake(&cfg, &tasks, std::slice::from_ref(&w));
    assert!(report.skipped.is_empty());
    assert!(report.assignments.iter().any(|a| a.task_id == ws_task.id));

    // A workspace task with a tree source requires both WORKSPACES and BLOB_TRANSFER.
    let ws_tree_task = Task::new(
        TaskSpec::new_command("cargo", vec!["test".into()]),
        TaskRequirements::generic(1, 30),
    )
    .with_workspace(
        WorkspaceRef::new("agent-1").with_tree(rusty_grid_core::blob::BlobId::of(b"manifest"), 16),
    );
    let tree_features = TaskView::of(&ws_tree_task).required_features;
    assert!(tree_features.contains(ProtocolCapabilities::WORKSPACES));
    assert!(tree_features.contains(ProtocolCapabilities::BLOB_TRANSFER));
    assert!(!tree_features.contains(ProtocolCapabilities::TREE_V2));

    // When `submit_with_priority_and_tree_v2(..., true)` records `requires_tree_v2` on the
    // queue entry, `ready_views` adds `ProtocolCapabilities::TREE_V2` for that task only.
    let v2_queue = TaskQueue::new();
    let v1_task = ws_tree_task.clone();
    let mut v2_task = ws_tree_task;
    v2_task.id = TaskId::new();
    v2_queue
        .submit_with_priority_and_tree_v2(v1_task.clone(), 0, false)
        .await
        .unwrap();
    v2_queue
        .submit_with_priority_and_tree_v2(v2_task.clone(), 0, true)
        .await
        .unwrap();
    let (views, _) = v2_queue.ready_views(None, 10).await;
    assert_eq!(views.len(), 2);
    let v1_view = views.iter().find(|v| v.task_id == v1_task.id).unwrap();
    let v2_view = views.iter().find(|v| v.task_id == v2_task.id).unwrap();
    assert!(!v1_view
        .required_features
        .contains(ProtocolCapabilities::TREE_V2));
    assert!(v2_view
        .required_features
        .contains(ProtocolCapabilities::TREE_V2));
}

#[tokio::test]
async fn rv158_probe_expired_hold_dispatched_before_release_skips_failover_charge() {
    let queue = TaskQueue::with_config(
        RetryPolicy {
            max_retries: 2,
            max_failovers: 2,
            retry_on_worker_disconnect: true,
            initial_backoff_ms: 0,
            ..RetryPolicy::default()
        },
        None,
    );
    queue
        .set_session_resume_hold(Duration::from_millis(20))
        .await;
    let w1 = Uuid::new_v4();
    let w2 = Uuid::new_v4();
    queue.set_worker_session_resume(w1, true);
    let t = queue.submit(echo_task("rv158-p2")).await.unwrap();
    queue.schedule_tasks_batch_in_sessions(&[(t, w1, 1)]).await;
    queue.mark_running_from(w1, Some(1), &t).await.unwrap();
    queue
        .handle_session_disconnected(&w1, Some(1), "drop", true)
        .await;
    assert!(
        queue.ready_task_ids(10).await.is_empty(),
        "held while hold active"
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(queue.ready_task_ids(10).await, vec![t]);
    let placed = queue.schedule_tasks_batch_in_sessions(&[(t, w2, 7)]).await;
    assert_eq!(
        placed.len(),
        1,
        "expired-but-unreleased hold was dispatched"
    );
    let _ = queue.release_expired_adoption_holds().await;
    let info = queue.get_task(&t).await.unwrap();
    eprintln!(
        "RV158 PROBE P2: state={:?} retry_count={} failover_count={}",
        info.state, info.retry_count, info.failover_count
    );
    assert_eq!(
        info.failover_count, 1,
        "deferred failover charge was skipped"
    );
}

#[tokio::test]
async fn test_cross_worker_cannot_adopt_another_workers_held_task() {
    let queue = TaskQueue::with_config(
        RetryPolicy {
            max_retries: 2,
            max_failovers: 2,
            retry_on_worker_disconnect: true,
            initial_backoff_ms: 0,
            ..RetryPolicy::default()
        },
        None,
    );
    queue.set_session_resume_hold(Duration::from_secs(30)).await;
    let worker_a = Uuid::new_v4();
    let worker_b = Uuid::new_v4();
    queue.set_worker_session_resume(worker_a, true);
    queue.set_worker_session_resume(worker_b, true);

    let tid = queue.submit(echo_task("held-by-a")).await.unwrap();
    queue
        .schedule_tasks_batch_in_sessions(&[(tid, worker_a, 1)])
        .await;
    queue
        .mark_running_from(worker_a, Some(1), &tid)
        .await
        .unwrap();
    queue
        .handle_session_disconnected(&worker_a, Some(1), "drop", true)
        .await;

    // `worker_b` attempts to adopt `worker_a`'s held task: must be rejected in `stale_to_cancel`.
    let b_outcome = queue.adopt_session_tasks(worker_b, 2, &[tid]).await;
    assert!(b_outcome.adopted.is_empty());
    assert_eq!(b_outcome.stale_to_cancel, vec![tid]);

    // `worker_b` attempts to send a Checkpoint and a TaskResult for `worker_a`'s held task:
    // both must be refused (`Ok(false)` and `ResultDisposition::Stale`).
    let cp = CheckpointData::new(1, b"forged-cp".to_vec());
    let cp_applied = queue
        .record_checkpoint_from(worker_b, Some(2), tid, cp)
        .await
        .unwrap();
    assert!(
        !cp_applied,
        "worker_b must not be able to checkpoint a task held by worker_a"
    );
    assert!(queue.get_checkpoint(&tid).await.is_none());

    let forged_res = TaskResult::success(worker_b, tid, "forged", 10, false);
    let res_outcome = queue
        .record_result_from(worker_b, Some(2), forged_res)
        .await
        .unwrap();
    assert_eq!(
        res_outcome.disposition,
        ResultDisposition::Stale,
        "worker_b must not be able to complete a task held by worker_a"
    );
    assert!(!res_outcome.adopted_from_hold);

    // And `worker_a`'s hold must still be intact so `worker_a` can adopt it.
    let a_outcome = queue.adopt_session_tasks(worker_a, 3, &[tid]).await;
    assert_eq!(a_outcome.adopted, vec![tid]);
    assert!(a_outcome.stale_to_cancel.is_empty());
    let info = queue.get_task(&tid).await.unwrap();
    assert_eq!(info.state, TaskState::Running);
    assert_eq!(info.assigned_worker_id, Some(worker_a));
}

#[tokio::test]
async fn rv158b_probe_bound_evicts_live_worker_resume_flag() {
    let queue = TaskQueue::new();
    queue.set_session_resume_hold(Duration::from_secs(60)).await;
    let live_worker = Uuid::new_v4();
    queue.set_worker_session_resume(live_worker, true);

    let tid = queue.submit(echo_task("rv158b-p4")).await.unwrap();
    queue
        .schedule_tasks_batch_in_sessions(&[(tid, live_worker, 1)])
        .await;
    queue
        .mark_running_from(live_worker, Some(1), &tid)
        .await
        .unwrap();

    for _ in 0..MAX_SESSION_RESUME_WORKERS {
        queue.set_worker_session_resume(Uuid::new_v4(), true);
    }

    assert!(
        queue
            .inner
            .read()
            .await
            .session_resume_workers
            .lock()
            .unwrap()
            .contains(&live_worker),
        "live worker with an active task must not be evicted by MAX_SESSION_RESUME_WORKERS churn"
    );
}

#[tokio::test]
async fn rv158b_probe_bound_evicts_worker_with_active_hold() {
    let queue = TaskQueue::new();
    queue.set_session_resume_hold(Duration::from_secs(60)).await;
    let worker_a = Uuid::new_v4();
    queue.set_worker_session_resume(worker_a, true);

    let tid = queue.submit(echo_task("rv158b-p4b")).await.unwrap();
    queue
        .schedule_tasks_batch_in_sessions(&[(tid, worker_a, 1)])
        .await;
    queue
        .mark_running_from(worker_a, Some(1), &tid)
        .await
        .unwrap();
    queue
        .handle_session_disconnected(&worker_a, Some(1), "socket drop", true)
        .await;

    for _ in 0..MAX_SESSION_RESUME_WORKERS {
        queue.set_worker_session_resume(Uuid::new_v4(), true);
    }

    assert!(
        queue
            .inner
            .read()
            .await
            .session_resume_workers
            .lock()
            .unwrap()
            .contains(&worker_a),
        "worker with an active adoption hold must not be evicted by MAX_SESSION_RESUME_WORKERS churn"
    );
}

#[tokio::test]
async fn rv158b_probe_evicted_live_worker_gets_no_hold() {
    let queue = TaskQueue::new();
    queue.set_session_resume_hold(Duration::from_secs(60)).await;
    let live_worker = Uuid::new_v4();
    queue.set_worker_session_resume(live_worker, true);

    let mut task = echo_task("rv158b-p4c");
    task.requirements.max_retries = Some(0);
    let tid = queue.submit(task).await.unwrap();
    queue
        .schedule_tasks_batch_in_sessions(&[(tid, live_worker, 1)])
        .await;
    queue
        .mark_running_from(live_worker, Some(1), &tid)
        .await
        .unwrap();

    for _ in 0..MAX_SESSION_RESUME_WORKERS {
        queue.set_worker_session_resume(Uuid::new_v4(), true);
    }

    queue
        .handle_session_disconnected(&live_worker, Some(1), "socket drop", true)
        .await;
    let info = queue.get_task(&tid).await.unwrap();
    assert_eq!(
        info.state,
        TaskState::Queued,
        "live worker's zero-retry task must be held for ResumeSession after disconnect despite churn"
    );
    let outcome = queue.adopt_session_tasks(live_worker, 2, &[tid]).await;
    assert_eq!(outcome.adopted, vec![tid]);
}

#[tokio::test]
async fn test_session_resume_workers_bounded_and_pruned_on_hold_expiry() {
    let queue = TaskQueue::new();
    queue
        .set_session_resume_hold(Duration::from_millis(20))
        .await;

    for _ in 0..(MAX_SESSION_RESUME_WORKERS + 100) {
        queue.set_worker_session_resume(Uuid::new_v4(), true);
    }
    assert_eq!(
        queue
            .inner
            .read()
            .await
            .session_resume_workers
            .lock()
            .unwrap()
            .len(),
        MAX_SESSION_RESUME_WORKERS
    );

    let queue2 = TaskQueue::new();
    queue2
        .set_session_resume_hold(Duration::from_millis(20))
        .await;
    let w = Uuid::new_v4();
    queue2.set_worker_session_resume(w, true);
    let tid = queue2.submit(echo_task("prune-w")).await.unwrap();
    queue2
        .schedule_tasks_batch_in_sessions(&[(tid, w, 1)])
        .await;
    queue2.mark_running_from(w, Some(1), &tid).await.unwrap();
    queue2
        .handle_session_disconnected(&w, Some(1), "drop", true)
        .await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    let _ = queue2.release_expired_adoption_holds().await;
    assert!(
        !queue2
            .inner
            .read()
            .await
            .session_resume_workers
            .lock()
            .unwrap()
            .contains(&w),
        "expired worker with no active tasks or holds must be pruned from session_resume_workers"
    );
}

#[tokio::test]
async fn test_session_resume_connected_survives_stale_session_disconnect_and_registration_churn() {
    let queue = TaskQueue::new();
    queue.set_session_resume_hold(Duration::from_secs(60)).await;
    let live_worker = Uuid::new_v4();

    // Worker connects on session 1, then quickly reconnects on session 2 (idle).
    queue.set_worker_session_resume(live_worker, true);
    queue.set_worker_session_resume_connected(live_worker, 1, true);
    queue.set_worker_session_resume(live_worker, true);
    queue.set_worker_session_resume_connected(live_worker, 2, true);

    // Session 1's late disconnect sweep runs after session 2 is already live.
    queue.set_worker_session_resume_connected(live_worker, 1, false);
    queue
        .handle_session_disconnected(&live_worker, Some(1), "superseded session 1", true)
        .await;

    // Over 4,096 ephemeral workers register and disconnect.
    for i in 0..(MAX_SESSION_RESUME_WORKERS + 128) {
        let ephemeral = Uuid::new_v4();
        let sid = 100 + i as u64;
        queue.set_worker_session_resume(ephemeral, true);
        queue.set_worker_session_resume_connected(ephemeral, sid, true);
        queue.set_worker_session_resume_connected(ephemeral, sid, false);
    }

    // The live worker on session 2 must still be marked SESSION_RESUME-capable.
    assert!(
        queue
            .inner
            .read()
            .await
            .session_resume_workers
            .lock()
            .unwrap()
            .contains(&live_worker),
        "live worker on session 2 must not be evicted after session 1 disconnect sweep and >4,096 registrations"
    );

    // When session 2 later disconnects and another wave of ephemeral workers registers,
    // the worker is no longer connected and can be evicted.
    queue.set_worker_session_resume_connected(live_worker, 2, false);
    queue
        .handle_session_disconnected(&live_worker, Some(2), "session 2 closed", true)
        .await;
    for _ in 0..(MAX_SESSION_RESUME_WORKERS + 10) {
        queue.set_worker_session_resume(Uuid::new_v4(), true);
    }
    assert!(
        !queue
            .inner
            .read()
            .await
            .session_resume_workers
            .lock()
            .unwrap()
            .contains(&live_worker),
        "disconnected idle worker must be evictable once its latest session disconnects"
    );
}

#[tokio::test]
async fn test_workspace_one_at_a_time_across_master_restart_and_disconnect_holds() {
    use rusty_grid_core::task::WorkspaceRef;

    let dir = tempfile::tempdir().unwrap();
    let worker_a = Uuid::new_v4();
    let worker_b = Uuid::new_v4();

    // 1. Reviewer's exact scenario: task A in `ws-a` is Running; store is reopened with a
    //    30 s hold; task B in `ws-a` is submitted.
    let task_a_id = {
        let (store, rec) = TaskStore::open(dir.path()).unwrap();
        let q = TaskQueue::with_config(replay_policy(), None);
        q.attach_store_and_replay(store, rec, Duration::ZERO)
            .await
            .unwrap();

        let task_a = echo_task("ws-a-task-a").with_workspace(WorkspaceRef::new("ws-a"));
        let a_id = q.submit(task_a).await.unwrap();
        q.schedule_tasks_batch_in_sessions(&[(a_id, worker_a, 1)])
            .await;
        q.mark_running_from(worker_a, Some(1), &a_id).await.unwrap();
        drop(q);
        a_id
    };

    let (store2, rec2) = TaskStore::open(dir.path()).unwrap();
    let q2 = TaskQueue::with_config(replay_policy(), None);
    q2.attach_store_and_replay(store2, rec2, Duration::from_secs(30))
        .await
        .unwrap();

    let task_b = echo_task("ws-a-task-b").with_workspace(WorkspaceRef::new("ws-a"));
    let task_b_id = q2.submit(task_b).await.unwrap();

    let (_aff, active_ws) = q2.workspace_scheduling_state().await;
    assert!(
        active_ws.contains("ws-a"),
        "held task A after master restart must keep workspace ws-a active"
    );

    // Neither pop_and_schedule, schedule_task, nor schedule_tasks_batch_in_sessions may
    // dispatch task B while task A is held for adoption.
    assert!(
        q2.pop_and_schedule(worker_b, |_| true).await.is_none(),
        "pop_and_schedule must not return task B while task A holds ws-a"
    );
    assert!(
        q2.schedule_task(&task_b_id, worker_b).await.is_none(),
        "schedule_task must not schedule task B while task A holds ws-a"
    );
    assert!(
        q2.schedule_tasks_batch_in_sessions(&[(task_b_id, worker_b, 1)])
            .await
            .is_empty(),
        "schedule_tasks_batch_in_sessions must not schedule task B while task A holds ws-a"
    );

    // After `adopt_session_tasks`, only A is Running and B stays Queued.
    let adopted = q2.adopt_session_tasks(worker_a, 2, &[task_a_id]).await;
    assert_eq!(adopted.adopted, vec![task_a_id]);
    assert_eq!(q2.get_state(&task_a_id).await, Some(TaskState::Running));
    assert_eq!(q2.get_state(&task_b_id).await, Some(TaskState::Queued));
    assert!(q2.pop_and_schedule(worker_b, |_| true).await.is_none());

    // 2. F1-c disconnect hold: `worker_a` (with SESSION_RESUME) disconnects; task A enters an
    //    adoption hold again, and task B must still be refused until A finishes.
    q2.set_worker_session_resume(worker_a, true);
    q2.handle_session_disconnected(&worker_a, Some(2), "brief drop", true)
        .await;
    assert_eq!(q2.get_state(&task_a_id).await, Some(TaskState::Queued));
    let (_aff, active_after_disc) = q2.workspace_scheduling_state().await;
    assert!(
        active_after_disc.contains("ws-a"),
        "F1-c disconnect hold on task A must keep workspace ws-a active"
    );
    assert!(q2.pop_and_schedule(worker_b, |_| true).await.is_none());
    assert!(q2.schedule_task(&task_b_id, worker_b).await.is_none());

    // Re-adopt and complete task A; now task B can be scheduled.
    let adopted2 = q2.adopt_session_tasks(worker_a, 3, &[task_a_id]).await;
    assert_eq!(adopted2.adopted, vec![task_a_id]);
    q2.record_result_from(
        worker_a,
        Some(3),
        TaskResult::success(worker_a, task_a_id, "ok", 5, false),
    )
    .await
    .unwrap();

    let (_aff, active_after_done) = q2.workspace_scheduling_state().await;
    assert!(!active_after_done.contains("ws-a"));
    let popped_b = q2.pop_and_schedule(worker_b, |_| true).await.unwrap();
    assert_eq!(popped_b.id, task_b_id);
    q2.close_and_compact_store().await;
}

#[tokio::test]
async fn test_workspace_queue_level_guards_refuse_second_task_when_engine_pass_is_stale() {
    use rusty_grid_core::task::WorkspaceRef;

    let q = TaskQueue::new();
    let w1 = Uuid::new_v4();
    let w2 = Uuid::new_v4();

    let t1 = q
        .submit(echo_task("ws-1").with_workspace(WorkspaceRef::new("ws-guard")))
        .await
        .unwrap();
    let t2 = q
        .submit(echo_task("ws-2").with_workspace(WorkspaceRef::new("ws-guard")))
        .await
        .unwrap();
    let _t3 = q
        .submit(echo_task("ws-3").with_workspace(WorkspaceRef::new("ws-guard")))
        .await
        .unwrap();
    let t_other = q
        .submit(echo_task("other").with_workspace(WorkspaceRef::new("ws-other")))
        .await
        .unwrap();

    // Simulate a stale engine pass (or direct batch call) attempting to schedule two tasks of
    // the same workspace (`t1` and `t2`) in one batch alongside `t_other`.
    let placed = q
        .schedule_tasks_batch_in_sessions(&[(t1, w1, 1), (t2, w2, 1), (t_other, w2, 1)])
        .await;
    let placed_ids: Vec<TaskId> = placed.iter().map(|(id, _, _)| *id).collect();
    assert_eq!(
        placed_ids,
        vec![t1, t_other],
        "schedule_batch_inner guard must refuse the second task of ws-guard"
    );
    assert_eq!(q.get_state(&t2).await, Some(TaskState::Queued));

    // While `t1` is Scheduled in `ws-guard`, both `schedule_task` and `pop_and_schedule` must
    // refuse `t2` and `t3`.
    assert!(
        q.schedule_task(&t2, w2).await.is_none(),
        "schedule_task guard must refuse t2 while t1 is Scheduled in ws-guard"
    );
    assert!(
        q.pop_and_schedule(w2, |_| true).await.is_none(),
        "pop_and_schedule guard must refuse t2/t3 while t1 is Scheduled in ws-guard"
    );

    // Complete `t1`; now `schedule_task` accepts `t2`, and while `t2` is Scheduled,
    // `pop_and_schedule` still refuses `t3`.
    q.record_result_from(w1, Some(1), TaskResult::success(w1, t1, "done", 1, false))
        .await
        .unwrap();
    let scheduled_t2 = q.schedule_task(&t2, w2).await.unwrap();
    assert_eq!(scheduled_t2.id, t2);
    assert!(
        q.pop_and_schedule(w1, |_| true).await.is_none(),
        "pop_and_schedule guard must refuse t3 while t2 is Scheduled in ws-guard"
    );

    // Requeue `t2` (e.g. dispatch failure); `ws-guard` is now free and `pop_and_schedule`
    // dispatches `t2`.
    q.requeue_task(&t2).await.unwrap();
    let popped = q.pop_and_schedule(w1, |_| true).await.unwrap();
    assert_eq!(popped.id, t2);
}

#[tokio::test]
async fn test_workspace_affinity_replay_uses_terminal_finish_timestamp() {
    use rusty_grid_core::task::WorkspaceRef;

    let dir = tempfile::tempdir().unwrap();
    let w1 = Uuid::new_v4();
    let tid = TaskId::new();
    let mut task = echo_task("ws-replay-ts").with_workspace(WorkspaceRef::new("ws-ts"));
    task.id = tid;

    {
        let (store, _rec) = TaskStore::open(dir.path()).unwrap();
        store
            .append_durable(&JournalRecord::TaskSubmitted {
                task: Box::new(task),
                priority: 0,
                sequence: 1,
                submitted_at_utc: 1_000,
                submitted_by_member: None,
            })
            .unwrap()
            .await
            .unwrap();
        store
            .append_durable(&JournalRecord::TaskDispatched {
                task_id: tid,
                worker_id: w1,
                timestamp_utc: 1_010,
            })
            .unwrap()
            .await
            .unwrap();
        store
            .append_durable(&JournalRecord::TaskTerminal {
                task_id: tid,
                state: TaskState::Completed,
                assigned_worker_id: Some(w1),
                retry_count: 0,
                failover_count: 0,
                evacuation_count: 0,
                rejection_count: 0,
                retry_attempt: None,
                result: Some(TaskResult::success(w1, tid, "ok", 5, false)),
                error_message: None,
                timestamp_utc: 2_500,
            })
            .unwrap()
            .await
            .unwrap();
    }

    let (store2, rec2) = TaskStore::open(dir.path()).unwrap();
    let q = TaskQueue::new();
    q.attach_store_and_replay(store2, rec2, Duration::ZERO)
        .await
        .unwrap();

    let workspaces = q.list_workspaces().await;
    assert_eq!(workspaces.len(), 1);
    assert_eq!(workspaces[0].name, "ws-ts");
    assert_eq!(workspaces[0].worker, w1);
    assert_eq!(
        workspaces[0].last_used_utc, 2_500,
        "replay must record workspace affinity with the TaskTerminal finish timestamp"
    );
    q.close_and_compact_store().await;
}

#[tokio::test]
async fn test_single_task_schedulers_track_resumed_from_checkpoint() {
    let q = TaskQueue::new();
    let w1 = Uuid::new_v4();
    let w2 = Uuid::new_v4();

    // Fresh task scheduled with `schedule_task` has `resumed_from_checkpoint == false`.
    let fresh_id = q.submit(echo_task("fresh-single")).await.unwrap();
    assert!(q.schedule_task(&fresh_id, w1).await.is_some());
    let fresh_outcome = q
        .record_result_from(w1, None, TaskResult::success(w1, fresh_id, "ok", 20, false))
        .await
        .unwrap();
    assert!(!fresh_outcome.resumed_from_checkpoint);

    // Task with a recorded checkpoint scheduled via `schedule_task` marks
    // `resumed_from_checkpoint == true`.
    let t_sched = q.submit(echo_task("resumed-schedule-task")).await.unwrap();
    assert!(q.schedule_task(&t_sched, w1).await.is_some());
    q.record_checkpoint_from(w1, None, t_sched, CheckpointData::new(1, b"cp1".to_vec()))
        .await
        .unwrap();
    q.requeue_task(&t_sched).await.unwrap();
    assert!(q.schedule_task(&t_sched, w2).await.is_some());
    let sched_outcome = q
        .record_result_from(w2, None, TaskResult::success(w2, t_sched, "ok", 10, false))
        .await
        .unwrap();
    assert!(sched_outcome.resumed_from_checkpoint);

    // Same check for `pop_and_schedule`.
    let t_pop = q.submit(echo_task("resumed-pop-schedule")).await.unwrap();
    let popped_first = q.pop_and_schedule(w1, |_| true).await.unwrap();
    assert_eq!(popped_first.id, t_pop);
    q.record_checkpoint_from(w1, None, t_pop, CheckpointData::new(1, b"cp2".to_vec()))
        .await
        .unwrap();
    q.requeue_task(&t_pop).await.unwrap();
    let popped_second = q.pop_and_schedule(w2, |_| true).await.unwrap();
    assert_eq!(popped_second.id, t_pop);
    let pop_outcome = q
        .record_result_from(w2, None, TaskResult::success(w2, t_pop, "ok", 10, false))
        .await
        .unwrap();
    assert!(pop_outcome.resumed_from_checkpoint);
}

fn count_cold_bin_files(root: &std::path::Path) -> usize {
    let cold_root = root.join(crate::cold_results::COLD_RESULTS_DIR);
    let Ok(shards) = std::fs::read_dir(cold_root) else {
        return 0;
    };
    let mut count = 0usize;
    for shard in shards.flatten() {
        if let Ok(files) = std::fs::read_dir(shard.path()) {
            for f in files.flatten() {
                if f.path().extension().and_then(|e| e.to_str()) == Some("bin") {
                    count += 1;
                }
            }
        }
    }
    count
}

#[tokio::test]
async fn cold_terminal_results_spill_load_compact_reopen_and_evict() {
    let dir = tempfile::tempdir().unwrap();
    let blob_store = crate::blob_store::BlobStore::open(
        &crate::blob_store::BlobStoreConfig::new(dir.path().join("blobs"), 200)
            .with_output_pin_retention(Duration::from_secs(60)),
    )
    .unwrap();
    let out_blob_id = blob_store.put(&[b'o'; 60]).await.unwrap();

    let (store, rec) = TaskStore::open(dir.path()).unwrap();
    let q = TaskQueue::new();
    q.attach_store_and_replay(store, rec, Duration::ZERO)
        .await
        .unwrap();
    q.set_hot_terminal_limits(2, 1024).await;

    let w1 = Uuid::new_v4();
    let mut tids = Vec::new();
    for i in 0..6 {
        let tid = q.submit(echo_task(&format!("cold-{i}"))).await.unwrap();
        assert!(q.schedule_task(&tid, w1).await.is_some());
        let mut res = TaskResult::success(w1, tid, "x".repeat(2048), 5 + i as u64, false);
        res.outputs.push(rusty_grid_core::blob::BlobFile {
            path: format!("out-{i}.bin"),
            id: out_blob_id,
            size: 60,
            executable: false,
        });
        q.record_result_from(w1, None, res).await.unwrap();
        tids.push(tid);
    }

    let (hot_tasks, _hot_bytes) = q.hot_terminal_stats().await;
    assert!(hot_tasks <= 2, "expected <= 2 hot tasks, got {hot_tasks}");

    {
        let inner = q.inner.read().await;
        let oldest = inner.tasks.get(&tids[0]).unwrap();
        assert!(
            oldest.result.is_none(),
            "oldest result must spill out of RAM"
        );
        assert!(
            oldest.cold_result.is_some(),
            "cold summary must stay in RAM"
        );
    }

    let detail = q.get_status_detail(&tids[0]).await.unwrap();
    assert_eq!(detail.result_exit_code, Some(0));
    assert_eq!(detail.result_error, None);

    let loaded = q.get_result(&tids[0]).await.unwrap();
    assert_eq!(loaded.stdout.len(), 2048);
    assert_eq!(loaded.outputs.len(), 1);
    assert_eq!(loaded.outputs[0].id, out_blob_id);

    // Inject an orphan `.bin` file in a valid cold shard dir.
    let orphan_dir = dir
        .path()
        .join(crate::cold_results::COLD_RESULTS_DIR)
        .join("ff");
    std::fs::create_dir_all(&orphan_dir).unwrap();
    let orphan_path = orphan_dir.join(format!("{}.bin", Uuid::new_v4()));
    std::fs::write(&orphan_path, b"orphan-data").unwrap();
    assert!(orphan_path.exists());

    q.compact_store().await.unwrap();
    q.close_and_compact_store().await;
    drop(q);

    let (store2, rec2) = TaskStore::open(dir.path()).unwrap();
    let q2 = TaskQueue::new();
    q2.attach_store_and_replay(store2, rec2, Duration::ZERO)
        .await
        .unwrap();
    assert!(
        !orphan_path.exists(),
        "orphan cold file must be unlinked on reopen"
    );
    for tid in &tids {
        let res = q2
            .get_result(tid)
            .await
            .expect("cold result survives reopen");
        assert_eq!(res.stdout.len(), 2048);
    }

    q2.repin_recovered_blobs(&blob_store).await;
    let b1 = blob_store.put(&[b'a'; 70]).await.unwrap();
    let _b2 = blob_store.put(&[b'b'; 80]).await.unwrap();
    assert!(
        blob_store.contains(&out_blob_id),
        "repin_recovered_blobs must pin cold output blobs"
    );
    assert!(
        !blob_store.contains(&b1),
        "unpinned blob b1 should evict before pinned cold output blob"
    );

    let before_bins = count_cold_bin_files(dir.path());
    assert_eq!(before_bins, 6);
    let evicted = q2.evict_terminal_tasks(1).await;
    assert_eq!(evicted, 5);
    let after_bins = count_cold_bin_files(dir.path());
    assert_eq!(
        after_bins, 1,
        "evicted cold result files must be unlinked from disk"
    );
    assert!(q2.get_result(&tids[0]).await.is_none());
    assert!(q2.get_result(&tids[5]).await.is_some());
    q2.close_and_compact_store().await;
}

#[tokio::test]
async fn concurrent_cold_maintenance_and_uncompacted_journal_replay_reuse() {
    // Part 1: concurrent spawn_cold_maintenance bursts under a tight hot limit.
    let dir1 = tempfile::tempdir().unwrap();
    let (store1, rec1) = TaskStore::open(dir1.path()).unwrap();
    let q1 = TaskQueue::new();
    q1.attach_store_and_replay(store1, rec1, Duration::ZERO)
        .await
        .unwrap();
    q1.set_hot_terminal_limits(4, 4096).await;

    let w1 = Uuid::new_v4();
    let mut tids1 = Vec::with_capacity(200);
    let mut tasks = Vec::with_capacity(200);
    for i in 0..200 {
        let tid = q1.submit(echo_task(&format!("burst-{i}"))).await.unwrap();
        assert!(q1.schedule_task(&tid, w1).await.is_some());
        tids1.push(tid);
    }
    for (i, tid) in tids1.iter().copied().enumerate() {
        let q_ref = q1.clone();
        tasks.push(tokio::spawn(async move {
            let res = TaskResult::success(
                w1,
                tid,
                format!("payload-{i:04}-{}", "y".repeat(64)),
                i as u64,
                false,
            );
            let (_outcome, ack) = q_ref
                .record_result_from_enqueued(w1, None, res)
                .await
                .unwrap();
            if let Some(ack) = ack {
                ack.await.unwrap();
            }
            q_ref.spawn_cold_maintenance().await;
        }));
    }
    for h in tasks {
        h.await.unwrap();
    }
    q1.flush_cold_maintenance().await;
    let (hot1, _) = q1.hot_terminal_stats().await;
    assert!(hot1 <= 4, "expected <= 4 hot tasks, got {hot1}");
    assert_eq!(count_cold_bin_files(dir1.path()), 196);
    for tid in &tids1 {
        assert!(q1.get_result(tid).await.is_some());
    }
    q1.close_and_compact_store().await;

    // Part 2: uncompacted journal replay above DEFAULT_MAX_HOT_TERMINAL_TASKS reuses existing
    // cold files with zero re-writes and still unlinks orphans.
    let total = crate::cold_results::DEFAULT_MAX_HOT_TERMINAL_TASKS + 16;
    let dir2 = tempfile::tempdir().unwrap();
    let (store2, rec2) = TaskStore::open(dir2.path()).unwrap();
    let q2 = TaskQueue::new();
    q2.attach_store_and_replay(store2, rec2, Duration::ZERO)
        .await
        .unwrap();

    let w2 = Uuid::new_v4();
    let mut tids2 = Vec::with_capacity(total);
    let mut acks = Vec::with_capacity(total);
    for i in 0..total {
        let tid = q2.submit(echo_task(&format!("replay-{i}"))).await.unwrap();
        assert!(q2.schedule_task(&tid, w2).await.is_some());
        let res = TaskResult::success(w2, tid, format!("out-{i}").into_bytes(), 1, false);
        let (_outcome, ack) = q2.record_result_from_enqueued(w2, None, res).await.unwrap();
        if let Some(a) = ack {
            acks.push(a);
        }
        tids2.push(tid);
    }
    for ack in acks {
        ack.await.unwrap();
    }
    q2.flush_cold_maintenance().await;
    assert_eq!(count_cold_bin_files(dir2.path()), 16);

    let orphan_dir = dir2
        .path()
        .join(crate::cold_results::COLD_RESULTS_DIR)
        .join("ff");
    std::fs::create_dir_all(&orphan_dir).unwrap();
    let orphan_path = orphan_dir.join(format!("{}.bin", Uuid::new_v4()));
    std::fs::write(&orphan_path, b"orphan-uncompacted").unwrap();

    // Drop without compact_store so TaskStore reopens from an uncompacted journal.bin.
    drop(q2);
    assert!(!dir2.path().join("snapshot.bin").exists());

    let (store3, rec3) = TaskStore::open(dir2.path()).unwrap();
    let q3 = TaskQueue::new();
    q3.attach_store_and_replay(store3, rec3, Duration::ZERO)
        .await
        .unwrap();
    assert!(
        !orphan_path.exists(),
        "orphan must still be removed on replay"
    );
    assert_eq!(count_cold_bin_files(dir2.path()), 16);
    {
        let inner = q3.inner.read().await;
        let cold = inner.cold_results.as_ref().unwrap();
        assert_eq!(
            cold.temp_write_count(),
            0,
            "existing valid cold files must be reused without rewriting"
        );
    }
    assert!(q3.get_result(&tids2[0]).await.is_some());
    assert!(q3.get_result(&tids2[total - 1]).await.is_some());
    q3.close_and_compact_store().await;
}

#[tokio::test]
async fn fair_share_ready_views_interleaves_submitters_with_weights() {
    let q = TaskQueue::new();
    let sub_a = SubmitterId::Client(Uuid::new_v4());
    let sub_b = SubmitterId::Client(Uuid::new_v4());

    let mut set_a = HashSet::new();
    let mut set_b = HashSet::new();
    for i in 0..20 {
        let tid = q
            .submit_from(echo_task(&format!("a-{i}")), 0, sub_a.clone())
            .await
            .unwrap();
        set_a.insert(tid);
    }
    for i in 0..5 {
        let tid = q
            .submit_from(echo_task(&format!("b-{i}")), 0, sub_b.clone())
            .await
            .unwrap();
        set_b.insert(tid);
    }
    assert_eq!(q.active_submitter_count().await, 2);

    let mut cursor = crate::fair_share::FairWalkCursor::default();
    let views = q.ready_views_fair(&mut cursor, 10).await;
    assert_eq!(views.len(), 10);
    let a_count = views.iter().filter(|v| set_a.contains(&v.task_id)).count();
    let b_count = views.iter().filter(|v| set_b.contains(&v.task_id)).count();
    assert_eq!((a_count, b_count), (5, 5));

    q.set_submitter_weight(sub_a.clone(), 3).await;
    let mut weighted_cursor = crate::fair_share::FairWalkCursor::default();
    let w_views = q.ready_views_fair(&mut weighted_cursor, 8).await;
    assert_eq!(w_views.len(), 8);
    let wa = w_views
        .iter()
        .filter(|v| set_a.contains(&v.task_id))
        .count();
    let wb = w_views
        .iter()
        .filter(|v| set_b.contains(&v.task_id))
        .count();
    assert_eq!((wa, wb), (6, 2));

    // Priority preservation within each submitter's queue.
    let a_high = q
        .submit_from(echo_task("a-high"), 50, sub_a.clone())
        .await
        .unwrap();
    let mut prio_cursor = crate::fair_share::FairWalkCursor::default();
    let p_views = q.ready_views_fair(&mut prio_cursor, 4).await;
    assert!(
        p_views.iter().any(|v| v.task_id == a_high),
        "high-priority task within submitter a must surface immediately"
    );
    let first_a = p_views
        .iter()
        .find(|v| set_a.contains(&v.task_id) || v.task_id == a_high)
        .unwrap();
    assert_eq!(first_a.task_id, a_high);
}

#[tokio::test]
async fn ready_views_fair_propagates_requires_tree_v2() {
    let q = TaskQueue::new();
    let sub_a = SubmitterId::Client(Uuid::new_v4());
    let sub_b = SubmitterId::Client(Uuid::new_v4());

    let id_v1 = q
        .submit_from_with_tree_v2(echo_task("v1-fair"), 0, sub_a, false)
        .await
        .unwrap();
    let id_v2 = q
        .submit_from_with_tree_v2(echo_task("v2-fair"), 0, sub_b, true)
        .await
        .unwrap();

    let mut cursor = crate::fair_share::FairWalkCursor::default();
    let views = q.ready_views_fair(&mut cursor, 10).await;
    assert_eq!(views.len(), 2);

    let v1_view = views.iter().find(|v| v.task_id == id_v1).unwrap();
    let v2_view = views.iter().find(|v| v.task_id == id_v2).unwrap();
    assert!(
        !v1_view
            .required_features
            .contains(ProtocolCapabilities::TREE_V2),
        "v1 fair view must not require TREE_V2"
    );
    assert!(
        v2_view
            .required_features
            .contains(ProtocolCapabilities::TREE_V2),
        "v2 fair view must propagate TREE_V2"
    );
}

/// An extension id this build does not know travels through the queue and the journal unchanged
/// (design 0082 D2.2), and a critical one is refused at submission (D2.3).
#[tokio::test]
async fn test_extensions_survive_the_queue_and_a_journal_replay_and_critical_unknown_is_refused() {
    use rusty_grid_core::task::{Extension, MAX_EXTENSIONS_PER_LIST};
    let dir = tempfile::tempdir().unwrap();
    let mut task = echo_task("with-extensions");
    task.extensions = vec![
        Extension {
            id: 0x1234,
            critical: false,
            bytes: vec![0x00, 0xff, 0x7f],
        },
        // Known (test registry), worker scope.
        Extension {
            id: 0x8002,
            critical: true,
            bytes: vec![9],
        },
    ];
    task.requirements.extensions = vec![Extension {
        id: 0x4321,
        critical: false,
        bytes: vec![],
    }];
    let expected = task.clone();
    let id = task.id;
    {
        let (store, recovered) = TaskStore::open(dir.path()).unwrap();
        let queue = TaskQueue::new();
        queue
            .attach_store_and_replay(store, recovered, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(queue.submit(task).await.unwrap(), id);
        assert_eq!(queue.get_task_spec(&id).await.unwrap(), expected);

        // Unknown critical: refused, nothing queued or stored.
        let mut critical = echo_task("critical-unknown");
        critical.extensions = vec![Extension {
            id: 0x1235,
            critical: true,
            bytes: vec![1],
        }];
        let critical_id = critical.id;
        let err = queue.submit(critical).await.unwrap_err().to_string();
        assert!(err.contains("needs extension 4661"), "{err}");
        assert!(queue.get_task_spec(&critical_id).await.is_none());

        // Over the count limit: refused.
        let mut many = echo_task("too-many");
        many.extensions = (1..=MAX_EXTENSIONS_PER_LIST as u16 + 1)
            .map(|i| Extension {
                id: 0x2000 + i,
                critical: false,
                bytes: vec![],
            })
            .collect();
        assert!(queue.submit(many).await.is_err());
    }

    let (store2, recovered2) = TaskStore::open(dir.path()).unwrap();
    let recovered_queue = TaskQueue::new();
    recovered_queue
        .attach_store_and_replay(store2, recovered2, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(recovered_queue.get_task_spec(&id).await.unwrap(), expected);
}

/// Extension values are charged against the active byte budget with the rest of the task.
#[test]
fn test_extension_bytes_count_into_the_active_byte_charge() {
    let plain = echo_task("budget");
    let mut with = plain.clone();
    with.extensions = vec![rusty_grid_core::task::Extension {
        id: 0x4000,
        critical: false,
        bytes: vec![0; 10_000],
    }];
    assert_eq!(
        crate::queue::task_active_bytes(&with) - crate::queue::task_active_bytes(&plain),
        10_003
    );
}

/// Design 0051 D5, revocation step 3: the tasks of a revoked member's worker are requeued at once,
/// without the session-resume hold, and lose their checkpoints; a task held for that worker by an
/// earlier disconnect is released too, and another worker's task is untouched.
#[tokio::test]
async fn test_handle_worker_revoked_requeues_at_once_and_drops_checkpoints() {
    let queue = TaskQueue::with_config(
        RetryPolicy {
            retry_on_worker_disconnect: true,
            initial_backoff_ms: 60_000,
            ..RetryPolicy::default()
        },
        None,
    );
    queue
        .set_session_resume_hold(Duration::from_secs(3_600))
        .await;
    let revoked = Uuid::new_v4();
    let kept = Uuid::new_v4();
    queue.set_worker_session_resume(revoked, true);
    queue.set_worker_session_resume(kept, true);

    // A task the revoked worker ran on its first session, held after that session dropped.
    let held = queue.submit(echo_task("held")).await.unwrap();
    queue
        .schedule_tasks_batch_in_sessions(&[(held, revoked, 1)])
        .await;
    queue
        .mark_running_from(revoked, Some(1), &held)
        .await
        .unwrap();
    assert!(queue
        .record_checkpoint_from(
            revoked,
            Some(1),
            held,
            CheckpointData::new(1, b"h".to_vec())
        )
        .await
        .unwrap());
    queue
        .handle_session_disconnected(&revoked, Some(1), "drop", false)
        .await;
    assert!(
        queue.schedule_task(&held, kept).await.is_none(),
        "the task is held for the revoked worker before the revocation"
    );

    // A task running on the revoked worker's current session, and one on another worker.
    let running = queue.submit(echo_task("running")).await.unwrap();
    queue
        .schedule_tasks_batch_in_sessions(&[(running, revoked, 2)])
        .await;
    queue
        .mark_running_from(revoked, Some(2), &running)
        .await
        .unwrap();
    assert!(queue
        .record_checkpoint_from(
            revoked,
            Some(2),
            running,
            CheckpointData::new(7, b"r".to_vec())
        )
        .await
        .unwrap());
    let other = queue.submit(echo_task("other")).await.unwrap();
    queue
        .schedule_tasks_batch_in_sessions(&[(other, kept, 5)])
        .await;
    queue
        .mark_running_from(kept, Some(5), &other)
        .await
        .unwrap();
    assert!(queue
        .record_checkpoint_from(kept, Some(5), other, CheckpointData::new(2, b"k".to_vec()))
        .await
        .unwrap());

    let swept = queue
        .handle_workers_revoked(&[revoked], "member revoked")
        .await;
    assert_eq!(swept.len(), 1);
    assert_eq!(swept[0].0, revoked);
    let affected: HashSet<TaskId> = swept[0].1.iter().copied().collect();
    assert_eq!(affected, HashSet::from([held, running]));

    for tid in [held, running] {
        let info = queue.get_task(&tid).await.unwrap();
        assert_eq!(
            info.state,
            TaskState::Queued,
            "requeued at once, not delayed"
        );
        assert_eq!(info.assigned_worker_id, None);
        assert_eq!(info.failover_count, 1, "charged as one failover");
        assert!(
            queue.get_checkpoint(&tid).await.is_none(),
            "the revoked worker's checkpoint is dropped"
        );
    }
    // The revoked worker cannot adopt or checkpoint them any more.
    let outcome = queue
        .adopt_session_tasks(revoked, 3, &[held, running])
        .await;
    assert!(outcome.adopted.is_empty());
    assert!(!queue
        .record_checkpoint_from(
            revoked,
            Some(2),
            running,
            CheckpointData::new(9, b"x".to_vec())
        )
        .await
        .unwrap());
    // No hold: another worker takes them now.
    assert!(queue.schedule_task(&held, kept).await.is_some());
    assert!(queue.schedule_task(&running, kept).await.is_some());

    let other_info = queue.get_task(&other).await.unwrap();
    assert_eq!(other_info.state, TaskState::Running);
    assert_eq!(other_info.assigned_worker_id, Some(kept));
    assert_eq!(
        queue.get_checkpoint(&other).await.map(|cp| cp.sequence),
        Some(2),
        "another worker's checkpoint stays"
    );
}

/// No journal record clears a checkpoint, so `handle_worker_revoked` compacts the store: a master
/// that crashes right after the revocation must not bring the dropped checkpoint back on replay.
#[tokio::test]
async fn test_handle_worker_revoked_dropped_checkpoint_stays_dropped_after_crash() {
    let dir = tempfile::tempdir().unwrap();
    let revoked = Uuid::new_v4();
    let (tid, kept_tid) = {
        let (store, rec) = TaskStore::open(dir.path()).unwrap();
        let q = TaskQueue::with_config(replay_policy(), None);
        q.attach_store_and_replay(store, rec, Duration::ZERO)
            .await
            .unwrap();
        let tid = q.submit(echo_task("revoked-cp")).await.unwrap();
        let kept_tid = q.submit(echo_task("kept-cp")).await.unwrap();
        let kept = Uuid::new_v4();
        q.schedule_tasks_batch_in_sessions(&[(tid, revoked, 1), (kept_tid, kept, 2)])
            .await;
        q.mark_running_from(revoked, Some(1), &tid).await.unwrap();
        q.mark_running_from(kept, Some(2), &kept_tid).await.unwrap();
        q.record_checkpoint_from(revoked, Some(1), tid, CheckpointData::new(4, b"r".to_vec()))
            .await
            .unwrap();
        q.record_checkpoint_from(
            kept,
            Some(2),
            kept_tid,
            CheckpointData::new(5, b"k".to_vec()),
        )
        .await
        .unwrap();
        q.handle_workers_revoked(&[revoked], "member revoked").await;
        assert!(q.get_checkpoint(&tid).await.is_none());
        // A crash: no shutdown compaction.
        drop(q);
        (tid, kept_tid)
    };

    let (store, rec) = TaskStore::open(dir.path()).unwrap();
    let q2 = TaskQueue::with_config(replay_policy(), None);
    q2.attach_store_and_replay(store, rec, Duration::ZERO)
        .await
        .unwrap();
    let info = q2.get_task(&tid).await.unwrap();
    assert_eq!(info.state, TaskState::Queued);
    assert!(
        q2.get_checkpoint(&tid).await.is_none(),
        "the revoked worker's checkpoint must not come back on replay"
    );
    assert_eq!(
        q2.get_checkpoint(&kept_tid).await.map(|cp| cp.sequence),
        Some(5),
        "another worker's checkpoint survives the replay"
    );
}

/// A retrying task whose last failed attempt ran on a revoked worker loses the checkpoint it
/// carries; one whose last failure was on another worker keeps it. Several workers of one member
/// are swept in one call and reported per worker.
#[tokio::test]
async fn test_handle_workers_revoked_drops_checkpoints_of_tasks_last_failed_on_them() {
    let queue = TaskQueue::with_config(
        RetryPolicy {
            retry_on_worker_disconnect: true,
            initial_backoff_ms: 60_000,
            ..RetryPolicy::default()
        },
        None,
    );
    let revoked = Uuid::new_v4();
    let revoked_too = Uuid::new_v4();
    let kept = Uuid::new_v4();

    // Each worker ran a task that checkpointed, then disconnected without a session resume.
    let mut retrying = Vec::new();
    for (session, worker) in [(1u64, revoked), (2u64, kept)] {
        let tid = queue.submit(echo_task("retrying")).await.unwrap();
        queue
            .schedule_tasks_batch_in_sessions(&[(tid, worker, session)])
            .await;
        queue
            .mark_running_from(worker, Some(session), &tid)
            .await
            .unwrap();
        assert!(queue
            .record_checkpoint_from(
                worker,
                Some(session),
                tid,
                CheckpointData::new(3, b"c".to_vec())
            )
            .await
            .unwrap());
        queue
            .handle_session_disconnected(&worker, Some(session), "drop", false)
            .await;
        assert_eq!(
            queue.get_task(&tid).await.unwrap().state,
            TaskState::Retrying
        );
        retrying.push(tid);
    }
    let (last_failed_on_revoked, last_failed_on_kept) = (retrying[0], retrying[1]);

    // The member's second worker runs a task when it is revoked.
    let running = queue.submit(echo_task("running")).await.unwrap();
    queue
        .schedule_tasks_batch_in_sessions(&[(running, revoked_too, 7)])
        .await;
    queue
        .mark_running_from(revoked_too, Some(7), &running)
        .await
        .unwrap();

    let swept = queue
        .handle_workers_revoked(&[revoked, revoked_too], "member revoked")
        .await;
    assert_eq!(
        swept,
        vec![(revoked, Vec::new()), (revoked_too, vec![running])],
        "the retrying task changes no state; the running one is requeued"
    );
    assert!(queue
        .get_checkpoint(&last_failed_on_revoked)
        .await
        .is_none());
    assert_eq!(
        queue.get_task(&last_failed_on_revoked).await.unwrap().state,
        TaskState::Retrying
    );
    assert_eq!(
        queue
            .get_checkpoint(&last_failed_on_kept)
            .await
            .map(|cp| cp.sequence),
        Some(3),
        "a task whose last failure was on another worker keeps its checkpoint"
    );
    assert_eq!(
        queue.get_task(&running).await.unwrap().state,
        TaskState::Queued
    );
}
