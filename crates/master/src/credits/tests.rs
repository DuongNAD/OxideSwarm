use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rusty_grid_core::task::TaskId;
use tempfile::TempDir;
use uuid::Uuid;

use super::{
    CreditsConfig, CreditsLedger, LedgerClock, ModelTunnelUsageInput, TaskUsageInput,
    CREDITS_JOURNAL_FILE, CREDITS_SNAPSHOT_FILE, ORGANISER_PARTICIPANT, WINDOW_24H_SECS,
    WINDOW_7D_SECS,
};

struct FakeClock {
    now: AtomicU64,
}

impl FakeClock {
    fn new(now: u64) -> Arc<Self> {
        Arc::new(Self {
            now: AtomicU64::new(now),
        })
    }

    fn set(&self, now: u64) {
        self.now.store(now, Ordering::SeqCst);
    }

    fn advance(&self, secs: u64) {
        self.now.fetch_add(secs, Ordering::SeqCst);
    }
}

impl LedgerClock for FakeClock {
    fn now_utc(&self) -> u64 {
        self.now.load(Ordering::SeqCst)
    }
}

fn task_input(
    credited_member: Option<&str>,
    debited_member: Option<&str>,
    duration_ms: u64,
    cpu_cores: usize,
    gpu_reserved: bool,
    is_gpu_executed: bool,
) -> TaskUsageInput {
    TaskUsageInput {
        task_id: TaskId::new(),
        worker_id: Uuid::new_v4(),
        duration_ms,
        cpu_cores,
        gpu_reserved,
        is_gpu_executed,
        exit_code: 0,
        verified: None,
        credited_member_id: credited_member.map(str::to_string),
        debited_member_id: debited_member.map(str::to_string),
        credited_label: None,
        debited_label: None,
    }
}

fn assert_zero_sum(snap: &super::CreditsSnapshot) {
    for sum in [&snap.net_sum_24h, &snap.net_sum_7d, &snap.net_sum_all_time] {
        assert_eq!(sum.tasks, 0, "tasks net sum must be 0");
        assert_eq!(sum.core_ms, 0, "core_ms net sum must be 0");
        assert_eq!(sum.gpu_ms, 0, "gpu_ms net sum must be 0");
        assert_eq!(sum.tunnel_sessions, 0, "tunnel_sessions net sum must be 0");
        assert_eq!(
            sum.tunnel_duration_ms, 0,
            "tunnel_duration_ms net sum must be 0"
        );
        assert_eq!(sum.tunnel_bytes, 0, "tunnel_bytes net sum must be 0");
    }
}

#[test]
fn double_entry_conservation_self_run_and_cross_participant() {
    let clock = FakeClock::new(10_000_000);
    let ledger = CreditsLedger::in_memory_with_clock(CreditsConfig::default(), clock);

    // alice runs 1 task of 1500ms * 2 cores for bob (CPU only).
    ledger
        .record_task(task_input(
            Some("alice"),
            Some("bob"),
            1500,
            2,
            false,
            false,
        ))
        .expect("book alice->bob");
    // bob runs 1 GPU-reserved task of 4000ms * 4 cores for organiser (v2).
    ledger
        .record_task(task_input(Some("bob"), None, 4000, 4, true, false))
        .expect("book bob->organiser");
    // alice runs 1 self-run task of 250ms * 1 core for alice (GPU executed).
    ledger
        .record_task(task_input(
            Some("alice"),
            Some("alice"),
            250,
            1,
            false,
            true,
        ))
        .expect("book alice self-run");

    // Stage 2: verified == Some(false) must be ignored in counters.
    let mut unverified = task_input(Some("alice"), Some("bob"), 9999, 8, true, true);
    unverified.verified = Some(false);
    ledger
        .record_task(unverified)
        .expect("append unverified record");

    let mut names = HashMap::new();
    names.insert("alice".to_string(), "Alice Lab".to_string());
    names.insert("bob".to_string(), "Bob Cluster".to_string());

    let snap = ledger.snapshot_with_display_names(&names);
    assert!(snap.display_only);
    assert!(!snap.window_truncated);
    assert_eq!(snap.total_records, 4);
    assert_zero_sum(&snap);

    let by_key: HashMap<&str, &super::ParticipantCreditsDto> = snap
        .participants
        .iter()
        .map(|p| (p.participant.as_str(), p))
        .collect();

    let alice = by_key["alice"];
    assert_eq!(alice.display_name.as_deref(), Some("Alice Lab"));
    // Contributed: 1500*2 + 250*1 = 3250 core_ms; 250 gpu_ms; 2 tasks.
    assert_eq!(alice.all_time.contributed.tasks, 2);
    assert_eq!(alice.all_time.contributed.core_ms, 3250);
    assert_eq!(alice.all_time.contributed.gpu_ms, 250);
    // Used (self-run counted once): 250 core_ms; 250 gpu_ms; 1 task.
    assert_eq!(alice.all_time.used.tasks, 1);
    assert_eq!(alice.all_time.used.core_ms, 250);
    assert_eq!(alice.all_time.used.gpu_ms, 250);
    assert_eq!(alice.all_time.net.core_ms, 3000);
    assert_eq!(alice.all_time.net.gpu_ms, 0);

    let bob = by_key["bob"];
    assert_eq!(bob.all_time.contributed.tasks, 1);
    assert_eq!(bob.all_time.contributed.core_ms, 16_000);
    assert_eq!(bob.all_time.contributed.gpu_ms, 4_000);
    assert_eq!(bob.all_time.used.tasks, 1);
    assert_eq!(bob.all_time.used.core_ms, 3_000);
    assert_eq!(bob.all_time.used.gpu_ms, 0);

    let org = by_key[ORGANISER_PARTICIPANT];
    assert_eq!(org.display_name, None);
    assert_eq!(org.all_time.contributed.tasks, 0);
    assert_eq!(org.all_time.used.tasks, 1);
    assert_eq!(org.all_time.used.core_ms, 16_000);
    assert_eq!(org.all_time.used.gpu_ms, 4_000);
}

#[test]
fn time_windows_and_model_tunnel_shared_envelope() {
    let clock = FakeClock::new(1_000_000);
    let ledger = CreditsLedger::in_memory_with_clock(CreditsConfig::default(), clock.clone());

    // Event 1 at t = 1_000_000 (later 8 days old -> all_time only).
    ledger
        .record_task(task_input(
            Some("alice"),
            Some("bob"),
            1000,
            2,
            false,
            false,
        ))
        .expect("old task");

    // Advance 6 days -> t = 1_000_000 + 6d. Record model tunnel usage.
    clock.advance(6 * WINDOW_24H_SECS);
    ledger
        .record_model_tunnel(ModelTunnelUsageInput {
            tunnel_id: Uuid::new_v4(),
            service: "llama-70b".to_string(),
            worker_id: Uuid::new_v4(),
            duration_ms: 5000,
            bytes_to_worker: 1024,
            bytes_from_worker: 8192,
            close_reason: "client_eof".to_string(),
            credited_member_id: Some("bob".to_string()),
            debited_member_id: Some("alice".to_string()),
            credited_label: None,
            debited_label: None,
        })
        .expect("tunnel");

    // Advance 2 more days -> t = 1_000_000 + 8d. Event 1 is now outside 7d, tunnel is 2d old (in 7d, outside 24h).
    clock.advance(2 * WINDOW_24H_SECS);
    ledger
        .record_task(task_input(Some("alice"), Some("bob"), 500, 1, false, false))
        .expect("recent task");

    let snap = ledger.snapshot();
    assert_zero_sum(&snap);
    let alice = snap
        .participants
        .iter()
        .find(|p| p.participant == "alice")
        .expect("alice");

    assert_eq!(alice.last_24h.contributed.tasks, 1);
    assert_eq!(alice.last_24h.contributed.core_ms, 500);
    assert_eq!(alice.last_24h.used.tunnel_sessions, 0);

    assert_eq!(alice.last_7d.contributed.tasks, 1);
    assert_eq!(alice.last_7d.used.tunnel_sessions, 1);
    assert_eq!(alice.last_7d.used.tunnel_duration_ms, 5000);
    assert_eq!(alice.last_7d.used.tunnel_bytes, 9216);

    assert_eq!(alice.all_time.contributed.tasks, 2);
    assert_eq!(alice.all_time.contributed.core_ms, 2500);
    assert_eq!(alice.all_time.used.tunnel_sessions, 1);
}

#[test]
fn durable_state_dir_persistence_and_rollover_compaction() {
    let dir = TempDir::new().expect("tempdir");
    let clock = FakeClock::new(2_000_000);

    {
        let ledger = CreditsLedger::open_with_clock(
            dir.path(),
            CreditsConfig {
                max_records: 3,
                max_bytes: 1_048_576,
            },
            clock.clone(),
        )
        .expect("open ledger");

        for i in 0..7u64 {
            let mut inp = if i % 2 == 0 {
                task_input(Some("alice"), Some("bob"), 100 * (i + 1), 2, false, false)
            } else {
                task_input(Some("bob"), Some("alice"), 100 * (i + 1), 1, true, false)
            };
            inp.credited_label = Some(if i % 2 == 0 { "team-a" } else { "team-b" }.to_string());
            inp.debited_label = Some(if i % 2 == 0 { "team-b" } else { "team-a" }.to_string());
            ledger.record_task(inp).expect("record");
        }

        let snap = ledger.snapshot();
        assert!(
            snap.retained_records <= 3,
            "retained {} > 3",
            snap.retained_records
        );
        assert!(snap.archived_records >= 4);
        assert_eq!(snap.total_records, 7);
        assert!(snap.window_truncated, "evicted records fall inside 7d");
        assert_zero_sum(&snap);
    }

    assert!(dir.path().join(CREDITS_SNAPSHOT_FILE).is_file());
    assert!(dir.path().join(CREDITS_JOURNAL_FILE).is_file());

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode_snap = fs::metadata(dir.path().join(CREDITS_SNAPSHOT_FILE))
            .expect("stat snap")
            .permissions()
            .mode()
            & 0o777;
        let mode_jrn = fs::metadata(dir.path().join(CREDITS_JOURNAL_FILE))
            .expect("stat jrn")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode_snap & 0o077, 0, "snapshot must not grant group/other");
        assert_eq!(mode_jrn & 0o077, 0, "journal must not grant group/other");
    }

    // Reopen and verify exact conservation & totals survive restart.
    let reopened = CreditsLedger::open_with_clock(
        dir.path(),
        CreditsConfig {
            max_records: 3,
            max_bytes: 1_048_576,
        },
        clock.clone(),
    )
    .expect("reopen ledger");

    let snap2 = reopened.snapshot();
    assert_eq!(snap2.total_records, 7);
    assert_zero_sum(&snap2);
    // Expected team-a contributed: even i in 0..=6 -> i=0,2,4,6: (100+300+500+700)*2 = 3200 core_ms
    // Expected team-b contributed: odd i in 0..=6 -> i=1,3,5: (200+400+600)*1 = 1200 core_ms, 1200 gpu_ms
    let by_p: HashMap<&str, &super::ParticipantCreditsDto> = snap2
        .participants
        .iter()
        .map(|p| (p.participant.as_str(), p))
        .collect();
    assert_eq!(by_p["team-a"].all_time.contributed.core_ms, 3200);
    assert_eq!(by_p["team-b"].all_time.used.core_ms, 3200);
    assert_eq!(by_p["team-b"].all_time.contributed.core_ms, 1200);
    assert_eq!(by_p["team-b"].all_time.contributed.gpu_ms, 1200);
    assert_eq!(by_p["team-a"].all_time.used.gpu_ms, 1200);

    // Move clock past 7d + 1s with no new evictions: window_truncated clears once all evicted records age past 7d.
    clock.set(2_000_000 + WINDOW_7D_SECS + 1);
    let aged = reopened.snapshot();
    assert!(!aged.window_truncated);
    assert_zero_sum(&aged);
}

#[test]
fn byte_budget_compaction_bounds_file_sizes() {
    let dir = TempDir::new().expect("tempdir");
    let ledger = CreditsLedger::open(
        dir.path(),
        CreditsConfig {
            max_records: 10_000,
            max_bytes: 2_500,
        },
    )
    .expect("open ledger");

    for _ in 0..25 {
        ledger
            .record_task(task_input(Some("alice"), Some("bob"), 100, 1, false, false))
            .expect("record");
    }

    let snap = ledger.snapshot();
    assert_eq!(snap.total_records, 25);
    assert!(
        snap.archived_records > 0,
        "byte limit should have triggered archival compaction"
    );
    assert!(
        snap.retained_records < 25,
        "retained records should shrink under byte limit"
    );
    assert_zero_sum(&snap);

    let total_on_disk = fs::metadata(dir.path().join(CREDITS_SNAPSHOT_FILE))
        .expect("snap meta")
        .len()
        + fs::metadata(dir.path().join(CREDITS_JOURNAL_FILE))
            .expect("jrn meta")
            .len();
    assert!(
        total_on_disk <= 2_500,
        "total credits files on disk ({total_on_disk} B) should stay within max_bytes (2500)"
    );
}

#[test]
fn torn_trailing_journal_line_is_truncated_while_mid_file_corruption_fails_closed() {
    let dir = TempDir::new().expect("tempdir");
    {
        let ledger =
            CreditsLedger::open(dir.path(), CreditsConfig::default()).expect("open ledger");
        ledger
            .record_task(task_input(Some("alice"), Some("bob"), 500, 2, false, false))
            .expect("rec 1");
        ledger
            .record_task(task_input(Some("bob"), Some("alice"), 700, 1, false, false))
            .expect("rec 2");
    }

    let jrn_path = dir.path().join(CREDITS_JOURNAL_FILE);
    {
        let mut f = OpenOptions::new()
            .append(true)
            .open(&jrn_path)
            .expect("open");
        f.write_all(b"{\"seq\":3,\"blake3\":\"partial")
            .expect("write torn tail");
    }

    let recovered =
        CreditsLedger::open(dir.path(), CreditsConfig::default()).expect("recover torn tail");
    assert_eq!(recovered.snapshot().total_records, 2);
    drop(recovered);

    // Corrupt a middle line while keeping a valid final line -> must fail closed.
    let existing = fs::read_to_string(&jrn_path).expect("read jrn");
    let lines: Vec<&str> = existing.lines().collect();
    assert_eq!(lines.len(), 2);
    let corrupted_line = lines[0].replacen("\"tasks\"", "\"tampered\"", 1);
    let flipped = if corrupted_line == lines[0] {
        lines[0].replacen("\"alice\"", "\"mallory\"", 1)
    } else {
        corrupted_line
    };
    assert_ne!(flipped, lines[0]);
    fs::write(&jrn_path, format!("{flipped}\n{}\n", lines[1])).expect("write corrupt");

    let err = match CreditsLedger::open(dir.path(), CreditsConfig::default()) {
        Ok(_) => panic!("mid-journal corruption must fail closed"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("Corrupt line") || err.to_string().contains("BLAKE3"));
}

#[cfg(unix)]
#[test]
fn refuses_symlinks_on_credits_files() {
    let dir = TempDir::new().expect("tempdir");
    let target = dir.path().join("outside.txt");
    fs::write(&target, b"decoy").expect("write target");
    std::os::unix::fs::symlink(&target, dir.path().join(CREDITS_JOURNAL_FILE))
        .expect("create symlink");

    let err = match CreditsLedger::open(dir.path(), CreditsConfig::default()) {
        Ok(_) => panic!("symlink journal must be refused"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("symlink"));
}
