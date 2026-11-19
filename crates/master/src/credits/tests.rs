use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rusty_grid_core::task::TaskId;
use tempfile::TempDir;
use uuid::Uuid;

use super::{
    CreditsConfig, CreditsLedger, LedgerClock, ModelTunnelUsageInput, TaskUsageInput,
    CREDITS_JOURNAL_FILE, CREDITS_SNAPSHOT_FILE, MAX_CREDIT_DURATION_MS, ORGANISER_PARTICIPANT,
    WINDOW_24H_SECS, WINDOW_7D_SECS,
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
    runner_member: Option<&str>,
    submitter_member: Option<&str>,
    duration_ms: u64,
    cpu_cores: usize,
    gpu_reserved: bool,
) -> TaskUsageInput {
    TaskUsageInput {
        task_id: TaskId::new(),
        worker_id: Uuid::new_v4(),
        duration_ms,
        cpu_cores,
        gpu_reserved,
        exit_code: 0,
        verified: None,
        runner_member_id: runner_member.map(str::to_string),
        submitter_member_id: submitter_member.map(str::to_string),
    }
}

#[test]
fn double_entry_conservation_self_run_and_cross_participant() {
    let clock = FakeClock::new(10_000_000);
    let ledger = CreditsLedger::in_memory_with_clock(CreditsConfig::default(), clock);

    // alice runs 1 task of 1500ms * 2 cores for bob (CPU only).
    ledger
        .record_task(task_input(Some("alice"), Some("bob"), 1500, 2, false))
        .expect("book alice->bob");
    // bob runs 1 GPU-reserved task of 4000ms * 4 cores for organiser (v2).
    ledger
        .record_task(task_input(Some("bob"), None, 4000, 4, true))
        .expect("book bob->organiser");
    // alice runs 1 self-run task of 250ms * 1 core (GPU reserved).
    ledger
        .record_task(task_input(Some("alice"), Some("alice"), 250, 1, true))
        .expect("book alice self-run");

    // Stage 2: verified == Some(false) must be ignored in counters.
    let mut unverified = task_input(Some("alice"), Some("bob"), 9999, 8, true);
    unverified.verified = Some(false);
    ledger
        .record_task(unverified)
        .expect("append unverified record");

    let mut names = HashMap::new();
    names.insert("alice".to_string(), "Alice Lab".to_string());
    names.insert("bob".to_string(), "Bob Cluster".to_string());

    let snap = ledger.snapshot_for_display(&HashMap::new(), &names, true);
    assert!(snap.display_only);
    assert!(!snap.window_truncated);
    assert_eq!(snap.total_records, 4);
    assert!(snap.conservation_holds());

    let by_label: HashMap<&str, &super::ParticipantCreditsDto> = snap
        .participants
        .iter()
        .map(|p| (p.label.as_str(), p))
        .collect();

    let alice = by_label["Alice Lab"];
    assert_eq!(alice.member_ids, vec!["alice".to_string()]);
    assert_eq!(alice.all_time.ran_for_others.tasks, 1);
    assert_eq!(alice.all_time.ran_for_others.core_ms, 3_000);
    assert_eq!(alice.all_time.ran_for_others.gpu_ms, 0);
    assert_eq!(alice.all_time.used_from_others.tasks, 0);
    assert_eq!(alice.all_time.self_run.tasks, 1);
    assert_eq!(alice.all_time.self_run.core_ms, 250);
    assert_eq!(alice.all_time.self_run.gpu_ms, 250);

    let bob = by_label["Bob Cluster"];
    assert_eq!(bob.member_ids, vec!["bob".to_string()]);
    assert_eq!(bob.all_time.ran_for_others.tasks, 1);
    assert_eq!(bob.all_time.ran_for_others.core_ms, 16_000);
    assert_eq!(bob.all_time.ran_for_others.gpu_ms, 4_000);
    assert_eq!(bob.all_time.used_from_others.tasks, 1);
    assert_eq!(bob.all_time.used_from_others.core_ms, 3_000);

    let org = by_label[ORGANISER_PARTICIPANT];
    assert!(org.member_ids.is_empty());
    assert_eq!(org.all_time.ran_for_others.tasks, 0);
    assert_eq!(org.all_time.used_from_others.tasks, 1);
    assert_eq!(org.all_time.used_from_others.core_ms, 16_000);
    assert_eq!(org.all_time.used_from_others.gpu_ms, 4_000);
}

#[test]
fn member_id_keying_and_display_time_label_grouping_and_observer_redaction() {
    let dir = TempDir::new().expect("tempdir");
    // Compact aggressively so both records are folded into `archived_pairs`.
    let ledger = CreditsLedger::open(
        dir.path(),
        CreditsConfig {
            max_records: 1,
            max_bytes: 1_048_576,
        },
    )
    .expect("open ledger");

    let m_a1 = "aaaa1111aaaa1111";
    let m_a2 = "aaaa2222aaaa2222";
    let m_b1 = "bbbb1111bbbb1111";

    // Archived cross-machine task between a1 and a2 (1000ms), plus a1 running for b1 (2000ms).
    ledger
        .record_task(task_input(Some(m_a1), Some(m_a2), 1000, 1, false))
        .expect("a1->a2");
    ledger
        .record_task(task_input(Some(m_a1), Some(m_b1), 2000, 1, false))
        .expect("a1->b1");
    ledger
        .record_task(task_input(Some(m_b1), Some(m_a2), 500, 1, false))
        .expect("b1->a2");

    let mut names = HashMap::new();
    names.insert(m_a1.to_string(), "alice-laptop".to_string());
    names.insert(m_a2.to_string(), "alice-desktop".to_string());
    names.insert(m_b1.to_string(), "bob-box".to_string());

    // Observer view before labels: must NEVER expose raw member ids or machine names.
    let obs_unlabeled = ledger.snapshot_for_display(&HashMap::new(), &names, false);
    assert!(obs_unlabeled.conservation_holds());
    assert_eq!(obs_unlabeled.participants.len(), 3);
    for p in &obs_unlabeled.participants {
        assert!(p.member_ids.is_empty());
        assert!(p.label.starts_with("participant-"));
        assert!(!p.label.contains(m_a1));
        assert!(!p.label.contains("alice-laptop"));
        assert!(!p.label.contains("bob-box"));
    }

    // Later, admin assigns both of Alice's machines the label "alice" and Bob's "bob".
    let mut labels = HashMap::new();
    labels.insert(m_a1.to_string(), "alice".to_string());
    labels.insert(m_a2.to_string(), "alice".to_string());
    labels.insert(m_b1.to_string(), "bob".to_string());

    let obs_labeled = ledger.snapshot_for_display(&labels, &names, false);
    assert!(obs_labeled.conservation_holds());
    let obs_map: HashMap<&str, _> = obs_labeled
        .participants
        .iter()
        .map(|p| (p.label.as_str(), p))
        .collect();
    assert_eq!(obs_map.len(), 2);
    // Cross-machine work between a1 and a2 automatically classifies as alice's self_run!
    assert_eq!(obs_map["alice"].all_time.self_run.core_ms, 1000);
    assert_eq!(obs_map["alice"].all_time.ran_for_others.core_ms, 2000);
    assert_eq!(obs_map["alice"].all_time.used_from_others.core_ms, 500);
    assert!(obs_map["alice"].member_ids.is_empty());

    // Admin sees same merged totals plus sorted member_ids.
    let adm_labeled = ledger.snapshot_for_display(&labels, &names, true);
    assert!(adm_labeled.conservation_holds());
    let adm_alice = adm_labeled
        .participants
        .iter()
        .find(|p| p.label == "alice")
        .unwrap();
    assert_eq!(
        adm_alice.member_ids,
        vec![m_a1.to_string(), m_a2.to_string()]
    );
    assert_eq!(adm_alice.all_time, obs_map["alice"].all_time);

    // Renaming Alice's label later preserves full archived history with zero split.
    labels.insert(m_a1.to_string(), "team-alice".to_string());
    labels.insert(m_a2.to_string(), "team-alice".to_string());
    let renamed = ledger.snapshot_for_display(&labels, &names, true);
    let team_alice = renamed
        .participants
        .iter()
        .find(|p| p.label == "team-alice")
        .unwrap();
    assert_eq!(team_alice.all_time.self_run.core_ms, 1000);
    assert_eq!(team_alice.all_time.ran_for_others.core_ms, 2000);
    assert_eq!(team_alice.all_time.used_from_others.core_ms, 500);
}

#[test]
fn duration_clamping_and_u128_preserve_conservation_under_extreme_values() {
    let ledger = CreditsLedger::in_memory(CreditsConfig::default());

    ledger
        .record_task(task_input(
            Some("alice"),
            Some("bob"),
            u64::MAX,
            999_999,
            false,
        ))
        .expect("extreme CPU task");
    ledger
        .record_model_tunnel(ModelTunnelUsageInput {
            tunnel_id: Uuid::new_v4(),
            service: "big".into(),
            worker_id: Uuid::new_v4(),
            duration_ms: u64::MAX,
            bytes_to_worker: u64::MAX,
            bytes_from_worker: u64::MAX,
            close_reason: "eof".into(),
            runner_member_id: Some("bob".into()),
            submitter_member_id: Some("alice".into()),
        })
        .expect("extreme tunnel");

    let snap = ledger.snapshot();
    assert!(snap.conservation_holds());
    let alice = snap
        .participants
        .iter()
        .find(|p| p.label == "alice")
        .unwrap();
    assert_eq!(
        alice.all_time.ran_for_others.core_ms,
        u128::from(MAX_CREDIT_DURATION_MS) * 4096
    );
    assert_eq!(alice.all_time.ran_for_others.gpu_ms, 0);
    assert_eq!(
        alice.all_time.used_from_others.tunnel_bytes,
        u128::from(u64::MAX) * 2
    );
}

#[test]
fn time_windows_and_model_tunnel_shared_envelope() {
    let clock = FakeClock::new(1_000_000);
    let ledger = CreditsLedger::in_memory_with_clock(CreditsConfig::default(), clock.clone());

    ledger
        .record_task(task_input(Some("alice"), Some("bob"), 1000, 2, false))
        .expect("old task");

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
            runner_member_id: Some("bob".to_string()),
            submitter_member_id: Some("alice".to_string()),
        })
        .expect("tunnel");

    clock.advance(2 * WINDOW_24H_SECS);
    ledger
        .record_task(task_input(Some("alice"), Some("bob"), 500, 1, false))
        .expect("recent task");

    let snap = ledger.snapshot();
    assert!(snap.conservation_holds());
    let alice = snap
        .participants
        .iter()
        .find(|p| p.label == "alice")
        .expect("alice");

    assert_eq!(alice.last_24h.ran_for_others.tasks, 1);
    assert_eq!(alice.last_24h.ran_for_others.core_ms, 500);
    assert_eq!(alice.last_24h.used_from_others.tunnel_sessions, 0);

    assert_eq!(alice.last_7d.ran_for_others.tasks, 1);
    assert_eq!(alice.last_7d.used_from_others.tunnel_sessions, 1);
    assert_eq!(alice.last_7d.used_from_others.tunnel_duration_ms, 5000);
    assert_eq!(alice.last_7d.used_from_others.tunnel_bytes, 9216);

    assert_eq!(alice.all_time.ran_for_others.tasks, 2);
    assert_eq!(alice.all_time.ran_for_others.core_ms, 2500);
    assert_eq!(alice.all_time.used_from_others.tunnel_sessions, 1);
}

#[test]
fn durable_state_dir_persistence_and_low_water_compaction() {
    let dir = TempDir::new().expect("tempdir");
    let clock = FakeClock::new(2_000_000);

    {
        let ledger = CreditsLedger::open_with_clock(
            dir.path(),
            CreditsConfig {
                max_records: 20,
                max_bytes: 4 * 1024 * 1024,
            },
            clock.clone(),
        )
        .expect("open ledger");

        for i in 0..120u64 {
            let inp = if i % 2 == 0 {
                task_input(Some("alice"), Some("bob"), 100, 2, false)
            } else {
                task_input(Some("bob"), Some("alice"), 100, 1, true)
            };
            ledger.record_task(inp).expect("record");
        }

        let compactions = ledger.compaction_count();
        assert!(
            (5..=15).contains(&compactions),
            "expected ~10 low-water compactions for 120 records at max_records=20, got {compactions}"
        );

        let snap = ledger.snapshot();
        assert!(
            snap.retained_records <= 20,
            "retained {} > 20",
            snap.retained_records
        );
        assert_eq!(snap.total_records, 120);
        assert!(snap.window_truncated, "evicted records fall inside 7d");
        assert!(snap.conservation_holds());
    }

    let snap_raw = fs::read_to_string(dir.path().join(CREDITS_SNAPSHOT_FILE)).expect("read snap");
    assert!(
        !snap_raw.contains('\n'),
        "snapshot must use compact JSON without pretty-print newlines"
    );
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

    let reopened = CreditsLedger::open_with_clock(
        dir.path(),
        CreditsConfig {
            max_records: 20,
            max_bytes: 4 * 1024 * 1024,
        },
        clock.clone(),
    )
    .expect("reopen ledger");

    let snap2 = reopened.snapshot();
    assert_eq!(snap2.total_records, 120);
    assert!(snap2.conservation_holds());
    let by_label: HashMap<&str, &super::ParticipantCreditsDto> = snap2
        .participants
        .iter()
        .map(|p| (p.label.as_str(), p))
        .collect();
    assert_eq!(by_label["alice"].all_time.ran_for_others.core_ms, 12_000);
    assert_eq!(by_label["bob"].all_time.used_from_others.core_ms, 12_000);
    assert_eq!(by_label["bob"].all_time.ran_for_others.core_ms, 6_000);
    assert_eq!(by_label["bob"].all_time.ran_for_others.gpu_ms, 6_000);
    assert_eq!(by_label["alice"].all_time.used_from_others.gpu_ms, 6_000);

    clock.set(2_000_000 + WINDOW_7D_SECS + 1);
    let aged = reopened.snapshot();
    assert!(!aged.window_truncated);
    assert!(aged.conservation_holds());
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
            .record_task(task_input(Some("alice"), Some("bob"), 100, 1, false))
            .expect("record");
    }

    let snap = ledger.snapshot();
    assert_eq!(snap.total_records, 25);
    assert!(snap.archived_records > 0);
    assert!(snap.retained_records < 25);
    assert!(snap.conservation_holds());

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
fn failed_journal_append_truncates_cleanly_and_recovers() {
    let dir = TempDir::new().expect("tempdir");
    let jrn_path = dir.path().join(CREDITS_JOURNAL_FILE);
    {
        let ledger =
            CreditsLedger::open(dir.path(), CreditsConfig::default()).expect("open ledger");
        ledger
            .record_task(task_input(Some("alice"), Some("bob"), 400, 1, false))
            .expect("rec 1");
        let len_after_one = fs::metadata(&jrn_path).expect("stat").len();

        ledger.inject_next_append_failure();
        assert!(
            ledger
                .record_task(task_input(Some("alice"), Some("bob"), 500, 1, false))
                .is_err(),
            "simulated write failure should return error on flush"
        );
        assert_eq!(
            fs::metadata(&jrn_path).expect("stat after rollback").len(),
            len_after_one,
            "partial write must be truncated back to pre-append byte offset"
        );

        ledger
            .record_task(task_input(Some("bob"), Some("alice"), 600, 1, false))
            .expect("post-recovery append must succeed");
    }

    let reopened = CreditsLedger::open(dir.path(), CreditsConfig::default()).expect("reopen");
    let snap = reopened.snapshot();
    assert_eq!(snap.total_records, 2);
    assert!(snap.conservation_holds());
}

#[test]
fn torn_trailing_line_truncates_and_mid_corruption_quarantines_cleanly() {
    let dir = TempDir::new().expect("tempdir");
    {
        let ledger =
            CreditsLedger::open(dir.path(), CreditsConfig::default()).expect("open ledger");
        ledger
            .record_task(task_input(Some("alice"), Some("bob"), 500, 2, false))
            .expect("rec 1");
        ledger
            .record_task(task_input(Some("bob"), Some("alice"), 700, 1, false))
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

    // Corrupt a middle line while keeping a valid final line -> must quarantine & start empty.
    let existing = fs::read_to_string(&jrn_path).expect("read jrn");
    let lines: Vec<&str> = existing.lines().collect();
    assert_eq!(lines.len(), 2);
    let flipped = lines[0].replacen("\"alice\"", "\"mallory\"", 1);
    assert_ne!(flipped, lines[0]);
    fs::write(&jrn_path, format!("{flipped}\n{}\n", lines[1])).expect("write corrupt");

    let quarantined = CreditsLedger::open(dir.path(), CreditsConfig::default())
        .expect("mid-journal corruption must quarantine rather than refusing master startup");
    assert_eq!(quarantined.snapshot().total_records, 0);
    drop(quarantined);

    let corrupt_files: Vec<String> = fs::read_dir(dir.path())
        .expect("readdir")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains(".corrupt."))
        .collect();
    assert!(
        !corrupt_files.is_empty(),
        "corrupt journal must be quarantined"
    );

    // Corrupt snapshot must also quarantine cleanly.
    fs::write(dir.path().join(CREDITS_SNAPSHOT_FILE), b"{not-valid-json").expect("corrupt snap");
    let after_snap_corrupt = CreditsLedger::open(dir.path(), CreditsConfig::default())
        .expect("corrupt snapshot must quarantine cleanly");
    assert_eq!(after_snap_corrupt.snapshot().total_records, 0);
}

#[test]
fn windows_open_truncate_compaction_and_tail_recovery() {
    // Runs on all platforms including `windows-latest`: exercises read+write opening (without
    // append-only flag), compaction `set_len(0)`, torn tail `set_len(valid_len)`, and reopen.
    let dir = TempDir::new().expect("tempdir");
    {
        let ledger = CreditsLedger::open(
            dir.path(),
            CreditsConfig {
                max_records: 2,
                max_bytes: 1_048_576,
            },
        )
        .expect("open ledger");
        for _ in 0..4 {
            ledger
                .record_task(task_input(Some("alice"), Some("bob"), 250, 1, false))
                .expect("record triggers set_len(0) compaction");
        }
    }

    let jrn_path = dir.path().join(CREDITS_JOURNAL_FILE);
    {
        let mut f = OpenOptions::new()
            .append(true)
            .open(&jrn_path)
            .expect("open");
        f.write_all(b"{\"seq\":99").expect("append torn tail");
    }

    let reopened = CreditsLedger::open(
        dir.path(),
        CreditsConfig {
            max_records: 2,
            max_bytes: 1_048_576,
        },
    )
    .expect("reopen truncates tail via set_len(valid_len)");
    reopened
        .record_task(task_input(Some("bob"), Some("alice"), 300, 2, false))
        .expect("append after tail truncation");
    let snap = reopened.snapshot();
    assert_eq!(snap.total_records, 5);
    assert!(snap.conservation_holds());

    let seen_labels: HashSet<&str> = snap.participants.iter().map(|p| p.label.as_str()).collect();
    assert!(seen_labels.contains("alice") && seen_labels.contains("bob"));
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
