//! Integration tests for TEAM-2 git ref task claims, leases, worktrees, and diff checks (design 0111, T2).

use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Barrier};

use chrono::{TimeZone, Utc};

use rusty_grid_cli::plan::lease::{
    check_diff, claim_task_at, lease_refname, release_task, renew_task_at, ClaimOptions,
    ReleaseOptions, RenewOptions,
};

fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_AUTHOR_NAME", "Test")
        .env("GIT_AUTHOR_EMAIL", "test@example.invalid")
        .env("GIT_COMMITTER_NAME", "Test")
        .env("GIT_COMMITTER_EMAIL", "test@example.invalid")
        .output()
        .expect("git must be installed");
    assert!(
        out.status.success(),
        "git {:?} in {} failed: stderr={} stdout={}",
        args,
        dir.display(),
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout),
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

struct BareFixture {
    _tmp: tempfile::TempDir,
    bare: std::path::PathBuf,
    clone_a: std::path::PathBuf,
    clone_b: std::path::PathBuf,
    worktrees_a: std::path::PathBuf,
    worktrees_b: std::path::PathBuf,
}

fn setup_bare_fixture(plan_toml: &str) -> BareFixture {
    let tmp = tempfile::tempdir().expect("tempdir");
    let bare = tmp.path().join("remote.git");
    let seed = tmp.path().join("seed");
    let clone_a = tmp.path().join("clone_a");
    let clone_b = tmp.path().join("clone_b");
    let worktrees_a = tmp.path().join("wt_a");
    let worktrees_b = tmp.path().join("wt_b");

    std::fs::create_dir_all(&bare).unwrap();
    git(&bare, &["init", "--bare", "-b", "master"]);

    std::fs::create_dir_all(seed.join(".oxideswarm")).unwrap();
    git(&seed, &["init", "-b", "master"]);
    std::fs::write(seed.join(".oxideswarm/plan.toml"), plan_toml).unwrap();
    std::fs::create_dir_all(seed.join("crates/cli/src/plan")).unwrap();
    std::fs::write(seed.join("crates/cli/src/plan/a.rs"), "// a\n").unwrap();
    std::fs::write(seed.join("crates/cli/src/b.rs"), "// b\n").unwrap();
    git(&seed, &["add", "."]);
    git(&seed, &["commit", "-m", "initial plan"]);
    let bare_str = bare.to_string_lossy().into_owned();
    git(&seed, &["remote", "add", "origin", &bare_str]);
    git(&seed, &["push", "-u", "origin", "master"]);

    git(tmp.path(), &["clone", &bare_str, clone_a.to_str().unwrap()]);
    git(tmp.path(), &["clone", &bare_str, clone_b.to_str().unwrap()]);

    BareFixture {
        _tmp: tmp,
        bare,
        clone_a,
        clone_b,
        worktrees_a,
        worktrees_b,
    }
}

fn sample_plan() -> &'static str {
    r#"
[project]
base = "master"
contracts = ["docs/reference.md"]

[[task]]
id = "T-DONE"
title = "Prerequisite done task"
status = "done"
owned_paths = ["crates/core/src/done.rs"]

[[task]]
id = "T-1"
title = "Task one with lease module ownership"
status = "open"
depends_on = ["T-DONE"]
owned_paths = ["crates/cli/src/plan/**"]

[[task]]
id = "T-2"
title = "Non-overlapping task two"
status = "open"
owned_paths = ["crates/cli/src/b.rs"]

[[task]]
id = "T-DEP-WAIT"
title = "Blocked by open T-1"
status = "open"
depends_on = ["T-1"]
owned_paths = ["crates/worker/src/lib.rs"]

[[task]]
id = "T-REVIEW"
title = "Already in review"
status = "review"
owned_paths = ["docs/other.md"]

[[task]]
id = "T-OVERLAP"
title = "Overlaps T-1 prefix path"
status = "open"
owned_paths = ["crates/cli/src/plan/lease.rs"]
"#
}

#[test]
#[cfg_attr(miri, ignore = "Miri: spawns a process")]
fn t2_concurrent_claims_exactly_one_wins() {
    let fix = setup_bare_fixture(sample_plan());
    let now = Utc.with_ymd_and_hms(2026, 11, 26, 10, 0, 0).unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let a_repo = fix.clone_a.clone();
    let a_wt = fix.worktrees_a.clone();
    let b_a = Arc::clone(&barrier);
    let t_a = std::thread::spawn(move || {
        let opts = ClaimOptions {
            task_id: "T-1".to_string(),
            holder: "agent-win-2".to_string(),
            person: Some("alice".to_string()),
            remote: "origin".to_string(),
            base: None,
            plan_path: None,
            branch: None,
            worktree_root: Some(a_wt),
            create_worktree: true,
            ttl_minutes: 45,
        };
        b_a.wait();
        claim_task_at(&a_repo, &opts, now)
    });

    let b_repo = fix.clone_b.clone();
    let b_wt = fix.worktrees_b.clone();
    let b_b = Arc::clone(&barrier);
    let t_b = std::thread::spawn(move || {
        let opts = ClaimOptions {
            task_id: "T-1".to_string(),
            holder: "agent-mac-ag".to_string(),
            person: Some("alice".to_string()),
            remote: "origin".to_string(),
            base: None,
            plan_path: None,
            branch: None,
            worktree_root: Some(b_wt),
            create_worktree: true,
            ttl_minutes: 45,
        };
        b_b.wait();
        claim_task_at(&b_repo, &opts, now)
    });

    let res_a = t_a.join().expect("thread a join");
    let res_b = t_b.join().expect("thread b join");

    let (winner, loser_wt) = match (res_a, res_b) {
        (Ok(w), Err(_)) => (w, fix.worktrees_b.join("wt-T-1")),
        (Err(_), Ok(w)) => (w, fix.worktrees_a.join("wt-T-1")),
        (other_a, other_b) => {
            panic!("expected 1 winner and 1 loser, got a={other_a:?} b={other_b:?}")
        }
    };

    assert_eq!(winner.refname, lease_refname("T-1"));
    assert!(winner.worktree_path.as_ref().unwrap().is_dir());
    assert!(!loser_wt.exists(), "loser must not leave a stray worktree");

    let remote_oid = git(
        &fix.bare,
        &["rev-parse", "--verify", "refs/oxideswarm/lease/T-1"],
    );
    assert_eq!(remote_oid, winner.oid);
}

#[test]
#[cfg_attr(miri, ignore = "Miri: spawns a process")]
fn t2_renew_takeover_release_and_old_holder_fails() {
    let fix = setup_bare_fixture(sample_plan());
    let t0 = Utc.with_ymd_and_hms(2026, 11, 26, 10, 0, 0).unwrap();

    // 1. Agent A claims T-1 with a 10-minute lease.
    let claim_a = claim_task_at(
        &fix.clone_a,
        &ClaimOptions {
            task_id: "T-1".to_string(),
            holder: "agent-a".to_string(),
            person: None,
            remote: "origin".to_string(),
            base: None,
            plan_path: None,
            branch: None,
            worktree_root: Some(fix.worktrees_a.clone()),
            create_worktree: true,
            ttl_minutes: 10,
        },
        t0,
    )
    .expect("agent-a claim");

    // 2. Non-holder (agent-b) cannot renew active lease.
    let err_non_holder = renew_task_at(
        &fix.clone_b,
        &RenewOptions {
            task_id: "T-1".to_string(),
            holder: "agent-b".to_string(),
            remote: "origin".to_string(),
            expected_oid: None,
            ttl_minutes: 45,
            blocked: None,
            verify_attempts: None,
            verify_failures: None,
            last_failure: None,
            last_fingerprint: None,
        },
        t0 + chrono::Duration::minutes(2),
    );
    assert!(err_non_holder.is_err());

    // 3. Holder (agent-a) renews cleanly (fast-forward parent link).
    let renew_a = renew_task_at(
        &fix.clone_a,
        &RenewOptions {
            task_id: "T-1".to_string(),
            holder: "agent-a".to_string(),
            remote: "origin".to_string(),
            expected_oid: Some(claim_a.oid.clone()),
            ttl_minutes: 10,
            blocked: None,
            verify_attempts: Some(1),
            verify_failures: Some(0),
            last_failure: None,
            last_fingerprint: None,
        },
        t0 + chrono::Duration::minutes(5),
    )
    .expect("holder fast-forward renew");
    assert_eq!(renew_a.old_oid, claim_a.oid);
    assert_ne!(renew_a.new_oid, claim_a.oid);
    let parent = git(&fix.bare, &["rev-parse", &format!("{}^", renew_a.new_oid)]);
    assert_eq!(parent, claim_a.oid);

    // 4. Before expiry (at t0 + 14m, expires at t0 + 15m), takeover by agent-b is refused.
    let err_early_takeover = claim_task_at(
        &fix.clone_b,
        &ClaimOptions {
            task_id: "T-1".to_string(),
            holder: "agent-b".to_string(),
            person: None,
            remote: "origin".to_string(),
            base: None,
            plan_path: None,
            branch: None,
            worktree_root: Some(fix.worktrees_b.clone()),
            create_worktree: false,
            ttl_minutes: 45,
        },
        t0 + chrono::Duration::minutes(14),
    );
    assert!(err_early_takeover.is_err());

    // 5. After expiry (t0 + 16m), agent-b takes over via --force-with-lease.
    let takeover_b = claim_task_at(
        &fix.clone_b,
        &ClaimOptions {
            task_id: "T-1".to_string(),
            holder: "agent-b".to_string(),
            person: None,
            remote: "origin".to_string(),
            base: None,
            plan_path: None,
            branch: None,
            worktree_root: Some(fix.worktrees_b.clone()),
            create_worktree: true,
            ttl_minutes: 45,
        },
        t0 + chrono::Duration::minutes(16),
    )
    .expect("expired lease takeover");
    assert_eq!(takeover_b.taken_over_from.as_deref(), Some("agent-a"));

    // 6. Old holder's next renew fails (holder mismatch + local ref stale).
    let err_old_renew = renew_task_at(
        &fix.clone_a,
        &RenewOptions {
            task_id: "T-1".to_string(),
            holder: "agent-a".to_string(),
            remote: "origin".to_string(),
            expected_oid: None,
            ttl_minutes: 45,
            blocked: None,
            verify_attempts: None,
            verify_failures: None,
            last_failure: None,
            last_fingerprint: None,
        },
        t0 + chrono::Duration::minutes(17),
    );
    assert!(err_old_renew.is_err());

    // 7. Non-holder cannot release without --force; holder release deletes remote ref.
    let err_bad_release = release_task(
        &fix.clone_a,
        &ReleaseOptions {
            task_id: "T-1".to_string(),
            holder: Some("agent-a".to_string()),
            remote: "origin".to_string(),
            force: false,
        },
    );
    assert!(err_bad_release.is_err());

    let released = release_task(
        &fix.clone_b,
        &ReleaseOptions {
            task_id: "T-1".to_string(),
            holder: Some("agent-b".to_string()),
            remote: "origin".to_string(),
            force: false,
        },
    )
    .expect("holder release deletes remote ref");
    assert_eq!(released.deleted_oid, takeover_b.oid);
    let ls_after = git(&fix.bare, &["for-each-ref", "refs/oxideswarm/lease/T-1"]);
    assert!(ls_after.is_empty(), "remote lease ref should be deleted");
}

#[test]
#[cfg_attr(miri, ignore = "Miri: spawns a process")]
fn t2_claim_preconditions_overlap_and_worktree_rollback() {
    let fix = setup_bare_fixture(sample_plan());
    let now = Utc.with_ymd_and_hms(2026, 11, 26, 11, 0, 0).unwrap();

    // Unmet dependency (T-DEP-WAIT depends on T-1 which is open) -> refused.
    assert!(claim_task_at(
        &fix.clone_a,
        &ClaimOptions {
            task_id: "T-DEP-WAIT".to_string(),
            holder: "agent-a".to_string(),
            person: None,
            remote: "origin".to_string(),
            base: None,
            plan_path: None,
            branch: None,
            worktree_root: None,
            create_worktree: false,
            ttl_minutes: 45,
        },
        now,
    )
    .is_err());

    // Non-open status (T-REVIEW) -> refused.
    assert!(claim_task_at(
        &fix.clone_a,
        &ClaimOptions {
            task_id: "T-REVIEW".to_string(),
            holder: "agent-a".to_string(),
            person: None,
            remote: "origin".to_string(),
            base: None,
            plan_path: None,
            branch: None,
            worktree_root: None,
            create_worktree: false,
            ttl_minutes: 45,
        },
        now,
    )
    .is_err());

    // Worktree failure after push (pre-created branch `team/T-1`) rolls back remote lease ref.
    git(&fix.clone_a, &["branch", "team/T-1"]);
    let err_wt = claim_task_at(
        &fix.clone_a,
        &ClaimOptions {
            task_id: "T-1".to_string(),
            holder: "agent-a".to_string(),
            person: None,
            remote: "origin".to_string(),
            base: None,
            plan_path: None,
            branch: None,
            worktree_root: Some(fix.worktrees_a.clone()),
            create_worktree: true,
            ttl_minutes: 45,
        },
        now,
    );
    assert!(
        err_wt.is_err(),
        "existing branch must fail git worktree add"
    );
    assert!(
        git(&fix.bare, &["for-each-ref", "refs/oxideswarm/lease/T-1"]).is_empty(),
        "remote lease ref must be rolled back when worktree add fails"
    );

    // Real claim of T-1 (`crates/cli/src/plan/**`) blocks T-OVERLAP (`crates/cli/src/plan/lease.rs`).
    claim_task_at(
        &fix.clone_a,
        &ClaimOptions {
            task_id: "T-1".to_string(),
            holder: "agent-a".to_string(),
            person: None,
            remote: "origin".to_string(),
            base: None,
            plan_path: None,
            branch: None,
            worktree_root: None,
            create_worktree: false,
            ttl_minutes: 45,
        },
        now,
    )
    .expect("claim T-1 without worktree");

    let err_overlap = claim_task_at(
        &fix.clone_b,
        &ClaimOptions {
            task_id: "T-OVERLAP".to_string(),
            holder: "agent-b".to_string(),
            person: None,
            remote: "origin".to_string(),
            base: None,
            plan_path: None,
            branch: None,
            worktree_root: None,
            create_worktree: false,
            ttl_minutes: 45,
        },
        now,
    );
    assert!(err_overlap.is_err());
    assert!(err_overlap.unwrap_err().contains("overlaps active lease"));
}

#[test]
#[cfg_attr(miri, ignore = "Miri: spawns a process")]
fn t2_check_diff_enforces_owned_paths_including_spaces() {
    let fix = setup_bare_fixture(sample_plan());
    let now = Utc.with_ymd_and_hms(2026, 11, 26, 12, 0, 0).unwrap();

    let claim = claim_task_at(
        &fix.clone_a,
        &ClaimOptions {
            task_id: "T-1".to_string(),
            holder: "agent-a".to_string(),
            person: None,
            remote: "origin".to_string(),
            base: None,
            plan_path: None,
            branch: None,
            worktree_root: Some(fix.worktrees_a.clone()),
            create_worktree: true,
            ttl_minutes: 45,
        },
        now,
    )
    .expect("claim T-1");
    let wt = claim.worktree_path.unwrap();

    // Commit an owned file with spaces inside `crates/cli/src/plan/**`.
    std::fs::write(wt.join("crates/cli/src/plan/new file.rs"), "// allowed\n").unwrap();
    git(&wt, &["add", "."]);
    git(&wt, &["commit", "-m", "add owned file with space"]);

    // Inferred task id from branch `team/T-1` passes check_diff.
    let ok = check_diff(&wt, "origin/master..HEAD", None, None, None).expect("check_diff ok");
    assert!(ok.ok, "unexpected violations: {:?}", ok.violations);
    assert_eq!(ok.task_id, "T-1");
    assert_eq!(ok.changed_files, vec!["crates/cli/src/plan/new file.rs"]);

    // Commit a path outside `crates/cli/src/plan/**` -> refused.
    std::fs::write(wt.join("crates/cli/src/b.rs"), "// forbidden\n").unwrap();
    git(&wt, &["add", "."]);
    git(&wt, &["commit", "-m", "touch unowned file"]);

    let bad = check_diff(&wt, "origin/master..HEAD", None, None, None).expect("check_diff bad");
    assert!(!bad.ok);
    assert_eq!(bad.violations, vec!["crates/cli/src/b.rs"]);
}
