//! Integration tests for TEAM-5 merge flow (`oxideswarm plan pr-body`, `--require-fresh`,
//! multi-record pretty JSON streaming, and `oxideswarm plan done` against a local bare repo).

use std::path::Path;
use std::process::Command;

use tempfile::TempDir;

use rusty_grid_cli::plan::build_snapshot;
use rusty_grid_cli::plan::lease::{
    claim_task, lookup_task_lease, ClaimOptions, DEFAULT_LEASE_TTL_MINS,
};
use rusty_grid_cli::pr_body::{
    build_plan_pr_body, mark_task_done, parse_verify_records, DoneOptions, PlanPrBodyOptions,
};

fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap_or_else(|e| panic!("git {:?} spawn failed: {e}", args));
    assert!(
        out.status.success(),
        "git {:?} in {} failed:\nstdout: {}\nstderr: {}",
        args,
        dir.display(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

#[test]
fn t4_team5_pr_body_staleness_and_plan_done_unblocks_dependents() {
    let root = TempDir::new().expect("tempdir");
    let bare = root.path().join("bare.git");
    let clone_a = root.path().join("clone_a");
    let clone_other = root.path().join("clone_other");
    let wts_root = root.path().join("wts");
    std::fs::create_dir_all(&wts_root).unwrap();

    git(
        root.path(),
        &[
            "init",
            "--bare",
            "--initial-branch=main",
            bare.to_str().unwrap(),
        ],
    );
    git(
        root.path(),
        &["clone", bare.to_str().unwrap(), clone_a.to_str().unwrap()],
    );
    git(&clone_a, &["config", "user.name", "Agent A"]);
    git(
        &clone_a,
        &["config", "user.email", "agent-a@oxideswarm.local"],
    );

    std::fs::create_dir_all(clone_a.join(".oxideswarm").join("verify")).unwrap();
    std::fs::create_dir_all(clone_a.join("alpha")).unwrap();
    std::fs::create_dir_all(clone_a.join("beta")).unwrap();
    std::fs::write(clone_a.join("alpha/seed.txt"), "seed\n").unwrap();
    std::fs::write(clone_a.join("beta/seed.txt"), "seed\n").unwrap();

    let plan_toml = r#"# Team wave plan (comments must survive plan done)
[project]
base = "main"

[profile.unit]
command = ["cargo", "test"]

[[task]]
id = "T-1"
title = "Alpha feature"
status = "open" # inline comment
owned_paths = ["alpha/**"]
verify = ["unit"]
person = "alice"

[[task]]
id = "T-2"
title = "Beta dependent"
status = "open"
depends_on = ["T-1"]
owned_paths = ["beta/**"]
verify = ["unit"]
"#;
    std::fs::write(clone_a.join(".oxideswarm/plan.toml"), plan_toml).unwrap();
    git(&clone_a, &["checkout", "-B", "main"]);
    git(&clone_a, &["add", "."]);
    git(&clone_a, &["commit", "-m", "initial plan"]);
    git(&clone_a, &["push", "-u", "origin", "main"]);

    // Second clone used later to advance `main` on bare.git
    git(
        root.path(),
        &[
            "clone",
            bare.to_str().unwrap(),
            clone_other.to_str().unwrap(),
        ],
    );
    git(&clone_other, &["config", "user.name", "Maintainer"]);
    git(
        &clone_other,
        &["config", "user.email", "maintainer@oxideswarm.local"],
    );

    // Claim T-1 cleanly and commit an alpha change + TEAM-3 style pretty-printed verify record.
    let claim = claim_task(
        &clone_a,
        &ClaimOptions {
            task_id: "T-1".to_string(),
            holder: "agent-a".to_string(),
            person: Some("alice".to_string()),
            remote: "origin".to_string(),
            base: None,
            plan_path: None,
            branch: None,
            worktree_root: Some(wts_root.clone()),
            create_worktree: true,
            ttl_minutes: DEFAULT_LEASE_TTL_MINS,
        },
    )
    .expect("claim T-1");
    let wt_t1 = Path::new(claim.worktree_path.as_deref().expect("worktree path"));
    git(wt_t1, &["config", "user.name", "Agent A"]);
    git(wt_t1, &["config", "user.email", "agent-a@oxideswarm.local"]);
    std::fs::write(wt_t1.join("alpha/feature.txt"), "new alpha\n").unwrap();
    git(wt_t1, &["add", "alpha/feature.txt"]);
    git(wt_t1, &["commit", "-m", "implement alpha"]);
    let head_sha = git(wt_t1, &["rev-parse", "HEAD"]);

    let pretty_t1 = format!(
        r#"{{
  "task": "T-1",
  "commit": "{head_sha}",
  "ok": true,
  "blocked": false,
  "timestamp_utc": "2026-11-26T13:30:00Z",
  "cells": [
    {{
      "profile": "unit",
      "os": "windows",
      "worker": "win-2",
      "link": "tcp",
      "toolchain": "rustc 1.96.0 x86_64-pc-windows-msvc",
      "exit": 0,
      "secs": 1.2,
      "task_id": "11111111-1111-1111-1111-111111111111"
    }}
  ]
}}"#
    );
    std::fs::create_dir_all(wt_t1.join(".oxideswarm/verify")).unwrap();
    let rec_path = wt_t1.join(".oxideswarm/verify/T-1.json");
    std::fs::write(&rec_path, &pretty_t1).unwrap();

    // Multi-record pretty-printed concatenation test (with explicit file containing 2 pretty objects).
    let two_pretty_path = wt_t1.join(".oxideswarm/verify/two.json");
    std::fs::write(&two_pretty_path, format!("{pretty_t1}\n{pretty_t1}\n")).unwrap();
    let streamed = parse_verify_records(&std::fs::read_to_string(&two_pretty_path).unwrap())
        .expect("parse concatenated pretty-printed verify records");
    assert_eq!(streamed.len(), 2);

    // Fresh base: status == PASS, stale == false.
    let fresh = build_plan_pr_body(
        wt_t1,
        &PlanPrBodyOptions {
            task_id: "T-1".to_string(),
            plan_path: None,
            base: None,
            remote: "origin".to_string(),
            record_paths: Vec::new(),
            branch: None,
            summary: Some("Alpha implementation.".to_string()),
            require_fresh: true,
        },
    )
    .expect("build fresh PR body");
    assert!(!fresh.stale);
    assert_eq!(fresh.status, "PASS");
    assert!(fresh.markdown.contains("**Status:** `PASS` (1 cell(s))"));
    assert!(!fresh.markdown.contains("STALE"));

    // Advance `main` on bare.git and fetch in wt_t1 -> verified base is now STALE!
    std::fs::write(clone_other.join("alpha/seed.txt"), "seed v2\n").unwrap();
    git(&clone_other, &["add", "alpha/seed.txt"]);
    git(&clone_other, &["commit", "-m", "advance main"]);
    git(&clone_other, &["push", "origin", "main"]);
    git(wt_t1, &["fetch", "origin", "main"]);

    let stale = build_plan_pr_body(
        wt_t1,
        &PlanPrBodyOptions {
            task_id: "T-1".to_string(),
            plan_path: None,
            base: None,
            remote: "origin".to_string(),
            record_paths: Vec::new(),
            branch: None,
            summary: None,
            require_fresh: true,
        },
    )
    .expect("build stale PR body");
    assert!(stale.stale);
    assert_eq!(stale.status, "STALE");
    assert!(stale
        .markdown
        .contains("**Status:** `STALE` (verified `PASS`, 1 cell(s))"));
    assert!(stale.markdown.contains("> **STALE:**"));

    // Verify real CLI exit code is non-zero under `--require-fresh` when stale.
    let exe = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent()?.parent()?.join("oxideswarm.exe").into())
        .filter(|p| p.is_file())
        .or_else(|| {
            std::env::current_exe()
                .ok()
                .and_then(|p| p.parent()?.parent()?.join("oxideswarm").into())
                .filter(|p| p.is_file())
        });
    if let Some(bin) = exe {
        let out = Command::new(&bin)
            .args([
                "plan",
                "pr-body",
                "--task",
                "T-1",
                "--repo",
                wt_t1.to_str().unwrap(),
                "--require-fresh",
            ])
            .output()
            .expect("run oxideswarm plan pr-body --require-fresh");
        assert!(
            !out.status.success(),
            "expected non-zero exit when stale under --require-fresh"
        );
        assert!(String::from_utf8_lossy(&out.stdout).contains("STALE"));
    }

    // D6.3: `plan done T-1` on updated base branch sets status="done" (preserving comments),
    // deletes remote lease, makes T-2 claimable in `plan status`, and allows `claim_task("T-2")`
    // once the updated `.oxideswarm/plan.toml` is pushed to the base branch.
    git(&clone_a, &["pull", "--ff-only", "origin", "main"]);
    let done = mark_task_done(
        &clone_a,
        &DoneOptions {
            task_id: "T-1".to_string(),
            plan_path: None,
            remote: "origin".to_string(),
        },
    )
    .expect("mark T-1 done");
    assert_eq!(done.previous_status, "open");
    assert_eq!(done.new_status, "done");
    assert!(done.lease_deleted);
    assert_eq!(done.unblocked_dependents, vec!["T-2".to_string()]);

    let updated_toml = std::fs::read_to_string(clone_a.join(".oxideswarm/plan.toml")).unwrap();
    assert!(updated_toml.contains("# Team wave plan (comments must survive plan done)"));
    assert!(updated_toml.contains("# inline comment"));
    assert!(updated_toml.contains("status = \"done\""));

    assert!(
        lookup_task_lease(&clone_a, "origin", "T-1")
            .expect("lookup deleted lease")
            .is_none(),
        "remote lease ref must be deleted after plan done"
    );

    let snap =
        build_snapshot(&clone_a, None, None, chrono::Utc::now()).expect("snapshot after plan done");
    assert!(
        !snap.text.contains("needs T-1"),
        "expected T-2 no longer blocked by T-1 in snapshot:\n{}",
        snap.text
    );

    git(&clone_a, &["add", ".oxideswarm/plan.toml"]);
    git(&clone_a, &["commit", "-m", "mark T-1 done"]);
    git(&clone_a, &["push", "origin", "main"]);

    let claim_t2 = claim_task(
        &clone_a,
        &ClaimOptions {
            task_id: "T-2".to_string(),
            holder: "agent-b".to_string(),
            person: None,
            remote: "origin".to_string(),
            base: None,
            plan_path: None,
            branch: None,
            worktree_root: Some(wts_root),
            create_worktree: true,
            ttl_minutes: DEFAULT_LEASE_TTL_MINS,
        },
    )
    .expect("T-2 should now be claimable after T-1 is done");
    assert_eq!(claim_t2.task_id, "T-2");
}
