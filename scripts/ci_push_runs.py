#!/usr/bin/env python3
"""ci_push_runs.py: classify the CI runs for pushes on master that are not green (CI-1, design 0088).

  python3 scripts/ci_push_runs.py [--days N] [--since YYYY-MM-DD --until YYYY-MM-DD]
                                  [--repo OWNER/REPO] [--branch master] [--workflow ci.yml]

One line per completed run that is not green, with its class:

  failing test     a job whose log has a failed test (`test result: FAILED`, a `... FAILED` line,
                   or a Python `FAILED (failures=..)`), or, with no log at hand, a failed test step
  never acquired   a job with the annotation "The job was not acquired by Runner"
  cancelled        a job with the annotation "The run was canceled by" (an account cancelled it)
  other            anything else (a failed download, a runner that died, a timeout, ...)

The last line says whether the window holds no run with a failing test (the exit check of Q80).
Only the standard library and the `gh` CLI (read-only calls) are used. The classification
(`classify_run`) is a pure function over recorded JSON; `fetch_*` is the thin `gh` layer.
"""
import argparse
import datetime
import json
import subprocess
import sys
import time

GREEN = {"success", "skipped", "neutral"}
NOT_ACQUIRED = "The job was not acquired by Runner"
CANCELLED_BY = "The run was canceled by"
TEST_FAILURE_MARKERS = ("test result: FAILED", "FAILED (failures=", "FAILED (errors=")
FAILING_TEST = "failing test"
NEVER_ACQUIRED = "never acquired"
CANCELLED = "cancelled"
OTHER = "other"


def annotation_text(annotation):
    return " ".join(str(annotation.get(k) or "") for k in ("title", "message"))


def log_has_failed_test(excerpt):
    """True when a log excerpt shows a failed test."""
    for line in (excerpt or "").splitlines():
        if any(m in line for m in TEST_FAILURE_MARKERS):
            return True
        stripped = line.rstrip()
        if stripped.startswith("test ") and stripped.endswith(" FAILED"):
            return True
    return False


def classify_job(job, annotations, log_excerpt):
    """Class of one job that is not green, or None when it is green."""
    if job.get("conclusion") in GREEN:
        return None
    if log_excerpt is not None:
        if log_has_failed_test(log_excerpt):
            return FAILING_TEST
    elif job.get("conclusion") == "failure":
        for step in job.get("steps") or []:
            if step.get("conclusion") == "failure" and "test" in (step.get("name") or "").lower():
                return FAILING_TEST
    texts = [annotation_text(a) for a in annotations or []]
    if any(NOT_ACQUIRED in t for t in texts):
        return NEVER_ACQUIRED
    if any(CANCELLED_BY in t for t in texts):
        return CANCELLED
    return OTHER


def classify_run(run, jobs, annotations_by_job=None, logs_by_job=None):
    """Return (class, detail) for a run, or None when it is green or not finished.

    run: a workflow run object; jobs: its job objects; annotations_by_job: {job id: [annotation]};
    logs_by_job: {job id: log excerpt} for the jobs whose log was read. Priority when a run holds
    several classes: failing test, never acquired, cancelled, other.
    """
    if run.get("status") != "completed" or run.get("conclusion") in GREEN:
        return None
    annotations_by_job = annotations_by_job or {}
    logs_by_job = logs_by_job or {}
    found = {}
    for job in jobs:
        cls = classify_job(job, annotations_by_job.get(job.get("id"), []), logs_by_job.get(job.get("id")))
        if cls:
            found.setdefault(cls, []).append(job.get("name") or str(job.get("id")))
    for cls in (FAILING_TEST, NEVER_ACQUIRED, CANCELLED, OTHER):
        if cls in found:
            return cls, ", ".join(found[cls])
    # A run that is not green but holds no job that is not green: for example cancelled before
    # any job started, or a startup failure.
    return OTHER, "run concluded %s with no failed job" % run.get("conclusion")


def window_summary(lines, total_runs, first, last):
    """The summary line; `lines` is a list of (run, class, detail)."""
    failing = [r for r, c, _ in lines if c == FAILING_TEST]
    counts = {}
    for _, c, _ in lines:
        counts[c] = counts.get(c, 0) + 1
    parts = ", ".join("%d %s" % (n, c) for c, n in sorted(counts.items())) or "none"
    verdict = ("NO run with a failing test" if not failing
               else "%d run(s) WITH a failing test" % len(failing))
    return "window %s .. %s: %d completed run(s), %d not green (%s); %s" % (
        first, last, total_runs, len(lines), parts, verdict)


def format_line(run, cls, detail):
    return "%s  %s  %s  %-14s %s" % (
        run.get("id"), (run.get("created_at") or "")[:19], (run.get("head_sha") or "")[:7], cls, detail)


# ---- thin gh layer ----------------------------------------------------------------------

def gh(args, check=True):
    """Run `gh`; a read is retried three times (a 5xx from the API is common)."""
    p = None
    for attempt in range(3):
        p = subprocess.run(["gh"] + args, capture_output=True, text=True, encoding="utf-8", errors="replace")
        if p.returncode == 0:
            return p.stdout
        time.sleep(2 * (attempt + 1))
    if check:
        raise RuntimeError("gh %s failed: %s" % (" ".join(args[:2]), p.stderr.strip()[:300]))
    return None


def gh_json(path):
    return json.loads(gh(["api", path]))


def fetch_runs(repo, workflow, branch, since, until):
    runs = []
    for page in range(1, 20):
        data = gh_json("repos/%s/actions/workflows/%s/runs?branch=%s&event=push&per_page=100&page=%d"
                       % (repo, workflow, branch, page))
        batch = data.get("workflow_runs") or []
        runs.extend(batch)
        if len(batch) < 100 or (batch and batch[-1]["created_at"] < since):
            break
    return [r for r in runs if since <= r["created_at"] < until]


def fetch_jobs(repo, run_id):
    jobs = []
    for page in range(1, 10):
        batch = gh_json("repos/%s/actions/runs/%s/jobs?per_page=100&page=%d" % (repo, run_id, page)).get("jobs") or []
        jobs.extend(batch)
        if len(batch) < 100:
            break
    return jobs


def fetch_annotations(repo, job_id):
    out = gh(["api", "repos/%s/check-runs/%s/annotations" % (repo, job_id)], check=False)
    return json.loads(out) if out else []


def fetch_log_excerpt(repo, job_id):
    out = gh(["api", "repos/%s/actions/jobs/%s/logs" % (repo, job_id)], check=False)
    if out is None:
        return None
    keep = [l for l in out.splitlines()
            if any(m in l for m in TEST_FAILURE_MARKERS) or l.rstrip().endswith(" FAILED")]
    return "\n".join(keep)


def collect(repo, workflow, branch, since, until):
    runs = fetch_runs(repo, workflow, branch, since, until)
    lines, completed = [], 0
    for run in sorted(runs, key=lambda r: r["created_at"]):
        if run.get("status") != "completed":
            continue
        completed += 1
        if run.get("conclusion") in GREEN:
            continue
        jobs = fetch_jobs(repo, run["id"])
        notgreen = [j for j in jobs if j.get("conclusion") not in GREEN]
        annotations = {j["id"]: fetch_annotations(repo, j["id"]) for j in notgreen}
        logs = {j["id"]: fetch_log_excerpt(repo, j["id"]) for j in notgreen if j.get("conclusion") == "failure"}
        res = classify_run(run, jobs, annotations, logs)
        if res:
            lines.append((run, res[0], res[1]))
    return lines, completed


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--days", type=int, default=7, help="window length in days (default 7)")
    ap.add_argument("--since", help="window start, YYYY-MM-DD (UTC), overrides --days")
    ap.add_argument("--until", help="window end (exclusive), YYYY-MM-DD, default tomorrow")
    ap.add_argument("--repo", help="OWNER/REPO (default: the repository of the working directory)")
    ap.add_argument("--branch", default="master")
    ap.add_argument("--workflow", default="ci.yml")
    a = ap.parse_args(argv)
    repo = a.repo or gh(["repo", "view", "--json", "nameWithOwner", "--jq", ".nameWithOwner"]).strip()
    today = datetime.datetime.now(datetime.timezone.utc).date()
    until = a.until or (today + datetime.timedelta(days=1)).isoformat()
    since = a.since or (today - datetime.timedelta(days=a.days - 1)).isoformat()
    lines, completed = collect(repo, a.workflow, a.branch, since, until)
    for run, cls, detail in lines:
        print(format_line(run, cls, detail))
    print(window_summary(lines, completed, since, until))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except RuntimeError as e:
        print("ci_push_runs: %s" % e, file=sys.stderr)
        sys.exit(2)
