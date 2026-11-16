#!/usr/bin/env python3
"""Release skew check (PROTO-2, design 0076 D2).

Usage: release_skew.py RELEASE_OXIDESWARM PR_OXIDESWARM [--timeout SECONDS]

Runs three master/worker/client pairs on loopback, each with a private temporary HOME, an
ephemeral grid port and no use of the real ~/.oxideswarm:

  1. release master, pull request worker and client
  2. pull request master, release worker and client
  3. pull request master, pull request worker and client

Each pair starts a master, one worker, then runs `workers` and `submit --command echo --wait -- hi`
with timeouts. A pair is classified as:

  works                          the task completed
  refused with the version error a peer was refused and the text names the protocol versions
  broken                         anything else

The script does not demand compatibility. It exits 1 only when a pair is `broken` (this covers a
refusal that prints no version error, and a pair that speaks the same version but does not work),
or when a pair is refused although the peer's version lies inside the refuser's supported range.
The three-row Markdown table goes to stdout and to $GITHUB_STEP_SUMMARY when that is set.

Python standard library only. Every process it starts is stopped by its own handle, never by name.
"""

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

WORKER_NAME = "skew-w1"
LOOPBACK = "127.0.0.1"

WORKS = "works"
REFUSED = "refused with the version error"
BROKEN = "broken"

# `Incompatible protocol version 10 from worker (supported: 9..=9; upgrade both ...)`: the number
# after "version" is the peer's, the range is what the refusing binary supports (its own current
# version is the upper end).
VERSION_ERROR = re.compile(
    r"Incompatible protocol version (\d+) from \w+ \(supported: (\d+)\.\.=(\d+)"
)


def find_version_error(*texts):
    """Returns (peer_version, min_supported, refuser_current) of the first version error, or None."""
    for text in texts:
        match = VERSION_ERROR.search(text or "")
        if match:
            return tuple(int(group) for group in match.groups())
    return None


def classify(workers_rc, workers_out, submit_rc, submit_out, worker_log):
    """Classifies one pair. Returns (verdict, ok, versions, detail).

    `ok` is False when the pair must fail the job: it is broken, or it was refused although the
    peer's version lies inside the refuser's supported range.
    `versions` is (peer_version, refuser_current) when a version error named them, else None.
    """
    if (
        submit_rc == 0
        and "completed successfully" in (submit_out or "")
        and workers_rc == 0
        and WORKER_NAME in (workers_out or "")
    ):
        return WORKS, True, None, "task completed"
    found = find_version_error(submit_out, workers_out, worker_log)
    if found:
        peer, minimum, current = found
        if minimum <= peer <= current:
            return (
                BROKEN,
                False,
                (peer, current),
                f"refused although peer version {peer} is inside the supported range "
                f"{minimum}..={current}",
            )
        return REFUSED, True, (peer, current), f"peer speaks {peer}, refuser supports {minimum}..={current}"
    return BROKEN, False, None, "neither completed nor a version error"


class Pair:
    """One master + worker + client run in a private directory."""

    def __init__(self, label, master_bin, worker_bin, timeout, root):
        self.label = label
        self.master_bin = master_bin
        self.worker_bin = worker_bin
        self.timeout = timeout
        self.dir = tempfile.mkdtemp(prefix="pair-", dir=root)
        self.home = os.path.join(self.dir, "home")
        os.makedirs(self.home)
        self.procs = []
        self.logs = {}

    def env(self):
        env = {k: v for k, v in os.environ.items() if not k.startswith(("RUSTY_GRID_", "OXIDESWARM_", "OXIDE_"))}
        env.update(
            HOME=self.home,
            USERPROFILE=self.home,
            XDG_CONFIG_HOME=os.path.join(self.home, ".config"),
            XDG_CACHE_HOME=os.path.join(self.home, ".cache"),
            XDG_DATA_HOME=os.path.join(self.home, ".local", "share"),
            TMPDIR=self.dir,
            NO_COLOR="1",
        )
        return env

    def spawn(self, name, binary, args):
        log_path = os.path.join(self.dir, name + ".log")
        self.logs[name] = log_path
        log = open(log_path, "wb")
        proc = subprocess.Popen(
            [binary] + args, cwd=self.dir, env=self.env(), stdout=log, stderr=subprocess.STDOUT
        )
        self.procs.append(proc)
        return proc

    def run(self, binary, args):
        try:
            done = subprocess.run(
                [binary] + args,
                cwd=self.dir,
                env=self.env(),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=self.timeout,
            )
            return done.returncode, done.stdout.decode("utf-8", "replace")
        except subprocess.TimeoutExpired as err:
            return 124, (err.stdout or b"").decode("utf-8", "replace") + "\n[timed out]"

    def read_log(self, name):
        try:
            with open(self.logs[name], "rb") as f:
                return f.read().decode("utf-8", "replace")
        except OSError:
            return ""

    def stop(self):
        for proc in self.procs:
            if proc.poll() is None:
                proc.terminate()
        deadline = time.time() + 5
        for proc in self.procs:
            try:
                proc.wait(timeout=max(0.1, deadline - time.time()))
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

    def execute(self):
        port_file = os.path.join(self.dir, "grid.port")
        dash_file = os.path.join(self.dir, "dashboard.port")
        master = self.spawn(
            "master",
            self.master_bin,
            [
                "master",
                "--listen", f"{LOOPBACK}:0",
                "--port-file", port_file,
                "--dashboard-port", "0",
                "--dashboard-port-file", dash_file,
            ],
        )
        port = None
        deadline = time.time() + self.timeout
        while time.time() < deadline and master.poll() is None:
            try:
                with open(port_file) as f:
                    port = f.read().strip()
            except OSError:
                port = None
            if port:
                break
            time.sleep(0.2)
        if not port:
            return None, "master did not start", ("", ""), (None, ""), (None, "")
        addr = f"{LOOPBACK}:{port}"
        self.spawn(
            "worker",
            self.worker_bin,
            ["worker", "--master", addr, "--no-portal", "--name", WORKER_NAME],
        )
        # Wait until the worker is listed or the worker log shows a version refusal.
        workers_rc, workers_out = None, ""
        deadline = time.time() + self.timeout
        while time.time() < deadline:
            workers_rc, workers_out = self.run(self.worker_bin, ["workers", "--master", addr])
            if workers_rc == 0 and WORKER_NAME in workers_out:
                break
            if find_version_error(workers_out, self.read_log("worker")):
                break
            time.sleep(0.5)
        submit_rc, submit_out = self.run(
            self.worker_bin,
            ["submit", "--master", addr, "--command", "echo", "--wait", "--", "hi"],
        )
        return addr, None, None, (workers_rc, workers_out), (submit_rc, submit_out)

    def cleanup(self):
        self.stop()
        shutil.rmtree(self.dir, ignore_errors=True)


def run_pair(label, master_bin, worker_bin, timeout, root):
    pair = Pair(label, master_bin, worker_bin, timeout, root)
    try:
        addr, error, _, (workers_rc, workers_out), (submit_rc, submit_out) = pair.execute()
        worker_log = pair.read_log("worker")
        if error:
            verdict, ok, versions, detail = BROKEN, False, None, error
        else:
            verdict, ok, versions, detail = classify(
                workers_rc, workers_out, submit_rc, submit_out, worker_log
            )
        transcript = "\n".join(
            [
                f"[master log]\n{pair.read_log('master')[-1500:]}",
                f"[worker log]\n{worker_log[-1500:]}",
                f"[workers rc={workers_rc}]\n{(workers_out or '')[-800:]}",
                f"[submit rc={submit_rc}]\n{(submit_out or '')[-800:]}",
            ]
        )
        return verdict, ok, versions, detail, transcript
    finally:
        pair.cleanup()


def render_table(rows):
    lines = [
        "| Master | Worker and client | Result | Protocol versions | Detail |",
        "|---|---|---|---|---|",
    ]
    for master, worker, verdict, versions, detail in rows:
        lines.append(f"| {master} | {worker} | {verdict} | {versions} | {detail} |")
    return "\n".join(lines)


def versions_text(pair_index, versions):
    """Human text for the protocol versions a pair revealed. `versions` is (peer, refuser)."""
    if not versions:
        return "same or compatible"
    peer, refuser = versions
    if pair_index == 1:  # release master refuses the PR peer
        return f"release {refuser}, pull request {peer}"
    return f"pull request {refuser}, release {peer}"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("release_bin", help="oxideswarm binary of the newest release")
    parser.add_argument("pr_bin", help="oxideswarm binary built from the pull request")
    parser.add_argument("--timeout", type=int, default=30, help="seconds per step (default 30)")
    args = parser.parse_args(argv)

    release_bin = os.path.abspath(args.release_bin)
    pr_bin = os.path.abspath(args.pr_bin)
    for path in (release_bin, pr_bin):
        if not (os.path.isfile(path) and os.access(path, os.X_OK)):
            print(f"error: {path} is not an executable file", file=sys.stderr)
            return 2

    plan = [
        ("release", "pull request", release_bin, pr_bin),
        ("pull request", "release", pr_bin, release_bin),
        ("pull request", "pull request", pr_bin, pr_bin),
    ]
    root = tempfile.mkdtemp(prefix="release-skew-")
    rows, failed, transcripts = [], [], []
    try:
        for index, (master_label, worker_label, master_bin, worker_bin) in enumerate(plan, 1):
            verdict, ok, versions, detail, transcript = run_pair(
                f"{master_label}/{worker_label}", master_bin, worker_bin, args.timeout, root
            )
            rows.append((master_label, worker_label, verdict, versions_text(index, versions), detail))
            if not ok:
                failed.append(f"{master_label} master with {worker_label} worker: {detail}")
                transcripts.append(f"== {master_label} master, {worker_label} worker ==\n{transcript}")
    finally:
        shutil.rmtree(root, ignore_errors=True)

    table = render_table(rows)
    print(table)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        try:
            with open(summary, "a", encoding="utf-8") as f:
                f.write("### Release skew\n\n" + table + "\n")
        except OSError as err:
            print(f"warning: could not write {summary}: {err}", file=sys.stderr)
    if failed:
        print("\nFAILED:", file=sys.stderr)
        for line in failed:
            print(f"- {line}", file=sys.stderr)
        for text in transcripts:
            print("\n" + text, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
