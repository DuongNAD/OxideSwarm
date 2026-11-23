#!/usr/bin/env python3
"""
tests/test_launcher_scripts.py - regression tests for the launcher / installer / packaging scripts.

These scripts cannot be covered by `cargo test`, so this file runs them against fake `rusty-grid`
binaries inside throw-away directories (private HOME, private ports) and checks the properties that
earlier versions violated:

  * `connect_remote.sh --dry-run` wrote the ticket into the auto-loaded ~/.oxideswarm/rusty-grid.toml
    (and a real run overwrote that file without a backup);
  * `switch_role.sh` killed processes by name pattern, exposed the dashboard on 0.0.0.0, hard-coded
    `--simulate-gpu`, and never started P2P;
  * the Termux supervisor always passed `--master 127.0.0.1:8080` and `--simulate-gpu`, and `--stop`
    left the real worker running;
  * no `.gitattributes` / LF `.cmd` files; launchers with the owner's hostname and LAN addresses;
  * installers that ran the worker (and every task) as root / SYSTEM.

Run:   python3 tests/test_launcher_scripts.py          (or: python3 -m unittest tests.test_launcher_scripts)
Mutation check: OX_SCRIPT_ROOT=/path/to/an/older/checkout python3 tests/test_launcher_scripts.py
(the tests should then fail for every property the older scripts violated).

Nothing here touches the real ~/.oxideswarm, needs a built binary, the network or root.
"""

import fnmatch
import glob
import hashlib
import http.server
import io
import json
import os
import pathlib
import platform
import re
import shlex
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import unittest
import zipfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.environ.get("OX_SCRIPT_ROOT") or os.path.join(HERE, ".."))

TICKET = '{"id":"00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff","addrs":[]}'
IS_WINDOWS = os.name == "nt"

# A stand-in for the real binary: records its arguments, prints the log lines the launchers wait for,
# and then idles as a *bash* process so that `ps` still shows "rusty-grid" in its command line.
FAKE_BINARY = """#!/bin/bash
echo "$*" >> "${FAKE_ARGS_LOG:?}"
if [ -n "${FAKE_EXEC_DIR:-}" ]; then
  case "$1" in
    master|worker) # see FAKE_PS: this process counts as exec'd only once FAKE_EXEC_DIR/<pid> exists
      sleep "${FAKE_EXEC_DELAY:-0}"; : > "$FAKE_EXEC_DIR/$$" ;;
  esac
fi
case "$1" in
  master) echo "RustyGrid Master started on fake" ;;
  worker) echo "Registration accepted; backoff counter reset" ;;
  secret) # `secret generate --out PATH`: create the file and return, like the real CLI
    while [ $# -gt 0 ]; do
      if [ "$1" = "--out" ]; then mkdir -p "$(dirname "$2")"; echo "$(printf 'ab%.0s' $(seq 1 32))" > "$2"; fi
      shift
    done
    exit 0 ;;
esac
[ -n "$FAKE_ONESHOT" ] && exit 0   # connect_remote.sh execs the worker in the foreground: return at once
while true; do sleep 1; done
"""

# A `ps` placed ahead of the real one on the launcher's PATH (TEST-35). Between the fork and the exec of
# the binary, a process the launcher has just started still shows the launcher's own command line. The
# fake binary above stretches that window: with FAKE_EXEC_DIR set, `ps -p PID -o command=` answers with
# the launcher's command line until the binary has created FAKE_EXEC_DIR/PID (FAKE_EXEC_DELAY seconds
# after it started). FAKE_PS_FOREIGN=1 answers with an unrelated program for every live PID, as `ps`
# does for a PID that now names another process. Every other call goes to the real ps.
FAKE_PS = """#!/bin/bash
if [ "$1" = "-p" ] && [ "$3" = "-o" ] && [ "$4" = "command=" ] && kill -0 "$2" 2>/dev/null; then
  if [ -n "${FAKE_PS_FOREIGN:-}" ]; then echo "/usr/libexec/unrelated-daemon --serve"; exit 0; fi
  if [ -n "${FAKE_EXEC_DIR:-}" ] && [ ! -e "$FAKE_EXEC_DIR/$2" ]; then echo "bash ./switch_role.sh"; exit 0; fi
fi
exec %(real_ps)s "$@"
"""


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def pid_alive(pid):
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    # A zombie still answers kill(0); treat it as gone.
    out = subprocess.run(["ps", "-p", str(pid), "-o", "stat="], stdout=subprocess.PIPE, text=True).stdout.strip()
    return bool(out) and not out.startswith("Z")


def wait_until(predicate, timeout=10.0, step=0.1):
    end = time.time() + timeout
    while time.time() < end:
        if predicate():
            return True
        time.sleep(step)
    return predicate()


def read(path):
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


def read_bytes(path):
    with open(path, "rb") as f:
        return f.read()


SKIP_DIRS = {
    "target",
    ".git",
    ".agents",
    "node_modules",
    "__pycache__",
    ".venv",
    "venv",
    "build",
    ".gradle",
    ".idea",
    "test_reports",
}


def tracked_or_present(patterns, root=None):
    """Files under root (default ROOT) matching the glob patterns, pruning build/cache dirs."""
    base = ROOT if root is None else root
    found = []
    for dirpath, dirnames, filenames in os.walk(base):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for fname in filenames:
            if any(fnmatch.fnmatch(fname, pat) for pat in patterns):
                path = os.path.join(dirpath, fname)
                if os.path.isfile(path):
                    found.append(path)
    return sorted(set(found))


def tracked_text_files(root=None):
    """All tracked text files under root (falls back to walking root when git is unavailable)."""
    base = ROOT if root is None else root
    files = []
    res = subprocess.run(
        ["git", "-C", base, "ls-files"],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    if res.returncode == 0 and res.stdout.strip():
        for rel in res.stdout.splitlines():
            parts = rel.replace("\\", "/").split("/")
            if any(part in SKIP_DIRS for part in parts[:-1]):
                continue
            p = os.path.join(base, rel)
            if os.path.isfile(p):
                files.append(p)
    else:
        files = tracked_or_present(["*"], root=base)
    binary_exts = (".png", ".ico", ".woff2", ".ttf", ".glb", ".bin", ".so", ".dll", ".exe", ".jar", ".keystore")
    out = []
    for p in sorted(set(files)):
        if p.lower().endswith(binary_exts):
            continue
        raw = read_bytes(p)
        if b"\x00" in raw[:8192]:
            continue
        out.append(p)
    return out


@unittest.skipIf(IS_WINDOWS, "the shell launchers need bash")
class ShellLauncherTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="ox_launcher_test_")
        self.home = os.path.join(self.tmp, "home")
        os.makedirs(self.home)
        self.repo = os.path.join(self.tmp, "repo")  # a fake checkout holding copies of the scripts
        os.makedirs(os.path.join(self.repo, "target", "release"))
        os.makedirs(os.path.join(self.repo, "target", "debug"))
        self.args_log = os.path.join(self.tmp, "fake_args.log")
        open(self.args_log, "w").close()
        for name in ("rusty-grid",):
            for profile in ("release", "debug"):
                p = os.path.join(self.repo, "target", profile, name)
                with open(p, "w") as f:
                    f.write(FAKE_BINARY)
                os.chmod(p, 0o755)
        self.cleanup_pids = []
        self.cleanup_procs = []  # Popen objects to reap after they were killed
        self.addCleanup(self._cleanup)

    def _cleanup(self):
        # Best effort, only ever our own leftovers: the decoys we started and anything whose command
        # line lives under this test's private temp directory (e.g. fake workers of a failed run).
        listing = subprocess.run(["ps", "-axo", "pid=,command="], stdout=subprocess.PIPE, text=True).stdout
        victims = set(self.cleanup_pids)
        for line in listing.splitlines():
            pid_text, _, command = line.strip().partition(" ")
            if self.tmp in command and pid_text.isdigit() and int(pid_text) != os.getpid():
                victims.add(int(pid_text))
        for pid in victims:
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
        for proc in self.cleanup_procs:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        shutil.rmtree(self.tmp, ignore_errors=True)

    def env(self, **extra):
        env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": self.home,
            "USERPROFILE": self.home,
            "FAKE_ARGS_LOG": self.args_log,
            "TMPDIR": self.tmp,
        }
        env.update(extra)
        return env

    def copy_script(self, relative, dst_name=None):
        src = os.path.join(ROOT, relative)
        dst = os.path.join(self.repo, dst_name or os.path.basename(relative))
        shutil.copy(src, dst)
        os.chmod(dst, 0o755)
        return dst

    def run_script(self, args, **env_extra):
        return subprocess.run(
            args,
            cwd=self.repo,
            env=self.env(**env_extra),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=90,
        )

    def launcher_failure(self, res, state):
        """Failure message for a launcher run: command, return code, output, logs and fake-binary calls."""
        parts = ["launcher %r returned %r" % (res.args, res.returncode), "---- stdout+stderr ----", res.stdout]
        for name in ("master.log", "worker.log", "master.log.old", "worker.log.old"):
            path = os.path.join(state, "run", name)
            if os.path.isfile(path):
                parts += ["---- %s ----" % name, read(path)]
        parts += ["---- fake binary calls ----", read(self.args_log)]
        return "\n".join(parts)

    def fake_args(self):
        return [line for line in read(self.args_log).splitlines() if line.strip()]

    def snapshot_tree(self, base):
        out = {}
        for dirpath, _dirs, files in os.walk(base):
            for name in files:
                p = os.path.join(dirpath, name)
                out[p] = (os.path.getsize(p), os.path.getmtime(p))
        return out

    # ---- connect_remote.sh ----------------------------------------------------------------

    def test_connect_remote_dry_run_changes_nothing_in_home(self):
        cfg_dir = os.path.join(self.home, ".oxideswarm")
        os.makedirs(cfg_dir)
        sentinel = os.path.join(cfg_dir, "rusty-grid.toml")
        with open(sentinel, "w") as f:
            f.write("# owner config, must survive\n[worker]\nmaster = \"10.0.0.5:8088\"\n")
        before = self.snapshot_tree(self.home)
        script = self.copy_script("connect_remote.sh")
        res = self.run_script(["bash", script, TICKET, "--dry-run"])
        self.assertEqual(res.returncode, 0, res.stdout)
        self.assertEqual(self.snapshot_tree(self.home), before, "dry-run must not create or modify anything")
        self.assertIn("master = \"10.0.0.5:8088\"", read(sentinel))

    def test_connect_remote_never_writes_default_config_and_backs_up_its_own(self):
        secret = os.path.join(self.tmp, "cluster.secret")
        with open(secret, "w") as f:
            f.write("ab" * 32 + "\n")
        state = os.path.join(self.tmp, "state")
        script = self.copy_script("connect_remote.sh")
        first = self.run_script(["bash", script, TICKET, "--foreground", "--secret-file", secret, "--name", "first"],
                                OXIDESWARM_HOME=state, FAKE_ONESHOT="1")
        self.assertEqual(first.returncode, 0, first.stdout)
        second = self.run_script(["bash", script, TICKET, "--foreground", "--secret-file", secret, "--name", "second"],
                                 OXIDESWARM_HOME=state, FAKE_ONESHOT="1")
        self.assertEqual(second.returncode, 0, second.stdout)

        remote = os.path.join(state, "remote-worker.toml")
        self.assertTrue(os.path.isfile(remote), os.listdir(state))
        self.assertFalse(os.path.exists(os.path.join(state, "rusty-grid.toml")),
                         "the auto-loaded rusty-grid.toml must never be written")
        self.assertFalse(os.path.exists(os.path.join(self.home, ".oxideswarm", "rusty-grid.toml")))
        self.assertEqual(stat.S_IMODE(os.stat(remote).st_mode), 0o600)
        self.assertIn('name = "second"', read(remote))
        backups = glob.glob(os.path.join(state, "remote-worker.toml.bak-*"))
        self.assertEqual(len(backups), 1, "the previous config must be backed up when it changes")
        self.assertIn('name = "first"', read(backups[0]))
        # the worker is started from the dedicated file, with no ticket or secret on the command line
        last = self.fake_args()[-1]
        self.assertEqual(last, "worker --config " + remote)

    def test_connect_remote_refuses_to_pair_without_a_secret(self):
        script = self.copy_script("connect_remote.sh")
        res = self.run_script(["bash", script, TICKET, "--foreground"],
                              OXIDESWARM_HOME=os.path.join(self.tmp, "state"), FAKE_ONESHOT="1")
        self.assertNotEqual(res.returncode, 0, "pairing without the cluster secret must fail unless --insecure")
        self.assertEqual(self.fake_args(), [], "no worker may be started")

    # ---- switch_role.sh -------------------------------------------------------------------

    def _start_decoy(self):
        # A process whose command line matches the old `pkill -f "rusty-grid (master|worker)"` pattern.
        decoy = subprocess.Popen(["bash", "-c", 'exec -a "rusty-grid worker unrelated-decoy" sleep 120'],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.cleanup_pids.append(decoy.pid)
        self.cleanup_procs.append(decoy)
        self.assertTrue(wait_until(lambda: "rusty-grid worker unrelated-decoy" in subprocess.run(
            ["ps", "-p", str(decoy.pid), "-o", "command="], stdout=subprocess.PIPE, text=True).stdout))
        return decoy

    def test_switch_role_master_flow_and_tracked_stop(self):
        script = self.copy_script("switch_role.sh")
        cport, dport = free_port(), free_port()
        state = os.path.join(self.tmp, "state")
        env = dict(OXIDESWARM_HOME=state, OXIDESWARM_CLUSTER_PORT=str(cport), OXIDESWARM_DASHBOARD_PORT=str(dport))

        res = self.run_script(["bash", script, "master"], **env)
        self.assertEqual(res.returncode, 0, self.launcher_failure(res, state))
        lines = self.fake_args()
        master_line = next(l for l in lines if l.startswith("master"))
        worker_line = next(l for l in lines if l.startswith("worker"))
        # cluster port on all interfaces, dashboard explicitly on loopback, no P2P unless asked
        self.assertIn("--listen 0.0.0.0:%d" % cport, master_line)
        self.assertIn("--web-ui-addr 127.0.0.1:%d" % dport, master_line)
        self.assertNotIn("0.0.0.0:%d" % dport, master_line)
        self.assertNotIn("--p2p", master_line)
        # the local worker targets the CLUSTER port, never the dashboard port, and does not fake a GPU
        self.assertIn("--master 127.0.0.1:%d" % cport, worker_line)
        self.assertNotIn("--simulate-gpu", " ".join(lines))
        self.assertTrue(os.path.isfile(os.path.join(state, "cluster.secret")) or "--secret-file" not in master_line)

        pidfile = os.path.join(state, "run", "master.pid")
        self.assertTrue(os.path.isfile(pidfile), "the master must be tracked through a PID file")
        master_pid = int(read(pidfile).strip())
        worker_pid = int(read(os.path.join(state, "run", "worker.pid")).strip())
        self.assertTrue(pid_alive(master_pid) and pid_alive(worker_pid))

        stop = self.run_script(["bash", script, "stop"], **env)
        self.assertEqual(stop.returncode, 0, self.launcher_failure(stop, state))
        self.assertTrue(wait_until(lambda: not pid_alive(master_pid) and not pid_alive(worker_pid)),
                        "stop must terminate the tracked master and worker")

    def exec_race_env(self, **extra):
        """Launcher environment whose `ps` is FAKE_PS, with FAKE_EXEC_DIR set (TEST-35)."""
        shim = os.path.join(self.tmp, "ps-shim")
        exec_dir = os.path.join(self.tmp, "exec-marks")
        os.makedirs(shim)
        os.makedirs(exec_dir)
        real_ps = shutil.which("ps")
        self.assertIsNotNone(real_ps, "the tests need ps")
        path = os.path.join(shim, "ps")
        with open(path, "w") as f:
            f.write(FAKE_PS % {"real_ps": shlex.quote(real_ps)})
        os.chmod(path, 0o755)
        env = dict(PATH=shim + os.pathsep + os.environ.get("PATH", "/usr/bin:/bin"), FAKE_EXEC_DIR=exec_dir)
        env.update(extra)
        return env

    def test_switch_role_master_waits_until_the_new_process_runs_the_binary(self):
        # TEST-35 (macos-latest, 2026-11-18): right after the fork the master's PID still showed the
        # launcher's own command line, so the start loop gave up on its first pass with an empty log,
        # and `stop_tracked` recognised the same PID as the master a moment later. Here that window
        # lasts 3 s, for the master and for the local worker.
        script = self.copy_script("switch_role.sh")
        state = os.path.join(self.tmp, "state")
        env = self.exec_race_env(OXIDESWARM_HOME=state, OXIDESWARM_CLUSTER_PORT=str(free_port()),
                                 OXIDESWARM_DASHBOARD_PORT=str(free_port()), FAKE_EXEC_DELAY="3")
        res = self.run_script(["bash", script, "master"], **env)
        self.assertEqual(res.returncode, 0, self.launcher_failure(res, state))
        self.assertIn("REGISTERED with Master", res.stdout, self.launcher_failure(res, state))
        master_pid = int(read(os.path.join(state, "run", "master.pid")).strip())
        worker_pid = int(read(os.path.join(state, "run", "worker.pid")).strip())
        stop = self.run_script(["bash", script, "stop"], **env)
        self.assertEqual(stop.returncode, 0, self.launcher_failure(stop, state))
        self.assertTrue(wait_until(lambda: not pid_alive(master_pid) and not pid_alive(worker_pid)),
                        "stop must terminate the tracked master and worker")

    def test_switch_role_worker_waits_until_the_new_process_runs_the_binary(self):
        # The worker role checks its process after `sleep 2`; here the binary replaces the launcher's
        # command line only after 3 s (TEST-35).
        script = self.copy_script("switch_role.sh")
        state = os.path.join(self.tmp, "state")
        env = self.exec_race_env(OXIDESWARM_HOME=state, OXIDESWARM_CLUSTER_PORT=str(free_port()),
                                 FAKE_EXEC_DELAY="3")
        res = self.run_script(["bash", script, "worker", "192.0.2.10:8088"], **env)
        self.assertEqual(res.returncode, 0, self.launcher_failure(res, state))
        self.assertIn("REGISTERED with Master", res.stdout, self.launcher_failure(res, state))
        worker_pid = int(read(os.path.join(state, "run", "worker.pid")).strip())
        stop = self.run_script(["bash", script, "stop"], **env)
        self.assertEqual(stop.returncode, 0, self.launcher_failure(stop, state))
        self.assertTrue(wait_until(lambda: not pid_alive(worker_pid)), "stop must terminate the tracked worker")

    def test_switch_role_master_start_neither_accepts_nor_stops_a_foreign_pid(self):
        # Once the master's log line is there, the start loop still checks the command line: a PID that
        # now names another program (recycled) fails the start, and the launcher never signals it.
        script = self.copy_script("switch_role.sh")
        state = os.path.join(self.tmp, "state")
        env = self.exec_race_env(OXIDESWARM_HOME=state, OXIDESWARM_CLUSTER_PORT=str(free_port()),
                                 OXIDESWARM_DASHBOARD_PORT=str(free_port()), FAKE_PS_FOREIGN="1")
        res = self.run_script(["bash", script, "master", "--no-worker"], **env)
        self.assertEqual(res.returncode, 1, self.launcher_failure(res, state))
        self.assertIn("Master failed to start", res.stdout)
        self.assertNotIn("Stopping master", res.stdout, "a PID that is not ours must never be signalled")
        binary = os.path.join(self.repo, "target", "release", "rusty-grid") + " master "
        listing = subprocess.run(["ps", "-axo", "pid=,command="], stdout=subprocess.PIPE, text=True).stdout
        pids = [int(line.split(None, 1)[0]) for line in listing.splitlines() if binary in line]
        self.assertEqual(len(pids), 1, listing)
        self.assertTrue(pid_alive(pids[0]), "the process behind the foreign PID must still run")

    def test_switch_role_never_kills_unrelated_processes(self):
        script = self.copy_script("switch_role.sh")
        decoy = self._start_decoy()
        env = dict(OXIDESWARM_HOME=os.path.join(self.tmp, "state"),
                   OXIDESWARM_CLUSTER_PORT=str(free_port()), OXIDESWARM_DASHBOARD_PORT=str(free_port()))
        # switching roles and stopping both stop "whatever this launcher started"; the decoy is not that
        for args in (["master", "--no-worker"], ["worker", "192.0.2.10:8088"], ["stop"]):
            self.run_script(["bash", script] + args, **env)
        self.assertTrue(pid_alive(decoy.pid),
                        "a process that merely has 'rusty-grid worker' in its command line was killed by name pattern")

    def test_switch_role_master_options_p2p_and_lan_dashboard(self):
        script = self.copy_script("switch_role.sh")
        cport, dport = free_port(), free_port()
        env = dict(OXIDESWARM_HOME=os.path.join(self.tmp, "state"),
                   OXIDESWARM_CLUSTER_PORT=str(cport), OXIDESWARM_DASHBOARD_PORT=str(dport))
        res = self.run_script(["bash", script, "master", "--p2p", "--dashboard-lan", "--no-worker"], **env)
        self.assertEqual(res.returncode, 0, res.stdout)
        master_line = next(l for l in self.fake_args() if l.startswith("master"))
        self.assertIn("--web-ui-addr 0.0.0.0:%d" % dport, master_line)
        self.assertIn("--p2p --p2p-ticket-file", master_line)
        self.assertFalse(any(l.startswith("worker") for l in self.fake_args()), "--no-worker must not start a worker")
        self.run_script(["bash", script, "stop"], **env)

    def test_switch_role_worker_targets_the_given_cluster_address(self):
        script = self.copy_script("switch_role.sh")
        state = os.path.join(self.tmp, "state")
        env = dict(OXIDESWARM_HOME=state, OXIDESWARM_CLUSTER_PORT=str(free_port()))
        res = self.run_script(["bash", script, "worker", "192.0.2.10:8088", "--", "--gpu"], **env)
        self.assertEqual(res.returncode, 0, res.stdout)
        worker_line = next(l for l in self.fake_args() if l.startswith("worker"))
        self.assertIn("--master 192.0.2.10:8088", worker_line)
        self.assertTrue(worker_line.rstrip().endswith("--gpu"), worker_line)
        self.assertNotIn("--simulate-gpu", worker_line)
        self.run_script(["bash", script, "stop"], **env)

    # ---- Termux supervisor ----------------------------------------------------------------

    def test_termux_supervisor_passes_only_requested_flags_and_stop_kills_the_worker(self):
        src = os.path.join(ROOT, "packaging", "android", "termux", "start_worker.sh")
        script = os.path.join(self.repo, "start_worker.sh")
        text = read(src).split("\n", 1)
        with open(script, "w") as f:  # the real shebang points into Termux
            f.write("#!/bin/bash\n" + text[1])
        os.chmod(script, 0o755)
        bindir = os.path.join(self.home, "bin")
        os.makedirs(bindir)
        shutil.copy(os.path.join(self.repo, "target", "debug", "rusty-grid"), os.path.join(bindir, "rusty-grid"))
        cfg_dir = os.path.join(self.home, ".config", "oxideswarm")
        os.makedirs(cfg_dir)
        with open(os.path.join(cfg_dir, "rusty-grid.toml"), "w") as f:
            f.write('[worker]\nmaster = "192.0.2.9:8088"\n')
        path = bindir + os.pathsep + os.environ.get("PATH", "")

        started = self.run_script(["bash", script, "--daemon"], PATH=path)
        self.assertEqual(started.returncode, 0, started.stdout)
        self.assertTrue(wait_until(lambda: self.fake_args(), timeout=10), "the daemon never started the worker")
        line = self.fake_args()[0]
        self.assertTrue(line.startswith("worker"), line)
        self.assertNotIn("--master", line, "the config file must decide the master")
        self.assertNotIn("--simulate-gpu", line, "a phone must not advertise a simulated GPU by default")
        self.assertNotIn(":8080", line)

        state = os.path.join(self.home, ".local", "state", "oxideswarm")
        child_file = os.path.join(state, "worker.child.pid")
        self.assertTrue(wait_until(lambda: os.path.isfile(child_file), timeout=5), "the worker PID must be recorded")
        child = int(read(child_file).strip())
        self.assertTrue(pid_alive(child))

        stopped = self.run_script(["bash", script, "--stop"], PATH=path)
        self.assertEqual(stopped.returncode, 0, stopped.stdout)
        self.assertTrue(wait_until(lambda: not pid_alive(child), timeout=10),
                        "--stop must also terminate the worker process, not only the supervisor")

    def test_termux_supervisor_refuses_to_start_without_a_master(self):
        src = os.path.join(ROOT, "packaging", "android", "termux", "start_worker.sh")
        script = os.path.join(self.repo, "start_worker.sh")
        with open(script, "w") as f:
            f.write("#!/bin/bash\n" + read(src).split("\n", 1)[1])
        os.chmod(script, 0o755)
        bindir = os.path.join(self.home, "bin")
        os.makedirs(bindir)
        shutil.copy(os.path.join(self.repo, "target", "debug", "rusty-grid"), os.path.join(bindir, "rusty-grid"))
        path = bindir + os.pathsep + os.environ.get("PATH", "")
        res = self.run_script(["bash", script, "--foreground"], PATH=path)
        self.assertNotEqual(res.returncode, 0, "no master configured: the supervisor must not guess 127.0.0.1:8080")
        self.assertEqual(self.fake_args(), [])

    # ---- macOS installer ------------------------------------------------------------------

    @unittest.skipUnless(sys.platform == "darwin" or shutil.which("plutil") is None, "dry-run output is checked on any bash host")
    def test_mac_installer_runs_the_worker_as_an_unprivileged_user(self):
        if os.geteuid() == 0:
            self.skipTest("must not run as root")
        script = os.path.join(ROOT, "packaging", "macos", "install_mac_daemon.sh")
        res = self.run_script(["bash", script, "--dry-run", "--master", "192.0.2.1:8088", "--insecure", "--yes"])
        self.assertEqual(res.returncode, 0, res.stdout)
        out = res.stdout
        self.assertIn("<key>UserName</key>", out, "the plist must name the account that runs the worker")
        user = re.search(r"<key>UserName</key>\s*<string>([^<]+)</string>", out)
        self.assertIsNotNone(user)
        self.assertNotEqual(user.group(1), "root")
        self.assertNotIn("--master", out.split("ProgramArguments", 1)[1].split("</array>", 1)[0],
                         "no master address / ticket in the world-readable plist")
        self.assertNotIn(":8080", out)
        self.assertIn("<key>OXIDESWARM_GEN3D_SCRIPT</key>", out,
                      "checkout with engines/ must set OXIDESWARM_GEN3D_SCRIPT in the plist")

        # When installed from a layout without engines/, OXIDESWARM_GEN3D_SCRIPT must stay unset
        copied = self.copy_script("packaging/macos/install_mac_daemon.sh")
        no_eng = self.run_script(["bash", copied, "--dry-run", "--master", "192.0.2.1:8088", "--insecure", "--yes"])
        self.assertEqual(no_eng.returncode, 0, no_eng.stdout)
        self.assertNotIn("OXIDESWARM_GEN3D_SCRIPT", no_eng.stdout,
                         "install without engines/ must leave OXIDESWARM_GEN3D_SCRIPT unset")

    def test_agent_mesh_launchers_propagate_no_shell_option(self):
        """Verifies that ubuntu, macos, and termux launchers propagate --no-shell to the agent."""
        fake_sh = (
            "#!/bin/bash\n"
            'if [ "$1" = "-c" ]; then\n'
            "    exit 0\n"
            "fi\n"
            'echo "$*" >> "${FAKE_ARGS_LOG:?}"\n'
            "kill -9 $PPID 2>/dev/null || true\n"
            "exit 0\n"
        )
        fake_cmd = (
            "@echo off\r\n"
            'if "%~1"=="-c" exit /b 0\r\n'
            'echo %* >> "%FAKE_ARGS_LOG%"\r\n'
            "exit /b 0\r\n"
        )
        for name in ("python3", "python"):
            p = os.path.join(self.repo, name)
            with open(p, "w", newline="\n") as f:
                f.write(fake_sh)
            os.chmod(p, 0o755)

        for name in ("python3.cmd", "python.cmd", "python3.bat", "python.bat"):
            p = os.path.join(self.repo, name)
            with open(p, "w", newline="\r\n") as f:
                f.write(fake_cmd)

        test_path = self.repo + os.pathsep + os.environ.get("PATH", "")

        # 1. Test Ubuntu launcher with OXIDE_MESH_NO_SHELL=1
        ub_script = self.copy_script(
            "packaging/agent_mesh/ubuntu/run_agent_node.sh",
            "run_agent_node_ubuntu.sh",
        )
        self.run_script(
            ["bash", ub_script, "ws://127.0.0.1:8088/ws", "node-ub"],
            OXIDE_MESH_NO_SHELL="1",
            PATH=test_path,
        )
        lines = self.fake_args()
        self.assertTrue(any("node-ub" in l and "--no-shell" in l for l in lines), f"Ubuntu launcher did not pass --no-shell: {lines}")

        # 2. Test macOS launcher with --no-shell CLI argument
        mac_script = self.copy_script(
            "packaging/agent_mesh/macos/run_agent_node.sh",
            "run_agent_node_macos.sh",
        )
        self.run_script(
            ["bash", mac_script, "ws://127.0.0.1:8088/ws", "node-mac", "--no-shell"],
            PATH=test_path,
        )
        lines = self.fake_args()
        self.assertTrue(any("node-mac" in l and "--no-shell" in l for l in lines), f"macOS launcher did not pass --no-shell: {lines}")

        # 3. Test Termux launcher with --no-shell CLI argument
        termux_script = self.copy_script("packaging/agent_mesh/android/run_agent_node_termux.sh")
        self.run_script(
            ["bash", termux_script, "ws://127.0.0.1:8088/ws", "node-termux", "--no-shell"],
            PATH=test_path,
        )
        lines = self.fake_args()
        self.assertTrue(any("node-termux" in l and "--no-shell" in l for l in lines), f"Termux launcher did not pass --no-shell: {lines}")

        # 4. Test Ubuntu and macOS launchers when native binary ./target/debug/agent-mesh exists (line 61)
        native_bin = os.path.join(self.repo, "target", "debug", "agent-mesh")
        os.makedirs(os.path.dirname(native_bin), exist_ok=True)
        with open(native_bin, "w", newline="\n") as f:
            f.write(fake_sh)
        os.chmod(native_bin, 0o755)

        self.run_script(
            ["bash", ub_script, "ws://127.0.0.1:8088/ws", "node-ub-native"],
            OXIDE_MESH_NO_SHELL="1",
            PATH=test_path,
        )
        self.run_script(
            ["bash", mac_script, "ws://127.0.0.1:8088/ws", "node-mac-native", "--no-shell"],
            PATH=test_path,
        )
        lines = self.fake_args()
        self.assertTrue(
            any("node --hub ws://127.0.0.1:8088/ws --id node-ub-native --platform ubuntu --no-shell" in l for l in lines),
            f"Ubuntu launcher native binary path did not pass --no-shell: {lines}",
        )
        self.assertTrue(
            any("node --hub ws://127.0.0.1:8088/ws --id node-mac-native --platform macos --no-shell" in l for l in lines),
            f"macOS launcher native binary path did not pass --no-shell: {lines}",
        )

    def test_android_adb_launcher_keeps_token_off_the_adb_command_line(self):
        """SEC-15: the hub token reaches the phone through stdin, never through `adb` arguments."""
        token = "SEC15-secret-token-0123456789"
        bindir = os.path.join(self.tmp, "fakebin")
        os.makedirs(bindir)
        stdin_log = os.path.join(self.tmp, "fake_adb_stdin.log")
        fake_adb = (
            "#!/bin/bash\n"
            'echo "$*" >> "${FAKE_ARGS_LOG:?}"\n'
            'if [ "$1" = "devices" ]; then\n'
            '    printf "List of devices attached\\nSERIAL01\\tdevice\\n"\n'
            "    exit 0\n"
            "fi\n"
            'if [ "$1" = "shell" ]; then\n'
            '    cat >> "${FAKE_STDIN_LOG:?}" || true\n'
            "fi\n"
            "exit 0\n"
        )
        adb_path = os.path.join(bindir, "adb")
        with open(adb_path, "w", newline="\n") as f:
            f.write(fake_adb)
        os.chmod(adb_path, 0o755)
        target = os.path.join(self.repo, "target", "aarch64-linux-android", "release")
        os.makedirs(target)
        with open(os.path.join(target, "agent-mesh"), "w") as f:
            f.write("#!/bin/sh\n")
        script = self.copy_script("packaging/agent_mesh/android/run_agent_node_adb.sh")
        path = bindir + os.pathsep + os.environ.get("PATH", "")

        res = self.run_script(
            ["bash", script, "ws://192.0.2.10:8088/ws", "node-phone"],
            PATH=path, OXIDE_MESH_TOKEN=token, FAKE_STDIN_LOG=stdin_log,
        )
        self.assertEqual(res.returncode, 0, res.stdout)
        args_text = read(self.args_log)
        self.assertIn("node --hub 'ws://192.0.2.10:8088/ws'", args_text, "the agent must still be started")
        self.assertNotIn(token, args_text, "the token must not appear in any adb argument")
        self.assertNotIn("OXIDE_MESH_TOKEN=", args_text, "the token must not be an env assignment on the command line")
        self.assertNotIn(token, res.stdout, "the launcher must not print the token")
        self.assertIn(token, read(stdin_log), "the token must be handed over on standard input")

        # Without a token nothing is sent on stdin and the command still runs.
        open(self.args_log, "w").close()
        open(stdin_log, "w").close()
        res = self.run_script(
            ["bash", script, "ws://127.0.0.1:8088/ws"], PATH=path, FAKE_STDIN_LOG=stdin_log,
        )
        self.assertEqual(res.returncode, 0, res.stdout)
        self.assertIn("node --hub", read(self.args_log))
        self.assertEqual(read(stdin_log).strip(), "")


# A stand-in for the released binary inside the USB-kit archives (packaging/usb). It records its
# arguments, checks that `join` gets the token on standard input, and answers `workers --json` with
# the worker it runs, in the layout the installer parses.
FAKE_KIT_BINARY = """#!/bin/bash
echo "$*" >> "${FAKE_ARGS_LOG:?}"
case "$1" in workers|doctor) echo "$1 RUST_LOG=${RUST_LOG:-}" >> "$FAKE_ARGS_LOG.env" ;; esac
case "$1" in
  --version) echo "rusty-grid 9.9.9"; exit 0 ;;
  join)
    shift
    invite_file=""
    while [ $# -gt 0 ]; do
      case "$1" in
        --invite-file) invite_file=$2; shift 2 ;;
        *) shift ;;
      esac
    done
    if [ -n "$invite_file" ]; then
      [ -f "$invite_file" ] || { echo "missing invite file: $invite_file" >&2; exit 3; }
      IFS= read -r pass || true
      pass=$(printf '%s' "$pass" | tr -d '\\r')
      if [ -z "$pass" ] || [ "$pass" = "wrong-passphrase" ]; then
        echo "Cannot unlock $invite_file: decryption failed" >&2
        exit 3
      fi
      mkdir -p "$OXIDESWARM_HOME"
      printf 'fake-node-key\\n' > "$OXIDESWARM_HOME/node.key"
      chmod 600 "$OXIDESWARM_HOME/node.key"
      mkey="${FAKE_MASTER_KEY:-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef}"
      {
        printf "[worker]\\nmaster_key = '%s'\\n" "$mkey"
        if [ -n "${FAKE_TICKET:-}" ]; then
          printf "p2p_ticket = '%s'\\n" "$FAKE_TICKET"
        fi
      } > "$OXIDESWARM_HOME/rusty-grid.toml"
      touch "$OXIDESWARM_HOME/joined"
      exit 0
    fi
    tok=$(cat)
    case "$tok" in
      oxs1-join:*)
        touch "$OXIDESWARM_HOME/joined"
        if [ -n "${FAKE_TICKET:-}" ]; then
          printf "[worker]\\np2p_ticket = '%s'\\n" "$FAKE_TICKET" > "$OXIDESWARM_HOME/rusty-grid.toml"
        fi
        exit 0 ;;
      *) echo "bad token" >&2; exit 3 ;;
    esac ;;
  worker)
    shift
    iso=""
    while [ $# -gt 0 ]; do
      [ "$1" = "--name" ] && echo "$2" > "$OXIDESWARM_HOME/fake_worker_name"
      [ "$1" = "--isolation" ] && iso=$2
      shift
    done
    echo $$ > "$OXIDESWARM_HOME/fake_worker_pid"
    # FAKE_WORKER_STOP_WITH_ISOLATION=fs: stop at once under that isolation (a kernel without it).
    if [ -n "${FAKE_WORKER_STOP_WITH_ISOLATION:-}" ] && [ "$iso" = "$FAKE_WORKER_STOP_WITH_ISOLATION" ]; then
      echo "fake worker: isolation $iso is not available here"
      exit 4
    fi
    # FAKE_WORKER_EXIT_AFTER=N: write 40 log lines, run N seconds, then stop with FAKE_WORKER_EXIT_CODE.
    if [ -n "${FAKE_WORKER_EXIT_AFTER:-}" ]; then
      n=1
      while [ $n -le 40 ]; do printf 'fake-log-line-%02d\\n' "$n"; n=$((n + 1)); done
      sleep "$FAKE_WORKER_EXIT_AFTER"
      echo "fake worker: FAKE-STOP-MARKER the sandbox base is refused"
      exit "${FAKE_WORKER_EXIT_CODE:-1}"
    fi
    while true; do sleep 1; done ;;
  workers)
    if [ -n "${FAKE_WORKERS_EMPTY:-}" ]; then echo "[]"; exit 0; fi
    pid=$(cat "$OXIDESWARM_HOME/fake_worker_pid" 2>/dev/null || true)
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      printf '[\\n  {\\n    "name": "%s",\\n    "tags": [\\n      "state:connected"\\n    ]\\n  }\\n]\\n' "$(cat "$OXIDESWARM_HOME/fake_worker_name")"
    else
      echo "[]"
    fi
    exit 0 ;;
  doctor) echo "Summary: fake"; exit 0 ;;
esac
exit 0
"""

KIT_TARGETS = ("aarch64-apple-darwin.tar.gz", "x86_64-apple-darwin.tar.gz",
               "x86_64-pc-windows-msvc.zip", "x86_64-unknown-linux-gnu.tar.gz")
# The release also carries a gpu-wgpu build of each target, named <target>-gpu.<ext>.
KIT_GPU_TARGETS = tuple(t.replace(".tar.gz", "-gpu.tar.gz").replace(".zip", "-gpu.zip") for t in KIT_TARGETS)
KIT_TOKEN = "oxs1-join:" + "QUJD" * 10 + "." + "0" * 32


@unittest.skipIf(IS_WINDOWS, "the bash kit scripts are tested on Linux and macOS; the .cmd path runs on Windows")
class UsbKitTests(unittest.TestCase):
    """packaging/usb: make_kit.sh writes a kit, install.sh installs from it, uninstall.sh removes it."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="ox_usbkit_test_")
        self.home = os.path.join(self.tmp, "home")
        os.makedirs(self.home)
        self.args_log = os.path.join(self.tmp, "fake_args.log")
        open(self.args_log, "w").close()
        self.addCleanup(self._cleanup)
        # Release-like archives with a fake binary, and their SHA256SUMS.
        self.archives = os.path.join(self.tmp, "archives")
        os.makedirs(self.archives)
        sums = []
        for target in KIT_TARGETS + KIT_GPU_TARGETS:
            name = "oxideswarm-9.9.9-" + target
            path = os.path.join(self.archives, name)
            if target.endswith(".tar.gz"):
                stage = os.path.join(self.tmp, "stage", name[:-len(".tar.gz")])
                os.makedirs(stage)
                exe = os.path.join(stage, "oxideswarm")
                binary = FAKE_KIT_BINARY
                if "-gpu." in target:
                    binary = binary.replace('echo "rusty-grid 9.9.9"', 'echo "rusty-grid 9.9.9 gpu-build"')
                with open(exe, "w") as f:
                    f.write(binary)
                os.chmod(exe, 0o755)
                subprocess.run(["tar", "-czf", path, "-C", os.path.dirname(stage), os.path.basename(stage)], check=True)
            else:
                with open(path, "wb") as f:
                    f.write(b"not used on this system")
            sums.append("%s  %s" % (hashlib.sha256(read_bytes(path)).hexdigest(), name))
        with open(os.path.join(self.archives, "SHA256SUMS"), "w") as f:
            f.write("\n".join(sums) + "\n")
        self.stick = os.path.join(self.tmp, "stick")
        os.makedirs(self.stick)

    def _cleanup(self):
        listing = subprocess.run(["ps", "-axo", "pid=,command="], stdout=subprocess.PIPE, text=True).stdout
        for line in listing.splitlines():
            pid_text, _, command = line.strip().partition(" ")
            if self.tmp in command and pid_text.isdigit() and int(pid_text) != os.getpid():
                try:
                    os.kill(int(pid_text), signal.SIGKILL)
                except OSError:
                    pass
        shutil.rmtree(self.tmp, ignore_errors=True)

    def env(self):
        return {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "HOME": self.home, "TMPDIR": self.tmp,
                "FAKE_ARGS_LOG": self.args_log, "OXIDESWARM_USB_ONLINE_SUMS_URL": "off"}

    def make_kit(self, token=KIT_TOKEN, extra=()):
        return subprocess.run(
            ["bash", os.path.join(ROOT, "packaging", "usb", "make_kit.sh"), self.stick,
             "--version", "9.9.9", "--archives", self.archives] + list(extra),
            input=token, env=self.env(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=60)

    def test_make_kit_writes_the_kit_and_keeps_the_token_off_the_command_line(self):
        res = self.make_kit()
        self.assertEqual(res.returncode, 0, res.stdout)
        kit = os.path.join(self.stick, "OxideSwarm")
        for name in ("install.sh", "install.ps1", "Install OxideSwarm.cmd", "Install OxideSwarm.command",
                     "Update OxideSwarm.cmd", "Update OxideSwarm.command",
                     "README.txt", "archives/SHA256SUMS"):
            self.assertTrue(os.path.isfile(os.path.join(kit, name)), name)
        token_path = os.path.join(kit, "join.token")
        self.assertEqual(read(token_path).strip(), KIT_TOKEN)
        self.assertEqual(stat.S_IMODE(os.stat(token_path).st_mode) & 0o077, 0)
        self.assertNotIn(KIT_TOKEN, res.stdout)
        self.assertEqual(glob.glob(os.path.join(kit, "._*")), [])
        # A second run refuses to overwrite, and anything that is not a token is refused.
        self.assertNotEqual(self.make_kit().returncode, 0)
        self.assertNotEqual(self.make_kit(token="hello", extra=["--replace"]).returncode, 0)

    def test_install_and_uninstall_from_the_kit_stay_in_their_folder(self):
        self.assertEqual(self.make_kit().returncode, 0)
        kit = os.path.join(self.stick, "OxideSwarm")
        root = os.path.join(self.tmp, "install-root")
        res = subprocess.run(["bash", os.path.join(kit, "install.sh"), "--install-root", root,
                              "--no-autostart", "--name", "kit-test"],
                             env=self.env(), stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True, timeout=90)
        self.assertEqual(res.returncode, 0, res.stdout)
        self.assertIn("checksum OK", res.stdout)
        self.assertIn("connected", res.stdout)
        calls = [line for line in read(self.args_log).splitlines() if line.strip()]
        self.assertIn("join --no-doctor", calls)
        worker_calls = [c for c in calls if c.startswith("worker ")]
        self.assertTrue(worker_calls and "--name kit-test" in worker_calls[0] and "--no-portal" in worker_calls[0],
                        calls)
        self.assertTrue(all(KIT_TOKEN not in c for c in calls), "the token reached a command line")
        self.assertTrue(os.path.isfile(os.path.join(root, "home", "joined")))
        # The user's own state folder and autostart folders are untouched.
        self.assertFalse(os.path.exists(os.path.join(self.home, ".oxideswarm")))
        self.assertFalse(os.path.exists(os.path.join(self.home, "Library", "LaunchAgents")))
        pid = int(read(os.path.join(root, "run", "worker.pid")).strip())
        self.assertTrue(pid_alive(pid))
        res = subprocess.run(["bash", os.path.join(kit, "uninstall.sh"), "--install-root", root, "--yes"],
                             env=self.env(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=60)
        self.assertEqual(res.returncode, 0, res.stdout)
        self.assertFalse(os.path.exists(root))
        self.assertTrue(wait_until(lambda: not pid_alive(pid), timeout=10), "the worker kept running")

    def test_install_uses_the_saved_p2p_ticket_for_its_checks(self):
        # On a network where the machines cannot reach each other (client isolation), the worker
        # goes through the ticket that join saved, and so must the installer's checks.
        self.assertEqual(self.make_kit().returncode, 0)
        kit = os.path.join(self.stick, "OxideSwarm")
        root = os.path.join(self.tmp, "install-root")
        ticket = '{"id":"00ff","addrs":[]}'
        env = dict(self.env(), FAKE_TICKET=ticket)
        res = subprocess.run(["bash", os.path.join(kit, "install.sh"), "--install-root", root,
                              "--no-autostart", "--name", "kit-p2p"],
                             env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True, timeout=90)
        self.assertEqual(res.returncode, 0, res.stdout)
        self.assertIn("P2P ticket", res.stdout)
        calls = [line for line in read(self.args_log).splitlines() if line.strip()]
        self.assertIn("workers --json --master " + ticket, calls)
        self.assertIn("doctor --master " + ticket, calls)

    def test_install_checks_run_without_log_lines(self):
        # Release 0.1.x writes informational logs to standard output: the installer's checks
        # must run with RUST_LOG=off, or the JSON listing and the doctor output carry log lines.
        self.assertEqual(self.make_kit().returncode, 0)
        kit = os.path.join(self.stick, "OxideSwarm")
        root = os.path.join(self.tmp, "install-root")
        res = subprocess.run(["bash", os.path.join(kit, "install.sh"), "--install-root", root,
                              "--no-autostart", "--name", "kit-quiet"],
                             env=self.env(), stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True, timeout=90)
        self.assertEqual(res.returncode, 0, res.stdout)
        calls = [line for line in read(self.args_log + ".env").splitlines() if line.strip()]
        self.assertTrue(any(c.startswith("workers ") for c in calls), calls)
        self.assertTrue(any(c.startswith("doctor ") for c in calls), calls)
        for call in calls:
            self.assertTrue(call.endswith("RUST_LOG=off"), call)

    def test_windows_installer_escapes_the_ticket_and_silences_logs(self):
        # Windows PowerShell 5.1 drops the inner double quotes of a native argument, which turns
        # the JSON ticket into an unreadable master address. The fix is static text here; the
        # live check runs on a Windows machine.
        text = read(os.path.join(ROOT, "packaging", "usb", "install.ps1"))
        self.assertIn("PSNativeCommandArgumentPassing", text)
        self.assertIn("$ticket.Replace('\"', '\\\"')", text)
        self.assertIn("$checkArgs = @('--master', $ticketArg)", text)
        start = text.index("Start-Process -FilePath 'wscript.exe'")
        quiet = text.index("$env:RUST_LOG = 'off'")
        listing = text.index("workers --json @checkArgs")
        self.assertTrue(start < quiet < listing, "RUST_LOG=off must be set after the worker starts and before the checks")

    def test_trial_kit_adds_no_autostart_and_caps_concurrency(self):
        # A kit for someone else's machine (make_kit.sh --no-autostart) must not add a start at
        # login, and the worker must leave half the cores free.
        self.assertEqual(self.make_kit(extra=["--no-autostart"]).returncode, 0)
        kit = os.path.join(self.stick, "OxideSwarm")
        self.assertTrue(os.path.isfile(os.path.join(kit, "no-autostart")))
        root = os.path.join(self.tmp, "install-root")
        res = subprocess.run(["bash", os.path.join(kit, "install.sh"), "--install-root", root, "--name", "kit-trial"],
                             env=self.env(), stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True, timeout=90)
        self.assertEqual(res.returncode, 0, res.stdout)
        self.assertIn("autostart: skipped", res.stdout)
        self.assertFalse(os.path.exists(os.path.join(self.home, "Library", "LaunchAgents")))
        self.assertFalse(os.path.exists(os.path.join(self.home, ".config", "systemd")))
        worker_calls = [c for c in read(self.args_log).splitlines() if c.startswith("worker ")]
        self.assertTrue(worker_calls and "--max-concurrency " in worker_calls[0], worker_calls)

    def install_with_cpu(self, cpu_info, *args, root_name="install-root"):
        root = os.path.join(self.tmp, root_name)
        env = self.env()
        env["OXIDESWARM_USB_CPU_INFO"] = cpu_info
        res = subprocess.run(["bash", os.path.join(self.stick, "OxideSwarm", "install.sh"), "--install-root", root,
                              "--no-autostart", "--name", "kit-slots"] + list(args),
                             env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True, timeout=90)
        worker_calls = [c for c in read(self.args_log).splitlines() if c.startswith("worker ")]
        open(self.args_log, "w").close()
        return root, res, worker_calls

    def test_install_sizes_the_slots_to_the_performance_cores(self):
        # USB-4: on 2026-11-10 an i7-1355U laptop (2 performance cores, 4 of its 12 threads)
        # with half its threads as slots ran 6 concurrent tasks at 39.6 s each against 2.9 s
        # for 2. The default now follows the performance cores, at most half of all threads.
        self.assertEqual(self.make_kit().returncode, 0)
        cases = [
            ("total=12 perf=4 battery=1", "4", "performance cores"),   # i7-1355U laptop
            ("total=20 perf=12 battery=0", "10", "performance cores"),  # i5-14600KF: capped at half
            ("total=8 perf=8 battery=0", "4", "performance cores"),    # one core type: half, as before
            ("total=12 perf=- battery=1", "3", "a quarter"),           # unknown core types, battery
            ("total=8 perf=- battery=0", "4", "half of 8"),            # unknown core types, no battery
            ("total=1 perf=- battery=1", "1", "a quarter"),
        ]
        for i, (cpu, slots, why) in enumerate(cases):
            root, res, calls = self.install_with_cpu(cpu, root_name="root-%d" % i)
            self.assertEqual(res.returncode, 0, res.stdout)
            self.assertIn("slots:   %s (%s" % (slots, why), res.stdout)
            self.assertTrue(calls and "--max-concurrency %s " % slots in calls[0] + " ", (cpu, calls))
            self.assertIn("slots=\n", read(os.path.join(root, "kit-install.conf")))

    def test_slots_flag_overrides_and_survives_an_update(self):
        self.assertEqual(self.make_kit().returncode, 0)
        root, res, calls = self.install_with_cpu("total=12 perf=4 battery=1", "--slots", "2")
        self.assertEqual(res.returncode, 0, res.stdout)
        self.assertIn("slots:   2 (set with --slots", res.stdout)
        self.assertTrue(calls and "--max-concurrency 2 " in calls[0] + " ", calls)
        self.assertIn("slots=2\n", read(os.path.join(root, "kit-install.conf")))
        # The update keeps a value chosen with --slots, even where the rule would pick another.
        env = self.env()
        env["OXIDESWARM_USB_CPU_INFO"] = "total=20 perf=12 battery=0"
        res = subprocess.run(["bash", os.path.join(self.stick, "OxideSwarm", "install.sh"), "--install-root", root,
                              "--update"], env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True, timeout=90)
        self.assertEqual(res.returncode, 0, res.stdout)
        worker_calls = [c for c in read(self.args_log).splitlines() if c.startswith("worker ")]
        self.assertTrue(worker_calls and "--max-concurrency 2 " in worker_calls[0] + " ", worker_calls)
        for bad in ("0", "257", "abc"):
            _, res, _ = self.install_with_cpu("total=8 perf=- battery=0", "--slots", bad, root_name="bad-" + bad)
            self.assertNotEqual(res.returncode, 0, res.stdout)
            self.assertIn("--slots needs a number from 1 to 256", res.stdout)

    def test_windows_installer_sizes_the_slots_to_the_performance_cores(self):
        # The same rule in install.ps1 (USB-4); the live check runs on a Windows machine.
        text = read(os.path.join(ROOT, "packaging", "usb", "install.ps1"))
        self.assertIn("GetSystemCpuSetInformation", text)
        self.assertIn("ReadByte(buffer, offset + 18)", text)  # SYSTEM_CPU_SET_INFORMATION EfficiencyClass
        self.assertIn("OXIDESWARM_USB_CPU_INFO", text)
        self.assertIn("[int]$Slots = 0", text)
        self.assertIn("-Slots needs a number from 1 to 256", text)
        self.assertIn("$slots = [Math]::Min([int]$cpu.perf, $half)", text)
        self.assertIn("[Math]::Floor($cpuTotal / 4)", text)
        self.assertIn('"slots=$recordedSlots"', text)
        self.assertIn("$settings['slots']", text)
        self.assertNotIn("[Environment]::ProcessorCount / 2", text)

    def test_windows_installers_compare_full_paths_of_localappdata(self):
        # Windows PowerShell 5.1's GetFullPath expands 8.3 short names (a CI runner's
        # C:\\Users\\RUNNER~1), so %LOCALAPPDATA% must go through it before any comparison with an
        # install folder that did; otherwise a folder in %LOCALAPPDATA% looked like one outside it.
        text = read(os.path.join(ROOT, "packaging", "usb", "install.ps1"))
        self.assertIn("$LocalAppDataFull = [IO.Path]::GetFullPath($env:LOCALAPPDATA).TrimEnd('\\')", text)
        self.assertIn("$InstallRoot.StartsWith($LocalAppDataFull + '\\'", text)
        uninstall = read(os.path.join(ROOT, "packaging", "usb", "uninstall.ps1"))
        self.assertIn("[IO.Path]::GetFullPath((Join-Path $env:LOCALAPPDATA 'OxideSwarm'))", uninstall)

    def install(self, *args):
        root = os.path.join(self.tmp, "install-root")
        return root, subprocess.run(["bash", os.path.join(self.stick, "OxideSwarm", "install.sh"), "--install-root", root,
                                     "--no-autostart", "--name", "kit-gpu"] + list(args),
                                    env=self.env(), stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True, timeout=90)

    def test_make_kit_adds_the_gpu_builds_only_with_gpu(self):
        self.assertEqual(self.make_kit().returncode, 0)
        names = os.listdir(os.path.join(self.stick, "OxideSwarm", "archives"))
        self.assertEqual([n for n in names if "-gpu." in n], [])
        res = self.make_kit(extra=["--replace", "--gpu"])
        self.assertEqual(res.returncode, 0, res.stdout)
        names = os.listdir(os.path.join(self.stick, "OxideSwarm", "archives"))
        for target in KIT_TARGETS + KIT_GPU_TARGETS:
            self.assertIn("oxideswarm-9.9.9-" + target, names)

    def test_install_takes_the_gpu_build_with_gpu(self):
        self.assertEqual(self.make_kit(extra=["--gpu"]).returncode, 0)
        root, res = self.install("--gpu")
        self.assertEqual(res.returncode, 0, res.stdout)
        self.assertIn("gpu:     yes", res.stdout)
        self.assertIn("-gpu.tar.gz (checksum OK)", res.stdout)
        self.assertIn("gpu-build", res.stdout)

    def test_install_without_a_terminal_keeps_the_cpu_build(self):
        # Nobody can be asked, so the GPU is not offered unless --gpu says so.
        self.assertEqual(self.make_kit(extra=["--gpu"]).returncode, 0)
        root, res = self.install()
        self.assertEqual(res.returncode, 0, res.stdout)
        self.assertIn("gpu:     no", res.stdout)
        self.assertNotIn("gpu-build", res.stdout)

    def install_on_a_terminal(self, answer):
        # The question needs a terminal on standard input; a pseudo-terminal stands in for one.
        import pty
        root = os.path.join(self.tmp, "install-root")
        argv = ["bash", os.path.join(self.stick, "OxideSwarm", "install.sh"), "--install-root", root,
                "--no-autostart", "--name", "kit-tty"]
        pid, fd = pty.fork()
        if pid == 0:
            os.execve("/bin/bash", argv, self.env())
        out, sent, deadline = b"", False, time.time() + 90
        while time.time() < deadline:
            try:
                chunk = os.read(fd, 4096)
            except OSError:
                break
            if not chunk:
                break
            out += chunk
            if not sent and b"[Y/n]" in out:
                os.write(fd, answer)
                sent = True
        os.close(fd)
        _, status = os.waitpid(pid, 0)
        return os.WEXITSTATUS(status) if os.WIFEXITED(status) else -1, out.decode("utf-8", "replace")

    def test_install_asks_about_the_gpu_on_a_terminal(self):
        self.assertEqual(self.make_kit(extra=["--gpu"]).returncode, 0)
        code, out = self.install_on_a_terminal(b"n\n")
        self.assertEqual(code, 0, out)
        self.assertIn("[Y/n]", out)
        self.assertIn("gpu:     no", out)
        self.assertNotIn("gpu-build", out)
        subprocess.run(["bash", os.path.join(self.stick, "OxideSwarm", "uninstall.sh"), "--install-root",
                        os.path.join(self.tmp, "install-root"), "--yes"], env=self.env(),
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=60)
        code, out = self.install_on_a_terminal(b"\n")
        self.assertEqual(code, 0, out)
        self.assertIn("gpu:     yes", out)
        self.assertIn("gpu-build", out)

    def test_install_gpu_needs_a_gpu_build_on_the_kit(self):
        self.assertEqual(self.make_kit().returncode, 0)
        root, res = self.install("--gpu")
        self.assertNotEqual(res.returncode, 0)
        self.assertIn("no GPU build", res.stdout)
        self.assertFalse(os.path.exists(os.path.join(root, "bin", "oxideswarm")))
        root, res = self.install("--gpu", "--no-gpu")
        self.assertNotEqual(res.returncode, 0)

    def test_windows_installer_offers_the_gpu_build(self):
        # Static text: the live check runs on a Windows machine.
        text = read(os.path.join(ROOT, "packaging", "usb", "install.ps1"))
        self.assertIn('-Filter "oxideswarm-*-$Target-gpu.zip"', text)
        self.assertIn("[Console]::IsInputRedirected", text)
        self.assertIn("$Archive = $GpuArchive", text)
        choice = text.index("$Archive = $GpuArchive")
        check = text.index("Get-FileHash $Archive.FullName")
        self.assertTrue(choice < check, "the GPU choice must come before the checksum check")

    def update(self, *args):
        root = os.path.join(self.tmp, "install-root")
        return subprocess.run(["bash", os.path.join(self.stick, "OxideSwarm", "install.sh"), "--install-root", root,
                               "--update"] + list(args),
                              env=self.env(), stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True, timeout=90)

    def test_update_keeps_the_name_the_build_and_the_membership(self):
        self.assertEqual(self.make_kit(extra=["--gpu"]).returncode, 0)
        root, res = self.install("--gpu")
        self.assertEqual(res.returncode, 0, res.stdout)
        old_pid = int(read(os.path.join(root, "run", "worker.pid")).strip())
        open(self.args_log, "w").close()
        res = self.update()
        self.assertEqual(res.returncode, 0, res.stdout)
        self.assertIn("(update)", res.stdout)
        self.assertIn("Updated. Worker 'kit-gpu'", res.stdout)
        self.assertIn("-gpu.tar.gz (checksum OK)", res.stdout)
        self.assertIn("gpu-build", res.stdout)
        self.assertNotIn("[Y/n]", res.stdout)
        calls = [c for c in read(self.args_log).splitlines() if c.strip()]
        self.assertFalse(any(c.startswith("join") for c in calls), calls)
        self.assertFalse(any(c.startswith("doctor") for c in calls), calls)
        worker_calls = [c for c in calls if c.startswith("worker ")]
        self.assertTrue(worker_calls and "--name kit-gpu" in worker_calls[0], calls)
        new_pid = int(read(os.path.join(root, "run", "worker.pid")).strip())
        self.assertNotEqual(old_pid, new_pid)
        self.assertTrue(wait_until(lambda: not pid_alive(old_pid), timeout=10), "the old worker kept running")
        self.assertTrue(pid_alive(new_pid))
        self.assertIn("gpu=1", read(os.path.join(root, "kit-install.conf")))

    def test_update_of_an_older_install_takes_the_name_from_its_worker_id_file(self):
        # Installs made before the update mode have no kit-install.conf: the name comes from the
        # worker's id file, and without a terminal the plain build is kept.
        self.assertEqual(self.make_kit(extra=["--gpu"]).returncode, 0)
        root, res = self.install()
        self.assertEqual(res.returncode, 0, res.stdout)
        os.remove(os.path.join(root, "kit-install.conf"))
        with open(os.path.join(root, "home", "worker_id.kit-legacy"), "w") as f:
            f.write("00000000-0000-0000-0000-000000000000\n")
        open(self.args_log, "w").close()
        res = self.update()
        self.assertEqual(res.returncode, 0, res.stdout)
        worker_calls = [c for c in read(self.args_log).splitlines() if c.startswith("worker ")]
        self.assertTrue(worker_calls and "--name kit-legacy" in worker_calls[0], worker_calls)
        self.assertNotIn("gpu-build", res.stdout)

    def test_update_needs_an_install(self):
        self.assertEqual(self.make_kit().returncode, 0)
        res = self.update()
        self.assertNotEqual(res.returncode, 0)
        self.assertIn("run the installer first", res.stdout)
        self.assertEqual([c for c in read(self.args_log).splitlines() if c.strip()], [])

    def test_update_of_a_gpu_install_refuses_a_kit_without_the_gpu_build(self):
        self.assertEqual(self.make_kit(extra=["--gpu"]).returncode, 0)
        root, res = self.install("--gpu")
        self.assertEqual(res.returncode, 0, res.stdout)
        self.assertEqual(self.make_kit(extra=["--replace"]).returncode, 0)
        res = self.update()
        self.assertNotEqual(res.returncode, 0)
        self.assertIn("lacks", res.stdout)
        self.assertEqual(self.update("--no-gpu").returncode, 0)

    def install_as(self, machine, name, root_name="install-root"):
        root = os.path.join(self.tmp, root_name)
        env = dict(self.env(), OXIDESWARM_USB_MACHINE_ID=machine)
        res = subprocess.run(["bash", os.path.join(self.stick, "OxideSwarm", "install.sh"), "--install-root", root,
                              "--no-autostart", "--name", name],
                             env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True, timeout=90)
        self.assertEqual(res.returncode, 0, res.stdout)
        return root, read(os.path.join(root, "home", "worker_id." + name)).strip()

    def uninstall_root(self, root):
        res = subprocess.run(["bash", os.path.join(self.stick, "OxideSwarm", "uninstall.sh"), "--install-root", root, "--yes"],
                             env=self.env(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=60)
        self.assertEqual(res.returncode, 0, res.stdout)

    def test_a_reinstall_keeps_the_worker_id_of_the_machine(self):
        # A reinstall must show up as the same worker, not as a new one beside a disconnected entry.
        self.assertEqual(self.make_kit().returncode, 0)
        root, first = self.install_as("machine-a", "kit-id")
        self.assertRegex(first, r"^[0-9a-f]{8}-[0-9a-f]{4}-8[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
        self.assertNotIn("machine-a", first)
        self.uninstall_root(root)
        root, again = self.install_as("machine-a", "kit-id")
        self.assertEqual(first, again)
        self.uninstall_root(root)
        _, other_machine = self.install_as("machine-b", "kit-id", "root-b")
        _, other_name = self.install_as("machine-a", "kit-id-2", "root-c")
        self.assertEqual(len({first, other_machine, other_name}), 3)

    def test_an_existing_worker_id_is_kept(self):
        self.assertEqual(self.make_kit().returncode, 0)
        root, _ = self.install_as("machine-a", "kit-keep")
        id_file = os.path.join(root, "home", "worker_id.kit-keep")
        with open(id_file, "w") as f:
            f.write("11111111-2222-4333-8444-555555555555\n")
        self.assertEqual(self.update().returncode, 0)
        self.assertEqual(read(id_file).strip(), "11111111-2222-4333-8444-555555555555")

    def test_windows_installer_derives_the_worker_id_from_the_machine(self):
        text = read(os.path.join(ROOT, "packaging", "usb", "install.ps1"))
        for needle in ("MachineGuid", "oxideswarm-usb-worker-id-v1", 'Join-Path $State "worker_id.$Name"',
                       "($h[6] -band 0x0f) -bor 0x80", "($h[8] -band 0x3f) -bor 0x80"):
            self.assertIn(needle, text)
        self.assertLess(text.index("oxideswarm-usb-worker-id-v1"), text.index("Start-Process -FilePath 'wscript.exe'"))

    def test_windows_installer_refuses_folders_another_account_controls(self):
        # Review M1: the folder gets a DACL built here (protected, three trusted SIDs, owner = this
        # user), a new one is created with it in one call, an existing one must belong to this user
        # and hold nothing another account may change, the folders on the way to it must not be
        # renamable by others, the result is verified, and an update runs the same checks.
        text = read(os.path.join(ROOT, "packaging", "usb", "install.ps1"))
        # The installer never runs icacls (it may print a command for the user, see BUG-31).
        self.assertNotRegex(text, r"(?mi)^\s*(&\s*)?['\"]?icacls(\.exe)?['\"]?\s")
        self.assertNotRegex(text, r"(?i)Start-Process[^\n]*icacls")
        for needle in ("SetAccessRuleProtection($true, $false)", "$sec.SetOwner($UserSid)",
                       "[IO.Directory]::CreateDirectory($InstallRoot, $sec)", "function Test-RootSafety",
                       "belongs to another account", "can rename or change", "$RenameRights",
                       "could not make $InstallRoot private",
                       "files appeared in $InstallRoot before it could be made private"):
            self.assertIn(needle, text)
        # Run for an update as well: directly in the `if (-not $PrivateRoot)` block, not nested deeper.
        check = text.index("\n    $why = Test-RootSafety $InstallRoot\n")
        self.assertTrue(text.rindex("if (-not $PrivateRoot) {", 0, check) > text.rindex("\n}", 0, check))
        self.assertLess(check, text.index("[IO.Directory]::CreateDirectory($InstallRoot, $sec)"))
        self.assertIn("'^[A-Za-z0-9_.-]{1,64}$'", text)
        # S6: an update keeps the isolation of the existing launcher when kit-install.txt lacks it.
        self.assertIn("'--isolation (off|fs)\\b'", text)
        # S5: the uninstaller deletes only a marked folder without links.
        uninstall = read(os.path.join(ROOT, "packaging", "usb", "uninstall.ps1"))
        delete = uninstall.index("Remove-Item -Recurse -Force -LiteralPath $InstallRoot")
        for needle in (".oxideswarm-usb-install", "contains links", "is a link; not touching it"):
            self.assertLess(uninstall.index(needle), delete, needle)

    def test_a_stale_pid_file_never_stops_another_process(self):
        # Review S7: a PID file can outlive a reboot; its number may then name any process.
        self.assertEqual(self.make_kit().returncode, 0)
        root, res = self.install()
        self.assertEqual(res.returncode, 0, res.stdout)
        decoy = subprocess.Popen(["/bin/sleep", "300"])
        self.addCleanup(decoy.kill)
        worker = int(read(os.path.join(root, "run", "worker.pid")).strip())
        self.addCleanup(lambda: pid_alive(worker) and os.kill(worker, signal.SIGKILL))
        with open(os.path.join(root, "run", "worker.pid"), "w") as f:
            f.write("%d\n" % decoy.pid)
        self.assertEqual(self.update().returncode, 0)
        self.assertIsNone(decoy.poll(), "the update stopped a process that is not this install's worker")
        with open(os.path.join(root, "run", "worker.pid"), "w") as f:
            f.write("%d\n" % decoy.pid)
        self.uninstall_root(root)
        self.assertIsNone(decoy.poll(), "the uninstaller stopped a process that is not this install's worker")

    def test_launchers_offer_to_eject_the_stick(self):
        # macOS: the prompt runs in a new shell after `cd /`, so the script read from the stick does
        # not keep the volume busy (checked by hand with a mounted exFAT image).
        for name in ("Install OxideSwarm.command", "Update OxideSwarm.command", "Uninstall OxideSwarm.command"):
            text = read(os.path.join(ROOT, "packaging", "usb", name))
            self.assertIn('diskutil eject "$1"', text, name)
            self.assertLess(text.index("cd / ||"), text.index("exec /bin/bash -c"), name)
        # Windows: cmd reads the batch file line by line from the stick, so everything after the
        # question sits on one line that is parsed before the eject.
        for name in ("Install OxideSwarm.cmd", "Update OxideSwarm.cmd", "Uninstall OxideSwarm.cmd"):
            text = read(os.path.join(ROOT, "packaging", "usb", name))
            lines = text.replace("\r\n", "\n").split("\n")
            last = [l for l in lines if l.startswith("if errorlevel 2 (")]
            self.assertEqual(len(last), 1, name)
            for part in ("InvokeVerb('Eject')", "pause", "exit /b %status%"):
                self.assertIn(part, last[0], name)
            self.assertEqual([l for l in lines[lines.index(last[0]) + 1:] if l.strip()], [], name)
            self.assertLess(text.index('cd /d "%SystemRoot%"'), text.index("choice /c YN"), name)

    def test_windows_installer_updates_in_place(self):
        # Static text: the live check runs on a Windows machine.
        text = read(os.path.join(ROOT, "packaging", "usb", "install.ps1"))
        for needle in ("[switch]$Update", "if (-not $Update) {", "kit-install.txt", "worker_id.*",
                       "Updated. Worker", "if (-not $Update -and -not (Test-Path $TokenFile))"):
            self.assertIn(needle, text)
        join = text.index("& $Exe join --no-doctor")
        self.assertIn("if (-not $Update) {", text[join - 200:join])
        self.assertTrue(text.index('Write-Host "Updated. Worker') < text.index("& $Exe doctor"),
                        "an update must end before the doctor check")
        launcher = read(os.path.join(ROOT, "packaging", "usb", "Update OxideSwarm.cmd"))
        self.assertIn("install.ps1\" -Update", launcher)

    def test_windows_installer_keeps_another_drive_private(self):
        # Static text: the live check runs on a Windows machine. An install outside %LOCALAPPDATA%
        # must sit on a fixed NTFS or ReFS drive other than the kit's, be limited to this user,
        # SYSTEM and Administrators before anything is written into it, and keep the task
        # folders and the file cache inside it.
        text = read(os.path.join(ROOT, "packaging", "usb", "install.ps1"))
        for needle in ("[IO.DriveInfo]::GetDrives()", "@('NTFS', 'ReFS')", "$drive.DriveType -ne 'Fixed'",
                       "it is on the drive this kit runs from", "not the root of a drive",
                       "already holds other files", "[Security.Principal.WindowsIdentity]::GetCurrent().User",
                       "SetAccessRuleProtection($true, $false)", "$SandboxBase = if ($PrivateRoot -and $Isolation -eq 'fs')",
                       "--workspace-root",
                       "AssocClass=Win32_LogicalDiskToPartition", "$disk.InterfaceType -eq 'USB'",
                       "'HKCU:\\Software\\OxideSwarm\\UsbKit'"):
            self.assertIn(needle, text)
        ask = text.index('Read-Host "Choose [1]"')
        self.assertIn("-not [Console]::IsInputRedirected", text[text.index("$explicitRoot ="):ask])
        lock = text.index("[IO.Directory]::CreateDirectory($InstallRoot, $sec)")
        self.assertTrue(lock < text.index("foreach ($d in @($Bin, $State, $RunDir, $LogDir))"),
                        "the folder must be limited before the installer writes into it")
        uninstall = read(os.path.join(ROOT, "packaging", "usb", "uninstall.ps1"))
        self.assertIn("'HKCU:\\Software\\OxideSwarm\\UsbKit'", uninstall)
        self.assertIn("$run.IndexOf($InstallRoot + '\\'", uninstall)

    def test_install_refuses_a_damaged_archive_before_installing_anything(self):
        self.assertEqual(self.make_kit().returncode, 0)
        kit = os.path.join(self.stick, "OxideSwarm")
        for path in glob.glob(os.path.join(kit, "archives", "*.tar.gz")):
            with open(path, "ab") as f:
                f.write(b"x")
        root = os.path.join(self.tmp, "install-root")
        res = subprocess.run(["bash", os.path.join(kit, "install.sh"), "--install-root", root, "--no-autostart"],
                             env=self.env(), stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True, timeout=60)
        self.assertNotEqual(res.returncode, 0)
        self.assertIn("checksum mismatch", res.stdout)
        self.assertFalse(os.path.exists(os.path.join(root, "bin", "oxideswarm")))
        self.assertEqual([c for c in read(self.args_log).splitlines() if c.strip()], [])

    def test_a_worker_that_stops_after_the_start_is_reported_with_its_log(self):
        # BUG-31 (2026-11-18, a Windows 11 laptop): the worker exited about 6 s after its start, after
        # the installer's one liveness check, so the installer waited for the listing and then said "the
        # worker runs but the master does not list it yet; see .../worker.log". The rollback had already
        # deleted that log. The installer now checks the worker every second while it waits, prints the
        # end of its log before the rollback, and keeps the log.
        self.assertEqual(self.make_kit().returncode, 0)
        kit = os.path.join(self.stick, "OxideSwarm")
        root = os.path.join(self.tmp, "install-root")
        env = dict(self.env(), FAKE_WORKER_EXIT_AFTER="5", FAKE_WORKER_EXIT_CODE="3", FAKE_WORKERS_EMPTY="1")
        res = subprocess.run(["bash", os.path.join(kit, "install.sh"), "--install-root", root, "--isolation", "off",
                              "--no-autostart", "--name", "kit-stops"],
                             env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True, timeout=120)
        out = res.stdout
        self.assertNotEqual(res.returncode, 0, out)
        log = os.path.join(root, "logs", "worker.log")
        self.assertIn("the worker stopped", out)
        self.assertIn("exit code 3", out)
        self.assertNotIn("does not list it", out)
        # The last 30 lines of the log, under a header, before the error and the rollback.
        self.assertIn("last 30 lines of " + log, out)
        self.assertIn("FAKE-STOP-MARKER", out)
        self.assertIn("fake-log-line-40", out)
        self.assertNotIn("fake-log-line-01", out)
        self.assertLess(out.index("FAKE-STOP-MARKER"), out.index("ERROR:"))
        # The rollback removed the install but kept the log, and the error names a file that exists.
        self.assertFalse(os.path.exists(os.path.join(root, "bin")))
        self.assertFalse(os.path.exists(os.path.join(root, "home")))
        self.assertTrue(os.path.isfile(log), out)
        self.assertIn("FAKE-STOP-MARKER", read(log))
        error_line = [line for line in out.splitlines() if line.startswith("ERROR:")][-1]
        self.assertIn(log, error_line)
        self.assertEqual(stat.S_IMODE(os.stat(os.path.dirname(log)).st_mode) & 0o077, 0,
                         "the kept log folder must be private to this user")

    def test_install_falls_back_to_isolation_off_when_the_isolated_worker_stops(self):
        # The isolation fallback of design 0055 D2 survives the liveness rewrite of BUG-31: a worker
        # that stops under the default --isolation fs is started again with --isolation off.
        self.assertEqual(self.make_kit().returncode, 0)
        kit = os.path.join(self.stick, "OxideSwarm")
        root = os.path.join(self.tmp, "install-root")
        env = dict(self.env(), FAKE_WORKER_STOP_WITH_ISOLATION="fs")
        res = subprocess.run(["bash", os.path.join(kit, "install.sh"), "--install-root", root,
                              "--no-autostart", "--name", "kit-fallback"],
                             env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True, timeout=120)
        self.assertEqual(res.returncode, 0, res.stdout)
        self.assertIn("trying --isolation off", res.stdout)
        self.assertIn("connected (isolation: off)", res.stdout)
        self.assertIn("isolation=off\n", read(os.path.join(root, "kit-install.conf")))

    def test_windows_drive_menu_offers_only_folders_the_install_accepts(self):
        # BUG-29 (2026-11-18): the folder menu offered E:\\OxideSwarm-USB, and the install then refused
        # it because another account could delete or re-permission folders at the root of E:\\. The menu
        # now runs the install's own root check on each drive and lists a refused one as not usable,
        # with the reason and without a number. Static text here; the run is in the Windows job.
        text = read(os.path.join(ROOT, "packaging", "usb", "install.ps1"))
        menu = text[text.index("$explicitRoot ="):text.index('Read-Host "Choose [1]"')]
        self.assertIn("$choices = @($DefaultRoot)", menu)
        self.assertIn("Test-RootSafety $candidate", menu)
        self.assertIn("not usable", menu)
        self.assertLess(menu.index("$choices = @($DefaultRoot)"), menu.index("Test-RootSafety $candidate"))
        for hook in ("OXIDESWARM_USB_DRIVE_CHOICES", "OXIDESWARM_USB_LIST_FOLDERS"):
            self.assertIn(hook, menu)

    def test_windows_uninstaller_removes_an_install_from_an_older_kit(self):
        # BUG-30 (2026-11-18): an install from a kit older than the .oxideswarm-usb-workspaces marker
        # could not be removed. Its workspaces folder is accepted when the install folder carries the
        # installer's marker, the folder is a real one with no link inside, and nothing else uses it.
        text = read(os.path.join(ROOT, "packaging", "usb", "uninstall.ps1"))
        block = text[text.index("$WorkspacesDir = Join-Path $InstallRoot 'workspaces'"):text.index("if (-not $Yes)")]
        for needle in ("'.oxideswarm-usb-install'", "ReparsePoint", "$foreignUsers.Count -eq 0",
                       "an install from an older kit", "$DefaultWorkerWorkspaceRoot"):
            self.assertIn(needle, block)

    def test_windows_installer_watches_the_worker_and_keeps_its_log(self):
        # BUG-31 in install.ps1: the worker is checked every second while the installer waits for the
        # listing, the end of its log is printed before the rollback, and the rollback keeps the log.
        text = read(os.path.join(ROOT, "packaging", "usb", "install.ps1"))
        for needle in ("last 30 lines of", "-Tail 30", "HasExited", "ExitCode", "the worker stopped",
                       "BeginInvoke()", "$script:KeepLogs = $true"):
            self.assertIn(needle, text)
        self.assertNotIn("the worker stopped right away", text)
        rollback = text[text.index("function Rollback-FreshInstall"):text.index("function Fail(")]
        self.assertIn("$script:KeepLogs", rollback)
        start = text.index("Start-Process -FilePath 'wscript.exe'")
        self.assertLess(text.index("$script:KeepLogs = $true"), start)

    def test_windows_task_folders_stay_in_the_install_folder_unless_isolation_needs_temp(self):
        # Sandbox base (design 0053 status log, 2026-11-18): a default install kept its task folders in
        # %TEMP%\\oxideswarm-usb\\sandboxes, which the worker refused on a laptop whose Temp folder let
        # three leftover accounts change it. They now stay in <install folder>\\work, as on another drive;
        # only -Isolation fs in %LOCALAPPDATA% keeps %TEMP%, because the isolated worker accepts no other
        # folder inside the user profile. The installer checks the folder first and prints, but never
        # runs, the icacls command that removes the grants.
        text = read(os.path.join(ROOT, "packaging", "usb", "install.ps1"))
        for needle in ("function Test-SandboxBase", "your Temp folder", "lets these accounts change it",
                       "/remove:g", "$PrivateRoot -and $Isolation -eq 'fs'", "RUSTY_GRID_SANDBOX_BASE_DIR"):
            self.assertIn(needle, text)
        self.assertNotRegex(text, r"(?mi)^\s*(&\s*)?['\"]?icacls(\.exe)?['\"]?\s")
        self.assertNotRegex(text, r"(?i)Start-Process[^\n]*icacls")
        check = text.index("Test-SandboxBase $SandboxBase")
        self.assertLess(check, text.index("& $Exe join --no-doctor"), "check the folder before joining")

    def test_windows_installer_refuses_custom_profile_workspace_root_under_isolation_fs(self):
        text = read(os.path.join(ROOT, "packaging", "usb", "install.ps1"))
        for needle in (
            "$allowedProfileWs1 = Join-Path $UserProfileFull 'AppData\\Local\\OxideSwarm-USB\\workspaces'",
            "$allowedProfileWs2 = Join-Path $UserProfileFull 'AppData\\Local\\OxideSwarm\\usb-kit\\workspaces'",
            "with -Isolation fs and workspaces:",
        ):
            self.assertIn(needle, text)
        self.assertLess(
            text.index("$allowedProfileWs1"),
            text.index("# 2. Create the folder"),
            "must refuse disallowed profile workspace roots before creating InstallRoot",
        )

    def test_make_kit_and_install_support_isolation_option_and_reject_invalid_values(self):
        bad_kit = self.make_kit(extra=["--isolation", "bogus"])
        self.assertNotEqual(bad_kit.returncode, 0)
        self.assertIn("--isolation must be off or fs", bad_kit.stdout)

        ok_kit = self.make_kit(extra=["--no-autostart", "--isolation", "off"])
        self.assertEqual(ok_kit.returncode, 0, ok_kit.stdout)
        kit = os.path.join(self.stick, "OxideSwarm")
        self.assertTrue(os.path.isfile(os.path.join(kit, "revoke-isolation-acls.ps1")))
        self.assertEqual(read(os.path.join(kit, "isolation")).strip(), "off")

        # Baked kit isolation=off is honoured by install.sh when --isolation is not passed.
        root1 = os.path.join(self.tmp, "install-baked-off")
        res1 = subprocess.run(
            ["bash", os.path.join(kit, "install.sh"), "--install-root", root1, "--name", "kit-iso-off"],
            env=self.env(), stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, timeout=90,
        )
        self.assertEqual(res1.returncode, 0, res1.stdout)
        self.assertIn("connected (isolation: off)", res1.stdout)
        worker_calls = [c for c in read(self.args_log).splitlines() if c.startswith("worker ")]
        self.assertTrue(worker_calls and "--isolation off" in worker_calls[-1], worker_calls)
        subprocess.run(
            ["bash", os.path.join(kit, "uninstall.sh"), "--install-root", root1, "--yes"],
            env=self.env(), stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, timeout=30,
        )

        # CLI --isolation fs overrides baked kit isolation=off.
        root2 = os.path.join(self.tmp, "install-cli-fs")
        res2 = subprocess.run(
            ["bash", os.path.join(kit, "install.sh"), "--install-root", root2, "--name", "kit-iso-fs", "--isolation", "fs"],
            env=self.env(), stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, timeout=90,
        )
        self.assertEqual(res2.returncode, 0, res2.stdout)
        self.assertIn("connected (isolation: fs)", res2.stdout)
        worker_calls2 = [c for c in read(self.args_log).splitlines() if c.startswith("worker ")]
        self.assertTrue(worker_calls2 and "--isolation fs" in worker_calls2[-1], worker_calls2)
        subprocess.run(
            ["bash", os.path.join(kit, "uninstall.sh"), "--install-root", root2, "--yes"],
            env=self.env(), stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, timeout=30,
        )

        # Invalid --isolation value on install.sh fails immediately.
        bad_inst = subprocess.run(
            ["bash", os.path.join(kit, "install.sh"), "--install-root", root2, "--isolation", "invalid"],
            env=self.env(), stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, timeout=30,
        )
        self.assertNotEqual(bad_inst.returncode, 0)
        self.assertIn("--isolation must be off or fs", bad_inst.stdout)

    def _make_v1_kit(self, *, roles=("worker",), workspaces=False, require_approval=True, no_autostart=True):
        extra = ["--no-autostart"] if no_autostart else []
        res = self.make_kit(extra=extra)
        self.assertEqual(res.returncode, 0, res.stdout)
        kit = os.path.join(self.stick, "OxideSwarm")
        tok_path = os.path.join(kit, "join.token")
        if os.path.exists(tok_path):
            os.remove(tok_path)
        with open(os.path.join(kit, "invite.oxs"), "wb") as f:
            f.write(b"OXSINV01fake-encrypted-payload")
        meta = {
            "kit_format": 1,
            "version": "9.9.9",
            "created_at": 1700000000,
            "expires_at": 1900000000,
            "machines": 2,
            "roles": list(roles),
            "workspaces": workspaces,
            "gpu": False,
            "require_approval": require_approval,
            "invite_id": "0123456789abcdef0123456789abcdef",
        }
        with open(os.path.join(kit, "kit.json"), "w", encoding="utf-8") as f:
            json.dump(meta, f)
        return kit

    def test_v1_kit_worker_roles_workspaces_logs_and_rollback(self):
        kit = self._make_v1_kit(roles=("worker",), workspaces=False, no_autostart=True)

        # Wrong passphrase triggers fresh-install rollback cleanly.
        root_bad = os.path.join(self.tmp, "v1-bad-pass")
        bad = subprocess.run(
            ["bash", os.path.join(kit, "install.sh"), "--install-root", root_bad, "--name", "bad-w"],
            env=self.env(), input="wrong-passphrase\n", stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, timeout=90,
        )
        self.assertNotEqual(bad.returncode, 0, bad.stdout)
        self.assertFalse(os.path.exists(os.path.join(root_bad, "bin", "oxideswarm")))
        self.assertFalse(os.path.exists(os.path.join(root_bad, "home", "node.key")))

        # Worker-only kit skips doctor, omits --workspaces when kit says false, writes install.log.
        root = os.path.join(self.tmp, "v1-worker")
        env1 = dict(self.env(), FAKE_MASTER_KEY="11" * 32, OXIDESWARM_USB_MACHINE_ID="machine-v1-test")
        res = subprocess.run(
            ["bash", os.path.join(kit, "install.sh"), "--install-root", root, "--name", "v1-worker"],
            env=env1, input="valid kit passphrase\n", stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, timeout=90,
        )
        self.assertEqual(res.returncode, 0, res.stdout)
        self.assertTrue(os.path.isfile(os.path.join(root, "home", "node.key")))
        self.assertTrue(os.path.isfile(os.path.join(root, "home", "rusty-grid.toml")))
        id_path = os.path.join(root, "home", "worker_id.v1-worker")
        self.assertTrue(os.path.isfile(id_path))
        wid1 = read(id_path).strip()
        self.assertTrue(wid1)
        self.assertTrue(os.path.isfile(os.path.join(root, "logs", "install.log")))

        log_lines = read(self.args_log).splitlines()
        self.assertTrue(any(l.startswith("join --invite-file ") for l in log_lines), log_lines)
        worker_lines = [l for l in log_lines if l.startswith("worker ")]
        self.assertTrue(worker_lines and "--name v1-worker" in worker_lines[-1], worker_lines)
        self.assertNotIn("--workspaces", worker_lines[-1])
        self.assertFalse(any(l == "doctor" or l.startswith("doctor ") for l in log_lines), log_lines)

        # --uninstall --yes --keep-key preserves home/node.key while removing bin/.
        un = subprocess.run(
            ["bash", os.path.join(kit, "uninstall.sh"), "--install-root", root, "--yes", "--keep-key"],
            env=env1, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, timeout=30,
        )
        self.assertEqual(un.returncode, 0, un.stdout)
        self.assertFalse(os.path.exists(os.path.join(root, "bin")))
        self.assertTrue(os.path.isfile(os.path.join(root, "home", "node.key")))

        # --workspaces flag + different FAKE_MASTER_KEY passes --workspaces (with cargo) & shifts worker_id.
        fake_bin = os.path.join(self.tmp, "fake-cargo-bin")
        os.makedirs(fake_bin, exist_ok=True)
        cargo_exe = os.path.join(fake_bin, "cargo")
        with open(cargo_exe, "w", encoding="utf-8") as cf:
            cf.write("#!/bin/sh\necho cargo 1.90.0\n")
        os.chmod(cargo_exe, 0o755)
        root_ws = os.path.join(self.tmp, "v1-worker-ws")
        env2 = dict(self.env(), FAKE_MASTER_KEY="22" * 32, OXIDESWARM_USB_MACHINE_ID="machine-v1-test")
        env2["PATH"] = fake_bin + os.pathsep + env2["PATH"]
        res_ws = subprocess.run(
            ["bash", os.path.join(kit, "install.sh"), "--install-root", root_ws, "--name", "v1-worker", "--workspaces"],
            env=env2, input="valid kit passphrase\n", stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, timeout=90,
        )
        self.assertEqual(res_ws.returncode, 0, res_ws.stdout)
        worker_lines2 = [l for l in read(self.args_log).splitlines() if l.startswith("worker ")]
        self.assertIn("--workspaces", worker_lines2[-1])
        wid2 = read(os.path.join(root_ws, "home", "worker_id.v1-worker")).strip()
        self.assertNotEqual(wid1, wid2)
        subprocess.run(
            ["bash", os.path.join(kit, "uninstall.sh"), "--install-root", root_ws, "--yes"],
            env=env2, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30,
        )

    def test_v1_kit_client_roles_and_online_sums(self):
        kit = self._make_v1_kit(roles=("client",), workspaces=False, no_autostart=True)
        sums_text = read(os.path.join(kit, "archives", "SHA256SUMS"))
        good_sums = os.path.join(self.tmp, "good_SHA256SUMS")
        bad_sums = os.path.join(self.tmp, "bad_SHA256SUMS")
        with open(good_sums, "w", encoding="utf-8") as f:
            f.write(sums_text)
        with open(bad_sums, "w", encoding="utf-8") as f:
            for line in sums_text.splitlines():
                if len(line) > 64:
                    f.write("0" * 64 + line[64:] + "\n")

        # Non-HTTPS URL must be refused.
        root_http = os.path.join(self.tmp, "v1-http")
        env_http = dict(self.env(), OXIDESWARM_USB_ONLINE_SUMS_URL="http://example.com/SHA256SUMS")
        res_http = subprocess.run(
            ["bash", os.path.join(kit, "install.sh"), "--install-root", root_http, "--name", "cli1"],
            env=env_http, input="valid kit passphrase\n", stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, timeout=90,
        )
        self.assertNotEqual(res_http.returncode, 0, res_http.stdout)
        self.assertFalse(os.path.exists(os.path.join(root_http, "bin")))

        # Mismatched file:// SHA256SUMS fails before unpacking.
        root_bad_sum = os.path.join(self.tmp, "v1-bad-sum")
        env_bad_sum = dict(self.env(), OXIDESWARM_USB_ONLINE_SUMS_URL=pathlib.Path(bad_sums).as_uri())
        res_bad_sum = subprocess.run(
            ["bash", os.path.join(kit, "install.sh"), "--install-root", root_bad_sum, "--name", "cli1"],
            env=env_bad_sum, input="valid kit passphrase\n", stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, timeout=90,
        )
        self.assertNotEqual(res_bad_sum.returncode, 0, res_bad_sum.stdout)
        self.assertFalse(os.path.exists(os.path.join(root_bad_sum, "bin")))

        # Client-only kit with matching file:// SHA256SUMS runs doctor and skips worker start.
        root_cli = os.path.join(self.tmp, "v1-client")
        env_good = dict(self.env(), OXIDESWARM_USB_ONLINE_SUMS_URL=pathlib.Path(good_sums).as_uri())
        res_cli = subprocess.run(
            ["bash", os.path.join(kit, "install.sh"), "--install-root", root_cli, "--name", "cli1"],
            env=env_good, input="valid kit passphrase\n", stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, timeout=90,
        )
        self.assertEqual(res_cli.returncode, 0, res_cli.stdout)
        log_lines = read(self.args_log).splitlines()
        self.assertTrue(any(l == "doctor" or l.startswith("doctor ") for l in log_lines), log_lines)
        self.assertFalse(any(l.startswith("worker ") for l in log_lines), log_lines)
        self.assertFalse(os.path.exists(os.path.join(root_cli, "start-worker.sh")))
        un = subprocess.run(
            ["bash", os.path.join(kit, "uninstall.sh"), "--install-root", root_cli, "--yes"],
            env=env_good, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30,
        )
        self.assertEqual(un.returncode, 0, un.stdout)
        self.assertFalse(os.path.exists(root_cli))


class StaticRepositoryChecks(unittest.TestCase):
    def test_windows_usb_scripts_enforce_cmdletbinding_isolation_and_manifest_revocation(self):
        install_ps1 = os.path.join(ROOT, "packaging", "usb", "install.ps1")
        uninstall_ps1 = os.path.join(ROOT, "packaging", "usb", "uninstall.ps1")
        install_cmd = os.path.join(ROOT, "packaging", "usb", "Install OxideSwarm.cmd")
        uninstall_cmd = os.path.join(ROOT, "packaging", "usb", "Uninstall OxideSwarm.cmd")
        revoke_ps1 = os.path.join(ROOT, "windows", "revoke-isolation-acls.ps1")

        inst_text = read(install_ps1)
        uninst_text = read(uninstall_ps1)
        inst_cmd_text = read(install_cmd)
        uninst_cmd_text = read(uninstall_cmd)
        rev_text = read(revoke_ps1)

        for cmd_text in (inst_cmd_text, uninst_cmd_text):
            self.assertIn("set PSModulePath=", cmd_text)
            self.assertLess(cmd_text.index("set PSModulePath="), cmd_text.index("powershell"))

        self.assertIn("[CmdletBinding()]", inst_text)
        self.assertIn("[ValidateSet('off', 'fs')]", inst_text)
        self.assertIn("--isolation $Isolation", inst_text)
        self.assertIn("RUSTY_GRID_ACL_GRANT_LOG", inst_text)
        self.assertIn("RUSTY_GRID_SANDBOX_BASE_DIR", inst_text)
        self.assertIn("FailAfterStart", inst_text)
        self.assertIn("revert them", inst_text)
        self.assertIn("connected (isolation: $Isolation)", inst_text)

        self.assertIn("[CmdletBinding()]", uninst_text)
        self.assertIn("windows_acl_grants.txt", uninst_text)
        self.assertIn("revoke-isolation-acls.ps1", uninst_text)
        self.assertIn("$global:LASTEXITCODE = 0", uninst_text)
        self.assertIn("keeping $InstallRoot and $Manifest", uninst_text)
        self.assertIn("oxideswarm-usb", uninst_text)
        self.assertIn("ReparsePoint", uninst_text)
        self.assertLess(
            uninst_text.index("revoke-isolation-acls.ps1"),
            uninst_text.index("Remove-Item -Recurse -Force -LiteralPath $InstallRoot"),
            "uninstall.ps1 must revoke isolation ACLs before deleting $InstallRoot",
        )
        self.assertLess(
            uninst_text.index("revoke-isolation-acls.ps1"),
            uninst_text.index("$UsbTempRoot"),
            "uninstall.ps1 must remove %TEMP%\\oxideswarm-usb only after ACL revocation succeeds",
        )
        self.assertIn("$env:OXIDESWARM_HOME", rev_text)
        self.assertIn("while another worker is running", rev_text)
        self.assertIn(r"\s+(worker)(\s|$)", rev_text)
        self.assertIn("$cimQueryFailed", rev_text)
        self.assertIn("System.IO.FileSystemAclExtensions", rev_text)
        self.assertNotIn(".oxideswarm_worker_profile.lock", rev_text)
        self.assertNotIn("[System.IO.Directory]::GetAccessControl", rev_text)
        self.assertIn("-notin @('sandboxes', 'workspaces')", rev_text)
        self.assertIn("$failures = 0", rev_text)
        self.assertIn("icacls /remove exited with code", rev_text)
        self.assertIn("exit 1", rev_text)
        self.assertIn("exit 0", rev_text)
        self.assertLess(
            rev_text.index("foreach ($restore in $sddlRestores)"),
            rev_text.index("foreach ($target in $targetPaths)"),
            "revoke-isolation-acls.ps1 must restore SDDL before icacls /remove",
        )
        self.assertLess(
            rev_text.index("if ($failures -gt 0)"),
            rev_text.index("Remove-Item -LiteralPath $ManifestPath -Force"),
            "revoke-isolation-acls.ps1 must refuse to remove the manifest when any failure occurred",
        )

        if os.name == "nt" and shutil.which("powershell"):
            ps_env = {k: v for k, v in os.environ.items() if k.upper() != "PSMODULEPATH"}
            for script in (install_ps1, uninstall_ps1):
                bad_param = subprocess.run(
                    ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", script, "-NoSuchParam"],
                    env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, timeout=30,
                )
                self.assertNotEqual(bad_param.returncode, 0, bad_param.stdout)
                self.assertIn("NoSuchParam", bad_param.stdout)

            bad_iso = subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", install_ps1, "-Isolation", "bogus"],
                env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=30,
            )
            self.assertNotEqual(bad_iso.returncode, 0, bad_iso.stdout)

            tmp = tempfile.mkdtemp(prefix="ox_win_usb_test_")
            tmp_home = os.path.join(tmp, "userprofile")
            tmp_localappdata = os.path.join(tmp_home, "AppData", "Local")
            tmp_appdata = os.path.join(tmp_home, "AppData", "Roaming")
            tmp_temp = os.path.join(tmp_localappdata, "Temp")
            for d in (tmp_home, tmp_localappdata, tmp_appdata, tmp_temp):
                os.makedirs(d, exist_ok=True)
            test_run_val = f"OxideSwarmUsbWorkerTest_{os.getpid()}"
            test_pointer_key = f"HKCU:\\Software\\OxideSwarm\\UsbKitTest_{os.getpid()}"
            ps_env = dict(
                ps_env,
                USERPROFILE=tmp_home,
                HOME=tmp_home,
                LOCALAPPDATA=tmp_localappdata,
                APPDATA=tmp_appdata,
                TEMP=tmp_temp,
                TMP=tmp_temp,
                OXIDESWARM_USB_RUN_VALUE=test_run_val,
                OXIDESWARM_USB_POINTER_KEY=test_pointer_key,
                OXIDESWARM_USB_ONLINE_SUMS_URL="off",
            )
            pids_log = os.path.join(tmp, "spawned_workers.pids")
            ps_env["FAKE_OXIDE_PIDS_LOG"] = pids_log
            try:
                # 1. Verify uninstall.ps1 is idempotent when no manifest file exists in $InstallRoot\home.
                fake_install = os.path.join(tmp, "win-fake-install")
                os.makedirs(os.path.join(fake_install, "bin"), exist_ok=True)
                os.makedirs(os.path.join(fake_install, "home"), exist_ok=True)
                with open(os.path.join(fake_install, "bin", "oxideswarm.exe"), "wb") as f:
                    f.write(b"MZfake")
                # A kit install carries the installer's marker; uninstall.ps1 deletes nothing without it.
                open(os.path.join(fake_install, ".oxideswarm-usb-install"), "w").close()
                uninst_res = subprocess.run(
                    [
                        "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                        "-File", uninstall_ps1, "-InstallRoot", fake_install, "-Yes",
                    ],
                    env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, timeout=30,
                )
                self.assertEqual(uninst_res.returncode, 0, uninst_res.stdout)
                self.assertIn("No isolation ACL grant manifest", uninst_res.stdout)
                self.assertFalse(os.path.exists(fake_install))

                # 1b. R2-S1 Scenario B: manifest exists, but all recorded paths are already gone
                # (so zero icacls invocations run). revoke-isolation-acls.ps1 must exit 0 and
                # uninstall.ps1 must not fail with a null $LASTEXITCODE.
                fake_no_icacls = os.path.join(tmp, "win-no-icacls")
                os.makedirs(os.path.join(fake_no_icacls, "bin"), exist_ok=True)
                os.makedirs(os.path.join(fake_no_icacls, "home"), exist_ok=True)
                shutil.copyfile(revoke_ps1, os.path.join(fake_no_icacls, "revoke-isolation-acls.ps1"))
                with open(os.path.join(fake_no_icacls, "bin", "oxideswarm.exe"), "wb") as f:
                    f.write(b"MZfake")
                # A kit install carries the installer's marker; uninstall.ps1 deletes nothing without it.
                open(os.path.join(fake_no_icacls, ".oxideswarm-usb-install"), "w").close()
                with open(os.path.join(fake_no_icacls, "home", "windows_acl_grants.txt"), "w", encoding="utf-8") as f:
                    f.write(os.path.join(tmp, "already-deleted-sandbox-base") + "\n")
                uninst_no_icacls = subprocess.run(
                    [
                        "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                        "-File", uninstall_ps1, "-InstallRoot", fake_no_icacls, "-Yes",
                    ],
                    env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, timeout=30,
                )
                self.assertEqual(uninst_no_icacls.returncode, 0, uninst_no_icacls.stdout)
                self.assertIn("Skipping missing path:", uninst_no_icacls.stdout)
                self.assertFalse(os.path.exists(fake_no_icacls))

                # 1c. R2-S1 Scenario A: an earlier icacls fails (exit 5), a later icacls succeeds (exit 0).
                # revoke-isolation-acls.ps1 must exit 1 and keep the manifest; uninstall.ps1 must exit 1
                # and keep both $InstallRoot and $Manifest.
                fake_icacls_dir = os.path.join(tmp, "fake_icacls_bin")
                os.makedirs(fake_icacls_dir, exist_ok=True)
                fake_icacls_cs = os.path.join(tmp, "FakeIcacls.cs")
                fake_icacls_exe = os.path.join(fake_icacls_dir, "icacls.exe")
                with open(fake_icacls_cs, "w", encoding="utf-8") as f:
                    f.write(
                        "using System;\n"
                        "public class Program {\n"
                        "    public static int Main(string[] args) {\n"
                        "        if (args.Length > 0 && args[0].Contains(\"fail_acl_dir\")) return 5;\n"
                        "        return 0;\n"
                        "    }\n"
                        "}\n"
                    )
                comp_icacls = subprocess.run(
                    [
                        "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command",
                        f"Add-Type -Path '{fake_icacls_cs}' -OutputAssembly '{fake_icacls_exe}' -OutputType ConsoleApplication",
                    ],
                    env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, timeout=30,
                )
                self.assertEqual(comp_icacls.returncode, 0, comp_icacls.stdout)
                fake_partial = os.path.join(tmp, "win-partial-icacls")
                fail_acl_dir = os.path.join(tmp, "fail_acl_dir")
                ok_acl_dir = os.path.join(tmp, "ok_acl_dir")
                for d in (os.path.join(fake_partial, "bin"), os.path.join(fake_partial, "home"), fail_acl_dir, ok_acl_dir):
                    os.makedirs(d, exist_ok=True)
                shutil.copyfile(revoke_ps1, os.path.join(fake_partial, "revoke-isolation-acls.ps1"))
                with open(os.path.join(fake_partial, "bin", "oxideswarm.exe"), "wb") as f:
                    f.write(b"MZfake")
                # A kit install carries the installer's marker; uninstall.ps1 deletes nothing without it.
                open(os.path.join(fake_partial, ".oxideswarm-usb-install"), "w").close()
                partial_manifest = os.path.join(fake_partial, "home", "windows_acl_grants.txt")
                with open(partial_manifest, "w", encoding="utf-8") as f:
                    f.write(fail_acl_dir + "\n" + ok_acl_dir + "\n")
                ps_env_fake_icacls = dict(ps_env, PATH=fake_icacls_dir + os.pathsep + ps_env.get("PATH", ""))
                uninst_partial = subprocess.run(
                    [
                        "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                        "-File", uninstall_ps1, "-InstallRoot", fake_partial, "-Yes",
                    ],
                    env=ps_env_fake_icacls, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, timeout=30,
                )
                self.assertNotEqual(uninst_partial.returncode, 0, uninst_partial.stdout)
                self.assertIn("icacls /remove exited with code 5", uninst_partial.stdout)
                self.assertIn("keeping", uninst_partial.stdout)
                self.assertTrue(os.path.isfile(partial_manifest), "manifest must be kept when an earlier icacls fails")
                self.assertTrue(os.path.isdir(fake_partial), "install root must be kept when an earlier icacls fails")

                # 2. Fake-binary end-to-end test of install.ps1 and uninstall.ps1 on Windows.
                kit_dir = os.path.join(tmp, "kit")
                os.makedirs(os.path.join(kit_dir, "archives"), exist_ok=True)
                for name in ("install.ps1", "uninstall.ps1"):
                    shutil.copyfile(os.path.join(ROOT, "packaging", "usb", name), os.path.join(kit_dir, name))
                shutil.copyfile(revoke_ps1, os.path.join(kit_dir, "revoke-isolation-acls.ps1"))
                with open(os.path.join(kit_dir, "join.token"), "w", encoding="utf-8") as f:
                    f.write(KIT_TOKEN + "\n")
                with open(os.path.join(kit_dir, "no-autostart"), "w", encoding="utf-8") as f:
                    f.write("trial\n")
                with open(os.path.join(kit_dir, "isolation"), "w", encoding="utf-8") as f:
                    f.write("fs\n")

                fake_exe = os.path.join(tmp, "oxideswarm.exe")
                cs_code = r"""
using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
public class Program {
    public static int Main(string[] args) {
        string joined = string.Join(" ", args);
        string home = Environment.GetEnvironmentVariable("OXIDESWARM_HOME") ?? "";
        if (!string.IsNullOrEmpty(home) && Directory.Exists(home)) {
            try { File.AppendAllText(Path.Combine(home, "args.log"), joined + Environment.NewLine); } catch {}
        }
        if (args.Length > 0 && args[0] == "--version") {
            Console.WriteLine("oxideswarm 9.9.9");
            return 0;
        }
        if (args.Length > 0 && args[0] == "join") {
            Console.In.ReadToEnd();
            return 0;
        }
        if (args.Length > 0 && args[0] == "worker") {
            string pidsLog = Environment.GetEnvironmentVariable("FAKE_OXIDE_PIDS_LOG");
            if (!string.IsNullOrEmpty(pidsLog)) {
                try { File.AppendAllText(pidsLog, Process.GetCurrentProcess().Id.ToString() + Environment.NewLine); } catch {}
            }
            if (joined.Contains("--isolation fs") && !string.IsNullOrEmpty(home)) {
                string dummyTarget = Path.Combine(home, "dummy_toolchain_dir");
                Directory.CreateDirectory(dummyTarget);
                string grantLog = Environment.GetEnvironmentVariable("RUSTY_GRID_ACL_GRANT_LOG");
                if (string.IsNullOrEmpty(grantLog)) {
                    grantLog = Path.Combine(home, "windows_acl_grants.txt");
                }
                File.WriteAllText(grantLog, dummyTarget + Environment.NewLine);
            }
            Thread.Sleep(60000);
            return 0;
        }
        if (args.Length > 0 && args[0] == "workers") {
            Console.WriteLine("[{\"name\":\"win-kit-test\",\"tags\":[\"state:connected\"]}]");
            return 0;
        }
        if (args.Length > 0 && args[0] == "doctor") {
            Console.WriteLine("doctor ok");
            return 0;
        }
        return 0;
    }
}
"""
                cs_file = os.path.join(tmp, "FakeOxide.cs")
                with open(cs_file, "w", encoding="utf-8") as f:
                    f.write(cs_code)
                compile_res = subprocess.run(
                    [
                        "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command",
                        f"Add-Type -Path '{cs_file}' -OutputAssembly '{fake_exe}' -OutputType ConsoleApplication",
                    ],
                    env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, timeout=30,
                )
                self.assertEqual(compile_res.returncode, 0, compile_res.stdout)

                import zipfile
                zip_name = "oxideswarm-9.9.9-x86_64-pc-windows-msvc.zip"
                zip_path = os.path.join(kit_dir, "archives", zip_name)
                with zipfile.ZipFile(zip_path, "w") as zf:
                    zf.write(fake_exe, "oxideswarm.exe")
                zip_sha = hashlib.sha256(read_bytes(zip_path)).hexdigest()
                with open(os.path.join(kit_dir, "archives", "SHA256SUMS"), "w", encoding="utf-8") as f:
                    f.write(f"{zip_sha}  {zip_name}\n")

                # 2a. Install using baked isolation=fs from kit\isolation, then uninstall (revoking manifest).
                # Under %LOCALAPPDATA%, where a double-clicked install goes: a folder elsewhere on the
                # kit's own drive is refused (a CI runner has only one drive), and under -Isolation fs
                # + workspaces the worker only accepts the default OxideSwarm-USB path inside the profile.
                inst_root_fs = os.path.join(tmp_localappdata, "OxideSwarm-USB")
                inst_fs = subprocess.run(
                    [
                        "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                        "-File", os.path.join(kit_dir, "install.ps1"),
                        "-InstallRoot", inst_root_fs, "-Name", "win-kit-test",
                    ],
                    env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, timeout=60,
                )
                self.assertEqual(inst_fs.returncode, 0, inst_fs.stdout)
                self.assertIn("connected (isolation: fs)", inst_fs.stdout)
                vbs_text = read(os.path.join(inst_root_fs, "start-worker.vbs"))
                self.assertIn("--isolation fs", vbs_text)
                self.assertIn("RUSTY_GRID_ACL_GRANT_LOG", vbs_text)
                self.assertIn("RUSTY_GRID_SANDBOX_BASE_DIR", vbs_text)
                if shutil.which("cargo"):
                    self.assertIn("--workspaces --workspace-root", vbs_text)
                    self.assertTrue(
                        os.path.isfile(os.path.join(inst_root_fs, "workspaces", ".oxideswarm-usb-workspaces"))
                    )
                manifest_fs = os.path.join(inst_root_fs, "home", "windows_acl_grants.txt")
                self.assertTrue(os.path.isfile(manifest_fs))

                # Append a non-sandboxes SDDL entry to verify N1 skips SDDL restore on arbitrary directories.
                non_sb_dir = os.path.join(tmp, "not_a_sandbox_root")
                os.makedirs(non_sb_dir, exist_ok=True)
                with open(manifest_fs, "a", encoding="utf-8") as mf:
                    mf.write(f"SDDL|{non_sb_dir}|D:P(A;;FA;;;SY)\n")

                usb_temp_root = os.path.join(tmp_temp, "oxideswarm-usb")
                usb_temp_blob = os.path.join(usb_temp_root, "sandboxes", "blob_cache", "dummy.bin")
                os.makedirs(os.path.dirname(usb_temp_blob), exist_ok=True)
                with open(usb_temp_blob, "wb") as bf:
                    bf.write(b"blob")

                # Verify S2 + N5: when another task lock is held, uninstall.ps1 refuses to delete
                # $InstallRoot or $Manifest and leaves the manifest intact.
                import ctypes
                lock_file = os.path.join(tmp_temp, "oxideswarm.task.active.lock")
                kernel32 = ctypes.windll.kernel32
                lock_handle = kernel32.CreateFileW(
                    lock_file, 0x40000000, 0, None, 2, 0x80, None
                )
                try:
                    uninst_busy = subprocess.run(
                        [
                            "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                            "-File", os.path.join(kit_dir, "uninstall.ps1"),
                            "-InstallRoot", inst_root_fs, "-Yes",
                        ],
                        env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                        text=True, timeout=60,
                    )
                    self.assertNotEqual(uninst_busy.returncode, 0, uninst_busy.stdout)
                    self.assertIn("while another worker is running", uninst_busy.stdout)
                    self.assertIn("keeping", uninst_busy.stdout)
                    self.assertTrue(os.path.isfile(manifest_fs), "manifest must be preserved when revoke is blocked")
                    self.assertTrue(os.path.isfile(usb_temp_blob), "%TEMP%\\oxideswarm-usb must not be removed before revoke succeeds")
                finally:
                    if lock_handle not in (-1, 0):
                        kernel32.CloseHandle(lock_handle)
                    if os.path.exists(lock_file):
                        os.remove(lock_file)

                uninst_fs = subprocess.run(
                    [
                        "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                        "-File", os.path.join(kit_dir, "uninstall.ps1"),
                        "-InstallRoot", inst_root_fs, "-Yes",
                    ],
                    env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, timeout=60,
                )
                self.assertEqual(uninst_fs.returncode, 0, uninst_fs.stdout)
                self.assertIn("Revoking isolation ACL grants recorded in", uninst_fs.stdout)
                self.assertIn("Refusing SDDL restore on non-sandbox/workspace base path", uninst_fs.stdout)
                self.assertFalse(os.path.exists(inst_root_fs))
                self.assertFalse(os.path.exists(usb_temp_root), "%TEMP%\\oxideswarm-usb must be removed by uninstall.ps1")

                # 2b. Install with explicit -Isolation off overriding baked isolation=fs.
                inst_root_off = os.path.join(tmp_localappdata, "installed-off")
                inst_off = subprocess.run(
                    [
                        "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                        "-File", os.path.join(kit_dir, "install.ps1"),
                        "-InstallRoot", inst_root_off, "-Name", "win-kit-test", "-Isolation", "off",
                    ],
                    env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, timeout=60,
                )
                self.assertEqual(inst_off.returncode, 0, inst_off.stdout)
                self.assertIn("connected (isolation: off)", inst_off.stdout)
                vbs_off = read(os.path.join(inst_root_off, "start-worker.vbs"))
                self.assertIn("--isolation off", vbs_off)
                self.assertFalse(os.path.exists(os.path.join(inst_root_off, "home", "windows_acl_grants.txt")))

                os.makedirs(os.path.dirname(usb_temp_blob), exist_ok=True)
                with open(usb_temp_blob, "wb") as bf:
                    bf.write(b"blob-off")

                uninst_off = subprocess.run(
                    [
                        "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                        "-File", os.path.join(kit_dir, "uninstall.ps1"),
                        "-InstallRoot", inst_root_off, "-Yes",
                    ],
                    env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, timeout=60,
                )
                self.assertEqual(uninst_off.returncode, 0, uninst_off.stdout)
                self.assertIn("No isolation ACL grant manifest", uninst_off.stdout)
                self.assertFalse(os.path.exists(inst_root_off))
                self.assertFalse(os.path.exists(usb_temp_root), "%TEMP%\\oxideswarm-usb must be removed in off mode too")

                # 2c. Verify uninstall.ps1 refuses to follow a reparse point at %TEMP%\oxideswarm-usb.
                reparse_target = os.path.join(tmp, "reparse_canary_dir")
                os.makedirs(reparse_target, exist_ok=True)
                canary_file = os.path.join(reparse_target, "canary.txt")
                with open(canary_file, "w", encoding="utf-8") as cf:
                    cf.write("keep")
                subprocess.run(
                    ["cmd", "/c", "mklink", "/J", usb_temp_root, reparse_target],
                    env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, timeout=15, check=True,
                )
                fake_reparse_inst = os.path.join(tmp, "win-reparse-install")
                os.makedirs(os.path.join(fake_reparse_inst, "bin"), exist_ok=True)
                os.makedirs(os.path.join(fake_reparse_inst, "home"), exist_ok=True)
                with open(os.path.join(fake_reparse_inst, "bin", "oxideswarm.exe"), "wb") as f:
                    f.write(b"MZfake")
                # A kit install carries the installer's marker; uninstall.ps1 deletes nothing without it.
                open(os.path.join(fake_reparse_inst, ".oxideswarm-usb-install"), "w").close()
                uninst_reparse = subprocess.run(
                    [
                        "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                        "-File", uninstall_ps1, "-InstallRoot", fake_reparse_inst, "-Yes",
                    ],
                    env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, timeout=30,
                )
                self.assertEqual(uninst_reparse.returncode, 0, uninst_reparse.stdout)
                self.assertIn("Refusing to remove reparse point", uninst_reparse.stdout)
                self.assertTrue(os.path.isfile(canary_file), "reparse target canary must not be deleted")
                subprocess.run(
                    ["cmd", "/c", "rmdir", usb_temp_root],
                    env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, timeout=15,
                )
            finally:
                for pid_candidate in (
                    pids_log,
                    os.path.join(tmp_localappdata, "OxideSwarm-USB", "run", "worker.pid"),
                    os.path.join(tmp_localappdata, "installed-off", "run", "worker.pid"),
                ):
                    if os.path.isfile(pid_candidate):
                        try:
                            for line in read(pid_candidate).splitlines():
                                p_str = line.strip()
                                if p_str.isdigit():
                                    subprocess.run(
                                        ["powershell", "-NoProfile", "-Command", f"Stop-Process -Id {p_str} -Force -ErrorAction SilentlyContinue"],
                                        env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10,
                                    )
                        except Exception:
                            pass
                subprocess.run(
                    [
                        "powershell", "-NoProfile", "-Command",
                        f"Remove-ItemProperty -Path 'HKCU:\\Software\\Microsoft\\Windows\\CurrentVersion\\Run' -Name '{test_run_val}' -ErrorAction SilentlyContinue; Remove-Item -Path '{test_pointer_key}' -Recurse -Force -ErrorAction SilentlyContinue",
                    ],
                    env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10,
                )
                shutil.rmtree(tmp, ignore_errors=True)

    def test_gitattributes_pins_line_endings(self):
        path = os.path.join(ROOT, ".gitattributes")
        self.assertTrue(os.path.isfile(path), ".gitattributes is missing")
        rules = read(path)
        self.assertRegex(rules, r"(?m)^\*\.sh\s+text\s+eol=lf")
        for ext in ("cmd", "ps1", "vbs"):
            self.assertRegex(rules, r"(?m)^\*\.%s\s+text\s+eol=crlf" % ext)

    def test_working_tree_line_endings_match_the_rules(self):
        bad = []
        for path in tracked_or_present(["*.sh"]):
            if b"\r" in read_bytes(path):
                bad.append("CR in shell script: " + os.path.relpath(path, ROOT))
        for path in tracked_or_present(["*.cmd", "*.ps1", "*.vbs", "*.bat"]):
            data = read_bytes(path)
            if data.count(b"\n") != data.count(b"\r\n"):
                bad.append("LF-only line in Windows script: " + os.path.relpath(path, ROOT))
        self.assertEqual(bad, [])

    def test_no_owner_specific_values_or_name_pattern_kills_in_scripts(self):
        # SHA-256 digests of lowercased forbidden owner tokens/paths so this test file itself
        # never stores literal owner hostnames, serials, LAN IPs, e-mail addresses, or paths.
        # Note: digests of short values such as IPv4 addresses are trivially reversible by
        # brute force (only 2^32 candidates); they are used here only so no literal owner
        # value appears in the committed tree, not for cryptographic secrecy.
        forbidden_owner_hashes = {
            "143def99c87ddb0f21b0ff7eaa44b4eba2f5e8168a6b1c6df2a89aea733b9a51": "the owner's PC name",
            "0eb1180c49728d6bcd1dd05f4f904d64ac2423e7a4a406dac2c5c58ea4051b6e": "the owner's phone serial",
            "2aa1693e357a3c25f0858772628394d0994a33bc57d47c1d94577ac7307a2ebd": "the owner's LAN address",
            "b29a2f994fdaf479ec46efd1afc4fbd5a6be90a44f337cb05b939a95019a3572": "the owner's LAN address",
            "e82f0a1129889229d878e2cd15e82f987fb83bcd686a1c23b32d467e78be93bc": "the owner's LAN address",
            "d635ad9424c6dee43e680e6e639c7bd2084c7f88a18e027531fe404ad915c9d0": "the owner's LAN address",
            "cc08f6ad8f7a8865c991e25a2e998ab1dd35bc094a4f47be8a1a6d92f9f962f0": "the owner's LAN address",
            "adbeebbc313a24f7a44027b2407f780c7b21018bd6f5bfb195810d6f8ef47563": "the owner's overlay/cellular address",
            "70aa0938747ee97a3e704d1541f2b8fe5dd0b488900600cc0a8d24c566096e3f": "the owner's e-mail address",
            "ed4c1136c28a4257e7acd5c4542269a3bd012403399910eee9c91c2481977714": "the owner's workspace directory",
            "2c12c92d538559a4299f30e55d0bb68b14b0a0f1bcd317596449b006ef50f611": "the owner's Windows node identifier",
            "439c9613bd51465cb70f3bbbfc4611edfc0c399cf34639c25f44d7f51dde6e15": "the owner's macOS node identifier",
            "6754388f2f7a7d48653f0f7265b1df465ad540be4fdbafb7bd21c2486af7a924": "the owner's checkout path",
        }
        offenders = []
        for path in tracked_text_files():
            rel = os.path.relpath(path, ROOT)
            text = read(path)
            for line_no, line in enumerate(text.splitlines(), 1):
                lower = line.lower()
                norm_slashes = re.sub(r"[\\/]+", "/", lower)
                candidates = set(re.findall(r"[a-z0-9_.-]+", lower))
                candidates.update(re.findall(r"[a-z0-9_-]+", lower))
                candidates.update(re.findall(r"[a-z]:/[a-z0-9_-]+/[a-z0-9_-]+", norm_slashes))
                for tok in candidates:
                    digest = hashlib.sha256(tok.encode("utf-8")).hexdigest()
                    if digest in forbidden_owner_hashes:
                        offenders.append("%s:%d: %s (%s)" % (rel, line_no, forbidden_owner_hashes[digest], tok))

        kill_patterns = {
            r"\bpkill\b(?!\s*\")": "a name-pattern kill",
            r"taskkill[^\n]*/IM\b": "a kill by image name",
            r"\bkillall\b": "a name-pattern kill",
        }
        for path in tracked_or_present(["*.sh", "*.cmd", "*.ps1", "*.vbs"]):
            rel = os.path.relpath(path, ROOT)
            if rel.startswith("tests" + os.sep):
                continue
            text = read(path)
            for pattern, why in kill_patterns.items():
                # comments that merely mention the old behaviour ("never pkill ...") are fine
                for line in text.splitlines():
                    stripped = line.strip()
                    if stripped.startswith(("#", "rem ", "REM ", "'", "::")):
                        continue
                    if re.search(pattern, line):
                        offenders.append("%s: %s (%s)" % (rel, why, line.strip()[:80]))
        self.assertEqual(offenders, [])

    def test_no_launcher_forces_a_simulated_gpu(self):
        allowed = {"test_integration.sh"}  # worker 3 of the loopback test is deliberately the one simulated-GPU node
        offenders = []
        for path in tracked_or_present(["*.sh", "*.cmd", "*.ps1", "*.vbs"]):
            rel = os.path.relpath(path, ROOT)
            if os.path.basename(rel) in allowed or rel.startswith("tests" + os.sep):
                continue
            if rel.endswith(os.path.join("termux", "start_worker.sh")):
                # opt-in flag only: must be guarded by a real boolean, never `${VAR:+...}` on the string "false"
                self.assertNotIn("${SIMULATE_GPU:+", read(path))
                continue
            for line in read(path).splitlines():
                if "--simulate-gpu" in line and not line.strip().startswith(("#", "rem ", "REM ", "'")):
                    offenders.append("%s: %s" % (rel, line.strip()[:80]))
        self.assertEqual(offenders, [])

    def test_docs_and_installers_never_use_the_dashboard_port_as_the_master_address(self):
        pattern = re.compile(r"(--master|-Master)[ =]+\"?[\w.\-<>\[\]]+:8080\b")
        offenders = []
        for path in tracked_or_present(["*.md", "*.sh", "*.ps1", "*.cmd", "*.kt", "*.xml"]):
            rel = os.path.relpath(path, ROOT)
            if rel.startswith(("tests" + os.sep, "ORIGINAL_REQUEST", "upgrade_and_optimization")):
                continue
            for n, line in enumerate(read(path).splitlines(), 1):
                if pattern.search(line):
                    offenders.append("%s:%d: %s" % (rel, n, line.strip()[:90]))
        self.assertEqual(offenders, [])

    def test_service_installers_do_not_default_to_root_or_system(self):
        plist = read(os.path.join(ROOT, "packaging", "macos", "com.oxideswarm.worker.plist"))
        self.assertIn("<key>UserName</key>", plist)
        self.assertNotRegex(plist, r"<key>UserName</key>\s*<string>root</string>")
        ps1 = read(os.path.join(ROOT, "packaging", "windows", "install_windows_service.ps1"))
        self.assertIn("NT SERVICE", ps1)
        self.assertRegex(ps1, r"obj=\s*\$serviceStartName")
        self.assertIn("AcceptSystemRisk", ps1)
        self.assertIn("Set-PrivateDirAcl -Path $DataDir", ps1)
        self.assertIn('Set-PrivateDirAcl -Path $DataDir -Principal $aclPrincipal -PrincipalRights "(OI)(CI)M" -ResetChildren', ps1)
        self.assertIn("/inheritance:r", ps1)
        self.assertIn("function Get-ServiceDirSafetyError", ps1)
        self.assertIn("function Find-FirstReparsePointUnder", ps1)
        self.assertIn("function Ensure-ServicePrivateDir", ps1)
        self.assertIn("function Test-ServicePrivateAcl", ps1)
        self.assertIn("[IO.Directory]::CreateDirectory($seg, $sec)", ps1)
        self.assertNotIn("New-Item -ItemType Directory", ps1)
        self.assertLess(
            re.search(r"(?m)^Assert-ServicePathsSafe\r?$", ps1).start(),
            ps1.index("Stop-Service -Name $ServiceName"),
            "safety checks must run before stopping or removing an existing service",
        )
        self.assertIn('/setowner ("*" + $OwnerSid) /T /C', ps1)
        self.assertIn("/reset /T /C", ps1)
        self.assertNotIn("Grant-ServiceModify -Path $DataDir", ps1)
        # the P2P ticket must not travel on the service command line or in the registry any more
        wrapper = read(os.path.join(ROOT, "packaging", "windows", "ServiceWrapper.cs"))
        self.assertNotIn("--p2p-ticket", wrapper)
        self.assertNotRegex(ps1, r"SetEnvironmentVariable\(\"OXIDESWARM_P2P_TICKET\",\s*\$P2pTicket")
        self.assertNotRegex(ps1, r"SetEnvironmentVariable\(\"OXIDESWARM_GEN3D_SCRIPT\"")

    def test_windows_powershell_parse_and_non_elevated_acl_helpers(self):
        usb_ps1 = read(os.path.join(ROOT, "packaging", "usb", "install.ps1"))
        for needle in (
            "function Set-PrivateDaclNoFollow",
            "FILE_FLAG_OPEN_REPARSE_POINT",
            "SetKernelObjectSecurity",
            "function Get-CheckedRootEntries",
            "function Format-RootLinkRefusal",
            "preserve cluster membership",
            "$applyErr = Set-PrivateDaclNoFollow $InstallRoot $sec $OwnerSids",
        ):
            self.assertIn(needle, usb_ps1)
        self.assertNotIn("SetAccessControl(", usb_ps1)

        if not (IS_WINDOWS and shutil.which("powershell")):
            return

        ps_env = {k: v for k, v in os.environ.items() if k.upper() != "PSMODULEPATH"}
        tmp = os.path.realpath(tempfile.mkdtemp(prefix="ox_win_ps_acl_"))
        try:
            # 1. PowerShell AST parser check on every .ps1 script in the repository.
            ps1_files = tracked_or_present(["*.ps1"])
            self.assertTrue(ps1_files, "expected tracked .ps1 files")
            parse_driver = os.path.join(tmp, "parse_check.ps1")
            with open(parse_driver, "w", encoding="utf-8", newline="\r\n") as f:
                f.write("$ErrorActionPreference = 'Stop'\r\n")
                for p in ps1_files:
                    esc = p.replace("'", "''")
                    f.write(
                        f"$tokens = $null; $errs = $null\r\n"
                        f"[void][System.Management.Automation.Language.Parser]::ParseFile('{esc}', [ref]$tokens, [ref]$errs)\r\n"
                        f"if ($errs -and $errs.Count -gt 0) {{ Write-Error \"Parse error in '{esc}': $($errs | Out-String)\"; exit 1 }}\r\n"
                    )
                f.write("Write-Output 'PARSE_OK'\r\n")
            parse_res = subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", parse_driver],
                env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=30,
            )
            self.assertEqual(parse_res.returncode, 0, parse_res.stdout)
            self.assertIn("PARSE_OK", parse_res.stdout)

            # 2. Non-elevated unit tests of install_windows_service.ps1's safety/ACL helpers.
            svc_ps1 = os.path.join(ROOT, "packaging", "windows", "install_windows_service.ps1").replace("'", "''")
            svc_driver = os.path.join(tmp, "svc_helpers_test.ps1")
            tmp_esc = tmp.replace("'", "''")
            svc_script = f"""
$ErrorActionPreference = 'Stop'
$tokens = $null; $errs = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile('{svc_ps1}', [ref]$tokens, [ref]$errs)
if ($errs.Count -gt 0) {{ throw "parse failed" }}
$wanted = @(
    'Get-ExtraAllowSids',
    'Resolve-PrincipalSid',
    'Get-ServiceDirSafetyError',
    'Get-ServiceFileSafetyError',
    'Find-FirstReparsePointUnder',
    'New-ServicePrivateSecurity',
    'Test-ServicePrivateAcl',
    'Ensure-ServicePrivateDir',
    'Set-PrivateDirAcl',
    'Set-PrivateFileAcl',
    'Resolve-LongPathPrefix'
)
foreach ($fnAst in $ast.FindAll({{ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] }}, $true)) {{
    if ($fnAst.Name -in $wanted) {{
        . ([scriptblock]::Create($fnAst.Extent.Text))
    }}
}}
$me = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$allowed = @($me, 'S-1-5-18', 'S-1-5-32-544')
$ancAllowed = @($me, 'S-1-5-18', 'S-1-5-32-544', 'S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464')
$base = Join-Path '{tmp_esc}' 'svc_case'
New-Item -ItemType Directory -Path $base | Out-Null
$base = (Get-Item -LiteralPath $base -Force).FullName
& icacls.exe $base /grant '*S-1-1-0:(OI)(CI)R' | Out-Null

# (a) Junction at ProgramData\\OxideSwarm or its child is refused before any change.
$canary = Join-Path $base 'canary_target'
New-Item -ItemType Directory -Path $canary | Out-Null
$juncPd = Join-Path $base 'OxideSwarmJunc'
& cmd.exe /c mklink /J $juncPd $canary | Out-Null
$errJunc = Get-ServiceDirSafetyError -TargetPath $juncPd -AllowedOwners $allowed -AllowedAncestorOwners $ancAllowed
if (-not $errJunc -or $errJunc -notmatch 'link or reparse point') {{ throw "expected link refusal on root: $errJunc" }}
$errAncJunc = Get-ServiceDirSafetyError -TargetPath (Join-Path $juncPd 'data') -AllowedOwners $allowed -AllowedAncestorOwners $ancAllowed
if (-not $errAncJunc -or $errAncJunc -notmatch 'ancestor .* is a link or reparse point') {{ throw "expected ancestor link refusal: $errAncJunc" }}
& cmd.exe /c rmdir $juncPd | Out-Null

# (b) Existing directory owned by an untrusted SID is refused.
$foreignSim = Join-Path $base 'PreCreatedByUser'
New-Item -ItemType Directory -Path $foreignSim | Out-Null
$errOwner = Get-ServiceDirSafetyError -TargetPath $foreignSim -AllowedOwners @('S-1-5-18') -AllowedAncestorOwners $ancAllowed
if (-not $errOwner -or $errOwner -notmatch 'already exists with owner') {{ throw "expected owner refusal: $errOwner" }}

# (c) Ensure-ServicePrivateDir creates missing directories in one step with a protected DACL.
$cleanPd = Join-Path $base 'OxideSwarmClean'
$dataDir = Join-Path $cleanPd 'data'
Ensure-ServicePrivateDir -Path $dataDir -OwnerSid $me -AllowedOwners $allowed -AllowedAncestorOwners $ancAllowed
foreach ($created in @($cleanPd, $dataDir)) {{
    $acl = Get-Acl -LiteralPath $created
    if (-not (Test-ServicePrivateAcl -Acl $acl -AllowedSids $allowed -AllowedOwners $allowed)) {{
        throw "created dir not private: $created"
    }}
}}

# (d) Grant Modify by SID (*S-1-5-19:(OI)(CI)M), simulate worker-created D:P children (.locks, cache),
#     then switch -ServiceAccount to *S-1-5-20 with -ResetChildren and verify old SID is removed and
#     children inherit the new Modify grant without D:P.
Set-PrivateDirAcl -Path $dataDir -Principal '*S-1-5-19' -PrincipalRights '(OI)(CI)M' -OwnerSid $me -AllowedOwners $allowed -AllowedAncestorOwners $ancAllowed
$acl19 = Get-Acl -LiteralPath $dataDir
$rules19 = @($acl19.GetAccessRules($true, $true, [Security.Principal.SecurityIdentifier]) | Where-Object {{ $_.IdentityReference.Value -eq 'S-1-5-19' }})
if ($rules19.Count -ne 1) {{ throw "expected 1 rule for S-1-5-19, got $($rules19.Count)" }}
if (($rules19[0].FileSystemRights -band [System.Security.AccessControl.FileSystemRights]::FullControl) -eq [System.Security.AccessControl.FileSystemRights]::FullControl) {{
    throw "service principal must get Modify, not FullControl: $($rules19[0].FileSystemRights)"
}}
if (-not ($rules19[0].FileSystemRights -band [System.Security.AccessControl.FileSystemRights]::Modify)) {{
    throw "service principal missing Modify: $($rules19[0].FileSystemRights)"
}}
foreach ($childName in @('.locks', 'cache')) {{
    $cPath = Join-Path $dataDir $childName
    New-Item -ItemType Directory -Path $cPath | Out-Null
    & icacls.exe $cPath /inheritance:r /grant:r "*$($me):(OI)(CI)(F)" '*S-1-5-18:(OI)(CI)(F)' '*S-1-5-32-544:(OI)(CI)(F)' '*S-1-5-19:(OI)(CI)(F)' | Out-Null
}}
Set-PrivateDirAcl -Path $dataDir -Principal '*S-1-5-20' -PrincipalRights '(OI)(CI)M' -ResetChildren -OwnerSid $me -AllowedOwners $allowed -AllowedAncestorOwners $ancAllowed
$acl20 = Get-Acl -LiteralPath $dataDir
$sids20 = @($acl20.GetAccessRules($true, $true, [Security.Principal.SecurityIdentifier]) | ForEach-Object {{ $_.IdentityReference.Value }})
if ('S-1-5-19' -in $sids20 -or 'S-1-5-20' -notin $sids20) {{
    throw "expected S-1-5-19 removed and S-1-5-20 added on dataDir: $($sids20 -join ',')"
}}
foreach ($childName in @('.locks', 'cache')) {{
    $cAcl = Get-Acl -LiteralPath (Join-Path $dataDir $childName)
    if ($cAcl.AreAccessRulesProtected) {{ throw "child $childName kept protected DACL after -ResetChildren" }}
    $cSids = @($cAcl.GetAccessRules($true, $true, [Security.Principal.SecurityIdentifier]) | ForEach-Object {{ $_.IdentityReference.Value }})
    if ('S-1-5-19' -in $cSids -or 'S-1-5-20' -notin $cSids) {{
        throw "child $childName did not inherit switched service account: $($cSids -join ',')"
    }}
}}

# (e) A junction planted inside $DataDir is caught by Find-FirstReparsePointUnder before icacls /T runs.
$evilJunc = Join-Path $dataDir 'evil_junc'
& cmd.exe /c mklink /J $evilJunc $canary | Out-Null
$foundLink = Find-FirstReparsePointUnder -Root $dataDir
if ($foundLink -ne $evilJunc) {{ throw "expected Find-FirstReparsePointUnder to find $evilJunc, got $foundLink" }}
$canaryAcl = Get-Acl -LiteralPath $canary
if ($canaryAcl.AreAccessRulesProtected) {{ throw "canary target DACL was modified" }}
& cmd.exe /c rmdir $evilJunc | Out-Null

# (f) Resolve-LongPathPrefix preserves missing leaf segments and does not dereference junctions.
$missingLeaf = Join-Path $cleanPd 'sub\leaf'
if ((Resolve-LongPathPrefix -InputPath $missingLeaf) -ne $missingLeaf) {{ throw "Resolve-LongPathPrefix altered missing leaf path" }}
$juncCheck = Join-Path $base 'junc_keep'
& cmd.exe /c mklink /J $juncCheck $canary | Out-Null
try {{
    if ((Resolve-LongPathPrefix -InputPath (Join-Path $juncCheck 'child')) -ne (Join-Path $juncCheck 'child')) {{
        throw "Resolve-LongPathPrefix dereferenced junction"
    }}
}} finally {{
    if (Test-Path -LiteralPath $juncCheck) {{ & cmd.exe /c rmdir $juncCheck | Out-Null }}
}}
Write-Output 'SVC_HELPERS_OK'
"""
            with open(svc_driver, "w", encoding="utf-8", newline="\r\n") as f:
                f.write(svc_script.replace("\r\n", "\n").replace("\n", "\r\n"))
            svc_res = subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", svc_driver],
                env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=45,
            )
            self.assertEqual(svc_res.returncode, 0, svc_res.stdout)
            self.assertIn("SVC_HELPERS_OK", svc_res.stdout)

            # 3. Non-elevated unit tests of packaging/usb/install.ps1's Set-PrivateDaclNoFollow
            #    and open-root -Update walk (skips cargo target trees and task junctions).
            usb_ps1_path = os.path.join(ROOT, "packaging", "usb", "install.ps1").replace("'", "''")
            usb_driver = os.path.join(tmp, "usb_helpers_test.ps1")
            usb_script = f"""
$ErrorActionPreference = 'Stop'
$tokens = $null; $errs = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile('{usb_ps1_path}', [ref]$tokens, [ref]$errs)
if ($errs.Count -gt 0) {{ throw "parse failed" }}
$UserSid = [Security.Principal.WindowsIdentity]::GetCurrent().User
$OwnerSids = @($UserSid.Value, [Security.Principal.WindowsIdentity]::GetCurrent().Owner.Value) | Select-Object -Unique
$TrustedSids = @($UserSid.Value, 'S-1-5-18', 'S-1-5-32-544')
$TrustedInstaller = 'S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464'
$RenameRights = 0x10000 -bor 0x40000 -bor 0x80000 -bor 0x40
$wantedUsb = @(
    'Get-PrivateSecurity',
    'Get-AccountName',
    'Get-ForeignGrant',
    'Test-PrivateAcl',
    'Set-PrivateDaclNoFollow',
    'Get-CheckedRootEntries',
    'Format-RootLinkRefusal',
    'Test-RootSafety'
)
foreach ($fnAst in $ast.FindAll({{ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] }}, $true)) {{
    if ($fnAst.Name -in $wantedUsb) {{
        . ([scriptblock]::Create($fnAst.Extent.Text))
    }}
}}
$uBase = Join-Path '{tmp_esc}' 'usb_case'
New-Item -ItemType Directory -Path $uBase | Out-Null
$uBase = (Get-Item -LiteralPath $uBase -Force).FullName
$sec = Get-PrivateSecurity
$sec.SetOwner($UserSid)
$sysDrive = [IO.Path]::GetPathRoot($env:SystemRoot)
$cleanTop = if ((Test-RootSafety $uBase) -eq $null) {{
    Join-Path $uBase 'clean_top'
}} else {{
    Join-Path $sysDrive ("OxsUsbAclTest_" + $PID + "_" + [guid]::NewGuid().ToString('N').Substring(0, 8))
}}
[void][IO.Directory]::CreateDirectory($cleanTop, $sec)
try {{
    # (a) Set-PrivateDaclNoFollow refuses a junction without touching the junction target's DACL.
    $targetOpen = Join-Path $uBase 'open_target'
    New-Item -ItemType Directory -Path $targetOpen | Out-Null
    & icacls.exe $targetOpen /grant '*S-1-1-0:(OI)(CI)R' | Out-Null
    $juncRoot = Join-Path $cleanTop 'junc_root'
    & cmd.exe /c mklink /J $juncRoot $targetOpen | Out-Null
    $resLink = Set-PrivateDaclNoFollow $juncRoot $sec $OwnerSids
    if ($resLink -ne 'link') {{ throw "expected 'link', got $resLink" }}
    if ((Get-Acl -LiteralPath $targetOpen).AreAccessRulesProtected) {{ throw "Set-PrivateDaclNoFollow modified junction target!" }}
    & cmd.exe /c rmdir $juncRoot | Out-Null

    # (b) Real open USB install root with task-created junctions inside work\\task-1 and workspaces\\ws1\\tree
    #     plus a target directory: Get-CheckedRootEntries skips them and Set-PrivateDaclNoFollow locks the root.
    $openRoot = Join-Path $cleanTop 'open_install'
    New-Item -ItemType Directory -Path $openRoot | Out-Null
    & icacls.exe $openRoot /grant '*S-1-5-11:(OI)(CI)R' | Out-Null
    if ((Get-Acl -LiteralPath $openRoot).AreAccessRulesProtected) {{ throw "expected openRoot to start with inherited/unprotected DACL" }}
    Set-Content -LiteralPath (Join-Path $openRoot '.oxideswarm-usb-install') -Value 'marker' -Encoding ASCII
    foreach ($sub in @('bin', 'home', 'run', 'logs', 'work\\.locks', 'work\\cache\\blobs\\objects', 'work\\task-99\\target\\debug', 'workspaces\\ws1\\tree\\target')) {{
        New-Item -ItemType Directory -Force -Path (Join-Path $openRoot $sub) | Out-Null
    }}
    $taskJunc = Join-Path $openRoot 'workspaces\\ws1\\tree\\dep_link'
    & cmd.exe /c mklink /J $taskJunc $targetOpen | Out-Null
    $whyOpen = Test-RootSafety $openRoot
    if ($whyOpen) {{ throw "task junction inside workspace should not block -Update: $whyOpen" }}
    $checkedPaths = @(Get-CheckedRootEntries $openRoot $true | ForEach-Object {{ $_.FullName }})
    if ($checkedPaths | Where-Object {{ $_ -like '*\\target*' -or $_ -like '*\\dep_link*' }}) {{
        throw "Get-CheckedRootEntries walked into task target/junction: $($checkedPaths -join '; ')"
    }}
    $resForeignOwner = Set-PrivateDaclNoFollow $openRoot $sec @('S-1-5-19')
    if ($resForeignOwner -notlike 'owner:*') {{ throw "expected owner:* refusal, got $resForeignOwner" }}
    $applyReal = Set-PrivateDaclNoFollow $openRoot $sec $OwnerSids
    if ($applyReal -ne $null) {{ throw "expected null on real root, got $applyReal" }}
    if (-not (Test-PrivateAcl (Get-Acl -LiteralPath $openRoot))) {{ throw "openRoot was not made private" }}

    # (c) A link at a checked control path (work\\cache) IS refused with guidance keeping <root>\\home.
    & icacls.exe $openRoot /grant '*S-1-5-11:(OI)(CI)R' | Out-Null
    Remove-Item -Recurse -Force -LiteralPath (Join-Path $openRoot 'work\\cache')
    & cmd.exe /c mklink /J (Join-Path $openRoot 'work\\cache') $targetOpen | Out-Null
    $whyCtrlLink = Test-RootSafety $openRoot
    if (-not $whyCtrlLink -or $whyCtrlLink -notmatch 'preserve cluster membership') {{
        throw "expected control-folder link refusal naming home preservation, got: $whyCtrlLink"
    }}
    & cmd.exe /c rmdir (Join-Path $openRoot 'work\\cache') | Out-Null
    & cmd.exe /c rmdir $taskJunc | Out-Null
}} finally {{
    $ErrorActionPreference = 'Continue'
    foreach ($maybeLink in @(
        (Join-Path $cleanTop 'junc_root'),
        (Join-Path $cleanTop 'open_install\\work\\cache'),
        (Join-Path $cleanTop 'open_install\\workspaces\\ws1\\tree\\dep_link')
    )) {{
        if (Test-Path -LiteralPath $maybeLink) {{
            $it = Get-Item -LiteralPath $maybeLink -Force -ErrorAction SilentlyContinue
            if ($it -and ($it.Attributes -band [IO.FileAttributes]::ReparsePoint)) {{
                & cmd.exe /c rmdir $maybeLink | Out-Null
            }}
        }}
    }}
    Remove-Item -Recurse -Force -LiteralPath $cleanTop -ErrorAction SilentlyContinue
}}
Write-Output 'USB_HELPERS_OK'
"""
            with open(usb_driver, "w", encoding="utf-8", newline="\r\n") as f:
                f.write(usb_script.replace("\r\n", "\n").replace("\n", "\r\n"))
            usb_res = subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", usb_driver],
                env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=45,
            )
            self.assertEqual(usb_res.returncode, 0, usb_res.stdout)
            self.assertIn("USB_HELPERS_OK", usb_res.stdout)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_cmd_pairing_script_does_not_strip_quotes_from_the_ticket(self):
        text = read(os.path.join(ROOT, "connect_remote.cmd"))
        self.assertNotIn('%TICKET:"=%', text, "removing every double quote destroys the JSON ticket")

    def test_reports_are_computed_not_hard_coded(self):
        for name in ("run_chaos_test.sh", "run_cluster_benchmark.sh", "test_integration.sh"):
            text = read(os.path.join(ROOT, name))
            self.assertNotRegex(text, r"100% ?PASS", name)
            self.assertNotIn('"verified_integrity": true', text, name)
        bench = read(os.path.join(ROOT, "benchmarks", "interconnect", "run_comparative_benchmark.sh"))
        self.assertNotIn("outperforms alternative solutions across all major axes", bench)

    def test_agent_mesh_launchers_support_no_shell(self):
        """Static verification that ubuntu, macos, android, and windows launchers support --no-shell."""
        launchers = [
            os.path.join(ROOT, "packaging", "agent_mesh", "ubuntu", "run_agent_node.sh"),
            os.path.join(ROOT, "packaging", "agent_mesh", "macos", "run_agent_node.sh"),
            os.path.join(ROOT, "packaging", "agent_mesh", "android", "run_agent_node_termux.sh"),
            os.path.join(ROOT, "packaging", "agent_mesh", "windows", "run_agent_node.cmd"),
        ]
        for path in launchers:
            text = read(path)
            self.assertIn("OXIDE_MESH_NO_SHELL", text, f"{path} must support OXIDE_MESH_NO_SHELL")
            self.assertIn("--no-shell", text, f"{path} must support --no-shell")

    def test_windows_ps1_agent_launcher_supports_no_shell(self):
        """SEC-15: run_agent_node.ps1 has -NoShell and OXIDE_MESH_NO_SHELL and passes --no-shell on."""
        path = os.path.join(ROOT, "packaging", "agent_mesh", "windows", "run_agent_node.ps1")
        text = read(path)
        self.assertRegex(text, r"\[switch\]\s*\$NoShell\b", "-NoShell switch parameter")
        self.assertIn("OXIDE_MESH_NO_SHELL", text)
        self.assertIn('"--no-shell"', text)
        # Both launch paths (native binary and Python fallback) must splat the flag.
        self.assertEqual(len(re.findall(r"--platform windows @NoShellArgs", text)), 2, text)
        data = read_bytes(path)
        self.assertEqual(data.count(b"\r\n"), data.count(b"\n"), "the .ps1 launcher must stay CRLF")

    def test_gitignore_covers_test_reports_and_jnilibs_so_files(self):
        probe = subprocess.run(
            ["git", "rev-parse", "--is-inside-work-tree"],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if probe.returncode != 0:
            self.skipTest("git check-ignore requires a git checkout")
        for rel in (
            "test_reports/chaos_test_report.md",
            "packaging/android/app/app/src/main/jniLibs/arm64-v8a/liboxideworker.so",
        ):
            proc = subprocess.run(
                ["git", "check-ignore", "-q", rel],
                cwd=ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self.assertEqual(
                proc.returncode,
                0,
                f".gitignore must ignore {rel} (git check-ignore exited with {proc.returncode})",
            )

    def test_fallback_scan_without_git_skips_target_and_cache_dirs(self):
        """Verify tracked_text_files fallback in a non-git directory (e.g. git archive) skips target/."""
        tmp = tempfile.mkdtemp(prefix="ox_archive_scan_")
        try:
            src_dir = os.path.join(tmp, "crates", "core", "src")
            target_dir = os.path.join(tmp, "target", "debug", "deps")
            git_dir = os.path.join(tmp, ".git", "logs")
            os.makedirs(src_dir)
            os.makedirs(target_dir)
            os.makedirs(git_dir)
            keep_file = os.path.join(src_dir, "lib.rs")
            with open(keep_file, "w", encoding="utf-8") as f:
                f.write("// clean source\n")
            with open(os.path.join(target_dir, "artifact.d"), "w", encoding="utf-8") as f:
                f.write("build artifact inside target/\n")
            with open(os.path.join(git_dir, "HEAD"), "w", encoding="utf-8") as f:
                f.write("git log inside .git/\n")
            scanned = tracked_text_files(root=tmp)
            self.assertEqual(scanned, [keep_file])
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_chaos_script_rejects_victim_done_line_after_sigkill(self):
        """FEAT-2 (Design 0031): run_chaos_test.sh must fail if an orphaned victim task writes done:${SB_VICTIM}."""
        chaos = read(os.path.join(ROOT, "run_chaos_test.sh"))
        self.assertIn('grep -q "^done:${SB_VICTIM}" "$LOG"', chaos)
        self.assertIn("kept running and wrote done:${SB_VICTIM} after the kill", chaos)

    def test_windows_usb_install_and_workspace_roots_never_collide_with_default_workspace_root(self):
        # USB-3: Windows NTFS is case-insensitive, so the USB kit's install folder and
        # --workspace-root must never equal worker::workspace::default_root_with(Platform::Windows, ..)
        # (<LOCALAPPDATA>\oxideswarm\workspaces) under any case spelling.
        ws_rs = read(os.path.join(ROOT, "crates", "worker", "src", "workspace.rs"))
        self.assertIn('Platform::Windows => PathBuf::from(get("LOCALAPPDATA")?)', ws_rs)
        self.assertIn('.join("oxideswarm").join("workspaces")', ws_rs)

        install_ps1 = read(os.path.join(ROOT, "packaging", "usb", "install.ps1"))
        uninstall_ps1 = read(os.path.join(ROOT, "packaging", "usb", "uninstall.ps1"))

        m_param = re.search(r'\[string\]\$InstallRoot\s*=\s*\(Join-Path \$env:LOCALAPPDATA "([^"]+)"\)', install_ps1)
        self.assertIsNotNone(m_param, "missing default -InstallRoot in install.ps1")
        m_def = re.search(r"\$DefaultRoot\s*=\s*Join-Path \$LocalAppDataFull '([^']+)'", install_ps1)
        self.assertIsNotNone(m_def, "missing $DefaultRoot in install.ps1")
        self.assertEqual(m_param.group(1), m_def.group(1))
        self.assertIn("$candidate = Join-Path $d.Name 'OxideSwarm-USB'", install_ps1)
        self.assertIn("Join-Path $env:LOCALAPPDATA 'OxideSwarm-USB'", uninstall_ps1)

        # Always pass --workspace-root when --workspaces is enabled, even inside %LOCALAPPDATA%.
        ws_block_start = install_ps1.index("if ($useWorkspaces -and $supportsIsolatedWorkspaces")
        ws_block_end = install_ps1.index("$log = Join-Path $LogDir 'worker.log'", ws_block_start)
        ws_block = install_ps1[ws_block_start:ws_block_end]
        self.assertNotIn("if (-not $PrivateRoot)", ws_block)
        self.assertIn(".oxideswarm-usb-workspaces", ws_block)
        self.assertIn("$workerArgs += ' --workspaces --workspace-root \"' + $WorkspaceRoot + '\"'", ws_block)
        self.assertIn("$LegacyDefaultRoot", install_ps1)
        self.assertIn("$DefaultWorkerWorkspaceRoot", install_ps1)

        # Case-insensitive equivalence check across arbitrary LocalAppData and component spellings.
        kit_dir_name = m_def.group(1)
        for local_app in (
            r"C:\Users\Alice\AppData\Local",
            r"C:\USERS\ALICE\APPDATA\LOCAL",
            r"c:\users\alice\appdata\local",
        ):
            worker_default_parent = f"{local_app}\\oxideswarm".lower()
            worker_default_root = f"{local_app}\\oxideswarm\\workspaces".lower()
            for ws_leaf in ("workspaces", "WORKSPACES", "Workspaces"):
                for root_case in (kit_dir_name, kit_dir_name.lower(), kit_dir_name.upper()):
                    kit_install = f"{local_app}\\{root_case}".lower()
                    kit_ws_root = f"{local_app}\\{root_case}\\{ws_leaf}".lower()
                    legacy_ws_root = f"{local_app}\\OxideSwarm\\usb-kit\\{ws_leaf}".lower()
                    self.assertNotEqual(kit_install, worker_default_parent)
                    self.assertNotEqual(kit_install, worker_default_root)
                    self.assertNotEqual(kit_ws_root, worker_default_root)
                    self.assertNotEqual(legacy_ws_root, worker_default_root)

        # uninstall.ps1 must check .oxideswarm-usb-workspaces and foreign users before prompt/stop/delete.
        check_idx = uninstall_ps1.index("function Get-WorkspaceDirUsers")
        marker_idx = uninstall_ps1.index("'.oxideswarm-usb-workspaces'")
        prompt_idx = uninstall_ps1.index("if (-not $Yes)")
        stop_idx = uninstall_ps1.index("Get-CimInstance Win32_Process | Where-Object { $_.ExecutablePath -eq $Exe }")
        delete_idx = uninstall_ps1.index("Remove-Item -Recurse -Force -LiteralPath $InstallRoot")
        self.assertLess(check_idx, marker_idx)
        self.assertLess(marker_idx, prompt_idx)
        self.assertLess(prompt_idx, stop_idx)
        self.assertLess(stop_idx, delete_idx)

    @unittest.skipUnless(IS_WINDOWS, "Windows PowerShell runtime test")
    def test_windows_usb3_install_folder_and_uninstall_workspace_guard(self):
        if not shutil.which("powershell"):
            self.skipTest("powershell is not available")
        import zipfile

        tmp = os.path.realpath(tempfile.mkdtemp(prefix="ox_win_usb3_"))
        tmp_home = os.path.join(tmp, "userprofile")
        tmp_localappdata = os.path.join(tmp_home, "AppData", "Local")
        tmp_appdata = os.path.join(tmp_home, "AppData", "Roaming")
        tmp_temp = os.path.join(tmp_localappdata, "Temp")
        for d in (tmp_home, tmp_localappdata, tmp_appdata, tmp_temp):
            os.makedirs(d, exist_ok=True)
        test_run_val = f"OxideSwarmUsb3Test_{os.getpid()}"
        test_pointer_key = f"HKCU:\\Software\\OxideSwarm\\UsbKitUsb3Test_{os.getpid()}"
        pids_log = os.path.join(tmp, "spawned_workers.pids")
        ps_env = {
            k: v for k, v in os.environ.items()
            if k.upper() not in ("PSMODULEPATH", "OXIDESWARM_HOME")
        }
        ps_env.update(
            USERPROFILE=tmp_home,
            HOME=tmp_home,
            LOCALAPPDATA=tmp_localappdata,
            APPDATA=tmp_appdata,
            TEMP=tmp_temp,
            TMP=tmp_temp,
            OXIDESWARM_USB_RUN_VALUE=test_run_val,
            OXIDESWARM_USB_POINTER_KEY=test_pointer_key,
            OXIDESWARM_USB_ONLINE_SUMS_URL="off",
            FAKE_OXIDE_PIDS_LOG=pids_log,
        )
        if not shutil.which("cargo", path=ps_env.get("PATH")):
            fake_cargo_dir = os.path.join(tmp, "fake_cargo_bin")
            os.makedirs(fake_cargo_dir, exist_ok=True)
            with open(os.path.join(fake_cargo_dir, "cargo.cmd"), "w", encoding="utf-8") as cf:
                cf.write("@echo off\r\necho cargo 1.90.0\r\n")
            ps_env["PATH"] = fake_cargo_dir + os.pathsep + ps_env.get("PATH", "")

        normal_proc = None
        try:
            kit_dir = os.path.join(tmp, "kit")
            os.makedirs(os.path.join(kit_dir, "archives"), exist_ok=True)
            for name in ("install.ps1", "uninstall.ps1"):
                shutil.copyfile(os.path.join(ROOT, "packaging", "usb", name), os.path.join(kit_dir, name))
            shutil.copyfile(
                os.path.join(ROOT, "windows", "revoke-isolation-acls.ps1"),
                os.path.join(kit_dir, "revoke-isolation-acls.ps1"),
            )
            with open(os.path.join(kit_dir, "join.token"), "w", encoding="utf-8") as f:
                f.write(KIT_TOKEN + "\n")
            with open(os.path.join(kit_dir, "no-autostart"), "w", encoding="utf-8") as f:
                f.write("trial\n")
            with open(os.path.join(kit_dir, "isolation"), "w", encoding="utf-8") as f:
                f.write("off\n")

            fake_exe = os.path.join(tmp, "oxideswarm.exe")
            cs_code = r"""
using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
public class Program {
    public static int Main(string[] args) {
        if (args.Length > 0 && args[0] == "--version") {
            Console.WriteLine("oxideswarm 9.9.9");
            return 0;
        }
        if (args.Length > 0 && args[0] == "join") {
            Console.In.ReadToEnd();
            return 0;
        }
        if (args.Length > 0 && args[0] == "worker") {
            string pidsLog = Environment.GetEnvironmentVariable("FAKE_OXIDE_PIDS_LOG");
            if (!string.IsNullOrEmpty(pidsLog)) {
                try { File.AppendAllText(pidsLog, Process.GetCurrentProcess().Id.ToString() + Environment.NewLine); } catch {}
            }
            Thread.Sleep(60000);
            return 0;
        }
        if (args.Length > 0 && args[0] == "workers") {
            Console.WriteLine("[{\"name\":\"win-kit-test\",\"tags\":[\"state:connected\"]}]");
            return 0;
        }
        if (args.Length > 0 && args[0] == "doctor") {
            Console.WriteLine("doctor ok");
            return 0;
        }
        return 0;
    }
}
"""
            cs_file = os.path.join(tmp, "FakeOxide.cs")
            with open(cs_file, "w", encoding="utf-8") as f:
                f.write(cs_code)
            compile_res = subprocess.run(
                [
                    "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command",
                    f"Add-Type -Path '{cs_file}' -OutputAssembly '{fake_exe}' -OutputType ConsoleApplication",
                ],
                env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=30,
            )
            self.assertEqual(compile_res.returncode, 0, compile_res.stdout)

            zip_name = "oxideswarm-9.9.9-x86_64-pc-windows-msvc.zip"
            zip_path = os.path.join(kit_dir, "archives", zip_name)
            with zipfile.ZipFile(zip_path, "w") as zf:
                zf.write(fake_exe, "oxideswarm.exe")
            zip_sha = hashlib.sha256(read_bytes(zip_path)).hexdigest()
            with open(os.path.join(kit_dir, "archives", "SHA256SUMS"), "w", encoding="utf-8") as f:
                f.write(f"{zip_sha}  {zip_name}\n")

            normal_ws_root = os.path.join(tmp_localappdata, "oxideswarm", "workspaces")
            proj_tree_hello = os.path.join(normal_ws_root, "proj-alpha", "tree", "hello.txt")
            os.makedirs(os.path.dirname(proj_tree_hello), exist_ok=True)
            with open(proj_tree_hello, "w", encoding="utf-8") as hf:
                hf.write("normal worker state\n")

            normal_worker_dir = os.path.join(tmp, "normal-worker")
            os.makedirs(normal_worker_dir, exist_ok=True)
            normal_exe = os.path.join(normal_worker_dir, "oxideswarm.exe")
            shutil.copyfile(fake_exe, normal_exe)
            normal_proc = subprocess.Popen(
                [normal_exe, "worker", "--name", "dev-local-worker", "--workspaces"],
                env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            )
            time.sleep(0.3)
            self.assertIsNone(normal_proc.poll(), "normal worker should remain running")

            for legacy_spell in ("OxideSwarm", "OXIDESWARM"):
                refused_legacy = subprocess.run(
                    [
                        "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                        "-File", os.path.join(kit_dir, "install.ps1"),
                        "-InstallRoot", os.path.join(tmp_localappdata, legacy_spell),
                        "-Name", "win-kit-test", "-Isolation", "off",
                    ],
                    env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, timeout=30,
                )
                self.assertNotEqual(refused_legacy.returncode, 0, refused_legacy.stdout)
                self.assertIn("default worker workspace root", refused_legacy.stdout)

            inst_default = subprocess.run(
                [
                    "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                    "-File", os.path.join(kit_dir, "install.ps1"),
                    "-Name", "win-kit-test", "-Isolation", "off",
                ],
                env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=60,
            )
            self.assertEqual(inst_default.returncode, 0, inst_default.stdout)
            default_usb_root = os.path.join(tmp_localappdata, "OxideSwarm-USB")
            default_usb_ws = os.path.join(default_usb_root, "workspaces")
            self.assertTrue(os.path.isdir(default_usb_root))
            self.assertTrue(os.path.isfile(os.path.join(default_usb_ws, ".oxideswarm-usb-workspaces")))
            vbs_default = read(os.path.join(default_usb_root, "start-worker.vbs"))
            self.assertIn(f'--workspaces --workspace-root ""{default_usb_ws}""', vbs_default)

            uninst_default = subprocess.run(
                [
                    "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                    "-File", os.path.join(kit_dir, "uninstall.ps1"),
                    "-Yes",
                ],
                env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=60,
            )
            self.assertEqual(uninst_default.returncode, 0, uninst_default.stdout)
            self.assertFalse(os.path.exists(default_usb_root))
            self.assertIsNone(normal_proc.poll(), "normal worker must keep running across USB uninstall")
            self.assertEqual(read(proj_tree_hello), "normal worker state\n")

            legacy_root = os.path.join(tmp_localappdata, "OxideSwarm")
            os.makedirs(os.path.join(legacy_root, "bin"), exist_ok=True)
            os.makedirs(os.path.join(legacy_root, "home"), exist_ok=True)
            shutil.copyfile(fake_exe, os.path.join(legacy_root, "bin", "oxideswarm.exe"))
            uninst_blocked = subprocess.run(
                [
                    "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                    "-File", os.path.join(kit_dir, "uninstall.ps1"),
                    "-Yes",
                ],
                env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=30,
            )
            self.assertNotEqual(uninst_blocked.returncode, 0, uninst_blocked.stdout)
            self.assertIn("dev-local-worker", uninst_blocked.stdout)
            self.assertIn(str(normal_proc.pid), uninst_blocked.stdout)
            self.assertTrue(os.path.isdir(legacy_root))
            self.assertEqual(read(proj_tree_hello), "normal worker state\n")

            normal_proc.terminate()
            normal_proc.wait(timeout=10)
            if normal_proc.stdout is not None:
                normal_proc.stdout.close()
            normal_proc = None

            uninst_unmarked = subprocess.run(
                [
                    "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                    "-File", os.path.join(kit_dir, "uninstall.ps1"),
                    "-Yes",
                ],
                env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=30,
            )
            self.assertNotEqual(uninst_unmarked.returncode, 0, uninst_unmarked.stdout)
            self.assertIn("default workspace root", uninst_unmarked.stdout)
            self.assertIn("missing .oxideswarm-usb-workspaces", uninst_unmarked.stdout)
            self.assertTrue(os.path.isdir(legacy_root))
            self.assertEqual(read(proj_tree_hello), "normal worker state\n")
        finally:
            if normal_proc is not None:
                if normal_proc.poll() is None:
                    normal_proc.terminate()
                    try:
                        normal_proc.wait(timeout=10)
                    except Exception:
                        normal_proc.kill()
                        normal_proc.wait(timeout=5)
                if normal_proc.stdout is not None:
                    normal_proc.stdout.close()
            for pid_candidate in (
                pids_log,
                os.path.join(tmp_localappdata, "OxideSwarm-USB", "run", "worker.pid"),
            ):
                if os.path.isfile(pid_candidate):
                    try:
                        for line in read(pid_candidate).splitlines():
                            p_str = line.strip()
                            if p_str.isdigit():
                                subprocess.run(
                                    ["powershell", "-NoProfile", "-Command", f"Stop-Process -Id {p_str} -Force -ErrorAction SilentlyContinue"],
                                    env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10,
                                )
                    except Exception:
                        pass
            subprocess.run(
                [
                    "powershell", "-NoProfile", "-Command",
                    f"Remove-ItemProperty -Path 'HKCU:\\Software\\Microsoft\\Windows\\CurrentVersion\\Run' -Name '{test_run_val}' -ErrorAction SilentlyContinue; Remove-Item -Path '{test_pointer_key}' -Recurse -Force -ErrorAction SilentlyContinue",
                ],
                env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10,
            )
            shutil.rmtree(tmp, ignore_errors=True)

    @unittest.skipUnless(IS_WINDOWS, "Windows PowerShell USB v1 kit test")
    def test_windows_usb_v1_kit_passphrase_roles_online_sums_and_rollback(self):
        tmp = tempfile.mkdtemp(prefix="ox_usb1_ps_")
        tmp_home = os.path.join(tmp, "home")
        tmp_localappdata = os.path.join(tmp, "localappdata")
        tmp_appdata = os.path.join(tmp, "appdata")
        tmp_temp = os.path.join(tmp, "temp")
        tmp_client_localappdata = os.path.join(tmp, "localappdata_client")
        for d in (tmp_home, tmp_localappdata, tmp_client_localappdata, tmp_appdata, tmp_temp):
            os.makedirs(d, exist_ok=True)
        test_run_val = f"OxideSwarmUsbWorkerV1Test_{os.getpid()}"
        test_pointer_key = rf"HKCU:\Software\OxideSwarm\UsbKitV1Test_{os.getpid()}"
        pids_log = os.path.join(tmp, "spawned_v1.pids")
        args_log = os.path.join(tmp, "v1_args.log")
        ps_env = {
            k: v for k, v in os.environ.items()
            if k.upper() not in ("PSMODULEPATH", "OXIDESWARM_HOME")
        }
        ps_env.update(
            USERPROFILE=tmp_home,
            HOME=tmp_home,
            LOCALAPPDATA=tmp_localappdata,
            APPDATA=tmp_appdata,
            TEMP=tmp_temp,
            TMP=tmp_temp,
            OXIDESWARM_USB_RUN_VALUE=test_run_val,
            OXIDESWARM_USB_POINTER_KEY=test_pointer_key,
            OXIDESWARM_USB_ONLINE_SUMS_URL="off",
            FAKE_OXIDE_PIDS_LOG=pids_log,
            FAKE_ARGS_LOG=args_log,
            FAKE_WORKER_NAME="v1-win-worker",
        )
        if not shutil.which("cargo", path=ps_env.get("PATH")):
            fake_cargo_dir = os.path.join(tmp, "fake_cargo_bin")
            os.makedirs(fake_cargo_dir, exist_ok=True)
            with open(os.path.join(fake_cargo_dir, "cargo.cmd"), "w", encoding="utf-8") as cf:
                cf.write("@echo off\r\necho cargo 1.90.0\r\n")
            ps_env["PATH"] = fake_cargo_dir + os.pathsep + ps_env.get("PATH", "")

        root_worker = os.path.join(tmp_localappdata, "OxideSwarm-USB")
        root_client = os.path.join(tmp_client_localappdata, "OxideSwarm-USB")
        try:
            kit_dir = os.path.join(tmp, "kit")
            os.makedirs(os.path.join(kit_dir, "archives"), exist_ok=True)
            for name in ("install.ps1", "uninstall.ps1"):
                shutil.copyfile(os.path.join(ROOT, "packaging", "usb", name), os.path.join(kit_dir, name))
            shutil.copyfile(
                os.path.join(ROOT, "windows", "revoke-isolation-acls.ps1"),
                os.path.join(kit_dir, "revoke-isolation-acls.ps1"),
            )
            with open(os.path.join(kit_dir, "invite.oxs"), "wb") as f:
                f.write(b"OXSINV01fake-encrypted-payload")
            with open(os.path.join(kit_dir, "isolation"), "w", encoding="utf-8") as f:
                f.write("off\n")

            def write_kit_json(roles, workspaces=False):
                meta = {
                    "kit_format": 1,
                    "version": "9.9.9",
                    "created_at": 1700000000,
                    "expires_at": 1900000000,
                    "machines": 2,
                    "roles": roles,
                    "workspaces": workspaces,
                    "gpu": False,
                    "require_approval": True,
                    "invite_id": "0123456789abcdef0123456789abcdef",
                }
                with open(os.path.join(kit_dir, "kit.json"), "w", encoding="utf-8") as mf:
                    json.dump(meta, mf)

            fake_exe = os.path.join(tmp, "oxideswarm.exe")
            cs_code = r"""
using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
public class Program {
    public static int Main(string[] args) {
        string argsLog = Environment.GetEnvironmentVariable("FAKE_ARGS_LOG");
        if (!string.IsNullOrEmpty(argsLog)) {
            try { File.AppendAllText(argsLog, string.Join(" ", args) + Environment.NewLine); } catch {}
        }
        if (args.Length > 0 && args[0] == "--version") {
            Console.WriteLine("oxideswarm 9.9.9");
            return 0;
        }
        if (args.Length > 0 && args[0] == "join") {
            string inviteFile = null;
            for (int i = 1; i < args.Length - 1; i++) {
                if (args[i] == "--invite-file") { inviteFile = args[i + 1]; break; }
            }
            if (string.IsNullOrEmpty(inviteFile) || !File.Exists(inviteFile)) {
                Console.Error.WriteLine("missing invite file");
                return 3;
            }
            string pass = Console.In.ReadLine() ?? "";
            if (string.IsNullOrEmpty(pass) || pass == "wrong-passphrase") {
                Console.Error.WriteLine("Cannot unlock invite.oxs: decryption failed");
                return 3;
            }
            string oxHome = Environment.GetEnvironmentVariable("OXIDESWARM_HOME");
            if (string.IsNullOrEmpty(oxHome)) {
                oxHome = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".oxideswarm");
            }
            Directory.CreateDirectory(oxHome);
            File.WriteAllText(Path.Combine(oxHome, "node.key"), "fake-node-key\n");
            string mkey = Environment.GetEnvironmentVariable("FAKE_MASTER_KEY");
            if (string.IsNullOrEmpty(mkey)) {
                mkey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
            }
            File.WriteAllText(Path.Combine(oxHome, "rusty-grid.toml"), "[worker]\nmaster_key = '" + mkey + "'\n");
            File.WriteAllText(Path.Combine(oxHome, "joined"), "1\n");
            return 0;
        }
        if (args.Length > 0 && args[0] == "worker") {
            string pidsLog = Environment.GetEnvironmentVariable("FAKE_OXIDE_PIDS_LOG");
            if (!string.IsNullOrEmpty(pidsLog)) {
                try { File.AppendAllText(pidsLog, Process.GetCurrentProcess().Id.ToString() + Environment.NewLine); } catch {}
            }
            Thread.Sleep(60000);
            return 0;
        }
        if (args.Length > 0 && args[0] == "workers") {
            string wname = Environment.GetEnvironmentVariable("FAKE_WORKER_NAME") ?? "v1-win-worker";
            Console.WriteLine("[{\"name\":\"" + wname + "\",\"tags\":[\"state:connected\"]}]");
            return 0;
        }
        if (args.Length > 0 && args[0] == "doctor") {
            Console.WriteLine("doctor ok");
            return 0;
        }
        return 0;
    }
}
"""
            cs_file = os.path.join(tmp, "FakeOxideV1.cs")
            with open(cs_file, "w", encoding="utf-8") as f:
                f.write(cs_code)
            compile_res = subprocess.run(
                [
                    "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command",
                    f"Add-Type -Path '{cs_file}' -OutputAssembly '{fake_exe}' -OutputType ConsoleApplication",
                ],
                env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=30,
            )
            self.assertEqual(compile_res.returncode, 0, compile_res.stdout)

            zip_name = "oxideswarm-9.9.9-x86_64-pc-windows-msvc.zip"
            zip_path = os.path.join(kit_dir, "archives", zip_name)
            with zipfile.ZipFile(zip_path, "w") as zf:
                zf.write(fake_exe, "oxideswarm.exe")
            zip_sha = hashlib.sha256(read_bytes(zip_path)).hexdigest()
            good_sums_text = f"{zip_sha}  {zip_name}\n"
            with open(os.path.join(kit_dir, "archives", "SHA256SUMS"), "w", encoding="utf-8") as f:
                f.write(good_sums_text)

            good_online = os.path.join(tmp, "online_good_SHA256SUMS")
            bad_online = os.path.join(tmp, "online_bad_SHA256SUMS")
            with open(good_online, "w", encoding="utf-8") as f:
                f.write(good_sums_text)
            with open(bad_online, "w", encoding="utf-8") as f:
                f.write(("0" * 64) + f"  {zip_name}\n")

            write_kit_json(["worker"], workspaces=False)
            install_ps1 = os.path.join(kit_dir, "install.ps1")
            uninstall_ps1 = os.path.join(kit_dir, "uninstall.ps1")

            # 1. Non-HTTPS URL must be refused cleanly.
            env_http = dict(ps_env, OXIDESWARM_USB_ONLINE_SUMS_URL="http://example.com/SHA256SUMS")
            res_http = subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", install_ps1,
                 "-Name", "v1-win-worker", "-InstallRoot", root_worker, "-NoAutostart"],
                env=env_http, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=60,
            )
            self.assertNotEqual(res_http.returncode, 0, res_http.stdout)
            self.assertIn("only https:// or file:// is allowed", res_http.stdout)
            self.assertFalse(os.path.exists(os.path.join(root_worker, "bin", "oxideswarm.exe")))

            # 2. Mismatched file:/// online SHA256SUMS must fail before unpacking.
            env_bad_sum = dict(ps_env, OXIDESWARM_USB_ONLINE_SUMS_URL=pathlib.Path(bad_online).as_uri())
            res_bad_sum = subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", install_ps1,
                 "-Name", "v1-win-worker", "-InstallRoot", root_worker, "-NoAutostart"],
                env=env_bad_sum, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=60,
            )
            self.assertNotEqual(res_bad_sum.returncode, 0, res_bad_sum.stdout)
            self.assertNotIn("on the drive this kit runs from", res_bad_sum.stdout)
            self.assertFalse(os.path.exists(args_log))
            self.assertFalse(os.path.exists(os.path.join(root_worker, "bin", "oxideswarm.exe")))

            # 3. Wrong passphrase triggers fresh-install rollback: removes bin, node.key, InstallRoot, PointerKey.
            env_good_online = dict(ps_env, OXIDESWARM_USB_ONLINE_SUMS_URL=pathlib.Path(good_online).as_uri())
            res_bad_pass = subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", install_ps1,
                 "-Name", "v1-win-worker", "-InstallRoot", root_worker, "-NoAutostart"],
                env=env_good_online, input="wrong-passphrase\r\n", stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, text=True, timeout=60,
            )
            self.assertNotEqual(res_bad_pass.returncode, 0, res_bad_pass.stdout)
            self.assertTrue(os.path.isfile(args_log), res_bad_pass.stdout)
            self.assertTrue(any(l.startswith("join --invite-file ") for l in read(args_log).splitlines()), res_bad_pass.stdout)
            self.assertFalse(os.path.exists(os.path.join(root_worker, "bin", "oxideswarm.exe")))
            self.assertFalse(os.path.exists(os.path.join(root_worker, "home", "node.key")))
            self.assertFalse(os.path.exists(root_worker))
            ptr_chk = subprocess.run(
                ["powershell", "-NoProfile", "-Command", f"if (Test-Path -Path '{test_pointer_key}') {{ exit 1 }} else {{ exit 0 }}"],
                env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=15,
            )
            self.assertEqual(ptr_chk.returncode, 0, f"Rollback left PointerKey behind: {ptr_chk.stdout}")

            # 4. Valid passphrase on worker-only v1 kit: installs, skips doctor & --workspaces, writes install.log.
            with open(args_log, "w", encoding="utf-8") as af:
                af.write("")
            res_ok = subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", install_ps1,
                 "-Name", "v1-win-worker", "-InstallRoot", root_worker, "-NoAutostart"],
                env=env_good_online, input="correct horse battery staple\r\ny\r\n", stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, text=True, timeout=90,
            )
            self.assertEqual(res_ok.returncode, 0, res_ok.stdout)
            self.assertTrue(os.path.isfile(os.path.join(root_worker, "home", "node.key")))
            self.assertTrue(os.path.isfile(os.path.join(root_worker, "home", "rusty-grid.toml")))
            self.assertTrue(os.path.isfile(os.path.join(root_worker, "home", "worker_id.v1-win-worker")))
            self.assertTrue(os.path.isfile(os.path.join(root_worker, "logs", "install.log")))

            arg_lines = read(args_log).splitlines()
            self.assertTrue(any(l.startswith("join --invite-file ") for l in arg_lines), arg_lines)
            worker_lines = [l for l in arg_lines if l.startswith("worker ")]
            self.assertTrue(worker_lines and "--name v1-win-worker" in worker_lines[-1], worker_lines)
            self.assertNotIn("--workspaces", worker_lines[-1])
            self.assertFalse(any(l == "doctor" or l.startswith("doctor ") for l in arg_lines), arg_lines)

            # uninstall.ps1 -Yes -KeepKey removes bin and PointerKey while keeping home\node.key.
            res_un = subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", uninstall_ps1,
                 "-InstallRoot", root_worker, "-Yes", "-KeepKey"],
                env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=60,
            )
            self.assertEqual(res_un.returncode, 0, res_un.stdout)
            self.assertFalse(os.path.exists(os.path.join(root_worker, "bin", "oxideswarm.exe")))
            self.assertTrue(os.path.isfile(os.path.join(root_worker, "home", "node.key")))
            ptr_chk2 = subprocess.run(
                ["powershell", "-NoProfile", "-Command", f"if (Test-Path -Path '{test_pointer_key}') {{ exit 1 }} else {{ exit 0 }}"],
                env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=15,
            )
            self.assertEqual(ptr_chk2.returncode, 0, f"uninstall.ps1 -KeepKey left PointerKey: {ptr_chk2.stdout}")

            # 5. Client-only v1 kit runs doctor and skips starting a worker.
            with open(args_log, "w", encoding="utf-8") as af:
                af.write("")
            write_kit_json(["client"], workspaces=False)
            client_env = dict(ps_env, LOCALAPPDATA=tmp_client_localappdata)
            res_cli = subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", install_ps1,
                 "-Name", "v1-win-client", "-InstallRoot", root_client, "-NoAutostart"],
                env=client_env, input="correct horse battery staple\r\ny\r\n", stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, text=True, timeout=90,
            )
            self.assertEqual(res_cli.returncode, 0, res_cli.stdout)
            cli_args = read(args_log).splitlines()
            self.assertTrue(any(l == "doctor" or l.startswith("doctor ") for l in cli_args), cli_args)
            self.assertFalse(any(l.startswith("worker ") for l in cli_args), cli_args)
            res_un_cli = subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", uninstall_ps1,
                 "-InstallRoot", root_client, "-Yes"],
                env=client_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=60,
            )
            self.assertEqual(res_un_cli.returncode, 0, res_un_cli.stdout)
            self.assertFalse(os.path.exists(root_client))
        finally:
            for pid_candidate in (
                pids_log,
                os.path.join(root_worker, "run", "worker.pid"),
                os.path.join(root_client, "run", "worker.pid"),
            ):
                if os.path.isfile(pid_candidate):
                    try:
                        for line in read(pid_candidate).splitlines():
                            p_str = line.strip()
                            if p_str.isdigit():
                                subprocess.run(
                                    ["powershell", "-NoProfile", "-Command", f"Stop-Process -Id {p_str} -Force -ErrorAction SilentlyContinue"],
                                    env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10,
                                )
                    except Exception:
                        pass
            subprocess.run(
                [
                    "powershell", "-NoProfile", "-Command",
                    f"Remove-ItemProperty -Path 'HKCU:\\Software\\Microsoft\\Windows\\CurrentVersion\\Run' -Name '{test_run_val}' -ErrorAction SilentlyContinue; Remove-Item -Path '{test_pointer_key}' -Recurse -Force -ErrorAction SilentlyContinue",
                ],
                env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10,
            )
            shutil.rmtree(tmp, ignore_errors=True)

    @unittest.skipUnless(IS_WINDOWS, "Windows PowerShell USB kit test")
    def test_windows_usb_drive_menu_old_install_uninstall_and_failed_start(self):
        # BUG-29, BUG-30 and BUG-31 of 2026-11-18, run against install.ps1 and uninstall.ps1.
        if not shutil.which("powershell"):
            self.skipTest("powershell is not available")
        tmp = os.path.realpath(tempfile.mkdtemp(prefix="ox_win_usb29_"))
        tmp_home = os.path.join(tmp, "userprofile")
        tmp_localappdata = os.path.join(tmp_home, "AppData", "Local")
        tmp_appdata = os.path.join(tmp_home, "AppData", "Roaming")
        tmp_temp = os.path.join(tmp_localappdata, "Temp")
        for d in (tmp_home, tmp_localappdata, tmp_appdata, tmp_temp):
            os.makedirs(d, exist_ok=True)
        test_run_val = f"OxideSwarmUsb29Test_{os.getpid()}"
        test_pointer_key = f"HKCU:\\Software\\OxideSwarm\\UsbKitUsb29Test_{os.getpid()}"
        pids_log = os.path.join(tmp, "spawned_workers.pids")
        ps_env = {
            k: v for k, v in os.environ.items()
            if k.upper() not in ("PSMODULEPATH", "OXIDESWARM_HOME")
        }
        ps_env.update(
            USERPROFILE=tmp_home,
            HOME=tmp_home,
            LOCALAPPDATA=tmp_localappdata,
            APPDATA=tmp_appdata,
            TEMP=tmp_temp,
            TMP=tmp_temp,
            OXIDESWARM_USB_RUN_VALUE=test_run_val,
            OXIDESWARM_USB_POINTER_KEY=test_pointer_key,
            OXIDESWARM_USB_ONLINE_SUMS_URL="off",
            FAKE_OXIDE_PIDS_LOG=pids_log,
        )

        def powershell(script, *args, env=None, timeout=90):
            return subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", script] + list(args),
                env=env or ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=timeout,
            )

        other_proc = None
        try:
            kit_dir = os.path.join(tmp, "kit")
            os.makedirs(os.path.join(kit_dir, "archives"), exist_ok=True)
            for name in ("install.ps1", "uninstall.ps1"):
                shutil.copyfile(os.path.join(ROOT, "packaging", "usb", name), os.path.join(kit_dir, name))
            shutil.copyfile(
                os.path.join(ROOT, "windows", "revoke-isolation-acls.ps1"),
                os.path.join(kit_dir, "revoke-isolation-acls.ps1"),
            )
            with open(os.path.join(kit_dir, "join.token"), "w", encoding="utf-8") as f:
                f.write(KIT_TOKEN + "\n")
            with open(os.path.join(kit_dir, "no-autostart"), "w", encoding="utf-8") as f:
                f.write("trial\n")
            with open(os.path.join(kit_dir, "isolation"), "w", encoding="utf-8") as f:
                f.write("off\n")

            fake_exe = os.path.join(tmp, "oxideswarm.exe")
            cs_code = r"""
using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
public class Program {
    public static int Main(string[] args) {
        if (args.Length > 0 && args[0] == "--version") {
            Console.WriteLine("oxideswarm 9.9.9");
            return 0;
        }
        if (args.Length > 0 && args[0] == "join") {
            Console.In.ReadToEnd();
            return 0;
        }
        if (args.Length > 0 && args[0] == "worker") {
            string pidsLog = Environment.GetEnvironmentVariable("FAKE_OXIDE_PIDS_LOG");
            if (!string.IsNullOrEmpty(pidsLog)) {
                try { File.AppendAllText(pidsLog, Process.GetCurrentProcess().Id.ToString() + Environment.NewLine); } catch {}
            }
            string exitAfter = Environment.GetEnvironmentVariable("FAKE_WORKER_EXIT_AFTER_MS");
            if (!string.IsNullOrEmpty(exitAfter)) {
                for (int i = 1; i <= 40; i++) Console.WriteLine("fake-log-line-" + i.ToString("00"));
                Thread.Sleep(int.Parse(exitAfter));
                Console.WriteLine("fake worker: FAKE-STOP-MARKER the sandbox base is refused");
                string code = Environment.GetEnvironmentVariable("FAKE_WORKER_EXIT_CODE");
                return string.IsNullOrEmpty(code) ? 1 : int.Parse(code);
            }
            Thread.Sleep(60000);
            return 0;
        }
        if (args.Length > 0 && args[0] == "workers") {
            if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("FAKE_WORKERS_EMPTY"))) {
                Console.WriteLine("[]");
                return 0;
            }
            Console.WriteLine("[{\"name\":\"win-kit-test\",\"tags\":[\"state:connected\"]}]");
            return 0;
        }
        if (args.Length > 0 && args[0] == "doctor") {
            Console.WriteLine("doctor ok");
            return 0;
        }
        return 0;
    }
}
"""
            cs_file = os.path.join(tmp, "FakeOxide.cs")
            with open(cs_file, "w", encoding="utf-8") as f:
                f.write(cs_code)
            compile_res = subprocess.run(
                [
                    "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command",
                    f"Add-Type -Path '{cs_file}' -OutputAssembly '{fake_exe}' -OutputType ConsoleApplication",
                ],
                env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=60,
            )
            self.assertEqual(compile_res.returncode, 0, compile_res.stdout)
            zip_name = "oxideswarm-9.9.9-x86_64-pc-windows-msvc.zip"
            zip_path = os.path.join(kit_dir, "archives", zip_name)
            with zipfile.ZipFile(zip_path, "w") as zf:
                zf.write(fake_exe, "oxideswarm.exe")
            zip_sha = hashlib.sha256(read_bytes(zip_path)).hexdigest()
            with open(os.path.join(kit_dir, "archives", "SHA256SUMS"), "w", encoding="utf-8") as f:
                f.write(f"{zip_sha}  {zip_name}\n")
            install_ps1 = os.path.join(kit_dir, "install.ps1")
            uninstall_ps1 = os.path.join(kit_dir, "uninstall.ps1")
            default_root = os.path.join(tmp_localappdata, "OxideSwarm-USB")

            # BUG-29: a folder whose way another account can change (here Authenticated Users may
            # modify a folder above it, as on the root of the laptop's E:) is listed as not usable,
            # without a number; the default stays first.
            shared = os.path.join(tmp, "shared-drive")
            os.makedirs(shared)
            grant = subprocess.run(["icacls", shared, "/grant", "*S-1-5-11:(OI)(CI)M"],
                                   stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   text=True, timeout=30)
            self.assertEqual(grant.returncode, 0, grant.stdout)
            refused_choice = os.path.join(shared, "OxideSwarm-USB")
            menu = powershell(install_ps1, "-Name", "win-menu",
                              env=dict(ps_env, OXIDESWARM_USB_DRIVE_CHOICES=refused_choice,
                                       OXIDESWARM_USB_LIST_FOLDERS="1"))
            self.assertEqual(menu.returncode, 0, menu.stdout)
            lines = menu.stdout.splitlines()
            first = [l for l in lines if l.strip().startswith("1)")]
            self.assertTrue(first and "(default)" in first[0] and "OxideSwarm-USB" in first[0], menu.stdout)
            refused = [l for l in lines if "shared-drive" in l]
            self.assertEqual(len(refused), 1, menu.stdout)
            self.assertIn("not usable", refused[0])
            self.assertIn("can rename or change", refused[0])
            self.assertFalse(re.match(r"^\s*\d+\)", refused[0]), "a refused folder must not be offered as a choice")
            self.assertFalse(os.path.exists(default_root), "listing the folders must install nothing")

            # BUG-32: under -Isolation fs and workspaces, install.ps1 stops before creating
            # InstallRoot when -InstallRoot puts <install>\workspaces inside %USERPROFILE% at any
            # path other than the two allowlisted USB-kit workspace roots.
            custom_local_root = os.path.join(tmp_localappdata, "custom-kit")
            bad_local_res = powershell(
                install_ps1, "-InstallRoot", custom_local_root, "-Name", "win-bug32a",
                "-Isolation", "fs", "-Workspaces", "-NoAutostart",
            )
            self.assertNotEqual(bad_local_res.returncode, 0, bad_local_res.stdout)
            self.assertIn("with -Isolation fs and workspaces", bad_local_res.stdout)
            self.assertIn("OxideSwarm-USB", bad_local_res.stdout)
            self.assertIn("outside", bad_local_res.stdout)
            self.assertFalse(os.path.exists(custom_local_root), bad_local_res.stdout)

            custom_profile_root = os.path.join(tmp_home, "custom-in-profile")
            bad_profile_res = powershell(
                install_ps1, "-InstallRoot", custom_profile_root, "-Name", "win-bug32b",
                "-Isolation", "fs", "-Workspaces", "-NoAutostart",
            )
            self.assertNotEqual(bad_profile_res.returncode, 0, bad_profile_res.stdout)
            self.assertIn("with -Isolation fs and workspaces", bad_profile_res.stdout)
            self.assertFalse(os.path.exists(custom_profile_root), bad_profile_res.stdout)

            # ... while the default %LOCALAPPDATA%\OxideSwarm-USB root succeeds under -Isolation fs
            # with workspaces and writes the .oxideswarm-usb-workspaces marker.
            ok_fs_res = powershell(
                install_ps1, "-InstallRoot", default_root, "-Name", "win-kit-test",
                "-Isolation", "fs", "-Workspaces", "-NoAutostart", timeout=120,
            )
            self.assertEqual(ok_fs_res.returncode, 0, ok_fs_res.stdout)
            self.assertIn("connected (isolation: fs)", ok_fs_res.stdout)
            ok_fs_vbs = read(os.path.join(default_root, "start-worker.vbs"))
            self.assertIn("--isolation fs", ok_fs_vbs)
            if shutil.which("cargo"):
                self.assertIn("--workspaces --workspace-root", ok_fs_vbs)
                self.assertTrue(
                    os.path.isfile(os.path.join(default_root, "workspaces", ".oxideswarm-usb-workspaces"))
                )
            un_ok_fs = powershell(uninstall_ps1, "-InstallRoot", default_root, "-Yes")
            self.assertEqual(un_ok_fs.returncode, 0, un_ok_fs.stdout)
            self.assertFalse(os.path.exists(default_root))

            # BUG-31: a worker that stops 5 s after its start is reported as stopped with its exit
            # code, the end of its log is printed, and the rollback keeps the log the error names.
            stop_root = os.path.join(tmp_localappdata, "stop-root")
            stop_env = dict(ps_env, FAKE_WORKER_EXIT_AFTER_MS="5000", FAKE_WORKER_EXIT_CODE="3",
                            FAKE_WORKERS_EMPTY="1")
            stopped = powershell(install_ps1, "-InstallRoot", stop_root, "-Name", "win-stops",
                                 "-Isolation", "off", "-NoAutostart", env=stop_env, timeout=120)
            out = stopped.stdout
            self.assertNotEqual(stopped.returncode, 0, out)
            self.assertIn("the worker stopped", out)
            self.assertIn("exit code 3", out)
            self.assertNotIn("does not list it", out)
            self.assertIn("last 30 lines of", out)
            self.assertIn("FAKE-STOP-MARKER", out)
            self.assertIn("fake-log-line-40", out)
            self.assertNotIn("fake-log-line-01", out)
            stop_log = os.path.join(stop_root, "logs", "worker.log")
            self.assertTrue(os.path.isfile(stop_log), out)
            self.assertFalse(os.path.exists(os.path.join(stop_root, "bin")), out)
            error_line = [l for l in out.splitlines() if l.startswith("ERROR:")][-1]
            self.assertIn("worker.log", error_line)
            # The default install keeps its task folders in <install folder>\work (isolation off).
            self.assertIn(os.path.join("stop-root", "work"), out)

            # BUG-30: an install from a kit older than the workspaces marker is removed.
            def old_install(name):
                root = os.path.join(tmp, name, "OxideSwarm")
                os.makedirs(os.path.join(root, "bin"))
                os.makedirs(os.path.join(root, "home"))
                os.makedirs(os.path.join(root, "workspaces", "proj", "tree"))
                shutil.copyfile(fake_exe, os.path.join(root, "bin", "oxideswarm.exe"))
                open(os.path.join(root, ".oxideswarm-usb-install"), "w").close()
                with open(os.path.join(root, "workspaces", "proj", "tree", "a.txt"), "w", encoding="utf-8") as f:
                    f.write("old workspace\n")
                return root

            old_root = old_install("old-kit")
            removed = powershell(uninstall_ps1, "-InstallRoot", old_root, "-Yes")
            self.assertEqual(removed.returncode, 0, removed.stdout)
            self.assertIn("an install from an older kit", removed.stdout)
            self.assertFalse(os.path.exists(old_root))

            # ... but a workspaces folder another worker uses is still refused,
            busy_root = old_install("busy-kit")
            busy_ws = os.path.join(busy_root, "workspaces")
            other_dir = os.path.join(tmp, "other-worker")
            os.makedirs(other_dir)
            other_exe = os.path.join(other_dir, "oxideswarm.exe")
            shutil.copyfile(fake_exe, other_exe)
            other_proc = subprocess.Popen(
                [other_exe, "worker", "--name", "other-worker", "--workspaces", "--workspace-root", busy_ws],
                env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            self.assertTrue(wait_until(lambda: os.path.isfile(pids_log) and str(other_proc.pid) in read(pids_log),
                                       timeout=30))
            busy = powershell(uninstall_ps1, "-InstallRoot", busy_root, "-Yes")
            self.assertNotEqual(busy.returncode, 0, busy.stdout)
            self.assertIn("other-worker", busy.stdout)
            self.assertTrue(os.path.isfile(os.path.join(busy_ws, "proj", "tree", "a.txt")))
            other_proc.kill()
            other_proc.wait(timeout=30)
            other_proc = None

            # ... and so is a folder without the installer's marker.
            unmarked_root = old_install("unmarked-kit")
            os.remove(os.path.join(unmarked_root, ".oxideswarm-usb-install"))
            unmarked = powershell(uninstall_ps1, "-InstallRoot", unmarked_root, "-Yes")
            self.assertNotEqual(unmarked.returncode, 0, unmarked.stdout)
            self.assertIn(".oxideswarm-usb-install", unmarked.stdout)
            self.assertTrue(os.path.isdir(os.path.join(unmarked_root, "workspaces")))

            # ... and so is a directory masquerading as the .oxideswarm-usb-workspaces marker.
            dir_marker_root = old_install("dir-marker-kit")
            os.makedirs(os.path.join(dir_marker_root, "workspaces", ".oxideswarm-usb-workspaces"))
            dir_marker_res = powershell(uninstall_ps1, "-InstallRoot", dir_marker_root, "-Yes")
            self.assertNotEqual(dir_marker_res.returncode, 0, dir_marker_res.stdout)
            self.assertTrue(os.path.isdir(os.path.join(dir_marker_root, "workspaces")))
        finally:
            if other_proc is not None and other_proc.poll() is None:
                other_proc.kill()
                try:
                    other_proc.wait(timeout=30)
                except Exception:
                    pass
            if os.path.isfile(pids_log):
                for line in read(pids_log).splitlines():
                    if line.strip().isdigit():
                        subprocess.run(
                            ["powershell", "-NoProfile", "-Command",
                             f"Stop-Process -Id {line.strip()} -Force -ErrorAction SilentlyContinue"],
                            env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL, timeout=30,
                        )
            subprocess.run(
                [
                    "powershell", "-NoProfile", "-Command",
                    f"Remove-ItemProperty -Path 'HKCU:\\Software\\Microsoft\\Windows\\CurrentVersion\\Run' -Name '{test_run_val}' -ErrorAction SilentlyContinue; Remove-Item -Path '{test_pointer_key}' -Recurse -Force -ErrorAction SilentlyContinue",
                ],
                env=ps_env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30,
            )
            shutil.rmtree(tmp, ignore_errors=True)

    def test_windows_ci_feat3_dedup_cannot_drop_step7_or_new_tests(self):
        ci_text = read(os.path.join(ROOT, ".github", "workflows", "ci.yml"))
        step7_marker = "- name: Windows AppContainer isolation tests (--test-threads=1 --nocapture)"
        step8_marker = "- name: cargo test --workspace --locked --no-fail-fast"
        diag_marker = "- name: Diagnostics after the workspace tests"
        self.assertIn(step7_marker, ci_text)
        self.assertIn(step8_marker, ci_text)
        self.assertIn(diag_marker, ci_text)

        step7_idx = ci_text.index(step7_marker)
        step8_idx = ci_text.index(step8_marker)
        diag_idx = ci_text.index(diag_marker, step8_idx)
        self.assertLess(step7_idx, step8_idx)

        step7_block = ci_text[step7_idx:step8_idx]
        step8_block = ci_text[step8_idx:diag_idx]

        self.assertIn("matrix.os == 'windows-latest'", step7_block)
        self.assertIn(
            "cargo test -p rusty_grid_worker --test test_feat3_os_isolation --locked -- --test-threads=1 --nocapture",
            step7_block,
        )
        # Skip variable may only exist as an uncommented env entry inside Step 8 and never globally or in Step 7.
        non_comment_ci = "\n".join(
            line for line in ci_text.splitlines() if not line.lstrip().startswith("#")
        )
        step7_non_comment = "\n".join(
            line for line in step7_block.splitlines() if not line.lstrip().startswith("#")
        )
        self.assertEqual(non_comment_ci.count("RUSTY_GRID_SKIP_FEAT3_OS_ISOLATION_STEP8"), 1)
        self.assertNotIn("RUSTY_GRID_SKIP_FEAT3_OS_ISOLATION_STEP8", step7_non_comment)
        self.assertIn(
            "RUSTY_GRID_SKIP_FEAT3_OS_ISOLATION_STEP8: ${{ matrix.os == 'windows-latest' && '1' || '' }}",
            step8_block,
        )

        run_line = next(
            (line.strip() for line in step8_block.splitlines() if line.strip().startswith("run:")),
            "",
        )
        self.assertIn("cargo test --workspace --locked --no-fail-fast", run_line)
        self.assertNotIn("--exclude", run_line)
        self.assertNotIn("--skip", run_line)

        iso_text = read(
            os.path.join(ROOT, "crates", "worker", "tests", "test_feat3_os_isolation.rs")
        )
        win_start = iso_text.index("#[cfg(windows)]\nmod windows {")
        fallback_start = iso_text.index(
            "#[cfg(not(any(target_os = \"macos\", target_os = \"linux\", windows)))]",
            win_start,
        )
        win_mod = iso_text[win_start:fallback_start]
        tests = list(re.finditer(r"(?m)^    async fn (test_[A-Za-z0-9_]+)\(\)\s*\{", win_mod))
        self.assertGreaterEqual(len(tests), 30)
        for m in tests:
            body_head = win_mod[m.end() : m.end() + 120]
            self.assertIn(
                "skip_if_duplicate_ci_pass!();",
                body_head,
                f"Windows isolation test {m.group(1)} must start with skip_if_duplicate_ci_pass!();",
            )


class WindowsOxctlStopTests(unittest.TestCase):
    """BUG-23: windows/oxctl.ps1 asks a tracked process to stop with a console Ctrl+C first.

    A forced end (taskkill /F) skips the master's shutdown handler, so a master with --state-dir never
    compacts its journal. The runtime test uses a fake oxideswarm.exe whose Ctrl+C handler leaves a marker
    file; the marker is the proof that the clean path ran.
    """

    OXCTL = os.path.join(ROOT, "windows", "oxctl.ps1")

    FAKE_CS = r"""
using System;
using System.IO;
using System.Threading;
public class Program {
    public static int Main(string[] args) {
        string a0 = args.Length > 0 ? args[0] : "";
        if (a0 == "--version") { Console.WriteLine("rusty-grid 9.9.9"); return 0; }
        if (a0 == "secret") {
            for (int i = 0; i + 1 < args.Length; i++) {
                if (args[i] == "--out") {
                    Directory.CreateDirectory(Path.GetDirectoryName(args[i + 1]));
                    File.WriteAllText(args[i + 1], new string('a', 64) + "\n");
                }
            }
            return 0;
        }
        if (a0 == "master" || a0 == "worker") {
            string markerDir = Environment.GetEnvironmentVariable("FAKE_OX_MARKER_DIR");
            bool ignore = Environment.GetEnvironmentVariable("FAKE_OX_IGNORE_CTRL_C") == "1";
            string role = a0;
            Console.CancelKeyPress += (sender, e) => {
                e.Cancel = true;
                if (!ignore) {
                    File.WriteAllText(Path.Combine(markerDir, role + ".stopped"), e.SpecialKey.ToString());
                    Environment.Exit(0);
                }
            };
            Console.WriteLine(role == "master" ? "RustyGrid Master started on fake" : "Registration accepted; backoff counter reset");
            Thread.Sleep(120000);
            return 0;
        }
        return 0;
    }
}
"""

    def test_stop_tracked_sends_ctrl_c_before_taskkill(self):
        text = read(self.OXCTL)
        body = text[text.index("function Stop-Tracked"):text.index("function Start-Tracked")]
        self.assertIn("Send-ConsoleCtrlC", body)
        self.assertLess(body.index("Send-ConsoleCtrlC"), body.index("taskkill.exe /F /T /PID"))
        helper = text[text.index("function Send-ConsoleCtrlC"):text.index("function Stop-Tracked")]
        for call in ("FreeConsole", "AttachConsole", "SetConsoleCtrlHandler", "GenerateConsoleCtrlEvent(0, 0)"):
            self.assertIn(call, helper)
        # The guard against a recycled PID and the PID-file tracking stay; nothing is stopped by image name.
        self.assertIn("Get-TrackedPid -Name $Name", body)
        self.assertNotRegex(text, r"(?i)taskkill(\.exe)?\s+/IM|Stop-Process\s+-Name")

    def _free_port(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    def _alive(self, pid):
        out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/NH"], stdin=subprocess.DEVNULL,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=30).stdout
        return re.search(rf"\b{pid}\b", out) is not None

    def _oxctl(self, env, log, *args, timeout=90):
        # oxctl starts long-lived processes with Start-Process, which inherit its handles: give it a
        # file, never a pipe, or the call would wait for the fake binary to exit.
        with open(log, "w", encoding="utf-8") as out:
            res = subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", self.OXCTL, *args],
                env=env, stdin=subprocess.DEVNULL, stdout=out, stderr=subprocess.STDOUT, timeout=timeout,
            )
        return res.returncode, read(log)

    @unittest.skipUnless(IS_WINDOWS, "Windows PowerShell runtime test")
    def test_stop_ends_master_and_worker_with_ctrl_c_and_falls_back_to_taskkill(self):
        if not shutil.which("powershell"):
            self.skipTest("powershell is not available")
        tmp = os.path.realpath(tempfile.mkdtemp(prefix="ox_oxctl_stop_"))
        home = os.path.join(tmp, "home")
        temp = os.path.join(tmp, "temp")
        for d in (home, temp):
            os.makedirs(d, exist_ok=True)
        state = os.path.join(home, ".oxideswarm")
        fake_exe = os.path.join(tmp, "oxideswarm.exe")
        env = {k: v for k, v in os.environ.items()
               if k.upper() not in ("PSMODULEPATH", "OX_EXTRA_ARGS", "OXIDESWARM_SECRET", "OXIDESWARM_SECRET_FILE")}
        env.update(
            USERPROFILE=home, HOME=home, OXIDESWARM_HOME=state, TEMP=temp, TMP=temp,
            OXIDESWARM_BIN=fake_exe, FAKE_OX_MARKER_DIR=tmp,
            OXIDESWARM_CLUSTER_PORT=str(self._free_port()), OXIDESWARM_DASHBOARD_PORT=str(self._free_port()),
        )
        pids = []
        try:
            cs_file = os.path.join(tmp, "FakeOx.cs")
            with open(cs_file, "w", encoding="utf-8") as f:
                f.write(self.FAKE_CS)
            compiled = subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command",
                 f"Add-Type -Path '{cs_file}' -OutputAssembly '{fake_exe}' -OutputType ConsoleApplication"],
                env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=60,
            )
            self.assertEqual(compiled.returncode, 0, compiled.stdout)

            def tracked(name):
                return int(read(os.path.join(state, "run", f"{name}.pid")).strip())

            # 1. Master and its local worker answer the Ctrl+C: both stop cleanly, no taskkill.
            code, out = self._oxctl(env, os.path.join(tmp, "start1.log"), "start", "master")
            self.assertEqual(code, 0, out)
            master_pid, worker_pid = tracked("master"), tracked("worker")
            pids += [master_pid, worker_pid]
            code, out = self._oxctl(env, os.path.join(tmp, "stop1.log"), "stop", "all")
            self.assertEqual(code, 0, out)
            self.assertIn("[OK] master stopped cleanly after Ctrl+C", out)
            self.assertIn("[OK] worker stopped cleanly after Ctrl+C", out)
            self.assertNotIn("taskkill /F /T.", out)
            for role, pid in (("master", master_pid), ("worker", worker_pid)):
                self.assertTrue(os.path.exists(os.path.join(tmp, f"{role}.stopped")), out)
                self.assertFalse(self._alive(pid), f"{role} PID {pid} still runs:\n{out}")
                self.assertFalse(os.path.exists(os.path.join(state, "run", f"{role}.pid")))

            # 2. A master that ignores the Ctrl+C is ended with taskkill /F /T after the grace period.
            env["FAKE_OX_IGNORE_CTRL_C"] = "1"
            code, out = self._oxctl(env, os.path.join(tmp, "start2.log"), "start", "master", "-NoWorker")
            self.assertEqual(code, 0, out)
            master_pid = tracked("master")
            pids.append(master_pid)
            code, out = self._oxctl(env, os.path.join(tmp, "stop2.log"), "stop", "master", timeout=120)
            self.assertEqual(code, 0, out)
            self.assertIn("did not exit within", out)
            self.assertIn("[OK] master ended with taskkill /F /T", out)
            self.assertFalse(self._alive(master_pid), out)
        finally:
            for pid in pids:
                if self._alive(pid):
                    subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)], stdin=subprocess.DEVNULL,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)
            shutil.rmtree(tmp, ignore_errors=True)


class AgentNodeCleartextTokenWarningTests(unittest.TestCase):
    """SEC-15: scripts/agent_node.py warns when it sends a token over ws:// beyond loopback."""

    @classmethod
    def setUpClass(cls):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "agent_node_under_test", os.path.join(ROOT, "scripts", "agent_node.py"))
        cls.mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.mod)

    def test_predicate_warns_only_for_remote_ws_with_a_token(self):
        f = self.mod.token_sent_in_clear_to_remote_host
        for url in ("ws://127.0.0.1:8088/ws", "ws://localhost:8088/ws", "ws://[::1]:8088/ws",
                    "ws://127.9.9.9/ws", "wss://hub.example.com/ws"):
            self.assertFalse(f(url, True), url)
        self.assertFalse(f("ws://192.0.2.1:8088/ws", False), "no token, no warning")
        for url in ("ws://192.0.2.1:8088/ws", "ws://hub.example.com/ws", "ws://[2001:db8::1]:8088/ws",
                    "ws://127.0.0.1@192.0.2.1/ws"):
            self.assertTrue(f(url, True), url)

    def _connect_logs(self, hub_url, token):
        import asyncio
        import types
        mod = self.mod

        async def refuse(*_a, **_k):
            raise RuntimeError("stop after the warning")

        saved = mod.websockets
        mod.websockets = types.SimpleNamespace(connect=refuse)
        try:
            node = mod.OxideAgentNode(hub_url, "n1", "linux", token=token)
            with self.assertLogs("OxideAgent", level="INFO") as logs:
                with self.assertRaises(RuntimeError):
                    asyncio.run(node.connect())
            return "\n".join(logs.output)
        finally:
            mod.websockets = saved

    def test_connect_logs_warning_without_the_token(self):
        token = "SEC15-py-token-0123456789"
        text = self._connect_logs("ws://192.0.2.1:8088/ws", token)
        self.assertIn("plain ws://", text)
        self.assertIn("WARNING", text)
        self.assertNotIn(token, text)

    def test_connect_does_not_warn_on_loopback(self):
        text = self._connect_logs("ws://127.0.0.1:8088/ws", "tok")
        self.assertNotIn("plain ws://", text)


class _FakeReleaseServer:
    """In-process HTTP server on 127.0.0.1 serving /v<version>/<file> from memory."""

    def __init__(self, routes):
        self.routes = dict(routes)
        self.requests = []
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                outer.requests.append(self.path)
                payload = outer.routes.get(self.path)
                if payload is None:
                    self.send_response(404)
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_args):
                pass

        self._httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.base_url = "http://127.0.0.1:%d" % self._httpd.server_address[1]
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *_exc):
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5)


def _make_tar_gz_bytes(folder_name, files_bytes):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, data in files_bytes.items():
            info = tarfile.TarInfo(name="%s/%s" % (folder_name, name))
            info.size = len(data)
            info.mode = 0o755
            tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _make_zip_bytes(folder_name, files_bytes):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, data in files_bytes.items():
            zf.writestr("%s/%s" % (folder_name, name), data)
    return buf.getvalue()


class Adopt3StaticChecks(unittest.TestCase):
    """ADOPT-3 static rules for scripts/install.{sh,ps1} and README.md."""

    def test_install_scripts_follow_adopt3_rules(self):
        sh_path = os.path.join(ROOT, "scripts", "install.sh")
        ps_path = os.path.join(ROOT, "scripts", "install.ps1")
        readme_path = os.path.join(ROOT, "README.md")
        self.assertTrue(os.path.isfile(sh_path), "scripts/install.sh must exist")
        self.assertTrue(os.path.isfile(ps_path), "scripts/install.ps1 must exist")

        sh_text = read(sh_path)
        ps_text = read(ps_path)
        for path, text in ((sh_path, sh_text), (ps_path, ps_text)):
            code_lines = [
                ln for ln in text.splitlines()
                if ln.strip() and not ln.lstrip().startswith("#")
            ]
            joined = "\n".join(code_lines)
            self.assertNotIn("sudo", joined, path)
            self.assertNotIn("launchctl", joined, path)
            self.assertNotIn("systemctl", joined, path)
            self.assertNotIn("New-Service", joined, path)
            self.assertNotIn("sc.exe", joined, path)
            self.assertNotIn(".oxideswarm", joined, path)
            self.assertIn("OXIDESWARM_DOWNLOAD_BASE", text, path)
            self.assertIn("gh attestation verify", text, path)

        if not IS_WINDOWS and shutil.which("bash"):
            res = subprocess.run(
                ["bash", "-n", sh_path],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=15,
            )
            self.assertEqual(res.returncode, 0, res.stdout)

        readme = read(readme_path)
        self.assertIn("scripts/install.sh", readme)
        self.assertIn("scripts/install.ps1", readme)
        self.assertIn("OXIDESWARM_DOWNLOAD_BASE", readme)
        pipe_patterns = (
            r"(?i)\b(curl|wget)\b[^\n|]*\|\s*(sh|bash|zsh)\b",
            r"(?i)\b(irm|iwr|Invoke-RestMethod|Invoke-WebRequest)\b[^\n|]*\|\s*(iex|Invoke-Expression)\b",
        )
        for pat in pipe_patterns:
            self.assertIsNone(re.search(pat, readme), "README must not show pipe-into-shell: %s" % pat)


@unittest.skipIf(IS_WINDOWS, "install.sh runs on macOS and Linux")
class InstallShTests(unittest.TestCase):
    """ADOPT-3 behavioral tests for scripts/install.sh in a private HOME against a local HTTP server."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="ox_install_sh_")
        self.home = os.path.join(self.tmp, "home")
        os.makedirs(self.home)
        self.script = os.path.join(ROOT, "scripts", "install.sh")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

    def _detect_target_or_shim(self):
        sysname = platform.system()
        mach = platform.machine().lower()
        mapping = {
            ("Darwin", "arm64"): "aarch64-apple-darwin",
            ("Darwin", "aarch64"): "aarch64-apple-darwin",
            ("Darwin", "x86_64"): "x86_64-apple-darwin",
            ("Linux", "x86_64"): "x86_64-unknown-linux-gnu",
            ("Linux", "amd64"): "x86_64-unknown-linux-gnu",
        }
        target = mapping.get((sysname, mach))
        if target:
            return target, None
        shim = os.path.join(self.tmp, "uname_shim")
        os.makedirs(shim, exist_ok=True)
        p = os.path.join(shim, "uname")
        with open(p, "w", encoding="utf-8") as f:
            f.write('#!/bin/sh\nif [ "$1" = "-s" ]; then echo Linux; else echo x86_64; fi\n')
        os.chmod(p, 0o755)
        return "x86_64-unknown-linux-gnu", shim

    def _make_fake_gh(self, exit_code=0):
        d = os.path.join(self.tmp, "fake_gh_%d" % exit_code)
        os.makedirs(d, exist_ok=True)
        log = os.path.join(self.tmp, "fake_gh.log")
        open(log, "w", encoding="utf-8").close()
        gh = os.path.join(d, "gh")
        with open(gh, "w", encoding="utf-8") as f:
            f.write('#!/bin/sh\nprintf "%s\\n" "$*" >> "${FAKE_GH_LOG:?}"\nexit "${FAKE_GH_EXIT:-0}"\n')
        os.chmod(gh, 0o755)
        return d, log

    def _no_gh_path(self):
        orig = os.environ.get("PATH", "/usr/bin:/bin")
        tool_dir = os.path.join(self.tmp, "no_gh_bin")
        os.makedirs(tool_dir, exist_ok=True)
        for cmd in (
            "sh", "bash", "uname", "mktemp", "rm", "mkdir", "cp", "mv", "chmod",
            "tar", "gzip", "gunzip", "tr", "awk", "sed", "grep", "cut", "cat",
            "dirname", "basename", "curl", "wget", "sha256sum", "shasum", "openssl",
            "head", "tail",
        ):
            found = shutil.which(cmd, path=orig)
            if found and not os.path.exists(os.path.join(tool_dir, cmd)):
                os.symlink(found, os.path.join(tool_dir, cmd))
        kept = [tool_dir]
        for entry in orig.split(os.pathsep):
            if not entry or not os.path.isdir(entry):
                continue
            if os.path.exists(os.path.join(entry, "gh")):
                continue
            kept.append(entry)
        res = os.pathsep.join(kept)
        self.assertIsNone(shutil.which("gh", path=res))
        return res

    def _build_release_routes(self, version, target, marker, gpu=False, corrupt_sum=False):
        stem = "oxideswarm-%s-%s%s" % (version, target, "-gpu" if gpu else "")
        archive_name = stem + ".tar.gz"
        files = {
            "oxideswarm": ("#!/bin/sh\necho oxideswarm-%s\n" % marker).encode("utf-8"),
            "rusty-grid": ("#!/bin/sh\necho rusty-grid-%s\n" % marker).encode("utf-8"),
            "ox-mode": ("#!/bin/sh\necho ox-mode-%s\n" % marker).encode("utf-8"),
        }
        tgz = _make_tar_gz_bytes(stem, files)
        digest = hashlib.sha256(tgz).hexdigest()
        if corrupt_sum:
            digest = ("0" if digest[0] != "0" else "1") + digest[1:]
        sums = ("%s  %s\n" % (digest, archive_name)).encode("utf-8")
        return {
            "/v%s/%s" % (version, archive_name): tgz,
            "/v%s/SHA256SUMS" % version: sums,
        }

    def _run(self, args, env_extra):
        env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": self.home,
            "TMPDIR": self.tmp,
        }
        env.update(env_extra)
        return subprocess.run(
            ["bash", self.script] + list(args),
            env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, timeout=60,
        )

    def test_matching_checksum_installs_with_gh_attestation(self):
        target, shim = self._detect_target_or_shim()
        gh_dir, gh_log = self._make_fake_gh(0)
        path = os.pathsep.join([p for p in (gh_dir, shim, os.environ.get("PATH", "/usr/bin:/bin")) if p])
        routes = self._build_release_routes("0.4.4", target, "V1")
        dest = os.path.join(self.tmp, "dest")
        with _FakeReleaseServer(routes) as srv:
            res = self._run(
                ["--version", "0.4.4", "--dir", dest],
                {"PATH": path, "OXIDESWARM_DOWNLOAD_BASE": srv.base_url, "FAKE_GH_LOG": gh_log, "FAKE_GH_EXIT": "0"},
            )
        self.assertEqual(res.returncode, 0, res.stdout)
        self.assertFalse(os.path.exists(os.path.join(self.home, ".oxideswarm")))
        for b in ("oxideswarm", "rusty-grid", "ox-mode"):
            p = os.path.join(dest, b)
            self.assertTrue(os.path.isfile(p), p)
            self.assertTrue(os.access(p, os.X_OK), p)
            self.assertIn("V1", read(p))
        gh_calls = read(gh_log)
        self.assertIn("attestation verify", gh_calls)
        self.assertIn("--repo DuongNAD/OxideSwarm", gh_calls)
        self.assertIn("--source-ref refs/tags/v0.4.4", gh_calls)
        self.assertIn("oxideswarm master", res.stdout)

    def test_without_gh_installs_after_sha256_and_prints_notice(self):
        target, shim = self._detect_target_or_shim()
        no_gh = self._no_gh_path()
        path = os.pathsep.join([p for p in (shim, no_gh) if p])
        routes = self._build_release_routes("0.4.4", target, "NOGH")
        dest = os.path.join(self.tmp, "dest_nogh")
        with _FakeReleaseServer(routes) as srv:
            res = self._run(
                ["--version", "0.4.4", "--dir", dest],
                {"PATH": path, "OXIDESWARM_DOWNLOAD_BASE": srv.base_url},
            )
        self.assertEqual(res.returncode, 0, res.stdout)
        self.assertIn("skipping provenance attestation", res.stdout)
        self.assertTrue(os.path.isfile(os.path.join(dest, "oxideswarm")))

    def test_attestation_failure_refuses_and_leaves_no_file(self):
        target, shim = self._detect_target_or_shim()
        gh_dir, gh_log = self._make_fake_gh(1)
        path = os.pathsep.join([p for p in (gh_dir, shim, os.environ.get("PATH", "/usr/bin:/bin")) if p])
        routes = self._build_release_routes("0.4.4", target, "BADATT")
        dest = os.path.join(self.tmp, "dest_bad_att")
        with _FakeReleaseServer(routes) as srv:
            res = self._run(
                ["--version", "0.4.4", "--dir", dest],
                {"PATH": path, "OXIDESWARM_DOWNLOAD_BASE": srv.base_url, "FAKE_GH_LOG": gh_log, "FAKE_GH_EXIT": "1"},
            )
        self.assertNotEqual(res.returncode, 0, res.stdout)
        self.assertFalse(os.path.exists(dest), "failed attestation must leave no files or dir")

    def test_checksum_mismatch_refuses_before_attestation_and_leaves_no_file(self):
        target, shim = self._detect_target_or_shim()
        gh_dir, gh_log = self._make_fake_gh(0)
        path = os.pathsep.join([p for p in (gh_dir, shim, os.environ.get("PATH", "/usr/bin:/bin")) if p])
        routes = self._build_release_routes("0.4.4", target, "CORRUPT", corrupt_sum=True)
        dest = os.path.join(self.tmp, "dest_bad_sum")
        with _FakeReleaseServer(routes) as srv:
            res = self._run(
                ["--version", "0.4.4", "--dir", dest],
                {"PATH": path, "OXIDESWARM_DOWNLOAD_BASE": srv.base_url, "FAKE_GH_LOG": gh_log, "FAKE_GH_EXIT": "0"},
            )
        self.assertNotEqual(res.returncode, 0, res.stdout)
        self.assertIn("checksum mismatch", res.stdout.lower())
        self.assertEqual(read(gh_log).strip(), "", "checksum mismatch must abort before gh attestation verify")
        self.assertFalse(os.path.exists(dest), "checksum mismatch must leave no files or dir")

    def test_unknown_architecture_is_named(self):
        shim = os.path.join(self.tmp, "uname_riscv")
        os.makedirs(shim)
        p = os.path.join(shim, "uname")
        with open(p, "w", encoding="utf-8") as f:
            f.write('#!/bin/sh\nif [ "$1" = "-s" ]; then echo Linux; else echo riscv64; fi\n')
        os.chmod(p, 0o755)
        dest = os.path.join(self.tmp, "dest_arch")
        with _FakeReleaseServer({}) as srv:
            res = self._run(
                ["--dir", dest],
                {"PATH": shim + os.pathsep + os.environ.get("PATH", "/usr/bin:/bin"), "OXIDESWARM_DOWNLOAD_BASE": srv.base_url},
            )
            reqs = list(srv.requests)
        self.assertNotEqual(res.returncode, 0, res.stdout)
        self.assertIn("Linux", res.stdout)
        self.assertIn("riscv64", res.stdout)
        self.assertEqual(reqs, [])
        self.assertFalse(os.path.exists(dest))

    def test_dry_run_writes_nothing(self):
        target, shim = self._detect_target_or_shim()
        path = os.pathsep.join([p for p in (shim, os.environ.get("PATH", "/usr/bin:/bin")) if p])
        dest = os.path.join(self.tmp, "dest_dry")
        with _FakeReleaseServer({}) as srv:
            res = self._run(
                ["--version", "0.4.4", "--dir", dest, "--dry-run"],
                {"PATH": path, "OXIDESWARM_DOWNLOAD_BASE": srv.base_url},
            )
            reqs = list(srv.requests)
        self.assertEqual(res.returncode, 0, res.stdout)
        self.assertEqual(reqs, [])
        self.assertIn("Dry run", res.stdout)
        self.assertIn(target, res.stdout)
        self.assertFalse(os.path.exists(dest))
        self.assertFalse(os.path.exists(os.path.join(self.home, ".oxideswarm")))

    def test_second_run_replaces_binaries(self):
        target, shim = self._detect_target_or_shim()
        gh_dir, gh_log = self._make_fake_gh(0)
        path = os.pathsep.join([p for p in (gh_dir, shim, os.environ.get("PATH", "/usr/bin:/bin")) if p])
        dest = os.path.join(self.tmp, "dest_replace")
        routes1 = self._build_release_routes("0.4.3", target, "MARKER_V1")
        routes2 = self._build_release_routes("0.4.4", target, "MARKER_V2")
        merged = dict(routes1, **routes2)
        with _FakeReleaseServer(merged) as srv:
            r1 = self._run(
                ["--version", "0.4.3", "--dir", dest],
                {"PATH": path, "OXIDESWARM_DOWNLOAD_BASE": srv.base_url, "FAKE_GH_LOG": gh_log, "FAKE_GH_EXIT": "0"},
            )
            self.assertEqual(r1.returncode, 0, r1.stdout)
            self.assertIn("MARKER_V1", read(os.path.join(dest, "oxideswarm")))
            r2 = self._run(
                ["--version", "0.4.4", "--dir", dest],
                {"PATH": path, "OXIDESWARM_DOWNLOAD_BASE": srv.base_url, "FAKE_GH_LOG": gh_log, "FAKE_GH_EXIT": "0"},
            )
            self.assertEqual(r2.returncode, 0, r2.stdout)
        for b in ("oxideswarm", "rusty-grid", "ox-mode"):
            text = read(os.path.join(dest, b))
            self.assertIn("MARKER_V2", text)
            self.assertNotIn("MARKER_V1", text)


@unittest.skipUnless(IS_WINDOWS and shutil.which("powershell"), "install.ps1 runs under Windows PowerShell")
class InstallPs1Tests(unittest.TestCase):
    """ADOPT-3 behavioral tests for scripts/install.ps1 in a private HOME against a local HTTP server."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="ox_install_ps1_")
        self.home = os.path.join(self.tmp, "home")
        self.localappdata = os.path.join(self.tmp, "localappdata")
        os.makedirs(self.home)
        os.makedirs(self.localappdata)
        self.script = os.path.join(ROOT, "scripts", "install.ps1")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

    def _make_fake_gh(self, exit_code=0):
        d = os.path.join(self.tmp, "fake_gh_%d" % exit_code)
        os.makedirs(d, exist_ok=True)
        log = os.path.join(self.tmp, "fake_gh.log")
        open(log, "w", encoding="utf-8").close()
        gh = os.path.join(d, "gh.cmd")
        with open(gh, "w", encoding="ascii", newline="\r\n") as f:
            f.write("@echo off\r\n>> \"%FAKE_GH_LOG%\" echo %*\r\nexit /b %FAKE_GH_EXIT%\r\n")
        return d, log

    def _no_gh_path(self):
        parts = []
        for entry in os.environ.get("PATH", "").split(os.pathsep):
            if not entry:
                continue
            if any(os.path.isfile(os.path.join(entry, name)) for name in ("gh.exe", "gh.cmd", "gh.bat")):
                continue
            parts.append(entry)
        res = os.pathsep.join(parts)
        self.assertIsNone(shutil.which("gh", path=res))
        return res

    def _build_release_routes(self, version, marker, gpu=False, corrupt_sum=False):
        stem = "oxideswarm-%s-x86_64-pc-windows-msvc%s" % (version, "-gpu" if gpu else "")
        archive_name = stem + ".zip"
        files = {
            "oxideswarm.exe": ("OX_FAKE_OXIDESWARM_" + marker).encode("ascii"),
            "rusty-grid.exe": ("OX_FAKE_RUSTY_GRID_" + marker).encode("ascii"),
            "ox-mode.exe": ("OX_FAKE_OX_MODE_" + marker).encode("ascii"),
        }
        zdata = _make_zip_bytes(stem, files)
        digest = hashlib.sha256(zdata).hexdigest()
        if corrupt_sum:
            digest = ("0" if digest[0] != "0" else "1") + digest[1:]
        sums = ("%s  %s\n" % (digest, archive_name)).encode("utf-8")
        return {
            "/v%s/%s" % (version, archive_name): zdata,
            "/v%s/SHA256SUMS" % version: sums,
        }

    def _run(self, args, env_extra=None, remove_keys=()):
        env = {k: v for k, v in os.environ.items() if k.upper() != "PSMODULEPATH"}
        env.update({
            "HOME": self.home,
            "USERPROFILE": self.home,
            "LOCALAPPDATA": self.localappdata,
            "TEMP": self.tmp,
            "TMP": self.tmp,
        })
        for rk in remove_keys:
            env.pop(rk, None)
        if env_extra:
            env.update(env_extra)
        return subprocess.run(
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", self.script] + list(args),
            env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, timeout=60,
        )

    def test_matching_checksum_installs_with_gh_attestation(self):
        gh_dir, gh_log = self._make_fake_gh(0)
        routes = self._build_release_routes("0.4.4", "WIN_V1")
        dest = os.path.join(self.tmp, "dest")
        with _FakeReleaseServer(routes) as srv:
            res = self._run(
                ["--version", "0.4.4", "--dir", dest],
                {
                    "PATH": gh_dir + os.pathsep + os.environ.get("PATH", ""),
                    "OXIDESWARM_DOWNLOAD_BASE": srv.base_url,
                    "FAKE_GH_LOG": gh_log,
                    "FAKE_GH_EXIT": "0",
                },
            )
        self.assertEqual(res.returncode, 0, res.stdout)
        self.assertFalse(os.path.exists(os.path.join(self.home, ".oxideswarm")))
        for b in ("oxideswarm.exe", "rusty-grid.exe", "ox-mode.exe"):
            p = os.path.join(dest, b)
            self.assertTrue(os.path.isfile(p), p)
            self.assertIn("WIN_V1", read(p))
        gh_calls = read(gh_log)
        self.assertIn("attestation verify", gh_calls)
        self.assertIn("--repo DuongNAD/OxideSwarm", gh_calls)
        self.assertIn("--source-ref refs/tags/v0.4.4", gh_calls)
        self.assertIn("oxideswarm master", res.stdout)

    def test_without_gh_installs_after_sha256_and_prints_notice(self):
        routes = self._build_release_routes("0.4.4", "WIN_NOGH")
        dest = os.path.join(self.tmp, "dest_nogh")
        with _FakeReleaseServer(routes) as srv:
            res = self._run(
                ["-Version", "0.4.4", "-Dir", dest],
                {"PATH": self._no_gh_path(), "OXIDESWARM_DOWNLOAD_BASE": srv.base_url},
            )
        self.assertEqual(res.returncode, 0, res.stdout)
        self.assertIn("skipping provenance attestation", res.stdout)
        self.assertTrue(os.path.isfile(os.path.join(dest, "oxideswarm.exe")))

    def test_attestation_failure_refuses_and_leaves_no_file(self):
        gh_dir, gh_log = self._make_fake_gh(1)
        routes = self._build_release_routes("0.4.4", "WIN_BADATT")
        dest = os.path.join(self.tmp, "dest_bad_att")
        with _FakeReleaseServer(routes) as srv:
            res = self._run(
                ["--version", "0.4.4", "--dir", dest],
                {
                    "PATH": gh_dir + os.pathsep + os.environ.get("PATH", ""),
                    "OXIDESWARM_DOWNLOAD_BASE": srv.base_url,
                    "FAKE_GH_LOG": gh_log,
                    "FAKE_GH_EXIT": "1",
                },
            )
        self.assertNotEqual(res.returncode, 0, res.stdout)
        self.assertFalse(os.path.exists(dest), "failed attestation must leave no files or dir")

    def test_checksum_mismatch_refuses_before_attestation_and_leaves_no_file(self):
        gh_dir, gh_log = self._make_fake_gh(0)
        routes = self._build_release_routes("0.4.4", "WIN_BADSUM", corrupt_sum=True)
        dest = os.path.join(self.tmp, "dest_bad_sum")
        with _FakeReleaseServer(routes) as srv:
            res = self._run(
                ["--version", "0.4.4", "--dir", dest],
                {
                    "PATH": gh_dir + os.pathsep + os.environ.get("PATH", ""),
                    "OXIDESWARM_DOWNLOAD_BASE": srv.base_url,
                    "FAKE_GH_LOG": gh_log,
                    "FAKE_GH_EXIT": "0",
                },
            )
        self.assertNotEqual(res.returncode, 0, res.stdout)
        self.assertIn("checksum mismatch", res.stdout.lower())
        self.assertEqual(read(gh_log).strip(), "", "checksum mismatch must abort before gh attestation verify")
        self.assertFalse(os.path.exists(dest), "checksum mismatch must leave no files or dir")

    def test_unknown_architecture_is_named(self):
        dest = os.path.join(self.tmp, "dest_arch")
        with _FakeReleaseServer({}) as srv:
            res = self._run(
                ["--dir", dest],
                {"OXIDESWARM_DOWNLOAD_BASE": srv.base_url, "PROCESSOR_ARCHITECTURE": "ARM64"},
                remove_keys=("PROCESSOR_ARCHITEW6432",),
            )
            reqs = list(srv.requests)
        self.assertNotEqual(res.returncode, 0, res.stdout)
        self.assertIn("Windows", res.stdout)
        self.assertIn("ARM64", res.stdout)
        self.assertEqual(reqs, [])
        self.assertFalse(os.path.exists(dest))

    def test_dry_run_writes_nothing(self):
        dest = os.path.join(self.tmp, "dest_dry")
        with _FakeReleaseServer({}) as srv:
            res = self._run(
                ["--version", "0.4.4", "--dir", dest, "--dry-run"],
                {"OXIDESWARM_DOWNLOAD_BASE": srv.base_url},
            )
            reqs = list(srv.requests)
        self.assertEqual(res.returncode, 0, res.stdout)
        self.assertEqual(reqs, [])
        self.assertIn("Dry run", res.stdout)
        self.assertIn("x86_64-pc-windows-msvc", res.stdout)
        self.assertFalse(os.path.exists(dest))
        self.assertFalse(os.path.exists(os.path.join(self.home, ".oxideswarm")))

    def test_second_run_replaces_binaries(self):
        gh_dir, gh_log = self._make_fake_gh(0)
        dest = os.path.join(self.tmp, "dest_replace")
        routes1 = self._build_release_routes("0.4.3", "MARKER_V1")
        routes2 = self._build_release_routes("0.4.4", "MARKER_V2")
        merged = dict(routes1, **routes2)
        env = {
            "PATH": gh_dir + os.pathsep + os.environ.get("PATH", ""),
            "FAKE_GH_LOG": gh_log,
            "FAKE_GH_EXIT": "0",
        }
        with _FakeReleaseServer(merged) as srv:
            env["OXIDESWARM_DOWNLOAD_BASE"] = srv.base_url
            r1 = self._run(["--version", "0.4.3", "--dir", dest], env)
            self.assertEqual(r1.returncode, 0, r1.stdout)
            self.assertIn("MARKER_V1", read(os.path.join(dest, "oxideswarm.exe")))
            r2 = self._run(["--version", "0.4.4", "--dir", dest], env)
            self.assertEqual(r2.returncode, 0, r2.stdout)
        for b in ("oxideswarm.exe", "rusty-grid.exe", "ox-mode.exe"):
            text = read(os.path.join(dest, b))
            self.assertIn("MARKER_V2", text)
            self.assertNotIn("MARKER_V1", text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
