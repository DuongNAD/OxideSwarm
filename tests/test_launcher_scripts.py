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

import glob
import os
import re
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.environ.get("OX_SCRIPT_ROOT") or os.path.join(HERE, ".."))

TICKET = '{"id":"00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff","addrs":[]}'
IS_WINDOWS = os.name == "nt"

# A stand-in for the real binary: records its arguments, prints the log lines the launchers wait for,
# and then idles as a *bash* process so that `ps` still shows "rusty-grid" in its command line.
FAKE_BINARY = """#!/bin/bash
echo "$*" >> "${FAKE_ARGS_LOG:?}"
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


def tracked_or_present(patterns):
    """Files under ROOT matching the glob patterns (git is not required)."""
    skip = ("/target/", "/.git/", "/.agents/", "/node_modules/", "/__pycache__/")
    found = []
    for pattern in patterns:
        for path in glob.glob(os.path.join(ROOT, "**", pattern), recursive=True):
            norm = path.replace("\\", "/")
            if any(s in norm for s in skip) or not os.path.isfile(path):
                continue
            found.append(path)
    return sorted(set(found))


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

    def copy_script(self, relative):
        src = os.path.join(ROOT, relative)
        dst = os.path.join(self.repo, os.path.basename(relative))
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
        self.assertEqual(res.returncode, 0, res.stdout)
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
        self.assertEqual(stop.returncode, 0, stop.stdout)
        self.assertTrue(wait_until(lambda: not pid_alive(master_pid) and not pid_alive(worker_pid)),
                        "stop must terminate the tracked master and worker")

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


class StaticRepositoryChecks(unittest.TestCase):
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
        forbidden = {
            r"DESKTOP-3EPV830": "the owner's PC name",
            r"R5CWC3QQ52H": "the owner's phone serial",
            r"192\.168\.1\.(144|123|147|156|166)\b": "the owner's LAN addresses",
            r"duonganhdn2000": "the owner's e-mail address",
            r"\bpkill\b(?!\s*\")": "a name-pattern kill",
            r"taskkill[^\n]*/IM\b": "a kill by image name",
            r"\bkillall\b": "a name-pattern kill",
        }
        skip = {
            # historical milestone script of the author's two machines (not a launcher)
            os.path.join("scripts", "run_milestones_1_and_2.ps1"),
        }
        offenders = []
        for path in tracked_or_present(["*.sh", "*.cmd", "*.ps1", "*.vbs"]):
            rel = os.path.relpath(path, ROOT)
            if rel in skip or rel.startswith("tests" + os.sep):
                continue
            text = read(path)
            for pattern, why in forbidden.items():
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
        # the P2P ticket must not travel on the service command line or in the registry any more
        wrapper = read(os.path.join(ROOT, "packaging", "windows", "ServiceWrapper.cs"))
        self.assertNotIn("--p2p-ticket", wrapper)
        self.assertNotRegex(ps1, r"SetEnvironmentVariable\(\"OXIDESWARM_P2P_TICKET\",\s*\$P2pTicket")

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
