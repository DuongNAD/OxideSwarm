#!/usr/bin/env python3
"""
tests/test_agent_bridge.py - regression tests for the message trigger in scripts/agent_bridge.py.

Every test runs the real script against a throw-away hub, with a private HOME and OXIDESWARM_HOME.
Nothing here touches Google Drive, the real ~/.oxideswarm, the network or a built binary.

Run:   python3 tests/test_agent_bridge.py
Mutation check: OX_SCRIPT_ROOT=/path/to/an/older/checkout python3 tests/test_agent_bridge.py
"""

import datetime
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.environ.get("OX_SCRIPT_ROOT") or os.path.join(HERE, ".."))
SCRIPT = os.path.join(ROOT, "scripts", "agent_bridge.py")

spec = importlib.util.spec_from_file_location("agent_bridge", SCRIPT)
bridge_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge_module)
SELF = bridge_module.SELF_SHORT
PEER = bridge_module.PEER_SHORT

VIETNAMESE_TITLE = "Kiểm thử máy Windows"
WAIT = 25


class BridgeCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="ox-bridge-")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.hub = os.path.join(self.tmp, "OxideSwarm_Agent_Hub")
        self.inbox = os.path.join(self.hub, "mailbox", f"to_{SELF}")
        self.incidents = os.path.join(self.hub, "incidents")
        self.nodes = os.path.join(self.hub, "nodes")
        for path in (self.inbox, self.incidents, self.nodes):
            os.makedirs(path)
        self.home = os.path.join(self.tmp, "home")
        os.makedirs(self.home)
        self.env = dict(os.environ, HOME=self.home, USERPROFILE=self.home)
        self.env["OXIDESWARM_HOME"] = os.path.join(self.tmp, "state")
        self.env.pop("OXIDESWARM_AGENT_HUB", None)
        self.env.pop("PYTHONIOENCODING", None)
        self.children = []
        self.addCleanup(self.stop_children)

    def stop_children(self):
        for child in self.children:
            if child.poll() is None:
                child.kill()
            child.communicate()

    def argv(self, *args, use_hub=True):
        hub = ["--hub-dir", self.hub] if use_hub else []
        return [sys.executable, SCRIPT, *hub, *args]

    def run_bridge(self, *args, use_hub=True, stdin="", env=None, timeout=WAIT):
        return subprocess.run(
            self.argv(*args, use_hub=use_hub),
            cwd=self.tmp,
            env=env or self.env,
            input=stdin.encode("utf-8"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )

    def start_bridge(self, *args, stdin=None):
        child = subprocess.Popen(
            self.argv(*args),
            cwd=self.tmp,
            env=self.env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if stdin is not None:
            child.stdin.write(stdin.encode("utf-8"))
        child.stdin.close()
        child.stdin = None
        self.children.append(child)
        return child

    def finish(self, child):
        stdout, stderr = child.communicate(timeout=WAIT)
        return child.returncode, stdout.decode("utf-8", "replace"), stderr.decode("utf-8", "replace")

    def watch_args(self, *extra):
        return ("watch", "--interval", "1", "--heartbeat-every", "3600", *extra)

    def write_message(self, title="Check the build", name="MSG_1.md", body="Body text of the message."):
        content = (
            f"# [HIGH] {title}\n\n- **From**: `agent-{PEER}` (`peer-host`)\n- **Priority**: `high`\n\n---\n\n{body}\n"
        )
        with open(os.path.join(self.inbox, name), "w", encoding="utf-8") as f:
            f.write(content)

    def write_incident(self, author, title="Worker timeout", name="INC-1.md"):
        path = os.path.join(self.incidents, name)
        with open(path, "w", encoding="utf-8") as f:
            f.write(f"# [OPEN] INC-1: {title}\n\n- **Reported By**: `agent-{author}` (`some-host`)\n")
        return path


class OnceOnMessage(BridgeCase):
    def test_a_new_message_ends_the_wait(self):
        child = self.start_bridge(*self.watch_args("--once-on-message", "--max-wait", "20"))
        time.sleep(1.5)
        self.assertIsNone(child.poll(), "the watcher must wait for a message")
        self.write_message()
        code, stdout, _ = self.finish(child)
        self.assertEqual(code, 0)
        self.assertIn("NEW MESSAGE", stdout)
        self.assertIn("Body text of the message.", stdout)

    def test_a_delivered_message_does_not_fire_again_after_rearming(self):
        self.write_message()
        code, stdout, _ = self.finish(self.start_bridge(*self.watch_args("--once-on-message", "--max-wait", "20")))
        self.assertEqual(code, 0)
        self.assertIn("NEW MESSAGE", stdout)

        code, stdout, _ = self.finish(self.start_bridge(*self.watch_args("--once-on-message", "--max-wait", "2")))
        self.assertEqual(code, 0)
        self.assertNotIn("NEW MESSAGE", stdout, "the message was delivered already")

    def test_a_message_sent_while_nothing_watches_is_delivered_by_the_next_watcher(self):
        self.finish(self.start_bridge(*self.watch_args("--max-wait", "1")))
        self.write_message()
        _, stdout, _ = self.finish(self.start_bridge(*self.watch_args("--once-on-message", "--max-wait", "20")))
        self.assertIn("NEW MESSAGE", stdout)

    def test_two_watchers_of_one_consumer_deliver_a_message_once(self):
        args = self.watch_args("--once-on-message", "--max-wait", "6")
        first, second = self.start_bridge(*args), self.start_bridge(*args)
        time.sleep(1.5)
        self.write_message()
        outputs = [self.finish(first)[1], self.finish(second)[1]]
        self.assertEqual(sum("NEW MESSAGE" in out for out in outputs), 1, outputs)

    def test_each_consumer_gets_its_own_delivery(self):
        self.write_message()
        for consumer in ("agent", "desktop"):
            args = self.watch_args("--once-on-message", "--max-wait", "20", "--consumer", consumer)
            self.assertIn("NEW MESSAGE", self.finish(self.start_bridge(*args))[1])


class Incidents(BridgeCase):
    def test_incidents_that_exist_at_the_first_run_do_not_fire(self):
        self.write_incident(PEER)
        _, stdout, _ = self.finish(self.start_bridge(*self.watch_args("--once-on-message", "--max-wait", "2")))
        self.assertNotIn("INCIDENT UPDATE", stdout)

    def test_a_peer_incident_fires_and_an_own_incident_does_not(self):
        self.finish(self.start_bridge(*self.watch_args("--max-wait", "1")))
        self.write_incident(SELF, name="INC-own.md")
        _, stdout, _ = self.finish(self.start_bridge(*self.watch_args("--once-on-message", "--max-wait", "2")))
        self.assertNotIn("INCIDENT UPDATE", stdout)

        self.write_incident(PEER, name="INC-peer.md")
        _, stdout, _ = self.finish(self.start_bridge(*self.watch_args("--once-on-message", "--max-wait", "20")))
        self.assertIn("INC-peer.md", stdout)

    def test_a_peer_update_of_a_known_incident_fires_again(self):
        path = self.write_incident(PEER)
        self.finish(self.start_bridge(*self.watch_args("--max-wait", "1")))
        later = time.time() + 5
        os.utime(path, (later, later))
        _, stdout, _ = self.finish(self.start_bridge(*self.watch_args("--once-on-message", "--max-wait", "20")))
        self.assertIn("INCIDENT UPDATE", stdout)


class Robustness(BridgeCase):
    def test_a_console_that_cannot_encode_vietnamese_does_not_crash_the_bridge(self):
        env = dict(self.env, PYTHONIOENCODING="cp1252")
        self.write_message(title=VIETNAMESE_TITLE, body="Nội dung tin nhắn")
        result = self.run_bridge(*self.watch_args("--once-on-message", "--max-wait", "20"), env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(VIETNAMESE_TITLE, result.stdout.decode("utf-8"))

        result = self.run_bridge("inbox", env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(VIETNAMESE_TITLE, result.stdout.decode("utf-8"))

    def test_the_watcher_survives_a_hub_that_goes_away_and_comes_back(self):
        child = self.start_bridge(*self.watch_args("--once-on-message", "--max-wait", "20"))
        time.sleep(1.5)
        away = self.hub + ".away"
        os.rename(self.hub, away)
        time.sleep(2.5)
        self.assertIsNone(child.poll(), "the watcher must outlive a missing hub")
        os.rename(away, self.hub)
        self.write_message()
        code, stdout, stderr = self.finish(child)
        self.assertEqual(code, 0, stderr)
        self.assertIn("NEW MESSAGE", stdout)
        self.assertIn("HUB UNAVAILABLE", stderr)

    def test_a_file_that_is_still_empty_waits_for_its_content(self):
        empty = os.path.join(self.inbox, "MSG_empty.md")
        open(empty, "w", encoding="utf-8").close()
        child = self.start_bridge(*self.watch_args("--once-on-message", "--max-wait", "20"))
        time.sleep(2.5)
        self.assertIsNone(child.poll(), "an empty file is not a message yet")
        self.write_message(name="MSG_empty.md")
        self.assertIn("NEW MESSAGE", self.finish(child)[1])

    def test_the_heartbeat_does_not_rewrite_the_shared_file_on_every_poll(self):
        state = os.path.join(self.nodes, f"{SELF}_state.json")
        child = self.start_bridge(*self.watch_args("--max-wait", "4"))
        seen = set()
        while child.poll() is None:
            if os.path.exists(state):
                seen.add(os.path.getmtime(state))
            time.sleep(0.2)
        self.assertEqual(len(seen), 1, "one heartbeat is due in this window")

    def test_auto_pull_only_fast_forwards(self):
        with open(SCRIPT, encoding="utf-8") as f:
            source = f.read()
        self.assertIn('"--ff-only"', source)
        self.assertNotIn('"--rebase"', source)


class OnEvent(BridgeCase):
    def test_the_command_gets_the_event_as_variables_and_never_as_shell_text(self):
        marker = os.path.join(self.tmp, "pwned")
        title = f"$(touch {marker}) `touch {marker}` & echo x > {marker}"
        out = os.path.join(self.tmp, "event.json")
        code = (
            "import json,os;"
            f"json.dump({{k:v for k,v in os.environ.items() if k.startswith('OXIDESWARM_EVENT_')}},open(r'{out}','w'))"
        )
        self.write_message(title=title)
        args = self.watch_args("--once-on-message", "--max-wait", "20", "--on-event", f'"{sys.executable}" -c "{code}"')
        self.assertEqual(self.finish(self.start_bridge(*args))[0], 0)
        with open(out, encoding="utf-8") as f:
            event = json.load(f)
        self.assertEqual(event["OXIDESWARM_EVENT_TITLE"], title[: bridge_module.MAX_TITLE_CHARS].strip())
        self.assertEqual(event["OXIDESWARM_EVENT_KIND"], "message")
        self.assertEqual(event["OXIDESWARM_EVENT_FROM"], f"agent-{PEER}")
        self.assertEqual(event["OXIDESWARM_EVENT_PRIORITY"], "high")
        self.assertTrue(event["OXIDESWARM_EVENT_FILE"].endswith("MSG_1.md"))
        self.assertFalse(os.path.exists(marker), "a peer title must not run as shell text")


class ClaudeCodeHook(BridgeCase):
    def hook_args(self):
        return self.watch_args("--hook", "--max-wait", "15")

    def test_an_event_exits_with_the_wake_code_and_a_notice_without_the_body(self):
        self.write_message(body="SECRET-BODY-TEXT")
        result = self.run_bridge(*self.hook_args(), stdin='{"session_id": "s1"}')
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertEqual(result.stdout, b"")
        notice = result.stderr.decode("utf-8")
        self.assertIn("Check the build", notice)
        self.assertIn(f"agent-{PEER}", notice)
        self.assertIn("scripts/agent_bridge.py inbox", notice)
        self.assertIn("not from the owner", notice)
        self.assertNotIn("SECRET-BODY-TEXT", notice)

    def test_the_notice_does_not_repeat_on_the_next_stop(self):
        self.write_message()
        self.assertEqual(self.run_bridge(*self.hook_args(), stdin='{"session_id": "s1"}').returncode, 2)
        args = self.watch_args("--hook", "--max-wait", "2")
        self.assertEqual(self.run_bridge(*args, stdin='{"session_id": "s1"}').returncode, 0)

    def test_a_session_keeps_one_watcher_across_turns(self):
        first = self.start_bridge(*self.hook_args(), stdin='{"session_id": "s1"}')
        time.sleep(2.5)
        self.assertIsNone(first.poll())

        started = time.time()
        result = self.run_bridge(*self.hook_args(), stdin='{"session_id": "s1"}')
        self.assertEqual(result.returncode, 0)
        self.assertLess(time.time() - started, 5, "the second hook must leave at once")

        other = self.start_bridge(*self.watch_args("--hook", "--max-wait", "3"), stdin='{"session_id": "s2"}')
        time.sleep(2)
        self.assertIsNone(other.poll(), "another session has its own watcher")

    def test_a_stdin_that_stays_open_does_not_block_the_hook(self):
        child = subprocess.Popen(
            self.argv(*self.watch_args("--hook", "--max-wait", "15")),
            cwd=self.tmp,
            env=self.env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.children.append(child)
        self.write_message()
        child.wait(timeout=WAIT)
        self.assertEqual(child.returncode, 2)

    def read_session_id(self, hook_input):
        """Returns the session id a process reads while its stdin stays open, and its live threads."""
        code = (
            "import importlib.util, json, sys, threading\n"
            "spec = importlib.util.spec_from_file_location('agent_bridge', sys.argv[1])\n"
            "module = importlib.util.module_from_spec(spec)\n"
            "spec.loader.exec_module(module)\n"
            "print(json.dumps([module.read_hook_session_id(), threading.active_count()]))\n"
        )
        child = subprocess.Popen(
            [sys.executable, "-c", code, SCRIPT],
            cwd=self.tmp,
            env=self.env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.children.append(child)
        child.stdin.write(hook_input)
        child.stdin.flush()
        child.wait(timeout=WAIT)
        stdout, stderr = child.communicate()
        self.assertEqual(child.returncode, 0, stderr)
        return json.loads(stdout)

    def test_no_read_stays_pending_on_a_stdin_that_stays_open(self):
        # On Windows a pending read can block any process that inherits stdin, such as git.
        self.assertEqual(self.read_session_id(b""), [None, 1])

    def test_the_session_id_arrives_on_a_stdin_that_stays_open(self):
        self.assertEqual(self.read_session_id(b'{"session_id": "s1"}'), ["s1", 1])

    def test_the_hook_watcher_leaves_a_fresh_state_file_and_refreshes_an_old_one(self):
        state_path = os.path.join(self.nodes, f"{SELF}_state.json")
        hook_once = self.watch_args("--hook", "--max-wait", "1")

        # 1. Missing state file: hook watcher publishes initial state.
        self.assertEqual(self.run_bridge(*hook_once, stdin='{"session_id": "s1"}').returncode, 0)
        self.assertTrue(os.path.exists(state_path), "hook watcher must create a missing state file")
        with open(state_path, encoding="utf-8") as f:
            created = json.load(f)
        self.assertIn("git", created)

        # 2. Fresh state file (`updated_at_utc` 60 s old, even with an older mtime): hook watcher
        #    uses `updated_at_utc` alone and leaves the file untouched.
        now = datetime.datetime.now(datetime.timezone.utc)
        fresh_ts = (now - datetime.timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
        fresh_marker = {
            "updated_at_utc": fresh_ts,
            "git": {"git_branch": "agent-active-branch", "git_head": "abc123456789"},
        }
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump(fresh_marker, f)
        old_mtime = time.time() - 300
        os.utime(state_path, (old_mtime, old_mtime))

        self.assertEqual(self.run_bridge(*hook_once, stdin='{"session_id": "s1"}').returncode, 0)
        with open(state_path, encoding="utf-8") as f:
            after_fresh = json.load(f)
        self.assertEqual(
            after_fresh.get("updated_at_utc"),
            fresh_ts,
            "hook watcher must use updated_at_utc alone and leave a <= 150 s state file untouched",
        )
        self.assertEqual(
            after_fresh.get("git", {}).get("git_branch"),
            "agent-active-branch",
        )

        # 3. Old state file (`updated_at_utc` > 150 s old): hook watcher updates only
        #    `updated_at_utc` and keeps the `git` block and all other fields untouched (F8 / P7).
        stale_ts = (now - datetime.timedelta(seconds=151)).strftime("%Y-%m-%dT%H:%M:%SZ")
        stale_marker = {
            "updated_at_utc": stale_ts,
            "git": {"git_branch": "agent-active-branch", "git_head": "abc123456789"},
            "lan_ip": "192.0.2.42",
        }
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump(stale_marker, f)
        self.assertEqual(self.run_bridge(*hook_once, stdin='{"session_id": "s1"}').returncode, 0)
        with open(state_path, encoding="utf-8") as f:
            after_stale = json.load(f)
        self.assertNotEqual(
            after_stale.get("updated_at_utc"),
            stale_ts,
            "hook watcher must refresh updated_at_utc on a state file older than 150 s",
        )
        expected_preserved = dict(stale_marker)
        expected_preserved.pop("updated_at_utc")
        actual_preserved = dict(after_stale)
        actual_preserved.pop("updated_at_utc", None)
        self.assertEqual(
            actual_preserved,
            expected_preserved,
            "hook watcher must keep all non-timestamp fields intact when refreshing an old state file",
        )

        # 4. Future timestamp (`updated_at_utc` ahead of now, F7): must not count as fresh forever.
        future_ts = (now + datetime.timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
        future_marker = dict(stale_marker, updated_at_utc=future_ts)
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump(future_marker, f)
        self.assertEqual(self.run_bridge(*hook_once, stdin='{"session_id": "s1"}').returncode, 0)
        with open(state_path, encoding="utf-8") as f:
            after_future = json.load(f)
        self.assertNotEqual(
            after_future.get("updated_at_utc"),
            future_ts,
            "hook watcher must refresh a future updated_at_utc timestamp",
        )

        # 5. Truncated / unreadable existing state file (F5): hook watcher must not overwrite it
        #    with `collect_state(repo_dir)`.
        truncated_bytes = b'{"updated_at_utc": "2026-11-01T00:00:00Z", "git": {"git_branch": "agent-active-branch"'
        with open(state_path, "wb") as f:
            f.write(truncated_bytes)
        self.assertEqual(self.run_bridge(*hook_once, stdin='{"session_id": "s1"}').returncode, 0)
        with open(state_path, "rb") as f:
            self.assertEqual(
                f.read(),
                truncated_bytes,
                "hook watcher must leave an existing unreadable/truncated state file untouched",
            )

    def test_a_normal_watcher_rewrites_a_10s_old_state_file(self):
        state_path = os.path.join(self.nodes, f"{SELF}_state.json")
        ten_secs_ago = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=10)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "updated_at_utc": ten_secs_ago,
                    "git": {"git_branch": "stale-worktree-branch"},
                },
                f,
            )

        normal_once = self.watch_args("--max-wait", "1")
        self.assertEqual(self.run_bridge(*normal_once).returncode, 0)
        with open(state_path, encoding="utf-8") as f:
            rewritten = json.load(f)
        self.assertNotEqual(rewritten.get("updated_at_utc"), ten_secs_ago)
        self.assertNotEqual(
            rewritten.get("git", {}).get("git_branch"),
            "stale-worktree-branch",
            "a normal watcher must rewrite a 10 s old state file with its own git state",
        )


class GitCalls(unittest.TestCase):
    def test_a_stuck_git_cannot_hold_the_watcher_past_its_deadline(self):
        stuck = subprocess.TimeoutExpired("git", 1)
        with mock.patch.object(bridge_module.subprocess, "check_output", side_effect=stuck) as git:
            self.assertFalse(bridge_module.get_git_info("repo")["found"])
        self.assertGreater(git.call_args.kwargs["timeout"], 0)

    def test_git_does_not_inherit_the_stdin_of_the_hook(self):
        with mock.patch.object(bridge_module.subprocess, "check_output", return_value="") as git:
            bridge_module.get_git_info("repo")
        self.assertEqual(git.call_count, 3)
        for call in git.call_args_list:
            self.assertEqual(call.kwargs["stdin"], subprocess.DEVNULL)


@unittest.skipIf(os.path.exists("G:\\"), "a real Google Drive is mounted on this machine")
class WithoutHub(BridgeCase):
    def assert_inert(self, result):
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, b"")
        self.assertEqual(os.listdir(self.home), [], "a machine without a hub must stay untouched")
        self.assertFalse(os.path.exists(self.env["OXIDESWARM_HOME"]))

    def test_peek_is_silent_and_creates_nothing(self):
        self.assert_inert(self.run_bridge("peek", use_hub=False))

    def test_the_hook_watcher_leaves_at_once_and_creates_nothing(self):
        self.assert_inert(self.run_bridge("watch", "--hook", use_hub=False, stdin="{}", timeout=10))


class Peek(BridgeCase):
    def test_peek_is_silent_without_unread_messages(self):
        result = self.run_bridge("peek")
        self.assertEqual((result.returncode, result.stdout), (0, b""))

    def test_peek_reports_unread_messages_without_their_content(self):
        self.write_message(body="SECRET-BODY-TEXT")
        output = self.run_bridge("peek").stdout.decode("utf-8")
        self.assertIn("1 unread message(s)", output)
        self.assertNotIn("SECRET-BODY-TEXT", output)

    def test_peek_writes_nothing_to_the_hub(self):
        self.run_bridge("peek")
        self.assertEqual(os.listdir(self.nodes), [])

    @unittest.skipIf(shutil.which("sh") is None, "needs sh")
    def test_the_hook_launcher_reaches_the_script(self):
        self.write_message()
        launcher = os.path.join(ROOT, "scripts", "agent_bridge_hook.sh")
        result = subprocess.run(
            ["sh", launcher, "--hub-dir", self.hub, "peek"],
            cwd=self.tmp,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=WAIT,
        )
        self.assertIn(b"1 unread message(s)", result.stdout, result.stderr)


class Send(BridgeCase):
    def write_peer_state(self, age_minutes):
        moment = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=age_minutes)
        with open(os.path.join(self.nodes, f"{PEER}_state.json"), "w", encoding="utf-8") as f:
            json.dump({"updated_at_utc": moment.strftime("%Y-%m-%dT%H:%M:%SZ")}, f)

    def send(self):
        return self.run_bridge("send", "--title", "T", "--body", "B").stdout.decode("utf-8")

    def test_send_warns_when_the_peer_never_published_state(self):
        self.assertIn("[WARN]", self.send())

    def test_send_warns_about_a_stale_peer(self):
        self.write_peer_state(30)
        self.assertIn("last seen 30 min ago", self.send())

    def test_send_is_quiet_about_a_fresh_peer(self):
        self.write_peer_state(0)
        output = self.send()
        self.assertIn("[INFO]", output)
        self.assertNotIn("[WARN]", output)

    def test_plain_http_secret_subcommands_are_absent(self):
        for verb in ("serve-secret", "pull-secret"):
            res = self.run_bridge(verb)
            self.assertNotEqual(res.returncode, 0, f"{verb} must not be accepted")
            self.assertIn("invalid choice", res.stderr.decode("utf-8"))


class Diagnose(BridgeCase):
    def setUp(self):
        super().setUp()
        self.repo = os.path.join(self.tmp, "repo")
        os.makedirs(self.repo)
        with open(os.path.join(self.repo, "Cargo.toml"), "w", encoding="utf-8") as f:
            f.write("[workspace]\nmembers = []\n")
        for cmd in (
            ["git", "init", "-q"],
            ["git", "config", "user.email", "test@oxideswarm.invalid"],
            ["git", "config", "user.name", "OxideSwarm Test"],
            ["git", "add", "Cargo.toml"],
            ["git", "commit", "-q", "-m", "initial"],
        ):
            subprocess.run(
                cmd,
                cwd=self.repo,
                env=self.env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
            )

    def argv(self, *args, use_hub=True):
        if "--repo" not in args:
            args = ("--repo", self.repo, *args)
        return super().argv(*args, use_hub=use_hub)

    def write_local_secret(self, secret_text="shared-cluster-secret-value"):
        ox_home = self.env["OXIDESWARM_HOME"]
        os.makedirs(ox_home, exist_ok=True)
        with open(os.path.join(ox_home, "cluster.secret"), "w", encoding="utf-8") as f:
            f.write(secret_text + "\n")

    def write_peer_node_state(
        self,
        peer_short=PEER,
        age_minutes=0,
        git_head=None,
        git_branch="master",
        secret_sha256_12=None,
        raw_updated_at=None,
    ):
        if raw_updated_at is not None:
            updated_at = raw_updated_at
        else:
            moment = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=age_minutes)
            updated_at = moment.strftime("%Y-%m-%dT%H:%M:%SZ")
        state = {
            "agent_id": f"agent-{peer_short}",
            "hostname": "peer-host",
            "platform": "peer-os",
            "arch": "x86_64",
            "updated_at_utc": updated_at,
            "lan_ip": "127.0.0.1",
            "local_ports": {
                "master_cluster_8088_listening": False,
                "dashboard_8080_listening": False,
            },
            "git": {
                "found": bool(git_head),
                "git_head": git_head,
                "git_branch": git_branch,
                "git_dirty": False,
                "dirty_count": 0,
            },
            "security_and_config": {
                "secret_exists": bool(secret_sha256_12),
                "secret_sha256_12": secret_sha256_12,
                "config_exists": False,
                "stale_p2p_ticket_in_config": False,
            },
        }
        with open(os.path.join(self.nodes, f"{peer_short}_state.json"), "w", encoding="utf-8") as f:
            json.dump(state, f)

    def test_diagnose_without_peer_reports_unknown_and_exits_non_zero(self):
        self.write_local_secret()
        res = self.run_bridge("diagnose")
        output = res.stdout.decode("utf-8")
        self.assertNotEqual(res.returncode, 0, output)
        self.assertIn("STATUS: UNKNOWN", output)
        self.assertIn("[PEER UNKNOWN]", output)
        self.assertNotIn("STATUS: HEALTHY", output)

    def test_diagnose_with_stale_or_invalid_peer_reports_unknown_and_exits_non_zero(self):
        self.write_local_secret()
        hb = self.run_bridge("heartbeat")
        self.assertEqual(hb.returncode, 0, hb.stderr)
        with open(os.path.join(self.nodes, f"{SELF}_state.json"), encoding="utf-8") as f:
            self_state = json.load(f)
        head = self_state["git"].get("git_head")
        fp = self_state["security_and_config"].get("secret_sha256_12")

        # 1. Peer state older than PEER_STALE_MINUTES (10 min old).
        self.write_peer_node_state(age_minutes=10, git_head=head, secret_sha256_12=fp)
        res = self.run_bridge("diagnose")
        output = res.stdout.decode("utf-8")
        self.assertNotEqual(res.returncode, 0, output)
        self.assertIn("STATUS: UNKNOWN", output)
        self.assertIn("[PEER STALE]", output)
        self.assertNotIn("STATUS: HEALTHY", output)

        # 2. Peer state with invalid updated_at_utc.
        self.write_peer_node_state(raw_updated_at="not-a-timestamp", git_head=head, secret_sha256_12=fp)
        res = self.run_bridge("diagnose")
        output = res.stdout.decode("utf-8")
        self.assertNotEqual(res.returncode, 0, output)
        self.assertIn("STATUS: UNKNOWN", output)
        self.assertIn("[PEER UNKNOWN]", output)
        self.assertNotIn("STATUS: HEALTHY", output)

    def test_diagnose_exits_zero_when_peer_is_fresh_and_synchronized(self):
        self.write_local_secret()
        self.assertEqual(self.run_bridge("heartbeat").returncode, 0)
        with open(os.path.join(self.nodes, f"{SELF}_state.json"), encoding="utf-8") as f:
            self_state = json.load(f)
        head = self_state["git"].get("git_head")
        fp = self_state["security_and_config"].get("secret_sha256_12")

        self.write_peer_node_state(age_minutes=0, git_head=head, secret_sha256_12=fp)
        res = self.run_bridge("diagnose")
        output = res.stdout.decode("utf-8")
        self.assertEqual(res.returncode, 0, output)
        self.assertIn("STATUS: HEALTHY", output)

    def test_diagnose_without_git_checkout_reports_unknown_and_exits_non_zero(self):
        self.write_local_secret()
        self.assertEqual(self.run_bridge("heartbeat").returncode, 0)
        with open(os.path.join(self.nodes, f"{SELF}_state.json"), encoding="utf-8") as f:
            self_state = json.load(f)
        head = self_state["git"].get("git_head")
        fp = self_state["security_and_config"].get("secret_sha256_12")
        self.write_peer_node_state(age_minutes=0, git_head=head, secret_sha256_12=fp)

        no_git_repo = os.path.join(self.tmp, "archive-export")
        os.makedirs(no_git_repo)
        with open(os.path.join(no_git_repo, "Cargo.toml"), "w", encoding="utf-8") as f:
            f.write("[workspace]\nmembers = []\n")

        res = self.run_bridge("--repo", no_git_repo, "diagnose")
        output = res.stdout.decode("utf-8")
        self.assertNotEqual(res.returncode, 0, output)
        self.assertIn("UNKNOWN (no git checkout)", output)
        self.assertIn("STATUS: UNKNOWN", output)
        self.assertNotIn("STATUS: HEALTHY", output)

        if not os.path.exists(os.path.join(ROOT, ".git")):
            res_default = subprocess.run(
                [sys.executable, SCRIPT, "--hub-dir", self.hub, "diagnose"],
                cwd=self.tmp,
                env=self.env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=WAIT,
            )
            out_default = res_default.stdout.decode("utf-8")
            self.assertNotEqual(res_default.returncode, 0, out_default)
            self.assertIn("UNKNOWN (no git checkout)", out_default)
            self.assertIn("STATUS: UNKNOWN", out_default)
            self.assertNotIn("STATUS: HEALTHY", out_default)

    def test_diagnose_exits_non_zero_on_git_drift_auth_mismatch_and_config_issues(self):
        self.write_local_secret()
        self.assertEqual(self.run_bridge("heartbeat").returncode, 0)
        with open(os.path.join(self.nodes, f"{SELF}_state.json"), encoding="utf-8") as f:
            self_state = json.load(f)
        head = self_state["git"].get("git_head")
        fp = self_state["security_and_config"].get("secret_sha256_12")

        # 1. Git drift -> non-zero exit
        self.write_peer_node_state(age_minutes=0, git_head="deadbeefcafe", secret_sha256_12=fp)
        res = self.run_bridge("diagnose")
        output = res.stdout.decode("utf-8")
        self.assertNotEqual(res.returncode, 0, output)
        self.assertIn("[GIT DRIFT]", output)
        self.assertNotIn("STATUS: HEALTHY", output)

        # 2. Secret fingerprint mismatch -> non-zero exit
        self.write_peer_node_state(age_minutes=0, git_head=head, secret_sha256_12="000000000000")
        res = self.run_bridge("diagnose")
        output = res.stdout.decode("utf-8")
        self.assertNotEqual(res.returncode, 0, output)
        self.assertIn("[AUTH MISMATCH]", output)
        self.assertNotIn("STATUS: HEALTHY", output)

        # 3. Stale p2p_ticket in rusty-grid.toml -> non-zero exit
        self.write_peer_node_state(age_minutes=0, git_head=head, secret_sha256_12=fp)
        cfg_path = os.path.join(self.env["OXIDESWARM_HOME"], "rusty-grid.toml")
        with open(cfg_path, "w", encoding="utf-8") as f:
            f.write('p2p_ticket = "oxticket1..."\n')
        res = self.run_bridge("diagnose")
        output = res.stdout.decode("utf-8")
        self.assertNotEqual(res.returncode, 0, output)
        self.assertIn("[LOCAL CRITICAL]", output)
        self.assertNotIn("STATUS: HEALTHY", output)

    def test_two_simulated_agents_on_one_platform_do_not_collide(self):
        self.write_local_secret()

        # Agent A (`--agent-id mac-a`) and Agent B (`--agent-id agent-mac-b`) run on the same host.
        res_a = self.run_bridge("heartbeat", "--agent-id", "mac-a")
        self.assertEqual(res_a.returncode, 0, res_a.stderr)
        res_b = self.run_bridge("--agent-id", "agent-mac-b", "heartbeat")
        self.assertEqual(res_b.returncode, 0, res_b.stderr)

        path_a = os.path.join(self.nodes, "mac-a_state.json")
        path_b = os.path.join(self.nodes, "mac-b_state.json")
        default_path = os.path.join(self.nodes, f"{SELF}_state.json")
        self.assertTrue(os.path.exists(path_a), "mac-a must write its own state file")
        self.assertTrue(os.path.exists(path_b), "mac-b must write its own state file")
        self.assertFalse(os.path.exists(default_path), "default platform state file must not be touched")

        with open(path_a, encoding="utf-8") as f:
            state_a = json.load(f)
        with open(path_b, encoding="utf-8") as f:
            state_b = json.load(f)
        self.assertEqual(state_a["agent_id"], "agent-mac-a")
        self.assertEqual(state_b["agent_id"], "agent-mac-b")

        # Both agents diagnose against each other cleanly without colliding.
        diag_a = self.run_bridge("diagnose", "--agent-id", "mac-a")
        out_a = diag_a.stdout.decode("utf-8")
        self.assertEqual(diag_a.returncode, 0, out_a)
        self.assertIn("Self: agent-mac-a, Peer: agent-mac-b", out_a)
        self.assertIn("STATUS: HEALTHY", out_a)

        diag_b = self.run_bridge("diagnose", "--agent-id", "mac-b")
        out_b = diag_b.stdout.decode("utf-8")
        self.assertEqual(diag_b.returncode, 0, out_b)
        self.assertIn("Self: agent-mac-b, Peer: agent-mac-a", out_b)
        self.assertIn("STATUS: HEALTHY", out_b)

        # Mailboxes are isolated per agent-id as well.
        send_res = self.run_bridge("send", "--agent-id", "mac-a", "--title", "Ping", "--body", "From A to B")
        self.assertEqual(send_res.returncode, 0, send_res.stderr)
        inbox_b = self.run_bridge("inbox", "--agent-id", "mac-b").stdout.decode("utf-8")
        self.assertIn("From A to B", inbox_b)
        inbox_a = self.run_bridge("inbox", "--agent-id", "mac-a").stdout.decode("utf-8")
        self.assertIn("No unread messages", inbox_a)

    def test_no_owner_paths_or_private_gateway_in_source(self):
        import hashlib
        import re

        with open(SCRIPT, encoding="utf-8") as f:
            source = f.read()
        # Store SHA-256 digests rather than literal owner strings so this test file passes
        # the repository-wide REL-2 guard in tests/test_launcher_scripts.py.
        forbidden_digests = {
            "ed4c1136c28a4257e7acd5c4542269a3bd012403399910eee9c91c2481977714",
            "da188019722950874ae0a04eb811a02d1f69e240c642dbde975888909ba6d55c",
            "c5eb5a4cc76a5cdb16e79864b9ccd26c3553f0c396d0a21bafb7be71c1efcd8c",
        }
        tokens = set(re.findall(r"[A-Za-z0-9_.-]+", source))
        found = [
            tok
            for tok in sorted(tokens)
            if hashlib.sha256(tok.encode("utf-8")).hexdigest() in forbidden_digests
        ]
        self.assertEqual(found, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
