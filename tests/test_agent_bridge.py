#!/usr/bin/env python3
"""
tests/test_agent_bridge.py - regression tests for the message trigger in scripts/agent_bridge.py.

Every test runs the real script against a throw-away hub, with a private HOME and OXIDESWARM_HOME.
Nothing here touches Google Drive, the real ~/.oxideswarm, the network or a built binary.

Run:   python3 tests/test_agent_bridge.py
Mutation check: OX_SCRIPT_ROOT=/path/to/an/older/checkout python3 tests/test_agent_bridge.py
"""

import datetime
import glob
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
        self.env.pop("OXIDESWARM_AGENT_ID", None)
        self.env.pop("PYTHONIOENCODING", None)
        # Never probe the host's real ports (an owner's master may listen on 8088).
        self.env["OXIDESWARM_BRIDGE_ASSUME_LISTENING"] = ""
        # Never discover a real Google Drive mount on G:/H:/D: during isolated tests.
        self.env["OXIDESWARM_BRIDGE_DISABLE_DRIVE_SCAN"] = "1"
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
        long_hook_args = self.watch_args("--hook", "--max-wait", "120")
        first = self.start_bridge(*long_hook_args, stdin='{"session_id": "s1"}')
        lock_pattern = os.path.join(self.env["OXIDESWARM_HOME"], "**", "watch_*.lock")
        deadline = time.monotonic() + 60.0
        while time.monotonic() < deadline:
            if glob.glob(lock_pattern, recursive=True):
                break
            if first.poll() is not None:
                break
            time.sleep(0.05)
        time.sleep(0.2)
        self.assertIsNone(first.poll(), "first hook watcher exited unexpectedly before second hook")

        started = time.monotonic()
        result = self.run_bridge(*long_hook_args, stdin='{"session_id": "s1"}', timeout=60)
        elapsed = time.monotonic() - started
        print(f"second hook exited in {elapsed:.3f}s")
        self.assertEqual(result.returncode, 0)
        self.assertIsNone(first.poll(), "first watcher must still be alive when the second hook leaves")
        self.assertLess(elapsed, 60, f"second hook hung for {elapsed:.2f}s")

        other = self.start_bridge(*self.watch_args("--hook", "--max-wait", "30"), stdin='{"session_id": "s2"}')
        deadline2 = time.monotonic() + 60.0
        while time.monotonic() < deadline2:
            if len(glob.glob(lock_pattern, recursive=True)) >= 2:
                break
            if other.poll() is not None:
                break
            time.sleep(0.05)
        time.sleep(0.2)
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

    def test_drive_scan_opt_out_isolates_hub_discovery(self):
        drive_hub = os.path.join("G:\\", "My Drive", "OxideSwarm_Agent_Hub")
        base_env = dict(os.environ, HOME=self.home, USERPROFILE=self.home)
        base_env.pop("OXIDESWARM_AGENT_HUB", None)
        base_env.pop(bridge_module.DISABLE_DRIVE_SCAN_ENV, None)
        with (
            mock.patch.object(bridge_module.glob, "glob", return_value=[]),
            mock.patch.object(
                bridge_module.os.path,
                "exists",
                side_effect=lambda p: p in ("G:\\", drive_hub),
            ),
            mock.patch.object(bridge_module.os, "listdir", return_value=[]),
        ):
            with mock.patch.dict(os.environ, dict(base_env, **{bridge_module.DISABLE_DRIVE_SCAN_ENV: "1"}), clear=True):
                self.assertNotIn(drive_hub, bridge_module.hub_candidates())
                self.assertIsNone(bridge_module.discover_hub_dir(None))
            with mock.patch.dict(os.environ, base_env, clear=True):
                self.assertIn(drive_hub, bridge_module.hub_candidates())
                self.assertEqual(bridge_module.discover_hub_dir(None), os.path.abspath(drive_hub))


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

    def test_two_simulated_masters_on_one_platform_are_a_split_brain(self):
        self.write_local_secret()
        env = dict(self.env, OXIDESWARM_BRIDGE_ASSUME_LISTENING="8088")
        for agent in ("mac-a", "mac-b"):
            res = self.run_bridge("heartbeat", "--agent-id", agent, env=env)
            self.assertEqual(res.returncode, 0, res.stderr)
        diag = self.run_bridge("diagnose", "--agent-id", "mac-a", env=env)
        self.assertIn("[SPLIT-BRAIN MASTER]", diag.stdout.decode("utf-8"))
        self.assertNotEqual(diag.returncode, 0)

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


class IdentityFromEnvironment(BridgeCase):
    """OXIDESWARM_AGENT_ID gives the shared Claude Code hooks a per-machine identity; --agent-id still wins."""

    def owner_inbox(self):
        path = os.path.join(self.hub, "mailbox", "to_owner")
        os.makedirs(path, exist_ok=True)
        return path

    def write_owner_message(self, title="For the coordinator"):
        with open(os.path.join(self.owner_inbox(), "MSG_owner_1.md"), "w", encoding="utf-8") as f:
            f.write(f"# [HIGH] {title}\n\n- **From**: `agent-win-2` (`peer-host`)\n\n---\n\nOwner body.\n")

    def test_inbox_reads_the_mailbox_the_variable_names_and_a_flag_wins(self):
        self.write_owner_message()
        self.write_message(title="For the platform agent")
        env = dict(self.env, OXIDESWARM_AGENT_ID="agent-owner")

        result = self.run_bridge("inbox", env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        stdout = result.stdout.decode("utf-8", "replace")
        self.assertIn("For the coordinator", stdout)
        self.assertNotIn("For the platform agent", stdout)

        result = self.run_bridge("inbox", "--agent-id", SELF, env=env)
        stdout = result.stdout.decode("utf-8", "replace")
        self.assertIn("For the platform agent", stdout)
        self.assertNotIn("For the coordinator", stdout)

    def test_send_signs_with_the_identity_of_the_variable(self):
        env = dict(self.env, OXIDESWARM_AGENT_ID="owner")
        result = self.run_bridge("send", "--peer-id", "win-2", "--title", "Next item", "--body", "W2b.", env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        sent = os.listdir(os.path.join(self.hub, "mailbox", "to_win-2"))
        self.assertEqual(len(sent), 1, sent)
        self.assertIn("_from_owner_", sent[0])
        with open(os.path.join(self.hub, "mailbox", "to_win-2", sent[0]), encoding="utf-8") as f:
            self.assertIn("`agent-owner`", f.read())

    def test_the_hook_commands_follow_the_variable(self):
        self.write_owner_message()
        env = dict(self.env, OXIDESWARM_AGENT_ID="owner")
        result = self.run_bridge("peek", env=env)
        self.assertIn("1 unread message(s)", result.stdout.decode("utf-8", "replace"))
        result = self.run_bridge("peek")
        self.assertEqual(result.stdout.decode("utf-8", "replace").strip(), "", "the platform mailbox is empty")

        self.env["OXIDESWARM_AGENT_ID"] = "owner"
        code, stdout, _ = self.finish(self.start_bridge(*self.watch_args("--once-on-message", "--max-wait", "20")))
        self.assertEqual(code, 0)
        self.assertIn("For the coordinator", stdout)
        self.assertTrue(os.path.exists(os.path.join(self.nodes, "owner_state.json")))
        self.assertFalse(os.path.exists(os.path.join(self.nodes, f"{SELF}_state.json")))


class Activity(BridgeCase):
    def activity(self, *args, **kw):
        return self.run_bridge("activity", "--agent-id", "agent-win", *args, **kw)

    def path(self, short="win"):
        return os.path.join(self.nodes, f"{short}_activity.json")

    def load(self, short="win"):
        with open(self.path(short), encoding="utf-8") as f:
            return json.load(f)

    def hand_write(self, short, state, age_minutes, task="old task", step="old step"):
        moment = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=age_minutes)
        stamp = moment.strftime("%Y-%m-%dT%H:%M:%SZ")
        record = {
            "agent_id": f"agent-{short}",
            "state": state,
            "task": task,
            "step": step,
            "updated_at_utc": stamp,
            "started_at_utc": stamp,
        }
        with open(self.path(short), "w", encoding="utf-8") as f:
            json.dump(record, f)

    def test_write_and_show_round_trip(self):
        res = self.activity("--state", "active", "--task", "RC smoke test", "--step", "building", "--note", "n")
        self.assertEqual(res.returncode, 0, res.stderr)
        record = self.load()
        self.assertEqual(
            (record["agent_id"], record["state"], record["task"], record["step"], record["note"]),
            ("agent-win", "active", "RC smoke test", "building", "n"),
        )
        self.assertRegex(record["updated_at_utc"], r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$")
        self.assertEqual(record["started_at_utc"], record["updated_at_utc"])
        self.assertEqual([n for n in os.listdir(self.nodes) if n.endswith(".tmp")], [])
        shown = self.run_bridge("activity", "--show").stdout.decode("utf-8")
        for text in ("agent-win", "ACTIVE", "RC smoke test", "building", "(note: n)", "just now"):
            self.assertIn(text, shown)
        self.assertNotIn("STALE", shown)

    def test_show_filters_by_agent_and_handles_none(self):
        self.assertIn("No agent has reported", self.run_bridge("activity", "--show").stdout.decode("utf-8"))
        self.hand_write("win", "idle", 1)
        self.hand_write("mac", "done", 1)
        only = self.run_bridge("activity", "--show", "--agent-id", "mac").stdout.decode("utf-8")
        self.assertIn("agent-mac", only)
        self.assertNotIn("agent-win", only)

    def test_invalid_state_is_rejected(self):
        res = self.activity("--state", "sleeping", "--task", "t")
        self.assertNotEqual(res.returncode, 0)
        self.assertFalse(os.path.exists(self.path()))
        self.assertNotEqual(self.activity("--task", "t").returncode, 0)

    def test_newline_and_length_are_rejected(self):
        for field, value in (("--task", "a\nb"), ("--step", "a\rb"), ("--note", "x" * 201)):
            res = self.activity("--state", "active", field, value)
            self.assertEqual(res.returncode, 2, field)
            self.assertIn("[ACTIVITY ERROR]", res.stderr.decode("utf-8"))
            self.assertFalse(os.path.exists(self.path()), field)
        self.assertEqual(self.activity("--state", "active", "--task", "x" * 200).returncode, 0)

    def test_started_at_is_kept_across_updates_and_reset_by_a_new_run(self):
        self.activity("--state", "active", "--task", "t", "--step", "one")
        first = self.load()
        old = "2026-01-01T00:00:00Z"
        first["started_at_utc"] = old
        with open(self.path(), "w", encoding="utf-8") as f:
            json.dump(first, f)
        self.activity("--state", "active", "--step", "two")
        second = self.load()
        self.assertEqual((second["started_at_utc"], second["task"], second["step"]), (old, "t", "two"))
        self.activity("--state", "blocked", "--note", "permission prompt")
        self.assertEqual(self.load()["started_at_utc"], old)
        self.activity("--state", "done")
        self.assertEqual(self.load()["started_at_utc"], old)
        self.activity("--state", "active", "--task", "next")
        self.assertNotEqual(self.load()["started_at_utc"], old)

    def test_active_and_blocked_go_stale_after_the_threshold_but_done_and_idle_do_not(self):
        limit = bridge_module.ACTIVITY_STALE_MINUTES
        self.assertEqual(limit, 20)
        now = datetime.datetime(2026, 11, 16, 12, 0, 0)
        for state, age, stale in (
            ("active", limit - 1, False),
            ("active", limit + 1, True),
            ("blocked", limit + 1, True),
            ("done", 600, False),
            ("idle", 600, False),
        ):
            stamp = (now - datetime.timedelta(minutes=age)).strftime("%Y-%m-%dT%H:%M:%SZ")
            record = {"agent_id": "agent-x", "state": state, "task": "t", "step": "s", "updated_at_utc": stamp}
            self.assertEqual(bridge_module.activity_is_stale(record, now), stale, (state, age))
            line = bridge_module.describe_activity(record, now)
            self.assertEqual("STALE" in line, stale, line)
            if stale:
                self.assertIn("permission prompt or stuck", line)

    def test_show_marks_a_hand_written_old_active_agent_stale(self):
        self.hand_write("win", "active", 45)
        shown = self.run_bridge("activity", "--show").stdout.decode("utf-8")
        self.assertIn("STALE", shown)
        self.assertIn("45 min ago", shown)

    def test_diagnose_and_peek_include_the_activity_line(self):
        self.hand_write(PEER, "blocked", 2, task="RC smoke test", step="waiting")
        diag = self.run_bridge("diagnose", "--peer-id", PEER).stdout.decode("utf-8")
        self.assertIn(f"agent-{PEER}: BLOCKED - RC smoke test / waiting", diag)
        self.assertIn("Unread Inbox", diag)
        peek = self.run_bridge("peek", "--peer-id", PEER).stdout.decode("utf-8")
        self.assertIn(f"Activity: agent-{PEER}: BLOCKED", peek)
        # diagnose publishes this machine's state; peek writes nothing.
        self.assertEqual(sorted(os.listdir(self.nodes)), sorted([f"{PEER}_activity.json", f"{SELF}_state.json"]))



class CoordCase(BridgeCase):
    """A fake repository with an origin/master and an origin/feat that is ahead of it, plus a temporary hub."""

    ME = "mac-1"

    def git(self, *args):
        return (
            subprocess.run(
                ["git", *args],
                cwd=self.repo,
                env=self.env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )
            .stdout.decode("utf-8")
            .strip()
        )

    def write_repo_file(self, rel, text):
        path = os.path.join(self.repo, *rel.split("/"))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)

    def commit_all(self, message):
        self.git("add", "-A")
        self.git("commit", "-q", "-m", message)

    def setUp(self):
        super().setUp()
        self.repo = os.path.join(self.tmp, "repo")
        os.makedirs(self.repo)
        self.git("init", "-q")
        self.git("config", "user.email", "test@oxideswarm.invalid")
        self.git("config", "user.name", "OxideSwarm Test")
        self.write_repo_file("Cargo.toml", "[workspace]\nmembers = []\n")
        self.write_repo_file("docs/design/0087-coordination.md", "# 0087\n")
        self.write_repo_file("docs/ROADMAP.md", "Decision Q84 and Q70 are closed.\n")
        self.write_repo_file("docs/BACKLOG.md", "## Phase 12\n\n| TEST-48 | a |\n| BUG-0 | b |\n")
        self.commit_all("base")
        self.git("update-ref", "refs/remotes/origin/master", "HEAD")
        self.write_repo_file("docs/design/0090-ahead.md", "# 0090\n")
        self.write_repo_file("docs/ROADMAP.md", "Decision Q84 and Q90 are closed.\n")
        self.write_repo_file("docs/BACKLOG.md", "## Phase 12\n\n## Phase 13\n\n| TEST-50 | a |\n")
        self.commit_all("ahead")
        self.git("update-ref", "refs/remotes/origin/feat", "HEAD")
        self.git("reset", "-q", "--hard", "origin/master")

    def argv(self, *args, use_hub=True):
        if "--repo" not in args:
            args = ("--repo", self.repo, *args)
        return super().argv(*args, use_hub=use_hub)

    def claim(self, *args, who=None, **kw):
        return self.run_bridge("claim", "--agent-id", who or self.ME, "--settle", "0", *args, **kw)

    def out(self, result):
        return result.stdout.decode("utf-8").strip()

    def hand_claim(self, kind, value, agent, created_at, branch="", title="theirs"):
        stamp = "".join(c for c in created_at if c.isdigit() or c in "TZ")
        folder = os.path.join(self.hub, "claims", kind)
        os.makedirs(folder, exist_ok=True)
        name = f"{value}__{agent}__{stamp}.json"
        record = {
            "kind": kind,
            "value": value,
            "agent": agent,
            "title": title,
            "branch": branch,
            "pr": None,
            "created_at": created_at,
        }
        with open(os.path.join(folder, name), "w", encoding="utf-8") as f:
            json.dump(record, f)
        return name

    def claim_files(self, kind):
        folder = os.path.join(self.hub, "claims", kind)
        return sorted(os.listdir(folder)) if os.path.isdir(folder) else []


class Claims(CoordCase):
    def test_next_free_value_of_every_kind_counts_master_branches_and_hub_claims(self):
        # design: 0087 on master, 0090 on origin/feat -> 0091; the claim itself then counts for the next agent.
        first = self.claim("design", "--title", "first")
        self.assertEqual((first.returncode, self.out(first)), (0, "0091"), first.stderr)
        second = self.claim("design", "--title", "second", who="win")
        self.assertEqual(self.out(second), "0092")
        self.assertEqual(len(self.claim_files("design")), 2)
        # q: Q84 on master, Q90 on a branch -> Q91.
        self.assertEqual(self.out(self.claim("q", "--title", "q")), "Q91")
        # phase: Phase 12 on master, Phase 13 on a branch -> 14.
        self.assertEqual(self.out(self.claim("phase", "--title", "p")), "14")
        # item: TEST-48 on master, TEST-50 on a branch -> TEST-51; another prefix is counted on its own.
        self.assertEqual(self.out(self.claim("item", "--prefix", "TEST", "--title", "t")), "TEST-51")
        self.assertEqual(self.out(self.claim("item", "--prefix", "BUG", "--title", "b")), "BUG-1")
        self.assertEqual(self.out(self.claim("item", "--prefix", "NEW", "--title", "n")), "NEW-1")

    def test_a_claim_record_holds_project_metadata_only(self):
        self.claim("design", "--title", "A title", "--branch", "claude/x", "--pr", "#300")
        name = self.claim_files("design")[0]
        self.assertTrue(name.startswith(f"0091__{self.ME}__") and name.endswith(".json"), name)
        with open(os.path.join(self.hub, "claims", "design", name), encoding="utf-8") as f:
            record = json.load(f)
        self.assertEqual(
            sorted(record), ["agent", "branch", "created_at", "kind", "nonce", "pr", "title", "value"]
        )
        self.assertRegex(record["created_at"], r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$")
        self.assertTrue(name.endswith(f"__{record['nonce']}.json"), name)
        self.assertEqual((record["pr"], record["branch"], record["value"]), (300, "claude/x", "0091"))

    def test_bad_input_is_refused(self):
        self.assertEqual(self.claim("item", "--title", "t").returncode, 2)
        self.assertEqual(self.claim("design").returncode, 2)
        self.assertEqual(self.claim("design", "--title", "two\nlines").returncode, 2)
        self.assertEqual(self.claim("design", "--title", "t", "--value", "Q4").returncode, 2)
        self.assertEqual(self.claim_files("design"), [])

    def test_the_later_claim_withdraws_and_takes_the_next_value(self):
        self.hand_claim("design", "0091", "win", "2000-01-01T00:00:00Z")
        result = self.claim("design", "--value", "0091", "--title", "late")
        self.assertEqual((result.returncode, self.out(result)), (0, "0092"), result.stderr)
        files = self.claim_files("design")
        self.assertEqual(len([f for f in files if f.startswith("0091__")]), 1)
        self.assertTrue(files[0].startswith("0091__win__"), files)
        self.assertTrue(any(f.startswith(f"0092__{self.ME}__") for f in files), files)

    def test_equal_timestamps_go_to_the_smaller_agent_id(self):
        moment = "2026-11-17T10:00:00Z"
        for me, other, expect_win in (("aaa", "zzz", True), ("zzz", "aaa", False)):
            with self.subTest(me=me):
                shutil.rmtree(os.path.join(self.hub, "claims", "design"), ignore_errors=True)
                self.hand_claim("design", "0100", other, moment)
                with mock.patch.object(bridge_module, "claim_now_iso", return_value=moment):
                    won, path = bridge_module.try_claim(self.hub, "design", "0100", me, "t", "", None, 0)
                self.assertEqual(won, expect_win)
                self.assertEqual(os.path.exists(path), expect_win)
                self.assertEqual(len(self.claim_files("design")), 2 if expect_win else 1)

    def test_two_sessions_of_one_agent_with_one_timestamp_do_not_both_win(self):
        moment = "2026-11-17T10:00:00.500000Z"
        for mine, rival, expect_win in (("aaaa0000", "bbbb0000", True), ("bbbb0000", "aaaa0000", False)):
            with self.subTest(mine=mine):
                shutil.rmtree(os.path.join(self.hub, "claims", "design"), ignore_errors=True)
                folder = os.path.join(self.hub, "claims", "design")
                os.makedirs(folder)
                rival_name = f"0100__owner__20261117T100000500000Z__{rival}.json"
                with open(os.path.join(folder, rival_name), "w", encoding="utf-8") as f:
                    json.dump(
                        {"kind": "design", "value": "0100", "agent": "owner", "title": "other session",
                         "created_at": moment, "nonce": rival},
                        f,
                    )
                with mock.patch.object(bridge_module, "claim_now_iso", return_value=moment), mock.patch.object(
                    bridge_module.secrets, "token_hex", return_value=mine
                ):
                    won, path = bridge_module.try_claim(self.hub, "design", "0100", "owner", "t", "", None, 0)
                self.assertEqual(won, expect_win)
                # Exactly one file holds the value, and the file names differ.
                holders = [f for f in self.claim_files("design") if f.startswith("0100__")]
                self.assertEqual(len(holders), 2 if expect_win else 1)
                self.assertNotEqual(os.path.basename(path), rival_name)

    def test_the_loser_of_a_same_agent_tie_takes_the_next_value(self):
        moment = "2026-11-17T10:00:00.500000Z"
        self.hand_claim("design", "0091", "owner", moment)
        with mock.patch.object(bridge_module, "claim_now_iso", return_value=moment), mock.patch.object(
            bridge_module.secrets, "token_hex", return_value="ffffffff"
        ):
            result = self.claim("design", "--value", "0091", "--title", "t", "--session", "window 2", who="owner")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(self.out(result), "0092")

    def test_old_claims_without_nonce_or_fraction_still_order_correctly(self):
        older = {"created_at": "2026-11-17T10:00:00Z", "agent": "z"}
        newer = {"created_at": "2026-11-17T10:00:00.250000Z", "agent": "a", "nonce": "x"}
        self.assertLess(bridge_module.claim_sort_key(older), bridge_module.claim_sort_key(newer))

    def test_three_lost_tries_exit_non_zero(self):
        # An earlier rival claim appears after every scan, so each try loses.
        real = bridge_module.try_claim

        def losing(hub, kind, value, agent, title, branch, pr, settle, session=""):
            self.hand_claim(kind, value, "win", "2000-01-01T00:00:00Z")
            return real(hub, kind, value, agent, title, branch, pr, settle, session=session)

        args = bridge_module.argparse.Namespace(
            hub_dir=self.hub,
            repo=self.repo,
            sub_repo=None,
            agent_id=None,
            sub_agent_id=self.ME,
            kind="design",
            prefix=None,
            title="t",
            branch=None,
            pr=None,
            value=None,
            settle=0.0,
            list=False,
            release=None,
            session=None,
        )
        with mock.patch.object(bridge_module, "try_claim", losing), mock.patch("sys.stderr"):
            self.assertEqual(bridge_module.cmd_claim(args), 1)
        self.assertFalse([f for f in self.claim_files("design") if f"__{self.ME}__" in f])

    def test_list_shows_in_use_open_and_stale(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        old = (now - datetime.timedelta(days=20)).strftime("%Y-%m-%dT%H:%M:%SZ")
        fresh = (now - datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.hand_claim("design", "0087", "win", fresh, title="already merged")
        self.hand_claim("design", "0095", "win", fresh, title="fresh plan")
        self.hand_claim("design", "0096", "win", old, branch="gone", title="dropped plan")
        self.hand_claim("design", "0097", "win", old, branch="feat", title="old but on origin")
        self.hand_claim("item", "TEST-50", "win", old, title="no branch named")
        shown = self.out(self.run_bridge("claim", "--list", "--repo", self.repo))
        lines = {line.split()[1]: line for line in shown.splitlines()}
        self.assertIn("[in use]", lines["0087"])
        self.assertIn("[open]", lines["0095"])
        self.assertIn("[stale]", lines["0096"])
        self.assertIn("[open]", lines["0097"])
        self.assertIn("[stale]", lines["TEST-50"])
        only = self.out(self.run_bridge("claim", "--list", "item", "--repo", self.repo))
        self.assertEqual(len(only.splitlines()), 1)

    def test_release_deletes_an_own_claim_and_refuses_another_agents(self):
        mine = self.claim("design", "--title", "mine")
        self.assertEqual(self.out(mine), "0091")
        other = self.hand_claim("design", "0092", "win", "2026-11-17T10:00:00Z")
        name = [f for f in self.claim_files("design") if f.startswith("0091__")][0]
        refused = self.run_bridge("claim", "--release", other, "--agent-id", self.ME)
        self.assertEqual(refused.returncode, 1)
        self.assertIn(other, self.claim_files("design"))
        ok = self.run_bridge("claim", "--release", name, "--agent-id", self.ME)
        self.assertEqual(ok.returncode, 0, ok.stderr)
        self.assertNotIn(name, self.claim_files("design"))
        self.assertEqual(self.run_bridge("claim", "--release", "nope.json", "--agent-id", self.ME).returncode, 1)

    def test_without_a_hub_claim_prints_a_notice_and_exits_zero(self):
        result = self.run_bridge("claim", "design", "--title", "t", "--settle", "0", use_hub=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("No hub found", self.out(result))


class ActivityFilesAndOverlap(CoordCase):
    def hand_write(self, short, state, age_minutes, files=None, branch=None, pr=None):
        moment = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=age_minutes)
        stamp = moment.strftime("%Y-%m-%dT%H:%M:%SZ")
        record = {"agent_id": f"agent-{short}", "state": state, "task": "t", "step": "s", "updated_at_utc": stamp}
        if files is not None:
            record["files"] = files
        if branch:
            record["branch"] = branch
        if pr:
            record["pr"] = pr
        with open(os.path.join(self.nodes, f"{short}_activity.json"), "w", encoding="utf-8") as f:
            json.dump(record, f)

    def activity(self, *args):
        return self.run_bridge("activity", "--agent-id", self.ME, *args)

    def mine(self):
        with open(os.path.join(self.nodes, f"{self.ME}_activity.json"), encoding="utf-8") as f:
            return json.load(f)

    def test_files_branch_and_pr_are_stored_and_kept_by_a_later_report(self):
        result = self.activity(
            "--state",
            "active",
            "--task",
            "x",
            "--branch",
            "claude/x",
            "--pr",
            "#12",
            "--files",
            "src/b.rs, src\\a.rs,./src/b.rs",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        record = self.mine()
        self.assertEqual((record["branch"], record["pr"], record["files"]), ("claude/x", 12, ["src/a.rs", "src/b.rs"]))
        self.activity("--state", "active", "--step", "later")
        self.assertEqual(self.mine()["files"], ["src/a.rs", "src/b.rs"])
        self.assertIn("branch claude/x, PR #12", self.out(self.run_bridge("activity", "--show")))

    def test_bad_files_and_pr_are_refused(self):
        for bad in ("/etc/passwd", "../x", "C:/x", "a/../../b"):
            self.assertEqual(self.activity("--state", "active", "--files", bad).returncode, 2, bad)
        self.assertEqual(self.activity("--state", "active", "--pr", "abc").returncode, 2)
        self.assertFalse(os.path.exists(os.path.join(self.nodes, f"{self.ME}_activity.json")))

    def test_files_from_git_lists_branch_uncommitted_and_untracked_paths(self):
        self.write_repo_file("src/committed.rs", "fn a() {}\n")
        self.commit_all("work")
        self.write_repo_file("docs/ROADMAP.md", "changed\n")
        self.write_repo_file("src/new file.rs", "fn b() {}\n")
        result = self.activity("--state", "active", "--files-from-git", "--branch", "claude/x")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.mine()["files"], ["docs/ROADMAP.md", "src/committed.rs", "src/new file.rs"])

    def test_overlap_names_shared_files_and_the_shared_by_design_class(self):
        self.hand_write("win", "active", 2, ["src/a.rs", "CHANGELOG.md", "only/win.rs"], branch="claude/w", pr=5)
        result = self.activity("--state", "active", "--files", "src/a.rs,CHANGELOG.md,only/mine.rs")
        text = self.out(result)
        self.assertEqual(result.returncode, 0)
        self.assertIn("[OVERLAP WARNING] src/a.rs: also changed by agent-win, branch claude/w, PR #5", text)
        self.assertIn("CHANGELOG.md: shared, expect a one-line merge", text)
        self.assertNotIn("WARNING] CHANGELOG.md", text)
        self.assertNotIn("only/", text)

    def test_overlap_ignores_stale_done_and_self_and_reports_unknown_files(self):
        self.hand_write("old", "active", 45, ["src/a.rs"])
        self.hand_write("fin", "done", 2, ["src/a.rs"])
        self.hand_write("blank", "blocked", 2)
        self.hand_write("blk", "blocked", 2, ["src/a.rs"], branch="b")
        self.activity("--state", "active", "--files", "src/a.rs")
        text = self.out(self.activity("--overlap"))
        self.assertIn("agent-blk, branch b", text)
        self.assertNotIn("agent-old", text)
        self.assertNotIn("agent-fin", text)
        self.assertNotIn("agent-mac-1", text)
        self.assertIn("Files unknown for: agent-blank", text)

    def test_overlap_alone_writes_nothing_and_never_blocks(self):
        self.hand_write("win", "active", 2, ["src/a.rs"])
        before = sorted(os.listdir(self.nodes))
        result = self.activity("--overlap", "--files", "src/a.rs")
        self.assertEqual(result.returncode, 0)
        self.assertIn("WARNING] src/a.rs", self.out(result))
        self.assertEqual(sorted(os.listdir(self.nodes)), before)
        # No files known for this agent at all.
        self.assertIn("No files recorded", self.out(self.activity("--overlap")))

    def test_overlap_without_a_hub_exits_zero_with_a_notice(self):
        result = self.run_bridge("activity", "--overlap", "--files", "a.rs", use_hub=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("No hub found", self.out(result))


class GridBackendTests(BridgeCase):
    def make_fake_oxideswarm(self, mode="ok"):
        log_path = os.path.join(self.tmp, "fake_ox_calls.jsonl")
        script_path = os.path.join(self.tmp, "fake_oxideswarm.py")
        with open(script_path, "w", encoding="utf-8") as f:
            f.write(
                f"""import json, os, sys
log_path = {log_path!r}
mode = {mode!r}
argv = sys.argv[1:]
with open(log_path, "a", encoding="utf-8") as log:
    log.write(json.dumps(argv) + "\\n")
if len(argv) < 2 or argv[0] != "mail":
    sys.stderr.write(f"unexpected argv: {{argv}}\\n")
    sys.exit(2)
sub = argv[1]
if mode == "v10":
    sys.stderr.write("Error: peer protocol version 10 does not support client message MailSend (requires version 11)\\n")
    sys.exit(1)
if sub == "send":
    print(json.dumps({{"id": 7, "to": ["agent-mac"], "topic": "hello", "attachments": [], "recipient_links": {{}}}}))
    sys.exit(0)
if sub == "fetch":
    msgs = [{{
        "id": 7,
        "from": "agent-mac",
        "to": [f"agent-{SELF}"],
        "topic": "Review request",
        "body": "Please review #487",
        "attachments": [],
        "created_at_utc": 1700000000,
    }}] if mode != "empty" else []
    print(json.dumps({{"cursor": 0, "messages": msgs, "saved_files": []}}))
    sys.exit(0)
if sub == "ack":
    print(json.dumps({{"consumer": f"agent-{SELF}", "cursor": 7}}))
    sys.exit(0)
if sub == "watch":
    if "--hook" in argv:
        sys.stderr.write("[oxideswarm mail] #7 from=agent-mac topic=Wakeup attachments=0\\n")
        sys.exit(2)
    print("#7 from=agent-mac topic=Wakeup attachments=0")
    sys.exit(0)
sys.exit(2)
"""
            )
        if os.name == "nt":
            shim = os.path.join(self.tmp, "fake_oxideswarm.cmd")
            with open(shim, "w", encoding="utf-8") as f:
                f.write(f'@echo off\r\n"{sys.executable}" "{script_path}" %*\r\n')
        else:
            shim = os.path.join(self.tmp, "fake_oxideswarm.sh")
            with open(shim, "w", encoding="utf-8") as f:
                f.write(f'#!/bin/sh\nexec "{sys.executable}" "{script_path}" "$@"\n')
            os.chmod(shim, 0o755)
        return shim, log_path

    def read_calls(self, log_path):
        if not os.path.exists(log_path):
            return []
        with open(log_path, encoding="utf-8") as f:
            return [json.loads(line) for line in f if line.strip()]

    def test_grid_backend_send_inbox_peek_ack_and_watch_via_env(self):
        shim, log_path = self.make_fake_oxideswarm("ok")
        env = dict(self.env, OXIDESWARM_BRIDGE_BACKEND="grid", OXIDESWARM_BIN=shim)

        res_send = self.run_bridge("send", "--title", "Hello", "--body", "Body", use_hub=False, env=env)
        self.assertEqual(res_send.returncode, 0, res_send.stderr.decode("utf-8", "replace"))
        self.assertIn("#7", res_send.stdout.decode("utf-8", "replace"))

        res_peek = self.run_bridge("peek", use_hub=False, env=env)
        self.assertEqual(res_peek.returncode, 0)
        self.assertIn("1 unread", res_peek.stdout.decode("utf-8", "replace"))

        res_inbox = self.run_bridge("inbox", use_hub=False, env=env)
        self.assertEqual(res_inbox.returncode, 0)
        self.assertIn("Please review #487", res_inbox.stdout.decode("utf-8", "replace"))

        res_ack = self.run_bridge("inbox", "--ack", "all", use_hub=False, env=env)
        self.assertEqual(res_ack.returncode, 0, res_ack.stderr.decode("utf-8", "replace"))

        res_hook = self.run_bridge("watch", "--hook", "--consumer", "agy", use_hub=False, env=env)
        self.assertEqual(res_hook.returncode, 2)

        calls = self.read_calls(log_path)
        self.assertEqual([c[1] for c in calls], ["send", "fetch", "fetch", "fetch", "ack", "watch"])
        watch_call = calls[-1]
        self.assertIn("--consumer", watch_call)
        self.assertIn(f"agent-{SELF}:agy", watch_call)
        self.assertIn("--topic", watch_call)
        self.assertIn(f"agent-{SELF}", watch_call)
        self.assertIn("--ack", watch_call)

        # Multi-byte UTF-8 title must stay <= 256 encoded bytes (`MAX_MAIL_TOPIC_BYTES`).
        res_utf8 = self.run_bridge(
            "send",
            "--title",
            "🔥" * 100,
            "--body",
            "utf8",
            use_hub=False,
            env=env,
        )
        self.assertEqual(res_utf8.returncode, 0, res_utf8.stderr.decode("utf-8", "replace"))
        send_call = self.read_calls(log_path)[-1]
        topic_val = send_call[send_call.index("--topic") + 1]
        self.assertLessEqual(len(topic_val.encode("utf-8")), 256)

    def test_grid_backend_falls_back_to_drive_when_master_is_v10(self):
        shim, _ = self.make_fake_oxideswarm("v10")
        self.write_message(title="Drive fallback", body="From Drive hub")
        res = self.run_bridge("--backend", "grid", "--oxideswarm-bin", shim, "inbox")
        self.assertEqual(res.returncode, 0, res.stderr.decode("utf-8", "replace"))
        self.assertIn("Drive fallback", res.stdout.decode("utf-8", "replace"))
        self.assertIn("falling back to Drive", res.stderr.decode("utf-8", "replace"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
