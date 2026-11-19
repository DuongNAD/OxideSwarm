#!/usr/bin/env python3
"""Unit and integration tests for benchmarks/campaigns/driver.py (APP-6, design 0094).

Standard library only, cross-platform (POSIX + Windows) against a fake `oxideswarm` wrapper.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import platform
import signal
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CAMPAIGNS_DIR = REPO_ROOT / "benchmarks" / "campaigns"
PILOT_MANIFEST = CAMPAIGNS_DIR / "2026-11-pilot" / "manifest.json"
DRY_RUN_MANIFEST = CAMPAIGNS_DIR / "2026-11-pilot" / "dry_run_manifest.json"
STANDIN_WORKLOAD = CAMPAIGNS_DIR / "2026-11-pilot" / "standin_workload.py"
DRIVER_PY = CAMPAIGNS_DIR / "driver.py"
if str(CAMPAIGNS_DIR) not in sys.path:
    sys.path.insert(0, str(CAMPAIGNS_DIR))

import driver  # noqa: E402


FAKE_CLI_SOURCE = r'''#!/usr/bin/env python3
import json
import os
import sys
import time
import uuid
from pathlib import Path


def _write_atomic(path: Path, text: str) -> None:
    tmp_path = path.parent / f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    tmp_path.write_text(text, encoding="utf-8")
    os.replace(tmp_path, path)


state_file = Path(os.environ["FAKE_OXIDESWARM_STATE"])
log_dir = Path(os.environ["FAKE_OXIDESWARM_LOG_DIR"])
log_dir.mkdir(parents=True, exist_ok=True)
state = json.loads(state_file.read_text(encoding="utf-8"))

args = sys.argv[1:]
subcmd = args[0] if args else ""

ts_ns = time.time_ns()
uid = uuid.uuid4().hex
log_entry = {
    "ts_ns": ts_ns,
    "pid": os.getpid(),
    "subcmd": subcmd,
    "args": args,
    "has_secret": bool(os.environ.get("OXIDESWARM_SECRET_FILE")),
}
_write_atomic(log_dir / f"{ts_ns:020d}-{os.getpid()}-{uid}.json", json.dumps(log_entry))

if subcmd == "workers":
    calls_file = state_file.with_suffix(".workers_calls")
    n = int(calls_file.read_text(encoding="utf-8")) + 1 if calls_file.exists() else 1
    _write_atomic(calls_file, str(n))
    if n > 1 and state.get("late_workers") is not None:
        print(json.dumps(state["late_workers"]))
    else:
        print(json.dumps(state["workers"]))
    raise SystemExit(0)

if subcmd == "doctor":
    print(json.dumps(state["doctor"]))
    raise SystemExit(0)

if subcmd == "cancel":
    task_id = args[1] if len(args) > 1 else ""
    print(json.dumps({"task_id": task_id, "cancelled": True}))
    raise SystemExit(0)

if subcmd == "submit":
    env_vars = {}
    i = 1
    while i < len(args):
        if args[i] == "--env" and i + 1 < len(args):
            k, v = args[i + 1].split("=", 1)
            env_vars[k] = v
            i += 2
        else:
            i += 1
    seed = env_vars.get("ANIMA_SIM_SEED", "unknown")
    counter_path = state_file.with_suffix(".count." + seed)
    attempt = int(counter_path.read_text(encoding="utf-8")) + 1 if counter_path.exists() else 1
    _write_atomic(counter_path, str(attempt))

    plan = state.get("behaviors", {}).get(seed, [{"mode": "verified"}])
    idx = min(attempt - 1, len(plan) - 1)
    behavior = plan[idx]
    mode = behavior["mode"]
    workers_pool = state.get("late_workers") or state["workers"]
    w0 = workers_pool[behavior.get("primary_index", 0)]
    w1 = workers_pool[behavior.get("replica_index", min(1, len(workers_pool) - 1))]
    w0_id = behavior.get("primary_id") or w0["id"]
    w0_name = behavior.get("primary_name") or w0["name"]
    w1_id = behavior.get("replica_id") or w1["id"]
    w1_name = behavior.get("replica_name") or w1["name"]

    if mode == "verified":
        out = {
            "worker_id": w0_id,
            "worker_name": w0_name,
            "task_id": "99999999-8888-7777-6666-555555555555",
            "exit_code": 0,
            "execution_time_ms": behavior.get("execution_time_ms", 250),
            "waited_ms": 300,
            "verify": {
                "status": "verified",
                "replicas": 1,
                "workers": [
                    {"id": w0_id, "name": w0_name, "os": "macos", "arch": "aarch64"},
                    {"id": w1_id, "name": w1_name, "os": "macos", "arch": "aarch64"},
                ],
                "compared": ["out/smoke/verify_checksums.txt"],
                "differing": [],
            },
        }
        print(json.dumps(out))
        raise SystemExit(0)

    if mode == "mismatch":
        out = {
            "worker_id": w0_id,
            "worker_name": w0_name,
            "task_id": "99999999-8888-7777-6666-555555555556",
            "exit_code": 0,
            "execution_time_ms": behavior.get("execution_time_ms", 260),
            "waited_ms": 320,
            "verify": {
                "status": "mismatch",
                "replicas": 1,
                "workers": [
                    {"id": w0_id, "name": w0_name, "os": "macos", "arch": "aarch64"},
                    {"id": w1_id, "name": w1_name, "os": "macos", "arch": "aarch64"},
                ],
                "compared": ["out/smoke/verify_checksums.txt"],
                "differing": [
                    {
                        "path": "out/smoke/verify_checksums.txt",
                        "workers": [
                            {"worker": w0_name, "blake3": "a" * 64},
                            {"worker": w1_name, "blake3": "b" * 64},
                        ],
                    }
                ],
            },
        }
        print(json.dumps(out))
        raise SystemExit(4)

    if mode == "verify_error":
        out = {
            "worker_id": w0_id,
            "worker_name": w0_name,
            "task_id": "99999999-8888-7777-6666-555555555557",
            "exit_code": 0,
            "execution_time_ms": 120,
            "verify": {
                "status": "error",
                "error": behavior.get("error", "only 1 worker online at 192.168.1.44:8088 for " + w0_name),
            },
        }
        if behavior.get("stderr"):
            print(behavior["stderr"], file=sys.stderr, flush=True)
        print(json.dumps(out))
        raise SystemExit(1)

    if mode == "task_exit_4":
        out = {
            "worker_id": w0_id,
            "worker_name": w0_name,
            "task_id": "99999999-8888-7777-6666-555555555558",
            "exit_code": 4,
            "execution_time_ms": 50,
            "error": behavior.get("error", "process exited with status 4 on " + w0_name),
        }
        print(json.dumps(out))
        raise SystemExit(4)

    if mode == "timeout_json":
        tid = behavior.get("task_id", "99999999-8888-7777-6666-555555555559")
        msg = f"Timed out after 10s waiting for task {tid} to complete"
        print(msg, file=sys.stderr, flush=True)
        print(json.dumps({"status": "timeout", "task_id": tid, "error": msg}), flush=True)
        raise SystemExit(1)

    if mode == "sleep":
        tid = behavior.get("task_id", "99999999-8888-7777-6666-555555555560")
        print(f"task_id={tid}", file=sys.stderr, flush=True)
        marker = behavior.get("marker_file")
        if marker:
            _write_atomic(Path(marker), "ready\n")
        time.sleep(float(behavior.get("sleep_secs", 30.0)))
        print(json.dumps({"status": "verified"}))
        raise SystemExit(0)

    if behavior.get("stderr"):
        print(behavior["stderr"], file=sys.stderr, flush=True)
    print(json.dumps({"error": behavior.get("error", "task crashed on " + w0_name)}))
    raise SystemExit(1)

raise SystemExit(2)
'''


def write_fake_oxideswarm(tmpdir: Path) -> Path:
    script = tmpdir / "fake_oxideswarm.py"
    script.write_text(FAKE_CLI_SOURCE, encoding="utf-8")
    if os.name == "nt":
        wrapper = tmpdir / "oxideswarm.cmd"
        wrapper.write_text(f'@"{sys.executable}" "{script}" %*\r\n', encoding="utf-8")
        return wrapper
    wrapper = tmpdir / "oxideswarm"
    wrapper.write_text(f"#!/bin/sh\nexec \"{sys.executable}\" \"{script}\" \"$@\"\n", encoding="utf-8")
    wrapper.chmod(wrapper.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return wrapper


def read_fake_cli_logs(log_dir: Path) -> list[dict]:
    entries: list[tuple[int, str, dict]] = []
    if not log_dir.exists():
        return []
    for path in log_dir.iterdir():
        if not path.name.endswith(".json"):
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        entries.append((int(data.get("ts_ns", 0)), path.name, data))
    entries.sort(key=lambda item: (item[0], item[1]))
    return [item[2] for item in entries]


def wait_for_file(path: Path, timeout_secs: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_secs
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for marker file {path}")


def make_test_manifest(
    manifest_dir: Path,
    *,
    units: list[dict] | None = None,
    runner_sha256: dict[str, str] | None = None,
    defaults_extra: dict | None = None,
    runner_ready: bool = True,
) -> Path:
    defaults = {
        "command": "./run_esel1_experiment",
        "args": [
            "--manifest-dir", "manifests", "--out", "out",
            "--seed", "{seed}",
            "--founders", "{n0}",
        ],
        "inputs": [],
        "outputs": ["out/smoke/**"],
        "env": {"ANIMA_DETERMINISTIC": "1", "ANIMA_USE_GPU": "0"},
        "verify": 1,
        "verify_file": "out/smoke/verify_checksums.txt",
        "verify_any_system": False,
        "require_isolation": "fs",
        "require_tags": [],
        "timeout_secs": 120,
        "mismatch_retries": 1,
    }
    if defaults_extra:
        defaults.update(defaults_extra)
    doc = {
        "schema_version": 1,
        "campaign_id": "test-pilot",
        "workload": {
            "name": "anima-engine-esel1",
            "binary_name": "run_esel1_experiment",
            "commit": "7a825cc56135df83d5a513ecf59fe3b2e9f1dd92",
            "deterministic": True,
            "gpu": False,
            "runner_ready": runner_ready,
            "blocker": "runner not updated yet" if not runner_ready else None,
            "runner_sha256": runner_sha256 or {"macos/aarch64": "0" * 64},
        },
        "defaults": defaults,
        "units": units
        or [
            {"id": "u001", "seed": 100001, "params": {"n0": 30}, "env": {"ANIMA_SIM_SEED": "100001"}},
            {"id": "u002", "seed": 100002, "params": {"n0": 30}, "env": {"ANIMA_SIM_SEED": "100002"}},
            {"id": "u003", "seed": 100003, "params": {"n0": 30}, "env": {"ANIMA_SIM_SEED": "100003"}},
        ],
    }
    path = manifest_dir / "manifest.json"
    path.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    return path


class CampaignDriverTests(unittest.TestCase):
    def test_pilot_manifest_has_200_valid_trial_units(self) -> None:
        manifest = driver.load_manifest(PILOT_MANIFEST)
        self.assertEqual(manifest["campaign_id"], "2026-11-pilot")
        self.assertEqual(len(manifest["units"]), 200)
        workload = manifest["workload"]
        self.assertTrue(workload["runner_ready"])
        self.assertEqual(workload["commit"], "963f436e68c7197bace3e9e7bf86bd8cd6a6601e")
        self.assertEqual(workload["branch"], "feat/ecological-selection")
        self.assertNotIn("blocker", workload)
        self.assertEqual(
            set(workload["runner_sha256"]), {"macos/aarch64", "windows/x86_64"}
        )
        ids = [u["id"] for u in manifest["units"]]
        self.assertEqual(ids[0], "u001")
        self.assertEqual(ids[-1], "u200")
        self.assertEqual(len(set(ids)), 200)
        # E-SEL1 confirmation seeds 760001, 761001, ..., 771001 are refused by the runner.
        confirmation_seeds = {760001 + 1000 * i for i in range(12)}
        for u in manifest["units"]:
            self.assertEqual(u["env"]["ANIMA_DETERMINISTIC"], "1")
            self.assertEqual(u["env"]["ANIMA_USE_GPU"], "0")
            # `--seed` and ANIMA_SIM_SEED must agree, or the runner exits 2.
            self.assertEqual(u["env"]["ANIMA_SIM_SEED"], str(u["seed"]))
            self.assertNotIn(u["seed"], confirmation_seeds)
            self.assertEqual(u["verify"], 1)
            self.assertEqual(u["verify_file"], "out/trial/verify_checksums.txt")
            self.assertEqual(u["outputs"], ["out/trial/**"])
            self.assertEqual(u["inputs"], [])
            self.assertFalse(u["verify_any_system"])
            self.assertEqual(u["require_isolation"], "fs")
            p = u["params"]
            self.assertNotEqual(p["live.food_cap_abundant"], p["live.food_cap_scarce"])
            self.assertEqual(
                u["args"],
                [
                    "--trial", "--out", "out",
                    "--seed", str(u["seed"]),
                    "--founders", str(p["live.founders"]),
                    "--food-cap-abundant", str(p["live.food_cap_abundant"]),
                    "--food-cap-scarce", str(p["live.food_cap_scarce"]),
                    "--duration-ticks", str(p["duration_ticks"]),
                ],
            )

    def test_pilot_manifest_dry_run_per_system_and_placeholder_hash(self) -> None:
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = driver.main([
                "--manifest", str(PILOT_MANIFEST),
                "--runner-system", "windows/x86_64",
                "--shard", "2/2",
                "--dry-run",
            ])
        self.assertEqual(rc, 0)
        out = buf.getvalue()
        self.assertNotIn("workload blocker", out)
        self.assertIn("selected=100", out)
        self.assertIn("--verify 1 --verify-file out/trial/verify_checksums.txt", out)
        self.assertIn("--os windows --arch x86_64", out)
        self.assertIn("--output out/trial/**", out)
        self.assertIn(
            "--command ./run_esel1_experiment -- --trial --out out --seed 100001 --founders 60",
            out,
        )
        self.assertNotIn("--manifest-dir", out)
        self.assertNotIn("--input", out)

        # The organiser fills runner_sha256 from their own builds: a `<fill-before-run>` placeholder,
        # and later any binary other than those builds, is refused.
        with tempfile.TemporaryDirectory() as d:
            runner_bin = Path(d) / "run_esel1_experiment"
            runner_bin.write_bytes(b"fake-runner-bytes")
            err = io.StringIO()
            with redirect_stdout(io.StringIO()), redirect_stderr(err):
                rc_placeholder = driver.main([
                    "--manifest", str(PILOT_MANIFEST),
                    "--runner", str(runner_bin),
                    "--runner-system", "macos/aarch64",
                    "--dry-run",
                ])
            self.assertEqual(rc_placeholder, 1)
            self.assertRegex(err.getvalue(), r"<fill-before-run>|SHA-256 mismatch")

    def test_standin_workload_and_dry_run_manifest(self) -> None:
        dry_manifest = driver.load_manifest(DRY_RUN_MANIFEST)
        self.assertEqual(dry_manifest["campaign_id"], "2026-11-pilot-dry-run")
        self.assertTrue(dry_manifest["workload"]["runner_ready"])
        self.assertEqual(len(dry_manifest["units"]), 12)
        self.assertEqual(dry_manifest["units"][0]["verify_file"], "out/smoke/verify_checksums.txt")

        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            for run_name in ("r1", "r2"):
                out_dir = tmp / run_name
                r = subprocess.run(
                    [
                        sys.executable,
                        str(STANDIN_WORKLOAD),
                        "--seed", "100001",
                        "--founders", "30",
                        "--food-cap-abundant", "50",
                        "--food-cap-scarce", "13",
                        "--duration-ticks", "18000",
                        "--work-units", "1000",
                        "--out", str(out_dir),
                    ],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                self.assertIn("verify_checksums.txt", r.stdout)

            r3_out = tmp / "r3"
            subprocess.run(
                [
                    sys.executable,
                    str(STANDIN_WORKLOAD),
                    "--seed", "100002",
                    "--work-units", "1000",
                    "--out", str(r3_out),
                ],
                capture_output=True,
                text=True,
                check=True,
            )

            v1 = (tmp / "r1" / "smoke" / "verify_checksums.txt").read_bytes()
            v2 = (tmp / "r2" / "smoke" / "verify_checksums.txt").read_bytes()
            v3 = (r3_out / "smoke" / "verify_checksums.txt").read_bytes()
            self.assertEqual(v1, v2)
            self.assertNotEqual(v1, v3)
            self.assertIn(b"100001,control,", v1)
            self.assertTrue((tmp / "r1" / "smoke" / "runs.csv").exists())

        # A blocked manifest passes `--dry-run` with a notice and refuses a live run without override.
        with tempfile.TemporaryDirectory() as d:
            blocked_manifest = make_test_manifest(Path(d), runner_ready=False)
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc_dry = driver.main(["--manifest", str(blocked_manifest), "--dry-run"])
            self.assertEqual(rc_dry, 0)
            self.assertIn("workload blocker", buf.getvalue())

            rc_live = driver.main(["--manifest", str(blocked_manifest), "--record", str(Path(d) / "r.json")])
            self.assertEqual(rc_live, 1)

    def test_secret_inputs_and_directory_walk_are_refused(self) -> None:
        bad_specs = [
            ".env",
            "/some/path/.env",
            "sub/.env.production",
            "local.txt:.env",
            "keys/cluster.secret",
            "C:\\work\\.env",
            "invite.oxs:invite.oxs",
            "foo/.env/bar",
        ]
        for spec in bad_specs:
            self.assertTrue(driver.is_forbidden_input_spec(spec), f"should refuse {spec}")
        self.assertFalse(driver.is_forbidden_input_spec("manifests:manifests"))
        self.assertFalse(driver.is_forbidden_input_spec("C:\\work\\env.json:env.json"))

        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            p = make_test_manifest(tmp, defaults_extra={"inputs": [".env"]})
            with self.assertRaises(ValueError):
                driver.load_manifest(p)

            for bad_name in ("nested/.env", "sub/.env.local", "sub/cluster.secret", "sub/invite.oxs"):
                inp_dir = tmp / ("dir_" + bad_name.replace("/", "_").replace(".", "_"))
                target = inp_dir / bad_name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text("SECRET=1\n", encoding="utf-8")
                with self.assertRaises(ValueError, msg=f"should refuse directory containing {bad_name}"):
                    driver.validate_no_secret_inputs([f"{inp_dir}:manifests"], "test")

            if os.name != "nt":
                real_secret = tmp / ".env"
                real_secret.write_text("TOPSECRET=1\n", encoding="utf-8")
                link_dir = tmp / "dir_symlink"
                link_dir.mkdir()
                (link_dir / "harmless_name.txt").symlink_to(real_secret)
                with self.assertRaises(ValueError):
                    driver.validate_no_secret_inputs([f"{link_dir}:manifests"], "test")

            clean_dir = tmp / "clean_manifests"
            clean_dir.mkdir()
            (clean_dir / "esel1-preregistration.json").write_text("{}\n", encoding="utf-8")
            driver.validate_no_secret_inputs([f"{clean_dir}:manifests"], "test")

    def test_runner_hash_check_and_placeholder_refusal(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            runner_bin = tmp / "run_esel1_experiment"
            runner_bin.write_bytes(b"fake-runner-bytes")
            digest = hashlib.sha256(b"fake-runner-bytes").hexdigest()

            workload_ok = {"binary_name": "run_esel1_experiment", "runner_sha256": {"macos/aarch64": digest}}
            spec, got = driver.verify_runner_hash(workload_ok, runner_bin, "macos/aarch64")
            self.assertEqual(got, digest)
            self.assertTrue(spec.endswith(":run_esel1_experiment"))

            # A Windows worker starts only `.exe`/`.com`/`.bat`/`.cmd` files (design 0025) and
            # resolves `./run_esel1_experiment` to `run_esel1_experiment.exe`.
            workload_win = {"binary_name": "run_esel1_experiment", "runner_sha256": {"windows/x86_64": digest}}
            spec_win, _ = driver.verify_runner_hash(workload_win, runner_bin, "win/amd64")
            self.assertTrue(spec_win.endswith(":run_esel1_experiment.exe"), spec_win)
            workload_win_exe = {"binary_name": "run_esel1_experiment.EXE", "runner_sha256": {"windows/x86_64": digest}}
            spec_win_exe, _ = driver.verify_runner_hash(workload_win_exe, runner_bin, "windows/x86_64")
            self.assertTrue(spec_win_exe.endswith(":run_esel1_experiment.EXE"), spec_win_exe)

            with self.assertRaises(ValueError):
                driver.verify_runner_hash(
                    {"binary_name": "run_esel1_experiment", "runner_sha256": {"macos/aarch64": "<fill-before-run>"}},
                    runner_bin,
                    "macos/aarch64",
                )
            with self.assertRaises(ValueError):
                driver.verify_runner_hash(
                    {"binary_name": "run_esel1_experiment", "runner_sha256": {"macos/aarch64": "f" * 64}},
                    runner_bin,
                    "macos/aarch64",
                )

    def test_runner_system_pins_os_and_arch_and_shards(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            units = [
                {"id": f"u{i:03d}", "seed": 300000 + i, "params": {"n0": 30}}
                for i in range(1, 5)
            ]
            manifest_path = make_test_manifest(tmp, units=units)
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = driver.main([
                    "--manifest", str(manifest_path),
                    "--runner-system", "darwin/arm64",
                    "--shard", "1/2",
                    "--dry-run",
                ])
            self.assertEqual(rc, 0)
            out = buf.getvalue()
            self.assertIn("system_pin=macos/aarch64", out)
            self.assertIn("--os macos --arch aarch64", out)
            self.assertIn("selected=2", out)

            s1 = driver.slice_shard(units, 1, 2)
            s2 = driver.slice_shard(units, 2, 2)
            self.assertEqual([u["id"] for u in s1], ["u001", "u002"])
            self.assertEqual([u["id"] for u in s2], ["u003", "u004"])

            conflict_manifest = make_test_manifest(
                tmp,
                units=[{"id": "u001", "seed": 1, "os": "windows"}],
            )
            rc_conflict = driver.main([
                "--manifest", str(conflict_manifest),
                "--runner-system", "macos/aarch64",
                "--dry-run",
            ])
            self.assertEqual(rc_conflict, 1)

    def test_task_exit_4_without_mismatch_is_failed_not_mismatch(self) -> None:
        behaviors = {"100001": [{"mode": "task_exit_4"}]}
        units = [{"id": "u001", "seed": 100001, "params": {"n0": 30}}]
        rc, record, invocations, _ = self._run_with_fake_cli(behaviors, ["--concurrency", "1"], units=units)
        self.assertEqual(rc, 1)
        self.assertEqual(record["summary"]["failed"], 1)
        self.assertEqual(record["summary"]["mismatch_persisted"], 0)
        self.assertEqual(record["summary"]["total_mismatch_attempts"], 0)
        self.assertEqual(record["units"][0]["status"], "failed")
        self.assertEqual(len(record["units"][0]["attempts"]), 1)
        self.assertEqual(record["units"][0]["attempts"][0]["task_exit_code"], 4)
        submit_calls = [inv for inv in invocations if inv["subcmd"] == "submit"]
        self.assertEqual(len(submit_calls), 1)

    def _run_with_fake_cli(self, behaviors: dict, driver_args: list[str], units: list[dict] | None = None):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            wrapper = write_fake_oxideswarm(tmp)
            manifest_path = make_test_manifest(tmp, units=units)
            participants_path = tmp / "participants.json"
            participants_path.write_text(
                json.dumps({
                    "alice-macbook-pro": "P1",
                    "bob-ryzen-box": "P2",
                    "charlie-late-laptop": "P3",
                }),
                encoding="utf-8",
            )
            secret_file = tmp / "run.secret"
            secret_file.write_text("not-a-real-secret\n", encoding="utf-8")
            hostname = platform.node() or "testhost"
            state = {
                "workers": [
                    {
                        "name": "alice-macbook-pro",
                        "id": "11111111-2222-3333-4444-555555555551",
                        "cpu_cores": 10,
                        "ram_mb": 16384,
                        "has_gpu": False,
                        "link": "TCP/LAN",
                        "tags": [
                            "id:11111111-2222-3333-4444-555555555551",
                            "os:macos",
                            "arch:aarch64",
                            "slots:4",
                            "iso:fs",
                            "link:TCP/LAN",
                        ],
                    },
                    {
                        "name": "bob-ryzen-box",
                        "id": "11111111-2222-3333-4444-555555555552",
                        "cpu_cores": 8,
                        "ram_mb": 32768,
                        "has_gpu": False,
                        "link": "P2P",
                        "tags": [
                            "id:11111111-2222-3333-4444-555555555552",
                            "os:macos",
                            "arch:aarch64",
                            "slots:4",
                            "iso:fs",
                            "link:P2P",
                        ],
                    },
                ],
                "doctor": {
                    "master": "192.168.1.44:8088",
                    "workers": [
                        {"name": "alice-macbook-pro", "id": "11111111-2222-3333-4444-555555555551", "os": "macos", "arch": "aarch64", "cores": 10, "ram_mb": 16384, "isolation": "fs", "link": "TCP/LAN", "link_kind": "tcp", "link_rtt_ms": 1.2},
                        {"name": "bob-ryzen-box", "id": "11111111-2222-3333-4444-555555555552", "os": "macos", "arch": "aarch64", "cores": 8, "ram_mb": 32768, "isolation": "fs", "link": "P2P", "link_kind": "p2p", "link_rtt_ms": 22.5},
                    ],
                    "checks": [
                        {"id": "C1", "name": "Connect", "worker": None, "outcome": "pass", "detail": f"connected from {hostname} to 192.168.1.44:8088 using oxs1-invite:supersecret.0123456789abcdef"},
                        {"id": "C7", "name": "Link", "worker": "bob-ryzen-box", "outcome": "pass", "detail": "bob-ryzen-box via relay"},
                    ],
                },
                "behaviors": behaviors,
            }
            state_path = tmp / "fake_state.json"
            state_path.write_text(json.dumps(state), encoding="utf-8")
            log_dir = tmp / "fake_logs"
            log_dir.mkdir()
            record_path = tmp / "record.json"

            old_state = os.environ.get("FAKE_OXIDESWARM_STATE")
            old_log_dir = os.environ.get("FAKE_OXIDESWARM_LOG_DIR")
            os.environ["FAKE_OXIDESWARM_STATE"] = str(state_path)
            os.environ["FAKE_OXIDESWARM_LOG_DIR"] = str(log_dir)
            try:
                base_args = [
                    "--manifest", str(manifest_path),
                    "--bin", str(wrapper),
                    "--master", "192.168.1.44:8088",
                    "--runner-system", "macos/aarch64",
                    "--secret-file", str(secret_file),
                    "--participants", str(participants_path),
                    "--record", str(record_path),
                    "--concurrency", "2",
                ]
                buf = io.StringIO()
                with redirect_stdout(buf):
                    rc = driver.main(base_args + driver_args)
                record = json.loads(record_path.read_text(encoding="utf-8"))
                invocations = read_fake_cli_logs(log_dir)
                return rc, record, invocations, base_args
            finally:
                for k, prev in (("FAKE_OXIDESWARM_STATE", old_state), ("FAKE_OXIDESWARM_LOG_DIR", old_log_dir)):
                    if prev is None:
                        os.environ.pop(k, None)
                    else:
                        os.environ[k] = prev

    def test_verified_run_and_privacy_scrubbing(self) -> None:
        rc, record, invocations, _ = self._run_with_fake_cli({}, [])
        self.assertEqual(rc, 0)
        self.assertEqual(record["summary"]["verified"], 3)
        self.assertEqual(record["summary"]["total_attempts"], 3)
        self.assertEqual(record["per_participant"]["P1"]["primary_tasks"], 3)
        self.assertEqual(record["per_participant"]["P1"]["verify_replicas"], 0)
        self.assertEqual(record["per_participant"]["P2"]["verify_replicas"], 3)
        self.assertEqual(record["per_participant"]["P2"]["verified_units"], 3)

        submit_calls = [inv for inv in invocations if inv["subcmd"] == "submit"]
        self.assertEqual(len(submit_calls), 3)
        for call in submit_calls:
            self.assertTrue(call["has_secret"])
            self.assertIn("--verify", call["args"])
            self.assertIn("--verify-file", call["args"])
            self.assertIn("out/smoke/verify_checksums.txt", call["args"])
            self.assertIn("--os", call["args"])
            self.assertIn("--arch", call["args"])
            self.assertIn("--json", call["args"])

        serialized = json.dumps(record).lower()
        hostname = (platform.node() or "").lower()
        forbidden = [
            "alice-macbook-pro",
            "bob-ryzen-box",
            "11111111-2222-3333-4444-555555555551",
            "11111111-2222-3333-4444-555555555552",
            "192.168.1.44",
            "oxs1-invite",
            "supersecret",
        ]
        if len(hostname) >= 3:
            forbidden.append(hostname)
        for secret in forbidden:
            self.assertNotIn(secret, serialized)

    def test_mismatch_reruns_once_and_records_both_attempts(self) -> None:
        behaviors = {
            "100001": [{"mode": "mismatch"}, {"mode": "verified"}],
            "100002": [{"mode": "mismatch"}, {"mode": "mismatch"}],
            "100003": [{"mode": "verify_error"}],
        }
        rc, record, _, _ = self._run_with_fake_cli(behaviors, ["--concurrency", "1"])
        self.assertEqual(rc, 4)
        self.assertEqual(record["summary"]["verified_on_rerun"], 1)
        self.assertEqual(record["summary"]["mismatch_persisted"], 1)
        self.assertEqual(record["summary"]["verify_error"], 1)
        self.assertEqual(record["summary"]["pending"], 1)
        self.assertEqual(record["summary"]["total_mismatch_attempts"], 3)
        self.assertEqual(record["summary"]["units_needing_investigation"], 3)

        by_id = {u["id"]: u for u in record["units"]}
        self.assertEqual(by_id["u001"]["status"], "verified_on_rerun")
        self.assertEqual(len(by_id["u001"]["attempts"]), 2)
        self.assertEqual(by_id["u002"]["status"], "mismatch_persisted")
        serialized = json.dumps(record).lower()
        self.assertNotIn("192.168.1.44", serialized)
        self.assertNotIn("alice-macbook-pro", serialized)

    def test_late_joiner_refresh_unknown_worker_and_master_hint_scrubbing(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            wrapper = write_fake_oxideswarm(tmp)
            manifest_path = make_test_manifest(
                tmp,
                units=[
                    {"id": "u001", "seed": 400001, "params": {"n0": 30}},
                    {"id": "u002", "seed": 400002, "params": {"n0": 30}},
                ],
            )
            participants_path = tmp / "participants.json"
            participants_path.write_text(
                json.dumps({"charlie-late-laptop": "P3", "alice-macbook-pro": "P1"}),
                encoding="utf-8",
            )
            state = {
                "workers": [
                    {
                        "name": "alice-macbook-pro",
                        "id": "22222222-2222-2222-2222-222222222221",
                        "cpu_cores": 8,
                        "ram_mb": 16384,
                        "has_gpu": False,
                        "tags": ["participant:P1", "os:macos", "arch:aarch64"],
                    }
                ],
                "late_workers": [
                    {
                        "name": "alice-macbook-pro",
                        "id": "22222222-2222-2222-2222-222222222221",
                        "cpu_cores": 8,
                        "ram_mb": 16384,
                        "has_gpu": False,
                        "tags": ["participant:P1", "os:macos", "arch:aarch64"],
                    },
                    {
                        "name": "charlie-late-laptop",
                        "id": "22222222-2222-2222-2222-222222222223",
                        "cpu_cores": 6,
                        "ram_mb": 8192,
                        "has_gpu": False,
                        "tags": ["participant:P3", "os:macos", "arch:aarch64"],
                    },
                ],
                "doctor": {"master": "secret-master.home.arpa:8088", "workers": [], "checks": []},
                "behaviors": {
                    "400001": [{
                        "mode": "verify_error",
                        "primary_index": 0,
                        "error": "replica on worker charlie-late-laptop failed: exit code 1",
                        "stderr": (
                            "Hint: if the Master at secret-master.home.arpa:8088 requires a cluster secret, pass --secret-file PATH\n"
                            "Verification failed: replica on worker charlie-late-laptop failed"
                        ),
                    }],
                    "400002": [{
                        "mode": "verify_error",
                        "primary_index": 0,
                        "error": "replica on worker ephemeral-unlisted-box failed: connection closed",
                        "stderr": "Note: worker ephemeral-unlisted-box that ran the first copy does not report os",
                    }],
                },
            }
            state_path = tmp / "state.json"
            state_path.write_text(json.dumps(state), encoding="utf-8")
            log_dir = tmp / "logs"
            log_dir.mkdir()
            record_path = tmp / "record.json"

            old_state = os.environ.get("FAKE_OXIDESWARM_STATE")
            old_log_dir = os.environ.get("FAKE_OXIDESWARM_LOG_DIR")
            os.environ["FAKE_OXIDESWARM_STATE"] = str(state_path)
            os.environ["FAKE_OXIDESWARM_LOG_DIR"] = str(log_dir)
            try:
                with redirect_stdout(io.StringIO()):
                    rc = driver.main([
                        "--manifest", str(manifest_path),
                        "--bin", str(wrapper),
                        "--master", "secret-master.home.arpa:8088",
                        "--relay-url", "https://relay.secret-example.net:8443",
                        "--participants", str(participants_path),
                        "--record", str(record_path),
                        "--concurrency", "1",
                    ])
                self.assertEqual(rc, 1)
                rec = json.loads(record_path.read_text(encoding="utf-8"))
                serialized = json.dumps(rec)
                self.assertNotIn("charlie-late-laptop", serialized)
                self.assertNotIn("ephemeral-unlisted-box", serialized)
                self.assertNotIn("secret-master.home.arpa", serialized)
                self.assertNotIn("relay.secret-example.net", serialized)
                self.assertNotIn("Hint:", serialized)
                u1_err = rec["units"][0]["attempts"][0]["verify"]["error"]
                self.assertIn("P3-w1", u1_err)
                self.assertEqual(rec["units"][1]["attempts"][0]["verify"]["error"], "replica on worker <worker> failed: connection closed")
                worker_calls = [inv for inv in read_fake_cli_logs(log_dir) if inv["subcmd"] == "workers"]
                self.assertGreaterEqual(len(worker_calls), 2)
            finally:
                for k, prev in (("FAKE_OXIDESWARM_STATE", old_state), ("FAKE_OXIDESWARM_LOG_DIR", old_log_dir)):
                    if prev is None:
                        os.environ.pop(k, None)
                    else:
                        os.environ[k] = prev

    def test_timeout_records_failed_and_cancels_task(self) -> None:
        # Both CLI JSON timeout and subprocess.TimeoutExpired kill path.
        task_uuid = "99999999-8888-7777-6666-555555555599"
        behaviors = {
            "500001": [{"mode": "timeout_json", "task_id": task_uuid}],
            # Keep 30.0 (6x the 5 s watchdog): on Windows oxideswarm.cmd, proc.kill() ends only cmd.exe while the Python child holds pipes open until sleep ends.
            "500002": [{"mode": "sleep", "sleep_secs": 30.0, "task_id": "99999999-8888-7777-6666-555555555598"}],
        }
        units = [
            {"id": "u001", "seed": 500001, "params": {"n0": 30}, "timeout_secs": 10},
            {"id": "u002", "seed": 500002, "params": {"n0": 30}, "timeout_secs": 1},
        ]
        rc, record, invocations, _ = self._run_with_fake_cli(
            behaviors,
            ["--concurrency", "1", "--submit-timeout-secs", "5"],
            units=units,
        )
        self.assertEqual(rc, 1)
        self.assertEqual(record["summary"]["failed"], 2)
        by_id = {u["id"]: u for u in record["units"]}
        u1 = by_id["u001"]["attempts"][0]
        u2 = by_id["u002"]["attempts"][0]
        self.assertEqual(u1["cli_exit_code"], 1, f"u001={u1!r}")
        self.assertTrue(u1.get("cancelled_on_master"), f"u001={u1!r}")
        self.assertTrue(u2.get("cancelled_on_master"), f"u002={u2!r}")
        self.assertEqual(u2["cli_exit_code"], 124, f"u002={u2!r}")
        self.assertIn("timed out", u2.get("error") or "", f"u002={u2!r}")
        serialized = json.dumps(record).lower()
        self.assertNotIn(task_uuid, serialized)
        cancel_calls = [inv for inv in invocations if inv["subcmd"] == "cancel"]
        self.assertEqual(len(cancel_calls), 2)

    def test_interrupt_records_interrupted_cancels_and_resumes(self) -> None:
        # Cross-platform signal exit code classification + resume retention check.
        self.assertEqual(driver.classify_attempt(-2, None, 1), "interrupted")
        self.assertEqual(driver.classify_attempt(130, None, 1), "interrupted")
        self.assertEqual(driver.classify_attempt(1, None, 1, interrupted=True), "interrupted")

        if os.name == "nt":
            return

        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            wrapper = write_fake_oxideswarm(tmp)
            marker = tmp / "u002_started.marker"
            units = [
                {"id": "u001", "seed": 600001, "params": {"n0": 30}},
                {"id": "u002", "seed": 600002, "params": {"n0": 30}},
                {"id": "u003", "seed": 600003, "params": {"n0": 30}},
            ]
            manifest_path = make_test_manifest(tmp, units=units)
            interrupt_tid = "99999999-8888-7777-6666-555555555602"
            state = {
                "workers": [
                    {"name": "w1", "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", "cpu_cores": 4, "ram_mb": 4096, "has_gpu": False, "tags": ["participant:P1", "os:macos", "arch:aarch64"]},
                    {"name": "w2", "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2", "cpu_cores": 4, "ram_mb": 4096, "has_gpu": False, "tags": ["participant:P2", "os:macos", "arch:aarch64"]},
                ],
                "doctor": {"workers": [], "checks": []},
                "behaviors": {
                    "600001": [{"mode": "verified"}],
                    "600002": [
                        {"mode": "sleep", "marker_file": str(marker), "sleep_secs": 600.0, "task_id": interrupt_tid},
                        {"mode": "verified"},
                    ],
                    "600003": [{"mode": "verified"}],
                },
            }
            state_path = tmp / "state.json"
            state_path.write_text(json.dumps(state), encoding="utf-8")
            log_dir = tmp / "logs"
            log_dir.mkdir()
            record_path = tmp / "record.json"

            env = dict(os.environ)
            env["FAKE_OXIDESWARM_STATE"] = str(state_path)
            env["FAKE_OXIDESWARM_LOG_DIR"] = str(log_dir)

            proc = subprocess.Popen(
                [
                    sys.executable,
                    str(DRIVER_PY),
                    "--manifest", str(manifest_path),
                    "--bin", str(wrapper),
                    "--record", str(record_path),
                    "--concurrency", "1",
                    "--interrupt-grace-secs", "0",
                ],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                wait_for_file(marker, timeout_secs=60.0)
                proc.send_signal(signal.SIGINT)
                stdout, stderr = proc.communicate(timeout=60.0)
            except Exception:
                proc.kill()
                proc.communicate()
                raise

            self.assertEqual(proc.returncode, 130, f"stderr={stderr}")
            rec1 = json.loads(record_path.read_text(encoding="utf-8"))
            self.assertTrue(rec1["interrupted"])
            self.assertEqual(rec1["summary"]["verified"], 1)
            self.assertEqual(rec1["summary"]["interrupted"], 1)
            self.assertGreaterEqual(rec1["summary"]["pending"], 2)
            by_id1 = {u["id"]: u for u in rec1["units"]}
            self.assertEqual(by_id1["u002"]["status"], "interrupted")
            self.assertNotIn(interrupt_tid, json.dumps(rec1))

            invs1 = read_fake_cli_logs(log_dir)
            cancel_calls = [inv for inv in invs1 if inv["subcmd"] == "cancel"]
            self.assertTrue(any(interrupt_tid in inv["args"] for inv in cancel_calls))

            # Plain `--resume` (without `--retry-failed`) MUST resubmit u002 and u003, skipping u001.
            old_state = os.environ.get("FAKE_OXIDESWARM_STATE")
            old_log_dir = os.environ.get("FAKE_OXIDESWARM_LOG_DIR")
            os.environ["FAKE_OXIDESWARM_STATE"] = str(state_path)
            os.environ["FAKE_OXIDESWARM_LOG_DIR"] = str(log_dir)
            try:
                with redirect_stdout(io.StringIO()):
                    rc2 = driver.main([
                        "--manifest", str(manifest_path),
                        "--bin", str(wrapper),
                        "--record", str(record_path),
                        "--concurrency", "1",
                        "--resume",
                    ])
                self.assertEqual(rc2, 0)
                rec2 = json.loads(record_path.read_text(encoding="utf-8"))
                self.assertFalse(rec2["interrupted"])
                self.assertEqual(rec2["summary"]["verified"], 3)
                self.assertEqual(rec2["summary"]["interrupted"], 0)
                self.assertEqual(rec2["summary"]["pending"], 0)

                submitted_seeds = []
                for entry in read_fake_cli_logs(log_dir):
                    if entry["subcmd"] == "submit":
                        for i, arg in enumerate(entry["args"]):
                            if arg == "--env" and entry["args"][i + 1].startswith("ANIMA_SIM_SEED="):
                                submitted_seeds.append(entry["args"][i + 1].split("=", 1)[1])
                self.assertEqual(submitted_seeds, ["600001", "600002", "600002", "600003"])
            finally:
                for k, prev in (("FAKE_OXIDESWARM_STATE", old_state), ("FAKE_OXIDESWARM_LOG_DIR", old_log_dir)):
                    if prev is None:
                        os.environ.pop(k, None)
                    else:
                        os.environ[k] = prev

    def test_resume_skips_completed_units_without_resubmitting(self) -> None:
        units = [
            {"id": f"u{i:03d}", "seed": 200000 + i, "params": {"n0": 30}, "env": {"ANIMA_SIM_SEED": str(200000 + i)}}
            for i in range(1, 5)
        ]
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            wrapper = write_fake_oxideswarm(tmp)
            manifest_path = make_test_manifest(tmp, units=units)
            state = {
                "workers": [
                    {"name": "w1", "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", "cpu_cores": 4, "ram_mb": 4096, "has_gpu": False, "tags": ["participant:P1", "os:macos", "arch:aarch64"]},
                    {"name": "w2", "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2", "cpu_cores": 4, "ram_mb": 4096, "has_gpu": False, "tags": ["participant:P2", "os:macos", "arch:aarch64"]},
                ],
                "doctor": {"workers": [], "checks": []},
                "behaviors": {},
            }
            state_path = tmp / "fake_state.json"
            state_path.write_text(json.dumps(state), encoding="utf-8")
            log_dir = tmp / "fake_logs"
            log_dir.mkdir()
            record_path = tmp / "record.json"

            old_state = os.environ.get("FAKE_OXIDESWARM_STATE")
            old_log_dir = os.environ.get("FAKE_OXIDESWARM_LOG_DIR")
            os.environ["FAKE_OXIDESWARM_STATE"] = str(state_path)
            os.environ["FAKE_OXIDESWARM_LOG_DIR"] = str(log_dir)
            try:
                common = [
                    "--manifest", str(manifest_path),
                    "--bin", str(wrapper),
                    "--record", str(record_path),
                    "--concurrency", "1",
                ]
                with redirect_stdout(io.StringIO()):
                    rc1 = driver.main(common + ["--limit", "2"])
                self.assertEqual(rc1, 0)
                rec1 = json.loads(record_path.read_text(encoding="utf-8"))
                self.assertEqual(rec1["summary"]["verified"], 2)
                self.assertEqual(rec1["summary"]["pending"], 2)

                with redirect_stdout(io.StringIO()):
                    rc2 = driver.main(common + ["--resume"])
                self.assertEqual(rc2, 0)
                rec2 = json.loads(record_path.read_text(encoding="utf-8"))
                self.assertEqual(rec2["summary"]["verified"], 4)
                self.assertEqual(rec2["summary"]["pending"], 0)
                self.assertEqual(len(rec2["resumed_at_utc"]), 1)

                submitted_seeds = []
                for entry in read_fake_cli_logs(log_dir):
                    if entry["subcmd"] == "submit":
                        for i, arg in enumerate(entry["args"]):
                            if arg == "--env" and entry["args"][i + 1].startswith("ANIMA_SIM_SEED="):
                                submitted_seeds.append(entry["args"][i + 1].split("=", 1)[1])
                self.assertEqual(submitted_seeds, ["200001", "200002", "200003", "200004"])
            finally:
                for k, prev in (("FAKE_OXIDESWARM_STATE", old_state), ("FAKE_OXIDESWARM_LOG_DIR", old_log_dir)):
                    if prev is None:
                        os.environ.pop(k, None)
                    else:
                        os.environ[k] = prev


if __name__ == "__main__":
    unittest.main()
