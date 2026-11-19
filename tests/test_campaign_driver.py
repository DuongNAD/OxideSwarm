#!/usr/bin/env python3
"""Unit tests for benchmarks/campaigns/driver.py (APP-6, design 0094).

Standard library only, cross-platform (POSIX + Windows) against a fake `oxideswarm` wrapper.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import platform
import stat
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CAMPAIGNS_DIR = REPO_ROOT / "benchmarks" / "campaigns"
PILOT_MANIFEST = CAMPAIGNS_DIR / "2026-11-pilot" / "manifest.json"
if str(CAMPAIGNS_DIR) not in sys.path:
    sys.path.insert(0, str(CAMPAIGNS_DIR))

import driver  # noqa: E402


FAKE_CLI_SOURCE = r'''#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

state_file = Path(os.environ["FAKE_OXIDESWARM_STATE"])
log_file = Path(os.environ["FAKE_OXIDESWARM_LOG"])
state = json.loads(state_file.read_text(encoding="utf-8"))

args = sys.argv[1:]
subcmd = args[0] if args else ""

with log_file.open("a", encoding="utf-8") as f:
    f.write(json.dumps({"subcmd": subcmd, "args": args, "has_secret": bool(os.environ.get("OXIDESWARM_SECRET_FILE"))}) + "\n")

if subcmd == "workers":
    print(json.dumps(state["workers"]))
    raise SystemExit(0)

if subcmd == "doctor":
    print(json.dumps(state["doctor"]))
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
    counter_path.write_text(str(attempt), encoding="utf-8")

    plan = state.get("behaviors", {}).get(seed, [{"mode": "verified"}])
    idx = min(attempt - 1, len(plan) - 1)
    behavior = plan[idx]
    mode = behavior["mode"]
    w0 = state["workers"][behavior.get("primary_index", 0)]
    w1 = state["workers"][behavior.get("replica_index", min(1, len(state["workers"]) - 1))]
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
                "compared": ["out/smoke/checksums.sha256"],
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
                "compared": ["out/smoke/checksums.sha256"],
                "differing": [
                    {
                        "path": "out/smoke/checksums.sha256",
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
        print(json.dumps(out))
        raise SystemExit(1)

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


def make_test_manifest(
    manifest_dir: Path,
    *,
    units: list[dict] | None = None,
    runner_sha256: dict[str, str] | None = None,
    defaults_extra: dict | None = None,
) -> Path:
    defaults = {
        "command": "./run_esel1_experiment",
        "args": ["--smoke", "--manifest-dir", "manifests", "--out", "out"],
        "inputs": [],
        "outputs": ["out/smoke/**"],
        "env": {"ANIMA_DETERMINISTIC": "1", "ANIMA_USE_GPU": "0"},
        "verify": 1,
        "verify_file": "out/smoke/checksums.sha256",
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
            "runner_sha256": runner_sha256
            or {
                "macos/aarch64": "0" * 64,
            },
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
    def test_pilot_manifest_has_200_valid_units(self) -> None:
        manifest = driver.load_manifest(PILOT_MANIFEST)
        self.assertEqual(manifest["campaign_id"], "2026-11-pilot")
        self.assertEqual(len(manifest["units"]), 200)
        ids = [u["id"] for u in manifest["units"]]
        self.assertEqual(ids[0], "u001")
        self.assertEqual(ids[-1], "u200")
        self.assertEqual(len(set(ids)), 200)
        for u in manifest["units"]:
            self.assertEqual(u["env"]["ANIMA_DETERMINISTIC"], "1")
            self.assertEqual(u["env"]["ANIMA_USE_GPU"], "0")
            self.assertEqual(u["verify"], 1)
            self.assertEqual(u["verify_file"], "out/smoke/checksums.sha256")
            self.assertFalse(u["verify_any_system"])
            self.assertEqual(u["require_isolation"], "fs")

    def test_secret_inputs_are_refused(self) -> None:
        bad_specs = [
            ".env",
            "/some/path/.env",
            "sub/.env.production",
            "local.txt:.env",
            "keys/cluster.secret",
            "C:\\work\\.env",
            "invite.oxs:invite.oxs",
        ]
        for spec in bad_specs:
            self.assertTrue(driver.is_forbidden_input_spec(spec), f"should refuse {spec}")
        self.assertFalse(driver.is_forbidden_input_spec("manifests:manifests"))
        self.assertFalse(driver.is_forbidden_input_spec("C:\\work\\env.json:env.json"))

        with tempfile.TemporaryDirectory() as d:
            p = make_test_manifest(Path(d), defaults_extra={"inputs": [".env"]})
            with self.assertRaises(ValueError):
                driver.load_manifest(p)

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

    def test_dry_run_skips_execution_and_writes_no_record(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            manifest_path = make_test_manifest(tmp)
            record_path = tmp / "results" / "record.json"
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = driver.main([
                    "--manifest", str(manifest_path),
                    "--bin", str(tmp / "nonexistent-oxideswarm"),
                    "--record", str(record_path),
                    "--dry-run",
                ])
            self.assertEqual(rc, 0)
            self.assertFalse(record_path.exists())
            out = buf.getvalue()
            self.assertIn("[dry-run] campaign=test-pilot total=3", out)
            self.assertIn("--verify 1 --verify-file out/smoke/checksums.sha256", out)

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
            log_path = tmp / "fake_log.jsonl"
            record_path = tmp / "record.json"

            old_state = os.environ.get("FAKE_OXIDESWARM_STATE")
            old_log = os.environ.get("FAKE_OXIDESWARM_LOG")
            os.environ["FAKE_OXIDESWARM_STATE"] = str(state_path)
            os.environ["FAKE_OXIDESWARM_LOG"] = str(log_path)
            try:
                base_args = [
                    "--manifest", str(manifest_path),
                    "--bin", str(wrapper),
                    "--master", "192.168.1.44:8088",
                    "--secret-file", str(secret_file),
                    "--participants", str(participants_path),
                    "--record", str(record_path),
                    "--concurrency", "2",
                ]
                buf = io.StringIO()
                with redirect_stdout(buf):
                    rc = driver.main(base_args + driver_args)
                record = json.loads(record_path.read_text(encoding="utf-8"))
                invocations = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
                return rc, record, invocations, base_args
            finally:
                for k, prev in (("FAKE_OXIDESWARM_STATE", old_state), ("FAKE_OXIDESWARM_LOG", old_log)):
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
            self.assertIn("out/smoke/checksums.sha256", call["args"])
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
        # 100001: mismatch -> verified on rerun; 100002: mismatch -> mismatch (persisted);
        # 100003: verify_error -> stays verify_error (with IP/name scrubbed).
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
        self.assertEqual(record["summary"]["total_mismatch_attempts"], 3)
        self.assertEqual(record["summary"]["units_needing_investigation"], 3)

        by_id = {u["id"]: u for u in record["units"]}
        self.assertEqual(by_id["u001"]["status"], "verified_on_rerun")
        self.assertEqual(len(by_id["u001"]["attempts"]), 2)
        self.assertEqual(by_id["u001"]["attempts"][0]["outcome"], "mismatch")
        self.assertEqual(by_id["u001"]["attempts"][1]["outcome"], "verified")

        self.assertEqual(by_id["u002"]["status"], "mismatch_persisted")
        self.assertEqual(len(by_id["u002"]["attempts"]), 2)
        serialized = json.dumps(record).lower()
        self.assertNotIn("192.168.1.44", serialized)
        self.assertNotIn("alice-macbook-pro", serialized)

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
            log_path = tmp / "fake_log.jsonl"
            record_path = tmp / "record.json"

            old_state = os.environ.get("FAKE_OXIDESWARM_STATE")
            old_log = os.environ.get("FAKE_OXIDESWARM_LOG")
            os.environ["FAKE_OXIDESWARM_STATE"] = str(state_path)
            os.environ["FAKE_OXIDESWARM_LOG"] = str(log_path)
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
                for line in log_path.read_text(encoding="utf-8").splitlines():
                    entry = json.loads(line)
                    if entry["subcmd"] == "submit":
                        for i, arg in enumerate(entry["args"]):
                            if arg == "--env" and entry["args"][i + 1].startswith("ANIMA_SIM_SEED="):
                                submitted_seeds.append(entry["args"][i + 1].split("=", 1)[1])
                self.assertEqual(submitted_seeds, ["200001", "200002", "200003", "200004"])
            finally:
                for k, prev in (("FAKE_OXIDESWARM_STATE", old_state), ("FAKE_OXIDESWARM_LOG", old_log)):
                    if prev is None:
                        os.environ.pop(k, None)
                    else:
                        os.environ[k] = prev


if __name__ == "__main__":
    unittest.main()
