#!/usr/bin/env python3
"""Unit and dry-run smoke tests for benchmarks/host-1 scripts."""

import importlib.util
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import unittest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
HOST1_DIR = os.path.join(REPO_ROOT, "benchmarks", "host-1")
SH_PATH = os.path.join(HOST1_DIR, "run_host1.sh")
PS1_PATH = os.path.join(HOST1_DIR, "run_host1.ps1")
SUMMARIZE_PATH = os.path.join(HOST1_DIR, "summarize_host1.py")

SPEC = importlib.util.spec_from_file_location("summarize_host1", SUMMARIZE_PATH)
SUMMARIZE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(SUMMARIZE)


class TestHost1Scripts(unittest.TestCase):
    def test_sh_is_lf_and_bash32_safe(self):
        with open(SH_PATH, "rb") as fh:
            raw = fh.read()
        self.assertNotIn(b"\r\n", raw, "run_host1.sh must use LF endings")
        text = raw.decode("utf-8")
        for forbidden in ("declare -A", "mapfile", "readarray", "|&", ",,}"):
            self.assertNotIn(forbidden, text, f"Forbidden bash 4+ token found: {forbidden}")

        bash = shutil.which("bash")
        if bash:
            proc = subprocess.run([bash, "-n", SH_PATH], capture_output=True, text=True)
            self.assertEqual(proc.returncode, 0, f"bash -n failed: {proc.stderr}")

    def test_ps1_parses_cleanly_on_windows(self):
        if os.name != "nt":
            self.skipTest("PowerShell AST check runs on Windows")
        ps = shutil.which("powershell")
        self.assertIsNotNone(ps, "powershell executable should exist on Windows")
        cmd = (
            "$errors = $null; $tokens = $null; "
            f"[void][System.Management.Automation.Language.Parser]::ParseFile('{PS1_PATH}', [ref]$tokens, [ref]$errors); "
            "if ($errors.Count -gt 0) { $errors | ForEach-Object { Write-Error $_.Message }; exit 1 }"
        )
        proc = subprocess.run([ps, "-NoProfile", "-Command", cmd], capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, f"PowerShell parse error: {proc.stderr}")

    def test_dry_run_emits_no_pii_and_valid_json(self):
        if os.name == "nt":
            ps = shutil.which("powershell")
            self.assertIsNotNone(ps)
            with tempfile.TemporaryDirectory() as tmp:
                out_dir = os.path.join(tmp, "out")
                m06 = os.path.join(tmp, "Qwen3-0.6B-Q8_0.gguf")
                m8 = os.path.join(tmp, "Qwen3-8B-Q4_K_M.gguf")
                with open(m06, "wb") as f1, open(m8, "wb") as f2:
                    f1.write(b"dummy06b")
                    f2.write(b"dummy8b")
                proc = subprocess.run(
                    [
                        ps,
                        "-NoProfile",
                        "-ExecutionPolicy",
                        "Bypass",
                        "-File",
                        PS1_PATH,
                        "-State",
                        "state1",
                        "-Model06B",
                        m06,
                        "-Model8B",
                        m8,
                        "-OutDir",
                        out_dir,
                        "-DryRun",
                    ],
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(proc.returncode, 0, f"ps1 dry-run failed: {proc.stderr}\n{proc.stdout}")
                facts_path = os.path.join(out_dir, "machine_facts.json")
                self.assertTrue(os.path.isfile(facts_path))
                with open(facts_path, "r", encoding="utf-8") as fh:
                    raw_text = fh.read()
                facts = json.loads(raw_text)
                for key in ("cpu_model", "physical_cores", "logical_processors", "ram_mib", "thread_sweep", "planned_commands"):
                    self.assertIn(key, facts)
                forbidden = [
                    socket.gethostname(),
                    os.environ.get("COMPUTERNAME", ""),
                    os.environ.get("USERNAME", ""),
                    os.environ.get("USER", ""),
                ]
                combined = proc.stdout + "\n" + raw_text
                self.assertEqual(SUMMARIZE.find_pii_leaks(combined, forbidden), [])

    def test_sanitize_and_q106_gate_math(self):
        sample = [
            {"model_filename": r"C:\secret_user_dir\Qwen3-0.6B-Q8_0.gguf"},
            {"model_filename": "/home/someuser/models/Qwen3-8B-Q4_K_M.gguf"},
        ]
        SUMMARIZE.sanitize_llama_bench_json(sample)
        self.assertEqual(sample[0]["model_filename"], "Qwen3-0.6B-Q8_0.gguf")
        self.assertEqual(sample[1]["model_filename"], "Qwen3-8B-Q4_K_M.gguf")

        pending = SUMMARIZE.compute_q106_gate([
            {"state": "state1", "backend": "CPU", "tg_ts": 50.0},
            {"state": "state1", "backend": "CUDA", "tg_ts": 100.0},
        ])
        self.assertEqual(pending["status"], "PENDING")
        self.assertIsNone(pending["passed"])

        pass_gpu = SUMMARIZE.compute_q106_gate([
            {"state": "state1", "backend": "CPU", "tg_ts": 50.0},
            {"state": "state3", "backend": "CPU", "tg_ts": 52.0},
            {"state": "state1", "backend": "CUDA", "tg_ts": 100.0},
            {"state": "state3", "backend": "CUDA", "tg_ts": 111.0},
        ])
        self.assertEqual(pass_gpu["status"], "PASS")
        self.assertTrue(pass_gpu["passed"])
        self.assertAlmostEqual(pass_gpu["gpu_gain_pct"], 11.0, places=2)

        fail_both = SUMMARIZE.compute_q106_gate([
            {"state": "state1", "backend": "CPU", "tg_ts": 50.0},
            {"state": "state3", "backend": "CPU", "tg_ts": 53.0},
            {"state": "state1", "backend": "CUDA", "tg_ts": 100.0},
            {"state": "state3", "backend": "CUDA", "tg_ts": 105.0},
        ])
        self.assertEqual(fail_both["status"], "FAIL")
        self.assertFalse(fail_both["passed"])


if __name__ == "__main__":
    unittest.main()
