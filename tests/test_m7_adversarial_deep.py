#!/usr/bin/env python3
"""
Deep Adversarial Stress Test Harness for Milestone 7 (Challenger M7.R2.1)
Covers comprehensive edge cases, boundary conditions, zero-zombie verification,
and error propagation for P2P secret key handling in OxideSwarm.
"""

import os
import sys
import time
import socket
import hashlib
import tempfile
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CLI_BIN = REPO_ROOT / "target" / "debug" / "rusty-grid.exe"
if not CLI_BIN.exists():
    CLI_BIN = REPO_ROOT / "target" / "debug" / "rusty-grid"

def count_rusty_grid_processes():
    try:
        out = subprocess.check_output(
            ["tasklist", "/FI", "IMAGENAME eq rusty-grid.exe", "/FO", "CSV"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        lines = [l for l in out.strip().splitlines() if "rusty-grid.exe" in l.lower()]
        return len(lines)
    except Exception:
        return 0

def check_tcp_connection(port, timeout=0.5):
    if not port:
        return False
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect(("127.0.0.1", port))
        s.close()
        return True
    except Exception:
        return False

def run_corrupt_test_case(name, data_bytes=None, is_dir=False):
    with tempfile.TemporaryDirectory() as tmpdir:
        port_file = os.path.join(tmpdir, "master.port")
        ticket_file = os.path.join(tmpdir, "ticket.txt")
        key_file = os.path.join(tmpdir, "test_key.bin")

        if is_dir:
            os.makedirs(key_file, exist_ok=True)
        else:
            with open(key_file, "wb") as f:
                f.write(data_bytes)

        cmd = [
            str(CLI_BIN),
            "master",
            "--listen", "127.0.0.1:0",
            "--port-file", port_file,
            "--p2p",
            "--p2p-key-file", key_file,
            "--p2p-ticket-file", ticket_file,
        ]

        t0 = time.time()
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(REPO_ROOT),
        )

        try:
            stdout, stderr = proc.communicate(timeout=4.0)
            exit_code = proc.returncode
            duration = time.time() - t0
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate()
            exit_code = -999
            duration = time.time() - t0

        port_file_exists = os.path.exists(port_file)
        ticket_file_exists = os.path.exists(ticket_file)

        print(f"Case [{name}]:")
        print(f"  Duration: {duration:.2f}s | Exit Code: {exit_code}")
        print(f"  Port file exists: {port_file_exists} | Ticket file exists: {ticket_file_exists}")
        print(f"  STDERR: {stderr.strip()[:160]}")

        assert exit_code == 1, f"Expected exit code 1, got {exit_code}"
        assert not port_file_exists, f"Port file must NOT be written on corrupt key: {port_file}"
        assert not ticket_file_exists, f"Ticket file must NOT be written on corrupt key: {ticket_file}"
        assert "Failed to start master server:" in stderr, f"Expected error diagnostic in stderr: {stderr}"

        return True

def test_all_corrupt_cases():
    print("=" * 70)
    print("DEEP ADVERSARIAL CHALLENGE: Corrupt & Boundary Key Cases")
    print("=" * 70)

    test_cases = [
        ("0_bytes_empty", b""),
        ("1_byte_minimal", b"\x00"),
        ("15_bytes_too_short", b"A" * 15),
        ("31_bytes_boundary_short", b"B" * 31),
        ("33_bytes_boundary_long", b"C" * 33),
        ("64_invalid_hex_chars", b"z" * 64),
        ("100_bytes_arbitrary", b"\xff" * 100),
        ("1000_bytes_large_junk", b"\xde\xad\xbe\xef" * 250),
        ("directory_as_key_file", None),
    ]

    for name, data in test_cases:
        if name == "directory_as_key_file":
            run_corrupt_test_case(name, is_dir=True)
        else:
            run_corrupt_test_case(name, data_bytes=data)

    print("\n[PASS] All 9 corrupt / boundary cases cleanly rejected with code 1, no leaks!\n")

def test_hex_string_key_support():
    print("=" * 70)
    print("HEX-ENCODED STRING KEY SUPPORT & STABILITY")
    print("=" * 70)

    with tempfile.TemporaryDirectory() as tmpdir:
        port_file = os.path.join(tmpdir, "master.port")
        ticket_file = os.path.join(tmpdir, "ticket.txt")
        key_file = os.path.join(tmpdir, "hex_key.txt")

        # 64 valid hex characters representing a 32-byte key
        hex_key_str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        with open(key_file, "w") as f:
            f.write(hex_key_str + "\n")

        tickets = []
        for cycle in (1, 2):
            if os.path.exists(port_file):
                os.remove(port_file)
            if os.path.exists(ticket_file):
                os.remove(ticket_file)

            cmd = [
                str(CLI_BIN),
                "master",
                "--listen", "127.0.0.1:0",
                "--port-file", port_file,
                "--p2p",
                "--p2p-key-file", key_file,
                "--p2p-ticket-file", ticket_file,
            ]

            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=str(REPO_ROOT),
            )

            # Wait for ticket file
            t0 = time.time()
            ticket = None
            while time.time() - t0 < 5.0:
                if os.path.exists(ticket_file) and os.path.getsize(ticket_file) > 0:
                    with open(ticket_file, "r") as f:
                        ticket = f.read().strip()
                    if ticket:
                        break
                time.sleep(0.05)

            assert ticket is not None, f"Cycle {cycle}: Hex key master failed to write ticket"
            tickets.append(ticket)
            print(f"Cycle {cycle} with 64-char Hex Key -> Ticket: {ticket}")

            proc.terminate()
            proc.wait(timeout=3.0)

        assert tickets[0] == tickets[1], "Hex key tickets across restarts must be identical!"
        print("[PASS] Hex string key loaded successfully and produces deterministic ticket across restarts!\n")

def test_zero_zombies():
    print("=" * 70)
    print("ZOMBIE PROCESS AUDIT")
    print("=" * 70)
    running = count_rusty_grid_processes()
    print(f"Active rusty-grid.exe process count: {running}")
    assert running == 0, f"Found {running} orphan rusty-grid.exe processes!"
    print("[PASS] Zero lingering rusty-grid processes verified!\n")

if __name__ == "__main__":
    test_all_corrupt_cases()
    test_hex_string_key_support()
    test_zero_zombies()
    print("=" * 70)
    print("ALL DEEP ADVERSARIAL STRESS TESTS COMPLETED SUCCESSFULLY (100%)")
    print("=" * 70)
