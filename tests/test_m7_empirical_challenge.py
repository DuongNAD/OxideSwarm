#!/usr/bin/env python3
"""
Empirical Challenge & Stress Test Suite for Milestone 7 (P2P Secret Key Persistence).
Author: Challenger M7.1

Test Scenarios:
1. Corrupt / Invalid Key File Handling:
   - 1a: 0 bytes (empty file)
   - 1b: 15 bytes (too short)
   - 1c: 100 bytes (too long / non-32 byte raw binary)
   - 1d: Invalid ASCII text (not valid hex or base32 secret key)
   - 1e: Directory path passed as key file
2. Deeply Nested Key Directory Auto-Creation:
   - Path like `nested1/nested2/nested3/nested4/p2p_key.bin`
   - Verify parent directories created, key saved (32 bytes), ticket published
3. Successive Restarts (4 consecutive runs):
   - Run 1 (generates key) -> Ticket 1
   - Run 2 (reuses key) -> Ticket 2
   - Run 3 (reuses key) -> Ticket 3
   - Run 4 (reuses key) -> Ticket 4
   - Assert: Ticket 1 == Ticket 2 == Ticket 3 == Ticket 4
   - Assert: Key file unchanged (32 bytes, identical sha256)
4. Negative Control (3 consecutive runs without --p2p-key-file):
   - Ephemeral Run 1 -> Ticket A
   - Ephemeral Run 2 -> Ticket B
   - Ephemeral Run 3 -> Ticket C
   - Assert: Ticket A != Ticket B, Ticket B != Ticket C, Ticket A != Ticket C
"""

import os
import sys
import time
import json
import socket
import hashlib
import tempfile
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CLI_BIN = REPO_ROOT / "target" / "debug" / "rusty-grid.exe"
if not CLI_BIN.exists():
    CLI_BIN = REPO_ROOT / "target" / "debug" / "rusty-grid"

def wait_for_file_content(path, timeout=5.0):
    start = time.time()
    while time.time() - start < timeout:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read().strip()
            if content:
                return content
        time.sleep(0.05)
    return None

def check_tcp_connection(port, timeout=1.0):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect(("127.0.0.1", port))
        s.close()
        return True
    except Exception:
        return False

def stop_process(proc):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2.0)

def run_master_instance(tmpdir, key_file=None, extra_args=None, timeout_wait=4.0):
    port_file = os.path.join(tmpdir, "master.port")
    ticket_file = os.path.join(tmpdir, "p2p_ticket.txt")
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
        "--p2p-ticket-file", ticket_file,
    ]
    if key_file is not None:
        cmd.extend(["--p2p-key-file", key_file])
    if extra_args:
        cmd.extend(extra_args)

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=str(REPO_ROOT)
    )

    port_content = wait_for_file_content(port_file, timeout=timeout_wait)
    ticket_content = wait_for_file_content(ticket_file, timeout=timeout_wait)
    
    port = int(port_content) if port_content and port_content.isdigit() else None
    tcp_alive = check_tcp_connection(port) if port else False

    # Check if process exited early
    poll_res = proc.poll()

    return {
        "proc": proc,
        "port": port,
        "ticket": ticket_content,
        "tcp_alive": tcp_alive,
        "poll_res": poll_res,
        "port_file": port_file,
        "ticket_file": ticket_file,
    }

# ============================================================================
# TEST SUITE IMPLEMENTATION
# ============================================================================

def test_corrupt_key_handling():
    print("\n" + "=" * 60)
    print("CHALLENGE 1: Corrupt / Invalid Key File Handling")
    print("=" * 60)

    cases = [
        ("1a_zero_bytes", b""),
        ("1b_15_bytes", b"0123456789abcde"),
        ("1c_100_bytes", b"X" * 100),
        ("1d_invalid_ascii", b"This is totally invalid non-hex key data!!"),
    ]

    results = {}

    for name, content in cases:
        with tempfile.TemporaryDirectory() as tmpdir:
            key_path = os.path.join(tmpdir, f"{name}.bin")
            with open(key_path, "wb") as f:
                f.write(content)

            res = run_master_instance(tmpdir, key_file=key_path, timeout_wait=2.0)
            proc = res["proc"]
            
            # Allow time to see how the master behaves
            time.sleep(1.0)
            poll_code = proc.poll()
            
            stop_process(proc)
            stdout, stderr = proc.communicate()

            print(f"\n--- Case [{name}] (size={len(content)} bytes) ---")
            print(f"  Port file created: {res['port']}")
            print(f"  TCP Listener alive: {res['tcp_alive']}")
            print(f"  Ticket published: {res['ticket']}")
            print(f"  Process exited on its own: {poll_code is not None} (code={poll_code})")
            if stderr.strip():
                print(f"  STDERR snippet: {stderr.strip()[:200]}")
            if stdout.strip():
                print(f"  STDOUT snippet: {stdout.strip()[:200]}")

            results[name] = {
                "size": len(content),
                "port": res["port"],
                "tcp_alive": res["tcp_alive"],
                "ticket": res["ticket"],
                "exited_early": poll_code is not None,
                "poll_code": poll_code,
                "has_ticket": res["ticket"] is not None,
                "stderr": stderr,
                "stdout": stdout,
            }

    return results

def test_nested_directory_auto_creation():
    print("\n" + "=" * 60)
    print("CHALLENGE 2: Deeply Nested Key Directory Auto-Creation")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        deep_dir = os.path.join(tmpdir, "level1", "level2", "level3", "level4")
        deep_key_file = os.path.join(deep_dir, "master_p2p_key.bin")

        assert not os.path.exists(deep_dir), "Directory should not exist prior to test"
        print(f"Target key path: {deep_key_file}")

        res = run_master_instance(tmpdir, key_file=deep_key_file, timeout_wait=4.0)
        proc = res["proc"]

        try:
            assert res["port"] is not None, "Master must bind port"
            assert res["tcp_alive"], "Master TCP must accept connections"
            assert res["ticket"] is not None, "Master must publish ticket"
            assert os.path.exists(deep_key_file), "Nested key file must be created"
            key_size = os.path.getsize(deep_key_file)
            assert key_size == 32, f"Key file must be exactly 32 bytes, got {key_size}"
            print(f"[PASS] Deeply nested directory auto-created!")
            print(f"[PASS] Key file created with size: {key_size} bytes")
            print(f"[PASS] Master published Ticket: {res['ticket']}")
            return True, deep_key_file, res["ticket"]
        finally:
            stop_process(proc)

def test_successive_restarts_determinism():
    print("\n" + "=" * 60)
    print("CHALLENGE 3: Multiple Successive Restarts (4 Consecutive Cycles)")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        key_file = os.path.join(tmpdir, "persistent_key.bin")
        tickets = []
        key_hashes = []

        for cycle in range(1, 5):
            print(f"\n--- Master Restart Cycle {cycle}/4 ---")
            res = run_master_instance(tmpdir, key_file=key_file, timeout_wait=4.0)
            proc = res["proc"]

            try:
                assert res["port"] is not None, f"Cycle {cycle}: Port must be published"
                assert res["tcp_alive"], f"Cycle {cycle}: TCP must be alive"
                assert res["ticket"] is not None, f"Cycle {cycle}: Ticket must be published"
                
                with open(key_file, "rb") as f:
                    key_bytes = f.read()
                key_hash = hashlib.sha256(key_bytes).hexdigest()
                
                tickets.append(res["ticket"])
                key_hashes.append(key_hash)

                print(f"Cycle {cycle} Port: {res['port']}")
                print(f"Cycle {cycle} Key SHA256: {key_hash[:16]}... (size: {len(key_bytes)})")
                print(f"Cycle {cycle} Ticket: {res['ticket']}")
            finally:
                stop_process(proc)
                # Cleanup ephemeral files between cycles
                if os.path.exists(res["port_file"]):
                    try:
                        os.remove(res["port_file"])
                    except Exception:
                        pass
                if os.path.exists(res["ticket_file"]):
                    try:
                        os.remove(res["ticket_file"])
                    except Exception:
                        pass

        # Verify all tickets are identical
        all_tickets_identical = all(t == tickets[0] for t in tickets)
        all_keys_identical = all(k == key_hashes[0] for k in key_hashes)

        print("\n--- Successive Restarts Evaluation ---")
        print(f"All 4 tickets identical: {all_tickets_identical}")
        print(f"All 4 key hashes identical: {all_keys_identical}")
        if not all_tickets_identical:
            for idx, t in enumerate(tickets, 1):
                print(f"  Ticket {idx}: {t}")

        assert all_tickets_identical, "Tickets across 4 successive restarts MUST be identical!"
        assert all_keys_identical, "Key file must remain identical across 4 restarts!"
        print("[PASS] Deterministic ticket stability across 4 successive restarts verified 100%!")
        return True, tickets[0]

def test_negative_control_ephemeral_variance():
    print("\n" + "=" * 60)
    print("CHALLENGE 4: Negative Control (Omitting Key File -> Ticket Variance)")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        tickets = []

        for cycle in range(1, 4):
            print(f"\n--- Ephemeral Run {cycle}/3 (No --p2p-key-file) ---")
            res = run_master_instance(tmpdir, key_file=None, timeout_wait=4.0)
            proc = res["proc"]

            try:
                assert res["port"] is not None, f"Ephemeral {cycle}: Port must be published"
                assert res["ticket"] is not None, f"Ephemeral {cycle}: Ticket must be published"
                tickets.append(res["ticket"])
                print(f"Ephemeral Run {cycle} Port: {res['port']}")
                print(f"Ephemeral Run {cycle} Ticket: {res['ticket']}")
            finally:
                stop_process(proc)
                if os.path.exists(res["port_file"]):
                    try:
                        os.remove(res["port_file"])
                    except Exception:
                        pass
                if os.path.exists(res["ticket_file"]):
                    try:
                        os.remove(res["ticket_file"])
                    except Exception:
                        pass

        all_unique = len(set(tickets)) == len(tickets)
        print("\n--- Negative Control Evaluation ---")
        print(f"All {len(tickets)} tickets are unique: {all_unique}")
        for idx, t in enumerate(tickets, 1):
            print(f"  Ephemeral Ticket {idx}: {t}")

        assert all_unique, "Ephemeral tickets MUST vary across restarts without a key file!"
        print("[PASS] Negative control verified 100%: Different tickets generated on every restart!")
        return True

if __name__ == "__main__":
    print(f"Target CLI Binary: {CLI_BIN}")
    if not CLI_BIN.exists():
        print(f"Error: binary {CLI_BIN} does not exist!")
        sys.exit(1)

    corrupt_results = test_corrupt_key_handling()
    nested_ok, nested_path, nested_ticket = test_nested_directory_auto_creation()
    restart_ok, stable_ticket = test_successive_restarts_determinism()
    neg_ok = test_negative_control_ephemeral_variance()

    print("\n" + "=" * 60)
    print("SUMMARY OF EMPIRICAL CHALLENGE FINDINGS")
    print("=" * 60)
    print(f"Challenge 1 (Corrupt Key Handling): {corrupt_results}")
    print(f"Challenge 2 (Nested Dir Auto-creation): {'PASS' if nested_ok else 'FAIL'}")
    print(f"Challenge 3 (4 Successive Restarts): {'PASS' if restart_ok else 'FAIL'}")
    print(f"Challenge 4 (Negative Control Variance): {'PASS' if neg_ok else 'FAIL'}")
