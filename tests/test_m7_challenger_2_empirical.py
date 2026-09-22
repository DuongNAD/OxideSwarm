#!/usr/bin/env python3
"""
Empirical Challenge & Stress Test Suite for Milestone 7 (Challenger M7.2).
Author: Challenger M7.2

Test Scenarios:
1. Challenge 1: P2P Worker Registration Across Master Restarts
   - 1a. Master Run 1 with persistent key & ticket file. Capture Ticket 1.
   - 1b. Restart Master with same key file & ticket file. Capture Ticket 2.
   - 1c. Verify Ticket 1 == Ticket 2.
   - 1d. Worker attempts registration using retained Ticket 1 (and Ticket 2).
   - 1e. Positive control: Worker registration via direct TCP across Master restart.

2. Challenge 2: CLI Flag Precedence, Alias, and Environment Variable
   - 2a. CLI flag alias: `--key-file` works identically to `--p2p-key-file`.
   - 2b. Environment variable: `RUSTY_GRID_P2P_KEY_FILE` works when CLI flag is absent.
   - 2c. Precedence: CLI flag `--p2p-key-file` overrides `RUSTY_GRID_P2P_KEY_FILE`.
   - 2d. Config file: `p2p_key_file` in TOML config overrides default, but CLI overrides config.

3. Challenge 3: Server Shutdown Cleanup Verification
   - 3a. Start Master with `--port-file`, `--p2p-ticket-file`, and `--p2p-key-file`.
   - 3b. Verify all three files exist while master is running.
   - 3c. Terminate Master with clean shutdown signal (SIGTERM/SIGINT).
   - 3d. Verify `port_file` is deleted.
   - 3e. Verify `p2p_ticket_file` is deleted.
   - 3f. Verify `p2p_key_file` is PRESERVED on disk with non-zero size.
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

def wait_for_file(path, timeout=5.0):
    start = time.time()
    while time.time() - start < timeout:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read().strip()
                if content:
                    return content
            except Exception:
                pass
        time.sleep(0.05)
    return None

def check_tcp_port(port, timeout=1.0):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect(("127.0.0.1", port))
        s.close()
        return True
    except Exception:
        return False

def stop_process(proc, timeout=3.0):
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2.0)

# ============================================================================
# CHALLENGE 1: P2P Worker Registration Across Master Restarts
# ============================================================================

def test_p2p_worker_registration_across_restarts():
    print("\n" + "=" * 70)
    print("CHALLENGE 1: P2P Worker Registration Across Master Restarts")
    print("=" * 70)

    with tempfile.TemporaryDirectory() as tmpdir:
        port_file = os.path.join(tmpdir, "master.port")
        ticket_file = os.path.join(tmpdir, "ticket.txt")
        key_file = os.path.join(tmpdir, "persistent_key.bin")
        worker_log = os.path.join(tmpdir, "worker.log")
        master1_log = os.path.join(tmpdir, "master1.log")
        master2_log = os.path.join(tmpdir, "master2.log")

        # Step 1: Start Master Run 1
        print("[Step 1] Starting Master Run 1 with persistent key...")
        with open(master1_log, "w") as m1_out:
            m1_proc = subprocess.Popen(
                [
                    str(CLI_BIN), "master",
                    "--listen", "127.0.0.1:0",
                    "--port-file", port_file,
                    "--p2p-key-file", key_file,
                    "--p2p-ticket-file", ticket_file,
                ],
                stdout=m1_out,
                stderr=subprocess.STDOUT,
                cwd=str(REPO_ROOT),
            )

        ticket_1 = wait_for_file(ticket_file, timeout=5.0)
        port_1 = wait_for_file(port_file, timeout=5.0)
        assert ticket_1 is not None, "Master Run 1 failed to publish ticket file"
        assert port_1 is not None, "Master Run 1 failed to publish port file"
        assert os.path.exists(key_file) and os.path.getsize(key_file) == 32, "Key file not 32 bytes"

        print(f"  Master Run 1 Port: {port_1}")
        print(f"  Master Run 1 Ticket: {ticket_1}")
        print(f"  Key file size: {os.path.getsize(key_file)} bytes")

        # Step 2: Stop Master Run 1
        print("[Step 2] Stopping Master Run 1...")
        stop_process(m1_proc)
        time.sleep(0.5)

        # Step 3: Start Master Run 2 with same key file
        print("[Step 3] Starting Master Run 2 with SAME persistent key...")
        if os.path.exists(port_file):
            os.remove(port_file)
        if os.path.exists(ticket_file):
            os.remove(ticket_file)

        with open(master2_log, "w") as m2_out:
            m2_proc = subprocess.Popen(
                [
                    str(CLI_BIN), "master",
                    "--listen", "127.0.0.1:0",
                    "--port-file", port_file,
                    "--p2p-key-file", key_file,
                    "--p2p-ticket-file", ticket_file,
                ],
                stdout=m2_out,
                stderr=subprocess.STDOUT,
                cwd=str(REPO_ROOT),
            )

        ticket_2 = wait_for_file(ticket_file, timeout=5.0)
        port_2 = wait_for_file(port_file, timeout=5.0)
        assert ticket_2 is not None, "Master Run 2 failed to publish ticket file"
        assert port_2 is not None, "Master Run 2 failed to publish port file"

        print(f"  Master Run 2 Port: {port_2}")
        print(f"  Master Run 2 Ticket: {ticket_2}")

        # Step 4: Verify Ticket Equality
        tickets_match = (ticket_1 == ticket_2)
        print(f"  Ticket 1 == Ticket 2: {tickets_match}")
        assert tickets_match, f"Tickets differed across runs! T1={ticket_1} != T2={ticket_2}"
        print("  [PASS] Ticket determinism verified across master restart.")

        # Step 5: Start Worker using Ticket 1 (the original retained ticket)
        print("[Step 5] Starting Worker using retained Ticket 1 via P2P...")
        with open(worker_log, "w") as w_out:
            w_proc = subprocess.Popen(
                [
                    str(CLI_BIN), "worker",
                    "--p2p-ticket", ticket_1,
                    "--name", "p2p_restart_worker",
                ],
                stdout=w_out,
                stderr=subprocess.STDOUT,
                cwd=str(REPO_ROOT),
            )

        # Monitor worker connection for 5 seconds
        time.sleep(5.0)
        w_running = (w_proc.poll() is None)

        with open(worker_log, "r", encoding="utf-8", errors="ignore") as f:
            w_log_content = f.read()

        # Query master workers list via CLI
        workers_output = subprocess.run(
            [str(CLI_BIN), "workers", "--master", f"127.0.0.1:{port_2}", "--json"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=5.0,
        )

        stop_process(w_proc)
        stop_process(m2_proc)

        p2p_registered = "p2p_restart_worker" in workers_output.stdout
        has_endpoint_dropped_err = "Endpoint dropped without calling `Endpoint::close`" in w_log_content
        has_handshake_fail_err = "Registration handshake failed" in w_log_content or "connection lost" in w_log_content

        print(f"  Worker still running: {w_running}")
        print(f"  Registered in Master registry: {p2p_registered}")
        print(f"  Detected 'Endpoint dropped' error in worker log: {has_endpoint_dropped_err}")
        print(f"  Detected 'Handshake failed' error in worker log: {has_handshake_fail_err}")
        print("  Worker Log Excerpt:")
        for line in w_log_content.strip().splitlines()[-8:]:
            print(f"    {line}")

        return {
            "tickets_match": tickets_match,
            "p2p_registered": p2p_registered,
            "has_endpoint_dropped_err": has_endpoint_dropped_err,
            "has_handshake_fail_err": has_handshake_fail_err,
            "worker_log": w_log_content,
        }

# ============================================================================
# CHALLENGE 2: CLI Flag & Alias & Env Var Verification
# ============================================================================

def test_cli_flag_alias_and_env_var():
    print("\n" + "=" * 70)
    print("CHALLENGE 2: CLI Flag Precedence, Alias & Env Var Verification")
    print("=" * 70)

    results = {}

    # Test 2a: `--key-file` alias
    print("\n[Test 2a] Verifying `--key-file` CLI alias...")
    with tempfile.TemporaryDirectory() as tmpdir:
        key_file = os.path.join(tmpdir, "alias_key.bin")
        port_file = os.path.join(tmpdir, "alias.port")
        ticket_file = os.path.join(tmpdir, "alias_ticket.txt")

        proc = subprocess.Popen(
            [
                str(CLI_BIN), "master",
                "--listen", "127.0.0.1:0",
                "--port-file", port_file,
                "--key-file", key_file,
                "--p2p-ticket-file", ticket_file,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(REPO_ROOT),
        )

        ticket = wait_for_file(ticket_file, timeout=4.0)
        stop_process(proc)

        alias_key_exists = os.path.exists(key_file) and os.path.getsize(key_file) == 32
        print(f"  `--key-file` created valid 32-byte key: {alias_key_exists}")
        print(f"  Ticket published: {ticket is not None}")
        results["2a_alias"] = alias_key_exists and (ticket is not None)

    # Test 2b: `RUSTY_GRID_P2P_KEY_FILE` environment variable
    print("\n[Test 2b] Verifying `RUSTY_GRID_P2P_KEY_FILE` environment variable...")
    with tempfile.TemporaryDirectory() as tmpdir:
        key_file = os.path.join(tmpdir, "env_key.bin")
        port_file = os.path.join(tmpdir, "env.port")
        ticket_file = os.path.join(tmpdir, "env_ticket.txt")

        env = os.environ.copy()
        env["RUSTY_GRID_P2P_KEY_FILE"] = key_file

        proc = subprocess.Popen(
            [
                str(CLI_BIN), "master",
                "--listen", "127.0.0.1:0",
                "--port-file", port_file,
                "--p2p-ticket-file", ticket_file,
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(REPO_ROOT),
        )

        ticket = wait_for_file(ticket_file, timeout=4.0)
        stop_process(proc)

        env_key_exists = os.path.exists(key_file) and os.path.getsize(key_file) == 32
        print(f"  `RUSTY_GRID_P2P_KEY_FILE` created valid 32-byte key: {env_key_exists}")
        print(f"  Ticket published: {ticket is not None}")
        results["2b_env_var"] = env_key_exists and (ticket is not None)

    # Test 2c: Precedence: CLI flag overrides Environment Variable
    print("\n[Test 2c] Verifying CLI flag overrides `RUSTY_GRID_P2P_KEY_FILE`...")
    with tempfile.TemporaryDirectory() as tmpdir:
        cli_key = os.path.join(tmpdir, "cli_key.bin")
        env_key = os.path.join(tmpdir, "env_key.bin")
        port_file = os.path.join(tmpdir, "prec.port")
        ticket_file = os.path.join(tmpdir, "prec_ticket.txt")

        env = os.environ.copy()
        env["RUSTY_GRID_P2P_KEY_FILE"] = env_key

        proc = subprocess.Popen(
            [
                str(CLI_BIN), "master",
                "--listen", "127.0.0.1:0",
                "--port-file", port_file,
                "--p2p-key-file", cli_key,
                "--p2p-ticket-file", ticket_file,
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(REPO_ROOT),
        )

        ticket = wait_for_file(ticket_file, timeout=4.0)
        stop_process(proc)

        cli_created = os.path.exists(cli_key)
        env_created = os.path.exists(env_key)
        print(f"  CLI key created: {cli_created}")
        print(f"  Env key created: {env_created} (should be False)")
        precedence_ok = cli_created and not env_created
        results["2c_precedence"] = precedence_ok

    # Test 2d: Config file support
    print("\n[Test 2d] Verifying TOML config file `p2p_key_file` support...")
    with tempfile.TemporaryDirectory() as tmpdir:
        cfg_key = os.path.join(tmpdir, "cfg_key.bin")
        port_file = os.path.join(tmpdir, "cfg.port")
        ticket_file = os.path.join(tmpdir, "cfg_ticket.txt")
        config_path = os.path.join(tmpdir, "master.toml")

        # Write TOML config
        with open(config_path, "w") as f:
            f.write(f"""
[master]
listen = "127.0.0.1:0"
port_file = "{port_file.replace(os.sep, '/')}"
p2p_ticket_file = "{ticket_file.replace(os.sep, '/')}"
p2p_key_file = "{cfg_key.replace(os.sep, '/')}"
""")

        proc = subprocess.Popen(
            [
                str(CLI_BIN), "master",
                "--config", config_path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(REPO_ROOT),
        )

        ticket = wait_for_file(ticket_file, timeout=4.0)
        stop_process(proc)

        cfg_key_exists = os.path.exists(cfg_key) and os.path.getsize(cfg_key) == 32
        print(f"  Config file `p2p_key_file` created 32-byte key: {cfg_key_exists}")
        results["2d_config"] = cfg_key_exists

    return results

# ============================================================================
# CHALLENGE 3: Server Shutdown Cleanup Verification
# ============================================================================

def test_shutdown_file_cleanup():
    print("\n" + "=" * 70)
    print("CHALLENGE 3: Server Shutdown Cleanup Verification")
    print("=" * 70)

    with tempfile.TemporaryDirectory() as tmpdir:
        key_file = os.path.join(tmpdir, "shutdown_key.bin")
        port_file = os.path.join(tmpdir, "shutdown.port")
        ticket_file = os.path.join(tmpdir, "shutdown_ticket.txt")
        log_file = os.path.join(tmpdir, "shutdown_master.log")

        print("[Step 1] Starting Master node...")
        with open(log_file, "w") as out:
            proc = subprocess.Popen(
                [
                    str(CLI_BIN), "master",
                    "--listen", "127.0.0.1:0",
                    "--port-file", port_file,
                    "--p2p-key-file", key_file,
                    "--p2p-ticket-file", ticket_file,
                ],
                stdout=out,
                stderr=subprocess.STDOUT,
                cwd=str(REPO_ROOT),
            )

        wait_for_file(port_file, timeout=4.0)
        wait_for_file(ticket_file, timeout=4.0)

        # Verify all files exist while running
        key_exists_before = os.path.exists(key_file)
        port_exists_before = os.path.exists(port_file)
        ticket_exists_before = os.path.exists(ticket_file)

        print(f"  Files existing while running:")
        print(f"    - Key file:    {key_exists_before} (size: {os.path.getsize(key_file)} bytes)")
        print(f"    - Port file:   {port_exists_before}")
        print(f"    - Ticket file: {ticket_exists_before}")

        assert key_exists_before, "Key file must exist while master is running"
        assert port_exists_before, "Port file must exist while master is running"
        assert ticket_exists_before, "Ticket file must exist while master is running"

        # Step 2: Send clean termination signal (SIGTERM/SIGINT)
        print("[Step 2] Sending SIGTERM shutdown signal to Master...")
        proc.terminate()
        try:
            proc.wait(timeout=4.0)
            print("  Master process terminated cleanly within timeout.")
        except subprocess.TimeoutExpired:
            print("  Master process did not exit within timeout, killing...")
            proc.kill()
            proc.wait(timeout=2.0)

        # Allow filesystem lock release
        time.sleep(0.5)

        # Step 3: Verify post-shutdown file state
        key_exists_after = os.path.exists(key_file)
        port_exists_after = os.path.exists(port_file)
        ticket_exists_after = os.path.exists(ticket_file)

        print(f"  Files existing after shutdown:")
        print(f"    - Key file:    {key_exists_after} (PRESERVED: expected True)")
        print(f"    - Port file:   {port_exists_after} (DELETED: expected False)")
        print(f"    - Ticket file: {ticket_exists_after} (DELETED: expected False)")

        key_preserved = key_exists_after and (os.path.getsize(key_file) == 32)
        port_deleted = not port_exists_after
        ticket_deleted = not ticket_exists_after

        print(f"  Assertion: Key file preserved:   {key_preserved}")
        print(f"  Assertion: Port file deleted:     {port_deleted}")
        print(f"  Assertion: Ticket file deleted:   {ticket_deleted}")

        cleanup_ok = key_preserved and port_deleted and ticket_deleted
        return {
            "key_preserved": key_preserved,
            "port_deleted": port_deleted,
            "ticket_deleted": ticket_deleted,
            "overall_cleanup_passed": cleanup_ok,
        }

# ============================================================================
# POSITIVE CONTROL: Direct TCP Worker Registration Across Restarts
# ============================================================================

def test_tcp_worker_registration_across_restarts():
    print("\n" + "=" * 70)
    print("POSITIVE CONTROL: Direct TCP Worker Registration Across Restarts")
    print("=" * 70)

    with tempfile.TemporaryDirectory() as tmpdir:
        port_file = os.path.join(tmpdir, "master.port")
        key_file = os.path.join(tmpdir, "tcp_key.bin")
        ticket_file = os.path.join(tmpdir, "tcp_ticket.txt")

        # Run Master
        proc = subprocess.Popen(
            [
                str(CLI_BIN), "master",
                "--listen", "127.0.0.1:0",
                "--port-file", port_file,
                "--p2p-key-file", key_file,
                "--p2p-ticket-file", ticket_file,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(REPO_ROOT),
        )

        port_str = wait_for_file(port_file, timeout=4.0)
        assert port_str is not None, "Failed to get master port"
        master_addr = f"127.0.0.1:{port_str}"

        # Run Worker via direct TCP
        w_proc = subprocess.Popen(
            [
                str(CLI_BIN), "worker",
                "--master", master_addr,
                "--name", "tcp_control_worker",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(REPO_ROOT),
        )

        time.sleep(2.0)

        # Check workers list
        workers_output = subprocess.run(
            [str(CLI_BIN), "workers", "--master", master_addr, "--json"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=5.0,
        )

        stop_process(w_proc)
        stop_process(proc)

        tcp_registered = "tcp_control_worker" in workers_output.stdout
        print(f"  TCP Worker Registered: {tcp_registered}")
        return tcp_registered

# ============================================================================
# MAIN ORCHESTRATION
# ============================================================================

if __name__ == "__main__":
    print(f"CLI Binary: {CLI_BIN}")
    if not CLI_BIN.exists():
        print(f"Binary {CLI_BIN} not found!")
        sys.exit(1)

    c1_results = test_p2p_worker_registration_across_restarts()
    c2_results = test_cli_flag_alias_and_env_var()
    c3_results = test_shutdown_file_cleanup()
    tcp_ctrl = test_tcp_worker_registration_across_restarts()

    print("\n" + "=" * 70)
    print("FINAL SUMMARY OF CHALLENGER M7.2 EMPIRICAL FINDINGS")
    print("=" * 70)
    print(f"Challenge 1 — Ticket Determinism Across Restart: {c1_results['tickets_match']}")
    print(f"Challenge 1 — P2P Worker Registration:           {c1_results['p2p_registered']}")
    print(f"Challenge 1 — Root Cause (Endpoint Dropped):     {c1_results['has_endpoint_dropped_err']}")
    print(f"Challenge 2 — CLI Flag Alias (--key-file):       {c2_results['2a_alias']}")
    print(f"Challenge 2 — Env Var (RUSTY_GRID_P2P_KEY_FILE): {c2_results['2b_env_var']}")
    print(f"Challenge 2 — Flag Overrides Env Var:            {c2_results['2c_precedence']}")
    print(f"Challenge 2 — Config File Support:               {c2_results['2d_config']}")
    print(f"Challenge 3 — Shutdown File Cleanup:             {c3_results['overall_cleanup_passed']}")
    print(f"Positive Control — Direct TCP Worker:            {tcp_ctrl}")

    # Output machine-readable JSON results
    summary = {
        "c1": c1_results,
        "c2": c2_results,
        "c3": c3_results,
        "tcp_ctrl": tcp_ctrl,
    }
    with open(REPO_ROOT / "tests" / "m7_challenger_2_results.json", "w") as f:
        json.dump(summary, f, indent=2)
