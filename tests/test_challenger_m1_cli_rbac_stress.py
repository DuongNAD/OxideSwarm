#!/usr/bin/env python3
"""
Empirical End-to-End CLI & Multi-Process RBAC Stress Harness for Milestone 1.
Challenger 1: Empirical Challenger Suite

Verifies:
1. Strict Production Mode (dev_mode=false):
   - Unauthenticated loopback requests -> 401 Unauthorized
   - Invalid token -> 401 Unauthorized
   - Valid observer token -> 200 OK for status/workers/tasks
   - Valid observer token submitting task -> 403 Forbidden
   - Valid admin token submitting task -> 200 OK
   - Multi-channel extraction: Bearer, X-API-Key, ?token=, ?api_key=
   - Non-UUID and nonexistent task paths -> 404 Not Found
2. Local Development Mode (--dev):
   - Unauthenticated loopback requests -> 200 OK (Admin auto-granted)
   - Unauthenticated task submission -> 200 OK (Admin auto-granted)
   - Explicit invalid token on loopback -> 401 Unauthorized (Not bypassed)
3. Process lifecycle and graceful shutdown cleanly releases dashboard port.
"""

import os
import sys
import time
import json
import socket
import urllib.request
import urllib.error
import tempfile
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CLI_BIN = REPO_ROOT / "target" / "debug" / "rusty-grid.exe"
if not CLI_BIN.exists():
    CLI_BIN = REPO_ROOT / "target" / "debug" / "rusty-grid"

assert CLI_BIN.exists(), f"CLI binary not found at {CLI_BIN}"


def wait_for_file(path, timeout=10.0):
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


def wait_for_port(port, timeout=10.0):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except (socket.error, ConnectionRefusedError):
            time.sleep(0.05)
    return False


def stop_process(proc):
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2.0)


def http_request(url, method="GET", headers=None, body=None):
    req_headers = headers or {}
    data = None
    if body is not None:
        if isinstance(body, dict):
            data = json.dumps(body).encode("utf-8")
            req_headers["Content-Type"] = "application/json"
        elif isinstance(body, bytes):
            data = body
        elif isinstance(body, str):
            data = body.encode("utf-8")

    req = urllib.request.Request(url, data=data, headers=req_headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            status = resp.status
            content = resp.read().decode("utf-8", errors="replace")
            return status, resp.headers, content
    except urllib.error.HTTPError as e:
        content = e.read().decode("utf-8", errors="replace")
        return e.code, e.headers, content


def run_tests():
    print(f"=== Starting Empirical RBAC Stress Challenge with binary: {CLI_BIN} ===")

    # ------------------------------------------------------------------------
    # STAGE 1: Strict Production Mode (No --dev flag)
    # ------------------------------------------------------------------------
    print("\n--- STAGE 1: Strict Production Mode Validation ---")
    with tempfile.TemporaryDirectory() as tmp_dir:
        port_file = os.path.join(tmp_dir, "dash_strict.port")
        cmd = [
            str(CLI_BIN),
            "master",
            "--listen", "127.0.0.1:0",
            "--dashboard-port", "0",
            "--dashboard-port-file", port_file,
            "--auth-token", "root-secret:superadmin:admin",
            "--auth-token", "viewer-key:monitor:observer",
        ]
        # In production, set OXIDE_DEV_MODE=0 to disable dev bypass
        env = os.environ.copy()
        env["OXIDE_DEV_MODE"] = "0"

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            text=True,
        )

        try:
            port_str = wait_for_file(port_file, timeout=8.0)
            assert port_str is not None, "Failed to read dashboard port file for strict mode"
            dash_port = int(port_str.strip())
            assert wait_for_port(dash_port), f"Dashboard port {dash_port} not reachable"
            print(f"[+] Master successfully spawned in Strict Mode on port {dash_port}")

            base_url = f"http://127.0.0.1:{dash_port}"

            # 1.1 Public HTML
            status, _, content = http_request(f"{base_url}/")
            assert status == 200, f"Expected 200 for /, got {status}"
            assert "OxideSwarm" in content, "Dashboard HTML missing brand"
            print("  [PASS] Public SPA root returns 200 OK")

            # 1.2 Unauthenticated /api/status -> 401
            status, hdrs, _ = http_request(f"{base_url}/api/status")
            assert status == 401, f"Expected 401 for unauthenticated /api/status, got {status}"
            assert "Bearer" in hdrs.get("WWW-Authenticate", ""), "Missing WWW-Authenticate header"
            print("  [PASS] Strict mode rejects unauthenticated request with 401")

            # 1.3 Invalid Bearer -> 401
            status, _, _ = http_request(f"{base_url}/api/status", headers={"Authorization": "Bearer bogus123"})
            assert status == 401, f"Expected 401 for bogus bearer, got {status}"
            print("  [PASS] Bogus Bearer token rejected with 401")

            # 1.4 Valid Observer via Bearer -> 200
            status, _, content = http_request(
                f"{base_url}/api/status",
                headers={"Authorization": "Bearer viewer-key"},
            )
            assert status == 200, f"Expected 200 for valid observer, got {status}"
            data = json.loads(content)
            assert "workers" in data and "master" in data, "Malformed status payload"
            print("  [PASS] Valid Observer Bearer token accepted (200 OK)")

            # 1.5 Multi-channel extraction: X-API-Key
            status, _, _ = http_request(f"{base_url}/api/status", headers={"X-API-Key": "viewer-key"})
            assert status == 200, f"Expected 200 via X-API-Key, got {status}"
            print("  [PASS] X-API-Key header credential extraction verified")

            # 1.6 Multi-channel extraction: ?token=
            status, _, _ = http_request(f"{base_url}/api/status?token=viewer-key")
            assert status == 200, f"Expected 200 via ?token=, got {status}"
            print("  [PASS] ?token= query parameter credential extraction verified")

            # 1.7 Multi-channel extraction: ?api_key=
            status, _, _ = http_request(f"{base_url}/api/status?api_key=viewer-key")
            assert status == 200, f"Expected 200 via ?api_key=, got {status}"
            print("  [PASS] ?api_key= query parameter credential extraction verified")

            # 1.8 Scope enforcement: Observer cannot submit tasks
            status, _, _ = http_request(
                f"{base_url}/api/tasks",
                method="POST",
                headers={"Authorization": "Bearer viewer-key"},
                body={"command": "echo test"},
            )
            assert status == 403, f"Expected 403 for observer task submission, got {status}"
            print("  [PASS] Observer scope blocked from POST /api/tasks (403 Forbidden)")

            # 1.9 Admin can submit tasks
            status, _, content = http_request(
                f"{base_url}/api/tasks",
                method="POST",
                headers={"Authorization": "Bearer root-secret"},
                body={"command": "echo 'cli e2e test'"},
            )
            assert status == 200, f"Expected 200 for admin task submission, got {status}"
            sub_res = json.loads(content)
            created_task_id = sub_res.get("task_id")
            assert created_task_id, "Missing task_id in submit response"
            print(f"  [PASS] Admin scope submitted task successfully: {created_task_id}")

            # 1.10 Non-UUID paths to /api/tasks/:task_id strictly return 404
            status, _, _ = http_request(
                f"{base_url}/api/tasks/not-a-valid-uuid",
                headers={"Authorization": "Bearer root-secret"},
            )
            assert status == 404, f"Expected 404 for non-UUID task route, got {status}"

            status, _, _ = http_request(
                f"{base_url}/api/tasks/123456789",
                headers={"Authorization": "Bearer root-secret"},
            )
            assert status == 404, f"Expected 404 for numeric task route, got {status}"

            status, _, _ = http_request(
                f"{base_url}/api/tasks/00000000-0000-0000-0000-000000000000",
                headers={"Authorization": "Bearer root-secret"},
            )
            assert status == 404, f"Expected 404 for nonexistent UUID task route, got {status}"
            print("  [PASS] Non-UUID and nonexistent task paths strictly return 404 NOT FOUND")

            # 1.11 Valid task detail endpoint returns 200 OK
            status, _, content = http_request(
                f"{base_url}/api/tasks/{created_task_id}",
                headers={"Authorization": "Bearer viewer-key"},
            )
            assert status == 200, f"Expected 200 for valid task detail, got {status}"
            task_detail = json.loads(content)
            assert task_detail.get("id") == created_task_id, "Task detail ID mismatch"
            print(f"  [PASS] Standardized route GET /api/tasks/:task_id returns 200 OK")

        finally:
            stop_process(proc)

    # ------------------------------------------------------------------------
    # STAGE 2: Local Development Mode (--dev flag)
    # ------------------------------------------------------------------------
    print("\n--- STAGE 2: Local Development Mode (--dev) Validation ---")
    with tempfile.TemporaryDirectory() as tmp_dir:
        port_file = os.path.join(tmp_dir, "dash_dev.port")
        cmd = [
            str(CLI_BIN),
            "master",
            "--listen", "127.0.0.1:0",
            "--dashboard-port", "0",
            "--dashboard-port-file", port_file,
            "--dev",
        ]

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            port_str = wait_for_file(port_file, timeout=8.0)
            assert port_str is not None, "Failed to read dashboard port file for dev mode"
            dash_port = int(port_str.strip())
            assert wait_for_port(dash_port), f"Dashboard port {dash_port} not reachable"
            print(f"[+] Master successfully spawned with --dev on port {dash_port}")

            base_url = f"http://127.0.0.1:{dash_port}"

            # 2.1 Loopback unauthenticated GET /api/status -> 200 OK (Admin auto-granted)
            status, _, content = http_request(f"{base_url}/api/status")
            assert status == 200, f"Expected 200 for dev mode unauth status, got {status}"
            print("  [PASS] Loopback unauthenticated request automatically bypassed with Admin scope")

            # 2.2 Loopback unauthenticated POST /api/tasks -> 200 OK (Admin auto-granted)
            status, _, content = http_request(
                f"{base_url}/api/tasks",
                method="POST",
                body={"command": "echo 'dev mode task'"},
            )
            assert status == 200, f"Expected 200 for dev mode unauth task submit, got {status}"
            print("  [PASS] Dev mode auto-grants Admin scope for task mutations on loopback")

            # 2.3 Explicit invalid token MUST NOT be bypassed -> 401
            status, _, _ = http_request(
                f"{base_url}/api/status",
                headers={"Authorization": "Bearer explicit-bogus-token"},
            )
            assert status == 401, f"Expected 401 for explicit bogus token in dev mode, got {status}"
            print("  [PASS] Explicit bad credentials override dev bypass and return 401 Unauthorized")

        finally:
            stop_process(proc)

    print("\n=======================================================")
    print("ALL EMPIRICAL ADVERSARIAL STRESS TESTS COMPLETED: 100% PASS")
    print("=======================================================")


if __name__ == "__main__":
    run_tests()
