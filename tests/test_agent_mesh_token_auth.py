#!/usr/bin/env python3
"""
Agent Mesh hub token authentication - Python node against the real Rust hub binary.
File: tests/test_agent_mesh_token_auth.py

Spawns `target/debug/agent-mesh hub --listen 127.0.0.1:0 --token <secret>` and checks, with the
Python node client (scripts/agent_node.py) and raw websockets, that:
1. a node without / with the wrong token is rejected (ERR_UNAUTHORIZED) and never registers;
2. a node with the right token (constructor argument or OXIDE_MESH_TOKEN / --token on the CLI) works;
3. a duplicate node id is NACKed (ERR_DUPLICATE_NODE_ID) and the original keeps working;
4. a forged `from` is NACKed (ERR_SENDER_MISMATCH) and nothing runs on the victim;
5. a WebSocket upgrade carrying a foreign browser Origin is refused (HTTP 403);
6. the REST catalog needs the token.

Run directly (needs `cargo build -p agent_mesh` first): python tests/test_agent_mesh_token_auth.py
"""

import asyncio
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid

try:
    import websockets
except ImportError:
    print("websockets required")
    sys.exit(1)

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
logger = logging.getLogger("TokenAuth")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from agent_node import OxideAgentNode  # noqa: E402

TOKEN = "py-mesh-token-0123456789abcdef"


def find_binary() -> str:
    for name in ("agent-mesh", "agent-mesh.exe"):
        path = os.path.join(PROJECT_ROOT, "target", "debug", name)
        if os.path.exists(path):
            return path
    raise FileNotFoundError("agent-mesh binary not found; run `cargo build -p agent_mesh` first")


def start_hub(token=TOKEN):
    """Starts the Rust hub on an ephemeral loopback port; returns (process, ws_url, http_base)."""
    env = {k: v for k, v in os.environ.items() if k != "OXIDE_MESH_TOKEN"}
    proc = subprocess.Popen(
        [find_binary(), "hub", "--listen", "127.0.0.1:0", "--token", token],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        env=env,
    )
    deadline = time.time() + 15
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            break
        if "running at " in line:
            ws_url = line.split("running at ", 1)[1].strip()
            http_base = "http://" + ws_url.split("://", 1)[1].rsplit("/ws", 1)[0]
            return proc, ws_url, http_base
    proc.kill()
    raise RuntimeError("hub did not report its address")


def http_get(url, bearer=None):
    req = urllib.request.Request(url)
    if bearer:
        req.add_header("Authorization", f"Bearer {bearer}")
    try:
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


async def try_register(url, node_id, token):
    """Connects a node; returns the (error_code, reason) the hub rejected it with, or None."""
    node = OxideAgentNode(url, node_id, "linux", token=token)
    await node.connect()
    listener = asyncio.create_task(node.listen())
    # Any request makes the rejection observable: it is answered with the hub's NACK.
    resp = await node.send_command("nobody", "ping", {}, timeout=2.0)
    await asyncio.sleep(0.1)
    listener.cancel()
    await node.close()
    if node.registration_error is not None:
        assert resp.get("type") == "DeliveryNack", resp
        return node.registration_error
    return None


async def test_missing_and_wrong_token_are_rejected(url, http):
    for token in (None, "wrong-token"):
        rejected = await try_register(url, "py-intruder", token)
        assert rejected is not None and rejected[0] == "ERR_UNAUTHORIZED", f"token={token!r}: {rejected}"
        assert TOKEN not in rejected[1]
    status, body = http_get(f"{http}/api/nodes", bearer=TOKEN)
    assert status == 200 and json.loads(body) == [], f"rejected nodes must not register: {body}"
    print("PASS missing / wrong token rejected, nothing registered")


async def test_right_token_routes_commands(url):
    a = OxideAgentNode(url, "py-alice", "linux", token=TOKEN)
    b = OxideAgentNode(url, "py-bob", "linux", token=TOKEN)
    await a.connect()
    await b.connect()
    la, lb = asyncio.create_task(a.listen()), asyncio.create_task(b.listen())
    try:
        resp = await a.send_command("py-bob", "echo", {"message": "authenticated"}, timeout=3.0)
        assert resp.get("type") == "CommandResponse", resp
        assert resp["stdout"] == "authenticated" and resp["from"] == "py-bob", resp
        assert a.registration_error is None and b.registration_error is None
        print("PASS right token registers and routes")

        # Duplicate id with the right token is still refused, and bob keeps his route.
        dup = await try_register(url, "py-bob", TOKEN)
        assert dup is not None and dup[0] == "ERR_DUPLICATE_NODE_ID", dup
        resp = await a.send_command("py-bob", "echo", {"message": "still bob"}, timeout=3.0)
        assert resp.get("stdout") == "still bob", resp
        print("PASS duplicate node id NACKed, original unaffected")
    finally:
        la.cancel()
        lb.cancel()
        await a.close()
        await b.close()


async def test_forged_from_is_nacked(url):
    victim = OxideAgentNode(url, "py-victim", "linux", token=TOKEN)
    await victim.connect()
    lv = asyncio.create_task(victim.listen())
    marker = os.path.join(tempfile.gettempdir(), f"forged_{uuid.uuid4().hex}")
    try:
        async with websockets.connect(url) as ws:
            await ws.send(json.dumps({
                "version": "1.0", "correlation_id": str(uuid.uuid4()), "type": "NodeRegistration",
                "from": "py-mallory", "to": "hub", "platform": "linux", "token": TOKEN,
            }))
            ack = json.loads(await asyncio.wait_for(ws.recv(), 3.0))
            assert ack["type"] == "NodeRegistrationAck", ack

            await ws.send(json.dumps({
                "version": "1.0", "correlation_id": str(uuid.uuid4()), "type": "CommandRequest",
                "from": "py-victim", "to": "py-victim", "command": "shell_exec",
                "args": {"cmd": f"touch '{marker}'"}, "require_ack": True,
            }))
            nack = json.loads(await asyncio.wait_for(ws.recv(), 3.0))
            assert nack["type"] == "DeliveryNack" and nack["error_code"] == "ERR_SENDER_MISMATCH", nack
        await asyncio.sleep(0.3)
        assert not os.path.exists(marker), "forged command was executed"
        assert victim.executed_commands_count == 0
        print("PASS forged `from` NACKed, nothing executed")
    finally:
        lv.cancel()
        await victim.close()
        if os.path.exists(marker):
            os.remove(marker)


async def test_foreign_origin_is_refused(url):
    try:
        async with websockets.connect(url, origin="https://evil.example"):
            raise AssertionError("a foreign browser Origin was allowed to open a socket")
    except AssertionError:
        raise
    except Exception as e:  # websockets 11: InvalidStatusCode; 14+: InvalidStatus
        assert "403" in str(e), f"expected an HTTP 403 refusal, got: {e!r}"
    print("PASS foreign Origin refused with 403")


async def test_rest_needs_token(http):
    assert http_get(f"{http}/api/nodes")[0] == 401
    assert http_get(f"{http}/api/nodes", bearer="nope")[0] == 401
    assert http_get(f"{http}/api/status")[0] == 401
    assert http_get(f"{http}/api/status", bearer=TOKEN)[0] == 200
    assert http_get(f"{http}/api/health")[0] == 200
    print("PASS REST catalog needs the token, health stays open")


def run_cli(url, *extra, env_token=None):
    env = {k: v for k, v in os.environ.items() if k != "OXIDE_MESH_TOKEN"}
    if env_token:
        env["OXIDE_MESH_TOKEN"] = env_token
    return subprocess.run(
        [sys.executable, os.path.join(SCRIPTS_DIR, "agent_node.py"), "--hub", url,
         "--id", f"py-cli-{uuid.uuid4().hex[:6]}", "--query-grid-status", "--timeout", "3", *extra],
        capture_output=True, text=True, timeout=30, env=env,
    )


def test_cli_token_flag_and_environment(url):
    # The hub has no grid bridge, so a *registered* node gets ERR_BRIDGE_NOT_CONFIGURED; a rejected
    # one gets ERR_UNAUTHORIZED. Both exit 1, the NACK code tells them apart.
    no_token = run_cli(url)
    assert no_token.returncode == 1 and "ERR_UNAUTHORIZED" in no_token.stdout, no_token.stdout + no_token.stderr

    via_flag = run_cli(url, "--token", TOKEN)
    assert "ERR_BRIDGE_NOT_CONFIGURED" in via_flag.stdout, via_flag.stdout + via_flag.stderr

    via_env = run_cli(url, env_token=TOKEN)
    assert "ERR_BRIDGE_NOT_CONFIGURED" in via_env.stdout, via_env.stdout + via_env.stderr
    print("PASS --token and OXIDE_MESH_TOKEN are honoured by agent_node.py")


async def main():
    proc, url, http = start_hub()
    try:
        await test_missing_and_wrong_token_are_rejected(url, http)
        await test_right_token_routes_commands(url)
        await test_forged_from_is_nacked(url)
        await test_foreign_origin_is_refused(url)
        await test_rest_needs_token(http)
        await asyncio.get_running_loop().run_in_executor(None, test_cli_token_flag_and_environment, url)
        print("ALL TOKEN AUTH TESTS PASSED")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"token auth test failed: {e}", exc_info=True)
        sys.exit(1)
