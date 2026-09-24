#!/usr/bin/env python3
"""
OxideSwarm Cross-Platform Coding Agent Node Client (Python)

Provides a lightweight, zero-compilation node client compatible with
Windows, macOS, Ubuntu Linux, and Android (Termux / NDK).

Features:
- Persistent outbound WebSocket connection (100% NAT/Firewall traversal)
- Automatic reconnection with exponential backoff
- Hardware and OS telemetry advertisement
- Subprocess command execution with timeout watchdog
- Bit-for-bit SHA-256 verified data payload streaming
- Cross-talk isolation (ignores messages not targeted to this node)
"""

import argparse
import asyncio
import hashlib
import json
import logging
import os
import platform
import subprocess
import sys
import time
import uuid

try:
    import websockets
except ImportError:
    print("[ERROR] 'websockets' library is required. Install via: pip install websockets")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("OxideAgent")


def detect_platform_tag() -> str:
    """Detects normalized platform tag (windows, macos, ubuntu, android)."""
    # Android Termux check
    if os.path.exists("/data/data/com.termux") or "ANDROID_ROOT" in os.environ:
        return "android"

    sys_plat = platform.system().lower()
    if sys_plat == "windows":
        return "windows"
    elif sys_plat == "darwin":
        return "macos"
    elif sys_plat == "linux":
        # Check if Ubuntu / Debian
        if os.path.exists("/etc/os-release"):
            try:
                with open("/etc/os-release", "r") as f:
                    content = f.read().lower()
                    if "ubuntu" in content:
                        return "ubuntu"
            except Exception:
                pass
        return "linux"
    return sys_plat


def collect_telemetry(platform_tag: str) -> dict:
    """Collects system hardware and OS capabilities."""
    return {
        "os_name": platform.system(),
        "os_release": platform.release(),
        "os_version": platform.version(),
        "arch": platform.machine(),
        "cpu_cores": os.cpu_count() or 1,
        "platform_tag": platform_tag,
        "python_version": platform.python_version(),
    }


class OxideAgentNode:
    def __init__(self, hub_url: str, node_id: str, platform_name: str, hostname: str = None):
        self.hub_url = hub_url
        self.node_id = node_id
        self.platform_name = platform_name
        self.hostname = hostname or platform.node() or "unknown-host"
        self.ws = None
        self.running = False
        self.active_commands = 0
        self.pending_commands = {}  # corr_id -> asyncio.Future (CommandResponse / terminal DeliveryNack)
        self.pending_acks = {}      # corr_id -> asyncio.Future (DeliveryAck / NodeListResponse)
        self.pending_futures = self.pending_acks  # Backwards compatibility alias
        self.received_data_payloads = []
        self.executed_commands_count = 0
        self.ignored_commands_count = 0

    async def connect(self):
        """Establishes WebSocket connection and registers node."""
        logger.info(f"Connecting to OxideRelay Hub at {self.hub_url}...")
        self.ws = await websockets.connect(
            self.hub_url,
            ping_interval=20,
            ping_timeout=10,
            max_size=16 * 1024 * 1024,
        )
        self.running = True
        logger.info(f"Connected! Registering as '{self.node_id}' ({self.platform_name})...")

        # Send NodeRegistration
        reg_msg = {
            "version": "1.0",
            "correlation_id": str(uuid.uuid4()),
            "type": "NodeRegistration",
            "from": self.node_id,
            "to": "hub",
            "platform": self.platform_name,
            "hostname": self.hostname,
            "capabilities": {
                "system": collect_telemetry(self.platform_name),
                "supported_commands": [
                    "echo",
                    "ping",
                    "shell_exec",
                    "system_info",
                    "sha256_verify",
                ],
            },
            "timestamp": int(time.time() * 1000),
        }
        await self.ws.send(json.dumps(reg_msg))

    async def run(self):
        """Main client loop with automatic reconnection."""
        backoff = 1.0
        while True:
            try:
                await self.connect()
                backoff = 1.0

                # Launch background heartbeat
                hb_task = asyncio.create_task(self._heartbeat_loop())

                # Message pump
                async for raw_msg in self.ws:
                    try:
                        msg = json.loads(raw_msg)
                        asyncio.create_task(self._dispatch_incoming(msg))
                    except Exception as e:
                        logger.error(f"Error handling incoming message: {e}")

                hb_task.cancel()

            except (websockets.exceptions.ConnectionClosed, ConnectionRefusedError, OSError) as e:
                logger.warning(f"Connection error: {e}. Reconnecting in {backoff:.1f}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 15.0)
            except Exception as e:
                logger.error(f"Unexpected client error: {e}. Retrying in 3s...")
                await asyncio.sleep(3.0)

    async def _heartbeat_loop(self):
        while self.running:
            try:
                await asyncio.sleep(10.0)
                if self.ws:
                    hb = {
                        "version": "1.0",
                        "correlation_id": str(uuid.uuid4()),
                        "type": "Heartbeat",
                        "from": self.node_id,
                        "to": "hub",
                        "active_commands": self.active_commands,
                        "timestamp": int(time.time() * 1000),
                    }
                    await self.ws.send(json.dumps(hb))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Failed to send heartbeat: {e}")

    async def _dispatch_incoming(self, msg: dict):
        msg_type = msg.get("type") or msg.get("msg_type")
        corr_id = msg.get("correlation_id") or msg.get("id")
        target = msg.get("to")

        # Verify targeting isolation
        if target and target != self.node_id and target != "all" and target != "hub":
            self.ignored_commands_count += 1
            logger.debug(f"Ignoring message for '{target}' (I am '{self.node_id}')")
            return

        if msg_type == "CommandRequest":
            await self._handle_command_request(msg)
        elif msg_type == "DataPayload":
            await self._handle_data_payload(msg)
        elif msg_type == "CommandResponse":
            if corr_id in self.pending_commands:
                fut = self.pending_commands.pop(corr_id)
                if not fut.done():
                    fut.set_result(msg)
        elif msg_type == "DeliveryAck":
            status = msg.get("status")
            if status == "target_forwarded":
                # Intermediate hub hop acknowledgement: do NOT resolve command response future!
                logger.debug(f"Hub forwarded ACK received for {corr_id} (awaiting CommandResponse)")
            else:
                if corr_id in self.pending_acks:
                    fut = self.pending_acks.pop(corr_id)
                    if not fut.done():
                        fut.set_result(msg)
        elif msg_type == "DeliveryNack":
            # DeliveryNack represents a routing failure (e.g. node not found); fail fast on both queues
            if corr_id in self.pending_commands:
                fut = self.pending_commands.pop(corr_id)
                if not fut.done():
                    fut.set_result(msg)
            if corr_id in self.pending_acks:
                fut = self.pending_acks.pop(corr_id)
                if not fut.done():
                    fut.set_result(msg)
        elif msg_type in ("NodeListResponse", "NodeRegistrationAck"):
            if corr_id in self.pending_acks:
                fut = self.pending_acks.pop(corr_id)
                if not fut.done():
                    fut.set_result(msg)

    async def _handle_command_request(self, req: dict):
        self.active_commands += 1
        self.executed_commands_count += 1
        corr_id = req.get("correlation_id") or req.get("id")
        sender = req.get("from")
        cmd_name = req.get("command")
        args = req.get("args") or {}
        start_time = time.time()

        logger.info(f"Executing command '{cmd_name}' from '{sender}' (corr_id={corr_id})")

        stdout = ""
        stderr = ""
        exit_code = 0
        status = "success"
        proc = None

        try:
            if cmd_name == "echo":
                if isinstance(args, dict):
                    stdout = args.get("message") or args.get("msg") or ""
                elif isinstance(args, list):
                    stdout = " ".join(str(x) for x in args)
                elif isinstance(args, str):
                    stdout = args
                else:
                    stdout = str(args)

            elif cmd_name == "ping":
                stdout = "pong"

            elif cmd_name == "system_info":
                stdout = json.dumps(collect_telemetry(self.platform_name), indent=2)

            elif cmd_name == "sha256_verify":
                data = args.get("data", "")
                expected = args.get("expected", "")
                actual = hashlib.sha256(data.encode("utf-8")).hexdigest()
                matches = actual.lower() == expected.lower()
                stdout = "VERIFIED_MATCH" if matches else "MISMATCH"
                exit_code = 0 if matches else 1
                status = "success" if matches else "failed"

            elif cmd_name == "shell_exec":
                if isinstance(args, dict):
                    cmd_str = args.get("cmd") or args.get("command") or ""
                elif isinstance(args, list):
                    cmd_str = " ".join(str(x) for x in args)
                else:
                    cmd_str = str(args)
                timeout_s = req.get("timeout_ms", 15000) / 1000.0

                proc = await asyncio.create_subprocess_shell(
                    cmd_str,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
                stdout = out.decode("utf-8", errors="replace")
                stderr = err.decode("utf-8", errors="replace")
                exit_code = proc.returncode
                status = "success" if exit_code == 0 else "failed"

            else:
                status = "failed"
                exit_code = 127
                stderr = f"Unsupported command: '{cmd_name}'"

        except asyncio.TimeoutError:
            if proc is not None:
                try:
                    if platform.system().lower() == "windows":
                        subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)], capture_output=True)
                    proc.kill()
                    await proc.wait()
                except Exception:
                    pass
            status = "timeout"
            exit_code = 124
            stderr = f"Command timed out after {req.get('timeout_ms', 15000)} ms"
        except Exception as e:
            status = "failed"
            exit_code = 1
            stderr = str(e)
        finally:
            self.active_commands -= 1

        duration_ms = int((time.time() - start_time) * 1000)
        res = {
            "version": "1.0",
            "correlation_id": corr_id,
            "type": "CommandResponse",
            "from": self.node_id,
            "to": sender,
            "command": cmd_name,
            "status": status,
            "exit_code": exit_code,
            "stdout": stdout,
            "stderr": stderr,
            "execution_duration_ms": duration_ms,
            "timestamp": int(time.time() * 1000),
        }
        if self.ws:
            await self.ws.send(json.dumps(res))

    async def _handle_data_payload(self, msg: dict):
        corr_id = msg.get("correlation_id") or msg.get("id")
        sender = msg.get("from")
        data_str = msg.get("data", "")
        claimed_hash = msg.get("checksum_sha256")

        self.received_data_payloads.append(msg)

        # Latin1 byte preservation for raw binary transport over JSON
        data_bytes = data_str.encode("latin1")
        calc_hash = hashlib.sha256(data_bytes).hexdigest()

        if claimed_hash and calc_hash.lower() != claimed_hash.lower():
            logger.error(f"Data corruption detected from {sender}! Hash mismatch: {calc_hash} != {claimed_hash}")
            nack = {
                "version": "1.0",
                "correlation_id": corr_id,
                "type": "DeliveryNack",
                "from": self.node_id,
                "to": sender,
                "error_code": "ERR_PAYLOAD_CORRUPT",
                "reason": f"SHA-256 mismatch: calculated {calc_hash}, expected {claimed_hash}",
                "timestamp": int(time.time() * 1000),
            }
            if self.ws:
                await self.ws.send(json.dumps(nack))
        else:
            logger.info(f"Received valid DataPayload ({len(data_bytes)} bytes, SHA-256 verified) from {sender}")
            ack = {
                "version": "1.0",
                "correlation_id": corr_id,
                "type": "DeliveryAck",
                "from": self.node_id,
                "to": sender,
                "status": "data_received_verified",
                "timestamp": int(time.time() * 1000),
            }
            if self.ws:
                await self.ws.send(json.dumps(ack))

        if corr_id in self.pending_acks:
            fut = self.pending_acks.pop(corr_id)
            if not fut.done():
                fut.set_result(msg)

    async def send_command(self, target_id: str, command: str, args: dict, timeout: float = 15.0) -> dict:
        """Sends command to target node and awaits execution response."""
        corr_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self.pending_commands[corr_id] = fut

        req = {
            "version": "1.0",
            "correlation_id": corr_id,
            "type": "CommandRequest",
            "from": self.node_id,
            "to": target_id,
            "command": command,
            "args": args,
            "require_ack": True,
            "timeout_ms": int(timeout * 1000),
            "timestamp": int(time.time() * 1000),
        }
        await self.ws.send(json.dumps(req))
        try:
            return await asyncio.wait_for(fut, timeout=timeout + 2.0)
        finally:
            self.pending_commands.pop(corr_id, None)

    async def send_data_payload(self, target_id: str, data_bytes: bytes, timeout: float = 10.0) -> tuple:
        """Sends binary data payload with SHA-256 checksum."""
        corr_id = str(uuid.uuid4())
        sha256_hash = hashlib.sha256(data_bytes).hexdigest()
        data_text = data_bytes.decode("latin1")

        payload_msg = {
            "version": "1.0",
            "correlation_id": corr_id,
            "type": "DataPayload",
            "from": self.node_id,
            "to": target_id,
            "payload_type": "raw_bytes",
            "sequence_number": 0,
            "total_chunks": 1,
            "chunk_size_bytes": len(data_bytes),
            "data": data_text,
            "checksum_sha256": sha256_hash,
            "timestamp": int(time.time() * 1000),
        }
        await self.ws.send(json.dumps(payload_msg))
        return corr_id, sha256_hash

    async def close(self):
        self.running = False
        if self.ws:
            await self.ws.close()


def main():
    detected_plat = detect_platform_tag()
    default_id = f"node-{detected_plat}-{platform.node().lower() or 'agent'}"

    parser = argparse.ArgumentParser(description="OxideSwarm Cross-Platform Agent Node Client")
    parser.add_argument("--hub", default="ws://127.0.0.1:8088/ws", help="OxideRelay Hub WebSocket URL")
    parser.add_argument("--id", default=default_id, help="Node ID")
    parser.add_argument("--platform", default=detected_plat, help="Platform tag (windows, macos, ubuntu, android)")
    parser.add_argument("--hostname", default=None, help="Hostname override")
    args = parser.parse_args()

    node = OxideAgentNode(args.hub, args.id, args.platform, args.hostname)
    try:
        asyncio.run(node.run())
    except KeyboardInterrupt:
        logger.info("Agent node stopped by user.")


if __name__ == "__main__":
    main()
