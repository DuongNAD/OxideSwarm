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
- Hub token authentication: pass --token or set OXIDE_MESH_TOKEN when the hub requires one
  (the token is sent in clear text inside the WebSocket; use wss:// on untrusted networks)
"""

import argparse
import asyncio
import hashlib
import json
import logging
import os
import platform
import signal
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

# DeliveryNack error codes the hub answers a NodeRegistration with when it refuses the node.
REGISTRATION_ERROR_CODES = ("ERR_UNAUTHORIZED", "ERR_DUPLICATE_NODE_ID", "ERR_INVALID_NODE_ID")
# Rejections that cannot fix themselves: do not hammer the hub, retry rarely.
REGISTRATION_FATAL_CODES = ("ERR_UNAUTHORIZED", "ERR_INVALID_NODE_ID")
REJECTED_RETRY_DELAY_S = 30.0


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


def kill_process_tree(proc) -> None:
    """Kills a command started by `shell_exec` together with everything it spawned.

    On POSIX the command runs in its own session (see `start_new_session` below), so its process
    group id is its PID and one `killpg` reaches the grandchildren too. Without that, a grandchild
    that inherited the output pipes keeps them open and even `proc.wait()` blocks until it exits.
    """
    try:
        if platform.system().lower() == "windows":
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)], capture_output=True)
        else:
            os.killpg(proc.pid, signal.SIGKILL)
    except Exception:
        pass
    try:
        proc.kill()
    except Exception:
        pass


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
    def __init__(self, hub_url: str, node_id: str, platform_name: str, hostname: str = None, token: str = None):
        self.hub_url = hub_url
        self.node_id = node_id
        self.platform_name = platform_name
        self.hostname = hostname or platform.node() or "unknown-host"
        # Hub token (OXIDE_MESH_TOKEN / --token); an empty string means "no token".
        self.token = token or None
        # (error_code, reason) when the hub answered the last registration with a NACK.
        self.registration_error = None
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
        self.registration_error = None
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
        if self.token:
            reg_msg["token"] = self.token
        await self.ws.send(json.dumps(reg_msg))

    def _fail_pending(self, msg: dict):
        """Resolves every outstanding request with `msg` (the hub's refusal), so callers fail fast."""
        for futures in (self.pending_commands, self.pending_acks):
            for corr_id in list(futures):
                fut = futures.pop(corr_id)
                if not fut.done():
                    fut.set_result(msg)

    def _rejection_delay(self) -> float:
        """How long to wait before registering again after a hub NACK."""
        code = self.registration_error[0] if self.registration_error else None
        return REJECTED_RETRY_DELAY_S if code in REGISTRATION_FATAL_CODES else 10.0

    async def listen(self):
        """Processes incoming messages from the WebSocket."""
        async for raw_msg in self.ws:
            try:
                msg = json.loads(raw_msg)
                if msg.get("type") == "DeliveryNack" and msg.get("error_code") in REGISTRATION_ERROR_CODES:
                    # Handled inline: the hub closes the socket right behind this frame, and the
                    # reconnect logic must already know why by the time the close is noticed.
                    await self._dispatch_incoming(msg)
                else:
                    asyncio.create_task(self._dispatch_incoming(msg))
            except Exception as e:
                logger.error(f"Error handling incoming message: {e}")

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
                try:
                    await self.listen()
                finally:
                    hb_task.cancel()

                # The hub closed the connection. Pace the reconnect instead of spinning, and back
                # off hard when it refused us for a reason that will not go away by itself.
                if self.registration_error:
                    delay = self._rejection_delay()
                    logger.error(f"Retrying the registration in {delay:.0f}s...")
                    await asyncio.sleep(delay)
                else:
                    logger.warning(f"Connection closed by hub. Reconnecting in {backoff:.1f}s...")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 1.5, 15.0)

            except (websockets.exceptions.ConnectionClosed, ConnectionRefusedError, OSError) as e:
                if self.registration_error:
                    delay = self._rejection_delay()
                    logger.error(f"Connection closed after the hub rejected the registration. Retrying in {delay:.0f}s...")
                    await asyncio.sleep(delay)
                else:
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
        elif msg_type == "DeliveryNack" and msg.get("error_code") in REGISTRATION_ERROR_CODES:
            # The hub refused this node (bad token, duplicate id, ...): it closes the socket next.
            self.registration_error = (msg.get("error_code"), msg.get("reason", ""))
            logger.error(f"Hub rejected the registration: {msg.get('error_code')} - {msg.get('reason', '')}")
            self._fail_pending(msg)
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
                    start_new_session=(os.name == "posix"),
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
                kill_process_tree(proc)
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5.0)
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
        try:
            try:
                await self.ws.send(json.dumps(req))
            except websockets.exceptions.ConnectionClosed:
                if self.registration_error:
                    # The hub already hung up on us after refusing the registration.
                    code, reason = self.registration_error
                    return {
                        "type": "DeliveryNack",
                        "correlation_id": corr_id,
                        "from": "hub",
                        "to": self.node_id,
                        "error_code": code,
                        "reason": reason,
                    }
                raise
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

    async def submit_grid_compute(
        self,
        kernel: str = "matrix_multiply",
        matrix_dim: int = 128,
        timeout_s: float = 30.0,
    ) -> dict:
        """
        Submits a GPU/matrix compute job to the OxideSwarm Grid via the Agent-Grid Bridge.
        Awaits and returns the CommandResponse envelope dictionary.
        """
        args = {
            "kernel_name": kernel,
            "kernel": kernel,
            "matrix_dim": matrix_dim,
            "simulated_matrix_dim": matrix_dim,
        }
        return await self.send_command("grid", "grid_compute", args, timeout=timeout_s)

    async def submit_grid_compile(
        self,
        crate_name: str,
        files: dict = None,
        flags: list = None,
        timeout_s: float = 60.0,
    ) -> dict:
        """
        Submits a distributed Rust compilation job to the OxideSwarm Grid via the Agent-Grid Bridge.
        Awaits and returns the CommandResponse envelope dictionary.
        """
        args = {
            "crate_name": crate_name,
            "source_files": files if files is not None else {
                "src/main.rs": 'fn main() { println!("Hello from compiled agent grid binary!"); }'
            },
            "compiler_flags": flags or ["build"],
        }
        return await self.send_command("grid", "grid_compile", args, timeout=timeout_s)

    async def query_grid_status(self, timeout_s: float = 10.0) -> dict:
        """
        Queries cluster status from the Master Grid scheduler via the Agent-Grid Bridge.
        """
        return await self.send_command("grid", "grid_status", {}, timeout=timeout_s)

    async def submit_3d_generate(
        self,
        prompt: str = None,
        image_bytes: bytes = None,
        quality: str = "standard",
        engine: str = "auto",
        export_texture: bool = True,
        output_path: str = None,
        timeout_s: float = 60.0,
    ) -> dict:
        """
        Submits a distributed 3D generative task to the OxideSwarm Grid via the Agent-Grid Bridge.
        Awaits and returns the CommandResponse envelope dictionary.
        Optionally verifies and saves the returned binary asset to output_path.
        """
        import base64
        args = {
            "quality": quality,
            "engine": engine,
            "export_texture": export_texture,
        }
        if prompt is not None:
            args["prompt"] = prompt
        if image_bytes is not None:
            args["image"] = base64.b64encode(image_bytes).decode("ascii")

        res = await self.send_command("grid", "grid_3d_generate", args, timeout=timeout_s)
        if output_path and res.get("status") == "success":
            self.extract_and_verify_asset(res, output_path=output_path)
        return res

    async def submit_auto_rig(
        self,
        input_mesh: bytes,
        rig_preset: str = "biped",
        embed_animations: list = None,
        preferred_provider: str = "builtin",
        output_path: str = None,
        timeout_s: float = 60.0,
    ) -> dict:
        """
        Submits an AutoRig task to the OxideSwarm Grid via the Agent-Grid Bridge.
        Awaits and returns the CommandResponse envelope dictionary.
        Optionally verifies and saves the rigged binary asset to output_path.
        """
        import base64
        args = {
            "input_mesh": base64.b64encode(input_mesh).decode("ascii"),
            "rig_preset": rig_preset,
            "embed_animations": embed_animations or [],
            "preferred_provider": preferred_provider,
        }
        res = await self.send_command("grid", "grid_auto_rig", args, timeout=timeout_s)
        if output_path and res.get("status") == "success":
            self.extract_and_verify_asset(res, output_path=output_path)
        return res

    def extract_and_verify_asset(self, response: dict, output_path: str = None) -> bytes:
        """
        Extracts and cryptographically verifies binary asset bytes from a CommandResponse envelope.
        Validates SHA-256 against asset_sha256 if present, and optionally writes to output_path.
        """
        import base64
        payload = response.get("payload") or {}
        b64_data = payload.get("asset_base64")
        if not b64_data:
            raise ValueError("Response payload does not contain 'asset_base64'")

        asset_bytes = base64.b64decode(b64_data)
        expected_sha256 = payload.get("asset_sha256")
        if expected_sha256:
            actual_sha256 = hashlib.sha256(asset_bytes).hexdigest()
            if actual_sha256.lower() != expected_sha256.lower():
                raise ValueError(
                    f"Asset SHA-256 verification failed: expected {expected_sha256}, got {actual_sha256}"
                )

        if output_path:
            out_dir = os.path.dirname(os.path.abspath(output_path))
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)
            with open(output_path, "wb") as f:
                f.write(asset_bytes)
            logger.info(f"Verified asset written to {output_path} ({len(asset_bytes)} bytes)")

        return asset_bytes

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
    parser.add_argument(
        "--token",
        default=os.environ.get("OXIDE_MESH_TOKEN") or None,
        help="Hub token (default: $OXIDE_MESH_TOKEN). Needed when the hub was started with --token; "
        "visible in the process list, so prefer the environment variable",
    )

    # Command submission options (flags & subcommands supported)
    parser.add_argument(
        "--submit-grid-compute",
        nargs="?",
        const="matrix_multiply",
        default=None,
        help="Submit compute job to grid and exit",
    )
    parser.add_argument(
        "--submit-grid-compile",
        nargs="?",
        const="sample_crate",
        default=None,
        help="Submit compilation job to grid and exit",
    )
    parser.add_argument(
        "--query-grid-status",
        "--grid-status",
        dest="query_grid_status",
        action="store_true",
        help="Query grid status and exit",
    )
    parser.add_argument("--kernel", default=None, help="Compute kernel name")
    parser.add_argument("--matrix-dim", type=int, default=128, help="Matrix dimension")
    parser.add_argument("--crate-name", default="sample_crate", help="Crate name for compilation")
    parser.add_argument("--flags", nargs="*", default=["build"], help="Compiler flags")
    parser.add_argument("--timeout", type=float, default=30.0, help="Operation timeout in seconds")

    # 3D Generative and AutoRig submission options
    parser.add_argument(
        "--submit-3d-generate",
        "--3d-generate",
        dest="submit_3d_generate",
        nargs="?",
        const="",
        default=None,
        help="Submit 3D generation job to grid (specify prompt or use --prompt)",
    )
    parser.add_argument(
        "--submit-auto-rig",
        "--auto-rig",
        dest="submit_auto_rig",
        nargs="?",
        const="",
        default=None,
        help="Submit AutoRig job to grid (specify input mesh path or use --input-mesh)",
    )
    parser.add_argument("--prompt", default=None, help="Text prompt for 3D generation")
    parser.add_argument("--image", default=None, help="Image file path for image-to-3D generation")
    parser.add_argument("--quality", default="standard", choices=["fast", "standard", "high"], help="3D quality tier")
    parser.add_argument("--engine", default="auto", help="3D generative engine (auto, rodin, trellis, hunyuan, triposr, tier0_sdf)")
    parser.add_argument("--export-texture", dest="export_texture", action="store_true", default=True, help="Export textures in generated 3D asset")
    parser.add_argument("--no-export-texture", dest="export_texture", action="store_false", help="Disable texture export")
    parser.add_argument("--input-mesh", "--mesh", dest="input_mesh", default=None, help="Input mesh file path for AutoRig")
    parser.add_argument("--rig-preset", default="biped", choices=["biped", "quadruped", "vehicle", "generic"], help="AutoRig preset skeleton")
    parser.add_argument("--provider", "--preferred-provider", dest="preferred_provider", default="builtin", help="Preferred AutoRig provider (builtin, blender)")
    parser.add_argument("-o", "--output", dest="output_path", default=None, help="Output file path to save resulting GLB asset")

    subparsers = parser.add_subparsers(dest="subcommand", help="Optional subcommand")
    compute_sub = subparsers.add_parser("grid-compute", help="Submit compute job")
    compute_sub.add_argument("kernel", nargs="?", default="matrix_multiply")
    compute_sub.add_argument("--matrix-dim", type=int, default=128)
    compute_sub.add_argument("--timeout", type=float, default=30.0)

    compile_sub = subparsers.add_parser("grid-compile", help="Submit compilation job")
    compile_sub.add_argument("--crate-name", default="sample_crate")
    compile_sub.add_argument("--flags", nargs="*", default=["build"])
    compile_sub.add_argument("--timeout", type=float, default=60.0)

    status_sub = subparsers.add_parser("grid-status", help="Query grid status")
    status_sub.add_argument("--timeout", type=float, default=10.0)

    gen3d_sub = subparsers.add_parser("3d-generate", help="Submit 3D generation job")
    gen3d_sub.add_argument("--prompt", default=None)
    gen3d_sub.add_argument("--image", default=None)
    gen3d_sub.add_argument("--quality", default="standard")
    gen3d_sub.add_argument("--engine", default="auto")
    gen3d_sub.add_argument("-o", "--output", dest="output_path", default=None)
    gen3d_sub.add_argument("--timeout", type=float, default=60.0)

    autorig_sub = subparsers.add_parser("auto-rig", help="Submit AutoRig job")
    autorig_sub.add_argument("input_mesh", nargs="?", default=None)
    autorig_sub.add_argument("--rig-preset", default="biped")
    autorig_sub.add_argument("--provider", default="builtin")
    autorig_sub.add_argument("-o", "--output", dest="output_path", default=None)
    autorig_sub.add_argument("--timeout", type=float, default=60.0)

    args = parser.parse_args()

    node = OxideAgentNode(args.hub, args.id, args.platform, args.hostname, token=args.token)

    async def execute():
        is_compute = (
            args.submit_grid_compute is not None
            or args.subcommand in ("grid-compute", "submit-grid-compute")
        )
        is_compile = (
            args.submit_grid_compile is not None
            or args.subcommand in ("grid-compile", "submit-grid-compile")
        )
        is_status = (
            getattr(args, "query_grid_status", False)
            or args.subcommand in ("grid-status", "query-grid-status")
        )
        is_3d = (
            args.submit_3d_generate is not None
            or args.subcommand in ("3d-generate", "grid-3d-generate", "submit-3d-generate")
        )
        is_autorig = (
            args.submit_auto_rig is not None
            or args.subcommand in ("auto-rig", "grid-auto-rig", "submit-auto-rig")
        )

        if is_3d:
            prompt = args.prompt or (
                args.submit_3d_generate
                if isinstance(args.submit_3d_generate, str) and args.submit_3d_generate
                else None
            )
            image_bytes = None
            if args.image:
                if not os.path.exists(args.image):
                    logger.error(f"Image file not found: {args.image}")
                    sys.exit(1)
                with open(args.image, "rb") as f:
                    image_bytes = f.read()

            if not prompt and not image_bytes:
                logger.error("3D generation requires --prompt or --image")
                sys.exit(1)

            await node.connect()
            listener_task = asyncio.create_task(node.listen())
            try:
                res = await node.submit_3d_generate(
                    prompt=prompt,
                    image_bytes=image_bytes,
                    quality=getattr(args, "quality", "standard"),
                    engine=getattr(args, "engine", "auto"),
                    export_texture=getattr(args, "export_texture", True),
                    output_path=getattr(args, "output_path", None),
                    timeout_s=args.timeout,
                )
                print(json.dumps(res, indent=2))
                is_ok = res.get("status") == "success" and res.get("exit_code") == 0
                sys.exit(0 if is_ok else 1)
            finally:
                listener_task.cancel()
                await node.close()

        elif is_autorig:
            mesh_path = (
                args.input_mesh
                or (
                    args.submit_auto_rig
                    if isinstance(args.submit_auto_rig, str) and args.submit_auto_rig
                    else None
                )
            )
            if not mesh_path or not os.path.exists(mesh_path):
                logger.error(f"AutoRig requires valid mesh file path, got: {mesh_path}")
                sys.exit(1)

            with open(mesh_path, "rb") as f:
                mesh_bytes = f.read()

            await node.connect()
            listener_task = asyncio.create_task(node.listen())
            try:
                res = await node.submit_auto_rig(
                    input_mesh=mesh_bytes,
                    rig_preset=getattr(args, "rig_preset", "biped"),
                    preferred_provider=getattr(args, "preferred_provider", "builtin"),
                    output_path=getattr(args, "output_path", None),
                    timeout_s=args.timeout,
                )
                print(json.dumps(res, indent=2))
                is_ok = res.get("status") == "success" and res.get("exit_code") == 0
                sys.exit(0 if is_ok else 1)
            finally:
                listener_task.cancel()
                await node.close()

        elif is_compute:
            kernel = args.kernel or (
                args.submit_grid_compute
                if isinstance(args.submit_grid_compute, str) and args.submit_grid_compute != "matrix_multiply"
                else "matrix_multiply"
            )
            if not kernel:
                kernel = "matrix_multiply"
            await node.connect()
            listener_task = asyncio.create_task(node.listen())
            try:
                res = await node.submit_grid_compute(
                    kernel=kernel,
                    matrix_dim=args.matrix_dim,
                    timeout_s=args.timeout,
                )
                print(json.dumps(res, indent=2))
                is_ok = res.get("status") == "success" and res.get("exit_code") == 0
                sys.exit(0 if is_ok else 1)
            finally:
                listener_task.cancel()
                await node.close()

        elif is_compile:
            crate_name = args.crate_name or (
                args.submit_grid_compile
                if isinstance(args.submit_grid_compile, str)
                else "sample_crate"
            )
            files = {
                "src/main.rs": 'fn main() { println!("Hello from compiled agent grid binary!"); }'
            }
            await node.connect()
            listener_task = asyncio.create_task(node.listen())
            try:
                res = await node.submit_grid_compile(
                    crate_name=crate_name,
                    files=files,
                    flags=args.flags,
                    timeout_s=args.timeout,
                )
                print(json.dumps(res, indent=2))
                is_ok = res.get("status") == "success" and res.get("exit_code") == 0
                sys.exit(0 if is_ok else 1)
            finally:
                listener_task.cancel()
                await node.close()

        elif is_status:
            await node.connect()
            listener_task = asyncio.create_task(node.listen())
            try:
                res = await node.query_grid_status(timeout_s=args.timeout)
                print(json.dumps(res, indent=2))
                is_ok = res.get("status") == "success" and res.get("exit_code") == 0
                sys.exit(0 if is_ok else 1)
            finally:
                listener_task.cancel()
                await node.close()

        else:
            await node.run()

    try:
        asyncio.run(execute())
    except KeyboardInterrupt:
        logger.info("Agent node stopped by user.")


if __name__ == "__main__":
    main()
