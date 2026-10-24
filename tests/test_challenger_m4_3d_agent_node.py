#!/usr/bin/env python3
"""
Milestone 4 Challenger Test Suite: Distributed 3D Generative & AutoRig Agent Node
File: tests/test_challenger_m4_3d_agent_node.py

Verifies:
1. Python Agent Node 3D generation submission via WebSocket bridge (prompt & image)
2. Python Agent Node AutoRig submission via WebSocket bridge
3. Cryptographic SHA-256 verification & bit-for-bit integrity validation
4. glTF 2.0 binary asset format compliance (magic 'glTF', version 2, JSON chunk)
5. CLI argument parsing and execution with disk export (--output)
6. Negative validation: missing inputs, SHA-256 tampering, and corrupted payloads
"""

import argparse
import asyncio
import base64
import hashlib
import json
import logging
import os
import struct
import subprocess
import sys
import tempfile
import time
import uuid

# Ensure scripts and tests directory are in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
for p in [current_dir, parent_dir, os.path.join(parent_dir, "scripts")]:
    if p not in sys.path:
        sys.path.insert(0, p)

try:
    import websockets
except ImportError:
    print("[ERROR] 'websockets' library is required. Install via: pip install websockets")
    sys.exit(1)

from agent_node import OxideAgentNode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("TestChallengerM4")


def create_minimal_glb() -> bytes:
    """Generates a syntactically valid minimal glTF 2.0 binary asset."""
    json_chunk = b'{"asset":{"version":"2.0"},"scenes":[{"nodes":[0]}],"nodes":[{"name":"RootNode"}]}'
    # Pad JSON chunk to 4-byte boundary with spaces (0x20)
    json_pad = (4 - (len(json_chunk) % 4)) % 4
    json_chunk += b" " * json_pad

    header_len = 12
    chunk0_header_len = 8
    total_len = header_len + chunk0_header_len + len(json_chunk)

    glb = bytearray()
    glb.extend(b"glTF")  # magic
    glb.extend(struct.pack("<I", 2))  # version 2
    glb.extend(struct.pack("<I", total_len))  # total length
    glb.extend(struct.pack("<I", len(json_chunk)))  # chunk 0 length
    glb.extend(struct.pack("<I", 0x4E4F534A))  # chunk 0 type: JSON
    glb.extend(json_chunk)
    return bytes(glb)


def create_humanoid_obj() -> bytes:
    """Generates a Wavefront OBJ humanoid mesh with anatomical proportions suitable for AutoRig landmark extraction."""
    lines = ["# OxideSwarm Humanoid OBJ"]
    boxes = [
        (-0.20, 0.50, 0.0, 0.08, 0.50, 0.08),  # left leg
        (0.20, 0.50, 0.0, 0.08, 0.50, 0.08),   # right leg
        (0.0, 1.30, 0.0, 0.22, 0.30, 0.12),    # torso
        (-0.38, 1.35, 0.0, 0.14, 0.06, 0.06),  # left arm
        (0.38, 1.35, 0.0, 0.14, 0.06, 0.06),   # right arm
        (0.0, 1.80, 0.0, 0.12, 0.20, 0.12),    # head
    ]
    v_lines = []
    f_lines = []
    base = 1
    for cx, cy, cz, hx, hy, hz in boxes:
        for dx in (-hx, hx):
            for dy in (-hy, hy):
                for dz in (-hz, hz):
                    v_lines.append(f"v {cx + dx:.4f} {cy + dy:.4f} {cz + dz:.4f}")
        faces = [
            (base, base + 2, base + 1),
            (base + 1, base + 2, base + 3),
            (base + 4, base + 5, base + 6),
            (base + 5, base + 7, base + 6),
            (base, base + 1, base + 4),
            (base + 1, base + 5, base + 4),
            (base + 2, base + 6, base + 3),
            (base + 3, base + 6, base + 7),
            (base, base + 4, base + 2),
            (base + 2, base + 4, base + 6),
            (base + 1, base + 3, base + 5),
            (base + 3, base + 7, base + 5),
        ]
        for f in faces:
            f_lines.append(f"f {f[0]} {f[1]} {f[2]}")
        base += 8
    lines.extend(v_lines)
    lines.extend(f_lines)
    return "\n".join(lines).encode("utf-8")


def validate_glb_bytes(data: bytes):
    """Validates glTF 2.0 binary header and structure."""
    if len(data) < 20:
        raise ValueError(f"GLB data too short: {len(data)} bytes (minimum 20)")
    magic = data[0:4]
    if magic != b"glTF":
        raise ValueError(f"Invalid magic: {magic}, expected b'glTF'")
    version = struct.unpack("<I", data[4:8])[0]
    if version != 2:
        raise ValueError(f"Invalid glTF version: {version}, expected 2")
    total_len = struct.unpack("<I", data[8:12])[0]
    if total_len != len(data):
        raise ValueError(f"Total length mismatch: header specifies {total_len}, actual is {len(data)}")
    chunk_type = struct.unpack("<I", data[16:20])[0]
    if chunk_type != 0x4E4F534A:
        raise ValueError(f"First chunk type is {hex(chunk_type)}, expected 0x4E4F534A (JSON)")


# ==============================================================================
# Simulated Mock Hub (Used when no external cluster hub is provided)
# ==============================================================================
class Mock3DHub:
    """In-process mock relay hub for standalone challenger execution."""

    def __init__(self, host="127.0.0.1", port=0):
        self.host = host
        self.port = port
        self.server = None
        self.actual_port = None
        self.glb_asset = create_minimal_glb()
        self.glb_sha256 = hashlib.sha256(self.glb_asset).hexdigest()
        self.glb_b64 = base64.b64encode(self.glb_asset).decode("ascii")

    async def _handler(self, websocket):
        try:
            async for raw in websocket:
                msg = json.loads(raw)
                msg_type = msg.get("type")
                corr_id = msg.get("correlation_id", "")
                from_node = msg.get("from", "")

                if msg_type == "NodeRegistration":
                    ack = {
                        "version": "1.0",
                        "correlation_id": corr_id,
                        "type": "NodeRegistrationAck",
                        "from": "hub",
                        "to": from_node,
                        "status": "accepted",
                        "timestamp": int(time.time() * 1000),
                    }
                    await websocket.send(json.dumps(ack))

                elif msg_type == "CommandRequest":
                    cmd = msg.get("command", "")
                    require_ack = msg.get("require_ack", False)
                    if require_ack:
                        d_ack = {
                            "version": "1.0",
                            "correlation_id": corr_id,
                            "type": "DeliveryAck",
                            "from": "hub",
                            "to": from_node,
                            "status": "target_forwarded",
                            "timestamp": int(time.time() * 1000),
                        }
                        await websocket.send(json.dumps(d_ack))

                    args = msg.get("args", {})
                    if cmd in ("grid_3d_generate", "3d_generate"):
                        prompt = args.get("prompt")
                        image = args.get("image")
                        if not prompt and not image:
                            resp = {
                                "version": "1.0",
                                "correlation_id": corr_id,
                                "type": "CommandResponse",
                                "from": "grid",
                                "to": from_node,
                                "command": cmd,
                                "status": "failed",
                                "exit_code": 1,
                                "stdout": "",
                                "stderr": "3D generation requires either non-empty 'prompt' or 'image'",
                                "timestamp": int(time.time() * 1000),
                            }
                        else:
                            resp = {
                                "version": "1.0",
                                "correlation_id": corr_id,
                                "type": "CommandResponse",
                                "from": "grid",
                                "to": from_node,
                                "command": cmd,
                                "status": "success",
                                "exit_code": 0,
                                "stdout": f"3D Asset generated successfully ({len(self.glb_asset)} bytes)",
                                "stderr": "",
                                "payload": {
                                    "task_id": str(uuid.uuid4()),
                                    "worker_id": str(uuid.uuid4()),
                                    "is_gpu_executed": False,
                                    "device_name": "Procedural-SDF-PureRust",
                                    "execution_time_ms": 42,
                                    "asset_bytes_len": len(self.glb_asset),
                                    "asset_sha256": self.glb_sha256,
                                    "asset_base64": self.glb_b64,
                                },
                                "timestamp": int(time.time() * 1000),
                            }
                        await websocket.send(json.dumps(resp))

                    elif cmd in ("grid_auto_rig", "auto_rig"):
                        mesh = args.get("input_mesh")
                        if not mesh:
                            resp = {
                                "version": "1.0",
                                "correlation_id": corr_id,
                                "type": "CommandResponse",
                                "from": "grid",
                                "to": from_node,
                                "command": cmd,
                                "status": "failed",
                                "exit_code": 1,
                                "stdout": "",
                                "stderr": "AutoRig requires non-empty 'input_mesh'",
                                "timestamp": int(time.time() * 1000),
                            }
                        else:
                            resp = {
                                "version": "1.0",
                                "correlation_id": corr_id,
                                "type": "CommandResponse",
                                "from": "grid",
                                "to": from_node,
                                "command": cmd,
                                "status": "success",
                                "exit_code": 0,
                                "stdout": f"AutoRig synthesis completed ({len(self.glb_asset)} bytes)",
                                "stderr": "",
                                "payload": {
                                    "task_id": str(uuid.uuid4()),
                                    "worker_id": str(uuid.uuid4()),
                                    "is_gpu_executed": False,
                                    "device_name": "OxideSwarm-AutoRig-Engine",
                                    "execution_time_ms": 38,
                                    "asset_bytes_len": len(self.glb_asset),
                                    "asset_sha256": self.glb_sha256,
                                    "asset_base64": self.glb_b64,
                                },
                                "timestamp": int(time.time() * 1000),
                            }
                        await websocket.send(json.dumps(resp))
        except websockets.exceptions.ConnectionClosed:
            pass

    async def start(self):
        self.server = await websockets.serve(self._handler, self.host, self.port)
        for sock in self.server.sockets:
            self.actual_port = sock.getsockname()[1]
            break
        return f"ws://{self.host}:{self.actual_port}/ws"

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()


# ==============================================================================
# Challenger Test Runner
# ==============================================================================
async def run_challenger_tests(hub_url: str):
    logger.info("==================================================================")
    logger.info("  STARTING M4 3D GENERATIVE & AUTORIG CHALLENGER SUITE           ")
    logger.info(f"  Target Hub: {hub_url}")
    logger.info("==================================================================")

    with tempfile.TemporaryDirectory() as temp_dir:
        node = OxideAgentNode(hub_url, "test-m4-agent-node", "windows")
        await node.connect()
        listen_task = asyncio.create_task(node.listen())

        try:
            # ------------------------------------------------------------------
            # Test 1: Submit 3D Generative Workload (Text-to-3D)
            # ------------------------------------------------------------------
            logger.info("\n[Test 1] Submitting Text-to-3D Generation task...")
            out_3d_path = os.path.join(temp_dir, "output_crystal.glb")
            res_3d = await node.submit_3d_generate(
                prompt="procedural glowing crystal",
                quality="fast",
                engine="sdf",
                export_texture=True,
                output_path=out_3d_path,
                timeout_s=30.0,
            )
            assert res_3d.get("status") == "success", f"3D generation failed: {res_3d}"
            assert res_3d.get("exit_code") == 0, f"Non-zero exit code: {res_3d}"
            assert res_3d.get("from") == "grid", "Response must originate from 'grid'"
            assert os.path.exists(out_3d_path), "Output GLB file was not created on disk"

            with open(out_3d_path, "rb") as f:
                asset_3d_bytes = f.read()
            validate_glb_bytes(asset_3d_bytes)
            logger.info(f"✓ Text-to-3D Generation verified: {len(asset_3d_bytes)} bytes, valid glTF 2.0")

            # ------------------------------------------------------------------
            # Test 2: Submit AutoRig Workload (Mesh-to-Rig)
            # ------------------------------------------------------------------
            logger.info("\n[Test 2] Submitting AutoRig Workload...")
            input_mesh = create_humanoid_obj()
            out_rig_path = os.path.join(temp_dir, "output_rigged.glb")
            res_rig = await node.submit_auto_rig(
                input_mesh=input_mesh,
                rig_preset="biped",
                preferred_provider="builtin",
                output_path=out_rig_path,
                timeout_s=30.0,
            )
            assert res_rig.get("status") == "success", f"AutoRig failed: {res_rig}"
            assert res_rig.get("exit_code") == 0, f"Non-zero exit code: {res_rig}"
            assert os.path.exists(out_rig_path), "Rigged GLB file was not created on disk"

            with open(out_rig_path, "rb") as f:
                asset_rig_bytes = f.read()
            validate_glb_bytes(asset_rig_bytes)
            logger.info(f"✓ AutoRig Workload verified: {len(asset_rig_bytes)} bytes, valid glTF 2.0")

            # ------------------------------------------------------------------
            # Test 3: Cryptographic Integrity & SHA-256 Tamper Detection
            # ------------------------------------------------------------------
            logger.info("\n[Test 3] Testing SHA-256 Tampering & Corruption Detection...")
            # Mutate payload with incorrect hash
            tampered_res = {
                "status": "success",
                "payload": {
                    "asset_base64": base64.b64encode(input_mesh).decode("ascii"),
                    "asset_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
                },
            }
            try:
                node.extract_and_verify_asset(tampered_res)
                raise AssertionError("Expected ValueError on SHA-256 mismatch, but none was raised")
            except ValueError as e:
                assert "SHA-256 verification failed" in str(e), f"Unexpected error message: {e}"
                logger.info("✓ SHA-256 tampering successfully caught and rejected with ValueError")

            # ------------------------------------------------------------------
            # Test 4: Negative Validations
            # ------------------------------------------------------------------
            logger.info("\n[Test 4] Testing Negative Validations (Missing prompt & mesh)...")
            res_empty_3d = await node.submit_3d_generate(prompt=None, image_bytes=None, timeout_s=10.0)
            assert res_empty_3d.get("status") == "failed", "Empty 3D spec must fail"

            res_empty_rig = await node.submit_auto_rig(input_mesh=b"", timeout_s=10.0)
            assert res_empty_rig.get("status") == "failed", "Empty mesh AutoRig must fail"
            logger.info("✓ Negative validations properly rejected by grid")

        finally:
            listen_task.cancel()
            await node.close()

        # ----------------------------------------------------------------------
        # Test 5: Subprocess CLI Execution (--submit-3d-generate)
        # ----------------------------------------------------------------------
        logger.info("\n[Test 5] Testing CLI Subprocess 3D Submission...")
        cli_out_3d = os.path.join(temp_dir, "cli_monolith.glb")
        py_bin = sys.executable
        cli_proc = await asyncio.create_subprocess_exec(
            py_bin,
            os.path.join(parent_dir, "scripts", "agent_node.py"),
            "--hub",
            hub_url,
            "--id",
            "cli-test-agent",
            "--submit-3d-generate",
            "procedural monolith",
            "--engine",
            "sdf",
            "-o",
            cli_out_3d,
            "--timeout",
            "30",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        cli_stdout, cli_stderr = await cli_proc.communicate()
        out_str = cli_stdout.decode("utf-8", errors="replace")
        err_str = cli_stderr.decode("utf-8", errors="replace")
        if cli_proc.returncode != 0:
            logger.error(f"CLI 3D STDOUT: {out_str}")
            logger.error(f"CLI 3D STDERR: {err_str}")
        assert cli_proc.returncode == 0, f"CLI 3D submission failed with exit code {cli_proc.returncode}"
        assert os.path.exists(cli_out_3d), "CLI did not create output file"
        with open(cli_out_3d, "rb") as f:
            cli_data = f.read()
        validate_glb_bytes(cli_data)
        logger.info(f"✓ CLI Subprocess 3D generation verified: {len(cli_data)} bytes exported")

        # ----------------------------------------------------------------------
        # Test 6: Subprocess CLI Execution (--submit-auto-rig)
        # ----------------------------------------------------------------------
        logger.info("\n[Test 6] Testing CLI Subprocess AutoRig Submission...")
        cli_mesh_in = os.path.join(temp_dir, "input_for_cli.obj")
        with open(cli_mesh_in, "wb") as f:
            f.write(create_humanoid_obj())

        cli_out_rig = os.path.join(temp_dir, "cli_rigged.glb")
        cli_rig_proc = await asyncio.create_subprocess_exec(
            py_bin,
            os.path.join(parent_dir, "scripts", "agent_node.py"),
            "--hub",
            hub_url,
            "--id",
            "cli-rig-agent",
            "--submit-auto-rig",
            "--input-mesh",
            cli_mesh_in,
            "--provider",
            "builtin",
            "-o",
            cli_out_rig,
            "--timeout",
            "30",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        rig_stdout, rig_stderr = await cli_rig_proc.communicate()
        rig_out_str = rig_stdout.decode("utf-8", errors="replace")
        rig_err_str = rig_stderr.decode("utf-8", errors="replace")
        if cli_rig_proc.returncode != 0:
            logger.error(f"CLI AutoRig STDOUT: {rig_out_str}")
            logger.error(f"CLI AutoRig STDERR: {rig_err_str}")
        assert cli_rig_proc.returncode == 0, f"CLI AutoRig failed with code {cli_rig_proc.returncode}"
        assert os.path.exists(cli_out_rig), "CLI AutoRig did not create output file"
        with open(cli_out_rig, "rb") as f:
            rig_data = f.read()
        validate_glb_bytes(rig_data)
        logger.info(f"✓ CLI Subprocess AutoRig verified: {len(rig_data)} bytes exported")

    logger.info("\n==================================================================")
    logger.info("  M4 3D GENERATIVE & AUTORIG CHALLENGER SUITE PASSED (100% SUCCESS)")
    logger.info("==================================================================")
    return True


async def main():
    parser = argparse.ArgumentParser(description="M4 3D Challenger Test Suite")
    parser.add_argument("--hub", default=None, help="Target OxideRelay Hub WebSocket URL")
    args = parser.parse_args()

    if args.hub:
        success = await run_challenger_tests(args.hub)
        sys.exit(0 if success else 1)
    else:
        # Spin up local mock hub for standalone execution
        mock_hub = Mock3DHub()
        hub_url = await mock_hub.start()
        logger.info(f"Started Mock3DHub on {hub_url}")
        try:
            success = await run_challenger_tests(hub_url)
            sys.exit(0 if success else 1)
        finally:
            await mock_hub.stop()


if __name__ == "__main__":
    asyncio.run(main())
