#!/usr/bin/env python3
"""
OxideSwarm Wave 2 Opaque-Box End-to-End Test Suite (`prod/backlog-completion-wave2`)

Covers 4 Tiers across Wave 2 features F1-F6:
  - F1: TEST-61  macOS Hard-Link Isolation & Lifeline Safety
  - F2: TEST-41  Master Shutdown / Durability Hand-Over (Zero Double Execution)
  - F3: AX-1     CLI Matrix Fan-Out (`submit --matrix`)
  - F4: AX-1     MCP `submit` Tool Matrix Support
  - F5: FEAT-5   Process Task Checkpoints (`OXIDESWARM_CHECKPOINT_DIR`, `OXCP1`)
  - F6: MESH-2   Agent Mesh Consumer Delivery (`--inbox-dir`, ACK/NACK, Telemetry)

Strictly opaque-box: zero internal Rust crate imports, exercises compiled
`oxideswarm` and `agent-mesh` binaries using Python stdlib `unittest` only.

Usage:
  python3 -m unittest tests/test_wave2_e2e.py -v
  python3 tests/test_wave2_e2e.py -v
  OXIDESWARM_E2E_TIER=1 python3 -m unittest tests/test_wave2_e2e.py -v
  OXIDESWARM_E2E_TIER=1,2 OXIDESWARM_PROFILE=release python3 tests/test_wave2_e2e.py -v
"""

from __future__ import annotations

import base64
import collections
import concurrent.futures
import hashlib
import json
import os
import pathlib
import queue
import re
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent

SCRUB_ENV_PREFIXES = ("RUSTY_GRID_", "OXIDESWARM_", "OXIDE_", "OX_SHARD_")
SCRUB_ENV_EXACT = (
    "OXIDE_MESH_TOKEN",
    "OXIDE_MESH_TLS_CA",
    "OXIDE_GRID_MASTER",
    "CARGO_MANIFEST_DIR",
    "GITHUB_STEP_SUMMARY",
)

EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


# ---------------------------------------------------------------------------
# Binary Discovery & Tier Selection
# ---------------------------------------------------------------------------


def find_binary(name: str) -> Path:
    """Locate compiled binary without touching Cargo at test import time."""
    env_map = {
        "oxideswarm": "OXIDESWARM_BIN",
        "agent-mesh": "AGENT_MESH_BIN",
    }
    override_var = env_map.get(name)
    if override_var:
        override = os.environ.get(override_var, "").strip()
        if override:
            p = Path(override).expanduser()
            if p.is_file() and os.access(p, os.X_OK):
                return p
            raise RuntimeError(
                f"{override_var}={override!r} does not point to an executable binary"
            )

    target_dir = Path(
        os.environ.get("CARGO_TARGET_DIR") or (REPO_ROOT / "target")
    ).expanduser()

    profile_override = os.environ.get("OXIDESWARM_PROFILE", "").strip().lower()
    if profile_override in ("release", "debug"):
        profiles = [profile_override]
    else:
        profiles = ["release", "debug"]

    exe_names = [f"{name}.exe", name] if os.name == "nt" else [name]
    candidates: list[Path] = []
    for prof in profiles:
        for exe in exe_names:
            cand = target_dir / prof / exe
            if cand.is_file() and os.access(cand, os.X_OK):
                candidates.append(cand)

    if not candidates:
        raise RuntimeError(
            f"Compiled binary '{name}' not found under {target_dir}/{{{','.join(profiles)}}}. "
            f"Build workspace binaries first or set {override_var or 'CARGO_TARGET_DIR'}."
        )

    if profile_override in ("release", "debug"):
        return candidates[0]

    # Prefer newest built binary across release/debug so latest build is tested.
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def _parse_selected_tiers() -> set[int]:
    raw = os.environ.get("OXIDESWARM_E2E_TIER", "1,2,3,4").strip()
    if not raw or raw.lower() == "all":
        return {1, 2, 3, 4}
    tiers: set[int] = set()
    for part in raw.split(","):
        token = part.strip()
        if not token:
            continue
        if token.lower().startswith("tier"):
            token = token[4:]
        val = int(token)
        if val not in (1, 2, 3, 4):
            raise ValueError(f"Unsupported OXIDESWARM_E2E_TIER value: {part!r}")
        tiers.add(val)
    return tiers or {1, 2, 3, 4}


def load_tests(
    loader: unittest.TestLoader,
    standard_tests: unittest.TestSuite,
    pattern: Optional[str],
) -> unittest.TestSuite:
    """Filter test classes by OXIDESWARM_E2E_TIER when module is loaded."""
    selected = _parse_selected_tiers()
    tier_classes = {
        1: Tier1FeatureCoverageTests,
        2: Tier2BoundaryCornerTests,
        3: Tier3CrossFeatureTests,
        4: Tier4WorkloadScenarios,
    }
    suite = unittest.TestSuite()
    for tier_num in sorted(selected):
        suite.addTests(loader.loadTestsFromTestCase(tier_classes[tier_num]))
    return suite


# ---------------------------------------------------------------------------
# Process, Isolation & Network Helpers
# ---------------------------------------------------------------------------


@dataclass
class CliResult:
    returncode: int
    stdout: str
    stderr: str
    elapsed_s: float


class ManagedProcess:
    """Tracks an exact Popen handle with non-blocking daemon stdout/stderr drainers."""

    def __init__(
        self,
        name: str,
        proc: subprocess.Popen[str],
        stdout_lines: Optional[collections.deque[str]] = None,
        stderr_lines: Optional[collections.deque[str]] = None,
    ) -> None:
        self.name = name
        self.proc = proc
        self.stdout_lines = (
            stdout_lines if stdout_lines is not None else collections.deque(maxlen=2000)
        )
        self.stderr_lines = (
            stderr_lines if stderr_lines is not None else collections.deque(maxlen=2000)
        )
        self._threads: list[threading.Thread] = []
        self._stdout_callbacks: list[Callable[[str], None]] = []
        self._lock = threading.Lock()

    def start_draining(
        self,
        drain_stdout: bool = True,
        drain_stderr: bool = True,
        on_stdout_line: Optional[Callable[[str], None]] = None,
    ) -> None:
        if on_stdout_line is not None:
            with self._lock:
                self._stdout_callbacks.append(on_stdout_line)

        if drain_stdout and self.proc.stdout is not None:
            t = threading.Thread(
                target=self._drain_stream,
                args=(self.proc.stdout, self.stdout_lines, True),
                daemon=True,
                name=f"{self.name}-stdout",
            )
            t.start()
            self._threads.append(t)

        if drain_stderr and self.proc.stderr is not None:
            t = threading.Thread(
                target=self._drain_stream,
                args=(self.proc.stderr, self.stderr_lines, False),
                daemon=True,
                name=f"{self.name}-stderr",
            )
            t.start()
            self._threads.append(t)

    def _drain_stream(
        self,
        stream: Any,
        buf: collections.deque[str],
        is_stdout: bool,
    ) -> None:
        try:
            for line in stream:
                stripped = line.rstrip("\r\n")
                buf.append(stripped)
                if is_stdout:
                    with self._lock:
                        cbs = list(self._stdout_callbacks)
                    for cb in cbs:
                        try:
                            cb(stripped)
                        except Exception:
                            pass
        except Exception:
            pass
        finally:
            try:
                stream.close()
            except Exception:
                pass

    def poll(self) -> Optional[int]:
        return self.proc.poll()

    def send_signal(self, sig: int) -> None:
        if self.proc.poll() is None:
            self.proc.send_signal(sig)

    def wait(self, timeout: float = 10.0) -> int:
        return self.proc.wait(timeout=timeout)

    def stop(self, sig: int = signal.SIGTERM, timeout: float = 5.0) -> Optional[int]:
        if self.proc.poll() is None:
            try:
                self.proc.send_signal(sig)
            except ProcessLookupError:
                pass
            try:
                self.proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                try:
                    self.proc.kill()
                except ProcessLookupError:
                    pass
                try:
                    self.proc.wait(timeout=3.0)
                except subprocess.TimeoutExpired:
                    pass
        return self.proc.poll()

    def logs(self) -> str:
        out = "\n".join(self.stdout_lines)
        err = "\n".join(self.stderr_lines)
        return (
            f"=== {self.name} (pid={self.proc.pid}, rc={self.proc.poll()}) ===\n"
            f"--- stdout ---\n{out}\n--- stderr ---\n{err}"
        )


class IsolatedEnv:
    """Per-test isolated filesystem, scrubbed environment, and strict Popen registry."""

    def __init__(self, prefix: str = "oxs_w2_") -> None:
        self._tmpdir_obj = tempfile.TemporaryDirectory(prefix=prefix)
        self.root = Path(self._tmpdir_obj.name).resolve()
        self.home = self.root / "home"
        self.ox_home = self.root / "ox_home"
        self.fake_bin = self.root / ".fake_bin"
        self.tmp = self.root / "tmp"
        self.work = self.root / "work"
        for d in (self.home, self.ox_home, self.fake_bin, self.tmp, self.work):
            d.mkdir(parents=True, exist_ok=True)

        pmset = self.fake_bin / "pmset"
        pmset.write_text("#!/bin/sh\necho \"Now drawing from 'AC Power'\"\nexit 0\n", encoding="utf-8")
        pmset.chmod(0o755)

        self._procs: list[ManagedProcess] = []
        self._cleanups: list[Callable[[], None]] = []

    def add_cleanup(self, fn: Callable[[], None]) -> None:
        self._cleanups.append(fn)

    def register_process(self, mp: ManagedProcess) -> ManagedProcess:
        self._procs.append(mp)
        return mp

    def base_env(
        self,
        extra: Optional[dict[str, str]] = None,
        home_dir: Optional[Path] = None,
        ox_home_dir: Optional[Path] = None,
        scrub_vars: Iterable[str] = (),
    ) -> dict[str, str]:
        env: dict[str, str] = {}
        for k, v in os.environ.items():
            if any(k.startswith(p) for p in SCRUB_ENV_PREFIXES):
                continue
            if k in SCRUB_ENV_EXACT:
                continue
            env[k] = v

        orig_home = os.environ.get("HOME") or os.environ.get("USERPROFILE")
        if "RUSTUP_HOME" not in env and orig_home:
            cand = os.path.join(orig_home, ".rustup")
            if os.path.isdir(cand):
                env["RUSTUP_HOME"] = cand
        if "CARGO_HOME" not in env and orig_home:
            cand = os.path.join(orig_home, ".cargo")
            if os.path.isdir(cand):
                env["CARGO_HOME"] = cand

        h = str(home_dir or self.home)
        oxh = str(ox_home_dir or self.ox_home)
        env["HOME"] = h
        env["USERPROFILE"] = h
        env["OXIDESWARM_HOME"] = oxh
        env["TMPDIR"] = str(self.tmp)
        env["XDG_CONFIG_HOME"] = str(self.root / "xdg_config")
        env["XDG_CACHE_HOME"] = str(self.root / "xdg_cache")
        env["XDG_DATA_HOME"] = str(self.root / "xdg_data")
        env["NO_COLOR"] = "1"
        env["RUSTY_GRID_NO_BATTERY"] = "1"
        env["RUSTY_GRID_DISABLE_DISCOVERY"] = "1"
        env["RUSTY_GRID_P2P_MINIMAL"] = "1"
        env["PATH"] = f"{self.fake_bin}{os.pathsep}{env.get('PATH', '')}"

        for k in scrub_vars:
            env.pop(k, None)
        if extra:
            env.update(extra)
        return env

    def run_cmd(
        self,
        argv: list[str],
        cwd: Optional[Path] = None,
        env_extra: Optional[dict[str, str]] = None,
        home_dir: Optional[Path] = None,
        ox_home_dir: Optional[Path] = None,
        timeout: float = 30.0,
        stdin_text: Optional[str] = None,
        stdin_bytes: Optional[bytes] = None,
    ) -> CliResult:
        env = self.base_env(extra=env_extra, home_dir=home_dir, ox_home_dir=ox_home_dir)
        t0 = time.monotonic()
        if stdin_bytes is not None:
            res_b = subprocess.run(
                argv,
                cwd=str(cwd or self.work),
                env=env,
                input=stdin_bytes,
                capture_output=True,
                timeout=timeout,
            )
            return CliResult(
                returncode=res_b.returncode,
                stdout=res_b.stdout.decode("utf-8", errors="replace"),
                stderr=res_b.stderr.decode("utf-8", errors="replace"),
                elapsed_s=time.monotonic() - t0,
            )
        res = subprocess.run(
            argv,
            cwd=str(cwd or self.work),
            env=env,
            input=stdin_text,
            text=True,
            capture_output=True,
            timeout=timeout,
        )
        return CliResult(
            returncode=res.returncode,
            stdout=res.stdout,
            stderr=res.stderr,
            elapsed_s=time.monotonic() - t0,
        )

    def spawn(
        self,
        name: str,
        argv: list[str],
        cwd: Optional[Path] = None,
        env_extra: Optional[dict[str, str]] = None,
        home_dir: Optional[Path] = None,
        ox_home_dir: Optional[Path] = None,
        stdin_pipe: bool = False,
        drain_stdout: bool = True,
        drain_stderr: bool = True,
        on_stdout_line: Optional[Callable[[str], None]] = None,
    ) -> ManagedProcess:
        env = self.base_env(extra=env_extra, home_dir=home_dir, ox_home_dir=ox_home_dir)
        proc = subprocess.Popen(
            argv,
            cwd=str(cwd or self.work),
            env=env,
            stdin=subprocess.PIPE if stdin_pipe else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        mp = ManagedProcess(name=name, proc=proc)
        mp.start_draining(
            drain_stdout=drain_stdout,
            drain_stderr=drain_stderr,
            on_stdout_line=on_stdout_line,
        )
        return self.register_process(mp)

    def close(self) -> None:
        for fn in reversed(self._cleanups):
            try:
                fn()
            except Exception:
                pass
        for mp in reversed(self._procs):
            try:
                mp.stop(sig=signal.SIGTERM, timeout=3.0)
            except Exception:
                pass
        shutil.rmtree(self.root, ignore_errors=True)
        try:
            self._tmpdir_obj.cleanup()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# HTTP & Prometheus Helpers
# ---------------------------------------------------------------------------


def http_get_text(
    url: str,
    headers: Optional[dict[str, str]] = None,
    timeout: float = 5.0,
) -> tuple[int, str]:
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        return exc.code, body


def http_get_json(
    url: str,
    headers: Optional[dict[str, str]] = None,
    timeout: float = 5.0,
) -> Any:
    status, body = http_get_text(url, headers=headers, timeout=timeout)
    if status != 200:
        raise RuntimeError(f"GET {url} returned HTTP {status}: {body}")
    return json.loads(body)


def parse_prom_metric(metrics_text: str, metric_name: str) -> Optional[float]:
    for line in metrics_text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if s.startswith(metric_name + " ") or s.startswith(metric_name + "{"):
            parts = s.split()
            if len(parts) >= 2:
                try:
                    return float(parts[-1])
                except ValueError:
                    return None
    return None


# ---------------------------------------------------------------------------
# Stdlib RFC 6455 WebSocket Client (Zero Third-Party Dependencies)
# ---------------------------------------------------------------------------


class StdlibWsClient:
    """Minimal, RFC 6455 compliant WebSocket client using Python stdlib sockets only."""

    _GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

    def __init__(self, ws_url: str, timeout: float = 10.0) -> None:
        if not ws_url.startswith("ws://"):
            raise ValueError(f"Only ws:// is supported by StdlibWsClient, got {ws_url!r}")
        rest = ws_url[5:]
        if "/" in rest:
            hostport, path = rest.split("/", 1)
            path = "/" + path
        else:
            hostport, path = rest, "/"
        if ":" in hostport:
            host, port_s = hostport.rsplit(":", 1)
            port = int(port_s)
        else:
            host, port = hostport, 80

        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        self._rx_buf = bytearray()
        self._tx_lock = threading.Lock()
        self.node_id: Optional[str] = None

        key_bytes = os.urandom(16)
        ws_key = base64.b64encode(key_bytes).decode("ascii")
        req = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            f"Upgrade: websocket\r\n"
            f"Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {ws_key}\r\n"
            f"Sec-WebSocket-Version: 13\r\n"
            f"\r\n"
        )
        self.sock.sendall(req.encode("ascii"))

        while b"\r\n\r\n" not in self._rx_buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("EOF during WebSocket HTTP handshake")
            self._rx_buf.extend(chunk)

        hdr_end = self._rx_buf.index(b"\r\n\r\n")
        raw_hdr = bytes(self._rx_buf[:hdr_end]).decode("latin1")
        del self._rx_buf[: hdr_end + 4]

        lines = raw_hdr.split("\r\n")
        if "101" not in lines[0]:
            raise ConnectionError(f"WebSocket upgrade rejected: {lines[0]!r}")

        expected_accept = base64.b64encode(
            hashlib.sha1((ws_key + self._GUID).encode("ascii")).digest()
        ).decode("ascii")
        headers = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()
        if headers.get("sec-websocket-accept") != expected_accept:
            raise ConnectionError(
                f"Invalid Sec-WebSocket-Accept: {headers.get('sec-websocket-accept')!r}"
            )

    def _recv_exact(self, n: int) -> bytes:
        while len(self._rx_buf) < n:
            chunk = self.sock.recv(max(4096, n - len(self._rx_buf)))
            if not chunk:
                raise EOFError("WebSocket connection closed by peer")
            self._rx_buf.extend(chunk)
        out = bytes(self._rx_buf[:n])
        del self._rx_buf[:n]
        return out

    def _send_frame(self, opcode: int, payload: bytes) -> None:
        mask_key = os.urandom(4)
        header = bytearray()
        header.append(0x80 | (opcode & 0x0F))
        length = len(payload)
        if length < 126:
            header.append(0x80 | length)
        elif length < 65536:
            header.append(0x80 | 126)
            header.extend(struct.pack("!H", length))
        else:
            header.append(0x80 | 127)
            header.extend(struct.pack("!Q", length))
        header.extend(mask_key)
        masked = bytes(b ^ mask_key[i & 3] for i, b in enumerate(payload))
        with self._tx_lock:
            self.sock.sendall(bytes(header) + masked)

    def send_text(self, text: str) -> None:
        self._send_frame(0x1, text.encode("utf-8"))

    def send_json(self, obj: dict[str, Any]) -> None:
        self.send_text(json.dumps(obj))

    def recv_text(self, timeout: float = 10.0) -> str:
        self.sock.settimeout(timeout)
        fragments: list[bytes] = []
        while True:
            b1, b2 = self._recv_exact(2)
            fin = (b1 & 0x80) != 0
            opcode = b1 & 0x0F
            masked = (b2 & 0x80) != 0
            length = b2 & 0x7F
            if length == 126:
                length = struct.unpack("!H", self._recv_exact(2))[0]
            elif length == 127:
                length = struct.unpack("!Q", self._recv_exact(8))[0]
            mask = self._recv_exact(4) if masked else b""
            payload = self._recv_exact(length)
            if masked:
                payload = bytes(b ^ mask[i & 3] for i, b in enumerate(payload))

            if opcode == 0x9:  # Ping
                self._send_frame(0xA, payload)
                continue
            if opcode == 0xA:  # Pong
                continue
            if opcode == 0x8:  # Close
                try:
                    self._send_frame(0x8, b"")
                except Exception:
                    pass
                raise EOFError("WebSocket close frame received")

            if opcode in (0x1, 0x2, 0x0):
                fragments.append(payload)
                if fin:
                    return b"".join(fragments).decode("utf-8", errors="replace")

    def recv_json(self, timeout: float = 10.0) -> dict[str, Any]:
        raw = self.recv_text(timeout=timeout)
        return json.loads(raw)

    def recv_until(
        self,
        predicate: Callable[[dict[str, Any]], bool],
        timeout: float = 10.0,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while True:
            rem = deadline - time.monotonic()
            if rem <= 0:
                raise TimeoutError("Timed out waiting for matching WebSocket message")
            msg = self.recv_json(timeout=rem)
            if predicate(msg):
                return msg

    def register(
        self,
        node_id: str,
        platform: str = "macos",
        token: Optional[str] = None,
    ) -> dict[str, Any]:
        self.node_id = node_id
        corr = str(uuid.uuid4())
        reg: dict[str, Any] = {
            "type": "NodeRegistration",
            "correlation_id": corr,
            "from": node_id,
            "to": "hub",
            "platform": platform,
            "hostname": "wave2-e2e",
            "capabilities": {"supports_data": True},
        }
        if token is not None:
            reg["token"] = token
        self.send_json(reg)
        ack = self.recv_until(
            lambda m: m.get("type") in ("NodeRegistrationAck", "DeliveryNack")
            and m.get("correlation_id") == corr,
            timeout=10.0,
        )
        if ack.get("type") != "NodeRegistrationAck":
            raise RuntimeError(f"Registration failed for {node_id}: {ack}")
        return ack

    def send_data_payload(
        self,
        to_node: str,
        raw_bytes: bytes,
        checksum_override: Optional[str] = None,
        correlation_id: Optional[str] = None,
        sequence_number: int = 0,
        total_chunks: int = 1,
        from_node: Optional[str] = None,
    ) -> str:
        corr = correlation_id or str(uuid.uuid4())
        sender = from_node or self.node_id or "ws-sender"
        digest = (
            checksum_override
            if checksum_override is not None
            else hashlib.sha256(raw_bytes).hexdigest()
        )
        latin1_text = raw_bytes.decode("latin1")
        env = {
            "type": "DataPayload",
            "correlation_id": corr,
            "from": sender,
            "to": to_node,
            "payload_type": "raw_bytes",
            "sequence_number": sequence_number,
            "total_chunks": total_chunks,
            "chunk_size_bytes": len(raw_bytes),
            "data": latin1_text,
            "checksum_sha256": digest,
        }
        self.send_json(env)
        return corr

    def close(self) -> None:
        try:
            self._send_frame(0x8, b"")
        except Exception:
            pass
        try:
            self.sock.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# OxideSwarm Cluster, MCP Client & Agent Mesh Harnesses
# ---------------------------------------------------------------------------


@dataclass
class MasterInstance:
    proc: ManagedProcess
    listen_addr: str
    port: int
    dashboard_port: Optional[int]
    state_dir: Path
    blob_dir: Path


@dataclass
class WorkerInstance:
    proc: ManagedProcess
    name: str
    sandbox_dir: Path


class OxideCluster:
    """High-level opaque-box harness around compiled `oxideswarm` binaries."""

    def __init__(self, env: IsolatedEnv) -> None:
        self.env = env
        self.bin = str(find_binary("oxideswarm"))
        self.secret_file: Optional[Path] = None

    def _base_argv(self, secret_file: Optional[Path] = None) -> list[str]:
        argv = [self.bin]
        sf = secret_file or self.secret_file
        if sf is not None:
            argv.extend(["--secret-file", str(sf)])
        return argv

    def generate_secret_file(self, path: Optional[Path] = None) -> Path:
        target = path or (self.env.root / "cluster.secret")
        res = self.env.run_cmd([self.bin, "secret", "generate", "--out", str(target)])
        if res.returncode != 0 or not target.is_file():
            raise RuntimeError(
                f"Failed to generate secret: rc={res.returncode}\n{res.stdout}\n{res.stderr}"
            )
        return target

    def start_master(
        self,
        name: str = "master",
        listen: str = "127.0.0.1:0",
        state_dir: Optional[Path] = None,
        blob_dir: Optional[Path] = None,
        secret_file: Optional[Path] = None,
        heartbeat_interval_secs: Optional[int] = None,
        heartbeat_timeout_secs: Optional[int] = None,
        reaper_interval_secs: Optional[int] = None,
        session_resume_hold_secs: Optional[int] = None,
        default_retry_max: Optional[int] = None,
        max_active_mb: Optional[int] = None,
        extra_args: Optional[list[str]] = None,
        timeout: float = 15.0,
    ) -> MasterInstance:
        tag = f"{name}-{uuid.uuid4().hex[:6]}"
        sd = state_dir or (self.env.root / f"state_{tag}")
        bd = blob_dir or (sd / "blobs")
        sd.mkdir(parents=True, exist_ok=True)
        bd.mkdir(parents=True, exist_ok=True)

        port_file = self.env.root / f"{tag}.port"
        dash_port_file = self.env.root / f"{tag}.dash_port"
        for pf in (port_file, dash_port_file):
            if pf.exists():
                pf.unlink()

        argv = self._base_argv(secret_file) + [
            "master",
            "--listen",
            listen,
            "--port-file",
            str(port_file),
            "--dashboard-port",
            "0",
            "--dashboard-port-file",
            str(dash_port_file),
            "--discovery-port",
            "0",
            "--state-dir",
            str(sd),
            "--blob-dir",
            str(bd),
            "--max-host-cpu-pct",
            "100",
        ]
        if heartbeat_interval_secs is not None:
            argv.extend(["--heartbeat-interval-secs", str(heartbeat_interval_secs)])
        if heartbeat_timeout_secs is not None:
            argv.extend(["--heartbeat-timeout-secs", str(heartbeat_timeout_secs)])
        if reaper_interval_secs is not None:
            argv.extend(["--reaper-interval-secs", str(reaper_interval_secs)])
        if session_resume_hold_secs is not None:
            argv.extend(["--session-resume-hold-secs", str(session_resume_hold_secs)])
        if default_retry_max is not None:
            argv.extend(["--default-retry-max", str(default_retry_max)])
        if max_active_mb is not None:
            argv.extend(["--max-active-mb", str(max_active_mb)])
        if extra_args:
            argv.extend(extra_args)

        mp = self.env.spawn(name=name, argv=argv)
        deadline = time.monotonic() + timeout
        bound_port: Optional[int] = None
        dash_port: Optional[int] = None
        while time.monotonic() < deadline:
            if mp.poll() is not None:
                raise RuntimeError(f"Master exited prematurely:\n{mp.logs()}")
            if port_file.is_file():
                raw = port_file.read_text(encoding="utf-8").strip()
                if raw.isdigit():
                    bound_port = int(raw)
            if dash_port_file.is_file():
                raw_d = dash_port_file.read_text(encoding="utf-8").strip()
                if raw_d.isdigit():
                    dash_port = int(raw_d)
            if bound_port is not None and dash_port is not None:
                break
            time.sleep(0.03)

        if bound_port is None:
            raise TimeoutError(
                f"Master did not write port file within {timeout}s:\n{mp.logs()}"
            )

        return MasterInstance(
            proc=mp,
            listen_addr=f"127.0.0.1:{bound_port}",
            port=bound_port,
            dashboard_port=dash_port,
            state_dir=sd,
            blob_dir=bd,
        )

    def workers_json(
        self, master_addr: str, secret_file: Optional[Path] = None
    ) -> list[dict[str, Any]]:
        argv = self._base_argv(secret_file) + [
            "workers",
            "--master",
            master_addr,
            "--json",
        ]
        res = self.env.run_cmd(argv, timeout=10.0)
        if res.returncode != 0:
            raise RuntimeError(
                f"workers --json failed (rc={res.returncode}): {res.stderr}"
            )
        return json.loads(res.stdout)

    def start_worker(
        self,
        master_addr: str,
        name: str,
        tags: Iterable[str] = (),
        isolation: str = "off",
        workspaces: bool = False,
        workspace_root: Optional[Path] = None,
        sandbox_base_dir: Optional[Path] = None,
        home_dir: Optional[Path] = None,
        ox_home_dir: Optional[Path] = None,
        secret_file: Optional[Path] = None,
        cores: int = 2,
        session_resume_grace_secs: Optional[int] = None,
        extra_args: Optional[list[str]] = None,
        wait_connected: bool = True,
        timeout: float = 15.0,
    ) -> WorkerInstance:
        sb = sandbox_base_dir or (self.env.root / f"sb_{name}_{uuid.uuid4().hex[:6]}")
        sb.mkdir(mode=0o700, parents=True, exist_ok=True)
        sb.chmod(0o700)
        wh = home_dir or (self.env.root / f"home_{name}")
        wh.mkdir(mode=0o700, parents=True, exist_ok=True)

        argv = self._base_argv(secret_file) + [
            "worker",
            "--master",
            master_addr,
            "--name",
            name,
            "--isolation",
            isolation,
            "--no-gpu",
            "--no-cuda",
            "--no-blender",
            "--no-portal",
            "--cores",
            str(cores),
            "--sandbox-base-dir",
            str(sb),
        ]
        for t in tags:
            argv.extend(["--tag", t])
        if workspaces:
            ws_root = workspace_root or (self.env.root / f"ws_{name}")
            ws_root.mkdir(mode=0o700, parents=True, exist_ok=True)
            ws_root.chmod(0o700)
            argv.extend(["--workspaces", "--workspace-root", str(ws_root)])
        if session_resume_grace_secs is not None:
            argv.extend(["--session-resume-grace-secs", str(session_resume_grace_secs)])
        if extra_args:
            argv.extend(extra_args)

        t_spawn = time.monotonic()
        mp = self.env.spawn(
            name=f"worker-{name}",
            argv=argv,
            home_dir=wh,
            ox_home_dir=ox_home_dir,
        )

        if wait_connected:
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if mp.poll() is not None:
                    raise RuntimeError(
                        f"Worker {name} exited prematurely:\n{mp.logs()}"
                    )
                try:
                    wlist = self.workers_json(master_addr, secret_file=secret_file)
                    for w in wlist:
                        if w.get("name") == name and "state:connected" in w.get("tags", []):
                            print(f"[timing] {name} connected after {time.monotonic() - t_spawn:.2f} s", flush=True)
                            return WorkerInstance(proc=mp, name=name, sandbox_dir=sb)
                except Exception:
                    pass
                time.sleep(0.05)
            raise TimeoutError(
                f"Worker {name} did not reach state:connected within {timeout}s:\n{mp.logs()}"
            )

        return WorkerInstance(proc=mp, name=name, sandbox_dir=sb)

    def submit_cli(
        self,
        master_addr: str,
        args: list[str],
        cwd: Optional[Path] = None,
        secret_file: Optional[Path] = None,
        timeout: float = 45.0,
    ) -> CliResult:
        argv = self._base_argv(secret_file) + [
            "submit",
            "--master",
            master_addr,
        ] + args
        return self.env.run_cmd(argv, cwd=cwd, timeout=timeout)

    def submit_cli_popen(
        self,
        master_addr: str,
        args: list[str],
        cwd: Optional[Path] = None,
        secret_file: Optional[Path] = None,
    ) -> ManagedProcess:
        argv = self._base_argv(secret_file) + [
            "submit",
            "--master",
            master_addr,
        ] + args
        return self.env.spawn(name="submit-bg", argv=argv, cwd=cwd)

    def result_cli(
        self,
        master_addr: str,
        task_id: str,
        wait: bool = True,
        json_out: bool = True,
        output_dir: Optional[Path] = None,
        secret_file: Optional[Path] = None,
        timeout: float = 30.0,
    ) -> CliResult:
        argv = self._base_argv(secret_file) + ["result", task_id, "--master", master_addr]
        if wait:
            argv.append("--wait")
        if json_out:
            argv.append("--json")
        if output_dir is not None:
            argv.extend(["--output-dir", str(output_dir)])
        return self.env.run_cmd(argv, timeout=timeout)

    def status_cli(
        self,
        master_addr: str,
        task_id: Optional[str] = None,
        workers: bool = False,
        json_out: bool = True,
        secret_file: Optional[Path] = None,
    ) -> CliResult:
        argv = self._base_argv(secret_file) + ["status", "--master", master_addr]
        if task_id:
            argv.extend(["--task-id", task_id])
        if workers:
            argv.append("--workers")
        if json_out:
            argv.append("--json")
        return self.env.run_cmd(argv, timeout=15.0)


class McpClient:
    """Newline-delimited JSON-RPC 2.0 client over `oxideswarm mcp` stdio."""

    def __init__(
        self,
        env: IsolatedEnv,
        master_addr: str,
        cwd: Optional[Path] = None,
        secret_file: Optional[Path] = None,
        extra_args: Optional[list[str]] = None,
    ) -> None:
        self.env = env
        bin_path = str(find_binary("oxideswarm"))
        argv = [bin_path]
        if secret_file is not None:
            argv.extend(["--secret-file", str(secret_file)])
        argv.extend(["mcp", "--master", master_addr])
        if extra_args:
            argv.extend(extra_args)

        self._line_q: queue.Queue[Optional[str]] = queue.Queue()
        self.all_stdout_lines: list[str] = []
        self.mp = env.spawn(
            name="oxideswarm-mcp",
            argv=argv,
            cwd=cwd,
            stdin_pipe=True,
            drain_stdout=True,
            drain_stderr=True,
            on_stdout_line=self._on_line,
        )
        self._next_id = 1

    def _on_line(self, line: str) -> None:
        self._line_q.put(line)

    def send(self, obj: dict[str, Any]) -> None:
        assert self.mp.proc.stdin is not None
        payload = json.dumps(obj, separators=(",", ":")) + "\n"
        self.mp.proc.stdin.write(payload)
        self.mp.proc.stdin.flush()

    def recv_jsonrpc(self, timeout: float = 20.0) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while True:
            rem = deadline - time.monotonic()
            if rem <= 0:
                raise TimeoutError(f"MCP stdout timed out after {timeout}s:\n{self.mp.logs()}")
            try:
                line = self._line_q.get(timeout=min(rem, 0.25))
            except queue.Empty:
                if self.mp.poll() is not None and self._line_q.empty():
                    raise EOFError(f"MCP server exited (rc={self.mp.poll()}):\n{self.mp.logs()}")
                continue
            if line is None:
                raise EOFError("MCP stdout closed")
            stripped = line.strip()
            if not stripped:
                continue
            self.all_stdout_lines.append(stripped)
            parsed = json.loads(stripped)
            if parsed.get("jsonrpc") != "2.0":
                raise AssertionError(f"Invalid JSON-RPC line: {parsed!r}")
            return parsed

    def request(self, method: str, params: Optional[dict[str, Any]] = None, timeout: float = 20.0) -> dict[str, Any]:
        req_id = self._next_id
        self._next_id += 1
        msg: dict[str, Any] = {"jsonrpc": "2.0", "id": req_id, "method": method}
        if params is not None:
            msg["params"] = params
        self.send(msg)
        while True:
            resp = self.recv_jsonrpc(timeout=timeout)
            if resp.get("id") == req_id:
                return resp

    def initialize(self) -> dict[str, Any]:
        resp = self.request(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "clientInfo": {"name": "wave2-e2e", "version": "1.0"},
            },
        )
        self.send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        return resp

    def ping(self, timeout: float = 5.0) -> dict[str, Any]:
        return self.request("ping", timeout=timeout)

    def tools_list(self) -> list[dict[str, Any]]:
        resp = self.request("tools/list")
        if "result" not in resp or "tools" not in resp["result"]:
            raise AssertionError(f"Unexpected tools/list response: {resp!r}")
        return resp["result"]["tools"]

    def call_tool(
        self, name: str, arguments: dict[str, Any], timeout: float = 40.0
    ) -> dict[str, Any]:
        return self.request(
            "tools/call",
            {"name": name, "arguments": arguments},
            timeout=timeout,
        )

    def close_stdin(self) -> None:
        if self.mp.proc.stdin is not None:
            try:
                self.mp.proc.stdin.close()
            except Exception:
                pass


@dataclass
class MeshHubInstance:
    proc: ManagedProcess
    ws_url: str
    http_url: str
    token: Optional[str]


class MeshCluster:
    """Opaque-box harness around compiled `agent-mesh` binary."""

    def __init__(self, env: IsolatedEnv) -> None:
        self.env = env
        self.bin = str(find_binary("agent-mesh"))

    def start_hub(
        self,
        token: Optional[str] = None,
        insecure: bool = False,
        extra_args: Optional[list[str]] = None,
        timeout: float = 15.0,
    ) -> MeshHubInstance:
        argv = [self.bin, "hub", "--listen", "127.0.0.1:0"]
        if token is not None:
            argv.extend(["--token", token])
        if insecure:
            argv.append("--insecure")
        if extra_args:
            argv.extend(extra_args)

        url_q: queue.Queue[str] = queue.Queue()

        def _on_line(line: str) -> None:
            if "running at " in line:
                url_q.put(line.split("running at ", 1)[1].strip())

        mp = self.env.spawn(name="mesh-hub", argv=argv, on_stdout_line=_on_line)
        try:
            ws_url = url_q.get(timeout=timeout)
        except queue.Empty as exc:
            raise TimeoutError(
                f"agent-mesh hub did not print ws URL within {timeout}s:\n{mp.logs()}"
            ) from exc

        http_url = "http://" + ws_url.split("://", 1)[1].rsplit("/ws", 1)[0]
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            if mp.poll() is not None:
                raise RuntimeError(f"agent-mesh hub exited prematurely:\n{mp.logs()}")
            try:
                status, _ = http_get_text(f"{http_url}/api/health", timeout=1.0)
                if status == 200:
                    return MeshHubInstance(
                        proc=mp, ws_url=ws_url, http_url=http_url, token=token
                    )
            except Exception:
                pass
            time.sleep(0.05)
        raise TimeoutError(f"agent-mesh hub /api/health never returned 200:\n{mp.logs()}")

    def start_node(
        self,
        hub: MeshHubInstance,
        node_id: str,
        inbox_dir: Optional[Path] = None,
        platform: str = "macos",
        token: Optional[str] = None,
        no_shell: bool = False,
        wait_registered: bool = True,
        timeout: float = 10.0,
    ) -> ManagedProcess:
        argv = [
            self.bin,
            "node",
            "--hub",
            hub.ws_url,
            "--id",
            node_id,
            "--platform",
            platform,
        ]
        if inbox_dir is not None:
            argv.extend(["--inbox-dir", str(inbox_dir)])
        tok = token if token is not None else hub.token
        if tok is not None:
            argv.extend(["--token", tok])
        if no_shell:
            argv.append("--no-shell")

        mp = self.env.spawn(name=f"mesh-node-{node_id}", argv=argv)
        if not wait_registered:
            return mp

        headers = {"Authorization": f"Bearer {tok}"} if tok else {}
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if mp.poll() is not None:
                raise RuntimeError(
                    f"agent-mesh node {node_id} exited prematurely:\n{mp.logs()}"
                )
            try:
                nodes = http_get_json(f"{hub.http_url}/api/nodes", headers=headers, timeout=1.0)
                if any(n.get("node_id") == node_id for n in nodes):
                    return mp
            except Exception:
                pass
            time.sleep(0.05)
        raise TimeoutError(
            f"agent-mesh node {node_id} not visible in /api/nodes:\n{mp.logs()}"
        )

    def run_data(
        self,
        hub: MeshHubInstance,
        to_node: str,
        payload: Optional[str] = None,
        from_node: str = "cli-sender",
        token: Optional[str] = None,
        payload_file: Optional[Path | str] = None,
        stdin_bytes: Optional[bytes] = None,
        extra_args: Optional[list[str]] = None,
        timeout: float = 15.0,
    ) -> CliResult:
        argv = [
            self.bin,
            "data",
            "--hub",
            hub.ws_url,
            "--from",
            from_node,
            "--to",
            to_node,
        ]
        if payload_file is not None:
            argv.extend(["--payload-file", str(payload_file)])
        elif payload is not None:
            argv.extend(["--payload", payload])
        else:
            raise ValueError("run_data requires either payload or payload_file")
        tok = token if token is not None else hub.token
        if tok is not None:
            argv.extend(["--token", tok])
        if extra_args:
            argv.extend(extra_args)
        return self.env.run_cmd(argv, timeout=timeout, stdin_bytes=stdin_bytes)


# ---------------------------------------------------------------------------
# Shared Specification Extraction & Assertion Helpers (Strict, Non-Cheating)
# ---------------------------------------------------------------------------


def _wait_until(
    predicate: Callable[[], Any],
    timeout: float = 15.0,
    interval: float = 0.03,
    desc: str = "condition",
) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        val = predicate()
        if val:
            return val
        time.sleep(interval)
    raise TimeoutError(f"Timed out after {timeout}s waiting for {desc}")


def _extract_matrix_cells(parsed_json: Any) -> list[dict[str, Any]]:
    """Extract cell records strictly from AX-1 CLI or MCP JSON output."""
    if isinstance(parsed_json, list):
        cells = parsed_json
    elif isinstance(parsed_json, dict):
        cells = parsed_json.get("cells")
        if cells is None:
            cells = parsed_json.get("results")
        if cells is None:
            raise AssertionError(
                f"Matrix JSON missing 'cells' or 'results' list: {parsed_json!r}"
            )
    else:
        raise AssertionError(f"Unexpected matrix JSON type {type(parsed_json)}: {parsed_json!r}")

    if not isinstance(cells, list) or not cells:
        raise AssertionError(f"Expected non-empty matrix cell list, got: {cells!r}")
    for c in cells:
        if not isinstance(c, dict):
            raise AssertionError(f"Expected dict cell record, got: {c!r}")
    return cells


def _cell_label_text(cell: dict[str, Any]) -> str:
    for key in ("cell", "label", "tags", "require_tags", "axes", "spec"):
        if key in cell and cell[key] is not None:
            val = cell[key]
            return json.dumps(val, sort_keys=True) if isinstance(val, (dict, list)) else str(val)
    return json.dumps(cell, sort_keys=True)


def _cell_worker(cell: dict[str, Any]) -> str:
    for key in ("worker_name", "worker", "worker_id"):
        v = cell.get(key)
        if v:
            return str(v)
    return ""


def _cell_is_unplaced(cell: dict[str, Any]) -> bool:
    status = str(cell.get("status", "")).lower()
    if "unplaced" in status:
        return True
    if cell.get("unplaced") is True:
        return True
    err = str(cell.get("error", "")).lower()
    return "unplaced" in err or "no connected worker" in err or "no matching worker" in err


def _cell_stdout(cell: dict[str, Any]) -> str:
    parts = [str(cell.get(k, "")) for k in ("stdout", "output_head", "output_tail", "head", "tail")]
    return "\n".join(p for p in parts if p)


def _mcp_tool_text(resp: dict[str, Any]) -> tuple[bool, str]:
    if "error" in resp:
        raise AssertionError(f"Unexpected JSON-RPC error: {resp['error']!r}")
    res = resp.get("result") or {}
    is_err = bool(res.get("isError", False))
    content = res.get("content") or []
    text = "\n".join(str(c.get("text", "")) for c in content if isinstance(c, dict))
    return is_err, text


def _build_mcp_matrix_arg(tools: list[dict[str, Any]], axes: dict[str, list[str]]) -> Any:
    """Adapt matrix input format cleanly from `tools/list` advertised schema."""
    submit_tool = next((t for t in tools if t.get("name") == "submit"), None)
    if submit_tool is None:
        raise AssertionError("MCP tools/list missing 'submit' tool")
    props = submit_tool.get("inputSchema", {}).get("properties", {})
    if "matrix" not in props:
        raise AssertionError("MCP 'submit' schema missing 'matrix' property")
    m_schema = props["matrix"]
    schema_type = m_schema.get("type")
    if schema_type == "array" or (
        isinstance(schema_type, list) and "array" in schema_type and "object" not in schema_type
    ):
        return [f"{k}={','.join(vs)}" for k, vs in axes.items()]
    return axes


def _collect_inbox_payloads(inbox_dir: Path) -> list[tuple[Path, bytes]]:
    """Collect delivered payload files inside `--inbox-dir` (excluding sidecar metadata)."""
    if not inbox_dir.exists():
        return []
    out: list[tuple[Path, bytes]] = []
    for p in sorted(inbox_dir.rglob("*")):
        if not p.is_file():
            continue
        name = p.name
        if name.endswith(".meta.json") or name.endswith(".sha256"):
            continue
        out.append((p, p.read_bytes()))
    return out


def _wait_inbox_for_bytes(
    inbox_dir: Path, expected_bytes: bytes, timeout: float = 5.0
) -> Path:
    def _find() -> Optional[Path]:
        for p, data in _collect_inbox_payloads(inbox_dir):
            if data == expected_bytes:
                return p
        return None

    return _wait_until(
        _find,
        timeout=timeout,
        interval=0.03,
        desc=f"payload ({len(expected_bytes)} bytes) in {inbox_dir}",
    )


def _hardlink_test_script() -> str:
    return (
        'ln "$T_SECRET" "$RUSTY_GRID_SANDBOX_DIR/linked" 2>/dev/null '
        '&& echo LINK_OK || echo LINK_DENIED; '
        'cat "$RUSTY_GRID_SANDBOX_DIR/linked" 2>/dev/null; '
        'echo END'
    )


def _gated_marker_script(marker: Path, gate: Path, done: Path) -> str:
    return (
        f'printf "run pid=$$\\n" >> "{marker}"\n'
        f'while [ ! -f "{gate}" ]; do sleep 0.01; done\n'
        f'touch "{done}"\n'
        f'echo "GATED_OK"\n'
    )


# ---------------------------------------------------------------------------
# Base TestCase with Automatic Isolation
# ---------------------------------------------------------------------------


class Wave2E2ETestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.env = IsolatedEnv()
        self.addCleanup(self.env.close)
        self.ox = OxideCluster(self.env)
        self.mesh = MeshCluster(self.env)

    def _require_darwin_for_seatbelt(self) -> None:
        if sys.platform != "darwin":
            self.skipTest("macOS Seatbelt isolation (--isolation fs/fs+net) requires Darwin")


# ===========================================================================
# TIER 1: Feature Coverage (30 tests — 5 per feature F1-F6)
# ===========================================================================


class Tier1FeatureCoverageTests(Wave2E2ETestBase):
    """Tier 1: Core feature coverage across F1-F6 (5 tests per feature)."""

    # --- F1: TEST-61 macOS Hard-Link Isolation & Lifeline Safety ---

    def test_t1_f1_01_isolation_off_control_hardlink_reads_secret(self) -> None:
        secret_file = self.env.home / "t1_secret.txt"
        secret_val = "oxideswarm-hardlink-secret-t1-01"
        secret_file.write_text(secret_val, encoding="utf-8")

        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-off", isolation="off", tags=["mode-off"])

        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "shell",
                "--require-tag", "mode-off",
                "--env", f"T_SECRET={secret_file}",
                "--wait", "--json",
                "--command", _hardlink_test_script(),
            ],
        )
        self.assertEqual(res.returncode, 0, res.stderr)
        data = json.loads(res.stdout)
        self.assertEqual(data["exit_code"], 0)
        self.assertIn("LINK_OK", data["stdout"])
        self.assertIn(secret_val, data["stdout"])
        self.assertIn("END", data["stdout"])

    def test_t1_f1_02_isolation_fs_blocks_hardlink_to_secret(self) -> None:
        self._require_darwin_for_seatbelt()
        secret_file = self.env.home / "t1_secret_fs.txt"
        secret_val = "oxideswarm-hardlink-secret-t1-02"
        secret_file.write_text(secret_val, encoding="utf-8")

        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-fs", isolation="fs", tags=["mode-fs"])

        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "shell",
                "--require-isolation", "fs",
                "--env", f"T_SECRET={secret_file}",
                "--wait", "--json",
                "--command", _hardlink_test_script(),
            ],
        )
        self.assertEqual(res.returncode, 0, res.stderr)
        data = json.loads(res.stdout)
        self.assertEqual(data["exit_code"], 0)
        self.assertIn("LINK_DENIED", data["stdout"])
        self.assertNotIn(secret_val, data["stdout"])
        self.assertIn("END", data["stdout"])

    def test_t1_f1_03_isolation_fs_net_blocks_hardlink_to_secret(self) -> None:
        self._require_darwin_for_seatbelt()
        secret_file = self.env.home / "t1_secret_fsnet.txt"
        secret_val = "oxideswarm-hardlink-secret-t1-03"
        secret_file.write_text(secret_val, encoding="utf-8")

        master = self.ox.start_master()
        self.ox.start_worker(
            master.listen_addr, name="w-fsnet", isolation="fs+net", tags=["mode-fsnet"]
        )

        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "shell",
                "--require-isolation", "fs+net",
                "--env", f"T_SECRET={secret_file}",
                "--wait", "--json",
                "--command", _hardlink_test_script(),
            ],
        )
        self.assertEqual(res.returncode, 0, res.stderr)
        data = json.loads(res.stdout)
        self.assertEqual(data["exit_code"], 0)
        self.assertIn("LINK_DENIED", data["stdout"])
        self.assertNotIn(secret_val, data["stdout"])
        self.assertIn("END", data["stdout"])

    def test_t1_f1_04_worker_isolation_tags_advertised_accurately(self) -> None:
        self._require_darwin_for_seatbelt()
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-off", isolation="off")
        self.ox.start_worker(master.listen_addr, name="w-fs", isolation="fs")
        self.ox.start_worker(master.listen_addr, name="w-fsnet", isolation="fs+net")

        workers = self.ox.workers_json(master.listen_addr)
        by_name = {w["name"]: set(w.get("tags", [])) for w in workers}
        self.assertIn("iso:off", by_name["w-off"])
        self.assertIn("iso:fs", by_name["w-fs"])
        self.assertIn("iso:fs+net", by_name["w-fsnet"])

    def test_t1_f1_05_concurrent_short_piped_stdout_preserved(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-pipe-1", isolation="off", cores=4)
        if sys.platform == "darwin":
            self.ox.start_worker(master.listen_addr, name="w-pipe-2", isolation="fs", cores=4)

        def _run_one(i: int) -> tuple[int, str]:
            r = self.ox.submit_cli(
                master.listen_addr,
                [
                    "--type", "shell",
                    "--wait", "--json",
                    "--command", f"printf 'PIPE_ITEM_{i:02d}\\n' | cat",
                ],
            )
            self.assertEqual(r.returncode, 0, r.stderr)
            d = json.loads(r.stdout)
            return d["exit_code"], d["stdout"]

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(_run_one, range(12)))
        for idx, (ec, out) in enumerate(results):
            self.assertEqual(ec, 0)
            self.assertEqual(out.strip(), f"PIPE_ITEM_{idx:02d}")

    # --- F2: TEST-41 Master Shutdown / Durability Hand-Over ---

    def test_t1_f2_01_clean_sigterm_exits_zero_and_releases_store_lock(self) -> None:
        state_dir = self.env.root / "f2_state_01"
        m1 = self.ox.start_master(name="m1", state_dir=state_dir)
        rc = m1.proc.stop(sig=signal.SIGTERM, timeout=10.0)
        self.assertEqual(rc, 0, m1.proc.logs())

        # Store lock must be cleanly released so master2 restarts on same state_dir & listen.
        m2 = self.ox.start_master(
            name="m2", listen=m1.listen_addr, state_dir=state_dir
        )
        self.assertEqual(m2.listen_addr, m1.listen_addr)
        rc2 = m2.proc.stop(sig=signal.SIGTERM, timeout=10.0)
        self.assertEqual(rc2, 0, m2.proc.logs())

    def test_t1_f2_02_restart_recovers_completed_task_result(self) -> None:
        state_dir = self.env.root / "f2_state_02"
        m1 = self.ox.start_master(name="m1", state_dir=state_dir)
        self.ox.start_worker(m1.listen_addr, name="w-rec")

        sub = self.ox.submit_cli(
            m1.listen_addr,
            ["--type", "shell", "--wait", "--json", "--command", "echo DURABLE_OK_02"],
        )
        self.assertEqual(sub.returncode, 0, sub.stderr)
        task_id = json.loads(sub.stdout)["task_id"]

        self.assertEqual(m1.proc.stop(sig=signal.SIGTERM, timeout=10.0), 0)
        m2 = self.ox.start_master(name="m2", listen=m1.listen_addr, state_dir=state_dir)

        res = self.ox.result_cli(m2.listen_addr, task_id, wait=True, json_out=True)
        self.assertEqual(res.returncode, 0, res.stderr)
        data = json.loads(res.stdout)
        self.assertEqual(data["exit_code"], 0)
        self.assertIn("DURABLE_OK_02", data["stdout"])

    def test_t1_f2_03_worker_name_preserves_identity_across_restart(self) -> None:
        state_dir = self.env.root / "f2_state_03"
        m1 = self.ox.start_master(
            name="m1",
            state_dir=state_dir,
            heartbeat_interval_secs=1,
            session_resume_hold_secs=30,
        )
        self.ox.start_worker(m1.listen_addr, name="w-stable-id")
        w1 = self.ox.workers_json(m1.listen_addr)
        id_tag_1 = next(t for t in w1[0]["tags"] if t.startswith("id:"))

        self.assertEqual(m1.proc.stop(sig=signal.SIGTERM, timeout=10.0), 0)
        m2 = self.ox.start_master(
            name="m2",
            listen=m1.listen_addr,
            state_dir=state_dir,
            heartbeat_interval_secs=1,
            session_resume_hold_secs=30,
        )

        def _reconnected() -> Optional[str]:
            try:
                ws = self.ox.workers_json(m2.listen_addr)
                for w in ws:
                    if w.get("name") == "w-stable-id" and "state:connected" in w.get("tags", []):
                        return next((t for t in w["tags"] if t.startswith("id:")), None)
            except Exception:
                return None
            return None

        id_tag_2 = _wait_until(_reconnected, timeout=15.0, desc="worker reconnect")
        self.assertEqual(id_tag_1, id_tag_2)

    def test_t1_f2_04_gated_task_finishing_at_sigterm_runs_strictly_once(self) -> None:
        state_dir = self.env.root / "f2_state_04"
        marker = self.env.root / "marker_04.log"
        gate = self.env.root / "gate_04"
        done = self.env.root / "done_04"

        m1 = self.ox.start_master(
            name="m1",
            state_dir=state_dir,
            heartbeat_interval_secs=1,
            session_resume_hold_secs=30,
        )
        self.ox.start_worker(m1.listen_addr, name="w-once", isolation="off")

        sub = self.ox.submit_cli(
            m1.listen_addr,
            [
                "--type", "shell",
                "--max-retries", "2",
                "--json",
                "--command", _gated_marker_script(marker, gate, done),
            ],
        )
        self.assertEqual(sub.returncode, 0, sub.stderr)
        task_id = json.loads(sub.stdout)["task_id"]

        _wait_until(
            lambda: marker.is_file() and len(marker.read_text().splitlines()) >= 1,
            timeout=15.0,
            desc="marker line 1",
        )
        gate.write_text("go\n", encoding="utf-8")
        _wait_until(done.is_file, timeout=10.0, interval=0.002, desc="done flag")

        rc = m1.proc.stop(sig=signal.SIGTERM, timeout=10.0)
        self.assertEqual(rc, 0, m1.proc.logs())

        time.sleep(0.15)
        m2 = self.ox.start_master(
            name="m2",
            listen=m1.listen_addr,
            state_dir=state_dir,
            heartbeat_interval_secs=1,
            session_resume_hold_secs=30,
        )
        res = self.ox.result_cli(m2.listen_addr, task_id, wait=True, json_out=True)
        self.assertEqual(res.returncode, 0, res.stderr)
        data = json.loads(res.stdout)
        self.assertEqual(data["exit_code"], 0)
        self.assertIn("GATED_OK", data["stdout"])

        time.sleep(0.4)
        lines = marker.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 1, f"Task ran more than once: {lines!r}")

    def test_t1_f2_05_unacked_result_in_worker_outbox_delivers_on_resume(self) -> None:
        state_dir = self.env.root / "f2_state_05"
        marker = self.env.root / "marker_05.log"
        gate = self.env.root / "gate_05"
        done = self.env.root / "done_05"

        m1 = self.ox.start_master(
            name="m1",
            state_dir=state_dir,
            heartbeat_interval_secs=1,
            session_resume_hold_secs=30,
        )
        self.ox.start_worker(m1.listen_addr, name="w-outbox", isolation="off")

        sub = self.ox.submit_cli(
            m1.listen_addr,
            [
                "--type", "shell",
                "--max-retries", "2",
                "--json",
                "--command", _gated_marker_script(marker, gate, done),
            ],
        )
        self.assertEqual(sub.returncode, 0, sub.stderr)
        task_id = json.loads(sub.stdout)["task_id"]

        _wait_until(
            lambda: marker.is_file() and len(marker.read_text().splitlines()) >= 1,
            timeout=15.0,
            desc="marker line 1",
        )
        # Trigger completion right as SIGTERM hits master1 so result either lands in
        # store compaction or stays in worker pending_resume_outbox across ResumeSession.
        gate.write_text("go\n", encoding="utf-8")
        time.sleep(0.002)
        rc = m1.proc.stop(sig=signal.SIGTERM, timeout=10.0)
        self.assertEqual(rc, 0, m1.proc.logs())

        _wait_until(done.is_file, timeout=10.0, desc="task finished during shutdown")

        m2 = self.ox.start_master(
            name="m2",
            listen=m1.listen_addr,
            state_dir=state_dir,
            heartbeat_interval_secs=1,
            session_resume_hold_secs=30,
        )
        res = self.ox.result_cli(m2.listen_addr, task_id, wait=True, json_out=True)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertEqual(json.loads(res.stdout)["exit_code"], 0)
        time.sleep(0.35)
        self.assertEqual(len(marker.read_text(encoding="utf-8").splitlines()), 1)

    # --- F3: AX-1 CLI Matrix Fan-Out (`submit --matrix`) ---

    def test_t1_f3_01_three_tagged_workers_1d_matrix_fanout(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-alpha", tags=["cell-a"])
        self.ox.start_worker(master.listen_addr, name="w-beta", tags=["cell-b"])
        self.ox.start_worker(master.listen_addr, name="w-gamma", tags=["cell-c"])

        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--matrix", "tag=cell-a,cell-b,cell-c",
                "--json",
                "--timeout-secs", "30",
                "--command", "sh", "--", "-c", "echo CELL_OK_$(uname -s)",
            ],
        )
        self.assertEqual(res.returncode, 0, f"stderr={res.stderr}\nstdout={res.stdout}")
        payload = json.loads(res.stdout)
        cells = _extract_matrix_cells(payload)
        self.assertEqual(len(cells), 3)

        workers_seen = {_cell_worker(c) for c in cells}
        self.assertEqual(len(workers_seen), 3, f"Expected 3 distinct workers: {cells!r}")
        labels = " ".join(_cell_label_text(c) for c in cells)
        for tag in ("cell-a", "cell-b", "cell-c"):
            self.assertIn(tag, labels)
        for c in cells:
            self.assertEqual(c.get("exit_code"), 0)
            self.assertIn("CELL_OK_", _cell_stdout(c))

    def test_t1_f3_02_2d_cartesian_matrix_expansion(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-g1-t1", tags=["grp-1", "tier-fast"])
        self.ox.start_worker(master.listen_addr, name="w-g1-t2", tags=["grp-1", "tier-slow"])
        self.ox.start_worker(master.listen_addr, name="w-g2-t1", tags=["grp-2", "tier-fast"])
        self.ox.start_worker(master.listen_addr, name="w-g2-t2", tags=["grp-2", "tier-slow"])

        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--matrix", "tag=grp-1,grp-2",
                "--matrix", "tag=tier-fast,tier-slow",
                "--json",
                "--timeout-secs", "30",
                "--command", "echo", "--", "CARTESIAN_OK",
            ],
        )
        self.assertEqual(res.returncode, 0, f"stderr={res.stderr}\nstdout={res.stdout}")
        cells = _extract_matrix_cells(json.loads(res.stdout))
        self.assertEqual(len(cells), 4)
        self.assertEqual(len({_cell_worker(c) for c in cells}), 4)
        for c in cells:
            self.assertEqual(c.get("exit_code"), 0)

    def test_t1_f3_03_matrix_json_outputs_structured_cells_and_summary(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-j1", tags=["j-1"])
        self.ox.start_worker(master.listen_addr, name="w-j2", tags=["j-2"])

        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--matrix", "tag=j-1,j-2",
                "--json",
                "--command", "sh", "--", "-c", "echo LINE1; echo LINE2",
            ],
        )
        self.assertEqual(res.returncode, 0, res.stderr)
        # Stdout must parse cleanly as single JSON document (no ANSI or log lines).
        parsed = json.loads(res.stdout)
        cells = _extract_matrix_cells(parsed)
        self.assertEqual(len(cells), 2)
        for c in cells:
            self.assertTrue(_cell_worker(c), f"Missing worker field in {c!r}")
            self.assertIn("exit_code", c)
            has_dur = any(k in c for k in ("duration_ms", "execution_time_ms", "elapsed_ms"))
            self.assertTrue(has_dur, f"Missing duration field in {c!r}")
            out = _cell_stdout(c)
            self.assertIn("LINE1", out)
            self.assertIn("LINE2", out)

    def test_t1_f3_04_matrix_shared_input_blob_deduplicated(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-in-a", tags=["in-a"])
        self.ox.start_worker(master.listen_addr, name="w-in-b", tags=["in-b"])
        self.ox.start_worker(master.listen_addr, name="w-in-c", tags=["in-c"])

        src = self.env.work / "shared_inputs"
        (src / "sub").mkdir(parents=True)
        (src / "a.bin").write_bytes(b"A" * 32768)
        (src / "b.bin").write_bytes(b"B" * 32768)
        (src / "sub" / "c.bin").write_bytes(b"C" * 16384)

        assert master.dashboard_port is not None
        metrics_url = f"http://127.0.0.1:{master.dashboard_port}/metrics"
        _, before_txt = http_get_text(metrics_url)
        before_objs = parse_prom_metric(before_txt, "oxideswarm_blob_store_objects") or 0.0

        res1 = self.ox.submit_cli(
            master.listen_addr,
            [
                "--matrix", "tag=in-a,in-b,in-c",
                "--input", f"{src}:work",
                "--json",
                "--command", "sh", "--", "-c", "wc -c < work/a.bin",
            ],
        )
        self.assertEqual(res1.returncode, 0, res1.stderr)
        cells1 = _extract_matrix_cells(json.loads(res1.stdout))
        self.assertEqual(len(cells1), 3)
        for c in cells1:
            self.assertIn("32768", _cell_stdout(c))

        _, after1_txt = http_get_text(metrics_url)
        after1_objs = parse_prom_metric(after1_txt, "oxideswarm_blob_store_objects") or 0.0
        delta_objs = after1_objs - before_objs
        # 3 files + 1 tree manifest = 4 unique objects (uploaded once, NOT 3x = 12).
        self.assertGreaterEqual(delta_objs, 3.0)
        self.assertLessEqual(delta_objs, 6.0, f"Blobs uploaded multiple times? delta={delta_objs}")

        res2 = self.ox.submit_cli(
            master.listen_addr,
            [
                "--matrix", "tag=in-a,in-b,in-c",
                "--input", f"{src}:work",
                "--json",
                "--command", "sh", "--", "-c", "wc -c < work/b.bin",
            ],
        )
        self.assertEqual(res2.returncode, 0, res2.stderr)
        _, after2_txt = http_get_text(metrics_url)
        after2_objs = parse_prom_metric(after2_txt, "oxideswarm_blob_store_objects") or 0.0
        self.assertEqual(after2_objs, after1_objs)

    def test_t1_f3_05_matrix_shared_workspace_sync_deduplicated(self) -> None:
        master = self.ox.start_master()
        for idx, tag in enumerate(("ws-a", "ws-b", "ws-c")):
            self.ox.start_worker(
                master.listen_addr,
                name=f"w-ws-{idx}",
                tags=[tag],
                workspaces=True,
            )

        ws_src = self.env.work / "ws_project"
        ws_src.mkdir()
        (ws_src / "manifest.txt").write_text("workspace-matrix-v1\n", encoding="utf-8")
        (ws_src / "data.bin").write_bytes(b"W" * 40960)

        assert master.dashboard_port is not None
        metrics_url = f"http://127.0.0.1:{master.dashboard_port}/metrics"
        _, before_txt = http_get_text(metrics_url)
        before_objs = parse_prom_metric(before_txt, "oxideswarm_blob_store_objects") or 0.0

        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--matrix", "tag=ws-a,ws-b,ws-c",
                "--workspace", "ax1-shared-ws",
                "--sync", str(ws_src),
                "--json",
                "--command", "cat", "--", "manifest.txt",
            ],
        )
        self.assertEqual(res.returncode, 0, res.stderr)
        cells = _extract_matrix_cells(json.loads(res.stdout))
        self.assertEqual(len(cells), 3)
        for c in cells:
            self.assertIn("workspace-matrix-v1", _cell_stdout(c))

        _, after_txt = http_get_text(metrics_url)
        after_objs = parse_prom_metric(after_txt, "oxideswarm_blob_store_objects") or 0.0
        delta_objs = after_objs - before_objs
        # Shared workspace synced once -> <= 4 objects (2 files + manifest/tree), NOT 3x.
        self.assertGreaterEqual(delta_objs, 2.0)
        self.assertLessEqual(delta_objs, 6.0, f"Workspace uploaded 3x? delta={delta_objs}")

    # --- F4: AX-1 MCP `submit` Tool Matrix Support ---

    def test_t1_f4_01_mcp_initialize_and_tools_list_exposes_matrix(self) -> None:
        master = self.ox.start_master()
        mcp = McpClient(self.env, master.listen_addr)
        init_resp = mcp.initialize()
        self.assertEqual(init_resp["result"]["protocolVersion"], "2024-11-05")

        tools = mcp.tools_list()
        submit_tool = next((t for t in tools if t.get("name") == "submit"), None)
        self.assertIsNotNone(submit_tool)
        props = submit_tool["inputSchema"]["properties"]
        self.assertIn("matrix", props)
        mcp.close_stdin()

    def test_t1_f4_02_mcp_submit_1d_matrix_across_3_workers(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-mcp-a", tags=["mcp-a"])
        self.ox.start_worker(master.listen_addr, name="w-mcp-b", tags=["mcp-b"])
        self.ox.start_worker(master.listen_addr, name="w-mcp-c", tags=["mcp-c"])

        mcp = McpClient(self.env, master.listen_addr)
        mcp.initialize()
        tools = mcp.tools_list()
        matrix_arg = _build_mcp_matrix_arg(tools, {"tag": ["mcp-a", "mcp-b", "mcp-c"]})

        resp = mcp.call_tool(
            "submit",
            {
                "type": "command",
                "command": "echo",
                "args": ["mcp-1d-ok"],
                "matrix": matrix_arg,
                "wait": True,
            },
        )
        is_err, text = _mcp_tool_text(resp)
        self.assertFalse(is_err, text)
        cells = _extract_matrix_cells(json.loads(text))
        self.assertEqual(len(cells), 3)
        self.assertEqual(len({_cell_worker(c) for c in cells}), 3)
        for c in cells:
            self.assertEqual(c.get("exit_code"), 0)
            self.assertIn("mcp-1d-ok", _cell_stdout(c))
        mcp.close_stdin()

    def test_t1_f4_03_mcp_submit_2d_matrix(self) -> None:
        master = self.ox.start_master()
        if sys.platform == "darwin":
            self.ox.start_worker(master.listen_addr, name="w-m11", isolation="off", tags=["row-1", "col-1"])
            self.ox.start_worker(master.listen_addr, name="w-m12", isolation="fs", tags=["row-1", "col-2"])
            self.ox.start_worker(master.listen_addr, name="w-m21", isolation="off", tags=["row-2", "col-1"])
            self.ox.start_worker(master.listen_addr, name="w-m22", isolation="fs", tags=["row-2", "col-2"])
        else:
            self.ox.start_worker(master.listen_addr, name="w-m11", tags=["row-1", "col-1"])
            self.ox.start_worker(master.listen_addr, name="w-m12", tags=["row-1", "col-2"])
            self.ox.start_worker(master.listen_addr, name="w-m21", tags=["row-2", "col-1"])
            self.ox.start_worker(master.listen_addr, name="w-m22", tags=["row-2", "col-2"])

        mcp = McpClient(self.env, master.listen_addr)
        mcp.initialize()
        tools = mcp.tools_list()
        submit_schema = next(t for t in tools if t["name"] == "submit")["inputSchema"]["properties"]["matrix"]
        if submit_schema.get("type") == "array":
            matrix_arg: Any = ["tag=row-1,row-2", "tag=col-1,col-2"]
            expected_cells = 4
        elif sys.platform == "darwin":
            matrix_arg = {"tag": ["row-1", "row-2"], "iso": ["off", "fs"]}
            expected_cells = 4
        else:
            matrix_arg = {"os": ["linux"], "tag": ["row-1", "row-2"]}
            expected_cells = 2

        resp = mcp.call_tool(
            "submit",
            {
                "type": "command",
                "command": "echo",
                "args": ["mcp-2d-ok"],
                "matrix": matrix_arg,
                "wait": True,
            },
        )
        is_err, text = _mcp_tool_text(resp)
        self.assertFalse(is_err, text)
        cells = _extract_matrix_cells(json.loads(text))
        self.assertEqual(len(cells), expected_cells)
        self.assertEqual(len({_cell_worker(c) for c in cells}), expected_cells)

        # Also exercise runtime MCP object-form matrix ({"tag": [...], "env:...": [...]}).
        obj_resp = mcp.call_tool(
            "submit",
            {
                "type": "shell",
                "script": 'echo "MODE=$MODE CELL=$OXIDESWARM_MATRIX_CELL"',
                "matrix": {"tag": ["row-1", "row-2"], "env:MODE": ["fast", "slow"]},
                "wait": True,
            },
        )
        obj_err, obj_text = _mcp_tool_text(obj_resp)
        self.assertFalse(obj_err, obj_text)
        obj_cells = _extract_matrix_cells(json.loads(obj_text))
        self.assertEqual(len(obj_cells), 4)
        for c in obj_cells:
            self.assertEqual(c.get("exit_code"), 0, c)
        obj_stdouts = "\n".join(_cell_stdout(c) for c in obj_cells)
        self.assertIn("MODE=fast", obj_stdouts)
        self.assertIn("MODE=slow", obj_stdouts)
        mcp.close_stdin()

    def test_t1_f4_04_mcp_submit_workspace_and_matrix(self) -> None:
        master = self.ox.start_master()
        for i, tag in enumerate(("mws-1", "mws-2", "mws-3")):
            self.ox.start_worker(
                master.listen_addr, name=f"w-mws-{i}", tags=[tag], workspaces=True
            )

        ws_dir = self.env.work / "mcp_ws"
        ws_dir.mkdir()
        (ws_dir / "hello.txt").write_text("mcp-workspace-content", encoding="utf-8")

        mcp = McpClient(self.env, master.listen_addr, cwd=ws_dir)
        mcp.initialize()
        tools = mcp.tools_list()
        matrix_arg = _build_mcp_matrix_arg(tools, {"tag": ["mws-1", "mws-2", "mws-3"]})

        resp = mcp.call_tool(
            "submit",
            {
                "type": "command",
                "command": "cat",
                "args": ["hello.txt"],
                "workspace": {"name": "mcp-matrix-ws", "sync_dir": "."},
                "matrix": matrix_arg,
                "wait": True,
            },
        )
        is_err, text = _mcp_tool_text(resp)
        self.assertFalse(is_err, text)
        cells = _extract_matrix_cells(json.loads(text))
        self.assertEqual(len(cells), 3)
        for c in cells:
            self.assertIn("mcp-workspace-content", _cell_stdout(c))
        mcp.close_stdin()

    def test_t1_f4_05_mcp_stdout_jsonrpc_hygiene_and_ping(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-hyg-1", tags=["hyg-1"])
        self.ox.start_worker(master.listen_addr, name="w-hyg-2", tags=["hyg-2"])

        mcp = McpClient(self.env, master.listen_addr)
        mcp.initialize()
        tools = mcp.tools_list()
        matrix_arg = _build_mcp_matrix_arg(tools, {"tag": ["hyg-1", "hyg-2"]})

        mcp.call_tool(
            "submit",
            {
                "type": "shell",
                "script": "echo HYG_OUT_1; echo HYG_ERR_1 >&2",
                "matrix": matrix_arg,
                "wait": True,
            },
        )
        pong = mcp.ping()
        self.assertEqual(pong.get("result"), {})
        self.assertGreaterEqual(len(mcp.all_stdout_lines), 4)
        for line in mcp.all_stdout_lines:
            parsed = json.loads(line)
            self.assertEqual(parsed.get("jsonrpc"), "2.0")
        mcp.close_stdin()

    # --- F5: FEAT-5 Process Task Checkpoints ---

    def test_t1_f5_01_command_task_receives_writable_checkpoint_dir(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-cp-cmd")

        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "command",
                "--wait", "--json",
                "--command", "sh", "--", "-c",
                'test -n "$OXIDESWARM_CHECKPOINT_DIR" && test -d "$OXIDESWARM_CHECKPOINT_DIR" '
                '&& echo "cp_cmd_ok" > "$OXIDESWARM_CHECKPOINT_DIR/probe.txt" '
                '&& cat "$OXIDESWARM_CHECKPOINT_DIR/probe.txt"',
            ],
        )
        self.assertEqual(res.returncode, 0, res.stderr)
        data = json.loads(res.stdout)
        self.assertEqual(data["exit_code"], 0)
        self.assertIn("cp_cmd_ok", data["stdout"])

    def test_t1_f5_02_shell_script_persists_checkpoint_dir(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-cp-sh")

        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "shell",
                "--wait", "--json",
                "--command",
                'set -e\n'
                'test -d "$OXIDESWARM_CHECKPOINT_DIR"\n'
                'echo "cp_shell_ok" > "$OXIDESWARM_CHECKPOINT_DIR/state.txt"\n'
                'cat "$OXIDESWARM_CHECKPOINT_DIR/state.txt"\n',
            ],
        )
        self.assertEqual(res.returncode, 0, res.stderr)
        data = json.loads(res.stdout)
        self.assertEqual(data["exit_code"], 0)
        self.assertIn("cp_shell_ok", data["stdout"])

    def test_t1_f5_03_workspace_task_receives_checkpoint_dir(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-cp-ws", workspaces=True)

        ws_src = self.env.work / "cp_ws_src"
        ws_src.mkdir()
        (ws_src / "input.txt").write_text("ws-data", encoding="utf-8")

        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "command",
                "--workspace", "cp-ws-test",
                "--sync", str(ws_src),
                "--wait", "--json",
                "--command", "sh", "--", "-c",
                'test -d "$OXIDESWARM_CHECKPOINT_DIR" '
                '&& echo "cp_ws_ok" > "$OXIDESWARM_CHECKPOINT_DIR/ws_cp.dat" '
                '&& test ! -e "ws_cp.dat" '
                '&& cat input.txt && cat "$OXIDESWARM_CHECKPOINT_DIR/ws_cp.dat"',
            ],
        )
        self.assertEqual(res.returncode, 0, res.stderr)
        data = json.loads(res.stdout)
        self.assertEqual(data["exit_code"], 0)
        self.assertIn("ws-data", data["stdout"])
        self.assertIn("cp_ws_ok", data["stdout"])

    def test_t1_f5_04_worker_sigkill_failover_restores_checkpoint_and_skips_step1(self) -> None:
        master = self.ox.start_master(
            session_resume_hold_secs=0,
            heartbeat_interval_secs=1,
            heartbeat_timeout_secs=3,
            reaper_interval_secs=1,
        )
        w1 = self.ox.start_worker(master.listen_addr, name="w-cp-1", isolation="off")

        trace_log = self.env.root / "cp_trace_04.log"
        cp_ready = self.env.root / "cp_ready_04"
        out_dir = self.env.root / "cp_out_04"
        script = (
            'set -e\n'
            'CP_FILE="$OXIDESWARM_CHECKPOINT_DIR/progress.state"\n'
            'if [ -f "$CP_FILE" ] && grep -q "STEP1_DONE" "$CP_FILE"; then\n'
            f'  echo "RESUMED_FROM_CHECKPOINT" >> "{trace_log}"\n'
            '  echo "final-step2-ok" > out.txt\n'
            '  exit 0\n'
            'fi\n'
            f'echo "STEP1_RUN" >> "{trace_log}"\n'
            'echo "STEP1_DONE" > "$CP_FILE"\n'
            f'touch "{cp_ready}"\n'
            'sleep 30\n'
        )
        sub_proc = self.ox.submit_cli_popen(
            master.listen_addr,
            [
                "--type", "command",
                "--max-retries", "2",
                "--output", "out.txt",
                "--output-dir", str(out_dir),
                "--json",
                "--command", "sh", "--", "-c", script,
            ],
        )

        _wait_until(cp_ready.is_file, timeout=15.0, desc="checkpoint file written")
        # Allow periodic checkpoint interval to emit WorkerMessage::Checkpoint.
        time.sleep(3.5)

        w1.proc.stop(sig=signal.SIGKILL, timeout=5.0)
        self.ox.start_worker(master.listen_addr, name="w-cp-2", isolation="off")

        rc = sub_proc.wait(timeout=30.0)
        stdout = "\n".join(sub_proc.stdout_lines)
        self.assertEqual(rc, 0, sub_proc.logs())
        data = json.loads(stdout)
        self.assertEqual(data["exit_code"], 0)

        trace = trace_log.read_text(encoding="utf-8").splitlines()
        self.assertEqual(trace.count("STEP1_RUN"), 1, f"Step 1 re-ran: {trace!r}")
        self.assertEqual(trace.count("RESUMED_FROM_CHECKPOINT"), 1, f"Did not resume: {trace!r}")
        self.assertTrue((out_dir / "out.txt").is_file())
        self.assertIn("final-step2-ok", (out_dir / "out.txt").read_text(encoding="utf-8"))

    def test_t1_f5_05_periodic_checkpoint_updates_state_before_worker_loss(self) -> None:
        master = self.ox.start_master(
            session_resume_hold_secs=0,
            heartbeat_interval_secs=1,
            heartbeat_timeout_secs=3,
            reaper_interval_secs=1,
        )
        w1 = self.ox.start_worker(master.listen_addr, name="w-per-1", isolation="off")

        stage2_flag = self.env.root / "stage2_ready_05"
        resumed_val = self.env.root / "resumed_stage_05.txt"
        script = (
            'set -e\n'
            'CP_FILE="$OXIDESWARM_CHECKPOINT_DIR/stage.txt"\n'
            'if [ -f "$CP_FILE" ]; then\n'
            f'  cat "$CP_FILE" > "{resumed_val}"\n'
            '  exit 0\n'
            'fi\n'
            'echo "STAGE=1" > "$CP_FILE"\n'
            'sleep 3\n'
            'echo "STAGE=2" > "$CP_FILE"\n'
            f'touch "{stage2_flag}"\n'
            'sleep 30\n'
        )
        sub_proc = self.ox.submit_cli_popen(
            master.listen_addr,
            [
                "--type", "shell",
                "--max-retries", "2",
                "--wait", "--json",
                "--command", script,
            ],
        )

        _wait_until(stage2_flag.is_file, timeout=15.0, desc="stage 2 written")
        # Wait for periodic checkpoint to flush STAGE=2.
        time.sleep(3.5)

        w1.proc.stop(sig=signal.SIGKILL, timeout=5.0)
        self.ox.start_worker(master.listen_addr, name="w-per-2", isolation="off")

        rc = sub_proc.wait(timeout=30.0)
        self.assertEqual(rc, 0, sub_proc.logs())
        self.assertTrue(resumed_val.is_file())
        self.assertEqual(resumed_val.read_text(encoding="utf-8").strip(), "STAGE=2")

    # --- F6: MESH-2 Agent Mesh Consumer Delivery & Telemetry ---

    def test_t1_f6_01_hub_and_node_inbox_dir_registration(self) -> None:
        hub = self.mesh.start_hub()
        inbox = self.env.root / "inbox_01"
        inbox.mkdir()
        self.mesh.start_node(hub, "consumer-01", inbox_dir=inbox)

        nodes = http_get_json(f"{hub.http_url}/api/nodes")
        ids = {n["node_id"] for n in nodes}
        self.assertIn("consumer-01", ids)

    def test_t1_f6_02_data_to_consumer_persists_verified_payload_and_exits_zero(self) -> None:
        hub = self.mesh.start_hub()
        inbox = self.env.root / "inbox_02"
        inbox.mkdir()
        self.mesh.start_node(hub, "consumer-02", inbox_dir=inbox)

        payload_str = "hello-mesh-wave2-verified"
        res = self.mesh.run_data(hub, to_node="consumer-02", payload=payload_str)
        self.assertEqual(res.returncode, 0, f"stderr={res.stderr}\nstdout={res.stdout}")

        saved = _wait_inbox_for_bytes(inbox, payload_str.encode("utf-8"), timeout=5.0)
        self.assertEqual(saved.read_bytes(), payload_str.encode("utf-8"))

    def test_t1_f6_03_data_cli_prints_matching_sha256_and_correlation_id(self) -> None:
        hub = self.mesh.start_hub()
        inbox = self.env.root / "inbox_03"
        inbox.mkdir()
        self.mesh.start_node(hub, "consumer-03", inbox_dir=inbox)

        payload_str = "digest-audit-payload-03"
        expected_sha = hashlib.sha256(payload_str.encode("utf-8")).hexdigest()
        res = self.mesh.run_data(hub, to_node="consumer-03", payload=payload_str)
        self.assertEqual(res.returncode, 0, res.stderr)
        combined = res.stdout + "\n" + res.stderr
        self.assertIn(expected_sha, combined.lower())
        self.assertRegex(
            combined,
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        )

    def test_t1_f6_04_rest_api_nodes_and_status_honest_telemetry(self) -> None:
        hub = self.mesh.start_hub()
        inbox = self.env.root / "inbox_04"
        inbox.mkdir()
        node = self.mesh.start_node(hub, "consumer-04", inbox_dir=inbox)

        nodes = http_get_json(f"{hub.http_url}/api/nodes")
        entry = next(n for n in nodes if n["node_id"] == "consumer-04")
        self.assertEqual(entry.get("status"), "online")

        status = http_get_json(f"{hub.http_url}/api/status")
        self.assertIsInstance(status, dict)
        self.assertEqual(status.get("connected_nodes"), 1, status)
        self.assertEqual(status.get("online_nodes"), 1, status)
        stats = status.get("stats")
        self.assertIsInstance(stats, dict, status)
        for stat_key in ("routed_data", "delivered_acks", "delivered_nacks"):
            self.assertIn(stat_key, stats, status)
            self.assertIsInstance(stats[stat_key], int, status)
            self.assertGreaterEqual(stats[stat_key], 0, status)

        # Disconnecting node must remove/update it honestly in /api/nodes and /api/status.
        node.stop(sig=signal.SIGTERM, timeout=5.0)

        def _node_gone() -> bool:
            cur = http_get_json(f"{hub.http_url}/api/nodes")
            cur_status = http_get_json(f"{hub.http_url}/api/status")
            nodes_clear = all(
                n["node_id"] != "consumer-04" or n.get("status") != "online" for n in cur
            )
            return (
                nodes_clear
                and cur_status.get("connected_nodes") == 0
                and cur_status.get("online_nodes") == 0
            )

        _wait_until(_node_gone, timeout=5.0, desc="disconnected node status update")

    def test_t1_f6_05_single_chunk_total_chunks_accuracy(self) -> None:
        hub = self.mesh.start_hub()
        ws_consumer = StdlibWsClient(hub.ws_url)
        self.env.add_cleanup(ws_consumer.close)
        ws_consumer.register("ws-consumer-05")

        payload_str = "single-chunk-check-05"
        # Run CLI data sender in background thread while ws_consumer inspects exact frame.
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            fut = pool.submit(self.mesh.run_data, hub, "ws-consumer-05", payload_str)
            msg = ws_consumer.recv_until(
                lambda m: m.get("type") == "DataPayload", timeout=8.0
            )
            self.assertEqual(msg["total_chunks"], 1)
            self.assertEqual(msg["sequence_number"], 0)
            self.assertEqual(msg["chunk_size_bytes"], len(payload_str.encode("utf-8")))
            self.assertEqual(
                msg["checksum_sha256"],
                hashlib.sha256(payload_str.encode("utf-8")).hexdigest(),
            )
            ws_consumer.send_json(
                {
                    "type": "DeliveryAck",
                    "correlation_id": msg["correlation_id"],
                    "from": "ws-consumer-05",
                    "to": msg["from"],
                    "status": "data_received_verified",
                }
            )
            cli_res = fut.result(timeout=10.0)
            self.assertEqual(cli_res.returncode, 0, cli_res.stderr)


# ===========================================================================
# TIER 2: Boundary, Corner & Adversarial Tests (30 tests — 5 per feature F1-F6)
# ===========================================================================


class Tier2BoundaryCornerTests(Wave2E2ETestBase):
    """Tier 2: Boundary value, negative, and stress cases across F1-F6."""

    # --- F1: TEST-61 Boundary ---

    def test_t2_f1_01_loaded_50x_control_loop_zero_empty_stdout(self) -> None:
        secret_file = self.env.home / "t2_secret_50x.txt"
        secret_val = "oxideswarm-hardlink-50x-secret"
        secret_file.write_text(secret_val, encoding="utf-8")

        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-50x-off", isolation="off", tags=["m-off"])
        if sys.platform == "darwin":
            self.ox.start_worker(master.listen_addr, name="w-50x-fs", isolation="fs", tags=["m-fs"])

        burners: list[subprocess.Popen[str]] = []

        def _cleanup_burners() -> None:
            for p in burners:
                if p.poll() is None:
                    try:
                        p.terminate()
                        p.wait(timeout=2.0)
                    except Exception:
                        try:
                            p.kill()
                            p.wait(timeout=2.0)
                        except Exception:
                            pass

        self.env.add_cleanup(_cleanup_burners)
        burner_code = "x = 1\nwhile True:\n    x = (x * 1664525 + 1013904223) & 0xFFFFFFFF\n"
        for _ in range(4):
            burners.append(
                subprocess.Popen(
                    [sys.executable, "-c", burner_code],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    text=True,
                )
            )

        try:
            for i in range(50):
                res = self.ox.submit_cli(
                    master.listen_addr,
                    [
                        "--type", "shell",
                        "--require-tag", "m-off",
                        "--env", f"T_SECRET={secret_file}",
                        "--wait", "--json",
                        "--command", _hardlink_test_script(),
                    ],
                )
                self.assertEqual(res.returncode, 0, f"iter={i} stderr={res.stderr}")
                data = json.loads(res.stdout)
                self.assertEqual(data["exit_code"], 0, f"iter={i} data={data}")
                self.assertTrue(data["stdout"].strip(), f"Empty stdout on iter={i}: {data}")
                self.assertIn("LINK_OK", data["stdout"], f"iter={i}")
                self.assertIn(secret_val, data["stdout"], f"iter={i}")
                self.assertIn("END", data["stdout"], f"iter={i}")

                if sys.platform == "darwin" and i % 10 == 0:
                    fs_res = self.ox.submit_cli(
                        master.listen_addr,
                        [
                            "--type", "shell",
                            "--require-isolation", "fs",
                            "--env", f"T_SECRET={secret_file}",
                            "--wait", "--json",
                            "--command", _hardlink_test_script(),
                        ],
                    )
                    self.assertEqual(fs_res.returncode, 0, fs_res.stderr)
                    fs_data = json.loads(fs_res.stdout)
                    self.assertNotIn(secret_val, fs_data["stdout"])
                    self.assertIn("END", fs_data["stdout"])
        finally:
            _cleanup_burners()

    def test_t2_f1_02_fs_isolation_denies_nested_subdir_and_symlink_escape(self) -> None:
        self._require_darwin_for_seatbelt()
        nested_dir = self.env.home / "deep" / "nested"
        nested_dir.mkdir(parents=True)
        secret_file = nested_dir / "vault.txt"
        secret_val = "nested-secret-t2-02"
        secret_file.write_text(secret_val, encoding="utf-8")
        sym = self.env.home / "vault_link"
        sym.symlink_to(secret_file)

        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-fs-sym", isolation="fs")

        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "shell",
                "--require-isolation", "fs",
                "--env", f"T_NESTED={secret_file}",
                "--env", f"T_SYM={sym}",
                "--wait", "--json",
                "--command",
                'cat "$T_NESTED" 2>/dev/null; cat "$T_SYM" 2>/dev/null; '
                'ln "$T_SYM" "$RUSTY_GRID_SANDBOX_DIR/h" 2>/dev/null && echo BAD || echo BLOCKED',
            ],
        )
        self.assertEqual(res.returncode, 0, res.stderr)
        data = json.loads(res.stdout)
        self.assertIn("BLOCKED", data["stdout"])
        self.assertNotIn(secret_val, data["stdout"])

    def test_t2_f1_03_isolation_fs_refuses_secret_cli_flag_and_env(self) -> None:
        self._require_darwin_for_seatbelt()
        master = self.ox.start_master()
        dummy_hex = "ab" * 32

        r_cli = self.env.run_cmd(
            [
                self.ox.bin,
                "--secret", dummy_hex,
                "worker",
                "--master", master.listen_addr,
                "--name", "w-bad-cli",
                "--isolation", "fs",
                "--no-gpu", "--no-portal",
            ],
            timeout=10.0,
        )
        self.assertNotEqual(r_cli.returncode, 0)
        cli_err = (r_cli.stderr + "\n" + r_cli.stdout).lower()
        self.assertIn("--isolation", cli_err, r_cli.stderr)
        self.assertIn("--secret", cli_err, r_cli.stderr)

        r_env = self.env.run_cmd(
            [
                self.ox.bin,
                "worker",
                "--master", master.listen_addr,
                "--name", "w-bad-env",
                "--isolation", "fs",
                "--no-gpu", "--no-portal",
            ],
            env_extra={"OXIDESWARM_SECRET": dummy_hex},
            timeout=10.0,
        )
        self.assertNotEqual(r_env.returncode, 0)
        env_err = (r_env.stderr + "\n" + r_env.stdout).lower()
        self.assertIn("--isolation", env_err, r_env.stderr)
        self.assertIn("oxideswarm_secret", env_err, r_env.stderr)

    def test_t2_f1_04_same_fs_temp_layout_verifies_real_hardlink_control(self) -> None:
        secret_file = self.env.home / "samefs_secret.txt"
        secret_val = "samefs-nlink-secret"
        secret_file.write_text(secret_val, encoding="utf-8")
        sb_off = self.env.root / "sb_samefs_off"
        sb_off.mkdir()
        self.assertEqual(os.stat(secret_file).st_dev, os.stat(sb_off).st_dev)

        master = self.ox.start_master()
        self.ox.start_worker(
            master.listen_addr, name="w-samefs", isolation="off", sandbox_base_dir=sb_off
        )

        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "shell",
                "--env", f"T_SECRET={secret_file}",
                "--wait", "--json",
                "--command",
                'ln "$T_SECRET" "$RUSTY_GRID_SANDBOX_DIR/hl" '
                '&& python3 -c "import os; print(\'NLINK=\', os.stat(os.environ[\'T_SECRET\']).st_nlink)" '
                '&& cat "$RUSTY_GRID_SANDBOX_DIR/hl"',
            ],
        )
        self.assertEqual(res.returncode, 0, res.stderr)
        data = json.loads(res.stdout)
        self.assertEqual(data["exit_code"], 0)
        self.assertIn("NLINK= 2", data["stdout"])
        self.assertIn(secret_val, data["stdout"])

    def test_t2_f1_05_isolation_off_strips_cluster_secret_env_but_keeps_task_env(self) -> None:
        secret_file = self.ox.generate_secret_file()
        self.ox.secret_file = secret_file
        master = self.ox.start_master(secret_file=secret_file)
        self.ox.start_worker(
            master.listen_addr, name="w-env-scrub", isolation="off", secret_file=secret_file
        )

        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "shell",
                "--env", "TASK_CUSTOM_VAR=visible_val_05",
                "--wait", "--json",
                "--command",
                'echo "SEC=${OXIDESWARM_SECRET:-UNSET}" && echo "CUS=${TASK_CUSTOM_VAR}"',
            ],
            secret_file=secret_file,
        )
        self.assertEqual(res.returncode, 0, res.stderr)
        out = json.loads(res.stdout)["stdout"]
        self.assertIn("SEC=UNSET", out)
        self.assertIn("CUS=visible_val_05", out)

    # --- F2: TEST-41 Boundary ---

    def test_t2_f2_01_zero_ms_delay_sigterm_multi_round_loop(self) -> None:
        state_dir = self.env.root / "f2_t2_state_01"
        marker = self.env.root / "marker_loop.log"

        m = self.ox.start_master(
            name="m0",
            state_dir=state_dir,
            heartbeat_interval_secs=1,
            session_resume_hold_secs=30,
        )
        listen_addr = m.listen_addr
        self.ox.start_worker(listen_addr, name="w-loop", isolation="off")

        for rnd in range(5):
            gate = self.env.root / f"gate_{rnd}"
            done = self.env.root / f"done_{rnd}"
            sub = self.ox.submit_cli(
                listen_addr,
                [
                    "--type", "shell",
                    "--max-retries", "2",
                    "--json",
                    "--command", _gated_marker_script(marker, gate, done),
                ],
            )
            self.assertEqual(sub.returncode, 0, sub.stderr)
            task_id = json.loads(sub.stdout)["task_id"]

            _wait_until(
                lambda r=rnd: marker.is_file() and len(marker.read_text().splitlines()) >= r + 1,
                timeout=15.0,
                desc=f"round {rnd} marker",
            )
            gate.write_text("go\n", encoding="utf-8")
            _wait_until(done.is_file, timeout=10.0, interval=0.001, desc=f"round {rnd} done")
            # 0ms delay SIGTERM immediately after $DONE appears.
            self.assertEqual(m.proc.stop(sig=signal.SIGTERM, timeout=10.0), 0, m.proc.logs())

            time.sleep(0.1)
            m = self.ox.start_master(
                name=f"m{rnd+1}",
                listen=listen_addr,
                state_dir=state_dir,
                heartbeat_interval_secs=1,
                session_resume_hold_secs=30,
            )
            res = self.ox.result_cli(listen_addr, task_id, wait=True, json_out=True)
            self.assertEqual(res.returncode, 0, res.stderr)
            self.assertEqual(json.loads(res.stdout)["exit_code"], 0)
            time.sleep(0.25)
            lines = marker.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), rnd + 1, f"Double execution at round {rnd}: {lines!r}")

    def test_t2_f2_02_sigterm_while_task_queued_runs_once_after_restart(self) -> None:
        state_dir = self.env.root / "f2_t2_state_02"
        marker = self.env.root / "marker_queued.log"
        m1 = self.ox.start_master(name="m1", state_dir=state_dir)

        sub = self.ox.submit_cli(
            m1.listen_addr,
            [
                "--type", "shell",
                "--json",
                "--command", f'printf "queued_run\\n" >> "{marker}"; echo QUEUED_DONE',
            ],
        )
        self.assertEqual(sub.returncode, 0, sub.stderr)
        task_id = json.loads(sub.stdout)["task_id"]
        self.assertFalse(marker.exists())

        self.assertEqual(m1.proc.stop(sig=signal.SIGTERM, timeout=10.0), 0)
        m2 = self.ox.start_master(name="m2", listen=m1.listen_addr, state_dir=state_dir)
        self.ox.start_worker(m2.listen_addr, name="w-queued", isolation="off")

        res = self.ox.result_cli(m2.listen_addr, task_id, wait=True, json_out=True)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("QUEUED_DONE", json.loads(res.stdout)["stdout"])
        time.sleep(0.3)
        self.assertEqual(len(marker.read_text(encoding="utf-8").splitlines()), 1)

    def test_t2_f2_03_worker_reconnect_within_resume_hold_vs_expired_hold(self) -> None:
        # Branch 1: Within-hold reconnect preserves session without failover re-execution.
        state_dir_in = self.env.root / "f2_t2_state_03_in"
        m_in1 = self.ox.start_master(
            name="m-in1",
            state_dir=state_dir_in,
            heartbeat_interval_secs=1,
            heartbeat_timeout_secs=3,
            reaper_interval_secs=1,
            session_resume_hold_secs=15,
        )
        self.ox.start_worker(m_in1.listen_addr, name="w-in-hold", isolation="off")
        marker_in = self.env.root / "marker_hold_in.log"
        gate_in = self.env.root / "gate_hold_in"
        done_in = self.env.root / "done_hold_in"
        sub_in = self.ox.submit_cli(
            m_in1.listen_addr,
            [
                "--type", "shell",
                "--max-retries", "2",
                "--json",
                "--command", _gated_marker_script(marker_in, gate_in, done_in),
            ],
        )
        self.assertEqual(sub_in.returncode, 0, sub_in.stderr)
        tid_in = json.loads(sub_in.stdout)["task_id"]
        _wait_until(
            lambda: marker_in.is_file() and len(marker_in.read_text().splitlines()) >= 1,
            timeout=10.0,
            desc="within-hold task started",
        )
        gate_in.write_text("go\n", encoding="utf-8")
        _wait_until(done_in.is_file, timeout=10.0, interval=0.002, desc="within-hold done")
        self.assertEqual(m_in1.proc.stop(sig=signal.SIGTERM, timeout=10.0), 0)

        m_in2 = self.ox.start_master(
            name="m-in2",
            listen=m_in1.listen_addr,
            state_dir=state_dir_in,
            heartbeat_interval_secs=1,
            heartbeat_timeout_secs=3,
            reaper_interval_secs=1,
            session_resume_hold_secs=15,
        )
        self.ox.start_worker(m_in2.listen_addr, name="w-in-spare", isolation="off")
        res_in = self.ox.result_cli(m_in2.listen_addr, tid_in, wait=True, json_out=True, timeout=20.0)
        self.assertEqual(res_in.returncode, 0, res_in.stderr)
        self.assertEqual(json.loads(res_in.stdout)["exit_code"], 0)
        time.sleep(0.25)
        self.assertEqual(len(marker_in.read_text(encoding="utf-8").splitlines()), 1)
        m_in2.proc.stop(sig=signal.SIGTERM, timeout=10.0)

        # Branch 2: Expired-hold failover when worker is SIGKILLed and hold (2s) expires.
        m = self.ox.start_master(
            heartbeat_interval_secs=1,
            heartbeat_timeout_secs=3,
            reaper_interval_secs=1,
            session_resume_hold_secs=2,
        )
        w1 = self.ox.start_worker(m.listen_addr, name="w-hold-1", isolation="off")
        marker = self.env.root / "marker_hold.log"
        gate = self.env.root / "gate_hold"
        done = self.env.root / "done_hold"

        sub = self.ox.submit_cli(
            m.listen_addr,
            [
                "--type", "shell",
                "--max-retries", "2",
                "--json",
                "--command", _gated_marker_script(marker, gate, done),
            ],
        )
        task_id = json.loads(sub.stdout)["task_id"]
        _wait_until(
            lambda: marker.is_file() and len(marker.read_text().splitlines()) >= 1,
            timeout=10.0,
            desc="w1 started",
        )

        # SIGKILL w1 so resume hold (2s) expires without reconnect -> failover to w2.
        w1.proc.stop(sig=signal.SIGKILL, timeout=3.0)
        gate.write_text("go\n", encoding="utf-8")
        self.ox.start_worker(m.listen_addr, name="w-hold-2", isolation="off")

        res = self.ox.result_cli(m.listen_addr, task_id, wait=True, json_out=True, timeout=20.0)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertEqual(json.loads(res.stdout)["exit_code"], 0)
        self.assertTrue(done.is_file())
        self.assertEqual(len(marker.read_text(encoding="utf-8").splitlines()), 2)

    def test_t2_f2_04_max_retries_zero_restart_does_not_double_run_completed_task(self) -> None:
        state_dir = self.env.root / "f2_t2_state_04"
        marker = self.env.root / "marker_mr0.log"
        gate = self.env.root / "gate_mr0"
        done = self.env.root / "done_mr0"

        m1 = self.ox.start_master(
            name="m1", state_dir=state_dir, heartbeat_interval_secs=1, session_resume_hold_secs=30
        )
        self.ox.start_worker(m1.listen_addr, name="w-mr0", isolation="off")

        sub = self.ox.submit_cli(
            m1.listen_addr,
            [
                "--type", "shell",
                "--max-retries", "0",
                "--json",
                "--command", _gated_marker_script(marker, gate, done),
            ],
        )
        task_id = json.loads(sub.stdout)["task_id"]
        _wait_until(
            lambda: marker.is_file() and len(marker.read_text().splitlines()) >= 1,
            timeout=10.0,
            desc="marker line",
        )
        gate.write_text("go\n", encoding="utf-8")
        _wait_until(done.is_file, timeout=10.0, interval=0.002, desc="done flag")

        self.assertEqual(m1.proc.stop(sig=signal.SIGTERM, timeout=10.0), 0)
        m2 = self.ox.start_master(
            name="m2",
            listen=m1.listen_addr,
            state_dir=state_dir,
            heartbeat_interval_secs=1,
            session_resume_hold_secs=30,
        )
        res = self.ox.result_cli(m2.listen_addr, task_id, wait=True, json_out=True)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertEqual(json.loads(res.stdout)["exit_code"], 0)
        time.sleep(0.3)
        self.assertEqual(len(marker.read_text(encoding="utf-8").splitlines()), 1)

    def test_t2_f2_05_consecutive_double_sigterm_restart_preserves_terminal_state(self) -> None:
        state_dir = self.env.root / "f2_t2_state_05"
        marker = self.env.root / "marker_dbl.log"
        gate = self.env.root / "gate_dbl"
        done = self.env.root / "done_dbl"

        m1 = self.ox.start_master(
            name="m1", state_dir=state_dir, heartbeat_interval_secs=1, session_resume_hold_secs=30
        )
        self.ox.start_worker(m1.listen_addr, name="w-dbl", isolation="off")

        sub = self.ox.submit_cli(
            m1.listen_addr,
            ["--type", "shell", "--json", "--command", _gated_marker_script(marker, gate, done)],
        )
        task_id = json.loads(sub.stdout)["task_id"]
        _wait_until(
            lambda: marker.is_file() and len(marker.read_text().splitlines()) >= 1,
            timeout=10.0,
            desc="marker",
        )
        gate.write_text("go\n", encoding="utf-8")
        _wait_until(done.is_file, timeout=10.0, interval=0.002, desc="done")

        self.assertEqual(m1.proc.stop(sig=signal.SIGTERM, timeout=10.0), 0)
        m2 = self.ox.start_master(
            name="m2",
            listen=m1.listen_addr,
            state_dir=state_dir,
            heartbeat_interval_secs=1,
            session_resume_hold_secs=30,
        )
        time.sleep(0.05)
        self.assertEqual(m2.proc.stop(sig=signal.SIGTERM, timeout=10.0), 0)
        m3 = self.ox.start_master(
            name="m3",
            listen=m1.listen_addr,
            state_dir=state_dir,
            heartbeat_interval_secs=1,
            session_resume_hold_secs=30,
        )

        res = self.ox.result_cli(m3.listen_addr, task_id, wait=True, json_out=True)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertEqual(json.loads(res.stdout)["exit_code"], 0)
        time.sleep(0.35)
        self.assertEqual(len(marker.read_text(encoding="utf-8").splitlines()), 1)

    # --- F3: AX-1 CLI Boundary ---

    def test_t2_f3_01_unplaced_cell_fails_promptly_without_hanging(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-unp-a", tags=["cell-a"])

        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--matrix", "tag=cell-a,cell-missing",
                "--json",
                "--timeout-secs", "30",
                "--command", "echo", "--", "should_not_hang",
            ],
            timeout=15.0,
        )
        self.assertNotEqual(res.returncode, 0)
        self.assertLess(res.elapsed_s, 5.0, "Unplaced cell hung instead of failing pre-flight")
        cells = _extract_matrix_cells(json.loads(res.stdout))
        self.assertEqual(len(cells), 2)
        unplaced = [c for c in cells if _cell_is_unplaced(c)]
        self.assertEqual(len(unplaced), 1, f"Expected 1 unplaced cell: {cells!r}")
        self.assertIn("cell-missing", _cell_label_text(unplaced[0]))
        placeable = [c for c in cells if not _cell_is_unplaced(c)]
        self.assertEqual(len(placeable), 1, f"Expected 1 placeable cell: {cells!r}")
        self.assertEqual(placeable[0].get("exit_code"), 0, placeable[0])

        # 0-placeable matrix must exit < 5.0s and skip input blob upload completely.
        skip_dir = self.env.work / "skip_upload_inputs"
        skip_dir.mkdir()
        (skip_dir / "payload.bin").write_bytes(b"Z" * 16384)
        assert master.dashboard_port is not None
        metrics_url = f"http://127.0.0.1:{master.dashboard_port}/metrics"
        _, before_txt = http_get_text(metrics_url)
        before_objs = parse_prom_metric(before_txt, "oxideswarm_blob_store_objects") or 0.0

        res_zero = self.ox.submit_cli(
            master.listen_addr,
            [
                "--matrix", "tag=nope-1,nope-2",
                "--input", f"{skip_dir}:in",
                "--json",
                "--timeout-secs", "30",
                "--command", "echo", "--", "never_runs",
            ],
            timeout=15.0,
        )
        self.assertNotEqual(res_zero.returncode, 0)
        self.assertLess(res_zero.elapsed_s, 5.0, "0-placeable matrix hung")
        zero_cells = _extract_matrix_cells(json.loads(res_zero.stdout))
        self.assertEqual(len(zero_cells), 2)
        self.assertTrue(all(_cell_is_unplaced(c) for c in zero_cells), zero_cells)
        _, after_txt = http_get_text(metrics_url)
        after_objs = parse_prom_metric(after_txt, "oxideswarm_blob_store_objects") or 0.0
        self.assertEqual(after_objs - before_objs, 0.0, "0-placeable matrix uploaded blobs")

    def test_t2_f3_02_partial_cell_failure_exits_nonzero_with_per_cell_codes(self) -> None:
        master = self.ox.start_master()
        sb1 = self.env.root / "sb_p1_pass"
        sb2 = self.env.root / "sb_p2_fail"
        sb1.mkdir()
        sb2.mkdir()
        self.ox.start_worker(master.listen_addr, name="w-p1", tags=["p-1"], sandbox_base_dir=sb1)
        self.ox.start_worker(master.listen_addr, name="w-p2", tags=["p-2"], sandbox_base_dir=sb2)

        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--matrix", "tag=p-1,p-2",
                "--json",
                "--command", "sh", "--", "-c",
                'case "$RUSTY_GRID_SANDBOX_DIR" in *sb_p1_pass*) echo PASS_CELL; exit 0 ;; *) echo FAIL_CELL; exit 7 ;; esac',
            ],
        )
        self.assertNotEqual(res.returncode, 0)
        cells = _extract_matrix_cells(json.loads(res.stdout))
        self.assertEqual(len(cells), 2)
        codes = sorted(c.get("exit_code") for c in cells)
        self.assertEqual(codes, [0, 7], f"Unexpected per-cell exit codes: {cells!r}")

    def test_t2_f3_03_exceeding_max_matrix_cell_limit_rejected_cleanly(self) -> None:
        master = self.ox.start_master()
        # 16 x 16 = 256 cells exceeds DEFAULT_MAX_MATRIX_CELLS (64) cleanly at validation.
        axis1 = ",".join(f"a{i}" for i in range(16))
        axis2 = ",".join(f"b{i}" for i in range(16))
        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--matrix", f"tag={axis1}",
                "--matrix", f"tag={axis2}",
                "--json",
                "--command", "echo", "--", "over-limit",
            ],
            timeout=10.0,
        )
        self.assertNotEqual(res.returncode, 0)
        self.assertLess(res.elapsed_s, 8.0)

        # Explicit --max-matrix-cells 2 rejects 3 cells, and --max-matrix-cells 257 exceeds hard cap 256.
        res_custom = self.ox.submit_cli(
            master.listen_addr,
            [
                "--matrix", "tag=c1,c2,c3",
                "--max-matrix-cells", "2",
                "--command", "echo", "--", "over-custom",
            ],
            timeout=10.0,
        )
        self.assertNotEqual(res_custom.returncode, 0)
        self.assertLess(res_custom.elapsed_s, 8.0)

        res_hard = self.ox.submit_cli(
            master.listen_addr,
            [
                "--matrix", "tag=c1",
                "--max-matrix-cells", "257",
                "--command", "echo", "--", "over-hard-cap",
            ],
            timeout=10.0,
        )
        self.assertNotEqual(res_hard.returncode, 0)
        self.assertLess(res_hard.elapsed_s, 8.0)

    def test_t2_f3_04_malformed_or_empty_matrix_syntax_rejected_nonzero(self) -> None:
        master = self.ox.start_master()
        for bad in ("", "keyonly", "=v1,v2", "tag=", "tag=a,,b", "env:BAD=KEY=1", "iso=off"):
            res = self.ox.submit_cli(
                master.listen_addr,
                ["--matrix", bad, "--command", "echo", "--", "bad"],
                timeout=10.0,
            )
            self.assertNotEqual(res.returncode, 0, f"Accepted malformed --matrix={bad!r}")

    def test_t2_f3_05_conflicting_require_tag_or_singular_matrix_handled_cleanly(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-conf", tags=["only-a"])

        # Combining --require-tag only-a with --matrix tag=only-b yields unmatchable cell;
        # combining --os macos with --matrix os=linux conflicts on singular os key.
        res1 = self.ox.submit_cli(
            master.listen_addr,
            [
                "--require-tag", "only-a",
                "--matrix", "tag=only-b",
                "--json",
                "--timeout-secs", "20",
                "--command", "echo", "--", "conflict",
            ],
            timeout=12.0,
        )
        self.assertNotEqual(res1.returncode, 0)
        self.assertLess(res1.elapsed_s, 10.0)

        res2 = self.ox.submit_cli(
            master.listen_addr,
            [
                "--os", "macos",
                "--matrix", "os=macos,linux",
                "--json",
                "--command", "echo", "--", "dup-os",
            ],
            timeout=10.0,
        )
        self.assertNotEqual(res2.returncode, 0)

    # --- F4: AX-1 MCP Boundary ---

    def test_t2_f4_01_mcp_unplaced_matrix_cell_returns_promptly_without_hang(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-mu", tags=["mu-ok"])

        mcp = McpClient(self.env, master.listen_addr)
        mcp.initialize()
        tools = mcp.tools_list()
        matrix_arg = _build_mcp_matrix_arg(tools, {"tag": ["mu-ok", "mu-missing"]})

        t0 = time.monotonic()
        resp = mcp.call_tool(
            "submit",
            {
                "type": "command",
                "command": "echo",
                "args": ["mcp-unplaced"],
                "matrix": matrix_arg,
                "timeout_secs": 30,
                "wait": True,
            },
            timeout=15.0,
        )
        elapsed = time.monotonic() - t0
        self.assertLess(elapsed, 5.0, f"MCP unplaced cell hung ({elapsed:.2f}s)")
        is_err, text = _mcp_tool_text(resp)
        self.assertTrue(is_err, f"Expected isError=true on unplaced cell: {resp!r}")
        cells = _extract_matrix_cells(json.loads(text))
        self.assertEqual(len(cells), 2)
        unplaced = [c for c in cells if _cell_is_unplaced(c)]
        self.assertEqual(len(unplaced), 1, f"Expected 1 unplaced cell: {cells!r}")
        self.assertIn("mu-missing", _cell_label_text(unplaced[0]))
        placeable = [c for c in cells if not _cell_is_unplaced(c)]
        self.assertEqual(len(placeable), 1, f"Expected 1 placeable cell: {cells!r}")
        self.assertEqual(placeable[0].get("exit_code"), 0, placeable[0])
        mcp.close_stdin()

    def test_t2_f4_02_mcp_invalid_matrix_schema_returns_error_and_server_stays_alive(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-rec-mcp", tags=["rec-ok"])
        mcp = McpClient(self.env, master.listen_addr)
        mcp.initialize()

        for bad_matrix in (
            {},
            [],
            "not-valid",
            {"tag": [f"x{i}" for i in range(200)]},
            ["env:BAD=KEY=1"],
            ["iso=off"],
            ["tag=a=b"],
            [123],
        ):
            resp = mcp.call_tool(
                "submit",
                {
                    "type": "command",
                    "command": "echo",
                    "args": ["bad"],
                    "matrix": bad_matrix,
                    "wait": True,
                },
                timeout=10.0,
            )
            self.assertIn("error", resp, f"Expected JSON-RPC -32602 for {bad_matrix!r}: {resp!r}")
            self.assertEqual(resp["error"].get("code"), -32602, resp)

        # Verify full tool recovery with a valid submit call + ping after -32602 errors.
        ok_resp = mcp.call_tool(
            "submit",
            {
                "type": "command",
                "command": "echo",
                "args": ["MCP_RECOVERED"],
                "matrix": ["tag=rec-ok"],
                "wait": True,
            },
            timeout=15.0,
        )
        ok_err, ok_text = _mcp_tool_text(ok_resp)
        self.assertFalse(ok_err, ok_text)
        ok_cells = _extract_matrix_cells(json.loads(ok_text))
        self.assertEqual(len(ok_cells), 1)
        self.assertEqual(ok_cells[0].get("exit_code"), 0, ok_cells[0])
        self.assertIn("MCP_RECOVERED", _cell_stdout(ok_cells[0]))

        pong = mcp.ping()
        self.assertEqual(pong.get("result"), {})
        mcp.close_stdin()

    def test_t2_f4_03_mcp_partial_cell_failure_reports_failure_cleanly(self) -> None:
        master = self.ox.start_master()
        sb1 = self.env.root / "sb_mf1_pass"
        sb2 = self.env.root / "sb_mf2_fail"
        sb1.mkdir()
        sb2.mkdir()
        self.ox.start_worker(master.listen_addr, name="w-mf1", tags=["mf-1"], sandbox_base_dir=sb1)
        self.ox.start_worker(master.listen_addr, name="w-mf2", tags=["mf-2"], sandbox_base_dir=sb2)

        mcp = McpClient(self.env, master.listen_addr)
        mcp.initialize()
        tools = mcp.tools_list()
        matrix_arg = _build_mcp_matrix_arg(tools, {"tag": ["mf-1", "mf-2"]})

        resp = mcp.call_tool(
            "submit",
            {
                "type": "shell",
                "script": 'case "$RUSTY_GRID_SANDBOX_DIR" in *sb_mf1_pass*) echo OK; exit 0 ;; *) exit 9 ;; esac',
                "matrix": matrix_arg,
                "wait": True,
            },
        )
        is_err, text = _mcp_tool_text(resp)
        cells = _extract_matrix_cells(json.loads(text))
        codes = sorted(c.get("exit_code") for c in cells)
        self.assertEqual(codes, [0, 9])
        self.assertTrue(is_err, f"Expected isError=true on partial failure: {resp!r}")
        self.assertEqual(mcp.ping().get("result"), {})
        mcp.close_stdin()

    def test_t2_f4_04_mcp_stdin_eof_grace_window_exits_cleanly(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-eof", tags=["eof-1"])
        mcp = McpClient(self.env, master.listen_addr)
        mcp.initialize()
        tools = mcp.tools_list()
        matrix_arg = _build_mcp_matrix_arg(tools, {"tag": ["eof-1"]})

        mcp.send(
            {
                "jsonrpc": "2.0",
                "id": 99,
                "method": "tools/call",
                "params": {
                    "name": "submit",
                    "arguments": {
                        "type": "command",
                        "command": "echo",
                        "args": ["grace-eof"],
                        "matrix": matrix_arg,
                        "wait": True,
                    },
                },
            }
        )
        mcp.close_stdin()
        resp: Optional[dict[str, Any]] = None
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            msg = mcp.recv_jsonrpc(timeout=max(0.5, deadline - time.monotonic()))
            if msg.get("id") == 99:
                resp = msg
                break
        self.assertIsNotNone(resp, f"Did not receive id=99 reply before EOF:\n{mcp.mp.logs()}")
        rc = mcp.mp.wait(timeout=10.0)
        self.assertEqual(rc, 0, mcp.mp.logs())

    def test_t2_f4_05_mcp_multiline_large_cell_stdout_preserves_jsonrpc_framing(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-big1", tags=["big-1"])
        self.ox.start_worker(master.listen_addr, name="w-big2", tags=["big-2"])

        mcp = McpClient(self.env, master.listen_addr)
        mcp.initialize()
        tools = mcp.tools_list()
        matrix_arg = _build_mcp_matrix_arg(tools, {"tag": ["big-1", "big-2"]})

        resp = mcp.call_tool(
            "submit",
            {
                "type": "shell",
                "script": 'python3 -c \'print("{\\"k\\":\\"v\\\\n\\"}\\n" * 600)\'',
                "matrix": matrix_arg,
                "wait": True,
            },
        )
        is_err, text = _mcp_tool_text(resp)
        self.assertFalse(is_err, text)
        cells = _extract_matrix_cells(json.loads(text))
        self.assertEqual(len(cells), 2)
        for line in mcp.all_stdout_lines:
            self.assertEqual(json.loads(line)["jsonrpc"], "2.0")
        mcp.close_stdin()

    # --- F5: FEAT-5 Boundary ---

    def test_t2_f5_01_output_glob_excludes_checkpoint_dir_files(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-cp-excl")
        out_dir = self.env.root / "excl_out"

        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "shell",
                "--output", "**/*",
                "--output", "./.Oxideswarm_Checkpoint/**",
                "--output-dir", str(out_dir),
                "--json",
                "--command",
                'echo "INTERNAL_CP_SECRET" > "$OXIDESWARM_CHECKPOINT_DIR/internal.cp"\n'
                'echo "PUBLIC_OUTPUT" > artifact.txt\n',
            ],
        )
        self.assertEqual(res.returncode, 0, res.stderr)
        data = json.loads(res.stdout)
        self.assertEqual(data["exit_code"], 0)

        collected = [p.name for p in out_dir.rglob("*") if p.is_file()]
        self.assertIn("artifact.txt", collected)
        self.assertNotIn("internal.cp", collected)
        output_paths = [o.get("path", "") for o in data.get("outputs", [])]
        self.assertFalse(any("internal.cp" in p for p in output_paths), output_paths)

        # Literal --output targeting reserved checkpoint dir must be rejected.
        lit_res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "shell",
                "--output", ".oxideswarm_checkpoint/internal.cp",
                "--wait",
                "--command", 'echo "INTERNAL" > "$OXIDESWARM_CHECKPOINT_DIR/internal.cp"',
            ],
        )
        self.assertNotEqual(lit_res.returncode, 0, lit_res.stdout)

    def test_t2_f5_02_symlink_inside_checkpoint_dir_cannot_exfiltrate_outside(self) -> None:
        master = self.ox.start_master(
            session_resume_hold_secs=0,
            heartbeat_interval_secs=1,
            heartbeat_timeout_secs=3,
            reaper_interval_secs=1,
        )
        outside_secret = self.env.root / "outside_host_secret.txt"
        outside_secret.write_text("DO_NOT_EXFILTRATE_VIA_CP_SYMLINK", encoding="utf-8")

        w1 = self.ox.start_worker(master.listen_addr, name="w-sym-1", isolation="off")
        cp_ready = self.env.root / "cp_sym_ready"
        attempt_log = self.env.root / "cp_sym_attempts.log"
        leak_probe = self.env.root / "cp_sym_probe.txt"

        script = (
            'set -e\n'
            f'echo "ATTEMPT" >> "{attempt_log}"\n'
            f'if [ $(wc -l < "{attempt_log}") -ge 2 ]; then\n'
            '  if [ -e "$OXIDESWARM_CHECKPOINT_DIR/escape_link" ] || [ -L "$OXIDESWARM_CHECKPOINT_DIR/escape_link" ]; then\n'
            f'    cat "$OXIDESWARM_CHECKPOINT_DIR/escape_link" > "{leak_probe}" 2>/dev/null || echo "SYMLINK_PRESENT" > "{leak_probe}"\n'
            '  else\n'
            f'    echo "NO_SYMLINK_RESTORED" > "{leak_probe}"\n'
            '  fi\n'
            '  exit 0\n'
            'fi\n'
            'echo "ok" > "$OXIDESWARM_CHECKPOINT_DIR/marker"\n'
            f'ln -s "{outside_secret}" "$OXIDESWARM_CHECKPOINT_DIR/escape_link"\n'
            f'touch "{cp_ready}"\n'
            'sleep 30\n'
        )
        sub_proc = self.ox.submit_cli_popen(
            master.listen_addr,
            ["--type", "shell", "--max-retries", "2", "--wait", "--json", "--command", script],
        )
        _wait_until(cp_ready.is_file, timeout=15.0, desc="symlink cp ready")
        time.sleep(3.5)
        w1.proc.stop(sig=signal.SIGKILL, timeout=5.0)
        self.ox.start_worker(master.listen_addr, name="w-sym-2", isolation="off")

        rc = sub_proc.wait(timeout=30.0)
        self.assertEqual(rc, 0, sub_proc.logs())
        self.assertTrue(leak_probe.is_file(), sub_proc.logs())
        probe_txt = leak_probe.read_text(encoding="utf-8")
        self.assertEqual(probe_txt.strip(), "NO_SYMLINK_RESTORED")
        self.assertNotIn("DO_NOT_EXFILTRATE_VIA_CP_SYMLINK", probe_txt)

    def test_t2_f5_03_empty_checkpoint_dir_handled_cleanly(self) -> None:
        master = self.ox.start_master(
            session_resume_hold_secs=0,
            heartbeat_interval_secs=1,
            heartbeat_timeout_secs=3,
            reaper_interval_secs=1,
        )
        w1 = self.ox.start_worker(master.listen_addr, name="w-emp-1", isolation="off")
        started = self.env.root / "emp_started"
        attempt_log = self.env.root / "emp_attempts.log"

        script = (
            'set -e\n'
            f'echo "ATTEMPT" >> "{attempt_log}"\n'
            f'if [ $(wc -l < "{attempt_log}") -ge 2 ]; then\n'
            '  COUNT=$(ls -A "$OXIDESWARM_CHECKPOINT_DIR" | wc -l | tr -d " ")\n'
            '  echo "EMPTY_CP_COUNT=$COUNT"\n'
            '  exit 0\n'
            'fi\n'
            f'touch "{started}"\n'
            'sleep 30\n'
        )
        sub_proc = self.ox.submit_cli_popen(
            master.listen_addr,
            ["--type", "shell", "--max-retries", "2", "--wait", "--json", "--command", script],
        )
        _wait_until(started.is_file, timeout=15.0, desc="empty cp task started")
        time.sleep(2.5)
        w1.proc.stop(sig=signal.SIGKILL, timeout=5.0)
        self.ox.start_worker(master.listen_addr, name="w-emp-2", isolation="off")

        rc = sub_proc.wait(timeout=30.0)
        self.assertEqual(rc, 0, sub_proc.logs())
        data = json.loads("\n".join(sub_proc.stdout_lines))
        self.assertEqual(data["exit_code"], 0)
        self.assertIn("EMPTY_CP_COUNT=0", data["stdout"])

    def test_t2_f5_04_oversized_checkpoint_handled_without_crashing_worker_or_master(self) -> None:
        # Sub-check 1: Master --max-active-mb 1 rejects 2 MiB checkpoint without crashing.
        master = self.ox.start_master(
            max_active_mb=1,
            session_resume_hold_secs=0,
            heartbeat_interval_secs=1,
            heartbeat_timeout_secs=3,
            reaper_interval_secs=1,
        )
        w1 = self.ox.start_worker(master.listen_addr, name="w-ovs-1", isolation="off")
        big_ready = self.env.root / "ovs_ready"
        attempt_log = self.env.root / "ovs_attempts.log"
        ovs_probe = self.env.root / "ovs_probe.txt"

        script = (
            'set -e\n'
            f'echo "ATTEMPT" >> "{attempt_log}"\n'
            f'if [ $(wc -l < "{attempt_log}") -ge 2 ]; then\n'
            '  if [ ! -f "$OXIDESWARM_CHECKPOINT_DIR/huge.bin" ]; then\n'
            f'    echo "OVERSIZED_NOT_RESTORED" > "{ovs_probe}"\n'
            '    echo "OVERSIZED_NOT_RESTORED"\n'
            '    exit 0\n'
            '  fi\n'
            f'  echo "UNEXPECTED_RESTORED" > "{ovs_probe}"\n'
            '  exit 2\n'
            'fi\n'
            'dd if=/dev/zero of="$OXIDESWARM_CHECKPOINT_DIR/huge.bin" bs=1048576 count=2 2>/dev/null\n'
            f'touch "{big_ready}"\n'
            'sleep 30\n'
        )
        sub_proc = self.ox.submit_cli_popen(
            master.listen_addr,
            ["--type", "shell", "--max-retries", "2", "--wait", "--json", "--command", script],
        )
        _wait_until(big_ready.is_file, timeout=15.0, desc="oversized cp written")
        time.sleep(3.5)
        w1.proc.stop(sig=signal.SIGKILL, timeout=5.0)
        w2 = self.ox.start_worker(master.listen_addr, name="w-ovs-2", isolation="off")

        sub_rc = sub_proc.wait(timeout=30.0)
        self.assertEqual(sub_rc, 0, sub_proc.logs())
        self.assertEqual(json.loads("\n".join(sub_proc.stdout_lines))["exit_code"], 0)
        self.assertTrue(ovs_probe.is_file(), sub_proc.logs())
        self.assertEqual(ovs_probe.read_text(encoding="utf-8").strip(), "OVERSIZED_NOT_RESTORED")
        self.assertEqual(len(attempt_log.read_text(encoding="utf-8").splitlines()), 2)

        # Master and w2 must remain alive and healthy after oversized checkpoint rejection/drop.
        self.assertIsNone(master.proc.poll(), master.proc.logs())
        self.assertIsNone(w2.proc.poll(), w2.proc.logs())
        health_res = self.ox.submit_cli(
            master.listen_addr,
            ["--type", "command", "--wait", "--json", "--command", "echo", "--", "STILL_ALIVE"],
        )
        self.assertEqual(health_res.returncode, 0, health_res.stderr)
        self.assertIn("STILL_ALIVE", json.loads(health_res.stdout)["stdout"])

        # Sub-check 2: Worker MAX_PROCESS_CHECKPOINT_BYTES (12 MiB) cap without --max-active-mb 1.
        master_12m = self.ox.start_master(name="m-12m")
        w_12m = self.ox.start_worker(master_12m.listen_addr, name="w-12m", isolation="off")
        res_13m = self.ox.submit_cli(
            master_12m.listen_addr,
            [
                "--type", "shell",
                "--wait", "--json",
                "--command",
                'dd if=/dev/zero of="$OXIDESWARM_CHECKPOINT_DIR/too_big_13m.bin" bs=1048576 count=13 2>/dev/null && sleep 1.5 && echo "WORKER_13M_DONE"',
            ],
            timeout=30.0,
        )
        self.assertEqual(res_13m.returncode, 0, res_13m.stderr)
        data_13m = json.loads(res_13m.stdout)
        self.assertEqual(data_13m["exit_code"], 0, data_13m)
        self.assertIn("WORKER_13M_DONE", data_13m["stdout"])
        self.assertIsNone(master_12m.proc.poll(), master_12m.proc.logs())
        self.assertIsNone(w_12m.proc.poll(), w_12m.proc.logs())
        health_12m = self.ox.submit_cli(
            master_12m.listen_addr,
            ["--type", "command", "--wait", "--json", "--command", "echo", "--", "STILL_ALIVE_12M"],
        )
        self.assertEqual(health_12m.returncode, 0, health_12m.stderr)
        self.assertIn("STILL_ALIVE_12M", json.loads(health_12m.stdout)["stdout"])

    def test_t2_f5_05_binary_nul_nested_checkpoint_roundtrip_and_non_oxcp1_isolation(self) -> None:
        """Verifies binary/NUL/nested checkpoint files and literal `OXCP1` filename roundtrip.

        Wire-level rejection of non-`OXCP1` `CheckpointData.delta_state` payloads (e.g., 3D/BuiltinTest
        JSON progress) is enforced in Rust crate tests (`test_feat5_process_checkpoints.rs`).
        """
        master = self.ox.start_master(
            session_resume_hold_secs=0,
            heartbeat_interval_secs=1,
            heartbeat_timeout_secs=3,
            reaper_interval_secs=1,
        )
        w1 = self.ox.start_worker(master.listen_addr, name="w-bin-1", isolation="off")
        cp_ready = self.env.root / "bin_cp_ready"
        verified_out = self.env.root / "bin_cp_verified.txt"

        py_script = (
            "import os, pathlib\n"
            "cp = pathlib.Path(os.environ['OXIDESWARM_CHECKPOINT_DIR'])\n"
            "sub = cp / 'nested' / 'dir'\n"
            "target = sub / 'blob.bin'\n"
            "json_like = cp / 'OXCP1'\n"
            "expected = b'\\x00\\x01OXCP1\\xff\\xfe{\"progress\":50}\\n'\n"
            "if target.is_file() and target.read_bytes() == expected and json_like.read_text() == '{\"legacy\":1}':\n"
            f"    pathlib.Path({str(verified_out)!r}).write_text('BIN_ROUNDTRIP_OK')\n"
            "    raise SystemExit(0)\n"
            "sub.mkdir(parents=True, exist_ok=True)\n"
            "target.write_bytes(expected)\n"
            "json_like.write_text('{\"legacy\":1}')\n"
            f"pathlib.Path({str(cp_ready)!r}).write_text('1')\n"
            "import time; time.sleep(30)\n"
        )
        sub_proc = self.ox.submit_cli_popen(
            master.listen_addr,
            [
                "--type", "command",
                "--max-retries", "2",
                "--wait", "--json",
                "--command", "python3", "--", "-c", py_script,
            ],
        )
        _wait_until(cp_ready.is_file, timeout=15.0, desc="binary cp ready")
        time.sleep(3.5)
        w1.proc.stop(sig=signal.SIGKILL, timeout=5.0)
        self.ox.start_worker(master.listen_addr, name="w-bin-2", isolation="off")

        rc = sub_proc.wait(timeout=30.0)
        self.assertEqual(rc, 0, sub_proc.logs())
        self.assertTrue(verified_out.is_file())
        self.assertEqual(verified_out.read_text(encoding="utf-8"), "BIN_ROUNDTRIP_OK")

    # --- F6: MESH-2 Boundary ---

    def test_t2_f6_01_data_to_missing_node_exits_nonzero_promptly(self) -> None:
        hub = self.mesh.start_hub()
        inbox = self.env.root / "inbox_miss"
        inbox.mkdir()
        self.mesh.start_node(hub, "present-node", inbox_dir=inbox)

        res = self.mesh.run_data(hub, to_node="missing-node", payload="orphan-payload")
        self.assertNotEqual(res.returncode, 0)
        self.assertLess(res.elapsed_s, 10.0)
        self.assertEqual(_collect_inbox_payloads(inbox), [])

    def test_t2_f6_02_node_invalid_inbox_dir_regular_file_fails_cleanly(self) -> None:
        hub = self.mesh.start_hub()
        reg_file = self.env.root / "not_a_dir.txt"
        reg_file.write_text("regular-file", encoding="utf-8")

        node = self.mesh.start_node(
            hub, "bad-inbox-node", inbox_dir=reg_file, wait_registered=False
        )
        rc = node.wait(timeout=10.0)
        self.assertNotEqual(rc, 0, node.logs())

        # Symlink --inbox-dir (both without and with trailing slash) must be rejected at startup.
        real_dir = self.env.root / "real_inbox_target"
        real_dir.mkdir()
        sym_inbox = self.env.root / "sym_inbox"
        sym_inbox.symlink_to(real_dir)

        for suffix, node_id in (("", "sym-inbox-node"), ("/", "sym-inbox-slash-node")):
            r_sym = self.env.run_cmd(
                [
                    self.mesh.bin,
                    "node",
                    "--hub",
                    hub.ws_url,
                    "--id",
                    node_id,
                    "--inbox-dir",
                    f"{sym_inbox}{suffix}",
                ],
                timeout=10.0,
            )
            self.assertNotEqual(
                r_sym.returncode,
                0,
                f"Expected symlink --inbox-dir ({suffix!r}) rejection: {r_sym.stdout}\n{r_sym.stderr}",
            )

    def test_t2_f6_03_readonly_inbox_dir_triggers_nack_and_nonzero_exit(self) -> None:
        if os.geteuid() == 0:
            self.skipTest("chmod 0o555 does not block writes for root (uid 0)")

        hub = self.mesh.start_hub()
        ro_inbox = self.env.root / "ro_inbox"
        ro_inbox.mkdir()
        node = self.mesh.start_node(hub, "ro-consumer", inbox_dir=ro_inbox)

        ro_inbox.chmod(0o555)
        self.env.add_cleanup(lambda: ro_inbox.chmod(0o755))

        res = self.mesh.run_data(hub, to_node="ro-consumer", payload="cannot-persist")
        self.assertNotEqual(res.returncode, 0)
        self.assertEqual(_collect_inbox_payloads(ro_inbox), [])
        self.assertIsNone(node.poll(), node.logs())

    def test_t2_f6_04_empty_payload_and_raw_binary_bytes_verified_on_disk(self) -> None:
        hub = self.mesh.start_hub()
        inbox = self.env.root / "inbox_bin"
        inbox.mkdir()
        self.mesh.start_node(hub, "bin-consumer", inbox_dir=inbox)

        # 1) Empty payload via CLI --payload
        res_empty = self.mesh.run_data(hub, to_node="bin-consumer", payload="")
        self.assertEqual(res_empty.returncode, 0, res_empty.stderr)
        self.assertIn(EMPTY_SHA256, (res_empty.stdout + res_empty.stderr).lower())
        _wait_inbox_for_bytes(inbox, b"", timeout=5.0)

        # 2) Binary (\x00\x80\xff) payload via CLI --payload-file
        cli_bin = bytes([0x00, 0x80, 0xFF, 0xFE, 0x01, 0x42]) * 32
        cli_bin_path = self.env.root / "cli_bin_payload.bin"
        cli_bin_path.write_bytes(cli_bin)
        cli_bin_sha = hashlib.sha256(cli_bin).hexdigest()
        res_bin = self.mesh.run_data(hub, to_node="bin-consumer", payload_file=cli_bin_path)
        self.assertEqual(res_bin.returncode, 0, res_bin.stderr)
        self.assertIn(cli_bin_sha, res_bin.stdout.lower())
        self.assertIn("Chunks: 1", res_bin.stdout)
        _wait_inbox_for_bytes(inbox, cli_bin, timeout=5.0)

        # 3) >1 MiB multi-chunk binary payload via CLI --payload-file
        multi_bin = bytes((i % 256) for i in range(1024 * 1024 + 16384))
        multi_path = self.env.root / "cli_multichunk.bin"
        multi_path.write_bytes(multi_bin)
        multi_sha = hashlib.sha256(multi_bin).hexdigest()
        res_multi = self.mesh.run_data(
            hub,
            to_node="bin-consumer",
            payload_file=multi_path,
            extra_args=["--timeout-ms", "15000"],
            timeout=20.0,
        )
        self.assertEqual(res_multi.returncode, 0, res_multi.stderr)
        self.assertIn(multi_sha, res_multi.stdout.lower())
        self.assertIn("Chunks: 2", res_multi.stdout)
        _wait_inbox_for_bytes(inbox, multi_bin, timeout=8.0)

        # 4) Raw binary payload via CLI `--payload-file -` (stdin)
        stdin_bin = b"\x00\x80\xffstdin-raw\xfe\x00\xff"
        stdin_sha = hashlib.sha256(stdin_bin).hexdigest()
        res_stdin = self.mesh.run_data(
            hub,
            to_node="bin-consumer",
            payload_file="-",
            stdin_bytes=stdin_bin,
        )
        self.assertEqual(res_stdin.returncode, 0, res_stdin.stderr)
        self.assertIn(stdin_sha, res_stdin.stdout.lower())
        _wait_inbox_for_bytes(inbox, stdin_bin, timeout=5.0)

        # 5) Raw non-UTF8 / NUL binary payload via stdlib WS peer
        raw_bin = bytes(range(256)) + b"\x00\xff\x80\r\n" + os.urandom(512)
        ws = StdlibWsClient(hub.ws_url)
        self.env.add_cleanup(ws.close)
        ws.register("ws-bin-sender")
        corr = ws.send_data_payload("bin-consumer", raw_bin)
        ack = ws.recv_until(lambda m: m.get("correlation_id") == corr, timeout=8.0)
        self.assertEqual(ack["type"], "DeliveryAck", ack)
        _wait_inbox_for_bytes(inbox, raw_bin, timeout=5.0)

        # Ensure zero leftover .tmp* files in inbox
        self.assertFalse(any(p.name.startswith(".tmp_") for p in inbox.iterdir()))

    def test_t2_f6_05_corrupted_sha256_ws_frame_triggers_nack_and_concurrent_burst(self) -> None:
        hub = self.mesh.start_hub()
        inbox = self.env.root / "inbox_burst"
        inbox.mkdir()
        self.mesh.start_node(hub, "burst-consumer", inbox_dir=inbox)

        ws = StdlibWsClient(hub.ws_url)
        self.env.add_cleanup(ws.close)
        ws.register("ws-corrupt-sender")

        # 1) Corrupted SHA-256 must return DeliveryNack with ERR_PAYLOAD_CORRUPT and zero corrupt file on disk.
        bad_payload = b"corrupt-checksum-bytes"
        corr_bad = ws.send_data_payload(
            "burst-consumer", bad_payload, checksum_override="0" * 64
        )
        nack = ws.recv_until(lambda m: m.get("correlation_id") == corr_bad, timeout=8.0)
        self.assertEqual(nack["type"], "DeliveryNack", nack)
        self.assertEqual(nack.get("error_code"), "ERR_PAYLOAD_CORRUPT", nack)
        self.assertEqual(_collect_inbox_payloads(inbox), [])

        # 2) Concurrent 265-message burst (> 256 buffer cap) including 256 KiB payload must all ACK & persist.
        expected_blobs: list[bytes] = [
            f"burst-{i:03d}-".encode() + os.urandom(64) for i in range(264)
        ]
        large_blob = b"L" * (256 * 1024)
        expected_blobs.append(large_blob)
        self.assertEqual(len(expected_blobs), 265)

        corrs: set[str] = set()
        for blob in expected_blobs:
            corrs.add(ws.send_data_payload("burst-consumer", blob))

        acks = 0
        deadline = time.monotonic() + 25.0
        while acks < len(corrs) and time.monotonic() < deadline:
            msg = ws.recv_json(timeout=max(0.5, deadline - time.monotonic()))
            if msg.get("correlation_id") in corrs:
                self.assertEqual(msg["type"], "DeliveryAck", msg)
                acks += 1
        self.assertEqual(acks, len(corrs))

        def _all_persisted() -> bool:
            on_disk = {data for _, data in _collect_inbox_payloads(inbox)}
            return all(b in on_disk for b in expected_blobs)

        _wait_until(_all_persisted, timeout=15.0, desc="all 265 burst payloads on disk")
        self.assertFalse(any(p.name.startswith(".tmp_") for p in inbox.iterdir()))


# ===========================================================================
# TIER 3: Pairwise Cross-Feature Interactions (7 tests)
# ===========================================================================


class Tier3CrossFeatureTests(Wave2E2ETestBase):
    """Tier 3: Pairwise interactions between F1-F6."""

    def test_t3_01_ax1_cli_matrix_mixed_isolation_fs_and_off_workers(self) -> None:
        self._require_darwin_for_seatbelt()
        secret_file = self.env.home / "t3_secret.txt"
        secret_val = "t3-mixed-iso-secret"
        secret_file.write_text(secret_val, encoding="utf-8")

        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-t3-off", isolation="off", tags=["iso-off"])
        self.ox.start_worker(master.listen_addr, name="w-t3-fs", isolation="fs", tags=["iso-fs"])

        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "shell",
                "--matrix", "tag=iso-off,iso-fs",
                "--env", f"T_SECRET={secret_file}",
                "--json",
                "--command", _hardlink_test_script(),
            ],
        )
        self.assertEqual(res.returncode, 0, res.stderr)
        cells = _extract_matrix_cells(json.loads(res.stdout))
        self.assertEqual(len(cells), 2)
        by_worker = {_cell_worker(c): _cell_stdout(c) for c in cells}
        self.assertIn(secret_val, by_worker["w-t3-off"])
        self.assertNotIn(secret_val, by_worker["w-t3-fs"])
        self.assertIn("LINK_DENIED", by_worker["w-t3-fs"])

    def test_t3_02_ax1_cli_matrix_with_feat5_checkpoint_worker_sigkill_resume(self) -> None:
        master = self.ox.start_master(
            session_resume_hold_secs=0,
            heartbeat_interval_secs=1,
            heartbeat_timeout_secs=3,
            reaper_interval_secs=1,
        )
        w1 = self.ox.start_worker(master.listen_addr, name="w-mx-1", tags=["lane-a"])
        self.ox.start_worker(master.listen_addr, name="w-mx-2", tags=["lane-b"])

        cp_flag = self.env.root / "t3_mx_cp_ready"
        trace_a = self.env.root / "t3_trace_a.log"
        script = (
            'set -e\n'
            'CP="$OXIDESWARM_CHECKPOINT_DIR/state"\n'
            f'if [ -f "$CP" ]; then echo "CELL_RESUMED" >> "{trace_a}"; echo OK_RESUME; exit 0; fi\n'
            f'echo "CELL_STEP1" >> "{trace_a}"\n'
            'echo "DONE1" > "$CP"\n'
            'case "$OXIDESWARM_MATRIX_CELL" in\n'
            f'  *lane-a*) touch "{cp_flag}"; sleep 30 ;;\n'
            '  *) echo OK_FAST ;;\n'
            'esac\n'
        )
        sub_proc = self.ox.submit_cli_popen(
            master.listen_addr,
            [
                "--type", "shell",
                "--matrix", "tag=lane-a,lane-b",
                "--max-retries", "2",
                "--json",
                "--command", script,
            ],
        )
        _wait_until(cp_flag.is_file, timeout=15.0, desc="matrix cell checkpoint written")
        time.sleep(3.5)
        w1.proc.stop(sig=signal.SIGKILL, timeout=5.0)
        # Peer worker with matching lane-a tag joins to pick up checkpointed cell.
        self.ox.start_worker(master.listen_addr, name="w-mx-1b", tags=["lane-a"])

        rc = sub_proc.wait(timeout=30.0)
        self.assertEqual(rc, 0, sub_proc.logs())
        cells = _extract_matrix_cells(json.loads("\n".join(sub_proc.stdout_lines)))
        self.assertEqual(len(cells), 2)
        for c in cells:
            self.assertEqual(c.get("exit_code"), 0, c)

        lane_a = next(c for c in cells if "lane-a" in _cell_label_text(c))
        lane_b = next(c for c in cells if "lane-b" in _cell_label_text(c))
        self.assertIn("OK_RESUME", _cell_stdout(lane_a), lane_a)
        self.assertIn("OK_FAST", _cell_stdout(lane_b), lane_b)

        trace_text = trace_a.read_text(encoding="utf-8")
        trace_lines = trace_text.splitlines()
        self.assertIn("CELL_RESUMED", trace_text, trace_lines)
        self.assertEqual(trace_lines.count("CELL_STEP1"), 2, trace_lines)
        self.assertEqual(trace_lines.count("CELL_RESUMED"), 1, trace_lines)

    def test_t3_03_feat5_checkpoint_inside_test61_fs_isolation_sandbox(self) -> None:
        self._require_darwin_for_seatbelt()
        secret_file = self.env.home / "t3_seatbelt_secret.txt"
        secret_val = "seatbelt-cannot-read-this"
        secret_file.write_text(secret_val, encoding="utf-8")

        master = self.ox.start_master(
            session_resume_hold_secs=0,
            heartbeat_interval_secs=1,
            heartbeat_timeout_secs=3,
            reaper_interval_secs=1,
        )
        w1 = self.ox.start_worker(master.listen_addr, name="w-sb-cp1", isolation="fs")

        script = (
            'set -e\n'
            'CP="$OXIDESWARM_CHECKPOINT_DIR/sb.state"\n'
            'cat "$T_SECRET" 2>/dev/null || true\n'
            'if [ -f "$CP" ]; then cat "$CP"; echo SB_RESUMED; exit 0; fi\n'
            'echo "CP_INSIDE_SEATBELT" > "$CP"\n'
            'sleep 30\n'
        )
        sub = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "shell",
                "--require-isolation", "fs",
                "--max-retries", "2",
                "--env", f"T_SECRET={secret_file}",
                "--json",
                "--command", script,
            ],
        )
        self.assertEqual(sub.returncode, 0, sub.stderr)
        task_id = json.loads(sub.stdout)["task_id"]

        def _is_running() -> bool:
            st = self.ox.status_cli(master.listen_addr, task_id=task_id, json_out=True)
            if st.returncode != 0:
                return False
            info = json.loads(st.stdout)
            return "running" in str(info.get("status", "")).lower() or "running" in str(info.get("state", "")).lower()

        _wait_until(_is_running, timeout=15.0, desc="fs-isolated task running")
        # Allow task to write checkpoint and periodic checkpoint interval to flush.
        time.sleep(3.5)
        w1.proc.stop(sig=signal.SIGKILL, timeout=5.0)
        self.ox.start_worker(master.listen_addr, name="w-sb-cp2", isolation="fs")

        res = self.ox.result_cli(master.listen_addr, task_id, wait=True, json_out=True, timeout=30.0)
        self.assertEqual(res.returncode, 0, res.stderr)
        data = json.loads(res.stdout)
        self.assertEqual(data["exit_code"], 0)
        self.assertIn("CP_INSIDE_SEATBELT", data["stdout"])
        self.assertIn("SB_RESUMED", data["stdout"])
        self.assertNotIn(secret_val, data["stdout"])

    def test_t3_04_feat5_checkpoint_across_test41_master_sigterm_restart(self) -> None:
        state_dir = self.env.root / "t3_state_04"
        m1 = self.ox.start_master(
            name="m1",
            state_dir=state_dir,
            heartbeat_interval_secs=1,
            session_resume_hold_secs=1,
            reaper_interval_secs=1,
        )
        w1 = self.ox.start_worker(m1.listen_addr, name="w-t3-cp-m1", isolation="off")
        cp_ready = self.env.root / "t3_cp_m_ready"
        trace = self.env.root / "t3_cp_m_trace.log"

        script = (
            'set -e\n'
            'CP="$OXIDESWARM_CHECKPOINT_DIR/m.state"\n'
            'if [ -f "$CP" ]; then echo "STAGE2_RESUMED" >> "' + str(trace) + '"; exit 0; fi\n'
            'echo "STAGE1" >> "' + str(trace) + '"\n'
            'echo "CP_SAVED" > "$CP"\n'
            f'touch "{cp_ready}"\n'
            'sleep 30\n'
        )
        sub = self.ox.submit_cli(
            m1.listen_addr,
            ["--type", "shell", "--max-retries", "2", "--json", "--command", script],
        )
        task_id = json.loads(sub.stdout)["task_id"]
        _wait_until(cp_ready.is_file, timeout=15.0, desc="checkpoint ready before master SIGTERM")
        time.sleep(3.5)

        w1.proc.stop(sig=signal.SIGKILL, timeout=5.0)
        self.assertEqual(m1.proc.stop(sig=signal.SIGTERM, timeout=10.0), 0)

        m2 = self.ox.start_master(
            name="m2",
            listen=m1.listen_addr,
            state_dir=state_dir,
            heartbeat_interval_secs=1,
            session_resume_hold_secs=1,
            reaper_interval_secs=1,
        )
        self.ox.start_worker(m2.listen_addr, name="w-t3-cp-m2", isolation="off")

        res = self.ox.result_cli(m2.listen_addr, task_id, wait=True, json_out=True, timeout=30.0)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertEqual(json.loads(res.stdout)["exit_code"], 0)
        lines = trace.read_text(encoding="utf-8").splitlines()
        self.assertEqual(lines.count("STAGE1"), 1, lines)
        self.assertEqual(lines.count("STAGE2_RESUMED"), 1, lines)

    def test_t3_05_ax1_mcp_matrix_with_workspace_sync_and_outputs(self) -> None:
        master = self.ox.start_master()
        for i, tag in enumerate(("mo-1", "mo-2")):
            self.ox.start_worker(
                master.listen_addr, name=f"w-mo-{i}", tags=[tag], workspaces=True
            )

        ws_dir = self.env.work / "mcp_ws_out"
        ws_dir.mkdir()
        (ws_dir / "seed.txt").write_text("seed-data", encoding="utf-8")
        out_dir = ws_dir / "collected"
        out_dir.mkdir()

        mcp = McpClient(self.env, master.listen_addr, cwd=ws_dir)
        mcp.initialize()
        tools = mcp.tools_list()
        matrix_arg = _build_mcp_matrix_arg(tools, {"tag": ["mo-1", "mo-2"]})

        resp = mcp.call_tool(
            "submit",
            {
                "type": "command",
                "command": "sh",
                "args": ["-c", "cat seed.txt > out_report.txt && echo OUT_OK"],
                "workspace": {"name": "mcp-out-ws", "sync_dir": "."},
                "outputs": ["out_report.txt"],
                "output_dir": "collected",
                "matrix": matrix_arg,
                "wait": True,
            },
        )
        is_err, text = _mcp_tool_text(resp)
        self.assertFalse(is_err, text)
        cells = _extract_matrix_cells(json.loads(text))
        self.assertEqual(len(cells), 2)
        for c in cells:
            self.assertEqual(c.get("exit_code"), 0)
        saved_files = list(out_dir.rglob("out_report.txt"))
        self.assertEqual(len(saved_files), 2, saved_files)
        for sf in saved_files:
            self.assertEqual(sf.read_text(encoding="utf-8"), "seed-data")
        mcp.close_stdin()

    def test_t3_06_mesh2_and_oxideswarm_cohosted_ephemeral_ports_and_auth(self) -> None:
        sf = self.ox.generate_secret_file()
        self.ox.secret_file = sf
        master = self.ox.start_master(secret_file=sf)
        self.ox.start_worker(master.listen_addr, name="w-cohost", secret_file=sf)

        tok = "cohost-mesh-secret-token"
        hub = self.mesh.start_hub(token=tok)
        inbox = self.env.root / "cohost_inbox"
        inbox.mkdir()
        self.mesh.start_node(hub, "cohost-node", inbox_dir=inbox, token=tok)

        ox_res = self.ox.submit_cli(
            master.listen_addr,
            ["--type", "command", "--wait", "--json", "--command", "echo", "--", "GRID_COHOST_OK"],
            secret_file=sf,
        )
        mesh_res = self.mesh.run_data(hub, to_node="cohost-node", payload="MESH_COHOST_OK", token=tok)

        self.assertEqual(ox_res.returncode, 0, ox_res.stderr)
        self.assertEqual(mesh_res.returncode, 0, mesh_res.stderr)
        self.assertIn("GRID_COHOST_OK", json.loads(ox_res.stdout)["stdout"])
        _wait_inbox_for_bytes(inbox, b"MESH_COHOST_OK", timeout=5.0)

    def test_t3_07_test41_master_sigterm_during_ax1_matrix_execution(self) -> None:
        state_dir = self.env.root / "t3_state_07"
        marker = self.env.root / "t3_mx_marker.log"
        gate = self.env.root / "t3_mx_gate"
        done = self.env.root / "t3_mx_done"

        m1 = self.ox.start_master(
            name="m1", state_dir=state_dir, heartbeat_interval_secs=1, session_resume_hold_secs=30
        )
        for tag in ("mx-1", "mx-2", "mx-3"):
            self.ox.start_worker(m1.listen_addr, name=f"w-{tag}", tags=[tag], isolation="off")

        script = (
            f'printf "cell_run\\n" >> "{marker}"\n'
            f'while [ ! -f "{gate}" ]; do sleep 0.01; done\n'
            f'printf "d\\n" >> "{done}"\n'
            'echo "MX_GATED_OK"\n'
        )
        sub_proc = self.ox.submit_cli_popen(
            m1.listen_addr,
            [
                "--type", "shell",
                "--matrix", "tag=mx-1,mx-2,mx-3",
                "--json",
                "--command", script,
            ],
        )

        _wait_until(
            lambda: marker.is_file() and len(marker.read_text().splitlines()) == 3,
            timeout=15.0,
            desc="all 3 matrix cells running",
        )
        gate.write_text("go\n", encoding="utf-8")
        _wait_until(
            lambda: done.is_file() and len(done.read_text().splitlines()) == 3,
            timeout=10.0,
            interval=0.005,
            desc="all 3 matrix cells finished",
        )
        self.assertEqual(m1.proc.stop(sig=signal.SIGTERM, timeout=10.0), 0)
        m2 = self.ox.start_master(
            name="m2",
            listen=m1.listen_addr,
            state_dir=state_dir,
            heartbeat_interval_secs=1,
            session_resume_hold_secs=30,
        )

        # Wait for all 3 workers to reconnect to restarted master m2.
        def _all_reconnected() -> bool:
            try:
                ws = self.ox.workers_json(m2.listen_addr)
                connected = [w for w in ws if "state:connected" in w.get("tags", [])]
                return len(connected) == 3
            except Exception:
                return False

        _wait_until(_all_reconnected, timeout=15.0, desc="3 workers reconnected to m2")

        # Wait for m2 status --json to reflect all 3 completed tasks with 0 failures.
        def _m2_completed() -> Optional[dict[str, Any]]:
            st = self.ox.status_cli(m2.listen_addr, json_out=True)
            if st.returncode != 0:
                return None
            info = json.loads(st.stdout)
            if info.get("completed_tasks") == 3:
                return info
            return None

        m2_status = _wait_until(_m2_completed, timeout=15.0, desc="m2 completed_tasks == 3")
        self.assertEqual(m2_status.get("completed_tasks"), 3, m2_status)
        self.assertEqual(m2_status.get("failed_tasks"), 0, m2_status)
        self.assertEqual(m2_status.get("total_tasks"), 3, m2_status)

        rc = sub_proc.wait(timeout=30.0)
        cells = _extract_matrix_cells(json.loads("\n".join(sub_proc.stdout_lines)))
        self.assertEqual(len(cells), 3, cells)
        if rc == 0:
            for c in cells:
                self.assertEqual(c.get("exit_code"), 0, c)
        else:
            # submit --matrix waits on m1 without auto-reconnect; if m1 exited before
            # forwarding TaskCompleted, cells must only show connection drop, never task failure.
            for c in cells:
                if c.get("exit_code") is not None:
                    self.assertEqual(c.get("exit_code"), 0, c)
                else:
                    err = str(c.get("error", "")).lower()
                    self.assertTrue(
                        "closed the connection" in err or "communication error" in err,
                        f"Unexpected non-disconnect cell failure: {c!r}",
                    )

        # Unconditionally verify all 3 matrix cell task_ids on m2 via result_cli.
        for c in cells:
            tid = c.get("task_id")
            self.assertTrue(tid, f"Missing task_id on matrix cell: {c!r}")
            res = self.ox.result_cli(m2.listen_addr, str(tid), wait=True, json_out=True)
            self.assertEqual(res.returncode, 0, f"m2 result failed for {tid}: {res.stderr}")
            rdata = json.loads(res.stdout)
            self.assertEqual(rdata.get("exit_code"), 0, rdata)
            self.assertIn("MX_GATED_OK", rdata.get("stdout", ""), rdata)

        time.sleep(0.35)
        marker_lines = marker.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(marker_lines), 3, f"Double execution detected: {marker_lines!r}")


# ===========================================================================
# TIER 4: Real-World Application Workloads (6 scenarios)
# ===========================================================================


class Tier4WorkloadScenarios(Wave2E2ETestBase):
    """Tier 4: Realistic multi-feature end-to-end user workflows."""

    def test_t4_01_multitarget_build_and_verify_fanout_pipeline(self) -> None:
        master = self.ox.start_master()
        for t in ("target-a", "target-b", "target-c"):
            self.ox.start_worker(master.listen_addr, name=f"w-{t}", tags=[t], workspaces=True)

        repo_src = self.env.work / "app_repo"
        repo_src.mkdir()
        (repo_src / "build.py").write_text(
            "import pathlib, sys\n"
            "target = sys.argv[1]\n"
            "pathlib.Path('dist').mkdir(exist_ok=True)\n"
            "pathlib.Path('dist/artifact.txt').write_text(f'built:{target}\\n')\n",
            encoding="utf-8",
        )

        # Stage 1: Matrix build using shared --workspace --sync + --output collection.
        stage1_out = self.env.root / "stage1_out"
        res1 = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "command",
                "--matrix", "tag=target-a,target-b,target-c",
                "--workspace", "wave2-app-build",
                "--sync", str(repo_src),
                "--output", "dist/artifact.txt",
                "--output-dir", str(stage1_out),
                "--json",
                "--command", "python3", "--", "build.py", "release",
            ],
        )
        self.assertEqual(res1.returncode, 0, res1.stderr)
        cells1 = _extract_matrix_cells(json.loads(res1.stdout))
        self.assertEqual(len(cells1), 3)
        self.assertEqual(len(list(stage1_out.rglob("artifact.txt"))), 3)

        # Stage 2: Matrix verification using shared --input blobs across all 3 workers.
        res2 = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "shell",
                "--matrix", "tag=target-a,target-b,target-c",
                "--input", f"{stage1_out}:artifacts",
                "--json",
                "--command",
                'count=$(grep -rl "built:release" artifacts | wc -l | tr -d " "); '
                'test "$count" = "3" && echo VERIFIED_OK',
            ],
        )
        self.assertEqual(res2.returncode, 0, res2.stderr)
        cells2 = _extract_matrix_cells(json.loads(res2.stdout))
        self.assertEqual(len(cells2), 3)
        for c in cells2:
            self.assertIn("VERIFIED_OK", _cell_stdout(c))

    def test_t4_02_fault_tolerant_multistage_compute_checkpoint_no_leakage(self) -> None:
        master = self.ox.start_master(
            session_resume_hold_secs=0,
            heartbeat_interval_secs=1,
            heartbeat_timeout_secs=3,
            reaper_interval_secs=1,
        )
        w1 = self.ox.start_worker(master.listen_addr, name="w-comp-1", isolation="off")
        stage2_flag = self.env.root / "t4_stage2_done"
        trace = self.env.root / "t4_compute_trace.log"
        out_dir = self.env.root / "t4_final_out"

        script = (
            'set -e\n'
            'CP="$OXIDESWARM_CHECKPOINT_DIR"\n'
            'if [ ! -f "$CP/stage1.done" ]; then\n'
            f'  echo "STAGE1" >> "{trace}"\n'
            '  echo "s1=100" > "$CP/stage1.done"\n'
            'fi\n'
            'if [ ! -f "$CP/stage2.done" ]; then\n'
            f'  echo "STAGE2" >> "{trace}"\n'
            '  echo "s2=200" > "$CP/stage2.done"\n'
            f'  touch "{stage2_flag}"\n'
            '  sleep 30\n'
            'fi\n'
            f'echo "STAGE3" >> "{trace}"\n'
            'cat "$CP/stage1.done" "$CP/stage2.done" > final_report.json\n'
        )
        sub_proc = self.ox.submit_cli_popen(
            master.listen_addr,
            [
                "--type", "shell",
                "--max-retries", "2",
                "--output", "**/*",
                "--output-dir", str(out_dir),
                "--json",
                "--command", script,
            ],
        )
        _wait_until(stage2_flag.is_file, timeout=15.0, desc="stage2 checkpoint written")
        time.sleep(3.5)
        w1.proc.stop(sig=signal.SIGKILL, timeout=5.0)
        self.ox.start_worker(master.listen_addr, name="w-comp-2", isolation="off")

        rc = sub_proc.wait(timeout=30.0)
        self.assertEqual(rc, 0, sub_proc.logs())
        lines = trace.read_text(encoding="utf-8").splitlines()
        self.assertEqual(lines, ["STAGE1", "STAGE2", "STAGE3"])
        names = {p.name for p in out_dir.rglob("*") if p.is_file()}
        self.assertIn("final_report.json", names)
        self.assertNotIn("stage1.done", names)
        self.assertNotIn("stage2.done", names)

    def test_t4_03_zero_duplication_rolling_master_restart_under_live_load(self) -> None:
        state_dir = self.env.root / "t4_state_03"
        marker = self.env.root / "t4_rolling_marker.log"
        gate = self.env.root / "t4_rolling_gate"
        done = self.env.root / "t4_rolling_done"

        m1 = self.ox.start_master(
            name="m1", state_dir=state_dir, heartbeat_interval_secs=1, session_resume_hold_secs=30
        )
        self.ox.start_worker(m1.listen_addr, name="w-roll-1", isolation="off", cores=4)
        self.ox.start_worker(m1.listen_addr, name="w-roll-2", isolation="off", cores=4)

        task_ids: list[str] = []
        for idx in range(4):
            sub = self.ox.submit_cli(
                m1.listen_addr,
                [
                    "--type", "shell",
                    "--json",
                    "--command",
                    f'printf "task_{idx}\\n" >> "{marker}"\n'
                    f'while [ ! -f "{gate}" ]; do sleep 0.01; done\n'
                    f'printf "d\\n" >> "{done}"\n'
                    f'echo "ROLL_{idx}_OK"\n',
                ],
            )
            self.assertEqual(sub.returncode, 0, sub.stderr)
            task_ids.append(json.loads(sub.stdout)["task_id"])

        _wait_until(
            lambda: marker.is_file() and len(marker.read_text().splitlines()) == 4,
            timeout=15.0,
            desc="4 live tasks running",
        )
        gate.write_text("go\n", encoding="utf-8")
        _wait_until(
            lambda: done.is_file() and len(done.read_text().splitlines()) == 4,
            timeout=10.0,
            interval=0.005,
            desc="4 live tasks done",
        )
        self.assertEqual(m1.proc.stop(sig=signal.SIGTERM, timeout=10.0), 0)

        m2 = self.ox.start_master(
            name="m2",
            listen=m1.listen_addr,
            state_dir=state_dir,
            heartbeat_interval_secs=1,
            session_resume_hold_secs=30,
        )
        for idx, tid in enumerate(task_ids):
            r = self.ox.result_cli(m2.listen_addr, tid, wait=True, json_out=True)
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertIn(f"ROLL_{idx}_OK", json.loads(r.stdout)["stdout"])

        time.sleep(0.4)
        executed = sorted(marker.read_text(encoding="utf-8").splitlines())
        self.assertEqual(executed, ["task_0", "task_1", "task_2", "task_3"])

    def test_t4_04_end_to_end_ai_agent_mcp_session_workflow(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-ai-1", tags=["ai-1"])
        self.ox.start_worker(master.listen_addr, name="w-ai-2", tags=["ai-2"])
        self.ox.start_worker(master.listen_addr, name="w-ai-3", tags=["ai-3"])

        mcp = McpClient(self.env, master.listen_addr)
        init = mcp.initialize()
        self.assertEqual(init["result"]["protocolVersion"], "2024-11-05")

        w_resp = mcp.call_tool("workers", {})
        is_err, w_txt = _mcp_tool_text(w_resp)
        self.assertFalse(is_err)
        self.assertIn("w-ai-1", w_txt)

        tools = mcp.tools_list()
        matrix_arg = _build_mcp_matrix_arg(tools, {"tag": ["ai-1", "ai-2", "ai-3"]})
        sub_resp = mcp.call_tool(
            "submit",
            {
                "type": "shell",
                "script": "echo AI_AGENT_STEP_DONE",
                "matrix": matrix_arg,
                "wait": True,
            },
        )
        is_err, sub_txt = _mcp_tool_text(sub_resp)
        self.assertFalse(is_err, sub_txt)
        cells = _extract_matrix_cells(json.loads(sub_txt))
        self.assertEqual(len(cells), 3)

        st_resp = mcp.call_tool("status", {"workers": True})
        st_err, _ = _mcp_tool_text(st_resp)
        self.assertFalse(st_err)
        self.assertEqual(mcp.ping()["result"], {})

        mcp.close_stdin()
        self.assertEqual(mcp.mp.wait(timeout=10.0), 0)

    def test_t4_05_resilient_mesh_multinode_data_distribution_pipeline(self) -> None:
        hub = self.mesh.start_hub(token="t4-mesh-pipeline")
        inbox_a = self.env.root / "t4_inbox_a"
        inbox_b = self.env.root / "t4_inbox_b"
        inbox_a.mkdir()
        inbox_b.mkdir()
        self.mesh.start_node(hub, "consumer-a", inbox_dir=inbox_a)
        self.mesh.start_node(hub, "consumer-b", inbox_dir=inbox_b)

        # Missing target fails cleanly without disrupting valid consumer deliveries.
        miss = self.mesh.run_data(hub, to_node="ghost-consumer", payload="drop")
        self.assertNotEqual(miss.returncode, 0)

        msg_a = '{"job":"alpha","seq":1}'
        msg_b = '{"job":"beta","seq":2}'
        self.assertEqual(self.mesh.run_data(hub, to_node="consumer-a", payload=msg_a).returncode, 0)
        self.assertEqual(self.mesh.run_data(hub, to_node="consumer-b", payload=msg_b).returncode, 0)

        ws = StdlibWsClient(hub.ws_url)
        self.env.add_cleanup(ws.close)
        ws.register("pipeline-sender", token="t4-mesh-pipeline")
        bin_b = b"\x00BIN\xff\x80" + os.urandom(2048)
        corr = ws.send_data_payload("consumer-b", bin_b)
        ack = ws.recv_until(lambda m: m.get("correlation_id") == corr, timeout=8.0)
        self.assertEqual(ack["type"], "DeliveryAck", ack)

        _wait_inbox_for_bytes(inbox_a, msg_a.encode("utf-8"), timeout=5.0)
        _wait_inbox_for_bytes(inbox_b, msg_b.encode("utf-8"), timeout=5.0)
        _wait_inbox_for_bytes(inbox_b, bin_b, timeout=5.0)

        # Strict isolation between consumer-a and consumer-b.
        a_blobs = {data for _, data in _collect_inbox_payloads(inbox_a)}
        b_blobs = {data for _, data in _collect_inbox_payloads(inbox_b)}
        self.assertNotIn(msg_b.encode("utf-8"), a_blobs)
        self.assertNotIn(bin_b, a_blobs)
        self.assertNotIn(msg_a.encode("utf-8"), b_blobs)

    def test_t4_06_hybrid_oxideswarm_compute_to_agent_mesh_delivery(self) -> None:
        master = self.ox.start_master()
        iso = "fs" if sys.platform == "darwin" else "off"
        self.ox.start_worker(master.listen_addr, name="w-hyb-1", isolation=iso, tags=["shard-1"])
        self.ox.start_worker(master.listen_addr, name="w-hyb-2", isolation=iso, tags=["shard-2"])

        hub = self.mesh.start_hub()
        archive_inbox = self.env.root / "hybrid_archive_inbox"
        archive_inbox.mkdir()
        self.mesh.start_node(hub, "archive-node", inbox_dir=archive_inbox)

        out_dir = self.env.root / "hybrid_outputs"
        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "shell",
                "--matrix", "tag=shard-1,shard-2",
                "--output", "summary.txt",
                "--output-dir", str(out_dir),
                "--json",
                "--command", "uname -s > summary.txt && echo HYBRID_COMPUTED",
            ],
        )
        self.assertEqual(res.returncode, 0, res.stderr)
        cells = _extract_matrix_cells(json.loads(res.stdout))
        self.assertEqual(len(cells), 2)

        report = json.dumps(
            {
                "cells": len(cells),
                "outputs": sorted(p.name for p in out_dir.rglob("*") if p.is_file()),
                "digests": [
                    hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in sorted(out_dir.rglob("*.txt"))
                ],
            },
            sort_keys=True,
        )
        deliv = self.mesh.run_data(hub, to_node="archive-node", payload=report)
        self.assertEqual(deliv.returncode, 0, deliv.stderr)
        saved = _wait_inbox_for_bytes(archive_inbox, report.encode("utf-8"), timeout=5.0)
        self.assertEqual(
            hashlib.sha256(saved.read_bytes()).hexdigest(),
            hashlib.sha256(report.encode("utf-8")).hexdigest(),
        )


if __name__ == "__main__":
    unittest.main()
