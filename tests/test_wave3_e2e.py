#!/usr/bin/env python3
"""
OxideSwarm Wave 3 Opaque-Box End-to-End Test Suite (`prod/backlog-completion-wave3`)

Covers 4 Tiers across Wave 3 backlog items:
  - AX-2:   Sharded runs (`oxideswarm shard`) with Rust `--build-once` & `--junit-out`
  - MESH-3: Remote master bridge (`agent-mesh hub --grid-connect`), placement, inline files, outputs, result, cancel
  - 3D-2:   Master VRAM & Blender capability routing (`requires_blender`, preset `required_vram_mb`)
  - 3D-3:   Honest AutoRig options (`rig_preset`, `embed_animations`, `preferred_provider`, magic-byte format, <=1s timeout/cancel)

Strictly opaque-box: zero internal Rust crate imports, pure Python stdlib `unittest` only.
Note: 3D-2 and 3D-3 tests require `oxideswarm` built with `--features 3d`.

Usage:
  python3 -m unittest tests/test_wave3_e2e.py -v
  python3 tests/test_wave3_e2e.py -v
  OXIDESWARM_E2E_TIER=1 python3 -m unittest tests/test_wave3_e2e.py -v
  OXIDESWARM_E2E_TIER=1,2 OXIDESWARM_PROFILE=debug python3 tests/test_wave3_e2e.py -v
"""

from __future__ import annotations

import base64
import collections
import concurrent.futures
import hashlib
import json
import os
import platform
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
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent

SCRUB_ENV_PREFIXES = ("RUSTY_GRID_", "OXIDESWARM_", "OXIDE_", "OX_SHARD_")
SCRUB_ENV_EXACT = (
    "OXIDE_MESH_TOKEN",
    "OXIDE_MESH_TLS_CA",
    "OXIDE_GRID_MASTER",
    "OXIDE_GRID_CONNECT",
    "CARGO_MANIFEST_DIR",
    "GITHUB_STEP_SUMMARY",
    "BLENDER_PATH",
)

EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()

BIPED_BONE_MARKERS = {
    "Hips",
    "Spine",
    "Chest",
    "Neck",
    "Head",
    "UpperLeg_L",
    "LowerLeg_L",
    "Foot_L",
    "UpperLeg_R",
    "LowerLeg_R",
    "Foot_R",
    "UpperArm_L",
    "LowerArm_L",
    "UpperArm_R",
    "LowerArm_R",
}


# ---------------------------------------------------------------------------
# Binary Discovery & Tier Selection
# ---------------------------------------------------------------------------


def find_binary(name: str) -> Path:
    """Locate compiled binary without invoking Cargo at test import time."""
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

    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def _parse_selected_tiers() -> set[int]:
    raw_env = os.environ.get("OXIDESWARM_E2E_TIER")
    if raw_env is None:
        return {1, 2, 3, 4}
    raw = raw_env.strip()
    if not raw or raw.lower() == "all":
        return {1, 2, 3, 4}
    tiers: set[int] = set()
    for part in raw.split(","):
        token = part.strip()
        if not token:
            raise ValueError(f"Empty tier token in OXIDESWARM_E2E_TIER={raw_env!r}")
        if token.lower().startswith("tier"):
            token = token[4:]
        val = int(token)
        if val not in (1, 2, 3, 4):
            raise ValueError(f"Unsupported OXIDESWARM_E2E_TIER value: {part!r}")
        tiers.add(val)
    if not tiers:
        raise ValueError(f"No valid tiers parsed from OXIDESWARM_E2E_TIER={raw_env!r}")
    return tiers


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


def _current_os_tag() -> str:
    if sys.platform == "darwin":
        return "macos"
    if sys.platform.startswith("linux"):
        return "linux"
    if os.name == "nt":
        return "windows"
    return sys.platform


def _current_arch_tag() -> str:
    m = platform.machine().lower()
    if m in ("arm64", "aarch64"):
        return "aarch64"
    if m in ("x86_64", "amd64"):
        return "x86_64"
    return m


# ---------------------------------------------------------------------------
# Process, Isolation & Network Helpers
# ---------------------------------------------------------------------------


@dataclass
class CliResult:
    returncode: int
    stdout: str
    stderr: str
    elapsed_s: float


def _is_pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _kill_process_group(pgid: int, sig: int = signal.SIGKILL) -> None:
    if os.name != "posix" or pgid <= 0:
        return
    try:
        os.killpg(pgid, sig)
    except (ProcessLookupError, PermissionError, OSError):
        pass
    try:
        os.kill(pgid, sig)
    except (ProcessLookupError, PermissionError, OSError):
        pass


class ManagedProcess:
    """Tracks an exact Popen handle + process group with bounded daemon stream drainers."""

    def __init__(
        self,
        name: str,
        proc: subprocess.Popen[str],
        stdout_lines: Optional[collections.deque[str]] = None,
        stderr_lines: Optional[collections.deque[str]] = None,
        pgid: Optional[int] = None,
    ) -> None:
        self.name = name
        self.proc = proc
        self.pgid = pgid if pgid is not None else proc.pid
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
        rc = self.proc.wait(timeout=timeout)
        for t in self._threads:
            t.join(timeout=2.0)
        return rc

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
                _kill_process_group(self.pgid, signal.SIGKILL)
                try:
                    self.proc.wait(timeout=3.0)
                except subprocess.TimeoutExpired:
                    pass
        # Always reap any stray grandchildren left in the session process group.
        _kill_process_group(self.pgid, signal.SIGKILL)
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

    def __init__(self, prefix: str = "oxs_w3_") -> None:
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
        pmset.write_text(
            "#!/bin/sh\necho \"Now drawing from 'AC Power'\"\nexit 0\n",
            encoding="utf-8",
        )
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
            for k, v in extra.items():
                if k == "__PATH_OVERRIDE__":
                    env["PATH"] = v
                elif k == "PATH":
                    env["PATH"] = f"{v}{os.pathsep}{env['PATH']}"
                else:
                    env[k] = v
        return env

    def path_without_tool(self, tool_name: str) -> str:
        """Return an isolated PATH (keeping self.fake_bin) with any dir containing `tool_name` removed."""
        exe_names = [f"{tool_name}.exe", tool_name] if os.name == "nt" else [tool_name]
        parts = [str(self.fake_bin)]
        for entry in os.environ.get("PATH", "").split(os.pathsep):
            if not entry:
                continue
            d = Path(entry)
            if any((d / exe).exists() for exe in exe_names):
                continue
            parts.append(entry)
        return os.pathsep.join(parts)

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
        popen_kwargs: dict[str, Any] = {
            "cwd": str(cwd or self.work),
            "env": env,
            "stdin": subprocess.PIPE if (stdin_bytes is not None or stdin_text is not None) else subprocess.DEVNULL,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
        }
        if os.name == "posix":
            popen_kwargs["start_new_session"] = True
        proc = subprocess.Popen(argv, **popen_kwargs)
        pgid = proc.pid
        if os.name == "posix":
            try:
                pgid = os.getpgid(proc.pid)
            except OSError:
                pgid = proc.pid
        inp_bytes: Optional[bytes]
        if stdin_bytes is not None:
            inp_bytes = stdin_bytes
        elif stdin_text is not None:
            inp_bytes = stdin_text.encode("utf-8")
        else:
            inp_bytes = None
        try:
            out_b, err_b = proc.communicate(input=inp_bytes, timeout=timeout)
        except subprocess.TimeoutExpired:
            _kill_process_group(pgid, signal.SIGKILL)
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            try:
                proc.communicate(timeout=2.0)
            except Exception:
                pass
            raise
        return CliResult(
            returncode=proc.returncode,
            stdout=out_b.decode("utf-8", errors="replace"),
            stderr=err_b.decode("utf-8", errors="replace"),
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
        popen_kwargs: dict[str, Any] = {
            "cwd": str(cwd or self.work),
            "env": env,
            "stdin": subprocess.PIPE if stdin_pipe else subprocess.DEVNULL,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
            "text": True,
            "bufsize": 1,
        }
        if os.name == "posix":
            popen_kwargs["start_new_session"] = True
        proc = subprocess.Popen(argv, **popen_kwargs)
        pgid = proc.pid
        if os.name == "posix":
            try:
                pgid = os.getpgid(proc.pid)
            except OSError:
                pgid = proc.pid
        mp = ManagedProcess(name=name, proc=proc, pgid=pgid)
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


def reserve_unused_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


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
        platform_name: str = "macos",
        token: Optional[str] = None,
    ) -> dict[str, Any]:
        self.node_id = node_id
        corr = str(uuid.uuid4())
        reg: dict[str, Any] = {
            "type": "NodeRegistration",
            "correlation_id": corr,
            "from": node_id,
            "to": "hub",
            "platform": platform_name,
            "hostname": "wave3-e2e",
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

    def send_command(
        self,
        to_node: str,
        command: str,
        args: Optional[dict[str, Any]] = None,
        correlation_id: Optional[str] = None,
        timeout_ms: int = 20000,
        from_node: Optional[str] = None,
    ) -> str:
        corr = correlation_id or str(uuid.uuid4())
        sender = from_node or self.node_id or "ws-caller"
        env = {
            "version": "1.0",
            "type": "CommandRequest",
            "correlation_id": corr,
            "from": sender,
            "to": to_node,
            "command": command,
            "args": args or {},
            "timeout_ms": timeout_ms,
            "require_ack": False,
            "timestamp": int(time.time() * 1000),
        }
        self.send_json(env)
        return corr

    def grid_call(
        self,
        command: str,
        args: Optional[dict[str, Any]] = None,
        timeout: float = 25.0,
        from_node: Optional[str] = None,
    ) -> dict[str, Any]:
        corr = self.send_command(
            to_node="grid",
            command=command,
            args=args or {},
            timeout_ms=int(timeout * 1000),
            from_node=from_node,
        )
        return self.recv_until(
            lambda m: m.get("correlation_id") == corr
            and m.get("type") in (
                "CommandResponse",
                "command_response",
                "DeliveryNack",
                "delivery_nack",
            ),
            timeout=timeout,
        )

    def grid_status(
        self, task_id: Optional[str] = None, timeout: float = 15.0
    ) -> dict[str, Any]:
        args: dict[str, Any] = {}
        if task_id is not None:
            args["task_id"] = task_id
        return self.grid_call("grid_status", args, timeout=timeout)

    def grid_submit(
        self, args: dict[str, Any], timeout: float = 30.0
    ) -> dict[str, Any]:
        return self.grid_call("grid_submit", args, timeout=timeout)

    def grid_result(
        self,
        task_id: str,
        wait: bool = True,
        timeout_secs: Optional[int] = None,
        timeout: float = 30.0,
    ) -> dict[str, Any]:
        args: dict[str, Any] = {"task_id": task_id, "wait": wait}
        if timeout_secs is not None:
            args["timeout_secs"] = timeout_secs
        return self.grid_call("grid_result", args, timeout=timeout)

    def grid_cancel(
        self, task_id: str, timeout: float = 15.0
    ) -> dict[str, Any]:
        return self.grid_call("grid_cancel", {"task_id": task_id}, timeout=timeout)

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
# Deterministic Runtime Shims (`cargo-nextest` & `blender`)
# ---------------------------------------------------------------------------


def install_fake_cargo_nextest(
    bin_dir: Path,
    log_path: Optional[Path] = None,
    archive_exit_code: int = 0,
    fail_shards: Iterable[int] = (),
    sleep_secs: float = 0.0,
    sleep_shards: Optional[dict[int, float]] = None,
    pid_dir: Optional[Path] = None,
    junit_rel_path: str = "target/nextest/default/junit.xml",
    malformed_junit_shards: Iterable[int] = (),
    omit_junit_shards: Iterable[int] = (),
) -> Path:
    """Create deterministic fake `cargo-nextest` and `cargo` binaries in `bin_dir`."""
    bin_dir.mkdir(parents=True, exist_ok=True)
    fail_set = {int(s) for s in fail_shards}
    bad_xml_set = {int(s) for s in malformed_junit_shards}
    omit_xml_set = {int(s) for s in omit_junit_shards}
    sleep_map = {str(int(k)): float(v) for k, v in (sleep_shards or {}).items()}

    script_path = bin_dir / "cargo-nextest"
    code = f'''#!/usr/bin/env python3
import json
import os
import re
import sys
import tarfile
import io
import time
from pathlib import Path

LOG_PATH = {repr(str(log_path)) if log_path else "None"}
DEFAULT_ARCHIVE_EXIT = {int(archive_exit_code)}
FAIL_SHARDS = {repr(sorted(fail_set))}
BAD_XML_SHARDS = {repr(sorted(bad_xml_set))}
OMIT_XML_SHARDS = {repr(sorted(omit_xml_set))}
DEFAULT_SLEEP = {float(sleep_secs)}
SLEEP_SHARDS = {repr(sleep_map)}
PID_DIR = {repr(str(pid_dir)) if pid_dir else "None"}
JUNIT_REL = {repr(junit_rel_path)}


def log_event(entry):
    target = os.environ.get("FAKE_NEXTEST_LOG") or LOG_PATH
    if not target:
        return
    p = Path(target)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\\n")


def find_flag_val(args, names):
    for i, a in enumerate(args):
        for n in names:
            if a == n and i + 1 < len(args):
                return args[i + 1]
            if a.startswith(n + "="):
                return a.split("=", 1)[1]
    return None


def resolve_tool_config_junit(tool_cfg):
    if not tool_cfg:
        return None
    rel = tool_cfg.split(":", 1)[1] if ":" in tool_cfg else tool_cfg
    p = Path(rel)
    if not p.is_file():
        return None
    txt = p.read_text(encoding="utf-8", errors="replace")
    m = re.search(r'^path\\s*=\\s*"([^"]+)"', txt, flags=re.MULTILINE)
    return m.group(1) if m else None


def main():
    args = sys.argv[1:]
    if args and args[0] == "nextest":
        args = args[1:]

    if not args or args[0] in ("--version", "-V"):
        print("cargo-nextest 0.9.85 (fake-e2e)")
        return 0

    sub = args[0]
    worker_id = (
        os.environ.get("RUSTY_GRID_WORKER_ID")
        or os.environ.get("OXIDESWARM_WORKER_ID")
        or ""
    )

    if sub == "archive":
        archive_file = find_flag_val(args[1:], ("--archive-file",))
        log_event({{
            "cmd": "archive",
            "pid": os.getpid(),
            "cwd": os.getcwd(),
            "argv": args,
            "worker_id": worker_id,
            "archive_file": archive_file,
        }})
        rc = int(os.environ.get("FAKE_NEXTEST_ARCHIVE_EXIT", str(DEFAULT_ARCHIVE_EXIT)))
        if rc != 0:
            sys.stderr.write(f"fake cargo-nextest archive failed with rc={{rc}}\\n")
            return rc
        if not archive_file:
            sys.stderr.write("missing --archive-file\\n")
            return 2
        out_p = Path(archive_file)
        out_p.parent.mkdir(parents=True, exist_ok=True)
        meta = json.dumps({{"built_by": "fake-nextest", "cwd": os.getcwd()}}).encode("utf-8")
        with tarfile.open(out_p, "w:gz") as tf:
            ti = tarfile.TarInfo("nextest-archive.json")
            ti.size = len(meta)
            tf.addfile(ti, io.BytesIO(meta))
        print(f"Archived tests to {{archive_file}}")
        return 0

    if sub == "run":
        archive_file = find_flag_val(args[1:], ("--archive-file",))
        workspace_remap = find_flag_val(args[1:], ("--workspace-remap",))
        partition = find_flag_val(args[1:], ("--partition",))
        junit_flag = find_flag_val(args[1:], ("--junit", "--junit-path"))
        tool_cfg = find_flag_val(args[1:], ("--tool-config-file",))

        shard_idx = int(os.environ.get("OX_SHARD_INDEX", "1"))
        shard_tot = int(os.environ.get("OX_SHARD_TOTAL", "1"))
        if partition and partition.startswith("slice:"):
            parts = partition[len("slice:"):].split("/", 1)
            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                shard_idx = int(parts[0])
                shard_tot = int(parts[1])

        archive_valid = None
        if archive_file is not None:
            ap = Path(archive_file)
            if not ap.is_file() or ap.stat().st_size == 0:
                sys.stderr.write(f"archive file missing or empty: {{archive_file}}\\n")
                return 2
            try:
                with tarfile.open(ap, "r:*") as tf:
                    archive_valid = "nextest-archive.json" in tf.getnames()
            except Exception as exc:
                sys.stderr.write(f"corrupt archive file {{archive_file}}: {{exc}}\\n")
                return 2
            if not archive_valid:
                sys.stderr.write("archive missing nextest-archive.json\\n")
                return 2

        sleep_map = dict(SLEEP_SHARDS)
        env_sleep_shards = os.environ.get("FAKE_NEXTEST_SLEEP_SHARDS", "").strip()
        if env_sleep_shards:
            sleep_map.update(json.loads(env_sleep_shards))
        sleep_val = float(
            sleep_map.get(
                str(shard_idx),
                os.environ.get("FAKE_NEXTEST_SLEEP_SECS", str(DEFAULT_SLEEP)),
            )
        )
        if sleep_val > 0:
            pid_target = os.environ.get("FAKE_NEXTEST_PID_DIR") or PID_DIR
            if pid_target:
                pd = Path(pid_target)
                pd.mkdir(parents=True, exist_ok=True)
                (pd / f"shard_{{shard_idx}}.pid").write_text(str(os.getpid()), encoding="utf-8")
            time.sleep(sleep_val)

        log_event({{
            "cmd": "run",
            "pid": os.getpid(),
            "cwd": os.getcwd(),
            "argv": args,
            "worker_id": worker_id,
            "shard_index": shard_idx,
            "shard_total": shard_tot,
            "partition": partition,
            "archive_file": archive_file,
            "archive_valid": archive_valid,
            "workspace_remap": workspace_remap,
        }})

        is_fail = shard_idx in FAIL_SHARDS
        is_bad_xml = shard_idx in BAD_XML_SHARDS
        is_omit_xml = shard_idx in OMIT_XML_SHARDS

        if not is_omit_xml:
            if is_bad_xml:
                xml_text = "<testsuites><testsuite name=\\"broken\\"><unclosed"
            elif is_fail:
                xml_text = (
                    f"<testsuites tests=\\"2\\" failures=\\"1\\" errors=\\"0\\" skipped=\\"0\\" time=\\"0.02\\">"
                    f"<testsuite name=\\"shard_{{shard_idx}}\\" tests=\\"2\\" failures=\\"1\\" errors=\\"0\\" skipped=\\"0\\" time=\\"0.02\\">"
                    f"<testcase name=\\"test_s{{shard_idx}}_pass\\" classname=\\"e2e.shard{{shard_idx}}\\" time=\\"0.01\\"/>"
                    f"<testcase name=\\"test_s{{shard_idx}}_fail\\" classname=\\"e2e.shard{{shard_idx}}\\" time=\\"0.01\\">"
                    f"<failure message=\\"boom on shard {{shard_idx}}\\" type=\\"AssertionError\\">assertion failed</failure>"
                    f"</testcase>"
                    f"</testsuite></testsuites>\\n"
                )
            else:
                xml_text = (
                    f"<testsuites tests=\\"2\\" failures=\\"0\\" errors=\\"0\\" skipped=\\"0\\" time=\\"0.02\\">"
                    f"<testsuite name=\\"shard_{{shard_idx}}\\" tests=\\"2\\" failures=\\"0\\" errors=\\"0\\" skipped=\\"0\\" time=\\"0.02\\">"
                    f"<testcase name=\\"test_s{{shard_idx}}_a\\" classname=\\"e2e.shard{{shard_idx}}\\" time=\\"0.01\\"/>"
                    f"<testcase name=\\"test_s{{shard_idx}}_b\\" classname=\\"e2e.shard{{shard_idx}}\\" time=\\"0.01\\"/>"
                    f"</testsuite></testsuites>\\n"
                )

            cfg_junit = resolve_tool_config_junit(tool_cfg)
            env_junit = os.environ.get("OX_JUNIT_PATH", "").strip() or os.environ.get("OXIDESWARM_JUNIT_PATH", "").strip()
            target = Path(junit_flag or cfg_junit or env_junit or JUNIT_REL)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(xml_text, encoding="utf-8")

        print(f"FAKE_NEXTEST_RUN shard={{shard_idx}}/{{shard_tot}} archive={{archive_file}} remap={{workspace_remap}}")
        return 100 if is_fail else 0

    sys.stderr.write(f"unsupported fake nextest subcommand: {{sub}}\\n")
    return 2


if __name__ == "__main__":
    sys.exit(main())
'''
    script_path.write_text(code, encoding="utf-8")
    script_path.chmod(0o755)

    cargo_path = bin_dir / "cargo"
    cargo_code = """#!/usr/bin/env python3
import os
import sys
from pathlib import Path

args = sys.argv[1:]
if args in (["--version"], ["-V"]):
    print("cargo 1.84.0 (fake-e2e)")
    sys.exit(0)
if args and args[0] == "nextest":
    nextest_bin = Path(__file__).resolve().with_name("cargo-nextest")
    os.execv(str(nextest_bin), [str(nextest_bin), *args[1:]])
sys.stderr.write(f"unexpected fake cargo invocation: {args!r}\\n")
sys.exit(2)
"""
    cargo_path.write_text(cargo_code, encoding="utf-8")
    cargo_path.chmod(0o755)
    return script_path


def install_fake_blender(
    bin_path: Path,
    log_path: Optional[Path] = None,
    version_text: str = "Blender 4.5.4",
    probe_hang_secs: float = 0.0,
    probe_pid_file: Optional[Path] = None,
    fail_flag_file: Optional[Path] = None,
    empty_out_flag_file: Optional[Path] = None,
    symlink_out_target: Optional[Path] = None,
    symlink_out_flag_file: Optional[Path] = None,
    hang_flag_file: Optional[Path] = None,
    hang_marker_file: Optional[Path] = None,
    hang_pid_file: Optional[Path] = None,
    python_executable: Optional[str] = None,
) -> Path:
    """Create a deterministic fake `blender` binary producing real GLB 2.0 rigs/animations."""
    bin_path.parent.mkdir(parents=True, exist_ok=True)
    shebang = f"#!{python_executable}" if python_executable else "#!/usr/bin/env python3"
    code = f'''{shebang}
import argparse
import json
import os
import struct
import subprocess
import sys
import time
from pathlib import Path

LOG_PATH = {repr(str(log_path)) if log_path else "None"}
VERSION_TEXT = {repr(version_text)}
PROBE_HANG_SECS = {float(probe_hang_secs)}
PROBE_PID_FILE = {repr(str(probe_pid_file)) if probe_pid_file else "None"}
FAIL_FLAG_FILE = {repr(str(fail_flag_file)) if fail_flag_file else "None"}
EMPTY_OUT_FLAG_FILE = {repr(str(empty_out_flag_file)) if empty_out_flag_file else "None"}
SYMLINK_OUT_TARGET = {repr(str(symlink_out_target)) if symlink_out_target else "None"}
SYMLINK_OUT_FLAG_FILE = {repr(str(symlink_out_flag_file)) if symlink_out_flag_file else "None"}
HANG_FLAG_FILE = {repr(str(hang_flag_file)) if hang_flag_file else "None"}
HANG_MARKER_FILE = {repr(str(hang_marker_file)) if hang_marker_file else "None"}
HANG_PID_FILE = {repr(str(hang_pid_file)) if hang_pid_file else "None"}


def log_event(entry):
    target = os.environ.get("FAKE_BLENDER_LOG") or LOG_PATH
    if not target:
        return
    p = Path(target)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\\n")


def detect_magic(raw: bytes) -> str:
    if raw[:4] == b"glTF":
        return "glb"
    stripped = raw.lstrip()
    if stripped.startswith(b"{{"):
        return "gltf"
    for line in stripped.splitlines()[:20]:
        s = line.strip()
        if not s or s.startswith(b"#"):
            continue
        if s.startswith((b"v ", b"vt ", b"vn ", b"f ", b"o ", b"g ", b"mtllib ", b"usemtl ")):
            return "obj"
    return "unknown"


def build_glb(preset: str, clips: list[str], probe_meta=None) -> bytes:
    if preset == "quadruped":
        bones = [
            "Pelvis_Quad", "Spine_Quad", "Neck_Quad", "Head_Quad",
            "FrontLeg_L", "FrontPaw_L", "FrontLeg_R", "FrontPaw_R",
            "HindLeg_L", "HindPaw_L", "HindLeg_R", "HindPaw_R",
            "Tail_Base", "Tail_Tip",
        ]
    elif preset == "generic":
        bones = ["Root_Generic", "Bone_A", "Bone_B", "Bone_C"]
    else:
        bones = [
            "Hips", "Spine", "Chest", "Neck", "Head",
            "UpperLeg_L", "LowerLeg_L", "Foot_L",
            "UpperLeg_R", "LowerLeg_R", "Foot_R",
            "UpperArm_L", "LowerArm_L",
            "UpperArm_R", "LowerArm_R",
        ]

    # Binary buffer: timestamps (3 floats) + rotations (3 vec4 quaternions)
    times = [0.0, 0.5, 1.0]
    quats = [
        0.0, 0.0, 0.0, 1.0,
        0.0, 0.3826834, 0.0, 0.9238795,
        0.0, 0.7071068, 0.0, 0.7071068,
    ]
    bin_buf = bytearray()
    t_off = 0
    bin_buf.extend(struct.pack("<3f", *times))
    q_off = len(bin_buf)
    bin_buf.extend(struct.pack("<12f", *quats))
    while len(bin_buf) % 4 != 0:
        bin_buf.append(0)

    nodes = []
    for idx, bname in enumerate(bones):
        node = {{"name": bname, "translation": [0.0, float(idx) * 0.1, 0.0]}}
        if idx + 1 < len(bones):
            node["children"] = [idx + 1]
        nodes.append(node)

    animations = []
    for clip in clips:
        animations.append({{
            "name": clip,
            "samplers": [
                {{"input": 0, "interpolation": "LINEAR", "output": 1}},
            ],
            "channels": [
                {{"sampler": 0, "target": {{"node": 0, "path": "rotation"}}}},
            ],
        }})

    asset_obj = {{"version": "2.0", "generator": "FakeBlender-4.5.4"}}
    if probe_meta is not None:
        asset_obj["extras"] = {{"wave3_probe": probe_meta}}

    gltf = {{
        "asset": asset_obj,
        "scene": 0,
        "scenes": [{{"nodes": [0]}}],
        "nodes": nodes,
        "skins": [{{"name": f"Armature_{{preset}}", "joints": list(range(len(bones)))}}],
        "buffers": [{{"byteLength": len(bin_buf)}}],
        "bufferViews": [
            {{"buffer": 0, "byteOffset": t_off, "byteLength": len(times) * 4}},
            {{"buffer": 0, "byteOffset": q_off, "byteLength": len(quats) * 4}},
        ],
        "accessors": [
            {{
                "bufferView": 0,
                "componentType": 5126,
                "count": len(times),
                "type": "SCALAR",
                "min": [0.0],
                "max": [1.0],
            }},
            {{
                "bufferView": 1,
                "componentType": 5126,
                "count": len(times),
                "type": "VEC4",
            }},
        ],
        "animations": animations,
    }}

    json_bytes = json.dumps(gltf, separators=(",", ":")).encode("utf-8")
    while len(json_bytes) % 4 != 0:
        json_bytes += b" "

    total_len = 12 + 8 + len(json_bytes) + 8 + len(bin_buf)
    header = struct.pack("<3I", 0x46546C67, 2, total_len)
    j_hdr = struct.pack("<2I", len(json_bytes), 0x4E4F534A)
    b_hdr = struct.pack("<2I", len(bin_buf), 0x004E4942)
    return header + j_hdr + json_bytes + b_hdr + bytes(bin_buf)


def main():
    argv = sys.argv[1:]
    if "--version" in argv or "-v" in argv:
        if PROBE_HANG_SECS > 0:
            child = subprocess.Popen(["sleep", str(int(PROBE_HANG_SECS))])
            if PROBE_PID_FILE:
                Path(PROBE_PID_FILE).write_text(
                    json.dumps({{"probe_pid": os.getpid(), "child_pid": child.pid}}),
                    encoding="utf-8",
                )
            time.sleep(PROBE_HANG_SECS)
            return 0
        print(VERSION_TEXT)
        return 0

    if "--" in argv:
        idx = argv.index("--")
        script_args = argv[idx + 1:]
    else:
        script_args = argv

    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--preset", default="biped")
    p.add_argument("--animations", default="idle,walk")
    ns, _ = p.parse_known_args(script_args)

    in_path = Path(ns.input)
    out_path = Path(ns.output)
    raw_in = in_path.read_bytes() if in_path.is_file() else b""
    magic_type = detect_magic(raw_in)
    ext = in_path.suffix.lower().lstrip(".")

    secret_leaks = {{
        k: (k in os.environ)
        for k in ("OXIDESWARM_SECRET", "OXIDESWARM_SECRET_FILE", "OXIDE_MESH_TOKEN")
    }}

    event = {{
        "pid": os.getpid(),
        "argv": argv,
        "input": str(in_path),
        "input_ext": ext,
        "magic_type": magic_type,
        "output": str(out_path),
        "preset": ns.preset,
        "animations": ns.animations,
        "secret_leaks": secret_leaks,
        "worker_id": os.environ.get("RUSTY_GRID_WORKER_ID", ""),
        "task_id": os.environ.get("RUSTY_GRID_TASK_ID", ""),
        "ts": time.monotonic(),
    }}
    log_event(event)
    print(f"FAKE_BLENDER_EVENT={{json.dumps(event)}}")
    sys.stdout.flush()

    if magic_type != "unknown" and ext != magic_type:
        sys.stderr.write(
            f"Input extension mismatch: path={{in_path}} ext={{ext}} magic={{magic_type}}\\n"
        )
        return 3

    if magic_type == "unknown":
        sys.stderr.write(f"Unrecognized input mesh bytes in {{in_path}}\\n")
        return 4

    if (FAIL_FLAG_FILE and Path(FAIL_FLAG_FILE).exists()) or os.environ.get("FAKE_BLENDER_FAIL") == "1":
        sys.stderr.write("ERR_SIMULATED_BLENDER_CRASH\\n")
        return 2

    if HANG_FLAG_FILE and Path(HANG_FLAG_FILE).exists():
        child = subprocess.Popen(["sleep", "60"])
        if HANG_PID_FILE:
            Path(HANG_PID_FILE).write_text(
                json.dumps({{
                    "blender_pid": os.getpid(),
                    "grandchild_pid": child.pid,
                    "task_id": os.environ.get("RUSTY_GRID_TASK_ID", ""),
                }}),
                encoding="utf-8",
            )
        if HANG_MARKER_FILE:
            Path(HANG_MARKER_FILE).write_text(f"{{os.getpid()}},{{child.pid}}\\n", encoding="utf-8")
        while True:
            time.sleep(0.2)

    if EMPTY_OUT_FLAG_FILE and Path(EMPTY_OUT_FLAG_FILE).exists():
        return 0

    if SYMLINK_OUT_FLAG_FILE and Path(SYMLINK_OUT_FLAG_FILE).exists() and SYMLINK_OUT_TARGET:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if out_path.exists() or out_path.is_symlink():
            out_path.unlink()
        os.symlink(SYMLINK_OUT_TARGET, out_path)
        return 0

    clips = [c.strip() for c in ns.animations.split(",") if c.strip()]
    print("[PROGRESS: 0.20] Importing mesh")
    sys.stdout.flush()
    print("[PROGRESS: 1.00] Exported rigged GLB")
    sys.stdout.flush()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    probe_meta = {{
        "input_ext": ext,
        "magic_type": magic_type,
        "secret_leaks": secret_leaks,
    }}
    out_path.write_bytes(build_glb(ns.preset, clips, probe_meta=probe_meta))
    return 0


if __name__ == "__main__":
    sys.exit(main())
'''
    bin_path.write_text(code, encoding="utf-8")
    bin_path.chmod(0o755)
    return bin_path


# ---------------------------------------------------------------------------
# Mesh Generators & GLB / JUnit Parsers
# ---------------------------------------------------------------------------


def make_minimal_obj_bytes() -> bytes:
    obj = (
        "# Minimal cube OBJ\n"
        "v 0.0 0.0 0.0\n"
        "v 1.0 0.0 0.0\n"
        "v 1.0 1.0 0.0\n"
        "v 0.0 1.0 0.0\n"
        "v 0.0 0.0 1.0\n"
        "v 1.0 0.0 1.0\n"
        "v 1.0 1.0 1.0\n"
        "v 0.0 1.0 1.0\n"
        "f 1 2 3\n"
        "f 1 3 4\n"
        "f 5 6 7\n"
        "f 5 7 8\n"
        "f 1 2 6\n"
        "f 1 6 5\n"
        "f 2 3 7\n"
        "f 2 7 6\n"
        "f 3 4 8\n"
        "f 3 8 7\n"
        "f 4 1 5\n"
        "f 4 5 8\n"
    )
    return obj.encode("utf-8")


def make_minimal_gltf_bytes() -> bytes:
    raw_buf = struct.pack("<9f3H", 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0, 1, 2)
    b64 = base64.b64encode(raw_buf).decode("ascii")
    gltf = {
        "asset": {"version": "2.0", "generator": "wave3-e2e"},
        "scene": 0,
        "scenes": [{"nodes": [0]}],
        "nodes": [{"mesh": 0}],
        "meshes": [{"primitives": [{"attributes": {"POSITION": 0}, "indices": 1}]}],
        "buffers": [{"uri": f"data:application/octet-stream;base64,{b64}", "byteLength": len(raw_buf)}],
        "bufferViews": [
            {"buffer": 0, "byteOffset": 0, "byteLength": 36},
            {"buffer": 0, "byteOffset": 36, "byteLength": 6},
        ],
        "accessors": [
            {"bufferView": 0, "componentType": 5126, "count": 3, "type": "VEC3", "min": [0, 0, 0], "max": [1, 1, 0]},
            {"bufferView": 1, "componentType": 5123, "count": 3, "type": "SCALAR"},
        ],
    }
    return json.dumps(gltf, indent=2).encode("utf-8")


def make_minimal_glb_bytes() -> bytes:
    # 8-vertex, 12-triangle elongated 3D cuboid:
    # x in [-0.4, 0.4], y in [0.0, 1.8], z in [-0.2, 0.2]
    vertices = (
        -0.4, 0.0, -0.2,  # 0: back-bottom-left
        0.4, 0.0, -0.2,   # 1: back-bottom-right
        0.4, 1.8, -0.2,   # 2: back-top-right
        -0.4, 1.8, -0.2,  # 3: back-top-left
        -0.4, 0.0, 0.2,   # 4: front-bottom-left
        0.4, 0.0, 0.2,    # 5: front-bottom-right
        0.4, 1.8, 0.2,    # 6: front-top-right
        -0.4, 1.8, 0.2,   # 7: front-top-left
    )
    indices = (
        0, 2, 1, 0, 3, 2,  # back (-Z)
        4, 5, 6, 4, 6, 7,  # front (+Z)
        0, 1, 5, 0, 5, 4,  # bottom (-Y)
        3, 6, 2, 3, 7, 6,  # top (+Y)
        0, 4, 7, 0, 7, 3,  # left (-X)
        1, 2, 6, 1, 6, 5,  # right (+X)
    )
    raw_buf = struct.pack("<24f36H", *vertices, *indices)
    gltf = {
        "asset": {"version": "2.0", "generator": "wave3-e2e"},
        "scene": 0,
        "scenes": [{"nodes": [0]}],
        "nodes": [{"mesh": 0}],
        "meshes": [{"primitives": [{"attributes": {"POSITION": 0}, "indices": 1}]}],
        "buffers": [{"byteLength": len(raw_buf)}],
        "bufferViews": [
            {"buffer": 0, "byteOffset": 0, "byteLength": 96},
            {"buffer": 0, "byteOffset": 96, "byteLength": 72},
        ],
        "accessors": [
            {
                "bufferView": 0,
                "componentType": 5126,
                "count": 8,
                "type": "VEC3",
                "min": [-0.4, 0.0, -0.2],
                "max": [0.4, 1.8, 0.2],
            },
            {"bufferView": 1, "componentType": 5123, "count": 36, "type": "SCALAR"},
        ],
    }
    jb = json.dumps(gltf, separators=(",", ":")).encode("utf-8")
    while len(jb) % 4 != 0:
        jb += b" "
    total = 12 + 8 + len(jb) + 8 + len(raw_buf)
    return (
        struct.pack("<3I", 0x46546C67, 2, total)
        + struct.pack("<2I", len(jb), 0x4E4F534A)
        + jb
        + struct.pack("<2I", len(raw_buf), 0x004E4942)
        + bytes(raw_buf)
    )


def _decode_glb_chunks(glb_bytes: bytes) -> tuple[dict[str, Any], bytes]:
    if len(glb_bytes) < 20:
        raise AssertionError(f"GLB too small ({len(glb_bytes)} bytes)")
    magic, version, total_len = struct.unpack("<3I", glb_bytes[:12])
    if magic != 0x46546C67:
        raise AssertionError(f"Invalid GLB magic: {glb_bytes[:4]!r}")
    if version != 2:
        raise AssertionError(f"Unexpected GLB version: {version}")
    if total_len != len(glb_bytes):
        raise AssertionError(f"GLB header length {total_len} != actual {len(glb_bytes)}")
    chunk_len, chunk_type = struct.unpack("<2I", glb_bytes[12:20])
    if chunk_type != 0x4E4F534A:
        raise AssertionError(f"First GLB chunk is not JSON: 0x{chunk_type:08x}")
    if 20 + chunk_len > len(glb_bytes):
        raise AssertionError("GLB JSON chunk overflows file length")
    parsed = json.loads(glb_bytes[20 : 20 + chunk_len].decode("utf-8"))
    if parsed.get("asset", {}).get("version") != "2.0":
        raise AssertionError(f"Invalid glTF asset version: {parsed.get('asset')!r}")
    bin_bytes = b""
    off = 20 + chunk_len
    if off + 8 <= len(glb_bytes):
        b_len, b_type = struct.unpack("<2I", glb_bytes[off : off + 8])
        if b_type == 0x004E4942 and off + 8 + b_len <= len(glb_bytes):
            bin_bytes = glb_bytes[off + 8 : off + 8 + b_len]
    return parsed, bin_bytes


def parse_glb_json(glb_bytes: bytes) -> dict[str, Any]:
    parsed, _ = _decode_glb_chunks(glb_bytes)
    return parsed


def _read_accessor_tuples(
    gltf: dict[str, Any], bin_chunk: bytes, accessor_idx: int
) -> list[tuple[Any, ...]]:
    accessors = gltf.get("accessors", [])
    buffer_views = gltf.get("bufferViews", [])
    buffers = gltf.get("buffers", [])
    if not (0 <= accessor_idx < len(accessors)):
        return []
    acc = accessors[accessor_idx]
    bv_idx = acc.get("bufferView")
    if not isinstance(bv_idx, int) or not (0 <= bv_idx < len(buffer_views)):
        return []
    bv = buffer_views[bv_idx]
    buf_idx = bv.get("buffer", 0)
    raw_buf = bin_chunk
    if not raw_buf and 0 <= buf_idx < len(buffers):
        uri = str(buffers[buf_idx].get("uri", ""))
        if uri.startswith("data:") and ";base64," in uri:
            raw_buf = base64.b64decode(uri.split(";base64,", 1)[1])
    if not raw_buf:
        return []

    comp_map = {
        5120: ("b", 1),
        5121: ("B", 1),
        5122: ("h", 2),
        5123: ("H", 2),
        5125: ("I", 4),
        5126: ("f", 4),
    }
    type_counts = {"SCALAR": 1, "VEC2": 2, "VEC3": 3, "VEC4": 4, "MAT2": 4, "MAT3": 9, "MAT4": 16}
    comp_fmt, comp_sz = comp_map.get(int(acc.get("componentType", 0)), ("", 0))
    n_comp = type_counts.get(str(acc.get("type", "")), 0)
    count = int(acc.get("count", 0))
    if not comp_fmt or n_comp == 0 or count <= 0:
        return []

    base_off = int(bv.get("byteOffset", 0)) + int(acc.get("byteOffset", 0))
    elem_size = comp_sz * n_comp
    stride = int(bv.get("byteStride") or elem_size)
    fmt = f"<{n_comp}{comp_fmt}"
    out: list[tuple[Any, ...]] = []
    for i in range(count):
        pos = base_off + i * stride
        if pos + elem_size > len(raw_buf):
            break
        vals = struct.unpack_from(fmt, raw_buf, pos)
        if comp_fmt == "f":
            vals = tuple(round(float(v), 6) for v in vals)
        out.append(vals)
    return out


def extract_glb_rig_summary(glb_bytes: bytes) -> dict[str, Any]:
    gltf, bin_chunk = _decode_glb_chunks(glb_bytes)
    nodes = gltf.get("nodes", [])
    node_names = [str(n.get("name", "")) for n in nodes if n.get("name")]
    skins = gltf.get("skins", [])
    skin_joint_names: list[str] = []
    for sk in skins:
        for j_idx in sk.get("joints", []):
            if 0 <= j_idx < len(nodes) and nodes[j_idx].get("name"):
                skin_joint_names.append(str(nodes[j_idx]["name"]))
    accessors = gltf.get("accessors", [])
    animations_info: dict[str, dict[str, Any]] = {}
    for anim in gltf.get("animations", []):
        name = str(anim.get("name", ""))
        channels = anim.get("channels", [])
        samplers = anim.get("samplers", [])
        counts = []
        nonconst_times = False
        nonconst_values = False
        for smp in samplers:
            in_idx = smp.get("input")
            out_idx = smp.get("output")
            if isinstance(in_idx, int) and 0 <= in_idx < len(accessors):
                counts.append(int(accessors[in_idx].get("count", 0)))
                t_vals = _read_accessor_tuples(gltf, bin_chunk, in_idx)
                if len(set(t_vals)) >= 2:
                    nonconst_times = True
            if isinstance(out_idx, int) and 0 <= out_idx < len(accessors):
                counts.append(int(accessors[out_idx].get("count", 0)))
                o_vals = _read_accessor_tuples(gltf, bin_chunk, out_idx)
                if len(set(o_vals)) >= 2:
                    nonconst_values = True
        animations_info[name] = {
            "channels": len(channels),
            "samplers": len(samplers),
            "min_accessor_count": min(counts) if counts else 0,
            "has_nonconstant_times": nonconst_times,
            "has_nonconstant_values": nonconst_values,
        }
    return {
        "node_names": node_names,
        "skin_count": len(skins),
        "skin_joint_names": skin_joint_names,
        "animations": animations_info,
        "extras": gltf.get("asset", {}).get("extras", {}),
    }


# ---------------------------------------------------------------------------
# OxideSwarm Cluster & Agent Mesh Harnesses
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
        fail_unfulfillable_tasks: Optional[bool] = None,
        preserve_gpu: Optional[bool] = None,
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
        if fail_unfulfillable_tasks is True:
            argv.append("--fail-unfulfillable-tasks")
        elif fail_unfulfillable_tasks is False:
            argv.extend(["--fail-unfulfillable-tasks", "false"])
        if preserve_gpu is not None:
            argv.extend(["--preserve-gpu", "true" if preserve_gpu else "false"])
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
        isolation_read_dirs: Iterable[Path | str] = (),
        workspaces: bool = False,
        workspace_root: Optional[Path] = None,
        sandbox_base_dir: Optional[Path] = None,
        home_dir: Optional[Path] = None,
        ox_home_dir: Optional[Path] = None,
        secret_file: Optional[Path] = None,
        cores: int = 2,
        max_concurrency: Optional[int] = None,
        gpu_mode: str = "none",
        vram_mb: Optional[int] = None,
        gpu_name: Optional[str] = None,
        blender_mode: str = "none",
        blender_path: Optional[Path | str] = None,
        blender_version: Optional[str] = None,
        session_resume_grace_secs: Optional[int] = None,
        env_extra: Optional[dict[str, str]] = None,
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
            "--no-portal",
            "--cores",
            str(cores),
            "--sandbox-base-dir",
            str(sb),
        ]
        if max_concurrency is not None:
            argv.extend(["--max-concurrency", str(max_concurrency)])

        if gpu_mode == "none":
            argv.extend(["--no-gpu", "--no-cuda"])
        elif gpu_mode == "simulate":
            argv.append("--simulate-gpu")
        elif gpu_mode == "hardware":
            argv.append("--gpu")
        else:
            raise ValueError(f"Unsupported gpu_mode: {gpu_mode!r}")

        if vram_mb is not None:
            argv.extend(["--vram-mb", str(vram_mb)])
        if gpu_name is not None:
            argv.extend(["--gpu-name", gpu_name])

        if blender_mode == "none":
            argv.append("--no-blender")
        elif blender_mode == "enabled":
            argv.append("--blender")
        elif blender_mode == "auto":
            pass
        else:
            raise ValueError(f"Unsupported blender_mode: {blender_mode!r}")

        if blender_path is not None:
            argv.extend(["--blender-path", str(blender_path)])
        if blender_version is not None:
            argv.extend(["--blender-version", blender_version])

        for rd in isolation_read_dirs:
            argv.extend(["--isolation-read-dir", str(rd)])
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

        mp = self.env.spawn(
            name=f"worker-{name}",
            argv=argv,
            home_dir=wh,
            ox_home_dir=ox_home_dir,
            env_extra=env_extra,
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
        env_extra: Optional[dict[str, str]] = None,
        timeout: float = 45.0,
    ) -> CliResult:
        argv = self._base_argv(secret_file) + [
            "submit",
            "--master",
            master_addr,
        ] + args
        return self.env.run_cmd(argv, cwd=cwd, env_extra=env_extra, timeout=timeout)

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

    def shard_cli(
        self,
        master_addr: Optional[str],
        args: list[str],
        cwd: Optional[Path] = None,
        secret_file: Optional[Path] = None,
        env_extra: Optional[dict[str, str]] = None,
        timeout: float = 45.0,
    ) -> CliResult:
        argv = self._base_argv(secret_file) + ["shard"]
        if master_addr is not None:
            argv.extend(["--master", master_addr])
        argv.extend(args)
        return self.env.run_cmd(argv, cwd=cwd, env_extra=env_extra, timeout=timeout)

    def shard_cli_popen(
        self,
        master_addr: str,
        args: list[str],
        cwd: Optional[Path] = None,
        secret_file: Optional[Path] = None,
    ) -> ManagedProcess:
        argv = self._base_argv(secret_file) + [
            "shard",
            "--master",
            master_addr,
        ] + args
        return self.env.spawn(name="shard-bg", argv=argv, cwd=cwd)

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

    def cancel_cli(
        self,
        master_addr: str,
        task_id: str,
        json_out: bool = True,
        secret_file: Optional[Path] = None,
        timeout: float = 15.0,
    ) -> CliResult:
        argv = self._base_argv(secret_file) + ["cancel", task_id, "--master", master_addr]
        if json_out:
            argv.append("--json")
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

    def _build_hub_argv(
        self,
        listen: str = "127.0.0.1:0",
        token: Optional[str] = None,
        insecure: bool = False,
        grid_connect: Optional[str] = None,
        grid_master: Optional[str] = None,
        secret: Optional[str] = None,
        secret_file: Optional[Path] = None,
        extra_args: Optional[list[str]] = None,
    ) -> list[str]:
        argv = [self.bin, "hub", "--listen", listen]
        if token is not None:
            argv.extend(["--token", token])
        if insecure:
            argv.append("--insecure")
        if grid_connect is not None:
            argv.extend(["--grid-connect", grid_connect])
        if grid_master is not None:
            argv.extend(["--grid-master", grid_master])
        if secret is not None:
            argv.extend(["--secret", secret])
        if secret_file is not None:
            argv.extend(["--secret-file", str(secret_file)])
        if extra_args:
            argv.extend(extra_args)
        return argv

    def run_hub_cli(
        self,
        listen: str = "127.0.0.1:0",
        token: Optional[str] = None,
        insecure: bool = False,
        grid_connect: Optional[str] = None,
        grid_master: Optional[str] = None,
        secret: Optional[str] = None,
        secret_file: Optional[Path] = None,
        env_extra: Optional[dict[str, str]] = None,
        extra_args: Optional[list[str]] = None,
        timeout: float = 10.0,
    ) -> CliResult:
        argv = self._build_hub_argv(
            listen=listen,
            token=token,
            insecure=insecure,
            grid_connect=grid_connect,
            grid_master=grid_master,
            secret=secret,
            secret_file=secret_file,
            extra_args=extra_args,
        )
        return self.env.run_cmd(argv, env_extra=env_extra, timeout=timeout)

    def start_hub(
        self,
        listen: str = "127.0.0.1:0",
        token: Optional[str] = None,
        insecure: bool = False,
        grid_connect: Optional[str] = None,
        grid_master: Optional[str] = None,
        secret: Optional[str] = None,
        secret_file: Optional[Path] = None,
        env_extra: Optional[dict[str, str]] = None,
        extra_args: Optional[list[str]] = None,
        timeout: float = 15.0,
    ) -> MeshHubInstance:
        argv = self._build_hub_argv(
            listen=listen,
            token=token,
            insecure=insecure,
            grid_connect=grid_connect,
            grid_master=grid_master,
            secret=secret,
            secret_file=secret_file,
            extra_args=extra_args,
        )

        url_q: queue.Queue[str] = queue.Queue()

        def _on_line(line: str) -> None:
            if "running at " in line:
                url_q.put(line.split("running at ", 1)[1].strip())

        mp = self.env.spawn(
            name="mesh-hub",
            argv=argv,
            env_extra=env_extra,
            on_stdout_line=_on_line,
        )
        ws_url: Optional[str] = None
        wait_deadline = time.monotonic() + timeout
        while time.monotonic() < wait_deadline:
            try:
                ws_url = url_q.get(timeout=0.05)
                break
            except queue.Empty:
                if mp.poll() is not None:
                    for t in mp._threads:
                        t.join(timeout=0.2)
                    try:
                        ws_url = url_q.get_nowait()
                        break
                    except queue.Empty:
                        pass
                    raise RuntimeError(
                        f"agent-mesh hub exited prematurely (rc={mp.poll()}):\n{mp.logs()}"
                    )
        if ws_url is None:
            raise TimeoutError(
                f"agent-mesh hub did not print ws URL within {timeout}s:\n{mp.logs()}"
            )

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
        platform_name: str = "macos",
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
            platform_name,
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
# Shared Specification Extraction & Assertion Helpers
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


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    out: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if s:
            out.append(json.loads(s))
    return out


def _extract_shard_records(parsed_json: Any) -> list[dict[str, Any]]:
    if isinstance(parsed_json, list):
        records = parsed_json
    elif isinstance(parsed_json, dict):
        records = None
        for key in ("shards", "cells", "results", "tasks"):
            if isinstance(parsed_json.get(key), list):
                records = parsed_json[key]
                break
        if records is None:
            raise AssertionError(
                f"Shard JSON missing 'shards'/'cells'/'results' list: {parsed_json!r}"
            )
    else:
        raise AssertionError(f"Unexpected shard JSON type {type(parsed_json)}: {parsed_json!r}")

    if not records:
        raise AssertionError(f"Expected non-empty shard list, got: {records!r}")
    for r in records:
        if not isinstance(r, dict):
            raise AssertionError(f"Expected dict shard record, got: {r!r}")
    return records


def _record_stdout(rec: dict[str, Any]) -> str:
    parts = [str(rec.get(k, "")) for k in ("stdout", "output_head", "output_tail", "head", "tail")]
    return "\n".join(p for p in parts if p)


def _record_worker(rec: dict[str, Any]) -> str:
    for key in ("worker_name", "worker", "worker_id"):
        v = rec.get(key)
        if v:
            return str(v)
    return ""


def _worker_has_blender(w: dict[str, Any]) -> bool:
    if isinstance(w.get("has_blender"), bool):
        return bool(w["has_blender"])
    caps = w.get("capabilities")
    if isinstance(caps, dict) and isinstance(caps.get("has_blender"), bool):
        return bool(caps["has_blender"])
    if w.get("blender_version"):
        return True
    tags = set(w.get("tags", []))
    return "feat:blender" in tags or "blender" in tags


def _worker_vram_mb(w: dict[str, Any]) -> Optional[int]:
    val = w.get("vram_mb")
    if val is None and isinstance(w.get("capabilities"), dict):
        val = w["capabilities"].get("vram_mb")
    return int(val) if isinstance(val, (int, float)) else None


def _collect_inbox_payloads(inbox_dir: Path) -> list[tuple[Path, bytes]]:
    if not inbox_dir.exists():
        return []
    out: list[tuple[Path, bytes]] = []
    for p in sorted(inbox_dir.rglob("*")):
        if not p.is_file():
            continue
        if p.name.endswith(".meta.json") or p.name.endswith(".sha256"):
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


def _decode_artifact_bytes(artifact: dict[str, Any]) -> bytes:
    if "data_base64" in artifact and isinstance(artifact["data_base64"], str):
        return base64.b64decode(artifact["data_base64"])
    if "data" in artifact and isinstance(artifact["data"], str):
        raw = artifact["data"]
        enc = str(artifact.get("encoding", "")).lower()
        if enc == "base64":
            return base64.b64decode(raw)
        if enc in ("utf8", "utf-8", "text"):
            return raw.encode("utf-8")
        try:
            return base64.b64decode(raw, validate=True)
        except Exception:
            return raw.encode("utf-8")
    if "content" in artifact and isinstance(artifact["content"], str):
        return artifact["content"].encode("utf-8")
    raise AssertionError(f"Artifact missing data/data_base64/content: {artifact!r}")


def _worker_uuid(w: dict[str, Any]) -> Optional[str]:
    for key in ("worker_id", "id"):
        val = w.get(key)
        if val:
            return str(val)
    for tag in w.get("tags", []):
        s = str(tag)
        if s.startswith("id:"):
            return s[3:]
    return None


def _resolve_worker_name_from_list(
    workers: list[dict[str, Any]], worker_ref: Optional[str]
) -> Optional[str]:
    if not worker_ref:
        return None
    ref = str(worker_ref).strip().lower()
    for w in workers:
        name = str(w.get("name", ""))
        wid = (_worker_uuid(w) or "").lower()
        if ref == name.lower() or (wid and ref == wid):
            return name
    return None


def _extract_mesh_artifacts(resp: dict[str, Any]) -> list[dict[str, Any]]:
    payload = resp.get("payload")
    if not isinstance(payload, dict):
        raise AssertionError(f"Expected dict payload with artifacts, got: {resp!r}")
    arts = payload.get("artifacts")
    if arts is None:
        arts = payload.get("output_artifacts")
    if not isinstance(arts, list) or not arts:
        raise AssertionError(f"Expected non-empty 'artifacts' list in payload, got: {resp!r}")
    return arts


def _assert_3d_not_disabled(res: CliResult) -> None:
    combined = f"{res.stdout}\n{res.stderr}".lower()
    if res.returncode != 0 and (
        "without 3d" in combined
        or "without the '3d' feature" in combined
        or "without the `3d` feature" in combined
        or "features 3d" in combined
        or "feature '3d'" in combined
        or "feature `3d`" in combined
        or "3d feature" in combined
        or ("3d" in combined and "not compiled" in combined)
        or ("3d" in combined and "not enabled" in combined)
    ):
        raise AssertionError(
            "oxideswarm binary was compiled WITHOUT `--features 3d`. "
            "Rebuild with `cargo build -p rusty_grid_cli --bin oxideswarm --features 3d` "
            f"before running 3D-2/3D-3 E2E tests. Output:\n{res.stdout}\n{res.stderr}"
        )


def _make_mini_rust_project(proj_dir: Path) -> None:
    proj_dir.mkdir(parents=True, exist_ok=True)
    (proj_dir / "Cargo.toml").write_text(
        '[package]\nname = "mini_shard_demo"\nversion = "0.1.0"\nedition = "2021"\n',
        encoding="utf-8",
    )
    src = proj_dir / "src"
    src.mkdir(exist_ok=True)
    (src / "lib.rs").write_text(
        "pub fn add(a: i32, b: i32) -> i32 { a + b }\n",
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Base TestCase with Automatic Isolation
# ---------------------------------------------------------------------------


class Wave3E2ETestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.env = IsolatedEnv()
        self.addCleanup(self.env.close)
        self.ox = OxideCluster(self.env)
        self.mesh = MeshCluster(self.env)

    def _require_darwin_for_seatbelt(self) -> None:
        if sys.platform != "darwin":
            self.skipTest("macOS Seatbelt isolation (--isolation fs) requires Darwin")

    def _resolve_worker_name(
        self,
        master_addr: str,
        worker_ref: Optional[str],
        secret_file: Optional[Path] = None,
    ) -> Optional[str]:
        workers = self.ox.workers_json(master_addr, secret_file=secret_file)
        return _resolve_worker_name_from_list(workers, worker_ref)

    def _ensure_3d_feature_enabled(
        self, master_addr: str, secret_file: Optional[Path] = None
    ) -> None:
        """Verify oxideswarm & connected workers were compiled with `--features 3d`."""
        probe = self.ox.submit_cli(
            master_addr,
            ["--prompt", "probe cube", "--timeout-secs", "15", "--wait", "--json"],
            secret_file=secret_file,
            timeout=20.0,
        )
        _assert_3d_not_disabled(probe)
        self.assertEqual(
            probe.returncode,
            0,
            f"3D feature probe failed (ensure `oxideswarm` is built with `--features 3d`):\n"
            f"stderr:\n{probe.stderr}\nstdout:\n{probe.stdout}",
        )

    def _assert_worker_has_3d_support(
        self,
        master_addr: str,
        secret_file: Optional[Path] = None,
        run_probe: bool = True,
    ) -> None:
        workers = self.ox.workers_json(master_addr, secret_file=secret_file)
        if not workers:
            return
        if run_probe:
            self._ensure_3d_feature_enabled(master_addr, secret_file=secret_file)

    def _assert_valid_animation(
        self,
        anims: dict[str, dict[str, Any]],
        clip: str,
        ctx: str,
        check_nonconstant: bool = True,
    ) -> None:
        self.assertIn(clip, anims, f"{ctx} missing animation '{clip}': {anims!r}")
        info = anims[clip]
        self.assertGreater(info.get("channels", 0), 0, f"{ctx} '{clip}' has 0 channels: {info!r}")
        self.assertGreater(info.get("samplers", 0), 0, f"{ctx} '{clip}' has 0 samplers: {info!r}")
        self.assertGreater(
            info.get("min_accessor_count", 0),
            1,
            f"{ctx} '{clip}' has empty/single-frame keyframes: {info!r}",
        )
        if check_nonconstant:
            self.assertTrue(
                info.get("has_nonconstant_times"),
                f"{ctx} '{clip}' has constant/duplicate timestamps: {info!r}",
            )
            self.assertTrue(
                info.get("has_nonconstant_values"),
                f"{ctx} '{clip}' has constant/static keyframe values: {info!r}",
            )


# ===========================================================================
# TIER 1: Feature Coverage (24 tests — 6 each for AX-2, MESH-3, 3D-2, 3D-3)
# ===========================================================================


class Tier1FeatureCoverageTests(Wave3E2ETestBase):
    """Tier 1: Core feature coverage across AX-2, MESH-3, 3D-2, and 3D-3."""

    # --- AX-2: Sharded Runs & Rust Build-Once (6 tests) ---

    def test_t1_ax2_01_shard_env_injection_and_all_shards_complete(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-sh-1", cores=2, max_concurrency=2)
        self.ox.start_worker(master.listen_addr, name="w-sh-2", cores=2, max_concurrency=2)

        res = self.ox.shard_cli(
            master.listen_addr,
            [
                "--count", "4",
                "--json",
                "--",
                "sh", "-c", 'echo "IDX=$OX_SHARD_INDEX TOT=$OX_SHARD_TOTAL"',
            ],
        )
        self.assertEqual(res.returncode, 0, f"stderr:\n{res.stderr}\nstdout:\n{res.stdout}")
        payload = json.loads(res.stdout)
        shards = _extract_shard_records(payload)
        self.assertEqual(len(shards), 4)

        seen_pairs: set[tuple[str, str]] = set()
        for s in shards:
            m = re.search(r"IDX=(\d+)\s+TOT=(\d+)", _record_stdout(s))
            self.assertIsNotNone(m, f"Missing OX_SHARD_* output in shard record: {s!r}")
            assert m is not None
            seen_pairs.add((m.group(1), m.group(2)))
        self.assertEqual(seen_pairs, {("1", "4"), ("2", "4"), ("3", "4"), ("4", "4")})

        if isinstance(payload, dict):
            if "machines" in payload:
                self.assertGreaterEqual(int(payload["machines"]), 1)
            if "waited_ms" in payload:
                self.assertGreaterEqual(int(payload["waited_ms"]), 0)
            if "task_time_sum_ms" in payload:
                self.assertGreaterEqual(int(payload["task_time_sum_ms"]), 0)

    def test_t1_ax2_02_slot_aware_spreading_across_workers(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-spread-1", cores=4, max_concurrency=2)
        self.ox.start_worker(master.listen_addr, name="w-spread-2", cores=4, max_concurrency=2)

        marker = self.env.root / "spread_marker.log"
        gate = self.env.root / "spread_gate"
        script = (
            f'printf "%s\\n" "$OX_SHARD_INDEX" >> "{marker}"\n'
            f'while [ ! -f "{gate}" ]; do sleep 0.02; done\n'
            'echo "SPREAD_OK_$OX_SHARD_INDEX"\n'
        )
        proc = self.ox.shard_cli_popen(
            master.listen_addr,
            ["--count", "4", "--json", "--", "sh", "-c", script],
        )
        _wait_until(
            lambda: marker.is_file() and len(marker.read_text(encoding="utf-8").splitlines()) == 4,
            timeout=15.0,
            desc="all 4 shards running simultaneously across 2 workers",
        )
        gate.write_text("go\n", encoding="utf-8")
        self.assertEqual(proc.wait(timeout=20.0), 0, proc.logs())

        out_text = "\n".join(proc.stdout_lines)
        payload = json.loads(out_text)
        shards = _extract_shard_records(payload)
        workers_used = {_record_worker(s) for s in shards if _record_worker(s)}
        self.assertEqual(
            len(workers_used),
            2,
            f"Expected shards spread across 2 workers, got {workers_used} in {shards!r}",
        )

    def test_t1_ax2_03_build_once_archive_built_once_and_reused_by_shards(self) -> None:
        nextest_bin = self.env.root / "nextest_bin"
        log_path = self.env.root / "nextest_invocations.jsonl"
        install_fake_cargo_nextest(nextest_bin, log_path=log_path)

        proj_dir = self.env.work / "mini_crate"
        _make_mini_rust_project(proj_dir)

        master = self.ox.start_master()
        worker_env = {"PATH": str(nextest_bin)}
        self.ox.start_worker(
            master.listen_addr, name="w-nx-1", cores=2, max_concurrency=2, env_extra=worker_env
        )
        self.ox.start_worker(
            master.listen_addr, name="w-nx-2", cores=2, max_concurrency=2, env_extra=worker_env
        )

        res = self.ox.shard_cli(
            master.listen_addr,
            [
                "--count", "3",
                "--build-once",
                "--build-source", str(proj_dir),
                "--json",
            ],
            cwd=proj_dir,
            env_extra=worker_env,
        )
        self.assertEqual(res.returncode, 0, f"stderr:\n{res.stderr}\nstdout:\n{res.stdout}")
        events = _read_jsonl(log_path)
        archives = [e for e in events if e.get("cmd") == "archive"]
        runs = [e for e in events if e.get("cmd") == "run"]

        self.assertEqual(
            len(archives),
            1,
            f"Expected --build-once to run archive exactly once per (os, arch), got {archives!r}",
        )
        self.assertEqual(len(runs), 3, f"Expected 3 shard runs, got {runs!r}")
        for r in runs:
            self.assertTrue(r.get("archive_file"), f"Shard run missing --archive-file: {r!r}")
            self.assertTrue(r.get("archive_valid"), f"Shard archive invalid: {r!r}")
            self.assertTrue(r.get("workspace_remap"), f"Shard run missing --workspace-remap: {r!r}")
        self.assertEqual({r.get("shard_index") for r in runs}, {1, 2, 3})

    def test_t1_ax2_04_junit_out_merges_per_shard_xml_reports(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-j-1", cores=2, max_concurrency=2)
        self.ox.start_worker(master.listen_addr, name="w-j-2", cores=2, max_concurrency=2)

        merged_xml = self.env.root / "reports" / "merged_junit.xml"
        shard_py = (
            "import os, pathlib\n"
            "idx = os.environ['OX_SHARD_INDEX']\n"
            "skip = '<skipped message=\"skipped in shard\"/>' if idx == '2' else ''\n"
            "sk_attr = '1' if idx == '2' else '0'\n"
            "xml = (\n"
            "  f'<testsuites tests=\"2\" failures=\"0\" errors=\"0\" skipped=\"{sk_attr}\" time=\"0.02\">'\n"
            "  f'<testsuite name=\"suite_{idx}\" tests=\"2\" failures=\"0\" errors=\"0\" skipped=\"{sk_attr}\" time=\"0.02\">'\n"
            "  f'<testcase name=\"tc_{idx}_a\" classname=\"w3.shard\" time=\"0.01\"/>'\n"
            "  f'<testcase name=\"tc_{idx}_b\" classname=\"w3.shard\" time=\"0.01\">{skip}</testcase>'\n"
            "  '</testsuite></testsuites>\\n'\n"
            ")\n"
            "p = pathlib.Path(os.environ.get('OX_JUNIT_PATH') or os.environ.get('OXIDESWARM_JUNIT_PATH') or 'target/nextest/default/junit.xml')\n"
            "p.parent.mkdir(parents=True, exist_ok=True)\n"
            "p.write_text(xml, encoding='utf-8')\n"
        )
        res = self.ox.shard_cli(
            master.listen_addr,
            [
                "--count", "3",
                "--junit-out", str(merged_xml),
                "--",
                sys.executable, "-c", shard_py,
            ],
        )
        self.assertEqual(res.returncode, 0, f"stderr:\n{res.stderr}\nstdout:\n{res.stdout}")
        self.assertTrue(merged_xml.is_file(), f"Missing merged JUnit XML at {merged_xml}")

        root = ET.fromstring(merged_xml.read_text(encoding="utf-8"))
        self.assertEqual(root.tag, "testsuites")
        testcases = root.findall(".//testcase")
        names = {tc.attrib.get("name") for tc in testcases}
        self.assertEqual(
            names,
            {"tc_1_a", "tc_1_b", "tc_2_a", "tc_2_b", "tc_3_a", "tc_3_b"},
        )
        self.assertEqual(int(root.attrib.get("tests", len(testcases))), 6)
        self.assertEqual(int(root.attrib.get("failures", 0)), 0)
        skipped_nodes = root.findall(".//skipped")
        self.assertEqual(len(skipped_nodes), 1)

    def test_t1_ax2_05_junit_merge_aggregates_failures_and_exits_nonzero(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-jf-1", cores=2, max_concurrency=2)

        merged_xml = self.env.root / "reports" / "merged_fail.xml"
        shard_py = (
            "import os, pathlib, sys\n"
            "idx = os.environ['OX_SHARD_INDEX']\n"
            "fail_block = '<failure message=\"shard2 boom\" type=\"AssertionError\">detail</failure>' if idx == '2' else ''\n"
            "f_cnt = '1' if idx == '2' else '0'\n"
            "xml = (\n"
            "  f'<testsuites tests=\"1\" failures=\"{f_cnt}\" errors=\"0\" skipped=\"0\" time=\"0.01\">'\n"
            "  f'<testsuite name=\"suite_{idx}\" tests=\"1\" failures=\"{f_cnt}\" errors=\"0\" skipped=\"0\" time=\"0.01\">'\n"
            "  f'<testcase name=\"tc_{idx}\" classname=\"w3.fail\" time=\"0.01\">{fail_block}</testcase>'\n"
            "  '</testsuite></testsuites>\\n'\n"
            ")\n"
            "p = pathlib.Path(os.environ.get('OX_JUNIT_PATH') or os.environ.get('OXIDESWARM_JUNIT_PATH') or 'target/nextest/default/junit.xml')\n"
            "p.parent.mkdir(parents=True, exist_ok=True)\n"
            "p.write_text(xml, encoding='utf-8')\n"
            "sys.exit(1 if idx == '2' else 0)\n"
        )
        res = self.ox.shard_cli(
            master.listen_addr,
            [
                "--count", "3",
                "--junit-out", str(merged_xml),
                "--",
                sys.executable, "-c", shard_py,
            ],
        )
        self.assertNotEqual(res.returncode, 0, "Expected non-zero exit when shard 2 failed")
        self.assertTrue(merged_xml.is_file(), "Merged JUnit XML must still be emitted on failure")

        root = ET.fromstring(merged_xml.read_text(encoding="utf-8"))
        self.assertEqual(int(root.attrib.get("tests", 0)), 3)
        self.assertEqual(int(root.attrib.get("failures", 0)), 1)
        failures = root.findall(".//failure")
        self.assertEqual(len(failures), 1)
        self.assertIn("shard2 boom", failures[0].attrib.get("message", ""))

    def test_t1_ax2_06_placement_flags_and_per_shard_output_dir_isolation(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-fast", tags=["fast"], cores=2)
        self.ox.start_worker(master.listen_addr, name="w-slow", tags=["slow"], cores=2)

        out_dir = self.env.root / "shard_outputs"
        res = self.ox.shard_cli(
            master.listen_addr,
            [
                "--count", "2",
                "--require-tag", "fast",
                "--os", _current_os_tag(),
                "--arch", _current_arch_tag(),
                "--output", "shard_out.txt",
                "--output-dir", str(out_dir),
                "--json",
                "--",
                "sh", "-c", 'echo "from_shard_$OX_SHARD_INDEX" > shard_out.txt',
            ],
        )
        self.assertEqual(res.returncode, 0, f"stderr:\n{res.stderr}\nstdout:\n{res.stdout}")
        shards = _extract_shard_records(json.loads(res.stdout))
        self.assertEqual(len(shards), 2)
        for s in shards:
            self.assertIn("w-fast", _record_worker(s))
            self.assertNotIn("w-slow", _record_worker(s))

        files = sorted(p for p in out_dir.rglob("shard_out.txt") if p.is_file())
        self.assertEqual(len(files), 2, f"Expected 2 isolated shard output files under {out_dir}")
        contents = sorted(p.read_text(encoding="utf-8").strip() for p in files)
        self.assertEqual(contents, ["from_shard_1", "from_shard_2"])

    # --- MESH-3: Remote Master Bridge `--grid-connect` (6 tests) ---

    def test_t1_mesh3_01_grid_connect_attaches_to_external_master_and_submits(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-ext-1", tags=["ext-ok"])

        hub = self.mesh.start_hub(grid_connect=master.listen_addr)
        ws = StdlibWsClient(hub.ws_url)
        self.env.add_cleanup(ws.close)
        ws.register("mesh-caller-1")

        st = ws.grid_status()
        self.assertEqual(st.get("status"), "success", st)
        st_out = st.get("stdout", "")
        self.assertIn("Grid Cluster Status", st_out, st)
        self.assertIn("Active Workers:", st_out, st)
        self.assertGreaterEqual(int(st.get("payload", {}).get("active_workers", 0)), 1, st)

        sub = ws.grid_submit(
            {"script": 'echo "MESH3_EXT_OK WID=$RUSTY_GRID_WORKER_ID"', "wait": True}
        )
        self.assertEqual(sub.get("status"), "success", sub)
        sub_out = sub.get("stdout", "")
        self.assertIn("MESH3_EXT_OK", sub_out)
        wid = sub.get("payload", {}).get("worker_id")
        self.assertTrue(wid, f"Missing worker_id in grid_submit payload: {sub!r}")
        self.assertIn(f"WID={wid}", sub_out)
        self.assertEqual(self._resolve_worker_name(master.listen_addr, wid), "w-ext-1")

    def test_t1_mesh3_02_grid_submit_async_status_and_grid_result(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-ext-async")
        hub = self.mesh.start_hub(grid_connect=master.listen_addr)

        ws = StdlibWsClient(hub.ws_url)
        self.env.add_cleanup(ws.close)
        ws.register("mesh-async-caller")

        marker = self.env.root / "mesh_async_started"
        gate = self.env.root / "mesh_async_gate"
        script = (
            f'touch "{marker}"\n'
            f'while [ ! -f "{gate}" ]; do sleep 0.02; done\n'
            'echo "ASYNC_RESULT_PAYLOAD_OK"\n'
        )
        sub = ws.grid_submit({"script": script, "wait": False})
        self.assertEqual(sub.get("status"), "success", sub)
        task_id = sub.get("payload", {}).get("task_id")
        self.assertTrue(task_id, f"Missing task_id in async grid_submit: {sub!r}")

        _wait_until(marker.is_file, timeout=15.0, desc="async mesh task started on external worker")
        st = ws.grid_status(task_id=task_id)
        self.assertEqual(st.get("status"), "success", st)

        gate.write_text("release\n", encoding="utf-8")
        res = ws.grid_result(task_id=task_id, wait=True)
        self.assertEqual(res.get("status"), "success", res)
        self.assertIn("ASYNC_RESULT_PAYLOAD_OK", res.get("stdout", ""))

    def test_t1_mesh3_03_grid_submit_placement_and_tag_preservation(self) -> None:
        if shutil.which("git") is None:
            fake_git = self.env.fake_bin / "git"
            fake_git.write_text("#!/bin/sh\necho 'git version 2.40.0 (fake-e2e)'\nexit 0\n", encoding="utf-8")
            fake_git.chmod(0o755)

        worker_iso = "fs" if sys.platform == "darwin" else "off"
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-alpha", tags=["alpha"], isolation=worker_iso)
        self.ox.start_worker(master.listen_addr, name="w-beta", tags=["beta"], isolation=worker_iso)
        hub = self.mesh.start_hub(grid_connect=master.listen_addr)

        ws = StdlibWsClient(hub.ws_url)
        self.env.add_cleanup(ws.close)
        ws.register("mesh-place-caller")

        submit_args: dict[str, Any] = {
            "script": 'echo "ON_WORKER=$RUSTY_GRID_WORKER_ID"',
            "tags": ["caller-tag-1", "caller-tag-2"],
            "os": _current_os_tag(),
            "arch": _current_arch_tag(),
            "worker": "w-beta",
            "require_tags": ["beta"],
            "require_tools": ["git"],
            "wait": True,
        }
        if sys.platform == "darwin":
            submit_args["isolation"] = "fs"
        sub = ws.grid_submit(submit_args)
        self.assertEqual(sub.get("status"), "success", sub)
        payload = sub.get("payload", {})
        task_id = payload.get("task_id")
        self.assertTrue(task_id, sub)
        payload_wid = payload.get("worker_id")
        self.assertTrue(payload_wid, f"Missing worker_id in grid_submit payload: {sub!r}")

        workers = self.ox.workers_json(master.listen_addr)
        self.assertEqual(_resolve_worker_name_from_list(workers, payload_wid), "w-beta")
        m_wid = re.search(r"ON_WORKER=([0-9a-fA-F-]+)", sub.get("stdout", ""))
        self.assertIsNotNone(m_wid, f"Missing RUSTY_GRID_WORKER_ID output: {sub!r}")
        assert m_wid is not None
        self.assertEqual(_resolve_worker_name_from_list(workers, m_wid.group(1)), "w-beta")

        st_cli = self.ox.status_cli(master.listen_addr, task_id=task_id, json_out=True)
        self.assertEqual(st_cli.returncode, 0, f"stderr:\n{st_cli.stderr}\nstdout:\n{st_cli.stdout}")
        st_json = json.loads(st_cli.stdout)
        assigned_uuid = st_json.get("assigned_worker")
        self.assertTrue(assigned_uuid, f"status --task-id --json missing assigned_worker: {st_json!r}")
        self.assertEqual(_resolve_worker_name_from_list(workers, assigned_uuid), "w-beta")
        self.assertNotEqual(_resolve_worker_name_from_list(workers, assigned_uuid), "w-alpha")
        if "tags" in st_json or "tags" in payload:
            exposed_tags = set(st_json.get("tags") or payload.get("tags") or [])
            self.assertTrue(
                {"caller-tag-1", "caller-tag-2"}.issubset(exposed_tags),
                f"Caller tags overwritten or dropped: {exposed_tags!r}",
            )

        # Verify caller `tags` are forwarded to Task::validate() (reserved tag must be rejected).
        bad_tag_sub = ws.grid_submit(
            {
                "script": "echo SHOULD_BE_REJECTED",
                "tags": ["speed:999"],
                "worker": "w-beta",
                "wait": True,
            }
        )
        self.assertNotEqual(
            bad_tag_sub.get("status"),
            "success",
            f"Reserved caller tag was ignored instead of forwarded to task.tags: {bad_tag_sub!r}",
        )

    def test_t1_mesh3_04_grid_submit_inline_files_and_expected_outputs(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-files")
        hub = self.mesh.start_hub(grid_connect=master.listen_addr)

        ws = StdlibWsClient(hub.ws_url)
        self.env.add_cleanup(ws.close)
        ws.register("mesh-files-caller")

        raw_bin = b"\x00\x01MESH_BIN\xff\xfe\x80"
        raw_bin_b64 = base64.b64encode(raw_bin).decode("ascii")
        expected_out = b"HARVESTED:" + raw_bin
        expected_sha = hashlib.sha256(expected_out).hexdigest()

        sub = ws.grid_submit(
            {
                "program": "sh",
                "args": ["-c", "./run_check.sh"],
                "files": [
                    {"path": "cfg/hello.txt", "text": "wave3-cfg"},
                    {"path": "data/blob.bin", "data": raw_bin_b64},
                    {
                        "path": "run_check.sh",
                        "text": (
                            "#!/bin/sh\n"
                            "set -e\n"
                            'test "$(cat cfg/hello.txt)" = "wave3-cfg"\n'
                            "mkdir -p out\n"
                            "printf 'HARVESTED:' > out/report.bin\n"
                            "cat data/blob.bin >> out/report.bin\n"
                            "echo STAGED_AND_HARVESTED\n"
                        ),
                        "executable": True,
                    },
                ],
                "outputs": ["out/report.bin"],
                "wait": True,
            }
        )
        self.assertEqual(sub.get("status"), "success", sub)
        self.assertIn("STAGED_AND_HARVESTED", sub.get("stdout", ""))
        artifacts = _extract_mesh_artifacts(sub)
        self.assertEqual(len(artifacts), 1, f"Expected 1 output artifact, got: {sub!r}")
        art = artifacts[0]
        self.assertEqual(art.get("path"), "out/report.bin")
        self.assertEqual(art.get("sha256"), expected_sha)
        self.assertEqual(_decode_artifact_bytes(art), expected_out)

    def test_t1_mesh3_05_grid_cancel_terminates_running_task_on_external_master(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-cancel-ext")
        hub = self.mesh.start_hub(grid_connect=master.listen_addr)

        ws = StdlibWsClient(hub.ws_url)
        self.env.add_cleanup(ws.close)
        ws.register("mesh-cancel-caller")

        marker = self.env.root / "mesh_cancel_started"
        sub = ws.grid_submit(
            {
                "script": f'touch "{marker}"; sleep 60',
                "wait": False,
            }
        )
        self.assertEqual(sub.get("status"), "success", sub)
        task_id = sub["payload"]["task_id"]
        _wait_until(marker.is_file, timeout=15.0, desc="long task started before grid_cancel")

        t0 = time.monotonic()
        cancel_resp = ws.grid_cancel(task_id=task_id)
        self.assertEqual(cancel_resp.get("status"), "success", cancel_resp)

        res = ws.grid_result(task_id=task_id, wait=True, timeout=10.0)
        elapsed = time.monotonic() - t0
        self.assertLess(elapsed, 5.0, f"grid_cancel took too long to settle: {elapsed:.2f}s")
        st_cli = self.ox.status_cli(master.listen_addr, task_id=task_id, json_out=True)
        self.assertEqual(st_cli.returncode, 0, f"stderr:\n{st_cli.stderr}\nstdout:\n{st_cli.stdout}")
        self.assertIn("cancel", st_cli.stdout.lower(), st_cli.stdout)
        self.assertIsNotNone(res)

    def test_t1_mesh3_06_env_var_oxide_grid_connect_and_secret_auth(self) -> None:
        sf = self.ox.generate_secret_file()
        secret_val = sf.read_text(encoding="utf-8").strip()
        master = self.ox.start_master(secret_file=sf)
        self.ox.start_worker(master.listen_addr, name="w-sec-ext", secret_file=sf)

        hub_token = "wave3-mesh-secret-token"
        hub = self.mesh.start_hub(
            token=hub_token,
            env_extra={
                "OXIDE_GRID_CONNECT": master.listen_addr,
                "OXIDESWARM_SECRET": secret_val,
            },
        )
        st_http = http_get_json(
            f"{hub.http_url}/api/status",
            headers={"Authorization": f"Bearer {hub_token}"},
        )
        self.assertIsInstance(st_http, dict)

        ws = StdlibWsClient(hub.ws_url)
        self.env.add_cleanup(ws.close)
        ws.register("mesh-auth-caller", token=hub_token)

        sub = ws.grid_submit({"script": "echo AUTH_ENV_GRID_OK", "wait": True})
        self.assertEqual(sub.get("status"), "success", sub)
        self.assertIn("AUTH_ENV_GRID_OK", sub.get("stdout", ""))

    # --- 3D-2: VRAM & Blender Capability Routing (6 tests) ---

    def test_t1_3d2_01_worker_blender_and_vram_capability_advertisement(self) -> None:
        fake_blender = self.env.root / "fake_blender_dir" / "blender"
        install_fake_blender(fake_blender)

        master = self.ox.start_master()
        self.ox.start_worker(
            master.listen_addr,
            name="w-2gb-nobl",
            gpu_mode="simulate",
            vram_mb=2048,
            blender_mode="none",
        )
        self.ox.start_worker(
            master.listen_addr,
            name="w-8gb-bl",
            gpu_mode="simulate",
            vram_mb=8192,
            blender_mode="enabled",
            blender_path=fake_blender,
        )

        workers = {w["name"]: w for w in self.ox.workers_json(master.listen_addr)}
        self.assertTrue(workers["w-2gb-nobl"].get("has_gpu"))
        self.assertEqual(_worker_vram_mb(workers["w-2gb-nobl"]), 2048)
        self.assertFalse(_worker_has_blender(workers["w-2gb-nobl"]))

        self.assertTrue(workers["w-8gb-bl"].get("has_gpu"))
        self.assertEqual(_worker_vram_mb(workers["w-8gb-bl"]), 8192)
        self.assertTrue(_worker_has_blender(workers["w-8gb-bl"]))

    def test_t1_3d2_02_autorig_preferred_provider_blender_routes_only_to_blender_worker(self) -> None:
        fake_blender = self.env.root / "bl_route" / "blender"
        bl_log = self.env.root / "bl_route.jsonl"
        install_fake_blender(fake_blender, log_path=bl_log)

        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-no-bl", blender_mode="none")
        self.ox.start_worker(
            master.listen_addr,
            name="w-has-bl",
            blender_mode="enabled",
            blender_path=fake_blender,
        )
        self._assert_worker_has_3d_support(master.listen_addr)

        in_mesh = self.env.work / "route_input.glb"
        in_mesh.write_bytes(make_minimal_glb_bytes())

        for i in range(3):
            out_glb = self.env.root / f"routed_{i}.glb"
            res = self.ox.submit_cli(
                master.listen_addr,
                [
                    "--type", "autorig",
                    "--input-mesh", str(in_mesh),
                    "--preferred-provider", "blender",
                    "-o", str(out_glb),
                    "--wait", "--json",
                ],
            )
            _assert_3d_not_disabled(res)
            self.assertEqual(res.returncode, 0, f"stderr:\n{res.stderr}\nstdout:\n{res.stdout}")
            data = json.loads(res.stdout)
            self.assertEqual(data.get("device_name"), "OxideSwarm-AutoRig-Blender")
            self.assertTrue(out_glb.is_file())

        events = _read_jsonl(bl_log)
        self.assertEqual(len(events), 3)

    def test_t1_3d2_03_autorig_blender_waits_for_busy_blender_worker_no_spill(self) -> None:
        fake_blender = self.env.root / "bl_hold" / "blender"
        bl_log = self.env.root / "bl_hold.jsonl"
        install_fake_blender(fake_blender, log_path=bl_log)

        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-idle-nobl", blender_mode="none")
        self.ox.start_worker(
            master.listen_addr,
            name="w-busy-bl",
            blender_mode="enabled",
            blender_path=fake_blender,
            cores=1,
            max_concurrency=1,
        )
        self._assert_worker_has_3d_support(master.listen_addr)

        marker = self.env.root / "bl_blocker_started"
        gate = self.env.root / "bl_blocker_gate"
        blocker = self.ox.submit_cli_popen(
            master.listen_addr,
            [
                "--type", "shell",
                "--worker", "w-busy-bl",
                "--wait",
                "--command", f'touch "{marker}"; while [ ! -f "{gate}" ]; do sleep 0.02; done',
            ],
        )
        _wait_until(marker.is_file, timeout=15.0, desc="w-busy-bl occupied")

        in_mesh = self.env.work / "hold_input.glb"
        in_mesh.write_bytes(make_minimal_glb_bytes())
        out_glb = self.env.root / "held_rig.glb"

        rig_sub = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(in_mesh),
                "--preferred-provider", "blender",
                "--json",
            ],
        )
        _assert_3d_not_disabled(rig_sub)
        self.assertEqual(rig_sub.returncode, 0, f"stderr:\n{rig_sub.stderr}\nstdout:\n{rig_sub.stdout}")
        task_id = json.loads(rig_sub.stdout)["task_id"]

        time.sleep(0.6)
        st = self.ox.status_cli(master.listen_addr, task_id=task_id, json_out=True)
        self.assertEqual(st.returncode, 0, f"stderr:\n{st.stderr}\nstdout:\n{st.stdout}")
        self.assertIn("queued", st.stdout.lower(), f"Task spilled instead of holding back: {st.stdout}")

        gate.write_text("go\n", encoding="utf-8")
        self.assertEqual(blocker.wait(timeout=15.0), 0, blocker.logs())

        res = self.ox.result_cli(master.listen_addr, task_id, wait=True, json_out=True, output_dir=out_glb.parent)
        self.assertEqual(res.returncode, 0, f"stderr:\n{res.stderr}\nstdout:\n{res.stdout}")
        self.assertEqual(json.loads(res.stdout).get("device_name"), "OxideSwarm-AutoRig-Blender")
        self.assertEqual(len(_read_jsonl(bl_log)), 1)

    def test_t1_3d2_04_gen3d_high_quality_skips_2gb_worker_and_lands_on_8gb_worker(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(
            master.listen_addr,
            name="w-2gb",
            gpu_mode="simulate",
            vram_mb=2048,
            blender_mode="none",
        )
        self.ox.start_worker(
            master.listen_addr,
            name="w-8gb",
            gpu_mode="simulate",
            vram_mb=8192,
            blender_mode="none",
        )
        self._assert_worker_has_3d_support(master.listen_addr)

        out_glb = self.env.root / "high_8gb.glb"
        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "3d",
                "--prompt", "armored rover",
                "--quality", "high",
                "-o", str(out_glb),
                "--wait", "--json",
            ],
        )
        _assert_3d_not_disabled(res)
        self.assertEqual(res.returncode, 0, f"stderr:\n{res.stderr}\nstdout:\n{res.stdout}")
        data = json.loads(res.stdout)
        task_id = data.get("task_id")
        self.assertTrue(task_id, f"Missing task_id in submit --wait --json: {data!r}")
        self.assertEqual(data.get("worker_name"), "w-8gb", data)
        self.assertTrue(out_glb.is_file(), "high quality 3D task did not write GLB")
        parse_glb_json(out_glb.read_bytes())

        st = json.loads(self.ox.status_cli(master.listen_addr, task_id=task_id, json_out=True).stdout)
        assigned = st.get("assigned_worker")
        self.assertTrue(assigned, f"Missing assigned_worker in status --task-id --json: {st!r}")
        self.assertEqual(self._resolve_worker_name(master.listen_addr, assigned), "w-8gb")
        self.assertNotEqual(self._resolve_worker_name(master.listen_addr, assigned), "w-2gb")

    def test_t1_3d2_05_gen3d_default_cpu_submit_succeeds_without_gpu_bug3(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(
            master.listen_addr,
            name="w-cpu-only",
            gpu_mode="none",
            blender_mode="none",
        )
        self._assert_worker_has_3d_support(master.listen_addr)

        out_glb = self.env.root / "cpu_default.glb"
        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--prompt", "simple robot",
                "-o", str(out_glb),
                "--wait", "--json",
            ],
        )
        _assert_3d_not_disabled(res)
        self.assertEqual(res.returncode, 0, f"stderr:\n{res.stderr}\nstdout:\n{res.stdout}")
        data = json.loads(res.stdout)
        self.assertEqual(data.get("device_name"), "Procedural-SDF-PureRust")
        self.assertTrue(out_glb.is_file())
        parse_glb_json(out_glb.read_bytes())

    def test_t1_3d2_06_blender_path_shared_between_probe_and_execution(self) -> None:
        custom_blender = self.env.root / "isolated_opt" / "custom-blender-bin"
        bl_log = self.env.root / "custom_bl.jsonl"
        install_fake_blender(custom_blender, log_path=bl_log)

        master = self.ox.start_master()
        self.ox.start_worker(
            master.listen_addr,
            name="w-custom-bl",
            blender_mode="auto",
            blender_path=custom_blender,
        )
        self._assert_worker_has_3d_support(master.listen_addr)

        workers = {w["name"]: w for w in self.ox.workers_json(master.listen_addr)}
        self.assertTrue(
            _worker_has_blender(workers["w-custom-bl"]),
            f"Worker failed to probe custom --blender-path: {workers['w-custom-bl']!r}",
        )

        in_mesh = self.env.work / "shared_path.glb"
        in_mesh.write_bytes(make_minimal_glb_bytes())
        out_glb = self.env.root / "shared_path_out.glb"
        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(in_mesh),
                "--preferred-provider", "blender",
                "-o", str(out_glb),
                "--wait", "--json",
            ],
        )
        _assert_3d_not_disabled(res)
        self.assertEqual(res.returncode, 0, f"stderr:\n{res.stderr}\nstdout:\n{res.stdout}")
        self.assertEqual(len(_read_jsonl(bl_log)), 1)

    # --- 3D-3: Honest AutoRig Options & Timeout/Cancel (6 tests) ---

    def test_t1_3d3_01_autorig_presets_biped_and_quadruped_honest_behavior(self) -> None:
        fake_blender = self.env.root / "bl_presets" / "blender"
        install_fake_blender(fake_blender)

        master = self.ox.start_master()
        self.ox.start_worker(
            master.listen_addr,
            name="w-presets",
            blender_mode="enabled",
            blender_path=fake_blender,
        )
        self._assert_worker_has_3d_support(master.listen_addr)

        in_mesh = self.env.work / "preset_in.glb"
        in_mesh.write_bytes(make_minimal_glb_bytes())

        biped_out = self.env.root / "biped.glb"
        res_bi = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(in_mesh),
                "--rig-preset", "biped",
                "--preferred-provider", "builtin",
                "-o", str(biped_out),
                "--wait", "--json",
            ],
        )
        _assert_3d_not_disabled(res_bi)
        self.assertEqual(res_bi.returncode, 0, f"stderr:\n{res_bi.stderr}\nstdout:\n{res_bi.stdout}")
        bi_rig = extract_glb_rig_summary(biped_out.read_bytes())
        bi_nodes = set(bi_rig["node_names"])
        bi_joints = set(bi_rig["skin_joint_names"])
        self.assertGreater(bi_rig["skin_count"], 0, f"Biped rig missing skins: {bi_rig!r}")
        self.assertGreaterEqual(len(bi_joints), 4, f"Biped rig missing skin joints: {bi_rig!r}")
        self.assertTrue(len(bi_nodes & BIPED_BONE_MARKERS) >= 4, f"Unexpected biped nodes: {bi_nodes!r}")

        for provider in ("builtin", "blender"):
            quad_out = self.env.root / f"quad_{provider}.glb"
            res_q = self.ox.submit_cli(
                master.listen_addr,
                [
                    "--type", "autorig",
                    "--input-mesh", str(in_mesh),
                    "--rig-preset", "quadruped",
                    "--preferred-provider", provider,
                    "-o", str(quad_out),
                    "--wait", "--json",
                ],
            )
            if res_q.returncode == 0:
                q_rig = extract_glb_rig_summary(quad_out.read_bytes())
                q_nodes = set(q_rig["node_names"])
                q_joints = set(q_rig["skin_joint_names"])
                self.assertGreater(
                    q_rig["skin_count"],
                    0,
                    f"quadruped on {provider} emitted unskinned mesh/nodes: {q_rig!r}",
                )
                self.assertNotEqual(
                    q_joints,
                    bi_joints,
                    f"quadruped on {provider} silently returned identical biped skin_joint_names!",
                )
                self.assertNotEqual(
                    q_nodes,
                    bi_nodes,
                    f"quadruped on {provider} silently returned the identical biped skeleton!",
                )
            else:
                msg = f"{res_q.stdout}\n{res_q.stderr}".lower()
                self.assertIn(
                    "quadruped",
                    msg,
                    f"quadruped refusal on {provider} must explicitly name 'quadruped': {msg}",
                )

    def test_t1_3d3_02_autorig_embed_animations_including_real_turnaround_keyframes(self) -> None:
        fake_blender = self.env.root / "bl_anims" / "blender"
        install_fake_blender(fake_blender)

        master = self.ox.start_master()
        self.ox.start_worker(
            master.listen_addr,
            name="w-anims",
            blender_mode="enabled",
            blender_path=fake_blender,
        )
        self._assert_worker_has_3d_support(master.listen_addr)

        in_mesh = self.env.work / "anim_in.glb"
        in_mesh.write_bytes(make_minimal_glb_bytes())

        for provider in ("builtin", "blender"):
            out_glb = self.env.root / f"anim_{provider}.glb"
            res = self.ox.submit_cli(
                master.listen_addr,
                [
                    "--type", "autorig",
                    "--input-mesh", str(in_mesh),
                    "--rig-preset", "biped",
                    "--embed-animations", "idle,walk,turnaround",
                    "--preferred-provider", provider,
                    "-o", str(out_glb),
                    "--wait", "--json",
                ],
            )
            _assert_3d_not_disabled(res)
            self.assertEqual(res.returncode, 0, f"{provider} failed:\n{res.stderr}\n{res.stdout}")
            rig = extract_glb_rig_summary(out_glb.read_bytes())
            anims = rig["animations"]
            for clip in ("idle", "walk", "turnaround"):
                self._assert_valid_animation(anims, clip, f"{provider} autorig", check_nonconstant=True)

    def test_t1_3d3_03_autorig_obj_gltf_glb_magic_bytes_through_blender(self) -> None:
        fake_blender = self.env.root / "bl_magic" / "blender"
        bl_log = self.env.root / "bl_magic.jsonl"
        install_fake_blender(fake_blender, log_path=bl_log)

        master = self.ox.start_master()
        self.ox.start_worker(
            master.listen_addr,
            name="w-magic",
            blender_mode="enabled",
            blender_path=fake_blender,
        )
        self._assert_worker_has_3d_support(master.listen_addr)

        inputs = [
            ("cube.obj", make_minimal_obj_bytes(), "obj"),
            ("tri.gltf", make_minimal_gltf_bytes(), "gltf"),
            ("tri.glb", make_minimal_glb_bytes(), "glb"),
        ]
        for fname, raw_bytes, expected_ext in inputs:
            p = self.env.work / fname
            p.write_bytes(raw_bytes)
            out_glb = self.env.root / f"rigged_{expected_ext}.glb"
            res = self.ox.submit_cli(
                master.listen_addr,
                [
                    "--type", "autorig",
                    "--input-mesh", str(p),
                    "--preferred-provider", "blender",
                    "-o", str(out_glb),
                    "--wait", "--json",
                ],
            )
            _assert_3d_not_disabled(res)
            self.assertEqual(res.returncode, 0, f"{fname} failed:\n{res.stderr}\n{res.stdout}")
            parse_glb_json(out_glb.read_bytes())

        events = _read_jsonl(bl_log)
        self.assertEqual([e.get("input_ext") for e in events], ["obj", "gltf", "glb"])

    def test_t1_3d3_04_autorig_reports_concrete_provider_in_device_name(self) -> None:
        fake_blender = self.env.root / "bl_dev" / "blender"
        install_fake_blender(fake_blender)

        master = self.ox.start_master()
        self.ox.start_worker(
            master.listen_addr,
            name="w-dev",
            blender_mode="enabled",
            blender_path=fake_blender,
        )
        self._assert_worker_has_3d_support(master.listen_addr)

        in_mesh = self.env.work / "dev_in.glb"
        in_mesh.write_bytes(make_minimal_glb_bytes())

        r_builtin = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(in_mesh),
                "--preferred-provider", "builtin",
                "--wait", "--json",
            ],
        )
        _assert_3d_not_disabled(r_builtin)
        self.assertEqual(r_builtin.returncode, 0, f"stderr:\n{r_builtin.stderr}\nstdout:\n{r_builtin.stdout}")
        self.assertEqual(json.loads(r_builtin.stdout).get("device_name"), "OxideSwarm-AutoRig-Builtin")

        r_blender = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(in_mesh),
                "--preferred-provider", "blender",
                "--wait", "--json",
            ],
        )
        self.assertEqual(r_blender.returncode, 0, f"stderr:\n{r_blender.stderr}\nstdout:\n{r_blender.stdout}")
        self.assertEqual(json.loads(r_blender.stdout).get("device_name"), "OxideSwarm-AutoRig-Blender")

    def test_t1_3d3_05_autorig_blender_timeout_settles_within_1s_of_expiry(self) -> None:
        hang_flag = self.env.root / "hang_timeout.flag"
        hang_flag.write_text("1", encoding="utf-8")
        hang_marker = self.env.root / "hang_timeout_started"
        hang_pid_file = self.env.root / "hang_timeout_pids.json"
        fake_blender = self.env.root / "bl_timeout" / "blender"
        install_fake_blender(
            fake_blender,
            hang_flag_file=hang_flag,
            hang_marker_file=hang_marker,
            hang_pid_file=hang_pid_file,
        )

        master = self.ox.start_master()
        self.ox.start_worker(
            master.listen_addr,
            name="w-timeout-bl",
            blender_mode="enabled",
            blender_path=fake_blender,
        )
        self._assert_worker_has_3d_support(master.listen_addr)

        in_mesh = self.env.work / "timeout_in.glb"
        in_mesh.write_bytes(make_minimal_glb_bytes())

        proc = self.ox.submit_cli_popen(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(in_mesh),
                "--preferred-provider", "blender",
                "--timeout-secs", "1",
                "--wait", "--json",
            ],
        )
        _wait_until(hang_marker.is_file, timeout=15.0, desc="fake Blender started hanging")
        t_started = time.monotonic()
        rc = proc.wait(timeout=10.0)
        elapsed_from_start = time.monotonic() - t_started

        self.assertNotEqual(rc, 0, "Timed-out AutoRig task must return non-zero")
        self.assertLessEqual(
            elapsed_from_start,
            3.0,
            f"Timeout took {elapsed_from_start:.2f}s after Blender start (must settle <= 1s after 1s expiry + CLI overhead)",
        )
        pids = json.loads(hang_pid_file.read_text(encoding="utf-8"))
        _wait_until(
            lambda: (not _is_pid_alive(pids["blender_pid"])) and (not _is_pid_alive(pids["grandchild_pid"])),
            timeout=2.0,
            desc="Blender and grandchild sleep processes terminated",
        )

    def test_t1_3d3_06_autorig_blender_cancel_settles_within_1s(self) -> None:
        hang_flag = self.env.root / "hang_cancel.flag"
        hang_flag.write_text("1", encoding="utf-8")
        hang_marker = self.env.root / "hang_cancel_started"
        hang_pid_file = self.env.root / "hang_cancel_pids.json"
        fake_blender = self.env.root / "bl_cancel" / "blender"
        install_fake_blender(
            fake_blender,
            hang_flag_file=hang_flag,
            hang_marker_file=hang_marker,
            hang_pid_file=hang_pid_file,
        )

        master = self.ox.start_master()
        self.ox.start_worker(
            master.listen_addr,
            name="w-cancel-bl",
            blender_mode="enabled",
            blender_path=fake_blender,
        )
        self._assert_worker_has_3d_support(master.listen_addr)

        in_mesh = self.env.work / "cancel_in.glb"
        in_mesh.write_bytes(make_minimal_glb_bytes())

        sub = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(in_mesh),
                "--preferred-provider", "blender",
                "--timeout-secs", "60",
                "--json",
            ],
        )
        _assert_3d_not_disabled(sub)
        self.assertEqual(sub.returncode, 0, f"stderr:\n{sub.stderr}\nstdout:\n{sub.stdout}")
        task_id = json.loads(sub.stdout)["task_id"]

        _wait_until(hang_marker.is_file, timeout=15.0, desc="fake Blender started for cancel")
        t_cancel = time.monotonic()
        c_res = self.ox.cancel_cli(master.listen_addr, task_id)
        self.assertEqual(c_res.returncode, 0, f"stderr:\n{c_res.stderr}\nstdout:\n{c_res.stdout}")

        r_res = self.ox.result_cli(master.listen_addr, task_id, wait=True, json_out=True, timeout=10.0)
        settle_s = time.monotonic() - t_cancel
        self.assertLessEqual(settle_s, 2.5, f"Blender cancel took {settle_s:.2f}s (target <= 1s + two cold CLIs)")
        r_data = json.loads(r_res.stdout)
        self.assertEqual(r_data.get("exit_code"), 130, r_data)
        if isinstance(r_data.get("execution_time_ms"), (int, float)):
            # Worker-side cancel settlement should be bounded well within ~1.5s of task run time.
            self.assertLessEqual(float(r_data["execution_time_ms"]), 15000.0, r_data)

        pids = json.loads(hang_pid_file.read_text(encoding="utf-8"))
        _wait_until(
            lambda: (not _is_pid_alive(pids["blender_pid"])) and (not _is_pid_alive(pids["grandchild_pid"])),
            timeout=2.0,
            desc="Blender and grandchild sleep processes dead after cancel",
        )

        # Recover worker slot cleanly on a subsequent healthy task.
        hang_flag.unlink()
        ok_out = self.env.root / "post_cancel_ok.glb"
        ok_res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(in_mesh),
                "--preferred-provider", "blender",
                "-o", str(ok_out),
                "--wait", "--json",
            ],
        )
        self.assertEqual(ok_res.returncode, 0, f"stderr:\n{ok_res.stderr}\nstdout:\n{ok_res.stdout}")
        self.assertTrue(ok_out.is_file())


# ===========================================================================
# TIER 2: Boundary, Negative & Adversarial Cases (24 tests — 6 per feature)
# ===========================================================================


class Tier2BoundaryCornerTests(Wave3E2ETestBase):
    """Tier 2: Boundary, negative, refusal, and adversarial checks."""

    # --- AX-2 Tier 2 (6 tests) ---

    def test_t2_ax2_01_invalid_count_zero_and_missing_command_rejected(self) -> None:
        master = self.ox.start_master()

        r_zero = self.ox.shard_cli(
            master.listen_addr, ["--count", "0", "--", "echo", "nope"], timeout=10.0
        )
        self.assertNotEqual(r_zero.returncode, 0)
        self.assertLess(r_zero.elapsed_s, 5.0)

        r_nocmd = self.ox.shard_cli(master.listen_addr, ["--count", "2"], timeout=10.0)
        self.assertNotEqual(r_nocmd.returncode, 0)
        self.assertLess(r_nocmd.elapsed_s, 5.0)

    def test_t2_ax2_02_zero_eligible_workers_fails_promptly_no_upload(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-cpu", tags=["cpu"])

        in_dir = self.env.work / "no_upload_src"
        in_dir.mkdir()
        (in_dir / "big.txt").write_text("do-not-upload-" * 200, encoding="utf-8")

        _, m_before = http_get_text(f"http://127.0.0.1:{master.dashboard_port}/metrics")
        blobs_before = parse_prom_metric(m_before, "oxideswarm_blob_store_objects")

        res = self.ox.shard_cli(
            master.listen_addr,
            [
                "--count", "3",
                "--require-tag", "nonexistent-gpu",
                "--input", f"{in_dir}:src",
                "--timeout-secs", "30",
                "--",
                "echo", "nope",
            ],
            timeout=15.0,
        )
        self.assertNotEqual(res.returncode, 0)
        self.assertLess(res.elapsed_s, 5.0, "Zero eligible workers should fail at preflight without hanging")

        _, m_after = http_get_text(f"http://127.0.0.1:{master.dashboard_port}/metrics")
        blobs_after = parse_prom_metric(m_after, "oxideswarm_blob_store_objects")
        self.assertIsNotNone(blobs_before, f"Missing oxideswarm_blob_store_objects before:\n{m_before}")
        self.assertIsNotNone(blobs_after, f"Missing oxideswarm_blob_store_objects after:\n{m_after}")
        self.assertEqual(blobs_after, blobs_before, "Blob store changed despite preflight failure")

    def test_t2_ax2_03_fail_fast_cancels_slow_remaining_shards(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-ff-1", cores=2, max_concurrency=2)
        self.ox.start_worker(master.listen_addr, name="w-ff-2", cores=2, max_concurrency=2)

        pid_dir = self.env.root / "ff_pids"
        pid_dir.mkdir(parents=True, exist_ok=True)
        script = (
            f'if [ "$OX_SHARD_INDEX" = "1" ]; then '
            f'i=0; while [ ! -f "{pid_dir}/shard_2.pid" ] && [ "$i" -lt 100 ]; do sleep 0.05; i=$((i+1)); done; '
            f'exit 9; '
            f'else '
            f'echo $$ > "{pid_dir}/shard_${{OX_SHARD_INDEX}}.pid"; '
            f'exec sleep 30; '
            f'fi'
        )
        res = self.ox.shard_cli(
            master.listen_addr,
            ["--count", "4", "--fail-fast", "--json", "--", "sh", "-c", script],
            timeout=20.0,
        )
        self.assertNotEqual(res.returncode, 0)
        self.assertLess(res.elapsed_s, 10.0, f"--fail-fast took {res.elapsed_s:.2f}s instead of cancelling early")
        payload = json.loads(res.stdout)
        self.assertGreaterEqual(
            int(payload.get("cancelled_shards", 0)),
            1,
            f"Expected --fail-fast JSON to report cancelled_shards >= 1: {payload!r}",
        )
        pid_files = sorted(pid_dir.glob("shard_*.pid"))
        self.assertTrue(pid_files, "Expected slow shards to have started before shard 1 failed")
        for pf in pid_files:
            pid = int(pf.read_text(encoding="utf-8").strip())
            _wait_until(
                lambda p=pid: not _is_pid_alive(p),
                timeout=3.0,
                desc=f"slow shard PID {pid} ({pf.name}) killed by --fail-fast",
            )

    def test_t2_ax2_04_build_once_missing_nextest_tool_and_builder_failure(self) -> None:
        proj_dir = self.env.work / "fail_crate"
        _make_mini_rust_project(proj_dir)

        master = self.ox.start_master()
        # (a) Worker lacks tool:cargo-nextest (scrub any host cargo-nextest from worker PATH)
        clean_path = self.env.path_without_tool("cargo-nextest")
        w_no_nx = self.ox.start_worker(
            master.listen_addr,
            name="w-no-nx",
            env_extra={"__PATH_OVERRIDE__": clean_path},
        )
        r_missing = self.ox.shard_cli(
            master.listen_addr,
            [
                "--count", "2",
                "--build-once",
                "--build-source", str(proj_dir),
            ],
            cwd=proj_dir,
            timeout=15.0,
        )
        self.assertNotEqual(r_missing.returncode, 0)
        self.assertLess(r_missing.elapsed_s, 6.0)
        w_no_nx.proc.stop(sig=signal.SIGTERM, timeout=5.0)
        _wait_until(
            lambda: "state:connected"
            not in (
                next(
                    (w.get("tags", []) for w in self.ox.workers_json(master.listen_addr) if w.get("name") == "w-no-nx"),
                    [],
                )
            ),
            timeout=5.0,
            desc="w-no-nx disconnected before part (b)",
        )

        # (b) Worker has cargo-nextest, but `archive` exits non-zero
        bad_nx_bin = self.env.root / "bad_nextest_bin"
        nx_log = self.env.root / "bad_nx.jsonl"
        install_fake_cargo_nextest(bad_nx_bin, log_path=nx_log, archive_exit_code=2)
        nx_env = {"PATH": str(bad_nx_bin)}
        self.ox.start_worker(master.listen_addr, name="w-bad-nx", env_extra=nx_env)

        r_fail_build = self.ox.shard_cli(
            master.listen_addr,
            [
                "--count", "3",
                "--build-once",
                "--build-source", str(proj_dir),
            ],
            cwd=proj_dir,
            env_extra=nx_env,
            timeout=20.0,
        )
        self.assertNotEqual(r_fail_build.returncode, 0)
        events = _read_jsonl(nx_log)
        self.assertEqual([e["cmd"] for e in events], ["archive"], "Shards must not run when builder fails")

    def test_t2_ax2_05_malformed_and_missing_junit_xml_and_symlink_refused(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-jbad")

        # (a) Malformed XML from shard 1
        bad_xml = self.env.root / "bad_merged.xml"
        malformed_py = (
            "import pathlib\n"
            "p = pathlib.Path('target/nextest/default/junit.xml')\n"
            "p.parent.mkdir(parents=True, exist_ok=True)\n"
            "p.write_text('<testsuites><broken', encoding='utf-8')\n"
        )
        r_bad = self.ox.shard_cli(
            master.listen_addr,
            ["--count", "2", "--junit-out", str(bad_xml), "--", sys.executable, "-c", malformed_py],
        )
        self.assertNotEqual(r_bad.returncode, 0, "Malformed shard JUnit XML must fail merge")

        # (b) All completed shards produce zero JUnit XML files -> merge must fail
        miss_xml = self.env.root / "miss_merged.xml"
        miss_py = "import os\nprint(f'shard {os.environ[\"OX_SHARD_INDEX\"]} ok without junit')\n"
        r_miss = self.ox.shard_cli(
            master.listen_addr,
            ["--count", "2", "--junit-out", str(miss_xml), "--", sys.executable, "-c", miss_py],
        )
        self.assertNotEqual(r_miss.returncode, 0, "All completed shards omitting JUnit XML must fail merge")
        self.assertFalse(miss_xml.exists(), "Zero-JUnit merge failure must not create --junit-out file")

        # (c) Symlinked --junit-out target must be refused
        victim = self.env.root / "junit_victim.txt"
        victim.write_text("DO_NOT_OVERWRITE", encoding="utf-8")
        sym_junit = self.env.root / "sym_junit.xml"
        os.symlink(victim, sym_junit)
        ok_py = (
            "import pathlib\n"
            "p = pathlib.Path('target/nextest/default/junit.xml')\n"
            "p.parent.mkdir(parents=True, exist_ok=True)\n"
            "p.write_text('<testsuites><testsuite name=\"s\" tests=\"1\"><testcase name=\"t\"/></testsuite></testsuites>', encoding='utf-8')\n"
        )
        r_sym = self.ox.shard_cli(
            master.listen_addr,
            ["--count", "1", "--junit-out", str(sym_junit), "--", sys.executable, "-c", ok_py],
        )
        self.assertNotEqual(r_sym.returncode, 0, "Symlinked --junit-out must be refused")
        self.assertEqual(victim.read_text(encoding="utf-8"), "DO_NOT_OVERWRITE")

    def test_t2_ax2_06_unequal_shard_count_gt_slots_and_symlink_output_dir(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-q-1", cores=2, max_concurrency=2)
        self.ox.start_worker(master.listen_addr, name="w-q-2", cores=2, max_concurrency=2)

        res = self.ox.shard_cli(
            master.listen_addr,
            ["--count", "7", "--json", "--", "sh", "-c", 'echo "Q=$OX_SHARD_INDEX"'],
        )
        self.assertEqual(res.returncode, 0, f"stderr:\n{res.stderr}\nstdout:\n{res.stdout}")
        shards = _extract_shard_records(json.loads(res.stdout))
        self.assertEqual(len(shards), 7)

        # Symlinked --output-dir must be refused cleanly.
        real_dir = self.env.root / "real_out_target"
        real_dir.mkdir()
        sym_out_dir = self.env.root / "sym_out_dir"
        os.symlink(real_dir, sym_out_dir)
        r_sym_dir = self.ox.shard_cli(
            master.listen_addr,
            [
                "--count", "1",
                "--output", "out.txt",
                "--output-dir", str(sym_out_dir),
                "--",
                "sh", "-c", "echo hi > out.txt",
            ],
        )
        self.assertNotEqual(r_sym_dir.returncode, 0, "Symlinked --output-dir must be refused")

    # --- MESH-3 Tier 2 (6 tests) ---

    def test_t2_mesh3_01_grid_connect_conflicts_with_grid_master(self) -> None:
        res = self.mesh.run_hub_cli(
            grid_master="127.0.0.1:0",
            grid_connect="127.0.0.1:8088",
        )
        self.assertNotEqual(res.returncode, 0, "Expected --grid-connect + --grid-master to fail")
        combined = f"{res.stdout}\n{res.stderr}".lower()
        self.assertTrue("grid-connect" in combined or "conflict" in combined, combined)

    def test_t2_mesh3_02_hub_local_file_path_and_traversal_refused(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-sec-path")
        hub = self.mesh.start_hub(grid_connect=master.listen_addr)

        canary = self.env.root / "hub_secret_canary.txt"
        canary_val = "TOP_SECRET_HUB_DISK_FILE"
        canary.write_text(canary_val, encoding="utf-8")

        ws = StdlibWsClient(hub.ws_url)
        self.env.add_cleanup(ws.close)
        ws.register("mesh-adv-files")

        # 1) Hub-local file path string passed as file data / non-base64 binary
        r_path = ws.grid_submit(
            {
                "script": "cat staged.txt || true",
                "files": [{"path": "staged.txt", "data": str(canary)}],
                "wait": True,
            }
        )
        self.assertNotEqual(r_path.get("status"), "success", f"Expected refusal of non-base64/local path: {r_path!r}")
        self.assertNotIn(canary_val, json.dumps(r_path))

        # 1b) Bare string-array files: [str(canary)] must also be refused without reading hub disk
        r_bare = ws.grid_submit(
            {
                "script": "cat hub_secret_canary.txt || true",
                "files": [str(canary)],
                "wait": True,
            }
        )
        self.assertNotEqual(r_bare.get("status"), "success", f"Expected refusal of bare string-array files: {r_bare!r}")
        self.assertNotIn(canary_val, json.dumps(r_bare))

        # 2) Path traversal in inline files
        r_trav_in = ws.grid_submit(
            {
                "script": "echo hi",
                "files": [{"path": "../escape.txt", "text": "bad"}],
                "wait": True,
            }
        )
        self.assertNotEqual(r_trav_in.get("status"), "success", r_trav_in)

        # 3) Path traversal in outputs
        r_trav_out = ws.grid_submit(
            {
                "script": "echo hi",
                "outputs": ["../../etc/passwd"],
                "wait": True,
            }
        )
        self.assertNotEqual(r_trav_out.get("status"), "success", r_trav_out)

    def test_t2_mesh3_03_missing_declared_output_fails_task(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-miss-out")
        hub = self.mesh.start_hub(grid_connect=master.listen_addr)

        ws = StdlibWsClient(hub.ws_url)
        self.env.add_cleanup(ws.close)
        ws.register("mesh-miss-out")

        sub = ws.grid_submit(
            {
                "script": "echo ran_without_creating_output",
                "outputs": ["never_created.txt"],
                "wait": True,
            }
        )
        payload = sub.get("payload", {})
        is_failed = (
            sub.get("status") != "success"
            or sub.get("exit_code", 0) != 0
            or payload.get("exit_code", 0) != 0
        )
        self.assertTrue(is_failed, f"Missing declared output must fail task: {sub!r}")
        self.assertIn("never_created.txt", json.dumps(sub))

    def test_t2_mesh3_04_unknown_and_terminal_grid_cancel_and_bad_uuid(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-cancel-edge")
        hub = self.mesh.start_hub(grid_connect=master.listen_addr)

        ws = StdlibWsClient(hub.ws_url)
        self.env.add_cleanup(ws.close)
        ws.register("mesh-edge-cancel")

        # Unknown UUID cancel
        r_unk = ws.grid_cancel(str(uuid.uuid4()))
        if r_unk.get("status") == "success":
            self.assertIs(r_unk.get("payload", {}).get("cancelled"), False, r_unk)

        # Malformed UUID cancel & result
        r_bad = ws.grid_cancel("not-a-uuid")
        self.assertTrue(
            r_bad.get("type") in ("DeliveryNack", "delivery_nack")
            or r_bad.get("status") != "success",
            r_bad,
        )
        r_res_unk = ws.grid_result(str(uuid.uuid4()), wait=False)
        self.assertTrue(
            r_res_unk.get("type") in ("DeliveryNack", "delivery_nack")
            or r_res_unk.get("status") != "success",
            r_res_unk,
        )

        # Terminal task cancel
        done = ws.grid_submit({"script": "echo TERMINAL_DONE", "wait": True})
        self.assertEqual(done.get("status"), "success", done)
        tid = done["payload"]["task_id"]
        r_term = ws.grid_cancel(tid)
        if r_term.get("status") == "success":
            self.assertIs(r_term.get("payload", {}).get("cancelled"), False, r_term)

    def test_t2_mesh3_05_auth_errors_wrong_secret_and_unconfigured_bridge(self) -> None:
        # (a) Unconfigured bridge -> ERR_BRIDGE_NOT_CONFIGURED on all grid_* commands
        hub_bare = self.mesh.start_hub()
        ws = StdlibWsClient(hub_bare.ws_url)
        self.env.add_cleanup(ws.close)
        ws.register("mesh-unconfigured")
        for cmd, args in (
            ("grid_status", {}),
            ("grid_submit", {"script": "echo hi", "wait": False}),
            ("grid_result", {"task_id": str(uuid.uuid4()), "wait": False}),
            ("grid_cancel", {"task_id": str(uuid.uuid4())}),
        ):
            resp = ws.grid_call(cmd, args)
            self.assertIn(resp.get("type"), ("DeliveryNack", "delivery_nack"), f"{cmd}: {resp!r}")
            self.assertEqual(resp.get("error_code"), "ERR_BRIDGE_NOT_CONFIGURED", f"{cmd}: {resp!r}")

        # (b) Wrong secret (valid 64-hex format) against secret-protected external master -> HMAC rejection
        sf = self.ox.generate_secret_file()
        secret_val = sf.read_text(encoding="utf-8").strip()
        master = self.ox.start_master(secret_file=sf)
        wrong_secret = ("f" * 64) if secret_val.startswith("0" * 8) else ("0" * 64)
        try:
            hub_wrong = self.mesh.start_hub(
                grid_connect=master.listen_addr,
                secret=wrong_secret,
                timeout=8.0,
            )
            ws2 = StdlibWsClient(hub_wrong.ws_url)
            self.env.add_cleanup(ws2.close)
            ws2.register("mesh-wrong-secret")
            resp = ws2.grid_status()
            self.assertTrue(
                resp.get("type") in ("DeliveryNack", "delivery_nack")
                or resp.get("status") != "success",
                f"Expected auth failure with wrong secret: {resp!r}",
            )
        except (RuntimeError, TimeoutError):
            # Failing fast on hub startup with wrong secret is also acceptable fail-closed behavior.
            pass

    def test_t2_mesh3_06_external_master_unreachable_or_stopped_fails_cleanly(self) -> None:
        closed_port = reserve_unused_tcp_port()
        dead_addr = f"127.0.0.1:{closed_port}"
        try:
            hub = self.mesh.start_hub(grid_connect=dead_addr, timeout=8.0)
            ws = StdlibWsClient(hub.ws_url)
            self.env.add_cleanup(ws.close)
            ws.register("mesh-dead-master")
            resp = ws.grid_status(timeout=10.0)
            self.assertTrue(
                resp.get("type") in ("DeliveryNack", "delivery_nack")
                or resp.get("status") != "success",
                f"Expected clean error against dead master: {resp!r}",
            )
            self.assertEqual(hub.proc.stop(sig=signal.SIGTERM, timeout=5.0), 0)
        except (RuntimeError, TimeoutError):
            # Refusing startup when external master is unreachable is also bounded & valid.
            pass

    # --- 3D-2 Tier 2 (6 tests) ---

    def test_t2_3d2_01_autorig_blender_preflight_warning_and_fail_unfulfillable(self) -> None:
        master = self.ox.start_master(fail_unfulfillable_tasks=True)
        self.ox.start_worker(master.listen_addr, name="w-nobl-only", blender_mode="none")
        self._assert_worker_has_3d_support(master.listen_addr)

        in_mesh = self.env.work / "unfulfillable.glb"
        in_mesh.write_bytes(make_minimal_glb_bytes())

        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(in_mesh),
                "--preferred-provider", "blender",
                "--wait", "--json",
            ],
            timeout=15.0,
        )
        _assert_3d_not_disabled(res)
        self.assertNotEqual(res.returncode, 0, "Expected unfulfillable Blender AutoRig task to fail")
        self.assertLess(res.elapsed_s, 10.0, "Must fail immediately under --fail-unfulfillable-tasks")
        self.assertIn(
            "no connected worker has blender available",
            res.stderr.lower(),
            f"Expected CLI preflight Blender warning on stderr:\n{res.stderr}",
        )
        combined = f"{res.stdout}\n{res.stderr}".lower()
        self.assertIn("blender", combined)

    def test_t2_3d2_02_autorig_blender_stays_queued_and_recovers_when_blender_joins(self) -> None:
        fake_blender = self.env.root / "bl_late" / "blender"
        install_fake_blender(fake_blender)

        master = self.ox.start_master(fail_unfulfillable_tasks=False)
        self.ox.start_worker(master.listen_addr, name="w-early-nobl", blender_mode="none")
        self._assert_worker_has_3d_support(master.listen_addr)

        in_mesh = self.env.work / "late_join.glb"
        in_mesh.write_bytes(make_minimal_glb_bytes())
        sub = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(in_mesh),
                "--preferred-provider", "blender",
                "--json",
            ],
        )
        _assert_3d_not_disabled(sub)
        self.assertEqual(sub.returncode, 0, f"stderr:\n{sub.stderr}\nstdout:\n{sub.stdout}")
        self.assertIn(
            "no connected worker has blender available",
            sub.stderr.lower(),
            f"Expected CLI preflight Blender warning on stderr:\n{sub.stderr}",
        )
        task_id = json.loads(sub.stdout)["task_id"]

        time.sleep(0.5)
        st = self.ox.status_cli(master.listen_addr, task_id=task_id, json_out=True)
        self.assertIn("queued", st.stdout.lower(), st.stdout)

        self.ox.start_worker(
            master.listen_addr,
            name="w-late-bl",
            blender_mode="enabled",
            blender_path=fake_blender,
        )
        res = self.ox.result_cli(master.listen_addr, task_id, wait=True, json_out=True)
        self.assertEqual(res.returncode, 0, f"stderr:\n{res.stderr}\nstdout:\n{res.stdout}")
        self.assertEqual(json.loads(res.stdout).get("device_name"), "OxideSwarm-AutoRig-Blender")

    def test_t2_3d2_03_stale_blender_path_advertises_no_blender_without_path_fallthrough(self) -> None:
        path_bl_dir = self.env.root / "path_blender_bin"
        install_fake_blender(path_bl_dir / "blender")
        stale_path = self.env.root / "does_not_exist" / "blender"

        master = self.ox.start_master(fail_unfulfillable_tasks=True)
        self.ox.start_worker(
            master.listen_addr,
            name="w-stale-bl",
            blender_mode="auto",
            blender_path=stale_path,
            env_extra={"PATH": str(path_bl_dir)},
        )
        workers = {w["name"]: w for w in self.ox.workers_json(master.listen_addr)}
        self.assertFalse(
            _worker_has_blender(workers["w-stale-bl"]),
            f"Stale --blender-path fell through to PATH blender: {workers['w-stale-bl']!r}",
        )

    def test_t2_3d2_04_hung_blender_probe_times_out_at_startup_and_advertises_no_blender(self) -> None:
        hung_bl = self.env.root / "bl_hung_probe" / "blender"
        probe_pids = self.env.root / "hung_probe_pids.json"
        install_fake_blender(hung_bl, probe_hang_secs=60.0, probe_pid_file=probe_pids)

        master = self.ox.start_master()
        t0 = time.monotonic()
        self.ox.start_worker(
            master.listen_addr,
            name="w-hung-probe",
            blender_mode="auto",
            blender_path=hung_bl,
            timeout=12.0,
        )
        startup_s = time.monotonic() - t0
        self.assertLess(startup_s, 7.0, f"Hung Blender probe blocked worker startup for {startup_s:.2f}s")

        workers = {w["name"]: w for w in self.ox.workers_json(master.listen_addr)}
        self.assertFalse(_worker_has_blender(workers["w-hung-probe"]))
        self.assertTrue(
            probe_pids.is_file(),
            f"Expected Blender --version probe PID file {probe_pids} (probe was never invoked)",
        )
        pids = json.loads(probe_pids.read_text(encoding="utf-8"))
        _wait_until(
            lambda: (not _is_pid_alive(pids["probe_pid"]))
            and (not _is_pid_alive(pids["child_pid"])),
            timeout=2.0,
            desc="hung probe and grandchild sleep processes killed",
        )

    def test_t2_3d2_05_gen3d_high_unfulfillable_on_2gb_and_explicit_vram_override(self) -> None:
        master = self.ox.start_master(fail_unfulfillable_tasks=True)
        self.ox.start_worker(
            master.listen_addr,
            name="w-2gb-only",
            gpu_mode="simulate",
            vram_mb=2048,
            blender_mode="none",
        )
        self._assert_worker_has_3d_support(master.listen_addr)

        r_fail = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "3d",
                "--prompt", "mech",
                "--quality", "high",
                "--wait", "--json",
            ],
            timeout=15.0,
        )
        _assert_3d_not_disabled(r_fail)
        self.assertNotEqual(r_fail.returncode, 0, "high quality 3D task must be unfulfillable on 2 GB only worker")
        self.assertIn("vram", f"{r_fail.stdout}\n{r_fail.stderr}".lower())

        out_ok = self.env.root / "override_vram.glb"
        r_ok = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "3d",
                "--prompt", "mech",
                "--quality", "high",
                "--vram-mb", "1024",
                "-o", str(out_ok),
                "--wait", "--json",
            ],
        )
        self.assertEqual(r_ok.returncode, 0, f"stderr:\n{r_ok.stderr}\nstdout:\n{r_ok.stdout}")
        self.assertTrue(out_ok.is_file())

    def test_t2_3d2_06_shape_memoization_and_multi_slot_vram_anti_overcommit(self) -> None:
        master = self.ox.start_master(fail_unfulfillable_tasks=True)
        self.ox.start_worker(
            master.listen_addr,
            name="w-2slot-8gb",
            cores=2,
            max_concurrency=2,
            gpu_mode="simulate",
            vram_mb=8192,
            blender_mode="none",
        )
        self._assert_worker_has_3d_support(master.listen_addr)

        in_mesh = self.env.work / "shape_sep.glb"
        in_mesh.write_bytes(make_minimal_glb_bytes())

        # Shape separation: failed blender request must not poison builtin on same pass.
        r_bl = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(in_mesh),
                "--preferred-provider", "blender",
                "--wait",
            ],
        )
        _assert_3d_not_disabled(r_bl)
        self.assertNotEqual(r_bl.returncode, 0)

        out_bi = self.env.root / "shape_builtin_ok.glb"
        r_bi = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(in_mesh),
                "--preferred-provider", "builtin",
                "-o", str(out_bi),
                "--wait",
            ],
        )
        self.assertEqual(r_bi.returncode, 0, f"stderr:\n{r_bi.stderr}\nstdout:\n{r_bi.stdout}")

        # Multi-slot VRAM anti-overcommit: two 6144 MB tasks cannot overlap on 8192 MB worker.
        log_file = self.env.root / "vram_overlap.log"
        gate1 = self.env.root / "vram_gate_1"
        gate2 = self.env.root / "vram_gate_2"
        m1 = self.env.root / "vram_start_1"
        m2 = self.env.root / "vram_start_2"

        p1 = self.ox.submit_cli_popen(
            master.listen_addr,
            [
                "--type", "shell",
                "--vram-mb", "6144",
                "--wait",
                "--command",
                f'echo "START_1" >> "{log_file}"; touch "{m1}"; '
                f'while [ ! -f "{gate1}" ]; do sleep 0.02; done; '
                f'echo "END_1" >> "{log_file}"',
            ],
        )
        _wait_until(m1.is_file, timeout=15.0, desc="task 1 (6144 MB) running")

        p2 = self.ox.submit_cli_popen(
            master.listen_addr,
            [
                "--type", "shell",
                "--vram-mb", "6144",
                "--wait",
                "--command",
                f'echo "START_2" >> "{log_file}"; touch "{m2}"; '
                f'while [ ! -f "{gate2}" ]; do sleep 0.02; done; '
                f'echo "END_2" >> "{log_file}"',
            ],
        )
        time.sleep(0.5)
        self.assertFalse(m2.is_file(), "Second 6144 MB task overcommitted 8192 MB VRAM worker!")

        gate1.write_text("go\n", encoding="utf-8")
        self.assertEqual(p1.wait(timeout=15.0), 0, p1.logs())
        _wait_until(m2.is_file, timeout=15.0, desc="task 2 started after task 1 released VRAM")
        gate2.write_text("go\n", encoding="utf-8")
        self.assertEqual(p2.wait(timeout=15.0), 0, p2.logs())

        lines = log_file.read_text(encoding="utf-8").splitlines()
        self.assertEqual(lines, ["START_1", "END_1", "START_2", "END_2"])

    # --- 3D-3 Tier 2 (6 tests) ---

    def test_t2_3d3_01_autorig_unknown_preset_and_generic_honest_refusal(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-p-ref")
        self._assert_worker_has_3d_support(master.listen_addr)

        in_mesh = self.env.work / "preset_bad.glb"
        in_mesh.write_bytes(make_minimal_glb_bytes())

        r_unk = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(in_mesh),
                "--rig-preset", "spider",
                "--wait",
            ],
        )
        _assert_3d_not_disabled(r_unk)
        self.assertNotEqual(r_unk.returncode, 0)
        self.assertIn("spider", f"{r_unk.stdout}\n{r_unk.stderr}".lower())

        # Reference biped skeleton for comparison against generic.
        bi_out = self.env.root / "ref_biped.glb"
        r_bi = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(in_mesh),
                "--rig-preset", "biped",
                "--preferred-provider", "builtin",
                "-o", str(bi_out),
                "--wait",
            ],
        )
        self.assertEqual(r_bi.returncode, 0, f"stderr:\n{r_bi.stderr}\nstdout:\n{r_bi.stdout}")
        bi_rig = extract_glb_rig_summary(bi_out.read_bytes())
        bi_nodes = set(bi_rig["node_names"])
        bi_joints = set(bi_rig["skin_joint_names"])

        gen_out = self.env.root / "check_generic.glb"
        r_gen = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(in_mesh),
                "--rig-preset", "generic",
                "--preferred-provider", "builtin",
                "-o", str(gen_out),
                "--wait",
            ],
        )
        if r_gen.returncode == 0:
            gen_rig = extract_glb_rig_summary(gen_out.read_bytes())
            self.assertGreater(gen_rig["skin_count"], 0, f"generic rig missing skins: {gen_rig!r}")
            self.assertNotEqual(
                set(gen_rig["skin_joint_names"]),
                bi_joints,
                "generic preset silently returned identical biped skin_joint_names!",
            )
            self.assertNotEqual(set(gen_rig["node_names"]), bi_nodes, "generic preset silently returned biped skeleton!")
        else:
            self.assertIn("generic", f"{r_gen.stdout}\n{r_gen.stderr}".lower())

    def test_t2_3d3_02_autorig_unknown_animation_clip_refused(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-anim-ref")
        self._assert_worker_has_3d_support(master.listen_addr)

        in_mesh = self.env.work / "anim_bad.glb"
        in_mesh.write_bytes(make_minimal_glb_bytes())

        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(in_mesh),
                "--embed-animations", "idle,backflip",
                "--wait",
            ],
        )
        _assert_3d_not_disabled(res)
        self.assertNotEqual(res.returncode, 0)
        self.assertIn("backflip", f"{res.stdout}\n{res.stderr}".lower())

    def test_t2_3d3_03_autorig_unknown_provider_refused(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-prov-ref")
        self._assert_worker_has_3d_support(master.listen_addr)

        in_mesh = self.env.work / "prov_bad.glb"
        in_mesh.write_bytes(make_minimal_glb_bytes())

        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(in_mesh),
                "--preferred-provider", "mixamo",
                "--wait",
            ],
        )
        _assert_3d_not_disabled(res)
        self.assertNotEqual(res.returncode, 0)
        self.assertIn("mixamo", f"{res.stdout}\n{res.stderr}".lower())

    def test_t2_3d3_04_explicit_blender_failures_fail_loudly_no_fallback(self) -> None:
        fail_flag = self.env.root / "bl_fail.flag"
        empty_flag = self.env.root / "bl_empty.flag"
        sym_flag = self.env.root / "bl_sym.flag"
        sym_target = self.env.root / "bl_secret_target.txt"
        sym_target.write_text("SECRET", encoding="utf-8")

        fake_blender = self.env.root / "bl_fail_modes" / "blender"
        install_fake_blender(
            fake_blender,
            fail_flag_file=fail_flag,
            empty_out_flag_file=empty_flag,
            symlink_out_target=sym_target,
            symlink_out_flag_file=sym_flag,
        )

        master = self.ox.start_master()
        self.ox.start_worker(
            master.listen_addr,
            name="w-bl-fail",
            blender_mode="enabled",
            blender_path=fake_blender,
        )
        self._assert_worker_has_3d_support(master.listen_addr)

        in_mesh = self.env.work / "bl_fail_in.glb"
        in_mesh.write_bytes(make_minimal_glb_bytes())

        for mode_name, flag in (
            ("nonzero_exit", fail_flag),
            ("missing_output", empty_flag),
            ("symlink_output", sym_flag),
        ):
            flag.write_text("1", encoding="utf-8")
            out_glb = self.env.root / f"should_not_exist_{mode_name}.glb"
            res = self.ox.submit_cli(
                master.listen_addr,
                [
                    "--type", "autorig",
                    "--input-mesh", str(in_mesh),
                    "--preferred-provider", "blender",
                    "-o", str(out_glb),
                    "--wait", "--json",
                ],
            )
            flag.unlink()
            _assert_3d_not_disabled(res)
            self.assertNotEqual(res.returncode, 0, f"Blender {mode_name} fell back silently!")
            self.assertFalse(out_glb.exists(), f"{out_glb} should not exist on {mode_name}")
            self.assertNotIn("OxideSwarm-AutoRig-Builtin", res.stdout + res.stderr)

    def test_t2_3d3_05_magic_bytes_deceptive_filename_and_corrupt_mesh_refused(self) -> None:
        fake_blender = self.env.root / "bl_deceptive" / "blender"
        bl_log = self.env.root / "bl_deceptive.jsonl"
        install_fake_blender(fake_blender, log_path=bl_log)

        master = self.ox.start_master()
        self.ox.start_worker(
            master.listen_addr,
            name="w-deceptive",
            blender_mode="enabled",
            blender_path=fake_blender,
        )
        self._assert_worker_has_3d_support(master.listen_addr)

        # (a) Valid OBJ bytes intentionally stored with a deceptive `.glb` filename!
        deceptive = self.env.work / "actually_obj_bytes.glb"
        deceptive.write_bytes(make_minimal_obj_bytes())
        out_ok = self.env.root / "deceptive_rigged.glb"
        r_ok = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(deceptive),
                "--preferred-provider", "blender",
                "-o", str(out_ok),
                "--wait", "--json",
            ],
        )
        _assert_3d_not_disabled(r_ok)
        self.assertEqual(r_ok.returncode, 0, f"Magic-byte detection failed:\n{r_ok.stderr}\n{r_ok.stdout}")
        events = _read_jsonl(bl_log)
        self.assertEqual(events[-1]["input_ext"], "obj")

        # (b) Empty & corrupt PNG-header inputs refused cleanly
        empty_file = self.env.work / "empty_mesh.glb"
        empty_file.write_bytes(b"")
        corrupt_file = self.env.work / "corrupt_mesh.glb"
        corrupt_file.write_bytes(b"\x89PNG\r\n\x1a\n" + os.urandom(64))

        for bad in (empty_file, corrupt_file):
            r_bad = self.ox.submit_cli(
                master.listen_addr,
                [
                    "--type", "autorig",
                    "--input-mesh", str(bad),
                    "--preferred-provider", "blender",
                    "--wait",
                ],
            )
            self.assertNotEqual(r_bad.returncode, 0, f"Corrupt/empty mesh {bad.name} unexpectedly succeeded")

    def test_t2_3d3_06_inprocess_sdf_or_builtin_cancel_and_secret_stripping(self) -> None:
        sf = self.ox.generate_secret_file()
        fake_blender = self.env.root / "bl_sec_check" / "blender"
        bl_log = self.env.root / "bl_sec_check.jsonl"
        install_fake_blender(fake_blender, log_path=bl_log)

        master = self.ox.start_master(secret_file=sf)
        self.ox.start_worker(
            master.listen_addr,
            name="w-sec-bl",
            secret_file=sf,
            blender_mode="enabled",
            blender_path=fake_blender,
        )
        self._assert_worker_has_3d_support(master.listen_addr, secret_file=sf)

        in_mesh = self.env.work / "sec_in.glb"
        in_mesh.write_bytes(make_minimal_glb_bytes())

        # Verify secret env vars are stripped in Blender child process.
        r_bl = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(in_mesh),
                "--preferred-provider", "blender",
                "--wait", "--json",
            ],
            secret_file=sf,
            env_extra={"OXIDESWARM_SECRET": "leak-canary", "OXIDE_MESH_TOKEN": "leak-tok"},
        )
        _assert_3d_not_disabled(r_bl)
        self.assertEqual(r_bl.returncode, 0, f"stderr:\n{r_bl.stderr}\nstdout:\n{r_bl.stdout}")
        events = _read_jsonl(bl_log)
        self.assertFalse(any(events[-1]["secret_leaks"].values()), f"Secret env leaked: {events[-1]!r}")

        # (a) In-process CPU SDF task with immediate cancel: must submit cleanly and finish or cancel <= 2.5s.
        sub_sdf = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "3d",
                "--engine", "sdf",
                "--prompt", "high detail mechanical turret",
                "--quality", "high",
                "--timeout-secs", "60",
                "--json",
            ],
            secret_file=sf,
        )
        _assert_3d_not_disabled(sub_sdf)
        self.assertEqual(sub_sdf.returncode, 0, f"SDF submit failed:\n{sub_sdf.stderr}\n{sub_sdf.stdout}")
        tid_sdf = json.loads(sub_sdf.stdout)["task_id"]
        t0 = time.monotonic()
        c_sdf = self.ox.cancel_cli(master.listen_addr, tid_sdf, secret_file=sf)
        self.assertEqual(c_sdf.returncode, 0, f"stderr:\n{c_sdf.stderr}\nstdout:\n{c_sdf.stdout}")
        r_sdf = self.ox.result_cli(master.listen_addr, tid_sdf, wait=True, json_out=True, secret_file=sf, timeout=10.0)
        self.assertLessEqual(time.monotonic() - t0, 2.5)
        sdf_data = json.loads(r_sdf.stdout)
        self.assertIn(sdf_data.get("exit_code"), (0, 130, 124), sdf_data)

        # (b) Deterministically held 3D high-VRAM task on CPU-only worker MUST cancel with exit_code == 130.
        sub_q = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "3d",
                "--prompt", "high detail mechanical turret",
                "--quality", "high",
                "--timeout-secs", "60",
                "--json",
            ],
            secret_file=sf,
        )
        _assert_3d_not_disabled(sub_q)
        self.assertEqual(sub_q.returncode, 0, f"Queued 3D submit failed:\n{sub_q.stderr}\n{sub_q.stdout}")
        tid_q = json.loads(sub_q.stdout)["task_id"]
        t1 = time.monotonic()
        c_q = self.ox.cancel_cli(master.listen_addr, tid_q, secret_file=sf)
        self.assertEqual(c_q.returncode, 0, f"stderr:\n{c_q.stderr}\nstdout:\n{c_q.stdout}")
        r_q = self.ox.result_cli(master.listen_addr, tid_q, wait=True, json_out=True, secret_file=sf, timeout=10.0)
        self.assertLessEqual(time.monotonic() - t1, 2.5)
        q_data = json.loads(r_q.stdout)
        self.assertEqual(q_data.get("exit_code"), 130, q_data)


# ===========================================================================
# TIER 3: Pairwise Cross-Feature Interactions (9 tests)
# ===========================================================================


class Tier3CrossFeatureTests(Wave3E2ETestBase):
    """Tier 3: Pairwise cross-feature interactions across AX-2, MESH-3, 3D-2, 3D-3, isolation, durability."""

    def test_t3_01_shard_under_macos_fs_isolation(self) -> None:
        self._require_darwin_for_seatbelt()
        secret_file = self.env.home / "t3_shard_secret.txt"
        secret_val = "wave3-shard-seatbelt-secret"
        secret_file.write_text(secret_val, encoding="utf-8")

        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-iso-1", isolation="fs")
        self.ox.start_worker(master.listen_addr, name="w-iso-2", isolation="fs")

        script = (
            'ln "$T_SECRET" "$RUSTY_GRID_SANDBOX_DIR/leak" 2>/dev/null && echo LINK_OK || echo LINK_DENIED; '
            'echo "SHARD=$OX_SHARD_INDEX/$OX_SHARD_TOTAL"'
        )
        res = self.ox.shard_cli(
            master.listen_addr,
            [
                "--count", "3",
                "--require-isolation", "fs",
                "--env", f"T_SECRET={secret_file}",
                "--json",
                "--",
                "sh", "-c", script,
            ],
        )
        self.assertEqual(res.returncode, 0, f"stderr:\n{res.stderr}\nstdout:\n{res.stdout}")
        shards = _extract_shard_records(json.loads(res.stdout))
        self.assertEqual(len(shards), 3)
        for s in shards:
            out = _record_stdout(s)
            self.assertIn("LINK_DENIED", out)
            self.assertNotIn(secret_val, out)

    def test_t3_02_shard_with_worker_sigkill_failover_and_junit_merge(self) -> None:
        master = self.ox.start_master(
            session_resume_hold_secs=0,
            heartbeat_interval_secs=1,
            heartbeat_timeout_secs=3,
            reaper_interval_secs=1,
        )
        w1 = self.ox.start_worker(master.listen_addr, name="w-die-1", cores=1, max_concurrency=1)

        started_1 = self.env.root / "t3_failover_s1"
        merged_xml = self.env.root / "t3_failover_junit.xml"
        shard_py = (
            "import os, pathlib, time\n"
            "idx = os.environ['OX_SHARD_INDEX']\n"
            f"m = pathlib.Path({repr(str(started_1))})\n"
            "try:\n"
            "    fd = os.open(str(m), os.O_CREAT | os.O_EXCL | os.O_WRONLY)\n"
            "    os.write(fd, idx.encode('utf-8'))\n"
            "    os.close(fd)\n"
            "    time.sleep(30)\n"
            "except FileExistsError:\n"
            "    pass\n"
            "xml = (\n"
            "  '<testsuites tests=\"1\" failures=\"0\" errors=\"0\" skipped=\"0\" time=\"0.01\">'\n"
            "  f'<testsuite name=\"s_{idx}\" tests=\"1\" failures=\"0\" errors=\"0\" skipped=\"0\" time=\"0.01\">'\n"
            "  f'<testcase name=\"tc_{idx}\" classname=\"t3.failover\" time=\"0.01\"/>'\n"
            "  '</testsuite></testsuites>'\n"
            ")\n"
            "p = pathlib.Path(os.environ.get('OX_JUNIT_PATH') or os.environ.get('OXIDESWARM_JUNIT_PATH') or 'target/nextest/default/junit.xml')\n"
            "p.parent.mkdir(parents=True, exist_ok=True)\n"
            "p.write_text(xml, encoding='utf-8')\n"
        )
        proc = self.ox.shard_cli_popen(
            master.listen_addr,
            [
                "--count", "3",
                "--max-retries", "2",
                "--junit-out", str(merged_xml),
                "--",
                sys.executable, "-c", shard_py,
            ],
        )
        _wait_until(started_1.is_file, timeout=15.0, desc="first shard running on w-die-1")
        self.ox.start_worker(master.listen_addr, name="w-survive-2", cores=4, max_concurrency=4)
        w1.proc.stop(sig=signal.SIGKILL, timeout=5.0)
        rc = proc.wait(timeout=30.0)
        self.assertEqual(rc, 0, proc.logs())
        root = ET.fromstring(merged_xml.read_text(encoding="utf-8"))
        self.assertEqual(len(root.findall(".//testcase")), 3)

    def test_t3_03_build_once_with_fail_fast_cancels_remaining_shards(self) -> None:
        nx_bin = self.env.root / "t3_nx_bin"
        nx_log = self.env.root / "t3_nx.jsonl"
        pid_dir = self.env.root / "t3_ff_pids"
        install_fake_cargo_nextest(
            nx_bin,
            log_path=nx_log,
            fail_shards=(1,),
            sleep_secs=0.0,
            sleep_shards={2: 20.0, 3: 20.0},
            pid_dir=pid_dir,
        )

        proj_dir = self.env.work / "t3_ff_crate"
        _make_mini_rust_project(proj_dir)

        master = self.ox.start_master()
        nx_env = {"PATH": str(nx_bin)}
        self.ox.start_worker(master.listen_addr, name="w-boff-1", cores=2, max_concurrency=2, env_extra=nx_env)
        self.ox.start_worker(master.listen_addr, name="w-boff-2", cores=2, max_concurrency=2, env_extra=nx_env)

        res = self.ox.shard_cli(
            master.listen_addr,
            [
                "--count", "3",
                "--build-once",
                "--build-source", str(proj_dir),
                "--fail-fast",
                "--json",
            ],
            cwd=proj_dir,
            env_extra=nx_env,
            timeout=25.0,
        )
        self.assertNotEqual(res.returncode, 0)
        self.assertLess(res.elapsed_s, 12.0, f"build-once + fail-fast hung: {res.elapsed_s:.2f}s")
        payload = json.loads(res.stdout)
        self.assertGreaterEqual(
            int(payload.get("cancelled_shards", 0)),
            1,
            f"Expected --build-once --fail-fast to report cancelled_shards >= 1: {payload!r}",
        )
        for pf in pid_dir.glob("shard_*.pid"):
            pid = int(pf.read_text(encoding="utf-8").strip())
            _wait_until(
                lambda p=pid: not _is_pid_alive(p),
                timeout=3.0,
                desc=f"sleeping build-once shard PID {pid} killed",
            )
        archives = [e for e in _read_jsonl(nx_log) if e.get("cmd") == "archive"]
        self.assertEqual(len(archives), 1)

    def test_t3_04_mesh_grid_connect_with_oxideswarm_durability_restart(self) -> None:
        state_dir = self.env.root / "t3_dur_state"
        m1 = self.ox.start_master(
            name="m1", state_dir=state_dir, heartbeat_interval_secs=1, session_resume_hold_secs=30
        )
        self.ox.start_worker(m1.listen_addr, name="w-dur-1", isolation="off")
        hub = self.mesh.start_hub(grid_connect=m1.listen_addr)

        ws = StdlibWsClient(hub.ws_url)
        self.env.add_cleanup(ws.close)
        ws.register("mesh-dur-caller")

        gate = self.env.root / "t3_dur_gate"
        marker = self.env.root / "t3_dur_started"
        sub = ws.grid_submit(
            {
                "script": f'touch "{marker}"; while [ ! -f "{gate}" ]; do sleep 0.02; done; echo DUR_MESH_OK',
                "wait": False,
            }
        )
        self.assertEqual(sub.get("status"), "success", sub)
        tid = sub["payload"]["task_id"]
        _wait_until(marker.is_file, timeout=15.0, desc="durability task started")
        gate.write_text("go\n", encoding="utf-8")

        _wait_until(
            lambda: "completed" in self.ox.status_cli(m1.listen_addr, task_id=tid).stdout.lower()
            or "succeeded" in self.ox.status_cli(m1.listen_addr, task_id=tid).stdout.lower()
            or "finished" in self.ox.status_cli(m1.listen_addr, task_id=tid).stdout.lower(),
            timeout=10.0,
            desc="task completed before master restart",
        )
        self.assertEqual(m1.proc.stop(sig=signal.SIGTERM, timeout=10.0), 0)

        self.ox.start_master(
            name="m2",
            listen=m1.listen_addr,
            state_dir=state_dir,
            heartbeat_interval_secs=1,
            session_resume_hold_secs=30,
        )
        res = ws.grid_result(task_id=tid, wait=True, timeout=15.0)
        self.assertEqual(res.get("status"), "success", res)
        self.assertIn("DUR_MESH_OK", res.get("stdout", ""))

    def test_t3_05_mesh_grid_connect_to_fs_isolated_worker_and_token_auth(self) -> None:
        self._require_darwin_for_seatbelt()
        sf = self.ox.generate_secret_file()
        master = self.ox.start_master(secret_file=sf)
        self.ox.start_worker(master.listen_addr, name="w-mesh-iso", isolation="fs", secret_file=sf)

        tok = "t3-mesh-iso-token"
        hub = self.mesh.start_hub(token=tok, grid_connect=master.listen_addr, secret_file=sf)
        ws = StdlibWsClient(hub.ws_url)
        self.env.add_cleanup(ws.close)
        ws.register("mesh-iso-caller", token=tok)

        secret_canary = self.env.home / "iso_mesh_secret.txt"
        secret_canary.write_text("DO_NOT_LEAK_SEATBELT", encoding="utf-8")

        sub = ws.grid_submit(
            {
                "program": "sh",
                "args": ["-c", "./check_iso.sh"],
                "isolation": "fs",
                "env": {"T_CANARY": str(secret_canary)},
                "files": [
                    {
                        "path": "check_iso.sh",
                        "text": (
                            "#!/bin/sh\n"
                            'ln "$T_CANARY" "$RUSTY_GRID_SANDBOX_DIR/leak" 2>/dev/null && echo LINK_OK || echo LINK_DENIED\n'
                            'test -z "${OXIDE_MESH_TOKEN:-}" && echo NO_MESH_TOK || echo HAS_MESH_TOK\n'
                            'test -z "${OXIDESWARM_SECRET:-}" && echo NO_OX_SEC || echo HAS_OX_SEC\n'
                            "echo ISO_MESH_DONE > out.txt\n"
                        ),
                        "executable": True,
                    }
                ],
                "outputs": ["out.txt"],
                "wait": True,
            }
        )
        self.assertEqual(sub.get("status"), "success", sub)
        out = sub.get("stdout", "")
        self.assertIn("LINK_DENIED", out)
        self.assertIn("NO_MESH_TOK", out)
        self.assertIn("NO_OX_SEC", out)
        self.assertEqual(len(_extract_mesh_artifacts(sub)), 1)

    def test_t3_06_worker_disconnect_failover_blender_only(self) -> None:
        fake_blender = self.env.root / "bl_failover" / "blender"
        hang_flag = self.env.root / "bl_failover_hang.flag"
        hang_flag.write_text("1", encoding="utf-8")
        hang_marker = self.env.root / "bl_failover_started"
        bl_log = self.env.root / "bl_failover.jsonl"
        install_fake_blender(
            fake_blender,
            log_path=bl_log,
            hang_flag_file=hang_flag,
            hang_marker_file=hang_marker,
        )

        master = self.ox.start_master(
            session_resume_hold_secs=0,
            heartbeat_interval_secs=1,
            heartbeat_timeout_secs=3,
            reaper_interval_secs=1,
        )
        self.ox.start_worker(master.listen_addr, name="w-fo-nobl", blender_mode="none")
        w2 = self.ox.start_worker(
            master.listen_addr,
            name="w-fo-bl-1",
            tags=["bl-1"],
            blender_mode="enabled",
            blender_path=fake_blender,
        )
        self._assert_worker_has_3d_support(master.listen_addr)

        in_mesh = self.env.work / "bl_fo.glb"
        in_mesh.write_bytes(make_minimal_glb_bytes())
        out_glb = self.env.root / "bl_fo_out.glb"

        proc = self.ox.submit_cli_popen(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(in_mesh),
                "--preferred-provider", "blender",
                "--max-retries", "2",
                "-o", str(out_glb),
                "--wait", "--json",
            ],
        )
        _wait_until(hang_marker.is_file, timeout=15.0, desc="AutoRig running on w-fo-bl-1")
        hang_flag.unlink()
        w2.proc.stop(sig=signal.SIGKILL, timeout=5.0)

        # Bring up second Blender worker w3; task must never spill onto w-fo-nobl.
        self.ox.start_worker(
            master.listen_addr,
            name="w-fo-bl-2",
            tags=["bl-2"],
            blender_mode="enabled",
            blender_path=fake_blender,
        )
        rc = proc.wait(timeout=25.0)
        self.assertEqual(rc, 0, proc.logs())
        self.assertTrue(out_glb.is_file())
        data = json.loads("\n".join(proc.stdout_lines))
        self.assertEqual(data.get("device_name"), "OxideSwarm-AutoRig-Blender")

    def test_t3_07_autorig_blender_under_isolation_fs_and_secret_stripping(self) -> None:
        self._require_darwin_for_seatbelt()
        sf = self.ox.generate_secret_file()
        bl_dir = self.env.root / "bl_iso_dir"
        fake_blender = bl_dir / "blender"
        py_bin = "/usr/bin/python3" if Path("/usr/bin/python3").is_file() else None
        install_fake_blender(fake_blender, log_path=None, python_executable=py_bin)

        master = self.ox.start_master(secret_file=sf)
        self.ox.start_worker(
            master.listen_addr,
            name="w-iso-bl",
            isolation="fs",
            isolation_read_dirs=[bl_dir],
            secret_file=sf,
            blender_mode="enabled",
            blender_path=fake_blender,
        )
        self._assert_worker_has_3d_support(master.listen_addr, secret_file=sf)

        in_mesh = self.env.work / "iso_bl.obj"
        in_mesh.write_bytes(make_minimal_obj_bytes())
        out_glb = self.env.root / "iso_bl_out.glb"
        res = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(in_mesh),
                "--preferred-provider", "blender",
                "--embed-animations", "idle,turnaround",
                "-o", str(out_glb),
                "--wait", "--json",
            ],
            secret_file=sf,
        )
        _assert_3d_not_disabled(res)
        self.assertEqual(res.returncode, 0, f"stderr:\n{res.stderr}\nstdout:\n{res.stdout}")
        rig = extract_glb_rig_summary(out_glb.read_bytes())
        self._assert_valid_animation(rig["animations"], "idle", "t3_07 blender fs")
        self._assert_valid_animation(rig["animations"], "turnaround", "t3_07 blender fs")
        probe = rig.get("extras", {}).get("wave3_probe")
        self.assertIsInstance(probe, dict, f"Missing wave3_probe extras in rigged GLB: {rig!r}")
        self.assertEqual(probe.get("input_ext"), "obj")
        leaks = probe.get("secret_leaks")
        self.assertIsInstance(leaks, dict, f"Missing secret_leaks in probe: {probe!r}")
        self.assertEqual(len(leaks), 3, f"Incomplete secret_leaks map: {leaks!r}")
        self.assertFalse(any(leaks.values()), f"Secret env leaked under --isolation fs: {leaks!r}")

    def test_t3_08_mesh3_grid_submit_runs_3d_autorig_on_remote_blender_worker(self) -> None:
        fake_blender = self.env.root / "bl_mesh3" / "blender"
        bl_log = self.env.root / "bl_mesh3.jsonl"
        install_fake_blender(fake_blender, log_path=bl_log)

        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-m3-nobl", blender_mode="none")
        self.ox.start_worker(
            master.listen_addr,
            name="w-m3-bl",
            tags=["rig-node"],
            blender_mode="enabled",
            blender_path=fake_blender,
        )
        self._assert_worker_has_3d_support(master.listen_addr)

        hub = self.mesh.start_hub(grid_connect=master.listen_addr)
        ws = StdlibWsClient(hub.ws_url)
        self.env.add_cleanup(ws.close)
        ws.register("mesh-3d-orchestrator")

        # Stage inline .obj mesh and invoke oxideswarm CLI / verify AutoRig via grid_submit.
        obj_b64 = base64.b64encode(make_minimal_obj_bytes()).decode("ascii")
        sub = ws.grid_submit(
            {
                "program": "sh",
                "args": [
                    "-c",
                    f'"{self.ox.bin}" submit --master "{master.listen_addr}" '
                    '--type autorig --input-mesh mesh.obj --preferred-provider blender '
                    '--embed-animations idle,turnaround -o rigged.glb --wait --json > autorig.json',
                ],
                "require_tags": ["rig-node"],
                "files": [{"path": "mesh.obj", "data": obj_b64}],
                "outputs": ["rigged.glb", "autorig.json"],
                "wait": True,
            }
        )
        self.assertEqual(sub.get("status"), "success", sub)
        artifacts = {a["path"]: _decode_artifact_bytes(a) for a in _extract_mesh_artifacts(sub)}
        self.assertIn("rigged.glb", artifacts)
        rig = extract_glb_rig_summary(artifacts["rigged.glb"])
        self._assert_valid_animation(rig["animations"], "turnaround", "t3_08 mesh3 autorig")
        meta = json.loads(artifacts["autorig.json"].decode("utf-8"))
        self.assertEqual(meta.get("device_name"), "OxideSwarm-AutoRig-Blender")

    def test_t3_09_cancel_and_result_fetch_leaves_no_output_artifact_on_cancelled_3d(self) -> None:
        hang_flag = self.env.root / "t3_cancel_hang.flag"
        hang_flag.write_text("1", encoding="utf-8")
        hang_marker = self.env.root / "t3_cancel_started"
        hang_pids = self.env.root / "t3_cancel_pids.json"
        fake_blender = self.env.root / "bl_t3_cancel" / "blender"
        install_fake_blender(
            fake_blender,
            hang_flag_file=hang_flag,
            hang_marker_file=hang_marker,
            hang_pid_file=hang_pids,
        )

        master = self.ox.start_master()
        self.ox.start_worker(
            master.listen_addr,
            name="w-t3-can",
            blender_mode="enabled",
            blender_path=fake_blender,
        )
        self._assert_worker_has_3d_support(master.listen_addr)

        in_mesh = self.env.work / "t3_can.glb"
        in_mesh.write_bytes(make_minimal_glb_bytes())
        should_not_exist = self.env.root / "cancelled_autorig.glb"

        proc = self.ox.submit_cli_popen(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(in_mesh),
                "--preferred-provider", "blender",
                "--timeout-secs", "60",
                "-o", str(should_not_exist),
                "--wait", "--json",
            ],
        )
        _wait_until(
            lambda: hang_marker.is_file() and hang_pids.is_file(),
            timeout=15.0,
            desc="t3 cancel task started with task_id",
        )
        pids = json.loads(hang_pids.read_text(encoding="utf-8"))
        tid = pids.get("task_id", "")
        self.assertTrue(tid, f"Missing RUSTY_GRID_TASK_ID in hang_pids: {pids!r}")
        c_res = self.ox.cancel_cli(master.listen_addr, tid)
        self.assertEqual(c_res.returncode, 0, f"stderr:\n{c_res.stderr}\nstdout:\n{c_res.stdout}")
        rc = proc.wait(timeout=10.0)
        self.assertNotEqual(rc, 0, proc.logs())
        self.assertFalse(should_not_exist.exists(), "Cancelled 3D task must not write -o output GLB")

        res = self.ox.result_cli(
            master.listen_addr, tid, wait=True, json_out=True, output_dir=should_not_exist.parent
        )
        self.assertEqual(json.loads(res.stdout).get("exit_code"), 130)
        self.assertFalse(should_not_exist.exists(), "Cancelled 3D task must not produce output GLB on result fetch")


# ===========================================================================
# TIER 4: End-to-End Realistic Workload Scenarios (6 tests)
# ===========================================================================


class Tier4WorkloadScenarios(Wave3E2ETestBase):
    """Tier 4: Multi-worker, multi-agent, and heterogeneous end-to-end pipelines."""

    def test_t4_01_distributed_rust_nextest_build_once_and_junit_pipeline(self) -> None:
        nx_bin = self.env.root / "t4_nx_bin"
        nx_log = self.env.root / "t4_nx.jsonl"
        install_fake_cargo_nextest(nx_bin, log_path=nx_log)
        nx_env = {"PATH": str(nx_bin)}

        proj_dir = self.env.work / "ci_rust_project"
        _make_mini_rust_project(proj_dir)

        master = self.ox.start_master()
        for i in range(1, 4):
            self.ox.start_worker(
                master.listen_addr,
                name=f"w-ci-{i}",
                cores=1,
                max_concurrency=1,
                env_extra=nx_env,
            )

        merged_junit = self.env.root / "ci_reports" / "junit.xml"
        res = self.ox.shard_cli(
            master.listen_addr,
            [
                "--count", "3",
                "--build-once",
                "--build-source", str(proj_dir),
                "--junit-out", str(merged_junit),
                "--json",
            ],
            cwd=proj_dir,
            env_extra=nx_env,
            timeout=40.0,
        )
        self.assertEqual(res.returncode, 0, f"stderr:\n{res.stderr}\nstdout:\n{res.stdout}")
        events = _read_jsonl(nx_log)
        archives = [e for e in events if e.get("cmd") == "archive"]
        runs = [e for e in events if e.get("cmd") == "run"]
        self.assertEqual(len(archives), 1)
        self.assertEqual(len(runs), 3)
        self.assertEqual({r["shard_index"] for r in runs}, {1, 2, 3})

        root = ET.fromstring(merged_junit.read_text(encoding="utf-8"))
        self.assertEqual(int(root.attrib.get("tests", 0)), 6)
        self.assertEqual(int(root.attrib.get("failures", 0)), 0)

    def test_t4_02_multi_agent_mesh_grid_connect_offload_and_cancel_pipeline(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-pool-a", tags=["pool-a"])
        self.ox.start_worker(master.listen_addr, name="w-pool-b", tags=["pool-b"])

        hub = self.mesh.start_hub(grid_connect=master.listen_addr)
        inbox_beta = self.env.root / "t4_beta_inbox"
        inbox_beta.mkdir()
        self.mesh.start_node(hub, "agent-beta", inbox_dir=inbox_beta)

        ws_alpha = StdlibWsClient(hub.ws_url)
        self.env.add_cleanup(ws_alpha.close)
        ws_alpha.register("agent-alpha")

        # Step 1: agent-alpha stages inline script + input and runs on pool-b.
        sub = ws_alpha.grid_submit(
            {
                "program": "sh",
                "args": ["-c", "./compute.sh"],
                "require_tags": ["pool-b"],
                "tags": ["pipeline-t4"],
                "files": [
                    {"path": "seed.txt", "text": "42"},
                    {
                        "path": "compute.sh",
                        "text": "#!/bin/sh\nmkdir -p out\necho \"VAL=$(cat seed.txt)\" > out/result.txt\n",
                        "executable": True,
                    },
                ],
                "outputs": ["out/result.txt"],
                "wait": False,
            }
        )
        self.assertEqual(sub.get("status"), "success", sub)
        tid = sub["payload"]["task_id"]

        # Step 2: fetch result via grid_result and relay artifact bytes to agent-beta.
        res = ws_alpha.grid_result(tid, wait=True)
        self.assertEqual(res.get("status"), "success", res)
        arts = _extract_mesh_artifacts(res)
        art_bytes = _decode_artifact_bytes(arts[0])
        self.assertEqual(art_bytes.strip(), b"VAL=42")

        corr = ws_alpha.send_data_payload("agent-beta", art_bytes)
        ack = ws_alpha.recv_until(lambda m: m.get("correlation_id") == corr, timeout=8.0)
        self.assertEqual(ack.get("type"), "DeliveryAck")
        _wait_inbox_for_bytes(inbox_beta, art_bytes, timeout=5.0)

        # Step 3: runaway task cancelled via grid_cancel.
        marker = self.env.root / "t4_runaway_started"
        runaway = ws_alpha.grid_submit(
            {"script": f'touch "{marker}"; sleep 60', "wait": False}
        )
        runaway_id = runaway["payload"]["task_id"]
        _wait_until(marker.is_file, timeout=15.0, desc="runaway mesh task started")
        can = ws_alpha.grid_cancel(runaway_id)
        self.assertEqual(can.get("status"), "success", can)

    def test_t4_03_sharded_run_results_relayed_via_agent_mesh_and_verified(self) -> None:
        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-rel-1", cores=2, max_concurrency=2)
        self.ox.start_worker(master.listen_addr, name="w-rel-2", cores=2, max_concurrency=2)

        hub = self.mesh.start_hub(grid_connect=master.listen_addr)
        reporter_inbox = self.env.root / "reporter_inbox"
        reporter_inbox.mkdir()
        self.mesh.start_node(hub, "reporter-node", inbox_dir=reporter_inbox)

        merged_xml = self.env.root / "t4_sharded_junit.xml"
        shard_py = (
            "import os, pathlib\n"
            "idx = os.environ['OX_SHARD_INDEX']\n"
            "xml = (\n"
            "  '<testsuites tests=\"1\" failures=\"0\" errors=\"0\" skipped=\"0\" time=\"0.01\">'\n"
            "  f'<testsuite name=\"s_{idx}\" tests=\"1\" failures=\"0\" errors=\"0\" skipped=\"0\" time=\"0.01\">'\n"
            "  f'<testcase name=\"t_{idx}\" classname=\"t4.relay\" time=\"0.01\"/>'\n"
            "  '</testsuite></testsuites>'\n"
            ")\n"
            "p = pathlib.Path(os.environ.get('OX_JUNIT_PATH') or os.environ.get('OXIDESWARM_JUNIT_PATH') or 'target/nextest/default/junit.xml')\n"
            "p.parent.mkdir(parents=True, exist_ok=True)\n"
            "p.write_text(xml, encoding='utf-8')\n"
        )
        r_shard = self.ox.shard_cli(
            master.listen_addr,
            ["--count", "3", "--junit-out", str(merged_xml), "--", sys.executable, "-c", shard_py],
        )
        self.assertEqual(r_shard.returncode, 0, f"stderr:\n{r_shard.stderr}\nstdout:\n{r_shard.stdout}")
        xml_bytes = merged_xml.read_bytes()

        deliv = self.mesh.run_data(hub, to_node="reporter-node", payload_file=merged_xml)
        self.assertEqual(deliv.returncode, 0, f"stderr:\n{deliv.stderr}\nstdout:\n{deliv.stdout}")
        delivered_file = _wait_inbox_for_bytes(reporter_inbox, xml_bytes, timeout=5.0)

        ws = StdlibWsClient(hub.ws_url)
        self.env.add_cleanup(ws.close)
        ws.register("reporter-verifier")
        verify_py = (
            "import xml.etree.ElementTree as ET\n"
            "root = ET.parse('junit.xml').getroot()\n"
            "cases = root.findall('.//testcase')\n"
            "assert len(cases) == 3, len(cases)\n"
            "print(f'VERIFIED_CASES={len(cases)}')\n"
        )
        sub = ws.grid_submit(
            {
                "program": sys.executable,
                "args": ["-c", verify_py],
                "files": [
                    {
                        "path": "junit.xml",
                        "data": base64.b64encode(delivered_file.read_bytes()).decode("ascii"),
                    }
                ],
                "wait": True,
            }
        )
        self.assertEqual(sub.get("status"), "success", sub)
        self.assertIn("VERIFIED_CASES=3", sub.get("stdout", ""))

    def test_t4_04_heterogeneous_3d_generation_to_blender_and_builtin_autorig_pipeline(self) -> None:
        fake_blender = self.env.root / "t4_bl" / "blender"
        bl_log = self.env.root / "t4_bl.jsonl"
        install_fake_blender(fake_blender, log_path=bl_log)

        master = self.ox.start_master()
        self.ox.start_worker(master.listen_addr, name="w-cpu", gpu_mode="none", blender_mode="none")
        self.ox.start_worker(
            master.listen_addr, name="w-low-gpu", gpu_mode="simulate", vram_mb=2048, blender_mode="none"
        )
        self.ox.start_worker(
            master.listen_addr,
            name="w-render",
            gpu_mode="simulate",
            vram_mb=8192,
            blender_mode="enabled",
            blender_path=fake_blender,
        )
        self._assert_worker_has_3d_support(master.listen_addr)

        # Step 1: default CPU-compatible 3D generation
        std_glb = self.env.root / "droid_std.glb"
        r1 = self.ox.submit_cli(
            master.listen_addr,
            ["--prompt", "cyber droid", "-o", str(std_glb), "--wait", "--json"],
        )
        _assert_3d_not_disabled(r1)
        self.assertEqual(r1.returncode, 0, f"stderr:\n{r1.stderr}\nstdout:\n{r1.stdout}")
        parse_glb_json(std_glb.read_bytes())

        # Step 2: high quality 3D generation skips 2 GB & CPU workers, lands on w-render (8 GB)
        high_glb = self.env.root / "droid_high.glb"
        r2 = self.ox.submit_cli(
            master.listen_addr,
            ["--type", "3d", "--prompt", "cyber droid", "--quality", "high", "-o", str(high_glb), "--wait", "--json"],
        )
        self.assertEqual(r2.returncode, 0, f"stderr:\n{r2.stderr}\nstdout:\n{r2.stdout}")
        parse_glb_json(high_glb.read_bytes())
        r2_data = json.loads(r2.stdout)
        tid2 = r2_data.get("task_id")
        self.assertTrue(tid2, f"Missing task_id in r2 submit JSON: {r2_data!r}")
        self.assertEqual(r2_data.get("worker_name"), "w-render", r2_data)
        st2 = json.loads(self.ox.status_cli(master.listen_addr, task_id=tid2, json_out=True).stdout)
        assigned2 = st2.get("assigned_worker")
        self.assertTrue(assigned2, f"Missing assigned_worker in status JSON: {st2!r}")
        self.assertEqual(self._resolve_worker_name(master.listen_addr, assigned2), "w-render")

        # Step 3: AutoRig via Blender on w-render with idle,walk,turnaround
        rig_bl = self.env.root / "droid_rigged_bl.glb"
        r3 = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(high_glb),
                "--preferred-provider", "blender",
                "--rig-preset", "biped",
                "--embed-animations", "idle,walk,turnaround",
                "-o", str(rig_bl),
                "--wait", "--json",
            ],
        )
        self.assertEqual(r3.returncode, 0, f"stderr:\n{r3.stderr}\nstdout:\n{r3.stdout}")
        self.assertEqual(json.loads(r3.stdout).get("device_name"), "OxideSwarm-AutoRig-Blender")
        rig3 = extract_glb_rig_summary(rig_bl.read_bytes())
        for clip in ("idle", "walk", "turnaround"):
            self._assert_valid_animation(rig3["animations"], clip, "t4_04 blender autorig")

        # Step 4: AutoRig .obj via Builtin
        obj_mesh = self.env.work / "prop.obj"
        obj_mesh.write_bytes(make_minimal_obj_bytes())
        rig_bi = self.env.root / "prop_rigged_bi.glb"
        r4 = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "autorig",
                "--input-mesh", str(obj_mesh),
                "--preferred-provider", "builtin",
                "--embed-animations", "idle,turnaround",
                "-o", str(rig_bi),
                "--wait", "--json",
            ],
        )
        self.assertEqual(r4.returncode, 0, f"stderr:\n{r4.stderr}\nstdout:\n{r4.stdout}")
        self.assertEqual(json.loads(r4.stdout).get("device_name"), "OxideSwarm-AutoRig-Builtin")
        rig4 = extract_glb_rig_summary(rig_bi.read_bytes())
        self._assert_valid_animation(rig4["animations"], "idle", "t4_04 builtin autorig")
        self._assert_valid_animation(rig4["animations"], "turnaround", "t4_04 builtin autorig")

    def test_t4_05_concurrent_mixed_vram_blender_builtin_and_cancel_stress(self) -> None:
        hang_flag = self.env.root / "t4_stress_hang.flag"
        hang_marker = self.env.root / "t4_stress_hang_started"
        hang_pids = self.env.root / "t4_stress_hang_pids.json"
        bl_hang = self.env.root / "t4_bl_hang" / "blender"
        bl_ok = self.env.root / "t4_bl_ok" / "blender"
        install_fake_blender(
            bl_hang,
            hang_flag_file=hang_flag,
            hang_marker_file=hang_marker,
            hang_pid_file=hang_pids,
        )
        install_fake_blender(bl_ok)

        master = self.ox.start_master()
        self.ox.start_worker(
            master.listen_addr, name="w-2gb-nobl", gpu_mode="simulate", vram_mb=2048, blender_mode="none"
        )
        self.ox.start_worker(
            master.listen_addr,
            name="w-8gb-bl-ok",
            tags=["bl-ok"],
            gpu_mode="simulate",
            vram_mb=8192,
            cores=2,
            max_concurrency=2,
            blender_mode="enabled",
            blender_path=bl_ok,
        )
        self.ox.start_worker(
            master.listen_addr,
            name="w-bl-hang",
            tags=["bl-hang"],
            blender_mode="enabled",
            blender_path=bl_hang,
        )
        self._assert_worker_has_3d_support(master.listen_addr)

        in_mesh = self.env.work / "stress_in.glb"
        in_mesh.write_bytes(make_minimal_glb_bytes())

        # Start hanging Blender task on w-bl-hang to cancel mid-flight.
        hang_flag.write_text("1", encoding="utf-8")
        sub_hang = self.ox.submit_cli(
            master.listen_addr,
            [
                "--type", "autorig",
                "--worker", "w-bl-hang",
                "--input-mesh", str(in_mesh),
                "--preferred-provider", "blender",
                "--timeout-secs", "60",
                "--json",
            ],
        )
        _assert_3d_not_disabled(sub_hang)
        self.assertEqual(sub_hang.returncode, 0, f"stderr:\n{sub_hang.stderr}\nstdout:\n{sub_hang.stdout}")
        hang_tid = json.loads(sub_hang.stdout)["task_id"]

        def run_high_3d(i: int) -> tuple[str, Path, CliResult]:
            out_p = self.env.root / f"stress_3d_{i}.glb"
            res = self.ox.submit_cli(
                master.listen_addr,
                ["--type", "3d", "--prompt", f"drone {i}", "--quality", "high", "-o", str(out_p), "--wait", "--json"],
            )
            return ("high_3d", out_p, res)

        def run_blender_rig(i: int) -> tuple[str, Path, CliResult]:
            out_p = self.env.root / f"stress_bl_{i}.glb"
            res = self.ox.submit_cli(
                master.listen_addr,
                [
                    "--type", "autorig",
                    "--require-tag", "bl-ok",
                    "--input-mesh", str(in_mesh),
                    "--preferred-provider", "blender",
                    "-o", str(out_p),
                    "--wait", "--json",
                ],
            )
            return ("blender_rig", out_p, res)

        def run_builtin_rig(i: int) -> tuple[str, Path, CliResult]:
            out_p = self.env.root / f"stress_bi_{i}.glb"
            res = self.ox.submit_cli(
                master.listen_addr,
                [
                    "--type", "autorig",
                    "--input-mesh", str(in_mesh),
                    "--preferred-provider", "builtin",
                    "-o", str(out_p),
                    "--wait", "--json",
                ],
            )
            return ("builtin_rig", out_p, res)

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            futs = [
                pool.submit(run_high_3d, 0),
                pool.submit(run_high_3d, 1),
                pool.submit(run_blender_rig, 0),
                pool.submit(run_blender_rig, 1),
                pool.submit(run_builtin_rig, 0),
                pool.submit(run_builtin_rig, 1),
            ]
            _wait_until(hang_marker.is_file, timeout=15.0, desc="stress hanging Blender started")
            t_cancel = time.monotonic()
            c_hang = self.ox.cancel_cli(master.listen_addr, hang_tid)
            self.assertEqual(c_hang.returncode, 0, f"stderr:\n{c_hang.stderr}\nstdout:\n{c_hang.stdout}")
            hang_res = self.ox.result_cli(master.listen_addr, hang_tid, wait=True, json_out=True, timeout=10.0)
            self.assertLessEqual(time.monotonic() - t_cancel, 2.5)
            self.assertEqual(json.loads(hang_res.stdout).get("exit_code"), 130)

            for f in futs:
                kind, out_p, r = f.result(timeout=45.0)
                self.assertEqual(r.returncode, 0, f"Concurrent {kind} failed:\n{r.stderr}\n{r.stdout}")
                self.assertTrue(out_p.is_file(), f"Missing output GLB for {kind}: {out_p}")
                parse_glb_json(out_p.read_bytes())
                data = json.loads(r.stdout)
                if kind == "high_3d":
                    self.assertEqual(data.get("worker_name"), "w-8gb-bl-ok", data)
                elif kind == "blender_rig":
                    self.assertEqual(data.get("device_name"), "OxideSwarm-AutoRig-Blender", data)
                elif kind == "builtin_rig":
                    self.assertEqual(data.get("device_name"), "OxideSwarm-AutoRig-Builtin", data)

        pids = json.loads(hang_pids.read_text(encoding="utf-8"))
        _wait_until(
            lambda: (not _is_pid_alive(pids["blender_pid"])) and (not _is_pid_alive(pids["grandchild_pid"])),
            timeout=2.0,
            desc="stress cancelled grandchild processes dead",
        )

    def test_t4_06_remote_agent_mesh_orchestrates_build_and_3d_autorig_pipeline(self) -> None:
        nx_bin = self.env.root / "t4_orch_nx"
        nx_log = self.env.root / "t4_orch_nx.jsonl"
        install_fake_cargo_nextest(nx_bin, log_path=nx_log)
        fake_blender = self.env.root / "t4_orch_bl" / "blender"
        install_fake_blender(fake_blender)

        sf = self.ox.generate_secret_file()
        master = self.ox.start_master(secret_file=sf)
        self.ox.start_worker(
            master.listen_addr,
            name="w-orch-ci",
            tags=["ci"],
            secret_file=sf,
            env_extra={"PATH": str(nx_bin)},
        )
        self.ox.start_worker(
            master.listen_addr,
            name="w-orch-3d",
            tags=["3d-rig"],
            secret_file=sf,
            gpu_mode="simulate",
            vram_mb=8192,
            blender_mode="enabled",
            blender_path=fake_blender,
        )
        self._assert_worker_has_3d_support(master.listen_addr, secret_file=sf)

        proj_dir = self.env.work / "orch_crate"
        _make_mini_rust_project(proj_dir)

        # 1. Run build-once sharded verification via oxideswarm shard.
        r_shard = self.ox.shard_cli(
            master.listen_addr,
            [
                "--count", "2",
                "--build-once",
                "--build-source", str(proj_dir),
                "--require-tag", "ci",
            ],
            cwd=proj_dir,
            secret_file=sf,
            env_extra={"PATH": str(nx_bin)},
        )
        self.assertEqual(r_shard.returncode, 0, f"stderr:\n{r_shard.stderr}\nstdout:\n{r_shard.stdout}")
        self.assertEqual(len([e for e in _read_jsonl(nx_log) if e.get("cmd") == "archive"]), 1)

        # 2. Orchestrate 3D AutoRig + negative traversal check over authenticated agent-mesh --grid-connect.
        hub = self.mesh.start_hub(token="t4-orch-tok", grid_connect=master.listen_addr, secret_file=sf)
        ws = StdlibWsClient(hub.ws_url)
        self.env.add_cleanup(ws.close)
        ws.register("remote-orchestrator", token="t4-orch-tok")

        bad_trav = ws.grid_submit(
            {
                "script": "echo nope",
                "files": [{"path": "../../bad", "text": "x"}],
                "wait": True,
            }
        )
        self.assertNotEqual(bad_trav.get("status"), "success")

        obj_b64 = base64.b64encode(make_minimal_obj_bytes()).decode("ascii")
        rig_sub = ws.grid_submit(
            {
                "program": "sh",
                "args": [
                    "-c",
                    f'"{self.ox.bin}" --secret-file "{sf}" submit --master "{master.listen_addr}" '
                    '--type autorig --input-mesh asset.obj --preferred-provider blender '
                    '--embed-animations idle,walk,turnaround -o rigged.glb --wait --json > meta.json',
                ],
                "require_tags": ["3d-rig"],
                "files": [{"path": "asset.obj", "data": obj_b64}],
                "outputs": ["rigged.glb", "meta.json"],
                "wait": True,
            }
        )
        self.assertEqual(rig_sub.get("status"), "success", rig_sub)
        arts = {a["path"]: _decode_artifact_bytes(a) for a in _extract_mesh_artifacts(rig_sub)}
        rig = extract_glb_rig_summary(arts["rigged.glb"])
        self.assertEqual(set(rig["animations"].keys()), {"idle", "walk", "turnaround"})
        for clip in ("idle", "walk", "turnaround"):
            self._assert_valid_animation(rig["animations"], clip, "t4_06 mesh3 autorig")
        self.assertEqual(json.loads(arts["meta.json"].decode("utf-8")).get("device_name"), "OxideSwarm-AutoRig-Blender")


if __name__ == "__main__":
    unittest.main()
