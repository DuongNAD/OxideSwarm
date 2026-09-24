#!/usr/bin/env python3
"""
sync_network.py — Zero-Dependency Cross-Platform Network Coordination & Ping Probe
Compatible with Python 3.6+ on macOS, Linux, and Windows.
No third-party packages required (pure Python standard library).

Authoritative reference:
  - ORIGINAL_REQUEST.md (update 2026-09-23T05:23:20Z)
  - explorer_protocol_strategy (OX-SYNC-PROTO-STRATEGY-01)
  - Protocol v1.0.0 (protocol_v1.json)
"""

import sys
import os
import json
import time
import socket
import argparse
import platform
import unicodedata
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

DEFAULT_PORT = 8085
SESSION_TTL = 180.0  # seconds

def find_sync_dir(cli_dir=None):
    """Locate Google Drive OxideSwarm_Sync directory, handling Unicode, Windows paths, and symlinks."""
    if cli_dir and os.path.exists(cli_dir):
        return os.path.abspath(cli_dir)

    env_dir = os.environ.get("OXIDESWARM_SYNC_DIR")
    if env_dir and os.path.exists(env_dir):
        return os.path.abspath(env_dir)

    # If running directly inside the sync directory:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if os.path.basename(unicodedata.normalize('NFC', script_dir)) == "OxideSwarm_Sync":
        return script_dir

    user_home = os.path.expanduser("~")
    user_profile = os.environ.get("USERPROFILE", user_home)

    candidates = [
        # macOS FileProvider symlink / target paths
        os.path.join(user_home, "Google Drive", "Drive của tôi", "OxideSwarm_Sync"),
        os.path.join(user_home, "Google Drive", "Drive của tôi", "OxideSwarm_Sync"),
        os.path.join(user_home, "Google Drive", "My Drive", "OxideSwarm_Sync"),
        os.path.join(user_home, "Library", "CloudStorage", "GoogleDrive-duonganhdn2000@gmail.com", "Drive của tôi", "OxideSwarm_Sync"),
        os.path.join(user_home, "Library", "CloudStorage", "GoogleDrive-duonganhdn2000@gmail.com", "Drive của tôi", "OxideSwarm_Sync"),
        os.path.join(user_home, "Library", "CloudStorage", "GoogleDrive-duonganhdn2000@gmail.com", "My Drive", "OxideSwarm_Sync"),
        # Windows mounted drive letters
        "G:\\Drive của tôi\\OxideSwarm_Sync",
        "G:\\Drive của tôi\\OxideSwarm_Sync",
        "G:\\My Drive\\OxideSwarm_Sync",
        os.path.join(user_profile, "Google Drive", "Drive của tôi", "OxideSwarm_Sync"),
        os.path.join(user_profile, "Google Drive", "Drive của tôi", "OxideSwarm_Sync"),
        os.path.join(user_profile, "Google Drive", "My Drive", "OxideSwarm_Sync"),
        # Fallback local testing directories
        os.path.join(user_home, "teamwork_projects", "oxideswarm_sync"),
        os.path.join(user_home, "teamwork_projects", "OxideSwarm_Sync"),
    ]

    for cand in candidates:
        if os.path.exists(cand):
            return os.path.abspath(cand)

    # Dynamic search in user Google Drive root
    for root_candidate in [os.path.join(user_home, "Google Drive"), os.path.join(user_profile, "Google Drive")]:
        if os.path.exists(root_candidate):
            try:
                for entry in os.listdir(root_candidate):
                    norm_entry = unicodedata.normalize('NFC', entry).lower()
                    if "drive" in norm_entry or "toi" in norm_entry or "my" in norm_entry:
                        target = os.path.join(root_candidate, entry, "OxideSwarm_Sync")
                        os.makedirs(target, exist_ok=True)
                        return os.path.abspath(target)
            except Exception:
                pass

    # Default fallback: create primary candidate
    primary = candidates[0]
    os.makedirs(primary, exist_ok=True)
    return os.path.abspath(primary)

def get_platform_name():
    s = platform.system().lower()
    return "macos" if s == "darwin" else ("windows" if s == "windows" else "linux")

def get_platform_short(plat=None):
    p = plat or get_platform_name()
    return "mac" if p == "macos" else ("win" if p == "windows" else "linux")

PLATFORM = get_platform_name()
PLATFORM_SHORT = get_platform_short(PLATFORM)
PEER_PLATFORM = "windows" if PLATFORM == "macos" else "macos"
PEER_SHORT = get_platform_short(PEER_PLATFORM)

def get_local_lan_ip():
    """Detect local outgoing LAN IP via kernel routing query without sending packets."""
    for target in [("8.8.8.8", 80), ("192.168.1.1", 80), ("1.1.1.1", 80)]:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(target)
            ip = s.getsockname()[0]
            s.close()
            if ip and not ip.startswith("127."):
                return ip
        except Exception:
            pass

    # Fallback to hostname
    try:
        ip = socket.gethostbyname(socket.gethostname())
        if ip and not ip.startswith("127."):
            return ip
    except Exception:
        pass

    return "127.0.0.1"

LOCAL_IP = get_local_lan_ip()
NODE_ID = f"{PLATFORM_SHORT}-{LOCAL_IP.replace('.', '-')}"

def atomic_write(filepath, content):
    """Write atomically to prevent partial reads by Google Drive sync."""
    parent = os.path.dirname(filepath)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)

    tmp_path = f"{filepath}.{NODE_ID}.{int(time.time() * 1000)}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, filepath)

def safe_read(filepath, retries=3, delay=0.2):
    """Read file with utf-8-sig to cleanly strip Windows PowerShell BOM and retry locks."""
    for attempt in range(retries):
        try:
            if not os.path.exists(filepath):
                return None
            size = os.path.getsize(filepath)
            if size == 0:
                time.sleep(delay)
                continue
            with open(filepath, "r", encoding="utf-8-sig") as f:
                content = f.read()
            if content.strip():
                return content
        except Exception:
            time.sleep(delay)
    return None

def categorize_socket_error(err):
    """Map BSD errno and Windows WSAError to diagnostic category and remediation hint."""
    err_str = str(err).lower()
    err_no = getattr(err, 'errno', None)
    win_err = getattr(err, 'winerror', None)
    code = err_no or win_err

    if code in (61, 10061, 111) or "refused" in err_str:
        return (
            "CONNECTION_REFUSED",
            "CHECK_LISTENER_BOUND_ALL_INTERFACES",
            "Server actively refused connection on probe port. Verify the server process is running and bound to 0.0.0.0."
        )
    elif code in (60, 10060, 110) or "timed out" in err_str:
        return (
            "CONNECTION_TIMED_OUT",
            "CHECK_FIREWALL_INBOUND_RULE",
            "Connection timed out. Target host firewall is likely blocking inbound TCP packets. Add an inbound firewall allow rule for port."
        )
    elif code in (51, 10051) or ("unreachable" in err_str and "network" in err_str):
        return (
            "NETWORK_UNREACHABLE",
            "VERIFY_LAN_SUBNET",
            "No route to network subnet. Verify both machines are connected to the same LAN / Wi-Fi network."
        )
    elif code in (65, 10065) or "no route to host" in err_str or "host unreachable" in err_str:
        return (
            "HOST_UNREACHABLE",
            "VERIFY_TARGET_IP",
            "Host unreachable (ARP resolution failed). Target device may be offline or using a newly assigned DHCP IP address."
        )
    elif code in (48, 10048, 98) or "address already in use" in err_str:
        return (
            "ADDRESS_IN_USE",
            "KILL_CONFLICTING_PROCESS",
            "Port is occupied by an existing process. Inspect lsof -i :port or netstat -ano and terminate conflicting process."
        )
    else:
        return (
            "GENERIC_SOCKET_ERROR",
            "CHECK_NETWORK_CONNECTIVITY",
            f"Encountered network error: {err}. Verify target IP and local network interfaces."
        )

def emit_error_log(sync_dir, phase, target_url, err):
    """Write structured cross-machine diagnostic error log."""
    category, suggested_action, hint = categorize_socket_error(err)
    timestamp = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())

    content = f"""================================================================================
OXIDESWARM CROSS-MACHINE DIAGNOSTIC ERROR LOG ({PLATFORM.upper()})
================================================================================
STATUS=ERROR
PLATFORM={PLATFORM}
NODE_ID={NODE_ID}
TIMESTAMP_UTC={timestamp}
PHASE={phase}
TARGET_URL={target_url}
ERROR_CATEGORY={category}
ERROR_TYPE={type(err).__name__}
ERROR_DETAILS={str(err)}
SUGGESTED_ACTION={suggested_action}
REMEDIATION_HINT={hint}
================================================================================
"""
    log_name = f"{PLATFORM_SHORT}_error_log.txt"
    log_path = os.path.join(sync_dir, log_name)
    atomic_write(log_path, content)
    print(f"[ERROR-LOG] Emitted diagnostic log ({category}) to: {log_path}")

class PingHttpHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {self.client_address[0]} - {fmt % args}\n"
        sys.stdout.write(line)
        sys.stdout.flush()

    def do_GET(self):
        if self.path in ("/ping", "/", "/status"):
            payload = {
                "status": "PONG",
                "service": "OxideSwarm-Sync-Probe",
                "responder_node": NODE_ID,
                "responder_platform": PLATFORM,
                "responder_host": socket.gethostname(),
                "server_epoch_ms": int(time.time() * 1000),
                "client_ip": self.client_address[0]
            }
            body = json.dumps(payload, indent=2).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)

            server_obj = getattr(self, "server", None)
            if server_obj and hasattr(server_obj, "on_ping_callback") and server_obj.on_ping_callback:
                try:
                    server_obj.on_ping_callback(self.client_address[0], payload)
                except Exception as e:
                    print(f"[WARN] Error in on_ping_callback: {e}")
        else:
            self.send_response(404)
            self.end_headers()

def write_ping_success(sync_dir, client_ip, server_ip, port, rtt_ms, pong_data, client_platform=None, client_node=None):
    """Write authoritative ping_success.txt conforming to verification assertions."""
    filepath = os.path.join(sync_dir, "ping_success.txt")
    timestamp = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    c_plat = client_platform or PLATFORM
    c_node = client_node or NODE_ID
    s_plat = pong_data.get('responder_platform', 'unknown')
    s_node = pong_data.get('responder_node', 'unknown')

    content = f"""================================================================================
OXIDESWARM CROSS-MACHINE PING SUCCESS CONFIRMATION
================================================================================
STATUS: PING SUCCESS
RESULT: SUCCESS
EXIT_CODE: 0
TIMESTAMP_UTC: {timestamp}
CLIENT_PLATFORM: {c_plat}
CLIENT_NODE_ID: {c_node}
CLIENT_IP: {client_ip}
SERVER_PLATFORM: {s_plat}
SERVER_NODE_ID: {s_node}
SERVER_IP: {server_ip}
SERVER_PORT: {port}
ROUND_TRIP_TIME_MS: {rtt_ms:.2f} ms
PACKETS_TRANSMITTED: 1
PACKETS_RECEIVED: 1
PACKET_LOSS: 0.0%
PROTOCOL_VERIFIED: HTTP_PONG_V1
================================================================================
VERIFICATION_RULE_CHECK: PASSED (RTT={rtt_ms:.2f}ms, ECHO=TRUE)
"""
    atomic_write(filepath, content)
    print(f"[OK] Authoritative ping_success.txt created at: {filepath}")

def publish_identity_files(sync_dir, port=DEFAULT_PORT, role_preference="server"):
    """Publish local claim JSON and plain IP text files (both short and full names)."""
    local_ip = get_local_lan_ip()
    now_epoch = int(time.time() * 1000)
    now_utc = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())

    ticket_file_name = f"p2p_ticket_{PLATFORM_SHORT}.txt"
    ticket_path = os.path.join(sync_dir, ticket_file_name)
    has_ticket = os.path.exists(ticket_path)

    claim_data = {
        "node_id": NODE_ID,
        "platform": PLATFORM,
        "hostname": socket.gethostname(),
        "ip": local_ip,
        "port": port,
        "role_preference": role_preference,
        "state": "CLAIMING",
        "session_id": f"ox-sync-{time.strftime('%Y%m%d', time.gmtime())}",
        "epoch_ms": now_epoch,
        "timestamp_utc": now_utc,
        "p2p_ticket_available": has_ticket,
        "p2p_ticket_file": ticket_file_name if has_ticket else None,
        "services": {
            "sync_probe_http": port,
            "oxideswarm_master_tcp": 8088,
            "oxideswarm_web_dashboard": 8080,
            "oxideswarm_udp_discovery": 8089
        }
    }
    claim_json = json.dumps(claim_data, indent=2) + "\n"

    # Dual-write short and full names
    atomic_write(os.path.join(sync_dir, f"claim_{PLATFORM_SHORT}.json"), claim_json)
    atomic_write(os.path.join(sync_dir, f"claim_{PLATFORM}.json"), claim_json)

    atomic_write(os.path.join(sync_dir, f"{PLATFORM_SHORT}_ip.txt"), f"{local_ip}\n")
    atomic_write(os.path.join(sync_dir, f"{PLATFORM}_ip.txt"), f"{local_ip}\n")

    return claim_data

def run_server(sync_dir, port=DEFAULT_PORT, max_pings=None, timeout=None):
    """Run lightweight HTTP Pong server."""
    local_ip = get_local_lan_ip()
    print(f"[SERVER] Starting HTTP Ping Probe on {local_ip}:{port} (Platform: {PLATFORM})")

    # 1. Publish identity files
    publish_identity_files(sync_dir, port, role_preference="server")

    # 2. Publish server_ready.txt
    ready_file = os.path.join(sync_dir, "server_ready.txt")
    ready_content = f"""{local_ip}:{port}
ROLE=SERVER
PLATFORM={PLATFORM}
NODE_ID={NODE_ID}
IP={local_ip}
PORT={port}
PROTOCOL=HTTP_PONG_V1
P2P_TICKET_FILE=p2p_ticket_{PLATFORM_SHORT}.txt
SESSION_ID=ox-sync-{time.strftime('%Y%m%d', time.gmtime())}
TIMESTAMP_UTC={time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}
EPOCH_MS={int(time.time() * 1000)}
"""
    atomic_write(ready_file, ready_content)
    print(f"[SERVER] Ready marker published: {ready_file}")

    # 3. Bind and serve
    try:
        server = HTTPServer(("0.0.0.0", port), PingHttpHandler)
    except Exception as err:
        emit_error_log(sync_dir, "SERVER_BIND", f"0.0.0.0:{port}", err)
        raise

    ping_count = [0]
    def on_ping(client_ip, data):
        ping_count[0] += 1
        print(f"[SERVER] Handled ping #{ping_count[0]} from client {client_ip}")

    server.on_ping_callback = on_ping
    print(f"[SERVER] Listening on 0.0.0.0:{port}... (Ctrl+C to stop)")

    t_start = time.time()
    try:
        while True:
            server.handle_request()
            if max_pings and ping_count[0] >= max_pings:
                print(f"[SERVER] Reached max pings ({max_pings}). Shutting down server.")
                break
            if timeout and (time.time() - t_start) >= timeout:
                print(f"[SERVER] Timeout of {timeout}s reached. Shutting down server.")
                break
    except KeyboardInterrupt:
        print("\n[SERVER] Stopped by user.")
    finally:
        server.server_close()

def discover_peer_endpoint(sync_dir, default_port=DEFAULT_PORT):
    """Scan sync directory (root and nodes/) to discover peer IP and port."""
    # 1. Check server_ready.txt
    ready_file = os.path.join(sync_dir, "server_ready.txt")
    ready_data = safe_read(ready_file)
    if ready_data:
        first_line = ready_data.strip().splitlines()[0]
        if ":" in first_line:
            host, p_str = first_line.split(":")[:2]
            try:
                return host, int(p_str)
            except ValueError:
                return host, default_port

    # 2. Check peer IP text files
    for ip_cand in [f"{PEER_SHORT}_ip.txt", f"{PEER_PLATFORM}_ip.txt"]:
        cand_path = os.path.join(sync_dir, ip_cand)
        ip_data = safe_read(cand_path)
        if ip_data and ip_data.strip():
            return ip_data.strip().splitlines()[0], default_port

    # 3. Check peer claim JSON
    for claim_cand in [f"claim_{PEER_SHORT}.json", f"claim_{PEER_PLATFORM}.json"]:
        claim_path = os.path.join(sync_dir, claim_cand)
        raw_json = safe_read(claim_path)
        if raw_json:
            try:
                p_data = json.loads(raw_json)
                if p_data.get("ip"):
                    return p_data["ip"], p_data.get("port", default_port)
            except Exception:
                pass

    # 4. Check nodes/ hierarchy (protocol_v1.json format)
    nodes_dir = os.path.join(sync_dir, "nodes")
    if os.path.exists(nodes_dir):
        for entry in os.listdir(nodes_dir):
            if entry != NODE_ID and PLATFORM_SHORT not in entry:
                ann_path = os.path.join(nodes_dir, entry, "announce.json")
                raw_ann = safe_read(ann_path)
                if raw_ann:
                    try:
                        a_data = json.loads(raw_ann)
                        ips = a_data.get("lan_ipv4", [])
                        if ips:
                            return ips[0], a_data.get("advertised_port", default_port)
                    except Exception:
                        pass

    return None, None

def run_client(sync_dir, target_ip=None, port=DEFAULT_PORT, max_retries=15, retry_delay=2.0):
    """Run client ping dial loop to target server."""
    local_ip = get_local_lan_ip()

    if not target_ip:
        disc_ip, disc_port = discover_peer_endpoint(sync_dir, port)
        if disc_ip:
            target_ip = disc_ip
            port = disc_port or port

    if not target_ip:
        target_ip = "127.0.0.1"
        print(f"[CLIENT] Warning: Peer IP not found in sync directory. Falling back to {target_ip}")

    url = f"http://{target_ip}:{port}/ping"
    print(f"[CLIENT] Target Server: {url} | Local Source: {local_ip} ({PLATFORM})")

    # 1. Publish client_ready.txt
    ready_file = os.path.join(sync_dir, "client_ready.txt")
    ready_content = f"""{local_ip}
ROLE=CLIENT
PLATFORM={PLATFORM}
NODE_ID={NODE_ID}
IP={local_ip}
TARGET_SERVER={target_ip}:{port}
TIMESTAMP_UTC={time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}
EPOCH_MS={int(time.time() * 1000)}
"""
    atomic_write(ready_file, ready_content)

    # 2. Ping dial loop
    last_err = None
    for attempt in range(1, max_retries + 1):
        print(f"[CLIENT] [{attempt}/{max_retries}] Pinging {url}...")
        t0 = time.time()
        try:
            req = Request(url, headers={"User-Agent": f"OxideSwarmSync/{NODE_ID}"})
            with urlopen(req, timeout=4.0) as resp:
                rtt_ms = (time.time() - t0) * 1000.0
                if resp.status == 200:
                    payload = json.loads(resp.read().decode("utf-8"))
                    if payload.get("status") == "PONG":
                        print(f"[CLIENT] [SUCCESS] Received PONG in {rtt_ms:.2f}ms from node {payload.get('responder_node')}")
                        write_ping_success(sync_dir, local_ip, target_ip, port, rtt_ms, payload)
                        return True
        except Exception as err:
            last_err = err
            print(f"[CLIENT] Attempt {attempt} failed: {err}")
            time.sleep(retry_delay)

    # Dial failed: write error log
    emit_error_log(sync_dir, "CLIENT_CONNECT", url, last_err)
    return False

def arbitrate_roles(sync_dir, port=DEFAULT_PORT, observe_seconds=3.0):
    """Execute race-condition-free role negotiation."""
    server_ready_file = os.path.join(sync_dir, "server_ready.txt")
    local_ip = get_local_lan_ip()

    # Rule 1: Existing established server within TTL
    if os.path.exists(server_ready_file):
        try:
            mtime = os.path.getmtime(server_ready_file)
            if (time.time() - mtime) < SESSION_TTL:
                ready_data = safe_read(server_ready_file)
                if ready_data:
                    first_line = ready_data.strip().splitlines()[0]
                    if ":" in first_line:
                        srv_ip, srv_p = first_line.split(":")[:2]
                        if srv_ip != local_ip:
                            print(f"[AUTO] Active server found at {first_line} (<180s old). Electing role: CLIENT")
                            return "client", srv_ip, int(srv_p)
        except Exception:
            pass

    # Rule 2: Publish local claim and observe
    my_claim = publish_identity_files(sync_dir, port, role_preference="server")
    print(f"[AUTO] Published local claim ({PLATFORM}). Observing sync dir for {observe_seconds}s...")
    time.sleep(observe_seconds)

    # Check for peer claim (root or nodes/)
    peer_claim = None
    for claim_cand in [f"claim_{PEER_SHORT}.json", f"claim_{PEER_PLATFORM}.json"]:
        cand_path = os.path.join(sync_dir, claim_cand)
        raw_json = safe_read(cand_path)
        if raw_json:
            try:
                peer_claim = json.loads(raw_json)
                break
            except Exception:
                pass

    if not peer_claim:
        nodes_dir = os.path.join(sync_dir, "nodes")
        if os.path.exists(nodes_dir):
            for entry in os.listdir(nodes_dir):
                if entry != NODE_ID and PLATFORM_SHORT not in entry:
                    ann_path = os.path.join(nodes_dir, entry, "announce.json")
                    raw_ann = safe_read(ann_path)
                    if raw_ann:
                        try:
                            a_data = json.loads(raw_ann)
                            peer_claim = {
                                "node_id": a_data.get("node_id", entry),
                                "platform": a_data.get("platform", PEER_PLATFORM),
                                "ip": a_data.get("lan_ipv4", ["127.0.0.1"])[0],
                                "port": a_data.get("advertised_port", port),
                                "epoch_ms": int(time.time() * 1000)
                            }
                            break
                        except Exception:
                            pass

    if not peer_claim:
        print(f"[AUTO] No peer claim detected from {PEER_PLATFORM}. Electing role: SERVER")
        return "server", "0.0.0.0", port

    # Rule 3: Temporal Seniority (|T_local - T_peer| >= 1000ms)
    t_local = my_claim["epoch_ms"]
    t_peer = peer_claim.get("epoch_ms", t_local)

    if abs(t_local - t_peer) >= 1000:
        if t_local < t_peer:
            print(f"[AUTO] Local claim is older (senior). Electing role: SERVER")
            return "server", "0.0.0.0", port
        else:
            print(f"[AUTO] Peer claim is older (senior). Electing role: CLIENT")
            return "client", peer_claim.get("ip", "127.0.0.1"), peer_claim.get("port", port)

    # Rule 4: Static Lexicographical Precedence ("macos" < "windows")
    if PLATFORM == "macos":
        print("[AUTO] Concurrent claims within 1000ms: Static precedence elects macOS as SERVER")
        return "server", "0.0.0.0", port
    else:
        print("[AUTO] Concurrent claims within 1000ms: Static precedence elects Windows as CLIENT")
        return "client", peer_claim.get("ip", "127.0.0.1"), peer_claim.get("port", port)

def scan_sync_directory(sync_dir):
    """Scan and parse all files in the sync directory and subdirectories."""
    print("=" * 60)
    print(f"  OxideSwarm Sync Directory Inspection")
    print(f"  Target Directory: {sync_dir}")
    print("=" * 60)

    if not os.path.exists(sync_dir):
        print(f"[ERROR] Directory does not exist: {sync_dir}")
        return

    entries = sorted(os.listdir(sync_dir))
    if not entries:
        print("[INFO] Directory is currently empty (0 files).")
        return

    print(f"[INFO] Found {len(entries)} top-level item(s):")
    for name in entries:
        full_path = os.path.join(sync_dir, name)
        if os.path.isdir(full_path):
            sub_items = os.listdir(full_path)
            print(f"  [DIR]  {name:<24} ({len(sub_items)} items inside)")
            for sub in sorted(sub_items):
                sub_full = os.path.join(full_path, sub)
                if os.path.isdir(sub_full):
                    deep_items = os.listdir(sub_full)
                    print(f"         └─ [DIR] {sub:<20} ({len(deep_items)} items)")
                    for deep in sorted(deep_items):
                        deep_full = os.path.join(sub_full, deep)
                        d_size = os.path.getsize(deep_full)
                        print(f"                └─ {deep:<20} ({d_size:>5} bytes)")
                        if deep.endswith(".json"):
                            try:
                                with open(deep_full, "r", encoding="utf-8-sig") as df:
                                    d_data = json.load(df)
                                if "lan_ipv4" in d_data:
                                    print(f"                   -> IP: {d_data.get('lan_ipv4')}, Role: {d_data.get('preferred_role')}")
                                elif "role" in d_data:
                                    print(f"                   -> Role: {d_data.get('role')}, State: {d_data.get('current_state')}")
                            except Exception:
                                pass
                else:
                    s_size = os.path.getsize(sub_full)
                    print(f"         └─ {sub:<24} ({s_size:>5} bytes)")
                    if sub.endswith(".log"):
                        try:
                            with open(sub_full, "r", encoding="utf-8-sig") as sf:
                                lines = sf.readlines()
                            if lines:
                                print(f"            -> Last line: {lines[-1].strip()[:80]}")
                        except Exception:
                            pass
        else:
            size = os.path.getsize(full_path)
            mtime = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(os.path.getmtime(full_path)))
            print(f"  [FILE] {name:<24} ({size:>5} bytes, modified {mtime})")

            # Parse special files
            if name.endswith(".json"):
                try:
                    with open(full_path, "r", encoding="utf-8-sig") as f:
                        data = json.load(f)
                    if "ip" in data:
                        print(f"         -> Node: {data.get('node_id')}, IP: {data.get('ip')}, Port: {data.get('port')}, Role: {data.get('role_preference')}")
                except Exception as e:
                    print(f"         -> [WARN] JSON parse error: {e}")
            elif name.endswith(".txt"):
                try:
                    with open(full_path, "r", encoding="utf-8-sig") as f:
                        preview = f.readline().strip()
                    print(f"         -> Line 1: {preview[:80]}")
                except Exception as e:
                    print(f"         -> [WARN] Read error: {e}")

    print("=" * 60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OxideSwarm Cross-Machine Network Protocol")
    parser.add_argument("--mode", choices=["auto", "server", "client", "scan", "publish"], default="auto",
                        help="Execution mode (default: auto)")
    parser.add_argument("--ip", default="", help="Target IP for client mode")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Port (default: 8085)")
    parser.add_argument("--sync-dir", default="", help="Custom Google Drive sync directory")
    parser.add_argument("--max-pings", type=int, default=None, help="Server mode: exit after N pings")
    parser.add_argument("--timeout", type=float, default=None, help="Server mode timeout in seconds")
    parser.add_argument("--observe-time", type=float, default=3.0, help="Auto mode: seconds to observe peer claim")
    args = parser.parse_args()

    sync_path = find_sync_dir(args.sync_dir)

    print("============================================================")
    print(f"  OxideSwarm Cross-Machine Network Protocol")
    print(f"  Sync Directory : {sync_path}")
    print(f"  Platform       : {PLATFORM} ({NODE_ID})")
    print(f"  Local LAN IP   : {LOCAL_IP}")
    print("============================================================")

    if args.mode == "scan":
        scan_sync_directory(sync_path)
    elif args.mode == "publish":
        claim = publish_identity_files(sync_path, args.port)
        print(f"[OK] Identity files published successfully for {PLATFORM} ({LOCAL_IP})")
    elif args.mode == "server":
        run_server(sync_path, args.port, max_pings=args.max_pings, timeout=args.timeout)
    elif args.mode == "client":
        ok = run_client(sync_path, target_ip=args.ip, port=args.port)
        sys.exit(0 if ok else 1)
    else:  # auto
        role, target_ip, port = arbitrate_roles(sync_path, port=args.port, observe_seconds=args.observe_time)
        if role == "server":
            run_server(sync_path, port, max_pings=args.max_pings, timeout=args.timeout)
        else:
            ok = run_client(sync_path, target_ip=target_ip, port=port)
            sys.exit(0 if ok else 1)
