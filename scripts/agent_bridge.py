#!/usr/bin/env python3
"""
agent_bridge.py — Cross-Machine Coding Agent Collaboration & Diagnostic CLI for OxideSwarm
Zero third-party dependencies (Python 3.7+ standard library only).
Works identically on macOS (agent-mac), Windows (agent-win), and Linux over a shared cloud folder (Google Drive).

Commands:
  python3 scripts/agent_bridge.py heartbeat [--repo /path/to/OxideSwarm] [--hub-dir /path/to/OxideSwarm_Agent_Hub]
  python3 scripts/agent_bridge.py diagnose  [--repo /path/to/OxideSwarm] [--hub-dir /path/to/OxideSwarm_Agent_Hub]
  python3 scripts/agent_bridge.py inbox     [--ack <FILENAME_OR_ALL>]    [--hub-dir /path/to/OxideSwarm_Agent_Hub]
  python3 scripts/agent_bridge.py send      --title "..." --body "..." [--priority normal|high|critical]
  python3 scripts/agent_bridge.py incident  --id INC-001 --title "..." --symptom "..." --root-cause "..." [--fix "..."] [--status OPEN|FIXED|VERIFIED|CLOSED]
"""

import argparse
import datetime
import glob
import hashlib
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import time
import unicodedata


def detect_platform():
    s = platform.system().lower()
    if s == "darwin":
        return "mac", "macos", "win"
    elif s == "windows":
        return "win", "windows", "mac"
    else:
        return "linux", "linux", "win"


SELF_SHORT, SELF_OS, PEER_SHORT = detect_platform()


def find_hub_dir(cli_dir=None):
    """Locate OxideSwarm_Agent_Hub on Google Drive across macOS and Windows."""
    if cli_dir and os.path.exists(cli_dir):
        return os.path.abspath(cli_dir)

    env_dir = os.environ.get("OXIDESWARM_AGENT_HUB")
    if env_dir and os.path.exists(env_dir):
        return os.path.abspath(env_dir)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    if os.path.basename(unicodedata.normalize("NFC", script_dir)) == "OxideSwarm_Agent_Hub":
        return script_dir

    user_home = os.path.expanduser("~")
    user_profile = os.environ.get("USERPROFILE", user_home)

    candidates = []
    # macOS CloudStorage Google Drive mounts
    for account_dir in sorted(glob.glob(os.path.join(user_home, "Library", "CloudStorage", "GoogleDrive-*"))):
        try:
            for entry in os.listdir(account_dir):
                full_entry = os.path.join(account_dir, entry)
                if os.path.isdir(full_entry) and not entry.startswith("."):
                    candidates.append(os.path.join(full_entry, "OxideSwarm_Agent_Hub"))
        except Exception:
            pass

    # Windows & standard Google Drive paths
    for drive_letter in ("G:\\", "H:\\", "D:\\"):
        if os.path.exists(drive_letter):
            try:
                for entry in os.listdir(drive_letter):
                    full_entry = os.path.join(drive_letter, entry)
                    if os.path.isdir(full_entry):
                        candidates.append(os.path.join(full_entry, "OxideSwarm_Agent_Hub"))
            except Exception:
                pass
            candidates.append(os.path.join(drive_letter, "My Drive", "OxideSwarm_Agent_Hub"))

    for base in (user_home, user_profile):
        gdrive = os.path.join(base, "Google Drive")
        if os.path.exists(gdrive):
            try:
                for entry in os.listdir(gdrive):
                    full_entry = os.path.join(gdrive, entry)
                    if os.path.isdir(full_entry):
                        candidates.append(os.path.join(full_entry, "OxideSwarm_Agent_Hub"))
            except Exception:
                pass
            candidates.append(os.path.join(gdrive, "My Drive", "OxideSwarm_Agent_Hub"))

    for cand in candidates:
        if os.path.exists(cand):
            return os.path.abspath(cand)

    # Fallback: create in first discovered Google Drive root if available, else ~/.oxideswarm/agent_hub
    if candidates:
        target = os.path.abspath(candidates[0])
    else:
        target = os.path.join(user_home, ".oxideswarm", "agent_hub")
    os.makedirs(target, exist_ok=True)
    return target


def utc_now_iso():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def atomic_write(filepath, content):
    parent = os.path.dirname(filepath)
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp_path = f"{filepath}.{SELF_SHORT}.{os.getpid()}.{int(time.time() * 1000)}.tmp"
    with open(tmp_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, filepath)


def safe_read_json(filepath):
    if not os.path.exists(filepath):
        return None
    for _ in range(3):
        try:
            with open(filepath, "r", encoding="utf-8-sig") as f:
                return json.load(f)
        except Exception:
            time.sleep(0.2)
    return None


def get_lan_ip():
    for target in [("8.8.8.8", 80), ("192.168.1.1", 80), ("1.1.1.1", 80)]:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(0.5)
            s.connect(target)
            ip = s.getsockname()[0]
            s.close()
            if ip and not ip.startswith("127."):
                return ip
        except Exception:
            pass
    try:
        ip = socket.gethostbyname(socket.gethostname())
        if ip and not ip.startswith("127."):
            return ip
    except Exception:
        pass
    return "127.0.0.1"


def check_tcp_port(host, port, timeout=1.0):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def find_repo_dir(explicit_repo=None):
    if explicit_repo and os.path.exists(os.path.join(explicit_repo, "Cargo.toml")):
        return os.path.abspath(explicit_repo)
    script_parent = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if os.path.exists(os.path.join(script_parent, "Cargo.toml")):
        return script_parent
    cwd = os.getcwd()
    if os.path.exists(os.path.join(cwd, "Cargo.toml")):
        return os.path.abspath(cwd)
    home = os.path.expanduser("~")
    candidates = [
        os.path.join(home, "Documents", "antigravity", "magical-mendel"),
        os.path.join(home, "Documents", "OxideSwarm"),
        os.path.join(home, "OxideSwarm"),
        "E:\\teamwork_projects\\OxideSwarm",
        "C:\\OxideSwarm",
        os.path.join(home, "source", "repos", "OxideSwarm"),
    ]
    for c in candidates:
        if os.path.exists(os.path.join(c, "Cargo.toml")):
            return os.path.abspath(c)
    return None


def get_git_info(repo_dir):
    if not repo_dir:
        return {"found": False}
    try:
        head = subprocess.check_output(
            ["git", "rev-parse", "--short=12", "HEAD"], cwd=repo_dir, stderr=subprocess.DEVNULL, text=True
        ).strip()
        branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=repo_dir, stderr=subprocess.DEVNULL, text=True
        ).strip()
        status_out = subprocess.check_output(
            ["git", "status", "--porcelain"], cwd=repo_dir, stderr=subprocess.DEVNULL, text=True
        ).strip()
        dirty_files = [line.strip() for line in status_out.splitlines() if line.strip()]
        return {
            "found": True,
            "repo_path": repo_dir,
            "git_head": head,
            "git_branch": branch,
            "git_dirty": len(dirty_files) > 0,
            "dirty_count": len(dirty_files),
        }
    except Exception as e:
        return {"found": False, "error": str(e)}


def get_oxideswarm_home_info():
    home = os.path.expanduser("~")
    ox_home = os.environ.get("OXIDESWARM_HOME", os.path.join(home, ".oxideswarm"))
    secret_path = os.path.join(ox_home, "cluster.secret")
    config_path = os.path.join(ox_home, "rusty-grid.toml")

    secret_exists = os.path.exists(secret_path)
    secret_sha256_12 = None
    if secret_exists:
        try:
            with open(secret_path, "rb") as f:
                raw = f.read().strip()
            secret_sha256_12 = hashlib.sha256(raw).hexdigest()[:12]
        except Exception:
            pass

    stale_p2p_ticket_in_config = False
    if os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8-sig") as f:
                for line in f:
                    stripped = line.strip()
                    if stripped and not stripped.startswith("#") and "p2p_ticket" in stripped:
                        stale_p2p_ticket_in_config = True
                        break
        except Exception:
            pass

    return {
        "oxideswarm_home": ox_home,
        "secret_exists": secret_exists,
        "secret_sha256_12": secret_sha256_12,
        "config_exists": os.path.exists(config_path),
        "stale_p2p_ticket_in_config": stale_p2p_ticket_in_config,
    }


def get_binaries_info(repo_dir):
    if not repo_dir:
        return {}
    ext = ".exe" if SELF_SHORT == "win" else ""
    rel_dir = os.path.join(repo_dir, "target", "release")
    bins = {}
    for name in ["oxideswarm", "rusty-grid", "ox-mode", "agent-mesh"]:
        p = os.path.join(rel_dir, name + ext)
        bins[name] = os.path.exists(p)
    return bins


def collect_state(repo_dir=None):
    repo = find_repo_dir(repo_dir)
    lan_ip = get_lan_ip()
    return {
        "agent_id": f"agent-{SELF_SHORT}",
        "hostname": socket.gethostname(),
        "platform": SELF_OS,
        "arch": platform.machine(),
        "updated_at_utc": utc_now_iso(),
        "lan_ip": lan_ip,
        "local_ports": {
            "master_cluster_8088_listening": check_tcp_port("127.0.0.1", 8088, 0.3),
            "dashboard_8080_listening": check_tcp_port("127.0.0.1", 8080, 0.3),
        },
        "git": get_git_info(repo),
        "security_and_config": get_oxideswarm_home_info(),
        "release_binaries": get_binaries_info(repo),
    }


def cmd_heartbeat(args):
    hub_dir = find_hub_dir(args.hub_dir)
    state = collect_state(args.repo)
    out_path = os.path.join(hub_dir, "nodes", f"{SELF_SHORT}_state.json")
    atomic_write(out_path, json.dumps(state, indent=2, ensure_ascii=False) + "\n")
    print(f"[OK] Heartbeat written to {out_path}")
    print(json.dumps(state, indent=2, ensure_ascii=False))


def cmd_diagnose(args):
    hub_dir = find_hub_dir(args.hub_dir)
    my_state = collect_state(args.repo)
    my_path = os.path.join(hub_dir, "nodes", f"{SELF_SHORT}_state.json")
    atomic_write(my_path, json.dumps(my_state, indent=2, ensure_ascii=False) + "\n")

    peer_path = os.path.join(hub_dir, "nodes", f"{PEER_SHORT}_state.json")
    peer_state = safe_read_json(peer_path)

    print("=" * 78)
    print(f"  OxideSwarm Multi-Agent Diagnostic Report (Self: agent-{SELF_SHORT}, Peer: agent-{PEER_SHORT})")
    print(f"  Hub Directory : {hub_dir}")
    print("=" * 78)

    issues = []
    recommendations = []

    sec = my_state.get("security_and_config", {})
    if sec.get("stale_p2p_ticket_in_config"):
        issues.append(
            f"[LOCAL CRITICAL] {sec.get('oxideswarm_home')}/rusty-grid.toml contains an active `p2p_ticket`."
        )
        recommendations.append(
            "Remove or comment out `p2p_ticket` in rusty-grid.toml so LAN workers can register via --master."
        )

    if not sec.get("secret_exists"):
        issues.append("[LOCAL WARN] ~/.oxideswarm/cluster.secret does not exist yet on this machine.")
        recommendations.append("Start Master (`./switch_role.sh master`) or copy cluster.secret from the Master machine.")

    if not peer_state:
        print(f"  - Peer State ({PEER_SHORT}_state.json): NOT FOUND YET")
        print(f"    -> Waiting for `agent-{PEER_SHORT}` to run: python scripts/agent_bridge.py heartbeat")
    else:
        print(f"  - Peer Hostname : {peer_state.get('hostname')} ({peer_state.get('platform')}/{peer_state.get('arch')})")
        print(f"  - Peer LAN IP   : {peer_state.get('lan_ip')} (Updated: {peer_state.get('updated_at_utc')})")

        my_git = my_state.get("git", {})
        peer_git = peer_state.get("git", {})
        if my_git.get("found") and peer_git.get("found"):
            if my_git.get("git_head") != peer_git.get("git_head"):
                issues.append(
                    f"[GIT DRIFT] Commit mismatch: agent-{SELF_SHORT}={my_git.get('git_head')} vs agent-{PEER_SHORT}={peer_git.get('git_head')}"
                )
                recommendations.append("Run `git pull` and rebuild binaries on the outdated node before testing.")
            else:
                print(f"  - Git Commit    : MATCH ({my_git.get('git_head')} on branch {my_git.get('git_branch')})")

        my_fp = sec.get("secret_sha256_12")
        peer_fp = peer_state.get("security_and_config", {}).get("secret_sha256_12")
        if my_fp and peer_fp:
            if my_fp != peer_fp:
                issues.append(
                    f"[AUTH MISMATCH] cluster.secret SHA-256 fingerprint differs! ({SELF_SHORT}={my_fp} vs {PEER_SHORT}={peer_fp})"
                )
                recommendations.append(
                    "Copy `~/.oxideswarm/cluster.secret` from the active Master to the Worker machine (never via Google Drive)."
                )
            else:
                print(f"  - Secret Hash   : MATCH (sha256[:12] = {my_fp})")

        peer_ip = peer_state.get("lan_ip")
        my_master_up = my_state.get("local_ports", {}).get("master_cluster_8088_listening")
        peer_master_up = peer_state.get("local_ports", {}).get("master_cluster_8088_listening")
        if my_master_up and peer_master_up:
            issues.append(
                f"[SPLIT-BRAIN MASTER] Both agent-{SELF_SHORT} ({my_state.get('lan_ip')}:8088) and agent-{PEER_SHORT} ({peer_ip}:8088) are running a Master coordinator simultaneously."
            )
            recommendations.append(
                f"Keep Master on one node and switch the other node to Worker (e.g. `windows\\switch_role.cmd worker {my_state.get('lan_ip')}:8088` or `./switch_role.sh worker {peer_ip}:8088`)."
            )
        if peer_ip and peer_ip != "127.0.0.1" and peer_master_up:
            reachable = check_tcp_port(peer_ip, 8088, timeout=1.5)
            if reachable:
                print(f"  - Peer Master   : REACHABLE at {peer_ip}:8088 (TCP OK)")
            else:
                issues.append(
                    f"[NETWORK BLOCKED] Peer Master is listening on {peer_ip}:8088, but TCP connection from {SELF_SHORT} failed."
                )
                recommendations.append(
                    f"Check Firewall on {peer_state.get('hostname')} (allow TCP 8088) or use `--p2p` mode."
                )

    inbox_dir = os.path.join(hub_dir, "mailbox", f"to_{SELF_SHORT}")
    inbox_files = sorted(glob.glob(os.path.join(inbox_dir, "*.md")))
    print(f"  - Unread Inbox  : {len(inbox_files)} message(s) in mailbox/to_{SELF_SHORT}/")
    for f in inbox_files:
        print(f"      * {os.path.basename(f)}")

    inc_files = sorted(glob.glob(os.path.join(hub_dir, "incidents", "*.md")))
    print(f"  - Incident Logs : {len(inc_files)} file(s) in incidents/")

    print("-" * 78)
    if not issues:
        print("  STATUS: HEALTHY (No configuration, git drift, or auth conflicts detected)")
    else:
        print("  DETECTED ISSUES:")
        for idx, iss in enumerate(issues, 1):
            print(f"    {idx}. {iss}")
        print("\n  RECOMMENDED ACTIONS:")
        for idx, rec in enumerate(recommendations, 1):
            print(f"    {idx}. {rec}")
    print("=" * 78)


def cmd_send(args):
    hub_dir = find_hub_dir(args.hub_dir)
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
    safe_slug = "".join(c if c.isalnum() else "_" for c in args.title.lower()).strip("_")[:40]
    filename = f"MSG_{ts}_from_{SELF_SHORT}_{safe_slug}.md"
    target_dir = os.path.join(hub_dir, "mailbox", f"to_{PEER_SHORT}")
    filepath = os.path.join(target_dir, filename)

    content = f"""# [{args.priority.upper()}] {args.title}

- **From**: `agent-{SELF_SHORT}` (`{socket.gethostname()}`)
- **To**: `agent-{PEER_SHORT}`
- **Timestamp (UTC)**: `{utc_now_iso()}`
- **Priority**: `{args.priority}`

---

## Message / Coordination Request

{args.body}
"""
    atomic_write(filepath, content)
    print(f"[OK] Sent message to agent-{PEER_SHORT}: {filepath}")


def cmd_inbox(args):
    hub_dir = find_hub_dir(args.hub_dir)
    inbox_dir = os.path.join(hub_dir, "mailbox", f"to_{SELF_SHORT}")
    archive_dir = os.path.join(hub_dir, "mailbox", "archive")
    os.makedirs(inbox_dir, exist_ok=True)
    os.makedirs(archive_dir, exist_ok=True)

    if args.ack:
        if args.ack.lower() == "all":
            targets = sorted(glob.glob(os.path.join(inbox_dir, "*.md")))
        else:
            targets = [os.path.join(inbox_dir, args.ack)]
        for t in targets:
            if os.path.exists(t):
                dest = os.path.join(archive_dir, os.path.basename(t))
                shutil.move(t, dest)
                print(f"[OK] Archived: {os.path.basename(t)} -> mailbox/archive/")
        return

    files = sorted(glob.glob(os.path.join(inbox_dir, "*.md")))
    if not files:
        print(f"[INBOX] No unread messages in mailbox/to_{SELF_SHORT}/")
        return

    for f in files:
        print("=" * 78)
        print(f"FILE: {os.path.basename(f)}")
        print("-" * 78)
        with open(f, "r", encoding="utf-8-sig") as fp:
            print(fp.read().strip())
        print("=" * 78)


def cmd_incident(args):
    hub_dir = find_hub_dir(args.hub_dir)
    inc_dir = os.path.join(hub_dir, "incidents")
    os.makedirs(inc_dir, exist_ok=True)
    safe_slug = "".join(c if c.isalnum() else "_" for c in args.title.lower()).strip("_")[:40]
    filename = f"{args.id}_{safe_slug}.md"
    filepath = os.path.join(inc_dir, filename)

    content = f"""# [{args.status.upper()}] {args.id}: {args.title}

- **Reported By**: `agent-{SELF_SHORT}` (`{socket.gethostname()}`)
- **Updated At (UTC)**: `{utc_now_iso()}`
- **Status**: `{args.status.upper()}`

---

## 1. Symptom
{args.symptom}

## 2. Root Cause Analysis (RCA)
{args.root_cause}

## 3. Fix & Optimization
{args.fix or "Under investigation / awaiting cross-node coordination."}

## 4. Cross-Platform Verification
{args.verification or "Pending verification across macOS and Windows."}
"""
    atomic_write(filepath, content)
    print(f"[OK] Incident recorded: {filepath}")


def cmd_serve_secret(args):
    """Serve ~/.oxideswarm/cluster.secret over LAN TCP only to the peer's LAN IP (never touches cloud storage)."""
    from http.server import BaseHTTPRequestHandler, HTTPServer

    hub_dir = find_hub_dir(args.hub_dir)
    peer_state = safe_read_json(os.path.join(hub_dir, "nodes", f"{PEER_SHORT}_state.json")) or {}
    allowed_peer_ip = peer_state.get("lan_ip")

    sec_info = get_oxideswarm_home_info()
    secret_path = os.path.join(sec_info["oxideswarm_home"], "cluster.secret")
    if not os.path.exists(secret_path):
        print(f"[ERROR] Secret file not found at {secret_path}", file=sys.stderr)
        sys.exit(1)

    with open(secret_path, "rb") as f:
        secret_bytes = f.read().strip() + b"\n"

    class SecretHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            client_ip = self.client_address[0]
            if allowed_peer_ip and client_ip not in (allowed_peer_ip, "127.0.0.1"):
                self.send_response(403)
                self.end_headers()
                return
            if self.path != "/cluster.secret":
                self.send_response(404)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(secret_bytes)))
            self.end_headers()
            self.wfile.write(secret_bytes)

        def log_message(self, fmt, *log_args):
            pass

    server = HTTPServer(("0.0.0.0", args.port), SecretHandler)
    server.timeout = 1.0
    deadline = time.time() + args.timeout
    print(
        f"[OK] Serving cluster.secret over LAN on {get_lan_ip()}:{args.port}/cluster.secret "
        f"(allowed peer: {allowed_peer_ip or 'any LAN'}, timeout: {args.timeout}s)"
    )
    while time.time() < deadline:
        server.handle_request()


def cmd_pull_secret(args):
    """Pull cluster.secret directly over LAN TCP from the peer Master and verify its SHA-256 prefix."""
    from urllib.request import urlopen

    hub_dir = find_hub_dir(args.hub_dir)
    peer_state = safe_read_json(os.path.join(hub_dir, "nodes", f"{PEER_SHORT}_state.json"))
    if not peer_state:
        print(f"[ERROR] Peer state {PEER_SHORT}_state.json not found.", file=sys.stderr)
        sys.exit(1)

    peer_ip = args.host or peer_state.get("lan_ip")
    expected_fp = peer_state.get("security_and_config", {}).get("secret_sha256_12")
    url = f"http://{peer_ip}:{args.port}/cluster.secret"
    print(f"[INFO] Pulling cluster.secret over LAN from {url} ...")
    with urlopen(url, timeout=5.0) as resp:
        raw = resp.read().strip()

    actual_fp = hashlib.sha256(raw).hexdigest()[:12]
    if expected_fp and actual_fp != expected_fp:
        print(f"[ERROR] SHA-256 fingerprint mismatch: expected {expected_fp}, got {actual_fp}", file=sys.stderr)
        sys.exit(1)

    sec_info = get_oxideswarm_home_info()
    ox_home = sec_info["oxideswarm_home"]
    os.makedirs(ox_home, exist_ok=True)
    secret_path = os.path.join(ox_home, "cluster.secret")
    atomic_write(secret_path, raw.decode("utf-8") + "\n")
    try:
        os.chmod(secret_path, 0o600)
    except Exception:
        pass
    print(f"[OK] Synchronized cluster.secret to {secret_path} (sha256[:12] = {actual_fp})")


def cmd_watch(args):
    """Continuously poll OxideSwarm_Agent_Hub for peer messages, incident updates, and git commits."""
    hub_dir = find_hub_dir(args.hub_dir)
    repo_dir = args.repo or os.getcwd()
    inbox_dir = os.path.join(hub_dir, "mailbox", f"to_{SELF_SHORT}")
    inc_dir = os.path.join(hub_dir, "incidents")
    os.makedirs(inbox_dir, exist_ok=True)
    os.makedirs(inc_dir, exist_ok=True)

    seen_inbox = {}
    seen_incidents = {}
    for fname in os.listdir(inc_dir):
        if fname.endswith(".md"):
            fpath = os.path.join(inc_dir, fname)
            try:
                seen_incidents[fname] = os.path.getmtime(fpath)
            except OSError:
                pass

    last_peer_head = None
    print(
        f"[WATCH] Monitoring {hub_dir} as agent-{SELF_SHORT} "
        f"(interval={args.interval}s, once_on_message={args.once_on_message}, auto_pull={args.auto_pull})"
    )
    sys.stdout.flush()

    while True:
        # 1. Refresh own heartbeat
        state = collect_state(repo_dir)
        out_path = os.path.join(hub_dir, "nodes", f"{SELF_SHORT}_state.json")
        atomic_write(out_path, json.dumps(state, indent=2, ensure_ascii=False) + "\n")

        # 2. Check peer git commit and optionally auto-pull
        peer_state = safe_read_json(os.path.join(hub_dir, "nodes", f"{PEER_SHORT}_state.json"))
        if peer_state:
            peer_head = peer_state.get("git", {}).get("git_head")
            self_head = state.get("git", {}).get("git_head")
            if peer_head and peer_head != last_peer_head:
                last_peer_head = peer_head
                if peer_head != self_head:
                    print(
                        f"\n[WATCH: PEER GIT UPDATE] agent-{PEER_SHORT} is at {peer_head[:12]} "
                        f"(local: {str(self_head)[:12]})"
                    )
                    if args.auto_pull and not state.get("git", {}).get("git_dirty"):
                        try:
                            pull_out = subprocess.check_output(
                                ["git", "pull", "--rebase", "origin", "master"],
                                cwd=find_repo_dir(repo_dir),
                                stderr=subprocess.STDOUT,
                                text=True,
                                timeout=20.0,
                            ).strip()
                            print(f"[WATCH: AUTO-PULL] {pull_out}")
                        except Exception as exc:
                            print(f"[WATCH: AUTO-PULL WARN] {exc}")
                    sys.stdout.flush()

        # 3. Check for new or updated incident reports
        triggered_event = False
        for fname in sorted(os.listdir(inc_dir)):
            if not fname.endswith(".md"):
                continue
            fpath = os.path.join(inc_dir, fname)
            try:
                mtime = os.path.getmtime(fpath)
            except OSError:
                continue
            if fname not in seen_incidents or mtime > seen_incidents[fname] + 0.5:
                seen_incidents[fname] = mtime
                print(f"\n{'=' * 72}\n[WATCH: INCIDENT UPDATE] {fname}\n{'=' * 72}")
                try:
                    with open(fpath, "r", encoding="utf-8") as f:
                        print(f.read().strip())
                except Exception as exc:
                    print(f"[WARN] Could not read {fname}: {exc}")
                sys.stdout.flush()
                triggered_event = True

        # 4. Check inbox for new peer messages
        for fname in sorted(os.listdir(inbox_dir)):
            if not fname.endswith(".md"):
                continue
            fpath = os.path.join(inbox_dir, fname)
            try:
                mtime = os.path.getmtime(fpath)
            except OSError:
                continue
            if fname not in seen_inbox or mtime > seen_inbox[fname] + 0.5:
                seen_inbox[fname] = mtime
                print(f"\n{'=' * 72}\n[WATCH: NEW MESSAGE] {fname}\n{'=' * 72}")
                try:
                    with open(fpath, "r", encoding="utf-8") as f:
                        print(f.read().strip())
                except Exception as exc:
                    print(f"[WARN] Could not read {fname}: {exc}")
                sys.stdout.flush()
                triggered_event = True

        if triggered_event and args.once_on_message:
            print("[WATCH] Event received (--once-on-message); exiting to trigger agent wakeup.")
            return

        time.sleep(max(2, args.interval))


def main():
    parser = argparse.ArgumentParser(description="OxideSwarm Multi-Agent Google Drive Bridge")
    parser.add_argument("--hub-dir", help="Explicit path to OxideSwarm_Agent_Hub", default=None)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_hb = sub.add_parser("heartbeat", help="Write current node state to nodes/<platform>_state.json")
    p_hb.add_argument("--repo", help="Path to OxideSwarm repository", default=None)
    p_hb.set_defaults(func=cmd_heartbeat)

    p_diag = sub.add_parser("diagnose", help="Compare Mac & Windows states and detect root causes")
    p_diag.add_argument("--repo", help="Path to OxideSwarm repository", default=None)
    p_diag.set_defaults(func=cmd_diagnose)

    p_send = sub.add_parser("send", help="Send a message to the peer Coding Agent")
    p_send.add_argument("--title", required=True, help="Message title")
    p_send.add_argument("--body", required=True, help="Message markdown body")
    p_send.add_argument("--priority", default="normal", choices=["normal", "high", "critical"])
    p_send.set_defaults(func=cmd_send)

    p_inbox = sub.add_parser("inbox", help="Read or archive messages in mailbox/to_<self>/")
    p_inbox.add_argument("--ack", help="Filename to archive, or 'all'", default=None)
    p_inbox.set_defaults(func=cmd_inbox)

    p_inc = sub.add_parser("incident", help="Create or update a root-cause incident report")
    p_inc.add_argument("--id", required=True, help="Incident ID (e.g. INC-20261024-001)")
    p_inc.add_argument("--title", required=True, help="Short bug/optimization title")
    p_inc.add_argument("--symptom", required=True, help="Observed failure or bottleneck")
    p_inc.add_argument("--root-cause", required=True, help="Root cause analysis")
    p_inc.add_argument("--fix", default="", help="Implemented fix")
    p_inc.add_argument("--verification", default="", help="Verification evidence")
    p_inc.add_argument("--status", default="OPEN", choices=["OPEN", "FIXED", "VERIFIED", "CLOSED"])
    p_inc.set_defaults(func=cmd_incident)

    p_ssec = sub.add_parser("serve-secret", help="Serve cluster.secret over LAN TCP only to the peer node IP")
    p_ssec.add_argument("--port", type=int, default=8086, help="LAN TCP port (default: 8086)")
    p_ssec.add_argument("--timeout", type=int, default=600, help="Seconds to keep the LAN endpoint open")
    p_ssec.set_defaults(func=cmd_serve_secret)

    p_psec = sub.add_parser("pull-secret", help="Pull cluster.secret over LAN TCP from the peer Master")
    p_psec.add_argument("--host", default=None, help="Peer Master LAN IP override")
    p_psec.add_argument("--port", type=int, default=8086, help="LAN TCP port (default: 8086)")
    p_psec.set_defaults(func=cmd_pull_secret)

    p_watch = sub.add_parser(
        "watch",
        help="Continuously poll Google Drive hub for peer messages, incidents, and git updates",
    )
    p_watch.add_argument("--repo", help="Path to OxideSwarm repository", default=None)
    p_watch.add_argument("--interval", type=int, default=10, help="Polling interval in seconds (default: 10)")
    p_watch.add_argument(
        "--once-on-message",
        action="store_true",
        help="Exit with code 0 as soon as a new message or incident arrives (triggers reactive agent wakeup)",
    )
    p_watch.add_argument(
        "--auto-pull",
        action="store_true",
        help="Automatically run 'git pull --rebase origin master' when peer pushes a new commit and working tree is clean",
    )
    p_watch.set_defaults(func=cmd_watch)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
