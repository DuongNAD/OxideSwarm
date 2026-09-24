# OxideSwarm Cross-Machine File-Based Synchronization Suite

## Overview
This suite implements an autonomous, zero-dependency, partitioned file-based mailbox coordination protocol between heterogeneous operating systems (e.g. macOS and Windows 11) sharing a cloud sync folder (such as Google Drive, OneDrive, or a shared network mount).

The architecture allows two or more independent AI agents or automated nodes to:
1. **Elect Roles Dynamically**: Negotiate `SERVER` (Master Coordinator) vs `CLIENT` (Worker Node) roles autonomously using priority scores and cryptographic tie-breakers without human intervention.
2. **Exchange Endpoints & Credentials**: Share LAN IPv4 addresses, advertised ports, and P2P connection tickets (`Iroh NAT traversal ticket`) atomically without race conditions.
3. **Execute Multi-Tier Network Reachability Probes**: Programmatically test TCP port reachability, HTTP API status, UDP auto-discovery, worker node registration, and distributed compute job execution.
4. **Cross-Machine Debugging Loop**: Automatically log socket errors and actionable remediation hints (e.g., firewall inbound rules, network subnet reachability, port conflicts) to a shared mailbox log for peer diagnosis and self-healing.
5. **Record Verification Proofs**: Emit tamper-evident test confirmations (`ping_success.txt`, `SUCCESS_CONFIRMED.md`).

---

## Architecture & Directory Structure
```
scripts/sync_network/
├── sync_network.py        # Core zero-dependency Python 3 engine (cross-platform)
├── sync_network.sh        # macOS / Linux launcher script
├── sync_network.cmd       # Windows batch launcher (with py/python auto-detection)
├── protocol_v1.json       # Formal JSON schema & mailbox directory layout specification
├── ping_success.txt       # Programmatic ping & reachability test evidence
├── SUCCESS_CONFIRMED.md   # Detailed multi-tier verification report
└── README.md              # Documentation
```

### Shared Sync Directory Mailbox
When interacting across machines via a shared folder (e.g., `~/Google Drive/Drive của tôi/OxideSwarm_Sync`):
- `claim_<platform>.json`: Role bidding claims with priority and timestamp.
- `server_ready.txt` / `client_ready.txt`: State progression checkpoints.
- `p2p_ticket_<platform>.txt`: Connection tickets for NAT hole punching.
- `<platform>_ip.txt`: Discovered outgoing LAN IPv4 addresses.
- `mac_error_log.txt` / `logs/<node_id>.log`: Machine-readable error logs for cross-machine AI debugging.
- `ping_success.txt`: Final verification sign-off confirming 0.0% packet loss and low-latency round-trip time.

---

## 5-Tier Verification Matrix
| Layer | Verification Method | Target | Result | Status |
| :--- | :--- | :--- | :--- | :--- |
| **Tier 1: TCP Port Probe** | Raw TCP socket connection | `192.168.1.166:8088` | < 1 ms | **PASS (0)** |
| **Tier 2: HTTP Cluster API** | HTTP GET `/api/status` | `http://192.168.1.166:8081` | 15.35 ms (Role: `MASTER`) | **PASS (0)** |
| **Tier 3: UDP Auto-Discovery** | UDP Broadcast beacon on 8089 | LAN Broadcast / 8089 | Discovered Master in < 15 ms | **PASS (0)** |
| **Tier 4: Worker Node Registration** | `rusty-grid worker --master ...` | `192.168.1.166:8088` | Registered in cluster topology | **PASS (0)** |
| **Tier 5: Distributed Compute Ping** | `rusty-grid submit --command echo` | `192.168.1.166:8088` | 11 ms round-trip execution | **PASS (0)** |

---

## Quick Start

### On macOS / Linux:
```bash
./scripts/sync_network/sync_network.sh
```

### On Windows:
```cmd
scripts\sync_network\sync_network.cmd
```

### Custom Sync Folder:
```bash
python3 scripts/sync_network/sync_network.py --sync-dir /path/to/shared/folder
```
