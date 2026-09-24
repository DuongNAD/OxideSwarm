# Project: OxideSwarm Cross-Machine Coordination & Network Ping

## Architecture
Cross-machine autonomous coordination and network testing between two AI nodes communicating exclusively through a shared Google Drive synchronization folder (`G:\Google Drive\OxideSwarm_Sync` / `G:\My Drive\OxideSwarm_Sync`).

```
+-------------------------------------------------------------------------------------------------+
|                       SHARED GOOGLE DRIVE SYNCHRONIZATION DIRECTORY                             |
|                           (G:\My Drive\OxideSwarm_Sync\)                                        |
|  - Single-Writer Partitioned Mailbox Architecture (prevents Google Drive sync conflict copies)   |
|  - 7-Stage State Machine: UNKNOWN -> ROLE_PROPOSED -> ROLE_CONFIRMED -> READY -> CONNECTING     |
|                           -> CONNECTED -> SUCCESS                                               |
|  - Deterministic Role Tie-Breaking: Priority score + lexicographic hash comparison             |
+-----------------------------------------------+-------------------------------------------------+
                                                |
                       +------------------------+------------------------+
                       |                                                 |
         Node A Mailbox (nodes/win_case_/)                 Node B Mailbox (nodes/peer_/)
         - announce.json (IPs, role bid)                   - announce.json (IPs, role bid)
         - state.json (state machine)                      - state.json (state machine)
         - ticket.json (P2P ticket / LAN port)             - ticket.json (if Server)
         - heartbeat.json (liveness pulse)                 - heartbeat.json (liveness pulse)
                       |                                                 |
                       +------------------------+------------------------+
                                                |
+-----------------------------------------------v-------------------------------------------------+
|                           NETWORK REACHABILITY & PING EXECUTION                                 |
|  - Tier 1: Fast TCP Socket Reachability (Port 8088 / Test-NetConnection)                        |
|  - Tier 2: HTTP Cluster Status & Telemetry Ping (Port 8081 / /api/status)                       |
|  - Tier 3: OxideSwarm Native Worker Registration & Status (rusty-grid worker / status)          |
|  - Tier 4: Native P2P NAT Traversal (Iroh QUIC / DERP Relay ticket fallback)                    |
|  - Tier 5: End-to-End Distributed Compute Verification (rusty-grid submit ping-pong)            |
+-----------------------------------------------+-------------------------------------------------+
                                                |
                                                v
+-------------------------------------------------------------------------------------------------+
|                         VERIFICATION & SHARED SUCCESS CONFIRMATION                              |
|  - verifications/<node_id>_result.json: Programmatic test results with exit code 0              |
|  - SUCCESS_CONFIRMED.md: Mutual success confirmation report in shared sync folder               |
+-------------------------------------------------------------------------------------------------+
```

## Feature Inventory
| # | Feature | Description | Milestone | Source |
|---|---------|-------------|-----------|--------|
| F1 | Sync Folder & Path Normalization | Manage `G:\My Drive\OxideSwarm_Sync` (with fallback to `G:\Google Drive\OxideSwarm_Sync`), ensure directory hierarchy | M1 | R1, Survey |
| F2 | Partitioned Mailbox Architecture | Single-writer directory structure (`nodes/<node_id>/`) avoiding Google Drive conflicted copies | M1 | R1, Survey |
| F3 | Autonomous Role Negotiation & Tie-Breaking | Bidding protocol with priority and deterministic tie-breaker hash resolving Server/Client | M1 | R1, Survey |
| F4 | Connection Parameter Exchange | Exchange local IPv4 (`192.168.1.166`), ports (8088, 8081), and Iroh P2P tickets via `announce.json` & `ticket.json` | M1 | R1, Survey |
| F5 | Server / Master Node Execution | Launch OxideSwarm Master on TCP 8088, Web UI 8081 with `--p2p`, and publish ticket | M2 | R2, Survey |
| F6 | Client / Worker Connection Execution | Execute connection tests and worker join using exchanged parameters (LAN TCP & P2P ticket) | M2 | R2, Survey |
| F7 | Iterative Debugging & Shared Logging | Capture execution logs to `logs/<node_id>.log` in shared folder, inspect peer logs, auto-adjust on failure | M2 | R3, Survey |
| F8 | Programmatic Network Ping Verification | Scripted multi-tier reachability tests (TCP, HTTP status, worker registration, compute ping) | M3 | Acceptance Criteria |
| F9 | Mutual Success Confirmation Report | Write `verifications/<node_id>_result.json` and `SUCCESS_CONFIRMED.md` in shared sync folder | M3 | Acceptance Criteria |
| F10 | Multi-Agent Review, Challenge & Forensic Audit Gate | Independent review, adversarial challenge, and zero-tolerance forensic integrity audit | M3 | System Constraints |

## Milestones
| # | Name | Scope | Dependencies | Status |
|---|------|-------|-------------|--------|
| M1 | Coordination Protocol & Role Negotiation | Partitioned sync folder setup, announce.json publishing, role negotiation tie-breaker, ticket exchange protocol | None | DONE |
| M2 | Connection Testing & Iterative Debugging | Master/Server execution with P2P, connection test scripts, iterative log capture, bidirectional debugging | M1 | DONE |
| M3 | Verification, Mutual Success & Forensic Audit | Programmatic test execution with exit code 0, SUCCESS_CONFIRMED.md publishing, multi-agent review, challenge & forensic audit | M2 | DONE |

## Interface Contracts
### Coordination State Machine & File Schema
- Protocol Version: `1.0.0`
- Base Directory: `G:\My Drive\OxideSwarm_Sync` (fallback `G:\Google Drive\OxideSwarm_Sync`)
- Node Mailbox: `nodes/<node_id>/`
- Files:
  - `announce.json`: `{ protocol_version, node_id, hostname, platform, timestamp_utc, preferred_role, priority, tie_breaker, lan_ipv4, advertised_port, dashboard_port, capabilities }`
  - `state.json`: `{ node_id, current_state, role, updated_utc }`
  - `ticket.json`: `{ server_node_id, timestamp_utc, lan_endpoints, p2p_ticket, web_dashboard_url }`
  - `heartbeat.json`: `{ node_id, timestamp_utc, status }`
  - `logs/<node_id>.log`: Real-time execution and error diagnostics
  - `verifications/<node_id>_result.json`: `{ node_id, status: "SUCCESS", transport_used, target_endpoint, rtt_ms, test_command, exit_code: 0, details }`
  - `SUCCESS_CONFIRMED.md`: Final mutual report confirming two-way communication

## Code Layout
- `target\debug\rusty-grid.exe`: OxideSwarm core executable (Master & Worker).
- `G:\My Drive\OxideSwarm_Sync\`: Shared cloud synchronization folder for inter-AI coordination.
- `e:\teamwork_projects\OxideSwarm\scripts\`: Coordination and connection test automation scripts.
- `e:\teamwork_projects\OxideSwarm\.agents\`: Agent metadata, plans, progress, and handoffs.
