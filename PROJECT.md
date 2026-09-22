# Project: Remote Cluster Interconnect (OxideSwarm)

## Architecture
OxideSwarm's remote interconnect enables zero-configuration, secure, high-performance inter-machine communication across arbitrary WAN and NAT boundaries without third-party VPNs or port forwarding.

```
+-----------------------------------------------------------------------------------+
|                                 MASTER NODE                                       |
|  - Persistent SecretKey (~/.oxideswarm/master_key.bin) -> Deterministic Ticket    |
|  - Iroh QUIC Endpoint + N0 DERP Relay (https://*.relay.n0.iroh.link)               |
|  - Awaits endpoint.online().await before publishing ticket                        |
|  - Tracks conn.paths(): selected path, is_ip / is_relay, smoothed RTT             |
|  - Exposes connection_type, is_relayed, rtt_ms on /api/status & Web Dashboard    |
+----------------------------------------+------------------------------------------+
                                         |
                       +-----------------+-----------------+
                       |                                   |
         Direct UDP Hole Punching                  DERP Relay Fallback
         (QUIC, sub-millisecond LAN,              (HTTPS/TLS Encapsulation,
          lowest WAN RTT)                          100% Symmetric NAT bypass)
                       |                                   |
                       +-----------------+-----------------+
                                         |
+----------------------------------------v------------------------------------------+
|                                 WORKER NODE                                       |
|  - 1-Click Connect (connect_remote.sh on macOS / connect_remote.cmd on Windows)  |
|  - Parses deterministic ticket, binds Iroh endpoint                               |
|  - Exponential Backoff Auto-Reconnect (<3s recovery on restart)                   |
|  - Reports telemetry & responds to millisecond RTT heartbeats                     |
+-----------------------------------------------------------------------------------+
```

## Feature Inventory
| # | Feature | Description | Milestone | Source |
|---|---------|-------------|-----------|--------|
| F1 | `endpoint.online()` Timing Fix | Await `endpoint.online().await` with timeout before publishing ticket to ensure home relay and STUN WAN IPs are populated | M1 | Survey (spec miner) [DONE] |
| F2 | Seamless DERP Relay Fallback | Guarantee 100% transparent fallback to N0 DERP relay if direct UDP hole-punching is blocked | M1 | R1, Survey [DONE] |
| F3 | Runtime Connection Introspection | Query `conn.paths()` on accepted/dialed connections to detect `is_selected()`, `is_ip()`, `is_relay()`, and `rtt()` | M1 | R1, R4, Survey [DONE] |
| F4 | Persistent SecretKey & Deterministic Ticket | Auto-default Master SecretKey storage (`~/.oxideswarm/master_key.bin`) and retain deterministic ticket across restarts | M2 | R2, Survey [DONE] |
| F5 | Exponential Backoff Auto-Reconnect | Worker self-healing reconnection loop with jitter and cached endpoint invalidation reconnecting in < 3s | M2 | R2, Survey [DONE] |
| F6 | 1-Click macOS Automation Script | `connect_remote.sh` with interactive pairing, config persistence, and LaunchDaemon integration | M2 | R2, Survey [DONE] |
| F7 | 1-Click Windows Automation Script | `connect_remote.cmd` with UAC auto-elevation, service registration, and PowerShell bypass | M2 | R2, Survey [DONE] |
| F8 | Comparative Technical Benchmark | Quantitative empirical study comparing OxideSwarm Native Iroh vs Tailscale vs Cloudflare Tunnel across RTT, throughput, RAM/CPU, firewall penetration, and complexity | M3 | R3, Survey [DONE] |
| F9 | Benchmark Deliverables Publishing | Generate `COMPARATIVE_INTERCONNECT_BENCHMARK.md`, `benchmark_data.json`, and `benchmark_matrix.csv` in `~/teamwork_projects/remote_cluster_interconnect` | M3 | R3, Survey [DONE] |
| F10 | Telemetry Model Extension | Add `interconnect_type`, `is_relayed`, and `rtt_ms` to `WorkerUiInfo` and `/api/status` DTOs | M4 | R4, Survey [DONE] |
| F11 | Dashboard Visual Indicators & Badges | Web UI Link Badges (`[● Direct P2P | 12ms]`, `[▲ DERP Relay | 68ms]`), dynamic SVG topology path styling, and copy ticket UI | M4 | R4, Survey [DONE] |
| F12 | Degradation & Switch Alerts | Real-time visual alert banner/toast on network failover or high latency degradation | M4 | R4, Survey [DONE] |
| F13 | CLI Telemetry Visualization | Display link mode (`Direct P2P` vs `DERP Relay`) and live RTT in `rusty-grid status` CLI output | M4 | R4, Survey [DONE] |
| F14 | Cross-Network NAT Simulation Test | Automated scenario simulating 2 nodes on separate subnets proving direct hole punching | M5 | Acceptance Criteria [DONE] |
| F15 | Relay Fallback Simulation Test | Automated scenario simulating UDP block proving transparent DERP relay fallback | M5 | Acceptance Criteria [DONE] |
| F16 | Ticket Determinism & Reconnect Test | Automated test verifying ticket stability across Master restarts and < 3s reconnection | M5 | Acceptance Criteria [DONE] |
| F17 | Forensic Integrity Audit | Independent forensic audit ensuring clean, non-fabricated, 100% authentic implementations | M5 | Protocol [DONE] |

## Milestones
| # | Name | Scope | Dependencies | Status |
|---|------|-------|-------------|--------|
| M1 | Native P2P Remote WAN Interconnect (R1) | Fix `endpoint.online()` timing, DERP relay fallback, and runtime connection path inspection | None | DONE |
| M2 | Stable Identity Pairing & 1-Click Automation (R2) | Persistent SecretKey, deterministic ticket, auto-reconnect backoff, `connect_remote.sh` & `connect_remote.cmd` | M1 | DONE |
| M3 | Quantitative Benchmark & Comparative Report (R3) | Empirical benchmarking report & data comparing Iroh, Tailscale, Cloudflare | None | DONE |
| M4 | Dashboard & CLI Telemetry Visualization (R4) | Web UI Link Badges, SVG topology, latency alerts, and CLI link status | M1, M2 | DONE |
| M5 | Integrated Verification, Hardening & Forensic Audit | Cross-network simulation, relay fallback test, ticket test, full workspace test, forensic audit | M1, M2, M3, M4 | DONE |

## Interface Contracts
### Master ↔ Worker P2P Transport (`crates/core/src/transport.rs`)
- `serialize_p2p_ticket(addr: &iroh::EndpointAddr) -> Result<String, serde_json::Error>`
- `parse_p2p_ticket(ticket: &str) -> Result<iroh::EndpointAddr, serde_json::Error>`
- `P2pPathInfo`: `is_relay: bool`, `is_ip: bool`, `remote_addr: Option<String>`, `rtt_ms: Option<f32>`
- `inspect_connection_paths(conn: &iroh::endpoint::Connection) -> Option<P2pPathInfo>`
- ALPN: `b"rusty-grid/v1"`

### Runtime Telemetry Contract (`crates/master/src/web_ui.rs`, `crates/core/src/protocol.rs`)
- `WorkerUiInfo`:
  - `interconnect_type: String` ("Direct P2P (QUIC)", "Relay (DERP)", "TCP/LAN")
  - `is_relayed: bool`
  - `rtt_ms: Option<f32>`
- `ClusterStatusDto`:
  - `p2p_ticket: Option<String>`
- Heartbeat:
  - `timestamp_ms: u64` (millisecond timestamp for sub-millisecond precision RTT calculation)

### 1-Click Script Contract (`connect_remote.sh`, `connect_remote.cmd`)
- Syntax: `connect_remote.sh [P2P_TICKET]` or interactive prompt
- Syntax: `connect_remote.cmd [P2P_TICKET]` or double-click interactive prompt
- Config Target: `~/.oxideswarm/rusty-grid.toml` (macOS/Linux) or `%USERPROFILE%\.oxideswarm\rusty-grid.toml` (Windows)
- Execution: Launches background daemon or service with automatic restart on reboot.

## Code Layout
- `crates/core/src/transport.rs`: P2P transport primitives, ticket serialization/deserialization.
- `crates/core/src/protocol.rs`: Master-worker wire protocol and heartbeat structures.
- `crates/master/src/server.rs`: Master server, P2P endpoint binding, SecretKey resolution, connection handling.
- `crates/master/src/web_ui.rs`: Axum web server and API status handlers.
- `crates/master/src/dashboard.html`: Web UI Dashboard single-page application.
- `crates/worker/src/client.rs`: Worker client loop, connection management, exponential backoff.
- `crates/worker/src/heartbeat.rs`: Worker heartbeat sender and RTT calculator.
- `crates/cli/src/main.rs`: CLI command parser and formatted output.
- `connect_remote.sh`: 1-click connection script for macOS/Linux.
- `connect_remote.cmd`: 1-click connection script for Windows.
- `packaging/windows/install_windows_service.ps1`: Windows SCM service installer.
- `/Users/duongnad/teamwork_projects/remote_cluster_interconnect/`: Deliverables directory containing reports and benchmark data.
