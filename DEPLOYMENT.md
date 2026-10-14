# OxideSwarm Agent Mesh: Cross-Platform Deployment & Operations Guide

The **OxideSwarm Agent Mesh** is a communication layer enabling coding agents and developer automation nodes to connect, exchange telemetry, transmit data payloads, and route structured commands across **Windows, macOS, Ubuntu Linux, and Android** over a local network (LAN) or a tunnel you control.

> **Security first.** Every node of the mesh executes what the hub forwards to it, including `shell_exec` and
> arbitrary programs, with the rights of the account the node runs under. The hub therefore:
> * listens on `127.0.0.1:8088` by default; a non-loopback `--listen` needs `--token` (or `OXIDE_MESH_TOKEN`),
>   or the explicit `--insecure` flag, which lets anyone who reaches the port run commands on every node;
> * does **not** encrypt: `ws://` sends the token and every command in clear text. Use `wss://` behind a TLS
>   proxy, or a VPN / SSH tunnel;
> * must **never be published to the Internet** (no quick tunnels, no router port forward).
>
> This mesh is separate from the compute grid (master/workers, `README.md`), which has its own cluster secret.
> Both default to TCP port 8088: when they run on one machine give the hub another port (examples below use 8090).
> Full model: [docs/SECURITY.md](docs/SECURITY.md).

---

## 1. Architectural Overview

```
                      ┌─────────────────────────────────┐
                      │         OxideRelay Hub          │
                      │    (axum / tokio-tungstenite)   │
                      │      Active Node Catalog        │
                      └───────▲───────▲────────▲────────┘
                              │       │        │
                Persistent WS │       │ WS     │ Persistent WS
                    (outbound)│       │        │ (outbound)
                              ▼       ▼        ▼
            ┌─────────────────┐ ┌───────────┐ ┌───────────────────┐
            │ Node 1: Windows │ │Node 2: Mac│ │ Node 3: Android   │
            │ Coding Agent    │ │Coding Agt │ │ Coding Agent      │
            └─────────────────┘ └───────────┘ └───────────────────┘
```

### Key Technical Pillars:
1. **100% NAT & Firewall Traversal (Outbound WebSocket Transport):**
   Nodes initiate persistent outbound TCP/WebSocket connections (`ws://` or `wss://`). Nodes never open inbound listening ports; only the Hub host has to be reachable (same LAN, VPN or SSH tunnel). That lets a node behind NAT or CGNAT take part, as long as it can reach the Hub.
2. **Targeted Node-to-Node Routing:**
   Nodes register with unique canonical IDs (e.g. `node-windows-case`, `node-android-s24`). Commands are routed point-to-point via the Hub without broadcast storming. The Hub rejects a second node that claims an id already in use and checks that the `from` of every envelope is the registered node.
3. **Dual Client Runtime:**
   - **Native Rust Binary (`agent-mesh`):** a single executable.
   - **Python Client (`scripts/agent_node.py`):** no compilation, needs Python 3.8+ and `websockets`; runs on Termux, servers, and desktops.
4. **Delivery tracking and payload integrity:**
   TCP stream ordering paired with application-layer **Correlation IDs (UUIDv4)**, two-tier delivery acknowledgments (`DeliveryAck`, `DeliveryNack`), and **SHA-256 verification of data payloads** (a check against corruption, not authentication).

---

## 2. Quickstart: Launching the Hub

The **OxideRelay Hub** serves as the central message switchboard and active directory catalog.

### Option A: Using the Rust Binary (`agent-mesh`)
```bash
# Build the binary
cargo build --release -p agent_mesh

# Choose a hub token (any strong random string); the hub and every node / client need the same one
export OXIDE_MESH_TOKEN="$(openssl rand -hex 24)"

# Loopback only (this machine); the token is optional here
./target/release/agent-mesh hub --listen 127.0.0.1:8090

# Reachable from other machines on a LAN you trust: a token is mandatory (--insecure is the unsafe override)
./target/release/agent-mesh hub --listen 0.0.0.0:8090
```
`--token` exists on every subcommand but is visible in the process list; prefer the `OXIDE_MESH_TOKEN` environment variable.

### Option B: On Windows
```powershell
$env:OXIDE_MESH_TOKEN = "<your hub token>"
.\target\release\agent-mesh.exe hub --listen 0.0.0.0:8090
```

### Hub HTTP endpoints:
The Hub also answers plain HTTP on the same port. When a token is configured, `/api/nodes` and `/api/status` need `Authorization: Bearer <token>`; the health check is open. Without a token they only answer requests whose `Host` is a loopback name (`127.0.0.1`, `localhost`, `[::1]`), so a web page cannot read them through DNS rebinding; to read them from another machine, start the hub with a token.
- **Node Catalog:** `http://127.0.0.1:8090/api/nodes` (connected nodes and capabilities)
- **Cluster Status:** `http://127.0.0.1:8090/api/status` (uptime, routed command counts, delivery metrics)
- **Health Check:** `http://127.0.0.1:8090/api/health`

---

## 3. Platform Deployment Guides

### 3.1 Windows 10/11 & Windows Server

#### Prerequisites:
- Windows 10/11 (x86_64 or ARM64)
- Native Rust toolchain OR Python 3.8+ (`pip install websockets`)

#### Turnkey Execution:
Set the hub token first (`$env:OXIDE_MESH_TOKEN = "<token>"`); the runner warns when it is missing.
```powershell
# Using the automated PowerShell runner:
.\packaging\agent_mesh\windows\run_agent_node.ps1 -Hub "ws://<HUB_IP>:8090/ws" -NodeId "node-win-desktop"

# Or using the Command Prompt runner:
packaging\agent_mesh\windows\run_agent_node.cmd ws://<HUB_IP>:8090/ws node-win-desktop
```

#### Running as a Background Windows Service (SCM):
To keep the node running across user logoffs (a service environment variable `OXIDE_MESH_TOKEN` must be set for the service, or the token passed with `--token`, which is visible in the process list). Without `obj=` a service runs as LocalSystem and would execute hub commands with full control of the PC; give it a low-privilege account:
```powershell
sc.exe create OxideAgentMesh binPath= "C:\OxideSwarm\agent-mesh.exe node --hub ws://<HUB_IP>:8090/ws --id node-win-srv --platform windows" start= auto obj= "NT AUTHORITY\LocalService"
sc.exe start OxideAgentMesh
```

---

### 3.2 macOS (Apple Silicon M-Series & Intel)

#### Prerequisites:
- macOS 11.0+ (Big Sur through macOS 15+ Sequoia)
- Python 3.8+ (`pip3 install websockets`) or Rust compiler

#### Turnkey Execution:
```bash
chmod +x packaging/agent_mesh/macos/run_agent_node.sh
./packaging/agent_mesh/macos/run_agent_node.sh "ws://<HUB_IP>:8090/ws" "node-macos-pro"
```

#### Running as an Apple `launchd` Daemon:
A LaunchAgent under your own account (not a root LaunchDaemon) keeps hub commands at your privilege level. Add `OXIDE_MESH_TOKEN` to its `EnvironmentVariables` (shown below) or the node cannot register on a token-protected hub. Install to `~/Library/LaunchAgents/com.oxideswarm.agent.plist`:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.oxideswarm.agent</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/agent-mesh</string>
        <string>node</string>
        <string>--hub</string>
        <string>ws://<HUB_IP>:8090/ws</string>
        <string>--id</string>
        <string>node-macos-pro</string>
        <string>--platform</string>
        <string>macos</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>OXIDE_MESH_TOKEN</key>
        <string>REPLACE_WITH_HUB_TOKEN</string>
    </dict>
    <key>KeepAlive</key>
    <true/>
    <key>RunAtLoad</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/tmp/agent_mesh.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/agent_mesh.err</string>
</dict>
</plist>
```
Activate via:
```bash
launchctl load ~/Library/LaunchAgents/com.oxideswarm.agent.plist
```

---

### 3.3 Ubuntu / Debian Linux

#### Prerequisites:
```bash
sudo apt update && sudo apt install -y python3 python3-pip git
pip3 install websockets
```

#### Turnkey Execution:
```bash
chmod +x packaging/agent_mesh/ubuntu/run_agent_node.sh
./packaging/agent_mesh/ubuntu/run_agent_node.sh "ws://<HUB_IP>:8090/ws" "node-ubuntu-cloud"
```

#### Running as a Systemd Service:
Install to `/etc/systemd/system/oxideswarm-agent.service`:
```ini
[Unit]
Description=OxideSwarm Agent Mesh Node Client
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu     # an unprivileged account: hub commands run with its rights
ExecStart=/usr/local/bin/agent-mesh node --hub ws://<HUB_IP>:8090/ws --id node-ubuntu-srv --platform ubuntu
Restart=always
RestartSec=3s
LimitNOFILE=65536
Environment=RUST_LOG=info
# Environment=OXIDE_MESH_TOKEN=<hub token>   (better: EnvironmentFile=/etc/oxideswarm-agent.env, mode 600)

[Install]
WantedBy=multi-user.target
```
Activate via:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now oxideswarm-agent
sudo journalctl -u oxideswarm-agent -f
```

---

### 3.4 Android (Android 7.0+ through Android 15+)

Android requires specialized handling to circumvent Doze mode and mobile memory management without requiring device root. Three turnkey deployment methods are supported:

#### Method 1: Termux Userspace Deployment (Zero Root, Zero Build Friction)
Termux provides a complete Linux userspace on Android without rooting:
```bash
# 1. Open Termux on Android device
# 2. Update packages and install python
pkg update -y && pkg install -y python python-pip git

# 3. Install lightweight websockets library
pip install websockets

# 4. Acquire CPU wake lock (prevents OS from freezing process in Doze mode)
termux-wake-lock

# 5. Launch the node runner
chmod +x packaging/agent_mesh/android/run_agent_node_termux.sh
bash packaging/agent_mesh/android/run_agent_node_termux.sh "ws://<HUB_IP>:8090/ws" "node-android-s24"
```

#### Method 2: Standalone Native ELF Binary via ADB Shell
For headless CI testbenches or automated test devices:
```bash
# 1. Cross-compile for ARM64 Android (on development host with Android NDK)
cargo build --release --target aarch64-linux-android -p agent_mesh

# 2. Run the turnkey ADB deployer script:
chmod +x packaging/agent_mesh/android/run_agent_node_adb.sh
./packaging/agent_mesh/android/run_agent_node_adb.sh "ws://<HUB_IP>:8090/ws" "node-android-adb"
```

#### Method 3: Android Foreground Service App (`packaging/android/app/`)
This app runs the **compute-grid worker** (not the agent-mesh node) in-process via JNI (`crates/android_bridge`):
- It needs `liboxideworker.so`, built first with `packaging/android/build_android_jni.sh --release`; without it the app has no worker. See [packaging/android/README.md](packaging/android/README.md).
- It uses an Android Foreground Service with an ongoing notification and holds `PARTIAL_WAKE_LOCK` / `WIFI_MODE_FULL_LOW_LATENCY`.
- Built-in tasks run inside the app process and are not exposed to the Android 12+ **Phantom Process Killer (PPK)**; command, shell and compile tasks spawn child processes, and those can be killed by it.
- It cannot hold the grid's cluster secret yet (see README, Known Limitations #5).

### 3.5 Upgrading a running grid (master and workers)
The compute grid's wire protocol (master, workers, `submit` / `status`) has no version negotiation and encodes
messages positionally, so a message type that an older master does not know is treated as a corrupt frame and the
sender is disconnected (and its running tasks failed over). Always **restart the master first**, then the workers
and CLIs. A worker newer than its master keeps working for the messages a master announces in its registration
reply (the live battery report is one); a client command newer than the master (`GetTaskResult`) is not guarded.
The Agent Mesh hub in this guide uses its own JSON envelopes and is unaffected.

---

## 4. Testing & Verification

### 4.1 Protocol Simulation (Python reference hub, NOT the Rust hub)
`tests/run_all_mesh_tests.py` runs the mesh protocol against a **Python re-implementation of the hub**
(`OxideRelayHub` inside `tests/test_agent_mesh_simulation.py`) with simulated nodes. It documents and checks the
protocol; it never starts the Rust `agent-mesh` binary, so a pass says nothing about the Rust hub.
```bash
python3 -c "import websockets; print('Websockets OK')"
python3 tests/run_all_mesh_tests.py
```
It executes an automated 8-phase scenario:
1. Starts the Python reference hub on loopback.
2. Starts 3 simulated nodes (`node-win-1`, `node-mac-2`, `node-android-3`).
3. Queries node catalog.
4. Routes targeted command from Node 1 to Node 3.
5. Asserts Node 2 isolation (execution count strictly 0).
6. Executes real OS subprocess command on Node 3.
7. Transmits 64 KB forward and 64 KB reverse payload with bit-for-bit SHA-256 validation.
8. Sends a 50-packet burst in each direction and counts losses.

### 4.2 Tests against the Rust hub
```bash
cargo test -p agent_mesh                          # unit + integration tests of the crate (hub, client, executor, bridge, token auth)
cargo build -p agent_mesh                         # needed by the two Python suites below
python3 tests/test_agent_mesh_token_auth.py       # real Rust hub: token, duplicate ids, forged `from`
python3 tests/test_agent_mesh_rust_hub_cross_interop.py
```
The crate tests cover:
- SHA-256 computation and verification
- Built-in commands (`echo`, `ping`, `system_info`)
- Shell execution and process timeout watchdog
- Relay Hub lifecycle and routing switchboard
- E2E command routing across clients
- Lossless bidirectional data payload transmission
- Negative routing with `DeliveryNack` generation

---

## 5. CLI Reference (`agent-mesh`)

| Subcommand | Flags | Description |
|---|---|---|
| `hub` | `--listen <ADDR>`, `--token <TOKEN>`, `--insecure` | Starts the WebSocket Relay Hub (default listen `127.0.0.1:8088`; a non-loopback address needs a token or `--insecure`) |
| `node` | `--hub <URL>`, `--id <NAME>`, `--platform <TAG>`, `--token <TOKEN>` | Starts an agent node client connected to the Hub |
| `send` | `--hub <URL>`, `--from <ID>`, `--to <TARGET>`, `--command <CMD>`, `--args <JSON>`, `--timeout-ms <MS>`, `--token <TOKEN>` | Sends a command to a specific node and prints execution output |
| `list` | `--hub <URL>`, `--from <ID>`, `--token <TOKEN>` | Prints all currently online nodes registered with the Hub |
| `data` | `--hub <URL>`, `--from <ID>`, `--to <TARGET>`, `--payload <STR>`, `--token <TOKEN>` | Sends a verified data payload to a target node |

`--token` reads `OXIDE_MESH_TOKEN` when not given. Default hub URL for the client subcommands is `ws://127.0.0.1:8088/ws`
(add `--hub ws://127.0.0.1:8090/ws` when the hub runs on 8090 as in the examples above).

---

## 6. Troubleshooting & Network Topologies

### Nodes on other networks
**Do not expose the Hub to the Internet.** Anyone who reaches it with the token (or, with `--insecure`, without
it) can make every node run commands as the node's user. A public quick tunnel (`cloudflared tunnel --url ...`)
or a router port forward publishes exactly that, and the token would then travel in clear text over `ws://`.
Reach a hub that is not on your LAN only through a channel that authenticates and encrypts, for example:
- **SSH tunnel** to a hub that listens on loopback: `ssh -N -L 8090:127.0.0.1:8090 user@hub-host`, then point the node at `ws://127.0.0.1:8090/ws`.
- **A VPN or private overlay network** you control (WireGuard, Tailscale), with the hub bound to that interface.
- If you must publish a `wss://` endpoint, terminate TLS in a reverse proxy that you also protect with its own authentication; the hub token alone is not meant to guard an Internet-facing service.

### LAN Wi-Fi Setup
Allow incoming TCP on the hub port from the local subnet only:
```powershell
# Windows Defender Firewall rule (local subnet only)
New-NetFirewallRule -DisplayName "OxideSwarm Hub" -Direction Inbound -LocalPort 8090 -Protocol TCP -Action Allow -RemoteAddress LocalSubnet
```
