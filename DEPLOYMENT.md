# OxideSwarm Agent Mesh: Cross-Platform Deployment & Operations Guide

The **OxideSwarm Agent Mesh** is a resilient, cross-platform communication framework enabling autonomous coding agents and developer automation nodes to connect, exchange telemetry, transmit data payloads, and route structured commands across **Windows, macOS, Ubuntu Linux, and Android** over local networks (LAN) and the public Internet.

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
   Nodes initiate persistent outbound TCP/WebSocket connections (`ws://` or `wss://`) over standard HTTP/HTTPS ports. Nodes never open inbound listening ports, completely bypassing domestic router NAT, carrier-grade NAT (**CGNAT** on cellular 4G/5G), and corporate firewalls.
2. **Targeted Node-to-Node Routing:**
   Nodes register with unique canonical IDs (e.g. `node-windows-case`, `node-android-s24`). Commands are routed point-to-point via the Hub without broadcast storming.
3. **Dual Client Runtime:**
   - **Native Rust Binary (`agent-mesh`):** High-performance, single-executable binary with sub-millisecond local execution.
   - **Polyglot Python Client (`scripts/agent_node.py`):** Zero-compilation client requiring only standard Python 3.8+ and `websockets`, running out of the box on Termux, servers, and desktops.
4. **Guaranteed Zero Data Loss:**
   TCP stream ordering paired with application-layer **Correlation IDs (UUIDv4)**, two-tier delivery acknowledgments (`DeliveryAck`, `DeliveryNack`), and **SHA-256 cryptographic payload verification**.

---

## 2. Quickstart: Launching the Hub

The **OxideRelay Hub** serves as the central message switchboard and active directory catalog.

### Option A: Using Pre-compiled Rust Binary (`agent-mesh`)
```bash
# Build the binary
cargo build --release -p agent_mesh

# Start the Hub on standard port 8088 (or custom port)
./target/release/agent-mesh hub --listen 0.0.0.0:8088
```

### Option B: On Windows
```powershell
.\target\release\agent-mesh.exe hub --listen 0.0.0.0:8088
```

### Hub HTTP Observability Endpoints:
Once launched, the Hub provides live REST observability:
- **Node Catalog:** `http://127.0.0.1:8088/api/nodes` (Returns JSON array of connected nodes and capabilities)
- **Cluster Status:** `http://127.0.0.1:8088/api/status` (Uptime, routed command counts, delivery metrics)
- **Health Check:** `http://127.0.0.1:8088/api/health`

---

## 3. Platform Deployment Guides

### 3.1 Windows 10/11 & Windows Server

#### Prerequisites:
- Windows 10/11 (x86_64 or ARM64)
- Native Rust toolchain OR Python 3.8+ (`pip install websockets`)

#### Turnkey Execution:
```powershell
# Using the automated PowerShell runner:
.\packaging\agent_mesh\windows\run_agent_node.ps1 -Hub "ws://192.168.1.100:8088/ws" -NodeId "node-win-desktop"

# Or using the Command Prompt runner:
packaging\agent_mesh\windows\run_agent_node.cmd ws://192.168.1.100:8088/ws node-win-desktop
```

#### Running as a Background Windows Service (SCM):
To ensure the node runs continuously across user logoffs:
```powershell
# Register service using sc.exe
sc.exe create OxideAgentMesh binPath= "C:\OxideSwarm\agent-mesh.exe node --hub ws://192.168.1.100:8088/ws --id node-win-srv --platform windows" start= auto
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
./packaging/agent_mesh/macos/run_agent_node.sh "ws://192.168.1.100:8088/ws" "node-macos-pro"
```

#### Running as an Apple `launchd` Daemon:
Install to `~/Library/LaunchAgents/com.oxideswarm.agent.plist`:
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
        <string>ws://192.168.1.100:8088/ws</string>
        <string>--id</string>
        <string>node-macos-pro</string>
        <string>--platform</string>
        <string>macos</string>
    </array>
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
./packaging/agent_mesh/ubuntu/run_agent_node.sh "ws://192.168.1.100:8088/ws" "node-ubuntu-cloud"
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
User=ubuntu
ExecStart=/usr/local/bin/agent-mesh node --hub ws://192.168.1.100:8088/ws --id node-ubuntu-srv --platform ubuntu
Restart=always
RestartSec=3s
LimitNOFILE=65536
Environment=RUST_LOG=info

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
bash packaging/agent_mesh/android/run_agent_node_termux.sh "ws://192.168.1.100:8088/ws" "node-android-s24"
```

#### Method 2: Standalone Native ELF Binary via ADB Shell
For headless CI testbenches or automated test devices:
```bash
# 1. Cross-compile for ARM64 Android (on development host with Android NDK)
cargo build --release --target aarch64-linux-android -p agent_mesh

# 2. Run the turnkey ADB deployer script:
chmod +x packaging/agent_mesh/android/run_agent_node_adb.sh
./packaging/agent_mesh/android/run_agent_node_adb.sh "ws://192.168.1.100:8088/ws" "node-android-adb"
```

#### Method 3: Android Foreground Service App (`packaging/android/app/`)
For commercial mobile deployments:
- Runs in-process via JNI using `crates/android_bridge`.
- Uses an Android Foreground Service with an ongoing notification.
- Holds `PARTIAL_WAKE_LOCK` and `WIFI_MODE_FULL_LOW_LATENCY`, rendering it 100% immune to the Android 12+ **Phantom Process Killer (PPK)**.

---

## 4. Testing & Verification

### 4.1 Programmatic 3-Node Simulation Test
To test the entire mesh locally (Hub + 3 simulated heterogeneous nodes):
```bash
# Verify Python test harness
python -c "import websockets; print('Websockets OK')"
python tests/run_all_mesh_tests.py
```
This executes an automated 8-phase test:
1. Spawns ephemeral Hub on loopback.
2. Spawns 3 simulated nodes (`node-win-1`, `node-mac-2`, `node-android-3`).
3. Queries node catalog.
4. Routes targeted command from Node 1 to Node 3.
5. Asserts Node 2 isolation (execution count strictly 0).
6. Executes real OS subprocess command on Node 3.
7. Transmits 64 KB forward and 64 KB reverse payload with bit-for-bit SHA-256 validation.
8. Verifies high-throughput 50-packet burst with zero packet loss.

### 4.2 Native Rust Unit and Integration Tests
```bash
cargo test -p agent_mesh
```
Runs 12 unit tests verifying:
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
| `hub` | `--listen <ADDR>` | Starts the WebSocket Relay Hub (default: `0.0.0.0:8088`) |
| `node` | `--hub <URL>`, `--id <NAME>`, `--platform <TAG>` | Starts an agent node client connected to the Hub |
| `send` | `--hub <URL>`, `--to <TARGET>`, `--command <CMD>`, `--args <JSON>` | Sends a command to a specific node and prints execution output |
| `list` | `--hub <URL>` | Prints all currently online nodes registered with the Hub |
| `data` | `--hub <URL>`, `--to <TARGET>`, `--payload <STR>` | Sends a verified data payload to a target node |

---

## 6. Troubleshooting & Network Topologies

### Connecting Across the Open Internet:
- **Cloudflare Tunnel (Zero Port-Forwarding):**
  Expose your desktop or server Hub with:
  ```bash
  cloudflared tunnel --url http://127.0.0.1:8088
  ```
  Nodes on cellular Android or external laptops can connect directly to `wss://<tunnel-domain>/ws`.
- **LAN Wi-Fi Setup:**
  Ensure the Hub host's firewall allows incoming TCP traffic on port 8088:
  ```powershell
  # Windows Defender Firewall rule
  New-NetFirewallRule -DisplayName "OxideSwarm Hub" -Direction Inbound -LocalPort 8088 -Protocol TCP -Action Allow
  ```
