# OxideSwarm Windows Launchers (Headless Background Mode)

The Windows launcher suite enables high-performance computing machines (such as dedicated Windows desktop towers with discrete GPUs or workstation compute nodes) to run the OxideSwarm **worker** or **master** daemon in the background. The `.cmd` and `.vbs` files in this directory are user-friendly wrapper scripts that delegate all lifecycle management to `oxctl.ps1`, ensuring consistent execution logic and state tracking across all operations.

> **Note:** These scripts are designed for Windows PowerShell 5.1+ and the Windows Command Prompt (`cmd.exe`). Log outputs are written to `%USERPROFILE%\.oxideswarm\run\`.

---

## 1. Capabilities and Architecture

- **Minimal Overhead & Zero External Runtimes**: Built entirely in Rust. Does not require Python, Node.js, Java, or Electron runtimes. An idle worker consumes approximately 7–15 MB of working set RAM and virtually 0% CPU. Run `status_worker.cmd` to inspect the exact real-time RAM usage of the active process.
- **Silent Background Execution**: Runs completely headless without intrusive console windows popping up or remaining open on your desktop (leveraging `WScript.Shell.Run` and PowerShell hidden window execution).
- **Automatic Reconnection & Resilience**: Workers continuously attempt reconnection using exponential backoff if the Master is temporarily restarted, unreachable, or undergoing maintenance.
- **Hardware GPU Acceleration (`gpu-wgpu`)**: The worker automatically detects local graphics hardware if the executable was compiled with the `gpu-wgpu` feature (`cargo build --release --bin oxideswarm --features gpu-wgpu` or `build_windows_worker.sh --gpu`). The `GPU:` entry in `worker.log` indicates whether hardware acceleration is active. If built without `gpu-wgpu`, no GPU is advertised to the cluster (CPU is never misrepresented as a GPU). Pass `--no-gpu` if you wish to reserve the local graphics card exclusively for desktop or gaming tasks.
- **PID-Scoped Process Lifecycle**: The launcher never terminates processes by executable name (`taskkill /IM ...`). It strictly tracks process IDs via PID files in `%USERPROFILE%\.oxideswarm\run\<role>.pid` and terminates only the child tree it spawned (`taskkill /F /T /PID <pid>`). A background Windows Service (`OxideSwarmWorker`) or an independently started instance will never be interrupted.
- **Structured Logging & Rotation**: Stdout and stderr streams are written to `%USERPROFILE%\.oxideswarm\run\<role>.log` and `<role>.err.log`. Upon each restart, existing logs are preserved by renaming them to `.old`.

---

## 2. Network Prerequisites: Cluster Port 8088 & Cluster Secret

1. **Cluster Port (`IP:8088`)**: The Master cluster coordinator listens on TCP port 8088 (`0.0.0.0:8088`). Workers connect exclusively to port 8088 (or pass `auto` for local UDP broadcast discovery). Port 8080 is reserved for the HTTP Web Observability Dashboard and must never be used as a worker target.
2. **Cluster Secret**: The Master generates `%USERPROFILE%\.oxideswarm\cluster.secret` on its first launch. You must securely copy this file to the worker machine at `%USERPROFILE%\.oxideswarm\cluster.secret` (using SCP, SSH, or an encrypted USB drive; never send secrets via unencrypted chat or email). Without a valid secret file, the worker will refuse to join remote or auto-discovered masters (exit code 6).
3. **Security Details**: For the comprehensive threat model and key rotation procedures, refer to [docs/SECURITY.md](../docs/SECURITY.md).

---

## 3. 1-Click Launcher Reference

| File | Description | Execution Method |
| :--- | :--- | :--- |
| **`run_worker_silent.vbs`** | **Launches worker in background** (Master = `auto`); displays a graphical prompt if startup fails | Double-click |
| `start_worker.cmd` | Starts worker via CLI: `start_worker.cmd [IP:8088\|auto] [--no-gpu ...]` | Command line or double-click |
| `run_master_silent.vbs` | Launches master in background (dashboard bound to loopback only) | Double-click |
| `start_master.cmd` | Starts master via CLI: `start_master.cmd [--lan-dashboard] [--p2p] [--no-worker]` | Command line |
| `switch_role.cmd` | Interactive role switcher: `switch_role.cmd master \| worker IP:8088 \| status \| stop` | Command line or double-click menu |
| `status_worker.cmd` | Inspects running launcher processes, actual working set RAM, service status, and log tail | Double-click or command line |
| `stop_worker.cmd` | Terminates **only** the processes started by these launchers (tracked by PID file) | Double-click or command line |
| `install_service.cmd` | Installs a persistent Windows Service running on system startup: `install_service.cmd IP:8088 [secret-path]` | Double-click / CLI (requests UAC elevation) |
| `oxctl.ps1` | Core PowerShell execution engine behind all the above launchers | Internal engine (do not run directly) |
| `..\connect_remote.cmd` | Pairs with a Master on an **external network** using a P2P ticket and cluster secret | Double-click |

> **Process Containment Note:** To stop a persistent Windows Service installed via `install_service.cmd`, execute `Stop-Service OxideSwarmWorker` in an Administrator PowerShell console.

---

## 4. Running a Master on Windows

Running `start_master.cmd` executes `master --listen 0.0.0.0:8088 --web-ui-addr 127.0.0.1:8080`:
- **Cluster Port 8088**: Bound to all interfaces (`0.0.0.0:8088`) so LAN workers can connect (authenticated via shared secret).
- **Dashboard Port 8080**: Bound strictly to loopback (`127.0.0.1:8080`) by default so web telemetry is accessible only on the local machine.
- **LAN Dashboard (`--lan-dashboard`)**: Exposes the Web UI to the LAN (`0.0.0.0:8080`) so phones and other machines can view cluster status. When enabled, authentication requires the dashboard token printed by `oxideswarm secret show --dashboard-token`.
- **P2P Endpoint (`--p2p`)**: Enables Iroh QUIC NAT traversal and writes a P2P connection ticket to `%USERPROFILE%\.oxideswarm\master_ticket.txt` for remote nodes.
- **Cluster Secret**: Generated automatically on the first start at `%USERPROFILE%\.oxideswarm\cluster.secret`. Copy this file to every worker node.

---

## 5. Configuration & Environment Variables

- **Master Address**: Defaults to `auto` (LAN UDP broadcast discovery on port 8089) if omitted. Explicitly specifying `HOST:8088` is recommended for deterministic routing.
- **Node Identifier**: Defaults to `<computername>-worker` (customizable via `-WorkerName` in `oxctl.ps1`).
- **GPU Acceleration**: Hardware GPU is enabled if `oxideswarm.exe` contains the `gpu-wgpu` feature. Use `--no-gpu` to disable GPU advertisement.
- **State Directory (`OXIDESWARM_HOME`)**: Defaults to `%USERPROFILE%\.oxideswarm`. Setting `OXIDESWARM_HOME` changes the location for PID files, logs, and tickets.
- **Custom Ports**:
  - `OXIDESWARM_CLUSTER_PORT` (default: `8088`)
  - `OXIDESWARM_DASHBOARD_PORT` (default: `8080`)
