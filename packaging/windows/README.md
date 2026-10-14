# OxideSwarm Windows Background Service Packaging

Production-grade deployment and automation tooling for running the **OxideSwarm** distributed grid worker (`rusty-grid.exe` / `oxideswarm.exe`) as a genuine, headless Windows Service managed by the Windows Service Control Manager (SCM).

> **Before you install.** A worker needs (1) the master's **cluster address `HOST:8088`** (8080 is the dashboard and
> never works), or a P2P ticket from a master started with `--p2p`, and (2) a copy of the master's **`cluster.secret`**
> (on the master: `~/.oxideswarm/cluster.secret`; copy it with `scp` or a USB stick, never chat/e-mail). The installer
> refuses to continue without the secret unless you pass `-Insecure`. The service runs as a low-privilege virtual
> account, **not SYSTEM**, because every task the master sends runs with the service's identity. See
> [../../docs/SECURITY.md](../../docs/SECURITY.md). The scripts in this folder were written for Windows PowerShell 5.1
> and have not been run by any automated test.

---

## 1. Architectural Overview

### 1.1 The Service Control Manager & Error 1053
Windows Services run under the management of the **Service Control Manager (SCM)** (`services.exe`). SCM requires every registered service binary to immediately connect to a named pipe and invoke `StartServiceCtrlDispatcherW`. 

Because standard console applications (including Tokio-based Rust executables like `rusty-grid.exe`) do not call the Win32 SCM dispatcher APIs within the default 30-second timeout, attempting to register them directly via `sc.exe create` or `New-Service` causes SCM to forcefully kill the process with:
```text
Error 1053: The service did not respond to the start or control request in a timely fashion.
```

### 1.2 Dual Wrapping Architecture
To guarantee maximum flexibility, reliability, and zero external friction, OxideSwarm provides **dual wrapping support**:

1. **NativeWrapper (`ServiceWrapper.cs`) — Default & Zero-Dependency**:
   - A high-performance, compact C# `ServiceBase` dispatcher.
   - Compiled on-the-fly during installation using the built-in Microsoft .NET Framework compiler (`csc.exe` located in `%WINDIR%\Microsoft.NET\Framework64\v4.0.30319\csc.exe`).
   - **Zero external downloads, NuGet packages, or third-party tools required**. Present and functional on 100% of modern Windows systems (Windows 10, 11, Server 2016-2025).
   - Manages asynchronous stdout/stderr stream redirection to log files with automatic log rotation (10 MB roll threshold).
   - Implements process tree supervision: if `rusty-grid.exe` exits unexpectedly, the wrapper signals SCM with a non-zero exit code to trigger automatic crash recovery.
   - On service stop or system shutdown, performs clean process tree termination via `taskkill /T /F` to ensure all child tasks and compilation jobs terminate cleanly.

2. **WinSW (`winsw.xml`) — Industry Standard Alternative**:
   - The battle-tested [Windows Service Wrapper](https://github.com/winsw/winsw) maintained under the Jenkins project.
   - Controlled completely via declarative XML (`winsw.xml`).
   - Provides rolling file logs, delayed automatic start, and native process lifecycle management.

### 1.3 Headless Session 0 Isolation
Windows Services execute exclusively in **Session 0**, which has been completely isolated from user desktop sessions (`WinSta0`) since Windows Vista. Both wrapping solutions instantiate `rusty-grid.exe` with:
- `CreateNoWindow = true`
- `WindowStyle = ProcessWindowStyle.Hidden`
- `UseShellExecute = false`

The worker operates completely silently in the background from system boot, surviving user logins, logouts, and lock screens without displaying any console or command prompt windows.

### 1.4 Service identity and where secrets live
* **Account.** `-ServiceAccount VirtualAccount` (default) registers the service as `NT SERVICE\OxideSwarmWorker`: no password, no network identity, only the rights granted to it. Tasks run with the same identity, so they cannot touch other users' profiles or system files by default. `-ServiceAccount LocalService` is the alternative; `LocalSystem` (full control of the PC for every task) needs `-ServiceAccount LocalSystem -AcceptSystemRisk`.
* **Config and secret.** The installer writes `rusty-grid.toml` (master address or P2P ticket, secret path) and a copy of `cluster.secret` into `C:\ProgramData\OxideSwarm\` and restricts both to SYSTEM, Administrators and the service account. The ticket is **not** put on the service command line or in the registry (older installers did both, and the JSON quotes did not survive Windows argument parsing, so the worker never accepted the ticket); the wrapper only runs `worker --config <that file>`. Log and sandbox directories are granted modify rights to the service account.
* **Re-pairing / upgrading.** Run the installer again with `-Force`: it stops and deletes the old service *before* it replaces any file, then rewrites the config. A leftover machine-wide `OXIDESWARM_P2P_TICKET` from older installs is cleared by both the installer and the uninstaller.

---

## 2. File Manifest

The `packaging/windows/` directory contains:

| File | Purpose |
|---|---|
| `install_windows_service.ps1` | Production PowerShell installer script. Handles elevation, deploys binaries, compiles the native wrapper or configures WinSW, sets up logs/config, configures SCM auto-start and failure recovery, and launches the service. |
| `uninstall_windows_service.ps1` | Clean uninstallation script. Gracefully stops the service, deregisters it from SCM, removes binaries, and cleans configuration (preserves logs unless `-PurgeData` is specified). |
| `verify_windows_service.ps1` | Comprehensive verification test suite. Queries SCM, CIM `Win32_Service`, verifies `StartupType = Auto`, checks Session 0 headless execution, validates binary paths, and outputs structured reports. |
| `ServiceWrapper.cs` | Standalone C# `ServiceBase` source code compileable via built-in `csc.exe`. |
| `winsw.xml` | Declarative XML configuration template for WinSW deployments. |
| `README.md` | Architecture, operations, and technical reference documentation. |

---

## 3. Quick Start Guide

### Prerequisites
- Windows 10 (64-bit), Windows 11, or Windows Server 2016+
- PowerShell 5.1 (built-in) or PowerShell 7+
- Administrator privileges
- Compiled `rusty-grid.exe` binary

### Step 1: Obtain the Worker Binary
You can cross-compile `rusty-grid.exe` from macOS or Linux using the repository's cross-compilation script:
```bash
# On macOS / Linux host:
bash build_windows_worker.sh --release
# Output binary: target/x86_64-pc-windows-gnu/release/rusty-grid.exe
```
Or build natively on Windows:
```cmd
cargo build --release --bin rusty-grid --features gpu-wgpu
```
(`--features gpu-wgpu` / `build_windows_worker.sh --gpu` is what lets the worker use the graphics card; without it no GPU is advertised. The cross-build for it is not tested here.)

### Step 2: Install as a Windows Service
Open an **Elevated (Administrator) PowerShell** terminal, navigate to `packaging\windows\`, and run:
```powershell
.\install_windows_service.ps1 -Master "192.168.1.100:8088" -SecretFile "C:\path\to\cluster.secret" -WorkerBin ".\rusty-grid.exe"
```
For a master on another network (started with `--p2p`), save its ticket JSON to a file and use
`-P2pTicketFile C:\path\ticket.txt` instead of `-Master` (or run `connect_remote.cmd` from the repository root, which does
this for you). Add `-Force` to replace an installed service. The installer waits up to 20 s for `Registration accepted`
in `worker.out.log` and warns if it does not appear: a *Running* service is not necessarily a connected worker.
*Note: If launched from a non-elevated PowerShell console, the script will automatically prompt for UAC elevation.*

### Step 3: Verify the Service
Run the automated verification suite:
```powershell
.\verify_windows_service.ps1
```
For automated CI/CD pipelines, request JSON output:
```powershell
.\verify_windows_service.ps1 -Format Json
```

### Step 4: Uninstall the Service
To remove the service and clean up binaries:
```powershell
.\uninstall_windows_service.ps1
```
To also purge all log files and data sandboxes:
```powershell
.\uninstall_windows_service.ps1 -PurgeData -Force
```

---

## 4. Script Parameter References

### 4.1 `install_windows_service.ps1`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `-InstallDir` | String | `C:\Program Files\OxideSwarm` | Destination directory for application binaries and service wrapper. |
| `-Master` | String | `""` (required unless a ticket is given) | The Master's **cluster** address `HOST:8088`. Port 8080 is the dashboard; the installer warns if you give it. |
| `-P2pTicketFile` | String | `""` | File with the P2P ticket JSON of a master started with `--p2p` (read once, never put on a command line). |
| `-P2pTicket` | String | `""` | Ticket given inline (visible in the process list of the installer; prefer `-P2pTicketFile`). |
| `-SecretFile` | String | `""` | Cluster secret copied from the master. Default lookup: `$env:OXIDESWARM_SECRET_FILE`, then `%USERPROFILE%\.oxideswarm\cluster.secret`. Required for any remote or P2P master. |
| `-Insecure` | Switch | `False` | Install without a secret: the worker obeys any host that answers as the master. Not recommended. |
| `-ServiceAccount` | String | `VirtualAccount` | `VirtualAccount` (`NT SERVICE\<ServiceName>`), `LocalService`, or `LocalSystem` (needs `-AcceptSystemRisk`). |
| `-AcceptSystemRisk` | Switch | `False` | Confirms that every task may run with full control of this PC (only with `-ServiceAccount LocalSystem`). |
| `-WorkerBin` | String | `""` (auto-detected) | Explicit path to source `rusty-grid.exe`. Auto-detects in target build folders if omitted. |
| `-ServiceMethod` | String | `NativeWrapper` | Service wrapping engine: `NativeWrapper` (zero external dependencies) or `WinSW`. |
| `-ServiceName` | String | `OxideSwarmWorker` | Service name registered in SCM. |
| `-DisplayName` | String | `OxideSwarm Distributed Grid Worker` | Human-readable service display name. |
| `-WorkerName` | String | `""` (uses hostname) | Custom worker node identifier advertised to the grid. |
| `-Cores` | Int32 | `0` (auto-detect) | Hardware core count override. |
| `-RamMb` | Int64 | `0` (auto-detect) | Host RAM in Megabytes override. |
| `-Gpu` | Switch | `False` | Insist on a real GPU. It is advertised only if the binary was built with `--features gpu-wgpu` and an adapter starts; otherwise the worker advertises none and logs a warning (the stock binary has no wgpu engine). The worker also looks for a GPU without this switch. |
| `-NoGpu` | Switch | `False` | Never advertise a GPU (also skips looking for one). |
| `-SimulateGpu` | Switch | `False` | Test mode: advertise a simulated GPU whose tasks run on the CPU and are labelled as simulated. |
| `-GpuName` | String | `""` | Custom GPU model string (e.g. `"NVIDIA RTX 4090"`). |
| `-MaxConcurrency` | Int32 | `0` (core count) | Maximum concurrent task executions permitted. |
| `-HeartbeatInterval` | Int32 | `3` | Heartbeat interval in seconds advertised to Master. |
| `-LogDir` | String | `C:\ProgramData\OxideSwarm\logs` | Destination directory for worker log files. |
| `-DataDir` | String | `C:\ProgramData\OxideSwarm\data` | Sandbox scratch base directory for tasks. |
| `-ConfigPath` | String | `C:\ProgramData\OxideSwarm\rusty-grid.toml` | Path for generated TOML configuration. |
| `-WinSWBin` | String | `""` | Path to WinSW executable if `WinSW` method is selected. |
| `-StartImmediately` | Boolean | `$true` | Immediately start the service upon successful registration. |
| `-Force` | Switch | `False` | Stop and delete an existing service of the same name before replacing files (upgrade / re-pair). Without it an existing service makes the installer stop with nothing changed. |
| `-NoElevate` | Switch | `False` | Suppress automatic UAC prompt; fail if not elevated. |

### 4.2 `uninstall_windows_service.ps1`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `-ServiceName` | String | `OxideSwarmWorker` | Name of the service to stop and remove. |
| `-InstallDir` | String | `C:\Program Files\OxideSwarm` | Installation directory containing wrapper binaries. |
| `-LogDir` | String | `C:\ProgramData\OxideSwarm\logs` | Directory containing log files. |
| `-DataDir` | String | `C:\ProgramData\OxideSwarm\data` | Directory containing task sandbox storage. |
| `-ConfigPath` | String | `C:\ProgramData\OxideSwarm\rusty-grid.toml` | Path to configuration file. |
| `-PurgeData` | Switch | `False` | If specified, permanently deletes logs, configs, and sandboxes. |
| `-Force` | Switch | `False` | Force-terminates processes if service does not stop within 10s. |
| `-NoElevate` | Switch | `False` | Suppress automatic UAC prompt; fail if not elevated. |

### 4.3 `verify_windows_service.ps1`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `-ServiceName` | String | `OxideSwarmWorker` | Service to inspect and validate. |
| `-LogDir` | String | `C:\ProgramData\OxideSwarm\logs` | Expected location of worker log files. |
| `-Format` | String | `Text` | Report format: `Text` (ANSI colored) or `Json` (machine-readable). |
| `-TimeoutSec` | Int32 | `5` | Maximum seconds to wait if service is transitioning. |
| `-RequireRunning` | Boolean | `$true` | Fail verification if service is Stopped. |

---

## 5. Filesystem & Installation Layout

Standard enterprise installation establishes the following layout:

```text
C:\Program Files\OxideSwarm\               <-- Read/Execute for Users, Full for Admins
├── bin\
│   ├── rusty-grid.exe                     <-- Primary compiled worker executable
│   └── oxideswarm.exe                     <-- Brand alias (copy/link of rusty-grid.exe)
├── OxideSwarmService.exe                  <-- Compiled native SCM wrapper
├── ServiceWrapper.cs                      <-- Retained C# wrapper source
└── OxideSwarmWorker.xml                   <-- WinSW configuration (if using WinSW)

C:\ProgramData\OxideSwarm\                 <-- Writable by SYSTEM & Administrators
├── rusty-grid.toml                        <-- Active runtime configuration (may hold a P2P ticket; ACL-restricted)
├── cluster.secret                         <-- Copy of the cluster secret (ACL-restricted)
├── logs\
│   ├── worker.out.log                     <-- Standard output log stream
│   ├── worker.err.log                     <-- Standard error log stream
│   └── worker.out.log.1                   <-- Rotated log archive segment
└── data\
    └── sandboxes\                         <-- Temporary task scratch directories
```

---

## 6. Service Management & Operations

### Standard Management Commands

```powershell
# Query service status via PowerShell
Get-Service -Name OxideSwarmWorker

# Query service details via CIM
Get-CimInstance Win32_Service -Filter "Name='OxideSwarmWorker'" | Select-Object Name, State, StartMode, PathName, ProcessId

# Start the service
Start-Service -Name OxideSwarmWorker

# Stop the service
Stop-Service -Name OxideSwarmWorker

# Restart the service
Restart-Service -Name OxideSwarmWorker

# Low-level query via sc.exe
sc.exe qc OxideSwarmWorker
sc.exe query OxideSwarmWorker
sc.exe qfailure OxideSwarmWorker
```

### Inspecting Logs
Standard output and standard error from the worker's Tokio and `tracing` subsystems are captured into `C:\ProgramData\OxideSwarm\logs\`:

```powershell
# Follow worker logs in real-time
Get-Content -Path "C:\ProgramData\OxideSwarm\logs\worker.out.log" -Tail 50 -Wait

# View error log entries
Get-Content -Path "C:\ProgramData\OxideSwarm\logs\worker.err.log" -Tail 50
```

### Windows Event Log
Lifecycle events (starts, unexpected terminations, SCM failures) are logged to the Windows **Application Event Log** under Source `OxideSwarmWorker`:
```powershell
Get-WinEvent -FilterHashtable @{LogName='Application'; ProviderName='OxideSwarmWorker'} -MaxEvents 20
```

### Crash Recovery & Auto-Restart
The service installer configures SCM failure recovery actions via `sc.exe failure`:
- **First Failure**: Restart service after 5,000 ms (5 seconds).
- **Second Failure**: Restart service after 5,000 ms (5 seconds).
- **Subsequent Failures**: Restart service after 5,000 ms (5 seconds).
- **Reset Failure Count**: 86,400 seconds (1 day).

If the underlying `rusty-grid.exe` process is terminated unexpectedly or encounters a panic, the wrapper reports the failure to SCM, and Windows will automatically respawn the service after 5 seconds.

### Interactive Debug Mode
To troubleshoot network connectivity or task execution without starting the Windows Service:
```cmd
"C:\Program Files\OxideSwarm\OxideSwarmService.exe" --console
```
This spawns the worker in the current console window, attaching stdout/stderr directly to the terminal until you press Enter or Ctrl+C.

---

## 7. Verification Matrix & Acceptance Criteria Compliance

The verification script (`verify_windows_service.ps1`) systematically executes the following tests:

| Test ID | Check Name | Target Subsystem | Verification Criterion |
|---|---|---|---|
| `SCM-01` | SCM Registration Exists | `Get-Service` | Confirms service is present in SCM registry. |
| `SCM-02` | Automatic Startup | `Win32_Service` | Asserts `StartMode == "Auto"`. |
| `SCM-03` | Service State (Running) | `Win32_Service` | Asserts `State == "Running"`. |
| `SCM-04` | Service Binary Path | Filesystem | Asserts `PathName` resolves to an existing executable. |
| `SCM-05` | Low-Level AUTO_START | `sc.exe qc` | Confirms `START_TYPE : 2 AUTO_START`. |
| `SCM-06` | SCM Crash Recovery | `sc.exe qfailure` | Confirms restart actions are configured. |
| `ISO-01` | Session 0 Headless Execution | `Get-Process` | Asserts `SessionId == 0` and `MainWindowHandle == 0`. |
| `LOG-01` | Log Output Streams | Filesystem | Confirms log directory exists and log files are generated. |
| `SEC-01` | Service Identity | `Win32_Service` | Warns when the service runs as LocalSystem (every task would inherit it). |
| `NET-01` | Worker Registered | `worker.out.log` | Looks for `Registration accepted` after the most recent connection/authentication error. A Running service alone proves nothing. |
