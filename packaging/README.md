# OxideSwarm Unified Background Packaging & Deployment Guide

Welcome to the unified packaging and background execution suite for **OxideSwarm** (`rusty-grid` / `oxideswarm`). This directory contains production-ready installation scripts, daemon configurations, wrappers, and validation tools designed to run the OxideSwarm distributed grid worker as an automated, persistent, silent background service across **macOS**, **Windows**, and **Android**.

> **Read first: three things every worker needs.** (1) The master's **cluster port, 8088**, as its address
> (`HOST:8088`; port 8080 is the dashboard and never works as a worker target). (2) A copy of the master's
> **`cluster.secret`** (on the master: `~/.oxideswarm/cluster.secret`, created on its first start), moved over a
> channel you trust, e.g. `scp user@master:~/.oxideswarm/cluster.secret ./cluster.secret` or a USB stick. A worker
> refuses to follow a remote master without it unless you pass `--insecure`/`-Insecure`. (3) The account that runs
> the service: every task the master sends runs with that account's rights, so the installers use an unprivileged one
> (not root/SYSTEM) by default. Details and threat model: [../docs/SECURITY.md](../docs/SECURITY.md).

---

## 1. Overview of OxideSwarm Background Packaging

An OxideSwarm compute grid relies on persistent worker nodes capable of receiving and executing compute jobs (distributed compilation, general compute, GPU workloads) without manual intervention. Running workers headlessly in the background presents platform-specific challenges:

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        OxideSwarm Cross-Platform Architecture                          │
├───────────────────┬──────────────────────────────────┬─────────────────────────────────┤
│      Platform     │        Subsystem / Engine        │     Key Challenges Solved       │
├───────────────────┼──────────────────────────────────┼─────────────────────────────────┤
│ macOS             │ Apple launchd (System Daemon)    │ Boot-time launch prior to login,│
│ (x86_64 / arm64)  │ /Library/LaunchDaemons           │ file descriptor ulimits, 24/7   │
│                   │ PID 1 supervision                │ crash auto-restart, no GUI popups│
├───────────────────┼──────────────────────────────────┼─────────────────────────────────┤
│ Windows           │ Service Control Manager (SCM)    │ Error 1053 dispatch timeout,   │
│ (x86_64)          │ Session 0 Headless Isolation     │ Session 0 isolation, zero-dep   │
│                   │ Native C# / WinSW Wrapper        │ C# csc.exe compiler, log rolls  │
├───────────────────┼──────────────────────────────────┼─────────────────────────────────┤
│ Android           │ Path A: Foreground Service (App) │ Android Doze mode, LMKD sweeps  │
│ (aarch64 / arm64) │ Path B: Termux Daemon + Boot     │ Phantom Process Killer (PPK),   │
│                   │ JNI In-Process / Wake Locks      │ CPU wake-locks, boot auto-start │
└───────────────────┴──────────────────────────────────┴─────────────────────────────────┘
```

### Core Design Philosophy
1. **Silent & Headless Execution**: Zero command windows, terminal popups, or user-session dependencies. Services run in background system domains or foreground service notifications.
2. **Auto-Start on Boot**: Workers automatically register and begin processing immediately when the operating system boots, surviving user logoffs or lock screens.
3. **Crash Resilience & Supervision**: Process supervisors automatically restart the worker upon unexpected exit or network failure, incorporating exponential or throttled backoff to prevent restart storms.
4. **Observable Logging**: Standard I/O streams are captured into log files (`RUST_LOG=info`); rotation depends on the platform wrapper (see each guide).
5. **Least Privilege**: no installer runs the worker as root or SYSTEM unless you ask for it explicitly (`--run-as-root`, `-ServiceAccount LocalSystem -AcceptSystemRisk`).
6. **Honest Verification**: the verify scripts check the installed definition, and the Windows/macOS installers also look for the `Registration accepted` log line, because a running service is not necessarily a connected worker.

---

## 2. Directory Structure

```
packaging/
├── README.md                            # Unified Master Guide (this file)
├── macos/                               # macOS LaunchDaemon Suite (R2)
│   ├── README.md                        # macOS architecture and operations guide
│   ├── com.oxideswarm.worker.plist      # Apple launchd property list definition
│   ├── install_mac_daemon.sh            # Root installer with permission & launchctl bootstrapping
│   ├── uninstall_mac_daemon.sh          # Daemon teardown and uninstaller script
│   └── verify_mac_daemon.sh             # 10-point test suite (plutil lint, schema, launchctl)
├── windows/                             # Windows Background Service Suite (R1)
│   ├── README.md                        # Windows architecture and operations guide
│   ├── ServiceWrapper.cs                # Zero-dependency C# ServiceBase dispatcher (built via csc.exe)
│   ├── winsw.xml                        # WinSW XML configuration template
│   ├── install_windows_service.ps1      # Production PowerShell installer (SCM, auto-start, recovery)
│   ├── uninstall_windows_service.ps1    # PowerShell uninstaller and service deregistration
│   └── verify_windows_service.ps1       # Comprehensive SCM verification and JSON diagnostic reporter
└── android/                             # Android Background Execution Suite (R3)
    ├── README.md                        # Android deployment and power management guide
    ├── EVALUATION_RUBRIC.md             # 10-point empirical verification rubric for auditors
    ├── app/                             # Path A: Production Foreground Service Android App (Kotlin)
    │   ├── build.gradle.kts             # Root Gradle build script
    │   ├── settings.gradle.kts          # Module settings
    │   ├── gradlew                      # Gradle wrapper script
    │   └── app/src/main/                # App source code (MainActivity, OxideWorkerService, JNI)
    │       ├── AndroidManifest.xml      # Foreground service types (specialUse|dataSync), wake locks
    │       └── java/com/oxideswarm/     # Kotlin service, wake lock management, boot receiver
    └── termux/                          # Path B: Termux Background Daemon & Supervisor
        ├── install_termux_daemon.sh     # One-click environment and daemon installer
        ├── start_worker.sh              # Multi-mode daemon supervisor with wake-lock holding
        └── start-oxideswarm             # Termux:Boot startup hook script
```

---

## 3. Platform Quickstart Guides

### 3.1 macOS: System LaunchDaemon

Deployed as an Apple System LaunchDaemon in `/Library/LaunchDaemons/` managed by `launchd` (PID 1).

#### Prerequisites
- macOS 10.15 Catalina through macOS 15 Sequoia (Apple Silicon M1-M4 and Intel x86_64).
- Root / Administrator access (`sudo`).
- Compiled worker binary (`rusty-grid` or `oxideswarm`).

#### Quickstart Commands

```bash
# 1. Build the release binary (if not already compiled). --features gpu-wgpu lets the worker use this
#    Mac's GPU (Metal); without it the worker advertises no GPU.
cargo build --release --bin rusty-grid --features gpu-wgpu

# 2. Install and launch the System LaunchDaemon (connects to the master's CLUSTER port 8088).
#    The worker runs as the user who ran sudo (or --user NAME), never as root; see packaging/macos/README.md
sudo bash packaging/macos/install_mac_daemon.sh --master 192.168.1.100:8088 --secret-file ~/cluster.secret

# Or connect over the internet using an Iroh P2P ticket from a master started with --p2p (ticket in a file, not on argv):
sudo bash packaging/macos/install_mac_daemon.sh --p2p-ticket-file ticket.txt --secret-file ~/cluster.secret

# 3. Dry-run installation (safe preview, no root, writes nothing)
bash packaging/macos/install_mac_daemon.sh --dry-run --master 192.168.1.100:8088 --secret-file ~/cluster.secret

# 4. Inspect daemon status in launchd
sudo launchctl print system/com.oxideswarm.worker

# 5. Tail live worker logs
tail -f /var/log/oxideswarm/worker.log

# 6. Uninstall the daemon
sudo bash packaging/macos/uninstall_mac_daemon.sh
# To also delete binaries and logs:
sudo bash packaging/macos/uninstall_mac_daemon.sh --purge
```

---

### 3.2 Windows: Background Service (SCM)

Windows Services run in **Session 0** under the Windows Service Control Manager (`services.exe`). To circumvent `Error 1053` (dispatcher timeout), OxideSwarm offers **dual wrapping architectures**:

1. **NativeWrapper (`ServiceWrapper.cs`) — Default & Zero-Dependency**:
   - Compiled on-the-fly using Windows' built-in .NET C# compiler (`csc.exe`).
   - Requires **zero external downloads, NuGet packages, or third-party dependencies**.
   - Handles asynchronous stdout/stderr stream redirection with 10 MB auto-rotation.
2. **WinSW (`winsw.xml`) — Jenkins Windows Service Wrapper**:
   - Industry-standard declarative XML wrapper.

#### Prerequisites
- Windows 10 (64-bit), Windows 11, or Windows Server 2016–2025.
- Elevated (Administrator) PowerShell terminal.
- Compiled `rusty-grid.exe` (cross-compiled from Mac/Linux via `bash build_windows_worker.sh` or built natively).

#### Quickstart Commands (Elevated PowerShell)

```powershell
# 1. Navigate to the Windows packaging directory
cd packaging\windows

# 2. Install using NativeWrapper (Default, zero external downloads). -Master is the CLUSTER port 8088, and the
#    secret file is the one copied from the master. The service runs as NT SERVICE\OxideSwarmWorker, not SYSTEM.
.\install_windows_service.ps1 -Master "192.168.1.100:8088" -SecretFile "C:\path\cluster.secret" -WorkerBin "..\..\target\x86_64-pc-windows-gnu\release\rusty-grid.exe"

# Or install using WinSW:
.\install_windows_service.ps1 -Master "192.168.1.100:8088" -SecretFile "C:\path\cluster.secret" -WorkerBin ".\rusty-grid.exe" -ServiceMethod "WinSW"

# Re-pair or upgrade an installed service (stops and replaces it) / connect through a P2P ticket file:
.\install_windows_service.ps1 -P2pTicketFile "C:\path\ticket.txt" -SecretFile "C:\path\cluster.secret" -Force

# 3. Verify service registration, auto-start, and Session 0 headless execution
.\verify_windows_service.ps1

# Export structured JSON verification report for CI/CD:
.\verify_windows_service.ps1 -Format Json

# 4. Service Control via standard Windows commands
Get-Service OxideSwarmWorker
Start-Service OxideSwarmWorker
Stop-Service OxideSwarmWorker

# 5. Tail service logs (look for "Registration accepted"; the installer waits for it and warns if it does not appear)
Get-Content C:\ProgramData\OxideSwarm\logs\worker.out.log -Wait -Tail 30

# 6. Uninstall service
.\uninstall_windows_service.ps1
# To purge all logs and data sandboxes:
.\uninstall_windows_service.ps1 -PurgeData
```

---

### 3.3 Android: Background Execution

Android's aggressive battery optimizations (Doze Mode, App Standby, Low Memory Killer Daemon, Linux Cgroups Freezer, and Android 12+ Phantom Process Killer) suspend or kill standard background processes within minutes. OxideSwarm provides two deployment solutions:

#### Path A: Native Android Foreground Service App (`packaging/android/app/`) — Production Recommended
A production-grade Kotlin app targeting API 24+ (Android 7.0 through Android 15+).
- **Dual-Engine Execution**:
  - **In-Process JNI (`liboxideworker.so`)**: runs the Tokio runtime and the worker loop inside the app process, so the worker itself is not a child process that the Android 12+ Phantom Process Killer (PPK) can reap. Tasks of type command / shell / compile are still spawned as child processes of the app and are exposed to PPK; only the built-in in-process task types are not. **You must build the library first**: `packaging/android/build_android_jni.sh --release` writes it to `packaging/android/app/app/src/main/jniLibs/arm64-v8a/`; an APK built without it has no worker.
  - **Managed Process Runner**: fallback that looks for a standalone `rusty-grid` binary (`/data/local/tmp/rusty-grid` or the app's files); it is not bundled.
  - **Known gap**: the app cannot hold the cluster secret yet, so it cannot join a master that requires one. Use Path B (Termux) until that is added.
- **Power Management Immunity**:
  - `foregroundServiceType="specialUse|dataSync"` with mandatory Android 14+ subtype justification property.
  - `PowerManager.PARTIAL_WAKE_LOCK` (`OxideSwarm::CpuWakeLock`) keeps the CPU awake during screen-off.
  - `WifiManager.WIFI_MODE_FULL_HIGH_PERF` (`OxideSwarm::WifiLock`) prevents Wi-Fi radio sleep.
  - `START_STICKY` automatic resurrection if reaped by temporary memory pressure.
  - `ACTION_BOOT_COMPLETED` receiver for automatic start on device boot.

##### Build & Install Path A:
```bash
# 0. Build the native worker library (needs the Android NDK and the aarch64-linux-android Rust target; see packaging/android/README.md)
packaging/android/build_android_jni.sh --release

cd packaging/android/app

# Compile a debug APK (the release APK is unsigned and cannot be installed as is)
./gradlew assembleDebug

# Install onto connected Android device via ADB
adb install -r app/build/outputs/apk/debug/app-debug.apk

# Launch Worker UI to configure coordinator address and start service
adb shell am start -n com.oxideswarm.worker/.MainActivity
```

#### Path B: Termux Background Daemon (`packaging/android/termux/`) — Developer / Automation
A lightweight script-based setup for developer workstations and headless Android appliances running Termux.
- Integrates `termux-wake-lock` to hold an Android partial wake lock.
- Automated boot startup via `Termux:Boot` hook script (`~/.termux/boot/start-oxideswarm`).
- Supervisor watchdog (`start_worker.sh`) providing process health checks, crash auto-restart, and log management.

##### Install & Run Path B (Inside Termux on Android):
```bash
# 1. Run the one-click installer
bash packaging/android/termux/install_termux_daemon.sh

# 2. Put the master's CLUSTER address (HOST:8088) in ~/.config/oxideswarm/rusty-grid.toml (master = "...")
#    and the copied secret in ~/.oxideswarm/cluster.secret (chmod 600), then start the worker daemon:
bash packaging/android/termux/start_worker.sh --daemon
#    (--master HOST:8088 overrides the config for one run; nothing is passed unless you give it)

# 3. Check daemon status and resource telemetry
bash packaging/android/termux/start_worker.sh --status

# 4. Stream live logs
bash packaging/android/termux/start_worker.sh --logs

# 5. Stop the daemon gracefully
bash packaging/android/termux/start_worker.sh --stop
```

---

## 4. Verification Scripts & Validation Commands

The scripts below check syntax, structure and (where possible) the installed definition. Some checks only run on
the target OS: the Windows scripts cannot run on macOS/Linux, so the Windows output further down is an illustration of
the format, not a recorded run.

### 4.1 Verification Commands Reference

| Platform | Verification Target | Command | Validation Scope |
|---|---|---|---|
| **macOS** | Property List Syntax | `plutil -lint packaging/macos/com.oxideswarm.worker.plist` | Apple XML DTD compliance, key-value typing |
| **macOS** | Daemon Test Suite | `bash packaging/macos/verify_mac_daemon.sh --non-root` | 11 automated checks: keys, limits, paths, service account (not root) |
| **macOS** | Shell Scripts | `bash -n packaging/macos/*.sh` | POSIX/Bash syntax, absence of parse errors |
| **Windows**| XML Configuration | Python ET / `xmllint packaging/windows/winsw.xml` | XML structure, well-formedness |
| **Windows**| PowerShell Integrity | Structural parser (`validate_ps1.py`) | Bracket/quote balance, cmdlet parameters |
| **Windows**| Live SCM Test | `.\packaging\windows\verify_windows_service.ps1` | SCM query, Auto startup, Session 0, recovery, not-SYSTEM identity, `Registration accepted` in the log |
| **Android**| Manifest XML | Python ET / `xmllint AndroidManifest.xml` | Android schema, permissions, FGS properties |
| **Android**| Termux Shell Scripts| `bash -n packaging/android/termux/*.sh` | Bash syntax validation, no parse errors |
| **Android**| Execution Rubric | Audit per `packaging/android/EVALUATION_RUBRIC.md` | 10-point empirical power-saving immunity |

### 4.2 Detailed Verification Results

#### macOS Verification
```bash
# 1. Apple Property List Linting
$ plutil -lint packaging/macos/com.oxideswarm.worker.plist
packaging/macos/com.oxideswarm.worker.plist: OK

# 2. Automated Daemon Verification Suite (Dry-Run / Non-Root)
$ bash packaging/macos/verify_mac_daemon.sh --non-root
[CHECK 1] Plist file existence and readability... [PASS] Found file
[CHECK 2] XML Syntax & structure linting (plutil -lint)... [PASS] OK
[CHECK 3] Validating required key 'Label'... [PASS] com.oxideswarm.worker
[CHECK 4] Validating required key 'ProgramArguments'... [PASS] Executable: /usr/local/bin/rusty-grid
[CHECK 5] Validating required key 'RunAtLoad'... [PASS] true
[CHECK 6] Validating required key 'KeepAlive'... [PASS] true
[CHECK 7] Validating key 'ThrottleInterval'... [PASS] 5s
[CHECK 8] Validating I/O log redirection keys... [PASS] StandardOutPath, StandardErrorPath
[CHECK 9] Validating ResourceLimits... [PASS] NumberOfFiles Soft: 65536, Hard: 65536
[CHECK 10] Validating execution environment... [PASS] WorkingDirectory = /var/lib/oxideswarm
[CHECK 10b] Validating service account (UserName, not root)... [PASS] UserName is the template placeholder
VERIFICATION RESULT: ALL CHECKS PASSED [OK] (Score: 11/11)
```
(Abbreviated from a run on macOS against the template plist.)

#### Windows Verification (illustrative output; not produced on this repository's macOS host)
```powershell
# SCM Verification Suite
PS > .\packaging\windows\verify_windows_service.ps1
[CHECK 1] Service Registration in SCM               [PASS] OxideSwarmWorker is registered
[CHECK 2] Service Startup Configuration             [PASS] StartMode is Automatic
[CHECK 3] Service Execution State                   [PASS] Status is Running (PID: 3412)
[CHECK 4] Binary Path Resolution                    [PASS] Target executable exists
[CHECK 5] SCM Failure Recovery Actions              [PASS] Service restart actions configured
[CHECK 6] Session 0 Headless Execution              [PASS] Process runs in Session 0 (Headless)
[CHECK 7] Log Directory and File Creation           [PASS] Active logs located in ProgramData
[SEC-01]  Service Runs Without SYSTEM Privileges    [PASS] Service runs as 'NT SERVICE\OxideSwarmWorker'
[NET-01]  Worker Registered With Master             [PASS] 'Registration accepted' found in worker.out.log
VERIFICATION RESULT: ALL CHECKS PASSED [OK]
```

---

## 5. Summary of Evaluation Rubrics

The evaluation of background execution reliability follows strict criteria documented in `packaging/android/EVALUATION_RUBRIC.md` and respective platform guides:

### 5.1 The 10-Point Android Background Immunity Matrix
To ensure an independent auditor can confirm technical compliance against Doze mode, cgroup freezing, and process killing:

1. **TC-01: Static Package & Manifest Audit** — Verifies presence of all 8 critical permissions (`INTERNET`, `FOREGROUND_SERVICE`, `WAKE_LOCK`, `RECEIVE_BOOT_COMPLETED`, `REQUEST_IGNORE_BATTERY_OPTIMIZATIONS`, `POST_NOTIFICATIONS`, `FOREGROUND_SERVICE_SPECIAL_USE`, `FOREGROUND_SERVICE_DATA_SYNC`) and Android 14 subtype properties.
2. **TC-02: Live OOM Score Adjustment Elevation Audit** — Inspects `/proc/$PID/oom_score_adj` to verify priority `<= 200` (`PERCEPTIBLE_APP`), preventing Low Memory Killer reaps.
3. **TC-03: Active Hardware CPU Wake-Lock Holding Audit** — Inspects `dumpsys power` to confirm `PARTIAL_WAKE_LOCK` (`OxideSwarm::CpuWakeLock`) is held, maintaining CPU clock cycles during screen off.
4. **TC-04: Low-Latency / High-Performance Wi-Fi Lock Audit** — Inspects `dumpsys wifi` to verify `WIFI_MODE_FULL_LOW_LATENCY` (with high-perf fallback) keeps network interfaces powered and responsive during screen-off compute.
5. **TC-05: Battery Optimization Exemption Whitelist Audit** — Validates `dumpsys deviceidle whitelist` and AppOps `RUN_ANY_IN_BACKGROUND` to confirm exemption against Deep Doze maintenance window deferrals.
6. **TC-06: Simulated Deep Doze Mode Endurance Test** — Uses `dumpsys deviceidle force-idle` to place the device into Deep Doze; confirms TCP socket and heartbeat stay active for 15+ minutes.
7. **TC-07: In-Doze Computational Task Dispatch & Execution** — Dispatches compute tasks while device is locked in Deep Doze; verifies instant execution without waiting for maintenance windows.
8. **TC-08: Linux Cgroups Freezer (`CachedAppOptimizer`) Exemption Verification** — Queries cgroup v1/v2 freeze states (`cgroup.freeze`) to confirm the worker process is never frozen by Android's `CachedAppOptimizer`.
9. **TC-09: Android 12+ Phantom Process Killer (PPK) Test** — Confirms that the in-process JNI worker loop has no child process while idle or running built-in tasks (`pgrep -P $PID` returns empty). It does not hold while command / shell / compile tasks are running, because those spawn child processes that PPK may kill.
10. **TC-10: Cold Device Reboot Auto-Start & Auto-Reconnect Verification** — Verifies direct boot / boot broadcast resurrection (`LOCKED_BOOT_COMPLETED` in BFU and `BOOT_COMPLETED` in AFU) without requiring manual app launch.

---

## 6. Comparison of Platform Packaging Capabilities

| Feature | macOS (`launchd`) | Windows (`SCM`) | Android Path A (`App`) | Android Path B (`Termux`) |
|---|---|---|---|---|
| **Service Engine** | `launchd` System Daemon | Service Control Manager | Foreground Service | Background Shell Supervisor |
| **Startup Phase** | System Boot (pre-login) | System Boot (pre-login) | Post-Boot Broadcast | Termux:Boot Hook |
| **Interactive UI** | Completely Headless | Session 0 Headless | Ongoing Status Notification | Headless or Terminal Log |
| **Crash Recovery** | Automatic (`KeepAlive`) | SCM Failure Actions | `START_STICKY` + Watchdog | Supervisor Auto-Restart |
| **File Limit (ulimit)** | Configured to 65,536 | OS System Default | N/A (App Sandbox) | Linux ulimit |
| **Network Resilience** | System Network Stack | System Network Stack | Wi-Fi Lock + P2P Iroh | Wi-Fi Lock via Termux API |
| **Power Protection** | macOS Sleep Prevention | Windows Power Settings | WakeLock + Doze Exemption | `termux-wake-lock` |
| **Log Management** | `/var/log/oxideswarm/` | 10 MB Auto-Roll Files | Monospace View + Logcat | State dir rolling logs |
