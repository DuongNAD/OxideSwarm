# OxideSwarm Android Worker Deployment & Architecture Guide

Welcome to the complete deployment and background execution guide for **OxideSwarm** on Android. This package enables standard Android smartphones and tablets (ARM64-v8a, API 24+ / Android 7.0 through Android 15+) to participate as active computational worker nodes in an OxideSwarm distributed grid.

---

## 1. The Challenge of Android Background Execution

Android's power-management architecture is designed to restrict and suspend background applications to preserve battery life and memory:

```
┌────────────────────────────────────────────────────────────────────────┐
│                   Android OS Power Management Obstacles                │
├────────────────────────────────────────────────────────────────────────┤
│ 1. Screen Off ──► Shallow Doze (Network paused, syncs deferred)        │
│ 2. Stationary ──► Deep Doze (Network blocked, CPU wake locks ignored)  │
│ 3. Background ──► Linux Cgroup Freezer (CachedAppOptimizer SIGSTOP)    │
│ 4. Heavy CPU  ──► Android 12+ Phantom Process Killer (SIGKILL signal 9)│
│ 5. Low RAM    ──► LMKD terminates cached processes (oom_score_adj 900)│
│ 6. OEM Power  ──► Custom vendor task killers (Samsung, Xiaomi, etc.)   │
└────────────────────────────────────────────────────────────────────────┘
```

Without specialized packaging, a background worker process will be frozen or terminated within **3 to 15 minutes** of turning off the phone screen.

OxideSwarm solves these challenges through two robust deployment paths:
* **Path A (Production Recommended):** Native Android Foreground Service App (`packaging/android/app/`).
* **Path B (Developer / Automation):** Termux Background Daemon (`packaging/android/termux/`).

---

## 2. Path A: Native Android Foreground Service App

Located in `packaging/android/app/`, this is a production-grade, turnkey Android application written in Kotlin targeting modern Android (compileSdk 34, minSdk 24).

### 2.1 Architectural Blueprint

```
┌──────────────────────────────────────────────────────────────────────────┐
│                   Android Process (com.oxideswarm.worker)                │
│                   oom_score_adj <= 200 (PERCEPTIBLE_APP)                 │
├──────────────────────────────────────────────────────────────────────────┤
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │ MainActivity (Kotlin / Material Design 3)                          │  │
│  │ - Coordinator Address, Worker Name, Cores & RAM Limit              │  │
│  │ - One-Tap Battery Exemption Intent & OEM Deep Links                │  │
│  │ - Live Monospace Terminal Log Streaming                            │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                    ▲                                     │
│                                    │ Service Binding                     │
│                                    ▼                                     │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │ OxideWorkerService (Persistent Foreground Service)                 │  │
│  │ - Ongoing Status Bar Notification (Live task telemetry)            │  │
│  │ - PowerManager.PARTIAL_WAKE_LOCK ("OxideSwarm::CpuWakeLock")       │  │
│  │ - WifiManager.WIFI_MODE_FULL_HIGH_PERF ("OxideSwarm::WifiLock")    │  │
│  │ - Android 14+ foregroundServiceType="specialUse|dataSync"          │  │
│  │ - START_STICKY auto-resurrection on memory normalization           │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                    ▲                                     │
│                                    │ Execution Dispatch                  │
│                                    ▼                                     │
│  ┌─────────────────────────────────┴──────────────────────────────────┐  │
│  │ Dual-Engine Runner Layer                                           │  │
│  │                                                                    │  │
│  │  [Primary] OxideWorkerBridge (In-Process JNI)                      │  │
│  │  - Loads 'liboxideworker.so' via dlopen                            │  │
│  │  - Executes Tokio Multi-Thread Runtime on native POSIX pthread     │  │
│  │  - Zero child processes -> 100% IMMUNE to Phantom Process Killer   │  │
│  │  - Full SELinux W^X (Write XOR Execute) compliance                 │  │
│  │                                                                    │  │
│  │  [Fallback] ManagedProcessRunner (Binary Process Supervision)      │  │
│  │  - Executes standalone 'rusty-grid' ELF binary                     │  │
│  │  - Concurrent stdout/stderr stream piping & crash auto-restart     │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                    ▲                                     │
│                                    │ ACTION_BOOT_COMPLETED               │
│  ┌─────────────────────────────────┴──────────────────────────────────┐  │
│  │ BootCompletedReceiver (Auto-start on System Boot)                  │  │
│  └────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Why In-Process JNI Wins
1. **Zero Phantom Child Processes:** When an Android app spawns a child process using `ProcessBuilder` or `fork()`, Android 12+ monitors its CPU usage. If it consumes heavy CPU in the background, `ActivityManagerService` terminates it with `SIGKILL` (Phantom Process Killer). With In-Process JNI, the Rust runtime lives entirely on a native thread within the JVM process. There are **no child processes**, completely bypassing PPK.
2. **SELinux W^X Compliance:** Android 10+ strictly prohibits executing binaries from writable app directories (`/data/data/...`). JNI libraries (`liboxideworker.so`) reside in the OS-managed read-only `nativeLibraryDir`, adhering to SELinux security rules.
3. **Elevated Priority Inheritance:** The Rust Tokio runtime inherits the Foreground Service's `oom_score_adj <= 200`.

### 2.3 Compiling & Installing the APK

#### Building from CLI:
```bash
cd packaging/android/app

# Clean and assemble release APK
./gradlew assembleRelease

# The generated APK will be at:
# app/build/outputs/apk/release/app-release-unsigned.apk
```

#### Installing via ADB:
```bash
adb install -r app/build/outputs/apk/debug/app-debug.apk
# Launch MainActivity
adb shell am start -n com.oxideswarm.worker/.MainActivity
```

### 2.4 First-Time Configuration Walkthrough
1. **Grant Notification Permission:** On Android 13+, tap "Allow" when prompted for notification permissions (required for foreground service visibility).
2. **Exempt from Battery Optimizations:** Tap the **"Disable Battery Optimization"** button. Android will display a system dialog asking to let the app always run in the background. Tap **"Allow"**.
3. **Configure Coordinator Address:** Enter the IP address and port of your OxideSwarm Master node (e.g. `192.168.1.100:8080`).
4. **Tap "Start Worker":** The worker connects immediately, displays an ongoing status notification, and begins heartbeating every 3 seconds.

---

## 3. Path B: Termux Background Daemon

For automated test environments, headless mobile clusters, or power users, OxideSwarm provides a complete suite of scripts to run `rusty-grid` directly inside Termux.

### 3.1 Termux Files & Components
* `install_termux_daemon.sh`: One-click setup script inside Termux that installs packages, sets binary permissions, installs `termux-api` wake-lock hooks, configures Termux:Boot, and guides battery optimization.
* `start_worker.sh`: Supervisor script that holds `termux-wake-lock`, manages background daemon redirection, monitors process health, and auto-restarts on network drops.
* `start-oxideswarm`: Termux:Boot script located at `~/.termux/boot/start-oxideswarm` to automatically boot the worker on device startup.

### 3.2 Prerequisites for Termux
1. **Install Termux from F-Droid:** (Do NOT use the outdated Google Play release).
   * Termux: [https://f-droid.org/packages/com.termux/](https://f-droid.org/packages/com.termux/)
   * Termux:Boot: [https://f-droid.org/packages/com.termux.boot/](https://f-droid.org/packages/com.termux.boot/)
   * Termux:API: [https://f-droid.org/packages/com.termux.api/](https://f-droid.org/packages/com.termux.api/)

### 3.3 Installation Instructions

1. **Push the cross-compiled binary to your phone:**
   ```bash
   adb push target/aarch64-linux-android/release/rusty-grid /sdcard/Download/
   adb push packaging/android/termux/*.sh /sdcard/Download/
   adb push packaging/android/termux/start-oxideswarm /sdcard/Download/
   ```

2. **Open Termux and run the installer:**
   ```bash
   # Grant storage permission if needed
   termux-setup-storage

   # Move installer scripts to home
   cp /sdcard/Download/install_termux_daemon.sh ~/
   chmod +x ~/install_termux_daemon.sh
   bash ~/install_termux_daemon.sh
   ```

3. **Managing the Termux Worker:**
   ```bash
   # Start worker in background
   start_worker.sh --daemon --master 192.168.1.100:8080

   # Check worker status, PID, and battery/temperature
   start_worker.sh --status

   # Tail live daemon logs
   start_worker.sh --logs

   # Stop background worker and release wake-lock
   start_worker.sh --stop
   ```

### 3.4 Crucial Notice: Android 12+ Phantom Process Killer
If running `rusty-grid` under Termux on Android 12, 13, 14, or 15, Android's `ActivityManagerService` limits background child processes and may send `SIGKILL` after sustained CPU utilization.

To permanently disable this limit, connect the phone to your PC via USB and execute:
```bash
adb shell "/system/bin/device_config put activity_manager max_phantom_processes 2147483647"
```

---

## 4. Mobile Hardware Telemetry

The OxideSwarm worker automatically detects and advertises mobile-specific hardware telemetry in its heartbeats:

| Telemetry Metric | Kernel / System Path Queried | Description |
| :--- | :--- | :--- |
| **Battery Percentage** | `/sys/class/power_supply/battery/capacity` | Current state of charge (0–100%) |
| **Charging Status** | `/sys/class/power_supply/battery/status` | `Charging`, `Discharging`, `Full` |
| **Thermal Throttling** | `/sys/class/thermal/thermal_zone*/temp` | Evaluates temperatures across all zones; flags throttling if > 75°C |
| **Cooling State** | `/sys/class/thermal/cooling_device*/cur_state` | Hardware governor throttling level |
| **SoC Model** | `getprop ro.soc.model` / `/proc/cpuinfo` | Identifies Qualcomm Snapdragon, MediaTek Dimensity, Google Tensor, etc. |

The Master coordinator's scheduler automatically factors this telemetry into dynamic load balancing, throttling task assignment if the mobile device exceeds 75°C or drops below 15% battery while discharging.

---

## 5. Verification & Acceptance Testing

To prove that the worker process cannot be suspended or frozen by the operating system, refer to the complete **10-Point Step-by-Step Evaluation Rubric**:
📄 **[`packaging/android/EVALUATION_RUBRIC.md`](./EVALUATION_RUBRIC.md)**

### Quick Smoke Test via ADB:
```bash
# 1. Assert Foreground Service OOM Score Elevation (must be <= 200)
PID=$(adb shell pidof com.oxideswarm.worker)
adb shell cat /proc/$PID/oom_score_adj

# 2. Assert Active Partial Wake Lock
adb shell dumpsys power | grep -A 5 "OxideSwarm::CpuWakeLock"

# 3. Simulate Deep Doze with Screen Off
adb shell dumpsys battery unplug
adb shell input keyevent 26
adb shell dumpsys deviceidle force-idle
adb shell dumpsys deviceidle step deep

# 4. Confirm Master continues receiving heartbeats every 3s
```

---

## 6. Directory Layout Reference

```
packaging/android/
├── README.md                      # This comprehensive guide
├── EVALUATION_RUBRIC.md           # 10-point empirical verification rubric
├── app/                           # Native Android Foreground Service App
│   ├── build.gradle.kts           # Root build configuration
│   ├── settings.gradle.kts        # Root settings configuration
│   ├── gradle.properties          # JVM & AndroidX properties
│   ├── gradlew                    # Standalone Gradle wrapper script
│   ├── app/
│   │   ├── build.gradle.kts       # App module build configuration (SDK 34 / 24)
│   │   ├── proguard-rules.pro     # JNI & Service preservation rules
│   │   └── src/main/
│   │       ├── AndroidManifest.xml # FGS, WakeLock, and Boot declarations
│   │       ├── java/com/oxideswarm/worker/
│   │       │   ├── MainActivity.kt               # Control & Monitoring UI
│   │       │   ├── service/
│   │       │   │   ├── OxideWorkerService.kt     # Foreground Service + Locks
│   │       │   │   └── WorkerNotificationManager.kt # Dynamic status notification
│   │       │   ├── receiver/
│   │       │   │   └── BootCompletedReceiver.kt  # Auto-start on boot
│   │       │   ├── runner/
│   │       │   │   ├── WorkerEngine.kt           # Common engine interface
│   │       │   │   ├── OxideWorkerBridge.kt      # In-process JNI bridge
│   │       │   │   └── ManagedProcessRunner.kt   # Binary process supervisor
│   │       │   └── util/
│   │       │       ├── BatteryOptimizationHelper.kt # Doze whitelist helper
│   │       │       └── SystemInfoHelper.kt       # Hardware & OOM inspection
│   │       └── res/                              # Layouts, colors, themes, drawables
└── termux/                        # Termux Daemon Environment
    ├── install_termux_daemon.sh   # One-click Termux setup script
    ├── start_worker.sh            # Supervision daemon script with auto-restart
    └── start-oxideswarm           # Termux:Boot auto-start hook
```

---
**Maintained by:** OxideSwarm Core Engineering Team  
**License:** Apache-2.0 / MIT
