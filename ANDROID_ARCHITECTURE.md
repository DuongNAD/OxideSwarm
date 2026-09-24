# OxideSwarm Android Native Architecture: High-Performance Mobile Compute Grid Specification

**Document Version:** 1.0.0 (Production Architecture)  
**Author:** OxideSwarm Architecture Team (Worker 1 / Core Systems)  
**Classification:** Core System Architecture Specification  
**Status:** Approved for Implementation & Benchmark Integration  

---

## Table of Contents

1. [Executive Summary & System Topology](#1-executive-summary--system-topology)
   - 1.1 [Mission & Architectural Philosophy](#11-mission--architectural-philosophy)
   - 1.2 [End-to-End System Topology](#12-end-to-end-system-topology)
   - 1.3 [Mobile Node Lifecycle State Machine](#13-mobile-node-lifecycle-state-machine)
2. [In-Depth Comparative Analysis of Open-Source Reference Models](#2-in-depth-comparative-analysis-of-open-source-reference-models)
   - 2.1 [Tailscale Android (`tailscale/tailscale-android`)](#21-tailscale-android-tailscaletailscale-android)
   - 2.2 [BOINC on Android (`BOINC/boinc`)](#22-boinc-on-android-boincboinc)
   - 2.3 [Syncthing-Android (`syncthing/syncthing-android`)](#23-syncthing-android-syncthingsyncthing-android)
   - 2.4 [Termux (`termux/termux-app`)](#24-termux-termuxtermux-app)
   - 2.5 [Iroh (`n0-computer/iroh`)](#25-iroh-n0-computeriroh)
   - 2.6 [Comprehensive Reference Model Comparison Matrix](#26-comprehensive-reference-model-comparison-matrix)
3. [Android Background Execution & Battery Optimization Engine](#3-android-background-execution--battery-optimization-engine)
   - 3.1 [Doze Mode Mechanics: Light vs. Deep Doze and Maintenance Windows](#31-doze-mode-mechanics-light-vs-deep-doze-and-maintenance-windows)
   - 3.2 [Battery Optimization Whitelisting (`REQUEST_IGNORE_BATTERY_OPTIMIZATIONS`)](#32-battery-optimization-whitelisting-request_ignore_battery_optimizations)
   - 3.3 [Immunity to Android 12+ Phantom Process Killer (PPK)](#33-immunity-to-android-12-phantom-process-killer-ppk)
   - 3.4 [Android 14+ (API 34) & 15+ Foreground Service Compliance](#34-android-14-api-34--15-foreground-service-compliance)
   - 3.5 [Hardware WakeLocks & Low-Latency Wi-Fi Optimization](#35-hardware-wakelocks--low-latency-wi-fi-optimization)
   - 3.6 [Dynamic Thermal & Battery Throttling Policies](#36-dynamic-thermal--battery-throttling-policies)
4. [Mobile NAT Traversal & Carrier Network Realities](#4-mobile-nat-traversal--carrier-network-realities)
   - 4.1 [Cellular CGNAT (RFC 6598) & Endpoint-Dependent Symmetric NAT](#41-cellular-cgnat-rfc-6598--endpoint-dependent-symmetric-nat)
   - 4.2 [Adaptive Dual-Tier Keepalive & Cellular Radio Control (RRC)](#42-adaptive-dual-tier-keepalive--cellular-radio-control-rrc)
   - 4.3 [Zero-Port Fallback via Iroh N0 DERP Relays (TLS 443)](#43-zero-port-fallback-via-iroh-n0-derp-relays-tls-443)
   - 4.4 [QUIC Connection Migration (RFC 9000 §9) for Seamless Roaming](#44-quic-connection-migration-rfc-9000-9-for-seamless-roaming)
5. [Dynamic Role Assignment & Mobile Coordination](#5-dynamic-role-assignment--mobile-coordination)
   - 5.1 [Mobile Master Refusal Matrix](#51-mobile-master-refusal-matrix)
   - 5.2 [Weighted Authority Scoring Function ($S_{auth}$) & Election](#52-weighted-authority-scoring-function-s_auth--election)
   - 5.3 [Cryptographic Master Lease Tickets & Graceful Handover](#53-cryptographic-master-lease-tickets--graceful-handover)
   - 5.4 [Zero-Latency Pre-Sleep Evacuation (`< 50ms`)](#54-zero-latency-pre-sleep-evacuation--50ms)
6. [Concrete Integration Roadmap & Verification Plan](#6-concrete-integration-roadmap--verification-plan)
   - 6.1 [Implementation Phases & Milestones](#61-implementation-phases--milestones)
   - 6.2 [Empirical Verification & Testing Framework](#62-empirical-verification--testing-framework)
   - 6.3 [Failure Modes & Architectural Invariants](#63-failure-modes--architectural-invariants)

---

## 1. Executive Summary & System Topology

### 1.1 Mission & Architectural Philosophy

The objective of the OxideSwarm Android Native Architecture is to transform consumer Android smartphones into reliable, high-throughput, battery-conscious compute nodes capable of executing parallel grid workloads (e.g. distributed Rust crate compilation, chunk hashing, parallel matrix operations, and localized machine learning inference) across heterogeneous networks anywhere in the world.

Unlike desktop workstations or server racks that benefit from static IP addresses, unmetered fiber connections, active fan cooling, and uninterrupted mains AC power, mobile devices operate in an adversarial runtime environment:
- **Mobile Operating System Aggression**: The Android OS systematically throttles, freezes, and terminates background execution via Doze mode, App Standby Buckets, Low Memory Killer Daemon (LMKD), and the Android 12+ Phantom Process Killer (PPK).
- **Hostile Cellular Carrier Topologies**: 4G LTE and 5G cellular carriers enforce Carrier-Grade NAT (CGNAT RFC 6598) with Endpoint-Dependent Symmetric port mapping (EDM), aggressive UDP session timeouts (15–30 seconds), and high-power radio tail states (`RRC_CONNECTED`).
- **Physical Hardware Constraints**: Passive thermal dissipation causes rapid surface overheating ($> 45^\circ\text{C}$), while unthrottled CPU usage can deplete device batteries within hours.

To conquer these constraints, OxideSwarm abandons fragile legacy patterns (such as forking subprocesses or relying on external VPN applications) and establishes a unified **In-Process JNI Architecture (`liboxideworker.so`)** powered by **Iroh QUIC P2P NAT Traversal**, an **Adaptive Cellular Keepalive Engine**, and an **Autonomous Role Arbitrage System**.

### 1.2 End-to-End System Topology

The OxideSwarm cluster topology seamlessly interconnects mobile devices, local workstations, and cloud instances into a resilient P2P mesh:

```
+─────────────────────────────────────────────────────────────────────────────────────────────+
│                                  OXIDESWARM SYSTEM TOPOLOGY                                 │
+─────────────────────────────────────────────────────────────────────────────────────────────+

   [ Remote Mobile Node 1 ]                [ Remote Mobile Node 2 ]
   Samsung Galaxy S24 Ultra                Google Pixel 8 Pro
   (Cellular 5G - Symmetric CGNAT)         (Public Wi-Fi - Restricted NAT)
   ┌───────────────────────────────┐       ┌───────────────────────────────┐
   │ Android Foreground Service    │       │ Android Foreground Service    │
   │  └── In-Process JNI Worker    │       │  └── In-Process JNI Worker    │
   │      (liboxideworker.so)      │       │      (liboxideworker.so)      │
   └──────────────┬────────────────┘       └──────────────┬────────────────┘
                  │ QUIC / TLS 443                        │ QUIC / UDP
                  │ (DERP Relayed)                        │ (Direct Hole-Punched)
                  ▼                                       ▼
       ┌─────────────────────┐                 ┌─────────────────────┐
       │   Iroh N0 Relay     │                 │   Iroh N0 Relay     │
       │ (relay.n0.iroh.link)│                 │ (relay.n0.iroh.link)│
       └──────────┬──────────┘                 └──────────┬──────────┘
                  │                                       │
                  │ TLS 443 Encrypted Tunnel              │ UDP Direct Peer Path
                  └───────────────────┬───────────────────┘
                                      ▼
                      +───────────────────────────────+
                      │       Active Master Node      │
                      │    (Workstation / Cloud VPS)  │
                      │                               │
                      │ ├── Task Scheduler & Queue    │
                      │ ├── Dynamic Load Balancer     │
                      │ ├── Real-Time Web Dashboard   │
                      │ └── Persistent P2P Ticket     │
                      +───────────────┬───────────────+
                                      │
            ┌─────────────────────────┴─────────────────────────┐
            ▼                                                   ▼
   [ Desktop Worker Node ]                             [ Plugged-In Mobile Master ]
   MacBook Pro (Apple Silicon)                         (Standby Coordinator Candidate)
   LAN / Static WAN                                    Local Wi-Fi + AC Power Connected
```

### 1.3 Mobile Node Lifecycle State Machine

A mobile node transitions through distinct operational states governed by battery level, thermal readings, network interface type, and OS lifecycle broadcasts:

```
                      +-----------------------------+
                      |           OFFLINE           |
                      +--------------┬--------------+
                                     │ User triggers Start / Boot Complete
                                     ▼
                      +-----------------------------+
                      |       INITIALIZING          |
                      |  - Elevate to FGS           |
                      |  - Acquire WakeLocks        |
                      |  - Load liboxideworker.so   |
                      +--------------┬--------------+
                                     │ JNI runtime spawned
                                     ▼
                      +-----------------------------+
                      |         CONNECTING          |
                      |  - Resolve Master Ticket    |
                      |  - Iroh QUIC Handshake      |
                      |  - Register Capabilities    |
                      +--------------┬--------------+
                                     │ Handshake ACK received
                                     ▼
           +---------------------------------------------------+
           │                   ACTIVE ONLINE                   │
           +---------------------------------------------------+
             │                      │                        │
             │ Thermal > 42°C       │ Battery < 30%          │ OS Sleep Broadcast /
             │ or Throttled         │ Discharging            │ Battery < 15%
             ▼                      ▼                        ▼
  +--------------------+  +--------------------+  +--------------------+
  |  THROTTLED COMPUTE |  |    COLD STANDBY    |  | PRE-SLEEP EVACUATE |
  | - Reduce cores 50% |  | - Reject new tasks |  | - Send Disconnecting|
  | - Notify Master    |  | - Complete current |  | - Requeue in <50ms |
  | - Cooldown timer   |  | - Low-power idle   |  | - Release locks    |
  +--------------------+  +--------------------+  +--------------------+
```

---

## 2. In-Depth Comparative Analysis of Open-Source Reference Models

To design a robust native Android architecture, OxideSwarm evaluates five proven open-source reference models that address mobile background execution, P2P networking, and volunteer compute.

### 2.1 Tailscale Android (`tailscale/tailscale-android`)

Tailscale delivers zero-configuration mesh VPN connectivity using WireGuard and Tailscale's Disco/DERP architecture.

#### Key Architectural Patterns
1. **In-Process Engine via Shared Library (`libtailscale.so`)**:
   Instead of running the `tailscaled` daemon as an external CLI binary, Tailscale compiles its Go networking engine using `gomobile bind` into a native shared C-library (`libtailscale.so`). The Android application loads this library directly into the Java Virtual Machine process via `System.loadLibrary("tailscale")`.
2. **Zero Child Processes**:
   All networking threads, WireGuard crypto routines, and timers run as POSIX pthreads mapped to Go routines inside the primary Android application process. The process table exhibits exactly **zero child processes**, rendering Tailscale completely immune to the Android 12+ Phantom Process Killer.
3. **Android `VpnService` Priority**:
   By encapsulating the engine in an Android `VpnService` (a specialized Foreground Service), the Linux kernel Out-Of-Memory score adjustment (`oom_score_adj`) is maintained at $\le 200$, preventing the Low Memory Killer Daemon (LMKD) from terminating the process.
4. **Dynamic Interface Handover**:
   Tailscale registers an Android `ConnectivityManager.NetworkCallback` that detects when the physical network interface switches (e.g. `wlan0` $\to$ `rmnet_data0`). It immediately injects a `linkChange` event via JNI into the native Go engine, triggering an instantaneous WireGuard endpoint update without dropping the virtual TUN interface.

#### Lessons for OxideSwarm
OxideSwarm adopts Tailscale's in-process shared library pattern (`liboxideworker.so` via Rust JNI) and dynamic `NetworkCallback` handover.

---

### 2.2 BOINC on Android (`BOINC/boinc`)

BOINC (Berkeley Open Infrastructure for Network Computing) is the pioneer of distributed volunteer computing, enabling mobile devices to compute scientific tasks for projects like Einstein@Home, Rosetta@Home, and Asteroids@home.

#### Key Architectural Patterns & Vulnerabilities
1. **Multi-Process Subprocess Architecture (Flawed)**:
   BOINC historically operates by having a Java GUI wrapper supervise a native coordinator binary (`boinc`), which in turn forks scientific project binaries (e.g. `einstein_arm64`) using POSIX `fork()` and `execv()`.
2. **The Android 10 W^X & Android 12 PPK Collapse**:
   - In Android 10, SELinux enforced W^X (`noexec` on `/data/data/` directories), preventing BOINC from executing dynamically downloaded science binaries from app-writable storage.
   - In Android 12, Google enabled the Phantom Process Killer. When BOINC's coordinator spawned scientific compute binaries consuming 100% CPU, the Android OS classified them as rogue phantom processes and terminated them with `SIGKILL` (signal 9). BOINC was forced to instruct users to run manual ADB shell commands (`device_config put activity_manager max_phantom_processes 2147483647`).
3. **Gold-Standard Mobile Power & Thermal Safeguards**:
   Despite process execution challenges, BOINC established the gold standard for mobile power policies:
   - **AC Power Constraint**: Defaults to computing **only when plugged into an external charger**. Unplugging the charger immediately pauses all tasks.
   - **Battery Level Gating**: Tasks compute only if battery percentage is $\ge 90\%$, protecting battery recharge curves.
   - **Thermal Ceiling**: Pauses computation if battery or SoC temperature exceeds $40^\circ\text{C}$, enforcing a mandatory 5-minute cooldown period.
   - **Core Cap**: Permits users to allocate a fraction of available CPU cores (e.g. 50%), preserving responsiveness for interactive Android apps.

#### Lessons for OxideSwarm
OxideSwarm adopts BOINC's power and thermal safeguard policies while deliberately avoiding BOINC's fatal architectural flaw: OxideSwarm executes compute tasks in-process or via sandboxed in-memory runtimes rather than spawning external child processes.

---

### 2.3 Syncthing-Android (`syncthing/syncthing-android` & `Syncthing-Fork`)

Syncthing is an open-source peer-to-peer file synchronization engine written in Go.

#### Key Architectural Patterns
1. **Subprocess Supervision & Loopback IPC**:
   Mainline Syncthing-Android packages the precompiled Go `syncthing` binary in the APK's `lib/` directory and executes it via `ProcessBuilder`. The Android wrapper interacts with the Go daemon via a localhost HTTP REST API and Unix Domain Sockets.
2. **Syncthing-Fork Power Gating**:
   Catfriend1's `syncthing-fork` heavily reworked power management:
   - **Selective WakeLocks**: Releases `PARTIAL_WAKE_LOCK` when the synchronization index is idle, allowing the phone to enter deep sleep between scheduled sync sweeps.
   - **Condition Gating**: Allows syncing only on specified Wi-Fi SSIDs, only while charging, or when battery level is above a configurable threshold (e.g. $> 50\%$).
3. **Subprocess Resilience Issues**:
   Like BOINC, Syncthing's external process model suffered from PPK termination on Android 12+ and high memory overhead due to JSON serialization over localhost loopback sockets.

#### Lessons for OxideSwarm
OxideSwarm incorporates Syncthing's fine-grained condition gating while replacing loopback HTTP IPC with direct, zero-copy Rust JNI calls.

---

### 2.4 Termux (`termux/termux-app`)

Termux provides an open-source terminal emulator and comprehensive Linux userland environment for Android.

#### Key Architectural Patterns
1. **Foreground Service & Hardware Locks**:
   Termux executes a persistent `TermuxService` Foreground Service. Running `termux-wake-lock` acquires a `PowerManager.PARTIAL_WAKE_LOCK` and Wi-Fi lock to keep background daemons alive.
2. **Direct POSIX Environment**:
   Termux compiles packages against Android Bionic libc and installs them into a custom prefix (`/data/data/com.termux/files/usr`).
3. **Severe PPK Susceptibility**:
   Termux represents the most prominent victim of Android 12's Phantom Process Killer. Users compiling code (`cargo build`, `clang`, `rustc`) or running multi-process server clusters regularly suffered sudden `[Process completed (signal 9)]` crashes when child processes exceeded 32 or consumed background CPU.

#### Lessons for OxideSwarm
While Termux is an invaluable development and benchmarking environment for rooted devices or power users with ADB access, it cannot serve as the primary deployment vehicle for general users. OxideSwarm must deliver a self-contained, turnkey APK that does not require ADB overrides.

---

### 2.5 Iroh (`n0-computer/iroh`)

Iroh is a next-generation peer-to-peer networking library written in Rust, combining QUIC transport, cryptographic node identities, and DERP relay NAT traversal.

#### Key Architectural Patterns
1. **Unified `iroh::Endpoint` (`magicsock`)**:
   Combines Quinn QUIC with Tailscale-inspired `magicsock` architecture. A single multiplexed UDP socket manages local network paths, STUN discovery, UPnP port mapping, and DERP relays.
2. **Permanent Cryptographic Identity**:
   Peers are identified by an Ed25519 public key (`NodeId`), completely independent of dynamic IP addresses or port assignments.
3. **Zero-Latency Relay Fallback (DERP / N0)**:
   Transports encrypted packets over HTTPS TLS 443 via global relays (`relay.n0.iroh.link`). DERP provides instantaneous connectivity with zero connection setup drop while direct UDP hole-punching occurs concurrently in the background.

#### Lessons for OxideSwarm
OxideSwarm natively embeds `iroh` directly into `crates/android_bridge` and `crates/worker`, providing out-of-the-box NAT traversal without third-party VPN daemons.

---

### 2.6 Comprehensive Reference Model Comparison Matrix

| Architectural Dimension | Tailscale Android | BOINC on Android | Syncthing-Android | Termux | Iroh (Embedded) | OxideSwarm (Target) |
|---|---|---|---|---|---|---|
| **Core Process Model** | In-Process (`.so` via gomobile) | Multi-Process (daemon + science apps) | Multi-Process (wrapper + Go binary) | Multi-Process (shell + child procs) | Library (Embedded in Rust) | **In-Process (`liboxideworker.so` via JNI)** |
| **Child Processes** | **0 (Zero)** | Multiple ($> 4$) | $1 - 3$ | Unbounded ($> 32$) | **0 (Zero)** | **0 (Zero)** |
| **Android 12+ PPK Immunity** | **100% Immune** | ❌ Vulnerable (SIGKILL) | ❌ Vulnerable (SIGKILL) | ❌ Vulnerable (SIGKILL) | N/A | **100% Immune** |
| **Android 14+ FGS Type** | `specialUse` / `vpn` | `dataSync` / legacy | `dataSync` | `specialUse` / legacy | N/A | **`specialUse` + `dataSync` fallback** |
| **IPC Overhead** | Low (In-Process) | Medium (POSIX pipes/sockets) | High (Localhost REST HTTP) | High (PTY / Pipes) | Zero (In-Process) | **Zero (In-Process JNI Callbacks)** |
| **NAT Traversal Tech** | WireGuard + DERP | None (Direct HTTP/S) | Syncthing Relay + UPnP | None (Direct TCP) | **QUIC + DERP Relay** | **Iroh QUIC + DERP Relay** |
| **Carrier CGNAT Traversal** | Excellent (TLS 443) | Poor (Requires public IP) | Moderate (Relay bandwidth) | Poor (Requires port forward) | **Excellent (TLS 443)** | **Excellent (TLS 443)** |
| **Roaming Handover** | WireGuard Roaming | HTTP Poll Retry | Reconnect on drop | Broken pipe | **QUIC Connection Migration**| **QUIC Connection Migration** |
| **Thermal Safeguards** | None (Lightweight) | Battery Temp Sensor | None | None | None | **Thermal HAL API + Battery Temp** |
| **Dynamic Role Arbitrage**| None | None | None | None | None | **Weighted Authority ($S_{auth}$)** |

---

## 3. Android Background Execution & Battery Optimization Engine

### 3.1 Doze Mode Mechanics: Light vs. Deep Doze and Maintenance Windows

Introduced in Android 6.0 (API 23) and expanded in Android 7.0 (API 24), Doze Mode is administered by `com.android.server.DeviceIdleController`. It consists of a two-stage hierarchical finite state machine designed to minimize battery drain when the user is not actively interacting with the device.

```
                         +───────────────────────────+
                         │       DEVICE ACTIVE       │
                         │ (Screen ON / User Active) │
                         +─────────────┬─────────────+
                                       │ Screen OFF & Running on Battery
                                       ▼
                         +───────────────────────────+
                         │        LIGHT DOZE         │
                         │  - No motion sensor check │
                         │  - Network restricted     │
                         │  - Jobs/Syncs deferred    │
                         │  - WakeLocks HONORED      │
                         +─────────────┬─────────────+
                                       │ Device completely stationary for ~30 min
                                       │ (Significant Motion Detector - SMD)
                                       ▼
                         +───────────────────────────+
                         │         DEEP DOZE         │
                         │  - Motion triggers exit   │
                         │  - Network DISABLED       │
                         │  - WakeLocks SUPPRESSED   │
                         │  - Alarms deferred        │
                         +─────────────┬─────────────+
                                       │
                         +─────────────┴─────────────+
                         │    Maintenance Window     │
                         │ (Expands 9m -> 15m -> 6h) │
                         +───────────────────────────+
```

#### Light Doze vs. Deep Doze Detailed Comparison

| Attribute / Subsystem | Light Doze (API 24+) | Deep Doze (API 23+) |
|---|---|---|
| **Activation Condition** | Screen off, running on battery. Movement does not prevent entry. | Screen off, running on battery, AND device completely motionless for qualifying period. |
| **Motion Sensor** | Disabled / Ignored. | Actively monitors hardware Significant Motion Detector (SMD). Physical movement instantly exits Deep Doze. |
| **CPU WakeLocks** | **Respected**. `PowerManager.PARTIAL_WAKE_LOCK` keeps the CPU cores active. | **Suppressed**. The Linux kernel forces CPU cores into deep low-power C-states (`WFI` / Wait For Interrupt). |
| **Network Access** | Blocked for background apps via eBPF / iptables. Whitelisted apps retain traffic. | Completely severed for non-whitelisted apps. Sockets experience packet loss or timeouts. |
| **Maintenance Windows**| Cycles frequently (every 5 to 15 minutes), lasting 1 to 2 minutes. | Cycles with geometric backoff (9 min, 15 min, 30 min, 1h, 2h, up to 6h max). |

**Architectural Consequence**: Relying on periodic maintenance windows is fatal for a distributed compute grid. If an Android worker stops responding to heartbeats for even 10–15 seconds, the Master node must evict the worker and re-enqueue all in-flight tasks. Therefore, **OxideSwarm cannot rely on standard background execution; it must achieve complete exemption from Deep Doze**.

---

### 3.2 Battery Optimization Whitelisting (`REQUEST_IGNORE_BATTERY_OPTIMIZATIONS`)

To maintain active CPU execution and continuous network connectivity while the phone is stationary with the screen off, OxideSwarm must be placed on the system's **Power Whitelist** (`PowerWhitelistManager`).

#### Permission & Activation Flow
In `AndroidManifest.xml`:
```xml
<uses-permission android:name="android.permission.REQUEST_IGNORE_BATTERY_OPTIMIZATIONS" />
```

In Kotlin (`BatteryOptimizationHelper.kt`):
```kotlin
fun requestIgnoreBatteryOptimizations(context: Context) {
    val powerManager = context.getSystemService(Context.POWER_SERVICE) as PowerManager
    val packageName = context.packageName
    if (!powerManager.isIgnoringBatteryOptimizations(packageName)) {
        val intent = Intent(Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS).apply {
            data = Uri.parse("package:$packageName")
            flags = Intent.FLAG_ACTIVITY_NEW_TASK
        }
        context.startActivity(intent)
    }
}
```

#### Exact Capabilities Granted by Battery Optimization Exemption

| System Capability | Default Non-Exempt App | Exempted OxideSwarm Worker |
|---|---|---|
| **CPU WakeLocks in Deep Doze** | Suppressed (CPU forced to sleep) | **Honored** (CPU executes continuously) |
| **Network Sockets in Deep Doze** | Dropped by kernel eBPF packet filter | **Open** (QUIC/UDP and TCP continue) |
| **App Standby Bucket** | Demoted to `RESTRICTED` or `NEVER` | Elevated to `EXEMPTED` |
| **Phantom Process Killer** | Subject to PPK (32 child limit) | **Still Subject to PPK** (No exemption!) |
| **LMKD Memory Pressure** | Based on `oom_score_adj` | **Still Subject to LMKD** (No exemption!) |

> **Key Architectural Insight**: Battery optimization exemption ensures that CPU wake locks and network sockets remain operational during Deep Doze. However, it does **not** protect against the Phantom Process Killer or Out-Of-Memory termination. That protection is provided by the **Foreground Service**.

---

### 3.3 Immunity to Android 12+ Phantom Process Killer (PPK)

#### The Mechanism of the Phantom Process Killer
In Android 12 (API 31), Google introduced `PhantomProcessList.java` inside `ActivityManagerService`. 
1. Android places the entire process tree of an app UID into a dedicated Linux cgroup v2 controller (`/sys/fs/cgroup/uid_<UID>/`).
2. Any native Linux process spawned under that UID (via `fork()`, `execve()`, or `ProcessBuilder`) that is *not* registered in `ActivityManagerService` as an ART Android component (Activity, Service, Receiver, Provider) is designated a **Phantom Process**.
3. If the total number of phantom processes across all background apps exceeds **32**, or if a phantom process consumes excessive CPU cycles while the parent app is not in the interactive foreground, `ActivityManagerService` terminates the process immediately via:
   $$\text{kill}(pid, \text{SIGKILL})$$
   Signal 9 is uncatchable: no destructors execute, no socket close frames are transmitted, and no cleanup occurs.

#### OxideSwarm's Immunity Mechanism: The In-Process POSIX Model
OxideSwarm completely eliminates phantom processes by compiling the worker engine as a native dynamic shared library (`liboxideworker.so`) and loading it into the Android Service process via JNI:

```
+─────────────────────────────────────────────────────────────────────────────+
│              Android App Process (com.oxideswarm.worker, PID 1420)           │
│                                                                             │
│  [ Dalvik / ART Virtual Machine ]                                           │
│    ├── Thread 1: Main UI Looper                                             │
│    ├── Thread 2: Binder IPC ThreadPool                                      │
│    └── Thread 3: OxideWorkerService (Foreground Service Lifecycle)          │
│          │                                                                  │
│          ▼ JNI Call (nativeStartWorker)                                     │
│  [ Native Rust Shared Library: liboxideworker.so ]                          │
│    └── POSIX pthread: "OxideWorker-Tokio"                                   │
│          │ (Created via std::thread::Builder / pthread_create)              │
│          ▼                                                                  │
│    [ Multi-Threaded Tokio Async Runtime ]                                   │
│      ├── WorkerClient Core Event Loop                                       │
│      ├── Iroh QUIC UDP Socket Engine                                        │
│      └── Parallel Task Execution Semaphore Pool                            │
+─────────────────────────────────────────────────────────────────────────────+
│  Process Accounting:                                                        │
│  - Total Processes Spawned: 1 (The Android Service Process)                 │
│  - Total Child / Phantom Processes Spawned: 0 (ZERO)                        │
│  - Verdict by PhantomProcessList: 100% IMMUNE TO PPK                        │
+─────────────────────────────────────────────────────────────────────────────+
```

Because threads share the same Linux Thread Group ID (`tgid`) and process memory descriptor (`mm_struct`), the Linux kernel and Android `PhantomProcessList` register **zero additional processes**. This confers 100% architectural immunity to PPK across Android 12, 13, 14, and 15+.

---

### 3.4 Android 14+ (API 34) & 15+ Foreground Service Compliance

Under Android 14+ (UPSIDE_DOWN_CAKE, API 34), Foreground Services require explicit declaration of service types and runtime permissions.

#### Manifest Configuration
```xml
<!-- Manifest Permissions -->
<uses-permission android:name="android.permission.FOREGROUND_SERVICE" />
<uses-permission android:name="android.permission.FOREGROUND_SERVICE_SPECIAL_USE" />
<uses-permission android:name="android.permission.FOREGROUND_SERVICE_DATA_SYNC" />
<uses-permission android:name="android.permission.POST_NOTIFICATIONS" />

<!-- Service Declaration -->
<service
    android:name="com.oxideswarm.worker.service.OxideWorkerService"
    android:directBootAware="true"
    android:enabled="true"
    android:exported="false"
    android:foregroundServiceType="specialUse|dataSync">
    <property
        android:name="android.app.PROPERTY_SPECIAL_USE_FGS_SUBTYPE"
        android:value="Distributed computing grid worker executing peer-to-peer compute tasks" />
</service>
```

#### Runtime Elevation Strategy
In `OxideWorkerService.kt`:
```kotlin
val notification = notificationManager.buildNotification(statusTitle, statusDetails)

if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
    // Android 14+ (API 34)
    startForeground(
        NOTIFICATION_ID,
        notification,
        ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE
    )
} else if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
    // Android 10+ (API 29)
    startForeground(
        NOTIFICATION_ID,
        notification,
        ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC
    )
} else {
    // Legacy Android
    startForeground(NOTIFICATION_ID, notification)
}
```

Running as a Foreground Service adjusts the process priority to `oom_score_adj <= 200` (`PERCEPTIBLE_APP`), preventing eviction by LMKD during heavy cluster computations.

---

### 3.5 Hardware WakeLocks & Low-Latency Wi-Fi Optimization

To prevent hardware components from powering down when the device display turns off:

1. **CPU Partial WakeLock (`PowerManager.PARTIAL_WAKE_LOCK`)**:
   - Tagged as `"OxideSwarm::CpuWakeLock"`.
   - Initialized with `setReferenceCounted(false)` to prevent lock counter leaks.
   - Configured with a 24-hour safety timeout and proactive re-acquisition to ensure safety against unhandled crashes.
2. **Low-Latency Wi-Fi Lock (`WifiManager.WIFI_MODE_FULL_LOW_LATENCY`)**:
   - On Android 10+ (API 29+), `WIFI_MODE_FULL_LOW_LATENCY` disables 802.11 power-saving modes (DTIM sleep).
   - Eliminates packet latency spikes (reducing jitter from 300ms down to $< 10\text{ms}$), critical for real-time task heartbeats and QUIC packet pacing.
   - Fallback to `WIFI_MODE_FULL_HIGH_PERF` on older Android versions.
3. **Multicast Lock (`WifiManager.MulticastLock`)**:
   - Enables reception of UDP multicast discovery beacons for automatic LAN Master pairing.

---

### 3.6 Dynamic Thermal & Battery Throttling Policies

Mobile smartphones feature passive cooling: heat from the SoC dissipates through the aluminum chassis and glass screen. Sustained multi-core execution requires active thermal management.

#### Dynamic Compute Safeguard Model

```
+─────────────────────────────────────────────────────────────────────────────+
│                       DYNAMIC COMPUTE SAFEGUARD MATRIX                      │
+─────────────────────────────────────────────────────────────────────────────+
  Metric Evaluated    │ Measurement / Source       │ Action Taken
──────────────────────┼────────────────────────────┼───────────────────────────
  Charging State      │ BatteryManager (AC/USB)    │ Only run full cores if AC
  Battery Level       │ BatteryManager (0 - 100%)  │ If < 30%: Eco Mode (1 core)
                      │                            │ If < 15%: Evacuate & Pause
  Thermal Status      │ PowerManager Thermal HAL   │ NONE/LIGHT: 100% capacity
                      │ (API 29+ Listener)         │ MODERATE: Throttle cores 50%
                      │                            │ SEVERE/CRITICAL: Halt tasks
  Battery Temperature │ BatteryManager (Temp °C)   │ If > 42.0°C: Force Cooldown
```

#### Implementation via JNI Telemetry Bridge
Rather than executing `/system/bin/dumpsys` (which fails in non-root app sandboxes due to missing `android.permission.DUMP`), the Kotlin service registers an `ACTION_BATTERY_CHANGED` receiver and `PowerManager.OnThermalStatusChangedListener`. Telemetry is injected directly into Rust via `nativeUpdateTelemetryDetailed` (enriched 5-parameter model) or `nativeUpdateTelemetry` (backward-compatible legacy overload):

```rust
#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeUpdateTelemetryDetailed<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    battery_pct: jint,
    is_charging: jboolean,
    thermal_throttled: jboolean,
    battery_temperature: jfloat,
    network_type: JString<'local>,
) -> jboolean {
    let net_opt: Option<String> = match env.get_string(&network_type) {
        Ok(s) => {
            let val: String = s.into();
            if val.trim().is_empty() { None } else { Some(val) }
        }
        Err(_) => None,
    };
    let temp_opt = if battery_temperature < -40.0 {
        None
    } else {
        Some(battery_temperature)
    };

    if update_telemetry_detailed_impl(
        battery_pct,
        is_charging != 0,
        thermal_throttled != 0,
        temp_opt,
        net_opt,
    ) {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

#[no_mangle]
pub extern "system" fn Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeUpdateTelemetry<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    battery_pct: jint,
    is_charging: jboolean,
    thermal_throttled: jboolean,
) -> jboolean {
    if update_telemetry_impl(battery_pct, is_charging != 0, thermal_throttled != 0) {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}
```

---

## 4. Mobile NAT Traversal & Carrier Network Realities

### 4.1 Cellular CGNAT (RFC 6598) & Endpoint-Dependent Symmetric NAT

Mobile network operators assign private IPv4 addresses from the shared `100.64.0.0/10` block (RFC 6598) to mobile subscriber devices. 

#### Endpoint-Independent vs. Endpoint-Dependent Mapping
- **Endpoint-Independent Mapping (EIM / Full Cone / Restricted Cone)**: Outbound packets from internal IP:port $(I_{ip}, I_{port})$ map to external IP:port $(E_{ip}, E_{port})$ regardless of destination. Standard STUN UDP hole-punching succeeds.
- **Endpoint-Dependent Mapping (EDM / Symmetric NAT)**: Cellular carrier Large-Scale NAT (LSN) devices allocate a *distinct* external port for each unique destination $(D_{ip}, D_{port})$:
  $$(I_{ip}, I_{port}) \to (D_{1}, P_{1}) \implies (E_{ip}, E_{port}^{1})$$
  $$(I_{ip}, I_{port}) \to (D_{2}, P_{2}) \implies (E_{ip}, E_{port}^{2}) \quad \text{where } E_{port}^{1} \neq E_{port}^{2}$$

Under modern cellular CGNAT, port allocation follows RFC 6056 randomized port selection algorithms. The port delta $\Delta P = E_{port}^{2} - E_{port}^{1}$ is pseudo-random across the 16-bit range $[1024, 65535]$. Consequently, **direct UDP hole-punching between two mobile devices behind symmetric CGNAT without an external relay is mathematically impossible ($P_{success} < 0.05\%$)**.

---

### 4.2 Adaptive Dual-Tier Keepalive & Cellular Radio Control (RRC)

#### The Cellular Tail Timer Trap
Cellular baseband processors transition through 3GPP Radio Resource Control (RRC) states:
1. **`RRC_CONNECTED`**: High-power transmission state ($\sim 1,000\text{mW} - 1,800\text{mW}$).
2. **`RRC_IDLE`**: Low-power Discontinuous Reception state ($\sim 10\text{mW} - 25\text{mW}$).

Whenever a mobile app transmits an outbound UDP packet, the modem transitions to `RRC_CONNECTED`. When transmission ceases, an **Inactivity Tail Timer** (typically 10–12 seconds) keeps the modem at full power before returning to `RRC_IDLE`.

If an app sends un-coalesced keepalives or telemetry every 10–15 seconds, the modem **never returns to idle**, consuming up to 35% of the device's battery in 4 hours on network idling alone.

#### Adaptive Dual-Tier Keepalive Protocol
OxideSwarm specifies an **Adaptive Keepalive Engine**:

$$\tau_{keepalive} = \begin{cases} 
60\text{s} & \text{if Transport is Wi-Fi or Ethernet} \\
25\text{s} & \text{if Transport is Cellular CGNAT (Idle / Quiescent)} \\
10\text{s} & \text{if Transport is Cellular CGNAT (Active Tasks)} 
\end{cases}$$

- **Telemetry Coalescing**: Heartbeat telemetry (CPU load, free RAM, battery %, thermal state) is bundled directly into the keepalive frame, eliminating separate telemetry packets.
- **RRC Sleep Windows**: On Wi-Fi, a 60-second keepalive permits prolonged Wi-Fi SoC low-power sleep. On Cellular, 25 seconds guarantees the carrier NAT binding does not expire (which typically drops at 30 seconds) while maximizing time spent in `RRC_IDLE`.

---

### 4.3 Zero-Port Fallback via Iroh N0 DERP Relays (TLS 443)

When symmetric CGNAT or carrier firewalls prevent direct UDP hole punching, OxideSwarm routes traffic through Iroh's **Designated Encrypted Relay for Packets (DERP)** infrastructure (`relay.n0.iroh.link`).

```
+───────────────────+                               +───────────────────+
│   Mobile Worker   │                               │    Master Node    │
│ (Symmetric CGNAT) │                               │  (Public / Cloud) │
+─────────┬─────────+                               +─────────┬─────────+
          │                                                   │
          │ 1. Connect TCP/443 (TLS)                          │ 1. Connect TCP/443 (TLS)
          ▼                                                   ▼
+───────────────────────────────────────────────────────────────────────+
│                      Iroh N0 Relay (relay.n0.iroh.link)               │
│ - Zero port allocation (single port 443)                              │
│ - End-to-end encrypted: Payloads encrypted with destination NodeId    │
│ - Transparent packet forwarding via Ed25519 identity addressing      │
+───────────────────────────────────────────────────────────────────────+
          ▲                                                   ▲
          │ 2. Concurrent Disco UDP Probes                     │
          └─────────────────◄─────────────────────────────────┘
                    3. Direct UDP Path Validated?
                     ├── YES: Seamlessly migrate to Direct QUIC
                     └── NO:  Continue over TLS 443 Relay (0% Drop Rate)
```

1. **Firewall Penetration**: Operates over standard TCP port 443 with TLS encryption. Indistinguishable from standard HTTPS traffic, bypassing corporate firewalls and carrier deep packet inspection.
2. **Zero Port Allocation Overhead**: Unlike legacy TURN (RFC 8656) which allocates a dedicated UDP port per client pair, DERP routes packets based on the recipient's Ed25519 public key (`iroh::NodeId`).
3. **End-to-End Cryptography**: Relays are untrusted: packets are encrypted using the peer's public key (via WireGuard-style ChaCha20-Poly1305 or TLS-Ring). The relay cannot inspect payload contents.

---

### 4.4 QUIC Connection Migration (RFC 9000 §9) for Seamless Roaming

When an Android user leaves home or office, their connection transitions from Wi-Fi (`192.168.1.147`) to Cellular (`100.72.14.82`).

#### The Failure of Legacy TCP/TLS
Under TCP/TLS:
- The OS resets the 4-tuple connection (`ECONNRESET`).
- Active compilation or computation streams abort midway.
- The worker must execute a full multi-second reconnection cycle: DNS resolution $\to$ TCP 3-way handshake $\to$ TLS 1.3 handshake $\to$ OxideSwarm registration.
- In-flight tasks are marked dead by the Master and re-dispatched, wasting compute time and battery.

#### QUIC Connection Migration Solution
Under RFC 9000 §9:
1. **Cryptographic Connection IDs (CIDs)**: QUIC connections are identified by 64-bit random Connection IDs rather than IP:port tuples.
2. **Handover Protocol**:
   - Android `ConnectivityManager.NetworkCallback` signals network switch.
   - The worker binds a UDP socket to the new network interface.
   - The worker sends a `PATH_CHALLENGE` frame containing an 8-byte cryptographic nonce from the new IP address.
   - The Master replies with `PATH_RESPONSE`.
   - The active path migrates to the new interface with **zero disconnection of in-flight task streams**.

---

## 5. Dynamic Role Assignment & Mobile Coordination

### 5.1 Mobile Master Refusal Matrix

While any OxideSwarm node can theoretically run `MasterServer`, mobile devices are inherently ill-suited to serve as persistent cluster coordinators. A mobile device MUST autonomously evaluate its environmental telemetry and **strictly refuse** the Master role when any of the following conditions occur:

```
+─────────────────────────────────────────────────────────────────────────────+
│                         MOBILE MASTER REFUSAL MATRIX                        │
+─────────────────────────────────────────────────────────────────────────────+
  Telemetry Metric     │ Refusal Condition  │ Justification
───────────────────────┼────────────────────┼──────────────────────────────────
  Power Source         │ !is_charging       │ Running Master on battery depletes
                       │                    │ device charge within 2–3 hours.
  Battery Reserve      │ battery_pct < 50%  │ Insufficient energy buffer even
                       │                    │ if temporarily charging.
  Network Interface    │ is_cellular OR     │ Master requires high-throughput,
                       │ is_metered         │ low-latency, unmetered connectivity.
  Thermal Headroom     │ status >= MODERATE │ Scheduler loops and HTTP dashboards
                       │ (or Temp > 40°C)   │ will induce severe thermal throttle.
  Execution Context    │ !has_fgs_active    │ OS Doze mode will freeze master
                       │                    │ within 3 minutes of screen-off.
```

If any refusal condition is met, the node automatically designates itself as pure `WorkerClient` and declines Master nomination.

---

### 5.2 Weighted Authority Scoring Function ($S_{auth}$) & Election

When a cluster's active Master becomes unreachable (e.g. desktop powered down, network drop), remaining nodes execute an autonomous **Weighted Authority Election**.

Each connected node computes its Authority Score $S_{auth} \in [-10000, +10000]$:

$$S_{auth} = P_{platform} + P_{power} + P_{net} + P_{thermal} + P_{uptime}$$

#### Component Scoring Weights

1. **Platform Weight ($P_{platform}$)**:
   - Dedicated Linux Server: $+5,000$
   - macOS Workstation: $+4,500$
   - Windows Desktop / Laptop: $+4,000$
   - Android Device: $+500$
2. **Power Supply ($P_{power}$)**:
   - AC Mains Power: $+2,000$
   - Battery Powered $> 80\%$: $+500$
   - Battery Powered $30\% - 80\%$: $+100$
   - Battery Powered $< 30\%$: $-3,000$
3. **Network Quality ($P_{net}$)**:
   - Gigabit Wired Ethernet: $+1,500$
   - Wi-Fi 6 / 5GHz Unmetered: $+1,000$
   - Standard 2.4GHz Wi-Fi: $+700$
   - Cellular 4G/5G: $-2,000$
4. **Thermal State ($P_{thermal}$)**:
   - Nominal / Cool: $0$
   - Moderate Thermal Throttle: $-1,500$
   - Severe / Critical Throttle: $-5,000$
5. **Uptime Stability ($P_{uptime}$)**:
   - $P_{uptime} = \min(1000, \text{uptime\_seconds} \times 0.1)$

#### Election Invariant
Any node with $S_{auth} < 2500$ is strictly disqualified from election. If a mobile device is plugged into AC power, on fast unmetered Wi-Fi, and cool, its score reaches $500 + 2000 + 1000 + 0 + 300 = \mathbf{3,800}$, qualifying it to serve as a **temporary emergency coordinator** if no desktop nodes are available.

---

### 5.3 Cryptographic Master Lease Tickets & Graceful Handover

To prevent split-brain clusters without requiring heavyweight consensus protocols:
1. **Time-Bounded Lease Ticket**: The active Master issues a signed cryptographic lease ticket:
   ```json
   {
     "cluster_id": "8f3d1a22-44be-4cf2-9e20-72cb61ea4230",
     "master_node_id": "iroh:pubkey:e98dfa64016b8d3493e8276f7b9c9...",
     "lease_sequence": 1042,
     "lease_expiry_epoch_ms": 1727078495000,
     "signature": "ed25519:sig:..."
   }
   ```
2. **Heartbeat Lease Renewal**: The Master renews its lease every 10 seconds.
3. **Graceful Handover to Superior Node**:
   When a desktop workstation ($S_{auth} = 9,500$) boots up and joins a cluster temporarily led by an Android smartphone ($S_{auth} = 3,800$), the desktop sends a `ClaimMasterRole` message presenting its superior score. The mobile Master flushes its pending task queue to the desktop, relinquishes leadership, and transitions to a worker node within $500\text{ms}$.

---

### 5.4 Zero-Latency Pre-Sleep Evacuation (`< 50ms`)

#### The Problem with Dead-Node Heartbeat Timeouts
Standard cluster systems detect node failure reactively when heartbeats cease:
$$\text{Timeout Window} = \text{Heartbeat Interval} \times \text{Missed Threshold} = 3\text{s} \times 4 = 12\text{s}$$
During these 12 seconds:
- In-flight tasks assigned to the dead worker remain blocked.
- Dependent compilation pipelines or map/reduce jobs stall.
- The Master wastes network bandwidth probing a dead socket.

#### Proactive Pre-Sleep Disconnect Protocol
Mobile nodes experience **predictable, detectable lifecycle events**:
- User screen lock or low-battery warning ($< 15\%$)
- Android system broadcasts: `ACTION_BATTERY_LOW`, `ACTION_SHUTDOWN`, `onTrimMemory(TRIM_MEMORY_RUNNING_CRITICAL)`
- Network switch down event from `ConnectivityManager`

Before the operating system suspends or terminates the process, the mobile worker executes a **Zero-Latency Graceful Evacuation**:

```
Android Worker                          Master Node
      │                                      │
      │  [1] Battery < 15% / Sleep Event     │
      ├─────────────────────────────────────►│
      │  WorkerMessage::Disconnecting {      │
      │    worker_id: Uuid,                  │
      │    reason: "BATTERY_CRITICAL",       │
      │    in_flight_tasks: [T1, T2]         │
      │  }                                   │
      │                                      │ [2] Immediate Re-queue:
      │                                      │     - Worker deregistered (<5ms)
      │                                      │     - T1, T2 placed at head of queue
      │                                      │     - Dispatched to healthy workers
      │                                      │     (Zero 12s timeout delay!)
      │                                      │
      │◄─────────────────────────────────────┤
      │  MasterMessage::ShutdownAck          │
      │  (or socket closed)                  │
      │                                      │
      │  [3] Clean Baseband Radio Release    │
```

1. Worker transmits `WorkerMessage::Disconnecting` with explicit eviction reason code.
2. Master immediately deregisters the worker and re-enqueues all in-flight tasks without waiting for any heartbeat timeout.
3. Master dispatches the orphaned tasks to other healthy nodes in $< 50\text{ms}$.
4. Worker cleanly closes the QUIC stream and releases wake locks, allowing the mobile device to sleep instantly.

---

## 6. Concrete Integration Roadmap & Verification Plan

### 6.1 Implementation Phases & Milestones

| Phase | Milestone | Key Architectural Deliverables |
|---|---|---|
| **Phase 1** | **JNI Bridge & Wire Protocol Extensions** | - Wire `p2p_ticket` into `crates/android_bridge::start_worker_impl`<br>- Add JNI `nativeUpdateTelemetry` for live battery and thermal injection<br>- Add JNI `nativeStartMaster` and `nativeStopMaster`<br>- Expose helper query functions for testing |
| **Phase 2** | **Android App & Service Modernization** | - Update `OxideWorkerBridge.kt` and `WorkerEngine.kt`<br>- Register `ACTION_BATTERY_CHANGED` receiver in `OxideWorkerService.kt`<br>- Register `OnThermalStatusChangedListener` in `OxideWorkerService.kt`<br>- Connect UI role switch and ticket input in `MainActivity.kt` |
| **Phase 3** | **Automated Cross-Compilation Toolchain** | - Implement `packaging/android/build_android_jni.sh`<br>- Automate NDK cross-compilation for `aarch64-linux-android`<br>- Automatically deploy `liboxideworker.so` to `jniLibs/arm64-v8a/` |
| **Phase 4** | **Testing & Verification** | - Unit and integration tests in `crates/android_bridge/tests/`<br>- Verify clean `cargo check --workspace` and `cargo test -p rusty_grid_android_bridge`<br>- Multi-agent review and forensic integrity audit |

---

### 6.2 Empirical Verification & Testing Framework

Verification follows a strict 6-point test matrix:

```
+─────────────────────────────────────────────────────────────────────────────+
│                       VERIFICATION TEST SUITE MATRIX                        │
+─────────────────────────────────────────────────────────────────────────────+
  Test ID  │ Test Scope                     │ Verification Command / Assertion
───────────┼────────────────────────────────┼──────────────────────────────────
  TC-01    │ P2P Ticket Configuration       │ `cargo test test_p2p_ticket_config`
           │ (Verify ticket passed to cfg)  │ Assert `WorkerConfig.p2p_ticket == Some(..)`
  TC-02    │ Live Telemetry Injection       │ `cargo test test_telemetry_injection`
           │ (Battery %, Charging, Thermal) │ Assert atomic telemetry cache updated
  TC-03    │ In-Process Worker Lifecycle    │ `cargo test test_worker_lifecycle`
           │ (Start, Status, Graceful Stop) │ Assert transitions: STOPPED->RUNNING->STOPPED
  TC-04    │ Mobile Coordinator Lifecycle   │ `cargo test test_master_lifecycle`
           │ (Start Master, Stop Master)    │ Assert bind, ticket returned, and clean stop
  TC-05    │ Compilation & Lints            │ `cargo check --workspace`
           │ (Zero compiler errors/warnings)│ Exit code 0
  TC-06    │ Full Bridge Test Suite         │ `cargo test -p rusty_grid_android_bridge`
           │ (100% integration tests pass)  │ Exit code 0, 0 failures
```

---

### 6.3 Failure Modes & Architectural Invariants

1. **Invariant 1 (Zero Subprocesses)**: Under no circumstances shall `liboxideworker.so` invoke `fork()` or `execve()` on Android. All async tasks run on Tokio pthreads.
2. **Invariant 2 (Non-Root Sandbox Safety)**: No telemetry routine shall invoke `/system/bin/dumpsys` or read restricted `/sys/class/` paths. All telemetry must be passed via JNI from the Android SDK.
3. **Invariant 3 (Battery Protection Rule)**: A mobile node whose battery is $< 15\%$ and discharging shall unconditionally reject incoming tasks and initiate pre-sleep disconnection.
4. **Invariant 4 (Zero-Port WAN Reachability)**: If direct UDP hole-punching fails, the client shall transparently route traffic over Iroh DERP relays via TLS 443 with 0 dropped frames.

---

*Specification authored, peer-reviewed, and verified for the OxideSwarm Native Architecture.*
