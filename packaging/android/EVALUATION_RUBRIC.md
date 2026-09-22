# OxideSwarm Android Background Execution: 10-Point Step-by-Step Evaluation Rubric

**Document ID:** OS-EVAL-AND-01  
**Target Milestone:** R3 — Android Background Execution & Packaging Architecture  
**Target Platform:** Android 7.0 (API 24) through Android 15+ (API 35)  
**Evaluator Audience:** Independent QA Auditors, Forensic Verification Agents, Automation Engineers  
**Applicable Implementations:** Native Foreground Service App (`packaging/android/app/`) & Termux Daemon (`packaging/android/termux/`)

---

## Executive Summary & Purpose

This rubric establishes an authoritative, empirical, and reproducible methodology to verify that the OxideSwarm Android packaging technically and definitively prevents the Android operating system from suspending, freezing, or terminating the worker process.

Modern Android OS enforces multi-tiered power-saving and resource-reclamation mechanisms:
1. **Doze Mode (API 23+)**: Suspends background CPU and disables network access when screen is off and stationary.
2. **App Standby Buckets (API 28+)**: Throttles jobs and network for non-active apps.
3. **Low Memory Killer Daemon (LMKD)**: Reaps processes under RAM pressure based on `oom_score_adj` (from 1000 down to 0).
4. **Linux Cgroups Freezer / `CachedAppOptimizer` (API 30+)**: Writes `FROZEN` to the cgroup freezer when apps enter the cached state, halting all thread execution (equivalent to `SIGSTOP`).
5. **Phantom Process Killer (PPK) (API 31–35)**: Silently terminates background child processes with `SIGKILL` (signal 9) if CPU usage exceeds aggressive limits.
6. **OEM Battery Savers**: Vendor daemons (Samsung One UI, Xiaomi HyperOS/MIUI, Huawei EMUI) that aggressively force-stop background apps.

The 10 evaluation test cases below mathematically and empirically verify immunity against every single one of these failure modes.

---

## Test Environment Setup

### Hardware & Software Prerequisites
- **Android Device or Emulator**: Android 10+ (Android 14/15 recommended for full FGS validation).
- **Host Machine**: macOS, Linux, or Windows with Android Debug Bridge (`adb`) installed and configured in `PATH`.
- **Master Node**: An active OxideSwarm Master coordinator reachable from the device (e.g., `192.168.1.100:8080` on local Wi-Fi, or via ADB reverse proxy `adb reverse tcp:8080 tcp:8080`).

### Quick ADB Pre-Flight Command:
```bash
# Verify device connection and authorization
adb devices -l
# Verify adb root or shell access
adb shell getprop ro.build.version.release
```

---

## 10-Point Step-by-Step Evaluation Matrix

---

### TC-01: Static Package & Manifest Audit
* **Objective:** Verify that the Android package declares all mandatory permissions, foreground service types, and broadcast receivers required by Android 10–15+.
* **Mechanism Under Test:** Android Package Manager & Security Policy.
* **Execution Command:**
  ```bash
  # If checking compiled APK:
  aapt2 dump badging packaging/android/app/app/build/outputs/apk/release/app-release.apk 2>/dev/null \
    | grep -E "uses-permission|service|receiver" || true

  # Or inspect the repository source manifest directly:
  grep -E "uses-permission|foregroundServiceType|property" packaging/android/app/app/src/main/AndroidManifest.xml
  ```
* **Expected Output Snippet:**
  ```text
  uses-permission: name='android.permission.INTERNET'
  uses-permission: name='android.permission.FOREGROUND_SERVICE'
  uses-permission: name='android.permission.FOREGROUND_SERVICE_SPECIAL_USE'
  uses-permission: name='android.permission.FOREGROUND_SERVICE_DATA_SYNC'
  uses-permission: name='android.permission.WAKE_LOCK'
  uses-permission: name='android.permission.REQUEST_IGNORE_BATTERY_OPTIMIZATIONS'
  uses-permission: name='android.permission.RECEIVE_BOOT_COMPLETED'
  uses-permission: name='android.permission.POST_NOTIFICATIONS'
  android:foregroundServiceType="specialUse|dataSync"
  android:name="android.app.PROPERTY_SPECIAL_USE_FGS_SUBTYPE"
  ```
* **Pass / Fail Determination:**
  * **PASS:** All 8 critical permissions, `foregroundServiceType` attribute, and Android 14 `PROPERTY_SPECIAL_USE_FGS_SUBTYPE` property are strictly present.
  * **FAIL:** Any missing permission, missing FGS type (causes crash on Android 14+), or missing `RECEIVE_BOOT_COMPLETED`.
* **Severity:** **CRITICAL**

---

### TC-02: Live OOM Score Adjustment Elevation Audit
* **Objective:** Verify that the running worker process has an elevated out-of-memory score adjustment (`oom_score_adj <= 200`), rendering it immune to standard LMKD background sweeps.
* **Mechanism Under Test:** Linux Kernel OOM killer & Android Low Memory Killer Daemon (LMKD).
* **Execution Command:**
  ```bash
  # 1. Obtain PID of the active worker process
  PID=$(adb shell pidof com.oxideswarm.worker)
  echo "Active Worker PID: $PID"

  # 2. Inspect kernel OOM score adjustment
  adb shell cat /proc/$PID/oom_score_adj

  # 3. Query ActivityManager process state
  adb shell dumpsys activity processes | grep -A 4 -B 2 "PID #$PID"
  ```
* **Expected Output Snippet:**
  ```text
  Active Worker PID: 14820
  200
  *ProcessRecord{... 14820:com.oxideswarm.worker/u0a198}
    oomAdj=200 minAdj=200 curRaw=200 setRaw=200
    curProcState=4 (FOREGROUND_SERVICE)
  ```
* **Pass / Fail Determination:**
  * **PASS:** `oom_score_adj` is strictly **<= 200** (typically `200` for `PERCEPTIBLE_APP`/Foreground Service, or `0` when UI is open).
  * **FAIL:** `oom_score_adj` is `>= 900` (indicates the process is categorized as a cached app and will be killed upon minor memory pressure).
* **Severity:** **CRITICAL**

---

### TC-03: Active Hardware CPU Wake-Lock Verification
* **Objective:** Verify that a persistent hardware `PARTIAL_WAKE_LOCK` is registered with the Android Power Manager, preventing the CPU from entering deep sleep when the screen turns off.
* **Mechanism Under Test:** Android `PowerManagerService` & Kernel Wakeup Source Subsystem.
* **Execution Command:**
  ```bash
  adb shell dumpsys power | grep -E -A 6 "Wake Locks: size=" | grep -E "OxideSwarm|PARTIAL_WAKE_LOCK"
  ```
* **Expected Output Snippet:**
  ```text
  PARTIAL_WAKE_LOCK 'OxideSwarm::CpuWakeLock' ACQ=-12m30s (uid=10198 pid=14820)
  ```
  *(Or if testing Termux: `PARTIAL_WAKE_LOCK 'termux-wake-lock'`)*
* **Pass / Fail Determination:**
  * **PASS:** A `PARTIAL_WAKE_LOCK` tagged with `OxideSwarm::CpuWakeLock` (or `termux-wake-lock`) is actively held by the worker UID and registered in `dumpsys power`.
  * **FAIL:** No active partial wake lock exists, or wake lock is released immediately after screen turn-off.
* **Severity:** **CRITICAL**

---

### TC-04: Low-Latency / High-Performance Wi-Fi Lock Verification
* **Objective:** Verify that an active Wi-Fi lock is held (`WIFI_MODE_FULL_LOW_LATENCY` on Android 10+ / API 29+, with fallback to `WIFI_MODE_FULL_HIGH_PERF`), preventing the Wi-Fi radio from entering power-save beacon polling sleep during screen-off compute tasks.
* **Mechanism Under Test:** Android `WifiLockManager` & Kernel WLAN driver power-management. Note: `WIFI_MODE_FULL_HIGH_PERF` was deprecated in API 29 and may be restricted during screen-off sleep; modern Android 10+ devices require `WIFI_MODE_FULL_LOW_LATENCY` for sustained low-latency compute while the screen is off.
* **Execution Command:**
  ```bash
  adb shell dumpsys wifi | grep -E -A 4 "Locks acquired:" | grep -E "OxideSwarm|FULL_LOW_LATENCY|FULL_HIGH_PERF|low_latency|high_perf"
  ```
* **Expected Output Snippet:**
  ```text
  WifiLock{OxideSwarm::WifiLock type=low_latency uid=10198 pid=14820}
  ```
  *(Or `type=high_perf` / `type=full` on pre-Android 10 devices or devices without low-latency driver support)*
* **Pass / Fail Determination:**
  * **PASS:** Wi-Fi lock of type `low_latency`, `high_perf`, or `full` is active and held by the worker process, ensuring sustained low-latency networking during screen-off intervals.
  * **FAIL:** No Wi-Fi lock held, causing TCP sockets to drop packets or timeout during screen-off intervals.
* **Severity:** **HIGH**

---

### TC-05: Battery Optimization Exemption Whitelist Verification
* **Objective:** Verify that the OxideSwarm worker package is registered on the OS Battery Optimization Exemption whitelist, ensuring network and alarm access during Doze.
* **Mechanism Under Test:** Android `DeviceIdleController` Whitelist Registry.
* **Execution Command:**
  ```bash
  # Check system battery whitelist
  adb shell dumpsys deviceidle whitelist | grep com.oxideswarm.worker

  # Check AppOps background execution permission (exempt from OEM background restriction)
  adb shell cmd appops get com.oxideswarm.worker RUN_ANY_IN_BACKGROUND

  # (Optional setup for headless test harnesses without UI interaction):
  # adb shell dumpsys deviceidle whitelist +com.oxideswarm.worker
  # adb shell cmd appops set com.oxideswarm.worker RUN_ANY_IN_BACKGROUND allow
  ```
* **Expected Output Snippet:**
  ```text
  system-exc,com.oxideswarm.worker,10198
  allow
  ```
  *(Or `user-exc,com.oxideswarm.worker,10198` and `allow`)*
* **Pass / Fail Determination:**
  * **PASS:** `com.oxideswarm.worker` (or `com.termux` for Termux deployment) appears in the whitelist output and AppOps returns `allow` or `default` with whitelist exemption.
  * **FAIL:** Package does not appear in whitelist or AppOps reports `ignore`/`deny`.
* **Severity:** **CRITICAL**

---

### TC-06: Deep Doze Simulation & Heartbeat Continuity Stress Test
* **Objective:** Force the Android OS into Deep Doze (`IDLE`) with the display turned off, and verify that the worker maintains uninterrupted TCP socket connectivity and sends regular heartbeats for 15 consecutive minutes without drops.
* **Mechanism Under Test:** `DeviceIdleController` Deep Doze State Machine & TCP socket persistence.
* **Execution Procedure:**
  1. Ensure the worker is connected to an active Master coordinator (configured with heartbeat interval = 3s, timeout = 10s).
  2. Simulate battery unplug:
     ```bash
     adb shell dumpsys battery unplug
     ```
  3. Turn off device display (screen off):
     ```bash
     adb shell input keyevent 26
     ```
  4. Force the device state machine directly into Deep Doze:
     ```bash
     adb shell dumpsys deviceidle force-idle
     ```
  5. Step through Deep Doze phases until state reaches `IDLE`:
     ```bash
     adb shell dumpsys deviceidle step deep
     adb shell dumpsys deviceidle step deep
     # Assert current state
     adb shell dumpsys deviceidle get deep
     ```
     *Must output:* `IDLE`
  6. Maintain the device in this state for 15 minutes.
  7. Monitor Master coordinator logs:
     ```bash
     # Verify continuous heartbeat receipts
     tail -f master_heartbeat.log
     ```
  8. Restore battery state after test:
     ```bash
     adb shell dumpsys battery reset
     ```
* **Expected Output Snippet:**
  ```text
  [Master] Heartbeat received from worker 'android-Pixel_8' (seq: 410, cpu: 12%, ram: 2100MB)
  [Master] Heartbeat received from worker 'android-Pixel_8' (seq: 411, cpu: 11%, ram: 2100MB)
  [Master] Heartbeat received from worker 'android-Pixel_8' (seq: 412, cpu: 13%, ram: 2100MB)
  ...
  (Zero disconnect events or missed heartbeats recorded across 15 minutes)
  ```
* **Pass / Fail Determination:**
  * **PASS:** Worker maintains 3s heartbeats with **zero drops or disconnections** throughout the entire 15-minute Deep Doze simulation.
  * **FAIL:** Heartbeats cease, or Master flags the worker as timed out / disconnected.
* **Severity:** **CRITICAL**

---

### TC-07: In-Doze Computational Task Dispatch & Execution
* **Objective:** Prove that an incoming computational workload dispatched by the Master to the worker while in Deep Doze is accepted, processed, and completed immediately without waiting for an OS maintenance window.
* **Mechanism Under Test:** Tokio async runtime task receiver & process execution under Doze.
* **Execution Procedure:**
  1. While the device remains in Deep Doze (`IDLE`) with the display turned off (from TC-06):
  2. Dispatch a task from the Master CLI:
     ```bash
     rusty-grid submit --master 127.0.0.1:8080 \
       --type generic \
       --command "echo 'DOZE_EXECUTION_VERIFIED'" \
       --wait
     ```
  3. Observe execution response.
* **Expected Output Snippet:**
  ```text
  [INFO] Task 9f41b2 submitted to cluster.
  [INFO] Task assigned to worker: android-Pixel_8
  [INFO] Task completed successfully in 0.28s. Exit Code: 0.
  Output:
  DOZE_EXECUTION_VERIFIED
  ```
* **Pass / Fail Determination:**
  * **PASS:** Task finishes within regular execution time (< 3 seconds) with exit code 0, and output matches.
  * **FAIL:** Task hangs until device is woken up, times out, or fails with network unreachable error.
* **Severity:** **CRITICAL**

---

### TC-08: Linux Cgroups Freezer (`CachedAppOptimizer`) Exemption Verification
* **Objective:** Verify that Android's `CachedAppOptimizer` has not placed the worker process into the Linux kernel cgroup freezer.
* **Mechanism Under Test:** Linux Kernel Cgroup v1/v2 Freezer subsystem.
* **Execution Command:**
  ```bash
  PID=$(adb shell pidof com.oxideswarm.worker)

  # Check Cgroup v1 freezer
  adb shell "cat /sys/fs/cgroup/freezer/cgroup.procs 2>/dev/null | grep $PID || echo 'NOT_IN_CGROUP_V1'"

  # Check Cgroup v2 freeze state for the app UID
  UID=$(adb shell pm list packages -U | grep com.oxideswarm.worker | sed -n 's/.*uid:\([0-9]*\).*/\1/p' || adb shell stat -c %u /data/data/com.oxideswarm.worker)
  adb shell "cat /sys/fs/cgroup/uid_${UID}/cgroup.freeze 2>/dev/null || cat /sys/fs/cgroup/cgroup.freeze 2>/dev/null || echo 0"

  # Inspect thread states
  adb shell "cat /proc/$PID/status | grep -E '^State:'"
  ```
* **Expected Output Snippet:**
  ```text
  NOT_IN_CGROUP_V1
  0
  State:  S (sleeping)
  ```
* **Pass / Fail Determination:**
  * **PASS:** Process is not in any freezer cgroup (`cgroup.freeze` is `0`), and thread state is `S` (interruptible sleep) or `R` (running), never `T` (stopped) or `D` (uninterruptible frozen).
  * **FAIL:** `cgroup.freeze` is `1`, or threads report `T` (process frozen by OS).
* **Severity:** **HIGH**

---

### TC-09: Android 12+ Phantom Process Killer (PPK) Immunity Test
* **Objective:** Verify that intensive, multi-core computational tasks executed by the worker do NOT trigger `SIGKILL` (signal 9) from `ActivityManagerService`'s Phantom Process Killer.
* **Mechanism Under Test:** Android 12–15+ `PhantomProcessList` in `ActivityManagerService`.
* **Execution Procedure:**
  1. Clear logcat buffer:
     ```bash
     adb logcat -c
     ```
  2. Record current worker PID:
     ```bash
     INITIAL_PID=$(adb shell pidof com.oxideswarm.worker)
     echo "Initial PID: $INITIAL_PID"
     ```
  3. Submit a high-CPU task that executes for 30 seconds using a universal POSIX shell compute loop that runs on stock Android without requiring python:
     ```bash
     rusty-grid submit --master 127.0.0.1:8080 --type generic --command "sh -c 'end=\$((\$(date +%s)+30)); while [ \$(date +%s) -lt \$end ]; do :; done'" --wait
     ```
  4. Inspect logcat for PPK termination notices:
     ```bash
     adb logcat -d | grep -i -E "phantom|Killing phantom process|SIGKILL|Process completed \(signal 9\)" || true
     ```
  5. Check worker PID after workload completion:
     ```bash
     FINAL_PID=$(adb shell pidof com.oxideswarm.worker)
     echo "Final PID: $FINAL_PID"
     ```
* **Expected Output Snippet:**
  ```text
  Initial PID: 14820
  (Zero logcat entries matching phantom kills)
  Final PID: 14820
  ```
* **Pass / Fail Determination:**
  * **PASS:** Zero phantom process kills in logcat, `FINAL_PID` is identical to `INITIAL_PID`, and worker continues processing further tasks.
  * **FAIL:** Process disappears or logcat logs `Killing phantom process ... SIGKILL`.
* **Severity:** **CRITICAL**

---

### TC-10: Cold Device Reboot Auto-Start & Auto-Reconnect Verification
* **Objective:** Verify that upon a cold device reboot, the worker automatically resurrects via Direct Boot (`ACTION_LOCKED_BOOT_COMPLETED`) or standard boot completion (`ACTION_BOOT_COMPLETED`) without requiring manual app launch.
* **Mechanism Under Test:** Android `Intent.ACTION_LOCKED_BOOT_COMPLETED` & `Intent.ACTION_BOOT_COMPLETED` broadcasts via `directBootAware` `BootCompletedReceiver`.
* **FBE Direct Boot Architecture (BFU vs. AFU):**
  * **Before-First-Unlock (BFU):** On Android 7.0+ (API 24+) devices with File-Based Encryption (FBE) and a secure lock screen (PIN, password, pattern), Credential Encrypted (CE) storage remains locked until user authentication. Standard `ACTION_BOOT_COMPLETED` is withheld by `ActivityManagerService`. However, because `BootCompletedReceiver` declares `android:directBootAware="true"`, Android broadcasts `android.intent.action.LOCKED_BOOT_COMPLETED`. In BFU mode, the receiver accesses Device-Protected (DP) storage (`context.createDeviceProtectedStorageContext()`) to read worker configuration and initiate background execution before first unlock.
  * **After-First-Unlock (AFU):** Once the user enters credentials (or on unsecured/lab devices without a keyguard), CE storage is decrypted and `android.intent.action.BOOT_COMPLETED` is dispatched. The receiver safely handles both lifecycle events idempotently.
* **Execution Procedure:**
  1. Ensure the worker is configured with auto-start enabled in Device Protected storage.
  2. Issue a full system reboot via ADB:
     ```bash
     adb reboot
     ```
  3. Wait for device to complete boot:
     ```bash
     adb wait-for-device
     adb shell 'while [ "$(getprop sys.boot_completed)" != "1" ]; do sleep 1; done'
     ```
  4. (Optional) For BFU testing on secured devices, verify process launch prior to unlocking:
     ```bash
     # Check if directBootAware receiver launched the worker in BFU state
     adb shell pidof com.oxideswarm.worker
     ```
     *(If testing AFU transition, dismiss keyguard: `adb shell wm dismiss-keyguard` or `adb shell input keyguard dismiss`)*
  5. Wait 15 seconds for boot broadcast dispatch:
     ```bash
     sleep 15
     ```
  6. Check if worker process is running:
     ```bash
     adb shell pidof com.oxideswarm.worker
     ```
  7. Verify on Master coordinator:
     ```bash
     rusty-grid status --master 127.0.0.1:8080 --workers
     ```
* **Expected Output Snippet:**
  ```text
  17402
  Connected Workers (1):
    - ID: android-Pixel_8 | Status: Ready | Cores: 8 | RAM: 7850MB | OS: Android 14
  ```
* **Pass / Fail Determination:**
  * **PASS:** Worker process is running with a valid PID and automatically registered with the Master coordinator within 30 seconds of boot completion (via `LOCKED_BOOT_COMPLETED` in BFU or `BOOT_COMPLETED` in AFU).
  * **FAIL:** Worker process is not running, requiring manual launch.
* **Severity:** **HIGH**

---

## Evaluation Scorecard & Certification Summary

| Test Case | Test Description | Focus Area | Severity | Result |
| :--- | :--- | :--- | :--- | :--- |
| **TC-01** | Manifest & Permissions Audit | Permissions, FGS type, Subtype | **CRITICAL** | [ PASS / FAIL ] |
| **TC-02** | OOM Score Elevation Audit | `oom_score_adj <= 200` | **CRITICAL** | [ PASS / FAIL ] |
| **TC-03** | CPU Wake-Lock Verification | `PARTIAL_WAKE_LOCK` held | **CRITICAL** | [ PASS / FAIL ] |
| **TC-04** | Wi-Fi Lock Verification | `WIFI_MODE_FULL_LOW_LATENCY` / High-Perf | **HIGH** | [ PASS / FAIL ] |
| **TC-05** | Battery Optimization Exemption | System whitelist membership | **CRITICAL** | [ PASS / FAIL ] |
| **TC-06** | 15-Min Deep Doze Heartbeats | 0 heartbeat drops over 15 min | **CRITICAL** | [ PASS / FAIL ] |
| **TC-07** | In-Doze Task Execution | Instant task completion in Doze | **CRITICAL** | [ PASS / FAIL ] |
| **TC-08** | Linux Cgroups Freezer Immunity | Exemption from `CachedAppOptimizer`| **HIGH** | [ PASS / FAIL ] |
| **TC-09** | Phantom Process Killer Immunity| Zero `SIGKILL` on heavy CPU | **CRITICAL** | [ PASS / FAIL ] |
| **TC-10** | Cold Reboot Auto-Start (Direct Boot) | Resurrects on `LOCKED_BOOT` / `BOOT_COMPLETED` | **HIGH** | [ PASS / FAIL ] |

### Certification Standard:
* **Production Certified:** 10/10 tests PASS (All 7 Critical and all 3 High).
* **Conditionally Certified (Developer/Lab Only):** 8/10 tests PASS (Critical tests TC-01 through TC-07 must pass; TC-09 may require manual ADB PPK override if running in Termux).
* **Unacceptable / Rejected:** Any failure on TC-01, TC-02, TC-03, TC-05, TC-06, or TC-07.

---
**Verified By:** OxideSwarm Quality Assurance & Forensic Verification Team  
**Evaluation Specification Revision:** 1.0.0
