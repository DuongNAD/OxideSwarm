# 0002: Mobile coordination: thermal and battery throttling, adaptive keepalive and role arbitrage

Status: Partial
Created: 2026-10-23    Updated: 2026-11-07

## Summary

Coordinate mobile workers and coordinator election in heterogenous clusters:

- throttle worker concurrency dynamically based on battery and thermal limits ($C_{\text{eff}} = \min(C_{\text{bat}}, C_{\text{therm}})$);
- adapt keepalive heartbeats to Radio Resource Control (RRC) power states (10s active, 25s cellular idle, 60s Wi-Fi idle);
- enforce a strict five-rule refusal matrix before a mobile node can assume the coordinator role;
- calculate node authority scores ($S_{\text{auth}} \in [-10000, 10000]$) with deterministic tie-breaking for coordinator election;
- authenticate coordinator delegation through HMAC-SHA256 signed lease tickets (`ClaimMasterRole`);
- drain in-flight tasks in under 50 ms before mobile deep sleep.

## Context

Behaviour before this change:

| Area | Behaviour | Code |
|---|---|---|
| Mobile Throttling | A worker only checked a single static 15% battery cutoff. Below 15%, the worker tagged itself `low_battery` and rejected compile tasks, but continued accepting arbitrary command workloads. Thermal states were binary (`thermal_throttled`). | `crates/worker/src/client.rs`; `crates/master/src/scheduler.rs` |
| Keepalive | Workers used a static 3-second heartbeat interval regardless of transport or power state. On cellular links, frequent wakeups prevented mobile radios from entering low-power RRC Idle states, draining batteries rapidly. | `WorkerConfig::default_heartbeat_interval` in `crates/worker/src/client.rs` and `ServerConfig::heartbeat_interval_secs` in `crates/master/src/server.rs` |
| Coordinator Role | Mobile nodes could act as master without checking charging state, network type, or foreground service elevation. A mobile master going into deep sleep crashed the cluster. | `crates/master/src/server.rs` |
| Election & Lease | No mechanism existed for workers to elect a replacement coordinator or verify lease validity cryptographically. Master failure required manual operator intervention. | `crates/core/src/cluster_auth.rs` |
| Sleep Evacuation | When Android OS signalled impending sleep, workers dropped TCP sockets abruptly without returning uncompleted tasks to the queue. In-flight tasks stalled until the dead-worker timeout expired. | `crates/worker/src/client.rs` |

## Goals

- **G1** Shed compute load progressively across four battery tiers and four thermal tiers, scaling effective concurrency to zero when critical.
- **G2** Preserve mobile battery life by adapting heartbeat intervals to 10s (active), 25s (cellular idle), and 60s (Wi-Fi/Ethernet idle), with dynamic dead peer windows on the master.
- **G3** Prevent mobile nodes from becoming master when running on battery, below 50% charge, on cellular data, thermally throttled, or without an active foreground service.
- **G4** Elect coordinators deterministically using a weighted authority score ($S_{\text{auth}} \ge 2500$) with four-stage tie-breaking.
- **G5** Delegate coordinator authority using 30-second HMAC-SHA256 signed lease tickets renewed every 10 seconds.
- **G6** Return active tasks to the master queue in under 50 ms when an OS pre-sleep signal is received.

## Non-goals

- Checkpointing process memory across distinct OS architectures.
- Managing cellular data usage budgets or APN configurations.
- Replacing consensus protocols (Raft/Paxos) for large multi-master server clusters.

## Design

### D1 Multi-tier thermal and battery scheduler throttling

The worker evaluates its physical telemetry and reports effective capacity $C_{\text{eff}}$ to the scheduler:

$$C_{\text{eff}} = \min(C_{\text{bat}}, C_{\text{therm}})$$

Battery tiers (`BatteryTier` in `crates/core/src/capabilities.rs`, discharging):
- **`BatteryTier::Normal` ($\ge 50\%$):** $C_{\text{bat}} = C_{\text{max}}$. Accepts all task types.
- **`BatteryTier::Low` ($30\%$ to $49\%$):** $C_{\text{bat}} = \max(1, \lfloor C_{\text{max}} / 2 \rfloor)$. Rejects heavy (`RustCompilation` and `GpuCompute`) tasks.
- **`BatteryTier::SubLow` ($15\%$ to $29\%$):** $C_{\text{bat}} = 1$. Rejects heavy (`RustCompilation` and `GpuCompute`) tasks and multi-core tasks (`cpu_cores > 1` in `crates/master/src/scheduler.rs:650-653`); accepts single-core command and shell tasks.
- **`BatteryTier::Critical` ($< 15\%$):** $C_{\text{bat}} = 0$. Rejects all incoming tasks.

When connected to external power (`is_charging == true`) or when no battery is present, battery capacity throttling is bypassed (`BatteryTier::Normal`, $C_{\text{bat}} = C_{\text{max}}$).

Thermal tiers (`ThermalTier` in `crates/core/src/capabilities.rs`):
- **`ThermalTier::Normal` ($\le 37.0^\circ\text{C}$ and not throttled):** $C_{\text{therm}} = C_{\text{max}}$.
- **`ThermalTier::Warm` ($> 37.0^\circ\text{C}$ to $40.0^\circ\text{C}$, or `thermal_throttled` without a $> 40.0^\circ\text{C}$ reading):** $C_{\text{therm}} = \max(1, \lfloor C_{\text{max}} / 2 \rfloor)$. Rejects heavy (`RustCompilation` and `GpuCompute`) tasks when discharging.
- **`ThermalTier::Hot` ($> 40.0^\circ\text{C}$ to $42.0^\circ\text{C}$):** $C_{\text{therm}} = 1$. Rejects heavy (`RustCompilation` and `GpuCompute`) tasks and multi-core tasks (`cpu_cores > 1` in `crates/master/src/scheduler.rs:666-669`).
- **`ThermalTier::Critical` ($> 42.0^\circ\text{C}$):** $C_{\text{therm}} = 0$. Rejects all tasks until temperature normalises.

Implemented in `crates/core/src/capabilities.rs` (`MobileLiveStatus::compute_battery_tier`, `MobileLiveStatus::compute_thermal_tier`, and `calculate_effective_concurrency`; `MobileLiveStatus::effective_concurrency` and `is_task_eligible` are unit-test helpers only), stored on the master in `crates/master/src/registry.rs` (`WorkerEntry::mobile_status` and `WorkerInfo::mobile_status`, updated by `WorkerRegistry::update_mobile_telemetry` while holding newly registered and re-registered mobile workers for up to `MOBILE_INITIAL_TELEMETRY_HOLD = 4 s` (`2 × MOBILE_TELEMETRY_CHECK_INTERVAL`) until their first `MobileTelemetry` arrives, clearing `battery_tier` on re-registration because `Register` carries a fresh battery reading, and retaining prior `battery_temperature` and `thermal_tier` across re-registration until the new session's first `MobileTelemetry` or `PRESERVED_THERMAL_TTL = 10 s` expires), and enforced by the master scheduler in `crates/master/src/scheduler.rs` (`effective_mobile_live_status`, `effective_scheduler_battery_tier`, and `effective_scheduler_thermal_tier` in `Engine::new` and `Engine::candidates`, which treat desktop/laptop OSes `macos`, `windows`, and non-Android `linux` as `BatteryTier::Normal` whenever battery is above `BatteryTier::Critical`). Host sensing in `crates/core/src/capabilities.rs` reads `/sys/class/power_supply` and `/sys/class/thermal` on Linux/Android (with `dumpsys battery` and `dumpsys thermalservice` fallbacks and JNI `OxideWorkerBridge.nativeUpdateMobileTelemetry` on Android), reads `pmset -g batt` on macOS (without die/battery temperature sensing), and returns `None` on Windows unless fed via `MobileTelemetryHandle` or `TEST_BATTERY_PROBE` (`MOB-2`).

### D2 RRC-aware adaptive keepalive controller

Mobile cellular radios operate in distinct Radio Resource Control (RRC) power states: RRC Connected (high power), RRC Inactive, and RRC Idle (low power). Constant 3-second heartbeats keep the cellular baseband continuously powered.

The adaptive keepalive controller dynamically selects heartbeat interval $\tau_{\text{worker}}$:
- **Active state (task executing in flight):** $\tau_{\text{worker}} = 10\text{ s}$. Ensures timely detection of worker stalls during execution.
- **Cellular idle state (no tasks, cellular link):** $\tau_{\text{worker}} = 25\text{ s}$. Matches cellular RRC tail timers, allowing baseband dormancy between heartbeats.
- **Wi-Fi / Ethernet idle state (unmetered LAN):** $\tau_{\text{worker}} = 60\text{ s}$. Minimises background radio wakeups.

The master calculates the dead peer window dynamically from the worker's reported interval:

$$W_{\text{dead}} = \max(2\text{ s}, 4 \times \tau_{\text{worker}})$$

Implemented in `crates/core/src/capabilities.rs` (`MobileLiveStatus::adaptive_keepalive_interval`, `MobileLiveStatus::recommended_keepalive_interval_or`, `MobileLiveStatus::recommended_keepalive_interval`, `calculate_keepalive_interval`), `crates/worker/src/heartbeat.rs` (`spawn_adaptive_heartbeat_task`), `crates/worker/src/client.rs` (`run_session`), and `crates/master/src/registry.rs` (`calculate_master_dead_peer_window`).

When a worker registers with `capabilities.mobile`, `Register` carries only `MobileCapabilities` (no `network_type`), so:
- The master seeds a $4 \times 60\text{ s}$ ($240\text{ s}$) dead-peer window on `Register` until the worker sends its first `WorkerMessage::MobileTelemetry` and subsequent `WorkerMessage::Heartbeat` of the session.
- A worker connected to a master announcing `mobile-telemetry` sends an initial `MobileTelemetry` frame immediately after `RegisterAck` (`last_reported` starts at `None`). When `network_type` is absent or unknown (such as on a battery-powered laptop, or while Wi-Fi roams), both the worker and the master use the negotiated heartbeat interval (`None` in `WorkerEntry::heartbeat_interval`, falling back to the reaper's `--heartbeat-timeout`).
- The master never shortens `heartbeat_interval` on task dispatch (`reserve_dispatch` / `increment_active_tasks`) or on `update_mobile_telemetry` (preserving the existing window when a telemetry report has a shorter interval or `network_type: None`); `record_heartbeat` applies the shortened or `None` window once the worker's next `Heartbeat` arrives, while `HeartbeatTracker` and `MobileTelemetryHandle` notify `spawn_adaptive_heartbeat_task` to restart its timer immediately whenever `active_tasks` rises or a new telemetry reading gives a shorter interval.
- Calling `MobileTelemetryHandle::set` or `MobileTelemetryHandle::update` marks the handle as externally fed (`MobileTelemetryHandle::is_externally_fed`), preventing the background host battery poller (`spawn_mobile_poller` / `update_polled` in `crates/worker/src/heartbeat.rs`) from overwriting readings pushed by the Android JNI bridge or test fixtures.
- The worker's dead-peer watchdog (`dead_after` in `run_session`) recomputes its silence threshold on every watchdog tick from the longest interval `spawn_adaptive_heartbeat_task` can pick (`MOB-1`).
- When a subsequent `MobileTelemetry` frame carries `network_type: None`, `"unknown"`, `"none"`, or `""` (sanitized to `None` before the retention check on the master), `WorkerRegistry::update_mobile_telemetry` retains the previously reported `network_type` (`wifi` or `cellular`) on `WorkerEntry::mobile_status`. The master therefore continues showing the last known `wifi` or `cellular` link and errs on the side of a longer dead-peer window (`MOB-2`).

### D3 Strict five-rule mobile master refusal matrix

> **Coordinator role on Android removed by decision D2 (`SEC-3`):** The Android application is a worker only and never runs an embedded master. The five-rule refusal matrix below remains in `crates/core/src/arbitrage.rs` as a library helper with unit tests, and has no production caller.

In the library helper, before a mobile node could assume or maintain the coordinator role, it checks five invariants:

| Rule | Requirement | Failure Reason |
|---|---|---|
| R1 | External power connected | `MasterRefusalReason::NotCharging` |
| R2 | Battery level $\ge 50\%$ (`MIN_MASTER_BATTERY_PCT`) | `MasterRefusalReason::BatteryBelowThreshold` |
| R3 | Unmetered network (Wi-Fi or Ethernet) | `MasterRefusalReason::CellularOrMeteredNetwork` |
| R4 | Battery temp $\le 40.0^\circ\text{C}$ (`MAX_MASTER_BATTERY_TEMP_C`) and unthrottled | `MasterRefusalReason::ThermalHeadroomExceeded` |
| R5 | Foreground service active | `MasterRefusalReason::ForegroundServiceInactive` |

On `PlatformType::Android`, if any condition fails (`check_mobile_master_eligibility` returns `Err`), `calculate_authority_score` returns `total_score: SCORE_MIN` (`-10000`, `crates/core/src/arbitrage.rs:21`, `:287-308`).

Defined in `crates/core/src/arbitrage.rs` (`evaluate_mobile_master_refusal_matrix`, `check_mobile_master_eligibility`, `evaluate_mobile_master_refusal`).

### D4 Deterministic weighted authority scoring and election

Each node calculates its authority score $S_{\text{auth}} \in [-10000, 10000]$ (`SCORE_MIN` to `SCORE_MAX`):

$$S_{\text{auth}} = W_{\text{plat}} + W_{\text{pwr}} + W_{\text{net}} + W_{\text{therm}} + W_{\text{up}}$$

Weights (`crates/core/src/arbitrage.rs`):
- **Platform ($W_{\text{plat}}$, `PlatformType::weight`):** `LinuxServer` (+5000), `MacOs` (+4500), `Windows` (+4000), `Android` (+500), `Unknown` (0).
- **Power ($W_{\text{pwr}}$):** AC/Desktop (+2000), Battery $> 80\%$ (+500), Battery $30\text{--}80\%$ (+100), Battery $< 30\%$ (-3000).
- **Network ($W_{\text{net}}$, string matching on lowercase `network_type` in `calculate_authority_score`):** cellular/metered matched by `is_cellular_or_metered` (`"cell"`, `"mobile"`, `"lte"`, `"5g"` / `"5gnr"` as an alphanumeric token such as `"5g"` or `"5G NR"` but not inside `"5ghz"`, `"4g"`, `"3g"`, `"cgnat"`, `"metered"`: -2000), Ethernet/LAN (`"ethernet"`, `"eth"`, `"lan"`, `"gigabit"`: +1500; also +1500 when `network_type` is `None` on non-Android platforms), 5 GHz / Wi-Fi 6 (`"wifi6"`, `"wifi 6"`, `"5ghz"`, `"5 ghz"`: +1000), standard Wi-Fi (`"wifi"`, `"wlan"`: +700), other / `None` on Android (0).
- **Thermal ($W_{\text{therm}}$):** Nominal $\le 37.0^\circ\text{C}$ and unthrottled (0), Warm $> 37.0^\circ\text{C}$ to $40.0^\circ\text{C}$ (-1500), Throttled or $> 40.0^\circ\text{C}$ (-5000).
- **Uptime ($W_{\text{up}}$):** $\min(1000, \lfloor \text{uptime\_seconds} / 10 \rfloor)$.

A node is eligible for election only if $S_{\text{auth}} \ge 2500$ (`ELECTION_THRESHOLD_SCORE`, `crates/core/src/arbitrage.rs:18`). Ties are resolved deterministically across four stages in `CandidateRanking::cmp` and `resolve_election_tie`:
1. Higher authority score $S_{\text{auth}}$ (`authority_score`).
2. Higher platform weight $W_{\text{plat}}$ (`platform_weight`).
3. Longer node uptime (`uptime_seconds`).
4. Lexicographically lower node UUID (`node_id.as_bytes()`).

Defined in `crates/core/src/arbitrage.rs` (`calculate_authority_score`, `CandidateRanking`, `resolve_election_tie`); automatic coordinator election remains **Design only** (`FEAT-6`).

### D5 Cryptographic master lease tickets and role handover

Coordinator authority is governed by a `MasterLeaseTicket`:
- **Validity:** 30 seconds (`LEASE_VALIDITY_DURATION_MS = 30000`).
- **Renewal interval:** 10 seconds (`LEASE_RENEWAL_INTERVAL_MS = 10000`).
- **Cryptographic signature:**

$$\text{HMAC-SHA256}(\text{cluster\_secret}, \text{"oxs1-master-lease"} \parallel \text{cluster\_id} \parallel \text{master\_id} \parallel \text{lease\_sequence} \parallel \text{authority\_score} \parallel \text{issued\_at\_epoch\_ms} \parallel \text{expires\_at\_epoch\_ms})$$

When an elected worker claims coordinator role, it transmits `WorkerMessage::ClaimMasterRole { ticket }`. `MasterLeaseTicket::verify` (`crates/core/src/arbitrage.rs:528-558`) checks expiry (`current_epoch_ms > expires_at_epoch_ms`), time range (`expires_at_epoch_ms <= issued_at_epoch_ms`), and the 32-byte HMAC-SHA256 signature (when a `ClusterSecret` is supplied), and `evaluate_master_claim` compares the claimant against the incumbent.

Types and verification helpers live in `crates/core/src/arbitrage.rs` (`MasterLeaseTicket`, `evaluate_master_claim`). Per [0026](0026-refuse-claim-master-role.md), `ProtocolCapabilities::ROLE_ARBITRAGE` (`1 << 7`, `crates/core/src/protocol.rs:66`) is not advertised in `ProtocolCapabilities::all_supported()`, and all `WorkerMessage::ClaimMasterRole` arms in `crates/master/src/server.rs` call `reject_master_role_claim` to refuse every claim with `MasterMessage::RoleHandoverAck { accepted: false, reason: Some("Automatic master failover is not supported; use manual role switching".into()), task_queue_snapshot: Vec::new(), connected_workers: Vec::new() }`. No production code constructs `ClaimMasterRole` and automatic coordinator failover/election remains **Design only** (role switching is performed manually via `switch_role.sh` / `windows/switch_role.cmd`).

### D6 Pre-sleep fast task evacuation hook

When the mobile operating system notifies the worker of impending sleep or background termination, the worker initiates fast evacuation:
1. Sets the evacuation reason and triggers the worker shutdown signal (`trigger_fast_evacuation_impl` and `Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeTriggerFastEvacuation` at `crates/android_bridge/src/lib.rs:946`, which delegates to `Java_com_oxideswarm_worker_service_OxideWorkerBridge_nativeTriggerFastEvacuation`, wired via `set_evacuation_reason_hook` and `current_evacuation_reason` in `crates/worker/src/client.rs`).
2. Sends a `WorkerMessage::Disconnecting { worker_id, reason }` frame to the master (`crates/core/src/protocol.rs:464`; there is no separate `drain` boolean field).
3. The master calls `TaskQueue::handle_session_disconnected` with `immediate_reschedule: true` (`crates/master/src/queue.rs:1921-1926`, invoked from `crates/master/src/server.rs`) to requeue in-flight tasks assigned to that worker without retry backoff delay (note: `immediate_reschedule: true` currently still increments `retry_count` and `failover_count` until `MOB-3` lands).
4. Completes socket teardown with a design target of $< 50\text{ ms}$ prior to OS suspension (the integration test `test_f15_pre_sleep_evacuation_under_50ms` in `tests/mobile_coordination_test.rs:920` asserts `< 100 ms`; note that `packaging/android/app/app/src/main/java/com/oxideswarm/worker/runner/OxideWorkerBridge.kt` does not yet declare or invoke `nativeTriggerFastEvacuation` until `AND-3` lands).

Implemented in `crates/worker/src/client.rs` (`set_evacuation_reason_hook`, `current_evacuation_reason`), `crates/android_bridge/src/lib.rs` (`trigger_fast_evacuation_impl`), and `crates/master/src/server.rs`.

## Compatibility

- Wire format carries lease tickets and disconnect reasons inside existing protocol enums (`WorkerMessage::ClaimMasterRole`, `WorkerMessage::Disconnecting`).
- Uses protocol version 2 (`PROTOCOL_VERSION_CURRENT = 2`, `PROTOCOL_VERSION_MIN_COMPATIBLE = 2`, `crates/core/src/protocol.rs:31-33`); capability bit `ProtocolCapabilities::ROLE_ARBITRAGE = Self(1 << 7)` (`crates/core/src/protocol.rs:66`) is reserved for design-only role arbitrage and is not advertised in `ProtocolCapabilities::all_supported()`.
- Unaware nodes ignore role arbitration frames and continue operating as normal workers.
- **Rolling upgrade windows (`MOB-1`, `DEPLOYMENT.md` §3.5):**
  - *Pre-`MOB-1` battery-powered laptop workers:* Restarting the master before upgrading workers leaves pre-`MOB-1` battery-powered laptop workers heartbeating every $60\text{ s}$ when idle. On `Register`, the master grants a $4 \times 60\text{ s}$ ($240\text{ s}$) window; if an older laptop worker emits a `MobileTelemetry` frame with `network_type: None` on a battery percentage change and then sends a `Heartbeat` before its binary is upgraded, the master switches that session's dead-peer window to `--heartbeat-timeout` ($10\text{ s}$ default) and will reap and reconnect the old laptop worker after $10\text{ s}$ of idle silence until the worker is upgraded (or `--heartbeat-timeout` is set $\ge 240\text{ s}$ during the upgrade window).
  - *Pre-`MOB-1` Android Wi-Fi phones:* A phone running an APK from before `MOB-1` heartbeats every $60\text{ s}$ on Wi-Fi even while executing a task (`active_tasks > 0`). Once that phone sends a `Heartbeat` with `active_tasks > 0`, the new master sets `heartbeat_interval = Some(10s)` (a $40\text{ s}$ dead-peer window). Consequently, any task longer than $\sim 40\text{ s}$ on a pre-`MOB-1` Wi-Fi phone fails over until the APK is updated (`--heartbeat-timeout` does not override this because `heartbeat_interval` is `Some(10s)`).

## Security

- All lease tickets require HMAC-SHA256 verification using the shared `cluster.secret`. An attacker without the secret cannot forge coordinator tickets or trigger false master takeovers.
- The 30-second lease ticket lifetime bounds replay windows. Expired or future-dated lease tickets are rejected.
- Authority scores are computed locally, but lease tickets require the cluster secret, preventing unauthenticated mobile nodes from assuming leadership.

## Test plan

- **Unit:** battery tier transitions, thermal concurrency caps, 5-rule refusal matrix evaluations, authority score math, and lease ticket signature verification (`tests/mobile_coordination_test.rs`).
- **Stress:** boundary testing for 0%, 14%, 15% battery states, rapid thermal tier oscillation, and 4-stage tie-breaking fuzzing (`tests/m5a_challenger_1_stress.rs`, `tests/m5a_challenger_2_stress.rs`).
- **Integration:** pre-sleep evacuation timing under load (`test_f15_pre_sleep_evacuation_under_50ms` in `tests/mobile_coordination_test.rs:920`, asserting reassignment within $< 100\text{ ms}$ against the $< 50\text{ ms}$ design target) and zero task loss.
- **Gates:** `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Arbitrage and refusal module | `crates/core/src/arbitrage.rs` | Refusal matrix, $S_{\text{auth}}$ calculation, and lease ticket types implemented with unit tests (Android coordinator role removed by D2; no production election caller) | Partial |
| W2 | Scheduler multi-tier throttling | `crates/core/src/capabilities.rs`, `crates/master/src/registry.rs`, `crates/master/src/scheduler.rs` | Concurrency scaled by $C_{\text{eff}} = \min(C_{\text{bat}}, C_{\text{therm}})$ in matchmaking using `WorkerInfo::mobile_status` (`MOB-2`) | Implemented |
| W3 | Adaptive keepalive controller | `crates/core/src/capabilities.rs`, `crates/worker/src/heartbeat.rs`, `crates/worker/src/client.rs`, `crates/master/src/registry.rs` | 10s active on any link, 25s cellular idle, 60s Wi-Fi idle, negotiated interval when `network_type` is unknown, and dynamic $W_{\text{dead}}$ calculation (`MOB-1`) | Implemented |
| W4 | Master lease claim handling | `crates/master/src/server.rs` | `ROLE_ARBITRAGE` (`1 << 7`, `protocol.rs:66`) is not advertised in `all_supported()`; all `WorkerMessage::ClaimMasterRole` arms in `server.rs` call `reject_master_role_claim` to refuse every claim with `RoleHandoverAck { accepted: false, task_queue_snapshot: Vec::new(), connected_workers: Vec::new() }` ([0026](0026-refuse-claim-master-role.md)); nothing constructs `ClaimMasterRole` | Design only |
| W5 | Pre-sleep fast evacuation | `crates/worker/src/client.rs`, `crates/android_bridge/src/lib.rs`, `crates/master/src/server.rs` | `Disconnecting { worker_id, reason }` frame sent and tasks requeued via `handle_session_disconnected(..., immediate_reschedule: true)` (`< 50 ms` design target, tested at `< 100 ms` in `tests/mobile_coordination_test.rs:920`; `immediate_reschedule` still increments retry counts, `MOB-3`; Kotlin `runner/OxideWorkerBridge.kt` JNI call pending, `AND-3`) | Partial |
| W6 | Stress test suites | `tests/mobile_coordination_test.rs`, `tests/m5a_challenger_1_stress.rs`, `tests/m5a_challenger_2_stress.rs` | All stress tests pass with 0 failures | Implemented |

## Status log

- 2026-10-23: Created and accepted for Milestone M5A.
- 2026-10-23: Implemented W1–W6 in branch `feat/m5a-mobile-coordination`.
- 2026-10-23: Verified via `cargo test -p rusty_grid_android_bridge --test mobile_coordination_test`, `m5a_challenger_1_stress`, and `m5a_challenger_2_stress` (16 passing tests).
- 2026-10-23: Fixed 0% battery boundary assertion in `tests/m6_challenger_2_stress.rs` to enforce full task refusal on critical battery. Status marked Implemented.
- 2026-11-04: Aligned D1–D6 symbols, battery tiers (`50/30/15`), thermal tiers (`37/40/42`), authority weights, 4-stage tie-breaking, `MasterLeaseTicket` canonical bytes, and fast evacuation hooks with the code; marked Android coordinator role as removed by decision D2 (`SEC-3`) and updated record status to `Partial` (`DOC-3`).
- 2026-11-04: Removed `ROLE_ARBITRAGE` from `ProtocolCapabilities::all_supported()` and replaced all `WorkerMessage::ClaimMasterRole` arms in `crates/master/src/server.rs` with `reject_master_role_claim` returning `RoleHandoverAck { accepted: false, task_queue_snapshot: Vec::new(), connected_workers: Vec::new() }`; marked W4 as `Design only` per decision D1 (`FEAT-6`, [0026](0026-refuse-claim-master-role.md)).
- 2026-11-04: Returned 10s keepalive when `active_tasks > 0` on any link, kept the negotiated interval when `network_type` is unknown, and marked W3 as `Implemented` (`MOB-1`).
- 2026-11-04: Stored `mobile_status: Option<MobileLiveStatus>` on `WorkerEntry` and `WorkerInfo`, wired `effective_mobile_live_status` and `effective_scheduler_thermal_tier` into `Engine::new` and `Engine::candidates`, documented macOS/Windows battery and thermal sensing, and marked W2 as `Implemented` (`MOB-2`).
- 2026-11-07: Kept a $4 \times 60\text{ s}$ registration window until the session's first `MobileTelemetry`, sent an initial `MobileTelemetry` immediately after `RegisterAck`, deferred lowering `heartbeat_interval` until `record_heartbeat` reports `active_tasks > 0`, recomputed worker `dead_after` from the longest adaptive interval, and documented the `DEPLOYMENT.md` §3.5 laptop upgrade window (`MOB-1`).
- 2026-11-07: Preserved `mobile_status` thermal and network fields across worker re-registration until fresh `MobileTelemetry` arrives, and documented that `update_mobile_telemetry` retains the last known `network_type` when a telemetry update carries `None` (`MOB-2`).
