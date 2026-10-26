# 0002: Mobile coordination: thermal and battery throttling, adaptive keepalive and role arbitrage

Status: Implemented
Created: 2026-10-23    Updated: 2026-10-23

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
| Keepalive | Workers used a static 3-second heartbeat interval regardless of transport or power state. On cellular links, frequent wakeups prevented mobile radios from entering low-power RRC Idle states, draining batteries rapidly. | `DEFAULT_HEARTBEAT_INTERVAL` in `crates/core/src/protocol.rs` |
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

Battery tiers (discharging):
- **Normal (> 30%):** $C_{\text{bat}} = C_{\text{max}}$. Accepts all task types.
- **Low (15% to 30%):** $C_{\text{bat}} = \max(1, \lfloor C_{\text{max}} / 2 \rfloor)$. Rejects GPU and compilation tasks.
- **Very Low (6% to 14%):** $C_{\text{bat}} = 1$. Rejects GPU and compilation tasks; accepts only lightweight command tasks.
- **Critical ($\le$ 5%):** $C_{\text{bat}} = 0$. Rejects all incoming tasks.

When connected to external power (AC / charging), battery capacity throttling is bypassed ($C_{\text{bat}} = C_{\text{max}}$).

Thermal tiers:
- **Nominal:** $C_{\text{therm}} = C_{\text{max}}$.
- **Moderate:** $C_{\text{therm}} = \max(1, \lfloor C_{\text{max}} / 2 \rfloor)$.
- **Severe:** $C_{\text{therm}} = 1$. Rejects GPU and compilation tasks.
- **Critical:** $C_{\text{therm}} = 0$. Rejects all tasks until temperature normalises.

Implemented in `crates/core/src/arbitrage.rs` (`evaluate_effective_concurrency`) and enforced by the master in `crates/master/src/scheduler.rs`.

### D2 RRC-aware adaptive keepalive controller

Mobile cellular radios operate in distinct Radio Resource Control (RRC) power states: RRC Connected (high power), RRC Inactive, and RRC Idle (low power). Constant 3-second heartbeats keep the cellular baseband continuously powered.

The adaptive keepalive controller dynamically selects heartbeat interval $\tau_{\text{worker}}$:
- **Active state (task executing in flight):** $\tau_{\text{worker}} = 10\text{ s}$. Ensures timely detection of worker stalls during execution.
- **Cellular idle state (no tasks, cellular link):** $\tau_{\text{worker}} = 25\text{ s}$. Matches cellular RRC tail timers, allowing baseband dormancy between heartbeats.
- **Wi-Fi / Ethernet idle state (unmetered LAN):** $\tau_{\text{worker}} = 60\text{ s}$. Minimises background radio wakeups.

The master calculates the dead peer window dynamically from the worker's reported interval:

$$W_{\text{dead}} = \max(2\text{ s}, 4 \times \tau_{\text{worker}})$$

Implemented in `crates/core/src/arbitrage.rs` (`AdaptiveKeepaliveController`) and `crates/master/src/registry.rs`.

### D3 Strict five-rule mobile master refusal matrix

Before an Android node can assume or maintain the coordinator role, it checks five mandatory invariants:

| Rule | Requirement | Failure Reason |
|---|---|---|
| R1 | External power connected | `MasterRefusalReason::NotCharging` |
| R2 | Battery level $\ge 50\%$ | `MasterRefusalReason::BatteryBelowThreshold` |
| R3 | Unmetered network (Wi-Fi or Ethernet) | `MasterRefusalReason::CellularOrMeteredNetwork` |
| R4 | Battery temp $< 40^\circ\text{C}$ and unthrottled | `MasterRefusalReason::ThermalHeadroomExceeded` |
| R5 | Foreground service active | `MasterRefusalReason::ForegroundServiceInactive` |

If any condition fails, the node immediately refuses coordinator election, sets its authority score to $-10000$, and yields the coordinator role if currently held.

Implemented in `crates/core/src/arbitrage.rs` (`check_mobile_master_eligibility`).

### D4 Deterministic weighted authority scoring and election

Each node calculates its authority score $S_{\text{auth}} \in [-10000, 10000]$:

$$S_{\text{auth}} = W_{\text{plat}} + W_{\text{pwr}} + W_{\text{net}} + W_{\text{therm}} + W_{\text{up}}$$

Weights:
- **Platform ($W_{\text{plat}}$):** Linux (+3000), macOS (+3000), Windows (+2500), Android (+500).
- **Power ($W_{\text{pwr}}$):** AC/Desktop (+2000), Battery >80% (+500), Battery 30–80% (+100), Battery <30% (-3000).
- **Network ($W_{\text{net}}$):** Ethernet (+1500), 5 GHz Wi-Fi / Wi-Fi 6 (+1000), 2.4 GHz Wi-Fi (+700), Cellular (-2000).
- **Thermal ($W_{\text{therm}}$):** Nominal (+500), Throttled (-5000), Overheating (-8000).
- **Uptime ($W_{\text{up}}$):** $\min(1000, \lfloor \text{uptime\_seconds} / 3600 \rfloor \times 100)$.

A node is eligible for election only if $S_{\text{auth}} \ge 2500$. Ties are resolved deterministically across four stages:
1. Higher authority score $S_{\text{auth}}$.
2. Longer node uptime.
3. Lower current concurrency utilisation.
4. Lexicographically lower node UUID.

Implemented in `crates/core/src/arbitrage.rs` (`calculate_authority_score`, `compare_node_authority`).

### D5 Cryptographic master lease tickets and role handover

Coordinator authority is governed by a `MasterLeaseTicket`:
- **Validity:** 30 seconds (`LEASE_VALIDITY_DURATION_MS = 30000`).
- **Renewal interval:** 10 seconds (`LEASE_RENEWAL_INTERVAL_MS = 10000`).
- **Cryptographic signature:**

$$\text{HMAC-SHA256}(\text{cluster\_secret}, \text{"oxs1-master-lease"} \parallel \text{master\_id} \parallel \text{epoch} \parallel \text{granted\_at\_ms} \parallel \text{expires\_at\_ms})$$

When an elected worker claims coordinator role, it transmits `WorkerMessage::ClaimMasterRole { ticket }`. Nodes verify the HMAC signature and epoch before accepting the new coordinator.

Implemented in `crates/core/src/arbitrage.rs` (`MasterLeaseTicket`) and `crates/master/src/server.rs`.

### D6 Pre-sleep fast task evacuation hook

When the mobile operating system notifies the worker of impending sleep or background termination, the worker initiates fast evacuation:
1. Stops accepting new task dispatches immediately.
2. Sends a `WorkerMessage::Disconnecting { reason, drain: true }` frame to the master.
3. The master immediately requeues all in-flight tasks assigned to that worker without incrementing retry failure counts.
4. Completes socket teardown within $< 50\text{ ms}$, ensuring zero task loss prior to OS suspension.

Implemented in `crates/worker/src/client.rs` (`evacuate_before_sleep`) and `crates/master/src/server.rs`.

## Compatibility

- Wire format carries lease tickets and drain flags inside existing protocol enums (`WorkerMessage::ClaimMasterRole`, `WorkerMessage::Disconnecting`).
- Compatible with protocol version 1; capability bit `ProtocolCapabilities::ROLE_ARBITRAGE = 1 << 5` identifies participating nodes.
- Unaware legacy nodes ignore role arbitration frames and continue operating as normal workers.

## Security

- All lease tickets require HMAC-SHA256 verification using the shared `cluster.secret`. An attacker without the secret cannot forge coordinator tickets or trigger false master takeovers.
- The 30-second lease ticket lifetime bounds replay windows. Expired or future-dated lease tickets are rejected.
- Authority scores are computed locally, but lease tickets require the cluster secret, preventing unauthenticated mobile nodes from assuming leadership.

## Test plan

- **Unit:** battery tier transitions, thermal concurrency caps, 5-rule refusal matrix evaluations, authority score math, and lease ticket signature verification (`tests/mobile_coordination_test.rs`).
- **Stress:** boundary testing for 0%, 14%, 15% battery states, rapid thermal tier oscillation, and 4-stage tie-breaking fuzzing (`tests/m5a_challenger_1_stress.rs`, `tests/m5a_challenger_2_stress.rs`).
- **Integration:** pre-sleep evacuation timing under load asserting total evacuation time $< 50\text{ ms}$ and zero task loss.
- **Gates:** `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Arbitrage and refusal module | `crates/core/src/arbitrage.rs` | Refusal matrix, $S_{\text{auth}}$ calculation, and lease ticket types implemented with unit tests | Implemented |
| W2 | Scheduler multi-tier throttling | `crates/master/src/scheduler.rs` | Concurrency scaled by $C_{\text{eff}} = \min(C_{\text{bat}}, C_{\text{therm}})$ in matchmaking | Implemented |
| W3 | Adaptive keepalive controller | `crates/core/src/arbitrage.rs`, `crates/master/src/registry.rs` | 10s / 25s / 60s intervals and dynamic $W_{\text{dead}}$ calculation | Implemented |
| W4 | Master lease claim handling | `crates/master/src/server.rs` | `ClaimMasterRole` verification and coordinator state transition | Implemented |
| W5 | Pre-sleep fast evacuation | `crates/worker/src/client.rs`, `crates/master/src/server.rs` | Disconnecting drain frame returned and tasks requeued in $< 50\text{ ms}$ | Implemented |
| W6 | Stress test suites | `tests/mobile_coordination_test.rs`, `tests/m5a_challenger_1_stress.rs`, `tests/m5a_challenger_2_stress.rs` | All stress tests pass with 0 failures | Implemented |

## Status log

- 2026-10-23: Created and accepted for Milestone M5A.
- 2026-10-23: Implemented W1–W6 in branch `feat/m5a-mobile-coordination`.
- 2026-10-23: Verified via `cargo test -p rusty_grid_android_bridge --test mobile_coordination_test`, `m5a_challenger_1_stress`, and `m5a_challenger_2_stress` (16 passing tests).
- 2026-10-23: Fixed 0% battery boundary assertion in `tests/m6_challenger_2_stress.rs` to enforce full task refusal on critical battery. Status marked Implemented.
