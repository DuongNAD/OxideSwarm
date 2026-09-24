# TEST_INFRA: Cross-Platform Coding Agent Communication Framework PoC

## 1. Test Philosophy & Methodology
- **Opaque-Box & Requirement-Driven**: The test infrastructure validates system behavior strictly against the interface contracts and requirements specified in `ORIGINAL_REQUEST.md` (`## 2026-09-23T09:50:43Z`) and `PROJECT.md`. Tests interact solely through network transport boundaries (RFC 6455 WebSockets over TCP) and wire protocol frames (`AgentMeshEnvelope`), treating the Hub and Agent Nodes as opaque systems.
- **Genuine Verification (Zero Cheating / No Facades)**: Every test establishes real TCP/WebSocket connections, transmits live serialized JSON envelopes, executes genuine subprocess commands or echo operations, calculates cryptographic SHA-256 hashes from raw byte arrays, and asserts exact match criteria. No mock shortcuts, artificial delays, or pre-canned responses are tolerated.
- **Port Discipline & Ephemeral Binding**: All tests bind the OxideRelay Hub to an ephemeral OS-assigned port (`127.0.0.1:0`), completely eliminating port collision risks, race conditions, or residual state across runs.
- **Process & Connection Lifecycle Sandboxing**: Test suites manage full socket setup and teardown, utilizing async contexts and timeout guards to prevent hung processes or orphaned sockets.

---

## 2. Feature Inventory & Multi-Tier Test Mapping

The test suite is organized into 4 rigorous tiers covering all functional and non-functional requirements:

| # | Feature Area | Requirement Source | Tier 1 (Equivalence) | Tier 2 (Boundaries) | Tier 3 (Pairwise/Cross) | Tier 4 (Real-World) |
|---|--------------|-------------------|:--------------------:|:-------------------:|:-----------------------:|:-------------------:|
| 1 | Hub Node Registration & Handshake | ORIGINAL_REQUEST §R1, PROJECT §F1, F2 | EP-1.1, EP-1.2 | BV-1.1, BV-1.2 | PW-1.1 | RW-1.1 |
| 2 | Active Directory Catalog Discovery | ORIGINAL_REQUEST §R1, PROJECT §F1 | EP-2.1 | BV-2.1 | PW-2.1 | RW-1.1 |
| 3 | Point-to-Point Command Routing | ORIGINAL_REQUEST §R2, AC1, PROJECT §F4 | EP-3.1, EP-3.2 | BV-3.1, BV-3.2 | PW-3.1 | RW-1.1 |
| 4 | Safe Command Execution & Telemetry | ORIGINAL_REQUEST §R2, AC1, PROJECT §F5 | EP-4.1, EP-4.2 | BV-4.1, BV-4.2 | PW-4.1 | RW-1.1 |
| 5 | Cross-Node Non-Interference Isolation | ORIGINAL_REQUEST §R2, AC1, PROJECT §F4 | EP-5.1 | BV-5.1 | PW-5.1 | RW-1.1 |
| 6 | Delivery ACK & NACK Error Signaling | ORIGINAL_REQUEST §R1, R2, PROJECT §F7 | EP-6.1, EP-6.2 | BV-6.1, BV-6.2 | PW-6.1 | RW-1.1 |
| 7 | Bidirectional Zero-Data-Loss Streaming | ORIGINAL_REQUEST §R2, AC3, PROJECT §F14 | EP-7.1, EP-7.2 | BV-7.1, BV-7.2 | PW-7.1 | RW-2.1, RW-2.2 |
| 8 | High-Throughput Burst Stability | ORIGINAL_REQUEST AC3, PROJECT §F14 | EP-8.1 | BV-8.1 | PW-8.1 | RW-2.3 |

---

## 3. Tier Breakdown & Test Specifications

### Tier 1: Equivalence Partitioning (Core Functional Contracts)
Validates standard happy-path interactions according to the protocol specifications:
- **EP-1.1 (Node Registration Handshake)**: Agent node connects via WebSocket, sends `NodeRegistration` envelope with platform metadata; Hub responds with `NodeRegistrationAck` (`status: "accepted"`).
- **EP-1.2 (Multi-Platform Identity Registration)**: Distinct nodes representing Windows, macOS, and Android successfully register unique `node_id`s simultaneously.
- **EP-2.1 (Active Node Catalog Query)**: Registered node dispatches `NodeList` query; Hub returns `NodeListResponse` containing complete, accurate directory of active nodes.
- **EP-3.1 (Direct Unicast Command Routing)**: Node 1 dispatches `CommandRequest` targeted specifically to Node 3; Hub forwards strictly to Node 3 without broadcast flooding.
- **EP-4.1 (Echo Command Execution)**: Node receives `echo` command, returns `CommandResponse` with exit code 0 and exact mirrored message payload.
- **EP-4.2 (Shell Subprocess Execution)**: Node executes shell command (`shell_exec`), returns `CommandResponse` with exit code 0, captured `stdout`, empty `stderr`, and positive `execution_duration_ms`.
- **EP-5.1 (Targeted Routing Isolation)**: When Node 1 sends command to Node 3, intermediary Node 2 receives zero command requests (execution counter remains 0).
- **EP-6.1 (Hop-by-Hop Delivery ACK)**: Hub immediately dispatches `DeliveryAck` to sender upon successful forwarding of command to target.
- **EP-7.1 (Binary Payload Forwarding)**: Node 1 sends `DataPayload` to Node 3; Node 3 receives exact chunk with preserved length.
- **EP-7.2 (Reverse Payload Forwarding)**: Node 3 sends `DataPayload` back to Node 1; Node 1 receives exact chunk with preserved length.

### Tier 2: Boundary Value Analysis & Error Handling
Validates system resilience against edge conditions, malformed inputs, and network anomalies:
- **BV-1.1 (Unknown Route Delivery NACK)**: Dispatching `CommandRequest` or `DataPayload` to an unregistered target (`node-ghost-404`) causes Hub to emit immediate `DeliveryNack` with `ERR_NODE_NOT_FOUND`.
- **BV-2.1 (Empty Command / Empty Args Handling)**: Command request with empty arguments executes safely without throwing unhandled exceptions.
- **BV-3.1 (Malformed JSON Resilience)**: Hub ignores or safely discards malformed frames without crashing or severing existing connections.
- **BV-4.1 (Subprocess Non-Zero Exit Code Capture)**: Command with non-existent executable or deliberate exit code (e.g. exit 127) produces `status: "failed"`, `exit_code: 127`, and populated `stderr`.
- **BV-5.1 (Graceful Disconnect Deregistration)**: Node cleanly disconnects; Hub updates catalog and immediately stops routing packets to that node.
- **BV-6.1 (Request Timeout Enforcement)**: Commands with configured timeouts return cleanly if target fails to respond within deadline.
- **BV-7.1 (64 KB Exact Payload Boundary)**: Exactly 65,536 bytes transferred in a single frame without frame truncation or socket buffer overflow.
- **BV-8.1 (Zero-Byte Payload Boundary)**: Empty payload transfer handled gracefully without division-by-zero or index errors.

### Tier 3: Pairwise Combinations & Concurrency
Validates interactions between simultaneous protocol features:
- **PW-1.1 (Concurrent Multi-Node Communication)**: Simultaneous command routing between Node 1 $\rightarrow$ Node 3 while Node 2 queries node list.
- **PW-3.1 (Interleaved Commands and Binary Streaming)**: Command execution interleaved with high-volume data payloads without correlation ID crossover or packet corruption.
- **PW-5.1 (Strict Multi-Node Isolation under Traffic)**: Continuous point-to-point traffic between Node 1 and Node 3 verified to never bleed into Node 2 under continuous load.
- **PW-8.1 (Rapid Sequential Burst Dispatch)**: Rapid dispatch of back-to-back envelopes verified for FIFO delivery ordering.

### Tier 4: Real-World Scenarios & Full Acceptance Criteria
End-to-end integration scenarios fulfilling all acceptance criteria of `ORIGINAL_REQUEST.md`:
- **RW-1.1 (AC1 - Heterogeneous 3-Node Mesh Simulation)**:
  - Spawns local ephemeral OxideRelay Hub.
  - Spawns 3 simulated agent nodes: Windows (`node-win-1`), macOS (`node-mac-2`), Android (`node-android-3`).
  - Verifies all 3 nodes register in Hub catalog.
  - Node 1 dispatches targeted shell command to Node 3.
  - Node 3 executes command and returns exit code 0, duration, and output.
  - Verifies Node 2 isolation (command execution count = 0).
  - Tests negative routing returning `DeliveryNack`.
- **RW-2.1 (AC3 - Forward 64 KB Binary Transfer with SHA-256 Verification)**:
  - Node 1 generates 65,536 cryptographically random bytes (`os.urandom(65536)`).
  - Calculates sender SHA-256 digest.
  - Dispatches `DataPayload` to Node 3.
  - Node 3 receives payload, computes receiver SHA-256 digest.
  - Asserts bit-for-bit equality (`sender_hash == receiver_hash`) and exact length 65,536.
- **RW-2.2 (AC3 - Reverse 64 KB Binary Transfer with SHA-256 Verification)**:
  - Node 3 generates 65,536 random bytes.
  - Dispatches `DataPayload` back to Node 1.
  - Node 1 receives and asserts bit-for-bit SHA-256 match.
- **RW-2.3 (AC3 - High-Frequency Concurrent Burst Stress Test)**:
  - Node 1 streams 50 unique binary/text packets to Node 3 in rapid succession.
  - Node 3 verifies receipt of all 50 packets with zero dropped packets and zero corruption (0% loss rate).

---

## 4. Test Suite Architecture & File Layout

```
tests/
├── test_agent_mesh_simulation.py    # Tier 1-4 3-Node simulation, command routing, isolation & NACK tests
├── test_agent_mesh_data_loss.py     # Tier 1-4 Bidirectional 64 KB SHA-256 & 50-packet burst zero-data-loss tests
└── run_all_mesh_tests.py            # Master programmatic runner executing both suites with exit code 0
```

---

## 5. Execution Semantics & Invocation Commands

### Standalone Test Suite Execution
```bash
# Execute 3-Node Heterogeneous Simulation & Command Routing Suite
python tests/test_agent_mesh_simulation.py

# Execute Bidirectional 64 KB SHA-256 & Zero-Data-Loss Burst Suite
python tests/test_agent_mesh_data_loss.py
```

### Master Acceptance Test Runner
```bash
# Execute the unified test runner covering all acceptance criteria
python tests/run_all_mesh_tests.py
```

### Exit Code Contract
- `0`: All test suites passed with 100% assertions satisfied.
- Non-zero (`1`): Any assertion failure, connection drop, data loss, or unhandled exception.

---

## 6. Coverage Thresholds & Quality Gates
- **Functional Acceptance Criteria Coverage**: 100% of criteria specified in `ORIGINAL_REQUEST.md` (`## 2026-09-23T09:50:43Z`).
- **Data Loss Tolerance**: Strictly 0.00% (Bit-for-bit cryptographic SHA-256 identity verified).
- **Isolation Guarantee**: Zero cross-talk leakage (Intermediary nodes receive 0 unintended command requests).
- **Execution Speed**: Full test suite completes in under 5.0 seconds.
