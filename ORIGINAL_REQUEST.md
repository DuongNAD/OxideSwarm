# Original User Request

## 2026-09-19T13:28:05+07:00

An open-source distributed computing framework written in Rust, designed to handle general master-worker tasks, distributed Rust compilation, and GPU workloads across multiple connected machines.

Working directory: /Volumes/KINGSTON/teamwork_projects/rusty_grid
Integrity mode: benchmark

## Requirements

### R1. Core Master-Worker Communication
Build a Master node that can accept connections from multiple Worker nodes. Workers must be able to register themselves, advertise their capabilities (e.g., CPU cores, GPU presence), and maintain an active connection.

### R2. Task Distribution and Execution
Implement a task queue on the Master. The Master must be able to distribute generic tasks to available Workers. Workers must execute the assigned tasks and return the results or error states back to the Master.

### R3. Workload-Specific Routing
The scheduling logic must support routing constraints. Specifically, it must route GPU tasks only to Workers that advertise GPU capabilities, and it must support distributing independent sub-tasks (like compiling separate Rust crates) across multiple Workers in parallel.

## Acceptance Criteria

### Automated Integration Testing
- [ ] An automated test script (`test_integration.sh` or standard Rust integration tests) can launch 1 Master process and 3 Worker processes locally (one of which simulates having a GPU).
- [ ] The test verifies that all 3 Workers successfully register with the Master.
- [ ] The test submits a generic task and verifies it is processed by a Worker and the correct result is returned to the Master.
- [ ] The test submits a GPU-specific task and verifies it is assigned *only* to the Worker simulating GPU capabilities.
- [ ] The test submits a batch of 5 independent tasks (simulating compilation jobs) and verifies they are executed in parallel across the available Workers.

## Follow-up — 2026-09-19T08:04:34Z

USER INSTRUCTION UPDATE: The user has explicitly requested to add Android support to the project ("làm thêm cả android nữa"). Please update the project requirements (PROJECT.md) and architectural plans to ensure the Worker client can be cross-compiled and run on Android devices (e.g., targeting `aarch64-linux-android`). The Android devices should be able to act as Worker nodes in the grid, advertising their mobile hardware capabilities. Please integrate this into the current or future milestones.

## Follow-up — 2026-09-19T08:28:05Z

USER INSTRUCTION UPDATE: For Milestone 5 (Unified CLI), the user requested that both the Master and Worker nodes be highly configurable depending on the use case. Please ensure the CLI includes robust configuration options (via CLI flags, environment variables, or config files). Examples include: custom IP/ports, adjusting heartbeat timeouts, limiting max concurrent tasks on a worker, and allowing a worker to manually override its hardware capabilities (e.g., forcing a limit on CPU cores used or manually toggling GPU presence).

## Follow-up — 2026-09-19T08:30:26Z

USER INSTRUCTION UPDATE: The user strongly prefers that remote/internet connections between the Master and Worker work *natively* without requiring users to configure Port Forwarding or install 3rd-party VPNs (like Tailscale). They asked to integrate open-source solutions to handle this. Please update the networking architecture (potentially exploring `libp2p`, `iroh`, or implementing NAT Traversal / Hole Punching / STUN/TURN) so that Workers can securely discover and connect to the Master across the internet directly out-of-the-box.

## Follow-up — 2026-09-19T08:44:37Z

USER INSTRUCTION UPDATE: The user requested "Dynamic System Load Awareness" to protect host machines from stuttering. If a Worker machine is running heavy external apps (e.g., playing a heavy game or rendering in Blender), it should dynamically receive fewer or no tasks. Please integrate real-time system load monitoring (e.g., using the `sysinfo` crate) into the Worker's heartbeat mechanism. The Master's Scheduler must use this real-time telemetry (current CPU/RAM usage of the host) to route tasks away from machines that are under heavy external load, creating an automatic backpressure/throttling system for host safety.

## Follow-up — 2026-09-19T08:50:18Z

USER INSTRUCTION UPDATE: The user has given explicit approval to integrate the discussed open-source libraries. Please officially proceed with embedding `iroh` (or `libp2p`) as the core networking dependency for P2P NAT traversal in Milestone 5. You are also authorized to reference or integrate map/reduce task orchestration patterns from frameworks like `Paladin` if it optimizes the Master's workload distribution. Keep the system lightweight and avoid heavy external dependencies like Redis.

## Follow-up — 2026-09-19T10:06:46Z

USER INSTRUCTION UPDATE: The user requested explicit build support for Windows (to run on their desktop PC) alongside the Android phone support. Please ensure Milestone 5 includes a `build_windows_worker.sh` script (or equivalent cross-compilation setup) targeting `x86_64-pc-windows-gnu` or `msvc`. This will allow the user to easily compile `.exe` binaries for their Windows machine directly from their current macOS workspace. Please add this script and validate it before final project sign-off.

## Follow-up — 2026-09-19T10:19:05Z

USER INSTRUCTION UPDATE: The user has requested to publish this project as a public open-source repository on GitHub upon completion. For the final handoff of Milestone 5, please ensure a comprehensive, highly professional `README.md` is generated. It should include the project architecture, features (P2P NAT traversal via iroh, sysinfo dynamic load balancing, CPU caps), and build/usage instructions for macOS, Windows (`.exe`), and Android. 

Note: You do not need to run git commands to push. Just prepare the README and codebase. I (the primary agent) will handle the `gh repo create` and `git push` process once you signal total project completion.

## Follow-up — 2026-09-19T10:20:14Z

USER INSTRUCTION UPDATE: The user has officially selected the project name: `OxideSwarm` (replacing `rusty_grid`). Please ensure the final `README.md`, CLI binary names, and internal project documentation reflect the new brand name `OxideSwarm` before the final project handoff.

## 2026-09-20T04:54:33Z

# Teamwork Project Prompt — Draft

> Status: Launched
> Requested team: The full agent team

Fix the 40 failing E2E integration tests in the OxideSwarm Rust project. The project currently has networking and registration logic issues that cause `cargo test --workspace` to fail. Use the full agent team to investigate and resolve these issues.

Working directory: d:\teamwork_projects\OxideSwarm
Integrity mode: benchmark

## Requirements

### R1. Root Cause Resolution
Analyze and resolve the root causes of the failing integration tests in `tests/e2e_cluster.rs` and the `test_integration.sh` script, particularly focusing on the worker registration, exit code assertions, and output stream truncation issues.

### R2. Preserve Existing Functionality
Ensure that the 25 currently passing tests continue to pass without regression.

## Verification Resources
- Test suite: `tests/e2e_cluster.rs` (can be run via `cargo test -p rusty_grid_cli --test e2e_cluster`)
- Bash acceptance script: `test_integration.sh`

## Acceptance Criteria

### Automated Tests
- [ ] Running `cargo test --workspace` must complete with 0 test failures.
- [ ] Running `bash test_integration.sh` must execute successfully without errors.

## 2026-09-21T10:00:36Z

# Teamwork Project Prompt — Draft

> Status: Launched
> Goal: Craft prompt → get user approval → delegate to teamwork_preview
> Requested team: [none — teamwork routes from the description]

Package the OxideSwarm distributed grid worker into automated installer scripts or packages for Mac, Windows, and Android. The goal is to configure the worker to run silently in the background on boot for each platform, choosing the most optimal implementation method.

Working directory: ~/teamwork_projects/oxideswarm_packaging
Integrity mode: development

## Requirements

### R1. Windows Background Service
Create an installer or automation script for Windows that correctly registers the cross-compiled `rusty-grid.exe` as a system service. It must start automatically on boot without displaying a console window.

### R2. MacOS Launch Daemon
Create an install script and `launchd` plist configuration for macOS. It must register the native Mac binary to run silently on system startup and automatically restart if it crashes.

### R3. Android Background Execution
Implement a solution to run the worker binary on Android continuously in the background, circumventing Doze mode restrictions (e.g., via a Java wrapper with a Foreground Service, or a robust Termux daemon script).

## Acceptance Criteria

### Verification & Testing
- [ ] Windows: Provide a PowerShell script that can verify the service is correctly registered and configured for auto-start in the Windows Service Control Manager.
- [ ] MacOS: Provide a bash script that validates the syntax of the generated `.plist` file and verifies it can be loaded into `launchctl`.
- [ ] Android: Provide a clear, step-by-step evaluation rubric. An independent agent must review the Android solution and confirm it technically prevents the OS from suspending the worker process.

## 2026-09-21T14:06:34Z

# Teamwork Project Prompt — Draft

> Status: Launched
> Goal: Craft prompt → get user approval → delegate to teamwork_preview
> Requested team: [none — teamwork routes from the description]

Build a real-time Agent-to-Agent communication bridge (via Antigravity MCP Server over LAN/P2P) enabling the AI on Mac and the AI on the Windows Case PC to directly exchange messages, share context, and coordinate distributed tasks in real-time, while refining the mobile cluster dashboard.

Working directory: ~/teamwork_projects/antigravity_mcp_bridge
Integrity mode: development

## Requirements

### R1. Cross-Machine Real-Time MCP Bridge
Develop an MCP (Model Context Protocol) server running over HTTP SSE/WebSocket that bridges the Antigravity AI agent on Mac (`192.168.1.144`) and the Antigravity AI agent on the Windows Case (`192.168.1.123`). It must provide tools for bidirectional agent messaging (`send_agent_message`, `read_agent_inbox`, `execute_remote_task`).

### R2. Mobile Dashboard Refinement
Fix the empty space issue identified in the Android phone screenshot. The dashboard terminal should dynamically size, and the device cards should present clean, high-density telemetry without clipping.

## Acceptance Criteria

### Verification & Testing
- [ ] Cross-Agent Handshake: A test script confirms Mac AI can send a payload to Windows AI and receive an acknowledgment within 100ms over LAN.
- [ ] MCP Tool Registration: Provide ready-to-use MCP configuration JSON for both Mac and Windows Antigravity instances.
- [ ] Mobile UI Rendering: Visual validation confirms the mobile dashboard renders properly with no awkward empty voids or cut-off elements.

## Follow-up — 2026-09-21T14:10:11Z

User requirement update: Please add an explicit on-demand reload/refresh button (`[Reload]` or `[Sync]`) to the top bar of the phone dashboard so the user can instantly refresh the cluster status, ping latency, and node list with a single tap directly on their phone.

## Follow-up — 2026-09-21T14:13:14Z

User requirement update: For the reload button on the mobile dashboard, please use a clean, sharp reload icon (SVG circular arrow / refresh icon) instead of plain text.

## Follow-up — 2026-09-21T14:13:49Z

User requirement update: Split the mobile dashboard into a clean 2-tab navigation system (`[Terminal]` and `[Nodes]` tabs):
- Tab 1 (Terminal): Dedicated full-height terminal/chat feed for executing commands and viewing logs.
- Tab 2 (Nodes): Dedicated full-page view listing all connected devices (MASTER, ORCH, WORKER) with detailed telemetry (IP, Cores, RAM, ping latency, uptime) without cluttering the terminal.

## Follow-up — 2026-09-21T14:14:16Z

User requirement refinement for mobile UI:
- Main Page (Trang chính): Focus entirely on the interactive Chat/Terminal interface, high-level status summary (nodes count, cores), and execution reports/logs.
- Secondary Page (Trang Thiết bị / Devices): Move the connected devices list (`MASTER`, `ORCH`, `WORKER`) to this separate tab/page, showing complete device specs and telemetry.
- Tab bar should make switching between Main Chat and Devices effortless.

## Follow-up — 2026-09-21T14:14:56Z

User requirement update for Devices layout:
- Arrange device cards/rows with uniform, evenly aligned columns (consistent widths, neat spacing).
- Status indicator must use a clean, crisp Green dot (Online/Active) or Red dot (Offline/Error) instead of text badges.

## 2026-09-21T15:55:00Z

# Teamwork Project Prompt — Draft

> Status: Launched
> Goal: Craft prompt → get user approval → delegate to teamwork_preview
> Requested team: [none — teamwork routes from the description]

Nghiên cứu kiểm thử chuyên sâu, đo lường benchmark phân tán trên thiết bị vật lý thật (MacBook M-series + Samsung Galaxy S24) và củng cố toàn diện độ tin cậy (fault-tolerance, chaos testing, khử flaky test) cho cụm tính toán phân tán OxideSwarm.

Working directory: ~/teamwork_projects/oxideswarm_testing_hardening
Integrity mode: development

## Requirements

### R1. Kiểm Tra & Chuẩn Hoá Toàn Bộ Test Suite (Codebase Audit & Flaky Test Elimination)
Rà soát và chạy toàn bộ test suite hiện có của workspace (`cargo test --workspace`). Phát hiện mọi bài test bị lỗi, flaky, hoặc có race condition trong các crate `core`, `master`, `worker`, và `cli`. Refactor mã nguồn để 100% bài test vượt qua ổn định và nhất quán.

### R2. Đo Lường & Đánh Giá Tải Tính Toán Phân Tán Thực Tế (Heterogeneous Distributed Benchmark)
Thiết kế và thực thi kịch bản đo kiểm tính toán phân tán thực tế (ví dụ: nhân ma trận phân tán, băm dữ liệu theo chunk song song) chạy đồng thời trên 10 Cores CPU của MacBook và 10 Cores CPU của Samsung Galaxy S24 qua mạng LAN. Đo đạc chính xác:
- Tốc độ xử lý (Throughput / Ops per second).
- Hệ số tăng tốc (Speedup factor) khi chia tải 2 thiết bị so với chạy đơn node.
- Mức tiêu thụ tài nguyên và độ ổn định nhiệt độ/pin trên điện thoại.

### R3. Kiểm Thử Khả Năng Chịu Lỗi & Tự Phục Hồi (Chaos Engineering & Failover Resilience)
Kiểm thử các tình huống lỗi mạng và ngắt node đột ngột:
- Đột ngột ngắt tiến trình worker trên điện thoại hoặc ngắt kết nối Wi-Fi khi đang chạy tác vụ nặng.
- Xác nhận Master phát hiện mất heartbeat đúng hạn (dead node detection).
- Đảm bảo cơ chế tự động phân bổ lại tác vụ (task requeueing / failover) cho node còn sống mà không làm mất hay sai lệch kết quả.
- Xác nhận worker tự động kết nối lại (auto-reconnect) mượt mà khi mạng được khôi phục.

### R4. Tối Ưu Độ Trễ & Kiểm Chứng Đo Lường Viễn Trình (P2P Latency & Telemetry Hardening)
Đo kiểm độ trễ round-trip P2P giữa Mac và điện thoại qua Iroh QUIC / TCP. Đảm bảo luồng dữ liệu telemetry thời gian thực (/api/status, /api/chat) vẫn phản hồi mượt mà (< 100ms cho status) ngay cả khi cụm đang chịu tải 100% CPU.

## Acceptance Criteria

### Automated Verification
- [ ] **Workspace Test Suite**: Lệnh `cargo test --workspace` hoàn thành với kết quả 100% PASS, 0 failure, không có test bị treo hay flaky.
- [ ] **Distributed Benchmark Suite**: Cung cấp script tự động `run_cluster_benchmark.sh` thực thi phân tán trên cả Mac và S24, xuất ra báo cáo số liệu so sánh tốc độ phân tán rõ ràng (Throughput, Execution Time, Speedup).
- [ ] **Chaos Resilience Test**: Cung cấp script tự động `run_chaos_test.sh` kiểm chứng kịch bản rớt worker giữa chừng; xác nhận Master tự điều phối lại task sang node khác thành công 100% mà không gây crash hay deadlock.
- [ ] **Telemetry Verification**: Đo đạc độ trễ API dưới áp lực tải nặng, xác thực số liệu hiển thị trên Web Dashboard khớp 100% với trạng thái thực tế của thiết bị.

## Verification Resources
- Thiết bị Master: MacBook (`192.168.1.144:8088`, Web UI `:8080`).
- Thiết bị Worker 1: `macbook-worker` (chạy trên localhost).
- Thiết bị Worker 2: Samsung Galaxy S24 kết nối qua ADB (`R5CWC3QQ52H` tại `192.168.1.147`).
- Cấu trúc dự án nguồn tại `/Users/duongnad/Documents/project/OxideSwarm`.
