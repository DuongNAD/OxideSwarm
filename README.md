# OxideSwarm

<div align="center">

<p align="center">
  <img src="assets/logo.svg" alt="OxideSwarm Logo" width="100%">
</p>

**Zero-Broker Distributed Computing Grid for Heterogeneous Clusters**  
*Apple Silicon, Windows GPU PCs, Android phones and Linux boxes working as one personal compute grid*

[![Rust 2021](https://img.shields.io/badge/Rust-2021_Edition-orange.svg?style=flat-square&logo=rust)](https://www.rust-lang.org)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg?style=flat-square)](LICENSE)
[![Tests](https://img.shields.io/badge/Tests-cargo_test_--workspace_(local,_no_CI)-lightgrey.svg?style=flat-square)](#8-verification--benchmarks)
[![P2P NAT](https://img.shields.io/badge/P2P%20NAT-iroh%20QUIC%2FDERP-purple.svg?style=flat-square)](https://iroh.computer)
[![Security](https://img.shields.io/badge/Auth-shared_secret-green.svg?style=flat-square)](docs/SECURITY.md)
[![Platforms](https://img.shields.io/badge/Platforms-macOS%20|%20Windows%20|%20Android%20|%20Linux-lightgrey.svg?style=flat-square)](#4-quickstart-guide)

</div>

---

## 1. Overview

**OxideSwarm** is a small distributed computing framework written in Rust. A **master** hands tasks (commands,
shell scripts, compile jobs, map/reduce chunks, GPU-class compute) to **workers** running on your own machines:
Apple Silicon MacBooks, Windows gaming PCs, Android phones and Linux servers.

* **Zero external message brokers**: Tokio actors, 4-byte length-delimited TCP streams and QUIC. No Redis, Kafka, RabbitMQ or ZooKeeper.
* **P2P NAT traversal (`iroh`)**: QUIC with DERP relay fallback connects machines on different networks without opening router ports or setting up a VPN (needs outbound Internet to the public n0 relays).
* **Shared-secret authentication**: only machines holding the same `cluster.secret` can join the grid or drive it, and workers only obey a master that proves it holds the secret. The cluster port is authenticated but **not encrypted**; see [docs/SECURITY.md](docs/SECURITY.md) before exposing it to a network you do not control.
* **Light background worker**: in the author's measurements an idle worker uses roughly 7-15 MB of RAM and almost no CPU (single runs on one Mac and one Windows PC, not reproduced in CI). The Windows runner starts headless, without console windows.
* **Quick role swapping and discovery**: `switch_role.sh` / `windows\switch_role.cmd` turn a machine into a master or a worker in a few seconds. Workers can find a master with `--master auto` (authenticated UDP beacon on `:8089`).
* **Decoupled 4-layer layout**: Rust core engine, Android JNI bridge, Agent Mesh coordination hub, cross-machine sync/benchmarks.
* **Process-tree lifecycle and bounded memory**: task process trees are terminated (`taskkill /F /T` / process groups), with LRU task eviction and stale-worker pruning on the master.

---

## 2. Architecture & Cluster Topology

```text
                            ┌──────────────────────────────────────────────┐
                            │              OXIDESWARM MASTER               │
                            │      (MacBook M-Series OR Windows GPU PC)    │
                            │   Cluster TCP :8088 (secret-authenticated)   │
                            │   Dashboard   :8080 (token; loopback default)│
                            │      UDP LAN discovery beacon :8089          │
                            └───────────────▲───────────────▲──────────────┘
                                            │               │
                            Outbound TCP    │               │  Outbound TCP / QUIC
                           (Auto-Reconnect) │               │  (P2P ticket + secret)
                                            │               │
        ┌───────────────────────────────────┴─┐   ┌─────────┴─────────────────────────────┐
        │        MOBILE COMPUTE NODE          │   │        WINDOWS GPU COMPUTE NODE       │
        │        Samsung Galaxy S24           │   │             Windows Case PC           │
        │       10 Cores Physical (ARM64)     │   │      (x86_64 + NVIDIA/AMD GPU)        │
        │  • Termux worker or JNI app         │   │  • headless background worker         │
        │  • Auto-cutoff on low battery (<15%)│   │  • Standby portal HTTP 307 redirect   │
        │  • Thermal throttling protection    │   │  • Anti-stuttering host protection    │
        └─────────────────────────────────────┘   └───────────────────────────────────────┘
```

---

## 3. Capabilities & Status

"Implemented + tests" means the code exists and is covered by automated tests in this repository that were run
locally (there is no CI configuration here). "Hardware-checked" means the author tried it once on real devices;
those results are historical and are **not reproduced by anything in the repo**.

| Category / Feature | What it is | Status |
| :--- | :--- | :--- |
| **1. Core grid & zero-broker** | Wire protocol with 4-byte length-delimited framing and dual-mode Serde; 1-byte format discriminator (`0x02` bincode, `0x01` tagged JSON, `0x7B` raw legacy JSON, `0x03` auth handshake). | Implemented + tests |
| **2. Master coordinator & scheduler** | 8-state FSM (`Submitted` ➔ `Queued` ➔ `Scheduled` ➔ `Running` ➔ `Completed` / `Failed` / `Retrying` / `Cancelled`), batch-spread scheduling, fast-path recovery when a worker's socket drops (asserted below 200 ms in `tests/m2_challenger_2_stress.rs`), 10 s heartbeat reaper. | Implemented + tests |
| **3. GPU workload routing** | Tasks that require a GPU go only to workers that **advertise** one. A worker advertises a *real* GPU only if its binary was built with `--features gpu-wgpu` **and** the compute engine started on an adapter (looked for automatically at start-up unless `--no-gpu`; `--gpu` insists and prints a warning when that is not possible); a build without wgpu advertises none. `--simulate-gpu` advertises a labelled **simulated** GPU (a CPU multiplication, for testing) and is eligible as well; the scheduler does not yet prefer a real GPU over a simulated one. | Implemented + tests |
| **4. Anti-stuttering backpressure** | Host CPU/RAM telemetry via `sysinfo`; above 85 % CPU (`--max-host-cpu-pct`) the master defers dispatches to that worker. | Implemented + tests |
| **5. In-memory Map/Reduce** | Chunking, built-in `word_count` / `line_count` kernels or your own mapper/reducer. | Implemented + tests |
| **6. Heterogeneous hardware** | macOS and Android (Galaxy S24) were tried by the author on real devices. Windows binaries cross-compile (`build_windows_worker.sh`); the Windows launchers and the service installer are PowerShell 5.1 / cmd code that is not run by any automated test. | Hardware-checked (macOS, Android); Windows untested in CI |
| **7. Headless Windows worker** | [windows/run_worker_silent.vbs](windows/run_worker_silent.vbs) starts a worker without console windows (it needs the master's `cluster.secret` first); [windows/status_worker.cmd](windows/status_worker.cmd) shows the real working set; [windows/stop_worker.cmd](windows/stop_worker.cmd) stops only the process it started. | Implemented, not tested in CI |
| **8. Role swapping** | [switch_role.sh](switch_role.sh) (macOS/Linux) and [windows/switch_role.cmd](windows/switch_role.cmd): stop whatever this launcher started, start the new role, wait until it is up (the macOS/Linux script also checks that a worker registered) and print the next steps. Takes a few seconds. There is **no automatic failover or election**; a person (or script) decides. | Implemented |
| **9. Master discovery** | **Tier 1** worker standby portal (`:8080` HTTP 307 to the master's dashboard; only when that dashboard is reachable from the network, i.e. the master was started with `--web-ui-addr`: a loopback-only dashboard, the default, is advertised as "none" and the portal keeps showing its standby page); **Tier 2** the dashboard probes the host it was loaded from, hosts saved in your browser and hosts the master lists in `/api/status`, then offers to switch (it does **not** scan a subnet); **Tier 3** UDP beacon on `:8089` (`--master auto`, assumes a /24 broadcast domain; with a secret the beacon is authenticated). | Tier 3 + tests; Tier 1/2 partial |
| **10. Web dashboard** | Axum server serving the SPA `crates/master/src/dashboard.html` (no CDN) plus REST, `/ws` and SSE. Loopback by default; reachable from phones/LAN only when you pass `--web-ui-addr 0.0.0.0:8080` (or `switch_role.sh master --dashboard-lan`); every request off the dev bypass needs the token printed by `oxideswarm secret show --dashboard-token`. | Implemented + tests |
| **11. Workflow modes (`ox-mode`)** | `test` (quick / lint / flaky / full), `dev` (fast check / local cluster / watch), `doc`, `research` (profile / bench / report). | Implemented + tests |
| **12. Benchmarks & chaos** | Historical single runs on the author's Mac + Galaxy S24: parallel SHA-256 at 341.3 MB/s (1.55x over one machine) and a mid-task worker kill detected in 319 ms. The raw logs were not kept, so treat these as anecdotes. Reproduce your own numbers: [run_cluster_benchmark.sh](run_cluster_benchmark.sh) (two local worker processes, or a phone with `--adb-id`) and [run_chaos_test.sh](run_chaos_test.sh) (starts its own cluster, kills a worker that holds running tasks, fails unless they were re-executed with the right output). | Historical numbers; scripts run locally |
| **13. Zero-copy I/O & QUIC multiplexing** | `bytes::Bytes` payloads; two iroh streams per connection (`STREAM_CONTROL = 0x01`, `STREAM_DATA = 0x02`) so bulk data cannot block heartbeats; scheduler micro-batching. | Implemented + tests |
| **14. Mid-task checkpointing** | `WorkerMessage::Checkpoint` / `MasterMessage::AssignTaskWithCheckpoint`; reassigned workers resume from the last delta. | Implemented + tests |
| **15. GPU compute (`wgpu`)** | `cargo build --release --bin oxideswarm --features gpu-wgpu` gives a worker that multiplies matrices on the machine's GPU (Metal on a Mac, Vulkan / DirectX 12 on Windows and Linux; the discrete GPU wins over the integrated one), checks the product against a sampled CPU recomputation and reports `Status: VERIFIED_OK` only if that check passed. A `GpuCompute` task is a **seeded `simulated_matrix_dim` x `simulated_matrix_dim` GEMM** tiled by `work_group_size` (used as given or rejected with the limit that was hit, never clamped); `kernel_name` is just a label and the input matrices are generated from `input_data`, so it is a benchmark workload, not a general kernel launcher. A GPU error, device loss or driver panic fails that task with a message and the engine is rebuilt for the next one; a dispatch that is already running on the GPU cannot be interrupted (the worker stops waiting). A worker never runs a real-GPU task on the CPU: the CPU path is the labelled `--simulate-gpu` test mode only. `hash.wgsl` is experimental and not dispatched. Time it with `cargo run --release -p rusty_grid_worker --features gpu-wgpu --example gemm_bench -- 1024`. The Windows build is cross-compiled and the Android library has no `gpu-wgpu` feature; neither is tested here. | Implemented + tests (real GPU measured on an Apple Silicon Mac) |
| **16. Security** | Shared-secret mutual authentication on the cluster port (TCP and P2P) and on the UDP beacon; token-protected, same-origin dashboard; agent-mesh hub token. Cluster traffic over plain TCP is **not encrypted**; the dashboard is plain HTTP. See [docs/SECURITY.md](docs/SECURITY.md). | Implemented + tests |
| **17. Agent Mesh ↔ Grid bridge** | `AgentGridBridge` (library) turns `grid_compute` / `grid_compile` / `grid_submit` / `grid_status` envelopes into master tasks and is exercised by tests. The shipped `agent-mesh hub` binary does **not** embed a master, so those commands are answered with `ERR_BRIDGE_NOT_CONFIGURED`. There is **no MCP server** in this repository yet. | Partial (library only) |

---

### Known Limitations & Roadmap

| # | Limitation / Goal | Status | Technical details |
|---|---|---|---|
| **1** | Cluster traffic over plain TCP is not encrypted | Known limitation | The handshake authenticates both ends but nothing encrypts the stream afterwards. On untrusted networks use `--p2p` (QUIC/TLS) or a VPN / SSH tunnel. |
| **2** | No automatic master failover | Roadmap | If the master dies, workers keep reconnecting and queued work waits. The election / signed-lease design in `ANDROID_ARCHITECTURE.md` is a design only. |
| **3** | MCP server for AI agents | Roadmap | Not in this repository. The CLI's `connect_master` helper (`crates/cli/src/connect.rs`) is the shared authenticated client path such a server would use. |
| **4** | Real GPU execution from the shipped binaries | Partial | Build with `--features gpu-wgpu` (item 15). Not yet: GPU support in the Android app, arbitrary kernels or caller-supplied matrices, a scheduler that prefers a real GPU over a simulated one. |
| **5** | Android app (JNI) and the cluster secret | Open | The APK has no field for the cluster secret yet, so it cannot join a master that requires one (every master with a non-loopback listener). Use the Termux worker, which takes `--secret-file`. |
| **6** | Distributed file system (`rusty_grid_fs`) | Roadmap | Not started. |
| **7** | UDP discovery on isolated subnets | Roadmap | A rendezvous fallback is not implemented; use `--master HOST:8088` or `--p2p`. |
| **8** | Mid-task checkpointing | Done | See feature 14. |
| **9** | Tasks may run twice after a worker crash | Known limitation | When a worker is killed with SIGKILL, the processes of its running tasks are not terminated with it (observed in `run_chaos_test.sh`: the victim's task finished writing its output after the kill) while the master re-executes the task on another worker. Make tasks idempotent. |
| **10** | No protocol version negotiation: upgrade the master first | Known limitation | The wire format is positional (bincode), so a message variant that the master's build does not know is a corrupt frame there and the sender is disconnected. New workers therefore send the live battery/thermal report only to a master that announces it in its `RegisterAck`, but `ClientMessage::GetTaskResult` has no such guard. When you update a cluster, restart the **master first**, then the workers and CLIs; a worker or client newer than its master is not supported. |

---

## 4. Quickstart Guide

Ports: **8088** is the cluster port (workers and `submit` / `status` talk to it), **8080** is the dashboard
(never a valid `--master` target), **8089/udp** is discovery. Details: [docs/SECURITY.md](docs/SECURITY.md#4-what-each-port-exposes).

### Step 0: Build

```bash
cargo build --release --bin oxideswarm --bin rusty-grid     # the two names are the same program
# Add --features gpu-wgpu on a machine whose GPU should do work (Apple Silicon Mac, Windows GPU PC);
# without it the worker cannot use the GPU and advertises none:
cargo build --release --bin oxideswarm --features gpu-wgpu
```

### Step 1: Start the master and share the secret

```bash
# macOS / Linux: cluster port 8088 on every interface, dashboard on this machine only, plus a local worker
./switch_role.sh master
./switch_role.sh master --p2p              # also publish a P2P ticket for machines on other networks (step D)
./switch_role.sh master --dashboard-lan    # also open the dashboard (:8080) to phones / the LAN
./switch_role.sh master --no-worker        # master only

# the same by hand
./target/release/oxideswarm master --listen 0.0.0.0:8088
./target/release/oxideswarm master --listen 0.0.0.0:8088 --p2p --p2p-ticket-file ~/.oxideswarm/master_ticket.txt
```

Windows master: `windows\switch_role.cmd master [--p2p] [--lan-dashboard]`.

The first start of a master that other machines can reach creates **`~/.oxideswarm/cluster.secret`** and prints
where it is. **Copy that file to every machine that should join** (workers and anyone running `submit`), over a
channel you trust, for example:

```bash
scp user@master:~/.oxideswarm/cluster.secret ~/.oxideswarm/cluster.secret && chmod 600 ~/.oxideswarm/cluster.secret
```

(Windows: `%USERPROFILE%\.oxideswarm\cluster.secret`; a USB stick works too. Never chat, e-mail or cloud-synced folders.)
A master bound to loopback only (plain `oxideswarm master`) needs no secret. The dashboard token is
`oxideswarm secret show --dashboard-token`. More: [docs/SECURITY.md](docs/SECURITY.md).

### Step 2: Connect compute workers

#### A. Windows GPU rig (headless background worker)
Put the master's `cluster.secret` into `%USERPROFILE%\.oxideswarm\`, then in the `windows\` folder:
* Double-click [`windows/run_worker_silent.vbs`](windows/run_worker_silent.vbs) (looks for the master with `auto`), or
* run `windows\switch_role.cmd worker <MASTER_IP>:8088`, or `windows\start_worker.cmd <MASTER_IP>:8088`
  (the worker uses the GPU only if `rusty-grid.exe` was built with `--features gpu-wgpu`, e.g.
  `./build_windows_worker.sh --gpu`; it then finds the card itself and prints the adapter at start-up, and
  without that feature it says so and advertises **no** GPU, also with `--gpu`), or
* install a service that starts at boot: `windows\install_service.cmd <MASTER_IP>:8088 C:\path\to\cluster.secret`
  (runs as the low-privilege virtual account `NT SERVICE\OxideSwarmWorker`, not SYSTEM; see
  [packaging/windows/README.md](packaging/windows/README.md)).

#### B. Android phone (Samsung Galaxy / Pixel)
* **Termux worker (works with the secret today)**: [`packaging/android/termux`](packaging/android/README.md): run
  `install_termux_daemon.sh`, put the master's cluster address (`<MASTER_IP>:8088`) and the copied secret in place,
  then `start_worker.sh --daemon`.
* **Android app (JNI)**: build `liboxideworker.so` first with `packaging/android/build_android_jni.sh --release`
  and then the APK ([guide](packaging/android/README.md)). The app cannot hold the cluster secret yet (see
  Known Limitations #5).
* **Dashboard from the phone**: start the master with `--dashboard-lan`, open `http://<MASTER_IP>:8080` and log in with
  the token from `oxideswarm secret show --dashboard-token`. The page's **"Scan Master"** button probes the current
  host, hosts saved in the browser and hosts the master lists in `/api/status`; it does not scan your subnet.

#### C. Instant role swapping (promote Windows to master)
* On the Windows PC: `windows\switch_role.cmd master` (or menu option `1`).
* On the Mac: `./switch_role.sh worker <WINDOWS_IP>:8088` (or menu option `2`).
Both machines need the same `cluster.secret` first. Nothing else is killed: the launchers stop only the processes
they started (PID files in `~/.oxideswarm/run`).

#### D. Remote WAN Connection (Khác Mạng LAN / Qua Internet — cần Ticket **và** secret)
Khi 2 máy không cùng mạng Wi-Fi (ví dụ: MacBook ở quán cafe/công ty, Máy Case Windows ở nhà):
1. **Trên Master**, bật P2P: `./switch_role.sh master --p2p` (hoặc `oxideswarm master --listen 0.0.0.0:8088 --p2p --p2p-ticket-file ~/.oxideswarm/master_ticket.txt`). Master cần Internet ra ngoài để tới relay công cộng của iroh (n0).
   - Ticket nằm trong `~/.oxideswarm/master_ticket.txt`, được in ra khi khởi động, hoặc bấm **"📋 Copy P2P Ticket"** trên Web Dashboard.
   - Ticket giữ nguyên qua các lần khởi động lại **nếu** `~/.oxideswarm/master_key.bin` còn và master tìm được relay trong lúc khởi động; nếu không, ticket có thể chứa địa chỉ trực tiếp tạm thời và đổi sau khi restart.
2. **Chép file secret** `~/.oxideswarm/cluster.secret` từ Master sang máy phụ bằng `scp` hoặc USB. Ticket chỉ là *địa chỉ* của master, không chứng minh ai đang chạy master, nên máy phụ từ chối ghép nối nếu thiếu secret (trừ khi cố tình thêm `--insecure`).
3. **Trên Máy Phụ (Worker)**:
   - **macOS/Linux**: `./connect_remote.sh "<TICKET>" --secret-file /đường/dẫn/cluster.secret` (hoặc chạy không tham số rồi dán Ticket). Ticket được lưu vào `~/.oxideswarm/remote-worker.toml` (quyền 600); file cấu hình mặc định `rusty-grid.toml` không bị ghi đè. Thêm `--service` để cài LaunchDaemon trên macOS (chạy bằng tài khoản của bạn, không phải root).
   - **Windows**: double-click [`connect_remote.cmd`](connect_remote.cmd), dán Ticket khi được hỏi (đừng truyền JSON qua tham số dòng lệnh: cmd.exe cắt dấu phẩy và dấu nháy). Script xin quyền Administrator, cài dịch vụ `OxideSwarmWorker` chạy bằng tài khoản ảo `NT SERVICE\OxideSwarmWorker` và chờ dòng `Registration accepted` trong log.
4. **Tự động nhận diện đường truyền**:
   - Hai máy đục lỗ tường lửa (UDP hole punching) để bắt tay trực tiếp (`🟢 Direct P2P | <RTT>ms`).
   - Nếu mạng chặn UDP, lưu lượng đi qua HTTPS relay (`🟣 DERP Relay | <RTT>ms`).
   - Số liệu so sánh nằm trong [Báo cáo Đối chuẩn Kỹ thuật](benchmarks/interconnect/COMPARATIVE_INTERCONNECT_BENCHMARK.md) (đo một lần trên máy của tác giả, chưa tái lập trong CI).
   - Kết nối P2P được mã hóa bởi iroh (TLS); thêm vào đó là bước xác thực bằng secret.

#### E. Cross-Machine Shared-Folder Probe (`scripts/sync_network`)
Công cụ nhỏ để 2 máy (macOS và Windows) dùng chung một thư mục cloud (Google Drive, OneDrive, LAN mount) kiểm tra xem chúng có thấy nhau trên LAN không:
- **Phân vai SERVER/CLIENT** qua file `claim_<platform>.json` trong thư mục chia sẻ (claim cũ hơn 180 giây bị bỏ qua).
- **Ping HTTP cổng 8085** giữa hai máy; ghi `ping_success.txt` khi thành công, hoặc log lỗi kèm gợi ý xử lý.
- Công cụ **không** khởi chạy hay kiểm tra grid (8088/8080/8089) và **không** trao đổi ticket hay secret; đừng bao giờ đặt secret/ticket vào thư mục đồng bộ cloud.
- Xem [Tài liệu Sync Network](scripts/sync_network/README.md); [docs/WHAT_WAS_DONE.md](docs/WHAT_WAS_DONE.md) là ghi chép lịch sử.

### Troubleshooting

| Symptom | Likely cause and fix |
| :--- | :--- |
| `Authentication required: ...` / `refusing to connect ... without a cluster secret` (exit code 6) | This machine has no (matching) secret. Copy `~/.oxideswarm/cluster.secret` from the master, or pass `--secret-file PATH`. `--insecure` skips the check and is only for networks you fully control. |
| `Authentication failed` | The secrets differ, e.g. the master ran `secret generate --force`. Copy the master's file again and restart the worker. |
| Worker logs `Connection refused` or times out | Wrong port (use the **cluster** port 8088, not the dashboard's 8080); the master listens on loopback only (the default; start it with `--listen 0.0.0.0:8088`); a firewall blocks 8088. |
| A worker started with `--master HOST:8088` never registers and logs P2P or `Invalid P2P ticket` errors | A `p2p_ticket` sits in the config file the CLI auto-loads (`RUSTY_GRID_CONFIG`, `./rusty-grid.toml`, `~/.oxideswarm/rusty-grid.toml`) and may take precedence over `--master`. Remove that line. `connect_remote.sh` now writes `remote-worker.toml` instead and never touches `rusty-grid.toml`. |
| `--master auto` finds nothing | The UDP beacon (`:8089`) is not routed (Wi-Fi client isolation, another subnet, VPN) or the beacon's secret does not match. Use `HOST:8088`. |
| Dashboard answers `401` or shows a login prompt | Paste the token from `oxideswarm secret show --dashboard-token` (on the master). A phone can only reach the dashboard if the master was started with `--web-ui-addr 0.0.0.0:8080` / `--dashboard-lan`. |
| `Address already in use` on 8088 | Another master, or the agent-mesh hub (it also defaults to 8088). Stop it or choose another port (`OXIDESWARM_CLUSTER_PORT` for the launchers, `--listen` by hand). |
| P2P pairing hangs | The master needs `--p2p` and Internet access to the public relays; the pairing machine needs the ticket **and** the secret. |
| `GPU tasks never reach the GPU` / `worker starts with "no GPU is advertised"` | The binary was built without the wgpu engine. Rebuild with `cargo build --release --bin oxideswarm --features gpu-wgpu` (Windows cross-build: `./build_windows_worker.sh --gpu`); the worker prints `GPU compute enabled on <adapter>` at start-up when it works (see Capabilities #15). `--gpu` on a binary without the engine is ignored with a warning. |
| A GPU task fails with `GPU unavailable` / `GPU device error` | The worker advertised a GPU but the engine could not use it (no adapter, driver reset). The task failed instead of running on the CPU; the next task rebuilds the engine. Check the worker log. |

---

## 5. CLI Reference & Workflow Modes (`ox-mode`)

OxideSwarm includes a unified CLI management tool:

```bash
# Launch the interactive terminal UI (TUI) menu:
./ox-mode

# Code Quality & Testing Mode (Linting + Flaky test detection):
./ox-mode test lint
./ox-mode test flaky --iterations 5

# Development Mode (Live-reload file watcher):
./ox-mode dev watch

# Research & Performance Profiling Mode:
./ox-mode research bench
./ox-mode research distributed
```

### Direct CLI commands (`oxideswarm` / `rusty-grid`)

`oxideswarm` and `rusty-grid` are the same program (`rusty-grid` is the older name; use either). Every command
accepts `--help`. Clients and workers read the cluster secret from `--secret-file`, `OXIDESWARM_SECRET_FILE`,
or `~/.oxideswarm/cluster.secret`.

```bash
# Master. Default listen is 127.0.0.1:8088 (this machine only); other machines need 0.0.0.0:8088.
./target/release/oxideswarm master --listen 0.0.0.0:8088
# Dashboard on the LAN as well (explicit; default is loopback):
./target/release/oxideswarm master --listen 0.0.0.0:8088 --web-ui-addr 0.0.0.0:8080

# Worker: --master takes the CLUSTER port (default 127.0.0.1:8088; a bare host gets :8088; 'auto' = LAN discovery)
./target/release/oxideswarm worker --master 192.168.1.100:8088
./target/release/oxideswarm worker --master 192.168.1.100:8088                  # a --features gpu-wgpu build finds and uses the GPU itself
./target/release/oxideswarm worker --master 192.168.1.100:8088 --gpu            # insist: warns and advertises no GPU if there is none / no wgpu
./target/release/oxideswarm worker --master 192.168.1.100:8088 --no-gpu         # keep this machine's GPU out of the grid

# Submit work (--command is the executable, arguments follow after --; the file must exist on the worker)
./target/release/oxideswarm submit --master 127.0.0.1:8088 --command echo --wait -- hello
./target/release/oxideswarm submit --master 127.0.0.1:8088 --command sha256sum --wait -- /path/present/on/the/worker
./target/release/oxideswarm submit --master 127.0.0.1:8088 --type gpu --gpu --wait   # routed to a worker that advertises a GPU

# Inspect the grid
./target/release/oxideswarm workers --master 127.0.0.1:8088
./target/release/oxideswarm status --master 127.0.0.1:8088 --json
./target/release/oxideswarm mapreduce --master 127.0.0.1:8088 --input data.txt --mapper word_count --reducer sum

# Secret management
./target/release/oxideswarm secret generate
./target/release/oxideswarm secret show --dashboard-token
```

Exit code `6` means authentication failed. Remote or auto-discovered masters are refused without a secret unless
you pass `--insecure`.

### Environment variables

| Variable | Used by | Meaning |
| :--- | :--- | :--- |
| `OXIDESWARM_SECRET_FILE` / `OXIDESWARM_SECRET` | CLI | cluster secret file / hex value (prefer the file; tasks inherit a worker's environment) |
| `RUSTY_GRID_CONFIG` | CLI | config file (otherwise `./rusty-grid.toml`, `./rusty-grid.json`, then `~/.oxideswarm/rusty-grid.toml`) |
| `RUSTY_GRID_MASTER_LISTEN`, `RUSTY_GRID_P2P`, `RUSTY_GRID_P2P_TICKET_FILE`, `RUSTY_GRID_P2P_KEY_FILE`, `RUSTY_GRID_EPHEMERAL_KEY`, `RUSTY_GRID_INSECURE` | master | same as the flags |
| `RUSTY_GRID_MASTER`, `RUSTY_GRID_P2P_TICKET`, `RUSTY_GRID_WORKER_NAME`, `RUSTY_GRID_CORES`, `RUSTY_GRID_GPU`, `RUSTY_GRID_NO_GPU`, `RUSTY_GRID_SIMULATE_GPU`, `RUSTY_GRID_MAX_CONCURRENCY`, `RUSTY_GRID_SANDBOX_BASE_DIR`, `RUSTY_GRID_TASK_PRIORITY` | worker / clients | same as the flags (`--task-priority normal\|below-normal\|idle`, default below-normal) |
| `RUSTY_GRID_WORKER_ID_FILE` | worker | file that keeps the worker's id across restarts, for a worker that has no `--name`. A *named* worker (`--name`, `RUSTY_GRID_WORKER_NAME` or `name` in the config) keeps its id in `~/.oxideswarm/worker_id.<name>`; a worker with neither gets a new id at each start. The master treats equal ids as one machine reconnecting and closes the older connection, so workers that share a home directory (two terminals, an NFS home, a copied `~/.oxideswarm`) need different names. |
| `RUSTY_GRID_ENABLE_PHYSICAL_GPU=1` | worker (library) | look for a real GPU through the wgpu engine without `--gpu` (the CLI always looks unless `--no-gpu`); `nvidia-smi` is no longer used |
| `OXIDE_DEV_MODE=1` | master | dashboard loopback bypass (same as `--dev`; never effective on a non-loopback dashboard) |
| `OXIDE_MESH_TOKEN` | agent-mesh | hub token |
| `OXIDESWARM_HOME`, `OXIDESWARM_BIN`, `OXIDESWARM_CLUSTER_PORT`, `OXIDESWARM_DASHBOARD_PORT`, `OXIDESWARM_WORKER_NAME` | launcher scripts only | where `switch_role.sh` / `connect_remote.sh` / `windows\*.cmd` keep PID files, logs and tickets (the CLI itself always uses `~/.oxideswarm`), which binary they run and which ports they use |

---

## 6. Cross-Platform Coding Agent Mesh (`agent-mesh`)

OxideSwarm also contains an agent-to-agent communication layer (`crates/agent_mesh`) that lets coding agents on
**Windows, macOS, Ubuntu Linux and Android (Termux / ADB)** discover peers, transmit data payloads and route
structured commands. A node can be told to run **any command, including `shell_exec`**, so treat the hub token
like a password.

```text
                      ┌─────────────────────────────────┐
                      │         OxideRelay Hub          │
                      │    (axum / tokio-tungstenite)   │
                      │      Active Node Catalog        │
                      └───────▲───────▲────────▲────────┘
                              │       │        │
                Persistent WS │       │ WS     │ Persistent WS
                    (outbound)│       │        │ (outbound)
                              ▼       ▼        ▼
            ┌─────────────────┐ ┌───────────┐ ┌───────────────────┐
            │ Node 1: Windows │ │Node 2: Mac│ │ Node 3: Android   │
            │ Coding Agent    │ │Coding Agt │ │ Coding Agent      │
            └─────────────────┘ └───────────┘ └───────────────────┘
```

### Key capabilities
- **Outbound-only nodes**: nodes connect out to the hub over WebSocket, so they need no inbound port (the hub host does).
- **Explicit point-to-point addressing** (`from: "node-1"`, `to: "node-3"`); the hub enforces that `from` is the registered node id and rejects duplicate ids.
- **Dual runtime**: Rust binary `agent-mesh`, and a dependency-light Python client `scripts/agent_node.py`.
- **Delivery tracking & integrity**: correlation ids, `DeliveryAck` / `DeliveryNack`, SHA-256 verification of data payloads.
- **Authentication**: the hub listens on `127.0.0.1:8088` by default; a non-loopback `--listen` needs `--token` (or `OXIDE_MESH_TOKEN`), or `--insecure`. `ws://` is not encrypted, so use `wss://` behind a TLS proxy or a VPN / SSH tunnel, and **never expose the hub to the Internet**.

### Quickstart
```bash
# 1. Build the Agent Mesh CLI:
cargo build --release -p agent_mesh

# 2. Pick a hub token and start the hub. The hub also defaults to port 8088, which the grid master uses,
#    so give it another port when both run on one machine:
export OXIDE_MESH_TOKEN="$(openssl rand -hex 24)"
./target/release/agent-mesh hub --listen 127.0.0.1:8090          # loopback; use 0.0.0.0:8090 + token for a LAN

# 3. Connect an agent node (Windows / Mac / Linux / Android); every command below reads OXIDE_MESH_TOKEN:
./target/release/agent-mesh node --hub ws://127.0.0.1:8090/ws --id node-mac-1 --platform macos
python3 scripts/agent_node.py --hub ws://127.0.0.1:8090/ws --id node-android-3 --platform android

# 4. Route a command from any node or terminal:
./target/release/agent-mesh send --hub ws://127.0.0.1:8090/ws --to node-android-3 --command shell_exec --args '{"cmd": "uname -m"}'

# 5. Query the online nodes catalog:
./target/release/agent-mesh list --hub ws://127.0.0.1:8090/ws
```

*Platform guides: [DEPLOYMENT.md](DEPLOYMENT.md).*

---

## 7. Workspace Crate Architecture

```text
OxideSwarm/
├── Cargo.toml                     # Workspace root manifest (no root package)
├── README.md                      # This file
├── CLAUDE.md                      # Architecture map for AI coding assistants
├── docs/SECURITY.md               # Threat model, ports, secret handling, rotation
├── DEPLOYMENT.md                  # Agent Mesh deployment guide
├── ANDROID_ARCHITECTURE.md        # Android architecture (parts are design only, marked as such)
├── switch_role.sh                 # Master <-> Worker role switcher for macOS / Linux
├── connect_remote.sh              # P2P WAN pairing for macOS / Linux
├── connect_remote.cmd             # P2P WAN pairing for Windows
├── test_integration.sh            # E2E: 1 master + 3 workers on loopback, private HOME
├── run_chaos_test.sh              # local failover test (kills a worker that holds running tasks)
├── run_cluster_benchmark.sh       # two-node benchmark (two local workers, or a phone via adb)
├── ox-mode -> scripts/mode.sh     # Unified CLI & TUI workflow automation runner
├── windows/                       # Windows launchers (oxctl.ps1 engine + .cmd/.vbs wrappers)
├── assets/                        # Vector branding assets
├── benchmarks/interconnect/       # Interconnect comparison: static recorded data + a validator script
├── scripts/
│   ├── sync_network/              # Shared-folder reachability probe (not part of the grid)
│   ├── agent_node.py              # Lightweight Python agent node client
│   └── mode.sh                    # Workflow mode automation script
├── packaging/
│   ├── agent_mesh/                # Agent node launchers (windows / macos / ubuntu / android)
│   ├── windows/                   # Windows service: ServiceWrapper.cs (csc.exe) or WinSW, installer, verifier
│   ├── android/                   # Android app project, Termux worker daemon, JNI build script
│   └── macos/                     # LaunchDaemon installer / verifier / uninstaller
├── crates/
│   ├── core/                      # [Layer 1] Wire protocol, codec, cluster_auth (secret handshake), discovery, workflow modes
│   ├── master/                    # [Layer 1] Scheduler, GPU routing, FSM, LRU bounding, Axum dashboard, RBAC auth
│   ├── worker/                    # [Layer 1] Task runner, process-tree kill, CPU SIMD / wgpu compute, standby portal
│   ├── cli/                       # [Layer 1] `oxideswarm` / `rusty-grid` / `ox-mode`; connect.rs (client plumbing), secret_cmd.rs
│   ├── android_bridge/            # [Layer 2] JNI bridge (liboxideworker.so)
│   └── agent_mesh/                # [Layer 3] Axum WebSocket relay hub & agent client
└── tests/                         # Integration tests (registered per crate in Cargo.toml) and Python suites
```

---

## 8. Verification & Benchmarks

There is no CI configuration in this repository: everything below is run by hand. The last full
`cargo test --workspace` on the maintainer's Mac (after the security hardening) passed 800+ tests. Tests that depend
on the public iroh relays are timing- and network-sensitive: `rusty_grid_android_bridge`'s
`test_adversarial_worker_connect_via_master_p2p_ticket` failed even before the hardening, and in
`tests/test_m5_adversarial_stress.py` the ticket-determinism and reconnect-timing checks failed in a recent run
because a different relay was chosen or the reconnect took 5 s instead of 3 s.

```bash
# Rust unit and integration tests (slow: many stress tests)
cargo test --workspace

# Agent Mesh against the RUST hub (build first with: cargo build -p agent_mesh)
cargo test -p agent_mesh
python3 tests/test_agent_mesh_token_auth.py
python3 tests/test_agent_mesh_rust_hub_cross_interop.py

# Protocol simulation against a PYTHON reference hub (tests/run_all_mesh_tests.py does NOT start the Rust hub)
python3 tests/run_all_mesh_tests.py

# Launcher / installer scripts (switch_role.sh, connect_remote.sh, Termux supervisor, macOS installer dry-run,
# line endings, no hard-coded owner values): runs them against fake binaries in a private HOME; no build needed
python3 tests/test_launcher_scripts.py

# End-to-end on loopback: 1 master + 3 workers, one simulated-GPU worker. Uses a private HOME and
# ephemeral ports, touches no running master/worker and only signals the PIDs it started.
./test_integration.sh

# Failover: starts its own cluster, SIGKILLs a worker that holds running tasks, checks re-execution
./run_chaos_test.sh

# Throughput of two nodes (two local worker processes by default; --adb-id SERIAL uses a phone)
./run_cluster_benchmark.sh

# Static comparison data check + two local Rust benches (does NOT measure Tailscale or Cloudflare)
./benchmarks/interconnect/run_comparative_benchmark.sh
```

The scripts exit non-zero and say FAIL when a check fails; their reports are computed from what was observed in
that run. The `100% PASS` banners in older write-ups such as `TEST_READY.md` and `docs/WHAT_WAS_DONE.md` are
historical and were not produced by these scripts.

---

## 9. License

OxideSwarm is dual-licensed under:
* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
* MIT License ([LICENSE-MIT](LICENSE-MIT))

You may choose either license at your option.
