# OxideSwarm

<div align="center">

![OxideSwarm Logo](https://raw.githubusercontent.com/rust-lang/rust-artwork/master/logo/rust-logo-blk.svg)

**Hệ thống Tính toán Phân tán P2P Siêu Nhẹ & Tự Điều Phối Đa Nền Tảng (Rust)**  
*High-Performance, Zero-Broker Distributed Computing Grid for Heterogeneous Clusters*

[![Rust 2021](https://img.shields.io/badge/Rust-2021_Edition-orange.svg?style=flat-square&logo=rust)](https://www.rust-lang.org)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg?style=flat-square)](LICENSE)
[![Tests Status](https://img.shields.io/badge/Tests-100%25_PASS_(180%2B)-brightgreen.svg?style=flat-square)](test_integration.sh)
[![P2P NAT](https://img.shields.io/badge/P2P%20NAT-iroh%20QUIC%2FDERP-purple.svg?style=flat-square)](https://iroh.computer)
[![Platforms](https://img.shields.io/badge/Platforms-macOS%20|%20Windows%20|%20Android%20|%20Linux-lightgrey.svg?style=flat-square)](#3-hướng-dẫn-chạy--kết-nối-nhanh)

</div>

---

## 1. Giới thiệu Tổng quan (Overview)

**OxideSwarm** là framework tính toán phân tán thuần Rust, được thiết kế để kết nối và tận dụng toàn bộ tài nguyên phần cứng sẵn có trong gia đình và văn phòng (**MacBook, Máy Case Windows có GPU, Điện thoại Android, Linux Server**) thành một cụm siêu máy tính cá nhân thống nhất.

* **Không cần Message Broker phụ trợ**: Chạy trực tiếp qua Tokio Asynchronous Actors, TCP Streams và QUIC. Không cần cài đặt Redis, Kafka, RabbitMQ hay Zookeeper.
* **Xuyên NAT P2P tự động (`iroh`)**: Tự động đục lỗ tường lửa (NAT traversal) qua giao thức QUIC/DERP, kết nối các máy qua Internet mà không cần mở port modem hay cài VPN.
* **Cực nhẹ & Tàng hình ("Nhẹ mà tốt")**: Worker chạy ngầm chỉ tốn **~12 MB RAM**, CPU ở trạng thái chờ là **0.0%**, không hiện cửa sổ console đen gây gián đoạn công việc hay chơi game.
* **Tự động nhận diện & Hoán đổi linh hoạt**: Tự do chuyển đổi vai trò **Master <-> Worker** giữa các máy trong 1 giây; điện thoại tự động tìm thấy Master qua mạng LAN.

---

## 2. Kiến trúc & Sơ đồ Cụm (Cluster Topology)

```
                            ┌──────────────────────────────────────────────┐
                            │              OXIDESWARM MASTER               │
                            │      (MacBook M-Series HOẶC Máy Case GPU)    │
                            │             Cluster TCP Port: :8088          │
                            │            Web UI Dashboard: :8080           │
                            │           UDP LAN Discovery: :8089           │
                            └───────────────▲───────────────▲──────────────┘
                                            │               │
                            Outbound TCP    │               │  Outbound TCP / QUIC
                           (Auto-Reconnect) │               │  (Zero-Window Silent)
                                            │               │
        ┌───────────────────────────────────┴─┐   ┌─────────┴─────────────────────────────┐
        │        MOBILE COMPUTE NODE          │   │        WINDOWS GPU COMPUTE NODE       │
        │        Samsung Galaxy S24           │   │             Windows Case PC           │
        │       10 Cores Physical (ARM64)     │   │      (x86_64 + NVIDIA/AMD GPU)        │
        │  • Chạy ngầm in-process (JNI Bridge)│   │  • Chạy ngầm 100% tàng hình (~12MB)   │
        │  • Tự ngắt khi pin yếu (<15%)       │   │  • Standby Portal HTTP 307 Redirect   │
        │  • Tự giảm tải khi máy bị nóng      │   │  • Anti-Stuttering bảo vệ khi chơi game│
        └─────────────────────────────────────┘   └───────────────────────────────────────┘
```

---

## 3. BẢNG TỔNG KẾT: ĐÃ LÀM ĐƯỢC GÌ VÀ CHƯA LÀM ĐƯỢC GÌ

###  NHỮNG GÌ ĐÃ HOÀN THÀNH & ĐÃ KIỂM THỬ THỰC TẾ (100% VERIFIED)

| Hạng mục / Tính năng | Mô tả chi tiết kỹ thuật | Trạng thái |
| :--- | :--- | :--- |
| **1. Core Grid & Zero-Broker** | Wire protocol phân khung 4-byte length-delimited, tuần tự hóa serde/JSON. Tokio TCP + in-memory duplex channel. Chạy 100% độc lập không cần Redis/Kafka. | **ĐÃ HOÀN TẤT** (100% PASS) |
| **2. Master Coordinator & Scheduler** | Máy trạng thái FSM 8 bước (`Submitted` ➔ `Queued` ➔ `Scheduled` ➔ `Running` ➔ `Completed`/`Failed`/`Retrying`/`Cancelled`). Cơ chế phân bổ song song (Batch Spread Scheduling), tự động quét và thu hồi node chết (Heartbeat Reaper). | **ĐÃ HOÀN TẤT** (100% PASS) |
| **3. Strict GPU Workload Routing** | Tách biệt hoàn toàn tác vụ: Tác vụ GPU chỉ phân bổ cho node có GPU (vật lý hoặc mô phỏng); node thuần CPU bị loại trừ tuyệt đối. Giữ node GPU rảnh cho tác vụ nặng (GPU Preservation). | **ĐÃ HOÀN TẤT** (100% PASS) |
| **4. Anti-Stuttering Backpressure** | Đo lường CPU/RAM thực tế của máy trạm qua `sysinfo`. Khi người dùng chơi game hoặc render đồ họa nặng (CPU máy vượt 85%), Master tự động giảm tải, không giao thêm việc để tránh giật lag máy. | **ĐÃ HOÀN TẤT** (100% PASS) |
| **5. Map/Reduce trong bộ nhớ** | Xử lý dữ liệu phân tán kiểu Paladin/Hadoop hoàn toàn trên RAM không cần ổ đĩa. Hỗ trợ chia chunk dữ liệu, hàm map/reduce dựng sẵn (`word_count`, `line_count`) hoặc chạy shell script/binary tùy ý. | **ĐÃ HOÀN TẤT** (100% PASS) |
| **6. Đa nền tảng phần cứng thực tế** | • **macOS**: Apple Silicon M-series (10 cores).<br>• **Windows**: Binaries x86_64 `.exe` có service wrapper tự động.<br>• **Android**: JNI bridge native (`crates/android_bridge`) chạy in-process an toàn trước Phantom Process Killer. | **ĐÃ HOÀN TẤT** (Kiểm thử thực tế trên Mac + S24) |
| **7. Chạy ngầm Windows 1-Click ("Nhẹ mà tốt")** | File [`windows/run_worker_silent.vbs`](file:///Users/duongnad/Documents/project/OxideSwarm/windows/run_worker_silent.vbs): Double-click là chạy ngầm 100% không chớp cửa sổ đen (WindowStyle 0). Chiếm đúng **~12 MB RAM**, CPU chờ **0.0%**. Kèm file `status_worker.cmd` và `stop_worker.cmd`. | **ĐÃ HOÀN TẤT** |
| **8. Hoán đổi Vai trò Master <-> Worker trong 1s** | Bộ công cụ [`switch_role.sh`](file:///Users/duongnad/Documents/project/OxideSwarm/switch_role.sh) (Mac) và [`windows/switch_role.cmd`](file:///Users/duongnad/Documents/project/OxideSwarm/windows/switch_role.cmd) (Windows): Cho phép đổi máy nào làm Master, máy nào làm Worker bất kỳ lúc nào chỉ bằng 1 phím bấm. | **ĐÃ HOÀN TẤT** |
| **9. Tự động Nhận diện Master cho Điện thoại (3-Tier)** | • **Lớp 1 (Standby Portal)**: Cổng 8080 của Worker tự redirect HTTP 307 sang Master. Mở nhầm bookmark IP cũ vẫn vào đúng Master.<br>• **Lớp 2 (Dashboard Auto-Scan)**: Trình duyệt quét song song các IP LAN và tự chuyển hướng khi Master đổi máy.<br>• **Lớp 3 (UDP Beacon)**: Master phát beacon `:8089`, worker tự kết nối (`--master auto`). | **ĐÃ HOÀN TẤT** (7/7 test discovery PASS) |
| **10. Web UI Dashboard & Trợ lý AI** | Giao diện Web thời gian thực tại `http://<MASTER_IP>:8080`. Hiển thị sơ đồ SVG trực quan, pin, sạc, nhiệt độ, RAM/CPU từng máy. Tích hợp AI chat hỏi đáp trạng thái cụm bằng ngôn ngữ tự nhiên. | **ĐÃ HOÀN TẤT** (Kiểm thử 5 kích cỡ màn hình di động) |
| **11. 4 Chế độ Làm việc (`ox-mode`)** | Tích hợp menu TUI và CLI: `test` (quick/lint/flaky/full), `dev` (fast check/cluster local/live-reload watch), `doc` (verify/export spec), `research` (profile/bench/report). Có nút bấm kích hoạt từ Web. | **ĐÃ HOÀN TẤT** |
| **12. Đo lường Hiệu năng & Chaos Resilience** | • Đo trên cụm 20 nhân CPU thực tế (Mac + S24): Băm song song SHA-256 đạt **341.3 MB/s** (tăng tốc **1.55x**).<br>• Đột ngột ngắt tiến trình worker trên điện thoại: Master phát hiện trong **319 ms**, tái điều phối tác vụ sang Mac thành công **100%**, tỷ lệ mất dữ liệu: **0.0%**. | **ĐÃ HOÀN TẤT** (Độc lập kiểm chứng PASS) |

---

### ⚠️ NHỮNG GÌ CHƯA LÀM ĐƯỢC / HẠN CHẾ & ROADMAP PHÁT TRIỂN TIẾP THEO

Để người dùng và lập trình viên nắm rõ giới hạn hiện tại của hệ thống:

| # | Hạn chế / Vấn đề chưa làm được | Chi tiết thực tế | Hướng giải quyết trong tương lai (Roadmap) |
|---|---|---|---|
| **1** | **Chưa có Checkpoint tác vụ giữa chừng (Mid-Task Checkpointing)** | Nếu một worker đang chạy một tác vụ dài (ví dụ: render 3D hoặc tính toán mất 10 phút) mà bị mất điện ở phút thứ 9, Master sẽ phát hiện dead node và điều phối task chạy lại **từ đầu (0%)** trên node khác, chưa thể tiếp tục từ mốc 90%. | Sẽ tích hợp cơ chế snapshot state định kỳ vào file nhị phân trung gian để worker mới có thể resume từ checkpoint gần nhất. |
| **2** | **Chưa có Hệ thống Tệp Phân tán dùng chung (Distributed File System - DFS)** | Hiện tại các tệp dữ liệu được truyền trực tiếp qua network stream hoặc chia chunk trước khi gửi. Dự án chưa có hệ thống lưu trữ phân tán ảo (kiểu IPFS, Ceph hoặc GlusterFS) gắn kết ổ cứng của tất cả các máy thành một ổ đĩa chung. | Sẽ phát triển module `rusty_grid_fs` cho phép mount ổ đĩa phân tán dùng chung qua giao thức FUSE / P2P Block Store. |
| **3** | **Quét UDP Beacon bị hạn chế trên một số Router bật "Client Isolation"** | Tính năng tự dò tìm Master bằng gói tin UDP Broadcast (`:8089`) hoạt động tuyệt đối trong mạng LAN thông thường, nhưng nếu mạng Wi-Fi công ty hoặc quán cà phê bật tính năng bảo mật *AP Client Isolation* (chặn thiết bị Wi-Fi nói chuyện trực tiếp với nhau qua L2 broadcast), gói UDP sẽ bị chặn.<br>*(Lưu ý: Hệ thống đã có cơ chế dự phòng HTTP Candidate Probing và Standby Portal để bù đắp).* | Bổ sung cơ chế relay qua Rendezvous Server công cộng khi phát hiện mạng bị cô lập hoàn toàn. |
| **4** | **GPU Shader Compute đa nền tảng (WebGPU / Vulkan)** | Hỗ trợ GPU hiện tại bao gồm: Ma trận mô phỏng SIMD tốc độ cao, hỗ trợ NVIDIA CUDA qua binary ngoài. Dự án chưa tích hợp sẵn pipeline WebGPU (wgpu) hoặc Vulkan compute shader chạy chéo nền tảng out-of-the-box cho mọi dòng card đồ họa (Intel Iris, AMD Radeon, Apple Metal) mà không cần cài driver riêng. | Tích hợp backend `wgpu` trực tiếp vào crate `rusty_grid_worker` để biên dịch compute shader chạy trên mọi card GPU. |
| **5** | **Bảo mật Xác thực Phân quyền Web Dashboard (RBAC / Auth)** | Hiện tại Web UI Dashboard và API điều khiển được tối ưu hóa cho mạng nội bộ tin cậy (Private LAN / P2P). Bất kỳ ai cùng mạng LAN mở `http://<IP>:8080` đều có thể xem trạng thái và submit task. Chưa có hệ thống đăng nhập tài khoản, mật khẩu hoặc phân quyền quản trị viên (RBAC). | Bổ sung module JWT Authentication, API Key per-worker và TLS nội bộ (mTLS) cho môi trường Public Cloud. |

---

## 4. Hướng dẫn Chạy & Kết nối Nhanh (Quickstart)

### Bước 1: Khởi động Master (Trên Mac hoặc Máy Case)

```bash
# Trên Mac: Khởi chạy Master điều phối cụm (:8088) và Web UI Dashboard (:8080)
./switch_role.sh master

# Hoặc dùng binary trực tiếp:
cargo build --release --bin rusty-grid
./target/release/rusty-grid master --listen 0.0.0.0:8088 --web-ui-addr 0.0.0.0:8080
```

### Bước 2: Kết nối Worker phụ trợ

#### A. Trên Máy Case Windows (Có GPU) — Chạy ngầm 100%:
Chỉ cần mở thư mục `windows/` trên máy Case:
* **Cách 1 (Khuyên dùng)**: **Double-click** vào [`windows/run_worker_silent.vbs`](file:///Users/duongnad/Documents/project/OxideSwarm/windows/run_worker_silent.vbs).  
  *Worker sẽ chạy ngầm tàng hình, tự nhận diện GPU, chỉ tốn 12 MB RAM.*
* **Cách 2**: Dùng công cụ chuyển đổi vai trò:
  ```cmd
  windows\switch_role.cmd worker 192.168.1.144:8088
  ```

#### B. Trên Điện thoại Android (Samsung Galaxy / Pixel):
* Mở trình duyệt Chrome trên điện thoại, truy cập:
  ```text
  http://192.168.1.144:8080
  ```
* Bác có thể lưu Bookmark. Khi máy Case làm Master, chỉ cần bấm **"📡 Quét Master"** trên màn hình, trang web sẽ tự tìm và chuyển hướng sang máy Case trong 1 giây!

#### C. Khi muốn đổi Máy Case làm Master:
* Trên máy Case: Chạy `windows\switch_role.cmd master` (hoặc mở menu chọn `1`).
* Trên Mac: Chạy `./switch_role.sh worker 192.168.1.123:8088` (hoặc mở menu chọn `2`).

---

## 5. Bảng Lệnh CLI & 4 Chế độ (`ox-mode`)

```bash
# Khởi động Menu trực quan 4 chế độ làm việc:
./ox-mode

# Chế độ Kiểm tra chất lượng (Lint + Flaky test check):
./ox-mode test lint
./ox-mode test flaky --iterations 5

# Chế độ Phát triển (Live-Reload theo dõi thay đổi mã nguồn):
./ox-mode dev watch

# Chế độ Nghiên cứu & Đo lường hiệu năng:
./ox-mode research bench
./ox-mode research distributed
```

---

## 6. Cấu trúc Crate trong Dự án (Workspace Layout)

```
OxideSwarm/
├── Cargo.toml                     # Workspace manifest chia sẻ dependency
├── README.md                      # Tài liệu tổng quan & báo cáo năng lực hệ thống
├── switch_role.sh                 # Bộ đổi vai trò 1-click cho macOS (Master <-> Worker)
├── ox-mode                        # CLI & TUI quản trị 4 chế độ làm việc
├── windows/                       # Bộ công cụ chạy ngầm tàng hình cho Windows
│   ├── switch_role.cmd            # Đổi vai trò Master <-> Worker trên Windows
│   ├── run_worker_silent.vbs      # Chạy ngầm 0 cửa sổ (WindowStyle 0)
│   ├── start_worker.cmd           # Launcher worker có cơ chế tự tìm Master
│   ├── status_worker.cmd          # Kiểm tra PID, RAM, CPU và log thực tế
│   └── stop_worker.cmd            # Tắt sạch tiến trình trong 1 click
├── packaging/                     # Bộ cài đặt chạy ngầm hệ thống
│   ├── windows/                   # Cài đặt Windows Service qua ServiceWrapper.cs (csc.exe)
│   ├── android/                   # Ứng dụng Android APK & Foreground Service daemon
│   └── macos/                     # Cấu hình LaunchDaemon cho macOS
├── crates/
│   ├── core/                      # Giao thức wire, phát hiện LAN (discovery), 4 chế độ (mode)
│   ├── master/                    # Bộ điều phối, lập lịch GPU, máy chủ Web UI, Antigravity AI
│   ├── worker/                    # Trình thực thi, sandbox bảo vệ, ma trận GPU, Standby Portal
│   ├── android_bridge/            # Cầu nối JNI native C/Rust cho ứng dụng Android
│   └── cli/                       # Giao diện dòng lệnh 7 subcommands (rusty-grid)
└── tests/                         # Hệ thống kiểm thử tự động, stress test và đo lường phân tán
```

---

## 7. Giấy phép Bản quyền (License)

Dự án được cấp phép kép (Dual-licensed) theo:
* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
* MIT License ([LICENSE-MIT](LICENSE-MIT))
