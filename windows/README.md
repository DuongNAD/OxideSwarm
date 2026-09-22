# OxideSwarm Windows Background Runner (Siêu nhẹ & Chạy ngầm 100%)

Gói giải pháp chạy ngầm độc lập dành riêng cho **Máy Case Windows** (`DESKTOP-3EPV830`):

## 1. Ưu điểm cốt lõi ("Nhẹ thôi nhưng tốt")
- **Siêu nhẹ (Ultra-lightweight)**:
  - Viết bằng Rust nguyên bản (Native binary)
  - Mức chiếm dụng RAM chỉ **~12 MB**
  - CPU ở chế độ chờ (idle): **0.0%**
  - Không phụ thuộc Node.js, Python, Java hay Electron
- **Chạy ngầm tàng hình (Zero-Window Silent Execution)**:
  - 100% không chớp cửa sổ đen (Console window) khi chạy
  - Không làm gián đoạn trải nghiệm chơi game hay làm việc trên máy Case
- **Độ tin cậy cao (Industrial Grade)**:
  - Tự động bắt tay GPU (NVIDIA / AMD) và báo về Master
  - Tự động kết nối lại (Auto-reconnect with exponential backoff) nếu Master khởi động lại
  - Ghi log xoay vòng gọn gàng vào `worker.log`

---

## 2. Các file thao tác 1-Click

| Tên File | Chức năng | Thao tác |
| :--- | :--- | :--- |
| **`run_worker_silent.vbs`** | **Chạy ngầm tức thì** (Khuyên dùng) | **Chỉ cần Double-click** |
| `start_worker.cmd` | Chạy ngầm kèm kiểm tra build | Double-click |
| `status_worker.cmd` | Xem tình trạng tiến trình & log thực tế | Double-click |
| `stop_worker.cmd` | Dừng chạy ngầm ngay lập tức | Double-click |
| `install_service.cmd` | Cài làm Windows Service (chạy tự động khi bật máy) | Double-click (Run as Admin) |

---

## 3. Cấu hình mặc định
- **Master IP**: `192.168.1.144:8088` (MacBook M-Series)
- **Tên Node**: `windows-case-gpu`
- **GPU Acceleration**: Bật (`--gpu`)
- **Heartbeat**: 3 giây/lần
