# OxideSwarm Windows Launchers (chạy ngầm, không cửa sổ)

Bộ launcher cho máy Windows (ví dụ máy Case có GPU) để chạy **worker** hoặc **master** OxideSwarm ở chế độ nền. Các file `.cmd`/`.vbs` chỉ là lớp vỏ; phần việc nằm trong `oxctl.ps1`, nên mọi vai trò dùng chung một logic.

> Các script này được viết cho Windows PowerShell 5.1 / cmd và **chưa có test tự động nào chạy chúng** (kho mã này được kiểm thử trên macOS). Hãy xem log nếu có gì lạ.

## 1. Ưu điểm và giới hạn (số liệu là đo một lần trên máy tác giả)
- **Nhẹ**: viết bằng Rust, không cần Node.js, Python, Java hay Electron. Worker chiếm khoảng 7-15 MB RAM khi rảnh và gần như không dùng CPU theo các lần đo của tác giả (không tái lập được trong CI). `status_worker.cmd` in dung lượng RAM **thực tế** của tiến trình đang chạy.
- **Chạy ngầm**: không có cửa sổ console chớp lên khi chạy.
- **Tự kết nối lại**: worker tự reconnect (exponential backoff) khi Master khởi động lại.
- **GPU**: worker tự dò card đồ họa, nhưng chỉ dùng được nếu `rusty-grid.exe` được build với engine wgpu (`cargo build --release --bin rusty-grid --features gpu-wgpu`, hoặc `./build_windows_worker.sh --gpu` khi cross-compile; launcher tự build kèm `gpu-wgpu` khi phải biên dịch tại chỗ, đặt `OXIDESWARM_CARGO_FEATURES=` để bỏ). Bản build không có wgpu **không khai báo GPU**, kể cả khi thêm `--gpu`, và nói rõ điều đó trong `worker.log` (dòng `GPU:` / `Warning:`); nó không còn chạy tác vụ GPU bằng CPU rồi gọi là GPU. Thêm `--no-gpu` để không đưa card của máy vào cụm. Phần build Windows chưa được chạy thử trong CI.
- **Log**: `%USERPROFILE%\.oxideswarm\run\worker.log` và `worker.err.log` (mỗi lần khởi động log cũ được đổi thành `.old`; không có log xoay vòng theo dung lượng ở chế độ này).

## 2. Cần chuẩn bị: cổng 8088 và file secret
1. **Địa chỉ Master là cổng cụm `IP:8088`** (cổng 8080 là dashboard, không dùng cho worker). Có thể để `auto` để dò Master trong LAN bằng UDP.
2. **Secret**: Master tạo `%USERPROFILE%\.oxideswarm\cluster.secret` (Windows) hoặc `~/.oxideswarm/cluster.secret` (Mac/Linux) ở lần chạy đầu. Hãy chép **file** đó sang máy Windows này, vào `%USERPROFILE%\.oxideswarm\cluster.secret`, bằng `scp` hoặc USB (không gửi qua chat/email). Thiếu secret, worker từ chối đi theo Master ở xa hoặc tìm bằng `auto` (trừ khi thêm `--insecure`).
3. Chi tiết mô hình bảo mật: [../docs/SECURITY.md](../docs/SECURITY.md).

## 3. Các file thao tác 1-Click

| Tên File | Chức năng | Thao tác |
| :--- | :--- | :--- |
| **`run_worker_silent.vbs`** | **Chạy worker ngầm** (Master = `auto`); hiện thông báo nếu không khởi động được | Double-click |
| `start_worker.cmd` | Chạy worker ngầm: `start_worker.cmd [IP:8088\|auto] [--gpu ...]` | Terminal / double-click |
| `run_master_silent.vbs` | Chạy master ngầm (dashboard chỉ trên máy này) | Double-click |
| `start_master.cmd` | `start_master.cmd [--lan-dashboard] [--p2p] [--no-worker]` | Terminal |
| `switch_role.cmd` | Đổi vai: `switch_role.cmd master \| worker IP:8088 \| status \| stop` (hoặc menu) | Terminal / double-click |
| `status_worker.cmd` | Xem tiến trình do launcher chạy, RAM thực tế, log cuối | Double-click |
| `stop_worker.cmd` | Dừng **chỉ** tiến trình do launcher đã chạy (theo PID file) | Double-click |
| `install_service.cmd` | Cài Windows Service chạy khi bật máy: `install_service.cmd IP:8088 [file-secret]` | Double-click / Terminal (tự xin quyền Admin) |
| `oxctl.ps1` | Engine PowerShell dùng chung cho các file trên | (không chạy trực tiếp) |
| `..\connect_remote.cmd` | Ghép nối với Master ở **mạng khác** bằng P2P ticket + secret | Double-click |

Không có file nào dừng tiến trình theo **tên** (`taskkill /IM rusty-grid.exe`): launcher chỉ dừng PID mà nó đã ghi trong `%USERPROFILE%\.oxideswarm\run`, nên Windows Service `OxideSwarmWorker` hay một master bạn chạy tay không bị đụng tới. Muốn dừng service: `Stop-Service OxideSwarmWorker` trong PowerShell quyền Admin.

## 4. Master trên Windows
`start_master.cmd` chạy `master --listen 0.0.0.0:8088 --web-ui-addr 127.0.0.1:8080`: cổng cụm mở cho LAN (được bảo vệ bằng secret, **không mã hóa**), dashboard chỉ mở trên chính máy này. Thêm `--lan-dashboard` nếu muốn mở dashboard cho điện thoại/LAN (khi đó đăng nhập bằng token lấy từ `rusty-grid secret show --dashboard-token`). Thêm `--p2p` để Master xuất ticket cho máy ở mạng khác (ticket nằm ở `%USERPROFILE%\.oxideswarm\master_ticket.txt`). Lần chạy đầu Master tự tạo `cluster.secret`; hãy chép file đó cho các worker.

## 5. Cấu hình mặc định
- **Master**: `auto` (dò UDP) nếu bạn không truyền địa chỉ; nên truyền `IP:8088` rõ ràng.
- **Tên node**: `<tên máy>-worker` (đổi bằng tham số `-WorkerName` khi gọi `oxctl.ps1` trực tiếp).
- **GPU**: tự dò khi `rusty-grid.exe` có `gpu-wgpu`; không có thì không khai báo GPU. `--no-gpu` để tắt.
- **Thư mục trạng thái**: `%USERPROFILE%\.oxideswarm` (đặt `OXIDESWARM_HOME` để đổi nơi lưu PID/log/ticket của launcher; CLI vẫn đọc secret mặc định ở `%USERPROFILE%\.oxideswarm\cluster.secret` trừ khi launcher được cho `OXIDESWARM_HOME`, khi đó nó truyền `--secret-file` tường minh).
- **Cổng**: `OXIDESWARM_CLUSTER_PORT` (mặc định 8088) và `OXIDESWARM_DASHBOARD_PORT` (mặc định 8080) chỉ đổi cổng mà launcher dùng; worker phải trỏ đúng cổng cụm.
