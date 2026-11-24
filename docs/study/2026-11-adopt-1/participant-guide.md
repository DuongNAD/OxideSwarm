# OxideSwarm First-Use Study — Participant Guide (EN / VI)

> One-page plain guide for participants (`ADOPT-1` round one). Read before starting. You can stop at any step.

---

## English

### What you will do (about 15 to 30 minutes)
1. **Install OxideSwarm** on your computer by following the public [README](../../../README.md#install) (or using the USB folder if the facilitator hands you one). Tell the facilitator aloud whenever something is unclear before asking for help.
2. **Join the temporary study cluster** using a one-time invite from the facilitator, approve the 6-digit pairing check, and start one sandboxed worker (`oxideswarm worker --isolation fs`).
3. **Run your first command task** (`oxideswarm submit --wait -- ...`) and check its output.
4. **Run a small file task and (optional) one job of your own:** send one tiny dummy input file and receive an output file back; if you named a slow computer job earlier, try running a small version of it.
5. **Stop the worker** cleanly (`Ctrl+C`) and confirm everything stops.

### How your machine stays protected
- This study uses a **dedicated temporary cluster**, separate from any personal or production cluster. If you already use OxideSwarm, set `OXIDESWARM_HOME` to a temporary folder so your own configuration is never touched.
- Tasks run with `--isolation fs` (where supported by your OS), restricting file writes to the task's own temporary sandbox and blocking reads of your home folder and SSH keys.
- You stay in control: only your terminal starts the worker, and pressing `Ctrl+C` ends running tasks immediately.

### What is recorded (and what is never collected)
- **Recorded (under an anonymous label `P1`..`P5`):** your OS and architecture, release version, how long each step took, where you paused or asked for help, what kind of job you tried, and (once, 14 days later) whether you used OxideSwarm again.
- **Never collected or stored in records:** your real name, username, computer hostname, IP address, passwords, invite codes (`oxs1-...`), pairing codes, `cluster.secret`, `node.key`, contents of your files, screen recordings, or background usage telemetry. OxideSwarm has no telemetry; across networks (`--p2p`), encrypted traffic between the machines may pass through a public relay (n0/iroh), which sees addresses and amounts, not content.

### How to stop or remove everything
- Press `Ctrl+C` in the worker terminal at any time to disconnect and terminate any running task on your machine.
- Tell the facilitator you want to stop; no reason is needed.
- To clean up afterwards, delete the downloaded `oxideswarm` binary folder and only the temporary folder set as `OXIDESWARM_HOME` for the study (never delete an existing `~/.oxideswarm`), and ask the facilitator to revoke your temporary study key (`oxideswarm members revoke`).

---

## Tiếng Việt

### Bạn sẽ làm gì (khoảng 15 đến 30 phút)
1. **Cài đặt OxideSwarm** trên máy của bạn chỉ từ phần hướng dẫn [README](../../../README.md#install) (hoặc từ thư mục USB nếu người hướng dẫn đưa cho bạn). Nếu thấy chỗ nào khó hiểu, hãy nói ra trước khi nhờ hỗ trợ.
2. **Tham gia cụm thử nghiệm tạm thời** bằng lời mời dùng một lần từ người hướng dẫn, đối chiếu mã ghép nối 6 chữ số, và chạy một worker ở chế độ cô lập (`oxideswarm worker --isolation fs`).
3. **Chạy tác vụ đầu tiên** (`oxideswarm submit --wait -- ...`) và xem kết quả trả về.
4. **Chạy thử tác vụ gửi/nhận tệp và (không bắt buộc) một công việc của chính bạn:** gửi một tệp thử nhỏ đi và nhận tệp kết quả về; nếu trước đó bạn có nêu một việc hay phải chờ trên máy, bạn có thể thử chạy một phiên bản nhỏ của việc đó.
5. **Dừng worker** an toàn bằng `Ctrl+C` và xác nhận các tiến trình tác vụ đã dừng hết.

### Máy tính của bạn được bảo vệ thế nào
- Buổi thử nghiệm dùng một **cụm tạm thời riêng**, hoàn toàn tách biệt với cụm cá nhân hay cụm vận hành khác. Nếu máy bạn đã có OxideSwarm sẵn, hãy đặt biến `OXIDESWARM_HOME` sang thư mục tạm để không chạm tới cấu hình cũ.
- Tác vụ chạy dưới `--isolation fs` (khi hệ điều hành hỗ trợ), chỉ được ghi vào thư mục tạm của chính tác vụ đó và bị chặn đọc thư mục cá nhân hay khóa SSH của bạn.
- Bạn toàn quyền dừng bất cứ lúc nào: chỉ bạn mới bật worker trên máy mình, và nhấn `Ctrl+C` sẽ dừng ngay các tác vụ đang chạy.

### Những gì được ghi nhận (và những gì tuyệt đối không thu thập)
- **Được ghi nhận (chỉ gắn nhãn ẩn danh `P1`..`P5`):** hệ điều hành và kiến trúc CPU, phiên bản phát hành đã cài, thời gian hoàn thành từng bước, chỗ nào bạn dừng lại hoặc cần trợ giúp, loại công việc bạn đã thử, và (một lần duy nhất sau 14 ngày) bạn có tự dùng lại OxideSwarm hay không.
- **Tuyệt đối không thu thập hay lưu lại:** tên thật, tên tài khoản máy (`username`), tên máy (`hostname`), địa chỉ IP, mật khẩu, chuỗi mời (`oxs1-...`), mã ghép nối, `cluster.secret`, `node.key`, nội dung tệp của bạn, video quay màn hình, hay telemetry ngầm. OxideSwarm không thu thập telemetry; khi kết nối khác mạng (`--p2p`), lưu lượng đã mã hóa giữa các máy có thể đi qua relay công cộng (n0/iroh), máy chủ này chỉ thấy địa chỉ và dung lượng, không thấy nội dung.

### Cách dừng hoặc xóa sạch sau khi thử nghiệm
- Nhấn `Ctrl+C` trong cửa sổ terminal đang chạy worker bất cứ lúc nào để ngắt kết nối và dừng mọi tác vụ đang chạy trên máy.
- Báo cho người hướng dẫn nếu bạn muốn dừng giữa chừng; không cần giải thích lý do.
- Để dọn sạch sau buổi thử nghiệm, chỉ xóa thư mục chứa tệp thực thi `oxideswarm` và thư mục tạm đã đặt làm `OXIDESWARM_HOME` cho buổi thử (không bao giờ xóa `~/.oxideswarm` đang có sẵn), đồng thời nhắc người hướng dẫn thu hồi khóa tạm của máy bạn (`oxideswarm members revoke`).
