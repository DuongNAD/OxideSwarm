#!/usr/bin/env bash
# ==============================================================================
# OxideSwarm Dynamic Role Switcher for macOS (Master <-> Worker on Demand)
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

BIN="${SCRIPT_DIR}/target/release/rusty-grid"
if [[ ! -f "$BIN" ]]; then
    echo "[INFO] Compiling rusty-grid release binary..."
    cargo build --release --bin rusty-grid
fi

LOCAL_IP="$(ipconfig getifaddr en0 2>/dev/null || ipconfig getifaddr en1 2>/dev/null || echo "127.0.0.1")"

kill_existing() {
    pkill -f "rusty-grid (master|worker)" 2>/dev/null || true
    sleep 1
}

detect_role() {
    if lsof -i :8088 -sTCP:LISTEN >/dev/null 2>&1; then
        echo "MASTER (Đang lắng nghe tại :8088, Web UI :8080)"
    elif pgrep -f "rusty-grid worker" >/dev/null 2>&1; then
        echo "WORKER (Đang chạy ngầm, kết nối về Master)"
    else
        echo "STOPPED (Chưa chạy)"
    fi
}

do_master() {
    echo ""
    echo "[INFO] Đang chuyển đổi sang vai trò MASTER..."
    kill_existing

    echo "[OK] Khởi chạy OxideSwarm Master trên Mac..."
    nohup "$BIN" master --listen 0.0.0.0:8088 --web-ui-addr 0.0.0.0:8080 > master.log 2>&1 &
    sleep 1

    # Optional: also run a local worker on Mac to contribute compute
    nohup "$BIN" worker --master 127.0.0.1:8088 --name macbook-worker --simulate-gpu > mac_worker.log 2>&1 &
    sleep 1

    echo ""
    echo "=============================================================================="
    echo "[SUCCESS] MacBook Pro đã trở thành MASTER thành công!"
    echo "=============================================================================="
    echo "  - Cổng Cụm điều phối : ${LOCAL_IP}:8088"
    echo "  - Web Dashboard       : http://${LOCAL_IP}:8080"
    echo ""
    echo "  * HƯỚNG DẪN KẾT NỐI TỪ MÁY CASE (WINDOWS):"
    echo "  * HƯỚNG DẪN KẾT NỐI TỪ MÁY CASE (WINDOWS):"
    echo "    Chỉ cần chạy trên máy Case: switch_role.cmd worker auto"
    echo "    (hoặc chỉ định trực tiếp: switch_role.cmd worker ${LOCAL_IP}:8088)"
    echo "=============================================================================="
    echo ""
}

do_worker() {
    local target_master="${1:-auto}"
    echo ""
    echo "[INFO] Đang chuyển đổi sang vai trò WORKER..."
    kill_existing

    echo "[OK] Kết nối MacBook tới Master tại ${target_master} (tự động dò tìm nếu là 'auto')..."
    nohup "$BIN" worker --master "$target_master" --name macbook-worker --simulate-gpu > mac_worker.log 2>&1 &
    sleep 1

    echo ""
    echo "=============================================================================="
    echo "[SUCCESS] MacBook Pro đã trở thành WORKER (macbook-worker)!"
    echo "=============================================================================="
    echo "  - Target Master : ${target_master}"
    echo "  - Standby Portal: http://${LOCAL_IP}:8080 (Redirect HTTP 307 tới Master Web UI)"
    echo "  - Tài nguyên    : 10 Cores CPU M-Series + Virtual Matrix Engine"
    echo "  - Trạng thái    : Đang chạy ngầm (xem log tại mac_worker.log)"
    echo "=============================================================================="
    echo ""
}

do_status() {
    echo ""
    echo "=============================================================================="
    echo "                 OxideSwarm macOS Node Status"
    echo "=============================================================================="
    echo "  Host IP       : ${LOCAL_IP} ($(hostname))"
    echo "  Current Role  : [$(detect_role)]"
    echo ""
    if pgrep -f "rusty-grid" >/dev/null 2>&1; then
        echo "Active Processes:"
        ps aux | grep -E "rusty-grid (master|worker)" | grep -v grep || true
    else
        echo "No active rusty-grid processes running."
    fi
    echo "=============================================================================="
    echo ""
}

do_stop() {
    echo "[INFO] Đang tắt tất cả tiến trình OxideSwarm trên Mac..."
    kill_existing
    echo "[OK] Tất cả tiến trình đã được dừng sạch sẽ."
}

# Check argument
CMD="${1:-}"
case "$CMD" in
    master)
        do_master
        ;;
    worker)
        do_worker "${2:-auto}"
        ;;
    status)
        do_status
        ;;
    stop)
        do_stop
        ;;
    *)
        # Interactive Menu
        clear
        echo "=============================================================================="
        echo "              OxideSwarm Role Switcher (macOS Node Coordinator)"
        echo "=============================================================================="
        echo "  Host IP       : ${LOCAL_IP} ($(hostname))"
        echo "  Current Role  : [$(detect_role)]"
        echo "=============================================================================="
        echo ""
        echo "  [1] Chuyển Mac thành MASTER (Điều phối cụm + Web Dashboard)"
        echo "  [2] Chuyển Mac thành WORKER (Tự động quét UDP 'auto' hoặc kết nối tới Master)"
        echo "  [3] Kiểm tra trạng thái hiện tại"
        echo "  [4] Dừng tất cả tiến trình (Stop All)"
        echo "  [5] Thoát"
        echo ""
        read -r -p "Nhập lựa chọn [1-5]: " CHOICE
        case "$CHOICE" in
            1) do_master ;;
            2)
                read -r -p "Nhập địa chỉ Master [Enter để tự động quét 'auto' hoặc 192.168.1.123:8088]: " CUSTOM_IP
                do_worker "${CUSTOM_IP:-auto}"
                ;;
            3) do_status ;;
            4) do_stop ;;
            5) exit 0 ;;
            *) echo "Lựa chọn không hợp lệ." ;;
        esac
        ;;
esac
