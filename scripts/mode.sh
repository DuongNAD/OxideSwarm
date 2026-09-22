#!/usr/bin/env bash
# ==============================================================================
# scripts/mode.sh — OxideSwarm Workflow Modes & Automation Presets Runner
#
# Provides 4 productivity modes for developing, verifying, and testing OxideSwarm:
#   1. test     — Unit testing, Clippy linting, Code format, Flaky test detection
#   2. dev      — Fast incremental check, Local Dev Cluster (Master + 2 Workers), Health status
#   3. doc      — Markdown spec verification, Architecture export, Rustdoc build
#   4. research — Hardware resource profiling, Micro-benchmarks, Distributed workload
#
# Usage:
#   ./scripts/mode.sh [test|dev|doc|research] [action] [options]
#   ./scripts/mode.sh                   # Launches interactive terminal menu
# ==============================================================================

set -euo pipefail

# ANSI color codes
CLR_RESET='\033[0m'
CLR_BOLD='\033[1m'
CLR_DIM='\033[2m'
CLR_CYAN='\033[0;36m'
CLR_GREEN='\033[0;32m'
CLR_YELLOW='\033[1;33m'
CLR_RED='\033[0;31m'
CLR_MAGENTA='\033[0;35m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

IS_JSON=false
for arg in "$@"; do
    if [[ "$arg" == "--json" || "$arg" == "-j" ]]; then
        IS_JSON=true
        break
    fi
done

log_header() {
    if [[ "${IS_JSON}" == "true" ]]; then return 0; fi
    echo -e "\n${CLR_BOLD}${CLR_CYAN}================================================================================${CLR_RESET}"
    echo -e "${CLR_BOLD}  $*${CLR_RESET}"
    echo -e "${CLR_BOLD}${CLR_CYAN}================================================================================${CLR_RESET}"
}

log_step() {
    if [[ "${IS_JSON}" == "true" ]]; then return 0; fi
    echo -e "  ${CLR_BOLD}${CLR_CYAN}●${CLR_RESET} $*"
}
log_pass() {
    if [[ "${IS_JSON}" == "true" ]]; then return 0; fi
    echo -e "  ${CLR_BOLD}${CLR_GREEN}[PASS]${CLR_RESET} $*"
}
log_fail() { echo -e "  ${CLR_BOLD}${CLR_RED}[FAIL]${CLR_RESET} $*" >&2; }
log_info() {
    if [[ "${IS_JSON}" == "true" ]]; then return 0; fi
    echo -e "  ${CLR_DIM}[INFO] $*${CLR_RESET}"
}

run_ox_mode_bin() {
    if [[ -x "${ROOT_DIR}/target/debug/ox-mode" ]]; then
        "${ROOT_DIR}/target/debug/ox-mode" "$@"
    else
        cargo run --quiet --bin ox-mode -- "$@"
    fi
}

# ==============================================================================
# MODE 1: TESTING & QUALITY ASSURANCE
# ==============================================================================
mode_test() {
    local action="quick"
    if [[ $# -gt 0 && ! "$1" =~ ^- ]]; then
        action="$1"
        shift
    fi

    log_header "🧪 CHẾ ĐỘ TEST & KIỂM TRA CHẤT LƯỢNG (Action: ${action})"

    case "${action}" in
        quick)
            log_step "Đang chạy Quick Unit Tests (cargo test --workspace --lib)..."
            run_ox_mode_bin test quick "$@"
            ;;
        lint)
            log_step "Đang chạy Lints & Code Formatting (cargo clippy & fmt)..."
            run_ox_mode_bin test lint "$@"
            ;;
        flaky)
            if [[ $# -gt 0 && "$1" =~ ^[0-9]+$ ]]; then
                local iters="$1"
                shift
                log_step "Đang chạy Flaky Test Detector (${iters} iterations)..."
                run_ox_mode_bin test flaky --iterations "${iters}" "$@"
            else
                log_step "Đang chạy Flaky Test Detector..."
                run_ox_mode_bin test flaky "$@"
            fi
            ;;
        full)
            log_step "Đang chạy Full Workspace Test Suite (unit + integration)..."
            run_ox_mode_bin test full "$@"
            ;;
        *)
            echo -e "${CLR_YELLOW}Unknown test action '${action}'. Available: quick, lint, flaky, full.${CLR_RESET}"
            return 1
            ;;
    esac
}

# ==============================================================================
# MODE 2: DEVELOPMENT & LOCAL CLUSTER
# ==============================================================================
mode_dev() {
    local action="fast"
    if [[ $# -gt 0 && ! "$1" =~ ^- ]]; then
        action="$1"
        shift
    fi

    log_header "⚡ CHẾ ĐỘ PHÁT TRIỂN & CỤM DEV LOCAL (Action: ${action})"

    case "${action}" in
        fast)
            log_step "Đang chạy Fast Incremental Check (cargo check --workspace)..."
            run_ox_mode_bin dev fast "$@"
            ;;
        watch|reload)
            log_step "Khởi chạy Live-Reload Watch Loop (tự động re-check khi thay đổi mã)..."
            run_ox_mode_bin dev watch "$@"
            ;;
        cluster)
            log_step "Khởi chạy cụm Local Dev Cluster (Master + 2 Workers)..."
            run_ox_mode_bin dev cluster "$@"
            ;;
        status)
            log_step "Kiểm tra sức khỏe cụm (Cluster Health Status)..."
            run_ox_mode_bin dev status "$@"
            ;;
        stop)
            log_step "Dừng cụm Local Dev Cluster..."
            run_ox_mode_bin dev stop "$@"
            ;;
        *)
            echo -e "${CLR_YELLOW}Unknown dev action '${action}'. Available: fast, watch, cluster, status, stop.${CLR_RESET}"
            return 1
            ;;
    esac
}

# ==============================================================================
# MODE 3: DOCUMENTATION & SPECIFICATION
# ==============================================================================
mode_doc() {
    local action="verify"
    if [[ $# -gt 0 && ! "$1" =~ ^- ]]; then
        action="$1"
        shift
    fi

    log_header "📚 CHẾ ĐỘ VIẾT TÀI LIỆU & ĐẶC TẢ KIẾN TRÚC (Action: ${action})"

    case "${action}" in
        verify)
            log_step "Kiểm tra tính toàn vẹn tài liệu Markdown & Specs..."
            run_ox_mode_bin doc verify "$@"
            ;;
        export|update)
            log_step "Xuất / Cập nhật Đặc tả Kiến trúc & Giao thức thống nhất..."
            run_ox_mode_bin doc export "$@"
            ;;
        build)
            log_step "Biên dịch tài liệu mã nguồn (cargo doc --workspace --no-deps)..."
            run_ox_mode_bin doc build "$@"
            ;;
        *)
            echo -e "${CLR_YELLOW}Unknown doc action '${action}'. Available: verify, export, update, build.${CLR_RESET}"
            return 1
            ;;
    esac
}

# ==============================================================================
# MODE 4: RESEARCH & PERFORMANCE BENCHMARKING
# ==============================================================================
mode_research() {
    local action="profile"
    if [[ $# -gt 0 && ! "$1" =~ ^- ]]; then
        action="$1"
        shift
    fi

    log_header "🔬 CHẾ ĐỘ NGHIÊN CỨU & ĐO KIỂM HIỆU NĂNG (Action: ${action})"

    case "${action}" in
        profile)
            log_step "Đo lường cấu hình phần cứng và tài nguyên hệ thống..."
            run_ox_mode_bin research profile "$@"
            ;;
        bench)
            log_step "Chạy Micro-Benchmarks (Serde, Queue & Codec Throughput)..."
            run_ox_mode_bin research bench "$@"
            ;;
        distributed)
            log_step "Chạy Distributed Scheduling Simulation Benchmark..."
            run_ox_mode_bin research distributed "$@"
            ;;
        report)
            log_step "Tạo Báo cáo Nghiên cứu hoàn chỉnh (target/research_report.json)..."
            run_ox_mode_bin research report "$@"
            ;;
        *)
            echo -e "${CLR_YELLOW}Unknown research action '${action}'. Available: profile, bench, distributed, report.${CLR_RESET}"
            return 1
            ;;
    esac
}

# ==============================================================================
# INTERACTIVE TERMINAL MENU
# ==============================================================================
interactive_menu() {
    run_ox_mode_bin -i
}

# ==============================================================================
# ENTRY POINT
# ==============================================================================
if [[ $# -eq 0 ]]; then
    interactive_menu
    exit 0
fi

MODE="$1"
shift || true

case "${MODE}" in
    test|tests|check-tests|verify|qa)
        mode_test "$@"
        ;;
    dev|develop|development|cluster)
        mode_dev "$@"
        ;;
    doc|docs|document|documentation|spec|specs)
        mode_doc "$@"
        ;;
    research|bench|benchmark|benchmarks|profile|perf)
        mode_research "$@"
        ;;
    -i|--interactive)
        interactive_menu
        ;;
    -h|--help|help)
        log_header "OxideSwarm Workflow Modes Helper (scripts/mode.sh)"
        echo -e "Usage:"
        echo -e "  ./scripts/mode.sh                     (Interactive TUI menu)"
        echo -e "  ./scripts/mode.sh test [quick|lint|flaky|full]"
        echo -e "  ./scripts/mode.sh dev [fast|cluster|status|stop]"
        echo -e "  ./scripts/mode.sh doc [verify|export|build]"
        echo -e "  ./scripts/mode.sh research [profile|bench|distributed|report]"
        echo -e ""
        echo -e "Or use the native Rust CLI binary:"
        echo -e "  ox-mode --help"
        echo -e "  rusty-grid mode --help"
        exit 0
        ;;
    *)
        echo -e "${CLR_RED}Lỗi: Chế độ không hợp lệ '${MODE}'.${CLR_RESET}"
        echo -e "Các chế độ hỗ trợ: ${CLR_BOLD}test, dev, doc, research${CLR_RESET} (hoặc chạy không tham số để mở Menu)."
        exit 1
        ;;
esac
