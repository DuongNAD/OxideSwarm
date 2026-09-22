#!/data/data/com.termux/files/usr/bin/bash
# ==============================================================================
# install_termux_daemon.sh — One-Click Setup Script for OxideSwarm on Termux
#
# Configures:
# 1. Environment packages (termux-api, coreutils, procps, curl)
# 2. Worker binary installation & permissions ($PREFIX/bin/rusty-grid)
# 3. Termux:Boot auto-start hook (~/.termux/boot/start-oxideswarm)
# 4. Supervisor daemon script (start_worker.sh)
# 5. Default configuration file (~/.config/oxideswarm/rusty-grid.toml)
# 6. Battery optimization disablement & Phantom Process Killer guidance
# ==============================================================================

set -euo pipefail

# ANSI color codes
RED="$(printf '\033[0;31m')"
GREEN="$(printf '\033[0;32m')"
YELLOW="$(printf '\033[1;33m')"
BLUE="$(printf '\033[0;34m')"
CYAN="$(printf '\033[0;36m')"
BOLD="$(printf '\033[1m')"
NC="$(printf '\033[0m')"

log_info() { echo -e "${BLUE}[INFO]${NC} $*"; }
log_ok()   { echo -e "${GREEN}[OK]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_err()  { echo -e "${RED}[ERROR]${NC} $*" >&2; }

echo -e "${CYAN}${BOLD}"
echo "=================================================================="
echo "          OxideSwarm Termux Worker Setup & Installer              "
echo "=================================================================="
echo -e "${NC}"

# 1. Validate Termux Environment
if [ -z "${PREFIX:-}" ] && [ ! -d "/data/data/com.termux" ]; then
    log_err "This script is designed to run exclusively inside Termux on Android."
    log_err "If you are running from host macOS or Linux, push this script and the binary to Termux first."
    exit 1
fi

PREFIX="${PREFIX:-/data/data/com.termux/files/usr}"
HOME="${HOME:-/data/data/com.termux/files/home}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log_ok "Termux environment detected (PREFIX: ${PREFIX})"

# 2. Install Required Packages
log_info "Updating package lists and installing required Termux packages..."
if command -v pkg >/dev/null 2>&1; then
    pkg update -y || true
    pkg install -y termux-api coreutils procps curl jq || {
        log_warn "Some packages failed to install via 'pkg'; continuing..."
    }
else
    log_warn "'pkg' command not found; skipping package installation."
fi

# 3. Locate or Install rusty-grid Binary
log_info "Installing OxideSwarm worker binary (rusty-grid)..."
DEST_BIN="${PREFIX}/bin/rusty-grid"
SOURCE_BIN=""

# Check possible source locations
CANDIDATES=(
    "${1:-}"
    "${SCRIPT_DIR}/rusty-grid"
    "${SCRIPT_DIR}/../../../target/aarch64-linux-android/release/rusty-grid"
    "/sdcard/Download/rusty-grid"
    "${HOME}/storage/downloads/rusty-grid"
    "${HOME}/rusty-grid"
)

for cand in "${CANDIDATES[@]}"; do
    if [ -n "$cand" ] && [ -f "$cand" ]; then
        SOURCE_BIN="$cand"
        break
    fi
done

if [ -n "$SOURCE_BIN" ]; then
    log_info "Copying binary from ${SOURCE_BIN} to ${DEST_BIN}..."
    cp -f "${SOURCE_BIN}" "${DEST_BIN}"
    chmod 755 "${DEST_BIN}"
    log_ok "Installed worker binary to ${DEST_BIN}"
elif [ -f "${DEST_BIN}" ]; then
    chmod 755 "${DEST_BIN}"
    log_ok "Existing worker binary found at ${DEST_BIN}"
else
    log_warn "Worker binary 'rusty-grid' was not found automatically in local paths."
    echo -e "Please copy the cross-compiled 'rusty-grid' binary to Termux:"
    echo -e "  From host PC: ${BOLD}adb push target/aarch64-linux-android/release/rusty-grid /sdcard/Download/${NC}"
    echo -e "  Inside Termux: ${BOLD}cp /sdcard/Download/rusty-grid \$PREFIX/bin/ && chmod +x \$PREFIX/bin/rusty-grid${NC}"
    echo ""
    read -rp "Enter full path to rusty-grid binary if available (or press Enter to skip): " MANUAL_PATH
    if [ -n "$MANUAL_PATH" ] && [ -f "$MANUAL_PATH" ]; then
        cp -f "$MANUAL_PATH" "${DEST_BIN}"
        chmod 755 "${DEST_BIN}"
        log_ok "Installed worker binary to ${DEST_BIN}"
    fi
fi

# Create symlink for brand name 'oxideswarm'
if [ -f "${DEST_BIN}" ]; then
    ln -sf "${DEST_BIN}" "${PREFIX}/bin/oxideswarm"
    log_ok "Created symlink: ${PREFIX}/bin/oxideswarm -> ${DEST_BIN}"
fi

# 4. Install Supervisor Script (start_worker.sh)
log_info "Installing start_worker.sh supervisor script..."
mkdir -p "${HOME}/bin"
DEST_SUPERVISOR="${PREFIX}/bin/start_worker.sh"
ALT_SUPERVISOR="${HOME}/bin/start_worker.sh"

if [ -f "${SCRIPT_DIR}/start_worker.sh" ]; then
    cp -f "${SCRIPT_DIR}/start_worker.sh" "${DEST_SUPERVISOR}"
    cp -f "${SCRIPT_DIR}/start_worker.sh" "${ALT_SUPERVISOR}"
    chmod 755 "${DEST_SUPERVISOR}" "${ALT_SUPERVISOR}"
    log_ok "Installed supervisor script to ${DEST_SUPERVISOR}"
else
    log_err "start_worker.sh not found in script directory: ${SCRIPT_DIR}"
fi

# 5. Configure Default Configuration File
CONFIG_DIR="${HOME}/.config/oxideswarm"
CONFIG_FILE="${CONFIG_DIR}/rusty-grid.toml"
mkdir -p "${CONFIG_DIR}"

if [ ! -f "${CONFIG_FILE}" ]; then
    log_info "Generating default configuration at ${CONFIG_FILE}..."
    cat > "${CONFIG_FILE}" <<EOF
# OxideSwarm Worker Configuration for Android Termux
[worker]
master = "127.0.0.1:8080"
name = "android-termux-$(getprop ro.product.model 2>/dev/null | tr ' ' '_' | tr -cd '[:alnum:]_-' || echo node)"
heartbeat_interval_secs = 3
max_concurrency = 4
keep_sandboxes = false

[worker.hardware]
# Leave commented out to auto-detect hardware CPU and RAM
# cores = 8
# ram_mb = 6144
simulate_gpu = false
EOF
    log_ok "Generated configuration file: ${CONFIG_FILE}"
else
    log_ok "Existing configuration preserved: ${CONFIG_FILE}"
fi

# 6. Configure Termux:Boot Auto-Start Hook
log_info "Configuring Termux:Boot auto-start hook..."
TERMUX_BOOT_DIR="${HOME}/.termux/boot"
mkdir -p "${TERMUX_BOOT_DIR}"
BOOT_HOOK="${TERMUX_BOOT_DIR}/start-oxideswarm"

if [ -f "${SCRIPT_DIR}/start-oxideswarm" ]; then
    cp -f "${SCRIPT_DIR}/start-oxideswarm" "${BOOT_HOOK}"
    chmod 755 "${BOOT_HOOK}"
    log_ok "Installed Termux:Boot script to ${BOOT_HOOK}"
else
    # Generate fallback hook directly
    cat > "${BOOT_HOOK}" <<'EOF'
#!/data/data/com.termux/files/usr/bin/bash
termux-wake-lock
sleep 10
if [ -x "$PREFIX/bin/start_worker.sh" ]; then
    "$PREFIX/bin/start_worker.sh" --daemon
fi
EOF
    chmod 755 "${BOOT_HOOK}"
    log_ok "Generated Termux:Boot hook at ${BOOT_HOOK}"
fi

# 7. Test termux-wake-lock
log_info "Testing termux-wake-lock..."
if command -v termux-wake-lock >/dev/null 2>&1; then
    termux-wake-lock && termux-wake-unlock || true
    log_ok "termux-wake-lock is verified functional."
else
    log_warn "termux-wake-lock is not yet installed or permission not granted."
    log_info "Please install 'Termux:API' application from F-Droid."
fi

# 8. Prompt Battery Optimization Settings
echo ""
echo -e "${YELLOW}${BOLD}CRITICAL STEP: Android Battery Optimization Exemption${NC}"
echo "To prevent Android from killing or freezing the worker when the screen turns off,"
echo "you must grant Termux and Termux:Boot exemption from battery optimizations."
echo ""

if command -v am >/dev/null 2>&1; then
    echo "Opening Android Battery Optimization settings dialog..."
    am start -a android.settings.IGNORE_BATTERY_OPTIMIZATION_SETTINGS >/dev/null 2>&1 || true
fi

echo -e "Navigate to: ${BOLD}Settings -> Apps -> Termux -> Battery -> Unrestricted${NC}"
echo -e "Also set:    ${BOLD}Settings -> Apps -> Termux:Boot -> Battery -> Unrestricted${NC}"

# 9. Android 12+ Phantom Process Killer Notice
echo ""
echo -e "${CYAN}${BOLD}NOTE: Android 12+ Phantom Process Killer (PPK)${NC}"
echo "On Android 12, 13, 14, and 15, child processes consuming high CPU may receive SIGKILL."
echo "To permanently disable the Phantom Process Killer, connect your phone to a PC via USB and run:"
echo -e "  ${BOLD}adb shell \"/system/bin/device_config put activity_manager max_phantom_processes 2147483647\"${NC}"
echo ""

# 10. Verification of Installed Binary
if [ -x "${DEST_BIN}" ]; then
    echo -e "${GREEN}${BOLD}Verification:${NC}"
    "${DEST_BIN}" --help | head -n 3 || true
    log_ok "OxideSwarm worker binary is verified executable!"
fi

echo -e "${GREEN}${BOLD}"
echo "=================================================================="
echo "          Installation & Setup Completed Successfully!            "
echo "=================================================================="
echo -e "${NC}"
echo "Management Commands:"
echo "  Start worker in foreground:  start_worker.sh --foreground"
echo "  Start worker in background:  start_worker.sh --daemon"
echo "  Check worker status & PID:   start_worker.sh --status"
echo "  View live logs:              start_worker.sh --logs"
echo "  Stop background worker:      start_worker.sh --stop"
echo "  Edit configuration:          nano ${CONFIG_FILE}"
echo ""
echo "Termux:Boot will now automatically start the worker whenever your phone boots."
echo "=================================================================="
exit 0
