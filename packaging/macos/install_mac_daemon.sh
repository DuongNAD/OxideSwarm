#!/usr/bin/env bash
# ==============================================================================
# install_mac_daemon.sh — Automated Installer for OxideSwarm macOS LaunchDaemon
# ==============================================================================
# Installs and configures the OxideSwarm worker node as a native macOS
# system-level LaunchDaemon that starts silently on boot and restarts on crash.
# ==============================================================================

set -euo pipefail

# ANSI color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC} $*"; }
log_success() { echo -e "${GREEN}[OK]${NC} $*"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# Configuration defaults
SERVICE_LABEL="com.oxideswarm.worker"
PLIST_PATH="/Library/LaunchDaemons/${SERVICE_LABEL}.plist"
BIN_INSTALL_DIR="/usr/local/bin"
BIN_NAME="rusty-grid"
ALIAS_NAME="oxideswarm"
DEST_BIN="${BIN_INSTALL_DIR}/${BIN_NAME}"
DEST_ALIAS="${BIN_INSTALL_DIR}/${ALIAS_NAME}"

LOG_DIR="/var/log/oxideswarm"
WORK_DIR="/var/lib/oxideswarm"
CONFIG_DIR="/etc/oxideswarm"

MASTER_ADDR="127.0.0.1:8080"
SOURCE_BIN=""
P2P_TICKET=""
WORKER_NAME=""
CONFIG_FILE=""
DRY_RUN=false

show_help() {
    cat <<EOF
OxideSwarm macOS LaunchDaemon Installer

Usage:
  sudo bash install_mac_daemon.sh [OPTIONS]

Options:
  --master <ADDR>        Master node TCP address (default: 127.0.0.1:8080)
  --bin <PATH>           Path to compiled rusty-grid binary
  --p2p-ticket <TICKET>  P2P connection ticket string for iroh NAT traversal
  --name <NAME>          Human-readable worker name override
  --config <PATH>        Path to worker.toml configuration file
  --bin-dir <DIR>        Binary install directory (default: /usr/local/bin)
  --log-dir <DIR>        Log output directory (default: /var/log/oxideswarm)
  --work-dir <DIR>       Working/sandbox directory (default: /var/lib/oxideswarm)
  --dry-run              Simulate installation steps without modifying system
  -h, --help             Display this help message and exit

Examples:
  sudo bash install_mac_daemon.sh --master 192.168.1.50:8080
  sudo bash install_mac_daemon.sh --bin ./target/release/rusty-grid
  sudo bash install_mac_daemon.sh --p2p-ticket "iroh-ticket-xyz" --name "mac-node-01"
EOF
}

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --master)
            MASTER_ADDR="${2:-}"
            shift 2
            ;;
        --bin)
            SOURCE_BIN="${2:-}"
            shift 2
            ;;
        --p2p-ticket)
            P2P_TICKET="${2:-}"
            shift 2
            ;;
        --name)
            WORKER_NAME="${2:-}"
            shift 2
            ;;
        --config)
            CONFIG_FILE="${2:-}"
            shift 2
            ;;
        --bin-dir)
            BIN_INSTALL_DIR="${2:-}"
            DEST_BIN="${BIN_INSTALL_DIR}/${BIN_NAME}"
            DEST_ALIAS="${BIN_INSTALL_DIR}/${ALIAS_NAME}"
            shift 2
            ;;
        --log-dir)
            LOG_DIR="${2:-}"
            shift 2
            ;;
        --work-dir)
            WORK_DIR="${2:-}"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            log_error "Unknown argument: $1"
            show_help
            exit 1
            ;;
    esac
done

echo -e "\n${BOLD}${CYAN}========================================================${NC}"
echo -e "${BOLD}${CYAN}   OxideSwarm macOS LaunchDaemon Installer (R2)${NC}"
echo -e "${BOLD}${CYAN}========================================================${NC}\n"

# 1. Root / Sudo Privilege Verification
if [[ "$DRY_RUN" == false && $EUID -ne 0 ]]; then
    log_error "This script requires root privileges to install system LaunchDaemons."
    log_error "Please run with sudo: sudo bash $0 [OPTIONS]"
    exit 1
fi

if [[ "$DRY_RUN" == true ]]; then
    log_warn "Running in DRY-RUN mode. No changes will be applied to the system."
fi

# 2. Binary Resolution and Deployment
if [[ -z "$SOURCE_BIN" ]]; then
    # Search common build locations
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

    CANDIDATES=(
        "${WORKSPACE_ROOT}/target/release/rusty-grid"
        "${WORKSPACE_ROOT}/target/debug/rusty-grid"
        "./target/release/rusty-grid"
        "./rusty-grid"
        "${DEST_BIN}"
    )

    for cand in "${CANDIDATES[@]}"; do
        if [[ -f "$cand" ]]; then
            SOURCE_BIN="$cand"
            break
        fi
    done
fi

if [[ -n "$SOURCE_BIN" && -f "$SOURCE_BIN" ]]; then
    log_info "Found worker binary at: ${SOURCE_BIN}"
    if [[ "$DRY_RUN" == false ]]; then
        mkdir -p "${BIN_INSTALL_DIR}"
        cp -f "${SOURCE_BIN}" "${DEST_BIN}"
        chown root:wheel "${DEST_BIN}"
        chmod 755 "${DEST_BIN}"
        
        # Create brand alias symlink: oxideswarm -> rusty-grid
        ln -sf "${DEST_BIN}" "${DEST_ALIAS}"
        log_success "Installed binary to ${DEST_BIN} (0755) and symlinked ${DEST_ALIAS}"
    else
        log_info "[DRY-RUN] Would install ${SOURCE_BIN} -> ${DEST_BIN} (0755) and symlink ${DEST_ALIAS}"
    fi
elif [[ -x "${DEST_BIN}" ]]; then
    log_info "Using existing executable at ${DEST_BIN}"
else
    log_warn "No pre-built binary found at candidate locations."
    log_warn "Ensure '${DEST_BIN}' is placed and executable before daemon starts."
fi

# 3. Directory Structure and Permissions Setup
log_info "Configuring system directories..."
if [[ "$DRY_RUN" == false ]]; then
    mkdir -p "${LOG_DIR}" "${WORK_DIR}" "${CONFIG_DIR}"
    chown root:wheel "${LOG_DIR}" "${WORK_DIR}" "${CONFIG_DIR}"
    chmod 755 "${LOG_DIR}" "${WORK_DIR}" "${CONFIG_DIR}"
    log_success "Directories initialized (${LOG_DIR}, ${WORK_DIR}, ${CONFIG_DIR})"
else
    log_info "[DRY-RUN] Would create and chown root:wheel: ${LOG_DIR}, ${WORK_DIR}, ${CONFIG_DIR}"
fi

# 4. Stop and Unload Existing Active Daemon (if registered)
if [[ "$DRY_RUN" == false ]]; then
    if launchctl list "${SERVICE_LABEL}" &>/dev/null; then
        log_info "Active service '${SERVICE_LABEL}' detected; unloading prior instance..."
        launchctl bootout "system/${SERVICE_LABEL}" 2>/dev/null || \
        launchctl bootout system "${PLIST_PATH}" 2>/dev/null || \
        launchctl unload -w "${PLIST_PATH}" 2>/dev/null || true
        sleep 1
        log_success "Prior service instance unloaded."
    fi
fi

# 5. Generate and Deploy LaunchDaemon Property List (.plist)
log_info "Generating LaunchDaemon property list at ${PLIST_PATH}..."

# Build ProgramArguments XML array elements
PROG_ARGS_XML="        <string>${DEST_BIN}</string>\n        <string>worker</string>"

if [[ -n "$CONFIG_FILE" ]]; then
    PROG_ARGS_XML="${PROG_ARGS_XML}\n        <string>--config</string>\n        <string>${CONFIG_FILE}</string>"
else
    PROG_ARGS_XML="${PROG_ARGS_XML}\n        <string>--master</string>\n        <string>${MASTER_ADDR}</string>"
fi

if [[ -n "$P2P_TICKET" ]]; then
    PROG_ARGS_XML="${PROG_ARGS_XML}\n        <string>--p2p-ticket</string>\n        <string>${P2P_TICKET}</string>"
fi

if [[ -n "$WORKER_NAME" ]]; then
    PROG_ARGS_XML="${PROG_ARGS_XML}\n        <string>--name</string>\n        <string>${WORKER_NAME}</string>"
fi

PLIST_CONTENT="<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">
<plist version=\"1.0\">
<dict>
    <!-- Daemon Identification -->
    <key>Label</key>
    <string>${SERVICE_LABEL}</string>

    <!-- Executable Invocation -->
    <key>ProgramArguments</key>
    <array>
$(echo -e "${PROG_ARGS_XML}")
    </array>

    <!-- Automatic Startup on Boot/Load -->
    <key>RunAtLoad</key>
    <true/>

    <!-- Supervision & Crash Recovery -->
    <key>KeepAlive</key>
    <true/>

    <!-- Crash Loop Throttling (Seconds) -->
    <key>ThrottleInterval</key>
    <integer>5</integer>

    <!-- Headless Standard I/O Log Redirection -->
    <key>StandardOutPath</key>
    <string>${LOG_DIR}/worker.log</string>
    <key>StandardErrorPath</key>
    <string>${LOG_DIR}/worker.err.log</string>

    <!-- Working Directory Context -->
    <key>WorkingDirectory</key>
    <string>${WORK_DIR}</string>

    <!-- Runtime Environment -->
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
        <key>RUST_LOG</key>
        <string>info</string>
    </dict>

    <!-- High Performance Grid Resource Limits -->
    <key>SoftResourceLimits</key>
    <dict>
        <key>NumberOfFiles</key>
        <integer>65536</integer>
    </dict>
    <key>HardResourceLimits</key>
    <dict>
        <key>NumberOfFiles</key>
        <integer>65536</integer>
    </dict>

    <!-- Scheduling and Process Tree Management -->
    <key>ProcessType</key>
    <string>Standard</string>
    <key>AbandonProcessGroup</key>
    <false/>
</dict>
</plist>"

if [[ "$DRY_RUN" == false ]]; then
    echo "${PLIST_CONTENT}" > "${PLIST_PATH}"
    chown root:wheel "${PLIST_PATH}"
    chmod 644 "${PLIST_PATH}"
    log_success "Deployed property list with strict permissions (root:wheel, 0644)"

    # Lint with plutil to ensure launchd parser compatibility
    log_info "Validating plist syntax with plutil..."
    if plutil -lint "${PLIST_PATH}"; then
        log_success "Property list syntax verified."
    else
        log_error "Property list lint failed! Aborting."
        exit 1
    fi
else
    log_info "[DRY-RUN] Generated Plist Content:\n${PLIST_CONTENT}"
fi

# 6. Register and Bootstrap with launchctl
if [[ "$DRY_RUN" == false ]]; then
    log_info "Registering LaunchDaemon via launchctl..."
    
    BOOTSTRAP_SUCCESS=false
    if launchctl bootstrap system "${PLIST_PATH}" 2>/dev/null; then
        BOOTSTRAP_SUCCESS=true
        log_success "Bootstrapped service via modern 'launchctl bootstrap system'."
    else
        log_warn "Bootstrap returned non-zero (or daemon already active); trying 'launchctl load -w'..."
        if launchctl load -w "${PLIST_PATH}" 2>/dev/null; then
            BOOTSTRAP_SUCCESS=true
            log_success "Loaded service via fallback 'launchctl load -w'."
        fi
    fi

    if [[ "$BOOTSTRAP_SUCCESS" == false ]]; then
        log_error "Failed to register daemon in launchd."
        exit 1
    fi

    # 7. Verification & Status Report
    sleep 1
    if launchctl list "${SERVICE_LABEL}" &>/dev/null; then
        INFO=$(launchctl list "${SERVICE_LABEL}" 2>/dev/null || true)
        PID=$(echo "$INFO" | awk -F'=' '/"PID"/ {print $2}' | tr -d ' ;' || echo "")
        
        if [[ -n "$PID" && "$PID" != "-" ]]; then
            log_success "OxideSwarm worker is actively running in background (PID: ${PID})."
        else
            log_success "OxideSwarm worker daemon registered in launchd (State: Active/Supervised)."
        fi
    else
        log_warn "Daemon registered. Use 'launchctl print system/${SERVICE_LABEL}' to inspect status."
    fi

    echo -e "\n${BOLD}${GREEN}=== macOS LaunchDaemon Installation Completed Successfully ===${NC}"
    echo -e "Service Label : ${CYAN}${SERVICE_LABEL}${NC}"
    echo -e "Plist Path    : ${CYAN}${PLIST_PATH}${NC}"
    echo -e "Log Files     : ${CYAN}${LOG_DIR}/worker.log${NC} & ${CYAN}${LOG_DIR}/worker.err.log${NC}"
    echo -e "Binary Path   : ${CYAN}${DEST_BIN}${NC} (alias: ${CYAN}${DEST_ALIAS}${NC})"
    echo -e "Status Check  : ${BOLD}sudo launchctl print system/${SERVICE_LABEL}${NC}\n"
else
    echo -e "\n${BOLD}${GREEN}=== Dry-Run Completed Successfully (No changes made) ===${NC}\n"
fi
