#!/usr/bin/env bash
# ==============================================================================
# uninstall_mac_daemon.sh — Uninstaller for OxideSwarm macOS LaunchDaemon
# ==============================================================================
# Safely stops, unloads, and removes the OxideSwarm worker LaunchDaemon.
# Supports optional purging of logs, configuration, and binaries.
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

SERVICE_LABEL="com.oxideswarm.worker"
PLIST_PATH="/Library/LaunchDaemons/${SERVICE_LABEL}.plist"
BIN_INSTALL_DIR="/usr/local/bin"
DEST_BIN="${BIN_INSTALL_DIR}/rusty-grid"
DEST_ALIAS="${BIN_INSTALL_DIR}/oxideswarm"

LOG_DIR="/var/log/oxideswarm"
WORK_DIR="/var/lib/oxideswarm"
CONFIG_DIR="/etc/oxideswarm"

PURGE=false
REMOVE_BINARY=false
REMOVE_LOGS=false
DRY_RUN=false

show_help() {
    cat <<EOF
OxideSwarm macOS LaunchDaemon Uninstaller

Usage:
  sudo bash uninstall_mac_daemon.sh [OPTIONS]

Options:
  --purge            Remove all installed files (service plist, binaries, logs, config, data)
  --remove-binary    Remove worker binary and alias from /usr/local/bin
  --remove-logs      Remove log directory /var/log/oxideswarm
  --dry-run          Simulate uninstallation actions without modifying system
  -h, --help         Display this help message and exit

Examples:
  sudo bash uninstall_mac_daemon.sh
  sudo bash uninstall_mac_daemon.sh --purge
  sudo bash uninstall_mac_daemon.sh --remove-logs
EOF
}

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --purge)
            PURGE=true
            REMOVE_BINARY=true
            REMOVE_LOGS=true
            shift
            ;;
        --remove-binary)
            REMOVE_BINARY=true
            shift
            ;;
        --remove-logs)
            REMOVE_LOGS=true
            shift
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
echo -e "${BOLD}${CYAN}  OxideSwarm macOS LaunchDaemon Uninstaller${NC}"
echo -e "${BOLD}${CYAN}========================================================${NC}\n"

# 1. Check Root Privileges (if not dry-run)
if [[ "$DRY_RUN" == false && $EUID -ne 0 ]]; then
    log_error "This script requires root privileges to manage system LaunchDaemons."
    log_error "Please run with sudo: sudo bash $0 [OPTIONS]"
    exit 1
fi

if [[ "$DRY_RUN" == true ]]; then
    log_warn "Running in DRY-RUN mode. No changes will be made."
fi

# 2. Stop and Unload LaunchDaemon
log_info "Stopping and deregistering '${SERVICE_LABEL}' from launchd..."

if [[ "$DRY_RUN" == false ]]; then
    # Try bootout via system domain target first, then fallback to plist path or legacy unload
    if launchctl list "${SERVICE_LABEL}" &>/dev/null || [[ -f "${PLIST_PATH}" ]]; then
        launchctl bootout "system/${SERVICE_LABEL}" 2>/dev/null || \
        launchctl bootout system "${PLIST_PATH}" 2>/dev/null || \
        launchctl unload -w "${PLIST_PATH}" 2>/dev/null || true
        sleep 1
        log_success "Daemon deregistered and stopped."
    else
        log_info "Daemon '${SERVICE_LABEL}' was not actively registered in launchd."
    fi
else
    log_info "[DRY-RUN] Would bootout/unload '${SERVICE_LABEL}' via launchctl"
fi

# 3. Remove Property List File
if [[ -f "${PLIST_PATH}" ]]; then
    log_info "Removing property list '${PLIST_PATH}'..."
    if [[ "$DRY_RUN" == false ]]; then
        rm -f "${PLIST_PATH}"
        log_success "Removed ${PLIST_PATH}"
    else
        log_info "[DRY-RUN] Would remove ${PLIST_PATH}"
    fi
else
    log_info "Property list '${PLIST_PATH}' does not exist (already removed)."
fi

# 4. Optional Removal of Binaries
if [[ "$REMOVE_BINARY" == true ]]; then
    log_info "Removing binary installations..."
    if [[ "$DRY_RUN" == false ]]; then
        if [[ -f "${DEST_BIN}" ]]; then
            rm -f "${DEST_BIN}"
            log_success "Removed ${DEST_BIN}"
        fi
        if [[ -L "${DEST_ALIAS}" || -f "${DEST_ALIAS}" ]]; then
            rm -f "${DEST_ALIAS}"
            log_success "Removed alias ${DEST_ALIAS}"
        fi
    else
        log_info "[DRY-RUN] Would remove ${DEST_BIN} and ${DEST_ALIAS}"
    fi
fi

# 5. Optional Removal of Logs
if [[ "$REMOVE_LOGS" == true ]]; then
    log_info "Removing log directory '${LOG_DIR}'..."
    if [[ "$DRY_RUN" == false ]]; then
        if [[ -d "${LOG_DIR}" ]]; then
            rm -rf "${LOG_DIR}"
            log_success "Removed ${LOG_DIR}"
        fi
    else
        log_info "[DRY-RUN] Would remove ${LOG_DIR}"
    fi
fi

# 6. Complete Purge (Working directory and configuration)
if [[ "$PURGE" == true ]]; then
    log_info "Purging working directories and configuration..."
    if [[ "$DRY_RUN" == false ]]; then
        if [[ -d "${WORK_DIR}" ]]; then
            rm -rf "${WORK_DIR}"
            log_success "Removed ${WORK_DIR}"
        fi
        if [[ -d "${CONFIG_DIR}" ]]; then
            rm -rf "${CONFIG_DIR}"
            log_success "Removed ${CONFIG_DIR}"
        fi
    else
        log_info "[DRY-RUN] Would remove ${WORK_DIR} and ${CONFIG_DIR}"
    fi
else
    if [[ "$REMOVE_LOGS" == false ]]; then
        log_info "Logs preserved at ${LOG_DIR} (use --remove-logs or --purge to delete)."
    fi
    log_info "Working directory preserved at ${WORK_DIR} (use --purge to delete)."
fi

echo -e "\n${BOLD}${GREEN}=== Uninstallation Completed Successfully ===${NC}\n"
exit 0
