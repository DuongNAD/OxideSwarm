#!/usr/bin/env bash
# ==============================================================================
# install_mac_daemon.sh — Automated Installer for OxideSwarm macOS LaunchDaemon
# ==============================================================================
# Installs and configures the OxideSwarm worker node as a native macOS
# system-level LaunchDaemon that starts silently on boot and restarts on crash.
#
# SECURITY MODEL (see docs/SECURITY.md)
#  * The worker executes whatever tasks the master sends, with the rights of the
#    account launchd runs it as. This installer therefore runs it as an
#    unprivileged account (the user that invoked sudo, or --user NAME) and NEVER
#    as root unless you pass --run-as-root explicitly.
#  * The cluster secret (proof that the master is genuine) and any P2P ticket are
#    stored in /etc/oxideswarm/ with mode 600 and owned by that account. They are
#    never written to the world-readable plist or to the process arguments.
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

MASTER_ADDR=""
SOURCE_BIN=""
P2P_TICKET=""
P2P_TICKET_FILE=""
SECRET_SRC=""
INSECURE=false
RUN_USER_ARG=""
RUN_AS_ROOT=false
ASSUME_YES=false
WORKER_NAME=""
CONFIG_FILE=""
DRY_RUN=false

show_help() {
    cat <<EOF
OxideSwarm macOS LaunchDaemon Installer

Usage:
  sudo bash install_mac_daemon.sh [OPTIONS]

Connection (one of --master / --p2p-ticket-file is required):
  --master <ADDR>            Master CLUSTER address HOST:8088 (not the dashboard port 8080)
  --p2p-ticket-file <FILE>   File holding the P2P ticket from a master started with --p2p
  --p2p-ticket <TICKET>      Ticket on the command line (visible in 'ps'; prefer the file)
  --secret-file <FILE>       Cluster secret copied from the master (default:
                             \$OXIDESWARM_SECRET_FILE, then <service user's home>/.oxideswarm/cluster.secret)
  --insecure                 Run WITHOUT a cluster secret (the worker then obeys any host
                             that answers as the master; not recommended)

Identity (every task runs as this account):
  --user <NAME>              Account that runs the worker (default: the user who ran sudo;
                             asked interactively if that is ambiguous)
  --run-as-root              Run as root. Any task the master dispatches then controls this Mac.
  --yes                      Do not ask for confirmation

Other:
  --name <NAME>              Human-readable worker name override
  --config <PATH>            Use this worker config as-is instead of generating one
  --bin <PATH>               Path to compiled rusty-grid binary
  --bin-dir <DIR>            Binary install directory (default: /usr/local/bin)
  --log-dir <DIR>            Log output directory (default: /var/log/oxideswarm)
  --work-dir <DIR>           Working/sandbox directory (default: /var/lib/oxideswarm)
  --dry-run                  Show what would be installed without modifying the system
  -h, --help                 Display this help message and exit

Examples:
  sudo bash install_mac_daemon.sh --master 192.168.1.50:8088 --secret-file ~/cluster.secret
  sudo bash install_mac_daemon.sh --p2p-ticket-file ticket.txt --secret-file ~/cluster.secret --name mac-node-01
  bash install_mac_daemon.sh --dry-run --master 192.168.1.50:8088 --insecure
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
        --p2p-ticket-file)
            P2P_TICKET_FILE="${2:-}"
            shift 2
            ;;
        --secret-file)
            SECRET_SRC="${2:-}"
            shift 2
            ;;
        --insecure)
            INSECURE=true
            shift
            ;;
        --user)
            RUN_USER_ARG="${2:-}"
            shift 2
            ;;
        --run-as-root)
            RUN_AS_ROOT=true
            shift
            ;;
        --yes|-y)
            ASSUME_YES=true
            shift
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

# A worker keeps the same worker id across restarts only when it has a name (the id file is
# ~/.oxideswarm/worker_id.<name>); a service that launchd respawns must not show up as a new machine.
if [[ -z "$WORKER_NAME" ]]; then
    WORKER_NAME="$(hostname -s 2>/dev/null || true)"
fi

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

# 2. Validate the connection settings
if [[ -n "$P2P_TICKET_FILE" ]]; then
    if [[ ! -r "$P2P_TICKET_FILE" ]]; then
        log_error "Cannot read P2P ticket file: ${P2P_TICKET_FILE}"
        exit 1
    fi
    P2P_TICKET="$(tr -d '\r\n' < "$P2P_TICKET_FILE")"
fi
if [[ -n "$P2P_TICKET" && "$P2P_TICKET" == *"'"* ]]; then
    log_error "The P2P ticket contains a single quote and cannot be stored in the TOML configuration."
    exit 1
fi
if [[ -z "$CONFIG_FILE" && -z "$MASTER_ADDR" && -z "$P2P_TICKET" ]]; then
    log_error "Nothing to connect to: pass --master HOST:8088 (the master's CLUSTER port) or --p2p-ticket-file FILE."
    show_help
    exit 1
fi
if [[ "$MASTER_ADDR" == *:8080 ]]; then
    log_warn "Port 8080 is the master's DASHBOARD. Workers must connect to the cluster port, normally 8088 (e.g. --master 192.168.1.50:8088)."
fi

# Whether a worker must hold the cluster secret to follow this master (mirrors the CLI rule):
# everything except a master on this very machine needs one.
master_needs_secret() {
    local host
    if [[ -n "$P2P_TICKET" ]]; then
        return 0
    fi
    if [[ -z "$MASTER_ADDR" ]]; then
        return 0
    fi
    host="${MASTER_ADDR%:*}"
    case "$host" in
        127.*|localhost|*.localhost|"[::1]"|"::1") return 1 ;;
    esac
    return 0
}

# 3. Decide which account runs the worker (and therefore every task)
USER_RE='^[A-Za-z_][A-Za-z0-9._-]*$'
if [[ "$RUN_AS_ROOT" == true ]]; then
    RUN_USER="root"
    log_warn "--run-as-root: every task the master dispatches will run as root and control this Mac."
elif [[ -n "$RUN_USER_ARG" ]]; then
    RUN_USER="$RUN_USER_ARG"
elif [[ -n "${SUDO_USER:-}" && "${SUDO_USER}" != "root" ]]; then
    RUN_USER="$SUDO_USER"
elif [[ $EUID -ne 0 ]]; then
    RUN_USER="$(id -un)"
else
    RUN_USER=""
fi

if [[ -z "$RUN_USER" ]]; then
    if [[ -t 0 && "$ASSUME_YES" == false ]]; then
        read -r -p "Account that will run the worker and every task it receives (not root): " RUN_USER
    fi
    if [[ -z "$RUN_USER" ]]; then
        log_error "Cannot tell which account should run the worker. Pass --user NAME (a dedicated or your own account), or --run-as-root to accept the risk."
        exit 1
    fi
elif [[ "$RUN_AS_ROOT" == false && -z "$RUN_USER_ARG" && -t 0 && "$ASSUME_YES" == false && "$DRY_RUN" == false ]]; then
    echo -e "The worker, and every task the master sends it, will run as the account ${BOLD}${RUN_USER}${NC}."
    read -r -p "Press Enter to accept, or type another account (not root): " ANSWER
    if [[ -n "$ANSWER" ]]; then
        RUN_USER="$ANSWER"
    fi
fi

if [[ ! "$RUN_USER" =~ $USER_RE ]]; then
    log_error "Invalid account name: ${RUN_USER}"
    exit 1
fi
if [[ "$RUN_USER" == "root" && "$RUN_AS_ROOT" == false ]]; then
    log_error "Refusing to run the worker as root without --run-as-root."
    exit 1
fi
if ! id -u "$RUN_USER" >/dev/null 2>&1; then
    log_error "Account '${RUN_USER}' does not exist."
    exit 1
fi
RUN_GROUP="$(id -gn "$RUN_USER")"

resolve_home() {
    local h=""
    if command -v dscl >/dev/null 2>&1; then
        h="$(dscl . -read "/Users/$1" NFSHomeDirectory 2>/dev/null | awk '{print $2}' || true)"
    fi
    if [[ -z "$h" ]]; then
        h="$(eval echo "~$1")"
    fi
    echo "$h"
}
RUN_HOME="$(resolve_home "$RUN_USER")"
log_info "Worker account : ${RUN_USER} (group ${RUN_GROUP}, home ${RUN_HOME})"

# 4. Locate the cluster secret
SECRET_DEST="${CONFIG_DIR}/cluster.secret"
SECRET_FOUND=""
if [[ -z "$CONFIG_FILE" ]]; then
    if [[ -z "$SECRET_SRC" && -n "${OXIDESWARM_SECRET_FILE:-}" ]]; then
        SECRET_SRC="$OXIDESWARM_SECRET_FILE"
    fi
    if [[ -z "$SECRET_SRC" && -f "${RUN_HOME}/.oxideswarm/cluster.secret" ]]; then
        SECRET_SRC="${RUN_HOME}/.oxideswarm/cluster.secret"
    fi
    if [[ -n "$SECRET_SRC" ]]; then
        if [[ ! -r "$SECRET_SRC" ]]; then
            log_error "Cluster secret file not readable: ${SECRET_SRC}"
            exit 1
        fi
        SECRET_FOUND="$SECRET_SRC"
    elif master_needs_secret && [[ "$INSECURE" == false ]]; then
        log_error "No cluster secret found. A master address or P2P ticket only says WHERE the master is;"
        log_error "the secret proves it is the real one. Copy the master's file over a trusted channel (scp, USB):"
        log_error "  scp user@master:~/.oxideswarm/cluster.secret ${RUN_HOME}/.oxideswarm/cluster.secret"
        log_error "then re-run with --secret-file PATH, or pass --insecure to continue WITHOUT authenticating the master."
        exit 1
    fi
    if [[ "$INSECURE" == true ]]; then
        log_warn "--insecure: the worker will NOT authenticate its master and will obey any host that answers as the master."
    fi
fi

# 5. Binary Resolution and Deployment
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

BIN_READY=false
if [[ -n "$SOURCE_BIN" && -f "$SOURCE_BIN" ]]; then
    log_info "Found worker binary at: ${SOURCE_BIN}"
    BIN_READY=true
    if [[ "$DRY_RUN" == false ]]; then
        mkdir -p "${BIN_INSTALL_DIR}"
        if [[ "$SOURCE_BIN" != "$DEST_BIN" ]]; then
            cp -f "${SOURCE_BIN}" "${DEST_BIN}"
        fi
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
    BIN_READY=true
else
    # Registering a daemon whose binary is missing would only crash-loop in launchd.
    if [[ "$DRY_RUN" == false ]]; then
        log_error "No rusty-grid binary found. Build one first (cargo build --release --bin rusty-grid) or pass --bin PATH."
        exit 1
    fi
    log_warn "[DRY-RUN] No pre-built binary found; a real run would stop here."
fi

# 6. Directory Structure and Permissions Setup
log_info "Configuring system directories..."
if [[ "$DRY_RUN" == false ]]; then
    mkdir -p "${LOG_DIR}" "${WORK_DIR}" "${CONFIG_DIR}"
    chown "${RUN_USER}:${RUN_GROUP}" "${LOG_DIR}" "${WORK_DIR}"
    chown root:wheel "${CONFIG_DIR}"
    chmod 755 "${LOG_DIR}" "${CONFIG_DIR}"
    chmod 700 "${WORK_DIR}"
    log_success "Directories initialized (${LOG_DIR}, ${WORK_DIR} owned by ${RUN_USER}; ${CONFIG_DIR} root-owned)"
else
    log_info "[DRY-RUN] Would create ${LOG_DIR} and ${WORK_DIR} (owner ${RUN_USER}) and ${CONFIG_DIR} (root:wheel)"
fi

# 7. Stop and Unload Existing Active Daemon (if registered)
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

# 8. Worker configuration (ticket + secret path live here, not in the plist or in argv)
toml_escape() {
    printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'
}

WORKER_CONFIG_PATH="${CONFIG_DIR}/worker.toml"
render_worker_config() {
    # $1 = "mask" hides the ticket (dry-run output)
    echo "# OxideSwarm worker configuration generated by install_mac_daemon.sh"
    echo "# Mode 600, owned by the service account: it holds the P2P ticket / secret path."
    echo "[worker]"
    if [[ -n "$MASTER_ADDR" ]]; then
        echo "master = \"$(toml_escape "$MASTER_ADDR")\""
    fi
    if [[ -n "$P2P_TICKET" ]]; then
        if [[ "${1:-}" == "mask" ]]; then
            echo "p2p_ticket = '<hidden>'"
        else
            echo "p2p_ticket = '${P2P_TICKET}'"
        fi
    fi
    if [[ -n "$WORKER_NAME" ]]; then
        echo "name = \"$(toml_escape "$WORKER_NAME")\""
    fi
    echo "heartbeat_interval_secs = 3"
    echo "sandbox_base_dir = \"$(toml_escape "${WORK_DIR}/sandboxes")\""
    echo "keep_sandboxes = false"
    if [[ -n "$SECRET_FOUND" ]]; then
        echo "secret_file = '${SECRET_DEST}'"
    fi
    if [[ "$INSECURE" == true ]]; then
        echo "insecure = true"
    fi
}

if [[ -n "$CONFIG_FILE" ]]; then
    WORKER_CONFIG_PATH="$CONFIG_FILE"
    log_info "Using caller-supplied worker config: ${WORKER_CONFIG_PATH} (make sure the service account can read it)"
elif [[ "$DRY_RUN" == false ]]; then
    ( umask 077; render_worker_config > "${WORKER_CONFIG_PATH}" )
    chown "${RUN_USER}:${RUN_GROUP}" "${WORKER_CONFIG_PATH}"
    chmod 600 "${WORKER_CONFIG_PATH}"
    log_success "Wrote ${WORKER_CONFIG_PATH} (0600, ${RUN_USER})"
    if [[ -n "$SECRET_FOUND" ]]; then
        ( umask 077; cat "$SECRET_FOUND" > "${SECRET_DEST}" )
        chown "${RUN_USER}:${RUN_GROUP}" "${SECRET_DEST}"
        chmod 600 "${SECRET_DEST}"
        log_success "Installed cluster secret at ${SECRET_DEST} (0600, ${RUN_USER})"
    fi
else
    log_info "[DRY-RUN] Would write ${WORKER_CONFIG_PATH} (0600, ${RUN_USER}):"
    render_worker_config mask | sed 's/^/        /'
    if [[ -n "$SECRET_FOUND" ]]; then
        log_info "[DRY-RUN] Would copy the cluster secret ${SECRET_FOUND} -> ${SECRET_DEST} (0600, ${RUN_USER})"
    fi
fi

# 9. Generate and Deploy LaunchDaemon Property List (.plist)
log_info "Generating LaunchDaemon property list at ${PLIST_PATH}..."

# Build ProgramArguments XML array elements
PROG_ARGS_XML="        <string>${DEST_BIN}</string>\n        <string>worker</string>\n        <string>--config</string>\n        <string>${WORKER_CONFIG_PATH}</string>"

if [[ "$RUN_USER" == "root" ]]; then
    USER_XML="    <!-- Running as root (--run-as-root): tasks have full control of this Mac -->"
else
    USER_XML="    <!-- Unprivileged account: every task the master sends runs with this identity, not as root -->
    <key>UserName</key>
    <string>${RUN_USER}</string>"
fi

PLIST_CONTENT="<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">
<plist version=\"1.0\">
<dict>
    <!-- Daemon Identification -->
    <key>Label</key>
    <string>${SERVICE_LABEL}</string>

${USER_XML}

    <!-- Executable Invocation (ticket and secret path are in the 0600 config, not here) -->
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
        <key>HOME</key>
        <string>${RUN_HOME}</string>
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
    log_success "Deployed property list with strict permissions (root:wheel, 0644; it contains no secrets)"

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
    if command -v plutil >/dev/null 2>&1; then
        LINT_TMP="$(mktemp "${TMPDIR:-/tmp}/oxideswarm-plist.XXXXXX")"
        echo "${PLIST_CONTENT}" > "${LINT_TMP}"
        if plutil -lint "${LINT_TMP}" >/dev/null; then
            log_success "[DRY-RUN] Generated plist passes plutil -lint."
        else
            log_error "[DRY-RUN] Generated plist FAILED plutil -lint."
        fi
        rm -f "${LINT_TMP}"
    fi
fi

# 10. Register and Bootstrap with launchctl
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

    # 11. Verification & Status Report
    sleep 1
    if launchctl list "${SERVICE_LABEL}" &>/dev/null; then
        INFO=$(launchctl list "${SERVICE_LABEL}" 2>/dev/null || true)
        PID=$(echo "$INFO" | awk -F'=' '/"PID"/ {print $2}' | tr -d ' ;' || echo "")

        if [[ -n "$PID" && "$PID" != "-" ]]; then
            log_success "OxideSwarm worker is actively running in background (PID: ${PID}) as ${RUN_USER}."
        else
            log_warn "Daemon registered in launchd but no process is running yet; check ${LOG_DIR}/worker.err.log."
        fi
    else
        log_warn "Daemon registered. Use 'launchctl print system/${SERVICE_LABEL}' to inspect status."
    fi

    echo -e "\n${BOLD}${GREEN}=== macOS LaunchDaemon Installation Completed ===${NC}"
    echo -e "Service Label : ${CYAN}${SERVICE_LABEL}${NC}"
    echo -e "Runs As       : ${CYAN}${RUN_USER}${NC}"
    echo -e "Plist Path    : ${CYAN}${PLIST_PATH}${NC}"
    echo -e "Worker Config : ${CYAN}${WORKER_CONFIG_PATH}${NC}"
    echo -e "Log Files     : ${CYAN}${LOG_DIR}/worker.log${NC} & ${CYAN}${LOG_DIR}/worker.err.log${NC}"
    echo -e "Binary Path   : ${CYAN}${DEST_BIN}${NC} (alias: ${CYAN}${DEST_ALIAS}${NC})"
    echo -e "Status Check  : ${BOLD}sudo launchctl print system/${SERVICE_LABEL}${NC}"
    echo -e "Joined grid?  : look for 'Registration accepted' in ${LOG_DIR}/worker.log\n"
else
    echo -e "\n${BOLD}${GREEN}=== Dry-Run Completed Successfully (No changes made) ===${NC}\n"
fi
