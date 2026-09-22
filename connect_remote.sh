#!/usr/bin/env bash
# ==============================================================================
# connect_remote.sh — 1-Click Zero-Config P2P Remote Pairing for macOS
# ==============================================================================
# Seamlessly connects this macOS machine as an OxideSwarm worker node across
# the internet using native P2P NAT traversal (iroh QUIC + DERP Relay).
#
# Features:
# - Interactive prompt or 1-click argument execution
# - Validates P2P ticket syntax
# - Persists configuration to ~/.oxideswarm/rusty-grid.toml
# - Supports foreground execution or persistent LaunchDaemon background service
# - Automatic reconnection with exponential backoff on network drop or master reboot
# ==============================================================================

set -euo pipefail

# ANSI color codes
BOLD='\033[1m'
CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

CONFIG_DIR="${HOME}/.oxideswarm"
CONFIG_FILE="${CONFIG_DIR}/rusty-grid.toml"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DAEMON_SCRIPT="${SCRIPT_DIR}/packaging/macos/install_mac_daemon.sh"

TICKET=""
MODE="interactive"
WORKER_NAME="$(hostname -s 2>/dev/null || echo "mac-node")-mac"
DRY_RUN=false

show_help() {
    cat <<EOF
OxideSwarm 1-Click Remote Worker Pairing (macOS)

Usage:
  ./connect_remote.sh [TICKET] [OPTIONS]

Arguments:
  TICKET                 P2P connection ticket JSON string from Master (optional;
                         prompted interactively if omitted)

Options:
  --service, --daemon    Install and register as background LaunchDaemon on boot
  --foreground           Run worker directly in foreground
  --name <NAME>          Override worker node name (default: ${WORKER_NAME})
  --dry-run              Validate ticket and simulate without running worker/service
  -h, --help             Show this help message and exit

Examples:
  ./connect_remote.sh
  ./connect_remote.sh '{"id":"...","addrs":[...]}'
  sudo ./connect_remote.sh '{"id":"..."}' --service
EOF
}

# 1. Parse Arguments & Options
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            show_help
            exit 0
            ;;
        --service|--daemon)
            MODE="service"
            shift
            ;;
        --foreground)
            MODE="foreground"
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --name)
            WORKER_NAME="${2:-$WORKER_NAME}"
            shift 2
            ;;
        *)
            if [[ -z "$TICKET" && ! "$1" =~ ^-- ]]; then
                TICKET="$1"
                shift
            else
                echo -e "${RED}[ERROR] Unknown option or extra argument: $1${NC}" >&2
                show_help
                exit 1
            fi
            ;;
    esac
done

echo -e "\n${BOLD}${CYAN}========================================================${NC}"
echo -e "${BOLD}${CYAN}   OxideSwarm 1-Click Remote Worker Pairing (macOS)${NC}"
echo -e "${BOLD}${CYAN}========================================================${NC}\n"

# 2. Resolve P2P Ticket
if [[ -z "$TICKET" ]]; then
    # Check if a previously saved ticket exists
    if [[ -f "$CONFIG_FILE" ]] && grep -q "p2p_ticket" "$CONFIG_FILE"; then
        SAVED_TICKET=$(grep "p2p_ticket" "$CONFIG_FILE" | head -n1 | sed -E 's/.*p2p_ticket[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/')
        if [[ -n "$SAVED_TICKET" ]]; then
            echo -e "${YELLOW}Found previously saved ticket in ${CONFIG_FILE}:${NC}"
            echo -e "  ${SAVED_TICKET:0:45}..."
            if [ -t 0 ]; then
                read -r -p "Use saved ticket? [Y/n]: " USE_SAVED
                if [[ "${USE_SAVED:-Y}" =~ ^[Yy]$ ]]; then
                    TICKET="$SAVED_TICKET"
                fi
            else
                # Non-interactive fallback
                TICKET="$SAVED_TICKET"
            fi
        fi
    fi
fi

# If ticket is still not resolved, prompt interactively or error out
if [[ -z "$TICKET" ]]; then
    if [ -t 0 ]; then
        while [[ -z "$TICKET" ]]; do
            read -r -p "Paste OxideSwarm P2P Ticket from Master: " TICKET
            TICKET=$(echo "$TICKET" | tr -d '\r\n')
        done
    else
        echo -e "${RED}[ERROR] No P2P ticket provided and non-interactive shell detected.${NC}" >&2
        echo -e "Usage: ./connect_remote.sh '<TICKET>' [--foreground|--service]" >&2
        exit 1
    fi
fi

# Clean whitespace / newlines from ticket
TICKET=$(echo "$TICKET" | tr -d '\r\n')

# 3. Validate Ticket Structure
if [[ ! "$TICKET" =~ \"id\"[[:space:]]*: ]] || [[ ! "$TICKET" =~ ^\{ ]]; then
    echo -e "${RED}[ERROR] Invalid P2P ticket format!${NC}" >&2
    echo -e "Expected JSON containing '\"id\": \"<node_id>\"' (e.g. {\"id\":\"...\",\"addrs\":[...]})." >&2
    exit 1
fi
echo -e "${GREEN}[OK] Validated P2P Ticket format.${NC}"

# 4. Persist Configuration to ~/.oxideswarm/rusty-grid.toml
mkdir -p "$CONFIG_DIR"
cat <<EOF > "$CONFIG_FILE"
# OxideSwarm 1-Click Remote Configuration
# Generated by connect_remote.sh on $(date -u +"%Y-%m-%dT%H:%M:%SZ")
[worker]
p2p_ticket = '${TICKET}'
name = "${WORKER_NAME}"
heartbeat_interval_secs = 3
keep_sandboxes = false
EOF
echo -e "${GREEN}[OK] Configuration saved to ${CONFIG_FILE}${NC}"

if [[ "$DRY_RUN" == true ]]; then
    echo -e "\n${BOLD}${GREEN}=== Dry-Run Completed Successfully ===${NC}"
    echo -e "Ticket: ${TICKET:0:30}..."
    echo -e "Target Config: ${CONFIG_FILE}"
    echo -e "Worker Name: ${WORKER_NAME}"
    exit 0
fi

# 5. Interactive Execution Mode Selection if not explicitly specified
if [[ "$MODE" == "interactive" ]]; then
    if [ -t 0 ]; then
        echo -e "\n${BOLD}Select Execution Mode:${NC}"
        echo -e "  ${CYAN}1) Run Worker in foreground${NC} (direct live output, press Ctrl+C to stop)"
        echo -e "  ${CYAN}2) Install as background LaunchDaemon service${NC} (runs silently on boot)"
        read -r -p "Enter choice [1/2, default: 1]: " USER_CHOICE
        case "${USER_CHOICE:-1}" in
            2)
                MODE="service"
                ;;
            *)
                MODE="foreground"
                ;;
        esac
    else
        MODE="foreground"
    fi
fi

# 6. Mode Execution
if [[ "$MODE" == "service" ]]; then
    if [[ ! -f "$DAEMON_SCRIPT" ]]; then
        echo -e "${RED}[ERROR] LaunchDaemon installer script not found at ${DAEMON_SCRIPT}${NC}" >&2
        exit 1
    fi

    echo -e "\n${BLUE}[INFO] Deploying system-level LaunchDaemon service...${NC}"
    if [[ $EUID -eq 0 ]]; then
        bash "$DAEMON_SCRIPT" --p2p-ticket "$TICKET" --name "$WORKER_NAME"
    else
        echo -e "${YELLOW}[INFO] Requesting sudo privileges to install LaunchDaemon...${NC}"
        sudo bash "$DAEMON_SCRIPT" --p2p-ticket "$TICKET" --name "$WORKER_NAME"
    fi
    echo -e "\n${BOLD}${GREEN}=== 1-Click macOS Background Pairing Complete! ===${NC}"
    echo -e "The worker is registered in launchd, running in the background, and will auto-reconnect on boot."
    exit 0
fi

# Foreground Mode Execution
# Locate or compile rusty-grid binary
BIN=""
CANDIDATES=(
    "${SCRIPT_DIR}/target/release/rusty-grid"
    "${SCRIPT_DIR}/target/debug/rusty-grid"
    "/usr/local/bin/rusty-grid"
    "$(command -v rusty-grid 2>/dev/null || true)"
)

for cand in "${CANDIDATES[@]}"; do
    if [[ -n "$cand" && -x "$cand" ]]; then
        BIN="$cand"
        break
    fi
done

if [[ -z "$BIN" ]]; then
    echo -e "${YELLOW}[INFO] No pre-compiled binary found. Building rusty-grid with cargo...${NC}"
    cargo build --release --bin rusty-grid
    BIN="${SCRIPT_DIR}/target/release/rusty-grid"
fi

echo -e "\n${BOLD}${GREEN}Starting OxideSwarm Worker...${NC}"
echo -e "${CYAN}Node Name        : ${WORKER_NAME}${NC}"
echo -e "${CYAN}Config Path      : ${CONFIG_FILE}${NC}"
echo -e "${CYAN}P2P NAT Traversal: Active (iroh QUIC + DERP Relay)${NC}"
echo -e "${CYAN}Auto-Reconnect   : Active (Exponential backoff < 3s)${NC}"
echo -e "${YELLOW}Press Ctrl+C to terminate the worker.${NC}\n"

exec "$BIN" worker --config "$CONFIG_FILE"
