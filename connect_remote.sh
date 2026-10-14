#!/usr/bin/env bash
# ==============================================================================
# connect_remote.sh — 1-Click P2P Remote Pairing for macOS / Linux
# ==============================================================================
# Connects this machine as an OxideSwarm worker to a master on ANOTHER network
# through native P2P NAT traversal (iroh QUIC + relay).
#
# Pairing needs two things from the master machine:
#   1. its P2P ticket   (master started with --p2p; ticket in the dashboard or in
#                        the file given by --p2p-ticket-file / ~/.oxideswarm/master_ticket.txt)
#   2. its cluster secret file (~/.oxideswarm/cluster.secret on the master). A ticket only
#      ADDRESSES the master, it does not prove who runs it, so the worker refuses to pair
#      without the secret. Copy the file over a trusted channel (scp, USB), never chat/email:
#        scp user@master:~/.oxideswarm/cluster.secret ~/.oxideswarm/cluster.secret
#        chmod 600 ~/.oxideswarm/cluster.secret
#
# What this script does:
# - validates the ticket and the presence of the secret
# - writes a DEDICATED config (<state dir>/remote-worker.toml, mode 600; never the
#   auto-loaded rusty-grid.toml, and never without a timestamped backup of the old one)
# - runs the worker in the foreground, or installs the macOS LaunchDaemon (--service)
#
# State directory: $OXIDESWARM_HOME, else ~/.oxideswarm.   --dry-run changes nothing.
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

CONFIG_DIR="${OXIDESWARM_HOME:-${HOME}/.oxideswarm}"
CONFIG_FILE="${CONFIG_DIR}/remote-worker.toml"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DAEMON_SCRIPT="${SCRIPT_DIR}/packaging/macos/install_mac_daemon.sh"

TICKET=""
TICKET_FILE_ARG=""
SECRET_FILE_ARG=""
SERVICE_USER=""
INSECURE=false
MODE="interactive"
case "$(uname -s)" in
    Darwin) NAME_SUFFIX="mac" ;;
    *) NAME_SUFFIX="$(uname -s | tr '[:upper:]' '[:lower:]')" ;;
esac
WORKER_NAME="$(hostname -s 2>/dev/null || echo "node")-${NAME_SUFFIX}"
DRY_RUN=false

show_help() {
    cat <<EOF
OxideSwarm 1-Click Remote Worker Pairing (macOS / Linux)

Usage:
  ./connect_remote.sh [TICKET] [OPTIONS]

Arguments:
  TICKET                 P2P connection ticket (JSON) from the master (optional; prompted
                         interactively if omitted; --ticket-file keeps it out of 'ps')

Options:
  --secret-file <PATH>   Cluster secret copied from the master machine (default:
                         \$OXIDESWARM_SECRET_FILE, then ${CONFIG_DIR}/cluster.secret)
  --ticket-file <PATH>   Read the ticket from a file instead of the command line
  --insecure             Pair WITHOUT a cluster secret: the worker will then obey any host
                         that answers as the master (NOT recommended)
  --service, --daemon    Install and register as background LaunchDaemon on boot (macOS;
                         the worker runs as your user, never as root)
  --user <NAME>          With --service: account that runs the worker (default: you)
  --foreground           Run worker directly in foreground
  --name <NAME>          Override worker node name (default: ${WORKER_NAME})
  --dry-run              Validate ticket and secret and show what would happen; writes nothing
  -h, --help             Show this help message and exit

Environment:
  OXIDESWARM_HOME        State directory (default: ~/.oxideswarm)

Examples:
  ./connect_remote.sh
  ./connect_remote.sh '{"id":"...","addrs":[...]}' --secret-file ./cluster.secret
  sudo ./connect_remote.sh --ticket-file ticket.txt --service
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
        --insecure)
            INSECURE=true
            shift
            ;;
        --name)
            WORKER_NAME="${2:-$WORKER_NAME}"
            shift 2
            ;;
        --secret-file)
            SECRET_FILE_ARG="${2:?--secret-file needs a PATH}"
            shift 2
            ;;
        --ticket-file)
            TICKET_FILE_ARG="${2:?--ticket-file needs a PATH}"
            shift 2
            ;;
        --user)
            SERVICE_USER="${2:?--user needs a NAME}"
            shift 2
            ;;
        *)
            if [[ -z "$TICKET" && ! "$1" =~ ^-- ]]; then
                TICKET="$1"
                shift
                # If ticket is JSON and was split across arguments by Windows shell/quotes, reassemble
                while [[ $# -gt 0 && ! "$1" =~ ^-- && "$TICKET" =~ ^\{ && ! "$TICKET" =~ \}$ ]]; do
                    TICKET="${TICKET} $1"
                    shift
                done
            else
                echo -e "${RED}[ERROR] Unknown option or extra argument: $1${NC}" >&2
                show_help
                exit 1
            fi
            ;;
    esac
done

echo -e "\n${BOLD}${CYAN}========================================================${NC}"
echo -e "${BOLD}${CYAN}   OxideSwarm 1-Click Remote Worker Pairing${NC}"
echo -e "${BOLD}${CYAN}========================================================${NC}\n"

# 2. Resolve P2P Ticket: argument > --ticket-file > previously saved ticket > prompt
if [[ -z "$TICKET" && -n "$TICKET_FILE_ARG" ]]; then
    if [[ ! -r "$TICKET_FILE_ARG" ]]; then
        echo -e "${RED}[ERROR] Cannot read ticket file: ${TICKET_FILE_ARG}${NC}" >&2
        exit 1
    fi
    TICKET="$(cat "$TICKET_FILE_ARG")"
fi

if [[ -z "$TICKET" ]]; then
    # Check if a previously saved ticket exists (our own file; written with single quotes)
    if [[ -f "$CONFIG_FILE" ]] && grep -q "p2p_ticket" "$CONFIG_FILE"; then
        SAVED_TICKET=$(grep "p2p_ticket" "$CONFIG_FILE" | head -n1 | sed -E "s/.*p2p_ticket[[:space:]]*=[[:space:]]*'([^']+)'.*/\\1/")
        if [[ -n "$SAVED_TICKET" && "$SAVED_TICKET" != *p2p_ticket* ]]; then
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

# 3. Validate Ticket Structure: a JSON object with a string "id" (the master's node id)
validate_ticket() {
    local t="$1"
    if [[ ! "$t" =~ ^\{ ]] || [[ ! "$t" =~ \}$ ]] || [[ ! "$t" =~ \"id\"[[:space:]]*: ]]; then
        return 1
    fi
    # A single quote would end the TOML literal string we store the ticket in.
    if [[ "$t" == *"'"* ]]; then
        return 1
    fi
    if command -v python3 >/dev/null 2>&1; then
        printf '%s' "$t" | python3 -c 'import sys, json
d = json.load(sys.stdin)
assert isinstance(d, dict) and isinstance(d.get("id"), str) and d["id"]' >/dev/null 2>&1 || return 1
    fi
    return 0
}

if ! validate_ticket "$TICKET"; then
    echo -e "${RED}[ERROR] Invalid P2P ticket format!${NC}" >&2
    echo -e "Expected JSON containing '\"id\": \"<node_id>\"' (e.g. {\"id\":\"...\",\"addrs\":[...]})." >&2
    exit 1
fi
echo -e "${GREEN}[OK] Validated P2P Ticket format.${NC}"

# 4. Resolve the cluster secret. A ticket is only an address, so pairing without the secret
#    would let any host that answers on that address become this worker's master.
SECRET_FILE=""
SECRET_NOTE=""
if [[ -n "$SECRET_FILE_ARG" ]]; then
    SECRET_FILE="$SECRET_FILE_ARG"
elif [[ -n "${OXIDESWARM_SECRET_FILE:-}" ]]; then
    SECRET_FILE="$OXIDESWARM_SECRET_FILE"
elif [[ -n "${OXIDESWARM_SECRET:-}" ]]; then
    SECRET_NOTE="environment variable OXIDESWARM_SECRET"
elif [[ -f "${CONFIG_DIR}/cluster.secret" ]]; then
    SECRET_FILE="${CONFIG_DIR}/cluster.secret"
fi

SECRET_MISSING=false
if [[ -n "$SECRET_FILE" ]]; then
    if [[ -r "$SECRET_FILE" ]]; then
        SECRET_NOTE="$SECRET_FILE"
        # Resolve to an absolute path: the daemon and the config file are used from elsewhere.
        SECRET_FILE="$(cd "$(dirname "$SECRET_FILE")" && pwd)/$(basename "$SECRET_FILE")"
    else
        SECRET_MISSING=true
        echo -e "${RED}[ERROR] Cluster secret file not readable: ${SECRET_FILE}${NC}" >&2
    fi
elif [[ -z "$SECRET_NOTE" ]]; then
    SECRET_MISSING=true
fi

secret_help() {
    cat >&2 <<EOF
A P2P ticket only addresses the master; it does not authenticate it. Copy the master's
cluster secret to this machine over a trusted channel first:

  On the master machine:   oxideswarm secret show          (prints the secret; or use the file)
  Copy the FILE:           scp user@master:~/.oxideswarm/cluster.secret ${CONFIG_DIR}/cluster.secret
                           (or carry it on a USB stick; never paste it into chat or email)
  Then:                    chmod 600 ${CONFIG_DIR}/cluster.secret

and re-run, or pass --secret-file PATH. To pair anyway, WITHOUT authenticating the master
(any host that answers can then run commands on this machine), pass --insecure.
EOF
}

if [[ "$SECRET_MISSING" == true && "$INSECURE" != true ]]; then
    if [[ "$DRY_RUN" == true ]]; then
        echo -e "${YELLOW}[WARN] No cluster secret found. A real run would stop here (see below).${NC}" >&2
        secret_help
    else
        echo -e "${RED}[ERROR] No cluster secret found.${NC}" >&2
        secret_help
        exit 1
    fi
elif [[ "$INSECURE" == true ]]; then
    echo -e "${RED}[WARN] --insecure: this worker will NOT authenticate the master.${NC}" >&2
else
    echo -e "${GREEN}[OK] Cluster secret: ${SECRET_NOTE}${NC}"
fi

# A ticket left in the auto-loaded rusty-grid.toml (written by older versions of this script)
# is applied to EVERY rusty-grid invocation and overrides --master. Warn, never edit it.
LEGACY_CONFIG="${CONFIG_DIR}/rusty-grid.toml"
if [[ -f "$LEGACY_CONFIG" ]] && grep -q "^[[:space:]]*p2p_ticket" "$LEGACY_CONFIG"; then
    echo -e "${YELLOW}[WARN] ${LEGACY_CONFIG} contains a p2p_ticket. The CLI loads that file for every run, so"
    echo -e "       'rusty-grid worker --master HOST:8088' will keep dialing that ticket instead. Remove the"
    echo -e "       p2p_ticket line (or the file) if it is stale; this script no longer writes it.${NC}"
fi

# 5. Persist configuration to the dedicated remote-worker.toml (mode 600)
render_config() {
    echo "# OxideSwarm 1-Click Remote Configuration"
    echo "# Generated by connect_remote.sh on $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo "# Use with:  rusty-grid worker --config ${CONFIG_FILE}"
    echo "[worker]"
    echo "p2p_ticket = '${TICKET}'"
    echo "name = \"${WORKER_NAME}\""
    echo "heartbeat_interval_secs = 3"
    echo "keep_sandboxes = false"
    if [[ -n "$SECRET_FILE" ]]; then
        echo "secret_file = '${SECRET_FILE}'"
    fi
    if [[ "$INSECURE" == true ]]; then
        echo "insecure = true"
    fi
}

write_config() {
    local tmp stamp
    ( umask 077; mkdir -p "$CONFIG_DIR" )
    tmp="$(mktemp "${CONFIG_DIR}/.remote-worker.XXXXXX")"
    render_config > "$tmp"
    chmod 600 "$tmp"
    if [[ -f "$CONFIG_FILE" ]]; then
        # Compare ignoring the timestamp line; keep a backup whenever the content differs.
        if ! diff -q <(grep -v '^# Generated by' "$CONFIG_FILE") <(grep -v '^# Generated by' "$tmp") >/dev/null 2>&1; then
            stamp="$(date +%Y%m%d%H%M%S)"
            cp -p "$CONFIG_FILE" "${CONFIG_FILE}.bak-${stamp}"
            echo -e "${YELLOW}[INFO] Existing config backed up to ${CONFIG_FILE}.bak-${stamp}${NC}"
        fi
    fi
    mv -f "$tmp" "$CONFIG_FILE"
    echo -e "${GREEN}[OK] Configuration saved to ${CONFIG_FILE} (mode 600)${NC}"
}

if [[ "$DRY_RUN" == true ]]; then
    echo -e "\n${BOLD}${GREEN}=== Dry-Run Completed Successfully ===${NC}"
    echo -e "Ticket: ${TICKET:0:30}..."
    echo -e "Would write config: ${CONFIG_FILE}"
    echo -e "Secret: ${SECRET_NOTE:-MISSING (a real run needs --secret-file or --insecure)}"
    echo -e "Worker Name: ${WORKER_NAME}"
    echo -e "Nothing was written."
    exit 0
fi

write_config

# 6. Interactive Execution Mode Selection if not explicitly specified
if [[ "$MODE" == "interactive" ]]; then
    if [ -t 0 ]; then
        echo -e "\n${BOLD}Select Execution Mode:${NC}"
        echo -e "  ${CYAN}1) Run Worker in foreground${NC} (direct live output, press Ctrl+C to stop)"
        echo -e "  ${CYAN}2) Install as background LaunchDaemon service${NC} (macOS; runs on boot)"
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

# 7. Mode Execution
if [[ "$MODE" == "service" ]]; then
    if [[ "$(uname -s)" != "Darwin" ]]; then
        echo -e "${RED}[ERROR] --service installs a macOS LaunchDaemon; on Linux run in the foreground or write a systemd unit that executes: rusty-grid worker --config ${CONFIG_FILE}${NC}" >&2
        exit 1
    fi
    if [[ ! -f "$DAEMON_SCRIPT" ]]; then
        echo -e "${RED}[ERROR] LaunchDaemon installer script not found at ${DAEMON_SCRIPT}${NC}" >&2
        exit 1
    fi

    # The ticket reaches the installer through a private temp file, never through argv.
    TICKET_TMP="$(umask 077; mktemp "${TMPDIR:-/tmp}/oxideswarm-ticket.XXXXXX")"
    trap 'rm -f "$TICKET_TMP"' EXIT
    printf '%s' "$TICKET" > "$TICKET_TMP"

    INSTALL_ARGS=(--p2p-ticket-file "$TICKET_TMP" --name "$WORKER_NAME")
    if [[ -n "$SECRET_FILE" ]]; then
        INSTALL_ARGS+=(--secret-file "$SECRET_FILE")
    fi
    if [[ "$INSECURE" == true ]]; then
        INSTALL_ARGS+=(--insecure)
    fi
    if [[ -n "$SERVICE_USER" ]]; then
        INSTALL_ARGS+=(--user "$SERVICE_USER")
    fi

    echo -e "\n${BLUE}[INFO] Deploying system-level LaunchDaemon service...${NC}"
    if [[ $EUID -eq 0 ]]; then
        bash "$DAEMON_SCRIPT" "${INSTALL_ARGS[@]}"
    else
        echo -e "${YELLOW}[INFO] Requesting sudo privileges to install LaunchDaemon...${NC}"
        sudo bash "$DAEMON_SCRIPT" "${INSTALL_ARGS[@]}"
    fi
    echo -e "\n${BOLD}${GREEN}=== LaunchDaemon installed ===${NC}"
    echo -e "The worker is registered in launchd and will reconnect on boot. Confirm that it joined with:"
    echo -e "  sudo launchctl print system/com.oxideswarm.worker   and   tail -f /var/log/oxideswarm/worker.log"
    exit 0
fi

# Foreground Mode Execution
# Locate or compile rusty-grid binary
BIN=""
CANDIDATES=(
    "${SCRIPT_DIR}/target/release/oxideswarm"
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
    # gpu-wgpu lets the worker use this machine's GPU; OXIDESWARM_CARGO_FEATURES="" builds without it.
    BUILD_FEATURES="${OXIDESWARM_CARGO_FEATURES-gpu-wgpu}"
    cargo build --release --bin rusty-grid ${BUILD_FEATURES:+--features "$BUILD_FEATURES"}
    BIN="${SCRIPT_DIR}/target/release/rusty-grid"
fi

echo -e "\n${BOLD}${GREEN}Starting OxideSwarm Worker...${NC}"
echo -e "${CYAN}Node Name        : ${WORKER_NAME}${NC}"
echo -e "${CYAN}Config Path      : ${CONFIG_FILE}${NC}"
echo -e "${CYAN}P2P NAT Traversal: iroh QUIC + relay (needs outbound Internet)${NC}"
echo -e "${CYAN}Reconnect        : automatic, with backoff${NC}"
echo -e "${YELLOW}Press Ctrl+C to terminate the worker.${NC}\n"

exec "$BIN" worker --config "$CONFIG_FILE"
