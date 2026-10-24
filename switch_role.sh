#!/usr/bin/env bash
# ==============================================================================
# OxideSwarm Dynamic Role Switcher for macOS / Linux (Master <-> Worker on Demand)
#
#   ./switch_role.sh master [--p2p] [--dashboard-lan] [--no-worker] [--secret-file PATH] [-- MASTER_ARGS...]
#   ./switch_role.sh worker [MASTER_ADDR|auto] [--secret-file PATH] [-- WORKER_ARGS...]
#   ./switch_role.sh status
#   ./switch_role.sh stop
#
# Every shared-secret rule of the grid applies (see docs/SECURITY.md):
#   * the master listens on 0.0.0.0:8088 and, on first start, creates
#     ~/.oxideswarm/cluster.secret; workers and clients must hold the same file;
#   * the dashboard (:8080) stays on loopback unless --dashboard-lan is given;
#   * --p2p additionally publishes a P2P ticket for machines on other networks.
#
# Processes are tracked through PID files in $OXIDESWARM_HOME/run (default
# ~/.oxideswarm/run). Nothing is ever killed by a name pattern, so a master or
# worker you started by hand, or an editor with "rusty-grid" in its title, is safe.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Ports (change only if 8088/8080 are taken on this machine; workers must then use the same port).
CLUSTER_PORT="${OXIDESWARM_CLUSTER_PORT:-8088}"
DASHBOARD_PORT="${OXIDESWARM_DASHBOARD_PORT:-8080}"

# OXIDESWARM_HOME only moves this script's state (PID files, logs, ticket, secret file).
# When it is set, the secret file is passed to the CLI explicitly.
STATE_DIR="${OXIDESWARM_HOME:-${HOME}/.oxideswarm}"
RUN_DIR="${STATE_DIR}/run"
MASTER_PID_FILE="${RUN_DIR}/master.pid"
WORKER_PID_FILE="${RUN_DIR}/worker.pid"
MASTER_LOG="${RUN_DIR}/master.log"
WORKER_LOG="${RUN_DIR}/worker.log"
TICKET_FILE="${STATE_DIR}/master_ticket.txt"
SECRET_FILE="${STATE_DIR}/cluster.secret"

# Binary: OXIDESWARM_BIN, else the release build (built on demand).
BIN="${OXIDESWARM_BIN:-}"
resolve_bin() {
    if [[ -n "$BIN" ]]; then
        return 0
    fi
    local cand
    for cand in "${SCRIPT_DIR}/target/release/oxideswarm" "${SCRIPT_DIR}/target/release/rusty-grid"; do
        if [[ -x "$cand" ]]; then
            BIN="$cand"
            return 0
        fi
    done
    # The wgpu engine is what lets a worker use this machine's GPU (Metal on a Mac). Without it the
    # worker advertises no GPU. OXIDESWARM_CARGO_FEATURES="" builds without it (faster, no GPU).
    local features="${OXIDESWARM_CARGO_FEATURES-gpu-wgpu}"
    echo "[INFO] Compiling rusty-grid release binary (features: ${features:-none})..."
    cargo build --release --bin rusty-grid ${features:+--features "$features"}
    BIN="${SCRIPT_DIR}/target/release/rusty-grid"
}

detect_lan_ip() {
    local ip=""
    ip="$(ipconfig getifaddr en0 2>/dev/null || true)"
    if [[ -z "$ip" ]]; then
        ip="$(ipconfig getifaddr en1 2>/dev/null || true)"
    fi
    if [[ -z "$ip" ]]; then
        ip="$(hostname -I 2>/dev/null | awk '{print $1}' || true)"
    fi
    if [[ -z "$ip" ]]; then
        ip="<HOST_IP>"
    fi
    echo "$ip"
}
LOCAL_IP="$(detect_lan_ip)"

default_worker_name() {
    local host
    host="$(hostname -s 2>/dev/null || echo node)"
    echo "${OXIDESWARM_WORKER_NAME:-${host}-worker}"
}

# --- PID file handling --------------------------------------------------------

read_pid() {
    local pid=""
    if [[ -f "$1" ]]; then
        pid="$(tr -d '[:space:]' < "$1" 2>/dev/null || true)"
    fi
    if [[ "$pid" =~ ^[0-9]+$ ]]; then
        echo "$pid"
    fi
}

# True when the PID file names a live process that really is one of our binaries
# (guards against a recycled PID).
pid_is_ours() {
    local pid cmd
    pid="$(read_pid "$1")"
    [[ -n "$pid" ]] || return 1
    kill -0 "$pid" 2>/dev/null || return 1
    cmd="$(ps -p "$pid" -o command= 2>/dev/null || true)"
    case "$cmd" in
        *rusty-grid*|*oxideswarm*) return 0 ;;
    esac
    if [[ -n "${OXIDESWARM_BIN:-}" ]]; then
        case "$cmd" in
            *"$(basename "$OXIDESWARM_BIN")"*) return 0 ;;
        esac
    fi
    return 1
}

stop_tracked() {
    local label="$1" pidfile="$2" pid i
    if pid_is_ours "$pidfile"; then
        pid="$(read_pid "$pidfile")"
        echo "[INFO] Stopping ${label} (PID ${pid})..."
        kill -TERM "$pid" 2>/dev/null || true
        for i in 1 2 3 4 5 6 7 8 9 10; do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.5
        done
        if kill -0 "$pid" 2>/dev/null; then
            echo "[WARN] ${label} did not exit within 5s; sending SIGKILL."
            kill -KILL "$pid" 2>/dev/null || true
        fi
    fi
    rm -f "$pidfile"
}

stop_all() {
    stop_tracked "worker" "$WORKER_PID_FILE"
    stop_tracked "master" "$MASTER_PID_FILE"
}

# Starts "$@" in the background with output appended to $2 and records its PID in $1.
start_tracked() {
    local pidfile="$1" logfile="$2"
    shift 2
    ( umask 077; mkdir -p "$RUN_DIR" )
    if [[ -f "$logfile" ]]; then
        mv -f "$logfile" "${logfile}.old" 2>/dev/null || true
    fi
    (
        umask 077
        nohup "$@" >> "$logfile" 2>&1 &
        echo $! > "$pidfile"
    )
}

show_log_tail() {
    if [[ -f "$1" ]]; then
        echo "---- last lines of $1 ----" >&2
        tail -n 15 "$1" >&2 || true
        echo "---------------------------" >&2
    fi
}

# Waits up to $2 half-seconds for the worker log $1 to show that the master accepted the
# registration. A running process is not a connected worker.
wait_registered() {
    local logfile="$1" tries="$2" i
    for i in $(seq 1 "$tries"); do
        if grep -q "Registration accepted" "$logfile" 2>/dev/null; then
            return 0
        fi
        sleep 0.5
    done
    return 1
}

# The CLI auto-loads ONE config file (RUSTY_GRID_CONFIG, ./rusty-grid.toml|json, then
# ~/.oxideswarm/rusty-grid.toml). A p2p_ticket left in it (older connect_remote.sh wrote one
# there) makes the worker dial that ticket instead of --master, so it never registers.
warn_config_ticket() {
    local cfg=""
    if [[ -n "${RUSTY_GRID_CONFIG:-}" ]]; then
        cfg="$RUSTY_GRID_CONFIG"
    elif [[ -f "rusty-grid.toml" ]]; then
        cfg="$PWD/rusty-grid.toml"
    elif [[ -f "rusty-grid.json" ]]; then
        cfg="$PWD/rusty-grid.json"
    elif [[ -f "${HOME}/.oxideswarm/rusty-grid.toml" ]]; then
        cfg="${HOME}/.oxideswarm/rusty-grid.toml"
    fi
    if [[ -n "$cfg" && -f "$cfg" ]] && grep -Eq "p2p_ticket" "$cfg" 2>/dev/null; then
        echo "[WARN] ${cfg} contains a p2p_ticket. The CLI loads that file on every run, so a worker may dial the ticket" >&2
        echo "       instead of the --master address and never register. Remove the p2p_ticket line if it is stale." >&2
    fi
}

# Describes whoever listens on a TCP port ("PID/command"), or nothing.
port_owner() {
    command -v lsof >/dev/null 2>&1 || return 0
    lsof -nP -iTCP:"$1" -sTCP:LISTEN 2>/dev/null | awk 'NR==2 {print $2 " (" $1 ")"}' || true
}

# Secret-related CLI arguments. Without OXIDESWARM_HOME the CLI's own defaults
# (OXIDESWARM_SECRET[_FILE], ~/.oxideswarm/cluster.secret) apply.
SECRET_ARGS=()
init_secret_args() {
    local explicit="${1:-}"
    SECRET_ARGS=()
    if [[ -n "$explicit" ]]; then
        SECRET_ARGS=(--secret-file "$explicit")
        SECRET_FILE="$explicit"
    elif [[ -n "${OXIDESWARM_HOME:-}" ]]; then
        SECRET_ARGS=(--secret-file "$SECRET_FILE")
    fi
}

# ------------------------------------------------------------------------------
# Roles
# ------------------------------------------------------------------------------

detect_role() {
    if pid_is_ours "$MASTER_PID_FILE"; then
        echo "MASTER (Listening on :${CLUSTER_PORT}, PID $(read_pid "$MASTER_PID_FILE"))"
    elif pid_is_ours "$WORKER_PID_FILE"; then
        echo "WORKER (Running in background, PID $(read_pid "$WORKER_PID_FILE"))"
    else
        echo "STOPPED (Not started by switch_role.sh)"
    fi
}

do_master() {
    local use_p2p=false dashboard_lan=false with_worker=true secret_override=""
    local extra=()
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --p2p) use_p2p=true; shift ;;
            --dashboard-lan) dashboard_lan=true; shift ;;
            --no-worker) with_worker=false; shift ;;
            --secret-file) secret_override="${2:?--secret-file needs a PATH}"; shift 2 ;;
            --) shift; extra=("$@"); break ;;
            *) echo "[ERROR] Unknown option for master: $1" >&2; exit 1 ;;
        esac
    done
    init_secret_args "$secret_override"
    resolve_bin

    echo ""
    echo "[INFO] Switching node role to MASTER..."
    stop_all

    local owner
    owner="$(port_owner "$CLUSTER_PORT")"
    if [[ -n "$owner" ]]; then
        echo "[ERROR] Port ${CLUSTER_PORT} is already in use by ${owner}; cannot start Master." >&2
        echo "        Stop that process (if it is a manually started Master) and retry." >&2
        exit 1
    fi

    # With a custom OXIDESWARM_HOME the secret file is created here; otherwise the master
    # creates ~/.oxideswarm/cluster.secret itself on first start.
    if [[ -n "${OXIDESWARM_HOME:-}" && ! -f "$SECRET_FILE" ]]; then
        "$BIN" secret generate --out "$SECRET_FILE"
    fi

    # The dashboard only leaves this machine when asked for. The explicit loopback address
    # keeps that true even for binaries whose default follows the --listen interface.
    local dash_addr="127.0.0.1:${DASHBOARD_PORT}"
    if [[ "$dashboard_lan" == true ]]; then
        dash_addr="0.0.0.0:${DASHBOARD_PORT}"
    fi

    local args=(master --listen "0.0.0.0:${CLUSTER_PORT}" --web-ui-addr "$dash_addr")
    if [[ "$use_p2p" == true ]]; then
        rm -f "$TICKET_FILE"
        args+=(--p2p --p2p-ticket-file "$TICKET_FILE")
    fi
    if [[ ${#SECRET_ARGS[@]} -gt 0 ]]; then
        args+=("${SECRET_ARGS[@]}")
    fi
    if [[ ${#extra[@]} -gt 0 ]]; then
        args+=("${extra[@]}")
    fi

    echo "[OK] Starting OxideSwarm Master: $(basename "$BIN") ${args[*]}"
    start_tracked "$MASTER_PID_FILE" "$MASTER_LOG" "$BIN" "${args[@]}"

    local i started=false
    for i in $(seq 1 50); do
        if ! pid_is_ours "$MASTER_PID_FILE"; then
            break
        fi
        if grep -q "RustyGrid Master started" "$MASTER_LOG" 2>/dev/null; then
            started=true
            break
        fi
        sleep 0.2
    done
    if [[ "$started" != true ]]; then
        echo "[ERROR] Master failed to start (see log tail below)." >&2
        show_log_tail "$MASTER_LOG"
        stop_tracked "master" "$MASTER_PID_FILE"
        exit 1
    fi

    local worker_note="disabled (--no-worker)"
    if [[ "$with_worker" == true ]]; then
        local wargs=(worker --master "127.0.0.1:${CLUSTER_PORT}" --name "$(default_worker_name)")
        if [[ ${#SECRET_ARGS[@]} -gt 0 ]]; then
            wargs+=("${SECRET_ARGS[@]}")
        fi
        warn_config_ticket
        start_tracked "$WORKER_PID_FILE" "$WORKER_LOG" "$BIN" "${wargs[@]}"
        sleep 1
        if ! pid_is_ours "$WORKER_PID_FILE"; then
            worker_note="FAILED to start, see ${WORKER_LOG}"
            show_log_tail "$WORKER_LOG"
        elif wait_registered "$WORKER_LOG" 10; then
            worker_note="running and REGISTERED with Master (PID $(read_pid "$WORKER_PID_FILE")), log: ${WORKER_LOG}"
        else
            worker_note="running but NOT YET registered after 5s, see ${WORKER_LOG}"
            show_log_tail "$WORKER_LOG"
        fi
    fi

    if [[ "$use_p2p" == true ]]; then
        # The master writes the ticket once its P2P endpoint is online (a few seconds).
        for i in $(seq 1 40); do
            [[ -s "$TICKET_FILE" ]] && break
            sleep 0.25
        done
    fi

    echo ""
    echo "=============================================================================="
    echo "[SUCCESS] This node is now MASTER (PID $(read_pid "$MASTER_PID_FILE"))"
    echo "=============================================================================="
    echo "  - Cluster TCP port    : ${LOCAL_IP}:${CLUSTER_PORT}  (workers connect here, NOT ${DASHBOARD_PORT})"
    if [[ "$dashboard_lan" == true ]]; then
        echo "  - Web Dashboard       : http://${LOCAL_IP}:${DASHBOARD_PORT}  (exposed on LAN)"
    else
        echo "  - Web Dashboard       : http://127.0.0.1:${DASHBOARD_PORT}  (loopback only; pass --dashboard-lan for LAN access)"
    fi
    echo "  - Dashboard Token     : $BIN secret show --dashboard-token"
    echo "  - Cluster secret      : ${SECRET_FILE}"
    echo "      Copy this file to every worker machine (scp or USB) and place it at"
    echo "      ~/.oxideswarm/cluster.secret (chmod 600). Never send via chat/e-mail."
    echo "  - Local worker        : ${worker_note}"
    echo "  - Master log          : ${MASTER_LOG}"
    if [[ "$use_p2p" == true ]]; then
        if [[ -s "$TICKET_FILE" ]]; then
            echo "  - P2P Ticket (WAN)    : $(tr -d '\r\n' < "$TICKET_FILE")"
        else
            echo "  - P2P Ticket (WAN)    : pending; will be written to ${TICKET_FILE} (requires outbound Internet to iroh relays)"
        fi
        echo "      Remote worker command: ./connect_remote.sh '<TICKET>' --secret-file <secret_path>"
    else
        echo "  - Remote networks     : re-run with --p2p to publish a WAN P2P ticket."
    fi
    echo ""
    echo "  * CONNECT FROM OTHER LAN MACHINES (after copying cluster.secret):"
    echo "      ./switch_role.sh worker ${LOCAL_IP}:${CLUSTER_PORT}      (macOS/Linux)"
    echo "      windows\\switch_role.cmd worker ${LOCAL_IP}:${CLUSTER_PORT}   (Windows)"
    echo "  * Security: authenticated TCP on :${CLUSTER_PORT} encrypts traffic post-handshake (ChaCha20-Poly1305)."
    echo "    Across untrusted WAN networks, use --p2p (QUIC) or a VPN/SSH tunnel."
    echo "=============================================================================="
    echo ""
}

do_worker() {
    local target_master="auto" secret_override="" got_target=false
    local extra=()
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --secret-file) secret_override="${2:?--secret-file needs a PATH}"; shift 2 ;;
            --) shift; extra=("$@"); break ;;
            -*) echo "[ERROR] Unknown option for worker: $1" >&2; exit 1 ;;
            *)
                if [[ "$got_target" == true ]]; then
                    echo "[ERROR] Unexpected extra argument: $1" >&2
                    exit 1
                fi
                target_master="$1"
                got_target=true
                shift
                ;;
        esac
    done
    init_secret_args "$secret_override"
    resolve_bin

    echo ""
    echo "[INFO] Switching node role to WORKER..."
    stop_all

    local name
    name="$(default_worker_name)"
    echo "[OK] Connecting ${name} to Master at ${target_master} (cluster port ${CLUSTER_PORT}; 'auto' = LAN UDP beacon)..."
    local args=(worker --master "$target_master" --name "$name")
    if [[ ${#SECRET_ARGS[@]} -gt 0 ]]; then
        args+=("${SECRET_ARGS[@]}")
    fi
    if [[ ${#extra[@]} -gt 0 ]]; then
        args+=("${extra[@]}")
    fi
    warn_config_ticket
    start_tracked "$WORKER_PID_FILE" "$WORKER_LOG" "$BIN" "${args[@]}"
    sleep 2

    if ! pid_is_ours "$WORKER_PID_FILE"; then
        echo "[ERROR] Worker exited immediately after startup (see log tail below)." >&2
        echo "        If the log reports a missing cluster secret: copy ~/.oxideswarm/cluster.secret from the Master" >&2
        echo "        (scp or USB), or pass --secret-file PATH. Only pass '-- --insecure' on a network you control." >&2
        show_log_tail "$WORKER_LOG"
        rm -f "$WORKER_PID_FILE"
        exit 1
    fi

    local reg_note
    if wait_registered "$WORKER_LOG" 20; then
        reg_note="REGISTERED with Master"
    else
        reg_note="NOT YET registered after ~12s (worker keeps retrying; check Master address :${CLUSTER_PORT} and secret)"
        show_log_tail "$WORKER_LOG"
    fi

    echo ""
    echo "=============================================================================="
    echo "[STARTED] Worker (${name}) is running, PID $(read_pid "$WORKER_PID_FILE")"
    echo "=============================================================================="
    echo "  - Target Master : ${target_master}"
    echo "  - Registration  : ${reg_note}"
    echo "  - Secret        : $( [[ -f "$SECRET_FILE" ]] && echo "$SECRET_FILE" || echo "via OXIDESWARM_SECRET[_FILE] / config" )"
    echo "  - Log           : ${WORKER_LOG}"
    echo "  - Verify        : '$BIN workers --master ${target_master}'"
    echo "  - GPU           : auto-detected when built with --features gpu-wgpu (see 'GPU:' in ${WORKER_LOG}); pass '-- --no-gpu' to disable"
    echo "=============================================================================="
    echo ""
}

do_status() {
    echo ""
    echo "=============================================================================="
    echo "                 OxideSwarm Node Status"
    echo "=============================================================================="
    echo "  Host IP       : ${LOCAL_IP} ($(hostname))"
    echo "  Current Role  : [$(detect_role)]"
    local owner
    owner="$(port_owner "$CLUSTER_PORT")"
    if [[ -n "$owner" ]]; then
        echo "  Port ${CLUSTER_PORT}     : listening (${owner})"
    fi
    echo ""
    local shown=false
    if pid_is_ours "$MASTER_PID_FILE"; then
        echo "  Master  PID $(read_pid "$MASTER_PID_FILE")  log: ${MASTER_LOG}"
        shown=true
    fi
    if pid_is_ours "$WORKER_PID_FILE"; then
        echo "  Worker  PID $(read_pid "$WORKER_PID_FILE")  log: ${WORKER_LOG}"
        shown=true
    fi
    if [[ "$shown" != true ]]; then
        echo "  No active processes tracked by switch_role.sh."
    fi
    if [[ -f "$SECRET_FILE" ]]; then
        echo "  Cluster secret: ${SECRET_FILE}"
    else
        echo "  Cluster secret: (not found at ${SECRET_FILE}; Master generates it on first start)"
    fi
    if [[ -s "$TICKET_FILE" ]]; then
        echo "  P2P Ticket    : $(tr -d '\r\n' < "$TICKET_FILE")"
    fi
    echo "=============================================================================="
    echo ""
}

do_stop() {
    echo "[INFO] Stopping OxideSwarm processes tracked by switch_role.sh..."
    stop_all
    echo "[OK] Stopped (only processes with PID files in ${RUN_DIR})."
}

usage() {
    awk 'NR >= 3 && /^# =====/ { exit } NR >= 3 { sub(/^# ?/, ""); print }' "${BASH_SOURCE[0]}"
}

# Check argument
CMD="${1:-}"
if [[ $# -gt 0 ]]; then
    shift
fi
case "$CMD" in
    master)
        do_master "$@"
        ;;
    worker)
        do_worker "$@"
        ;;
    status)
        do_status
        ;;
    stop)
        do_stop
        ;;
    -h|--help|help)
        usage
        ;;
    "")
        # Interactive Menu
        clear
        echo "=============================================================================="
        echo "              OxideSwarm Role Switcher (Node Coordinator)"
        echo "=============================================================================="
        echo "  Host IP       : ${LOCAL_IP} ($(hostname))"
        echo "  Current Role  : [$(detect_role)]"
        echo "=============================================================================="
        echo ""
        echo "  [1] Promote this machine to MASTER (Cluster coordinator; loopback dashboard by default)"
        echo "  [2] Switch this machine to WORKER (UDP 'auto' discovery or explicit Master IP:8088)"
        echo "  [3] Show current node status"
        echo "  [4] Stop processes started by this launcher"
        echo "  [5] Exit"
        echo ""
        read -r -p "Select option [1-5]: " CHOICE
        case "$CHOICE" in
            1)
                MASTER_OPTS=()
                read -r -p "Expose dashboard on LAN (--dashboard-lan)? [y/N]: " ANS
                if [[ "$ANS" =~ ^[Yy]$ ]]; then MASTER_OPTS+=(--dashboard-lan); fi
                read -r -p "Enable P2P for remote networks (--p2p)? [y/N]: " ANS
                if [[ "$ANS" =~ ^[Yy]$ ]]; then MASTER_OPTS+=(--p2p); fi
                if [[ ${#MASTER_OPTS[@]} -gt 0 ]]; then
                    do_master "${MASTER_OPTS[@]}"
                else
                    do_master
                fi
                ;;
            2)
                read -r -p "Enter Master address IP:8088 [Press Enter for 'auto' discovery]: " CUSTOM_IP
                do_worker "${CUSTOM_IP:-auto}"
                ;;
            3) do_status ;;
            4) do_stop ;;
            5) exit 0 ;;
            *) echo "Invalid option." ;;
        esac
        ;;
    *)
        echo "[ERROR] Unknown command: $CMD" >&2
        usage >&2
        exit 1
        ;;
esac
