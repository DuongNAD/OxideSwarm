#!/data/data/com.termux/files/usr/bin/bash
# ==============================================================================
# OxideSwarm Worker Background Daemon & Supervisor for Termux
#
# Holds CPU wake-lock, supervises the rusty-grid worker process, restarts on
# transient failures, and manages logging.
#
# Where the worker connects is decided by the config file
# (~/.config/oxideswarm/rusty-grid.toml: master = "HOST:8088" or p2p_ticket = '...').
# --master overrides it for one run. The master's CLUSTER port is 8088; 8080 is
# its dashboard and never a valid worker target.
#
# The worker also needs the master's cluster secret (default
# ~/.oxideswarm/cluster.secret, or --secret-file). Copy it from the master:
#   scp user@master:~/.oxideswarm/cluster.secret ~/.oxideswarm/cluster.secret
#   chmod 600 ~/.oxideswarm/cluster.secret
# ==============================================================================

set -uo pipefail

# ANSI color codes for terminal output
RED="$(printf '\033[0;31m')"
GREEN="$(printf '\033[0;32m')"
YELLOW="$(printf '\033[1;33m')"
BLUE="$(printf '\033[0;34m')"
CYAN="$(printf '\033[0;36m')"
BOLD="$(printf '\033[1m')"
NC="$(printf '\033[0m')"

# Working paths
CONFIG_DIR="${HOME}/.config/oxideswarm"
CONFIG_FILE="${CONFIG_DIR}/rusty-grid.toml"
STATE_DIR="${HOME}/.local/state/oxideswarm"
PID_FILE="${STATE_DIR}/worker.pid"            # the supervisor (this script)
CHILD_PID_FILE="${STATE_DIR}/worker.child.pid" # the rusty-grid worker it supervises
LOG_FILE="${STATE_DIR}/worker.log"
SANDBOX_DIR="${HOME}/.cache/oxideswarm/sandboxes"

mkdir -p "${CONFIG_DIR}" "${STATE_DIR}" "${SANDBOX_DIR}"

log_info() { echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') $*"; }
log_ok()   { echo -e "${GREEN}[OK]${NC}   $(date '+%Y-%m-%d %H:%M:%S') $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') $*"; }
log_err()  { echo -e "${RED}[ERR]${NC}  $(date '+%Y-%m-%d %H:%M:%S') $*" >&2; }

# Default parameters (empty = let the config file decide)
MASTER_ADDR=""
WORKER_NAME=""
CORES=""
RAM_MB=""
SECRET_FILE=""
SIMULATE_GPU=false
INSECURE=false
MODE="foreground"

usage() {
    cat <<EOF
${BOLD}OxideSwarm Termux Worker Supervisor${NC}

${BOLD}Usage:${NC} $(basename "$0") [COMMAND] [OPTIONS]

${BOLD}Commands:${NC}
  --daemon              Run worker silently in background with auto-restart
  --foreground          Run worker in foreground with live console logs (default)
  --status              Check worker daemon status, PID, and resource telemetry
  --stop                Gracefully terminate the supervisor AND its worker process, release wake-lock
  --restart             Restart background worker daemon
  --logs                Stream live daemon logs (tail -f)
  -h, --help            Show this help message

${BOLD}Options:${NC}
  --master <ADDR>       Master CLUSTER address HOST:8088 (default: from ${CONFIG_FILE};
                        8080 is the dashboard, not a valid target)
  --secret-file <PATH>  Cluster secret copied from the master (default: ~/.oxideswarm/cluster.secret)
  --insecure            Follow a remote master WITHOUT a cluster secret (not recommended)
  --name <NAME>         Worker node identifier advertised to Master
  --cores <N>           Limit CPU cores allocated
  --ram-mb <MB>         Override advertised RAM in Megabytes
  --simulate-gpu        Advertise simulated GPU capabilities (testing only: GPU tasks will then
                        be routed to this phone and run on the CPU)
EOF
    exit 0
}

# Locate rusty-grid binary
locate_binary() {
    if [ -x "${PREFIX:-/data/data/com.termux/files/usr}/bin/rusty-grid" ]; then
        echo "${PREFIX:-/data/data/com.termux/files/usr}/bin/rusty-grid"
    elif [ -x "${HOME}/bin/rusty-grid" ]; then
        echo "${HOME}/bin/rusty-grid"
    elif command -v rusty-grid >/dev/null 2>&1; then
        command -v rusty-grid
    elif [ -x "./rusty-grid" ]; then
        echo "./rusty-grid"
    else
        log_err "Could not locate executable 'rusty-grid' binary."
        log_err "Run 'install_termux_daemon.sh' to install it to \$PREFIX/bin/."
        exit 1
    fi
}

acquire_wake_lock() {
    if command -v termux-wake-lock >/dev/null 2>&1; then
        termux-wake-lock
        log_ok "Acquired Android CPU wake-lock via termux-wake-lock."
    else
        log_warn "'termux-wake-lock' not found. Screen-off Doze mode may suspend this worker."
        log_warn "Install termux-api via: pkg install termux-api"
    fi
}

release_wake_lock() {
    if command -v termux-wake-unlock >/dev/null 2>&1; then
        termux-wake-unlock
        log_info "Released Android CPU wake-lock via termux-wake-unlock."
    fi
}

rotate_logs_if_needed() {
    if [ -f "$LOG_FILE" ]; then
        local size_bytes
        size_bytes=$(wc -c < "$LOG_FILE" 2>/dev/null || stat -c %s "$LOG_FILE" 2>/dev/null || echo 0)
        # 20 MB max log size
        if [ "$size_bytes" -gt 20971520 ]; then
            mv "$LOG_FILE" "${LOG_FILE}.old"
            log_info "Rotated worker log file (>20MB)."
        fi
    fi
}

read_pid_file() {
    if [ -f "$1" ]; then
        tr -d '[:space:]' < "$1" 2>/dev/null || true
    fi
}

# True when PID is alive and (where /proc allows the check) still one of our processes,
# so a recycled PID is never signalled.
pid_alive() {
    local pid="$1" cmdline
    [ -n "$pid" ] || return 1
    kill -0 "$pid" 2>/dev/null || return 1
    if [ -r "/proc/${pid}/cmdline" ]; then
        cmdline="$(tr '\0' ' ' < "/proc/${pid}/cmdline" 2>/dev/null || true)"
        case "$cmdline" in
            *rusty-grid*|*oxideswarm*|*start_worker*) return 0 ;;
            *) return 1 ;;
        esac
    fi
    return 0
}

wait_for_exit() {
    # wait_for_exit PID SECONDS
    local pid="$1" tenths=$(( $2 * 10 )) i=0
    while [ "$i" -lt "$tenths" ]; do
        pid_alive "$pid" || return 0
        sleep 0.1
        i=$((i + 1))
    done
    pid_alive "$pid" && return 1
    return 0
}

get_status() {
    local pid child
    pid="$(read_pid_file "$PID_FILE")"
    child="$(read_pid_file "$CHILD_PID_FILE")"
    if [ -n "$pid" ] && pid_alive "$pid"; then
        echo -e "${GREEN}${BOLD}● OxideSwarm Worker is ACTIVE${NC} (supervisor PID: ${pid}, worker PID: ${child:-starting})"

        # Print process memory & CPU if ps/proc available
        if [ -d "/proc/${pid}" ]; then
            local oom_score
            oom_score=$(cat "/proc/${pid}/oom_score_adj" 2>/dev/null || echo "unknown")
            echo -e "  - OOM Score Adjustment: ${oom_score}"
        fi

        # Battery info if termux-battery-status available
        if command -v termux-battery-status >/dev/null 2>&1; then
            local battery_info
            battery_info=$(termux-battery-status 2>/dev/null || echo "")
            if [ -n "$battery_info" ]; then
                local percentage temp
                percentage=$(echo "$battery_info" | grep -o '"percentage": [0-9]*' | awk '{print $2}' || echo "?")
                temp=$(echo "$battery_info" | grep -o '"temperature": [0-9.]*' | awk '{print $2}' || echo "?")
                echo -e "  - Device Battery: ${percentage}% | Temperature: ${temp}°C"
            fi
        fi

        echo -e "  - Log File: ${LOG_FILE}"
        echo -e "  - Config:   ${CONFIG_FILE}"
        return 0
    elif [ -n "$pid" ]; then
        echo -e "${YELLOW}${BOLD}● OxideSwarm Worker is NOT running${NC} (Stale PID file found: ${pid})"
        rm -f "$PID_FILE"
        if [ -n "$child" ] && pid_alive "$child"; then
            echo -e "${RED}  ! An orphaned worker process (PID ${child}) is still alive; run '$(basename "$0") --stop' to remove it.${NC}"
        fi
        return 1
    else
        echo -e "${RED}${BOLD}● OxideSwarm Worker is STOPPED${NC}"
        return 3
    fi
}

stop_worker() {
    local pid child
    pid="$(read_pid_file "$PID_FILE")"
    child="$(read_pid_file "$CHILD_PID_FILE")"

    if [ -n "$pid" ] && pid_alive "$pid"; then
        log_info "Stopping supervisor (PID $pid); it forwards the stop to the worker (PID ${child:-?})..."
        kill -TERM "$pid" 2>/dev/null || true
        if ! wait_for_exit "$pid" 10; then
            log_warn "Supervisor $pid did not exit in 10s; sending SIGKILL."
            kill -KILL "$pid" 2>/dev/null || true
        fi
    elif [ -n "$pid" ]; then
        log_warn "Stale supervisor PID file found ($pid). Removing."
    fi

    # Whatever happened to the supervisor, the worker process must not outlive it
    # (a SIGKILLed supervisor cannot clean up after itself).
    if [ -n "$child" ] && pid_alive "$child"; then
        log_info "Stopping worker process (PID $child)..."
        kill -TERM "$child" 2>/dev/null || true
        if ! wait_for_exit "$child" 5; then
            log_warn "Worker $child did not exit in 5s; sending SIGKILL."
            kill -KILL "$child" 2>/dev/null || true
        fi
    fi

    if [ -n "$pid" ] || [ -n "$child" ]; then
        rm -f "$PID_FILE" "$CHILD_PID_FILE"
        log_ok "OxideSwarm worker stopped."
    else
        log_info "No active worker PID file found."
    fi

    release_wake_lock
}

# Parse command line options
while [[ $# -gt 0 ]]; do
    case "$1" in
        --daemon)
            MODE="daemon"
            shift
            ;;
        --foreground)
            MODE="foreground"
            shift
            ;;
        --status)
            get_status
            exit $?
            ;;
        --stop)
            stop_worker
            exit 0
            ;;
        --restart)
            stop_worker
            sleep 1
            MODE="daemon"
            shift
            ;;
        --logs)
            if [ -f "$LOG_FILE" ]; then
                exec tail -n 50 -f "$LOG_FILE"
            else
                log_err "Log file not found at ${LOG_FILE}"
                exit 1
            fi
            ;;
        --master)
            MASTER_ADDR="${2:?--master needs HOST:8088}"
            shift 2
            ;;
        --secret-file)
            SECRET_FILE="${2:?--secret-file needs a PATH}"
            shift 2
            ;;
        --insecure)
            INSECURE=true
            shift
            ;;
        --name)
            WORKER_NAME="${2:?--name needs a value}"
            shift 2
            ;;
        --cores)
            CORES="${2:?--cores needs a value}"
            shift 2
            ;;
        --ram-mb)
            RAM_MB="${2:?--ram-mb needs a value}"
            shift 2
            ;;
        --simulate-gpu)
            SIMULATE_GPU=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            log_err "Unknown argument: $1"
            usage
            ;;
    esac
done

BINARY=$(locate_binary)

# The config file decides the target unless --master was given.
config_has_target() {
    [ -f "$CONFIG_FILE" ] && grep -Eq '^[[:space:]]*(master|p2p_ticket)[[:space:]]*=' "$CONFIG_FILE"
}
if [ -z "$MASTER_ADDR" ] && ! config_has_target; then
    log_err "No master configured. Set master = \"HOST:8088\" (or p2p_ticket) in ${CONFIG_FILE},"
    log_err "or pass --master HOST:8088. Use the master's CLUSTER port 8088, not the dashboard port 8080."
    exit 1
fi
case "$MASTER_ADDR" in
    *:8080)
        log_warn "Port 8080 is the master's DASHBOARD. Workers connect to the cluster port (normally 8088)."
        ;;
esac

# If default name not supplied, build one from Android device model
if [ -z "$WORKER_NAME" ]; then
    DEVICE_MODEL=""
    if command -v getprop >/dev/null 2>&1; then
        DEVICE_MODEL=$(getprop ro.product.model 2>/dev/null | tr ' ' '_' | tr -cd '[:alnum:]_-' || true)
    fi
    if [ -z "$DEVICE_MODEL" ]; then
        DEVICE_MODEL="termux-$(hostname || echo $$)"
    fi
    WORKER_NAME="android-${DEVICE_MODEL}"
fi

# Assemble worker command arguments. Nothing is passed unless it was asked for, so the config
# file keeps control (command-line flags would override it).
WORKER_ARGS=("worker" "--name" "${WORKER_NAME}" "--sandbox-base-dir" "${SANDBOX_DIR}")

if [ -n "$MASTER_ADDR" ]; then
    WORKER_ARGS+=("--master" "${MASTER_ADDR}")
fi

if [ -n "$CORES" ]; then
    WORKER_ARGS+=("--cores" "${CORES}")
fi

if [ -n "$RAM_MB" ]; then
    WORKER_ARGS+=("--ram-mb" "${RAM_MB}")
fi

if [ "$SIMULATE_GPU" = true ]; then
    WORKER_ARGS+=("--simulate-gpu")
fi

if [ -n "$SECRET_FILE" ]; then
    WORKER_ARGS+=("--secret-file" "${SECRET_FILE}")
fi

if [ "$INSECURE" = true ]; then
    WORKER_ARGS+=("--insecure")
fi

if [ -f "$CONFIG_FILE" ]; then
    WORKER_ARGS+=("--config" "${CONFIG_FILE}")
fi

# Supervision loop function
run_supervision_loop() {
    acquire_wake_lock

    CHILD_PID=""
    SHUTTING_DOWN=false

    # Forward the stop to the worker: a plain `exit` would leave the child running, and bash only
    # runs a trap after a foreground child ends, so the worker runs in the background and is
    # `wait`ed for (wait is interrupted by a trapped signal).
    on_stop_signal() {
        SHUTTING_DOWN=true
        log_info "Stop requested; terminating worker loop..."
        if [ -n "$CHILD_PID" ] && kill -0 "$CHILD_PID" 2>/dev/null; then
            kill -TERM "$CHILD_PID" 2>/dev/null || true
            wait_for_exit "$CHILD_PID" 5 || kill -KILL "$CHILD_PID" 2>/dev/null || true
        fi
        rm -f "$CHILD_PID_FILE"
        release_wake_lock
        exit 0
    }
    trap on_stop_signal INT TERM HUP

    local consecutive_failures=0
    local max_backoff=30

    log_info "Starting OxideSwarm Worker Supervision Loop"
    log_info "Binary:       ${BINARY}"
    log_info "Master:       ${MASTER_ADDR:-<from ${CONFIG_FILE}>}"
    log_info "Worker Name:  ${WORKER_NAME}"
    log_info "Command:      ${BINARY} ${WORKER_ARGS[*]}"

    while true; do
        rotate_logs_if_needed
        local start_ts
        start_ts=$(date +%s)

        log_ok "Spawning worker engine process..."
        "$BINARY" "${WORKER_ARGS[@]}" &
        CHILD_PID=$!
        echo "$CHILD_PID" > "$CHILD_PID_FILE"
        wait "$CHILD_PID"
        local exit_code=$?
        CHILD_PID=""
        rm -f "$CHILD_PID_FILE"

        local end_ts
        end_ts=$(date +%s)
        local runtime=$((end_ts - start_ts))

        log_warn "Worker process exited with code ${exit_code} (ran for ${runtime}s)."

        # If it ran for more than 60 seconds, treat previous failures as resolved
        if [ $runtime -gt 60 ]; then
            consecutive_failures=0
        else
            consecutive_failures=$((consecutive_failures + 1))
        fi

        # Compute backoff
        local backoff=$((consecutive_failures * 3))
        if [ $backoff -gt $max_backoff ]; then
            backoff=$max_backoff
        fi
        if [ $backoff -eq 0 ]; then
            backoff=2
        fi

        log_info "Restarting worker in ${backoff} seconds (failure count: ${consecutive_failures})..."
        sleep "$backoff" &
        wait $!
    done
}

if [ "$MODE" = "daemon" ]; then
    # Check if already running
    EXISTING_PID="$(read_pid_file "$PID_FILE")"
    if [ -n "$EXISTING_PID" ] && pid_alive "$EXISTING_PID"; then
        log_warn "Worker is already running (PID ${EXISTING_PID})."
        exit 0
    fi

    log_info "Launching worker in background daemon mode..."
    rotate_logs_if_needed

    # Re-run this script as the supervisor, forwarding exactly the options that were given.
    DAEMON_ARGS=("--foreground" "--name" "${WORKER_NAME}")
    if [ -n "$MASTER_ADDR" ]; then DAEMON_ARGS+=("--master" "${MASTER_ADDR}"); fi
    if [ -n "$CORES" ]; then DAEMON_ARGS+=("--cores" "${CORES}"); fi
    if [ -n "$RAM_MB" ]; then DAEMON_ARGS+=("--ram-mb" "${RAM_MB}"); fi
    if [ -n "$SECRET_FILE" ]; then DAEMON_ARGS+=("--secret-file" "${SECRET_FILE}"); fi
    if [ "$SIMULATE_GPU" = true ]; then DAEMON_ARGS+=("--simulate-gpu"); fi
    if [ "$INSECURE" = true ]; then DAEMON_ARGS+=("--insecure"); fi

    nohup "$0" "${DAEMON_ARGS[@]}" >> "${LOG_FILE}" 2>&1 &

    DAEMON_PID=$!
    echo "$DAEMON_PID" > "$PID_FILE"

    log_ok "Worker daemon spawned with PID ${DAEMON_PID}."
    log_info "Logs are streaming to: ${LOG_FILE}"
    echo "To monitor: $(basename "$0") --logs"
    echo "To check status: $(basename "$0") --status"
    echo "To stop: $(basename "$0") --stop"
    exit 0
else
    # Foreground execution
    echo "$$" > "$PID_FILE"
    run_supervision_loop
fi
