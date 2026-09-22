#!/data/data/com.termux/files/usr/bin/bash
# ==============================================================================
# OxideSwarm Worker Background Daemon & Supervisor for Termux
#
# Holds CPU wake-lock, supervises the rusty-grid worker process, restarts on
# transient failures, and manages logging.
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
PID_FILE="${STATE_DIR}/worker.pid"
LOG_FILE="${STATE_DIR}/worker.log"
SANDBOX_DIR="${HOME}/.cache/oxideswarm/sandboxes"

mkdir -p "${CONFIG_DIR}" "${STATE_DIR}" "${SANDBOX_DIR}"

log_info() { echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') $*"; }
log_ok()   { echo -e "${GREEN}[OK]${NC}   $(date '+%Y-%m-%d %H:%M:%S') $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') $*"; }
log_err()  { echo -e "${RED}[ERR]${NC}  $(date '+%Y-%m-%d %H:%M:%S') $*" >&2; }

# Default parameters
MASTER_ADDR="127.0.0.1:8080"
WORKER_NAME=""
CORES=""
RAM_MB=""
SIMULATE_GPU=false
MODE="foreground"

usage() {
    cat <<EOF
${BOLD}OxideSwarm Termux Worker Supervisor${NC}

${BOLD}Usage:${NC} $(basename "$0") [COMMAND] [OPTIONS]

${BOLD}Commands:${NC}
  --daemon              Run worker silently in background with auto-restart
  --foreground          Run worker in foreground with live console logs (default)
  --status              Check worker daemon status, PID, and resource telemetry
  --stop                Gracefully terminate background worker and release wake-lock
  --restart             Restart background worker daemon
  --logs                Stream live daemon logs (tail -f)
  -h, --help            Show this help message

${BOLD}Options:${NC}
  --master <ADDR>       Master coordinator address (default: 127.0.0.1:8080 or config file)
  --name <NAME>         Worker node identifier advertised to Master
  --cores <N>           Limit CPU cores allocated
  --ram-mb <MB>         Override advertised RAM in Megabytes
  --simulate-gpu        Advertise simulated GPU capabilities
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

get_status() {
    if [ -f "$PID_FILE" ]; then
        local pid
        pid=$(cat "$PID_FILE" 2>/dev/null || echo "")
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo -e "${GREEN}${BOLD}● OxideSwarm Worker is ACTIVE${NC} (PID: ${pid})"
            
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
        else
            echo -e "${YELLOW}${BOLD}● OxideSwarm Worker is NOT running${NC} (Stale PID file found: ${pid})"
            rm -f "$PID_FILE"
            return 1
        fi
    else
        echo -e "${RED}${BOLD}● OxideSwarm Worker is STOPPED${NC}"
        return 3
    fi
}

stop_worker() {
    if [ -f "$PID_FILE" ]; then
        local pid
        pid=$(cat "$PID_FILE" 2>/dev/null || echo "")
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            log_info "Stopping worker process (PID $pid)..."
            kill -TERM "$pid" 2>/dev/null || true
            
            # Wait up to 5 seconds for graceful shutdown
            for _ in $(seq 1 10); do
                if ! kill -0 "$pid" 2>/dev/null; then
                    break
                fi
                sleep 0.5
            done

            if kill -0 "$pid" 2>/dev/null; then
                log_warn "Process $pid did not exit in 5s; sending SIGKILL..."
                kill -KILL "$pid" 2>/dev/null || true
            fi
            rm -f "$PID_FILE"
            log_ok "OxideSwarm worker stopped successfully."
        else
            log_warn "Stale PID file found ($pid). Removing."
            rm -f "$PID_FILE"
        fi
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
            MASTER_ADDR="$2"
            shift 2
            ;;
        --name)
            WORKER_NAME="$2"
            shift 2
            ;;
        --cores)
            CORES="$2"
            shift 2
            ;;
        --ram-mb)
            RAM_MB="$2"
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

# Assemble worker command arguments
WORKER_ARGS=("worker" "--master" "${MASTER_ADDR}" "--name" "${WORKER_NAME}" "--sandbox-base-dir" "${SANDBOX_DIR}")

if [ -n "$CORES" ]; then
    WORKER_ARGS+=("--cores" "${CORES}")
fi

if [ -n "$RAM_MB" ]; then
    WORKER_ARGS+=("--ram-mb" "${RAM_MB}")
fi

if [ "$SIMULATE_GPU" = true ]; then
    WORKER_ARGS+=("--simulate-gpu")
fi

if [ -f "$CONFIG_FILE" ]; then
    WORKER_ARGS+=("--config" "${CONFIG_FILE}")
fi

# Supervision loop function
run_supervision_loop() {
    acquire_wake_lock

    # Setup trap to clean up wake-lock and child on termination
    trap 'log_info "Interrupted; terminating worker loop..."; release_wake_lock; exit 0' SIGINT SIGTERM SIGHUP

    local consecutive_failures=0
    local max_backoff=30

    log_info "Starting OxideSwarm Worker Supervision Loop"
    log_info "Binary:       ${BINARY}"
    log_info "Master:       ${MASTER_ADDR}"
    log_info "Worker Name:  ${WORKER_NAME}"
    log_info "Command:      ${BINARY} ${WORKER_ARGS[*]}"

    while true; do
        rotate_logs_if_needed
        local start_ts
        start_ts=$(date +%s)

        log_ok "Spawning worker engine process..."
        set +e
        "$BINARY" "${WORKER_ARGS[@]}"
        local exit_code=$?
        set -e

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
        sleep "$backoff"
    done
}

if [ "$MODE" = "daemon" ]; then
    # Check if already running
    if [ -f "$PID_FILE" ]; then
        EXISTING_PID=$(cat "$PID_FILE" 2>/dev/null || echo "")
        if [ -n "$EXISTING_PID" ] && kill -0 "$EXISTING_PID" 2>/dev/null; then
            log_warn "Worker is already running (PID ${EXISTING_PID})."
            exit 0
        fi
    fi

    log_info "Launching worker in background daemon mode..."
    rotate_logs_if_needed

    # Run supervision loop under nohup
    nohup "$0" --foreground --master "${MASTER_ADDR}" --name "${WORKER_NAME}" \
        ${CORES:+--cores "$CORES"} ${RAM_MB:+--ram-mb "$RAM_MB"} \
        ${SIMULATE_GPU:+--simulate-gpu} \
        >> "${LOG_FILE}" 2>&1 &

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
