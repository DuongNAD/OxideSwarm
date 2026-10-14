#!/usr/bin/env bash
# ==============================================================================
# run_cluster_benchmark.sh
#
# Two-node distributed benchmark: parallel chunk hashing and dense matrix
# multiplication, each run on node A alone, node B alone, and on both together.
#
#   node A : a worker process on this machine
#   node B : either a second worker process on this machine (default, hardware free)
#            or an Android phone reached through adb (--adb-id SERIAL)
#
# The script starts its OWN master and workers (private HOME, tracked PIDs) and never
# signals anything it did not start. All figures in the reports are computed from the
# raw per-task results saved in <output-dir>/raw_logs; a run exits non-zero and the
# report says FAIL when any task result is missing or wrong (wrong digest, wrong
# matrix element, non-zero exit). Speedups are measurements on this host and network,
# not a claim about any other setup, and they are NOT reproduced in CI.
#
# Phone mode needs: adb, a USB-debugging phone on the same network as this machine,
# an aarch64-linux-android rusty-grid binary (--phone-bin), and this machine's LAN IP
# (--lan-ip, auto-detected when possible). The cluster secret is pushed to the phone.
# ==============================================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC} $*"; }
log_ok()      { echo -e "${GREEN}[OK]${NC} $*"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_err()     { echo -e "${RED}[ERROR]${NC} $*" >&2; }
log_section() { echo -e "\n${CYAN}${BOLD}======================================================================${NC}\n${CYAN}${BOLD} $*${NC}\n${CYAN}${BOLD}======================================================================${NC}\n"; }

# ------------------------------------------------------------------------------
# 1. CLI options & defaults (no device serials or addresses are baked in)
# ------------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADB_ID="${ADB_ID:-}"
LAN_IP=""
PHONE_BIN=""
CLI_BIN=""
OUTPUT_DIR="${OUTPUT_DIR:-./test_reports}"
WORKLOAD="all"              # "hash", "matrix", or "all"
HASH_CHUNKS=10
HASH_CHUNK_SIZE_MB=10
MATRIX_TILES=10
MATRIX_DIM=60

print_help() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Two-node distributed benchmark for OxideSwarm (hash + matrix workloads).

Options:
  --adb-id <SERIAL>     Use this adb device as node B (default: a second local worker; also ADB_ID)
  --phone-bin <PATH>    aarch64-linux-android rusty-grid to push (default: target/aarch64-linux-android/release/rusty-grid)
  --lan-ip <IP>         This machine's LAN IP as seen by the phone (default: auto-detect; required if detection fails)
  --cli-bin <PATH>      rusty-grid binary (default: target/release/rusty-grid, else target/debug/rusty-grid)
  --output-dir <DIR>    Directory for emitted reports & logs (default: ./test_reports)
  --workload <TYPE>     'hash', 'matrix', or 'all' (default: all)
  --chunks <N>          Number of hash chunks (default: 10)
  --chunk-size <MB>     Megabytes per hash chunk (default: 10)
  --matrix-tiles <N>    Number of matrix tiles (default: 10)
  --matrix-dim <N>      Matrix dimension N for NxN tiles (default: 60)
  -h, --help            Print this help message and exit
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --adb-id)       ADB_ID="$2"; shift 2 ;;
        --phone-bin)    PHONE_BIN="$2"; shift 2 ;;
        --lan-ip)       LAN_IP="$2"; shift 2 ;;
        --cli-bin)      CLI_BIN="$2"; shift 2 ;;
        --output-dir)   OUTPUT_DIR="$2"; shift 2 ;;
        --workload)     WORKLOAD="$2"; shift 2 ;;
        --chunks)       HASH_CHUNKS="$2"; shift 2 ;;
        --chunk-size)   HASH_CHUNK_SIZE_MB="$2"; shift 2 ;;
        --matrix-tiles) MATRIX_TILES="$2"; shift 2 ;;
        --matrix-dim)   MATRIX_DIM="$2"; shift 2 ;;
        -h|--help)      print_help; exit 0 ;;
        *) log_err "Unknown option: $1"; print_help; exit 1 ;;
    esac
done

case "$WORKLOAD" in hash|matrix|all) ;; *) log_err "--workload must be hash, matrix or all"; exit 1 ;; esac
command -v python3 >/dev/null 2>&1 || { log_err "python3 is required (analysis and report generation)."; exit 1; }

if [[ -z "$CLI_BIN" ]]; then
    for cand in "${SCRIPT_DIR}/target/release/rusty-grid" "${SCRIPT_DIR}/target/debug/rusty-grid"; do
        if [[ -x "$cand" ]]; then CLI_BIN="$cand"; break; fi
    done
fi
[[ -n "$CLI_BIN" && -x "$CLI_BIN" ]] || { log_err "rusty-grid binary not found. Run 'cargo build --release --bin rusty-grid' or pass --cli-bin."; exit 1; }
CLI_BIN="$(cd "$(dirname "$CLI_BIN")" && pwd)/$(basename "$CLI_BIN")"

PHONE_MODE=false
ADB=""
if [[ -n "$ADB_ID" ]]; then
    PHONE_MODE=true
    for cand in "${ADB_PATH:-}" "${ANDROID_HOME:-}/platform-tools/adb" "${ANDROID_SDK_ROOT:-}/platform-tools/adb" "$HOME/Library/Android/sdk/platform-tools/adb" "$(command -v adb 2>/dev/null || true)"; do
        if [[ -n "$cand" && -x "$cand" ]]; then ADB="$cand"; break; fi
    done
    [[ -n "$ADB" ]] || { log_err "adb not found in PATH or the standard Android SDK directories."; exit 1; }
    [[ -n "$PHONE_BIN" ]] || PHONE_BIN="${SCRIPT_DIR}/target/aarch64-linux-android/release/rusty-grid"
    [[ -f "$PHONE_BIN" ]] || { log_err "Phone binary not found at ${PHONE_BIN} (build it with build_android_worker.sh, or pass --phone-bin)."; exit 1; }
    if [[ -z "$LAN_IP" ]]; then
        LAN_IP="$(ipconfig getifaddr en0 2>/dev/null || ipconfig getifaddr en1 2>/dev/null || hostname -I 2>/dev/null | awk '{print $1}' || true)"
    fi
    [[ -n "$LAN_IP" ]] || { log_err "Could not detect this machine's LAN IP; pass --lan-ip."; exit 1; }
fi

# ------------------------------------------------------------------------------
# 2. Workspace, isolated environment and process bookkeeping
# ------------------------------------------------------------------------------
WORK="$(mktemp -d "${TMPDIR:-/tmp}/oxideswarm_bench_XXXXXX")"
WORK="$(cd "$WORK" && pwd -P)"
TEST_HOME="${WORK}/home"
mkdir -p "$TEST_HOME" "$OUTPUT_DIR"
OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"
RAW_LOG_DIR="${OUTPUT_DIR}/raw_logs"
rm -rf "$RAW_LOG_DIR"
mkdir -p "$RAW_LOG_DIR"
EMPTY_CONFIG="${WORK}/empty.toml"
: > "$EMPTY_CONFIG"

GRID_ENV=(env)
for var in $(env | sed -n 's/^\(RUSTY_GRID_[A-Za-z0-9_]*\)=.*/\1/p') OXIDESWARM_SECRET OXIDESWARM_SECRET_FILE OXIDE_DEV_MODE; do
    GRID_ENV+=(-u "$var")
done
GRID_ENV+=(HOME="$TEST_HOME" USERPROFILE="$TEST_HOME" RUSTY_GRID_CONFIG="$EMPTY_CONFIG")
grid() { "${GRID_ENV[@]}" "$CLI_BIN" "$@"; }

export TELEMETRY_CSV="${OUTPUT_DIR}/telemetry_samples.csv"
export REPORT_MD="${OUTPUT_DIR}/benchmark_report.md"
export REPORT_JSON="${OUTPUT_DIR}/benchmark_data.json"

MASTER_PID=""
NODE_A_PID=""
NODE_B_PID=""
PHONE_WORKER_PID=""
TELEMETRY_PID=""

stop_pid() {  # TERM, brief wait, KILL; only for PIDs this script started
    local pid="$1" i
    [[ -n "$pid" ]] || return 0
    if kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null || true
        for i in 1 2 3 4 5 6 7 8 9 10; do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.2
        done
        kill -KILL "$pid" 2>/dev/null || true
    fi
    wait "$pid" 2>/dev/null || true
}

phone_sh() { "$ADB" -s "$ADB_ID" shell "$@"; }

stop_phone_worker() {
    if [[ "$PHONE_MODE" == true && -n "$PHONE_WORKER_PID" ]]; then
        phone_sh "kill $PHONE_WORKER_PID" >/dev/null 2>&1 || true
        PHONE_WORKER_PID=""
    fi
}

cleanup() {
    local exit_code=$?
    log_info "Stopping the processes this script started..."
    stop_pid "$TELEMETRY_PID"
    stop_phone_worker
    stop_pid "$NODE_B_PID"
    stop_pid "$NODE_A_PID"
    stop_pid "$MASTER_PID"
    if [[ $exit_code -eq 0 ]]; then
        rm -rf "$WORK"
    else
        log_warn "Benchmark did not complete (exit ${exit_code}); scratch files kept in ${WORK}"
    fi
}
trap cleanup EXIT INT TERM

now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }

# ------------------------------------------------------------------------------
# 3. Master (own instance; auth/dashboard policy as the CLI enforces it)
# ------------------------------------------------------------------------------
log_section "Phase 1: Starting master and workers"
PORT_FILE="${WORK}/master.port"
DASH_PORT_FILE="${WORK}/dashboard.port"
if [[ "$PHONE_MODE" == true ]]; then
    # Reachable by the phone => the master creates a cluster secret in the private HOME and
    # requires it; the dashboard is read with the token derived from that secret.
    MASTER_ARGS=(master --listen 0.0.0.0:0 --port-file "$PORT_FILE" --dashboard-port 0 --dashboard-port-file "$DASH_PORT_FILE")
else
    # Everything on loopback: no secret needed; --dev gives the harness loopback-only dashboard access.
    MASTER_ARGS=(master --listen 127.0.0.1:0 --port-file "$PORT_FILE" --dashboard-port 0 --dashboard-port-file "$DASH_PORT_FILE" --dev)
fi
"${GRID_ENV[@]}" "$CLI_BIN" "${MASTER_ARGS[@]}" > "${RAW_LOG_DIR}/master.log" 2>&1 &
MASTER_PID=$!
for _ in $(seq 1 100); do
    [[ -s "$PORT_FILE" && -s "$DASH_PORT_FILE" ]] && break
    kill -0 "$MASTER_PID" 2>/dev/null || { cat "${RAW_LOG_DIR}/master.log" >&2; log_err "master exited during startup"; exit 1; }
    sleep 0.1
done
[[ -s "$PORT_FILE" && -s "$DASH_PORT_FILE" ]] || { log_err "master did not publish its ports"; exit 1; }
MASTER_PORT="$(tr -d '\r\n' < "$PORT_FILE")"
DASH_PORT="$(tr -d '\r\n' < "$DASH_PORT_FILE")"
LOCAL_MASTER="127.0.0.1:${MASTER_PORT}"
WEB_API_URL="http://127.0.0.1:${DASH_PORT}"
export MASTER_ADDR="$LOCAL_MASTER" WEB_API_URL

CURL_AUTH=()
if [[ "$PHONE_MODE" == true ]]; then
    DASH_TOKEN="$(grid secret show --dashboard-token)"
    CURL_AUTH=(-H "Authorization: Bearer ${DASH_TOKEN}")
fi
api_status() {
    if [[ ${#CURL_AUTH[@]} -gt 0 ]]; then
        curl -s --max-time 3 "${CURL_AUTH[@]}" "${WEB_API_URL}/api/status"
    else
        curl -s --max-time 3 "${WEB_API_URL}/api/status"
    fi
}
log_ok "Master up: cluster ${LOCAL_MASTER}, dashboard ${WEB_API_URL} (PID ${MASTER_PID})"

# ------------------------------------------------------------------------------
# 4. Worker helpers: node A is always local; node B is a phone or a second local worker
# ------------------------------------------------------------------------------
NODE_A_NAME="bench-node-a"
NODE_B_NAME="bench-node-b"
mkdir -p "${WORK}/sandbox_a" "${WORK}/sandbox_b"

start_local_worker() {  # name sandbox logfile -> STARTED_PID
    "${GRID_ENV[@]}" "$CLI_BIN" worker --master "$LOCAL_MASTER" --name "$1" --no-gpu \
        --sandbox-base-dir "$2" > "$3" 2>&1 &
    STARTED_PID=$!
}

start_node_a() { start_local_worker "$NODE_A_NAME" "${WORK}/sandbox_a" "${RAW_LOG_DIR}/node_a_worker.log"; NODE_A_PID=$STARTED_PID; }
stop_node_a()  { stop_pid "$NODE_A_PID"; NODE_A_PID=""; }

PHONE_SECRET_REMOTE="/data/local/tmp/oxideswarm_bench.secret"
PHONE_BIN_REMOTE="/data/local/tmp/rusty-grid-bench"
prepare_phone() {
    local state
    state="$("$ADB" -s "$ADB_ID" get-state 2>/dev/null || echo offline)"
    [[ "$state" == "device" ]] || { log_err "adb device ${ADB_ID} is not ready (state: ${state})."; exit 1; }
    PHONE_MODEL="$(phone_sh getprop ro.product.model 2>/dev/null | tr -d '\r\n')"
    PHONE_OS="Android $(phone_sh getprop ro.build.version.release 2>/dev/null | tr -d '\r\n')"
    PHONE_SOC="$(phone_sh getprop ro.soc.model 2>/dev/null | tr -d '\r\n')"
    "$ADB" -s "$ADB_ID" push "$PHONE_BIN" "$PHONE_BIN_REMOTE" >/dev/null
    phone_sh "chmod 755 $PHONE_BIN_REMOTE"
    "$ADB" -s "$ADB_ID" push "${TEST_HOME}/.oxideswarm/cluster.secret" "$PHONE_SECRET_REMOTE" >/dev/null
    phone_sh "chmod 600 $PHONE_SECRET_REMOTE"
    log_ok "Phone ready: ${PHONE_MODEL} (${PHONE_OS})"
}
start_node_b() {
    if [[ "$PHONE_MODE" == true ]]; then
        PHONE_WORKER_PID="$(phone_sh "nohup $PHONE_BIN_REMOTE worker --master ${LAN_IP}:${MASTER_PORT} --name $NODE_B_NAME --secret-file $PHONE_SECRET_REMOTE > /data/local/tmp/oxideswarm_bench_worker.log 2>&1 & echo \$!" | tr -d '\r\n')"
        [[ "$PHONE_WORKER_PID" =~ ^[0-9]+$ ]] || { log_err "could not start the phone worker"; exit 1; }
    else
        start_local_worker "$NODE_B_NAME" "${WORK}/sandbox_b" "${RAW_LOG_DIR}/node_b_worker.log"
        NODE_B_PID=$STARTED_PID
    fi
}
stop_node_b() {
    if [[ "$PHONE_MODE" == true ]]; then stop_phone_worker; else stop_pid "$NODE_B_PID"; NODE_B_PID=""; fi
}

# Waits until exactly the wanted node names are Connected on the master (dashboard API).
wait_for_nodes() {  # names...
    local want="$*" i
    for i in $(seq 1 120); do
        if api_status | python3 -c '
import sys, json
want = set(sys.argv[1:])
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
have = {w.get("name") for w in d.get("workers", []) if w.get("status") == "Connected"}
sys.exit(0 if have == want else 1)' $want 2>/dev/null; then
            return 0
        fi
        sleep 0.25
    done
    log_err "timed out waiting for connected workers: ${want}"
    api_status | python3 -m json.tool >&2 || true
    return 1
}

if [[ "$PHONE_MODE" == true ]]; then prepare_phone; else PHONE_MODEL="local worker process"; PHONE_OS="$(uname -sr)"; PHONE_SOC="n/a"; fi

# Node inventory for the report: connect both once, record what the master saw, then isolate.
start_node_a
start_node_b
wait_for_nodes "$NODE_A_NAME" "$NODE_B_NAME"
api_status > "${RAW_LOG_DIR}/cluster_inventory.json"
log_ok "Both nodes registered; inventory saved."

# ------------------------------------------------------------------------------
# 5. Telemetry sampler: 1 Hz API latency (+ phone temperature/battery in phone mode)
# ------------------------------------------------------------------------------
log_section "Phase 2: Telemetry sampler"
echo "timestamp,epoch_s,s24_ap_temp_c,s24_skin_temp_c,s24_bat_temp_hal_c,s24_bat_temp_sys_c,s24_bat_pct,s24_bat_voltage_mv,s24_thermal_status,api_latency_ms" > "${TELEMETRY_CSV}"

start_telemetry_daemon() {
    (
        while true; do
            NOW_EPOCH=$(python3 -c 'import time; print(int(time.time()))')
            NOW_ISO=$(python3 -c 'import datetime; print(datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))')
            AP_T=""; SKIN_T=""; BAT_T_HAL=""; BAT_T_SYS=""; BAT_P=""; BAT_V=""; TH_STAT=""
            if [[ "$PHONE_MODE" == true ]]; then
                TEL_RAW=$("$ADB" -s "$ADB_ID" shell "dumpsys thermalservice 2>/dev/null | grep -E 'mValue=|Thermal Status:'; dumpsys battery | head -n 25 2>/dev/null" 2>/dev/null || echo "")
                AP_T=$(echo "$TEL_RAW" | grep "mName=AP" | head -n1 | sed -n 's/.*mValue=\([0-9.]*\).*/\1/p' || true)
                SKIN_T=$(echo "$TEL_RAW" | grep "mName=SKIN" | head -n1 | sed -n 's/.*mValue=\([0-9.]*\).*/\1/p' || true)
                BAT_T_HAL=$(echo "$TEL_RAW" | grep "mName=BAT" | head -n1 | sed -n 's/.*mValue=\([0-9.]*\).*/\1/p' || true)
                TH_STAT=$(echo "$TEL_RAW" | grep "Thermal Status:" | head -n1 | awk '{print $3}' || true)
                BAT_P=$(echo "$TEL_RAW" | grep -m1 "level:" | awk '{print $2}' || true)
                BAT_V=$(echo "$TEL_RAW" | awk '/^[[:space:]]*voltage:[[:space:]]*[0-9]+/ {print $2}' | head -n1 || true)
                BAT_T_SYS=$(echo "$TEL_RAW" | grep -m1 "temperature:" | awk '{printf "%.1f", $2/10}' || true)
            fi
            if [[ ${#CURL_AUTH[@]} -gt 0 ]]; then
                LAT_S=$(curl -o /dev/null -s -w '%{time_total}' "${CURL_AUTH[@]}" "${WEB_API_URL}/api/status" 2>/dev/null || echo "")
            else
                LAT_S=$(curl -o /dev/null -s -w '%{time_total}' "${WEB_API_URL}/api/status" 2>/dev/null || echo "")
            fi
            LAT_MS=""
            if [[ -n "$LAT_S" ]]; then LAT_MS=$(python3 -c "print(f'{float(\"${LAT_S}\") * 1000:.2f}')" 2>/dev/null || echo ""); fi
            echo "${NOW_ISO},${NOW_EPOCH},${AP_T},${SKIN_T},${BAT_T_HAL},${BAT_T_SYS},${BAT_P},${BAT_V},${TH_STAT},${LAT_MS}" >> "${TELEMETRY_CSV}"
            sleep 1
        done
    ) &
    TELEMETRY_PID=$!
}
start_telemetry_daemon
log_ok "1 Hz sampler running (PID ${TELEMETRY_PID})."

# ------------------------------------------------------------------------------
# 6. Workloads. Each phase saves one JSON result per task; nothing is counted here.
# ------------------------------------------------------------------------------
TIMINGS="${RAW_LOG_DIR}/timings.env"
: > "$TIMINGS"

run_batch() {  # phase_name count payload
    local phase="$1" count="$2" payload="$3" i t0 t1
    local pids=()
    t0=$(now_ms)
    for i in $(seq 1 "$count"); do
        grid submit --master "$LOCAL_MASTER" --type shell --wait --json -- "$payload" \
            > "${RAW_LOG_DIR}/${phase}_${i}.json" 2> "${RAW_LOG_DIR}/${phase}_${i}.err" &
        pids+=($!)
    done
    for i in "${pids[@]}"; do wait "$i" || true; done
    t1=$(now_ms)
    echo "${phase}_ms=$(( t1 - t0 ))" >> "$TIMINGS"
    log_ok "${phase}: ${count} tasks finished in $(( t1 - t0 )) ms"
}

# Phases: <prefix>_a (node A only), <prefix>_b (node B only), <prefix>_cluster (both).
run_three_modes() {  # prefix count payload
    local prefix="$1" count="$2" payload="$3"
    stop_node_b; sleep 1
    wait_for_nodes "$NODE_A_NAME"
    run_batch "${prefix}_a" "$count" "$payload"

    stop_node_a; start_node_b; sleep 1
    wait_for_nodes "$NODE_B_NAME"
    run_batch "${prefix}_b" "$count" "$payload"

    start_node_a
    wait_for_nodes "$NODE_A_NAME" "$NODE_B_NAME"
    # A restarted worker gets a new id: record which id belongs to which node for this phase.
    api_status > "${RAW_LOG_DIR}/${prefix}_cluster_workers.json"
    run_batch "${prefix}_cluster" "$count" "$payload"
}

TOTAL_HASH_MB=$(( HASH_CHUNKS * HASH_CHUNK_SIZE_MB ))
HASH_REF="$(python3 -c "import hashlib; print(hashlib.sha256(bytes(${HASH_CHUNK_SIZE_MB} * 1048576)).hexdigest())")"

if [[ "$WORKLOAD" == "hash" || "$WORKLOAD" == "all" ]]; then
    log_section "Phase 3: Parallel chunk hashing (${TOTAL_HASH_MB} MB in ${HASH_CHUNKS} chunks)"
    HASH_PAYLOAD='
HOST_OS=$(uname -s)
if command -v sha256sum >/dev/null 2>&1; then S=sha256sum; else S="shasum -a 256"; fi
HASH=$(dd if=/dev/zero bs=1048576 count='${HASH_CHUNK_SIZE_MB}' 2>/dev/null | $S | awk "{print \$1}")
printf "HASH_RESULT host=%s hash=%s\n" "$HOST_OS" "$HASH"
'
    run_three_modes hash "$HASH_CHUNKS" "$HASH_PAYLOAD"
fi

EXPECTED_C00="$(python3 -c "print(f'{2.0 * ${MATRIX_DIM}:.1f}')")"
if [[ "$WORKLOAD" == "matrix" || "$WORKLOAD" == "all" ]]; then
    log_section "Phase 4: Dense matrix multiplication (${MATRIX_TILES} tiles of ${MATRIX_DIM}x${MATRIX_DIM})"
    MATRIX_PAYLOAD='
HOST_OS=$(uname -s)
awk -v host="$HOST_OS" '\''BEGIN {
  N = '${MATRIX_DIM}';
  for(i=0; i<N; i++) for(j=0; j<N; j++) { a[i,j]=1.0; b[i,j]=2.0; }
  for(i=0; i<N; i++) for(j=0; j<N; j++) {
    sum=0;
    for(k=0; k<N; k++) sum += a[i,k]*b[k,j];
    c[i,j]=sum;
  }
  printf "MATRIX_RESULT host=%s flops=%d c00=%.1f\n", host, 2*N*N*N, c[0,0];
}'\'''
    run_three_modes matrix "$MATRIX_TILES" "$MATRIX_PAYLOAD"
fi

stop_pid "$TELEMETRY_PID"; TELEMETRY_PID=""

# ------------------------------------------------------------------------------
# 7. Analysis and reports: everything below is computed from the raw results
# ------------------------------------------------------------------------------
log_section "Phase 5: Verifying results and writing reports"
export WORKLOAD HASH_CHUNKS HASH_CHUNK_SIZE_MB TOTAL_HASH_MB HASH_REF MATRIX_TILES MATRIX_DIM EXPECTED_C00
export RAW_LOG_DIR OUTPUT_DIR NODE_A_NAME NODE_B_NAME PHONE_MODE PHONE_MODEL PHONE_OS PHONE_SOC
export HOST_DESC="$(uname -srm)"
export HOST_CPU="$(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ *//' || echo unknown)"

set +e
python3 - << 'PYEOF'
import csv, datetime, glob, json, os, sys

env = os.environ
raw = env["RAW_LOG_DIR"]
out = env["OUTPUT_DIR"]
workload = env["WORKLOAD"]
phone_mode = env["PHONE_MODE"] == "true"

def fnum(v, default=None):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default

timings = {}
with open(os.path.join(raw, "timings.env")) as f:
    for line in f:
        if "=" in line:
            k, v = line.strip().split("=", 1)
            timings[k] = int(v)

def load_phase(phase, expected_count):
    results = []
    for i in range(1, expected_count + 1):
        p = os.path.join(raw, f"{phase}_{i}.json")
        try:
            with open(p) as f:
                results.append(json.load(f))
        except Exception:
            results.append(None)
    return results

def check(results, token):
    """Returns (verified_count, worker_id counter, host counter)."""
    verified = 0
    workers, hosts = {}, {}
    for r in results:
        if not r:
            continue
        wid = r.get("worker_id", "unknown")
        stdout = r.get("stdout", "")
        ok = r.get("exit_code") == 0 and token(stdout)
        if ok:
            verified += 1
            workers[wid] = workers.get(wid, 0) + 1
            for part in stdout.split():
                if part.startswith("host="):
                    hosts[part[5:]] = hosts.get(part[5:], 0) + 1
    return verified, workers, hosts

failures = []
data = {
    "timestamp": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "host": env["HOST_DESC"],
    "host_cpu": env["HOST_CPU"],
    "mode": "phone" if phone_mode else "two local worker processes",
    "node_b": {"model": env["PHONE_MODEL"], "os": env["PHONE_OS"], "soc": env["PHONE_SOC"]},
    "note": "Single run on the host above; not reproduced in CI. Speedups are measurements, not guarantees.",
    "workloads": {},
}

# Inventory (cores/RAM as the master saw them)
inv = {}
try:
    with open(os.path.join(raw, "cluster_inventory.json")) as f:
        for w in json.load(f).get("workers", []):
            inv[w.get("name")] = {"cpu_cores": w.get("cpu_cores"), "ram_mb": w.get("ram_mb")}
except Exception:
    pass
data["nodes"] = {"node_a": inv.get(env["NODE_A_NAME"], {}), "node_b": inv.get(env["NODE_B_NAME"], {})}
total_cores = sum((v.get("cpu_cores") or 0) for v in data["nodes"].values())
data["total_cores_reported"] = total_cores

def run_workload(key, prefix, count, token, unit_label, work_units):
    section = {"tasks_per_mode": count}
    for mode in ("a", "b", "cluster"):
        phase = f"{prefix}_{mode}"
        results = load_phase(phase, count)
        verified, workers, hosts = check(results, token)
        ms = timings.get(f"{phase}_ms")
        section[mode] = {
            "execution_time_ms": ms,
            "tasks_verified": verified,
            "tasks_expected": count,
            "rate": round(work_units / (ms / 1000.0), 2) if ms else None,
            "worker_task_counts": workers,
            "host_task_counts": hosts,
        }
        if verified != count:
            failures.append(f"{key}/{mode}: {verified}/{count} results verified")
    ta, tb, tc = (section[m]["execution_time_ms"] for m in ("a", "b", "cluster"))
    cluster_workers = section["cluster"]["worker_task_counts"]
    # Worker ids change on every restart, so the node of each id comes from the master's own
    # worker list captured right before the cluster run.
    ids_by_name = {}
    try:
        with open(os.path.join(raw, f"{prefix}_cluster_workers.json")) as f:
            for w in json.load(f).get("workers", []):
                ids_by_name[w.get("name")] = w.get("id")
    except Exception:
        pass
    section["cluster_distribution"] = {
        "node_a_tasks": cluster_workers.get(ids_by_name.get(env["NODE_A_NAME"]), 0),
        "node_b_tasks": cluster_workers.get(ids_by_name.get(env["NODE_B_NAME"]), 0),
    }
    if ta and tb and tc:
        section["speedup_vs_node_b"] = round(tb / tc, 2)
        section["speedup_vs_node_a"] = round(ta / tc, 2)
        ideal = (ta * tb) / (ta + tb)
        section["harmonic_ideal_time_ms"] = round(ideal, 2)
        section["parallel_efficiency"] = round(ideal / tc, 2)
    section["rate_unit"] = unit_label
    data["workloads"][key] = section

if workload in ("hash", "all"):
    ref = env["HASH_REF"]
    run_workload("parallel_chunk_hashing", "hash", int(env["HASH_CHUNKS"]),
                 lambda s: f"hash={ref}" in s, "MB/s", int(env["TOTAL_HASH_MB"]))
    data["workloads"]["parallel_chunk_hashing"]["expected_sha256_per_chunk"] = ref
    data["workloads"]["parallel_chunk_hashing"]["chunk_size_mb"] = int(env["HASH_CHUNK_SIZE_MB"])

if workload in ("matrix", "all"):
    dim = int(env["MATRIX_DIM"])
    c00 = env["EXPECTED_C00"]
    flops_tile = 2 * dim ** 3
    tiles = int(env["MATRIX_TILES"])
    run_workload("distributed_dense_matrix_multiplication", "matrix", tiles,
                 lambda s: f"c00={c00}" in s, "MFLOPS", (flops_tile * tiles) / 1e6)
    data["workloads"]["distributed_dense_matrix_multiplication"]["matrix_dimension"] = dim
    data["workloads"]["distributed_dense_matrix_multiplication"]["expected_c00"] = float(c00)

# Telemetry
rows = []
csv_path = env["TELEMETRY_CSV"]
if os.path.exists(csv_path):
    with open(csv_path) as f:
        rows = list(csv.DictReader(f))
lat = [fnum(r.get("api_latency_ms")) for r in rows]
lat = [x for x in lat if x is not None and x > 0]
th = [fnum(r.get("s24_thermal_status")) for r in rows]
th = [x for x in th if x is not None]
bat = [fnum(r.get("s24_bat_pct")) for r in rows]
bat = [x for x in bat if x is not None and x > 0]
telemetry = {"sample_count": len(rows), "api_latency_samples": len(lat)}
if lat:
    s = sorted(lat)
    telemetry["api_latency_ms"] = {
        "min": round(min(lat), 2), "avg": round(sum(lat) / len(lat), 2),
        "p95": round(s[min(len(s) - 1, int(len(s) * 0.95))], 2), "max": round(max(lat), 2),
        "threshold_ms": 100.0, "within_threshold": max(lat) < 100.0,
    }
if phone_mode:
    telemetry["phone_thermal_samples_above_normal"] = sum(1 for x in th if x > 0)
    if bat:
        telemetry["phone_battery_pct_first_last"] = [bat[0], bat[-1]]
data["telemetry"] = telemetry

data["verdict"] = "PASS" if not failures else "FAIL"
data["failures"] = failures

with open(env["REPORT_JSON"], "w") as f:
    json.dump(data, f, indent=2)

def row(label, sec, mode):
    s = sec[mode]
    status = "PASS" if s["tasks_verified"] == s["tasks_expected"] else "FAIL"
    return f"| {label} | {s['execution_time_ms']} ms | {s['rate']} {sec['rate_unit']} | {s['tasks_verified']}/{s['tasks_expected']} {status} |"

lines = [
    "# Two-node distributed benchmark report",
    "",
    f"- Generated: `{data['timestamp']}` on `{data['host']}` ({data['host_cpu']})",
    f"- Mode: {data['mode']}; node B: {env['PHONE_MODEL']} ({env['PHONE_OS']})",
    f"- Cores reported by the master: {total_cores}",
    f"- **Verdict: {data['verdict']}**" + ("" if not failures else " - " + "; ".join(failures)),
    f"- {data['note']}",
    "",
]
for key, sec in data["workloads"].items():
    lines += [f"## {key}", "", "| Mode | Wall clock | Rate | Verified results |", "| :--- | ---: | ---: | :--- |",
              row("Node A alone", sec, "a"), row("Node B alone", sec, "b"), row("Both nodes", sec, "cluster"), ""]
    d = sec["cluster_distribution"]
    lines.append(f"Cluster run placement: node A {d['node_a_tasks']} task(s), node B {d['node_b_tasks']} task(s).")
    if "speedup_vs_node_b" in sec:
        lines.append(f"Speedup of the cluster run: {sec['speedup_vs_node_a']}x vs node A alone, {sec['speedup_vs_node_b']}x vs node B alone; "
                     f"parallel efficiency {sec['parallel_efficiency']} (1.0 = harmonic ideal).")
    lines.append("")
lat_info = telemetry.get("api_latency_ms")
lines += ["## Telemetry", "", f"- Samples: {telemetry['sample_count']} (1 Hz)"]
if lat_info:
    lines.append(f"- Dashboard `/api/status` latency (ms): min {lat_info['min']}, avg {lat_info['avg']}, p95 {lat_info['p95']}, max {lat_info['max']}"
                 f" - {'within' if lat_info['within_threshold'] else 'ABOVE'} the {lat_info['threshold_ms']} ms threshold")
if phone_mode:
    lines.append(f"- Phone samples with thermal status above normal: {telemetry['phone_thermal_samples_above_normal']}")
with open(env["REPORT_MD"], "w") as f:
    f.write("\n".join(lines) + "\n")

print(f"[{'OK' if not failures else 'FAIL'}] verdict {data['verdict']}: {env['REPORT_MD']}")
for fl in failures:
    print(f"  - {fl}", file=sys.stderr)
sys.exit(0 if not failures else 2)
PYEOF
ANALYSIS_EXIT=$?
set -e

if [[ $ANALYSIS_EXIT -ne 0 ]]; then
    log_err "Benchmark FAILED verification (see ${REPORT_MD} and ${RAW_LOG_DIR})."
    exit "$ANALYSIS_EXIT"
fi

log_ok "Benchmark complete. Reports:"
log_ok "  - Markdown : ${REPORT_MD}"
log_ok "  - JSON     : ${REPORT_JSON}"
log_ok "  - Telemetry: ${TELEMETRY_CSV}"
echo
cat "${REPORT_MD}"
