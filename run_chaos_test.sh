#!/usr/bin/env bash
# ==============================================================================
# run_chaos_test.sh — Local, hardware-free failover test for the OxideSwarm grid
#
# Starts its OWN master and two workers on this machine (loopback, ephemeral port,
# private HOME), submits long-running tasks, SIGKILLs one worker while it provably
# holds running tasks, and checks that the master notices, requeues the orphaned
# tasks onto the surviving worker, and that every task finishes with the correct
# output. It then restarts a worker and checks that it re-registers and runs a task.
#
# It needs no phone, no ADB, no LAN and touches nothing outside its temp dir: only
# the PIDs it started are ever signalled (never a pattern like "pkill rusty-grid").
#
# What a PASS means here (every item is checked, none is assumed):
#   - at least one task was running on the victim at the moment it was killed
#   - the master released the task the dead worker was running (latency is measured)
#   - every orphaned task reached "Completed" and its marker log shows it was
#     re-executed on the survivor, with the expected SHA-256 payload
#   - the replacement worker registered and ran a task
# Any failed check aborts with a non-zero exit code and no report claims a PASS.
#
# Outputs (under --output-dir):  chaos_test_report.md  chaos_test_data.json
#
# A variant with a real phone in the loop is not provided: results would have to be
# collected from the phone through the master, and the previous hard-wired version
# could print a pass without any failover having happened.
# ==============================================================================
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

log_info()  { echo -e "${BLUE}[INFO]${NC} $*"; }
log_pass()  { echo -e "${GREEN}[PASS]${NC} $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_fail()  { echo -e "${RED}[FAIL]${NC} $*" >&2; exit 1; }
log_stage() {
    echo -e "\n${BOLD}${CYAN}================================================================================${NC}"
    echo -e "${BOLD}${CYAN}  $*${NC}"
    echo -e "${BOLD}${CYAN}================================================================================${NC}"
}

now_ms() {
    python3 -c "import time; print(int(time.time() * 1000))"
}

iso_utc() {
    date -u +"%Y-%m-%dT%H:%M:%SZ"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-${SCRIPT_DIR}/test_reports}"
TASKS_COUNT=6
WORKLOAD_DURATION=6
CLI_BIN=""

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  --tasks-count <N>     Number of long-running tasks to inject (default: ${TASKS_COUNT}, min 2)
  --duration <SECS>     Seconds each task sleeps, i.e. how long the kill window is (default: ${WORKLOAD_DURATION})
  --output-dir <DIR>    Directory for the reports (default: ${OUTPUT_DIR})
  --cli-bin <PATH>      rusty-grid binary (default: target/release/rusty-grid, else target/debug/rusty-grid)
  -h, --help            Show this help message and exit
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tasks-count) TASKS_COUNT="$2"; shift 2 ;;
        --duration)    WORKLOAD_DURATION="$2"; shift 2 ;;
        --output-dir)  OUTPUT_DIR="$2"; shift 2 ;;
        --cli-bin)     CLI_BIN="$2"; shift 2 ;;
        -h|--help)     usage ;;
        *) echo "Unknown argument: $1" >&2; usage ;;
    esac
done

if [[ "$TASKS_COUNT" -lt 2 ]]; then
    log_fail "--tasks-count must be at least 2 so that both workers receive work."
fi

if [[ -z "$CLI_BIN" ]]; then
    for cand in "${SCRIPT_DIR}/target/release/rusty-grid" "${SCRIPT_DIR}/target/debug/rusty-grid"; do
        if [[ -x "$cand" ]]; then CLI_BIN="$cand"; break; fi
    done
fi
[[ -n "$CLI_BIN" && -x "$CLI_BIN" ]] || log_fail "rusty-grid binary not found. Run: cargo build --bin rusty-grid (or pass --cli-bin)."
CLI_BIN="$(cd "$(dirname "$CLI_BIN")" && pwd)/$(basename "$CLI_BIN")"
command -v python3 >/dev/null 2>&1 || log_fail "python3 is required (JSON parsing and timing)."

# SHA-256 helper: sha256sum (Linux) or shasum (macOS).
if command -v sha256sum >/dev/null 2>&1; then
    SHA_CMD="sha256sum"
else
    SHA_CMD="shasum -a 256"
fi

# ------------------------------------------------------------------------------
# Isolated environment and process bookkeeping
# ------------------------------------------------------------------------------
WORK="$(mktemp -d "${TMPDIR:-/tmp}/oxideswarm_chaos_XXXXXX")"
# Physical path: tasks report their cwd with symlinks resolved (/var -> /private/var on macOS).
WORK="$(cd "$WORK" && pwd -P)"
TEST_HOME="${WORK}/home"
MARKERS="${WORK}/markers"
mkdir -p "$TEST_HOME" "$MARKERS" "$OUTPUT_DIR"
EMPTY_CONFIG="${WORK}/empty.toml"
: > "$EMPTY_CONFIG"

GRID_ENV=(env)
for var in $(env | sed -n 's/^\(RUSTY_GRID_[A-Za-z0-9_]*\)=.*/\1/p') OXIDESWARM_SECRET OXIDESWARM_SECRET_FILE OXIDE_DEV_MODE; do
    GRID_ENV+=(-u "$var")
done
GRID_ENV+=(HOME="$TEST_HOME" USERPROFILE="$TEST_HOME" RUSTY_GRID_CONFIG="$EMPTY_CONFIG")

MASTER_PID=""
VICTIM_PID=""
SURVIVOR_PID=""
REPLACEMENT_PID=""

cleanup() {
    local code=$?
    for pid in "$REPLACEMENT_PID" "$SURVIVOR_PID" "$VICTIM_PID" "$MASTER_PID"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done
    sleep 0.5
    for pid in "$REPLACEMENT_PID" "$SURVIVOR_PID" "$VICTIM_PID" "$MASTER_PID"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill -KILL "$pid" 2>/dev/null || true
        fi
    done
    if [[ $code -ne 0 ]]; then
        echo -e "${RED}${BOLD}CHAOS TEST FAILED (exit ${code}); logs kept in ${WORK}${NC}" >&2
    else
        rm -rf "$WORK"
    fi
    exit $code
}
trap cleanup EXIT INT TERM

grid() { "${GRID_ENV[@]}" "$CLI_BIN" "$@"; }

# JSON field extraction without jq: json_get '<json>' '<python expression on d>'
json_get() {
    printf '%s' "$1" | python3 -c "import sys, json
d = json.load(sys.stdin)
print($2)"
}

TEST_START_MS=$(now_ms)
TEST_START_ISO=$(iso_utc)

# ==============================================================================
# STAGE 1: START ISOLATED CLUSTER (1 master, victim worker, survivor worker)
# ==============================================================================
log_stage "STAGE 1: START ISOLATED CLUSTER"
PORT_FILE="${WORK}/master.port"
"${GRID_ENV[@]}" "$CLI_BIN" master --listen 127.0.0.1:0 --port-file "$PORT_FILE" > "${WORK}/master.log" 2>&1 &
MASTER_PID=$!
for _ in $(seq 1 100); do
    [[ -s "$PORT_FILE" ]] && break
    kill -0 "$MASTER_PID" 2>/dev/null || { cat "${WORK}/master.log" >&2; log_fail "master exited during startup"; }
    sleep 0.1
done
[[ -s "$PORT_FILE" ]] || log_fail "master did not publish its port"
MASTER_ADDR="127.0.0.1:$(tr -d '\r\n' < "$PORT_FILE")"
log_pass "Master up at ${MASTER_ADDR} (PID ${MASTER_PID})"

SB_VICTIM="${WORK}/sandbox_victim"
SB_SURVIVOR="${WORK}/sandbox_survivor"
mkdir -p "$SB_VICTIM" "$SB_SURVIVOR"

start_worker() {  # name sandbox logfile -> sets STARTED_PID
    "${GRID_ENV[@]}" "$CLI_BIN" worker --master "$MASTER_ADDR" --name "$1" --cores 2 --no-gpu \
        --max-concurrency 4 --sandbox-base-dir "$2" > "$3" 2>&1 &
    STARTED_PID=$!
}

start_worker chaos-victim "$SB_VICTIM" "${WORK}/victim.log";     VICTIM_PID=$STARTED_PID
start_worker chaos-survivor "$SB_SURVIVOR" "${WORK}/survivor.log"; SURVIVOR_PID=$STARTED_PID

worker_count() {
    grid status --master "$MASTER_ADDR" --workers --json 2>/dev/null \
        | python3 -c "import sys, json
try:
    print(len(json.load(sys.stdin)))
except Exception:
    print(0)"
}

for _ in $(seq 1 100); do
    [[ "$(worker_count)" -ge 2 ]] && break
    sleep 0.2
done
[[ "$(worker_count)" -ge 2 ]] || log_fail "both workers did not register (see ${WORK}/victim.log and survivor.log)"

worker_id_of() { sed -n 's/^RustyGrid Worker \([0-9a-f-]*\) initializing.*/\1/p' "$1" | head -n1; }
VICTIM_ID="$(worker_id_of "${WORK}/victim.log")"
SURVIVOR_ID="$(worker_id_of "${WORK}/survivor.log")"
[[ -n "$VICTIM_ID" && -n "$SURVIVOR_ID" ]] || log_fail "could not read the worker ids from the worker logs"
log_pass "Workers registered: victim=${VICTIM_ID} survivor=${SURVIVOR_ID}"

# ==============================================================================
# STAGE 2: INJECT LONG-RUNNING TASKS
# ==============================================================================
log_stage "STAGE 2: INJECT ${TASKS_COUNT} TASKS (~${WORKLOAD_DURATION}s each)"
EXPECTED_HASH="$(seq 1 5000 | $SHA_CMD | awk '{print $1}')"
declare -a TASK_IDS=()
for i in $(seq 1 "$TASKS_COUNT"); do
    # Each task appends "start:<cwd>" and "done:<cwd>:<sha256>" to its own marker log. The cwd is the
    # per-task directory inside the sandbox of the worker that ran it, which identifies that worker.
    SCRIPT="echo \"start:\$(pwd)\" >> '${MARKERS}/task_${i}.log'; H=\$(seq 1 5000 | ${SHA_CMD} | awk '{print \$1}'); sleep ${WORKLOAD_DURATION}; echo \"done:\$(pwd):\$H\" >> '${MARKERS}/task_${i}.log'"
    OUT="$(grid submit --master "$MASTER_ADDR" --type command --command sh --timeout-secs 120 -- -c "$SCRIPT" 2>&1)"
    TID="$(echo "$OUT" | sed -n 's/.*Task ID: \([0-9a-f-]\{36\}\).*/\1/p' | head -n1)"
    [[ -n "$TID" ]] || log_fail "task ${i} was not accepted: ${OUT}"
    TASK_IDS+=("$TID")
done
log_pass "${TASKS_COUNT} tasks submitted"

task_state() {  # task id -> "state worker"
    local out
    out="$(grid status --master "$MASTER_ADDR" --task-id "$1" --json 2>/dev/null || echo '{}')"
    json_get "$out" "d.get('state', 'Unknown') + ' ' + str(d.get('assigned_worker') or '-')"
}

# Wait until the victim provably holds running tasks.
VICTIM_TASKS=()
for _ in $(seq 1 100); do
    VICTIM_TASKS=()
    for i in $(seq 1 "$TASKS_COUNT"); do
        if [[ -f "${MARKERS}/task_${i}.log" ]] && grep -q "^start:${SB_VICTIM}" "${MARKERS}/task_${i}.log"; then
            VICTIM_TASKS+=("$i")
        fi
    done
    [[ ${#VICTIM_TASKS[@]} -ge 1 ]] && break
    sleep 0.1
done
if [[ ${#VICTIM_TASKS[@]} -eq 0 ]]; then
    log_fail "no task started on the victim worker, so a kill would not exercise failover (increase --tasks-count)"
fi
log_pass "Victim is running ${#VICTIM_TASKS[@]} task(s) (task numbers: ${VICTIM_TASKS[*]})"

# ==============================================================================
# STAGE 3: SIGKILL THE VICTIM (by PID, only the one this script started)
# ==============================================================================
log_stage "STAGE 3: SIGKILL VICTIM WORKER (PID ${VICTIM_PID})"
T_KILL_MS=$(now_ms)
T_KILL_ISO=$(iso_utc)
kill -KILL "$VICTIM_PID"
wait "$VICTIM_PID" 2>/dev/null || true
VICTIM_PID=""
log_pass "Victim killed at ${T_KILL_ISO}"

# ==============================================================================
# STAGE 4: MASTER MUST NOTICE THE DEAD WORKER
# ==============================================================================
log_stage "STAGE 4: DEAD-NODE DETECTION"
# The CLI does not expose worker connection state, but a task held by a dead worker must leave the
# "Running on the victim" state once the master has noticed (EOF fast path, or the heartbeat reaper).
PROBE_IDX=$(( ${VICTIM_TASKS[0]} - 1 ))
DETECTED=false
DETECTION_MS=0
for _ in $(seq 1 300); do
    read -r PSTATE PWORKER <<< "$(task_state "${TASK_IDS[$PROBE_IDX]}")"
    if [[ "$PSTATE" != "Running" || "$PWORKER" != "$VICTIM_ID" ]]; then
        DETECTION_MS=$(( $(now_ms) - T_KILL_MS ))
        DETECTED=true
        break
    fi
    sleep 0.1
done
[[ "$DETECTED" == true ]] || log_fail "master still shows task ${VICTIM_TASKS[0]} as Running on the killed worker 30 s after the kill"
log_pass "Master released the orphaned task (state ${PSTATE}) ${DETECTION_MS} ms after the kill"

# ==============================================================================
# STAGE 5: ORPHANED TASKS MUST FINISH ON THE SURVIVOR WITH CORRECT OUTPUT
# ==============================================================================
log_stage "STAGE 5: REQUEUE AND COMPLETION"
DEADLINE=$(( $(date +%s) + WORKLOAD_DURATION * 4 + 60 ))
ALL_COMPLETED=false
while [[ $(date +%s) -lt $DEADLINE ]]; do
    COMPLETED=0
    for idx in $(seq 0 $((TASKS_COUNT - 1))); do
        read -r STATE _ <<< "$(task_state "${TASK_IDS[$idx]}")"
        case "$STATE" in
            Completed) COMPLETED=$((COMPLETED + 1)) ;;
            Failed|Cancelled) log_fail "task $((idx + 1)) (${TASK_IDS[$idx]}) ended in state ${STATE}" ;;
        esac
    done
    log_info "  [Progress] Completed: ${COMPLETED}/${TASKS_COUNT}"
    if [[ $COMPLETED -eq $TASKS_COUNT ]]; then ALL_COMPLETED=true; break; fi
    sleep 1
done
[[ "$ALL_COMPLETED" == true ]] || log_fail "not every task completed before the deadline"
FAILOVER_MS=$(( $(now_ms) - T_KILL_MS ))
log_pass "All ${TASKS_COUNT} tasks reached Completed ${FAILOVER_MS} ms after the kill"

# Content checks on the marker logs (the orphaned shell of a SIGKILLed worker keeps running and may
# add its own "done:" line from the victim's directory; only the survivor's line counts).
FAILOVER_VERIFIED_COUNT=0
TASK_ROWS=""
for i in $(seq 1 "$TASKS_COUNT"); do
    LOG="${MARKERS}/task_${i}.log"
    [[ -f "$LOG" ]] || log_fail "task ${i} left no marker log"
    ORPHANED=false
    for v in "${VICTIM_TASKS[@]}"; do [[ "$v" == "$i" ]] && ORPHANED=true; done
    if [[ "$ORPHANED" == true ]]; then
        # Completed state on the survivor can lag the marker by a moment; wait briefly for the line.
        for _ in $(seq 1 100); do
            grep -q "^done:${SB_SURVIVOR}[^:]*:${EXPECTED_HASH}$" "$LOG" && break
            sleep 0.2
        done
        grep -q "^done:${SB_SURVIVOR}[^:]*:${EXPECTED_HASH}$" "$LOG" \
            || log_fail "orphaned task ${i} was not re-executed on the survivor with the expected SHA-256 (log: $(tr '\n' '|' < "$LOG"))"
        FAILOVER_VERIFIED_COUNT=$((FAILOVER_VERIFIED_COUNT + 1))
        TASK_ROWS="${TASK_ROWS}| ${i} | ${TASK_IDS[$((i - 1))]:0:8} | victim | re-executed on survivor | verified |
"
    else
        grep -q "^done:.*:${EXPECTED_HASH}$" "$LOG" \
            || log_fail "task ${i} finished without the expected SHA-256 (log: $(tr '\n' '|' < "$LOG"))"
        TASK_ROWS="${TASK_ROWS}| ${i} | ${TASK_IDS[$((i - 1))]:0:8} | survivor | uninterrupted | verified |
"
    fi
done
[[ $FAILOVER_VERIFIED_COUNT -ge 1 ]] || log_fail "no orphaned task was verified, the failover path was not exercised"
log_pass "${FAILOVER_VERIFIED_COUNT} orphaned task(s) re-executed on the survivor with the expected payload hash"

# ==============================================================================
# STAGE 6: A REPLACEMENT WORKER MUST RE-REGISTER AND RUN WORK
# ==============================================================================
log_stage "STAGE 6: RECOVERY"
T_RECOVERY_MS=$(now_ms)
start_worker chaos-victim "$SB_VICTIM" "${WORK}/replacement.log"; REPLACEMENT_PID=$STARTED_PID
RECOVERED=false
for _ in $(seq 1 150); do
    if [[ "$(worker_count)" -ge 2 ]]; then RECOVERED=true; break; fi
    sleep 0.1
done
[[ "$RECOVERED" == true ]] || log_fail "the replacement worker did not register"
RECONNECT_MS=$(( $(now_ms) - T_RECOVERY_MS ))
log_pass "Replacement worker registered after ${RECONNECT_MS} ms"

SMOKE_OUT="$(grid submit --master "$MASTER_ADDR" --type command --command echo --wait --json -- POST_CHAOS_SMOKE)"
SMOKE_EXIT="$(json_get "$SMOKE_OUT" "d.get('exit_code', 1)")"
SMOKE_STDOUT="$(json_get "$SMOKE_OUT" "d.get('stdout', '').strip()")"
[[ "$SMOKE_EXIT" == "0" && "$SMOKE_STDOUT" == "POST_CHAOS_SMOKE" ]] || log_fail "post-recovery smoke task failed: ${SMOKE_OUT}"
log_pass "Post-recovery smoke task ran with exit code 0"

TEST_DURATION_MS=$(( $(now_ms) - TEST_START_MS ))

# ==============================================================================
# STAGE 7: REPORTS (every figure below was measured in this run)
# ==============================================================================
log_stage "STAGE 7: REPORTS"
DATA_JSON_PATH="${OUTPUT_DIR}/chaos_test_data.json"
REPORT_MD_PATH="${OUTPUT_DIR}/chaos_test_report.md"

TESTED_ON="$(uname -srm)"
python3 - "$DATA_JSON_PATH" <<PYEOF
import json, sys
data = {
    "test_name": "OxideSwarm local failover test (1 master + 2 workers, SIGKILL of one worker)",
    "timestamp_utc": "${TEST_START_ISO}",
    "host": "${TESTED_ON}",
    "execution_duration_ms": ${TEST_DURATION_MS},
    "tasks_submitted": ${TASKS_COUNT},
    "task_sleep_seconds": ${WORKLOAD_DURATION},
    "tasks_running_on_victim_at_kill": ${#VICTIM_TASKS[@]},
    "orphaned_tasks_reexecuted_on_survivor": ${FAILOVER_VERIFIED_COUNT},
    "kill_timestamp_utc": "${T_KILL_ISO}",
    "detection_latency_ms": ${DETECTION_MS},
    "kill_to_all_completed_ms": ${FAILOVER_MS},
    "replacement_registration_ms": ${RECONNECT_MS},
    "expected_payload_sha256": "${EXPECTED_HASH}",
    "post_recovery_smoke_exit_code": ${SMOKE_EXIT},
    # Written only after every assertion above succeeded; a failed check exits before this point.
    "all_assertions_passed": True,
}
with open(sys.argv[1], "w") as f:
    json.dump(data, f, indent=2)
PYEOF

cat > "${REPORT_MD_PATH}" <<MDEOF
# Local failover test report

Generated by \`run_chaos_test.sh\` on ${TESTED_ON} at ${TEST_START_ISO}.
Scope: one master and two workers on one machine over loopback. This shows the worker-loss
requeue path works on this build and host; it says nothing about phones, Wi-Fi or WAN links.

| Measurement | Value |
| :--- | :--- |
| Tasks submitted | ${TASKS_COUNT} (each sleeps ${WORKLOAD_DURATION} s) |
| Tasks running on the victim when it was killed | ${#VICTIM_TASKS[@]} |
| Orphaned tasks re-executed on the survivor with the expected SHA-256 | ${FAILOVER_VERIFIED_COUNT} |
| Time until the master released the dead worker's task | ${DETECTION_MS} ms |
| Time from the kill until all tasks were Completed | ${FAILOVER_MS} ms |
| Time until a replacement worker re-registered | ${RECONNECT_MS} ms |
| Post-recovery smoke task exit code | ${SMOKE_EXIT} |

| Task | Id | Started on | Fate | Payload hash |
| :---: | :--- | :--- | :--- | :--- |
${TASK_ROWS}
Note: SIGKILL of a worker does not stop the task processes it had spawned; the orphaned
shells of the killed worker keep running until their sleep ends. That is expected here and
is why only the survivor's completion line is accepted as proof of re-execution.
MDEOF

log_pass "Wrote ${DATA_JSON_PATH} and ${REPORT_MD_PATH}"

echo -e "\n${BOLD}${GREEN}================================================================================${NC}"
echo -e "${BOLD}${GREEN}  CHAOS TEST: ALL CHECKS PASSED (measured in this run)${NC}"
echo -e "${GREEN}  - tasks on the victim at kill time : ${#VICTIM_TASKS[@]}${NC}"
echo -e "${GREEN}  - re-executed on the survivor      : ${FAILOVER_VERIFIED_COUNT}${NC}"
echo -e "${GREEN}  - detection latency                : ${DETECTION_MS} ms${NC}"
echo -e "${GREEN}  - replacement registration         : ${RECONNECT_MS} ms${NC}"
echo -e "${GREEN}  - report                           : ${REPORT_MD_PATH}${NC}"
echo -e "${BOLD}${GREEN}================================================================================${NC}\n"
exit 0
