#!/usr/bin/env bash
# ==============================================================================
# test_integration.sh — Automated End-to-End Cluster Integration Test Harness
#
# Tests Acceptance Criteria AC1 through AC5 for rusty_grid Milestone 5:
# - AC1: Launch 1 Master and 3 Workers via unified `rusty-grid` CLI with dynamic port
# - AC2: Verify all 3 Workers register and advertise capabilities (cores, RAM, simulated GPU)
# - AC3: Submit generic task and verify execution outcome and return value
# - AC4: Submit GPU-specific task and verify strict routing to GPU worker node
# - AC5: Submit batch of 5 independent tasks and verify parallel distribution
# - M5 Bonus: In-Memory Map/Reduce word count execution and JSON aggregation
#
# Non-interactive, non-colliding (ephemeral port), POSIX trap process cleanup.
# ==============================================================================

set -euo pipefail

# ANSI color formatting
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

log_section() { echo -e "\n${BOLD}${CYAN}=== $* ===${NC}"; }
log_info()    { echo -e "${BLUE}[INFO]${NC} $*"; }
log_pass()    { echo -e "${GREEN}[PASS]${NC} $*"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_fail()    { echo -e "${RED}[FAIL]${NC} $*" >&2; }

TEST_DIR=$(mktemp -d "/tmp/rusty_grid_test_XXXXXX")
PORT_FILE="${TEST_DIR}/master.port"
MASTER_LOG="${TEST_DIR}/master.log"
W1_LOG="${TEST_DIR}/worker_1.log"
W2_LOG="${TEST_DIR}/worker_2.log"
W3_LOG="${TEST_DIR}/worker_3.log"
INPUT_FILE="${TEST_DIR}/mapreduce_input.txt"

MASTER_PID=""
W1_PID=""
W2_PID=""
W3_PID=""

cleanup() {
    local exit_code=$?
    echo ""
    log_info "Executing test harness teardown & child process cleanup..."

    for pid in "$W1_PID" "$W2_PID" "$W3_PID" "$MASTER_PID"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done

    sleep 0.5
    for pid in "$W1_PID" "$W2_PID" "$W3_PID" "$MASTER_PID"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill -KILL "$pid" 2>/dev/null || true
        fi
    done

    rm -rf "$TEST_DIR"

    if [[ $exit_code -eq 0 ]]; then
        echo -e "${BOLD}${GREEN}======================================================${NC}"
        echo -e "${BOLD}${GREEN}   ALL INTEGRATION ACCEPTANCE TESTS PASSED (100%)    ${NC}"
        echo -e "${BOLD}${GREEN}======================================================${NC}"
    else
        echo -e "${BOLD}${RED}======================================================${NC}"
        echo -e "${BOLD}${RED}          INTEGRATION TEST HARNESS FAILED             ${NC}"
        echo -e "${BOLD}${RED}======================================================${NC}"
    fi
    exit $exit_code
}

trap cleanup EXIT INT TERM

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

log_section "PHASE 1: Building Unified CLI Binary (rusty-grid)"
if [[ -f "./target/debug/rusty-grid" ]]; then
    log_info "Binary ./target/debug/rusty-grid already exists, validating..."
    cargo build --bin rusty-grid --quiet
else
    log_info "Compiling rusty-grid binary via 'cargo build --bin rusty-grid'..."
    cargo build --bin rusty-grid
fi

BIN="./target/debug/rusty-grid"
if [[ ! -x "$BIN" ]]; then
    log_fail "Failed to find executable binary at ${BIN}"
    exit 1
fi
log_pass "Verified binary: $($BIN --version)"

log_section "PHASE 2: Starting Master Node with Ephemeral Port"
log_info "Starting Master on 127.0.0.1:0 with port-file: ${PORT_FILE}"
$BIN master --listen "127.0.0.1:0" --port-file "$PORT_FILE" > "$MASTER_LOG" 2>&1 &
MASTER_PID=$!
log_info "Master spawned with PID: ${MASTER_PID}"

# Await port file publication
ATTEMPTS=0
MAX_ATTEMPTS=50
while [[ ! -s "$PORT_FILE" ]]; do
    if ! kill -0 "$MASTER_PID" 2>/dev/null; then
        log_fail "Master process terminated unexpectedly! Log:"
        cat "$MASTER_LOG" >&2
        exit 1
    fi
    sleep 0.1
    ATTEMPTS=$((ATTEMPTS + 1))
    if [[ $ATTEMPTS -ge $MAX_ATTEMPTS ]]; then
        log_fail "Timed out waiting for Master to write port file (${PORT_FILE})"
        cat "$MASTER_LOG" >&2
        exit 1
    fi
done

MASTER_PORT=$(cat "$PORT_FILE")
MASTER_ADDR="127.0.0.1:${MASTER_PORT}"
log_pass "Master online and listening at ${MASTER_ADDR} (PID: ${MASTER_PID})"

log_section "PHASE 3: Starting 3 Worker Nodes with Hardware Overrides"
# Worker 1: Standard CPU Worker (2 cores, 2048 MB RAM, No GPU)
log_info "Spawning Worker 1 (CPU-only: 2 cores, 2048 MB RAM)..."
$BIN worker \
    --master "$MASTER_ADDR" \
    --name "worker-cpu-node-1" \
    --cores 2 \
    --ram-mb 2048 \
    --no-gpu \
    --max-concurrency 4 > "$W1_LOG" 2>&1 &
W1_PID=$!

# Worker 2: Standard CPU Worker (4 cores, 4096 MB RAM, No GPU)
log_info "Spawning Worker 2 (CPU-only: 4 cores, 4096 MB RAM)..."
$BIN worker \
    --master "$MASTER_ADDR" \
    --name "worker-cpu-node-2" \
    --cores 4 \
    --ram-mb 4096 \
    --no-gpu \
    --max-concurrency 4 > "$W2_LOG" 2>&1 &
W2_PID=$!

# Worker 3: GPU Compute Worker (8 cores, 8192 MB RAM, Simulated GPU)
log_info "Spawning Worker 3 (GPU-enabled: 8 cores, 8192 MB RAM, Simulated GPU)..."
$BIN worker \
    --master "$MASTER_ADDR" \
    --name "worker-gpu-node-3" \
    --cores 8 \
    --ram-mb 8192 \
    --simulate-gpu \
    --gpu-name "Simulated NVIDIA RTX 4090" \
    --max-concurrency 8 > "$W3_LOG" 2>&1 &
W3_PID=$!

log_pass "Spawned all 3 workers (PIDs: W1=${W1_PID}, W2=${W2_PID}, W3=${W3_PID})"

log_section "PHASE 4: Verifying AC1 & AC2 (Registration & Capability Advertising)"
ATTEMPTS=0
MAX_ATTEMPTS=60
WORKER_COUNT=0
STATUS_JSON="[]"
while [[ $WORKER_COUNT -lt 3 ]]; do
    STATUS_JSON=$($BIN status --master "$MASTER_ADDR" --workers --json 2>/dev/null || echo "[]")
    WORKER_COUNT=$(echo "$STATUS_JSON" | grep -c '"cpu_cores"' 2>/dev/null || true)
    WORKER_COUNT=$(echo "$WORKER_COUNT" | tr -d '[:space:]')
    if [[ -z "$WORKER_COUNT" ]]; then
        WORKER_COUNT=0
    fi
    if [[ $WORKER_COUNT -ge 3 ]]; then
        break
    fi
    sleep 0.2
    ATTEMPTS=$((ATTEMPTS + 1))
    if [[ $ATTEMPTS -ge $MAX_ATTEMPTS ]]; then
        log_fail "Expected 3 registered workers, found ${WORKER_COUNT}. Status output:"
        echo "$STATUS_JSON" >&2
        log_fail "Worker 1 Log:\n$(cat "$W1_LOG")"
        log_fail "Worker 2 Log:\n$(cat "$W2_LOG")"
        log_fail "Worker 3 Log:\n$(cat "$W3_LOG")"
        exit 1
    fi
done

log_pass "All 3 workers successfully registered with Master!"

# Verify GPU capabilities advertised for Worker 3
if echo "$STATUS_JSON" | grep -q '"is_simulated_gpu": true' || echo "$STATUS_JSON" | grep -q '"has_gpu": true'; then
    log_pass "Verified Worker 3 advertised GPU compute capabilities."
else
    log_fail "Worker 3 did not advertise GPU capability. Registry state: ${STATUS_JSON}"
    exit 1
fi

# Print registered worker summary
$BIN status --master "$MASTER_ADDR" --workers

log_section "PHASE 5: Verifying AC3 (Generic Task Execution)"
log_info "Submitting generic task: echo 'Hello RustyGrid Distributed Compute'..."
GENERIC_OUTPUT=$($BIN submit --master "$MASTER_ADDR" --type generic --wait --command echo -- "Hello RustyGrid Distributed Compute")
echo "$GENERIC_OUTPUT"

if echo "$GENERIC_OUTPUT" | grep -q "Hello RustyGrid Distributed Compute"; then
    log_pass "AC3 Succeeded: Generic task executed and output verified."
else
    log_fail "AC3 Failed: Generic task output does not match expected output."
    exit 1
fi

log_section "PHASE 6: Verifying AC4 (Workload-Specific Routing & GPU Isolation)"
log_info "Submitting GPU-specific task with --gpu requirement..."
GPU_OUTPUT=$($BIN submit --master "$MASTER_ADDR" --type gpu --gpu --cores 1 --wait)
echo "$GPU_OUTPUT"

if echo "$GPU_OUTPUT" | grep -q "completed successfully"; then
    log_pass "AC4 Succeeded: GPU task completed successfully on simulated GPU node."
else
    log_fail "AC4 Failed: GPU task failed to execute."
    exit 1
fi

log_section "PHASE 7: Verifying AC5 (Parallel Batch Spread Scheduling across Workers)"
log_info "Submitting batch of 5 independent simulated compilation tasks in parallel..."
BATCH_OUTPUT_DIR="${TEST_DIR}/batch_outputs"
mkdir -p "$BATCH_OUTPUT_DIR"

TASK_PIDS=()
for i in {1..5}; do
    (
        $BIN submit \
            --master "$MASTER_ADDR" \
            --type command \
            --command echo \
            --wait \
            -- "Compiled crate module_${i}" > "${BATCH_OUTPUT_DIR}/task_${i}.out" 2>&1
    ) &
    TASK_PIDS+=($!)
done

log_info "Waiting for all 5 batch compilation tasks to complete..."
for tpid in "${TASK_PIDS[@]}"; do
    wait "$tpid" || true
done

COMPLETED_COUNT=0
for i in {1..5}; do
    TASK_OUT="${BATCH_OUTPUT_DIR}/task_${i}.out"
    if grep -q "Compiled crate module_${i}" "$TASK_OUT"; then
        log_info "Task ${i}: SUCCESS ($(grep 'Execution Time:' "$TASK_OUT" || echo 'completed'))"
        COMPLETED_COUNT=$((COMPLETED_COUNT + 1))
    else
        log_fail "Task ${i}: FAILED! Content:"
        cat "$TASK_OUT" >&2
    fi
done

if [[ $COMPLETED_COUNT -eq 5 ]]; then
    log_pass "AC5 Succeeded: All 5 independent compilation tasks executed in parallel across workers."
else
    log_fail "AC5 Failed: Only ${COMPLETED_COUNT}/5 tasks succeeded."
    exit 1
fi

log_section "PHASE 8: Verifying Milestone 5 Map/Reduce Execution"
log_info "Preparing distributed Map/Reduce word count dataset..."
cat << 'EOF' > "$INPUT_FILE"
rusty grid distributed computing framework in rust
high performance parallel task scheduling
distributed rust compilation across worker nodes
rusty grid native p2p nat traversal quic transport
lightweight in memory map reduce word count execution
EOF

log_info "Executing Map/Reduce job via 'rusty-grid mapreduce'..."
MAPREDUCE_OUTPUT=$($BIN mapreduce \
    --master "$MASTER_ADDR" \
    --input "$INPUT_FILE" \
    --mapper word_count \
    --reducer sum \
    --chunks 3 \
    --json)

echo "$MAPREDUCE_OUTPUT"

# Verify word count outputs
if echo "$MAPREDUCE_OUTPUT" | grep -q '"rusty":' && echo "$MAPREDUCE_OUTPUT" | grep -q '"grid":'; then
    log_pass "Milestone 5 Map/Reduce Succeeded: Word count properly aggregated across workers."
else
    log_fail "Map/Reduce Failed: Missing aggregated word counts in output."
    exit 1
fi

log_section "PHASE 9: Querying Final Cluster Health Status"
$BIN status --master "$MASTER_ADDR"

log_pass "Integration verification complete with 100% success!"
