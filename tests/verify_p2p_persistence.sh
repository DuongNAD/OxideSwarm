#!/usr/bin/env bash
# ==============================================================================
# tests/verify_p2p_persistence.sh — P2P SecretKey & Ticket Persistence Verification
#
# Acceptance Criteria:
# - Run master node Run 1 with --p2p-key-file test_key.bin and --p2p-ticket-file ticket.txt
# - Verify test_key.bin is created and ticket.txt is published
# - Capture ticket from Run 1 and cleanly stop master
# - Run master node Run 2 with identical key file and ticket file
# - Verify test_key.bin is reused and ticket.txt is published
# - Capture ticket from Run 2 and cleanly stop master
# - Objectively verify:
#     1. ticket 1 == ticket 2 (exact string equality)
#     2. test_key.bin exists and has valid non-zero content
# - Non-interactive, non-colliding ephemeral ports, POSIX trap process cleanup.
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/oxide_p2p_persist_XXXXXX")
PORT_FILE="${TEST_DIR}/master.port"
KEY_FILE="${TEST_DIR}/test_key.bin"
TICKET_FILE="${TEST_DIR}/ticket.txt"
LOG_RUN1="${TEST_DIR}/master_run1.log"
LOG_RUN2="${TEST_DIR}/master_run2.log"

MASTER_PID=""

cleanup() {
    local exit_code=$?
    echo ""
    log_info "Executing test teardown & child process cleanup..."

    if [[ -n "$MASTER_PID" ]] && kill -0 "$MASTER_PID" 2>/dev/null; then
        kill -TERM "$MASTER_PID" 2>/dev/null || true
        local wait_attempts=0
        while kill -0 "$MASTER_PID" 2>/dev/null && [[ $wait_attempts -lt 20 ]]; do
            sleep 0.1
            wait_attempts=$((wait_attempts + 1))
        done
        if kill -0 "$MASTER_PID" 2>/dev/null; then
            kill -KILL "$MASTER_PID" 2>/dev/null || true
            if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || -n "${WINDIR:-}" ]]; then
                taskkill //F //PID "$MASTER_PID" 2>/dev/null || true
            fi
        fi
    fi

    # Allow Windows OS time to release held file handles before directory removal
    sleep 0.3
    rm -rf "$TEST_DIR" 2>/dev/null || true

    if [[ $exit_code -eq 0 ]]; then
        echo -e "\n${BOLD}${GREEN}======================================================${NC}"
        echo -e "${BOLD}${GREEN}  P2P TICKET PERSISTENCE VERIFICATION PASSED (100%)    ${NC}"
        echo -e "${BOLD}${GREEN}======================================================${NC}\n"
    else
        echo -e "\n${BOLD}${RED}======================================================${NC}"
        echo -e "${BOLD}${RED}  P2P TICKET PERSISTENCE VERIFICATION FAILED           ${NC}"
        echo -e "${BOLD}${RED}======================================================${NC}\n"
    fi
    exit $exit_code
}

trap cleanup EXIT INT TERM

wait_for_file_and_content() {
    local file_path="$1"
    local pid="$2"
    local log_file="$3"
    local max_attempts="${4:-100}"
    local attempts=0

    while [[ ! -s "$file_path" ]]; do
        if ! kill -0 "$pid" 2>/dev/null; then
            log_fail "Master process terminated unexpectedly! Log output:"
            cat "$log_file" >&2
            return 1
        fi
        sleep 0.1
        attempts=$((attempts + 1))
        if [[ $attempts -ge $max_attempts ]]; then
            log_fail "Timed out waiting for file (${file_path}) to be written."
            cat "$log_file" >&2
            return 1
        fi
    done
    return 0
}

stop_master_process() {
    local pid="$1"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null || true
        local attempts=0
        while kill -0 "$pid" 2>/dev/null && [[ $attempts -lt 30 ]]; do
            sleep 0.1
            attempts=$((attempts + 1))
        done
        if kill -0 "$pid" 2>/dev/null; then
            kill -KILL "$pid" 2>/dev/null || true
            if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || -n "${WINDIR:-}" ]]; then
                taskkill //F //PID "$pid" 2>/dev/null || true
            fi
        fi
    fi
    # Allow socket recycling and NTFS lock release
    sleep 0.5
}

log_section "PHASE 1: Locating & Validating CLI Binary"
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || -n "${WINDIR:-}" || -f "./target/debug/rusty-grid.exe" ]]; then
    BIN="./target/debug/rusty-grid.exe"
else
    BIN="./target/debug/rusty-grid"
fi

if [[ -f "$BIN" ]]; then
    log_info "Binary found at $BIN, refreshing build..."
    cargo build --bin rusty-grid --quiet
else
    log_info "Compiling rusty-grid binary via 'cargo build --bin rusty-grid'..."
    cargo build --bin rusty-grid
fi

if [[ ! -f "$BIN" && ! -x "$BIN" ]]; then
    log_fail "Failed to locate executable binary at ${BIN}"
    exit 1
fi
log_pass "Verified binary: $($BIN --version)"

log_section "PHASE 2: Pre-test Environment Sanitation"
rm -f "$KEY_FILE" "$TICKET_FILE" "$PORT_FILE"
log_pass "Confirmed test key file does not exist prior to Run 1."

log_section "PHASE 3: Master Run 1 — Initial Key Generation & Ticket Creation"
log_info "Starting Master Run 1 on ephemeral port with key file: ${KEY_FILE}"
$BIN master \
    --listen "127.0.0.1:0" \
    --port-file "$PORT_FILE" \
    --p2p \
    --p2p-key-file "$KEY_FILE" \
    --p2p-ticket-file "$TICKET_FILE" > "$LOG_RUN1" 2>&1 &
MASTER_PID=$!
log_info "Master Run 1 spawned with PID: ${MASTER_PID}"

wait_for_file_and_content "$PORT_FILE" "$MASTER_PID" "$LOG_RUN1" 50
wait_for_file_and_content "$TICKET_FILE" "$MASTER_PID" "$LOG_RUN1" 100

PORT_1=$(tr -d '\r\n' < "$PORT_FILE")
TICKET_1=$(tr -d '\r\n' < "$TICKET_FILE")
log_pass "Master Run 1 listening on port: ${PORT_1}"
log_info "Master Run 1 published Ticket: ${TICKET_1}"

if [[ -z "$TICKET_1" ]]; then
    log_fail "Ticket 1 is empty!"
    exit 1
fi

if [[ ! -f "$KEY_FILE" || ! -s "$KEY_FILE" ]]; then
    log_fail "Key file was NOT created or has 0 bytes after Master Run 1!"
    cat "$LOG_RUN1" >&2
    exit 1
fi

KEY_SIZE_1=$(wc -c < "$KEY_FILE" | tr -d '[:space:]')
log_pass "Verified key file created (size: ${KEY_SIZE_1} bytes)."

log_info "Stopping Master Run 1..."
stop_master_process "$MASTER_PID"
MASTER_PID=""
log_pass "Master Run 1 terminated cleanly."

log_section "PHASE 4: Inter-run State Check & Ticket Reset"
if [[ ! -s "$KEY_FILE" ]]; then
    log_fail "Key file vanished after stopping Master Run 1!"
    exit 1
fi

rm -f "$TICKET_FILE" "$PORT_FILE"
log_pass "Reset ticket.txt and master.port for Run 2; preserved test_key.bin."

log_section "PHASE 5: Master Run 2 — Key Reuse & Stable Ticket Generation"
log_info "Starting Master Run 2 using existing key file: ${KEY_FILE}"
$BIN master \
    --listen "127.0.0.1:0" \
    --port-file "$PORT_FILE" \
    --p2p \
    --p2p-key-file "$KEY_FILE" \
    --p2p-ticket-file "$TICKET_FILE" > "$LOG_RUN2" 2>&1 &
MASTER_PID=$!
log_info "Master Run 2 spawned with PID: ${MASTER_PID}"

wait_for_file_and_content "$PORT_FILE" "$MASTER_PID" "$LOG_RUN2" 50
wait_for_file_and_content "$TICKET_FILE" "$MASTER_PID" "$LOG_RUN2" 100

PORT_2=$(tr -d '\r\n' < "$PORT_FILE")
TICKET_2=$(tr -d '\r\n' < "$TICKET_FILE")
log_pass "Master Run 2 listening on port: ${PORT_2}"
log_info "Master Run 2 published Ticket: ${TICKET_2}"

if [[ -z "$TICKET_2" ]]; then
    log_fail "Ticket 2 is empty!"
    exit 1
fi

KEY_SIZE_2=$(wc -c < "$KEY_FILE" | tr -d '[:space:]')
if [[ "$KEY_SIZE_1" != "$KEY_SIZE_2" ]]; then
    log_fail "Key file size changed between runs! Run 1: ${KEY_SIZE_1} bytes, Run 2: ${KEY_SIZE_2} bytes."
    exit 1
fi
log_pass "Verified key file size remained identical across runs (${KEY_SIZE_2} bytes)."

log_info "Stopping Master Run 2..."
stop_master_process "$MASTER_PID"
MASTER_PID=""
log_pass "Master Run 2 terminated cleanly."

log_section "PHASE 6: Objectively Verifying Ticket Stability"
log_info "Ticket Run 1: ${TICKET_1}"
log_info "Ticket Run 2: ${TICKET_2}"

if [[ "$TICKET_1" == "$TICKET_2" ]]; then
    log_pass "TICKET MATCH VERIFIED! Ticket 1 is exactly identical to Ticket 2."
else
    log_fail "TICKET MISMATCH! Persistent key failed to produce stable tickets."
    log_fail "Ticket 1: ${TICKET_1}"
    log_fail "Ticket 2: ${TICKET_2}"
    exit 1
fi

log_section "PHASE 7: Verifying Ephemeral Behavior (Negative Control)"
log_info "Starting two runs without --p2p-key-file to prove tickets differ without persistence..."
rm -f "$TICKET_FILE" "$PORT_FILE"

# Ephemeral Run A
$BIN master --listen "127.0.0.1:0" --port-file "$PORT_FILE" --p2p --p2p-ticket-file "$TICKET_FILE" > "${TEST_DIR}/eph_a.log" 2>&1 &
EPH_A_PID=$!
wait_for_file_and_content "$TICKET_FILE" "$EPH_A_PID" "${TEST_DIR}/eph_a.log" 100
TICKET_EPH_A=$(tr -d '\r\n' < "$TICKET_FILE")
stop_master_process "$EPH_A_PID"

rm -f "$TICKET_FILE" "$PORT_FILE"

# Ephemeral Run B
$BIN master --listen "127.0.0.1:0" --port-file "$PORT_FILE" --p2p --p2p-ticket-file "$TICKET_FILE" > "${TEST_DIR}/eph_b.log" 2>&1 &
EPH_B_PID=$!
wait_for_file_and_content "$TICKET_FILE" "$EPH_B_PID" "${TEST_DIR}/eph_b.log" 100
TICKET_EPH_B=$(tr -d '\r\n' < "$TICKET_FILE")
stop_master_process "$EPH_B_PID"

if [[ "$TICKET_EPH_A" != "$TICKET_EPH_B" ]]; then
    log_pass "Verified negative control: ephemeral runs generated different tickets as expected."
else
    log_warn "Ephemeral tickets were identical (unexpected for random keys)."
fi

log_pass "All persistence checks passed with 100% success!"
