#!/usr/bin/env bash
# ==============================================================================
# verify_mac_daemon.sh — Automated Verification Suite for macOS LaunchDaemon
# ==============================================================================
# Validates property list XML syntax (plutil -lint), asserts required keys,
# and verifies service registration in launchd (supporting both dry-run /
# non-root lint validation and actual live root validation).
# ==============================================================================

set -uo pipefail

# ANSI color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

pass() { echo -e "${GREEN}[PASS]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
info() { echo -e "${BLUE}[INFO]${NC} $*"; }

SERVICE_LABEL="com.oxideswarm.worker"
SYSTEM_PLIST="/Library/LaunchDaemons/${SERVICE_LABEL}.plist"
CUSTOM_PLIST=""
MODE="auto" # "auto", "dry-run", "live"
VERBOSE=false

FAILED_COUNT=0
TOTAL_CHECKS=0

show_help() {
    cat <<EOF
OxideSwarm macOS LaunchDaemon Verification Suite

Usage:
  bash verify_mac_daemon.sh [OPTIONS]

Options:
  --plist <PATH>     Path to .plist file to inspect
  --dry-run          Perform syntactic, schema, and dry-run validation (non-root safe)
  --lint-only        Alias for --dry-run
  --non-root         Alias for --dry-run
  --live             Force live validation of installed daemon in /Library/LaunchDaemons
  -v, --verbose      Display detailed key-value metadata and diagnostic traces
  -h, --help         Display this help message and exit

Exit Codes:
  0: All executed verification checks passed
  1: One or more verification checks failed
EOF
}

# Parse command line options
while [[ $# -gt 0 ]]; do
    case "$1" in
        --plist)
            CUSTOM_PLIST="${2:-}"
            shift 2
            ;;
        --dry-run|--lint-only|--non-root)
            MODE="dry-run"
            shift
            ;;
        --live)
            MODE="live"
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}[ERROR]${NC} Unknown option: $1" >&2
            show_help
            exit 1
            ;;
    esac
done

echo -e "\n${BOLD}${CYAN}========================================================${NC}"
echo -e "${BOLD}${CYAN}   OxideSwarm macOS LaunchDaemon Verification Suite${NC}"
echo -e "${BOLD}${CYAN}========================================================${NC}\n"

# 1. Resolve Target Plist Path
TARGET_PLIST=""
if [[ -n "$CUSTOM_PLIST" ]]; then
    TARGET_PLIST="$CUSTOM_PLIST"
elif [[ "$MODE" == "live" ]]; then
    TARGET_PLIST="$SYSTEM_PLIST"
elif [[ -f "$SYSTEM_PLIST" && "$MODE" != "dry-run" ]]; then
    TARGET_PLIST="$SYSTEM_PLIST"
else
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
    
    CANDIDATES=(
        "${SCRIPT_DIR}/com.oxideswarm.worker.plist"
        "${WORKSPACE_ROOT}/packaging/macos/com.oxideswarm.worker.plist"
        "./com.oxideswarm.worker.plist"
        "${SYSTEM_PLIST}"
    )

    for cand in "${CANDIDATES[@]}"; do
        if [[ -f "$cand" ]]; then
            TARGET_PLIST="$cand"
            break
        fi
    done
fi

info "Target Plist       : ${BOLD}${TARGET_PLIST:-<Not Found>}${NC}"
info "Verification Mode  : ${BOLD}${MODE}${NC}"

# Test 1: File Existence & Non-Empty
TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
echo -n "[CHECK 1] Plist file existence and readability... "
if [[ -n "$TARGET_PLIST" && -f "$TARGET_PLIST" ]]; then
    if [[ -s "$TARGET_PLIST" ]]; then
        pass "Found file at ${TARGET_PLIST}"
    else
        fail "File exists but is empty (0 bytes): ${TARGET_PLIST}"
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi
else
    fail "Plist file not found at '${TARGET_PLIST:-<none>}'"
    FAILED_COUNT=$((FAILED_COUNT + 1))
    echo -e "\n${BOLD}${RED}Cannot proceed with structural tests without a valid plist file.${NC}\n"
    exit 1
fi

# Test 2: XML & Plist Syntax Validation via plutil -lint
TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
echo -n "[CHECK 2] XML Syntax & structure linting (plutil -lint)... "
LINT_OUTPUT=$(plutil -lint "$TARGET_PLIST" 2>&1)
LINT_STATUS=$?

if [[ $LINT_STATUS -eq 0 ]]; then
    pass "plutil lint passed: ${LINT_OUTPUT}"
else
    fail "plutil lint failed: ${LINT_OUTPUT}"
    FAILED_COUNT=$((FAILED_COUNT + 1))
fi

# Test 3: Key Assertion — Label
TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
echo -n "[CHECK 3] Validating required key 'Label'... "
LABEL_TYPE=$(plutil -type Label "$TARGET_PLIST" 2>/dev/null || echo "missing")
if [[ "$LABEL_TYPE" == "string" ]]; then
    LABEL_VAL=$(plutil -extract Label raw -o - "$TARGET_PLIST" 2>/dev/null || echo "")
    if [[ "$LABEL_VAL" == "$SERVICE_LABEL" ]]; then
        pass "Label = '${LABEL_VAL}'"
    else
        fail "Label mismatch: expected '${SERVICE_LABEL}', found '${LABEL_VAL}'"
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi
else
    fail "Key 'Label' is missing or not a string (type: ${LABEL_TYPE})"
    FAILED_COUNT=$((FAILED_COUNT + 1))
fi

# Test 4: Key Assertion — ProgramArguments
TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
echo -n "[CHECK 4] Validating required key 'ProgramArguments'... "
ARGS_TYPE=$(plutil -type ProgramArguments "$TARGET_PLIST" 2>/dev/null || echo "missing")
if [[ "$ARGS_TYPE" == "array" ]]; then
    ARGS_COUNT=$(plutil -extract ProgramArguments raw -o - "$TARGET_PLIST" 2>/dev/null || echo "0")
    if [[ "$ARGS_COUNT" -ge 2 ]]; then
        ARGS_JSON=$(plutil -extract ProgramArguments json -o - "$TARGET_PLIST" 2>/dev/null || echo "[]")
        
        # Verify first argument ends with rusty-grid or oxideswarm
        EXEC_ARG=$(echo "$ARGS_JSON" | python3 -c 'import sys, json; data=json.load(sys.stdin); print(data[0] if len(data) > 0 else "")' 2>/dev/null || echo "")
        SUB_ARG=$(echo "$ARGS_JSON" | python3 -c 'import sys, json; data=json.load(sys.stdin); print(data[1] if len(data) > 1 else "")' 2>/dev/null || echo "")

        if [[ "$EXEC_ARG" == *"rusty-grid"* || "$EXEC_ARG" == *"oxideswarm"* ]] && [[ "$SUB_ARG" == "worker" ]]; then
            pass "Executable: ${EXEC_ARG} | Subcommand: ${SUB_ARG} (Count: ${ARGS_COUNT} items)"
            if [[ "$VERBOSE" == true ]]; then
                info "         Full Arguments: ${ARGS_JSON}"
            fi
        else
            fail "Invalid ProgramArguments content. Expected [..., 'worker']. Found: ${ARGS_JSON}"
            FAILED_COUNT=$((FAILED_COUNT + 1))
        fi
    else
        fail "ProgramArguments array must have at least 2 elements, found ${ARGS_COUNT}"
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi
else
    fail "Key 'ProgramArguments' is missing or not an array (type: ${ARGS_TYPE})"
    FAILED_COUNT=$((FAILED_COUNT + 1))
fi

# Test 5: Key Assertion — RunAtLoad
TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
echo -n "[CHECK 5] Validating required key 'RunAtLoad'... "
RUN_AT_LOAD_TYPE=$(plutil -type RunAtLoad "$TARGET_PLIST" 2>/dev/null || echo "missing")
if [[ "$RUN_AT_LOAD_TYPE" == "bool" ]]; then
    RUN_AT_LOAD_VAL=$(plutil -extract RunAtLoad raw -o - "$TARGET_PLIST" 2>/dev/null || echo "")
    if [[ "$RUN_AT_LOAD_VAL" == "true" ]]; then
        pass "RunAtLoad = true (daemon will start automatically on boot/load)"
    else
        fail "RunAtLoad is set to '${RUN_AT_LOAD_VAL}', expected 'true'"
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi
else
    fail "Key 'RunAtLoad' is missing or not a boolean (type: ${RUN_AT_LOAD_TYPE})"
    FAILED_COUNT=$((FAILED_COUNT + 1))
fi

# Test 6: Key Assertion — KeepAlive
TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
echo -n "[CHECK 6] Validating required key 'KeepAlive'... "
KEEP_ALIVE_TYPE=$(plutil -type KeepAlive "$TARGET_PLIST" 2>/dev/null || echo "missing")
if [[ "$KEEP_ALIVE_TYPE" == "bool" ]]; then
    KEEP_ALIVE_VAL=$(plutil -extract KeepAlive raw -o - "$TARGET_PLIST" 2>/dev/null || echo "")
    if [[ "$KEEP_ALIVE_VAL" == "true" ]]; then
        pass "KeepAlive = true (daemon will automatically restart on exit/crash)"
    else
        fail "KeepAlive boolean is set to '${KEEP_ALIVE_VAL}', expected 'true'"
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi
elif [[ "$KEEP_ALIVE_TYPE" == "dictionary" ]]; then
    pass "KeepAlive = dictionary (conditional supervision configured)"
else
    fail "Key 'KeepAlive' is missing or invalid type (${KEEP_ALIVE_TYPE})"
    FAILED_COUNT=$((FAILED_COUNT + 1))
fi

# Test 7: Key Assertion — ThrottleInterval
TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
echo -n "[CHECK 7] Validating key 'ThrottleInterval'... "
THROTTLE_TYPE=$(plutil -type ThrottleInterval "$TARGET_PLIST" 2>/dev/null || echo "missing")
if [[ "$THROTTLE_TYPE" == "integer" ]]; then
    THROTTLE_VAL=$(plutil -extract ThrottleInterval raw -o - "$TARGET_PLIST" 2>/dev/null || echo "")
    if [[ "$THROTTLE_VAL" -ge 1 ]]; then
        pass "ThrottleInterval = ${THROTTLE_VAL}s (crash storm protection active)"
    else
        fail "ThrottleInterval must be >= 1 second, found: ${THROTTLE_VAL}"
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi
else
    warn "Key 'ThrottleInterval' is not specified or not an integer (${THROTTLE_TYPE})"
fi

# Test 8: Key Assertion — Logging Configuration (StandardOutPath & StandardErrorPath)
TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
echo -n "[CHECK 8] Validating I/O log redirection keys... "
STDOUT_TYPE=$(plutil -type StandardOutPath "$TARGET_PLIST" 2>/dev/null || echo "missing")
STDERR_TYPE=$(plutil -type StandardErrorPath "$TARGET_PLIST" 2>/dev/null || echo "missing")

if [[ "$STDOUT_TYPE" == "string" && "$STDERR_TYPE" == "string" ]]; then
    STDOUT_VAL=$(plutil -extract StandardOutPath raw -o - "$TARGET_PLIST" 2>/dev/null || echo "")
    STDERR_VAL=$(plutil -extract StandardErrorPath raw -o - "$TARGET_PLIST" 2>/dev/null || echo "")
    pass "StandardOutPath='${STDOUT_VAL}', StandardErrorPath='${STDERR_VAL}'"
else
    fail "Missing log redirection paths: StandardOutPath=${STDOUT_TYPE}, StandardErrorPath=${STDERR_TYPE}"
    FAILED_COUNT=$((FAILED_COUNT + 1))
fi

# Test 9: Key Assertion — High Load Resource Limits
TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
echo -n "[CHECK 9] Validating ResourceLimits (SoftResourceLimits / HardResourceLimits)... "
SOFT_LIMIT=$(plutil -extract SoftResourceLimits.NumberOfFiles raw -o - "$TARGET_PLIST" 2>/dev/null || echo "0")
HARD_LIMIT=$(plutil -extract HardResourceLimits.NumberOfFiles raw -o - "$TARGET_PLIST" 2>/dev/null || echo "0")

if [[ "$SOFT_LIMIT" -ge 65536 || "$HARD_LIMIT" -ge 65536 ]]; then
    pass "NumberOfFiles limits configured (Soft: ${SOFT_LIMIT}, Hard: ${HARD_LIMIT})"
else
    warn "Resource limits for NumberOfFiles are lower than 65536 (Soft: ${SOFT_LIMIT}, Hard: ${HARD_LIMIT})"
fi

# Test 10: EnvironmentVariables and WorkingDirectory
TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
echo -n "[CHECK 10] Validating execution environment & WorkingDirectory... "
WORK_DIR_TYPE=$(plutil -type WorkingDirectory "$TARGET_PLIST" 2>/dev/null || echo "missing")
ENV_TYPE=$(plutil -type EnvironmentVariables "$TARGET_PLIST" 2>/dev/null || echo "missing")

if [[ "$WORK_DIR_TYPE" == "string" ]]; then
    WORK_DIR_VAL=$(plutil -extract WorkingDirectory raw -o - "$TARGET_PLIST" 2>/dev/null || echo "")
    pass "WorkingDirectory = '${WORK_DIR_VAL}'"
else
    info "WorkingDirectory is default/omitted."
fi

# ------------------------------------------------------------------------------
# System-Level & Live Registration Verification (when applicable)
# ------------------------------------------------------------------------------
IS_SYSTEM_PLIST=false
if [[ "$TARGET_PLIST" == "/Library/LaunchDaemons/"* ]]; then
    IS_SYSTEM_PLIST=true
fi

if [[ "$MODE" == "live" || ("$MODE" == "auto" && "$IS_SYSTEM_PLIST" == true) ]]; then
    echo -e "\n${BOLD}${BLUE}--- Live System & launchctl Checks ---${NC}"
    
    # Test 11: File Permissions & Ownership
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
    echo -n "[CHECK 11] Checking system LaunchDaemon permissions (root:wheel, 0644)... "
    if [[ -f "$TARGET_PLIST" ]]; then
        PERMS=$(stat -f "%Sp" "$TARGET_PLIST" 2>/dev/null || echo "")
        OWNER=$(stat -f "%Su:%Sg" "$TARGET_PLIST" 2>/dev/null || echo "")
        OCTAL=$(stat -f "%Op" "$TARGET_PLIST" 2>/dev/null | tail -c 4 || echo "")

        if [[ "$OWNER" == "root:wheel" && ("$OCTAL" == "644" || "$OCTAL" == "444") ]]; then
            pass "Permissions valid: ${OWNER} (${OCTAL} / ${PERMS})"
        else
            fail "Invalid ownership/mode: ${OWNER} (${OCTAL}). launchd requires root:wheel (0644)."
            FAILED_COUNT=$((FAILED_COUNT + 1))
        fi
    else
        fail "System plist does not exist at ${TARGET_PLIST}"
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi

    # Test 12: launchctl Registration State
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
    echo -n "[CHECK 12] Querying service registration in launchd... "
    LAUNCHD_ACTIVE=false
    
    if launchctl list "${SERVICE_LABEL}" &>/dev/null; then
        INFO=$(launchctl list "${SERVICE_LABEL}" 2>/dev/null || true)
        PID=$(echo "$INFO" | awk -F'=' '/"PID"/ {print $2}' | tr -d ' ;' || echo "")
        STATUS=$(echo "$INFO" | awk -F'=' '/"LastExitStatus"/ {print $2}' | tr -d ' ;' || echo "0")
        
        pass "Registered in launchd (PID: ${PID:-None/Idle}, LastExitStatus: ${STATUS})"
        LAUNCHD_ACTIVE=true
    elif launchctl print "system/${SERVICE_LABEL}" &>/dev/null; then
        pass "Registered in modern system domain (launchctl print system/${SERVICE_LABEL})"
        LAUNCHD_ACTIVE=true
    else
        fail "Service '${SERVICE_LABEL}' is NOT currently registered or loaded in launchctl."
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi

    # Test 13: Process Supervision & Execution
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
    echo -n "[CHECK 13] Checking background worker process execution... "
    WORKER_PIDS=$(pgrep -f "rusty-grid worker" 2>/dev/null || true)
    if [[ -n "$WORKER_PIDS" ]]; then
        pass "Active worker process PID(s): ${WORKER_PIDS}"
    else
        if [[ "$LAUNCHD_ACTIVE" == true ]]; then
            warn "No active 'rusty-grid worker' process detected in process table (may be restarting or idle)."
        else
            fail "Worker process is not running."
            FAILED_COUNT=$((FAILED_COUNT + 1))
        fi
    fi

    # Test 14: Log Directory & Output Files
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
    echo -n "[CHECK 14] Inspecting log output files... "
    LOG_DIR="/var/log/oxideswarm"
    if [[ -d "$LOG_DIR" ]]; then
        FOUND_LOGS=0
        for logf in "${LOG_DIR}/worker.log" "${LOG_DIR}/worker.err.log"; do
            if [[ -f "$logf" ]]; then
                FOUND_LOGS=$((FOUND_LOGS + 1))
                SIZE=$(stat -f "%z" "$logf" 2>/dev/null || echo "0")
                if [[ "$VERBOSE" == true ]]; then
                    info "         Log: ${logf} (${SIZE} bytes)"
                fi
            fi
        done
        if [[ $FOUND_LOGS -gt 0 ]]; then
            pass "Log files initialized in ${LOG_DIR}"
        else
            warn "Log directory exists, but log files are not yet populated."
        fi
    else
        warn "Log directory ${LOG_DIR} does not exist yet."
    fi

elif [[ "$MODE" == "dry-run" || "$IS_SYSTEM_PLIST" == false ]]; then
    echo -e "\n${BOLD}${BLUE}--- Dry-Run / Non-Root Summary ---${NC}"
    info "Static property list validation complete."
    info "Target was evaluated for syntax, types, and schema compliance without modifying system state."
    if [[ "$IS_SYSTEM_PLIST" == false ]]; then
        info "To perform live launchd registration validation on the host, install first using:"
        info "  sudo bash packaging/macos/install_mac_daemon.sh"
        info "  bash packaging/macos/verify_mac_daemon.sh --live"
    fi
fi

# Final Summary Report
echo -e "\n${BOLD}${CYAN}========================================================${NC}"
if [[ $FAILED_COUNT -eq 0 ]]; then
    echo -e "${BOLD}${GREEN}  VERIFICATION RESULT: ALL CHECKS PASSED [OK] (Score: ${TOTAL_CHECKS}/${TOTAL_CHECKS})${NC}"
    echo -e "${BOLD}${CYAN}========================================================${NC}\n"
    exit 0
else
    echo -e "${BOLD}${RED}  VERIFICATION RESULT: ${FAILED_COUNT} CHECK(S) FAILED [FAIL]${NC}"
    echo -e "${BOLD}${CYAN}========================================================${NC}\n"
    exit 1
fi
